//go:build nativeupgrade

// Package upgrade proves that intent written by the published predecessor
// native package survives an in-place package upgrade. The predecessor
// application creates the database with its own public API. The candidate
// application opens the same file, keeps that intent, and pushes it through
// the real adapter and extension.
//
// Expected values come from the client contract and the authored dataset in
// this file. They never come from an earlier run.
package upgrade

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	appVersion      = "1.0.0"
	clientTimestamp = "2026-09-01T10:00:00.000000Z"
	phaseTimeout    = 20 * time.Minute
)

// step is one public SDK action. The platform applications run steps in
// order and report each observation.
type step struct {
	Op       string `json:"op"`
	Database string `json:"database,omitempty"`
	ClientID string `json:"client_id,omitempty"`
	SQL      string `json:"sql,omitempty"`
	Params   []any  `json:"params,omitempty"`
	Name     string `json:"name,omitempty"`
}

type snapshotQuery struct {
	Name string `json:"name"`
	SQL  string `json:"sql"`
}

type phaseConfig struct {
	Phase      string          `json:"phase"`
	ServerURL  string          `json:"server_url"`
	Token      string          `json:"token"`
	AppVersion string          `json:"app_version"`
	Snapshots  []snapshotQuery `json:"snapshots"`
	Steps      []step          `json:"steps"`
}

type authoredField struct {
	FieldID     string          `json:"field_id"`
	LogicalType string          `json:"logical_type"`
	Value       json.RawMessage `json:"value"`
}

type mutation struct {
	MutationID           string          `json:"mutation_id"`
	LocalOrder           int64           `json:"local_order"`
	TableID              string          `json:"table_id"`
	TableName            string          `json:"table_name"`
	RecordID             string          `json:"record_id"`
	PrimaryKeyFieldID    string          `json:"primary_key_field_id"`
	PrimaryKeyType       string          `json:"primary_key_logical_type"`
	Operation            string          `json:"operation"`
	SchemaVersion        int64           `json:"schema_version"`
	SchemaHash           string          `json:"schema_hash"`
	BaseVersion          *string         `json:"base_version"`
	ClientVersion        string          `json:"client_version"`
	Status               string          `json:"status"`
	SourceKind           string          `json:"source_kind"`
	DependsOnMutationID  *string         `json:"depends_on_mutation_id"`
	NormalizedMutationID *string         `json:"normalized_mutation_id"`
	SealedBatchID        *string         `json:"sealed_batch_id"`
	SealedOrdinal        *int64          `json:"sealed_ordinal"`
	Fields               []authoredField `json:"fields"`
}

type observation struct {
	Name          string                       `json:"name"`
	Snapshots     map[string][]json.RawMessage `json:"snapshots"`
	Pending       []mutation                   `json:"pending"`
	PendingCount  int                          `json:"pending_count"`
	RejectedCount int                          `json:"rejected_count"`
}

type phaseResult struct {
	Phase        string        `json:"phase"`
	Package      string        `json:"package_version"`
	Error        string        `json:"error"`
	Observations []observation `json:"observations"`
}

func (r phaseResult) observation(t *testing.T, name string) observation {
	t.Helper()
	for _, value := range r.Observations {
		if value.Name == name {
			return value
		}
	}
	t.Fatalf("%s phase reported no %q observation: %s", r.Phase, name, r.Error)
	return observation{}
}

// complete fails when the application stopped before its last step.
func (r phaseResult) complete(t *testing.T) {
	t.Helper()
	if r.Error != "" {
		t.Fatalf("%s application failed: %s", r.Phase, r.Error)
	}
}

type environment struct {
	databaseURL    string
	appServerURL   string
	jwtSecret      string
	runner         string
	controlAddress string
	predecessor    string
	candidate      string
}

func loadEnvironment(t *testing.T) environment {
	t.Helper()
	value := environment{
		databaseURL:    os.Getenv("SYNCHRO_UPGRADE_DATABASE_URL"),
		appServerURL:   os.Getenv("SYNCHRO_TEST_URL"),
		jwtSecret:      os.Getenv("SYNCHRO_TEST_JWT_SECRET"),
		runner:         os.Getenv("SYNCHRO_UPGRADE_RUNNER"),
		controlAddress: os.Getenv("SYNCHRO_UPGRADE_CONTROL_ADDRESS"),
		predecessor:    os.Getenv("SYNCHRO_UPGRADE_PREDECESSOR_VERSION"),
		candidate:      os.Getenv("SYNCHRO_UPGRADE_CANDIDATE_VERSION"),
	}
	missing := []string{}
	for name, item := range map[string]string{
		"SYNCHRO_UPGRADE_DATABASE_URL":        value.databaseURL,
		"SYNCHRO_TEST_URL":                    value.appServerURL,
		"SYNCHRO_TEST_JWT_SECRET":             value.jwtSecret,
		"SYNCHRO_UPGRADE_RUNNER":              value.runner,
		"SYNCHRO_UPGRADE_CONTROL_ADDRESS":     value.controlAddress,
		"SYNCHRO_UPGRADE_PREDECESSOR_VERSION": value.predecessor,
		"SYNCHRO_UPGRADE_CANDIDATE_VERSION":   value.candidate,
	} {
		if strings.TrimSpace(item) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) != 0 {
		sort.Strings(missing)
		t.Fatalf("required upgrade environment is missing: %s", strings.Join(missing, ", "))
	}
	if value.predecessor == value.candidate {
		t.Fatalf("predecessor and candidate versions are both %s", value.predecessor)
	}
	value.appServerURL = strings.TrimRight(value.appServerURL, "/")
	return value
}

// dataset holds the run-scoped identities. Every row value is authored in
// the functions below.
type dataset struct {
	userID, clientID       string
	c1, c2, c3, c4         string
	o1, o2, o3, o4         string
	l1, l2, l3, l4, l5, l6 string
}

func newDataset(t *testing.T) dataset {
	t.Helper()
	id := func() string {
		var raw [16]byte
		if _, err := rand.Read(raw[:]); err != nil {
			t.Fatalf("generate identity: %v", err)
		}
		raw[6] = raw[6]&0x0f | 0x40
		raw[8] = raw[8]&0x3f | 0x80
		text := hex.EncodeToString(raw[:])
		return text[0:8] + "-" + text[8:12] + "-" + text[12:16] + "-" + text[16:20] + "-" + text[20:32]
	}
	return dataset{
		userID: "upgrade-" + id(), clientID: id(),
		c1: id(), c2: id(), c3: id(), c4: id(),
		o1: id(), o2: id(), o3: id(), o4: id(),
		l1: id(), l2: id(), l3: id(), l4: id(), l5: id(), l6: id(),
	}
}

// Server-authored source rows. orders.user_id comes from the server trigger.
func (d dataset) seedStatements() []statementWithArgs {
	return []statementWithArgs{
		{`INSERT INTO public.customers (id, user_id, name, email, balance, is_active, address) VALUES
			($1, $4, 'Ada Lovelace', 'ada@example.test', 100.00, true, '{"city":"London"}'),
			($2, $4, 'Grace Hopper', 'grace@example.test', 250.50, true, '{"city":"Arlington"}'),
			($3, $4, 'Alan Turing', NULL, 0, false, '{}')`, []any{d.c1, d.c2, d.c3, d.userID}},
		{`INSERT INTO public.orders (id, customer_id, status, total_price, order_comment) VALUES
			($1, $4, 'pending', 40.00, 'first order'),
			($2, $4, 'shipped', 15.25, NULL),
			($3, $5, 'pending', 102.99, 'gift')`, []any{d.o1, d.o2, d.o3, d.c1, d.c2}},
		{`INSERT INTO public.line_items (id, order_id, quantity, unit_price, line_status) VALUES
			($1, $6, 2, 10.00, 'open'),
			($2, $6, 1, 20.00, 'open'),
			($3, $7, 5, 3.05, 'shipped'),
			($4, $8, 1, 99.99, 'open'),
			($5, $8, 3, 1.00, 'open')`, []any{d.l1, d.l2, d.l3, d.l4, d.l5, d.o1, d.o2, d.o3}},
	}
}

// Subsequent source progress after the predecessor stops.
func (d dataset) remoteProgress() statementWithArgs {
	return statementWithArgs{`UPDATE public.customers SET name = 'Alan M. Turing', balance = 5 WHERE id = $1`, []any{d.c3}}
}

// Offline intent written by the predecessor application.
func (d dataset) offlineWrites() []step {
	execute := func(sql string, params ...any) step { return step{Op: "execute", SQL: sql, Params: params} }
	return []step{
		execute(`INSERT INTO customers (id, user_id, name, email, balance, is_active, address, created_at, updated_at) VALUES (?, ?, 'Katherine Johnson', 'katherine@example.test', '12.34', 1, '{"city":"Hampton"}', ?, ?)`,
			d.c4, d.userID, clientTimestamp, clientTimestamp),
		execute(`INSERT INTO orders (id, customer_id, user_id, status, total_price, currency, order_comment, created_at, updated_at) VALUES (?, ?, ?, 'pending', '12.34', 'USD', 'offline order', ?, ?)`,
			d.o4, d.c4, d.userID, clientTimestamp, clientTimestamp),
		execute(`INSERT INTO line_items (id, order_id, quantity, unit_price, discount, tax, line_status, created_at, updated_at) VALUES (?, ?, 2, '6.17', '0', '0', 'open', ?, ?)`,
			d.l6, d.o4, clientTimestamp, clientTimestamp),
		execute(`UPDATE customers SET name = 'Ada King', balance = '75' WHERE id = ?`, d.c1),
		execute(`UPDATE orders SET status = 'packed', order_comment = 'offline edit' WHERE id = ?`, d.o1),
		execute(`UPDATE line_items SET quantity = 4 WHERE id = ?`, d.l1),
		execute(`DELETE FROM line_items WHERE id = ?`, d.l2),
		execute(`UPDATE line_items SET deleted_at = ? WHERE id = ?`, clientTimestamp, d.l5),
		execute(`INSERT INTO local_notes (id, body) VALUES ('note-1', 'kept across the upgrade')`),
		execute(`INSERT INTO local_notes (id, body) VALUES ('note-2', 'local only')`),
	}
}

// A write after the upgrade goes through the capture triggers that the
// candidate migration left in the predecessor database.
func (d dataset) upgradedWrite() step {
	return step{Op: "execute", SQL: `UPDATE orders SET order_comment = 'after upgrade' WHERE id = ?`, Params: []any{d.o3}}
}

var snapshots = []snapshotQuery{
	{"customers", `SELECT id, user_id, name, email, balance, is_active, address, deleted_at IS NOT NULL AS deleted FROM customers ORDER BY id`},
	{"orders", `SELECT id, customer_id, user_id, status, total_price, order_comment, deleted_at IS NOT NULL AS deleted FROM orders ORDER BY id`},
	{"line_items", `SELECT id, order_id, quantity, unit_price, line_status, deleted_at IS NOT NULL AS deleted FROM line_items ORDER BY id`},
	{"local_notes", `SELECT id, body FROM local_notes ORDER BY id`},
}

type row = map[string]any

// Local rows use the SQLite representation of each portable type:
// decimal and JSON as canonical text and boolean as 0 or 1.
func customer(id, userID, name string, email any, balance string, active int, address string) row {
	return row{"id": id, "user_id": userID, "name": name, "email": email, "balance": balance, "is_active": active, "address": address, "deleted": 0}
}

func order(id, customerID, userID, status, total string, comment any) row {
	return row{"id": id, "customer_id": customerID, "user_id": userID, "status": status, "total_price": total, "order_comment": comment, "deleted": 0}
}

func lineItem(id, orderID string, quantity int, unitPrice, status string, deleted int) row {
	return row{"id": id, "order_id": orderID, "quantity": quantity, "unit_price": unitPrice, "line_status": status, "deleted": deleted}
}

func (d dataset) syncedClientRows() map[string][]row {
	return map[string][]row{
		"customers": {
			customer(d.c1, d.userID, "Ada Lovelace", "ada@example.test", "100", 1, `{"city":"London"}`),
			customer(d.c2, d.userID, "Grace Hopper", "grace@example.test", "250.5", 1, `{"city":"Arlington"}`),
			customer(d.c3, d.userID, "Alan Turing", nil, "0", 0, `{}`),
		},
		"orders": {
			order(d.o1, d.c1, d.userID, "pending", "40", "first order"),
			order(d.o2, d.c1, d.userID, "shipped", "15.25", nil),
			order(d.o3, d.c2, d.userID, "pending", "102.99", "gift"),
		},
		"line_items": {
			lineItem(d.l1, d.o1, 2, "10", "open", 0),
			lineItem(d.l2, d.o1, 1, "20", "open", 0),
			lineItem(d.l3, d.o2, 5, "3.05", "shipped", 0),
			lineItem(d.l4, d.o3, 1, "99.99", "open", 0),
			lineItem(d.l5, d.o3, 3, "1", "open", 0),
		},
		"local_notes": {},
	}
}

// A local DELETE on a table with a deletion field becomes a local soft
// delete, so the row remains with its deletion state.
func (d dataset) offlineClientRows() map[string][]row {
	return map[string][]row{
		"customers": {
			customer(d.c1, d.userID, "Ada King", "ada@example.test", "75", 1, `{"city":"London"}`),
			customer(d.c2, d.userID, "Grace Hopper", "grace@example.test", "250.5", 1, `{"city":"Arlington"}`),
			customer(d.c3, d.userID, "Alan Turing", nil, "0", 0, `{}`),
			customer(d.c4, d.userID, "Katherine Johnson", "katherine@example.test", "12.34", 1, `{"city":"Hampton"}`),
		},
		"orders": {
			order(d.o1, d.c1, d.userID, "packed", "40", "offline edit"),
			order(d.o2, d.c1, d.userID, "shipped", "15.25", nil),
			order(d.o3, d.c2, d.userID, "pending", "102.99", "gift"),
			order(d.o4, d.c4, d.userID, "pending", "12.34", "offline order"),
		},
		"line_items": {
			lineItem(d.l1, d.o1, 4, "10", "open", 0),
			lineItem(d.l2, d.o1, 1, "20", "open", 1),
			lineItem(d.l3, d.o2, 5, "3.05", "shipped", 0),
			lineItem(d.l4, d.o3, 1, "99.99", "open", 0),
			lineItem(d.l5, d.o3, 3, "1", "open", 1),
			lineItem(d.l6, d.o4, 2, "6.17", "open", 0),
		},
		"local_notes": {
			{"id": "note-1", "body": "kept across the upgrade"},
			{"id": "note-2", "body": "local only"},
		},
	}
}

// After push and pull the client holds each canonical server row.
func (d dataset) finalClientRows() map[string][]row {
	rows := d.offlineClientRows()
	for index, value := range rows["customers"] {
		if value["id"] == d.c3 {
			rows["customers"][index] = customer(d.c3, d.userID, "Alan M. Turing", nil, "5", 0, `{}`)
		}
	}
	for index, value := range rows["orders"] {
		if value["id"] == d.o3 {
			rows["orders"][index] = order(d.o3, d.c2, d.userID, "pending", "102.99", "after upgrade")
		}
	}
	return rows
}

// Server rows use PostgreSQL text output for each column.
func (d dataset) finalServerRows() map[string][]row {
	customerRow := func(id, name string, email any, balance, active, address string) row {
		return row{"id": id, "user_id": d.userID, "name": name, "email": email, "balance": balance, "is_active": active, "address": address, "deleted": "false"}
	}
	orderRow := func(id, customerID, status, total string, comment any) row {
		return row{"id": id, "customer_id": customerID, "user_id": d.userID, "status": status, "total_price": total, "order_comment": comment, "deleted": "false"}
	}
	itemRow := func(id, orderID, quantity, unitPrice, deleted string) row {
		return row{"id": id, "order_id": orderID, "quantity": quantity, "unit_price": unitPrice, "line_status": "open", "deleted": deleted}
	}
	items := []row{
		itemRow(d.l1, d.o1, "4", "10.00", "false"),
		itemRow(d.l2, d.o1, "1", "20.00", "true"),
		itemRow(d.l3, d.o2, "5", "3.05", "false"),
		itemRow(d.l4, d.o3, "1", "99.99", "false"),
		itemRow(d.l5, d.o3, "3", "1.00", "true"),
		itemRow(d.l6, d.o4, "2", "6.17", "false"),
	}
	items[2]["line_status"] = "shipped"
	return map[string][]row{
		"customers": {
			customerRow(d.c1, "Ada King", "ada@example.test", "75.00", "true", `{"city": "London"}`),
			customerRow(d.c2, "Grace Hopper", "grace@example.test", "250.50", "true", `{"city": "Arlington"}`),
			customerRow(d.c3, "Alan M. Turing", nil, "5.00", "false", `{}`),
			customerRow(d.c4, "Katherine Johnson", "katherine@example.test", "12.34", "true", `{"city": "Hampton"}`),
		},
		"orders": {
			orderRow(d.o1, d.c1, "packed", "40.00", "offline edit"),
			orderRow(d.o2, d.c1, "shipped", "15.25", nil),
			orderRow(d.o3, d.c2, "pending", "102.99", "after upgrade"),
			orderRow(d.o4, d.c4, "pending", "12.34", "offline order"),
		},
		"line_items": items,
	}
}

// expectedMutation is one authored intent in client-contract terms. Field
// values use the protocol 3 canonical JSON representation.
type expectedMutation struct {
	table, record, operation string
	fields                   map[string]string
}

func (d dataset) offlineIntent() []expectedMutation {
	jsonText := func(value string) string {
		encoded, _ := json.Marshal(value)
		return string(encoded)
	}
	return []expectedMutation{
		{"customers", d.c4, "insert", map[string]string{
			"user_id": jsonText(d.userID), "name": `"Katherine Johnson"`, "email": `"katherine@example.test"`,
			"balance": `"12.34"`, "is_active": `true`, "address": jsonText(`{"city":"Hampton"}`),
		}},
		{"orders", d.o4, "insert", map[string]string{
			"customer_id": jsonText(d.c4), "user_id": jsonText(d.userID), "status": `"pending"`,
			"total_price": `"12.34"`, "currency": `"USD"`, "order_comment": `"offline order"`,
		}},
		{"line_items", d.l6, "insert", map[string]string{
			"order_id": jsonText(d.o4), "quantity": `2`, "unit_price": `"6.17"`,
			"discount": `"0"`, "tax": `"0"`, "line_status": `"open"`,
		}},
		{"customers", d.c1, "update", map[string]string{"name": `"Ada King"`, "balance": `"75"`}},
		{"orders", d.o1, "update", map[string]string{"status": `"packed"`, "order_comment": `"offline edit"`}},
		{"line_items", d.l1, "update", map[string]string{"quantity": `4`}},
		{"line_items", d.l2, "delete", map[string]string{}},
		{"line_items", d.l5, "delete", map[string]string{}},
	}
}

func (d dataset) upgradedIntent() expectedMutation {
	return expectedMutation{"orders", d.o3, "update", map[string]string{"order_comment": `"after upgrade"`}}
}

type statementWithArgs struct {
	sql  string
	args []any
}

// serverSchema is the server's own registry for the synced tables.
type serverSchema struct {
	version     int64
	hash        string
	tableIDs    map[string]string
	pkFieldIDs  map[string]string
	fieldIDs    map[string]map[string]string
	fieldTypes  map[string]map[string]string
	fieldColumn map[string]string
}

func loadServerSchema(ctx context.Context, t *testing.T, database *sql.DB) serverSchema {
	t.Helper()
	schema := serverSchema{
		tableIDs: map[string]string{}, pkFieldIDs: map[string]string{},
		fieldIDs: map[string]map[string]string{}, fieldTypes: map[string]map[string]string{},
		fieldColumn: map[string]string{},
	}
	if err := database.QueryRowContext(ctx, `SELECT schema_version, schema_hash FROM synchro.sync_schema_manifest ORDER BY schema_version DESC LIMIT 1`).Scan(&schema.version, &schema.hash); err != nil {
		t.Fatalf("read current server schema manifest: %v", err)
	}
	rows, err := database.QueryContext(ctx, `SELECT r.table_name, r.table_id::text, r.primary_key_field_id::text, f.physical_column, f.field_id::text, f.portable_type
		FROM synchro.sync_registry r
		JOIN synchro.sync_registry_generations g ON g.generation = r.registry_generation AND g.state = 'active'
		JOIN synchro.sync_registry_fields f ON f.registry_generation = r.registry_generation AND f.relation_id = r.relation_id
		WHERE r.registration_kind = 'synced' AND r.table_name IN ('customers', 'orders', 'line_items')`)
	if err != nil {
		t.Fatalf("read server registry: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var table, tableID, pkFieldID, column, fieldID, portableType string
		if err := rows.Scan(&table, &tableID, &pkFieldID, &column, &fieldID, &portableType); err != nil {
			t.Fatalf("scan server registry: %v", err)
		}
		schema.tableIDs[table] = tableID
		schema.pkFieldIDs[table] = pkFieldID
		if schema.fieldIDs[table] == nil {
			schema.fieldIDs[table] = map[string]string{}
			schema.fieldTypes[table] = map[string]string{}
		}
		schema.fieldIDs[table][column] = fieldID
		schema.fieldTypes[table][column] = portableType
		schema.fieldColumn[fieldID] = table + "." + column
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read server registry rows: %v", err)
	}
	if len(schema.tableIDs) != 3 {
		t.Fatalf("server registry has %d of the 3 dataset tables", len(schema.tableIDs))
	}
	return schema
}

func TestNativePackageUpgrade(t *testing.T) {
	env := loadEnvironment(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*phaseTimeout)
	defer cancel()
	database, err := sql.Open("pgx", env.databaseURL)
	if err != nil {
		t.Fatalf("open server database: %v", err)
	}
	defer database.Close()
	schema := loadServerSchema(ctx, t, database)
	data := newDataset(t)
	token := bearerToken(t, env.jwtSecret, data.userID)

	for _, statement := range data.seedStatements() {
		if _, err := database.ExecContext(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("author server rows: %v", err)
		}
	}
	waitForServerEffects(ctx, t, database, data.seedEffects())

	control := startControl(t, env.controlAddress)
	predecessorSteps := []step{
		{Op: "open", Database: "synchro-upgrade.db", ClientID: data.clientID},
		{Op: "create_local_table"},
		{Op: "start"},
		{Op: "sync"},
		{Op: "observe", Name: "synced"},
		{Op: "stop"},
	}
	predecessorSteps = append(predecessorSteps, data.offlineWrites()...)
	predecessorSteps = append(predecessorSteps, step{Op: "observe", Name: "offline"}, step{Op: "close"})
	predecessor := control.run(ctx, t, env, phaseConfig{
		Phase: "predecessor", ServerURL: env.appServerURL, Token: token, AppVersion: appVersion,
		Snapshots: snapshots, Steps: predecessorSteps,
	})
	if predecessor.Package != env.predecessor {
		t.Fatalf("predecessor application reported package %q, want %q", predecessor.Package, env.predecessor)
	}

	synced := predecessor.observation(t, "synced")
	requireRows(t, "predecessor synced", synced, data.syncedClientRows())
	offline := predecessor.observation(t, "offline")
	requireRows(t, "predecessor offline", offline, data.offlineClientRows())
	requireIntent(t, "predecessor offline", offline.Pending, data.offlineIntent(), schema)
	if offline.PendingCount != len(data.offlineIntent()) || offline.RejectedCount != 0 {
		t.Fatalf("predecessor offline pending=%d rejected=%d, want %d and 0", offline.PendingCount, offline.RejectedCount, len(data.offlineIntent()))
	}
	predecessor.complete(t)

	progress := data.remoteProgress()
	if _, err := database.ExecContext(ctx, progress.sql, progress.args...); err != nil {
		t.Fatalf("author subsequent source progress: %v", err)
	}
	waitForServerEffects(ctx, t, database, map[string]int{"customers/" + data.c3: 2})

	// The predecessor state above is the fixture. The assertion covers the
	// candidate package and the resulting server state.
	t.Run("assertion", func(t *testing.T) {
		candidateSteps := []step{
			{Op: "open", Database: "synchro-upgrade.db", ClientID: data.clientID},
			{Op: "observe", Name: "retained"},
			data.upgradedWrite(),
			{Op: "observe", Name: "written"},
			{Op: "start"},
			{Op: "sync"},
			{Op: "observe", Name: "final"},
			{Op: "stop"},
			{Op: "close"},
		}
		candidate := control.run(ctx, t, env, phaseConfig{
			Phase: "candidate", ServerURL: env.appServerURL, Token: token, AppVersion: appVersion,
			Snapshots: snapshots, Steps: candidateSteps,
		})
		if candidate.Package != env.candidate {
			t.Fatalf("candidate application reported package %q, want %q", candidate.Package, env.candidate)
		}

		// The upgraded package reads the predecessor's exact queued intent.
		retained := candidate.observation(t, "retained")
		if diff := compareJSON(offline.Pending, retained.Pending); diff != "" {
			t.Fatalf("candidate changed retained predecessor intent: %s", diff)
		}
		requireRows(t, "candidate retained", retained, data.offlineClientRows())
		if retained.PendingCount != len(data.offlineIntent()) || retained.RejectedCount != 0 {
			t.Fatalf("candidate retained pending=%d rejected=%d, want %d and 0", retained.PendingCount, retained.RejectedCount, len(data.offlineIntent()))
		}

		written := candidate.observation(t, "written")
		requireIntent(t, "candidate written", written.Pending, append(data.offlineIntent(), data.upgradedIntent()), schema)
		if diff := compareJSON(offline.Pending, written.Pending[:len(offline.Pending)]); diff != "" {
			t.Fatalf("candidate write changed retained predecessor intent: %s", diff)
		}
		if last := written.Pending[len(written.Pending)-1]; last.LocalOrder <= offline.Pending[len(offline.Pending)-1].LocalOrder {
			t.Fatalf("candidate intent local order %d does not follow retained order %d", last.LocalOrder, offline.Pending[len(offline.Pending)-1].LocalOrder)
		}

		final := candidate.observation(t, "final")
		if final.PendingCount != 0 || len(final.Pending) != 0 || final.RejectedCount != 0 {
			t.Fatalf("candidate final pending=%d rejected=%d, want 0 and 0", final.PendingCount, final.RejectedCount)
		}
		requireRows(t, "candidate final", final, data.finalClientRows())

		candidate.complete(t)
		requireServerRows(ctx, t, database, data)
	})
}

// seedEffects names each authored source row and its minimum number of
// materialized changes before the predecessor may pull it.
func (d dataset) seedEffects() map[string]int {
	effects := map[string]int{}
	for _, id := range []string{d.c1, d.c2, d.c3} {
		effects["customers/"+id] = 1
	}
	for _, id := range []string{d.o1, d.o2, d.o3} {
		effects["orders/"+id] = 1
	}
	for _, id := range []string{d.l1, d.l2, d.l3, d.l4, d.l5} {
		effects["line_items/"+id] = 1
	}
	return effects
}

func waitForServerEffects(ctx context.Context, t *testing.T, database *sql.DB, effects map[string]int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		pending := []string{}
		for key, minimum := range effects {
			table, record, _ := strings.Cut(key, "/")
			var count int
			if err := database.QueryRowContext(ctx, `SELECT count(*) FROM synchro.sync_changelog WHERE table_name = $1 AND record_id = $2`, table, record).Scan(&count); err != nil {
				t.Fatalf("read materialized source changes: %v", err)
			}
			if count < minimum {
				pending = append(pending, key)
			}
		}
		if len(pending) == 0 {
			return
		}
		if time.Now().After(deadline) {
			sort.Strings(pending)
			t.Fatalf("WAL materialization did not reach %s", strings.Join(pending, ", "))
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func requireRows(t *testing.T, label string, value observation, expected map[string][]row) {
	t.Helper()
	for _, table := range []string{"customers", "orders", "line_items", "local_notes"} {
		sortRows(expected[table])
		if diff := compareJSON(expected[table], value.Snapshots[table]); diff != "" {
			t.Fatalf("%s %s rows differ: %s", label, table, diff)
		}
	}
}

func requireIntent(t *testing.T, label string, actual []mutation, expected []expectedMutation, schema serverSchema) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Fatalf("%s has %d pending mutations, want %d: %s", label, len(actual), len(expected), mustJSON(actual))
	}
	seen := map[string]bool{}
	for index, want := range expected {
		got := actual[index]
		context := fmt.Sprintf("%s mutation %d (%s %s %s)", label, index, want.operation, want.table, want.record)
		if got.TableName != want.table || got.RecordID != want.record || got.Operation != want.operation {
			t.Fatalf("%s is %s %s %s", context, got.Operation, got.TableName, got.RecordID)
		}
		if got.Status != "pending" {
			t.Fatalf("%s status is %q, want pending", context, got.Status)
		}
		if got.TableID != schema.tableIDs[want.table] || got.PrimaryKeyFieldID != schema.pkFieldIDs[want.table] || got.PrimaryKeyType != "string" {
			t.Fatalf("%s identity is table %s key %s/%s, want %s key %s/string", context, got.TableID, got.PrimaryKeyFieldID, got.PrimaryKeyType, schema.tableIDs[want.table], schema.pkFieldIDs[want.table])
		}
		if got.SchemaVersion != schema.version || got.SchemaHash != schema.hash {
			t.Fatalf("%s authored schema is %d/%s, want %d/%s", context, got.SchemaVersion, got.SchemaHash, schema.version, schema.hash)
		}
		if want.operation == "insert" && got.BaseVersion != nil {
			t.Fatalf("%s has base version %q", context, *got.BaseVersion)
		}
		if want.operation != "insert" && (got.BaseVersion == nil || *got.BaseVersion == "") {
			t.Fatalf("%s has no base version", context)
		}
		if _, err := time.Parse(time.RFC3339Nano, got.ClientVersion); err != nil || !strings.HasSuffix(got.ClientVersion, "Z") {
			t.Fatalf("%s client version %q is not an RFC 3339 UTC timestamp", context, got.ClientVersion)
		}
		if got.MutationID == "" || seen[got.MutationID] {
			t.Fatalf("%s mutation ID %q is empty or duplicated", context, got.MutationID)
		}
		seen[got.MutationID] = true
		if index > 0 && got.LocalOrder <= actual[index-1].LocalOrder {
			t.Fatalf("%s local order %d does not follow %d", context, got.LocalOrder, actual[index-1].LocalOrder)
		}
		if got.DependsOnMutationID != nil || got.NormalizedMutationID != nil || got.SealedBatchID != nil || got.SealedOrdinal != nil {
			t.Fatalf("%s has unexpected dependency or sealing state: %s", context, mustJSON(got))
		}
		gotFields := map[string]string{}
		for _, field := range got.Fields {
			column, known := schema.fieldColumn[field.FieldID]
			if !known || !strings.HasPrefix(column, want.table+".") {
				t.Fatalf("%s has field %s outside %s", context, field.FieldID, want.table)
			}
			name := strings.TrimPrefix(column, want.table+".")
			if field.LogicalType != schema.fieldTypes[want.table][name] {
				t.Fatalf("%s field %s type is %q, want %q", context, name, field.LogicalType, schema.fieldTypes[want.table][name])
			}
			gotFields[name] = canonical(t, field.Value)
		}
		wantFields := map[string]string{}
		for name, value := range want.fields {
			wantFields[name] = canonical(t, json.RawMessage(value))
		}
		if !reflect.DeepEqual(gotFields, wantFields) {
			t.Fatalf("%s authored fields are %v, want %v", context, gotFields, wantFields)
		}
	}
}

func requireServerRows(ctx context.Context, t *testing.T, database *sql.DB, data dataset) {
	t.Helper()
	queries := map[string]string{
		"customers": `SELECT id::text, user_id, name, email, balance::text, is_active::text, address::text, (deleted_at IS NOT NULL)::text AS deleted FROM public.customers WHERE user_id = $1 ORDER BY id::text`,
		"orders":    `SELECT id::text, customer_id::text, user_id, status, total_price::text, order_comment, (deleted_at IS NOT NULL)::text AS deleted FROM public.orders WHERE user_id = $1 ORDER BY id::text`,
		"line_items": `SELECT l.id::text, l.order_id::text, l.quantity::text, l.unit_price::text, l.line_status, (l.deleted_at IS NOT NULL)::text AS deleted
			FROM public.line_items l JOIN public.orders o ON o.id = l.order_id WHERE o.user_id = $1 ORDER BY l.id::text`,
	}
	expected := data.finalServerRows()
	for _, table := range []string{"customers", "orders", "line_items"} {
		rows, err := database.QueryContext(ctx, queries[table], data.userID)
		if err != nil {
			t.Fatalf("read server %s: %v", table, err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatalf("read server %s columns: %v", table, err)
		}
		actual := []row{}
		for rows.Next() {
			values := make([]sql.NullString, len(columns))
			targets := make([]any, len(columns))
			for index := range values {
				targets[index] = &values[index]
			}
			if err := rows.Scan(targets...); err != nil {
				t.Fatalf("scan server %s: %v", table, err)
			}
			item := row{}
			for index, column := range columns {
				if values[index].Valid {
					item[column] = values[index].String
				} else {
					item[column] = nil
				}
			}
			actual = append(actual, item)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("read server %s rows: %v", table, err)
		}
		rows.Close()
		sortRows(expected[table])
		if diff := compareJSON(expected[table], actual); diff != "" {
			t.Fatalf("server %s rows differ: %s", table, diff)
		}
	}
}

// Snapshots order rows by the text of their identity in both databases.
func sortRows(rows []row) {
	sort.Slice(rows, func(i, j int) bool { return rows[i]["id"].(string) < rows[j]["id"].(string) })
}

func canonical(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		t.Fatalf("decode JSON value %s: %v", raw, err)
	}
	return mustJSON(value)
}

func mustJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("<unencodable: %v>", err)
	}
	return string(encoded)
}

// compareJSON compares two values by their decoded JSON form. It returns an
// empty string when they are equal.
func compareJSON(expected, actual any) string {
	normalize := func(value any) (any, string) {
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, ""
		}
		decoder := json.NewDecoder(bytes.NewReader(encoded))
		decoder.UseNumber()
		var decoded any
		if err := decoder.Decode(&decoded); err != nil {
			return nil, ""
		}
		normalized, _ := json.Marshal(decoded)
		return decoded, string(normalized)
	}
	if expected == nil {
		expected = []any{}
	}
	_, left := normalize(expected)
	_, right := normalize(actual)
	if value := reflect.ValueOf(actual); !value.IsValid() || (value.Kind() == reflect.Slice && value.Len() == 0) {
		right = "[]"
	}
	if value := reflect.ValueOf(expected); value.Kind() == reflect.Slice && value.Len() == 0 {
		left = "[]"
	}
	if left == right {
		return ""
	}
	return fmt.Sprintf("want %s, got %s", left, right)
}

func bearerToken(t *testing.T, secret, subject string) string {
	t.Helper()
	encoding := base64.RawURLEncoding
	now := time.Now().Unix()
	header := encoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload := encoding.EncodeToString([]byte(mustJSON(map[string]any{"sub": subject, "iat": now, "exp": now + int64((6 * time.Hour).Seconds())})))
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(header + "." + payload))
	return header + "." + payload + "." + encoding.EncodeToString(mac.Sum(nil))
}

// control serves the current phase configuration to the platform
// application and receives its result.
type control struct {
	url     string
	mutex   sync.Mutex
	config  []byte
	results chan []byte
}

func startControl(t *testing.T, address string) *control {
	t.Helper()
	var secret [16]byte
	if _, err := rand.Read(secret[:]); err != nil {
		t.Fatalf("generate control path: %v", err)
	}
	path := "/" + hex.EncodeToString(secret[:])
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("listen for upgrade control: %v", err)
	}
	value := &control{url: "http://" + listener.Addr().String() + path, results: make(chan []byte, 1)}
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+path+"/config", func(response http.ResponseWriter, _ *http.Request) {
		value.mutex.Lock()
		config := value.config
		value.mutex.Unlock()
		if config == nil {
			http.Error(response, "no phase is active", http.StatusNotFound)
			return
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write(config)
	})
	mux.HandleFunc("POST "+path+"/result", func(response http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(io.LimitReader(request.Body, 8<<20))
		if err != nil {
			http.Error(response, "result is unreadable", http.StatusBadRequest)
			return
		}
		select {
		case value.results <- body:
			response.WriteHeader(http.StatusNoContent)
		default:
			http.Error(response, "a result is already recorded", http.StatusConflict)
		}
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	})
	return value
}

// run publishes one phase configuration and runs the platform runner. The
// runner installs the phase's package build, launches it, and returns after
// the application has reported. A failed application reports the
// observations that it completed, so the caller checks those first and
// then requires complete.
func (c *control) run(ctx context.Context, t *testing.T, env environment, config phaseConfig) phaseResult {
	t.Helper()
	encoded, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("encode %s configuration: %v", config.Phase, err)
	}
	c.mutex.Lock()
	c.config = encoded
	c.mutex.Unlock()
	defer func() {
		c.mutex.Lock()
		c.config = nil
		c.mutex.Unlock()
	}()

	doneFile := filepath.Join(t.TempDir(), config.Phase+".done")
	phaseContext, cancel := context.WithTimeout(ctx, phaseTimeout)
	defer cancel()
	command := exec.CommandContext(phaseContext, env.runner, config.Phase)
	command.Env = append(os.Environ(), "SYNCHRO_UPGRADE_CONTROL_URL="+c.url, "SYNCHRO_UPGRADE_DONE_FILE="+doneFile)
	output := &bytes.Buffer{}
	command.Stdout = output
	command.Stderr = output
	if err := command.Start(); err != nil {
		t.Fatalf("start %s runner: %v", config.Phase, err)
	}
	exited := make(chan error, 1)
	go func() { exited <- command.Wait() }()

	var body []byte
	var runnerErr error
	select {
	case body = <-c.results:
		if err := os.WriteFile(doneFile, []byte("done\n"), 0o600); err != nil {
			t.Fatalf("signal %s completion: %v", config.Phase, err)
		}
		runnerErr = <-exited
	case runnerErr = <-exited:
		select {
		case body = <-c.results:
		default:
			t.Fatalf("%s runner exited without a result: %v\n%s", config.Phase, runnerErr, output.String())
		}
	}
	var result phaseResult
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		t.Fatalf("decode %s result: %v\n%s", config.Phase, err, body)
	}
	if runnerErr != nil && result.Error == "" {
		t.Fatalf("%s runner failed: %v\n%s", config.Phase, runnerErr, output.String())
	}
	if result.Phase != config.Phase {
		t.Fatalf("result phase is %q, want %q", result.Phase, config.Phase)
	}
	if errors.Is(phaseContext.Err(), context.DeadlineExceeded) {
		t.Fatalf("%s phase exceeded %s", config.Phase, phaseTimeout)
	}
	return result
}
