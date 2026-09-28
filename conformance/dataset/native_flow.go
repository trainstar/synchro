package dataset

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gowebpki/jcs"
)

// NativePlatform drives one native client platform through the authored flow.
// The Swift, Kotlin, and React Native conformance drivers implement it.
type NativePlatform interface {
	// Open installs a fresh durable client for user and synchronizes it to idle.
	Open(ctx context.Context, key, user string) error
	// Write applies one authored local write through the public write path.
	Write(ctx context.Context, key string, write LocalWrite) error
	// Synchronize runs one public synchronization to idle.
	Synchronize(ctx context.Context, key string) error
	// Capture returns the local rows that selectors name and the count of
	// every local application row.
	Capture(ctx context.Context, key string, selectors []RowRef) ([]map[string]json.RawMessage, int, error)
}

// RowRef names one dataset row.
type RowRef struct {
	Table string
	ID    string
}

// LocalWrite is one local insert. Columns hold the authored local SQLite
// values as JSON: an int64 is a JSON integer, and every text type is a JSON
// string. Support holds values that the statement writes but the application
// does not author, so they stay out of the pushed mutation.
type LocalWrite struct {
	Table   string
	ID      string
	Columns map[string]json.RawMessage
	Support map[string]json.RawMessage
}

// nativeLocalTimestamp is the local creation and update time of the pushed
// row. The canonical server row replaces it.
const nativeLocalTimestamp = `"2026-03-02T07:05:00.000000Z"`

// ErrNativeMismatch reports local native state that differs from the
// authored expectation.
var ErrNativeMismatch = errors.New("native dataset state differs from the authored expectation")

const nativeMaterializeTimeout = 2 * time.Minute

// RunNativeFlow runs the authored flow through one native platform. The
// caller registers the dataset first. source is an administrator connection
// to the source database, which acts as the trusted application server.
func RunNativeFlow(ctx context.Context, source *sql.DB, platform NativePlatform, logf func(string, ...any)) error {
	if err := applyNativeStep(ctx, source, AuthoredSeed); err != nil {
		return err
	}
	selectors := authoredSelectors()
	incremental := make(map[string]string, len(AuthoredUsers))
	for _, user := range AuthoredUsers {
		incremental[user] = "dataset-" + user
	}

	push := AuthoredSetPush
	write, err := nativeLocalWrite(push)
	if err != nil {
		return err
	}
	key := incremental[push.User]
	if err := openLogged(ctx, platform, key, push.User, logf); err != nil {
		return err
	}
	if err := platform.Write(ctx, key, write); err != nil {
		return fmt.Errorf("write the authored push for %s: %w", push.User, err)
	}
	if err := platform.Synchronize(ctx, key); err != nil {
		return fmt.Errorf("push the authored write for %s: %w", push.User, err)
	}
	// The push fires source triggers whose rows reach the client in a later pull.
	if err := waitNativeMaterialized(ctx, source); err != nil {
		return err
	}
	if err := platform.Synchronize(ctx, key); err != nil {
		return fmt.Errorf("pull the authored push for %s: %w", push.User, err)
	}
	for _, user := range AuthoredUsers {
		if user == push.User {
			continue
		}
		if err := openLogged(ctx, platform, incremental[user], user, logf); err != nil {
			return err
		}
	}
	if err := requireNativeCheckpoint(ctx, source, platform, incremental, selectors, AuthoredInitial, logf); err != nil {
		return fmt.Errorf("initial checkpoint: %w", err)
	}

	for _, step := range AuthoredHistory {
		if err := applyNativeStep(ctx, source, step); err != nil {
			return err
		}
	}
	for _, user := range AuthoredUsers {
		started := time.Now()
		if err := platform.Synchronize(ctx, incremental[user]); err != nil {
			return fmt.Errorf("synchronize %s after the authored history: %w", user, err)
		}
		logf("dataset final sync %s: %s", user, time.Since(started))
	}
	if err := requireNativeCheckpoint(ctx, source, platform, incremental, selectors, AuthoredFinal, logf); err != nil {
		return fmt.Errorf("incremental final checkpoint: %w", err)
	}

	rebuilt := make(map[string]string, len(AuthoredUsers))
	for _, user := range AuthoredUsers {
		rebuilt[user] = "dataset-rebuild-" + user
		if err := openLogged(ctx, platform, rebuilt[user], user, logf); err != nil {
			return err
		}
	}
	if err := requireNativeCheckpoint(ctx, source, platform, rebuilt, selectors, AuthoredFinal, logf); err != nil {
		return fmt.Errorf("rebuild final checkpoint: %w", err)
	}
	return nil
}

func openLogged(ctx context.Context, platform NativePlatform, key, user string, logf func(string, ...any)) error {
	started := time.Now()
	if err := platform.Open(ctx, key, user); err != nil {
		return fmt.Errorf("open a fresh client %s for %s: %w", key, user, err)
	}
	logf("dataset initial sync %s (%s): %s", user, key, time.Since(started))
	return nil
}

// nativeLocalWrite maps the authored wire columns to local SQLite values.
func nativeLocalWrite(push AuthoredPush) (LocalWrite, error) {
	table, found := LookupTable(push.Table)
	if !found || push.Op != "insert" {
		return LocalWrite{}, errors.New("authored push is not a dataset insert")
	}
	columns := make(map[string]json.RawMessage, len(push.Columns))
	for name, wire := range push.Columns {
		portableType := ""
		for _, column := range table.Columns {
			if column.Name == name {
				portableType = column.Type
			}
		}
		switch portableType {
		case "":
			return LocalWrite{}, fmt.Errorf("authored push column %s is not synced", name)
		case "int64":
			var text string
			if err := json.Unmarshal([]byte(wire), &text); err != nil {
				return LocalWrite{}, fmt.Errorf("authored push column %s is not int64 text", name)
			}
			if _, err := strconv.ParseInt(text, 10, 64); err != nil {
				return LocalWrite{}, fmt.Errorf("authored push column %s is not int64 text", name)
			}
			columns[name] = json.RawMessage(text)
		case "bytes":
			return LocalWrite{}, fmt.Errorf("authored push column %s has no local JSON form", name)
		default:
			columns[name] = json.RawMessage(wire)
		}
	}
	// The local schema declares these columns NOT NULL without a default. The
	// application writes local values, and the server fills its own.
	owner, err := json.Marshal(push.User)
	if err != nil {
		return LocalWrite{}, err
	}
	support := map[string]json.RawMessage{"owner_id": owner, "created_at": json.RawMessage(nativeLocalTimestamp), "updated_at": json.RawMessage(nativeLocalTimestamp)}
	return LocalWrite{Table: push.Table, ID: push.ID, Columns: columns, Support: support}, nil
}

// authoredSelectors names every authored row identity of both checkpoints,
// so a row that a later step removes stays visible to the comparison.
func authoredSelectors() []RowRef {
	seen := map[string]bool{}
	var selectors []RowRef
	for _, checkpoint := range []Checkpoint{AuthoredInitial, AuthoredFinal} {
		for _, tables := range checkpoint.Rows {
			for table, ids := range tables {
				for _, id := range ids {
					if !seen[id] {
						seen[id] = true
						selectors = append(selectors, RowRef{Table: table, ID: id})
					}
				}
			}
		}
	}
	sort.Slice(selectors, func(left, right int) bool { return selectors[left].ID < selectors[right].ID })
	return selectors
}

func applyNativeStep(ctx context.Context, source *sql.DB, step AuthoredStep) error {
	transaction, err := source.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin dataset step %s: %w", step.Name, err)
	}
	if _, err := transaction.ExecContext(ctx, step.SQL); err != nil {
		_ = transaction.Rollback()
		return fmt.Errorf("apply dataset step %s: %w", step.Name, err)
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit dataset step %s: %w", step.Name, err)
	}
	for _, grant := range step.Grants {
		if _, err := source.ExecContext(ctx, "SELECT synchro.synchro_grant_user_scope($1, $2)", grant[0], grant[1]); err != nil {
			return fmt.Errorf("grant %s to %s: %w", grant[1], grant[0], err)
		}
	}
	for _, revoke := range step.Revokes {
		if _, err := source.ExecContext(ctx, "SELECT synchro.synchro_revoke_user_scope($1, $2)", revoke[0], revoke[1]); err != nil {
			return fmt.Errorf("revoke %s from %s: %w", revoke[1], revoke[0], err)
		}
	}
	return waitNativeMaterialized(ctx, source)
}

// waitNativeMaterialized waits until every committed fence is materialized.
// An active poison fails the wait with its bounded failure class.
func waitNativeMaterialized(ctx context.Context, source *sql.DB) error {
	deadline := time.Now().Add(nativeMaterializeTimeout)
	for {
		var pending int
		var poison sql.NullString
		if err := source.QueryRowContext(ctx, `
			SELECT (SELECT count(*) FROM synchro.sync_write_fences WHERE coverage = 'pending'),
			       (SELECT failure_class FROM synchro.sync_wal_poison WHERE lifecycle = 'active' LIMIT 1)`,
		).Scan(&pending, &poison); err != nil {
			return fmt.Errorf("observe dataset materialization: %w", err)
		}
		if poison.Valid {
			return fmt.Errorf("dataset source work poisoned the stream: %s", poison.String)
		}
		if pending == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("dataset materialization did not finish: %d pending fences", pending)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// requireNativeCheckpoint compares every local row and value of each client
// with the hand-written checkpoint and the canonical source values.
func requireNativeCheckpoint(ctx context.Context, source *sql.DB, platform NativePlatform, clients map[string]string, selectors []RowRef, checkpoint Checkpoint, logf func(string, ...any)) error {
	sourceRows, err := readNativeSourceRows(ctx, source)
	if err != nil {
		return err
	}
	tables := make(map[string]string, len(selectors))
	for _, selector := range selectors {
		tables[selector.ID] = selector.Table
	}
	delivered := make(map[string]bool, len(checkpoint.Values))
	for _, user := range AuthoredUsers {
		expected := map[string]bool{}
		for _, scope := range checkpoint.Assigned[user] {
			for _, ids := range checkpoint.Rows[scope] {
				for _, id := range ids {
					expected[id] = true
				}
			}
		}
		rows, total, err := platform.Capture(ctx, clients[user], selectors)
		if err != nil {
			return fmt.Errorf("capture %s: %w", user, err)
		}
		logf("dataset local rows %s (%s): %d", user, clients[user], total)
		local := make(map[string]map[string]json.RawMessage, len(rows))
		for _, row := range rows {
			var id string
			if err := json.Unmarshal(row["id"], &id); err != nil || tables[id] == "" || local[id] != nil {
				return fmt.Errorf("%w: %s holds an unexpected local row %s", ErrNativeMismatch, user, row["id"])
			}
			local[id] = row
		}
		if total != len(local) {
			return fmt.Errorf("%w: %s holds %d local rows, %d of them authored", ErrNativeMismatch, user, total, len(local))
		}
		for id := range expected {
			if local[id] == nil {
				return fmt.Errorf("%w: %s lacks %s/%s", ErrNativeMismatch, user, tables[id], id)
			}
		}
		for id := range local {
			if !expected[id] {
				return fmt.Errorf("%w: %s holds %s/%s outside its scopes", ErrNativeMismatch, user, tables[id], id)
			}
			table, _ := LookupTable(tables[id])
			row := local[id]
			if len(row) != len(table.Columns) {
				return fmt.Errorf("%w: %s %s/%s has %d local columns, want %d", ErrNativeMismatch, user, table.Name, id, len(row), len(table.Columns))
			}
			for _, column := range table.Columns {
				raw, found := row[column.Name]
				if !found {
					return fmt.Errorf("%w: %s %s/%s lacks column %s", ErrNativeMismatch, user, table.Name, id, column.Name)
				}
				wire, err := localWire(column.Type, raw)
				if err != nil {
					return fmt.Errorf("%w: %s %s/%s.%s: %v", ErrNativeMismatch, user, table.Name, id, column.Name, err)
				}
				if err := CompareWire(column.Type, wire, sourceRows[id][column.Name]); err != nil {
					return fmt.Errorf("%w: %s %s/%s.%s: %v", ErrNativeMismatch, user, table.Name, id, column.Name, err)
				}
			}
		}
		for _, value := range checkpoint.Values {
			row := local[value.ID]
			if row == nil {
				continue
			}
			column, _ := lookupColumn(value.Table, value.Column)
			wire, err := localWire(column.Type, row[value.Column])
			if err != nil || !bytes.Equal(wire, []byte(value.Wire)) {
				return fmt.Errorf("%w: %s %s/%s.%s = %s, hand-written %s", ErrNativeMismatch, user, value.Table, value.ID, value.Column, wire, value.Wire)
			}
			delivered[value.Table+"/"+value.ID+"/"+value.Column] = true
		}
	}
	for _, value := range checkpoint.Values {
		if !delivered[value.Table+"/"+value.ID+"/"+value.Column] {
			return fmt.Errorf("%w: no client holds hand-written value %s/%s.%s", ErrNativeMismatch, value.Table, value.ID, value.Column)
		}
	}
	return nil
}

func lookupColumn(tableName, columnName string) (Column, bool) {
	table, _ := LookupTable(tableName)
	for _, column := range table.Columns {
		if column.Name == columnName {
			return column, true
		}
	}
	return Column{}, false
}

// readNativeSourceRows reads every canonical source value by record identity.
func readNativeSourceRows(ctx context.Context, source *sql.DB) (map[string]map[string]*string, error) {
	values := map[string]map[string]*string{}
	for _, table := range Tables {
		rows, err := source.QueryContext(ctx, SourceRowsSQL(table))
		if err != nil {
			return nil, fmt.Errorf("query dataset source %s: %w", table.Name, err)
		}
		for rows.Next() {
			cells := make([]sql.NullString, len(table.Columns))
			targets := make([]any, len(cells))
			for index := range cells {
				targets[index] = &cells[index]
			}
			if err := rows.Scan(targets...); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scan dataset source %s: %w", table.Name, err)
			}
			row := make(map[string]*string, len(cells))
			for index, cell := range cells {
				if cell.Valid {
					text := cell.String
					row[table.Columns[index].Name] = &text
				}
			}
			values[*row["id"]] = row
		}
		if err := rows.Close(); err != nil {
			return nil, fmt.Errorf("close dataset source %s: %w", table.Name, err)
		}
	}
	return values, nil
}

// localWire maps one captured local SQLite value to its canonical wire value.
// A React Native bridge tags an unsafe integer and a blob as JSON objects.
func localWire(portableType string, raw json.RawMessage) (json.RawMessage, error) {
	raw = bytes.TrimSpace(raw)
	if string(raw) == "null" {
		return json.RawMessage("null"), nil
	}
	if len(raw) > 0 && raw[0] == '{' {
		var tagged map[string]string
		if err := json.Unmarshal(raw, &tagged); err != nil || len(tagged) != 2 {
			return nil, errors.New("tagged local value is invalid")
		}
		switch {
		case tagged["type"] == "int64" && portableType == "int64":
			if _, err := strconv.ParseInt(tagged["value"], 10, 64); err != nil {
				return nil, errors.New("tagged local int64 is invalid")
			}
			return wireString(tagged["value"])
		case tagged["type"] == "bytes" && portableType == "bytes":
			return wireString(tagged["base64"])
		default:
			return nil, fmt.Errorf("tagged local value does not match %s", portableType)
		}
	}
	switch portableType {
	case "int", "int64":
		var number json.Number
		if err := json.Unmarshal(raw, &number); err != nil {
			return nil, fmt.Errorf("local %s is not a JSON number", portableType)
		}
		value, err := strconv.ParseInt(number.String(), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("local %s is not an integer", portableType)
		}
		if portableType == "int64" {
			return wireString(strconv.FormatInt(value, 10))
		}
		return json.RawMessage(strconv.FormatInt(value, 10)), nil
	case "float":
		var value float64
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, errors.New("local float is not a JSON number")
		}
		text, err := jcs.NumberToJSON(value)
		if err != nil {
			return nil, errors.New("local float has no canonical form")
		}
		return json.RawMessage(text), nil
	default:
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return nil, fmt.Errorf("local %s is not text", portableType)
		}
		return wireString(text)
	}
}

func wireString(text string) (json.RawMessage, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(text); err != nil {
		return nil, err
	}
	return json.RawMessage(strings.TrimSuffix(buffer.String(), "\n")), nil
}
