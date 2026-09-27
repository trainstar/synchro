package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/dataset"
)

// datasetTable is one manifest table of the synthetic dataset.
type datasetTable struct {
	ID       string
	Name     string
	PKField  string
	FieldIDs map[string]string
	Names    map[string]string
	Types    map[string]string
}

// datasetClient is a protocol client that keeps the rows it received per
// scope. It applies rebuild pages and pull changes without sync semantics of
// its own, so its state is a direct record of server output.
type datasetClient struct {
	User            string
	Token           string
	ID              string
	Generation      int64
	Schema          json.RawMessage
	ScopeSetVersion int64
	Cursors         map[string]*string
	Tables          map[string]*datasetTable
	ByID            map[string]*datasetTable
	Rows            map[string]map[string]map[string]json.RawMessage
}

type datasetRuntime struct {
	t       *testing.T
	ctx     context.Context
	harness *blackbox.Harness
	admin   *sql.DB
	http    *http.Client
}

func provisionRealDatasetHarness(t *testing.T, ctx context.Context) *blackbox.Harness {
	t.Helper()
	if !*provision || !*install {
		t.Fatal("real dataset proof requires --provision --install")
	}
	environment, err := blackbox.LoadEnvironment()
	if err != nil {
		t.Fatalf("load real dataset environment: %v", err)
	}
	harness, err := blackbox.Provision(ctx, blackbox.HarnessConfig{
		Environment: environment,
		SourceSetup: &blackbox.SourceSetup{
			Name:            "dataset",
			SchemaSQL:       dataset.SchemaSQL,
			RegistrationSQL: dataset.RegistrationSQL,
			Tables:          dataset.TableNames(),
		},
	})
	if err != nil {
		t.Fatalf("provision real dataset harness: %v", err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := harness.Close(closeContext); err != nil {
			t.Errorf("close real dataset harness: %v", err)
		}
	})
	// Only the dataset scopes remain assigned to dataset users.
	if err := harness.Operator().UnregisterDefaultSharedScope(ctx); err != nil {
		t.Fatalf("unregister the diagnostic shared scope: %v", err)
	}
	return harness
}

func newDatasetRuntime(t *testing.T, ctx context.Context, harness *blackbox.Harness) *datasetRuntime {
	return &datasetRuntime{
		t: t, ctx: ctx, harness: harness, admin: openIssue49Admin(t, ctx, harness),
		http: &http.Client{Timeout: 2 * time.Minute},
	}
}

// with returns a runtime that reports failures to t.
func (runtime *datasetRuntime) with(t *testing.T) *datasetRuntime {
	copied := *runtime
	copied.t = t
	return &copied
}

// post sends one protocol request. It returns the status and the raw body.
func (runtime *datasetRuntime) post(token, path string, payload any) (int, []byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, fmt.Errorf("encode %s request: %w", path, err)
	}
	request, err := http.NewRequestWithContext(runtime.ctx, http.MethodPost, runtime.harness.AdapterURL()+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, fmt.Errorf("create %s request: %w", path, err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := runtime.http.Do(request)
	if err != nil {
		return 0, nil, fmt.Errorf("send %s request: %w", path, err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 64<<20))
	if err != nil {
		return 0, nil, fmt.Errorf("read %s response: %w", path, err)
	}
	return response.StatusCode, data, nil
}

func decodeDataset(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	return decoder.Decode(target)
}

func (runtime *datasetRuntime) sync(token, path string, payload, target any) {
	runtime.t.Helper()
	status, data, err := runtime.post(token, path, payload)
	if err != nil || status != http.StatusOK {
		runtime.t.Fatalf("dataset %s status=%d err=%v body=%.400s", path, status, err, data)
	}
	if err := decodeDataset(data, target); err != nil {
		runtime.t.Fatalf("decode dataset %s response: %v", path, err)
	}
}

type datasetConnectResponse struct {
	ClientGeneration int64           `json:"client_generation"`
	ScopeSetVersion  int64           `json:"scope_set_version"`
	Schema           json.RawMessage `json:"schema"`
	SchemaDefinition *struct {
		Tables []struct {
			TableID           string `json:"table_id"`
			Name              string `json:"name"`
			PrimaryKeyFieldID string `json:"primary_key_field_id"`
			Fields            []struct {
				FieldID string `json:"field_id"`
				Name    string `json:"name"`
				Type    string `json:"type"`
			} `json:"fields"`
		} `json:"tables"`
	} `json:"schema_definition"`
	Scopes struct {
		Add []struct {
			ID     string  `json:"id"`
			Cursor *string `json:"cursor"`
		} `json:"add"`
		Remove []string `json:"remove"`
	} `json:"scopes"`
	ScopeCursorUpdates map[string]*string `json:"scope_cursor_updates"`
}

func (runtime *datasetRuntime) token(user string) string {
	runtime.t.Helper()
	token, err := runtime.harness.NativeBearerToken(runtime.ctx, user, time.Now())
	if err != nil {
		runtime.t.Fatalf("sign dataset token for %s: %v", user, err)
	}
	return token
}

// connect registers a fresh client for user and applies its assignment.
func (runtime *datasetRuntime) connect(user, clientID string) *datasetClient {
	runtime.t.Helper()
	client := &datasetClient{
		User: user, Token: runtime.token(user), ID: clientID, Cursors: map[string]*string{},
		Rows: map[string]map[string]map[string]json.RawMessage{},
	}
	var response datasetConnectResponse
	runtime.sync(client.Token, "/sync/connect", map[string]any{
		"client_id": clientID, "platform": "conformance", "app_version": "0.3.0", "protocol_version": 3,
		"schema": map[string]any{"version": 0, "hash": ""}, "scope_set_version": 0, "known_scopes": map[string]any{},
	}, &response)
	runtime.applyConnect(client, response)
	return client
}

// reconnect presents the client's current state and applies the assignment delta.
func (runtime *datasetRuntime) reconnect(client *datasetClient) {
	runtime.t.Helper()
	known := make(map[string]any, len(client.Cursors))
	for scope, cursor := range client.Cursors {
		known[scope] = map[string]any{"cursor": cursor}
	}
	var schema map[string]any
	if err := json.Unmarshal(client.Schema, &schema); err != nil {
		runtime.t.Fatalf("decode dataset client schema: %v", err)
	}
	var response datasetConnectResponse
	runtime.sync(client.Token, "/sync/connect", map[string]any{
		"client_id": client.ID, "client_generation": client.Generation, "platform": "conformance",
		"app_version": "0.3.0", "protocol_version": 3, "schema": schema,
		"scope_set_version": client.ScopeSetVersion, "known_scopes": known,
	}, &response)
	runtime.applyConnect(client, response)
}

func (runtime *datasetRuntime) applyConnect(client *datasetClient, response datasetConnectResponse) {
	runtime.t.Helper()
	var schema struct {
		Version json.Number `json:"version"`
		Hash    string      `json:"hash"`
		Action  string      `json:"action"`
	}
	if err := decodeDataset(response.Schema, &schema); err != nil || response.ClientGeneration <= 0 {
		runtime.t.Fatalf("dataset connect envelope is invalid: %v", err)
	}
	client.Generation = response.ClientGeneration
	client.ScopeSetVersion = response.ScopeSetVersion
	client.Schema, _ = json.Marshal(map[string]any{"version": schema.Version, "hash": schema.Hash})
	if response.SchemaDefinition != nil {
		client.Tables = map[string]*datasetTable{}
		client.ByID = map[string]*datasetTable{}
		for _, raw := range response.SchemaDefinition.Tables {
			table := &datasetTable{
				ID: raw.TableID, Name: raw.Name, PKField: raw.PrimaryKeyFieldID,
				FieldIDs: map[string]string{}, Names: map[string]string{}, Types: map[string]string{},
			}
			for _, field := range raw.Fields {
				table.FieldIDs[field.Name] = field.FieldID
				table.Names[field.FieldID] = field.Name
				table.Types[field.Name] = field.Type
			}
			client.Tables[raw.Name] = table
			client.ByID[raw.TableID] = table
		}
	} else if schema.Action != "none" || client.Tables == nil {
		runtime.t.Fatalf("dataset connect schema action %q has no usable manifest", schema.Action)
	}
	for _, scope := range response.Scopes.Remove {
		delete(client.Cursors, scope)
		delete(client.Rows, scope)
	}
	for _, added := range response.Scopes.Add {
		client.Cursors[added.ID] = added.Cursor
	}
	for scope, cursor := range response.ScopeCursorUpdates {
		client.Cursors[scope] = cursor
	}
	for scope, cursor := range client.Cursors {
		if cursor == nil {
			runtime.rebuild(client, scope)
		}
	}
}

type datasetRecord struct {
	Scope         string                     `json:"scope"`
	Table         string                     `json:"table"`
	Op            string                     `json:"op"`
	PK            map[string]json.RawMessage `json:"pk"`
	Row           map[string]json.RawMessage `json:"row"`
	ServerVersion string                     `json:"server_version"`
}

func (client *datasetClient) store(scope string, record datasetRecord, t *testing.T) {
	t.Helper()
	table := client.ByID[record.Table]
	if table == nil {
		t.Fatalf("dataset change names unknown table %s", record.Table)
	}
	var id string
	if err := json.Unmarshal(record.PK[table.PKField], &id); err != nil || len(record.PK) != 1 {
		t.Fatalf("dataset change key is invalid: %v", record.PK)
	}
	key := table.Name + "/" + id
	if client.Rows[scope] == nil {
		client.Rows[scope] = map[string]map[string]json.RawMessage{}
	}
	if record.Op == "delete" {
		delete(client.Rows[scope], key)
		return
	}
	if record.Row == nil || record.ServerVersion == "" {
		t.Fatalf("dataset %s change for %s has no row or version", record.Op, key)
	}
	row := make(map[string]json.RawMessage, len(record.Row))
	for fieldID, value := range record.Row {
		name, ok := table.Names[fieldID]
		if !ok {
			t.Fatalf("dataset row for %s has unknown field %s", key, fieldID)
		}
		row[name] = value
	}
	client.Rows[scope][key] = row
}

// rebuildTimings records the first page and complete rebuild durations.
type rebuildTimings struct {
	FirstPage time.Duration
	Total     time.Duration
	Pages     int
	Records   int
	Bytes     int
}

func (runtime *datasetRuntime) rebuild(client *datasetClient, scope string) rebuildTimings {
	runtime.t.Helper()
	rebuildID := dataset.NewRandom(uint64(time.Now().UnixNano())).UUID()
	var cursor any
	client.Rows[scope] = map[string]map[string]json.RawMessage{}
	var timings rebuildTimings
	started := time.Now()
	for page := 0; page < 10000; page++ {
		status, data, err := runtime.post(client.Token, "/sync/rebuild", map[string]any{
			"client_id": client.ID, "client_generation": client.Generation, "schema": json.RawMessage(client.Schema),
			"scope": scope, "rebuild_id": rebuildID, "cursor": cursor, "limit": 500,
		})
		if page == 0 {
			timings.FirstPage = time.Since(started)
		}
		if err != nil || status != http.StatusOK {
			runtime.t.Fatalf("dataset rebuild %s status=%d err=%v body=%.400s", scope, status, err, data)
		}
		timings.Pages++
		timings.Bytes += len(data)
		var response struct {
			Scope            string          `json:"scope"`
			Records          []datasetRecord `json:"records"`
			HasMore          bool            `json:"has_more"`
			Cursor           *string         `json:"cursor"`
			FinalScopeCursor *string         `json:"final_scope_cursor"`
		}
		if err := decodeDataset(data, &response); err != nil || response.Scope != scope {
			runtime.t.Fatalf("dataset rebuild %s response is invalid: %v", scope, err)
		}
		for _, record := range response.Records {
			record.Op = "upsert"
			client.store(scope, record, runtime.t)
		}
		timings.Records += len(response.Records)
		if response.HasMore {
			if response.Cursor == nil {
				runtime.t.Fatalf("dataset rebuild %s continuation is missing", scope)
			}
			cursor = *response.Cursor
			continue
		}
		if response.FinalScopeCursor == nil {
			runtime.t.Fatalf("dataset rebuild %s final cursor is missing", scope)
		}
		client.Cursors[scope] = response.FinalScopeCursor
		timings.Total = time.Since(started)
		return timings
	}
	runtime.t.Fatalf("dataset rebuild %s exceeded its page bound", scope)
	return timings
}

// pull reads pages until the terminal page and returns the change count.
func (runtime *datasetRuntime) pull(client *datasetClient) int {
	runtime.t.Helper()
	changes := 0
	for page := 0; page < 10000; page++ {
		scopes := make(map[string]any, len(client.Cursors))
		for scope, cursor := range client.Cursors {
			scopes[scope] = map[string]any{"cursor": cursor}
		}
		var response struct {
			Changes         []datasetRecord    `json:"changes"`
			ScopeSetVersion int64              `json:"scope_set_version"`
			ScopeCursors    map[string]*string `json:"scope_cursors"`
			ScopeUpdates    struct {
				Add []struct {
					ID     string  `json:"id"`
					Cursor *string `json:"cursor"`
				} `json:"add"`
				Remove []string `json:"remove"`
			} `json:"scope_updates"`
			Rebuild []string `json:"rebuild"`
			HasMore bool     `json:"has_more"`
		}
		runtime.sync(client.Token, "/sync/pull", map[string]any{
			"client_id": client.ID, "client_generation": client.Generation, "schema": json.RawMessage(client.Schema),
			"scope_set_version": client.ScopeSetVersion, "scopes": scopes, "limit": 1000,
		}, &response)
		for _, change := range response.Changes {
			client.store(change.Scope, change, runtime.t)
		}
		changes += len(response.Changes)
		client.ScopeSetVersion = response.ScopeSetVersion
		for _, scope := range response.ScopeUpdates.Remove {
			delete(client.Cursors, scope)
			delete(client.Rows, scope)
		}
		for _, added := range response.ScopeUpdates.Add {
			client.Cursors[added.ID] = added.Cursor
		}
		for scope, cursor := range response.ScopeCursors {
			client.Cursors[scope] = cursor
		}
		for _, scope := range response.Rebuild {
			runtime.rebuild(client, scope)
		}
		if !response.HasMore {
			return changes
		}
	}
	runtime.t.Fatal("dataset pull exceeded its page bound")
	return changes
}

// applyTransaction commits one source transaction as the trusted application
// server. It returns the commit duration.
func (runtime *datasetRuntime) applyTransaction(statements []dataset.Statement) time.Duration {
	runtime.t.Helper()
	started := time.Now()
	transaction, err := runtime.admin.BeginTx(runtime.ctx, nil)
	if err != nil {
		runtime.t.Fatalf("begin dataset source transaction: %v", err)
	}
	for _, statement := range statements {
		if _, err := transaction.ExecContext(runtime.ctx, statement.SQL, statement.Args...); err != nil {
			_ = transaction.Rollback()
			runtime.t.Fatalf("dataset source statement failed: %v: %.200s", err, statement.SQL)
		}
	}
	if err := transaction.Commit(); err != nil {
		runtime.t.Fatalf("commit dataset source transaction: %v", err)
	}
	return time.Since(started)
}

func (runtime *datasetRuntime) applyAssignments(grants, revokes [][2]string) {
	runtime.t.Helper()
	for _, grant := range grants {
		if err := runtime.harness.Operator().GrantUserScope(runtime.ctx, grant[0], grant[1]); err != nil {
			runtime.t.Fatalf("grant %s to %s: %v", grant[1], grant[0], err)
		}
	}
	for _, revoke := range revokes {
		if err := runtime.harness.Operator().RevokeUserScope(runtime.ctx, revoke[0], revoke[1]); err != nil {
			runtime.t.Fatalf("revoke %s from %s: %v", revoke[1], revoke[0], err)
		}
	}
}

// waitMaterialized waits until every committed fence is materialized. An
// active poison fails the wait with its bounded failure class.
func (runtime *datasetRuntime) waitMaterialized(timeout time.Duration) time.Duration {
	runtime.t.Helper()
	started := time.Now()
	deadline := started.Add(timeout)
	var pending int
	for {
		var poison sql.NullString
		err := runtime.admin.QueryRowContext(runtime.ctx, `
			SELECT (SELECT count(*) FROM synchro.sync_write_fences WHERE coverage = 'pending'),
			       (SELECT failure_class || ': ' || failure_detail FROM synchro.sync_wal_poison WHERE lifecycle = 'active' LIMIT 1)`,
		).Scan(&pending, &poison)
		if err != nil {
			runtime.t.Fatalf("observe dataset materialization: %v", err)
		}
		if poison.Valid {
			runtime.t.Fatalf("dataset source work poisoned the stream: %s", poison.String)
		}
		if pending == 0 {
			return time.Since(started)
		}
		if time.Now().After(deadline) {
			runtime.t.Fatalf("dataset materialization did not finish: %d pending fences", pending)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// expectedState reads the authored business rule and every source row.
type expectedState struct {
	scopes map[string]map[string]bool
	rows   map[string]map[string]*string
}

func (runtime *datasetRuntime) expected() expectedState {
	runtime.t.Helper()
	state := expectedState{scopes: map[string]map[string]bool{}, rows: map[string]map[string]*string{}}
	rows, err := runtime.admin.QueryContext(runtime.ctx, dataset.ScopeRowsSQL)
	if err != nil {
		runtime.t.Fatalf("query dataset scope rule: %v", err)
	}
	for rows.Next() {
		var scope, table, id string
		if err := rows.Scan(&scope, &table, &id); err != nil {
			runtime.t.Fatalf("scan dataset scope rule: %v", err)
		}
		if state.scopes[scope] == nil {
			state.scopes[scope] = map[string]bool{}
		}
		state.scopes[scope][table+"/"+id] = true
	}
	if err := rows.Close(); err != nil {
		runtime.t.Fatalf("close dataset scope rule: %v", err)
	}
	for _, table := range dataset.Tables {
		rows, err := runtime.admin.QueryContext(runtime.ctx, dataset.SourceRowsSQL(table))
		if err != nil {
			runtime.t.Fatalf("query dataset source %s: %v", table.Name, err)
		}
		for rows.Next() {
			values := make([]sql.NullString, len(table.Columns))
			targets := make([]any, len(values))
			for index := range values {
				targets[index] = &values[index]
			}
			if err := rows.Scan(targets...); err != nil {
				runtime.t.Fatalf("scan dataset source %s: %v", table.Name, err)
			}
			row := make(map[string]*string, len(values))
			for index, column := range table.Columns {
				if values[index].Valid {
					value := values[index].String
					row[column.Name] = &value
				} else {
					row[column.Name] = nil
				}
			}
			state.rows[table.Name+"/"+*row["id"]] = row
		}
		if err := rows.Close(); err != nil {
			runtime.t.Fatalf("close dataset source %s: %v", table.Name, err)
		}
	}
	return state
}

func (runtime *datasetRuntime) assignedScopes(user string) []string {
	runtime.t.Helper()
	rows, err := runtime.admin.QueryContext(runtime.ctx, dataset.AssignedScopesSQL, user)
	if err != nil {
		runtime.t.Fatalf("query dataset assignment rule: %v", err)
	}
	defer rows.Close()
	var scopes []string
	for rows.Next() {
		var scope string
		if err := rows.Scan(&scope); err != nil {
			runtime.t.Fatalf("scan dataset assignment rule: %v", err)
		}
		scopes = append(scopes, scope)
	}
	return scopes
}

// mismatch compares the client state with the expected state. It returns the
// first difference, or an empty string when the complete state is equal.
func (client *datasetClient) mismatch(expected expectedState, assigned []string) string {
	scopes := make([]string, 0, len(client.Cursors))
	for scope := range client.Cursors {
		scopes = append(scopes, scope)
	}
	slices.Sort(scopes)
	if !slices.Equal(scopes, assigned) {
		return fmt.Sprintf("%s scopes %v, want %v", client.User, scopes, assigned)
	}
	for _, scope := range assigned {
		if client.Cursors[scope] == nil {
			return fmt.Sprintf("%s scope %s has no cursor", client.User, scope)
		}
		got := client.Rows[scope]
		want := expected.scopes[scope]
		for key := range want {
			if _, ok := got[key]; !ok {
				return fmt.Sprintf("%s scope %s lacks %s", client.User, scope, key)
			}
		}
		for key, row := range got {
			if !want[key] {
				return fmt.Sprintf("%s scope %s has unexpected %s", client.User, scope, key)
			}
			source := expected.rows[key]
			tableName := key[:strings.Index(key, "/")]
			table, _ := dataset.LookupTable(tableName)
			if len(row) != len(table.Columns) {
				return fmt.Sprintf("%s scope %s row %s has %d fields, want %d", client.User, scope, key, len(row), len(table.Columns))
			}
			for _, column := range table.Columns {
				if err := dataset.CompareWire(column.Type, row[column.Name], source[column.Name]); err != nil {
					return fmt.Sprintf("%s scope %s row %s field %s: %v", client.User, scope, key, column.Name, err)
				}
			}
		}
	}
	return ""
}

// converge pulls until the client equals the expected state or the timeout ends.
func (runtime *datasetRuntime) converge(client *datasetClient, expected expectedState, timeout time.Duration) {
	runtime.t.Helper()
	assigned := runtime.assignedScopes(client.User)
	deadline := time.Now().Add(timeout)
	for {
		runtime.pull(client)
		difference := client.mismatch(expected, assigned)
		if difference == "" {
			return
		}
		if time.Now().After(deadline) {
			runtime.t.Fatalf("dataset client did not converge: %s", difference)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

var errDatasetPush = errors.New("dataset push outcome is invalid")

// pushOutcome is one accepted push outcome.
type pushOutcome struct {
	Status        string                     `json:"status"`
	Code          string                     `json:"code"`
	ServerRow     map[string]json.RawMessage `json:"server_row"`
	ServerVersion string                     `json:"server_version"`
}

// push sends one batch and returns the accepted and rejected outcomes.
func (runtime *datasetRuntime) push(client *datasetClient, mutations []map[string]any) ([]pushOutcome, []pushOutcome, time.Duration) {
	runtime.t.Helper()
	random := dataset.NewRandom(uint64(time.Now().UnixNano()))
	for _, mutation := range mutations {
		mutation["mutation_id"] = random.UUID()
		mutation["authored_schema"] = json.RawMessage(client.Schema)
		mutation["client_version"] = time.Now().UTC().Format("2006-01-02T15:04:05.000000Z")
	}
	started := time.Now()
	status, data, err := runtime.post(client.Token, "/sync/push", map[string]any{
		"client_id": client.ID, "client_generation": client.Generation, "batch_id": random.UUID(),
		"schema": json.RawMessage(client.Schema), "mutations": mutations,
	})
	elapsed := time.Since(started)
	if err != nil || status != http.StatusOK {
		runtime.t.Fatalf("dataset push status=%d err=%v body=%.400s", status, err, data)
	}
	var response struct {
		Accepted []pushOutcome `json:"accepted"`
		Rejected []pushOutcome `json:"rejected"`
	}
	if err := decodeDataset(data, &response); err != nil || len(response.Accepted)+len(response.Rejected) != len(mutations) {
		runtime.t.Fatalf("%v: %v", errDatasetPush, err)
	}
	return response.Accepted, response.Rejected, elapsed
}

// mutation builds one wire mutation from wire column values keyed by name.
func (client *datasetClient) mutation(t *testing.T, tableName, id, op, baseVersion string, columns map[string]string) map[string]any {
	t.Helper()
	table := client.Tables[tableName]
	if table == nil {
		t.Fatalf("dataset manifest lacks %s", tableName)
	}
	wire := make(map[string]json.RawMessage, len(columns))
	for name, value := range columns {
		fieldID, ok := table.FieldIDs[name]
		if !ok || !json.Valid([]byte(value)) {
			t.Fatalf("dataset mutation column %s.%s is invalid", tableName, name)
		}
		wire[fieldID] = json.RawMessage(value)
	}
	mutation := map[string]any{"table": table.ID, "pk": map[string]string{table.PKField: id}, "op": op, "columns": wire}
	if baseVersion != "" {
		mutation["base_version"] = baseVersion
	}
	return mutation
}
