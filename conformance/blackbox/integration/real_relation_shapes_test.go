package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

// Issue #211 and #220 require that every relation shape that registration
// accepts completes capture, pull, push, rebuild, and seed export. The expected
// state is always the committed source table, read with SQL.

const realShapeUserScope = "user:diagnostic-user"

// realShapeRow is one live row as a client holds it. Values are keyed by
// physical column name.
type realShapeRow struct {
	values  map[string]any
	version string
}

type realShapeTable struct {
	name      string
	reference realSchemaTableReference
	names     map[string]string
	excluded  []string
}

// createRealShapeTable creates one source table, its projection view, and a
// membership function that places every live row in scope.
func createRealShapeTable(t *testing.T, ctx context.Context, admin *sql.DB, name, definition, keyType, scope string) {
	t.Helper()
	statements := []string{
		fmt.Sprintf("CREATE TABLE public.%s %s", name, definition),
		fmt.Sprintf("ALTER TABLE public.%s ENABLE ROW LEVEL SECURITY", name),
		fmt.Sprintf("CREATE POLICY synchro_owner_all ON public.%s AS PERMISSIVE FOR ALL TO synchro_owner USING (true) WITH CHECK (true)", name),
		fmt.Sprintf("GRANT SELECT, INSERT, UPDATE ON TABLE public.%s TO synchro_owner", name),
		fmt.Sprintf("GRANT SELECT ON TABLE public.%s TO synchro_worker", name),
		fmt.Sprintf("SELECT synchro.synchro_prepare_projection_view('public.%[1]s', '%[1]s', ARRAY['id'])", name),
		fmt.Sprintf(`CREATE FUNCTION public.%[1]s_membership(p_id %[2]s)
			RETURNS SETOF text
			LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
			BEGIN ATOMIC
				SELECT %[3]s::text
				FROM synchro_projection.%[1]s AS p
				WHERE p.record_id = p_id::text AND NOT p.deleted;
			END`, name, keyType, quoteRealShapeLiteral(scope)),
		fmt.Sprintf("REVOKE ALL ON FUNCTION public.%s_membership(%s) FROM PUBLIC", name, keyType),
		fmt.Sprintf("GRANT EXECUTE ON FUNCTION public.%s_membership(%s) TO synchro_owner, synchro_worker", name, keyType),
	}
	for _, statement := range statements {
		if _, err := admin.ExecContext(ctx, statement); err != nil {
			t.Fatalf("create shape table %s: %v: %s", name, err, statement)
		}
	}
}

func quoteRealShapeLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func registerRealShapeTable(ctx context.Context, admin *sql.DB, name string, excluded ...string) error {
	if excluded == nil {
		excluded = []string{}
	}
	_, err := admin.ExecContext(ctx, `SELECT synchro.synchro_register_table(
		'public.' || $1::text, 'public.' || $1::text || '_membership', 'single_scope',
		'id', 'updated_at', 'deleted_at', 'enabled', $2::text[])`, name, excluded)
	return err
}

// requireRealShapeRegistrationRejected checks that a rejected registration
// leaves the registry generation and the publication unchanged.
func requireRealShapeRegistrationRejected(t *testing.T, ctx context.Context, admin *sql.DB, harness *blackbox.Harness, name, reason string) {
	t.Helper()
	before := latestRegistryGeneration(t, ctx, admin)
	membersBefore := realShapePublicationMembers(t, ctx, admin, harness)
	err := registerRealShapeTable(ctx, admin, name)
	if err == nil || !strings.Contains(err.Error(), reason) {
		t.Fatalf("registration of %s error = %v, want %q", name, err, reason)
	}
	if after := latestRegistryGeneration(t, ctx, admin); after != before {
		t.Fatalf("rejected registration of %s changed the registry generation %d -> %d", name, before, after)
	}
	if membersAfter := realShapePublicationMembers(t, ctx, admin, harness); !reflect.DeepEqual(membersAfter, membersBefore) {
		t.Fatalf("rejected registration of %s changed the publication: %v -> %v", name, membersBefore, membersAfter)
	}
}

func realShapePublicationMembers(t *testing.T, ctx context.Context, admin *sql.DB, harness *blackbox.Harness) []string {
	t.Helper()
	rows, err := admin.QueryContext(ctx, `
		SELECT member.prrelid::regclass::text
		FROM pg_catalog.pg_publication publication
		JOIN pg_catalog.pg_publication_rel member ON member.prpubid = publication.oid
		WHERE publication.pubname = $1
		ORDER BY 1`, harness.Names().Publication)
	if err != nil {
		t.Fatalf("read publication members: %v", err)
	}
	defer rows.Close()
	var members []string
	for rows.Next() {
		var member string
		if err := rows.Scan(&member); err != nil {
			t.Fatalf("scan publication member: %v", err)
		}
		members = append(members, member)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read publication members: %v", err)
	}
	return members
}

func waitForRealShapeTable(t *testing.T, ctx context.Context, harness *blackbox.Harness, name string, excluded ...string) realShapeTable {
	t.Helper()
	var lastErr error
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		reference, err := fetchRealSchemaTableReference(ctx, harness.AdapterURL(), name)
		if err == nil {
			names := make(map[string]string, len(reference.Fields))
			for column, fieldID := range reference.Fields {
				names[fieldID] = column
			}
			for _, column := range excluded {
				if _, present := reference.Fields[column]; present {
					t.Fatalf("shape table %s exposed the excluded %s field", name, column)
				}
			}
			return realShapeTable{name: name, reference: reference, names: names, excluded: excluded}
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("shape table %s did not activate: %v; %s", name, lastErr, harness.FailureDiagnostics())
	return realShapeTable{}
}

// realShapeServerRows reads every live source row in wire form. Text values
// are exact. Timestamps use the protocol UTC microsecond form.
func realShapeServerRows(t *testing.T, ctx context.Context, admin *sql.DB, table realShapeTable) map[string]map[string]any {
	t.Helper()
	return realShapeSourceRows(t, ctx, admin, table, "source.deleted_at IS NULL")
}

// realShapeSourceRows reads the source rows that match the filter, including
// tombstones when the filter selects them.
func realShapeSourceRows(t *testing.T, ctx context.Context, admin *sql.DB, table realShapeTable, filter string) map[string]map[string]any {
	t.Helper()
	excluded := ""
	for _, column := range table.excluded {
		excluded += " - " + quoteRealShapeLiteral(column)
	}
	rows, err := admin.QueryContext(ctx, fmt.Sprintf(`
		SELECT source.id::text,
		       (to_jsonb(source) - 'updated_at' - 'deleted_at'%s) || jsonb_build_object(
		           'updated_at', to_char(timezone('UTC', source.updated_at), 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
		           'deleted_at', to_char(timezone('UTC', source.deleted_at), 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'))
		FROM public.%s AS source
		WHERE %s`, excluded, table.name, filter))
	if err != nil {
		t.Fatalf("read source rows of %s: %v", table.name, err)
	}
	defer rows.Close()
	result := make(map[string]map[string]any)
	for rows.Next() {
		var id string
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
			t.Fatalf("scan source row of %s: %v", table.name, err)
		}
		var values map[string]any
		if err := json.Unmarshal(raw, &values); err != nil {
			t.Fatalf("decode source row of %s: %v", table.name, err)
		}
		result[id] = values
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read source rows of %s: %v", table.name, err)
	}
	return result
}

// realShapeServerVersions reads the authoritative opaque version of each row.
// The extension returns this value as server_version.
func realShapeServerVersions(t *testing.T, ctx context.Context, admin *sql.DB, table realShapeTable, liveOnly bool) map[string]string {
	t.Helper()
	rows, err := admin.QueryContext(ctx, `
		SELECT version.record_id, version.row_version::text
		FROM synchro.sync_row_versions version
		JOIN synchro.sync_registry registry ON registry.relation_id = version.relation_id
		JOIN synchro.sync_registry_generations generation
		  ON generation.generation = registry.registry_generation AND generation.state = 'active'
		WHERE registry.physical_schema = 'public'
		  AND registry.physical_relation = $1
		  AND (NOT $2 OR NOT version.deleted)`, table.name, liveOnly)
	if err != nil {
		t.Fatalf("read row versions of %s: %v", table.name, err)
	}
	defer rows.Close()
	versions := make(map[string]string)
	for rows.Next() {
		var id, version string
		if err := rows.Scan(&id, &version); err != nil {
			t.Fatalf("scan row version of %s: %v", table.name, err)
		}
		versions[id] = version
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read row versions of %s: %v", table.name, err)
	}
	return versions
}

// requireRealShapeSourceValues requires each named source column of one row to
// hold the expected value. It reads the row even when it is a tombstone.
func requireRealShapeSourceValues(t *testing.T, ctx context.Context, admin *sql.DB, table realShapeTable, id string, expected map[string]any) {
	t.Helper()
	row, ok := realShapeSourceRows(t, ctx, admin, table, "source.id::text = "+quoteRealShapeLiteral(id))[id]
	if !ok {
		t.Fatalf("source row %s.%q is missing", table.name, id)
	}
	for column, value := range expected {
		if !reflect.DeepEqual(row[column], value) {
			t.Fatalf("source %s.%q column %s = %#v, want %#v", table.name, id, column, row[column], value)
		}
	}
}

// applyRealShapeRecord applies one pull change, rebuild record, seed record, or
// push outcome row to the client state.
func applyRealShapeRecord(t *testing.T, state map[string]realShapeRow, table realShapeTable, operation string, record map[string]any) {
	t.Helper()
	pk, ok := record["pk"].(map[string]any)
	if !ok {
		t.Fatalf("shape record key is invalid: %#v", record)
	}
	id, ok := pk[table.reference.PKField].(string)
	if !ok {
		t.Fatalf("shape record identity is invalid: %#v", record)
	}
	row, _ := record["row"].(map[string]any)
	if row == nil {
		row, _ = record["server_row"].(map[string]any)
	}
	if operation == "delete" || row == nil || row[table.reference.Fields["deleted_at"]] != nil {
		delete(state, id)
		return
	}
	version, ok := record["server_version"].(string)
	if !ok || version == "" {
		t.Fatalf("shape record server version is invalid: %#v", record)
	}
	values := make(map[string]any, len(row))
	for fieldID, value := range row {
		name, ok := table.names[fieldID]
		if !ok {
			t.Fatalf("shape record of %s has an unknown field %s", table.name, fieldID)
		}
		values[name] = value
	}
	state[id] = realShapeRow{values: values, version: version}
}

func realShapeStateVersions(state map[string]realShapeRow) map[string]string {
	versions := make(map[string]string, len(state))
	for id, row := range state {
		versions[id] = row.version
	}
	return versions
}

func realShapeStateValues(state map[string]realShapeRow) map[string]map[string]any {
	values := make(map[string]map[string]any, len(state))
	for id, row := range state {
		values[id] = row.values
	}
	return values
}

// pullRealShapeUntilServer pulls until the client state of each table equals
// the committed source rows.
func pullRealShapeUntilServer(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
	token string,
	admin *sql.DB,
	client *realProtocolClient,
	scope string,
	tables []realShapeTable,
	states map[string]map[string]realShapeRow,
) {
	t.Helper()
	want := make(map[string]map[string]map[string]any, len(tables))
	byID := make(map[string]realShapeTable, len(tables))
	for _, table := range tables {
		want[table.name] = realShapeServerRows(t, ctx, admin, table)
		byID[table.reference.TableID] = table
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/pull", realPullPayload(client, client.Scopes, 100))
		if errorBody, _ := response["error"].(map[string]any); status == http.StatusServiceUnavailable && errorBody["code"] == "capture_pending" {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if rebuild, _ := response["rebuild"].([]any); status != http.StatusOK || len(rebuild) != 0 {
			t.Fatalf("shape pull status = %d, want 200 without rebuild: %#v", status, response)
		}
		cursors, _ := response["scope_cursors"].(map[string]any)
		for scopeID, cursor := range cursors {
			if _, assigned := client.Scopes[scopeID]; !assigned {
				t.Fatalf("shape pull returned an unassigned scope cursor %s", scopeID)
			}
			client.Scopes[scopeID] = map[string]any{"cursor": cursor}
		}
		for _, change := range requireRealChanges(t, response) {
			tableID, _ := change["table"].(string)
			table, ok := byID[tableID]
			if !ok || change["scope"] != scope {
				continue
			}
			operation, _ := change["op"].(string)
			if operation != "upsert" && operation != "delete" {
				t.Fatalf("shape pull operation is invalid: %#v", change)
			}
			applyRealShapeRecord(t, states[table.name], table, operation, change)
		}
		matched := true
		for _, table := range tables {
			if !reflect.DeepEqual(realShapeStateValues(states[table.name]), want[table.name]) ||
				!reflect.DeepEqual(realShapeStateVersions(states[table.name]), realShapeServerVersions(t, ctx, admin, table, true)) {
				matched = false
			}
		}
		if matched {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, table := range tables {
		got := realShapeStateValues(states[table.name])
		if !reflect.DeepEqual(got, want[table.name]) {
			t.Errorf("client rows of %s differ from the server:\n got: %#v\nwant: %#v", table.name, got, want[table.name])
		}
		if versions, server := realShapeStateVersions(states[table.name]), realShapeServerVersions(t, ctx, admin, table, true); !reflect.DeepEqual(versions, server) {
			t.Errorf("client versions of %s differ from the server:\n got: %#v\nwant: %#v", table.name, versions, server)
		}
	}
	t.Fatalf("pull did not reach the server state; health=%#v; %s", loadIssue49Health(t, ctx, admin), harness.FailureDiagnostics())
}

// rebuildRealShapeRows rebuilds one scope, requires each table to equal the
// committed source rows, and returns the rebuilt client state.
func rebuildRealShapeRows(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
	token string,
	admin *sql.DB,
	client *realProtocolClient,
	scope, rebuildID string,
	tables []realShapeTable,
) map[string]map[string]realShapeRow {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for attempt := 0; ; attempt++ {
		// Capture is asynchronous, so a new rebuild repeats until capture reaches the source.
		attemptID := fmt.Sprintf("%08x%s", attempt, rebuildID[8:])
		records, _ := rebuildRealScope(t, ctx, harness, token, client, scope, attemptID)
		states, matched := realShapeRecordStates(t, ctx, admin, records, tables)
		if matched {
			return states
		}
		if time.Now().After(deadline) {
			t.Logf("rebuild did not reach the server state; health=%#v", loadIssue49Health(t, ctx, admin))
			requireRealShapeRecords(t, ctx, admin, records, tables, "rebuild")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// realShapeRecordStates builds client state from rebuild or seed records and
// reports whether each table equals the committed source rows.
func realShapeRecordStates(t *testing.T, ctx context.Context, admin *sql.DB, records []map[string]any, tables []realShapeTable) (map[string]map[string]realShapeRow, bool) {
	t.Helper()
	states := make(map[string]map[string]realShapeRow, len(tables))
	matched := true
	for _, table := range tables {
		states[table.name] = make(map[string]realShapeRow)
		for _, record := range records {
			if record["table"] == table.reference.TableID {
				applyRealShapeRecord(t, states[table.name], table, "upsert", record)
			}
		}
		if !reflect.DeepEqual(realShapeStateValues(states[table.name]), realShapeServerRows(t, ctx, admin, table)) ||
			!reflect.DeepEqual(realShapeStateVersions(states[table.name]), realShapeServerVersions(t, ctx, admin, table, true)) {
			matched = false
		}
	}
	return states, matched
}

func requireRealShapeRecords(t *testing.T, ctx context.Context, admin *sql.DB, records []map[string]any, tables []realShapeTable, source string) map[string]map[string]realShapeRow {
	t.Helper()
	states, matched := realShapeRecordStates(t, ctx, admin, records, tables)
	if !matched {
		for _, table := range tables {
			t.Errorf("%s rows of %s:\n got: %#v\nwant: %#v", source, table.name, realShapeStateValues(states[table.name]), realShapeServerRows(t, ctx, admin, table))
			t.Errorf("%s versions of %s:\n got: %#v\nwant: %#v", source, table.name, realShapeStateVersions(states[table.name]), realShapeServerVersions(t, ctx, admin, table, true))
		}
		t.Fatalf("%s rows differ from the server", source)
	}
	return states
}

func realShapeMutation(client *realProtocolClient, table realShapeTable, operation, mutationID, id string, base string, columns map[string]any) map[string]any {
	mutation := map[string]any{
		"mutation_id":     mutationID,
		"table":           table.reference.TableID,
		"pk":              map[string]any{table.reference.PKField: id},
		"authored_schema": client.Schema,
		"op":              operation,
		"client_version":  phase4ClientVersion,
	}
	if base != "" {
		mutation["base_version"] = base
	}
	if columns != nil {
		authored := make(map[string]any, len(columns))
		for name, value := range columns {
			authored[table.reference.Fields[name]] = value
		}
		mutation["columns"] = authored
	}
	return mutation
}

// pushRealShapeApplied pushes one batch, requires every mutation to apply, and
// requires each returned row to equal the committed source row.
func pushRealShapeApplied(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
	token string,
	admin *sql.DB,
	client *realProtocolClient,
	batchID string,
	mutations []map[string]any,
	tables map[string]realShapeTable,
	states map[string]map[string]realShapeRow,
) map[string]any {
	t.Helper()
	status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(client, batchID, mutations))
	if status != http.StatusOK {
		t.Fatalf("shape push status = %d, want 200: %#v", status, response)
	}
	accepted := requireOutcomeList(t, response, "accepted")
	if len(accepted) != len(mutations) || len(requireOutcomeList(t, response, "rejected")) != 0 {
		t.Fatalf("shape push did not apply every mutation: %#v", response)
	}
	for index, outcome := range accepted {
		mutation := mutations[index]
		if outcome["mutation_id"] != mutation["mutation_id"] || outcome["status"] != "applied" {
			t.Fatalf("shape push outcome is invalid: %#v", outcome)
		}
		var table realShapeTable
		for _, candidate := range tables {
			if candidate.reference.TableID == mutation["table"] {
				table = candidate
			}
		}
		operation := mutation["op"].(string)
		applyRealShapeRecord(t, states[table.name], table, operation, outcome)
		id := mutation["pk"].(map[string]any)[table.reference.PKField].(string)

		// The authored mutation is the oracle for the push effect.
		switch operation {
		case "insert", "update":
			authored := make(map[string]any)
			for fieldID, value := range mutation["columns"].(map[string]any) {
				authored[table.names[fieldID]] = value
			}
			authored["deleted_at"] = nil
			requireRealShapeSourceValues(t, ctx, admin, table, id, authored)
		case "delete":
			if deleted := realShapeSourceRows(t, ctx, admin, table, "source.deleted_at IS NOT NULL")[id]; deleted == nil {
				t.Fatalf("accepted delete of %s.%q left no source tombstone", table.name, id)
			}
		}

		// The source is the oracle for complete hydration of the returned row.
		server := realShapeServerRows(t, ctx, admin, table)
		row, live := states[table.name][id]
		want, serverLive := server[id]
		if live != serverLive || live && !reflect.DeepEqual(row.values, want) {
			t.Fatalf("shape push outcome row of %s.%q differs from the server:\n got: %#v\nwant: %#v", table.name, id, row.values, want)
		}

		// The returned version is the new authoritative version and replaces the base.
		version, _ := outcome["server_version"].(string)
		if current := realShapeServerVersions(t, ctx, admin, table, false)[id]; version == "" || version != current {
			t.Fatalf("shape push outcome version of %s.%q = %q, want the current version %q", table.name, id, version, current)
		}
		if base, _ := mutation["base_version"].(string); base != "" && base == version {
			t.Fatalf("shape push outcome version of %s.%q did not change from its base %q", table.name, id, base)
		}
	}
	return response
}

func realShapeTables(tables ...realShapeTable) map[string]realShapeTable {
	byName := make(map[string]realShapeTable, len(tables))
	for _, table := range tables {
		byName[table.name] = table
	}
	return byName
}

func execRealShape(t *testing.T, ctx context.Context, admin *sql.DB, statement string, arguments ...any) {
	t.Helper()
	if _, err := admin.ExecContext(ctx, statement, arguments...); err != nil {
		t.Fatalf("write source rows: %v: %s", err, statement)
	}
}

// exportRealShapeSeed exports one portable scope page and requires each table
// to equal the committed source rows. It returns the seed state and receipt.
func exportRealShapeSeed(t *testing.T, ctx context.Context, admin *sql.DB, scope string, tables []realShapeTable) (map[string]map[string]realShapeRow, string) {
	t.Helper()
	connection, err := admin.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire seed export connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(ctx, "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE"); err != nil {
		t.Fatalf("begin seed export: %v", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = connection.ExecContext(context.Background(), "ROLLBACK")
		}
	}()
	manifest := issue49QueryJSONObject(t, ctx, connection, "SELECT synchro.synchro_portable_seed_manifest(100)")
	scopes, _ := manifest["portable_scopes"].([]any)
	var pageToken, receipt string
	for _, raw := range scopes {
		entry, _ := raw.(map[string]any)
		if entry["id"] == scope {
			pageToken, _ = entry["page_token"].(string)
			receipt, _ = entry["continuation"].(string)
		}
	}
	if pageToken == "" || receipt == "" {
		t.Fatalf("seed manifest omitted scope %s: %#v", scope, manifest)
	}
	page := issue49QueryJSONObject(t, ctx, connection, "SELECT synchro.synchro_portable_seed_scope($1, $2, $3, $4, $5)", scope, pageToken, receipt, int64(0), 100)
	rawRecords, ok := page["records"].([]any)
	if !ok || page["has_more"] != false {
		t.Fatalf("seed page is invalid: %#v", page)
	}
	records := make([]map[string]any, 0, len(rawRecords))
	for _, raw := range rawRecords {
		record, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("seed record is invalid: %#v", raw)
		}
		records = append(records, record)
	}
	states := requireRealShapeRecords(t, ctx, admin, records, tables, "seed")
	if _, err := connection.ExecContext(ctx, "COMMIT"); err != nil {
		t.Fatalf("commit seed export: %v", err)
	}
	committed = true
	return states, receipt
}

// connectRealSeededShapeClient connects a fresh client with a seed receipt and
// requires the receipt to become the client cursor for the seeded scope.
func connectRealSeededShapeClient(t *testing.T, ctx context.Context, harness *blackbox.Harness, token, clientID, scope, receipt string) *realProtocolClient {
	t.Helper()
	status, response := postConnect(t, ctx, harness.AdapterURL(), token, map[string]any{
		"client_id":         clientID,
		"platform":          "conformance",
		"app_version":       "0.3.0",
		"protocol_version":  3,
		"schema":            map[string]any{"version": 0, "hash": ""},
		"scope_set_version": 0,
		"known_scopes":      map[string]any{},
		"seed_receipts":     map[string]any{scope: receipt},
	})
	if status != http.StatusOK {
		t.Fatalf("seeded connect status = %d, want 200: %#v", status, response)
	}
	if cursor := issue49AddedScopeCursor(response, scope); cursor == "" {
		t.Fatalf("seed receipt did not become a client cursor: %#v", response)
	}
	return parseRealProtocolClient(t, response, clientID)
}

// rebuildRealShapeOtherScope gives the other assigned scope a cursor, so a
// pull of every assigned scope requests no rebuild.
func rebuildRealShapeOtherScope(t *testing.T, ctx context.Context, harness *blackbox.Harness, token string, client *realProtocolClient, scope, rebuildID string) {
	t.Helper()
	other := "cf:global"
	if scope == other {
		other = realShapeUserScope
	}
	rebuildRealScope(t, ctx, harness, token, client, other, rebuildID)
}

// TestRealEmptyTextKeyCompletesSync proves that an empty text primary key
// completes WAL capture, pull, push, rebuild, and seed continuation.
func TestRealEmptyTextKeyCompletesSync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	waitForIssue49CanonicalHealth(t, ctx, admin, true)
	createRealShapeTable(t, ctx, admin, "rs_empty_keys", `(
		id text PRIMARY KEY,
		value text NOT NULL,
		updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
		deleted_at timestamptz)`, "text", "cf:global")
	if err := registerRealShapeTable(ctx, admin, "rs_empty_keys"); err != nil {
		t.Fatalf("register empty-key table: %v", err)
	}
	table := waitForRealShapeTable(t, ctx, harness, "rs_empty_keys")
	tables := []realShapeTable{table}
	waitForIssue49CanonicalHealth(t, ctx, admin, true)

	execRealShape(t, ctx, admin, "INSERT INTO public.rs_empty_keys (id, value) VALUES ('', 'empty-alpha'), ('plain', 'plain-alpha')")
	execRealShape(t, ctx, admin, "UPDATE public.rs_empty_keys SET value = 'empty-beta' WHERE id = ''")
	execRealShape(t, ctx, admin, "DELETE FROM public.rs_empty_keys WHERE id = 'plain'")
	client := connectRealProtocolClient(t, ctx, harness, token, "empty-key-client")
	rebuildRealShapeOtherScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8211-00000000e000")
	states := rebuildRealShapeRows(t, ctx, harness, token, admin, client, "cf:global", "00000000-0000-4000-8211-00000000c000", tables)
	if _, present := states["rs_empty_keys"][""]; !present {
		t.Fatalf("rebuild omitted the empty key: %#v", states)
	}
	execRealShape(t, ctx, admin, "UPDATE public.rs_empty_keys SET value = 'empty-gamma' WHERE id = ''")
	pullRealShapeUntilServer(t, ctx, harness, token, admin, client, "cf:global", tables, states)

	pushRealShapeApplied(t, ctx, harness, token, admin, client, "00000000-0000-4000-8211-00000000b001", []map[string]any{
		realShapeMutation(client, table, "update", "00000000-0000-4000-8211-000000000001", "", states["rs_empty_keys"][""].version, map[string]any{"value": "empty-pushed"}),
	}, realShapeTables(tables...), states)
	pullRealShapeUntilServer(t, ctx, harness, token, admin, client, "cf:global", tables, states)
	rebuildRealShapeRows(t, ctx, harness, token, admin, client, "cf:global", "00000000-0000-4000-8211-00000000c001", tables)

	seedStates, receipt := exportRealShapeSeed(t, ctx, admin, "cf:global", tables)
	seeded := connectRealSeededShapeClient(t, ctx, harness, token, "empty-key-seeded-client", "cf:global", receipt)
	rebuildRealShapeOtherScope(t, ctx, harness, token, seeded, "cf:global", "00000000-0000-4000-8211-00000000e001")
	execRealShape(t, ctx, admin, "UPDATE public.rs_empty_keys SET value = 'empty-after-seed' WHERE id = ''")
	pullRealShapeUntilServer(t, ctx, harness, token, admin, seeded, "cf:global", tables, seedStates)

	pushRealShapeApplied(t, ctx, harness, token, admin, seeded, "00000000-0000-4000-8211-00000000b002", []map[string]any{
		realShapeMutation(seeded, table, "delete", "00000000-0000-4000-8211-000000000002", "", seedStates["rs_empty_keys"][""].version, nil),
	}, realShapeTables(tables...), seedStates)
	pullRealShapeUntilServer(t, ctx, harness, token, admin, client, "cf:global", tables, states)
	if _, present := states["rs_empty_keys"][""]; present {
		t.Fatalf("pull kept the deleted empty key: %#v", states)
	}
}

// TestRealIncludePrimaryKeyCompletesSync proves that a scalar primary key with
// INCLUDE columns completes sync and seed continuation, and that a composite
// key stays rejected. The table uses the portable scope, so a seed can export it.
func TestRealIncludePrimaryKeyCompletesSync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	waitForIssue49CanonicalHealth(t, ctx, admin, true)
	createRealShapeTable(t, ctx, admin, "rs_include_keys", `(
		id text,
		value text NOT NULL,
		note text,
		updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
		deleted_at timestamptz,
		PRIMARY KEY (id) INCLUDE (value, note))`, "text", "cf:global")
	createRealShapeTable(t, ctx, admin, "rs_composite_keys", `(
		id text,
		part text,
		value text NOT NULL,
		updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
		deleted_at timestamptz,
		PRIMARY KEY (id, part))`, "text", realShapeUserScope)
	requireRealShapeRegistrationRejected(t, ctx, admin, harness, "rs_composite_keys", "exactly one column")
	if err := registerRealShapeTable(ctx, admin, "rs_include_keys"); err != nil {
		t.Fatalf("register INCLUDE key table: %v", err)
	}
	table := waitForRealShapeTable(t, ctx, harness, "rs_include_keys")
	tables := []realShapeTable{table}
	waitForIssue49CanonicalHealth(t, ctx, admin, true)

	client := connectRealProtocolClient(t, ctx, harness, token, "include-key-client")
	rebuildRealShapeOtherScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8211-00000000e010")
	states := rebuildRealShapeRows(t, ctx, harness, token, admin, client, "cf:global", "00000000-0000-4000-8211-00000000c010", tables)
	execRealShape(t, ctx, admin, "INSERT INTO public.rs_include_keys (id, value, note) VALUES ('source-a', 'alpha', NULL), ('source-b', 'bravo', 'note-b')")
	execRealShape(t, ctx, admin, "UPDATE public.rs_include_keys SET value = 'alpha-updated', note = 'note-a' WHERE id = 'source-a'")
	execRealShape(t, ctx, admin, "DELETE FROM public.rs_include_keys WHERE id = 'source-b'")
	pullRealShapeUntilServer(t, ctx, harness, token, admin, client, "cf:global", tables, states)

	pushRealShapeApplied(t, ctx, harness, token, admin, client, "00000000-0000-4000-8211-00000000b011", []map[string]any{
		realShapeMutation(client, table, "insert", "00000000-0000-4000-8211-000000000011", "pushed-c", "", map[string]any{"value": "charlie", "note": nil}),
		realShapeMutation(client, table, "update", "00000000-0000-4000-8211-000000000012", "source-a", states["rs_include_keys"]["source-a"].version, map[string]any{"note": nil}),
	}, realShapeTables(tables...), states)
	pushRealShapeApplied(t, ctx, harness, token, admin, client, "00000000-0000-4000-8211-00000000b012", []map[string]any{
		realShapeMutation(client, table, "delete", "00000000-0000-4000-8211-000000000013", "pushed-c", states["rs_include_keys"]["pushed-c"].version, nil),
	}, realShapeTables(tables...), states)
	pullRealShapeUntilServer(t, ctx, harness, token, admin, client, "cf:global", tables, states)
	rebuildRealShapeRows(t, ctx, harness, token, admin, client, "cf:global", "00000000-0000-4000-8211-00000000c011", tables)

	seedStates, receipt := exportRealShapeSeed(t, ctx, admin, "cf:global", tables)
	seeded := connectRealSeededShapeClient(t, ctx, harness, token, "include-key-seeded-client", "cf:global", receipt)
	rebuildRealShapeOtherScope(t, ctx, harness, token, seeded, "cf:global", "00000000-0000-4000-8211-00000000e011")
	execRealShape(t, ctx, admin, "UPDATE public.rs_include_keys SET value = 'alpha-after-seed' WHERE id = 'source-a'")
	pullRealShapeUntilServer(t, ctx, harness, token, admin, seeded, "cf:global", tables, seedStates)

	pushRealShapeApplied(t, ctx, harness, token, admin, seeded, "00000000-0000-4000-8211-00000000b013", []map[string]any{
		realShapeMutation(seeded, table, "delete", "00000000-0000-4000-8211-000000000014", "source-a", seedStates["rs_include_keys"]["source-a"].version, nil),
	}, realShapeTables(tables...), seedStates)
	pullRealShapeUntilServer(t, ctx, harness, token, admin, client, "cf:global", tables, states)
	if _, present := states["rs_include_keys"]["source-a"]; present {
		t.Fatalf("pull kept the deleted INCLUDE key row: %#v", states)
	}
}

// realWideDefinition returns a table with the key, the two lifecycle fields,
// and columns c01 through cNN. The fields total columns plus three.
func realWideDefinition(columns int, extra string) string {
	parts := []string{"id text PRIMARY KEY"}
	for index := 1; index <= columns; index++ {
		parts = append(parts, fmt.Sprintf("c%02d text", index))
	}
	parts = append(parts, "updated_at timestamptz NOT NULL DEFAULT clock_timestamp()", "deleted_at timestamptz")
	if extra != "" {
		parts = append(parts, extra)
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// realWideValues gives every column a distinct value. Every seventh column is
// SQL NULL, so null values cross every path.
func realWideValues(columns int, prefix string) map[string]any {
	values := make(map[string]any, columns)
	for index := 1; index <= columns; index++ {
		name := fmt.Sprintf("c%02d", index)
		if index%7 == 0 {
			values[name] = nil
			continue
		}
		values[name] = fmt.Sprintf("%s-%s", prefix, name)
	}
	return values
}

func realWideInsert(table string, id string, values map[string]any) (string, []any) {
	columns := []string{"id"}
	placeholders := []string{"$1"}
	arguments := []any{id}
	for index := 1; index <= len(values); index++ {
		name := fmt.Sprintf("c%02d", index)
		columns = append(columns, name)
		arguments = append(arguments, values[name])
		placeholders = append(placeholders, fmt.Sprintf("$%d", len(arguments)))
	}
	return fmt.Sprintf("INSERT INTO public.%s (%s) VALUES (%s)", table, strings.Join(columns, ", "), strings.Join(placeholders, ", ")), arguments
}

// TestRealWideProjectionCompletesSync proves that 50-field and 51-field tables
// keep every value, including SQL null, through capture, push, rebuild, and
// seed export, and that an excluded column never appears.
func TestRealWideProjectionCompletesSync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	waitForIssue49CanonicalHealth(t, ctx, admin, true)
	widths := map[string]int{"rs_wide_50": 47, "rs_wide_51": 48}
	createRealShapeTable(t, ctx, admin, "rs_wide_50", realWideDefinition(47, ""), "text", "cf:global")
	createRealShapeTable(t, ctx, admin, "rs_wide_51", realWideDefinition(48, "secret text NOT NULL DEFAULT 'hidden-secret'"), "text", "cf:global")
	if err := registerRealShapeTable(ctx, admin, "rs_wide_50"); err != nil {
		t.Fatalf("register 50-field table: %v", err)
	}
	if err := registerRealShapeTable(ctx, admin, "rs_wide_51", "secret"); err != nil {
		t.Fatalf("register 51-field table: %v", err)
	}
	tables := []realShapeTable{
		waitForRealShapeTable(t, ctx, harness, "rs_wide_50"),
		waitForRealShapeTable(t, ctx, harness, "rs_wide_51", "secret"),
	}
	for _, table := range tables {
		if got, want := len(table.reference.Fields), widths[table.name]+3; got != want {
			t.Fatalf("%s field count = %d, want %d", table.name, got, want)
		}
	}
	waitForIssue49CanonicalHealth(t, ctx, admin, true)

	client := connectRealProtocolClient(t, ctx, harness, token, "wide-projection-client")
	rebuildRealShapeOtherScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8211-00000000e020")
	states := rebuildRealShapeRows(t, ctx, harness, token, admin, client, "cf:global", "00000000-0000-4000-8211-00000000c020", tables)
	for _, table := range tables {
		statement, arguments := realWideInsert(table.name, "source-row", realWideValues(widths[table.name], "source"))
		execRealShape(t, ctx, admin, statement, arguments...)
		execRealShape(t, ctx, admin, fmt.Sprintf("UPDATE public.%s SET c01 = NULL, c07 = 'source-c07-set' WHERE id = 'source-row'", table.name))
	}
	pullRealShapeUntilServer(t, ctx, harness, token, admin, client, "cf:global", tables, states)

	for index, table := range tables {
		batch := fmt.Sprintf("00000000-0000-4000-8211-00000000b02%d", index)
		pushRealShapeApplied(t, ctx, harness, token, admin, client, batch, []map[string]any{
			realShapeMutation(client, table, "insert", fmt.Sprintf("00000000-0000-4000-8211-00000000002%d", index), "pushed-row", "", realWideValues(widths[table.name], "pushed")),
			realShapeMutation(client, table, "update", fmt.Sprintf("00000000-0000-4000-8211-00000000003%d", index), "source-row", states[table.name]["source-row"].version, map[string]any{"c02": nil, "c14": "pushed-c14"}),
		}, realShapeTables(tables...), states)
	}
	pullRealShapeUntilServer(t, ctx, harness, token, admin, client, "cf:global", tables, states)
	rebuildRealShapeRows(t, ctx, harness, token, admin, client, "cf:global", "00000000-0000-4000-8211-00000000c021", tables)
	exportRealShapeSeed(t, ctx, admin, "cf:global", tables)
}

// TestRealPartitionedTableCompletesSync proves the D-02 root identity rule. A
// default publication rejects the root without state change. After the
// explicit transition, root writes to both leaves complete sync across a
// PostgreSQL restart, and a registered leaf under a published root is rejected.
func TestRealPartitionedTableCompletesSync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	waitForIssue49CanonicalHealth(t, ctx, admin, true)
	createRealShapeTable(t, ctx, admin, "rs_parts", `(
		id text PRIMARY KEY,
		value text NOT NULL,
		updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
		deleted_at timestamptz) PARTITION BY RANGE (id)`, "text", realShapeUserScope)
	execRealShape(t, ctx, admin, "CREATE TABLE public.rs_parts_low PARTITION OF public.rs_parts FOR VALUES FROM (MINVALUE) TO ('m')")
	execRealShape(t, ctx, admin, "CREATE TABLE public.rs_parts_high PARTITION OF public.rs_parts FOR VALUES FROM ('m') TO (MAXVALUE)")
	createRealShapeTable(t, ctx, admin, "rs_parts_leaf_root", `(
		id text PRIMARY KEY,
		value text NOT NULL,
		updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
		deleted_at timestamptz) PARTITION BY RANGE (id)`, "text", realShapeUserScope)
	createRealShapeTable(t, ctx, admin, "rs_parts_leaf", `PARTITION OF public.rs_parts_leaf_root FOR VALUES FROM (MINVALUE) TO (MAXVALUE)`, "text", realShapeUserScope)

	requireRealShapeRegistrationRejected(t, ctx, admin, harness, "rs_parts", "publish_via_partition_root")
	execRealShape(t, ctx, admin, fmt.Sprintf("ALTER PUBLICATION %q SET (publish_via_partition_root = true)", harness.Names().Publication))
	if err := registerRealShapeTable(ctx, admin, "rs_parts"); err != nil {
		t.Fatalf("register partitioned table after the explicit transition: %v", err)
	}
	// A shared publication can publish a partitioned table that Synchro does not register.
	publication := fmt.Sprintf("%q", harness.Names().Publication)
	execRealShape(t, ctx, admin, "ALTER PUBLICATION "+publication+" ADD TABLE public.rs_parts_leaf_root")
	requireRealShapeRegistrationRejected(t, ctx, admin, harness, "rs_parts_leaf", "not published under its own identity")
	execRealShape(t, ctx, admin, "ALTER PUBLICATION "+publication+" DROP TABLE public.rs_parts_leaf_root")
	table := waitForRealShapeTable(t, ctx, harness, "rs_parts")
	tables := []realShapeTable{table}
	waitForIssue49CanonicalHealth(t, ctx, admin, true)

	client := connectRealProtocolClient(t, ctx, harness, token, "partition-client")
	rebuildRealShapeOtherScope(t, ctx, harness, token, client, realShapeUserScope, "00000000-0000-4000-8211-00000000e030")
	states := rebuildRealShapeRows(t, ctx, harness, token, admin, client, realShapeUserScope, "00000000-0000-4000-8211-00000000c030", tables)
	execRealShape(t, ctx, admin, "INSERT INTO public.rs_parts (id, value) VALUES ('alpha', 'alpha-low'), ('zulu', 'zulu-high'), ('bravo', 'bravo-low')")
	execRealShape(t, ctx, admin, "UPDATE public.rs_parts SET value = value || '-updated' WHERE id IN ('alpha', 'zulu')")
	execRealShape(t, ctx, admin, "DELETE FROM public.rs_parts WHERE id = 'bravo'")
	pullRealShapeUntilServer(t, ctx, harness, token, admin, client, realShapeUserScope, tables, states)

	pushRealShapeApplied(t, ctx, harness, token, admin, client, "00000000-0000-4000-8211-00000000b031", []map[string]any{
		realShapeMutation(client, table, "insert", "00000000-0000-4000-8211-000000000031", "charlie", "", map[string]any{"value": "charlie-low"}),
		realShapeMutation(client, table, "insert", "00000000-0000-4000-8211-000000000032", "yankee", "", map[string]any{"value": "yankee-high"}),
		realShapeMutation(client, table, "update", "00000000-0000-4000-8211-000000000033", "zulu", states["rs_parts"]["zulu"].version, map[string]any{"value": "zulu-pushed"}),
		realShapeMutation(client, table, "delete", "00000000-0000-4000-8211-000000000034", "alpha", states["rs_parts"]["alpha"].version, nil),
	}, realShapeTables(tables...), states)
	pullRealShapeUntilServer(t, ctx, harness, token, admin, client, realShapeUserScope, tables, states)

	if err := harness.RestartPostgres(ctx); err != nil {
		t.Fatalf("restart PostgreSQL with a partitioned table: %v", err)
	}
	waitForRealGeneratedReadyAfterRestart(t, ctx, harness)
	waitForIssue49PublicReady(t, ctx, harness.AdapterURL(), true)
	witnessID := "00000000-0000-4000-8211-00000000d031"
	if err := harness.Source().ExecContext(ctx, "INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'partition-witness')", witnessID); err != nil {
		t.Fatalf("write witness row: %v", err)
	}
	execRealShape(t, ctx, admin, "INSERT INTO public.rs_parts (id, value) VALUES ('delta', 'delta-low'), ('xray', 'xray-high')")
	execRealShape(t, ctx, admin, "UPDATE public.rs_parts SET value = 'yankee-after-restart' WHERE id = 'yankee'")
	waitForRealWALRecords(t, ctx, harness, "cf_items", witnessID)
	pullRealShapeUntilServer(t, ctx, harness, token, admin, client, realShapeUserScope, tables, states)
	rebuildRealShapeRows(t, ctx, harness, token, admin, client, realShapeUserScope, "00000000-0000-4000-8211-00000000c031", tables)
	requireRealGeneratedCaptureHealthy(t, ctx, harness)
}

// TestRealKeyOnlyInsertCompletesSync proves D-01. An insert with empty columns
// creates a key-only row and a default-only row with exact server state and
// exact replay. An update with empty columns is a whole-request 400 that does
// no durable work.
func TestRealKeyOnlyInsertCompletesSync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	waitForIssue49CanonicalHealth(t, ctx, admin, true)
	createRealShapeTable(t, ctx, admin, "rs_key_only", `(
		id text PRIMARY KEY,
		updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
		deleted_at timestamptz)`, "text", realShapeUserScope)
	createRealShapeTable(t, ctx, admin, "rs_default_only", `(
		id text PRIMARY KEY,
		label text NOT NULL DEFAULT 'server-default',
		updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
		deleted_at timestamptz)`, "text", realShapeUserScope)
	for _, name := range []string{"rs_key_only", "rs_default_only"} {
		if err := registerRealShapeTable(ctx, admin, name); err != nil {
			t.Fatalf("register %s: %v", name, err)
		}
	}
	keyOnly := waitForRealShapeTable(t, ctx, harness, "rs_key_only")
	defaultOnly := waitForRealShapeTable(t, ctx, harness, "rs_default_only")
	tables := []realShapeTable{keyOnly, defaultOnly}
	waitForIssue49CanonicalHealth(t, ctx, admin, true)

	client := connectRealProtocolClient(t, ctx, harness, token, "key-only-client")
	rebuildRealShapeOtherScope(t, ctx, harness, token, client, realShapeUserScope, "00000000-0000-4000-8220-00000000e000")
	states := rebuildRealShapeRows(t, ctx, harness, token, admin, client, realShapeUserScope, "00000000-0000-4000-8220-00000000c000", tables)
	inserts := []map[string]any{
		realShapeMutation(client, keyOnly, "insert", "00000000-0000-4000-8220-000000000001", "key-one", "", map[string]any{}),
		realShapeMutation(client, defaultOnly, "insert", "00000000-0000-4000-8220-000000000002", "default-one", "", map[string]any{}),
	}
	first := pushRealShapeApplied(t, ctx, harness, token, admin, client, "00000000-0000-4000-8220-00000000b001", inserts, realShapeTables(tables...), states)
	requireRealShapeSourceValues(t, ctx, admin, defaultOnly, "default-one", map[string]any{"label": "server-default"})
	if label := states["rs_default_only"]["default-one"].values["label"]; label != "server-default" {
		t.Fatalf("default-only insert label = %#v, want the server default", label)
	}
	status, replay := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(client, "00000000-0000-4000-8220-00000000b001", inserts))
	if status != http.StatusOK || !reflect.DeepEqual(replay, first) {
		t.Fatalf("key-only insert replay differs: status=%d\n got: %#v\nwant: %#v", status, replay, first)
	}
	pullRealShapeUntilServer(t, ctx, harness, token, admin, client, realShapeUserScope, tables, states)

	var batchesBefore int
	if err := admin.QueryRowContext(ctx, "SELECT count(*) FROM synchro.sync_push_batches").Scan(&batchesBefore); err != nil {
		t.Fatalf("count push batches: %v", err)
	}
	status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(client, "00000000-0000-4000-8220-00000000b002", []map[string]any{
		realShapeMutation(client, defaultOnly, "update", "00000000-0000-4000-8220-000000000003", "default-one", states["rs_default_only"]["default-one"].version, map[string]any{}),
	}))
	if errorBody, _ := response["error"].(map[string]any); status != http.StatusBadRequest || errorBody["code"] != "invalid_request" {
		t.Fatalf("empty update status = %d response = %#v, want 400 invalid_request", status, response)
	}
	var batchesAfter int
	if err := admin.QueryRowContext(ctx, "SELECT count(*) FROM synchro.sync_push_batches").Scan(&batchesAfter); err != nil {
		t.Fatalf("count push batches: %v", err)
	}
	if batchesAfter != batchesBefore {
		t.Fatalf("empty update stored a push batch: %d -> %d", batchesBefore, batchesAfter)
	}

	pushRealShapeApplied(t, ctx, harness, token, admin, client, "00000000-0000-4000-8220-00000000b003", []map[string]any{
		realShapeMutation(client, keyOnly, "delete", "00000000-0000-4000-8220-000000000004", "key-one", states["rs_key_only"]["key-one"].version, nil),
		realShapeMutation(client, defaultOnly, "delete", "00000000-0000-4000-8220-000000000005", "default-one", states["rs_default_only"]["default-one"].version, nil),
	}, realShapeTables(tables...), states)
	pullRealShapeUntilServer(t, ctx, harness, token, admin, client, realShapeUserScope, tables, states)
	rebuildRealShapeRows(t, ctx, harness, token, admin, client, realShapeUserScope, "00000000-0000-4000-8220-00000000c001", tables)
}

// TestRealKeyOnlyInsertOnPredecessorServer proves the D-01 cross-version rule
// with the published predecessor extension. The adapter only forwards push to
// synchro_push, and the harness starts no adapter before the update, so the
// test calls synchro_push directly as the adapter does. The predecessor
// rejects the empty-column insert as invalid_request with no durable work. A
// control insert that authors a field applies on the same server. After the
// update, the unchanged request applies.
func TestRealKeyOnlyInsertOnPredecessorServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	if !*provision || !*install {
		t.Fatal("real proof requires --provision --install")
	}
	environment, err := blackbox.LoadEnvironment()
	if err != nil {
		t.Fatalf("load predecessor environment: %v", err)
	}
	baselineArtifact := os.Getenv("SYNCHRO_CONFORMANCE_UPDATE_BASELINE_EXTENSION_ARTIFACT")
	if baselineArtifact == "" {
		t.Fatal("extension update baseline artifact is unavailable")
	}
	harness, err := blackbox.Provision(ctx, blackbox.HarnessConfig{
		Environment:                     environment,
		UpdateBaselineExtensionArtifact: baselineArtifact,
		UpdateBaselineExtensionVersion:  readUpdateBaselineVersion(t),
	})
	if err != nil {
		t.Fatalf("provision predecessor harness: %v", err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := harness.Close(closeContext); err != nil {
			t.Errorf("close predecessor harness: %v", err)
		}
	})
	token, err := harness.DiagnosticBearerToken(time.Now())
	if err != nil {
		t.Fatalf("sign predecessor token: %v", err)
	}
	admin := openIssue49Admin(t, ctx, harness)
	createRealShapeTable(t, ctx, admin, "rs_default_only_compat", `(
		id text PRIMARY KEY,
		label text NOT NULL DEFAULT 'server-default',
		updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
		deleted_at timestamptz)`, "text", realShapeUserScope)
	if err := registerRealShapeTable(ctx, admin, "rs_default_only_compat"); err != nil {
		t.Fatalf("register predecessor table: %v", err)
	}
	var reference realSchemaTableReference
	deadline := time.Now().Add(30 * time.Second)
	for {
		var body []byte
		if err := admin.QueryRowContext(ctx, "SELECT synchro.synchro_schema_manifest()").Scan(&body); err == nil {
			if reference, err = parseRealSchemaTableReference(body, "rs_default_only_compat"); err == nil {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("predecessor table did not activate; %s", harness.FailureDiagnostics())
		}
		time.Sleep(50 * time.Millisecond)
	}
	names := make(map[string]string, len(reference.Fields))
	for column, fieldID := range reference.Fields {
		names[fieldID] = column
	}
	table := realShapeTable{name: "rs_default_only_compat", reference: reference, names: names}
	connectRequest, err := json.Marshal(map[string]any{
		"client_id":         "predecessor-client",
		"platform":          "conformance",
		"app_version":       "0.3.0",
		"protocol_version":  3,
		"schema":            map[string]any{"version": 0, "hash": ""},
		"scope_set_version": 0,
		"known_scopes":      map[string]any{},
	})
	if err != nil {
		t.Fatalf("encode predecessor connect: %v", err)
	}
	client := parseRealProtocolClient(t, issue49QueryJSONObject(t, ctx, admin, "SELECT synchro.synchro_connect('diagnostic-user', $1::jsonb)", string(connectRequest)), "predecessor-client")
	predecessorPush := func(payload map[string]any) map[string]any {
		t.Helper()
		encoded, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("encode predecessor push: %v", err)
		}
		var raw []byte
		if err := admin.QueryRowContext(ctx, "SELECT synchro.synchro_push('diagnostic-user', $1::jsonb)::text", string(encoded)).Scan(&raw); err != nil {
			t.Fatalf("predecessor push: %v", err)
		}
		var response map[string]any
		if err := json.Unmarshal(raw, &response); err != nil {
			t.Fatalf("decode predecessor push: %v", err)
		}
		return response
	}
	keyOnly := phase4PushPayload(client, "00000000-0000-4000-8220-00000000b101", []map[string]any{
		realShapeMutation(client, table, "insert", "00000000-0000-4000-8220-000000000101", "key-only", "", map[string]any{}),
	})
	rejected := predecessorPush(keyOnly)
	issue49RequireJSONProtocolError(t, rejected, "invalid_request")
	var batches, rows int
	if err := admin.QueryRowContext(ctx, "SELECT (SELECT count(*) FROM synchro.sync_push_batches), (SELECT count(*) FROM public.rs_default_only_compat)").Scan(&batches, &rows); err != nil {
		t.Fatalf("observe predecessor rejection: %v", err)
	}
	if batches != 0 || rows != 0 {
		t.Fatalf("predecessor rejection did durable work: batches=%d rows=%d", batches, rows)
	}
	control := predecessorPush(phase4PushPayload(client, "00000000-0000-4000-8220-00000000b102", []map[string]any{
		realShapeMutation(client, table, "insert", "00000000-0000-4000-8220-000000000102", "authored", "", map[string]any{"label": "authored-label"}),
	}))
	if accepted, ok := control["accepted"].([]any); !ok || len(accepted) != 1 {
		t.Fatalf("predecessor control insert did not apply: %#v", control)
	}

	if _, err := harness.UpdateExtension(ctx); err != nil {
		t.Fatalf("update extension from the predecessor: %v", err)
	}
	states := map[string]map[string]realShapeRow{table.name: {}}
	pushRealShapeApplied(t, ctx, harness, token, admin, client, "00000000-0000-4000-8220-00000000b101", keyOnly["mutations"].([]map[string]any), realShapeTables(table), states)
	requireRealShapeSourceValues(t, ctx, admin, table, "key-only", map[string]any{"label": "server-default"})
	if label := states[table.name]["key-only"].values["label"]; label != "server-default" {
		t.Fatalf("upgraded key-only insert label = %#v, want the server default", label)
	}
}
