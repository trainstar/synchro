package integration

import (
	"context"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

type realGeneratedRow struct {
	deleted bool
	row     map[string]any
}

type realGeneratedExpectation struct {
	recordID string
	deleted  bool
	value    string
}

func TestRealWALSkipsUnpublishedGeneratedColumns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	if err := harness.Operator().RegisterGeneratedSourceTableWithoutGeneratedColumns(ctx); err != nil {
		t.Fatalf("register generated source table without generated columns: %v", err)
	}
	runRealGeneratedColumnCapture(t, ctx, harness, token, false)
}

func TestRealWALCapturesPublishedStoredGeneratedColumn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	if err := harness.Operator().PublishStoredGeneratedColumns(ctx); err != nil {
		t.Fatalf("publish stored generated columns: %v", err)
	}
	if err := harness.Operator().RegisterGeneratedSourceTableWithStoredColumn(ctx); err != nil {
		t.Fatalf("register generated source table with stored column: %v", err)
	}
	runRealGeneratedColumnCapture(t, ctx, harness, token, true)
}

func runRealGeneratedColumnCapture(t *testing.T, ctx context.Context, harness *blackbox.Harness, token string, valueLengthSynced bool) {
	t.Helper()
	table := waitForRealGeneratedTable(t, ctx, harness)
	valueField := requireRealSchemaField(t, table, "value")
	valueLengthField, valueLengthPresent := table.Fields["value_length"]
	if valueLengthPresent != valueLengthSynced {
		t.Fatalf("generated table value_length field presence = %t, want %t: %#v", valueLengthPresent, valueLengthSynced, table.Fields)
	}
	for _, excluded := range []string{"search_vector", "value_upper"} {
		if _, present := table.Fields[excluded]; present {
			t.Fatalf("generated table exposed the excluded %s field: %#v", excluded, table.Fields)
		}
	}
	waitForIssue49CanonicalHealth(t, ctx, openIssue49Admin(t, ctx, harness), true)

	client := connectRealProtocolClient(t, ctx, harness, token, "generated-column-client")
	rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8177-00000000b001")
	rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8177-00000000b002")
	rows := make(map[string]realGeneratedRow)
	firstID := "00000000-0000-4000-8177-000000000001"
	secondID := "00000000-0000-4000-8177-000000000002"
	thirdID := "00000000-0000-4000-8177-000000000003"

	writeRealGeneratedRows(t, ctx, harness, []realGeneratedWrite{
		{"INSERT INTO cf_generated_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'alpha')", firstID},
		{"INSERT INTO cf_generated_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'beta')", secondID},
		{"UPDATE cf_generated_items SET value = 'alphabet' WHERE id = $1", firstID},
		{"DELETE FROM cf_generated_items WHERE id = $1", secondID},
	})
	pullRealGeneratedRows(t, ctx, harness, token, client, table, rows, []realGeneratedExpectation{
		{recordID: firstID, value: "alphabet"},
		{recordID: secondID, deleted: true},
	})
	requireRealGeneratedCaptureHealthy(t, ctx, harness)
	requireRealGeneratedRow(t, rows, firstID, valueField, valueLengthField, valueLengthSynced, "alphabet")

	if err := harness.RestartPostgres(ctx); err != nil {
		t.Fatalf("restart PostgreSQL with generated source table: %v", err)
	}
	waitForRealGeneratedReadyAfterRestart(t, ctx, harness)
	waitForIssue49PublicReady(t, ctx, harness.AdapterURL(), true)

	writeRealGeneratedRows(t, ctx, harness, []realGeneratedWrite{
		{"INSERT INTO cf_generated_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'gamma')", thirdID},
		{"UPDATE cf_generated_items SET value = 'gamma-ray-burst' WHERE id = $1", thirdID},
		{"DELETE FROM cf_generated_items WHERE id = $1", firstID},
	})
	pullRealGeneratedRows(t, ctx, harness, token, client, table, rows, []realGeneratedExpectation{
		{recordID: firstID, deleted: true},
		{recordID: secondID, deleted: true},
		{recordID: thirdID, value: "gamma-ray-burst"},
	})
	requireRealGeneratedCaptureHealthy(t, ctx, harness)
	requireRealGeneratedRow(t, rows, thirdID, valueField, valueLengthField, valueLengthSynced, "gamma-ray-burst")
}

func waitForRealGeneratedTable(t *testing.T, ctx context.Context, harness *blackbox.Harness) realSchemaTableReference {
	t.Helper()
	var lastErr error
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		table, err := fetchRealSchemaTableReference(ctx, harness.AdapterURL(), "cf_generated_items")
		if err == nil {
			return table
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("generated source table did not activate: %v; %s", lastErr, harness.FailureDiagnostics())
	return realSchemaTableReference{}
}

func waitForRealGeneratedReadyAfterRestart(t *testing.T, ctx context.Context, harness *blackbox.Harness) {
	t.Helper()
	admin := openIssue49Admin(t, ctx, harness)
	var detail map[string]any
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		detail = loadIssue49Health(t, ctx, admin)
		if ready, ok := detail["ready"].(bool); ok && ready {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("canonical health did not recover after PostgreSQL restart: %#v; %s", detail, harness.FailureDiagnostics())
}

type realGeneratedWrite struct {
	statement string
	recordID  string
}

func writeRealGeneratedRows(t *testing.T, ctx context.Context, harness *blackbox.Harness, writes []realGeneratedWrite) {
	t.Helper()
	for _, write := range writes {
		if err := harness.Source().ExecContext(ctx, write.statement, write.recordID); err != nil {
			t.Fatalf("write generated source row %s: %v", write.recordID, err)
		}
	}
}

func pullRealGeneratedRows(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
	token string,
	client *realProtocolClient,
	table realSchemaTableReference,
	rows map[string]realGeneratedRow,
	expected []realGeneratedExpectation,
) {
	t.Helper()
	valueField := requireRealSchemaField(t, table, "value")
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		response := pullRealClientWithLimit(t, ctx, harness, token, client, client.Scopes, 100)
		for _, change := range requireRealChanges(t, response) {
			if change["table"] != table.TableID {
				continue
			}
			pk, ok := change["pk"].(map[string]any)
			if !ok {
				t.Fatalf("generated source pull key is invalid: %#v", change)
			}
			recordID, ok := pk[table.PKField].(string)
			if !ok {
				t.Fatalf("generated source pull record identity is invalid: %#v", change)
			}
			switch change["op"] {
			case "delete":
				rows[recordID] = realGeneratedRow{deleted: true}
			case "upsert":
				row, ok := change["row"].(map[string]any)
				if !ok {
					t.Fatalf("generated source pull row is invalid: %#v", change)
				}
				rows[recordID] = realGeneratedRow{row: row}
			default:
				t.Fatalf("generated source pull operation is invalid: %#v", change)
			}
		}
		if realGeneratedRowsMatch(rows, valueField, expected) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	detail := loadIssue49Health(t, ctx, openIssue49Admin(t, ctx, harness))
	t.Fatalf("generated source rows did not materialize: rows=%#v expected=%#v health=%#v; %s", rows, expected, detail, harness.FailureDiagnostics())
}

func realGeneratedRowsMatch(rows map[string]realGeneratedRow, valueField string, expected []realGeneratedExpectation) bool {
	for _, expectation := range expected {
		row, ok := rows[expectation.recordID]
		if !ok || row.deleted != expectation.deleted {
			return false
		}
		if !expectation.deleted && row.row[valueField] != expectation.value {
			return false
		}
	}
	return true
}

func requireRealGeneratedRow(
	t *testing.T,
	rows map[string]realGeneratedRow,
	recordID string,
	valueField string,
	valueLengthField string,
	valueLengthSynced bool,
	value string,
) {
	t.Helper()
	row := rows[recordID].row
	if row[valueField] != value {
		t.Fatalf("generated source row %s value = %#v, want %q", recordID, row[valueField], value)
	}
	if !valueLengthSynced {
		return
	}
	valueLength, ok := row[valueLengthField].(float64)
	if !ok || valueLength != float64(len(value)) {
		t.Fatalf("generated source row %s value_length = %#v, want %d", recordID, row[valueLengthField], len(value))
	}
}

func requireRealGeneratedCaptureHealthy(t *testing.T, ctx context.Context, harness *blackbox.Harness) {
	t.Helper()
	admin := openIssue49Admin(t, ctx, harness)
	waitForIssue49CanonicalHealth(t, ctx, admin, true)
	detail := loadIssue49Health(t, ctx, admin)
	checks := issue49HealthChecks(t, detail)
	observations, _ := detail["observations"].(map[string]any)
	if checks["poison"] != "ok" || checks["publication"] != "ok" || observations["poison"] != nil {
		t.Fatalf("generated source capture health is invalid: %#v", detail)
	}
	var activePoison int
	if err := admin.QueryRowContext(ctx, "SELECT count(*) FROM synchro.sync_wal_poison WHERE lifecycle = 'active'").Scan(&activePoison); err != nil {
		t.Fatalf("count active generated source poison: %v", err)
	}
	if activePoison != 0 {
		t.Fatalf("generated source capture left %d active poison records", activePoison)
	}
}
