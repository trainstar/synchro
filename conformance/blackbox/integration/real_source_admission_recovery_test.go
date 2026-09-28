package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/dataset"
)

// The decoder retains at most 10,000 pgoutput payload records for one source
// transaction (docs/src/content/docs/operations/configuration.mdx). One
// registered row insert produces one row record and one fence message. A
// transaction can also carry one relation message.
const (
	admissionRecordLimit = 10000
	admissionAcceptRows  = 4999
	admissionRejectRows  = 5001
)

const admissionInsertStatement = `
	INSERT INTO cf_items (id, owner_id, value)
	SELECT ('00000000-0000-4000-8d05-' || lpad(($1::integer + source.index)::text, 12, '0'))::uuid,
	       'diagnostic-user',
	       'admission-' || ($1::integer + source.index)::text
	FROM generate_series(0, $2::integer - 1) source(index)`

// admissionTransaction is one committed source transaction and its record
// count from an independent pgoutput decoding session.
type admissionTransaction struct {
	name      string
	rows      int
	xid       string
	records   int64
	relations int64
	commitLSN string
}

// TestRealSourceAdmissionRecovery characterizes the fixed source transaction
// record limit and its documented fail-stop recovery. A transaction at the
// limit side of the boundary materializes. The next transaction above the
// limit poisons the stream with no partial progress. An authorized stream
// reset recovers every committed row, and later ordinary writes flow again.
func TestRealSourceAdmissionRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	runtime := newDatasetRuntime(t, ctx, harness)
	admin := runtime.admin

	prefixID := "00000000-0000-4000-8d05-000000000001"
	runtime.applyTransaction(admissionStatements("INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'admission-prefix')", prefixID))
	runtime.waitMaterialized(time.Minute)
	prefix := observeAdmissionProgress(t, ctx, admin)

	controller, err := blackbox.NewNativeController(blackbox.NativeControllerConfig{Harness: harness})
	if err != nil {
		t.Fatalf("create admission WAL controller: %v", err)
	}
	resume, err := controller.PauseWALMaterialization(ctx)
	if err != nil {
		t.Fatalf("pause admission WAL materialization: %v", err)
	}
	paused := true
	defer func() {
		if paused {
			cleanup, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			if err := resume(cleanup); err != nil {
				t.Errorf("resume admission WAL materialization during cleanup: %v", err)
			}
		}
	}()

	accepted := commitAdmissionTransaction(t, ctx, admin, "accepted", 1000, admissionAcceptRows)
	rejected := commitAdmissionTransaction(t, ctx, admin, "rejected", 10000, admissionRejectRows)
	witnessID := "00000000-0000-4000-8d05-000000099999"
	runtime.applyTransaction(admissionStatements("INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'admission-witness')", witnessID))
	for _, transaction := range []*admissionTransaction{&accepted, &rejected} {
		countAdmissionRecords(t, ctx, admin, transaction)
		t.Logf("admission %s rows=%d pgoutput_records=%d relation_messages=%d commit_lsn=%s",
			transaction.name, transaction.rows, transaction.records, transaction.relations, transaction.commitLSN)
	}
	if accepted.records > admissionRecordLimit || accepted.records < 2*admissionAcceptRows ||
		rejected.records <= admissionRecordLimit || rejected.records > 2*admissionRejectRows+1 {
		t.Fatalf("admission workload does not straddle the record limit: accepted=%d rejected=%d", accepted.records, rejected.records)
	}

	if err := resume(ctx); err != nil {
		t.Fatalf("resume admission WAL materialization: %v", err)
	}
	paused = false
	poison := waitForIssue49Poison(t, ctx, harness, witnessID)
	if poison.FailureClass != "decode_failed" || poison.CommitLSN != rejected.commitLSN || !poison.AcknowledgementBlocked ||
		poison.LaterRecordMaterialized || !poison.WorkerBlocked || !poison.ReadinessBlocked || !poison.PoisonCheckFailed {
		t.Fatalf("record-limit transaction did not become blocking decode poison: %#v rejected=%s", poison, rejected.commitLSN)
	}
	if got := admissionMaterializedRows(t, ctx, admin, accepted.commitLSN); got != int64(accepted.rows) {
		t.Fatalf("accepted transaction materialized %d rows, want %d", got, accepted.rows)
	}
	if got := admissionMaterializedRows(t, ctx, admin, rejected.commitLSN); got != 0 {
		t.Fatalf("rejected transaction partially materialized %d rows", got)
	}
	var walTransactions int64
	if err := admin.QueryRowContext(ctx, "SELECT count(*) FROM synchro.sync_wal_transactions WHERE commit_lsn = $1::pg_lsn", rejected.commitLSN).Scan(&walTransactions); err != nil || walTransactions != 0 {
		t.Fatalf("rejected transaction has %d durable WAL transactions: %v", walTransactions, err)
	}
	blocked := observeIssue49BlockedAcknowledgement(t, ctx, admin, rejected.commitLSN)
	if !blocked.ProgressBeforePoison || !blocked.SlotBeforePoison || !blocked.SlotMatchesProgress || blocked.ProgressEndLSN == prefix {
		t.Fatalf("acknowledgement did not stop after the accepted transaction: prefix=%s %#v", prefix, blocked)
	}
	if status, body := getIssue49Readiness(t, ctx, harness.AdapterURL()); status != http.StatusServiceUnavailable || !bytes.Equal(body, []byte(`{"ready":false}`)) {
		t.Fatalf("readiness did not block: status=%d body=%q", status, body)
	}

	reset, err := harness.Operator().RunStreamReset(ctx)
	if err != nil {
		t.Fatalf("run authorized stream reset: %v; %s", err, harness.FailureDiagnostics())
	}
	if reset.TargetStreamGeneration == "" || reset.TargetStreamGeneration == reset.SourceStreamGeneration {
		t.Fatalf("stream reset did not create a new generation: %#v", reset)
	}
	var lifecycle string
	if err := admin.QueryRowContext(ctx, "SELECT lifecycle FROM synchro.sync_wal_poison WHERE commit_lsn = $1::pg_lsn", rejected.commitLSN).Scan(&lifecycle); err != nil || lifecycle != "reset" {
		t.Fatalf("poison lifecycle after reset = %q: %v", lifecycle, err)
	}
	waitAdmissionReady(t, ctx, harness)

	client := runtime.connect("diagnostic-user", "admission-recovery")
	requireAdmissionRows(t, ctx, admin, client, 1+accepted.rows+rejected.rows+1)

	laterID := "00000000-0000-4000-8d05-000000100000"
	runtime.applyTransaction(admissionStatements("INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'admission-later')", laterID))
	runtime.waitMaterialized(time.Minute)
	if changes := runtime.pull(client); changes != 1 {
		t.Fatalf("later ordinary write produced %d pull changes, want 1", changes)
	}
	requireAdmissionRows(t, ctx, admin, client, 1+accepted.rows+rejected.rows+2)
}

func admissionStatements(statement string, args ...any) []dataset.Statement {
	return []dataset.Statement{{SQL: statement, Args: args}}
}

func commitAdmissionTransaction(t *testing.T, ctx context.Context, admin *sql.DB, name string, firstID, rows int) admissionTransaction {
	t.Helper()
	transaction, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin %s admission transaction: %v", name, err)
	}
	defer transaction.Rollback()
	result := admissionTransaction{name: name, rows: rows}
	if err := transaction.QueryRowContext(ctx, "SELECT pg_current_xact_id()::text").Scan(&result.xid); err != nil {
		t.Fatalf("read %s admission transaction identity: %v", name, err)
	}
	if _, err := transaction.ExecContext(ctx, admissionInsertStatement, firstID, rows); err != nil {
		t.Fatalf("stage %s admission transaction: %v", name, err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit %s admission transaction: %v", name, err)
	}
	return result
}

// countAdmissionRecords decodes the paused active slot in a separate session.
// It counts every payload message of one transaction except BEGIN and COMMIT.
// The commit LSN is the final LSN field of the pgoutput BEGIN message.
func countAdmissionRecords(t *testing.T, ctx context.Context, admin *sql.DB, transaction *admissionTransaction) {
	t.Helper()
	if err := admin.QueryRowContext(ctx, `
		SELECT count(*) FILTER (WHERE get_byte(change.data, 0) NOT IN (66, 67)),
		       count(*) FILTER (WHERE get_byte(change.data, 0) = 82),
		       COALESCE(min(('0/0'::pg_lsn + ('x' || encode(substring(change.data FROM 2 FOR 8), 'hex'))::bit(64)::bigint::numeric)::text)
		           FILTER (WHERE get_byte(change.data, 0) = 66), '')
		FROM pg_logical_slot_peek_binary_changes(
			(SELECT active_slot_name FROM synchro.sync_runtime_state WHERE singleton),
			NULL, NULL, 'proto_version', '1',
			'publication_names', current_setting('synchro.publication_name'),
			'messages', 'true'
		) AS change
		WHERE change.xid::text = ($1::xid8::text::numeric % 4294967296)::text`, transaction.xid,
	).Scan(&transaction.records, &transaction.relations, &transaction.commitLSN); err != nil || transaction.commitLSN == "" {
		t.Fatalf("decode %s admission transaction: commit=%q err=%v", transaction.name, transaction.commitLSN, err)
	}
}

func observeAdmissionProgress(t *testing.T, ctx context.Context, admin *sql.DB) string {
	t.Helper()
	var end string
	if err := admin.QueryRowContext(ctx, "SELECT acknowledged_end_lsn::text FROM synchro.sync_wal_progress WHERE singleton").Scan(&end); err != nil {
		t.Fatalf("observe admission progress: %v", err)
	}
	return end
}

func admissionMaterializedRows(t *testing.T, ctx context.Context, admin *sql.DB, commitLSN string) int64 {
	t.Helper()
	var rows int64
	if err := admin.QueryRowContext(ctx,
		"SELECT count(*) FROM synchro.sync_captured_rows WHERE source_commit_lsn = $1::pg_lsn", commitLSN,
	).Scan(&rows); err != nil {
		t.Fatalf("count materialized admission rows: %v", err)
	}
	return rows
}

func waitAdmissionReady(t *testing.T, ctx context.Context, harness *blackbox.Harness) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		status, body := getIssue49Readiness(t, ctx, harness.AdapterURL())
		if status == http.StatusOK && bytes.Equal(body, []byte(`{"ready":true}`)) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("readiness did not recover after reset: status=%d body=%q", status, body)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// requireAdmissionRows requires the client user scope to hold exactly the
// source cf_items rows, with their exact values.
func requireAdmissionRows(t *testing.T, ctx context.Context, admin *sql.DB, client *datasetClient, want int) {
	t.Helper()
	rows, err := admin.QueryContext(ctx, "SELECT id::text, value FROM cf_items WHERE owner_id = 'diagnostic-user'")
	if err != nil {
		t.Fatalf("read admission source rows: %v", err)
	}
	defer rows.Close()
	source := map[string]string{}
	for rows.Next() {
		var id, value string
		if err := rows.Scan(&id, &value); err != nil {
			t.Fatalf("scan admission source row: %v", err)
		}
		source["cf_items/"+id] = value
	}
	received := client.Rows["user:diagnostic-user"]
	if len(source) != want || len(received) != want {
		t.Fatalf("admission rows: source=%d received=%d want=%d", len(source), len(received), want)
	}
	for key, value := range source {
		row, ok := received[key]
		var got string
		if !ok || json.Unmarshal(row["value"], &got) != nil || got != value {
			t.Fatalf("admission row %s was not delivered with value %q", key, value)
		}
	}
}
