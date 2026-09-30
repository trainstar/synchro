package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

const realWALWorkerActivityQuery = `
	SELECT count(*), COALESCE(min(pid), 0)
	FROM pg_catalog.pg_stat_activity
	WHERE datname = current_database()
	  AND backend_type = 'synchro WAL consumer'`

// TestRealWALRestoresSlotPositionAfterImmediateShutdown proves that the worker restores a rewound slot after a crash.
// PostgreSQL writes a logical slot to disk only when its restart point or catalog xmin changes, or at a checkpoint.
// An open transaction pins both values and a long checkpoint interval stops timed checkpoints.
// The crash then loads the slot from disk behind the durable acknowledgement of a registered write and an idle advance.
func TestRealWALRestoresSlotPositionAfterImmediateShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	waitForIssue49CanonicalHealth(t, ctx, admin, true)
	client := connectRealProtocolClient(t, ctx, harness, token, "wal-crash-restore-client")
	rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8186-000000000021")
	rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8186-000000000022")
	table := requireRealTable(t, client, "cf_items")

	for _, statement := range []string{
		"ALTER SYSTEM SET checkpoint_timeout = '1d'",
		"SELECT pg_catalog.pg_reload_conf()",
		"CREATE TABLE synchro_crash_unpublished (id bigserial PRIMARY KEY, payload text NOT NULL)",
	} {
		if _, err := admin.ExecContext(ctx, statement); err != nil {
			t.Fatalf("prepare slot rewind (%s): %v", statement, err)
		}
	}
	pin, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin slot pin transaction: %v", err)
	}
	// The crash ends this transaction, so the deferred rollback then reports a lost connection.
	defer func() { _ = pin.Rollback() }()
	if _, err := pin.ExecContext(ctx, "INSERT INTO synchro_crash_unpublished (payload) VALUES ('slot-pin')"); err != nil {
		t.Fatalf("write slot pin transaction: %v", err)
	}
	// A standby snapshot after the pin write lets the next slot advance write the pinned slot state to disk.
	if _, err := admin.ExecContext(ctx, "SELECT pg_catalog.pg_log_standby_snapshot()"); err != nil {
		t.Fatalf("log standby snapshot: %v", err)
	}
	pinFlush := loadRealWALFlushLSN(t, ctx, admin)
	waitForRealWALIdleCondition(t, 30*time.Second, "slot did not pass the pinned standby snapshot", func() (bool, any) {
		sample := loadRealWALIdleSample(t, ctx, admin)
		return sample.aligned() && realWALLSNAtOrAfter(sample.slot.String, pinFlush), sample
	})

	firstID := "00000000-0000-4000-8186-000000000001"
	insertRealWALCrashRow(t, ctx, harness, firstID, "before-crash")
	waitForRealWALRecords(t, ctx, harness, "cf_items", firstID)
	pullUntilRealRecords(t, ctx, harness, token, client, []realRecordExpectation{{
		scopeID:  "user:diagnostic-user",
		table:    table,
		recordID: firstID,
		value:    "before-crash",
	}})
	first, err := harness.Operator().ObserveWALRecords(ctx, []string{firstID})
	if err != nil || len(first.Records) != 1 {
		t.Fatalf("observe registered write before the crash: records=%#v err=%v", first.Records, err)
	}
	firstCommit := first.Records[0].CommitLSN

	if _, err := admin.ExecContext(ctx, "INSERT INTO synchro_crash_unpublished (payload) VALUES ('idle-advance')"); err != nil {
		t.Fatalf("write unpublished WAL: %v", err)
	}
	idleFlush := loadRealWALFlushLSN(t, ctx, admin)
	var idle realWALIdleSample
	waitForRealWALIdleCondition(t, 30*time.Second, "idle acknowledgement did not pass the unpublished write", func() (bool, any) {
		idle = loadRealWALIdleSample(t, ctx, admin)
		return idle.aligned() && realWALLSNAtOrAfter(idle.acknowledged.String, idleFlush), idle
	})

	if _, err := admin.ExecContext(ctx, "ALTER SYSTEM SET synchro.auto_start = 'off'"); err != nil {
		t.Fatalf("disable WAL worker automatic start: %v", err)
	}
	if err := harness.CrashRestartPostgres(ctx); err != nil {
		t.Fatalf("crash restart PostgreSQL: %v; %s", err, harness.FailureDiagnostics())
	}
	admin = openIssue49Admin(t, ctx, harness)
	var workers, workerPID int
	if err := admin.QueryRowContext(ctx, realWALWorkerActivityQuery).Scan(&workers, &workerPID); err != nil || workers != 0 {
		t.Fatalf("WAL worker ran before the slot observation: count=%d pid=%d err=%v", workers, workerPID, err)
	}
	var generationStart, durable, rewound string
	if err := admin.QueryRowContext(ctx, `
		SELECT progress.generation_start_lsn::text,
		       progress.acknowledged_end_lsn::text,
		       slot.confirmed_flush_lsn::text
		FROM synchro.sync_runtime_state runtime
		JOIN synchro.sync_wal_progress progress
		  ON progress.singleton
		 AND progress.stream_generation = runtime.stream_generation
		JOIN pg_catalog.pg_replication_slots slot ON slot.slot_name = runtime.active_slot_name
		WHERE runtime.singleton`).Scan(&generationStart, &durable, &rewound); err != nil {
		t.Fatalf("load slot boundary after the crash: %v", err)
	}
	if !realWALLSNAtOrAfter(durable, idle.acknowledged.String) {
		t.Fatalf("durable acknowledgement %s is before the idle acknowledgement %s", durable, idle.acknowledged.String)
	}
	if !realWALLSNAtOrAfter(rewound, generationStart) || !realWALLSNAfter(firstCommit, rewound) || !realWALLSNAfter(durable, rewound) {
		t.Fatalf("crash did not rewind the slot before the registered write: generation_start=%s slot=%s write_commit=%s acknowledged=%s",
			generationStart, rewound, firstCommit, durable)
	}
	t.Logf("slot rewind after the crash: slot=%s write_commit=%s acknowledged=%s", rewound, firstCommit, durable)

	if _, err := admin.ExecContext(ctx, "ALTER SYSTEM RESET synchro.auto_start"); err != nil {
		t.Fatalf("enable WAL worker automatic start: %v", err)
	}
	if err := harness.RestartPostgres(ctx); err != nil {
		t.Fatalf("restart PostgreSQL with the WAL worker: %v; %s", err, harness.FailureDiagnostics())
	}
	admin = openIssue49Admin(t, ctx, harness)
	waitForRealWALIdleCondition(t, 30*time.Second, "slot was not restored to the durable acknowledgement", func() (bool, any) {
		sample := loadRealWALIdleSample(t, ctx, admin)
		return sample.aligned() && realWALLSNAtOrAfter(sample.acknowledged.String, durable), sample
	})
	waitForRealWALReadiness(t, ctx, harness, admin)
	waitForIssue49PublicReady(t, ctx, harness.AdapterURL(), true)
	requireRealWALIdleNoActivePoison(t, ctx, admin)

	secondID := "00000000-0000-4000-8186-000000000002"
	insertRealWALCrashRow(t, ctx, harness, secondID, "after-crash")
	waitForRealWALRecords(t, ctx, harness, "cf_items", firstID, secondID)
	deliveries := map[any]int{}
	var secondValue any
	countDeliveries := func() {
		t.Helper()
		changes, ok := pullRealClient(t, ctx, harness, token, client)["changes"].([]any)
		if !ok {
			t.Fatal("real pull changes are invalid")
		}
		for _, rawChange := range changes {
			change, ok := rawChange.(map[string]any)
			if !ok {
				t.Fatal("real pull change is invalid")
			}
			pk, _ := change["pk"].(map[string]any)
			if change["table"] == table.ID {
				deliveries[pk[table.PrimaryKeyField]]++
				if row, ok := change["row"].(map[string]any); ok && pk[table.PrimaryKeyField] == secondID {
					secondValue = row[table.ValueField]
				}
			}
		}
	}
	deadline := time.Now().Add(20 * time.Second)
	for deliveries[secondID] == 0 && time.Now().Before(deadline) {
		countDeliveries()
	}
	countDeliveries()
	if deliveries[firstID] != 0 || deliveries[secondID] != 1 || secondValue != "after-crash" {
		t.Fatalf("pull after the crash delivered first=%d second=%d second_value=%v, want 0, 1, after-crash",
			deliveries[firstID], deliveries[secondID], secondValue)
	}
}

func insertRealWALCrashRow(t *testing.T, ctx context.Context, harness *blackbox.Harness, recordID, value string) {
	t.Helper()
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		recordID,
		"diagnostic-user",
		value,
	); err != nil {
		t.Fatalf("insert source row %s: %v", recordID, err)
	}
}

func crashRealWALWorkerBackend(t *testing.T, ctx context.Context, harness *blackbox.Harness, admin *sql.DB) {
	t.Helper()
	priorPID, err := harness.CrashWALWorkerBackend(ctx)
	if err != nil {
		t.Fatalf("crash owned WAL worker backend: %v; %s", err, harness.FailureDiagnostics())
	}
	var workerCount, workerPID int
	var lastErr error
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		// A backend crash makes the postmaster end all backends. Queries fail until it accepts connections again.
		lastErr = admin.QueryRowContext(ctx, realWALWorkerActivityQuery).Scan(&workerCount, &workerPID)
		if lastErr == nil && workerCount == 1 && workerPID > 0 && workerPID != priorPID {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("WAL worker did not start again after the backend crash: prior=%d count=%d pid=%d err=%v; %s", priorPID, workerCount, workerPID, lastErr, harness.FailureDiagnostics())
}

func waitForRealWALReadiness(t *testing.T, ctx context.Context, harness *blackbox.Harness, admin *sql.DB) {
	t.Helper()
	var last string
	var lastErr error
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		var encoded []byte
		var readiness map[string]any
		lastErr = admin.QueryRowContext(ctx, "SELECT synchro.synchro_readiness()").Scan(&encoded)
		if lastErr == nil {
			last = string(encoded)
			lastErr = json.Unmarshal(encoded, &readiness)
		}
		if ready, ok := readiness["ready"].(bool); ok && ready {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("readiness did not recover after the fault: readiness=%s err=%v; %s", last, lastErr, harness.FailureDiagnostics())
}
