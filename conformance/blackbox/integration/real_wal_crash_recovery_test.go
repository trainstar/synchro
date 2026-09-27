package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"syscall"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

type realWALCrashFault int

const (
	realWALImmediateShutdown realWALCrashFault = iota
	realWALBackendCrash
)

const realWALWorkerActivityQuery = `
	SELECT count(*), COALESCE(min(pid), 0)
	FROM pg_catalog.pg_stat_activity
	WHERE datname = current_database()
	  AND backend_type = 'synchro WAL consumer'`

type realWALSlotBoundary struct {
	acknowledged string
	confirmed    string
	equal        bool
	// order is the sign of the acknowledgement minus the reference position.
	order int
}

func TestRealWALRestoresSlotPositionAfterImmediateShutdown(t *testing.T) {
	runRealWALSlotRestore(t, realWALImmediateShutdown, "00000000-0000-4000-8186-000000000001", "00000000-0000-4000-8186-000000000002")
}

func TestRealWALRestoresSlotPositionAfterBackendCrash(t *testing.T) {
	runRealWALSlotRestore(t, realWALBackendCrash, "00000000-0000-4000-8186-000000000011", "00000000-0000-4000-8186-000000000012")
}

func runRealWALSlotRestore(t *testing.T, fault realWALCrashFault, firstID, secondID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)

	insertRealWALCrashRow(t, ctx, harness, firstID, "before-crash")
	waitForRealWALRecords(t, ctx, harness, "cf_items", firstID)
	before := waitForRealWALSlotBoundary(t, ctx, harness, admin, "0/0", func(boundary realWALSlotBoundary) bool {
		return boundary.equal
	}).acknowledged

	switch fault {
	case realWALImmediateShutdown:
		if err := harness.CrashRestartPostgres(ctx); err != nil {
			t.Fatalf("crash restart PostgreSQL: %v; %s", err, harness.FailureDiagnostics())
		}
	case realWALBackendCrash:
		crashRealWALWorkerBackend(t, ctx, harness, admin)
	default:
		t.Fatalf("unknown WAL crash fault %d", fault)
	}
	admin = openIssue49Admin(t, ctx, harness)
	waitForRealWALReadiness(t, ctx, harness, admin)

	restored := loadRealWALSlotBoundary(t, ctx, admin, before)
	if !restored.equal || restored.order < 0 {
		t.Fatalf("WAL slot boundary after the fault is invalid: before=%s boundary=%#v", before, restored)
	}
	var activePoison int
	if err := admin.QueryRowContext(ctx, "SELECT count(*) FROM synchro.sync_wal_poison WHERE lifecycle = 'active'").Scan(&activePoison); err != nil {
		t.Fatalf("count active WAL poison: %v", err)
	}
	if activePoison != 0 {
		t.Fatalf("WAL crash recovery left %d active poison records", activePoison)
	}

	insertRealWALCrashRow(t, ctx, harness, secondID, "after-crash")
	waitForRealWALRecords(t, ctx, harness, "cf_items", secondID)
	waitForRealWALSlotBoundary(t, ctx, harness, admin, before, func(boundary realWALSlotBoundary) bool {
		return boundary.order > 0
	})
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
	var workerCount, priorPID int
	if err := admin.QueryRowContext(ctx, realWALWorkerActivityQuery).Scan(&workerCount, &priorPID); err != nil || workerCount != 1 || priorPID <= 0 {
		t.Fatalf("unique WAL worker is unavailable before the backend crash: count=%d pid=%d err=%v", workerCount, priorPID, err)
	}
	if err := syscall.Kill(priorPID, syscall.SIGKILL); err != nil {
		t.Fatalf("kill WAL worker backend %d: %v", priorPID, err)
	}
	var workerPID int
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

func waitForRealWALSlotBoundary(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
	admin *sql.DB,
	reference string,
	done func(realWALSlotBoundary) bool,
) realWALSlotBoundary {
	t.Helper()
	var boundary realWALSlotBoundary
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		boundary = loadRealWALSlotBoundary(t, ctx, admin, reference)
		if done(boundary) {
			return boundary
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("WAL slot boundary did not reach the expected state: reference=%s boundary=%#v; %s", reference, boundary, harness.FailureDiagnostics())
	return realWALSlotBoundary{}
}

func loadRealWALSlotBoundary(t *testing.T, ctx context.Context, admin *sql.DB, reference string) realWALSlotBoundary {
	t.Helper()
	var boundary realWALSlotBoundary
	if err := admin.QueryRowContext(ctx, `
		SELECT progress.acknowledged_end_lsn::text,
		       slot.confirmed_flush_lsn::text,
		       progress.acknowledged_end_lsn = slot.confirmed_flush_lsn,
		       sign(pg_catalog.pg_wal_lsn_diff(progress.acknowledged_end_lsn, $1::pg_lsn))::integer
		FROM synchro.sync_runtime_state runtime
		JOIN synchro.sync_wal_progress progress
		  ON progress.singleton
		 AND progress.stream_generation = runtime.stream_generation
		JOIN pg_catalog.pg_replication_slots slot ON slot.slot_name = runtime.active_slot_name
		WHERE runtime.singleton`, reference).Scan(
		&boundary.acknowledged,
		&boundary.confirmed,
		&boundary.equal,
		&boundary.order,
	); err != nil {
		t.Fatalf("load WAL slot boundary: %v", err)
	}
	return boundary
}
