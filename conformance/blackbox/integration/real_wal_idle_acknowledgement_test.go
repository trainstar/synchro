package integration

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

const realWALIdleSampleQuery = `
SELECT progress.processed_end_lsn::text,
       COALESCE(progress.acknowledged_end_lsn, progress.generation_start_lsn)::text,
       slot.confirmed_flush_lsn::text,
       pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(), slot.restart_lsn)::bigint,
       synchro.synchro_health_detail()->'checks'->'wal_byte_lag'->>'state',
       synchro.synchro_health_detail()->'checks'->'materialization_progress'->>'state'
FROM synchro.sync_wal_progress progress
JOIN synchro.sync_runtime_state runtime ON runtime.singleton
JOIN pg_catalog.pg_replication_slots slot ON slot.slot_name = runtime.active_slot_name
WHERE progress.singleton`

const (
	realWALIdleHeartbeatSetting = "synchro.max_worker_heartbeat_age_seconds"
	realWALIdleRetentionBound   = 4_194_304
	// realWALIdleDefaultByteLag is the extension default of synchro.max_wal_lag_bytes.
	realWALIdleDefaultByteLag = 67_108_864
)

type realWALIdleSample struct {
	processed     sql.NullString
	acknowledged  sql.NullString
	slot          sql.NullString
	retainedBytes sql.NullInt64
	walByteLag    sql.NullString
	progress      sql.NullString
}

// aligned reports that processed, the acknowledgement, and the slot are equal.
func (sample realWALIdleSample) aligned() bool {
	if !sample.processed.Valid || !sample.acknowledged.Valid || !sample.slot.Valid {
		return false
	}
	processed, processedOK := parseRealWALLSN(sample.processed.String)
	acknowledged, acknowledgedOK := parseRealWALLSN(sample.acknowledged.String)
	slot, slotOK := parseRealWALLSN(sample.slot.String)
	return processedOK && acknowledgedOK && slotOK && processed == acknowledged && acknowledged == slot
}

func parseRealWALLSN(text string) (uint64, bool) {
	high, low, found := strings.Cut(text, "/")
	if !found {
		return 0, false
	}
	highValue, err := strconv.ParseUint(high, 16, 32)
	if err != nil {
		return 0, false
	}
	lowValue, err := strconv.ParseUint(low, 16, 32)
	if err != nil {
		return 0, false
	}
	return highValue<<32 | lowValue, true
}

func realWALLSNAtOrAfter(lsn, bound string) bool {
	value, valueOK := parseRealWALLSN(lsn)
	limit, limitOK := parseRealWALLSN(bound)
	return valueOK && limitOK && value >= limit
}

func realWALLSNAfter(lsn, bound string) bool {
	value, valueOK := parseRealWALLSN(lsn)
	limit, limitOK := parseRealWALLSN(bound)
	return valueOK && limitOK && value > limit
}

func realWALLSNEqual(left, right string) bool {
	leftValue, leftOK := parseRealWALLSN(left)
	rightValue, rightOK := parseRealWALLSN(right)
	return leftOK && rightOK && leftValue == rightValue
}

// TestRealWALIdleAcknowledgementFollowsFlush proves that an idle worker acknowledges unpublished WAL.
// The slot follows the flush position, keeps the WAL byte lag in its limit, and releases retained WAL.
// A registered write after the idle acknowledgement reaches a protocol client exactly once.
func TestRealWALIdleAcknowledgementFollowsFlush(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	var byteLagLimit string
	var heartbeatLimitSeconds int
	if err := admin.QueryRowContext(ctx, `
		SELECT current_setting('synchro.max_wal_lag_bytes'),
		       current_setting('synchro.max_worker_heartbeat_age_seconds')::integer`).Scan(&byteLagLimit, &heartbeatLimitSeconds); err != nil {
		t.Fatalf("load WAL readiness limits: %v", err)
	}
	if byteLagLimit != strconv.Itoa(realWALIdleDefaultByteLag) {
		t.Fatalf("WAL byte lag limit is %s, want the default %d", byteLagLimit, realWALIdleDefaultByteLag)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.ExecContext(cleanupContext, "DROP TABLE IF EXISTS synchro_diag_unpublished"); err != nil {
			t.Errorf("drop unpublished diagnostic table: %v", err)
		}
	})
	waitForIssue49CanonicalHealth(t, ctx, admin, true)
	client := connectRealProtocolClient(t, ctx, harness, token, "wal-idle-acknowledgement-client")
	rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8231-000000000011")
	rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8231-000000000012")
	table := requireRealTable(t, client, "cf_items")

	flushZero := loadRealWALFlushLSN(t, ctx, admin)
	// The idle acknowledgement is due when the progress age reaches half of the heartbeat limit.
	// A healthy worker poll finishes within the other half, so the budget is the full heartbeat limit.
	followBudget := time.Duration(heartbeatLimitSeconds) * time.Second
	followDelay := waitForRealWALIdleCondition(t, followBudget, "idle acknowledgement did not follow the flush position", func() (bool, any) {
		sample := loadRealWALIdleSample(t, ctx, admin)
		return sample.aligned() && realWALLSNAtOrAfter(sample.slot.String, flushZero), sample
	})
	t.Logf("idle acknowledgement follow delay=%s", followDelay)

	// A long heartbeat limit disables the heartbeat-age acknowledgement during the burst on any host speed.
	// The barrier acknowledgement below needs a worker poll that starts after the reload.
	setRealWALIdleHeartbeatLimit(t, ctx, admin, "ALTER SYSTEM SET synchro.max_worker_heartbeat_age_seconds = 86400")
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = admin.ExecContext(cleanupContext, "ALTER SYSTEM RESET synchro.max_worker_heartbeat_age_seconds")
		_, _ = admin.ExecContext(cleanupContext, "SELECT pg_catalog.pg_reload_conf()")
	})
	firstID := "00000000-0000-4000-8231-000000000001"
	insertRealWALIdleRow(t, ctx, harness, firstID, "idle-acknowledgement-barrier")
	// The slot advance is visible before the acknowledgement transaction commits.
	// The barrier therefore waits for the durable acknowledgement, not for the slot.
	waitForRealWALIdleRecord(t, ctx, harness, firstID, 20*time.Second, "")

	if _, err := admin.ExecContext(ctx, `
		CREATE TABLE synchro_diag_unpublished (id bigserial PRIMARY KEY, payload text NOT NULL)`); err != nil {
		t.Fatalf("create unpublished diagnostic table: %v", err)
	}
	if _, err := admin.ExecContext(ctx, "ALTER TABLE synchro_diag_unpublished ALTER COLUMN payload SET STORAGE EXTERNAL"); err != nil {
		t.Fatalf("store unpublished diagnostic payload externally: %v", err)
	}
	var largestByteLag int64
	sampleBurst := func() {
		t.Helper()
		sample := loadRealWALIdleSample(t, ctx, admin)
		if sample.walByteLag.String != "ok" {
			t.Fatalf("WAL byte lag check failed during the unpublished burst: %+v", sample)
		}
		if lag := loadRealWALByteLag(t, ctx, admin); lag > largestByteLag {
			largestByteLag = lag
		}
	}
	var burstStart string
	if err := admin.QueryRowContext(ctx, "SELECT pg_catalog.pg_current_wal_lsn()::text").Scan(&burstStart); err != nil {
		t.Fatalf("load WAL position before the unpublished burst: %v", err)
	}
	// Only the acknowledgement at half the byte lag limit can keep the check ok.
	burstStarted := time.Now()
	for row := 0; row < 80; row++ {
		time.Sleep(time.Until(burstStarted.Add(time.Duration(row) * 150 * time.Millisecond)))
		sampleBurst()
		if _, err := admin.ExecContext(ctx, "INSERT INTO synchro_diag_unpublished (payload) VALUES (repeat('x', 1048576))"); err != nil {
			t.Fatalf("insert unpublished diagnostic row %d: %v", row, err)
		}
	}
	burstDuration := time.Since(burstStarted)
	var burstBytes int64
	if err := admin.QueryRowContext(
		ctx,
		"SELECT pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(), $1::pg_lsn)::bigint",
		burstStart,
	).Scan(&burstBytes); err != nil {
		t.Fatalf("measure unpublished burst WAL: %v", err)
	}
	if burstBytes <= realWALIdleDefaultByteLag {
		t.Fatalf("unpublished burst wrote %d WAL bytes, want more than %d", burstBytes, realWALIdleDefaultByteLag)
	}
	burstEnd := loadRealWALFlushLSN(t, ctx, admin)
	sampleBurst()
	t.Logf("idle acknowledgement burst WAL=%d duration=%s largest WAL byte lag=%d", burstBytes, burstDuration, largestByteLag)
	setRealWALIdleHeartbeatLimit(t, ctx, admin, "ALTER SYSTEM RESET synchro.max_worker_heartbeat_age_seconds")

	retentionDelay := waitForRealWALIdleCondition(t, 90*time.Second, "slot did not release retained WAL after the burst", func() (bool, any) {
		sample := loadRealWALIdleSample(t, ctx, admin)
		return sample.retainedBytes.Valid && sample.retainedBytes.Int64 < realWALIdleRetentionBound, sample
	})
	t.Logf("idle acknowledgement retention delay=%s", retentionDelay)
	for second := 0; second < 30; second++ {
		time.Sleep(time.Second)
		sample := loadRealWALIdleSample(t, ctx, admin)
		if !sample.retainedBytes.Valid || sample.retainedBytes.Int64 >= realWALIdleRetentionBound || sample.walByteLag.String != "ok" {
			t.Fatalf("slot retained WAL again after the burst: %+v", sample)
		}
	}

	waitForRealWALIdleCondition(t, 30*time.Second, "idle acknowledgement did not pass the burst end", func() (bool, any) {
		sample := loadRealWALIdleSample(t, ctx, admin)
		return sample.aligned() && realWALLSNAfter(sample.slot.String, burstEnd), sample
	})
	secondID := "00000000-0000-4000-8231-000000000002"
	insertRealWALIdleRow(t, ctx, harness, secondID, "after-idle-acknowledgement")
	waitForRealWALRecords(t, ctx, harness, "cf_items", secondID)
	pullUntilRealRecords(t, ctx, harness, token, client, []realRecordExpectation{{
		scopeID:  "user:diagnostic-user",
		table:    table,
		recordID: secondID,
		value:    "after-idle-acknowledgement",
	}})
	response := pullRealClient(t, ctx, harness, token, client)
	changes, ok := response["changes"].([]any)
	if !ok {
		t.Fatal("real pull changes are invalid")
	}
	for _, rawChange := range changes {
		change, ok := rawChange.(map[string]any)
		if !ok {
			t.Fatal("real pull change is invalid")
		}
		pk, _ := change["pk"].(map[string]any)
		if change["table"] == table.ID && pk[table.PrimaryKeyField] == secondID {
			t.Fatal("real pull delivered the write after idle acknowledgement again")
		}
	}
}

// TestRealWALIdleAcknowledgementDoesNotSkipTransactions proves that an idle acknowledgement stops before undecoded rows.
// A full row batch, an open source transaction, and an active poison each keep the next transaction.
func TestRealWALIdleAcknowledgementDoesNotSkipTransactions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	resetRealWALIdleSettings(t, admin, realWALIdleHeartbeatSetting)
	security49SetSystemHealthLimit(t, ctx, admin, realWALIdleHeartbeatSetting, 120)
	waitForIssue49CanonicalHealth(t, ctx, admin, true)

	release := holdRealWALIdleWorkerGate(t, ctx, harness)
	if _, err := admin.ExecContext(ctx, `
		SELECT pg_catalog.pg_logical_emit_message(false, 'synchro_test_idle', 'x')
		FROM pg_catalog.generate_series(1, 1200)`); err != nil {
		t.Fatalf("emit row-limited idle messages: %v", err)
	}
	rowLimitedID := "00000000-0000-4000-8231-000000000003"
	insertRealWALIdleRow(t, ctx, harness, rowLimitedID, "after-row-limited-messages")
	release()
	rowLimitDelay := waitForRealWALIdleRecord(t, ctx, harness, rowLimitedID, 10*time.Second, "")
	t.Logf("idle acknowledgement row limit recovery=%s", rowLimitDelay)

	security49SetSystemHealthLimit(t, ctx, admin, realWALIdleHeartbeatSetting, 2)
	transaction, err := harness.Source().BeginTx(ctx)
	if err != nil {
		t.Fatalf("begin open source transaction: %v", err)
	}
	transactionOpen := true
	t.Cleanup(func() {
		if transactionOpen {
			if err := transaction.Rollback(); err != nil {
				t.Errorf("roll back open source transaction: %v", err)
			}
		}
	})
	openID := "00000000-0000-4000-8231-000000000004"
	if _, err := transaction.ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', $2)",
		openID,
		"open-during-idle-acknowledgement",
	); err != nil {
		t.Fatalf("insert open source row: %v", err)
	}
	if _, err := admin.ExecContext(ctx, `
		SELECT pg_catalog.pg_logical_emit_message(false, 'synchro_test_idle', repeat('x', 65536))
		FROM pg_catalog.generate_series(1, 64)`); err != nil {
		t.Fatalf("emit idle messages during open transaction: %v", err)
	}
	openFlush := loadRealWALFlushLSN(t, ctx, admin)
	waitForRealWALIdleCondition(t, 10*time.Second, "slot did not follow WAL during the open transaction", func() (bool, any) {
		sample := loadRealWALIdleSample(t, ctx, admin)
		return realWALLSNAtOrAfter(sample.slot.String, openFlush), sample
	})
	transactionOpen = false
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit open source transaction: %v", err)
	}
	openDelay := waitForRealWALIdleRecord(t, ctx, harness, openID, 10*time.Second, openFlush)
	t.Logf("idle acknowledgement open transaction recovery=%s", openDelay)

	if _, err := admin.ExecContext(ctx, `
		INSERT INTO synchro.sync_wal_poison (
			stream_generation, commit_lsn, failure_class, failure_detail
		)
		SELECT stream_generation, pg_catalog.pg_current_wal_lsn(),
		       'decode_failed', 'idle_acknowledgement_poison_probe'
		FROM synchro.sync_runtime_state
		WHERE singleton`); err != nil {
		t.Fatalf("commit idle acknowledgement poison probe: %v", err)
	}
	// The worker reports the blocked state after a poll that sees the poison.
	// A sample after that report cannot include an acknowledgement that started before the poison.
	waitForWorkerState(t, ctx, admin, "blocked", time.Now().Add(5*time.Second))
	blocked := loadRealWALIdleSample(t, ctx, admin)
	if _, err := admin.ExecContext(ctx, `
		SELECT pg_catalog.pg_logical_emit_message(false, 'synchro_test_idle', repeat('x', 65536))
		FROM pg_catalog.generate_series(1, 32)`); err != nil {
		t.Fatalf("emit idle messages while poison blocks: %v", err)
	}
	time.Sleep(5 * time.Second)
	stillBlocked := loadRealWALIdleSample(t, ctx, admin)
	if !realWALLSNEqual(stillBlocked.acknowledged.String, blocked.acknowledged.String) ||
		!realWALLSNEqual(stillBlocked.slot.String, blocked.slot.String) {
		t.Fatalf("idle acknowledgement moved while poison blocked: before=%+v after=%+v", blocked, stillBlocked)
	}
	if _, err := admin.ExecContext(ctx, `
		UPDATE synchro.sync_wal_poison
		SET lifecycle = 'repaired', resolved_at = now()
		WHERE failure_detail = 'idle_acknowledgement_poison_probe' AND lifecycle = 'active'`); err != nil {
		t.Fatalf("repair idle acknowledgement poison probe: %v", err)
	}
	repairDelay := waitForRealWALIdleCondition(t, 10*time.Second, "idle acknowledgement did not resume after repair", func() (bool, any) {
		sample := loadRealWALIdleSample(t, ctx, admin)
		ready, _ := loadIssue49Health(t, ctx, admin)["ready"].(bool)
		return ready && realWALLSNAfter(sample.acknowledged.String, blocked.acknowledged.String), sample
	})
	t.Logf("idle acknowledgement poison repair recovery=%s", repairDelay)
}

// TestRealWALIdleAcknowledgementCrashRecovery proves that the worker completes an idle acknowledgement after a crash.
// A crash after the processed boundary write and a crash after the slot advance both recover without a poison.
func TestRealWALIdleAcknowledgementCrashRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	resetRealWALIdleSettings(t, admin, realWALIdleHeartbeatSetting)
	security49SetSystemHealthLimit(t, ctx, admin, realWALIdleHeartbeatSetting, 120)

	waitForRealWALIdleReady(t, ctx, admin)
	release := holdRealWALIdleWorkerGate(t, ctx, harness)
	processedOne := recordRealWALIdleProcessedBoundary(t, ctx, admin)
	unacknowledged := loadRealWALIdleSample(t, ctx, admin)
	if !realWALLSNAfter(processedOne, unacknowledged.acknowledged.String) {
		t.Fatalf("processed boundary %s is not after the acknowledgement: %+v", processedOne, unacknowledged)
	}
	if unacknowledged.progress.String == "ok" {
		t.Fatalf("materialization progress is ok before the idle acknowledgement: %+v", unacknowledged)
	}
	workerPID, err := harness.Operator().CurrentWALWorkerPID(ctx)
	if err != nil {
		t.Fatalf("observe WAL worker before processed recovery: %v", err)
	}
	release()
	processedDelay := waitForRealWALIdleCondition(t, 10*time.Second, "worker did not acknowledge the processed boundary", func() (bool, any) {
		sample := loadRealWALIdleSample(t, ctx, admin)
		return sample.aligned() && realWALLSNAtOrAfter(sample.processed.String, processedOne) &&
			sample.progress.String == "ok", sample
	})
	requireRealWALIdleNoActivePoison(t, ctx, admin)
	if currentPID, err := harness.Operator().CurrentWALWorkerPID(ctx); err != nil || currentPID != workerPID {
		t.Fatalf("WAL worker changed during processed recovery: before=%d after=%d err=%v", workerPID, currentPID, err)
	}
	t.Logf("idle acknowledgement processed boundary recovery=%s", processedDelay)

	waitForRealWALIdleReady(t, ctx, admin)
	release = holdRealWALIdleWorkerGate(t, ctx, harness)
	processedTwo := recordRealWALIdleProcessedBoundary(t, ctx, admin)
	var slotName string
	if err := admin.QueryRowContext(ctx, `
		SELECT active_slot_name::text
		FROM synchro.sync_runtime_state
		WHERE singleton`).Scan(&slotName); err != nil {
		t.Fatalf("load active replication slot: %v", err)
	}
	var advanced string
	if err := admin.QueryRowContext(
		ctx,
		"SELECT end_lsn::text FROM pg_catalog.pg_replication_slot_advance($1, $2::pg_lsn)",
		slotName,
		processedTwo,
	).Scan(&advanced); err != nil {
		t.Fatalf("advance replication slot to the processed boundary: %v", err)
	}
	if !realWALLSNEqual(advanced, processedTwo) {
		t.Fatalf("replication slot advanced to %s, want %s", advanced, processedTwo)
	}
	workerPID, err = harness.Operator().CurrentWALWorkerPID(ctx)
	if err != nil {
		t.Fatalf("observe WAL worker before slot recovery: %v", err)
	}
	var terminated bool
	if err := admin.QueryRowContext(ctx, "SELECT pg_catalog.pg_terminate_backend($1)", workerPID).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate WAL worker after slot advance: terminated=%t err=%v", terminated, err)
	}
	restartStarted := time.Now()
	restartDeadline := restartStarted.Add(30 * time.Second)
	replacementPID := 0
	for time.Now().Before(restartDeadline) {
		if currentPID, err := harness.Operator().CurrentWALWorkerPID(ctx); err == nil && currentPID != workerPID {
			replacementPID = currentPID
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if replacementPID == 0 {
		t.Fatalf("WAL worker %d did not restart within 30s", workerPID)
	}
	restartDelay := time.Since(restartStarted)
	release()
	slotDelay := waitForRealWALIdleCondition(t, 20*time.Second, "restarted worker did not adopt the advanced slot", func() (bool, any) {
		sample := loadRealWALIdleSample(t, ctx, admin)
		ready, _ := loadIssue49Health(t, ctx, admin)["ready"].(bool)
		return ready &&
			realWALLSNEqual(sample.acknowledged.String, sample.slot.String) &&
			realWALLSNAtOrAfter(sample.acknowledged.String, processedTwo) &&
			realWALLSNEqual(sample.processed.String, sample.acknowledged.String), sample
	})
	requireRealWALIdleNoActivePoison(t, ctx, admin)
	t.Logf("idle acknowledgement slot recovery restart=%s recovery=%s", restartDelay, slotDelay)
}

func resetRealWALIdleSettings(t *testing.T, database *sql.DB, settings ...string) {
	t.Helper()
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		for _, setting := range settings {
			if _, err := database.ExecContext(cleanupContext, "ALTER SYSTEM RESET "+setting); err != nil {
				t.Errorf("reset %s: %v", setting, err)
			}
		}
		if _, err := database.ExecContext(cleanupContext, "SELECT pg_catalog.pg_reload_conf()"); err != nil {
			t.Errorf("reload reset idle acknowledgement settings: %v", err)
		}
	})
}

func holdRealWALIdleWorkerGate(t *testing.T, ctx context.Context, harness *blackbox.Harness) func() {
	t.Helper()
	release, err := harness.Operator().HoldWALWorkerGate(ctx)
	if err != nil {
		t.Fatalf("hold WAL worker gate: %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if err := release(cleanupContext); err != nil {
			t.Errorf("release WAL worker gate in cleanup: %v", err)
		}
	})
	return func() {
		t.Helper()
		if err := release(ctx); err != nil {
			t.Fatalf("release WAL worker gate: %v", err)
		}
	}
}

func setRealWALIdleHeartbeatLimit(t *testing.T, ctx context.Context, admin *sql.DB, statement string) {
	t.Helper()
	if _, err := admin.ExecContext(ctx, statement); err != nil {
		t.Fatalf("change the worker heartbeat limit: %v", err)
	}
	if _, err := admin.ExecContext(ctx, "SELECT pg_catalog.pg_reload_conf()"); err != nil {
		t.Fatalf("reload the worker heartbeat limit: %v", err)
	}
}

func insertRealWALIdleRow(t *testing.T, ctx context.Context, harness *blackbox.Harness, recordID, value string) {
	t.Helper()
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', $2)",
		recordID,
		value,
	); err != nil {
		t.Fatalf("insert idle acknowledgement source row: %v", err)
	}
}

func loadRealWALIdleSample(t *testing.T, ctx context.Context, database *sql.DB) realWALIdleSample {
	t.Helper()
	var sample realWALIdleSample
	if err := database.QueryRowContext(ctx, realWALIdleSampleQuery).Scan(
		&sample.processed,
		&sample.acknowledged,
		&sample.slot,
		&sample.retainedBytes,
		&sample.walByteLag,
		&sample.progress,
	); err != nil {
		t.Fatalf("sample idle acknowledgement progress: %v", err)
	}
	return sample
}

func loadRealWALFlushLSN(t *testing.T, ctx context.Context, database *sql.DB) string {
	t.Helper()
	var lsn string
	if err := database.QueryRowContext(ctx, "SELECT pg_catalog.pg_current_wal_flush_lsn()::text").Scan(&lsn); err != nil {
		t.Fatalf("load WAL flush position: %v", err)
	}
	return lsn
}

func loadRealWALByteLag(t *testing.T, ctx context.Context, database *sql.DB) int64 {
	t.Helper()
	var lag int64
	if err := database.QueryRowContext(ctx, `
		SELECT pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(), slot.confirmed_flush_lsn)::bigint
		FROM synchro.sync_runtime_state runtime
		JOIN pg_catalog.pg_replication_slots slot ON slot.slot_name = runtime.active_slot_name
		WHERE runtime.singleton`).Scan(&lag); err != nil {
		t.Fatalf("load WAL byte lag: %v", err)
	}
	return lag
}

func waitForRealWALIdleCondition(t *testing.T, timeout time.Duration, failure string, condition func() (bool, any)) time.Duration {
	t.Helper()
	started := time.Now()
	deadline := started.Add(timeout)
	for {
		satisfied, observed := condition()
		if satisfied {
			return time.Since(started)
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("%s within %s: %+v", failure, timeout, observed)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// waitForRealWALIdleRecord requires one materialized and acknowledged record without a blocking poison.
// A nonempty commitAfter also requires the record commit position after that LSN.
func waitForRealWALIdleRecord(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
	recordID string,
	timeout time.Duration,
	commitAfter string,
) time.Duration {
	t.Helper()
	return waitForRealWALIdleCondition(t, timeout, "WAL record was not materialized and acknowledged", func() (bool, any) {
		observation, err := harness.Operator().ObserveWALRecords(ctx, []string{recordID})
		if err != nil {
			t.Fatalf("observe idle acknowledgement record: %v", err)
		}
		return len(observation.Records) == 1 &&
			observation.Records[0].FenceCoverage == "materialized" &&
			(commitAfter == "" || realWALLSNAfter(observation.Records[0].CommitLSN, commitAfter)) &&
			observation.ContiguousAcknowledged &&
			observation.SlotMatchesAcknowledgement &&
			!observation.BlockingPoison, observation
	})
}

func waitForRealWALIdleReady(t *testing.T, ctx context.Context, database *sql.DB) {
	t.Helper()
	waitForIssue49CanonicalHealth(t, ctx, database, true)
	waitForRealWALIdleCondition(t, 30*time.Second, "processed, acknowledgement, and slot did not align", func() (bool, any) {
		sample := loadRealWALIdleSample(t, ctx, database)
		return sample.aligned(), sample
	})
}

// recordRealWALIdleProcessedBoundary writes the durable state that a crash after step A leaves.
func recordRealWALIdleProcessedBoundary(t *testing.T, ctx context.Context, database *sql.DB) string {
	t.Helper()
	rows, err := database.QueryContext(ctx, `
		UPDATE synchro.sync_wal_progress
		SET processed_end_lsn = pg_catalog.pg_current_wal_flush_lsn(), updated_at = now()
		WHERE singleton
		  AND processed_end_lsn = COALESCE(acknowledged_end_lsn, generation_start_lsn)
		RETURNING processed_end_lsn::text`)
	if err != nil {
		t.Fatalf("record idle processed boundary: %v", err)
	}
	defer rows.Close()
	var boundaries []string
	for rows.Next() {
		var boundary string
		if err := rows.Scan(&boundary); err != nil {
			t.Fatalf("read idle processed boundary: %v", err)
		}
		boundaries = append(boundaries, boundary)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("finish idle processed boundary: %v", err)
	}
	if len(boundaries) != 1 {
		t.Fatalf("idle processed boundary update returned %d rows, want 1", len(boundaries))
	}
	return boundaries[0]
}

func requireRealWALIdleNoActivePoison(t *testing.T, ctx context.Context, database *sql.DB) {
	t.Helper()
	var active bool
	if err := database.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM synchro.sync_wal_poison WHERE lifecycle = 'active'
		)`).Scan(&active); err != nil {
		t.Fatalf("load active WAL poison: %v", err)
	}
	if active {
		t.Fatal("idle acknowledgement recovery left an active WAL poison")
	}
}
