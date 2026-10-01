package integration

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

const workerIdleSampleQuery = `
	SELECT w.xmin::text, w.state, w.heartbeat_at, w.wal_observed_at,
	       w.oldest_unmaterialized_commit_timestamp,
	       w.materialized_end_lsn::text, p.materialized_end_lsn::text,
	       synchro.synchro_health_detail()->'checks'->'heartbeat'->>'state'
	FROM synchro.sync_wal_worker_state w
	CROSS JOIN synchro.sync_wal_progress p
	WHERE w.worker_id = 'synchro_wal_consumer' AND p.singleton`

type workerIdleSample struct {
	xmin                                string
	state                               string
	heartbeatAt                         time.Time
	walObservedAt                       sql.NullTime
	oldestUnmaterializedCommitTimestamp sql.NullTime
	workerMaterializedEndLSN            sql.NullString
	progressMaterializedEndLSN          sql.NullString
	heartbeatCheck                      string
}

type workerIdleWindow struct {
	writeCount       int
	smallestWriteGap time.Duration
}

// TestRealWorkerIdleWritesFollowHeartbeatLimit proves that idle writes follow the reloaded heartbeat limit.
// It also proves that state and copied progress changes write without waiting for the idle interval.
// Only the 30-second window requires the heartbeat check in each sample.
// At a smaller limit, one storage flush longer than about half of the limit makes the check fail.
// The test host does not bound the flush time.
func TestRealWorkerIdleWritesFollowHeartbeatLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.ExecContext(cleanupContext, "ALTER SYSTEM RESET synchro.max_worker_heartbeat_age_seconds"); err != nil {
			t.Errorf("reset worker heartbeat limit: %v", err)
		}
		if _, err := admin.ExecContext(cleanupContext, "SELECT pg_catalog.pg_reload_conf()"); err != nil {
			t.Errorf("reload reset worker heartbeat limit: %v", err)
		}
	})

	waitForIssue49CanonicalHealth(t, ctx, admin, true)

	security49SetSystemHealthLimit(t, ctx, admin, "synchro.max_worker_heartbeat_age_seconds", 2)
	time.Sleep(3 * time.Second)
	limitTwo := sampleWorkerIdleWindow(t, ctx, admin, 10*time.Second, 100*time.Millisecond, true, false)
	if limitTwo.writeCount < 5 || limitTwo.writeCount > 11 {
		t.Fatalf("two-second heartbeat limit wrote %d times, want 5..11", limitTwo.writeCount)
	}
	if limitTwo.smallestWriteGap < time.Second {
		t.Fatalf("two-second heartbeat limit minimum write gap = %s, want at least 1s", limitTwo.smallestWriteGap)
	}
	t.Logf("worker idle writes limit=2s writes=%d minimum_heartbeat_gap=%s", limitTwo.writeCount, limitTwo.smallestWriteGap)

	security49SetSystemHealthLimit(t, ctx, admin, "synchro.max_worker_heartbeat_age_seconds", 1)
	time.Sleep(2 * time.Second)
	limitOne := sampleWorkerIdleWindow(t, ctx, admin, 5*time.Second, 100*time.Millisecond, false, false)
	if limitOne.smallestWriteGap < 500*time.Millisecond {
		t.Fatalf("one-second heartbeat limit minimum write gap = %s, want at least 500ms", limitOne.smallestWriteGap)
	}
	t.Logf("worker idle writes limit=1s writes=%d minimum_heartbeat_gap=%s", limitOne.writeCount, limitOne.smallestWriteGap)

	security49SetSystemHealthLimit(t, ctx, admin, "synchro.max_worker_heartbeat_age_seconds", 30)
	time.Sleep(time.Second)
	limitThirty := sampleWorkerIdleWindow(t, ctx, admin, 20*time.Second, 100*time.Millisecond, false, true)
	if limitThirty.writeCount < 1 || limitThirty.writeCount > 2 {
		t.Fatalf("thirty-second heartbeat limit wrote %d times, want 1..2", limitThirty.writeCount)
	}
	if limitThirty.smallestWriteGap < 15*time.Second {
		t.Fatalf("thirty-second heartbeat limit minimum write gap = %s, want at least 15s", limitThirty.smallestWriteGap)
	}
	t.Logf("worker idle writes limit=30s writes=%d minimum_heartbeat_gap=%s", limitThirty.writeCount, limitThirty.smallestWriteGap)

	progressBaseline := waitForWorkerIdleWrite(t, ctx, admin)
	progressStarted := time.Now()
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'idle-write-progress-probe')",
		"00000000-0000-4000-8233-000000000001",
	); err != nil {
		t.Fatalf("commit worker progress probe: %v", err)
	}
	progressChangedAt, progressLSN := waitForWorkerProgressChange(
		t,
		ctx,
		admin,
		progressBaseline.progressMaterializedEndLSN,
		progressStarted.Add(5*time.Second),
	)
	workerCopiedAt := waitForWorkerProgressCopy(
		t,
		ctx,
		admin,
		progressLSN,
		progressChangedAt.Add(5*time.Second),
	)
	progressCopyDelay := workerCopiedAt.Sub(progressChangedAt)
	if progressCopyDelay > 5*time.Second {
		t.Fatalf("worker copied progress after %s, want at most 5s", progressCopyDelay)
	}
	t.Logf("worker progress copy delay=%s", progressCopyDelay)

	waitForWorkerIdleWrite(t, ctx, admin)
	blockStarted := time.Now()
	if _, err := admin.ExecContext(ctx, `
		INSERT INTO synchro.sync_wal_poison (
			stream_generation, commit_lsn, failure_class, failure_detail
		)
		SELECT stream_generation, pg_catalog.pg_current_wal_lsn(),
		       'decode_failed', 'idle_write_state_probe'
		FROM synchro.sync_runtime_state
		WHERE singleton`); err != nil {
		t.Fatalf("commit worker state probe: %v", err)
	}
	blockedAt := waitForWorkerState(t, ctx, admin, "blocked", blockStarted.Add(5*time.Second))
	blockDelay := blockedAt.Sub(blockStarted)
	if _, err := admin.ExecContext(ctx, `
		UPDATE synchro.sync_wal_poison
		SET lifecycle = 'repaired', resolved_at = now()
		WHERE failure_detail = 'idle_write_state_probe' AND lifecycle = 'active'`); err != nil {
		t.Fatalf("repair worker state probe: %v", err)
	}
	runningStarted := time.Now()
	runningAt := waitForWorkerState(t, ctx, admin, "running", runningStarted.Add(5*time.Second))
	runningDelay := runningAt.Sub(runningStarted)
	waitForIssue49CanonicalHealth(t, ctx, admin, true)
	t.Logf("worker state delays blocked=%s running=%s", blockDelay, runningDelay)
}

func sampleWorkerIdleWindow(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	duration time.Duration,
	interval time.Duration,
	requireStableWALObservation bool,
	requireHeartbeatOK bool,
) workerIdleWindow {
	t.Helper()
	previous := loadWorkerIdleSample(t, ctx, database)
	if requireHeartbeatOK {
		requireWorkerHeartbeatOK(t, ctx, database, previous)
	}
	baselineWALObservedAt := previous.walObservedAt
	baselineOldestCommit := previous.oldestUnmaterializedCommitTimestamp
	lastWriteHeartbeat := previous.heartbeatAt
	result := workerIdleWindow{}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("sample worker idle writes: %v", ctx.Err())
		case <-timer.C:
			return result
		case <-ticker.C:
			sample := loadWorkerIdleSample(t, ctx, database)
			if requireHeartbeatOK {
				requireWorkerHeartbeatOK(t, ctx, database, sample)
			}
			if requireStableWALObservation &&
				(!equalNullTime(sample.walObservedAt, baselineWALObservedAt) ||
					!equalNullTime(sample.oldestUnmaterializedCommitTimestamp, baselineOldestCommit)) {
				t.Fatalf(
					"idle WAL observation changed: wal_observed_at=%v oldest_commit=%v baseline_wal_observed_at=%v baseline_oldest_commit=%v",
					sample.walObservedAt,
					sample.oldestUnmaterializedCommitTimestamp,
					baselineWALObservedAt,
					baselineOldestCommit,
				)
			}
			if sample.xmin != previous.xmin {
				gap := sample.heartbeatAt.Sub(lastWriteHeartbeat)
				if result.smallestWriteGap == 0 || gap < result.smallestWriteGap {
					result.smallestWriteGap = gap
				}
				result.writeCount++
				lastWriteHeartbeat = sample.heartbeatAt
			}
			previous = sample
		}
	}
}

func waitForWorkerIdleWrite(t *testing.T, ctx context.Context, database *sql.DB) workerIdleSample {
	t.Helper()
	previous := loadWorkerIdleSample(t, ctx, database)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		sample := loadWorkerIdleSample(t, ctx, database)
		if sample.xmin != previous.xmin {
			return sample
		}
		previous = sample
	}
	t.Fatal("worker did not write within 20s")
	return workerIdleSample{}
}

func waitForWorkerProgressChange(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	baseline sql.NullString,
	deadline time.Time,
) (time.Time, sql.NullString) {
	t.Helper()
	for time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		sample := loadWorkerIdleSample(t, ctx, database)
		if !equalNullString(sample.progressMaterializedEndLSN, baseline) {
			return time.Now(), sample.progressMaterializedEndLSN
		}
	}
	t.Fatal("materialized progress did not change within 5s")
	return time.Time{}, sql.NullString{}
}

func waitForWorkerProgressCopy(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	progressLSN sql.NullString,
	deadline time.Time,
) time.Time {
	t.Helper()
	for time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		sample := loadWorkerIdleSample(t, ctx, database)
		if equalNullString(sample.workerMaterializedEndLSN, progressLSN) {
			return time.Now()
		}
	}
	t.Fatal("worker did not copy materialized progress within 5s")
	return time.Time{}
}

func waitForWorkerState(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	want string,
	deadline time.Time,
) time.Time {
	t.Helper()
	for time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		sample := loadWorkerIdleSample(t, ctx, database)
		if sample.state == want {
			return time.Now()
		}
	}
	t.Fatalf("worker state did not become %q within 5s", want)
	return time.Time{}
}

func loadWorkerIdleSample(t *testing.T, ctx context.Context, database *sql.DB) workerIdleSample {
	t.Helper()
	var sample workerIdleSample
	if err := database.QueryRowContext(ctx, workerIdleSampleQuery).Scan(
		&sample.xmin,
		&sample.state,
		&sample.heartbeatAt,
		&sample.walObservedAt,
		&sample.oldestUnmaterializedCommitTimestamp,
		&sample.workerMaterializedEndLSN,
		&sample.progressMaterializedEndLSN,
		&sample.heartbeatCheck,
	); err != nil {
		t.Fatalf("sample worker idle writes: %v", err)
	}
	return sample
}

func requireWorkerHeartbeatOK(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	sample workerIdleSample,
) {
	t.Helper()
	if sample.heartbeatCheck != "ok" {
		var backendPID int
		var backendActive bool
		if err := database.QueryRowContext(ctx, `
			SELECT worker.backend_pid,
			       EXISTS (
				       SELECT 1
				       FROM pg_catalog.pg_stat_activity activity
				       WHERE activity.pid = worker.backend_pid
			       )
			FROM synchro.sync_wal_worker_state worker
			WHERE worker.worker_id = 'synchro_wal_consumer'`).Scan(&backendPID, &backendActive); err != nil {
			t.Fatalf("observe failed worker heartbeat: %v", err)
		}
		t.Fatalf(
			"worker heartbeat check = %q, want ok: state=%s heartbeat_at=%s local_age=%s xmin=%s backend_pid=%d backend_active=%t",
			sample.heartbeatCheck,
			sample.state,
			sample.heartbeatAt.Format(time.RFC3339Nano),
			time.Since(sample.heartbeatAt),
			sample.xmin,
			backendPID,
			backendActive,
		)
	}
}

func equalNullTime(left, right sql.NullTime) bool {
	return left.Valid == right.Valid && (!left.Valid || left.Time.Equal(right.Time))
}

func equalNullString(left, right sql.NullString) bool {
	return left.Valid == right.Valid && (!left.Valid || left.String == right.String)
}
