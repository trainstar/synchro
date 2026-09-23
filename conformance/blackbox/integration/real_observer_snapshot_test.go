package integration

import (
	"context"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

func TestRealWALObservationUsesOneSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	recordID := "00000000-0000-4000-8f35-000000000001"
	if err := harness.Source().ExecContext(ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'snapshot-control')",
		recordID,
	); err != nil {
		t.Fatalf("insert observation witness: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", recordID)
	initial, err := harness.Operator().ObserveWALRecords(ctx, []string{recordID})
	if err != nil || len(initial.Records) != 1 {
		t.Fatalf("read observation witness: records=%d err=%v", len(initial.Records), err)
	}
	record := initial.Records[0]
	workerBarrier, _ := beginIssue49StoppedWALWorker(t, ctx, harness, admin)
	defer workerBarrier.Rollback()

	for _, test := range []struct {
		name    string
		observe func(context.Context, []string) (blackbox.WALPipelineObservation, error)
	}{
		{"diagnostic records", harness.Operator().ObserveWALRecords},
		{"table records", func(ctx context.Context, ids []string) (blackbox.WALPipelineObservation, error) {
			return harness.Operator().ObserveWALRecordsForTable(ctx, "cf_items", ids)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := admin.ExecContext(ctx, `
				WITH reset AS (
					UPDATE synchro.sync_wal_transactions SET replay_count = 0
					WHERE commit_lsn = $1::pg_lsn RETURNING 1
				)
				UPDATE synchro.sync_wal_progress SET acknowledged_end_lsn = NULL
				WHERE singleton AND EXISTS (SELECT 1 FROM reset)`, record.CommitLSN); err != nil {
				t.Fatalf("prepare prior observation state: %v", err)
			}
			change, err := admin.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin observation boundary: %v", err)
			}
			defer change.Rollback()
			if _, err := change.ExecContext(ctx, "LOCK TABLE synchro.sync_wal_progress IN ACCESS EXCLUSIVE MODE"); err != nil {
				t.Fatalf("hold observation boundary: %v", err)
			}
			type result struct {
				observation blackbox.WALPipelineObservation
				err         error
			}
			completed := make(chan result, 1)
			go func() {
				observation, err := test.observe(ctx, []string{recordID})
				completed <- result{observation, err}
			}()
			blocked := false
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				if err := admin.QueryRowContext(ctx, `
					SELECT EXISTS (
						SELECT 1 FROM pg_catalog.pg_locks
						WHERE relation = 'synchro.sync_wal_progress'::regclass
						  AND mode = 'AccessShareLock' AND NOT granted
					)`).Scan(&blocked); err != nil {
					t.Fatalf("observe blocked state read: %v", err)
				}
				if blocked {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if !blocked {
				t.Fatal("observer did not reach the boundary after reading records")
			}
			if _, err := change.ExecContext(ctx, `
				WITH replayed AS (
					UPDATE synchro.sync_wal_transactions SET replay_count = 1
					WHERE commit_lsn = $1::pg_lsn RETURNING 1
				)
				UPDATE synchro.sync_wal_progress SET acknowledged_end_lsn = $2::pg_lsn
				WHERE singleton AND EXISTS (SELECT 1 FROM replayed)`,
				record.CommitLSN, record.EndLSN,
			); err != nil {
				t.Fatalf("commit coupled replay and acknowledgement: %v", err)
			}
			if err := change.Commit(); err != nil {
				t.Fatalf("release observation boundary: %v", err)
			}
			var observed result
			select {
			case observed = <-completed:
			case <-ctx.Done():
				t.Fatal("observer did not complete")
			}
			if observed.err != nil || len(observed.observation.Records) != 1 {
				t.Fatalf("complete blocked observation: records=%d err=%v", len(observed.observation.Records), observed.err)
			}
			if observed.observation.Records[0].ReplayCount != 0 || observed.observation.ContiguousAcknowledged {
				t.Fatalf("observation mixed commits: replay_count=%d acknowledged=%t",
					observed.observation.Records[0].ReplayCount, observed.observation.ContiguousAcknowledged)
			}
			current, err := test.observe(ctx, []string{recordID})
			if err != nil || len(current.Records) != 1 || current.Records[0].ReplayCount != 1 || !current.ContiguousAcknowledged {
				t.Fatalf("next observation did not include the committed update: records=%d acknowledged=%t err=%v",
					len(current.Records), current.ContiguousAcknowledged, err)
			}
		})
	}
}
