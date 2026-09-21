package integration

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

const issue50TextValueBytes = 32 * 1024

const issue50InsertRowsStatement = `
	INSERT INTO cf_items (id, owner_id, value)
	SELECT ('00000000-0000-4000-8c50-' || lpad((source.index + 100)::text, 12, '0'))::uuid,
	       'diagnostic-user',
	       repeat(md5(source.index::text), 1024)
	FROM generate_series(0, $1::integer - 1) source(index)`

// TestRealIssue50ActiveWALIntakeBounds records the active-worker baseline for
// oversized source transactions. It proves that intake fails closed without
// materializing a partial transaction or advancing the valid prefix.
func TestRealIssue50ActiveWALIntakeBounds(t *testing.T) {
	for _, workload := range []struct {
		name string
		rows int
	}{
		{name: "32MiB", rows: 1024},
		{name: "128MiB", rows: 4096},
	} {
		t.Run(workload.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
			defer cancel()
			harness, token := provisionRealProofHarness(t, ctx)
			client := connectRealProtocolClient(t, ctx, harness, token, "issue50-active-intake")
			rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8c50-000000000001")
			rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8c50-000000000002")
			table := requireRealTable(t, client, "cf_items")
			admin := openIssue49Admin(t, ctx, harness)

			prefixID := "00000000-0000-4000-8c50-000000000010"
			if err := harness.Source().ExecContext(
				ctx,
				"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'issue50-valid-prefix')",
				prefixID,
			); err != nil {
				t.Fatalf("commit Issue 50 valid source prefix: %v", err)
			}
			waitForRealWALRecords(t, ctx, harness, "cf_items", prefixID)
			var prefix blackbox.WALPipelineObservation
			var prefixErr error
			prefixDeadline := time.Now().Add(30 * time.Second)
			for time.Now().Before(prefixDeadline) {
				prefix, prefixErr = harness.Operator().ObserveWALRecords(ctx, []string{prefixID})
				if prefixErr == nil && len(prefix.Records) == 1 && prefix.WorkerRunning &&
					!prefix.BlockingPoison && prefix.ContiguousAcknowledged &&
					prefix.AcknowledgementMatchesObservedEnd && prefix.SlotMatchesObservedEnd {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			if prefixErr != nil || len(prefix.Records) != 1 || !prefix.WorkerRunning || prefix.BlockingPoison ||
				!prefix.ContiguousAcknowledged || !prefix.AcknowledgementMatchesObservedEnd || !prefix.SlotMatchesObservedEnd {
				t.Fatalf("establish Issue 50 acknowledged WAL prefix: observation=%#v err=%v", prefix, prefixErr)
			}
			prefixEndLSN := prefix.Records[0].EndLSN
			pullUntilRealRecords(t, ctx, harness, token, client, []realRecordExpectation{{
				scopeID: "user:diagnostic-user", table: table, recordID: prefixID, value: "issue50-valid-prefix",
			}})
			acknowledgeRealClientCursors(t, ctx, harness, token, client)

			controller, err := blackbox.NewNativeController(blackbox.NativeControllerConfig{Harness: harness})
			if err != nil {
				t.Fatalf("create Issue 50 WAL controller: %v", err)
			}
			resumeWAL, err := controller.PauseWALMaterialization(ctx)
			if err != nil {
				t.Fatalf("pause Issue 50 WAL materialization: %v", err)
			}
			walPaused := true
			defer func() {
				if !walPaused {
					return
				}
				cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cleanupCancel()
				if err := resumeWAL(cleanupContext); err != nil {
					t.Errorf("resume Issue 50 WAL materialization during cleanup: %v", err)
				}
			}()

			sourceTransaction, err := harness.Source().BeginTx(ctx)
			if err != nil {
				t.Fatalf("begin Issue 50 oversized source transaction: %v", err)
			}
			sourceCommitted := false
			defer func() {
				if !sourceCommitted {
					_ = sourceTransaction.Rollback()
				}
			}()
			if _, err := sourceTransaction.ExecContext(ctx, issue50InsertRowsStatement, workload.rows); err != nil {
				t.Fatalf("stage Issue 50 oversized source transaction: %v", err)
			}
			if err := sourceTransaction.Commit(); err != nil {
				t.Fatalf("commit Issue 50 oversized source transaction: %v", err)
			}
			sourceCommitted = true

			witnessID := "00000000-0000-4000-8c50-000000009999"
			if err := harness.Source().ExecContext(
				ctx,
				"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'issue50-later-witness')",
				witnessID,
			); err != nil {
				t.Fatalf("commit Issue 50 later source witness: %v", err)
			}

			workerPID, err := harness.Operator().CurrentWALWorkerPID(ctx)
			if err != nil {
				t.Fatalf("observe Issue 50 WAL worker: %v", err)
			}
			baselineRSS, err := readRealProcessRSSBytes(ctx, workerPID)
			if err != nil {
				t.Fatalf("read Issue 50 baseline worker RSS: %v", err)
			}
			samplingContext, stopSampling := context.WithCancel(ctx)
			type samplingResult struct {
				peak int64
				err  error
			}
			samplingReady := make(chan error, 1)
			samplingResults := make(chan samplingResult, 1)
			go func() {
				peak := baselineRSS
				first := true
				for {
					rss, sampleErr := readRealProcessRSSBytes(samplingContext, workerPID)
					if samplingContext.Err() != nil {
						samplingResults <- samplingResult{peak: peak}
						return
					}
					if sampleErr != nil {
						if first {
							samplingReady <- sampleErr
						}
						samplingResults <- samplingResult{peak: peak, err: sampleErr}
						return
					}
					if rss > peak {
						peak = rss
					}
					if first {
						samplingReady <- nil
						first = false
					}
					timer := time.NewTimer(time.Millisecond)
					select {
					case <-samplingContext.Done():
						if !timer.Stop() {
							<-timer.C
						}
						samplingResults <- samplingResult{peak: peak}
						return
					case <-timer.C:
					}
				}
			}()
			samplingStopped := false
			defer func() {
				if samplingStopped {
					return
				}
				stopSampling()
				<-samplingResults
			}()
			if err := <-samplingReady; err != nil {
				stopSampling()
				<-samplingResults
				samplingStopped = true
				t.Fatalf("start Issue 50 WAL worker RSS sampling: %v", err)
			}

			if err := resumeWAL(ctx); err != nil {
				t.Fatalf("resume Issue 50 WAL materialization: %v", err)
			}
			walPaused = false
			poison := waitForIssue49Poison(t, ctx, harness, witnessID)
			stopSampling()
			sample := <-samplingResults
			samplingStopped = true
			if sample.err != nil || sample.peak <= 0 {
				t.Fatalf("sample Issue 50 WAL worker RSS: peak=%d err=%v", sample.peak, sample.err)
			}

			blockedAcknowledgement := observeIssue49BlockedAcknowledgement(t, ctx, admin, poison.CommitLSN)
			var transactions, events, capturedRows, changes int
			if err := admin.QueryRowContext(ctx, `
				SELECT (SELECT count(*) FROM synchro.sync_wal_transactions WHERE commit_lsn = $1::pg_lsn),
				       (SELECT count(*) FROM synchro.sync_wal_events WHERE commit_lsn = $1::pg_lsn),
				       (SELECT count(*) FROM synchro.sync_captured_rows WHERE source_commit_lsn = $1::pg_lsn),
				       (SELECT count(*) FROM synchro.sync_changelog WHERE commit_lsn = $1::pg_lsn)`, poison.CommitLSN).Scan(
				&transactions,
				&events,
				&capturedRows,
				&changes,
			); err != nil {
				t.Fatalf("observe Issue 50 oversized transaction materialization: %v", err)
			}
			readyStatus, readyBody := getIssue49Readiness(t, ctx, harness.AdapterURL())
			currentPID, err := harness.Operator().CurrentWALWorkerPID(ctx)
			if err != nil {
				t.Fatalf("observe Issue 50 blocked WAL worker: %v", err)
			}
			payloadBytes := int64(workload.rows * issue50TextValueBytes)
			t.Logf("Issue 50 source_payload_bytes=%d worker_rss_baseline_bytes=%d worker_rss_peak_bytes=%d", payloadBytes, baselineRSS, sample.peak)

			if poison.FailureClass != "decode_failed" || poison.CommitLSN == "" ||
				!poison.AcknowledgementBlocked || poison.LaterRecordMaterialized || !poison.LaterFencePending ||
				!poison.WorkerBlocked || !poison.ReadinessBlocked || !poison.PoisonCheckFailed {
				t.Fatalf("Issue 50 oversized transaction did not persist a blocking decode poison: %#v", poison)
			}
			if transactions != 0 || events != 0 || capturedRows != 0 || changes != 0 {
				t.Fatalf("Issue 50 oversized transaction partially materialized: transactions=%d events=%d captured_rows=%d changes=%d", transactions, events, capturedRows, changes)
			}
			if !blockedAcknowledgement.SlotMatchesProgress || !blockedAcknowledgement.ProgressBeforePoison ||
				!blockedAcknowledgement.SlotBeforePoison || blockedAcknowledgement.ProgressEndLSN != prefixEndLSN ||
				blockedAcknowledgement.SlotFlushLSN != prefixEndLSN {
				t.Fatalf("Issue 50 acknowledgement moved beyond the valid prefix: prefix=%s acknowledgement=%#v", prefixEndLSN, blockedAcknowledgement)
			}
			if readyStatus != http.StatusServiceUnavailable || !bytes.Equal(readyBody, []byte(`{"ready":false}`)) {
				t.Fatalf("Issue 50 readiness did not block: status=%d body=%q", readyStatus, readyBody)
			}
			if currentPID != workerPID {
				t.Fatalf("Issue 50 WAL worker changed during oversized intake: before=%d after=%d", workerPID, currentPID)
			}
		})
	}
}
