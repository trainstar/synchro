package integration

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestRealCaptureFenceRejectsOutOfOrderRowWrites proves SYNC-WAL-009 for a
// nested write that changes a row before the fence of an earlier write fires.
func TestRealCaptureFenceRejectsOutOfOrderRowWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	if _, err := admin.ExecContext(ctx, `CREATE FUNCTION public.cf_touch_peer_document() RETURNS trigger LANGUAGE plpgsql AS $$
	BEGIN
		UPDATE public.cf_documents
		SET title = title || '-peer'
		WHERE title = NEW.title AND id <> NEW.id;
		RETURN NULL;
	END
	$$`); err != nil {
		t.Fatalf("create peer trigger function: %v", err)
	}
	waitForIssue49CanonicalHealth(t, ctx, admin, true)
	activateRealTriggerDMLTrigger(t, ctx, admin, realTriggerDMLTrigger{
		name: "trigger_touch_peer", event: "INSERT", relation: "cf_documents", function: "cf_touch_peer_document",
	})

	firstID := "00000000-0000-4000-8181-00000000a001"
	secondID := "00000000-0000-4000-8181-00000000a002"
	controlID := "00000000-0000-4000-8181-00000000a003"
	t.Run("assertion", func(t *testing.T) {
		changeErr := harness.Source().ExecContext(ctx,
			"INSERT INTO cf_documents (id, owner_id, title) VALUES ($1, 'diagnostic-user', 'capture-fence-peer'), ($2, 'diagnostic-user', 'capture-fence-peer')",
			firstID, secondID,
		)
		if changeErr == nil || !strings.Contains(changeErr.Error(), "(SQLSTATE 27000)") {
			t.Fatalf("out-of-order row write did not fail with SQLSTATE 27000: %v", changeErr)
		}
		var fences, versions int
		if err := admin.QueryRowContext(ctx, `
			SELECT (SELECT count(*) FROM synchro.sync_write_fences
			        WHERE old_record_id IN ($1, $2) OR new_record_id IN ($1, $2)),
			       (SELECT count(*) FROM synchro.sync_row_versions WHERE record_id IN ($1, $2))`,
			firstID, secondID,
		).Scan(&fences, &versions); err != nil {
			t.Fatalf("count rejected write state: %v", err)
		}
		if fences != 0 || versions != 0 {
			t.Fatalf("rejected write left state: fences=%d versions=%d", fences, versions)
		}
		commitRealTriggerDMLStatements(t, ctx, harness, []realTriggerDMLStatement{{
			"INSERT INTO cf_documents (id, owner_id, title) VALUES ($1, 'diagnostic-user', 'capture-fence-control')",
			[]any{controlID},
		}})
		if fences := waitForRealTriggerDMLCommit(t, ctx, harness, admin, controlID); fences != 1 {
			t.Fatalf("control commit emitted %d fences, want 1", fences)
		}
		requireRealTriggerDMLCaptureHealthy(t, ctx, admin)
		requireRealTriggerDMLRow(t, ctx, admin, realTriggerDMLRow{relation: "cf_documents", recordID: controlID, column: "title"})
	})
}
