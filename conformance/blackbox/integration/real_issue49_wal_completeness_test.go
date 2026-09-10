package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

// TestRealIssue49CompletePullVisibleWALRepresentation proves the complete
// durable representation clause of SYNC-WAL-001.
func TestRealIssue49CompletePullVisibleWALRepresentation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-complete-wal-representation")
	rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8e01-000000000001")
	rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8e01-000000000002")
	table := requireRealTable(t, client, "cf_items")
	admin := openIssue49Admin(t, ctx, harness)

	recordIDs := []string{
		"00000000-0000-4000-8e01-000000000011",
		"00000000-0000-4000-8e01-000000000012",
	}
	sourceTransaction, err := harness.Source().BeginTx(ctx)
	if err != nil {
		t.Fatalf("begin complete WAL representation transaction: %v", err)
	}
	for index, recordID := range recordIDs {
		if _, err := sourceTransaction.ExecContext(
			ctx,
			"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', $2)",
			recordID,
			"issue49-complete-representation-"+string(rune('a'+index)),
		); err != nil {
			_ = sourceTransaction.Rollback()
			t.Fatalf("insert complete WAL representation row %d: %v", index+1, err)
		}
	}
	if err := sourceTransaction.Commit(); err != nil {
		t.Fatalf("commit complete WAL representation transaction: %v", err)
	}

	representation := waitForIssue49CompleteWALRepresentation(t, ctx, admin, recordIDs)
	pullStatus, pullResponse := postSync(
		t,
		ctx,
		harness.AdapterURL(),
		token,
		"/sync/pull",
		realPullPayload(client, issue49CloneScopes(client.Scopes), 100),
	)

	t.Run("assertion", func(t *testing.T) {
		want := issue49CompleteWALRepresentation{
			Fences:             2,
			Transactions:       1,
			Events:             2,
			Projections:        2,
			CapturedRows:       2,
			Edges:              2,
			Changes:            2,
			DeclaredEvents:     2,
			DeclaredEffects:    2,
			Acknowledged:       true,
			OneSourceIdentity:  true,
			ConsistentVersions: true,
		}
		if representation != want {
			t.Fatalf("committed WAL transaction lost part of its pull-visible representation: got=%#v want=%#v", representation, want)
		}
		if pullStatus != http.StatusOK {
			t.Fatalf("complete WAL representation pull status = %d: %#v", pullStatus, pullResponse)
		}
		changes := requireRealChanges(t, pullResponse)
		if len(changes) != 2 {
			t.Fatalf("complete WAL representation pull returned %d changes: %#v", len(changes), changes)
		}
		requireRealPullChange(t, changes, "user:diagnostic-user", table, recordIDs[0], "issue49-complete-representation-a")
		requireRealPullChange(t, changes, "user:diagnostic-user", table, recordIDs[1], "issue49-complete-representation-b")
	})
}

// TestRealIssue49CaptureReadinessRequiresEveryCheck proves the omitted
// state-fault clauses of SYNC-WAL-008. Existing Issue 49 tests cover poison,
// relation, schema, extension, database, and lag failures.
func TestRealIssue49CaptureReadinessRequiresEveryCheck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	healthRecordID := "00000000-0000-4000-8e02-000000000001"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'issue49-readiness-progress')",
		healthRecordID,
	); err != nil {
		t.Fatalf("insert readiness progress row: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", healthRecordID)
	waitForIssue49CanonicalHealth(t, ctx, admin, true)
	baseline := loadIssue49Health(t, ctx, admin)

	faultSQL := []struct {
		check string
		sql   string
	}{
		{
			check: "registry_generation",
			sql: `WITH active AS (
				SELECT generation, stream_generation
				FROM synchro.sync_registry_generations
				WHERE state = 'active'
			), pending AS (
				INSERT INTO synchro.sync_registry_generations (
					stream_generation, state, validated, parent_generation
				)
				SELECT stream_generation, 'pending', true, generation FROM active
				RETURNING generation
			)
			UPDATE synchro.sync_wal_progress
			SET registry_generation = pending.generation
			FROM pending
			WHERE singleton`,
		},
		{
			check: "capture_triggers",
			sql:   "ALTER TABLE public.cf_items DISABLE TRIGGER synchro_capture_fence",
		},
		{
			check: "publication",
			sql: `DO $block$
			DECLARE publication_name name;
			BEGIN
				SELECT active_publication_name INTO STRICT publication_name
				FROM synchro.sync_runtime_state WHERE singleton;
				EXECUTE format('ALTER PUBLICATION %I DROP TABLE public.cf_items', publication_name);
			END
			$block$`,
		},
		{
			check: "replication_slot",
			sql:   "UPDATE synchro.sync_runtime_state SET active_slot_name = 'issue49_missing_slot' WHERE singleton",
		},
		{
			check: "materialization_progress",
			sql:   "UPDATE synchro.sync_wal_progress SET acknowledged_end_lsn = NULL WHERE singleton",
		},
		{
			check: "worker",
			sql:   "UPDATE synchro.sync_wal_worker_state SET state = 'stopped' WHERE worker_id = 'synchro_wal_consumer'",
		},
		{
			check: "heartbeat",
			sql:   "UPDATE synchro.sync_wal_worker_state SET heartbeat_at = now() - interval '1 hour' WHERE worker_id = 'synchro_wal_consumer'",
		},
	}
	faults := make(map[string]map[string]any, len(faultSQL)+1)
	for _, fault := range faultSQL {
		transaction, err := admin.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin %s readiness fault: %v", fault.check, err)
		}
		if _, err := transaction.ExecContext(ctx, fault.sql); err != nil {
			_ = transaction.Rollback()
			t.Fatalf("inject %s readiness fault: %v", fault.check, err)
		}
		faults[fault.check] = loadIssue49Health(t, ctx, transaction)
		if err := transaction.Rollback(); err != nil {
			t.Fatalf("rollback %s readiness fault: %v", fault.check, err)
		}
		waitForIssue49CanonicalHealth(t, ctx, admin, true)
	}

	interrupted, err := harness.Operator().CreateInterruptedStreamReset(ctx)
	if err != nil {
		t.Fatalf("create readiness stream-reset fault: %v", err)
	}
	faults["stream_reset"] = loadIssue49Health(t, ctx, admin)
	if err := harness.Operator().RecoverInterruptedStreamReset(ctx); err != nil {
		t.Fatalf("recover readiness stream-reset fault %s: %v", interrupted.ResetID, err)
	}
	waitForIssue49CanonicalHealth(t, ctx, admin, true)

	t.Run("assertion", func(t *testing.T) {
		baselineChecks := issue49HealthChecks(t, baseline)
		if len(baselineChecks) != len(issue49HealthCheckNames) || baseline["ready"] != true {
			t.Fatalf("healthy readiness did not contain the complete canonical check set: %#v", baseline)
		}
		for _, check := range issue49HealthCheckNames {
			if baselineChecks[check] != "ok" {
				t.Fatalf("healthy canonical readiness check %q = %q", check, baselineChecks[check])
			}
		}
		for check, detail := range faults {
			if detail["ready"] != false || issue49HealthChecks(t, detail)[check] == "ok" {
				t.Fatalf("capture readiness ignored unhealthy %q state: %#v", check, detail)
			}
		}
	})
}

// TestRealIssue49FenceCorrelatesOldRecordIdentity proves the old-key clause
// of SYNC-WAL-009 independently from new-key and row-version correlation.
func TestRealIssue49FenceCorrelatesOldRecordIdentity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	recordID := "00000000-0000-4000-8e03-000000000001"
	wrongOldID := "00000000-0000-4000-8e03-000000000002"
	laterID := "00000000-0000-4000-8e03-000000000003"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'issue49-old-key-base')",
		recordID,
	); err != nil {
		t.Fatalf("insert old-key correlation base row: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", recordID)

	barrier, _ := beginIssue49StoppedWALWorker(t, ctx, harness, admin)
	defer barrier.Rollback()
	if err := harness.Source().ExecContext(
		ctx,
		"UPDATE cf_items SET value = 'issue49-old-key-update', updated_at = clock_timestamp() WHERE id = $1",
		recordID,
	); err != nil {
		t.Fatalf("commit old-key correlation update: %v", err)
	}
	var fenceID string
	if err := barrier.QueryRowContext(ctx, `
		UPDATE synchro.sync_write_fences
		SET old_record_id = $2
		WHERE fence_id = (
			SELECT fence_id FROM synchro.sync_write_fences
			WHERE coverage = 'pending' AND operation = 'update'
			  AND old_record_id = $1 AND new_record_id = $1
			ORDER BY created_at DESC LIMIT 1
		)
		RETURNING fence_id::text`, recordID, wrongOldID).Scan(&fenceID); err != nil {
		t.Fatalf("inject old-record fence mismatch: %v", err)
	}
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'issue49-after-old-key-mismatch')",
		laterID,
	); err != nil {
		t.Fatalf("commit source write after old-key mismatch: %v", err)
	}
	if err := barrier.Commit(); err != nil {
		t.Fatalf("release old-key correlation barrier: %v", err)
	}
	barrier = nil
	outcome := waitForIssue49FenceMismatchOutcome(t, ctx, admin, fenceID, laterID)

	t.Run("assertion", func(t *testing.T) {
		if !outcome.FencePending || outcome.OldRecordID != wrongOldID || outcome.NewRecordID != recordID ||
			!outcome.BlockingCorrelationPoison || outcome.CorrelatedEvent || outcome.CorrelatedEffect || outcome.LaterMaterialized {
			t.Fatalf("old-record mismatch did not block complete fence correlation: %#v", outcome)
		}
	})
}

// TestRealIssue49FenceCorrelatesCaptureKeys proves both capture-key clauses
// of SYNC-WAL-009 independently from synced-row key correlation.
func TestRealIssue49FenceCorrelatesCaptureKeys(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	documentID := "00000000-0000-4000-8e04-000000000001"
	accessID := "00000000-0000-4000-8e04-000000000002"
	wrongOldID := "00000000-0000-4000-8e04-000000000003"
	wrongNewID := "00000000-0000-4000-8e04-000000000004"
	laterID := "00000000-0000-4000-8e04-000000000005"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_documents (id, owner_id, title) VALUES ($1, 'diagnostic-user', 'issue49-capture-key-document')",
		documentID,
	); err != nil {
		t.Fatalf("insert capture-key document: %v", err)
	}
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_document_access (id, document_id, owner_id) VALUES ($1, $2, 'diagnostic-user')",
		accessID,
		documentID,
	); err != nil {
		t.Fatalf("insert capture-key base row: %v", err)
	}
	if !waitForIssue49CaptureDependencyFence(t, ctx, admin, accessID) {
		t.Fatal("capture-key base fence did not materialize")
	}

	barrier, _ := beginIssue49StoppedWALWorker(t, ctx, harness, admin)
	defer barrier.Rollback()
	if err := harness.Source().ExecContext(
		ctx,
		"UPDATE cf_document_access SET owner_id = 'issue49-capture-key-owner' WHERE id = $1",
		accessID,
	); err != nil {
		t.Fatalf("commit capture-key correlation update: %v", err)
	}
	var fenceID string
	if err := barrier.QueryRowContext(ctx, `
		UPDATE synchro.sync_write_fences
		SET old_capture_key = jsonb_build_object('id', $2::text),
		    new_capture_key = jsonb_build_object('id', $3::text)
		WHERE fence_id = (
			SELECT fence_id FROM synchro.sync_write_fences
			WHERE coverage = 'pending' AND registration_kind = 'capture_dependency'
			  AND operation = 'update'
			  AND old_capture_key->>'id' = $1 AND new_capture_key->>'id' = $1
			ORDER BY created_at DESC LIMIT 1
		)
		RETURNING fence_id::text`, accessID, wrongOldID, wrongNewID).Scan(&fenceID); err != nil {
		t.Fatalf("inject capture-key fence mismatch: %v", err)
	}
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'issue49-after-capture-key-mismatch')",
		laterID,
	); err != nil {
		t.Fatalf("commit source write after capture-key mismatch: %v", err)
	}
	if err := barrier.Commit(); err != nil {
		t.Fatalf("release capture-key correlation barrier: %v", err)
	}
	barrier = nil
	outcome := waitForIssue49FenceMismatchOutcome(t, ctx, admin, fenceID, laterID)

	t.Run("assertion", func(t *testing.T) {
		if !outcome.FencePending || outcome.OldCaptureID != wrongOldID || outcome.NewCaptureID != wrongNewID ||
			!outcome.BlockingCorrelationPoison || outcome.CorrelatedEvent || outcome.CorrelatedEffect || outcome.LaterMaterialized {
			t.Fatalf("capture-key mismatch did not block complete fence correlation: %#v", outcome)
		}
	})
}

// TestRealIssue49ResetCoversEveryFenceOperation proves the complete operation
// and registration-kind coverage clauses of SYNC-WAL-011.
func TestRealIssue49ResetCoversEveryFenceOperation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)

	syncedUpdateID := "00000000-0000-4000-8e05-000000000001"
	syncedDeleteID := "00000000-0000-4000-8e05-000000000002"
	syncedInsertID := "00000000-0000-4000-8e05-000000000003"
	documentIDs := []string{
		"00000000-0000-4000-8e05-000000000011",
		"00000000-0000-4000-8e05-000000000012",
		"00000000-0000-4000-8e05-000000000013",
	}
	accessUpdateID := "00000000-0000-4000-8e05-000000000021"
	accessDeleteID := "00000000-0000-4000-8e05-000000000022"
	accessInsertID := "00000000-0000-4000-8e05-000000000023"

	setup, err := harness.Source().BeginTx(ctx)
	if err != nil {
		t.Fatalf("begin reset operation setup: %v", err)
	}
	for _, recordID := range []string{syncedUpdateID, syncedDeleteID} {
		if _, err := setup.ExecContext(
			ctx,
			"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'issue49-reset-operation-base')",
			recordID,
		); err != nil {
			_ = setup.Rollback()
			t.Fatalf("insert reset synced operation base: %v", err)
		}
	}
	for _, documentID := range documentIDs {
		if _, err := setup.ExecContext(
			ctx,
			"INSERT INTO cf_documents (id, owner_id, title) VALUES ($1, 'diagnostic-user', 'issue49-reset-operation-document')",
			documentID,
		); err != nil {
			_ = setup.Rollback()
			t.Fatalf("insert reset capture document: %v", err)
		}
	}
	for index, accessID := range []string{accessUpdateID, accessDeleteID} {
		if _, err := setup.ExecContext(
			ctx,
			"INSERT INTO cf_document_access (id, document_id, owner_id) VALUES ($1, $2, 'diagnostic-user')",
			accessID,
			documentIDs[index],
		); err != nil {
			_ = setup.Rollback()
			t.Fatalf("insert reset capture operation base: %v", err)
		}
	}
	if err := setup.Commit(); err != nil {
		t.Fatalf("commit reset operation setup: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", syncedUpdateID, syncedDeleteID)
	waitForRealWALRecords(t, ctx, harness, "cf_documents", documentIDs...)
	for _, accessID := range []string{accessUpdateID, accessDeleteID} {
		if !waitForIssue49CaptureDependencyFence(t, ctx, admin, accessID) {
			t.Fatalf("reset capture operation base %s did not materialize", accessID)
		}
	}

	if err := harness.Operator().InjectRegisteredTruncate(ctx); err != nil {
		t.Fatalf("commit reset operation poison: %v", err)
	}
	statements := []struct {
		query     string
		arguments []any
	}{
		{"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'issue49-reset-insert')", []any{syncedInsertID}},
		{"UPDATE cf_items SET value = 'issue49-reset-update', updated_at = clock_timestamp() WHERE id = $1", []any{syncedUpdateID}},
		{"DELETE FROM cf_items WHERE id = $1", []any{syncedDeleteID}},
		{"INSERT INTO cf_document_access (id, document_id, owner_id) VALUES ($1, $2, 'diagnostic-user')", []any{accessInsertID, documentIDs[2]}},
		{"UPDATE cf_document_access SET owner_id = 'issue49-reset-updated-owner' WHERE id = $1", []any{accessUpdateID}},
		{"DELETE FROM cf_document_access WHERE id = $1", []any{accessDeleteID}},
	}
	for index, statement := range statements {
		if err := harness.Source().ExecContext(ctx, statement.query, statement.arguments...); err != nil {
			t.Fatalf("commit pending reset operation %d: %v", index+1, err)
		}
	}
	waitForIssue49Poison(t, ctx, harness, syncedInsertID)
	expectedFenceIDs, combinations := loadIssue49PendingOperationFences(
		t,
		ctx,
		admin,
		[]string{syncedInsertID, syncedUpdateID, syncedDeleteID, accessInsertID, accessUpdateID, accessDeleteID},
	)
	reset, err := harness.Operator().RunStreamReset(ctx)
	if err != nil {
		t.Fatalf("run complete-operation stream reset: %v; %s", err, harness.FailureDiagnostics())
	}
	coverage := observeIssue49ResetFenceCoverage(t, ctx, admin, reset.ResetID, expectedFenceIDs)
	syntheticEvents, syntheticEffects := observeIssue49ResetSyntheticEffects(t, ctx, admin, expectedFenceIDs)

	t.Run("assertion", func(t *testing.T) {
		if len(expectedFenceIDs) != 6 || combinations != "capture_dependency:delete,capture_dependency:insert,capture_dependency:update,synced:delete,synced:insert,synced:update" {
			t.Fatalf("reset operation fence set is incomplete: count=%d combinations=%q", len(expectedFenceIDs), combinations)
		}
		if coverage.Staged != 6 || coverage.Covered != 6 || coverage.UniqueCovered != 6 ||
			!coverage.ExactFenceSet || coverage.MetadataMismatches != 0 || coverage.PendingExpected != 0 ||
			coverage.PendingRegistered != 0 || !coverage.SnapshotMarkersBounded || coverage.CoverageModes != "reset_baseline" ||
			syntheticEvents != 0 || syntheticEffects != 0 {
			t.Fatalf("reset omitted or fabricated operation fence coverage: coverage=%#v events=%d effects=%d", coverage, syntheticEvents, syntheticEffects)
		}
	})
}

// TestRealIssue49MembershipBackfillRetainsContinuationAcrossWorkerLoss proves
// the process-fault clauses of SYNC-MEMBERSHIP-002 and SYNC-MEMBERSHIP-003.
func TestRealIssue49MembershipBackfillRetainsContinuationAcrossWorkerLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	const sharedScope = "cf:issue49-complete-membership"
	if _, err := admin.ExecContext(ctx, "SELECT synchro.synchro_register_shared_scope($1, false)", sharedScope); err != nil {
		t.Fatalf("register complete membership scope: %v", err)
	}
	client := connectRealProtocolClient(
		t,
		ctx,
		harness,
		token,
		"issue49-complete-membership-client",
		"cf:global",
		sharedScope,
		"user:diagnostic-user",
	)
	if err := harness.Source().ExecContext(ctx, `
		INSERT INTO cf_items (id, owner_id, value)
		SELECT ('00000000-0000-4000-8e06-' || lpad(value::text, 12, '0'))::uuid,
		       'diagnostic-user', 'issue49-complete-membership-' || value::text
		FROM generate_series(1000, 2000) value`); err != nil {
		t.Fatalf("insert multi-batch membership source set: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", "00000000-0000-4000-8e06-000000002000")

	rebuildID := "00000000-0000-4000-8e06-000000000001"
	firstStatus, firstPage := requestRealRebuildPage(
		t, ctx, harness, token, client, "user:diagnostic-user", rebuildID, nil, 1,
	)
	if firstStatus != http.StatusOK || !issue49IntermediateRebuildPage(firstPage) {
		t.Fatalf("create prior-generation rebuild continuation: status=%d response=%#v", firstStatus, firstPage)
	}
	firstCursor := issue49RebuildCursor(firstPage)
	var priorRegistry, priorOwnerMembership, priorSharedMembership int64
	if err := admin.QueryRowContext(ctx, `
		SELECT generation, owner.membership_generation, shared.membership_generation
		FROM synchro.sync_registry_generations generation
		CROSS JOIN synchro.sync_scope_state owner
		CROSS JOIN synchro.sync_scope_state shared
		WHERE generation.state = 'active'
		  AND owner.scope_id = 'user:diagnostic-user'
		  AND shared.scope_id = $1`, sharedScope).Scan(
		&priorRegistry,
		&priorOwnerMembership,
		&priorSharedMembership,
	); err != nil {
		t.Fatalf("observe prior membership continuation generations: %v", err)
	}

	startupBarrier, originalWorkerPID := beginIssue49StoppedWALWorker(t, ctx, harness, admin)
	defer startupBarrier.Rollback()
	if _, err := admin.ExecContext(ctx, `
		CREATE OR REPLACE FUNCTION public.cf_items_membership(p_id uuid)
		RETURNS SETOF text
		LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
		BEGIN ATOMIC
			SELECT 'user:' || (p.owner_id #>> '{}')
			FROM synchro_projection.cf_items AS p
			WHERE p.record_id = p_id::text AND NOT p.deleted
			UNION ALL
			SELECT 'cf:issue49-complete-membership'
			FROM synchro_projection.cf_items AS p
			WHERE p.record_id = p_id::text AND NOT p.deleted;
		END`); err != nil {
		t.Fatalf("install complete multi-scope membership rule: %v", err)
	}
	if _, err := admin.ExecContext(ctx, `
		SELECT synchro.synchro_register_table(
			'public.cf_items', 'public.cf_items_membership', 'multi_scope',
			'id', 'updated_at', 'deleted_at', 'enabled',
			p_affected_scopes => ARRAY['user:diagnostic-user', $1]::text[]
		)`, sharedScope); err != nil {
		t.Fatalf("stage complete multi-scope membership transition: %v", err)
	}
	var registryGeneration int64
	if err := admin.QueryRowContext(ctx, `
		SELECT max(registry_generation)
		FROM synchro.sync_registry_membership_stages
		WHERE state = 'pending'`).Scan(&registryGeneration); err != nil || registryGeneration <= priorRegistry {
		t.Fatalf("observe complete membership stage: generation=%d prior=%d err=%v", registryGeneration, priorRegistry, err)
	}

	pendingBeforeLoss := observeIssue49PendingMembershipContinuation(
		t, ctx, startupBarrier, registryGeneration, priorRegistry, sharedScope, rebuildID,
	)
	continuationStatus, continuationResponse := requestRealRebuildPage(
		t, ctx, harness, token, client, "user:diagnostic-user", rebuildID, firstCursor, 1,
	)
	secondCursor := issue49RebuildCursor(continuationResponse)
	if secondCursor == "" {
		secondCursor = firstCursor
	}

	activationBarrier, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin membership activation barrier: %v", err)
	}
	defer activationBarrier.Rollback()
	if _, err := activationBarrier.ExecContext(ctx, `
		SELECT 1 FROM synchro.sync_registry_membership_stages
		WHERE registry_generation = $1 FOR UPDATE`, registryGeneration); err != nil {
		t.Fatalf("lock membership activation boundary: %v", err)
	}
	if err := startupBarrier.Commit(); err != nil {
		t.Fatalf("release membership worker startup barrier: %v", err)
	}
	startupBarrier = nil

	firstBlockedWorker := waitForIssue49WorkerLock(t, ctx, harness, admin, originalWorkerPID, "")
	walBarrier, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin membership worker-restart barrier: %v", err)
	}
	defer walBarrier.Rollback()
	var walBarrierPID int
	if err := walBarrier.QueryRowContext(ctx, "SELECT pg_catalog.pg_backend_pid()").Scan(&walBarrierPID); err != nil {
		t.Fatalf("observe membership worker-restart barrier: %v", err)
	}
	walLockResult := make(chan error, 1)
	go func() {
		_, lockErr := walBarrier.ExecContext(ctx, "LOCK TABLE synchro.sync_wal_transactions IN ACCESS EXCLUSIVE MODE")
		walLockResult <- lockErr
	}()
	lockDeadline := time.Now().Add(30 * time.Second)
	var walLockQueued bool
	var walLockErr error
	for time.Now().Before(lockDeadline) {
		walLockErr = admin.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_catalog.pg_locks lock
				JOIN pg_catalog.pg_class relation ON relation.oid = lock.relation
				JOIN pg_catalog.pg_namespace namespace ON namespace.oid = relation.relnamespace
				WHERE lock.pid = $1 AND NOT lock.granted
				  AND namespace.nspname = 'synchro' AND relation.relname = 'sync_wal_transactions'
			)`, walBarrierPID).Scan(&walLockQueued)
		if walLockErr == nil && walLockQueued {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !walLockQueued {
		t.Fatalf("membership worker-restart barrier did not queue: pid=%d err=%v", walBarrierPID, walLockErr)
	}
	var terminated bool
	if err := admin.QueryRowContext(ctx, "SELECT pg_catalog.pg_terminate_backend($1)", firstBlockedWorker).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate membership backfill worker: terminated=%t err=%v", terminated, err)
	}
	if err := <-walLockResult; err != nil {
		t.Fatalf("acquire membership worker-restart barrier: %v", err)
	}
	secondBlockedWorker := waitForIssue49WorkerLock(
		t, ctx, harness, admin, firstBlockedWorker, "sync_wal_transactions",
	)
	pendingAfterLoss := observeIssue49PendingMembershipContinuation(
		t, ctx, walBarrier, registryGeneration, priorRegistry, sharedScope, rebuildID,
	)
	restartedStatus, restartedResponse := requestRealRebuildPage(
		t, ctx, harness, token, client, "user:diagnostic-user", rebuildID, secondCursor, 1,
	)
	thirdCursor := issue49RebuildCursor(restartedResponse)
	if thirdCursor == "" {
		thirdCursor = secondCursor
	}

	if err := walBarrier.Commit(); err != nil {
		t.Fatalf("release membership worker-restart boundary: %v", err)
	}
	walBarrier = nil
	if err := activationBarrier.Commit(); err != nil {
		t.Fatalf("release membership activation boundary: %v", err)
	}
	activationBarrier = nil
	waitForIssue49MembershipStage(t, ctx, admin, registryGeneration)
	activated := observeIssue49ActivatedMembershipContinuation(t, ctx, admin, registryGeneration, priorRegistry, sharedScope, rebuildID)
	client.Schema = loadRealSchemaTableReference(t, ctx, harness, "cf_items").Schema
	staleStatus, staleResponse := requestRealRebuildPage(
		t, ctx, harness, token, client, "user:diagnostic-user", rebuildID, thirdCursor, 1,
	)

	t.Run("assertion", func(t *testing.T) {
		if firstBlockedWorker == originalWorkerPID || secondBlockedWorker == firstBlockedWorker {
			t.Fatalf("membership process fault did not restart the WAL worker: original=%d first=%d second=%d", originalWorkerPID, firstBlockedWorker, secondBlockedWorker)
		}
		for phase, observation := range map[string]issue49MembershipContinuationObservation{
			"before_worker_loss": pendingBeforeLoss,
			"after_worker_loss":  pendingAfterLoss,
		} {
			if observation.StageState != "pending" || !observation.PriorRegistryActive ||
				observation.OwnerMembership != priorOwnerMembership || observation.SharedMembership != priorSharedMembership ||
				observation.SourceRows != 1001 || observation.OwnerEdges != 1001 || observation.SharedEdges != 0 ||
				!observation.RebuildSessionPresent {
				t.Fatalf("%s exposed partial membership or discarded prior continuation: %#v", phase, observation)
			}
		}
		if continuationStatus != http.StatusOK || !issue49IntermediateRebuildPage(continuationResponse) ||
			restartedStatus != http.StatusOK || !issue49IntermediateRebuildPage(restartedResponse) {
			t.Fatalf("prior-generation continuation did not survive backfill and worker loss: first=%d %#v restarted=%d %#v", continuationStatus, continuationResponse, restartedStatus, restartedResponse)
		}
		if activated.StageState != "activated" || !activated.Verified || activated.StagedRecords != 1001 ||
			activated.StagedEdges != 2002 || activated.OwnerEdges != 1001 || activated.SharedEdges != 1001 ||
			activated.OwnerMembership != priorOwnerMembership+1 || activated.SharedMembership != priorSharedMembership+1 ||
			activated.PriorRegistryState != "superseded" || activated.RebuildSessionPresent {
			t.Fatalf("membership activation was not complete, scoped, and atomic: %#v", activated)
		}
		assertIssue49ProtocolError(t, staleStatus, staleResponse, http.StatusConflict, "rebuild_restart_required", false)
	})
}

type issue49CompleteWALRepresentation struct {
	Fences             int64
	Transactions       int64
	Events             int64
	Projections        int64
	CapturedRows       int64
	Edges              int64
	Changes            int64
	DeclaredEvents     int64
	DeclaredEffects    int64
	Acknowledged       bool
	OneSourceIdentity  bool
	ConsistentVersions bool
}

type issue49FenceMismatchOutcome struct {
	FencePending              bool
	OldRecordID               string
	NewRecordID               string
	OldCaptureID              string
	NewCaptureID              string
	BlockingCorrelationPoison bool
	CorrelatedEvent           bool
	CorrelatedEffect          bool
	LaterMaterialized         bool
}

type issue49MembershipContinuationObservation struct {
	StageState            string
	PriorRegistryActive   bool
	OwnerMembership       int64
	SharedMembership      int64
	SourceRows            int64
	OwnerEdges            int64
	SharedEdges           int64
	RebuildSessionPresent bool
}

type issue49ActivatedMembershipObservation struct {
	StageState            string
	Verified              bool
	StagedRecords         int64
	StagedEdges           int64
	OwnerMembership       int64
	SharedMembership      int64
	OwnerEdges            int64
	SharedEdges           int64
	PriorRegistryState    string
	RebuildSessionPresent bool
}

func waitForIssue49CompleteWALRepresentation(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	recordIDs []string,
) issue49CompleteWALRepresentation {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var result issue49CompleteWALRepresentation
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = database.QueryRowContext(ctx, `
			WITH relation AS (
				SELECT registry.relation_id
				FROM synchro.sync_registry registry
				JOIN synchro.sync_registry_generations generation
				  ON generation.generation = registry.registry_generation
				WHERE generation.state = 'active' AND registry.table_name = 'cf_items'
			), fences AS (
				SELECT fence.* FROM synchro.sync_write_fences fence
				JOIN relation USING (relation_id)
				WHERE fence.new_record_id = ANY($1)
			), transactions AS (
				SELECT transaction.*
				FROM synchro.sync_wal_transactions transaction
				WHERE EXISTS (
					SELECT 1 FROM fences fence
					WHERE (fence.transaction_xid::text::numeric % 4294967296) = transaction.source_xid::text::numeric
				)
			)
			SELECT (SELECT count(*) FROM fences),
			       (SELECT count(*) FROM transactions),
			       (SELECT count(*) FROM synchro.sync_wal_events event JOIN fences fence USING (fence_id)),
			       (SELECT count(*) FROM synchro.sync_captured_projections projection JOIN relation USING (relation_id) WHERE projection.record_id = ANY($1)),
			       (SELECT count(*) FROM synchro.sync_captured_rows captured JOIN relation USING (relation_id) WHERE captured.record_id = ANY($1)),
			       (SELECT count(*) FROM synchro.sync_bucket_edges edge JOIN relation USING (relation_id) WHERE edge.record_id = ANY($1)),
			       (SELECT count(*) FROM synchro.sync_changelog change JOIN relation USING (relation_id) WHERE change.record_id = ANY($1)),
			       COALESCE((SELECT sum(event_count) FROM transactions), 0),
			       COALESCE((SELECT sum(effect_count) FROM transactions), 0),
			       COALESCE((SELECT progress.acknowledged_end_lsn >= (SELECT max(end_lsn) FROM transactions) FROM synchro.sync_wal_progress progress WHERE progress.singleton), false),
			       (SELECT count(DISTINCT source_xid::text) = 1 FROM transactions),
			       NOT EXISTS (
					SELECT 1 FROM synchro.sync_wal_events event
					JOIN fences fence USING (fence_id)
					LEFT JOIN synchro.sync_changelog change
					  ON change.stream_generation = event.stream_generation
					 AND change.commit_lsn = event.commit_lsn
					 AND change.event_ordinal = event.event_ordinal
					 AND change.relation_id = event.relation_id
					 AND change.record_id = fence.new_record_id
					 AND change.row_version = fence.row_version
					WHERE change.seq IS NULL
			       )`, recordIDs).Scan(
			&result.Fences,
			&result.Transactions,
			&result.Events,
			&result.Projections,
			&result.CapturedRows,
			&result.Edges,
			&result.Changes,
			&result.DeclaredEvents,
			&result.DeclaredEffects,
			&result.Acknowledged,
			&result.OneSourceIdentity,
			&result.ConsistentVersions,
		)
		if lastErr == nil && result.Transactions == 1 && result.Acknowledged {
			return result
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("complete WAL representation did not reach durable acknowledgement: observation=%#v err=%v", result, lastErr)
	return issue49CompleteWALRepresentation{}
}

func beginIssue49StoppedWALWorker(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
	database *sql.DB,
) (*sql.Tx, int) {
	t.Helper()
	barrier, err := database.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin WAL worker startup barrier: %v", err)
	}
	if _, err := barrier.ExecContext(ctx, "LOCK TABLE synchro.sync_wal_worker_state IN ACCESS EXCLUSIVE MODE"); err != nil {
		_ = barrier.Rollback()
		t.Fatalf("lock WAL worker startup boundary: %v", err)
	}
	workerPID, err := harness.Operator().CurrentWALWorkerPID(ctx)
	if err != nil {
		_ = barrier.Rollback()
		t.Fatalf("observe current WAL worker: %v", err)
	}
	var terminated bool
	if err := database.QueryRowContext(ctx, "SELECT pg_catalog.pg_terminate_backend($1)", workerPID).Scan(&terminated); err != nil || !terminated {
		_ = barrier.Rollback()
		t.Fatalf("stop WAL worker: terminated=%t err=%v", terminated, err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var present bool
		if err := database.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid = $1)", workerPID).Scan(&present); err != nil {
			_ = barrier.Rollback()
			t.Fatalf("observe stopped WAL worker: %v", err)
		}
		if !present {
			return barrier, workerPID
		}
		time.Sleep(25 * time.Millisecond)
	}
	_ = barrier.Rollback()
	t.Fatal("WAL worker did not stop at startup barrier")
	return nil, 0
}

func waitForIssue49FenceMismatchOutcome(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	fenceID string,
	laterRecordID string,
) issue49FenceMismatchOutcome {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var result issue49FenceMismatchOutcome
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = database.QueryRowContext(ctx, `
			SELECT fence.coverage = 'pending',
			       COALESCE(fence.old_record_id, ''),
			       COALESCE(fence.new_record_id, ''),
			       COALESCE(fence.old_capture_key->>'id', ''),
			       COALESCE(fence.new_capture_key->>'id', ''),
			       EXISTS (
					SELECT 1 FROM synchro.sync_wal_poison poison
					WHERE poison.lifecycle = 'active' AND poison.failure_class = 'fence_correlation_failed'
			       ),
			       EXISTS (SELECT 1 FROM synchro.sync_wal_events event WHERE event.fence_id = fence.fence_id),
			       EXISTS (
					SELECT 1 FROM synchro.sync_wal_events event
					JOIN synchro.sync_changelog change
					  ON change.stream_generation = event.stream_generation
					 AND change.commit_lsn = event.commit_lsn
					 AND change.event_ordinal = event.event_ordinal
					 AND change.relation_id = event.relation_id
					WHERE event.fence_id = fence.fence_id
			       ),
			       EXISTS (SELECT 1 FROM synchro.sync_changelog change WHERE change.record_id = $2)
			FROM synchro.sync_write_fences fence
			WHERE fence.fence_id = $1::uuid`, fenceID, laterRecordID).Scan(
			&result.FencePending,
			&result.OldRecordID,
			&result.NewRecordID,
			&result.OldCaptureID,
			&result.NewCaptureID,
			&result.BlockingCorrelationPoison,
			&result.CorrelatedEvent,
			&result.CorrelatedEffect,
			&result.LaterMaterialized,
		)
		if lastErr == nil && (result.BlockingCorrelationPoison || result.CorrelatedEvent || result.LaterMaterialized) {
			return result
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("fence mismatch produced no terminal observation: observation=%#v err=%v", result, lastErr)
	return issue49FenceMismatchOutcome{}
}

func loadIssue49PendingOperationFences(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	recordIDs []string,
) ([]string, string) {
	t.Helper()
	var encoded, combinations string
	if err := database.QueryRowContext(ctx, `
		WITH selected AS (
			SELECT fence_id::text, registration_kind, operation
			FROM synchro.sync_write_fences
			WHERE coverage = 'pending'
			  AND (
				old_record_id = ANY($1) OR new_record_id = ANY($1)
				OR old_capture_key->>'id' = ANY($1) OR new_capture_key->>'id' = ANY($1)
			  )
		)
		SELECT COALESCE(pg_catalog.array_to_json(array_agg(fence_id ORDER BY fence_id))::text, '[]'),
		       COALESCE(string_agg(registration_kind || ':' || operation, ',' ORDER BY registration_kind || ':' || operation), '')
		FROM selected`, recordIDs).Scan(&encoded, &combinations); err != nil {
		t.Fatalf("load complete reset operation fence set: %v", err)
	}
	var fenceIDs []string
	if err := json.Unmarshal([]byte(encoded), &fenceIDs); err != nil {
		t.Fatalf("decode complete reset operation fence set: %v", err)
	}
	return fenceIDs, combinations
}

func observeIssue49ResetSyntheticEffects(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	fenceIDs []string,
) (int64, int64) {
	t.Helper()
	var events, effects int64
	if err := database.QueryRowContext(ctx, `
		WITH expected AS (SELECT value::uuid AS fence_id FROM unnest($1::text[]) value),
		events AS (
			SELECT event.* FROM synchro.sync_wal_events event JOIN expected USING (fence_id)
		)
		SELECT (SELECT count(*) FROM events),
		       (SELECT count(*) FROM synchro.sync_changelog change
		        JOIN events event
		          ON event.stream_generation = change.stream_generation
		         AND event.commit_lsn = change.commit_lsn
		         AND event.event_ordinal = change.event_ordinal
		         AND event.relation_id = change.relation_id)`, fenceIDs).Scan(&events, &effects); err != nil {
		t.Fatalf("observe reset synthetic WAL effects: %v", err)
	}
	return events, effects
}

func waitForIssue49WorkerLock(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
	database *sql.DB,
	previousPID int,
	tableName string,
) int {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var lastPID int
	var lastErr error
	for time.Now().Before(deadline) {
		lastPID, lastErr = harness.Operator().CurrentWALWorkerPID(ctx)
		if lastErr == nil && lastPID != previousPID {
			var blocked bool
			lastErr = database.QueryRowContext(ctx, `
				SELECT EXISTS (
					SELECT 1 FROM pg_catalog.pg_locks lock
					LEFT JOIN pg_catalog.pg_class relation ON relation.oid = lock.relation
					LEFT JOIN pg_catalog.pg_namespace namespace ON namespace.oid = relation.relnamespace
					WHERE lock.pid = $1 AND NOT lock.granted
					  AND ($2 = '' OR (namespace.nspname = 'synchro' AND relation.relname = $2))
				)`, lastPID, tableName).Scan(&blocked)
			if lastErr == nil && blocked {
				return lastPID
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("replacement WAL worker did not reach expected lock wait: table=%q previous=%d last=%d err=%v", tableName, previousPID, lastPID, lastErr)
	return 0
}

func observeIssue49PendingMembershipContinuation(
	t *testing.T,
	ctx context.Context,
	queryer issue49SQLQueryer,
	registryGeneration int64,
	priorRegistry int64,
	sharedScope string,
	rebuildID string,
) issue49MembershipContinuationObservation {
	t.Helper()
	var result issue49MembershipContinuationObservation
	if err := queryer.QueryRowContext(ctx, `
		SELECT stage.state,
		       EXISTS (SELECT 1 FROM synchro.sync_registry_generations WHERE generation = $2 AND state = 'active'),
		       owner.membership_generation,
		       shared.membership_generation,
		       (SELECT count(*) FROM public.cf_items WHERE deleted_at IS NULL),
		       (SELECT count(*) FROM synchro.sync_bucket_edges WHERE table_name = 'cf_items' AND bucket_id = 'user:diagnostic-user'),
		       (SELECT count(*) FROM synchro.sync_bucket_edges WHERE table_name = 'cf_items' AND bucket_id = $3),
		       EXISTS (SELECT 1 FROM synchro.sync_rebuild_sessions WHERE rebuild_id = $4::uuid)
		FROM synchro.sync_registry_membership_stages stage
		CROSS JOIN synchro.sync_scope_state owner
		CROSS JOIN synchro.sync_scope_state shared
		WHERE stage.registry_generation = $1
		  AND owner.scope_id = 'user:diagnostic-user'
		  AND shared.scope_id = $3`, registryGeneration, priorRegistry, sharedScope, rebuildID).Scan(
		&result.StageState,
		&result.PriorRegistryActive,
		&result.OwnerMembership,
		&result.SharedMembership,
		&result.SourceRows,
		&result.OwnerEdges,
		&result.SharedEdges,
		&result.RebuildSessionPresent,
	); err != nil {
		t.Fatalf("observe pending membership continuation: %v", err)
	}
	return result
}

func waitForIssue49MembershipStage(t *testing.T, ctx context.Context, database *sql.DB, registryGeneration int64) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	var state string
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = database.QueryRowContext(ctx, `
			SELECT state FROM synchro.sync_registry_membership_stages
			WHERE registry_generation = $1`, registryGeneration).Scan(&state)
		if lastErr == nil && state == "activated" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("membership stage did not activate: generation=%d state=%q err=%v", registryGeneration, state, lastErr)
}

func observeIssue49ActivatedMembershipContinuation(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	registryGeneration int64,
	priorRegistry int64,
	sharedScope string,
	rebuildID string,
) issue49ActivatedMembershipObservation {
	t.Helper()
	var result issue49ActivatedMembershipObservation
	if err := database.QueryRowContext(ctx, `
		SELECT stage.state, stage.verified, stage.staged_record_count, stage.staged_edge_count,
		       owner.membership_generation, shared.membership_generation,
		       (SELECT count(*) FROM synchro.sync_bucket_edges WHERE table_name = 'cf_items' AND bucket_id = 'user:diagnostic-user'),
		       (SELECT count(*) FROM synchro.sync_bucket_edges WHERE table_name = 'cf_items' AND bucket_id = $3),
		       (SELECT state FROM synchro.sync_registry_generations WHERE generation = $2),
		       EXISTS (SELECT 1 FROM synchro.sync_rebuild_sessions WHERE rebuild_id = $4::uuid)
		FROM synchro.sync_registry_membership_stages stage
		CROSS JOIN synchro.sync_scope_state owner
		CROSS JOIN synchro.sync_scope_state shared
		WHERE stage.registry_generation = $1
		  AND owner.scope_id = 'user:diagnostic-user'
		  AND shared.scope_id = $3`, registryGeneration, priorRegistry, sharedScope, rebuildID).Scan(
		&result.StageState,
		&result.Verified,
		&result.StagedRecords,
		&result.StagedEdges,
		&result.OwnerMembership,
		&result.SharedMembership,
		&result.OwnerEdges,
		&result.SharedEdges,
		&result.PriorRegistryState,
		&result.RebuildSessionPresent,
	); err != nil {
		t.Fatalf("observe activated membership continuation: %v", err)
	}
	return result
}

func issue49IntermediateRebuildPage(response map[string]any) bool {
	records, ok := response["records"].([]any)
	return ok && len(records) == 1 && response["has_more"] == true && issue49RebuildCursor(response) != ""
}

func issue49RebuildCursor(response map[string]any) string {
	cursor, _ := response["cursor"].(string)
	return cursor
}
