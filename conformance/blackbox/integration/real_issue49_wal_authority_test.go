package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/trainstar/synchro/conformance/blackbox"
)

// TestRealIssue49WALIsTheOnlyAtomicPublicationPath proves SYNC-WAL-001,
// SYNC-WAL-002, and SYNC-WAL-004 at the server boundary.
func TestRealIssue49WALIsTheOnlyAtomicPublicationPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-wal-authority")
	rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8d01-000000000001")
	rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8d01-000000000002")
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")
	admin := openIssue49Admin(t, ctx, harness)

	lock, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin WAL publication barrier: %v", err)
	}
	defer lock.Rollback()
	if _, err := lock.ExecContext(ctx, "LOCK TABLE synchro.sync_wal_transactions IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatalf("lock WAL publication boundary: %v", err)
	}

	directID := "00000000-0000-4000-8d01-000000000011"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		directID,
		"diagnostic-user",
		"issue49-direct-wal-only",
	); err != nil {
		t.Fatalf("commit direct source write behind WAL barrier: %v", err)
	}
	pushID := "00000000-0000-4000-8d01-000000000012"
	pushStatus, pushResponse := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		"00000000-0000-4000-8d01-000000000013",
		[]map[string]any{phase4InsertMutation(
			client,
			table,
			ownerField,
			"00000000-0000-4000-8d01-000000000014",
			pushID,
			"issue49-push-wal-only",
		)},
	))
	if pushStatus != http.StatusOK || len(requireOutcomeList(t, pushResponse, "accepted")) != 1 {
		t.Fatalf("accepted push did not commit behind WAL barrier: status=%d response=%#v", pushStatus, pushResponse)
	}

	before := observeIssue49WALStages(t, ctx, lock, []string{directID, pushID})
	if err := lock.Commit(); err != nil {
		t.Fatalf("release WAL publication barrier: %v", err)
	}
	lock = nil
	waitForRealWALRecords(t, ctx, harness, "cf_items", directID, pushID)
	after := observeIssue49WALStages(t, ctx, admin, []string{directID, pushID})
	pullUntilRealRecords(t, ctx, harness, token, client, []realRecordExpectation{
		{scopeID: "user:diagnostic-user", table: table, recordID: directID, value: "issue49-direct-wal-only"},
		{scopeID: "user:diagnostic-user", table: table, recordID: pushID, value: "issue49-push-wal-only"},
	})

	replayID := "00000000-0000-4000-8d01-000000000021"
	replay, err := harness.Operator().RunWALReplayRestartControl(ctx, replayID)
	if err != nil {
		t.Fatalf("interrupt WAL replay after durable materialization: %v; %s", err, harness.FailureDiagnostics())
	}
	pullUntilRealRecords(t, ctx, harness, token, client, []realRecordExpectation{{
		scopeID: "user:diagnostic-user", table: table, recordID: replayID, value: "restart-before-acknowledgement",
	}})

	t.Run("assertion", func(t *testing.T) {
		if before != (issue49WALStageCounts{Fences: 2, PendingFences: 2}) {
			t.Fatalf("writes became visible before WAL replay: %#v", before)
		}
		if after != (issue49WALStageCounts{Fences: 2, Events: 2, Captured: 2, Edges: 2, Changes: 2}) {
			t.Fatalf("WAL replay did not publish exactly one complete result per write: %#v", after)
		}
		if !replay.WorkerExitedBeforeAcknowledgement || !replay.WorkerRestarted || len(replay.BeforeRestart.Records) != 1 ||
			len(replay.AfterRestart.Records) != 1 || replay.BeforeRestart.ContiguousAcknowledged ||
			!replay.AfterRestart.ContiguousAcknowledged || !replay.AfterRestart.AcknowledgementMatchesObservedEnd ||
			!replay.AfterRestart.SlotMatchesObservedEnd || replay.AfterRestart.Records[0].ReplayCount != 1 ||
			replay.BeforeStages != replay.AfterStages {
			t.Fatalf("interrupted WAL replay was not atomic and idempotent: %#v", replay)
		}
	})
}

// TestRealIssue49WALPoisonBlocksContiguousProgress proves SYNC-WAL-001,
// SYNC-WAL-005, SYNC-WAL-008, and SYNC-HEALTH-001. It also checks redaction
// on the diagnostic and readiness surfaces that this test can observe.
func TestRealIssue49WALPoisonBlocksContiguousProgress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	connectRealProtocolClient(t, ctx, harness, token, "issue49-poison-client")
	prefixID := "00000000-0000-4000-8d02-000000000000"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'issue49-contiguous-prefix')",
		prefixID,
	); err != nil {
		t.Fatalf("commit known contiguous WAL prefix: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", prefixID)
	prefixPipeline, err := harness.Operator().ObserveWALRecords(ctx, []string{prefixID})
	if err != nil || len(prefixPipeline.Records) != 1 || !prefixPipeline.AcknowledgementMatchesObservedEnd ||
		!prefixPipeline.SlotMatchesObservedEnd {
		t.Fatalf("establish exact contiguous WAL prefix: observation=%#v err=%v", prefixPipeline, err)
	}
	prefixEndLSN := prefixPipeline.Records[0].EndLSN

	poisonID := "00000000-0000-4000-8d02-000000000001"
	if err := harness.Operator().InjectDecoderMetadataChange(ctx, poisonID); err != nil {
		t.Fatalf("commit Issue 49 decoder poison: %v", err)
	}
	laterID := "00000000-0000-4000-8d02-000000000002"
	privateValue := "issue49-private-value-6a50362d"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		laterID,
		"diagnostic-user",
		privateValue,
	); err != nil {
		t.Fatalf("commit source write after Issue 49 poison: %v", err)
	}
	before := waitForIssue49Poison(t, ctx, harness, laterID)
	beforeAcknowledgement := observeIssue49BlockedAcknowledgement(t, ctx, openIssue49Admin(t, ctx, harness), before.CommitLSN)

	readyStatus, readyBody := getIssue49Readiness(t, ctx, harness.AdapterURL())
	detail := loadIssue49Health(t, ctx, openIssue49Admin(t, ctx, harness))
	walDiagnostic, err := harness.Operator().WALDiagnostics(ctx)
	if err != nil {
		t.Fatalf("load redacted WAL diagnostics: %v", err)
	}
	invalidToken := "issue49-private-token-141b366f"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, harness.AdapterURL()+"/sync/pull", strings.NewReader(`{"private":"issue49-private-body-f3177b27"}`))
	if err != nil {
		t.Fatalf("create redaction control request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+invalidToken)
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 30 * time.Second}).Do(request)
	if err != nil {
		t.Fatalf("send redaction control request: %v", err)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
	_ = response.Body.Close()
	diagnostics := harness.FailureDiagnostics()

	if err := harness.RestartPostgres(ctx); err != nil {
		t.Fatalf("restart PostgreSQL with active poison: %v", err)
	}
	afterRestart := waitForIssue49Poison(t, ctx, harness, laterID)
	afterRestartAcknowledgement := observeIssue49BlockedAcknowledgement(t, ctx, openIssue49Admin(t, ctx, harness), afterRestart.CommitLSN)
	retried, err := harness.Operator().RetryWALPoison(ctx)
	if err != nil || !retried {
		t.Fatalf("request same-identity poison retry: requested=%t err=%v", retried, err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", poisonID, laterID)
	recovery, err := harness.Operator().ObserveWALPoisonRecovery(ctx, poisonID)
	if err != nil {
		t.Fatalf("observe Issue 49 poison recovery: %v", err)
	}
	recoveredPipeline, err := harness.Operator().ObserveWALRecords(ctx, []string{poisonID, laterID})
	if err != nil {
		t.Fatalf("observe recovered contiguous WAL acknowledgement: %v", err)
	}
	waitForIssue49PublicReady(t, ctx, harness.AdapterURL(), true)

	t.Run("assertion", func(t *testing.T) {
		if before.FailureClass != "decode_failed" || before.CommitLSN == "" || !before.AcknowledgementBlocked ||
			before.LaterRecordMaterialized || !before.LaterFencePending || !before.WorkerBlocked ||
			!before.ReadinessBlocked || !before.PoisonCheckFailed || before.WALLagSeconds < 0 ||
			afterRestart.CommitLSN != before.CommitLSN || !afterRestart.AcknowledgementBlocked ||
			afterRestart.LaterRecordMaterialized || !afterRestart.LaterFencePending {
			t.Fatalf("poison did not block one contiguous durable prefix: before=%#v after=%#v", before, afterRestart)
		}
		if !beforeAcknowledgement.SlotMatchesProgress || !beforeAcknowledgement.ProgressBeforePoison ||
			!beforeAcknowledgement.SlotBeforePoison || beforeAcknowledgement.ProgressEndLSN == "" ||
			beforeAcknowledgement.ProgressEndLSN != prefixEndLSN || beforeAcknowledgement.SlotFlushLSN != prefixEndLSN ||
			afterRestartAcknowledgement != beforeAcknowledgement {
			t.Fatalf("logical slot advanced past the blocked durable prefix: before=%#v after=%#v", beforeAcknowledgement, afterRestartAcknowledgement)
		}
		if readyStatus != http.StatusServiceUnavailable || !bytes.Equal(readyBody, []byte(`{"ready":false}`)) {
			t.Fatalf("public readiness exposed invalid poison state: status=%d body=%q", readyStatus, readyBody)
		}
		checks := issue49HealthChecks(t, detail)
		if checks["poison"] != "failed" || detail["ready"] != false {
			t.Fatalf("canonical health did not identify blocking poison: %#v", detail)
		}
		for _, private := range []string{privateValue, invalidToken, "issue49-private-body-f3177b27", token} {
			if strings.Contains(walDiagnostic, private) || strings.Contains(diagnostics, private) || bytes.Contains(readyBody, []byte(private)) {
				t.Fatal("operational output disclosed private input")
			}
		}
		if recovery.PoisonCount != 1 || recovery.FailureClass != "decode_failed" || recovery.Lifecycle != "repaired" ||
			recovery.AttemptCount != 2 || !recovery.RetryRequested || !recovery.Resolved || !recovery.SameCommitPosition {
			t.Fatalf("poison recovery did not repair the same WAL identity: %#v", recovery)
		}
		if len(recoveredPipeline.Records) != 2 || !recoveredPipeline.ContiguousAcknowledged ||
			!recoveredPipeline.AcknowledgementMatchesObservedEnd || !recoveredPipeline.SlotMatchesObservedEnd ||
			recoveredPipeline.AcknowledgedEndLSN == "" ||
			recoveredPipeline.AcknowledgedEndLSN != recoveredPipeline.SlotConfirmedFlushLSN {
			t.Fatalf("logical slot did not acknowledge the exact recovered contiguous end LSN: %#v", recoveredPipeline)
		}
	})
}

// TestRealIssue49ResetLifecycleAndFenceCoverage proves SYNC-WAL-010 and
// SYNC-WAL-011 through process loss and an authoritative reset baseline.
func TestRealIssue49ResetLifecycleAndFenceCoverage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	createRealCheckpoint(t, ctx, harness)
	if err := harness.Operator().InjectRegisteredTruncate(ctx); err != nil {
		t.Fatalf("commit reset-triggering poison: %v", err)
	}
	baselineID := "00000000-0000-4000-8d03-000000000001"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		baselineID,
		"diagnostic-user",
		"issue49-reset-baseline",
	); err != nil {
		t.Fatalf("commit reset baseline row: %v", err)
	}
	secondBaselineID := "00000000-0000-4000-8d03-000000000002"
	documentID := "00000000-0000-4000-8d03-000000000003"
	accessID := "00000000-0000-4000-8d03-000000000004"
	for step, statement := range []struct {
		query     string
		arguments []any
	}{
		{"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'issue49-reset-second')", []any{secondBaselineID}},
		{"INSERT INTO cf_documents (id, owner_id, title) VALUES ($1, 'diagnostic-user', 'issue49-reset-document')", []any{documentID}},
		{"INSERT INTO cf_document_access (id, document_id, owner_id) VALUES ($1, $2, 'diagnostic-user')", []any{accessID, documentID}},
	} {
		if err := harness.Source().ExecContext(ctx, statement.query, statement.arguments...); err != nil {
			t.Fatalf("commit diverse reset baseline fence %d: %v", step+1, err)
		}
	}
	waitForIssue49Poison(t, ctx, harness, baselineID)

	interrupted, err := harness.Operator().CreateInterruptedStreamReset(ctx)
	if err != nil {
		t.Fatalf("create interrupted permanent candidate slot: %v", err)
	}
	if err := harness.RestartPostgres(ctx); err != nil {
		t.Fatalf("restart PostgreSQL during durable reset preparation: %v", err)
	}
	preparing, candidatePresent, err := harness.Operator().ObservePreparingReset(ctx)
	if err != nil || !preparing || !candidatePresent {
		t.Fatalf("reset lifecycle did not survive process loss: preparing=%t candidate=%t err=%v", preparing, candidatePresent, err)
	}
	if err := harness.Operator().RecoverInterruptedStreamReset(ctx); err != nil {
		t.Fatalf("discard interrupted pre-activation reset: %v", err)
	}
	preparing, candidatePresent, err = harness.Operator().ObservePreparingReset(ctx)
	if err != nil || preparing || candidatePresent {
		t.Fatalf("interrupted reset candidate remained active: preparing=%t candidate=%t err=%v", preparing, candidatePresent, err)
	}
	expectedFences, expectedKinds := loadIssue49PendingResetFences(t, ctx, openIssue49Admin(t, ctx, harness))

	reset, err := harness.Operator().RunStreamReset(ctx)
	if err != nil {
		t.Fatalf("run verified replacement reset: %v; %s", err, harness.FailureDiagnostics())
	}
	observation, err := harness.Operator().ObserveStreamReset(ctx, reset.ResetID, "cf_items", baselineID)
	if err != nil {
		t.Fatalf("observe reset baseline coverage: %v", err)
	}
	coverage := observeIssue49ResetFenceCoverage(t, ctx, openIssue49Admin(t, ctx, harness), reset.ResetID, expectedFences)

	t.Run("assertion", func(t *testing.T) {
		if interrupted.ResetID == reset.ResetID || interrupted.CandidateSlotName != reset.CandidateSlotName {
			t.Fatalf("reset recovery did not replace the interrupted lifecycle: interrupted=%#v reset=%#v", interrupted, reset)
		}
		if reset.SourceStreamGeneration == reset.TargetStreamGeneration || reset.OldSlotName == reset.CandidateSlotName ||
			observation.Lifecycle != "cleanup_complete" || observation.ActiveSlotName != reset.CandidateSlotName ||
			observation.ActiveStreamGeneration != reset.TargetStreamGeneration || !observation.OldSlotAbsent ||
			!observation.CandidateSlotValid || !observation.PoisonCleared || !observation.ReadinessReady {
			t.Fatalf("reset slot activation was not complete and atomic: reset=%#v observation=%#v", reset, observation)
		}
		if !observation.BaselineRecordPresent || !observation.BaselineProvenanceMatches ||
			!observation.BaselineMembershipPresent || observation.FenceCoverage != "reset_baseline" ||
			!observation.NoSyntheticEvent || !observation.NoSyntheticEffect || !observation.CheckpointsInvalidated {
			t.Fatalf("reset did not cover the accepted fence exactly once: %#v", observation)
		}
		if len(expectedFences) < 4 || expectedKinds != "capture_dependency,synced" ||
			coverage.Staged != int64(len(expectedFences)) || coverage.Covered != int64(len(expectedFences)) ||
			coverage.UniqueCovered != int64(len(expectedFences)) || !coverage.ExactFenceSet ||
			coverage.MetadataMismatches != 0 || coverage.PendingExpected != 0 || coverage.PendingRegistered != 0 ||
			!coverage.SnapshotMarkersBounded || coverage.CoverageModes != "reset_baseline" {
			t.Fatalf("reset did not cover every snapshot-era fence exactly once: expected=%d kinds=%q observation=%#v", len(expectedFences), expectedKinds, coverage)
		}
	})
}

// TestRealIssue49RegistryIdentityAndKeyDrift proves SYNC-REGISTRY-001 and
// SYNC-REGISTRY-002 against exact PostgreSQL catalog identity.
func TestRealIssue49RegistryIdentityAndKeyDrift(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)

	var registeredOID int64
	if err := admin.QueryRowContext(ctx, `
		SELECT registry.physical_relation_oid::bigint
		FROM synchro.sync_registry registry
		JOIN synchro.sync_registry_generations generation
		  ON generation.generation = registry.registry_generation
		WHERE generation.state = 'active' AND registry.table_name = 'cf_items'`).Scan(&registeredOID); err != nil || registeredOID <= 0 {
		t.Fatalf("load registered relation identity: oid=%d err=%v", registeredOID, err)
	}
	if _, err := admin.ExecContext(ctx, `
		CREATE SCHEMA issue49_shadow;
		CREATE TABLE issue49_shadow.cf_items (
			id uuid PRIMARY KEY,
			owner_id text NOT NULL,
			value text NOT NULL,
			updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
			deleted_at timestamptz
		);
		CREATE FUNCTION issue49_shadow.cf_items_membership(p_id uuid)
		RETURNS SETOF text LANGUAGE SQL STABLE SECURITY INVOKER
		SET search_path = pg_catalog
		BEGIN ATOMIC SELECT 'user:shadow'; END`); err != nil {
		t.Fatalf("create same-name shadow relation: %v", err)
	}
	connection, err := admin.Conn(ctx)
	if err != nil {
		t.Fatalf("open qualified-identity connection: %v", err)
	}
	if _, err := connection.ExecContext(ctx, "SET search_path = issue49_shadow, public, pg_catalog"); err != nil {
		_ = connection.Close()
		t.Fatalf("set hostile search path: %v", err)
	}
	shadowHealth := loadIssue49Health(t, ctx, connection)
	_ = connection.Close()

	recordID := "00000000-0000-4000-8d04-000000000001"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO public.cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		recordID,
		"diagnostic-user",
		"issue49-qualified-identity",
	); err != nil {
		t.Fatalf("write exact registered relation: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", recordID)
	beforePKUpdate := observeIssue49WALStages(t, ctx, admin, []string{recordID})
	changedID := "00000000-0000-4000-8d04-000000000002"
	if err := harness.Source().ExecContext(ctx, "UPDATE public.cf_items SET id = $2 WHERE id = $1", recordID, changedID); err == nil {
		t.Fatal("registered primary-key update succeeded")
	}
	afterPKUpdate := observeIssue49WALStages(t, ctx, admin, []string{recordID})
	changedStages := observeIssue49WALStages(t, ctx, admin, []string{changedID})

	if _, err := admin.ExecContext(ctx, `
		CREATE TABLE issue49_shadow.unsupported_keys (
			id numeric PRIMARY KEY,
			owner_id text NOT NULL,
			value text NOT NULL,
			updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
			deleted_at timestamptz
		);
		ALTER TABLE issue49_shadow.unsupported_keys ENABLE ROW LEVEL SECURITY;
		CREATE POLICY issue49_owner ON issue49_shadow.unsupported_keys
			AS PERMISSIVE FOR ALL TO synchro_owner USING (true) WITH CHECK (true);
		CREATE FUNCTION issue49_shadow.unsupported_keys_membership(p_id numeric)
		RETURNS SETOF text LANGUAGE SQL STABLE SECURITY INVOKER
		SET search_path = pg_catalog
		BEGIN ATOMIC SELECT 'user:unsupported'; END;
		ALTER TABLE issue49_shadow.unsupported_keys OWNER TO synchro_owner;
		ALTER FUNCTION issue49_shadow.unsupported_keys_membership(numeric) OWNER TO synchro_owner`); err != nil {
		t.Fatalf("create unsupported-key registration control: %v", err)
	}
	unsupportedRegistration := `SELECT synchro.synchro_register_table(
		'issue49_shadow.unsupported_keys',
		'issue49_shadow.unsupported_keys_membership',
		'single_scope', 'id', 'updated_at', 'deleted_at', 'enabled'
	)`
	if _, err := admin.ExecContext(ctx, unsupportedRegistration); err == nil {
		t.Fatal("nonportable primary key registration succeeded")
	}

	if _, err := admin.ExecContext(ctx, "ALTER TABLE public.cf_items REPLICA IDENTITY FULL"); err != nil {
		t.Fatalf("inject replica-identity drift: %v", err)
	}
	replicaDrift := loadIssue49Health(t, ctx, admin)
	if _, err := admin.ExecContext(ctx, "ALTER TABLE public.cf_items REPLICA IDENTITY DEFAULT"); err != nil {
		t.Fatalf("restore replica identity: %v", err)
	}
	waitForIssue49CanonicalHealth(t, ctx, admin, true)

	if _, err := admin.ExecContext(ctx, `
		ALTER TABLE public.cf_items RENAME TO cf_items_registered_oid;
		CREATE TABLE public.cf_items (
			id uuid PRIMARY KEY,
			owner_id text NOT NULL,
			value text NOT NULL,
			updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
			deleted_at timestamptz
		)`); err != nil {
		t.Fatalf("inject registered relation OID drift: %v", err)
	}
	var replacementOID, persistedOID int64
	if err := admin.QueryRowContext(ctx, `
		SELECT 'public.cf_items'::regclass::oid::bigint,
		       registry.physical_relation_oid::bigint
		FROM synchro.sync_registry registry
		JOIN synchro.sync_registry_generations generation
		  ON generation.generation = registry.registry_generation
		WHERE generation.state = 'active' AND registry.table_name = 'cf_items'`).Scan(&replacementOID, &persistedOID); err != nil {
		t.Fatalf("observe relation OID drift: %v", err)
	}
	oidDrift := loadIssue49Health(t, ctx, admin)

	t.Run("assertion", func(t *testing.T) {
		if shadowHealth["ready"] != true || issue49HealthChecks(t, shadowHealth)["relation_identity"] != "ok" {
			t.Fatalf("same-name shadow relation changed qualified identity: %#v", shadowHealth)
		}
		if beforePKUpdate != afterPKUpdate || changedStages != (issue49WALStageCounts{}) {
			t.Fatalf("primary-key update emitted fence or row effects: before=%#v after=%#v changed=%#v", beforePKUpdate, afterPKUpdate, changedStages)
		}
		if issue49HealthChecks(t, replicaDrift)["relation_identity"] != "failed" || replicaDrift["ready"] != false {
			t.Fatalf("replica-identity drift remained ready: %#v", replicaDrift)
		}
		if registeredOID != persistedOID || replacementOID == persistedOID ||
			issue49HealthChecks(t, oidDrift)["relation_identity"] != "failed" || oidDrift["ready"] != false {
			t.Fatalf("OID drift rebound or remained ready: registered=%d replacement=%d persisted=%d health=%#v", registeredOID, replacementOID, persistedOID, oidDrift)
		}
	})
}

// TestRealIssue49HealthUsesFiniteCanonicalObservations proves SYNC-HEALTH-001,
// SYNC-HEALTH-002, and the health portion of SYNC-WAL-008.
func TestRealIssue49HealthUsesFiniteCanonicalObservations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	waitForIssue49CanonicalHealth(t, ctx, admin, true)
	baseline := loadIssue49Health(t, ctx, admin)
	status, body := getIssue49Readiness(t, ctx, harness.AdapterURL())

	if _, err := admin.ExecContext(ctx, "ALTER SYSTEM SET synchro.max_wal_lag_seconds = '0'"); err != nil {
		t.Fatalf("set invalid finite-lag limit: %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = admin.ExecContext(cleanupContext, "ALTER SYSTEM SET synchro.max_wal_lag_seconds = '30'")
		_, _ = admin.ExecContext(cleanupContext, "SELECT pg_catalog.pg_reload_conf()")
	})
	if _, err := admin.ExecContext(ctx, "SELECT pg_catalog.pg_reload_conf()"); err != nil {
		t.Fatalf("reload invalid finite-lag limit: %v", err)
	}
	waitForIssue49PublicReady(t, ctx, harness.AdapterURL(), false)
	invalid := loadIssue49Health(t, ctx, admin)
	invalidStatus, invalidBody := getIssue49Readiness(t, ctx, harness.AdapterURL())

	t.Run("assertion", func(t *testing.T) {
		if status != http.StatusOK || !bytes.Equal(body, []byte(`{"ready":true}`)) || len(baseline) != 3 {
			t.Fatalf("public readiness did not mirror canonical healthy state: status=%d body=%q detail=%#v", status, body, baseline)
		}
		checks := issue49HealthChecks(t, baseline)
		for _, name := range issue49HealthCheckNames {
			if checks[name] != "ok" {
				t.Fatalf("healthy canonical check %q = %q", name, checks[name])
			}
		}
		observations, ok := baseline["observations"].(map[string]any)
		if !ok {
			t.Fatalf("canonical health observations are invalid: %#v", baseline)
		}
		for _, name := range []string{"heartbeat_age_seconds", "wal_lag_bytes", "wal_lag_seconds"} {
			value, ok := observations[name].(float64)
			if !ok || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
				t.Fatalf("health observation %q is not finite and nonnegative: %#v", name, observations[name])
			}
		}
		if invalidStatus != http.StatusServiceUnavailable || !bytes.Equal(invalidBody, []byte(`{"ready":false}`)) ||
			invalid["ready"] != false || issue49HealthChecks(t, invalid)["wal_time_lag"] != "failed" {
			t.Fatalf("invalid finite-lag limit remained ready: status=%d body=%q detail=%#v", invalidStatus, invalidBody, invalid)
		}
		if bytes.Contains(invalidBody, []byte("checks")) || bytes.Contains(invalidBody, []byte("observations")) {
			t.Fatalf("public readiness disclosed detailed health state: %q", invalidBody)
		}
	})
}

// TestRealIssue49DatabaseAuthorityAndInstallation proves SYNC-DBAUTH-001,
// SYNC-DBAUTH-002, SYNC-INSTALL-001, and SYNC-INSTALL-002.
func TestRealIssue49DatabaseAuthorityAndInstallation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	environment, err := blackbox.LoadEnvironment()
	if err != nil {
		t.Fatalf("load installation proof environment: %v", err)
	}
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)

	var serverMajor int
	var extensionVersion, extensionSchema string
	if err := admin.QueryRowContext(ctx, `
		SELECT current_setting('server_version_num')::integer / 10000,
		       extension.extversion,
		       namespace.nspname
		FROM pg_catalog.pg_extension extension
		JOIN pg_catalog.pg_namespace namespace ON namespace.oid = extension.extnamespace
		WHERE extension.extname = 'synchro_pg'`).Scan(&serverMajor, &extensionVersion, &extensionSchema); err != nil {
		t.Fatalf("observe clean extension installation: %v", err)
	}
	var otherVersions, updatePaths int
	if err := admin.QueryRowContext(ctx, `
		SELECT count(*) FROM pg_catalog.pg_available_extension_versions
		WHERE name = 'synchro_pg' AND version <> '0.3.0'`).Scan(&otherVersions); err != nil {
		t.Fatalf("observe extension migration versions: %v", err)
	}
	if err := admin.QueryRowContext(ctx, "SELECT count(*) FROM pg_catalog.pg_extension_update_paths('synchro_pg')").Scan(&updatePaths); err != nil {
		t.Fatalf("observe extension update paths: %v", err)
	}

	var restrictedGroups int
	if err := admin.QueryRowContext(ctx, `
		SELECT count(*)
		FROM pg_catalog.pg_roles
		WHERE rolname = ANY($1)
		  AND NOT rolcanlogin AND NOT rolreplication
		  AND NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolbypassrls`,
		[]string{"synchro_owner", "synchro_adapter", "synchro_seed", "synchro_monitor", "synchro_operator", "synchro_worker"},
	).Scan(&restrictedGroups); err != nil {
		t.Fatalf("observe extension authority groups: %v", err)
	}
	loginGroups := map[string]string{
		environment.Adapter.Username:  "synchro_adapter",
		environment.Observer.Username: "synchro_monitor",
		environment.Operator.Username: "synchro_operator",
		environment.Worker.Username:   "synchro_worker",
	}
	for login, group := range loginGroups {
		var exact bool
		if err := admin.QueryRowContext(ctx, `
			SELECT count(*) = 1 AND COALESCE(bool_and(granted.rolname = $2), false)
			FROM pg_catalog.pg_auth_members membership
			JOIN pg_catalog.pg_roles granted ON granted.oid = membership.roleid
			JOIN pg_catalog.pg_roles member ON member.oid = membership.member
			WHERE member.rolname = $1`, login, group).Scan(&exact); err != nil || !exact {
			t.Fatalf("runtime login %q does not belong only to %q: exact=%t err=%v", login, group, exact, err)
		}
	}
	var workerBoundary, soleWorker, workerHBA bool
	if err := admin.QueryRowContext(ctx, `
		SELECT rolcanlogin AND rolreplication AND NOT rolinherit
		       AND NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolbypassrls
		FROM pg_catalog.pg_roles WHERE rolname = $1`, environment.Worker.Username).Scan(&workerBoundary); err != nil {
		t.Fatalf("observe worker login boundary: %v", err)
	}
	if err := admin.QueryRowContext(ctx, `
		SELECT count(*) = 1 AND bool_and(rolname = $1)
		FROM pg_catalog.pg_roles
		WHERE rolcanlogin AND rolreplication AND NOT rolsuper`, environment.Worker.Username).Scan(&soleWorker); err != nil {
		t.Fatalf("observe replication principal cardinality: %v", err)
	}
	if err := admin.QueryRowContext(ctx, `
		SELECT count(*) = 1
		FROM pg_catalog.pg_hba_file_rules
		WHERE type = 'host' AND $1 = ANY(database) AND $2 = ANY(user_name)
		  AND address = '0.0.0.0' AND netmask = '0.0.0.0'
		  AND auth_method = 'scram-sha-256'`, harness.Names().Database, environment.Worker.Username).Scan(&workerHBA); err != nil {
		t.Fatalf("observe worker HBA boundary: %v", err)
	}

	var publicAuthority, excessMetadataAuthority bool
	if err := admin.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_catalog.pg_namespace namespace
			CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(namespace.nspacl, pg_catalog.acldefault('n', namespace.nspowner))) acl
			WHERE namespace.nspname IN ('synchro', 'synchro_projection') AND acl.grantee = 0
			UNION ALL
			SELECT 1 FROM pg_catalog.pg_class relation
			JOIN pg_catalog.pg_namespace namespace ON namespace.oid = relation.relnamespace
			CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(relation.relacl, pg_catalog.acldefault(CASE WHEN relation.relkind = 'S' THEN 'S'::"char" ELSE 'r'::"char" END, relation.relowner))) acl
			WHERE namespace.nspname = 'synchro' AND relation.relkind IN ('r', 'p', 'S') AND acl.grantee = 0
			UNION ALL
			SELECT 1 FROM pg_catalog.pg_proc procedure
			JOIN pg_catalog.pg_namespace namespace ON namespace.oid = procedure.pronamespace
			CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(procedure.proacl, pg_catalog.acldefault('f', procedure.proowner))) acl
			WHERE namespace.nspname = 'synchro' AND acl.grantee = 0
			UNION ALL
			SELECT 1 FROM pg_catalog.pg_type type
			JOIN pg_catalog.pg_namespace namespace ON namespace.oid = type.typnamespace
			CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(type.typacl, pg_catalog.acldefault('T', type.typowner))) acl
			WHERE namespace.nspname = 'synchro' AND acl.grantee = 0
		)`).Scan(&publicAuthority); err != nil {
		t.Fatalf("observe PUBLIC extension authority: %v", err)
	}
	if err := admin.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_catalog.pg_class relation
			JOIN pg_catalog.pg_namespace namespace ON namespace.oid = relation.relnamespace
			CROSS JOIN unnest(ARRAY['synchro_adapter', 'synchro_seed', 'synchro_monitor']) AS roles(role_name)
			WHERE namespace.nspname = 'synchro' AND relation.relkind IN ('r', 'p', 'S')
			  AND pg_catalog.has_table_privilege(role_name, relation.oid, 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
		)`).Scan(&excessMetadataAuthority); err != nil {
		t.Fatalf("observe direct runtime metadata authority: %v", err)
	}

	trackedSQL, artifactSQL := loadIssue49InstallSQL(t, environment.ExtensionArtifact)

	t.Run("assertion", func(t *testing.T) {
		if serverMajor != 18 || extensionVersion != "0.3.0" || extensionSchema != "synchro" || otherVersions != 0 || updatePaths != 0 {
			t.Fatalf("clean PostgreSQL 18 installation has migration drift: major=%d version=%q schema=%q other=%d paths=%d", serverMajor, extensionVersion, extensionSchema, otherVersions, updatePaths)
		}
		if restrictedGroups != 6 || !workerBoundary || !soleWorker || !workerHBA {
			t.Fatalf("database role split is invalid: groups=%d worker=%t sole=%t hba=%t", restrictedGroups, workerBoundary, soleWorker, workerHBA)
		}
		if publicAuthority || excessMetadataAuthority {
			t.Fatalf("default-deny authority failed: public=%t runtime_metadata=%t", publicAuthority, excessMetadataAuthority)
		}
		if !bytes.Equal(trackedSQL, artifactSQL) {
			t.Fatal("packaged pgrx SQL differs from the tracked generated SQL")
		}
	})
}

// TestRealIssue49MembershipActivationIsStagedAndScoped proves the server parts
// of SYNC-MEMBERSHIP-002 and SYNC-MEMBERSHIP-003.
func TestRealIssue49MembershipActivationIsStagedAndScoped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	const relocatedScope = "cf:issue49-relocated"
	if _, err := admin.ExecContext(ctx, "SELECT synchro.synchro_register_shared_scope($1, false)", relocatedScope); err != nil {
		t.Fatalf("register nontrivial membership destination scope: %v", err)
	}
	client := connectRealProtocolClient(
		t,
		ctx,
		harness,
		token,
		"issue49-membership-client",
		"cf:global",
		relocatedScope,
		"user:diagnostic-user",
	)
	rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8d06-000000000001")
	rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8d06-000000000002")
	rebuildRealScope(t, ctx, harness, token, client, relocatedScope, "00000000-0000-4000-8d06-000000000003")
	acknowledgeRealClientCursors(t, ctx, harness, token, client)
	beforeCheckpoints := observeCheckpointMap(t, ctx, harness, client.ID)
	if len(beforeCheckpoints) != 3 {
		t.Fatalf("nontrivial membership checkpoint setup is incomplete: %#v", beforeCheckpoints)
	}

	if err := harness.Source().ExecContext(ctx, `
		INSERT INTO cf_items (id, owner_id, value)
		SELECT ('00000000-0000-4000-8d06-' || lpad(value::text, 12, '0'))::uuid,
		       'diagnostic-user', 'issue49-membership-' || value::text
		FROM generate_series(100, 355) value`); err != nil {
		t.Fatalf("insert membership backfill source set: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", "00000000-0000-4000-8d06-000000000355")

	var priorRegistry, priorOwnerMembership, priorRelocatedMembership int64
	if err := admin.QueryRowContext(ctx, "SELECT generation FROM synchro.sync_registry_generations WHERE state = 'active'").Scan(&priorRegistry); err != nil {
		t.Fatalf("observe prior active registry: %v", err)
	}
	if err := admin.QueryRowContext(ctx, `
		SELECT owner.membership_generation, relocated.membership_generation
		FROM synchro.sync_scope_state owner
		CROSS JOIN synchro.sync_scope_state relocated
		WHERE owner.scope_id = 'user:diagnostic-user' AND relocated.scope_id = $1`, relocatedScope).Scan(
		&priorOwnerMembership,
		&priorRelocatedMembership,
	); err != nil {
		t.Fatalf("observe prior affected membership generations: %v", err)
	}
	barrier, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin membership activation barrier: %v", err)
	}
	defer barrier.Rollback()
	if _, err := barrier.ExecContext(ctx, "LOCK TABLE synchro.sync_bucket_edges IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatalf("lock active membership edges: %v", err)
	}
	if _, err := admin.ExecContext(ctx, `
		CREATE OR REPLACE FUNCTION public.cf_items_membership(p_id uuid)
		RETURNS SETOF text
		LANGUAGE SQL STABLE SECURITY INVOKER SET search_path = pg_catalog, synchro
		BEGIN ATOMIC
			SELECT CASE
				WHEN (p.owner_id #>> '{}') = 'diagnostic-user' THEN 'cf:issue49-relocated'
				ELSE 'user:' || (p.owner_id #>> '{}')
			END
			FROM synchro_projection.cf_items AS p
			WHERE p.record_id = p_id::text AND NOT p.deleted;
		END`); err != nil {
		t.Fatalf("install nontrivial membership rule: %v", err)
	}
	if _, err := admin.ExecContext(ctx, `
		SELECT synchro.synchro_register_table(
			'public.cf_items', 'public.cf_items_membership', 'single_scope',
			'id', 'updated_at', 'deleted_at', 'enabled',
			p_affected_scopes => ARRAY['user:diagnostic-user', $1]::text[]
		)`, relocatedScope); err != nil {
		t.Fatalf("stage nontrivial membership transition: %v", err)
	}
	var registryGeneration int64
	if err := admin.QueryRowContext(ctx, `
		SELECT max(registry_generation)
		FROM synchro.sync_registry_membership_stages
		WHERE state = 'pending'`).Scan(&registryGeneration); err != nil || registryGeneration <= priorRegistry {
		t.Fatalf("observe nontrivial membership stage: generation=%d prior=%d err=%v", registryGeneration, priorRegistry, err)
	}
	var pendingState string
	var pendingVerified, oldRegistryStillActive bool
	var sourceRows, activeOwnerEdges, activeRelocatedEdges int64
	if err := barrier.QueryRowContext(ctx, `
		SELECT stage.state,
		       stage.verified,
		       EXISTS (SELECT 1 FROM synchro.sync_registry_generations WHERE generation = $2 AND state = 'active'),
		       (SELECT count(*) FROM public.cf_items WHERE deleted_at IS NULL),
		       (SELECT count(*) FROM synchro.sync_bucket_edges WHERE table_name = 'cf_items' AND bucket_id = 'user:diagnostic-user'),
		       (SELECT count(*) FROM synchro.sync_bucket_edges WHERE table_name = 'cf_items' AND bucket_id = $3)
		FROM synchro.sync_registry_membership_stages stage
		WHERE stage.registry_generation = $1`, registryGeneration, priorRegistry, relocatedScope).Scan(
		&pendingState,
		&pendingVerified,
		&oldRegistryStillActive,
		&sourceRows,
		&activeOwnerEdges,
		&activeRelocatedEdges,
	); err != nil {
		t.Fatalf("observe pending membership stage: %v", err)
	}
	if err := barrier.Commit(); err != nil {
		t.Fatalf("release membership activation barrier: %v", err)
	}
	barrier = nil
	if err := waitForIssue49MembershipActivation(
		ctx, admin, registryGeneration, relocatedScope, priorOwnerMembership, priorRelocatedMembership,
	); err != nil {
		t.Fatalf("wait for complete membership activation: %v; %s", err, harness.FailureDiagnostics())
	}

	var activeState, priorState, affectedScopes string
	var verified bool
	var stagedRecords, stagedEdges, currentOwnerEdges, currentRelocatedEdges int64
	var activationCommit, activationEnd string
	if err := admin.QueryRowContext(ctx, `
		SELECT stage.state, stage.verified, stage.staged_record_count, stage.staged_edge_count,
		       stage.activation_commit_lsn::text, stage.activation_end_lsn::text,
		       pg_catalog.array_to_json(stage.affected_scopes)::text,
		       (SELECT state FROM synchro.sync_registry_generations WHERE generation = $2),
		       (SELECT count(*) FROM synchro.sync_bucket_edges WHERE table_name = 'cf_items' AND bucket_id = 'user:diagnostic-user'),
		       (SELECT count(*) FROM synchro.sync_bucket_edges WHERE table_name = 'cf_items' AND bucket_id = $3)
		FROM synchro.sync_registry_membership_stages stage
		WHERE stage.registry_generation = $1`, registryGeneration, priorRegistry, relocatedScope).Scan(
		&activeState,
		&verified,
		&stagedRecords,
		&stagedEdges,
		&activationCommit,
		&activationEnd,
		&affectedScopes,
		&priorState,
		&currentOwnerEdges,
		&currentRelocatedEdges,
	); err != nil {
		t.Fatalf("observe activated membership stage: %v", err)
	}
	afterCheckpoints := observeCheckpointMap(t, ctx, harness, client.ID)
	status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/pull", realPullPayload(client, issue49CloneScopes(client.Scopes), 100))

	t.Run("assertion", func(t *testing.T) {
		if pendingState != "pending" || pendingVerified || !oldRegistryStillActive ||
			activeOwnerEdges != sourceRows || activeRelocatedEdges != 0 {
			t.Fatalf("pending backfill exposed partial membership: state=%q verified=%t old_active=%t rows=%d owner=%d relocated=%d", pendingState, pendingVerified, oldRegistryStillActive, sourceRows, activeOwnerEdges, activeRelocatedEdges)
		}
		if activeState != "activated" || !verified || stagedRecords != sourceRows || stagedEdges != sourceRows ||
			currentOwnerEdges != 0 || currentRelocatedEdges != sourceRows || activationCommit == "" || activationEnd == "" ||
			!issue49ScopeSetEquals(affectedScopes, relocatedScope, "user:diagnostic-user") || priorState != "superseded" {
			t.Fatalf("membership backfill activation is incomplete: state=%q verified=%t records=%d edges=%d owner=%d relocated=%d commit=%q end=%q scopes=%s prior=%q", activeState, verified, stagedRecords, stagedEdges, currentOwnerEdges, currentRelocatedEdges, activationCommit, activationEnd, affectedScopes, priorState)
		}
		if _, present := afterCheckpoints["user:diagnostic-user"]; present {
			t.Fatalf("membership transition retained the old affected checkpoint: before=%#v after=%#v", beforeCheckpoints, afterCheckpoints)
		}
		if _, present := afterCheckpoints[relocatedScope]; present ||
			!sameCheckpointPosition(afterCheckpoints["cf:global"], beforeCheckpoints["cf:global"]) {
			t.Fatalf("membership transition invalidated unrelated checkpoints: before=%#v after=%#v", beforeCheckpoints, afterCheckpoints)
		}
		if status != http.StatusOK {
			t.Fatalf("post-membership pull status = %d, want 200: %#v", status, response)
		}
		rebuild, ok := response["rebuild"].([]any)
		if !ok || !issue49AnyScopeSetEquals(rebuild, relocatedScope, "user:diagnostic-user") {
			t.Fatalf("membership transition did not rebuild exactly the affected scopes: %#v", response)
		}
	})
}

// TestRealIssue49FenceCorrelationAndCapturePending proves SYNC-WAL-009.
func TestRealIssue49FenceCorrelationAndCapturePending(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-fence-client")
	rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8d07-000000000001")
	rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8d07-000000000002")
	acknowledgeRealClientCursors(t, ctx, harness, token, client)
	beforeCheckpoints := observeCheckpointMap(t, ctx, harness, client.ID)

	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")
	acceptedRecordID := "00000000-0000-4000-8d07-000000000011"
	mutationID := "00000000-0000-4000-8d07-000000000012"
	status, acceptedResponse := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		"00000000-0000-4000-8d07-000000000013",
		[]map[string]any{phase4InsertMutation(client, table, ownerField, mutationID, acceptedRecordID, "issue49-capture-pending")},
	))
	if status != http.StatusOK || len(requireOutcomeList(t, acceptedResponse, "accepted")) != 1 {
		t.Fatalf("capture-pending control push was not accepted: status=%d response=%#v", status, acceptedResponse)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", acceptedRecordID)

	correlationID := "00000000-0000-4000-8d07-000000000021"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		correlationID,
		"diagnostic-user",
		"issue49-correlation-insert",
	); err != nil {
		t.Fatalf("insert fence-correlation row: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", correlationID)
	transaction, err := harness.Source().BeginTx(ctx)
	if err != nil {
		t.Fatalf("begin repeated-operation correlation transaction: %v", err)
	}
	for step, statement := range []string{
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'issue49-conflict-update') ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value, updated_at = clock_timestamp()",
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'issue49-do-nothing') ON CONFLICT (id) DO NOTHING",
		"UPDATE cf_items SET value = 'issue49-repeat-update', updated_at = clock_timestamp() WHERE id = $1",
		"DELETE FROM cf_items WHERE id = $1",
	} {
		if _, err := transaction.ExecContext(ctx, statement, correlationID); err != nil {
			_ = transaction.Rollback()
			t.Fatalf("execute correlation operation %d: %v", step+1, err)
		}
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit repeated-operation correlation transaction: %v", err)
	}
	correlation := waitForIssue49FenceCorrelation(t, ctx, admin, correlationID, 4)

	captureID := "00000000-0000-4000-8d07-000000000031"
	captureDocumentID := "00000000-0000-4000-8d07-000000000032"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_documents (id, owner_id, title) VALUES ($1, $2, $3)",
		captureDocumentID,
		"diagnostic-user",
		"issue49-capture-dependency-parent",
	); err != nil {
		t.Fatalf("insert capture-dependency parent row: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_documents", captureDocumentID)
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_document_access (id, document_id, owner_id) VALUES ($1, $2, $3)",
		captureID,
		captureDocumentID,
		"diagnostic-user",
	); err != nil {
		t.Fatalf("insert capture-dependency correlation row: %v", err)
	}
	captureDistinct := waitForIssue49CaptureDependencyFence(t, ctx, admin, captureID)

	if _, err := admin.ExecContext(ctx, `
		UPDATE synchro.sync_write_fences
		SET coverage = 'pending',
		    stream_generation = NULL,
		    commit_lsn = NULL,
		    event_ordinal = NULL,
		    materialized_at = NULL
		WHERE mutation_id = $1 AND user_id = 'diagnostic-user' AND client_id = $2`, mutationID, client.ID); err != nil {
		t.Fatalf("inject uncovered accepted fence: %v", err)
	}
	pullStatus, pullResponse := postSync(t, ctx, harness.AdapterURL(), token, "/sync/pull", realPullPayload(client, issue49CloneScopes(client.Scopes), 100))
	rebuildStatus, rebuildResponse := requestRealRebuildPage(
		t,
		ctx,
		harness,
		token,
		client,
		"user:diagnostic-user",
		"00000000-0000-4000-8d07-000000000041",
		nil,
		100,
	)
	afterCheckpoints := observeCheckpointMap(t, ctx, harness, client.ID)

	mismatchRecordID := "00000000-0000-4000-8d07-000000000051"
	mismatchedRecordID := "00000000-0000-4000-8d07-000000000052"
	laterRecordID := "00000000-0000-4000-8d07-000000000053"
	mismatch := injectIssue49FenceKeyVersionMismatch(
		t, ctx, harness, admin, mismatchRecordID, mismatchedRecordID, laterRecordID,
	)
	mismatchPoison := waitForIssue49Poison(t, ctx, harness, laterRecordID)
	mismatchState := observeIssue49FenceMismatchBlock(t, ctx, admin, mismatch, mismatchRecordID, mismatchedRecordID)

	t.Run("assertion", func(t *testing.T) {
		if correlation.Fences != 4 || correlation.Events != 4 || correlation.UniqueFenceIDs != 4 ||
			correlation.Mismatches != 0 || correlation.KeyMismatches != 0 || correlation.VersionMismatches != 0 ||
			correlation.Operations != "insert,update,update,delete" ||
			correlation.DMLOrdinals != "1,1,2,3" {
			t.Fatalf("actual row operations did not correlate one-to-one: %#v", correlation)
		}
		if !captureDistinct {
			t.Fatal("capture-dependency fence reused synced-table identity")
		}
		assertIssue49CapturePending(t, pullStatus, pullResponse)
		assertIssue49CapturePending(t, rebuildStatus, rebuildResponse)
		if !issue49CheckpointMapsEqual(beforeCheckpoints, afterCheckpoints) {
			t.Fatalf("capture_pending advanced durable progress: before=%#v after=%#v", beforeCheckpoints, afterCheckpoints)
		}
		if mismatch.OriginalVersion == mismatch.MismatchedVersion || mismatch.FenceID == "" ||
			mismatchPoison.FailureClass != "fence_correlation_failed" || !mismatchPoison.AcknowledgementBlocked ||
			!mismatchPoison.RelationIDMatchesRegistry || mismatchPoison.LaterRecordMaterialized ||
			!mismatchPoison.LaterFencePending || !mismatchPoison.WorkerBlocked ||
			!mismatchState.FencePending || !mismatchState.KeyChanged || !mismatchState.VersionChanged ||
			mismatchState.EventMaterialized || mismatchState.OriginalRecordMaterialized || mismatchState.MismatchedRecordMaterialized {
			t.Fatalf("key and version mismatch did not block capture: mismatch=%#v poison=%#v state=%#v", mismatch, mismatchPoison, mismatchState)
		}
	})
}

// TestRealIssue49AdapterDelegationAndServerScopes proves SYNC-BOUNDARY-001 and
// SYNC-SCOPE-005 at all public synchronization endpoints.
func TestRealIssue49AdapterDelegationAndServerScopes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-boundary-client")
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")

	type endpointControl struct {
		name      string
		function  string
		path      string
		payload   map[string]any
		wantAfter int
	}
	controls := []endpointControl{
		{
			name:     "connect",
			function: "synchro.synchro_connect(pg_catalog.text,pg_catalog.jsonb)",
			path:     "/sync/connect",
			payload: map[string]any{
				"client_id":         "issue49-delegation-connect",
				"platform":          "conformance",
				"app_version":       "0.3.0",
				"protocol_version":  3,
				"schema":            map[string]any{"version": 0, "hash": ""},
				"scope_set_version": 0,
				"known_scopes":      map[string]any{},
			},
			wantAfter: http.StatusOK,
		},
		{
			name:      "pull",
			function:  "synchro.synchro_pull(pg_catalog.text,pg_catalog.jsonb)",
			path:      "/sync/pull",
			payload:   realPullPayload(client, issue49CloneScopes(client.Scopes), 100),
			wantAfter: http.StatusOK,
		},
		{
			name:     "push",
			function: "synchro.synchro_push(pg_catalog.text,pg_catalog.jsonb)",
			path:     "/sync/push",
			payload: phase4PushPayload(client, "00000000-0000-4000-8d08-000000000001", []map[string]any{
				phase4InsertMutation(
					client,
					table,
					ownerField,
					"00000000-0000-4000-8d08-000000000002",
					"00000000-0000-4000-8d08-000000000003",
					"issue49-delegated-push",
				),
			}),
			wantAfter: http.StatusOK,
		},
		{
			name:     "rebuild",
			function: "synchro.synchro_rebuild(pg_catalog.text,pg_catalog.jsonb)",
			path:     "/sync/rebuild",
			payload: map[string]any{
				"client_id":         client.ID,
				"client_generation": client.Generation,
				"schema":            client.Schema,
				"scope":             "user:diagnostic-user",
				"rebuild_id":        "00000000-0000-4000-8d08-000000000004",
				"cursor":            nil,
				"limit":             100,
			},
			wantAfter: http.StatusOK,
		},
	}
	for _, control := range controls {
		control := control
		t.Run(control.name, func(t *testing.T) {
			revoke := "REVOKE EXECUTE ON FUNCTION " + control.function + " FROM synchro_adapter"
			grant := "GRANT EXECUTE ON FUNCTION " + control.function + " TO synchro_adapter"
			if _, err := admin.ExecContext(ctx, revoke); err != nil {
				t.Fatalf("revoke canonical %s function: %v", control.name, err)
			}
			restored := false
			defer func() {
				if !restored {
					_, _ = admin.ExecContext(context.Background(), grant)
				}
			}()
			deniedStatus, deniedResponse := postSync(t, ctx, harness.AdapterURL(), token, control.path, control.payload)
			if _, err := admin.ExecContext(ctx, grant); err != nil {
				t.Fatalf("restore canonical %s function: %v", control.name, err)
			}
			restored = true
			afterStatus, _ := postSync(t, ctx, harness.AdapterURL(), token, control.path, control.payload)
			if deniedStatus != http.StatusInternalServerError || afterStatus != control.wantAfter {
				t.Fatalf("adapter did not delegate %s semantics: denied=%d response=%#v restored=%d", control.name, deniedStatus, deniedResponse, afterStatus)
			}
			if control.name == "push" {
				waitForRealWALRecords(t, ctx, harness, "cf_items", "00000000-0000-4000-8d08-000000000003")
			}
		})
	}

	acknowledgeRealClientCursors(t, ctx, harness, token, client)
	before := observeCheckpointMap(t, ctx, harness, client.ID)
	unknownScopes := issue49CloneScopes(client.Scopes)
	unknownScopes["owner_id = 'diagnostic-user' OR true"] = client.Scopes["user:diagnostic-user"]
	unknownStatus, unknownResponse := postSync(t, ctx, harness.AdapterURL(), token, "/sync/pull", realPullPayload(client, unknownScopes, 100))
	predicatePayload := realPullPayload(client, issue49CloneScopes(client.Scopes), 100)
	predicatePayload["predicate"] = "owner_id = current_user"
	predicateStatus, predicateResponse := postSync(t, ctx, harness.AdapterURL(), token, "/sync/pull", predicatePayload)
	after := observeCheckpointMap(t, ctx, harness, client.ID)

	t.Run("scope_assertion", func(t *testing.T) {
		assertIssue49ProtocolError(t, unknownStatus, unknownResponse, http.StatusBadRequest, "invalid_request", false)
		assertIssue49ProtocolError(t, predicateStatus, predicateResponse, http.StatusBadRequest, "invalid_request", false)
		if !issue49CheckpointMapsEqual(before, after) {
			t.Fatalf("client-authored replication model advanced progress: before=%#v after=%#v", before, after)
		}
	})
}

type issue49SQLQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type issue49WALStageCounts struct {
	Fences        int64
	PendingFences int64
	Events        int64
	Captured      int64
	Edges         int64
	Changes       int64
}

type issue49FenceCorrelation struct {
	Fences            int64
	Events            int64
	UniqueFenceIDs    int64
	Mismatches        int64
	KeyMismatches     int64
	VersionMismatches int64
	Operations        string
	DMLOrdinals       string
}

type issue49BlockedAcknowledgement struct {
	PoisonCommitLSN      string
	ProgressEndLSN       string
	SlotFlushLSN         string
	SlotMatchesProgress  bool
	ProgressBeforePoison bool
	SlotBeforePoison     bool
}

type issue49ResetFenceCoverage struct {
	Staged                 int64
	Covered                int64
	UniqueCovered          int64
	ExactFenceSet          bool
	MetadataMismatches     int64
	PendingExpected        int64
	PendingRegistered      int64
	SnapshotMarkersBounded bool
	CoverageModes          string
}

type issue49FenceMismatch struct {
	FenceID           string
	OriginalVersion   string
	MismatchedVersion string
}

type issue49FenceMismatchBlock struct {
	FencePending                 bool
	KeyChanged                   bool
	VersionChanged               bool
	EventMaterialized            bool
	OriginalRecordMaterialized   bool
	MismatchedRecordMaterialized bool
}

var issue49HealthCheckNames = []string{
	"database_contract",
	"extension_objects_stale",
	"registry_generation",
	"schema_generation",
	"relation_identity",
	"publication",
	"capture_triggers",
	"replication_slot",
	"stream_reset",
	"poison",
	"materialization_progress",
	"worker",
	"heartbeat",
	"wal_byte_lag",
	"wal_time_lag",
}

func openIssue49Admin(t *testing.T, ctx context.Context, harness *blackbox.Harness) *sql.DB {
	t.Helper()
	database, err := sql.Open("pgx", harness.DatabaseURL())
	if err != nil {
		t.Fatalf("open Issue 49 administrator connection: %v", err)
	}
	database.SetMaxOpenConns(4)
	database.SetMaxIdleConns(1)
	if err := database.PingContext(ctx); err != nil {
		_ = database.Close()
		t.Fatalf("ping Issue 49 administrator connection: %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close Issue 49 administrator connection: %v", err)
		}
	})
	return database
}

func observeIssue49WALStages(t *testing.T, ctx context.Context, queryer issue49SQLQueryer, recordIDs []string) issue49WALStageCounts {
	t.Helper()
	var result issue49WALStageCounts
	err := queryer.QueryRowContext(ctx, `
		WITH relation AS (
			SELECT registry.relation_id
			FROM synchro.sync_registry registry
			JOIN synchro.sync_registry_generations generation
			  ON generation.generation = registry.registry_generation
			WHERE generation.state = 'active' AND registry.table_name = 'cf_items'
		), selected_fences AS (
			SELECT fence.*
			FROM synchro.sync_write_fences fence
			JOIN relation ON relation.relation_id = fence.relation_id
			WHERE fence.old_record_id = ANY($1) OR fence.new_record_id = ANY($1)
		)
		SELECT (SELECT count(*) FROM selected_fences),
		       (SELECT count(*) FROM selected_fences WHERE coverage = 'pending'),
		       (SELECT count(*) FROM synchro.sync_wal_events event JOIN selected_fences fence ON fence.fence_id = event.fence_id),
		       (SELECT count(*) FROM synchro.sync_captured_rows captured JOIN relation ON relation.relation_id = captured.relation_id WHERE captured.record_id = ANY($1)),
		       (SELECT count(*) FROM synchro.sync_bucket_edges edge JOIN relation ON relation.relation_id = edge.relation_id WHERE edge.record_id = ANY($1)),
		       (SELECT count(*) FROM synchro.sync_changelog change JOIN relation ON relation.relation_id = change.relation_id WHERE change.record_id = ANY($1))`, recordIDs).Scan(
		&result.Fences,
		&result.PendingFences,
		&result.Events,
		&result.Captured,
		&result.Edges,
		&result.Changes,
	)
	if err != nil {
		t.Fatalf("observe Issue 49 WAL stages: %v", err)
	}
	return result
}

func observeIssue49BlockedAcknowledgement(
	t *testing.T,
	ctx context.Context,
	queryer issue49SQLQueryer,
	poisonCommitLSN string,
) issue49BlockedAcknowledgement {
	t.Helper()
	var result issue49BlockedAcknowledgement
	if err := queryer.QueryRowContext(ctx, `
		SELECT $1::pg_lsn::text,
		       COALESCE(progress.acknowledged_end_lsn::text, ''),
		       COALESCE(slot.confirmed_flush_lsn::text, ''),
		       COALESCE(progress.acknowledged_end_lsn = slot.confirmed_flush_lsn, false),
		       COALESCE(progress.acknowledged_end_lsn < $1::pg_lsn, false),
		       COALESCE(slot.confirmed_flush_lsn < $1::pg_lsn, false)
		FROM synchro.sync_wal_progress progress
		JOIN synchro.sync_runtime_state runtime ON runtime.singleton
		JOIN pg_catalog.pg_replication_slots slot ON slot.slot_name = runtime.active_slot_name
		WHERE progress.singleton`, poisonCommitLSN).Scan(
		&result.PoisonCommitLSN,
		&result.ProgressEndLSN,
		&result.SlotFlushLSN,
		&result.SlotMatchesProgress,
		&result.ProgressBeforePoison,
		&result.SlotBeforePoison,
	); err != nil {
		t.Fatalf("observe exact blocked logical-slot acknowledgement: %v", err)
	}
	return result
}

func loadIssue49PendingResetFences(t *testing.T, ctx context.Context, queryer issue49SQLQueryer) ([]string, string) {
	t.Helper()
	var encoded, kinds string
	if err := queryer.QueryRowContext(ctx, `
		WITH pending AS (
			SELECT fence.fence_id::text, fence.registration_kind
			FROM synchro.sync_write_fences fence
			JOIN synchro.sync_registry registry ON registry.relation_id = fence.relation_id
			JOIN synchro.sync_registry_generations generation
			  ON generation.generation = registry.registry_generation
			WHERE generation.state = 'active' AND fence.coverage = 'pending'
		)
		SELECT COALESCE(pg_catalog.array_to_json(array_agg(fence_id ORDER BY fence_id))::text, '[]'),
		       COALESCE(string_agg(DISTINCT registration_kind, ',' ORDER BY registration_kind), '')
		FROM pending`).Scan(&encoded, &kinds); err != nil {
		t.Fatalf("load pending reset-era fences: %v", err)
	}
	var fenceIDs []string
	if err := json.Unmarshal([]byte(encoded), &fenceIDs); err != nil {
		t.Fatalf("decode pending reset-era fence identities: %v", err)
	}
	return fenceIDs, kinds
}

func observeIssue49ResetFenceCoverage(
	t *testing.T,
	ctx context.Context,
	queryer issue49SQLQueryer,
	resetID string,
	expectedFenceIDs []string,
) issue49ResetFenceCoverage {
	t.Helper()
	var result issue49ResetFenceCoverage
	if err := queryer.QueryRowContext(ctx, `
		WITH expected AS (
			SELECT value::uuid AS fence_id FROM unnest($2::text[]) value
		), staged AS (
			SELECT coverage.*
			FROM synchro.sync_stream_reset_fence_coverage coverage
			WHERE coverage.reset_id = $1::uuid
		), covered AS (
			SELECT fence.*
			FROM synchro.sync_write_fences fence
			WHERE fence.reset_id = $1::uuid
		), selected_reset AS (
			SELECT * FROM synchro.sync_stream_resets WHERE reset_id = $1::uuid
		)
		SELECT (SELECT count(*) FROM staged),
		       (SELECT count(*) FROM covered),
		       (SELECT count(DISTINCT fence_id) FROM covered),
		       NOT EXISTS (
			   (SELECT fence_id FROM expected EXCEPT SELECT fence_id FROM staged)
			   UNION ALL
			   (SELECT fence_id FROM staged EXCEPT SELECT fence_id FROM expected)
		       ),
		       (SELECT count(*)
		        FROM staged coverage
		        JOIN synchro.sync_write_fences fence USING (fence_id)
		        CROSS JOIN selected_reset reset
		        WHERE coverage.relation_id <> fence.relation_id
		           OR coverage.registration_kind <> fence.registration_kind
		           OR coverage.table_id IS DISTINCT FROM fence.table_id
		           OR coverage.operation <> fence.operation
		           OR coverage.old_record_id IS DISTINCT FROM fence.old_record_id
		           OR coverage.new_record_id IS DISTINCT FROM fence.new_record_id
		           OR coverage.old_capture_key IS DISTINCT FROM fence.old_capture_key
		           OR coverage.new_capture_key IS DISTINCT FROM fence.new_capture_key
		           OR coverage.row_version <> fence.row_version
		           OR coverage.candidate_slot_name <> reset.candidate_slot_name
		           OR coverage.consistent_point <> reset.consistent_point
		           OR coverage.target_stream_generation <> reset.target_stream_generation
		           OR fence.coverage <> 'reset_baseline'
		           OR fence.stream_generation <> reset.target_stream_generation
		           OR fence.reset_slot_name <> reset.candidate_slot_name
		           OR fence.reset_consistent_point <> reset.consistent_point
		           OR fence.commit_lsn IS NOT NULL OR fence.event_ordinal IS NOT NULL
		           OR fence.materialized_at IS NULL),
		       (SELECT count(*) FROM synchro.sync_write_fences fence JOIN expected USING (fence_id) WHERE fence.coverage = 'pending'),
		       (SELECT count(*)
		        FROM synchro.sync_write_fences fence
		        JOIN synchro.sync_registry registry ON registry.relation_id = fence.relation_id
		        JOIN synchro.sync_registry_generations generation
		          ON generation.generation = registry.registry_generation
		        WHERE generation.state = 'active' AND fence.coverage = 'pending'),
		       (SELECT count(*) = 2
		               AND count(*) FILTER (
		                   WHERE marker.phase = 'before'
		                     AND marker.marker_xid = reset.snapshot_before_xid
		                     AND marker.marker_nonce = reset.snapshot_before_nonce
		               ) = 1
		               AND count(*) FILTER (
		                   WHERE marker.phase = 'after'
		                     AND marker.marker_xid = reset.snapshot_after_xid
		                     AND marker.marker_nonce = reset.snapshot_after_nonce
		               ) = 1
		               AND bool_and(
		                   reset.snapshot_before_xid < reset.snapshot_after_xid
		                   AND reset.consistent_point = reset.activation_barrier
		               )
		        FROM synchro.sync_stream_reset_snapshot_markers marker
		        CROSS JOIN selected_reset reset
		        WHERE marker.reset_id = $1::uuid),
		       COALESCE((SELECT string_agg(DISTINCT coverage, ',' ORDER BY coverage) FROM covered), '')`, resetID, expectedFenceIDs).Scan(
		&result.Staged,
		&result.Covered,
		&result.UniqueCovered,
		&result.ExactFenceSet,
		&result.MetadataMismatches,
		&result.PendingExpected,
		&result.PendingRegistered,
		&result.SnapshotMarkersBounded,
		&result.CoverageModes,
	); err != nil {
		t.Fatalf("observe all reset-era fence coverage: %v", err)
	}
	return result
}

func waitForIssue49MembershipActivation(
	ctx context.Context,
	database *sql.DB,
	registryGeneration int64,
	relocatedScope string,
	priorOwnerMembership int64,
	priorRelocatedMembership int64,
) error {
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		var state string
		var ownerMembership, relocatedMembership int64
		err := database.QueryRowContext(ctx, `
			SELECT stage.state, owner.membership_generation, relocated.membership_generation
			FROM synchro.sync_registry_membership_stages stage
			JOIN synchro.sync_scope_state owner ON owner.scope_id = 'user:diagnostic-user'
			JOIN synchro.sync_scope_state relocated ON relocated.scope_id = $2
			WHERE stage.registry_generation = $1`, registryGeneration, relocatedScope).Scan(
			&state,
			&ownerMembership,
			&relocatedMembership,
		)
		if err != nil {
			return errors.New("read nontrivial membership activation failed")
		}
		if state == "activated" {
			if ownerMembership != priorOwnerMembership+1 || relocatedMembership != priorRelocatedMembership+1 {
				return errors.New("affected membership generations did not advance exactly once")
			}
			return nil
		}
		if state != "pending" {
			return errors.New("nontrivial membership activation entered an invalid state")
		}
		select {
		case <-ctx.Done():
			return errors.New("nontrivial membership activation was canceled")
		case <-time.After(50 * time.Millisecond):
		}
	}
	return errors.New("nontrivial membership activation expired")
}

func waitForIssue49Poison(t *testing.T, ctx context.Context, harness *blackbox.Harness, laterRecordID string) blackbox.WALPoisonObservation {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var observation blackbox.WALPoisonObservation
	var err error
	for time.Now().Before(deadline) {
		observation, err = harness.Operator().ObserveBlockingPoison(ctx, laterRecordID)
		if err == nil && observation.WorkerBlocked && observation.LaterFencePending {
			return observation
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("Issue 49 poison was not durable and blocking: observation=%#v err=%v; %s", observation, err, harness.FailureDiagnostics())
	return blackbox.WALPoisonObservation{}
}

func getIssue49Readiness(t *testing.T, ctx context.Context, adapterURL string) (int, []byte) {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, adapterURL+"/ready", nil)
	if err != nil {
		t.Fatalf("create Issue 49 readiness request: %v", err)
	}
	response, err := (&http.Client{Timeout: 30 * time.Second}).Do(request)
	if err != nil {
		t.Fatalf("send Issue 49 readiness request: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	if err != nil || len(body) > 4096 {
		t.Fatalf("read bounded Issue 49 readiness response: size=%d err=%v", len(body), err)
	}
	return response.StatusCode, body
}

func waitForIssue49PublicReady(t *testing.T, ctx context.Context, adapterURL string, want bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var status int
	var body []byte
	for time.Now().Before(deadline) {
		status, body = getIssue49Readiness(t, ctx, adapterURL)
		if want && status == http.StatusOK && bytes.Equal(body, []byte(`{"ready":true}`)) {
			return
		}
		if !want && status == http.StatusServiceUnavailable && bytes.Equal(body, []byte(`{"ready":false}`)) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("public readiness did not reach %t: status=%d body=%q", want, status, body)
}

func loadIssue49Health(t *testing.T, ctx context.Context, queryer issue49SQLQueryer) map[string]any {
	t.Helper()
	var encoded []byte
	if err := queryer.QueryRowContext(ctx, "SELECT synchro.synchro_health_detail()").Scan(&encoded); err != nil {
		t.Fatalf("load canonical Issue 49 health: %v", err)
	}
	var detail map[string]any
	if err := json.Unmarshal(encoded, &detail); err != nil {
		t.Fatalf("decode canonical Issue 49 health: %v", err)
	}
	return detail
}

func issue49HealthChecks(t *testing.T, detail map[string]any) map[string]string {
	t.Helper()
	rawChecks, ok := detail["checks"].(map[string]any)
	if !ok {
		t.Fatalf("canonical health checks are invalid: %#v", detail)
	}
	checks := make(map[string]string, len(rawChecks))
	for name, raw := range rawChecks {
		check, ok := raw.(map[string]any)
		state, stateOK := check["state"].(string)
		if !ok || !stateOK || state == "" {
			t.Fatalf("canonical health check %q is invalid: %#v", name, raw)
		}
		checks[name] = state
	}
	return checks
}

func waitForIssue49CanonicalHealth(t *testing.T, ctx context.Context, queryer issue49SQLQueryer, want bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var detail map[string]any
	for time.Now().Before(deadline) {
		detail = loadIssue49Health(t, ctx, queryer)
		if ready, ok := detail["ready"].(bool); ok && ready == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("canonical health did not reach %t: %#v", want, detail)
}

func loadIssue49InstallSQL(t *testing.T, artifactRoot string) ([]byte, []byte) {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate Issue 49 test source")
	}
	trackedPath := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", "..", "..", "extensions", "synchro-pg", "sql", "synchro_pg--0.3.0.sql"))
	tracked, err := os.ReadFile(trackedPath)
	if err != nil {
		t.Fatalf("read tracked generated pgrx SQL: %v", err)
	}
	var candidates []string
	err = filepath.WalkDir(artifactRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() && entry.Name() == "synchro_pg--0.3.0.sql" {
			candidates = append(candidates, path)
		}
		return nil
	})
	if err != nil || len(candidates) != 1 {
		t.Fatalf("locate packaged pgrx SQL: count=%d err=%v", len(candidates), err)
	}
	artifact, err := os.ReadFile(candidates[0])
	if err != nil {
		t.Fatalf("read packaged pgrx SQL: %v", err)
	}
	return tracked, artifact
}

func injectIssue49FenceKeyVersionMismatch(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
	database *sql.DB,
	recordID string,
	mismatchedRecordID string,
	laterRecordID string,
) issue49FenceMismatch {
	t.Helper()
	barrier, err := database.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin fence mismatch barrier: %v", err)
	}
	defer barrier.Rollback()
	if _, err := barrier.ExecContext(ctx, "LOCK TABLE synchro.sync_wal_worker_state IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatalf("lock fence mismatch worker startup boundary: %v", err)
	}
	workerPID, err := harness.Operator().CurrentWALWorkerPID(ctx)
	if err != nil {
		t.Fatalf("observe fence mismatch WAL worker: %v", err)
	}
	var terminated bool
	if err := database.QueryRowContext(ctx, "SELECT pg_catalog.pg_terminate_backend($1)", workerPID).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("stop WAL worker before fence mismatch: terminated=%t err=%v", terminated, err)
	}
	workerDeadline := time.Now().Add(10 * time.Second)
	workerStopped := false
	for time.Now().Before(workerDeadline) {
		var present bool
		if err := database.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_catalog.pg_stat_activity
				WHERE datname = current_database() AND pid = $1
			)`, workerPID).Scan(&present); err != nil {
			t.Fatalf("observe stopped fence mismatch WAL worker: %v", err)
		}
		if !present {
			workerStopped = true
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !workerStopped {
		t.Fatal("WAL worker did not stop before fence mismatch injection")
	}
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'issue49-fence-mismatch')",
		recordID,
	); err != nil {
		t.Fatalf("commit fence mismatch source transaction: %v", err)
	}
	var result issue49FenceMismatch
	if err := barrier.QueryRowContext(ctx, `
		SELECT fence_id::text, row_version::text
		FROM synchro.sync_write_fences
		WHERE new_record_id = $1 AND coverage = 'pending'`, recordID).Scan(
		&result.FenceID,
		&result.OriginalVersion,
	); err != nil {
		t.Fatalf("load original fence key and version: %v", err)
	}
	if err := barrier.QueryRowContext(ctx, `
		UPDATE synchro.sync_write_fences
		SET new_record_id = $2, row_version = gen_random_uuid()
		WHERE fence_id = $1::uuid
		RETURNING row_version::text`, result.FenceID, mismatchedRecordID).Scan(&result.MismatchedVersion); err != nil {
		t.Fatalf("inject durable fence key and version mismatch: %v", err)
	}
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'issue49-after-fence-mismatch')",
		laterRecordID,
	); err != nil {
		t.Fatalf("commit source transaction after fence mismatch: %v", err)
	}
	if err := barrier.Commit(); err != nil {
		t.Fatalf("release fence mismatch materialization boundary: %v", err)
	}
	return result
}

func observeIssue49FenceMismatchBlock(
	t *testing.T,
	ctx context.Context,
	queryer issue49SQLQueryer,
	mismatch issue49FenceMismatch,
	recordID string,
	mismatchedRecordID string,
) issue49FenceMismatchBlock {
	t.Helper()
	var result issue49FenceMismatchBlock
	if err := queryer.QueryRowContext(ctx, `
		SELECT fence.coverage = 'pending',
		       fence.new_record_id = $2,
		       fence.row_version::text = $3,
		       EXISTS (SELECT 1 FROM synchro.sync_wal_events event WHERE event.fence_id = fence.fence_id),
		       EXISTS (SELECT 1 FROM synchro.sync_changelog change WHERE change.record_id = $4),
		       EXISTS (SELECT 1 FROM synchro.sync_changelog change WHERE change.record_id = $2)
		FROM synchro.sync_write_fences fence
		WHERE fence.fence_id = $1::uuid`, mismatch.FenceID, mismatchedRecordID, mismatch.MismatchedVersion, recordID).Scan(
		&result.FencePending,
		&result.KeyChanged,
		&result.VersionChanged,
		&result.EventMaterialized,
		&result.OriginalRecordMaterialized,
		&result.MismatchedRecordMaterialized,
	); err != nil {
		t.Fatalf("observe blocking fence key and version mismatch: %v", err)
	}
	return result
}

func waitForIssue49FenceCorrelation(t *testing.T, ctx context.Context, database *sql.DB, recordID string, want int64) issue49FenceCorrelation {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var result issue49FenceCorrelation
	var err error
	for time.Now().Before(deadline) {
		err = database.QueryRowContext(ctx, `
			WITH selected AS (
				SELECT fence.*
				FROM synchro.sync_write_fences fence
				WHERE fence.old_record_id = $1 OR fence.new_record_id = $1
			), correlated AS (
			SELECT fence.*,
			       event.fence_id AS event_fence_id,
			       event.stream_generation AS event_stream_generation,
			       event.commit_lsn AS event_commit_lsn,
			       event.event_ordinal AS wal_event_ordinal,
			       event.registration_kind AS event_kind,
				       event.relation_id AS event_relation_id,
				       event.physical_schema AS event_schema,
				       event.physical_relation AS event_relation,
				       event.physical_relation_oid AS event_oid,
				       event.operation AS event_operation
				FROM selected fence
				LEFT JOIN synchro.sync_wal_events event ON event.fence_id = fence.fence_id
			)
			SELECT count(*),
			       count(event_fence_id),
			       count(DISTINCT fence_id),
		       count(*) FILTER (WHERE event_fence_id IS NULL
		          OR event_kind <> registration_kind OR event_relation_id <> relation_id
		          OR event_schema <> physical_schema OR event_relation <> physical_relation
		          OR event_oid <> physical_relation_oid OR event_operation <> operation),
		       count(*) FILTER (WHERE NOT EXISTS (
		          SELECT 1 FROM synchro.sync_changelog change
		          WHERE change.stream_generation = event_stream_generation
		            AND change.commit_lsn = event_commit_lsn
		            AND change.event_ordinal = correlated.wal_event_ordinal
		            AND change.relation_id = correlated.relation_id
		            AND change.record_id = COALESCE(correlated.new_record_id, correlated.old_record_id)
		       )),
		       count(*) FILTER (WHERE NOT EXISTS (
		          SELECT 1 FROM synchro.sync_changelog change
		          WHERE change.stream_generation = event_stream_generation
		            AND change.commit_lsn = event_commit_lsn
		            AND change.event_ordinal = correlated.wal_event_ordinal
		            AND change.relation_id = correlated.relation_id
		            AND change.row_version = correlated.row_version
		       )),
		       string_agg(operation, ',' ORDER BY transaction_xid::text::bigint, dml_ordinal),
			       string_agg(dml_ordinal::text, ',' ORDER BY transaction_xid::text::bigint, dml_ordinal)
			FROM correlated`, recordID).Scan(
			&result.Fences,
			&result.Events,
			&result.UniqueFenceIDs,
			&result.Mismatches,
			&result.KeyMismatches,
			&result.VersionMismatches,
			&result.Operations,
			&result.DMLOrdinals,
		)
		if err == nil && result.Fences == want && result.Events == want {
			return result
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("fence correlation did not materialize: observation=%#v err=%v", result, err)
	return issue49FenceCorrelation{}
}

func waitForIssue49CaptureDependencyFence(t *testing.T, ctx context.Context, database *sql.DB, recordID string) bool {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var distinct bool
	var err error
	for time.Now().Before(deadline) {
		err = database.QueryRowContext(ctx, `
			SELECT fence.registration_kind = 'capture_dependency'
			       AND fence.table_id IS NULL
			       AND fence.new_capture_key IS NOT NULL
			       AND event.registration_kind = fence.registration_kind
			       AND event.relation_id = fence.relation_id
			       AND event.operation = fence.operation
			FROM synchro.sync_write_fences fence
			JOIN synchro.sync_wal_events event ON event.fence_id = fence.fence_id
			WHERE fence.new_capture_key->>'id' = $1
			ORDER BY fence.created_at DESC LIMIT 1`, recordID).Scan(&distinct)
		if err == nil && distinct {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !errors.Is(err, sql.ErrNoRows) && err != nil {
		t.Fatalf("observe capture-dependency fence: %v", err)
	}
	return false
}

func assertIssue49CapturePending(t *testing.T, status int, response map[string]any) {
	t.Helper()
	assertIssue49ProtocolError(t, status, response, http.StatusServiceUnavailable, "capture_pending", true)
}

func assertIssue49ProtocolError(t *testing.T, status int, response map[string]any, wantStatus int, wantCode string, wantRetryable bool) {
	t.Helper()
	if status != wantStatus || len(response) != 1 {
		t.Fatalf("protocol error status or shape is invalid: status=%d response=%#v", status, response)
	}
	errorBody, ok := response["error"].(map[string]any)
	if !ok || errorBody["code"] != wantCode || errorBody["retryable"] != wantRetryable {
		t.Fatalf("protocol error is invalid: want_code=%q want_retryable=%t response=%#v", wantCode, wantRetryable, response)
	}
}

func issue49CloneScopes(scopes map[string]any) map[string]any {
	clone := make(map[string]any, len(scopes))
	for scopeID, cursor := range scopes {
		clone[scopeID] = cursor
	}
	return clone
}

func issue49CheckpointMapsEqual(left, right map[string]blackbox.ClientCheckpointObservation) bool {
	if len(left) != len(right) {
		return false
	}
	for scopeID, leftCheckpoint := range left {
		rightCheckpoint, ok := right[scopeID]
		if !ok || !sameCheckpointPosition(leftCheckpoint, rightCheckpoint) {
			return false
		}
	}
	return true
}

func issue49ScopeSetEquals(encoded string, expected ...string) bool {
	var scopes []string
	if json.Unmarshal([]byte(encoded), &scopes) != nil {
		return false
	}
	values := make([]any, len(scopes))
	for index, scope := range scopes {
		values[index] = scope
	}
	return issue49AnyScopeSetEquals(values, expected...)
}

func issue49AnyScopeSetEquals(actual []any, expected ...string) bool {
	if len(actual) != len(expected) {
		return false
	}
	want := make(map[string]struct{}, len(expected))
	for _, scope := range expected {
		want[scope] = struct{}{}
	}
	for _, raw := range actual {
		scope, ok := raw.(string)
		if !ok {
			return false
		}
		if _, ok := want[scope]; !ok {
			return false
		}
		delete(want, scope)
	}
	return len(want) == 0
}
