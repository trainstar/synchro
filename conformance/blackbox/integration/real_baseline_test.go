package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/observer"
)

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func TestRealClass3ProjectionBootstrap(t *testing.T) {
	if !*provision || !*install {
		t.Fatal("TestRealClass3ProjectionBootstrap requires --provision --install")
	}
	environment, err := blackbox.LoadEnvironment()
	if err != nil {
		t.Fatalf("load real harness environment: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	harness, err := blackbox.Provision(ctx, blackbox.HarnessConfig{Environment: environment})
	if err != nil {
		t.Fatalf("provision real harness: %v", err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := harness.Close(closeContext); err != nil {
			t.Errorf("close real harness: %v", err)
		}
	})

	historicalID := "00000000-0000-0000-0000-000000009401"
	catchupID := "00000000-0000-0000-0000-000000009402"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_late_registration (id, owner_id, value) VALUES ($1, $2, $3)",
		historicalID, "diagnostic-user", "historical",
	); err != nil {
		t.Fatalf("insert historical projection bootstrap row: %v", err)
	}
	if err := harness.Source().ExecContext(ctx, `
		INSERT INTO cf_late_registration (id, owner_id, value)
		SELECT ('10000000-0000-4000-8000-' || lpad(value::text, 12, '0'))::uuid,
		       'diagnostic-user',
		       'historical-filler-' || value::text
		FROM generate_series(1, 2048) value`); err != nil {
		t.Fatalf("insert projection bootstrap staging rows: %v", err)
	}
	if err := harness.Operator().RegisterLateSourceTable(ctx); err != nil {
		t.Fatalf("register populated late source: %v", err)
	}
	generation, err := harness.Operator().PendingLateSourceRegistryGeneration(ctx)
	if err != nil {
		t.Fatalf("load pending late source generation: %v", err)
	}
	barrierControl, err := harness.Operator().NewProjectionBootstrapBarrier()
	if err != nil {
		t.Fatalf("block projection bootstrap barrier: %v", err)
	}
	t.Cleanup(func() { _ = barrierControl.Close() })

	type bootstrapOutcome struct {
		result blackbox.ProjectionBootstrapResult
		err    error
	}
	completed := make(chan bootstrapOutcome, 1)
	go func() {
		result, bootstrapErr := harness.Operator().RunProjectionBootstrap(ctx, generation)
		completed <- bootstrapOutcome{result: result, err: bootstrapErr}
	}()
	candidateObserved := false
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case outcome := <-completed:
			t.Fatalf("projection bootstrap ended before candidate catch-up: %v", outcome.err)
		default:
		}
		_, candidatePresent, observeErr := harness.Operator().ObservePreparingReset(ctx)
		if observeErr != nil {
			t.Fatalf("observe projection bootstrap candidate: %v", observeErr)
		}
		if candidatePresent {
			candidateObserved = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !candidateObserved {
		t.Fatal("projection bootstrap candidate slot was not observed")
	}
	barrierContext, barrierCancel := context.WithTimeout(ctx, 15*time.Second)
	defer barrierCancel()
	if err := barrierControl.QueueBarrier(barrierContext); err != nil {
		select {
		case outcome := <-completed:
			t.Fatalf("queue projection bootstrap barrier: %v; bootstrap result: %v", err, outcome.err)
		default:
			t.Fatalf("queue projection bootstrap barrier: %v", err)
		}
	}
	if err := barrierControl.WaitForBarrier(ctx); err != nil {
		t.Fatalf("wait for projection bootstrap barrier: %v", err)
	}
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_late_registration (id, owner_id, value) VALUES ($1, $2, $3)",
		catchupID, "diagnostic-user", "candidate-catchup",
	); err != nil {
		t.Fatalf("insert candidate catch-up row: %v", err)
	}
	if err := barrierControl.ReleaseBarrier(); err != nil {
		t.Fatalf("release projection bootstrap barrier: %v", err)
	}

	var outcome bootstrapOutcome
	select {
	case outcome = <-completed:
	case <-ctx.Done():
		t.Fatalf("projection bootstrap did not complete; %s", harness.FailureDiagnostics())
	}
	if outcome.err != nil {
		t.Fatalf("run projection bootstrap: %v; %s", outcome.err, harness.FailureDiagnostics())
	}
	result := outcome.result
	if result.RegistryGeneration != generation || result.SourceStreamGeneration == "" || result.ActiveSlotName == "" {
		t.Fatalf("projection bootstrap identity is invalid: %#v", result)
	}
	if result.CandidateSlotName == result.ActiveSlotName || result.ActivationBarrier == "" {
		t.Fatalf("projection bootstrap boundary is invalid: %#v", result)
	}
	if result.SchemaVersion == nil || result.SchemaHash == nil || len(*result.SchemaHash) != 64 {
		t.Fatalf("projection bootstrap manifest identity is invalid: %#v", result)
	}
	if !slices.Equal(result.AffectedScopes, []string{"user:diagnostic-user"}) {
		t.Fatalf("projection bootstrap affected scopes = %#v", result.AffectedScopes)
	}

	observation, err := harness.Operator().ObserveProjectionBootstrap(
		ctx,
		result.BootstrapID,
		historicalID,
		catchupID,
	)
	if err != nil {
		t.Fatalf("observe projection bootstrap: %v", err)
	}
	if observation.Lifecycle != "cleanup_complete" ||
		!observation.StreamUnchanged ||
		!observation.ActiveSlotUnchanged ||
		!observation.CandidateSlotAbsent ||
		!observation.RegistryActive ||
		!observation.ManifestPublished ||
		!observation.HistoricalRecordPresent ||
		!observation.CatchupRecordPresent ||
		!observation.HistoricalMembershipPresent ||
		!observation.CatchupMembershipPresent ||
		observation.CatchupFenceCoverage != "projection_bootstrap" ||
		!observation.CatchupFenceProvenanceMatches ||
		!observation.NoPendingFences ||
		!observation.StageCleared {
		t.Fatalf("projection bootstrap observation is incomplete: %#v", observation)
	}
}

func TestRealClass3ProjectionBootstrapRecoversAfterProcessTermination(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	historicalID := "00000000-0000-0000-0000-00000000a701"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_late_registration (id, owner_id, value) VALUES ($1, $2, $3)",
		historicalID,
		"diagnostic-user",
		"interrupted-bootstrap",
	); err != nil {
		t.Fatalf("insert interrupted projection bootstrap row: %v", err)
	}
	if err := harness.Operator().RegisterLateSourceTable(ctx); err != nil {
		t.Fatalf("register interrupted projection bootstrap source: %v", err)
	}
	generation, err := harness.Operator().PendingLateSourceRegistryGeneration(ctx)
	if err != nil {
		t.Fatalf("load interrupted projection bootstrap generation: %v", err)
	}

	type bootstrapOutcome struct {
		result blackbox.ProjectionBootstrapResult
		err    error
	}
	processContext, terminateProcess := context.WithCancel(ctx)
	firstCompleted := make(chan bootstrapOutcome, 1)
	go func() {
		result, runErr := harness.Operator().RunProjectionBootstrap(processContext, generation)
		firstCompleted <- bootstrapOutcome{result: result, err: runErr}
	}()
	t.Cleanup(terminateProcess)

	var interrupted blackbox.ActiveProjectionBootstrapObservation
	observed := false
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case outcome := <-firstCompleted:
			t.Fatalf("projection bootstrap completed before termination: %#v, %v", outcome.result, outcome.err)
		default:
		}
		observation, present, observeErr := harness.Operator().ObserveActiveProjectionBootstrap(ctx)
		if observeErr != nil {
			t.Fatalf("observe active projection bootstrap: %v", observeErr)
		}
		if present && observation.CandidateSlotPresent && observation.Lifecycle != "activated" {
			interrupted = observation
			observed = true
			terminateProcess()
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !observed {
		terminateProcess()
		t.Fatal("projection bootstrap did not expose a pre-activation process termination boundary")
	}
	select {
	case outcome := <-firstCompleted:
		if outcome.err == nil {
			t.Fatalf("terminated projection bootstrap returned success: %#v", outcome.result)
		}
	case <-ctx.Done():
		t.Fatal("terminated projection bootstrap process did not exit")
	}
	if err := harness.Operator().WaitForCandidateOperationRelease(ctx); err != nil {
		t.Fatalf("wait for terminated projection bootstrap lock release: %v", err)
	}

	stalled, present, err := harness.Operator().ObserveActiveProjectionBootstrap(ctx)
	if err != nil {
		t.Fatalf("observe interrupted projection bootstrap state: %v", err)
	}
	if !present || stalled.BootstrapID != interrupted.BootstrapID || stalled.Lifecycle == "activated" {
		t.Fatalf("interrupted projection bootstrap state is invalid: %#v", stalled)
	}

	recovered, err := harness.Operator().RunProjectionBootstrap(ctx, generation)
	if err != nil {
		t.Fatalf("recover interrupted projection bootstrap: %v; %s", err, harness.FailureDiagnostics())
	}
	if recovered.BootstrapID == interrupted.BootstrapID || recovered.CandidateSlotName == interrupted.CandidateSlotName {
		t.Fatal("projection bootstrap recovery reused a discarded pre-activation candidate")
	}
	oldState, err := harness.Operator().ObserveProjectionBootstrapRecovery(ctx, interrupted.BootstrapID)
	if err != nil {
		t.Fatalf("observe discarded projection bootstrap: %v", err)
	}
	if oldState.Lifecycle != "aborted" || !oldState.CandidateSlotAbsent || !oldState.StageCleared {
		t.Fatalf("discarded projection bootstrap cleanup is incomplete: %#v", oldState)
	}
	newState, err := harness.Operator().ObserveProjectionBootstrapRecovery(ctx, recovered.BootstrapID)
	if err != nil {
		t.Fatalf("observe recovered projection bootstrap: %v", err)
	}
	if newState.Lifecycle != "cleanup_complete" || !newState.CandidateSlotAbsent || !newState.StageCleared {
		t.Fatalf("recovered projection bootstrap cleanup is incomplete: %#v", newState)
	}
}

func TestRealWALPipeline(t *testing.T) {
	if !*provision || !*install {
		t.Fatal("TestRealWALPipeline requires --provision --install")
	}
	environment, err := blackbox.LoadEnvironment()
	if err != nil {
		t.Fatalf("load real harness environment: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	harness, err := blackbox.Provision(ctx, blackbox.HarnessConfig{Environment: environment})
	if err != nil {
		t.Fatalf("provision real harness: %v", err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := harness.Close(closeContext); err != nil {
			t.Errorf("close real harness: %v", err)
		}
	})
	createRealCheckpoint(t, ctx, harness)

	firstID := "00000000-0000-0000-0000-000000009101"
	secondID := "00000000-0000-0000-0000-000000009102"
	err = harness.Source().CommitInReverseBeginOrder(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		[]any{firstID, "diagnostic-user", "first-begun-last-committed"},
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		[]any{secondID, "diagnostic-user", "second-begun-first-committed"},
	)
	if err != nil {
		t.Fatalf("commit source transactions: %v", err)
	}

	var observation blackbox.WALPipelineObservation
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		observation, err = harness.Operator().ObserveWALRecords(ctx, []string{firstID, secondID})
		if err != nil {
			t.Fatalf("observe WAL records: %v", err)
		}
		if len(observation.Records) == 2 && observation.ContiguousAcknowledged {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(observation.Records) != 2 {
		t.Fatalf("WAL record count = %d, want 2; %s", len(observation.Records), harness.FailureDiagnostics())
	}
	if observation.Records[0].RecordID != secondID || observation.Records[1].RecordID != firstID {
		t.Fatalf("WAL commit order = [%s, %s], want [%s, %s]", observation.Records[0].RecordID, observation.Records[1].RecordID, secondID, firstID)
	}
	if !observation.WorkerRunning || observation.BlockingPoison || !observation.ContiguousAcknowledged {
		t.Fatalf("WAL pipeline state = %#v", observation)
	}
	for _, record := range observation.Records {
		if record.CommitLSN == "" || record.EndLSN == "" || record.FenceCoverage != "materialized" {
			t.Fatalf("WAL record is incomplete: %#v", record)
		}
		if record.EventOrdinal != 0 || record.EffectOrdinal != 0 {
			t.Fatalf("WAL source ordinals are invalid: %#v", record)
		}
		if !uuidPattern.MatchString(record.RowVersion) {
			t.Fatalf("WAL row version is not opaque UUID: %q", record.RowVersion)
		}
	}

	documentID := "00000000-0000-0000-0000-000000009201"
	firstMemberID := "00000000-0000-0000-0000-000000009202"
	secondMemberID := "00000000-0000-0000-0000-000000009203"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_documents (id, owner_id, title) VALUES ($1, $2, $3)",
		documentID, "document-owner-before", "membership dependency",
	); err != nil {
		t.Fatalf("insert membership dependency source: %v", err)
	}
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_document_members (id, document_id, member_id) VALUES ($1, $2, $3)",
		firstMemberID, documentID, "document-member-one",
	); err != nil {
		t.Fatalf("insert membership dependency target: %v", err)
	}
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_document_members (id, document_id, member_id) VALUES ($1, $2, $3)",
		secondMemberID, documentID, "document-member-two",
	); err != nil {
		t.Fatalf("insert second membership dependency target: %v", err)
	}
	waitForMembershipBuckets(t, ctx, harness, firstMemberID, []string{"user:document-member-one", "user:document-owner-before"})
	waitForMembershipBuckets(t, ctx, harness, secondMemberID, []string{"user:document-member-two", "user:document-owner-before"})
	transaction, err := harness.Source().BeginTx(ctx)
	if err != nil {
		t.Fatalf("begin membership dependency transaction: %v", err)
	}
	if _, err := transaction.ExecContext(
		ctx,
		"UPDATE cf_document_members SET member_id = $1, updated_at = clock_timestamp() WHERE id = $2",
		"document-member-one-after", firstMemberID,
	); err != nil {
		_ = transaction.Rollback()
		t.Fatalf("update membership dependency target: %v", err)
	}
	if _, err := transaction.ExecContext(
		ctx,
		"UPDATE cf_documents SET owner_id = $1, updated_at = clock_timestamp() WHERE id = $2",
		"document-owner-after", documentID,
	); err != nil {
		_ = transaction.Rollback()
		t.Fatalf("update membership dependency source: %v", err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit membership dependency transaction: %v", err)
	}
	waitForMembershipBuckets(t, ctx, harness, firstMemberID, []string{"user:document-member-one-after", "user:document-owner-after"})
	waitForMembershipBuckets(t, ctx, harness, secondMemberID, []string{"user:document-member-two", "user:document-owner-after"})
	effects, err := harness.Operator().ObserveDependencyEffects(
		ctx,
		documentID,
		[]string{documentID, firstMemberID, secondMemberID},
	)
	if err != nil {
		t.Fatalf("observe membership dependency effects: %v", err)
	}
	assertCanonicalDependencyEffects(t, effects)

	accessID := "00000000-0000-0000-0000-000000009204"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_document_access (id, document_id, owner_id) VALUES ($1, $2, $3)",
		accessID, documentID, "document-access-before",
	); err != nil {
		t.Fatalf("insert capture dependency source: %v", err)
	}
	waitForMembershipBuckets(t, ctx, harness, firstMemberID, []string{"user:document-access-before", "user:document-member-one-after", "user:document-owner-after"})
	waitForMembershipBuckets(t, ctx, harness, secondMemberID, []string{"user:document-access-before", "user:document-member-two", "user:document-owner-after"})
	if err := harness.Source().ExecContext(
		ctx,
		"UPDATE cf_document_access SET owner_id = $1 WHERE id = $2",
		"document-access-after", accessID,
	); err != nil {
		t.Fatalf("update capture dependency source: %v", err)
	}
	waitForMembershipBuckets(t, ctx, harness, firstMemberID, []string{"user:document-access-after", "user:document-member-one-after", "user:document-owner-after"})
	waitForMembershipBuckets(t, ctx, harness, secondMemberID, []string{"user:document-access-after", "user:document-member-two", "user:document-owner-after"})
	var capture blackbox.CaptureDependencyObservation
	deadline = time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		capture, err = harness.Operator().ObserveCaptureDependency(ctx, accessID)
		if err == nil && capture.CurrentOwnerID == "document-access-after" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("observe capture dependency projection: %v; %s", err, harness.FailureDiagnostics())
	}
	if capture.RegistrationKind != "capture_dependency" || !capture.TableIDAbsent || capture.ProjectionOwnerID != "document-access-after" || capture.CurrentOwnerID != "document-access-after" {
		t.Fatalf("capture dependency projection is invalid: %#v", capture)
	}
	if capture.FenceCoverage != "materialized" || capture.DirectEffectCount != 0 {
		t.Fatalf("capture dependency fence or direct effect is invalid: %#v", capture)
	}
	captureEffects, err := harness.Operator().ObserveCaptureDependencyEffects(
		ctx,
		accessID,
		[]string{firstMemberID, secondMemberID},
	)
	if err != nil {
		t.Fatalf("observe capture dependency effects: %v", err)
	}
	assertCaptureDependencyEffects(t, captureEffects, firstMemberID, secondMemberID)

	if err := harness.Operator().InjectRegisteredTruncate(ctx); err != nil {
		t.Fatalf("commit registered truncate control: %v", err)
	}
	laterID := "00000000-0000-0000-0000-000000009301"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		laterID, "diagnostic-user", "must-remain-blocked",
	); err != nil {
		t.Fatalf("commit source transaction after poison: %v", err)
	}
	var poison blackbox.WALPoisonObservation
	deadline = time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		poison, err = harness.Operator().ObserveBlockingPoison(ctx, laterID)
		if err == nil && poison.LaterFencePending && poison.WorkerBlocked {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("observe blocking WAL poison: %v; %s", err, harness.FailureDiagnostics())
	}
	if poison.FailureClass != "truncate_unsupported" || poison.RelationID == "" || poison.CommitLSN == "" {
		t.Fatalf("WAL poison identity is incomplete: %#v", poison)
	}
	if !poison.RelationIDMatchesRegistry || !poison.AcknowledgementBlocked || poison.LaterRecordMaterialized {
		t.Fatalf("WAL poison did not block contiguous processing: %#v", poison)
	}
	if !poison.LaterFencePending || !poison.WorkerBlocked || !poison.ReadinessBlocked || !poison.PoisonCheckFailed {
		t.Fatalf("WAL poison did not remain fail-closed: %#v", poison)
	}
	if poison.WALLagSeconds < 0 {
		t.Fatalf("WAL poison lag is invalid: %#v", poison)
	}
	interrupted, err := harness.Operator().CreateInterruptedStreamReset(ctx)
	if err != nil {
		t.Fatalf("create interrupted stream reset: %v", err)
	}
	preparing, candidatePresent, err := harness.Operator().ObservePreparingReset(ctx)
	if err != nil || !preparing || !candidatePresent {
		t.Fatalf("interrupted stream reset state is incomplete: preparing=%t candidate=%t err=%v", preparing, candidatePresent, err)
	}
	if err := harness.Operator().RecoverInterruptedStreamReset(ctx); err != nil {
		t.Fatalf("recover interrupted stream reset: %v", err)
	}
	preparing, candidatePresent, err = harness.Operator().ObservePreparingReset(ctx)
	if err != nil || preparing || candidatePresent {
		t.Fatalf("interrupted stream reset was not discarded: preparing=%t candidate=%t err=%v reset=%s", preparing, candidatePresent, err, interrupted.ResetID)
	}

	lockedBaselineID := "00000000-0000-0000-0000-000000009303"
	blockedSource, err := harness.Source().BeginTx(ctx)
	if err != nil {
		t.Fatalf("begin pre-reset source transaction: %v", err)
	}
	if _, err := blockedSource.ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		lockedBaselineID, "diagnostic-user", "must-precede-reset-snapshot",
	); err != nil {
		_ = blockedSource.Rollback()
		t.Fatalf("insert pre-reset source transaction: %v", err)
	}
	type resetOutcome struct {
		result blackbox.StreamResetResult
		err    error
	}
	resetResult := make(chan resetOutcome, 1)
	go func() {
		result, resetErr := harness.Operator().RunStreamReset(ctx)
		resetResult <- resetOutcome{result: result, err: resetErr}
	}()
	preparingObserved := false
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		preparing, candidatePresent, observeErr := harness.Operator().ObservePreparingReset(ctx)
		if observeErr != nil {
			_ = blockedSource.Rollback()
			t.Fatalf("observe reset lock ordering: %v", observeErr)
		}
		if candidatePresent {
			_ = blockedSource.Rollback()
			t.Fatal("reset candidate slot appeared before an earlier source transaction committed")
		}
		if preparing {
			preparingObserved = true
			time.Sleep(250 * time.Millisecond)
			_, candidatePresent, observeErr = harness.Operator().ObservePreparingReset(ctx)
			if observeErr != nil || candidatePresent {
				_ = blockedSource.Rollback()
				t.Fatalf("reset did not remain blocked before slot creation: present=%t err=%v", candidatePresent, observeErr)
			}
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !preparingObserved {
		_ = blockedSource.Rollback()
		t.Fatal("reset did not reach source-lock acquisition")
	}
	lockedDocumentID := "00000000-0000-0000-0000-000000009304"
	secondRelationWrite := make(chan error, 1)
	go func() {
		_, writeErr := blockedSource.ExecContext(
			ctx,
			"INSERT INTO cf_documents (id, owner_id, title) VALUES ($1, $2, $3)",
			lockedDocumentID, "diagnostic-user", "same-transaction-second-relation",
		)
		secondRelationWrite <- writeErr
	}()
	select {
	case writeErr := <-secondRelationWrite:
		if writeErr != nil {
			_ = blockedSource.Rollback()
			t.Fatalf("write second relation while reset waits: %v", writeErr)
		}
	case <-time.After(5 * time.Second):
		_ = blockedSource.Rollback()
		t.Fatal("reset deadlocked with a multi-relation source transaction")
	}
	if err := blockedSource.Commit(); err != nil {
		t.Fatalf("commit pre-reset source transaction: %v", err)
	}
	var reset blackbox.StreamResetResult
	select {
	case outcome := <-resetResult:
		reset = outcome.result
		err = outcome.err
	case <-time.After(30 * time.Second):
		t.Fatal("controlled stream reset did not resume after source commit")
	}
	if err != nil {
		t.Fatalf("run controlled stream reset: %v; %s", err, harness.FailureDiagnostics())
	}
	if reset.ResetID == "" || reset.SourceStreamGeneration == reset.TargetStreamGeneration || reset.OldSlotName == reset.CandidateSlotName {
		t.Fatalf("stream reset identity is invalid: %#v", reset)
	}
	var resetObservation blackbox.StreamResetObservation
	deadline = time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		resetObservation, err = harness.Operator().ObserveStreamReset(ctx, reset.ResetID, "cf_items", laterID)
		if err == nil && resetObservation.ReadinessReady {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("observe controlled stream reset: %v; %s", err, harness.FailureDiagnostics())
	}
	if resetObservation.Lifecycle != "cleanup_complete" || resetObservation.ActiveSlotName != reset.CandidateSlotName || resetObservation.ActiveStreamGeneration != reset.TargetStreamGeneration {
		t.Fatalf("stream reset did not activate its candidate: %#v", resetObservation)
	}
	if !resetObservation.OldSlotAbsent || !resetObservation.CandidateSlotValid || !resetObservation.PoisonCleared {
		t.Fatalf("stream reset slot lifecycle is invalid: %#v", resetObservation)
	}
	if !resetObservation.BaselineRecordPresent || !resetObservation.BaselineProvenanceMatches || !resetObservation.BaselineMembershipPresent || resetObservation.FenceCoverage != "reset_baseline" {
		t.Fatalf("stream reset baseline is incomplete: %#v", resetObservation)
	}
	if !resetObservation.NoSyntheticEvent || !resetObservation.NoSyntheticEffect || !resetObservation.CheckpointsInvalidated || !resetObservation.ReadinessReady {
		t.Fatalf("stream reset exposed invalid incremental state: %#v", resetObservation)
	}
	lockedBaseline, err := harness.Operator().ObserveStreamReset(ctx, reset.ResetID, "cf_items", lockedBaselineID)
	if err != nil || !lockedBaseline.BaselineRecordPresent || !lockedBaseline.BaselineProvenanceMatches || lockedBaseline.FenceCoverage != "reset_baseline" || !lockedBaseline.NoSyntheticEvent || !lockedBaseline.NoSyntheticEffect {
		t.Fatalf("pre-snapshot source transaction is absent from reset baseline: %#v, %v", lockedBaseline, err)
	}
	lockedDocument, err := harness.Operator().ObserveStreamReset(ctx, reset.ResetID, "cf_documents", lockedDocumentID)
	if err != nil || !lockedDocument.BaselineRecordPresent || !lockedDocument.BaselineProvenanceMatches || lockedDocument.FenceCoverage != "reset_baseline" || !lockedDocument.NoSyntheticEvent || !lockedDocument.NoSyntheticEffect {
		t.Fatalf("multi-relation source transaction is absent from reset baseline: %#v, %v", lockedDocument, err)
	}
	capture, err = harness.Operator().ObserveCaptureDependency(ctx, accessID)
	if err != nil || capture.CurrentOwnerID != "document-access-after" {
		t.Fatalf("stream reset lost capture dependency baseline: %#v, %v", capture, err)
	}

	postResetID := "00000000-0000-0000-0000-000000009302"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		postResetID, "diagnostic-user", "post-reset-wal",
	); err != nil {
		t.Fatalf("commit post-reset source transaction: %v", err)
	}
	deadline = time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		observation, err = harness.Operator().ObserveWALRecords(ctx, []string{postResetID})
		if err == nil && len(observation.Records) == 1 && observation.ContiguousAcknowledged {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil || len(observation.Records) != 1 || !observation.WorkerRunning || observation.BlockingPoison || !observation.ContiguousAcknowledged {
		t.Fatalf("post-reset WAL did not resume: %#v, %v; %s", observation, err, harness.FailureDiagnostics())
	}

	if err := harness.Operator().AdvanceActiveSlotPastDurableBoundary(ctx); err != nil {
		t.Fatalf("advance active slot past durable boundary: %v; %s", err, harness.FailureDiagnostics())
	}
	deadline = time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		resetObservation, err = harness.Operator().ObserveStreamReset(ctx, reset.ResetID, "cf_items", laterID)
		if err == nil && !resetObservation.ReadinessReady && strings.Contains(resetObservation.ReadinessFailures, "materialization_progress") {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil || resetObservation.ReadinessReady || !strings.Contains(resetObservation.ReadinessFailures, "materialization_progress") {
		t.Fatalf("ahead replication slot remained ready: %#v, %v", resetObservation, err)
	}
}

func TestRealWALDecodeFailureRepairsSameIdentity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)

	poisonRecordID := "00000000-0000-4000-8000-00000000d001"
	if err := harness.Operator().InjectDecoderMetadataChange(ctx, poisonRecordID); err != nil {
		t.Fatalf("commit decoder poison transaction: %v", err)
	}

	laterRecordID := "00000000-0000-4000-8000-00000000d002"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		laterRecordID,
		"diagnostic-user",
		"decode-repair-later",
	); err != nil {
		t.Fatalf("insert transaction after decoder poison: %v", err)
	}

	var beforeRestart blackbox.WALPoisonObservation
	var err error
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		beforeRestart, err = harness.Operator().ObserveBlockingPoison(ctx, laterRecordID)
		if err == nil && beforeRestart.FailureClass == "decode_failed" &&
			beforeRestart.LaterFencePending && beforeRestart.WorkerBlocked {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("observe decoder poison: %v; %s", err, harness.FailureDiagnostics())
	}
	if beforeRestart.FailureClass != "decode_failed" || beforeRestart.CommitLSN == "" ||
		beforeRestart.RelationID != "" || beforeRestart.RelationIDMatchesRegistry ||
		!beforeRestart.AcknowledgementBlocked || beforeRestart.LaterRecordMaterialized ||
		!beforeRestart.LaterFencePending || !beforeRestart.WorkerBlocked ||
		!beforeRestart.ReadinessBlocked || !beforeRestart.PoisonCheckFailed {
		t.Fatalf("decoder poison did not remain fail-closed: %#v", beforeRestart)
	}

	if err := harness.RestartPostgres(ctx); err != nil {
		t.Fatalf("restart PostgreSQL with decoder poison: %v", err)
	}
	var afterRestart blackbox.WALPoisonObservation
	deadline = time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		afterRestart, err = harness.Operator().ObserveBlockingPoison(ctx, laterRecordID)
		if err == nil && afterRestart.WorkerBlocked {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil || afterRestart.FailureClass != "decode_failed" ||
		afterRestart.CommitLSN != beforeRestart.CommitLSN || !afterRestart.LaterFencePending ||
		!afterRestart.AcknowledgementBlocked || afterRestart.LaterRecordMaterialized {
		t.Fatalf("decoder poison changed across restart: before=%#v after=%#v err=%v", beforeRestart, afterRestart, err)
	}

	retryRequested, err := harness.Operator().RetryWALPoison(ctx)
	if err != nil || !retryRequested {
		t.Fatalf("request decoder poison retry: requested=%t err=%v", retryRequested, err)
	}
	var recovered blackbox.WALPipelineObservation
	deadline = time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		recovered, err = harness.Operator().ObserveWALRecords(ctx, []string{poisonRecordID, laterRecordID})
		if err == nil && len(recovered.Records) == 2 && recovered.ContiguousAcknowledged {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil || len(recovered.Records) != 2 || recovered.BlockingPoison ||
		!recovered.WorkerRunning || !recovered.ContiguousAcknowledged ||
		!recovered.AcknowledgementMatchesObservedEnd || !recovered.SlotMatchesObservedEnd {
		t.Fatalf("decoder poison recovery did not resume the contiguous stream: %#v, %v; %s", recovered, err, harness.FailureDiagnostics())
	}
	if recovered.Records[0].RecordID != poisonRecordID || recovered.Records[1].RecordID != laterRecordID {
		t.Fatalf("decoder poison recovery order is invalid: %#v", recovered.Records)
	}
	lifecycle, err := harness.Operator().ObserveWALPoisonRecovery(ctx, poisonRecordID)
	if err != nil {
		t.Fatalf("observe decoder poison recovery: %v", err)
	}
	if lifecycle.PoisonCount != 1 || lifecycle.FailureClass != "decode_failed" ||
		lifecycle.Lifecycle != "repaired" || lifecycle.AttemptCount != 2 ||
		!lifecycle.RetryRequested || !lifecycle.Resolved || !lifecycle.SameCommitPosition {
		t.Fatalf("decoder poison recovery lifecycle is invalid: %#v", lifecycle)
	}
}

func createRealCheckpoint(t *testing.T, ctx context.Context, harness *blackbox.Harness) {
	t.Helper()
	const clientID = "wal-reset-checkpoint-client"
	token, err := harness.DiagnosticBearerToken(time.Now())
	if err != nil {
		t.Fatalf("sign checkpoint client token: %v", err)
	}
	status, connected := postSync(t, ctx, harness.AdapterURL(), token, "/sync/connect", map[string]any{
		"client_id":         clientID,
		"platform":          "conformance",
		"app_version":       "0.3.0",
		"protocol_version":  3,
		"schema":            map[string]any{"version": 0, "hash": ""},
		"scope_set_version": 0,
		"known_scopes":      map[string]any{},
	})
	if status != http.StatusOK {
		t.Fatalf("checkpoint client connect status = %d: %#v", status, connected)
	}
	schema, ok := connected["schema"].(map[string]any)
	if !ok {
		t.Fatalf("checkpoint client schema is invalid: %#v", connected)
	}
	delete(schema, "action")
	delete(schema, "reason")
	generation, ok := connected["client_generation"].(float64)
	if !ok || generation <= 0 {
		t.Fatalf("checkpoint client generation is invalid: %#v", connected)
	}
	scopeSetVersion, ok := connected["scope_set_version"].(float64)
	if !ok || scopeSetVersion <= 0 {
		t.Fatalf("checkpoint client scope version is invalid: %#v", connected)
	}
	scopes := make(map[string]any)
	if delta, ok := connected["scopes"].(map[string]any); ok {
		if additions, ok := delta["add"].([]any); ok {
			for _, raw := range additions {
				assignment, ok := raw.(map[string]any)
				if !ok {
					continue
				}
				id, _ := assignment["id"].(string)
				if id != "" {
					scopes[id] = map[string]any{"cursor": assignment["cursor"]}
				}
			}
		}
	}
	status, pulled := postSync(t, ctx, harness.AdapterURL(), token, "/sync/pull", map[string]any{
		"client_id":         clientID,
		"client_generation": generation,
		"schema":            schema,
		"scope_set_version": scopeSetVersion,
		"scopes":            scopes,
		"limit":             100,
	})
	if status != http.StatusOK {
		t.Fatalf("checkpoint client pull status = %d: %#v", status, pulled)
	}
	present, err := harness.Operator().HasClientCheckpoint(ctx, clientID)
	if err != nil || !present {
		t.Fatalf("checkpoint client did not create durable progress: %v", err)
	}
}

func waitForMembershipBuckets(t *testing.T, ctx context.Context, harness *blackbox.Harness, recordID string, expected []string) {
	t.Helper()
	var buckets []string
	var err error
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		buckets, err = harness.Operator().ObserveMembershipBuckets(ctx, "cf_document_members", recordID)
		if err != nil {
			t.Fatalf("observe membership buckets: %v", err)
		}
		if slices.Equal(buckets, expected) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("membership buckets = %v, want %v; %s", buckets, expected, harness.FailureDiagnostics())
}

func assertCanonicalDependencyEffects(t *testing.T, effects []blackbox.MembershipEffectObservation) {
	t.Helper()
	if len(effects) != 8 {
		t.Fatalf("membership dependency effect count = %d, want 8: %#v", len(effects), effects)
	}
	for _, effect := range effects {
		if effect.BucketID == "user:document-member-two" {
			t.Fatalf("dependency-only retained scope emitted an effect: %#v", effect)
		}
	}
	for _, bucket := range []struct {
		id        string
		operation int16
		count     int
	}{
		{id: "user:document-member-one", operation: 3, count: 1},
		{id: "user:document-member-one-after", operation: 1, count: 1},
		{id: "user:document-owner-after", operation: 1, count: 3},
		{id: "user:document-owner-before", operation: 3, count: 3},
	} {
		var selected []blackbox.MembershipEffectObservation
		for _, effect := range effects {
			if effect.BucketID == bucket.id {
				selected = append(selected, effect)
			}
		}
		if len(selected) != bucket.count {
			t.Fatalf("membership dependency bucket %q effect count = %d, want %d: %#v", bucket.id, len(selected), bucket.count, selected)
		}
		for index, effect := range selected {
			if effect.Operation != bucket.operation || effect.EventOrdinal != 1 || effect.EffectOrdinal != int32(index) {
				t.Fatalf("membership dependency effect is not canonical: %#v", effect)
			}
			if index > 0 {
				prior := selected[index-1]
				if prior.TableID > effect.TableID || prior.TableID == effect.TableID && prior.RecordID > effect.RecordID {
					t.Fatalf("membership dependency row order is not canonical: %#v", selected)
				}
			}
		}
	}
}

func assertCaptureDependencyEffects(t *testing.T, effects []blackbox.MembershipEffectObservation, firstMemberID, secondMemberID string) {
	t.Helper()
	if len(effects) != 4 {
		t.Fatalf("capture dependency effect count = %d, want 4: %#v", len(effects), effects)
	}
	wantedRecords := []string{firstMemberID, secondMemberID}
	for _, bucket := range []struct {
		id        string
		operation int16
	}{
		{id: "user:document-access-after", operation: 1},
		{id: "user:document-access-before", operation: 3},
	} {
		var selected []blackbox.MembershipEffectObservation
		for _, effect := range effects {
			if effect.BucketID == bucket.id {
				selected = append(selected, effect)
			}
		}
		if len(selected) != 2 {
			t.Fatalf("capture dependency bucket %q effect count = %d, want 2: %#v", bucket.id, len(selected), selected)
		}
		for index, effect := range selected {
			if effect.Operation != bucket.operation || effect.EventOrdinal != 0 || effect.RecordID != wantedRecords[index] || !uuidPattern.MatchString(effect.TableID) {
				t.Fatalf("capture dependency effect is not canonical: %#v", effect)
			}
		}
	}
}

var (
	provision = flag.Bool("provision", false, "provision the isolated PostgreSQL diagnostic harness")
	install   = flag.Bool("install", false, "install the verified extension bundle")
)

func TestRealHTTPHarness(t *testing.T) {
	if !*provision || !*install {
		t.Fatal("TestRealHTTPHarness requires --provision --install")
	}
	environment, err := blackbox.LoadEnvironment()
	if err != nil {
		t.Fatalf("load real harness environment: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	harness, err := blackbox.Provision(ctx, blackbox.HarnessConfig{Environment: environment})
	if err != nil {
		t.Fatalf("provision real harness: %v", err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := harness.Close(closeContext); err != nil {
			t.Errorf("close real harness: %v", err)
		}
	})
	if harness.RestartCount() < 1 {
		t.Fatal("real harness did not restart PostgreSQL after extension installation")
	}
	observerDatabase, err := harness.OpenObserver(ctx)
	if err != nil {
		t.Fatalf("open observer connection: %v", err)
	}
	t.Cleanup(func() {
		if err := observerDatabase.Close(); err != nil {
			t.Errorf("close observer connection: %v", err)
		}
	})
	verifyRealObserverBoundary(t, ctx, observerDatabase, environment.Observer.Username)
	token, err := harness.DiagnosticBearerToken(time.Now())
	if err != nil {
		t.Fatalf("sign diagnostic token: %v", err)
	}
	request := map[string]any{
		"client_id":         "blackbox-http-client",
		"platform":          "conformance",
		"app_version":       "0.3.0",
		"protocol_version":  2,
		"schema":            map[string]any{"version": 0, "hash": ""},
		"scope_set_version": 0,
		"known_scopes":      map[string]any{},
	}
	status, response := postConnect(t, ctx, harness.AdapterURL(), token, request)
	if status != http.StatusUpgradeRequired {
		t.Fatalf("protocol 2 connect status = %d, want 426: %#v", status, response)
	}
	errorBody, ok := response["error"].(map[string]any)
	if !ok || errorBody["code"] != "upgrade_required" {
		t.Fatalf("protocol 2 connect error is invalid: %#v", response)
	}

	request["protocol_version"] = 3
	status, response = postConnect(t, ctx, harness.AdapterURL(), token, request)
	if status != http.StatusOK {
		t.Fatalf("protocol 3 connect status = %d, want 200: %#v; %s", status, response, harness.FailureDiagnostics())
	}
	if response["protocol_version"] != float64(3) {
		t.Fatalf("protocol 3 connect response is invalid: %#v", response)
	}
	if generation, ok := response["client_generation"].(float64); !ok || generation <= 0 {
		t.Fatalf("protocol 3 connect client generation is invalid: %#v", response)
	}
}

func TestRealS05SelectiveRebuildPreservesCheckpoints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	client := connectRealProtocolClient(t, ctx, harness, token, "s05-checkpoint-client")

	initial := observeCheckpointMap(t, ctx, harness, client.ID)
	assertDiagnosticCheckpointScopes(t, initial)
	rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8000-00000000a501")
	rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8000-00000000a502")

	userRowID := "00000000-0000-4000-8000-00000000a511"
	globalRowID := "00000000-0000-4000-8000-00000000a512"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		userRowID, "diagnostic-user", "s05-user-row",
	); err != nil {
		t.Fatalf("insert S-05 user source row: %v", err)
	}
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_global_items (id, value) VALUES ($1, $2)",
		globalRowID, "s05-global-row",
	); err != nil {
		t.Fatalf("insert S-05 global source row: %v", err)
	}

	userTable := requireRealTable(t, client, "cf_items")
	globalTable := requireRealTable(t, client, "cf_global_items")
	pullUntilRealRecords(t, ctx, harness, token, client, []realRecordExpectation{
		{
			scopeID:  "user:diagnostic-user",
			table:    userTable,
			recordID: userRowID,
			value:    "s05-user-row",
		},
		{
			scopeID:  "cf:global",
			table:    globalTable,
			recordID: globalRowID,
			value:    "s05-global-row",
		},
	})
	acknowledgeRealClientCursors(t, ctx, harness, token, client)

	before := observeCheckpointMap(t, ctx, harness, client.ID)
	assertDiagnosticCheckpointScopes(t, before)
	advanced := false
	for scopeID, initialCheckpoint := range initial {
		if !sameCheckpointPosition(initialCheckpoint, before[scopeID]) {
			advanced = true
		}
	}
	if !advanced {
		t.Fatal("S-05 pull did not advance any checkpoint from its initial position")
	}
	unreadGlobalRowID := "00000000-0000-4000-8000-00000000a513"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_global_items (id, value) VALUES ($1, $2)",
		unreadGlobalRowID, "s05-unread-global-row",
	); err != nil {
		t.Fatalf("insert S-05 unread global row: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_global_items", unreadGlobalRowID)

	_, finalScopeCursor := rebuildRealScope(
		t,
		ctx,
		harness,
		token,
		client,
		"user:diagnostic-user",
		"00000000-0000-4000-8000-00000000a503",
	)
	if finalScopeCursor == "" {
		t.Fatal("S-05 selective rebuild returned an empty final_scope_cursor")
	}
	after := observeCheckpointMap(t, ctx, harness, client.ID)
	assertDiagnosticCheckpointScopes(t, after)
	if before["user:diagnostic-user"] != after["user:diagnostic-user"] {
		t.Fatal("S-05 selective rebuild changed the target checkpoint")
	}
	if before["cf:global"] != after["cf:global"] {
		t.Fatal("S-05 selective rebuild changed the unrelated checkpoint")
	}

	delivery := pullRealClient(t, ctx, harness, token, client)
	scopeSetVersion, ok := delivery["scope_set_version"].(float64)
	if !ok || int64(scopeSetVersion) != client.ScopeSetVersion {
		t.Fatal("S-05 selective rebuild changed the scope-set version")
	}
	changes := requireRealChanges(t, delivery)
	if len(changes) != 1 {
		t.Fatalf("S-05 unrelated unread delivery count = %d, want 1", len(changes))
	}
	requireRealPullChange(t, changes, "cf:global", globalTable, unreadGlobalRowID, "s05-unread-global-row")
	afterDelivery := observeCheckpointMap(t, ctx, harness, client.ID)
	if !sameCheckpointPosition(before["cf:global"], afterDelivery["cf:global"]) {
		t.Fatal("S-05 unrelated checkpoint advanced before client acknowledgement")
	}
	acknowledgeRealClientCursors(t, ctx, harness, token, client)
	final := observeCheckpointMap(t, ctx, harness, client.ID)
	if sameCheckpointPosition(before["cf:global"], final["cf:global"]) {
		t.Fatal("S-05 unrelated unread history did not advance after acknowledgement")
	}
}

func TestRealS16ConcurrentPushCASIgnoresClientTime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)

	recordID := "00000000-0000-4000-8000-00000000a611"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		recordID, "diagnostic-user", "s16-base",
	); err != nil {
		t.Fatalf("insert S-16 source row: %v", err)
	}
	waitForRealWALRecord(t, ctx, harness, recordID)

	pastClient := connectRealProtocolClient(t, ctx, harness, token, "s16-past-client")
	futureClient := connectRealProtocolClient(t, ctx, harness, token, "s16-future-client")
	pastRecords, _ := rebuildRealScope(
		t, ctx, harness, token, pastClient, "user:diagnostic-user", "00000000-0000-4000-8000-00000000a621",
	)
	futureRecords, _ := rebuildRealScope(
		t, ctx, harness, token, futureClient, "user:diagnostic-user", "00000000-0000-4000-8000-00000000a622",
	)
	pastTable := requireRealTable(t, pastClient, "cf_items")
	futureTable := requireRealTable(t, futureClient, "cf_items")
	if pastTable != futureTable {
		t.Fatal("S-16 clients received different logical table identities")
	}
	baseVersion := requireRebuildRecordVersion(t, pastRecords, pastTable, recordID, "s16-base")
	if otherVersion := requireRebuildRecordVersion(t, futureRecords, futureTable, recordID, "s16-base"); otherVersion != baseVersion {
		t.Fatal("S-16 clients did not receive the same base_version")
	}

	control, err := harness.Operator().HoldItemForConcurrentPush(ctx, recordID)
	if err != nil {
		t.Fatalf("hold S-16 source row: %v", err)
	}
	t.Cleanup(func() {
		if err := control.Release(); err != nil {
			t.Errorf("release S-16 overlap control: %v", err)
		}
	})

	attempts := []realPushAttempt{
		{
			client:        pastClient,
			batchID:       "00000000-0000-4000-8000-00000000a631",
			mutationID:    "00000000-0000-4000-8000-00000000a632",
			clientVersion: "2000-01-01T00:00:00.000000Z",
			value:         "s16-past",
		},
		{
			client:        futureClient,
			batchID:       "00000000-0000-4000-8000-00000000a633",
			mutationID:    "00000000-0000-4000-8000-00000000a634",
			clientVersion: "2099-01-01T00:00:00.000000Z",
			value:         "s16-future",
		},
	}
	results := make(chan realPushResult, len(attempts))
	startAttempt := func(attempt realPushAttempt) {
		go func() {
			status, body, requestErr := executeSyncRequest(
				ctx,
				harness.AdapterURL(),
				token,
				"/sync/push",
				realPushPayload(attempt, pastTable, recordID, baseVersion),
			)
			results <- realPushResult{attempt: attempt, status: status, body: body, err: requestErr}
		}()
	}
	startAttempt(attempts[0])
	overlapContext, overlapCancel := context.WithTimeout(ctx, 10*time.Second)
	err = control.WaitForBlockedPushes(overlapContext, 1)
	overlapCancel()
	if err != nil {
		_ = control.Release()
		t.Fatalf("observe first S-16 blocked push: %v", err)
	}
	startAttempt(attempts[1])
	overlapContext, overlapCancel = context.WithTimeout(ctx, 10*time.Second)
	err = control.WaitForBlockedPushes(overlapContext, len(attempts))
	overlapCancel()
	if err != nil {
		_ = control.Release()
		t.Fatalf("observe second S-16 blocked push: %v", err)
	}
	if err := control.Release(); err != nil {
		t.Fatalf("release S-16 source row: %v", err)
	}

	completed := make([]realPushResult, 0, len(attempts))
	for range attempts {
		select {
		case result := <-results:
			if result.err != nil {
				t.Fatalf("execute S-16 concurrent push: %v", result.err)
			}
			if result.status != http.StatusOK {
				t.Fatalf("S-16 concurrent push status = %d, want 200", result.status)
			}
			completed = append(completed, result)
		case <-ctx.Done():
			t.Fatal("S-16 concurrent pushes did not complete")
		}
	}

	type classifiedOutcome struct {
		attempt realPushAttempt
		outcome map[string]any
	}
	var accepted []classifiedOutcome
	var rejected []classifiedOutcome
	for _, result := range completed {
		if result.body["batch_id"] != result.attempt.batchID {
			t.Fatal("S-16 push did not echo its batch identity")
		}
		acceptedOutcomes := requireOutcomeList(t, result.body, "accepted")
		rejectedOutcomes := requireOutcomeList(t, result.body, "rejected")
		if len(acceptedOutcomes)+len(rejectedOutcomes) != 1 {
			t.Fatal("S-16 push did not return exactly one mutation outcome")
		}
		for _, outcome := range acceptedOutcomes {
			accepted = append(accepted, classifiedOutcome{attempt: result.attempt, outcome: outcome})
		}
		for _, outcome := range rejectedOutcomes {
			rejected = append(rejected, classifiedOutcome{attempt: result.attempt, outcome: outcome})
		}
	}
	if len(accepted) != 1 || len(rejected) != 1 {
		t.Fatal("S-16 concurrent pushes did not select exactly one winner")
	}
	winner := accepted[0]
	loser := rejected[0]
	if winner.attempt.mutationID != attempts[0].mutationID || loser.attempt.mutationID != attempts[1].mutationID {
		t.Fatal("S-16 client time overrode deterministic lock order")
	}
	if winner.outcome["mutation_id"] != winner.attempt.mutationID || winner.outcome["status"] != "applied" {
		t.Fatal("S-16 winner outcome is invalid")
	}
	winnerVersion, ok := winner.outcome["server_version"].(string)
	if !ok || !uuidPattern.MatchString(winnerVersion) || winnerVersion == baseVersion ||
		winnerVersion == attempts[0].clientVersion || winnerVersion == attempts[1].clientVersion {
		t.Fatal("S-16 winner returned an invalid server version")
	}
	assertOutcomeValue(t, winner.outcome, pastTable, winner.attempt.value)
	if loser.outcome["mutation_id"] != loser.attempt.mutationID ||
		loser.outcome["status"] != "conflict" || loser.outcome["code"] != "version_conflict" {
		t.Fatal("S-16 loser did not return version_conflict")
	}
	if loser.outcome["server_version"] != winnerVersion {
		t.Fatal("S-16 loser did not return the winning server version")
	}
	assertOutcomeValue(t, loser.outcome, pastTable, winner.attempt.value)

	finalState, err := harness.Operator().ObserveItemStateMatch(
		ctx,
		recordID,
		winner.attempt.value,
		winnerVersion,
	)
	if err != nil {
		t.Fatalf("observe S-16 final source state: %v", err)
	}
	if !finalState.Live || !finalState.ValueMatches || !finalState.VersionMatches {
		t.Fatal("S-16 final source state does not match the winning mutation")
	}
}

type realProtocolClient struct {
	ID              string
	Generation      int64
	Schema          map[string]any
	ScopeSetVersion int64
	Scopes          map[string]any
	Tables          map[string]realProtocolTable
}

type realProtocolTable struct {
	ID              string
	PrimaryKeyField string
	ValueField      string
}

type realRecordExpectation struct {
	scopeID  string
	table    realProtocolTable
	recordID string
	value    string
}

type realPushAttempt struct {
	client        *realProtocolClient
	batchID       string
	mutationID    string
	clientVersion string
	value         string
}

type realPushResult struct {
	attempt realPushAttempt
	status  int
	body    map[string]any
	err     error
}

func provisionRealProofHarness(t *testing.T, ctx context.Context) (*blackbox.Harness, string) {
	t.Helper()
	if !*provision || !*install {
		t.Fatal("real proof requires --provision --install")
	}
	environment, err := blackbox.LoadEnvironment()
	if err != nil {
		t.Fatalf("load real proof environment: %v", err)
	}
	harness, err := blackbox.Provision(ctx, blackbox.HarnessConfig{Environment: environment})
	if err != nil {
		t.Fatalf("provision real proof harness: %v", err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := harness.Close(closeContext); err != nil {
			t.Errorf("close real proof harness: %v", err)
		}
	})
	token, err := harness.DiagnosticBearerToken(time.Now())
	if err != nil {
		t.Fatalf("sign real proof token: %v", err)
	}
	return harness, token
}

func connectRealProtocolClient(t *testing.T, ctx context.Context, harness *blackbox.Harness, token, clientID string) *realProtocolClient {
	t.Helper()
	status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/connect", map[string]any{
		"client_id":         clientID,
		"platform":          "conformance",
		"app_version":       "0.3.0",
		"protocol_version":  3,
		"schema":            map[string]any{"version": 0, "hash": ""},
		"scope_set_version": 0,
		"known_scopes":      map[string]any{},
	})
	if status != http.StatusOK {
		t.Fatalf("real protocol client connect status = %d, want 200", status)
	}
	generation, ok := response["client_generation"].(float64)
	if !ok || generation <= 0 {
		t.Fatal("real protocol client generation is invalid")
	}
	scopeSetVersion, ok := response["scope_set_version"].(float64)
	if !ok || scopeSetVersion <= 0 {
		t.Fatal("real protocol client scope version is invalid")
	}
	schemaDescriptor, ok := response["schema"].(map[string]any)
	if !ok {
		t.Fatal("real protocol client schema is invalid")
	}
	schemaVersion, versionOK := schemaDescriptor["version"].(float64)
	schemaHash, hashOK := schemaDescriptor["hash"].(string)
	if !versionOK || schemaVersion <= 0 || !hashOK || len(schemaHash) != 64 {
		t.Fatal("real protocol client schema reference is invalid")
	}
	definition, ok := response["schema_definition"].(map[string]any)
	if !ok {
		t.Fatal("real protocol client schema definition is missing")
	}
	tables := parseRealProtocolTables(t, definition)
	scopes := make(map[string]any)
	delta, ok := response["scopes"].(map[string]any)
	if !ok {
		t.Fatal("real protocol client scope assignment is invalid")
	}
	additions, ok := delta["add"].([]any)
	if !ok {
		t.Fatal("real protocol client scope additions are invalid")
	}
	for _, raw := range additions {
		assignment, ok := raw.(map[string]any)
		if !ok {
			t.Fatal("real protocol client scope addition is invalid")
		}
		scopeID, ok := assignment["id"].(string)
		if !ok || scopeID == "" {
			t.Fatal("real protocol client scope identity is invalid")
		}
		scopes[scopeID] = map[string]any{"cursor": assignment["cursor"]}
	}
	assigned := make([]string, 0, len(scopes))
	for scopeID := range scopes {
		assigned = append(assigned, scopeID)
	}
	slices.Sort(assigned)
	if !slices.Equal(assigned, []string{"cf:global", "user:diagnostic-user"}) {
		t.Fatal("real protocol client did not receive both diagnostic scopes")
	}
	return &realProtocolClient{
		ID:              clientID,
		Generation:      int64(generation),
		Schema:          map[string]any{"version": int64(schemaVersion), "hash": schemaHash},
		ScopeSetVersion: int64(scopeSetVersion),
		Scopes:          scopes,
		Tables:          tables,
	}
}

func parseRealProtocolTables(t *testing.T, definition map[string]any) map[string]realProtocolTable {
	t.Helper()
	rawTables, ok := definition["tables"].([]any)
	if !ok {
		t.Fatal("real protocol manifest tables are invalid")
	}
	tables := make(map[string]realProtocolTable)
	for _, rawTable := range rawTables {
		tableObject, ok := rawTable.(map[string]any)
		if !ok {
			t.Fatal("real protocol manifest table is invalid")
		}
		name, _ := tableObject["name"].(string)
		if name != "cf_items" && name != "cf_global_items" {
			continue
		}
		table := realProtocolTable{}
		table.ID, _ = tableObject["table_id"].(string)
		table.PrimaryKeyField, _ = tableObject["primary_key_field_id"].(string)
		fields, ok := tableObject["fields"].([]any)
		if !ok {
			t.Fatal("real protocol manifest fields are invalid")
		}
		for _, rawField := range fields {
			field, ok := rawField.(map[string]any)
			if !ok {
				t.Fatal("real protocol manifest field is invalid")
			}
			if field["name"] == "value" {
				table.ValueField, _ = field["field_id"].(string)
			}
		}
		if !uuidPattern.MatchString(table.ID) || !uuidPattern.MatchString(table.PrimaryKeyField) || !uuidPattern.MatchString(table.ValueField) {
			t.Fatal("real protocol manifest identity is invalid")
		}
		tables[name] = table
	}
	return tables
}

func requireRealTable(t *testing.T, client *realProtocolClient, name string) realProtocolTable {
	t.Helper()
	table, ok := client.Tables[name]
	if !ok {
		t.Fatal("required real protocol table is missing")
	}
	return table
}

func rebuildRealScope(t *testing.T, ctx context.Context, harness *blackbox.Harness, token string, client *realProtocolClient, scopeID, rebuildID string) ([]map[string]any, string) {
	t.Helper()
	var cursor any
	var records []map[string]any
	for page := 0; page < 16; page++ {
		status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/rebuild", map[string]any{
			"client_id":         client.ID,
			"client_generation": client.Generation,
			"schema":            client.Schema,
			"scope":             scopeID,
			"rebuild_id":        rebuildID,
			"cursor":            cursor,
			"limit":             100,
		})
		if status != http.StatusOK {
			t.Fatalf("real scope rebuild status = %d, want 200", status)
		}
		if response["scope"] != scopeID {
			t.Fatal("real scope rebuild returned the wrong scope")
		}
		pageRecords, ok := response["records"].([]any)
		if !ok {
			t.Fatal("real scope rebuild records are invalid")
		}
		for _, rawRecord := range pageRecords {
			record, ok := rawRecord.(map[string]any)
			if !ok {
				t.Fatal("real scope rebuild record is invalid")
			}
			records = append(records, record)
		}
		hasMore, ok := response["has_more"].(bool)
		if !ok {
			t.Fatal("real scope rebuild finality is invalid")
		}
		if hasMore {
			nextCursor, ok := response["cursor"].(string)
			if !ok || nextCursor == "" {
				t.Fatal("real scope rebuild continuation is invalid")
			}
			cursor = nextCursor
			continue
		}
		finalScopeCursor, ok := response["final_scope_cursor"].(string)
		if !ok || finalScopeCursor == "" {
			t.Fatal("real scope rebuild final_scope_cursor is invalid")
		}
		client.Scopes[scopeID] = map[string]any{"cursor": finalScopeCursor}
		return records, finalScopeCursor
	}
	t.Fatal("real scope rebuild exceeded its page bound")
	return nil, ""
}

func pullUntilRealRecords(t *testing.T, ctx context.Context, harness *blackbox.Harness, token string, client *realProtocolClient, expected []realRecordExpectation) {
	t.Helper()
	seen := make(map[string]bool, len(expected))
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
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
			for _, wanted := range expected {
				if change["scope"] != wanted.scopeID || change["table"] != wanted.table.ID {
					continue
				}
				pk, _ := change["pk"].(map[string]any)
				if pk[wanted.table.PrimaryKeyField] != wanted.recordID {
					continue
				}
				if seen[wanted.recordID] {
					t.Fatal("real pull delivered a diagnostic row more than once")
				}
				row, ok := change["row"].(map[string]any)
				version, versionOK := change["server_version"].(string)
				if !ok || row[wanted.table.ValueField] != wanted.value || !versionOK || !uuidPattern.MatchString(version) {
					t.Fatal("real pull row is invalid")
				}
				seen[wanted.recordID] = true
			}
		}
		if len(seen) == len(expected) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("real pull did not deliver all diagnostic rows")
}

func pullRealClient(t *testing.T, ctx context.Context, harness *blackbox.Harness, token string, client *realProtocolClient) map[string]any {
	t.Helper()
	status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/pull", map[string]any{
		"client_id":         client.ID,
		"client_generation": client.Generation,
		"schema":            client.Schema,
		"scope_set_version": client.ScopeSetVersion,
		"scopes":            client.Scopes,
		"limit":             100,
	})
	if status != http.StatusOK {
		t.Fatalf("real client pull status = %d, want 200", status)
	}
	rebuild, ok := response["rebuild"].([]any)
	if !ok || len(rebuild) != 0 {
		t.Fatal("real client pull requested an unexpected rebuild")
	}
	scopeCursors, ok := response["scope_cursors"].(map[string]any)
	if !ok {
		t.Fatal("real client pull cursors are invalid")
	}
	for scopeID, rawCursor := range scopeCursors {
		cursor, ok := rawCursor.(string)
		if !ok || cursor == "" {
			t.Fatal("real client pull cursor is invalid")
		}
		if _, assigned := client.Scopes[scopeID]; !assigned {
			t.Fatal("real client pull returned an unassigned scope cursor")
		}
		client.Scopes[scopeID] = map[string]any{"cursor": cursor}
	}
	return response
}

func acknowledgeRealClientCursors(t *testing.T, ctx context.Context, harness *blackbox.Harness, token string, client *realProtocolClient) {
	t.Helper()
	response := pullRealClient(t, ctx, harness, token, client)
	changes, ok := response["changes"].([]any)
	if !ok || len(changes) != 0 {
		t.Fatal("real acknowledgement pull returned unexpected changes")
	}
}

func observeCheckpointMap(t *testing.T, ctx context.Context, harness *blackbox.Harness, clientID string) map[string]blackbox.ClientCheckpointObservation {
	t.Helper()
	observations, err := harness.Operator().ObserveClientCheckpoints(ctx, clientID)
	if err != nil {
		t.Fatalf("observe real client checkpoints: %v", err)
	}
	result := make(map[string]blackbox.ClientCheckpointObservation, len(observations))
	for _, observation := range observations {
		if _, duplicate := result[observation.ScopeID]; duplicate {
			t.Fatal("real checkpoint observation contains a duplicate scope")
		}
		result[observation.ScopeID] = observation
	}
	return result
}

func assertDiagnosticCheckpointScopes(t *testing.T, checkpoints map[string]blackbox.ClientCheckpointObservation) {
	t.Helper()
	if len(checkpoints) != 2 {
		t.Fatal("real checkpoint observation does not contain exactly two scopes")
	}
	if _, ok := checkpoints["cf:global"]; !ok {
		t.Fatal("real checkpoint observation is missing the global scope")
	}
	if _, ok := checkpoints["user:diagnostic-user"]; !ok {
		t.Fatal("real checkpoint observation is missing the user scope")
	}
}

func sameCheckpointPosition(left, right blackbox.ClientCheckpointObservation) bool {
	left.UpdatedAt = ""
	right.UpdatedAt = ""
	return left == right
}

func waitForRealWALRecord(t *testing.T, ctx context.Context, harness *blackbox.Harness, recordID string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		observation, err := harness.Operator().ObserveWALRecords(ctx, []string{recordID})
		if err == nil && len(observation.Records) == 1 && observation.ContiguousAcknowledged {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("real WAL did not materialize the diagnostic row")
}

func requireRebuildRecordVersion(t *testing.T, records []map[string]any, table realProtocolTable, recordID, expectedValue string) string {
	t.Helper()
	for _, record := range records {
		if record["table"] != table.ID {
			continue
		}
		pk, _ := record["pk"].(map[string]any)
		if pk[table.PrimaryKeyField] != recordID {
			continue
		}
		row, rowOK := record["row"].(map[string]any)
		version, versionOK := record["server_version"].(string)
		if !rowOK || row[table.ValueField] != expectedValue || !versionOK || !uuidPattern.MatchString(version) {
			t.Fatal("real rebuild diagnostic row is invalid")
		}
		return version
	}
	t.Fatal("real rebuild did not return the diagnostic row")
	return ""
}

func realPushPayload(attempt realPushAttempt, table realProtocolTable, recordID, baseVersion string) map[string]any {
	return map[string]any{
		"client_id":         attempt.client.ID,
		"client_generation": attempt.client.Generation,
		"batch_id":          attempt.batchID,
		"schema":            attempt.client.Schema,
		"mutations": []map[string]any{
			{
				"mutation_id":     attempt.mutationID,
				"table":           table.ID,
				"pk":              map[string]any{table.PrimaryKeyField: recordID},
				"authored_schema": attempt.client.Schema,
				"op":              "update",
				"base_version":    baseVersion,
				"client_version":  attempt.clientVersion,
				"columns":         map[string]any{table.ValueField: attempt.value},
			},
		},
	}
}

func requireOutcomeList(t *testing.T, response map[string]any, name string) []map[string]any {
	t.Helper()
	rawOutcomes, ok := response[name].([]any)
	if !ok {
		t.Fatal("predicate mutation_outcome_array_shape failed")
	}
	outcomes := make([]map[string]any, 0, len(rawOutcomes))
	for _, rawOutcome := range rawOutcomes {
		outcome, ok := rawOutcome.(map[string]any)
		if !ok {
			t.Fatal("predicate mutation_outcome_object_shape failed")
		}
		outcomes = append(outcomes, outcome)
	}
	return outcomes
}

func assertOutcomeValue(t *testing.T, outcome map[string]any, table realProtocolTable, expectedValue string) {
	t.Helper()
	row, ok := outcome["server_row"].(map[string]any)
	if !ok || row[table.ValueField] != expectedValue {
		t.Fatal("S-16 authoritative outcome does not contain the winning value")
	}
}

func postConnect(t *testing.T, ctx context.Context, baseURL, token string, payload map[string]any) (int, map[string]any) {
	return postSync(t, ctx, baseURL, token, "/sync/connect", payload)
}

func postSync(t *testing.T, ctx context.Context, baseURL, token, path string, payload map[string]any) (int, map[string]any) {
	t.Helper()
	status, response, err := executeSyncRequest(ctx, baseURL, token, path, payload)
	if err != nil {
		t.Fatalf("execute sync request: %v", err)
	}
	return status, response
}

func executeSyncRequest(ctx context.Context, baseURL, token, path string, payload map[string]any) (int, map[string]any, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, errors.New("encode sync request failed")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, errors.New("create sync request failed")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 30 * time.Second}).Do(request)
	if err != nil {
		return 0, nil, errors.New("send sync request failed")
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return 0, nil, errors.New("read sync response failed")
	}
	var decoded map[string]any
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return 0, nil, errors.New("decode sync response failed")
	}
	return response.StatusCode, decoded, nil
}

func verifyRealObserverBoundary(t *testing.T, ctx context.Context, database *sql.DB, expectedRole string) {
	t.Helper()
	var currentRole string
	if err := database.QueryRowContext(ctx, "SELECT current_user").Scan(&currentRole); err != nil {
		t.Fatalf("read observer role identity: %v", err)
	}
	if currentRole != expectedRole {
		t.Fatalf("observer current role = %q, want %q", currentRole, expectedRole)
	}

	postgresObserver, err := observer.NewPostgres(observer.PostgresConfig{
		DB: database,
		SourceTables: []observer.SourceTable{
			{
				Name:     "global_items",
				Relation: "public.cf_global_items",
				Columns:  []string{"id", "value", "updated_at", "deleted_at"},
				OrderBy:  []string{"id"},
			},
			{
				Name:     "items",
				Relation: "public.cf_items",
				Columns:  []string{"id", "owner_id", "value", "updated_at", "deleted_at"},
				OrderBy:  []string{"id"},
			},
		},
		MaximumRows: 100,
	})
	if err != nil {
		t.Fatalf("create real PostgreSQL observer: %v", err)
	}
	snapshot, err := postgresObserver.Snapshot(ctx, observer.SnapshotRequest{
		SourceTables: []string{"global_items", "items"},
		OperationalCatalogs: []string{
			"pg_catalog.pg_replication_slots",
			"pg_catalog.pg_publication",
			"pg_catalog.pg_stat_activity",
			"pg_catalog.pg_stat_database",
		},
	})
	if err != nil {
		t.Fatalf("capture real observer snapshot: %v", err)
	}
	if len(snapshot.SourceTables) != 2 || len(snapshot.OperationalCatalogs) != 4 || len(snapshot.Functions) != 0 {
		t.Fatalf("real observer snapshot shape is invalid: %#v", snapshot)
	}

	denied := []struct {
		name      string
		statement string
	}{
		{
			name:      "source write",
			statement: "INSERT INTO public.cf_items (id, owner_id, value) VALUES ('00000000-0000-0000-0000-000000009001', 'observer', 'denied')",
		},
		{name: "internal sync-table read", statement: "SELECT count(*) FROM synchro.sync_changelog"},
		{name: "temporary object creation", statement: "CREATE TEMP TABLE observer_denied_temp (id integer)"},
		{name: "unapproved function execution", statement: "SELECT synchro.synchro_schema_manifest()"},
	}
	for _, operation := range denied {
		operation := operation
		t.Run(operation.name, func(t *testing.T) {
			requireDatabasePermissionDenied(t, ctx, database, operation.statement)
		})
	}
}

func requireDatabasePermissionDenied(t *testing.T, ctx context.Context, database *sql.DB, statement string) {
	t.Helper()
	_, err := database.ExecContext(ctx, statement)
	if err == nil {
		t.Fatal("observer database operation succeeded")
	}
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "42501" {
		t.Fatalf("observer database operation error = %v, want PostgreSQL insufficient_privilege", err)
	}
}
