package kotlin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const pendingCycleScenarioID = "SCN-PERF-PENDING-CYCLE-001"

// PendingCycleResult records direct Kotlin Android evidence for one pending mutation cycle.
type PendingCycleResult struct {
	PushCall    SynchronizationResult
	PullCall    SynchronizationResult
	ClientFacts []CaptureFacts
	ServerFacts scenarios.StateFacts
	Evidence    scenarios.PendingCycleNativeEvidence
}

// RunPendingCycleScenario executes the authored pending-cycle flow through Kotlin Android.
func RunPendingCycleScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, platform *Platform, client Client) (PendingCycleResult, error) {
	steps, err := kotlinScenarioStepMap(scenario, pendingCycleScenarioID, 7)
	if err != nil {
		return PendingCycleResult{}, err
	}
	if controller == nil || platform == nil {
		return PendingCycleResult{}, errors.New("Kotlin Android pending-cycle dependencies are unavailable")
	}
	for _, id := range []string{"STEP-PERF-PENDING-CYCLE-001", "STEP-PERF-PENDING-CYCLE-002", "STEP-PERF-PENDING-CYCLE-CAPTURE-PENDING-001", "STEP-PERF-PENDING-CYCLE-003"} {
		if err := kotlinScenarioClient(steps[scenarios.StepID(id)], client); err != nil {
			return PendingCycleResult{}, err
		}
	}
	if err := validateKotlinPendingCycleCallBindings(steps); err != nil {
		return PendingCycleResult{}, err
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return PendingCycleResult{}, fmt.Errorf("install Kotlin Android pending-cycle contract: %w", err)
	}
	if err := platform.Install(ctx, InstallRequest{Client: client, Initialization: "current"}); err != nil {
		return PendingCycleResult{}, fmt.Errorf("install Kotlin Android pending-cycle client: %w", err)
	}
	resumeWAL, err := controller.PauseWALMaterialization(ctx)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("pause Kotlin Android pending-cycle WAL materialization: %w", err)
	}
	walPaused := true
	defer func() {
		if walPaused {
			cleanupContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = resumeWAL(cleanupContext)
		}
	}()
	unprotectedCommit, err := kotlinScenarioOperation(steps, "STEP-PERF-PENDING-CYCLE-UNPROTECTED-COMMIT-001", "model/commit-source-transaction")
	if err != nil {
		return PendingCycleResult{}, err
	}
	if observation, applyErr := controller.ApplyStep(ctx, unprotectedCommit); applyErr != nil || observation.Disposition != "success" {
		return PendingCycleResult{}, fmt.Errorf("commit Kotlin Android pending-cycle unprotected row: %w", kotlinResultError(applyErr, observation.Disposition))
	}
	unprotectedAlias, err := scenarios.PendingCycleUnprotectedIdentityAlias(scenario.NativeIdentityAliases)
	if err != nil {
		return PendingCycleResult{}, err
	}
	unprotectedValues, err := controller.IdentityValues([]scenarios.NativeIdentityAlias{unprotectedAlias})
	if err != nil || len(unprotectedValues) != 1 {
		return PendingCycleResult{}, errors.New("Kotlin Android pending-cycle unprotected row has no runtime identity")
	}
	var unprotectedRuntimeID string
	if json.Unmarshal(unprotectedValues[0].RuntimeValue, &unprotectedRuntimeID) != nil || unprotectedRuntimeID == "" {
		return PendingCycleResult{}, errors.New("Kotlin Android pending-cycle unprotected runtime identity is invalid")
	}
	unprotectedAuthoredID, unprotectedValue, err := scenarios.PendingCycleUnprotectedRowTarget(unprotectedCommit, []scenarios.NativeIdentityAlias{unprotectedAlias}, unprotectedRuntimeID)
	if err != nil {
		return PendingCycleResult{}, err
	}
	beforeWrite, err := platform.scenarioSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Kotlin Android pending-cycle pre-write state: %w", err)
	}
	write, err := kotlinScenarioOperation(steps, "STEP-PERF-PENDING-CYCLE-001", "local/write")
	if err != nil {
		return PendingCycleResult{}, err
	}
	var authoredWrite struct {
		TableID string `json:"table_id"`
	}
	if json.Unmarshal(write.Payload, &authoredWrite) != nil || authoredWrite.TableID == "" {
		return PendingCycleResult{}, errors.New("Kotlin Android pending-cycle authored table identity is invalid")
	}
	deletedAtField, err := controller.ApplicationDeletedAtField(authoredWrite.TableID)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("resolve Kotlin Android pending-cycle deleted-at field: %w", err)
	}
	write, err = controller.ApplicationWrite(write)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("bind Kotlin Android pending mutation to the application schema: %w", err)
	}
	action, selector, err := decodeLocalWrite(write, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("decode Kotlin Android pending-cycle runtime target: %w", err)
	}
	recordID, ok := selector.PrimaryKey.Value.(string)
	if !ok || recordID == "" {
		return PendingCycleResult{}, errors.New("Kotlin Android pending-cycle runtime record identity is invalid")
	}
	target := scenarios.PendingCycleNativeTarget{TableName: action.TableName, PrimaryKeyField: action.PrimaryKeyField, RecordID: recordID, Value: "pending", DeletedAtField: deletedAtField}
	for field, value := range action.Fields {
		if text, ok := value.Value.(string); ok && text == target.Value {
			if target.ValueField != "" {
				return PendingCycleResult{}, errors.New("Kotlin Android pending-cycle runtime value field is ambiguous")
			}
			target.ValueField = field
		}
	}
	if target.ValueField == "" {
		return PendingCycleResult{}, errors.New("Kotlin Android pending-cycle runtime value field is absent")
	}
	target.UnprotectedAuthoredRecordID = unprotectedAuthoredID
	target.UnprotectedRecordID = unprotectedRuntimeID
	target.UnprotectedValue = unprotectedValue
	local, err := platform.ApplyStep(ctx, client, write)
	if err != nil || local.Disposition != "success" {
		return PendingCycleResult{}, fmt.Errorf("apply Kotlin Android pending mutation: %w", kotlinResultError(err, local.Disposition))
	}
	afterWrite, err := platform.scenarioSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Kotlin Android pending-cycle local write: %w", err)
	}
	if err := validateKotlinPendingCyclePostWrite(beforeWrite, afterWrite); err != nil {
		return PendingCycleResult{}, err
	}
	authoredPush, err := kotlinScenarioOperation(steps, "STEP-PERF-PENDING-CYCLE-002", "push/submit")
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("run Kotlin Android pending push: %w", err)
	}
	capturePendingPull, err := kotlinScenarioOperation(steps, "STEP-PERF-PENDING-CYCLE-CAPTURE-PENDING-001", "pull/request-page")
	if err != nil {
		return PendingCycleResult{}, err
	}
	capturePendingPull, err = kotlinPendingCycleRuntimePull(capturePendingPull)
	if err != nil {
		return PendingCycleResult{}, err
	}
	callID := "pending_push"
	state, err := platform.clientFor(client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("access Kotlin Android pending-cycle transport: %w", err)
	}
	state.mu.Lock()
	if state.session == nil {
		state.mu.Unlock()
		return PendingCycleResult{}, errors.New("Kotlin Android pending-cycle transport session is unavailable")
	}
	transportCheckpoint := state.session.Checkpoint()
	state.mu.Unlock()
	begin, err := platform.BeginCall(ctx, CallRequest{Client: client, CallID: callID, Method: "start", Operations: []scenarios.Operation{authoredPush}})
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("begin Kotlin Android pending-cycle call: %w", err)
	}
	callActive := true
	defer func() {
		if callActive {
			cleanupContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = platform.AbortCall(cleanupContext, CallRequest{Client: client, CallID: callID})
		}
	}()
	if begin.CallID != callID || begin.State != "in_flight" || begin.Completion != "" || len(begin.Steps) != 1 {
		return PendingCycleResult{}, errors.New("Kotlin Android pending-cycle call did not enter flight")
	}
	observations, err := kotlinPendingCycleTransportObservations(platform, client, transportCheckpoint)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Kotlin Android pending-cycle begin transport: %w", err)
	}
	if err := validateKotlinPendingCycleBeginTransport(observations); err != nil {
		return PendingCycleResult{}, err
	}
	if err := validateKotlinPendingCycleStepWire(scenario, "STEP-PERF-PENDING-CYCLE-002", "push", begin.Steps[0], observations[1]); err != nil {
		return PendingCycleResult{}, err
	}
	if err := controller.BindApplicationPush(authoredPush); err != nil {
		return PendingCycleResult{}, fmt.Errorf("bind Kotlin Android pending push transaction: %w", err)
	}
	pendingPullStep, err := platform.AwaitStep(ctx, AwaitRequest{Client: client, CallID: callID, Operation: capturePendingPull})
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("await Kotlin Android pending-cycle capture-pending pull: %w", err)
	}
	observations, err = kotlinPendingCycleTransportObservations(platform, client, transportCheckpoint)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Kotlin Android pending-cycle capture-pending transport: %w", err)
	}
	if err := validateKotlinPendingCycleTransport(observations, 3); err != nil {
		return PendingCycleResult{}, err
	}
	if err := validateKotlinPendingCycleStepWire(scenario, "STEP-PERF-PENDING-CYCLE-CAPTURE-PENDING-001", "pull", pendingPullStep, observations[2]); err != nil {
		return PendingCycleResult{}, err
	}
	afterPush, err := platform.scenarioSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Kotlin Android pending-cycle accepted push: %w", err)
	}
	if err := resumeWAL(ctx); err != nil {
		return PendingCycleResult{}, fmt.Errorf("resume Kotlin Android pending-cycle WAL materialization: %w", err)
	}
	walPaused = false
	unprotectedMaterialize, err := kotlinScenarioOperation(steps, "STEP-PERF-PENDING-CYCLE-UNPROTECTED-MATERIALIZE-001", "process/materialize-source-transaction")
	if err != nil {
		return PendingCycleResult{}, err
	}
	if result, processErr := controller.ProcessStep(ctx, nil, unprotectedMaterialize); processErr != nil || result.Disposition != "success" {
		return PendingCycleResult{}, fmt.Errorf("materialize Kotlin Android pending-cycle unprotected row: %w", kotlinResultError(processErr, result.Disposition))
	}
	materialize, err := kotlinScenarioOperation(steps, "STEP-PERF-PENDING-CYCLE-MATERIALIZE-001", "process/materialize-source-transaction")
	if err != nil {
		return PendingCycleResult{}, err
	}
	if result, err := controller.ProcessStep(ctx, nil, materialize); err != nil || result.Disposition != "success" {
		return PendingCycleResult{}, fmt.Errorf("materialize Kotlin Android pending mutation: %w", kotlinResultError(err, result.Disposition))
	}
	pull, err := kotlinScenarioOperation(steps, "STEP-PERF-PENDING-CYCLE-003", "pull/request-page")
	if err != nil {
		return PendingCycleResult{}, err
	}
	pull, err = kotlinPendingCycleRuntimePull(pull)
	if err != nil {
		return PendingCycleResult{}, err
	}
	beforePull, err := platform.scenarioSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Kotlin Android pending pull checkpoint: %w", err)
	}
	states, err := androidCursorScopeStates(beforePull.ScopeStates)
	if err != nil || len(states) != 1 || states[0].Cursor == nil || *states[0].Cursor == "" {
		return PendingCycleResult{}, errors.New("Kotlin Android pending pull checkpoint is invalid")
	}
	retryPullStep, err := platform.AwaitStep(ctx, AwaitRequest{Client: client, CallID: callID, Operation: pull})
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("await Kotlin Android pending-cycle retry pull: %w", err)
	}
	observations, err = kotlinPendingCycleTransportObservations(platform, client, transportCheckpoint)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Kotlin Android pending-cycle retry transport: %w", err)
	}
	if err := validateKotlinPendingCycleTransport(observations, 4); err != nil {
		return PendingCycleResult{}, err
	}
	if err := validateKotlinPendingCycleStepWire(scenario, "STEP-PERF-PENDING-CYCLE-003", "pull", retryPullStep, observations[3]); err != nil {
		return PendingCycleResult{}, err
	}
	completed, err := platform.AwaitCall(ctx, CallRequest{Client: client, CallID: callID})
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("complete Kotlin Android pending-cycle call: %w", err)
	}
	callActive = false
	if completed.CallID != callID || completed.State != "completed" || completed.Completion != "idle" {
		return PendingCycleResult{}, errors.New("Kotlin Android pending-cycle call did not complete idle")
	}
	observations, err = kotlinPendingCycleTransportObservations(platform, client, transportCheckpoint)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Kotlin Android pending-cycle completed transport: %w", err)
	}
	if err := validateKotlinPendingCycleTransport(observations, 4); err != nil {
		return PendingCycleResult{}, err
	}
	push := SynchronizationResult{
		Completion:                completed.Completion,
		Steps:                     []StepObservation{begin.Steps[0], pendingPullStep, retryPullStep},
		DurationNanoseconds:       completed.DurationNanoseconds,
		ProvenanceMaintenanceWork: completed.ProvenanceMaintenanceWork,
		ReplayedMutationCount:     completed.ReplayedMutationCount,
		transportObservations:     cloneObservations(observations[1:]),
	}
	pullCall := SynchronizationResult{
		Completion:                completed.Completion,
		Steps:                     []StepObservation{pendingPullStep, retryPullStep},
		DurationNanoseconds:       completed.DurationNanoseconds,
		ProvenanceMaintenanceWork: completed.ProvenanceMaintenanceWork,
		ReplayedMutationCount:     completed.ReplayedMutationCount,
		transportObservations: cloneObservations([]TransportObservation{
			observations[2],
			observations[3],
		}),
	}
	afterPull, err := platform.scenarioSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Kotlin Android pending-cycle pull state: %w", err)
	}
	restartPayload, err := json.Marshal(map[string]string{"user_id": client.UserID, "client_id": client.ClientID})
	if err != nil {
		return PendingCycleResult{}, errors.New("encode Kotlin Android pending-cycle restart failed")
	}
	restart := scenarios.Operation{ContractOperation: "process", Name: "restart-client", Payload: restartPayload}
	if observation, processErr := platform.ProcessStep(ctx, client, restart); processErr != nil || observation.Disposition != "success" {
		return PendingCycleResult{}, fmt.Errorf("restart Kotlin Android pending-cycle client: %w", kotlinResultError(processErr, observation.Disposition))
	}
	afterRestart, err := platform.scenarioSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Kotlin Android pending-cycle restarted state: %w", err)
	}
	found := false
	for _, expectation := range scenario.Model.ExpectedState {
		if expectation.ID != scenarios.ExpectationID("EXPECT-PERF-PENDING-CYCLE-SEMANTIC-001") {
			continue
		}
		var payload map[string]any
		if expectation.Predicate.ContractPredicate != "wire-outcome" || expectation.Predicate.Name != "canonical-wire-outcome" || expectation.StateFacts != nil || json.Unmarshal(expectation.Predicate.Payload, &payload) != nil || len(payload) != 0 {
			return PendingCycleResult{}, errors.New("Kotlin Android pending-cycle canonical wire expectation is invalid")
		}
		found = true
	}
	if !found {
		return PendingCycleResult{}, errors.New("Kotlin Android pending-cycle canonical wire expectation is absent")
	}

	updatedValue := target.Value + "-updated"
	updateBaseVersion, err := kotlinPendingCycleServerVersion(target, afterRestart)
	if err != nil {
		return PendingCycleResult{}, err
	}
	updateStep, err := scenarios.PendingCycleSynchronizedCRUDOperation(write, authoredPush, materialize, "update", target.ValueField, target.Value, updatedValue, updateBaseVersion)
	if err != nil {
		return PendingCycleResult{}, err
	}
	if observation, lifecycleErr := platform.Lifecycle(ctx, LifecycleRequest{Client: client, Operation: "stop"}); lifecycleErr != nil || observation.Disposition != "success" {
		return PendingCycleResult{}, fmt.Errorf("stop Kotlin Android pending-cycle client before update: %w", kotlinResultError(lifecycleErr, observation.Disposition))
	}
	if observation, applyErr := platform.ApplyStep(ctx, client, updateStep.LocalWrite); applyErr != nil || observation.Disposition != "success" {
		return PendingCycleResult{}, fmt.Errorf("apply Kotlin Android pending-cycle update: %w", kotlinResultError(applyErr, observation.Disposition))
	}
	beforeCleanup, err := platform.scenarioSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Kotlin Android pending-cycle update intent: %w", err)
	}

	assignment, err := scenarios.PendingCycleCleanupAssignment(client.UserID, client.ClientID)
	if err != nil {
		return PendingCycleResult{}, err
	}
	if observation, assignmentErr := controller.ApplyStep(ctx, assignment); assignmentErr != nil || observation.Disposition != "success" {
		return PendingCycleResult{}, fmt.Errorf("assign Kotlin Android pending-cycle cleanup scope: %w", kotlinResultError(assignmentErr, observation.Disposition))
	}
	faultedUpdate, err := scenarios.PendingCycleTemporaryUnavailablePush(updateStep.ApplicationPush)
	if err != nil {
		return PendingCycleResult{}, err
	}
	releaseFault, armed, err := platform.armTemporaryUnavailablePush([]scenarios.Operation{faultedUpdate})
	if err != nil || !armed {
		return PendingCycleResult{}, fmt.Errorf("arm Kotlin Android pending-cycle temporary-unavailable push: %w", err)
	}
	defer releaseFault()
	cleanupCall, err := kotlinScenarioCall(ctx, platform, client, "start")
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("run Kotlin Android pending-cycle scope cleanup: %w", kotlinResultError(err, cleanupCall.Completion))
	}
	if err := validateKotlinPendingCycleCleanupCall(cleanupCall); err != nil {
		return PendingCycleResult{}, err
	}
	afterCleanup, err := platform.scenarioSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Kotlin Android pending-cycle scope cleanup: %w", err)
	}
	if observation, lifecycleErr := platform.Lifecycle(ctx, LifecycleRequest{Client: client, Operation: "stop"}); lifecycleErr != nil || observation.Disposition != "success" {
		return PendingCycleResult{}, fmt.Errorf("stop Kotlin Android pending-cycle client during push backoff: %w", kotlinResultError(lifecycleErr, observation.Disposition))
	}
	releaseFault()
	afterUpdate, err := runKotlinPendingCycleGeneratedPush(ctx, controller, platform, client, updateStep, "update")
	if err != nil {
		return PendingCycleResult{}, err
	}
	deleteBaseVersion, err := kotlinPendingCycleServerVersion(target, afterUpdate)
	if err != nil {
		return PendingCycleResult{}, err
	}
	deleteStep, err := scenarios.PendingCycleSynchronizedCRUDOperation(write, authoredPush, materialize, "delete", target.ValueField, "", "", deleteBaseVersion)
	if err != nil {
		return PendingCycleResult{}, err
	}
	if observation, lifecycleErr := platform.Lifecycle(ctx, LifecycleRequest{Client: client, Operation: "stop"}); lifecycleErr != nil || observation.Disposition != "success" {
		return PendingCycleResult{}, fmt.Errorf("stop Kotlin Android pending-cycle client before delete: %w", kotlinResultError(lifecycleErr, observation.Disposition))
	}
	if observation, applyErr := platform.ApplyStep(ctx, client, deleteStep.LocalWrite); applyErr != nil || observation.Disposition != "success" {
		return PendingCycleResult{}, fmt.Errorf("apply Kotlin Android pending-cycle delete: %w", kotlinResultError(applyErr, observation.Disposition))
	}
	beforeDelete, err := platform.scenarioSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Kotlin Android pending-cycle delete intent: %w", err)
	}
	afterDelete, err := runKotlinPendingCycleGeneratedPush(ctx, controller, platform, client, deleteStep, "delete")
	if err != nil {
		return PendingCycleResult{}, err
	}
	clientFacts, err := platform.Capture(ctx, []Client{client}, []string{"application-rows", "pending-mutations", "rejected-mutations", "checkpoints", "provenance"})
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Kotlin Android pending-cycle client state: %w", err)
	}
	serverCaptures, err := controller.Capture(ctx, []string{client.Key}, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return PendingCycleResult{}, fmt.Errorf("capture Kotlin Android pending-cycle server state: %w", kotlinResultError(err, ""))
	}
	evidence, err := kotlinPendingCycleEvidence(target, updatedValue, beforeWrite, afterWrite, afterPush, beforePull, afterPull, afterRestart, beforeCleanup, afterCleanup, afterUpdate, beforeDelete, afterDelete)
	if err != nil {
		return PendingCycleResult{}, err
	}
	if err := scenarios.ValidatePendingCycleNativeEvidence(evidence); err != nil {
		return PendingCycleResult{}, fmt.Errorf("validate Kotlin Android pending-cycle native evidence: %w", err)
	}
	if err := scenarios.ValidatePendingCycleServerFacts(serverCaptures[0].StateFacts, target); err != nil {
		return PendingCycleResult{}, fmt.Errorf("validate Kotlin Android pending-cycle server evidence: %w", err)
	}
	return PendingCycleResult{PushCall: push, PullCall: pullCall, ClientFacts: clientFacts, ServerFacts: serverCaptures[0].StateFacts, Evidence: evidence}, nil
}

func validateKotlinPendingCyclePostWrite(before, after Result) error {
	if before.PendingChangeCount == nil || before.MutationLedgerCount == nil || after.PendingChangeCount == nil || after.MutationLedgerCount == nil {
		return errors.New("Kotlin Android pending-cycle post-write capture is incomplete")
	}
	if *after.PendingChangeCount != 1 || *after.MutationLedgerCount != *before.MutationLedgerCount+1 {
		return fmt.Errorf("Kotlin Android pending-cycle post-write capture is invalid: pending changes %d, mutation ledger before %d after %d; want pending changes 1 and one mutation-ledger increase", *after.PendingChangeCount, *before.MutationLedgerCount, *after.MutationLedgerCount)
	}
	return nil
}

func kotlinPendingCycleRuntimePull(operation scenarios.Operation) (scenarios.Operation, error) {
	var payload map[string]any
	if err := json.Unmarshal(operation.Payload, &payload); err != nil {
		return scenarios.Operation{}, errors.New("decode Kotlin Android pending pull runtime binding failed")
	}
	scopes, ok := payload["scopes"].([]any)
	if !ok || len(scopes) != 1 {
		return scenarios.Operation{}, errors.New("Kotlin Android pending pull scope binding is invalid")
	}
	scope, ok := scopes[0].(map[string]any)
	if !ok || scope["cursor_source"] != "none" {
		return scenarios.Operation{}, errors.New("Kotlin Android pending pull authored cursor source is invalid")
	}
	scope["cursor_source"] = "local_checkpoint"
	encoded, err := json.Marshal(payload)
	if err != nil {
		return scenarios.Operation{}, errors.New("encode Kotlin Android pending pull runtime binding failed")
	}
	operation.Payload = encoded
	if err := scenarios.ValidateOperation(operation); err != nil {
		return scenarios.Operation{}, errors.New("encode Kotlin Android pending pull runtime binding failed")
	}
	return operation, nil
}

func validateKotlinPendingCycleCallBindings(steps map[scenarios.StepID]scenarios.Step) error {
	expected := []struct {
		id         string
		stage      string
		method     string
		completion string
	}{
		{id: "STEP-PERF-PENDING-CYCLE-002", stage: "begin", method: "start"},
		{id: "STEP-PERF-PENDING-CYCLE-CAPTURE-PENDING-001", stage: "await-step"},
		{id: "STEP-PERF-PENDING-CYCLE-003", stage: "await-call", completion: "idle"},
	}
	for _, value := range expected {
		step, found := steps[scenarios.StepID(value.id)]
		if !found || step.NativeBinding == nil || step.NativeBinding.Kind != "public-call" || step.NativeBinding.CallID == nil || string(*step.NativeBinding.CallID) != "pending_push" || step.NativeBinding.Stage != value.stage || step.NativeBinding.Method != value.method || step.NativeBinding.Completion != value.completion {
			return fmt.Errorf("Kotlin Android pending-cycle call binding %s is invalid", value.id)
		}
	}
	return nil
}

func kotlinPendingCycleTransportObservations(platform *Platform, client Client, checkpoint uint64) ([]TransportObservation, error) {
	state, err := platform.clientFor(client)
	if err != nil {
		return nil, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.session == nil {
		return nil, errors.New("Kotlin Android pending-cycle transport session is unavailable")
	}
	return state.session.ObservationsAfter(checkpoint)
}

func validateKotlinPendingCycleBeginTransport(observations []TransportObservation) error {
	if err := validateKotlinPendingCycleTransport(observations, 2); err != nil {
		return err
	}
	connect := observations[0]
	if connect.StatusCode != 200 || connect.ErrorCode != nil || connect.Retryable == nil || *connect.Retryable {
		return errors.New("Kotlin Android pending-cycle setup connect did not succeed")
	}
	return nil
}

func validateKotlinPendingCycleTransport(observations []TransportObservation, expected int) error {
	expectedClasses := []string{"connect", "push", "pull", "pull"}
	if expected < 1 || expected > len(expectedClasses) || len(observations) != expected {
		return fmt.Errorf("Kotlin Android pending-cycle transport has %d observations, want %d", len(observations), expected)
	}
	for index, observation := range observations {
		if observation.OperationClass != expectedClasses[index] {
			return fmt.Errorf("Kotlin Android pending-cycle transport operation class %q at position %d, want %q", observation.OperationClass, index, expectedClasses[index])
		}
		if observation.Retryable == nil {
			return fmt.Errorf("Kotlin Android pending-cycle %s transport retryability is absent", observation.OperationClass)
		}
	}
	return nil
}

func validateKotlinPendingCycleStepWire(scenario scenarios.Scenario, stepID, operationClass string, step StepObservation, observed TransportObservation) error {
	if step.Disposition != "success" || step.Wire == nil || step.Wire.HTTPStatus != observed.StatusCode || observed.Retryable == nil || step.Wire.Retryable != *observed.Retryable || !equalKotlinOptionalStrings(step.Wire.ErrorCode, observed.ErrorCode) {
		return fmt.Errorf("Kotlin Android pending-cycle %s step result does not match transport", stepID)
	}
	if observed.OperationClass != operationClass {
		return fmt.Errorf("Kotlin Android pending-cycle %s operation class is %q, want %q", stepID, observed.OperationClass, operationClass)
	}
	return validateKotlinWireObservation(scenario, stepID, observed)
}

func validateKotlinPendingCycleCleanupCall(call SynchronizationResult) error {
	if call.Completion != "blocked" {
		return fmt.Errorf("Kotlin Android pending-cycle cleanup call completion = %q, want blocked", call.Completion)
	}
	pushCount := 0
	for _, observation := range call.transportObservations {
		if observation.OperationClass != "push" {
			continue
		}
		pushCount++
		if observation.StatusCode != 503 || observation.Retryable == nil || !*observation.Retryable || observation.ErrorCode == nil || *observation.ErrorCode != "temporary_unavailable" {
			return errors.New("Kotlin Android pending-cycle cleanup push did not remain in retryable backoff")
		}
	}
	if pushCount == 0 {
		return errors.New("Kotlin Android pending-cycle cleanup did not attempt the pending update")
	}
	return nil
}

func runKotlinPendingCycleGeneratedPush(ctx context.Context, controller *blackbox.NativeController, platform *Platform, client Client, step scenarios.PendingCycleNativeCRUDStep, name string) (Result, error) {
	state, err := platform.clientFor(client)
	if err != nil {
		return Result{}, fmt.Errorf("access Kotlin Android pending-cycle %s transport: %w", name, err)
	}
	checkpoint := state.session.Checkpoint()
	call, err := kotlinScenarioCall(ctx, platform, client, "start")
	if err != nil {
		return Result{}, fmt.Errorf("run Kotlin Android pending-cycle %s push: %w", name, err)
	}
	observation, err := kotlinScenarioWire(call, "push")
	if err != nil {
		if err := waitForTransportObservation(ctx, state, checkpoint, "push"); err != nil {
			return Result{}, fmt.Errorf("wait for Kotlin Android pending-cycle %s push: %w", name, err)
		}
		observations, err := state.session.ObservationsAfter(checkpoint)
		if err != nil {
			return Result{}, fmt.Errorf("capture Kotlin Android pending-cycle %s recovery transport: %w", name, err)
		}
		found := false
		for _, candidate := range observations {
			if candidate.OperationClass == "push" && candidate.StatusCode == 200 && candidate.Retryable != nil && !*candidate.Retryable {
				observation = candidate
				found = true
				break
			}
		}
		if !found {
			return Result{}, fmt.Errorf("Kotlin Android pending-cycle %s recovery did not produce a successful push", name)
		}
	}
	if observation.StatusCode != 200 || observation.Retryable == nil || *observation.Retryable {
		return Result{}, fmt.Errorf("Kotlin Android pending-cycle %s push did not complete successfully", name)
	}
	if err := controller.BindApplicationPush(step.ApplicationPush); err != nil {
		return Result{}, fmt.Errorf("bind Kotlin Android pending-cycle %s push transaction: %w", name, err)
	}
	if result, processErr := controller.ProcessStep(ctx, nil, step.Materialize); processErr != nil || result.Disposition != "success" {
		return Result{}, fmt.Errorf("materialize Kotlin Android pending-cycle %s: %w", name, kotlinResultError(processErr, result.Disposition))
	}
	snapshot, err := platform.scenarioSnapshot(ctx, client)
	if err != nil {
		return Result{}, fmt.Errorf("capture Kotlin Android pending-cycle synchronized %s: %w", name, err)
	}
	return snapshot, nil
}

func kotlinPendingCycleServerVersion(target scenarios.PendingCycleNativeTarget, snapshot Result) (string, error) {
	var metadata []rowMetadataRecord
	if err := decodeFactArray(snapshot.RowMetadata, &metadata, maximumRecords); err != nil {
		return "", errors.New("Kotlin Android pending-cycle row metadata version is invalid")
	}
	version := ""
	for _, value := range metadata {
		if value.TableName != target.TableName || value.RecordID != target.RecordID {
			continue
		}
		if version != "" || value.ServerVersion == "" {
			return "", errors.New("Kotlin Android pending-cycle row metadata version is invalid")
		}
		version = value.ServerVersion
	}
	if version == "" {
		return "", errors.New("Kotlin Android pending-cycle row metadata version is absent")
	}
	return version, nil
}

func kotlinPendingCycleEvidence(target scenarios.PendingCycleNativeTarget, updatedValue string, snapshots ...Result) (scenarios.PendingCycleNativeEvidence, error) {
	if len(snapshots) != 11 {
		return scenarios.PendingCycleNativeEvidence{}, errors.New("Kotlin Android pending-cycle inspection sequence is incomplete")
	}
	states := make([]scenarios.PendingCycleNativeState, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if snapshot.ApplicationRowCount == nil || snapshot.PendingChangeCount == nil || snapshot.MutationLedgerCount == nil || snapshot.MutationOutcomeCount == nil || snapshot.RejectedMutationCount == nil || snapshot.ScopeStateCount == nil || snapshot.ScopeRowCount == nil || snapshot.RowMetadataCount == nil {
			return scenarios.PendingCycleNativeEvidence{}, errors.New("Kotlin Android pending-cycle inspection counts are incomplete")
		}
		applicationRows, decodeErr := androidApplicationRows(snapshot.ApplicationRows)
		if decodeErr != nil {
			return scenarios.PendingCycleNativeEvidence{}, decodeErr
		}
		applicationRowCount, applicationRows, err := kotlinLogicalApplicationRows(*snapshot.ApplicationRowCount, applicationRows, []kotlinApplicationRowLifecycle{
			{PrimaryKeyField: target.PrimaryKeyField, RecordID: target.RecordID, DeletedAtField: target.DeletedAtField},
			{PrimaryKeyField: target.PrimaryKeyField, RecordID: target.UnprotectedRecordID, DeletedAtField: target.DeletedAtField},
		})
		if err != nil {
			return scenarios.PendingCycleNativeEvidence{}, err
		}
		state := scenarios.PendingCycleNativeState{
			ProcessID:                   snapshot.ProcessID,
			DatabaseIdentityFingerprint: snapshot.DatabaseIdentityFingerprint,
			ApplicationRowCount:         applicationRowCount,
			PendingChangeCount:          *snapshot.PendingChangeCount,
			MutationLedgerCount:         *snapshot.MutationLedgerCount,
			MutationOutcomeCount:        *snapshot.MutationOutcomeCount,
			RejectedMutationCount:       *snapshot.RejectedMutationCount,
			ScopeStateCount:             *snapshot.ScopeStateCount,
			ScopeRowCount:               *snapshot.ScopeRowCount,
			RowMetadataCount:            *snapshot.RowMetadataCount,
		}
		for _, row := range applicationRows {
			var recordID string
			if json.Unmarshal(row[target.PrimaryKeyField], &recordID) != nil {
				return scenarios.PendingCycleNativeEvidence{}, errors.New("Kotlin Android pending-cycle application row identity is invalid")
			}
			switch recordID {
			case target.RecordID:
				if state.TargetRowPresent || json.Unmarshal(row[target.ValueField], &state.TargetRowValue) != nil {
					return scenarios.PendingCycleNativeEvidence{}, errors.New("Kotlin Android pending-cycle target application row is invalid")
				}
				state.TargetRowPresent = true
			case target.UnprotectedRecordID:
				if state.UnprotectedRowPresent || json.Unmarshal(row[target.ValueField], &state.UnprotectedRowValue) != nil {
					return scenarios.PendingCycleNativeEvidence{}, errors.New("Kotlin Android pending-cycle unprotected application row is invalid")
				}
				state.UnprotectedRowPresent = true
			default:
				return scenarios.PendingCycleNativeEvidence{}, errors.New("Kotlin Android pending-cycle application row differs from its targets")
			}
		}
		var mutations []retainedMutation
		if err := decodeFactArray(snapshot.RetainedMutations, &mutations, maximumRecords); err != nil {
			return scenarios.PendingCycleNativeEvidence{}, errors.New("Kotlin Android pending-cycle mutation inspection is invalid")
		}
		for _, mutation := range mutations {
			if mutation.TableName != target.TableName || mutation.RecordID != target.RecordID {
				continue
			}
			state.TargetMutations = append(state.TargetMutations, scenarios.PendingCycleNativeMutation{Operation: mutation.Operation, Status: mutation.Status, ClientVersion: mutation.ClientVersion})
		}
		var metadata []rowMetadataRecord
		if err := decodeFactArray(snapshot.RowMetadata, &metadata, maximumRecords); err != nil {
			return scenarios.PendingCycleNativeEvidence{}, errors.New("Kotlin Android pending-cycle row metadata inspection is invalid")
		}
		for _, value := range metadata {
			if value.TableName != target.TableName || value.RecordID != target.RecordID {
				continue
			}
			if state.TargetServerVersion != "" {
				return scenarios.PendingCycleNativeEvidence{}, errors.New("Kotlin Android pending-cycle row metadata is duplicated")
			}
			state.TargetServerVersion = value.ServerVersion
			checksum, checksumErr := androidChecksumDigest(value.RowChecksum)
			if checksumErr != nil {
				return scenarios.PendingCycleNativeEvidence{}, checksumErr
			}
			if checksum != nil {
				state.TargetRowChecksum = *checksum
			}
		}
		scopeStates, decodeErr := androidCursorScopeStates(snapshot.ScopeStates)
		if decodeErr != nil {
			return scenarios.PendingCycleNativeEvidence{}, decodeErr
		}
		if len(scopeStates) == 1 {
			scope := scopeStates[0]
			state.ScopeID = scope.ScopeID
			if scope.Cursor != nil {
				state.ScopeCursor = *scope.Cursor
			}
			checksum, checksumErr := androidChecksumDigest(scope.Checksum)
			if checksumErr != nil {
				return scenarios.PendingCycleNativeEvidence{}, checksumErr
			}
			if checksum != nil {
				state.ScopeChecksum = *checksum
			}
			if scope.LocalChecksum != "" {
				localChecksum, checksumErr := androidChecksumDigest(&scope.LocalChecksum)
				if checksumErr != nil {
					return scenarios.PendingCycleNativeEvidence{}, checksumErr
				}
				if localChecksum != nil {
					state.LocalScopeChecksum = *localChecksum
				}
			}
		}
		scopeRows, decodeErr := androidScopeRows(snapshot.ScopeRows)
		if decodeErr != nil {
			return scenarios.PendingCycleNativeEvidence{}, decodeErr
		}
		for _, row := range scopeRows {
			if row.TableName != target.TableName {
				return scenarios.PendingCycleNativeEvidence{}, errors.New("Kotlin Android pending-cycle scope row differs from its table")
			}
			switch row.RecordID {
			case target.RecordID:
				if state.TargetScopeRowPresent {
					return scenarios.PendingCycleNativeEvidence{}, errors.New("Kotlin Android pending-cycle target scope row is duplicated")
				}
				state.TargetScopeRowPresent = true
				state.TargetScopeRowChecksum = row.Checksum
			case target.UnprotectedRecordID:
				if state.UnprotectedScopeRowPresent {
					return scenarios.PendingCycleNativeEvidence{}, errors.New("Kotlin Android pending-cycle unprotected scope row is duplicated")
				}
				state.UnprotectedScopeRowPresent = true
				state.UnprotectedScopeRowChecksum = row.Checksum
			default:
				return scenarios.PendingCycleNativeEvidence{}, errors.New("Kotlin Android pending-cycle scope row differs from its targets")
			}
			if state.ScopeID == "" {
				state.ScopeID = row.ScopeID
			}
		}
		states = append(states, state)
	}
	return scenarios.PendingCycleNativeEvidence{
		Target:        target,
		UpdatedValue:  updatedValue,
		BeforeWrite:   states[0],
		AfterWrite:    states[1],
		AfterPush:     states[2],
		BeforePull:    states[3],
		AfterPull:     states[4],
		AfterRestart:  states[5],
		BeforeCleanup: states[6],
		AfterCleanup:  states[7],
		AfterUpdate:   states[8],
		BeforeDelete:  states[9],
		AfterDelete:   states[10],
	}, nil
}

type kotlinApplicationRowLifecycle struct {
	PrimaryKeyField string
	RecordID        string
	DeletedAtField  string
}

func kotlinLogicalApplicationRows(rawCount int, rows []map[string]json.RawMessage, lifecycles []kotlinApplicationRowLifecycle) (int, []map[string]json.RawMessage, error) {
	logicalCount := rawCount
	logicalRows := make([]map[string]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		deletedAtField := ""
		for _, lifecycle := range lifecycles {
			var recordID string
			if lifecycle.DeletedAtField != "" && json.Unmarshal(row[lifecycle.PrimaryKeyField], &recordID) == nil && recordID == lifecycle.RecordID {
				if deletedAtField != "" {
					return 0, nil, errors.New("Kotlin Android application row lifecycle is ambiguous")
				}
				deletedAtField = lifecycle.DeletedAtField
			}
		}
		if deletedAtField == "" {
			logicalRows = append(logicalRows, row)
			continue
		}
		deletedAt, hasDeletedAt := row[deletedAtField]
		if !hasDeletedAt {
			return 0, nil, errors.New("Kotlin Android application row deleted-at field is absent")
		}
		if !json.Valid(deletedAt) {
			return 0, nil, errors.New("Kotlin Android application row deleted-at field is invalid")
		}
		var value any
		if err := json.Unmarshal(deletedAt, &value); err != nil {
			return 0, nil, errors.New("Kotlin Android application row deleted-at field is invalid")
		}
		if value == nil {
			logicalRows = append(logicalRows, row)
			continue
		}
		logicalCount--
	}
	if logicalCount < 0 {
		return 0, nil, errors.New("Kotlin Android application row count is invalid")
	}
	return logicalCount, logicalRows, nil
}
