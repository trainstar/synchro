package swift

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

// PendingCycleResult records direct Swift evidence for one pending mutation cycle.
type PendingCycleResult struct {
	PushCall    SynchronizationResult
	PullCall    SynchronizationResult
	ClientFacts []CaptureFacts
	ServerFacts scenarios.StateFacts
	Evidence    scenarios.PendingCycleNativeEvidence
}

// RunPendingCycleScenario executes the authored pending-cycle flow through Swift.
func RunPendingCycleScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, platform *Platform, client Client) (PendingCycleResult, error) {
	steps, err := swiftScenarioStepMap(scenario, pendingCycleScenarioID, 7)
	if err != nil {
		return PendingCycleResult{}, err
	}
	if controller == nil || platform == nil {
		return PendingCycleResult{}, errors.New("Swift pending-cycle dependencies are unavailable")
	}
	for _, id := range []string{
		"STEP-PERF-PENDING-CYCLE-001",
		"STEP-PERF-PENDING-CYCLE-002",
		"STEP-PERF-PENDING-CYCLE-CAPTURE-PENDING-001",
		"STEP-PERF-PENDING-CYCLE-003",
	} {
		if err := swiftScenarioClient(steps[scenarios.StepID(id)], client); err != nil {
			return PendingCycleResult{}, err
		}
	}
	callID, err := validateSwiftPendingCycleCallBindings(steps)
	if err != nil {
		return PendingCycleResult{}, err
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return PendingCycleResult{}, fmt.Errorf("install Swift pending-cycle contract: %w", err)
	}
	if err := platform.Install(ctx, client, "current", ""); err != nil {
		return PendingCycleResult{}, fmt.Errorf("install Swift pending-cycle client: %w", err)
	}
	resumeWAL, err := controller.PauseWALMaterialization(ctx)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("pause Swift pending-cycle WAL materialization: %w", err)
	}
	walPaused := true
	defer func() {
		if walPaused {
			cleanupContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = resumeWAL(cleanupContext)
		}
	}()
	unprotectedCommit, err := swiftScenarioOperation(steps, "STEP-PERF-PENDING-CYCLE-UNPROTECTED-COMMIT-001", "model/commit-source-transaction")
	if err != nil {
		return PendingCycleResult{}, err
	}
	if observation, applyErr := controller.ApplyStep(ctx, unprotectedCommit); applyErr != nil || observation.Disposition != "success" {
		return PendingCycleResult{}, fmt.Errorf("commit Swift pending-cycle unprotected row: %w", resultError(applyErr, observation.Disposition))
	}
	unprotectedAlias, err := scenarios.PendingCycleUnprotectedIdentityAlias(scenario.NativeIdentityAliases)
	if err != nil {
		return PendingCycleResult{}, err
	}
	unprotectedValues, err := controller.IdentityValues([]scenarios.NativeIdentityAlias{unprotectedAlias})
	if err != nil || len(unprotectedValues) != 1 {
		return PendingCycleResult{}, errors.New("Swift pending-cycle unprotected row has no runtime identity")
	}
	var unprotectedRuntimeID string
	if json.Unmarshal(unprotectedValues[0].RuntimeValue, &unprotectedRuntimeID) != nil || unprotectedRuntimeID == "" {
		return PendingCycleResult{}, errors.New("Swift pending-cycle unprotected runtime identity is invalid")
	}
	unprotectedAuthoredID, unprotectedValue, err := scenarios.PendingCycleUnprotectedRowTarget(unprotectedCommit, []scenarios.NativeIdentityAlias{unprotectedAlias}, unprotectedRuntimeID)
	if err != nil {
		return PendingCycleResult{}, err
	}
	beforeWrite, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Swift pending-cycle pre-write state: %w", err)
	}

	write, err := swiftScenarioOperation(steps, "STEP-PERF-PENDING-CYCLE-001", "local/write")
	if err != nil {
		return PendingCycleResult{}, err
	}
	var authoredWrite struct {
		TableID string `json:"table_id"`
	}
	if json.Unmarshal(write.Payload, &authoredWrite) != nil || authoredWrite.TableID == "" {
		return PendingCycleResult{}, errors.New("Swift pending-cycle authored table identity is invalid")
	}
	deletedAtField, err := controller.ApplicationDeletedAtField(authoredWrite.TableID)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("resolve Swift pending-cycle deleted-at field: %w", err)
	}
	write, err = controller.ApplicationWrite(write)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("bind Swift pending mutation to the application schema: %w", err)
	}
	action, selector, err := decodeLocalWrite(write, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("decode Swift pending-cycle runtime target: %w", err)
	}
	var recordID string
	if json.Unmarshal(selector.PrimaryKey, &recordID) != nil || recordID == "" {
		return PendingCycleResult{}, errors.New("Swift pending-cycle runtime record identity is invalid")
	}
	target := scenarios.PendingCycleNativeTarget{TableName: action.TableName, PrimaryKeyField: action.PrimaryKeyField, RecordID: recordID, Value: "pending", DeletedAtField: deletedAtField}
	for field, value := range action.Fields {
		var text string
		if json.Unmarshal(value, &text) == nil && text == target.Value {
			if target.ValueField != "" {
				return PendingCycleResult{}, errors.New("Swift pending-cycle runtime value field is ambiguous")
			}
			target.ValueField = field
		}
	}
	if target.ValueField == "" {
		return PendingCycleResult{}, errors.New("Swift pending-cycle runtime value field is absent")
	}
	target.UnprotectedAuthoredRecordID = unprotectedAuthoredID
	target.UnprotectedRecordID = unprotectedRuntimeID
	target.UnprotectedValue = unprotectedValue
	local, err := platform.ApplyStep(ctx, client, write)
	if err != nil || local.Disposition != "success" {
		return PendingCycleResult{}, fmt.Errorf("apply Swift pending mutation: %w", resultError(err, local.Disposition))
	}
	afterWrite, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Swift pending-cycle local write: %w", err)
	}
	if err := validateSwiftPendingCyclePostWrite(beforeWrite, afterWrite); err != nil {
		return PendingCycleResult{}, err
	}

	authoredPush, err := swiftScenarioOperation(steps, "STEP-PERF-PENDING-CYCLE-002", "push/submit")
	if err != nil {
		return PendingCycleResult{}, err
	}
	capturePending, err := swiftScenarioOperation(steps, "STEP-PERF-PENDING-CYCLE-CAPTURE-PENDING-001", "pull/request-page")
	if err != nil {
		return PendingCycleResult{}, err
	}
	capturePending, err = swiftPendingCycleRuntimePull(capturePending)
	if err != nil {
		return PendingCycleResult{}, err
	}
	pull, err := swiftScenarioOperation(steps, "STEP-PERF-PENDING-CYCLE-003", "pull/request-page")
	if err != nil {
		return PendingCycleResult{}, err
	}
	pull, err = swiftPendingCycleRuntimePull(pull)
	if err != nil {
		return PendingCycleResult{}, err
	}
	state, err := platform.client(client)
	if err != nil {
		return PendingCycleResult{}, err
	}
	state.mu.Lock()
	transportCheckpoint := state.session.Checkpoint()
	state.mu.Unlock()
	begin, err := platform.BeginCall(ctx, client, callID, "start", RequestOperations{authoredPush})
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("begin Swift pending-cycle call: %w", err)
	}
	callActive := true
	defer func() {
		if callActive {
			cleanupContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = platform.AbortCall(cleanupContext, client, callID)
		}
	}()
	if err := validateSwiftPendingCycleBegin(begin, callID); err != nil {
		return PendingCycleResult{}, err
	}
	if err := validateSwiftPendingCycleStepWire(scenario, "STEP-PERF-PENDING-CYCLE-002", begin.Steps[0]); err != nil {
		return PendingCycleResult{}, err
	}
	if err := controller.BindApplicationPush(authoredPush); err != nil {
		return PendingCycleResult{}, fmt.Errorf("bind Swift pending push transaction: %w", err)
	}
	pendingPull, err := platform.AwaitStep(ctx, client, callID, capturePending)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("await Swift pending capture-pending pull: %w", err)
	}
	if err := validateSwiftPendingCycleStepWire(scenario, "STEP-PERF-PENDING-CYCLE-CAPTURE-PENDING-001", pendingPull); err != nil {
		return PendingCycleResult{}, err
	}
	afterPush, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Swift pending-cycle accepted push: %w", err)
	}
	if err := resumeWAL(ctx); err != nil {
		return PendingCycleResult{}, fmt.Errorf("resume Swift pending-cycle WAL materialization: %w", err)
	}
	walPaused = false

	unprotectedMaterialize, err := swiftScenarioOperation(steps, "STEP-PERF-PENDING-CYCLE-UNPROTECTED-MATERIALIZE-001", "process/materialize-source-transaction")
	if err != nil {
		return PendingCycleResult{}, err
	}
	if result, processErr := controller.ProcessStep(ctx, nil, unprotectedMaterialize); processErr != nil || result.Disposition != "success" {
		return PendingCycleResult{}, fmt.Errorf("materialize Swift pending-cycle unprotected row: %w", resultError(processErr, result.Disposition))
	}
	materialize, err := swiftScenarioOperation(steps, "STEP-PERF-PENDING-CYCLE-MATERIALIZE-001", "process/materialize-source-transaction")
	if err != nil {
		return PendingCycleResult{}, err
	}
	if result, processErr := controller.ProcessStep(ctx, nil, materialize); processErr != nil || result.Disposition != "success" {
		return PendingCycleResult{}, fmt.Errorf("materialize Swift pending mutation: %w", resultError(processErr, result.Disposition))
	}
	beforePull, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Swift pending pull checkpoint: %w", err)
	}
	retryPull, err := platform.AwaitStep(ctx, client, callID, pull)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("await Swift pending-cycle retry pull: %w", err)
	}
	if err := validateSwiftPendingCycleStepWire(scenario, "STEP-PERF-PENDING-CYCLE-003", retryPull); err != nil {
		return PendingCycleResult{}, err
	}
	completed, err := platform.AwaitCall(ctx, client, callID)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("complete Swift pending-cycle call: %w", err)
	}
	callActive = false
	if err := validateSwiftPendingCycleCompletion(completed, callID); err != nil {
		return PendingCycleResult{}, err
	}
	state.mu.Lock()
	transport, transportErr := state.session.ObservationsAfter(transportCheckpoint)
	state.mu.Unlock()
	if transportErr != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Swift pending-cycle transport: %w", transportErr)
	}
	coveredTransport, err := validateSwiftPendingCycleTransport(scenario, transport)
	if err != nil {
		return PendingCycleResult{}, err
	}
	push := SynchronizationResult{
		Completion:                completed.Completion,
		CallErrorCategory:         completed.CallErrorCategory,
		Steps:                     []StepObservation{begin.Steps[0], pendingPull, retryPull},
		DurationNanoseconds:       completed.DurationNanoseconds,
		ProvenanceMaintenanceWork: completed.ProvenanceMaintenanceWork,
		ReplayedMutationCount:     completed.ReplayedMutationCount,
		transportObservations:     cloneTransportObservations(coveredTransport),
	}
	pullCall := SynchronizationResult{
		Completion:                completed.Completion,
		CallErrorCategory:         completed.CallErrorCategory,
		Steps:                     []StepObservation{pendingPull, retryPull},
		DurationNanoseconds:       completed.DurationNanoseconds,
		ProvenanceMaintenanceWork: completed.ProvenanceMaintenanceWork,
		ReplayedMutationCount:     completed.ReplayedMutationCount,
		transportObservations:     cloneTransportObservations(coveredTransport[1:]),
	}
	afterPull, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Swift pending-cycle pull state: %w", err)
	}
	restartPayload, err := json.Marshal(map[string]string{"user_id": client.UserID, "client_id": client.ClientID})
	if err != nil {
		return PendingCycleResult{}, errors.New("encode Swift pending-cycle restart failed")
	}
	restart := scenarios.Operation{ContractOperation: "process", Name: "restart-client", Payload: restartPayload}
	if observation, processErr := platform.ProcessStep(ctx, client, restart); processErr != nil || observation.Disposition != "success" {
		return PendingCycleResult{}, fmt.Errorf("restart Swift pending-cycle client: %w", resultError(processErr, observation.Disposition))
	}
	afterRestart, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Swift pending-cycle restarted state: %w", err)
	}
	wireExpectationFound := false
	for _, expectation := range scenario.Model.ExpectedState {
		if expectation.ID != scenarios.ExpectationID("EXPECT-PERF-PENDING-CYCLE-SEMANTIC-001") {
			continue
		}
		var payload map[string]any
		if expectation.Predicate.ContractPredicate != "wire-outcome" || expectation.Predicate.Name != "canonical-wire-outcome" || expectation.StateFacts != nil || json.Unmarshal(expectation.Predicate.Payload, &payload) != nil || len(payload) != 0 {
			return PendingCycleResult{}, errors.New("Swift pending-cycle canonical wire expectation is invalid")
		}
		wireExpectationFound = true
	}
	if !wireExpectationFound {
		return PendingCycleResult{}, errors.New("Swift pending-cycle canonical wire expectation is absent")
	}

	updatedValue := target.Value + "-updated"
	updateBaseVersion, err := swiftPendingCycleServerVersion(target, afterRestart)
	if err != nil {
		return PendingCycleResult{}, err
	}
	updateStep, err := scenarios.PendingCycleSynchronizedCRUDOperation(write, authoredPush, materialize, "update", target.ValueField, target.Value, updatedValue, updateBaseVersion)
	if err != nil {
		return PendingCycleResult{}, err
	}
	if observation, lifecycleErr := platform.Lifecycle(ctx, client, "stop"); lifecycleErr != nil || observation.Disposition != "success" {
		return PendingCycleResult{}, fmt.Errorf("stop Swift pending-cycle client before update: %w", resultError(lifecycleErr, observation.Disposition))
	}
	if observation, applyErr := platform.ApplyStep(ctx, client, updateStep.LocalWrite); applyErr != nil || observation.Disposition != "success" {
		return PendingCycleResult{}, fmt.Errorf("apply Swift pending-cycle update: %w", resultError(applyErr, observation.Disposition))
	}
	beforeCleanup, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Swift pending-cycle update intent: %w", err)
	}

	assignment, err := scenarios.PendingCycleCleanupAssignment(client.UserID, client.ClientID)
	if err != nil {
		return PendingCycleResult{}, err
	}
	if observation, assignmentErr := controller.ApplyStep(ctx, assignment); assignmentErr != nil || observation.Disposition != "success" {
		return PendingCycleResult{}, fmt.Errorf("assign Swift pending-cycle cleanup scope: %w", resultError(assignmentErr, observation.Disposition))
	}
	faultedUpdate, err := scenarios.PendingCycleTemporaryUnavailablePush(updateStep.ApplicationPush)
	if err != nil {
		return PendingCycleResult{}, err
	}
	releaseFault, armed, err := platform.armTemporaryUnavailablePush(RequestOperations{faultedUpdate})
	if err != nil || !armed {
		return PendingCycleResult{}, fmt.Errorf("arm Swift pending-cycle temporary-unavailable push: %w", err)
	}
	defer releaseFault()
	cleanupCall, err := swiftScenarioCall(ctx, platform, client, "start")
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("run Swift pending-cycle scope cleanup: %w", resultError(err, cleanupCall.Completion))
	}
	if err := validateSwiftPendingCycleCleanupCall(cleanupCall); err != nil {
		return PendingCycleResult{}, err
	}
	afterCleanup, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Swift pending-cycle scope cleanup: %w", err)
	}
	if observation, lifecycleErr := platform.Lifecycle(ctx, client, "stop"); lifecycleErr != nil || observation.Disposition != "success" {
		return PendingCycleResult{}, fmt.Errorf("stop Swift pending-cycle client during push backoff: %w", resultError(lifecycleErr, observation.Disposition))
	}
	releaseFault()
	afterUpdate, err := runSwiftPendingCycleGeneratedPush(ctx, controller, platform, client, updateStep, "update")
	if err != nil {
		return PendingCycleResult{}, err
	}
	deleteBaseVersion, err := swiftPendingCycleServerVersion(target, afterUpdate)
	if err != nil {
		return PendingCycleResult{}, err
	}
	deleteStep, err := scenarios.PendingCycleSynchronizedCRUDOperation(write, authoredPush, materialize, "delete", target.ValueField, "", "", deleteBaseVersion)
	if err != nil {
		return PendingCycleResult{}, err
	}
	if observation, lifecycleErr := platform.Lifecycle(ctx, client, "stop"); lifecycleErr != nil || observation.Disposition != "success" {
		return PendingCycleResult{}, fmt.Errorf("stop Swift pending-cycle client before delete: %w", resultError(lifecycleErr, observation.Disposition))
	}
	if observation, applyErr := platform.ApplyStep(ctx, client, deleteStep.LocalWrite); applyErr != nil || observation.Disposition != "success" {
		return PendingCycleResult{}, fmt.Errorf("apply Swift pending-cycle delete: %w", resultError(applyErr, observation.Disposition))
	}
	beforeDelete, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Swift pending-cycle delete intent: %w", err)
	}
	afterDelete, err := runSwiftPendingCycleGeneratedPush(ctx, controller, platform, client, deleteStep, "delete")
	if err != nil {
		return PendingCycleResult{}, err
	}
	clientFacts, err := platform.Capture(ctx, []Client{client}, []string{"application-rows", "pending-mutations", "rejected-mutations", "checkpoints", "provenance"})
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Swift pending-cycle client state: %w", err)
	}
	serverCaptures, err := controller.Capture(ctx, []string{client.Key}, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return PendingCycleResult{}, fmt.Errorf("capture Swift pending-cycle server state: %w", err)
	}
	evidence, err := swiftPendingCycleEvidence(target, updatedValue, beforeWrite, afterWrite, afterPush, beforePull, afterPull, afterRestart, beforeCleanup, afterCleanup, afterUpdate, beforeDelete, afterDelete)
	if err != nil {
		return PendingCycleResult{}, err
	}
	if err := scenarios.ValidatePendingCycleNativeEvidence(evidence); err != nil {
		return PendingCycleResult{}, fmt.Errorf("validate Swift pending-cycle native evidence: %w", err)
	}
	if err := scenarios.ValidatePendingCycleServerFacts(serverCaptures[0].StateFacts, target); err != nil {
		return PendingCycleResult{}, fmt.Errorf("validate Swift pending-cycle server evidence: %w", err)
	}
	return PendingCycleResult{PushCall: push, PullCall: pullCall, ClientFacts: clientFacts, ServerFacts: serverCaptures[0].StateFacts, Evidence: evidence}, nil
}

func validateSwiftPendingCyclePostWrite(before, after runnerResult) error {
	if before.PendingChangeCount == nil || before.MutationLedgerCount == nil || after.PendingChangeCount == nil || after.MutationLedgerCount == nil {
		return errors.New("Swift pending-cycle post-write capture is incomplete")
	}
	if *after.PendingChangeCount != 1 || *after.MutationLedgerCount != *before.MutationLedgerCount+1 {
		return fmt.Errorf("Swift pending-cycle post-write capture is invalid: pending changes %d, mutation ledger before %d after %d; want pending changes 1 and one mutation-ledger increase", *after.PendingChangeCount, *before.MutationLedgerCount, *after.MutationLedgerCount)
	}
	return nil
}

func swiftPendingCycleRuntimePull(operation scenarios.Operation) (scenarios.Operation, error) {
	var payload map[string]any
	if err := json.Unmarshal(operation.Payload, &payload); err != nil {
		return scenarios.Operation{}, errors.New("decode Swift pending pull runtime binding failed")
	}
	scopes, ok := payload["scopes"].([]any)
	if !ok || len(scopes) != 1 {
		return scenarios.Operation{}, errors.New("Swift pending pull scope binding is invalid")
	}
	scope, ok := scopes[0].(map[string]any)
	if !ok || scope["cursor_source"] != "none" {
		return scenarios.Operation{}, errors.New("Swift pending pull authored cursor source is invalid")
	}
	scope["cursor_source"] = "local_checkpoint"
	encoded, err := json.Marshal(payload)
	if err != nil {
		return scenarios.Operation{}, errors.New("encode Swift pending pull runtime binding failed")
	}
	operation.Payload = encoded
	if err := scenarios.ValidateOperation(operation); err != nil {
		return scenarios.Operation{}, errors.New("encode Swift pending pull runtime binding failed")
	}
	return operation, nil
}

func validateSwiftPendingCycleCallBindings(steps map[scenarios.StepID]scenarios.Step) (string, error) {
	expected := []struct {
		id, stage, method, completion string
	}{
		{"STEP-PERF-PENDING-CYCLE-002", "begin", "start", ""},
		{"STEP-PERF-PENDING-CYCLE-CAPTURE-PENDING-001", "await-step", "", ""},
		{"STEP-PERF-PENDING-CYCLE-003", "await-call", "", "idle"},
	}
	const callID = "pending_push"
	for _, want := range expected {
		step, found := steps[scenarios.StepID(want.id)]
		if !found || step.NativeBinding == nil {
			return "", fmt.Errorf("Swift pending-cycle step %s has no native call binding", want.id)
		}
		binding := step.NativeBinding
		if binding.Kind != "public-call" || binding.CallID == nil || string(*binding.CallID) != callID || binding.Stage != want.stage || binding.Method != want.method || binding.Completion != want.completion {
			return "", fmt.Errorf("Swift pending-cycle step %s native call binding is invalid", want.id)
		}
	}
	return callID, nil
}

func validateSwiftPendingCycleBegin(call CallResult, callID string) error {
	if call.CallID != callID || call.State != "in_flight" || call.Completion != "" || call.CallErrorCategory != "" || len(call.Steps) != 1 {
		return errors.New("Swift pending-cycle push did not enter the staged call")
	}
	return nil
}

func validateSwiftPendingCycleStepWire(scenario scenarios.Scenario, stepID string, observed StepObservation) error {
	for _, expected := range scenario.WireExpectations {
		if expected.StepID != scenarios.StepID(stepID) {
			continue
		}
		if observed.Disposition != "success" || observed.Wire == nil || observed.Wire.HTTPStatus != expected.HTTPStatus || observed.Wire.Retryable != expected.Retryable || !equalOptionalStrings(observed.Wire.ErrorCode, expected.ErrorCode) {
			return fmt.Errorf("Swift pending-cycle wire result %s differs from its authored expectation", stepID)
		}
		return nil
	}
	return fmt.Errorf("Swift pending-cycle wire expectation %s is absent", stepID)
}

func validateSwiftPendingCycleCompletion(call CallResult, callID string) error {
	if call.CallID != callID || call.State != "completed" || call.Completion != "idle" || call.CallErrorCategory != "" || len(call.Steps) != 0 {
		return errors.New("Swift pending-cycle call did not complete idle")
	}
	return nil
}

func validateSwiftPendingCycleTransport(scenario scenarios.Scenario, observed []transportObservation) ([]transportObservation, error) {
	if len(observed) != 4 {
		return nil, fmt.Errorf("Swift pending-cycle transport count = %d, want 4", len(observed))
	}
	connect := observed[0]
	if connect.OperationClass != "connect" || connect.StatusCode != 200 || connect.ErrorCode != nil || connect.Retryable {
		return nil, errors.New("Swift pending-cycle staged call setup connect is invalid")
	}
	expected := []struct {
		stepID, operationClass string
	}{
		{"STEP-PERF-PENDING-CYCLE-002", "push"},
		{"STEP-PERF-PENDING-CYCLE-CAPTURE-PENDING-001", "pull"},
		{"STEP-PERF-PENDING-CYCLE-003", "pull"},
	}
	covered := observed[1:]
	for index, want := range expected {
		if covered[index].OperationClass != want.operationClass {
			return nil, fmt.Errorf("Swift pending-cycle transport %d operation class = %q, want %q", index+1, covered[index].OperationClass, want.operationClass)
		}
		if err := validateSwiftWireObservation(scenario, want.stepID, covered[index]); err != nil {
			return nil, err
		}
	}
	return covered, nil
}

func validateSwiftPendingCycleCleanupCall(call SynchronizationResult) error {
	if call.Completion != "blocked" {
		return fmt.Errorf("Swift pending-cycle cleanup call completion = %q, want blocked", call.Completion)
	}
	pushCount := 0
	for _, observation := range call.transportObservations {
		if observation.OperationClass != "push" {
			continue
		}
		pushCount++
		if observation.StatusCode != 503 || !observation.Retryable || observation.ErrorCode == nil || *observation.ErrorCode != "temporary_unavailable" {
			return errors.New("Swift pending-cycle cleanup push did not remain in retryable backoff")
		}
	}
	if pushCount == 0 {
		return errors.New("Swift pending-cycle cleanup did not attempt the pending update")
	}
	return nil
}

func runSwiftPendingCycleGeneratedPush(ctx context.Context, controller *blackbox.NativeController, platform *Platform, client Client, step scenarios.PendingCycleNativeCRUDStep, name string) (runnerResult, error) {
	state, err := platform.client(client)
	if err != nil {
		return runnerResult{}, fmt.Errorf("access Swift pending-cycle %s transport: %w", name, err)
	}
	checkpoint := state.session.Checkpoint()
	call, err := swiftScenarioCall(ctx, platform, client, "start")
	if err != nil {
		return runnerResult{}, fmt.Errorf("run Swift pending-cycle %s push: %w", name, err)
	}
	observation, err := swiftScenarioWire(call, "push")
	if err != nil {
		if err := waitForTransportObservation(ctx, state, checkpoint, "push"); err != nil {
			return runnerResult{}, fmt.Errorf("wait for Swift pending-cycle %s push: %w", name, err)
		}
		observations, err := state.session.ObservationsAfter(checkpoint)
		if err != nil {
			return runnerResult{}, fmt.Errorf("capture Swift pending-cycle %s recovery transport: %w", name, err)
		}
		found := false
		for _, candidate := range observations {
			if candidate.OperationClass == "push" && candidate.StatusCode == 200 && !candidate.Retryable {
				observation = candidate
				found = true
				break
			}
		}
		if !found {
			return runnerResult{}, fmt.Errorf("Swift pending-cycle %s recovery did not produce a successful push", name)
		}
	}
	if observation.StatusCode != 200 || observation.Retryable {
		return runnerResult{}, fmt.Errorf("Swift pending-cycle %s push did not complete successfully", name)
	}
	if err := controller.BindApplicationPush(step.ApplicationPush); err != nil {
		return runnerResult{}, fmt.Errorf("bind Swift pending-cycle %s push transaction: %w", name, err)
	}
	if result, processErr := controller.ProcessStep(ctx, nil, step.Materialize); processErr != nil || result.Disposition != "success" {
		return runnerResult{}, fmt.Errorf("materialize Swift pending-cycle %s: %w", name, resultError(processErr, result.Disposition))
	}
	snapshot, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return runnerResult{}, fmt.Errorf("capture Swift pending-cycle synchronized %s: %w", name, err)
	}
	return snapshot, nil
}

func swiftPendingCycleServerVersion(target scenarios.PendingCycleNativeTarget, snapshot runnerResult) (string, error) {
	version := ""
	for _, metadata := range snapshot.RowMetadataRecords {
		if metadata.TableName != target.TableName || metadata.RecordID != target.RecordID {
			continue
		}
		if version != "" || metadata.ServerVersion == "" {
			return "", errors.New("Swift pending-cycle row metadata version is invalid")
		}
		version = metadata.ServerVersion
	}
	if version == "" {
		return "", errors.New("Swift pending-cycle row metadata version is absent")
	}
	return version, nil
}

func swiftPendingCycleEvidence(target scenarios.PendingCycleNativeTarget, updatedValue string, snapshots ...runnerResult) (scenarios.PendingCycleNativeEvidence, error) {
	if len(snapshots) != 11 {
		return scenarios.PendingCycleNativeEvidence{}, errors.New("Swift pending-cycle inspection sequence is incomplete")
	}
	states := make([]scenarios.PendingCycleNativeState, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if snapshot.ApplicationRowCount == nil || snapshot.PendingChangeCount == nil || snapshot.MutationLedgerCount == nil || snapshot.MutationOutcomeCount == nil || snapshot.RejectedMutationCount == nil || snapshot.ScopeStateCount == nil || snapshot.ScopeRowCount == nil || snapshot.RowMetadataCount == nil {
			return scenarios.PendingCycleNativeEvidence{}, errors.New("Swift pending-cycle inspection counts are incomplete")
		}
		applicationRowCount, applicationRows, err := swiftLogicalApplicationRows(*snapshot.ApplicationRowCount, snapshot.ApplicationRows, []swiftApplicationRowLifecycle{
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
				return scenarios.PendingCycleNativeEvidence{}, errors.New("Swift pending-cycle application row identity is invalid")
			}
			switch recordID {
			case target.RecordID:
				if state.TargetRowPresent || json.Unmarshal(row[target.ValueField], &state.TargetRowValue) != nil {
					return scenarios.PendingCycleNativeEvidence{}, errors.New("Swift pending-cycle target application row is invalid")
				}
				state.TargetRowPresent = true
			case target.UnprotectedRecordID:
				if state.UnprotectedRowPresent || json.Unmarshal(row[target.ValueField], &state.UnprotectedRowValue) != nil {
					return scenarios.PendingCycleNativeEvidence{}, errors.New("Swift pending-cycle unprotected application row is invalid")
				}
				state.UnprotectedRowPresent = true
			default:
				return scenarios.PendingCycleNativeEvidence{}, errors.New("Swift pending-cycle application row differs from its targets")
			}
		}
		for _, mutation := range snapshot.RetainedMutations {
			if mutation.TableName != target.TableName || mutation.RecordID != target.RecordID {
				continue
			}
			state.TargetMutations = append(state.TargetMutations, scenarios.PendingCycleNativeMutation{Operation: mutation.Operation, Status: mutation.Status, ClientVersion: mutation.ClientVersion})
		}
		for _, metadata := range snapshot.RowMetadataRecords {
			if metadata.TableName != target.TableName || metadata.RecordID != target.RecordID {
				continue
			}
			if state.TargetServerVersion != "" {
				return scenarios.PendingCycleNativeEvidence{}, errors.New("Swift pending-cycle row metadata is duplicated")
			}
			state.TargetServerVersion = metadata.ServerVersion
			checksum, checksumErr := swiftChecksumDigest(metadata.RowChecksum)
			if checksumErr != nil {
				return scenarios.PendingCycleNativeEvidence{}, checksumErr
			}
			if checksum != nil {
				state.TargetRowChecksum = *checksum
			}
		}
		if len(snapshot.ScopeStates) == 1 {
			scope := snapshot.ScopeStates[0]
			state.ScopeID = scope.ScopeID
			if scope.Cursor != nil {
				state.ScopeCursor = *scope.Cursor
			}
			checksum, checksumErr := swiftChecksumDigest(scope.Checksum)
			if checksumErr != nil {
				return scenarios.PendingCycleNativeEvidence{}, checksumErr
			}
			if checksum != nil {
				state.ScopeChecksum = *checksum
			}
			if scope.LocalChecksum != "" {
				localChecksum, checksumErr := swiftChecksumDigest(pointerString(scope.LocalChecksum))
				if checksumErr != nil {
					return scenarios.PendingCycleNativeEvidence{}, checksumErr
				}
				if localChecksum != nil {
					state.LocalScopeChecksum = *localChecksum
				}
			}
		}
		for _, row := range snapshot.ScopeRows {
			if row.TableName != target.TableName {
				return scenarios.PendingCycleNativeEvidence{}, errors.New("Swift pending-cycle scope row differs from its table")
			}
			switch row.RecordID {
			case target.RecordID:
				if state.TargetScopeRowPresent {
					return scenarios.PendingCycleNativeEvidence{}, errors.New("Swift pending-cycle target scope row is duplicated")
				}
				state.TargetScopeRowPresent = true
				state.TargetScopeRowChecksum = row.Checksum
			case target.UnprotectedRecordID:
				if state.UnprotectedScopeRowPresent {
					return scenarios.PendingCycleNativeEvidence{}, errors.New("Swift pending-cycle unprotected scope row is duplicated")
				}
				state.UnprotectedScopeRowPresent = true
				state.UnprotectedScopeRowChecksum = row.Checksum
			default:
				return scenarios.PendingCycleNativeEvidence{}, errors.New("Swift pending-cycle scope row differs from its targets")
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

type swiftApplicationRowLifecycle struct {
	PrimaryKeyField string
	RecordID        string
	DeletedAtField  string
}

func swiftLogicalApplicationRows(rawCount int, rows []map[string]json.RawMessage, lifecycles []swiftApplicationRowLifecycle) (int, []map[string]json.RawMessage, error) {
	logicalCount := rawCount
	logicalRows := make([]map[string]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		deletedAtField := ""
		for _, lifecycle := range lifecycles {
			var recordID string
			if lifecycle.DeletedAtField != "" && json.Unmarshal(row[lifecycle.PrimaryKeyField], &recordID) == nil && recordID == lifecycle.RecordID {
				if deletedAtField != "" {
					return 0, nil, errors.New("Swift application row lifecycle is ambiguous")
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
			return 0, nil, errors.New("Swift application row deleted-at field is absent")
		}
		if !json.Valid(deletedAt) {
			return 0, nil, errors.New("Swift application row deleted-at field is invalid")
		}
		var value any
		if err := json.Unmarshal(deletedAt, &value); err != nil {
			return 0, nil, errors.New("Swift application row deleted-at field is invalid")
		}
		if value == nil {
			logicalRows = append(logicalRows, row)
			continue
		}
		logicalCount--
	}
	if logicalCount < 0 {
		return 0, nil, errors.New("Swift application row count is invalid")
	}
	return logicalCount, logicalRows, nil
}
