package swift

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

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
	steps, err := swiftScenarioStepMap(scenario, pendingCycleScenarioID, 6)
	if err != nil {
		return PendingCycleResult{}, err
	}
	if controller == nil || platform == nil {
		return PendingCycleResult{}, errors.New("Swift pending-cycle dependencies are unavailable")
	}
	for _, id := range []string{
		"STEP-PERF-PENDING-CYCLE-001",
		"STEP-PERF-PENDING-CYCLE-002",
		"STEP-PERF-PENDING-CYCLE-003",
	} {
		if err := swiftScenarioClient(steps[scenarios.StepID(id)], client); err != nil {
			return PendingCycleResult{}, err
		}
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return PendingCycleResult{}, fmt.Errorf("install Swift pending-cycle contract: %w", err)
	}
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
	if err := platform.Install(ctx, client, "current", ""); err != nil {
		return PendingCycleResult{}, fmt.Errorf("install Swift pending-cycle client: %w", err)
	}
	beforeWrite, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Swift pending-cycle pre-write state: %w", err)
	}

	write, err := swiftScenarioOperation(steps, "STEP-PERF-PENDING-CYCLE-001", "local/write")
	if err != nil {
		return PendingCycleResult{}, err
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
	target := scenarios.PendingCycleNativeTarget{TableName: action.TableName, PrimaryKeyField: action.PrimaryKeyField, RecordID: recordID, Value: "pending"}
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

	push, err := swiftScenarioCall(ctx, platform, client, "start")
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("run Swift pending push: %w", err)
	}
	pushObservation, err := swiftScenarioWire(push, "push")
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("Swift pending push transport is absent: completion %q, call error category <none>, operation classes %s: %w", push.Completion, swiftPendingCycleOperationClasses(push), err)
	}
	if push.Completion != "idle" || pushObservation.StatusCode != 200 || pushObservation.Retryable {
		// The observed values separate a push the server rejected from a push
		// the client left in durable backoff.
		state, stateErr := platform.client(client)
		report := "client unavailable"
		if stateErr == nil {
			report = state.session.stderrReport()
		}
		return PendingCycleResult{}, fmt.Errorf(
			"Swift pending push did not complete successfully: completion %q, status %d, retryable %t, error code %s (runner reported: %s)",
			push.Completion, pushObservation.StatusCode, pushObservation.Retryable, optionalStringOrNone(pushObservation.ErrorCode), report)
	}
	if err := validateSwiftWireExpectation(scenario, "STEP-PERF-PENDING-CYCLE-002", "push", push); err != nil {
		return PendingCycleResult{}, err
	}
	authoredPush, err := swiftScenarioOperation(steps, "STEP-PERF-PENDING-CYCLE-002", "push/submit")
	if err != nil {
		return PendingCycleResult{}, err
	}
	if err := controller.BindApplicationPush(authoredPush); err != nil {
		return PendingCycleResult{}, fmt.Errorf("bind Swift pending push transaction: %w", err)
	}
	afterPush, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Swift pending-cycle accepted push: %w", err)
	}

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
	if _, err := controller.ProcessStep(ctx, nil, materialize); err != nil {
		return PendingCycleResult{}, fmt.Errorf("materialize Swift pending mutation: %w", err)
	}
	pull, err := swiftScenarioOperation(steps, "STEP-PERF-PENDING-CYCLE-003", "pull/request-page")
	if err != nil {
		return PendingCycleResult{}, err
	}
	beforePull, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Swift pending pull checkpoint: %w", err)
	}
	if len(beforePull.ScopeStates) != 1 || beforePull.ScopeStates[0].Cursor == nil || *beforePull.ScopeStates[0].Cursor == "" {
		scopes := make([]string, 0, len(beforePull.ScopeStates))
		for _, scope := range beforePull.ScopeStates {
			scopes = append(scopes, scope.ScopeID+":"+optionalStringOrNone(scope.Cursor))
		}
		state, stateErr := platform.client(client)
		report := "client unavailable"
		if stateErr == nil {
			report = state.session.stderrReport()
		}
		return PendingCycleResult{}, fmt.Errorf("Swift pending pull checkpoint is invalid: scopes %v (runner reported: %s)", scopes, report)
	}
	var runtimePullPayload map[string]any
	if err := json.Unmarshal(pull.Payload, &runtimePullPayload); err != nil {
		return PendingCycleResult{}, errors.New("decode Swift pending pull runtime binding failed")
	}
	rawScopes, ok := runtimePullPayload["scopes"].([]any)
	if !ok || len(rawScopes) != 1 {
		return PendingCycleResult{}, errors.New("Swift pending pull scope binding is invalid")
	}
	for _, rawScope := range rawScopes {
		scope, ok := rawScope.(map[string]any)
		if !ok || scope["cursor_source"] != "none" {
			return PendingCycleResult{}, errors.New("Swift pending pull authored cursor source is invalid")
		}
		scope["cursor_source"] = "local_checkpoint"
	}
	runtimePull := pull
	runtimePull.Payload, err = json.Marshal(runtimePullPayload)
	if err != nil || scenarios.ValidateOperation(runtimePull) != nil {
		return PendingCycleResult{}, errors.New("encode Swift pending pull runtime binding failed")
	}
	pullCall, err := platform.Synchronize(ctx, client, "sync-now", RequestOperations{runtimePull})
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("run Swift pending pull: %w", err)
	}
	if pullCall.Completion != "idle" || len(pullCall.Steps) != 1 || len(pullCall.transportObservations) != 1 || pullCall.transportObservations[0].StatusCode != 200 {
		return PendingCycleResult{}, errors.New("Swift pending pull did not complete successfully")
	}
	if err := validateSwiftWireExpectation(scenario, "STEP-PERF-PENDING-CYCLE-003", "pull", pullCall); err != nil {
		return PendingCycleResult{}, err
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

func swiftPendingCycleOperationClasses(call SynchronizationResult) string {
	if len(call.transportObservations) == 0 {
		return "<none>"
	}
	classes := make([]string, 0, len(call.transportObservations))
	for _, observation := range call.transportObservations {
		classes = append(classes, observation.OperationClass)
	}
	return strings.Join(classes, ",")
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
	call, err := swiftScenarioCall(ctx, platform, client, "start")
	if err != nil {
		return runnerResult{}, fmt.Errorf("run Swift pending-cycle %s push: %w", name, err)
	}
	observation, err := swiftScenarioWire(call, "push")
	if err != nil {
		return runnerResult{}, err
	}
	if call.Completion != "idle" || observation.StatusCode != 200 || observation.Retryable {
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
		state := scenarios.PendingCycleNativeState{
			ProcessID:                   snapshot.ProcessID,
			DatabaseIdentityFingerprint: snapshot.DatabaseIdentityFingerprint,
			ApplicationRowCount:         *snapshot.ApplicationRowCount,
			PendingChangeCount:          *snapshot.PendingChangeCount,
			MutationLedgerCount:         *snapshot.MutationLedgerCount,
			MutationOutcomeCount:        *snapshot.MutationOutcomeCount,
			RejectedMutationCount:       *snapshot.RejectedMutationCount,
			ScopeStateCount:             *snapshot.ScopeStateCount,
			ScopeRowCount:               *snapshot.ScopeRowCount,
			RowMetadataCount:            *snapshot.RowMetadataCount,
		}
		for _, row := range snapshot.ApplicationRows {
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
