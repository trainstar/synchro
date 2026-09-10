package kotlin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

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
	steps, err := kotlinScenarioStepMap(scenario, pendingCycleScenarioID, 4)
	if err != nil {
		return PendingCycleResult{}, err
	}
	if controller == nil || platform == nil {
		return PendingCycleResult{}, errors.New("Kotlin Android pending-cycle dependencies are unavailable")
	}
	for _, id := range []string{"STEP-PERF-PENDING-CYCLE-001", "STEP-PERF-PENDING-CYCLE-002", "STEP-PERF-PENDING-CYCLE-003"} {
		if err := kotlinScenarioClient(steps[scenarios.StepID(id)], client); err != nil {
			return PendingCycleResult{}, err
		}
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return PendingCycleResult{}, fmt.Errorf("install Kotlin Android pending-cycle contract: %w", err)
	}
	if err := platform.Install(ctx, InstallRequest{Client: client, Initialization: "current"}); err != nil {
		return PendingCycleResult{}, fmt.Errorf("install Kotlin Android pending-cycle client: %w", err)
	}
	beforeWrite, err := platform.scenarioSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Kotlin Android pending-cycle pre-write state: %w", err)
	}
	write, err := kotlinScenarioOperation(steps, "STEP-PERF-PENDING-CYCLE-001", "local/write")
	if err != nil {
		return PendingCycleResult{}, err
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
	target := scenarios.PendingCycleNativeTarget{TableName: action.TableName, PrimaryKeyField: action.PrimaryKeyField, RecordID: recordID, Value: "pending"}
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
	local, err := platform.ApplyStep(ctx, client, write)
	if err != nil || local.Disposition != "success" {
		return PendingCycleResult{}, fmt.Errorf("apply Kotlin Android pending mutation: %w", kotlinResultError(err, local.Disposition))
	}
	afterWrite, err := platform.scenarioSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Kotlin Android pending-cycle local write: %w", err)
	}
	push, err := kotlinScenarioCall(ctx, platform, client, "start")
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("run Kotlin Android pending push: %w", err)
	}
	pushObservation, err := kotlinScenarioWire(push, "push")
	if err != nil {
		return PendingCycleResult{}, err
	}
	if push.Completion != "idle" || pushObservation.StatusCode != 200 || pushObservation.Retryable == nil || *pushObservation.Retryable {
		return PendingCycleResult{}, errors.New("Kotlin Android pending push did not complete successfully")
	}
	if err := validateKotlinWireExpectation(scenario, "STEP-PERF-PENDING-CYCLE-002", "push", push); err != nil {
		return PendingCycleResult{}, err
	}
	authoredPush, err := kotlinScenarioOperation(steps, "STEP-PERF-PENDING-CYCLE-002", "push/submit")
	if err != nil {
		return PendingCycleResult{}, err
	}
	if err := controller.BindApplicationPush(authoredPush); err != nil {
		return PendingCycleResult{}, fmt.Errorf("bind Kotlin Android pending push transaction: %w", err)
	}
	afterPush, err := platform.scenarioSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Kotlin Android pending-cycle accepted push: %w", err)
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
	beforePull, err := platform.scenarioSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Kotlin Android pending pull checkpoint: %w", err)
	}
	states, err := androidCursorScopeStates(beforePull.ScopeStates)
	if err != nil || len(states) != 1 || states[0].Cursor == nil || *states[0].Cursor == "" {
		return PendingCycleResult{}, errors.New("Kotlin Android pending pull checkpoint is invalid")
	}
	var runtimePayload map[string]any
	if err := json.Unmarshal(pull.Payload, &runtimePayload); err != nil {
		return PendingCycleResult{}, errors.New("decode Kotlin Android pending pull runtime binding failed")
	}
	rawScopes, ok := runtimePayload["scopes"].([]any)
	if !ok || len(rawScopes) != 1 {
		return PendingCycleResult{}, errors.New("Kotlin Android pending pull scope binding is invalid")
	}
	for _, rawScope := range rawScopes {
		scope, ok := rawScope.(map[string]any)
		if !ok || scope["cursor_source"] != "none" {
			return PendingCycleResult{}, errors.New("Kotlin Android pending pull authored cursor source is invalid")
		}
		scope["cursor_source"] = "local_checkpoint"
	}
	runtimePull := pull
	runtimePull.Payload, err = json.Marshal(runtimePayload)
	if err != nil || scenarios.ValidateOperation(runtimePull) != nil {
		return PendingCycleResult{}, errors.New("encode Kotlin Android pending pull runtime binding failed")
	}
	pullCall, err := platform.Synchronize(ctx, SynchronizeRequest{Client: client, Method: "sync-now", Operations: []scenarios.Operation{runtimePull}})
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("run Kotlin Android pending pull: %w", err)
	}
	if pullCall.Completion != "idle" || len(pullCall.Steps) != 1 || len(pullCall.transportObservations) != 1 || pullCall.transportObservations[0].StatusCode != 200 {
		return PendingCycleResult{}, errors.New("Kotlin Android pending pull did not complete successfully")
	}
	if err := validateKotlinWireExpectation(scenario, "STEP-PERF-PENDING-CYCLE-003", "pull", pullCall); err != nil {
		return PendingCycleResult{}, err
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
	return PendingCycleResult{PushCall: push, PullCall: pullCall, ClientFacts: clientFacts, ServerFacts: serverCaptures[0].StateFacts, Evidence: evidence}, nil
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
	call, err := kotlinScenarioCall(ctx, platform, client, "start")
	if err != nil {
		return Result{}, fmt.Errorf("run Kotlin Android pending-cycle %s push: %w", name, err)
	}
	observation, err := kotlinScenarioWire(call, "push")
	if err != nil {
		return Result{}, err
	}
	if call.Completion != "idle" || observation.StatusCode != 200 || observation.Retryable == nil || *observation.Retryable {
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
		applicationRows, decodeErr := androidApplicationRows(snapshot.ApplicationRows)
		if decodeErr != nil {
			return scenarios.PendingCycleNativeEvidence{}, decodeErr
		}
		for _, row := range applicationRows {
			var recordID string
			if json.Unmarshal(row[target.PrimaryKeyField], &recordID) != nil || recordID != target.RecordID {
				continue
			}
			if state.TargetRowPresent || json.Unmarshal(row[target.ValueField], &state.TargetRowValue) != nil {
				return scenarios.PendingCycleNativeEvidence{}, errors.New("Kotlin Android pending-cycle application row is invalid")
			}
			state.TargetRowPresent = true
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
			if row.TableName != target.TableName || row.RecordID != target.RecordID {
				continue
			}
			if state.TargetScopeRowPresent {
				return scenarios.PendingCycleNativeEvidence{}, errors.New("Kotlin Android pending-cycle scope row is duplicated")
			}
			state.TargetScopeRowPresent = true
			state.TargetScopeRowChecksum = row.Checksum
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
