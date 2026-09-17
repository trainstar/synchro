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

const queueReplayScenarioID = "SCN-PERF-QUEUE-REPLAY-001"

// QueueReplayResult records direct Kotlin Android evidence for queue-replay.
type QueueReplayResult struct {
	ReplayCalls []SynchronizationResult
	ClientFacts []CaptureFacts
	ServerFacts scenarios.StateFacts
	Successor   scenarios.NativeQueueSuccessorEvidence
	CRUD        scenarios.NativeCRUDEvidence
}

type queueOutcomeCounts struct {
	Total    uint64
	Rejected uint64
}

// RunQueueReplayScenario executes the authored queue workload through Kotlin Android.
func RunQueueReplayScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, platform *Platform, client Client) (QueueReplayResult, error) {
	steps, err := kotlinScenarioStepMap(scenario, queueReplayScenarioID, 9)
	if err != nil {
		return QueueReplayResult{}, err
	}
	if controller == nil || platform == nil {
		return QueueReplayResult{}, errors.New("Kotlin Android queue-replay dependencies are unavailable")
	}
	for _, step := range steps {
		if step.NativeBinding == nil || step.NativeBinding.Kind != "workload" || step.NativeBinding.UserID != client.UserID || step.NativeBinding.ClientID != client.ClientID || step.NativeBinding.Workload == nil {
			return QueueReplayResult{}, fmt.Errorf("Kotlin Android queue-replay step %s workload binding is invalid", step.ID)
		}
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return QueueReplayResult{}, fmt.Errorf("install Kotlin Android queue-replay contract: %w", err)
	}
	if err := platform.Install(ctx, InstallRequest{Client: client, Initialization: "current"}); err != nil {
		return QueueReplayResult{}, fmt.Errorf("install Kotlin Android queue-replay client: %w", err)
	}
	current, err := scenarios.InitialQueueReplaySchema(scenario.Model.Setup[0])
	if err != nil {
		return QueueReplayResult{}, err
	}
	nextCommitLSN := uint64(1)
	replayCalls := make([]SynchronizationResult, 0, len(scenario.Steps))
	priorOutcomes := queueOutcomeCounts{}
	scenarioStarted := time.Now()
	for index := 1; index <= len(scenario.Steps); index++ {
		stepID := scenarios.StepID(fmt.Sprintf("STEP-PERF-QUEUE-REPLAY-%03d", index))
		step := steps[stepID]
		stepStarted := time.Now()
		inputs, err := scenarios.BuildQueueReplayInputs(step, current, nextCommitLSN)
		if err != nil {
			return QueueReplayResult{}, err
		}
		for ordinal, operation := range inputs.Local {
			operation, err = controller.ApplicationWrite(operation)
			if err != nil {
				return QueueReplayResult{}, fmt.Errorf("bind Kotlin Android queue-replay local write %d for step %s after scenario elapsed %s and step elapsed %s: %w", ordinal+1, stepID, time.Since(scenarioStarted), time.Since(stepStarted), err)
			}
			observation, applyErr := platform.ApplyStep(ctx, client, operation)
			if applyErr != nil || observation.Disposition != "success" {
				return QueueReplayResult{}, fmt.Errorf("apply Kotlin Android queue-replay local write %d for step %s after scenario elapsed %s and step elapsed %s: %w", ordinal+1, stepID, time.Since(scenarioStarted), time.Since(stepStarted), kotlinResultError(applyErr, observation.Disposition))
			}
		}
		restart := scenarios.Operation{ContractOperation: "process", Name: "restart-client", Payload: queueJSON(map[string]any{"user_id": client.UserID, "client_id": client.ClientID})}
		if err := scenarios.ValidateOperation(restart); err != nil {
			return QueueReplayResult{}, fmt.Errorf("validate Kotlin Android queue-replay restart: %w", err)
		}
		if observation, err := platform.ProcessStep(ctx, client, restart); err != nil || observation.Disposition != "success" {
			return QueueReplayResult{}, fmt.Errorf("restart Kotlin Android queue-replay client for step %s: %w", stepID, kotlinResultError(err, observation.Disposition))
		}
		if observation, err := controller.ApplyStep(ctx, inputs.Publish); err != nil || observation.Disposition != "success" {
			return QueueReplayResult{}, fmt.Errorf("publish Kotlin Android queue-replay schema for step %s: %w", stepID, kotlinResultError(err, observation.Disposition))
		}
		if err := queueRequireSchemaReset(ctx, platform, client, stepID); err != nil {
			return QueueReplayResult{}, err
		}
		loss, err := queueResponseLossOperation(client, inputs.BatchID)
		if err != nil {
			return QueueReplayResult{}, err
		}
		lost, err := platform.Synchronize(ctx, SynchronizeRequest{Client: client, Method: "reset-schema-and-start", Operations: []scenarios.Operation{inputs.DropPush}})
		if err != nil {
			return QueueReplayResult{}, fmt.Errorf("run Kotlin Android queue-replay response-loss push for step %s: %w", stepID, err)
		}
		if lost.Completion != "blocked" || len(lost.transportObservations) == 0 || lost.transportObservations[len(lost.transportObservations)-1].OperationClass != "push" || lost.transportObservations[len(lost.transportObservations)-1].StatusCode != 200 {
			return QueueReplayResult{}, fmt.Errorf("Kotlin Android queue-replay response-loss push for step %s did not preserve the committed response", stepID)
		}
		if observation, err := platform.ProcessStep(ctx, client, loss); err != nil || observation.Disposition != "success" {
			return QueueReplayResult{}, fmt.Errorf("relaunch Kotlin Android queue-replay client for step %s: %w", stepID, kotlinResultError(err, observation.Disposition))
		}
		replayed, err := kotlinScenarioCall(ctx, platform, client, "start")
		if err != nil {
			return QueueReplayResult{}, fmt.Errorf("replay Kotlin Android queue-replay batch for step %s: %w", stepID, err)
		}
		push, err := kotlinScenarioWire(replayed, "push")
		if err != nil {
			return QueueReplayResult{}, err
		}
		if replayed.Completion != "idle" || push.StatusCode != 200 || push.Retryable == nil || *push.Retryable {
			return QueueReplayResult{}, fmt.Errorf("Kotlin Android queue-replay replay for step %s did not complete successfully", stepID)
		}
		capturedOutcomes, err := queueCapturedOutcomeCounts(ctx, platform, client)
		if err != nil {
			return QueueReplayResult{}, fmt.Errorf("capture Kotlin Android queue-replay outcomes for step %s: %w", stepID, err)
		}
		if err := validateQueueWorkloadOutcomeCounts(step, priorOutcomes, capturedOutcomes); err != nil {
			return QueueReplayResult{}, err
		}
		priorOutcomes = capturedOutcomes
		replayCalls = append(replayCalls, replayed)
		current = inputs.NextSchema
		nextCommitLSN += 2
	}
	clientFacts, err := platform.Capture(ctx, []Client{client}, []string{"pending-mutations", "rejected-mutations"})
	if err != nil {
		return QueueReplayResult{}, fmt.Errorf("capture Kotlin Android queue-replay client state: %w", err)
	}
	serverCaptures, err := controller.Capture(ctx, []string{client.Key}, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return QueueReplayResult{}, fmt.Errorf("capture Kotlin Android queue-replay server state: %w", kotlinResultError(err, ""))
	}
	expected, err := kotlinScenarioExpectedState(scenario, "EXPECT-PERF-QUEUE-REPLAY-SEMANTIC-001")
	if err != nil {
		return QueueReplayResult{}, err
	}
	clientState, err := mergeKotlinCaptureFacts(clientFacts)
	if err != nil {
		return QueueReplayResult{}, err
	}
	actual, err := mergeKotlinStateFacts(serverCaptures[0].StateFacts, clientState)
	if err != nil {
		return QueueReplayResult{}, err
	}
	if err := validateKotlinStateProjection(expected, actual); err != nil {
		return QueueReplayResult{}, err
	}
	successor, err := runKotlinQueueSuccessorProof(ctx, scenario.Model.Setup[0], current, controller, platform, client)
	if err != nil {
		return QueueReplayResult{}, err
	}
	crud, err := runKotlinQueueReplayCRUD(ctx, scenario.Model.Setup[0], current, controller, platform, client)
	if err != nil {
		return QueueReplayResult{}, err
	}
	return QueueReplayResult{ReplayCalls: replayCalls, ClientFacts: clientFacts, ServerFacts: serverCaptures[0].StateFacts, Successor: successor, CRUD: crud}, nil
}

func runKotlinQueueSuccessorProof(ctx context.Context, setup scenarios.Operation, current scenarios.QueueReplaySchema, controller *blackbox.NativeController, platform *Platform, client Client) (scenarios.NativeQueueSuccessorEvidence, error) {
	inspection, err := scenarios.NativeCRUDInspectionForSetup(setup, client.UserID)
	if err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, err
	}
	proofClient := Client{
		Key: client.Key + "-successor-proof", UserID: client.UserID,
		ClientID: client.ClientID + "-successor-proof", DatabaseKey: client.DatabaseKey + "-successor-proof",
	}
	assignment, err := scenarios.NativeCRUDInspectionAssignment(proofClient.UserID, proofClient.ClientID, inspection.ScopeID)
	if err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, err
	}
	if observation, applyErr := controller.ApplyStep(ctx, assignment); applyErr != nil || observation.Disposition != "success" {
		return scenarios.NativeQueueSuccessorEvidence{}, fmt.Errorf("assign Kotlin Android queue successor proof scope: %w", kotlinResultError(applyErr, observation.Disposition))
	}
	if err := platform.Install(ctx, InstallRequest{Client: proofClient, Initialization: "current"}); err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, fmt.Errorf("install Kotlin Android queue successor proof client: %w", err)
	}
	plan, err := scenarios.NewNativeCRUDPlan(current.CRUDSchema(), inspection.StreamGeneration, proofClient.UserID, proofClient.ClientID)
	if err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, err
	}
	insert, err := plan.Step("insert", nil, 30)
	if err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, err
	}
	versions := make(map[string]string, len(plan.Targets()))
	for _, target := range plan.Targets() {
		versions[target.TableID] = "queued-successor-preview-version"
	}
	update, err := plan.Step("update", versions, 32)
	if err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, err
	}
	boundInsert, err := bindKotlinQueueReplayCRUDWrites(controller, insert.LocalWrites)
	if err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, err
	}
	boundUpdate, err := bindKotlinQueueReplayCRUDWrites(controller, update.LocalWrites)
	if err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, err
	}
	targets, err := bindKotlinQueueReplayCRUDTargets(controller, plan.Targets(), boundInsert, boundUpdate)
	if err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, err
	}
	if err := applyKotlinQueueReplayCRUDWrites(ctx, platform, proofClient, boundInsert, "successor proof insert"); err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, err
	}
	before, err := platform.scenarioSnapshot(ctx, proofClient)
	if err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, fmt.Errorf("capture Kotlin Android queued intent before restart: %w", err)
	}
	restart, err := scenarios.NativeCRUDRestartOperation(proofClient.UserID, proofClient.ClientID)
	if err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, err
	}
	if observation, restartErr := platform.ProcessStep(ctx, proofClient, restart); restartErr != nil || observation.Disposition != "success" {
		return scenarios.NativeQueueSuccessorEvidence{}, fmt.Errorf("restart Kotlin Android queue successor proof client: %w", kotlinResultError(restartErr, observation.Disposition))
	}
	afterRestart, err := platform.scenarioSnapshot(ctx, proofClient)
	if err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, fmt.Errorf("capture Kotlin Android queued intent after restart: %w", err)
	}
	if err := applyKotlinQueueReplayCRUDWrites(ctx, platform, proofClient, boundUpdate, "successor proof update"); err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, err
	}
	afterChange, err := platform.scenarioSnapshot(ctx, proofClient)
	if err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, fmt.Errorf("capture Kotlin Android changed queued intent: %w", err)
	}
	evidence, err := kotlinQueueSuccessorEvidence(targets, before.RetainedMutations, afterRestart.RetainedMutations, afterChange.RetainedMutations)
	if err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, err
	}
	if err := scenarios.ValidateNativeQueueSuccessorEvidence(evidence); err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, fmt.Errorf("validate Kotlin Android queue successor evidence: %w", err)
	}
	return evidence, nil
}

func kotlinQueueSuccessorEvidence(targets []scenarios.NativeCRUDTarget, beforeRaw, restartedRaw, changedRaw json.RawMessage) (scenarios.NativeQueueSuccessorEvidence, error) {
	decode := func(raw json.RawMessage) ([]retainedMutation, error) {
		var values []retainedMutation
		if err := decodeFactArray(raw, &values, maximumRecords); err != nil {
			return nil, errors.New("Kotlin Android queue successor mutation inspection is invalid")
		}
		return values, nil
	}
	before, err := decode(beforeRaw)
	if err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, err
	}
	restarted, err := decode(restartedRaw)
	if err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, err
	}
	changed, err := decode(changedRaw)
	if err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, err
	}
	evidence := scenarios.NativeQueueSuccessorEvidence{Rows: make([]scenarios.NativeQueueSuccessorRow, 0, len(targets))}
	for _, target := range targets {
		originals := kotlinRetainedForRow(before, target)
		if len(originals) != 1 {
			return scenarios.NativeQueueSuccessorEvidence{}, fmt.Errorf("Kotlin Android queue successor original count for table %q is %d", target.TableID, len(originals))
		}
		original := originals[0]
		restartedMutation, found := kotlinRetainedByID(restarted, original.MutationID)
		if !found {
			return scenarios.NativeQueueSuccessorEvidence{}, fmt.Errorf("Kotlin Android queue successor original for table %q is absent after restart", target.TableID)
		}
		changedOriginal, found := kotlinRetainedByID(changed, original.MutationID)
		if !found {
			return scenarios.NativeQueueSuccessorEvidence{}, fmt.Errorf("Kotlin Android queue successor original for table %q is absent after changed intent", target.TableID)
		}
		successors := make([]retainedMutation, 0, 1)
		for _, mutation := range kotlinRetainedForRow(changed, target) {
			if mutation.DependsOnMutationID != nil && *mutation.DependsOnMutationID == original.MutationID && mutation.Operation == "update" {
				successors = append(successors, mutation)
			}
		}
		if len(successors) != 1 {
			return scenarios.NativeQueueSuccessorEvidence{}, fmt.Errorf("Kotlin Android queue successor changed-intent count for table %q is %d", target.TableID, len(successors))
		}
		evidence.Rows = append(evidence.Rows, scenarios.NativeQueueSuccessorRow{
			BeforeRestart: kotlinNativeQueuedMutation(original), AfterRestart: kotlinNativeQueuedMutation(restartedMutation),
			OriginalAfterChange: kotlinNativeQueuedMutation(changedOriginal), Successor: kotlinNativeQueuedMutation(successors[0]),
		})
	}
	return evidence, nil
}

func kotlinRetainedForRow(values []retainedMutation, target scenarios.NativeCRUDTarget) []retainedMutation {
	matched := make([]retainedMutation, 0, 2)
	for _, mutation := range values {
		if mutation.TableName == target.TableName && mutation.RecordID == target.RecordID {
			matched = append(matched, mutation)
		}
	}
	return matched
}

func kotlinRetainedByID(values []retainedMutation, mutationID string) (retainedMutation, bool) {
	for _, mutation := range values {
		if mutation.MutationID == mutationID {
			return mutation, true
		}
	}
	return retainedMutation{}, false
}

func kotlinNativeQueuedMutation(value retainedMutation) scenarios.NativeQueuedMutation {
	fields := make([]scenarios.NativeQueuedField, 0, len(value.AuthoredFields))
	for _, field := range value.AuthoredFields {
		fields = append(fields, scenarios.NativeQueuedField{FieldID: field.FieldID, LogicalType: field.LogicalType, Value: append(json.RawMessage(nil), field.Value...)})
	}
	return scenarios.NativeQueuedMutation{
		MutationID: value.MutationID, LocalOrder: value.LocalOrder, TableID: value.TableID, TableName: value.TableName,
		RecordID: value.RecordID, PrimaryKeyFieldID: value.PrimaryKeyFieldID, PrimaryKeyLogicalType: value.PrimaryKeyLogicalType,
		Operation: value.Operation, AuthoredSchemaVersion: value.AuthoredSchema.Version, AuthoredSchemaHash: value.AuthoredSchema.Hash,
		BaseVersion: value.BaseVersion, ClientVersion: value.ClientVersion, Status: value.Status, SourceKind: value.SourceKind,
		DependsOnMutationID: value.DependsOnMutationID, NormalizedMutationID: value.NormalizedMutationID,
		SealedBatchID: value.SealedBatchID, SealedOrdinal: value.SealedOrdinal, AuthoredFields: fields,
	}
}

func runKotlinQueueReplayCRUD(ctx context.Context, setup scenarios.Operation, current scenarios.QueueReplaySchema, controller *blackbox.NativeController, platform *Platform, client Client) (scenarios.NativeCRUDEvidence, error) {
	inspection, err := scenarios.NativeCRUDInspectionForSetup(setup, client.UserID)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	inspectionClient := Client{
		Key: client.Key + "-crud-inspection", UserID: client.UserID,
		ClientID: client.ClientID + "-crud-inspection", DatabaseKey: client.DatabaseKey + "-crud-inspection",
	}
	assignment, err := scenarios.NativeCRUDInspectionAssignment(inspectionClient.UserID, inspectionClient.ClientID, inspection.ScopeID)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	if observation, applyErr := controller.ApplyStep(ctx, assignment); applyErr != nil || observation.Disposition != "success" {
		return scenarios.NativeCRUDEvidence{}, fmt.Errorf("assign Kotlin Android queue-replay CRUD inspection scope: %w", kotlinResultError(applyErr, observation.Disposition))
	}
	if err := platform.Install(ctx, InstallRequest{Client: inspectionClient, Initialization: "current"}); err != nil {
		return scenarios.NativeCRUDEvidence{}, fmt.Errorf("install Kotlin Android queue-replay CRUD inspection client: %w", err)
	}
	plan, err := scenarios.NewNativeCRUDPlan(current.CRUDSchema(), inspection.StreamGeneration, inspectionClient.UserID, inspectionClient.ClientID)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	insertStep, err := plan.Step("insert", nil, 20)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	boundInsert, err := bindKotlinQueueReplayCRUDWrites(controller, insertStep.LocalWrites)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	previewVersions := make(map[string]string, len(plan.Targets()))
	for _, target := range plan.Targets() {
		previewVersions[target.TableID] = "preview-server-version"
	}
	previewUpdate, err := plan.Step("update", previewVersions, 22)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	boundPreviewUpdate, err := bindKotlinQueueReplayCRUDWrites(controller, previewUpdate.LocalWrites)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	targets, err := bindKotlinQueueReplayCRUDTargets(controller, plan.Targets(), boundInsert, boundPreviewUpdate)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	beforeSnapshot, err := platform.scenarioSnapshot(ctx, inspectionClient)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, fmt.Errorf("capture Kotlin Android queue-replay CRUD initial state: %w", err)
	}
	before, err := kotlinQueueReplayCRUDState(beforeSnapshot, targets)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	if err := applyKotlinQueueReplayCRUDWrites(ctx, platform, inspectionClient, boundInsert, "insert"); err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	insertWrite, err := captureKotlinQueueReplayCRUDState(ctx, platform, inspectionClient, targets, "local insert")
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	insertResponse, insertAccepted, insertRestart, err := completeKotlinQueueReplayCRUDStep(ctx, controller, platform, inspectionClient, insertStep, targets)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	insertVersions, err := scenarios.NativeCRUDServerVersions(insertAccepted)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}

	updateStep, err := plan.Step("update", insertVersions, 22)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	boundUpdate, err := bindKotlinQueueReplayCRUDWrites(controller, updateStep.LocalWrites)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	targets, err = bindKotlinQueueReplayCRUDTargets(controller, plan.Targets(), boundInsert, boundUpdate)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	if observation, lifecycleErr := platform.Lifecycle(ctx, LifecycleRequest{Client: inspectionClient, Operation: "stop"}); lifecycleErr != nil || observation.Disposition != "success" {
		return scenarios.NativeCRUDEvidence{}, fmt.Errorf("stop Kotlin Android queue-replay CRUD client before update: %w", kotlinResultError(lifecycleErr, observation.Disposition))
	}
	if err := applyKotlinQueueReplayCRUDWrites(ctx, platform, inspectionClient, boundUpdate, "update"); err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	updateWrite, err := captureKotlinQueueReplayCRUDState(ctx, platform, inspectionClient, targets, "local update")
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	updateResponse, updateAccepted, updateRestart, err := completeKotlinQueueReplayCRUDStep(ctx, controller, platform, inspectionClient, updateStep, targets)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	updateVersions, err := scenarios.NativeCRUDServerVersions(updateAccepted)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}

	deleteStep, err := plan.Step("delete", updateVersions, 24)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	boundDelete, err := bindKotlinQueueReplayCRUDWrites(controller, deleteStep.LocalWrites)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	if observation, lifecycleErr := platform.Lifecycle(ctx, LifecycleRequest{Client: inspectionClient, Operation: "stop"}); lifecycleErr != nil || observation.Disposition != "success" {
		return scenarios.NativeCRUDEvidence{}, fmt.Errorf("stop Kotlin Android queue-replay CRUD client before delete: %w", kotlinResultError(lifecycleErr, observation.Disposition))
	}
	if err := applyKotlinQueueReplayCRUDWrites(ctx, platform, inspectionClient, boundDelete, "delete"); err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	deleteWrite, err := captureKotlinQueueReplayCRUDState(ctx, platform, inspectionClient, targets, "local delete")
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	deleteResponse, deleteAccepted, deleteRestart, err := completeKotlinQueueReplayCRUDStep(ctx, controller, platform, inspectionClient, deleteStep, targets)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	evidence := scenarios.NativeCRUDEvidence{
		Targets: targets, Before: before, AfterInsertWrite: insertWrite, AfterInsertResponse: insertAccepted, AfterInsertRestart: insertRestart,
		AfterUpdateWrite: updateWrite, AfterUpdateResponse: updateAccepted, AfterUpdateRestart: updateRestart,
		AfterDeleteWrite: deleteWrite, AfterDeleteResponse: deleteAccepted, AfterDeleteRestart: deleteRestart,
		Responses: []scenarios.NativeCRUDResponse{insertResponse, updateResponse, deleteResponse},
	}
	if err := scenarios.ValidateNativeCRUDEvidence(evidence); err != nil {
		return scenarios.NativeCRUDEvidence{}, fmt.Errorf("validate Kotlin Android queue-replay CRUD evidence: %w", err)
	}
	return evidence, nil
}

func bindKotlinQueueReplayCRUDWrites(controller *blackbox.NativeController, operations []scenarios.Operation) ([]scenarios.Operation, error) {
	bound := make([]scenarios.Operation, 0, len(operations))
	for index, operation := range operations {
		value, err := controller.ApplicationWrite(operation)
		if err != nil {
			return nil, fmt.Errorf("bind Kotlin Android queue-replay CRUD local write %d: %w", index+1, err)
		}
		bound = append(bound, value)
	}
	return bound, nil
}

func bindKotlinQueueReplayCRUDTargets(controller *blackbox.NativeController, planTargets []scenarios.NativeCRUDPlanTarget, inserts, updates []scenarios.Operation) ([]scenarios.NativeCRUDTarget, error) {
	if len(planTargets) != len(inserts) || len(planTargets) != len(updates) {
		return nil, errors.New("Kotlin Android queue-replay CRUD runtime target count is invalid")
	}
	targets := make([]scenarios.NativeCRUDTarget, 0, len(planTargets))
	for index, target := range planTargets {
		bound, err := scenarios.BindNativeCRUDTarget(target, inserts[index], updates[index])
		if err != nil {
			return nil, fmt.Errorf("bind Kotlin Android queue-replay CRUD target %q: %w", target.TableID, err)
		}
		bound.DeletedAtField, err = controller.ApplicationDeletedAtField(target.TableID)
		if err != nil {
			return nil, fmt.Errorf("bind Kotlin Android queue-replay CRUD deleted-at field %q: %w", target.TableID, err)
		}
		targets = append(targets, bound)
	}
	return targets, nil
}

func applyKotlinQueueReplayCRUDWrites(ctx context.Context, platform *Platform, client Client, operations []scenarios.Operation, name string) error {
	for index, operation := range operations {
		observation, err := platform.ApplyStep(ctx, client, operation)
		if err != nil || observation.Disposition != "success" {
			return fmt.Errorf("apply Kotlin Android queue-replay CRUD %s %d: %w", name, index+1, kotlinResultError(err, observation.Disposition))
		}
	}
	return nil
}

func completeKotlinQueueReplayCRUDStep(ctx context.Context, controller *blackbox.NativeController, platform *Platform, client Client, step scenarios.NativeCRUDStep, targets []scenarios.NativeCRUDTarget) (scenarios.NativeCRUDResponse, scenarios.NativeCRUDState, scenarios.NativeCRUDState, error) {
	call, err := kotlinScenarioCall(ctx, platform, client, "start")
	if err != nil || call.Completion != "idle" {
		return scenarios.NativeCRUDResponse{}, scenarios.NativeCRUDState{}, scenarios.NativeCRUDState{}, fmt.Errorf("run Kotlin Android queue-replay CRUD %s: %w", step.Operation, kotlinResultError(err, call.Completion))
	}
	response := kotlinQueueReplayCRUDResponse(step.Operation, call)
	if err := controller.BindApplicationPush(step.ApplicationPush); err != nil {
		return scenarios.NativeCRUDResponse{}, scenarios.NativeCRUDState{}, scenarios.NativeCRUDState{}, fmt.Errorf("bind Kotlin Android queue-replay CRUD %s push: %w", step.Operation, err)
	}
	if observation, processErr := controller.ProcessStep(ctx, nil, step.Materialize); processErr != nil || observation.Disposition != "success" {
		return scenarios.NativeCRUDResponse{}, scenarios.NativeCRUDState{}, scenarios.NativeCRUDState{}, fmt.Errorf("materialize Kotlin Android queue-replay CRUD %s: %w", step.Operation, kotlinResultError(processErr, observation.Disposition))
	}
	accepted, err := captureKotlinQueueReplayCRUDState(ctx, platform, client, targets, step.Operation+" response")
	if err != nil {
		return scenarios.NativeCRUDResponse{}, scenarios.NativeCRUDState{}, scenarios.NativeCRUDState{}, err
	}
	restart, err := scenarios.NativeCRUDRestartOperation(client.UserID, client.ClientID)
	if err != nil {
		return scenarios.NativeCRUDResponse{}, scenarios.NativeCRUDState{}, scenarios.NativeCRUDState{}, err
	}
	if observation, restartErr := platform.ProcessStep(ctx, client, restart); restartErr != nil || observation.Disposition != "success" {
		return scenarios.NativeCRUDResponse{}, scenarios.NativeCRUDState{}, scenarios.NativeCRUDState{}, fmt.Errorf("restart Kotlin Android queue-replay CRUD client after %s: %w", step.Operation, kotlinResultError(restartErr, observation.Disposition))
	}
	restarted, err := captureKotlinQueueReplayCRUDState(ctx, platform, client, targets, step.Operation+" restart")
	if err != nil {
		return scenarios.NativeCRUDResponse{}, scenarios.NativeCRUDState{}, scenarios.NativeCRUDState{}, err
	}
	return response, accepted, restarted, nil
}

func captureKotlinQueueReplayCRUDState(ctx context.Context, platform *Platform, client Client, targets []scenarios.NativeCRUDTarget, name string) (scenarios.NativeCRUDState, error) {
	snapshot, err := platform.scenarioSnapshot(ctx, client)
	if err != nil {
		return scenarios.NativeCRUDState{}, fmt.Errorf("capture Kotlin Android queue-replay CRUD %s state: %w", name, err)
	}
	return kotlinQueueReplayCRUDState(snapshot, targets)
}

func kotlinQueueReplayCRUDState(snapshot Result, targets []scenarios.NativeCRUDTarget) (scenarios.NativeCRUDState, error) {
	if snapshot.ApplicationRowCount == nil || snapshot.PendingChangeCount == nil || snapshot.MutationLedgerCount == nil || snapshot.MutationOutcomeCount == nil || snapshot.RejectedMutationCount == nil || snapshot.RowMetadataCount == nil {
		return scenarios.NativeCRUDState{}, errors.New("Kotlin Android queue-replay CRUD inspection counts are incomplete")
	}
	applicationRows, err := androidApplicationRows(snapshot.ApplicationRows)
	if err != nil {
		return scenarios.NativeCRUDState{}, err
	}
	lifecycles := make([]kotlinApplicationRowLifecycle, 0, len(targets))
	for _, target := range targets {
		lifecycles = append(lifecycles, kotlinApplicationRowLifecycle{PrimaryKeyField: target.PrimaryKeyField, RecordID: target.RecordID, DeletedAtField: target.DeletedAtField})
	}
	applicationRowCount, applicationRows, err := kotlinLogicalApplicationRows(*snapshot.ApplicationRowCount, applicationRows, lifecycles)
	if err != nil {
		return scenarios.NativeCRUDState{}, err
	}
	var mutations []retainedMutation
	if err := decodeFactArray(snapshot.RetainedMutations, &mutations, maximumRecords); err != nil {
		return scenarios.NativeCRUDState{}, errors.New("Kotlin Android queue-replay CRUD mutation inspection is invalid")
	}
	var metadata []rowMetadataRecord
	if err := decodeFactArray(snapshot.RowMetadata, &metadata, maximumRecords); err != nil {
		return scenarios.NativeCRUDState{}, errors.New("Kotlin Android queue-replay CRUD metadata inspection is invalid")
	}
	state := scenarios.NativeCRUDState{
		ProcessID: snapshot.ProcessID, DatabaseIdentityFingerprint: snapshot.DatabaseIdentityFingerprint,
		ApplicationRowCount: applicationRowCount, PendingChangeCount: *snapshot.PendingChangeCount,
		MutationLedgerCount: *snapshot.MutationLedgerCount, MutationOutcomeCount: *snapshot.MutationOutcomeCount,
		RejectedMutationCount: *snapshot.RejectedMutationCount, RowMetadataCount: *snapshot.RowMetadataCount,
		Rows: make([]scenarios.NativeCRUDRowState, 0, len(targets)),
	}
	for _, target := range targets {
		rowState := scenarios.NativeCRUDRowState{TableID: target.TableID}
		for _, row := range applicationRows {
			var recordID string
			if json.Unmarshal(row[target.PrimaryKeyField], &recordID) != nil || recordID != target.RecordID {
				continue
			}
			if rowState.Present || !json.Valid(row[target.ValueField]) {
				return scenarios.NativeCRUDState{}, errors.New("Kotlin Android queue-replay CRUD application row is invalid")
			}
			rowState.Present = true
			rowState.Value = append(json.RawMessage(nil), row[target.ValueField]...)
		}
		for _, mutation := range mutations {
			if mutation.TableName != target.TableName || mutation.RecordID != target.RecordID {
				continue
			}
			if rowState.Mutation != nil {
				return scenarios.NativeCRUDState{}, errors.New("Kotlin Android queue-replay CRUD mutation is duplicated")
			}
			rowState.Mutation = &scenarios.NativeCRUDMutation{Operation: mutation.Operation, Status: mutation.Status, ClientVersion: mutation.ClientVersion}
		}
		for _, value := range metadata {
			if value.TableName != target.TableName || value.RecordID != target.RecordID {
				continue
			}
			if rowState.ServerVersion != "" {
				return scenarios.NativeCRUDState{}, errors.New("Kotlin Android queue-replay CRUD metadata is duplicated")
			}
			rowState.ServerVersion = value.ServerVersion
			checksum, err := androidChecksumDigest(value.RowChecksum)
			if err != nil {
				return scenarios.NativeCRUDState{}, err
			}
			if checksum != nil {
				rowState.RowChecksum = *checksum
			}
		}
		state.Rows = append(state.Rows, rowState)
	}
	return state, nil
}

func kotlinQueueReplayCRUDResponse(operation string, call SynchronizationResult) scenarios.NativeCRUDResponse {
	response := scenarios.NativeCRUDResponse{Operation: operation, Completion: call.Completion, Transport: make([]scenarios.NativeCRUDTransport, 0, len(call.transportObservations))}
	for _, observation := range call.transportObservations {
		value := scenarios.NativeCRUDTransport{OperationClass: observation.OperationClass, StatusCode: observation.StatusCode}
		if observation.ErrorCode != nil {
			value.ErrorCode = *observation.ErrorCode
		}
		if observation.Retryable != nil {
			value.Retryable = *observation.Retryable
			value.RetryablePresent = true
		}
		if observation.RequestFacts != nil && observation.RequestFacts.MutationCount != nil {
			value.MutationCount = *observation.RequestFacts.MutationCount
			value.MutationCountPresent = true
		}
		response.Transport = append(response.Transport, value)
	}
	return response
}

func queueCapturedOutcomeCounts(ctx context.Context, platform *Platform, client Client) (queueOutcomeCounts, error) {
	snapshot, err := platform.scenarioSnapshot(ctx, client)
	if err != nil {
		return queueOutcomeCounts{}, err
	}
	return queueCapturedOutcomeCountsFromResult(snapshot)
}

func queueCapturedOutcomeCountsFromResult(snapshot Result) (queueOutcomeCounts, error) {
	if snapshot.MutationOutcomeCount == nil || snapshot.RejectedMutationCount == nil || *snapshot.MutationOutcomeCount < 0 || *snapshot.RejectedMutationCount < 0 {
		return queueOutcomeCounts{}, errors.New("Kotlin Android queue-replay captured outcome counts are incomplete")
	}
	return queueOutcomeCounts{Total: uint64(*snapshot.MutationOutcomeCount), Rejected: uint64(*snapshot.RejectedMutationCount)}, nil
}

func validateQueueWorkloadOutcomeCounts(step scenarios.Step, prior, captured queueOutcomeCounts) error {
	var expected scenarios.QueueReplayWorkload
	if err := json.Unmarshal(step.Operation.Payload, &expected); err != nil {
		return fmt.Errorf("Kotlin Android queue-replay workload %s outcome payload is invalid", step.ID)
	}
	if prior.Rejected > prior.Total || captured.Rejected > captured.Total || captured.Total < prior.Total || captured.Rejected < prior.Rejected {
		return fmt.Errorf("Kotlin Android queue-replay workload %s captured outcome counts moved backward", step.ID)
	}
	observedRejected := captured.Rejected - prior.Rejected
	observedAccepted := (captured.Total - prior.Total) - observedRejected
	if observedAccepted != expected.AcceptedCount || observedRejected != expected.RejectedCount {
		return fmt.Errorf("Kotlin Android queue-replay workload %s outcome counts observed accepted=%d rejected=%d expected accepted=%d rejected=%d", step.ID, observedAccepted, observedRejected, expected.AcceptedCount, expected.RejectedCount)
	}
	return nil
}

func queueRequireSchemaReset(ctx context.Context, platform *Platform, client Client, stepID scenarios.StepID) error {
	result, err := kotlinScenarioCall(ctx, platform, client, "start")
	if err != nil {
		return fmt.Errorf("observe Kotlin Android queue-replay schema boundary for step %s: %w", stepID, err)
	}
	if result.Completion != "error" {
		return fmt.Errorf("Kotlin Android queue-replay schema boundary for step %s did not require recovery", stepID)
	}
	failure, err := platform.scenarioFailure(ctx, client)
	if err != nil {
		return fmt.Errorf("inspect Kotlin Android queue-replay schema boundary for step %s: %w", stepID, err)
	}
	if failure == nil || failure.Operation != "schema" || failure.Code != "unsupported_schema" || failure.Retryable || failure.RecoveryAction != "schema_reset" {
		return fmt.Errorf("Kotlin Android queue-replay schema boundary for step %s did not require schema reset", stepID)
	}
	return nil
}

func queueResponseLossOperation(client Client, batchID string) (scenarios.Operation, error) {
	operation := scenarios.Operation{ContractOperation: "process", Name: "response-loss", Payload: queueJSON(map[string]any{"authenticated_user_id": client.UserID, "client_id": client.ClientID, "batch_id": batchID})}
	if err := scenarios.ValidateOperation(operation); err != nil {
		return scenarios.Operation{}, fmt.Errorf("validate Kotlin Android queue-replay response loss: %w", err)
	}
	return operation, nil
}

func queueJSON(value any) json.RawMessage {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return encoded
}
