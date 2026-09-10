package swift

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/gowebpki/jcs"
	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const queueReplayScenarioID = "SCN-PERF-QUEUE-REPLAY-001"

// QueueReplayResult records direct Swift evidence for the queue-replay scenario.
type QueueReplayResult struct {
	ReplayCalls []SynchronizationResult
	ClientFacts []CaptureFacts
	ServerFacts scenarios.StateFacts
	Successor   scenarios.NativeQueueSuccessorEvidence
	CRUD        scenarios.NativeCRUDEvidence
}

type queueSchema struct {
	Version uint64
	Hash    string
	Tables  []queueSchemaTable
}

type queueSchemaTable struct {
	TableID           string             `json:"table_id"`
	RelationID        string             `json:"relation_id"`
	Name              string             `json:"name"`
	Composition       string             `json:"composition"`
	PrimaryKeyFieldID string             `json:"primary_key_field_id"`
	CreatedAtFieldID  *string            `json:"created_at_field_id"`
	UpdatedAtFieldID  *string            `json:"updated_at_field_id"`
	DeletedAtFieldID  *string            `json:"deleted_at_field_id"`
	Fields            []queueSchemaField `json:"fields"`
	Indexes           []queueSchemaIndex `json:"indexes"`
}

type queueSchemaField struct {
	FieldID          string          `json:"field_id"`
	Name             string          `json:"name"`
	Type             string          `json:"type"`
	PrimaryKey       bool            `json:"primary_key"`
	Nullable         bool            `json:"nullable"`
	Writable         bool            `json:"writable"`
	DecimalPrecision any             `json:"decimal_precision"`
	DecimalScale     any             `json:"decimal_scale"`
	DefaultWireJSON  json.RawMessage `json:"default_wire_json"`
}

type queueSchemaIndex struct {
	IndexID  string   `json:"index_id"`
	Name     string   `json:"name"`
	FieldIDs []string `json:"field_ids"`
	Unique   bool     `json:"unique"`
}

type queueSetupPayload struct {
	InitialSchema struct {
		Schema queueSchemaRef     `json:"schema"`
		Tables []queueSchemaTable `json:"tables"`
	} `json:"initial_schema"`
}

type queueSchemaRef struct {
	Version uint64 `json:"version"`
	Hash    string `json:"hash"`
}

type queueWorkloadPayload struct {
	Profile       string `json:"profile"`
	UserID        string `json:"user_id"`
	ClientID      string `json:"client_id"`
	TableID       string `json:"table_id"`
	AcceptedCount uint64 `json:"accepted_count"`
	RejectedCount uint64 `json:"rejected_count"`
}

type queueLocalPayload struct {
	AuthenticatedUserID string             `json:"authenticated_user_id"`
	ClientID            string             `json:"client_id"`
	MutationID          string             `json:"mutation_id"`
	TableID             string             `json:"table_id"`
	PK                  map[string]string  `json:"pk"`
	AuthoredSchema      queueSchemaRef     `json:"authored_schema"`
	Operation           string             `json:"operation"`
	ClientVersion       string             `json:"client_version"`
	Columns             []queueLocalColumn `json:"columns"`
}

type queueLocalColumn struct {
	FieldID string `json:"field_id"`
	Value   string `json:"value"`
}

// RunQueueReplayScenario executes the authored queue workload through Swift.
func RunQueueReplayScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, platform *Platform, client Client) (QueueReplayResult, error) {
	steps, err := swiftScenarioStepMap(scenario, queueReplayScenarioID, 9)
	if err != nil {
		return QueueReplayResult{}, err
	}
	if controller == nil || platform == nil {
		return QueueReplayResult{}, errors.New("Swift queue-replay dependencies are unavailable")
	}
	for _, step := range steps {
		if step.NativeBinding == nil || step.NativeBinding.Kind != "workload" || step.NativeBinding.UserID != client.UserID || step.NativeBinding.ClientID != client.ClientID || step.NativeBinding.Workload == nil {
			return QueueReplayResult{}, fmt.Errorf("Swift queue-replay step %s workload binding is invalid", step.ID)
		}
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return QueueReplayResult{}, fmt.Errorf("install Swift queue-replay contract: %w", err)
	}
	if err := platform.Install(ctx, client, "current", ""); err != nil {
		return QueueReplayResult{}, fmt.Errorf("install Swift queue-replay client: %w", err)
	}

	current, err := queueInitialSchema(scenario.Model.Setup[0])
	if err != nil {
		return QueueReplayResult{}, err
	}
	nextCommitLSN := uint64(1)
	replayCalls := make([]SynchronizationResult, 0, len(scenario.Steps))
	for index := 1; index <= len(scenario.Steps); index++ {
		stepID := scenarios.StepID(fmt.Sprintf("STEP-PERF-QUEUE-REPLAY-%03d", index))
		step := steps[stepID]
		workload, publish, dropPush, batchID, next, err := queueWorkloadOperations(step, current, nextCommitLSN)
		if err != nil {
			return QueueReplayResult{}, err
		}
		for ordinal, operation := range workload {
			operation, err = controller.ApplicationWrite(operation)
			if err != nil {
				return QueueReplayResult{}, fmt.Errorf("bind Swift queue-replay local write %d for step %s: %w", ordinal+1, stepID, err)
			}
			observation, applyErr := platform.ApplyStep(ctx, client, operation)
			if applyErr != nil || observation.Disposition != "success" {
				return QueueReplayResult{}, fmt.Errorf("apply Swift queue-replay local write %d for step %s: %w", ordinal+1, stepID, resultError(applyErr, observation.Disposition))
			}
		}
		restart := scenarios.Operation{ContractOperation: "process", Name: "restart-client", Payload: queueJSON(map[string]any{"user_id": client.UserID, "client_id": client.ClientID})}
		if err := scenarios.ValidateOperation(restart); err != nil {
			return QueueReplayResult{}, fmt.Errorf("validate Swift queue-replay restart: %w", err)
		}
		if _, err := platform.ProcessStep(ctx, client, restart); err != nil {
			return QueueReplayResult{}, fmt.Errorf("restart Swift queue-replay client for step %s: %w", stepID, err)
		}
		if _, err := controller.ApplyStep(ctx, publish); err != nil {
			return QueueReplayResult{}, fmt.Errorf("publish Swift queue-replay schema for step %s: %w", stepID, err)
		}
		if err := queueRequireSchemaReset(ctx, platform, client, stepID); err != nil {
			return QueueReplayResult{}, err
		}
		loss, err := queueResponseLossOperation(client, batchID)
		if err != nil {
			return QueueReplayResult{}, err
		}
		lost, err := platform.Synchronize(ctx, client, "reset-schema-and-start", RequestOperations{dropPush})
		if err != nil {
			return QueueReplayResult{}, fmt.Errorf("run Swift queue-replay response-loss push for step %s: %w", stepID, err)
		}
		if lost.Completion != "blocked" || len(lost.transportObservations) == 0 || lost.transportObservations[len(lost.transportObservations)-1].OperationClass != "push" || lost.transportObservations[len(lost.transportObservations)-1].StatusCode != 200 {
			return QueueReplayResult{}, fmt.Errorf("Swift queue-replay response-loss push for step %s did not preserve the committed response", stepID)
		}
		if _, err := platform.ProcessStep(ctx, client, loss); err != nil {
			return QueueReplayResult{}, fmt.Errorf("relaunch Swift queue-replay client for step %s: %w", stepID, err)
		}
		replayed, err := swiftScenarioCall(ctx, platform, client, "start")
		if err != nil {
			return QueueReplayResult{}, fmt.Errorf("replay Swift queue-replay batch for step %s: %w", stepID, err)
		}
		pushObservation, err := swiftScenarioWire(replayed, "push")
		if err != nil {
			return QueueReplayResult{}, err
		}
		if replayed.Completion != "idle" || pushObservation.StatusCode != 200 || pushObservation.Retryable {
			return QueueReplayResult{}, fmt.Errorf("Swift queue-replay replay for step %s did not complete successfully", stepID)
		}
		replayCalls = append(replayCalls, replayed)
		current = next
		nextCommitLSN += 2
	}

	clientFacts, err := platform.Capture(ctx, []Client{client}, []string{"pending-mutations", "rejected-mutations"})
	if err != nil {
		return QueueReplayResult{}, fmt.Errorf("capture Swift queue-replay client state: %w", err)
	}
	serverCaptures, err := controller.Capture(ctx, []string{client.Key}, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return QueueReplayResult{}, fmt.Errorf("capture Swift queue-replay server state: %w", err)
	}
	expected, err := swiftScenarioExpectedState(scenario, "EXPECT-PERF-QUEUE-REPLAY-SEMANTIC-001")
	if err != nil {
		return QueueReplayResult{}, err
	}
	clientState, err := mergeSwiftCaptureFacts(clientFacts)
	if err != nil {
		return QueueReplayResult{}, err
	}
	actual, err := mergeSwiftStateFacts(serverCaptures[0].StateFacts, clientState)
	if err != nil {
		return QueueReplayResult{}, err
	}
	if err := validateSwiftStateProjection(expected, actual); err != nil {
		return QueueReplayResult{}, err
	}
	successor, err := runSwiftQueueSuccessorProof(ctx, scenario.Model.Setup[0], current, controller, platform, client)
	if err != nil {
		return QueueReplayResult{}, err
	}
	crud, err := runSwiftQueueReplayCRUD(ctx, scenario.Model.Setup[0], current, controller, platform, client)
	if err != nil {
		return QueueReplayResult{}, err
	}
	return QueueReplayResult{ReplayCalls: replayCalls, ClientFacts: clientFacts, ServerFacts: serverCaptures[0].StateFacts, Successor: successor, CRUD: crud}, nil
}

func runSwiftQueueSuccessorProof(ctx context.Context, setup scenarios.Operation, current queueSchema, controller *blackbox.NativeController, platform *Platform, client Client) (scenarios.NativeQueueSuccessorEvidence, error) {
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
		return scenarios.NativeQueueSuccessorEvidence{}, fmt.Errorf("assign Swift queue successor proof scope: %w", resultError(applyErr, observation.Disposition))
	}
	if err := platform.Install(ctx, proofClient, "current", ""); err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, fmt.Errorf("install Swift queue successor proof client: %w", err)
	}
	plan, err := scenarios.NewNativeCRUDPlan(swiftQueueReplayCRUDSchema(current), inspection.StreamGeneration, proofClient.UserID, proofClient.ClientID)
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
	boundInsert, err := bindSwiftQueueReplayCRUDWrites(controller, insert.LocalWrites)
	if err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, err
	}
	boundUpdate, err := bindSwiftQueueReplayCRUDWrites(controller, update.LocalWrites)
	if err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, err
	}
	targets, err := bindSwiftQueueReplayCRUDTargets(plan.Targets(), boundInsert, boundUpdate)
	if err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, err
	}
	if err := applySwiftQueueReplayCRUDWrites(ctx, platform, proofClient, boundInsert, "successor proof insert"); err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, err
	}
	before, err := platform.captureSnapshot(ctx, proofClient)
	if err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, fmt.Errorf("capture Swift queued intent before restart: %w", err)
	}
	restart, err := scenarios.NativeCRUDRestartOperation(proofClient.UserID, proofClient.ClientID)
	if err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, err
	}
	if observation, restartErr := platform.ProcessStep(ctx, proofClient, restart); restartErr != nil || observation.Disposition != "success" {
		return scenarios.NativeQueueSuccessorEvidence{}, fmt.Errorf("restart Swift queue successor proof client: %w", resultError(restartErr, observation.Disposition))
	}
	afterRestart, err := platform.captureSnapshot(ctx, proofClient)
	if err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, fmt.Errorf("capture Swift queued intent after restart: %w", err)
	}
	if err := applySwiftQueueReplayCRUDWrites(ctx, platform, proofClient, boundUpdate, "successor proof update"); err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, err
	}
	afterChange, err := platform.captureSnapshot(ctx, proofClient)
	if err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, fmt.Errorf("capture Swift changed queued intent: %w", err)
	}
	evidence, err := swiftQueueSuccessorEvidence(targets, before.RetainedMutations, afterRestart.RetainedMutations, afterChange.RetainedMutations)
	if err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, err
	}
	if err := scenarios.ValidateNativeQueueSuccessorEvidence(evidence); err != nil {
		return scenarios.NativeQueueSuccessorEvidence{}, fmt.Errorf("validate Swift queue successor evidence: %w", err)
	}
	return evidence, nil
}

func swiftQueueSuccessorEvidence(targets []scenarios.NativeCRUDTarget, before, restarted, changed []retainedMutation) (scenarios.NativeQueueSuccessorEvidence, error) {
	evidence := scenarios.NativeQueueSuccessorEvidence{Rows: make([]scenarios.NativeQueueSuccessorRow, 0, len(targets))}
	for _, target := range targets {
		originals := swiftRetainedForRow(before, target)
		if len(originals) != 1 {
			return scenarios.NativeQueueSuccessorEvidence{}, fmt.Errorf("Swift queue successor original count for table %q is %d", target.TableID, len(originals))
		}
		original := originals[0]
		restartedMutation, found := swiftRetainedByID(restarted, original.MutationID)
		if !found {
			return scenarios.NativeQueueSuccessorEvidence{}, fmt.Errorf("Swift queue successor original for table %q is absent after restart", target.TableID)
		}
		changedOriginal, found := swiftRetainedByID(changed, original.MutationID)
		if !found {
			return scenarios.NativeQueueSuccessorEvidence{}, fmt.Errorf("Swift queue successor original for table %q is absent after changed intent", target.TableID)
		}
		successors := make([]retainedMutation, 0, 1)
		for _, mutation := range swiftRetainedForRow(changed, target) {
			if mutation.DependsOnMutationID != nil && *mutation.DependsOnMutationID == original.MutationID && mutation.Operation == "update" {
				successors = append(successors, mutation)
			}
		}
		if len(successors) != 1 {
			return scenarios.NativeQueueSuccessorEvidence{}, fmt.Errorf("Swift queue successor changed-intent count for table %q is %d", target.TableID, len(successors))
		}
		evidence.Rows = append(evidence.Rows, scenarios.NativeQueueSuccessorRow{
			BeforeRestart: swiftNativeQueuedMutation(original), AfterRestart: swiftNativeQueuedMutation(restartedMutation),
			OriginalAfterChange: swiftNativeQueuedMutation(changedOriginal), Successor: swiftNativeQueuedMutation(successors[0]),
		})
	}
	return evidence, nil
}

func swiftRetainedForRow(values []retainedMutation, target scenarios.NativeCRUDTarget) []retainedMutation {
	matched := make([]retainedMutation, 0, 2)
	for _, mutation := range values {
		if mutation.TableName == target.TableName && mutation.RecordID == target.RecordID {
			matched = append(matched, mutation)
		}
	}
	return matched
}

func swiftRetainedByID(values []retainedMutation, mutationID string) (retainedMutation, bool) {
	for _, mutation := range values {
		if mutation.MutationID == mutationID {
			return mutation, true
		}
	}
	return retainedMutation{}, false
}

func swiftNativeQueuedMutation(value retainedMutation) scenarios.NativeQueuedMutation {
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

func runSwiftQueueReplayCRUD(ctx context.Context, setup scenarios.Operation, current queueSchema, controller *blackbox.NativeController, platform *Platform, client Client) (scenarios.NativeCRUDEvidence, error) {
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
		return scenarios.NativeCRUDEvidence{}, fmt.Errorf("assign Swift queue-replay CRUD inspection scope: %w", resultError(applyErr, observation.Disposition))
	}
	if err := platform.Install(ctx, inspectionClient, "current", ""); err != nil {
		return scenarios.NativeCRUDEvidence{}, fmt.Errorf("install Swift queue-replay CRUD inspection client: %w", err)
	}
	plan, err := scenarios.NewNativeCRUDPlan(swiftQueueReplayCRUDSchema(current), inspection.StreamGeneration, inspectionClient.UserID, inspectionClient.ClientID)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	insertStep, err := plan.Step("insert", nil, 20)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	boundInsert, err := bindSwiftQueueReplayCRUDWrites(controller, insertStep.LocalWrites)
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
	boundPreviewUpdate, err := bindSwiftQueueReplayCRUDWrites(controller, previewUpdate.LocalWrites)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	targets, err := bindSwiftQueueReplayCRUDTargets(plan.Targets(), boundInsert, boundPreviewUpdate)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	beforeSnapshot, err := platform.captureSnapshot(ctx, inspectionClient)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, fmt.Errorf("capture Swift queue-replay CRUD initial state: %w", err)
	}
	before, err := swiftQueueReplayCRUDState(beforeSnapshot, targets)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	if err := applySwiftQueueReplayCRUDWrites(ctx, platform, inspectionClient, boundInsert, "insert"); err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	insertWrite, err := captureSwiftQueueReplayCRUDState(ctx, platform, inspectionClient, targets, "local insert")
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	insertResponse, insertAccepted, insertRestart, err := completeSwiftQueueReplayCRUDStep(ctx, controller, platform, inspectionClient, insertStep, targets)
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
	boundUpdate, err := bindSwiftQueueReplayCRUDWrites(controller, updateStep.LocalWrites)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	targets, err = bindSwiftQueueReplayCRUDTargets(plan.Targets(), boundInsert, boundUpdate)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	if observation, lifecycleErr := platform.Lifecycle(ctx, inspectionClient, "stop"); lifecycleErr != nil || observation.Disposition != "success" {
		return scenarios.NativeCRUDEvidence{}, fmt.Errorf("stop Swift queue-replay CRUD client before update: %w", resultError(lifecycleErr, observation.Disposition))
	}
	if err := applySwiftQueueReplayCRUDWrites(ctx, platform, inspectionClient, boundUpdate, "update"); err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	updateWrite, err := captureSwiftQueueReplayCRUDState(ctx, platform, inspectionClient, targets, "local update")
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	updateResponse, updateAccepted, updateRestart, err := completeSwiftQueueReplayCRUDStep(ctx, controller, platform, inspectionClient, updateStep, targets)
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
	boundDelete, err := bindSwiftQueueReplayCRUDWrites(controller, deleteStep.LocalWrites)
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	if observation, lifecycleErr := platform.Lifecycle(ctx, inspectionClient, "stop"); lifecycleErr != nil || observation.Disposition != "success" {
		return scenarios.NativeCRUDEvidence{}, fmt.Errorf("stop Swift queue-replay CRUD client before delete: %w", resultError(lifecycleErr, observation.Disposition))
	}
	if err := applySwiftQueueReplayCRUDWrites(ctx, platform, inspectionClient, boundDelete, "delete"); err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	deleteWrite, err := captureSwiftQueueReplayCRUDState(ctx, platform, inspectionClient, targets, "local delete")
	if err != nil {
		return scenarios.NativeCRUDEvidence{}, err
	}
	deleteResponse, deleteAccepted, deleteRestart, err := completeSwiftQueueReplayCRUDStep(ctx, controller, platform, inspectionClient, deleteStep, targets)
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
		return scenarios.NativeCRUDEvidence{}, fmt.Errorf("validate Swift queue-replay CRUD evidence: %w", err)
	}
	return evidence, nil
}

func swiftQueueReplayCRUDSchema(current queueSchema) scenarios.NativeCRUDSchema {
	tables := make([]scenarios.NativeCRUDSchemaTable, 0, len(current.Tables))
	for _, table := range current.Tables {
		fields := make([]scenarios.NativeCRUDSchemaField, 0, len(table.Fields))
		for _, field := range table.Fields {
			fields = append(fields, scenarios.NativeCRUDSchemaField{FieldID: field.FieldID, Type: field.Type, PrimaryKey: field.PrimaryKey, Writable: field.Writable})
		}
		tables = append(tables, scenarios.NativeCRUDSchemaTable{TableID: table.TableID, PrimaryKeyFieldID: table.PrimaryKeyFieldID, Fields: fields})
	}
	return scenarios.NativeCRUDSchema{Version: current.Version, Hash: current.Hash, Tables: tables}
}

func bindSwiftQueueReplayCRUDWrites(controller *blackbox.NativeController, operations []scenarios.Operation) ([]scenarios.Operation, error) {
	bound := make([]scenarios.Operation, 0, len(operations))
	for index, operation := range operations {
		value, err := controller.ApplicationWrite(operation)
		if err != nil {
			return nil, fmt.Errorf("bind Swift queue-replay CRUD local write %d: %w", index+1, err)
		}
		bound = append(bound, value)
	}
	return bound, nil
}

func bindSwiftQueueReplayCRUDTargets(planTargets []scenarios.NativeCRUDPlanTarget, inserts, updates []scenarios.Operation) ([]scenarios.NativeCRUDTarget, error) {
	if len(planTargets) != len(inserts) || len(planTargets) != len(updates) {
		return nil, errors.New("Swift queue-replay CRUD runtime target count is invalid")
	}
	targets := make([]scenarios.NativeCRUDTarget, 0, len(planTargets))
	for index, target := range planTargets {
		bound, err := scenarios.BindNativeCRUDTarget(target, inserts[index], updates[index])
		if err != nil {
			return nil, fmt.Errorf("bind Swift queue-replay CRUD target %q: %w", target.TableID, err)
		}
		targets = append(targets, bound)
	}
	return targets, nil
}

func applySwiftQueueReplayCRUDWrites(ctx context.Context, platform *Platform, client Client, operations []scenarios.Operation, name string) error {
	for index, operation := range operations {
		observation, err := platform.ApplyStep(ctx, client, operation)
		if err != nil || observation.Disposition != "success" {
			return fmt.Errorf("apply Swift queue-replay CRUD %s %d: %w", name, index+1, resultError(err, observation.Disposition))
		}
	}
	return nil
}

func completeSwiftQueueReplayCRUDStep(ctx context.Context, controller *blackbox.NativeController, platform *Platform, client Client, step scenarios.NativeCRUDStep, targets []scenarios.NativeCRUDTarget) (scenarios.NativeCRUDResponse, scenarios.NativeCRUDState, scenarios.NativeCRUDState, error) {
	call, err := swiftScenarioCall(ctx, platform, client, "start")
	if err != nil || call.Completion != "idle" {
		return scenarios.NativeCRUDResponse{}, scenarios.NativeCRUDState{}, scenarios.NativeCRUDState{}, fmt.Errorf("run Swift queue-replay CRUD %s: %w", step.Operation, resultError(err, call.Completion))
	}
	response := swiftQueueReplayCRUDResponse(step.Operation, call)
	if err := controller.BindApplicationPush(step.ApplicationPush); err != nil {
		return scenarios.NativeCRUDResponse{}, scenarios.NativeCRUDState{}, scenarios.NativeCRUDState{}, fmt.Errorf("bind Swift queue-replay CRUD %s push: %w", step.Operation, err)
	}
	if observation, processErr := controller.ProcessStep(ctx, nil, step.Materialize); processErr != nil || observation.Disposition != "success" {
		return scenarios.NativeCRUDResponse{}, scenarios.NativeCRUDState{}, scenarios.NativeCRUDState{}, fmt.Errorf("materialize Swift queue-replay CRUD %s: %w", step.Operation, resultError(processErr, observation.Disposition))
	}
	accepted, err := captureSwiftQueueReplayCRUDState(ctx, platform, client, targets, step.Operation+" response")
	if err != nil {
		return scenarios.NativeCRUDResponse{}, scenarios.NativeCRUDState{}, scenarios.NativeCRUDState{}, err
	}
	restart, err := scenarios.NativeCRUDRestartOperation(client.UserID, client.ClientID)
	if err != nil {
		return scenarios.NativeCRUDResponse{}, scenarios.NativeCRUDState{}, scenarios.NativeCRUDState{}, err
	}
	if observation, restartErr := platform.ProcessStep(ctx, client, restart); restartErr != nil || observation.Disposition != "success" {
		return scenarios.NativeCRUDResponse{}, scenarios.NativeCRUDState{}, scenarios.NativeCRUDState{}, fmt.Errorf("restart Swift queue-replay CRUD client after %s: %w", step.Operation, resultError(restartErr, observation.Disposition))
	}
	restarted, err := captureSwiftQueueReplayCRUDState(ctx, platform, client, targets, step.Operation+" restart")
	if err != nil {
		return scenarios.NativeCRUDResponse{}, scenarios.NativeCRUDState{}, scenarios.NativeCRUDState{}, err
	}
	return response, accepted, restarted, nil
}

func captureSwiftQueueReplayCRUDState(ctx context.Context, platform *Platform, client Client, targets []scenarios.NativeCRUDTarget, name string) (scenarios.NativeCRUDState, error) {
	snapshot, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return scenarios.NativeCRUDState{}, fmt.Errorf("capture Swift queue-replay CRUD %s state: %w", name, err)
	}
	return swiftQueueReplayCRUDState(snapshot, targets)
}

func swiftQueueReplayCRUDState(snapshot runnerResult, targets []scenarios.NativeCRUDTarget) (scenarios.NativeCRUDState, error) {
	if snapshot.ApplicationRowCount == nil || snapshot.PendingChangeCount == nil || snapshot.MutationLedgerCount == nil || snapshot.MutationOutcomeCount == nil || snapshot.RejectedMutationCount == nil || snapshot.RowMetadataCount == nil {
		return scenarios.NativeCRUDState{}, errors.New("Swift queue-replay CRUD inspection counts are incomplete")
	}
	state := scenarios.NativeCRUDState{
		ProcessID: snapshot.ProcessID, DatabaseIdentityFingerprint: snapshot.DatabaseIdentityFingerprint,
		ApplicationRowCount: *snapshot.ApplicationRowCount, PendingChangeCount: *snapshot.PendingChangeCount,
		MutationLedgerCount: *snapshot.MutationLedgerCount, MutationOutcomeCount: *snapshot.MutationOutcomeCount,
		RejectedMutationCount: *snapshot.RejectedMutationCount, RowMetadataCount: *snapshot.RowMetadataCount,
		Rows: make([]scenarios.NativeCRUDRowState, 0, len(targets)),
	}
	for _, target := range targets {
		rowState := scenarios.NativeCRUDRowState{TableID: target.TableID}
		for _, row := range snapshot.ApplicationRows {
			var recordID string
			if json.Unmarshal(row[target.PrimaryKeyField], &recordID) != nil || recordID != target.RecordID {
				continue
			}
			if rowState.Present || !json.Valid(row[target.ValueField]) {
				return scenarios.NativeCRUDState{}, errors.New("Swift queue-replay CRUD application row is invalid")
			}
			rowState.Present = true
			rowState.Value = append(json.RawMessage(nil), row[target.ValueField]...)
		}
		for _, mutation := range snapshot.RetainedMutations {
			if mutation.TableName != target.TableName || mutation.RecordID != target.RecordID {
				continue
			}
			if rowState.Mutation != nil {
				return scenarios.NativeCRUDState{}, errors.New("Swift queue-replay CRUD mutation is duplicated")
			}
			rowState.Mutation = &scenarios.NativeCRUDMutation{Operation: mutation.Operation, Status: mutation.Status, ClientVersion: mutation.ClientVersion}
		}
		for _, metadata := range snapshot.RowMetadataRecords {
			if metadata.TableName != target.TableName || metadata.RecordID != target.RecordID {
				continue
			}
			if rowState.ServerVersion != "" {
				return scenarios.NativeCRUDState{}, errors.New("Swift queue-replay CRUD metadata is duplicated")
			}
			rowState.ServerVersion = metadata.ServerVersion
			checksum, err := swiftChecksumDigest(metadata.RowChecksum)
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

func swiftQueueReplayCRUDResponse(operation string, call SynchronizationResult) scenarios.NativeCRUDResponse {
	response := scenarios.NativeCRUDResponse{Operation: operation, Completion: call.Completion, Transport: make([]scenarios.NativeCRUDTransport, 0, len(call.transportObservations))}
	for _, observation := range call.transportObservations {
		value := scenarios.NativeCRUDTransport{
			OperationClass: observation.OperationClass, StatusCode: observation.StatusCode,
			Retryable: observation.Retryable, RetryablePresent: true,
		}
		if observation.ErrorCode != nil {
			value.ErrorCode = *observation.ErrorCode
		}
		if observation.RequestFacts != nil && observation.RequestFacts.MutationCount != nil {
			value.MutationCount = *observation.RequestFacts.MutationCount
			value.MutationCountPresent = true
		}
		response.Transport = append(response.Transport, value)
	}
	return response
}

func queueRequireSchemaReset(ctx context.Context, platform *Platform, client Client, stepID scenarios.StepID) error {
	result, err := swiftScenarioCall(ctx, platform, client, "start")
	if err != nil {
		return fmt.Errorf("observe Swift queue-replay schema boundary for step %s: %w", stepID, err)
	}
	if result.Completion != "error" {
		return fmt.Errorf("Swift queue-replay schema boundary for step %s did not require recovery", stepID)
	}
	snapshot, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return fmt.Errorf("inspect Swift queue-replay schema boundary for step %s: %w", stepID, err)
	}
	if snapshot.Failure == nil || snapshot.Failure.Operation != "schema" || snapshot.Failure.Code != "unsupported_schema" || snapshot.Failure.Retryable || snapshot.Failure.RecoveryAction != "schema_reset" {
		return fmt.Errorf("Swift queue-replay schema boundary for step %s did not require schema reset", stepID)
	}
	return nil
}

func queueInitialSchema(operation scenarios.Operation) (queueSchema, error) {
	var payload queueSetupPayload
	if err := json.Unmarshal(operation.Payload, &payload); err != nil || payload.InitialSchema.Schema.Version == 0 || payload.InitialSchema.Schema.Hash == "" || len(payload.InitialSchema.Tables) != 1 {
		return queueSchema{}, errors.New("Swift queue-replay initial schema is invalid")
	}
	return queueSchema{Version: payload.InitialSchema.Schema.Version, Hash: payload.InitialSchema.Schema.Hash, Tables: payload.InitialSchema.Tables}, nil
}

func queueWorkloadOperations(step scenarios.Step, current queueSchema, commitLSN uint64) ([]scenarios.Operation, scenarios.Operation, scenarios.Operation, string, queueSchema, error) {
	binding := step.NativeBinding
	if binding == nil || binding.Workload == nil {
		return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, errors.New("Swift queue-replay workload is absent")
	}
	if binding.Workload.AuthoredSchema.Version != current.Version || binding.Workload.AuthoredSchema.Hash != current.Hash {
		return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("Swift queue-replay step %s schema does not match the current schema", step.ID)
	}
	var payload queueWorkloadPayload
	if err := json.Unmarshal(step.Operation.Payload, &payload); err != nil || payload.Profile != "pending_mutations" || payload.UserID != binding.UserID || payload.ClientID != binding.ClientID || payload.TableID == "" || payload.RejectedCount != 1 || payload.AcceptedCount != binding.Workload.RecordCount-1 {
		return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("Swift queue-replay step %s workload payload is invalid", step.ID)
	}
	if binding.Workload.RecordCount == 0 || binding.Workload.BatchSize == 0 || len(current.Tables) != 1 || len(binding.Workload.Targets) != 1 {
		return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("Swift queue-replay step %s workload parameters are invalid", step.ID)
	}
	table := &current.Tables[0]
	if table.TableID != payload.TableID || table.PrimaryKeyFieldID != binding.Workload.Targets[0].PrimaryKeyFieldID || binding.Workload.Targets[0].TableID != table.TableID {
		return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("Swift queue-replay step %s workload target is invalid", step.ID)
	}
	rejectionField, acceptedField, err := queueMutationFields(*table)
	if err != nil {
		return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, err
	}
	total := binding.Workload.RecordCount
	local, err := queueExpandWorkload(step)
	if err != nil {
		return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("expand Swift queue-replay workload %s: %w", step.ID, err)
	}
	if uint64(len(local)) != total {
		return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("Swift queue-replay step %s expanded to %d operations, want %d", step.ID, len(local), total)
	}
	wire := make([]map[string]any, 0, total)
	for ordinal, operation := range local {
		var value queueLocalPayload
		if err := json.Unmarshal(operation.Payload, &value); err != nil || value.AuthenticatedUserID != binding.UserID || value.ClientID != binding.ClientID || value.TableID != table.TableID || value.MutationID == "" || value.Operation != "insert" || value.ClientVersion != binding.Workload.ClientVersion || value.AuthoredSchema.Version != current.Version || value.AuthoredSchema.Hash != current.Hash || len(value.PK) != 1 || value.PK[table.PrimaryKeyFieldID] == "" {
			return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("Swift queue-replay local write %d has invalid authored identity", ordinal+1)
		}
		columns := make(map[string]any, len(value.Columns))
		for _, column := range value.Columns {
			if column.FieldID == "" || column.FieldID == table.PrimaryKeyFieldID {
				return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("Swift queue-replay local write %d has an invalid field", ordinal+1)
			}
			if _, duplicate := columns[column.FieldID]; duplicate {
				return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("Swift queue-replay local write %d repeats a field", ordinal+1)
			}
			columns[column.FieldID] = column.Value
		}
		if len(columns) == 0 || ordinal+1 < len(local) && len(columns) != 1 || ordinal+1 == len(local) && len(columns) != 2 {
			return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("Swift queue-replay local write %d has the wrong field count", ordinal+1)
		}
		if _, ok := columns[acceptedField.FieldID]; !ok {
			return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("Swift queue-replay local write %d lacks the accepted field", ordinal+1)
		}
		if ordinal+1 < len(local) {
			if _, rejected := columns[rejectionField.FieldID]; rejected {
				return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("Swift queue-replay local write %d has the rejection field", ordinal+1)
			}
		} else if _, rejected := columns[rejectionField.FieldID]; !rejected {
			return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("Swift queue-replay terminal local write lacks the rejection field")
		}
		wire = append(wire, map[string]any{
			"mutation_id":     value.MutationID,
			"table":           value.TableID,
			"pk":              value.PK,
			"authored_schema": map[string]any{"version": value.AuthoredSchema.Version, "hash": value.AuthoredSchema.Hash},
			"op":              value.Operation,
			"client_version":  value.ClientVersion,
			"columns":         columns,
		})
	}
	next, publish, err := queueNextSchema(current, rejectionField.FieldID, current.Version+1)
	if err != nil {
		return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, err
	}
	batchID := queueWorkloadUUID("batch", binding.UserID, binding.ClientID, current.Version, total)
	request := map[string]any{
		"client_id":         binding.ClientID,
		"client_generation": 1,
		"batch_id":          batchID,
		"schema":            map[string]any{"version": next.Version, "hash": next.Hash},
		"mutations":         wire,
	}
	pushPayload := map[string]any{
		"authenticated_user_id": binding.UserID,
		"request":               request,
		"delivery":              "drop_after_server",
		"commit_lsn":            strconv.FormatUint(commitLSN, 10),
		"end_lsn":               strconv.FormatUint(commitLSN+1, 10),
	}
	dropPush := scenarios.Operation{ContractOperation: "push", Name: "submit", Payload: queueJSON(pushPayload)}
	if err := scenarios.ValidateOperation(dropPush); err != nil {
		return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("validate Swift queue-replay push: %w", err)
	}
	return local, publish, dropPush, batchID, next, nil
}

func queueExpandWorkload(step scenarios.Step) ([]scenarios.Operation, error) {
	binding := step.NativeBinding
	if binding == nil || binding.Workload == nil {
		return nil, fmt.Errorf("step %s has no native workload binding", step.ID)
	}
	parameters := binding.Workload
	kinds := make([]scenarios.NativeWorkloadMutationKind, 0, parameters.RecordCount)
	for _, kind := range parameters.MutationKinds {
		for count := uint64(0); count < kind.Count; count++ {
			kinds = append(kinds, kind)
		}
	}
	if len(kinds) != int(parameters.RecordCount) {
		return nil, fmt.Errorf("step %s workload mutation kinds do not cover record_count", step.ID)
	}
	operations := make([]scenarios.Operation, 0, parameters.RecordCount)
	for ordinal := uint64(0); ordinal < parameters.RecordCount; ordinal++ {
		target := parameters.Targets[ordinal%uint64(len(parameters.Targets))]
		batchOrdinal := ordinal / parameters.BatchSize
		ordinalInBatch := ordinal % parameters.BatchSize
		kind := kinds[ordinal]
		fieldIDs := append([]string(nil), kind.FieldIDs...)
		sort.Strings(fieldIDs)
		columns := make([]map[string]string, 0, len(fieldIDs))
		for _, fieldID := range fieldIDs {
			columns = append(columns, map[string]string{
				"field_id": fieldID,
				"value":    fmt.Sprintf("workload-%d-%06d", parameters.Seed, ordinal+1),
			})
		}
		payload, err := json.Marshal(map[string]any{
			"authenticated_user_id": binding.UserID,
			"client_id":             binding.ClientID,
			"mutation_id":           queueNativeWorkloadUUID(parameters.Seed, target, batchOrdinal, ordinalInBatch),
			"table_id":              target.TableID,
			"pk": map[string]string{
				target.PrimaryKeyFieldID: fmt.Sprintf("workload-%d-%s-%06d", parameters.Seed, target.ScopeID, ordinal+1),
			},
			"authored_schema": map[string]any{
				"version": parameters.AuthoredSchema.Version,
				"hash":    parameters.AuthoredSchema.Hash,
			},
			"operation":      kind.Operation,
			"client_version": parameters.ClientVersion,
			"columns":        columns,
		})
		if err != nil {
			return nil, fmt.Errorf("step %s generated local/write %d payload: %w", step.ID, ordinal+1, err)
		}
		operation := scenarios.Operation{ContractOperation: "local", Name: "write", Payload: payload}
		if err := scenarios.ValidateOperation(operation); err != nil {
			return nil, fmt.Errorf("step %s generated local/write %d: %w", step.ID, ordinal+1, err)
		}
		operations = append(operations, operation)
	}
	encoded, err := json.Marshal(operations)
	if err != nil {
		return nil, fmt.Errorf("step %s encode generated operations: %w", step.ID, err)
	}
	digest := sha256.Sum256(encoded)
	actualDigest := hex.EncodeToString(digest[:])
	if parameters.Expectation.OperationDigest != actualDigest {
		return nil, fmt.Errorf("step %s generated operation digest %s does not match expectation", step.ID, actualDigest)
	}
	return operations, nil
}

func queueNativeWorkloadUUID(seed uint64, target scenarios.NativeWorkloadTarget, batchOrdinal, ordinalInBatch uint64) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("synchro:native-workload:v1:%d:%s:%s:%d:%d", seed, target.ScopeID, target.TableID, batchOrdinal, ordinalInBatch)))
	digest[6] = digest[6]&0x0f | 0x40
	digest[8] = digest[8]&0x3f | 0x80
	encoded := hex.EncodeToString(digest[:16])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}

func queueMutationFields(table queueSchemaTable) (queueSchemaField, queueSchemaField, error) {
	fields := make([]queueSchemaField, 0, len(table.Fields))
	for _, field := range table.Fields {
		if !field.PrimaryKey && field.Writable && field.Type == "string" {
			fields = append(fields, field)
		}
	}
	sort.Slice(fields, func(left, right int) bool { return fields[left].FieldID < fields[right].FieldID })
	if len(fields) < 2 {
		return queueSchemaField{}, queueSchemaField{}, errors.New("Swift queue-replay table has fewer than two writable string fields")
	}
	return fields[0], fields[1], nil
}

func queueNextSchema(current queueSchema, removedField string, version uint64) (queueSchema, scenarios.Operation, error) {
	if len(current.Tables) != 1 {
		return queueSchema{}, scenarios.Operation{}, errors.New("Swift queue-replay schema has an unexpected table count")
	}
	table := current.Tables[0]
	addedField := "queue_value_" + strconv.FormatUint(version, 10)
	for _, field := range table.Fields {
		if field.FieldID == addedField {
			return queueSchema{}, scenarios.Operation{}, fmt.Errorf("Swift queue-replay field %s already exists", addedField)
		}
	}
	updatedFields := make([]queueSchemaField, 0, len(table.Fields))
	for _, field := range table.Fields {
		if field.FieldID != removedField {
			updatedFields = append(updatedFields, field)
		}
	}
	updatedFields = append(updatedFields, queueSchemaField{FieldID: addedField, Name: addedField, Type: "string", Nullable: false, Writable: true, DefaultWireJSON: json.RawMessage(`""`)})
	table.Fields = updatedFields
	bodyTables := []map[string]any{queueSchemaManifestTable(table)}
	bodyWithoutHash := queueJSON(map[string]any{
		"parent_schema":       map[string]any{"version": current.Version, "hash": current.Hash},
		"schema_version":      version,
		"transition_class":    "class_4",
		"compatibility_floor": version,
		"tables":              bodyTables,
	})
	canonical, err := jcs.Transform(bodyWithoutHash)
	if err != nil {
		return queueSchema{}, scenarios.Operation{}, fmt.Errorf("canonicalize Swift queue-replay schema: %w", err)
	}
	digest := sha256.Sum256(append([]byte("synchro:v3:schema-manifest:v1\x00"), canonical...))
	hash := hex.EncodeToString(digest[:])
	bodyObject := map[string]any{
		"parent_schema":       map[string]any{"version": current.Version, "hash": current.Hash},
		"schema_version":      version,
		"schema_hash":         hash,
		"transition_class":    "class_4",
		"compatibility_floor": version,
		"tables":              bodyTables,
	}
	body, err := jcs.Transform(queueJSON(bodyObject))
	if err != nil {
		return queueSchema{}, scenarios.Operation{}, fmt.Errorf("encode Swift queue-replay schema: %w", err)
	}
	updated := queueSchema{Version: version, Hash: hash, Tables: []queueSchemaTable{table}}
	payload := map[string]any{
		"schema":              map[string]any{"version": version, "hash": hash},
		"body":                string(body),
		"transition_class":    "class_4",
		"compatibility_floor": version,
		"tables":              []map[string]any{queueSchemaProtocolTable(table)},
		"affected_scopes":     []string{},
	}
	operation := scenarios.Operation{ContractOperation: "model", Name: "publish-schema", Payload: queueJSON(payload)}
	if err := scenarios.ValidateOperation(operation); err != nil {
		return queueSchema{}, scenarios.Operation{}, fmt.Errorf("validate Swift queue-replay schema publication: %w", err)
	}
	return updated, operation, nil
}

func queueSchemaProtocolTable(table queueSchemaTable) map[string]any {
	return map[string]any{
		"table_id":             table.TableID,
		"relation_id":          table.RelationID,
		"name":                 table.Name,
		"composition":          table.Composition,
		"primary_key_field_id": table.PrimaryKeyFieldID,
		"created_at_field_id":  table.CreatedAtFieldID,
		"updated_at_field_id":  table.UpdatedAtFieldID,
		"deleted_at_field_id":  table.DeletedAtFieldID,
		"fields":               queueSchemaProtocolFields(table.Fields),
		"indexes":              queueSchemaIndexes(table.Indexes),
	}
}

func queueSchemaManifestTable(table queueSchemaTable) map[string]any {
	fields := make([]map[string]any, 0, len(table.Fields))
	for _, field := range table.Fields {
		fields = append(fields, map[string]any{
			"field_id": field.FieldID,
			"name":     field.Name,
			"type":     field.Type,
			"nullable": field.Nullable,
			"writable": field.Writable,
		})
	}
	sort.Slice(fields, func(left, right int) bool {
		return fields[left]["field_id"].(string) < fields[right]["field_id"].(string)
	})
	indexes := queueSchemaIndexes(table.Indexes)
	sort.Slice(indexes, func(left, right int) bool {
		return indexes[left]["index_id"].(string) < indexes[right]["index_id"].(string)
	})
	return map[string]any{
		"table_id":             table.TableID,
		"relation_id":          table.RelationID,
		"name":                 table.Name,
		"composition":          table.Composition,
		"primary_key_field_id": table.PrimaryKeyFieldID,
		"lifecycle": map[string]any{
			"created_at_field_id": table.CreatedAtFieldID,
			"updated_at_field_id": table.UpdatedAtFieldID,
			"deleted_at_field_id": table.DeletedAtFieldID,
		},
		"fields":  fields,
		"indexes": indexes,
	}
}

func queueSchemaProtocolFields(values []queueSchemaField) []map[string]any {
	result := make([]map[string]any, 0, len(values))
	for _, field := range values {
		var defaultValue any
		if len(field.DefaultWireJSON) != 0 && string(field.DefaultWireJSON) != "null" {
			_ = json.Unmarshal(field.DefaultWireJSON, &defaultValue)
		}
		result = append(result, map[string]any{
			"field_id":          field.FieldID,
			"name":              field.Name,
			"type":              field.Type,
			"primary_key":       field.PrimaryKey,
			"nullable":          field.Nullable,
			"writable":          field.Writable,
			"decimal_precision": field.DecimalPrecision,
			"decimal_scale":     field.DecimalScale,
			"default_wire_json": defaultValue,
		})
	}
	return result
}

func queueSchemaIndexes(values []queueSchemaIndex) []map[string]any {
	result := make([]map[string]any, 0, len(values))
	for _, index := range values {
		result = append(result, map[string]any{"index_id": index.IndexID, "name": index.Name, "field_ids": index.FieldIDs, "unique": index.Unique})
	}
	return result
}

func queueResponseLossOperation(client Client, batchID string) (scenarios.Operation, error) {
	operation := scenarios.Operation{ContractOperation: "process", Name: "response-loss", Payload: queueJSON(map[string]any{"authenticated_user_id": client.UserID, "client_id": client.ClientID, "batch_id": batchID})}
	if err := scenarios.ValidateOperation(operation); err != nil {
		return scenarios.Operation{}, fmt.Errorf("validate Swift queue-replay response loss: %w", err)
	}
	return operation, nil
}

func queueWorkloadUUID(kind, userID, clientID string, schemaVersion, ordinal uint64) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("synchro:workload:%s:%s:%s:%d:%d", kind, userID, clientID, schemaVersion, ordinal)))
	digest[6] = digest[6]&0x0f | 0x40
	digest[8] = digest[8]&0x3f | 0x80
	encoded := hex.EncodeToString(digest[:16])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}

func queueJSON(value any) json.RawMessage {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return encoded
}
