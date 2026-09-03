package kotlin

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

// QueueReplayResult records direct Kotlin Android evidence for queue-replay.
type QueueReplayResult struct {
	ReplayCalls []SynchronizationResult
	ClientFacts []CaptureFacts
	ServerFacts scenarios.StateFacts
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

type queueOutcomeCounts struct {
	Total    uint64
	Rejected uint64
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
	current, err := queueInitialSchema(scenario.Model.Setup[0])
	if err != nil {
		return QueueReplayResult{}, err
	}
	nextCommitLSN := uint64(1)
	replayCalls := make([]SynchronizationResult, 0, len(scenario.Steps))
	priorOutcomes := queueOutcomeCounts{}
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
				return QueueReplayResult{}, fmt.Errorf("bind Kotlin Android queue-replay local write %d for step %s: %w", ordinal+1, stepID, err)
			}
			observation, applyErr := platform.ApplyStep(ctx, client, operation)
			if applyErr != nil || observation.Disposition != "success" {
				return QueueReplayResult{}, fmt.Errorf("apply Kotlin Android queue-replay local write %d for step %s: %w", ordinal+1, stepID, kotlinResultError(applyErr, observation.Disposition))
			}
		}
		restart := scenarios.Operation{ContractOperation: "process", Name: "restart-client", Payload: queueJSON(map[string]any{"user_id": client.UserID, "client_id": client.ClientID})}
		if err := scenarios.ValidateOperation(restart); err != nil {
			return QueueReplayResult{}, fmt.Errorf("validate Kotlin Android queue-replay restart: %w", err)
		}
		if observation, err := platform.ProcessStep(ctx, client, restart); err != nil || observation.Disposition != "success" {
			return QueueReplayResult{}, fmt.Errorf("restart Kotlin Android queue-replay client for step %s: %w", stepID, kotlinResultError(err, observation.Disposition))
		}
		if observation, err := controller.ApplyStep(ctx, publish); err != nil || observation.Disposition != "success" {
			return QueueReplayResult{}, fmt.Errorf("publish Kotlin Android queue-replay schema for step %s: %w", stepID, kotlinResultError(err, observation.Disposition))
		}
		if err := queueRequireSchemaReset(ctx, platform, client, stepID); err != nil {
			return QueueReplayResult{}, err
		}
		loss, err := queueResponseLossOperation(client, batchID)
		if err != nil {
			return QueueReplayResult{}, err
		}
		lost, err := platform.Synchronize(ctx, SynchronizeRequest{Client: client, Method: "reset-schema-and-start", Operations: []scenarios.Operation{dropPush}})
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
		current = next
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
	return QueueReplayResult{ReplayCalls: replayCalls, ClientFacts: clientFacts, ServerFacts: serverCaptures[0].StateFacts}, nil
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
	var expected queueWorkloadPayload
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

func queueInitialSchema(operation scenarios.Operation) (queueSchema, error) {
	var payload queueSetupPayload
	if err := json.Unmarshal(operation.Payload, &payload); err != nil || payload.InitialSchema.Schema.Version == 0 || payload.InitialSchema.Schema.Hash == "" || len(payload.InitialSchema.Tables) != 1 {
		return queueSchema{}, errors.New("Kotlin Android queue-replay initial schema is invalid")
	}
	return queueSchema{Version: payload.InitialSchema.Schema.Version, Hash: payload.InitialSchema.Schema.Hash, Tables: payload.InitialSchema.Tables}, nil
}

func queueWorkloadOperations(step scenarios.Step, current queueSchema, commitLSN uint64) ([]scenarios.Operation, scenarios.Operation, scenarios.Operation, string, queueSchema, error) {
	binding := step.NativeBinding
	if binding == nil || binding.Workload == nil {
		return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, errors.New("Kotlin Android queue-replay workload is absent")
	}
	if binding.Workload.AuthoredSchema.Version != current.Version || binding.Workload.AuthoredSchema.Hash != current.Hash {
		return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("Kotlin Android queue-replay step %s schema does not match the current schema", step.ID)
	}
	var payload queueWorkloadPayload
	if err := json.Unmarshal(step.Operation.Payload, &payload); err != nil || payload.Profile != "pending_mutations" || payload.UserID != binding.UserID || payload.ClientID != binding.ClientID || payload.TableID == "" || payload.RejectedCount != 1 || payload.AcceptedCount != binding.Workload.RecordCount-1 {
		return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("Kotlin Android queue-replay step %s workload payload is invalid", step.ID)
	}
	if binding.Workload.RecordCount == 0 || binding.Workload.BatchSize == 0 || len(current.Tables) != 1 || len(binding.Workload.Targets) != 1 {
		return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("Kotlin Android queue-replay step %s workload parameters are invalid", step.ID)
	}
	table := &current.Tables[0]
	if table.TableID != payload.TableID || table.PrimaryKeyFieldID != binding.Workload.Targets[0].PrimaryKeyFieldID || binding.Workload.Targets[0].TableID != table.TableID {
		return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("Kotlin Android queue-replay step %s workload target is invalid", step.ID)
	}
	rejectionField, acceptedField, err := queueMutationFields(*table)
	if err != nil {
		return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, err
	}
	local, err := queueExpandWorkload(step)
	if err != nil {
		return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("expand Kotlin Android queue-replay workload %s: %w", step.ID, err)
	}
	if uint64(len(local)) != binding.Workload.RecordCount {
		return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("Kotlin Android queue-replay step %s expanded to %d operations, want %d", step.ID, len(local), binding.Workload.RecordCount)
	}
	wire := make([]map[string]any, 0, len(local))
	for ordinal, operation := range local {
		var value queueLocalPayload
		if err := json.Unmarshal(operation.Payload, &value); err != nil || value.AuthenticatedUserID != binding.UserID || value.ClientID != binding.ClientID || value.TableID != table.TableID || value.MutationID == "" || value.Operation != "insert" || value.ClientVersion != binding.Workload.ClientVersion || value.AuthoredSchema.Version != current.Version || value.AuthoredSchema.Hash != current.Hash || len(value.PK) != 1 || value.PK[table.PrimaryKeyFieldID] == "" {
			return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("Kotlin Android queue-replay local write %d has invalid authored identity", ordinal+1)
		}
		columns := make(map[string]any, len(value.Columns))
		for _, column := range value.Columns {
			if column.FieldID == "" || column.FieldID == table.PrimaryKeyFieldID {
				return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("Kotlin Android queue-replay local write %d has an invalid field", ordinal+1)
			}
			if _, duplicate := columns[column.FieldID]; duplicate {
				return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("Kotlin Android queue-replay local write %d repeats a field", ordinal+1)
			}
			columns[column.FieldID] = column.Value
		}
		if len(columns) == 0 || ordinal+1 < len(local) && len(columns) != 1 || ordinal+1 == len(local) && len(columns) != 2 {
			return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("Kotlin Android queue-replay local write %d has the wrong field count", ordinal+1)
		}
		if _, ok := columns[acceptedField.FieldID]; !ok {
			return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("Kotlin Android queue-replay local write %d lacks the accepted field", ordinal+1)
		}
		if ordinal+1 < len(local) {
			if _, rejected := columns[rejectionField.FieldID]; rejected {
				return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("Kotlin Android queue-replay local write %d has the rejection field", ordinal+1)
			}
		} else if _, rejected := columns[rejectionField.FieldID]; !rejected {
			return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, errors.New("Kotlin Android queue-replay terminal local write lacks the rejection field")
		}
		wire = append(wire, map[string]any{"mutation_id": value.MutationID, "table": value.TableID, "pk": value.PK, "authored_schema": map[string]any{"version": value.AuthoredSchema.Version, "hash": value.AuthoredSchema.Hash}, "op": value.Operation, "client_version": value.ClientVersion, "columns": columns})
	}
	next, publish, err := queueNextSchema(current, rejectionField.FieldID, current.Version+1)
	if err != nil {
		return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, err
	}
	batchID := queueWorkloadUUID("batch", binding.UserID, binding.ClientID, current.Version, binding.Workload.RecordCount)
	pushPayload := map[string]any{
		"authenticated_user_id": binding.UserID,
		"request": map[string]any{
			"client_id": binding.ClientID, "client_generation": 1, "batch_id": batchID,
			"schema": map[string]any{"version": next.Version, "hash": next.Hash}, "mutations": wire,
		},
		"delivery": "drop_after_server", "commit_lsn": strconv.FormatUint(commitLSN, 10), "end_lsn": strconv.FormatUint(commitLSN+1, 10),
	}
	dropPush := scenarios.Operation{ContractOperation: "push", Name: "submit", Payload: queueJSON(pushPayload)}
	if err := scenarios.ValidateOperation(dropPush); err != nil {
		return nil, scenarios.Operation{}, scenarios.Operation{}, "", queueSchema{}, fmt.Errorf("validate Kotlin Android queue-replay push: %w", err)
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
			columns = append(columns, map[string]string{"field_id": fieldID, "value": fmt.Sprintf("workload-%d-%06d", parameters.Seed, ordinal+1)})
		}
		payload, err := json.Marshal(map[string]any{
			"authenticated_user_id": binding.UserID, "client_id": binding.ClientID,
			"mutation_id": queueNativeWorkloadUUID(parameters.Seed, target, batchOrdinal, ordinalInBatch), "table_id": target.TableID,
			"pk":              map[string]string{target.PrimaryKeyFieldID: fmt.Sprintf("workload-%d-%s-%06d", parameters.Seed, target.ScopeID, ordinal+1)},
			"authored_schema": map[string]any{"version": parameters.AuthoredSchema.Version, "hash": parameters.AuthoredSchema.Hash},
			"operation":       kind.Operation, "client_version": parameters.ClientVersion, "columns": columns,
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
	if parameters.Expectation.OperationDigest != hex.EncodeToString(digest[:]) {
		return nil, fmt.Errorf("step %s generated operation digest does not match expectation", step.ID)
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
		return queueSchemaField{}, queueSchemaField{}, errors.New("Kotlin Android queue-replay table has fewer than two writable string fields")
	}
	return fields[0], fields[1], nil
}

func queueNextSchema(current queueSchema, removedField string, version uint64) (queueSchema, scenarios.Operation, error) {
	if len(current.Tables) != 1 {
		return queueSchema{}, scenarios.Operation{}, errors.New("Kotlin Android queue-replay schema has an unexpected table count")
	}
	table := current.Tables[0]
	addedField := "queue_value_" + strconv.FormatUint(version, 10)
	for _, field := range table.Fields {
		if field.FieldID == addedField {
			return queueSchema{}, scenarios.Operation{}, fmt.Errorf("Kotlin Android queue-replay field %s already exists", addedField)
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
	bodyWithoutHash := queueJSON(map[string]any{"parent_schema": map[string]any{"version": current.Version, "hash": current.Hash}, "schema_version": version, "transition_class": "class_4", "compatibility_floor": version, "tables": bodyTables})
	canonical, err := jcs.Transform(bodyWithoutHash)
	if err != nil {
		return queueSchema{}, scenarios.Operation{}, fmt.Errorf("canonicalize Kotlin Android queue-replay schema: %w", err)
	}
	digest := sha256.Sum256(append([]byte("synchro:v3:schema-manifest:v1\x00"), canonical...))
	hash := hex.EncodeToString(digest[:])
	bodyObject := map[string]any{"parent_schema": map[string]any{"version": current.Version, "hash": current.Hash}, "schema_version": version, "schema_hash": hash, "transition_class": "class_4", "compatibility_floor": version, "tables": bodyTables}
	body, err := jcs.Transform(queueJSON(bodyObject))
	if err != nil {
		return queueSchema{}, scenarios.Operation{}, fmt.Errorf("encode Kotlin Android queue-replay schema: %w", err)
	}
	updated := queueSchema{Version: version, Hash: hash, Tables: []queueSchemaTable{table}}
	payload := map[string]any{"schema": map[string]any{"version": version, "hash": hash}, "body": string(body), "transition_class": "class_4", "compatibility_floor": version, "tables": []map[string]any{queueSchemaProtocolTable(table)}, "affected_scopes": []string{}}
	operation := scenarios.Operation{ContractOperation: "model", Name: "publish-schema", Payload: queueJSON(payload)}
	if err := scenarios.ValidateOperation(operation); err != nil {
		return queueSchema{}, scenarios.Operation{}, fmt.Errorf("validate Kotlin Android queue-replay schema publication: %w", err)
	}
	return updated, operation, nil
}

func queueSchemaProtocolTable(table queueSchemaTable) map[string]any {
	return map[string]any{"table_id": table.TableID, "relation_id": table.RelationID, "name": table.Name, "composition": table.Composition, "primary_key_field_id": table.PrimaryKeyFieldID, "created_at_field_id": table.CreatedAtFieldID, "updated_at_field_id": table.UpdatedAtFieldID, "deleted_at_field_id": table.DeletedAtFieldID, "fields": queueSchemaProtocolFields(table.Fields), "indexes": queueSchemaIndexes(table.Indexes)}
}

func queueSchemaManifestTable(table queueSchemaTable) map[string]any {
	fields := make([]map[string]any, 0, len(table.Fields))
	for _, field := range table.Fields {
		fields = append(fields, map[string]any{"field_id": field.FieldID, "name": field.Name, "type": field.Type, "nullable": field.Nullable, "writable": field.Writable})
	}
	sort.Slice(fields, func(left, right int) bool {
		return fields[left]["field_id"].(string) < fields[right]["field_id"].(string)
	})
	indexes := queueSchemaIndexes(table.Indexes)
	sort.Slice(indexes, func(left, right int) bool {
		return indexes[left]["index_id"].(string) < indexes[right]["index_id"].(string)
	})
	return map[string]any{"table_id": table.TableID, "relation_id": table.RelationID, "name": table.Name, "composition": table.Composition, "primary_key_field_id": table.PrimaryKeyFieldID, "lifecycle": map[string]any{"created_at_field_id": table.CreatedAtFieldID, "updated_at_field_id": table.UpdatedAtFieldID, "deleted_at_field_id": table.DeletedAtFieldID}, "fields": fields, "indexes": indexes}
}

func queueSchemaProtocolFields(values []queueSchemaField) []map[string]any {
	result := make([]map[string]any, 0, len(values))
	for _, field := range values {
		var defaultValue any
		if len(field.DefaultWireJSON) != 0 && string(field.DefaultWireJSON) != "null" {
			_ = json.Unmarshal(field.DefaultWireJSON, &defaultValue)
		}
		result = append(result, map[string]any{"field_id": field.FieldID, "name": field.Name, "type": field.Type, "primary_key": field.PrimaryKey, "nullable": field.Nullable, "writable": field.Writable, "decimal_precision": field.DecimalPrecision, "decimal_scale": field.DecimalScale, "default_wire_json": defaultValue})
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
		return scenarios.Operation{}, fmt.Errorf("validate Kotlin Android queue-replay response loss: %w", err)
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
