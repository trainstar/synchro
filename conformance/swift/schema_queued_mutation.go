package swift

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const schemaQueuedMutationScenarioID = "SCN-SCHEMA-QUEUED-MUTATION-001"

// SchemaQueuedMutationResult records direct Swift evidence for one blocked schema-incompatible mutation.
type SchemaQueuedMutationResult struct {
	BaselineCall       SynchronizationResult
	UnsupportedCall    SynchronizationResult
	ResetCall          SynchronizationResult
	ClientFacts        []CaptureFacts
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

type schemaQueuedMutationRebuildPayload struct {
	Limit uint64 `json:"limit"`
}

type schemaQueuedMutationPushPayload struct {
	AuthenticatedUserID string `json:"authenticated_user_id"`
	Request             struct {
		ClientID  string `json:"client_id"`
		BatchID   string `json:"batch_id"`
		Mutations []struct {
			MutationID string `json:"mutation_id"`
		} `json:"mutations"`
	} `json:"request"`
}

// RunSchemaQueuedMutationScenario executes the authored durable blocked-mutation flow through Swift.
func RunSchemaQueuedMutationScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, platform *Platform, client Client) (SchemaQueuedMutationResult, error) {
	steps, err := swiftScenarioStepMap(scenario, schemaQueuedMutationScenarioID, 16)
	if err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	if controller == nil || platform == nil {
		return SchemaQueuedMutationResult{}, errors.New("Swift schema-queued-mutation dependencies are unavailable")
	}
	if err := validateSchemaQueuedMutationBindings(scenario, steps, client); err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	expected, err := swiftScenarioExpectedState(scenario, "EXPECT-SCHEMA-QUEUED-MUTATION-STATE-001")
	if err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("install Swift schema-queued-mutation contract: %w", err)
	}
	// The scenario authors its own baseline rebuild, so the client starts empty
	// and performs that rebuild in the authored call. A current initialization
	// would bootstrap the rebuild during setup instead.
	if err := platform.Install(ctx, client, "empty", ""); err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("install Swift schema-queued-mutation client: %w", err)
	}

	commit, _ := swiftScenarioOperation(steps, "STEP-SCHEMA-QUEUED-MUTATION-001", "model/commit-source-transaction")
	if observation, applyErr := controller.ApplyStep(ctx, commit); applyErr != nil || observation.Disposition != "success" {
		return SchemaQueuedMutationResult{}, fmt.Errorf("commit Swift schema-queued-mutation baseline: %w", resultError(applyErr, observation.Disposition))
	}
	materialize, _ := swiftScenarioOperation(steps, "STEP-SCHEMA-QUEUED-MUTATION-002", "process/materialize-source-transaction")
	if observation, processErr := controller.ProcessStep(ctx, nil, materialize); processErr != nil || observation.Disposition != "success" {
		return SchemaQueuedMutationResult{}, fmt.Errorf("materialize Swift schema-queued-mutation baseline: %w", resultError(processErr, observation.Disposition))
	}

	baseline, err := swiftScenarioCall(ctx, platform, client, steps["STEP-SCHEMA-QUEUED-MUTATION-003"].NativeBinding.Method)
	if err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("run Swift schema-queued-mutation baseline: %w", err)
	}
	if err := validateSchemaQueuedMutationBaseline(scenario, steps, baseline); err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	// The compatible write replaces the S1 row version and checksum, so bind
	// the S1 aliases from this capture.
	baselineFacts, err := platform.Capture(ctx, []Client{client}, []string{"checkpoints", "provenance"})
	if err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("capture Swift schema-queued-mutation baseline state: %w", err)
	}
	baselineState, err := mergeSwiftCaptureFacts(baselineFacts)
	if err != nil {
		return SchemaQueuedMutationResult{}, err
	}

	compatibleWrite, err := applySwiftSchemaQueuedMutationWrite(ctx, controller, platform, client, steps, "STEP-SCHEMA-QUEUED-MUTATION-COMPATIBLE-WRITE-001")
	if err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	compatiblePublish, _ := swiftScenarioOperation(steps, "STEP-SCHEMA-QUEUED-MUTATION-COMPATIBLE-PUBLISH-001", "model/publish-schema")
	if observation, applyErr := controller.ApplyStep(ctx, compatiblePublish); applyErr != nil || observation.Disposition != "success" {
		return SchemaQueuedMutationResult{}, fmt.Errorf("publish Swift schema-queued-mutation compatible schema: %w", resultError(applyErr, observation.Disposition))
	}
	compatiblePush, _ := swiftScenarioOperation(steps, "STEP-SCHEMA-QUEUED-MUTATION-COMPATIBLE-PUSH-001", "push/submit")
	if err := controller.BindApplicationPush(compatiblePush); err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("bind Swift schema-queued-mutation compatible push: %w", err)
	}
	// The baseline call left the engine started, and it rejects a second start.
	if _, err := platform.Lifecycle(ctx, client, "stop"); err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("stop Swift schema-queued-mutation client before its compatible start: %w", err)
	}
	compatible, err := swiftScenarioCall(ctx, platform, client, steps["STEP-SCHEMA-QUEUED-MUTATION-COMPATIBLE-CONNECT-001"].NativeBinding.Method)
	if err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("run Swift schema-queued-mutation compatible start: %w", err)
	}
	if err := validateSchemaQueuedMutationPushCall(scenario, "STEP-SCHEMA-QUEUED-MUTATION-COMPATIBLE-CONNECT-001", "STEP-SCHEMA-QUEUED-MUTATION-COMPATIBLE-PUSH-001", compatible); err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	// The server capture binds the accepted push and checks the exact server
	// row it wrote. The local row keeps that value through the migration.
	if _, err := controller.Capture(ctx, []string{client.Key}, []string{"server-state"}); err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("capture Swift schema-queued-mutation compatible server state: %w", err)
	}
	if err := requireSwiftSchemaQueuedMutationRow(ctx, platform, client, compatibleWrite); err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("Swift schema-queued-mutation compatible row: %w", err)
	}

	// The S2 write names the field the compatible schema added. The local row
	// must hold it before S3 removes that field.
	write, err := applySwiftSchemaQueuedMutationWrite(ctx, controller, platform, client, steps, "STEP-SCHEMA-QUEUED-MUTATION-005")
	if err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	if err := requireSwiftSchemaQueuedMutationRow(ctx, platform, client, write); err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("Swift schema-queued-mutation compatible field: %w", err)
	}

	publish, _ := swiftScenarioOperation(steps, "STEP-SCHEMA-QUEUED-MUTATION-006", "model/publish-schema")
	if observation, applyErr := controller.ApplyStep(ctx, publish); applyErr != nil || observation.Disposition != "success" {
		return SchemaQueuedMutationResult{}, fmt.Errorf("publish Swift schema-queued-mutation schema: %w", resultError(applyErr, observation.Disposition))
	}

	// The compatible call left the engine started, and it rejects a second
	// start. The authored step expects a real connect that the server answers
	// with an unsupported action, so the client stops before it starts again.
	if _, err := platform.Lifecycle(ctx, client, "stop"); err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("stop Swift schema-queued-mutation client before its unsupported start: %w", err)
	}
	unsupported, err := swiftScenarioCall(ctx, platform, client, steps["STEP-SCHEMA-QUEUED-MUTATION-UNSUPPORTED-001"].NativeBinding.Method)
	if err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("observe Swift schema-queued-mutation unsupported schema: %w", err)
	}
	if err := validateSchemaQueuedMutationCall(scenario, "STEP-SCHEMA-QUEUED-MUTATION-UNSUPPORTED-001", "connect", unsupported); err != nil {
		return SchemaQueuedMutationResult{}, err
	}

	reset, err := swiftScenarioCall(ctx, platform, client, steps["STEP-SCHEMA-QUEUED-MUTATION-007"].NativeBinding.Method)
	if err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("run Swift schema-queued-mutation reset: %w", err)
	}
	if err := validateSchemaQueuedMutationPushCall(scenario, "STEP-SCHEMA-QUEUED-MUTATION-007", "STEP-SCHEMA-QUEUED-MUTATION-008", reset); err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	keptWrite, err := schemaQueuedMutationKeptWrite(steps["STEP-SCHEMA-QUEUED-MUTATION-005"].Operation, publish)
	if err == nil {
		keptWrite, err = controller.ApplicationWrite(keptWrite)
	}
	if err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("bind Swift schema-queued-mutation kept write: %w", err)
	}
	if err := requireSwiftSchemaQueuedMutationRow(ctx, platform, client, keptWrite); err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("Swift schema-queued-mutation row after reset: %w", err)
	}
	push, _ := swiftScenarioOperation(steps, "STEP-SCHEMA-QUEUED-MUTATION-008", "push/submit")
	if err := controller.BindApplicationPush(push); err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("bind Swift schema-queued-mutation push: %w", err)
	}

	restart, _ := swiftScenarioOperation(steps, "STEP-SCHEMA-QUEUED-MUTATION-009", "process/restart-client")
	if observation, processErr := platform.ProcessStep(ctx, client, restart); processErr != nil || observation.Disposition != "success" {
		return SchemaQueuedMutationResult{}, fmt.Errorf("restart Swift schema-queued-mutation client: %w", resultError(processErr, observation.Disposition))
	}

	clientFacts, err := platform.Capture(ctx, []Client{client}, []string{"application-rows", "pending-mutations", "rejected-mutations", "checkpoints", "provenance"})
	if err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("capture Swift schema-queued-mutation client state: %w", err)
	}
	clientState, err := mergeSwiftCaptureFacts(clientFacts)
	if err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	if err := requireSwiftSchemaQueuedMutationRow(ctx, platform, client, keptWrite); err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("Swift schema-queued-mutation row after restart: %w", err)
	}
	serverCaptures, err := controller.Capture(ctx, []string{client.Key}, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return SchemaQueuedMutationResult{}, fmt.Errorf("capture Swift schema-queued-mutation server state: %w", err)
	}
	// The authored schema hash is a corpus value. The client observes the
	// hash the server published, so compare the authored client state against
	// the runtime schema each authored alias resolves to.
	runtimeExpected, err := swiftSchemaQueuedMutationRuntimeState(controller, scenario.NativeIdentityAliases, expected)
	if err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	// The queue records identities the client generates. The projection
	// compares declared values, so it cannot compare them. Compare the queue
	// through the aliases the scenario declares instead.
	if err := validateSwiftStateProjection(swiftStateFactsWithoutGeneratedIdentities(runtimeExpected), swiftStateFactsWithoutGeneratedIdentities(clientState)); err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("Swift schema-queued-mutation client state differs from the authored model: %w", err)
	}
	if err := validateSchemaQueuedMutationQueue(controller, scenario.NativeIdentityAliases, expected, clientState); err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	identities, err := resolveSchemaQueuedMutationIdentities(controller, scenario.NativeIdentityAliases, baseline, reset, baselineState, clientState, serverCaptures[0].StateFacts)
	if err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	return SchemaQueuedMutationResult{
		BaselineCall:       baseline,
		UnsupportedCall:    unsupported,
		ResetCall:          reset,
		ClientFacts:        clientFacts,
		ServerFacts:        serverCaptures[0].StateFacts,
		IdentityResolution: identities,
	}, nil
}

func validateSchemaQueuedMutationBindings(scenario scenarios.Scenario, steps map[scenarios.StepID]scenarios.Step, client Client) error {
	wanted := []struct {
		id, key, kind, method string
	}{
		{"STEP-SCHEMA-QUEUED-MUTATION-001", "model/commit-source-transaction", "controller", ""},
		{"STEP-SCHEMA-QUEUED-MUTATION-002", "process/materialize-source-transaction", "controller", ""},
		{"STEP-SCHEMA-QUEUED-MUTATION-003", "rebuild/request-page", "public-call", "start"},
		{"STEP-SCHEMA-QUEUED-MUTATION-BASELINE-BEGIN-001", "local/begin-rebuild", "public-call", "start"},
		{"STEP-SCHEMA-QUEUED-MUTATION-004", "local/apply-rebuild-page", "public-call", "start"},
		{"STEP-SCHEMA-QUEUED-MUTATION-BASELINE-FINALIZE-001", "local/finalize-rebuild", "public-call", "start"},
		{"STEP-SCHEMA-QUEUED-MUTATION-COMPATIBLE-WRITE-001", "local/write", "local-write", ""},
		{"STEP-SCHEMA-QUEUED-MUTATION-COMPATIBLE-PUBLISH-001", "model/publish-schema", "controller", ""},
		{"STEP-SCHEMA-QUEUED-MUTATION-COMPATIBLE-CONNECT-001", "connect/send", "public-call", "start"},
		{"STEP-SCHEMA-QUEUED-MUTATION-COMPATIBLE-PUSH-001", "push/submit", "public-call", "start"},
		{"STEP-SCHEMA-QUEUED-MUTATION-005", "local/write", "local-write", ""},
		{"STEP-SCHEMA-QUEUED-MUTATION-006", "model/publish-schema", "controller", ""},
		{"STEP-SCHEMA-QUEUED-MUTATION-UNSUPPORTED-001", "connect/send", "public-call", "start"},
		{"STEP-SCHEMA-QUEUED-MUTATION-007", "connect/send", "public-call", "reset-schema-and-start"},
		{"STEP-SCHEMA-QUEUED-MUTATION-008", "push/submit", "public-call", "reset-schema-and-start"},
		{"STEP-SCHEMA-QUEUED-MUTATION-009", "process/restart-client", "process", ""},
	}
	if len(steps) != len(wanted) {
		return errors.New("Swift schema-queued-mutation step bindings are incomplete")
	}
	callIDs := make(map[string]string)
	for _, expected := range wanted {
		step, found := steps[scenarios.StepID(expected.id)]
		if !found || scenarios.OperationKey(step.Operation) != expected.key || step.NativeBinding == nil || step.NativeBinding.Kind != expected.kind || step.NativeBinding.Method != expected.method || step.ExpectedOutcome.Disposition != "success" {
			return fmt.Errorf("Swift schema-queued-mutation binding %s is invalid", expected.id)
		}
		if expected.kind != "controller" {
			if err := swiftScenarioClient(step, client); err != nil {
				return err
			}
		}
		if expected.kind == "public-call" {
			if step.NativeBinding.CallID == nil || *step.NativeBinding.CallID == "" || step.NativeBinding.Stage != "synchronous" {
				return fmt.Errorf("Swift schema-queued-mutation public binding %s is invalid", expected.id)
			}
			group := "baseline"
			if expected.id == "STEP-SCHEMA-QUEUED-MUTATION-UNSUPPORTED-001" {
				group = "unsupported"
			} else if expected.id == "STEP-SCHEMA-QUEUED-MUTATION-COMPATIBLE-CONNECT-001" || expected.id == "STEP-SCHEMA-QUEUED-MUTATION-COMPATIBLE-PUSH-001" {
				group = "compatible"
			} else if expected.id == "STEP-SCHEMA-QUEUED-MUTATION-007" || expected.id == "STEP-SCHEMA-QUEUED-MUTATION-008" {
				group = "reset"
			}
			if prior, found := callIDs[group]; found && prior != string(*step.NativeBinding.CallID) {
				return fmt.Errorf("Swift schema-queued-mutation %s call bindings do not share one call identity", group)
			}
			callIDs[group] = string(*step.NativeBinding.CallID)
			completion, err := schemaQueuedMutationCompletion(scenario, step)
			if err != nil || step.NativeBinding.Completion != completion {
				return fmt.Errorf("Swift schema-queued-mutation completion %s is not derived from its authored outcome", expected.id)
			}
		}
	}
	distinct := make(map[string]struct{}, len(callIDs))
	for _, callID := range callIDs {
		distinct[callID] = struct{}{}
	}
	if len(callIDs) != 4 || len(distinct) != 4 {
		return errors.New("Swift schema-queued-mutation public call identities are invalid")
	}
	return nil
}

func validateSchemaQueuedMutationBaseline(scenario scenarios.Scenario, steps map[scenarios.StepID]scenarios.Step, result SynchronizationResult) error {
	if completion, err := schemaQueuedMutationCompletion(scenario, steps["STEP-SCHEMA-QUEUED-MUTATION-BASELINE-FINALIZE-001"]); err != nil || result.Completion != completion {
		return errors.New("Swift schema-queued-mutation baseline completion is invalid")
	}
	rebuild, err := swiftScenarioWire(result, "rebuild")
	if err != nil {
		return err
	}
	if err := validateSwiftWireExpectation(scenario, "STEP-SCHEMA-QUEUED-MUTATION-003", "rebuild", result); err != nil {
		return err
	}
	operation, _ := swiftScenarioOperation(steps, "STEP-SCHEMA-QUEUED-MUTATION-003", "rebuild/request-page")
	var payload schemaQueuedMutationRebuildPayload
	if json.Unmarshal(operation.Payload, &payload) != nil || payload.Limit == 0 || rebuild.RequestFacts == nil || rebuild.RequestFacts.Limit == nil || uint64(*rebuild.RequestFacts.Limit) != payload.Limit {
		return errors.New("Swift schema-queued-mutation rebuild limit differs from the authored request")
	}
	return nil
}

func validateSchemaQueuedMutationCall(scenario scenarios.Scenario, stepID, operationClass string, result SynchronizationResult) error {
	step, found := schemaQueuedMutationStep(scenario, stepID)
	if !found {
		return fmt.Errorf("Swift schema-queued-mutation step %s is absent", stepID)
	}
	completion, err := schemaQueuedMutationCompletion(scenario, step)
	if err != nil || result.Completion != completion {
		return fmt.Errorf("Swift schema-queued-mutation step %s completion differs from its authored outcome", stepID)
	}
	return validateSwiftWireExpectation(scenario, stepID, operationClass, result)
}

// validateSchemaQueuedMutationPushCall checks one start call that connects and
// then pushes the retained queue in one authored batch.
func validateSchemaQueuedMutationPushCall(scenario scenarios.Scenario, connectStepID, pushStepID string, result SynchronizationResult) error {
	step, found := schemaQueuedMutationStep(scenario, pushStepID)
	if !found {
		return fmt.Errorf("Swift schema-queued-mutation push step %s is absent", pushStepID)
	}
	completion, err := schemaQueuedMutationCompletion(scenario, step)
	if err != nil || result.Completion != completion {
		return fmt.Errorf("Swift schema-queued-mutation call %s completion differs from its authored terminal outcome", pushStepID)
	}
	if err := validateSwiftWireExpectation(scenario, connectStepID, "connect", result); err != nil {
		return err
	}
	if err := validateSwiftWireExpectation(scenario, pushStepID, "push", result); err != nil {
		return err
	}
	push, err := swiftScenarioWire(result, "push")
	if err != nil || push.RequestFacts == nil || push.RequestFacts.MutationCount == nil {
		return errors.New("Swift schema-queued-mutation push facts are incomplete")
	}
	var payload schemaQueuedMutationPushPayload
	if json.Unmarshal(step.Operation.Payload, &payload) != nil || payload.AuthenticatedUserID != step.NativeBinding.UserID || payload.Request.ClientID != step.NativeBinding.ClientID || payload.Request.BatchID == "" || len(payload.Request.Mutations) == 0 || int64(len(payload.Request.Mutations)) != int64(*push.RequestFacts.MutationCount) {
		return errors.New("Swift schema-queued-mutation push does not preserve the authored batch")
	}
	for _, mutation := range payload.Request.Mutations {
		if mutation.MutationID == "" {
			return errors.New("Swift schema-queued-mutation push mutation identity is absent")
		}
	}
	return nil
}

// applySwiftSchemaQueuedMutationWrite binds one authored local write to the
// runtime table and applies it through the client.
func applySwiftSchemaQueuedMutationWrite(ctx context.Context, controller *blackbox.NativeController, platform *Platform, client Client, steps map[scenarios.StepID]scenarios.Step, stepID string) (scenarios.Operation, error) {
	write, _ := swiftScenarioOperation(steps, stepID, "local/write")
	write, err := controller.ApplicationWrite(write)
	if err != nil {
		return scenarios.Operation{}, fmt.Errorf("bind Swift schema-queued-mutation local write %s: %w", stepID, err)
	}
	if observation, applyErr := platform.ApplyStep(ctx, client, write); applyErr != nil || observation.Disposition != "success" {
		return scenarios.Operation{}, fmt.Errorf("apply Swift schema-queued-mutation local write %s: %w", stepID, resultError(applyErr, observation.Disposition))
	}
	return write, nil
}

// schemaQueuedMutationKeptWrite keeps the columns of an authored local write
// that the authored target schema still declares. The unresolved S2 write
// changes one field that S3 removes and one field that S3 keeps with a value
// the server does not hold. Only a kept local row passes a check of the kept
// field. A server replacement of the row fails it (#267).
func schemaQueuedMutationKeptWrite(write, publish scenarios.Operation) (scenarios.Operation, error) {
	var target struct {
		Tables []struct {
			TableID string `json:"table_id"`
			Fields  []struct {
				FieldID string `json:"field_id"`
			} `json:"fields"`
		} `json:"tables"`
	}
	var payload map[string]json.RawMessage
	var columns []map[string]json.RawMessage
	var tableID string
	if json.Unmarshal(publish.Payload, &target) != nil || json.Unmarshal(write.Payload, &payload) != nil ||
		json.Unmarshal(payload["table_id"], &tableID) != nil || json.Unmarshal(payload["columns"], &columns) != nil {
		return scenarios.Operation{}, errors.New("schema-queued-mutation kept write is invalid")
	}
	declared := make(map[string]bool)
	for _, table := range target.Tables {
		if table.TableID == tableID {
			for _, field := range table.Fields {
				declared[field.FieldID] = true
			}
		}
	}
	kept := make([]map[string]json.RawMessage, 0, len(columns))
	for _, column := range columns {
		var fieldID string
		if json.Unmarshal(column["field_id"], &fieldID) == nil && declared[fieldID] {
			kept = append(kept, column)
		}
	}
	if len(kept) == 0 || len(kept) == len(columns) {
		return scenarios.Operation{}, fmt.Errorf("schema-queued-mutation write keeps %d of %d fields, want some but not all", len(kept), len(columns))
	}
	encoded, err := json.Marshal(kept)
	if err != nil {
		return scenarios.Operation{}, err
	}
	payload["columns"] = encoded
	write.Payload, err = json.Marshal(payload)
	return write, err
}

func requireSwiftSchemaQueuedMutationRow(ctx context.Context, platform *Platform, client Client, write scenarios.Operation) error {
	snapshot, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return err
	}
	return scenarios.RequireLocalWriteRow(write, snapshot.ApplicationRows)
}

func schemaQueuedMutationCompletion(scenario scenarios.Scenario, step scenarios.Step) (string, error) {
	for _, wire := range scenario.WireExpectations {
		if wire.StepID != step.ID {
			continue
		}
		if wire.Action == "unsupported" {
			return "error", nil
		}
		if wire.HTTPStatus >= 200 && wire.HTTPStatus < 300 {
			return "idle", nil
		}
		if wire.Retryable || wire.HTTPStatus == 0 {
			return "blocked", nil
		}
		return "error", nil
	}
	if step.ExpectedOutcome.Disposition == "error" {
		return "error", nil
	}
	return "idle", nil
}

func schemaQueuedMutationStep(scenario scenarios.Scenario, id string) (scenarios.Step, bool) {
	for _, step := range scenario.Steps {
		if step.ID == scenarios.StepID(id) {
			return step, true
		}
	}
	return scenarios.Step{}, false
}

// swiftSchemaQueuedMutationRuntimeState replaces each authored schema
// reference in the expected client state with the runtime schema its alias
// resolves to. The scenario declares both schema aliases for this
// expectation, so an authored hash never matches an observed hash directly.
// swiftStateFactsWithoutGeneratedIdentities drops the client queue and outcome
// families. Both record the mutation identity the client generates, which the
// declared value projection cannot compare.
func swiftStateFactsWithoutGeneratedIdentities(facts scenarios.StateFacts) scenarios.StateFacts {
	projected := scenarios.CloneStateFacts(facts)
	for index := range projected.Clients {
		projected.Clients[index].Queue = nil
		projected.Clients[index].Outcomes = nil
	}
	return projected
}

// validateSchemaQueuedMutationQueue compares the authored queue entry against
// the observed entry through the identities the scenario declares. The client
// generates the mutation identifier, the record identity, and the base
// version, and it generates one timestamp the scenario declares no alias for.
func validateSchemaQueuedMutationQueue(controller *blackbox.NativeController, aliases []scenarios.NativeIdentityAlias, expected, observed scenarios.StateFacts) error {
	if len(expected.Clients) != 1 || len(observed.Clients) != 1 {
		return errors.New("Swift schema-queued-mutation client state shape differs from the authored model")
	}
	want := expected.Clients[0].Queue
	got := observed.Clients[0].Queue
	if len(want) != len(got) {
		return fmt.Errorf("Swift schema-queued-mutation queue authored %d entries observed %d", len(want), len(got))
	}
	resolvable := make([]scenarios.NativeIdentityAlias, 0, len(aliases))
	for _, alias := range aliases {
		switch alias.Kind {
		case "table", "primary-key", "mutation-id", "schema":
			resolvable = append(resolvable, alias)
		}
	}
	values, err := controller.IdentityValues(resolvable)
	if err != nil {
		return fmt.Errorf("resolve Swift schema-queued-mutation queue identity: %w", err)
	}
	authoredByAlias := make(map[string]json.RawMessage, len(resolvable))
	for _, alias := range resolvable {
		authoredByAlias[alias.Alias] = alias.Value
	}
	// Two schema aliases share one kind, so key the schema map by its authored
	// value instead.
	runtimeSchemas := make(map[scenarios.SchemaFact]scenarios.SchemaFact, len(values))
	resolved := make(map[string]blackbox.NativeIdentityResolution, len(values))
	for _, value := range values {
		authored, found := authoredByAlias[value.Alias]
		if !found {
			continue
		}
		if value.Kind == "schema" {
			var authoredSchema, runtimeSchema scenarios.SchemaFact
			if json.Unmarshal(authored, &authoredSchema) != nil || json.Unmarshal(value.RuntimeValue, &runtimeSchema) != nil ||
				runtimeSchema.Version == 0 || runtimeSchema.Hash == "" {
				return fmt.Errorf("Swift schema-queued-mutation schema alias %q has no valid runtime value", value.Alias)
			}
			runtimeSchemas[authoredSchema] = runtimeSchema
			continue
		}
		resolved[value.Kind] = blackbox.NativeIdentityResolution{
			Kind:          value.Kind,
			Alias:         value.Alias,
			AuthoredValue: authored,
			RuntimeValue:  value.RuntimeValue,
		}
	}
	for index := range want {
		if err := schemaQueuedMutationEntryMatches(controller, want[index].TableID, want[index], got[index], resolved, runtimeSchemas, observed.Clients[0].Provenance); err != nil {
			return err
		}
	}
	// The outcome records the same generated mutation identity as the queue.
	wantOutcomes := expected.Clients[0].Outcomes
	gotOutcomes := observed.Clients[0].Outcomes
	if len(wantOutcomes) != len(gotOutcomes) {
		return fmt.Errorf("Swift schema-queued-mutation outcomes authored %d observed %d", len(wantOutcomes), len(gotOutcomes))
	}
	mutation, found := resolved["mutation-id"]
	if !found && len(wantOutcomes) > 0 {
		return errors.New("Swift schema-queued-mutation scenario declares no mutation-id alias")
	}
	for index := range wantOutcomes {
		if !resolutionMatchesString(mutation, wantOutcomes[index].MutationID, gotOutcomes[index].MutationID) {
			return fmt.Errorf("Swift schema-queued-mutation outcome mutation identity authored %q observed %q",
				wantOutcomes[index].MutationID, gotOutcomes[index].MutationID)
		}
		if wantOutcomes[index].State != gotOutcomes[index].State || wantOutcomes[index].Reason != gotOutcomes[index].Reason {
			return fmt.Errorf("Swift schema-queued-mutation outcome authored %s/%s observed %s/%s",
				wantOutcomes[index].State, wantOutcomes[index].Reason, gotOutcomes[index].State, gotOutcomes[index].Reason)
		}
	}
	return nil
}

// schemaQueuedMutationColumnSummary names each queued column by its field, its
// logical type, and its wire value.
func schemaQueuedMutationColumnSummary(columns []scenarios.FieldFact) string {
	entries := make([]string, 0, len(columns))
	for _, column := range columns {
		entries = append(entries, fmt.Sprintf("%s/%s=%s", column.FieldID, column.Type, column.WireJSON))
	}
	sort.Strings(entries)
	return "[" + strings.Join(entries, " ") + "]"
}

func schemaQueuedMutationEntryMatches(controller *blackbox.NativeController, authoredTable string, want, got scenarios.QueuedMutationFact, resolved map[string]blackbox.NativeIdentityResolution, runtimeSchemas map[scenarios.SchemaFact]scenarios.SchemaFact, provenance []scenarios.ProvenanceFact) error {
	for _, identity := range []struct {
		kind          string
		name          string
		authored, got string
	}{
		{"mutation-id", "mutation_id", want.MutationID, got.MutationID},
		{"table", "table_id", want.TableID, got.TableID},
	} {
		value, found := resolved[identity.kind]
		if !found {
			return fmt.Errorf("Swift schema-queued-mutation scenario declares no %s alias", identity.kind)
		}
		var runtime string
		if json.Unmarshal(value.RuntimeValue, &runtime) != nil || runtime == "" {
			return fmt.Errorf("Swift schema-queued-mutation %s alias has no runtime value", identity.kind)
		}
		if !resolutionMatchesString(value, identity.authored, identity.got) {
			return fmt.Errorf("Swift schema-queued-mutation queue %s authored %q observed %q wants runtime %q", identity.name, identity.authored, identity.got, runtime)
		}
	}
	// The base version is the server version the client held for the row it
	// updated. The queue cannot verify its own base version, so take the
	// runtime value from the client provenance record for the same row, which
	// the pull established independently of the queue.
	if want.BaseVersion != nil {
		if got.BaseVersion == nil {
			return errors.New("Swift schema-queued-mutation queue observed no base version")
		}
		matched := false
		for _, record := range provenance {
			if record.CanonicalWireJSON == got.CanonicalWireJSON && record.Version == *got.BaseVersion {
				matched = true
				break
			}
		}
		if !matched {
			versions := make([]string, 0, len(provenance))
			for _, record := range provenance {
				versions = append(versions, record.CanonicalWireJSON+":"+record.Version)
			}
			sort.Strings(versions)
			return fmt.Errorf("Swift schema-queued-mutation queue base version %q has no provenance record; provenance %v", *got.BaseVersion, versions)
		}
	}
	if want.Operation != got.Operation || want.Status != got.Status || want.LocalOrder != got.LocalOrder {
		return fmt.Errorf("Swift schema-queued-mutation queue entry state authored %s/%s/%d observed %s/%s/%d",
			want.Operation, want.Status, want.LocalOrder, got.Operation, got.Status, got.LocalOrder)
	}
	// The queue records the schema the client wrote under. The authored hash is
	// a corpus value, so resolve it before the comparison.
	runtimeSchema, bound := runtimeSchemas[want.AuthoredSchema]
	if !bound {
		return fmt.Errorf("Swift schema-queued-mutation authored queue schema %d has no alias", want.AuthoredSchema.Version)
	}
	if runtimeSchema != got.AuthoredSchema {
		return fmt.Errorf("Swift schema-queued-mutation queue schema authored %d/%s observed %d/%s",
			runtimeSchema.Version, runtimeSchema.Hash, got.AuthoredSchema.Version, got.AuthoredSchema.Hash)
	}
	// The queue records each column by its runtime field identifier. The
	// scenario declares no alias for a field, so resolve the authored field
	// through the controller binding.
	if len(want.AuthoredColumns) != len(got.AuthoredColumns) {
		return fmt.Errorf("Swift schema-queued-mutation queue columns authored %s observed %s",
			schemaQueuedMutationColumnSummary(want.AuthoredColumns), schemaQueuedMutationColumnSummary(got.AuthoredColumns))
	}
	for index, column := range want.AuthoredColumns {
		runtimeField, err := controller.RuntimeFieldID(authoredTable, column.FieldID)
		if err != nil {
			return fmt.Errorf("resolve Swift schema-queued-mutation queue column %q: %w", column.FieldID, err)
		}
		observedColumn := got.AuthoredColumns[index]
		if observedColumn.FieldID != runtimeField || observedColumn.Type != column.Type || observedColumn.WireJSON != column.WireJSON {
			return fmt.Errorf("Swift schema-queued-mutation queue column %q wants runtime %q; authored %s observed %s",
				column.FieldID, runtimeField,
				schemaQueuedMutationColumnSummary(want.AuthoredColumns), schemaQueuedMutationColumnSummary(got.AuthoredColumns))
		}
	}
	return nil
}

func swiftSchemaQueuedMutationRuntimeState(controller *blackbox.NativeController, aliases []scenarios.NativeIdentityAlias, expected scenarios.StateFacts) (scenarios.StateFacts, error) {
	schemaAliases := make([]scenarios.NativeIdentityAlias, 0, len(aliases))
	for _, alias := range aliases {
		if alias.Kind == "schema" || alias.Kind == "table" {
			schemaAliases = append(schemaAliases, alias)
		}
	}
	if len(schemaAliases) == 0 {
		return scenarios.StateFacts{}, errors.New("Swift schema-queued-mutation scenario declares no schema alias")
	}
	values, err := controller.IdentityValues(schemaAliases)
	if err != nil {
		return scenarios.StateFacts{}, fmt.Errorf("resolve Swift schema-queued-mutation schema identity: %w", err)
	}
	authoredByAlias := make(map[string]scenarios.NativeIdentityAlias, len(schemaAliases))
	for _, alias := range schemaAliases {
		authoredByAlias[alias.Alias] = alias
	}
	runtime := make(map[scenarios.SchemaFact]scenarios.SchemaFact, len(values))
	// The queue records the authored table identifier. The client stores the
	// runtime table the authored table binds to, so resolve it the same way.
	runtimeTables := make(map[string]string, len(values))
	for _, value := range values {
		alias, found := authoredByAlias[value.Alias]
		if !found {
			continue
		}
		if value.Kind == "table" {
			// The queue records the runtime table identifier, not the runtime
			// table name. ApplicationIdentifier carries the name, which the
			// provenance family records instead.
			var authoredTable, runtimeTable string
			if json.Unmarshal(alias.Value, &authoredTable) != nil || authoredTable == "" ||
				json.Unmarshal(value.RuntimeValue, &runtimeTable) != nil || runtimeTable == "" {
				return scenarios.StateFacts{}, fmt.Errorf("Swift schema-queued-mutation table alias %q has no valid runtime value", value.Alias)
			}
			runtimeTables[authoredTable] = runtimeTable
			continue
		}
		var authored, resolved scenarios.SchemaFact
		if json.Unmarshal(alias.Value, &authored) != nil || json.Unmarshal(value.RuntimeValue, &resolved) != nil ||
			resolved.Version == 0 || resolved.Hash == "" {
			return scenarios.StateFacts{}, fmt.Errorf("Swift schema-queued-mutation schema alias %q has no valid runtime value", value.Alias)
		}
		runtime[authored] = resolved
	}
	projected := scenarios.CloneStateFacts(expected)
	for clientIndex := range projected.Clients {
		client := &projected.Clients[clientIndex]
		if client.CurrentSchema != nil {
			resolved, found := runtime[*client.CurrentSchema]
			if !found {
				return scenarios.StateFacts{}, fmt.Errorf("Swift schema-queued-mutation authored schema %d has no alias", client.CurrentSchema.Version)
			}
			client.CurrentSchema = &resolved
		}
		for queueIndex := range client.Queue {
			resolved, found := runtime[client.Queue[queueIndex].AuthoredSchema]
			if !found {
				return scenarios.StateFacts{}, fmt.Errorf("Swift schema-queued-mutation authored queue schema %d has no alias", client.Queue[queueIndex].AuthoredSchema.Version)
			}
			client.Queue[queueIndex].AuthoredSchema = resolved
			runtimeTable, bound := runtimeTables[client.Queue[queueIndex].TableID]
			if !bound {
				return scenarios.StateFacts{}, fmt.Errorf("Swift schema-queued-mutation authored queue table %q has no alias", client.Queue[queueIndex].TableID)
			}
			client.Queue[queueIndex].TableID = runtimeTable
		}
	}
	return projected, nil
}

// schemaQueuedMutationRuntimeRebuildID binds the authored rebuild alias to the
// server rebuild session the client actually requested. The request carries a
// fingerprint of the identity, not the identity, so match the server session
// that produces that fingerprint.
func schemaQueuedMutationRuntimeRebuildID(baseline SynchronizationResult, server scenarios.StateFacts) (string, error) {
	fingerprints := make([]string, 0, len(baseline.transportObservations))
	for _, observation := range baseline.transportObservations {
		if observation.OperationClass != "rebuild" || observation.RequestFacts == nil || observation.RequestFacts.RebuildIDFingerprint == nil {
			continue
		}
		fingerprints = append(fingerprints, *observation.RequestFacts.RebuildIDFingerprint)
	}
	if len(fingerprints) == 0 {
		return "", errors.New("Swift schema-queued-mutation rebuild request carries no rebuild identity")
	}
	for _, rebuild := range server.Rebuilds {
		for _, fingerprint := range fingerprints {
			if cursorFingerprint(rebuild.RebuildID) == fingerprint {
				return rebuild.RebuildID, nil
			}
		}
	}
	sessions := make([]string, 0, len(server.Rebuilds))
	for _, rebuild := range server.Rebuilds {
		sessions = append(sessions, rebuild.ClientID+"/"+rebuild.ScopeID+":"+cursorFingerprint(rebuild.RebuildID))
	}
	sort.Strings(sessions)
	return "", fmt.Errorf("Swift schema-queued-mutation rebuild identity has no server session; requested %v server sessions %v", fingerprints, sessions)
}

// An alias that the final expectation declares resolves from the final client
// state. The other row-version and checksum aliases name the S1 source row,
// which the compatible push replaced, so they resolve from the baseline state.
func resolveSchemaQueuedMutationIdentities(controller *blackbox.NativeController, aliases []scenarios.NativeIdentityAlias, baseline, reset SynchronizationResult, baselineState, client, server scenarios.StateFacts) ([]blackbox.NativeIdentityResolution, error) {
	values, err := controller.IdentityValues(aliases)
	if err != nil {
		return nil, err
	}
	runtime := make(map[string]json.RawMessage, len(aliases))
	for _, value := range values {
		runtime[value.Alias] = append(json.RawMessage(nil), value.RuntimeValue...)
	}
	connect, err := swiftScenarioWire(reset, "connect")
	if err != nil || connect.RequestFacts == nil || connect.RequestFacts.ClientGeneration == nil {
		return nil, errors.New("Swift schema-queued-mutation client generation is absent")
	}
	encodedGeneration, err := json.Marshal(*connect.RequestFacts.ClientGeneration)
	if err != nil {
		return nil, fmt.Errorf("encode Swift schema-queued-mutation client generation: %w", err)
	}
	runtime["client-generation-one"] = encodedGeneration
	// IdentityValues binds server-owned kinds only. A client-owned identity
	// must come from an observed fact, so source each remaining alias from the
	// evidence the run produced.
	if connect.RequestFacts.ScopeSetVersion == nil {
		return nil, errors.New("Swift schema-queued-mutation scope-set version is absent")
	}
	encodedScopeSet, err := json.Marshal(*connect.RequestFacts.ScopeSetVersion)
	if err != nil {
		return nil, fmt.Errorf("encode Swift schema-queued-mutation scope-set version: %w", err)
	}
	for _, alias := range aliases {
		switch alias.Kind {
		case "scope-set-version":
			runtime[alias.Alias] = encodedScopeSet
		case "rebuild-id":
			runtimeID, resolveErr := schemaQueuedMutationRuntimeRebuildID(baseline, server)
			if resolveErr != nil {
				return nil, resolveErr
			}
			encoded, encodeErr := json.Marshal(runtimeID)
			if encodeErr != nil {
				return nil, fmt.Errorf("encode Swift schema-queued-mutation rebuild identity: %w", encodeErr)
			}
			runtime[alias.Alias] = encoded
		case "row-version":
			state := baselineState
			if len(alias.ExpectationIDs) != 0 {
				state = client
			}
			if len(state.Clients) != 1 || len(state.Clients[0].Provenance) != 1 {
				return nil, errors.New("Swift schema-queued-mutation provenance evidence is absent")
			}
			encoded, encodeErr := json.Marshal(state.Clients[0].Provenance[0].Version)
			if encodeErr != nil {
				return nil, fmt.Errorf("encode Swift schema-queued-mutation row version: %w", encodeErr)
			}
			runtime[alias.Alias] = encoded
		case "checksum":
			if len(baselineState.Clients) != 1 || len(baselineState.Clients[0].Checkpoints) != 1 || baselineState.Clients[0].Checkpoints[0].Checksum == nil {
				return nil, errors.New("Swift schema-queued-mutation checkpoint evidence is absent")
			}
			encoded, encodeErr := json.Marshal(*baselineState.Clients[0].Checkpoints[0].Checksum)
			if encodeErr != nil {
				return nil, fmt.Errorf("encode Swift schema-queued-mutation checkpoint checksum: %w", encodeErr)
			}
			runtime[alias.Alias] = encoded
		}
	}
	for _, alias := range aliases {
		if len(runtime[alias.Alias]) == 0 {
			return nil, fmt.Errorf("Swift schema-queued-mutation alias %q has no runtime evidence", alias.Alias)
		}
	}
	return resolveSwiftNativeIdentities(aliases, runtime)
}
