package swift

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

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
	steps, err := swiftScenarioStepMap(scenario, schemaQueuedMutationScenarioID, 12)
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

	write, _ := swiftScenarioOperation(steps, "STEP-SCHEMA-QUEUED-MUTATION-005", "local/write")
	write, err = controller.ApplicationWrite(write)
	if err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("bind Swift schema-queued-mutation local write: %w", err)
	}
	if observation, applyErr := platform.ApplyStep(ctx, client, write); applyErr != nil || observation.Disposition != "success" {
		return SchemaQueuedMutationResult{}, fmt.Errorf("apply Swift schema-queued-mutation local write: %w", resultError(applyErr, observation.Disposition))
	}

	publish, _ := swiftScenarioOperation(steps, "STEP-SCHEMA-QUEUED-MUTATION-006", "model/publish-schema")
	if observation, applyErr := controller.ApplyStep(ctx, publish); applyErr != nil || observation.Disposition != "success" {
		return SchemaQueuedMutationResult{}, fmt.Errorf("publish Swift schema-queued-mutation schema: %w", resultError(applyErr, observation.Disposition))
	}

	// The baseline call left the engine started, and it rejects a second start.
	// The authored step expects a real connect that the server answers with an
	// unsupported action, so the client stops before it starts again.
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
	if err := validateSchemaQueuedMutationReset(scenario, reset); err != nil {
		return SchemaQueuedMutationResult{}, err
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
	// The authored schema hash is a corpus value. The client observes the
	// hash the server published, so compare the authored client state against
	// the runtime schema each authored alias resolves to.
	runtimeExpected, err := swiftSchemaQueuedMutationRuntimeState(controller, scenario.NativeIdentityAliases, expected)
	if err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	if err := validateSwiftStateProjection(runtimeExpected, clientState); err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("Swift schema-queued-mutation client state differs from the authored model: %w", err)
	}
	serverCaptures, err := controller.Capture(ctx, []string{client.Key}, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return SchemaQueuedMutationResult{}, fmt.Errorf("capture Swift schema-queued-mutation server state: %w", err)
	}
	identities, err := resolveSchemaQueuedMutationIdentities(controller, scenario.NativeIdentityAliases, reset)
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
	if len(callIDs) != 3 || callIDs["baseline"] == callIDs["unsupported"] || callIDs["baseline"] == callIDs["reset"] || callIDs["unsupported"] == callIDs["reset"] {
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

func validateSchemaQueuedMutationReset(scenario scenarios.Scenario, result SynchronizationResult) error {
	terminal, found := schemaQueuedMutationStep(scenario, "STEP-SCHEMA-QUEUED-MUTATION-008")
	if !found {
		return errors.New("Swift schema-queued-mutation reset terminal step is absent")
	}
	completion, err := schemaQueuedMutationCompletion(scenario, terminal)
	if err != nil || result.Completion != completion {
		return errors.New("Swift schema-queued-mutation reset completion differs from its authored terminal outcome")
	}
	if err := validateSwiftWireExpectation(scenario, "STEP-SCHEMA-QUEUED-MUTATION-007", "connect", result); err != nil {
		return err
	}
	if err := validateSwiftWireExpectation(scenario, "STEP-SCHEMA-QUEUED-MUTATION-008", "push", result); err != nil {
		return err
	}
	push, err := swiftScenarioWire(result, "push")
	if err != nil || push.RequestFacts == nil || push.RequestFacts.MutationCount == nil {
		return errors.New("Swift schema-queued-mutation push facts are incomplete")
	}
	step, found := schemaQueuedMutationStep(scenario, "STEP-SCHEMA-QUEUED-MUTATION-008")
	if !found {
		return errors.New("Swift schema-queued-mutation push step is absent")
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

func resolveSchemaQueuedMutationIdentities(controller *blackbox.NativeController, aliases []scenarios.NativeIdentityAlias, reset SynchronizationResult) ([]blackbox.NativeIdentityResolution, error) {
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
	for _, alias := range aliases {
		if len(runtime[alias.Alias]) == 0 {
			return nil, fmt.Errorf("Swift schema-queued-mutation alias %q has no runtime evidence", alias.Alias)
		}
	}
	return resolveSwiftNativeIdentities(aliases, runtime)
}
