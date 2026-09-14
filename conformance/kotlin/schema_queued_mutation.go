package kotlin

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

// SchemaQueuedMutationResult records direct Kotlin Android evidence for one blocked schema-incompatible mutation.
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

// RunSchemaQueuedMutationScenario executes the authored durable blocked-mutation flow through Kotlin Android.
func RunSchemaQueuedMutationScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, platform *Platform, client Client) (SchemaQueuedMutationResult, error) {
	steps, err := kotlinScenarioStepMap(scenario, schemaQueuedMutationScenarioID, 12)
	if err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	if controller == nil || platform == nil {
		return SchemaQueuedMutationResult{}, errors.New("Kotlin Android schema-queued-mutation dependencies are unavailable")
	}
	if err := validateSchemaQueuedMutationBindings(scenario, steps, client); err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	expected, err := kotlinScenarioExpectedState(scenario, "EXPECT-SCHEMA-QUEUED-MUTATION-STATE-001")
	if err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("install Kotlin Android schema-queued-mutation contract: %w", err)
	}
	// The scenario authors its baseline rebuild, so empty initialization is required.
	// Current initialization would run that rebuild before the authored call.
	if err := platform.Install(ctx, InstallRequest{Client: client, Initialization: "empty"}); err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("install Kotlin Android schema-queued-mutation client: %w", err)
	}

	commit, err := kotlinScenarioOperation(steps, "STEP-SCHEMA-QUEUED-MUTATION-001", "model/commit-source-transaction")
	if err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	if observation, applyErr := controller.ApplyStep(ctx, commit); applyErr != nil || observation.Disposition != "success" {
		return SchemaQueuedMutationResult{}, fmt.Errorf("commit Kotlin Android schema-queued-mutation baseline: %w", kotlinResultError(applyErr, observation.Disposition))
	}
	materialize, err := kotlinScenarioOperation(steps, "STEP-SCHEMA-QUEUED-MUTATION-002", "process/materialize-source-transaction")
	if err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	if observation, processErr := controller.ProcessStep(ctx, nil, materialize); processErr != nil || observation.Disposition != "success" {
		return SchemaQueuedMutationResult{}, fmt.Errorf("materialize Kotlin Android schema-queued-mutation baseline: %w", kotlinResultError(processErr, observation.Disposition))
	}

	baseline, err := kotlinScenarioCall(ctx, platform, client, steps["STEP-SCHEMA-QUEUED-MUTATION-003"].NativeBinding.Method)
	if err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("run Kotlin Android schema-queued-mutation baseline: %w", err)
	}
	if err := validateSchemaQueuedMutationBaseline(scenario, steps, baseline); err != nil {
		return SchemaQueuedMutationResult{}, err
	}

	write, err := kotlinScenarioOperation(steps, "STEP-SCHEMA-QUEUED-MUTATION-005", "local/write")
	if err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	write, err = controller.ApplicationWrite(write)
	if err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("bind Kotlin Android schema-queued-mutation local write: %w", err)
	}
	if observation, applyErr := platform.ApplyStep(ctx, client, write); applyErr != nil || observation.Disposition != "success" {
		return SchemaQueuedMutationResult{}, fmt.Errorf("apply Kotlin Android schema-queued-mutation local write: %w", kotlinResultError(applyErr, observation.Disposition))
	}

	publish, err := kotlinScenarioOperation(steps, "STEP-SCHEMA-QUEUED-MUTATION-006", "model/publish-schema")
	if err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	if observation, applyErr := controller.ApplyStep(ctx, publish); applyErr != nil || observation.Disposition != "success" {
		return SchemaQueuedMutationResult{}, fmt.Errorf("publish Kotlin Android schema-queued-mutation schema: %w", kotlinResultError(applyErr, observation.Disposition))
	}

	// The baseline call leaves the engine started, and the authored unsupported call must connect again.
	if _, err := platform.Lifecycle(ctx, LifecycleRequest{Client: client, Operation: "stop"}); err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("stop Kotlin Android schema-queued-mutation client before its unsupported start: %w", err)
	}
	unsupported, err := kotlinScenarioCall(ctx, platform, client, steps["STEP-SCHEMA-QUEUED-MUTATION-UNSUPPORTED-001"].NativeBinding.Method)
	if err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("observe Kotlin Android schema-queued-mutation unsupported schema: %w", err)
	}
	if err := validateSchemaQueuedMutationCall(scenario, "STEP-SCHEMA-QUEUED-MUTATION-UNSUPPORTED-001", "connect", unsupported); err != nil {
		return SchemaQueuedMutationResult{}, err
	}

	reset, err := kotlinScenarioCall(ctx, platform, client, steps["STEP-SCHEMA-QUEUED-MUTATION-007"].NativeBinding.Method)
	if err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("run Kotlin Android schema-queued-mutation reset: %w", err)
	}
	if err := validateSchemaQueuedMutationReset(scenario, reset); err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	push, err := kotlinScenarioOperation(steps, "STEP-SCHEMA-QUEUED-MUTATION-008", "push/submit")
	if err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	if err := controller.BindApplicationPush(push); err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("bind Kotlin Android schema-queued-mutation push: %w", err)
	}

	restart, err := kotlinScenarioOperation(steps, "STEP-SCHEMA-QUEUED-MUTATION-009", "process/restart-client")
	if err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	if observation, processErr := platform.ProcessStep(ctx, client, restart); processErr != nil || observation.Disposition != "success" {
		return SchemaQueuedMutationResult{}, fmt.Errorf("restart Kotlin Android schema-queued-mutation client: %w", kotlinResultError(processErr, observation.Disposition))
	}

	clientFacts, err := platform.Capture(ctx, []Client{client}, []string{"application-rows", "pending-mutations", "rejected-mutations", "checkpoints", "provenance"})
	if err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("capture Kotlin Android schema-queued-mutation client state: %w", err)
	}
	clientState, err := mergeKotlinCaptureFacts(clientFacts)
	if err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	serverCaptures, err := controller.Capture(ctx, []string{client.Key}, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return SchemaQueuedMutationResult{}, fmt.Errorf("capture Kotlin Android schema-queued-mutation server state: %w", kotlinResultError(err, ""))
	}
	runtimeExpected, err := kotlinSchemaQueuedMutationRuntimeState(controller, scenario.NativeIdentityAliases, expected)
	if err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	if err := validateKotlinStateProjection(kotlinStateFactsWithoutGeneratedIdentities(runtimeExpected), kotlinStateFactsWithoutGeneratedIdentities(clientState)); err != nil {
		return SchemaQueuedMutationResult{}, fmt.Errorf("Kotlin Android schema-queued-mutation client state differs from the authored model: %w", err)
	}
	if err := validateKotlinSchemaQueuedMutationQueue(controller, scenario.NativeIdentityAliases, expected, clientState); err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	identities, err := resolveKotlinSchemaQueuedMutationIdentities(controller, scenario.NativeIdentityAliases, baseline, reset, clientState, serverCaptures[0].StateFacts)
	if err != nil {
		return SchemaQueuedMutationResult{}, err
	}
	return SchemaQueuedMutationResult{BaselineCall: baseline, UnsupportedCall: unsupported, ResetCall: reset, ClientFacts: clientFacts, ServerFacts: serverCaptures[0].StateFacts, IdentityResolution: identities}, nil
}

func validateSchemaQueuedMutationBindings(scenario scenarios.Scenario, steps map[scenarios.StepID]scenarios.Step, client Client) error {
	wanted := []struct{ id, key, kind, method string }{
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
		return errors.New("Kotlin Android schema-queued-mutation step bindings are incomplete")
	}
	callIDs := make(map[string]string)
	for _, expected := range wanted {
		step, found := steps[scenarios.StepID(expected.id)]
		if !found || scenarios.OperationKey(step.Operation) != expected.key || step.NativeBinding == nil || step.NativeBinding.Kind != expected.kind || step.NativeBinding.Method != expected.method || step.ExpectedOutcome.Disposition != "success" {
			return fmt.Errorf("Kotlin Android schema-queued-mutation binding %s is invalid", expected.id)
		}
		if expected.kind == "controller" {
			continue
		}
		if err := kotlinScenarioClient(step, client); err != nil {
			return err
		}
		if expected.kind != "public-call" {
			continue
		}
		if step.NativeBinding.CallID == nil || *step.NativeBinding.CallID == "" || step.NativeBinding.Stage != "synchronous" {
			return fmt.Errorf("Kotlin Android schema-queued-mutation public binding %s is invalid", expected.id)
		}
		group := "baseline"
		if expected.id == "STEP-SCHEMA-QUEUED-MUTATION-UNSUPPORTED-001" {
			group = "unsupported"
		} else if expected.id == "STEP-SCHEMA-QUEUED-MUTATION-007" || expected.id == "STEP-SCHEMA-QUEUED-MUTATION-008" {
			group = "reset"
		}
		if prior, found := callIDs[group]; found && prior != string(*step.NativeBinding.CallID) {
			return fmt.Errorf("Kotlin Android schema-queued-mutation %s call binding observed %q, expected %q", group, *step.NativeBinding.CallID, prior)
		}
		callIDs[group] = string(*step.NativeBinding.CallID)
		completion, err := schemaQueuedMutationCompletion(scenario, step)
		if err != nil || step.NativeBinding.Completion != completion {
			return fmt.Errorf("Kotlin Android schema-queued-mutation completion %s is not derived from its authored outcome", expected.id)
		}
	}
	if len(callIDs) != 3 || callIDs["baseline"] == callIDs["unsupported"] || callIDs["baseline"] == callIDs["reset"] || callIDs["unsupported"] == callIDs["reset"] {
		return fmt.Errorf("Kotlin Android schema-queued-mutation public call identities are invalid: %v", callIDs)
	}
	return nil
}

func validateSchemaQueuedMutationBaseline(scenario scenarios.Scenario, steps map[scenarios.StepID]scenarios.Step, result SynchronizationResult) error {
	completion, err := schemaQueuedMutationCompletion(scenario, steps["STEP-SCHEMA-QUEUED-MUTATION-BASELINE-FINALIZE-001"])
	if err != nil || result.Completion != completion {
		return fmt.Errorf("Kotlin Android schema-queued-mutation baseline completion observed %q, expected %q", result.Completion, completion)
	}
	rebuild, err := kotlinScenarioWire(result, "rebuild")
	if err != nil {
		return err
	}
	if err := validateKotlinWireExpectation(scenario, "STEP-SCHEMA-QUEUED-MUTATION-003", "rebuild", result); err != nil {
		return err
	}
	operation, err := kotlinScenarioOperation(steps, "STEP-SCHEMA-QUEUED-MUTATION-003", "rebuild/request-page")
	if err != nil {
		return err
	}
	var payload schemaQueuedMutationRebuildPayload
	if json.Unmarshal(operation.Payload, &payload) != nil || payload.Limit == 0 || rebuild.RequestFacts == nil || rebuild.RequestFacts.Limit == nil || uint64(*rebuild.RequestFacts.Limit) != payload.Limit {
		observed := "<absent>"
		if rebuild.RequestFacts != nil && rebuild.RequestFacts.Limit != nil {
			observed = fmt.Sprintf("%d", *rebuild.RequestFacts.Limit)
		}
		return fmt.Errorf("Kotlin Android schema-queued-mutation rebuild limit observed %s, expected %d", observed, payload.Limit)
	}
	return nil
}

func validateSchemaQueuedMutationCall(scenario scenarios.Scenario, stepID, operationClass string, result SynchronizationResult) error {
	step, found := schemaQueuedMutationStep(scenario, stepID)
	if !found {
		return fmt.Errorf("Kotlin Android schema-queued-mutation step %s is absent", stepID)
	}
	completion, err := schemaQueuedMutationCompletion(scenario, step)
	if err != nil || result.Completion != completion {
		return fmt.Errorf("Kotlin Android schema-queued-mutation step %s completion observed %q, expected %q", stepID, result.Completion, completion)
	}
	return validateKotlinWireExpectation(scenario, stepID, operationClass, result)
}

func validateSchemaQueuedMutationReset(scenario scenarios.Scenario, result SynchronizationResult) error {
	terminal, found := schemaQueuedMutationStep(scenario, "STEP-SCHEMA-QUEUED-MUTATION-008")
	if !found {
		return errors.New("Kotlin Android schema-queued-mutation reset terminal step is absent")
	}
	completion, err := schemaQueuedMutationCompletion(scenario, terminal)
	if err != nil || result.Completion != completion {
		return fmt.Errorf("Kotlin Android schema-queued-mutation reset completion observed %q, expected %q", result.Completion, completion)
	}
	if err := validateKotlinWireExpectation(scenario, "STEP-SCHEMA-QUEUED-MUTATION-007", "connect", result); err != nil {
		return err
	}
	if err := validateKotlinWireExpectation(scenario, "STEP-SCHEMA-QUEUED-MUTATION-008", "push", result); err != nil {
		return err
	}
	push, err := kotlinScenarioWire(result, "push")
	if err != nil || push.RequestFacts == nil || push.RequestFacts.MutationCount == nil {
		return errors.New("Kotlin Android schema-queued-mutation push facts are incomplete")
	}
	step, found := schemaQueuedMutationStep(scenario, "STEP-SCHEMA-QUEUED-MUTATION-008")
	if !found {
		return errors.New("Kotlin Android schema-queued-mutation push step is absent")
	}
	var payload schemaQueuedMutationPushPayload
	if json.Unmarshal(step.Operation.Payload, &payload) != nil || payload.AuthenticatedUserID != step.NativeBinding.UserID || payload.Request.ClientID != step.NativeBinding.ClientID || payload.Request.BatchID == "" || len(payload.Request.Mutations) == 0 || int64(len(payload.Request.Mutations)) != int64(*push.RequestFacts.MutationCount) {
		return fmt.Errorf("Kotlin Android schema-queued-mutation push mutation count observed %d, expected authored count %d", *push.RequestFacts.MutationCount, len(payload.Request.Mutations))
	}
	for _, mutation := range payload.Request.Mutations {
		if mutation.MutationID == "" {
			return errors.New("Kotlin Android schema-queued-mutation push mutation identity is absent")
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

// kotlinStateFactsWithoutGeneratedIdentities drops client families with generated identities.
func kotlinStateFactsWithoutGeneratedIdentities(facts scenarios.StateFacts) scenarios.StateFacts {
	projected := scenarios.CloneStateFacts(facts)
	for index := range projected.Clients {
		projected.Clients[index].Queue = nil
		projected.Clients[index].Outcomes = nil
	}
	return projected
}

func validateKotlinSchemaQueuedMutationQueue(controller *blackbox.NativeController, aliases []scenarios.NativeIdentityAlias, expected, observed scenarios.StateFacts) error {
	if len(expected.Clients) != 1 || len(observed.Clients) != 1 {
		return fmt.Errorf("Kotlin Android schema-queued-mutation client count observed %d, expected %d", len(observed.Clients), len(expected.Clients))
	}
	want, got := expected.Clients[0].Queue, observed.Clients[0].Queue
	if len(want) != len(got) {
		// A queue length alone cannot show where the mutation went. Name the
		// neighbouring durable counts, which separate a mutation the client
		// never queued from one it pushed, sealed, or rejected.
		count := func(value *uint64) string {
			if value == nil {
				return "none"
			}
			return fmt.Sprintf("%d", *value)
		}
		client := observed.Clients[0]
		return fmt.Errorf(
			"Kotlin Android schema-queued-mutation queue entries observed %d, expected %d; "+
				"queue count %s, sealed batches %s, outcomes %s, rows %s, observed outcome records %d",
			len(got), len(want),
			count(client.QueueCount), count(client.SealedBatchCount), count(client.OutcomeCount),
			count(client.RowCount), len(client.Outcomes),
		)
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
		return fmt.Errorf("resolve Kotlin Android schema-queued-mutation queue identity: %w", err)
	}
	authoredByAlias := make(map[string]json.RawMessage, len(resolvable))
	for _, alias := range resolvable {
		authoredByAlias[alias.Alias] = alias.Value
	}
	runtimeSchemas := make(map[scenarios.SchemaFact]scenarios.SchemaFact, len(values))
	resolved := make(map[string]blackbox.NativeIdentityResolution, len(values))
	for _, value := range values {
		authored, found := authoredByAlias[value.Alias]
		if !found {
			continue
		}
		if value.Kind == "schema" {
			var authoredSchema, runtimeSchema scenarios.SchemaFact
			if json.Unmarshal(authored, &authoredSchema) != nil || json.Unmarshal(value.RuntimeValue, &runtimeSchema) != nil || runtimeSchema.Version == 0 || runtimeSchema.Hash == "" {
				return fmt.Errorf("Kotlin Android schema-queued-mutation schema alias %q has no valid runtime value", value.Alias)
			}
			runtimeSchemas[authoredSchema] = runtimeSchema
			continue
		}
		resolved[value.Kind] = blackbox.NativeIdentityResolution{Kind: value.Kind, Alias: value.Alias, AuthoredValue: authored, RuntimeValue: value.RuntimeValue}
	}
	for index := range want {
		if err := schemaQueuedMutationEntryMatches(controller, want[index].TableID, want[index], got[index], resolved, runtimeSchemas, observed.Clients[0].Provenance); err != nil {
			return err
		}
	}
	wantOutcomes, gotOutcomes := expected.Clients[0].Outcomes, observed.Clients[0].Outcomes
	if len(wantOutcomes) != len(gotOutcomes) {
		return fmt.Errorf("Kotlin Android schema-queued-mutation outcomes observed %d, expected %d", len(gotOutcomes), len(wantOutcomes))
	}
	mutation, found := resolved["mutation-id"]
	if !found && len(wantOutcomes) > 0 {
		return errors.New("Kotlin Android schema-queued-mutation scenario declares no mutation-id alias")
	}
	for index := range wantOutcomes {
		if !kotlinResolutionMatchesString(mutation, wantOutcomes[index].MutationID, gotOutcomes[index].MutationID) {
			return fmt.Errorf("Kotlin Android schema-queued-mutation outcome mutation identity authored %q observed %q", wantOutcomes[index].MutationID, gotOutcomes[index].MutationID)
		}
		if wantOutcomes[index].State != gotOutcomes[index].State || wantOutcomes[index].Reason != gotOutcomes[index].Reason {
			return fmt.Errorf("Kotlin Android schema-queued-mutation outcome authored %s/%s observed %s/%s", wantOutcomes[index].State, wantOutcomes[index].Reason, gotOutcomes[index].State, gotOutcomes[index].Reason)
		}
	}
	return nil
}

func schemaQueuedMutationColumnSummary(columns []scenarios.FieldFact) string {
	entries := make([]string, 0, len(columns))
	for _, column := range columns {
		entries = append(entries, fmt.Sprintf("%s/%s=%s", column.FieldID, column.Type, column.WireJSON))
	}
	sort.Strings(entries)
	return "[" + strings.Join(entries, " ") + "]"
}

func schemaQueuedMutationEntryMatches(controller *blackbox.NativeController, authoredTable string, want, got scenarios.QueuedMutationFact, resolved map[string]blackbox.NativeIdentityResolution, runtimeSchemas map[scenarios.SchemaFact]scenarios.SchemaFact, provenance []scenarios.ProvenanceFact) error {
	for _, identity := range []struct{ kind, name, authored, got string }{
		{"mutation-id", "mutation_id", want.MutationID, got.MutationID},
		{"table", "table_id", want.TableID, got.TableID},
	} {
		value, found := resolved[identity.kind]
		if !found {
			return fmt.Errorf("Kotlin Android schema-queued-mutation scenario declares no %s alias", identity.kind)
		}
		var runtime string
		if json.Unmarshal(value.RuntimeValue, &runtime) != nil || runtime == "" {
			return fmt.Errorf("Kotlin Android schema-queued-mutation %s alias has no runtime value", identity.kind)
		}
		if !kotlinResolutionMatchesString(value, identity.authored, identity.got) {
			return fmt.Errorf("Kotlin Android schema-queued-mutation queue %s authored %q observed %q expects runtime %q", identity.name, identity.authored, identity.got, runtime)
		}
	}
	if want.BaseVersion != nil {
		if got.BaseVersion == nil {
			return errors.New("Kotlin Android schema-queued-mutation queue observed no base version")
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
			return fmt.Errorf("Kotlin Android schema-queued-mutation queue base version %q has no provenance record, observed provenance %v", *got.BaseVersion, versions)
		}
	}
	if want.Operation != got.Operation || want.Status != got.Status || want.LocalOrder != got.LocalOrder {
		return fmt.Errorf("Kotlin Android schema-queued-mutation queue entry authored %s/%s/%d observed %s/%s/%d", want.Operation, want.Status, want.LocalOrder, got.Operation, got.Status, got.LocalOrder)
	}
	runtimeSchema, bound := runtimeSchemas[want.AuthoredSchema]
	if !bound {
		return fmt.Errorf("Kotlin Android schema-queued-mutation authored queue schema %d has no alias", want.AuthoredSchema.Version)
	}
	if runtimeSchema != got.AuthoredSchema {
		return fmt.Errorf("Kotlin Android schema-queued-mutation queue schema authored %d/%s observed %d/%s", runtimeSchema.Version, runtimeSchema.Hash, got.AuthoredSchema.Version, got.AuthoredSchema.Hash)
	}
	if len(want.AuthoredColumns) != len(got.AuthoredColumns) {
		return fmt.Errorf("Kotlin Android schema-queued-mutation queue columns authored %s observed %s", schemaQueuedMutationColumnSummary(want.AuthoredColumns), schemaQueuedMutationColumnSummary(got.AuthoredColumns))
	}
	for index, column := range want.AuthoredColumns {
		runtimeField, err := controller.RuntimeFieldID(authoredTable, column.FieldID)
		if err != nil {
			return fmt.Errorf("resolve Kotlin Android schema-queued-mutation queue column %q: %w", column.FieldID, err)
		}
		observedColumn := got.AuthoredColumns[index]
		if observedColumn.FieldID != runtimeField || observedColumn.Type != column.Type || observedColumn.WireJSON != column.WireJSON {
			return fmt.Errorf("Kotlin Android schema-queued-mutation queue column %q wants runtime %q, observed %s, expected %s", column.FieldID, runtimeField, schemaQueuedMutationColumnSummary(got.AuthoredColumns), schemaQueuedMutationColumnSummary(want.AuthoredColumns))
		}
	}
	return nil
}

func kotlinSchemaQueuedMutationRuntimeState(controller *blackbox.NativeController, aliases []scenarios.NativeIdentityAlias, expected scenarios.StateFacts) (scenarios.StateFacts, error) {
	schemaAliases := make([]scenarios.NativeIdentityAlias, 0, len(aliases))
	for _, alias := range aliases {
		if alias.Kind == "schema" || alias.Kind == "table" {
			schemaAliases = append(schemaAliases, alias)
		}
	}
	if len(schemaAliases) == 0 {
		return scenarios.StateFacts{}, errors.New("Kotlin Android schema-queued-mutation scenario declares no schema alias")
	}
	values, err := controller.IdentityValues(schemaAliases)
	if err != nil {
		return scenarios.StateFacts{}, fmt.Errorf("resolve Kotlin Android schema-queued-mutation schema identity: %w", err)
	}
	authoredByAlias := make(map[string]scenarios.NativeIdentityAlias, len(schemaAliases))
	for _, alias := range schemaAliases {
		authoredByAlias[alias.Alias] = alias
	}
	runtime := make(map[scenarios.SchemaFact]scenarios.SchemaFact, len(values))
	runtimeTables := make(map[string]string, len(values))
	for _, value := range values {
		alias, found := authoredByAlias[value.Alias]
		if !found {
			continue
		}
		if value.Kind == "table" {
			var authoredTable, runtimeTable string
			if json.Unmarshal(alias.Value, &authoredTable) != nil || authoredTable == "" || json.Unmarshal(value.RuntimeValue, &runtimeTable) != nil || runtimeTable == "" {
				return scenarios.StateFacts{}, fmt.Errorf("Kotlin Android schema-queued-mutation table alias %q has no valid runtime value", value.Alias)
			}
			runtimeTables[authoredTable] = runtimeTable
			continue
		}
		var authored, resolved scenarios.SchemaFact
		if json.Unmarshal(alias.Value, &authored) != nil || json.Unmarshal(value.RuntimeValue, &resolved) != nil || resolved.Version == 0 || resolved.Hash == "" {
			return scenarios.StateFacts{}, fmt.Errorf("Kotlin Android schema-queued-mutation schema alias %q has no valid runtime value", value.Alias)
		}
		runtime[authored] = resolved
	}
	projected := scenarios.CloneStateFacts(expected)
	for clientIndex := range projected.Clients {
		client := &projected.Clients[clientIndex]
		if client.CurrentSchema != nil {
			resolved, found := runtime[*client.CurrentSchema]
			if !found {
				return scenarios.StateFacts{}, fmt.Errorf("Kotlin Android schema-queued-mutation authored schema %d has no alias", client.CurrentSchema.Version)
			}
			client.CurrentSchema = &resolved
		}
		for queueIndex := range client.Queue {
			resolved, found := runtime[client.Queue[queueIndex].AuthoredSchema]
			if !found {
				return scenarios.StateFacts{}, fmt.Errorf("Kotlin Android schema-queued-mutation authored queue schema %d has no alias", client.Queue[queueIndex].AuthoredSchema.Version)
			}
			client.Queue[queueIndex].AuthoredSchema = resolved
			runtimeTable, bound := runtimeTables[client.Queue[queueIndex].TableID]
			if !bound {
				return scenarios.StateFacts{}, fmt.Errorf("Kotlin Android schema-queued-mutation authored queue table %q has no alias", client.Queue[queueIndex].TableID)
			}
			client.Queue[queueIndex].TableID = runtimeTable
		}
	}
	return projected, nil
}

func schemaQueuedMutationRuntimeRebuildID(baseline SynchronizationResult, server scenarios.StateFacts) (string, error) {
	fingerprints := make([]string, 0, len(baseline.transportObservations))
	for _, observation := range baseline.transportObservations {
		if observation.OperationClass != "rebuild" || observation.RequestFacts == nil || observation.RequestFacts.RebuildIDFingerprint == nil {
			continue
		}
		fingerprints = append(fingerprints, *observation.RequestFacts.RebuildIDFingerprint)
	}
	if len(fingerprints) == 0 {
		return "", errors.New("Kotlin Android schema-queued-mutation rebuild request carries no rebuild identity")
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
	return "", fmt.Errorf("Kotlin Android schema-queued-mutation rebuild identity has no server session, requested %v, observed server sessions %v", fingerprints, sessions)
}

func resolveKotlinSchemaQueuedMutationIdentities(controller *blackbox.NativeController, aliases []scenarios.NativeIdentityAlias, baseline, reset SynchronizationResult, client, server scenarios.StateFacts) ([]blackbox.NativeIdentityResolution, error) {
	values, err := controller.IdentityValues(aliases)
	if err != nil {
		return nil, err
	}
	runtime := make(map[string]json.RawMessage, len(aliases))
	for _, value := range values {
		runtime[value.Alias] = append(json.RawMessage(nil), value.RuntimeValue...)
	}
	connect, err := kotlinScenarioWire(reset, "connect")
	if err != nil || connect.RequestFacts == nil || connect.RequestFacts.ClientGeneration == nil {
		return nil, errors.New("Kotlin Android schema-queued-mutation client generation is absent")
	}
	encodedGeneration, err := json.Marshal(*connect.RequestFacts.ClientGeneration)
	if err != nil {
		return nil, fmt.Errorf("encode Kotlin Android schema-queued-mutation client generation: %w", err)
	}
	runtime["client-generation-one"] = encodedGeneration
	if connect.RequestFacts.ScopeSetVersion == nil {
		return nil, errors.New("Kotlin Android schema-queued-mutation scope-set version is absent")
	}
	encodedScopeSet, err := json.Marshal(*connect.RequestFacts.ScopeSetVersion)
	if err != nil {
		return nil, fmt.Errorf("encode Kotlin Android schema-queued-mutation scope-set version: %w", err)
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
				return nil, fmt.Errorf("encode Kotlin Android schema-queued-mutation rebuild identity: %w", encodeErr)
			}
			runtime[alias.Alias] = encoded
		case "row-version":
			if len(client.Clients) != 1 || len(client.Clients[0].Provenance) != 1 {
				return nil, errors.New("Kotlin Android schema-queued-mutation provenance evidence is absent")
			}
			encoded, encodeErr := json.Marshal(client.Clients[0].Provenance[0].Version)
			if encodeErr != nil {
				return nil, fmt.Errorf("encode Kotlin Android schema-queued-mutation row version: %w", encodeErr)
			}
			runtime[alias.Alias] = encoded
		case "checksum":
			if len(client.Clients) != 1 || len(client.Clients[0].Checkpoints) != 1 || client.Clients[0].Checkpoints[0].Checksum == nil {
				return nil, errors.New("Kotlin Android schema-queued-mutation checkpoint evidence is absent")
			}
			encoded, encodeErr := json.Marshal(*client.Clients[0].Checkpoints[0].Checksum)
			if encodeErr != nil {
				return nil, fmt.Errorf("encode Kotlin Android schema-queued-mutation checkpoint checksum: %w", encodeErr)
			}
			runtime[alias.Alias] = encoded
		}
	}
	for _, alias := range aliases {
		if len(runtime[alias.Alias]) == 0 {
			return nil, fmt.Errorf("Kotlin Android schema-queued-mutation alias %q has no runtime evidence", alias.Alias)
		}
	}
	return resolveKotlinNativeIdentities(aliases, runtime)
}
