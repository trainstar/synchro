package swift

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const forgedCursorScenarioID = "SCN-REBUILD-FORGED-CURSOR-001"

var forgedCursorAliasNames = []string{
	"row-one-mutation",
	"row-two-mutation",
	"source-batch",
	"client-generation-one",
	"current-schema",
	"items-table",
	"row-one-primary-key",
	"row-two-primary-key",
	"scope-a",
	"forged-rebuild",
}

// ForgedCursorResult records direct Swift evidence for one rejected forged continuation.
type ForgedCursorResult struct {
	Call               CallResult
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

// RunForgedCursorScenario executes the authored forged rebuild-continuation flow through Swift.
func RunForgedCursorScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, artifact *blackbox.NativeArtifact, platform *Platform, client Client) (ForgedCursorResult, error) {
	steps, err := swiftScenarioStepMap(scenario, forgedCursorScenarioID, 6)
	if err != nil {
		return ForgedCursorResult{}, err
	}
	if controller == nil || artifact == nil || platform == nil {
		return ForgedCursorResult{}, errors.New("Swift forged-cursor dependencies are unavailable")
	}
	if err := validateForgedCursorBindings(steps, client); err != nil {
		return ForgedCursorResult{}, err
	}

	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return ForgedCursorResult{}, fmt.Errorf("install Swift forged-cursor contract: %w", err)
	}
	seedPath, err := artifact.StageCurrentSeed(ctx, client.UserID, client.ClientID, scenarios.StepID("STEP-REBUILD-FORGED-CURSOR-001"))
	if err != nil {
		return ForgedCursorResult{}, fmt.Errorf("stage Swift forged-cursor current seed: %w", err)
	}
	if err := platform.Install(ctx, client, "seed", seedPath); err != nil {
		return ForgedCursorResult{}, fmt.Errorf("install Swift forged-cursor client: %w", err)
	}

	for _, id := range []string{"STEP-REBUILD-FORGED-CURSOR-001", "STEP-REBUILD-FORGED-CURSOR-002"} {
		write, _ := swiftScenarioOperation(steps, id, "local/write")
		write, err = controller.ApplicationWrite(write)
		if err != nil {
			return ForgedCursorResult{}, fmt.Errorf("bind Swift forged-cursor local write %s: %w", id, err)
		}
		observed, applyErr := platform.ApplyStep(ctx, client, write)
		if applyErr != nil || observed.Disposition != "success" {
			return ForgedCursorResult{}, fmt.Errorf("apply Swift forged-cursor local write %s: %w", id, resultError(applyErr, observed.Disposition))
		}
	}

	push, _ := swiftScenarioOperation(steps, "STEP-REBUILD-FORGED-CURSOR-003", "push/submit")
	materialize, _ := swiftScenarioOperation(steps, "STEP-REBUILD-FORGED-CURSOR-004", "process/materialize-source-transaction")
	firstPage, _ := swiftScenarioOperation(steps, "STEP-REBUILD-FORGED-CURSOR-005", "rebuild/request-page")
	forgedPage, _ := swiftScenarioOperation(steps, "STEP-REBUILD-FORGED-CURSOR-006", "rebuild/request-page")
	callID := string(*steps[scenarios.StepID("STEP-REBUILD-FORGED-CURSOR-003")].NativeBinding.CallID)

	state, err := platform.client(client)
	if err != nil {
		return ForgedCursorResult{}, err
	}
	state.mu.Lock()
	transportCheckpoint := state.session.Checkpoint()
	state.mu.Unlock()

	begin, err := platform.BeginCall(ctx, client, callID, "start", RequestOperations{push})
	if err != nil {
		return ForgedCursorResult{}, fmt.Errorf("begin Swift forged-cursor call: %w", err)
	}
	if begin.CallID != callID || begin.State != "in_flight" || begin.Completion != "" || len(begin.Steps) != 1 {
		return ForgedCursorResult{}, errors.New("Swift forged-cursor push did not enter the staged call")
	}
	if err := validateForgedCursorStepWire(scenario, "STEP-REBUILD-FORGED-CURSOR-003", begin.Steps[0]); err != nil {
		return ForgedCursorResult{}, err
	}
	if err := controller.BindApplicationPush(push); err != nil {
		return ForgedCursorResult{}, fmt.Errorf("bind Swift forged-cursor push transaction: %w", err)
	}
	materialized, err := controller.ProcessStep(ctx, nil, materialize)
	if err != nil || materialized.Disposition != "success" {
		return ForgedCursorResult{}, fmt.Errorf("materialize Swift forged-cursor push: %w", resultError(err, materialized.Disposition))
	}

	if err := platform.armRebuildCursorOverride(client.ClientID, forgedRebuildCursor); err != nil {
		return ForgedCursorResult{}, fmt.Errorf("arm Swift forged rebuild continuation: %w", err)
	}
	first, err := platform.AwaitStep(ctx, client, callID, firstPage)
	if err != nil {
		return ForgedCursorResult{}, fmt.Errorf("await Swift forged-cursor first page: %w", err)
	}
	if err := validateForgedCursorStepWire(scenario, "STEP-REBUILD-FORGED-CURSOR-005", first); err != nil {
		return ForgedCursorResult{}, err
	}
	serverBefore, err := captureForgedCursorServer(ctx, controller)
	if err != nil {
		return ForgedCursorResult{}, err
	}

	forged, err := platform.AwaitStep(ctx, client, callID, forgedPage)
	if err != nil {
		return ForgedCursorResult{}, fmt.Errorf("await Swift forged continuation rejection: %w", err)
	}
	if err := validateForgedCursorStepWire(scenario, "STEP-REBUILD-FORGED-CURSOR-006", forged); err != nil {
		return ForgedCursorResult{}, err
	}
	serverAfter, err := captureForgedCursorServer(ctx, controller)
	if err != nil {
		return ForgedCursorResult{}, err
	}
	if err := validateForgedCursorServerFreeze(serverBefore, serverAfter); err != nil {
		return ForgedCursorResult{}, err
	}

	completed, err := platform.AwaitCall(ctx, client, callID)
	if err != nil {
		return ForgedCursorResult{}, fmt.Errorf("complete Swift forged-cursor call: %w", err)
	}
	if completed.CallID != callID || completed.State != "completed" || completed.Completion != "error" {
		return ForgedCursorResult{}, errors.New("Swift forged-cursor call did not complete with an error")
	}
	state.mu.Lock()
	transport, transportErr := state.session.ObservationsAfter(transportCheckpoint)
	state.mu.Unlock()
	if transportErr != nil {
		return ForgedCursorResult{}, fmt.Errorf("capture Swift forged-cursor transport: %w", transportErr)
	}
	if err := validateForgedCursorTransport(scenario, transport); err != nil {
		return ForgedCursorResult{}, err
	}
	runtime, resolutions, err := resolveForgedCursorIdentities(controller, scenario.NativeIdentityAliases, transport, serverAfter)
	if err != nil {
		return ForgedCursorResult{}, err
	}
	if err := validateForgedCursorState(scenario, serverAfter, runtime); err != nil {
		return ForgedCursorResult{}, err
	}
	return ForgedCursorResult{Call: completed, ServerFacts: serverAfter, IdentityResolution: resolutions}, nil
}

func validateForgedCursorBindings(steps map[scenarios.StepID]scenarios.Step, client Client) error {
	expected := []struct {
		id, key, kind, stage, method, completion, disposition string
	}{
		{"STEP-REBUILD-FORGED-CURSOR-001", "local/write", "local-write", "", "", "", "success"},
		{"STEP-REBUILD-FORGED-CURSOR-002", "local/write", "local-write", "", "", "", "success"},
		{"STEP-REBUILD-FORGED-CURSOR-003", "push/submit", "public-call", "begin", "start", "", "success"},
		{"STEP-REBUILD-FORGED-CURSOR-004", "process/materialize-source-transaction", "controller", "", "", "", "success"},
		{"STEP-REBUILD-FORGED-CURSOR-005", "rebuild/request-page", "public-call", "await-step", "", "", "success"},
		{"STEP-REBUILD-FORGED-CURSOR-006", "rebuild/request-page", "public-call", "await-call", "", "error", "success"},
	}
	var callID scenarios.NativeCallID
	for _, wanted := range expected {
		step := steps[scenarios.StepID(wanted.id)]
		if _, err := swiftScenarioOperation(steps, wanted.id, wanted.key); err != nil {
			return err
		}
		binding := step.NativeBinding
		if binding == nil || binding.Kind != wanted.kind || binding.Stage != wanted.stage || binding.Method != wanted.method || binding.Completion != wanted.completion || step.ExpectedOutcome.Disposition != wanted.disposition {
			return fmt.Errorf("Swift forged-cursor binding %s is invalid", wanted.id)
		}
		if wanted.kind == "controller" {
			continue
		}
		if err := swiftScenarioClient(step, client); err != nil {
			return err
		}
		if wanted.kind != "public-call" {
			continue
		}
		if binding.CallID == nil || *binding.CallID == "" {
			return fmt.Errorf("Swift forged-cursor binding %s has no call identity", wanted.id)
		}
		if callID == "" {
			callID = *binding.CallID
		} else if callID != *binding.CallID {
			return errors.New("Swift forged-cursor bindings do not share one public call")
		}
	}
	return nil
}

func validateForgedCursorStepWire(scenario scenarios.Scenario, stepID string, observed StepObservation) error {
	for _, expected := range scenario.WireExpectations {
		if expected.StepID != scenarios.StepID(stepID) {
			continue
		}
		if observed.Disposition != "success" || observed.Wire == nil || observed.Wire.HTTPStatus != expected.HTTPStatus || observed.Wire.Retryable != expected.Retryable || !equalOptionalStrings(observed.Wire.ErrorCode, expected.ErrorCode) {
			return fmt.Errorf("Swift forged-cursor wire result %s differs from its authored expectation", stepID)
		}
		return nil
	}
	return fmt.Errorf("Swift forged-cursor wire expectation %s is absent", stepID)
}

func captureForgedCursorServer(ctx context.Context, controller *blackbox.NativeController) (scenarios.StateFacts, error) {
	captures, err := controller.Capture(ctx, []string{"client-a"}, []string{"server-state"})
	if err != nil || len(captures) != 1 {
		return scenarios.StateFacts{}, fmt.Errorf("capture Swift forged-cursor server state: %w", err)
	}
	return captures[0].StateFacts, nil
}

func validateForgedCursorServerFreeze(before, after scenarios.StateFacts) error {
	normalizedBefore, err := scenarios.NormalizeStateFacts(before)
	if err != nil {
		return fmt.Errorf("normalize Swift forged-cursor pre-rejection state: %w", err)
	}
	normalizedAfter, err := scenarios.NormalizeStateFacts(after)
	if err != nil {
		return fmt.Errorf("normalize Swift forged-cursor post-rejection state: %w", err)
	}
	if !reflect.DeepEqual(normalizedBefore, normalizedAfter) {
		return errors.New("Swift forged continuation changed authoritative server state")
	}
	return nil
}

func validateForgedCursorTransport(scenario scenarios.Scenario, observations []transportObservation) error {
	classes := []string{"connect", "push", "rebuild", "rebuild"}
	steps := []string{"", "STEP-REBUILD-FORGED-CURSOR-003", "STEP-REBUILD-FORGED-CURSOR-005", "STEP-REBUILD-FORGED-CURSOR-006"}
	if len(observations) != len(classes) {
		return fmt.Errorf("Swift forged-cursor transport count = %d, want %d", len(observations), len(classes))
	}
	for index, class := range classes {
		observed := observations[index]
		if observed.OperationClass != class {
			return errors.New("Swift forged-cursor transport order differs from the authored call")
		}
		if index == 0 {
			if observed.StatusCode != http.StatusOK || observed.ErrorCode != nil || observed.Retryable {
				return errors.New("Swift forged-cursor setup connect did not succeed")
			}
			continue
		}
		if err := validateSwiftWireObservation(scenario, steps[index], observed); err != nil {
			return err
		}
	}
	push := observations[1]
	first := observations[2]
	forged := observations[3]
	if push.RequestFacts == nil || push.RequestFacts.ClientGeneration == nil || push.RequestFacts.MutationCount == nil || *push.RequestFacts.MutationCount != 2 {
		return errors.New("Swift forged-cursor push facts are incomplete")
	}
	if first.RequestFacts == nil || forged.RequestFacts == nil || first.RebuildResponseFacts == nil {
		return errors.New("Swift forged-cursor rebuild facts are incomplete")
	}
	firstRequest := first.RequestFacts
	forgedRequest := forged.RequestFacts
	if firstRequest.ClientGeneration == nil || forgedRequest.ClientGeneration == nil || *firstRequest.ClientGeneration != *forgedRequest.ClientGeneration || *firstRequest.ClientGeneration != *push.RequestFacts.ClientGeneration || firstRequest.SchemaVersion != forgedRequest.SchemaVersion || firstRequest.SchemaHash != forgedRequest.SchemaHash || firstRequest.ScopeFingerprint == nil || forgedRequest.ScopeFingerprint == nil || *firstRequest.ScopeFingerprint != *forgedRequest.ScopeFingerprint || firstRequest.RebuildIDFingerprint == nil || forgedRequest.RebuildIDFingerprint == nil || *firstRequest.RebuildIDFingerprint != *forgedRequest.RebuildIDFingerprint || firstRequest.Limit == nil || forgedRequest.Limit == nil || *firstRequest.Limit != 1 || *forgedRequest.Limit != 1 {
		return errors.New("Swift forged-cursor request identities are inconsistent")
	}
	if firstRequest.CursorPresent == nil || *firstRequest.CursorPresent || firstRequest.CursorFingerprint != nil || forgedRequest.CursorPresent == nil || !*forgedRequest.CursorPresent || forgedRequest.CursorFingerprint == nil || *forgedRequest.CursorFingerprint != cursorFingerprint(forgedRebuildCursor) {
		return errors.New("Swift forged-cursor request chain is invalid")
	}
	response := first.RebuildResponseFacts
	if response.RecordCount != 1 || !response.HasMore || !response.HasCursor || response.HasFinalScopeCursor || response.HasChecksum || response.ScopeFingerprint != *firstRequest.ScopeFingerprint {
		return errors.New("Swift forged-cursor first response is not an intermediate page")
	}
	if forged.RebuildResponseFacts != nil || forged.PullResponseFacts != nil {
		return errors.New("Swift forged continuation returned unauthorized response records")
	}
	return nil
}

func resolveForgedCursorIdentities(controller *blackbox.NativeController, aliases []scenarios.NativeIdentityAlias, transport []transportObservation, server scenarios.StateFacts) (map[string]json.RawMessage, []blackbox.NativeIdentityResolution, error) {
	if len(aliases) != len(forgedCursorAliasNames) || len(transport) != 4 || len(server.Rebuilds) != 1 || transport[1].RequestFacts == nil || transport[1].RequestFacts.ClientGeneration == nil || transport[2].RequestFacts == nil || transport[2].RequestFacts.RebuildIDFingerprint == nil {
		return nil, nil, errors.New("Swift forged-cursor identity evidence is incomplete")
	}
	wanted := make(map[string]struct{}, len(forgedCursorAliasNames))
	for _, alias := range forgedCursorAliasNames {
		wanted[alias] = struct{}{}
	}
	for _, alias := range aliases {
		if _, found := wanted[alias.Alias]; !found {
			return nil, nil, fmt.Errorf("Swift forged-cursor identity alias %q is unexpected", alias.Alias)
		}
		delete(wanted, alias.Alias)
	}
	if len(wanted) != 0 {
		return nil, nil, errors.New("Swift forged-cursor identity alias set is incomplete")
	}
	runtime := make(map[string]json.RawMessage, len(aliases))
	values, err := controller.IdentityValues(aliases)
	if err != nil {
		return nil, nil, err
	}
	for _, value := range values {
		runtime[value.Alias] = append(json.RawMessage(nil), value.RuntimeValue...)
	}
	generated := map[string]any{
		"client-generation-one": *transport[1].RequestFacts.ClientGeneration,
		"forged-rebuild":        server.Rebuilds[0].RebuildID,
	}
	for alias, value := range generated {
		encoded, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			return nil, nil, fmt.Errorf("encode Swift forged-cursor alias %q: %w", alias, marshalErr)
		}
		runtime[alias] = encoded
	}
	for _, alias := range forgedCursorAliasNames {
		if len(runtime[alias]) == 0 {
			return nil, nil, fmt.Errorf("Swift forged-cursor alias %q has no runtime evidence", alias)
		}
	}
	if cursorFingerprint(server.Rebuilds[0].RebuildID) != *transport[2].RequestFacts.RebuildIDFingerprint {
		return nil, nil, errors.New("Swift forged-cursor rebuild identity differs between server and transport evidence")
	}
	resolutions, err := resolveSwiftNativeIdentities(aliases, runtime)
	if err != nil {
		return nil, nil, err
	}
	return runtime, resolutions, nil
}

func validateForgedCursorState(scenario scenarios.Scenario, server scenarios.StateFacts, runtime map[string]json.RawMessage) error {
	expected, err := swiftScenarioExpectedState(scenario, "EXPECT-REBUILD-FORGED-CURSOR-STATE-001")
	if err != nil || len(expected.Rebuilds) != 1 {
		return errors.New("Swift forged-cursor authored state expectation is invalid")
	}
	var rebuildID string
	if json.Unmarshal(runtime["forged-rebuild"], &rebuildID) != nil || rebuildID == "" {
		return errors.New("Swift forged-cursor runtime rebuild identity is invalid")
	}
	expected.Rebuilds[0].RebuildID = rebuildID
	if err := validateSwiftStateProjection(expected, server); err != nil {
		return fmt.Errorf("validate Swift forged-cursor server projection: %w", err)
	}
	return nil
}
