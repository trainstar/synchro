package kotlin

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

// ForgedCursorResult records direct Kotlin Android evidence for one rejected forged continuation.
type ForgedCursorResult struct {
	Call               ClientCallResult
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
}

// RunForgedCursorScenario executes the authored forged rebuild-continuation flow through Kotlin Android.
func RunForgedCursorScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, artifact *blackbox.NativeArtifact, platform *Platform, client Client) (ForgedCursorResult, error) {
	steps, err := kotlinScenarioStepMap(scenario, forgedCursorScenarioID, 6)
	if err != nil {
		return ForgedCursorResult{}, err
	}
	if controller == nil || artifact == nil || platform == nil {
		return ForgedCursorResult{}, errors.New("Kotlin Android forged-cursor dependencies are unavailable")
	}
	if err := validateKotlinForgedCursorBindings(steps, client); err != nil {
		return ForgedCursorResult{}, err
	}

	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return ForgedCursorResult{}, fmt.Errorf("install Kotlin Android forged-cursor contract: %w", err)
	}
	seedPath, err := artifact.StageCurrentSeed(ctx, client.UserID, client.ClientID, scenarios.StepID("STEP-REBUILD-FORGED-CURSOR-001"))
	if err != nil {
		return ForgedCursorResult{}, fmt.Errorf("stage Kotlin Android forged-cursor current seed: %w", err)
	}
	if err := platform.Install(ctx, InstallRequest{Client: client, Initialization: "seed", SeedPath: seedPath}); err != nil {
		return ForgedCursorResult{}, fmt.Errorf("install Kotlin Android forged-cursor client: %w", err)
	}

	for _, id := range []string{"STEP-REBUILD-FORGED-CURSOR-001", "STEP-REBUILD-FORGED-CURSOR-002"} {
		write, err := kotlinScenarioOperation(steps, id, "local/write")
		if err != nil {
			return ForgedCursorResult{}, err
		}
		write, err = controller.ApplicationWrite(write)
		if err != nil {
			return ForgedCursorResult{}, fmt.Errorf("bind Kotlin Android forged-cursor local write %s: %w", id, err)
		}
		observed, applyErr := platform.ApplyStep(ctx, client, write)
		if applyErr != nil || observed.Disposition != "success" {
			return ForgedCursorResult{}, fmt.Errorf("apply Kotlin Android forged-cursor local write %s: %w", id, kotlinResultError(applyErr, observed.Disposition))
		}
	}

	push, err := kotlinScenarioOperation(steps, "STEP-REBUILD-FORGED-CURSOR-003", "push/submit")
	if err != nil {
		return ForgedCursorResult{}, err
	}
	materialize, err := kotlinScenarioOperation(steps, "STEP-REBUILD-FORGED-CURSOR-004", "process/materialize-source-transaction")
	if err != nil {
		return ForgedCursorResult{}, err
	}
	firstPage, err := kotlinScenarioOperation(steps, "STEP-REBUILD-FORGED-CURSOR-005", "rebuild/request-page")
	if err != nil {
		return ForgedCursorResult{}, err
	}
	forgedPage, err := kotlinScenarioOperation(steps, "STEP-REBUILD-FORGED-CURSOR-006", "rebuild/request-page")
	if err != nil {
		return ForgedCursorResult{}, err
	}
	callID := string(*steps[scenarios.StepID("STEP-REBUILD-FORGED-CURSOR-003")].NativeBinding.CallID)

	state, err := platform.clientFor(client)
	if err != nil {
		return ForgedCursorResult{}, err
	}
	state.mu.Lock()
	transportCheckpoint := state.session.Checkpoint()
	state.mu.Unlock()

	begin, err := platform.BeginCall(ctx, CallRequest{Client: client, CallID: callID, Method: "start", Operations: []scenarios.Operation{push}})
	if err != nil {
		return ForgedCursorResult{}, fmt.Errorf("begin Kotlin Android forged-cursor call: %w", err)
	}
	if begin.CallID != callID || begin.State != "in_flight" || begin.Completion != "" || len(begin.Steps) != 1 {
		return ForgedCursorResult{}, errors.New("Kotlin Android forged-cursor push did not enter the staged call")
	}
	if err := validateKotlinForgedCursorStepWire(scenario, "STEP-REBUILD-FORGED-CURSOR-003", begin.Steps[0]); err != nil {
		return ForgedCursorResult{}, err
	}
	if err := controller.BindApplicationPush(push); err != nil {
		return ForgedCursorResult{}, fmt.Errorf("bind Kotlin Android forged-cursor push transaction: %w", err)
	}
	if observed, processErr := controller.ProcessStep(ctx, nil, materialize); processErr != nil || observed.Disposition != "success" {
		return ForgedCursorResult{}, fmt.Errorf("materialize Kotlin Android forged-cursor push: %w", kotlinResultError(processErr, observed.Disposition))
	}

	first, err := platform.AwaitStep(ctx, AwaitRequest{Client: client, CallID: callID, Operation: firstPage})
	if err != nil {
		return ForgedCursorResult{}, fmt.Errorf("await Kotlin Android forged-cursor first page: %w", err)
	}
	if err := validateKotlinForgedCursorStepWire(scenario, "STEP-REBUILD-FORGED-CURSOR-005", first); err != nil {
		return ForgedCursorResult{}, err
	}
	serverBefore, err := captureKotlinForgedCursorServer(ctx, controller)
	if err != nil {
		return ForgedCursorResult{}, err
	}

	// AwaitStep installs the deterministic override only for the authored forged cursor source.
	forged, err := platform.AwaitStep(ctx, AwaitRequest{Client: client, CallID: callID, Operation: forgedPage})
	if err != nil {
		return ForgedCursorResult{}, fmt.Errorf("await Kotlin Android forged continuation rejection: %w", err)
	}
	if err := validateKotlinForgedCursorStepWire(scenario, "STEP-REBUILD-FORGED-CURSOR-006", forged); err != nil {
		return ForgedCursorResult{}, err
	}
	serverAfter, err := captureKotlinForgedCursorServer(ctx, controller)
	if err != nil {
		return ForgedCursorResult{}, err
	}
	if err := validateKotlinForgedCursorServerFreeze(serverBefore, serverAfter); err != nil {
		return ForgedCursorResult{}, err
	}

	completed, err := platform.AwaitCall(ctx, CallRequest{Client: client, CallID: callID})
	if err != nil {
		return ForgedCursorResult{}, fmt.Errorf("complete Kotlin Android forged-cursor call: %w", err)
	}
	if completed.CallID != callID || completed.State != "completed" || completed.Completion != "error" {
		return ForgedCursorResult{}, errors.New("Kotlin Android forged-cursor call did not complete with an error")
	}
	state.mu.Lock()
	transport, transportErr := state.session.ObservationsAfter(transportCheckpoint)
	state.mu.Unlock()
	if transportErr != nil {
		return ForgedCursorResult{}, fmt.Errorf("capture Kotlin Android forged-cursor transport: %w", transportErr)
	}
	if err := validateKotlinForgedCursorTransport(scenario, transport); err != nil {
		return ForgedCursorResult{}, err
	}
	runtime, resolutions, err := resolveKotlinForgedCursorIdentities(controller, scenario.NativeIdentityAliases, transport, serverAfter)
	if err != nil {
		return ForgedCursorResult{}, err
	}
	if err := validateKotlinForgedCursorState(scenario, serverAfter, runtime); err != nil {
		return ForgedCursorResult{}, err
	}
	return ForgedCursorResult{Call: completed, ServerFacts: serverAfter, IdentityResolution: resolutions}, nil
}

func validateKotlinForgedCursorBindings(steps map[scenarios.StepID]scenarios.Step, client Client) error {
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
		if _, err := kotlinScenarioOperation(steps, wanted.id, wanted.key); err != nil {
			return err
		}
		binding := step.NativeBinding
		if binding == nil || binding.Kind != wanted.kind || binding.Stage != wanted.stage || binding.Method != wanted.method || binding.Completion != wanted.completion || step.ExpectedOutcome.Disposition != wanted.disposition {
			return fmt.Errorf("Kotlin Android forged-cursor binding %s is invalid", wanted.id)
		}
		if wanted.kind == "controller" {
			continue
		}
		if err := kotlinScenarioClient(step, client); err != nil {
			return err
		}
		if wanted.kind != "public-call" {
			continue
		}
		if binding.CallID == nil || *binding.CallID == "" {
			return fmt.Errorf("Kotlin Android forged-cursor binding %s has no call identity", wanted.id)
		}
		if callID == "" {
			callID = *binding.CallID
		} else if callID != *binding.CallID {
			return errors.New("Kotlin Android forged-cursor bindings do not share one public call")
		}
	}
	return nil
}

func validateKotlinForgedCursorStepWire(scenario scenarios.Scenario, stepID string, observed StepObservation) error {
	for _, expected := range scenario.WireExpectations {
		if expected.StepID != scenarios.StepID(stepID) {
			continue
		}
		if observed.Disposition != "success" || observed.Wire == nil || observed.Wire.HTTPStatus != expected.HTTPStatus || observed.Wire.Retryable != expected.Retryable || !equalKotlinOptionalStrings(observed.Wire.ErrorCode, expected.ErrorCode) {
			return fmt.Errorf("Kotlin Android forged-cursor wire result %s differs from its authored expectation", stepID)
		}
		return nil
	}
	return fmt.Errorf("Kotlin Android forged-cursor wire expectation %s is absent", stepID)
}

func captureKotlinForgedCursorServer(ctx context.Context, controller *blackbox.NativeController) (scenarios.StateFacts, error) {
	captures, err := controller.Capture(ctx, []string{"client-a"}, []string{"server-state"})
	if err != nil || len(captures) != 1 {
		return scenarios.StateFacts{}, fmt.Errorf("capture Kotlin Android forged-cursor server state: %w", kotlinResultError(err, ""))
	}
	return captures[0].StateFacts, nil
}

func validateKotlinForgedCursorServerFreeze(before, after scenarios.StateFacts) error {
	normalizedBefore, err := scenarios.NormalizeStateFacts(before)
	if err != nil {
		return fmt.Errorf("normalize Kotlin Android forged-cursor pre-rejection state: %w", err)
	}
	normalizedAfter, err := scenarios.NormalizeStateFacts(after)
	if err != nil {
		return fmt.Errorf("normalize Kotlin Android forged-cursor post-rejection state: %w", err)
	}
	if !reflect.DeepEqual(normalizedBefore, normalizedAfter) {
		return errors.New("Kotlin Android forged continuation changed authoritative server state")
	}
	return nil
}

func validateKotlinForgedCursorTransport(scenario scenarios.Scenario, observations []TransportObservation) error {
	classes := []string{"connect", "push", "rebuild", "rebuild"}
	steps := []string{"", "STEP-REBUILD-FORGED-CURSOR-003", "STEP-REBUILD-FORGED-CURSOR-005", "STEP-REBUILD-FORGED-CURSOR-006"}
	if len(observations) != len(classes) {
		return fmt.Errorf("Kotlin Android forged-cursor transport count = %d, want %d", len(observations), len(classes))
	}
	for index, class := range classes {
		observed := observations[index]
		if observed.OperationClass != class {
			return errors.New("Kotlin Android forged-cursor transport order differs from the authored call")
		}
		if index == 0 {
			if observed.StatusCode != http.StatusOK || observed.ErrorCode != nil || observed.Retryable == nil || *observed.Retryable {
				return errors.New("Kotlin Android forged-cursor setup connect did not succeed")
			}
			continue
		}
		if err := validateKotlinWireObservation(scenario, steps[index], observed); err != nil {
			return err
		}
	}
	push := observations[1]
	first := observations[2]
	forged := observations[3]
	if push.RequestFacts == nil || push.RequestFacts.ClientGeneration == nil || push.RequestFacts.MutationCount == nil || *push.RequestFacts.MutationCount != 2 {
		return errors.New("Kotlin Android forged-cursor push facts are incomplete")
	}
	if first.RequestFacts == nil || forged.RequestFacts == nil || first.RebuildResponseFacts == nil {
		return errors.New("Kotlin Android forged-cursor rebuild facts are incomplete")
	}
	firstRequest := first.RequestFacts
	forgedRequest := forged.RequestFacts
	if firstRequest.ClientGeneration == nil || forgedRequest.ClientGeneration == nil || *firstRequest.ClientGeneration != *forgedRequest.ClientGeneration || *firstRequest.ClientGeneration != *push.RequestFacts.ClientGeneration || firstRequest.SchemaVersion != forgedRequest.SchemaVersion || firstRequest.SchemaHash != forgedRequest.SchemaHash || firstRequest.ScopeFingerprint == nil || forgedRequest.ScopeFingerprint == nil || *firstRequest.ScopeFingerprint != *forgedRequest.ScopeFingerprint || firstRequest.RebuildIDFingerprint == nil || forgedRequest.RebuildIDFingerprint == nil || *firstRequest.RebuildIDFingerprint != *forgedRequest.RebuildIDFingerprint || firstRequest.Limit == nil || forgedRequest.Limit == nil || *firstRequest.Limit != 1 || *forgedRequest.Limit != 1 {
		return errors.New("Kotlin Android forged-cursor request identities are inconsistent")
	}
	if firstRequest.CursorPresent == nil || *firstRequest.CursorPresent || firstRequest.CursorFingerprint != nil || forgedRequest.CursorPresent == nil || !*forgedRequest.CursorPresent || forgedRequest.CursorFingerprint == nil || *forgedRequest.CursorFingerprint != cursorFingerprint(forgedRebuildCursor) {
		return errors.New("Kotlin Android forged-cursor request chain is invalid")
	}
	response := first.RebuildResponseFacts
	if response.RecordCount != 1 || !response.HasMore || !response.HasCursor || response.HasFinalScopeCursor || response.HasChecksum || response.ScopeFingerprint != *firstRequest.ScopeFingerprint {
		return errors.New("Kotlin Android forged-cursor first response is not an intermediate page")
	}
	if forged.RebuildResponseFacts != nil || forged.PullResponseFacts != nil {
		return errors.New("Kotlin Android forged continuation returned unauthorized response records")
	}
	return nil
}

func resolveKotlinForgedCursorIdentities(controller *blackbox.NativeController, aliases []scenarios.NativeIdentityAlias, transport []TransportObservation, server scenarios.StateFacts) (map[string]json.RawMessage, []blackbox.NativeIdentityResolution, error) {
	if len(aliases) != len(forgedCursorAliasNames) || len(transport) != 4 || len(server.Rebuilds) != 1 || transport[1].RequestFacts == nil || transport[1].RequestFacts.ClientGeneration == nil || transport[2].RequestFacts == nil || transport[2].RequestFacts.RebuildIDFingerprint == nil {
		return nil, nil, errors.New("Kotlin Android forged-cursor identity evidence is incomplete")
	}
	wanted := make(map[string]struct{}, len(forgedCursorAliasNames))
	for _, alias := range forgedCursorAliasNames {
		wanted[alias] = struct{}{}
	}
	for _, alias := range aliases {
		if _, found := wanted[alias.Alias]; !found {
			return nil, nil, fmt.Errorf("Kotlin Android forged-cursor identity alias %q is unexpected", alias.Alias)
		}
		delete(wanted, alias.Alias)
	}
	if len(wanted) != 0 {
		return nil, nil, errors.New("Kotlin Android forged-cursor identity alias set is incomplete")
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
			return nil, nil, fmt.Errorf("encode Kotlin Android forged-cursor alias %q: %w", alias, marshalErr)
		}
		runtime[alias] = encoded
	}
	for _, alias := range forgedCursorAliasNames {
		if len(runtime[alias]) == 0 {
			return nil, nil, fmt.Errorf("Kotlin Android forged-cursor alias %q has no runtime evidence", alias)
		}
	}
	if cursorFingerprint(server.Rebuilds[0].RebuildID) != *transport[2].RequestFacts.RebuildIDFingerprint {
		return nil, nil, errors.New("Kotlin Android forged-cursor rebuild identity differs between server and transport evidence")
	}
	resolutions, err := resolveKotlinNativeIdentities(aliases, runtime)
	if err != nil {
		return nil, nil, err
	}
	return runtime, resolutions, nil
}

func validateKotlinForgedCursorState(scenario scenarios.Scenario, server scenarios.StateFacts, runtime map[string]json.RawMessage) error {
	expected, err := kotlinScenarioExpectedState(scenario, "EXPECT-REBUILD-FORGED-CURSOR-STATE-001")
	if err != nil || len(expected.Rebuilds) != 1 {
		return errors.New("Kotlin Android forged-cursor authored state expectation is invalid")
	}
	var rebuildID string
	if json.Unmarshal(runtime["forged-rebuild"], &rebuildID) != nil || rebuildID == "" {
		return errors.New("Kotlin Android forged-cursor runtime rebuild identity is invalid")
	}
	expected.Rebuilds[0].RebuildID = rebuildID
	if err := validateKotlinStateProjection(expected, server); err != nil {
		return fmt.Errorf("validate Kotlin Android forged-cursor server projection: %w", err)
	}
	return nil
}
