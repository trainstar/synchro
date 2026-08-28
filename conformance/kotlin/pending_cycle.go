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
	write, err := kotlinScenarioOperation(steps, "STEP-PERF-PENDING-CYCLE-001", "local/write")
	if err != nil {
		return PendingCycleResult{}, err
	}
	write, err = controller.ApplicationWrite(write)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("bind Kotlin Android pending mutation to the application schema: %w", err)
	}
	local, err := platform.ApplyStep(ctx, client, write)
	if err != nil || local.Disposition != "success" {
		return PendingCycleResult{}, fmt.Errorf("apply Kotlin Android pending mutation: %w", kotlinResultError(err, local.Disposition))
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
	snapshot, err := platform.scenarioSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Kotlin Android pending pull checkpoint: %w", err)
	}
	states, err := androidCursorScopeStates(snapshot.ScopeStates)
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
	clientFacts, err := platform.Capture(ctx, []Client{client}, []string{"application-rows", "pending-mutations", "rejected-mutations", "checkpoints", "provenance"})
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Kotlin Android pending-cycle client state: %w", err)
	}
	serverCaptures, err := controller.Capture(ctx, []string{client.Key}, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return PendingCycleResult{}, fmt.Errorf("capture Kotlin Android pending-cycle server state: %w", kotlinResultError(err, ""))
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
	return PendingCycleResult{PushCall: push, PullCall: pullCall, ClientFacts: clientFacts, ServerFacts: serverCaptures[0].StateFacts}, nil
}
