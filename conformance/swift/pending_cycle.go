package swift

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const pendingCycleScenarioID = "SCN-PERF-PENDING-CYCLE-001"

// PendingCycleResult records direct Swift evidence for one pending mutation cycle.
type PendingCycleResult struct {
	PushCall    SynchronizationResult
	PullCall    SynchronizationResult
	ClientFacts []CaptureFacts
	ServerFacts scenarios.StateFacts
}

// RunPendingCycleScenario executes the authored pending-cycle flow through Swift.
func RunPendingCycleScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, platform *Platform, client Client) (PendingCycleResult, error) {
	steps, err := swiftScenarioStepMap(scenario, pendingCycleScenarioID, 4)
	if err != nil {
		return PendingCycleResult{}, err
	}
	if controller == nil || platform == nil {
		return PendingCycleResult{}, errors.New("Swift pending-cycle dependencies are unavailable")
	}
	for _, id := range []string{
		"STEP-PERF-PENDING-CYCLE-001",
		"STEP-PERF-PENDING-CYCLE-002",
		"STEP-PERF-PENDING-CYCLE-003",
	} {
		if err := swiftScenarioClient(steps[scenarios.StepID(id)], client); err != nil {
			return PendingCycleResult{}, err
		}
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return PendingCycleResult{}, fmt.Errorf("install Swift pending-cycle contract: %w", err)
	}
	if err := platform.Install(ctx, client, "current", ""); err != nil {
		return PendingCycleResult{}, fmt.Errorf("install Swift pending-cycle client: %w", err)
	}

	write, err := swiftScenarioOperation(steps, "STEP-PERF-PENDING-CYCLE-001", "local/write")
	if err != nil {
		return PendingCycleResult{}, err
	}
	write, err = controller.ApplicationWrite(write)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("bind Swift pending mutation to the application schema: %w", err)
	}
	local, err := platform.ApplyStep(ctx, client, write)
	if err != nil || local.Disposition != "success" {
		return PendingCycleResult{}, fmt.Errorf("apply Swift pending mutation: %w", resultError(err, local.Disposition))
	}

	push, err := swiftScenarioCall(ctx, platform, client, "start")
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("run Swift pending push: %w", err)
	}
	pushObservation, err := swiftScenarioWire(push, "push")
	if err != nil {
		return PendingCycleResult{}, err
	}
	if push.Completion != "idle" || pushObservation.StatusCode != 200 || pushObservation.Retryable {
		// The observed values separate a push the server rejected from a push
		// the client left in durable backoff.
		state, stateErr := platform.client(client)
		report := "client unavailable"
		if stateErr == nil {
			report = state.session.stderrReport()
		}
		return PendingCycleResult{}, fmt.Errorf(
			"Swift pending push did not complete successfully: completion %q, status %d, retryable %t, error code %s (runner reported: %s)",
			push.Completion, pushObservation.StatusCode, pushObservation.Retryable, optionalStringOrNone(pushObservation.ErrorCode), report)
	}
	if err := validateSwiftWireExpectation(scenario, "STEP-PERF-PENDING-CYCLE-002", "push", push); err != nil {
		return PendingCycleResult{}, err
	}
	authoredPush, err := swiftScenarioOperation(steps, "STEP-PERF-PENDING-CYCLE-002", "push/submit")
	if err != nil {
		return PendingCycleResult{}, err
	}
	if err := controller.BindApplicationPush(authoredPush); err != nil {
		return PendingCycleResult{}, fmt.Errorf("bind Swift pending push transaction: %w", err)
	}

	materialize, err := swiftScenarioOperation(steps, "STEP-PERF-PENDING-CYCLE-MATERIALIZE-001", "process/materialize-source-transaction")
	if err != nil {
		return PendingCycleResult{}, err
	}
	if _, err := controller.ProcessStep(ctx, nil, materialize); err != nil {
		return PendingCycleResult{}, fmt.Errorf("materialize Swift pending mutation: %w", err)
	}
	pull, err := swiftScenarioOperation(steps, "STEP-PERF-PENDING-CYCLE-003", "pull/request-page")
	if err != nil {
		return PendingCycleResult{}, err
	}
	snapshot, err := platform.captureSnapshot(ctx, client)
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Swift pending pull checkpoint: %w", err)
	}
	if len(snapshot.ScopeStates) != 1 || snapshot.ScopeStates[0].Cursor == nil || *snapshot.ScopeStates[0].Cursor == "" {
		scopes := make([]string, 0, len(snapshot.ScopeStates))
		for _, scope := range snapshot.ScopeStates {
			scopes = append(scopes, scope.ScopeID+":"+optionalStringOrNone(scope.Cursor))
		}
		state, stateErr := platform.client(client)
		report := "client unavailable"
		if stateErr == nil {
			report = state.session.stderrReport()
		}
		return PendingCycleResult{}, fmt.Errorf("Swift pending pull checkpoint is invalid: scopes %v (runner reported: %s)", scopes, report)
	}
	var runtimePullPayload map[string]any
	if err := json.Unmarshal(pull.Payload, &runtimePullPayload); err != nil {
		return PendingCycleResult{}, errors.New("decode Swift pending pull runtime binding failed")
	}
	rawScopes, ok := runtimePullPayload["scopes"].([]any)
	if !ok || len(rawScopes) != 1 {
		return PendingCycleResult{}, errors.New("Swift pending pull scope binding is invalid")
	}
	for _, rawScope := range rawScopes {
		scope, ok := rawScope.(map[string]any)
		if !ok || scope["cursor_source"] != "none" {
			return PendingCycleResult{}, errors.New("Swift pending pull authored cursor source is invalid")
		}
		scope["cursor_source"] = "local_checkpoint"
	}
	runtimePull := pull
	runtimePull.Payload, err = json.Marshal(runtimePullPayload)
	if err != nil || scenarios.ValidateOperation(runtimePull) != nil {
		return PendingCycleResult{}, errors.New("encode Swift pending pull runtime binding failed")
	}
	pullCall, err := platform.Synchronize(ctx, client, "sync-now", RequestOperations{runtimePull})
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("run Swift pending pull: %w", err)
	}
	if pullCall.Completion != "idle" || len(pullCall.Steps) != 1 || len(pullCall.transportObservations) != 1 || pullCall.transportObservations[0].StatusCode != 200 {
		return PendingCycleResult{}, errors.New("Swift pending pull did not complete successfully")
	}
	if err := validateSwiftWireExpectation(scenario, "STEP-PERF-PENDING-CYCLE-003", "pull", pullCall); err != nil {
		return PendingCycleResult{}, err
	}

	clientFacts, err := platform.Capture(ctx, []Client{client}, []string{"application-rows", "pending-mutations", "rejected-mutations", "checkpoints", "provenance"})
	if err != nil {
		return PendingCycleResult{}, fmt.Errorf("capture Swift pending-cycle client state: %w", err)
	}
	serverCaptures, err := controller.Capture(ctx, []string{client.Key}, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return PendingCycleResult{}, fmt.Errorf("capture Swift pending-cycle server state: %w", err)
	}
	wireExpectationFound := false
	for _, expectation := range scenario.Model.ExpectedState {
		if expectation.ID != scenarios.ExpectationID("EXPECT-PERF-PENDING-CYCLE-SEMANTIC-001") {
			continue
		}
		var payload map[string]any
		if expectation.Predicate.ContractPredicate != "wire-outcome" || expectation.Predicate.Name != "canonical-wire-outcome" || expectation.StateFacts != nil || json.Unmarshal(expectation.Predicate.Payload, &payload) != nil || len(payload) != 0 {
			return PendingCycleResult{}, errors.New("Swift pending-cycle canonical wire expectation is invalid")
		}
		wireExpectationFound = true
	}
	if !wireExpectationFound {
		return PendingCycleResult{}, errors.New("Swift pending-cycle canonical wire expectation is absent")
	}
	return PendingCycleResult{PushCall: push, PullCall: pullCall, ClientFacts: clientFacts, ServerFacts: serverCaptures[0].StateFacts}, nil
}
