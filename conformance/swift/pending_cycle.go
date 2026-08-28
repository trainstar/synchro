package swift

import (
	"context"
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
		return PendingCycleResult{}, errors.New("Swift pending push did not complete successfully")
	}
	if err := validateSwiftWireExpectation(scenario, "STEP-PERF-PENDING-CYCLE-002", "push", push); err != nil {
		return PendingCycleResult{}, err
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
	pullCall, err := platform.Synchronize(ctx, client, "sync-now", RequestOperations{pull})
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
	expected, err := swiftScenarioExpectedState(scenario, "EXPECT-PERF-PENDING-CYCLE-SEMANTIC-001")
	if err != nil {
		return PendingCycleResult{}, err
	}
	clientState, err := mergeSwiftCaptureFacts(clientFacts)
	if err != nil {
		return PendingCycleResult{}, err
	}
	actual, err := mergeSwiftStateFacts(serverCaptures[0].StateFacts, clientState)
	if err != nil {
		return PendingCycleResult{}, err
	}
	if err := validateSwiftStateProjection(expected, actual); err != nil {
		return PendingCycleResult{}, err
	}
	return PendingCycleResult{PushCall: push, PullCall: pullCall, ClientFacts: clientFacts, ServerFacts: serverCaptures[0].StateFacts}, nil
}
