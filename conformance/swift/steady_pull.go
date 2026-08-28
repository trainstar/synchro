package swift

import (
	"context"
	"errors"
	"fmt"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const steadyPullScenarioID = "SCN-PERF-STEADY-PULL-001"

// SteadyPullResult records direct Swift evidence for the steady-pull scenario.
type SteadyPullResult struct {
	BaselineCall SynchronizationResult
	MeasuredCall SynchronizationResult
	ClientFacts  []CaptureFacts
	ServerFacts  scenarios.StateFacts
}

// RunSteadyPullScenario executes the authored steady-pull flow through Swift.
func RunSteadyPullScenario(ctx context.Context, scenario scenarios.Scenario, controller *blackbox.NativeController, platform *Platform, client Client) (SteadyPullResult, error) {
	steps, err := swiftScenarioStepMap(scenario, steadyPullScenarioID, 8)
	if err != nil {
		return SteadyPullResult{}, err
	}
	if controller == nil || platform == nil {
		return SteadyPullResult{}, errors.New("Swift steady-pull dependencies are unavailable")
	}
	for _, id := range []string{
		"STEP-PERF-STEADY-PULL-BASELINE-REQUEST-001",
		"STEP-PERF-STEADY-PULL-001",
	} {
		if err := swiftScenarioClient(steps[scenarios.StepID(id)], client); err != nil {
			return SteadyPullResult{}, err
		}
	}
	if err := controller.Install(ctx, scenario.Model.Setup[0]); err != nil {
		return SteadyPullResult{}, fmt.Errorf("install Swift steady-pull contract: %w", err)
	}
	if err := platform.Install(ctx, client, "empty", ""); err != nil {
		return SteadyPullResult{}, fmt.Errorf("install Swift steady-pull client: %w", err)
	}

	if _, err := swiftScenarioOperation(steps, "STEP-PERF-STEADY-PULL-BASELINE-REQUEST-001", "rebuild/request-page"); err != nil {
		return SteadyPullResult{}, err
	}
	measuredPull, err := swiftScenarioOperation(steps, "STEP-PERF-STEADY-PULL-001", "pull/request-page")
	if err != nil {
		return SteadyPullResult{}, err
	}
	baseline, err := swiftScenarioCall(ctx, platform, client, "start")
	if err != nil {
		return SteadyPullResult{}, fmt.Errorf("run Swift steady-pull baseline: %w", err)
	}
	if len(baseline.transportObservations) != 3 || baseline.transportObservations[0].OperationClass != "connect" || baseline.transportObservations[1].OperationClass != "rebuild" || baseline.transportObservations[2].OperationClass != "pull" {
		return SteadyPullResult{}, errors.New("Swift steady-pull baseline did not produce connect, rebuild, and pull")
	}
	if baseline.transportObservations[1].StatusCode != 200 {
		return SteadyPullResult{}, errors.New("Swift steady-pull baseline rebuild did not succeed")
	}
	if err := validateSwiftWireExpectation(scenario, "STEP-PERF-STEADY-PULL-BASELINE-REQUEST-001", "rebuild", baseline); err != nil {
		return SteadyPullResult{}, err
	}

	commit, err := swiftScenarioOperation(steps, "STEP-PERF-STEADY-PULL-COMMIT-001", "model/commit-source-transaction")
	if err != nil {
		return SteadyPullResult{}, err
	}
	if _, err := controller.ApplyStep(ctx, commit); err != nil {
		return SteadyPullResult{}, fmt.Errorf("commit Swift steady-pull source transaction: %w", err)
	}
	materialize, err := swiftScenarioOperation(steps, "STEP-PERF-STEADY-PULL-MATERIALIZE-001", "process/materialize-source-transaction")
	if err != nil {
		return SteadyPullResult{}, err
	}
	if _, err := controller.ProcessStep(ctx, nil, materialize); err != nil {
		return SteadyPullResult{}, fmt.Errorf("materialize Swift steady-pull source transaction: %w", err)
	}

	measured, err := platform.Synchronize(ctx, client, "sync-now", RequestOperations{measuredPull})
	if err != nil {
		return SteadyPullResult{}, fmt.Errorf("run Swift measured pull: %w", err)
	}
	if measured.Completion != "idle" || len(measured.Steps) != 1 || len(measured.transportObservations) != 1 || measured.transportObservations[0].StatusCode != 200 {
		return SteadyPullResult{}, errors.New("Swift measured pull did not complete successfully")
	}
	if err := validateSwiftWireExpectation(scenario, "STEP-PERF-STEADY-PULL-001", "pull", measured); err != nil {
		return SteadyPullResult{}, err
	}

	clientFacts, err := platform.Capture(ctx, []Client{client}, []string{
		"application-rows",
		"pending-mutations",
		"rejected-mutations",
		"checkpoints",
		"provenance",
		"rebuild-state",
	})
	if err != nil {
		return SteadyPullResult{}, fmt.Errorf("capture Swift steady-pull client state: %w", err)
	}
	serverCaptures, err := controller.Capture(ctx, []string{client.Key}, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return SteadyPullResult{}, fmt.Errorf("capture Swift steady-pull server state: %w", err)
	}
	actualClient, err := mergeSwiftCaptureFacts(clientFacts)
	if err != nil {
		return SteadyPullResult{}, err
	}
	expected, err := swiftScenarioExpectedState(scenario, "EXPECT-PERF-STEADY-PULL-SEMANTIC-001")
	if err != nil {
		return SteadyPullResult{}, err
	}
	mergedFacts, err := mergeSwiftStateFacts(serverCaptures[0].StateFacts, actualClient)
	if err != nil {
		return SteadyPullResult{}, err
	}
	if err := validateSwiftStateProjection(expected, mergedFacts); err != nil {
		return SteadyPullResult{}, err
	}
	return SteadyPullResult{BaselineCall: baseline, MeasuredCall: measured, ClientFacts: clientFacts, ServerFacts: serverCaptures[0].StateFacts}, nil
}

func mergeSwiftCaptureFacts(values []CaptureFacts) (scenarios.StateFacts, error) {
	parts := make([]scenarios.StateFacts, 0, len(values))
	for _, value := range values {
		parts = append(parts, value.StateFacts)
	}
	return mergeSwiftStateFacts(parts...)
}
