package reactnative

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestValidateSchemaCheckScenarioAcceptsAuthoredContract(t *testing.T) {
	if err := ValidateSchemaCheckScenario(loadSchemaCheckAuthoredScenario(t)); err != nil {
		t.Fatalf("validate authored schema-check scenario: %v", err)
	}
}

func TestValidateSchemaCheckScenarioRejectsContractChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*scenarios.Scenario)
	}{
		{"step order", func(scenario *scenarios.Scenario) {
			scenario.Steps[0], scenario.Steps[1] = scenario.Steps[1], scenario.Steps[0]
		}},
		{"unsupported completion", func(scenario *scenarios.Scenario) {
			for index := range scenario.Steps {
				if scenario.Steps[index].ID == "STEP-PERF-SCHEMA-CHECK-016" {
					scenario.Steps[index].NativeBinding.Completion = "idle"
				}
			}
		}},
		{"controller operation", func(scenario *scenarios.Scenario) {
			for index := range scenario.Steps {
				if scenario.Steps[index].ID == "STEP-PERF-SCHEMA-CHECK-CLASS2-PUBLISH-001" {
					scenario.Steps[index].Operation.Name = "other"
				}
			}
		}},
		{"Android proof target", func(scenario *scenarios.Scenario) {
			for index := range scenario.ProofObligations {
				if string(scenario.ProofObligations[index].ObligationID) == "OBL-PERF-SCHEMA-CHECK-RN-ANDROID-CURRENT-001" {
					scenario.ProofObligations[index].MakeTarget = "test-rn-schema-check-android"
				}
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := cloneSchemaCheckScenario(loadSchemaCheckAuthoredScenario(t))
			test.mutate(&scenario)
			if err := ValidateSchemaCheckScenario(scenario); err == nil {
				t.Fatal("changed schema-check contract was accepted")
			}
		})
	}
}

func TestSchemaCheckCallsCarryAllAuthoredDispatchSamples(t *testing.T) {
	scenario := loadSchemaCheckAuthoredScenario(t)
	calls, err := schemaCheckCalls(scenario)
	if err != nil {
		t.Fatalf("derive schema-check calls: %v", err)
	}
	// The authored scenario owns these counts. Derive them so the test cannot
	// drift from the contract it checks.
	publicSteps := 0
	for _, step := range scenario.Steps {
		if step.NativeBinding != nil && step.NativeBinding.Kind == "public-call" {
			publicSteps++
		}
	}
	if len(calls) != publicSteps || len(scenario.WireExpectations) != publicSteps {
		t.Fatalf("schema-check calls=%d wires=%d, want %d public-call steps", len(calls), len(scenario.WireExpectations), publicSteps)
	}
	plan, err := schemaCheckDispatchPlan(scenario)
	if err != nil {
		t.Fatalf("read schema-check dispatch plan: %v", err)
	}
	samples := make(map[string]int, len(plan.Strata))
	measured := 0
	for _, call := range calls {
		if call.step.MeasurementSample != nil {
			measured++
			samples[string(call.step.MeasurementSample.StratumID)]++
		}
	}
	if measured != 18 {
		t.Fatalf("schema-check measured calls=%d want=18", measured)
	}
	for _, stratum := range plan.Strata {
		if samples[string(stratum.StratumID)] != 3 {
			t.Fatalf("schema-check stratum=%s samples=%d want=3", stratum.StratumID, samples[string(stratum.StratumID)])
		}
	}
	if len(scenario.NativeLifecycleBoundaries) != 18 {
		t.Fatalf("schema-check lifecycle boundaries=%d want=18", len(scenario.NativeLifecycleBoundaries))
	}
	publicCalls := make(map[scenarios.StepID]schemaCheckCall, len(calls))
	for _, call := range calls {
		publicCalls[call.step.ID] = call
	}
	for _, boundary := range scenario.NativeLifecycleBoundaries {
		call, found := publicCalls[boundary.AfterStepID]
		if !found || boundary.UserID != call.step.NativeBinding.UserID || boundary.ClientID != call.step.NativeBinding.ClientID {
			t.Fatalf("schema-check lifecycle boundary=%s step=%s is not bound to its public call", boundary.ID, boundary.AfterStepID)
		}
	}
}

func TestNewSchemaCheckCoordinatorKeepsAndroidSidecarOnHostLoopback(t *testing.T) {
	scenario := loadSchemaCheckAuthoredScenario(t)
	coordinator, err := NewSchemaCheckCoordinator(SchemaCheckCoordinatorConfig{
		Scenario: scenario, Platform: "android", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil || coordinator == nil {
		t.Fatalf("Android schema-check coordinator was rejected: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	if !strings.HasPrefix(coordinator.URL(), "http://127.0.0.1:") {
		t.Fatalf("Android schema-check coordinator URL=%q", coordinator.URL())
	}
	if !strings.HasPrefix(coordinator.adapter, "http://10.0.2.2:") {
		t.Fatalf("Android schema-check adapter URL=%q", coordinator.adapter)
	}
	// One exchange opens, synchronizes, and captures each authored call, plus
	// one for each lifecycle boundary and one terminal exchange.
	calls, err := schemaCheckCalls(scenario)
	if err != nil {
		t.Fatalf("derive schema-check calls: %v", err)
	}
	want := len(calls)*3 + len(scenario.NativeLifecycleBoundaries) + 1
	if coordinator.ExchangeCount() != want {
		t.Fatalf("schema-check exchange count=%d want=%d", coordinator.ExchangeCount(), want)
	}
}

func TestSchemaCheckCommandEncodesEmptyStepsAsArray(t *testing.T) {
	coordinator, err := NewSchemaCheckCoordinator(SchemaCheckCoordinatorConfig{
		Scenario: loadSchemaCheckAuthoredScenario(t), Platform: "ios", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create schema-check coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	command := coordinator.command(coordinator.calls[0], "client", "open", map[string]any{"client_key": coordinator.calls[0].sessionKey}, nil)
	encoded, err := json.Marshal(command)
	if err != nil || command.Action.Steps == nil || len(command.Action.Steps) != 0 || !strings.Contains(string(encoded), `"steps":[]`) {
		t.Fatalf("schema-check empty command steps=%#v encoded=%s error=%v", command.Action.Steps, encoded, err)
	}
}

func TestSchemaCheckDistinctRebuildCountIgnoresPageReceipts(t *testing.T) {
	server := scenarios.StateFacts{Rebuilds: []scenarios.RebuildFact{
		{UserID: "user-a", ClientID: "client-a", RebuildID: "rebuild-one", PageCount: 2},
		{UserID: "user-a", ClientID: "client-a", RebuildID: "rebuild-one", PageCount: 1},
		{UserID: "user-a", ClientID: "client-a", RebuildID: "rebuild-two", PageCount: 1},
	}}
	if got := schemaCheckDistinctRebuildCount(server, "user-a", "client-a"); got != 2 {
		t.Fatalf("distinct schema-check rebuilds=%d want=2", got)
	}
}

func loadSchemaCheckAuthoredScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	scenario, err := LoadSchemaCheckScenario(context.Background(), repoRoot)
	if err != nil {
		t.Fatalf("load authored schema-check scenario: %v", err)
	}
	return scenario
}

func cloneSchemaCheckScenario(scenario scenarios.Scenario) scenarios.Scenario {
	raw, err := json.Marshal(scenario)
	if err != nil {
		panic(err)
	}
	var clone scenarios.Scenario
	if err := json.Unmarshal(raw, &clone); err != nil {
		panic(err)
	}
	return clone
}
