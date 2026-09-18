package swift

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestSeededEmptyStartupDirectBindingGroupsRemainClosed(t *testing.T) {
	root := filepath.Join("..", "..")
	scenario, err := scenarios.LoadFile(context.Background(), root, "conformance/scenarios/performance/seeded-empty-startup-001.json")
	if err != nil {
		t.Fatalf("load seeded-startup scenario: %v", err)
	}
	steps, err := swiftScenarioStepMap(scenario, seededEmptyStartupScenarioID, 15)
	if err != nil {
		t.Fatalf("validate seeded-startup scenario: %v", err)
	}
	for _, number := range []int{3, 6, 9, 11, 13, 15} {
		id := scenarios.StepID(fmt.Sprintf("STEP-PERF-SEEDED-EMPTY-STARTUP-%03d", number))
		step := steps[id]
		if step.NativeBinding == nil || step.NativeBinding.Kind != "public-call" || step.NativeBinding.Method != "start" || step.NativeBinding.Completion != "idle" {
			t.Fatalf("seeded-startup step %s is not a synchronous start binding", id)
		}
		if scenarios.OperationKey(step.Operation) != "connect/send" {
			t.Fatalf("seeded-startup step %s operation = %s", id, scenarios.OperationKey(step.Operation))
		}
	}
}

func TestSteadyPullBaselineAcceptsAuthoredRebuildPageSequence(t *testing.T) {
	status := 200
	result := SynchronizationResult{
		Completion: "idle",
		transportObservations: []transportObservation{
			{OperationClass: "connect", StatusCode: status},
			{OperationClass: "rebuild", StatusCode: status},
			{OperationClass: "rebuild", StatusCode: status},
			{OperationClass: "pull", StatusCode: status},
		},
	}
	scenario := scenarios.Scenario{WireExpectations: []scenarios.WireExpectation{
		{StepID: "STEP-PERF-STEADY-PULL-BASELINE-REQUEST-001", HTTPStatus: status},
		{StepID: "STEP-PERF-STEADY-PULL-001", HTTPStatus: status},
	}}
	if err := validateSwiftSteadyPullBaselineWires(scenario, result); err != nil {
		t.Fatalf("validate authored steady-pull baseline page sequence: %v", err)
	}
}

func TestSteadyPullBaselineRejectsUnexpectedWireOutcome(t *testing.T) {
	result := SynchronizationResult{
		Completion: "idle",
		transportObservations: []transportObservation{
			{OperationClass: "connect", StatusCode: 200},
			{OperationClass: "rebuild", StatusCode: 500, ErrorCode: pointerString("invalid_response")},
			{OperationClass: "pull", StatusCode: 200},
		},
	}
	scenario := scenarios.Scenario{WireExpectations: []scenarios.WireExpectation{
		{StepID: "STEP-PERF-STEADY-PULL-BASELINE-REQUEST-001", HTTPStatus: 200},
		{StepID: "STEP-PERF-STEADY-PULL-001", HTTPStatus: 200},
	}}
	if err := validateSwiftSteadyPullBaselineWires(scenario, result); err == nil {
		t.Fatal("unexpected steady-pull rebuild response passed authored wire validation")
	}
}

func TestSchemaCheckBindingsFollowAuthoredWireCompletions(t *testing.T) {
	root := filepath.Join("..", "..")
	scenario, err := scenarios.LoadFile(context.Background(), root, "conformance/scenarios/performance/schema-check-001.json")
	if err != nil {
		t.Fatalf("load schema-check scenario: %v", err)
	}
	steps, err := swiftScenarioStepMap(scenario, schemaCheckScenarioID, 43)
	if err != nil {
		t.Fatalf("validate schema-check scenario: %v", err)
	}
	publicCount, err := validateSchemaCheckBindings(scenario, steps)
	if err != nil {
		t.Fatalf("validate schema-check bindings: %v", err)
	}
	if publicCount != len(scenario.WireExpectations) {
		t.Fatalf("schema-check public calls = %d, wire expectations = %d", publicCount, len(scenario.WireExpectations))
	}
	for _, step := range scenario.Steps {
		if step.NativeBinding.Kind != "public-call" {
			continue
		}
		wire, err := schemaCheckWireExpectation(scenario, step.ID)
		if err != nil {
			t.Fatalf("read schema-check wire expectation %s: %v", step.ID, err)
		}
		if step.NativeBinding.Completion != schemaCheckNativeCompletion(wire) {
			t.Fatalf("schema-check step %s completion = %q, want authored completion %q", step.ID, step.NativeBinding.Completion, schemaCheckNativeCompletion(wire))
		}
	}
}

func TestSchemaCheckUnsupportedWireDerivesErrorCompletion(t *testing.T) {
	root := filepath.Join("..", "..")
	scenario, err := scenarios.LoadFile(context.Background(), root, "conformance/scenarios/performance/schema-check-001.json")
	if err != nil {
		t.Fatalf("load schema-check scenario: %v", err)
	}
	for _, id := range []scenarios.StepID{
		"STEP-PERF-SCHEMA-CHECK-016",
		"STEP-PERF-SCHEMA-CHECK-017",
		"STEP-PERF-SCHEMA-CHECK-018",
	} {
		step, found := schemaCheckStep(scenario, id)
		if !found || step.NativeBinding == nil {
			t.Fatalf("schema-check step %s is absent", id)
		}
		wire, err := schemaCheckWireExpectation(scenario, id)
		if err != nil {
			t.Fatalf("read schema-check wire expectation %s: %v", id, err)
		}
		if wire.Action != "unsupported" || wire.HTTPStatus != 200 {
			t.Fatalf("schema-check step %s does not carry the authored unsupported 200 wire case", id)
		}
		if got := schemaCheckNativeCompletion(wire); got != "error" || step.NativeBinding.Completion != got {
			t.Fatalf("schema-check step %s completion = %q, want error from unsupported wire action", id, step.NativeBinding.Completion)
		}
	}
}

func TestSchemaCheckBindingRejectsCompletionNotDerivedFromWire(t *testing.T) {
	root := filepath.Join("..", "..")
	scenario, err := scenarios.LoadFile(context.Background(), root, "conformance/scenarios/performance/schema-check-001.json")
	if err != nil {
		t.Fatalf("load schema-check scenario: %v", err)
	}
	for index := range scenario.Steps {
		if scenario.Steps[index].ID != "STEP-PERF-SCHEMA-CHECK-016" {
			continue
		}
		binding := *scenario.Steps[index].NativeBinding
		binding.Completion = "idle"
		scenario.Steps[index].NativeBinding = &binding
	}
	steps, err := swiftScenarioStepMap(scenario, schemaCheckScenarioID, 43)
	if err != nil {
		t.Fatalf("validate mutated schema-check scenario: %v", err)
	}
	if _, err := validateSchemaCheckBindings(scenario, steps); err == nil {
		t.Fatal("schema-check binding with an idle completion for unsupported wire action passed validation")
	}
}

func schemaCheckStep(scenario scenarios.Scenario, id scenarios.StepID) (scenarios.Step, bool) {
	for _, step := range scenario.Steps {
		if step.ID == id {
			return step, true
		}
	}
	return scenarios.Step{}, false
}
