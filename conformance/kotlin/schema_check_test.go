package kotlin

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestSchemaCheckBindingsFollowAuthoredWireCompletions(t *testing.T) {
	scenario := loadSchemaCheckScenario(t)
	steps, err := kotlinScenarioStepMap(scenario, schemaCheckScenarioID, 43)
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
	scenario := loadSchemaCheckScenario(t)
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
	scenario := loadSchemaCheckScenario(t)
	for index := range scenario.Steps {
		if scenario.Steps[index].ID != "STEP-PERF-SCHEMA-CHECK-016" {
			continue
		}
		binding := *scenario.Steps[index].NativeBinding
		binding.Completion = "idle"
		scenario.Steps[index].NativeBinding = &binding
	}
	steps, err := kotlinScenarioStepMap(scenario, schemaCheckScenarioID, 43)
	if err != nil {
		t.Fatalf("validate mutated schema-check scenario: %v", err)
	}
	if _, err := validateSchemaCheckBindings(scenario, steps); err == nil {
		t.Fatal("schema-check binding with an idle completion for unsupported wire action passed validation")
	}
}

func loadSchemaCheckScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	scenario, err := scenarios.LoadFile(context.Background(), filepath.Join("..", ".."), "conformance/scenarios/performance/schema-check-001.json")
	if err != nil {
		t.Fatalf("load schema-check scenario: %v", err)
	}
	return scenario
}

func schemaCheckStep(scenario scenarios.Scenario, id scenarios.StepID) (scenarios.Step, bool) {
	for _, step := range scenario.Steps {
		if step.ID == id {
			return step, true
		}
	}
	return scenarios.Step{}, false
}
