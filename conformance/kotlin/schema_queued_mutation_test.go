package kotlin

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestSchemaQueuedMutationBindingsFollowAuthoredWireCompletions(t *testing.T) {
	scenario := loadSchemaQueuedMutationScenario(t)
	steps, err := kotlinScenarioStepMap(scenario, schemaQueuedMutationScenarioID, 12)
	if err != nil {
		t.Fatalf("validate schema-queued-mutation scenario: %v", err)
	}
	client := Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "schema-queued-mutation-client-a"}
	if err := validateSchemaQueuedMutationBindings(scenario, steps, client); err != nil {
		t.Fatalf("validate schema-queued-mutation bindings: %v", err)
	}
	unsupported, found := schemaQueuedMutationStep(scenario, "STEP-SCHEMA-QUEUED-MUTATION-UNSUPPORTED-001")
	if !found {
		t.Fatal("authored unsupported step is absent")
	}
	completion, err := schemaQueuedMutationCompletion(scenario, unsupported)
	if err != nil || completion != "error" {
		t.Fatalf("unsupported completion = %q, want error", completion)
	}
}

func TestSchemaQueuedMutationBindingsRejectUnsupportedIdleCompletion(t *testing.T) {
	scenario := loadSchemaQueuedMutationScenario(t)
	for index := range scenario.Steps {
		if scenario.Steps[index].ID != "STEP-SCHEMA-QUEUED-MUTATION-UNSUPPORTED-001" {
			continue
		}
		binding := *scenario.Steps[index].NativeBinding
		binding.Completion = "idle"
		scenario.Steps[index].NativeBinding = &binding
	}
	steps, err := kotlinScenarioStepMap(scenario, schemaQueuedMutationScenarioID, 12)
	if err != nil {
		t.Fatalf("validate mutated schema-queued-mutation scenario: %v", err)
	}
	client := Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "schema-queued-mutation-client-a"}
	if err := validateSchemaQueuedMutationBindings(scenario, steps, client); err == nil {
		t.Fatal("unsupported wire action with idle completion passed binding validation")
	}
}

func loadSchemaQueuedMutationScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	scenario, err := scenarios.LoadFile(context.Background(), filepath.Join("..", ".."), "conformance/scenarios/server/schema-queued-mutation-001.json")
	if err != nil {
		t.Fatalf("load schema-queued-mutation scenario: %v", err)
	}
	return scenario
}
