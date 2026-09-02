package reactnative

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestValidateSchemaQueuedMutationScenarioAcceptsAuthoredContract(t *testing.T) {
	if err := ValidateSchemaQueuedMutationScenario(loadSchemaQueuedMutationAuthoredScenario(t)); err != nil {
		t.Fatalf("validate authored schema-queued-mutation scenario: %v", err)
	}
}

func TestValidateSchemaQueuedMutationScenarioRejectsContractChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*scenarios.Scenario)
	}{
		{"step order", func(scenario *scenarios.Scenario) {
			scenario.Steps[0], scenario.Steps[1] = scenario.Steps[1], scenario.Steps[0]
		}},
		{"unsupported completion", func(scenario *scenarios.Scenario) {
			for index := range scenario.Steps {
				if scenario.Steps[index].ID == "STEP-SCHEMA-QUEUED-MUTATION-UNSUPPORTED-001" {
					binding := *scenario.Steps[index].NativeBinding
					binding.Completion = "blocked"
					scenario.Steps[index].NativeBinding = &binding
				}
			}
		}},
		{"lifecycle boundary", func(scenario *scenarios.Scenario) {
			scenario.NativeLifecycleBoundaries = append(scenario.NativeLifecycleBoundaries, scenarios.NativeLifecycleBoundary{ID: "unexpected"})
		}},
		{"Android proof target", func(scenario *scenarios.Scenario) {
			for index := range scenario.ProofObligations {
				if string(scenario.ProofObligations[index].ObligationID) == "OBL-SCHEMA-QUEUED-MUTATION-RN-ANDROID-CURRENT-001" {
					scenario.ProofObligations[index].MakeTarget = "test-rn-schema-queued-mutation-android"
				}
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := cloneSchemaQueuedMutationScenario(loadSchemaQueuedMutationAuthoredScenario(t))
			test.mutate(&scenario)
			if err := ValidateSchemaQueuedMutationScenario(scenario); err == nil {
				t.Fatal("changed schema-queued-mutation contract was accepted")
			}
		})
	}
}

func TestSchemaQueuedMutationCommandUsesEmptyStepsArray(t *testing.T) {
	coordinator, err := NewSchemaQueuedMutationCoordinator(SchemaQueuedMutationCoordinatorConfig{
		Scenario: loadSchemaQueuedMutationAuthoredScenario(t), Platform: "ios", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token", AppVersion: "0.3.0",
	})
	if err != nil {
		t.Fatalf("create schema-queued-mutation coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	command := coordinator.command(schemaQueuedMutationInitialClientKey, "client", "open", map[string]any{"client_key": schemaQueuedMutationInitialClientKey}, nil)
	encoded, err := json.Marshal(command)
	if err != nil {
		t.Fatalf("encode schema-queued-mutation command: %v", err)
	}
	if command.Action.Steps == nil || !strings.Contains(string(encoded), `"steps":[]`) {
		t.Fatalf("schema-queued-mutation command steps=%#v encoded=%s", command.Action.Steps, encoded)
	}
}

func TestSchemaQueuedMutationFinalCaptureAcceptsReopenedStatus(t *testing.T) {
	coordinator := &SchemaQueuedMutationCoordinator{}
	for _, status := range []string{"stopped", "uninitialized"} {
		t.Run(status, func(t *testing.T) {
			raw := fmt.Sprintf(`{"kind":"capture","capture":{"client_state":null,"pending_mutations":[],"rejected_mutations":[],"sync_status":{"state":%q,"retry_at":null,"operation":null,"failure":null},"sync_events":[],"request_trace":{"observations":[],"overflowed":false,"sequenceCheckpoint":0}},"process":{"process_id":"process","database_identity_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}`, status)
			if _, err := coordinator.validateFinalCapture(json.RawMessage(raw)); err != nil {
				t.Fatalf("validate reopened status %q: %v", status, err)
			}
		})
	}
}

func TestNewSchemaQueuedMutationCoordinatorKeepsAndroidSidecarOnHostLoopback(t *testing.T) {
	coordinator, err := NewSchemaQueuedMutationCoordinator(SchemaQueuedMutationCoordinatorConfig{
		Scenario: loadSchemaQueuedMutationAuthoredScenario(t), Platform: "android", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token", AppVersion: "0.3.0",
	})
	if err != nil || coordinator == nil {
		t.Fatalf("Android schema-queued-mutation coordinator=%v error=%v", coordinator, err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	if !strings.HasPrefix(coordinator.URL(), "http://127.0.0.1:") {
		t.Fatalf("Android schema-queued-mutation coordinator URL=%q", coordinator.URL())
	}
	if !strings.HasPrefix(coordinator.adapter, "http://10.0.2.2:") {
		t.Fatalf("Android schema-queued-mutation adapter URL=%q", coordinator.adapter)
	}
	if coordinator.ExchangeCount() != 10 {
		t.Fatalf("schema-queued-mutation exchanges=%d want=10", coordinator.ExchangeCount())
	}
}

func loadSchemaQueuedMutationAuthoredScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	scenario, err := LoadSchemaQueuedMutationScenario(context.Background(), repoRoot)
	if err != nil {
		t.Fatalf("load authored schema-queued-mutation scenario: %v", err)
	}
	return scenario
}

func cloneSchemaQueuedMutationScenario(scenario scenarios.Scenario) scenarios.Scenario {
	data, err := json.Marshal(scenario)
	if err != nil {
		panic(err)
	}
	var clone scenarios.Scenario
	if err := json.Unmarshal(data, &clone); err != nil {
		panic(err)
	}
	return clone
}
