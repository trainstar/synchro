package kotlin

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestPushResponseLossBindingsFollowAuthoredWireCompletions(t *testing.T) {
	scenario := loadPushResponseLossScenario(t)
	steps, err := kotlinScenarioStepMap(scenario, pushResponseLossScenarioID, 6)
	if err != nil {
		t.Fatalf("map push-response-loss scenario: %v", err)
	}
	client := Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "push-response-loss-client-a"}
	if err := validatePushResponseLossBindings(scenario, steps, client); err != nil {
		t.Fatalf("validate push-response-loss bindings: %v", err)
	}
	initial, err := pushResponseLossWireExpectation(scenario, "STEP-PUSH-RESPONSE-LOSS-002")
	if err != nil {
		t.Fatalf("read initial wire expectation: %v", err)
	}
	if got := pushResponseLossNativeCompletion(initial); got != "blocked" {
		t.Fatalf("initial completion = %q, want blocked", got)
	}
	final, err := pushResponseLossWireExpectation(scenario, "STEP-PUSH-RESPONSE-LOSS-004")
	if err != nil {
		t.Fatalf("read final wire expectation: %v", err)
	}
	if got := pushResponseLossNativeCompletion(final); got != steps["STEP-PUSH-RESPONSE-LOSS-004"].NativeBinding.Completion {
		t.Fatalf("final completion = %q, want %q", got, steps["STEP-PUSH-RESPONSE-LOSS-004"].NativeBinding.Completion)
	}
}

func TestPushResponseLossBindingsRejectCompletionNotDerivedFromWire(t *testing.T) {
	scenario := loadPushResponseLossScenario(t)
	for index := range scenario.Steps {
		if scenario.Steps[index].ID != "STEP-PUSH-RESPONSE-LOSS-004" {
			continue
		}
		binding := *scenario.Steps[index].NativeBinding
		binding.Completion = "blocked"
		scenario.Steps[index].NativeBinding = &binding
	}
	steps, err := kotlinScenarioStepMap(scenario, pushResponseLossScenarioID, 6)
	if err != nil {
		t.Fatalf("map mutated push-response-loss scenario: %v", err)
	}
	client := Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "push-response-loss-client-a"}
	if err := validatePushResponseLossBindings(scenario, steps, client); err == nil {
		t.Fatal("push-response-loss binding accepted a completion that differs from the authored wire expectation")
	}
}

func TestPushResponseLossBindingsRejectChangedEqualReplay(t *testing.T) {
	scenario := loadPushResponseLossScenario(t)
	for index := range scenario.Steps {
		if scenario.Steps[index].ID != "STEP-PUSH-RESPONSE-LOSS-005" {
			continue
		}
		payload := string(scenario.Steps[index].Operation.Payload)
		payload = replacePushResponseLossValue(t, payload, "response-loss", "response-loss-changed")
		scenario.Steps[index].Operation.Payload = []byte(payload)
	}
	steps, err := kotlinScenarioStepMap(scenario, pushResponseLossScenarioID, 6)
	if err != nil {
		t.Fatalf("map mutated push-response-loss scenario: %v", err)
	}
	client := Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "push-response-loss-client-a"}
	if err := validatePushResponseLossBindings(scenario, steps, client); err == nil {
		t.Fatal("push-response-loss binding accepted changed content for an equal replay")
	}
}

func loadPushResponseLossScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	root := filepath.Join("..", "..")
	scenario, err := scenarios.LoadFile(context.Background(), root, "conformance/scenarios/server/push-response-loss-001.json")
	if err != nil {
		t.Fatalf("load push-response-loss scenario: %v", err)
	}
	return scenario
}

func replacePushResponseLossValue(t *testing.T, value, old, replacement string) string {
	t.Helper()
	index := -1
	for offset := 0; offset+len(old) <= len(value); offset++ {
		if value[offset:offset+len(old)] == old {
			index = offset
		}
	}
	if index < 0 {
		t.Fatalf("value %q is absent from replay payload", old)
	}
	return value[:index] + replacement + value[index+len(old):]
}
