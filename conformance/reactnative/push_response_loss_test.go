package reactnative

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestValidatePushResponseLossScenarioAcceptsAuthoredContract(t *testing.T) {
	scenario := loadPushResponseLossAuthoredScenario(t)
	if err := ValidatePushResponseLossScenario(scenario); err != nil {
		t.Fatalf("validate authored response-loss scenario: %v", err)
	}
}

func TestValidatePushResponseLossScenarioRejectsContractChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*scenarios.Scenario)
	}{
		{"step order", func(scenario *scenarios.Scenario) {
			scenario.Steps[0], scenario.Steps[1] = scenario.Steps[1], scenario.Steps[0]
		}},
		{"response-loss delivery", func(scenario *scenarios.Scenario) {
			var payload map[string]any
			if err := json.Unmarshal(scenario.Steps[1].Operation.Payload, &payload); err != nil {
				panic(err)
			}
			payload["delivery"] = "apply"
			scenario.Steps[1].Operation.Payload, _ = json.Marshal(payload)
		}},
		{"changed replay", func(scenario *scenarios.Scenario) {
			var payload map[string]any
			if err := json.Unmarshal(scenario.Steps[5].Operation.Payload, &payload); err != nil {
				panic(err)
			}
			request := payload["request"].(map[string]any)
			mutation := request["mutations"].([]any)[0].(map[string]any)
			mutation["columns"].(map[string]any)["value"] = "response-loss"
			scenario.Steps[5].Operation.Payload, _ = json.Marshal(payload)
		}},
		{"wire status", func(scenario *scenarios.Scenario) {
			scenario.WireExpectations[0].HTTPStatus = http.StatusOK
		}},
		{"identity kind", func(scenario *scenarios.Scenario) {
			scenario.NativeIdentityAliases[0].Kind = "batch-id"
		}},
		{"assertion oracle", func(scenario *scenarios.Scenario) {
			scenario.Assertions[0].Oracle.ExpectedSource = "system-under-test"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := clonePushResponseLossScenario(loadPushResponseLossAuthoredScenario(t))
			test.mutate(&scenario)
			if err := ValidatePushResponseLossScenario(scenario); err == nil {
				t.Fatal("changed response-loss contract was accepted")
			}
		})
	}
}

func TestNewPushResponseLossCoordinatorUsesHostLoopbackProxy(t *testing.T) {
	upstream := httptest.NewServer(http.NotFoundHandler())
	defer upstream.Close()
	coordinator, err := NewPushResponseLossCoordinator(PushResponseLossCoordinatorConfig{
		Scenario: loadPushResponseLossAuthoredScenario(t), Platform: "android", ServerURL: upstream.URL, AuthToken: "unit-token", AppVersion: "0.3.0",
	})
	if err != nil {
		t.Fatalf("create response-loss coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	if !strings.HasPrefix(coordinator.URL(), "http://127.0.0.1:") {
		t.Fatalf("coordinator URL = %q", coordinator.URL())
	}
	if !strings.HasPrefix(coordinator.adapter, "http://10.0.2.2:") {
		t.Fatalf("Android adapter URL = %q", coordinator.adapter)
	}
	if coordinator.upstream != upstream.URL {
		t.Fatalf("upstream URL = %q, want %q", coordinator.upstream, upstream.URL)
	}
	if coordinator.ExchangeCount() != 10 {
		t.Fatalf("exchange count = %d, want 10", coordinator.ExchangeCount())
	}
}

func TestPushResponseLossCommandUsesNonNilEmptyStepManifest(t *testing.T) {
	coordinator, err := NewPushResponseLossCoordinator(PushResponseLossCoordinatorConfig{
		Scenario: loadPushResponseLossAuthoredScenario(t), Platform: "ios", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create response-loss coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	command := coordinator.command("client", "open", map[string]any{"client_key": coordinator.clientKey}, nil)
	if command.Action.Steps == nil || len(command.Action.Steps) != 0 {
		t.Fatalf("empty command steps = %#v", command.Action.Steps)
	}
}

func loadPushResponseLossAuthoredScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	scenario, err := LoadPushResponseLossScenario(context.Background(), repositoryRoot)
	if err != nil {
		t.Fatalf("load authored response-loss scenario: %v", err)
	}
	return scenario
}

func clonePushResponseLossScenario(scenario scenarios.Scenario) scenarios.Scenario {
	encoded, _ := json.Marshal(scenario)
	var clone scenarios.Scenario
	_ = json.Unmarshal(encoded, &clone)
	return clone
}
