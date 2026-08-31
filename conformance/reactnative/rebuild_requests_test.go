package reactnative

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestValidateRebuildRequestsScenarioAcceptsAuthoredContract(t *testing.T) {
	if err := ValidateRebuildRequestsScenario(loadRebuildRequestsAuthoredScenario(t)); err != nil {
		t.Fatalf("validate authored rebuild-requests scenario: %v", err)
	}
}

func TestValidateRebuildRequestsScenarioRejectsContractChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*scenarios.Scenario)
	}{
		{"step order", func(scenario *scenarios.Scenario) {
			scenario.Steps[0], scenario.Steps[1] = scenario.Steps[1], scenario.Steps[0]
		}},
		{"call identity", func(scenario *scenarios.Scenario) {
			callID := scenarios.NativeCallID("other-call")
			scenario.Steps[3].NativeBinding.CallID = &callID
		}},
		{"Android proof target", func(scenario *scenarios.Scenario) {
			for index := range scenario.ProofObligations {
				if string(scenario.ProofObligations[index].ObligationID) == "OBL-PERF-REBUILD-REQUESTS-RN-ANDROID-CURRENT-001" {
					scenario.ProofObligations[index].MakeTarget = "test-rn-rebuild-requests-android"
				}
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := cloneRebuildRequestsScenario(loadRebuildRequestsAuthoredScenario(t))
			test.mutate(&scenario)
			if err := ValidateRebuildRequestsScenario(scenario); err == nil {
				t.Fatal("changed rebuild-requests contract was accepted")
			}
		})
	}
}

func TestNewRebuildRequestsCoordinatorKeepsAndroidSidecarOnHostLoopback(t *testing.T) {
	coordinator, err := NewRebuildRequestsCoordinator(RebuildRequestsCoordinatorConfig{
		Scenario: loadRebuildRequestsAuthoredScenario(t), Platform: "android",
		ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token", AppVersion: "0.3.0",
	})
	if err != nil || coordinator == nil {
		t.Fatalf("Android rebuild-requests coordinator was rejected: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	if !strings.HasPrefix(coordinator.URL(), "http://127.0.0.1:") {
		t.Fatalf("Android rebuild-requests coordinator URL = %q", coordinator.URL())
	}
	if !strings.HasPrefix(coordinator.adapter, "http://10.0.2.2:") {
		t.Fatalf("Android rebuild-requests adapter URL = %q", coordinator.adapter)
	}
	if got, want := coordinator.ExchangeCount(), rebuildRequestsExchangeCount; got != want {
		t.Fatalf("rebuild-requests exchange count = %d, want %d", got, want)
	}
}

func TestRebuildRequestsCommandEncodesEmptyStepsAsArray(t *testing.T) {
	scenario := loadRebuildRequestsAuthoredScenario(t)
	coordinator, err := NewRebuildRequestsCoordinator(RebuildRequestsCoordinatorConfig{
		Scenario: scenario, Platform: "ios", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create rebuild-requests coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	command := coordinator.command("client", "open", map[string]any{"client_key": clientKey}, nil)
	if command.Action.Steps == nil || len(command.Action.Steps) != 0 {
		t.Fatalf("rebuild-requests empty command steps = %#v", command.Action.Steps)
	}
}

func TestValidateFirstRebuildResponseRequiresIntermediatePage(t *testing.T) {
	valid := []byte(`{"scope":"runtime-scope","records":[{"table":"runtime-items","pk":{},"row":{},"row_checksum":{},"server_version":"v1"}],"has_more":true,"cursor":"cursor-1"}`)
	if err := validateFirstRebuildResponse(valid); err != nil {
		t.Fatalf("validate intermediate rebuild response: %v", err)
	}
	terminal := []byte(`{"scope":"runtime-scope","records":[{}],"has_more":false,"final_scope_cursor":"cursor-2","checksum":{}}`)
	if err := validateFirstRebuildResponse(terminal); err == nil {
		t.Fatal("terminal rebuild response was accepted as the first page")
	}
}

func loadRebuildRequestsAuthoredScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	scenario, err := LoadRebuildRequestsScenario(context.Background(), repoRoot)
	if err != nil {
		t.Fatalf("load authored rebuild-requests scenario: %v", err)
	}
	return scenario
}

func cloneRebuildRequestsScenario(scenario scenarios.Scenario) scenarios.Scenario {
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
