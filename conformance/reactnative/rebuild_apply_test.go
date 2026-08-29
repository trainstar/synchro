package reactnative

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestValidateRebuildApplyScenarioAcceptsAuthoredContract(t *testing.T) {
	if err := ValidateRebuildApplyScenario(loadRebuildApplyAuthoredScenario(t)); err != nil {
		t.Fatalf("validate authored rebuild-apply scenario: %v", err)
	}
}

func TestValidateRebuildApplyScenarioRejectsContractChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*scenarios.Scenario)
	}{
		{"workload client", func(s *scenarios.Scenario) { s.Steps[1].NativeBinding.ClientID = s.Steps[0].NativeBinding.ClientID }},
		{"page size", func(s *scenarios.Scenario) {
			s.Steps[0].Operation.Payload = json.RawMessage(`{"profile":"scope_cardinality","scope_id":"scope-a","record_count":1,"page_size":0}`)
		}},
		{"iOS proof target", func(s *scenarios.Scenario) {
			for i := range s.ProofObligations {
				if string(s.ProofObligations[i].ObligationID) == "OBL-PERF-REBUILD-APPLY-RN-IOS-CURRENT-001" {
					s.ProofObligations[i].MakeTarget = "test-rn-rebuild-apply-ios"
				}
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := cloneRebuildApplyScenario(loadRebuildApplyAuthoredScenario(t))
			test.mutate(&scenario)
			if err := ValidateRebuildApplyScenario(scenario); err == nil {
				t.Fatal("changed rebuild-apply contract was accepted")
			}
		})
	}
}

func TestNewRebuildApplyCoordinatorKeepsAndroidSidecarOnHostLoopback(t *testing.T) {
	coordinator, err := NewRebuildApplyCoordinator(RebuildApplyCoordinatorConfig{
		Scenario: loadRebuildApplyAuthoredScenario(t), Platform: "android", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token", AppVersion: "0.3.0",
	})
	if err != nil || coordinator == nil {
		t.Fatalf("Android rebuild-apply coordinator was rejected: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	if !strings.HasPrefix(coordinator.URL(), "http://127.0.0.1:") {
		t.Fatalf("Android rebuild-apply coordinator URL = %q", coordinator.URL())
	}
	if !strings.HasPrefix(coordinator.adapter, "http://10.0.2.2:") {
		t.Fatalf("Android rebuild-apply adapter URL = %q", coordinator.adapter)
	}
	if got, want := coordinator.ExchangeCount(), len(coordinator.config.Scenario.Steps)*3+1; got != want {
		t.Fatalf("rebuild-apply exchange count = %d, want %d", got, want)
	}
}

func loadRebuildApplyAuthoredScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	scenario, err := LoadRebuildApplyScenario(context.Background(), repoRoot)
	if err != nil {
		t.Fatalf("load authored rebuild-apply scenario: %v", err)
	}
	return scenario
}

func cloneRebuildApplyScenario(scenario scenarios.Scenario) scenarios.Scenario {
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
