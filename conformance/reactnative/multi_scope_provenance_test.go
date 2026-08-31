package reactnative

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestValidateMultiScopeProvenanceScenarioAcceptsAuthoredContract(t *testing.T) {
	if err := ValidateMultiScopeProvenanceScenario(loadMultiScopeProvenanceScenario(t)); err != nil {
		t.Fatalf("validate authored multi-scope provenance scenario: %v", err)
	}
}

func TestValidateMultiScopeProvenanceScenarioRejectsContractChanges(t *testing.T) {
	scenario := cloneMultiScopeProvenanceScenario(loadMultiScopeProvenanceScenario(t))
	for index := range scenario.ProofObligations {
		if string(scenario.ProofObligations[index].ObligationID) == "OBL-PERF-MULTI-SCOPE-PROVENANCE-RN-ANDROID-CURRENT-001" {
			scenario.ProofObligations[index].MakeTarget = "test-rn-other"
		}
	}
	if err := ValidateMultiScopeProvenanceScenario(scenario); err == nil {
		t.Fatal("changed Android proof target was accepted")
	}
}

func TestNewMultiScopeProvenanceCoordinatorKeepsAndroidSidecarOnHostLoopback(t *testing.T) {
	coordinator, err := NewMultiScopeProvenanceCoordinator(MultiScopeProvenanceCoordinatorConfig{Scenario: loadMultiScopeProvenanceScenario(t), Platform: "android", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token"})
	if err != nil || coordinator == nil {
		t.Fatalf("Android multi-scope provenance coordinator was rejected: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	if !strings.HasPrefix(coordinator.URL(), "http://127.0.0.1:") {
		t.Fatalf("Android coordinator URL = %q", coordinator.URL())
	}
	if !strings.HasPrefix(coordinator.adapter, "http://10.0.2.2:") {
		t.Fatalf("Android adapter URL = %q", coordinator.adapter)
	}
}

func loadMultiScopeProvenanceScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	scenario, err := LoadMultiScopeProvenanceScenario(context.Background(), root)
	if err != nil {
		t.Fatalf("load authored multi-scope provenance scenario: %v", err)
	}
	return scenario
}
func cloneMultiScopeProvenanceScenario(scenario scenarios.Scenario) scenarios.Scenario {
	raw, err := json.Marshal(scenario)
	if err != nil {
		panic(err)
	}
	var clone scenarios.Scenario
	if json.Unmarshal(raw, &clone) != nil {
		panic("decode scenario")
	}
	return clone
}
