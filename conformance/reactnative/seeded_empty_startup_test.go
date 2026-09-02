package reactnative

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestValidateSeededEmptyStartupScenarioAcceptsAuthoredContract(t *testing.T) {
	if err := ValidateSeededEmptyStartupScenario(loadSeededEmptyStartupAuthoredScenario(t)); err != nil {
		t.Fatalf("validate authored seeded-empty-startup scenario: %v", err)
	}
}

func TestValidateSeededEmptyStartupScenarioRejectsContractChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*scenarios.Scenario)
	}{
		{"call completion", func(scenario *scenarios.Scenario) { scenario.Steps[2].NativeBinding.Completion = "" }},
		{"assignment operation", func(scenario *scenarios.Scenario) { scenario.Steps[1].Operation.Name = "install-current-contract" }},
		{"Android proof target", func(scenario *scenarios.Scenario) {
			for index := range scenario.ProofObligations {
				if string(scenario.ProofObligations[index].ObligationID) == "OBL-PERF-SEEDED-EMPTY-STARTUP-RN-ANDROID-CURRENT-001" {
					scenario.ProofObligations[index].MakeTarget = "test-rn-seeded-empty-startup-android"
				}
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := cloneSeededEmptyStartupScenario(loadSeededEmptyStartupAuthoredScenario(t))
			test.mutate(&scenario)
			if err := ValidateSeededEmptyStartupScenario(scenario); err == nil {
				t.Fatal("changed seeded-empty-startup contract was accepted")
			}
		})
	}
}

func TestNewSeededEmptyStartupCoordinatorKeepsAndroidSidecarOnHostLoopback(t *testing.T) {
	coordinator, err := NewSeededEmptyStartupCoordinator(SeededEmptyStartupCoordinatorConfig{
		Scenario: loadSeededEmptyStartupAuthoredScenario(t), Platform: "android", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create Android seeded-empty-startup coordinator: %v", err)
	}
	defer func() {
		if err := coordinator.Close(context.Background()); err != nil {
			t.Errorf("close Android seeded-empty-startup coordinator: %v", err)
		}
	}()
	if !strings.HasPrefix(coordinator.URL(), "http://127.0.0.1:") {
		t.Fatalf("Android seeded-empty-startup sidecar URL = %q", coordinator.URL())
	}
	if !strings.HasPrefix(coordinator.adapter, "http://10.0.2.2:") {
		t.Fatalf("Android seeded-empty-startup adapter URL = %q", coordinator.adapter)
	}
	if coordinator.StageCount() <= 0 {
		t.Fatalf("seeded-empty-startup stage count = %d", coordinator.StageCount())
	}
}

func TestNewSeededEmptyStartupCoordinatorRejectsUnknownPlatform(t *testing.T) {
	coordinator, err := NewSeededEmptyStartupCoordinator(SeededEmptyStartupCoordinatorConfig{
		Scenario: loadSeededEmptyStartupAuthoredScenario(t), Platform: "windows", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err == nil || coordinator != nil {
		t.Fatal("unknown-platform seeded-empty-startup coordinator was accepted")
	}
}

func TestSeededEmptyStartupBootstrapTraceMatchesAuthoredScopeProjections(t *testing.T) {
	clients, err := seededEmptyStartupClients(loadSeededEmptyStartupAuthoredScenario(t))
	if err != nil {
		t.Fatalf("load seeded-empty-startup clients: %v", err)
	}
	seeded := clients[0]
	empty := clients[3]
	if seeded.connectScopeProjectionLen != 1 || empty.connectScopeProjectionLen != 0 {
		t.Fatalf("seeded and empty connect scope projections = %d and %d, expected 1 and 0", seeded.connectScopeProjectionLen, empty.connectScopeProjectionLen)
	}
	if seeded.pullScopeProjectionLen != 2 || empty.pullScopeProjectionLen != 2 {
		t.Fatalf("seeded and empty pull scope projections = %d and %d, expected 2 and 2", seeded.pullScopeProjectionLen, empty.pullScopeProjectionLen)
	}
	seededTrace := validBootstrapTrace(testSchema())
	seededTrace.Observations[0].RequestFacts = requestFacts(0, testSchema(), 0, seeded.connectScopeProjectionLen, "", "")
	seededTrace.Observations[2].RequestFacts = requestFacts(1, testSchema(), 1, seeded.pullScopeProjectionLen, "", "")
	if err := validateSeededEmptyStartupBootstrapTrace(seededTrace, seeded.connectScopeProjectionLen, seeded.pullScopeProjectionLen); err != nil {
		t.Fatalf("validate seeded bootstrap trace: %v", err)
	}
	emptyTrace := validBootstrapTrace(testSchema())
	emptyTrace.Observations[2].RequestFacts = requestFacts(1, testSchema(), 1, empty.pullScopeProjectionLen, "", "")
	if err := validateSeededEmptyStartupBootstrapTrace(emptyTrace, empty.connectScopeProjectionLen, empty.pullScopeProjectionLen); err != nil {
		t.Fatalf("validate empty bootstrap trace: %v", err)
	}
	if err := validateSeededEmptyStartupBootstrapTrace(validBootstrapTrace(testSchema()), seeded.connectScopeProjectionLen, seeded.pullScopeProjectionLen); err == nil ||
		!strings.Contains(err.Error(), "observed 0, expected 1") {
		t.Fatalf("seeded bootstrap connect scope mutant error = %v, expected observed 0 and expected 1", err)
	}
	pullMutant := validBootstrapTrace(testSchema())
	pullMutant.Observations[0].RequestFacts = requestFacts(0, testSchema(), 0, seeded.connectScopeProjectionLen, "", "")
	if err := validateSeededEmptyStartupBootstrapTrace(pullMutant, seeded.connectScopeProjectionLen, seeded.pullScopeProjectionLen); err == nil ||
		!strings.Contains(err.Error(), "observed 1, expected 2") {
		t.Fatalf("seeded bootstrap pull scope mutant error = %v, expected observed 1 and expected 2", err)
	}
}

func loadSeededEmptyStartupAuthoredScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	scenario, err := LoadSeededEmptyStartupScenario(context.Background(), repoRoot)
	if err != nil {
		t.Fatalf("load authored seeded-empty-startup scenario: %v", err)
	}
	return scenario
}

func cloneSeededEmptyStartupScenario(scenario scenarios.Scenario) scenarios.Scenario {
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
