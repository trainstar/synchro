package scenarios

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/trainstar/synchro/conformance/internal/contract"
)

func TestSemanticCorpusBindingsMatchApprovedCells(t *testing.T) {
	bundle, err := contract.Load(context.Background(), "../../")
	if err != nil {
		t.Fatalf("load authored contract: %v", err)
	}
	authored, err := LoadAll(context.Background(), "../../")
	if err != nil {
		t.Fatalf("load authored scenarios: %v", err)
	}

	wantScenarios := []contract.ScenarioID{
		"SCN-PERF-MULTI-SCOPE-PROVENANCE-001",
		"SCN-PERF-PENDING-CYCLE-001",
		"SCN-PERF-QUEUE-REPLAY-001",
		"SCN-PERF-REBUILD-APPLY-001",
		"SCN-PERF-REBUILD-CARDINALITY-001",
		"SCN-PERF-REBUILD-REQUESTS-001",
		"SCN-PERF-SCHEMA-CHECK-001",
		"SCN-PERF-SEEDED-EMPTY-STARTUP-001",
		"SCN-PERF-STEADY-PULL-001",
		"SCN-PERF-WARM-CONNECT-001",
		"SCN-PUSH-RESPONSE-LOSS-001",
		"SCN-REBUILD-FORGED-CURSOR-001",
		"SCN-RETENTION-RECONNECT-001",
		"SCN-SCHEMA-QUEUED-MUTATION-001",
	}
	wantTargets := map[contract.SupportCellID]string{
		"SUP-MACOS-CURRENT-001":      "test-swift",
		"SUP-ANDROID-CURRENT-001":    "test-kotlin",
		"SUP-RN-IOS-CURRENT-001":     "test-rn-e2e-ios",
		"SUP-RN-ANDROID-CURRENT-001": "test-rn-e2e-android",
	}

	var gotScenarios []contract.ScenarioID
	for _, scenario := range authored {
		assertions := make(map[contract.AssertionID]Assertion, len(scenario.Assertions))
		for _, assertion := range scenario.Assertions {
			assertions[assertion.ID] = assertion
		}
		bindings := make(map[contract.SupportCellID]ProofObligation)
		for _, obligation := range scenario.ProofObligations {
			if obligation.ProofType != "native-e2e" || !hasSemanticAssertion(obligation, assertions) {
				continue
			}
			if obligation.SupportCellID == nil {
				t.Fatalf("%s semantic native obligation %s has no support cell", scenario.ID, obligation.ObligationID)
			}
			if _, duplicate := bindings[*obligation.SupportCellID]; duplicate {
				t.Fatalf("%s has duplicate semantic binding for %s", scenario.ID, *obligation.SupportCellID)
			}
			bindings[*obligation.SupportCellID] = obligation
		}
		if len(bindings) == 0 {
			continue
		}
		gotScenarios = append(gotScenarios, scenario.ID)
		if len(bindings) != len(wantTargets) {
			t.Fatalf("%s has %d semantic cells, want %d", scenario.ID, len(bindings), len(wantTargets))
		}
		for cellID, target := range wantTargets {
			if scenario.ID == "SCN-PERF-WARM-CONNECT-001" {
				switch cellID {
				case "SUP-RN-IOS-CURRENT-001":
					target = "test-rn-warm-connect-ios"
				case "SUP-RN-ANDROID-CURRENT-001":
					target = "test-rn-warm-connect-android"
				}
			}
			obligation, found := bindings[cellID]
			if !found {
				t.Fatalf("%s omits semantic cell %s", scenario.ID, cellID)
			}
			if obligation.MakeTarget != target || !reflect.DeepEqual(obligation.Argv, []string{"make", target}) {
				t.Fatalf("%s semantic cell %s uses target %s and argv %v", scenario.ID, cellID, obligation.MakeTarget, obligation.Argv)
			}
		}
	}

	sort.Slice(gotScenarios, func(left, right int) bool { return gotScenarios[left] < gotScenarios[right] })
	sort.Slice(wantScenarios, func(left, right int) bool { return wantScenarios[left] < wantScenarios[right] })
	if !reflect.DeepEqual(gotScenarios, wantScenarios) {
		t.Fatalf("semantic corpus scenarios = %v, want %v", gotScenarios, wantScenarios)
	}
	for cellID, target := range wantTargets {
		cell, found := supportCellByID(bundle, cellID)
		if !found {
			t.Fatalf("semantic support cell %s is absent", cellID)
		}
		wantPolicy := "required"
		if cellID == "SUP-MACOS-CURRENT-001" {
			wantPolicy = "tested"
		}
		if cell.Policy != wantPolicy {
			t.Fatalf("semantic support cell %s policy = %s, want %s for %s", cellID, cell.Policy, wantPolicy, target)
		}
	}
}

func hasSemanticAssertion(obligation ProofObligation, assertions map[contract.AssertionID]Assertion) bool {
	for _, assertionID := range obligation.AssertionIDs {
		if assertion, found := assertions[assertionID]; found && assertion.Oracle.Kind != "performance-budget" {
			return true
		}
	}
	return false
}
