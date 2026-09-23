package reactnative

import (
	"fmt"
	"testing"

	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func corpusScenarioIDs(authored []scenarios.Scenario, cell string, runners map[string]func(*testing.T, string)) ([]string, error) {
	var selected []string
	seen := make(map[string]bool)
	for _, scenario := range authored {
		for _, obligation := range scenario.ProofObligations {
			if obligation.ProofType == "native-e2e" && obligation.SupportCellID != nil &&
				string(*obligation.SupportCellID) == cell {
				id := string(scenario.ID)
				if runners[id] == nil || seen[id] {
					return nil, fmt.Errorf("React Native scenario %s has a missing or duplicate runner", id)
				}
				seen[id] = true
				selected = append(selected, id)
				break
			}
		}
	}
	if len(selected) == 0 || len(selected) != len(runners) {
		return nil, fmt.Errorf("React Native corpus has %d authored scenarios and %d runners", len(selected), len(runners))
	}
	return selected, nil
}

func TestCorpusRejectsMissingAndExtraRunners(t *testing.T) {
	cell := contract.SupportCellID("SUP-RN-IOS-CURRENT-001")
	authored := []scenarios.Scenario{{
		ID: "SCN-PERF-WARM-CONNECT-001",
		ProofObligations: []scenarios.ProofObligation{{
			ProofType: "native-e2e", SupportCellID: &cell,
		}},
	}}
	runner := func(*testing.T, string) {}
	valid := map[string]func(*testing.T, string){warmConnectScenarioID: runner}
	ids, err := corpusScenarioIDs(authored, string(cell), valid)
	if err != nil || len(ids) != 1 || ids[0] != warmConnectScenarioID {
		t.Fatalf("valid corpus selection: %v, %v", ids, err)
	}
	for name, runners := range map[string]map[string]func(*testing.T, string){
		"missing": nil,
		"nil":     {warmConnectScenarioID: nil},
		"extra":   {warmConnectScenarioID: runner, steadyPullScenarioID: runner},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := corpusScenarioIDs(authored, string(cell), runners); err == nil {
				t.Fatal("incomplete corpus accepted")
			}
		})
	}
	if _, err := corpusScenarioIDs(nil, string(cell), nil); err == nil {
		t.Fatal("empty corpus accepted")
	}
	if _, err := corpusScenarioIDs(append(authored, authored...), string(cell), valid); err == nil {
		t.Fatal("duplicate scenario accepted")
	}
}
