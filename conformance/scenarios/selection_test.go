package scenarios

import (
	"context"
	"testing"
)

func TestAuthoredScenarioSelectionHasExactlyOneProofHome(t *testing.T) {
	authored, err := LoadAll(context.Background(), "../../")
	if err != nil {
		t.Fatalf("load authored scenarios: %v", err)
	}
	for _, scenario := range authored {
		model := IsModelCorpusScenario(scenario)
		native := IsNativeDerivationScenario(scenario)
		if model == native {
			t.Errorf("scenario %s has model selection=%t and native derivation selection=%t, want exactly one", scenario.ID, model, native)
		}
	}
}
