package server

import (
	"context"
	"testing"

	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/modelrunner"
	"github.com/trainstar/synchro/conformance/scenarios"
	"github.com/trainstar/synchro/conformance/vectors"
)

func TestPhase3AuthoredScenariosValidateAndPassTheModel(t *testing.T) {
	ctx := context.Background()
	repoRoot := "../../.."

	bundle, err := contract.Load(ctx, repoRoot)
	if err != nil {
		t.Fatalf("load authored contract: %v", err)
	}
	vectorCatalog, err := vectors.Load(ctx, repoRoot)
	if err != nil {
		t.Fatalf("load vector catalog: %v", err)
	}
	authored, err := scenarios.LoadAll(ctx, repoRoot)
	if err != nil {
		t.Fatalf("load authored scenarios: %v", err)
	}

	if err := scenarios.ValidateAllWithVectors(authored, bundle, vectorCatalog); err != nil {
		t.Fatalf("validate authored scenarios: %v", err)
	}
	selected := make([]scenarios.Scenario, 0, len(authored))
	for _, scenario := range authored {
		if scenarios.IsModelCorpusScenario(scenario) {
			selected = append(selected, scenario)
		}
	}
	if len(selected) == 0 {
		t.Fatal("model corpus selection is empty")
	}
	for _, scenario := range selected {
		scenario := scenario
		t.Run(string(scenario.ID), func(t *testing.T) {
			result, err := modelrunner.RunScenario(ctx, scenario)
			if err != nil {
				t.Fatalf("run authored model: %v", err)
			}
			if !result.Passed {
				t.Fatal("authored model did not pass")
			}
		})
	}
}
