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

	expected := map[contract.ScenarioID]struct{}{
		"SCN-WAL-ORDER-001":                   {},
		"SCN-PULL-DIVERGENT-CHECKPOINTS-001":  {},
		"SCN-PULL-HYDRATION-FAILURE-001":      {},
		"SCN-WAL-DECODE-FAILURE-001":          {},
		"SCN-REGISTRY-RELOAD-001":             {},
		"SCN-PUSH-RESPONSE-LOSS-001":          {},
		"SCN-REBUILD-FORGED-CURSOR-001":       {},
		"SCN-SCHEMA-QUEUED-MUTATION-001":      {},
		"SCN-RETENTION-RECONNECT-001":         {},
		"SCN-MEMBERSHIP-REASSIGNMENT-001":     {},
		"SCN-PERF-WARM-CONNECT-001":           {},
		"SCN-PERF-STEADY-PULL-001":            {},
		"SCN-PERF-PENDING-CYCLE-001":          {},
		"SCN-PERF-REBUILD-REQUESTS-001":       {},
		"SCN-PERF-CORE-SYNC-PATH-001":         {},
		"SCN-PERF-FANOUT-001":                 {},
		"SCN-PERF-SHARED-PRIVATE-SCOPES-001":  {},
		"SCN-PERF-REBUILD-CARDINALITY-001":    {},
		"SCN-PERF-SCHEMA-CHECK-001":           {},
		"SCN-PERF-SEEDED-EMPTY-STARTUP-001":   {},
		"SCN-PERF-QUEUE-REPLAY-001":           {},
		"SCN-PERF-REBUILD-APPLY-001":          {},
		"SCN-PERF-MULTI-SCOPE-PROVENANCE-001": {},
		"SCN-PERF-CONFIGURED-BOUNDS-001":      {},
	}
	if len(authored) != len(expected) {
		t.Fatalf("authored scenario count = %d, want %d", len(authored), len(expected))
	}
	for _, scenario := range authored {
		if _, found := expected[scenario.ID]; !found {
			t.Errorf("unexpected authored scenario %s", scenario.ID)
		}
		delete(expected, scenario.ID)
	}
	if len(expected) != 0 {
		t.Fatalf("authored scenario set omits %d required IDs", len(expected))
	}

	if err := scenarios.ValidateAllWithVectors(authored, bundle, vectorCatalog); err != nil {
		t.Fatalf("validate authored scenarios: %v", err)
	}
	for _, scenario := range authored {
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
