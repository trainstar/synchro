package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestRunRejectsInvalidCommandsAndFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"missing command", nil, "command is required"},
		{"unknown command", []string{"unknown"}, "unknown command"},
		{"malformed flag", []string{"catalog", "--unknown"}, "catalog flags are invalid"},
		{"missing root", []string{"catalog", "--write"}, "catalog requires --repo-root PATH"},
		{"missing mode", []string{"catalog", "--repo-root", "."}, "catalog requires exactly one"},
		{"both modes", []string{"catalog", "--repo-root", ".", "--write", "--check"}, "catalog requires exactly one"},
		{"positional extra", []string{"catalog", "--repo-root", ".", "--check", "extra"}, "does not accept positional"},
		{"model missing root", []string{"model"}, "model requires --repo-root PATH"},
		{"model malformed flag", []string{"model", "--unknown"}, "model flags are invalid"},
		{"blackbox missing mode", []string{"blackbox", "--repo-root", "."}, "blackbox requires --mode"},
		{"blackbox invalid mode", []string{"blackbox", "--repo-root", ".", "--mode", "other"}, "blackbox requires --mode"},
		{"baseline missing root", []string{"baseline", "--output", "baseline-test"}, "baseline requires --repo-root PATH"},
		{"baseline missing output", []string{"baseline", "--repo-root", "."}, "baseline requires --output PATH"},
		{"mutants missing root", []string{"mutants"}, "mutants requires --repo-root PATH"},
		{"mutants positional extra", []string{"mutants", "--repo-root", ".", "extra"}, "does not accept positional"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := run(context.Background(), test.args)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("run error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestRunModelExecutesSelectedAuthoredScenario(t *testing.T) {
	err := run(context.Background(), []string{
		"model", "--repo-root", repositoryRoot(t), "--scenario", "SCN-WAL-ORDER-001",
	})
	if err != nil {
		t.Fatalf("run selected authored scenario: %v", err)
	}
}

func TestRunStrictBlackboxFailsClosedWithoutReleaseEvidence(t *testing.T) {
	err := run(context.Background(), []string{"blackbox", "--repo-root", repositoryRoot(t), "--mode", "strict"})
	if err == nil || !strings.Contains(err.Error(), "strict protocol 3 black-box execution is unavailable") {
		t.Fatalf("strict black-box result = %v", err)
	}
}

func TestRunSyntheticHarnessDetectsSemanticFaults(t *testing.T) {
	err := run(context.Background(), []string{"blackbox", "--repo-root", repositoryRoot(t), "--mode", "harness"})
	if err != nil {
		t.Fatalf("run synthetic harness: %v", err)
	}
}

func TestRunMutantsRequiresAllFourSemanticDetections(t *testing.T) {
	err := run(context.Background(), []string{"mutants", "--repo-root", repositoryRoot(t)})
	if err != nil {
		t.Fatalf("run mutants: %v", err)
	}
}

func TestRunReportsBoundedSchemaFailureWithoutPayload(t *testing.T) {
	root := cliRepository(t)
	schemaPath := filepath.Join(root, filepath.FromSlash("conformance/schemas/scenario-v2.schema.json"))
	if err := os.WriteFile(schemaPath, []byte(`{"$schema":"https://example.test/unknown"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	err := run(context.Background(), []string{"catalog", "--repo-root", root, "--check"})
	if err == nil {
		t.Fatal("schema failure was accepted")
	}
	message := err.Error()
	if !strings.Contains(message, "catalog check failed: validate scenario") {
		t.Fatalf("schema diagnostic = %q", message)
	}
	if strings.Contains(message, "payload-secret") || len(message) > 256 {
		t.Fatalf("schema diagnostic is unsafe or unbounded: %q", message)
	}
}

func TestRunReportsMissingRepositoryRoot(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	err := run(context.Background(), []string{"catalog", "--repo-root", missing, "--check"})
	if err == nil || !strings.Contains(err.Error(), "catalog check failed: resolve real repository root") {
		t.Fatalf("missing root diagnostic = %v", err)
	}
}

func TestRunReportsCatalogDrift(t *testing.T) {
	root := cliRepository(t)
	if err := scenarios.WriteGeneratedCatalog(context.Background(), root); err != nil {
		t.Fatalf("write baseline catalog: %v", err)
	}
	path := filepath.Join(root, "conformance", "catalog.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[0] = '['
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	err = run(context.Background(), []string{"catalog", "--repo-root", root, "--check"})
	if err == nil || !strings.Contains(err.Error(), "scenario catalog bytes do not match generated catalog") {
		t.Fatalf("catalog drift diagnostic = %v", err)
	}
}

func TestRunPreservesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := run(ctx, []string{"catalog", "--repo-root", t.TempDir(), "--check"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error = %v", err)
	}
}

func TestOperationFailureIncludesBoundedCauseAndUnwraps(t *testing.T) {
	cause := errors.New(strings.Repeat("x", 500))
	err := operationFailure{operation: "catalog check", outcome: "failed", cause: cause}
	if !errors.Is(err, cause) {
		t.Fatal("operation failure did not preserve its cause")
	}
	if message := err.Error(); !strings.Contains(message, ": ") || len(message) > 200 {
		t.Fatalf("bounded operation diagnostic = %q", message)
	}
}

func cliRepository(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeCLIFile(t, root, "Makefile", []byte(`test-conformance test-blackbox:
test-swift:
test-kotlin:
test-rn-e2e-ios:
test-rn-e2e-android:
test-conformance-scenarios:
`))
	writeCLIFile(t, root, "conformance/schemas/scenario-v2.schema.json", cliScenarioSchema(t))
	writeCLIFile(t, root, "conformance/scenarios/valid.json", cliScenarioBytes(t))
	return root
}

func cliScenarioSchema(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(repositoryRoot(t), "conformance", "schemas", "scenario-v2.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate CLI test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func cliScenarioBytes(t *testing.T) []byte {
	t.Helper()
	scenario := scenarios.Scenario{
		SchemaURI:           "https://synchro.dev/conformance/schemas/scenario-v2.schema.json",
		SchemaVersion:       2,
		ID:                  "SCN-CLI-001",
		Title:               "CLI scenario",
		RequirementIDs:      []contract.RequirementID{"SYNC-TEST-001"},
		NormativeReferences: []contract.NormativeReference{{Path: "docs/src/content/docs/spec/04-invariants.mdx", Anchor: "#canonical-time-format"}},
		ProofTypes:          []string{"reference-model"},
		ProofObligations: []scenarios.ProofObligation{{
			ObligationID:           "OBL-CLI-001",
			RequirementIDs:         []contract.RequirementID{"SYNC-TEST-001"},
			AssertionIDs:           []contract.AssertionID{"ASSERT-CLI-001"},
			ProofType:              "reference-model",
			ArtifactInventoryIDs:   []contract.ArtifactInventoryID{"ARTDEF-TEST-001"},
			PerformanceBudgetIDs:   []contract.BudgetID{},
			RequiredMeasurementIDs: []contract.MeasurementID{},
			RequiredVectorSetIDs:   []contract.VectorSetID{},
			MakeTarget:             "test-conformance-scenarios",
			Argv:                   []string{"make", "test-conformance-scenarios"},
		}},
		Ownership: []scenarios.Ownership{{
			ScenarioID:        "SCN-CLI-001",
			RequirementID:     "SYNC-TEST-001",
			ProofObligationID: "OBL-CLI-001",
			AssertionID:       "ASSERT-CLI-001",
			ProofType:         "reference-model",
		}},
		Model: scenarios.ModelSpec{
			Setup: []scenarios.Operation{{ContractOperation: "model", Name: "author-state", Payload: json.RawMessage(`{"payload-secret":true}`)}},
			ExpectedState: []scenarios.ModelExpectation{{
				ID:        "EXPECT-CLI-001",
				Predicate: scenarios.Predicate{ContractPredicate: "state-equality", Name: "state-unchanged", Payload: json.RawMessage(`{}`)},
			}},
		},
		BarrierPlan:      scenarios.BarrierPlan{Barriers: []scenarios.Barrier{}},
		FaultPlans:       []scenarios.FaultPlan{},
		Replay:           scenarios.ReplaySpec{Mode: "deterministic"},
		NegativeControls: []scenarios.NegativeControl{},
		Steps: []scenarios.Step{{
			ID:              "STEP-CLI-001",
			Phase:           "exercise",
			Transport:       "model",
			Operation:       scenarios.Operation{ContractOperation: "model", Name: "evaluate-state", Payload: json.RawMessage(`{}`)},
			ExpectedOutcome: scenarios.ExpectedOutcome{Disposition: "success"},
		}},
		WireExpectations: []scenarios.WireExpectation{},
		Assertions: []scenarios.Assertion{{
			ID:                "ASSERT-CLI-001",
			RequirementIDs:    []contract.RequirementID{"SYNC-TEST-001"},
			Description:       "The observed state equals the authored state.",
			ExpectationIDs:    []scenarios.ExpectationID{"EXPECT-CLI-001"},
			Predicate:         scenarios.Predicate{ContractPredicate: "state-equality", Name: "state-unchanged", Payload: json.RawMessage(`{}`)},
			Oracle:            scenarios.Oracle{Kind: "model-state-equality", ExpectedSource: "authored-model", ObservedSource: "system-under-test"},
			DetectsControlIDs: []contract.ControlID{},
		}},
	}
	data, err := json.MarshalIndent(scenario, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return append(data, '\n')
}

func writeCLIFile(t *testing.T, root, path string, data []byte) {
	t.Helper()
	absolute := filepath.Join(root, filepath.FromSlash(path))
	if err := os.MkdirAll(filepath.Dir(absolute), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(absolute, data, 0o644); err != nil {
		t.Fatal(err)
	}
}
