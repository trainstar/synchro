package integration

import (
	"context"
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

type serverProofBinding struct {
	scenarioID   string
	obligationID string
	testName     string
}

type realTestDeclaration struct {
	linuxX64       bool
	validSignature bool
}

var requiredServerProofs = map[string][]string{
	"SCN-PERF-CONFIGURED-BOUNDS-001":     {"OBL-PERF-CONFIGURED-BOUNDS-PG-LINUX-X64-001"},
	"SCN-WAL-ORDER-001":                  {"OBL-WAL-ORDER-PG-LINUX-X64-001"},
	"SCN-PULL-DIVERGENT-CHECKPOINTS-001": {"OBL-PULL-DIVERGENT-PG-LINUX-X64-001"},
	"SCN-PULL-HYDRATION-FAILURE-001":     {"OBL-PULL-HYDRATION-PG-LINUX-X64-001"},
	"SCN-WAL-DECODE-FAILURE-001":         {"OBL-WAL-DECODE-PG-LINUX-X64-001"},
	"SCN-REGISTRY-RELOAD-001":            {"OBL-REGISTRY-RELOAD-PG-LINUX-X64-001"},
	"SCN-PUSH-RESPONSE-LOSS-001":         {"OBL-PUSH-RESPONSE-LOSS-PG-LINUX-X64-001"},
	"SCN-REBUILD-FORGED-CURSOR-001":      {"OBL-REBUILD-FORGED-CURSOR-PG-LINUX-X64-001"},
	"SCN-SCHEMA-QUEUED-MUTATION-001":     {"OBL-SCHEMA-QUEUED-MUTATION-PG-LINUX-X64-001"},
	"SCN-RETENTION-RECONNECT-001":        {"OBL-RETENTION-RECONNECT-PG-LINUX-X64-001"},
	"SCN-MEMBERSHIP-REASSIGNMENT-001":    {"OBL-MEMBERSHIP-REASSIGNMENT-PG-LINUX-X64-001"},
}

// serverProofBindings is the sole server-proof map. Synthetic harness runs are
// layer-6 self-tests and negative controls, so they must not enter this map.
var serverProofBindings = []serverProofBinding{
	{"SCN-WAL-ORDER-001", "OBL-WAL-ORDER-PG-LINUX-X64-001", "TestRealWALPipeline"},
	{"SCN-PERF-CONFIGURED-BOUNDS-001", "OBL-PERF-CONFIGURED-BOUNDS-PG-LINUX-X64-001", "TestRealConfiguredBoundsMeasurement"},
	{"SCN-PULL-DIVERGENT-CHECKPOINTS-001", "OBL-PULL-DIVERGENT-PG-LINUX-X64-001", "TestRealS02DivergentPullPaginationIsStarvationFree"},
	{"SCN-PULL-HYDRATION-FAILURE-001", "OBL-PULL-HYDRATION-PG-LINUX-X64-001", "TestRealS03PullHydrationFailurePreservesCursors"},
	{"SCN-WAL-DECODE-FAILURE-001", "OBL-WAL-DECODE-PG-LINUX-X64-001", "TestRealWALDecodeFailureRepairsSameIdentity"},
	{"SCN-REGISTRY-RELOAD-001", "OBL-REGISTRY-RELOAD-PG-LINUX-X64-001", "TestRealRegistryGenerationReloadAtCommitBoundary"},
	{"SCN-PUSH-RESPONSE-LOSS-001", "OBL-PUSH-RESPONSE-LOSS-PG-LINUX-X64-001", "TestRealS11PushResponseLossReplaysExactCanonicalResponse"},
	{"SCN-REBUILD-FORGED-CURSOR-001", "OBL-REBUILD-FORGED-CURSOR-PG-LINUX-X64-001", "TestRealS04RebuildRejectsForgedCursorAndFreezesBoundary"},
	{"SCN-SCHEMA-QUEUED-MUTATION-001", "OBL-SCHEMA-QUEUED-MUTATION-PG-LINUX-X64-001", "TestRealSchemaIncompatibleMutationPersistsCanonicalIntent"},
	{"SCN-RETENTION-RECONNECT-001", "OBL-RETENTION-RECONNECT-PG-LINUX-X64-001", "TestRealS12StaleClientCompactionAndReconnect"},
	{"SCN-MEMBERSHIP-REASSIGNMENT-001", "OBL-MEMBERSHIP-REASSIGNMENT-PG-LINUX-X64-001", "TestRealWALPipeline"},
}

var nonScenarioRealTests = map[string]string{
	"TestRealClass3ProjectionBootstrap":                                "regression",
	"TestRealClass3ProjectionBootstrapRecoversAfterProcessTermination": "regression",
	"TestRealExtensionReinstallRebindsWorkerSlot":                      "regression",
	"TestRealHTTPHarness":                                              "framework",
	"TestRealMutationControlChecksumCorrectness":                       "adversarial",
	"TestRealMutationControlCursorAdvancement":                         "adversarial",
	"TestRealMutationControlMutationConservation":                      "adversarial",
	"TestRealMutationControlProgressOrder":                             "adversarial",
	"TestRealMutationControlScopeIsolation":                            "adversarial",
	"TestRealMutationControlWALAcknowledgement":                        "adversarial",
	"TestRealR1PerformanceBenchmark":                                   "benchmark",
	"TestRealS05SelectiveRebuildPreservesCheckpoints":                  "regression",
	"TestRealS11MixedPushOutcomesPreservePartitionOrder":               "regression",
	"TestRealS16ConcurrentPushCASIgnoresClientTime":                    "regression",
	"TestRealS17InvalidPushShapesDoNoDurableWork":                      "adversarial",
	"TestRealS20PushMutationCountBoundsAreAtomic":                      "adversarial",
}

func TestServerProofMapMatchesAuthoredScenariosAndRealTests(t *testing.T) {
	authored, declarations := loadServerProofMapInputs(t)
	if failures := validateServerProofMap(authored, declarations, serverProofBindings, nonScenarioRealTests); len(failures) > 0 {
		t.Fatalf("server proof map drift:\n%s", strings.Join(failures, "\n"))
	}
}

func TestServerProofMapRejectsDrift(t *testing.T) {
	authored, declarations := loadServerProofMapInputs(t)
	tests := []struct {
		name     string
		expected string
		mutate   func([]serverProofBinding, map[string]string, map[string]realTestDeclaration) ([]serverProofBinding, map[string]string, map[string]realTestDeclaration)
	}{
		{"renamed real test", "proof binding SCN-WAL-ORDER-001|OBL-WAL-ORDER-PG-LINUX-X64-001 names unknown real test TestRealRenamed", func(bindings []serverProofBinding, classifications map[string]string, declarations map[string]realTestDeclaration) ([]serverProofBinding, map[string]string, map[string]realTestDeclaration) {
			bindings[0].testName = "TestRealRenamed"
			return bindings, classifications, declarations
		}},
		{"synthetic harness test", "proof binding SCN-WAL-ORDER-001|OBL-WAL-ORDER-PG-LINUX-X64-001 names non-real test TestRunSyntheticHarnessDetectsSemanticFaults", func(bindings []serverProofBinding, classifications map[string]string, declarations map[string]realTestDeclaration) ([]serverProofBinding, map[string]string, map[string]realTestDeclaration) {
			bindings[0].testName = "TestRunSyntheticHarnessDetectsSemanticFaults"
			return bindings, classifications, declarations
		}},
		{"duplicate binding", "duplicate proof binding SCN-WAL-ORDER-001|OBL-WAL-ORDER-PG-LINUX-X64-001", func(bindings []serverProofBinding, classifications map[string]string, declarations map[string]realTestDeclaration) ([]serverProofBinding, map[string]string, map[string]realTestDeclaration) {
			return append(bindings, bindings[0]), classifications, declarations
		}},
		{"missing binding", "missing proof binding SCN-WAL-ORDER-001|OBL-WAL-ORDER-PG-LINUX-X64-001", func(bindings []serverProofBinding, classifications map[string]string, declarations map[string]realTestDeclaration) ([]serverProofBinding, map[string]string, map[string]realTestDeclaration) {
			return bindings[1:], classifications, declarations
		}},
		{"unknown obligation", "unexpected proof binding SCN-WAL-ORDER-001|OBL-UNKNOWN-001", func(bindings []serverProofBinding, classifications map[string]string, declarations map[string]realTestDeclaration) ([]serverProofBinding, map[string]string, map[string]realTestDeclaration) {
			bindings[0].obligationID = "OBL-UNKNOWN-001"
			return bindings, classifications, declarations
		}},
		{"unclassified real test", "unclassified real test TestRealHTTPHarness", func(bindings []serverProofBinding, classifications map[string]string, declarations map[string]realTestDeclaration) ([]serverProofBinding, map[string]string, map[string]realTestDeclaration) {
			delete(classifications, "TestRealHTTPHarness")
			return bindings, classifications, declarations
		}},
		{"invalid test signature", "real test declaration TestRealWALPipeline has invalid signature", func(bindings []serverProofBinding, classifications map[string]string, declarations map[string]realTestDeclaration) ([]serverProofBinding, map[string]string, map[string]realTestDeclaration) {
			declaration := declarations["TestRealWALPipeline"]
			declaration.validSignature = false
			declarations["TestRealWALPipeline"] = declaration
			return bindings, classifications, declarations
		}},
		{"constrained mapped test", "proof binding SCN-WAL-ORDER-001|OBL-WAL-ORDER-PG-LINUX-X64-001 names real test TestRealWALPipeline unavailable on linux-x64", func(bindings []serverProofBinding, classifications map[string]string, declarations map[string]realTestDeclaration) ([]serverProofBinding, map[string]string, map[string]realTestDeclaration) {
			declaration := declarations["TestRealWALPipeline"]
			declaration.linuxX64 = false
			declarations["TestRealWALPipeline"] = declaration
			return bindings, classifications, declarations
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bindings := append([]serverProofBinding(nil), serverProofBindings...)
			classifications := make(map[string]string, len(nonScenarioRealTests))
			for name, classification := range nonScenarioRealTests {
				classifications[name] = classification
			}
			declarations := cloneRealTestDeclarations(declarations)
			bindings, classifications, declarations = test.mutate(bindings, classifications, declarations)
			failures := validateServerProofMap(authored, declarations, bindings, classifications)
			if !containsFailure(failures, test.expected) {
				t.Fatalf("failures = %v, want %q", failures, test.expected)
			}
		})
	}
}

func loadServerProofMapInputs(t *testing.T) ([]scenarios.Scenario, map[string]realTestDeclaration) {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	authored, err := scenarios.LoadAll(context.Background(), repoRoot)
	if err != nil {
		t.Fatalf("load authored scenarios: %v", err)
	}
	declarations, err := realTestDeclarations(".")
	if err != nil {
		t.Fatalf("load real test declarations: %v", err)
	}
	return authored, declarations
}

func realTestDeclarations(directory string) (map[string]realTestDeclaration, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	declarations := make(map[string]realTestDeclaration)
	files := token.NewFileSet()
	linuxX64 := build.Default
	linuxX64.GOOS = "linux"
	linuxX64.GOARCH = "amd64"
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(directory, entry.Name())
		availableOnLinuxX64, err := linuxX64.MatchFile(directory, entry.Name())
		if err != nil {
			return nil, fmt.Errorf("match %s for linux-x64: %w", path, err)
		}
		parsed, err := parser.ParseFile(files, path, nil, 0)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
		for _, declaration := range parsed.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv != nil || !strings.HasPrefix(function.Name.Name, "TestReal") {
				continue
			}
			if _, duplicate := declarations[function.Name.Name]; duplicate {
				return nil, fmt.Errorf("duplicate real test declaration %s", function.Name.Name)
			}
			declarations[function.Name.Name] = realTestDeclaration{
				linuxX64:       availableOnLinuxX64,
				validSignature: isGoTestSignature(function.Type),
			}
		}
	}
	return declarations, nil
}

func isGoTestSignature(function *ast.FuncType) bool {
	if function.TypeParams != nil || function.Results != nil || function.Params == nil || len(function.Params.List) != 1 {
		return false
	}
	parameter, ok := function.Params.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	testingType, ok := parameter.X.(*ast.SelectorExpr)
	if !ok || testingType.Sel.Name != "T" {
		return false
	}
	testingPackage, ok := testingType.X.(*ast.Ident)
	return ok && testingPackage.Name == "testing"
}

func cloneRealTestDeclarations(input map[string]realTestDeclaration) map[string]realTestDeclaration {
	cloned := make(map[string]realTestDeclaration, len(input))
	for name, declaration := range input {
		cloned[name] = declaration
	}
	return cloned
}

func validateServerProofMap(authored []scenarios.Scenario, declarations map[string]realTestDeclaration, bindings []serverProofBinding, classifications map[string]string) []string {
	scenarioByID := make(map[string]scenarios.Scenario, len(authored))
	for _, scenario := range authored {
		scenarioByID[string(scenario.ID)] = scenario
	}

	var failures []string
	requiredKeys := make(map[string]struct{})
	for scenarioID, obligations := range requiredServerProofs {
		for _, obligationID := range obligations {
			requiredKeys[scenarioID+"|"+obligationID] = struct{}{}
		}
	}
	bindingKeys := make(map[string]struct{}, len(bindings))
	mappedTests := make(map[string]struct{})
	for _, binding := range bindings {
		key := binding.scenarioID + "|" + binding.obligationID
		if _, duplicate := bindingKeys[key]; duplicate {
			failures = append(failures, "duplicate proof binding "+key)
			continue
		}
		bindingKeys[key] = struct{}{}
		if _, required := requiredKeys[key]; !required {
			failures = append(failures, "unexpected proof binding "+key)
		}
		if !strings.HasPrefix(binding.testName, "TestReal") {
			failures = append(failures, fmt.Sprintf("proof binding %s names non-real test %s", key, binding.testName))
		} else if declaration, found := declarations[binding.testName]; !found {
			failures = append(failures, fmt.Sprintf("proof binding %s names unknown real test %s", key, binding.testName))
		} else if strings.Contains(binding.obligationID, "-PG-LINUX-X64-") && !declaration.linuxX64 {
			failures = append(failures, fmt.Sprintf("proof binding %s names real test %s unavailable on linux-x64", key, binding.testName))
		}
		mappedTests[binding.testName] = struct{}{}

		scenario, found := scenarioByID[binding.scenarioID]
		if !found {
			failures = append(failures, "proof binding names unknown scenario "+binding.scenarioID)
			continue
		}
		obligationFound := false
		for _, obligation := range scenario.ProofObligations {
			if string(obligation.ObligationID) != binding.obligationID {
				continue
			}
			obligationFound = true
			if obligation.ProofType != "server-black-box" {
				failures = append(failures, fmt.Sprintf("proof binding %s selects proof type %s", key, obligation.ProofType))
			}
			if obligation.MakeTarget != "test-blackbox" || len(obligation.Argv) != 2 || obligation.Argv[0] != "make" || obligation.Argv[1] != "test-blackbox" {
				failures = append(failures, "proof binding "+key+" does not select exact test-blackbox execution")
			}
			break
		}
		if !obligationFound {
			failures = append(failures, "proof binding names unknown obligation "+key)
		}
	}

	for scenarioID, obligations := range requiredServerProofs {
		_, found := scenarioByID[scenarioID]
		if !found {
			failures = append(failures, "required proof scenario is absent "+scenarioID)
			continue
		}
		for _, obligationID := range obligations {
			key := scenarioID + "|" + obligationID
			if _, found := bindingKeys[key]; !found {
				failures = append(failures, "missing proof binding "+key)
			}
		}
	}
	for testName, declaration := range declarations {
		if !declaration.validSignature {
			failures = append(failures, "real test declaration "+testName+" has invalid signature")
		}
	}

	allowedClassifications := map[string]struct{}{
		"adversarial": {},
		"benchmark":   {},
		"framework":   {},
		"regression":  {},
	}
	for testName, classification := range classifications {
		if _, found := declarations[testName]; !found {
			failures = append(failures, "unknown classified real test "+testName)
		}
		if _, mapped := mappedTests[testName]; mapped {
			failures = append(failures, "mapped real test "+testName+" also has a non-scenario classification")
		}
		if _, allowed := allowedClassifications[classification]; !allowed {
			failures = append(failures, fmt.Sprintf("real test %s has unknown classification %s", testName, classification))
		}
	}
	for testName := range declarations {
		if _, mapped := mappedTests[testName]; mapped {
			continue
		}
		if _, classified := classifications[testName]; !classified {
			failures = append(failures, "unclassified real test "+testName)
		}
	}

	sort.Strings(failures)
	return failures
}

func containsFailure(failures []string, expected string) bool {
	for _, failure := range failures {
		if failure == expected {
			return true
		}
	}
	return false
}
