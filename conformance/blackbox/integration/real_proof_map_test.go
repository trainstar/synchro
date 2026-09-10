package integration

import (
	"context"
	"encoding/json"
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
	testNames    []string
}

type realTestDeclaration struct {
	linuxX64       bool
	validSignature bool
}

type integrationMutant struct {
	Patch         string `json:"patch"`
	RequirementID string `json:"requirement_id"`
	ControlID     string `json:"control_id"`
	TestTarget    string `json:"test_target"`
}

type integrationMutantManifest struct {
	Mutants []integrationMutant `json:"mutants"`
}

var requiredServerProofs = map[string][]string{
	"SCN-PERF-CONFIGURED-BOUNDS-001": {"OBL-PERF-CONFIGURED-BOUNDS-PG-LINUX-X64-001"},
	"SCN-PERF-WARM-CONNECT-001":      {"OBL-PERF-WARM-CONNECT-PG-LINUX-X64-001"},
	"SCN-PERF-REBUILD-REQUESTS-001":  {"OBL-PERF-REBUILD-REQUESTS-PG-LINUX-X64-001"},
	"SCN-PERF-SCHEMA-CHECK-001": {
		"OBL-PERF-SCHEMA-CHECK-PG-LINUX-X64-001",
		"OBL-PERF-SCHEMA-CHECK-PROJECTION-FAULT-001",
	},
	"SCN-PERF-SEEDED-EMPTY-STARTUP-001": {
		"OBL-PERF-SEEDED-EMPTY-STARTUP-PG-LINUX-X64-001",
		"OBL-PERF-SEEDED-EMPTY-STARTUP-CONTINUATION-FAULT-001",
		"OBL-PERF-SEEDED-EMPTY-STARTUP-TRANSACTION-FAULT-001",
		"OBL-PERF-SEEDED-EMPTY-STARTUP-TOKEN-FAULT-001",
		"OBL-PERF-SEEDED-EMPTY-STARTUP-ARTIFACT-FAULT-001",
	},
	"SCN-PERF-STEADY-PULL-001": {"OBL-PERF-STEADY-PULL-PG-LINUX-X64-001"},
	"SCN-WAL-ORDER-001": {
		"OBL-WAL-ORDER-PG-LINUX-X64-001",
		"OBL-WAL-NO-LOSS-PG-LINUX-X64-001",
		"OBL-WAL-NO-LOSS-FAULT-LINUX-X64-001",
		"OBL-WAL-ONLY-PUBLICATION-PG-LINUX-X64-001",
		"OBL-WAL-ONLY-PUBLICATION-FAULT-LINUX-X64-001",
		"OBL-WAL-REPLAY-PG-LINUX-X64-001",
		"OBL-WAL-REPLAY-FAULT-LINUX-X64-001",
	},
	"SCN-PULL-DIVERGENT-CHECKPOINTS-001": {"OBL-PULL-DIVERGENT-PG-LINUX-X64-001"},
	"SCN-PULL-HYDRATION-FAILURE-001":     {"OBL-PULL-HYDRATION-PG-LINUX-X64-001"},
	"SCN-WAL-DECODE-FAILURE-001": {
		"OBL-WAL-DECODE-PG-LINUX-X64-001",
		"OBL-WAL-ACK-PG-LINUX-X64-001",
		"OBL-WAL-ACK-FAULT-LINUX-X64-001",
		"OBL-WAL-RESET-LIFECYCLE-PG-LINUX-X64-001",
		"OBL-WAL-RESET-LIFECYCLE-FAULT-LINUX-X64-001",
		"OBL-WAL-RESET-COVERAGE-PG-LINUX-X64-001",
		"OBL-WAL-RESET-COVERAGE-FAULT-LINUX-X64-001",
	},
	"SCN-REGISTRY-RELOAD-001": {
		"OBL-REGISTRY-RELOAD-PG-LINUX-X64-001",
		"OBL-REGISTRY-RELOAD-WAL-008-PG-LINUX-X64-001",
		"OBL-REGISTRY-RELOAD-WAL-008-FAULT-LINUX-X64-001",
	},
	"SCN-PUSH-RESPONSE-LOSS-001": {
		"OBL-PUSH-RESPONSE-LOSS-PG-LINUX-X64-001",
		"OBL-PUSH-RESPONSE-LOSS-FAILURE-003-FAULT-001",
	},
	"SCN-REBUILD-FORGED-CURSOR-001": {"OBL-REBUILD-FORGED-CURSOR-PG-LINUX-X64-001"},
	"SCN-SCHEMA-QUEUED-MUTATION-001": {
		"OBL-SCHEMA-QUEUED-MUTATION-PG-LINUX-X64-001",
		"OBL-SCHEMA-QUEUED-MUTATION-MANIFEST-FAULT-001",
	},
	"SCN-RETENTION-RECONNECT-001": {
		"OBL-RETENTION-RECONNECT-PG-LINUX-X64-001",
		"OBL-RETENTION-RECONNECT-RETENTION-001-FAULT-LINUX-X64-001",
	},
	"SCN-MEMBERSHIP-REASSIGNMENT-001": {
		"OBL-MEMBERSHIP-REASSIGNMENT-PG-LINUX-X64-001",
		"OBL-WAL-FENCE-CORRELATION-PG-LINUX-X64-001",
		"OBL-WAL-FENCE-CORRELATION-FAULT-LINUX-X64-001",
	},
	"SCN-PERF-MULTI-SCOPE-PROVENANCE-001": {
		"OBL-PERF-MULTI-SCOPE-REBUILD-001-PG-LINUX-X64-001",
		"OBL-MEMBERSHIP-GENERATION-PG-LINUX-X64-001",
		"OBL-MEMBERSHIP-GENERATION-FAULT-LINUX-X64-001",
		"OBL-MEMBERSHIP-BACKFILL-PG-LINUX-X64-001",
		"OBL-MEMBERSHIP-BACKFILL-FAULT-LINUX-X64-001",
	},
}

// serverProofBindings is the sole server and fault proof map. Synthetic harness
// runs are layer-6 self-tests and negative controls, so they cannot enter it.
var serverProofBindings = []serverProofBinding{
	{"SCN-WAL-ORDER-001", "OBL-WAL-ORDER-PG-LINUX-X64-001", []string{"TestRealWALPipeline"}},
	{"SCN-PERF-CONFIGURED-BOUNDS-001", "OBL-PERF-CONFIGURED-BOUNDS-PG-LINUX-X64-001", []string{"TestRealConfiguredBoundsMeasurement"}},
	{"SCN-PERF-WARM-CONNECT-001", "OBL-PERF-WARM-CONNECT-PG-LINUX-X64-001", []string{"TestRealIssue49ConnectRejectsFreshReuseAndInvalidEnvelopeValues", "TestRealIssue49SemanticVersionPrecedence", "TestRealIssue49PortableIntegerBoundariesAndCounterOverflow"}},
	{"SCN-PERF-REBUILD-REQUESTS-001", "OBL-PERF-REBUILD-REQUESTS-PG-LINUX-X64-001", []string{"TestRealIssue49RebuildReplayEpochAndMonotonicCursor"}},
	{"SCN-PERF-SCHEMA-CHECK-001", "OBL-PERF-SCHEMA-CHECK-PG-LINUX-X64-001", []string{"TestRealClass3ProjectionBootstrap", "TestRealIssue49RemainingSemantics"}},
	{"SCN-PERF-SCHEMA-CHECK-001", "OBL-PERF-SCHEMA-CHECK-PROJECTION-FAULT-001", []string{"TestRealClass3ProjectionBootstrapRecoversAfterProcessTermination"}},
	{"SCN-PERF-SEEDED-EMPTY-STARTUP-001", "OBL-PERF-SEEDED-EMPTY-STARTUP-PG-LINUX-X64-001", []string{"TestRealIssue49PortableSeedScopeContinuationAndTokenBindings", "TestRealIssue49RemainingSemantics"}},
	{"SCN-PERF-SEEDED-EMPTY-STARTUP-001", "OBL-PERF-SEEDED-EMPTY-STARTUP-CONTINUATION-FAULT-001", []string{"TestRealIssue49RemainingSemantics"}},
	{"SCN-PERF-SEEDED-EMPTY-STARTUP-001", "OBL-PERF-SEEDED-EMPTY-STARTUP-TRANSACTION-FAULT-001", []string{"TestRealIssue49RemainingSemantics"}},
	{"SCN-PERF-SEEDED-EMPTY-STARTUP-001", "OBL-PERF-SEEDED-EMPTY-STARTUP-TOKEN-FAULT-001", []string{"TestRealIssue49RemainingSemantics"}},
	{"SCN-PERF-SEEDED-EMPTY-STARTUP-001", "OBL-PERF-SEEDED-EMPTY-STARTUP-ARTIFACT-FAULT-001", []string{"TestRealIssue49RemainingSemantics"}},
	{"SCN-PERF-STEADY-PULL-001", "OBL-PERF-STEADY-PULL-PG-LINUX-X64-001", []string{"TestRealMutationControlChecksumCorrectness", "TestRealIssue49RebuildReplayEpochAndMonotonicCursor", "TestRealIssue49RemainingSemantics"}},
	{"SCN-PULL-DIVERGENT-CHECKPOINTS-001", "OBL-PULL-DIVERGENT-PG-LINUX-X64-001", []string{"TestRealS02DivergentPullPaginationIsStarvationFree"}},
	{"SCN-PULL-HYDRATION-FAILURE-001", "OBL-PULL-HYDRATION-PG-LINUX-X64-001", []string{"TestRealS03PullHydrationFailurePreservesCursors"}},
	{"SCN-WAL-DECODE-FAILURE-001", "OBL-WAL-DECODE-PG-LINUX-X64-001", []string{"TestRealWALDecodeFailureRepairsSameIdentity"}},
	{"SCN-REGISTRY-RELOAD-001", "OBL-REGISTRY-RELOAD-PG-LINUX-X64-001", []string{"TestRealRegistryGenerationReloadAtCommitBoundary"}},
	{"SCN-REGISTRY-RELOAD-001", "OBL-REGISTRY-RELOAD-WAL-008-PG-LINUX-X64-001", []string{"TestRealIssue49CaptureReadinessRequiresEveryCheck", "TestRealIssue49WALPoisonBlocksContiguousProgress"}},
	{"SCN-REGISTRY-RELOAD-001", "OBL-REGISTRY-RELOAD-WAL-008-FAULT-LINUX-X64-001", []string{"TestRealIssue49CaptureReadinessRequiresEveryCheck", "TestRealIssue49WALPoisonBlocksContiguousProgress"}},
	{"SCN-PUSH-RESPONSE-LOSS-001", "OBL-PUSH-RESPONSE-LOSS-PG-LINUX-X64-001", []string{"TestRealS11PushResponseLossReplaysExactCanonicalResponse", "TestRealIssue49MutationLifecycleVersionsVocabularyAndCrossBatchReplay", "TestRealIssue49RemainingSemantics"}},
	{"SCN-PUSH-RESPONSE-LOSS-001", "OBL-PUSH-RESPONSE-LOSS-FAILURE-003-FAULT-001", []string{"TestRealIssue49RemainingSemantics"}},
	{"SCN-REBUILD-FORGED-CURSOR-001", "OBL-REBUILD-FORGED-CURSOR-PG-LINUX-X64-001", []string{"TestRealS04RebuildRejectsForgedCursorAndFreezesBoundary", "TestRealIssue49RebuildReplayEpochAndMonotonicCursor"}},
	{"SCN-SCHEMA-QUEUED-MUTATION-001", "OBL-SCHEMA-QUEUED-MUTATION-PG-LINUX-X64-001", []string{"TestRealSchemaIncompatibleMutationPersistsCanonicalIntent", "TestRealIssue49PublishedSchemaIdentityIsImmutable"}},
	{"SCN-SCHEMA-QUEUED-MUTATION-001", "OBL-SCHEMA-QUEUED-MUTATION-MANIFEST-FAULT-001", []string{"TestRealIssue49PublishedSchemaIdentityIsImmutable"}},
	{"SCN-RETENTION-RECONNECT-001", "OBL-RETENTION-RECONNECT-PG-LINUX-X64-001", []string{"TestRealS12StaleClientCompactionAndReconnect", "TestRealIssue49RemainingSemantics"}},
	{"SCN-RETENTION-RECONNECT-001", "OBL-RETENTION-RECONNECT-RETENTION-001-FAULT-LINUX-X64-001", []string{"TestRealS12StaleClientCompactionAndReconnect", "TestRealIssue49RemainingSemantics"}},
	{"SCN-MEMBERSHIP-REASSIGNMENT-001", "OBL-MEMBERSHIP-REASSIGNMENT-PG-LINUX-X64-001", []string{"TestRealWALPipeline"}},
	{"SCN-WAL-ORDER-001", "OBL-WAL-NO-LOSS-PG-LINUX-X64-001", []string{"TestRealIssue49CompletePullVisibleWALRepresentation", "TestRealIssue49WALIsTheOnlyAtomicPublicationPath"}},
	{"SCN-WAL-ORDER-001", "OBL-WAL-NO-LOSS-FAULT-LINUX-X64-001", []string{"TestRealIssue49CompletePullVisibleWALRepresentation", "TestRealIssue49WALPoisonBlocksContiguousProgress"}},
	{"SCN-WAL-ORDER-001", "OBL-WAL-ONLY-PUBLICATION-PG-LINUX-X64-001", []string{"TestRealIssue49WALIsTheOnlyAtomicPublicationPath"}},
	{"SCN-WAL-ORDER-001", "OBL-WAL-ONLY-PUBLICATION-FAULT-LINUX-X64-001", []string{"TestRealIssue49WALIsTheOnlyAtomicPublicationPath"}},
	{"SCN-WAL-ORDER-001", "OBL-WAL-REPLAY-PG-LINUX-X64-001", []string{"TestRealIssue49WALIsTheOnlyAtomicPublicationPath"}},
	{"SCN-WAL-ORDER-001", "OBL-WAL-REPLAY-FAULT-LINUX-X64-001", []string{"TestRealIssue49WALIsTheOnlyAtomicPublicationPath"}},
	{"SCN-WAL-DECODE-FAILURE-001", "OBL-WAL-ACK-PG-LINUX-X64-001", []string{"TestRealIssue49WALPoisonBlocksContiguousProgress"}},
	{"SCN-WAL-DECODE-FAILURE-001", "OBL-WAL-ACK-FAULT-LINUX-X64-001", []string{"TestRealIssue49WALPoisonBlocksContiguousProgress"}},
	{"SCN-WAL-DECODE-FAILURE-001", "OBL-WAL-RESET-LIFECYCLE-PG-LINUX-X64-001", []string{"TestRealIssue49ResetLifecycleAndFenceCoverage"}},
	{"SCN-WAL-DECODE-FAILURE-001", "OBL-WAL-RESET-LIFECYCLE-FAULT-LINUX-X64-001", []string{"TestRealIssue49ResetLifecycleAndFenceCoverage"}},
	{"SCN-WAL-DECODE-FAILURE-001", "OBL-WAL-RESET-COVERAGE-PG-LINUX-X64-001", []string{"TestRealIssue49ResetCoversEveryFenceOperation", "TestRealIssue49ResetLifecycleAndFenceCoverage"}},
	{"SCN-WAL-DECODE-FAILURE-001", "OBL-WAL-RESET-COVERAGE-FAULT-LINUX-X64-001", []string{"TestRealIssue49ResetCoversEveryFenceOperation", "TestRealIssue49ResetLifecycleAndFenceCoverage"}},
	{"SCN-MEMBERSHIP-REASSIGNMENT-001", "OBL-WAL-FENCE-CORRELATION-PG-LINUX-X64-001", []string{"TestRealIssue49FenceCorrelationAndCapturePending"}},
	{"SCN-MEMBERSHIP-REASSIGNMENT-001", "OBL-WAL-FENCE-CORRELATION-FAULT-LINUX-X64-001", []string{"TestRealIssue49FenceCorrelationAndCapturePending", "TestRealIssue49FenceCorrelatesOldRecordIdentity", "TestRealIssue49FenceCorrelatesCaptureKeys"}},
	{"SCN-PERF-MULTI-SCOPE-PROVENANCE-001", "OBL-MEMBERSHIP-GENERATION-PG-LINUX-X64-001", []string{"TestRealIssue49MembershipActivationIsStagedAndScoped"}},
	{"SCN-PERF-MULTI-SCOPE-PROVENANCE-001", "OBL-MEMBERSHIP-GENERATION-FAULT-LINUX-X64-001", []string{"TestRealIssue49MembershipBackfillRetainsContinuationAcrossWorkerLoss"}},
	{"SCN-PERF-MULTI-SCOPE-PROVENANCE-001", "OBL-MEMBERSHIP-BACKFILL-PG-LINUX-X64-001", []string{"TestRealIssue49MembershipActivationIsStagedAndScoped"}},
	{"SCN-PERF-MULTI-SCOPE-PROVENANCE-001", "OBL-MEMBERSHIP-BACKFILL-FAULT-LINUX-X64-001", []string{"TestRealIssue49MembershipBackfillRetainsContinuationAcrossWorkerLoss"}},
	{"SCN-PERF-MULTI-SCOPE-PROVENANCE-001", "OBL-PERF-MULTI-SCOPE-REBUILD-001-PG-LINUX-X64-001", []string{"TestRealS05SelectiveRebuildPreservesCheckpoints"}},
}

var nonScenarioRealTests = map[string]string{
	"TestRealClass3ProjectionBootstrap":                                "regression",
	"TestRealClass3ProjectionBootstrapRecoversAfterProcessTermination": "regression",
	"TestRealExtensionReinstallRebindsWorkerSlot":                      "regression",
	"TestRealHTTPHarness":                                              "framework",
	"TestRealMutationControlCursorAdvancement":                         "adversarial",
	"TestRealMutationControlMutationConservation":                      "adversarial",
	"TestRealNativeCaptureServerObservationSignals":                    "regression",
	"TestRealMutationControlProgressOrder":                             "adversarial",
	"TestRealMutationControlScopeIsolation":                            "adversarial",
	"TestRealMutationControlWALAcknowledgement":                        "adversarial",
	"TestRealR1PerformanceBenchmark":                                   "benchmark",
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

func TestIssue49WALMutantsMapToAuthoredControlsAndRealProofs(t *testing.T) {
	authored, declarations := loadServerProofMapInputs(t)
	repoRoot, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	manifestBytes, err := os.ReadFile(filepath.Join(repoRoot, "conformance", "mutants", "integration", "manifest.json"))
	if err != nil {
		t.Fatalf("read integration mutant manifest: %v", err)
	}
	var manifest integrationMutantManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatalf("decode integration mutant manifest: %v", err)
	}
	patches, err := filepath.Glob(filepath.Join(repoRoot, "conformance", "mutants", "integration", "issue49-wal-*.patch"))
	if err != nil {
		t.Fatalf("discover issue49 WAL mutants: %v", err)
	}
	for index, patch := range patches {
		relative, err := filepath.Rel(repoRoot, patch)
		if err != nil {
			t.Fatalf("resolve mutant path %s: %v", patch, err)
		}
		patches[index] = filepath.ToSlash(relative)
	}
	if failures := validateIssue49WALMutantProofs(authored, declarations, serverProofBindings, manifest.Mutants, patches); len(failures) > 0 {
		t.Fatalf("issue49 WAL mutant proof map drift:\n%s", strings.Join(failures, "\n"))
	}
	mutated := append([]integrationMutant(nil), manifest.Mutants...)
	for index := range mutated {
		if !strings.HasPrefix(mutated[index].Patch, "conformance/mutants/integration/issue49-wal-") {
			continue
		}
		mutated[index].TestTarget = "TestRealMissingWALProof"
		expected := fmt.Sprintf("issue49 WAL mutant %s names unknown real test TestRealMissingWALProof", mutated[index].Patch)
		failures := validateIssue49WALMutantProofs(authored, declarations, serverProofBindings, mutated, patches)
		if !containsFailure(failures, expected) {
			t.Fatalf("mutant failures = %v, want %q", failures, expected)
		}
		return
	}
	t.Fatal("integration mutant manifest has no issue49 WAL row")
}

func TestServerProofMapRejectsDrift(t *testing.T) {
	authored, declarations := loadServerProofMapInputs(t)
	tests := []struct {
		name     string
		expected string
		mutate   func([]serverProofBinding, map[string]string, map[string]realTestDeclaration) ([]serverProofBinding, map[string]string, map[string]realTestDeclaration)
	}{
		{"renamed real test", "proof binding SCN-WAL-ORDER-001|OBL-WAL-ORDER-PG-LINUX-X64-001 names unknown real test TestRealRenamed", func(bindings []serverProofBinding, classifications map[string]string, declarations map[string]realTestDeclaration) ([]serverProofBinding, map[string]string, map[string]realTestDeclaration) {
			bindings[0].testNames = []string{"TestRealRenamed"}
			return bindings, classifications, declarations
		}},
		{"synthetic harness test", "proof binding SCN-WAL-ORDER-001|OBL-WAL-ORDER-PG-LINUX-X64-001 names non-real test TestRunSyntheticHarnessDetectsSemanticFaults", func(bindings []serverProofBinding, classifications map[string]string, declarations map[string]realTestDeclaration) ([]serverProofBinding, map[string]string, map[string]realTestDeclaration) {
			bindings[0].testNames = []string{"TestRunSyntheticHarnessDetectsSemanticFaults"}
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
		if len(binding.testNames) == 0 {
			failures = append(failures, "proof binding "+key+" names no real tests")
		}
		for _, testName := range binding.testNames {
			if !strings.HasPrefix(testName, "TestReal") {
				failures = append(failures, fmt.Sprintf("proof binding %s names non-real test %s", key, testName))
			} else if declaration, found := declarations[testName]; !found {
				failures = append(failures, fmt.Sprintf("proof binding %s names unknown real test %s", key, testName))
			} else if strings.Contains(binding.obligationID, "-LINUX-X64-") && !declaration.linuxX64 {
				failures = append(failures, fmt.Sprintf("proof binding %s names real test %s unavailable on linux-x64", key, testName))
			}
			mappedTests[testName] = struct{}{}
		}

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
			expectedProofType := "server-black-box"
			if strings.Contains(binding.obligationID, "-FAULT-") {
				expectedProofType = "fault-injection"
			}
			if obligation.ProofType != expectedProofType {
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

func validateIssue49WALMutantProofs(authored []scenarios.Scenario, declarations map[string]realTestDeclaration, bindings []serverProofBinding, mutants []integrationMutant, patches []string) []string {
	type controlOwner struct {
		scenarioID  string
		requirement string
	}
	scenarioByID := make(map[string]scenarios.Scenario, len(authored))
	controlOwners := make(map[string][]controlOwner)
	for _, scenario := range authored {
		scenarioID := string(scenario.ID)
		scenarioByID[scenarioID] = scenario
		for _, obligation := range scenario.ProofObligations {
			if obligation.ProofType != "negative-control" || obligation.ControlID == nil || len(obligation.RequirementIDs) != 1 {
				continue
			}
			controlID := string(*obligation.ControlID)
			controlOwners[controlID] = append(controlOwners[controlID], controlOwner{
				scenarioID:  scenarioID,
				requirement: string(obligation.RequirementIDs[0]),
			})
		}
	}

	expectedPatches := make(map[string]struct{}, len(patches))
	for _, patch := range patches {
		expectedPatches[patch] = struct{}{}
	}
	mappedPatches := make(map[string]struct{}, len(expectedPatches))
	var failures []string
	if len(expectedPatches) == 0 {
		failures = append(failures, "no issue49 WAL mutant patches were discovered")
	}
	for _, mutant := range mutants {
		if !strings.HasPrefix(mutant.Patch, "conformance/mutants/integration/issue49-wal-") {
			continue
		}
		if _, duplicate := mappedPatches[mutant.Patch]; duplicate {
			failures = append(failures, "duplicate issue49 WAL mutant mapping "+mutant.Patch)
			continue
		}
		mappedPatches[mutant.Patch] = struct{}{}
		if _, found := expectedPatches[mutant.Patch]; !found {
			failures = append(failures, "issue49 WAL mutant mapping names unknown patch "+mutant.Patch)
		}

		owners := controlOwners[mutant.ControlID]
		if len(owners) != 1 {
			failures = append(failures, fmt.Sprintf("issue49 WAL mutant %s control %s has %d authored owners", mutant.Patch, mutant.ControlID, len(owners)))
			continue
		}
		owner := owners[0]
		if owner.requirement != mutant.RequirementID {
			failures = append(failures, fmt.Sprintf("issue49 WAL mutant %s requirement %s does not match authored control requirement %s", mutant.Patch, mutant.RequirementID, owner.requirement))
		}
		declaration, found := declarations[mutant.TestTarget]
		if !found {
			failures = append(failures, fmt.Sprintf("issue49 WAL mutant %s names unknown real test %s", mutant.Patch, mutant.TestTarget))
		} else if !declaration.linuxX64 {
			failures = append(failures, fmt.Sprintf("issue49 WAL mutant %s names real test %s unavailable on linux-x64", mutant.Patch, mutant.TestTarget))
		}

		realProofFound := false
		for _, binding := range bindings {
			if binding.scenarioID != owner.scenarioID || !containsString(binding.testNames, mutant.TestTarget) {
				continue
			}
			scenario := scenarioByID[owner.scenarioID]
			for _, obligation := range scenario.ProofObligations {
				if string(obligation.ObligationID) == binding.obligationID && containsContractRequirement(obligation, mutant.RequirementID) {
					realProofFound = true
					break
				}
			}
		}
		if !realProofFound {
			failures = append(failures, fmt.Sprintf("issue49 WAL mutant %s test %s has no matching real proof binding", mutant.Patch, mutant.TestTarget))
		}
	}
	for patch := range expectedPatches {
		if _, found := mappedPatches[patch]; !found {
			failures = append(failures, "missing issue49 WAL mutant mapping "+patch)
		}
	}

	sort.Strings(failures)
	return failures
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func containsContractRequirement(obligation scenarios.ProofObligation, target string) bool {
	for _, requirementID := range obligation.RequirementIDs {
		if string(requirementID) == target {
			return true
		}
	}
	return false
}

func containsFailure(failures []string, expected string) bool {
	for _, failure := range failures {
		if failure == expected {
			return true
		}
	}
	return false
}
