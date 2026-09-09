package integration

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

type issue49SecurityProofBinding struct {
	mutantPatch      string
	scenarioID       string
	requirementID    string
	assertionID      string
	controlID        string
	faultPlanID      string
	serverObligation string
	faultObligation  string
	realTest         string
}

// issue49SecurityProofBindings assigns each Issue 49 security mutant to its
// authored control. An empty mutantPatch supplies the second installation
// requirement proved by the same real test.
var issue49SecurityProofBindings = []issue49SecurityProofBinding{
	{"issue49-security-adapter-boundary.patch", "SCN-PERF-CORE-SYNC-PATH-001", "SYNC-BOUNDARY-001", "ASSERT-PERF-CORE-SYNC-PATH-BOUNDARY-001", "CTRL-BOUNDARY-001", "FPL-PERF-CORE-SYNC-PATH-BOUNDARY-001", "OBL-PERF-CORE-SYNC-PATH-BOUNDARY-PG-LINUX-X64-001", "", "TestRealIssue49SecurityAdapterAuthorityAndScopeBoundary"},
	{"issue49-security-client-scope.patch", "SCN-PERF-SHARED-PRIVATE-SCOPES-001", "SYNC-SCOPE-005", "ASSERT-PERF-SHARED-PRIVATE-SCOPES-SCOPE-005", "CTRL-SCOPE-005", "FPL-PERF-SHARED-PRIVATE-SCOPES-SCOPE-005", "OBL-PERF-SHARED-PRIVATE-SCOPES-SCOPE-005-PG-LINUX-X64-001", "", "TestRealIssue49SecurityAdapterAuthorityAndScopeBoundary"},
	{"issue49-security-registry-identity.patch", "SCN-PERF-FANOUT-001", "SYNC-REGISTRY-001", "ASSERT-PERF-FANOUT-REGISTRY-001", "CTRL-REGISTRY-001", "FPL-PERF-FANOUT-REGISTRY-001", "OBL-PERF-FANOUT-REGISTRY-001-PG-LINUX-X64-001", "OBL-PERF-FANOUT-REGISTRY-001-FAULT-001", "TestRealIssue49SecurityRegistryIdentityAndKeys"},
	{"issue49-security-registry-key-update.patch", "SCN-PERF-FANOUT-001", "SYNC-REGISTRY-002", "ASSERT-PERF-FANOUT-REGISTRY-002", "CTRL-REGISTRY-002", "FPL-PERF-FANOUT-REGISTRY-002", "OBL-PERF-FANOUT-REGISTRY-002-PG-LINUX-X64-001", "", "TestRealIssue49SecurityRegistryIdentityAndKeys"},
	{"issue49-security-health-aggregation.patch", "SCN-PERF-CONFIGURED-BOUNDS-001", "SYNC-HEALTH-001", "ASSERT-PERF-CONFIGURED-BOUNDS-HEALTH-001", "CTRL-HEALTH-001", "FPL-PERF-CONFIGURED-BOUNDS-HEALTH-001", "OBL-PERF-CONFIGURED-BOUNDS-HEALTH-001-PG-LINUX-X64-001", "OBL-PERF-CONFIGURED-BOUNDS-HEALTH-001-FAULT-001", "TestRealIssue49SecurityCaptureHealthFailsClosed"},
	{"issue49-security-health-limits.patch", "SCN-PERF-CONFIGURED-BOUNDS-001", "SYNC-HEALTH-002", "ASSERT-PERF-CONFIGURED-BOUNDS-HEALTH-002", "CTRL-HEALTH-002", "FPL-PERF-CONFIGURED-BOUNDS-HEALTH-002", "OBL-PERF-CONFIGURED-BOUNDS-HEALTH-002-PG-LINUX-X64-001", "OBL-PERF-CONFIGURED-BOUNDS-HEALTH-002-FAULT-001", "TestRealIssue49SecurityCaptureHealthFailsClosed"},
	{"issue49-security-public-authority.patch", "SCN-PERF-CORE-SYNC-PATH-001", "SYNC-DBAUTH-001", "ASSERT-PERF-CORE-SYNC-PATH-DBAUTH-001", "CTRL-DBAUTH-001", "FPL-PERF-CORE-SYNC-PATH-DBAUTH-001-001", "OBL-PERF-CORE-SYNC-PATH-DBAUTH-001-PG-LINUX-X64-001", "", "TestRealIssue49SecurityDatabaseAuthority"},
	{"issue49-security-role-separation.patch", "SCN-PERF-CORE-SYNC-PATH-001", "SYNC-DBAUTH-002", "ASSERT-PERF-CORE-SYNC-PATH-DBAUTH-002", "CTRL-DBAUTH-002", "FPL-PERF-CORE-SYNC-PATH-DBAUTH-002-001", "OBL-PERF-CORE-SYNC-PATH-DBAUTH-002-PG-LINUX-X64-001", "", "TestRealIssue49SecurityDatabaseAuthority"},
	{"issue49-security-logging-redaction.patch", "SCN-PERF-CORE-SYNC-PATH-001", "SYNC-LOGGING-001", "ASSERT-PERF-CORE-SYNC-PATH-LOGGING-001", "CTRL-LOGGING-001", "FPL-PERF-CORE-SYNC-PATH-LOGGING-001", "OBL-PERF-CORE-SYNC-PATH-LOGGING-PG-LINUX-X64-001", "OBL-PERF-CORE-SYNC-PATH-LOGGING-FAULT-001", "TestRealIssue49SecurityOperationalRedaction"},
	{"issue49-security-install-baseline.patch", "SCN-PERF-CORE-SYNC-PATH-001", "SYNC-INSTALL-002", "ASSERT-PERF-CORE-SYNC-PATH-INSTALL-002", "CTRL-INSTALL-002", "FPL-PERF-CORE-SYNC-PATH-INSTALL-002-001", "OBL-PERF-CORE-SYNC-PATH-INSTALL-002-PG-LINUX-X64-001", "", "TestRealIssue49SecurityInstallationAuthority"},
	{"", "SCN-PERF-CORE-SYNC-PATH-001", "SYNC-INSTALL-001", "ASSERT-PERF-CORE-SYNC-PATH-INSTALL-001", "CTRL-INSTALL-001", "FPL-PERF-CORE-SYNC-PATH-INSTALL-001-001", "OBL-PERF-CORE-SYNC-PATH-INSTALL-001-PG-LINUX-X64-001", "", "TestRealIssue49SecurityInstallationAuthority"},
}

func TestIssue49SecurityProofBindings(t *testing.T) {
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
	byID := make(map[string]scenarios.Scenario, len(authored))
	for _, scenario := range authored {
		byID[string(scenario.ID)] = scenario
	}
	seenPatches := make(map[string]struct{})
	for _, binding := range issue49SecurityProofBindings {
		if binding.mutantPatch != "" {
			seenPatches[binding.mutantPatch] = struct{}{}
			if _, err := os.Stat(filepath.Join(repoRoot, "conformance", "mutants", "integration", binding.mutantPatch)); err != nil {
				t.Fatalf("inspect mutant %s: %v", binding.mutantPatch, err)
			}
		}
		scenario, found := byID[binding.scenarioID]
		if !found {
			t.Fatalf("binding names unknown scenario %s", binding.scenarioID)
		}
		assertIssue49SecurityBinding(t, scenario, binding)
		declaration, found := declarations[binding.realTest]
		if !found || !declaration.validSignature || !declaration.linuxX64 {
			t.Fatalf("binding names unavailable real test %s", binding.realTest)
		}
	}

	entries, err := os.ReadDir(filepath.Join(repoRoot, "conformance", "mutants", "integration"))
	if err != nil {
		t.Fatalf("list security mutants: %v", err)
	}
	var actual []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "issue49-security-") && strings.HasSuffix(entry.Name(), ".patch") {
			actual = append(actual, entry.Name())
		}
	}
	sort.Strings(actual)
	want := make([]string, 0, len(seenPatches))
	for patch := range seenPatches {
		want = append(want, patch)
	}
	sort.Strings(want)
	if len(actual) != len(want) {
		t.Fatalf("security mutant bindings = %v, want %v", want, actual)
	}
	for index := range actual {
		if actual[index] != want[index] {
			t.Fatalf("security mutant bindings = %v, want %v", want, actual)
		}
	}
}

func assertIssue49SecurityBinding(t *testing.T, scenario scenarios.Scenario, binding issue49SecurityProofBinding) {
	t.Helper()
	planFound, controlFound, assertionFound, serverFound, faultFound := false, false, false, false, binding.faultObligation == ""
	for _, plan := range scenario.FaultPlans {
		if string(plan.ID) == binding.faultPlanID && string(plan.RequirementID) == binding.requirementID && string(plan.ControlID) == binding.controlID &&
			len(plan.ExpectedAssertionIDs) == 1 && string(plan.ExpectedAssertionIDs[0]) == binding.assertionID {
			planFound = true
		}
	}
	for _, control := range scenario.NegativeControls {
		if string(control.ControlID) == binding.controlID && string(control.RequirementID) == binding.requirementID && string(control.FaultID) != "" &&
			len(control.DetectedBy) == 1 && string(control.DetectedBy[0]) == binding.assertionID {
			controlFound = true
		}
	}
	for _, assertion := range scenario.Assertions {
		if string(assertion.ID) == binding.assertionID && len(assertion.RequirementIDs) == 1 && string(assertion.RequirementIDs[0]) == binding.requirementID &&
			len(assertion.DetectsControlIDs) == 1 && string(assertion.DetectsControlIDs[0]) == binding.controlID {
			assertionFound = true
		}
	}
	for _, obligation := range scenario.ProofObligations {
		if string(obligation.ObligationID) == binding.serverObligation && obligation.ProofType == "server-black-box" {
			serverFound = true
		}
		if string(obligation.ObligationID) == binding.faultObligation && obligation.ProofType == "fault-injection" {
			faultFound = true
		}
	}
	if !planFound || !controlFound || !assertionFound || !serverFound || !faultFound {
		t.Fatalf("incomplete Issue 49 security binding: %#v", binding)
	}
}
