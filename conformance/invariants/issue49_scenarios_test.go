package invariants

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/trainstar/synchro/conformance/faults"
)

var issue49RequirementIDs = []string{
	"SYNC-CURSOR-001",
	"SYNC-VERSION-001",
	"SYNC-SCOPE-002",
	"SYNC-CRUD-001",
	"SYNC-MUTATION-001",
	"SYNC-MUTATION-003",
	"SYNC-REBUILD-001",
	"SYNC-REBUILD-004",
	"SYNC-REBUILD-005",
	"SYNC-CURSOR-002",
	"SYNC-CURSOR-004",
	"SYNC-INTEGRITY-001",
	"SYNC-PROVENANCE-001",
	"SYNC-CONFLICT-001",
	"SYNC-CONFLICT-002",
	"SYNC-TIME-002",
	"SYNC-VOCAB-001",
	"SYNC-IDEMPOTENCY-002",
	"SYNC-PULL-004",
	"SYNC-CURSOR-005",
	"SYNC-INTEGRITY-003",
	"SYNC-INTEGRITY-004",
	"SYNC-INTEGRITY-005",
	"SYNC-INTEGRITY-006",
	"SYNC-STATE-001",
	"SYNC-CLIENT-VERSION-001",
	"SYNC-PROTOCOL-004",
	"SYNC-OUTCOME-002",
	"SYNC-PULL-005",
	"SYNC-REBUILD-008",
	"SYNC-REBUILD-009",
	"SYNC-REBUILD-010",
	"SYNC-REBUILD-011",
	"SYNC-SCHEMA-005",
	"SYNC-SCHEMA-006",
	"SYNC-SCHEMA-007",
	"SYNC-RETENTION-001",
	"SYNC-QUEUE-004",
	"SYNC-SCOPE-006",
	"SYNC-SCHEMA-003",
}

var (
	issue49CatalogOnce sync.Once
	issue49Catalog     *faults.Catalog
	issue49CatalogErr  error
)

func TestIssue49NormativeControlScenariosAreComplete(t *testing.T) {
	seen := make(map[string]struct{}, len(issue49RequirementIDs))
	for _, requirementID := range issue49RequirementIDs {
		if _, duplicate := seen[requirementID]; duplicate {
			t.Fatalf("duplicate Issue 49 requirement ID %q", requirementID)
		}
		seen[requirementID] = struct{}{}
		issue49ControlScenario(t, requirementID)
	}
	if len(seen) != 40 {
		t.Fatalf("Issue 49 requirement count = %d, want 40", len(seen))
	}
}

func issue49Proof(t *testing.T, requirementID string, positive, mutant bool) {
	t.Helper()
	issue49ControlScenario(t, requirementID)
	if !positive {
		t.Fatalf("%s positive transition violated its independent predicate", requirementID)
	}
	if mutant {
		t.Fatalf("%s independent predicate accepted its concrete state mutant", requirementID)
	}
}

func issue49ControlScenario(t *testing.T, requirementID string) faults.Control {
	t.Helper()
	catalog := issue49FaultCatalog(t)
	wantControlID := "CTRL-" + requirementID[len("SYNC-"):]
	var matched []faults.Control
	for _, control := range catalog.Controls {
		if len(control.RequirementIDs) == 1 && control.RequirementIDs[0] == requirementID {
			matched = append(matched, control)
		}
	}
	if len(matched) != 1 {
		t.Fatalf("%s normative control count = %d, want 1", requirementID, len(matched))
	}
	control := matched[0]
	if control.ID != wantControlID || control.FaultID == "" || control.Injection.Parameters.Scenario == "" || control.Injection.Parameters.Defect == "" || control.ExpectedDetection == "" {
		t.Fatalf("%s normative control scenario is incomplete: %#v", requirementID, control)
	}
	return control
}

func issue49FaultCatalog(t *testing.T) *faults.Catalog {
	t.Helper()
	issue49CatalogOnce.Do(func() {
		_, source, _, ok := runtime.Caller(0)
		if !ok {
			issue49CatalogErr = errors.New("resolve Issue 49 source path")
			return
		}
		repoRoot := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
		issue49Catalog, issue49CatalogErr = faults.LoadCatalog(context.Background(), repoRoot)
	})
	if issue49CatalogErr != nil {
		t.Fatalf("load normative fault scenarios: %v", issue49CatalogErr)
	}
	return issue49Catalog
}
