package swift

import (
	"encoding/json"
	"testing"
)

func TestRebuildRequestsPauseRequiresUninitializedAssignment(t *testing.T) {
	attempt := rebuildAttemptRecord{ScopeID: "scope-a", RebuildID: "rebuild-a", PageLimit: 1}
	snapshot := runnerResult{
		RebuildAttempts: []rebuildAttemptRecord{attempt},
		ScopeStates:     []scopeStateRecord{{ScopeID: "scope-a"}},
	}
	if err := validateRebuildRequestsFirstPause(snapshot); err != nil {
		t.Fatalf("validate assigned rebuild pause: %v", err)
	}

	for name, mutate := range map[string]func(*scopeStateRecord){
		"cursor":   func(scope *scopeStateRecord) { scope.Cursor = pointerString("cursor") },
		"checksum": func(scope *scopeStateRecord) { scope.Checksum = pointerString("checksum") },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := snapshot
			invalid.ScopeStates = append([]scopeStateRecord(nil), snapshot.ScopeStates...)
			mutate(&invalid.ScopeStates[0])
			if err := validateRebuildRequestsFirstPause(invalid); err == nil {
				t.Fatalf("rebuild pause accepted an assigned scope with %s", name)
			}
		})
	}
	mismatched := snapshot
	mismatched.ScopeStates = []scopeStateRecord{{ScopeID: "scope-b"}}
	if err := validateRebuildRequestsFirstPause(mismatched); err == nil {
		t.Fatal("rebuild pause accepted an assignment for another scope")
	}
}

func TestRebuildRequestsPauseRejectsAppliedApplicationRow(t *testing.T) {
	snapshot := runnerResult{
		ApplicationRows: []map[string]json.RawMessage{{}},
		RebuildAttempts: []rebuildAttemptRecord{{ScopeID: "scope-a", RebuildID: "rebuild-a", PageLimit: 1}},
		ScopeStates:     []scopeStateRecord{{ScopeID: "scope-a"}},
	}
	if err := validateRebuildRequestsFirstPause(snapshot); err == nil {
		t.Fatal("rebuild pause accepted an application row before local apply")
	}
}

func TestRebuildRequestsRestartPreservesUninitializedAssignment(t *testing.T) {
	cursor := "rebuild-page-cursor"
	snapshot := runnerResult{
		ApplicationRows:    []map[string]json.RawMessage{{}},
		ScopeRows:          []scopeRowRecord{{ScopeID: "scope-a"}},
		RowMetadataRecords: []rowMetadataRecord{{RecordID: "row-a"}},
		RebuildAttempts:    []rebuildAttemptRecord{{ScopeID: "scope-a", RebuildID: "rebuild-a", Cursor: &cursor, PageLimit: 1}},
		ScopeStates:        []scopeStateRecord{{ScopeID: "scope-a"}},
	}
	if err := validateRebuildRequestsRestart(snapshot); err != nil {
		t.Fatalf("validate partial rebuild restart: %v", err)
	}
	snapshot.ScopeStates[0].Checksum = pointerString("checksum")
	if err := validateRebuildRequestsRestart(snapshot); err == nil {
		t.Fatal("partial rebuild restart accepted an assigned scope checksum")
	}
}
