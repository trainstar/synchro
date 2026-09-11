package kotlin

import "testing"

func TestRebuildRequestsFirstPauseAcceptsUninitializedAssignment(t *testing.T) {
	pageLimit := 1
	zero := 0
	snapshot := warmConnectSnapshot{
		scopeStates:     []scopeStateRecord{{ScopeID: "scope-a"}},
		rebuildAttempts: []rebuildAttemptRecord{{ScopeID: "scope-a", RebuildID: "rebuild-a", PageLimit: pageLimit}},
	}
	if err := validateKotlinRebuildRequestsFirstPause(snapshot); err != nil {
		t.Fatalf("validate first rebuild pause: %v", err)
	}
	cursor := "cursor-a"
	snapshot.scopeStates[0].Cursor = &cursor
	if err := validateKotlinRebuildRequestsFirstPause(snapshot); err == nil {
		t.Fatal("initialized assignment passed first rebuild pause validation")
	}
	snapshot.scopeStates[0].Cursor = nil
	snapshot.scopeStates[0].ScopeID = "scope-b"
	if err := validateKotlinRebuildRequestsFirstPause(snapshot); err == nil {
		t.Fatal("mismatched assignment passed first rebuild pause validation")
	}
	snapshot.scopeStates[0].ScopeID = "scope-a"
	snapshot.result.ApplicationRowCount = &zero
	if err := validateKotlinRebuildRequestsFirstRestart(snapshot); err != nil {
		t.Fatalf("validate first rebuild restart: %v", err)
	}
	one := 1
	snapshot.result.ApplicationRowCount = &one
	snapshot.rebuildAttempts[0].Cursor = &cursor
	snapshot.scopeRows = []scopeRowRecord{{}}
	snapshot.rowMetadata = []rowMetadataRecord{{}}
	if err := validateKotlinRebuildRequestsRestart(snapshot); err != nil {
		t.Fatalf("validate partial rebuild restart: %v", err)
	}
}
