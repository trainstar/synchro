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
		RebuildReceipts: []rebuildReceiptRecord{{
			RebuildIDFingerprint:    cursorFingerprint("rebuild-a"),
			PageCount:               1,
			ReturnedRecordCount:     1,
			RequestChainExpected:    []string{"final"},
			RequestChainObserved:    []string{"partial"},
			RecordsInCanonicalOrder: true,
			RowChecksumsValid:       true,
			ComputedScopeChecksum:   pointerString("computed"),
		}},
		ScopeStates: []scopeStateRecord{{ScopeID: "scope-a"}},
	}
	if err := validateRebuildRequestsRestart(snapshot); err != nil {
		t.Fatalf("validate partial rebuild restart: %v", err)
	}
	snapshot.ScopeStates[0].Checksum = pointerString("checksum")
	if err := validateRebuildRequestsRestart(snapshot); err == nil {
		t.Fatal("partial rebuild restart accepted an assigned scope checksum")
	}
	snapshot.ScopeStates[0].Checksum = nil
	for name, mutate := range map[string]func(*rebuildReceiptRecord){
		"page count":   func(receipt *rebuildReceiptRecord) { receipt.PageCount = 2 },
		"record count": func(receipt *rebuildReceiptRecord) { receipt.ReturnedRecordCount = 2 },
		"complete request chain": func(receipt *rebuildReceiptRecord) {
			receipt.RequestChainObserved = append([]string(nil), receipt.RequestChainExpected...)
		},
		"final checksum": func(receipt *rebuildReceiptRecord) {
			receipt.FinalScopeChecksum = pointerString("final")
		},
		"stored checksum": func(receipt *rebuildReceiptRecord) {
			receipt.StoredScopeChecksum = pointerString("stored")
		},
	} {
		t.Run(name, func(t *testing.T) {
			invalid := snapshot
			invalid.RebuildReceipts = append([]rebuildReceiptRecord(nil), snapshot.RebuildReceipts...)
			mutate(&invalid.RebuildReceipts[0])
			if err := validateRebuildRequestsRestart(invalid); err == nil {
				t.Fatalf("partial rebuild restart accepted receipt with %s", name)
			}
		})
	}
}

func TestRebuildRequestsFinalReceiptRejectsEmptyRequestChain(t *testing.T) {
	checksum := "checksum"
	receipt := rebuildReceiptRecord{
		RebuildIDFingerprint:    cursorFingerprint("rebuild-a"),
		PageCount:               2,
		ReturnedRecordCount:     2,
		RecordsInCanonicalOrder: true,
		RowChecksumsValid:       true,
		ComputedScopeChecksum:   &checksum,
		FinalScopeChecksum:      &checksum,
	}
	if validateRebuildRequestsReceipt(receipt, "rebuild-a", 2, 2, true, true) {
		t.Fatal("final rebuild receipt accepted an empty request chain")
	}
}
