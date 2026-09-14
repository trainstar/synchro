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
	snapshot.rebuildReceiptProofs = []rebuildReceiptProofRecord{{
		RebuildIDFingerprint:      cursorFingerprint("rebuild-a"),
		PageCount:                 1,
		ReturnedRecordCount:       1,
		RequestChainValid:         false,
		RecordsInCanonicalOrder:   true,
		RowChecksumsValid:         true,
		ScopeChecksumValid:        false,
		FinalChecksumMatchesLocal: false,
	}}
	if err := validateKotlinRebuildRequestsRestart(snapshot); err != nil {
		t.Fatalf("validate partial rebuild restart: %v", err)
	}
	for name, mutate := range map[string]func(*rebuildReceiptProofRecord){
		"page count":                  func(receipt *rebuildReceiptProofRecord) { receipt.PageCount = 2 },
		"record count":                func(receipt *rebuildReceiptProofRecord) { receipt.ReturnedRecordCount = 2 },
		"complete request chain":      func(receipt *rebuildReceiptProofRecord) { receipt.RequestChainValid = true },
		"final scope checksum":        func(receipt *rebuildReceiptProofRecord) { receipt.ScopeChecksumValid = true },
		"stored local scope checksum": func(receipt *rebuildReceiptProofRecord) { receipt.FinalChecksumMatchesLocal = true },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := snapshot
			invalid.rebuildReceiptProofs = append([]rebuildReceiptProofRecord(nil), snapshot.rebuildReceiptProofs...)
			mutate(&invalid.rebuildReceiptProofs[0])
			if err := validateKotlinRebuildRequestsRestart(invalid); err == nil {
				t.Fatalf("partial rebuild restart accepted receipt with %s", name)
			}
		})
	}
}
