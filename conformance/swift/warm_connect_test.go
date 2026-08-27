package swift

import (
	"encoding/json"
	"testing"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestWarmConnectTransportRequiresCompletedRebuildReceiptProof(t *testing.T) {
	generation := int64(1)
	scopeSetVersion := int64(1)
	rebuildID := "00000000-0000-4000-8000-000000009001"
	rebuildFingerprint := cursorFingerprint(rebuildID)
	schemaHash := "721d2c95e6f34cd9733feea9f5118fba391eee10d07663dad066cfc59439fa44"
	zeroScopes := 0
	oneScope := 1
	complete := true
	rebuildCursor := cursorFingerprint("cursor-a")
	bootstrapCursor := cursorFingerprint("cursor-b")
	warmCursor := cursorFingerprint("cursor-c")
	durableCursor := "cursor-c"
	request := func() *transportRequestFacts {
		return &transportRequestFacts{
			ClientGeneration: &generation,
			SchemaVersion:    1,
			SchemaHash:       schemaHash,
		}
	}
	bootstrapConnect := request()
	bootstrapConnect.ScopeCount = &zeroScopes
	bootstrap := []transportObservation{{RequestFacts: bootstrapConnect}, {RequestFacts: request()}, {RequestFacts: request()}}
	bootstrap[1].RequestFacts.RebuildIDFingerprint = &rebuildFingerprint
	bootstrap[1].RebuildResponseFacts = &transportRebuildResponseFacts{
		HasFinalScopeCursor:         true,
		HasChecksum:                 true,
		FinalScopeCursorFingerprint: &rebuildCursor,
	}
	bootstrap[2].RequestFacts.ScopeSetVersion = &scopeSetVersion
	bootstrap[2].RequestFacts.ScopeCount = &oneScope
	bootstrap[2].CursorFingerprints = []string{rebuildCursor}
	bootstrap[2].CursorFingerprintsComplete = &complete
	bootstrap[2].PullResponseFacts = &transportPullResponseFacts{
		ScopeCursorFingerprints:         []string{bootstrapCursor},
		ScopeCursorFingerprintsComplete: true,
	}
	warm := []transportObservation{{RequestFacts: request()}, {RequestFacts: request()}}
	warm[0].RequestFacts.ScopeSetVersion = &scopeSetVersion
	warm[0].RequestFacts.ScopeCount = &oneScope
	warm[1].RequestFacts.ScopeSetVersion = &scopeSetVersion
	warm[1].RequestFacts.ScopeCount = &oneScope
	warm[1].CursorFingerprints = []string{bootstrapCursor}
	warm[1].CursorFingerprintsComplete = &complete
	warm[1].PullResponseFacts = &transportPullResponseFacts{
		ScopeCursorFingerprints:         []string{warmCursor},
		ScopeCursorFingerprintsComplete: true,
	}
	runtime := map[string]json.RawMessage{
		"client-a-generation":   json.RawMessage(`1`),
		"scope-set-version-one": json.RawMessage(`1`),
		"baseline-rebuild":      json.RawMessage(`"` + rebuildID + `"`),
		"current-schema":        json.RawMessage(`{"version":1,"hash":"` + schemaHash + `"}`),
	}
	validReceipt := rebuildReceiptRecord{
		RebuildIDFingerprint:  rebuildFingerprint,
		PageCount:             1,
		ReturnedRecordCount:   0,
		RequestChainExpected:  []string{"final"},
		RequestChainObserved:  []string{"final"},
		RecordIdentitiesHex:   []string{},
		ReceivedRowChecksums:  []string{},
		ComputedRowChecksums:  []string{},
		ComputedScopeChecksum: pointerString("scope-checksum"),
		FinalScopeChecksum:    pointerString("scope-checksum"),
	}
	validSnapshot := runnerResult{
		Schema:          &schemaRef{Version: 1, Hash: schemaHash},
		ScopeStates:     []scopeStateRecord{{Cursor: &durableCursor}},
		RebuildAttempts: []rebuildAttemptRecord{},
		RebuildReceipts: []rebuildReceiptRecord{validReceipt},
	}
	if err := validateWarmConnectTransportIdentities(runtime, bootstrap, warm, validSnapshot); err != nil {
		t.Fatalf("valid completed rebuild evidence was rejected: %v", err)
	}
	extraScope := 2
	warm[0].RequestFacts.ScopeCount = &extraScope
	if err := validateWarmConnectTransportIdentities(runtime, bootstrap, warm, validSnapshot); err == nil {
		t.Fatal("extra cursorless warm-connect scope was accepted")
	}
	warm[0].RequestFacts.ScopeCount = &oneScope
	warm[1].CursorFingerprints[0] = cursorFingerprint("different")
	if err := validateWarmConnectTransportIdentities(runtime, bootstrap, warm, validSnapshot); err == nil {
		t.Fatal("warm pull unrelated to the bootstrap response was accepted")
	}
	warm[1].CursorFingerprints[0] = bootstrapCursor
	bootstrap[2].PullResponseFacts.ScopeCursorFingerprints[0] = cursorFingerprint("different")
	if err := validateWarmConnectTransportIdentities(runtime, bootstrap, warm, validSnapshot); err == nil {
		t.Fatal("bootstrap response unrelated to the warm pull was accepted")
	}
	bootstrap[2].PullResponseFacts.ScopeCursorFingerprints[0] = bootstrapCursor
	invalidCursorSnapshot := validSnapshot
	invalidCursorSnapshot.ScopeStates = append([]scopeStateRecord(nil), validSnapshot.ScopeStates...)
	differentCursor := "different"
	invalidCursorSnapshot.ScopeStates[0].Cursor = &differentCursor
	if err := validateWarmConnectTransportIdentities(runtime, bootstrap, warm, invalidCursorSnapshot); err == nil {
		t.Fatal("durable cursor unrelated to the warm response was accepted")
	}
	terminalMutations := []struct {
		name   string
		mutate func(*transportRebuildResponseFacts)
	}{
		{"has more", func(facts *transportRebuildResponseFacts) { facts.HasMore = true }},
		{"has page cursor", func(facts *transportRebuildResponseFacts) { facts.HasCursor = true }},
		{"missing final cursor", func(facts *transportRebuildResponseFacts) { facts.HasFinalScopeCursor = false }},
		{"missing checksum", func(facts *transportRebuildResponseFacts) { facts.HasChecksum = false }},
	}
	for _, test := range terminalMutations {
		t.Run("terminal rebuild "+test.name, func(t *testing.T) {
			invalidBootstrap := append([]transportObservation(nil), bootstrap...)
			facts := *bootstrap[1].RebuildResponseFacts
			test.mutate(&facts)
			invalidBootstrap[1].RebuildResponseFacts = &facts
			if err := validateWarmConnectTransportIdentities(runtime, invalidBootstrap, warm, validSnapshot); err == nil {
				t.Fatal("invalid terminal rebuild response was accepted")
			}
		})
	}

	tests := []struct {
		name     string
		snapshot runnerResult
	}{
		{
			name: "active attempt remains",
			snapshot: runnerResult{
				Schema:          validSnapshot.Schema,
				ScopeStates:     validSnapshot.ScopeStates,
				RebuildAttempts: []rebuildAttemptRecord{{RebuildID: rebuildID}},
				RebuildReceipts: validSnapshot.RebuildReceipts,
			},
		},
		{
			name: "receipt belongs to another rebuild",
			snapshot: runnerResult{
				Schema:          validSnapshot.Schema,
				ScopeStates:     validSnapshot.ScopeStates,
				RebuildAttempts: []rebuildAttemptRecord{},
				RebuildReceipts: []rebuildReceiptRecord{rawReceipt(cursorFingerprint("00000000-0000-4000-8000-000000009002"), true, false)},
			},
		},
		{
			name: "receipt scope checksum proof failed",
			snapshot: runnerResult{
				Schema:          validSnapshot.Schema,
				ScopeStates:     validSnapshot.ScopeStates,
				RebuildAttempts: []rebuildAttemptRecord{},
				RebuildReceipts: []rebuildReceiptRecord{rawReceipt(rebuildFingerprint, false, false)},
			},
		},
		{
			name: "baseline receipt contains records",
			snapshot: runnerResult{
				Schema:          validSnapshot.Schema,
				ScopeStates:     validSnapshot.ScopeStates,
				RebuildAttempts: []rebuildAttemptRecord{},
				RebuildReceipts: []rebuildReceiptRecord{rawReceipt(rebuildFingerprint, true, true)},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateWarmConnectTransportIdentities(runtime, bootstrap, warm, test.snapshot); err == nil {
				t.Fatal("invalid completed rebuild evidence was accepted")
			}
		})
	}
}

func rawReceipt(fingerprint string, scopeMatches, hasRecords bool) rebuildReceiptRecord {
	receipt := rebuildReceiptRecord{
		RebuildIDFingerprint:  fingerprint,
		PageCount:             1,
		RequestChainExpected:  []string{"final"},
		RequestChainObserved:  []string{"final"},
		ComputedScopeChecksum: pointerString("scope-checksum"),
		FinalScopeChecksum:    pointerString("scope-checksum"),
	}
	if !scopeMatches {
		receipt.FinalScopeChecksum = pointerString("different")
	}
	if hasRecords {
		receipt.ReturnedRecordCount = 1
		receipt.RecordIdentitiesHex = []string{"record"}
		receipt.ReceivedRowChecksums = []string{"received"}
		receipt.ComputedRowChecksums = []string{"computed"}
	}
	return receipt
}

func TestWarmConnectApplicationIdentityRequiresRuntimePrimaryKey(t *testing.T) {
	tableRuntime := json.RawMessage(`"00000000-0000-4000-8000-000000000002"`)
	primaryRuntime := json.RawMessage(`"00000000-0000-4000-8000-000000000001"`)
	expected := scenarios.RowFact{TableID: "items", CanonicalWireJSON: `"row-a"`}
	snapshot := runnerResult{
		ScopeRows:          []scopeRowRecord{{TableName: "cf_items", RecordID: "runtime-row-a"}},
		RowMetadataRecords: []rowMetadataRecord{{TableName: "cf_items", RecordID: "runtime-row-a"}},
	}
	resolved := map[string]blackbox.NativeIdentityResolution{
		"items-table":       {RuntimeValue: tableRuntime},
		"items-primary-key": {AuthoredValue: json.RawMessage(`"id"`), RuntimeValue: primaryRuntime},
	}
	identity := warmConnectApplicationIdentity{
		tableRuntimeValue:   tableRuntime,
		primaryRuntimeValue: primaryRuntime,
		tableName:           "cf_items",
		primaryKeyName:      "cf_id",
		rows:                []map[string]json.RawMessage{{"cf_id": json.RawMessage(`"runtime-row-a"`)}},
	}
	if err := validateWarmConnectApplicationIdentity(expected, snapshot, identity, resolved); err != nil {
		t.Fatalf("valid application identity failed: %v", err)
	}

	identity.rows = []map[string]json.RawMessage{{"id": json.RawMessage(`"runtime-row-a"`)}}
	if err := validateWarmConnectApplicationIdentity(expected, snapshot, identity, resolved); err == nil {
		t.Fatal("row without the runtime primary-key field passed")
	}
	identity.rows = []map[string]json.RawMessage{{"cf_id": json.RawMessage(`"row-a"`)}}
	if err := validateWarmConnectApplicationIdentity(expected, snapshot, identity, resolved); err == nil {
		t.Fatal("authored record identity passed as the runtime primary-key value")
	}
	identity.rows = []map[string]json.RawMessage{{"cf_id": json.RawMessage(`"runtime-row-a"`)}}
	identity.primaryRuntimeValue = json.RawMessage(`"ffffffff-ffff-4fff-8fff-ffffffffffff"`)
	if err := validateWarmConnectApplicationIdentity(expected, snapshot, identity, resolved); err == nil {
		t.Fatal("unrelated runtime primary-key identity passed")
	}
}
