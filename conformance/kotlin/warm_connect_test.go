package kotlin

import (
	"encoding/json"
	"testing"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestWarmConnectTransportRequiresExactCompletedRebuildProof(t *testing.T) {
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
	request := func() *TransportRequestFacts {
		return &TransportRequestFacts{
			ClientGeneration: &generation,
			SchemaVersion:    1,
			SchemaHash:       schemaHash,
		}
	}
	bootstrapConnect := request()
	bootstrapConnect.ScopeCount = &zeroScopes
	bootstrap := []TransportObservation{{RequestFacts: bootstrapConnect}, {RequestFacts: request()}, {RequestFacts: request()}}
	bootstrap[1].RequestFacts.RebuildIDFingerprint = &rebuildFingerprint
	bootstrap[1].RebuildResponseFacts = &TransportRebuildResponseFacts{
		HasFinalScopeCursor:         true,
		HasChecksum:                 true,
		FinalScopeCursorFingerprint: &rebuildCursor,
	}
	bootstrap[2].RequestFacts.ScopeSetVersion = &scopeSetVersion
	bootstrap[2].RequestFacts.ScopeCount = &oneScope
	bootstrap[2].CursorFingerprints = []string{rebuildCursor}
	bootstrap[2].CursorFingerprintsComplete = &complete
	bootstrap[2].PullResponseFacts = &TransportPullResponseFacts{
		ScopeCursorFingerprints:         []string{bootstrapCursor},
		ScopeCursorFingerprintsComplete: true,
	}
	warm := []TransportObservation{{RequestFacts: request()}, {RequestFacts: request()}}
	warm[0].RequestFacts.ScopeSetVersion = &scopeSetVersion
	warm[0].RequestFacts.ScopeCount = &oneScope
	warm[1].RequestFacts.ScopeSetVersion = &scopeSetVersion
	warm[1].RequestFacts.ScopeCount = &oneScope
	warm[1].CursorFingerprints = []string{bootstrapCursor}
	warm[1].CursorFingerprintsComplete = &complete
	warm[1].PullResponseFacts = &TransportPullResponseFacts{
		ScopeCursorFingerprints:         []string{warmCursor},
		ScopeCursorFingerprintsComplete: true,
	}
	runtime := map[string]json.RawMessage{
		"client-a-generation":   json.RawMessage(`1`),
		"scope-set-version-one": json.RawMessage(`1`),
		"baseline-rebuild":      json.RawMessage(`"` + rebuildID + `"`),
		"current-schema":        json.RawMessage(`{"version":1,"hash":"` + schemaHash + `"}`),
	}
	validProof := rebuildReceiptProofRecord{
		RebuildIDFingerprint:    rebuildFingerprint,
		PageCount:               1,
		ReturnedRecordCount:     0,
		RequestChainValid:       true,
		RecordsInCanonicalOrder: true,
		RowChecksumsValid:       true,
		ScopeChecksumValid:      true,
	}
	validSnapshot := warmConnectSnapshot{
		schema:               schemaRef{Version: 1, Hash: schemaHash},
		scopeStates:          []scopeStateRecord{{Cursor: &durableCursor}},
		rebuildAttempts:      []rebuildAttemptRecord{},
		rebuildReceiptProofs: []rebuildReceiptProofRecord{validProof},
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
	invalidCursorSnapshot.scopeStates = append([]scopeStateRecord(nil), validSnapshot.scopeStates...)
	differentCursor := "different"
	invalidCursorSnapshot.scopeStates[0].Cursor = &differentCursor
	if err := validateWarmConnectTransportIdentities(runtime, bootstrap, warm, invalidCursorSnapshot); err == nil {
		t.Fatal("durable cursor unrelated to the warm response was accepted")
	}
	terminalMutations := []struct {
		name   string
		mutate func(*TransportRebuildResponseFacts)
	}{
		{"has more", func(facts *TransportRebuildResponseFacts) { facts.HasMore = true }},
		{"has page cursor", func(facts *TransportRebuildResponseFacts) { facts.HasCursor = true }},
		{"missing final cursor", func(facts *TransportRebuildResponseFacts) { facts.HasFinalScopeCursor = false }},
		{"missing checksum", func(facts *TransportRebuildResponseFacts) { facts.HasChecksum = false }},
	}
	for _, test := range terminalMutations {
		t.Run("terminal rebuild "+test.name, func(t *testing.T) {
			invalidBootstrap := append([]TransportObservation(nil), bootstrap...)
			facts := *bootstrap[1].RebuildResponseFacts
			test.mutate(&facts)
			invalidBootstrap[1].RebuildResponseFacts = &facts
			if err := validateWarmConnectTransportIdentities(runtime, invalidBootstrap, warm, validSnapshot); err == nil {
				t.Fatal("invalid terminal rebuild response was accepted")
			}
		})
	}

	nonempty := validSnapshot
	nonempty.rebuildReceiptProofs = append([]rebuildReceiptProofRecord(nil), validSnapshot.rebuildReceiptProofs...)
	nonempty.rebuildReceiptProofs[0].ReturnedRecordCount = 1
	if err := validateWarmConnectTransportIdentities(runtime, bootstrap, warm, nonempty); err == nil {
		t.Fatal("nonempty baseline rebuild receipt was accepted")
	}

	active := validSnapshot
	active.rebuildAttempts = []rebuildAttemptRecord{{RebuildID: rebuildID}}
	if err := validateWarmConnectTransportIdentities(runtime, bootstrap, warm, active); err == nil {
		t.Fatal("active completed rebuild attempt was accepted")
	}
}

func TestCompletedWarmConnectRebuildIDRequiresOneExactEvent(t *testing.T) {
	events := json.RawMessage(`[
		{"type":"state_changed","status":"ready"},
		{"type":"rebuild_completed","scope_id":"scope-runtime","rebuild_id":"rebuild-runtime"}
	]`)
	value, err := completedWarmConnectRebuildID(events, "scope-runtime")
	if err != nil || value != "rebuild-runtime" {
		t.Fatalf("completed rebuild identity = %q, %v", value, err)
	}

	duplicate := json.RawMessage(`[
		{"type":"rebuild_completed","scope_id":"scope-runtime","rebuild_id":"rebuild-one"},
		{"type":"rebuild_completed","scope_id":"scope-runtime","rebuild_id":"rebuild-two"}
	]`)
	if _, err := completedWarmConnectRebuildID(duplicate, "scope-runtime"); err == nil {
		t.Fatal("ambiguous completed rebuild identity was accepted")
	}

	extended := json.RawMessage(`[
		{"type":"rebuild_completed","scope_id":"scope-runtime","rebuild_id":"rebuild-runtime","cursor":"private"}
	]`)
	if _, err := completedWarmConnectRebuildID(extended, "scope-runtime"); err == nil {
		t.Fatal("extended completed rebuild event was accepted")
	}
}

func TestRebuildAttemptFactsDeduplicateActiveAndCompletedIdentity(t *testing.T) {
	rebuildID := "00000000-0000-4000-8000-000000009001"
	fingerprint := cursorFingerprint(rebuildID)
	result := Result{
		RebuildAttempts:      json.RawMessage(`[{"scope_id":"scope-runtime","rebuild_id":"` + rebuildID + `","client_generation":1,"schema_version":1,"schema_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","generation":1,"cursor":null,"page_limit":100}]`),
		RebuildReceiptProofs: json.RawMessage(`[{"rebuild_id_fingerprint":"` + fingerprint + `","page_count":1,"returned_record_count":0,"request_chain_valid":true,"records_in_canonical_order":true,"row_checksums_valid":true,"scope_checksum_valid":true,"final_checksum_matches_local":true}]`),
	}
	count, err := androidRebuildAttemptFactCount(result)
	if err != nil {
		t.Fatalf("count rebuild attempt facts: %v", err)
	}
	if count != 1 {
		t.Fatalf("rebuild attempt facts = %d, want 1", count)
	}
}

func TestWarmConnectApplicationIdentityRequiresRuntimePrimaryKey(t *testing.T) {
	tableRuntime := json.RawMessage(`"00000000-0000-4000-8000-000000000002"`)
	primaryRuntime := json.RawMessage(`"00000000-0000-4000-8000-000000000001"`)
	expected := scenarios.RowFact{TableID: "items", CanonicalWireJSON: `"row-a"`}
	snapshot := warmConnectSnapshot{
		scopeRows:   []scopeRowRecord{{TableName: "cf_items", RecordID: "runtime-row-a"}},
		rowMetadata: []rowMetadataRecord{{TableName: "cf_items", RecordID: "runtime-row-a"}},
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
