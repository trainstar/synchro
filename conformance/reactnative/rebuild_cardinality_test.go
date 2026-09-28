package reactnative

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestValidateRebuildCardinalityScenarioAcceptsAuthoredContract(t *testing.T) {
	if err := ValidateRebuildCardinalityScenario(loadRebuildCardinalityAuthoredScenario(t)); err != nil {
		t.Fatalf("validate authored rebuild-cardinality scenario: %v", err)
	}
}

func TestValidateRebuildCardinalityScenarioRejectsContractChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*scenarios.Scenario)
	}{
		{"workload client", func(scenario *scenarios.Scenario) {
			scenario.Steps[1].NativeBinding.ClientID = scenario.Steps[0].NativeBinding.ClientID
		}},
		{"page size", func(scenario *scenarios.Scenario) {
			scenario.Steps[0].Operation.Payload = json.RawMessage(`{"profile":"scope_cardinality","scope_id":"scope-a","record_count":1,"page_size":0}`)
		}},
		{"Android proof target", func(scenario *scenarios.Scenario) {
			for index := range scenario.ProofObligations {
				if string(scenario.ProofObligations[index].ObligationID) == "OBL-PERF-REBUILD-CARDINALITY-RN-ANDROID-CURRENT-001" {
					scenario.ProofObligations[index].MakeTarget = "test-rn-rebuild-cardinality-android"
				}
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := cloneRebuildCardinalityScenario(loadRebuildCardinalityAuthoredScenario(t))
			test.mutate(&scenario)
			if err := ValidateRebuildCardinalityScenario(scenario); err == nil {
				t.Fatal("changed rebuild-cardinality contract was accepted")
			}
		})
	}
}

func TestNewRebuildCardinalityCoordinatorKeepsAndroidSidecarOnHostLoopback(t *testing.T) {
	coordinator, err := NewRebuildCardinalityCoordinator(RebuildCardinalityCoordinatorConfig{
		Scenario: loadRebuildCardinalityAuthoredScenario(t), Platform: "android", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil || coordinator == nil {
		t.Fatalf("Android rebuild-cardinality coordinator was rejected: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	if !strings.HasPrefix(coordinator.URL(), "http://127.0.0.1:") {
		t.Fatalf("Android rebuild-cardinality coordinator URL = %q", coordinator.URL())
	}
	if !strings.HasPrefix(coordinator.adapter, "http://10.0.2.2:") {
		t.Fatalf("Android rebuild-cardinality adapter URL = %q", coordinator.adapter)
	}
	want := 1
	for _, step := range coordinator.config.Scenario.Steps {
		workload, err := decodeRebuildCardinalityWorkload(step)
		if err != nil {
			t.Fatalf("decode rebuild-cardinality workload: %v", err)
		}
		want += 3 + rebuildCardinalityApplicationRowBatchCount(workload.RecordCount)
	}
	if got := coordinator.ExchangeCount(); got != want {
		t.Fatalf("rebuild-cardinality exchange count = %d, want %d", got, want)
	}
}

func TestRebuildCardinalityApplicationRowCommandsUseBoundedBatches(t *testing.T) {
	scenario := loadRebuildCardinalityAuthoredScenario(t)
	step := scenario.Steps[3]
	selectors := make([]map[string]any, 101)
	rowKeys := make(map[string]struct{}, len(selectors))
	for index := range selectors {
		recordID := fmt.Sprintf("runtime-row-%03d", index+1)
		selectors[index] = map[string]any{
			"table_name": "runtime_items", "primary_key_field": "runtime_id", "primary_key": recordID,
		}
		rowKeys[recordID] = struct{}{}
	}
	coordinator := &RebuildCardinalityCoordinator{
		config:               RebuildCardinalityCoordinatorConfig{Scenario: scenario},
		adapter:              "http://127.0.0.1:8080",
		steps:                []scenarios.Step{step},
		workloads:            []rebuildCardinalityWorkload{{Profile: "scope_cardinality", ScopeID: "scope-a", RecordCount: 101, PageSize: 100}},
		authTokens:           map[string]string{step.NativeBinding.ClientID: "unit-token"},
		applicationSelectors: selectors,
		applicationRowKeys:   rowKeys,
		applicationRowsSeen:  make(map[string]struct{}, len(rowKeys)),
		primaryKey:           "runtime_id",
		stage:                rebuildCardinalityStageApplicationRows,
	}
	response, err := coordinator.advanceLocked(context.Background(), 1)
	if err != nil {
		t.Fatalf("advance rebuild-cardinality application rows: %v", err)
	}
	parameters := response.Command.Action.Action.Parameters
	got, ok := parameters["row_selectors"].([]map[string]any)
	if !ok {
		t.Fatalf("rebuild-cardinality row selectors type = %T", parameters["row_selectors"])
	}
	if len(got) != rebuildCardinalityApplicationRowBatchSize {
		t.Fatalf("rebuild-cardinality row selector batch = %d, want %d", len(got), rebuildCardinalityApplicationRowBatchSize)
	}
	if coordinator.applicationSelectorPos != 0 {
		t.Fatalf("rebuild-cardinality selector position advanced before result: %d", coordinator.applicationSelectorPos)
	}
}

func TestRebuildCardinalityCaptureRequestsGroupedReceiptProofs(t *testing.T) {
	scenario := loadRebuildCardinalityAuthoredScenario(t)
	step := scenario.Steps[3]
	coordinator := &RebuildCardinalityCoordinator{
		config:     RebuildCardinalityCoordinatorConfig{Scenario: scenario},
		adapter:    "http://127.0.0.1:8080",
		steps:      []scenarios.Step{step},
		workloads:  []rebuildCardinalityWorkload{{Profile: "scope_cardinality", ScopeID: "scope-a", RecordCount: 101, PageSize: 100}},
		authTokens: map[string]string{step.NativeBinding.ClientID: "unit-token"},
		tableName:  "runtime_items",
		stage:      rebuildCardinalityStageCapture,
	}
	response, err := coordinator.advanceLocked(context.Background(), 1)
	if err != nil {
		t.Fatalf("advance rebuild-cardinality capture: %v", err)
	}
	parameters := response.Command.Action.Action.Parameters
	wantSources := []string{"scope-state", "pending-mutations", "rejected-mutations", "sync-status", "sync-events", "provenance", "request-trace", "durable-proof"}
	if !reflect.DeepEqual(parameters["sources"], wantSources) {
		t.Fatalf("rebuild-cardinality capture sources = %#v, want %#v", parameters["sources"], wantSources)
	}
	wantIdentity := map[string]any{"table_name": "runtime_items", "record_id": "rebuild-cardinality-absent-row"}
	if !reflect.DeepEqual(parameters["durable_proof_identity"], wantIdentity) {
		t.Fatalf("rebuild-cardinality durable proof identity = %#v, want %#v", parameters["durable_proof_identity"], wantIdentity)
	}
}

func TestRebuildCardinalityReceiptAttemptCountUsesRebuildIdentities(t *testing.T) {
	receipts := []rebuildReceiptProof{{
		RebuildIDFingerprint: strings.Repeat("a", 64), PageCount: 2, ReturnedRecordCount: 101,
		RequestChainValid: true, RecordsInCanonicalOrder: true, RowChecksumsValid: true,
		ScopeChecksumValid: true, FinalChecksumMatches: true,
	}}
	attempts, err := rebuildAttemptFactCount(nil, receipts)
	if err != nil || attempts != 1 {
		t.Fatalf("two-page rebuild attempt count = %d, want 1: %v", attempts, err)
	}

	receipts = []rebuildReceiptProof{
		{RebuildIDFingerprint: strings.Repeat("a", 64), PageCount: 1, ReturnedRecordCount: 100, RequestChainValid: true, RecordsInCanonicalOrder: true, RowChecksumsValid: true, ScopeChecksumValid: true, FinalChecksumMatches: true},
		{RebuildIDFingerprint: strings.Repeat("b", 64), PageCount: 1, ReturnedRecordCount: 1, RequestChainValid: true, RecordsInCanonicalOrder: true, RowChecksumsValid: true, ScopeChecksumValid: true, FinalChecksumMatches: true},
	}
	attempts, err = rebuildAttemptFactCount(nil, receipts)
	if err != nil || attempts != 2 {
		t.Fatalf("distinct rebuild attempt count = %d, want 2: %v", attempts, err)
	}
}

func TestValidateRebuildCardinalityCaptureRequiresReceiptPredicatesAndCompletion(t *testing.T) {
	scenario := loadRebuildCardinalityAuthoredScenario(t)
	step := scenario.Steps[3]
	workload, err := decodeRebuildCardinalityWorkload(step)
	if err != nil {
		t.Fatal(err)
	}
	coordinator := func() *RebuildCardinalityCoordinator {
		return &RebuildCardinalityCoordinator{
			expected: rebuildCardinalityExpectedState(scenario), steps: []scenarios.Step{step},
			workloads: []rebuildCardinalityWorkload{workload}, tableName: "runtime_items",
			runtimeIDs: map[string]json.RawMessage{
				"current-schema": json.RawMessage(`{"version":1,"hash":"` + strings.Repeat("d", 64) + `"}`),
				"scope-a":        json.RawMessage(`"scope-a"`),
			},
		}
	}
	if err := coordinator().validateCapture(rebuildCardinalityCaptureFixture(t, workload)); err != nil {
		t.Fatalf("valid rebuild-cardinality capture rejected: %v", err)
	}
	withProof := func(change func(receipts []any) []any) func(*finalCapture) {
		return func(capture *finalCapture) {
			var proof map[string]any
			if err := json.Unmarshal(capture.DurableProof, &proof); err != nil {
				t.Fatalf("decode rebuild-cardinality proof fixture: %v", err)
			}
			proof["rebuild_receipt_proofs"] = change(proof["rebuild_receipt_proofs"].([]any))
			capture.DurableProof = marshalRebuildCardinalityFixture(t, proof)
		}
	}
	withReceipt := func(member string, value any) func(*finalCapture) {
		return withProof(func(receipts []any) []any {
			receipts[0].(map[string]any)[member] = value
			return receipts
		})
	}
	withEvents := func(events ...any) func(*finalCapture) {
		return func(capture *finalCapture) {
			capture.Events = marshalRebuildCardinalityFixture(t, append([]any{}, events...))
		}
	}
	for _, test := range []struct {
		name   string
		change func(*finalCapture)
	}{
		{name: "request chain invalid", change: withReceipt("request_chain_valid", false)},
		{name: "records out of canonical order", change: withReceipt("records_in_canonical_order", false)},
		{name: "row checksums invalid", change: withReceipt("row_checksums_valid", false)},
		{name: "scope checksum invalid", change: withReceipt("scope_checksum_valid", false)},
		{name: "final checksum differs", change: withReceipt("final_checksum_matches_local", false)},
		{name: "returned records differ", change: withReceipt("returned_record_count", workload.RecordCount+1)},
		{name: "one rebuild split across two receipt proofs", change: withProof(func(receipts []any) []any {
			first, second := map[string]any{}, map[string]any{}
			for member, value := range receipts[0].(map[string]any) {
				first[member], second[member] = value, value
			}
			first["page_count"], first["returned_record_count"] = 1, workload.PageSize
			second["page_count"], second["returned_record_count"] = 1, workload.RecordCount-workload.PageSize
			return []any{first, second}
		})},
		{name: "completion event absent", change: withEvents()},
		{name: "completion event for another scope", change: withEvents(map[string]any{"type": "rebuild_completed", "scope_id": "scope-b", "rebuild_id": "rebuild-a"})},
		{name: "completion event without receipt", change: withEvents(map[string]any{"type": "rebuild_completed", "scope_id": "scope-a", "rebuild_id": "rebuild-b"})},
		{name: "provenance row differs from scope row", change: func(capture *finalCapture) {
			var provenance []map[string]any
			if err := json.Unmarshal(capture.Provenance, &provenance); err != nil {
				t.Fatalf("decode rebuild-cardinality provenance fixture: %v", err)
			}
			provenance[0]["recordID"] = "runtime-row-other"
			capture.Provenance = marshalRebuildCardinalityFixture(t, provenance)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			capture := rebuildCardinalityCaptureFixture(t, workload)
			test.change(&capture)
			if err := coordinator().validateCapture(capture); err == nil {
				t.Fatal("changed rebuild-cardinality capture passed validation")
			}
		})
	}
}

func TestRebuildClientGenerationUsesEstablishedRequestTrace(t *testing.T) {
	traces := []traceSnapshot{{Observations: []transportObservation{
		{OperationClass: "connect", RequestFacts: json.RawMessage(`{"schema_version":1}`)},
		{OperationClass: "rebuild", RequestFacts: json.RawMessage(`{"client_generation":7}`)},
		{OperationClass: "pull", RequestFacts: json.RawMessage(`{"client_generation":7}`)},
	}}}
	generation, err := rebuildClientGeneration(traces)
	if err != nil || generation != 7 {
		t.Fatalf("rebuild client generation = %d, error = %v, want 7", generation, err)
	}
	traces[0].Observations[2].RequestFacts = json.RawMessage(`{"client_generation":8}`)
	if _, err := rebuildClientGeneration(traces); err == nil {
		t.Fatal("changed rebuild client generation accepted")
	}
	traces = []traceSnapshot{{Observations: traces[0].Observations[:1]}}
	if _, err := rebuildClientGeneration(traces); err == nil || !strings.Contains(err.Error(), "request-trace source") {
		t.Fatalf("absent rebuild client generation error = %v, want request-trace source", err)
	}
}

func rebuildCardinalityCaptureFixture(t *testing.T, workload rebuildCardinalityWorkload) finalCapture {
	t.Helper()
	pages := (workload.RecordCount + workload.PageSize - 1) / workload.PageSize
	scopeFingerprint := strings.Repeat("a", 64)
	finalCursorFingerprint := strings.Repeat("b", 64)
	continuationFingerprint := strings.Repeat("c", 64)
	observations := []any{map[string]any{
		"sequence": 1, "operationClass": "connect", "statusCode": 200,
		"durationNanoseconds": 1, "requestFacts": map[string]any{"schema_version": 1},
	}}
	for page := uint64(0); page < pages; page++ {
		remaining := workload.RecordCount - page*workload.PageSize
		records := workload.PageSize
		if remaining < records {
			records = remaining
		}
		terminal := page == pages-1
		requestFacts := map[string]any{
			"client_generation": 1, "limit": workload.PageSize,
			"scope_fingerprint": scopeFingerprint, "rebuild_id_fingerprint": hashFingerprint("rebuild-a"),
		}
		if page > 0 {
			requestFacts["cursor_fingerprint"] = continuationFingerprint
		}
		responseFacts := map[string]any{
			"record_count": records, "has_more": !terminal, "has_cursor": !terminal,
			"has_final_scope_cursor": terminal, "has_checksum": terminal,
			"scope_fingerprint": scopeFingerprint,
		}
		if terminal {
			responseFacts["final_scope_cursor_fingerprint"] = finalCursorFingerprint
		}
		observations = append(observations, map[string]any{
			"sequence": page + 2, "operationClass": "rebuild", "statusCode": 200,
			"durationNanoseconds": 1, "requestFacts": requestFacts, "rebuildResponseFacts": responseFacts,
		})
	}
	observations = append(observations, map[string]any{
		"sequence": pages + 2, "operationClass": "pull", "statusCode": 200,
		"durationNanoseconds": 1, "cursorFingerprints": []string{finalCursorFingerprint},
		"cursorFingerprintsComplete": true, "requestFacts": map[string]any{"client_generation": 1, "scope_count": 1},
		"pullResponseFacts": map[string]any{
			"change_count": 0, "has_more": false, "rebuild_scope_count": 0,
			"checksum_count": 1, "scope_cursor_fingerprints": []string{finalCursorFingerprint},
			"scope_cursor_fingerprints_complete": true,
		},
	})
	scopeRows := make([]any, workload.RecordCount)
	for index := range scopeRows {
		scopeRows[index] = map[string]any{"scopeID": "scope-a", "tableName": "runtime_items", "recordID": fmt.Sprintf("runtime-row-%03d", index+1)}
	}
	state := map[string]any{
		"schema":              map[string]any{"version": 1, "hash": strings.Repeat("d", 64)},
		"scopeStates":         []any{map[string]any{"scopeID": "scope-a"}},
		"scopeRows":           scopeRows,
		"rebuildAttempts":     []any{},
		"applicationRowCount": workload.RecordCount, "provenanceCount": workload.RecordCount,
		"scopeStateCount": 1, "scopeRowCount": workload.RecordCount, "rowMetadataCount": workload.RecordCount,
		"rebuildAttemptCount": 0, "rebuildReceiptCount": pages,
		"provenanceMaintenanceWorkCursor": "cursor",
	}
	proof := map[string]any{
		"row_metadata": nil,
		"rebuild_receipt_proofs": []any{map[string]any{
			"rebuild_id_fingerprint": hashFingerprint("rebuild-a"), "page_count": pages,
			"returned_record_count": workload.RecordCount, "request_chain_valid": true,
			"records_in_canonical_order": true, "row_checksums_valid": true,
			"scope_checksum_valid": true, "final_checksum_matches_local": true,
		}},
	}
	return finalCapture{
		ClientState: marshalRebuildCardinalityFixture(t, state),
		Pending:     marshalRebuildCardinalityFixture(t, []any{}),
		Rejected:    marshalRebuildCardinalityFixture(t, []any{}),
		Status:      marshalRebuildCardinalityFixture(t, map[string]any{"state": "ready", "retry_at": nil, "operation": nil, "failure": nil}),
		Events: marshalRebuildCardinalityFixture(t, []any{map[string]any{
			"type": "rebuild_completed", "scope_id": "scope-a", "rebuild_id": "rebuild-a",
		}}),
		Provenance: marshalRebuildCardinalityFixture(t, scopeRows),
		Trace: marshalRebuildCardinalityFixture(t, map[string]any{
			"observations": observations, "overflowed": false, "sequenceCheckpoint": pages + 2,
		}),
		DurableProof: marshalRebuildCardinalityFixture(t, proof),
	}
}

func marshalRebuildCardinalityFixture(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal rebuild-cardinality fixture: %v", err)
	}
	return raw
}

func loadRebuildCardinalityAuthoredScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	scenario, err := LoadRebuildCardinalityScenario(context.Background(), repoRoot)
	if err != nil {
		t.Fatalf("load authored rebuild-cardinality scenario: %v", err)
	}
	return scenario
}

func cloneRebuildCardinalityScenario(scenario scenarios.Scenario) scenarios.Scenario {
	data, err := json.Marshal(scenario)
	if err != nil {
		panic(err)
	}
	var clone scenarios.Scenario
	if err := json.Unmarshal(data, &clone); err != nil {
		panic(err)
	}
	return clone
}
