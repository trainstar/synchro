package reactnative

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestValidateRebuildApplyScenarioAcceptsAuthoredContract(t *testing.T) {
	if err := ValidateRebuildApplyScenario(loadRebuildApplyAuthoredScenario(t)); err != nil {
		t.Fatalf("validate authored rebuild-apply scenario: %v", err)
	}
}

func TestValidateRebuildApplyScenarioRejectsContractChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*scenarios.Scenario)
	}{
		{"workload client", func(s *scenarios.Scenario) { s.Steps[1].NativeBinding.ClientID = s.Steps[0].NativeBinding.ClientID }},
		{"page size", func(s *scenarios.Scenario) {
			s.Steps[0].Operation.Payload = json.RawMessage(`{"profile":"scope_cardinality","scope_id":"scope-a","record_count":1,"page_size":0}`)
		}},
		{"iOS proof target", func(s *scenarios.Scenario) {
			for i := range s.ProofObligations {
				if string(s.ProofObligations[i].ObligationID) == "OBL-PERF-REBUILD-APPLY-RN-IOS-CURRENT-001" {
					s.ProofObligations[i].MakeTarget = "test-rn-rebuild-apply-ios"
				}
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := cloneRebuildApplyScenario(loadRebuildApplyAuthoredScenario(t))
			test.mutate(&scenario)
			if err := ValidateRebuildApplyScenario(scenario); err == nil {
				t.Fatal("changed rebuild-apply contract was accepted")
			}
		})
	}
}

func TestNewRebuildApplyCoordinatorKeepsAndroidSidecarOnHostLoopback(t *testing.T) {
	coordinator, err := NewRebuildApplyCoordinator(RebuildApplyCoordinatorConfig{
		Scenario: loadRebuildApplyAuthoredScenario(t), Platform: "android", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token", AppVersion: "0.3.0",
	})
	if err != nil || coordinator == nil {
		t.Fatalf("Android rebuild-apply coordinator was rejected: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	if !strings.HasPrefix(coordinator.URL(), "http://127.0.0.1:") {
		t.Fatalf("Android rebuild-apply coordinator URL = %q", coordinator.URL())
	}
	if !strings.HasPrefix(coordinator.adapter, "http://10.0.2.2:") {
		t.Fatalf("Android rebuild-apply adapter URL = %q", coordinator.adapter)
	}
}

func TestRebuildApplyAuthoredFlowServesExactlyExchangeCount(t *testing.T) {
	scenario := loadRebuildApplyAuthoredScenario(t)
	workloads := make([]rebuildApplyWorkload, len(scenario.Steps))
	for index, step := range scenario.Steps {
		if err := json.Unmarshal(step.Operation.Payload, &workloads[index]); err != nil {
			t.Fatalf("decode rebuild-apply workload %s: %v", step.ID, err)
		}
	}
	coordinator := &RebuildApplyCoordinator{
		config:    RebuildApplyCoordinatorConfig{Scenario: scenario},
		adapter:   "http://127.0.0.1:8080",
		steps:     scenario.Steps,
		workloads: workloads,
		tableName: "runtime_items",
	}

	type exchange struct {
		actor   string
		command string
		client  string
		state   string
	}
	want := make([]exchange, 0, len(scenario.Steps)*3+1)
	for _, step := range scenario.Steps {
		client := step.NativeBinding.ClientID
		want = append(want,
			exchange{actor: "client", command: "open", client: client, state: "command"},
			exchange{actor: "client", command: "synchronize-step", client: client, state: "command"},
			exchange{actor: "observer", command: "capture", client: client, state: "command"},
		)
	}
	want = append(want, exchange{state: "complete"})

	if got := len(want); got != coordinator.ExchangeCount() {
		t.Fatalf("authored rebuild-apply exchanges = %d, want ExchangeCount=%d", got, coordinator.ExchangeCount())
	}
	if got, wantCount := coordinator.ExchangeCount(), 28; got != wantCount {
		t.Fatalf("authored rebuild-apply ExchangeCount = %d, want %d", got, wantCount)
	}
	for index, expected := range want[:len(want)-1] {
		step := scenario.Steps[index/3]
		clientKey := "rebuild-apply-" + expected.client
		response := coordinator.command(clientKey, expected.client, expected.actor, expected.command, map[string]any{}, nil)
		if expected.command == "capture" {
			coordinator.current = index / 3
			coordinator.stage = rebuildApplyStageCapture
			advanced, err := coordinator.advanceLocked(context.Background(), uint64(index+1))
			if err != nil {
				t.Fatalf("advance authored rebuild-apply capture %d: %v", index+1, err)
			}
			response = advanced.Command
		}
		if response.Action.Action.Actor != expected.actor || response.Action.Action.Command != expected.command || response.Runtime.ClientID != expected.client {
			t.Fatalf("authored rebuild-apply exchange %d = %s/%s client=%s, want %s/%s client=%s", index+1, response.Action.Action.Actor, response.Action.Action.Command, response.Runtime.ClientID, expected.actor, expected.command, expected.client)
		}
		if response.Runtime.ClientKey != clientKey || step.NativeBinding.ClientID != expected.client {
			t.Fatalf("authored rebuild-apply exchange %d client binding = %q, want %q", index+1, response.Runtime.ClientKey, clientKey)
		}
		if expected.command == "capture" && response.Action.Action.Parameters["detail_policy"] != "complete-or-omit" {
			t.Fatalf("authored rebuild-apply exchange %d detail policy = %#v, want complete-or-omit", index+1, response.Action.Action.Parameters["detail_policy"])
		}
	}
	if terminal := want[len(want)-1]; terminal.state != "complete" || terminal.actor != "" || terminal.command != "" || terminal.client != "" {
		t.Fatalf("authored rebuild-apply terminal exchange = %#v", terminal)
	}
}

func TestValidateRebuildApplyCaptureDistinguishesActiveAndTerminalAttempts(t *testing.T) {
	scenario := loadRebuildApplyAuthoredScenario(t)
	for _, index := range []int{3, 6} {
		step := scenario.Steps[index]
		t.Run(string(step.ID), func(t *testing.T) {
			var workload rebuildApplyWorkload
			if err := json.Unmarshal(step.Operation.Payload, &workload); err != nil {
				t.Fatalf("decode rebuild-apply workload: %v", err)
			}
			coordinator := &RebuildApplyCoordinator{
				expected:  rebuildApplyExpectedState(scenario),
				steps:     []scenarios.Step{step},
				workloads: []rebuildApplyWorkload{workload},
			}
			capture := rebuildApplyCaptureFixture(t, workload)
			if err := coordinator.validateCapture(capture); err != nil {
				t.Fatalf("validate active and terminal rebuild attempt counts: %v", err)
			}
		})
	}
}

func TestRebuildApplyCaptureRequestsTerminalReceiptProof(t *testing.T) {
	scenario := loadRebuildApplyAuthoredScenario(t)
	step := scenario.Steps[3]
	coordinator := &RebuildApplyCoordinator{
		config:    RebuildApplyCoordinatorConfig{Scenario: scenario},
		adapter:   "http://127.0.0.1:8080",
		steps:     []scenarios.Step{step},
		workloads: []rebuildApplyWorkload{{Profile: "scope_cardinality", ScopeID: "scope-a", RecordCount: 101, PageSize: 100}},
		tableName: "runtime_items",
		stage:     rebuildApplyStageCapture,
	}
	response, err := coordinator.advanceLocked(context.Background(), 1)
	if err != nil {
		t.Fatalf("advance rebuild-apply capture: %v", err)
	}
	parameters := response.Command.Action.Action.Parameters
	wantSources := []string{"scope-state", "pending-mutations", "rejected-mutations", "sync-status", "sync-events", "provenance", "request-trace", "durable-proof"}
	if !reflect.DeepEqual(parameters["sources"], wantSources) {
		t.Fatalf("rebuild-apply capture sources = %#v, want %#v", parameters["sources"], wantSources)
	}
	wantIdentity := map[string]any{"table_name": "runtime_items", "record_id": "rebuild-apply-absent-row"}
	if !reflect.DeepEqual(parameters["durable_proof_identity"], wantIdentity) {
		t.Fatalf("rebuild-apply durable proof identity = %#v, want %#v", parameters["durable_proof_identity"], wantIdentity)
	}
	if parameters["detail_policy"] != "complete-or-omit" {
		t.Fatalf("rebuild-apply detail policy = %#v, want complete-or-omit", parameters["detail_policy"])
	}
}

func TestValidateRebuildApplyCaptureRejectsPartialOverBoundDetails(t *testing.T) {
	scenario := loadRebuildApplyAuthoredScenario(t)
	step := scenario.Steps[6]
	var workload rebuildApplyWorkload
	if err := json.Unmarshal(step.Operation.Payload, &workload); err != nil {
		t.Fatalf("decode over-bound rebuild-apply workload: %v", err)
	}
	capture := rebuildApplyCaptureFixture(t, workload)
	rows := make([]any, 512)
	for index := range rows {
		rows[index] = map[string]any{"scopeID": "scope-a"}
	}
	var state map[string]any
	if err := json.Unmarshal(capture.ClientState, &state); err != nil {
		t.Fatalf("decode over-bound rebuild-apply state: %v", err)
	}
	state["scopeRows"] = rows
	capture.ClientState = marshalRebuildApplyFixture(t, state)
	capture.Provenance = marshalRebuildApplyFixture(t, rows)
	coordinator := &RebuildApplyCoordinator{
		expected: rebuildApplyExpectedState(scenario), steps: []scenarios.Step{step}, workloads: []rebuildApplyWorkload{workload},
	}
	if err := coordinator.validateCapture(capture); err == nil || !strings.Contains(err.Error(), "provenance details count: observed=512 expected=0") {
		t.Fatalf("partial over-bound rebuild-apply detail error = %v, want complete-or-omit rejection", err)
	}
}

func TestRebuildApplyCountFailureNamesExchangeObservedAndExpected(t *testing.T) {
	scenario := loadRebuildApplyAuthoredScenario(t)
	step := scenario.Steps[3]
	var workload rebuildApplyWorkload
	if err := json.Unmarshal(step.Operation.Payload, &workload); err != nil {
		t.Fatalf("decode rebuild-apply workload: %v", err)
	}
	capture := rebuildApplyCaptureFixture(t, workload)
	var state map[string]any
	if err := json.Unmarshal(capture.ClientState, &state); err != nil {
		t.Fatalf("decode rebuild-apply state fixture: %v", err)
	}
	state["rebuildReceiptCount"] = 3
	capture.ClientState = marshalRebuildApplyFixture(t, state)
	captureResult := map[string]any{
		"kind": "capture",
		"capture": map[string]json.RawMessage{
			"client_state": capture.ClientState, "pending_mutations": capture.Pending,
			"rejected_mutations": capture.Rejected, "sync_status": capture.Status,
			"sync_events": capture.Events, "provenance": capture.Provenance,
			"request_trace": capture.Trace, "durable_proof": capture.DurableProof,
		},
		"process": map[string]any{},
	}
	envelope := map[string]any{
		"schema_version": 1, "outcome": "passed", "result": captureResult,
		"error_code": nil, "error_detail": nil,
	}
	body := marshalRebuildApplyFixture(t, map[string]any{"schema_version": 1, "sequence": 13, "result": envelope})
	coordinator := &RebuildApplyCoordinator{
		config: RebuildApplyCoordinatorConfig{Scenario: scenario}, token: "unit-token",
		prepared: true, stage: rebuildApplyStageComplete, nextSeq: 13,
		expected: rebuildApplyExpectedState(scenario), steps: []scenarios.Step{step}, workloads: []rebuildApplyWorkload{workload},
	}
	request := httptest.NewRequest(http.MethodPost, "/exchange", strings.NewReader(string(body)))
	request.Header.Set("Authorization", "Bearer unit-token")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	coordinator.ServeHTTP(response, request)
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("rebuild-apply count mismatch status = %d, want %d", response.Code, http.StatusUnprocessableEntity)
	}
	_, err := coordinator.Result()
	if err == nil || !strings.Contains(err.Error(), "exchange 13") || !strings.Contains(err.Error(), "observed=3 expected=2") || !strings.Contains(err.Error(), "exchanges served=12") || !strings.Contains(err.Error(), "ExchangeCount=28") {
		t.Fatalf("rebuild-apply count mismatch error = %v, want exchange 13, observed=3 expected=2, exchanges served=12, and ExchangeCount=28", err)
	}
}

func TestRebuildApplyIncompleteResultNamesServedAndExpectedExchanges(t *testing.T) {
	scenario := loadRebuildApplyAuthoredScenario(t)
	coordinator := &RebuildApplyCoordinator{config: RebuildApplyCoordinatorConfig{Scenario: scenario}, nextSeq: 4}
	_, err := coordinator.Result()
	if err == nil || !strings.Contains(err.Error(), "exchanges served=3") || !strings.Contains(err.Error(), "ExchangeCount=28") {
		t.Fatalf("incomplete rebuild-apply error = %v, want exchanges served=3 and ExchangeCount=28", err)
	}
}

func TestRebuildApplyFinalWorkloadReachesTerminalExchange(t *testing.T) {
	scenario := loadRebuildApplyAuthoredScenario(t)
	var workload rebuildApplyWorkload
	if err := json.Unmarshal(scenario.Steps[0].Operation.Payload, &workload); err != nil {
		t.Fatalf("decode final rebuild-apply workload: %v", err)
	}
	coordinator := &RebuildApplyCoordinator{
		steps: []scenarios.Step{scenario.Steps[0]}, workloads: []rebuildApplyWorkload{workload}, stage: rebuildApplyStageComplete,
	}
	_, err := coordinator.advanceLocked(context.Background(), 1)
	if err == nil || !strings.Contains(err.Error(), "trace evidence is incomplete") || strings.Contains(err.Error(), "workload index is invalid") {
		t.Fatalf("final rebuild-apply transition error = %v, want terminal validation after final workload", err)
	}
}

func rebuildApplyCaptureFixture(t *testing.T, workload rebuildApplyWorkload) finalCapture {
	t.Helper()
	pages := (workload.RecordCount + workload.PageSize - 1) / workload.PageSize
	scopeFingerprint := strings.Repeat("a", 64)
	finalCursorFingerprint := strings.Repeat("b", 64)
	continuationFingerprint := strings.Repeat("c", 64)
	observations := []any{map[string]any{
		"sequence": 1, "operationClass": "connect", "statusCode": 200,
		"durationNanoseconds": 1, "requestFacts": map[string]any{"client_generation": 1},
	}}
	for page := uint64(0); page < pages; page++ {
		remaining := workload.RecordCount - page*workload.PageSize
		records := workload.PageSize
		if remaining < records {
			records = remaining
		}
		terminal := page == pages-1
		requestFacts := map[string]any{"limit": workload.PageSize}
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
		"cursorFingerprintsComplete": true, "requestFacts": map[string]any{"scope_count": 1},
		"pullResponseFacts": map[string]any{
			"change_count": 0, "has_more": false, "rebuild_scope_count": 0,
			"checksum_count": 1, "scope_cursor_fingerprints": []string{finalCursorFingerprint},
			"scope_cursor_fingerprints_complete": true,
		},
	})
	detailCount := workload.RecordCount
	if detailCount > 512 {
		detailCount = 0
	}
	scopeRows := make([]any, detailCount)
	for index := range scopeRows {
		scopeRows[index] = map[string]any{"scopeID": "scope-a"}
	}
	state := map[string]any{
		"schema":              map[string]any{"version": 1, "hash": strings.Repeat("d", 64)},
		"scopeStates":         []any{map[string]any{"scopeID": "scope-a"}},
		"scopeRows":           scopeRows,
		"rebuildAttempts":     []any{},
		"applicationRowCount": workload.RecordCount, "provenanceCount": workload.RecordCount,
		"scopeStateCount": 1, "scopeRowCount": workload.RecordCount,
		"rebuildAttemptCount": 0, "rebuildReceiptCount": pages,
		"provenanceMaintenanceWorkCursor": "cursor",
	}
	proof := map[string]any{
		"row_metadata": nil,
		"rebuild_receipt_proofs": []any{map[string]any{
			"rebuild_id_fingerprint": strings.Repeat("e", 64), "page_count": pages,
			"returned_record_count": workload.RecordCount, "request_chain_valid": true,
			"records_in_canonical_order": true, "row_checksums_valid": true,
			"scope_checksum_valid": true, "final_checksum_matches_local": true,
		}},
	}
	return finalCapture{
		ClientState: marshalRebuildApplyFixture(t, state),
		Pending:     marshalRebuildApplyFixture(t, []any{}),
		Rejected:    marshalRebuildApplyFixture(t, []any{}),
		Status:      marshalRebuildApplyFixture(t, map[string]any{"state": "ready", "retry_at": nil, "operation": nil, "failure": nil}),
		Events:      marshalRebuildApplyFixture(t, []any{map[string]any{"type": "rebuild_completed"}}),
		Provenance:  marshalRebuildApplyFixture(t, scopeRows),
		Trace: marshalRebuildApplyFixture(t, map[string]any{
			"observations": observations, "overflowed": false, "sequenceCheckpoint": pages + 2,
		}),
		DurableProof: marshalRebuildApplyFixture(t, proof),
	}
}

func marshalRebuildApplyFixture(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal rebuild-apply fixture: %v", err)
	}
	return raw
}

func loadRebuildApplyAuthoredScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	scenario, err := LoadRebuildApplyScenario(context.Background(), repoRoot)
	if err != nil {
		t.Fatalf("load authored rebuild-apply scenario: %v", err)
	}
	return scenario
}

func cloneRebuildApplyScenario(scenario scenarios.Scenario) scenarios.Scenario {
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
