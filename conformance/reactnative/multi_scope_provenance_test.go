package reactnative

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestValidateMultiScopeProvenanceScenarioAcceptsAuthoredContract(t *testing.T) {
	scenario := loadMultiScopeProvenanceScenario(t)
	if err := ValidateMultiScopeProvenanceScenario(scenario); err != nil {
		t.Fatalf("validate authored multi-scope provenance scenario: %v", err)
	}
	calls, err := multiScopeProvenanceCalls(scenario)
	if err != nil {
		t.Fatalf("read authored multi-scope provenance calls: %v", err)
	}
	last := calls[len(calls)-1]
	if len(calls) != 8 || !last.restart || last.preRestartCall != "STEP-PERF-MULTI-SCOPE-PROVENANCE-007-CONNECT-001" || last.step.ID != "STEP-PERF-MULTI-SCOPE-PROVENANCE-008-CONNECT-001" {
		t.Fatalf("authored restart call is incomplete: %#v", last)
	}
}

func TestValidateMultiScopeProvenanceScenarioRejectsContractChanges(t *testing.T) {
	scenario := cloneMultiScopeProvenanceScenario(loadMultiScopeProvenanceScenario(t))
	for index := range scenario.ProofObligations {
		if string(scenario.ProofObligations[index].ObligationID) == "OBL-PERF-MULTI-SCOPE-PROVENANCE-RN-ANDROID-CURRENT-001" {
			scenario.ProofObligations[index].MakeTarget = "test-rn-other"
		}
	}
	if err := ValidateMultiScopeProvenanceScenario(scenario); err == nil {
		t.Fatal("changed Android proof target was accepted")
	}
}

func TestValidateMultiScopeProvenanceScenarioRejectsMissingDurableRestart(t *testing.T) {
	scenario := cloneMultiScopeProvenanceScenario(loadMultiScopeProvenanceScenario(t))
	steps := make([]scenarios.Step, 0, len(scenario.Steps)-1)
	for _, step := range scenario.Steps {
		if step.ID != "STEP-PERF-MULTI-SCOPE-PROVENANCE-007-RESTART-001" {
			steps = append(steps, step)
		}
	}
	scenario.Steps = steps
	if err := ValidateMultiScopeProvenanceScenario(scenario); err == nil {
		t.Fatal("missing durable restart was accepted")
	}
}

func TestValidateMultiScopeProvenanceRestartRequiresNewProcessAndSameDatabase(t *testing.T) {
	digest := strings.Repeat("a", 64)
	prior := actionProcessIdentity{ProcessID: "process-a", DatabaseIdentityFingerprint: digest}
	opened := func(processID, databaseFingerprint string) json.RawMessage {
		return json.RawMessage(`{"kind":"opened","status":{"state":"local_ready","retry_at":null,"operation":null,"failure":null},"process":{"process_id":"` + processID + `","database_identity_fingerprint":"` + databaseFingerprint + `"}}`)
	}
	if _, err := validateMultiScopeProvenanceRestart(prior, opened(prior.ProcessID, digest)); err == nil {
		t.Fatal("restart retained the old process identity")
	}
	if _, err := validateMultiScopeProvenanceRestart(prior, opened("process-b", strings.Repeat("b", 64))); err == nil {
		t.Fatal("restart changed the database identity")
	}
	if _, err := validateMultiScopeProvenanceRestart(prior, opened("process-b", digest)); err != nil {
		t.Fatalf("valid restart was rejected: %v", err)
	}
}

func TestValidateMultiScopeProvenanceNoProgressIncludesApplicationRows(t *testing.T) {
	before := finalCapture{
		Rows:        json.RawMessage(`[{"id":"row-a","value":"before"}]`),
		ClientState: multiScopeProvenanceClientState(t, "1", 1),
		Pending:     json.RawMessage(`[]`),
		Rejected:    json.RawMessage(`[]`),
		Provenance:  json.RawMessage(`[]`),
		Trace:       multiScopeProvenanceNoProgressTrace(t, "cursor"),
	}
	after := before
	after.Rows = json.RawMessage(`[{"id":"row-a","value":"after"}]`)
	if err := validateMultiScopeProvenanceNoProgress(before, after); err == nil {
		t.Fatal("post-restart application row change was accepted")
	}
}

func TestValidateMultiScopeProvenanceNoProgressIgnoresOnlyMaintenanceCursor(t *testing.T) {
	before := finalCapture{
		Rows:        json.RawMessage(`[]`),
		ClientState: multiScopeProvenanceClientState(t, "1", 0),
		Pending:     json.RawMessage(`[]`),
		Rejected:    json.RawMessage(`[]`),
		Provenance:  json.RawMessage(`[]`),
		Trace:       multiScopeProvenanceNoProgressTrace(t, "cursor"),
	}
	after := before
	after.ClientState = multiScopeProvenanceClientState(t, "2", 0)
	if err := validateMultiScopeProvenanceNoProgress(before, after); err != nil {
		t.Fatalf("maintenance-only progress was rejected: %v", err)
	}
	after.ClientState = multiScopeProvenanceClientState(t, "2", 1)
	if err := validateMultiScopeProvenanceNoProgress(before, after); err == nil {
		t.Fatal("application row count change was accepted")
	}
}

func TestMultiScopeProvenanceCaptureUsesBoundedRuntimeRowSelectors(t *testing.T) {
	values := []blackbox.NativeIdentityValue{
		{Kind: "table", Alias: "items-table", RuntimeValue: json.RawMessage(`"runtime-items-table"`), ApplicationIdentifier: "cf_items"},
		{Kind: "primary-key", Alias: "row-one-primary-key", RuntimeValue: json.RawMessage(`"00000000-0000-4000-8000-000000000001"`), ApplicationIdentifier: "id"},
		{Kind: "primary-key", Alias: "row-two-primary-key", RuntimeValue: json.RawMessage(`"00000000-0000-4000-8000-000000000002"`), ApplicationIdentifier: "id"},
	}
	selectors, err := multiScopeProvenanceApplicationSelectors(values)
	if err != nil {
		t.Fatalf("derive multi-scope row selectors: %v", err)
	}
	want := []map[string]any{
		{"table_name": "cf_items", "primary_key_field": "id", "primary_key": "00000000-0000-4000-8000-000000000001"},
		{"table_name": "cf_items", "primary_key_field": "id", "primary_key": "00000000-0000-4000-8000-000000000002"},
	}
	if !reflect.DeepEqual(selectors, want) {
		t.Fatalf("multi-scope row selectors = %#v, want %#v", selectors, want)
	}
}

func TestMultiScopeProvenanceNoProgressBindsRenewedCursorsToTheServer(t *testing.T) {
	before := finalCapture{
		Rows: json.RawMessage(`[]`), Pending: json.RawMessage(`[]`),
		Rejected: json.RawMessage(`[]`), Provenance: json.RawMessage(`[]`),
		ClientState: multiScopeProvenanceClientState(t, "1", 0),
	}
	after := before
	state, err := decodeClientState(before.ClientState)
	if err != nil {
		t.Fatal(err)
	}
	renewedCursor := "renewed-cursor"
	state.ScopeStates[0].Cursor = &renewedCursor
	after.ClientState, err = json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	after.Trace = multiScopeProvenanceNoProgressTrace(t, renewedCursor)
	if err := validateMultiScopeProvenanceNoProgress(before, after); err != nil {
		t.Fatalf("server cursor renewal was rejected: %v", err)
	}
	after.Trace = multiScopeProvenanceNoProgressTrace(t, "another-cursor")
	if err := validateMultiScopeProvenanceNoProgress(before, after); err == nil {
		t.Fatal("cursor without a matching server response was accepted")
	}
}

func multiScopeProvenanceNoProgressTrace(t *testing.T, cursor string) json.RawMessage {
	t.Helper()
	complete := true
	raw, err := json.Marshal(traceSnapshot{
		SequenceCheckpoint: 1,
		Observations: []transportObservation{{
			Sequence: 1, OperationClass: "pull", StatusCode: 200, DurationNanoseconds: 1,
			RequestFacts: json.RawMessage(`{"scope_count":1}`), CursorFingerprintsComplete: &complete,
			CursorFingerprints: []string{hashFingerprint("cursor")},
			PullResponseFacts: json.RawMessage(`{"change_count":0,"has_more":false,"rebuild_scope_count":0,"checksum_count":1,"scope_cursor_fingerprints":["` +
				hashFingerprint(cursor) + `"],"scope_cursor_fingerprints_complete":true}`),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func multiScopeProvenanceClientState(t *testing.T, maintenanceCursor string, applicationRows uint64) json.RawMessage {
	t.Helper()
	cursor := "cursor"
	encoded, err := json.Marshal(inspectedClientState{
		Schema:                          &clientSchema{Version: 1, Hash: strings.Repeat("a", 64)},
		ScopeStates:                     []clientScopeState{{ScopeID: "scope-a", Cursor: &cursor, Generation: 1}},
		ScopeStateCount:                 1,
		ScopeRows:                       []clientScopeRow{},
		RebuildAttempts:                 []rebuildAttempt{},
		ApplicationRowCount:             applicationRows,
		ProvenanceMaintenanceWorkCursor: maintenanceCursor,
	})
	if err != nil {
		t.Fatalf("encode multi-scope client state: %v", err)
	}
	return encoded
}

func TestNewMultiScopeProvenanceCoordinatorKeepsAndroidSidecarOnHostLoopback(t *testing.T) {
	coordinator, err := NewMultiScopeProvenanceCoordinator(MultiScopeProvenanceCoordinatorConfig{Scenario: loadMultiScopeProvenanceScenario(t), Platform: "android", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token"})
	if err != nil || coordinator == nil {
		t.Fatalf("Android multi-scope provenance coordinator was rejected: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	if !strings.HasPrefix(coordinator.URL(), "http://127.0.0.1:") {
		t.Fatalf("Android coordinator URL = %q", coordinator.URL())
	}
	if !strings.HasPrefix(coordinator.adapter, "http://10.0.2.2:") {
		t.Fatalf("Android adapter URL = %q", coordinator.adapter)
	}
}

func loadMultiScopeProvenanceScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	scenario, err := LoadMultiScopeProvenanceScenario(context.Background(), root)
	if err != nil {
		t.Fatalf("load authored multi-scope provenance scenario: %v", err)
	}
	return scenario
}

func cloneMultiScopeProvenanceScenario(scenario scenarios.Scenario) scenarios.Scenario {
	raw, err := json.Marshal(scenario)
	if err != nil {
		panic(err)
	}
	var clone scenarios.Scenario
	if json.Unmarshal(raw, &clone) != nil {
		panic("decode scenario")
	}
	return clone
}
