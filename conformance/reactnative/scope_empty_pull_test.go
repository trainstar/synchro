package reactnative

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

var scopeEmptyPullTestEvidence = scopeEmptyPullEvidence{grantedScope: "cf:global", tableName: "cf_global_items", primaryField: "id", recordID: "row-granted"}

const (
	scopeEmptyPullTestRebuildID = "rebuild-granted"
	scopeEmptyPullTestCursor    = "cursor-granted"
)

func loadScopeEmptyPullAuthoredScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	scenario, err := LoadScopeEmptyPullScenario(context.Background(), filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("load React Native scope-empty-pull scenario: %v", err)
	}
	return scenario
}

func scopeEmptyPullTestJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode scope-empty-pull fixture: %v", err)
	}
	return encoded
}

func scopeEmptyPullTestConnect(t *testing.T) transportObservation {
	return transportObservation{
		Sequence: 1, OperationClass: "connect", StatusCode: 200, DurationNanoseconds: 1,
		RequestFacts: scopeEmptyPullTestJSON(t, map[string]any{"protocol_version": 3, "schema_version": 0, "schema_hash": "", "scope_set_version": 0, "scope_count": 0}),
	}
}

func scopeEmptyPullTestPull(t *testing.T, sequence uint64, rebuildScopes int) transportObservation {
	complete := true
	return transportObservation{
		Sequence: sequence, OperationClass: "pull", StatusCode: 200, DurationNanoseconds: 1,
		CursorFingerprints: []string{}, CursorFingerprintsComplete: &complete,
		RequestFacts:      scopeEmptyPullTestJSON(t, map[string]any{"client_generation": 1, "schema_version": 1, "schema_hash": strings.Repeat("a", 64), "scope_set_version": 1, "scope_count": 0, "limit": 100}),
		PullResponseFacts: scopeEmptyPullTestJSON(t, map[string]any{"change_count": 0, "has_more": false, "rebuild_scope_count": rebuildScopes, "checksum_count": rebuildScopes, "scope_cursor_fingerprints": []string{}, "scope_cursor_fingerprints_complete": true}),
	}
}

func scopeEmptyPullTestRebuild(t *testing.T) transportObservation {
	scope := hashFingerprint(scopeEmptyPullTestEvidence.grantedScope)
	return transportObservation{
		Sequence: 4, OperationClass: "rebuild", StatusCode: 200, DurationNanoseconds: 1,
		RequestFacts:         scopeEmptyPullTestJSON(t, map[string]any{"client_generation": 1, "schema_version": 1, "schema_hash": strings.Repeat("a", 64), "scope_fingerprint": scope, "rebuild_id_fingerprint": hashFingerprint(scopeEmptyPullTestRebuildID), "cursor_present": false, "limit": 100}),
		RebuildResponseFacts: scopeEmptyPullTestJSON(t, map[string]any{"record_count": 1, "has_more": false, "has_cursor": false, "has_final_scope_cursor": true, "has_checksum": true, "scope_fingerprint": scope, "final_scope_cursor_fingerprint": hashFingerprint(scopeEmptyPullTestCursor)}),
	}
}

func scopeEmptyPullTestTrace(t *testing.T, observations ...transportObservation) json.RawMessage {
	return scopeEmptyPullTestJSON(t, traceSnapshot{Observations: observations, SequenceCheckpoint: uint64(len(observations))})
}

func scopeEmptyPullTestState(t *testing.T, granted bool) json.RawMessage {
	state := map[string]any{
		"schema": map[string]any{"version": 1, "hash": strings.Repeat("a", 64)}, "scopeStates": []any{}, "scopeRows": []any{}, "rebuildAttempts": []any{},
		"applicationRowCount": 0, "mutationLedgerCount": 0, "mutationOutcomeCount": 0, "sealedBatchCount": 0, "rejectedMutationCount": 0,
		"scopeStateCount": 0, "scopeRowCount": 0, "provenanceCount": 0, "rowMetadataCount": 0, "rebuildAttemptCount": 0, "rebuildReceiptCount": 0,
		"provenanceMaintenanceWorkCursor": "0",
	}
	if granted {
		state["scopeStates"] = []any{map[string]any{"scopeID": "cf:global", "cursor": scopeEmptyPullTestCursor, "checksum": strings.Repeat("c", 64), "localChecksum": strings.Repeat("c", 64), "generation": 1}}
		state["scopeRows"] = []any{scopeEmptyPullTestScopeRow()}
		state["applicationRowCount"], state["scopeStateCount"], state["scopeRowCount"], state["provenanceCount"] = 1, 1, 1, 1
	}
	return scopeEmptyPullTestJSON(t, state)
}

func scopeEmptyPullTestScopeRow() clientScopeRow {
	return clientScopeRow{ScopeID: "cf:global", TableName: "cf_global_items", RecordID: "row-granted", Checksum: strings.Repeat("d", 64), Generation: 1}
}

func scopeEmptyPullTestStartCapture(t *testing.T) finalCapture {
	return finalCapture{
		ClientState: scopeEmptyPullTestState(t, false), Pending: json.RawMessage(`[]`), Rejected: json.RawMessage(`[]`),
		Status: json.RawMessage(`{"state":"ready","retry_at":null,"operation":null,"failure":null}`),
		Trace:  scopeEmptyPullTestTrace(t, scopeEmptyPullTestConnect(t), scopeEmptyPullTestPull(t, 2, 0)),
	}
}

func scopeEmptyPullTestFinalCapture(t *testing.T) finalCapture {
	return finalCapture{
		ClientState: scopeEmptyPullTestState(t, true), Pending: json.RawMessage(`[]`), Rejected: json.RawMessage(`[]`),
		Status:     json.RawMessage(`{"state":"ready","retry_at":null,"operation":null,"failure":null}`),
		Events:     json.RawMessage(`[{"type":"rebuild_completed","scope_id":"cf:global","rebuild_id":"` + scopeEmptyPullTestRebuildID + `"}]`),
		Provenance: scopeEmptyPullTestJSON(t, []clientScopeRow{scopeEmptyPullTestScopeRow()}),
		Rows:       json.RawMessage(`[{"id":"row-granted"}]`),
		Trace:      scopeEmptyPullTestTrace(t, scopeEmptyPullTestConnect(t), scopeEmptyPullTestPull(t, 2, 0), scopeEmptyPullTestPull(t, 3, 1), scopeEmptyPullTestRebuild(t)),
	}
}

func scopeEmptyPullTestStart(t *testing.T) traceSnapshot {
	t.Helper()
	start, err := validateScopeEmptyPullStartCapture(scopeEmptyPullTestStartCapture(t), 100)
	if err != nil {
		t.Fatalf("validate authored start capture: %v", err)
	}
	return start
}

func TestValidateScopeEmptyPullScenarioAcceptsAuthoredContract(t *testing.T) {
	scenario := loadScopeEmptyPullAuthoredScenario(t)
	steps, limit, err := scopeEmptyPullScenarioSteps(scenario)
	if err != nil {
		t.Fatalf("bind authored scope-empty-pull steps: %v", err)
	}
	if limit != 100 || steps.connect.NativeBinding.Method != "start" || steps.syncPull.NativeBinding.Method != "sync-now" {
		t.Fatalf("authored scope-empty-pull calls = limit %d, %s then %s", limit, steps.connect.NativeBinding.Method, steps.syncPull.NativeBinding.Method)
	}
}

func TestValidateScopeEmptyPullScenarioRejectsContractChanges(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*scenarios.Scenario)
	}{
		{"reconnecting sync call", func(s *scenarios.Scenario) {
			for index := range s.Steps {
				if s.Steps[index].ID == "STEP-SCOPE-EMPTY-PULL-SYNC-PULL-001" {
					binding := *s.Steps[index].NativeBinding
					binding.Method = "start"
					s.Steps[index].NativeBinding = &binding
				}
			}
		}},
		{"different rebuild limit", func(s *scenarios.Scenario) {
			for index := range s.Steps {
				if s.Steps[index].ID == "STEP-SCOPE-EMPTY-PULL-REBUILD-001" {
					var payload map[string]any
					_ = json.Unmarshal(s.Steps[index].Operation.Payload, &payload)
					payload["limit"] = 1
					s.Steps[index].Operation.Payload, _ = json.Marshal(payload)
				}
			}
		}},
		{"Android proof target", func(s *scenarios.Scenario) {
			obligations := append([]scenarios.ProofObligation(nil), s.ProofObligations...)
			for index := range obligations {
				if string(obligations[index].ObligationID) == "OBL-SCOPE-EMPTY-PULL-RN-ANDROID-CURRENT-001" {
					obligations[index].MakeTarget = "test-rn-other"
				}
			}
			s.ProofObligations = obligations
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			scenario := loadScopeEmptyPullAuthoredScenario(t)
			scenario.Steps = append([]scenarios.Step(nil), scenario.Steps...)
			test.mutate(&scenario)
			if err := ValidateScopeEmptyPullScenario(scenario); err == nil {
				t.Fatal("changed scope-empty-pull contract was accepted")
			}
		})
	}
}

func TestNewScopeEmptyPullCoordinatorKeepsAndroidSidecarOnHostLoopback(t *testing.T) {
	coordinator, err := NewScopeEmptyPullCoordinator(ScopeEmptyPullCoordinatorConfig{
		Scenario: loadScopeEmptyPullAuthoredScenario(t), Platform: "android", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("Android scope-empty-pull coordinator was rejected: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	if !strings.HasPrefix(coordinator.URL(), "http://127.0.0.1:") || !strings.HasPrefix(coordinator.adapter, "http://10.0.2.2:") {
		t.Fatalf("Android scope-empty-pull coordinator URL = %q, adapter = %q", coordinator.URL(), coordinator.adapter)
	}
}

func TestScopeEmptyPullCommandsFollowAuthoredCalls(t *testing.T) {
	coordinator, err := NewScopeEmptyPullCoordinator(ScopeEmptyPullCoordinatorConfig{
		Scenario: loadScopeEmptyPullAuthoredScenario(t), Platform: "ios", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create scope-empty-pull coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	passed := func(result string) json.RawMessage {
		return json.RawMessage(`{"schema_version":1,"outcome":"passed","result":` + result + `,"error_code":null,"error_detail":null}`)
	}
	process := `{"process_id":"process-a","database_identity_fingerprint":"` + strings.Repeat("b", 64) + `"}`
	status := `{"state":"ready","retry_at":null,"operation":null,"failure":null}`
	for index, test := range []struct {
		result          json.RawMessage
		actor, command  string
		method          any
		wantCaptureKeys []string
	}{
		{json.RawMessage(`null`), "client", "open", nil, nil},
		{passed(`{"kind":"opened","status":` + status + `,"process":` + process + `}`), "client", "synchronize-step", "start", nil},
		{passed(`{"kind":"synchronized","completion":"idle","status":` + status + `,"process":` + process + `}`), "observer", "capture", nil, scopeEmptyPullStartCaptureSources},
	} {
		response, err := coordinator.exchangeLocked(context.Background(), exchangeRequest{SchemaVersion: 1, Sequence: uint64(index + 1), Result: test.result})
		if err != nil {
			t.Fatalf("scope-empty-pull exchange %d: %v", index+1, err)
		}
		command := response.Command
		if response.State != "command" || command == nil || command.Action.Action.Actor != test.actor || command.Action.Action.Command != test.command || command.Runtime.PullPageSize != 100 || command.Runtime.ClientKey != scopeEmptyPullClientKey {
			t.Fatalf("scope-empty-pull exchange %d = %#v, want %s/%s", index+1, command, test.actor, test.command)
		}
		if test.method != nil && command.Action.Action.Parameters["method"] != test.method {
			t.Fatalf("scope-empty-pull exchange %d method = %v, want %v", index+1, command.Action.Action.Parameters["method"], test.method)
		}
		if test.wantCaptureKeys != nil && strings.Join(command.Action.Action.Parameters["sources"].([]string), ",") != strings.Join(test.wantCaptureKeys, ",") {
			t.Fatalf("scope-empty-pull exchange %d sources = %v", index+1, command.Action.Action.Parameters["sources"])
		}
	}
	if coordinator.ExchangeCount() != 6 {
		t.Fatalf("scope-empty-pull ExchangeCount = %d, want five commands and one completion", coordinator.ExchangeCount())
	}
}

func TestScopeEmptyPullAcceptsAuthoredFlow(t *testing.T) {
	start := scopeEmptyPullTestStart(t)
	if err := validateScopeEmptyPullFinalCapture(scopeEmptyPullTestFinalCapture(t), start, 100, scopeEmptyPullTestEvidence, scopeEmptyPullTestRebuildID); err != nil {
		t.Fatalf("validate authored final capture: %v", err)
	}
}

// CTRL-SCOPE-009 restores the guard that returns from the pull loop when the
// local scope set is empty. The client then sends no pull in either call.
func TestScopeEmptyPullRejectsEmptyScopeGuard(t *testing.T) {
	guardedStart := scopeEmptyPullTestStartCapture(t)
	guardedStart.Trace = scopeEmptyPullTestTrace(t, scopeEmptyPullTestConnect(t))
	if _, err := validateScopeEmptyPullStartCapture(guardedStart, 100); err == nil {
		t.Fatal("start call with no pull passed")
	}
	guardedFinal := scopeEmptyPullTestStartCapture(t)
	guardedFinal.Provenance, guardedFinal.Rows = json.RawMessage(`[]`), json.RawMessage(`[]`)
	guardedFinal.Events = json.RawMessage(`[]`)
	if _, err := completedRebuildID(guardedFinal.Events, scopeEmptyPullTestEvidence.grantedScope); err == nil {
		t.Fatal("session with no rebuild produced a completed rebuild identity")
	}
	if err := validateScopeEmptyPullFinalCapture(guardedFinal, scopeEmptyPullTestStart(t), 100, scopeEmptyPullTestEvidence, scopeEmptyPullTestRebuildID); err == nil {
		t.Fatal("sync cycle with no pull passed")
	}
}

func TestScopeEmptyPullRejectsDivergentStart(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*testing.T, *finalCapture)
	}{
		{"known scope on connect", func(t *testing.T, capture *finalCapture) {
			connect := scopeEmptyPullTestConnect(t)
			connect.RequestFacts = scopeEmptyPullTestJSON(t, map[string]any{"protocol_version": 3, "schema_version": 0, "schema_hash": "", "scope_set_version": 0, "scope_count": 1})
			capture.Trace = scopeEmptyPullTestTrace(t, connect, scopeEmptyPullTestPull(t, 2, 0))
		}},
		{"scope in pull", func(t *testing.T, capture *finalCapture) {
			pull := scopeEmptyPullTestPull(t, 2, 0)
			pull.RequestFacts = scopeEmptyPullTestJSON(t, map[string]any{"client_generation": 1, "schema_version": 1, "schema_hash": strings.Repeat("a", 64), "scope_set_version": 1, "scope_count": 1, "limit": 100})
			capture.Trace = scopeEmptyPullTestTrace(t, scopeEmptyPullTestConnect(t), pull)
		}},
		{"added scope at start", func(t *testing.T, capture *finalCapture) {
			capture.Trace = scopeEmptyPullTestTrace(t, scopeEmptyPullTestConnect(t), scopeEmptyPullTestPull(t, 2, 1))
		}},
		{"known scope state", func(t *testing.T, capture *finalCapture) { capture.ClientState = scopeEmptyPullTestState(t, true) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			capture := scopeEmptyPullTestStartCapture(t)
			test.change(t, &capture)
			if _, err := validateScopeEmptyPullStartCapture(capture, 100); err == nil {
				t.Fatal("divergent start capture passed")
			}
		})
	}
}

func TestScopeEmptyPullRejectsDivergentSync(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*testing.T, *finalCapture)
	}{
		{"reconnect", func(t *testing.T, capture *finalCapture) {
			connect := scopeEmptyPullTestConnect(t)
			connect.Sequence = 3
			pull, rebuild := scopeEmptyPullTestPull(t, 4, 1), scopeEmptyPullTestRebuild(t)
			rebuild.Sequence = 5
			capture.Trace = scopeEmptyPullTestTrace(t, scopeEmptyPullTestConnect(t), scopeEmptyPullTestPull(t, 2, 0), connect, pull, rebuild)
		}},
		{"no added scope", func(t *testing.T, capture *finalCapture) {
			capture.Trace = scopeEmptyPullTestTrace(t, scopeEmptyPullTestConnect(t), scopeEmptyPullTestPull(t, 2, 0), scopeEmptyPullTestPull(t, 3, 0), scopeEmptyPullTestRebuild(t))
		}},
		{"other rebuild scope", func(t *testing.T, capture *finalCapture) {
			rebuild := scopeEmptyPullTestRebuild(t)
			rebuild.RequestFacts = scopeEmptyPullTestJSON(t, map[string]any{"client_generation": 1, "schema_version": 1, "schema_hash": strings.Repeat("a", 64), "scope_fingerprint": hashFingerprint("user:user-a"), "rebuild_id_fingerprint": hashFingerprint(scopeEmptyPullTestRebuildID), "cursor_present": false, "limit": 100})
			capture.Trace = scopeEmptyPullTestTrace(t, scopeEmptyPullTestConnect(t), scopeEmptyPullTestPull(t, 2, 0), scopeEmptyPullTestPull(t, 3, 1), rebuild)
		}},
		{"other cursor", func(t *testing.T, capture *finalCapture) {
			var state map[string]any
			_ = json.Unmarshal(capture.ClientState, &state)
			state["scopeStates"].([]any)[0].(map[string]any)["cursor"] = "cursor-other"
			capture.ClientState = scopeEmptyPullTestJSON(t, state)
		}},
		{"no row", func(t *testing.T, capture *finalCapture) { capture.Rows = json.RawMessage(`[]`) }},
		{"other row", func(t *testing.T, capture *finalCapture) { capture.Rows = json.RawMessage(`[{"id":"row-other"}]`) }},
		{"no provenance", func(t *testing.T, capture *finalCapture) { capture.Provenance = json.RawMessage(`[]`) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			capture := scopeEmptyPullTestFinalCapture(t)
			test.change(t, &capture)
			if err := validateScopeEmptyPullFinalCapture(capture, scopeEmptyPullTestStart(t), 100, scopeEmptyPullTestEvidence, scopeEmptyPullTestRebuildID); err == nil {
				t.Fatal("divergent final capture passed")
			}
		})
	}
}
