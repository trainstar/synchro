package reactnative

import (
	"context"
	"encoding/json"
	"path/filepath"
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
	if got, want := coordinator.ExchangeCount(), len(coordinator.config.Scenario.Steps)*3+1; got != want {
		t.Fatalf("rebuild-apply exchange count = %d, want %d", got, want)
	}
}

func TestValidateRebuildApplyCaptureUsesAuthoredRebuildAttemptCount(t *testing.T) {
	scenario := loadRebuildApplyAuthoredScenario(t)
	expected := rebuildApplyExpectedState(scenario)
	step := scenario.Steps[0]
	marshal := func(value any) json.RawMessage {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("marshal rebuild-apply capture fixture: %v", err)
		}
		return raw
	}
	scopeFingerprint := strings.Repeat("a", 64)
	finalCursorFingerprint := strings.Repeat("b", 64)
	trace := map[string]any{
		"observations": []any{
			map[string]any{
				"sequence": 1, "operationClass": "connect", "statusCode": 200,
				"durationNanoseconds": 1, "requestFacts": map[string]any{"client_generation": 1},
			},
			map[string]any{
				"sequence": 2, "operationClass": "rebuild", "statusCode": 200,
				"durationNanoseconds": 1, "requestFacts": map[string]any{"limit": 100},
				"rebuildResponseFacts": map[string]any{
					"record_count": 1, "has_more": false, "has_cursor": false,
					"has_final_scope_cursor": true, "has_checksum": true,
					"scope_fingerprint":              scopeFingerprint,
					"final_scope_cursor_fingerprint": finalCursorFingerprint,
				},
			},
			map[string]any{
				"sequence": 3, "operationClass": "pull", "statusCode": 200,
				"durationNanoseconds": 1, "cursorFingerprints": []string{finalCursorFingerprint},
				"cursorFingerprintsComplete": true, "requestFacts": map[string]any{"scope_count": 1},
				"pullResponseFacts": map[string]any{
					"change_count": 0, "has_more": false, "rebuild_scope_count": 0,
					"checksum_count": 1, "scope_cursor_fingerprints": []string{finalCursorFingerprint},
					"scope_cursor_fingerprints_complete": true,
				},
			},
		},
		"overflowed": false, "sequenceCheckpoint": 3,
	}
	state := map[string]any{
		"schema":              map[string]any{"version": 1, "hash": strings.Repeat("c", 64)},
		"scopeStates":         []any{map[string]any{"scopeID": "scope-a"}},
		"scopeRows":           []any{map[string]any{"scopeID": "scope-a"}},
		"rebuildAttempts":     []any{},
		"applicationRowCount": 1, "provenanceCount": 1, "scopeStateCount": 1,
		"rebuildAttemptCount": 1, "rebuildReceiptCount": 1,
		"provenanceMaintenanceWorkCursor": "cursor",
	}
	coordinator := &RebuildApplyCoordinator{
		expected:  expected,
		steps:     []scenarios.Step{step},
		workloads: []rebuildApplyWorkload{{Profile: "scope_cardinality", ScopeID: "scope-a", RecordCount: 1, PageSize: 100}},
	}
	capture := finalCapture{
		ClientState: marshal(state), Pending: marshal([]any{}), Rejected: marshal([]any{}),
		Status:     marshal(map[string]any{"state": "ready", "retry_at": nil, "operation": nil, "failure": nil}),
		Events:     marshal([]any{map[string]any{"type": "rebuild_completed"}}),
		Provenance: marshal([]any{map[string]any{"scopeID": "scope-a"}}), Trace: marshal(trace),
	}
	if err := coordinator.validateCapture(capture); err != nil {
		t.Fatalf("validate authored rebuild attempt count: %v", err)
	}
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
