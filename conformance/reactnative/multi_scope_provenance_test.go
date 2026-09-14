package reactnative

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

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
		ClientState: json.RawMessage(`{"scope_states":[]}`),
		Pending:     json.RawMessage(`[]`),
		Rejected:    json.RawMessage(`[]`),
		Provenance:  json.RawMessage(`[]`),
	}
	after := before
	after.Rows = json.RawMessage(`[{"id":"row-a","value":"after"}]`)
	if err := validateMultiScopeProvenanceNoProgress(before, after); err == nil {
		t.Fatal("post-restart application row change was accepted")
	}
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
