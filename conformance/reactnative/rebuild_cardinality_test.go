package reactnative

import (
	"context"
	"encoding/json"
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
		Scenario: loadRebuildCardinalityAuthoredScenario(t), Platform: "android", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token", AppVersion: "0.3.0",
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
	if got, want := coordinator.ExchangeCount(), len(coordinator.config.Scenario.Steps)*3+1; got != want {
		t.Fatalf("rebuild-cardinality exchange count = %d, want %d", got, want)
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
