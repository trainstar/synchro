package modelrunner

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/reference"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestExpandScopeTopologyWorkloadDispatchesClosedOperations(t *testing.T) {
	model, snapshot := topologyWorkloadSnapshot(t)
	beforeModel := model.Snapshot()
	beforeSnapshot := snapshot
	payload := topologyWorkloadPayload(t, 2, 3)

	operations, err := expandScopeTopologyWorkload(snapshot, payload)
	if err != nil {
		t.Fatalf("expand scope topology workload: %v", err)
	}
	if !reflect.DeepEqual(snapshot, beforeSnapshot) || !reflect.DeepEqual(model.Snapshot(), beforeModel) {
		t.Fatal("scope topology expansion changed reference state")
	}

	wantKeys := []string{
		"model/commit-source-transaction",
		"process/materialize-source-transaction",
		"model/stage-registry-membership-generation",
		"model/activate-registry-membership-generation",
		"model/commit-source-transaction",
		"process/materialize-source-transaction",
	}
	if len(operations) != len(wantKeys) {
		t.Fatalf("operation count = %d, want %d", len(operations), len(wantKeys))
	}
	for index, operation := range operations {
		key := scenarios.OperationKey(operation)
		if key != wantKeys[index] {
			t.Fatalf("operation %d = %q, want %q", index, key, wantKeys[index])
		}
		if key == "workload/prepare" || strings.HasPrefix(key, "process/restart-") {
			t.Fatalf("operation %d is not substantive: %q", index, key)
		}
		if err := scenarios.ValidateOperation(operation); err != nil {
			t.Fatalf("operation %d is not closed and typed: %v", index, err)
		}
	}

	var stage struct {
		AffectedScopes []string `json:"affected_scopes"`
		ScopeRules     []struct {
			Evaluations []struct {
				Scopes []string `json:"scopes"`
			} `json:"evaluations"`
		} `json:"scope_rules"`
		DependencyImpacts []struct {
			AffectedRows []json.RawMessage `json:"affected_rows"`
		} `json:"dependency_impacts"`
	}
	if err := json.Unmarshal(operations[2].Payload, &stage); err != nil {
		t.Fatalf("decode staged topology: %v", err)
	}
	if !reflect.DeepEqual(stage.AffectedScopes, []string{"scope-a", "scope-b"}) {
		t.Fatalf("staged scopes = %v, want scope-a and scope-b", stage.AffectedScopes)
	}
	if len(stage.ScopeRules) != 1 || len(stage.ScopeRules[0].Evaluations) != 3 {
		t.Fatalf("staged scope rule does not contain the exact fanout: %#v", stage.ScopeRules)
	}
	for _, evaluation := range stage.ScopeRules[0].Evaluations {
		if !reflect.DeepEqual(evaluation.Scopes, []string{"scope-a", "scope-b"}) {
			t.Fatalf("staged scope rule does not contain the exact fanout: %#v", stage.ScopeRules)
		}
	}
	if len(stage.DependencyImpacts) != 1 || len(stage.DependencyImpacts[0].AffectedRows) != 3 {
		t.Fatalf("staged dependency impact does not contain three rows: %#v", stage.DependencyImpacts)
	}

	for index, operation := range operations {
		if _, err := model.Apply(context.Background(), operation); err != nil {
			t.Fatalf("dispatch operation %d (%s): %v", index, scenarios.OperationKey(operation), err)
		}
	}
	after := model.Snapshot()
	if after.Registry.CurrentGeneration != 2 || len(after.Stream.Transactions) != 2 || len(after.Stream.Materializations) != 4 || len(after.Rows) != 3 {
		t.Fatalf("typed topology operations did not materialize the expected state: %#v", after)
	}
	for _, scopeID := range []reference.ScopeID{"scope-a", "scope-b"} {
		scope, found := topologySnapshotScope(after, scopeID)
		if !found || scope.Cardinality != 3 || len(scope.Effects) != 3 {
			t.Fatalf("scope %q did not receive the materialized topology row: %#v", scopeID, scope)
		}
	}
}

func TestExpandScopeTopologyWorkloadRejectsMissingAuthoritativeInputs(t *testing.T) {
	_, snapshot := topologyWorkloadSnapshot(t)
	payload := topologyWorkloadPayload(t, 8, 1)
	tests := []struct {
		name    string
		mutate  func(*reference.StateSnapshot)
		message string
	}{
		{
			name: "scopes",
			mutate: func(value *reference.StateSnapshot) {
				value.Scopes = value.Scopes[:7]
			},
			message: "authoritative scopes",
		},
		{
			name: "schema",
			mutate: func(value *reference.StateSnapshot) {
				value.Schemas = nil
			},
			message: "schema",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := snapshot
			test.mutate(&candidate)
			if _, err := expandScopeTopologyWorkload(candidate, payload); err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("missing %s error = %v", test.name, err)
			}
		})
	}
}

func TestExpandScopeTopologyWorkloadPreservesCumulativeSampleEvidence(t *testing.T) {
	tests := []struct {
		path string
		want map[string]int
	}{
		{
			path: "conformance/scenarios/performance/fanout-001.json",
			want: map[string]int{"1/1": 3, "2/2": 3, "8/8": 3},
		},
		{
			path: "conformance/scenarios/performance/shared-private-scopes-001.json",
			want: map[string]int{"1/1000": 3, "8/1000": 3},
		},
	}
	for _, test := range tests {
		t.Run(filepath.Base(test.path), func(t *testing.T) {
			model, snapshot, scenario := topologyScenarioSnapshot(t, test.path)
			observed := make(map[string]int)
			rowCount := uint64(0)
			materializationCount := 0
			topologyStepCount := 0
			for index, step := range scenario.Steps {
				if scenarios.OperationKey(step.Operation) != "workload/prepare" {
					continue
				}
				topologyStepCount++
				var request scopeTopologyRequest
				if err := json.Unmarshal(step.Operation.Payload, &request); err != nil {
					t.Fatalf("decode sample %d: %v", index+1, err)
				}
				operations, err := expandScopeTopologyWorkload(snapshot, topologyPayloadMap(t, step.Operation.Payload))
				if err != nil {
					t.Fatalf("expand sample %d: %v", index+1, err)
				}
				assertScopeTopologySampleEvidence(t, operations, request, uint64(index+1))
				for _, operation := range operations {
					if _, err := model.Apply(context.Background(), operation); err != nil {
						t.Fatalf("dispatch sample %d %s: %v", index+1, scenarios.OperationKey(operation), err)
					}
				}
				observed[fmt.Sprintf("%d/%d", request.ScopeFanout, request.ImpactRows)]++
				missingRows := uint64(0)
				if request.ImpactRows > rowCount {
					missingRows = request.ImpactRows - rowCount
					rowCount = request.ImpactRows
				}
				materializationCount += int(missingRows) + 1
				snapshot = model.Snapshot()
			}
			if !reflect.DeepEqual(observed, test.want) {
				t.Fatalf("sample strata = %v, want %v", observed, test.want)
			}
			if len(snapshot.Rows) != int(rowCount) || len(snapshot.Stream.Materializations) != materializationCount || snapshot.Registry.CurrentGeneration != reference.Generation(topologyStepCount+1) {
				t.Fatalf("cumulative sample state is incomplete: %#v", snapshot)
			}
		})
	}
}

func topologyWorkloadSnapshot(t *testing.T) (*reference.Model, reference.StateSnapshot) {
	t.Helper()
	model, snapshot, _ := topologyScenarioSnapshot(t, "conformance/scenarios/performance/fanout-001.json")
	return model, snapshot
}

func topologyScenarioSnapshot(t *testing.T, scenarioPath string) (*reference.Model, reference.StateSnapshot, scenarios.Scenario) {
	t.Helper()
	repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	scenario, err := scenarios.LoadFile(context.Background(), repositoryRoot, scenarioPath)
	if err != nil {
		t.Fatalf("load topology scenario: %v", err)
	}
	model, err := NewModel(404)
	if err != nil {
		t.Fatalf("create model: %v", err)
	}
	if _, err := model.Apply(context.Background(), scenario.Model.Setup[0]); err != nil {
		t.Fatalf("install topology scenario: %v", err)
	}
	return model, model.Snapshot(), scenario
}

func topologyWorkloadPayload(t *testing.T, fanout, impactRows uint64) map[string]json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(map[string]any{
		"profile":      "scope_topology",
		"scope_fanout": fanout,
		"impact_rows":  impactRows,
	})
	if err != nil {
		t.Fatalf("encode topology workload payload: %v", err)
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &payload); err != nil {
		t.Fatalf("decode topology workload payload: %v", err)
	}
	return payload
}

func topologyPayloadMap(t *testing.T, raw json.RawMessage) map[string]json.RawMessage {
	t.Helper()
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode topology workload payload: %v", err)
	}
	return payload
}

func assertScopeTopologySampleEvidence(t *testing.T, operations []scenarios.Operation, request scopeTopologyRequest, sample uint64) {
	t.Helper()
	if len(operations) != 6 {
		t.Fatalf("sample %d operation count = %d, want 6", sample, len(operations))
	}
	var stage struct {
		AffectedScopes []string `json:"affected_scopes"`
		ScopeRules     []struct {
			Evaluations []struct {
				Row struct {
					CanonicalWireJSON string `json:"canonical_wire_json"`
				} `json:"row"`
				Scopes []string `json:"scopes"`
			} `json:"evaluations"`
		} `json:"scope_rules"`
		DependencyImpacts []struct {
			AffectedRows []json.RawMessage `json:"affected_rows"`
		} `json:"dependency_impacts"`
	}
	if err := json.Unmarshal(operations[2].Payload, &stage); err != nil {
		t.Fatalf("decode sample %d staged topology: %v", sample, err)
	}
	if uint64(len(stage.AffectedScopes)) != request.ScopeFanout || len(stage.DependencyImpacts) != 1 || uint64(len(stage.DependencyImpacts[0].AffectedRows)) != request.ImpactRows {
		t.Fatalf("sample %d does not retain exact fanout and impact evidence: %#v", sample, stage)
	}
	wantRow := strconv.Quote("scope-topology-row-000001")
	for _, rule := range stage.ScopeRules {
		for _, evaluation := range rule.Evaluations {
			if evaluation.Row.CanonicalWireJSON == wantRow && uint64(len(evaluation.Scopes)) == request.ScopeFanout {
				return
			}
		}
	}
	t.Fatalf("sample %d has no row evaluation with exact fanout %d", sample, request.ScopeFanout)
}

func topologySnapshotScope(snapshot reference.StateSnapshot, wanted reference.ScopeID) (reference.ScopeState, bool) {
	for _, entry := range snapshot.Scopes {
		if entry.Key == wanted {
			return entry.Value, true
		}
	}
	return reference.ScopeState{}, false
}
