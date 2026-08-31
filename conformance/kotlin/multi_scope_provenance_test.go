package kotlin

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/trainstar/synchro/conformance/modelrunner"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestNewMultiScopeProvenanceCallReadsAuthoredMeasurement(t *testing.T) {
	callID := scenarios.NativeCallID("call-a")
	step := scenarios.Step{
		ID:        "connect",
		Transport: "http",
		NativeBinding: &scenarios.NativeStepBinding{
			Kind: "public-call", UserID: "user-a", ClientID: "client-a", CallID: &callID,
			Stage: "synchronous", Method: "sync-now", Completion: "idle",
		},
		MeasurementSample: &scenarios.MeasurementSample{Parameters: json.RawMessage(`{"provenance_scope_count":1}`)},
		Operation: scenarios.Operation{ContractOperation: "connect", Name: "send", Payload: json.RawMessage(`{
			"user_id":"user-a","client_id":"client-a","runtime_version":3,"protocol_version":3,
			"schema":{"version":1,"hash":"schema-hash"},"schema_reset":false,"scope_set_version":1,
			"known_scopes":[{"scope_id":"scope-a"},{"scope_id":"scope-b"}]
		}`)},
	}
	call, err := newMultiScopeProvenanceCall(step, nil)
	if err != nil {
		t.Fatalf("create call: %v", err)
	}
	if call.MeasuredScopeCount != 1 || call.KnownScopeCount != 2 {
		t.Fatalf("scope counts = (%d, %d), want measured 1 and known 2", call.MeasuredScopeCount, call.KnownScopeCount)
	}
}

func TestMultiScopeProvenanceRebuildBindingsRequireAuthoredOrder(t *testing.T) {
	callID := scenarios.NativeCallID("call-a")
	binding := &scenarios.NativeStepBinding{Kind: "public-call", UserID: "user-a", ClientID: "client-a", CallID: &callID, Stage: "synchronous", Method: "sync-now", Completion: "idle"}
	call := &multiScopeProvenanceCall{Client: Client{UserID: "user-a", ClientID: "client-a"}, CallID: string(callID)}
	begin := scenarios.Step{ID: "begin", Transport: "local", NativeBinding: binding, Operation: scenarios.Operation{Payload: json.RawMessage(`{
		"user_id":"user-a","client_id":"client-a","client_generation":1,
		"schema":{"version":1,"hash":"schema-hash"},"scope_id":"scope-a",
		"rebuild_id":"authored-rebuild","limit":100
	}`)}}
	rebuild, err := newMultiScopeProvenanceRebuild(begin, call)
	if err != nil {
		t.Fatalf("create rebuild: %v", err)
	}
	apply := scenarios.Step{ID: "apply-before-request", Transport: "local", NativeBinding: binding, Operation: scenarios.Operation{Payload: json.RawMessage(`{
		"user_id":"user-a","client_id":"client-a","scope_id":"scope-a",
		"rebuild_id":"authored-rebuild","page_ordinal":1,"request_token_source":"request"
	}`)}}
	if err := bindMultiScopeProvenanceRebuildApply(apply, call, rebuild); err == nil {
		t.Fatal("expected apply-before-request to be rejected")
	}
}

func TestMultiScopeProvenanceScopeSetVersionUsesAuthoredAnchor(t *testing.T) {
	scopeSetVersion := int64(7)
	plan := multiScopeProvenancePlan{CallOrder: []scenarios.StepID{"connect"}, Calls: map[scenarios.StepID]*multiScopeProvenanceCall{"connect": {}}}
	alias := scenarios.NativeIdentityAlias{Alias: "scope-version", StepIDs: []scenarios.StepID{"connect"}}
	got, err := multiScopeProvenanceRuntimeScopeSetVersion(plan, []SynchronizationResult{{transportObservations: []TransportObservation{{RequestFacts: &TransportRequestFacts{ScopeSetVersion: &scopeSetVersion}}}}}, alias)
	if err != nil {
		t.Fatalf("resolve scope-set-version: %v", err)
	}
	if got != 7 {
		t.Fatalf("scope-set-version = %d, want 7", got)
	}
}

func TestMultiScopeProvenancePlanAcceptsAuthoredScenario(t *testing.T) {
	scenario, err := scenarios.LoadFile(context.Background(), "../..", "conformance/scenarios/performance/multi-scope-provenance-001.json")
	if err != nil {
		t.Fatalf("load authored scenario: %v", err)
	}
	plan, err := multiScopeProvenancePlanForScenario(scenario)
	if err != nil {
		t.Fatalf("build authored plan: %v", err)
	}
	if len(plan.Calls) == 0 || len(plan.Clients) == 0 || plan.TransactionCount == 0 {
		t.Fatal("authored plan has incomplete coverage")
	}
}

func TestMultiScopeProvenanceModelResultMatchesAuthoredScenario(t *testing.T) {
	scenario, err := scenarios.LoadFile(context.Background(), "../..", "conformance/scenarios/performance/multi-scope-provenance-001.json")
	if err != nil {
		t.Fatalf("load authored scenario: %v", err)
	}
	modelScenario, err := multiScopeProvenanceModelScenario(scenario)
	if err != nil {
		t.Fatalf("prepare authored model: %v", err)
	}
	result, err := modelrunner.RunScenario(context.Background(), modelScenario)
	if err != nil {
		t.Fatalf("run authored model: %v", err)
	}
	if err := validateMultiScopeProvenanceModelResult(scenario, result); err != nil {
		t.Fatalf("validate authored model result: %v", err)
	}
}
