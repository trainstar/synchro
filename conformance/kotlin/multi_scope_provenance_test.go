package kotlin

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/trainstar/synchro/conformance/blackbox"
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
	if len(plan.Calls) != 8 || plan.RestartStep != "STEP-PERF-MULTI-SCOPE-PROVENANCE-007-RESTART-001" || plan.PreRestartCall != "STEP-PERF-MULTI-SCOPE-PROVENANCE-007-CONNECT-001" || plan.PostRestartCall != "STEP-PERF-MULTI-SCOPE-PROVENANCE-008-CONNECT-001" {
		t.Fatalf("authored restart plan is incomplete: %#v", plan)
	}
}

func TestMultiScopeProvenancePlanRejectsMissingDurableRestart(t *testing.T) {
	scenario, err := scenarios.LoadFile(context.Background(), "../..", "conformance/scenarios/performance/multi-scope-provenance-001.json")
	if err != nil {
		t.Fatalf("load authored scenario: %v", err)
	}
	steps := make([]scenarios.Step, 0, len(scenario.Steps)-1)
	for _, step := range scenario.Steps {
		if step.ID != "STEP-PERF-MULTI-SCOPE-PROVENANCE-007-RESTART-001" {
			steps = append(steps, step)
		}
	}
	scenario.Steps = steps
	if _, err := multiScopeProvenancePlanForScenario(scenario); err == nil {
		t.Fatal("plan accepted a missing durable restart")
	}
}

func TestMultiScopeProvenanceNoProgressIncludesApplicationRows(t *testing.T) {
	beforeCount := uint64(1)
	afterCount := uint64(2)
	plan := multiScopeProvenancePlan{RestartClient: Client{Key: "client-a", UserID: "user-a", ClientID: "client-a"}}
	before := scenarios.StateFacts{Clients: []scenarios.ClientDurabilityFact{{UserID: "user-a", ClientID: "client-a", RowCount: &beforeCount}}}
	after := scenarios.StateFacts{Clients: []scenarios.ClientDurabilityFact{{UserID: "user-a", ClientID: "client-a", RowCount: &afterCount}}}
	if err := validateMultiScopeProvenanceNoProgress(plan, before, after); err == nil {
		t.Fatal("post-restart application row change was accepted")
	}
}

func TestMultiScopeProvenancePairsRecordsByIdentityWhenAliasesReverseOrder(t *testing.T) {
	resolution := func(authored, runtime string) blackbox.NativeIdentityResolution {
		return blackbox.NativeIdentityResolution{AuthoredValue: json.RawMessage(strconv.Quote(authored)), RuntimeValue: json.RawMessage(strconv.Quote(runtime))}
	}
	// Each runtime value sorts in the reverse order of its authored value.
	resolutions := map[string]blackbox.NativeIdentityResolution{
		"row-a-key": resolution("row-a", "z-record"), "row-b-key": resolution("row-b", "a-record"),
		"scope-a": resolution("scope-a", "z-scope"), "scope-b": resolution("scope-b", "a-scope"),
		"row-a-version": resolution("v-a", "z-version"), "row-b-version": resolution("v-b", "a-version"),
	}
	tableNames := map[string]string{"items": "cf_items"}
	expected := []scenarios.ProvenanceFact{
		{TableID: "items", CanonicalWireJSON: `"row-a"`, Scopes: []string{"scope-a", "scope-b"}, Version: "v-a"},
		{TableID: "items", CanonicalWireJSON: `"row-b"`, Scopes: []string{"scope-a"}, Version: "v-b"},
	}
	observed := []scenarios.ProvenanceFact{
		{TableID: "cf_items", CanonicalWireJSON: `"a-record"`, Scopes: []string{"z-scope"}, Version: "a-version"},
		{TableID: "cf_items", CanonicalWireJSON: `"z-record"`, Scopes: []string{"a-scope", "z-scope"}, Version: "z-version"},
	}
	if err := validateMultiScopeProvenanceProvenance(expected, observed, resolutions, tableNames); err != nil {
		t.Fatalf("provenance with order-reversing aliases was rejected: %v", err)
	}
	// Each record now carries the scopes and version of the other record.
	swapped := []scenarios.ProvenanceFact{
		{TableID: "cf_items", CanonicalWireJSON: `"a-record"`, Scopes: []string{"a-scope", "z-scope"}, Version: "z-version"},
		{TableID: "cf_items", CanonicalWireJSON: `"z-record"`, Scopes: []string{"z-scope"}, Version: "a-version"},
	}
	if err := validateMultiScopeProvenanceProvenance(expected, swapped, resolutions, tableNames); err == nil {
		t.Fatal("provenance swapped between record identities was accepted")
	}
}

func TestMultiScopeProvenanceRecordIdentityKeepsJSONType(t *testing.T) {
	resolutions := map[string]blackbox.NativeIdentityResolution{
		"row-key": {AuthoredValue: json.RawMessage(`"42"`), RuntimeValue: json.RawMessage(`"7"`)},
	}
	if !multiScopeProvenanceCanonicalIdentityMatches(resolutions, `"42"`, `"7"`) {
		t.Fatal("string record identity did not resolve")
	}
	if multiScopeProvenanceCanonicalIdentityMatches(resolutions, `42`, `"7"`) {
		t.Fatal("numeric authored key resolved through a string alias")
	}
	if multiScopeProvenanceCanonicalIdentityMatches(resolutions, `"42"`, `7`) {
		t.Fatal("numeric observed key resolved through a string alias")
	}
}
