package kotlin

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestRetentionReconnectBindingsFollowAuthoredWireCompletions(t *testing.T) {
	scenario := loadRetentionReconnectScenario(t)
	steps, err := kotlinScenarioStepMap(scenario, retentionReconnectScenarioID, 9)
	if err != nil {
		t.Fatalf("validate retention-reconnect scenario: %v", err)
	}
	client := Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "retention-reconnect-client-a"}
	if err := validateRetentionReconnectBindings(scenario, steps, client); err != nil {
		t.Fatalf("validate retention-reconnect bindings: %v", err)
	}
}

func TestRetentionReconnectUnsupportedWireDerivesErrorCompletion(t *testing.T) {
	wire := scenarios.WireExpectation{Action: "unsupported", HTTPStatus: 200}
	if got := retentionReconnectNativeCompletion(wire); got != "error" {
		t.Fatalf("unsupported completion = %q, want error", got)
	}
}

func TestRetentionReconnectBindingRejectsTerminalCompletionNotDerivedFromWire(t *testing.T) {
	scenario := loadRetentionReconnectScenario(t)
	for index := range scenario.Steps {
		if scenario.Steps[index].ID != "STEP-RETENTION-RECONNECT-RENEW-001" {
			continue
		}
		binding := *scenario.Steps[index].NativeBinding
		binding.Completion = "error"
		scenario.Steps[index].NativeBinding = &binding
	}
	steps, err := kotlinScenarioStepMap(scenario, retentionReconnectScenarioID, 9)
	if err != nil {
		t.Fatalf("validate mutated retention-reconnect scenario: %v", err)
	}
	client := Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "retention-reconnect-client-a"}
	if err := validateRetentionReconnectBindings(scenario, steps, client); err == nil {
		t.Fatal("retention-reconnect terminal completion passed without its authored wire completion")
	}
}

func TestRetentionReconnectObservedIdentitiesUseWireAndCapturedState(t *testing.T) {
	scenario := loadRetentionReconnectScenario(t)
	commitStep := findRetentionReconnectStep(t, scenario, "commit-source-transaction")
	primaryAliases := make([]scenarios.NativeIdentityAlias, 0)
	for _, alias := range scenario.NativeIdentityAliases {
		if alias.Kind == "primary-key" && containsRetentionReconnectStep(alias.StepIDs, commitStep.ID) {
			primaryAliases = append(primaryAliases, alias)
		}
	}
	if len(primaryAliases) == 0 {
		t.Fatal("retention-reconnect scenario has no committed primary-key aliases")
	}
	rows := make([]scenarios.RowFact, 0, len(primaryAliases))
	for _, alias := range primaryAliases {
		var primary string
		if err := json.Unmarshal(alias.Value, &primary); err != nil {
			t.Fatalf("decode primary-key alias %q: %v", alias.Alias, err)
		}
		encoded, err := json.Marshal(primary)
		if err != nil {
			t.Fatalf("encode primary-key alias %q: %v", alias.Alias, err)
		}
		rows = append(rows, scenarios.RowFact{CanonicalWireJSON: string(encoded), Version: "observed-version", Checksum: strings.Repeat("a", 64)})
	}
	scopeAlias := findRetentionReconnectAlias(t, scenario, "scope")
	var scopeID string
	if err := json.Unmarshal(scopeAlias.Value, &scopeID); err != nil {
		t.Fatalf("decode scope alias: %v", err)
	}
	generation := int64(17)
	scopeSetVersion := int64(23)
	requestFacts := func() *TransportRequestFacts {
		return &TransportRequestFacts{ClientGeneration: &generation, SchemaVersion: 1, SchemaHash: strings.Repeat("b", 64)}
	}
	connectFacts := requestFacts()
	connectFacts.ScopeSetVersion = &scopeSetVersion
	initial := RetentionReconnectCall{Transport: []TransportObservation{{OperationClass: "push", RequestFacts: requestFacts()}}}
	renewal := RetentionReconnectCall{Transport: []TransportObservation{{OperationClass: "push", RequestFacts: requestFacts()}, {OperationClass: "connect", RequestFacts: connectFacts}}}
	server := scenarios.StateFacts{Rows: rows, Rebuilds: []scenarios.RebuildFact{{ScopeID: scopeID, RebuildID: "observed-rebuild"}}}
	runtime, err := retentionReconnectObservedIdentityValues(scenario.NativeIdentityAliases, initial, renewal, server, "observed-batch", []string{"observed-mutation"}, retentionReconnectPrimaryKey{Authored: "unbound-authored", Runtime: "unbound-runtime"}, "observed-rebuild")
	if err != nil {
		t.Fatalf("resolve observed retention-reconnect identities: %v", err)
	}
	var gotGeneration, gotScopeSetVersion int64
	if err := json.Unmarshal(runtime[findRetentionReconnectAlias(t, scenario, "client-generation").Alias], &gotGeneration); err != nil || gotGeneration != generation {
		t.Fatalf("client generation = %d, want %d", gotGeneration, generation)
	}
	if err := json.Unmarshal(runtime[findRetentionReconnectAlias(t, scenario, "scope-set-version").Alias], &gotScopeSetVersion); err != nil || gotScopeSetVersion != scopeSetVersion {
		t.Fatalf("scope-set version = %d, want %d", gotScopeSetVersion, scopeSetVersion)
	}
	if got := string(runtime[findRetentionReconnectAlias(t, scenario, "row-version").Alias]); got != `"observed-version"` {
		t.Fatalf("row version = %s, want observed capture", got)
	}
	if got := string(runtime[findRetentionReconnectAlias(t, scenario, "checksum").Alias]); got != `"`+strings.Repeat("a", 64)+`"` {
		t.Fatalf("checksum = %s, want observed capture", got)
	}
	if got := string(runtime[findRetentionReconnectAlias(t, scenario, "rebuild-id").Alias]); got != `"observed-rebuild"` {
		t.Fatalf("rebuild ID = %s, want observed capture", got)
	}

	rows[1].Checksum = strings.Repeat("c", 64)
	server.Rows = rows
	if _, err := retentionReconnectObservedIdentityValues(scenario.NativeIdentityAliases, initial, renewal, server, "observed-batch", []string{"observed-mutation"}, retentionReconnectPrimaryKey{Authored: "unbound-authored", Runtime: "unbound-runtime"}, "observed-rebuild"); err == nil {
		t.Fatal("retention-reconnect accepted inconsistent captured checksums")
	}
}

func TestRetentionReconnectQueueUsesNormalizedSealedBatchIdentity(t *testing.T) {
	raw := json.RawMessage(`[{"mutation_id":"mutation-a","local_order":0,"table_id":"items","table_name":"items","record_id":"record-a","primary_key_field_id":"id","primary_key_logical_type":"string","operation":"insert","authored_schema":{"version":1,"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"base_version":null,"client_version":"client-version","status":"sealed","sealed_batch_id":"batch-a","authored_fields":[]}]`)
	snapshot := Result{MutationLedgerCount: intPointer(1), RetainedMutations: raw}
	batchID, mutationIDs, err := retentionReconnectQueueIdentityEvidence(snapshot, 1)
	if err != nil {
		t.Fatalf("read normalized sealed queue identity: %v", err)
	}
	if batchID != "batch-a" || len(mutationIDs) != 1 || mutationIDs[0] != "mutation-a" {
		t.Fatalf("sealed queue identity = %q/%v, want batch-a/mutation-a", batchID, mutationIDs)
	}
	identities, err := retentionReconnectSealedIdentities(mutationIDs, 1)
	if err != nil {
		t.Fatalf("read sealed mutation identities: %v", err)
	}
	if _, found := identities["mutation-a"]; !found {
		t.Fatal("sealed mutation identity is absent")
	}
}

func TestRetentionReconnectQueueRejectsChangedSealedMutationIdentity(t *testing.T) {
	raw := json.RawMessage(`[{"mutation_id":"mutation-b","local_order":0,"table_id":"items","table_name":"items","record_id":"record-a","primary_key_field_id":"id","primary_key_logical_type":"string","operation":"insert","authored_schema":{"version":1,"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"base_version":null,"client_version":"client-version","status":"sealed","sealed_batch_id":"batch-a","authored_fields":[]}]`)
	snapshot := Result{MutationLedgerCount: intPointer(1), RetainedMutations: raw}
	identities, err := retentionReconnectSealedIdentities([]string{"mutation-a"}, 1)
	if err != nil {
		t.Fatalf("read expected sealed mutation identities: %v", err)
	}
	if err := validateRetentionReconnectQueue(snapshot, identities); err == nil {
		t.Fatal("retention-reconnect accepted changed sealed mutation identity")
	}
}

func findRetentionReconnectStep(t *testing.T, scenario scenarios.Scenario, name string) scenarios.Step {
	t.Helper()
	for _, step := range scenario.Steps {
		if step.Operation.Name == name {
			return step
		}
	}
	t.Fatalf("retention-reconnect step %q is absent", name)
	return scenarios.Step{}
}

func findRetentionReconnectAlias(t *testing.T, scenario scenarios.Scenario, kind string) scenarios.NativeIdentityAlias {
	t.Helper()
	for _, alias := range scenario.NativeIdentityAliases {
		if alias.Kind == kind {
			return alias
		}
	}
	t.Fatalf("retention-reconnect alias kind %q is absent", kind)
	return scenarios.NativeIdentityAlias{}
}

func containsRetentionReconnectStep(ids []scenarios.StepID, wanted scenarios.StepID) bool {
	for _, id := range ids {
		if id == wanted {
			return true
		}
	}
	return false
}

func loadRetentionReconnectScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	root := filepath.Join("..", "..")
	scenario, err := scenarios.LoadFile(context.Background(), root, "conformance/scenarios/server/retention-reconnect-001.json")
	if err != nil {
		t.Fatalf("load retention-reconnect scenario: %v", err)
	}
	return scenario
}

func intPointer(value int) *int {
	return &value
}
