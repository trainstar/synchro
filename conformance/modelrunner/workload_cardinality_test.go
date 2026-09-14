package modelrunner

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/trainstar/synchro/conformance/reference"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestScopeCardinalityExpansionUsesEveryImmutablePage(t *testing.T) {
	tests := []struct {
		name           string
		current        uint64
		target         uint64
		wantPages      uint64
		wantMembership bool
	}{
		{name: "small", current: 0, target: 1, wantPages: 1},
		{name: "medium", current: 1, target: 101, wantPages: 2, wantMembership: true},
		{name: "large", current: 101, target: 1000, wantPages: 10, wantMembership: true},
		{name: "repeat", current: 1000, target: 1000, wantPages: 10},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := cardinalityTestSnapshot(t, test.current)
			before := snapshot
			payload, err := json.Marshal(map[string]any{
				"profile":      "scope_cardinality",
				"scope_id":     "scope-a",
				"record_count": test.target,
				"page_size":    100,
			})
			if err != nil {
				t.Fatal(err)
			}
			var decoded map[string]json.RawMessage
			if err := json.Unmarshal(payload, &decoded); err != nil {
				t.Fatal(err)
			}
			operations, err := expandScopeCardinalityWorkload(snapshot, decoded)
			if err != nil {
				t.Fatalf("expand scope cardinality: %v", err)
			}
			if !reflect.DeepEqual(snapshot, before) {
				t.Fatal("scope cardinality expansion mutated the reference snapshot")
			}

			for index, operation := range operations {
				if err := scenarios.ValidateOperation(operation); err != nil {
					t.Fatalf("operation %d %s is not closed: %v", index, scenarios.OperationKey(operation), err)
				}
				key := scenarios.OperationKey(operation)
				if key == "workload/prepare" || key == "process/restart-wal-worker" || key == "process/restart-client" {
					t.Fatalf("operation %d is not substantive: %s", index, key)
				}
			}
			if operationKeyCount(operations, "model/commit-source-transaction") != 1 || operationKeyCount(operations, "process/materialize-source-transaction") != 1 {
				t.Fatalf("source and WAL materialization operations = %#v", operationKeys(operations))
			}
			if operationKeyCount(operations, "local/begin-rebuild") != 1 || operationKeyCount(operations, "local/finalize-rebuild") != 1 {
				t.Fatalf("local rebuild lifecycle operations = %#v", operationKeys(operations))
			}
			if got := operationKeyCount(operations, "rebuild/request-page"); got != int(test.wantPages) {
				t.Fatalf("rebuild page request count = %d, want %d", got, test.wantPages)
			}
			if got := operationKeyCount(operations, "local/apply-rebuild-page"); got != int(test.wantPages) {
				t.Fatalf("local rebuild page apply count = %d, want %d", got, test.wantPages)
			}
			if test.wantMembership {
				if operationKeyCount(operations, "model/stage-registry-membership-generation") != 1 || operationKeyCount(operations, "model/activate-registry-membership-generation") != 1 {
					t.Fatalf("membership operations = %#v", operationKeys(operations))
				}
			} else if operationKeyCount(operations, "model/stage-registry-membership-generation") != 0 || operationKeyCount(operations, "model/activate-registry-membership-generation") != 0 {
				t.Fatalf("unexpected membership operations = %#v", operationKeys(operations))
			}

			wantOrdinals := make([]uint64, 0, test.wantPages)
			for pageIndex := uint64(0); pageIndex < test.wantPages; pageIndex++ {
				wantOrdinals = append(wantOrdinals, pageIndex*cardinalityPageSize+1)
			}
			if got := pageOrdinals(t, operations, "local/apply-rebuild-page", "request_token_source"); !reflect.DeepEqual(got, wantOrdinals) {
				t.Fatalf("local applied page ordinals = %v, want %v", got, wantOrdinals)
			}
			gotSources := rebuildCursorSources(t, operations)
			if len(gotSources) != int(test.wantPages) || gotSources[0] != "none" {
				t.Fatalf("rebuild cursor sources = %v, want first page without a token", gotSources)
			}
			for _, source := range gotSources[1:] {
				if source != "local_rebuild_continuation" {
					t.Fatalf("rebuild continuation source = %q, want local_rebuild_continuation", source)
				}
			}
		})
	}
}

func TestScopeCardinalityExpansionRejectsUnclosedSamples(t *testing.T) {
	snapshot := cardinalityTestSnapshot(t, 1)
	payload := map[string]json.RawMessage{
		"profile":      json.RawMessage(`"scope_cardinality"`),
		"scope_id":     json.RawMessage(`"scope-a"`),
		"record_count": json.RawMessage(`2`),
		"page_size":    json.RawMessage(`100`),
	}
	if _, err := expandScopeCardinalityWorkload(snapshot, payload); err == nil {
		t.Fatal("unclosed scope cardinality sample was accepted")
	}
}

func TestScopeCardinalityExpansionExecutesOwnedScenarios(t *testing.T) {
	ctx := context.Background()
	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	repoRoot := filepath.Clean(filepath.Join(workingDirectory, "../.."))
	paths := []string{
		"conformance/scenarios/performance/rebuild-cardinality-001.json",
		"conformance/scenarios/performance/rebuild-apply-001.json",
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			scenario, err := scenarios.LoadFile(ctx, repoRoot, path)
			if err != nil {
				t.Fatalf("load scenario: %v", err)
			}
			model, err := NewModel(seedForScenario(scenario))
			if err != nil {
				t.Fatalf("new model: %v", err)
			}
			if _, err := model.Apply(ctx, scenario.Model.Setup[0]); err != nil {
				t.Fatalf("apply setup: %v", err)
			}
			for _, step := range scenario.Steps {
				var payload map[string]json.RawMessage
				if err := json.Unmarshal(step.Operation.Payload, &payload); err != nil {
					t.Fatalf("decode %s payload: %v", step.ID, err)
				}
				operations, err := expandWorkloadForBinding(model.Snapshot(), step.Operation, step.NativeBinding)
				if err != nil {
					t.Fatalf("expand %s: %v", step.ID, err)
				}
				for index, operation := range operations.Operations {
					if _, err := model.Apply(ctx, operation); err != nil {
						if scenarios.OperationKey(operation) == "local/apply-rebuild-page" {
							snapshot := model.Snapshot()
							for _, entry := range snapshot.Rebuilds {
								t.Logf("server rebuild after failure: pages=%d next=%d continuation=%t", len(entry.Value.Pages), entry.Value.NextRowOrdinal, entry.Value.HasContinuation)
							}
							for _, entry := range snapshot.ClientLocal {
								for _, attempt := range entry.Value.RebuildAttempts {
									t.Logf("local attempt after failure: page_count=%d continuation=%t phase=%s", len(attempt.AppliedPages), attempt.HasContinuation, attempt.Phase)
								}
							}
						}
						t.Fatalf("apply %s operation %d %s payload %s: %v", step.ID, index, scenarios.OperationKey(operation), operation.Payload, err)
					}
				}
			}
			snapshot := model.Snapshot()
			scope, found := cardinalityScopeState(snapshot, cardinalityScope)
			if !found || scope.Cardinality != 1000 {
				t.Fatalf("final scope cardinality = %d, want 1000", scope.Cardinality)
			}
			if len(snapshot.ClientLocal) != len(scenario.Steps) {
				t.Fatalf("local client count = %d, want %d", len(snapshot.ClientLocal), len(scenario.Steps))
			}
			for _, step := range scenario.Steps {
				binding := step.NativeBinding
				if binding == nil || binding.Workload == nil {
					t.Fatalf("step %s has no workload binding", step.ID)
				}
				client := reference.ClientKey{UserID: reference.UserID(binding.UserID), ClientID: reference.ClientID(binding.ClientID)}
				var local reference.ClientLocalState
				found := false
				for _, entry := range snapshot.ClientLocal {
					if entry.Key == client {
						local = entry.Value
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("local client %q is absent", binding.ClientID)
				}
				want := int(binding.Workload.RecordCount)
				if len(local.Rows) != want || len(local.Provenance) != want || len(local.ScopeCheckpoints) != 1 || len(local.RebuildAttempts) != 1 {
					t.Fatalf("local client %q facts = rows:%d provenance:%d checkpoints:%d attempts:%d, want %d:%d:1:1", binding.ClientID, len(local.Rows), len(local.Provenance), len(local.ScopeCheckpoints), len(local.RebuildAttempts), want, want)
				}
				if local.RebuildAttempts[0].Phase != reference.LocalRebuildAttemptPhaseCompleted {
					t.Fatalf("local client %q rebuild phase = %q, want completed", binding.ClientID, local.RebuildAttempts[0].Phase)
				}
			}
		})
	}
}

func cardinalityTestSnapshot(t *testing.T, current uint64) reference.StateSnapshot {
	t.Helper()
	model := installedWorkloadModel(t, "conformance/scenarios/performance/rebuild-cardinality-001.json")
	snapshot := model.Snapshot()
	if current == 0 {
		return snapshot
	}
	relation, err := cardinalityRelationInfo(snapshot)
	if err != nil {
		t.Fatalf("resolve cardinality relation: %v", err)
	}
	stream := snapshot.Stream.Authority.ActiveGeneration
	boundary := reference.StreamPosition{StreamGeneration: stream, Kind: reference.PositionKindTransactionEnd, CommitLSN: 10}
	rows := make([]reference.SnapshotEntry[reference.RowIdentity, reference.AuthoritativeRow], 0, current)
	evaluations := make([]reference.MembershipEvaluation, 0, current)
	memberships := make([]reference.ScopeMembership, 0, current)
	for ordinal := uint64(1); ordinal <= current; ordinal++ {
		row, err := cardinalityNewRow(relation, ordinal, 10)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, reference.SnapshotEntry[reference.RowIdentity, reference.AuthoritativeRow]{Key: row.Identity, Value: row})
		evaluations = append(evaluations, reference.MembershipEvaluation{Row: row.Identity, Scopes: []reference.ScopeID{cardinalityScope}})
		memberships = append(memberships, reference.ScopeMembership{Row: row.Identity, Generation: 1, Included: true})
	}
	snapshot.Rows = rows
	for index := range snapshot.Registry.Generations {
		if snapshot.Registry.Generations[index].Generation != snapshot.Registry.CurrentGeneration {
			continue
		}
		snapshot.Registry.Generations[index].ActivationBoundary = boundary
		snapshot.Registry.Generations[index].ScopeRules[0].Evaluations = evaluations
	}
	for index := range snapshot.Scopes {
		if snapshot.Scopes[index].Key != cardinalityScope {
			continue
		}
		snapshot.Scopes[index].Value.Membership = memberships
		snapshot.Scopes[index].Value.Cardinality = reference.Cardinality(current)
		snapshot.Scopes[index].Value.HighWatermark = boundary
	}
	snapshot.Stream.Authority.GlobalMaterializationBoundary = boundary
	snapshot.Stream.Transactions = cardinalityTestTransactions(stream, current)
	return snapshot
}

func cardinalityTestTransactions(stream reference.StreamGeneration, current uint64) []reference.StreamTransaction {
	if current == 0 {
		return nil
	}
	return []reference.StreamTransaction{{
		ReplayKey:          reference.TransactionReplayKey{StreamGeneration: stream, CommitLSN: 10},
		End:                reference.StreamPosition{StreamGeneration: stream, Kind: reference.PositionKindTransactionEnd, CommitLSN: 10},
		EndLSN:             11,
		RegistryGeneration: 1,
		Lifecycle:          reference.TransactionLifecycleMaterialized,
	}}
}

func operationKeyCount(operations []scenarios.Operation, key string) int {
	count := 0
	for _, operation := range operations {
		if scenarios.OperationKey(operation) == key {
			count++
		}
	}
	return count
}

func operationKeys(operations []scenarios.Operation) []string {
	keys := make([]string, 0, len(operations))
	for _, operation := range operations {
		keys = append(keys, scenarios.OperationKey(operation))
	}
	return keys
}

func pageOrdinals(t *testing.T, operations []scenarios.Operation, key, sourceField string) []uint64 {
	t.Helper()
	ordinals := make([]uint64, 0)
	for _, operation := range operations {
		if scenarios.OperationKey(operation) != key {
			continue
		}
		var payload struct {
			PageOrdinal        uint64 `json:"page_ordinal"`
			CursorSource       string `json:"cursor_source"`
			RequestTokenSource string `json:"request_token_source"`
		}
		if err := json.Unmarshal(operation.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if sourceField == "cursor_source" && payload.CursorSource == "" || sourceField == "request_token_source" && payload.RequestTokenSource == "" {
			t.Fatalf("%s page %d has no %s", key, payload.PageOrdinal, sourceField)
		}
		ordinals = append(ordinals, payload.PageOrdinal)
	}
	return ordinals
}

func rebuildCursorSources(t *testing.T, operations []scenarios.Operation) []string {
	t.Helper()
	sources := make([]string, 0)
	for _, operation := range operations {
		if scenarios.OperationKey(operation) != "rebuild/request-page" {
			continue
		}
		var payload struct {
			CursorSource string `json:"cursor_source"`
		}
		if err := json.Unmarshal(operation.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		sources = append(sources, payload.CursorSource)
	}
	return sources
}
