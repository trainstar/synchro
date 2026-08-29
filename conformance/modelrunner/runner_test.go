package modelrunner

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/trainstar/synchro/conformance/reference"
	"github.com/trainstar/synchro/conformance/scenarios"
)

type codedTestError struct{ code string }

func (e codedTestError) Error() string     { return "redacted" }
func (e codedTestError) ErrorCode() string { return e.code }

func TestWorkloadMacroExpansionNeverDispatchesMacro(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{"scope_topology", "conformance/scenarios/performance/fanout-001.json"},
		{"scope_cardinality", "conformance/scenarios/performance/rebuild-cardinality-001.json"},
		{"pending_mutations", "conformance/scenarios/performance/queue-replay-001.json"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := loadWorkloadScenario(t, test.path)
			model := installedWorkloadModel(t, test.path)
			plan, err := expandWorkload(model.Snapshot(), scenario.Steps[0].Operation)
			if err != nil {
				t.Fatalf("expand workload: %v", err)
			}
			operations := plan.Operations
			if len(operations) == 0 {
				t.Fatal("workload expanded to no operations")
			}
			if len(plan.Samples) != 0 {
				t.Fatal("general workload unexpectedly produced configured sample records")
			}
			for _, operation := range operations {
				if scenarios.OperationKey(operation) == "workload/prepare" {
					t.Fatal("macro was returned as a dispatch operation")
				}
				if err := scenarios.ValidateOperation(operation); err != nil {
					t.Fatalf("expanded operation is not typed and closed: %v", err)
				}
			}
		})
	}
}

func TestWarmConnectClientInventedRequestScopeKeepsTraceGreenButFails(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/warm-connect-001.json")
	mutant := result
	execution := cloneSemanticStep(result.Steps[10])
	var request map[string]json.RawMessage
	if err := json.Unmarshal(execution.Operation.Payload, &request); err != nil {
		t.Fatalf("decode connect payload: %v", err)
	}
	request["known_scopes"] = json.RawMessage(`[{"scope_id":"client-invented-scope"}]`)
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("encode connect mutant: %v", err)
	}
	execution.Operation.Payload = payload
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	mutant.Steps[10] = execution
	assertSemanticMutantFails(t, result, mutant)
}

func TestWarmConnectSemanticComponents(t *testing.T) {
	scenario, err := scenarios.LoadFile(context.Background(), "../..", "conformance/scenarios/performance/warm-connect-001.json")
	if err != nil {
		t.Fatalf("load warm scenario: %v", err)
	}
	model, err := NewModel(seedForScenario(scenario))
	if err != nil {
		t.Fatalf("create warm model: %v", err)
	}
	result, err := run(context.Background(), model, scenario, false)
	if err != nil {
		t.Fatalf("run warm scenario without predicate evaluation: %v", err)
	}
	steps, ok := exactSemanticSteps(result,
		"model/set-client-assignments", "connect/send", "rebuild/request-page", "local/begin-rebuild",
		"local/apply-rebuild-page", "local/finalize-rebuild", "pull/request-page", "local/apply-pull-page",
		"model/commit-source-transaction", "process/materialize-source-transaction", "connect/send",
		"pull/request-page", "local/apply-pull-page",
	)
	if !ok {
		t.Fatal("warm semantic trace shape is invalid")
	}
	if !freshRebuildAssignmentSatisfied(steps[0]) {
		t.Fatal("warm bootstrap assignment is invalid")
	}
	if !freshRebuildConnectSatisfied(steps[1]) {
		t.Fatal("warm bootstrap connect is invalid")
	}
	baseline, ok := warmPullBaselineSatisfied(steps[2], steps[3], steps[4], steps[5])
	if !ok {
		var request semanticRebuildRequest
		_ = json.Unmarshal(steps[2].Operation.Payload, &request)
		client := reference.ClientKey{UserID: reference.UserID(request.UserID), ClientID: reference.ClientID(request.ClientID)}
		before, _ := snapshotLocalClient(steps[3].Before.ClientLocal, client)
		after, _ := snapshotLocalClient(steps[5].After.ClientLocal, client)
		checkpoint, _ := localCheckpoint(after, reference.ScopeID(request.ScopeID))
		localAssignment, _ := localAssignment(after, reference.ScopeID(request.ScopeID))
		server, _ := snapshotClient(steps[5].After.Clients, client)
		serverScope, _ := serverAssignment(server, reference.ScopeID(request.ScopeID))
		t.Fatalf("warm bootstrap rebuild is invalid: records=%d before_rows=%d before_checkpoints=%d after_rows=%d after_checkpoints=%d attempts=%d checkpoint_position=%+v expected_position=%+v local_assignment=%+v server_assignment=%+v", len(steps[2].Result.Rebuild.Records), len(before.Rows), len(before.ScopeCheckpoints), len(after.Rows), len(after.ScopeCheckpoints), len(after.RebuildAttempts), checkpoint.Position, expectedRebuildBoundary(steps[2].Before), localAssignment, serverScope)
	}
	if !warmBaselineAcknowledgementSatisfied(steps[6], steps[7], baseline) {
		t.Fatal("warm bootstrap acknowledgement is invalid")
	}
	if !steadyPullMaterializationSatisfied(steps[8], steps[9], baseline.Scope) {
		t.Fatal("warm source materialization is invalid")
	}
	if !warmConnectExecutionSatisfied(steps[10]) {
		t.Fatal("measured warm connect is invalid")
	}
	if !steadyTerminalPullSatisfied(steps[11], baseline) {
		t.Fatal("measured warm pull is invalid")
	}
	if !steadyTerminalApplySatisfied(steps[12], steps[11], baseline) {
		t.Fatal("measured warm apply is invalid")
	}
}

func TestWarmConnectUnexpectedResponseBodyKeepsTraceGreenButFails(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/warm-connect-001.json")
	mutant := result
	execution := cloneSemanticStep(result.Steps[10])
	execution.Result.HTTP.Body = []byte(`{"invented":"body"}`)
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	mutant.Steps[10] = execution
	assertSemanticMutantFails(t, result, mutant)
}

func TestWarmConnectAssignmentLineageOutsideScopeStateKeepsTraceGreenButFails(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/warm-connect-001.json")
	mutant := result
	execution := cloneSemanticStep(result.Steps[10])
	client := execution.Result.Connect.Client
	execution.Before = mutateSemanticAssignments(execution.Before, client, 2, true)
	execution.After = mutateSemanticAssignments(execution.After, client, 2, true)
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	mutant.Steps[10] = execution
	mutant.FinalSnapshot = execution.After
	assertSemanticMutantFails(t, result, mutant)
}

func TestWarmConnectLocalServerAssignmentMismatchKeepsTraceGreenButFails(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/warm-connect-001.json")
	mutant := result
	execution := cloneSemanticStep(result.Steps[10])
	client := execution.Result.Connect.Client
	execution.After = mutateSemanticLocalAssignment(execution.After, client, 2)
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	mutant.Steps[10] = execution
	mutant.FinalSnapshot = execution.After
	assertSemanticMutantFails(t, result, mutant)
}

func TestWarmConnectMissingMeasuredPullFails(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/warm-connect-001.json")
	mutant := result
	mutant.Steps = append([]OperationExecution(nil), result.Steps[:11]...)
	mutant.FinalSnapshot = mutant.Steps[len(mutant.Steps)-1].After
	if performanceContractSatisfied(mutant.ScenarioID, mutant) {
		t.Fatal("warm startup without its measured pull satisfied the protected predicate")
	}
}

func TestWarmConnectExtraMeasuredPullFails(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/warm-connect-001.json")
	mutant := result
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	mutant.Steps = append(mutant.Steps, cloneSemanticStep(result.Steps[11]))
	if performanceContractSatisfied(mutant.ScenarioID, mutant) {
		t.Fatal("warm startup with a second measured pull satisfied the protected predicate")
	}
}

func TestWarmConnectForbiddenMeasuredRequestFails(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/warm-connect-001.json")
	mutant := result
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	mutant.Steps[11] = cloneSemanticStep(mutant.Steps[11])
	mutant.Steps[11].OperationKey = "push/submit"
	if performanceContractSatisfied(mutant.ScenarioID, mutant) {
		t.Fatal("warm startup with a forbidden measured request satisfied the protected predicate")
	}
}

func TestWarmConnectWrongFinalStateFails(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/warm-connect-001.json")
	mutant := result
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	apply := cloneSemanticStep(mutant.Steps[12])
	apply.After = forgeSemanticLocalFields(apply.After, reference.ClientKey{UserID: "user-a", ClientID: "client-a"}, `"wrong"`)
	mutant.Steps[12] = apply
	mutant.FinalSnapshot = apply.After
	assertSemanticMutantFails(t, result, mutant)
}

func TestSteadyPullSemanticMutantKeepsTraceGreenButFails(t *testing.T) {
	const path = "conformance/scenarios/performance/steady-pull-001.json"
	result := runSemanticPerformanceScenario(t, path)
	mutant := result
	execution := cloneSemanticStep(result.Steps[7])
	localEntries := append([]reference.SnapshotEntry[reference.ClientKey, reference.ClientLocalState](nil), execution.After.ClientLocal...)
	local := localEntries[0].Value
	local.ScopeCheckpoints = append([]reference.LocalScopeCheckpoint(nil), local.ScopeCheckpoints...)
	local.ScopeCheckpoints[0].Verified = false
	localEntries[0].Value = local
	execution.After.ClientLocal = localEntries
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	mutant.Steps[7] = execution
	mutant.FinalSnapshot = execution.After
	scenario := loadWorkloadScenario(t, path)
	facts := scenario.Model.ExpectedState[0].StateFacts
	if facts == nil || stateFactsFailure(*facts, mutant.FinalSnapshot) == "" {
		t.Fatal("authored state facts accepted installed terminal progress without verification")
	}
	assertSemanticMutantFails(t, result, mutant)
}

func TestSteadyPullWrongVerifiedChecksumFailsAuthoredFacts(t *testing.T) {
	const path = "conformance/scenarios/performance/steady-pull-001.json"
	result := runSemanticPerformanceScenario(t, path)
	mutant := result
	execution := cloneSemanticStep(result.Steps[7])
	localEntries := append([]reference.SnapshotEntry[reference.ClientKey, reference.ClientLocalState](nil), execution.After.ClientLocal...)
	local := localEntries[0].Value
	local.ScopeCheckpoints = append([]reference.LocalScopeCheckpoint(nil), local.ScopeCheckpoints...)
	local.ScopeCheckpoints[0].Checksum[0] ^= 0xff
	localEntries[0].Value = local
	execution.After.ClientLocal = localEntries
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	mutant.Steps[7] = execution
	mutant.FinalSnapshot = execution.After
	scenario := loadWorkloadScenario(t, path)
	facts := scenario.Model.ExpectedState[0].StateFacts
	if facts == nil || stateFactsFailure(*facts, mutant.FinalSnapshot) == "" {
		t.Fatal("authored state facts accepted a verified checkpoint with the wrong checksum")
	}
	assertSemanticMutantFails(t, result, mutant)
}

func TestSteadyPullSemanticFieldMutantKeepsTraceGreenButFails(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/steady-pull-001.json")
	mutant := result
	execution := cloneSemanticStep(result.Steps[7])
	localEntries := append([]reference.SnapshotEntry[reference.ClientKey, reference.ClientLocalState](nil), execution.After.ClientLocal...)
	local := localEntries[0].Value
	local.Rows = append([]reference.LocalRow(nil), local.Rows...)
	local.Rows[0].Fields = append([]reference.FieldValue(nil), local.Rows[0].Fields...)
	local.Rows[0].Fields[1].WireJSON = `"forged-local-value"`
	localEntries[0].Value = local
	execution.After.ClientLocal = localEntries
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	mutant.Steps[7] = execution
	mutant.FinalSnapshot = execution.After
	assertSemanticMutantFails(t, result, mutant)
}

func TestSteadyPullZeroTerminalCursorKeepsTraceGreenButFails(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/steady-pull-001.json")
	mutant := result
	pull := cloneSemanticStep(result.Steps[6])
	pull.After = mutateSemanticServerCheckpoint(pull.After, reference.ClientKey{UserID: "user-a", ClientID: "client-a"}, func(checkpoint *reference.ClientCheckpoint) {
		checkpoint.Cursor = reference.OpaqueToken{}
	})
	apply := cloneSemanticStep(result.Steps[7])
	apply.Before = pull.After
	apply.After = mutateSemanticServerCheckpoint(apply.After, reference.ClientKey{UserID: "user-a", ClientID: "client-a"}, func(checkpoint *reference.ClientCheckpoint) {
		checkpoint.Cursor = reference.OpaqueToken{}
	})
	apply.After = mutateSemanticLocalCheckpoint(apply.After, reference.ClientKey{UserID: "user-a", ClientID: "client-a"}, func(checkpoint *reference.LocalScopeCheckpoint) {
		checkpoint.Cursor = reference.OpaqueToken{}
	})
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	mutant.Steps[6] = pull
	mutant.Steps[7] = apply
	mutant.FinalSnapshot = apply.After
	assertSemanticMutantFails(t, result, mutant)
}

func TestSteadyPullAtomicApplyAssignmentMutationKeepsTraceGreenButFails(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/steady-pull-001.json")
	mutant := result
	apply := cloneSemanticStep(result.Steps[7])
	client := reference.ClientKey{UserID: "user-a", ClientID: "client-a"}
	apply.After = mutateSemanticAssignments(apply.After, client, 2, true)
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	mutant.Steps[7] = apply
	mutant.FinalSnapshot = apply.After
	assertSemanticMutantFails(t, result, mutant)
}

func TestSteadyPullForgedAuthoritativeFieldsRetainChecksumAndFail(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/steady-pull-001.json")
	mutant := result
	materialize := cloneSemanticStep(result.Steps[5])
	materialize.After = forgeSemanticAuthoritativeFields(materialize.After, `"forged-steady-value"`)
	pull := cloneSemanticStep(result.Steps[6])
	pull.Before = materialize.After
	pull.After = forgeSemanticAuthoritativeFields(pull.After, `"forged-steady-value"`)
	apply := cloneSemanticStep(result.Steps[7])
	apply.Before = pull.After
	apply.After = forgeSemanticAuthoritativeFields(apply.After, `"forged-steady-value"`)
	apply.After = forgeSemanticLocalFields(apply.After, reference.ClientKey{UserID: "user-a", ClientID: "client-a"}, `"forged-steady-value"`)
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	mutant.Steps[5] = materialize
	mutant.Steps[6] = pull
	mutant.Steps[7] = apply
	mutant.FinalSnapshot = apply.After
	assertSemanticMutantFails(t, result, mutant)
}

func TestPendingCycleDisconnectedSnapshotsKeepTraceGreenButFail(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/pending-cycle-001.json")
	mutant := result
	materialize := cloneSemanticStep(result.Steps[2])
	materialize.Before = result.Steps[1].Before
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	mutant.Steps[2] = materialize
	assertSemanticMutantFails(t, result, mutant)
}

func TestPendingCycleUnrelatedMaterializationKeepsTraceGreenButFails(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/pending-cycle-001.json")
	mutant := result
	materialize := cloneSemanticStep(result.Steps[2])
	materialize.Operation.Payload = json.RawMessage(`{"stream_generation":"stream-1","commit_lsn":"999"}`)
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	mutant.Steps[2] = materialize
	assertSemanticMutantFails(t, result, mutant)
}

func TestPendingCycleConflictToAppliedDiscontinuityKeepsTraceGreenButFails(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/pending-cycle-001.json")
	mutant := result
	push := cloneSemanticStep(result.Steps[1])
	push.Result.Push.Mutations[0].State = reference.MutationOutcomeConflict
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	mutant.Steps[1] = push
	assertSemanticMutantFails(t, result, mutant)
}

func TestPendingCycleForgedFieldsRetainChecksumAndFail(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/pending-cycle-001.json")
	mutant := result
	push := cloneSemanticStep(result.Steps[1])
	push.After = forgePendingSemanticFields(push.After, `"forged-pending-value"`)
	materialize := cloneSemanticStep(result.Steps[2])
	materialize.Before = push.After
	materialize.After = forgePendingSemanticFields(materialize.After, `"forged-pending-value"`)
	pull := cloneSemanticStep(result.Steps[3])
	pull.Before = materialize.After
	pull.After = materialize.After
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	mutant.Steps[1] = push
	mutant.Steps[2] = materialize
	mutant.Steps[3] = pull
	mutant.FinalSnapshot = pull.After
	assertSemanticMutantFails(t, result, mutant)
}

func TestPendingCycleForgedStoredAndReturnedScopeChecksumFails(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/pending-cycle-001.json")
	mutant := result
	materialize := cloneSemanticStep(result.Steps[2])
	materialize.After.Scopes = append([]reference.SnapshotEntry[reference.ScopeID, reference.ScopeState](nil), materialize.After.Scopes...)
	var forged reference.Checksum
	for index := range materialize.After.Scopes {
		if materialize.After.Scopes[index].Key == "scope-a" {
			state := materialize.After.Scopes[index].Value
			state.Checksum[0] ^= 0xff
			forged = state.Checksum
			materialize.After.Scopes[index].Value = state
		}
	}
	if forged == (reference.Checksum{}) {
		t.Fatal("scope checksum mutant did not find scope A")
	}
	pull := cloneSemanticStep(result.Steps[3])
	pull.Before = materialize.After
	pull.After = materialize.After
	pull.Result.Pull.ScopeChecksums = append([]reference.ScopeChecksumObservation(nil), pull.Result.Pull.ScopeChecksums...)
	pull.Result.Pull.ScopeChecksums[0].Checksum = forged
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	mutant.Steps[2] = materialize
	mutant.Steps[3] = pull
	mutant.FinalSnapshot = pull.After
	assertSemanticMutantFails(t, result, mutant)
}

func TestIndependentRowChecksumRejectsForgedCanonicalIdentity(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/steady-pull-001.json")
	snapshot := result.Steps[5].After
	row := snapshot.Rows[0].Value
	row.Identity.CanonicalIdentityBytes = "forged-row-identity"
	if independentRowChecksumMatches(snapshot, row) {
		t.Fatal("row checksum accepted forged canonical identity bytes")
	}
}

func TestRebuildRequestsWrongSnapshotBoundaryKeepsTraceGreenButFails(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/rebuild-requests-001.json")
	mutant := result
	execution := cloneSemanticStep(result.Steps[8])
	rebuilds := append([]reference.SnapshotEntry[reference.RebuildKey, reference.RebuildSession](nil), execution.After.Rebuilds...)
	session := rebuilds[0].Value
	session.SnapshotBoundary = reference.StreamPosition{
		StreamGeneration: "stream-1",
		Kind:             reference.PositionKindTransactionEnd,
		CommitLSN:        99,
	}
	rebuilds[0].Value = session
	execution.After.Rebuilds = rebuilds
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	mutant.Steps[8] = execution
	mutant.FinalSnapshot = execution.After
	assertSemanticMutantFails(t, result, mutant)
}

func TestRebuildRequestsNumericFreshGenerationKeepsTraceGreenButFails(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/rebuild-requests-001.json")
	mutant := result
	execution := cloneSemanticStep(result.Steps[3])
	var payload map[string]any
	if err := json.Unmarshal(execution.Operation.Payload, &payload); err != nil {
		t.Fatalf("decode authored connect payload: %v", err)
	}
	payload["client_generation"] = float64(0)
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("encode authored connect payload: %v", err)
	}
	execution.Operation.Payload = encoded
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	mutant.Steps[3] = execution
	assertSemanticMutantFails(t, result, mutant)
}

func TestRebuildRequestsReplayedFirstPageKeepsTraceGreenButFails(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/rebuild-requests-001.json")
	mutant := result
	execution := cloneSemanticStep(result.Steps[5])
	execution.Result.Rebuild.Replayed = true
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	mutant.Steps[5] = execution
	assertSemanticMutantFails(t, result, mutant)
}

func TestRebuildRequestsFinalCursorMismatchKeepsTraceGreenButFails(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/rebuild-requests-001.json")
	mutant := result
	execution := cloneSemanticStep(result.Steps[9])
	execution.Result.Rebuild.FinalCursor = result.Steps[5].Result.Rebuild.Continuation
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	mutant.Steps[9] = execution
	assertSemanticMutantFails(t, result, mutant)
}

func TestRebuildRequestsLiveConcurrentRowLeakKeepsTraceGreenButFails(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/rebuild-requests-001.json")
	mutant := result
	execution := cloneSemanticStep(result.Steps[9])
	var concurrent reference.AuthoritativeRow
	for _, entry := range execution.After.Rows {
		if entry.Value.Identity.CanonicalWireJSON == `"row-c"` {
			concurrent = entry.Value
			break
		}
	}
	if concurrent.Identity.CanonicalIdentityBytes == "" {
		t.Fatal("concurrent row is absent")
	}
	execution.Result.Rebuild.Records = append(execution.Result.Rebuild.Records, reference.RebuildRecordObservation{
		Row: concurrent.Identity, Version: concurrent.Version, Deleted: concurrent.Deleted, HasChecksum: true, Checksum: concurrent.Checksum,
	})
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	mutant.Steps[9] = execution
	assertSemanticMutantFails(t, result, mutant)
}

func TestRebuildRequestsMissingPostBoundaryPullRowKeepsTraceGreenButFails(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/rebuild-requests-001.json")
	mutant := result
	execution := cloneSemanticStep(result.Steps[12])
	execution.Result.Pull.Changes = nil
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	mutant.Steps[12] = execution
	assertSemanticMutantFails(t, result, mutant)
}

func TestRebuildRequestsForgedPullChecksumKeepsTraceGreenButFails(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/rebuild-requests-001.json")
	mutant := result
	execution := cloneSemanticStep(result.Steps[12])
	execution.Result.Pull.ScopeChecksums = append([]reference.ScopeChecksumObservation(nil), execution.Result.Pull.ScopeChecksums...)
	if len(execution.Result.Pull.ScopeChecksums) != 1 {
		t.Fatal("terminal rebuild pull checksum is absent")
	}
	execution.Result.Pull.ScopeChecksums[0].Checksum[0] ^= 0xff
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	mutant.Steps[12] = execution
	assertSemanticMutantFails(t, result, mutant)
}

func TestRebuildRequestsUnacknowledgedAssignmentKeepsTraceGreenButFails(t *testing.T) {
	result := runSemanticPerformanceScenario(t, "conformance/scenarios/performance/rebuild-requests-001.json")
	mutant := result
	execution := cloneSemanticStep(result.Steps[12])
	clients := append([]reference.SnapshotEntry[reference.ClientKey, reference.ClientState](nil), execution.After.Clients...)
	found := false
	for index := range clients {
		if clients[index].Key.UserID != "user-a" || clients[index].Key.ClientID != "client-a" {
			continue
		}
		state := clients[index].Value
		state.ScopeAssignments = append([]reference.ScopeAssignment(nil), state.ScopeAssignments...)
		for assignmentIndex := range state.ScopeAssignments {
			if state.ScopeAssignments[assignmentIndex].Scope == "scope-a" {
				state.ScopeAssignments[assignmentIndex].RebuildRequired = true
				found = true
			}
		}
		clients[index].Value = state
	}
	if !found {
		t.Fatal("rebuild assignment mutant did not find scope A")
	}
	execution.After.Clients = clients
	mutant.Steps = append([]OperationExecution(nil), result.Steps...)
	mutant.Steps[12] = execution
	mutant.FinalSnapshot = execution.After
	assertSemanticMutantFails(t, result, mutant)
}

func runSemanticPerformanceScenario(t *testing.T, path string) Result {
	t.Helper()
	scenario, err := scenarios.LoadFile(context.Background(), "../..", path)
	if err != nil {
		t.Fatalf("load semantic scenario: %v", err)
	}
	result, err := RunScenario(context.Background(), scenario)
	if err != nil {
		t.Fatalf("run semantic scenario: %v", err)
	}
	if !performanceContractSatisfied(result.ScenarioID, result) {
		t.Fatal("baseline semantic predicate did not pass")
	}
	return result
}

func cloneSemanticStep(execution OperationExecution) OperationExecution {
	execution.Operation = cloneOperation(execution.Operation)
	execution.Result = cloneStepResult(execution.Result)
	return execution
}

func mutateSemanticAssignments(snapshot reference.StateSnapshot, client reference.ClientKey, generation reference.Generation, includeLocal bool) reference.StateSnapshot {
	snapshot.Clients = append([]reference.SnapshotEntry[reference.ClientKey, reference.ClientState](nil), snapshot.Clients...)
	for index := range snapshot.Clients {
		if snapshot.Clients[index].Key != client {
			continue
		}
		state := snapshot.Clients[index].Value
		state.ScopeAssignments = append([]reference.ScopeAssignment(nil), state.ScopeAssignments...)
		state.ScopeAssignments[0].MembershipGeneration = generation
		snapshot.Clients[index].Value = state
	}
	if includeLocal {
		snapshot = mutateSemanticLocalAssignment(snapshot, client, generation)
	}
	return snapshot
}

func mutateSemanticLocalAssignment(snapshot reference.StateSnapshot, client reference.ClientKey, generation reference.Generation) reference.StateSnapshot {
	snapshot.ClientLocal = append([]reference.SnapshotEntry[reference.ClientKey, reference.ClientLocalState](nil), snapshot.ClientLocal...)
	for index := range snapshot.ClientLocal {
		if snapshot.ClientLocal[index].Key != client {
			continue
		}
		state := snapshot.ClientLocal[index].Value
		state.ScopeAssignments = append([]reference.LocalScopeAssignment(nil), state.ScopeAssignments...)
		state.ScopeAssignments[0].MembershipGeneration = generation
		snapshot.ClientLocal[index].Value = state
	}
	return snapshot
}

func mutateSemanticServerCheckpoint(snapshot reference.StateSnapshot, client reference.ClientKey, mutate func(*reference.ClientCheckpoint)) reference.StateSnapshot {
	snapshot.Clients = append([]reference.SnapshotEntry[reference.ClientKey, reference.ClientState](nil), snapshot.Clients...)
	for index := range snapshot.Clients {
		if snapshot.Clients[index].Key != client {
			continue
		}
		state := snapshot.Clients[index].Value
		state.Checkpoints = append([]reference.ClientCheckpoint(nil), state.Checkpoints...)
		mutate(&state.Checkpoints[0])
		snapshot.Clients[index].Value = state
	}
	return snapshot
}

func mutateSemanticLocalCheckpoint(snapshot reference.StateSnapshot, client reference.ClientKey, mutate func(*reference.LocalScopeCheckpoint)) reference.StateSnapshot {
	snapshot.ClientLocal = append([]reference.SnapshotEntry[reference.ClientKey, reference.ClientLocalState](nil), snapshot.ClientLocal...)
	for index := range snapshot.ClientLocal {
		if snapshot.ClientLocal[index].Key != client {
			continue
		}
		state := snapshot.ClientLocal[index].Value
		state.ScopeCheckpoints = append([]reference.LocalScopeCheckpoint(nil), state.ScopeCheckpoints...)
		mutate(&state.ScopeCheckpoints[0])
		snapshot.ClientLocal[index].Value = state
	}
	return snapshot
}

func forgeSemanticAuthoritativeFields(snapshot reference.StateSnapshot, wireJSON string) reference.StateSnapshot {
	snapshot.Rows = append([]reference.SnapshotEntry[reference.RowIdentity, reference.AuthoritativeRow](nil), snapshot.Rows...)
	for index := range snapshot.Rows {
		row := snapshot.Rows[index].Value
		row.FieldValues = append([]reference.FieldValue(nil), row.FieldValues...)
		forgeSemanticFieldValues(row.FieldValues, wireJSON)
		snapshot.Rows[index].Value = row
	}
	return snapshot
}

func forgeSemanticLocalFields(snapshot reference.StateSnapshot, client reference.ClientKey, wireJSON string) reference.StateSnapshot {
	snapshot.ClientLocal = append([]reference.SnapshotEntry[reference.ClientKey, reference.ClientLocalState](nil), snapshot.ClientLocal...)
	for index := range snapshot.ClientLocal {
		if snapshot.ClientLocal[index].Key != client {
			continue
		}
		state := snapshot.ClientLocal[index].Value
		state.Rows = append([]reference.LocalRow(nil), state.Rows...)
		for rowIndex := range state.Rows {
			state.Rows[rowIndex].Fields = append([]reference.FieldValue(nil), state.Rows[rowIndex].Fields...)
			forgeSemanticFieldValues(state.Rows[rowIndex].Fields, wireJSON)
		}
		snapshot.ClientLocal[index].Value = state
	}
	return snapshot
}

func forgePendingSemanticFields(snapshot reference.StateSnapshot, wireJSON string) reference.StateSnapshot {
	snapshot = forgeSemanticLocalFields(snapshot, reference.ClientKey{UserID: "user-a", ClientID: "client-a"}, wireJSON)
	snapshot = forgeSemanticAuthoritativeFields(snapshot, wireJSON)
	snapshot.Stream.SourceRows = append([]reference.SourceRowEntry(nil), snapshot.Stream.SourceRows...)
	for index := range snapshot.Stream.SourceRows {
		row := snapshot.Stream.SourceRows[index].Row
		row.FieldValues = append([]reference.FieldValue(nil), row.FieldValues...)
		forgeSemanticFieldValues(row.FieldValues, wireJSON)
		snapshot.Stream.SourceRows[index].Row = row
	}
	snapshot.Stream.Transactions = append([]reference.StreamTransaction(nil), snapshot.Stream.Transactions...)
	for transactionIndex := range snapshot.Stream.Transactions {
		transaction := snapshot.Stream.Transactions[transactionIndex]
		transaction.Events = append([]reference.SourceEvent(nil), transaction.Events...)
		for eventIndex := range transaction.Events {
			event := transaction.Events[eventIndex]
			if event.HasAfter {
				event.After.Fields = append([]reference.FieldValue(nil), event.After.Fields...)
				forgeSemanticFieldValues(event.After.Fields, wireJSON)
			}
			transaction.Events[eventIndex] = event
		}
		snapshot.Stream.Transactions[transactionIndex] = transaction
	}
	return snapshot
}

func forgeSemanticFieldValues(fields []reference.FieldValue, wireJSON string) {
	for index := range fields {
		if fields[index].Field == "value" {
			fields[index].WireJSON = wireJSON
			return
		}
	}
}

func assertSemanticMutantFails(t *testing.T, baseline, mutant Result) {
	t.Helper()
	if exactHTTPCount(baseline) != exactHTTPCount(mutant) {
		t.Fatal("semantic mutant changed the HTTP request count")
	}
	for index := range baseline.Steps {
		baselineHTTP, mutantHTTP := baseline.Steps[index].Result.HTTP, mutant.Steps[index].Result.HTTP
		if baseline.Steps[index].OperationKey != mutant.Steps[index].OperationKey || (baselineHTTP == nil) != (mutantHTTP == nil) || baselineHTTP != nil && baselineHTTP.Status != mutantHTTP.Status {
			t.Fatal("semantic mutant changed the trace shape or HTTP status")
		}
	}
	if performanceContractSatisfied(mutant.ScenarioID, mutant) {
		t.Fatal("semantic mutant satisfied the protected predicate")
	}
}

func TestSourceStepResolutionRequiresEarlierSuccessfulPull(t *testing.T) {
	operation := scenarios.Operation{ContractOperation: "local", Name: "apply-pull-page", Payload: json.RawMessage(`{"user_id":"user-a","client_id":"client-a","source_step_id":"STEP-PULL-001"}`)}
	pull := reference.StepResult{Kind: reference.StepResultKindPull, HTTP: &reference.HTTPObservation{Status: 200}, Pull: &reference.PullObservation{}}
	prior := map[scenarios.StepID]priorStep{"STEP-PULL-001": {Index: 0, StepID: "STEP-PULL-001", OperationKey: "pull/request-page", Result: pull}}
	input, err := resolvedInputForOperation(reference.StateSnapshot{ProtocolVersion: 3}, operation, prior, 1, scenarios.Scenario{ID: "SCN-TEST-001"})
	if err != nil {
		t.Fatalf("resolve source step: %v", err)
	}
	if input.SourceStep == nil || input.SourceStep.StepID != "STEP-PULL-001" {
		t.Fatalf("resolved source step = %#v", input.SourceStep)
	}
	input.SourceStep.Result.Pull.Changes = append(input.SourceStep.Result.Pull.Changes, reference.PullChangeObservation{Scope: "mutated"})
	if len(prior["STEP-PULL-001"].Result.Pull.Changes) != 0 {
		t.Fatal("resolved source result aliases the private prior-step result")
	}

	for name, candidate := range map[string]priorStep{
		"same step":       {Index: 1, StepID: "STEP-PULL-001", OperationKey: "pull/request-page", Result: pull},
		"wrong operation": {Index: 0, StepID: "STEP-PULL-001", OperationKey: "pull/request-page-other", Result: pull},
		"wrong result":    {Index: 0, StepID: "STEP-PULL-001", OperationKey: "pull/request-page", Result: reference.StepResult{Kind: reference.StepResultKindLocal}},
		"wrong status":    {Index: 0, StepID: "STEP-PULL-001", OperationKey: "pull/request-page", Result: reference.StepResult{Kind: reference.StepResultKindPull, HTTP: &reference.HTTPObservation{Status: 500}, Pull: &reference.PullObservation{}}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := resolvedInputForOperation(reference.StateSnapshot{ProtocolVersion: 3}, operation, map[scenarios.StepID]priorStep{"STEP-PULL-001": candidate}, 1, scenarios.Scenario{ID: "SCN-TEST-001"})
			if err == nil {
				t.Fatal("misbound source step was accepted")
			}
		})
	}
}

func TestInitialSnapshotMustContainOnlyProtocolVersion(t *testing.T) {
	valid := reference.StateSnapshot{ProtocolVersion: 3}
	if err := requireFreshModel(valid); err != nil {
		t.Fatalf("zero protocol 3 state rejected: %v", err)
	}
	mutant := valid
	mutant.Stream.Transactions = []reference.StreamTransaction{{}}
	if err := requireFreshModel(mutant); err == nil {
		t.Fatal("setup accepted preseeded source transaction state")
	}
}

func TestExpectedErrorMatchingUsesCanonicalErrorCodeOnly(t *testing.T) {
	code := "source_transaction_poison_blocked"
	expected := scenarios.ExpectedOutcome{Disposition: "error", ErrorCode: &code}
	if err := matchExpectedOutcome(expected, codedTestError{code: code}); err != nil {
		t.Fatalf("typed expected error rejected: %v", err)
	}
	if err := matchExpectedOutcome(expected, errors.New("source_transaction_poison_blocked")); err == nil {
		t.Fatal("free-form error text satisfied a typed expected error")
	}
	wrong := codedTestError{code: "source_transaction_predecessor_pending"}
	if err := matchExpectedOutcome(expected, wrong); err == nil {
		t.Fatal("wrong canonical error code was accepted")
	}
}

func TestTransportFailureWireExpectationRejectsReceivedHTTPResponse(t *testing.T) {
	scenario := scenarios.Scenario{WireExpectations: []scenarios.WireExpectation{{
		StepID:       "STEP-TRANSPORT-001",
		AssertionID:  "ASSERT-TRANSPORT-001",
		ContractCase: "transport_failure",
		HTTPStatus:   0,
		Retryable:    true,
	}}}
	execution := OperationExecution{
		StepID: "STEP-TRANSPORT-001",
		Result: reference.StepResult{HTTP: &reference.HTTPObservation{Retryable: true}},
	}
	if err := evaluateWireExpectations(scenario, []OperationExecution{execution}); err != nil {
		t.Fatalf("validate transport failure without response: %v", err)
	}

	execution.Result.HTTP.Body = []byte(`{"error":"fabricated"}`)
	if err := evaluateWireExpectations(scenario, []OperationExecution{execution}); err == nil {
		t.Fatal("transport failure accepted a fabricated HTTP response")
	}
}

func TestPortableSeedCorruptionFailsClosed(t *testing.T) {
	fixture := reference.PortableSeedFixture{
		FixtureID: PortableSeedFixtureID, ArtifactDefinitionID: PortableSeedArtifactID,
		ArtifactBytes: []byte("artifact"), ManifestBytes: []byte("manifest"),
	}
	if err := ValidatePortableSeedFixture(fixture, reference.StateSnapshot{ProtocolVersion: 3}); err == nil {
		t.Fatal("corrupt portable artifact was accepted")
	}
}

func TestReplayHashIsStableForEquivalentOperations(t *testing.T) {
	left := []ReplayOperation{{StepID: "STEP-001", OperationKey: "process/restart-wal-worker", Payload: []byte(`{"worker_id":"worker-a"}`)}}
	right := []ReplayOperation{{StepID: "STEP-001", OperationKey: "process/restart-wal-worker", Payload: []byte(`{"worker_id":"worker-a"}`)}}
	if hashReplay(left) != hashReplay(right) {
		t.Fatal("equivalent replay operations produced different hashes")
	}
	if !reflect.DeepEqual(left, right) {
		t.Fatal("test replay operations are not equivalent")
	}
}

func TestAuthoredStateFactsRejectDeterministicWrongState(t *testing.T) {
	scenario, err := scenarios.LoadFile(context.Background(), "../..", "conformance/scenarios/server/wal-order-001.json")
	if err != nil {
		t.Fatalf("load scenario: %v", err)
	}
	facts := scenario.Model.ExpectedState[0].StateFacts
	if facts == nil {
		t.Fatal("scenario has no authored state facts")
	}
	facts.RowCount = uint64Pointer(2)

	result, err := RunScenario(context.Background(), scenario)
	if err == nil {
		t.Fatal("deterministic wrong state satisfied the authored state facts")
	}
	var runErr *RunError
	if !errors.As(err, &runErr) || runErr.Kind != RunErrorPredicate || runErr.Expectation != scenario.Model.ExpectedState[0].ID {
		t.Fatalf("wrong-state failure = %#v", err)
	}
	if !result.Replay.StateMatch {
		t.Fatal("wrong-state mutant did not preserve deterministic replay")
	}
}

func TestSchemaDispatchMeasurementRejectsRepeatedStratum(t *testing.T) {
	scenario, err := scenarios.LoadFile(context.Background(), "../..", "conformance/scenarios/performance/schema-check-001.json")
	if err != nil {
		t.Fatalf("load schema-dispatch scenario: %v", err)
	}

	mutant := scenario
	mutant.Steps = make([]scenarios.Step, 0, len(scenario.Steps)-1)
	replacementIDs := []string{
		"SAMPLE-SCHEMA-CLASS-3-AFFECTED-004",
		"SAMPLE-SCHEMA-CLASS-3-AFFECTED-005",
		"SAMPLE-SCHEMA-CLASS-3-AFFECTED-006",
	}
	replacements := 0
	for _, original := range scenario.Steps {
		if original.ID == "STEP-PERF-SCHEMA-CHECK-CLASS4-PUBLISH-001" {
			continue
		}
		step := original
		if step.MeasurementSample != nil && step.MeasurementSample.StratumID == "STR-SCHEMA-CLASS-4-001" {
			binding := *step.MeasurementSample
			binding.StratumID = "STR-SCHEMA-CLASS-3-AFFECTED-001"
			binding.SampleID = replacementIDs[replacements]
			binding.Parameters = json.RawMessage(`{"schema_case":"class_3_affected"}`)
			step.MeasurementSample = &binding
			replacements++
		}
		mutant.Steps = append(mutant.Steps, step)
	}
	if replacements != len(replacementIDs) {
		t.Fatalf("replaced strata = %d, want %d", replacements, len(replacementIDs))
	}

	result, err := RunScenario(context.Background(), mutant)
	if err == nil {
		t.Fatal("a repeated schema stratum with no Class 4 observations passed")
	}
	var runErr *RunError
	if !errors.As(err, &runErr) || runErr.Kind != RunErrorPredicate || runErr.Expectation != "EXPECT-PERF-SCHEMA-CHECK-DISPATCH-001" {
		t.Fatalf("mutant failure = %#v", err)
	}
	for _, predicate := range result.Predicates {
		if predicate.ExpectationID == "EXPECT-PERF-SCHEMA-CHECK-DISPATCH-001" {
			if predicate.Passed {
				t.Fatal("schema-dispatch predicate accepted repeated Class 3 observations without Class 4 observations")
			}
			return
		}
	}
	t.Fatal("schema-dispatch predicate result is absent")
}

func TestSchemaDispatchMeasurementRejectsMislabelledStratum(t *testing.T) {
	scenario, err := scenarios.LoadFile(context.Background(), "../..", "conformance/scenarios/performance/schema-check-001.json")
	if err != nil {
		t.Fatalf("load schema-dispatch scenario: %v", err)
	}

	mutant := scenario
	mutant.Steps = append([]scenarios.Step(nil), scenario.Steps...)
	changed := false
	for index, step := range mutant.Steps {
		if step.MeasurementSample == nil || step.MeasurementSample.StratumID != "STR-SCHEMA-CLASS-4-001" {
			continue
		}
		binding := *step.MeasurementSample
		binding.StratumID = "STR-SCHEMA-CLASS-3-AFFECTED-001"
		mutant.Steps[index].MeasurementSample = &binding
		changed = true
		break
	}
	if !changed {
		t.Fatal("schema-dispatch scenario has no Class 4 sample")
	}

	result, err := RunScenario(context.Background(), mutant)
	if err == nil {
		t.Fatal("a Class 4 observation labelled as a Class 3 stratum passed")
	}
	var runErr *RunError
	if !errors.As(err, &runErr) || runErr.Kind != RunErrorPredicate || runErr.Expectation != "EXPECT-PERF-SCHEMA-CHECK-DISPATCH-001" {
		t.Fatalf("mutant failure = %#v", err)
	}
	if result.Passed {
		t.Fatal("mislabelled schema-dispatch result passed")
	}
}

func TestProvenanceStateFactsRejectWrongEdges(t *testing.T) {
	scenario, err := scenarios.LoadFile(context.Background(), "../..", "conformance/scenarios/performance/multi-scope-provenance-001.json")
	if err != nil {
		t.Fatalf("load scenario: %v", err)
	}
	facts := scenario.Model.ExpectedState[0].StateFacts
	provenanceClient := -1
	if facts != nil {
		for index, client := range facts.Clients {
			if client.UserID == "user-a" && client.ClientID == "client-f" {
				provenanceClient = index
				break
			}
		}
	}
	if provenanceClient == -1 || len(facts.Clients[provenanceClient].Provenance) != 2 {
		t.Fatal("scenario has no exact authored provenance facts")
	}
	mutants := map[string]func(*scenarios.StateFacts){
		"stale scope": func(value *scenarios.StateFacts) {
			value.Clients[provenanceClient].Provenance[0].Scopes = []string{"scope-a", "scope-b"}
		},
		"missing scope": func(value *scenarios.StateFacts) {
			value.Clients[provenanceClient].Provenance[0].Scopes = nil
		},
		"cross row": func(value *scenarios.StateFacts) {
			value.Clients[provenanceClient].Provenance[0].CanonicalWireJSON = `"scope-topology-row-000002"`
		},
		"wrong version": func(value *scenarios.StateFacts) {
			value.Clients[provenanceClient].Provenance[0].Version = "wrong-version"
		},
	}
	for name, mutate := range mutants {
		t.Run(name, func(t *testing.T) {
			candidate := scenario
			candidate.Model.ExpectedState = append([]scenarios.ModelExpectation(nil), scenario.Model.ExpectedState...)
			copiedFacts := *facts
			copiedFacts.Clients = append([]scenarios.ClientDurabilityFact(nil), facts.Clients...)
			copiedFacts.Clients[provenanceClient].Provenance = append([]scenarios.ProvenanceFact(nil), facts.Clients[provenanceClient].Provenance...)
			candidate.Model.ExpectedState[0].StateFacts = &copiedFacts
			mutate(&copiedFacts)
			if _, err := RunScenario(context.Background(), candidate); err == nil {
				t.Fatal("wrong provenance facts satisfied the authored model")
			}
		})
	}
}

func uint64Pointer(value uint64) *uint64 { return &value }
