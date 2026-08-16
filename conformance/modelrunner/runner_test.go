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

func TestProvenanceStateFactsRejectWrongEdges(t *testing.T) {
	scenario, err := scenarios.LoadFile(context.Background(), "../..", "conformance/scenarios/performance/multi-scope-provenance-001.json")
	if err != nil {
		t.Fatalf("load scenario: %v", err)
	}
	facts := scenario.Model.ExpectedState[0].StateFacts
	if facts == nil || len(facts.Clients) != 1 || len(facts.Clients[0].Provenance) != 2 {
		t.Fatal("scenario has no exact authored provenance facts")
	}
	mutants := map[string]func(*scenarios.StateFacts){
		"stale scope": func(value *scenarios.StateFacts) {
			value.Clients[0].Provenance[0].Scopes = []string{"scope-a", "scope-b"}
		},
		"missing scope": func(value *scenarios.StateFacts) {
			value.Clients[0].Provenance[0].Scopes = nil
		},
		"cross row": func(value *scenarios.StateFacts) {
			value.Clients[0].Provenance[0].CanonicalWireJSON = `"scope-topology-row-000002"`
		},
		"wrong version": func(value *scenarios.StateFacts) {
			value.Clients[0].Provenance[0].Version = "wrong-version"
		},
	}
	for name, mutate := range mutants {
		t.Run(name, func(t *testing.T) {
			candidate := scenario
			candidate.Model.ExpectedState = append([]scenarios.ModelExpectation(nil), scenario.Model.ExpectedState...)
			copiedFacts := *facts
			copiedFacts.Clients = append([]scenarios.ClientDurabilityFact(nil), facts.Clients...)
			copiedFacts.Clients[0].Provenance = append([]scenarios.ProvenanceFact(nil), facts.Clients[0].Provenance...)
			candidate.Model.ExpectedState[0].StateFacts = &copiedFacts
			mutate(&copiedFacts)
			if _, err := RunScenario(context.Background(), candidate); err == nil {
				t.Fatal("wrong provenance facts satisfied the authored model")
			}
		})
	}
}

func uint64Pointer(value uint64) *uint64 { return &value }
