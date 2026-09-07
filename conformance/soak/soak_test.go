package soak

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/trainstar/synchro/conformance/faults"
	"github.com/trainstar/synchro/conformance/invariants"
	"github.com/trainstar/synchro/conformance/scenarios"
	"github.com/trainstar/synchro/conformance/vectors"
)

func TestGeneratorDeterminismAcrossSeeds(t *testing.T) {
	catalog := testCatalog(t)
	config := Config{OperationCount: 32, FaultRate: 50}
	first, err := Generate(42, config, catalog)
	if err != nil {
		t.Fatalf("generate first plan: %v", err)
	}
	repeated, err := Generate(42, config, catalog)
	if err != nil {
		t.Fatalf("generate repeated plan: %v", err)
	}
	if !reflect.DeepEqual(first, repeated) {
		t.Fatal("identical seeds produced different plans")
	}
	different, err := Generate(43, config, catalog)
	if err != nil {
		t.Fatalf("generate different plan: %v", err)
	}
	if reflect.DeepEqual(first.Operations, different.Operations) {
		t.Fatal("different seeds produced identical operation plans")
	}
	if len(first.Operations) < MinimumCoverageOperations {
		t.Fatalf("operation count = %d, want coverage of %d families", len(first.Operations), MinimumCoverageOperations)
	}
	for index, kind := range operationKinds {
		if first.Operations[index].Kind != kind {
			t.Fatalf("operation %d = %s, want coverage prefix %s", index+1, first.Operations[index].Kind, kind)
		}
	}
	for _, operation := range first.Operations {
		if operation.FaultPlan == nil {
			continue
		}
		if !faultSupportsOperation(operation.Kind, *operation.FaultPlan) {
			t.Fatalf("operation %d selected unsupported fault mechanism %q", operation.Sequence, operation.FaultPlan.Injection.Mechanism)
		}
	}
}

func TestGeneratorRejectsShortCoverageConfiguration(t *testing.T) {
	_, err := Generate(1, Config{OperationCount: MinimumCoverageOperations - 1}, testCatalog(t))
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("short configuration error = %v, want ErrInvalidConfig", err)
	}
	if _, err := ConfigForDuration(0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("zero duration error = %v, want ErrInvalidConfig", err)
	}
}

func TestAssembleObservationCarriesRedoneFacts(t *testing.T) {
	rawCursor := "cursor"
	position := invariants.CursorPositionObservation{
		UserID: "user-a", ClientID: "client-a", ScopeID: "scope-a", Generation: 1,
		RawCursor: rawCursor, StreamGeneration: "stream-a",
		Position: invariants.PositionObservation{Kind: "generation_start"},
	}
	capture := ObservationCapture{
		CursorPositions:        []invariants.CursorPositionObservation{position},
		PullResults:            []invariants.PullResultObservation{{ExchangeSequence: 1, UserID: "user-a", ClientID: "client-a", Cursors: []invariants.CursorPositionObservation{position}}},
		CursorAcknowledgements: []invariants.CursorAcknowledgementObservation{{ExchangeSequence: 1, Cursor: position}},
		ServerRowIdentities:    []invariants.ServerRowIdentityObservation{{TableID: "table", CanonicalWireJSON: `"pk"`, RowIdentity: []byte("identity")}},
	}
	observation := AssembleObservation(7, capture)
	if len(observation.CursorPositions) != 1 || len(observation.PullResults) != 1 || len(observation.CursorAcknowledgements) != 1 || len(observation.ServerRowIdentities) != 1 {
		t.Fatalf("assembled observation lost redo facts: %#v", observation)
	}
}

func TestRunRejectsIncompleteCaptureBeforeCheckers(t *testing.T) {
	plan, err := Generate(2, Config{OperationCount: MinimumCoverageOperations}, testCatalog(t))
	if err != nil {
		t.Fatalf("generate plan: %v", err)
	}
	_, err = Run(context.Background(), plan, emptyCaptureHarness{}, journalPath(t, "incomplete"))
	if !errors.Is(err, ErrCaptureIncomplete) {
		t.Fatalf("incomplete capture error = %v, want ErrCaptureIncomplete", err)
	}
}

func TestNegativeControlEmptyExecution(t *testing.T) {
	_, err := runPlan(context.Background(), Plan{}, scriptedHarness{}, nil)
	if !errors.Is(err, ErrZeroOperations) {
		t.Fatalf("empty execution error = %v, want ErrZeroOperations", err)
	}
}

func TestNegativeControlKnownCheckerViolation(t *testing.T) {
	plan, err := Generate(3, Config{OperationCount: MinimumCoverageOperations}, testCatalog(t))
	if err != nil {
		t.Fatalf("generate plan: %v", err)
	}
	_, err = Run(context.Background(), plan, changingProcessHarness{}, journalPath(t, "violation"))
	if !errors.Is(err, ErrInvariantViolation) {
		t.Fatalf("checker violation error = %v, want ErrInvariantViolation", err)
	}
}

func TestNegativeControlMalformedJournal(t *testing.T) {
	path := journalPath(t, "malformed")
	if err := os.WriteFile(path, []byte("not-json\n"), 0o600); err != nil {
		t.Fatalf("write malformed journal: %v", err)
	}
	if _, err := ReadJournal(path); !errors.Is(err, ErrInvalidJournal) {
		t.Fatalf("malformed journal error = %v, want ErrInvalidJournal", err)
	}
}

func TestJournalBindsInputsFactsAndCatalog(t *testing.T) {
	catalog := testCatalog(t)
	plan, err := Generate(4, Config{OperationCount: MinimumCoverageOperations}, catalog)
	if err != nil {
		t.Fatalf("generate plan: %v", err)
	}
	path := journalPath(t, "round-trip")
	if _, err := Run(context.Background(), plan, stableHarness{}, path); err != nil {
		t.Fatalf("run plan: %v", err)
	}
	journal, err := ReadJournal(path)
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	if journal.CatalogIdentity != plan.CatalogIdentity || !reflect.DeepEqual(journal.Operations, plan.Operations) {
		t.Fatal("journal did not preserve immutable plan identity and inputs")
	}
	if len(journal.OperationFacts) != len(plan.Operations) || len(journal.Observations) != len(plan.Operations) {
		t.Fatalf("journal facts = %d and observations = %d, want %d each", len(journal.OperationFacts), len(journal.Observations), len(plan.Operations))
	}
	for _, fact := range journal.OperationFacts {
		if fact.Status != "completed" || fact.ObservationSequence == 0 {
			t.Fatalf("invalid completion fact: %#v", fact)
		}
	}
	if len(journal.Observations[0].Attachments) == 0 {
		t.Fatal("journal did not retain bounded wire attachment identities")
	}
}

func TestReplayRejectsChangedOperationConfigurationAndCatalog(t *testing.T) {
	catalog := testCatalog(t)
	plan, err := Generate(5, Config{OperationCount: MinimumCoverageOperations}, catalog)
	if err != nil {
		t.Fatalf("generate plan: %v", err)
	}
	path := journalPath(t, "replay-mismatch")
	if _, err := Run(context.Background(), plan, stableHarness{}, path); err != nil {
		t.Fatalf("run plan: %v", err)
	}
	original, err := ReadJournal(path)
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}

	changedOperation := original
	changedOperation.Operations = append([]Operation(nil), original.Operations...)
	changedOperation.Operations[0].Input = json.RawMessage(`{"kind":"changed"}`)
	if _, err := ReplayPlan(changedOperation, catalog); !errors.Is(err, ErrReplayMismatch) {
		t.Fatalf("changed operation error = %v, want ErrReplayMismatch", err)
	}

	changedConfig := original
	changedConfig.Config = cloneConfig(original.Config)
	changedConfig.Config.FaultRate++
	if _, err := ReplayPlan(changedConfig, catalog); !errors.Is(err, ErrReplayMismatch) {
		t.Fatalf("changed configuration error = %v, want ErrReplayMismatch", err)
	}

	changedCatalog := *catalog
	changedCatalog.Release = "0.3.0-changed"
	if _, err := ReplayPlan(original, &changedCatalog); !errors.Is(err, ErrReplayMismatch) {
		t.Fatalf("changed catalog error = %v, want ErrReplayMismatch", err)
	}
}

func TestReplayRunDoesNotOverwriteSourceJournal(t *testing.T) {
	catalog := testCatalog(t)
	plan, err := Generate(6, Config{OperationCount: MinimumCoverageOperations}, catalog)
	if err != nil {
		t.Fatalf("generate plan: %v", err)
	}
	path := journalPath(t, "replay-read-only")
	if _, err := Run(context.Background(), plan, stableHarness{}, path); err != nil {
		t.Fatalf("run plan: %v", err)
	}
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read source journal bytes: %v", err)
	}
	if _, err := ReplayRun(context.Background(), path, catalog, stableHarness{}); err != nil {
		t.Fatalf("replay run: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read replay journal bytes: %v", err)
	}
	if !bytes.Equal(original, after) {
		t.Fatal("read-only replay changed the source journal")
	}
}

func TestRunnerOrdersCrossFamilyViolations(t *testing.T) {
	position := invariants.PositionObservation{Kind: "invalid"}
	first := invariants.Observation{
		Sequence:      1,
		Operator:      &invariants.OperatorObservation{Checkpoints: []invariants.OperatorCheckpointObservation{{UserID: "u", ClientID: "c", ScopeID: "s", Position: position}}},
		WireExchanges: []invariants.WireExchangeObservation{{Sequence: 1, ResponseStatus: 500, ExpectMutationConservation: true, ExpectChecksumConvergence: true, ExpectScopeIsolation: true}},
		Clients:       []invariants.ClientObservation{{State: scenarios.ClientDurabilityFact{UserID: "u", ClientID: "c"}, Process: &invariants.ProcessIdentityObservation{ProcessID: "p1", DatabaseIdentityFingerprint: digest}, Complete: true}},
	}
	second := first
	second.Sequence = 2
	second.Operator = &invariants.OperatorObservation{}
	second.WireExchanges = nil
	second.Clients = []invariants.ClientObservation{{State: scenarios.ClientDurabilityFact{UserID: "u", ClientID: "c"}, Process: &invariants.ProcessIdentityObservation{ProcessID: "p2", DatabaseIdentityFingerprint: digest}, Complete: true}}
	violations, err := checkAll([]invariants.Observation{first, second})
	if err != nil {
		t.Fatalf("check all: %v", err)
	}
	if len(violations) < 4 {
		t.Fatalf("violations = %d, want multiple checker families", len(violations))
	}
	for index := 1; index < len(violations); index++ {
		if compareViolations(violations[index-1], violations[index]) > 0 {
			t.Fatalf("violations are not in total order: %#v", violations)
		}
	}
	if violations[0].Family != invariants.InvariantChecksumConvergence {
		t.Fatalf("first family = %s, want checksum-convergence by total order", violations[0].Family)
	}
}

func testCatalog(t *testing.T) *faults.Catalog {
	t.Helper()
	catalog, err := faults.LoadCatalog(context.Background(), "../..")
	if err != nil {
		t.Fatalf("load fault catalog: %v", err)
	}
	return catalog
}

func journalPath(t *testing.T, name string) string {
	t.Helper()
	return t.TempDir() + "/soak-" + name + ".jsonl"
}

const digest = "0000000000000000000000000000000000000000000000000000000000000000"

type emptyCaptureHarness struct{}

func (emptyCaptureHarness) Execute(context.Context, Operation) (ObservationCapture, error) {
	return ObservationCapture{}, nil
}

type changingProcessHarness struct{}

func (changingProcessHarness) Execute(_ context.Context, operation Operation) (ObservationCapture, error) {
	return captureForOperation(operation, "pid-"+string(rune('a'+operation.Sequence))), nil
}

type stableHarness struct{}

func (stableHarness) Execute(_ context.Context, operation Operation) (ObservationCapture, error) {
	processID := "pid-stable"
	if operation.Sequence >= 6 {
		processID = "pid-restarted"
	}
	return captureForOperation(operation, processID), nil
}

func (scriptedHarness) Execute(context.Context, Operation) (ObservationCapture, error) {
	return ObservationCapture{}, nil
}

type scriptedHarness struct{}

func captureForOperation(operation Operation, processID string) ObservationCapture {
	clients := []invariants.ClientObservation{{
		State:    scenarios.ClientDurabilityFact{UserID: operation.UserID, ClientID: operation.ClientID},
		Process:  &invariants.ProcessIdentityObservation{ProcessID: processID, DatabaseIdentityFingerprint: digest},
		Complete: true,
	}}
	capture := ObservationCapture{
		Manifest:            &vectors.Manifest{},
		ServerState:         &scenarios.StateFacts{},
		Operator:            &invariants.OperatorObservation{},
		Clients:             clients,
		WireExchanges:       []invariants.WireExchangeObservation{{Sequence: operation.Sequence, OperationClass: "noop", ResponseStatus: 200, RequestBody: []byte(`{}`), ResponseBody: []byte(`{}`)}},
		ServerRowIdentities: []invariants.ServerRowIdentityObservation{},
	}
	if operation.FaultPlan != nil {
		capture.FaultActivation = &FaultActivationObservation{ControlID: string(operation.FaultPlan.ControlID), Target: operation.ClientID, Activated: true, CleanedUp: true}
	}
	if operation.Kind == OperationProcessDeath {
		capture.Clients[0].RestartBoundary = true
	}
	if operation.Kind == OperationPull {
		capture = addPullFacts(capture, operation)
	}
	return capture
}

func addPullFacts(capture ObservationCapture, operation Operation) ObservationCapture {
	positionA := pullPosition(operation, operation.ScopeID, "cursor-a")
	positionB := pullPosition(operation, otherScope(operation.ScopeID), "cursor-b")
	cursorA := "cursor-a"
	cursorB := "cursor-b"
	capture.Clients[0].Scopes = []invariants.ClientScopeObservation{{ScopeID: positionA.ScopeID, RawCursor: &cursorA, Generation: 1}, {ScopeID: positionB.ScopeID, RawCursor: &cursorB, Generation: 1}}
	capture.Operator.Checkpoints = []invariants.OperatorCheckpointObservation{{UserID: operation.UserID, ClientID: operation.ClientID, ScopeID: positionA.ScopeID, StreamGeneration: "stream", Position: positionA.Position}, {UserID: operation.UserID, ClientID: operation.ClientID, ScopeID: positionB.ScopeID, StreamGeneration: "stream", Position: positionB.Position}}
	request := `{"client_id":"` + operation.ClientID + `","scopes":{"` + positionA.ScopeID + `":{"cursor":"cursor-a"},"` + positionB.ScopeID + `":{"cursor":"cursor-b"}}}`
	response := `{"has_more":false,"scope_cursors":{"` + positionA.ScopeID + `":"cursor-a","` + positionB.ScopeID + `":"cursor-b"},"changes":[{"scope":"` + positionA.ScopeID + `","table":"00000000-0000-4000-8000-000000000001","pk":{"00000000-0000-4000-8000-000000000002":"row"}}],"rebuild":[],"scope_updates":{"remove":[]}}`
	capture.WireExchanges[0] = invariants.WireExchangeObservation{Sequence: operation.Sequence, OperationClass: "pull", ResponseStatus: 200, RequestBody: []byte(request), ResponseBody: []byte(response)}
	capture.CursorPositions = []invariants.CursorPositionObservation{positionA, positionB}
	capture.PullResults = []invariants.PullResultObservation{{ExchangeSequence: operation.Sequence, UserID: operation.UserID, ClientID: operation.ClientID, Changes: []invariants.PullChangeIdentityObservation{{ScopeID: positionA.ScopeID, TableID: "00000000-0000-4000-8000-000000000001", PrimaryKeyFieldID: "00000000-0000-4000-8000-000000000002", PrimaryKey: json.RawMessage(`"row"`)}}, Cursors: []invariants.CursorPositionObservation{positionA, positionB}}}
	capture.CursorAcknowledgements = []invariants.CursorAcknowledgementObservation{{ExchangeSequence: operation.Sequence, Cursor: positionA}, {ExchangeSequence: operation.Sequence, Cursor: positionB}}
	return capture
}

func pullPosition(operation Operation, scopeID, cursor string) invariants.CursorPositionObservation {
	return invariants.CursorPositionObservation{UserID: operation.UserID, ClientID: operation.ClientID, ScopeID: scopeID, Generation: 1, RawCursor: cursor, StreamGeneration: "stream", Position: invariants.PositionObservation{Kind: "transaction_end", CommitLSN: stringPointer("0/1")}}
}

func otherScope(scopeID string) string {
	if scopeID == "scope-a" {
		return "scope-b"
	}
	return "scope-a"
}

func stringPointer(value string) *string {
	return &value
}
