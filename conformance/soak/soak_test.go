package soak

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strconv"
	"strings"
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

func TestGeneratorAcceptsCoverageBoundaryConfigurations(t *testing.T) {
	for _, count := range []int{MinimumCoverageOperations, MaximumOperationCount} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			plan, err := Generate(7, Config{OperationCount: count}, testCatalog(t))
			if err != nil {
				t.Fatalf("generate %d-operation plan: %v", count, err)
			}
			if len(plan.Operations) != count {
				t.Fatalf("operation count = %d, want %d", len(plan.Operations), count)
			}
		})
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

func TestRunRejectsNoopExchangeBeforeCheckers(t *testing.T) {
	plan, err := Generate(8, Config{OperationCount: MinimumCoverageOperations}, testCatalog(t))
	if err != nil {
		t.Fatalf("generate plan: %v", err)
	}
	_, err = Run(context.Background(), plan, noopExchangeHarness{}, journalPath(t, "noop-exchange"))
	if !errors.Is(err, ErrCaptureIncomplete) {
		t.Fatalf("noop exchange error = %v, want ErrCaptureIncomplete", err)
	}
}

func TestRestartBoundaryMatchesUserAndClient(t *testing.T) {
	operation := Operation{Sequence: 2, Kind: OperationProcessDeath, UserID: "user-a", ClientID: "client-a", ScopeID: "scope-a"}
	prior := []invariants.Observation{{Clients: []invariants.ClientObservation{
		{State: scenarios.ClientDurabilityFact{UserID: "user-a", ClientID: "client-a"}},
		{State: scenarios.ClientDurabilityFact{UserID: "user-b", ClientID: "client-a"}},
	}}}
	clients := []invariants.ClientObservation{
		{State: scenarios.ClientDurabilityFact{UserID: "user-a", ClientID: "client-a"}, Scopes: []invariants.ClientScopeObservation{{ScopeID: "scope-a"}}, Process: &invariants.ProcessIdentityObservation{ProcessID: "new-a"}, RestartBoundary: true, Complete: true},
		{State: scenarios.ClientDurabilityFact{UserID: "user-b", ClientID: "client-a"}, Scopes: []invariants.ClientScopeObservation{{ScopeID: "scope-b"}}, Process: &invariants.ProcessIdentityObservation{ProcessID: "same-b"}, Complete: true},
	}
	if err := validateCaptureClients(operation, clients, prior); err != nil {
		t.Fatalf("validate multi-user restart boundary: %v", err)
	}
}

func TestCaptureRejectsDifferentOperationTarget(t *testing.T) {
	plan, err := Generate(13, Config{
		OperationCount: MinimumCoverageOperations,
		Users:          []string{"user-a", "user-b"},
		Clients:        []string{"client-a", "client-b"},
		Scopes:         []string{"scope-a", "scope-b"},
	}, testCatalog(t))
	if err != nil {
		t.Fatalf("generate multi-target plan: %v", err)
	}
	operation := plan.Operations[2]
	if operation.Kind != OperationPull {
		t.Fatalf("operation kind = %s, want pull", operation.Kind)
	}
	different := operation
	if different.UserID == "user-a" {
		different.UserID = "user-b"
	} else {
		different.UserID = "user-a"
	}
	if different.ClientID == "client-a" {
		different.ClientID = "client-b"
	} else {
		different.ClientID = "client-a"
	}
	if different.ScopeID == "scope-a" {
		different.ScopeID = "scope-b"
	} else {
		different.ScopeID = "scope-a"
	}

	mutations := []struct {
		name  string
		apply func(*ObservationCapture, ObservationCapture)
	}{
		{name: "capture", apply: func(capture *ObservationCapture, wrong ObservationCapture) {
			*capture = wrong
		}},
		{name: "wire", apply: func(capture *ObservationCapture, wrong ObservationCapture) {
			capture.WireExchanges = wrong.WireExchanges
		}},
		{name: "pull-fact", apply: func(capture *ObservationCapture, wrong ObservationCapture) {
			capture.PullResults = wrong.PullResults
		}},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			capture := captureForOperation(operation, "pid-target")
			mutation.apply(&capture, captureForOperation(different, "pid-wrong-target"))
			if err := validateObservationCapture(operation, capture, nil); !errors.Is(err, ErrCaptureIncomplete) {
				t.Fatalf("different target error = %v, want ErrCaptureIncomplete", err)
			}
		})
	}
}

func TestCheckerCoverageRejectsUnjudgedApplicableFamilies(t *testing.T) {
	plan, err := Generate(11, Config{OperationCount: MinimumCoverageOperations}, testCatalog(t))
	if err != nil {
		t.Fatalf("generate plan: %v", err)
	}
	if err := validateCheckerCoverage(plan, nil); !errors.Is(err, ErrCheckerCoverage) {
		t.Fatalf("checker coverage error = %v, want ErrCheckerCoverage", err)
	}
}

func TestNegativeControlEmptyExecution(t *testing.T) {
	_, err := runPlan(context.Background(), Plan{}, scriptedHarness{}, nil)
	if !errors.Is(err, ErrZeroOperations) {
		t.Fatalf("empty execution error = %v, want ErrZeroOperations", err)
	}
}

func TestNegativeControlKnownCheckerViolation(t *testing.T) {
	result, path, _ := knownCheckerViolation(t, "violation")
	journal, err := ReadJournal(path)
	if !errors.Is(err, ErrJournalUnsealed) {
		t.Fatalf("violation journal error = %v, want ErrJournalUnsealed", err)
	}
	if journal.RunDigest != "" {
		t.Fatal("violation journal has a completion trailer")
	}
	fact := journal.OperationFacts[result.OperationsExecuted-1]
	if fact.Status != "failed" || fact.ObservationSequence != 0 || fact.FailureCode != "invariant-violation" {
		t.Fatalf("violation fact = %#v, want failed invariant-violation fact", fact)
	}
}

func TestReplayRunReproducesKnownCheckerViolation(t *testing.T) {
	original, path, catalog := knownCheckerViolation(t, "violation-replay")
	replayed, err := ReplayRun(context.Background(), path, catalog, changingProcessHarness{})
	if !errors.Is(err, ErrInvariantViolation) {
		t.Fatalf("replayed checker violation error = %v, want ErrInvariantViolation", err)
	}
	if !reflect.DeepEqual(replayed.Violations, original.Violations) {
		t.Fatalf("replayed violations = %#v, want %#v", replayed.Violations, original.Violations)
	}
}

func TestReadJournalRejectsRecordsAfterFailedFact(t *testing.T) {
	_, path, _ := knownCheckerViolation(t, "failed-fact-suffix")
	suffix := `{"type":"operation_fact","operation_fact":{"sequence":8,"status":"completed","observation_sequence":8}}` + "\n"
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open violation journal for append: %v", err)
	}
	if _, err := file.WriteString(suffix); err != nil {
		t.Fatalf("append completed fact after failure: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close appended journal: %v", err)
	}
	if _, err := ReadJournal(path); !errors.Is(err, ErrInvalidJournal) {
		t.Fatalf("failed-fact suffix journal error = %v, want ErrInvalidJournal", err)
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
	if journal.RunDigest == "" {
		t.Fatal("sealed journal has no run digest")
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

func TestReadJournalReportsUnsealedKilledRun(t *testing.T) {
	plan, err := Generate(9, Config{OperationCount: MinimumCoverageOperations}, testCatalog(t))
	if err != nil {
		t.Fatalf("generate plan: %v", err)
	}
	path := journalPath(t, "unsealed")
	if _, err := Run(context.Background(), plan, emptyCaptureHarness{}, path); !errors.Is(err, ErrCaptureIncomplete) {
		t.Fatalf("killed run error = %v, want ErrCaptureIncomplete", err)
	}
	journal, err := ReadJournal(path)
	if !errors.Is(err, ErrJournalUnsealed) {
		t.Fatalf("unsealed journal error = %v, want ErrJournalUnsealed", err)
	}
	if len(journal.Operations) != len(plan.Operations) || journal.RunDigest != "" {
		t.Fatalf("unsealed journal = %#v", journal)
	}
}

func TestNegativeControlMalformedUnsealedJournal(t *testing.T) {
	plan, err := Generate(12, Config{OperationCount: MinimumCoverageOperations}, testCatalog(t))
	if err != nil {
		t.Fatalf("generate plan: %v", err)
	}
	path := journalPath(t, "malformed-unsealed")
	if _, err := Run(context.Background(), plan, emptyCaptureHarness{}, path); !errors.Is(err, ErrCaptureIncomplete) {
		t.Fatalf("killed run error = %v, want ErrCaptureIncomplete", err)
	}
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read unsealed journal: %v", err)
	}
	mutations := []struct {
		name string
		edit func(*journalRecord)
	}{
		{name: "missing-completion-observation", edit: func(record *journalRecord) {
			record.OperationFact.Status = "completed"
			record.OperationFact.ObservationSequence = 1
			record.OperationFact.FailureCode = ""
		}},
		{name: "oversize-failure-code", edit: func(record *journalRecord) {
			record.OperationFact.FailureCode = strings.Repeat("x", 65)
		}},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			if err := os.WriteFile(path, tamperJournalRecord(original, "operation-fact", mutation.edit), 0o600); err != nil {
				t.Fatalf("write malformed unsealed journal: %v", err)
			}
			if _, err := ReadJournal(path); !errors.Is(err, ErrInvalidJournal) {
				t.Fatalf("malformed unsealed journal error = %v, want ErrInvalidJournal", err)
			}
			if err := os.WriteFile(path, original, 0o600); err != nil {
				t.Fatalf("restore unsealed journal: %v", err)
			}
		})
	}
}

func TestReadJournalRejectsTamperedSealedFactsAndObservations(t *testing.T) {
	plan, err := Generate(10, Config{OperationCount: MinimumCoverageOperations}, testCatalog(t))
	if err != nil {
		t.Fatalf("generate plan: %v", err)
	}
	path := journalPath(t, "tampered-sealed")
	if _, err := Run(context.Background(), plan, stableHarness{}, path); err != nil {
		t.Fatalf("run plan: %v", err)
	}
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read sealed journal: %v", err)
	}
	mutations := []struct {
		name string
		kind string
		edit func(*journalRecord)
	}{
		{name: "fact", kind: "operation-fact", edit: func(record *journalRecord) {
			record.OperationFact.ObservationSequence = 2
		}},
		{name: "observation", kind: "observation", edit: func(record *journalRecord) {
			record.Observation.Attachments[0].RequestBytes++
		}},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			if err := os.WriteFile(path, tamperJournalRecord(original, mutation.kind, mutation.edit), 0o600); err != nil {
				t.Fatalf("write tampered journal: %v", err)
			}
			if _, err := ReadJournal(path); !errors.Is(err, ErrInvalidJournal) {
				t.Fatalf("tampered journal error = %v, want ErrInvalidJournal", err)
			}
			if err := os.WriteFile(path, original, 0o600); err != nil {
				t.Fatalf("restore sealed journal: %v", err)
			}
		})
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

func knownCheckerViolation(t *testing.T, name string) (RunResult, string, *faults.Catalog) {
	t.Helper()
	catalog := testCatalog(t)
	plan, err := Generate(3, Config{OperationCount: MinimumCoverageOperations}, catalog)
	if err != nil {
		t.Fatalf("generate plan: %v", err)
	}
	path := journalPath(t, name)
	result, err := Run(context.Background(), plan, changingProcessHarness{}, path)
	if !errors.Is(err, ErrInvariantViolation) {
		t.Fatalf("checker violation error = %v, want ErrInvariantViolation", err)
	}
	return result, path, catalog
}

func journalPath(t *testing.T, name string) string {
	t.Helper()
	return t.TempDir() + "/soak-" + name + ".jsonl"
}

const digest = "0000000000000000000000000000000000000000000000000000000000000000"

const stableManifestJSON = `{"schema_version":1,"schema_hash":"dec0f17c4a7ed5522fb5e135c896d61dc722feacbddfa52a69917043ce415c8b","parent_schema":null,"transition_class":"initial","compatibility_floor":1,"tables":[{"table_id":"00000000-0000-4000-8000-000000000030","relation_id":"00000000-0000-4000-8000-000000000034","name":"items","composition":"single_scope","primary_key_field_id":"00000000-0000-4000-8000-000000000031","lifecycle":{"created_at_field_id":null,"updated_at_field_id":null,"deleted_at_field_id":null},"fields":[{"field_id":"00000000-0000-4000-8000-000000000031","name":"id","type":"string","nullable":false,"writable":false},{"field_id":"00000000-0000-4000-8000-000000000033","name":"value","type":"string","nullable":false,"writable":true}],"indexes":[]}]}`

const (
	stableTableID       = "00000000-0000-4000-8000-000000000030"
	stablePKFieldID     = "00000000-0000-4000-8000-000000000031"
	stableValueFieldID  = "00000000-0000-4000-8000-000000000033"
	stableServerVersion = "00000000-0000-4000-8000-000000000050"
	stableSecondVersion = "00000000-0000-4000-8000-000000000051"
)

type emptyCaptureHarness struct{}

func (emptyCaptureHarness) Execute(context.Context, Operation) (ObservationCapture, error) {
	return ObservationCapture{}, nil
}

type noopExchangeHarness struct{}

func (noopExchangeHarness) Execute(_ context.Context, operation Operation) (ObservationCapture, error) {
	capture := captureForOperation(operation, "pid-noop")
	for index := range capture.WireExchanges {
		capture.WireExchanges[index].OperationClass = "noop"
		capture.WireExchanges[index].ExpectMutationConservation = false
		capture.WireExchanges[index].ExpectChecksumConvergence = false
		capture.WireExchanges[index].ExpectScopeIsolation = false
	}
	return capture, nil
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
	manifest, clients, serverState, serverRows, positions, checkpoints, rowDigest, scopeDigest := stableDurableCapture(operation, processID)
	capture := ObservationCapture{
		Manifest:            &manifest,
		ServerState:         &serverState,
		Operator:            &invariants.OperatorObservation{Checkpoints: checkpoints},
		Clients:             clients,
		CursorPositions:     positions,
		ServerRowIdentities: serverRows,
	}
	switch operation.Kind {
	case OperationPush:
		capture.WireExchanges = []invariants.WireExchangeObservation{stablePushExchange(operation)}
	case OperationPull:
		capture.WireExchanges, capture.PullResults, capture.CursorAcknowledgements = stablePullExchanges(operation, positions, rowDigest, scopeDigest)
	default:
		capture.WireExchanges = []invariants.WireExchangeObservation{{Sequence: 1, OperationClass: operationExchangeClass(operation.Kind), ResponseStatus: 200, RequestBody: mustJSON(map[string]any{"user_id": operation.UserID, "client_id": operation.ClientID, "scope_id": operation.ScopeID}), ResponseBody: []byte(`{}`)}}
	}
	if operation.FaultPlan != nil {
		capture.FaultActivation = &FaultActivationObservation{ControlID: string(operation.FaultPlan.ControlID), Target: operation.ClientID, Activated: true, CleanedUp: true}
	}
	if operation.Kind == OperationProcessDeath {
		capture.Clients[0].RestartBoundary = true
	}
	return capture
}

func stableDurableCapture(operation Operation, processID string) (vectors.Manifest, []invariants.ClientObservation, scenarios.StateFacts, []invariants.ServerRowIdentityObservation, []invariants.CursorPositionObservation, []invariants.OperatorCheckpointObservation, [32]byte, [32]byte) {
	manifest, err := vectors.ParseManifest(json.RawMessage(stableManifestJSON))
	if err != nil {
		panic(err)
	}
	row := vectors.Row{PK: json.RawMessage(`"row-authored"`), Fields: []vectors.RowField{{FieldID: stablePKFieldID, Value: json.RawMessage(`"row-authored"`)}, {FieldID: stableValueFieldID, Value: json.RawMessage(`"value-authored"`)}}}
	secondRow := vectors.Row{PK: json.RawMessage(`"row-existing"`), Fields: []vectors.RowField{{FieldID: stablePKFieldID, Value: json.RawMessage(`"row-existing"`)}, {FieldID: stableValueFieldID, Value: json.RawMessage(`"value-existing"`)}}}
	rowDigest, err := vectors.RowDigest(manifest, stableTableID, row, stableServerVersion)
	if err != nil {
		panic(err)
	}
	secondDigest, err := vectors.RowDigest(manifest, stableTableID, secondRow, stableSecondVersion)
	if err != nil {
		panic(err)
	}
	rowIdentity, err := vectors.RowIdentity(manifest, stableTableID, row.PK)
	if err != nil {
		panic(err)
	}
	secondIdentity, err := vectors.RowIdentity(manifest, stableTableID, secondRow.PK)
	if err != nil {
		panic(err)
	}
	selectedScope := operation.ScopeID
	other := otherScope(selectedScope)
	entries := []vectors.DigestEntry{{RowIdentity: rowIdentity, RowDigest: rowDigest}, {RowIdentity: secondIdentity, RowDigest: secondDigest}}
	scopeDigest, err := vectors.ScopeDigest(manifest.Hash(), selectedScope, entries)
	if err != nil {
		panic(err)
	}
	otherDigest, err := vectors.ScopeDigest(manifest.Hash(), other, entries)
	if err != nil {
		panic(err)
	}
	selectedCursor := "cursor-" + selectedScope
	otherCursor := "cursor-" + other
	positionA := pullPosition(operation, selectedScope, selectedCursor)
	positionB := pullPosition(operation, other, otherCursor)
	checkpoints := []invariants.OperatorCheckpointObservation{{UserID: operation.UserID, ClientID: operation.ClientID, ScopeID: positionA.ScopeID, StreamGeneration: "stream", Position: positionA.Position}, {UserID: operation.UserID, ClientID: operation.ClientID, ScopeID: positionB.ScopeID, StreamGeneration: "stream", Position: positionB.Position}}
	clients := []invariants.ClientObservation{{
		State:     scenarios.ClientDurabilityFact{UserID: operation.UserID, ClientID: operation.ClientID, Checkpoints: []scenarios.CheckpointFact{{ScopeID: selectedScope, HasCursor: true, HasChecksum: true, Verified: true}, {ScopeID: other, HasCursor: true, HasChecksum: true, Verified: true}}},
		Rows:      []invariants.ClientRowObservation{{TableID: stableTableID, Row: row, ServerVersion: stableServerVersion, StoredDigest: &rowDigest}, {TableID: stableTableID, Row: secondRow, ServerVersion: stableSecondVersion, StoredDigest: &secondDigest}},
		Scopes:    []invariants.ClientScopeObservation{{ScopeID: selectedScope, RawCursor: &selectedCursor, AuthoritativeDigest: &scopeDigest, LocalDigest: &scopeDigest, Generation: 4}, {ScopeID: other, RawCursor: &otherCursor, AuthoritativeDigest: &otherDigest, LocalDigest: &otherDigest, Generation: 4}},
		ScopeRows: []invariants.ClientScopeRowObservation{{ScopeID: selectedScope, Entry: vectors.DigestEntry{RowIdentity: rowIdentity, RowDigest: rowDigest}, Generation: 4}, {ScopeID: selectedScope, Entry: vectors.DigestEntry{RowIdentity: secondIdentity, RowDigest: secondDigest}, Generation: 4}, {ScopeID: other, Entry: vectors.DigestEntry{RowIdentity: rowIdentity, RowDigest: rowDigest}, Generation: 4}, {ScopeID: other, Entry: vectors.DigestEntry{RowIdentity: secondIdentity, RowDigest: secondDigest}, Generation: 4}},
		Process:   &invariants.ProcessIdentityObservation{ProcessID: processID, DatabaseIdentityFingerprint: digest},
		Complete:  true,
	}}
	serverState := scenarios.StateFacts{Scopes: []scenarios.ScopeFact{{ScopeID: selectedScope, MembershipGeneration: 4, Cardinality: 2}, {ScopeID: other, MembershipGeneration: 4, Cardinality: 2}}, RowScopeEdges: []scenarios.RowScopeEdgeFact{{TableID: stableTableID, CanonicalWireJSON: `"row-authored"`, ScopeID: selectedScope}, {TableID: stableTableID, CanonicalWireJSON: `"row-existing"`, ScopeID: selectedScope}, {TableID: stableTableID, CanonicalWireJSON: `"row-authored"`, ScopeID: other}, {TableID: stableTableID, CanonicalWireJSON: `"row-existing"`, ScopeID: other}}}
	serverRows := []invariants.ServerRowIdentityObservation{{TableID: stableTableID, CanonicalWireJSON: `"row-authored"`, RowIdentity: rowIdentity}, {TableID: stableTableID, CanonicalWireJSON: `"row-existing"`, RowIdentity: secondIdentity}}
	return manifest, clients, serverState, serverRows, []invariants.CursorPositionObservation{positionA, positionB}, checkpoints, rowDigest, scopeDigest
}

func stablePushExchange(operation Operation) invariants.WireExchangeObservation {
	schema := map[string]any{"version": 1, "hash": digest}
	mutationID := "00000000-0000-4000-8000-000000000060"
	batchID := "00000000-0000-4000-8000-000000000061"
	pk := map[string]any{stablePKFieldID: "row-push"}
	columns := map[string]any{stableValueFieldID: "value-push"}
	request := map[string]any{"authenticated_user_id": operation.UserID, "client_id": operation.ClientID, "scope_id": operation.ScopeID, "client_generation": 1, "batch_id": batchID, "schema": schema, "mutations": []any{map[string]any{"mutation_id": mutationID, "table": stableTableID, "pk": pk, "authored_schema": schema, "op": "insert", "client_version": "2032-01-02T03:04:05.000000Z", "columns": columns}}}
	outcome := map[string]any{"mutation_id": mutationID, "status": "applied", "table": stableTableID, "pk": pk, "outcome_schema": schema, "server_row": columns, "server_version": "00000000-0000-4000-8000-000000000063", "row_checksum": map[string]any{"algorithm": "sha256", "version": 1, "encoding": "hex", "digest": digest}}
	response := map[string]any{"batch_id": batchID, "accepted": []any{outcome}, "rejected": []any{}}
	return invariants.WireExchangeObservation{Sequence: 1, OperationClass: "push", ResponseStatus: 200, RequestBody: mustJSON(request), ResponseBody: mustJSON(response), ExpectMutationConservation: true}
}

func stablePullExchanges(operation Operation, positions []invariants.CursorPositionObservation, rowDigest, scopeDigest [32]byte) ([]invariants.WireExchangeObservation, []invariants.PullResultObservation, []invariants.CursorAcknowledgementObservation) {
	selected := operation.ScopeID
	other := otherScope(selected)
	selectedCursor := positions[0].RawCursor
	otherCursor := positions[1].RawCursor
	request := map[string]any{"user_id": operation.UserID, "client_id": operation.ClientID, "scopes": map[string]any{selected: map[string]any{"cursor": selectedCursor}, other: map[string]any{"cursor": otherCursor}}}
	row := map[string]any{stablePKFieldID: "row-authored", stableValueFieldID: "value-authored"}
	response := map[string]any{"changes": []any{map[string]any{"scope": selected, "table": stableTableID, "pk": map[string]any{stablePKFieldID: "row-authored"}, "row": row, "server_version": stableServerVersion, "row_checksum": map[string]any{"algorithm": "sha256", "version": 1, "encoding": "hex", "digest": hex.EncodeToString(rowDigest[:])}}}, "scope_cursors": map[string]any{selected: selectedCursor, other: otherCursor}, "scope_updates": map[string]any{"add": []any{}, "remove": []any{}}, "rebuild": []any{}, "has_more": false, "checksums": map[string]any{selected: map[string]any{"algorithm": "sha256", "version": 1, "encoding": "hex", "digest": hex.EncodeToString(scopeDigest[:])}}}
	zeroRequest := map[string]any{"user_id": operation.UserID, "client_id": operation.ClientID, "scopes": map[string]any{selected: map[string]any{"cursor": selectedCursor}}}
	zeroResponse := map[string]any{"changes": []any{}, "scope_cursors": map[string]any{selected: selectedCursor}, "scope_updates": map[string]any{"add": []any{}, "remove": []any{}}, "rebuild": []any{}, "has_more": false}
	checksumExchange := invariants.WireExchangeObservation{Sequence: 1, OperationClass: "pull", ResponseStatus: 200, RequestBody: mustJSON(request), ResponseBody: mustJSON(response), ExpectChecksumConvergence: true}
	scopeExchange := invariants.WireExchangeObservation{Sequence: 2, OperationClass: "pull", ResponseStatus: 200, RequestBody: mustJSON(zeroRequest), ResponseBody: mustJSON(zeroResponse), ExpectScopeIsolation: true}
	change := invariants.PullChangeIdentityObservation{ScopeID: selected, TableID: stableTableID, PrimaryKeyFieldID: stablePKFieldID, PrimaryKey: json.RawMessage(`"row-authored"`)}
	result := invariants.PullResultObservation{ExchangeSequence: 1, UserID: operation.UserID, ClientID: operation.ClientID, Changes: []invariants.PullChangeIdentityObservation{change}, Cursors: append([]invariants.CursorPositionObservation(nil), positions...)}
	acknowledgements := []invariants.CursorAcknowledgementObservation{{ExchangeSequence: 1, Cursor: positions[0]}, {ExchangeSequence: 1, Cursor: positions[1]}}
	return []invariants.WireExchangeObservation{checksumExchange, scopeExchange}, []invariants.PullResultObservation{result}, acknowledgements
}

func mustJSON(value any) []byte {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return encoded
}

func pullPosition(operation Operation, scopeID, cursor string) invariants.CursorPositionObservation {
	return invariants.CursorPositionObservation{UserID: operation.UserID, ClientID: operation.ClientID, ScopeID: scopeID, Generation: 4, RawCursor: cursor, StreamGeneration: "stream", Position: invariants.PositionObservation{Kind: "transaction_end", CommitLSN: stringPointer("0/1")}}
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

func tamperJournalRecord(data []byte, recordType string, edit func(*journalRecord)) []byte {
	lines := bytes.Split(data, []byte{'\n'})
	for index, line := range lines {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var record journalRecord
		if err := json.Unmarshal(line, &record); err != nil || record.Type != recordType {
			continue
		}
		edit(&record)
		encoded, err := json.Marshal(record)
		if err != nil {
			panic(err)
		}
		lines[index] = encoded
		return bytes.Join(lines, []byte{'\n'})
	}
	panic("journal record type not found")
}
