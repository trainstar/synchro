package soak

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/trainstar/synchro/conformance/vectors"
)

func TestCompareReplayRequiresTheSameFailureIdentity(t *testing.T) {
	violation := func(count int, digest string) *OperationFact {
		return &OperationFact{Sequence: 6, Status: "failed", FailureCode: "invariant-violation", ViolationCount: count, ViolationDigest: digest}
	}
	harness := func(stage, class string) *OperationFact {
		return &OperationFact{Sequence: 6, Status: "failed", FailureCode: "harness-error", FailureStage: stage, FailureClass: class}
	}
	digestA, digestB := fmt.Sprintf("%064d", 1), fmt.Sprintf("%064d", 2)
	tests := []struct {
		name               string
		retained, replayed *OperationFact
		want               ReplayOutcome
	}{
		{"completed runs", nil, nil, ReplayReproduced},
		{"completed run now fails", nil, violation(1, digestA), ReplayDiverged},
		{"failed run now completes", violation(1, digestA), nil, ReplayDiverged},
		{"same violation set", violation(3, digestA), violation(3, digestA), ReplayReproduced},
		{"different violation set", violation(3, digestA), violation(3, digestB), ReplayDiverged},
		{"different operation", violation(3, digestA), &OperationFact{Sequence: 7, Status: "failed", FailureCode: "invariant-violation", ViolationCount: 3, ViolationDigest: digestA}, ReplayDiverged},
		{"same harness stage and class", harness("wal-restart", "rejected"), harness("wal-restart", "rejected"), ReplayReproduced},
		{"different harness stage at the same operation", harness("wal-restart", "rejected"), harness("drain-pull", "transport"), ReplayDiverged},
		{"different harness class at the same stage", harness("drain-pull", "rejected"), harness("drain-pull", "transport"), ReplayDiverged},
		{"unidentified harness failure", harness("", ""), harness("", ""), ReplayInconclusive},
		{"capture failure", &OperationFact{Sequence: 6, Status: "failed", FailureCode: "capture-incomplete"}, &OperationFact{Sequence: 6, Status: "failed", FailureCode: "capture-incomplete"}, ReplayInconclusive},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := CompareReplay(test.retained, test.replayed); got != test.want {
				t.Fatalf("replay outcome = %s, want %s", got, test.want)
			}
		})
	}
}

// Two different harness failures at the same operation keep different
// identities in the journal, so a replay that fails elsewhere is not reported
// as a reproduction. An unidentified failure is inconclusive.
func TestRunRetainsHarnessFailureStageAndClass(t *testing.T) {
	catalog := testCatalog(t)
	plan, err := Generate(8, Config{OperationCount: MinimumCoverageOperations}, catalog)
	if err != nil {
		t.Fatalf("generate plan: %v", err)
	}
	path := journalPath(t, "stage-failure")
	walFailure := failingHarness{err: &StageError{Stage: "wal-restart", Class: "rejected", Err: errors.New("replay boundary missing")}}
	if _, err := Run(context.Background(), plan, walFailure, path); err == nil {
		t.Fatal("failing harness completed")
	}
	journal, err := ReadJournal(path)
	if !errors.Is(err, ErrJournalUnsealed) {
		t.Fatalf("read failed journal: %v", err)
	}
	retained := journal.OperationFacts[len(journal.OperationFacts)-1]
	if retained.FailureStage != "wal-restart" || retained.FailureClass != "rejected" {
		t.Fatalf("retained failure identity = %#v", retained)
	}
	for _, test := range []struct {
		name    string
		harness Harness
		want    ReplayOutcome
	}{
		{"same failure", walFailure, ReplayReproduced},
		{"transport failure at the same operation", failingHarness{err: &StageError{Stage: "drain-pull", Class: "transport", Err: errors.New("connection reset")}}, ReplayDiverged},
	} {
		t.Run(test.name, func(t *testing.T) {
			replayed, _ := ReplayRun(context.Background(), path, catalog, test.harness)
			if got := CompareReplay(&retained, replayed.Failure); got != test.want {
				t.Fatalf("replay outcome = %s, want %s", got, test.want)
			}
		})
	}
	unidentified := journalPath(t, "unidentified-failure")
	if _, err := Run(context.Background(), plan, failingHarness{err: errors.New("unclassified")}, unidentified); err == nil {
		t.Fatal("failing harness completed")
	}
	journal, _ = ReadJournal(unidentified)
	fact := journal.OperationFacts[len(journal.OperationFacts)-1]
	replayed, _ := ReplayRun(context.Background(), unidentified, catalog, failingHarness{err: errors.New("unclassified")})
	if got := CompareReplay(&fact, replayed.Failure); got != ReplayInconclusive {
		t.Fatalf("unidentified failure replay outcome = %s, want inconclusive", got)
	}
}

// More violations than the diagnostic sample still retain one terminal fact
// whose count and digest identify the complete set, and the journal replays.
func TestRunRetainsTerminalFactAboveTheViolationSample(t *testing.T) {
	const unauthorizedRows = 300
	catalog := testCatalog(t)
	plan, err := Generate(9, Config{OperationCount: MinimumCoverageOperations}, catalog)
	if err != nil {
		t.Fatalf("generate plan: %v", err)
	}
	path := journalPath(t, "many-violations")
	harness := unauthorizedRowsHarness{rows: unauthorizedRows}
	result, err := Run(context.Background(), plan, harness, path)
	if !errors.Is(err, ErrInvariantViolation) || len(result.Violations) <= 256 {
		t.Fatalf("large violation run error = %v, violations = %d", err, len(result.Violations))
	}
	journal, err := ReadJournal(path)
	if !errors.Is(err, ErrJournalUnsealed) {
		t.Fatalf("read large violation journal: %v", err)
	}
	retained := journal.OperationFacts[len(journal.OperationFacts)-1]
	if retained.Status != "failed" || retained.ViolationCount != len(result.Violations) || len(retained.Violations) != MaximumFailureViolationSample {
		t.Fatalf("retained fact count = %d sample = %d, want %d and %d", retained.ViolationCount, len(retained.Violations), len(result.Violations), MaximumFailureViolationSample)
	}
	replayed, _ := ReplayRun(context.Background(), path, catalog, harness)
	if got := CompareReplay(&retained, replayed.Failure); got != ReplayReproduced {
		t.Fatalf("large violation replay outcome = %s, want reproduced", got)
	}
	replayed, _ = ReplayRun(context.Background(), path, catalog, unauthorizedRowsHarness{rows: unauthorizedRows + 1})
	if got := CompareReplay(&retained, replayed.Failure); got != ReplayDiverged {
		t.Fatalf("different violation set replay outcome = %s, want diverged", got)
	}
}

type failingHarness struct{ err error }

func (h failingHarness) Execute(context.Context, Operation) (ObservationCapture, error) {
	return ObservationCapture{}, h.err
}

type unauthorizedRowsHarness struct{ rows int }

func (h unauthorizedRowsHarness) Execute(_ context.Context, operation Operation) (ObservationCapture, error) {
	capture := captureForOperation(operation, "pid-stable")
	for index := range h.rows {
		pk := json.RawMessage(fmt.Sprintf(`"row-unauthorized-%04d"`, index))
		addClientRow(&capture, vectors.Row{PK: pk, Fields: []vectors.RowField{{FieldID: stablePKFieldID, Value: pk}, {FieldID: stableValueFieldID, Value: json.RawMessage(`"value"`)}}}, "")
	}
	return capture, nil
}
