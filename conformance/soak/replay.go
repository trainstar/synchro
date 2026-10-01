package soak

import "fmt"

// StageError identifies a harness failure by the operation stage that failed
// and a bounded failure class. Replay compares these instead of error text,
// which can hold request data or cluster-specific identifiers. Each value uses
// only lowercase letters, digits, and hyphens.
type StageError struct {
	Stage string
	Class string
	Err   error
}

func (e *StageError) Error() string {
	return fmt.Sprintf("soak %s failed (%s): %v", e.Stage, e.Class, e.Err)
}

func (e *StageError) Unwrap() error { return e.Err }

// ReplayOutcome classifies one replay against its retained journal.
type ReplayOutcome string

const (
	// ReplayReproduced means the replay produced the same retained outcome.
	ReplayReproduced ReplayOutcome = "reproduced"
	// ReplayDiverged means the replay produced a different outcome.
	ReplayDiverged ReplayOutcome = "diverged"
	// ReplayInconclusive means the retained failure has no precise identity,
	// so equal failure codes cannot show that replay reached the same behavior.
	ReplayInconclusive ReplayOutcome = "inconclusive"
)

// CompareReplay compares the retained terminal failure with the replayed one.
// A nil retained fact means the retained run completed.
func CompareReplay(retained, replayed *OperationFact) ReplayOutcome {
	if retained == nil || replayed == nil {
		if retained == nil && replayed == nil {
			return ReplayReproduced
		}
		return ReplayDiverged
	}
	if retained.Sequence != replayed.Sequence || retained.FailureCode != replayed.FailureCode {
		return ReplayDiverged
	}
	switch retained.FailureCode {
	case "invariant-violation":
		if retained.ViolationCount == replayed.ViolationCount && retained.ViolationDigest == replayed.ViolationDigest {
			return ReplayReproduced
		}
		return ReplayDiverged
	case "harness-error":
		if retained.FailureStage == "" {
			return ReplayInconclusive
		}
		if retained.FailureStage == replayed.FailureStage && retained.FailureClass == replayed.FailureClass {
			return ReplayReproduced
		}
		return ReplayDiverged
	default:
		return ReplayInconclusive
	}
}
