// Package modelrunner executes authored protocol version 3 scenarios against
// the independent reference model.
package modelrunner

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/trainstar/synchro/conformance/reference"
	"github.com/trainstar/synchro/conformance/scenarios"
)

// RunErrorKind identifies the phase that rejected a model run.
type RunErrorKind string

const (
	RunErrorSetup                RunErrorKind = "setup"
	RunErrorStep                 RunErrorKind = "step"
	RunErrorOperation            RunErrorKind = "operation"
	RunErrorExpectedOutcome      RunErrorKind = "expected_outcome"
	RunErrorPredicate            RunErrorKind = "predicate"
	RunErrorNegativeControl      RunErrorKind = "negative_control"
	RunErrorResolvedInput        RunErrorKind = "resolved_input"
	RunErrorApplyResolvedMissing RunErrorKind = "apply_resolved_missing"
)

// RunError reports one fail-closed model-run failure.
type RunError struct {
	Kind         RunErrorKind
	StepID       scenarios.StepID
	OperationKey string
	Expectation  scenarios.ExpectationID
	AssertionID  string
	ExpectedCode string
	ActualCode   string
	Err          error
}

func (e *RunError) Error() string {
	if e == nil {
		return "<nil>"
	}
	location := ""
	if e.StepID != "" {
		location += fmt.Sprintf(" step %s", e.StepID)
	}
	if e.OperationKey != "" {
		location += fmt.Sprintf(" operation %s", e.OperationKey)
	}
	if e.Expectation != "" {
		location += fmt.Sprintf(" expectation %s", e.Expectation)
	}
	if e.Err == nil {
		return fmt.Sprintf("model run %s%s", e.Kind, location)
	}
	return fmt.Sprintf("model run %s%s: %v", e.Kind, location, e.Err)
}

func (e *RunError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// OperationExecution records a typed operation and both adjacent model
// snapshots. Error values are retained for diagnostics and are not replay
// inputs.
type OperationExecution struct {
	StepID       scenarios.StepID
	Operation    scenarios.Operation
	OperationKey string
	Result       reference.StepResult
	Err          error
	Before       reference.StateSnapshot
	After        reference.StateSnapshot
	Expanded     []scenarios.Operation
	Samples      []WorkloadSampleExecution
}

// WorkloadSampleFamily identifies one configured-limit family.
type WorkloadSampleFamily string

const (
	WorkloadSampleFanout     WorkloadSampleFamily = "fanout"
	WorkloadSampleImpact     WorkloadSampleFamily = "impact"
	WorkloadSamplePull       WorkloadSampleFamily = "pull"
	WorkloadSampleRebuild    WorkloadSampleFamily = "rebuild"
	WorkloadSampleCompaction WorkloadSampleFamily = "compaction"
	WorkloadSampleBackfill   WorkloadSampleFamily = "backfill"
	WorkloadSamplePush       WorkloadSampleFamily = "push"
)

// WorkloadSampleBoundary identifies one limit boundary.
type WorkloadSampleBoundary string

const (
	WorkloadBoundaryLower   WorkloadSampleBoundary = "lower"
	WorkloadBoundaryUpper   WorkloadSampleBoundary = "upper"
	WorkloadBoundaryInvalid WorkloadSampleBoundary = "invalid"
)

// WorkloadSampleExecution records one sampled expanded operation. A record
// contains either Result or ErrorCode.
type WorkloadSampleExecution struct {
	Family                 WorkloadSampleFamily
	Boundary               WorkloadSampleBoundary
	Value                  uint64
	ExpandedOperationIndex int
	Result                 *reference.StepResult
	ErrorCode              string
	Before                 reference.StateSnapshot
	After                  reference.StateSnapshot
}

// PredicateResult records one semantic predicate evaluation.
type PredicateResult struct {
	ExpectationID scenarios.ExpectationID
	AssertionID   string
	Name          string
	Passed        bool
	Reason        string
}

// ReplayOperation is the deterministic operation record used to reproduce a
// run without dynamic token values.
type ReplayOperation struct {
	StepID       scenarios.StepID `json:"step_id"`
	OperationKey string           `json:"operation_key"`
	Payload      []byte           `json:"payload"`
}

// ReplayData is deterministic input data for one model run.
type ReplayData struct {
	Seed       int64             `json:"seed"`
	Operations []ReplayOperation `json:"operations"`
	SHA256     string            `json:"sha256"`
	StateMatch bool              `json:"state_match"`
}

// Result contains the setup, ordered steps, snapshots, and predicate results
// for one model run.
type Result struct {
	ScenarioID    string                  `json:"scenario_id"`
	Setup         []OperationExecution    `json:"-"`
	Steps         []OperationExecution    `json:"-"`
	SetupSnapshot reference.StateSnapshot `json:"-"`
	FinalSnapshot reference.StateSnapshot `json:"-"`
	Predicates    []PredicateResult       `json:"predicates"`
	Replay        ReplayData              `json:"replay"`
	Passed        bool                    `json:"passed"`
}

func hashReplay(operations []ReplayOperation) string {
	data := make([]byte, 0)
	for _, operation := range operations {
		data = append(data, []byte(operation.StepID)...)
		data = append(data, 0)
		data = append(data, []byte(operation.OperationKey)...)
		data = append(data, 0)
		data = append(data, operation.Payload...)
		data = append(data, 0)
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}
