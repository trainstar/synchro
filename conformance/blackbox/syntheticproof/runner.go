package syntheticproof

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/execution"
	"github.com/trainstar/synchro/conformance/modelrunner"
	"github.com/trainstar/synchro/conformance/scenarios"
)

var (
	// ErrRunInput reports a run request that is not bound to the scenario.
	ErrRunInput = errors.New("black-box run input is invalid")
	// ErrRunProtocol reports malformed or non-closed HTTP data.
	ErrRunProtocol = errors.New("black-box response protocol is invalid")
	// ErrRunTransport reports a failed raw HTTP operation.
	ErrRunTransport = errors.New("black-box HTTP execution failed")
	// ErrRunReference reports a reference-model execution failure.
	ErrRunReference = errors.New("black-box reference execution failed")
)

// FailureKind identifies the terminal phase that rejected a run.
type FailureKind string

const (
	FailureNone      FailureKind = ""
	FailureSemantic  FailureKind = "semantic"
	FailureProtocol  FailureKind = "protocol"
	FailureTransport FailureKind = "transport"
	FailureReference FailureKind = "reference"
)

// RunFailure contains bounded terminal failure metadata.
type RunFailure struct {
	Kind      FailureKind   `json:"kind"`
	Assertion AssertionName `json:"assertion,omitempty"`
	StepID    string        `json:"step_id,omitempty"`
	Reason    string        `json:"reason"`
}

// RunResult contains non-evidence diagnostics from one completed run.
type RunResult struct {
	Passed               bool                        `json:"passed"`
	ExitCode             int                         `json:"exit_code"`
	Result               execution.Result            `json:"result"`
	Failure              RunFailure                  `json:"failure"`
	Checks               []AssertionCheck            `json:"checks"`
	Exchanges            []blackbox.ExchangeMetadata `json:"exchanges"`
	PrivateAttachmentIDs []string                    `json:"private_attachment_ids"`
	AttachmentIDs        []string                    `json:"attachment_ids"`
}

// RunError reports a completed run that did not pass.
type RunError struct {
	Failure RunFailure
	cause   error
}

func (e *RunError) Error() string {
	if e == nil {
		return "black-box run failed"
	}
	if e.Failure.StepID != "" {
		return fmt.Sprintf("black-box run %s at step %s: %s", e.Failure.Kind, e.Failure.StepID, e.Failure.Reason)
	}
	return fmt.Sprintf("black-box run %s: %s", e.Failure.Kind, e.Failure.Reason)
}

func (e *RunError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// RunnerConfig supplies one raw client and authored execution bindings.
type RunnerConfig struct {
	Client           *blackbox.Client
	Recorder         blackbox.RecorderConfig
	ArtifactBindings []execution.ArtifactBinding
	VectorResults    []execution.VectorResult
	Now              func() time.Time
}

// Runner is one isolated full-run entry point.
type Runner struct {
	runMu            sync.Mutex
	client           blackbox.Client
	recorder         *blackbox.Recorder
	artifactBindings []execution.ArtifactBinding
	vectorResults    []execution.VectorResult
	now              func() time.Time
}

// NewRunner creates one isolated runner.
func NewRunner(config RunnerConfig) (*Runner, error) {
	if config.Client == nil || config.Client.BaseURL == "" || config.Client.Tokens == nil {
		return nil, errors.New("runner raw HTTP client is incomplete")
	}
	recorder, err := blackbox.NewRecorder(config.Recorder)
	if err != nil {
		return nil, err
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	client := config.Client.WithRecorder(recorder, config.Recorder.MaxRawBodyBytes)
	return &Runner{
		client:           client,
		recorder:         recorder,
		artifactBindings: append([]execution.ArtifactBinding(nil), config.ArtifactBindings...),
		vectorResults:    append([]execution.VectorResult(nil), config.VectorResults...),
		now:              now,
	}, nil
}

// Run executes setup and all operations through raw loopback HTTP.
func (r *Runner) Run(ctx context.Context, scenario scenarios.Scenario, obligation scenarios.ProofObligation) (RunResult, error) {
	if r == nil {
		return RunResult{}, errors.New("runner is required")
	}
	if ctx == nil {
		return RunResult{}, fmt.Errorf("%w: context", ErrRunInput)
	}
	if err := ctx.Err(); err != nil {
		return RunResult{}, err
	}
	if err := validateRunRequest(scenario, obligation); err != nil {
		return RunResult{}, err
	}
	if err := r.validateBindings(obligation); err != nil {
		return RunResult{}, err
	}

	r.runMu.Lock()
	defer r.runMu.Unlock()
	started := r.now().Round(0).UTC()
	recordOffset := r.recorder.Len()
	result := RunResult{ExitCode: 1, Result: execution.ResultError}
	runErr := r.executeScenario(ctx, scenario, &result)
	records, recordErr := r.recorder.Snapshot(recordOffset)
	if recordErr != nil && runErr == nil {
		result.Failure = RunFailure{Kind: FailureTransport, Reason: "bounded metadata collection failed"}
		runErr = &RunError{Failure: result.Failure, cause: ErrRunTransport}
	}
	result.Exchanges = records
	result.PrivateAttachmentIDs = attachmentIDs(records)
	if runErr == nil {
		result.Passed = true
		result.ExitCode = 0
		result.Result = execution.ResultPassed
		result.Failure = RunFailure{}
	} else if result.Failure.Kind == FailureSemantic {
		result.Result = execution.ResultFailed
	}
	completed := r.now().Round(0).UTC()
	if completed.Before(started) {
		result.Passed = false
		result.ExitCode = 1
		result.Result = execution.ResultError
		result.Failure = RunFailure{Kind: FailureReference, Reason: "runner clock moved backward"}
		runErr = &RunError{Failure: result.Failure, cause: ErrRunReference}
	}
	if runErr != nil {
		return result, runErr
	}
	return result, nil
}

func (r *Runner) executeScenario(ctx context.Context, scenario scenarios.Scenario, result *RunResult) error {
	expected, err := modelrunner.RunScenario(ctx, scenario)
	if err != nil || !expected.Passed || len(expected.Setup) != 1 || len(expected.Steps) != len(scenario.Steps) {
		result.Failure = RunFailure{Kind: FailureReference, Reason: "reference model did not complete"}
		return &RunError{Failure: result.Failure, cause: ErrRunReference}
	}
	executions := make([]syntheticExecution, 0, len(expected.Steps)+1)
	executions = append(executions, syntheticExecution{
		stepID:    syntheticSetupStepID,
		operation: cloneSyntheticOperation(expected.Setup[0].Operation),
		result:    expected.Setup[0].Result,
	})
	for index, item := range expected.Steps {
		executions = append(executions, syntheticExecution{
			stepID:    string(scenario.Steps[index].ID),
			operation: cloneSyntheticOperation(item.Operation),
			result:    item.Result,
		})
	}
	for _, item := range executions {
		if err := ctx.Err(); err != nil {
			result.Failure = RunFailure{Kind: FailureTransport, StepID: item.stepID, Reason: "run context ended"}
			return &RunError{Failure: result.Failure, cause: err}
		}
		if err := r.executeOperation(ctx, string(scenario.ID), item, result); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) executeOperation(ctx context.Context, scenarioID string, item syntheticExecution, result *RunResult) error {
	body, err := syntheticRequestBody(scenarioID, item.stepID, item.operation)
	if err != nil {
		result.Failure = RunFailure{Kind: FailureProtocol, StepID: item.stepID, Reason: "request encoding failed"}
		return &RunError{Failure: result.Failure, cause: ErrRunProtocol}
	}
	request := blackbox.Request{
		Method: http.MethodPost,
		Path:   syntheticExecutePath,
		Headers: http.Header{
			"Content-Type":    []string{"application/json"},
			"Idempotency-Key": []string{scenarioID + ":" + item.stepID},
		},
		Body:  body,
		Class: scenarios.OperationKey(item.operation),
	}
	response, err := r.client.Do(ctx, request)
	if err != nil {
		result.Failure = RunFailure{Kind: FailureTransport, StepID: item.stepID, Reason: "raw HTTP request failed"}
		return &RunError{Failure: result.Failure, cause: ErrRunTransport}
	}
	observed, err := decodeWireEnvelope(response.Body)
	if err != nil {
		result.Failure = RunFailure{Kind: FailureProtocol, StepID: item.stepID, Reason: "strict response decoding failed"}
		return &RunError{Failure: result.Failure, cause: ErrRunProtocol}
	}
	expected := wireEnvelope{
		SchemaVersion: 1,
		ScenarioID:    scenarioID,
		StepID:        item.stepID,
		OperationKey:  scenarios.OperationKey(item.operation),
		RequestID:     "reference-model-dynamic",
		OpaqueValue:   syntheticOpaqueValue(scenarioID, item.stepID),
		Result:        item.result,
	}
	checks, compareErr := compareWireSemantics(expected, observed, response.Status)
	for index := range checks {
		checks[index].StepID = item.stepID
	}
	result.Checks = append(result.Checks, checks...)
	if compareErr != nil {
		var comparison *ComparisonFailure
		assertion := AssertionSemanticResponse
		if errors.As(compareErr, &comparison) {
			assertion = comparison.Assertion
		}
		result.Failure = RunFailure{Kind: FailureSemantic, Assertion: assertion, StepID: item.stepID, Reason: "semantic response assertion failed"}
		return &RunError{Failure: result.Failure, cause: compareErr}
	}
	if replayEligible(item.operation) {
		replay, err := r.client.Do(ctx, request)
		if err != nil {
			result.Failure = RunFailure{Kind: FailureTransport, StepID: item.stepID, Reason: "replay HTTP request failed"}
			return &RunError{Failure: result.Failure, cause: ErrRunTransport}
		}
		if _, err := decodeWireEnvelope(replay.Body); err != nil {
			result.Failure = RunFailure{Kind: FailureProtocol, StepID: item.stepID, Reason: "strict replay decoding failed"}
			return &RunError{Failure: result.Failure, cause: ErrRunProtocol}
		}
		if err := blackbox.CompareExactReplay(response, replay); err != nil {
			result.Checks = append(result.Checks, AssertionCheck{Name: AssertionExactReplay, StepID: item.stepID, Passed: false, Reason: "exact replay changed"})
			result.Failure = RunFailure{Kind: FailureSemantic, Assertion: AssertionExactReplay, StepID: item.stepID, Reason: "exact replay assertion failed"}
			return &RunError{Failure: result.Failure, cause: err}
		}
		result.Checks = append(result.Checks, AssertionCheck{Name: AssertionExactReplay, StepID: item.stepID, Passed: true, Reason: "semantic assertion passed"})
	}
	return nil
}

func decodeWireEnvelope(body []byte) (wireEnvelope, error) {
	var envelope wireEnvelope
	if err := blackbox.DecodeStrictResponse(body, &envelope); err != nil {
		return wireEnvelope{}, err
	}
	if envelope.SchemaVersion != 1 || envelope.ScenarioID == "" || envelope.StepID == "" || envelope.OperationKey == "" || envelope.RequestID == "" || envelope.OpaqueValue == "" || envelope.Result.Kind == "" {
		return wireEnvelope{}, errors.New("strict response has an incomplete closed envelope")
	}
	return envelope, nil
}

func replayEligible(operation scenarios.Operation) bool {
	switch scenarios.OperationKey(operation) {
	case "push/submit", "rebuild/request-page":
		return true
	default:
		return false
	}
}

func validateRunRequest(scenario scenarios.Scenario, obligation scenarios.ProofObligation) error {
	if scenario.ID == "" || obligation.ObligationID == "" || len(obligation.AssertionIDs) == 0 || obligation.MakeTarget == "" || len(obligation.Argv) != 2 || obligation.Argv[0] != "make" || obligation.Argv[1] != obligation.MakeTarget {
		return fmt.Errorf("%w: scenario or obligation", ErrRunInput)
	}
	found := false
	for _, authored := range scenario.ProofObligations {
		if authored.ObligationID == obligation.ObligationID {
			found = reflect.DeepEqual(authored, obligation)
			break
		}
	}
	if !found {
		return fmt.Errorf("%w: obligation is not authored by the scenario", ErrRunInput)
	}
	return nil
}

func (r *Runner) validateBindings(obligation scenarios.ProofObligation) error {
	wantedArtifacts := make(map[string]struct{}, len(obligation.ArtifactInventoryIDs))
	for _, id := range obligation.ArtifactInventoryIDs {
		wantedArtifacts[string(id)] = struct{}{}
	}
	seenArtifacts := make(map[string]struct{}, len(wantedArtifacts))
	for _, binding := range r.artifactBindings {
		if _, found := wantedArtifacts[binding.InventoryID]; !found {
			return fmt.Errorf("%w: artifact binding", ErrRunInput)
		}
		seenArtifacts[binding.InventoryID] = struct{}{}
	}
	if len(seenArtifacts) != len(wantedArtifacts) {
		return fmt.Errorf("%w: artifact binding count", ErrRunInput)
	}
	wantedVectors := make(map[string]struct{}, len(obligation.RequiredVectorSetIDs))
	for _, id := range obligation.RequiredVectorSetIDs {
		wantedVectors[string(id)] = struct{}{}
	}
	seenVectors := make(map[string]struct{}, len(wantedVectors))
	for _, vector := range r.vectorResults {
		if _, found := wantedVectors[vector.VectorSetID]; !found {
			return fmt.Errorf("%w: vector binding", ErrRunInput)
		}
		seenVectors[vector.VectorSetID] = struct{}{}
	}
	if len(seenVectors) != len(wantedVectors) {
		return fmt.Errorf("%w: vector binding count", ErrRunInput)
	}
	return nil
}

func attachmentIDs(records []blackbox.ExchangeMetadata) []string {
	seen := make(map[string]struct{}, len(records)*2)
	result := make([]string, 0, len(records)*2)
	for _, record := range records {
		for _, id := range []string{record.RequestAttachmentID, record.ResponseAttachmentID} {
			if _, found := seen[id]; found {
				continue
			}
			seen[id] = struct{}{}
			result = append(result, id)
		}
	}
	return result
}
