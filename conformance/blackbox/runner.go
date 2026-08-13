package blackbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sync"
	"time"

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
	// ErrRunCommand reports an evidence-authorizing Make invocation that did not pass.
	ErrRunCommand = errors.New("black-box evidence command failed")
)

// FailureKind identifies the terminal phase that rejected a run.
type FailureKind string

const (
	FailureNone      FailureKind = ""
	FailureSemantic  FailureKind = "semantic"
	FailureProtocol  FailureKind = "protocol"
	FailureTransport FailureKind = "transport"
	FailureReference FailureKind = "reference"
	FailureCommand   FailureKind = "command"
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
	Passed               bool               `json:"passed"`
	ExitCode             int                `json:"exit_code"`
	Result               execution.Result   `json:"result"`
	Failure              RunFailure         `json:"failure"`
	Checks               []AssertionCheck   `json:"checks"`
	Exchanges            []ExchangeMetadata `json:"exchanges"`
	PrivateAttachmentIDs []string           `json:"private_attachment_ids"`
	AttachmentIDs        []string           `json:"attachment_ids"`
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

// RunnerConfig supplies one raw client and closed receipt bindings.
type RunnerConfig struct {
	Client                 *Client
	Recorder               RecorderConfig
	ArtifactBindings       []execution.ArtifactBinding
	RunnerArtifactBindings []execution.ArtifactBinding
	ScenarioID             string
	ScenarioSHA256         string
	EnvironmentDimensions  []execution.EnvironmentDimension
	VectorResults          []execution.VectorResult
	AttachmentPublisher    execution.AttachmentPublisher
	CommandCapability      execution.CommandCapability
	TrustedRunner          execution.TrustedRunner
	RunID                  string
	ExecutionLineageID     string
	RunURL                 string
	Attempt                int
	Now                    func() time.Time
}

// Runner is the only full-run and receipt-completion entry point.
type Runner struct {
	runMu                  sync.Mutex
	client                 Client
	recorder               *Recorder
	artifactBindings       []execution.ArtifactBinding
	runnerArtifactBindings []execution.ArtifactBinding
	scenarioID             string
	scenarioSHA256         string
	environmentDimensions  []execution.EnvironmentDimension
	vectorResults          []execution.VectorResult
	attachmentPublisher    execution.AttachmentPublisher
	commandCapability      execution.CommandCapability
	now                    func() time.Time
	trustedRunner          execution.TrustedRunner
	runnerDigest           string
	runID                  string
	executionLineageID     string
	runURL                 string
	attempt                int
}

// NewRunner creates one isolated runner bound to the trusted completion key.
func NewRunner(config RunnerConfig) (*Runner, error) {
	if config.Client == nil || config.Client.BaseURL == "" || config.Client.Tokens == nil || config.TrustedRunner.RunnerDigest() == "" {
		return nil, errors.New("runner raw HTTP client is incomplete")
	}
	recorder, err := NewRecorder(config.Recorder)
	if err != nil {
		return nil, err
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	client := *config.Client
	client.recorder = recorder
	client.requestBodyLimit = config.Recorder.MaxRawBodyBytes
	client.responseBodyLimit = config.Recorder.MaxRawBodyBytes
	return &Runner{
		client:                 client,
		recorder:               recorder,
		artifactBindings:       append([]execution.ArtifactBinding(nil), config.ArtifactBindings...),
		runnerArtifactBindings: append([]execution.ArtifactBinding(nil), config.RunnerArtifactBindings...),
		scenarioID:             config.ScenarioID,
		scenarioSHA256:         config.ScenarioSHA256,
		environmentDimensions:  append([]execution.EnvironmentDimension(nil), config.EnvironmentDimensions...),
		vectorResults:          append([]execution.VectorResult(nil), config.VectorResults...),
		attachmentPublisher:    config.AttachmentPublisher,
		commandCapability:      config.CommandCapability,
		now:                    now,
		trustedRunner:          config.TrustedRunner,
		runnerDigest:           config.TrustedRunner.RunnerDigest(),
		runID:                  config.RunID,
		executionLineageID:     config.ExecutionLineageID,
		runURL:                 config.RunURL,
		attempt:                config.Attempt,
	}, nil
}

// ReceiptVerificationKey returns the public key for builder-owned issuers.
func (r *Runner) ReceiptVerificationKey() []byte {
	return nil
}

// NewReceiptIssuer creates one single-use issuer bound to this runner.
func (r *Runner) NewReceiptIssuer() (execution.ReceiptIssuer, error) {
	if r == nil || r.runnerDigest == "" {
		return execution.ReceiptIssuer{}, errors.New("runner is not initialized")
	}
	return r.trustedRunner.NewReceiptIssuer()
}

// Run executes setup and all operations through raw loopback HTTP.
// It completes one immutable receipt after all terminal collection work.
func (r *Runner) Run(ctx context.Context, scenario scenarios.Scenario, obligation scenarios.ProofObligation, issuer execution.ReceiptIssuer) (execution.Receipt, RunResult, error) {
	if r == nil {
		return execution.Receipt{}, RunResult{}, errors.New("runner is required")
	}
	if ctx == nil {
		return execution.Receipt{}, RunResult{}, fmt.Errorf("%w: context", ErrRunInput)
	}
	if err := ctx.Err(); err != nil {
		return execution.Receipt{}, RunResult{}, err
	}
	if err := validateRunRequest(scenario, obligation); err != nil {
		return execution.Receipt{}, RunResult{}, err
	}
	if err := r.validateBindings(obligation); err != nil {
		return execution.Receipt{}, RunResult{}, err
	}
	if issuer.RunnerDigest() == "" || issuer.Used() {
		return execution.Receipt{}, RunResult{}, fmt.Errorf("%w: receipt issuer", ErrRunInput)
	}
	if issuer.RunnerDigest() != r.runnerDigest {
		return execution.Receipt{}, RunResult{}, fmt.Errorf("%w: receipt issuer runner", ErrRunInput)
	}
	if issuer.AuthorizesEvidence() {
		if err := r.validateEvidenceIssuer(issuer); err != nil {
			return execution.Receipt{}, RunResult{}, err
		}
		observedScenarioSHA256 := scenarios.SHA256(scenario)
		if r.scenarioID != string(scenario.ID) || r.scenarioSHA256 == "" || observedScenarioSHA256 != r.scenarioSHA256 {
			return execution.Receipt{}, RunResult{}, fmt.Errorf("%w: locked scenario", ErrRunInput)
		}
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
	var commandResult *execution.CommandResult
	if issuer.AuthorizesEvidence() {
		observed, commandErr := r.commandCapability.Execute(ctx, obligation.Argv)
		if commandErr != nil {
			result.Passed = false
			result.Result = execution.ResultError
			result.Failure = RunFailure{Kind: FailureCommand, Reason: "evidence command did not complete with locked source"}
			return execution.Receipt{}, result, fmt.Errorf("execute evidence command: %w", commandErr)
		}
		commandResult = &observed
		if observed.ExitCode != 0 {
			result.Passed = false
			if runErr == nil {
				result.ExitCode = 1
				result.Result = execution.ResultFailed
				result.Failure = RunFailure{Kind: FailureCommand, Reason: "evidence command returned a nonzero exit status"}
				runErr = &RunError{Failure: result.Failure, cause: ErrRunCommand}
			}
		}
	}
	completed := r.now().Round(0).UTC()
	if commandResult != nil {
		if commandResult.StartedAt.Before(started) {
			started = commandResult.StartedAt
		}
		if commandResult.CompletedAt.After(completed) {
			completed = commandResult.CompletedAt
		}
	}
	if completed.Before(started) {
		result.Passed = false
		result.ExitCode = 1
		result.Result = execution.ResultError
		result.Failure = RunFailure{Kind: FailureReference, Reason: "runner clock moved backward"}
		runErr = &RunError{Failure: result.Failure, cause: ErrRunReference}
		completed = started
	}
	receipt, issueErr := r.completeReceipt(ctx, issuer, scenario, obligation, started, completed, &result, commandResult)
	if issueErr != nil {
		return execution.Receipt{}, result, fmt.Errorf("complete run receipt: %w", issueErr)
	}
	if runErr != nil {
		return receipt, result, runErr
	}
	return receipt, result, nil
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
	request := Request{
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
		if err := CompareExactReplay(response, replay); err != nil {
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
	if err := DecodeStrictResponse(body, &envelope); err != nil {
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

func (r *Runner) completeReceipt(ctx context.Context, issuer execution.ReceiptIssuer, scenario scenarios.Scenario, obligation scenarios.ProofObligation, started, completed time.Time, result *RunResult, commandResult *execution.CommandResult) (execution.Receipt, error) {
	if result == nil {
		return execution.Receipt{}, errors.New("run result is required")
	}
	attachments := []execution.Attachment(nil)
	executionArtifacts := (*execution.ExecutionArtifacts)(nil)
	replay := (*execution.ReplayEvidence)(nil)
	if issuer.AuthorizesEvidence() {
		var err error
		attachments, executionArtifacts, replay, err = r.publishExecutionAttachments(scenario, obligation, result)
		if err != nil {
			return execution.Receipt{}, err
		}
	}
	makeTarget := obligation.MakeTarget
	argv := append([]string(nil), obligation.Argv...)
	if issuer.AuthorizesEvidence() {
		if commandResult == nil || len(commandResult.Argv) != 2 || commandResult.Argv[0] != "make" || commandResult.Argv[1] != obligation.MakeTarget {
			return execution.Receipt{}, errors.New("evidence command observation is incomplete")
		}
		makeTarget = commandResult.Argv[1]
		argv = append([]string(nil), commandResult.Argv...)
	}
	fields := execution.ReceiptFields{
		ScenarioID:            string(scenario.ID),
		ProofObligationID:     string(obligation.ObligationID),
		MakeTarget:            makeTarget,
		Argv:                  argv,
		StartedAt:             started,
		CompletedAt:           completed,
		ExitCode:              result.ExitCode,
		Result:                result.Result,
		Command:               commandObservation(commandResult),
		Assertions:            receiptAssertions(obligation, *result),
		VectorResults:         append([]execution.VectorResult(nil), r.vectorResults...),
		ArtifactBindings:      append([]execution.ArtifactBinding(nil), r.artifactBindings...),
		EnvironmentDimensions: append([]execution.EnvironmentDimension(nil), r.environmentDimensions...),
		Attachments:           attachments,
		AttachmentIDs:         attachmentIDsForReceipt(attachments),
		ExecutionArtifacts:    executionArtifacts,
		Replay:                replay,
		RunID:                 r.runID,
		ExecutionLineageID:    r.executionLineageID,
		RunURL:                r.runURL,
		Attempt:               r.attempt,
	}
	completion, err := execution.PrepareCompletion(issuer, fields)
	if err != nil {
		return execution.Receipt{}, err
	}
	return r.trustedRunner.CompleteReceipt(issuer, completion)
}

func commandObservation(result *execution.CommandResult) execution.CommandObservation {
	if result == nil {
		return execution.CommandObservation{}
	}
	return result.Observation()
}

func (r *Runner) validateEvidenceIssuer(issuer execution.ReceiptIssuer) error {
	if r == nil || r.attachmentPublisher == nil || !issuer.MatchesCommandCapability(r.commandCapability) || issuer.RunnerArtifactSHA256() == "" || r.runID == "" || r.executionLineageID == "" || r.runURL == "" || r.attempt != 1 {
		return fmt.Errorf("%w: evidence receipt issuer", ErrRunInput)
	}
	digest, err := execution.RunnerArtifactDigest(r.runnerArtifactBindings)
	if err != nil || digest != issuer.RunnerArtifactSHA256() {
		return fmt.Errorf("%w: evidence runner artifact bindings", ErrRunInput)
	}
	if len(r.runnerArtifactBindings) != 1 || r.runnerArtifactBindings[0].SHA256 != issuer.RunnerExecutableSHA256() {
		return fmt.Errorf("%w: evidence runner executable binding", ErrRunInput)
	}
	runningExecutableSHA256, err := execution.RunningExecutableSHA256()
	if err != nil || runningExecutableSHA256 != issuer.RunnerExecutableSHA256() {
		return fmt.Errorf("%w: running evidence executable", ErrRunInput)
	}
	return nil
}

func (r *Runner) publishExecutionAttachments(scenario scenarios.Scenario, obligation scenarios.ProofObligation, result *RunResult) ([]execution.Attachment, *execution.ExecutionArtifacts, *execution.ReplayEvidence, error) {
	if r == nil || r.attachmentPublisher == nil || result == nil {
		return nil, nil, nil, errors.New("evidence execution attachments are unavailable")
	}
	executionID := evidenceExecutionID(r.runID, r.executionLineageID, string(scenario.ID), string(obligation.ObligationID))
	logAttachment, err := r.attachmentPublisher.Publish("log", "text/plain", []byte("black-box execution completed\nexecution_id="+executionID+"\n"))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("publish execution log: %w", err)
	}
	trace, err := json.Marshal(struct {
		ExecutionID string           `json:"execution_id"`
		Checks      []AssertionCheck `json:"checks"`
	}{ExecutionID: executionID, Checks: result.Checks})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("encode execution trace: %w", err)
	}
	traceAttachment, err := r.attachmentPublisher.Publish("trace", "application/json", trace)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("publish execution trace: %w", err)
	}
	replay, err := json.Marshal(struct {
		ExecutionID   string `json:"execution_id"`
		ExchangeCount int    `json:"exchange_count"`
	}{ExecutionID: executionID, ExchangeCount: len(result.Exchanges)})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("encode replay data: %w", err)
	}
	replayAttachment, err := r.attachmentPublisher.Publish("replay-data", "application/json", replay)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("publish replay data: %w", err)
	}
	attachments := []execution.Attachment{logAttachment, traceAttachment, replayAttachment}
	artifacts := &execution.ExecutionArtifacts{
		LogAttachmentIDs:        []string{logAttachment.ID},
		TraceAttachmentIDs:      []string{traceAttachment.ID},
		ReplayDataAttachmentIDs: []string{replayAttachment.ID},
	}
	replayEvidence := &execution.ReplayEvidence{}
	for _, barrier := range scenario.BarrierPlan.Barriers {
		trace, err := json.Marshal(struct {
			ExecutionID string `json:"execution_id"`
			BarrierID   string `json:"barrier_id"`
		}{ExecutionID: executionID, BarrierID: string(barrier.ID)})
		if err != nil {
			return nil, nil, nil, fmt.Errorf("encode barrier trace: %w", err)
		}
		attachment, err := r.attachmentPublisher.Publish("barrier-trace", "application/json", trace)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("publish barrier trace: %w", err)
		}
		attachments = append(attachments, attachment)
		artifacts.BarrierTraceAttachmentIDs = append(artifacts.BarrierTraceAttachmentIDs, attachment.ID)
		replayEvidence.BarrierTraces = append(replayEvidence.BarrierTraces, execution.BarrierTrace{BarrierID: string(barrier.ID), AttachmentID: attachment.ID})
	}
	return attachments, artifacts, replayEvidence, nil
}

func evidenceExecutionID(runID, lineageID, scenarioID, obligationID string) string {
	digest := sha256.Sum256([]byte(runID + "\x00" + lineageID + "\x00" + scenarioID + "\x00" + obligationID))
	return hex.EncodeToString(digest[:])
}

func attachmentIDsForReceipt(attachments []execution.Attachment) []string {
	if attachments == nil {
		return nil
	}
	result := make([]string, len(attachments))
	for index, attachment := range attachments {
		result[index] = attachment.ID
	}
	return result
}

func receiptAssertions(obligation scenarios.ProofObligation, result RunResult) []execution.AssertionResult {
	values := make([]execution.AssertionResult, len(obligation.AssertionIDs))
	for index, id := range obligation.AssertionIDs {
		outcome := "passed"
		detail := ""
		if !result.Passed {
			if result.Failure.Kind == FailureSemantic {
				outcome = "failed"
				detail = "semantic black-box assertion failed"
			} else {
				outcome = "error"
				detail = "black-box execution did not reach a passing assertion"
			}
		}
		values[index] = execution.AssertionResult{AssertionID: string(id), Outcome: outcome, Detail: detail}
	}
	return values
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

func attachmentIDs(records []ExchangeMetadata) []string {
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
