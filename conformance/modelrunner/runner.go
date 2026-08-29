package modelrunner

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
	"github.com/trainstar/synchro/conformance/reference"
	"github.com/trainstar/synchro/conformance/scenarios"
)

type resolvedApplier interface {
	ApplyResolved(context.Context, scenarios.Operation, reference.ResolvedOperationInput) (reference.StepResult, error)
}

type pullHydrationFaultApplier interface {
	ApplyResolvedWithPullHydrationFault(context.Context, scenarios.Operation, reference.ResolvedOperationInput, reference.PullHydrationFault) (reference.StepResult, error)
}

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time { return c.now }

// NewModel constructs the only permitted initial model state.
func NewModel(seed int64) (*reference.Model, error) {
	return reference.New(reference.Config{
		State: reference.State{ProtocolVersion: 3},
		Clock: fixedClock{now: time.Unix(0, 0).UTC()},
		Seed:  seed,
	})
}

// Run constructs no state and executes one scenario on the supplied fresh
// model. RunModel is retained as the plan-level name for the same operation.
func Run(ctx context.Context, model *reference.Model, scenario scenarios.Scenario) (Result, error) {
	return run(ctx, model, scenario, true)
}

// RunModel executes one scenario through the reference ApplyResolved API.
func RunModel(ctx context.Context, model *reference.Model, scenario scenarios.Scenario) (Result, error) {
	return Run(ctx, model, scenario)
}

// RunScenario creates a protocol 3 model and runs one scenario.
func RunScenario(ctx context.Context, scenario scenarios.Scenario) (Result, error) {
	model, err := NewModel(seedForScenario(scenario))
	if err != nil {
		return Result{ScenarioID: string(scenario.ID)}, err
	}
	return Run(ctx, model, scenario)
}

func seedForScenario(scenario scenarios.Scenario) int64 {
	digest := sha256.Sum256([]byte(scenario.ID))
	var value int64
	for _, part := range digest[:8] {
		value = (value << 8) | int64(part)
	}
	if value == 0 {
		return 1
	}
	return value
}

func run(ctx context.Context, model *reference.Model, scenario scenarios.Scenario, evaluate bool) (Result, error) {
	result := Result{ScenarioID: string(scenario.ID), Replay: ReplayData{Seed: seedForScenario(scenario)}}
	if ctx == nil {
		return result, runFailure(RunErrorOperation, "", "", errors.New("context is required"))
	}
	if model == nil {
		return result, runFailure(RunErrorOperation, "", "", errors.New("model is required"))
	}
	if err := ctx.Err(); err != nil {
		return result, runFailure(RunErrorOperation, "", "", err)
	}
	if err := requireFreshModel(model.Snapshot()); err != nil {
		return result, runFailure(RunErrorSetup, "", "", err)
	}
	if len(scenario.Model.Setup) != 1 {
		return result, runFailure(RunErrorSetup, "", "", errors.New("model setup must contain exactly one operation"))
	}
	setup := scenario.Model.Setup[0]
	if scenarios.OperationKey(setup) != "model/install-current-contract" {
		return result, runFailure(RunErrorSetup, "", scenarios.OperationKey(setup), errors.New("model setup must use model/install-current-contract"))
	}
	if err := scenarios.ValidateOperation(setup); err != nil {
		return result, runFailure(RunErrorSetup, "", scenarios.OperationKey(setup), err)
	}

	before := model.Snapshot()
	setupResult, err := dispatch(ctx, model, setup, reference.ResolvedOperationInput{})
	setupExecution := OperationExecution{
		Operation:    cloneOperation(setup),
		OperationKey: scenarios.OperationKey(setup),
		Result:       cloneStepResult(setupResult),
		Err:          err,
		Before:       before,
		After:        model.Snapshot(),
	}
	result.Setup = append(result.Setup, setupExecution)
	result.Replay.Operations = append(result.Replay.Operations, replayOperation("", setup))
	if err != nil {
		return finishFailure(result, &RunError{Kind: RunErrorSetup, OperationKey: setupExecution.OperationKey, Err: err})
	}
	result.SetupSnapshot = setupExecution.After

	prior := make(map[scenarios.StepID]priorStep)
	for index, step := range scenario.Steps {
		if err := ctx.Err(); err != nil {
			return finishFailure(result, &RunError{Kind: RunErrorStep, StepID: step.ID, OperationKey: scenarios.OperationKey(step.Operation), Err: err})
		}
		execution, stepResult, stepErr := executeStep(ctx, model, scenario, index, step, prior)
		result.Steps = append(result.Steps, execution)
		result.Replay.Operations = append(result.Replay.Operations, replayOperations(step.ID, step.Operation, execution.Expanded)...)
		if stepErr != nil {
			return finishFailure(result, stepErr)
		}
		if stepResult != nil && stepResult.Result.Kind != "" {
			prior[step.ID] = *stepResult
		}
	}

	result.FinalSnapshot = model.Snapshot()
	result.Replay.SHA256 = hashReplay(result.Replay.Operations)
	if !evaluate {
		result.Replay.StateMatch = true
		result.Passed = true
		return result, nil
	}
	result.Replay.StateMatch, err = replayMatches(ctx, scenario, result)
	if err != nil {
		return finishFailure(result, err)
	}
	if err := evaluateWireExpectations(scenario, result.Steps); err != nil {
		return finishFailure(result, err)
	}
	predicates, err := evaluateModelPredicates(scenario, result)
	result.Predicates = predicates
	if err != nil {
		return finishFailure(result, err)
	}
	result.Passed = true
	return result, nil
}

type priorStep struct {
	Index        int
	StepID       string
	OperationKey string
	Result       reference.StepResult
}

func executeStep(ctx context.Context, model *reference.Model, scenario scenarios.Scenario, index int, step scenarios.Step, prior map[scenarios.StepID]priorStep) (OperationExecution, *priorStep, *RunError) {
	key := scenarios.OperationKey(step.Operation)
	execution := OperationExecution{
		StepID:       step.ID,
		Operation:    cloneOperation(step.Operation),
		OperationKey: key,
		Before:       model.Snapshot(),
	}
	if err := scenarios.ValidateOperation(step.Operation); err != nil {
		execution.After = model.Snapshot()
		execution.Err = err
		return execution, nil, &RunError{Kind: RunErrorOperation, StepID: step.ID, OperationKey: key, Err: err}
	}

	class, known := scenarios.LookupOperationClass(key)
	if !known {
		err := fmt.Errorf("operation %q is not in the closed scenario registry", key)
		execution.After = model.Snapshot()
		execution.Err = err
		return execution, nil, &RunError{Kind: RunErrorOperation, StepID: step.ID, OperationKey: key, Err: err}
	}
	if class == scenarios.OperationClassModelRunnerMacro {
		plan, err := expandWorkloadForBinding(model.Snapshot(), step.Operation, step.NativeBinding)
		if err != nil {
			execution.After = model.Snapshot()
			execution.Err = err
			return execution, nil, &RunError{Kind: RunErrorOperation, StepID: step.ID, OperationKey: key, Err: err}
		}
		execution.Expanded = cloneOperations(plan.Operations)
		samplesByOperation := make(map[int]workloadSamplePlan, len(plan.Samples))
		for _, sample := range plan.Samples {
			if sample.ExpandedOperationIndex < 0 || sample.ExpandedOperationIndex >= len(plan.Operations) {
				err := errors.New("workload sample targets an operation outside the expansion")
				execution.After = model.Snapshot()
				execution.Err = err
				return execution, nil, &RunError{Kind: RunErrorOperation, StepID: step.ID, OperationKey: key, Err: err}
			}
			if _, duplicate := samplesByOperation[sample.ExpandedOperationIndex]; duplicate {
				err := errors.New("workload samples target the same expanded operation")
				execution.After = model.Snapshot()
				execution.Err = err
				return execution, nil, &RunError{Kind: RunErrorOperation, StepID: step.ID, OperationKey: key, Err: err}
			}
			samplesByOperation[sample.ExpandedOperationIndex] = sample
		}
		var last reference.StepResult
		for operationIndex, operation := range plan.Operations {
			input, err := resolvedInputForOperation(model.Snapshot(), operation, prior, index, scenario)
			if err != nil {
				execution.After = model.Snapshot()
				execution.Err = err
				return execution, nil, &RunError{Kind: RunErrorResolvedInput, StepID: step.ID, OperationKey: key, Err: err}
			}
			operationBefore := model.Snapshot()
			operationResult, operationErr := dispatch(ctx, model, operation, input)
			operationAfter := model.Snapshot()
			if sample, sampled := samplesByOperation[operationIndex]; sampled {
				record, sampleErr := evaluateWorkloadSample(sample, operationResult, operationErr, operationBefore, operationAfter)
				execution.Samples = append(execution.Samples, record)
				if sampleErr != nil {
					execution.After = operationAfter
					execution.Err = sampleErr
					return execution, nil, &RunError{Kind: RunErrorExpectedOutcome, StepID: step.ID, OperationKey: key, ActualCode: record.ErrorCode, Err: sampleErr}
				}
				if operationErr != nil {
					continue
				}
			}
			if operationErr != nil {
				execution.After = model.Snapshot()
				execution.Err = operationErr
				if step.ExpectedOutcome.Disposition == "error" && matchExpectedOutcome(step.ExpectedOutcome, operationErr) == nil {
					return execution, nil, nil
				}
				return execution, nil, expectedOutcomeFailure(step, execution, operationErr)
			}
			last = operationResult
		}
		execution.Result = cloneStepResult(last)
		if err := matchExpectedOutcome(step.ExpectedOutcome, nil); err != nil {
			execution.After = model.Snapshot()
			execution.Err = err
			return execution, nil, &RunError{Kind: RunErrorExpectedOutcome, StepID: step.ID, OperationKey: key, Err: err}
		}
		execution.After = model.Snapshot()
		return execution, &priorStep{Index: index, StepID: string(step.ID), OperationKey: key, Result: cloneStepResult(last)}, nil
	}

	input, err := resolvedInputForOperation(model.Snapshot(), step.Operation, prior, index, scenario)
	if err != nil {
		execution.After = model.Snapshot()
		execution.Err = err
		return execution, nil, &RunError{Kind: RunErrorResolvedInput, StepID: step.ID, OperationKey: key, Err: err}
	}
	operationResult, operationErr := dispatchStep(ctx, model, scenario, step, input)
	execution.Result = cloneStepResult(operationResult)
	execution.Err = operationErr
	execution.After = model.Snapshot()
	if step.MeasurementSample != nil {
		measurement, err := deriveSchemaDispatchMeasurementSample(*step.MeasurementSample, execution)
		if err != nil {
			execution.Err = err
			return execution, nil, &RunError{Kind: RunErrorOperation, StepID: step.ID, OperationKey: key, Err: err}
		}
		execution.SchemaDispatchMeasurement = &measurement
	}
	if err := matchExpectedOutcome(step.ExpectedOutcome, operationErr); err != nil {
		return execution, nil, &RunError{
			Kind:         RunErrorExpectedOutcome,
			StepID:       step.ID,
			OperationKey: key,
			ExpectedCode: expectedCode(step.ExpectedOutcome),
			ActualCode:   ErrorCode(operationErr),
			Err:          err,
		}
	}
	if operationErr != nil {
		if step.ExpectedOutcome.Disposition == "error" {
			return execution, nil, nil
		}
		return execution, nil, &RunError{Kind: RunErrorStep, StepID: step.ID, OperationKey: key, Err: operationErr}
	}
	return execution, &priorStep{Index: index, StepID: string(step.ID), OperationKey: key, Result: cloneStepResult(operationResult)}, nil
}

func evaluateWorkloadSample(sample workloadSamplePlan, result reference.StepResult, operationErr error, before, after reference.StateSnapshot) (WorkloadSampleExecution, error) {
	record := WorkloadSampleExecution{
		Family:                 sample.Family,
		Boundary:               sample.Boundary,
		Value:                  sample.Value,
		ExpandedOperationIndex: sample.ExpandedOperationIndex,
		Before:                 before,
		After:                  after,
	}
	if operationErr != nil {
		record.ErrorCode = ErrorCode(operationErr)
	} else {
		cloned := cloneStepResult(result)
		record.Result = &cloned
	}
	if sample.Expected.ErrorCode != "" {
		if operationErr == nil || record.ErrorCode != sample.Expected.ErrorCode {
			return record, errors.New("workload sample did not return its expected canonical error code")
		}
		if sample.Expected.PreserveState && !reflect.DeepEqual(before, after) {
			return record, errors.New("workload sample changed state after its expected error")
		}
		return record, nil
	}
	if operationErr != nil {
		return record, errors.New("workload sample returned an unexpected operation error")
	}
	if sample.Expected.ResultKind == "" || result.Kind != sample.Expected.ResultKind {
		return record, errors.New("workload sample returned an unexpected result kind")
	}
	if sample.Expected.HTTPStatus != 0 {
		if result.HTTP == nil || result.HTTP.Status != sample.Expected.HTTPStatus {
			return record, errors.New("workload sample returned an unexpected HTTP status")
		}
		if sample.Expected.HTTPCode == "" {
			if result.HTTP.HasCode {
				return record, errors.New("successful workload sample returned an HTTP error code")
			}
		} else if !result.HTTP.HasCode || result.HTTP.Code != sample.Expected.HTTPCode {
			return record, errors.New("workload sample returned an unexpected canonical HTTP code")
		}
	}
	if sample.Expected.PreserveState && !reflect.DeepEqual(before, after) {
		return record, errors.New("workload sample changed state after its expected HTTP error")
	}
	if sample.Boundary != WorkloadBoundaryInvalid {
		if err := validateAcceptedWorkloadSample(sample, result); err != nil {
			return record, err
		}
	}
	return record, nil
}

func validateAcceptedWorkloadSample(sample workloadSamplePlan, result reference.StepResult) error {
	switch sample.Family {
	case WorkloadSampleFanout, WorkloadSampleImpact:
		if result.Schema == nil || result.Schema.Reason != "membership_generation_staged" {
			return errors.New("administrative workload sample was not accepted")
		}
	case WorkloadSampleBackfill:
		if result.Schema == nil || result.Schema.Reason != "membership_generation_staged" || result.Schema.BatchSize != sample.Value {
			return errors.New("backfill workload sample did not retain its batch size")
		}
		if !sample.Expected.CheckBatchCount || result.Schema.BatchCount != sample.Expected.BatchCount {
			return errors.New("backfill workload sample did not execute the expected batches")
		}
	case WorkloadSamplePull:
		if result.Pull == nil {
			return errors.New("pull workload sample has no typed observation")
		}
	case WorkloadSampleRebuild:
		if result.Rebuild == nil || uint64(len(result.Rebuild.Records)) > sample.Value {
			return errors.New("rebuild workload sample did not honor its accepted page limit")
		}
	case WorkloadSampleCompaction:
		if result.Retention == nil || result.Retention.BatchSize != sample.Value || result.Retention.DeletedCount > sample.Value {
			return errors.New("compaction workload sample did not honor its accepted batch size")
		}
	case WorkloadSamplePush:
		if result.Push == nil || uint64(len(result.Push.Mutations)) != sample.Value {
			return errors.New("push workload sample did not retain its mutation count")
		}
	default:
		return errors.New("workload sample family is not closed")
	}
	return nil
}

func dispatch(ctx context.Context, model *reference.Model, operation scenarios.Operation, input reference.ResolvedOperationInput) (reference.StepResult, error) {
	applier, ok := any(model).(resolvedApplier)
	if !ok {
		return reference.StepResult{}, &RunError{Kind: RunErrorApplyResolvedMissing, OperationKey: scenarios.OperationKey(operation), Err: errors.New("reference.Model.ApplyResolved is not available")}
	}
	return applier.ApplyResolved(ctx, cloneOperation(operation), cloneResolvedInput(input))
}

func dispatchStep(ctx context.Context, model *reference.Model, scenario scenarios.Scenario, step scenarios.Step, input reference.ResolvedOperationInput) (reference.StepResult, error) {
	fault, applies, err := pullHydrationFaultForStep(scenario, step, model.Snapshot())
	if err != nil {
		return reference.StepResult{}, err
	}
	if !applies {
		return dispatch(ctx, model, step.Operation, input)
	}
	applier, ok := any(model).(pullHydrationFaultApplier)
	if !ok {
		return reference.StepResult{}, &RunError{Kind: RunErrorApplyResolvedMissing, OperationKey: scenarios.OperationKey(step.Operation), Err: errors.New("reference pull hydration fault API is not available")}
	}
	return applier.ApplyResolvedWithPullHydrationFault(ctx, cloneOperation(step.Operation), cloneResolvedInput(input), fault)
}

func pullHydrationFaultForStep(scenario scenarios.Scenario, step scenarios.Step, snapshot reference.StateSnapshot) (reference.PullHydrationFault, bool, error) {
	if scenario.ID != "SCN-PULL-HYDRATION-FAILURE-001" || step.ID != "STEP-PULL-HYDRATION-PULL-001" {
		return reference.PullHydrationFault{}, false, nil
	}
	if scenarios.OperationKey(step.Operation) != "pull/request-page" || len(scenario.FaultPlans) != 1 {
		return reference.PullHydrationFault{}, false, errors.New("pull hydration scenario has an invalid fault target")
	}
	plan := scenario.FaultPlans[0]
	if plan.Injection.Mechanism != "wire-fault" || plan.Injection.Target != "selected pull candidate projection hydration" || plan.Injection.Operator != "omit" {
		return reference.PullHydrationFault{}, false, errors.New("pull hydration scenario has an invalid fault recipe")
	}
	barrierFound := false
	for _, barrier := range scenario.BarrierPlan.Barriers {
		if barrier.ID == plan.BarrierID {
			barrierFound = true
			break
		}
	}
	if !barrierFound {
		return reference.PullHydrationFault{}, false, errors.New("pull hydration scenario fault barrier is absent")
	}
	var request struct {
		Scopes []struct {
			ScopeID string `json:"scope_id"`
		} `json:"scopes"`
	}
	if err := json.Unmarshal(step.Operation.Payload, &request); err != nil {
		return reference.PullHydrationFault{}, false, fmt.Errorf("decode pull hydration fault request: %w", err)
	}
	requested := make(map[reference.ScopeID]struct{}, len(request.Scopes))
	for _, scope := range request.Scopes {
		requested[reference.ScopeID(scope.ScopeID)] = struct{}{}
	}
	projections := make(map[reference.ProjectionKey]struct{})
	for _, scope := range snapshot.Scopes {
		if _, found := requested[scope.Key]; !found {
			continue
		}
		for _, effect := range scope.Value.Effects {
			if effect.HasCapturedProjection {
				projections[effect.CapturedProjection] = struct{}{}
			}
		}
	}
	if len(projections) != 1 {
		return reference.PullHydrationFault{}, false, fmt.Errorf("pull hydration fault resolved %d candidate projections, want 1", len(projections))
	}
	for projection := range projections {
		return reference.PullHydrationFault{Projection: projection}, true, nil
	}
	panic("unreachable")
}

func resolvedInputForOperation(snapshot reference.StateSnapshot, operation scenarios.Operation, prior map[scenarios.StepID]priorStep, index int, scenario scenarios.Scenario) (reference.ResolvedOperationInput, error) {
	key := scenarios.OperationKey(operation)
	switch key {
	case "local/apply-pull-page":
		stepID, err := sourceStepID(operation.Payload)
		if err != nil {
			return reference.ResolvedOperationInput{}, err
		}
		candidate, found := prior[scenarios.StepID(stepID)]
		if !found || candidate.Index >= index {
			return reference.ResolvedOperationInput{}, fmt.Errorf("source_step_id %q does not identify an earlier completed step", stepID)
		}
		if candidate.OperationKey != "pull/request-page" || candidate.Result.Kind != reference.StepResultKindPull || candidate.Result.HTTP == nil || candidate.Result.HTTP.Status != 200 {
			return reference.ResolvedOperationInput{}, fmt.Errorf("source_step_id %q is not an earlier successful pull/request-page", stepID)
		}
		return reference.ResolvedOperationInput{SourceStep: &reference.ResolvedStep{
			StepID:       candidate.StepID,
			OperationKey: candidate.OperationKey,
			Result:       cloneStepResult(candidate.Result),
		}}, nil
	case "artifact/install-portable-seed":
		fixture, err := buildSeedForScenario(snapshot, scenario)
		if err != nil {
			return reference.ResolvedOperationInput{}, err
		}
		return reference.ResolvedOperationInput{PortableSeed: &fixture}, nil
	default:
		return reference.ResolvedOperationInput{}, nil
	}
}

func sourceStepID(payload json.RawMessage) (string, error) {
	var object map[string]json.RawMessage
	if err := jsonstrict.Decode(payload, &object); err != nil {
		return "", fmt.Errorf("decode local/apply-pull-page payload: %w", err)
	}
	raw, ok := object["source_step_id"]
	if !ok {
		return "", errors.New("local/apply-pull-page source_step_id is required")
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil || value == "" {
		return "", errors.New("local/apply-pull-page source_step_id must be a nonempty string")
	}
	return value, nil
}

func matchExpectedOutcome(expected scenarios.ExpectedOutcome, actual error) error {
	switch expected.Disposition {
	case "success":
		if actual != nil {
			return fmt.Errorf("expected success: %w", actual)
		}
		return nil
	case "error":
		if actual == nil {
			return fmt.Errorf("expected error code %q, operation succeeded", expectedCode(expected))
		}
		want := expectedCode(expected)
		got, ok := ErrorCodeOK(actual)
		if !ok || got != want {
			return fmt.Errorf("expected canonical error code %q, got %q: %w", want, got, actual)
		}
		return nil
	default:
		return errors.New("expected outcome has an unknown disposition")
	}
}

func expectedCode(expected scenarios.ExpectedOutcome) string {
	if expected.ErrorCode == nil {
		return ""
	}
	return *expected.ErrorCode
}

// ErrorCode extracts a canonical code from a typed error. It deliberately
// does not inspect or parse free-form error text.
func ErrorCode(err error) string {
	code, _ := ErrorCodeOK(err)
	return code
}

func ErrorCodeOK(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	var errorCoder interface{ ErrorCode() string }
	if errors.As(err, &errorCoder) && errorCoder.ErrorCode() != "" {
		return errorCoder.ErrorCode(), true
	}
	var codeProvider interface{ Code() string }
	if errors.As(err, &codeProvider) && codeProvider.Code() != "" {
		return codeProvider.Code(), true
	}
	return "", false
}

func evaluateWireExpectations(scenario scenarios.Scenario, executions []OperationExecution) error {
	byStep := make(map[scenarios.StepID]OperationExecution, len(executions))
	for _, execution := range executions {
		byStep[execution.StepID] = execution
	}
	for _, expectation := range scenario.WireExpectations {
		execution, ok := byStep[expectation.StepID]
		if !ok {
			return &RunError{Kind: RunErrorPredicate, StepID: expectation.StepID, AssertionID: string(expectation.AssertionID), Err: errors.New("wire expectation has no executed step")}
		}
		observation := execution.Result.HTTP
		if observation == nil || observation.Status != expectation.HTTPStatus || observation.Retryable != expectation.Retryable {
			status := 0
			retryable := false
			if observation != nil {
				status = observation.Status
				retryable = observation.Retryable
			}
			code := ""
			if observation != nil && observation.HasCode {
				code = string(observation.Code)
			}
			return &RunError{Kind: RunErrorPredicate, StepID: expectation.StepID, AssertionID: string(expectation.AssertionID), Err: fmt.Errorf("wire status, retryability, and code = %d/%t/%q, want %d/%t", status, retryable, code, expectation.HTTPStatus, expectation.Retryable)}
		}
		if expectation.HTTPStatus == 0 && (len(observation.Body) != 0 || observation.HasRetryAfterMilliseconds || observation.RetryAfterMilliseconds != 0) {
			return &RunError{Kind: RunErrorPredicate, StepID: expectation.StepID, AssertionID: string(expectation.AssertionID), Err: errors.New("transport failure contains a received HTTP response")}
		}
		wantCode := ""
		if expectation.ErrorCode != nil {
			wantCode = *expectation.ErrorCode
		}
		gotCode := ""
		if observation.HasCode {
			gotCode = string(observation.Code)
		}
		if wantCode != gotCode {
			return &RunError{Kind: RunErrorPredicate, StepID: expectation.StepID, AssertionID: string(expectation.AssertionID), Err: errors.New("canonical wire error code does not match the authored expectation")}
		}
	}
	return nil
}

func evaluateModelPredicates(scenario scenarios.Scenario, result Result) ([]PredicateResult, error) {
	assertionsByExpectation := make(map[scenarios.ExpectationID][]scenarios.Assertion)
	for _, assertion := range scenario.Assertions {
		for _, expectationID := range assertion.ExpectationIDs {
			assertionsByExpectation[expectationID] = append(assertionsByExpectation[expectationID], assertion)
		}
	}
	results := make([]PredicateResult, 0, len(scenario.Model.ExpectedState))
	for _, expectation := range scenario.Model.ExpectedState {
		if err := validatePredicate(expectation.Predicate); err != nil {
			return results, &RunError{Kind: RunErrorPredicate, Expectation: expectation.ID, Err: err}
		}
		passed, reason := evaluatePredicate(expectation, result)
		assertions := assertionsByExpectation[expectation.ID]
		assertionID := ""
		if len(assertions) > 0 {
			assertionID = string(assertions[0].ID)
		}
		results = append(results, PredicateResult{ExpectationID: expectation.ID, AssertionID: assertionID, Name: expectation.Predicate.Name, Passed: passed, Reason: reason})
		if !passed {
			return results, &RunError{Kind: RunErrorPredicate, Expectation: expectation.ID, AssertionID: assertionID, Err: errors.New(reason)}
		}
	}
	return results, nil
}

func validatePredicate(predicate scenarios.Predicate) error {
	if err := jsonstrict.ValidateValue(predicate.Payload); err != nil {
		return fmt.Errorf("predicate payload is invalid: %w", err)
	}
	switch predicate.Name {
	case "schema-dispatch-observations-satisfied", "schema-dispatch-measurement-satisfied":
		if _, err := scenarios.DecodeSchemaDispatchMeasurementPlan(predicate.Payload); err != nil {
			return err
		}
		return nil
	case "state-equals-authored-model", "state-unchanged", "canonical-wire-outcome", "legal-state-transition", "artifact-policy-satisfied", "performance-contract-satisfied":
	default:
		return errors.New("predicate name is not in the closed authored set")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(predicate.Payload, &object); err != nil || object == nil || len(object) != 0 {
		return errors.New("predicate payload must be an empty object")
	}
	return nil
}

func evaluatePredicate(expectation scenarios.ModelExpectation, result Result) (bool, string) {
	switch expectation.Predicate.Name {
	case "state-equals-authored-model":
		if !result.Replay.StateMatch {
			return false, "deterministic replay did not reproduce the model state"
		}
		if expectation.StateFacts == nil {
			return false, "authored state facts are absent"
		}
		if reason := stateFactsFailure(*expectation.StateFacts, result.FinalSnapshot); reason != "" {
			return false, reason
		}
		return true, "durable state matched the authored facts"
	case "state-unchanged":
		for _, step := range result.Steps {
			failed := step.Err != nil || step.Result.HTTP != nil && step.Result.HTTP.Status >= 400
			if failed && reflect.DeepEqual(step.Before, step.After) {
				return true, "expected failing operation preserved the model snapshot"
			}
		}
		return false, "no expected failing operation preserved the model snapshot"
	case "canonical-wire-outcome":
		return true, "wire expectations matched during execution"
	case "legal-state-transition":
		reason := transitionSemanticFailure(result)
		return reason == "", reason
	case "schema-dispatch-observations-satisfied":
		plan, err := scenarios.DecodeSchemaDispatchMeasurementPlan(expectation.Predicate.Payload)
		if err != nil {
			return false, "schema-dispatch predicate payload is invalid"
		}
		return schemaDispatchObservationsSatisfied(result, plan)
	case "artifact-policy-satisfied":
		for _, step := range result.Steps {
			if step.OperationKey == "artifact/install-portable-seed" {
				return true, "portable seed was resolved through the reference input boundary"
			}
		}
		return false, "no portable seed installation was executed"
	case "performance-contract-satisfied":
		return performanceContractSatisfied(result.ScenarioID, result), "the authored performance trace did not satisfy its closed contract"
	case "schema-dispatch-measurement-satisfied":
		plan, err := scenarios.DecodeSchemaDispatchMeasurementPlan(expectation.Predicate.Payload)
		if err != nil {
			return false, "schema-dispatch predicate payload is invalid"
		}
		return schemaDispatchMeasurementSatisfied(result, plan)
	default:
		return false, "predicate is not implemented"
	}
}

func requireFreshModel(snapshot reference.StateSnapshot) error {
	if snapshot.ProtocolVersion != 3 {
		return errors.New("model initial state must use protocol version 3")
	}
	if snapshot.CurrentSchema != (reference.SchemaRef{}) || len(snapshot.Schemas) != 0 || len(snapshot.Registry.Generations) != 0 || len(snapshot.Relations) != 0 || len(snapshot.Clients) != 0 || len(snapshot.Rows) != 0 || len(snapshot.Scopes) != 0 || len(snapshot.Stream.Transactions) != 0 || len(snapshot.Stream.SourceRows) != 0 || len(snapshot.Stream.Materializations) != 0 || len(snapshot.Fences) != 0 || len(snapshot.Projections) != 0 || len(snapshot.Batches) != 0 || len(snapshot.Mutations) != 0 || len(snapshot.Rebuilds) != 0 || len(snapshot.ClientLocal) != 0 || len(snapshot.RetentionFloors) != 0 || len(snapshot.Seed.Exports) != 0 || len(snapshot.Seed.Records) != 0 || len(snapshot.Events) != 0 {
		return errors.New("model initial state must contain only reference.State{ProtocolVersion:3}")
	}
	return nil
}

func finishFailure(result Result, failure error) (Result, error) {
	result.Replay.SHA256 = hashReplay(result.Replay.Operations)
	if result.FinalSnapshot.ProtocolVersion == 0 {
		result.FinalSnapshot = lastSnapshot(result)
	}
	result.Passed = false
	var runErr *RunError
	if errors.As(failure, &runErr) {
		return result, runErr
	}
	return result, &RunError{Kind: RunErrorPredicate, Err: failure}
}

func lastSnapshot(result Result) reference.StateSnapshot {
	if len(result.Steps) > 0 {
		return result.Steps[len(result.Steps)-1].After
	}
	if len(result.Setup) > 0 {
		return result.Setup[len(result.Setup)-1].After
	}
	return reference.StateSnapshot{}
}

func runFailure(kind RunErrorKind, stepID scenarios.StepID, operation string, err error) error {
	return &RunError{Kind: kind, StepID: stepID, OperationKey: operation, Err: err}
}

func replayOperation(stepID scenarios.StepID, operation scenarios.Operation) ReplayOperation {
	return ReplayOperation{StepID: stepID, OperationKey: scenarios.OperationKey(operation), Payload: append([]byte(nil), operation.Payload...)}
}

func replayOperations(stepID scenarios.StepID, operation scenarios.Operation, expanded []scenarios.Operation) []ReplayOperation {
	if len(expanded) == 0 {
		return []ReplayOperation{replayOperation(stepID, operation)}
	}
	result := make([]ReplayOperation, 0, len(expanded))
	for _, item := range expanded {
		result = append(result, replayOperation(stepID, item))
	}
	return result
}

func cloneOperation(operation scenarios.Operation) scenarios.Operation {
	operation.Payload = append(json.RawMessage(nil), operation.Payload...)
	return operation
}

func cloneOperations(operations []scenarios.Operation) []scenarios.Operation {
	if operations == nil {
		return nil
	}
	result := make([]scenarios.Operation, len(operations))
	for index, operation := range operations {
		result[index] = cloneOperation(operation)
	}
	return result
}

func cloneResolvedInput(input reference.ResolvedOperationInput) reference.ResolvedOperationInput {
	result := reference.ResolvedOperationInput{}
	if input.SourceStep != nil {
		step := *input.SourceStep
		step.Result = cloneStepResult(step.Result)
		result.SourceStep = &step
	}
	if input.PortableSeed != nil {
		fixture := *input.PortableSeed
		fixture.ArtifactBytes = append([]byte(nil), input.PortableSeed.ArtifactBytes...)
		fixture.ManifestBytes = append([]byte(nil), input.PortableSeed.ManifestBytes...)
		fixture.PortableScopeIDs = append([]reference.ScopeID(nil), input.PortableSeed.PortableScopeIDs...)
		fixture.Scopes = append([]reference.PortableSeedScopeFixture(nil), input.PortableSeed.Scopes...)
		fixture.Rows = make([]reference.PortableSeedRowFixture, len(input.PortableSeed.Rows))
		for index, row := range input.PortableSeed.Rows {
			fixture.Rows[index] = row
			fixture.Rows[index].Row = cloneAuthoritativeRow(row.Row)
		}
		result.PortableSeed = &fixture
	}
	return result
}

func cloneStepResult(source reference.StepResult) reference.StepResult {
	result := source
	if source.HTTP != nil {
		value := *source.HTTP
		value.Body = append([]byte(nil), value.Body...)
		result.HTTP = &value
	}
	if source.Connect != nil {
		value := *source.Connect
		value.AddedScopes = append([]reference.ScopeID(nil), value.AddedScopes...)
		value.RemovedScopes = append([]reference.ScopeID(nil), value.RemovedScopes...)
		value.ScopeCursors = append([]reference.ScopeCursorObservation(nil), value.ScopeCursors...)
		result.Connect = &value
	}
	if source.Local != nil {
		value := *source.Local
		result.Local = &value
	}
	if source.Lifecycle != nil {
		value := *source.Lifecycle
		result.Lifecycle = &value
	}
	if source.Push != nil {
		value := *source.Push
		value.Mutations = append([]reference.MutationObservation(nil), value.Mutations...)
		result.Push = &value
	}
	if source.Pull != nil {
		value := *source.Pull
		value.Changes = append([]reference.PullChangeObservation(nil), value.Changes...)
		value.ScopeCursors = append([]reference.ScopeCursorObservation(nil), value.ScopeCursors...)
		value.AddedScopes = append([]reference.ScopeID(nil), value.AddedScopes...)
		value.RemovedScopes = append([]reference.ScopeID(nil), value.RemovedScopes...)
		value.RebuildScopes = append([]reference.ScopeID(nil), value.RebuildScopes...)
		value.ScopeChecksums = append([]reference.ScopeChecksumObservation(nil), value.ScopeChecksums...)
		result.Pull = &value
	}
	if source.Rebuild != nil {
		value := *source.Rebuild
		value.Records = append([]reference.RebuildRecordObservation(nil), value.Records...)
		result.Rebuild = &value
	}
	if source.WAL != nil {
		value := *source.WAL
		value.AffectedScopes = append([]reference.ScopeID(nil), value.AffectedScopes...)
		result.WAL = &value
	}
	if source.Schema != nil {
		value := *source.Schema
		value.AffectedScopes = append([]reference.ScopeID(nil), value.AffectedScopes...)
		result.Schema = &value
	}
	if source.Retention != nil {
		value := *source.Retention
		result.Retention = &value
	}
	if source.Client != nil {
		value := *source.Client
		result.Client = &value
	}
	return result
}

func cloneAuthoritativeRow(source reference.AuthoritativeRow) reference.AuthoritativeRow {
	source.FieldValues = append([]reference.FieldValue(nil), source.FieldValues...)
	if source.DeletedAt != nil {
		value := *source.DeletedAt
		source.DeletedAt = &value
	}
	if source.DeleteReason != nil {
		value := *source.DeleteReason
		source.DeleteReason = &value
	}
	if source.UpdatedAt != nil {
		value := *source.UpdatedAt
		source.UpdatedAt = &value
	}
	return source
}
