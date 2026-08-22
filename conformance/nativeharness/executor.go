package nativeharness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/nativeexecution"
	"github.com/trainstar/synchro/conformance/scenarios"
)

// Executor is the generic native action dispatcher.
//
// It uses actor, command, and operation keys only. Scenario identity, action
// identity, action order, expected outcomes, and wire expectations do not
// affect capability dispatch.
type Executor struct {
	Controller Controller
	Artifact   Artifact
	Platform   Platform

	mu     sync.Mutex
	closed bool
}

var _ nativeexecution.Executor = (*Executor)(nil)

// NewExecutor validates capability wiring and creates one generic executor.
func NewExecutor(config Config) (*Executor, error) {
	if config.Controller == nil {
		return nil, errors.New("native harness controller capability is required")
	}
	if config.Artifact == nil {
		return nil, errors.New("native harness artifact capability is required")
	}
	if config.Platform == nil {
		return nil, errors.New("native harness platform capability is required")
	}
	return &Executor{
		Controller: config.Controller,
		Artifact:   config.Artifact,
		Platform:   config.Platform,
	}, nil
}

// Close closes every capability exactly once.
func (e *Executor) Close(ctx context.Context) error {
	if e == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("native harness close context is required")
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()

	return errors.Join(
		e.Controller.Close(ctx),
		e.Artifact.Close(ctx),
		e.Platform.Close(ctx),
	)
}

// Execute implements nativeexecution.Executor.
func (e *Executor) Execute(ctx context.Context, request nativeexecution.ExecuteRequest) (nativeexecution.ActionResult, error) {
	if e == nil {
		return nativeexecution.ActionResult{}, errors.New("native harness executor is nil")
	}
	if ctx == nil {
		return nativeexecution.ActionResult{}, errors.New("native harness action context is required")
	}
	if err := ctx.Err(); err != nil {
		return nativeexecution.ActionResult{}, err
	}
	e.mu.Lock()
	closed := e.closed
	e.mu.Unlock()
	if closed {
		return nativeexecution.ActionResult{}, errors.New("native harness is closed")
	}

	switch request.Action.Actor + "/" + request.Action.Command {
	case "controller/install-model":
		return e.executeInstall(ctx, request)
	case "controller/apply-step":
		return e.executeControllerSteps(ctx, request, e.Controller.ApplyStep)
	case "controller/request-step":
		return e.executeControllerSteps(ctx, request, e.Controller.RequestStep)
	case "artifact/stage-step":
		return e.executeArtifactSteps(ctx, request)
	case "client/open":
		return e.executeOpen(ctx, request)
	case "client/execute-step":
		return e.executeLocalSteps(ctx, request)
	case "client/synchronize-step":
		return e.executeSynchronizeSteps(ctx, request)
	case "client/begin-call":
		return e.executeBeginCall(ctx, request)
	case "client/await-call":
		return e.executeAwaitCall(ctx, request)
	case "client/lifecycle":
		return e.executeLifecycle(ctx, request)
	case "process/execute-step":
		return e.executeProcessSteps(ctx, request)
	case "process/terminate", "process/relaunch":
		return e.executeProcessBoundary(ctx, request)
	case "observer/await-step":
		return e.executeAwaitSteps(ctx, request)
	case "observer/capture":
		return e.executeCapture(ctx, request)
	case "observer/measure":
		return e.executeMeasure(ctx, request)
	default:
		return nativeexecution.ActionResult{}, fmt.Errorf("native harness actor and command are unsupported: %s/%s", request.Action.Actor, request.Action.Command)
	}
}

func (e *Executor) executeInstall(ctx context.Context, request nativeexecution.ExecuteRequest) (nativeexecution.ActionResult, error) {
	if request.SetupOperation == nil || len(request.Steps) != 0 {
		return nativeexecution.ActionResult{}, errors.New("native install request is incomplete")
	}
	operation := cloneOperation(*request.SetupOperation)
	if err := e.Controller.Install(ctx, InstallRequest{Operation: operation}); err != nil {
		return nativeexecution.ActionResult{}, fmt.Errorf("native controller install failed: %w", err)
	}
	return nativeexecution.ActionResult{}, nil
}

type stepCapability func(context.Context, StepRequest) (nativeexecution.StepObservation, error)

func (e *Executor) executeControllerSteps(ctx context.Context, request nativeexecution.ExecuteRequest, capability stepCapability) (nativeexecution.ActionResult, error) {
	observations := make([]nativeexecution.StepObservation, 0, len(request.Steps))
	samples := make([]nativeexecution.MeasurementSampleObservation, 0)
	for _, step := range request.Steps {
		stepRequest, err := stepRequestFor(request, step)
		if err != nil {
			return nativeexecution.ActionResult{}, err
		}
		observation, err := capability(ctx, stepRequest)
		if err != nil {
			return nativeexecution.ActionResult{}, fmt.Errorf("native controller operation %s failed: %w", scenarios.OperationKey(step.Operation), err)
		}
		observation.StepID = step.ID
		observations = append(observations, observation)
		stepSamples, err := e.measureSamples(ctx, request, step, stepRequest)
		if err != nil {
			return nativeexecution.ActionResult{}, err
		}
		samples = append(samples, stepSamples...)
	}
	return nativeexecution.ActionResult{StepObservations: observations, MeasurementSamples: samples}, nil
}

func (e *Executor) executeArtifactSteps(ctx context.Context, request nativeexecution.ExecuteRequest) (nativeexecution.ActionResult, error) {
	observations := make([]nativeexecution.StepObservation, 0, len(request.Steps))
	samples := make([]nativeexecution.MeasurementSampleObservation, 0)
	for _, step := range request.Steps {
		stepRequest, err := stepRequestFor(request, step)
		if err != nil {
			return nativeexecution.ActionResult{}, err
		}
		observation, err := e.Artifact.StageStep(ctx, stepRequest)
		if err != nil {
			return nativeexecution.ActionResult{}, fmt.Errorf("native artifact operation %s failed: %w", scenarios.OperationKey(step.Operation), err)
		}
		observation.StepID = step.ID
		observations = append(observations, observation)
		stepSamples, err := e.measureSamples(ctx, request, step, stepRequest)
		if err != nil {
			return nativeexecution.ActionResult{}, err
		}
		samples = append(samples, stepSamples...)
	}
	return nativeexecution.ActionResult{StepObservations: observations, MeasurementSamples: samples}, nil
}

func (e *Executor) executeOpen(ctx context.Context, request nativeexecution.ExecuteRequest) (nativeexecution.ActionResult, error) {
	var parameters scenarios.NativeClientOpenParameters
	if err := json.Unmarshal(request.Action.Parameters, &parameters); err != nil {
		return nativeexecution.ActionResult{}, errors.New("native open parameters are invalid")
	}
	client, found := findClient(request.Clients, parameters.ClientKey)
	if !found {
		return nativeexecution.ActionResult{}, errors.New("native open client is not declared")
	}
	if err := e.Platform.Open(ctx, OpenRequest{
		Client:         client,
		ClientKey:      parameters.ClientKey,
		DatabaseMode:   parameters.DatabaseMode,
		Initialization: parameters.Initialization,
		SeedStepID:     cloneStepID(parameters.SeedStepID),
	}); err != nil {
		return nativeexecution.ActionResult{}, fmt.Errorf("native platform open failed: %w", err)
	}
	return nativeexecution.ActionResult{}, nil
}

func (e *Executor) executeLocalSteps(ctx context.Context, request nativeexecution.ExecuteRequest) (nativeexecution.ActionResult, error) {
	observations := make([]nativeexecution.StepObservation, 0, len(request.Steps))
	samples := make([]nativeexecution.MeasurementSampleObservation, 0)
	clientKey, err := clientKeyParameter(request.Action.Parameters)
	if err != nil {
		return nativeexecution.ActionResult{}, err
	}
	for _, step := range request.Steps {
		stepRequest, err := stepRequestForClient(request, step, clientKey)
		if err != nil {
			return nativeexecution.ActionResult{}, err
		}
		observation, err := e.Platform.LocalAction(ctx, LocalActionRequest{ClientKey: clientKey, Operation: stepRequest.Operation})
		if err != nil {
			return nativeexecution.ActionResult{}, fmt.Errorf("native local operation %s failed: %w", scenarios.OperationKey(step.Operation), err)
		}
		observation.StepID = step.ID
		observations = append(observations, observation)
		stepSamples, err := e.measureSamples(ctx, request, step, stepRequest)
		if err != nil {
			return nativeexecution.ActionResult{}, err
		}
		samples = append(samples, stepSamples...)
	}
	return nativeexecution.ActionResult{StepObservations: observations, MeasurementSamples: samples}, nil
}

func (e *Executor) executeSynchronizeSteps(ctx context.Context, request nativeexecution.ExecuteRequest) (nativeexecution.ActionResult, error) {
	var parameters scenarios.NativeSynchronizeParameters
	if err := json.Unmarshal(request.Action.Parameters, &parameters); err != nil {
		return nativeexecution.ActionResult{}, errors.New("native synchronize parameters are invalid")
	}
	steps := make([]StepRequest, 0, len(request.Steps))
	for _, step := range request.Steps {
		stepRequest, err := stepRequestForClient(request, step, parameters.ClientKey)
		if err != nil {
			return nativeexecution.ActionResult{}, err
		}
		steps = append(steps, stepRequest)
	}
	synchronization, observations, err := e.Platform.Synchronize(ctx, SynchronizeRequest{
		ClientKey: parameters.ClientKey,
		Method:    parameters.Method,
		Steps:     steps,
	})
	if err != nil {
		return nativeexecution.ActionResult{}, fmt.Errorf("native synchronize failed: %w", err)
	}
	if len(observations) != len(request.Steps) {
		return nativeexecution.ActionResult{}, errors.New("native synchronize observations do not close the covered steps")
	}
	samples := make([]nativeexecution.MeasurementSampleObservation, 0)
	for index, step := range request.Steps {
		observations[index].StepID = step.ID
		stepSamples, err := e.measureSamples(ctx, request, step, steps[index])
		if err != nil {
			return nativeexecution.ActionResult{}, err
		}
		samples = append(samples, stepSamples...)
	}
	return nativeexecution.ActionResult{StepObservations: observations, Synchronization: &synchronization, MeasurementSamples: samples}, nil
}

func (e *Executor) executeBeginCall(ctx context.Context, request nativeexecution.ExecuteRequest) (nativeexecution.ActionResult, error) {
	var parameters scenarios.NativeBeginCallParameters
	if err := json.Unmarshal(request.Action.Parameters, &parameters); err != nil {
		return nativeexecution.ActionResult{}, errors.New("native begin-call parameters are invalid")
	}
	steps := make([]StepRequest, 0, len(request.Steps))
	for _, step := range request.Steps {
		stepRequest, stepErr := stepRequestForClient(request, step, parameters.ClientKey)
		if stepErr != nil {
			return nativeexecution.ActionResult{}, stepErr
		}
		steps = append(steps, stepRequest)
	}
	call, observations, err := e.Platform.BeginCall(ctx, CallRequest{
		ClientKey: parameters.ClientKey,
		CallID:    parameters.CallID,
		Method:    parameters.Method,
		Steps:     steps,
	})
	if err != nil {
		return nativeexecution.ActionResult{}, fmt.Errorf("native begin-call failed: %w", err)
	}
	if len(observations) != len(request.Steps) {
		return nativeexecution.ActionResult{}, errors.New("native begin-call observations do not close the covered steps")
	}
	samples := make([]nativeexecution.MeasurementSampleObservation, 0)
	for index, step := range request.Steps {
		observations[index].StepID = step.ID
		stepSamples, sampleErr := e.measureSamples(ctx, request, step, steps[index])
		if sampleErr != nil {
			return nativeexecution.ActionResult{}, sampleErr
		}
		samples = append(samples, stepSamples...)
	}
	return nativeexecution.ActionResult{ClientCall: &call, StepObservations: observations, MeasurementSamples: samples}, nil
}

func (e *Executor) executeAwaitCall(ctx context.Context, request nativeexecution.ExecuteRequest) (nativeexecution.ActionResult, error) {
	var parameters scenarios.NativeAwaitCallParameters
	if err := json.Unmarshal(request.Action.Parameters, &parameters); err != nil {
		return nativeexecution.ActionResult{}, errors.New("native await-call parameters are invalid")
	}
	call, err := e.Platform.AwaitCall(ctx, CallRequest{
		ClientKey: parameters.ClientKey,
		CallID:    parameters.CallID,
	})
	if err != nil {
		return nativeexecution.ActionResult{}, fmt.Errorf("native await-call failed: %w", err)
	}
	return nativeexecution.ActionResult{ClientCall: &call}, nil
}

func (e *Executor) executeLifecycle(ctx context.Context, request nativeexecution.ExecuteRequest) (nativeexecution.ActionResult, error) {
	var parameters scenarios.NativeLifecycleParameters
	if err := json.Unmarshal(request.Action.Parameters, &parameters); err != nil {
		return nativeexecution.ActionResult{}, errors.New("native lifecycle parameters are invalid")
	}
	if err := e.Platform.Lifecycle(ctx, LifecycleRequest{ClientKey: parameters.ClientKey, Operation: parameters.Operation}); err != nil {
		return nativeexecution.ActionResult{}, fmt.Errorf("native lifecycle operation failed: %w", err)
	}
	return nativeexecution.ActionResult{}, nil
}

func (e *Executor) executeProcessSteps(ctx context.Context, request nativeexecution.ExecuteRequest) (nativeexecution.ActionResult, error) {
	var parameters scenarios.NativeProcessStepParameters
	if err := json.Unmarshal(request.Action.Parameters, &parameters); err != nil {
		return nativeexecution.ActionResult{}, errors.New("native process parameters are invalid")
	}
	observations := make([]nativeexecution.StepObservation, 0, len(request.Steps))
	samples := make([]nativeexecution.MeasurementSampleObservation, 0)
	for _, step := range request.Steps {
		stepRequest, err := stepRequestForClientPointer(request, step, parameters.ClientKey)
		if err != nil {
			return nativeexecution.ActionResult{}, err
		}
		var observation nativeexecution.StepObservation
		if parameters.ClientKey == nil {
			observation, err = e.Controller.ProcessStep(ctx, stepRequest)
		} else {
			observation, err = e.Platform.ProcessStep(ctx, stepRequest)
		}
		if err != nil {
			return nativeexecution.ActionResult{}, fmt.Errorf("native process operation %s failed: %w", scenarios.OperationKey(step.Operation), err)
		}
		observation.StepID = step.ID
		observations = append(observations, observation)
		stepSamples, err := e.measureSamples(ctx, request, step, stepRequest)
		if err != nil {
			return nativeexecution.ActionResult{}, err
		}
		samples = append(samples, stepSamples...)
	}
	return nativeexecution.ActionResult{StepObservations: observations, MeasurementSamples: samples}, nil
}

func (e *Executor) executeProcessBoundary(ctx context.Context, request nativeexecution.ExecuteRequest) (nativeexecution.ActionResult, error) {
	var parameters scenarios.NativeProcessBoundaryParameters
	if err := json.Unmarshal(request.Action.Parameters, &parameters); err != nil {
		return nativeexecution.ActionResult{}, errors.New("native process boundary parameters are invalid")
	}
	boundary, err := e.Platform.ProcessBoundary(ctx, ProcessBoundaryRequest{
		ClientKey:     parameters.ClientKey,
		Operation:     request.Action.Command,
		Boundary:      parameters.Boundary,
		AfterActionID: parameters.AfterActionID,
	})
	if err != nil {
		return nativeexecution.ActionResult{}, fmt.Errorf("native process boundary failed: %w", err)
	}
	return nativeexecution.ActionResult{ProcessBoundary: &boundary}, nil
}

func (e *Executor) executeAwaitSteps(ctx context.Context, request nativeexecution.ExecuteRequest) (nativeexecution.ActionResult, error) {
	var parameters scenarios.NativeAwaitStepParameters
	if err := json.Unmarshal(request.Action.Parameters, &parameters); err != nil {
		return nativeexecution.ActionResult{}, errors.New("native await-step parameters are invalid")
	}
	observations := make([]nativeexecution.StepObservation, 0, len(request.Steps))
	samples := make([]nativeexecution.MeasurementSampleObservation, 0)
	for _, step := range request.Steps {
		stepRequest, err := stepRequestForClient(request, step, parameters.ClientKey)
		if err != nil {
			return nativeexecution.ActionResult{}, err
		}
		observation, err := e.Platform.AwaitStep(ctx, AwaitRequest{
			ClientKey: parameters.ClientKey,
			CallID:    cloneCallID(parameters.CallID),
			Step:      stepRequest,
		})
		if err != nil {
			return nativeexecution.ActionResult{}, fmt.Errorf("native await operation %s failed: %w", scenarios.OperationKey(step.Operation), err)
		}
		observation.StepID = step.ID
		observations = append(observations, observation)
		stepSamples, err := e.measureSamples(ctx, request, step, stepRequest)
		if err != nil {
			return nativeexecution.ActionResult{}, err
		}
		samples = append(samples, stepSamples...)
	}
	return nativeexecution.ActionResult{StepObservations: observations, MeasurementSamples: samples}, nil
}

func (e *Executor) executeCapture(ctx context.Context, request nativeexecution.ExecuteRequest) (nativeexecution.ActionResult, error) {
	var parameters scenarios.NativeCaptureParameters
	if err := json.Unmarshal(request.Action.Parameters, &parameters); err != nil {
		return nativeexecution.ActionResult{}, errors.New("native capture parameters are invalid")
	}
	observation, err := e.capture(ctx, CaptureRequest{
		ClientKeys: append([]string(nil), parameters.ClientKeys...),
		Sources:    append([]string(nil), parameters.Sources...),
	})
	if err != nil {
		return nativeexecution.ActionResult{}, err
	}
	return nativeexecution.ActionResult{CaptureObservation: &observation}, nil
}

func (e *Executor) executeMeasure(ctx context.Context, request nativeexecution.ExecuteRequest) (nativeexecution.ActionResult, error) {
	var parameters scenarios.NativeMeasureParameters
	if err := json.Unmarshal(request.Action.Parameters, &parameters); err != nil {
		return nativeexecution.ActionResult{}, errors.New("native measure parameters are invalid")
	}
	budgets, err := selectedBudgets(request.BudgetInstructions, parameters.PerformanceBudgetIDs)
	if err != nil {
		return nativeexecution.ActionResult{}, err
	}
	observations, err := e.Platform.MeasureBudgets(ctx, BudgetRequest{Budgets: budgets})
	if err != nil {
		return nativeexecution.ActionResult{}, fmt.Errorf("native budget measurement failed: %w", err)
	}
	if err := exactBudgetClosure(parameters.PerformanceBudgetIDs, observations); err != nil {
		return nativeexecution.ActionResult{}, err
	}
	return nativeexecution.ActionResult{BudgetObservations: observations}, nil
}

func (e *Executor) measureSamples(ctx context.Context, request nativeexecution.ExecuteRequest, step nativeexecution.ExecutionStep, stepRequest StepRequest) ([]nativeexecution.MeasurementSampleObservation, error) {
	if step.MeasurementSample == nil {
		return nil, nil
	}
	sample := step.MeasurementSample
	measurement, stratum, err := selectedMeasurement(request.MeasurementInstructions, sample.MeasurementID, sample.StratumID)
	if err != nil {
		return nil, err
	}
	value, err := e.Platform.MeasureSample(ctx, SampleRequest{
		Measurement: measurement,
		Stratum:     stratum,
		SampleID:    sample.SampleID,
		Parameters:  append([]byte(nil), sample.Parameters...),
		ClientKey:   cloneString(stepRequest.ClientKey),
		Operation:   cloneOperation(stepRequest.Operation),
	})
	if err != nil {
		return nil, fmt.Errorf("native metric sample %s failed: %w", sample.SampleID, err)
	}
	if value.MeasurementID != sample.MeasurementID || value.StratumID != sample.StratumID || value.SampleID != sample.SampleID {
		return nil, errors.New("native metric sample identity is not closed")
	}
	return []nativeexecution.MeasurementSampleObservation{value}, nil
}

func selectedBudgets(definitions []nativeexecution.BudgetInstruction, ids []contract.BudgetID) ([]nativeexecution.BudgetInstruction, error) {
	byID := make(map[contract.BudgetID]nativeexecution.BudgetInstruction, len(definitions))
	for _, definition := range definitions {
		definition.DataProfile.Parameters = append([]byte(nil), definition.DataProfile.Parameters...)
		byID[definition.ID] = definition
	}
	selected := make([]nativeexecution.BudgetInstruction, 0, len(ids))
	for _, id := range ids {
		definition, found := byID[id]
		if !found {
			return nil, fmt.Errorf("native budget %s is not defined", id)
		}
		selected = append(selected, definition)
	}
	return selected, nil
}

func selectedMeasurement(definitions []nativeexecution.MeasurementInstruction, measurementID contract.MeasurementID, stratumID contract.StratumID) (nativeexecution.MeasurementInstruction, contract.PerformanceStratum, error) {
	for _, definition := range definitions {
		if definition.ID != measurementID {
			continue
		}
		definition.DataProfile.Parameters = append([]byte(nil), definition.DataProfile.Parameters...)
		definition.Metrics = append([]contract.PerformanceMetric(nil), definition.Metrics...)
		definition.Strata = append([]contract.PerformanceStratum(nil), definition.Strata...)
		for index := range definition.Strata {
			definition.Strata[index].Parameters = append([]byte(nil), definition.Strata[index].Parameters...)
			if definition.Strata[index].StratumID == stratumID {
				return definition, definition.Strata[index], nil
			}
		}
		return nativeexecution.MeasurementInstruction{}, contract.PerformanceStratum{}, fmt.Errorf("native measurement %s does not define stratum %s", measurementID, stratumID)
	}
	return nativeexecution.MeasurementInstruction{}, contract.PerformanceStratum{}, fmt.Errorf("native measurement %s is not defined", measurementID)
}

func (e *Executor) capture(ctx context.Context, request CaptureRequest) (nativeexecution.CaptureObservation, error) {
	if err := exactSourceRequest(request.Sources); err != nil {
		return nativeexecution.CaptureObservation{}, err
	}
	byClass := make(map[CaptureSourceClass][]string)
	classes := make([]CaptureSourceClass, 0, len(request.Sources))
	for _, source := range request.Sources {
		class, _ := CaptureSourceClassFor(source)
		if _, found := byClass[class]; !found {
			classes = append(classes, class)
		}
		byClass[class] = append(byClass[class], source)
	}

	parts := make([]scenarios.StateFacts, 0, len(classes))
	for _, class := range classes {
		sources := byClass[class]
		captureRequest := CaptureRequest{
			ClientKeys: append([]string(nil), request.ClientKeys...),
			Sources:    append([]string(nil), sources...),
		}
		var values []CaptureSourceObservation
		var err error
		switch class {
		case CaptureSourceClassController:
			values, err = e.Controller.Capture(ctx, captureRequest)
		case CaptureSourceClassArtifact:
			values, err = e.Artifact.Capture(ctx, captureRequest)
		case CaptureSourceClassPlatform:
			values, err = e.Platform.Capture(ctx, captureRequest)
		default:
			return nativeexecution.CaptureObservation{}, errors.New("native capture source class is unsupported")
		}
		if err != nil {
			return nativeexecution.CaptureObservation{}, fmt.Errorf("native %s capture failed: %w", class, err)
		}
		if err := exactSourceClosure(sources, values); err != nil {
			return nativeexecution.CaptureObservation{}, fmt.Errorf("native %s capture closure: %w", class, err)
		}
		for _, value := range values {
			parts = append(parts, value.StateFacts)
		}
	}
	merged, err := MergeStateFacts(parts)
	if err != nil {
		return nativeexecution.CaptureObservation{}, fmt.Errorf("merge native capture facts: %w", err)
	}
	return nativeexecution.CaptureObservation{
		Sources:    append([]string(nil), request.Sources...),
		StateFacts: merged,
	}, nil
}

func stepRequestFor(request nativeexecution.ExecuteRequest, step nativeexecution.ExecutionStep) (StepRequest, error) {
	return stepRequestForClientPointer(request, step, nil)
}

func stepRequestForClient(request nativeexecution.ExecuteRequest, step nativeexecution.ExecutionStep, clientKey string) (StepRequest, error) {
	return stepRequestForClientPointer(request, step, &clientKey)
}

func stepRequestForClientPointer(_ nativeexecution.ExecuteRequest, step nativeexecution.ExecutionStep, clientKey *string) (StepRequest, error) {
	operation := cloneOperation(step.Operation)
	if scenarios.OperationKey(operation) == "/" {
		return StepRequest{}, errors.New("native step operation key is incomplete")
	}
	return StepRequest{ClientKey: cloneString(clientKey), Phase: step.Phase, Transport: step.Transport, Operation: operation}, nil
}

func cloneOperation(operation scenarios.Operation) scenarios.Operation {
	operation.Payload = append([]byte(nil), operation.Payload...)
	return operation
}

func cloneStepID(value *scenarios.StepID) *scenarios.StepID {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneCallID(value *scenarios.NativeCallID) *scenarios.NativeCallID {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func findClient(clients []scenarios.NativeClient, key string) (scenarios.NativeClient, bool) {
	for _, client := range clients {
		if client.Key == key {
			return client, true
		}
	}
	return scenarios.NativeClient{}, false
}

func clientKeyParameter(data json.RawMessage) (string, error) {
	var parameters scenarios.NativeClientParameters
	if err := json.Unmarshal(data, &parameters); err != nil || parameters.ClientKey == "" {
		return "", errors.New("native client parameters are invalid")
	}
	return parameters.ClientKey, nil
}
