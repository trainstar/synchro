package nativeexecution

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/trainstar/synchro/conformance/execution"
	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/internal/performance"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	manifestSchemaVersion           = 3
	traceSchemaVersion              = 2
	maximumTraceDurationNanoseconds = int64(1<<53 - 1)
)

// Outcome is the closed terminal result for a native action or trace.
type Outcome string

const (
	OutcomePassed Outcome = "passed"
	OutcomeFailed Outcome = "failed"
	OutcomeError  Outcome = "error"
)

// Manifest is one catalog-bound native plan for a support cell.
type Manifest struct {
	SchemaVersion            int                            `json:"schema_version"`
	ScenarioID               string                         `json:"scenario_id"`
	ScenarioSHA256           string                         `json:"scenario_sha256"`
	PerformanceCatalogSHA256 string                         `json:"performance_catalog_sha256"`
	ObligationID             string                         `json:"obligation_id"`
	SupportCellID            string                         `json:"support_cell_id"`
	Component                string                         `json:"component"`
	Platform                 string                         `json:"platform"`
	MakeTarget               string                         `json:"make_target"`
	PerformanceBudgets       []contract.PerformanceBudget   `json:"performance_budgets"`
	RequiredMeasurements     []contract.RequiredMeasurement `json:"required_measurements"`
	Clients                  []scenarios.NativeClient       `json:"clients"`
	Actions                  []ManifestAction               `json:"actions"`
}

// ManifestAction resolves one native action to its immutable scenario input.
type ManifestAction struct {
	Sequence         int                          `json:"sequence"`
	Action           scenarios.NativeAction       `json:"action"`
	SetupOperation   *scenarios.Operation         `json:"setup_operation,omitempty"`
	Steps            []scenarios.Step             `json:"steps,omitempty"`
	WorkloadExpansions map[scenarios.StepID][]scenarios.Operation `json:"workload_expansions,omitempty"`
	WireExpectations []scenarios.WireExpectation  `json:"wire_expectations,omitempty"`
	Expectations     []scenarios.ModelExpectation `json:"expectations,omitempty"`
}

// BuildManifest resolves a validated selection into one transport-neutral plan.
func BuildManifest(selection Selection) (Manifest, error) {
	if selection.scenario.NativeExecution == nil || selection.obligation.ProofType != "native-e2e" {
		return Manifest{}, errors.New("native selection is incomplete")
	}
	if selection.scenario.ID == "" || selection.obligation.ObligationID == "" || selection.obligation.SupportCellID == nil || string(*selection.obligation.SupportCellID) != selection.supportCellID {
		return Manifest{}, errors.New("native selection identity is inconsistent")
	}
	if selection.component == "" || selection.platform == "" || selection.obligation.MakeTarget == "" {
		return Manifest{}, errors.New("native selection environment is incomplete")
	}
	if !validSHA256(selection.digest) || !validSHA256(selection.performanceCatalogSHA256) {
		return Manifest{}, errors.New("native selection digest is invalid")
	}

	scenario, err := scenarios.Clone(selection.scenario)
	if err != nil {
		return Manifest{}, fmt.Errorf("clone native scenario: %w", err)
	}
	preserveStateFactProjections(selection.scenario.Model.ExpectedState, scenario.Model.ExpectedState)
	budgets, measurements, err := clonePerformanceDefinitions(selection.performanceBudgets, selection.requiredMeasurements)
	if err != nil {
		return Manifest{}, fmt.Errorf("clone native performance definitions: %w", err)
	}
	if err := validateSelectedDefinitions(selection, budgets, measurements); err != nil {
		return Manifest{}, err
	}

	steps := make(map[scenarios.StepID]scenarios.Step, len(scenario.Steps))
	for _, step := range scenario.Steps {
		if _, duplicate := steps[step.ID]; duplicate {
			return Manifest{}, errors.New("native scenario step is duplicated")
		}
		steps[step.ID] = step
	}
	wireByStep := make(map[scenarios.StepID]scenarios.WireExpectation, len(scenario.WireExpectations))
	for _, wire := range scenario.WireExpectations {
		wireByStep[wire.StepID] = wire
	}
	expectations := make(map[scenarios.ExpectationID]scenarios.ModelExpectation, len(scenario.Model.ExpectedState))
	for _, expectation := range scenario.Model.ExpectedState {
		expectations[expectation.ID] = expectation
	}

	actions := make([]ManifestAction, 0, len(scenario.NativeExecution.Actions))
	coveredSteps := make(map[scenarios.StepID]struct{}, len(steps))
	for index, action := range scenario.NativeExecution.Actions {
		resolved := ManifestAction{Sequence: index + 1, Action: action}
		switch {
		case action.Actor == "controller" && action.Command == "install-model":
			if len(scenario.Model.Setup) != 1 {
				return Manifest{}, fmt.Errorf("native action %s cannot resolve model setup", action.ID)
			}
			operation := scenario.Model.Setup[0]
			resolved.SetupOperation = &operation
		case len(action.CoversStepIDs) != 0:
			resolved.Steps = make([]scenarios.Step, 0, len(action.CoversStepIDs))
			for _, stepID := range action.CoversStepIDs {
				step, found := steps[stepID]
				if !found {
					return Manifest{}, fmt.Errorf("native action %s cannot resolve step %s", action.ID, stepID)
				}
				if _, duplicate := coveredSteps[stepID]; duplicate {
					return Manifest{}, fmt.Errorf("native step %s is covered more than once", stepID)
				}
				coveredSteps[stepID] = struct{}{}
				resolved.Steps = append(resolved.Steps, step)
				if scenarios.OperationKey(step.Operation) == "workload/prepare" {
					expanded := selection.workloadExpansions[stepID]
					if len(expanded) == 0 {
						return Manifest{}, fmt.Errorf("native workload step %s has no concrete expansion", stepID)
					}
					if resolved.WorkloadExpansions == nil {
						resolved.WorkloadExpansions = make(map[scenarios.StepID][]scenarios.Operation)
					}
					resolved.WorkloadExpansions[stepID] = cloneOperations(expanded)
				}
				if wire, found := wireByStep[stepID]; found {
					resolved.WireExpectations = append(resolved.WireExpectations, wire)
				}
			}
		}
		if action.Actor == "observer" && action.Command == "capture" {
			var parameters scenarios.NativeCaptureParameters
			if err := json.Unmarshal(action.Parameters, &parameters); err != nil {
				return Manifest{}, fmt.Errorf("native action %s cannot resolve capture expectations", action.ID)
			}
			for _, expectationID := range parameters.ExpectationIDs {
				expectation, found := expectations[expectationID]
				if !found {
					return Manifest{}, fmt.Errorf("native action %s cannot resolve expectation %s", action.ID, expectationID)
				}
				resolved.Expectations = append(resolved.Expectations, expectation)
			}
		}
		actions = append(actions, resolved)
	}
	if len(coveredSteps) != len(steps) {
		return Manifest{}, errors.New("native actions do not close the scenario steps")
	}
	if err := validateManifestPerformanceClosure(actions, budgets, measurements); err != nil {
		return Manifest{}, err
	}

	return Manifest{
		SchemaVersion:            manifestSchemaVersion,
		ScenarioID:               string(scenario.ID),
		ScenarioSHA256:           selection.digest,
		PerformanceCatalogSHA256: selection.performanceCatalogSHA256,
		ObligationID:             string(selection.obligation.ObligationID),
		SupportCellID:            selection.supportCellID,
		Component:                selection.component,
		Platform:                 selection.platform,
		MakeTarget:               selection.obligation.MakeTarget,
		PerformanceBudgets:       budgets,
		RequiredMeasurements:     measurements,
		Clients:                  append([]scenarios.NativeClient(nil), scenario.NativeExecution.Clients...),
		Actions:                  actions,
	}, nil
}

// ExecutionAction contains only fields needed for generic dispatch.
type ExecutionAction struct {
	Actor      string
	Command    string
	Parameters json.RawMessage
}

// ExecutionStep contains one operation without its expected outcome.
type ExecutionStep struct {
	ID                scenarios.StepID
	Phase             string
	Transport         string
	Operation         scenarios.Operation
	ExpandedOperations []scenarios.Operation
	MeasurementSample *scenarios.MeasurementSample
}

// BudgetInstruction contains the authored instrumentation input without its verdict limit.
type BudgetInstruction struct {
	ID                contract.BudgetID
	Metric            string
	Unit              string
	DataProfile       contract.DataProfile
	MeasurementMethod contract.MeasurementMethod
}

// MeasurementInstruction contains the authored instrumentation input without closure minima.
type MeasurementInstruction struct {
	ID                contract.MeasurementID
	DataProfile       contract.DataProfile
	MeasurementMethod contract.MeasurementMethod
	Metrics           []contract.PerformanceMetric
	Strata            []contract.PerformanceStratum
}

// ExecuteRequest supplies one sanitized action to the native executor.
type ExecuteRequest struct {
	Action              ExecutionAction
	SetupOperation      *scenarios.Operation
	Steps               []ExecutionStep
	BudgetInstructions  []BudgetInstruction
	MeasurementInstructions []MeasurementInstruction
	Clients             []scenarios.NativeClient
}

// WireObservation records the raw wire result for one covered step.
type WireObservation struct {
	HTTPStatus int     `json:"http_status"`
	ErrorCode  *string `json:"error_code,omitempty"`
	Retryable  bool    `json:"retryable"`
}

// StepObservation binds one covered step to its raw terminal result.
type StepObservation struct {
	StepID      scenarios.StepID `json:"step_id"`
	Disposition string           `json:"disposition"`
	ErrorCode   *string          `json:"error_code,omitempty"`
	Wire        *WireObservation `json:"wire,omitempty"`
}

// CaptureObservation records raw source names and normalized durable facts.
type CaptureObservation struct {
	Sources    []string             `json:"sources"`
	StateFacts scenarios.StateFacts `json:"state_facts"`
}

// SynchronizationResult records one raw public synchronization completion.
type SynchronizationResult struct {
	Completion string `json:"completion"`
}

// BudgetObservation records raw bounded counters for one authored budget.
type BudgetObservation struct {
	BudgetID    contract.BudgetID                `json:"budget_id"`
	Measurement execution.PerformanceMeasurement `json:"measurement"`
}

// MeasurementSampleObservation records exact metric values for one authored sample.
type MeasurementSampleObservation struct {
	MeasurementID contract.MeasurementID  `json:"measurement_id"`
	StratumID     contract.StratumID      `json:"stratum_id"`
	SampleID      string                  `json:"sample_id"`
	MetricValues  []execution.MetricValue `json:"metric_values"`
}

// ProcessBoundaryResult records one raw process boundary.
type ProcessBoundaryResult struct {
	ClientKey                   string  `json:"client_key,omitempty"`
	Boundary                    string  `json:"boundary"`
	AfterActionID               string  `json:"after_action_id"`
	PriorProcessID              string  `json:"prior_process_id"`
	CurrentProcessID            *string `json:"current_process_id,omitempty"`
	TerminationConfirmed        bool    `json:"termination_confirmed"`
	DatabaseIdentityFingerprint string  `json:"database_identity_fingerprint"`
}

// ClientCallResult records one raw public client-call fact.
type ClientCallResult struct {
	CallID     scenarios.NativeCallID `json:"call_id"`
	State      string                 `json:"state"`
	Completion string                 `json:"completion,omitempty"`
}

// ActionResult contains raw observations only.
type ActionResult struct {
	StepObservations   []StepObservation              `json:"step_observations,omitempty"`
	Synchronization    *SynchronizationResult         `json:"synchronization,omitempty"`
	CaptureObservation *CaptureObservation            `json:"capture_observation,omitempty"`
	BudgetObservations []BudgetObservation            `json:"budget_observations,omitempty"`
	MeasurementSamples []MeasurementSampleObservation `json:"measurement_samples,omitempty"`
	ProcessBoundary    *ProcessBoundaryResult         `json:"process_boundary,omitempty"`
	ClientCall         *ClientCallResult              `json:"client_call,omitempty"`
}

// Executor implements the single native action boundary.
type Executor interface {
	Execute(context.Context, ExecuteRequest) (ActionResult, error)
}

// ExpectationResult is one centrally derived semantic result.
type ExpectationResult struct {
	ID      scenarios.ExpectationID `json:"id"`
	Outcome Outcome                 `json:"outcome"`
}

// PerformanceResult is one centrally evaluated budget result.
type PerformanceResult struct {
	BudgetID      contract.BudgetID                `json:"budget_id"`
	ObservedValue float64                          `json:"observed_value"`
	Measurement   execution.PerformanceMeasurement `json:"measurement"`
	Outcome       Outcome                          `json:"outcome"`
}

// MeasurementStratumResult records bounded sample closure for one stratum.
type MeasurementStratumResult struct {
	StratumID   contract.StratumID `json:"stratum_id"`
	SampleCount int                `json:"sample_count"`
}

// RequiredMeasurementResult is one centrally derived closure result.
type RequiredMeasurementResult struct {
	MeasurementID contract.MeasurementID     `json:"measurement_id"`
	Strata        []MeasurementStratumResult `json:"strata"`
	Outcome       Outcome                    `json:"outcome"`
}

// ActionTrace records one ordered, centrally evaluated action.
type ActionTrace struct {
	Sequence                   int                            `json:"sequence"`
	ActionID                   scenarios.NativeActionID       `json:"action_id"`
	Actor                      string                         `json:"actor"`
	Command                    string                         `json:"command"`
	CoveredStepIDs             []scenarios.StepID             `json:"covered_step_ids"`
	Outcome                    Outcome                        `json:"outcome"`
	DurationNanoseconds        int64                          `json:"duration_nanoseconds"`
	StepObservations           []StepObservation              `json:"step_observations,omitempty"`
	Synchronization            *SynchronizationResult         `json:"synchronization,omitempty"`
	CaptureSources             []string                       `json:"capture_sources,omitempty"`
	ExpectationResults         []ExpectationResult            `json:"expectation_results,omitempty"`
	MeasurementSamples         []MeasurementSampleObservation `json:"measurement_samples,omitempty"`
	PerformanceResults         []PerformanceResult            `json:"performance_results,omitempty"`
	RequiredMeasurementResults []RequiredMeasurementResult    `json:"required_measurement_results,omitempty"`
	ProcessBoundary            *ProcessBoundaryResult         `json:"process_boundary,omitempty"`
	ClientCall                 *ClientCallResult              `json:"client_call,omitempty"`
}

// Trace records exact action order and the central terminal outcome.
type Trace struct {
	SchemaVersion            int           `json:"schema_version"`
	ScenarioID               string        `json:"scenario_id"`
	ScenarioSHA256           string        `json:"scenario_sha256"`
	PerformanceCatalogSHA256 string        `json:"performance_catalog_sha256"`
	ObligationID             string        `json:"obligation_id"`
	SupportCellID            string        `json:"support_cell_id"`
	Outcome                  Outcome       `json:"outcome"`
	Actions                  []ActionTrace `json:"actions"`
}

// Run executes every action through one executor in authored order.
func Run(ctx context.Context, selection Selection, executor Executor) (Trace, error) {
	trace := Trace{
		SchemaVersion: traceSchemaVersion,
		Outcome:       OutcomeError,
	}
	manifest, err := BuildManifest(selection)
	if err != nil {
		return trace, err
	}
	trace.ScenarioID = manifest.ScenarioID
	trace.ScenarioSHA256 = manifest.ScenarioSHA256
	trace.PerformanceCatalogSHA256 = manifest.PerformanceCatalogSHA256
	trace.ObligationID = manifest.ObligationID
	trace.SupportCellID = manifest.SupportCellID
	if ctx == nil {
		return trace, errors.New("native execution context is nil")
	}
	if executor == nil {
		return trace, errors.New("native executor is nil")
	}

	state, err := newExecutionState(manifest)
	if err != nil {
		return trace, fmt.Errorf("prepare native observations: %w", err)
	}
	for _, action := range manifest.Actions {
		if err := ctx.Err(); err != nil {
			return trace, fmt.Errorf("native execution canceled before action %s: %w", action.Action.ID, err)
		}
		immutableAction, err := cloneManifestAction(action)
		if err != nil {
			return trace, fmt.Errorf("clone native action %s: %w", action.Action.ID, err)
		}
		requestAction, err := cloneManifestAction(action)
		if err != nil {
			return trace, fmt.Errorf("clone native request %s: %w", action.Action.ID, err)
		}
		request, err := sanitizeExecuteRequest(requestAction, manifest)
		if err != nil {
			return trace, fmt.Errorf("sanitize native request: %w", err)
		}

		started := time.Now()
		result, executeErr := executor.Execute(ctx, request)
		duration, durationErr := boundedDurationSince(started)
		actionTrace := ActionTrace{
			Sequence:            immutableAction.Sequence,
			ActionID:            immutableAction.Action.ID,
			Actor:               immutableAction.Action.Actor,
			Command:             immutableAction.Action.Command,
			CoveredStepIDs:      append([]scenarios.StepID(nil), immutableAction.Action.CoversStepIDs...),
			Outcome:             OutcomeError,
			DurationNanoseconds: duration,
		}
		if durationErr != nil {
			trace.Actions = append(trace.Actions, actionTrace)
			return trace, fmt.Errorf("native action %s duration is invalid", immutableAction.Action.ID)
		}
		if executeErr != nil {
			trace.Actions = append(trace.Actions, actionTrace)
			return trace, executorExecutionError{actionID: immutableAction.Action.ID, cause: executeErr}
		}

		central, err := state.acceptActionResult(immutableAction, result)
		if err != nil {
			trace.Actions = append(trace.Actions, actionTrace)
			return trace, fmt.Errorf("validate native action %s observations: %w", immutableAction.Action.ID, err)
		}
		actionTrace.Outcome = central.outcome
		actionTrace.StepObservations = cloneStepObservations(central.stepObservations)
		actionTrace.Synchronization = cloneSynchronizationResult(central.synchronization)
		actionTrace.CaptureSources = append([]string(nil), central.captureSources...)
		actionTrace.ExpectationResults = append([]ExpectationResult(nil), central.expectationResults...)
		actionTrace.MeasurementSamples = cloneMeasurementSamples(central.measurementSamples)
		actionTrace.PerformanceResults = append([]PerformanceResult(nil), central.performanceResults...)
		actionTrace.RequiredMeasurementResults = cloneRequiredMeasurementResults(central.requiredMeasurementResults)
		actionTrace.ProcessBoundary = traceProcessBoundary(central.processBoundary)
		actionTrace.ClientCall = cloneClientCall(central.clientCall)
		trace.Actions = append(trace.Actions, actionTrace)
		if central.outcome == OutcomeFailed {
			trace.Outcome = OutcomeFailed
			return trace, fmt.Errorf("native action %s failed central evaluation", immutableAction.Action.ID)
		}
	}
	if err := state.close(); err != nil {
		return trace, fmt.Errorf("close native observations: %w", err)
	}
	trace.Outcome = OutcomePassed
	return trace, nil
}

func sanitizeExecuteRequest(action ManifestAction, manifest Manifest) (ExecuteRequest, error) {
	request := ExecuteRequest{
		Action: ExecutionAction{
			Actor:      action.Action.Actor,
			Command:    action.Action.Command,
			Parameters: append([]byte(nil), action.Action.Parameters...),
		},
		Clients: append([]scenarios.NativeClient(nil), manifest.Clients...),
	}
	if action.SetupOperation != nil {
		operation := cloneExecutionOperation(*action.SetupOperation)
		request.SetupOperation = &operation
	}
	request.Steps = make([]ExecutionStep, 0, len(action.Steps))
	for _, step := range action.Steps {
		resolved := ExecutionStep{ID: step.ID, Phase: step.Phase, Transport: step.Transport, Operation: cloneExecutionOperation(step.Operation)}
		resolved.ExpandedOperations = cloneOperations(action.WorkloadExpansions[step.ID])
		if step.MeasurementSample != nil {
			sample := *step.MeasurementSample
			sample.Parameters = append([]byte(nil), sample.Parameters...)
			resolved.MeasurementSample = &sample
		}
		request.Steps = append(request.Steps, resolved)
	}
	request.BudgetInstructions = make([]BudgetInstruction, 0, len(manifest.PerformanceBudgets))
	for _, definition := range manifest.PerformanceBudgets {
		request.BudgetInstructions = append(request.BudgetInstructions, BudgetInstruction{
			ID:                definition.ID,
			Metric:            definition.Metric,
			Unit:              definition.Unit,
			DataProfile:       contract.DataProfile{ProfileType: definition.DataProfile.ProfileType, Parameters: append([]byte(nil), definition.DataProfile.Parameters...)},
			MeasurementMethod: definition.MeasurementMethod,
		})
	}
	request.MeasurementInstructions = make([]MeasurementInstruction, 0, len(manifest.RequiredMeasurements))
	for _, definition := range manifest.RequiredMeasurements {
		instruction := MeasurementInstruction{
			ID:                definition.ID,
			DataProfile:       contract.DataProfile{ProfileType: definition.DataProfile.ProfileType, Parameters: append([]byte(nil), definition.DataProfile.Parameters...)},
			MeasurementMethod: definition.MeasurementMethod,
			Metrics:           append([]contract.PerformanceMetric(nil), definition.Metrics...),
			Strata:            append([]contract.PerformanceStratum(nil), definition.Strata...),
		}
		for index := range instruction.Strata {
			instruction.Strata[index].Parameters = append([]byte(nil), instruction.Strata[index].Parameters...)
		}
		request.MeasurementInstructions = append(request.MeasurementInstructions, instruction)
	}
	return request, nil
}

func cloneExecutionOperation(operation scenarios.Operation) scenarios.Operation {
	operation.Payload = append([]byte(nil), operation.Payload...)
	return operation
}

func cloneOperations(source []scenarios.Operation) []scenarios.Operation {
	result := make([]scenarios.Operation, len(source))
	for index, operation := range source {
		result[index] = cloneExecutionOperation(operation)
	}
	return result
}

type centralActionResult struct {
	outcome                    Outcome
	stepObservations           []StepObservation
	synchronization            *SynchronizationResult
	captureSources             []string
	expectationResults         []ExpectationResult
	measurementSamples         []MeasurementSampleObservation
	performanceResults         []PerformanceResult
	requiredMeasurementResults []RequiredMeasurementResult
	processBoundary            *ProcessBoundaryResult
	clientCall                 *ClientCallResult
}

type sampleKey struct {
	measurementID contract.MeasurementID
	stratumID     contract.StratumID
	sampleID      string
}

type expectedSample struct {
	key     sampleKey
	stepID  scenarios.StepID
	metrics []contract.PerformanceMetric
}

type executionState struct {
	budgets               map[contract.BudgetID]contract.PerformanceBudget
	measurements          map[contract.MeasurementID]contract.RequiredMeasurement
	expectedSamples       map[sampleKey]expectedSample
	expectedByMeasurement map[contract.MeasurementID][]expectedSample
	observedSamples       map[sampleKey]MeasurementSampleObservation
	expectedSteps         map[scenarios.StepID]scenarios.Step
	wireByStep            map[scenarios.StepID]scenarios.WireExpectation
	observedSteps         map[scenarios.StepID]StepObservation
	evaluatedBudgets      map[contract.BudgetID]struct{}
	evaluatedMeasurements map[contract.MeasurementID]struct{}
	activeCalls           map[scenarios.NativeCallID]string
	activeCallByClient    map[string]scenarios.NativeCallID
	closedCalls           map[scenarios.NativeCallID]struct{}
	terminatedProcesses   map[string]processTermination
}

type processTermination struct {
	actionID                    scenarios.NativeActionID
	boundary                    string
	priorProcessID              string
	databaseIdentityFingerprint string
}

func newExecutionState(manifest Manifest) (*executionState, error) {
	state := &executionState{
		budgets:               make(map[contract.BudgetID]contract.PerformanceBudget, len(manifest.PerformanceBudgets)),
		measurements:          make(map[contract.MeasurementID]contract.RequiredMeasurement, len(manifest.RequiredMeasurements)),
		expectedSamples:       make(map[sampleKey]expectedSample),
		expectedByMeasurement: make(map[contract.MeasurementID][]expectedSample),
		observedSamples:       make(map[sampleKey]MeasurementSampleObservation),
		expectedSteps:         make(map[scenarios.StepID]scenarios.Step),
		wireByStep:            make(map[scenarios.StepID]scenarios.WireExpectation),
		observedSteps:         make(map[scenarios.StepID]StepObservation),
		evaluatedBudgets:      make(map[contract.BudgetID]struct{}),
		evaluatedMeasurements: make(map[contract.MeasurementID]struct{}),
		activeCalls:           make(map[scenarios.NativeCallID]string),
		activeCallByClient:    make(map[string]scenarios.NativeCallID),
		closedCalls:           make(map[scenarios.NativeCallID]struct{}),
		terminatedProcesses:   make(map[string]processTermination),
	}
	for _, budget := range manifest.PerformanceBudgets {
		if _, duplicate := state.budgets[budget.ID]; duplicate {
			return nil, errors.New("performance budget definition is duplicated")
		}
		state.budgets[budget.ID] = budget
	}
	for _, measurement := range manifest.RequiredMeasurements {
		if _, duplicate := state.measurements[measurement.ID]; duplicate {
			return nil, errors.New("required measurement definition is duplicated")
		}
		state.measurements[measurement.ID] = measurement
	}
	for _, action := range manifest.Actions {
		for _, wire := range action.WireExpectations {
			if _, duplicate := state.wireByStep[wire.StepID]; duplicate {
				return nil, errors.New("wire expectation is bound more than once")
			}
			state.wireByStep[wire.StepID] = wire
		}
		for _, step := range action.Steps {
			if _, duplicate := state.expectedSteps[step.ID]; duplicate {
				return nil, errors.New("covered step is bound more than once")
			}
			state.expectedSteps[step.ID] = step
			if step.MeasurementSample == nil {
				continue
			}
			sample := step.MeasurementSample
			definition, found := state.measurements[sample.MeasurementID]
			if !found {
				return nil, errors.New("authored sample has no selected measurement definition")
			}
			if !measurementHasStratum(definition, sample.StratumID) {
				return nil, errors.New("authored sample has no selected stratum definition")
			}
			key := sampleKey{measurementID: sample.MeasurementID, stratumID: sample.StratumID, sampleID: sample.SampleID}
			if key.sampleID == "" {
				return nil, errors.New("authored sample identity is incomplete")
			}
			if _, duplicate := state.expectedSamples[key]; duplicate {
				return nil, errors.New("authored sample tuple is duplicated")
			}
			expected := expectedSample{key: key, stepID: step.ID, metrics: append([]contract.PerformanceMetric(nil), definition.Metrics...)}
			state.expectedSamples[key] = expected
			state.expectedByMeasurement[sample.MeasurementID] = append(state.expectedByMeasurement[sample.MeasurementID], expected)
		}
	}
	return state, nil
}

func (s *executionState) acceptActionResult(action ManifestAction, result ActionResult) (centralActionResult, error) {
	steps, stepFailure, err := s.acceptSteps(action, result.StepObservations)
	if err != nil {
		return centralActionResult{}, err
	}
	samples, err := s.acceptSamples(action, result.MeasurementSamples)
	if err != nil {
		return centralActionResult{}, err
	}
	central := centralActionResult{
		outcome:            OutcomePassed,
		stepObservations:   steps,
		measurementSamples: samples,
	}
	if stepFailure {
		central.outcome = OutcomeFailed
	}
	synchronization, synchronizationFailure, err := evaluateSynchronization(action, result.Synchronization)
	if err != nil {
		return centralActionResult{}, err
	}
	central.synchronization = synchronization
	if synchronizationFailure {
		central.outcome = OutcomeFailed
	}

	switch action.Action.Actor + "/" + action.Action.Command {
	case "observer/capture":
		if len(result.BudgetObservations) != 0 || result.ProcessBoundary != nil || result.ClientCall != nil {
			return centralActionResult{}, errors.New("capture action contains unrelated raw facts")
		}
		sources, expectationResults, failed, err := s.evaluateCapture(action, result.CaptureObservation)
		if err != nil {
			return centralActionResult{}, err
		}
		central.captureSources = sources
		central.expectationResults = expectationResults
		if failed {
			central.outcome = OutcomeFailed
		}
	case "observer/measure":
		if result.CaptureObservation != nil || result.ProcessBoundary != nil || result.ClientCall != nil {
			return centralActionResult{}, errors.New("measure action contains unrelated raw facts")
		}
		var parameters scenarios.NativeMeasureParameters
		if err := json.Unmarshal(action.Action.Parameters, &parameters); err != nil {
			return centralActionResult{}, errors.New("measure parameters are invalid")
		}
		budgetResults, budgetFailure, err := s.evaluateBudgets(parameters.PerformanceBudgetIDs, result.BudgetObservations)
		if err != nil {
			return centralActionResult{}, err
		}
		measurementResults, err := s.evaluateRequiredMeasurements(parameters.MeasurementIDs)
		if err != nil {
			return centralActionResult{}, err
		}
		central.performanceResults = budgetResults
		central.requiredMeasurementResults = measurementResults
		if budgetFailure {
			central.outcome = OutcomeFailed
		}
	case "process/terminate", "process/relaunch":
		if result.CaptureObservation != nil || len(result.BudgetObservations) != 0 || result.ClientCall != nil {
			return centralActionResult{}, errors.New("process action contains unrelated raw facts")
		}
		boundary, err := s.evaluateProcessBoundary(action, result.ProcessBoundary)
		if err != nil {
			return centralActionResult{}, err
		}
		central.processBoundary = boundary
	case "client/begin-call", "client/await-call":
		if result.CaptureObservation != nil || len(result.BudgetObservations) != 0 || result.ProcessBoundary != nil {
			return centralActionResult{}, errors.New("client-call action contains unrelated raw facts")
		}
		call, failed, err := s.evaluateClientCall(action, result.ClientCall)
		if err != nil {
			return centralActionResult{}, err
		}
		central.clientCall = call
		if failed {
			central.outcome = OutcomeFailed
		}
	default:
		if result.CaptureObservation != nil || len(result.BudgetObservations) != 0 || result.ProcessBoundary != nil || result.ClientCall != nil {
			return centralActionResult{}, errors.New("action contains unrelated raw facts")
		}
	}
	return central, nil
}

func evaluateSynchronization(action ManifestAction, observation *SynchronizationResult) (*SynchronizationResult, bool, error) {
	if action.Action.Actor != "client" || action.Action.Command != "synchronize-step" {
		if observation != nil {
			return nil, false, errors.New("synchronization observation is unrelated to the action")
		}
		return nil, false, nil
	}
	if observation == nil {
		return nil, false, errors.New("synchronization observation is missing")
	}
	if observation.Completion != "idle" && observation.Completion != "blocked" && observation.Completion != "error" {
		return nil, false, errors.New("synchronization completion is invalid")
	}
	var parameters scenarios.NativeSynchronizeParameters
	if err := json.Unmarshal(action.Action.Parameters, &parameters); err != nil {
		return nil, false, errors.New("synchronization parameters are invalid")
	}
	result := &SynchronizationResult{Completion: observation.Completion}
	return result, observation.Completion != parameters.Completion, nil
}

func (s *executionState) acceptSteps(action ManifestAction, values []StepObservation) ([]StepObservation, bool, error) {
	if len(values) != len(action.Steps) {
		return nil, false, errors.New("step observations do not close the covered steps")
	}
	actual := make(map[scenarios.StepID]StepObservation, len(values))
	for _, value := range values {
		if value.StepID == "" {
			return nil, false, errors.New("step observation identity is incomplete")
		}
		if _, duplicate := actual[value.StepID]; duplicate {
			return nil, false, errors.New("step observation is duplicated")
		}
		if value.Disposition != "success" && value.Disposition != "error" {
			return nil, false, errors.New("step observation disposition is invalid")
		}
		if value.Wire != nil && value.Wire.HTTPStatus != 0 && (value.Wire.HTTPStatus < 100 || value.Wire.HTTPStatus > 599) {
			return nil, false, errors.New("step wire observation status is invalid")
		}
		if invalidOptionalCode(value.ErrorCode) || value.Wire != nil && invalidOptionalCode(value.Wire.ErrorCode) {
			return nil, false, errors.New("step observation error code is invalid")
		}
		actual[value.StepID] = value
	}
	wire := make(map[scenarios.StepID]scenarios.WireExpectation, len(action.WireExpectations))
	for _, expectation := range action.WireExpectations {
		if _, duplicate := wire[expectation.StepID]; duplicate {
			return nil, false, errors.New("covered step has duplicate wire expectations")
		}
		wire[expectation.StepID] = expectation
	}

	normalized := make([]StepObservation, 0, len(action.Steps))
	failed := false
	for _, step := range action.Steps {
		value, found := actual[step.ID]
		if !found {
			return nil, false, errors.New("step observation is missing or unbound")
		}
		if _, duplicate := s.observedSteps[step.ID]; duplicate {
			return nil, false, errors.New("covered step was already observed")
		}
		expectedWire, hasWire := wire[step.ID]
		if hasWire != (value.Wire != nil) {
			return nil, false, errors.New("wire observation does not close the covered step")
		}
		if value.Disposition != step.ExpectedOutcome.Disposition || !sameOptionalString(value.ErrorCode, step.ExpectedOutcome.ErrorCode) {
			failed = true
		}
		if hasWire && (value.Wire.HTTPStatus != expectedWire.HTTPStatus || value.Wire.Retryable != expectedWire.Retryable || !sameOptionalString(value.Wire.ErrorCode, expectedWire.ErrorCode)) {
			failed = true
		}
		bounded := cloneStepObservation(value)
		if !sameOptionalString(value.ErrorCode, step.ExpectedOutcome.ErrorCode) {
			bounded.ErrorCode = nil
		}
		if hasWire && !sameOptionalString(value.Wire.ErrorCode, expectedWire.ErrorCode) {
			bounded.Wire.ErrorCode = nil
		}
		normalized = append(normalized, bounded)
		delete(actual, step.ID)
	}
	if len(actual) != 0 {
		return nil, false, errors.New("step observation is extra or unbound")
	}
	for _, value := range normalized {
		s.observedSteps[value.StepID] = cloneStepObservation(value)
	}
	return normalized, failed, nil
}

func (s *executionState) evaluateCapture(action ManifestAction, observation *CaptureObservation) ([]string, []ExpectationResult, bool, error) {
	if observation == nil {
		return nil, nil, false, errors.New("capture observation is missing")
	}
	var parameters scenarios.NativeCaptureParameters
	if err := json.Unmarshal(action.Action.Parameters, &parameters); err != nil {
		return nil, nil, false, errors.New("capture parameters are invalid")
	}
	if !sameStrings(expectationStrings(parameters.ExpectationIDs), authoredExpectationStrings(action.Expectations)) {
		return nil, nil, false, errors.New("capture parameters do not match the authored expectations")
	}
	if !sameStrings(parameters.Sources, observation.Sources) {
		return nil, nil, false, errors.New("capture sources do not close the authored action")
	}
	facts, err := normalizeStateFacts(observation.StateFacts)
	if err != nil {
		return nil, nil, false, fmt.Errorf("capture state facts: %w", err)
	}
	results := make([]ExpectationResult, 0, len(action.Expectations))
	failed := false
	for _, expectation := range action.Expectations {
		outcome := OutcomePassed
		switch expectation.Predicate.ContractPredicate {
		case "state-equality":
			if expectation.StateFacts == nil {
				return nil, nil, false, errors.New("state expectation has no authored facts")
			}
			want, err := normalizeStateFacts(*expectation.StateFacts)
			if err != nil {
				return nil, nil, false, errors.New("authored state facts are invalid")
			}
			if !stateFactsProjectionEqual(want, facts) {
				outcome = OutcomeFailed
			}
		case "wire-outcome":
			if err := s.validateWireClosure(); err != nil {
				return nil, nil, false, err
			}
		case "state-transition":
			if err := s.validateTransitionClosure(expectation); err != nil {
				return nil, nil, false, err
			}
		case "performance-measurement":
			continue
		default:
			return nil, nil, false, fmt.Errorf("expectation %s has no raw central observation", expectation.ID)
		}
		failed = failed || outcome == OutcomeFailed
		results = append(results, ExpectationResult{ID: expectation.ID, Outcome: outcome})
	}
	return append([]string(nil), parameters.Sources...), results, failed, nil
}

func (s *executionState) validateWireClosure() error {
	for stepID := range s.wireByStep {
		if _, found := s.observedSteps[stepID]; !found {
			return errors.New("wire expectation has no bound raw step observation")
		}
	}
	return nil
}

func (s *executionState) validateTransitionClosure(expectation scenarios.ModelExpectation) error {
	if len(s.observedSteps) != len(s.expectedSteps) {
		return errors.New("state transition has incomplete covered-step observations")
	}
	if expectation.Predicate.Name != "schema-dispatch-observations-satisfied" {
		return errors.New("state transition has no raw central evaluator")
	}
	plan, err := scenarios.DecodeSchemaDispatchMeasurementPlan(expectation.Predicate.Payload)
	if err != nil {
		return errors.New("schema-dispatch transition plan is invalid")
	}
	expected := s.expectedByMeasurement[plan.MeasurementID]
	if len(expected) == 0 {
		return errors.New("schema-dispatch transition has no authored samples")
	}
	counts := make(map[contract.StratumID]uint64, len(plan.Strata))
	for _, sample := range expected {
		if _, observed := s.observedSamples[sample.key]; !observed {
			return errors.New("schema-dispatch transition is missing a raw sample")
		}
		counts[sample.key.stratumID]++
	}
	for _, stratum := range plan.Strata {
		if counts[stratum.StratumID] < plan.MinimumSampleCountPerStratum {
			return errors.New("schema-dispatch transition does not close an authored stratum")
		}
		delete(counts, stratum.StratumID)
	}
	if len(counts) != 0 {
		return errors.New("schema-dispatch transition contains an unbound stratum")
	}
	return nil
}

func (s *executionState) evaluateProcessBoundary(action ManifestAction, observation *ProcessBoundaryResult) (*ProcessBoundaryResult, error) {
	if observation == nil {
		return nil, errors.New("process boundary observation is missing")
	}
	var parameters scenarios.NativeProcessBoundaryParameters
	if err := json.Unmarshal(action.Action.Parameters, &parameters); err != nil {
		return nil, errors.New("process boundary parameters are invalid")
	}
	if observation.ClientKey != parameters.ClientKey || observation.Boundary != parameters.Boundary || observation.AfterActionID != string(parameters.AfterActionID) {
		return nil, errors.New("process boundary observation is unbound")
	}
	if !validProcessIdentifier(observation.PriorProcessID) || !validSHA256(observation.DatabaseIdentityFingerprint) {
		return nil, errors.New("process boundary identity evidence is invalid")
	}
	switch action.Action.Command {
	case "terminate":
		if _, active := s.activeCallByClient[parameters.ClientKey]; active {
			return nil, errors.New("process termination cannot interrupt an active client call")
		}
		if _, terminated := s.terminatedProcesses[parameters.ClientKey]; terminated {
			return nil, errors.New("process termination observation is duplicated")
		}
		if !observation.TerminationConfirmed || observation.CurrentProcessID != nil {
			return nil, errors.New("process termination is not confirmed")
		}
		s.terminatedProcesses[parameters.ClientKey] = processTermination{
			actionID:                    action.Action.ID,
			boundary:                    parameters.Boundary,
			priorProcessID:              observation.PriorProcessID,
			databaseIdentityFingerprint: observation.DatabaseIdentityFingerprint,
		}
	case "relaunch":
		termination, found := s.terminatedProcesses[parameters.ClientKey]
		if !found || termination.actionID != parameters.AfterActionID || termination.boundary != parameters.Boundary {
			return nil, errors.New("process relaunch has no bound termination evidence")
		}
		if observation.CurrentProcessID == nil || !validProcessIdentifier(*observation.CurrentProcessID) {
			return nil, errors.New("process relaunch current process is invalid")
		}
		if observation.PriorProcessID != termination.priorProcessID || *observation.CurrentProcessID == observation.PriorProcessID {
			return nil, errors.New("process relaunch does not prove a distinct process")
		}
		if observation.DatabaseIdentityFingerprint != termination.databaseIdentityFingerprint {
			return nil, errors.New("process relaunch changed the database identity")
		}
		delete(s.terminatedProcesses, parameters.ClientKey)
	default:
		return nil, errors.New("process boundary observation is unrelated to the action")
	}
	return cloneProcessBoundary(observation), nil
}

func (s *executionState) evaluateClientCall(action ManifestAction, observation *ClientCallResult) (*ClientCallResult, bool, error) {
	if observation == nil {
		return nil, false, errors.New("client-call observation is missing")
	}
	switch action.Action.Command {
	case "begin-call":
		var parameters scenarios.NativeBeginCallParameters
		if err := json.Unmarshal(action.Action.Parameters, &parameters); err != nil {
			return nil, false, errors.New("begin-call parameters are invalid")
		}
		if observation.CallID != parameters.CallID {
			return nil, false, errors.New("begin-call observation is unbound")
		}
		if _, active := s.activeCalls[parameters.CallID]; active {
			return nil, false, errors.New("begin-call observation is duplicated")
		}
		if _, closed := s.closedCalls[parameters.CallID]; closed {
			return nil, false, errors.New("begin-call observation reuses a closed call")
		}
		if _, active := s.activeCallByClient[parameters.ClientKey]; active {
			return nil, false, errors.New("begin-call observation overlaps an active client call")
		}
		failed := observation.State != "in_flight" || observation.Completion != ""
		if !failed {
			s.activeCalls[parameters.CallID] = parameters.ClientKey
			s.activeCallByClient[parameters.ClientKey] = parameters.CallID
		}
		return boundedClientCall(observation, parameters.CallID, "in_flight", ""), failed, nil
	case "await-call":
		var parameters scenarios.NativeAwaitCallParameters
		if err := json.Unmarshal(action.Action.Parameters, &parameters); err != nil {
			return nil, false, errors.New("await-call parameters are invalid")
		}
		if observation.CallID != parameters.CallID {
			return nil, false, errors.New("await-call observation is unbound")
		}
		clientKey, active := s.activeCalls[parameters.CallID]
		if !active || clientKey != parameters.ClientKey {
			return nil, false, errors.New("await-call observation has no active call")
		}
		failed := observation.State != "completed" || observation.Completion != parameters.Completion
		if !failed {
			delete(s.activeCalls, parameters.CallID)
			delete(s.activeCallByClient, parameters.ClientKey)
			s.closedCalls[parameters.CallID] = struct{}{}
		}
		return boundedClientCall(observation, parameters.CallID, "completed", parameters.Completion), failed, nil
	default:
		return nil, false, errors.New("client-call observation is unbound")
	}
}

func (s *executionState) acceptSamples(action ManifestAction, values []MeasurementSampleObservation) ([]MeasurementSampleObservation, error) {
	expected := make([]expectedSample, 0)
	for _, step := range action.Steps {
		if step.MeasurementSample == nil {
			continue
		}
		key := sampleKey{
			measurementID: step.MeasurementSample.MeasurementID,
			stratumID:     step.MeasurementSample.StratumID,
			sampleID:      step.MeasurementSample.SampleID,
		}
		binding, found := s.expectedSamples[key]
		if !found || binding.stepID != step.ID {
			return nil, errors.New("sampled step has no central measurement binding")
		}
		expected = append(expected, binding)
	}
	if len(values) != len(expected) {
		return nil, errors.New("measurement sample observations do not close the authored action")
	}
	actual := make(map[sampleKey]MeasurementSampleObservation, len(values))
	for _, value := range values {
		key := sampleKey{measurementID: value.MeasurementID, stratumID: value.StratumID, sampleID: value.SampleID}
		if _, duplicate := actual[key]; duplicate {
			return nil, errors.New("measurement sample tuple is duplicated")
		}
		actual[key] = value
	}
	normalized := make([]MeasurementSampleObservation, 0, len(expected))
	for _, authored := range expected {
		value, found := actual[authored.key]
		if !found {
			return nil, errors.New("measurement sample does not match its authored sampled step")
		}
		if _, duplicate := s.observedSamples[authored.key]; duplicate {
			return nil, errors.New("measurement sample tuple was already observed")
		}
		metricValues, err := normalizeMetricValues(authored.metrics, value.MetricValues)
		if err != nil {
			return nil, err
		}
		normalized = append(normalized, MeasurementSampleObservation{
			MeasurementID: authored.key.measurementID,
			StratumID:     authored.key.stratumID,
			SampleID:      authored.key.sampleID,
			MetricValues:  metricValues,
		})
		delete(actual, authored.key)
	}
	if len(actual) != 0 {
		return nil, errors.New("measurement sample is extra or unbound")
	}
	for _, value := range normalized {
		key := sampleKey{measurementID: value.MeasurementID, stratumID: value.StratumID, sampleID: value.SampleID}
		s.observedSamples[key] = value
	}
	return normalized, nil
}

func normalizeMetricValues(metrics []contract.PerformanceMetric, values []execution.MetricValue) ([]execution.MetricValue, error) {
	if len(values) != len(metrics) {
		return nil, errors.New("measurement sample metric values do not match the authored metric set")
	}
	actual := make(map[string]float64, len(values))
	for _, value := range values {
		if value.MetricID == "" {
			return nil, errors.New("measurement sample metric ID is incomplete")
		}
		if _, duplicate := actual[value.MetricID]; duplicate {
			return nil, errors.New("measurement sample metric ID is duplicated")
		}
		if !performance.IsBoundedObservation(value.Value) {
			return nil, errors.New("measurement sample metric value is not finite and bounded")
		}
		actual[value.MetricID] = value.Value
	}
	normalized := make([]execution.MetricValue, 0, len(metrics))
	for _, metric := range metrics {
		value, found := actual[string(metric.ID)]
		if !found {
			return nil, errors.New("measurement sample metric values do not match the authored metric set")
		}
		normalized = append(normalized, execution.MetricValue{MetricID: string(metric.ID), Value: value})
		delete(actual, string(metric.ID))
	}
	if len(actual) != 0 {
		return nil, errors.New("measurement sample metric value is extra or unbound")
	}
	return normalized, nil
}

func (s *executionState) evaluateBudgets(ids []contract.BudgetID, observations []BudgetObservation) ([]PerformanceResult, bool, error) {
	if len(ids) != len(observations) {
		return nil, false, errors.New("budget observations do not close the authored action")
	}
	actual := make(map[contract.BudgetID]BudgetObservation, len(observations))
	for _, observation := range observations {
		if observation.BudgetID == "" {
			return nil, false, errors.New("budget observation identity is incomplete")
		}
		if _, duplicate := actual[observation.BudgetID]; duplicate {
			return nil, false, errors.New("budget observation ID is duplicated")
		}
		actual[observation.BudgetID] = observation
	}
	results := make([]PerformanceResult, 0, len(ids))
	hasFailure := false
	for _, id := range ids {
		observation, found := actual[id]
		definition, defined := s.budgets[id]
		if !found || !defined {
			return nil, false, errors.New("budget observation is missing or unbound")
		}
		if _, duplicate := s.evaluatedBudgets[id]; duplicate {
			return nil, false, errors.New("budget observation was already evaluated")
		}
		evaluation, err := performance.EvaluateBudget(definition, observation.Measurement)
		if err != nil {
			return nil, false, fmt.Errorf("evaluate authored budget: %w", err)
		}
		outcome := OutcomePassed
		if !evaluation.Passed {
			outcome = OutcomeFailed
			hasFailure = true
		}
		results = append(results, PerformanceResult{
			BudgetID:      id,
			ObservedValue: evaluation.ObservedValue,
			Measurement:   observation.Measurement,
			Outcome:       outcome,
		})
		s.evaluatedBudgets[id] = struct{}{}
		delete(actual, id)
	}
	if len(actual) != 0 {
		return nil, false, errors.New("budget observation is extra or unbound")
	}
	return results, hasFailure, nil
}

func (s *executionState) evaluateRequiredMeasurements(ids []contract.MeasurementID) ([]RequiredMeasurementResult, error) {
	results := make([]RequiredMeasurementResult, 0, len(ids))
	seen := make(map[contract.MeasurementID]struct{}, len(ids))
	for _, id := range ids {
		if _, duplicate := seen[id]; duplicate {
			return nil, errors.New("required measurement ID is duplicated")
		}
		seen[id] = struct{}{}
		definition, found := s.measurements[id]
		if !found {
			return nil, errors.New("required measurement ID is unbound")
		}
		if _, duplicate := s.evaluatedMeasurements[id]; duplicate {
			return nil, errors.New("required measurement was already evaluated")
		}
		expected := s.expectedByMeasurement[id]
		if len(expected) == 0 {
			return nil, errors.New("required measurement has no authored sampled steps")
		}
		counts := make(map[contract.StratumID]int, len(definition.Strata))
		for _, sample := range expected {
			if _, observed := s.observedSamples[sample.key]; !observed {
				return nil, errors.New("required measurement is missing an authored sample")
			}
			counts[sample.key.stratumID]++
		}
		minimum, err := definition.MinimumSampleCountPerStratum.Int64()
		if err != nil || minimum <= 0 || float64(minimum) > performance.MaximumObservationMagnitude {
			return nil, errors.New("required measurement minimum sample count is invalid")
		}
		strata := make([]MeasurementStratumResult, 0, len(definition.Strata))
		for _, stratum := range definition.Strata {
			count := counts[stratum.StratumID]
			if int64(count) < minimum {
				return nil, errors.New("required measurement does not satisfy minimum sample closure")
			}
			strata = append(strata, MeasurementStratumResult{StratumID: stratum.StratumID, SampleCount: count})
			delete(counts, stratum.StratumID)
		}
		if len(counts) != 0 {
			return nil, errors.New("required measurement contains an unauthored stratum")
		}
		results = append(results, RequiredMeasurementResult{MeasurementID: id, Strata: strata, Outcome: OutcomePassed})
		s.evaluatedMeasurements[id] = struct{}{}
	}
	return results, nil
}

func (s *executionState) close() error {
	if len(s.observedSteps) != len(s.expectedSteps) {
		return errors.New("covered step observations are incomplete")
	}
	if len(s.observedSamples) != len(s.expectedSamples) {
		return errors.New("measurement sample observations are incomplete")
	}
	if len(s.evaluatedBudgets) != len(s.budgets) {
		return errors.New("performance budget observations are incomplete")
	}
	if len(s.evaluatedMeasurements) != len(s.measurements) {
		return errors.New("required measurement observations are incomplete")
	}
	if len(s.activeCalls) != 0 || len(s.activeCallByClient) != 0 {
		return errors.New("client-call observations contain an active call")
	}
	if len(s.terminatedProcesses) != 0 {
		return errors.New("process boundary observations contain an unrelaunched process")
	}
	return nil
}

func validateSelectedDefinitions(selection Selection, budgets []contract.PerformanceBudget, measurements []contract.RequiredMeasurement) error {
	if !sameStrings(budgetStrings(selection.obligation.PerformanceBudgetIDs), budgetDefinitionStrings(budgets)) {
		return errors.New("native selection budget definitions do not match its obligation")
	}
	if !sameStrings(measurementStrings(selection.obligation.RequiredMeasurementIDs), measurementDefinitionStrings(measurements)) {
		return errors.New("native selection measurement definitions do not match its obligation")
	}
	supportID := contract.SupportCellID(selection.supportCellID)
	for _, budget := range budgets {
		if budget.ScenarioID != selection.scenario.ID || !containsSupportCell(budget.SupportCellIDs, supportID) {
			return errors.New("native selection has an unrelated budget definition")
		}
	}
	for _, measurement := range measurements {
		if measurement.ScenarioID != selection.scenario.ID || !containsSupportCell(measurement.SupportCellIDs, supportID) {
			return errors.New("native selection has an unrelated measurement definition")
		}
		if len(measurement.Metrics) == 0 || len(measurement.Strata) == 0 {
			return errors.New("native selection has an incomplete measurement definition")
		}
	}
	return nil
}

func validateManifestPerformanceClosure(actions []ManifestAction, budgets []contract.PerformanceBudget, measurements []contract.RequiredMeasurement) error {
	budgetDefinitions := make(map[contract.BudgetID]struct{}, len(budgets))
	for _, budget := range budgets {
		if _, duplicate := budgetDefinitions[budget.ID]; duplicate {
			return errors.New("native manifest has a duplicate budget definition")
		}
		budgetDefinitions[budget.ID] = struct{}{}
	}
	measurementDefinitions := make(map[contract.MeasurementID]struct{}, len(measurements))
	for _, measurement := range measurements {
		if _, duplicate := measurementDefinitions[measurement.ID]; duplicate {
			return errors.New("native manifest has a duplicate measurement definition")
		}
		measurementDefinitions[measurement.ID] = struct{}{}
	}
	seenBudgets := make(map[contract.BudgetID]struct{}, len(budgets))
	seenMeasurements := make(map[contract.MeasurementID]struct{}, len(measurements))
	for _, action := range actions {
		for _, step := range action.Steps {
			if step.MeasurementSample != nil {
				if _, found := measurementDefinitions[step.MeasurementSample.MeasurementID]; !found {
					return errors.New("native manifest sampled step has no selected measurement definition")
				}
			}
		}
		if action.Action.Actor != "observer" || action.Action.Command != "measure" {
			continue
		}
		var parameters scenarios.NativeMeasureParameters
		if err := json.Unmarshal(action.Action.Parameters, &parameters); err != nil {
			return fmt.Errorf("native action %s has invalid measure parameters", action.Action.ID)
		}
		for _, id := range parameters.PerformanceBudgetIDs {
			if _, found := budgetDefinitions[id]; !found {
				return errors.New("native measure action references an unselected budget")
			}
			if _, duplicate := seenBudgets[id]; duplicate {
				return errors.New("native measure action duplicates a selected budget")
			}
			seenBudgets[id] = struct{}{}
		}
		for _, id := range parameters.MeasurementIDs {
			if _, found := measurementDefinitions[id]; !found {
				return errors.New("native measure action references an unselected measurement")
			}
			if _, duplicate := seenMeasurements[id]; duplicate {
				return errors.New("native measure action duplicates a selected measurement")
			}
			seenMeasurements[id] = struct{}{}
		}
	}
	if len(seenBudgets) != len(budgetDefinitions) || len(seenMeasurements) != len(measurementDefinitions) {
		return errors.New("native measure actions do not close selected performance definitions")
	}
	return nil
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	leftCopy := append([]string(nil), left...)
	rightCopy := append([]string(nil), right...)
	sort.Strings(leftCopy)
	sort.Strings(rightCopy)
	for index := range leftCopy {
		if leftCopy[index] != rightCopy[index] || index > 0 && leftCopy[index] == leftCopy[index-1] {
			return false
		}
	}
	return true
}

func expectationStrings(ids []scenarios.ExpectationID) []string {
	values := make([]string, len(ids))
	for index, id := range ids {
		values[index] = string(id)
	}
	return values
}

func authoredExpectationStrings(expectations []scenarios.ModelExpectation) []string {
	values := make([]string, len(expectations))
	for index, expectation := range expectations {
		values[index] = string(expectation.ID)
	}
	return values
}

func measurementStrings(ids []contract.MeasurementID) []string {
	values := make([]string, len(ids))
	for index, id := range ids {
		values[index] = string(id)
	}
	return values
}

func budgetStrings(ids []contract.BudgetID) []string {
	values := make([]string, len(ids))
	for index, id := range ids {
		values[index] = string(id)
	}
	return values
}

func budgetDefinitionStrings(values []contract.PerformanceBudget) []string {
	result := make([]string, len(values))
	for index, value := range values {
		result[index] = string(value.ID)
	}
	return result
}

func measurementDefinitionStrings(values []contract.RequiredMeasurement) []string {
	result := make([]string, len(values))
	for index, value := range values {
		result[index] = string(value.ID)
	}
	return result
}

func measurementHasStratum(measurement contract.RequiredMeasurement, wanted contract.StratumID) bool {
	for _, stratum := range measurement.Strata {
		if stratum.StratumID == wanted {
			return true
		}
	}
	return false
}

func sameOptionalString(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func invalidOptionalCode(value *string) bool {
	return value != nil && *value == ""
}

func validSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}

func validProcessIdentifier(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' {
			continue
		}
		switch character {
		case '-', '_', '.', ':':
			continue
		default:
			return false
		}
	}
	return true
}

func boundedDurationSince(started time.Time) (int64, error) {
	duration := time.Since(started).Nanoseconds()
	if duration < 0 || duration > maximumTraceDurationNanoseconds {
		return 0, errors.New("duration is out of bounds")
	}
	return duration, nil
}

func cloneManifestAction(source ManifestAction) (ManifestAction, error) {
	clone, err := cloneJSON(source)
	if err != nil {
		return ManifestAction{}, err
	}
	preserveStateFactProjections(source.Expectations, clone.Expectations)
	return clone, nil
}

func clonePerformanceDefinitions(budgets []contract.PerformanceBudget, measurements []contract.RequiredMeasurement) ([]contract.PerformanceBudget, []contract.RequiredMeasurement, error) {
	definitions := struct {
		Budgets      []contract.PerformanceBudget
		Measurements []contract.RequiredMeasurement
	}{Budgets: budgets, Measurements: measurements}
	clone, err := cloneJSON(definitions)
	if err != nil {
		return nil, nil, err
	}
	return clone.Budgets, clone.Measurements, nil
}

func cloneJSON[T any](source T) (T, error) {
	var clone T
	data, err := json.Marshal(source)
	if err != nil {
		return clone, err
	}
	if err := json.Unmarshal(data, &clone); err != nil {
		return clone, err
	}
	return clone, nil
}

func cloneStepObservation(source StepObservation) StepObservation {
	result := source
	result.ErrorCode = cloneString(source.ErrorCode)
	if source.Wire != nil {
		wire := *source.Wire
		wire.ErrorCode = cloneString(source.Wire.ErrorCode)
		result.Wire = &wire
	}
	return result
}

func cloneStepObservations(source []StepObservation) []StepObservation {
	result := make([]StepObservation, len(source))
	for index, observation := range source {
		result[index] = cloneStepObservation(observation)
	}
	return result
}

func cloneMeasurementSamples(source []MeasurementSampleObservation) []MeasurementSampleObservation {
	result := make([]MeasurementSampleObservation, len(source))
	for index, sample := range source {
		result[index] = sample
		result[index].MetricValues = append([]execution.MetricValue(nil), sample.MetricValues...)
	}
	return result
}

func cloneRequiredMeasurementResults(source []RequiredMeasurementResult) []RequiredMeasurementResult {
	result := make([]RequiredMeasurementResult, len(source))
	for index, measurement := range source {
		result[index] = measurement
		result[index].Strata = append([]MeasurementStratumResult(nil), measurement.Strata...)
	}
	return result
}

func cloneProcessBoundary(source *ProcessBoundaryResult) *ProcessBoundaryResult {
	if source == nil {
		return nil
	}
	result := *source
	result.CurrentProcessID = cloneString(source.CurrentProcessID)
	return &result
}

func traceProcessBoundary(source *ProcessBoundaryResult) *ProcessBoundaryResult {
	result := cloneProcessBoundary(source)
	if result != nil {
		result.ClientKey = ""
	}
	return result
}

func cloneClientCall(source *ClientCallResult) *ClientCallResult {
	if source == nil {
		return nil
	}
	result := *source
	return &result
}

func cloneSynchronizationResult(source *SynchronizationResult) *SynchronizationResult {
	if source == nil {
		return nil
	}
	result := *source
	return &result
}

func boundedClientCall(source *ClientCallResult, callID scenarios.NativeCallID, state, completion string) *ClientCallResult {
	result := &ClientCallResult{CallID: callID, State: "mismatched"}
	if source.State == state {
		result.State = state
	}
	if source.Completion == completion {
		result.Completion = completion
	}
	return result
}

func cloneString(source *string) *string {
	if source == nil {
		return nil
	}
	result := *source
	return &result
}

type executorExecutionError struct {
	actionID scenarios.NativeActionID
	cause    error
}

func (e executorExecutionError) Error() string {
	return fmt.Sprintf("execute native action %s failed", e.actionID)
}

func (e executorExecutionError) Unwrap() error { return e.cause }
