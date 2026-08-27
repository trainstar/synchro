package scenarios

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"

	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
)

type measurementBindingFailureCategory string

const (
	measurementBindingUnknownRootStep         measurementBindingFailureCategory = "unknown-root-step"
	measurementBindingRootNotConfiguredBounds measurementBindingFailureCategory = "root-not-configured-bounds"
	measurementBindingRootInlineConflict      measurementBindingFailureCategory = "root-inline-conflict"
	measurementBindingNonExerciseStep         measurementBindingFailureCategory = "non-exercise-step"
	measurementBindingIncompleteIdentifiers   measurementBindingFailureCategory = "incomplete-identifiers"
	measurementBindingEmptyOperationField     measurementBindingFailureCategory = "empty-operation-field"
	measurementBindingUnknownMeasurement      measurementBindingFailureCategory = "unknown-measurement"
	measurementBindingMeasurementOwnership    measurementBindingFailureCategory = "measurement-ownership"
	measurementBindingDuplicateSample         measurementBindingFailureCategory = "duplicate-sample"
	measurementBindingDuplicateOperation      measurementBindingFailureCategory = "duplicate-operation"
	measurementBindingUnknownStratum          measurementBindingFailureCategory = "unknown-stratum"
	measurementBindingParameterMismatch       measurementBindingFailureCategory = "parameter-mismatch"
	measurementBindingOperationValueMismatch  measurementBindingFailureCategory = "operation-value-mismatch"
	measurementBindingConfiguredBoundMismatch measurementBindingFailureCategory = "configured-bound-mismatch"
	measurementBindingMinimumSamples          measurementBindingFailureCategory = "minimum-samples"
)

type measurementBindingFailure struct {
	category measurementBindingFailureCategory
	detail   string
}

func (f measurementBindingFailure) Error() string {
	return fmt.Sprintf("measurement binding %s: %s", f.category, f.detail)
}

type boundMeasurementSample struct {
	step   Step
	sample MeasurementSample
	root   bool
}

type measurementBindingCollection struct {
	byMeasurement map[contract.MeasurementID][]boundMeasurementSample
	byTuple       map[string]boundMeasurementSample
	byOperation   map[MeasurementOperationID]boundMeasurementSample
	byStep        map[StepID][]boundMeasurementSample
}

func (v *scenarioValidator) validateMeasurementBindings(measurements map[contract.MeasurementID]contract.RequiredMeasurement) {
	bound, failures := collectMeasurementBindings(v.scenario, measurements)
	v.errors = append(v.errors, failures...)
	v.validateSchemaDispatchMeasurementPlans(measurements, bound.byMeasurement)
}

func collectMeasurementBindings(scenario Scenario, measurements map[contract.MeasurementID]contract.RequiredMeasurement) (measurementBindingCollection, []error) {
	collection := measurementBindingCollection{
		byMeasurement: make(map[contract.MeasurementID][]boundMeasurementSample),
		byTuple:       make(map[string]boundMeasurementSample),
		byOperation:   make(map[MeasurementOperationID]boundMeasurementSample),
		byStep:        make(map[StepID][]boundMeasurementSample),
	}
	steps := make(map[StepID]Step, len(scenario.Steps))
	for _, step := range scenario.Steps {
		if _, found := steps[step.ID]; !found {
			steps[step.ID] = step
		}
	}

	var failures []error
	add := func(category measurementBindingFailureCategory, format string, args ...any) {
		failures = append(failures, measurementBindingFailure{category: category, detail: fmt.Sprintf(format, args...)})
	}
	bind := func(step Step, sample MeasurementSample, root bool) {
		if step.Phase != "exercise" {
			add(measurementBindingNonExerciseStep, "%s sample on step %s must execute in the exercise phase", scenario.ID, step.ID)
		}
		if sample.MeasurementID == "" || sample.StratumID == "" || sample.SampleID == "" || sample.Operation.ID == "" {
			add(measurementBindingIncompleteIdentifiers, "%s sample on step %s has incomplete IDs", scenario.ID, step.ID)
		}
		if sample.Operation.Family == "" || sample.Operation.Boundary == "" || !validMeasurementJSON(sample.Operation.Value) {
			add(measurementBindingEmptyOperationField, "%s sample on step %s has an empty operation family, boundary, or value", scenario.ID, step.ID)
		}

		item, found := measurements[sample.MeasurementID]
		if !found {
			add(measurementBindingUnknownMeasurement, "%s sample on step %s references unknown measurement %s", scenario.ID, step.ID, sample.MeasurementID)
			return
		}
		if item.ScenarioID != scenario.ID {
			add(measurementBindingMeasurementOwnership, "%s sample on step %s binds measurement %s from scenario %s", scenario.ID, step.ID, sample.MeasurementID, item.ScenarioID)
			return
		}

		tuple := measurementSampleTuple(sample)
		if _, duplicate := collection.byTuple[tuple]; duplicate {
			add(measurementBindingDuplicateSample, "%s sample on step %s duplicates sample tuple %s", scenario.ID, step.ID, tuple)
		} else {
			collection.byTuple[tuple] = boundMeasurementSample{step: step, sample: sample, root: root}
		}
		if sample.Operation.ID != "" {
			if _, duplicate := collection.byOperation[sample.Operation.ID]; duplicate {
				add(measurementBindingDuplicateOperation, "%s sample on step %s duplicates operation ID %s", scenario.ID, step.ID, sample.Operation.ID)
			} else {
				collection.byOperation[sample.Operation.ID] = boundMeasurementSample{step: step, sample: sample, root: root}
			}
		}

		stratum, found := measurementStratum(item, sample.StratumID)
		if !found {
			add(measurementBindingUnknownStratum, "%s sample on step %s references unknown stratum %s", scenario.ID, step.ID, sample.StratumID)
			return
		}
		if !canonicalMeasurementJSONEqual(sample.Parameters, stratum.Parameters) {
			add(measurementBindingParameterMismatch, "%s sample on step %s parameters do not match stratum %s", scenario.ID, step.ID, sample.StratumID)
		}
		if !canonicalMeasurementJSONEqual(sample.Operation.Value, sample.Parameters) {
			add(measurementBindingOperationValueMismatch, "%s sample on step %s operation value does not match parameters", scenario.ID, step.ID)
		}
		if root && !configuredBoundOperationMatches(sample) {
			add(measurementBindingConfiguredBoundMismatch, "%s root sample on step %s does not match its configured bound", scenario.ID, step.ID)
		}

		bound := boundMeasurementSample{step: step, sample: sample, root: root}
		collection.byMeasurement[sample.MeasurementID] = append(collection.byMeasurement[sample.MeasurementID], bound)
		collection.byStep[step.ID] = append(collection.byStep[step.ID], bound)
	}

	for _, step := range scenario.Steps {
		if step.MeasurementSample != nil {
			bind(step, *step.MeasurementSample, false)
		}
	}
	for _, binding := range scenario.MeasurementBindings {
		step, found := steps[binding.StepID]
		if !found {
			add(measurementBindingUnknownRootStep, "%s root binding references unknown step %s", scenario.ID, binding.StepID)
			continue
		}
		if step.MeasurementSample != nil {
			add(measurementBindingRootInlineConflict, "%s root binding step %s also has an inline sample", scenario.ID, binding.StepID)
		}
		if !isConfiguredBoundsMacro(step) {
			add(measurementBindingRootNotConfiguredBounds, "%s root binding step %s is not a configured-bounds macro", scenario.ID, binding.StepID)
		}
		bind(step, binding.MeasurementSample, true)
	}

	measurementIDs := make([]string, 0, len(measurements))
	for measurementID, item := range measurements {
		if item.ScenarioID == scenario.ID {
			measurementIDs = append(measurementIDs, string(measurementID))
		}
	}
	sort.Strings(measurementIDs)
	for _, rawID := range measurementIDs {
		measurementID := contract.MeasurementID(rawID)
		item := measurements[measurementID]
		minimum, err := item.MinimumSampleCountPerStratum.Int64()
		if err != nil || minimum <= 0 {
			add(measurementBindingMinimumSamples, "%s measurement %s has an invalid minimum sample count", scenario.ID, measurementID)
			continue
		}
		counts := make(map[contract.StratumID]int, len(item.Strata))
		for _, sample := range collection.byMeasurement[measurementID] {
			counts[sample.sample.StratumID]++
		}
		for _, stratum := range item.Strata {
			if counts[stratum.StratumID] < int(minimum) {
				add(measurementBindingMinimumSamples, "%s measurement %s stratum %s has %d bound samples, want at least %d", scenario.ID, measurementID, stratum.StratumID, counts[stratum.StratumID], minimum)
			}
		}
	}

	return collection, failures
}

func isConfiguredBoundsMacro(step Step) bool {
	if step.Operation.ContractOperation != "workload" || step.Operation.Name != "prepare" {
		return false
	}
	var payload map[string]json.RawMessage
	if err := jsonstrict.Decode(step.Operation.Payload, &payload); err != nil {
		return false
	}
	var profile string
	return json.Unmarshal(payload["profile"], &profile) == nil && profile == "configured_limits"
}

func configuredBoundOperationMatches(sample MeasurementSample) bool {
	var parameters map[string]json.RawMessage
	if err := jsonstrict.Decode(sample.Parameters, &parameters); err != nil {
		return false
	}
	var family, boundary string
	if err := json.Unmarshal(parameters["bound_family"], &family); err != nil {
		return false
	}
	if err := json.Unmarshal(parameters["boundary"], &boundary); err != nil {
		return false
	}
	return sample.Operation.Family == family && sample.Operation.Boundary == boundary
}

func measurementSampleTuple(sample MeasurementSample) string {
	return strings.Join([]string{string(sample.MeasurementID), string(sample.StratumID), sample.SampleID}, "|")
}

func measurementStratum(item contract.RequiredMeasurement, wanted contract.StratumID) (contract.PerformanceStratum, bool) {
	for _, stratum := range item.Strata {
		if stratum.StratumID == wanted {
			return stratum, true
		}
	}
	return contract.PerformanceStratum{}, false
}

func canonicalMeasurementJSONEqual(left, right json.RawMessage) bool {
	var leftValue any
	var rightValue any
	if jsonstrict.Decode(left, &leftValue) != nil || jsonstrict.Decode(right, &rightValue) != nil {
		return false
	}
	return reflect.DeepEqual(leftValue, rightValue)
}

func validMeasurementJSON(value json.RawMessage) bool {
	var decoded any
	return len(value) != 0 && jsonstrict.Decode(value, &decoded) == nil && decoded != nil
}

func (v *scenarioValidator) validateSchemaDispatchMeasurementPlans(measurements map[contract.MeasurementID]contract.RequiredMeasurement, bound map[contract.MeasurementID][]boundMeasurementSample) {
	var expected *SchemaDispatchMeasurementPlan
	for _, predicate := range scenarioPredicates(v.scenario) {
		if predicate.Name != "schema-dispatch-observations-satisfied" && predicate.Name != "schema-dispatch-measurement-satisfied" {
			continue
		}
		plan, err := DecodeSchemaDispatchMeasurementPlan(predicate.Payload)
		if err != nil {
			v.add("%s schema-dispatch predicate is invalid: %v", v.scenario.ID, err)
			continue
		}
		item, found := measurements[plan.MeasurementID]
		if !found || item.ScenarioID != v.scenario.ID {
			v.add("%s schema-dispatch predicate references an unavailable measurement %s", v.scenario.ID, plan.MeasurementID)
			continue
		}
		minimum, err := item.MinimumSampleCountPerStratum.Int64()
		if err != nil || minimum <= 0 || uint64(minimum) != plan.MinimumSampleCountPerStratum {
			v.add("%s schema-dispatch predicate does not match measurement %s minimum sample count", v.scenario.ID, plan.MeasurementID)
		}
		strata, err := schemaStrataForMeasurement(item)
		if err != nil || !reflect.DeepEqual(plan.Strata, strata) {
			v.add("%s schema-dispatch predicate does not match measurement %s strata", v.scenario.ID, plan.MeasurementID)
		}
		if len(bound[plan.MeasurementID]) == 0 {
			v.add("%s schema-dispatch predicate has no bound executable samples", v.scenario.ID)
		}
		if expected == nil {
			copy := plan
			expected = &copy
		} else if !reflect.DeepEqual(*expected, plan) {
			v.add("%s schema-dispatch predicates do not share one measurement plan", v.scenario.ID)
		}
	}
}

func scenarioPredicates(scenario Scenario) []Predicate {
	predicates := make([]Predicate, 0, len(scenario.Model.ExpectedState)+len(scenario.Assertions))
	for _, expectation := range scenario.Model.ExpectedState {
		predicates = append(predicates, expectation.Predicate)
	}
	for _, assertion := range scenario.Assertions {
		predicates = append(predicates, assertion.Predicate)
	}
	return predicates
}

func schemaStrataForMeasurement(item contract.RequiredMeasurement) ([]SchemaDispatchMeasurementStratum, error) {
	strata := make([]SchemaDispatchMeasurementStratum, 0, len(item.Strata))
	seen := make(map[string]struct{}, len(item.Strata))
	for _, stratum := range item.Strata {
		var parameters map[string]json.RawMessage
		if err := jsonstrict.Decode(stratum.Parameters, &parameters); err != nil || len(parameters) != 1 {
			return nil, errors.New("schema measurement stratum parameters are invalid")
		}
		raw, found := parameters["schema_case"]
		if !found {
			return nil, errors.New("schema measurement stratum has no schema_case")
		}
		var schemaCase string
		if err := json.Unmarshal(raw, &schemaCase); err != nil || schemaCase == "" {
			return nil, errors.New("schema measurement stratum schema_case is invalid")
		}
		if _, duplicate := seen[schemaCase]; duplicate {
			return nil, errors.New("schema measurement stratum schema_case is duplicated")
		}
		seen[schemaCase] = struct{}{}
		strata = append(strata, SchemaDispatchMeasurementStratum{StratumID: stratum.StratumID, SchemaCase: schemaCase})
	}
	return strata, nil
}

// MeasurementClosureFailureCategory identifies a rejected runtime measurement
// observation closure.
type MeasurementClosureFailureCategory string

const (
	MeasurementClosureUnknownObligation         MeasurementClosureFailureCategory = "unknown-obligation"
	MeasurementClosureSupportMismatch           MeasurementClosureFailureCategory = "support-mismatch"
	MeasurementClosureDefinitionMismatch        MeasurementClosureFailureCategory = "definition-mismatch"
	MeasurementClosureBindingMismatch           MeasurementClosureFailureCategory = "binding-mismatch"
	MeasurementClosureDuplicateObservation      MeasurementClosureFailureCategory = "duplicate-observation"
	MeasurementClosureMissingObservation        MeasurementClosureFailureCategory = "missing-observation"
	MeasurementClosureExtraObservation          MeasurementClosureFailureCategory = "extra-observation"
	MeasurementClosureStepMismatch              MeasurementClosureFailureCategory = "step-mismatch"
	MeasurementClosureOperationIDMismatch       MeasurementClosureFailureCategory = "operation-id-mismatch"
	MeasurementClosureOperationFamilyMismatch   MeasurementClosureFailureCategory = "operation-family-mismatch"
	MeasurementClosureOperationBoundaryMismatch MeasurementClosureFailureCategory = "operation-boundary-mismatch"
	MeasurementClosureOperationValueMismatch    MeasurementClosureFailureCategory = "operation-value-mismatch"
	MeasurementClosureSampleMismatch            MeasurementClosureFailureCategory = "sample-mismatch"
	MeasurementClosureMetricMismatch            MeasurementClosureFailureCategory = "metric-mismatch"
	MeasurementClosureInvalidMetricValue        MeasurementClosureFailureCategory = "invalid-metric-value"
)

// MeasurementClosureFailure reports one exact runtime closure failure.
type MeasurementClosureFailure struct {
	Category MeasurementClosureFailureCategory
	Detail   string
}

func (f MeasurementClosureFailure) Error() string {
	return fmt.Sprintf("measurement closure %s: %s", f.Category, f.Detail)
}

// ValidateMeasurementObservationClosure validates independently produced
// observations against one exact scenario obligation and support cell.
func ValidateMeasurementObservationClosure(scenario Scenario, obligationID contract.ObligationID, supportCellID contract.SupportCellID, definitions []contract.RequiredMeasurement, observations []MeasurementObservation) error {
	var failures []error
	add := func(category MeasurementClosureFailureCategory, format string, args ...any) {
		failures = append(failures, MeasurementClosureFailure{Category: category, Detail: fmt.Sprintf(format, args...)})
	}

	var obligation *ProofObligation
	for index := range scenario.ProofObligations {
		if scenario.ProofObligations[index].ObligationID == obligationID {
			obligation = &scenario.ProofObligations[index]
			break
		}
	}
	if obligation == nil {
		add(MeasurementClosureUnknownObligation, "%s has no obligation %s", scenario.ID, obligationID)
		return joinMeasurementClosureErrors(failures)
	}
	if obligation.SupportCellID == nil || *obligation.SupportCellID != supportCellID {
		add(MeasurementClosureSupportMismatch, "%s obligation %s does not own support cell %s", scenario.ID, obligationID, supportCellID)
	}

	required := make(map[contract.MeasurementID]struct{}, len(obligation.RequiredMeasurementIDs))
	for _, measurementID := range obligation.RequiredMeasurementIDs {
		if _, duplicate := required[measurementID]; duplicate {
			add(MeasurementClosureDefinitionMismatch, "%s obligation %s duplicates measurement %s", scenario.ID, obligationID, measurementID)
		}
		required[measurementID] = struct{}{}
	}
	definitionByID := make(map[contract.MeasurementID]contract.RequiredMeasurement, len(definitions))
	for _, definition := range definitions {
		if _, expected := required[definition.ID]; !expected {
			add(MeasurementClosureDefinitionMismatch, "%s obligation %s received extra measurement definition %s", scenario.ID, obligationID, definition.ID)
			continue
		}
		if _, duplicate := definitionByID[definition.ID]; duplicate {
			add(MeasurementClosureDefinitionMismatch, "%s obligation %s received duplicate measurement definition %s", scenario.ID, obligationID, definition.ID)
			continue
		}
		if definition.ScenarioID != scenario.ID {
			add(MeasurementClosureDefinitionMismatch, "measurement %s belongs to scenario %s, not %s", definition.ID, definition.ScenarioID, scenario.ID)
		}
		if !containsSupportCell(definition.SupportCellIDs, supportCellID) {
			add(MeasurementClosureDefinitionMismatch, "measurement %s does not own support cell %s", definition.ID, supportCellID)
		}
		definitionByID[definition.ID] = definition
	}
	for measurementID := range required {
		if _, found := definitionByID[measurementID]; !found {
			add(MeasurementClosureDefinitionMismatch, "%s obligation %s is missing measurement definition %s", scenario.ID, obligationID, measurementID)
		}
	}
	if len(failures) != 0 {
		return joinMeasurementClosureErrors(failures)
	}

	bound, bindingFailures := collectMeasurementBindings(measurementScenarioSubset(scenario, required), definitionByID)
	for _, failure := range bindingFailures {
		add(MeasurementClosureBindingMismatch, "%v", failure)
	}
	if len(failures) != 0 {
		return joinMeasurementClosureErrors(failures)
	}
	expected := make(map[string]boundMeasurementSample)
	for measurementID := range required {
		for _, sample := range bound.byMeasurement[measurementID] {
			expected[measurementSampleTuple(sample.sample)] = sample
		}
	}

	seen := make(map[string]struct{}, len(observations))
	for _, observation := range observations {
		tuple := strings.Join([]string{string(observation.MeasurementID), string(observation.StratumID), observation.SampleID}, "|")
		want, found := expected[tuple]
		if found {
			if _, duplicate := seen[tuple]; duplicate {
				add(MeasurementClosureDuplicateObservation, "observation for %s is duplicated", tuple)
				continue
			}
			seen[tuple] = struct{}{}
			if observation.StepID != want.step.ID {
				add(MeasurementClosureStepMismatch, "observation %s names step %s, want %s", tuple, observation.StepID, want.step.ID)
			}
			validateObservedOperation(add, tuple, want.sample.Operation, observation.Operation)
			validateObservedMetrics(add, tuple, definitionByID[observation.MeasurementID], observation.Metrics)
			continue
		}

		if want, found := bound.byOperation[observation.Operation.ID]; found {
			add(MeasurementClosureSampleMismatch, "operation %s reports sample %s, want %s", observation.Operation.ID, tuple, measurementSampleTuple(want.sample))
			continue
		}
		if candidates := bound.byStep[observation.StepID]; len(candidates) == 1 {
			want := candidates[0]
			validateObservedOperation(add, measurementSampleTuple(want.sample), want.sample.Operation, observation.Operation)
			add(MeasurementClosureSampleMismatch, "step %s reports sample %s, want %s", observation.StepID, tuple, measurementSampleTuple(want.sample))
			continue
		}
		add(MeasurementClosureExtraObservation, "observation for %s has no authored binding", tuple)
	}
	for tuple := range expected {
		if _, found := seen[tuple]; !found {
			add(MeasurementClosureMissingObservation, "authored sample %s has no observation", tuple)
		}
	}
	return joinMeasurementClosureErrors(failures)
}

func measurementScenarioSubset(scenario Scenario, required map[contract.MeasurementID]struct{}) Scenario {
	subset := scenario
	subset.Steps = make([]Step, len(scenario.Steps))
	copy(subset.Steps, scenario.Steps)
	for index := range subset.Steps {
		sample := subset.Steps[index].MeasurementSample
		if sample == nil {
			continue
		}
		if _, found := required[sample.MeasurementID]; !found {
			subset.Steps[index].MeasurementSample = nil
		}
	}
	subset.MeasurementBindings = make([]MeasurementBinding, 0, len(scenario.MeasurementBindings))
	for _, binding := range scenario.MeasurementBindings {
		if _, found := required[binding.MeasurementSample.MeasurementID]; found {
			subset.MeasurementBindings = append(subset.MeasurementBindings, binding)
		}
	}
	return subset
}

func validateObservedOperation(add func(MeasurementClosureFailureCategory, string, ...any), tuple string, want, got MeasurementOperationTarget) {
	if got.ID != want.ID {
		add(MeasurementClosureOperationIDMismatch, "observation %s operation ID %s, want %s", tuple, got.ID, want.ID)
	}
	if got.Family != want.Family {
		add(MeasurementClosureOperationFamilyMismatch, "observation %s operation family %s, want %s", tuple, got.Family, want.Family)
	}
	if got.Boundary != want.Boundary {
		add(MeasurementClosureOperationBoundaryMismatch, "observation %s operation boundary %s, want %s", tuple, got.Boundary, want.Boundary)
	}
	if !canonicalMeasurementJSONEqual(got.Value, want.Value) {
		add(MeasurementClosureOperationValueMismatch, "observation %s operation value does not match", tuple)
	}
}

func validateObservedMetrics(add func(MeasurementClosureFailureCategory, string, ...any), tuple string, definition contract.RequiredMeasurement, values []MeasurementMetricValue) {
	required := make(map[contract.MetricID]struct{}, len(definition.Metrics))
	for _, metric := range definition.Metrics {
		required[metric.ID] = struct{}{}
	}
	seen := make(map[contract.MetricID]struct{}, len(values))
	for _, metric := range values {
		if _, expected := required[metric.MetricID]; !expected {
			add(MeasurementClosureMetricMismatch, "observation %s has extra metric %s", tuple, metric.MetricID)
			continue
		}
		if _, duplicate := seen[metric.MetricID]; duplicate {
			add(MeasurementClosureMetricMismatch, "observation %s duplicates metric %s", tuple, metric.MetricID)
			continue
		}
		seen[metric.MetricID] = struct{}{}
		if math.IsNaN(metric.Value) || math.IsInf(metric.Value, 0) || metric.Value < 0 || metric.Value > 1<<53-1 {
			add(MeasurementClosureInvalidMetricValue, "observation %s metric %s is not finite, nonnegative, and portable", tuple, metric.MetricID)
		}
	}
	for metricID := range required {
		if _, found := seen[metricID]; !found {
			add(MeasurementClosureMetricMismatch, "observation %s is missing metric %s", tuple, metricID)
		}
	}
}

func containsSupportCell(cells []contract.SupportCellID, wanted contract.SupportCellID) bool {
	for _, cell := range cells {
		if cell == wanted {
			return true
		}
	}
	return false
}

func joinMeasurementClosureErrors(failures []error) error {
	if len(failures) == 0 {
		return nil
	}
	sort.SliceStable(failures, func(left, right int) bool { return failures[left].Error() < failures[right].Error() })
	return fmt.Errorf("measurement observation closure failed: %w", errors.Join(failures...))
}
