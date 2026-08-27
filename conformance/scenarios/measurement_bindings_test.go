package scenarios

import (
	"encoding/json"
	"errors"
	"math"
	"testing"

	"github.com/trainstar/synchro/conformance/internal/contract"
)

func TestMeasurementBindingFixtureAndNegativeControls(t *testing.T) {
	base, definitions := measurementBindingFixture()
	if _, failures := collectMeasurementBindings(base, definitions); len(failures) != 0 {
		t.Fatalf("valid measurement binding fixture failed: %v", failures)
	}

	tests := []struct {
		name     string
		mutate   func(*Scenario, map[contract.MeasurementID]contract.RequiredMeasurement)
		category measurementBindingFailureCategory
	}{
		{"unknown root step", func(s *Scenario, _ map[contract.MeasurementID]contract.RequiredMeasurement) {
			s.MeasurementBindings[0].StepID = "STEP-UNKNOWN-001"
		}, measurementBindingUnknownRootStep},
		{"root is not configured", func(s *Scenario, _ map[contract.MeasurementID]contract.RequiredMeasurement) {
			s.Steps[0].Operation.Name = "evaluate"
		}, measurementBindingRootNotConfiguredBounds},
		{"root and inline conflict", func(s *Scenario, _ map[contract.MeasurementID]contract.RequiredMeasurement) {
			inline := measurementBindingSample()
			inline.SampleID = "SAMPLE-INLINE-001"
			inline.Operation.ID = "MOP-INLINE-001"
			s.Steps[0].MeasurementSample = &inline
		}, measurementBindingRootInlineConflict},
		{"non-exercise step", func(s *Scenario, _ map[contract.MeasurementID]contract.RequiredMeasurement) {
			s.Steps[0].Phase = "setup"
		}, measurementBindingNonExerciseStep},
		{"incomplete identifiers", func(s *Scenario, _ map[contract.MeasurementID]contract.RequiredMeasurement) {
			s.MeasurementBindings[0].MeasurementSample.SampleID = ""
		}, measurementBindingIncompleteIdentifiers},
		{"empty operation field", func(s *Scenario, _ map[contract.MeasurementID]contract.RequiredMeasurement) {
			s.MeasurementBindings[0].MeasurementSample.Operation.Family = ""
		}, measurementBindingEmptyOperationField},
		{"empty operation value", func(s *Scenario, _ map[contract.MeasurementID]contract.RequiredMeasurement) {
			s.MeasurementBindings[0].MeasurementSample.Operation.Value = nil
		}, measurementBindingEmptyOperationField},
		{"unknown measurement", func(s *Scenario, _ map[contract.MeasurementID]contract.RequiredMeasurement) {
			s.MeasurementBindings[0].MeasurementSample.MeasurementID = "MEAS-UNKNOWN-001"
		}, measurementBindingUnknownMeasurement},
		{"wrong scenario ownership", func(_ *Scenario, definitions map[contract.MeasurementID]contract.RequiredMeasurement) {
			item := definitions["MEAS-BIND-001"]
			item.ScenarioID = "SCN-OTHER-001"
			definitions[item.ID] = item
		}, measurementBindingMeasurementOwnership},
		{"duplicate sample tuple", func(s *Scenario, _ map[contract.MeasurementID]contract.RequiredMeasurement) {
			sample := measurementBindingSample()
			sample.Operation.ID = "MOP-DUPLICATE-SAMPLE-001"
			s.Steps = append(s.Steps, Step{ID: "STEP-DUPLICATE-SAMPLE-001", Phase: "exercise", MeasurementSample: &sample})
		}, measurementBindingDuplicateSample},
		{"duplicate operation ID", func(s *Scenario, _ map[contract.MeasurementID]contract.RequiredMeasurement) {
			sample := measurementBindingSample()
			sample.SampleID = "SAMPLE-DUPLICATE-OP-001"
			s.Steps = append(s.Steps, Step{ID: "STEP-DUPLICATE-OP-001", Phase: "exercise", MeasurementSample: &sample})
		}, measurementBindingDuplicateOperation},
		{"unknown stratum", func(s *Scenario, _ map[contract.MeasurementID]contract.RequiredMeasurement) {
			s.MeasurementBindings[0].MeasurementSample.StratumID = "STR-UNKNOWN-001"
		}, measurementBindingUnknownStratum},
		{"parameter mismatch", func(s *Scenario, _ map[contract.MeasurementID]contract.RequiredMeasurement) {
			s.MeasurementBindings[0].MeasurementSample.Parameters = json.RawMessage(`{"bound_family":"throughput","boundary":"configured","case":"other"}`)
		}, measurementBindingParameterMismatch},
		{"operation value mismatch", func(s *Scenario, _ map[contract.MeasurementID]contract.RequiredMeasurement) {
			s.MeasurementBindings[0].MeasurementSample.Operation.Value = json.RawMessage(`{"bound_family":"throughput","boundary":"configured","case":"other"}`)
		}, measurementBindingOperationValueMismatch},
		{"configured family and boundary mismatch", func(s *Scenario, _ map[contract.MeasurementID]contract.RequiredMeasurement) {
			s.MeasurementBindings[0].MeasurementSample.Operation.Family = "latency"
			s.MeasurementBindings[0].MeasurementSample.Operation.Boundary = "client"
		}, measurementBindingConfiguredBoundMismatch},
		{"insufficient samples", func(_ *Scenario, definitions map[contract.MeasurementID]contract.RequiredMeasurement) {
			item := definitions["MEAS-BIND-001"]
			item.MinimumSampleCountPerStratum = "2"
			definitions[item.ID] = item
		}, measurementBindingMinimumSamples},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := cloneScenario(base)
			mutatedDefinitions := cloneMeasurementDefinitions(definitions)
			test.mutate(&scenario, mutatedDefinitions)
			_, failures := collectMeasurementBindings(scenario, mutatedDefinitions)
			if !hasMeasurementBindingCategory(failures, test.category) {
				t.Fatalf("failures = %v, want category %q", failures, test.category)
			}
		})
	}
}

func TestMeasurementObservationClosureAcceptsExactObservation(t *testing.T) {
	scenario, definitions, supportCell := measurementClosureFixture()
	if err := ValidateMeasurementObservationClosure(scenario, "OBL-BIND-001", supportCell, definitions, []MeasurementObservation{measurementObservation()}); err != nil {
		t.Fatalf("valid measurement observation closure failed: %v", err)
	}
}

func TestMeasurementObservationClosureIgnoresOtherObligationBindings(t *testing.T) {
	scenario, definitions, supportCell := measurementClosureFixture()
	otherSupportCell := contract.SupportCellID("SUP-OTHER-001")
	otherSample := measurementBindingSample()
	otherSample.MeasurementID = "MEAS-OTHER-001"
	otherSample.StratumID = "STR-OTHER-001"
	otherSample.SampleID = "SAMPLE-OTHER-001"
	otherSample.Operation.ID = "MOP-OTHER-001"
	scenario.MeasurementBindings = append(scenario.MeasurementBindings, MeasurementBinding{StepID: "STEP-ROOT-001", MeasurementSample: otherSample})
	scenario.ProofObligations = append(scenario.ProofObligations, ProofObligation{
		ObligationID:           "OBL-OTHER-001",
		SupportCellID:          &otherSupportCell,
		RequiredMeasurementIDs: []contract.MeasurementID{otherSample.MeasurementID},
	})

	if err := ValidateMeasurementObservationClosure(scenario, "OBL-BIND-001", supportCell, definitions, []MeasurementObservation{measurementObservation()}); err != nil {
		t.Fatalf("selected measurement closure failed because another obligation has bindings: %v", err)
	}
}

func TestMeasurementObservationClosureRejectsObligationAndDefinitions(t *testing.T) {
	scenario, definitions, supportCell := measurementClosureFixture()

	tests := []struct {
		name        string
		obligation  contract.ObligationID
		supportCell contract.SupportCellID
		mutate      func([]contract.RequiredMeasurement) []contract.RequiredMeasurement
		category    MeasurementClosureFailureCategory
	}{
		{"unknown obligation", "OBL-UNKNOWN-001", supportCell, nil, MeasurementClosureUnknownObligation},
		{"support mismatch", "OBL-BIND-001", "SUP-OTHER-001", nil, MeasurementClosureSupportMismatch},
		{"missing definition", "OBL-BIND-001", supportCell, func(_ []contract.RequiredMeasurement) []contract.RequiredMeasurement { return nil }, MeasurementClosureDefinitionMismatch},
		{"extra definition", "OBL-BIND-001", supportCell, func(items []contract.RequiredMeasurement) []contract.RequiredMeasurement {
			return append(items, contract.RequiredMeasurement{ID: "MEAS-EXTRA-001", ScenarioID: scenario.ID, SupportCellIDs: []contract.SupportCellID{supportCell}})
		}, MeasurementClosureDefinitionMismatch},
		{"duplicate definition", "OBL-BIND-001", supportCell, func(items []contract.RequiredMeasurement) []contract.RequiredMeasurement {
			return append(items, items[0])
		}, MeasurementClosureDefinitionMismatch},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			items := append([]contract.RequiredMeasurement(nil), definitions...)
			if test.mutate != nil {
				items = test.mutate(items)
			}
			err := ValidateMeasurementObservationClosure(scenario, test.obligation, test.supportCell, items, []MeasurementObservation{measurementObservation()})
			requireMeasurementClosureCategory(t, err, test.category)
		})
	}
}

func TestMeasurementObservationClosureRejectsInvalidAuthoredBinding(t *testing.T) {
	scenario, definitions, supportCell := measurementClosureFixture()
	scenario.MeasurementBindings[0].MeasurementSample.Operation.Value = json.RawMessage(`{"case":`)

	err := ValidateMeasurementObservationClosure(scenario, "OBL-BIND-001", supportCell, definitions, []MeasurementObservation{measurementObservation()})
	requireMeasurementClosureCategory(t, err, MeasurementClosureBindingMismatch)
}

func TestMeasurementObservationClosureRejectsObservationControls(t *testing.T) {
	scenario, definitions, supportCell := measurementClosureFixture()
	tests := []struct {
		name     string
		mutate   func(*[]MeasurementObservation)
		category MeasurementClosureFailureCategory
	}{
		{"duplicate observation", func(observations *[]MeasurementObservation) {
			*observations = append(*observations, measurementObservation())
		}, MeasurementClosureDuplicateObservation},
		{"missing observation", func(observations *[]MeasurementObservation) {
			*observations = nil
		}, MeasurementClosureMissingObservation},
		{"extra observation", func(observations *[]MeasurementObservation) {
			*observations = []MeasurementObservation{{StepID: "STEP-EXTRA-001", MeasurementID: "MEAS-EXTRA-001", StratumID: "STR-ONE-001", SampleID: "SAMPLE-EXTRA-001", Operation: MeasurementOperationTarget{ID: "MOP-EXTRA-001"}}}
		}, MeasurementClosureExtraObservation},
		{"wrong step", func(observations *[]MeasurementObservation) {
			(*observations)[0].StepID = "STEP-WRONG-001"
		}, MeasurementClosureStepMismatch},
		{"operation ID mismatch", func(observations *[]MeasurementObservation) {
			(*observations)[0].Operation.ID = "MOP-WRONG-001"
		}, MeasurementClosureOperationIDMismatch},
		{"operation family mismatch", func(observations *[]MeasurementObservation) {
			(*observations)[0].Operation.Family = "latency"
		}, MeasurementClosureOperationFamilyMismatch},
		{"operation boundary mismatch", func(observations *[]MeasurementObservation) {
			(*observations)[0].Operation.Boundary = "client"
		}, MeasurementClosureOperationBoundaryMismatch},
		{"operation value mismatch", func(observations *[]MeasurementObservation) {
			(*observations)[0].Operation.Value = json.RawMessage(`{"case":"other"}`)
		}, MeasurementClosureOperationValueMismatch},
		{"wrong sample tuple", func(observations *[]MeasurementObservation) {
			(*observations)[0].SampleID = "SAMPLE-WRONG-001"
		}, MeasurementClosureSampleMismatch},
		{"missing metric", func(observations *[]MeasurementObservation) {
			(*observations)[0].Metrics = (*observations)[0].Metrics[:1]
		}, MeasurementClosureMetricMismatch},
		{"extra metric", func(observations *[]MeasurementObservation) {
			(*observations)[0].Metrics = append((*observations)[0].Metrics, MeasurementMetricValue{MetricID: "MET-EXTRA-001", Value: 1})
		}, MeasurementClosureMetricMismatch},
		{"duplicate metric", func(observations *[]MeasurementObservation) {
			(*observations)[0].Metrics = append((*observations)[0].Metrics, (*observations)[0].Metrics[0])
		}, MeasurementClosureMetricMismatch},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observations := []MeasurementObservation{measurementObservation()}
			test.mutate(&observations)
			err := ValidateMeasurementObservationClosure(scenario, "OBL-BIND-001", supportCell, definitions, observations)
			requireMeasurementClosureCategory(t, err, test.category)
		})
	}
}

func TestMeasurementObservationClosureRejectsInvalidMetricValues(t *testing.T) {
	scenario, definitions, supportCell := measurementClosureFixture()
	for _, value := range []float64{math.NaN(), math.Inf(1), -1, 1 << 53} {
		t.Run(metricValueName(value), func(t *testing.T) {
			observation := measurementObservation()
			observation.Metrics[0].Value = value
			err := ValidateMeasurementObservationClosure(scenario, "OBL-BIND-001", supportCell, definitions, []MeasurementObservation{observation})
			requireMeasurementClosureCategory(t, err, MeasurementClosureInvalidMetricValue)
		})
	}
}

func measurementBindingFixture() (Scenario, map[contract.MeasurementID]contract.RequiredMeasurement) {
	return Scenario{
		ID: "SCN-BIND-001",
		Steps: []Step{{
			ID:                "STEP-ROOT-001",
			Phase:             "exercise",
			Operation:         Operation{ContractOperation: "workload", Name: "prepare", Payload: json.RawMessage(`{"profile":"configured_limits"}`)},
			MeasurementSample: nil,
		}},
		MeasurementBindings: []MeasurementBinding{{StepID: "STEP-ROOT-001", MeasurementSample: measurementBindingSample()}},
	}, measurementDefinitions()
}

func measurementDefinitions() map[contract.MeasurementID]contract.RequiredMeasurement {
	return map[contract.MeasurementID]contract.RequiredMeasurement{"MEAS-BIND-001": {
		ID: "MEAS-BIND-001", ScenarioID: "SCN-BIND-001", Strata: []contract.PerformanceStratum{{StratumID: "STR-ONE-001", Parameters: json.RawMessage(`{"bound_family":"throughput","boundary":"configured","case":"steady"}`)}}, Metrics: []contract.PerformanceMetric{{ID: "MET-ONE-001"}, {ID: "MET-TWO-001"}}, MinimumSampleCountPerStratum: "1",
	}}
}

func measurementBindingSample() MeasurementSample {
	parameters := json.RawMessage(`{"bound_family":"throughput","boundary":"configured","case":"steady"}`)
	return MeasurementSample{MeasurementID: "MEAS-BIND-001", StratumID: "STR-ONE-001", SampleID: "SAMPLE-ROOT-001", Parameters: parameters, Operation: MeasurementOperationTarget{ID: "MOP-ROOT-001", Family: "throughput", Boundary: "configured", Value: append(json.RawMessage(nil), parameters...)}}
}

func measurementClosureFixture() (Scenario, []contract.RequiredMeasurement, contract.SupportCellID) {
	scenario, definitions := measurementBindingFixture()
	supportCell := contract.SupportCellID("SUP-BIND-001")
	for id, item := range definitions {
		item.SupportCellIDs = []contract.SupportCellID{supportCell}
		definitions[id] = item
	}
	scenario.ProofObligations = []ProofObligation{{ObligationID: "OBL-BIND-001", SupportCellID: &supportCell, RequiredMeasurementIDs: []contract.MeasurementID{"MEAS-BIND-001"}}}
	return scenario, []contract.RequiredMeasurement{definitions["MEAS-BIND-001"]}, supportCell
}

func measurementObservation() MeasurementObservation {
	sample := measurementBindingSample()
	return MeasurementObservation{StepID: "STEP-ROOT-001", Operation: sample.Operation, MeasurementID: sample.MeasurementID, StratumID: sample.StratumID, SampleID: sample.SampleID, Metrics: []MeasurementMetricValue{{MetricID: "MET-ONE-001", Value: 1}, {MetricID: "MET-TWO-001", Value: 2}}}
}

func cloneMeasurementDefinitions(source map[contract.MeasurementID]contract.RequiredMeasurement) map[contract.MeasurementID]contract.RequiredMeasurement {
	clone := make(map[contract.MeasurementID]contract.RequiredMeasurement, len(source))
	for id, item := range source {
		item.SupportCellIDs = append([]contract.SupportCellID(nil), item.SupportCellIDs...)
		item.Metrics = append([]contract.PerformanceMetric(nil), item.Metrics...)
		item.Strata = append([]contract.PerformanceStratum(nil), item.Strata...)
		for index := range item.Strata {
			item.Strata[index].Parameters = append(json.RawMessage(nil), item.Strata[index].Parameters...)
		}
		clone[id] = item
	}
	return clone
}

func hasMeasurementBindingCategory(failures []error, want measurementBindingFailureCategory) bool {
	for _, err := range failures {
		var failure measurementBindingFailure
		if errors.As(err, &failure) && failure.category == want {
			return true
		}
	}
	return false
}

func requireMeasurementClosureCategory(t *testing.T, err error, want MeasurementClosureFailureCategory) {
	t.Helper()
	if err == nil {
		t.Fatalf("closure accepted input, want category %q", want)
	}
	var first MeasurementClosureFailure
	if !errors.As(err, &first) {
		t.Fatalf("closure error %v does not expose MeasurementClosureFailure through errors.As", err)
	}
	if !hasMeasurementClosureCategory(err, want) {
		t.Fatalf("closure error = %v, want category %q", err, want)
	}
}

func hasMeasurementClosureCategory(err error, want MeasurementClosureFailureCategory) bool {
	var failure MeasurementClosureFailure
	if errors.As(err, &failure) && failure.Category == want {
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return hasMeasurementClosureCategory(wrapped.Unwrap(), want)
	}
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		return false
	}
	for _, child := range joined.Unwrap() {
		if hasMeasurementClosureCategory(child, want) {
			return true
		}
	}
	return false
}

func metricValueName(value float64) string {
	switch {
	case math.IsNaN(value):
		return "nan"
	case math.IsInf(value, 1):
		return "positive infinity"
	default:
		return "negative"
	}
}
