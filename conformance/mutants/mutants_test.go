package mutants

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/trainstar/synchro/conformance/reference"
	"github.com/trainstar/synchro/conformance/scenarios"
)

type mutantFixture struct {
	name        string
	path        string
	scenarioID  string
	requirement string
	assertion   string
	newMutant   func(Subject) Mutant
}

func allFixtures() []mutantFixture {
	return []mutantFixture{
		{
			name:        OmitMutationName,
			path:        "conformance/scenarios/performance/pending-cycle-001.json",
			scenarioID:  ScenarioMutationOutcome,
			requirement: RequirementMutationOutcome,
			assertion:   AssertionMutationOutcome,
			newMutant: func(subject Subject) Mutant {
				return NewOmitMutation(subject)
			},
		},
		{
			name:        ConstantChecksumName,
			path:        "conformance/scenarios/performance/steady-pull-001.json",
			scenarioID:  ScenarioChecksum,
			requirement: RequirementChecksum,
			assertion:   AssertionChecksum,
			newMutant: func(subject Subject) Mutant {
				return NewConstantChecksum(subject)
			},
		},
		{
			name:        DuplicateDeliveryName,
			path:        "conformance/scenarios/server/pull-divergent-checkpoints-001.json",
			scenarioID:  ScenarioDuplicate,
			requirement: RequirementDuplicate,
			assertion:   AssertionDuplicate,
			newMutant: func(subject Subject) Mutant {
				return NewDuplicateDelivery(subject)
			},
		},
		{
			name:        WrongScopeName,
			path:        "conformance/scenarios/server/pull-divergent-checkpoints-001.json",
			scenarioID:  ScenarioWrongScope,
			requirement: RequirementWrongScope,
			assertion:   AssertionWrongScope,
			newMutant: func(subject Subject) Mutant {
				return NewWrongScope(subject)
			},
		},
	}
}

func loadFixtureScenario(t *testing.T, path string) scenarios.Scenario {
	t.Helper()
	scenario, err := scenarios.LoadFile(context.Background(), "../..", path)
	if err != nil {
		t.Fatalf("load scenario %s: %v", path, err)
	}
	return scenario
}

func TestBasePasses(t *testing.T) {
	for index, fixture := range allFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			scenario := loadFixtureScenario(t, fixture.path)
			base, err := NewBase(int64(index + 1))
			if err != nil {
				t.Fatalf("create base: %v", err)
			}
			result, err := Run(context.Background(), scenario, base)
			if err != nil {
				t.Fatalf("run base: %v", err)
			}
			if !result.Passed || result.Detected {
				t.Fatalf("base result = %#v", result)
			}
			if result.Failure.Kind != FailureNone {
				t.Fatalf("base failure = %#v", result.Failure)
			}
			if len(result.Executions) != len(scenario.Steps)+1 {
				t.Fatalf("base execution count = %d, want %d", len(result.Executions), len(scenario.Steps)+1)
			}
			for _, execution := range result.Executions {
				if execution.Err != nil {
					t.Fatalf("base operation %s failed: %v", execution.OperationKey, execution.Err)
				}
			}
		})
	}
}

func TestEachOneChangeMutantFailsItsOwnedSemanticAssertion(t *testing.T) {
	for index, fixture := range allFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			scenario := loadFixtureScenario(t, fixture.path)
			base, err := NewBase(int64(index + 101))
			if err != nil {
				t.Fatalf("create base: %v", err)
			}
			mutant := fixture.newMutant(base)
			result, err := Run(context.Background(), scenario, mutant)
			if err != nil {
				t.Fatalf("run mutant: %v", err)
			}
			if !result.Detected || result.Passed {
				t.Fatalf("mutant result = %#v", result)
			}
			if result.Failure.Kind != FailureSemantic {
				t.Fatalf("mutant failure = %#v", result.Failure)
			}
			if result.Failure.RequirementID != fixture.requirement || result.RequirementID != fixture.requirement {
				t.Fatalf("mutant requirement = %q/%q, want %q", result.RequirementID, result.Failure.RequirementID, fixture.requirement)
			}
			if result.ScenarioID != fixture.scenarioID || result.AssertionID != fixture.assertion || result.Failure.AssertionID != fixture.assertion {
				t.Fatalf("mutant binding = %q/%q/%q, want %q/%q", result.ScenarioID, result.AssertionID, result.Failure.AssertionID, fixture.scenarioID, fixture.assertion)
			}
			for _, execution := range result.Executions {
				if execution.Err != nil {
					t.Fatalf("mutant crashed or failed at %s: %v", execution.OperationKey, execution.Err)
				}
			}
		})
	}
}

func TestMutantBindingFailsBeforeExecution(t *testing.T) {
	tests := []struct {
		name   string
		change func(*scenarios.Scenario, mutantFixture, Mutant) Mutant
	}{
		{
			name: "wrong scenario",
			change: func(scenario *scenarios.Scenario, _ mutantFixture, mutant Mutant) Mutant {
				scenario.ID = "SCN-WRONG-001"
				return mutant
			},
		},
		{
			name: "missing assertion",
			change: func(scenario *scenarios.Scenario, fixture mutantFixture, mutant Mutant) Mutant {
				removeAssertion(scenario, fixture.assertion)
				return mutant
			},
		},
		{
			name: "missing assertion ownership",
			change: func(scenario *scenarios.Scenario, fixture mutantFixture, mutant Mutant) Mutant {
				removeOwnership(scenario, fixture.requirement, fixture.assertion)
				return mutant
			},
		},
		{
			name: "replaced assertion ownership",
			change: func(scenario *scenarios.Scenario, fixture mutantFixture, mutant Mutant) Mutant {
				for index := range scenario.Ownership {
					if string(scenario.Ownership[index].AssertionID) == fixture.assertion && string(scenario.Ownership[index].RequirementID) == fixture.requirement {
						scenario.Ownership[index].AssertionID = "ASSERT-REPLACED-001"
					}
				}
				return mutant
			},
		},
		{
			name: "control assertion",
			change: func(scenario *scenarios.Scenario, fixture mutantFixture, mutant Mutant) Mutant {
				control, found := firstControlAssertion(scenario.Assertions)
				if !found {
					panic("fixture has no authored control assertion")
				}
				for index := range scenario.Assertions {
					if string(scenario.Assertions[index].ID) == fixture.assertion {
						control.ID = scenario.Assertions[index].ID
						scenario.Assertions[index] = control
					}
				}
				return mutant
			},
		},
		{
			name: "unrelated expectation binding",
			change: func(scenario *scenarios.Scenario, fixture mutantFixture, mutant Mutant) Mutant {
				for index := range scenario.Assertions {
					if string(scenario.Assertions[index].ID) == fixture.assertion {
						scenario.Assertions[index].ExpectationIDs = []scenarios.ExpectationID{"EXPECT-PERF-STEADY-PULL-PERFORMANCE-001"}
					}
				}
				return mutant
			},
		},
	}
	for _, fixture := range allFixtures() {
		for _, test := range tests {
			t.Run(fixture.name+"/"+test.name, func(t *testing.T) {
				scenario := loadFixtureScenario(t, fixture.path)
				subject := &countingSubject{}
				mutant := test.change(&scenario, fixture, fixture.newMutant(subject))
				_, err := Run(context.Background(), scenario, mutant)
				if err == nil {
					t.Fatal("invalid mutant binding was accepted")
				}
				if subject.executions != 0 {
					t.Fatalf("subject executions = %d, want 0", subject.executions)
				}
			})
		}
	}
}

type countingSubject struct {
	executions int
}

func (subject *countingSubject) Execute(context.Context, scenarios.Operation) (reference.StepResult, error) {
	subject.executions++
	return reference.StepResult{}, nil
}

func (subject *countingSubject) RawResult(string) (reference.StepResult, bool) {
	return reference.StepResult{}, false
}

func removeAssertion(scenario *scenarios.Scenario, assertionID string) {
	for index := range scenario.Assertions {
		if string(scenario.Assertions[index].ID) == assertionID {
			scenario.Assertions = append(scenario.Assertions[:index], scenario.Assertions[index+1:]...)
			return
		}
	}
}

func firstControlAssertion(assertions []scenarios.Assertion) (scenarios.Assertion, bool) {
	for _, assertion := range assertions {
		if len(assertion.DetectsControlIDs) != 0 {
			return assertion, true
		}
	}
	return scenarios.Assertion{}, false
}

func removeOwnership(scenario *scenarios.Scenario, requirementID, assertionID string) {
	kept := scenario.Ownership[:0]
	for _, ownership := range scenario.Ownership {
		if string(ownership.RequirementID) == requirementID && string(ownership.AssertionID) == assertionID {
			continue
		}
		kept = append(kept, ownership)
	}
	scenario.Ownership = kept
}

func TestMutantRunsPreserveAllAuthoredControlsAndFaultPlans(t *testing.T) {
	authored, err := scenarios.LoadAll(context.Background(), "../..")
	if err != nil {
		t.Fatalf("load authored scenarios: %v", err)
	}
	wantControls, wantFaultPlans := authoredControlStructures(t, authored)
	if len(wantControls) != 26 || len(wantFaultPlans) != 26 {
		t.Fatalf("authored control counts = %d/%d, want 26/26", len(wantControls), len(wantFaultPlans))
	}

	byID := make(map[string]*scenarios.Scenario, len(authored))
	for index := range authored {
		byID[string(authored[index].ID)] = &authored[index]
	}
	for index, fixture := range allFixtures() {
		base, baseErr := NewBase(int64(index + 501))
		if baseErr != nil {
			t.Fatalf("create base: %v", baseErr)
		}
		if _, runErr := Run(context.Background(), *byID[fixture.scenarioID], fixture.newMutant(base)); runErr != nil {
			t.Fatalf("run %s: %v", fixture.name, runErr)
		}
	}

	gotControls, gotFaultPlans := authoredControlStructures(t, authored)
	if !reflect.DeepEqual(gotControls, wantControls) || !reflect.DeepEqual(gotFaultPlans, wantFaultPlans) {
		t.Fatal("mutant runs changed authored negative controls or fault plans")
	}
}

func authoredControlStructures(t *testing.T, authored []scenarios.Scenario) (map[string]json.RawMessage, map[string]json.RawMessage) {
	t.Helper()
	controls := make(map[string]json.RawMessage)
	faultPlans := make(map[string]json.RawMessage)
	for _, scenario := range authored {
		for _, control := range scenario.NegativeControls {
			controls[string(control.ControlID)] = marshalStructure(t, control)
		}
		for _, faultPlan := range scenario.FaultPlans {
			faultPlans[string(faultPlan.ID)] = marshalStructure(t, faultPlan)
		}
	}
	return controls, faultPlans
}

func marshalStructure(t *testing.T, value any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal authored structure: %v", err)
	}
	return data
}

func TestRequireAllDetectedRejectsSurvivorsAndAcceptsTheFourDetections(t *testing.T) {
	results := make([]Result, 0, len(allFixtures()))
	for index, fixture := range allFixtures() {
		scenario := loadFixtureScenario(t, fixture.path)
		base, err := NewBase(int64(index + 201))
		if err != nil {
			t.Fatalf("create base: %v", err)
		}
		result, err := Run(context.Background(), scenario, fixture.newMutant(base))
		if err != nil {
			t.Fatalf("run %s: %v", fixture.name, err)
		}
		results = append(results, result)
	}
	if err := RequireAllDetected(results); err != nil {
		t.Fatalf("require all detections: %v", err)
	}

	baseScenario := loadFixtureScenario(t, allFixtures()[0].path)
	base, err := NewBase(301)
	if err != nil {
		t.Fatalf("create base: %v", err)
	}
	baseResult, err := Run(context.Background(), baseScenario, base)
	if err != nil {
		t.Fatalf("run base: %v", err)
	}
	withBase := append(append([]Result(nil), results...), baseResult)
	if err := RequireAllDetected(withBase); err != nil {
		t.Fatalf("base result changed mutant closure: %v", err)
	}

	survivor := append([]Result(nil), results...)
	survivor[0].Detected = false
	if err := RequireAllDetected(survivor); err == nil {
		t.Fatal("surviving mutant was accepted")
	}
}

type guardMode string

const (
	guardCrash     guardMode = "crash"
	guardParse     guardMode = "parse"
	guardField     guardMode = "field"
	guardUnrelated guardMode = "unrelated"
)

type guardMutant struct {
	subject Subject
	mode    guardMode
}

func (m guardMutant) Subject() Subject {
	return m.subject
}

func (m guardMutant) Descriptor() Descriptor {
	return Descriptor{
		Name:          "guard-" + string(m.mode),
		Kind:          MutationKindConstantChecksum,
		ScenarioID:    ScenarioChecksum,
		RequirementID: RequirementChecksum,
		AssertionID:   AssertionChecksum,
		OperationKey:  "pull/request-page",
	}
}

func (m guardMutant) MutationApplied() bool {
	return true
}

func (m guardMutant) Execute(ctx context.Context, op scenarios.Operation) (reference.StepResult, error) {
	switch m.mode {
	case guardCrash:
		panic("test crash")
	case guardParse:
		return reference.StepResult{}, errors.New("parse failure")
	}
	result, err := m.subject.Execute(ctx, op)
	if err != nil || operationKey(op) != "pull/request-page" || result.Pull == nil || result.Pull.HasMore || len(result.Pull.ScopeChecksums) == 0 {
		return result, err
	}
	mutated := cloneStepResult(result)
	switch m.mode {
	case guardField:
		mutated.Pull.ScopeChecksums[0].HasChecksum = false
	case guardUnrelated:
		mutated.Pull.AddedScopes = append(mutated.Pull.AddedScopes, "unrelated-scope")
	}
	return mutated, nil
}

func TestDetectionRejectsCrashParseFieldPresenceAndUnrelatedFailures(t *testing.T) {
	scenario := loadFixtureScenario(t, "conformance/scenarios/performance/steady-pull-001.json")
	for _, mode := range []guardMode{guardCrash, guardParse, guardField, guardUnrelated} {
		t.Run(string(mode), func(t *testing.T) {
			base, err := NewBase(401)
			if err != nil {
				t.Fatalf("create base: %v", err)
			}
			result, err := Run(context.Background(), scenario, guardMutant{subject: base, mode: mode})
			if err != nil {
				t.Fatalf("run guard mutant: %v", err)
			}
			if result.Detected {
				t.Fatalf("guard failure counted as detection: %#v", result)
			}
			want := map[guardMode]FailureKind{
				guardCrash:     FailureCrash,
				guardParse:     FailureExecution,
				guardField:     FailureFieldPresence,
				guardUnrelated: FailureUnrelated,
			}[mode]
			if result.Failure.Kind != want {
				t.Fatalf("guard failure kind = %q, want %q", result.Failure.Kind, want)
			}
		})
	}
}
