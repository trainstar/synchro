package nativeexecution

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestSelectDerivesNativeActionsFromBoundSteps(t *testing.T) {
	selection, err := Select(context.Background(), "../..", "SCN-PERF-STEADY-PULL-001", "SUP-MACOS-CURRENT-001")
	if err != nil {
		t.Fatalf("select bound native scenario: %v", err)
	}
	if len(selection.plan.clients) != 1 || selection.plan.clients[0].UserID != "user-a" || selection.plan.clients[0].ClientID != "client-a" {
		t.Fatalf("derived clients = %#v", selection.plan.clients)
	}

	covered := make([]scenarios.StepID, 0, len(selection.scenario.Steps))
	for _, action := range selection.plan.actions {
		covered = append(covered, action.CoversStepIDs...)
	}
	want := make([]scenarios.StepID, len(selection.scenario.Steps))
	for index, step := range selection.scenario.Steps {
		want[index] = step.ID
	}
	if !reflect.DeepEqual(covered, want) {
		t.Fatalf("derived covered steps = %v, want %v", covered, want)
	}
	if selection.plan.actions[0].Actor != "controller" || selection.plan.actions[0].Command != "install-model" {
		t.Fatalf("first derived action = %#v", selection.plan.actions[0])
	}
}

func TestSelectDerivesEveryBoundScenario(t *testing.T) {
	ctx := context.Background()
	authored, err := scenarios.LoadAll(ctx, "../..")
	if err != nil {
		t.Fatalf("load authored scenarios: %v", err)
	}
	cases := 0
	for _, scenario := range authored {
		if !scenarios.IsNativeDerivationScenario(scenario) {
			continue
		}
		for _, obligation := range scenario.ProofObligations {
			if obligation.ProofType != "native-e2e" || obligation.SupportCellID == nil {
				continue
			}
			cases++
			supportCellID := *obligation.SupportCellID
			selection, err := Select(ctx, "../..", string(scenario.ID), string(supportCellID))
			if err != nil {
				t.Fatalf("select %s/%s: %v", scenario.ID, supportCellID, err)
			}
			manifest, err := BuildManifest(selection)
			if err != nil {
				t.Fatalf("build manifest %s/%s: %v", scenario.ID, supportCellID, err)
			}
			steps := make(map[scenarios.StepID]scenarios.Step, len(scenario.Steps))
			for _, action := range manifest.Actions {
				for _, step := range action.Steps {
					if _, duplicate := steps[step.ID]; duplicate {
						t.Fatalf("manifest %s/%s covers step %s more than once", scenario.ID, supportCellID, step.ID)
					}
					steps[step.ID] = step
				}
			}
			if len(steps) != len(scenario.Steps) {
				t.Fatalf("manifest %s/%s covers %d steps, want %d", scenario.ID, supportCellID, len(steps), len(scenario.Steps))
			}
			requiredMeasurements := make(map[contract.MeasurementID]struct{}, len(obligation.RequiredMeasurementIDs))
			for _, measurementID := range obligation.RequiredMeasurementIDs {
				requiredMeasurements[measurementID] = struct{}{}
			}
			for _, binding := range scenario.MeasurementBindings {
				step := steps[binding.StepID]
				_, required := requiredMeasurements[binding.MeasurementSample.MeasurementID]
				if required && (step.MeasurementSample == nil || !reflect.DeepEqual(*step.MeasurementSample, binding.MeasurementSample)) {
					t.Fatalf("manifest %s/%s lost measurement sample for %s", scenario.ID, supportCellID, binding.StepID)
				}
				if !required && step.MeasurementSample != nil {
					t.Fatalf("manifest %s/%s retained unselected measurement sample for %s", scenario.ID, supportCellID, binding.StepID)
				}
			}
		}
	}
	if cases == 0 {
		t.Fatal("no bound native scenario was selected")
	}
}

func TestDeriveGroupsOneSynchronousPublicCall(t *testing.T) {
	callID := scenarios.NativeCallID("sync")
	scenario := scenarios.Scenario{
		ID: "SCN-DERIVE-001",
		Steps: []scenarios.Step{
			{
				ID:              "STEP-DERIVE-001",
				Phase:           "exercise",
				Transport:       "http",
				NativeBinding:   &scenarios.NativeStepBinding{Kind: "public-call", UserID: "user-a", ClientID: "client-a", CallID: &callID, Stage: "synchronous", Method: "start", Completion: "idle"},
				ExpectedOutcome: scenarios.ExpectedOutcome{Disposition: "success"},
			},
			{
				ID:              "STEP-DERIVE-002",
				Phase:           "exercise",
				Transport:       "local",
				NativeBinding:   &scenarios.NativeStepBinding{Kind: "public-call", UserID: "user-a", ClientID: "client-a", CallID: &callID, Stage: "synchronous", Method: "start", Completion: "idle"},
				ExpectedOutcome: scenarios.ExpectedOutcome{Disposition: "success"},
			},
		},
		Assertions: []scenarios.Assertion{{
			ID:             "ASSERT-DERIVE-001",
			ExpectationIDs: []scenarios.ExpectationID{"EXPECT-DERIVE-001"},
		}},
		Model: scenarios.ModelSpec{
			Setup:         []scenarios.Operation{{ContractOperation: "model", Name: "install-current-contract", Payload: json.RawMessage(`{}`)}},
			ExpectedState: []scenarios.ModelExpectation{{ID: "EXPECT-DERIVE-001", Predicate: scenarios.Predicate{ContractPredicate: "wire-outcome"}}},
		},
	}
	plan, err := deriveNativePlan(scenario, scenarios.ProofObligation{ObligationID: "OBL-DERIVE-001", ProofType: "native-e2e", AssertionIDs: []contract.AssertionID{"ASSERT-DERIVE-001"}})
	if err != nil {
		t.Fatalf("derive synchronous call: %v", err)
	}
	var grouped *Action
	for index := range plan.actions {
		action := &plan.actions[index]
		if action.Command == "synchronize-step" {
			grouped = action
			break
		}
	}
	if grouped == nil || !reflect.DeepEqual(grouped.CoversStepIDs, []scenarios.StepID{"STEP-DERIVE-001", "STEP-DERIVE-002"}) {
		t.Fatalf("derived synchronous action = %#v", grouped)
	}
}

func TestDeriveRejectsUnboundStep(t *testing.T) {
	_, err := deriveNativePlan(scenarios.Scenario{
		ID:    "SCN-DERIVE-UNBOUND-001",
		Model: scenarios.ModelSpec{Setup: []scenarios.Operation{{ContractOperation: "model", Name: "install-current-contract", Payload: json.RawMessage(`{}`)}}, ExpectedState: []scenarios.ModelExpectation{{ID: "EXPECT-DERIVE-UNBOUND-001"}}},
		Steps: []scenarios.Step{{ID: "STEP-DERIVE-UNBOUND-001"}},
	}, scenarios.ProofObligation{ObligationID: "OBL-DERIVE-UNBOUND-001", ProofType: "native-e2e"})
	if err == nil {
		t.Fatal("unbound step produced native actions")
	}
}
