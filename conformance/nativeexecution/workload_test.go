package nativeexecution

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestDeriveNativeWorkloadProducesStableOperations(t *testing.T) {
	scenario := nativeWorkloadScenario("be92be6fd59ff625d750e5b6b1efa9c122a5e2ccf1a5210b29df25a8fbb6e8b6")
	operations, err := deriveNativeWorkload(scenario.Steps[0])
	if err != nil {
		t.Fatalf("derive native workload: %v", err)
	}
	if len(operations) != 3 {
		t.Fatalf("generated operations = %d, want 3", len(operations))
	}
	if operations[0].ContractOperation != "local" || operations[0].Name != "write" || operations[0].Payload == nil {
		t.Fatalf("first generated operation = %#v", operations[0])
	}
	second, err := deriveNativeWorkload(scenario.Steps[0])
	if err != nil {
		t.Fatalf("repeat native workload derivation: %v", err)
	}
	if !reflect.DeepEqual(operations, second) {
		t.Fatal("same workload parameters produced different operations")
	}

	scenario.Steps[0].NativeBinding.Workload.Seed++
	if _, err := deriveNativeWorkload(scenario.Steps[0]); err == nil || !strings.Contains(err.Error(), "operation digest") {
		t.Fatalf("changed seed error = %v", err)
	}
}

func TestBuildManifestExpandsWorkloadAndPreservesMeasurementBinding(t *testing.T) {
	scenario := nativeWorkloadScenario("be92be6fd59ff625d750e5b6b1efa9c122a5e2ccf1a5210b29df25a8fbb6e8b6")
	parameters := json.RawMessage(`{"record_count":3}`)
	scenario.Steps[0].MeasurementSample = &scenarios.MeasurementSample{
		MeasurementID: "MEAS-WORKLOAD-001",
		StratumID:     "STR-WORKLOAD-001",
		SampleID:      "SAMPLE-WORKLOAD-001",
		Parameters:    parameters,
		Operation: scenarios.MeasurementOperationTarget{
			ID: "MOP-WORKLOAD-001", Family: "queue-replay", Boundary: "small", Value: append(json.RawMessage(nil), parameters...),
		},
	}
	plan, err := deriveNativePlan(scenario, scenarios.ProofObligation{
		ObligationID:           "OBL-WORKLOAD-001",
		ProofType:              "native-e2e",
		AssertionIDs:           []contract.AssertionID{"ASSERT-WORKLOAD-001"},
		RequiredMeasurementIDs: []contract.MeasurementID{"MEAS-WORKLOAD-001"},
	})
	if err != nil {
		t.Fatalf("derive native workload plan: %v", err)
	}
	selection := testSelection(plan.actions, scenario.Steps, nil, scenario.Model.ExpectedState)
	selection.scenario = scenario
	selection.plan = plan
	selection.obligation.AssertionIDs = []contract.AssertionID{"ASSERT-WORKLOAD-001"}
	selection.obligation.RequiredMeasurementIDs = []contract.MeasurementID{"MEAS-WORKLOAD-001"}
	selection.requiredMeasurements = []contract.RequiredMeasurement{{
		ID:                           "MEAS-WORKLOAD-001",
		ScenarioID:                   scenario.ID,
		SupportCellIDs:               []contract.SupportCellID{"SUP-IOS-MIN-001"},
		Metrics:                      []contract.PerformanceMetric{{ID: "MET-WORKLOAD-001"}},
		Strata:                       []contract.PerformanceStratum{{StratumID: "STR-WORKLOAD-001", Parameters: parameters}},
		MinimumSampleCountPerStratum: "1",
	}}
	manifest, err := BuildManifest(selection)
	if err != nil {
		t.Fatalf("build workload manifest: %v", err)
	}
	var execute *ManifestAction
	for index := range manifest.Actions {
		action := &manifest.Actions[index]
		if action.Action.Actor == "client" && action.Action.Command == "execute-step" {
			execute = action
			break
		}
	}
	if execute == nil || len(execute.WorkloadExpansions["STEP-WORKLOAD-001"]) != 3 {
		t.Fatalf("manifest workload expansion = %#v", execute)
	}
	if len(execute.Steps) != 1 || execute.Steps[0].MeasurementSample == nil || execute.Steps[0].MeasurementSample.SampleID != "SAMPLE-WORKLOAD-001" {
		t.Fatalf("manifest workload measurement sample = %#v", execute.Steps)
	}
	request, err := sanitizeExecuteRequest(*execute, manifest)
	if err != nil {
		t.Fatalf("sanitize workload execute request: %v", err)
	}
	if len(request.Steps) != 1 || len(request.Steps[0].ExpandedOperations) != 3 {
		t.Fatalf("workload execution step = %#v", request.Steps)
	}
}

func nativeWorkloadScenario(digest string) scenarios.Scenario {
	workload := &scenarios.NativeWorkloadParameters{
		RecordCount:    3,
		BatchSize:      2,
		Seed:           101,
		AuthoredSchema: scenarios.SchemaFact{Version: 1, Hash: strings.Repeat("a", 64)},
		ClientVersion:  "2026-08-11T00:00:00.000000Z",
		Targets: []scenarios.NativeWorkloadTarget{
			{ScopeID: "scope-a", TableID: "items", PrimaryKeyFieldID: "id"},
			{ScopeID: "scope-b", TableID: "items", PrimaryKeyFieldID: "id"},
		},
		MutationKinds: []scenarios.NativeWorkloadMutationKind{
			{Operation: "insert", Count: 2, FieldIDs: []string{"value"}},
			{Operation: "insert", Count: 1, FieldIDs: []string{"obsolete_value"}},
		},
		Expectation: scenarios.NativeWorkloadExpectation{
			OperationCount:  3,
			BatchCount:      2,
			OperationDigest: digest,
			PerScopeCardinalities: []scenarios.NativeWorkloadScopeCardinality{
				{ScopeID: "scope-a", Cardinality: 2},
				{ScopeID: "scope-b", Cardinality: 1},
			},
		},
	}
	step := scenarios.Step{
		ID:              "STEP-WORKLOAD-001",
		Phase:           "exercise",
		Transport:       "model",
		NativeBinding:   &scenarios.NativeStepBinding{Kind: "workload", UserID: "user-a", ClientID: "client-a", Workload: workload},
		Operation:       scenarios.Operation{ContractOperation: "workload", Name: "prepare", Payload: json.RawMessage(`{"profile":"pending_mutations","user_id":"user-a","client_id":"client-a","table_id":"items","accepted_count":2,"rejected_count":1}`)},
		ExpectedOutcome: scenarios.ExpectedOutcome{Disposition: "success"},
	}
	return scenarios.Scenario{
		ID: "SCN-WORKLOAD-001",
		Model: scenarios.ModelSpec{
			Setup: []scenarios.Operation{{ContractOperation: "model", Name: "install-current-contract", Payload: json.RawMessage(`{}`)}},
			ExpectedState: []scenarios.ModelExpectation{{
				ID:        "EXPECT-WORKLOAD-001",
				Predicate: scenarios.Predicate{ContractPredicate: "state-equality"},
			}},
		},
		Steps: []scenarios.Step{step},
		Assertions: []scenarios.Assertion{{
			ID:             "ASSERT-WORKLOAD-001",
			ExpectationIDs: []scenarios.ExpectationID{"EXPECT-WORKLOAD-001"},
		}},
	}
}
