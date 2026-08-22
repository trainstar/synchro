package scenarios

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/vectors"
)

func TestValidateAuthoredTimeScenario(t *testing.T) {
	bundle, err := contract.Load(context.Background(), "../../")
	if err != nil {
		t.Fatalf("load authored contract: %v", err)
	}
	scenario := authoredTimeScenario()
	if err := Validate(scenario, bundle); err != nil {
		t.Fatalf("validate authored time scenario: %v", err)
	}
}

func TestValidateNativeSynchronizationGroupsOrderedHTTPSteps(t *testing.T) {
	bundle, err := contract.Load(context.Background(), "../../")
	if err != nil {
		t.Fatalf("load authored contract: %v", err)
	}
	base := groupedNativeTimeScenario()
	if err := Validate(base, bundle); err != nil {
		t.Fatalf("validate grouped native synchronization: %v", err)
	}

	tests := []struct {
		name     string
		mutate   func(*Scenario)
		category string
	}{
		{"reversed grouped steps", func(s *Scenario) {
			action := &s.NativeExecution.Actions[2]
			action.CoversStepIDs[0], action.CoversStepIDs[1] = action.CoversStepIDs[1], action.CoversStepIDs[0]
		}, "outside authored step order"},
		{"second step identity mismatch", func(s *Scenario) {
			s.Steps[1].Operation.Payload = json.RawMessage(`{"user_id":"user-b","client_id":"client-b","client_generation":1,"schema":{"version":1,"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"scope_set_version":1,"scopes":[],"limit":1}`)
		}, "does not match step"},
		{"second step completion mismatch", func(s *Scenario) {
			s.WireExpectations[1].ContractCase = "temporary_unavailable"
			s.WireExpectations[1].HTTPStatus = 503
			s.WireExpectations[1].ErrorCode = stringPointer("temporary_unavailable")
			s.WireExpectations[1].Retryable = true
		}, "does not match step"},
		{"second step phase mismatch", func(s *Scenario) {
			s.Steps[1].Phase = "setup"
		}, "phase"},
		{"second step non-http", func(s *Scenario) {
			s.Steps[1].Transport = "local"
		}, "cannot execute step"},
		{"multiple steps on non-grouping command", func(s *Scenario) {
			action := &s.NativeExecution.Actions[2]
			action.Command = "execute-step"
			action.Parameters = json.RawMessage(`{"client_key":"client-a"}`)
		}, "must cover exactly one"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mutant := cloneScenario(base)
			test.mutate(&mutant)
			if err := requireErrorCategory(Validate(mutant, bundle), test.category); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestValidateUsesTargetsCapturedFromMakefile(t *testing.T) {
	path := "conformance/scenarios/valid.json"
	root := scenarioRepository(t, map[string][]byte{path: scenarioFixture("SCN-TARGETS-001", "Target source")})
	loaded, err := LoadFile(context.Background(), root, path)
	if err != nil {
		t.Fatalf("load scenario target context: %v", err)
	}
	bundle, err := contract.Load(context.Background(), "../../")
	if err != nil {
		t.Fatalf("load authored contract: %v", err)
	}
	scenario := authoredTimeScenario()
	scenario.makeTargets = cloneStringSet(loaded.makeTargets)
	if err := Validate(scenario, bundle); err != nil {
		t.Fatalf("validate targets captured from Makefile: %v", err)
	}
	delete(scenario.makeTargets, "test-blackbox")
	if err := requireErrorCategory(Validate(scenario, bundle), "not defined by the repository Makefile"); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRejectsSemanticMutants(t *testing.T) {
	bundle, err := contract.Load(context.Background(), "../../")
	if err != nil {
		t.Fatalf("load authored contract: %v", err)
	}
	tests := []struct {
		name     string
		base     func() Scenario
		mutate   func(*Scenario)
		category string
	}{
		{"unrelated assertion", authoredTimeScenario, func(s *Scenario) {
			s.Assertions = append(s.Assertions, Assertion{ID: "ASSERT-TIME-UNRELATED-001", RequirementIDs: []contract.RequirementID{"SYNC-TIME-001"}, Description: "unrelated", ExpectationIDs: []ExpectationID{"EXPECT-TIME-001"}, Predicate: Predicate{ContractPredicate: "state-equality", Name: "state-equals-authored-model", Payload: []byte(`{}`)}, Oracle: Oracle{Kind: "model-state-equality", ExpectedSource: "authored-model", ObservedSource: "system-under-test"}})
		}, "not bound to a proof obligation"},
		{"outside-scenario assertion", authoredTimeScenario, func(s *Scenario) {
			s.Assertions[0].RequirementIDs = []contract.RequirementID{"SYNC-CURSOR-001"}
		}, "outside the scenario"},
		{"omitted ownership", authoredTimeScenario, func(s *Scenario) { s.Ownership = s.Ownership[:len(s.Ownership)-1] }, "ownership"},
		{"nil support cell", authoredTimeScenario, func(s *Scenario) { s.ProofObligations[1].SupportCellID = nil }, "requires a swift-client support cell"},
		{"excluded support cell", authoredTimeScenario, func(s *Scenario) { s.ProofObligations[0].SupportCellID = ptrSupport("SUP-PG-017") }, "excluded"},
		{"wrong target", authoredTimeScenario, func(s *Scenario) { s.ProofObligations[0].MakeTarget = "test-conformance" }, "cannot prove"},
		{"missing repository target", authoredTimeScenario, func(s *Scenario) { delete(s.makeTargets, "test-blackbox") }, "not defined by the repository Makefile"},
		{"wrong argv", authoredTimeScenario, func(s *Scenario) { s.ProofObligations[0].Argv = []string{"make", "test-swift"} }, "argv"},
		{"unknown model operation", authoredTimeScenario, func(s *Scenario) { s.Model.Setup[0].Name = "unknown" }, "model setup operation"},
		{"unknown step operation", authoredTimeScenario, func(s *Scenario) { s.Steps[0].Operation.Name = "unknown" }, "unknown operation"},
		{"wrong wire status", authoredTimeScenario, func(s *Scenario) { s.WireExpectations[0].HTTPStatus = 201 }, "canonical status"},
		{"wrong wire error code", authoredTimeScenario, func(s *Scenario) { s.WireExpectations[0].ErrorCode = stringPointer("wrong") }, "error code"},
		{"wrong wire retryability", authoredTimeScenario, func(s *Scenario) { s.WireExpectations[0].Retryable = true }, "retryability"},
		{"transport mismatch", authoredTimeScenario, func(s *Scenario) { s.Steps[0].Transport = "local" }, "transport"},
		{"runner as subject", authoredTimeScenario, func(s *Scenario) {
			s.NegativeControls[0].SubjectArtifactInventoryIDs = append(s.NegativeControls[0].SubjectArtifactInventoryIDs, "ARTDEF-CONFORMANCE-RUNNER-001")
		}, "mutated subject"},
		{"missing required artifact", authoredTimeScenario, func(s *Scenario) {
			s.ProofObligations[0].ArtifactInventoryIDs = []contract.ArtifactInventoryID{"ARTDEF-PG-EXTENSION-001"}
		}, "requires artifact role adapter"},
		{"non-singleton negative requirement", authoredTimeScenario, func(s *Scenario) {
			s.ProofObligations[len(s.ProofObligations)-1].RequirementIDs = append(s.ProofObligations[len(s.ProofObligations)-1].RequirementIDs, "SYNC-CURSOR-001")
		}, "negative-control must own exactly one requirement"},
		{"non-singleton negative assertion", authoredTimeScenario, func(s *Scenario) {
			s.ProofObligations[len(s.ProofObligations)-1].AssertionIDs = append(s.ProofObligations[len(s.ProofObligations)-1].AssertionIDs, "ASSERT-TIME-PG-001")
		}, "negative-control must own exactly one assertion"},
		{"orphan fault plan", authoredTimeScenario, func(s *Scenario) {
			plan := s.FaultPlans[0]
			plan.ID = "FPL-TIMESTAMP-002"
			s.FaultPlans = append(s.FaultPlans, plan)
		}, "orphaned"},
		{"wrong injection recipe", authoredTimeScenario, func(s *Scenario) { s.FaultPlans[0].Injection.Parameters.Defect = "wrong defect" }, "injection recipe"},
		{"missing normative reference", authoredTimeScenario, func(s *Scenario) { s.NormativeReferences = nil }, "mandatory requirement anchor"},
		{"extra outside-snapshot reference", authoredTimeScenario, func(s *Scenario) {
			s.NormativeReferences = append(s.NormativeReferences, contract.NormativeReference{Path: "docs/src/content/docs/spec/not-frozen.mdx", Anchor: "#not-frozen"})
		}, "frozen contract snapshot"},
		{"unbound expectation", authoredTimeScenario, func(s *Scenario) {
			s.Model.ExpectedState = append(s.Model.ExpectedState, ModelExpectation{ID: "EXPECT-TIME-002", Predicate: Predicate{ContractPredicate: "state-equality", Name: "state-equals-authored-model", Payload: []byte(`{}`)}, StateFacts: &StateFacts{Registry: &RegistryFact{CurrentGeneration: 1}}})
		}, "not bound to an assertion"},
		{"noncontiguous barrier order", authoredTimeScenario, func(s *Scenario) { s.BarrierPlan.Barriers[0].ReleaseOrder = 2 }, "contiguous"},
		{"randomized replay without seed", authoredTimeScenario, func(s *Scenario) { s.Replay.Mode = "randomized" }, "seed"},
		{"nonempty vector IDs", authoredTimeScenario, func(s *Scenario) {
			s.ProofObligations[0].RequiredVectorSetIDs = []contract.VectorSetID{"VSET-TASK4-001"}
		}, "vector"},
		{"missing required proof cell", authoredTimeScenario, func(s *Scenario) {
			s.ProofObligations = append(s.ProofObligations[:1], s.ProofObligations[2:]...)
		}, "requires exactly one native-e2e proof obligation"},
		{"duplicate proof key", authoredTimeScenario, func(s *Scenario) {
			duplicate := s.ProofObligations[1]
			duplicate.ObligationID = "OBL-TIME-DUP-001"
			s.ProofObligations = append(s.ProofObligations, duplicate)
			s.Ownership = append(s.Ownership, Ownership{ScenarioID: s.ID, RequirementID: "SYNC-TIME-001", ProofObligationID: duplicate.ObligationID, AssertionID: duplicate.AssertionIDs[0], ProofType: duplicate.ProofType, SupportCellID: duplicate.SupportCellID})
		}, "duplicate obligation proof key"},
		{"extra non-required reference-model proof", authoredTimeScenario, func(s *Scenario) {
			s.ProofTypes = append(s.ProofTypes, "reference-model")
			obligation := ProofObligation{ObligationID: "OBL-TIME-EXTRA-MODEL-001", RequirementIDs: []contract.RequirementID{"SYNC-TIME-001"}, AssertionIDs: []contract.AssertionID{"ASSERT-TIME-PG-001"}, ProofType: "reference-model", SupportCellID: nil, ArtifactInventoryIDs: []contract.ArtifactInventoryID{"ARTDEF-CONFORMANCE-RUNNER-001"}, PerformanceBudgetIDs: []contract.BudgetID{}, RequiredMeasurementIDs: []contract.MeasurementID{}, RequiredVectorSetIDs: []contract.VectorSetID{}, MakeTarget: "test-conformance", Argv: []string{"make", "test-conformance"}, FaultPlanID: nil, ControlID: nil}
			s.ProofObligations = append(s.ProofObligations, obligation)
			s.Ownership = append(s.Ownership, Ownership{ScenarioID: s.ID, RequirementID: "SYNC-TIME-001", ProofObligationID: obligation.ObligationID, AssertionID: "ASSERT-TIME-PG-001", ProofType: obligation.ProofType, SupportCellID: nil})
		}, "non-required proof type reference-model"},
		{"multiple optional fault-injection obligations", authoredTimeScenarioWithFaultInjection, func(s *Scenario) {
			duplicate := s.ProofObligations[len(s.ProofObligations)-1]
			duplicate.ObligationID = "OBL-TIME-FI-002"
			s.ProofObligations = append(s.ProofObligations, duplicate)
			s.Ownership = append(s.Ownership, Ownership{ScenarioID: s.ID, RequirementID: "SYNC-TIME-001", ProofObligationID: duplicate.ObligationID, AssertionID: duplicate.AssertionIDs[0], ProofType: duplicate.ProofType, SupportCellID: duplicate.SupportCellID})
		}, "multiple optional fault-injection"},
		{"non-singleton fault requirement", authoredTimeScenarioWithFaultInjection, func(s *Scenario) {
			index := len(s.ProofObligations) - 1
			s.ProofObligations[index].RequirementIDs = append(s.ProofObligations[index].RequirementIDs, "SYNC-CURSOR-001")
		}, "fault-injection must own exactly one requirement"},
		{"non-singleton fault assertion", authoredTimeScenarioWithFaultInjection, func(s *Scenario) {
			index := len(s.ProofObligations) - 1
			s.ProofObligations[index].AssertionIDs = append(s.ProofObligations[index].AssertionIDs, "ASSERT-TIME-PG-001")
		}, "fault-injection must own exactly one assertion"},
		{"missing native execution", authoredTimeScenario, func(s *Scenario) {
			s.NativeExecution = nil
		}, "require native_execution"},
		{"native execution without obligation", authoredTimeScenario, func(s *Scenario) {
			var obligations []ProofObligation
			removed := make(map[contract.ObligationID]struct{})
			for _, obligation := range s.ProofObligations {
				if obligation.ProofType == "native-e2e" {
					removed[obligation.ObligationID] = struct{}{}
					continue
				}
				obligations = append(obligations, obligation)
			}
			s.ProofObligations = obligations
			s.ProofTypes = []string{"server-black-box", "negative-control"}
			var ownership []Ownership
			for _, item := range s.Ownership {
				if _, found := removed[item.ProofObligationID]; !found {
					ownership = append(ownership, item)
				}
			}
			s.Ownership = ownership
		}, "without a native-e2e proof obligation"},
		{"duplicate native action", authoredTimeScenario, func(s *Scenario) {
			duplicate := s.NativeExecution.Actions[2]
			s.NativeExecution.Actions = append(s.NativeExecution.Actions, duplicate)
		}, "duplicate native action ID"},
		{"omitted native step", authoredTimeScenario, func(s *Scenario) {
			s.NativeExecution.Actions = append(s.NativeExecution.Actions[:2], s.NativeExecution.Actions[3:]...)
		}, "has 0 covering actions"},
		{"duplicate native step", authoredTimeScenario, func(s *Scenario) {
			duplicate := s.NativeExecution.Actions[2]
			duplicate.ID = "NACT-TIME-SYNC-002"
			s.NativeExecution.Actions = append(s.NativeExecution.Actions, duplicate)
		}, "has 2 covering actions"},
		{"unknown native step", authoredTimeScenario, func(s *Scenario) {
			s.NativeExecution.Actions[2].CoversStepIDs[0] = "STEP-TIME-UNKNOWN-001"
		}, "covers unknown step"},
		{"mismatched native command", authoredTimeScenario, func(s *Scenario) {
			s.NativeExecution.Actions[2].Command = "execute-step"
			s.NativeExecution.Actions[2].Parameters = json.RawMessage(`{"client_key":"client-a"}`)
		}, "cannot execute step"},
		{"unknown native client", authoredTimeScenario, func(s *Scenario) {
			s.NativeExecution.Actions[2].Parameters = json.RawMessage(`{"client_key":"client-unknown","method":"start","completion":"idle"}`)
		}, "references unknown client"},
		{"native client identity mismatch", authoredTimeScenario, func(s *Scenario) {
			s.NativeExecution.Clients[0].UserID = "user-b"
		}, "does not match step"},
		{"native client use before open", authoredTimeScenario, func(s *Scenario) {
			s.NativeExecution.Actions[1], s.NativeExecution.Actions[2] = s.NativeExecution.Actions[2], s.NativeExecution.Actions[1]
		}, "before open"},
		{"missing native install", authoredTimeScenario, func(s *Scenario) {
			s.NativeExecution.Actions = s.NativeExecution.Actions[1:]
		}, "install-model actions"},
		{"premature native observation", authoredTimeScenario, func(s *Scenario) {
			s.NativeExecution.Actions[2], s.NativeExecution.Actions[3] = s.NativeExecution.Actions[3], s.NativeExecution.Actions[2]
		}, "before all scenario steps complete"},
		{"missing native expectation", authoredTimeScenario, func(s *Scenario) {
			s.NativeExecution.Actions = s.NativeExecution.Actions[:3]
		}, "native expectation EXPECT-TIME-001 has 0 capture actions"},
		{"extra native expectation", authoredTimeScenario, func(s *Scenario) {
			s.NativeExecution.Actions[3].Parameters = json.RawMessage(`{"client_keys":[],"sources":["server-state"],"expectation_ids":["EXPECT-TIME-UNKNOWN-001"]}`)
		}, "extra expectation"},
		{"local native capture without client", authoredTimeScenario, func(s *Scenario) {
			s.NativeExecution.Actions[3].Parameters = json.RawMessage(`{"client_keys":[],"sources":["sync-status"],"expectation_ids":["EXPECT-TIME-001"]}`)
		}, "local capture sources require at least one client key"},
		{"extra native measurement", authoredTimeScenario, func(s *Scenario) {
			s.NativeExecution.Actions = append(s.NativeExecution.Actions, NativeAction{ID: "NACT-TIME-MEASURE-001", Phase: "verify", Actor: "observer", Command: "measure", CoversStepIDs: []StepID{}, Parameters: json.RawMessage(`{"performance_budget_ids":[],"measurement_ids":["MEAS-TIME-EXTRA-001"]}`)})
		}, "extra measurement"},
		{"extra native performance budget", authoredTimeScenario, func(s *Scenario) {
			s.NativeExecution.Actions = append(s.NativeExecution.Actions, NativeAction{ID: "NACT-TIME-MEASURE-001", Phase: "verify", Actor: "observer", Command: "measure", CoversStepIDs: []StepID{}, Parameters: json.RawMessage(`{"performance_budget_ids":["BUD-TIME-EXTRA-001"],"measurement_ids":[]}`)})
		}, "extra performance budget"},
		{"missing native measurement", authoredTimeScenario, func(s *Scenario) {
			s.ProofObligations[1].RequiredMeasurementIDs = []contract.MeasurementID{"MEAS-TIME-MISSING-001"}
		}, "native measurement MEAS-TIME-MISSING-001 has 0 measure actions"},
		{"missing native performance budget", authoredTimeScenario, func(s *Scenario) {
			s.ProofObligations[1].PerformanceBudgetIDs = []contract.BudgetID{"BUD-TIME-MISSING-001"}
		}, "native performance budget BUD-TIME-MISSING-001 has 0 measure actions"},
		{"native completion mismatch", authoredTimeScenario, func(s *Scenario) {
			s.NativeExecution.Actions[2].Parameters = json.RawMessage(`{"client_key":"client-a","method":"start","completion":"error"}`)
		}, "does not match step"},
		{"unknown native parameter", authoredTimeScenario, func(s *Scenario) {
			s.NativeExecution.Actions[2].Parameters = json.RawMessage(`{"client_key":"client-a","method":"start","completion":"idle","extra":true}`)
		}, "unknown member"},
		{"native reuse without seed", authoredTimeScenario, func(s *Scenario) {
				s.NativeExecution.Actions[1].Parameters = json.RawMessage(`{"client_key":"client-a","database_mode":"reuse","initialization":"empty","seed_step_id":null}`)
		}, "reuse open requires a seed step"},
		{"duplicate native database", authoredTimeScenario, func(s *Scenario) {
			s.NativeExecution.Clients = append(s.NativeExecution.Clients, NativeClient{Key: "client-b", UserID: "user-b", ClientID: "client-b", DatabaseKey: "database-a"})
		}, "share database key"},
		{"unknown native process trigger", authoredTimeScenario, func(s *Scenario) {
			terminate := NativeAction{ID: "NACT-TIME-TERMINATE-001", Phase: "exercise", Actor: "process", Command: "terminate", CoversStepIDs: []StepID{}, Parameters: json.RawMessage(`{"client_key":"client-a","boundary":"queue-resolved","after_action_id":"NACT-TIME-UNKNOWN-001"}`)}
			s.NativeExecution.Actions = append(s.NativeExecution.Actions[:3], append([]NativeAction{terminate}, s.NativeExecution.Actions[3:]...)...)
		}, "does not name an earlier boundary action"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := test.base()
			if err := Validate(base, bundle); err != nil {
				t.Fatalf("test base is invalid before mutation: %v", err)
			}
			mutant := cloneScenario(base)
			if err := Validate(mutant, bundle); err != nil {
				t.Fatalf("deep clone is invalid before mutation: %v", err)
			}
			test.mutate(&mutant)
			err := Validate(mutant, bundle)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(test.category)) {
				t.Fatalf("error = %v, want category %q", err, test.category)
			}
		})
	}
}

func TestValidateNativeProcessBoundaryOrdering(t *testing.T) {
	bundle, err := contract.Load(context.Background(), "../../")
	if err != nil {
		t.Fatalf("load authored contract: %v", err)
	}
	scenario := authoredTimeScenario()
	terminate := NativeAction{ID: "NACT-TIME-TERMINATE-001", Phase: "exercise", Actor: "process", Command: "terminate", CoversStepIDs: []StepID{}, Parameters: json.RawMessage(`{"client_key":"client-a","boundary":"queue-resolved","after_action_id":"NACT-TIME-SYNC-001"}`)}
	relaunch := NativeAction{ID: "NACT-TIME-RELAUNCH-001", Phase: "exercise", Actor: "process", Command: "relaunch", CoversStepIDs: []StepID{}, Parameters: json.RawMessage(`{"client_key":"client-a","boundary":"queue-resolved","after_action_id":"NACT-TIME-TERMINATE-001"}`)}
	scenario.NativeExecution.Actions = append(scenario.NativeExecution.Actions[:3], append([]NativeAction{terminate, relaunch}, scenario.NativeExecution.Actions[3:]...)...)
	if err := Validate(scenario, bundle); err != nil {
		t.Fatalf("validate ordered native process boundary: %v", err)
	}

	mutant := cloneScenario(scenario)
	mutant.NativeExecution.Actions[4].Parameters = json.RawMessage(`{"client_key":"client-a","boundary":"queue-resolved","after_action_id":"NACT-TIME-SYNC-001"}`)
	if err := requireErrorCategory(Validate(mutant, bundle), "does not match the latest termination"); err != nil {
		t.Fatal(err)
	}
}

func TestValidateTransportFailureWireCaseIsClosed(t *testing.T) {
	bundle, err := contract.Load(context.Background(), "../../")
	if err != nil {
		t.Fatalf("load authored contract: %v", err)
	}
	base := authoredTimeScenario()
	base.WireExpectations[0] = WireExpectation{
		StepID:       "STEP-TIME-001",
		AssertionID:  "ASSERT-TIME-PG-001",
		ContractCase: "transport_failure",
		HTTPStatus:   0,
		ErrorCode:    nil,
		Retryable:    true,
	}
	base.NativeExecution.Actions[2].Parameters = json.RawMessage(`{"client_key":"client-a","method":"start","completion":"blocked"}`)
	if err := Validate(base, bundle); err != nil {
		t.Fatalf("validate transport failure wire case: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*WireExpectation)
	}{
		{"response status", func(wire *WireExpectation) { wire.HTTPStatus = 503 }},
		{"error code", func(wire *WireExpectation) { wire.ErrorCode = stringPointer("temporary_unavailable") }},
		{"not retryable", func(wire *WireExpectation) { wire.Retryable = false }},
		{"zero status success", func(wire *WireExpectation) {
			wire.ContractCase = "pull_success"
			wire.Retryable = false
		}},
		{"response error without code", func(wire *WireExpectation) {
			wire.ContractCase = "temporary_unavailable"
			wire.HTTPStatus = 503
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mutant := cloneScenario(base)
			test.mutate(&mutant.WireExpectations[0])
			if err := requireErrorCategory(Validate(mutant, bundle), "canonical status, error code, and retryability"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestValidatePerformanceClosureAndValidateAll(t *testing.T) {
	bundle, err := contract.Load(context.Background(), "../../")
	if err != nil {
		t.Fatalf("load authored contract: %v", err)
	}
	base := authoredTimeScenario()
	partial := cloneBundle(bundle)
	partial.Performance.Budgets = nil
	partial.Performance.RequiredMeasurements = nil
	if err := Validate(base, partial); err != nil {
		t.Fatalf("validate with partial performance catalog: %v", err)
	}
	if err := ValidateAll([]Scenario{base}, partial); err != nil {
		t.Fatalf("validate one selected scenario with partial performance catalog: %v", err)
	}

	positiveBundle := cloneBundle(partial)
	budget := bundle.Performance.Budgets[0]
	budget.ID = "BUD-TIME-001"
	budget.ScenarioID = base.ID
	budget.SupportCellIDs = []contract.SupportCellID{"SUP-PG-018"}
	budget.ArtifactInventoryIDs = []contract.ArtifactInventoryID{"ARTDEF-PG-EXTENSION-001", "ARTDEF-ADAPTER-001"}
	measurement := bundle.Performance.RequiredMeasurements[0]
	measurement.ID = "MEAS-TIME-001"
	measurement.ScenarioID = base.ID
	measurement.SupportCellIDs = []contract.SupportCellID{"SUP-PG-018"}
	measurement.ArtifactInventoryIDs = []contract.ArtifactInventoryID{"ARTDEF-PG-EXTENSION-001", "ARTDEF-ADAPTER-001"}
	positiveBundle.Performance.Budgets = []contract.PerformanceBudget{budget}
	positiveBundle.Performance.RequiredMeasurements = []contract.RequiredMeasurement{measurement}
	positiveScenario := cloneScenario(base)
	performanceAssertion := Assertion{ID: "ASSERT-TIME-PERF-001", RequirementIDs: []contract.RequirementID{"SYNC-TIME-001"}, Description: "The performance contract is satisfied.", ExpectationIDs: []ExpectationID{"EXPECT-TIME-001"}, Predicate: Predicate{ContractPredicate: "performance-measurement", Name: "performance-contract-satisfied", Payload: []byte(`{}`)}, Oracle: Oracle{Kind: "performance-budget", ExpectedSource: "authored-model", ObservedSource: "system-under-test"}, DetectsControlIDs: []contract.ControlID{}}
	positiveScenario.Assertions = append(positiveScenario.Assertions, performanceAssertion)
	positiveScenario.ProofObligations[0].AssertionIDs = append(positiveScenario.ProofObligations[0].AssertionIDs, performanceAssertion.ID)
	positiveScenario.ProofObligations[0].PerformanceBudgetIDs = []contract.BudgetID{budget.ID}
	positiveScenario.ProofObligations[0].RequiredMeasurementIDs = []contract.MeasurementID{measurement.ID}
	positiveScenario.Ownership = append(positiveScenario.Ownership, Ownership{ScenarioID: positiveScenario.ID, RequirementID: "SYNC-TIME-001", ProofObligationID: positiveScenario.ProofObligations[0].ObligationID, AssertionID: performanceAssertion.ID, ProofType: positiveScenario.ProofObligations[0].ProofType, SupportCellID: positiveScenario.ProofObligations[0].SupportCellID})
	if err := Validate(positiveScenario, positiveBundle); err != nil {
		t.Fatalf("validate populated performance ownership: %v", err)
	}

	if err := requireErrorCategory(Validate(base, positiveBundle), "performance item BUD-TIME-001"); err != nil {
		t.Fatal(err)
	}
	measurementOnlyBundle := cloneBundle(partial)
	measurementOnlyBundle.Performance.RequiredMeasurements = []contract.RequiredMeasurement{measurement}
	if err := requireErrorCategory(Validate(base, measurementOnlyBundle), "performance item MEAS-TIME-001"); err != nil {
		t.Fatal(err)
	}

	unknownDeclaration := cloneScenario(base)
	unknownDeclaration.ProofObligations[0].PerformanceBudgetIDs = []contract.BudgetID{"BUD-UNKNOWN-001"}
	if err := requireErrorCategory(Validate(unknownDeclaration, partial), "unknown performance budget"); err != nil {
		t.Fatal(err)
	}
	unknownMeasurement := cloneScenario(base)
	unknownMeasurement.ProofObligations[0].RequiredMeasurementIDs = []contract.MeasurementID{"MEAS-UNKNOWN-001"}
	if err := requireErrorCategory(Validate(unknownMeasurement, partial), "unknown required measurement"); err != nil {
		t.Fatal(err)
	}

	wrongScenarioDeclaration := cloneScenario(base)
	wrongScenarioDeclaration.ProofObligations[0].PerformanceBudgetIDs = []contract.BudgetID{bundle.Performance.Budgets[0].ID}
	wrongScenarioBundle := cloneBundle(bundle)
	if err := requireErrorCategory(Validate(wrongScenarioDeclaration, wrongScenarioBundle), "authored for scenario"); err != nil {
		t.Fatal(err)
	}

	if err := requireErrorCategory(ValidateAll([]Scenario{base}, bundle), "absent scenario"); err != nil {
		t.Fatal(err)
	}

	second := cloneScenario(base)
	second.ID = "SCN-TIME-002"
	for index := range second.Ownership {
		second.Ownership[index].ScenarioID = second.ID
	}
	if err := Validate(second, partial); err != nil {
		t.Fatalf("validate independently valid second scenario: %v", err)
	}
	if err := requireErrorCategory(ValidateAll([]Scenario{base, second}, partial), "selected requirement"); err != nil {
		t.Fatal(err)
	}
	if err := requireErrorCategory(ValidateAll([]Scenario{base, base}, partial), "duplicate scenario ID"); err != nil {
		t.Fatal(err)
	}
}

func TestValidateErrorsAreDeterministicAndNilBundlesFailClosed(t *testing.T) {
	base := authoredTimeScenario()
	if err := Validate(base, nil); err == nil || !strings.Contains(err.Error(), "bundle is nil") {
		t.Fatalf("nil bundle error = %v", err)
	}
	if err := ValidateAll([]Scenario{base}, nil); err == nil || !strings.Contains(err.Error(), "bundle is nil") {
		t.Fatalf("nil bundle ValidateAll error = %v", err)
	}

	bundle, err := contract.Load(context.Background(), "../../")
	if err != nil {
		t.Fatalf("load authored contract: %v", err)
	}
	mutant := cloneScenario(base)
	mutant.Model.Setup[0].Name = "unknown"
	mutant.WireExpectations[0].HTTPStatus = 201
	first := Validate(mutant, bundle)
	second := Validate(mutant, bundle)
	if first == nil || second == nil || first.Error() != second.Error() {
		t.Fatalf("repeated errors differ: first=%v second=%v", first, second)
	}
}

type vectorSetLookupStub map[contract.VectorSetID]bool

func (s vectorSetLookupStub) Has(id contract.VectorSetID) bool { return s[id] }

func TestValidateVectorSetClosure(t *testing.T) {
	bundle, err := contract.Load(context.Background(), "../../")
	if err != nil {
		t.Fatalf("load authored contract: %v", err)
	}
	partial := cloneBundle(bundle)
	partial.Performance.Budgets = nil
	partial.Performance.RequiredMeasurements = nil
	const knownID contract.VectorSetID = "VSET-TASK4-001"

	empty := authoredTimeScenario()
	if err := Validate(empty, partial); err != nil {
		t.Fatalf("existing Validate rejected empty vector list: %v", err)
	}
	if err := ValidateAll([]Scenario{empty}, partial); err != nil {
		t.Fatalf("existing ValidateAll rejected empty vector list: %v", err)
	}

	known := cloneScenario(empty)
	known.ProofObligations[0].RequiredVectorSetIDs = []contract.VectorSetID{knownID}
	lookup := vectorSetLookupStub{knownID: true}
	if err := ValidateWithVectors(known, partial, lookup); err != nil {
		t.Fatalf("ValidateWithVectors rejected known vector set: %v", err)
	}
	if err := ValidateAllWithVectors([]Scenario{known}, partial, lookup); err != nil {
		t.Fatalf("ValidateAllWithVectors rejected known vector set: %v", err)
	}

	if err := requireErrorCategory(Validate(known, partial), "vector-set catalog"); err != nil {
		t.Fatal(err)
	}
	if err := requireErrorCategory(ValidateAll([]Scenario{known}, partial), "vector-set catalog"); err != nil {
		t.Fatal(err)
	}

	unknown := cloneScenario(empty)
	unknown.ProofObligations[0].RequiredVectorSetIDs = []contract.VectorSetID{"VSET-UNKNOWN-001"}
	if err := requireErrorCategory(ValidateWithVectors(unknown, partial, lookup), "unknown vector set"); err != nil {
		t.Fatal(err)
	}

	duplicate := cloneScenario(empty)
	duplicate.ProofObligations[0].RequiredVectorSetIDs = []contract.VectorSetID{knownID, knownID}
	if err := requireErrorCategory(ValidateWithVectors(duplicate, partial, lookup), "duplicate required vector set"); err != nil {
		t.Fatal(err)
	}
}

func TestValidateVectorSetClosureUsesFrozenCatalog(t *testing.T) {
	bundle, err := contract.Load(context.Background(), "../../")
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := vectors.Load(context.Background(), "../../")
	if err != nil {
		t.Fatal(err)
	}
	scenario := authoredTimeScenario()
	scenario.ProofObligations[0].RequiredVectorSetIDs = []contract.VectorSetID{"VSET-CANONICAL-001"}
	if err := ValidateWithVectors(scenario, bundle, catalog); err != nil {
		t.Fatalf("ValidateWithVectors rejected frozen vector set: %v", err)
	}
	scenario.ProofObligations[0].RequiredVectorSetIDs = []contract.VectorSetID{"VSET-ABSENT-001"}
	if err := requireErrorCategory(ValidateWithVectors(scenario, bundle, catalog), "unknown vector set"); err != nil {
		t.Fatal(err)
	}
}

func requireErrorCategory(err error, category string) error {
	if err == nil {
		return fmt.Errorf("expected validation error containing %q", category)
	}
	if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(category)) {
		return fmt.Errorf("validation error = %v, want category %q", err, category)
	}
	return nil
}

func authoredTimeScenario() Scenario {
	const scenarioID contract.ScenarioID = "SCN-TIME-001"
	const requirementID contract.RequirementID = "SYNC-TIME-001"
	clientCells := []struct {
		id     contract.SupportCellID
		target string
	}{
		{"SUP-IOS-MIN-001", "test-swift"},
		{"SUP-IOS-CURRENT-001", "test-swift"},
		{"SUP-MACOS-MIN-001", "test-swift"},
		{"SUP-MACOS-CURRENT-001", "test-swift"},
		{"SUP-ANDROID-MIN-001", "test-kotlin"},
		{"SUP-ANDROID-CURRENT-001", "test-kotlin"},
		{"SUP-RN-IOS-MIN-001", "test-rn-e2e-ios"},
		{"SUP-RN-IOS-CURRENT-001", "test-rn-e2e-ios"},
		{"SUP-RN-ANDROID-MIN-001", "test-rn-e2e-android"},
		{"SUP-RN-ANDROID-CURRENT-001", "test-rn-e2e-android"},
	}
	s := Scenario{
		SchemaURI:           "https://synchro.dev/conformance/schemas/scenario-v2.schema.json",
		SchemaVersion:       2,
		ID:                  scenarioID,
		Title:               "Canonical time",
		RequirementIDs:      []contract.RequirementID{requirementID},
		NormativeReferences: []contract.NormativeReference{{Path: "docs/src/content/docs/spec/04-invariants.mdx", Anchor: "#canonical-time-format"}},
		ProofTypes:          []string{"server-black-box", "native-e2e", "negative-control"},
		Model: ModelSpec{
			Setup:         []Operation{{ContractOperation: "model", Name: "install-current-contract", Payload: []byte(minimalInstallPayload)}},
			ExpectedState: []ModelExpectation{{ID: "EXPECT-TIME-001", Predicate: Predicate{ContractPredicate: "state-equality", Name: "state-equals-authored-model", Payload: []byte(`{}`)}, StateFacts: &StateFacts{Registry: &RegistryFact{CurrentGeneration: 1}}}},
		},
		BarrierPlan: BarrierPlan{Barriers: []Barrier{{ID: "BAR-TIME-001", Name: "fault", ReleaseOrder: 1, Participants: []string{"runner"}}}},
		Replay:      ReplaySpec{Mode: "deterministic", BarrierTraceRequired: true},
		Steps: []Step{{
			ID:              "STEP-TIME-001",
			Phase:           "exercise",
			Transport:       "http",
			Operation:       Operation{ContractOperation: "pull", Name: "request-page", Payload: []byte(`{"user_id":"user-a","client_id":"client-a","client_generation":1,"schema":{"version":1,"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"scope_set_version":1,"scopes":[],"limit":1}`)},
			ExpectedOutcome: ExpectedOutcome{Disposition: "success"},
		}},
		NativeExecution: &NativeExecutionPlan{
			Version: 1,
			Clients: []NativeClient{{
				Key:         "client-a",
				UserID:      "user-a",
				ClientID:    "client-a",
				DatabaseKey: "database-a",
			}},
			Actions: []NativeAction{
				{ID: "NACT-TIME-INSTALL-001", Phase: "setup", Actor: "controller", Command: "install-model", CoversStepIDs: []StepID{}, Parameters: json.RawMessage(`{}`)},
				{ID: "NACT-TIME-OPEN-001", Phase: "setup", Actor: "client", Command: "open", CoversStepIDs: []StepID{}, Parameters: json.RawMessage(`{"client_key":"client-a","database_mode":"create","initialization":"empty","seed_step_id":null}`)},
				{ID: "NACT-TIME-SYNC-001", Phase: "exercise", Actor: "client", Command: "synchronize-step", CoversStepIDs: []StepID{"STEP-TIME-001"}, Parameters: json.RawMessage(`{"client_key":"client-a","method":"start","completion":"idle"}`)},
				{ID: "NACT-TIME-CAPTURE-001", Phase: "verify", Actor: "observer", Command: "capture", CoversStepIDs: []StepID{}, Parameters: json.RawMessage(`{"client_keys":[],"sources":["server-state"],"expectation_ids":["EXPECT-TIME-001"]}`)},
			},
		},
		makeTargets: validationMakeTargets(),
	}

	assertionIDs := make([]contract.AssertionID, 0, 11)
	addAssertion := func(id contract.AssertionID, kind, predicate, expectation string, detects []contract.ControlID) {
		assertionIDs = append(assertionIDs, id)
		s.Assertions = append(s.Assertions, Assertion{ID: id, RequirementIDs: []contract.RequirementID{requirementID}, Description: "assertion", ExpectationIDs: []ExpectationID{ExpectationID(expectation)}, Predicate: Predicate{ContractPredicate: predicate, Name: predicateName(predicate, kind)}, Oracle: Oracle{Kind: kind, ExpectedSource: "authored-model", ObservedSource: "system-under-test"}, DetectsControlIDs: detects})
	}
	addAssertion("ASSERT-TIME-PG-001", "wire-contract", "wire-outcome", "EXPECT-TIME-001", []contract.ControlID{"CTRL-TIMESTAMP-001"})
	for index := range clientCells {
		addAssertion(contract.AssertionID("ASSERT-TIME-CLIENT-"+formatThree(index+1)), "model-state-equality", "state-equality", "EXPECT-TIME-001", nil)
	}

	addObligation := func(id contract.ObligationID, proof string, support *contract.SupportCellID, target string, artifacts []contract.ArtifactInventoryID, assertion contract.AssertionID, plan *contract.FaultPlanID, control *contract.ControlID) {
		s.ProofObligations = append(s.ProofObligations, ProofObligation{ObligationID: id, RequirementIDs: []contract.RequirementID{requirementID}, AssertionIDs: []contract.AssertionID{assertion}, ProofType: proof, SupportCellID: support, ArtifactInventoryIDs: artifacts, PerformanceBudgetIDs: []contract.BudgetID{}, RequiredMeasurementIDs: []contract.MeasurementID{}, RequiredVectorSetIDs: []contract.VectorSetID{}, MakeTarget: target, Argv: []string{"make", target}, FaultPlanID: plan, ControlID: control})
		s.Ownership = append(s.Ownership, Ownership{ScenarioID: scenarioID, RequirementID: requirementID, ProofObligationID: id, AssertionID: assertion, ProofType: proof, SupportCellID: support})
	}
	addObligation("OBL-TIME-PG-001", "server-black-box", ptrSupport("SUP-PG-018"), "test-blackbox", []contract.ArtifactInventoryID{"ARTDEF-PG-EXTENSION-001", "ARTDEF-ADAPTER-001"}, assertionIDs[0], nil, nil)
	for index, cell := range clientCells {
		artifacts := []contract.ArtifactInventoryID{"ARTDEF-PG-EXTENSION-001", "ARTDEF-ADAPTER-001"}
		if cell.target == "test-swift" {
			artifacts = append(artifacts, "ARTDEF-SWIFT-SPM-001")
		} else if cell.target == "test-kotlin" {
			artifacts = append(artifacts, "ARTDEF-KOTLIN-MAVEN-001")
		} else if cell.target == "test-rn-e2e-ios" {
			artifacts = append(artifacts, "ARTDEF-SWIFT-SPM-001", "ARTDEF-COCOAPODS-001", "ARTDEF-RN-NPM-001")
		} else {
			artifacts = append(artifacts, "ARTDEF-KOTLIN-MAVEN-001", "ARTDEF-RN-NPM-001")
		}
		cellID := cell.id
		addObligation(contract.ObligationID("OBL-TIME-CLIENT-"+formatThree(index+1)), "native-e2e", &cellID, cell.target, artifacts, assertionIDs[index+1], nil, nil)
	}
	planID := contract.FaultPlanID("FPL-TIMESTAMP-001")
	controlID := contract.ControlID("CTRL-TIMESTAMP-001")
	addObligation("OBL-TIME-NC-001", "negative-control", nil, "test-conformance", []contract.ArtifactInventoryID{"ARTDEF-CONFORMANCE-RUNNER-001", "ARTDEF-ADAPTER-001"}, assertionIDs[0], &planID, &controlID)
	s.FaultPlans = []FaultPlan{{ID: planID, RequirementID: requirementID, FaultID: "FAULT-TIME-001", ControlID: controlID, BarrierID: "BAR-TIME-001", ExpectedAssertionIDs: []contract.AssertionID{assertionIDs[0]}, Injection: InjectionRecipe{Mechanism: "wire-fault", Target: "push.client_version timestamp decoder", Operator: "replace", Parameters: InjectionParameters{Scenario: "portable datetime mutation with a timestamp offset and missing microseconds", Defect: "accept the non-UTC or noncanonical representation instead of rejecting before mutation state changes"}}}}
	s.NegativeControls = []NegativeControl{{ControlID: controlID, RequirementID: requirementID, FaultID: "FAULT-TIME-001", SubjectArtifactInventoryIDs: []contract.ArtifactInventoryID{"ARTDEF-ADAPTER-001"}, DetectedBy: []contract.AssertionID{assertionIDs[0]}}}
	s.WireExpectations = []WireExpectation{{StepID: "STEP-TIME-001", AssertionID: assertionIDs[0], ContractCase: "pull_success", HTTPStatus: 200, ErrorCode: nil, Retryable: false}}
	return s
}

func groupedNativeTimeScenario() Scenario {
	scenario := authoredTimeScenario()
	second := scenario.Steps[0]
	second.ID = "STEP-TIME-002"
	scenario.Steps = append(scenario.Steps, second)
	secondWire := scenario.WireExpectations[0]
	secondWire.StepID = second.ID
	scenario.WireExpectations = append(scenario.WireExpectations, secondWire)
	scenario.NativeExecution.Actions[2].CoversStepIDs = append(
		scenario.NativeExecution.Actions[2].CoversStepIDs,
		second.ID,
	)
	return scenario
}

const minimalInstallPayload = `{
  "installation":{"installed":true,"schema_name":"synchro","extension_version":"0.3.0","protocol_version":3,"minimum_client_runtime":3,"stale_client_interval_milliseconds":1,"endpoints":[],"capabilities":[]},
  "initial_schema":{"schema":{"version":1,"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"body":"{}","transition_class":"initial","compatibility_floor":1,"tables":[],"affected_scopes":[]},
  "initial_registry":{"registry_generation":1,"relations":[],"capture_dependencies":[],"scope_rules":[],"dependency_impacts":[]},
  "stream":{"stream_generation":"stream-1","database":"synchro","worker_id":"worker-1","slot_id":"slot-1"},
  "empty_scopes":[],
  "clients":[],
  "write_policies":[],
  "configured_limits":{"max_scope_fanout":8,"max_impact_rows":1000,"pull_maximum":1000,"rebuild_maximum":1000,"compaction_batch_maximum":10000,"backfill_batch_maximum":1000}
}`

func predicateName(predicate, oracle string) string {
	if oracle == "wire-contract" {
		return "canonical-wire-outcome"
	}
	return "state-equals-authored-model"
}

func ptrSupport(id contract.SupportCellID) *contract.SupportCellID { return &id }

func stringPointer(value string) *string { return &value }

func formatThree(value int) string {
	return fmt.Sprintf("%03d", value)
}

func cloneScenario(s Scenario) Scenario {
	data, err := json.Marshal(s)
	if err != nil {
		panic(fmt.Sprintf("clone scenario: %v", err))
	}
	var clone Scenario
	if err := json.Unmarshal(data, &clone); err != nil {
		panic(fmt.Sprintf("unmarshal cloned scenario: %v", err))
	}
	clone.sourcePath = s.sourcePath
	clone.sourceBytes = append([]byte(nil), s.sourceBytes...)
	clone.makeTargets = cloneStringSet(s.makeTargets)
	return clone
}

func validationMakeTargets() map[string]struct{} {
	return map[string]struct{}{
		"test-conformance":                   {},
		"test-blackbox":                      {},
		"test-swift":                         {},
		"test-native-swift-schema-queue":     {},
		"test-native-swift-steady-pull":      {},
		"test-native-swift-rebuild-requests": {},
		"test-kotlin":                        {},
		"test-rn-e2e-ios":                    {},
		"test-rn-e2e-android":                {},
	}
}

func cloneBundle(bundle *contract.Bundle) *contract.Bundle {
	clone := *bundle
	clone.Requirements.Requirements = append([]contract.Requirement(nil), bundle.Requirements.Requirements...)
	clone.Support.Cells = append([]contract.SupportCell(nil), bundle.Support.Cells...)
	clone.Faults.Faults = append([]contract.Fault(nil), bundle.Faults.Faults...)
	clone.Faults.Controls = append([]contract.Control(nil), bundle.Faults.Controls...)
	clone.Artifacts.Artifacts = append([]contract.ArtifactInventoryItem(nil), bundle.Artifacts.Artifacts...)
	clone.Performance.Budgets = append([]contract.PerformanceBudget(nil), bundle.Performance.Budgets...)
	clone.Performance.RequiredMeasurements = append([]contract.RequiredMeasurement(nil), bundle.Performance.RequiredMeasurements...)
	return &clone
}

func authoredTimeScenarioWithFaultInjection() Scenario {
	scenario := authoredTimeScenario()
	scenario.ProofTypes = append(scenario.ProofTypes, "fault-injection")
	plan := scenario.FaultPlans[0]
	supportID := contract.SupportCellID("SUP-PG-018")
	obligation := ProofObligation{ObligationID: "OBL-TIME-FI-001", RequirementIDs: []contract.RequirementID{"SYNC-TIME-001"}, AssertionIDs: []contract.AssertionID{"ASSERT-TIME-PG-001"}, ProofType: "fault-injection", SupportCellID: &supportID, ArtifactInventoryIDs: []contract.ArtifactInventoryID{"ARTDEF-PG-EXTENSION-001", "ARTDEF-ADAPTER-001"}, PerformanceBudgetIDs: []contract.BudgetID{}, RequiredMeasurementIDs: []contract.MeasurementID{}, RequiredVectorSetIDs: []contract.VectorSetID{}, MakeTarget: "test-blackbox", Argv: []string{"make", "test-blackbox"}, FaultPlanID: &plan.ID, ControlID: &plan.ControlID}
	scenario.ProofObligations = append(scenario.ProofObligations, obligation)
	scenario.Ownership = append(scenario.Ownership, Ownership{ScenarioID: scenario.ID, RequirementID: "SYNC-TIME-001", ProofObligationID: obligation.ObligationID, AssertionID: "ASSERT-TIME-PG-001", ProofType: obligation.ProofType, SupportCellID: obligation.SupportCellID})
	return scenario
}
