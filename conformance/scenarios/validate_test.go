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

func TestValidateNativeStepBindingsGroupOnePublicCall(t *testing.T) {
	bundle, err := contract.Load(context.Background(), "../../")
	if err != nil {
		t.Fatalf("load authored contract: %v", err)
	}
	base := groupedBoundNativeTimeScenario()
	if err := Validate(base, bundle); err != nil {
		t.Fatalf("validate grouped native step bindings: %v", err)
	}

	tests := []struct {
		name     string
		mutate   func(*Scenario)
		category string
	}{
		{"missing binding", func(s *Scenario) {
			s.Steps[1].NativeBinding = nil
		}, "native bindings on every step"},
		{"missing identity aliases", func(s *Scenario) {
			s.NativeIdentityAliases = nil
		}, "require native identity aliases"},
		{"unknown identity kind", func(s *Scenario) {
			s.NativeIdentityAliases[0].Kind = "unknown"
		}, "unknown native identity kind"},
		{"unbound identity alias", func(s *Scenario) {
			s.NativeIdentityAliases[0].StepIDs = nil
			s.NativeIdentityAliases[0].ExpectationIDs = nil
		}, "must bind at least one step or expectation"},
		{"unsafe identity integer", func(s *Scenario) {
			s.NativeIdentityAliases[0].Value = json.RawMessage(`9007199254740992`)
		}, "exact JSON range"},
		{"collapsed identity aliases", func(s *Scenario) {
			s.NativeIdentityAliases = append(s.NativeIdentityAliases, NativeIdentityAlias{Kind: "scope", Alias: "scope-b", Value: json.RawMessage(`"scope-a"`), StepIDs: []StepID{"STEP-TIME-001"}})
		}, "share one authored value"},
		{"unknown kind", func(s *Scenario) {
			s.Steps[0].NativeBinding.Kind = "unknown"
		}, "unknown native binding kind"},
		{"wrong transport", func(s *Scenario) {
			s.Steps[0].NativeBinding.Kind = "artifact"
		}, "cannot own transport"},
		{"client mismatch", func(s *Scenario) {
			s.Steps[0].NativeBinding.ClientID = "client-b"
		}, "client identity does not match"},
		{"inconsistent call", func(s *Scenario) {
			s.Steps[1].NativeBinding.ClientID = "client-b"
		}, "inconsistent client, method, completion, or phase"},
		{"synchronous call crosses phase", func(s *Scenario) {
			s.Steps[1].Phase = "setup"
		}, "synchronous native call"},
		{"terminal completion mismatch", func(s *Scenario) {
			s.Steps[0].NativeBinding.Completion = "blocked"
			s.Steps[1].NativeBinding.Completion = "blocked"
		}, "does not match terminal step"},
		{"unsupported action completion mismatch", func(s *Scenario) {
			s.Steps[1].Operation = Operation{ContractOperation: "connect", Name: "send", Payload: json.RawMessage(`{"user_id":"user-a","client_id":"client-a","runtime_version":3,"protocol_version":3,"schema_reset":false,"schema":{"version":1,"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"scope_set_version":1,"known_scopes":[]}`)}
			s.WireExpectations[1].ContractCase = "connect_success"
			s.WireExpectations[1].Action = "unsupported"
		}, "does not match terminal step"},
		{"action requires connect success", func(s *Scenario) {
			s.WireExpectations[1].Action = "unsupported"
		}, "requires a connect_success connect outcome"},
		{"terminal local failure", func(s *Scenario) {
			errorCode := "source_transaction_poison_blocked"
			s.Steps[1].Transport = "local"
			s.Steps[1].Operation = Operation{ContractOperation: "local", Name: "apply-pull-page", Payload: json.RawMessage(`{"user_id":"user-a","client_id":"client-a","source_step_id":"STEP-TIME-001"}`)}
			s.Steps[1].ExpectedOutcome = ExpectedOutcome{Disposition: "error", ErrorCode: &errorCode}
			s.WireExpectations = s.WireExpectations[:1]
		}, "does not match terminal step"},
		{"effect after terminal response", func(s *Scenario) {
			errorCode := "temporary_unavailable"
			s.WireExpectations[0].ContractCase = "temporary_unavailable"
			s.WireExpectations[0].HTTPStatus = 503
			s.WireExpectations[0].ErrorCode = &errorCode
			s.WireExpectations[0].Retryable = true
			s.Steps[0].NativeBinding.Completion = "blocked"
			s.Steps[1].NativeBinding.Completion = "blocked"
		}, "after terminal step"},
		{"controller splits public call", func(s *Scenario) {
			controller := Step{
				ID:              "STEP-TIME-CONTROLLER-001",
				Phase:           "exercise",
				Transport:       "model",
				NativeBinding:   &NativeStepBinding{Kind: "controller"},
				Operation:       Operation{ContractOperation: "model", Name: "set-client-assignments", Payload: json.RawMessage(`{"user_id":"user-a","client_id":"client-a","assignments":[]}`)},
				ExpectedOutcome: ExpectedOutcome{Disposition: "success"},
			}
			s.Steps = append(s.Steps[:1], append([]Step{controller}, s.Steps[1:]...)...)
		}, "resumes after another call or binding"},
		{"server process as client lifecycle", func(s *Scenario) {
			s.Steps[0].Transport = "process"
			s.Steps[0].Operation = Operation{ContractOperation: "process", Name: "materialize-source-transaction", Payload: json.RawMessage(`{"stream_generation":"stream-1","commit_lsn":"1"}`)}
			s.Steps[0].NativeBinding = &NativeStepBinding{Kind: "process", UserID: "user-a", ClientID: "client-a"}
			s.WireExpectations = s.WireExpectations[1:]
		}, "cannot own operation"},
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

func TestValidateNativeStepBindingsRejectResumedCallAndWorkloadMacro(t *testing.T) {
	bundle, err := contract.Load(context.Background(), "../../")
	if err != nil {
		t.Fatalf("load authored contract: %v", err)
	}

	resumed := groupedBoundNativeTimeScenario()
	secondCall := NativeCallID("time_second")
	resumed.Steps[1].NativeBinding.CallID = &secondCall
	third := resumed.Steps[0]
	third.ID = "STEP-TIME-003"
	thirdCall := NativeCallID("time_sync")
	third.NativeBinding = &NativeStepBinding{Kind: "public-call", UserID: "user-a", ClientID: "client-a", CallID: &thirdCall, Stage: "synchronous", Method: "start", Completion: "idle"}
	resumed.Steps = append(resumed.Steps, third)
	thirdWire := resumed.WireExpectations[0]
	thirdWire.StepID = third.ID
	resumed.WireExpectations = append(resumed.WireExpectations, thirdWire)
	if err := requireErrorCategory(Validate(resumed, bundle), "resumes after another call"); err != nil {
		t.Fatal(err)
	}

	workload := authoredTimeScenario()
	workload.Steps[0].Transport = "model"
	workload.Steps[0].Operation = Operation{ContractOperation: "workload", Name: "prepare", Payload: json.RawMessage(`{}`)}
	workload.Steps[0].NativeBinding = &NativeStepBinding{Kind: "controller"}
	workload.WireExpectations = nil
	if err := requireErrorCategory(Validate(workload, bundle), "cannot execute a workload macro"); err != nil {
		t.Fatal(err)
	}
}

func TestValidateNativeWorkloadBindingRejectsBoundAndNondeterministicParameters(t *testing.T) {
	bundle, err := contract.Load(context.Background(), "../../")
	if err != nil {
		t.Fatalf("load authored contract: %v", err)
	}
	base := boundNativeWorkloadScenario()
	if err := Validate(base, bundle); err != nil {
		t.Fatalf("validate native workload binding: %v", err)
	}

	tests := []struct {
		name     string
		mutate   func(*Scenario)
		category string
	}{
		{"over-bound record count", func(s *Scenario) {
			s.Steps[0].NativeBinding.Workload.RecordCount = maxNativeWorkloadRecords + 1
		}, "record_count must be between"},
		{"nondeterministic seed", func(s *Scenario) {
			s.Steps[0].NativeBinding.Workload.Seed = 0
		}, "seed must be nonzero and deterministic"},
		{"inexact JSON seed", func(s *Scenario) {
			s.Steps[0].NativeBinding.Workload.Seed = maxNativeWorkloadSeed + 1
		}, "seed must be nonzero and deterministic"},
		{"generated row alias", func(s *Scenario) {
			s.NativeIdentityAliases[0].StepIDs = []StepID{s.Steps[0].ID}
			s.NativeIdentityAliases[0].ExpectationIDs = nil
		}, "must not bind generated workload step"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := cloneScenario(base)
			test.mutate(&scenario)
			if err := requireErrorCategory(Validate(scenario, bundle), test.category); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNativeWorkloadSchemaRejectsOutOfBoundAndNondeterministicParameters(t *testing.T) {
	base, err := LoadFile(
		context.Background(),
		"../../",
		"conformance/scenarios/performance/queue-replay-001.json",
	)
	if err != nil {
		t.Fatalf("load schema-valid workload source: %v", err)
	}
	workloadSource := boundNativeWorkloadScenario()
	base.NativeIdentityAliases = workloadSource.NativeIdentityAliases
	base.NativeIdentityAliases[0].StepIDs = []StepID{}
	for index := range base.Steps {
		base.Steps[index].NativeBinding = &NativeStepBinding{Kind: "controller"}
	}
	base.Steps[0].NativeBinding = workloadSource.Steps[0].NativeBinding
	encode := func(s Scenario) []byte {
		t.Helper()
		data, err := json.Marshal(s)
		if err != nil {
			t.Fatalf("encode native workload scenario: %v", err)
		}
		return data
	}
	path := "conformance/scenarios/testing/native-workload.json"
	if _, err := LoadBytes(context.Background(), "../../", path, encode(base)); err != nil {
		t.Fatalf("load schema-valid native workload: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*Scenario)
	}{
		{"over-bound count", func(s *Scenario) { s.Steps[0].NativeBinding.Workload.RecordCount = maxNativeWorkloadRecords + 1 }},
		{"nondeterministic seed", func(s *Scenario) { s.Steps[0].NativeBinding.Workload.Seed = 0 }},
		{"inexact JSON seed", func(s *Scenario) { s.Steps[0].NativeBinding.Workload.Seed = maxNativeWorkloadSeed + 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := cloneScenario(base)
			test.mutate(&scenario)
			if _, err := LoadBytes(context.Background(), "../../", path, encode(scenario)); err == nil {
				t.Fatal("schema accepted invalid native workload parameters")
			}
		})
	}
}

func TestValidateNativeStepBindingsPermitStagedCallAroundControllerOperations(t *testing.T) {
	bundle, err := contract.Load(context.Background(), "../../")
	if err != nil {
		t.Fatalf("load authored contract: %v", err)
	}
	base := groupedBoundNativeTimeScenario()
	base.Steps[0].Phase = "setup"
	base.Steps[0].NativeBinding.Stage = "begin"
	base.Steps[0].NativeBinding.Completion = ""
	base.Steps[1].NativeBinding.Stage = "await-call"
	base.Steps[1].NativeBinding.Method = ""
	controller := Step{
		ID:              "STEP-TIME-CONTROLLER-001",
		Phase:           "setup",
		Transport:       "model",
		NativeBinding:   &NativeStepBinding{Kind: "controller"},
		Operation:       Operation{ContractOperation: "model", Name: "set-client-assignments", Payload: json.RawMessage(`{"user_id":"user-a","client_id":"client-a","assignments":[]}`)},
		ExpectedOutcome: ExpectedOutcome{Disposition: "success"},
	}
	base.Steps = append(base.Steps[:1], append([]Step{controller}, base.Steps[1:]...)...)
	if err := Validate(base, bundle); err != nil {
		t.Fatalf("validate noncontiguous staged call: %v", err)
	}
	continued := cloneScenario(base)
	errorCode := "temporary_unavailable"
	continued.WireExpectations[0].ContractCase = errorCode
	continued.WireExpectations[0].HTTPStatus = 503
	continued.WireExpectations[0].ErrorCode = &errorCode
	continued.WireExpectations[0].Retryable = true
	if err := Validate(continued, bundle); err != nil {
		t.Fatalf("validate staged call after intermediate response: %v", err)
	}

	tests := []struct {
		name     string
		mutate   func(*Scenario)
		category string
	}{
		{"duplicate begin", func(s *Scenario) {
			s.Steps[2].NativeBinding.Stage = "begin"
			s.Steps[2].NativeBinding.Method = "start"
			s.Steps[2].NativeBinding.Completion = ""
		}, "terminal await-call"},
		{"missing begin", func(s *Scenario) {
			s.Steps[0].NativeBinding.Stage = "await-step"
			s.Steps[0].NativeBinding.Method = ""
		}, "must begin"},
		{"binding interrupts active call", func(s *Scenario) {
			s.Steps[1].NativeBinding = &NativeStepBinding{Kind: "local-write", UserID: "user-a", ClientID: "client-a"}
			s.Steps[1].Transport = "local"
			s.Steps[1].Operation = Operation{ContractOperation: "local", Name: "write", Payload: json.RawMessage(`{"authenticated_user_id":"user-a","client_id":"client-a","mutation_id":"00000000-0000-4000-8000-000000000001","table_id":"items","pk":{"id":"a"},"authored_schema":{"version":1,"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"operation":"insert","columns":[],"client_version":"2024-01-01T00:00:00.000000Z"}`)}
		}, "interrupted by binding"},
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

func TestValidateNativeControllerBindingOwnsRawHTTP(t *testing.T) {
	bundle, err := contract.Load(context.Background(), "../../")
	if err != nil {
		t.Fatalf("load authored contract: %v", err)
	}
	scenario := authoredTimeScenario()
	scenario.Steps[0].NativeBinding = &NativeStepBinding{Kind: "controller"}
	if err := Validate(scenario, bundle); err != nil {
		t.Fatalf("validate controller-owned HTTP step: %v", err)
	}

	scenario.Steps[0].NativeBinding.UserID = "user-a"
	if err := requireErrorCategory(Validate(scenario, bundle), "must not contain client call fields"); err != nil {
		t.Fatal(err)
	}
}

func TestValidateNativeLifecycleBoundaryFollowsTerminalPublicCallStep(t *testing.T) {
	bundle, err := contract.Load(context.Background(), "../../")
	if err != nil {
		t.Fatalf("load authored contract: %v", err)
	}
	base := groupedBoundNativeTimeScenario()
	base.NativeLifecycleBoundaries = []NativeLifecycleBoundary{{
		ID:          "time_stop",
		Phase:       "exercise",
		AfterStepID: "STEP-TIME-002",
		UserID:      "user-a",
		ClientID:    "client-a",
		Method:      "stop",
	}}
	if err := Validate(base, bundle); err != nil {
		t.Fatalf("validate native lifecycle boundary: %v", err)
	}

	mutant := cloneScenario(base)
	mutant.NativeLifecycleBoundaries[0].AfterStepID = "STEP-TIME-001"
	if err := requireErrorCategory(Validate(mutant, bundle), "must follow the terminal step"); err != nil {
		t.Fatal(err)
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
			s.ProofObligations = append(s.ProofObligations[:2], s.ProofObligations[3:]...)
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
		{"incomplete optional PostgreSQL fault-injection architecture coverage", authoredTimeScenarioWithFaultInjection, func(s *Scenario) {
			duplicate := s.ProofObligations[len(s.ProofObligations)-1]
			duplicate.ObligationID = "OBL-TIME-FI-002"
			s.ProofObligations = append(s.ProofObligations, duplicate)
			s.Ownership = append(s.Ownership, Ownership{ScenarioID: s.ID, RequirementID: "SYNC-TIME-001", ProofObligationID: duplicate.ObligationID, AssertionID: duplicate.AssertionIDs[0], ProofType: duplicate.ProofType, SupportCellID: duplicate.SupportCellID})
		}, "required extension architecture cells"},
		{"non-singleton fault requirement", authoredTimeScenarioWithFaultInjection, func(s *Scenario) {
			index := len(s.ProofObligations) - 1
			s.ProofObligations[index].RequirementIDs = append(s.ProofObligations[index].RequirementIDs, "SYNC-CURSOR-001")
		}, "fault-injection must own exactly one requirement"},
		{"non-singleton fault assertion", authoredTimeScenarioWithFaultInjection, func(s *Scenario) {
			index := len(s.ProofObligations) - 1
			s.ProofObligations[index].AssertionIDs = append(s.ProofObligations[index].AssertionIDs, "ASSERT-TIME-PG-001")
		}, "fault-injection must own exactly one assertion"},
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
	base.Steps[0].NativeBinding.Completion = "blocked"
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

func TestValidateWireFaultRequiresMatchingTemporaryUnavailableExpectation(t *testing.T) {
	bundle, err := contract.Load(context.Background(), "../../")
	if err != nil {
		t.Fatalf("load authored contract: %v", err)
	}
	scenario, err := LoadFile(context.Background(), "../../", "conformance/scenarios/server/retention-reconnect-001.json")
	if err != nil {
		t.Fatalf("load retention reconnect scenario: %v", err)
	}
	if err := Validate(scenario, bundle); err != nil {
		t.Fatalf("validate retention reconnect wire fault: %v", err)
	}
	mutant := cloneScenario(scenario)
	mutant.WireExpectations[0].ContractCase = "push_success"
	mutant.WireExpectations[0].HTTPStatus = 200
	mutant.WireExpectations[0].ErrorCode = nil
	mutant.WireExpectations[0].Retryable = false
	if err := requireErrorCategory(Validate(mutant, bundle), "wire fault requires temporary_unavailable"); err != nil {
		t.Fatal(err)
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
	budget.SupportCellIDs = []contract.SupportCellID{"SUP-PG-LINUX-X64-001"}
	budget.ArtifactInventoryIDs = []contract.ArtifactInventoryID{"ARTDEF-PG-EXTENSION-001", "ARTDEF-ADAPTER-001"}
	measurement := bundle.Performance.RequiredMeasurements[0]
	measurement.ID = "MEAS-TIME-001"
	measurement.ScenarioID = base.ID
	measurement.SupportCellIDs = []contract.SupportCellID{"SUP-PG-LINUX-X64-001"}
	measurement.ArtifactInventoryIDs = []contract.ArtifactInventoryID{"ARTDEF-PG-EXTENSION-001", "ARTDEF-ADAPTER-001"}
	measurement.Strata = measurement.Strata[:1]
	measurement.MinimumSampleCountPerStratum = json.Number("1")
	positiveBundle.Performance.Budgets = []contract.PerformanceBudget{budget}
	positiveBundle.Performance.RequiredMeasurements = []contract.RequiredMeasurement{measurement}
	positiveScenario := cloneScenario(base)
	performanceAssertion := Assertion{ID: "ASSERT-TIME-PERF-001", RequirementIDs: []contract.RequirementID{"SYNC-TIME-001"}, Description: "The performance contract is satisfied.", ExpectationIDs: []ExpectationID{"EXPECT-TIME-001"}, Predicate: Predicate{ContractPredicate: "performance-measurement", Name: "performance-contract-satisfied", Payload: []byte(`{}`)}, Oracle: Oracle{Kind: "performance-budget", ExpectedSource: "authored-model", ObservedSource: "system-under-test"}, DetectsControlIDs: []contract.ControlID{}}
	positiveScenario.Assertions = append(positiveScenario.Assertions, performanceAssertion)
	positiveScenario.ProofObligations[0].AssertionIDs = append(positiveScenario.ProofObligations[0].AssertionIDs, performanceAssertion.ID)
	positiveScenario.ProofObligations[0].PerformanceBudgetIDs = []contract.BudgetID{budget.ID}
	positiveScenario.ProofObligations[0].RequiredMeasurementIDs = []contract.MeasurementID{measurement.ID}
	positiveScenario.Ownership = append(positiveScenario.Ownership, Ownership{ScenarioID: positiveScenario.ID, RequirementID: "SYNC-TIME-001", ProofObligationID: positiveScenario.ProofObligations[0].ObligationID, AssertionID: performanceAssertion.ID, ProofType: positiveScenario.ProofObligations[0].ProofType, SupportCellID: positiveScenario.ProofObligations[0].SupportCellID})
	parameters := append(json.RawMessage(nil), measurement.Strata[0].Parameters...)
	positiveScenario.Steps[0].MeasurementSample = &MeasurementSample{
		MeasurementID: measurement.ID,
		StratumID:     measurement.Strata[0].StratumID,
		SampleID:      "SAMPLE-TIME-001",
		Parameters:    parameters,
		Operation: MeasurementOperationTarget{
			ID:       "MOP-TIME-001",
			Family:   "time",
			Boundary: "single",
			Value:    append(json.RawMessage(nil), parameters...),
		},
	}
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
	callID := NativeCallID("time_sync")
	clientCells := []struct {
		id     contract.SupportCellID
		target string
	}{
		{"SUP-MACOS-CURRENT-001", "test-swift"},
		{"SUP-ANDROID-CURRENT-001", "test-kotlin"},
		{"SUP-RN-IOS-CURRENT-001", "test-rn-e2e-ios"},
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
			NativeBinding:   &NativeStepBinding{Kind: "public-call", UserID: "user-a", ClientID: "client-a", CallID: &callID, Stage: "synchronous", Method: "start", Completion: "idle"},
			Operation:       Operation{ContractOperation: "pull", Name: "request-page", Payload: []byte(`{"user_id":"user-a","client_id":"client-a","client_generation":1,"schema":{"version":1,"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"scope_set_version":1,"scopes":[],"limit":1}`)},
			ExpectedOutcome: ExpectedOutcome{Disposition: "success"},
		}},
		NativeIdentityAliases: []NativeIdentityAlias{{Kind: "scope", Alias: "scope-a", Value: json.RawMessage(`"scope-a"`), StepIDs: []StepID{"STEP-TIME-001"}}},
		makeTargets:           validationMakeTargets(),
	}

	assertionIDs := make([]contract.AssertionID, 0, 9)
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
	addObligation("OBL-TIME-PG-LINUX-X64-001", "server-black-box", ptrSupport("SUP-PG-LINUX-X64-001"), "test-blackbox", []contract.ArtifactInventoryID{"ARTDEF-PG-EXTENSION-001", "ARTDEF-ADAPTER-001"}, assertionIDs[0], nil, nil)
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

func boundNativeWorkloadScenario() Scenario {
	scenario := authoredTimeScenario()
	workload := &NativeWorkloadParameters{
		RecordCount:    2,
		BatchSize:      2,
		Seed:           101,
		AuthoredSchema: SchemaFact{Version: 1, Hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		ClientVersion:  "2026-08-11T00:00:00.000000Z",
		Targets: []NativeWorkloadTarget{{
			ScopeID:           "scope-a",
			TableID:           "items",
			PrimaryKeyFieldID: "id",
		}},
		MutationKinds: []NativeWorkloadMutationKind{
			{Operation: "insert", Count: 1, FieldIDs: []string{"value"}},
			{Operation: "insert", Count: 1, FieldIDs: []string{"obsolete_value"}},
		},
		Expectation: NativeWorkloadExpectation{
			OperationCount:  2,
			BatchCount:      1,
			OperationDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			PerScopeCardinalities: []NativeWorkloadScopeCardinality{{
				ScopeID:     "scope-a",
				Cardinality: 2,
			}},
		},
	}
	scenario.Steps[0].Transport = "model"
	scenario.Steps[0].Operation = Operation{ContractOperation: "workload", Name: "prepare", Payload: json.RawMessage(`{"profile":"pending_mutations","user_id":"user-a","client_id":"client-a","table_id":"items","accepted_count":1,"rejected_count":1}`)}
	scenario.Steps[0].NativeBinding = &NativeStepBinding{Kind: "workload", UserID: "user-a", ClientID: "client-a", Workload: workload}
	scenario.WireExpectations = nil
	scenario.NativeIdentityAliases = []NativeIdentityAlias{{
		Kind:           "schema",
		Alias:          "schema-a",
		Value:          json.RawMessage(`{"version":1,"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`),
		ExpectationIDs: []ExpectationID{"EXPECT-TIME-001"},
	}}
	return scenario
}

func groupedBoundNativeTimeScenario() Scenario {
	scenario := authoredTimeScenario()
	second := scenario.Steps[0]
	second.ID = "STEP-TIME-002"
	callID := *scenario.Steps[0].NativeBinding.CallID
	second.NativeBinding = &NativeStepBinding{
		Kind:       "public-call",
		UserID:     "user-a",
		ClientID:   "client-a",
		CallID:     &callID,
		Stage:      "synchronous",
		Method:     "start",
		Completion: "idle",
	}
	scenario.Steps = append(scenario.Steps, second)
	secondWire := scenario.WireExpectations[0]
	secondWire.StepID = second.ID
	scenario.WireExpectations = append(scenario.WireExpectations, secondWire)
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
		"test-conformance":    {},
		"test-blackbox":       {},
		"test-swift":          {},
		"test-kotlin":         {},
		"test-rn-e2e-ios":     {},
		"test-rn-e2e-android": {},
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
	for _, fixture := range []struct {
		obligationID contract.ObligationID
		supportID    contract.SupportCellID
	}{
		{"OBL-TIME-FI-LINUX-X64-001", "SUP-PG-LINUX-X64-001"},
	} {
		supportID := fixture.supportID
		obligation := ProofObligation{ObligationID: fixture.obligationID, RequirementIDs: []contract.RequirementID{"SYNC-TIME-001"}, AssertionIDs: []contract.AssertionID{"ASSERT-TIME-PG-001"}, ProofType: "fault-injection", SupportCellID: &supportID, ArtifactInventoryIDs: []contract.ArtifactInventoryID{"ARTDEF-PG-EXTENSION-001", "ARTDEF-ADAPTER-001"}, PerformanceBudgetIDs: []contract.BudgetID{}, RequiredMeasurementIDs: []contract.MeasurementID{}, RequiredVectorSetIDs: []contract.VectorSetID{}, MakeTarget: "test-blackbox", Argv: []string{"make", "test-blackbox"}, FaultPlanID: &plan.ID, ControlID: &plan.ControlID}
		scenario.ProofObligations = append(scenario.ProofObligations, obligation)
		scenario.Ownership = append(scenario.Ownership, Ownership{ScenarioID: scenario.ID, RequirementID: "SYNC-TIME-001", ProofObligationID: obligation.ObligationID, AssertionID: "ASSERT-TIME-PG-001", ProofType: obligation.ProofType, SupportCellID: obligation.SupportCellID})
	}
	return scenario
}
