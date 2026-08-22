package nativeexecution

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestRunDispatchesByCapabilityInsteadOfActionIdentityOrPosition(t *testing.T) {
	actions := []scenarios.NativeAction{
		{
			ID:            "NACT-DISPATCH-INSTALL-901",
			Phase:         "setup",
			Actor:         "client",
			Command:       "open",
			CoversStepIDs: []scenarios.StepID{},
			Parameters:    json.RawMessage(`{"client_key":"client-secret","database_mode":"create","seed_step_id":null}`),
		},
		{
			ID:            "NACT-DISPATCH-OPEN-902",
			Phase:         "setup",
			Actor:         "controller",
			Command:       "install-model",
			CoversStepIDs: []scenarios.StepID{},
			Parameters:    json.RawMessage(`{}`),
		},
		{
			ID:            "NACT-DISPATCH-FIRST-903",
			Phase:         "exercise",
			Actor:         "client",
			Command:       "lifecycle",
			CoversStepIDs: []scenarios.StepID{},
			Parameters:    json.RawMessage(`{"client_key":"client-secret","operation":"start"}`),
		},
	}
	selection := testSelection(actions, nil, nil, nil)

	var dispatched []string
	executor := executorFunc(func(_ context.Context, request ExecuteRequest) (ActionResult, error) {
		dispatched = append(dispatched, request.Action.Actor+"/"+request.Action.Command)
		return ActionResult{}, nil
	})
	trace, err := Run(context.Background(), selection, executor)
	if err != nil {
		t.Fatalf("run native plan: %v", err)
	}
	want := []string{"client/open", "controller/install-model", "client/lifecycle"}
	if !reflect.DeepEqual(dispatched, want) {
		t.Fatalf("dispatched capabilities = %v, want %v", dispatched, want)
	}
	if trace.Outcome != OutcomePassed || len(trace.Actions) != len(actions) {
		t.Fatalf("trace = %+v", trace)
	}
}

func TestRunCentrallyFailsRawStepMismatch(t *testing.T) {
	step := scenarios.Step{
		ID:              "STEP-CENTRAL-001",
		Phase:           "exercise",
		Transport:       "http",
		Operation:       scenarios.Operation{ContractOperation: "connect", Name: "send", Payload: json.RawMessage(`{}`)},
		ExpectedOutcome: scenarios.ExpectedOutcome{Disposition: "success"},
	}
	action := scenarios.NativeAction{
		ID:            "NACT-CENTRAL-STEP-001",
		Phase:         "exercise",
		Actor:         "client",
		Command:       "synchronize-step",
		CoversStepIDs: []scenarios.StepID{step.ID},
		Parameters:    json.RawMessage(`{"client_key":"client-secret","method":"start","completion":"idle"}`),
	}
	wire := scenarios.WireExpectation{StepID: step.ID, HTTPStatus: 200}
	selection := testSelection([]scenarios.NativeAction{action}, []scenarios.Step{step}, []scenarios.WireExpectation{wire}, nil)
	executor := executorFunc(func(context.Context, ExecuteRequest) (ActionResult, error) {
		return ActionResult{
			StepObservations: []StepObservation{{
				StepID:      step.ID,
				Disposition: "success",
				Wire:        &WireObservation{HTTPStatus: 503, Retryable: true},
			}},
			Synchronization: &SynchronizationResult{Completion: "idle"},
		}, nil
	})

	trace, err := Run(context.Background(), selection, executor)
	if err == nil || !strings.Contains(err.Error(), "failed central evaluation") {
		t.Fatalf("central mismatch error = %v", err)
	}
	if trace.Outcome != OutcomeFailed || len(trace.Actions) != 1 || trace.Actions[0].Outcome != OutcomeFailed {
		t.Fatalf("central mismatch trace = %+v", trace)
	}
	if trace.Actions[0].StepObservations[0].Wire.HTTPStatus != 503 {
		t.Fatalf("trace lost bounded raw observation: %+v", trace.Actions[0])
	}
}

func TestRunRequiresAndCentrallyEvaluatesSynchronizationResult(t *testing.T) {
	step := scenarios.Step{
		ID:              "STEP-SYNCHRONIZATION-001",
		Phase:           "exercise",
		ExpectedOutcome: scenarios.ExpectedOutcome{Disposition: "success"},
	}
	action := scenarios.NativeAction{
		ID:            "NACT-SYNCHRONIZATION-001",
		Phase:         "exercise",
		Actor:         "client",
		Command:       "synchronize-step",
		CoversStepIDs: []scenarios.StepID{step.ID},
		Parameters:    json.RawMessage(`{"client_key":"client-secret","method":"start","completion":"idle"}`),
	}
	selection := testSelection([]scenarios.NativeAction{action}, []scenarios.Step{step}, nil, nil)
	stepObservation := []StepObservation{{StepID: step.ID, Disposition: "success"}}

	t.Run("missing", func(t *testing.T) {
		executor := executorFunc(func(context.Context, ExecuteRequest) (ActionResult, error) {
			return ActionResult{StepObservations: stepObservation}, nil
		})
		trace, err := Run(context.Background(), selection, executor)
		if err == nil || trace.Outcome != OutcomeError {
			t.Fatalf("missing synchronization result: trace=%+v err=%v", trace, err)
		}
	})

	t.Run("mismatch", func(t *testing.T) {
		executor := executorFunc(func(context.Context, ExecuteRequest) (ActionResult, error) {
			return ActionResult{
				StepObservations: stepObservation,
				Synchronization:  &SynchronizationResult{Completion: "blocked"},
			}, nil
		})
		trace, err := Run(context.Background(), selection, executor)
		if err == nil || trace.Outcome != OutcomeFailed || trace.Actions[0].Synchronization == nil || trace.Actions[0].Synchronization.Completion != "blocked" {
			t.Fatalf("synchronization mismatch: trace=%+v err=%v", trace, err)
		}
	})

	t.Run("match", func(t *testing.T) {
		executor := executorFunc(func(context.Context, ExecuteRequest) (ActionResult, error) {
			return ActionResult{
				StepObservations: stepObservation,
				Synchronization:  &SynchronizationResult{Completion: "idle"},
			}, nil
		})
		trace, err := Run(context.Background(), selection, executor)
		if err != nil || trace.Outcome != OutcomePassed {
			t.Fatalf("matching synchronization result: trace=%+v err=%v", trace, err)
		}
	})
}

func TestRunRejectsSynchronizationResultOnOtherCommand(t *testing.T) {
	action := scenarios.NativeAction{
		ID:            "NACT-SYNCHRONIZATION-UNBOUND-001",
		Phase:         "setup",
		Actor:         "client",
		Command:       "open",
		CoversStepIDs: []scenarios.StepID{},
		Parameters:    json.RawMessage(`{"client_key":"client-secret","database_mode":"create","seed_step_id":null}`),
	}
	executor := executorFunc(func(context.Context, ExecuteRequest) (ActionResult, error) {
		return ActionResult{Synchronization: &SynchronizationResult{Completion: "idle"}}, nil
	})
	trace, err := Run(context.Background(), testSelection([]scenarios.NativeAction{action}, nil, nil, nil), executor)
	if err == nil || trace.Outcome != OutcomeError {
		t.Fatalf("unbound synchronization result: trace=%+v err=%v", trace, err)
	}
}

func TestRunRejectsMissingExtraDuplicateAndUnboundStepFacts(t *testing.T) {
	steps := []scenarios.Step{
		{ID: "STEP-CLOSURE-001", Phase: "exercise", ExpectedOutcome: scenarios.ExpectedOutcome{Disposition: "success"}},
		{ID: "STEP-CLOSURE-002", Phase: "exercise", ExpectedOutcome: scenarios.ExpectedOutcome{Disposition: "success"}},
	}
	action := scenarios.NativeAction{
		ID:            "NACT-CLOSURE-001",
		Phase:         "exercise",
		Actor:         "client",
		Command:       "synchronize-step",
		CoversStepIDs: []scenarios.StepID{steps[0].ID, steps[1].ID},
		Parameters:    json.RawMessage(`{"client_key":"client-secret","method":"start","completion":"idle"}`),
	}
	selection := testSelection([]scenarios.NativeAction{action}, steps, nil, nil)
	tests := []struct {
		name         string
		observations []StepObservation
	}{
		{name: "missing", observations: []StepObservation{{StepID: steps[0].ID, Disposition: "success"}}},
		{name: "extra", observations: []StepObservation{{StepID: steps[0].ID, Disposition: "success"}, {StepID: steps[1].ID, Disposition: "success"}, {StepID: "STEP-EXTRA-001", Disposition: "success"}}},
		{name: "duplicate", observations: []StepObservation{{StepID: steps[0].ID, Disposition: "success"}, {StepID: steps[0].ID, Disposition: "success"}}},
		{name: "unbound", observations: []StepObservation{{StepID: steps[0].ID, Disposition: "success"}, {StepID: "STEP-OTHER-001", Disposition: "success"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := executorFunc(func(context.Context, ExecuteRequest) (ActionResult, error) {
				return ActionResult{StepObservations: test.observations}, nil
			})
			trace, err := Run(context.Background(), selection, executor)
			if err == nil {
				t.Fatal("accepted raw step closure defect")
			}
			if trace.Outcome != OutcomeError || len(trace.Actions) != 1 || trace.Actions[0].Outcome != OutcomeError {
				t.Fatalf("closure trace = %+v", trace)
			}
		})
	}
}

func TestRunComparesOnlyAuthoredStateFactProjection(t *testing.T) {
	wantCount := uint64(1)
	want := scenarios.StateFacts{
		RowCount: &wantCount,
		Rows:     []scenarios.RowFact{},
	}
	expectation := scenarios.ModelExpectation{
		ID: "EXPECT-CAPTURE-001",
		Predicate: scenarios.Predicate{
			ContractPredicate: "state-equality",
			Name:              "state-equals-authored-model",
			Payload:           json.RawMessage(`{}`),
		},
		StateFacts: &want,
	}
	action := scenarios.NativeAction{
		ID:            "NACT-CAPTURE-001",
		Phase:         "verify",
		Actor:         "observer",
		Command:       "capture",
		CoversStepIDs: []scenarios.StepID{},
		Parameters:    json.RawMessage(`{"client_keys":[],"sources":["server-state"],"expectation_ids":["EXPECT-CAPTURE-001"]}`),
	}
	selection := testSelection([]scenarios.NativeAction{action}, nil, nil, []scenarios.ModelExpectation{expectation})
	observedCount := uint64(1)
	observed := scenarios.StateFacts{
		RowCount: &observedCount,
		Clients: []scenarios.ClientDurabilityFact{{
			UserID:   "user-owned-secret",
			ClientID: "client-owned-secret",
		}},
	}
	executor := executorFunc(func(context.Context, ExecuteRequest) (ActionResult, error) {
		return ActionResult{CaptureObservation: &CaptureObservation{
			Sources:    []string{"server-state"},
			StateFacts: observed,
		}}, nil
	})

	trace, err := Run(context.Background(), selection, executor)
	if err != nil {
		t.Fatalf("compare partial state facts: %v", err)
	}
	if trace.Outcome != OutcomePassed || trace.Actions[0].ExpectationResults[0].Outcome != OutcomePassed {
		t.Fatalf("partial projection trace = %+v", trace)
	}
	encoded, err := json.Marshal(trace)
	if err != nil {
		t.Fatalf("marshal trace: %v", err)
	}
	if strings.Contains(string(encoded), "user-owned-secret") || strings.Contains(string(encoded), "client-owned-secret") {
		t.Fatalf("trace contains user-owned capture facts: %s", encoded)
	}
}

func TestRunCentrallyFailsAuthoredStateFactMismatch(t *testing.T) {
	wantCount := uint64(1)
	want := scenarios.StateFacts{RowCount: &wantCount}
	expectation := scenarios.ModelExpectation{
		ID:         "EXPECT-CAPTURE-FAIL-001",
		Predicate:  scenarios.Predicate{ContractPredicate: "state-equality", Name: "state-equals-authored-model", Payload: json.RawMessage(`{}`)},
		StateFacts: &want,
	}
	action := scenarios.NativeAction{
		ID:            "NACT-CAPTURE-FAIL-001",
		Phase:         "verify",
		Actor:         "observer",
		Command:       "capture",
		CoversStepIDs: []scenarios.StepID{},
		Parameters:    json.RawMessage(`{"client_keys":[],"sources":["server-state"],"expectation_ids":["EXPECT-CAPTURE-FAIL-001"]}`),
	}
	selection := testSelection([]scenarios.NativeAction{action}, nil, nil, []scenarios.ModelExpectation{expectation})
	observedCount := uint64(2)
	executor := executorFunc(func(context.Context, ExecuteRequest) (ActionResult, error) {
		return ActionResult{CaptureObservation: &CaptureObservation{
			Sources:    []string{"server-state"},
			StateFacts: scenarios.StateFacts{RowCount: &observedCount},
		}}, nil
	})

	trace, err := Run(context.Background(), selection, executor)
	if err == nil {
		t.Fatal("accepted mismatched authored state fact")
	}
	if trace.Outcome != OutcomeFailed || trace.Actions[0].ExpectationResults[0].Outcome != OutcomeFailed {
		t.Fatalf("state mismatch trace = %+v", trace)
	}
}

func TestRunRejectsDuplicateFactsOutsideAuthoredProjection(t *testing.T) {
	wantCount := uint64(0)
	want := scenarios.StateFacts{RowCount: &wantCount}
	expectation := scenarios.ModelExpectation{
		ID:         "EXPECT-DUPLICATE-001",
		Predicate:  scenarios.Predicate{ContractPredicate: "state-equality", Name: "state-equals-authored-model", Payload: json.RawMessage(`{}`)},
		StateFacts: &want,
	}
	action := scenarios.NativeAction{
		ID:            "NACT-DUPLICATE-001",
		Phase:         "verify",
		Actor:         "observer",
		Command:       "capture",
		CoversStepIDs: []scenarios.StepID{},
		Parameters:    json.RawMessage(`{"client_keys":[],"sources":["server-state"],"expectation_ids":["EXPECT-DUPLICATE-001"]}`),
	}
	selection := testSelection([]scenarios.NativeAction{action}, nil, nil, []scenarios.ModelExpectation{expectation})
	client := scenarios.ClientDurabilityFact{UserID: "user-owned-secret", ClientID: "client-owned-secret"}
	executor := executorFunc(func(context.Context, ExecuteRequest) (ActionResult, error) {
		return ActionResult{CaptureObservation: &CaptureObservation{
			Sources: []string{"server-state"},
			StateFacts: scenarios.StateFacts{
				RowCount: &wantCount,
				Clients:  []scenarios.ClientDurabilityFact{client, client},
			},
		}}, nil
	})

	trace, err := Run(context.Background(), selection, executor)
	if err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("duplicate fact error = %v", err)
	}
	if trace.Outcome != OutcomeError || trace.Actions[0].Outcome != OutcomeError {
		t.Fatalf("duplicate fact trace = %+v", trace)
	}
}

func TestRunRequiresProcessIdentityAndDatabaseContinuityEvidence(t *testing.T) {
	open := scenarios.NativeAction{
		ID:            "NACT-PROCESS-OPEN-001",
		Phase:         "setup",
		Actor:         "client",
		Command:       "open",
		CoversStepIDs: []scenarios.StepID{},
		Parameters:    json.RawMessage(`{"client_key":"client-secret","database_mode":"create","seed_step_id":null}`),
	}
	terminate := scenarios.NativeAction{
		ID:            "NACT-PROCESS-TERMINATE-002",
		Phase:         "exercise",
		Actor:         "process",
		Command:       "terminate",
		CoversStepIDs: []scenarios.StepID{},
		Parameters:    json.RawMessage(`{"client_key":"client-secret","boundary":"queue-inserted","after_action_id":"NACT-PROCESS-OPEN-001"}`),
	}
	relaunch := scenarios.NativeAction{
		ID:            "NACT-PROCESS-RELAUNCH-003",
		Phase:         "exercise",
		Actor:         "process",
		Command:       "relaunch",
		CoversStepIDs: []scenarios.StepID{},
		Parameters:    json.RawMessage(`{"client_key":"client-secret","boundary":"queue-inserted","after_action_id":"NACT-PROCESS-TERMINATE-002"}`),
	}
	selection := testSelection([]scenarios.NativeAction{open, terminate, relaunch}, nil, nil, nil)
	fingerprint := strings.Repeat("c", 64)
	currentProcess := "process-2"
	executor := executorFunc(func(_ context.Context, request ExecuteRequest) (ActionResult, error) {
		switch request.Action.Command {
		case "terminate":
			return ActionResult{ProcessBoundary: &ProcessBoundaryResult{
				ClientKey:                   "client-secret",
				Boundary:                    "queue-inserted",
				AfterActionID:               "NACT-PROCESS-OPEN-001",
				PriorProcessID:              "process-1",
				TerminationConfirmed:        true,
				DatabaseIdentityFingerprint: fingerprint,
			}}, nil
		case "relaunch":
			return ActionResult{ProcessBoundary: &ProcessBoundaryResult{
				ClientKey:                   "client-secret",
				Boundary:                    "queue-inserted",
				AfterActionID:               "NACT-PROCESS-TERMINATE-002",
				PriorProcessID:              "process-1",
				CurrentProcessID:            &currentProcess,
				DatabaseIdentityFingerprint: fingerprint,
			}}, nil
		default:
			return ActionResult{}, nil
		}
	})

	trace, err := Run(context.Background(), selection, executor)
	if err != nil || trace.Outcome != OutcomePassed {
		t.Fatalf("process continuity evidence: trace=%+v err=%v", trace, err)
	}
	if trace.Actions[1].ProcessBoundary == nil || trace.Actions[1].ProcessBoundary.ClientKey != "" || trace.Actions[2].ProcessBoundary == nil {
		t.Fatalf("bounded process trace = %+v", trace.Actions)
	}
}

func TestRunRejectsEchoOnlyAndDiscontinuousProcessEvidence(t *testing.T) {
	open := scenarios.NativeAction{
		ID:            "NACT-PROCESS-FAIL-OPEN-001",
		Phase:         "setup",
		Actor:         "client",
		Command:       "open",
		CoversStepIDs: []scenarios.StepID{},
		Parameters:    json.RawMessage(`{"client_key":"client-secret","database_mode":"create","seed_step_id":null}`),
	}
	terminate := scenarios.NativeAction{
		ID:            "NACT-PROCESS-FAIL-TERMINATE-002",
		Phase:         "exercise",
		Actor:         "process",
		Command:       "terminate",
		CoversStepIDs: []scenarios.StepID{},
		Parameters:    json.RawMessage(`{"client_key":"client-secret","boundary":"queue-inserted","after_action_id":"NACT-PROCESS-FAIL-OPEN-001"}`),
	}
	relaunch := scenarios.NativeAction{
		ID:            "NACT-PROCESS-FAIL-RELAUNCH-003",
		Phase:         "exercise",
		Actor:         "process",
		Command:       "relaunch",
		CoversStepIDs: []scenarios.StepID{},
		Parameters:    json.RawMessage(`{"client_key":"client-secret","boundary":"queue-inserted","after_action_id":"NACT-PROCESS-FAIL-TERMINATE-002"}`),
	}

	t.Run("echo only termination", func(t *testing.T) {
		selection := testSelection([]scenarios.NativeAction{open, terminate}, nil, nil, nil)
		executor := executorFunc(func(_ context.Context, request ExecuteRequest) (ActionResult, error) {
			if request.Action.Command == "terminate" {
				return ActionResult{ProcessBoundary: &ProcessBoundaryResult{
					ClientKey:     "client-secret",
					Boundary:      "queue-inserted",
					AfterActionID: "NACT-PROCESS-FAIL-OPEN-001",
				}}, nil
			}
			return ActionResult{}, nil
		})
		trace, err := Run(context.Background(), selection, executor)
		if err == nil || trace.Outcome != OutcomeError {
			t.Fatalf("echo-only termination: trace=%+v err=%v", trace, err)
		}
	})

	for _, test := range []struct {
		name                string
		currentProcess      string
		relaunchFingerprint string
	}{
		{name: "same process", currentProcess: "process-1", relaunchFingerprint: strings.Repeat("d", 64)},
		{name: "changed database", currentProcess: "process-2", relaunchFingerprint: strings.Repeat("e", 64)},
	} {
		t.Run(test.name, func(t *testing.T) {
			selection := testSelection([]scenarios.NativeAction{open, terminate, relaunch}, nil, nil, nil)
			terminationFingerprint := strings.Repeat("d", 64)
			executor := executorFunc(func(_ context.Context, request ExecuteRequest) (ActionResult, error) {
				switch request.Action.Command {
				case "terminate":
					return ActionResult{ProcessBoundary: &ProcessBoundaryResult{
						ClientKey:                   "client-secret",
						Boundary:                    "queue-inserted",
						AfterActionID:               "NACT-PROCESS-FAIL-OPEN-001",
						PriorProcessID:              "process-1",
						TerminationConfirmed:        true,
						DatabaseIdentityFingerprint: terminationFingerprint,
					}}, nil
				case "relaunch":
					currentProcess := test.currentProcess
					return ActionResult{ProcessBoundary: &ProcessBoundaryResult{
						ClientKey:                   "client-secret",
						Boundary:                    "queue-inserted",
						AfterActionID:               "NACT-PROCESS-FAIL-TERMINATE-002",
						PriorProcessID:              "process-1",
						CurrentProcessID:            &currentProcess,
						DatabaseIdentityFingerprint: test.relaunchFingerprint,
					}}, nil
				default:
					return ActionResult{}, nil
				}
			})
			trace, err := Run(context.Background(), selection, executor)
			if err == nil || trace.Outcome != OutcomeError {
				t.Fatalf("discontinuous process evidence: trace=%+v err=%v", trace, err)
			}
		})
	}
}

func TestRawResultTypesCannotCarryVerdicts(t *testing.T) {
	for _, value := range []any{ActionResult{}, StepObservation{}, SynchronizationResult{}, CaptureObservation{}} {
		typeOf := reflect.TypeOf(value)
		for _, forbidden := range []string{"Outcome", "ExpectationResults"} {
			if _, found := typeOf.FieldByName(forbidden); found {
				t.Fatalf("%s exposes adapter verdict field %s", typeOf.Name(), forbidden)
			}
		}
	}
}

func TestRunHandlesManifestErrorBeforeTraceBinding(t *testing.T) {
	trace, err := Run(context.Background(), Selection{}, executorFunc(func(context.Context, ExecuteRequest) (ActionResult, error) {
		return ActionResult{}, nil
	}))
	if err == nil {
		t.Fatal("accepted invalid selection")
	}
	if trace.SchemaVersion != traceSchemaVersion || trace.Outcome != OutcomeError || trace.ScenarioID != "" || trace.ScenarioSHA256 != "" || trace.ObligationID != "" || trace.SupportCellID != "" {
		t.Fatalf("manifest error trace = %+v", trace)
	}
}

func TestRunBoundsExecutorErrorsAndPreservesCause(t *testing.T) {
	cause := errors.New("user-owned-secret")
	executor := executorFunc(func(context.Context, ExecuteRequest) (ActionResult, error) {
		return ActionResult{}, cause
	})
	action := scenarios.NativeAction{
		ID:            "NACT-ERROR-001",
		Phase:         "setup",
		Actor:         "client",
		Command:       "open",
		CoversStepIDs: []scenarios.StepID{},
		Parameters:    json.RawMessage(`{"client_key":"client-secret","database_mode":"create","seed_step_id":null}`),
	}
	_, err := Run(context.Background(), testSelection([]scenarios.NativeAction{action}, nil, nil, nil), executor)
	if err == nil || !errors.Is(err, cause) {
		t.Fatalf("executor cause = %v", err)
	}
	if strings.Contains(err.Error(), "user-owned-secret") {
		t.Fatalf("executor error exposed cause: %v", err)
	}
}

type executorFunc func(context.Context, ExecuteRequest) (ActionResult, error)

func (function executorFunc) Execute(ctx context.Context, request ExecuteRequest) (ActionResult, error) {
	return function(ctx, request)
}

func testSelection(actions []scenarios.NativeAction, steps []scenarios.Step, wire []scenarios.WireExpectation, expectations []scenarios.ModelExpectation) Selection {
	supportCell := contract.SupportCellID("SUP-IOS-MIN-001")
	return Selection{
		scenario: scenarios.Scenario{
			ID:               "SCN-NATIVE-CENTRAL-001",
			Model:            scenarios.ModelSpec{Setup: []scenarios.Operation{{ContractOperation: "model", Name: "install-current-contract", Payload: json.RawMessage(`{}`)}}, ExpectedState: expectations},
			Steps:            steps,
			WireExpectations: wire,
			NativeExecution: &scenarios.NativeExecutionPlan{
				Version: 1,
				Clients: []scenarios.NativeClient{{
					Key:         "client-secret",
					UserID:      "user-owned-secret",
					ClientID:    "client-owned-secret",
					DatabaseKey: "database-secret",
				}},
				Actions: actions,
			},
		},
		obligation: scenarios.ProofObligation{
			ObligationID:  "OBL-NATIVE-CENTRAL-001",
			ProofType:     "native-e2e",
			SupportCellID: &supportCell,
			MakeTarget:    "test-swift",
		},
		supportCellID:            string(supportCell),
		component:                "swift-client",
		platform:                 "ios",
		digest:                   strings.Repeat("a", 64),
		performanceCatalogSHA256: strings.Repeat("b", 64),
	}
}
