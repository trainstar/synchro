package swift

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestPushResponseLossBindingsFollowAuthoredWireCompletions(t *testing.T) {
	scenario := loadPushResponseLossScenario(t)
	steps, err := swiftScenarioStepMap(scenario, pushResponseLossScenarioID, 6)
	if err != nil {
		t.Fatalf("map push-response-loss scenario: %v", err)
	}
	client := Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "push-response-loss-client-a"}
	if err := validatePushResponseLossBindings(scenario, steps, client); err != nil {
		t.Fatalf("validate push-response-loss bindings: %v", err)
	}
	initial, err := pushResponseLossWireExpectation(scenario, "STEP-PUSH-RESPONSE-LOSS-002")
	if err != nil {
		t.Fatalf("read initial wire expectation: %v", err)
	}
	if got := pushResponseLossNativeCompletion(initial); got != "blocked" {
		t.Fatalf("initial completion = %q, want blocked", got)
	}
	final, err := pushResponseLossWireExpectation(scenario, "STEP-PUSH-RESPONSE-LOSS-004")
	if err != nil {
		t.Fatalf("read final wire expectation: %v", err)
	}
	if got := pushResponseLossNativeCompletion(final); got != steps["STEP-PUSH-RESPONSE-LOSS-004"].NativeBinding.Completion {
		t.Fatalf("final completion = %q, want %q", got, steps["STEP-PUSH-RESPONSE-LOSS-004"].NativeBinding.Completion)
	}
	if got := steps["STEP-PUSH-RESPONSE-LOSS-003"].Operation.Name; got != "restart-client" {
		t.Fatalf("response-loss process operation = %q, want restart-client", got)
	}
}

func TestPushResponseLossTerminalStateRejectsRetryableContinuation(t *testing.T) {
	status := "error"
	state := runnerResult{Status: &status, Failure: &runnerFailure{Operation: "pushing", Code: "idempotency_conflict", Retryable: false, RecoveryAction: "none"}}
	if err := validatePushResponseLossTerminalState(state); err != nil {
		t.Fatalf("validate terminal response-loss state: %v", err)
	}
	state.Failure.Retryable = true
	if err := validatePushResponseLossTerminalState(state); err == nil {
		t.Fatal("retryable response-loss failure was accepted as terminal")
	}
}

func TestPushResponseLossRetryStageBindsPushAfterColdStartConnect(t *testing.T) {
	call := SynchronizationResult{
		Completion: "blocked",
		transportObservations: []transportObservation{
			{OperationClass: "connect", StatusCode: 200},
			{OperationClass: "push", StatusCode: 429, ErrorCode: pointerString("retry_later"), Retryable: true},
		},
	}
	if err := validatePushResponseLossRetryStage(call, 429, "retry_later"); err != nil {
		t.Fatalf("validate cold-start retry stage: %v", err)
	}
	call.transportObservations = append(call.transportObservations, transportObservation{OperationClass: "pull", StatusCode: 200})
	if err := validatePushResponseLossRetryStage(call, 429, "retry_later"); err == nil {
		t.Fatal("retry stage accepted an extra transport operation")
	}
}

func TestPushResponseLossDurableComparisonDetectsDrift(t *testing.T) {
	count := 1
	status := "backoff"
	before := runnerResult{Status: &status, MutationLedgerCount: &count, SealedBatchCount: &count}
	after := before
	errorStatus := "error"
	after.Status = &errorStatus
	after.Failure = &runnerFailure{Operation: "pushing", Code: "idempotency_conflict", RecoveryAction: "none"}
	if !reflect.DeepEqual(durableRunnerState(before), durableRunnerState(after)) {
		t.Fatal("terminal status changed the Swift durable projection")
	}
	driftedCount := 0
	after.SealedBatchCount = &driftedCount
	if reflect.DeepEqual(durableRunnerState(before), durableRunnerState(after)) {
		t.Fatal("sealed batch drift passed the Swift durable comparison")
	}
}

func TestPushResponseLossBindingsRejectCompletionNotDerivedFromWire(t *testing.T) {
	scenario := loadPushResponseLossScenario(t)
	for index := range scenario.Steps {
		if scenario.Steps[index].ID != "STEP-PUSH-RESPONSE-LOSS-004" {
			continue
		}
		binding := *scenario.Steps[index].NativeBinding
		binding.Completion = "blocked"
		scenario.Steps[index].NativeBinding = &binding
	}
	steps, err := swiftScenarioStepMap(scenario, pushResponseLossScenarioID, 6)
	if err != nil {
		t.Fatalf("map mutated push-response-loss scenario: %v", err)
	}
	client := Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "push-response-loss-client-a"}
	if err := validatePushResponseLossBindings(scenario, steps, client); err == nil {
		t.Fatal("push-response-loss binding accepted a completion that differs from the authored wire expectation")
	}
}

func TestPushResponseLossBindingsRejectChangedEqualReplay(t *testing.T) {
	scenario := loadPushResponseLossScenario(t)
	for index := range scenario.Steps {
		if scenario.Steps[index].ID != "STEP-PUSH-RESPONSE-LOSS-005" {
			continue
		}
		payload := string(scenario.Steps[index].Operation.Payload)
		payload = replacePushResponseLossValue(t, payload, "response-loss", "response-loss-changed")
		scenario.Steps[index].Operation.Payload = []byte(payload)
	}
	steps, err := swiftScenarioStepMap(scenario, pushResponseLossScenarioID, 6)
	if err != nil {
		t.Fatalf("map mutated push-response-loss scenario: %v", err)
	}
	client := Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "push-response-loss-client-a"}
	if err := validatePushResponseLossBindings(scenario, steps, client); err == nil {
		t.Fatal("push-response-loss binding accepted changed content for an equal replay")
	}
}

func loadPushResponseLossScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	root := filepath.Join("..", "..")
	scenario, err := scenarios.LoadFile(context.Background(), root, "conformance/scenarios/server/push-response-loss-001.json")
	if err != nil {
		t.Fatalf("load push-response-loss scenario: %v", err)
	}
	return scenario
}

func replacePushResponseLossValue(t *testing.T, value, old, replacement string) string {
	t.Helper()
	index := -1
	for offset := 0; offset+len(old) <= len(value); offset++ {
		if value[offset:offset+len(old)] == old {
			index = offset
		}
	}
	if index < 0 {
		t.Fatalf("value %q is absent from replay payload", old)
	}
	return value[:index] + replacement + value[index+len(old):]
}
