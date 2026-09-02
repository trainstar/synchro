package reactnative

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestValidateSchemaCheckScenarioAcceptsAuthoredContract(t *testing.T) {
	if err := ValidateSchemaCheckScenario(loadSchemaCheckAuthoredScenario(t)); err != nil {
		t.Fatalf("validate authored schema-check scenario: %v", err)
	}
}

func TestValidateSchemaCheckScenarioRejectsContractChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*scenarios.Scenario)
	}{
		{"step order", func(scenario *scenarios.Scenario) {
			scenario.Steps[0], scenario.Steps[1] = scenario.Steps[1], scenario.Steps[0]
		}},
		{"unsupported completion", func(scenario *scenarios.Scenario) {
			for index := range scenario.Steps {
				if scenario.Steps[index].ID == "STEP-PERF-SCHEMA-CHECK-016" {
					scenario.Steps[index].NativeBinding.Completion = "idle"
				}
			}
		}},
		{"controller operation", func(scenario *scenarios.Scenario) {
			for index := range scenario.Steps {
				if scenario.Steps[index].ID == "STEP-PERF-SCHEMA-CHECK-CLASS2-PUBLISH-001" {
					scenario.Steps[index].Operation.Name = "other"
				}
			}
		}},
		{"Android proof target", func(scenario *scenarios.Scenario) {
			for index := range scenario.ProofObligations {
				if string(scenario.ProofObligations[index].ObligationID) == "OBL-PERF-SCHEMA-CHECK-RN-ANDROID-CURRENT-001" {
					scenario.ProofObligations[index].MakeTarget = "test-rn-schema-check-android"
				}
			}
		}},
		{"measurement case", func(scenario *scenarios.Scenario) {
			for index := range scenario.Steps {
				if scenario.Steps[index].MeasurementSample != nil {
					scenario.Steps[index].MeasurementSample.Parameters = json.RawMessage(`{"schema_case":"changed"}`)
					return
				}
			}
		}},
		{"measurement operation case", func(scenario *scenarios.Scenario) {
			for index := range scenario.Steps {
				if scenario.Steps[index].MeasurementSample != nil {
					scenario.Steps[index].MeasurementSample.Operation.Value = json.RawMessage(`{"schema_case":"changed"}`)
					return
				}
			}
		}},
		{"controller wire", func(scenario *scenarios.Scenario) {
			wire := scenario.WireExpectations[0]
			wire.StepID = "STEP-PERF-SCHEMA-CHECK-CLASS1-COMMIT-001"
			scenario.WireExpectations = append(scenario.WireExpectations, wire)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := cloneSchemaCheckScenario(loadSchemaCheckAuthoredScenario(t))
			test.mutate(&scenario)
			if err := ValidateSchemaCheckScenario(scenario); err == nil {
				t.Fatalf("changed schema-check contract was accepted: error=%v", err)
			}
		})
	}
}

func TestSchemaCheckCallsCarryAllAuthoredDispatchSamples(t *testing.T) {
	scenario := loadSchemaCheckAuthoredScenario(t)
	calls, err := schemaCheckCalls(scenario)
	if err != nil {
		t.Fatalf("derive schema-check calls: %v", err)
	}
	// The authored scenario owns these counts. Derive them so the test cannot
	// drift from the contract it checks.
	publicSteps := 0
	for _, step := range scenario.Steps {
		if step.NativeBinding != nil && step.NativeBinding.Kind == "public-call" {
			publicSteps++
		}
	}
	if len(calls) != publicSteps || len(scenario.WireExpectations) != publicSteps {
		t.Fatalf("schema-check calls=%d wires=%d, want %d public-call steps", len(calls), len(scenario.WireExpectations), publicSteps)
	}
	plan, err := schemaCheckDispatchPlan(scenario)
	if err != nil {
		t.Fatalf("read schema-check dispatch plan: %v", err)
	}
	samples := make(map[string]int, len(plan.Strata))
	measured := 0
	for _, call := range calls {
		if call.step.MeasurementSample != nil {
			measured++
			samples[string(call.step.MeasurementSample.StratumID)]++
		}
	}
	if measured != 18 {
		t.Fatalf("schema-check measured calls=%d want=18", measured)
	}
	for _, stratum := range plan.Strata {
		if samples[string(stratum.StratumID)] != 3 {
			t.Fatalf("schema-check stratum=%s samples=%d want=3", stratum.StratumID, samples[string(stratum.StratumID)])
		}
	}
	if len(scenario.NativeLifecycleBoundaries) != 18 {
		t.Fatalf("schema-check lifecycle boundaries=%d want=18", len(scenario.NativeLifecycleBoundaries))
	}
	publicCalls := make(map[scenarios.StepID]schemaCheckCall, len(calls))
	for _, call := range calls {
		publicCalls[call.step.ID] = call
	}
	for _, boundary := range scenario.NativeLifecycleBoundaries {
		call, found := publicCalls[boundary.AfterStepID]
		if !found || boundary.UserID != call.step.NativeBinding.UserID || boundary.ClientID != call.step.NativeBinding.ClientID {
			t.Fatalf("schema-check lifecycle boundary=%s step=%s is not bound to its public call", boundary.ID, boundary.AfterStepID)
		}
	}
}

func TestNewSchemaCheckCoordinatorKeepsAndroidSidecarOnHostLoopback(t *testing.T) {
	scenario := loadSchemaCheckAuthoredScenario(t)
	coordinator, err := NewSchemaCheckCoordinator(SchemaCheckCoordinatorConfig{
		Scenario: scenario, Platform: "android", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil || coordinator == nil {
		t.Fatalf("Android schema-check coordinator was rejected: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	if !strings.HasPrefix(coordinator.URL(), "http://127.0.0.1:") {
		t.Fatalf("Android schema-check coordinator URL=%q", coordinator.URL())
	}
	if !strings.HasPrefix(coordinator.adapter, "http://10.0.2.2:") {
		t.Fatalf("Android schema-check adapter URL=%q", coordinator.adapter)
	}
	// One exchange opens, synchronizes, and captures each authored call, plus
	// one for each lifecycle boundary and one terminal exchange.
	calls, err := schemaCheckCalls(scenario)
	if err != nil {
		t.Fatalf("derive schema-check calls: %v", err)
	}
	want := len(calls)*3 + len(scenario.NativeLifecycleBoundaries) + 1
	if coordinator.ExchangeCount() != want {
		t.Fatalf("schema-check exchange count=%d want=%d", coordinator.ExchangeCount(), want)
	}
}

func TestSchemaCheckCommandEncodesEmptyStepsAsArray(t *testing.T) {
	coordinator, err := NewSchemaCheckCoordinator(SchemaCheckCoordinatorConfig{
		Scenario: loadSchemaCheckAuthoredScenario(t), Platform: "ios", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create schema-check coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	command := coordinator.command(coordinator.calls[0], "client", "open", map[string]any{"client_key": coordinator.calls[0].sessionKey}, nil)
	encoded, err := json.Marshal(command)
	if err != nil || command.Action.Steps == nil || len(command.Action.Steps) != 0 || !strings.Contains(string(encoded), `"steps":[]`) {
		t.Fatalf("schema-check empty command steps=%#v encoded=%s error=%v", command.Action.Steps, encoded, err)
	}
}

func TestSchemaCheckSynchronizationCommandsCarryAuthoredStartStep(t *testing.T) {
	coordinator, err := NewSchemaCheckCoordinator(SchemaCheckCoordinatorConfig{
		Scenario: loadSchemaCheckAuthoredScenario(t), Platform: "ios", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create schema-check coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	for _, call := range coordinator.calls {
		command := coordinator.command(call, "client", "synchronize-step", map[string]any{
			"client_key": call.sessionKey, "method": call.step.NativeBinding.Method, "completion": call.step.NativeBinding.Completion,
		}, []scenarios.StepID{call.step.ID})
		method, methodOK := command.Action.Action.Parameters["method"].(string)
		if command.Action.Action.Actor != "client" || command.Action.Action.Command != "synchronize-step" || !methodOK || method != "start" {
			t.Fatalf("schema-check step=%s actor=%q command=%q method=%q method_ok=%t", call.step.ID, command.Action.Action.Actor, command.Action.Action.Command, method, methodOK)
		}
		if command.Action.Steps == nil || len(command.Action.Steps) != 1 {
			t.Fatalf("schema-check step=%s command steps=%#v", call.step.ID, command.Action.Steps)
		}
		operation := command.Action.Steps[0].Operation
		if operation.ContractOperation != call.step.Operation.ContractOperation || operation.Name != call.step.Operation.Name || !semanticRawJSONEqual(operation.Payload, call.step.Operation.Payload) {
			t.Fatalf("schema-check step=%s operation=%#v want=%#v", call.step.ID, operation, call.step.Operation)
		}
	}
}

func TestSchemaCheckCaptureOmitsUndeclaredDurableProof(t *testing.T) {
	coordinator, err := NewSchemaCheckCoordinator(SchemaCheckCoordinatorConfig{
		Scenario: loadSchemaCheckAuthoredScenario(t), Platform: "ios", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create schema-check coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	coordinator.waiting = schemaCheckWaitingSync
	response, err := coordinator.advanceLocked(context.Background(), 1)
	if err != nil || response.Command == nil {
		t.Fatalf("create schema-check capture command: command=%#v error=%v", response.Command, err)
	}
	parameters := response.Command.Action.Action.Parameters
	sources, ok := parameters["sources"].([]string)
	want := []string{"scope-state", "sync-status", "sync-events", "request-trace"}
	if !ok || !slices.Equal(sources, want) {
		t.Fatalf("schema-check capture sources=%#v want=%#v", parameters["sources"], want)
	}
	if _, found := parameters["durable_proof_identity"]; found {
		t.Fatalf("schema-check capture included an undeclared durable proof identity")
	}
}

func TestSchemaCheckTraceRequiresSuccessfulConnectInCallWindow(t *testing.T) {
	trace := traceSnapshot{Observations: []transportObservation{{
		Sequence: 1, OperationClass: "connect", StatusCode: http.StatusOK, DurationNanoseconds: 1,
		RequestFacts: json.RawMessage(`{"client_generation":1,"scope_set_version":1}`),
	}}}
	if !schemaCheckTraceContainsConnect(trace) {
		t.Fatal("schema-check successful connect without a terminal server schema was rejected")
	}
	trace.Observations[0].StatusCode = http.StatusConflict
	if schemaCheckTraceContainsConnect(trace) {
		t.Fatal("schema-check unsuccessful connect was accepted")
	}
}

func TestSchemaCheckIdentityEvidenceUsesCapturedPullRequests(t *testing.T) {
	trace := func(generation, scopeSetVersion uint64) json.RawMessage {
		t.Helper()
		raw, err := json.Marshal(traceSnapshot{Observations: []transportObservation{{
			OperationClass: "pull",
			RequestFacts: json.RawMessage(fmt.Sprintf(
				`{"client_generation":%d,"scope_set_version":%d}`,
				generation,
				scopeSetVersion,
			)),
		}}})
		if err != nil {
			t.Fatalf("encode schema-check identity trace: %v", err)
		}
		return raw
	}
	coordinator := &SchemaCheckCoordinator{
		calls: []schemaCheckCall{
			{step: scenarios.Step{ID: "step-a"}, clientKey: "client-a"},
			{step: scenarios.Step{ID: "step-b"}, clientKey: "client-b"},
		},
		captures: map[scenarios.StepID]finalCapture{
			"step-a": {Trace: trace(7, 9)},
			"step-b": {Trace: trace(7, 9)},
		},
	}
	generation, scopeSetVersion, err := coordinator.observedClientIdentities()
	if err != nil || generation != 7 || scopeSetVersion != 9 {
		t.Fatalf("schema-check observed identity generation=%d scope_set_version=%d want generation=7 scope_set_version=9 error=%v", generation, scopeSetVersion, err)
	}
	coordinator.captures["step-b"] = finalCapture{Trace: trace(8, 9)}
	if _, _, err := coordinator.observedClientIdentities(); err == nil || !strings.Contains(err.Error(), "observed") || !strings.Contains(err.Error(), "want") {
		t.Fatalf("schema-check inconsistent identity error=%v want observed and expected values", err)
	}
}

func TestSchemaCheckHasNoRawObserverRead(t *testing.T) {
	raw, err := os.ReadFile("schema_check.go")
	if err != nil {
		t.Fatalf("read schema-check coordinator source: %v", err)
	}
	for _, forbidden := range []string{"OpenObserver(", "synchro.sync_clients"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("schema-check coordinator observed forbidden raw read %q want controller and captured evidence only", forbidden)
		}
	}
}

func loadSchemaCheckAuthoredScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	scenario, err := LoadSchemaCheckScenario(context.Background(), repoRoot)
	if err != nil {
		t.Fatalf("load authored schema-check scenario: %v", err)
	}
	return scenario
}

func cloneSchemaCheckScenario(scenario scenarios.Scenario) scenarios.Scenario {
	raw, err := json.Marshal(scenario)
	if err != nil {
		panic(err)
	}
	var clone scenarios.Scenario
	if err := json.Unmarshal(raw, &clone); err != nil {
		panic(err)
	}
	return clone
}
