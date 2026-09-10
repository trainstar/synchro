package reactnative

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestValidatePendingCycleScenarioAcceptsAuthoredContract(t *testing.T) {
	scenario := loadPendingCycleAuthoredScenario(t)
	if err := ValidatePendingCycleScenario(scenario); err != nil {
		t.Fatalf("validate authored pending-cycle scenario: %v", err)
	}
}

func TestValidatePendingCycleScenarioRejectsContractChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*scenarios.Scenario)
	}{
		{
			name: "step order",
			mutate: func(scenario *scenarios.Scenario) {
				scenario.Steps[0], scenario.Steps[1] = scenario.Steps[1], scenario.Steps[0]
			},
		},
		{
			name: "lifecycle boundary",
			mutate: func(scenario *scenarios.Scenario) {
				scenario.NativeLifecycleBoundaries = append(scenario.NativeLifecycleBoundaries, scenarios.NativeLifecycleBoundary{ID: "unexpected"})
			},
		},
		{
			name: "iOS proof target",
			mutate: func(scenario *scenarios.Scenario) {
				for index := range scenario.ProofObligations {
					if string(scenario.ProofObligations[index].ObligationID) == "OBL-PERF-PENDING-CYCLE-RN-IOS-CURRENT-001" {
						scenario.ProofObligations[index].MakeTarget = "test-rn-performance-ios"
					}
				}
			},
		},
		{
			name: "expected outcome",
			mutate: func(scenario *scenarios.Scenario) {
				scenario.Steps[0].ExpectedOutcome.Disposition = "error"
			},
		},
		{
			name: "Issue 49 assertion claim",
			mutate: func(scenario *scenarios.Scenario) {
				scenario.Assertions[len(scenario.Assertions)-1].Oracle.ExpectedSource = "system-under-test"
			},
		},
		{
			name: "Issue 49 native proof claim",
			mutate: func(scenario *scenarios.Scenario) {
				for index := range scenario.ProofObligations {
					if string(scenario.ProofObligations[index].ObligationID) == "OBL-PERF-PENDING-CYCLE-RN-IOS-CURRENT-001" {
						scenario.ProofObligations[index].RequirementIDs = scenario.ProofObligations[index].RequirementIDs[:1]
					}
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := cloneReactNativePendingCycleScenario(loadPendingCycleAuthoredScenario(t))
			test.mutate(&scenario)
			if err := ValidatePendingCycleScenario(scenario); err == nil {
				t.Fatal("changed pending-cycle contract was accepted")
			}
		})
	}
}

func TestPendingCycleCommandUsesAuthoredPullOperation(t *testing.T) {
	scenario := loadPendingCycleAuthoredScenario(t)
	steps := make(map[scenarios.StepID]scenarios.Step, len(scenario.Steps))
	for _, step := range scenario.Steps {
		steps[step.ID] = step
	}
	coordinator := &PendingCycleCoordinator{
		steps:     steps,
		clientKey: "client-a",
		clientID:  "client-a",
	}
	command := coordinator.command("client", "synchronize-step", map[string]any{"client_key": "client-a"}, []scenarios.StepID{pendingCyclePullStepID})
	if len(command.Action.Steps) != 1 || !bytes.Equal(command.Action.Steps[0].Operation.Payload, coordinator.steps[pendingCyclePullStepID].Operation.Payload) {
		t.Fatal("pending-cycle pull command did not preserve the authored operation")
	}
}

func TestPendingCycleCaptureRequestsDirectNativeEvidence(t *testing.T) {
	coordinator := &PendingCycleCoordinator{
		clientKey: "client-a",
		target: scenarios.PendingCycleNativeTarget{
			TableName:           "cf_items",
			PrimaryKeyField:     "id",
			RecordID:            "runtime-row",
			ValueField:          "value",
			Value:               "pending",
			UnprotectedRecordID: "runtime-unprotected-row",
			UnprotectedValue:    "unprotected",
		},
	}
	command := coordinator.captureCommand()
	parameters := command.Action.Action.Parameters
	sources, ok := parameters["sources"].([]string)
	if !ok || len(sources) != len(pendingCycleCaptureSources) {
		t.Fatalf("pending-cycle capture sources = %#v", parameters["sources"])
	}
	for index, source := range pendingCycleCaptureSources {
		if sources[index] != source {
			t.Fatalf("pending-cycle capture source %d = %q, want %q", index, sources[index], source)
		}
	}
	identity, ok := parameters["durable_proof_identity"].(map[string]any)
	if !ok || identity["table_name"] != "cf_items" || identity["record_id"] != "runtime-row" {
		t.Fatalf("pending-cycle durable-proof identity = %#v", parameters["durable_proof_identity"])
	}
	selectors, ok := parameters["row_selectors"].([]map[string]any)
	if !ok || len(selectors) != 2 || selectors[1]["primary_key"] != "runtime-unprotected-row" {
		t.Fatalf("pending-cycle row selectors = %#v", parameters["row_selectors"])
	}
	if coordinator.ExchangeCount() != int(pendingCycleStageComplete)+1 {
		t.Fatalf("pending-cycle exchange count = %d", coordinator.ExchangeCount())
	}
}

func TestPendingCycleRuntimeTargetRejectsAmbiguousValueField(t *testing.T) {
	operation := scenarios.Operation{
		ContractOperation: "local",
		Name:              "write",
		Payload:           json.RawMessage(`{"table_id":"cf_items","pk":{"id":"runtime-row"},"columns":{"value":"pending","duplicate":"pending"}}`),
	}
	if _, err := pendingCycleNativeTarget(operation); err == nil {
		t.Fatal("ambiguous pending-cycle runtime target was accepted")
	}
}

func TestPendingCycleTemporaryUnavailableFaultAcceptsOnlyGeneratedUpdate(t *testing.T) {
	coordinator := &PendingCycleCoordinator{faultArmed: true}
	if err := coordinator.recordTemporaryUnavailablePush(json.RawMessage(`{"mutations":[{"op":"update"}]}`)); err != nil {
		t.Fatalf("generated update fault was rejected: %v", err)
	}
	if err := coordinator.recordTemporaryUnavailablePush(json.RawMessage(`{"mutations":[{"op":"delete"}]}`)); err == nil {
		t.Fatal("generated delete fault was accepted")
	}
}

func TestNewPendingCycleCoordinatorKeepsAndroidSidecarOnHostLoopback(t *testing.T) {
	coordinator, err := NewPendingCycleCoordinator(PendingCycleCoordinatorConfig{
		Scenario:   loadPendingCycleAuthoredScenario(t),
		Platform:   "android",
		ServerURL:  "http://127.0.0.1:8080",
		AuthToken:  "unit-token",
		AppVersion: "0.3.0",
	})
	if err != nil || coordinator == nil {
		t.Fatalf("Android pending-cycle coordinator was rejected: %v", err)
	}
	defer func() {
		if err := coordinator.Close(context.Background()); err != nil {
			t.Errorf("close Android pending-cycle coordinator: %v", err)
		}
	}()
	if !strings.HasPrefix(coordinator.URL(), "http://127.0.0.1:") {
		t.Fatalf("Android pending-cycle coordinator URL = %q", coordinator.URL())
	}
	if !strings.HasPrefix(coordinator.adapter, "http://10.0.2.2:") {
		t.Fatalf("Android pending-cycle adapter URL = %q", coordinator.adapter)
	}
	if coordinator.upstream != "http://127.0.0.1:8080" {
		t.Fatalf("Android pending-cycle upstream URL = %q", coordinator.upstream)
	}
}

func TestNewPendingCycleCoordinatorRejectsUnknownPlatform(t *testing.T) {
	coordinator, err := NewPendingCycleCoordinator(PendingCycleCoordinatorConfig{
		Scenario:   loadPendingCycleAuthoredScenario(t),
		Platform:   "windows",
		ServerURL:  "http://127.0.0.1:8080",
		AuthToken:  "unit-token",
		AppVersion: "0.3.0",
	})
	if err == nil || coordinator != nil {
		t.Fatal("unknown-platform pending-cycle coordinator was accepted")
	}
}

func loadPendingCycleAuthoredScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	scenario, err := LoadPendingCycleScenario(context.Background(), repoRoot)
	if err != nil {
		t.Fatalf("load authored pending-cycle scenario: %v", err)
	}
	return scenario
}

func cloneReactNativePendingCycleScenario(scenario scenarios.Scenario) scenarios.Scenario {
	data, err := json.Marshal(scenario)
	if err != nil {
		panic(err)
	}
	var clone scenarios.Scenario
	if err := json.Unmarshal(data, &clone); err != nil {
		panic(err)
	}
	return clone
}

// A pull between an accepted push and its materialization returns 503
// capture_pending, and the client retries it. The trace must accept that retry
// and must still reject a failure that the contract does not allow.
func TestPendingCycleTraceHonorsCapturePendingRetry(t *testing.T) {
	scenario := loadPendingCycleAuthoredScenario(t)
	schema := testSchema()
	build := func(mutate func(*traceSnapshot)) json.RawMessage {
		t.Helper()
		trace := validBootstrapTrace(schema)
		trace.Observations = append(trace.Observations,
			transport("connect", 4, requestFacts(1, schema, 1, 1, "", "")),
			transport("push", 5, requestFacts(1, schema, 1, 1, "", "")),
			transport("pull", 6, requestFacts(1, schema, 1, 1, "", "")),
			transportWithPull("pull", 7, requestFacts(1, schema, 1, 1, "", ""), "cursor-b", "cursor-c"),
		)
		trace.Observations[5].StatusCode = pendingCycleCapturePendingStatus
		trace.SequenceCheckpoint = 7
		if mutate != nil {
			mutate(&trace)
		}
		raw, err := json.Marshal(trace)
		if err != nil {
			t.Fatalf("encode pending-cycle trace: %v", err)
		}
		return raw
	}

	if err := validatePendingCycleTrace(scenario, build(nil)); err != nil {
		t.Fatalf("capture_pending retry was rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*traceSnapshot)
	}{
		{"capture pending before the push", func(trace *traceSnapshot) {
			trace.Observations[4].StatusCode = pendingCycleCapturePendingStatus
		}},
		{"no pull after the push", func(trace *traceSnapshot) {
			trace.Observations = trace.Observations[:5]
			trace.SequenceCheckpoint = 5
		}},
		{"second push", func(trace *traceSnapshot) {
			trace.Observations[6].OperationClass = "push"
			trace.Observations[6].StatusCode = 200
		}},
		{"pull never succeeds", func(trace *traceSnapshot) {
			trace.Observations[6].StatusCode = pendingCycleCapturePendingStatus
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validatePendingCycleTrace(scenario, build(test.mutate)); err == nil {
				t.Fatal("invalid pending-cycle trace was accepted")
			}
		})
	}
}
