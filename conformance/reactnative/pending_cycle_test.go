package reactnative

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
			name: "capture-pending stage",
			mutate: func(scenario *scenarios.Scenario) {
				scenario.Steps[3].NativeBinding.Stage = "await-call"
			},
		},
		{
			name: "capture-pending status",
			mutate: func(scenario *scenarios.Scenario) {
				pendingCycleScenarioWire(t, scenario, pendingCycleCapturePendingStepID).HTTPStatus = http.StatusOK
			},
		},
		{
			name: "capture-pending error code",
			mutate: func(scenario *scenarios.Scenario) {
				code := "temporary_unavailable"
				pendingCycleScenarioWire(t, scenario, pendingCycleCapturePendingStepID).ErrorCode = &code
			},
		},
		{
			name: "capture-pending retryability",
			mutate: func(scenario *scenarios.Scenario) {
				pendingCycleScenarioWire(t, scenario, pendingCycleCapturePendingStepID).Retryable = false
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

func TestPendingCycleStagedCommandsBindOneAuthoredCall(t *testing.T) {
	coordinator, err := NewPendingCycleCoordinator(PendingCycleCoordinatorConfig{
		Scenario: loadPendingCycleAuthoredScenario(t), Platform: "ios", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create pending-cycle coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()

	coordinator.initialPushRecorded = true
	close(coordinator.initialPushDone)
	coordinator.capturePendingRecorded = true
	close(coordinator.capturePendingDone)
	coordinator.retryPullRecorded = true
	close(coordinator.retryPullDone)

	tests := []struct {
		stage  pendingCycleStage
		actor  string
		name   string
		stepID scenarios.StepID
	}{
		{pendingCycleStageAfterInitialWrite, "client", "begin-call", pendingCyclePushStepID},
		{pendingCycleStageInitialPushBegun, "observer", "await-step", pendingCyclePushStepID},
		{pendingCycleStageInitialPushObserved, "observer", "await-step", pendingCycleCapturePendingStepID},
		{pendingCycleStageBeforePull, "client", "await-call", pendingCyclePullStepID},
	}
	for _, test := range tests {
		t.Run(string(test.stepID), func(t *testing.T) {
			coordinator.stage = test.stage
			response, err := coordinator.advanceLocked(context.Background(), 1)
			if err != nil {
				t.Fatalf("advance pending-cycle stage: %v", err)
			}
			action := response.Command.Action.Action
			if action.Actor != test.actor || action.Command != test.name || action.Parameters["call_id"] != pendingCycleCallID {
				t.Fatalf("pending-cycle command = %#v, want %s/%s call %q", action, test.actor, test.name, pendingCycleCallID)
			}
			if len(response.Command.Action.Steps) != 1 || !bytes.Equal(response.Command.Action.Steps[0].Operation.Payload, coordinator.steps[test.stepID].Operation.Payload) {
				t.Fatalf("pending-cycle command step = %#v, want %s", response.Command.Action.Steps, test.stepID)
			}
		})
	}
}

func TestPendingCycleCapturePendingWireRejectsChangedResponse(t *testing.T) {
	scenario := loadPendingCycleAuthoredScenario(t)
	valid := []byte(`{"error":{"code":"capture_pending","message":"capture pending","retryable":true}}`)
	if err := pendingCycleValidateHTTPWire(scenario, pendingCycleCapturePendingStepID, http.StatusServiceUnavailable, valid); err != nil {
		t.Fatalf("validate capture-pending wire: %v", err)
	}
	for _, test := range []struct {
		name   string
		status int
		body   []byte
	}{
		{"status", http.StatusOK, valid},
		{"code", http.StatusServiceUnavailable, []byte(`{"error":{"code":"temporary_unavailable","message":"capture pending","retryable":true}}`)},
		{"retryability", http.StatusServiceUnavailable, []byte(`{"error":{"code":"capture_pending","message":"capture pending","retryable":false}}`)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := pendingCycleValidateHTTPWire(scenario, pendingCycleCapturePendingStepID, test.status, test.body); err == nil {
				t.Fatal("changed capture-pending wire was accepted")
			}
		})
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

func TestPendingCycleCloseReleasesWALMaterialization(t *testing.T) {
	coordinator, err := NewPendingCycleCoordinator(PendingCycleCoordinatorConfig{
		Scenario: loadPendingCycleAuthoredScenario(t), Platform: "ios", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create pending-cycle coordinator: %v", err)
	}
	releases := 0
	coordinator.resumeWAL = func(context.Context) error {
		releases++
		return nil
	}
	if err := coordinator.Close(context.Background()); err != nil {
		t.Fatalf("close pending-cycle coordinator: %v", err)
	}
	if releases != 1 {
		t.Fatalf("pending-cycle WAL releases = %d, want 1", releases)
	}
}

func TestPendingCycleProxyHoldsRetryPullUntilMaterialization(t *testing.T) {
	upstreamCalled := make(chan struct{}, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		upstreamCalled <- struct{}{}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{}`))
	}))
	defer upstream.Close()
	coordinator, err := NewPendingCycleCoordinator(PendingCycleCoordinatorConfig{
		Scenario: loadPendingCycleAuthoredScenario(t), Platform: "ios", ServerURL: upstream.URL, AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create pending-cycle coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	coordinator.initialPushRecorded = true
	coordinator.capturePendingRecorded = true

	completed := make(chan struct{})
	go func() {
		request := httptest.NewRequest(http.MethodPost, "/sync/pull", strings.NewReader(`{}`))
		coordinator.proxyAdapter(httptest.NewRecorder(), request)
		close(completed)
	}()
	select {
	case <-upstreamCalled:
		t.Fatal("retry pull reached the adapter before materialization")
	case <-time.After(50 * time.Millisecond):
	}
	coordinator.signalInitialPullMaterialized()
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("retry pull did not resume after materialization")
	}
	select {
	case <-upstreamCalled:
	default:
		t.Fatal("retry pull did not reach the adapter after materialization")
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

func pendingCycleScenarioWire(t *testing.T, scenario *scenarios.Scenario, stepID scenarios.StepID) *scenarios.WireExpectation {
	t.Helper()
	for index := range scenario.WireExpectations {
		if scenario.WireExpectations[index].StepID == stepID {
			return &scenario.WireExpectations[index]
		}
	}
	t.Fatalf("pending-cycle wire expectation %s is absent", stepID)
	return nil
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
		complete := true
		trace.Observations[5].CursorFingerprints = []string{hashFingerprint("cursor-b")}
		trace.Observations[5].CursorFingerprintsComplete = &complete
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
		{"missing retry pull", func(trace *traceSnapshot) {
			trace.Observations = trace.Observations[:6]
			trace.SequenceCheckpoint = 6
		}},
		{"second push", func(trace *traceSnapshot) {
			trace.Observations[6].OperationClass = "push"
			trace.Observations[6].StatusCode = 200
		}},
		{"pull never succeeds", func(trace *traceSnapshot) {
			trace.Observations[6].StatusCode = pendingCycleCapturePendingStatus
		}},
		{"capture pending cursor absent", func(trace *traceSnapshot) {
			trace.Observations[5].CursorFingerprints = nil
			trace.Observations[5].CursorFingerprintsComplete = nil
		}},
		{"capture pending cursor changed", func(trace *traceSnapshot) {
			trace.Observations[5].CursorFingerprints = []string{hashFingerprint("wrong-cursor")}
		}},
		{"retry cursor absent", func(trace *traceSnapshot) {
			trace.Observations[6].CursorFingerprints = []string{}
		}},
		{"retry cursor changed", func(trace *traceSnapshot) {
			trace.Observations[6].CursorFingerprints = []string{hashFingerprint("wrong-cursor")}
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
