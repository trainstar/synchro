package reactnative

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestValidateSteadyPullScenarioAcceptsAuthoredContract(t *testing.T) {
	scenario := loadSteadyPullAuthoredScenario(t)
	if err := ValidateSteadyPullScenario(scenario); err != nil {
		t.Fatalf("validate authored steady-pull scenario: %v", err)
	}
}

func TestValidateSteadyPullScenarioRejectsContractChanges(t *testing.T) {
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
					if string(scenario.ProofObligations[index].ObligationID) == "OBL-PERF-STEADY-PULL-RN-IOS-CURRENT-001" {
						scenario.ProofObligations[index].MakeTarget = "test-rn-performance-ios"
					}
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := cloneReactNativeScenario(loadSteadyPullAuthoredScenario(t))
			test.mutate(&scenario)
			if err := ValidateSteadyPullScenario(scenario); err == nil {
				t.Fatal("changed steady-pull contract was accepted")
			}
		})
	}
}

func TestSteadyPullTraceRejectsMeasuredPullWithoutChecksumDelta(t *testing.T) {
	bootstrap := validBootstrapTrace(testSchema())
	final := traceSnapshot{
		Observations: append(append([]transportObservation(nil), bootstrap.Observations...), transportWithPull(
			"pull", 4, requestFacts(1, testSchema(), 1, 1, "", ""), "cursor-b", "cursor-c",
		)),
		SequenceCheckpoint: 4,
	}
	measured, err := steadyPullTrace(final, &bootstrap)
	if err != nil {
		t.Fatalf("validate steady-pull trace shape: %v", err)
	}
	if len(measured) != 1 {
		t.Fatalf("measured observations = %d, want 1", len(measured))
	}
	capture := validFinalCapture(*warmConnectExpectedState(loadAuthoredScenario(t)))
	state, err := decodeClientState(capture.ClientState)
	if err != nil {
		t.Fatalf("decode steady-pull trace state: %v", err)
	}
	if err := validateSteadyPullTransportIdentities(state, capture, bootstrap, measured); err == nil {
		t.Fatal("measured pull without one change was accepted")
	}
}

func TestSteadyPullFinalEvidenceAcceptsOmittedDurabilityCounts(t *testing.T) {
	steadyScenario := loadSteadyPullAuthoredScenario(t)
	warmScenario := loadAuthoredScenario(t)
	capture := validFinalCapture(*warmConnectExpectedState(warmScenario))
	state, err := decodeClientState(capture.ClientState)
	if err != nil {
		t.Fatalf("decode steady-pull final state: %v", err)
	}
	if err := validateFinalClientEvidenceForExpected(steadyPullExpectedState(steadyScenario), state, capture); err != nil {
		t.Fatalf("validate steady-pull final evidence: %v", err)
	}
}

func TestSteadyPullServerStateAllowsUnprojectedRebuildFacts(t *testing.T) {
	scenario := loadSteadyPullAuthoredScenario(t)
	expected := *steadyPullExpectedState(scenario)
	actual := scenarios.CloneStateFacts(expected)
	actual.Clients = nil
	rebuildCount := uint64(0)
	actual.RebuildCount = &rebuildCount
	if err := validateServerState(expected, actual); err != nil {
		t.Fatalf("validate steady-pull server projection: %v", err)
	}
}

func TestSteadyPullFinalCaptureUsesNativeResponseKeys(t *testing.T) {
	process := actionProcessIdentity{
		ProcessID:                   "process-a",
		DatabaseIdentityFingerprint: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	coordinator := &SteadyPullCoordinator{
		config:  SteadyPullCoordinatorConfig{Scenario: loadSteadyPullAuthoredScenario(t)},
		stage:   steadyPullStageApplicationRows,
		process: &process,
	}
	valid := json.RawMessage(`{"schema_version":1,"outcome":"passed","result":{"kind":"capture","capture":{"client_state":null,"pending_mutations":null,"rejected_mutations":null,"sync_status":null,"sync_events":null,"provenance":null,"request_trace":null,"durable_proof":null},"process":{"process_id":"process-a","database_identity_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},"error_code":null,"error_detail":null}`)
	if err := coordinator.acceptResultLocked(valid); err == nil || errors.Is(err, errInvalidExchange) {
		t.Fatalf("native response keys did not reach semantic validation: %v", err)
	}

	sourceNames := json.RawMessage(`{"schema_version":1,"outcome":"passed","result":{"kind":"capture","capture":{"scope-state":null,"pending-mutations":null,"rejected-mutations":null,"sync-status":null,"sync-events":null,"provenance":null,"request-trace":null,"durable-proof":null},"process":{"process_id":"process-a","database_identity_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},"error_code":null,"error_detail":null}`)
	if err := coordinator.acceptResultLocked(sourceNames); !errors.Is(err, errInvalidExchange) {
		t.Fatalf("capture source names error = %v, want invalid exchange", err)
	}
}

func TestResolutionSchemaRuntimeMatchesCapturedState(t *testing.T) {
	runtime := clientSchema{
		Version: 7,
		Hash:    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	resolution := blackbox.NativeIdentityResolution{
		AuthoredValue: json.RawMessage(`null`),
		RuntimeValue:  json.RawMessage(`{"version":7,"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`),
	}
	if !resolutionSchemaRuntimeMatches(resolution, runtime) {
		t.Fatal("runtime schema resolution did not match captured state")
	}

	resolution.RuntimeValue = json.RawMessage(`{"version":8,"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`)
	if resolutionSchemaRuntimeMatches(resolution, runtime) {
		t.Fatal("changed runtime schema resolution matched captured state")
	}
}

func TestNewSteadyPullCoordinatorAcceptsAndroid(t *testing.T) {
	coordinator, err := NewSteadyPullCoordinator(SteadyPullCoordinatorConfig{
		Scenario:   loadSteadyPullAuthoredScenario(t),
		Platform:   "android",
		ServerURL:  "http://127.0.0.1:8080",
		AuthToken:  "unit-token",
		AppVersion: "0.3.0",
	})
	if err != nil || coordinator == nil {
		t.Fatalf("Android steady-pull coordinator was rejected: %v", err)
	}
	defer func() {
		if err := coordinator.Close(context.Background()); err != nil {
			t.Errorf("close Android steady-pull coordinator: %v", err)
		}
	}()
	// The Detox process consumes this URL from the host, so it stays on host
	// loopback for every platform. The emulator alias 10.0.2.2 resolves only
	// inside the emulator and applies to the adapter URL, not to this sidecar.
	if !strings.HasPrefix(coordinator.URL(), "http://127.0.0.1:") {
		t.Fatalf("Android steady-pull coordinator URL = %q", coordinator.URL())
	}
}

func TestNewSteadyPullCoordinatorRejectsUnknownPlatform(t *testing.T) {
	coordinator, err := NewSteadyPullCoordinator(SteadyPullCoordinatorConfig{
		Scenario:   loadSteadyPullAuthoredScenario(t),
		Platform:   "windows",
		ServerURL:  "http://127.0.0.1:8080",
		AuthToken:  "unit-token",
		AppVersion: "0.3.0",
	})
	if err == nil || coordinator != nil {
		t.Fatal("unknown-platform steady-pull coordinator was accepted")
	}
}

func loadSteadyPullAuthoredScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	scenario, err := LoadSteadyPullScenario(context.Background(), repoRoot)
	if err != nil {
		t.Fatalf("load authored steady-pull scenario: %v", err)
	}
	return scenario
}

func cloneReactNativeScenario(scenario scenarios.Scenario) scenarios.Scenario {
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
