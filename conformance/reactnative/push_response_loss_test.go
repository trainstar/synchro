package reactnative

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestValidatePushResponseLossScenarioAcceptsAuthoredContract(t *testing.T) {
	scenario := loadPushResponseLossAuthoredScenario(t)
	if err := ValidatePushResponseLossScenario(scenario); err != nil {
		t.Fatalf("validate authored response-loss scenario: %v", err)
	}
}

func TestValidatePushResponseLossScenarioRejectsContractChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*scenarios.Scenario)
	}{
		{"step order", func(scenario *scenarios.Scenario) {
			scenario.Steps[0], scenario.Steps[1] = scenario.Steps[1], scenario.Steps[0]
		}},
		{"response-loss delivery", func(scenario *scenarios.Scenario) {
			var payload map[string]any
			if err := json.Unmarshal(scenario.Steps[1].Operation.Payload, &payload); err != nil {
				panic(err)
			}
			payload["delivery"] = "apply"
			scenario.Steps[1].Operation.Payload, _ = json.Marshal(payload)
		}},
		{"changed replay", func(scenario *scenarios.Scenario) {
			var payload map[string]any
			if err := json.Unmarshal(scenario.Steps[5].Operation.Payload, &payload); err != nil {
				panic(err)
			}
			request := payload["request"].(map[string]any)
			mutation := request["mutations"].([]any)[0].(map[string]any)
			mutation["columns"].(map[string]any)["value"] = "response-loss"
			scenario.Steps[5].Operation.Payload, _ = json.Marshal(payload)
		}},
		{"wire status", func(scenario *scenarios.Scenario) {
			scenario.WireExpectations[0].HTTPStatus = http.StatusOK
		}},
		{"identity kind", func(scenario *scenarios.Scenario) {
			scenario.NativeIdentityAliases[0].Kind = "batch-id"
		}},
		{"assertion oracle", func(scenario *scenarios.Scenario) {
			scenario.Assertions[0].Oracle.ExpectedSource = "system-under-test"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := clonePushResponseLossScenario(loadPushResponseLossAuthoredScenario(t))
			test.mutate(&scenario)
			if err := ValidatePushResponseLossScenario(scenario); err == nil {
				t.Fatal("changed response-loss contract was accepted")
			}
		})
	}
}

func TestNewPushResponseLossCoordinatorUsesHostLoopbackProxy(t *testing.T) {
	upstream := httptest.NewServer(http.NotFoundHandler())
	defer upstream.Close()
	coordinator, err := NewPushResponseLossCoordinator(PushResponseLossCoordinatorConfig{
		Scenario: loadPushResponseLossAuthoredScenario(t), Platform: "android", ServerURL: upstream.URL, AuthToken: "unit-token", AppVersion: "0.3.0",
	})
	if err != nil {
		t.Fatalf("create response-loss coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	if !strings.HasPrefix(coordinator.URL(), "http://127.0.0.1:") {
		t.Fatalf("coordinator URL = %q", coordinator.URL())
	}
	if !strings.HasPrefix(coordinator.adapter, "http://10.0.2.2:") {
		t.Fatalf("Android adapter URL = %q", coordinator.adapter)
	}
	if coordinator.upstream != upstream.URL {
		t.Fatalf("upstream URL = %q, want %q", coordinator.upstream, upstream.URL)
	}
	if coordinator.ExchangeCount() != 10 {
		t.Fatalf("exchange count = %d, want 10", coordinator.ExchangeCount())
	}
}

func TestPushResponseLossProxyWritesInvalidInitialResponseStart(t *testing.T) {
	committed := make(chan struct{}, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/sync/push" {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		committed <- struct{}{}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte(`{}`))
	}))
	defer upstream.Close()
	coordinator, err := NewPushResponseLossCoordinator(PushResponseLossCoordinatorConfig{
		Scenario: loadPushResponseLossAuthoredScenario(t), Platform: "android", ServerURL: upstream.URL, AuthToken: "unit-token", AppVersion: "0.3.0",
	})
	if err != nil {
		t.Fatalf("create response-loss coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	proxy := httptest.NewServer(coordinator.Handler())
	defer proxy.Close()
	connection, err := net.DialTimeout("tcp", proxy.Listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("connect initial push client: %v", err)
	}
	defer func() { _ = connection.Close() }()
	request := "POST /sync/push HTTP/1.1\r\nHost: " + proxy.Listener.Addr().String() + "\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}"
	if _, err := io.WriteString(connection, request); err != nil {
		t.Fatalf("send initial push request: %v", err)
	}
	select {
	case <-committed:
	case <-time.After(time.Second):
		t.Fatal("initial push did not reach the upstream server")
	}
	coordinator.releaseInitialResponse()
	if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set initial push read deadline: %v", err)
	}
	response, err := io.ReadAll(connection)
	if err != nil {
		t.Fatalf("read initial response loss: %v", err)
	}
	if string(response) != "SYNCHRO RESPONSE LOSS\r\n\r\n" {
		t.Fatalf("initial response loss bytes = %q, want an invalid response start", response)
	}
	coordinator.proxyMu.Lock()
	pushes, proxyErr := coordinator.pushRequests, coordinator.proxyErr
	coordinator.proxyMu.Unlock()
	if pushes != 1 {
		t.Fatalf("initial response loss proxy pushes = %d, want 1 before the SDK replay", pushes)
	}
	if proxyErr != nil {
		t.Fatalf("initial response loss proxy error = %v", proxyErr)
	}
}

func TestPushResponseLossCommandUsesNonNilEmptyStepManifest(t *testing.T) {
	coordinator, err := NewPushResponseLossCoordinator(PushResponseLossCoordinatorConfig{
		Scenario: loadPushResponseLossAuthoredScenario(t), Platform: "ios", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create response-loss coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	command := coordinator.command("client", "open", map[string]any{"client_key": coordinator.clientKey}, nil)
	if command.Action.Steps == nil || len(command.Action.Steps) != 0 {
		t.Fatalf("empty command steps = %#v", command.Action.Steps)
	}
}

func TestValidatePushResponseLossTraceMatchesAuthoredReplay(t *testing.T) {
	scenario := loadPushResponseLossAuthoredScenario(t)
	if err := validatePushResponseLossTrace(scenario, validPushResponseLossTrace()); err != nil {
		t.Fatalf("valid push replay trace failed: %v", err)
	}
}

func TestValidatePushResponseLossTraceReportsObservedAndExpectedValues(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*traceSnapshot)
		wantDetail string
	}{
		{"initial status", func(trace *traceSnapshot) { trace.Observations[0].StatusCode = http.StatusOK }, "status:200"},
		{"replay status", func(trace *traceSnapshot) { trace.Observations[1].StatusCode = http.StatusConflict }, "status:409"},
		{"replay duration", func(trace *traceSnapshot) { trace.Observations[1].DurationNanoseconds = 0 }, "duration:0"},
		{"replay request facts", func(trace *traceSnapshot) { trace.Observations[1].RequestFacts = nil }, "request_facts:false"},
		{"push count", func(trace *traceSnapshot) { trace.Observations = trace.Observations[:1]; trace.SequenceCheckpoint = 1 }, "count 1"},
		{"generation", func(trace *traceSnapshot) {
			trace.Observations[1].RequestFacts = json.RawMessage(`{"client_generation":2}`)
		}, "generation = 2, want 1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			trace := validPushResponseLossTrace()
			test.mutate(&trace)
			err := validatePushResponseLossTrace(loadPushResponseLossAuthoredScenario(t), trace)
			if err == nil {
				t.Fatal("invalid push replay trace was accepted")
			}
			if !strings.Contains(err.Error(), test.wantDetail) || !strings.Contains(err.Error(), "want") {
				t.Fatalf("diagnostic = %q, want observed detail %q and expected value", err, test.wantDetail)
			}
		})
	}
}

func validPushResponseLossTrace() traceSnapshot {
	return traceSnapshot{
		Observations: []transportObservation{
			{Sequence: 1, OperationClass: "push", StatusCode: 0, DurationNanoseconds: 1, RequestFacts: json.RawMessage(`{"client_generation":1}`)},
			{Sequence: 2, OperationClass: "push", StatusCode: http.StatusOK, DurationNanoseconds: 1, RequestFacts: json.RawMessage(`{"client_generation":1}`)},
		},
		SequenceCheckpoint: 2,
	}
}

func loadPushResponseLossAuthoredScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	scenario, err := LoadPushResponseLossScenario(context.Background(), repositoryRoot)
	if err != nil {
		t.Fatalf("load authored response-loss scenario: %v", err)
	}
	return scenario
}

func clonePushResponseLossScenario(scenario scenarios.Scenario) scenarios.Scenario {
	encoded, _ := json.Marshal(scenario)
	var clone scenarios.Scenario
	_ = json.Unmarshal(encoded, &clone)
	return clone
}
