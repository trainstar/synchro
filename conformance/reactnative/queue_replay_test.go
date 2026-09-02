package reactnative

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestValidateQueueReplayScenarioAcceptsAuthoredContract(t *testing.T) {
	if err := ValidateQueueReplayScenario(loadQueueReplayAuthoredScenario(t)); err != nil {
		t.Fatalf("validate authored queue-replay scenario: %v", err)
	}
}

func TestValidateQueueReplayScenarioRejectsContractChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*scenarios.Scenario)
	}{
		{"step order", func(scenario *scenarios.Scenario) {
			scenario.Steps[0], scenario.Steps[1] = scenario.Steps[1], scenario.Steps[0]
		}},
		{"workload count", func(scenario *scenarios.Scenario) {
			scenario.Steps[0].NativeBinding.Workload.RecordCount++
		}},
		{"iOS proof target", func(scenario *scenarios.Scenario) {
			for index := range scenario.ProofObligations {
				if string(scenario.ProofObligations[index].ObligationID) == "OBL-PERF-QUEUE-REPLAY-RN-IOS-CURRENT-001" {
					scenario.ProofObligations[index].MakeTarget = "test-rn-queue-replay-ios"
				}
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := cloneQueueReplayScenario(loadQueueReplayAuthoredScenario(t))
			test.mutate(&scenario)
			if err := ValidateQueueReplayScenario(scenario); err == nil {
				t.Fatal("changed queue-replay contract was accepted")
			}
		})
	}
}

func TestQueueReplayWorkloadsFollowAuthoredCountsAndDigests(t *testing.T) {
	scenario := loadQueueReplayAuthoredScenario(t)
	workloads, err := queueReplayWorkloads(scenario)
	if err != nil {
		t.Fatalf("derive queue-replay workloads: %v", err)
	}
	if len(workloads) != len(scenario.Steps) {
		t.Fatalf("derived queue-replay workloads = %d, want %d", len(workloads), len(scenario.Steps))
	}
	for index, workload := range workloads {
		want := scenario.Steps[index].NativeBinding.Workload.RecordCount
		if uint64(len(workload.local)) != want {
			t.Fatalf("queue-replay workload %d local writes = %d, want %d", index+1, len(workload.local), want)
		}
		if scenarios.OperationKey(workload.publish) != "model/publish-schema" || scenarios.OperationKey(workload.dropPush) != "push/submit" {
			t.Fatalf("queue-replay workload %d did not derive schema and push operations", index+1)
		}
	}
}

func TestQueueReplayResponseLossBindingUsesCommittedDelivery(t *testing.T) {
	scenario := loadQueueReplayAuthoredScenario(t)
	workloads, err := queueReplayWorkloads(scenario)
	if err != nil {
		t.Fatalf("derive queue-replay workloads: %v", err)
	}
	if len(workloads) == 0 {
		t.Fatalf("queue-replay derived workload count = %d, want at least 1", len(workloads))
	}
	committed, err := pushResponseLossAppliedOperation(workloads[0].dropPush)
	if err != nil {
		t.Fatalf("convert queue-replay response-loss push: %v", err)
	}
	var authoredPayload, committedPayload map[string]any
	if err := json.Unmarshal(workloads[0].dropPush.Payload, &authoredPayload); err != nil {
		t.Fatalf("decode queue-replay authored response-loss push: %v", err)
	}
	if err := json.Unmarshal(committed.Payload, &committedPayload); err != nil {
		t.Fatalf("decode queue-replay committed response-loss push: %v", err)
	}
	if got, want := authoredPayload["delivery"], "drop_after_server"; got != want {
		t.Fatalf("queue-replay authored response-loss delivery = %v, want %q", got, want)
	}
	if got, want := committedPayload["delivery"], "apply"; got != want {
		t.Fatalf("queue-replay committed response-loss delivery = %v, want %q", got, want)
	}
}

func TestQueueReplaySynchronizedResultNamesObservedAndExpectedValues(t *testing.T) {
	coordinator := &QueueReplayCoordinator{process: &actionProcessIdentity{
		ProcessID:                   "process-a",
		DatabaseIdentityFingerprint: strings.Repeat("a", 64),
	}}
	err := coordinator.validateSynchronized(json.RawMessage(`{"kind":"synchronized","completion":"idle","status":{"state":"ready","retry_at":null,"operation":null,"failure":null},"process":{"process_id":"process-b","database_identity_fingerprint":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}}`), "blocked")
	if err == nil {
		t.Fatal("invalid synchronized result was accepted")
	}
	for _, detail := range []string{
		"members=[completion kind process status] want_count=4",
		`kind="synchronized" want="synchronized"`,
		`completion="idle" want="blocked"`,
		"status_members=[failure operation retry_at state]",
		`state="ready" retry_at=null operation=null failure=null`,
		`process={process_id:"process-b" database_identity_fingerprint:"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"} want={process_id:"process-a" database_identity_fingerprint:"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`,
	} {
		if !strings.Contains(err.Error(), detail) {
			t.Fatalf("synchronized diagnostic = %q, want detail %q", err, detail)
		}
	}
}

func TestQueueReplayProxyDropsCommittedPushAndForwardsReplay(t *testing.T) {
	received := make(chan string, 2)
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/sync/push" {
			t.Errorf("upstream request method=%q path=%q, want POST /sync/push", request.Method, request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read upstream request: %v", err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		received <- string(body)
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"accepted":true}`))
	}))
	defer upstream.Close()

	coordinator := &QueueReplayCoordinator{upstream: upstream.URL}
	proxy := httptest.NewServer(coordinator)
	defer proxy.Close()
	coordinator.armResponseLossPush()
	defer func() { _ = coordinator.releaseResponseLossPush() }()

	requestBody := `{"client_id":"client-a","batch_id":"batch-a"}`
	request, err := http.NewRequest(http.MethodPost, proxy.URL+"/sync/push", strings.NewReader(requestBody))
	if err != nil {
		t.Fatalf("create initial proxy request: %v", err)
	}
	initialResult := make(chan error, 1)
	go func() {
		response, err := proxy.Client().Do(request)
		if response != nil {
			_ = response.Body.Close()
		}
		initialResult <- err
	}()
	if got := <-received; got != requestBody {
		t.Fatalf("committed push body = %q, want %q", got, requestBody)
	}
	contextWithTimeout, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.waitForResponseLossPush(contextWithTimeout); err != nil {
		t.Fatalf("wait for committed response-loss push: %v", err)
	}
	select {
	case err := <-initialResult:
		t.Fatalf("initial committed push completed before coordinator release: %v", err)
	default:
	}
	if err := coordinator.releaseResponseLossPush(); err != nil {
		t.Fatalf("release committed response-loss push: %v", err)
	}
	select {
	case err := <-initialResult:
		if err == nil {
			t.Fatal("initial committed push response was not dropped")
		}
		if !strings.Contains(err.Error(), "malformed HTTP") {
			t.Fatalf("initial committed push did not return an explicit malformed response: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("initial committed push did not complete after coordinator release")
	}

	replay, err := http.NewRequest(http.MethodPost, proxy.URL+"/sync/push", strings.NewReader(requestBody))
	if err != nil {
		t.Fatalf("create replay proxy request: %v", err)
	}
	response, err := proxy.Client().Do(replay)
	if err != nil {
		t.Fatalf("send replay push: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read replay response: %v", err)
	}
	if response.StatusCode != http.StatusOK || string(body) != `{"accepted":true}` {
		t.Fatalf("replay response status=%d body=%q, want status=200 body=%q", response.StatusCode, body, `{"accepted":true}`)
	}
	if got := <-received; got != requestBody {
		t.Fatalf("replayed push body = %q, want %q", got, requestBody)
	}
}

func TestQueueReplayStageCountMatchesDerivedCoordinatorStages(t *testing.T) {
	scenario := loadQueueReplayAuthoredScenario(t)
	workloads, err := queueReplayWorkloads(scenario)
	if err != nil {
		t.Fatalf("derive queue-replay workloads: %v", err)
	}
	coordinator := &QueueReplayCoordinator{steps: workloads}
	want := 4
	for _, workload := range workloads {
		want += len(workload.local) + 6
	}
	if actual := coordinator.StageCount(); actual != want {
		t.Fatalf("queue-replay coordinator stage count = %d, want %d", actual, want)
	}
}

func TestQueueReplayResponseLossUsesAnAsynchronousBlockedCall(t *testing.T) {
	coordinator := &QueueReplayCoordinator{
		clientKey: "client-a",
		stage:     queueReplayStageSchemaBoundary,
		steps:     []queueReplayWorkload{{}},
		process: &actionProcessIdentity{
			ProcessID:                   "process-a",
			DatabaseIdentityFingerprint: strings.Repeat("a", 64),
		},
	}
	response, err := coordinator.advanceLocked(context.Background(), 7)
	if err != nil {
		t.Fatalf("advance queue-replay response-loss begin: %v", err)
	}
	defer func() { _ = coordinator.releaseResponseLossPush() }()
	if coordinator.stage != queueReplayStageResponseLossBegun || response.Command == nil {
		t.Fatalf("queue-replay response-loss begin stage=%d command=%#v", coordinator.stage, response.Command)
	}
	action := response.Command.Action.Action
	if action.Actor != "client" || action.Command != "begin-call" || action.Parameters["method"] != "reset-schema-and-start" || action.Parameters["call_id"] != coordinator.responseLossCallID() {
		t.Fatalf("queue-replay response-loss begin action = %#v", action)
	}
	process := `{"process_id":"process-a","database_identity_fingerprint":"` + strings.Repeat("a", 64) + `"}`
	if err := coordinator.validateResponseLossCallBegun(json.RawMessage(`{"kind":"call-begun","call_id":"`+coordinator.responseLossCallID()+`","state":"in_flight","process":`+process+`}`)); err != nil {
		t.Fatalf("validate queue-replay response-loss call begin: %v", err)
	}
	blocked := json.RawMessage(`{"kind":"call-completed","call_id":"` + coordinator.responseLossCallID() + `","state":"completed","completion":"blocked","status":{"state":"backoff","retry_at":"2026-09-02T00:00:01Z","operation":"push","failure":null},"process":` + process + `}`)
	if err := coordinator.validateResponseLossCallCompleted(blocked); err != nil {
		t.Fatalf("validate queue-replay response-loss blocked call: %v", err)
	}
	forged := json.RawMessage(`{"kind":"call-completed","call_id":"` + coordinator.responseLossCallID() + `","state":"completed","completion":"blocked","status":{"state":"ready","retry_at":null,"operation":null,"failure":null},"process":` + process + `}`)
	if err := coordinator.validateResponseLossCallCompleted(forged); err == nil {
		t.Fatal("queue-replay response-loss accepted blocked completion without a backoff")
	} else if !strings.Contains(err.Error(), `state="ready" want="backoff"`) {
		t.Fatalf("queue-replay response-loss diagnostic = %q, want observed and expected backoff states", err)
	}
}

func TestNewQueueReplayCoordinatorKeepsAndroidSidecarOnHostLoopback(t *testing.T) {
	coordinator, err := NewQueueReplayCoordinator(QueueReplayCoordinatorConfig{
		Scenario: loadQueueReplayAuthoredScenario(t), Platform: "android", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token", AppVersion: "0.3.0",
	})
	if err != nil || coordinator == nil {
		t.Fatalf("Android queue-replay coordinator was rejected: %v", err)
	}
	defer func() {
		if err := coordinator.Close(context.Background()); err != nil {
			t.Errorf("close Android queue-replay coordinator: %v", err)
		}
	}()
	if !strings.HasPrefix(coordinator.URL(), "http://127.0.0.1:") {
		t.Fatalf("Android queue-replay coordinator URL = %q", coordinator.URL())
	}
	if !strings.HasPrefix(coordinator.adapter, "http://10.0.2.2:") {
		t.Fatalf("Android queue-replay adapter URL = %q", coordinator.adapter)
	}
	if coordinator.upstream != "http://127.0.0.1:8080" {
		t.Fatalf("Android queue-replay upstream URL = %q, want %q", coordinator.upstream, "http://127.0.0.1:8080")
	}
}

func loadQueueReplayAuthoredScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	scenario, err := LoadQueueReplayScenario(context.Background(), repoRoot)
	if err != nil {
		t.Fatalf("load authored queue-replay scenario: %v", err)
	}
	return scenario
}

func cloneQueueReplayScenario(scenario scenarios.Scenario) scenarios.Scenario {
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
