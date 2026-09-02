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

func TestQueueReplaySynchronizedResultDiagnosticsNameOnlyFailedField(t *testing.T) {
	const process = `"process":{"process_id":"process-a","database_identity_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`
	const status = `"status":{"state":"ready","retry_at":null,"operation":null,"failure":null}`
	tests := []struct {
		name      string
		raw       string
		want      string
		forbidden []string
	}{
		{
			name:      "members",
			raw:       `{"kind":"synchronized","completion":"blocked","status":{"state":"ready","retry_at":null,"operation":null,"failure":null},"process":{"process_id":"process-a","database_identity_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"extra":true}`,
			want:      "members",
			forbidden: []string{"kind=", "completion=", "status=", "process_id="},
		},
		{
			name:      "kind",
			raw:       `{"kind":"other","completion":"blocked",` + status + `,` + process + `}`,
			want:      `kind="other" want="synchronized"`,
			forbidden: []string{"completion=", "status=", "process_id="},
		},
		{
			name:      "completion",
			raw:       `{"kind":"synchronized","completion":"idle",` + status + `,` + process + `}`,
			want:      `completion="idle" want="blocked"`,
			forbidden: []string{"kind=", "status=", "process_id="},
		},
		{
			name:      "status",
			raw:       `{"kind":"synchronized","completion":"blocked","status":{"state":""},` + process + `}`,
			want:      "status",
			forbidden: []string{"kind=", "completion=", "process_id="},
		},
		{
			name:      "process id",
			raw:       `{"kind":"synchronized","completion":"blocked",` + status + `,"process":{"process_id":"process-b","database_identity_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}`,
			want:      `process_id="process-b" want="process-a"`,
			forbidden: []string{"kind=", "completion=", "status=", "database_identity_fingerprint="},
		},
		{
			name:      "database identity fingerprint",
			raw:       `{"kind":"synchronized","completion":"blocked",` + status + `,"process":{"process_id":"process-a","database_identity_fingerprint":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}}`,
			want:      `database_identity_fingerprint="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" want="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"`,
			forbidden: []string{"kind=", "completion=", "status=", "process_id="},
		},
	}
	coordinator := &QueueReplayCoordinator{process: &actionProcessIdentity{
		ProcessID:                   "process-a",
		DatabaseIdentityFingerprint: strings.Repeat("a", 64),
	}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := coordinator.validateSynchronized(json.RawMessage(test.raw), "blocked")
			if err == nil {
				t.Fatal("invalid synchronized result was accepted")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("synchronized diagnostic = %q, want detail %q", err, test.want)
			}
			for _, detail := range test.forbidden {
				if strings.Contains(err.Error(), detail) {
					t.Fatalf("synchronized diagnostic = %q, must not name %q", err, detail)
				}
			}
		})
	}
}

func TestQueueReplayProxyForwardsCommittedPushAndReplay(t *testing.T) {
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

	requestBody := `{"client_id":"client-a","batch_id":"batch-a"}`
	request, err := http.NewRequest(http.MethodPost, proxy.URL+"/sync/push", strings.NewReader(requestBody))
	if err != nil {
		t.Fatalf("create initial proxy request: %v", err)
	}
	response, err := proxy.Client().Do(request)
	if err != nil {
		t.Fatalf("send committed push: %v", err)
	}
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatalf("read committed push response: %v", err)
	}
	if response.StatusCode != http.StatusOK || string(body) != `{"accepted":true}` {
		t.Fatalf("committed push response status=%d body=%q, want status=200 body=%q", response.StatusCode, body, `{"accepted":true}`)
	}
	if got := <-received; got != requestBody {
		t.Fatalf("committed push body = %q, want %q", got, requestBody)
	}

	replay, err := http.NewRequest(http.MethodPost, proxy.URL+"/sync/push", strings.NewReader(requestBody))
	if err != nil {
		t.Fatalf("create replay proxy request: %v", err)
	}
	response, err = proxy.Client().Do(replay)
	if err != nil {
		t.Fatalf("send replay push: %v", err)
	}
	body, err = io.ReadAll(response.Body)
	_ = response.Body.Close()
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
		want += len(workload.local) + 5
	}
	if actual := coordinator.StageCount(); actual != want {
		t.Fatalf("queue-replay coordinator stage count = %d, want %d", actual, want)
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
