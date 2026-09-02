package reactnative

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestValidateForgedCursorScenarioAcceptsAuthoredContract(t *testing.T) {
	if err := ValidateForgedCursorScenario(loadForgedCursorAuthoredScenario(t)); err != nil {
		t.Fatalf("validate authored forged-cursor scenario: %v", err)
	}
}

func TestValidateForgedCursorScenarioRejectsContractChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*scenarios.Scenario)
	}{
		{"step order", func(scenario *scenarios.Scenario) {
			scenario.Steps[0], scenario.Steps[1] = scenario.Steps[1], scenario.Steps[0]
		}},
		{"forged cursor source", func(scenario *scenarios.Scenario) {
			var payload map[string]any
			if err := json.Unmarshal(scenario.Steps[5].Operation.Payload, &payload); err != nil {
				panic(err)
			}
			payload["cursor_source"] = "local_rebuild_continuation"
			scenario.Steps[5].Operation.Payload, _ = json.Marshal(payload)
		}},
		{"wire status", func(scenario *scenarios.Scenario) {
			scenario.WireExpectations[2].HTTPStatus = http.StatusOK
		}},
		{"identity kind", func(scenario *scenarios.Scenario) {
			scenario.NativeIdentityAliases[0].Kind = "batch-id"
		}},
		{"Android proof target", func(scenario *scenarios.Scenario) {
			for index := range scenario.ProofObligations {
				if string(scenario.ProofObligations[index].ObligationID) == "OBL-REBUILD-FORGED-CURSOR-RN-ANDROID-CURRENT-001" {
					scenario.ProofObligations[index].MakeTarget = "test-rn-forged-cursor-android"
				}
			}
		}},
		{"assertion oracle", func(scenario *scenarios.Scenario) {
			scenario.Assertions[0].Oracle.ExpectedSource = "system-under-test"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := cloneForgedCursorScenario(loadForgedCursorAuthoredScenario(t))
			test.mutate(&scenario)
			if err := ValidateForgedCursorScenario(scenario); err == nil {
				t.Fatal("changed forged-cursor contract was accepted")
			}
		})
	}
}

func TestNewForgedCursorCoordinatorUsesHostLoopbackProxy(t *testing.T) {
	upstream := httptest.NewServer(http.NotFoundHandler())
	defer upstream.Close()
	coordinator, err := NewForgedCursorCoordinator(ForgedCursorCoordinatorConfig{
		Scenario: loadForgedCursorAuthoredScenario(t), Platform: "android", ServerURL: upstream.URL, AuthToken: "unit-token", AppVersion: "0.3.0",
	})
	if err != nil {
		t.Fatalf("create Android forged-cursor coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	if !strings.HasPrefix(coordinator.URL(), "http://127.0.0.1:") {
		t.Fatalf("Android forged-cursor coordinator URL = %q", coordinator.URL())
	}
	if !strings.HasPrefix(coordinator.adapter, "http://10.0.2.2:") {
		t.Fatalf("Android forged-cursor adapter URL = %q", coordinator.adapter)
	}
	if coordinator.upstream != upstream.URL {
		t.Fatalf("forged-cursor upstream URL = %q, want %q", coordinator.upstream, upstream.URL)
	}
	if coordinator.clientKey != coordinator.serverClient.ClientID {
		t.Fatalf("forged-cursor client key = %q, want authored client %q", coordinator.clientKey, coordinator.serverClient.ClientID)
	}
	if coordinator.ExchangeCount() != 9 {
		t.Fatalf("forged-cursor exchange count = %d, want 9", coordinator.ExchangeCount())
	}
}

func TestForgedCursorCommandEncodesOnlyAuthoredSteps(t *testing.T) {
	coordinator, err := NewForgedCursorCoordinator(ForgedCursorCoordinatorConfig{
		Scenario: loadForgedCursorAuthoredScenario(t), Platform: "ios", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create forged-cursor coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	empty := coordinator.command("client", "open", map[string]any{"client_key": coordinator.clientKey}, nil)
	if empty.Action.Steps == nil || len(empty.Action.Steps) != 0 {
		t.Fatalf("forged-cursor empty command steps = %#v", empty.Action.Steps)
	}
	tests := []struct {
		name          string
		stepID        scenarios.StepID
		wantOperation string
	}{
		{"first insert", forgedCursorStepOrder[0], "local/write"},
		{"second insert", forgedCursorStepOrder[1], "local/write"},
		{"push start", forgedCursorStepOrder[2], "push/submit"},
		{"first rebuild page", forgedCursorStepOrder[4], "rebuild/request-page"},
		{"forged rebuild page", forgedCursorStepOrder[5], "rebuild/request-page"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := coordinator.command("observer", "await-step", map[string]any{"client_key": coordinator.clientKey}, []scenarios.StepID{test.stepID})
			if len(command.Action.Steps) != 1 {
				t.Fatalf("forged-cursor command step count = %d, want 1", len(command.Action.Steps))
			}
			operation := command.Action.Steps[0].Operation
			if operation.ContractOperation+"/"+operation.Name != test.wantOperation {
				t.Fatalf("forged-cursor command operation = %q/%q, want %q", operation.ContractOperation, operation.Name, test.wantOperation)
			}
		})
	}
}

func TestMutateForgedCursorFirstResponseInstallsDeterministicCursor(t *testing.T) {
	raw := []byte(`{"scope":"scope-a","records":[{"table":"items"}],"has_more":true,"cursor":"real-opaque-cursor"}`)
	mutated, err := mutateForgedCursorFirstResponse(raw)
	if err != nil {
		t.Fatalf("mutate forged-cursor first response: %v", err)
	}
	var response struct {
		Cursor string `json:"cursor"`
	}
	if err := json.Unmarshal(mutated, &response); err != nil {
		t.Fatalf("decode mutated forged-cursor response: %v", err)
	}
	if response.Cursor != forgedCursorOverride || hashFingerprint(response.Cursor) != hashFingerprint(forgedCursorOverride) {
		t.Fatalf("mutated forged-cursor response fingerprint = %q, want deterministic override", hashFingerprint(response.Cursor))
	}
	terminal := []byte(`{"scope":"scope-a","records":[{"table":"items"}],"has_more":false,"final_scope_cursor":"terminal","checksum":{}}`)
	if _, err := mutateForgedCursorFirstResponse(terminal); err == nil {
		t.Fatal("terminal rebuild response accepted as the forged-cursor first page")
	}
}

func TestForgedCursorProxyMutatesOnlyFirstRebuildPage(t *testing.T) {
	var requests atomic.Uint64
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestNumber := requests.Add(1)
		writer.Header().Set("Content-Type", "application/json")
		if requestNumber == 1 {
			writer.WriteHeader(http.StatusOK)
			_, _ = writer.Write([]byte(`{"scope":"scope-a","records":[{"table":"items"}],"has_more":true,"cursor":"real-opaque-cursor"}`))
			return
		}
		writer.WriteHeader(http.StatusBadRequest)
		_, _ = writer.Write([]byte(`{"error":{"code":"invalid_request","message":"invalid request","retryable":false}}`))
	}))
	defer upstream.Close()
	coordinator, err := NewForgedCursorCoordinator(ForgedCursorCoordinatorConfig{
		Scenario: loadForgedCursorAuthoredScenario(t), Platform: "ios", ServerURL: upstream.URL, AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create forged-cursor proxy coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()

	first := httptest.NewRecorder()
	coordinator.proxyAdapter(first, httptest.NewRequest(http.MethodPost, "/sync/rebuild", strings.NewReader(`{}`)))
	if first.Code != http.StatusOK {
		t.Fatalf("forged-cursor first proxy status = %d, want %d", first.Code, http.StatusOK)
	}
	var firstBody struct {
		Cursor string `json:"cursor"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &firstBody); err != nil || firstBody.Cursor != forgedCursorOverride {
		t.Fatalf("forged-cursor first proxy cursor = %q, want deterministic override: %v", firstBody.Cursor, err)
	}
	if err := coordinator.waitForFirstPage(context.Background()); err != nil {
		t.Fatalf("wait for forged-cursor first proxy page: %v", err)
	}

	coordinator.releaseForgedPage()
	second := httptest.NewRecorder()
	coordinator.proxyAdapter(second, httptest.NewRequest(http.MethodPost, "/sync/rebuild", strings.NewReader(`{}`)))
	if second.Code != http.StatusBadRequest || requests.Load() != 2 {
		t.Fatalf("forged-cursor rejected proxy status = %d requests = %d, want 400/2", second.Code, requests.Load())
	}
	if err := coordinator.waitForForgedPage(context.Background()); err != nil {
		t.Fatalf("wait for forged-cursor rejection: %v", err)
	}
}

func TestForgedCursorProxyHoldsPushUntilMaterializationBarrier(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte(`{"results":[]}`))
	}))
	defer upstream.Close()
	coordinator, err := NewForgedCursorCoordinator(ForgedCursorCoordinatorConfig{
		Scenario: loadForgedCursorAuthoredScenario(t), Platform: "ios", ServerURL: upstream.URL, AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create forged-cursor push proxy coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()

	done := make(chan struct{})
	response := httptest.NewRecorder()
	go func() {
		coordinator.proxyAdapter(response, httptest.NewRequest(http.MethodPost, "/sync/push", strings.NewReader(`{}`)))
		close(done)
	}()
	if err := coordinator.waitForPushCommit(context.Background()); err != nil {
		t.Fatalf("wait for forged-cursor push commit: %v", err)
	}
	select {
	case <-done:
		t.Fatal("forged-cursor push response crossed the materialization barrier")
	default:
	}
	coordinator.releasePushResponse()
	<-done
	if response.Code != http.StatusOK {
		t.Fatalf("forged-cursor push proxy status = %d, want %d", response.Code, http.StatusOK)
	}
}

func TestForgedCursorPushTimeoutAwaitsInFlightCall(t *testing.T) {
	coordinator, err := NewForgedCursorCoordinator(ForgedCursorCoordinatorConfig{
		Scenario: loadForgedCursorAuthoredScenario(t), Platform: "ios", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create forged-cursor timeout coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	coordinator.stage = forgedCursorStageCallBegun
	deadline, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()

	response, err := coordinator.advanceLocked(deadline, 5)
	if err != nil {
		t.Fatalf("advance forged-cursor push timeout: %v", err)
	}
	if coordinator.stage != forgedCursorStagePushTimeoutDiagnostic || response.Command == nil {
		t.Fatalf("forged-cursor push timeout stage=%d command=%v, want diagnostic await-call", coordinator.stage, response.Command)
	}
	action := response.Command.Action.Action
	if action.Actor != "client" || action.Command != "await-call" || len(response.Command.Action.Steps) != 0 ||
		action.Parameters["client_key"] != coordinator.clientKey || action.Parameters["call_id"] != coordinator.callID {
		t.Fatalf("forged-cursor push timeout command=%+v steps=%d", action, len(response.Command.Action.Steps))
	}
	select {
	case <-coordinator.allowPushResponse:
	default:
		t.Fatal("forged-cursor push timeout did not release a late push response")
	}
}

func TestForgedCursorPushTimeoutReportsCallCompletion(t *testing.T) {
	coordinator, err := NewForgedCursorCoordinator(ForgedCursorCoordinatorConfig{
		Scenario: loadForgedCursorAuthoredScenario(t), Platform: "ios", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create forged-cursor diagnostic coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	coordinator.stage = forgedCursorStagePushTimeoutDiagnostic
	raw := json.RawMessage(`{"schema_version":1,"outcome":"passed","result":{"kind":"call-completed","call_id":"forged_rebuild","state":"completed","completion":"error","status":{"state":"error","retry_at":null,"operation":"rebuild","failure":{"operation":"rebuild","code":"invalid_request","retryable":false,"recovery_action":"restart"}},"process":{"process_id":"process-a","database_identity_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},"error_code":null,"error_detail":null}`)

	err = coordinator.acceptLocked(raw)
	if err == nil {
		t.Fatal("forged-cursor push timeout accepted a completed call")
	}
	for _, want := range []string{`completion="error"`, `status={"state":"error"`, "error_detail=<none>"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("forged-cursor push timeout diagnostic = %q, want %q", err, want)
		}
	}
}

func TestForgedCursorPushTimeoutReportsCallErrorDetail(t *testing.T) {
	coordinator, err := NewForgedCursorCoordinator(ForgedCursorCoordinatorConfig{
		Scenario: loadForgedCursorAuthoredScenario(t), Platform: "ios", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create forged-cursor error diagnostic coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	coordinator.stage = forgedCursorStagePushTimeoutDiagnostic
	raw := json.RawMessage(`{"schema_version":1,"outcome":"error","result":null,"error_code":"execution_failed","error_detail":"sync did not complete within 30000 ms, last status local_ready"}`)

	err = coordinator.acceptLocked(raw)
	if err == nil {
		t.Fatal("forged-cursor push timeout accepted a failed call")
	}
	for _, want := range []string{"completion=unavailable", "status=unavailable", `error_code="execution_failed"`, `error_detail="sync did not complete within 30000 ms, last status local_ready"`} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("forged-cursor push timeout error diagnostic = %q, want %q", err, want)
		}
	}
}

func TestValidateForgedCursorServerFreezeRejectsStateChange(t *testing.T) {
	count := uint64(1)
	before := scenarios.StateFacts{RebuildCount: &count, Rebuilds: []scenarios.RebuildFact{{RebuildID: "runtime-rebuild", PageCount: 1, Status: "staged"}}}
	if err := validateForgedCursorServerFreeze(before, scenarios.CloneStateFacts(before)); err != nil {
		t.Fatalf("unchanged forged-cursor server state was rejected: %v", err)
	}
	after := scenarios.CloneStateFacts(before)
	after.Rebuilds[0].PageCount++
	if err := validateForgedCursorServerFreeze(before, after); err == nil {
		t.Fatal("changed forged-cursor server state was accepted")
	}
}

func loadForgedCursorAuthoredScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	scenario, err := LoadForgedCursorScenario(context.Background(), repositoryRoot)
	if err != nil {
		t.Fatalf("load authored forged-cursor scenario: %v", err)
	}
	return scenario
}

func cloneForgedCursorScenario(scenario scenarios.Scenario) scenarios.Scenario {
	encoded, err := json.Marshal(scenario)
	if err != nil {
		panic(err)
	}
	var clone scenarios.Scenario
	if err := json.Unmarshal(encoded, &clone); err != nil {
		panic(err)
	}
	return clone
}
