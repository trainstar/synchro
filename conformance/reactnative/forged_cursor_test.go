package reactnative

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
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

func TestForgedCursorCommandCarriesAuthoredPullPageSize(t *testing.T) {
	coordinator, err := NewForgedCursorCoordinator(ForgedCursorCoordinatorConfig{
		Scenario: loadForgedCursorAuthoredScenario(t), Platform: "ios", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create forged-cursor coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()

	command := coordinator.command("client", "open", map[string]any{"client_key": coordinator.clientKey}, nil)
	if command.Runtime.PullPageSize != 1 {
		t.Fatalf("forged-cursor runtime pull page size = %d, want authored size 1", command.Runtime.PullPageSize)
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
	twoRecords := []byte(`{"scope":"scope-a","records":[{"table":"items"},{"table":"items"}],"has_more":true,"cursor":"real-opaque-cursor"}`)
	_, err = mutateForgedCursorFirstResponse(twoRecords)
	if err == nil || !strings.Contains(err.Error(), "record count=2, want 1") || strings.Contains(err.Error(), "%!w") {
		t.Fatalf("forged-cursor two-record response error = %q, want exact count without nil wrapping", err)
	}
}

func TestForgedCursorProxyMutatesOnlyFirstRebuildPage(t *testing.T) {
	const firstUpstreamBody = `{"scope":"scope-a","records":[{"table":"items"}],"has_more":true,"cursor":"real-opaque-cursor"}`
	const rejectedBody = `{"error":{"code":"invalid_request","message":"invalid request","retryable":false}}`
	var requests atomic.Uint64
	forwardedLimits := make(chan uint64, 2)
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestNumber := requests.Add(1)
		var rebuild struct {
			Limit uint64 `json:"limit"`
		}
		if err := json.NewDecoder(request.Body).Decode(&rebuild); err != nil {
			forwardedLimits <- 0
		} else {
			forwardedLimits <- rebuild.Limit
		}
		writer.Header().Set("Content-Type", "application/json")
		if requestNumber == 1 {
			writer.WriteHeader(http.StatusOK)
			records := `[{"table":"items"},{"table":"items"}]`
			if rebuild.Limit == 1 {
				records = `[{"table":"items"}]`
			}
			_, _ = writer.Write([]byte(`{"scope":"scope-a","records":` + records + `,"has_more":true,"cursor":"real-opaque-cursor"}`))
			return
		}
		writer.WriteHeader(http.StatusBadRequest)
		_, _ = writer.Write([]byte(rejectedBody))
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
	coordinator.proxyAdapter(first, httptest.NewRequest(http.MethodPost, "/sync/rebuild", strings.NewReader(`{"client_id":"client-a","limit":100}`)))
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
	coordinator.proxyAdapter(second, httptest.NewRequest(http.MethodPost, "/sync/rebuild", strings.NewReader(`{"client_id":"client-a","limit":100}`)))
	if second.Code != http.StatusBadRequest || requests.Load() != 2 {
		t.Fatalf("forged-cursor rejected proxy status = %d requests = %d, want 400/2", second.Code, requests.Load())
	}
	if second.Body.String() != rejectedBody {
		t.Fatalf("forged-cursor rejected proxy body = %q, want unchanged upstream body %q", second.Body.String(), rejectedBody)
	}
	if firstLimit, secondLimit := <-forwardedLimits, <-forwardedLimits; firstLimit != 1 || secondLimit != 1 {
		t.Fatalf("forged-cursor forwarded rebuild limits = %d/%d, want authored 1/1", firstLimit, secondLimit)
	}
	if err := coordinator.waitForForgedPage(context.Background()); err != nil {
		t.Fatalf("wait for forged-cursor rejection: %v", err)
	}
	if err := coordinator.requireValidatedRejectedResponse(); err != nil {
		t.Fatalf("validate forged-cursor rejected wire response: %v", err)
	}
	diagnostic := coordinator.rebuildResponseDiagnostic()
	for _, want := range []string{
		fmt.Sprintf("{request:1 upstream_status:200 proxied_status:200 upstream_body:%q proxied_body:%q}", firstUpstreamBody, first.Body.String()),
		fmt.Sprintf("{request:2 upstream_status:400 proxied_status:400 upstream_body:%q proxied_body:%q}", rejectedBody, rejectedBody),
	} {
		if !strings.Contains(diagnostic, want) {
			t.Fatalf("forged-cursor rebuild response diagnostic = %q, want %q", diagnostic, want)
		}
	}
}

func TestValidateForgedCursorRejectedResponseRequiresAuthoredByteIdentity(t *testing.T) {
	scenario := loadForgedCursorAuthoredScenario(t)
	valid := []byte(`{"error": {"code": "invalid_request", "message": "invalid rebuild request", "retryable": false}}`)
	if err := validateForgedCursorRejectedResponse(scenario, http.StatusBadRequest, http.StatusBadRequest, valid, valid); err != nil {
		t.Fatalf("validate authored forged-cursor rejection: %v", err)
	}
	tests := []struct {
		name           string
		upstreamStatus int
		proxiedStatus  int
		upstreamBody   []byte
		proxiedBody    []byte
	}{
		{name: "changed bytes", upstreamStatus: http.StatusBadRequest, proxiedStatus: http.StatusBadRequest, upstreamBody: valid, proxiedBody: []byte(`{"error":{"code":"invalid_request","message":"invalid rebuild request","retryable":false}}`)},
		{name: "changed status", upstreamStatus: http.StatusBadRequest, proxiedStatus: http.StatusBadGateway, upstreamBody: valid, proxiedBody: valid},
		{name: "wrong code", upstreamStatus: http.StatusBadRequest, proxiedStatus: http.StatusBadRequest, upstreamBody: []byte(`{"error":{"code":"sync_integrity_failure","message":"invalid rebuild request","retryable":false}}`), proxiedBody: []byte(`{"error":{"code":"sync_integrity_failure","message":"invalid rebuild request","retryable":false}}`)},
		{name: "retryable", upstreamStatus: http.StatusBadRequest, proxiedStatus: http.StatusBadRequest, upstreamBody: []byte(`{"error":{"code":"invalid_request","message":"invalid rebuild request","retryable":true}}`), proxiedBody: []byte(`{"error":{"code":"invalid_request","message":"invalid rebuild request","retryable":true}}`)},
		{name: "missing message", upstreamStatus: http.StatusBadRequest, proxiedStatus: http.StatusBadRequest, upstreamBody: []byte(`{"error":{"code":"invalid_request","message":"","retryable":false}}`), proxiedBody: []byte(`{"error":{"code":"invalid_request","message":"","retryable":false}}`)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateForgedCursorRejectedResponse(scenario, test.upstreamStatus, test.proxiedStatus, test.upstreamBody, test.proxiedBody); err == nil {
				t.Fatal("changed forged-cursor rejection was accepted")
			}
		})
	}
}

func TestForgedCursorRebuildResponseDiagnosticBoundsBodies(t *testing.T) {
	coordinator := &ForgedCursorCoordinator{}
	upstreamBody := []byte(strings.Repeat("u", 513))
	proxiedBody := []byte(strings.Repeat("p", 514))
	coordinator.recordRebuildResponse(2, http.StatusBadRequest, http.StatusBadRequest, upstreamBody, proxiedBody)
	diagnostic := coordinator.rebuildResponseDiagnostic()
	for _, want := range []string{
		fmt.Sprintf("upstream_body:%q", boundedRaw(upstreamBody)),
		fmt.Sprintf("proxied_body:%q", boundedRaw(proxiedBody)),
	} {
		if !strings.Contains(diagnostic, want) {
			t.Fatalf("bounded forged-cursor rebuild response diagnostic = %q, want %q", diagnostic, want)
		}
	}
}

func TestForgedCursorCallCompleteRequiresValidatedWireResponse(t *testing.T) {
	coordinator, err := NewForgedCursorCoordinator(ForgedCursorCoordinatorConfig{
		Scenario: loadForgedCursorAuthoredScenario(t), Platform: "android", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create forged-cursor response diagnostic coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	upstreamBody := []byte(`{"error":{"code":"invalid_request","message":"invalid request","retryable":false}}`)
	coordinator.recordRebuildResponse(2, http.StatusBadRequest, http.StatusBadRequest, upstreamBody, upstreamBody)
	process := actionProcessIdentity{ProcessID: "process-a", DatabaseIdentityFingerprint: strings.Repeat("a", 64)}
	coordinator.process = &process
	raw := json.RawMessage(`{"kind":"call-completed","call_id":"forged_rebuild","state":"completed","completion":"error","status":{"state":"error","retry_at":null,"operation":null,"failure":{"operation":"rebuilding","code":"server_error","retryable":false,"recovery_action":"retry"}},"process":{"process_id":"process-a","database_identity_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}`)
	err = coordinator.validateCallComplete(raw)
	if err == nil || !strings.Contains(err.Error(), "lacks authored wire proof") {
		t.Fatalf("forged-cursor call without wire proof error=%q, want missing-proof failure", err)
	}
	coordinator.markRejectedResponseValidated()
	if err := coordinator.validateCallComplete(raw); err != nil {
		t.Fatalf("validate Android forged-cursor call with authored wire proof: %v", err)
	}
}

func TestValidateForgedCursorErrorStatusFollowsNativeAuthority(t *testing.T) {
	scenario := loadForgedCursorAuthoredScenario(t)
	tests := []struct {
		platform string
		code     string
	}{
		{platform: "ios", code: "invalid_request"},
		{platform: "android", code: "server_error"},
	}
	for _, test := range tests {
		t.Run(test.platform, func(t *testing.T) {
			status := json.RawMessage(fmt.Sprintf(`{"state":"error","retry_at":null,"operation":null,"failure":{"operation":"rebuilding","code":%q,"retryable":false,"recovery_action":"retry"}}`, test.code))
			if err := validateForgedCursorErrorStatus(scenario, test.platform, status); err != nil {
				t.Fatalf("validate %s forged-cursor native failure: %v", test.platform, err)
			}
			activeOperation := json.RawMessage(fmt.Sprintf(`{"state":"error","retry_at":null,"operation":"rebuilding","failure":{"operation":"rebuilding","code":%q,"retryable":false,"recovery_action":"retry"}}`, test.code))
			if err := validateForgedCursorErrorStatus(scenario, test.platform, activeOperation); err == nil {
				t.Fatal("forged-cursor error status with an active operation was accepted")
			}
			wrongFailureOperation := json.RawMessage(fmt.Sprintf(`{"state":"error","retry_at":null,"operation":null,"failure":{"operation":"rebuild","code":%q,"retryable":false,"recovery_action":"retry"}}`, test.code))
			if err := validateForgedCursorErrorStatus(scenario, test.platform, wrongFailureOperation); err == nil {
				t.Fatal("forged-cursor non-native failure operation was accepted")
			}
			wrongCode := json.RawMessage(`{"state":"error","retry_at":null,"operation":null,"failure":{"operation":"rebuilding","code":"invalid_response","retryable":false,"recovery_action":"retry"}}`)
			if err := validateForgedCursorErrorStatus(scenario, test.platform, wrongCode); err == nil {
				t.Fatal("forged-cursor non-native lifecycle code was accepted")
			}
		})
	}
}

func TestValidateForgedCursorUnfinishedReceiptRequiresIncompleteProof(t *testing.T) {
	expected := forgedCursorExpectedState(loadForgedCursorAuthoredScenario(t))
	if expected == nil || len(expected.Rebuilds) != 1 {
		t.Fatal("authored forged-cursor rebuild is unavailable")
	}
	rebuild := expected.Rebuilds[0]
	receipt := rebuildReceiptProof{
		RebuildIDFingerprint:    hashFingerprint(rebuild.RebuildID),
		PageCount:               rebuild.PageCount,
		ReturnedRecordCount:     rebuild.PageCount,
		RequestChainValid:       false,
		RecordsInCanonicalOrder: true,
		RowChecksumsValid:       true,
		ScopeChecksumValid:      false,
		FinalChecksumMatches:    false,
	}
	if err := validateForgedCursorUnfinishedReceipt(receipt, rebuild.RebuildID, rebuild.PageCount); err != nil {
		t.Fatalf("validate incomplete forged-cursor receipt: %v", err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*rebuildReceiptProof)
	}{
		{name: "completed chain", mutate: func(receipt *rebuildReceiptProof) { receipt.RequestChainValid = true }},
		{name: "scope checksum", mutate: func(receipt *rebuildReceiptProof) { receipt.ScopeChecksumValid = true }},
		{name: "local final checksum", mutate: func(receipt *rebuildReceiptProof) { receipt.FinalChecksumMatches = true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := receipt
			test.mutate(&changed)
			if err := validateForgedCursorUnfinishedReceipt(changed, rebuild.RebuildID, rebuild.PageCount); err == nil {
				t.Fatal("changed incomplete forged-cursor receipt was accepted")
			}
		})
	}
}

func TestForgedCursorProxyMatchesDirectConnectAndRelaysUpstreamResponse(t *testing.T) {
	const requestBody = `{"client_id":"client-a","client_generation":1,"platform":"android"}`
	longErrorBody := strings.Repeat("x", 513)
	tests := []struct {
		name           string
		upstreamStatus int
		upstreamBody   string
		chunked        bool
		wantProxyError bool
	}{
		{name: "success", upstreamStatus: http.StatusOK, upstreamBody: `{"client_generation":1}`},
		{name: "adapter error", upstreamStatus: http.StatusBadRequest, upstreamBody: `{"error": {"code": "invalid_request", "message": "connect contains an unknown scope", "retryable": false}}`, wantProxyError: true},
		{name: "chunked trailer and close", upstreamStatus: http.StatusOK, upstreamBody: `{"client_generation":1}`, chunked: true},
		{name: "bounded diagnostic", upstreamStatus: http.StatusBadGateway, upstreamBody: longErrorBody, wantProxyError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			type observedRequest struct {
				method            string
				requestURI        string
				host              string
				header            http.Header
				body              []byte
				contentLength     int64
				transferEncodings []string
				trailer           http.Header
				close             bool
				readErr           error
			}
			observations := make(chan observedRequest, 2)
			upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				raw, err := io.ReadAll(request.Body)
				observations <- observedRequest{
					method: request.Method, requestURI: request.URL.RequestURI(), host: request.Host,
					header: request.Header.Clone(), body: raw, contentLength: request.ContentLength,
					transferEncodings: append([]string(nil), request.TransferEncoding...), trailer: request.Trailer.Clone(),
					close: request.Close, readErr: err,
				}
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(test.upstreamStatus)
				_, _ = writer.Write([]byte(test.upstreamBody))
			}))
			defer upstream.Close()

			coordinator, err := NewForgedCursorCoordinator(ForgedCursorCoordinatorConfig{
				Scenario: loadForgedCursorAuthoredScenario(t), Platform: "ios", ServerURL: upstream.URL, AuthToken: "unit-token",
			})
			if err != nil {
				t.Fatalf("create forged-cursor connect proxy: %v", err)
			}
			defer func() { _ = coordinator.Close(context.Background()) }()

			send := func(target string) (int, string) {
				request, err := http.NewRequest(http.MethodPost, target+"/sync/connect", strings.NewReader(requestBody))
				if err != nil {
					t.Fatalf("create forged-cursor connect request: %v", err)
				}
				request.Header = http.Header{
					"Accept-Encoding": {"gzip"},
					"Authorization":   {"Bearer unit-token"},
					"Connection":      {"keep-alive"},
					"Content-Type":    {"application/json"},
					"User-Agent":      {"okhttp/4.12.0"},
					"X-App-Version":   {"0.3.0"},
				}
				if test.chunked {
					request.ContentLength = -1
					request.TransferEncoding = []string{"chunked"}
					request.Trailer = http.Header{"X-Body-Checksum": {"sha256:unit"}}
					request.Close = true
				}
				response, err := http.DefaultClient.Do(request)
				if err != nil {
					t.Fatalf("execute forged-cursor connect request: %v", err)
				}
				defer response.Body.Close()
				body, err := io.ReadAll(response.Body)
				if err != nil {
					t.Fatalf("read forged-cursor connect response: %v", err)
				}
				return response.StatusCode, string(body)
			}

			directStatus, directBody := send(upstream.URL)
			direct := <-observations
			proxy := httptest.NewServer(coordinator)
			defer proxy.Close()
			proxyStatus, proxyBody := send(proxy.URL)
			proxied := <-observations

			if directStatus != test.upstreamStatus || proxyStatus != directStatus {
				t.Fatalf("forged-cursor connect response status direct=%d proxy=%d want=%d", directStatus, proxyStatus, test.upstreamStatus)
			}
			if directBody != test.upstreamBody || proxyBody != directBody {
				t.Fatalf("forged-cursor connect response body direct=%q proxy=%q want=%q", directBody, proxyBody, test.upstreamBody)
			}
			if direct.readErr != nil || proxied.readErr != nil {
				t.Fatalf("read forged-cursor connect body direct=%v proxy=%v", direct.readErr, proxied.readErr)
			}
			if !reflect.DeepEqual(proxied, direct) {
				t.Fatalf("proxied forged-cursor connect differs from direct request:\nproxy=%#v\ndirect=%#v", proxied, direct)
			}
			proxyErr := coordinator.proxyFailure("connect")
			if test.wantProxyError {
				if proxyErr == nil {
					t.Fatalf("forged-cursor connect proxy status=%d want recorded error", test.upstreamStatus)
				}
				diagnosticBody := test.upstreamBody
				if len(diagnosticBody) > 512 {
					diagnosticBody = diagnosticBody[:512] + fmt.Sprintf("...(%d bytes)", len(test.upstreamBody))
				}
				want := fmt.Sprintf("connect upstream status=%d want=200 response_bytes=%d response_body=%q", test.upstreamStatus, len(test.upstreamBody), diagnosticBody)
				if !strings.Contains(proxyErr.Error(), want) {
					t.Fatalf("forged-cursor connect proxy error=%q want substring=%q", proxyErr, want)
				}
			} else if proxyErr != nil {
				t.Fatalf("forged-cursor connect proxy error=%v want=nil", proxyErr)
			}
		})
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
