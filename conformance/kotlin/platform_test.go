package kotlin

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestPlatformConfigDefaultsAndBoundsPushBatchSize(t *testing.T) {
	config := Config{
		ADBPath:                  os.Args[0],
		DeviceSerial:             "emulator-5554",
		ApplicationAPKPath:       writeFixture(t, "application.apk"),
		InstrumentationAPKPath:   writeFixture(t, "instrumentation.apk"),
		ApplicationID:            "com.trainstar.synchro.conformance",
		InstrumentationComponent: "com.trainstar.test/androidx.test.runner.AndroidJUnitRunner",
		ServerURL:                "http://127.0.0.1:8090",
		AuthToken:                func(context.Context, Client) (string, error) { return "token", nil },
		Platform:                 "android",
		AppVersion:               "0.3.0",
	}
	normalized, err := normalizePlatformConfig(config)
	if err != nil {
		t.Fatalf("normalize default push batch size: %v", err)
	}
	if normalized.PushBatchSize != 100 {
		t.Fatalf("default push batch size = %d, want 100", normalized.PushBatchSize)
	}
	config.PushBatchSize = 1000
	if normalized, err = normalizePlatformConfig(config); err != nil || normalized.PushBatchSize != 1000 {
		t.Fatalf("normalize maximum push batch size: %d, %v", normalized.PushBatchSize, err)
	}
	config.PushBatchSize = 1001
	if _, err := normalizePlatformConfig(config); err == nil {
		t.Fatal("oversized push batch size passed platform validation")
	}
}

func TestAdapterReversePortSelectsOnlyLoopbackServers(t *testing.T) {
	tests := []struct {
		serverURL string
		port      int
		required  bool
	}{
		{serverURL: "http://127.0.0.1:8090", port: 8090, required: true},
		{serverURL: "http://localhost", port: 80, required: true},
		{serverURL: "https://[::1]", port: 443, required: true},
		{serverURL: "https://adapter.example.test:8443", required: false},
	}
	for _, test := range tests {
		port, required, err := adapterReversePort(test.serverURL)
		if err != nil {
			t.Fatalf("select adapter reverse for %q: %v", test.serverURL, err)
		}
		if port != test.port || required != test.required {
			t.Errorf("adapter reverse for %q = %d, %t", test.serverURL, port, required)
		}
	}
}

func TestPlatformTemporaryUnavailablePushFaultIsTargetedAndHeldUntilRelease(t *testing.T) {
	upstreamCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		upstreamCalls++
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"upstream":true}`))
	}))
	defer upstream.Close()
	platform, err := NewPlatform(Config{
		ADBPath:                  os.Args[0],
		DeviceSerial:             "emulator-5554",
		ApplicationAPKPath:       writeFixture(t, "application.apk"),
		InstrumentationAPKPath:   writeFixture(t, "instrumentation.apk"),
		ApplicationID:            "com.trainstar.synchro.conformance",
		InstrumentationComponent: "com.trainstar.test/androidx.test.runner.AndroidJUnitRunner",
		ServerURL:                upstream.URL,
		AuthToken:                func(context.Context, Client) (string, error) { return "token", nil },
		Platform:                 "android",
		AppVersion:               "0.3.0",
	})
	if err != nil {
		t.Fatalf("create Kotlin Android platform: %v", err)
	}
	defer func() {
		if err := platform.Close(context.Background()); err != nil {
			t.Fatalf("close Kotlin Android platform: %v", err)
		}
	}()
	operation := scenarios.Operation{
		ContractOperation: "push",
		Name:              "submit",
		Payload:           temporaryUnavailablePushDispatchPayload(),
		WireFault:         &scenarios.WireFaultControl{Mode: "temporary_unavailable"},
	}
	release, armed, err := platform.armTemporaryUnavailablePush([]scenarios.Operation{operation})
	if err != nil || !armed {
		t.Fatalf("arm temporary unavailable push fault: armed %t, error %v", armed, err)
	}
	defer release()

	postRaw := func(payload string) (*http.Response, []byte) {
		t.Helper()
		response, err := http.Post(platform.config.ServerURL+"/sync/push", "application/json", strings.NewReader(payload))
		if err != nil {
			t.Fatalf("post proxied push: %v", err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatalf("read proxied push response: %v", err)
		}
		return response, body
	}
	post := func(clientID, batchID string) (*http.Response, []byte) {
		t.Helper()
		return postRaw(`{"client_id":"` + clientID + `","batch_id":"` + batchID + `"}`)
	}

	response, _ := postRaw(`{"client_id":"client-a"}`)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("malformed non-targeted push status = %d, want 200", response.StatusCode)
	}
	response, _ = post("client-b", "batch-a")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("non-targeted push status = %d, want 200", response.StatusCode)
	}
	response, body := post("client-a", "runtime-batch-a")
	if response.StatusCode != http.StatusServiceUnavailable || response.Header.Get("Retry-After") != "5" || !strings.Contains(string(body), `"temporary_unavailable"`) {
		t.Fatalf("temporary unavailable response = status %d, retry-after %q, body %q", response.StatusCode, response.Header.Get("Retry-After"), body)
	}
	response, body = post("client-a", "runtime-batch-a")
	if response.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(body), `"temporary_unavailable"`) {
		t.Fatalf("retried push result = status %d, body %q", response.StatusCode, body)
	}
	if upstreamCalls != 2 {
		t.Fatalf("armed fault forwarded a targeted push upstream, calls = %d", upstreamCalls)
	}
	release()
	response, _ = post("client-a", "runtime-batch-a")
	if response.StatusCode != http.StatusOK || upstreamCalls != 3 {
		t.Fatalf("released push result = status %d, upstream calls %d", response.StatusCode, upstreamCalls)
	}
}

func TestPlatformRecordsProxiedRebuildContinuationFingerprint(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		request.Body.Close()
		if err != nil || string(body) != `{"client_id":"client-a"}` {
			t.Errorf("proxied rebuild request body = %q, %v", body, err)
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"cursor":"continuation-a","has_more":true}`))
	}))
	defer upstream.Close()
	platform, err := NewPlatform(Config{
		ADBPath:                  os.Args[0],
		DeviceSerial:             "emulator-5554",
		ApplicationAPKPath:       writeFixture(t, "application.apk"),
		InstrumentationAPKPath:   writeFixture(t, "instrumentation.apk"),
		ApplicationID:            "com.trainstar.synchro.conformance",
		InstrumentationComponent: "com.trainstar.test/androidx.test.runner.AndroidJUnitRunner",
		ServerURL:                upstream.URL,
		AuthToken:                func(context.Context, Client) (string, error) { return "token", nil },
		Platform:                 "android",
		AppVersion:               "0.3.0",
	})
	if err != nil {
		t.Fatalf("create Kotlin Android platform: %v", err)
	}
	defer func() {
		if err := platform.Close(context.Background()); err != nil {
			t.Fatalf("close Kotlin Android platform: %v", err)
		}
	}()
	response, err := http.Post(platform.config.ServerURL+"/sync/rebuild", "application/json", strings.NewReader(`{"client_id":"client-a"}`))
	if err != nil {
		t.Fatalf("post proxied rebuild: %v", err)
	}
	body, readErr := io.ReadAll(response.Body)
	response.Body.Close()
	if readErr != nil || response.StatusCode != http.StatusOK || string(body) != `{"cursor":"continuation-a","has_more":true}` {
		t.Fatalf("proxied rebuild response = status %d, body %q, error %v", response.StatusCode, body, readErr)
	}
	platform.mu.Lock()
	observed := platform.rebuildResponseCursors["client-a"]
	platform.mu.Unlock()
	if observed != cursorFingerprint("continuation-a") {
		t.Fatalf("proxied rebuild cursor fingerprint = %q", observed)
	}
}

func TestClientOperationClassesRouteToDirectHandlers(t *testing.T) {
	tests := []struct {
		operation scenarios.Operation
		want      string
	}{
		{operation: scenarios.Operation{ContractOperation: "local", Name: "write"}, want: "apply"},
		{operation: scenarios.Operation{ContractOperation: "connect", Name: "send"}, want: "request"},
		{operation: scenarios.Operation{ContractOperation: "pull", Name: "request-page"}, want: "request"},
		{operation: scenarios.Operation{ContractOperation: "process", Name: "restart-client"}, want: "process"},
		{operation: scenarios.Operation{ContractOperation: "model", Name: "publish-schema"}, want: ""},
	}
	for _, test := range tests {
		if got := dispatchOperation(test.operation); got != test.want {
			t.Errorf("dispatch for %s/%s = %q, want %q", test.operation.ContractOperation, test.operation.Name, got, test.want)
		}
	}
}

func TestDurableFactsRejectUnknownMembers(t *testing.T) {
	facts := []byte(`[{"scope_id":"scope-a","cursor":null,"checksum":null,"generation":1,"local_checksum":"","unknown":true}]`)
	if _, err := androidCheckpointFacts(facts); err == nil {
		t.Fatal("unknown durable fact member passed strict parsing")
	}
}

func TestAggregateCountsProduceFactsWithoutDetailedRecords(t *testing.T) {
	thousand := 1000
	result := Result{ApplicationRowCount: &thousand, ProvenanceCount: &thousand}
	client := &platformClient{client: Client{UserID: "user-a", ClientID: "client-a"}}
	application, err := androidClientFactsForSource("application-rows", client, result)
	if err != nil || application.RowCount == nil || *application.RowCount != 1000 {
		t.Fatalf("aggregate application facts = %+v, %v", application, err)
	}
	provenance, err := androidClientFactsForSource("provenance", client, result)
	if err != nil || provenance.ProvenanceCount == nil || *provenance.ProvenanceCount != 1000 || len(provenance.Provenance) != 0 {
		t.Fatalf("aggregate provenance facts = %+v, %v", provenance, err)
	}
}

func TestOperationWindowReportsMaintenanceCursorDelta(t *testing.T) {
	beforeCursor := int64(3)
	afterCursor := int64(5)
	client := platformClient{maintenanceCursor: beforeCursor}
	before := Result{ProvenanceMaintenanceWorkCursor: &beforeCursor}
	after := Result{ProvenanceMaintenanceWorkCursor: &afterCursor}
	work, err := client.maintenanceWorkDelta(before, after)
	if err != nil {
		t.Fatalf("maintenance delta failed: %v", err)
	}
	if work != 2 {
		t.Fatalf("maintenance delta = %d, want 2", work)
	}
	if _, err := client.maintenanceWorkDelta(after, before); err == nil {
		t.Fatal("backward maintenance cursor passed validation")
	}
}

func TestOperationWindowDurationReachesPublicResults(t *testing.T) {
	window := operationWindow{duration: 3 * time.Nanosecond}
	if got := synchronizationResult("idle", nil, window).DurationNanoseconds; got != 3 {
		t.Fatalf("synchronization duration = %d, want 3", got)
	}
	if got := clientCallResultWithWindow(ClientCallResult{}, window).DurationNanoseconds; got != 3 {
		t.Fatalf("client call duration = %d, want 3", got)
	}
	if got := observationWithWindow(StepObservation{}, window).DurationNanoseconds; got != 3 {
		t.Fatalf("step duration = %d, want 3", got)
	}
}

func TestTypedValuesRequireMatchingJSONPrimitiveTypes(t *testing.T) {
	valid := []TypedValue{
		{Type: "null", Value: nil},
		{Type: "string", Value: "value"},
		{Type: "bytes", Value: "AQI"},
		{Type: "boolean", Value: true},
		{Type: "integer", Value: json.Number("42")},
		{Type: "double", Value: json.Number("4.2")},
	}
	for _, value := range valid {
		if !validTypedValue(value, true) {
			t.Errorf("valid typed value %q was rejected", value.Type)
		}
	}

	invalid := []TypedValue{
		{Type: "string", Value: true},
		{Type: "boolean", Value: "true"},
		{Type: "integer", Value: "42"},
		{Type: "double", Value: "4.2"},
		{Type: "bytes", Value: true},
	}
	for _, value := range invalid {
		if validTypedValue(value, true) {
			t.Errorf("mismatched typed value %q passed validation", value.Type)
		}
	}
}

func TestTransportObservationsRequireCompleteOperationFacts(t *testing.T) {
	retryable := false
	generation := int64(1)
	mutationCount := 1
	valid := TransportObservation{
		Sequence:            1,
		OperationClass:      "push",
		StatusCode:          200,
		Retryable:           &retryable,
		DurationNanoseconds: 1,
		RequestFacts: &TransportRequestFacts{
			ClientGeneration: &generation,
			SchemaVersion:    1,
			SchemaHash:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			MutationCount:    &mutationCount,
		},
	}
	if err := validateTransportObservation(valid); err != nil {
		t.Fatalf("valid push observation failed: %v", err)
	}

	invalid := valid
	invalid.Retryable = nil
	if err := validateTransportObservation(invalid); err == nil {
		t.Fatal("push observation without retryability passed")
	}
	invalid = valid
	invalid.RequestFacts = nil
	if err := validateTransportObservation(invalid); err == nil {
		t.Fatal("push observation without request facts passed")
	}
	invalid = valid
	invalid.StatusCode = 503
	retryable = true
	invalid.Retryable = &retryable
	if err := validateTransportObservation(invalid); err == nil {
		t.Fatal("HTTP failure without canonical error code passed")
	}
	errorCode := "temporary_unavailable"
	invalid.ErrorCode = &errorCode
	if err := validateTransportObservation(invalid); err != nil {
		t.Fatalf("canonical retryable failure failed: %v", err)
	}
}

func TestMappedTransportObservationPreservesServerReportedErrorCode(t *testing.T) {
	retryable := true
	generation := int64(1)
	mutationCount := 1
	errorCode := "temporary_unavailable"
	observation := TransportObservation{
		Sequence:            1,
		OperationClass:      "push",
		StatusCode:          503,
		ErrorCode:           &errorCode,
		Retryable:           &retryable,
		DurationNanoseconds: 1,
		RequestFacts: &TransportRequestFacts{
			ClientGeneration: &generation,
			SchemaVersion:    1,
			SchemaHash:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			MutationCount:    &mutationCount,
		},
	}

	mapped, err := mapTransportObservation(observation)
	if err != nil {
		t.Fatalf("map transport observation: %v", err)
	}
	if mapped.Wire == nil || mapped.Wire.ErrorCode == nil || *mapped.Wire.ErrorCode != errorCode {
		t.Fatalf("mapped transport error code = %#v", mapped.Wire)
	}
}

func TestRetainedMutationDecodesSealedBatchID(t *testing.T) {
	raw := json.RawMessage(`[{"mutation_id":"mutation-a","local_order":0,"table_id":"items","table_name":"items","record_id":"record-a","primary_key_field_id":"id","primary_key_logical_type":"string","operation":"insert","authored_schema":{"version":1,"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"base_version":null,"client_version":"client-version","status":"sealed","sealed_batch_id":"batch-a","authored_fields":[]}]`)
	var values []retainedMutation
	if err := decodeFactArray(raw, &values, maximumRecords); err != nil {
		t.Fatalf("decode retained mutation: %v", err)
	}
	if len(values) != 1 || values[0].SealedBatchID == nil || *values[0].SealedBatchID != "batch-a" {
		t.Fatalf("decoded sealed batch ID = %#v", values)
	}
}

func TestAuthoredRequestFactsMustMatchObservedRequest(t *testing.T) {
	retryable := false
	protocolVersion := 3
	scopeSetVersion := int64(0)
	scopeCount := 0
	operation := scenarios.Operation{
		ContractOperation: "connect",
		Name:              "send",
		Payload:           json.RawMessage(`{"protocol_version":3,"client_generation":null,"schema":{"version":0,"hash":""},"scope_set_version":0,"known_scopes":[]}`),
	}
	observation := TransportObservation{
		Sequence:            1,
		OperationClass:      "connect",
		StatusCode:          200,
		Retryable:           &retryable,
		DurationNanoseconds: 1,
		RequestFacts: &TransportRequestFacts{
			ProtocolVersion: &protocolVersion,
			ScopeSetVersion: &scopeSetVersion,
			ScopeCount:      &scopeCount,
		},
	}
	if err := validateOperationTransportFacts(operation, observation); err != nil {
		t.Fatalf("matching connect facts failed: %v", err)
	}
	protocolVersion = 2
	if err := validateOperationTransportFacts(operation, observation); err == nil {
		t.Fatal("mismatched connect protocol version passed")
	}
}

func TestCursorSourcesBindToExactDurableFingerprints(t *testing.T) {
	checkpoint := "checkpoint-a"
	pull := scenarios.Operation{
		ContractOperation: "pull",
		Name:              "request-page",
		Payload:           json.RawMessage(`{"scopes":[{"scope_id":"scope-a","cursor_source":"local_checkpoint"}]}`),
	}
	complete := true
	pullObservation := TransportObservation{
		CursorFingerprints:         []string{cursorFingerprint(checkpoint)},
		CursorFingerprintsComplete: &complete,
	}
	source := Result{ScopeStates: json.RawMessage(`[{"scope_id":"user:user-a","cursor":"` + checkpoint + `","checksum":null,"generation":1,"local_checksum":""}]`)}
	if err := validateCursorSourceBinding(pull, pullObservation, source, nil); err != nil {
		t.Fatalf("bind pull checkpoint cursor: %v", err)
	}
	pullObservation.CursorFingerprints[0] = cursorFingerprint("different")
	if err := validateCursorSourceBinding(pull, pullObservation, source, nil); err == nil {
		t.Fatal("mismatched pull checkpoint cursor passed")
	}

	continuation := "continuation-a"
	rebuildID := "00000000-0000-4000-8000-000000000001"
	rebuild := scenarios.Operation{
		ContractOperation: "rebuild",
		Name:              "request-page",
		Payload:           json.RawMessage(`{"rebuild_id":"` + rebuildID + `","cursor_source":"local_rebuild_continuation"}`),
	}
	present := true
	rebuildObservation := TransportObservation{RequestFacts: &TransportRequestFacts{CursorPresent: &present, CursorFingerprint: pointer(cursorFingerprint(continuation))}}
	source = Result{RebuildAttempts: json.RawMessage(`[{"scope_id":"user:user-a","rebuild_id":"` + rebuildID + `","client_generation":1,"schema_version":1,"schema_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","generation":1,"cursor":"` + continuation + `","page_limit":1}]`)}
	if err := validateCursorSourceBinding(rebuild, rebuildObservation, source, nil); err != nil {
		t.Fatalf("bind rebuild continuation cursor: %v", err)
	}
	rebuildObservation.RequestFacts.CursorFingerprint = pointer(cursorFingerprint("different"))
	if err := validateCursorSourceBinding(rebuild, rebuildObservation, source, nil); err == nil {
		t.Fatal("mismatched rebuild continuation cursor passed")
	}
}

func TestGroupedPullBindsToPrecedingTerminalRebuildCursor(t *testing.T) {
	scopeCursor := "rebuilt-checkpoint"
	retryable := false
	complete := true
	generation := int64(1)
	scopeSetVersion := int64(1)
	scopeCount := 1
	limit := 100
	schemaHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	rebuildID := "00000000-0000-4000-8000-000000000001"
	operations := []scenarios.Operation{
		{
			ContractOperation: "rebuild",
			Name:              "request-page",
			Payload:           json.RawMessage(`{"client_generation":1,"schema":{"version":1,"hash":"` + schemaHash + `"},"scope_id":"scope-a","rebuild_id":"` + rebuildID + `","cursor_source":"none","limit":100}`),
		},
		{
			ContractOperation: "pull",
			Name:              "request-page",
			Payload:           json.RawMessage(`{"client_generation":1,"schema":{"version":1,"hash":"` + schemaHash + `"},"scope_set_version":1,"scopes":[{"scope_id":"scope-a","cursor_source":"local_checkpoint"}],"limit":100}`),
		},
	}
	rebuildFingerprint := cursorFingerprint("00000000-0000-4000-8000-000000000002")
	terminalFingerprint := cursorFingerprint(scopeCursor)
	scopeFingerprint := cursorFingerprint("scope-a")
	present := false
	observations := []TransportObservation{
		{
			Sequence:            1,
			OperationClass:      "rebuild",
			StatusCode:          200,
			Retryable:           &retryable,
			DurationNanoseconds: 1,
			RequestFacts: &TransportRequestFacts{
				ClientGeneration:     &generation,
				SchemaVersion:        1,
				SchemaHash:           schemaHash,
				Limit:                &limit,
				ScopeFingerprint:     &scopeFingerprint,
				RebuildIDFingerprint: &rebuildFingerprint,
				CursorPresent:        &present,
			},
			RebuildResponseFacts: &TransportRebuildResponseFacts{
				HasFinalScopeCursor:         true,
				HasChecksum:                 true,
				ScopeFingerprint:            scopeFingerprint,
				FinalScopeCursorFingerprint: &terminalFingerprint,
			},
		},
		{
			Sequence:                   2,
			OperationClass:             "pull",
			StatusCode:                 200,
			Retryable:                  &retryable,
			DurationNanoseconds:        1,
			CursorFingerprints:         []string{terminalFingerprint},
			CursorFingerprintsComplete: &complete,
			RequestFacts: &TransportRequestFacts{
				ClientGeneration: &generation,
				SchemaVersion:    1,
				SchemaHash:       schemaHash,
				ScopeSetVersion:  &scopeSetVersion,
				ScopeCount:       &scopeCount,
				Limit:            &limit,
			},
			PullResponseFacts: &TransportPullResponseFacts{
				ChecksumCount:                   1,
				ScopeCursorFingerprints:         []string{cursorFingerprint("next")},
				ScopeCursorFingerprintsComplete: true,
			},
		},
	}
	if _, err := mapTransportOperations(operations, observations, Result{}); err != nil {
		t.Fatalf("bind grouped rebuild checkpoint: %v", err)
	}

	observations[1].CursorFingerprints[0] = cursorFingerprint("different")
	if _, err := mapTransportOperations(operations, observations, Result{}); err == nil {
		t.Fatal("pull cursor unrelated to preceding rebuild passed")
	}
}

func TestTerminalRebuildResponseRequiresValidCursorFingerprint(t *testing.T) {
	retryable := false
	fingerprint := cursorFingerprint("terminal-cursor")
	generation := int64(1)
	limit := 1
	present := false
	rebuildIDFingerprint := cursorFingerprint("rebuild-id")
	scopeFingerprint := cursorFingerprint("scope-a")
	observation := TransportObservation{
		Sequence:            1,
		OperationClass:      "rebuild",
		StatusCode:          200,
		Retryable:           &retryable,
		DurationNanoseconds: 1,
		RequestFacts: &TransportRequestFacts{
			ClientGeneration:     &generation,
			SchemaVersion:        1,
			SchemaHash:           "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Limit:                &limit,
			ScopeFingerprint:     &scopeFingerprint,
			RebuildIDFingerprint: &rebuildIDFingerprint,
			CursorPresent:        &present,
		},
		RebuildResponseFacts: &TransportRebuildResponseFacts{
			HasFinalScopeCursor:         true,
			ScopeFingerprint:            scopeFingerprint,
			FinalScopeCursorFingerprint: &fingerprint,
		},
	}
	if err := validateTransportObservation(observation); err != nil {
		t.Fatalf("valid terminal rebuild observation failed: %v", err)
	}
	observation.RebuildResponseFacts.FinalScopeCursorFingerprint = nil
	if err := validateTransportObservation(observation); err == nil {
		t.Fatal("terminal rebuild without cursor fingerprint passed")
	}
	observation.RebuildResponseFacts.FinalScopeCursorFingerprint = pointer("invalid")
	if err := validateTransportObservation(observation); err == nil {
		t.Fatal("terminal rebuild with invalid cursor fingerprint passed")
	}
}

func TestForgedCursorBindsToDeterministicTransportOverride(t *testing.T) {
	present := true
	operation := scenarios.Operation{
		ContractOperation: "rebuild",
		Name:              "request-page",
		Payload:           json.RawMessage(`{"rebuild_id":"00000000-0000-4000-8000-000000000001","cursor_source":"forged"}`),
	}
	observation := TransportObservation{RequestFacts: &TransportRequestFacts{CursorPresent: &present, CursorFingerprint: pointer(cursorFingerprint(forgedRebuildCursor))}}
	if err := validateCursorSourceBinding(operation, observation, Result{}, nil); err != nil {
		t.Fatalf("bind deterministic forged rebuild cursor: %v", err)
	}
	observation.RequestFacts.CursorFingerprint = pointer(cursorFingerprint("different"))
	if err := validateCursorSourceBinding(operation, observation, Result{}, nil); err == nil {
		t.Fatal("mismatched forged rebuild cursor passed")
	}
}

func pointer[T any](value T) *T {
	return &value
}

func temporaryUnavailablePushDispatchPayload() json.RawMessage {
	return json.RawMessage(`{
		"authenticated_user_id":"user-a",
		"request":{"client_id":"client-a","client_generation":1,"batch_id":"batch-a","schema":{"version":1,"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"mutations":[]},
		"delivery":"transport_failure","commit_lsn":"1","end_lsn":"2"
	}`)
}
