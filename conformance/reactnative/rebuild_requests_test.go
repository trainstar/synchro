package reactnative

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestValidateRebuildRequestsScenarioAcceptsAuthoredContract(t *testing.T) {
	if err := ValidateRebuildRequestsScenario(loadRebuildRequestsAuthoredScenario(t)); err != nil {
		t.Fatalf("validate authored rebuild-requests scenario: %v", err)
	}
}

func TestValidateRebuildRequestsScenarioRejectsContractChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*scenarios.Scenario)
	}{
		{"step order", func(scenario *scenarios.Scenario) {
			scenario.Steps[0], scenario.Steps[1] = scenario.Steps[1], scenario.Steps[0]
		}},
		{"call identity", func(scenario *scenarios.Scenario) {
			callID := scenarios.NativeCallID("other-call")
			scenario.Steps[3].NativeBinding.CallID = &callID
		}},
		{"Android proof target", func(scenario *scenarios.Scenario) {
			for index := range scenario.ProofObligations {
				if string(scenario.ProofObligations[index].ObligationID) == "OBL-PERF-REBUILD-REQUESTS-RN-ANDROID-CURRENT-001" {
					scenario.ProofObligations[index].MakeTarget = "test-rn-rebuild-requests-android"
				}
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := cloneRebuildRequestsScenario(loadRebuildRequestsAuthoredScenario(t))
			test.mutate(&scenario)
			if err := ValidateRebuildRequestsScenario(scenario); err == nil {
				t.Fatal("changed rebuild-requests contract was accepted")
			}
		})
	}
}

func TestNewRebuildRequestsCoordinatorKeepsAndroidSidecarOnHostLoopback(t *testing.T) {
	coordinator, err := NewRebuildRequestsCoordinator(RebuildRequestsCoordinatorConfig{
		Scenario: loadRebuildRequestsAuthoredScenario(t), Platform: "android",
		ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token", AppVersion: "0.3.0",
	})
	if err != nil || coordinator == nil {
		t.Fatalf("Android rebuild-requests coordinator was rejected: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	if !strings.HasPrefix(coordinator.URL(), "http://127.0.0.1:") {
		t.Fatalf("Android rebuild-requests coordinator URL = %q", coordinator.URL())
	}
	if !strings.HasPrefix(coordinator.adapter, "http://10.0.2.2:") {
		t.Fatalf("Android rebuild-requests adapter URL = %q", coordinator.adapter)
	}
	if got, want := coordinator.ExchangeCount(), 9; got != want {
		t.Fatalf("rebuild-requests exchange count = %d, want %d", got, want)
	}
}

func TestRebuildRequestsStagesOnePublicStepPerCommand(t *testing.T) {
	coordinator, err := NewRebuildRequestsCoordinator(RebuildRequestsCoordinatorConfig{
		Scenario: loadRebuildRequestsAuthoredScenario(t), Platform: "ios", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create rebuild-requests coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()

	tests := []struct {
		stage  rebuildRequestsStage
		actor  string
		name   string
		stepID scenarios.StepID
	}{
		{rebuildRequestsStageBegin, "client", "begin-call", rebuildRequestsStepOrder[3]},
		{rebuildRequestsStageFirstPage, "observer", "await-step", rebuildRequestsStepOrder[5]},
		{rebuildRequestsStageFinalPage, "observer", "await-step", rebuildRequestsStepOrder[9]},
		{rebuildRequestsStagePull, "observer", "await-step", rebuildRequestsStepOrder[12]},
	}
	for _, test := range tests {
		t.Run(string(test.stepID), func(t *testing.T) {
			coordinator.stage = test.stage
			response, err := coordinator.advanceLocked(context.Background(), 1)
			if err != nil {
				t.Fatalf("advance rebuild-requests stage: %v", err)
			}
			if response.Command.Action.Action.Actor != test.actor ||
				response.Command.Action.Action.Command != test.name {
				t.Fatalf("rebuild-requests command = %q/%q, want %q/%q", response.Command.Action.Action.Actor, response.Command.Action.Action.Command, test.actor, test.name)
			}
			if len(response.Command.Action.Steps) != 1 {
				t.Fatalf("rebuild-requests command step count = %d, want 1", len(response.Command.Action.Steps))
			}
			operation := response.Command.Action.Steps[0].Operation
			if operation.ContractOperation != coordinator.steps[test.stepID].Operation.ContractOperation ||
				operation.Name != coordinator.steps[test.stepID].Operation.Name ||
				!bytes.Equal(operation.Payload, coordinator.steps[test.stepID].Operation.Payload) {
				t.Fatalf("rebuild-requests command step = %#v, want %s", operation, test.stepID)
			}
		})
	}

	coordinator.stage = rebuildRequestsStageAwaitCall
	response, err := coordinator.advanceLocked(context.Background(), 1)
	if err != nil {
		t.Fatalf("advance rebuild-requests await-call: %v", err)
	}
	if response.Command.Action.Action.Actor != "client" ||
		response.Command.Action.Action.Command != "await-call" ||
		response.Command.Action.Steps == nil ||
		len(response.Command.Action.Steps) != 0 {
		t.Fatalf("rebuild-requests await-call command = %#v", response.Command.Action)
	}
}

func TestRebuildRequestsAuthoredFlowMatchesExchangeCount(t *testing.T) {
	coordinator, err := NewRebuildRequestsCoordinator(RebuildRequestsCoordinatorConfig{
		Scenario: loadRebuildRequestsAuthoredScenario(t), Platform: "ios", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create rebuild-requests coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	coordinator.prepared = true
	for _, record := range []string{"row-a-primary-key", "row-b-primary-key", "row-c-primary-key"} {
		coordinator.runtimeIDs[record] = json.RawMessage(`"` + strings.TrimSuffix(record, "-primary-key") + `"`)
	}

	expectedCommands := map[rebuildRequestsStage][2]string{
		rebuildRequestsStageOpen:            {"client", "open"},
		rebuildRequestsStageBegin:           {"client", "begin-call"},
		rebuildRequestsStageFirstPage:       {"observer", "await-step"},
		rebuildRequestsStageFinalPage:       {"observer", "await-step"},
		rebuildRequestsStagePull:            {"observer", "await-step"},
		rebuildRequestsStageAwaitCall:       {"client", "await-call"},
		rebuildRequestsStageFinalCapture:    {"observer", "capture"},
		rebuildRequestsStageApplicationRows: {"observer", "capture"},
	}
	commands := 0
	for coordinator.stage != rebuildRequestsStageComplete {
		stage := coordinator.stage
		exchangeBody := rebuildRequestsExchangeBodyForTest(uint64(commands+1), rebuildRequestsResultForStageForTest(coordinator))
		response := exchangeRebuildRequestsRequestForTest(coordinator, exchangeBody)
		if response.Code != http.StatusOK {
			_, coordinatorErr := coordinator.Result()
			t.Fatalf("rebuild-requests exchange %d at stage %s status = %d, want %d: %v", commands+1, stage, response.Code, http.StatusOK, coordinatorErr)
		}
		var next exchangeResponse
		if err := json.Unmarshal(response.Body.Bytes(), &next); err != nil {
			t.Fatalf("decode rebuild-requests exchange %d response: %v", commands+1, err)
		}
		if next.State != "command" || next.Command == nil {
			t.Fatalf("rebuild-requests exchange %d at stage %s returned %#v, want command", commands+1, stage, next)
		}
		wanted := expectedCommands[stage]
		actual := next.Command.Action.Action
		if actual.Actor != wanted[0] || actual.Command != wanted[1] {
			t.Fatalf("rebuild-requests exchange %d at stage %s command = %q/%q, want %q/%q", commands+1, stage, actual.Actor, actual.Command, wanted[0], wanted[1])
		}
		commands++
	}

	if got, want := coordinator.ExchangeCount(), commands+1; got != want {
		t.Fatalf("rebuild-requests full-flow exchange count = %d, want command walk plus terminal exchange %d", got, want)
	}
	if got, want := coordinator.ExchangeCount(), 9; got != want {
		t.Fatalf("rebuild-requests ExchangeCount = %d, want authored e2e stage count %d", got, want)
	}
	if got, want := coordinator.nextSeq-1, uint64(commands); got != want {
		t.Fatalf("rebuild-requests exchanges served = %d, want %d command exchanges", got, want)
	}
	terminal := exchangeRebuildRequestsRequestForTest(coordinator, rebuildRequestsExchangeBodyForTest(uint64(commands+1), rebuildRequestsResultForStageForTest(coordinator)))
	if terminal.Code != http.StatusUnprocessableEntity {
		t.Fatalf("rebuild-requests terminal exchange status = %d, want %d", terminal.Code, http.StatusUnprocessableEntity)
	}
	_, terminalErr := coordinator.Result()
	if terminalErr == nil || !strings.Contains(terminalErr.Error(), "current stage=complete") || !strings.Contains(terminalErr.Error(), "exchanges served=8") || !strings.Contains(terminalErr.Error(), "ExchangeCount=9") {
		t.Fatalf("rebuild-requests terminal error = %v, want current stage, served exchanges, and ExchangeCount", terminalErr)
	}
}

func TestRebuildRequestsIncompleteResultNamesServedAndExpectedExchanges(t *testing.T) {
	coordinator, err := NewRebuildRequestsCoordinator(RebuildRequestsCoordinatorConfig{
		Scenario: loadRebuildRequestsAuthoredScenario(t), Platform: "ios", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create rebuild-requests coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	coordinator.stage = rebuildRequestsStageComplete
	coordinator.nextSeq = 9

	_, err = coordinator.Result()
	if err == nil || !strings.Contains(err.Error(), "current stage=complete") || !strings.Contains(err.Error(), "exchanges served=8") || !strings.Contains(err.Error(), "ExchangeCount=9") {
		t.Fatalf("incomplete rebuild-requests error = %v, want current stage, served exchanges, and ExchangeCount", err)
	}
}

func TestRebuildRequestsFailedResultNamesServedAndExpectedExchanges(t *testing.T) {
	coordinator, err := NewRebuildRequestsCoordinator(RebuildRequestsCoordinatorConfig{
		Scenario: loadRebuildRequestsAuthoredScenario(t), Platform: "ios", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create rebuild-requests coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	coordinator.failed = fmt.Errorf("terminal validation failed")
	coordinator.stage = rebuildRequestsStageComplete
	coordinator.nextSeq = 9

	_, err = coordinator.Result()
	if err == nil || !strings.Contains(err.Error(), "terminal validation failed") || !strings.Contains(err.Error(), "current stage=complete") || !strings.Contains(err.Error(), "exchanges served=8") || !strings.Contains(err.Error(), "ExchangeCount=9") {
		t.Fatalf("failed rebuild-requests error = %v, want cause, current stage, served exchanges, and ExchangeCount", err)
	}
}

func TestRebuildRequestsCommandEncodesEmptyStepsAsArray(t *testing.T) {
	scenario := loadRebuildRequestsAuthoredScenario(t)
	coordinator, err := NewRebuildRequestsCoordinator(RebuildRequestsCoordinatorConfig{
		Scenario: scenario, Platform: "ios", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create rebuild-requests coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	command := coordinator.command("client", "open", map[string]any{"client_key": clientKey}, nil)
	if command.Action.Steps == nil || len(command.Action.Steps) != 0 {
		t.Fatalf("rebuild-requests empty command steps = %#v", command.Action.Steps)
	}
}

func TestRebuildRequestsExchangeDiagnosticNamesGuardValues(t *testing.T) {
	coordinator, err := NewRebuildRequestsCoordinator(RebuildRequestsCoordinatorConfig{
		Scenario: loadRebuildRequestsAuthoredScenario(t), Platform: "android", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create rebuild-requests coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	coordinator.prepared = true
	response := exchangeRebuildRequestsRequestForTest(coordinator, []byte(`{"schema_version":1,"sequence":2,"result":null}`))
	if response.Code != http.StatusConflict {
		t.Fatalf("out-of-order rebuild-requests exchange status = %d, want %d", response.Code, http.StatusConflict)
	}
	_, err = coordinator.Result()
	if err == nil {
		t.Fatal("out-of-order rebuild-requests exchange did not preserve coordinator failure")
	}
	for _, value := range []string{
		"closed=false", "prepared=true", "completed=false", "got sequence=2", "want sequence=1",
	} {
		if !strings.Contains(err.Error(), value) {
			t.Fatalf("rebuild-requests diagnostic = %q, want it to contain %q", err, value)
		}
	}
}

func TestRebuildRequestsStageResultKindsMatchRunner(t *testing.T) {
	coordinator, err := NewRebuildRequestsCoordinator(RebuildRequestsCoordinatorConfig{
		Scenario: loadRebuildRequestsAuthoredScenario(t), Platform: "ios", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create rebuild-requests coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	digest := strings.Repeat("a", 64)
	coordinator.process = &actionProcessIdentity{ProcessID: "process-a", DatabaseIdentityFingerprint: digest}
	process := `{"process_id":"process-a","database_identity_fingerprint":"` + digest + `"}`
	callBegun := resultEnvelopeForTest(map[string]any{
		"kind": "call-begun", "call_id": coordinator.callID, "state": "in_flight", "process": json.RawMessage(process),
	})
	awaited := resultEnvelopeForTest(map[string]any{
		"kind": "awaited", "status": json.RawMessage(`{"state":"ready","retry_at":null,"operation":null,"failure":null}`), "process": json.RawMessage(process),
	})
	tests := []struct {
		stage    rebuildRequestsStage
		result   json.RawMessage
		wantKind string
		wantErr  bool
	}{
		{rebuildRequestsStageFirstPage, callBegun, "call-begun", false},
		{rebuildRequestsStageFinalPage, awaited, "awaited", false},
		{rebuildRequestsStagePull, awaited, "awaited", false},
		{rebuildRequestsStageAwaitCall, awaited, "awaited", false},
		{rebuildRequestsStageAwaitCall, callBegun, "call-begun", true},
	}
	for _, test := range tests {
		t.Run(test.wantKind+"-"+stageNameForTest(test.stage), func(t *testing.T) {
			coordinator.stage = test.stage
			err := coordinator.acceptResultLocked(test.result)
			if test.wantErr && err == nil {
				t.Fatalf("stage %s accepted observed result kind %q, want rejection", stageNameForTest(test.stage), test.wantKind)
			}
			if !test.wantErr && err != nil {
				t.Fatalf("stage %s rejected observed result kind %q: %v", stageNameForTest(test.stage), test.wantKind, err)
			}
		})
	}
}

func TestValidateFirstRebuildResponseRequiresIntermediatePage(t *testing.T) {
	valid := []byte(`{"scope":"runtime-scope","records":[{"table":"runtime-items","pk":{},"row":{},"row_checksum":{},"server_version":"v1"}],"has_more":true,"cursor":"cursor-1"}`)
	if err := validateFirstRebuildResponse(valid); err != nil {
		t.Fatalf("validate intermediate rebuild response: %v", err)
	}
	terminal := []byte(`{"scope":"runtime-scope","records":[{},{}],"has_more":false,"final_scope_cursor":"cursor-2","checksum":{}}`)
	err := validateFirstRebuildResponse(terminal)
	if err == nil {
		t.Fatal("terminal rebuild response was accepted as the first page")
	}
	for _, fact := range []string{
		"members=5", "records=2", "has_more=false", "cursor=absent", "final_scope_cursor=nonempty", "checksum=present",
	} {
		if !strings.Contains(err.Error(), fact) {
			t.Fatalf("terminal rebuild response diagnostic = %q, want it to contain %q", err, fact)
		}
	}
}

func TestValidateRebuildRequestsTransportAcceptsAdvancedIncrementalPullCursor(t *testing.T) {
	transport := traceSnapshot{Observations: rebuildRequestsTransportForTest(), SequenceCheckpoint: 4}
	if err := validateRebuildRequestsTransport(loadRebuildRequestsAuthoredScenario(t), transport); err != nil {
		t.Fatalf("incremental pull with an advanced response cursor was rejected: %v", err)
	}
}

func TestValidateRebuildRequestsTransportRequiresOneIncrementalPullResponseCursor(t *testing.T) {
	transport := traceSnapshot{Observations: rebuildRequestsTransportForTest(), SequenceCheckpoint: 4}
	transport.Observations[3].PullResponseFacts = json.RawMessage(`{"change_count":1,"has_more":false,"rebuild_scope_count":0,"checksum_count":1,"scope_cursor_fingerprints":[],"scope_cursor_fingerprints_complete":true}`)
	err := validateRebuildRequestsTransport(loadRebuildRequestsAuthoredScenario(t), transport)
	if err == nil {
		t.Fatal("incremental pull without a response cursor was accepted")
	}
	for _, fact := range []string{
		"first.client_generation=1",
		"pull.client_generation=1",
		"first.schema_version=1",
		"pull.schema_version=1",
		`first.schema_hash="cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"`,
		`pull.schema_hash="cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"`,
		"pull.scope_set_version=1",
		"pull.scope_count=1",
		"pull.limit=1",
		`final.final_scope_cursor_fingerprint="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"`,
		"pull.cursor_fingerprints_complete=true",
		"pull.cursor_fingerprints=[aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa]",
		"pull_response.change_count=1",
		"pull_response.has_more=false",
		"pull_response.rebuild_scope_count=0",
		"pull_response.checksum_count=1",
		"pull_response.scope_cursor_fingerprints_complete=true",
		"pull_response.scope_cursor_fingerprints=[]",
	} {
		if !strings.Contains(err.Error(), fact) {
			t.Fatalf("incremental pull diagnostic = %q, want it to contain %q", err, fact)
		}
	}
}

func TestValidateRebuildRequestsDurableCountsNamesEveryCheckedValue(t *testing.T) {
	state := rebuildRequestsDurableStateForTest()
	state.RebuildReceiptCount = 1
	err := validateRebuildRequestsDurableCounts(state)
	if err == nil {
		t.Fatal("one-page rebuild receipt count was accepted")
	}
	for _, value := range []string{
		"application_row_count=3 want=3",
		"mutation_ledger_count=0 want=0",
		"mutation_outcome_count=0 want=0",
		"sealed_batch_count=0 want=0",
		"rejected_mutation_count=0 want=0",
		"scope_state_count=1 want=1",
		"scope_row_count=3 want=3",
		"provenance_count=3 want=3",
		"row_metadata_count=3 want=3",
		"rebuild_attempt_count=0 want=0",
		"rebuild_receipt_count=1 want=2",
		"scope_state_detail_count=1 want=1",
		"scope_row_detail_count=3 want=3",
	} {
		if !strings.Contains(err.Error(), value) {
			t.Fatalf("durable-count diagnostic = %q, want it to contain %q", err, value)
		}
	}
}

func TestValidateRebuildRequestsDurableCountsAcceptsTwoPageReceipts(t *testing.T) {
	if err := validateRebuildRequestsDurableCounts(rebuildRequestsDurableStateForTest()); err != nil {
		t.Fatalf("two-page rebuild receipt count was rejected: %v", err)
	}
}

func loadRebuildRequestsAuthoredScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	scenario, err := LoadRebuildRequestsScenario(context.Background(), repoRoot)
	if err != nil {
		t.Fatalf("load authored rebuild-requests scenario: %v", err)
	}
	return scenario
}

func cloneRebuildRequestsScenario(scenario scenarios.Scenario) scenarios.Scenario {
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

func exchangeRebuildRequestsRequestForTest(coordinator *RebuildRequestsCoordinator, body []byte) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "http://coordinator.test/exchange", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+coordinator.Token())
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	coordinator.ServeHTTP(response, request)
	return response
}

func resultEnvelopeForTest(result map[string]any) json.RawMessage {
	value, err := json.Marshal(map[string]any{
		"schema_version": 1, "outcome": "passed", "result": result, "error_code": nil, "error_detail": nil,
	})
	if err != nil {
		panic(err)
	}
	return value
}

func stageNameForTest(stage rebuildRequestsStage) string {
	return stage.String()
}

func rebuildRequestsExchangeBodyForTest(sequence uint64, result json.RawMessage) []byte {
	value, err := json.Marshal(map[string]any{
		"schema_version": 1,
		"sequence":       sequence,
		"result":         result,
	})
	if err != nil {
		panic(err)
	}
	return value
}

func rebuildRequestsResultForStageForTest(coordinator *RebuildRequestsCoordinator) json.RawMessage {
	process := json.RawMessage(`{"process_id":"process-a","database_identity_fingerprint":"` + strings.Repeat("a", 64) + `"}`)
	status := json.RawMessage(`{"state":"ready","retry_at":null,"operation":null,"failure":null}`)
	switch coordinator.stage {
	case rebuildRequestsStageOpen:
		return json.RawMessage("null")
	case rebuildRequestsStageBegin:
		return resultEnvelopeForTest(map[string]any{
			"kind": "opened", "status": status, "process": process,
		})
	case rebuildRequestsStageFirstPage:
		return resultEnvelopeForTest(map[string]any{
			"kind": "call-begun", "call_id": coordinator.callID, "state": "in_flight", "process": process,
		})
	case rebuildRequestsStageFinalPage, rebuildRequestsStagePull:
		return resultEnvelopeForTest(map[string]any{
			"kind": "awaited", "status": status, "process": process,
		})
	case rebuildRequestsStageAwaitCall:
		return resultEnvelopeForTest(map[string]any{
			"kind": "awaited", "status": status, "process": process,
		})
	case rebuildRequestsStageFinalCapture:
		return resultEnvelopeForTest(map[string]any{
			"kind": "call-completed", "call_id": coordinator.callID, "state": "completed", "completion": "idle", "status": status, "process": process,
		})
	case rebuildRequestsStageApplicationRows:
		return rebuildRequestsCaptureResultForTest(process, []string{
			"client_state", "pending_mutations", "rejected_mutations", "sync_status", "sync_events", "provenance", "request_trace", "durable_proof",
		})
	case rebuildRequestsStageComplete:
		return rebuildRequestsCaptureResultForTest(process, []string{"application_rows"})
	default:
		panic(fmt.Sprintf("unexpected rebuild-requests stage %d", coordinator.stage))
	}
}

func rebuildRequestsCaptureResultForTest(process json.RawMessage, keys []string) json.RawMessage {
	capture := make(map[string]any, len(keys))
	for _, key := range keys {
		capture[key] = json.RawMessage("null")
	}
	return resultEnvelopeForTest(map[string]any{
		"kind": "capture", "capture": capture, "process": process,
	})
}

func rebuildRequestsTransportForTest() []transportObservation {
	const (
		firstCursor = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		pullCursor  = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		schemaHash  = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
		scopeHash   = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
		rebuildHash = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	)
	return []transportObservation{
		{Sequence: 1, OperationClass: "connect", StatusCode: http.StatusOK, DurationNanoseconds: 1, RequestFacts: json.RawMessage(`{}`)},
		{Sequence: 2, OperationClass: "rebuild", StatusCode: http.StatusOK, DurationNanoseconds: 1, RequestFacts: json.RawMessage(`{"client_generation":1,"schema_version":1,"schema_hash":"` + schemaHash + `","scope_fingerprint":"` + scopeHash + `","rebuild_id_fingerprint":"` + rebuildHash + `","limit":1}`), RebuildResponseFacts: json.RawMessage(`{"record_count":1,"has_more":true,"has_cursor":true,"has_final_scope_cursor":false,"has_checksum":false,"scope_fingerprint":"` + scopeHash + `"}`)},
		{Sequence: 3, OperationClass: "rebuild", StatusCode: http.StatusOK, DurationNanoseconds: 1, RequestFacts: json.RawMessage(`{"client_generation":1,"schema_version":1,"schema_hash":"` + schemaHash + `","scope_fingerprint":"` + scopeHash + `","rebuild_id_fingerprint":"` + rebuildHash + `","limit":1}`), RebuildResponseFacts: json.RawMessage(`{"record_count":1,"has_more":false,"has_cursor":false,"has_final_scope_cursor":true,"has_checksum":true,"scope_fingerprint":"` + scopeHash + `","final_scope_cursor_fingerprint":"` + firstCursor + `"}`)},
		{Sequence: 4, OperationClass: "pull", StatusCode: http.StatusOK, DurationNanoseconds: 1, CursorFingerprints: []string{firstCursor}, CursorFingerprintsComplete: boolPointer(true), RequestFacts: json.RawMessage(`{"client_generation":1,"schema_version":1,"schema_hash":"` + schemaHash + `","scope_set_version":1,"scope_count":1,"limit":1}`), PullResponseFacts: json.RawMessage(`{"change_count":1,"has_more":false,"rebuild_scope_count":0,"checksum_count":1,"scope_cursor_fingerprints":["` + pullCursor + `"],"scope_cursor_fingerprints_complete":true}`)},
	}
}

func boolPointer(value bool) *bool { return &value }

func rebuildRequestsDurableStateForTest() inspectedClientState {
	return inspectedClientState{
		ApplicationRowCount: 3, MutationLedgerCount: 0, MutationOutcomeCount: 0,
		SealedBatchCount: 0, RejectedMutationCount: 0, ScopeStateCount: 1,
		ScopeRowCount: 3, ProvenanceCount: 3, RowMetadataCount: 3,
		RebuildAttemptCount: 0, RebuildReceiptCount: 2,
		ScopeStates: []clientScopeState{{}}, ScopeRows: []clientScopeRow{{}, {}, {}},
	}
}
