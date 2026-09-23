package reactnative

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/internal/contract"
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
		{"page replay assertion claim", func(scenario *scenarios.Scenario) {
			for index := range scenario.Assertions {
				if string(scenario.Assertions[index].ID) == "ASSERT-PERF-REBUILD-REQUESTS-REPLAY-009-001" {
					scenario.Assertions[index].RequirementIDs = []contract.RequirementID{"SYNC-REBUILD-010"}
				}
			}
		}},
		{"native isolation proof claim", func(scenario *scenarios.Scenario) {
			for index := range scenario.ProofObligations {
				if string(scenario.ProofObligations[index].ObligationID) == "OBL-PERF-REBUILD-REQUESTS-RN-IOS-CURRENT-001" {
					scenario.ProofObligations[index].AssertionIDs = scenario.ProofObligations[index].AssertionIDs[:len(scenario.ProofObligations[index].AssertionIDs)-1]
				}
			}
		}},
		{"scope isolation control target", func(scenario *scenarios.Scenario) {
			for index := range scenario.ProofObligations {
				if string(scenario.ProofObligations[index].ObligationID) == "OBL-PERF-REBUILD-REQUESTS-ISOLATION-010-CONTROL-001" {
					scenario.ProofObligations[index].MakeTarget = "test-conformance"
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
	if got, want := coordinator.ExchangeCount(), 15; got != want {
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
	coordinator.sourceApplied = true
	close(coordinator.firstPageObserved)
	close(coordinator.finalPageObserved)

	tests := []struct {
		stage     rebuildRequestsStage
		actor     string
		name      string
		stepID    scenarios.StepID
		stepCount int
	}{
		{rebuildRequestsStageBegin, "client", "begin-call", rebuildRequestsStepOrder[3], 1},
		{rebuildRequestsStageFirstPage, "observer", "await-step", rebuildRequestsStepOrder[5], 1},
		{rebuildRequestsStageFirstRecoveryPage, "observer", "await-step", rebuildRequestsStepOrder[5], 1},
		{rebuildRequestsStageFinalPage, "observer", "capture", "", 0},
		{rebuildRequestsStageFinalRecoveryPage, "observer", "await-step", rebuildRequestsStepOrder[9], 1},
		{rebuildRequestsStagePull, "observer", "await-step", rebuildRequestsStepOrder[12], 1},
	}
	for _, test := range tests {
		name := string(test.stepID)
		if name == "" {
			name = test.stage.String()
		}
		t.Run(name, func(t *testing.T) {
			coordinator.stage = test.stage
			response, err := coordinator.advanceLocked(context.Background(), 1)
			if err != nil {
				t.Fatalf("advance rebuild-requests stage: %v", err)
			}
			if response.Command.Action.Action.Actor != test.actor ||
				response.Command.Action.Action.Command != test.name {
				t.Fatalf("rebuild-requests command = %q/%q, want %q/%q", response.Command.Action.Action.Actor, response.Command.Action.Action.Command, test.actor, test.name)
			}
			if len(response.Command.Action.Steps) != test.stepCount {
				t.Fatalf("rebuild-requests command step count = %d, want %d", len(response.Command.Action.Steps), test.stepCount)
			}
			if test.stepCount == 1 {
				operation := response.Command.Action.Steps[0].Operation
				if operation.ContractOperation != coordinator.steps[test.stepID].Operation.ContractOperation ||
					operation.Name != coordinator.steps[test.stepID].Operation.Name ||
					!bytes.Equal(operation.Payload, coordinator.steps[test.stepID].Operation.Payload) {
					t.Fatalf("rebuild-requests command step = %#v, want %s", operation, test.stepID)
				}
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

func TestRebuildRequestsProxyBarriersPreserveExactPageReplays(t *testing.T) {
	firstRequest := `{"scope":"runtime-scope","rebuild_id":"rebuild-a","cursor":null}`
	finalRequest := `{"scope":"runtime-scope","rebuild_id":"rebuild-a","cursor":"cursor-1"}`
	firstResponse := `{"scope":"runtime-scope","records":[{"table":"runtime-items","pk":{},"row":{},"row_checksum":{},"server_version":"v1"}],"has_more":true,"cursor":"cursor-1"}`
	finalResponse := `{"scope":"runtime-scope","records":[{"table":"runtime-items","pk":{},"row":{},"row_checksum":{},"server_version":"v2"}],"has_more":false,"final_scope_cursor":"cursor-2","checksum":{}}`
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read rebuild-requests proxy test body: %v", err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusOK)
		if bytes.Equal(body, []byte(firstRequest)) {
			_, _ = writer.Write([]byte(firstResponse))
			return
		}
		if bytes.Equal(body, []byte(finalRequest)) {
			_, _ = writer.Write([]byte(finalResponse))
			return
		}
		t.Errorf("unexpected rebuild-requests proxy test body: %s", body)
	}))
	defer upstream.Close()

	coordinator, err := NewRebuildRequestsCoordinator(RebuildRequestsCoordinatorConfig{
		Scenario: loadRebuildRequestsAuthoredScenario(t), Platform: "ios", ServerURL: upstream.URL, AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create rebuild-requests coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()

	startRequest := func(body string) (*httptest.ResponseRecorder, <-chan struct{}) {
		response := httptest.NewRecorder()
		done := make(chan struct{})
		go func() {
			coordinator.proxyAdapter(
				response,
				httptest.NewRequest(http.MethodPost, "/sync/rebuild", strings.NewReader(body)),
			)
			close(done)
		}()
		return response, done
	}
	await := func(channel <-chan struct{}, name string) {
		select {
		case <-channel:
		case <-time.After(time.Second):
			t.Fatalf("wait for %s", name)
		}
	}

	firstDelivery, firstDone := startRequest(firstRequest)
	await(coordinator.firstPageObserved, "first page observation")
	select {
	case <-firstDone:
		t.Fatal("first page response crossed its restart barrier")
	default:
	}
	coordinator.releaseFirstPage()
	await(firstDone, "released first page response")
	if firstDelivery.Code != http.StatusOK {
		t.Fatalf("first page delivery status = %d, want %d", firstDelivery.Code, http.StatusOK)
	}

	firstReplay, firstReplayDone := startRequest(firstRequest)
	await(firstReplayDone, "first page replay")
	if firstReplay.Code != http.StatusOK {
		t.Fatalf("first page replay status = %d, want %d", firstReplay.Code, http.StatusOK)
	}

	finalDelivery, finalDone := startRequest(finalRequest)
	await(coordinator.finalPageObserved, "final page observation")
	select {
	case <-finalDone:
		t.Fatal("final page response crossed its restart barrier")
	default:
	}
	coordinator.releaseFinalPage()
	await(finalDone, "released final page response")
	if finalDelivery.Code != http.StatusOK {
		t.Fatalf("final page delivery status = %d, want %d", finalDelivery.Code, http.StatusOK)
	}

	finalReplay, finalReplayDone := startRequest(finalRequest)
	await(finalReplayDone, "final page replay")
	if finalReplay.Code != http.StatusOK {
		t.Fatalf("final page replay status = %d, want %d", finalReplay.Code, http.StatusOK)
	}

	coordinator.proxyMu.Lock()
	firstReplays := append([]rebuildPageReplay(nil), coordinator.firstReplays...)
	finalReplays := append([]rebuildPageReplay(nil), coordinator.finalReplays...)
	coordinator.proxyMu.Unlock()
	if len(firstReplays) != 2 || firstReplays[0] != firstReplays[1] {
		t.Fatalf("first page replay evidence = %#v, want two exact deliveries", firstReplays)
	}
	if len(finalReplays) != 2 || finalReplays[0] != finalReplays[1] {
		t.Fatalf("final page replay evidence = %#v, want two exact deliveries", finalReplays)
	}
}

func TestRebuildRequestsRecoveryOpenAcknowledgementReleasesHeldPages(t *testing.T) {
	coordinator, err := NewRebuildRequestsCoordinator(RebuildRequestsCoordinatorConfig{
		Scenario: loadRebuildRequestsAuthoredScenario(t), Platform: "ios",
		ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create rebuild-requests release coordinator: %v", err)
	}
	defer func() { _ = coordinator.Close(context.Background()) }()
	digest := strings.Repeat("a", 64)
	coordinator.process = &actionProcessIdentity{
		ProcessID: "process-a", DatabaseIdentityFingerprint: digest,
	}
	opened := func(processID string) json.RawMessage {
		return resultEnvelopeForTest(map[string]any{
			"kind":   "opened",
			"status": json.RawMessage(`{"state":"ready","retry_at":null,"operation":null,"failure":null}`),
			"process": json.RawMessage(
				`{"process_id":"` + processID + `","database_identity_fingerprint":"` + digest + `"}`,
			),
		})
	}
	assertBlocked := func(channel <-chan struct{}, name string) {
		select {
		case <-channel:
			t.Fatalf("%s was released before new-process acknowledgement", name)
		default:
		}
	}
	assertReleased := func(channel <-chan struct{}, name string) {
		select {
		case <-channel:
		default:
			t.Fatalf("%s was not released after new-process acknowledgement", name)
		}
	}

	coordinator.stage = rebuildRequestsStageFirstRecoveryBegin
	assertBlocked(coordinator.allowFirstPage, "first page")
	if err := coordinator.acceptResultLocked(opened("process-b")); err != nil {
		t.Fatalf("accept first recovery process: %v", err)
	}
	if _, err := coordinator.advanceLocked(context.Background(), 1); err != nil {
		t.Fatalf("advance first recovery process: %v", err)
	}
	assertReleased(coordinator.allowFirstPage, "first page")

	coordinator.stage = rebuildRequestsStageFinalRecoveryBegin
	assertBlocked(coordinator.allowFinalPage, "final page")
	if err := coordinator.acceptResultLocked(opened("process-c")); err != nil {
		t.Fatalf("accept final recovery process: %v", err)
	}
	if _, err := coordinator.advanceLocked(context.Background(), 2); err != nil {
		t.Fatalf("advance final recovery process: %v", err)
	}
	assertReleased(coordinator.allowFinalPage, "final page")
}

func TestCombineRebuildRequestsTracesPreservesSegmentedChronology(t *testing.T) {
	full := rebuildRequestsTransportForTest()
	first := traceSnapshot{
		Observations:       append([]transportObservation(nil), full[:2]...),
		SequenceCheckpoint: 2,
	}
	finalObservations := append([]transportObservation(nil), full[2:]...)
	for index := range finalObservations {
		finalObservations[index].Sequence = uint64(index + 1)
	}
	final := traceSnapshot{Observations: finalObservations, SequenceCheckpoint: 3}
	combined, err := combineRebuildRequestsTraces(first, final)
	if err != nil {
		t.Fatalf("combine complete rebuild-requests traces: %v", err)
	}
	if !reflect.DeepEqual(combined.Observations, full) || combined.SequenceCheckpoint != 5 {
		t.Fatalf("combined rebuild-requests trace = %#v, want %#v", combined, full)
	}
	if err := validateRebuildRequestsTransport(loadRebuildRequestsAuthoredScenario(t), combined); err != nil {
		t.Fatalf("validate combined rebuild-requests chronology: %v", err)
	}

	missing := first
	missing.Observations = missing.Observations[:1]
	missing.SequenceCheckpoint = 1
	if _, err := combineRebuildRequestsTraces(missing, final); err == nil {
		t.Fatal("missing first recovery trace segment was accepted")
	}

	reordered := final
	reordered.Observations = append([]transportObservation(nil), final.Observations...)
	reordered.Observations[0], reordered.Observations[1] = reordered.Observations[1], reordered.Observations[0]
	for index := range reordered.Observations {
		reordered.Observations[index].Sequence = uint64(index + 1)
	}
	combined, err = combineRebuildRequestsTraces(first, reordered)
	if err != nil {
		t.Fatalf("combine reordered rebuild-requests trace for semantic control: %v", err)
	}
	if err := validateRebuildRequestsTransport(loadRebuildRequestsAuthoredScenario(t), combined); err == nil {
		t.Fatal("reordered rebuild-requests trace was accepted")
	}

	changed := final
	changed.Observations = append([]transportObservation(nil), final.Observations...)
	changed.Observations[1].RequestFacts = json.RawMessage(`{"client_generation":1,"schema_version":1,"schema_hash":"changed","scope_fingerprint":"scope","rebuild_id_fingerprint":"rebuild","limit":1}`)
	combined, err = combineRebuildRequestsTraces(first, changed)
	if err != nil {
		t.Fatalf("combine changed rebuild-requests trace for semantic control: %v", err)
	}
	if err := validateRebuildRequestsTransport(loadRebuildRequestsAuthoredScenario(t), combined); err == nil {
		t.Fatal("changed final-page replay trace was accepted")
	}
}

func TestRebuildRequestsCloseReleasesHeldResponses(t *testing.T) {
	coordinator, err := NewRebuildRequestsCoordinator(RebuildRequestsCoordinatorConfig{
		Scenario: loadRebuildRequestsAuthoredScenario(t), Platform: "ios",
		ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create rebuild-requests cleanup coordinator: %v", err)
	}
	waitContext, cancelWait := context.WithCancel(context.Background())
	defer cancelWait()
	waiting := make(chan struct{})
	waitDone := make(chan error, 1)
	go func() {
		coordinator.mu.Lock()
		close(waiting)
		err := coordinator.waitForFirstPage(waitContext)
		coordinator.mu.Unlock()
		waitDone <- err
	}()
	<-waiting
	closed := make(chan error, 1)
	go func() { closed <- coordinator.Close(context.Background()) }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close rebuild-requests cleanup coordinator: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close waited for the blocked exchange mutex")
	}
	if err := <-waitDone; err == nil {
		t.Fatal("closed coordinator left its page waiter successful")
	}
	for name, channel := range map[string]<-chan struct{}{
		"first page": coordinator.allowFirstPage,
		"final page": coordinator.allowFinalPage,
	} {
		select {
		case <-channel:
		default:
			t.Fatalf("%s hold remained blocked after close", name)
		}
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
	coordinator.nextSeq = 15

	_, err = coordinator.Result()
	if err == nil || !strings.Contains(err.Error(), "current stage=complete") || !strings.Contains(err.Error(), "exchanges served=14") || !strings.Contains(err.Error(), "ExchangeCount=15") {
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
	coordinator.nextSeq = 15

	_, err = coordinator.Result()
	if err == nil || !strings.Contains(err.Error(), "terminal validation failed") || !strings.Contains(err.Error(), "current stage=complete") || !strings.Contains(err.Error(), "exchanges served=14") || !strings.Contains(err.Error(), "ExchangeCount=15") {
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
	firstRecoveryTrace, err := json.Marshal(traceSnapshot{
		Observations:       rebuildRequestsTransportForTest()[:2],
		SequenceCheckpoint: 2,
	})
	if err != nil {
		t.Fatalf("encode first recovery trace: %v", err)
	}
	firstRecoveryCapture := resultEnvelopeForTest(map[string]any{
		"kind": "capture",
		"capture": map[string]any{
			"request_trace": json.RawMessage(firstRecoveryTrace),
		},
		"process": json.RawMessage(process),
	})
	tests := []struct {
		stage    rebuildRequestsStage
		result   json.RawMessage
		wantKind string
		wantErr  bool
	}{
		{rebuildRequestsStageFirstPage, callBegun, "call-begun", false},
		{rebuildRequestsStageFirstRestart, awaited, "awaited", false},
		{rebuildRequestsStageFirstRecoveryPage, callBegun, "call-begun", false},
		{rebuildRequestsStageFinalPage, awaited, "awaited", false},
		{rebuildRequestsStageFinalRestart, firstRecoveryCapture, "capture", false},
		{rebuildRequestsStageFinalRecoveryPage, callBegun, "call-begun", false},
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

func TestObserveRebuildResponseRejectsChangedExactReplay(t *testing.T) {
	coordinator := &RebuildRequestsCoordinator{
		sourceApplied:     true,
		firstPageObserved: make(chan struct{}),
		allowFirstPage:    make(chan struct{}),
	}
	request := []byte(`{"scope":"runtime-scope","rebuild_id":"rebuild-a","cursor":null}`)
	first := []byte(`{"scope":"runtime-scope","records":[{"table":"runtime-items","pk":{},"row":{},"row_checksum":{},"server_version":"v1"}],"has_more":true,"cursor":"cursor-1"}`)
	changed := []byte(`{"scope":"runtime-scope","records":[{"table":"runtime-items","pk":{},"row":{},"row_checksum":{},"server_version":"v2"}],"has_more":true,"cursor":"cursor-1"}`)
	if _, err := coordinator.observeRebuildResponse(request, first); err != nil {
		t.Fatalf("observe first rebuild response: %v", err)
	}
	if _, err := coordinator.observeRebuildResponse(request, changed); err == nil {
		t.Fatal("changed first-page replay was accepted")
	}
}

func TestValidateRebuildRequestsTransportAcceptsAdvancedIncrementalPullCursor(t *testing.T) {
	transport := traceSnapshot{Observations: rebuildRequestsTransportForTest(), SequenceCheckpoint: 5}
	if err := validateRebuildRequestsTransport(loadRebuildRequestsAuthoredScenario(t), transport); err != nil {
		t.Fatalf("incremental pull with an advanced response cursor was rejected: %v", err)
	}
}

func TestValidateRebuildRequestsTransportRequiresOneIncrementalPullResponseCursor(t *testing.T) {
	transport := traceSnapshot{Observations: rebuildRequestsTransportForTest(), SequenceCheckpoint: 5}
	transport.Observations[4].PullResponseFacts = json.RawMessage(`{"change_count":1,"has_more":false,"rebuild_scope_count":0,"checksum_count":1,"scope_cursor_fingerprints":[],"scope_cursor_fingerprints_complete":true}`)
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

func TestValidateRebuildRequestsRowsAcceptsNativeMetadataChecksum(t *testing.T) {
	coordinator, state, proof, evidence := rebuildRequestsRowsForTest(t)
	if err := coordinator.validateRows(state, proof, evidence); err != nil {
		t.Fatalf("native selected row metadata checksum was rejected: %v", err)
	}
}

func TestValidateRebuildRequestsRowsNamesEverySelectedMetadataMember(t *testing.T) {
	coordinator, state, proof, evidence := rebuildRequestsRowsForTest(t)
	invalidChecksum := "invalid"
	proof.RowMetadata = &durableMetadata{
		TableName: "other-table", RecordID: "other-record", ServerVersion: "", RowChecksum: &invalidChecksum,
	}
	err := coordinator.validateRows(state, proof, evidence)
	if err == nil {
		t.Fatal("invalid selected row metadata was accepted")
	}
	for _, value := range []string{
		`table_name="other-table" want="items"`,
		`record_id="other-record" want="row-c"`,
		`server_version="" want=nonempty`,
		`row_checksum=invalid want="` + strings.Repeat("c", 64) + `"`,
		"row_checksum_decode_error=",
	} {
		if !strings.Contains(err.Error(), value) {
			t.Fatalf("selected row metadata diagnostic = %q, want it to contain %q", err, value)
		}
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
		{Sequence: 3, OperationClass: "connect", StatusCode: http.StatusOK, DurationNanoseconds: 1, RequestFacts: json.RawMessage(`{}`)},
		{Sequence: 4, OperationClass: "rebuild", StatusCode: http.StatusOK, DurationNanoseconds: 1, RequestFacts: json.RawMessage(`{"client_generation":1,"schema_version":1,"schema_hash":"` + schemaHash + `","scope_fingerprint":"` + scopeHash + `","rebuild_id_fingerprint":"` + rebuildHash + `","limit":1}`), RebuildResponseFacts: json.RawMessage(`{"record_count":1,"has_more":false,"has_cursor":false,"has_final_scope_cursor":true,"has_checksum":true,"scope_fingerprint":"` + scopeHash + `","final_scope_cursor_fingerprint":"` + firstCursor + `"}`)},
		{Sequence: 5, OperationClass: "pull", StatusCode: http.StatusOK, DurationNanoseconds: 1, CursorFingerprints: []string{firstCursor}, CursorFingerprintsComplete: boolPointer(true), RequestFacts: json.RawMessage(`{"client_generation":1,"schema_version":1,"schema_hash":"` + schemaHash + `","scope_set_version":1,"scope_count":1,"limit":1}`), PullResponseFacts: json.RawMessage(`{"change_count":1,"has_more":false,"rebuild_scope_count":0,"checksum_count":1,"scope_cursor_fingerprints":["` + pullCursor + `"],"scope_cursor_fingerprints_complete":true}`)},
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

func rebuildRequestsRowsForTest(t *testing.T) (*RebuildRequestsCoordinator, inspectedClientState, durableProof, rebuildRequestsIdentityEvidence) {
	t.Helper()
	coordinator, err := NewRebuildRequestsCoordinator(RebuildRequestsCoordinatorConfig{
		Scenario: loadRebuildRequestsAuthoredScenario(t), Platform: "android", ServerURL: "http://127.0.0.1:8080", AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create rebuild-requests coordinator: %v", err)
	}
	t.Cleanup(func() { _ = coordinator.Close(context.Background()) })
	rows := []clientScopeRow{
		{ScopeID: "scope-a", TableName: "items", RecordID: "row-a", Checksum: strings.Repeat("a", 64), Generation: 1},
		{ScopeID: "scope-a", TableName: "items", RecordID: "row-b", Checksum: strings.Repeat("b", 64), Generation: 1},
		{ScopeID: "scope-a", TableName: "items", RecordID: "row-c", Checksum: strings.Repeat("c", 64), Generation: 1},
	}
	coordinator.runtimeIDs["row-a-primary-key"] = json.RawMessage(`"row-a"`)
	coordinator.runtimeIDs["row-b-primary-key"] = json.RawMessage(`"row-b"`)
	coordinator.runtimeIDs["row-c-primary-key"] = json.RawMessage(`"row-c"`)
	provenance, err := json.Marshal(rows)
	if err != nil {
		t.Fatalf("encode rebuild-requests provenance: %v", err)
	}
	coordinator.finalResult = &finalCapture{Provenance: provenance}
	checksum := `{"algorithm":"sha256","version":1,"encoding":"hex","digest":"` + strings.Repeat("c", 64) + `"}`
	proof := durableProof{RowMetadata: &durableMetadata{
		TableName: "items", RecordID: "row-c", ServerVersion: "version-c", RowChecksum: &checksum,
	}}
	return coordinator, inspectedClientState{ScopeRows: rows}, proof, rebuildRequestsIdentityEvidence{tableName: "items"}
}
