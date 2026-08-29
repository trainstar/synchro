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

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestExchangeRejectsMalformedAndDuplicateMembers(t *testing.T) {
	coordinator := newUnitCoordinator(t)
	defer closeUnitCoordinator(t, coordinator)

	for _, body := range [][]byte{
		[]byte(`{"schema_version":1,"sequence":1`),
		[]byte(`{"schema_version":1,"sequence":1,"sequence":1,"result":null}`),
	} {
		response := exchangeRequestForTest(coordinator, body)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("malformed exchange status = %d, want %d", response.Code, http.StatusBadRequest)
		}
	}
}

func TestExchangeRejectsOutOfOrderSequence(t *testing.T) {
	coordinator := newUnitCoordinator(t)
	defer closeUnitCoordinator(t, coordinator)

	response := exchangeRequestForTest(coordinator, []byte(`{"schema_version":1,"sequence":2,"result":null}`))
	if response.Code != http.StatusConflict {
		t.Fatalf("out-of-order exchange status = %d, want %d", response.Code, http.StatusConflict)
	}
	if _, err := coordinator.Result(); err == nil {
		t.Fatal("out-of-order exchange did not preserve coordinator failure")
	}
}

func TestExchangeRejectsUnavailableEnvelope(t *testing.T) {
	coordinator := newUnitCoordinator(t)
	defer closeUnitCoordinator(t, coordinator)

	body := []byte(`{"schema_version":1,"sequence":1,"result":{"schema_version":1,"outcome":"error","result":null,"error_code":"unavailable"}}`)
	response := exchangeRequestForTest(coordinator, body)
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("unavailable envelope status = %d, want %d", response.Code, http.StatusUnprocessableEntity)
	}
}

func TestExchangeInitialRequestReturnsOpenCommandWithRepeatedSequence(t *testing.T) {
	coordinator := newUnitCoordinator(t)
	defer closeUnitCoordinator(t, coordinator)

	response := exchangeRequestForTest(coordinator, []byte(`{"schema_version":1,"sequence":1,"result":null}`))
	if response.Code != http.StatusOK {
		t.Fatalf("initial exchange status = %d, want %d", response.Code, http.StatusOK)
	}
	var result exchangeResponse
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode initial exchange response: %v", err)
	}
	if result.Sequence != 1 || result.State != "command" || result.Command == nil || result.Command.Action.Action.Command != "open" {
		t.Fatalf("initial response = %#v, want open command with sequence 1", result)
	}
}

func TestStoppedLifecycleRequiresStableIdentity(t *testing.T) {
	digest := strings.Repeat("a", 64)
	baseline, err := validateOpenedResult(json.RawMessage(`{"kind":"opened","status":{"state":"local_ready","retry_at":null,"operation":null,"failure":null},"process":{"process_id":"process-a","database_identity_fingerprint":"` + digest + `"}}`))
	if err != nil {
		t.Fatalf("valid opened result was rejected: %v", err)
	}
	valid := `{"kind":"lifecycle","operation":"stop","status":{"state":"stopped","retry_at":null,"operation":null,"failure":null},"process":{"process_id":"process-a","database_identity_fingerprint":"` + digest + `"}}`
	if err := validateStoppedLifecycleResult(json.RawMessage(valid), baseline); err != nil {
		t.Fatalf("valid stopped lifecycle result was rejected: %v", err)
	}
	tests := []struct {
		name string
		raw  string
	}{
		{"wrong operation", strings.Replace(valid, `"operation":"stop"`, `"operation":"enter-background"`, 1)},
		{"wrong state", strings.Replace(valid, `"state":"stopped"`, `"state":"ready"`, 1)},
		{"active retry", strings.Replace(valid, `"retry_at":null`, `"retry_at":"later"`, 1)},
		{"changed process", strings.Replace(valid, `"process_id":"process-a"`, `"process_id":"process-b"`, 1)},
		{"changed database", strings.Replace(valid, digest, strings.Repeat("b", 64), 1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateStoppedLifecycleResult(json.RawMessage(test.raw), baseline); err == nil {
				t.Fatal("invalid stopped lifecycle result was accepted")
			}
		})
	}
}

func TestBootstrapTraceRejectsMissingPull(t *testing.T) {
	trace := validBootstrapTrace(testSchema())
	trace.Observations = trace.Observations[:2]
	trace.SequenceCheckpoint = 2
	if err := validateBootstrapTrace(trace); err == nil {
		t.Fatal("bootstrap trace without pull was accepted")
	}
}

func TestBootstrapTraceRejectsInvalidResponseFacts(t *testing.T) {
	mutateRebuildBoolean := func(name string, value bool) func(*traceSnapshot) {
		return func(trace *traceSnapshot) {
			var facts map[string]any
			if err := json.Unmarshal(trace.Observations[1].RebuildResponseFacts, &facts); err != nil {
				panic(err)
			}
			facts[name] = value
			trace.Observations[1].RebuildResponseFacts, _ = json.Marshal(facts)
		}
	}
	tests := []struct {
		name   string
		mutate func(*traceSnapshot)
	}{
		{"absent rebuild facts", func(trace *traceSnapshot) { trace.Observations[1].RebuildResponseFacts = nil }},
		{"null rebuild facts", func(trace *traceSnapshot) { trace.Observations[1].RebuildResponseFacts = json.RawMessage(`null`) }},
		{"malformed rebuild facts", func(trace *traceSnapshot) {
			trace.Observations[1].RebuildResponseFacts = json.RawMessage(`{"record_count":0}`)
		}},
		{"paginated rebuild facts", mutateRebuildBoolean("has_more", true)},
		{"page cursor in terminal rebuild facts", mutateRebuildBoolean("has_cursor", true)},
		{"missing final cursor in rebuild facts", mutateRebuildBoolean("has_final_scope_cursor", false)},
		{"missing checksum in rebuild facts", mutateRebuildBoolean("has_checksum", false)},
		{"absent pull facts", func(trace *traceSnapshot) { trace.Observations[2].PullResponseFacts = nil }},
		{"null pull facts", func(trace *traceSnapshot) { trace.Observations[2].PullResponseFacts = json.RawMessage(`null`) }},
		{"malformed pull facts", func(trace *traceSnapshot) {
			trace.Observations[2].PullResponseFacts = json.RawMessage(`{"change_count":"zero","has_more":false,"rebuild_scope_count":0,"checksum_count":0}`)
		}},
		{"missing pull response cursors", func(trace *traceSnapshot) {
			trace.Observations[2].PullResponseFacts = json.RawMessage(`{"change_count":0,"has_more":false,"rebuild_scope_count":0,"checksum_count":1}`)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			trace := validBootstrapTrace(testSchema())
			test.mutate(&trace)
			if err := validateBootstrapTrace(trace); err == nil {
				t.Fatal("invalid response facts were accepted")
			}
		})
	}
}

func TestWarmConnectScopeAuthorityNegativeControl(t *testing.T) {
	t.Run("assertion", func(t *testing.T) {
		scenario := loadAuthoredScenario(t)
		if err := ValidateScenario(scenario); err != nil {
			t.Fatalf("validate authored negative-control binding: %v", err)
		}
		trace := validBootstrapTrace(testSchema())
		trace.Observations[0].RequestFacts = requestFacts(0, testSchema(), 0, 1, "", "")
		if err := validateBootstrapTrace(trace); err == nil {
			t.Fatal("client-authored bootstrap scope mutant was accepted")
		}
	})
}

func TestBootstrapRebuildEvidenceRequiresLocalChecksumMatch(t *testing.T) {
	trace := validBootstrapTrace(testSchema())
	proof := validFinalCapture(*warmConnectExpectedState(loadAuthoredScenario(t))).DurableProof
	var value map[string]any
	if err := json.Unmarshal(proof, &value); err != nil {
		t.Fatalf("decode bootstrap proof: %v", err)
	}
	value["row_metadata"] = nil
	receipt := value["rebuild_receipt_proofs"].([]any)[0].(map[string]any)
	receipt["final_checksum_matches_local"] = true
	proof, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode bootstrap proof: %v", err)
	}
	if err := validateBootstrapRebuildEvidence(proof, trace); err != nil {
		t.Fatalf("valid bootstrap rebuild proof failed: %v", err)
	}
	receipt["final_checksum_matches_local"] = false
	proof, err = json.Marshal(value)
	if err != nil {
		t.Fatalf("encode bootstrap checksum control: %v", err)
	}
	if err := validateBootstrapRebuildEvidence(proof, trace); err == nil {
		t.Fatal("bootstrap checksum mismatch was accepted")
	}
}

func TestWarmTraceRejectsExtraRequest(t *testing.T) {
	bootstrap := validBootstrapTrace(testSchema())
	final := traceSnapshot{
		Observations: append(append([]transportObservation(nil), bootstrap.Observations...),
			transport("connect", 4, requestFacts(1, testSchema(), 1, 1, "", "")),
			transport("pull", 5, requestFacts(1, testSchema(), 1, 1, "", "")),
			transport("other", 6, requestFacts(1, testSchema(), 1, 1, "", "")),
		),
		SequenceCheckpoint: 6,
	}
	if _, err := warmTrace(final, &bootstrap); err == nil {
		t.Fatal("warm trace with an extra request was accepted")
	}
}

func TestWarmTraceAcceptsReorderedJSONFacts(t *testing.T) {
	bootstrap := validBootstrapTrace(testSchema())
	final := traceSnapshot{
		Observations: append(append([]transportObservation(nil), bootstrap.Observations...),
			transport("connect", 4, requestFacts(1, testSchema(), 1, 1, "", "")),
			transportWithPull("pull", 5, requestFacts(1, testSchema(), 1, 1, "", ""), "cursor-b", "cursor-c"),
		),
		SequenceCheckpoint: 5,
	}
	final.Observations[0].RequestFacts = json.RawMessage(`{"scope_count":0,"scope_set_version":0,"schema_hash":"721d2c95e6f34cd9733feea9f5118fba391eee10d07663dad066cfc59439fa44","schema_version":1,"client_generation":0}`)
	if _, err := warmTrace(final, &bootstrap); err != nil {
		t.Fatalf("reordered bootstrap facts failed: %v", err)
	}
}

func TestWarmTraceRejectsChangedBootstrapFacts(t *testing.T) {
	bootstrap := validBootstrapTrace(testSchema())
	final := traceSnapshot{
		Observations: append(append([]transportObservation(nil), bootstrap.Observations...),
			transport("connect", 4, requestFacts(1, testSchema(), 1, 1, "", "")),
			transportWithPull("pull", 5, requestFacts(1, testSchema(), 1, 1, "", ""), "cursor-b", "cursor-c"),
		),
		SequenceCheckpoint: 5,
	}
	final.Observations[0].RequestFacts = requestFacts(0, testSchema(), 0, 1, "", "")
	if _, err := warmTrace(final, &bootstrap); err == nil {
		t.Fatal("changed bootstrap facts were accepted")
	}
}

func TestWarmTraceRejectsInvalidPullEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*transportObservation)
	}{
		{"absent response facts", func(observation *transportObservation) { observation.PullResponseFacts = nil }},
		{"null response facts", func(observation *transportObservation) { observation.PullResponseFacts = json.RawMessage(`null`) }},
		{"malformed response facts", func(observation *transportObservation) { observation.PullResponseFacts = json.RawMessage(`{}`) }},
		{"absent cursor fingerprints", func(observation *transportObservation) { observation.CursorFingerprints = nil }},
		{"incomplete cursor fingerprints", func(observation *transportObservation) {
			complete := false
			observation.CursorFingerprintsComplete = &complete
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bootstrap := validBootstrapTrace(testSchema())
			final := validWarmTrace(bootstrap)
			test.mutate(&final.Observations[4])
			if _, err := warmTrace(final, &bootstrap); err == nil {
				t.Fatal("invalid warm pull evidence was accepted")
			}
		})
	}
}

func TestValidateScenarioRejectsDetachedProofTargetsAndControl(t *testing.T) {
	tests := []struct {
		name         string
		obligationID string
		mutate       func(*scenarios.ProofObligation)
	}{
		{"iOS target", "OBL-PERF-WARM-CONNECT-RN-IOS-CURRENT-001", func(obligation *scenarios.ProofObligation) { obligation.MakeTarget = "test-rn-e2e-ios" }},
		{"Android argv", "OBL-PERF-WARM-CONNECT-RN-ANDROID-CURRENT-001", func(obligation *scenarios.ProofObligation) { obligation.Argv[1] = "test-rn-e2e-android" }},
		{"control target", "OBL-PERF-WARM-CONNECT-CONTROL-001", func(obligation *scenarios.ProofObligation) { obligation.MakeTarget = "test-conformance" }},
		{"control identity", "OBL-PERF-WARM-CONNECT-CONTROL-001", func(obligation *scenarios.ProofObligation) { obligation.ControlID = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scenario := loadAuthoredScenario(t)
			index := proofObligationIndex(scenario, test.obligationID)
			if index < 0 {
				t.Fatalf("proof obligation %s is absent", test.obligationID)
			}
			test.mutate(&scenario.ProofObligations[index])
			if err := ValidateScenario(scenario); err == nil {
				t.Fatal("detached proof obligation was accepted")
			}
		})
	}
}

func TestFinalCaptureRejectsChecksumAndIdentityChanges(t *testing.T) {
	scenario := loadAuthoredScenario(t)
	expected := warmConnectExpectedState(scenario)
	if expected == nil || len(expected.Clients) != 1 || len(expected.Rows) != 1 {
		t.Fatal("authored warm-connect state is incomplete")
	}
	capture := validFinalCapture(*expected)
	state, err := decodeClientState(capture.ClientState)
	if err != nil {
		t.Fatalf("decode valid client state: %v", err)
	}
	if err := validateFinalClientEvidence(scenario, state, capture); err != nil {
		t.Fatalf("valid authored client evidence failed: %v", err)
	}

	var changed map[string]any
	if err := json.Unmarshal(capture.ClientState, &changed); err != nil {
		t.Fatalf("decode checksum control: %v", err)
	}
	changed["scopeRows"].([]any)[0].(map[string]any)["checksum"] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	changedState, err := json.Marshal(changed)
	if err != nil {
		t.Fatalf("encode checksum control: %v", err)
	}
	capture.ClientState = changedState
	state, err = decodeClientState(capture.ClientState)
	if err != nil {
		t.Fatalf("decode changed checksum state: %v", err)
	}
	if err := validateFinalClientEvidence(scenario, state, capture); err == nil {
		t.Fatal("changed checksum was accepted")
	}

	capture = validFinalCapture(*expected)
	var proof map[string]any
	if err := json.Unmarshal(capture.DurableProof, &proof); err != nil {
		t.Fatalf("decode receipt checksum control: %v", err)
	}
	proof["rebuild_receipt_proofs"].([]any)[0].(map[string]any)["final_checksum_matches_local"] = true
	capture.DurableProof, err = json.Marshal(proof)
	if err != nil {
		t.Fatalf("encode receipt checksum control: %v", err)
	}
	state, err = decodeClientState(capture.ClientState)
	if err != nil {
		t.Fatalf("decode receipt checksum state: %v", err)
	}
	if err := validateFinalClientEvidence(scenario, state, capture); err == nil {
		t.Fatal("mismatched final receipt checksum was accepted")
	}

	capture = validFinalCapture(*expected)
	bootstrap := validBootstrapTrace(testSchema())
	warm := []transportObservation{
		transport("connect", 4, requestFacts(1, testSchema(), 1, 1, "", "")),
		transportWithPull("pull", 5, requestFacts(1, testSchema(), 1, 1, "", ""), "cursor-b", "cursor-c"),
	}
	state, err = decodeClientState(capture.ClientState)
	if err != nil {
		t.Fatalf("decode identity control state: %v", err)
	}
	if err := validateTransportIdentities(state, capture, bootstrap, warm); err != nil {
		t.Fatalf("valid transport cursor chain failed: %v", err)
	}
	warm[0] = transport("connect", 4, requestFacts(2, testSchema(), 1, 1, "", ""))
	if err := validateTransportIdentities(state, capture, bootstrap, warm); err == nil {
		t.Fatal("changed warm client generation was accepted")
	}

	warm[0] = transport("connect", 4, requestFacts(1, testSchema(), 1, 1, "", ""))
	warm[1].CursorFingerprints[0] = hashFingerprint("different-cursor")
	if err := validateTransportIdentities(state, capture, bootstrap, warm); err == nil {
		t.Fatal("changed warm cursor fingerprint was accepted")
	}

	warm = []transportObservation{
		transport("connect", 4, requestFacts(1, testSchema(), 1, 1, "", "")),
		transportWithPull("pull", 5, requestFacts(1, testSchema(), 1, 1, "", ""), "cursor-b", "cursor-c"),
	}
	bootstrap.Observations[2].PullResponseFacts = json.RawMessage(validPullResponseFacts("different-cursor"))
	if err := validateTransportIdentities(state, capture, bootstrap, warm); err == nil {
		t.Fatal("warm pull unrelated to the bootstrap response was accepted")
	}

	bootstrap = validBootstrapTrace(testSchema())
	differentCursor := "different-cursor"
	state.ScopeStates[0].Cursor = &differentCursor
	if err := validateTransportIdentities(state, capture, bootstrap, warm); err == nil {
		t.Fatal("durable cursor unrelated to the warm response was accepted")
	}
}

func TestWarmConnectFinalEvidenceRequiresAuthoredDurabilityCounts(t *testing.T) {
	scenario := loadAuthoredScenario(t)
	expected := warmConnectExpectedState(scenario)
	if expected == nil || len(expected.Clients) != 1 {
		t.Fatal("authored warm-connect client state is incomplete")
	}
	capture := validFinalCapture(*expected)
	state, err := decodeClientState(capture.ClientState)
	if err != nil {
		t.Fatalf("decode valid client state: %v", err)
	}
	expected.Clients[0].QueueCount = nil
	if err := validateFinalClientEvidence(scenario, state, capture); err == nil {
		t.Fatal("warm-connect evidence without authored mutation ledger count was accepted")
	}
}

func newUnitCoordinator(t *testing.T) *Coordinator {
	t.Helper()
	coordinator, err := NewCoordinator(CoordinatorConfig{
		Scenario:  loadAuthoredScenario(t),
		Platform:  "ios",
		ServerURL: "http://127.0.0.1:8080",
		AuthToken: "unit-token",
	})
	if err != nil {
		t.Fatalf("create coordinator: %v", err)
	}
	if err := coordinator.Prepare(context.Background()); err != nil {
		t.Fatalf("prepare coordinator: %v", err)
	}
	return coordinator
}

func closeUnitCoordinator(t *testing.T, coordinator *Coordinator) {
	t.Helper()
	if err := coordinator.Close(context.Background()); err != nil {
		t.Errorf("close coordinator: %v", err)
	}
}

func exchangeRequestForTest(coordinator *Coordinator, body []byte) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "http://coordinator.test/exchange", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+coordinator.Token())
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	coordinator.ServeHTTP(response, request)
	return response
}

func loadAuthoredScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	scenario, err := LoadWarmConnectScenario(context.Background(), repoRoot)
	if err != nil {
		t.Fatalf("load authored warm-connect scenario: %v", err)
	}
	return scenario
}

func testSchema() clientSchema {
	return clientSchema{Version: 1, Hash: "721d2c95e6f34cd9733feea9f5118fba391eee10d07663dad066cfc59439fa44"}
}

func validBootstrapTrace(schema clientSchema) traceSnapshot {
	return traceSnapshot{
		Observations: []transportObservation{
			transport("connect", 1, requestFacts(0, schema, 0, 0, "", "")),
			transportWithResponse("rebuild", 2, requestFacts(1, schema, 0, 1, "rebuild-a", "scope-a"), validRebuildResponseFacts()),
			transportWithPull("pull", 3, requestFacts(1, schema, 1, 1, "", ""), "cursor-a", "cursor-b"),
		},
		SequenceCheckpoint: 3,
	}
}

func validWarmTrace(bootstrap traceSnapshot) traceSnapshot {
	return traceSnapshot{
		Observations: append(append([]transportObservation(nil), bootstrap.Observations...),
			transport("connect", 4, requestFacts(1, testSchema(), 1, 1, "", "")),
			transportWithPull("pull", 5, requestFacts(1, testSchema(), 1, 1, "", ""), "cursor-b", "cursor-c"),
		),
		SequenceCheckpoint: 5,
	}
}

func transport(operation string, sequence uint64, facts json.RawMessage) transportObservation {
	return transportObservation{Sequence: sequence, OperationClass: operation, StatusCode: 200, DurationNanoseconds: 1, RequestFacts: facts}
}

func transportWithResponse(operation string, sequence uint64, facts json.RawMessage, response string) transportObservation {
	value := transport(operation, sequence, facts)
	value.RebuildResponseFacts = json.RawMessage(response)
	return value
}

func transportWithPull(operation string, sequence uint64, facts json.RawMessage, requestCursor, responseCursor string) transportObservation {
	value := transport(operation, sequence, facts)
	value.PullResponseFacts = json.RawMessage(validPullResponseFacts(responseCursor))
	complete := true
	value.CursorFingerprints = []string{hashFingerprint(requestCursor)}
	value.CursorFingerprintsComplete = &complete
	return value
}

func validRebuildResponseFacts() string {
	value, err := json.Marshal(map[string]any{
		"record_count": 0, "has_more": false, "has_cursor": false, "has_final_scope_cursor": true,
		"has_checksum": true, "scope_fingerprint": hashFingerprint("scope-a"),
		"final_scope_cursor_fingerprint": hashFingerprint("cursor-a"),
	})
	if err != nil {
		panic(err)
	}
	return string(value)
}

func validPullResponseFacts(cursor string) string {
	value, err := json.Marshal(map[string]any{
		"change_count": 0, "has_more": false, "rebuild_scope_count": 0, "checksum_count": 1,
		"scope_cursor_fingerprints":          []string{hashFingerprint(cursor)},
		"scope_cursor_fingerprints_complete": true,
	})
	if err != nil {
		panic(err)
	}
	return string(value)
}

func proofObligationIndex(scenario scenarios.Scenario, id string) int {
	for index := range scenario.ProofObligations {
		if string(scenario.ProofObligations[index].ObligationID) == id {
			return index
		}
	}
	return -1
}

func requestFacts(generation uint64, schema clientSchema, scopeSet, scopeCount uint64, rebuildID, scope string) json.RawMessage {
	values := map[string]any{
		"client_generation": generation,
		"schema_version":    schema.Version,
		"schema_hash":       schema.Hash,
		"scope_set_version": scopeSet,
		"scope_count":       scopeCount,
	}
	if rebuildID != "" {
		values["rebuild_id_fingerprint"] = hashFingerprint(rebuildID)
	}
	if scope != "" {
		values["scope_fingerprint"] = hashFingerprint(scope)
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		panic(err)
	}
	return encoded
}

func validFinalCapture(expected scenarios.StateFacts) finalCapture {
	client := expected.Clients[0]
	row := expected.Rows[0]
	checkpoint := client.Checkpoints[0]
	provenance := client.Provenance[0]
	checksumJSON := func(digest string) string {
		value, err := json.Marshal(map[string]any{"algorithm": "sha256", "version": 1, "encoding": "hex", "digest": digest})
		if err != nil {
			panic(err)
		}
		return string(value)
	}
	state := map[string]any{
		"schema":                          client.CurrentSchema,
		"scopeStates":                     []any{map[string]any{"scopeID": checkpoint.ScopeID, "cursor": "cursor-c", "checksum": checksumJSON(*checkpoint.Checksum), "localChecksum": checksumJSON(*checkpoint.Checksum), "generation": 1}},
		"scopeRows":                       []any{map[string]any{"scopeID": checkpoint.ScopeID, "tableName": "runtime_items", "recordID": "runtime-row-a", "checksum": row.Checksum, "generation": 1}},
		"rebuildAttempts":                 []any{},
		"applicationRowCount":             *client.RowCount,
		"mutationLedgerCount":             *client.QueueCount,
		"mutationOutcomeCount":            *client.OutcomeCount,
		"sealedBatchCount":                *client.SealedBatchCount,
		"rejectedMutationCount":           0,
		"scopeStateCount":                 *client.CheckpointCount,
		"scopeRowCount":                   1,
		"provenanceCount":                 *client.ProvenanceCount,
		"rowMetadataCount":                1,
		"rebuildAttemptCount":             0,
		"rebuildReceiptCount":             1,
		"provenanceMaintenanceWorkCursor": "0",
	}
	stateRaw, err := json.Marshal(state)
	if err != nil {
		panic(err)
	}
	proofRaw, err := json.Marshal(map[string]any{
		"row_metadata": map[string]any{"table_name": "runtime_items", "record_id": "runtime-row-a", "server_version": provenance.Version, "row_checksum": checksumJSON(row.Checksum)},
		"rebuild_receipt_proofs": []any{map[string]any{
			"rebuild_id_fingerprint": hashFingerprint("rebuild-a"), "page_count": 1, "returned_record_count": 0,
			"request_chain_valid": true, "records_in_canonical_order": true, "row_checksums_valid": true,
			"scope_checksum_valid": true, "final_checksum_matches_local": false,
		}},
	})
	if err != nil {
		panic(err)
	}
	provenanceRaw, err := json.Marshal([]any{map[string]any{"scopeID": checkpoint.ScopeID, "tableName": "runtime_items", "recordID": "runtime-row-a", "checksum": row.Checksum, "generation": 1}})
	if err != nil {
		panic(err)
	}
	return finalCapture{
		ClientState:  stateRaw,
		Pending:      json.RawMessage(`[]`),
		Rejected:     json.RawMessage(`[]`),
		Status:       json.RawMessage(`{"state":"ready","retry_at":null,"operation":null,"failure":null}`),
		Events:       json.RawMessage(`[{"type":"rebuild_completed","scope_id":"scope-a","rebuild_id":"rebuild-a"}]`),
		Provenance:   provenanceRaw,
		DurableProof: proofRaw,
	}
}
