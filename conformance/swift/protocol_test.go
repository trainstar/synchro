package swift

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestValidateRunnerResponseAcceptsClientCallResult(t *testing.T) {
	result, err := validateRunnerResponse([]byte(`{"schema_version":1,"outcome":"passed","result":{"call_id":"sync_cycle","state":"completed","completion":"error","call_error_category":"blocking_failure","process_id":"1234","database_identity_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","transport_observations":{"observations":[],"overflowed":false,"sequence_checkpoint":0}},"error_code":null}`))
	if err != nil {
		t.Fatalf("validate runner response: %v", err)
	}
	call, err := runnerClientCallResult(result)
	if err != nil {
		t.Fatalf("convert client call result: %v", err)
	}
	if call.CallID != "sync_cycle" || call.State != "completed" || call.Completion != "error" || call.CallErrorCategory != "blocking_failure" {
		t.Fatalf("unexpected client call result: %+v", call)
	}
}

func TestSynchronizationResultRetainsCallErrorCategory(t *testing.T) {
	result := synchronizationResult("error", "blocking_failure", nil, operationWindow{})
	if result.CallErrorCategory != "blocking_failure" {
		t.Fatalf("call error category = %q, want blocking_failure", result.CallErrorCategory)
	}
}

func TestCallResultWithWindowRetainsCallErrorCategory(t *testing.T) {
	result := callResultWithWindow(callResult{
		CallID:            "sync_cycle",
		State:             "completed",
		Completion:        "error",
		CallErrorCategory: "blocking_failure",
	}, operationWindow{duration: time.Nanosecond})
	if result.CallErrorCategory != "blocking_failure" {
		t.Fatalf("call error category = %q, want blocking_failure", result.CallErrorCategory)
	}
}

func TestValidateRunnerResponseRejectsInvalidRetainedDeleteProof(t *testing.T) {
	for _, fields := range []string{
		`"rows_affected":0,"retained_delete_captured":false`,
		`"rows_affected":1,"retained_delete_captured":true`,
		`"retained_delete_captured":true`,
	} {
		data := `{"schema_version":1,"outcome":"passed","result":{` + fields + `,"process_id":"1234","database_identity_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","transport_observations":{"observations":[],"overflowed":false,"sequence_checkpoint":0}},"error_code":null}`
		if _, err := validateRunnerResponse([]byte(data)); err == nil {
			t.Fatalf("accepted invalid retained delete proof: %s", fields)
		}
	}
}

func TestRunnerClientCallResultRejectsIncompleteResult(t *testing.T) {
	callID := "sync_cycle"
	state := "completed"
	for _, result := range []runnerResult{
		{},
		{CallID: &callID},
		{State: &state},
	} {
		if _, err := runnerClientCallResult(result); err == nil {
			t.Fatal("expected incomplete client call result to fail")
		}
	}
}

func TestValidateRunnerResponseRejectsMalformedRebuildReceipt(t *testing.T) {
	members := []string{
		`"rebuild_id_fingerprint":"` + strings.Repeat("e", 64) + `"`,
		`"page_count":2`,
		`"returned_record_count":101`,
		`"request_chain_expected":["first","second"]`,
		`"request_chain_observed":["first","second"]`,
		`"records_in_canonical_order":true`,
		`"row_checksums_valid":true`,
	}
	response := func(receipt []string) []byte {
		return []byte(`{"schema_version":1,"outcome":"passed","result":{"rebuild_receipts":[{` + strings.Join(receipt, ",") + `}],"process_id":"1234","database_identity_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","transport_observations":{"observations":[],"overflowed":false,"sequence_checkpoint":0}},"error_code":null}`)
	}
	result, err := validateRunnerResponse(response(members))
	if err != nil || len(result.RebuildReceipts) != 1 || result.RebuildReceipts[0].ReturnedRecordCount != 101 {
		t.Fatalf("valid rebuild receipt rejected: result=%#v err=%v", result.RebuildReceipts, err)
	}
	for index := range members {
		name := strings.Trim(strings.SplitN(members[index], ":", 2)[0], `"`)
		t.Run("missing "+name, func(t *testing.T) {
			receipt := append(append([]string(nil), members[:index]...), members[index+1:]...)
			if _, err := validateRunnerResponse(response(receipt)); err == nil {
				t.Fatal("accepted a rebuild receipt without a required member")
			}
		})
	}
	t.Run("unknown member", func(t *testing.T) {
		if _, err := validateRunnerResponse(response(append(append([]string(nil), members...), `"unknown":true`))); err == nil {
			t.Fatal("accepted a rebuild receipt with an unknown member")
		}
	})
}

func TestValidateRunnerResponseAcceptsPassedResult(t *testing.T) {
	result, err := validateRunnerResponse([]byte(`{"schema_version":1,"outcome":"passed","result":{"status":"ready","pending_change_count":0,"scope_states":[{"scope_id":"scope-a","cursor":"cursor-a","checksum":"checksum-a","local_checksum":"checksum-a","generation":1}],"scope_rows":[{"scope_id":"scope-a","table_name":"items","record_id":"row-a","checksum":"row-checksum","generation":1}],"row_metadata":{"table_name":"items","record_id":"row-a","server_version":"version-a","row_checksum":"checksum-a"},"rebuild_attempts":[],"process_id":"1234","database_identity_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","transport_observations":{"observations":[],"overflowed":false,"sequence_checkpoint":0}},"error_code":null}`))
	if err != nil {
		t.Fatalf("validate runner response: %v", err)
	}
	if result.Status == nil || *result.Status != "ready" {
		t.Fatal("runner status was not retained")
	}
	if result.PendingChangeCount == nil || *result.PendingChangeCount != 0 || len(result.ScopeStates) != 1 || len(result.ScopeRows) != 1 || result.RowMetadata == nil || len(result.RebuildAttempts) != 0 {
		t.Fatal("runner scope inspection was not retained")
	}
}

func TestValidateRunnerResponseDecodesAtomicCaptureFacts(t *testing.T) {
	data := `{"schema_version":1,"outcome":"passed","result":{"status":"ready","pending_change_count":0,"application_row_count":0,"mutation_ledger_count":0,"mutation_outcome_count":0,"sealed_batch_count":0,"rejected_mutation_count":0,"scope_state_count":0,"scope_row_count":0,"provenance_count":0,"row_metadata_count":1,"rebuild_attempt_count":0,"rebuild_receipt_count":0,"application_rows":[],"retained_mutations":[],"rejected_mutations":[],"scope_states":[],"scope_rows":[],"row_metadata_records":[{"table_name":"items","record_id":"row-a","server_version":"version-a","row_checksum":null}],"rebuild_attempts":[],"rebuild_receipts":[],"scope_states_truncated":false,"scope_rows_truncated":false,"rebuild_attempts_truncated":false,"rebuild_receipts_truncated":false,"row_metadata_truncated":false,"capture_overflowed":false,"provenance_maintenance_work_cursor":0,"events":[],"process_id":"1234","database_identity_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","transport_observations":{"observations":[],"overflowed":false,"sequence_checkpoint":0}},"error_code":null}`
	result, err := validateRunnerResponse([]byte(data))
	if err != nil {
		t.Fatalf("decode atomic capture: %v", err)
	}
	if err := validateCaptureResult(result); err != nil {
		t.Fatalf("validate atomic capture: %v", err)
	}
	if result.ScopeStatesTruncated == nil || *result.ScopeStatesTruncated || result.CaptureOverflowed == nil || *result.CaptureOverflowed || len(result.RowMetadataRecords) != 1 || result.ProvenanceMaintenanceWorkCursor == nil || *result.ProvenanceMaintenanceWorkCursor != 0 {
		t.Fatalf("atomic capture facts were not retained: %+v", result)
	}
}

func TestValidateRunnerResponseRejectsLegacyOrIncompleteAtomicCapture(t *testing.T) {
	base := `{"schema_version":1,"outcome":"passed","result":{"status":"ready","pending_change_count":0,"application_row_count":0,"mutation_ledger_count":0,"mutation_outcome_count":0,"sealed_batch_count":0,"rejected_mutation_count":0,"scope_state_count":0,"scope_row_count":0,"provenance_count":0,"row_metadata_count":0,"rebuild_attempt_count":0,"rebuild_receipt_count":0,"application_rows":[],"retained_mutations":[],"rejected_mutations":[],"scope_states":[],"scope_rows":[],"row_metadata_records":[],"rebuild_attempts":[],"rebuild_receipts":[],"scope_states_truncated":false,"scope_rows_truncated":false,"rebuild_attempts_truncated":false,"rebuild_receipts_truncated":false,"row_metadata_truncated":false,"capture_overflowed":false,"provenance_maintenance_work_cursor":0,"events":[],"process_id":"1234","database_identity_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","transport_observations":{"observations":[],"overflowed":false,"sequence_checkpoint":0}},"error_code":null}`
	for _, data := range []string{
		strings.Replace(base, `"capture_overflowed":false`, `"capture_overflowed":true`, 1),
		strings.Replace(base, `,"capture_overflowed":false`, ``, 1),
		strings.Replace(base, `"rebuild_receipts":[]`, `"rebuild_receipt_proofs":[]`, 1),
	} {
		result, err := validateRunnerResponse([]byte(data))
		if err == nil {
			err = validateCaptureResult(result)
		}
		if err == nil {
			t.Fatal("accepted incomplete or inconsistent atomic capture")
		}
	}
}

func TestValidateRunnerResponseAcceptsApplicationRows(t *testing.T) {
	result, err := validateRunnerResponse([]byte(`{"schema_version":1,"outcome":"passed","result":{"application_rows":[{"id":"row-a"}],"process_id":"1234","database_identity_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","transport_observations":{"observations":[],"overflowed":false,"sequence_checkpoint":0}},"error_code":null}`))
	if err != nil {
		t.Fatalf("validate application rows: %v", err)
	}
	if len(result.ApplicationRows) != 1 || string(result.ApplicationRows[0]["id"]) != `"row-a"` {
		t.Fatalf("unexpected application rows: %+v", result.ApplicationRows)
	}
}

func TestValidateRunnerResponseAcceptsLargeAggregateCounts(t *testing.T) {
	result, err := validateRunnerResponse([]byte(`{"schema_version":1,"outcome":"passed","result":{"application_row_count":1000,"mutation_ledger_count":1000,"mutation_outcome_count":1000,"sealed_batch_count":1,"rejected_mutation_count":1,"scope_state_count":1,"scope_row_count":1000,"provenance_count":1000,"row_metadata_count":1000,"rebuild_attempt_count":1,"rebuild_receipt_count":10,"process_id":"1234","database_identity_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","transport_observations":{"observations":[],"overflowed":false,"sequence_checkpoint":0}},"error_code":null}`))
	if err != nil {
		t.Fatalf("validate aggregate counts: %v", err)
	}
	if result.ApplicationRowCount == nil || *result.ApplicationRowCount != 1000 || result.ProvenanceCount == nil || *result.ProvenanceCount != 1000 {
		t.Fatalf("aggregate counts were not retained: %+v", result)
	}
}

// validPullRunnerResponse is a complete passed response. Each negative below
// starts from it and changes one fact, so a rejection can come only from the
// check for that fact.
const validPullRunnerResponse = `{"schema_version":1,"outcome":"passed","result":{"process_id":"1234","database_identity_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","transport_observations":{"observations":[{"sequence":1,"operation_class":"pull","status_code":200,"retryable":null,"duration_nanoseconds":1,"cursor_fingerprints":["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],"cursor_fingerprints_complete":true,"request_facts":{"client_generation":1,"schema_version":1,"schema_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","scope_set_version":1,"scope_count":1,"limit":1},"pull_response_facts":{"change_count":1,"has_more":false,"rebuild_scope_count":0,"checksum_count":1,"scope_cursor_fingerprints":["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"],"scope_cursor_fingerprints_complete":true}}],"overflowed":false,"sequence_checkpoint":1}},"error_code":null}`

type runnerResponseParts struct {
	envelope    map[string]any
	result      map[string]any
	snapshot    map[string]any
	observation map[string]any
}

func runnerResponseWith(t *testing.T, change func(parts runnerResponseParts)) []byte {
	t.Helper()
	var envelope map[string]any
	decoder := json.NewDecoder(strings.NewReader(validPullRunnerResponse))
	decoder.UseNumber()
	if err := decoder.Decode(&envelope); err != nil {
		t.Fatalf("decode valid runner response: %v", err)
	}
	result := envelope["result"].(map[string]any)
	snapshot := result["transport_observations"].(map[string]any)
	observation := snapshot["observations"].([]any)[0].(map[string]any)
	change(runnerResponseParts{envelope: envelope, result: result, snapshot: snapshot, observation: observation})
	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("encode changed runner response: %v", err)
	}
	return encoded
}

// asConnect turns the valid pull observation into a valid connect observation.
func asConnect(parts runnerResponseParts) {
	parts.observation["operation_class"] = "connect"
	for _, member := range []string{"cursor_fingerprints", "cursor_fingerprints_complete", "pull_response_facts"} {
		delete(parts.observation, member)
	}
	parts.observation["request_facts"] = map[string]any{
		"client_generation": 1, "schema_version": 1, "schema_hash": strings.Repeat("a", 64),
		"protocol_version": 3, "scope_set_version": 1, "scope_count": 1,
	}
}

// asConnectFailure turns the valid observation into a retryable connect failure.
func asConnectFailure(parts runnerResponseParts, status int) {
	asConnect(parts)
	parts.observation["status_code"] = status
	parts.observation["error_code"] = "temporary_unavailable"
	parts.observation["retryable"] = true
}

func TestValidateRunnerResponseRejectsInvalidShapes(t *testing.T) {
	if _, err := validateRunnerResponse([]byte(validPullRunnerResponse)); err != nil {
		t.Fatalf("valid runner response rejected: %v", err)
	}
	validError := `{"schema_version":1,"outcome":"error","result":null,"error_code":"capture_row_cardinality"}`
	if _, err := validateRunnerResponse([]byte(validError)); runnerFailureCode(err) != "capture_row_cardinality" {
		t.Fatalf("valid runner error response = %v", err)
	}
	tests := []struct {
		name string
		data []byte
	}{
		{name: "wrong schema", data: runnerResponseWith(t, func(parts runnerResponseParts) { parts.envelope["schema_version"] = 2 })},
		{name: "passed without result", data: runnerResponseWith(t, func(parts runnerResponseParts) { parts.envelope["result"] = nil })},
		{name: "error without code", data: []byte(strings.Replace(validError, `"capture_row_cardinality"`, "null", 1))},
		{name: "unknown outcome", data: runnerResponseWith(t, func(parts runnerResponseParts) { parts.envelope["outcome"] = "unknown" })},
		{name: "unknown member", data: runnerResponseWith(t, func(parts runnerResponseParts) { parts.result["secret"] = "x" })},
		{name: "missing transport observations", data: runnerResponseWith(t, func(parts runnerResponseParts) { delete(parts.result, "transport_observations") })},
		{name: "trailing value", data: []byte(validPullRunnerResponse + ` {}`)},
		{name: "duplicate member", data: []byte(`{"schema_version":1,` + strings.TrimPrefix(validPullRunnerResponse, "{"))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := validateRunnerResponse(test.data); err == nil {
				t.Fatal("invalid runner response passed validation")
			}
		})
	}
}

func TestValidateRunnerResponseValidatesRawTransportObservations(t *testing.T) {
	if _, err := validateRunnerResponse([]byte(validPullRunnerResponse)); err != nil {
		t.Fatalf("valid pull transport observation rejected: %v", err)
	}
	if _, err := validateRunnerResponse(runnerResponseWith(t, asConnect)); err != nil {
		t.Fatalf("valid connect transport observation rejected: %v", err)
	}
	if _, err := validateRunnerResponse(runnerResponseWith(t, func(parts runnerResponseParts) {
		asConnect(parts)
		parts.observation["cursor_fingerprints"] = []any{}
		parts.observation["cursor_fingerprints_complete"] = true
	})); err != nil {
		t.Fatalf("valid connect cursor observation rejected: %v", err)
	}
	if _, err := validateRunnerResponse(runnerResponseWith(t, func(parts runnerResponseParts) { asConnectFailure(parts, 503) })); err != nil {
		t.Fatalf("valid connect transport failure rejected: %v", err)
	}
	for _, test := range []struct {
		name   string
		change func(parts runnerResponseParts)
	}{
		{name: "overflow", change: func(parts runnerResponseParts) { parts.snapshot["overflowed"] = true }},
		{name: "omitted range", change: func(parts runnerResponseParts) {
			asConnect(parts)
			parts.observation["sequence"] = 2
			parts.snapshot["sequence_checkpoint"] = 2
		}},
		{name: "unknown class", change: func(parts runnerResponseParts) {
			asConnect(parts)
			delete(parts.observation, "request_facts")
			parts.observation["operation_class"] = "unknown"
		}},
		{name: "zero duration", change: func(parts runnerResponseParts) {
			asConnect(parts)
			parts.observation["duration_nanoseconds"] = 0
		}},
		{name: "status below bounds", change: func(parts runnerResponseParts) { asConnectFailure(parts, 99) }},
		{name: "status above bounds", change: func(parts runnerResponseParts) { asConnectFailure(parts, 600) }},
		{name: "cursor on schemas", change: func(parts runnerResponseParts) {
			asConnect(parts)
			parts.observation["operation_class"] = "schemas"
			delete(parts.observation, "request_facts")
			parts.observation["cursor_fingerprints"] = []any{strings.Repeat("a", 64)}
			parts.observation["cursor_fingerprints_complete"] = true
		}},
		{name: "pull cursor metadata missing", change: func(parts runnerResponseParts) {
			delete(parts.observation, "cursor_fingerprints")
			delete(parts.observation, "cursor_fingerprints_complete")
		}},
		{name: "pull cursor metadata incomplete", change: func(parts runnerResponseParts) {
			parts.observation["cursor_fingerprints"] = []any{}
			parts.observation["cursor_fingerprints_complete"] = false
		}},
		{name: "pull request facts missing", change: func(parts runnerResponseParts) { delete(parts.observation, "request_facts") }},
		{name: "pull response facts missing", change: func(parts runnerResponseParts) { delete(parts.observation, "pull_response_facts") }},
		{name: "unknown request fact", change: func(parts runnerResponseParts) {
			parts.observation["request_facts"].(map[string]any)["secret"] = "x"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := validateRunnerResponse(runnerResponseWith(t, test.change)); err == nil {
				t.Fatal("invalid transport observations passed validation")
			}
		})
	}
}

func TestValidateRunnerResponseAcceptsEmptyScopePull(t *testing.T) {
	pull := func(scopeCount string) string {
		return `{"schema_version":1,"outcome":"passed","result":{"process_id":"1234","database_identity_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","transport_observations":{"observations":[{"sequence":1,"operation_class":"pull","status_code":200,"duration_nanoseconds":1,"cursor_fingerprints":[],"cursor_fingerprints_complete":true,"request_facts":{"client_generation":1,"schema_version":1,"schema_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","scope_set_version":1,"scope_count":` + scopeCount + `,"limit":1},"pull_response_facts":{"change_count":0,"has_more":false,"rebuild_scope_count":1,"checksum_count":1,"scope_cursor_fingerprints":[],"scope_cursor_fingerprints_complete":true}}],"overflowed":false,"sequence_checkpoint":1}},"error_code":null}`
	}
	result, err := validateRunnerResponse([]byte(pull("0")))
	if err != nil {
		t.Fatalf("empty-scope pull observation rejected: %v", err)
	}
	if err := validateTransportObservation(cloneTransportObservation(result.TransportObservations.Observations[0])); err != nil {
		t.Fatalf("cloned empty-scope pull observation rejected: %v", err)
	}
	if _, err := validateRunnerResponse([]byte(pull("-1"))); err == nil {
		t.Fatal("negative pull scope count passed validation")
	}
}

func TestValidateRunnerResponseAcceptsPushMutationCount(t *testing.T) {
	data := `{"schema_version":1,"outcome":"passed","result":{"process_id":"1234","database_identity_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","transport_observations":{"observations":[{"sequence":1,"operation_class":"push","status_code":200,"retryable":null,"duration_nanoseconds":1,"request_facts":{"client_generation":1,"schema_version":1,"schema_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","mutation_count":2}}],"overflowed":false,"sequence_checkpoint":1}},"error_code":null}`
	result, err := validateRunnerResponse([]byte(data))
	if err != nil {
		t.Fatalf("valid push observation rejected: %v", err)
	}
	facts := result.TransportObservations.Observations[0].RequestFacts
	if facts == nil || facts.MutationCount == nil || *facts.MutationCount != 2 {
		t.Fatalf("unexpected push request facts: %#v", facts)
	}
}

func TestTerminalRebuildResponseRequiresValidCursorFingerprint(t *testing.T) {
	valid := validTerminalRebuildObservation("terminal-cursor")
	if err := validateTransportObservation(valid); err != nil {
		t.Fatalf("valid terminal rebuild observation failed: %v", err)
	}

	invalid := valid
	response := *valid.RebuildResponseFacts
	response.FinalScopeCursorFingerprint = nil
	invalid.RebuildResponseFacts = &response
	if err := validateTransportObservation(invalid); err == nil {
		t.Fatal("terminal rebuild without cursor fingerprint passed")
	}
	response.FinalScopeCursorFingerprint = pointerString("invalid")
	if err := validateTransportObservation(invalid); err == nil {
		t.Fatal("terminal rebuild with invalid cursor fingerprint passed")
	}
}

func TestValidateRunnerResponseRejectsChangedTransportCheckpoint(t *testing.T) {
	first := &transportObservationSnapshot{
		Observations:       []transportObservation{{Sequence: 1, OperationClass: "connect", StatusCode: 200, DurationNanoseconds: 1}},
		SequenceCheckpoint: 1,
	}
	process := &runnerProcess{}
	if err := process.acceptTransportObservations(first); err != nil {
		t.Fatalf("accept first transport snapshot: %v", err)
	}
	changed := &transportObservationSnapshot{
		Observations:       []transportObservation{{Sequence: 1, OperationClass: "connect", StatusCode: 201, DurationNanoseconds: 1}},
		SequenceCheckpoint: 1,
	}
	if err := process.acceptTransportObservations(changed); err == nil {
		t.Fatal("changed transport checkpoint passed validation")
	}
}

func TestValidateRunnerResponseReturnsBoundedRunnerError(t *testing.T) {
	_, err := validateRunnerResponse([]byte(`{"schema_version":1,"outcome":"error","result":null,"error_code":"capture_row_cardinality"}`))
	if runnerFailureCode(err) != "capture_row_cardinality" {
		t.Fatalf("runner error = %v", err)
	}
}

func TestValidateRunnerResponseRejectsOversizedJSONL(t *testing.T) {
	if _, err := validateRunnerResponse(bytes.Repeat([]byte{' '}, maximumRunnerLineBytes+1)); err == nil {
		t.Fatal("accepted a response larger than the JSONL bound")
	}
}

func TestValidateRunnerResponseValidatesRawFailure(t *testing.T) {
	valid := `{"operation":"connecting","code":"auth_required","retryable":false,"message":"auth failed","recoveryAction":"none","metadata":{}}`
	data := `{"schema_version":1,"outcome":"passed","result":{"failure":` + valid + `,"process_id":"1234","database_identity_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","transport_observations":{"observations":[],"overflowed":false,"sequence_checkpoint":0}},"error_code":null}`
	if _, err := validateRunnerResponse([]byte(data)); err != nil {
		t.Fatalf("valid raw failure rejected: %v", err)
	}
	for _, failure := range []string{
		`{"operation":"connecting","code":"auth_required","retryable":false,"message":"auth failed","recoveryAction":"none"}`,
		valid[:len(valid)-1] + `,"unknown":true}`,
	} {
		invalid := `{"schema_version":1,"outcome":"passed","result":{"failure":` + failure + `,"process_id":"1234","database_identity_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","transport_observations":{"observations":[],"overflowed":false,"sequence_checkpoint":0}},"error_code":null}`
		if _, err := validateRunnerResponse([]byte(invalid)); err == nil {
			t.Fatal("accepted malformed raw failure")
		}
	}
}

func TestValidateRunnerResponseRequiresStrictEnvelope(t *testing.T) {
	for _, data := range []string{
		`{"schema_version":1,"outcome":"passed","result":{"transport_observations":{"observations":[],"overflowed":false,"sequence_checkpoint":0}}}`,
		`{"schema_version":1,"outcome":"passed","result":{"transport_observations":{"observations":[],"overflowed":false,"sequence_checkpoint":0}},"error_code":null,"extra":true}`,
	} {
		if _, err := validateRunnerResponse([]byte(data)); err == nil {
			t.Fatal("accepted incomplete or extended runner envelope")
		}
	}
}

func TestValidateRunnerCommandUsesCurrentOnlyProtocol(t *testing.T) {
	command := runnerCommand{
		SchemaVersion: 1,
		Operation:     "begin-call",
		CallID:        "sync_cycle",
		Method:        "retry-after-error",
	}
	if err := validateRunnerCommand(command); err != nil {
		t.Fatalf("validate current begin-call command: %v", err)
	}
	encoded, err := json.Marshal(command)
	if err != nil {
		t.Fatalf("encode current begin-call command: %v", err)
	}
	if string(encoded) == "" {
		t.Fatal("encoded current begin-call command is empty")
	}
	for _, invalid := range []runnerCommand{
		{SchemaVersion: 1, Operation: "begin-call", CallID: "sync_cycle", Method: "retry"},
		{SchemaVersion: 1, Operation: "lifecycle", LifecycleOperation: "background"},
		{SchemaVersion: 1, Operation: "local-action"},
	} {
		if err := validateRunnerCommand(invalid); err == nil {
			t.Fatal("accepted legacy or incomplete runner command")
		}
	}
}

func TestValidateRunnerCommandRejectsBounds(t *testing.T) {
	for _, command := range []runnerCommand{
		{SchemaVersion: 1, Operation: "begin-call", CallID: strings.Repeat("a", 129), Method: "start"},
		{SchemaVersion: 1, Operation: "begin-call", CallID: "sync_cycle", Method: "start", PullPageSize: 1001},
		{SchemaVersion: 1, Operation: "begin-call", CallID: "sync_cycle", Method: "start", PushBatchSize: 1001},
		{SchemaVersion: 1, Operation: "capture", RowSelectors: make([]runnerRowSelector, maximumRunnerSelectors+1)},
	} {
		if err := validateRunnerCommand(command); err == nil {
			t.Fatal("accepted an out-of-bounds runner command")
		}
	}
}

func TestValidateRunnerCommandAcceptsScalarRowSelector(t *testing.T) {
	command := runnerCommand{
		SchemaVersion: 1,
		Operation:     "capture",
		RowSelectors: []runnerRowSelector{{
			TableName:       "items",
			PrimaryKeyField: "id",
			PrimaryKey:      json.RawMessage(`"runtime-row-a"`),
		}},
	}
	if err := validateRunnerCommand(command); err != nil {
		t.Fatalf("validate scalar row selector: %v", err)
	}
}

func TestValidateRunnerCommandAcceptsPushBatchSizeForOpenOnly(t *testing.T) {
	open := runnerCommand{
		SchemaVersion: 1,
		Operation:     "open",
		DatabasePath:  "/tmp/client.sqlite",
		ServerURL:     "http://127.0.0.1:8090",
		AuthToken:     "token",
		ClientID:      "client-a",
		PushBatchSize: 1000,
	}
	if err := validateRunnerCommand(open); err != nil {
		t.Fatalf("validate open push batch size: %v", err)
	}
	nonOpen := runnerCommand{SchemaVersion: 1, Operation: "lifecycle", LifecycleOperation: "stop", PushBatchSize: 1}
	if err := validateRunnerCommand(nonOpen); err == nil {
		t.Fatal("push batch size passed outside open")
	}
}

func TestRunnerProcessRetainsImmutableNestedObservations(t *testing.T) {
	fingerprint := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	scopeFingerprint := strings.Repeat("b", 64)
	// Each call returns new values, so a mutation of one snapshot cannot change another.
	accepted := func() *transportObservationSnapshot {
		errorCode := "temporary_unavailable"
		retryable := true
		complete := true
		pullClientGeneration := int64(1)
		scopeSetVersion := int64(1)
		scopeCount := 1
		pullLimit := 100
		clientGeneration := int64(1)
		limit := 100
		requestScopeFingerprint := scopeFingerprint
		rebuildIDFingerprint := strings.Repeat("c", 64)
		cursorPresent := false
		responseBodySHA256 := strings.Repeat("d", 64)
		return &transportObservationSnapshot{
			Observations: []transportObservation{{
				Sequence:            1,
				OperationClass:      "pull",
				StatusCode:          503,
				ErrorCode:           &errorCode,
				Retryable:           &retryable,
				DurationNanoseconds: 1,
				RequestFacts: &transportRequestFacts{
					ClientGeneration: &pullClientGeneration,
					SchemaVersion:    1,
					SchemaHash:       strings.Repeat("e", 64),
					ScopeSetVersion:  &scopeSetVersion,
					ScopeCount:       &scopeCount,
					Limit:            &pullLimit,
				},
				CursorFingerprints:         []string{fingerprint},
				CursorFingerprintsComplete: &complete,
			}, {
				Sequence:            2,
				OperationClass:      "rebuild",
				StatusCode:          200,
				DurationNanoseconds: 1,
				RequestFacts: &transportRequestFacts{
					ClientGeneration:     &clientGeneration,
					SchemaVersion:        1,
					SchemaHash:           strings.Repeat("e", 64),
					Limit:                &limit,
					ScopeFingerprint:     &requestScopeFingerprint,
					RebuildIDFingerprint: &rebuildIDFingerprint,
					CursorPresent:        &cursorPresent,
				},
				RebuildResponseFacts: &transportRebuildResponseFacts{
					RecordCount:        1,
					HasMore:            true,
					HasCursor:          true,
					ScopeFingerprint:   scopeFingerprint,
					ResponseBodySHA256: &responseBodySHA256,
				},
			}},
			SequenceCheckpoint: 2,
		}
	}
	if err := validateTransportObservationSnapshot(accepted()); err != nil {
		t.Fatalf("accepted history fixture is invalid: %v", err)
	}
	requireRetained := func(boundary string, process *runnerProcess) {
		t.Helper()
		stored, err := process.transportObservationsAfter(0)
		if err != nil {
			t.Fatalf("%s: read retained history: %v", boundary, err)
		}
		if want := accepted().Observations; !reflect.DeepEqual(stored, want) {
			got, _ := json.Marshal(stored)
			expected, _ := json.Marshal(want)
			t.Errorf("%s changed retained history:\n got %s\nwant %s", boundary, got, expected)
		}
	}

	input := accepted()
	process := &runnerProcess{}
	if err := process.acceptTransportObservations(input); err != nil {
		t.Fatalf("accept observations: %v", err)
	}
	*input.Observations[0].ErrorCode = "changed"
	*input.Observations[0].Retryable = false
	input.Observations[0].CursorFingerprints[0] = "changed"
	*input.Observations[1].RequestFacts.ScopeFingerprint = "changed"
	*input.Observations[1].RebuildResponseFacts.ResponseBodySHA256 = "changed"
	requireRetained("accepted input mutation", process)

	process = &runnerProcess{}
	if err := process.acceptTransportObservations(accepted()); err != nil {
		t.Fatalf("accept observations: %v", err)
	}
	returned, err := process.transportObservationsAfter(0)
	if err != nil || len(returned) != 2 {
		t.Fatalf("read observations: %v, %d", err, len(returned))
	}
	*returned[0].ErrorCode = "returned mutation"
	*returned[0].Retryable = false
	returned[0].CursorFingerprints[0] = "returned mutation"
	*returned[1].RequestFacts.ScopeFingerprint = "returned mutation"
	*returned[1].RebuildResponseFacts.ResponseBodySHA256 = "returned mutation"
	requireRetained("returned observation mutation", process)
}
