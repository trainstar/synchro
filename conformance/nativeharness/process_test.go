package nativeharness

import (
	"encoding/json"
	"testing"
)

func TestValidateRunnerResponseAcceptsClientCallResult(t *testing.T) {
	result, err := validateRunnerResponse([]byte(`{"schema_version":1,"outcome":"passed","result":{"call_id":"sync_cycle","state":"completed","completion":"idle","transport_observations":{"observations":[],"overflowed":false,"sequence_checkpoint":0}},"error_code":null}`))
	if err != nil {
		t.Fatalf("validate runner response: %v", err)
	}
	call, err := runnerClientCallResult(result)
	if err != nil {
		t.Fatalf("convert client call result: %v", err)
	}
	if call.CallID != "sync_cycle" || call.State != "completed" || call.Completion != "idle" {
		t.Fatalf("unexpected client call result: %+v", call)
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

func TestValidateRunnerResponseRejectsMalformedRebuildReceiptProof(t *testing.T) {
	validProof := `"rebuild_id_fingerprint":"rebuild-fingerprint","page_count":2,"returned_record_count":2,"request_chain_valid":true,"records_in_canonical_order":true,"row_checksums_valid":true,"scope_checksum_valid":true,"final_checksum_matches_local":true`
	for _, proof := range []string{
		`"page_count":2,"returned_record_count":2,"request_chain_valid":true,"records_in_canonical_order":true,"row_checksums_valid":true,"scope_checksum_valid":true,"final_checksum_matches_local":true`,
		validProof + `,"unknown":true`,
	} {
		data := `{"schema_version":1,"outcome":"passed","result":{"rebuild_receipt_proofs":[{` + proof + `}],"transport_observations":{"observations":[],"overflowed":false,"sequence_checkpoint":0}},"error_code":null}`
		if _, err := validateRunnerResponse([]byte(data)); err == nil {
			t.Fatal("accepted malformed rebuild receipt proof")
		}
	}
}

func TestValidateRunnerResponseAcceptsPassedResult(t *testing.T) {
	result, err := validateRunnerResponse([]byte(`{"schema_version":1,"outcome":"passed","result":{"status":"ready","pending_change_count":0,"scope_states":[{"scope_id":"scope-a","cursor":"cursor-a","checksum":"checksum-a","local_checksum":"checksum-a","generation":1}],"scope_rows":[{"scope_id":"scope-a","table_name":"items","record_id":"row-a","checksum":"row-checksum","generation":1}],"row_metadata":{"table_name":"items","record_id":"row-a","server_version":"version-a","row_checksum":"checksum-a"},"rebuild_attempts":[],"transport_observations":{"observations":[],"overflowed":false,"sequence_checkpoint":0}},"error_code":null}`))
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

func TestValidateRunnerResponseAcceptsApplicationRows(t *testing.T) {
	result, err := validateRunnerResponse([]byte(`{"schema_version":1,"outcome":"passed","result":{"application_rows":[{"id":"row-a"}],"transport_observations":{"observations":[],"overflowed":false,"sequence_checkpoint":0}},"error_code":null}`))
	if err != nil {
		t.Fatalf("validate application rows: %v", err)
	}
	if len(result.ApplicationRows) != 1 || string(result.ApplicationRows[0]["id"]) != `"row-a"` {
		t.Fatalf("unexpected application rows: %+v", result.ApplicationRows)
	}
}

func TestValidateRunnerResponseRejectsInvalidShapes(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{name: "wrong schema", data: `{"schema_version":2,"outcome":"passed","result":{"status":"ready"},"error_code":null}`},
		{name: "passed without result", data: `{"schema_version":1,"outcome":"passed","result":null,"error_code":null}`},
		{name: "error without code", data: `{"schema_version":1,"outcome":"error","result":null,"error_code":null}`},
		{name: "unknown outcome", data: `{"schema_version":1,"outcome":"unknown","result":null,"error_code":null}`},
		{name: "unknown member", data: `{"schema_version":1,"outcome":"passed","result":{"status":"ready","secret":"x","transport_observations":{"observations":[],"overflowed":false,"sequence_checkpoint":0}},"error_code":null}`},
		{name: "missing transport observations", data: `{"schema_version":1,"outcome":"passed","result":{"status":"ready"},"error_code":null}`},
		{name: "trailing value", data: `{"schema_version":1,"outcome":"passed","result":{"status":"ready"},"error_code":null} {}`},
		{name: "duplicate member", data: `{"schema_version":1,"schema_version":1,"outcome":"passed","result":{"transport_observations":{"observations":[],"overflowed":false,"sequence_checkpoint":0}},"error_code":null}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := validateRunnerResponse([]byte(test.data)); err == nil {
				t.Fatal("invalid runner response passed validation")
			}
		})
	}
}

func TestValidateRunnerResponseValidatesRawTransportObservations(t *testing.T) {
	valid := `{"schema_version":1,"outcome":"passed","result":{"transport_observations":{"observations":[{"sequence":1,"operation_class":"pull","status_code":200,"retryable":false,"duration_nanoseconds":1,"cursor_fingerprints":["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],"cursor_fingerprints_complete":true,"request_facts":{"client_generation":1,"schema_version":1,"schema_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","scope_set_version":1,"scope_count":1,"limit":1},"pull_response_facts":{"change_count":1,"has_more":false,"rebuild_scope_count":0,"checksum_count":1}}],"overflowed":false,"sequence_checkpoint":1}},"error_code":null}`
	if _, err := validateRunnerResponse([]byte(valid)); err != nil {
		t.Fatalf("valid transport observations rejected: %v", err)
	}
	for _, test := range []struct {
		name string
		body string
	}{
		{name: "overflow", body: `{"observations":[],"overflowed":true,"sequence_checkpoint":0}`},
		{name: "omitted range", body: `{"observations":[{"sequence":2,"operation_class":"connect","status_code":200,"duration_nanoseconds":1}],"overflowed":false,"sequence_checkpoint":2}`},
		{name: "unknown class", body: `{"observations":[{"sequence":1,"operation_class":"unknown","status_code":200,"duration_nanoseconds":1}],"overflowed":false,"sequence_checkpoint":1}`},
		{name: "zero duration", body: `{"observations":[{"sequence":1,"operation_class":"connect","status_code":200,"duration_nanoseconds":0}],"overflowed":false,"sequence_checkpoint":1}`},
		{name: "status out of bounds", body: `{"observations":[{"sequence":1,"operation_class":"connect","status_code":99,"duration_nanoseconds":1}],"overflowed":false,"sequence_checkpoint":1}`},
		{name: "cursor on connect", body: `{"observations":[{"sequence":1,"operation_class":"connect","status_code":200,"duration_nanoseconds":1,"cursor_fingerprints":["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],"cursor_fingerprints_complete":true}],"overflowed":false,"sequence_checkpoint":1}`},
		{name: "pull cursor metadata missing", body: `{"observations":[{"sequence":1,"operation_class":"pull","status_code":200,"duration_nanoseconds":1}],"overflowed":false,"sequence_checkpoint":1}`},
		{name: "pull cursor metadata incomplete", body: `{"observations":[{"sequence":1,"operation_class":"pull","status_code":200,"duration_nanoseconds":1,"cursor_fingerprints":[],"cursor_fingerprints_complete":false}],"overflowed":false,"sequence_checkpoint":1}`},
		{name: "pull request facts missing", body: `{"observations":[{"sequence":1,"operation_class":"pull","status_code":200,"duration_nanoseconds":1,"cursor_fingerprints":[],"cursor_fingerprints_complete":true,"pull_response_facts":{"change_count":0,"has_more":false,"rebuild_scope_count":0,"checksum_count":1}}],"overflowed":false,"sequence_checkpoint":1}`},
		{name: "pull response facts missing", body: `{"observations":[{"sequence":1,"operation_class":"pull","status_code":200,"duration_nanoseconds":1,"cursor_fingerprints":[],"cursor_fingerprints_complete":true,"request_facts":{"client_generation":1,"schema_version":1,"schema_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","scope_set_version":1,"scope_count":1,"limit":1}}],"overflowed":false,"sequence_checkpoint":1}`},
		{name: "unknown request fact", body: `{"observations":[{"sequence":1,"operation_class":"pull","status_code":200,"duration_nanoseconds":1,"cursor_fingerprints":[],"cursor_fingerprints_complete":true,"request_facts":{"client_generation":1,"schema_version":1,"schema_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","scope_set_version":1,"scope_count":1,"limit":1,"secret":"x"},"pull_response_facts":{"change_count":0,"has_more":false,"rebuild_scope_count":0,"checksum_count":1}}],"overflowed":false,"sequence_checkpoint":1}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := `{"schema_version":1,"outcome":"passed","result":{"transport_observations":` + test.body + `},"error_code":null}`
			if _, err := validateRunnerResponse([]byte(data)); err == nil {
				t.Fatal("invalid transport observations passed validation")
			}
		})
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
