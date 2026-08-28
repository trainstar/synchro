package swift

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestPlatformRebuildCursorMutationIsConformanceOnlyAndOneShot(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"cursor":"real-cursor","has_more":true,"row":{"large":9223372036854775807}}`))
	}))
	defer upstream.Close()
	databaseDirectory := t.TempDir()
	if err := os.Chmod(databaseDirectory, 0o700); err != nil {
		t.Fatalf("make test database directory private: %v", err)
	}
	platform, err := NewPlatform(Config{
		RunnerPath:                   os.Args[0],
		ApplicationDatabaseDirectory: databaseDirectory,
		ServerURL:                    upstream.URL,
		AuthToken:                    func(context.Context, Client) (string, error) { return "token", nil },
		Platform:                     "macos",
		AppVersion:                   "0.3.0",
	})
	if err != nil {
		t.Fatalf("create Swift platform: %v", err)
	}
	defer func() {
		if err := platform.Close(context.Background()); err != nil {
			t.Fatalf("close Swift platform: %v", err)
		}
	}()
	if err := platform.armRebuildCursorOverride("client-a", "forged-cursor"); err != nil {
		t.Fatalf("arm rebuild cursor mutation: %v", err)
	}
	requests := []struct {
		clientID string
		expected string
	}{
		{clientID: "client-b", expected: "real-cursor"},
		{clientID: "client-a", expected: "forged-cursor"},
		{clientID: "client-a", expected: "real-cursor"},
	}
	for index, request := range requests {
		response, err := http.Post(
			platform.config.ServerURL+"/sync/rebuild",
			"application/json",
			strings.NewReader(`{"client_id":"`+request.clientID+`"}`),
		)
		if err != nil {
			t.Fatalf("request proxied rebuild %d: %v", index, err)
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			t.Fatalf("read proxied rebuild %d: %v", index, readErr)
		}
		var value struct {
			Cursor string                     `json:"cursor"`
			Row    map[string]json.RawMessage `json:"row"`
		}
		if err := json.Unmarshal(body, &value); err != nil || value.Cursor != request.expected {
			t.Fatalf("proxied rebuild %d cursor = %q, want %q: %v", index, value.Cursor, request.expected, err)
		}
		if string(value.Row["large"]) != "9223372036854775807" {
			t.Fatalf("proxied rebuild %d changed Int64 row value", index)
		}
	}
	if got := platform.rebuildResponseCursors["client-a"]; got != cursorFingerprint("real-cursor") {
		t.Fatalf("recorded rebuild cursor fingerprint = %q", got)
	}
}

func TestPlatformConfigDefaultsAndBoundsPushBatchSize(t *testing.T) {
	databaseDirectory := t.TempDir()
	if err := os.Chmod(databaseDirectory, 0o700); err != nil {
		t.Fatalf("make test database directory private: %v", err)
	}
	config := Config{
		RunnerPath:                   os.Args[0],
		ApplicationDatabaseDirectory: databaseDirectory,
		ServerURL:                    "http://127.0.0.1:8090",
		AuthToken:                    func(context.Context, Client) (string, error) { return "token", nil },
		Platform:                     "macos",
		AppVersion:                   "0.3.0",
	}
	normalized, err := normalizePlatformConfig(config)
	if err != nil {
		t.Fatalf("normalize default push batch size: %v", err)
	}
	if normalized.PushBatchSize != 100 {
		t.Fatalf("default push batch size = %d, want 100", normalized.PushBatchSize)
	}
	config.PullPageSize = 1
	if normalized, err = normalizePlatformConfig(config); err != nil || normalized.PullPageSize != 1 {
		t.Fatalf("normalize minimum pull page size: %d, %v", normalized.PullPageSize, err)
	}
	config.PullPageSize = 1001
	if _, err := normalizePlatformConfig(config); err == nil {
		t.Fatal("over-bound pull page size passed")
	}
	config.PullPageSize = 0
	config.PushBatchSize = 1000
	if normalized, err = normalizePlatformConfig(config); err != nil || normalized.PushBatchSize != 1000 {
		t.Fatalf("normalize maximum push batch size: %d, %v", normalized.PushBatchSize, err)
	}
	config.PushBatchSize = 1001
	if _, err := normalizePlatformConfig(config); err == nil {
		t.Fatal("oversized push batch size passed platform validation")
	}
}

func TestRequestsSelectTheirNativeTransportBehavior(t *testing.T) {
	tests := []struct {
		name        string
		operation   scenarios.Operation
		wantClass   string
		wantDrop    bool
		wantBatchID string
	}{
		{
			name:      "connect",
			operation: scenarios.Operation{ContractOperation: "connect", Name: "send"},
			wantClass: "connect",
		},
		{
			name: "normal push",
			operation: scenarios.Operation{
				ContractOperation: "push",
				Name:              "submit",
				Payload:           pushDispatchPayload("apply"),
			},
			wantClass:   "push",
			wantBatchID: "batch-a",
		},
		{
			name: "response loss",
			operation: scenarios.Operation{
				ContractOperation: "push",
				Name:              "submit",
				Payload:           pushDispatchPayload("drop_after_server"),
			},
			wantClass:   "push",
			wantDrop:    true,
			wantBatchID: "batch-a",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			class, drop, batchID, err := requestDispatch(test.operation)
			if err != nil {
				t.Fatalf("select request behavior: %v", err)
			}
			if class != test.wantClass || drop != test.wantDrop || batchID != test.wantBatchID {
				t.Fatalf("behavior = %q, %t, %q", class, drop, batchID)
			}
		})
	}

	if _, _, _, err := requestDispatch(scenarios.Operation{ContractOperation: "local", Name: "write"}); err == nil {
		t.Fatal("local write was accepted as a transport request")
	}
}

func TestCaptureRejectsIncompleteOrAmbiguousDurableFacts(t *testing.T) {
	status := "ready"
	pending := 0
	zero := 0
	one := 1
	maintenanceCursor := int64(4)
	truncated := false
	truncatedValue := true
	overflowed := false
	complete := runnerResult{
		Status:                          &status,
		PendingChangeCount:              &pending,
		ApplicationRowCount:             &one,
		MutationLedgerCount:             &zero,
		MutationOutcomeCount:            &zero,
		SealedBatchCount:                &zero,
		RejectedMutationCount:           &zero,
		ScopeStateCount:                 &zero,
		ScopeRowCount:                   &one,
		ProvenanceCount:                 &one,
		RowMetadataCount:                &one,
		RebuildAttemptCount:             &zero,
		RebuildReceiptCount:             &zero,
		ApplicationRows:                 []map[string]json.RawMessage{},
		RetainedMutations:               []retainedMutation{},
		RejectedMutations:               []retainedRejection{},
		ScopeStates:                     []scopeStateRecord{},
		ScopeRows:                       []scopeRowRecord{{ScopeID: "scope-a", TableName: "items", RecordID: "row-a", Checksum: "checksum-a", Generation: 1}},
		RowMetadataRecords:              []rowMetadataRecord{{TableName: "items", RecordID: "row-a", ServerVersion: "version-a"}},
		RebuildAttempts:                 []rebuildAttemptRecord{},
		RebuildReceipts:                 []rebuildReceiptRecord{},
		ScopeStatesTruncated:            &truncated,
		ScopeRowsTruncated:              &truncated,
		RebuildAttemptsTruncated:        &truncated,
		RebuildReceiptsTruncated:        &truncated,
		RowMetadataTruncated:            &truncated,
		CaptureOverflowed:               &overflowed,
		ProvenanceMaintenanceWorkCursor: &maintenanceCursor,
		Events:                          []eventRecord{},
	}
	if err := validateCaptureResult(complete); err != nil {
		t.Fatalf("complete durable facts were rejected: %v", err)
	}

	incomplete := complete
	incomplete.RowMetadataRecords = nil
	if err := validateCaptureResult(incomplete); err == nil {
		t.Fatal("incomplete durable facts were accepted")
	}

	duplicated := complete
	duplicated.ScopeRows = append(duplicated.ScopeRows, duplicated.ScopeRows[0])
	if err := validateCaptureResult(duplicated); err == nil {
		t.Fatal("duplicated provenance was accepted")
	}

	duplicatedMetadata := complete
	duplicatedMetadata.RowMetadataRecords = append(duplicatedMetadata.RowMetadataRecords, duplicatedMetadata.RowMetadataRecords[0])
	if err := validateCaptureResult(duplicatedMetadata); err == nil {
		t.Fatal("duplicated row metadata was accepted")
	}

	negativeCursor := complete
	negative := int64(-1)
	negativeCursor.ProvenanceMaintenanceWorkCursor = &negative
	if err := validateCaptureResult(negativeCursor); err == nil {
		t.Fatal("negative provenance maintenance cursor was accepted")
	}

	digest := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	extendedChecksum := `{"algorithm":"sha256","version":1,"encoding":"hex","digest":"` + digest + `","unknown":true}`
	if _, err := swiftChecksumDigest(&extendedChecksum); err == nil {
		t.Fatal("extended checksum facts were accepted")
	}

	large := 1000
	bounded := complete
	bounded.ApplicationRowCount = &large
	bounded.ApplicationRows = nil
	bounded.ScopeRowCount = &large
	bounded.ScopeRows = nil
	bounded.ScopeRowsTruncated = &truncatedValue
	bounded.ProvenanceCount = &large
	bounded.RowMetadataCount = &large
	bounded.RowMetadataRecords = nil
	bounded.RowMetadataTruncated = &truncatedValue
	bounded.CaptureOverflowed = &truncatedValue
	if err := validateCaptureResult(bounded); err != nil {
		t.Fatalf("bounded aggregate capture was rejected: %v", err)
	}
	facts, err := clientFactsForSource("provenance", Client{UserID: "user-a", ClientID: "client-a"}, bounded)
	if err != nil || facts.ProvenanceCount == nil || *facts.ProvenanceCount != 1000 || len(facts.Provenance) != 0 {
		t.Fatalf("aggregate provenance facts = %+v, %v", facts, err)
	}

	receiptCount := 2
	rebuildID := "00000000-0000-4000-8000-000000009001"
	multiPage := complete
	multiPage.RebuildReceiptCount = &receiptCount
	multiPage.RebuildReceipts = []rebuildReceiptRecord{{
		RebuildIDFingerprint: cursorFingerprint(rebuildID),
		PageCount:            2,
		ReturnedRecordCount:  3,
		RequestChainExpected: []string{"final"},
		RequestChainObserved: []string{"final"},
		RecordIdentitiesHex:  []string{"a", "b", "c"},
		ReceivedRowChecksums: []string{"a", "b", "c"},
		ComputedRowChecksums: []string{"a", "b", "c"},
	}}
	if err := validateCaptureResult(multiPage); err != nil {
		t.Fatalf("grouped multi-page rebuild proof was rejected: %v", err)
	}
	facts, err = clientFactsForSource("rebuild-state", Client{UserID: "user-a", ClientID: "client-a"}, multiPage)
	if err != nil || facts.RebuildAttemptCount == nil || *facts.RebuildAttemptCount != 1 {
		t.Fatalf("completed rebuild facts = %+v, %v", facts, err)
	}

	activeCount := 1
	active := multiPage
	active.RebuildAttemptCount = &activeCount
	active.RebuildAttempts = []rebuildAttemptRecord{{RebuildID: rebuildID}}
	facts, err = clientFactsForSource("rebuild-state", Client{UserID: "user-a", ClientID: "client-a"}, active)
	if err != nil || facts.RebuildAttemptCount == nil || *facts.RebuildAttemptCount != 1 {
		t.Fatalf("active rebuild and its receipts were counted twice: %+v, %v", facts, err)
	}

	mismatchedPages := multiPage
	mismatchedPages.RebuildReceipts = append([]rebuildReceiptRecord(nil), multiPage.RebuildReceipts...)
	mismatchedPages.RebuildReceipts[0].PageCount = 1
	if err := validateCaptureResult(mismatchedPages); err == nil {
		t.Fatal("rebuild receipt page-count mismatch was accepted")
	}

	duplicatedReceipt := multiPage.RebuildReceipts[0]
	duplicatedReceipt.PageCount = 1
	duplicatedReceipts := multiPage
	duplicatedReceipts.RebuildReceipts = []rebuildReceiptRecord{duplicatedReceipt, duplicatedReceipt}
	if err := validateCaptureResult(duplicatedReceipts); err == nil {
		t.Fatal("duplicated rebuild receipt was accepted")
	}
}

func TestOperationWindowsUseMonotonicProvenanceMaintenanceCursorDelta(t *testing.T) {
	beforeCursor := int64(7)
	afterCursor := int64(9)
	before := runnerResult{
		ProvenanceMaintenanceWorkCursor: &beforeCursor,
		ScopeRows:                       []scopeRowRecord{{ScopeID: "scope-a", TableName: "items", RecordID: "shared", Checksum: "old"}},
	}
	after := runnerResult{
		ProvenanceMaintenanceWorkCursor: &afterCursor,
		ScopeRows:                       []scopeRowRecord{{ScopeID: "scope-a", TableName: "items", RecordID: "shared", Checksum: "updated"}},
	}
	work, err := provenanceMaintenanceDelta(before, after)
	if err != nil {
		t.Fatalf("provenance maintenance delta: %v", err)
	}
	if work != 2 {
		t.Fatalf("provenance maintenance work = %d, want 2", work)
	}
	if _, err := provenanceMaintenanceDelta(after, before); err == nil {
		t.Fatal("backward provenance maintenance cursor was accepted")
	}
}

func TestExecutedHTTPFailuresKeepSuccessfulDisposition(t *testing.T) {
	code := "temporary_unavailable"
	for _, observation := range []transportObservation{
		validPushObservation(0, nil, true),
		validPushObservation(503, &code, true),
	} {
		mapped, err := transportStepObservation(observation)
		if err != nil {
			t.Fatalf("map executed HTTP request: %v", err)
		}
		if mapped.Disposition != "success" || mapped.ErrorCode != nil || mapped.Wire == nil || mapped.Wire.HTTPStatus != observation.StatusCode {
			t.Fatalf("mapped request = %#v", mapped)
		}
	}
}

func TestCaptureRejectsEmptyClientSet(t *testing.T) {
	platform := &Platform{clients: make(map[string]*platformClient)}
	if _, err := platform.Capture(context.Background(), nil, []string{"provenance"}); err == nil {
		t.Fatal("empty capture client set was accepted")
	}
}

func TestGroupedRequestsMatchTransportObservationsExactly(t *testing.T) {
	operations := RequestOperations{
		{ContractOperation: "connect", Name: "send"},
		{ContractOperation: "push", Name: "submit", Payload: pushDispatchPayload("apply")},
	}
	observations := []transportObservation{validConnectObservation(), validPushObservation(200, nil, false)}
	mapped, err := mapTransportOperations(operations, observations, runnerResult{})
	if err != nil {
		t.Fatalf("map grouped requests: %v", err)
	}
	if len(mapped) != 2 || mapped[0].Wire.HTTPStatus != 200 || mapped[1].Wire.HTTPStatus != 200 {
		t.Fatalf("grouped mapping = %#v", mapped)
	}
	if _, err := mapTransportOperations(operations, []transportObservation{observations[1], observations[0]}, runnerResult{}); err == nil {
		t.Fatal("out-of-order grouped observations were accepted")
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
	pullObservation := transportObservation{
		OperationClass:             "pull",
		CursorFingerprints:         []string{cursorFingerprint(checkpoint)},
		CursorFingerprintsComplete: &complete,
	}
	source := runnerResult{ScopeStates: []scopeStateRecord{{ScopeID: "user:user-a", Cursor: &checkpoint}}}
	if err := validateCursorSourceBinding(pull, pullObservation, source, nil); err != nil {
		t.Fatalf("bind pull checkpoint cursor: %v", err)
	}
	pullObservation.CursorFingerprints[0] = cursorFingerprint("different")
	if err := validateCursorSourceBinding(pull, pullObservation, source, nil); err == nil {
		t.Fatal("mismatched pull checkpoint cursor passed")
	}

	continuation := "continuation-a"
	rebuildID := "00000000-0000-4000-8000-000000000001"
	runtimeRebuildID := "00000000-0000-4000-8000-000000000002"
	rebuild := scenarios.Operation{
		ContractOperation: "rebuild",
		Name:              "request-page",
		Payload:           json.RawMessage(`{"rebuild_id":"` + rebuildID + `","cursor_source":"local_rebuild_continuation"}`),
	}
	present := true
	rebuildObservation := transportObservation{RequestFacts: &transportRequestFacts{CursorPresent: &present, CursorFingerprint: pointerString(cursorFingerprint(continuation)), RebuildIDFingerprint: pointerString(cursorFingerprint(runtimeRebuildID))}}
	source = runnerResult{RebuildAttempts: []rebuildAttemptRecord{{RebuildID: runtimeRebuildID, Cursor: &continuation}}}
	if err := validateCursorSourceBinding(rebuild, rebuildObservation, source, nil); err != nil {
		t.Fatalf("bind rebuild continuation cursor: %v", err)
	}
	rebuildObservation.RequestFacts.CursorFingerprint = pointerString(cursorFingerprint("different"))
	if err := validateCursorSourceBinding(rebuild, rebuildObservation, source, nil); err == nil {
		t.Fatal("mismatched rebuild continuation cursor passed")
	}
}

func TestGroupedPullBindsToPrecedingTerminalRebuildCursor(t *testing.T) {
	scopeCursor := "rebuilt-checkpoint"
	complete := true
	generation := int64(1)
	scopeSetVersion := int64(1)
	scopeCount := 1
	limit := 100
	operations := RequestOperations{
		{
			ContractOperation: "rebuild",
			Name:              "request-page",
			Payload:           json.RawMessage(`{"scope_id":"scope-a","rebuild_id":"00000000-0000-4000-8000-000000000001","cursor_source":"none"}`),
		},
		{
			ContractOperation: "pull",
			Name:              "request-page",
			Payload:           json.RawMessage(`{"scopes":[{"scope_id":"scope-a","cursor_source":"local_checkpoint"}]}`),
		},
	}
	observations := []transportObservation{
		validTerminalRebuildObservation(scopeCursor),
		{
			Sequence:                   2,
			OperationClass:             "pull",
			StatusCode:                 200,
			DurationNanoseconds:        1,
			CursorFingerprints:         []string{cursorFingerprint(scopeCursor)},
			CursorFingerprintsComplete: &complete,
			RequestFacts: &transportRequestFacts{
				ClientGeneration: &generation,
				SchemaVersion:    1,
				SchemaHash:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				ScopeSetVersion:  &scopeSetVersion,
				ScopeCount:       &scopeCount,
				Limit:            &limit,
			},
			PullResponseFacts: &transportPullResponseFacts{
				ChecksumCount:                   1,
				ScopeCursorFingerprints:         []string{cursorFingerprint("next")},
				ScopeCursorFingerprintsComplete: true,
			},
		},
	}
	if _, err := mapTransportOperations(operations, observations, runnerResult{}); err != nil {
		t.Fatalf("bind grouped rebuild checkpoint: %v", err)
	}

	observations[1].CursorFingerprints[0] = cursorFingerprint("different")
	if _, err := mapTransportOperations(operations, observations, runnerResult{}); err == nil {
		t.Fatal("pull cursor unrelated to preceding rebuild passed")
	}
}

func TestForgedCursorBindsToDeterministicTransportOverride(t *testing.T) {
	present := true
	operation := scenarios.Operation{
		ContractOperation: "rebuild",
		Name:              "request-page",
		Payload:           json.RawMessage(`{"rebuild_id":"00000000-0000-4000-8000-000000000001","cursor_source":"forged"}`),
	}
	observation := transportObservation{RequestFacts: &transportRequestFacts{CursorPresent: &present, CursorFingerprint: pointerString(cursorFingerprint(forgedRebuildCursor))}}
	if err := validateCursorSourceBinding(operation, observation, runnerResult{}, nil); err != nil {
		t.Fatalf("bind deterministic forged rebuild cursor: %v", err)
	}
	observation.RequestFacts.CursorFingerprint = pointerString(cursorFingerprint("different"))
	if err := validateCursorSourceBinding(operation, observation, runnerResult{}, nil); err == nil {
		t.Fatal("mismatched forged rebuild cursor passed")
	}
}

func TestProvenanceCapturePreservesExactFacts(t *testing.T) {
	facts, err := provenanceFacts(
		[]scopeRowRecord{
			{ScopeID: "scope-b", TableName: "items", RecordID: "row-a"},
			{ScopeID: "scope-a", TableName: "items", RecordID: "row-a"},
		},
		[]rowMetadataRecord{{TableName: "items", RecordID: "row-a", ServerVersion: "version-a"}},
	)
	if err != nil {
		t.Fatalf("capture provenance facts: %v", err)
	}
	if len(facts) != 1 || facts[0].TableID != "items" || facts[0].CanonicalWireJSON != `"row-a"` || facts[0].Version != "version-a" || len(facts[0].Scopes) != 2 || facts[0].Scopes[0] != "scope-a" || facts[0].Scopes[1] != "scope-b" {
		t.Fatalf("provenance facts = %#v", facts)
	}
}

func TestRecoveredPushCountsObservedMutations(t *testing.T) {
	one := 1
	three := 3
	observations := []transportObservation{
		{OperationClass: "connect"},
		{OperationClass: "push", RequestFacts: &transportRequestFacts{MutationCount: &one}},
		{OperationClass: "pull"},
		{OperationClass: "push", RequestFacts: &transportRequestFacts{MutationCount: &three}},
	}
	if count := pushMutationCount(observations); count != 4 {
		t.Fatalf("replayed mutation count = %d, want 4", count)
	}
}

func pushDispatchPayload(delivery string) json.RawMessage {
	return json.RawMessage(`{
		"authenticated_user_id":"user-a",
		"request":{
			"client_id":"client-a",
			"client_generation":1,
			"batch_id":"batch-a",
			"schema":{"version":1,"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			"mutations":[{}]
		},
		"delivery":"` + delivery + `",
		"commit_lsn":"1",
		"end_lsn":"2"
	}`)
}

func validConnectObservation() transportObservation {
	generation := int64(1)
	protocolVersion := 3
	scopeSetVersion := int64(1)
	scopeCount := 1
	return transportObservation{
		Sequence:            1,
		OperationClass:      "connect",
		StatusCode:          200,
		DurationNanoseconds: 1,
		RequestFacts: &transportRequestFacts{
			ClientGeneration: &generation,
			SchemaVersion:    1,
			SchemaHash:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			ProtocolVersion:  &protocolVersion,
			ScopeSetVersion:  &scopeSetVersion,
			ScopeCount:       &scopeCount,
		},
	}
}

func validPushObservation(status int, code *string, retryable bool) transportObservation {
	generation := int64(1)
	mutationCount := 1
	return transportObservation{
		Sequence:            2,
		OperationClass:      "push",
		StatusCode:          status,
		ErrorCode:           code,
		Retryable:           retryable,
		DurationNanoseconds: 1,
		RequestFacts: &transportRequestFacts{
			ClientGeneration: &generation,
			SchemaVersion:    1,
			SchemaHash:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			MutationCount:    &mutationCount,
		},
	}
}

func validTerminalRebuildObservation(scopeCursor string) transportObservation {
	generation := int64(1)
	limit := 100
	present := false
	fingerprint := cursorFingerprint(scopeCursor)
	scopeFingerprint := cursorFingerprint("scope-a")
	return transportObservation{
		Sequence:            1,
		OperationClass:      "rebuild",
		StatusCode:          200,
		DurationNanoseconds: 1,
		RequestFacts: &transportRequestFacts{
			ClientGeneration:     &generation,
			SchemaVersion:        1,
			SchemaHash:           "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Limit:                &limit,
			ScopeFingerprint:     &scopeFingerprint,
			RebuildIDFingerprint: pointerString("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
			CursorPresent:        &present,
		},
		RebuildResponseFacts: &transportRebuildResponseFacts{
			HasFinalScopeCursor:         true,
			HasChecksum:                 true,
			ScopeFingerprint:            scopeFingerprint,
			FinalScopeCursorFingerprint: &fingerprint,
		},
	}
}
