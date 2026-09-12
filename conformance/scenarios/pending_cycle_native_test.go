package scenarios

import (
	"encoding/json"
	"testing"
)

func TestValidatePendingCycleNativeEvidenceRejectsSemanticMutants(t *testing.T) {
	if err := ValidatePendingCycleNativeEvidence(validPendingCycleNativeEvidence()); err != nil {
		t.Fatalf("validate pending-cycle native evidence: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*PendingCycleNativeEvidence)
	}{
		{
			name: "local write bypasses queue",
			mutate: func(evidence *PendingCycleNativeEvidence) {
				evidence.AfterWrite.TargetMutations = nil
			},
		},
		{
			name: "local operation changes",
			mutate: func(evidence *PendingCycleNativeEvidence) {
				evidence.AfterWrite.TargetMutations[0].Operation = "update"
			},
		},
		{
			name: "server apply echoes into queue",
			mutate: func(evidence *PendingCycleNativeEvidence) {
				evidence.AfterPull.PendingChangeCount = 1
			},
		},
		{
			name: "server version copies client version",
			mutate: func(evidence *PendingCycleNativeEvidence) {
				evidence.AfterPush.TargetServerVersion = evidence.AfterWrite.TargetMutations[0].ClientVersion
				evidence.BeforePull.TargetServerVersion = evidence.AfterWrite.TargetMutations[0].ClientVersion
			},
		},
		{
			name: "cursor advances without row checksum",
			mutate: func(evidence *PendingCycleNativeEvidence) {
				evidence.AfterPull.TargetScopeRowChecksum = "different-row-checksum"
			},
		},
		{
			name: "cursor does not advance",
			mutate: func(evidence *PendingCycleNativeEvidence) {
				evidence.AfterPull.ScopeCursor = evidence.BeforePull.ScopeCursor
			},
		},
		{
			name: "restart reuses process",
			mutate: func(evidence *PendingCycleNativeEvidence) {
				evidence.AfterRestart.ProcessID = evidence.AfterPull.ProcessID
			},
		},
		{
			name: "scope cleanup drops pending intent",
			mutate: func(evidence *PendingCycleNativeEvidence) {
				evidence.AfterCleanup.PendingChangeCount = 0
				evidence.AfterCleanup.TargetMutations = nil
			},
		},
		{
			name: "scope cleanup unseals retryable intent",
			mutate: func(evidence *PendingCycleNativeEvidence) {
				evidence.AfterCleanup.TargetMutations[0].Status = "pending"
			},
		},
		{
			name: "scope cleanup deletes protected row",
			mutate: func(evidence *PendingCycleNativeEvidence) {
				evidence.AfterCleanup.ApplicationRowCount = 0
				evidence.AfterCleanup.TargetRowPresent = false
				evidence.AfterCleanup.TargetRowValue = ""
			},
		},
		{
			name: "scope cleanup retains unprotected row",
			mutate: func(evidence *PendingCycleNativeEvidence) {
				evidence.AfterCleanup.ApplicationRowCount = 2
				evidence.AfterCleanup.UnprotectedRowPresent = true
				evidence.AfterCleanup.UnprotectedRowValue = evidence.Target.UnprotectedValue
			},
		},
		{
			name: "scope cleanup retains unprotected provenance",
			mutate: func(evidence *PendingCycleNativeEvidence) {
				evidence.AfterCleanup.ScopeRowCount = 1
				evidence.AfterCleanup.UnprotectedScopeRowPresent = true
				evidence.AfterCleanup.UnprotectedScopeRowChecksum = "unprotected-checksum"
			},
		},
		{
			name: "scope cleanup creates synthetic mutation",
			mutate: func(evidence *PendingCycleNativeEvidence) {
				evidence.AfterCleanup.MutationLedgerCount++
			},
		},
		{
			name: "scope cleanup rewrites metadata",
			mutate: func(evidence *PendingCycleNativeEvidence) {
				evidence.AfterCleanup.TargetServerVersion = "rewritten-version"
			},
		},
		{
			name: "scope cleanup invents a receipt",
			mutate: func(evidence *PendingCycleNativeEvidence) {
				evidence.AfterCleanup.ScopeCursor = "invented-cursor"
			},
		},
		{
			name: "update remains pending",
			mutate: func(evidence *PendingCycleNativeEvidence) {
				evidence.AfterUpdate.PendingChangeCount = 1
			},
		},
		{
			name: "update skips the assigned scope rebuild",
			mutate: func(evidence *PendingCycleNativeEvidence) {
				evidence.AfterUpdate.ScopeCursor = ""
				evidence.AfterUpdate.ScopeChecksum = ""
				evidence.AfterUpdate.LocalScopeChecksum = ""
			},
		},
		{
			name: "delete remains pending",
			mutate: func(evidence *PendingCycleNativeEvidence) {
				evidence.AfterDelete.PendingChangeCount = 1
			},
		},
		{
			name: "delete drops the source tombstone checksum",
			mutate: func(evidence *PendingCycleNativeEvidence) {
				evidence.AfterDelete.TargetRowChecksum = ""
			},
		},
		{
			name: "delete cursor does not advance",
			mutate: func(evidence *PendingCycleNativeEvidence) {
				evidence.AfterDelete.ScopeCursor = evidence.BeforeDelete.ScopeCursor
			},
		},
		{
			name: "delete changes the empty scope checksum",
			mutate: func(evidence *PendingCycleNativeEvidence) {
				evidence.AfterDelete.ScopeChecksum = "changed-empty-scope-checksum"
				evidence.AfterDelete.LocalScopeChecksum = "changed-empty-scope-checksum"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			evidence := validPendingCycleNativeEvidence()
			test.mutate(&evidence)
			if err := ValidatePendingCycleNativeEvidence(evidence); err == nil {
				t.Fatal("pending-cycle semantic mutant passed")
			}
		})
	}
}

func validPendingCycleNativeEvidence() PendingCycleNativeEvidence {
	before := PendingCycleNativeState{
		ProcessID:                   "101",
		DatabaseIdentityFingerprint: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ScopeStateCount:             1,
		ScopeID:                     "user:user-a",
		ScopeCursor:                 "cursor-before",
		ScopeChecksum:               "empty-scope-checksum",
		LocalScopeChecksum:          "empty-scope-checksum",
	}
	written := before
	written.ApplicationRowCount = 1
	written.PendingChangeCount = 1
	written.MutationLedgerCount = 1
	written.TargetRowPresent = true
	written.TargetRowValue = "pending"
	written.TargetMutations = []PendingCycleNativeMutation{{Operation: "insert", Status: "pending", ClientVersion: "client-version"}}
	pushed := written
	pushed.PendingChangeCount = 0
	pushed.MutationOutcomeCount = 1
	pushed.RowMetadataCount = 1
	pushed.TargetMutations = nil
	pushed.TargetServerVersion = "server-version"
	pushed.TargetRowChecksum = "row-checksum"
	pulled := pushed
	pulled.ScopeRowCount = 1
	pulled.ScopeCursor = "cursor-after"
	pulled.ScopeChecksum = "scope-checksum"
	pulled.LocalScopeChecksum = "scope-checksum"
	pulled.TargetScopeRowPresent = true
	pulled.TargetScopeRowChecksum = "row-checksum"
	pulled.ApplicationRowCount = 2
	pulled.RowMetadataCount = 2
	pulled.ScopeRowCount = 2
	pulled.UnprotectedRowPresent = true
	pulled.UnprotectedRowValue = "unprotected"
	pulled.UnprotectedScopeRowPresent = true
	pulled.UnprotectedScopeRowChecksum = "unprotected-checksum"
	restarted := pulled
	restarted.ProcessID = "202"
	pendingUpdate := restarted
	pendingUpdate.PendingChangeCount = 1
	pendingUpdate.MutationLedgerCount = 2
	pendingUpdate.TargetRowValue = "pending-updated"
	pendingUpdate.TargetMutations = []PendingCycleNativeMutation{{Operation: "update", Status: "pending", ClientVersion: "update-client-version"}}
	cleaned := pendingUpdate
	cleaned.TargetMutations = append([]PendingCycleNativeMutation(nil), pendingUpdate.TargetMutations...)
	cleaned.ApplicationRowCount = 1
	cleaned.RowMetadataCount = 1
	cleaned.ScopeRowCount = 0
	cleaned.ScopeID = "cf:global"
	cleaned.ScopeCursor = ""
	cleaned.ScopeChecksum = ""
	cleaned.LocalScopeChecksum = ""
	cleaned.TargetScopeRowPresent = false
	cleaned.TargetScopeRowChecksum = ""
	cleaned.UnprotectedRowPresent = false
	cleaned.UnprotectedRowValue = ""
	cleaned.UnprotectedScopeRowPresent = false
	cleaned.UnprotectedScopeRowChecksum = ""
	cleaned.TargetMutations[0].Status = "sealed"
	updated := cleaned
	updated.PendingChangeCount = 0
	updated.MutationOutcomeCount = 2
	updated.TargetMutations = nil
	updated.TargetServerVersion = "update-server-version"
	updated.TargetRowChecksum = "update-row-checksum"
	updated.ScopeCursor = "rebuilt-cursor"
	updated.ScopeChecksum = "empty-scope-checksum"
	updated.LocalScopeChecksum = "empty-scope-checksum"
	pendingDelete := updated
	pendingDelete.ApplicationRowCount = 0
	pendingDelete.PendingChangeCount = 1
	pendingDelete.MutationLedgerCount = 3
	pendingDelete.TargetRowPresent = false
	pendingDelete.TargetRowValue = ""
	pendingDelete.TargetMutations = []PendingCycleNativeMutation{{Operation: "delete", Status: "pending", ClientVersion: "delete-client-version"}}
	deleted := pendingDelete
	deleted.PendingChangeCount = 0
	deleted.MutationOutcomeCount = 3
	deleted.TargetMutations = nil
	deleted.TargetServerVersion = "delete-server-version"
	deleted.TargetRowChecksum = "delete-row-checksum"
	deleted.ScopeCursor = "delete-pull-cursor"
	return PendingCycleNativeEvidence{
		Target: PendingCycleNativeTarget{
			TableName:                   "items",
			PrimaryKeyField:             "id",
			RecordID:                    "row-id",
			ValueField:                  "value",
			Value:                       "pending",
			UnprotectedAuthoredRecordID: "unprotected-row",
			UnprotectedRecordID:         "unprotected-runtime-row",
			UnprotectedValue:            "unprotected",
		},
		UpdatedValue:  "pending-updated",
		BeforeWrite:   before,
		AfterWrite:    written,
		AfterPush:     pushed,
		BeforePull:    pushed,
		AfterPull:     pulled,
		AfterRestart:  restarted,
		BeforeCleanup: pendingUpdate,
		AfterCleanup:  cleaned,
		AfterUpdate:   updated,
		BeforeDelete:  pendingDelete,
		AfterDelete:   deleted,
	}
}

func TestValidatePendingCycleServerFactsRequiresUnprotectedRow(t *testing.T) {
	count := uint64(1)
	target := validPendingCycleNativeEvidence().Target
	facts := StateFacts{
		RowCount: &count,
		Rows: []RowFact{{
			TableID:           "items",
			CanonicalWireJSON: `"unprotected-row"`,
			Version:           "unprotected-version",
			Checksum:          "unprotected-checksum",
		}},
	}
	if err := ValidatePendingCycleServerFacts(facts, target); err != nil {
		t.Fatalf("validate pending-cycle server facts: %v", err)
	}
	facts.Rows[0].CanonicalWireJSON = `"pending-row"`
	if err := ValidatePendingCycleServerFacts(facts, target); err == nil {
		t.Fatal("pending-cycle server facts accepted the protected row as the unprotected row")
	}
}

func TestPendingCycleUnprotectedRowTargetSelectsTheNonPrimaryField(t *testing.T) {
	operation := Operation{
		ContractOperation: "model",
		Name:              "commit-source-transaction",
		Payload:           json.RawMessage(`{"stream_generation":"stream-1","commit_lsn":"18","end_lsn":"19","events":[{"event_ordinal":1,"relation":"public.items","operation":"insert","before":null,"after":{"identity":{"kind":"synced","synced_row":{"canonical_identity_bytes":"identity","table_id":"items","primary_key_field_id":"id","portable_type":"string","canonical_wire_json":"\"row-a\""},"capture_key":null},"fields":[{"field":"id","type":"string","wire_json":"\"different-runtime-value\""},{"field":"value","type":"string","wire_json":"\"unprotected\""}],"version":"v1","checksum":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","deleted":false}}]}`),
	}
	aliases := []NativeIdentityAlias{{Alias: "unprotected-row-primary-key", Kind: "primary-key", Value: json.RawMessage(`"row-a"`)}}
	authoredID, value, err := PendingCycleUnprotectedRowTarget(operation, aliases, "runtime-row-a")
	if err != nil {
		t.Fatalf("resolve unprotected row target: %v", err)
	}
	if authoredID != "row-a" || value != "unprotected" {
		t.Fatalf("unprotected target = %q/%q, want row-a/unprotected", authoredID, value)
	}
}

func TestPendingCycleSynchronizedCRUDOperationsPreserveIdentity(t *testing.T) {
	insert := Operation{
		ContractOperation: "local",
		Name:              "write",
		Payload:           json.RawMessage(`{"authenticated_user_id":"user-a","client_id":"client-a","mutation_id":"insert-a","table_id":"runtime_items","pk":{"runtime_id":"row-a"},"authored_schema":{"version":1,"hash":"schema-a"},"operation":"insert","client_version":"client-version-a","columns":{"runtime_value":"pending","owner_id":"user-a"}}`),
	}
	push := Operation{
		ContractOperation: "push",
		Name:              "submit",
		Payload:           json.RawMessage(`{"authenticated_user_id":"user-a","request":{"client_id":"client-a","client_generation":1,"batch_id":"00000000-0000-4000-8000-000000004002","schema":{"version":1,"hash":"721d2c95e6f34cd9733feea9f5118fba391eee10d07663dad066cfc59439fa44"},"mutations":[{"mutation_id":"00000000-0000-4000-8000-000000004001","table":"items","pk":{"id":"pending-row"},"authored_schema":{"version":1,"hash":"721d2c95e6f34cd9733feea9f5118fba391eee10d07663dad066cfc59439fa44"},"op":"insert","client_version":"2026-08-11T00:00:00.000000Z","columns":{"value":"pending"}}]},"delivery":"apply","commit_lsn":"20","end_lsn":"21"}`),
	}
	materialize := Operation{ContractOperation: "process", Name: "materialize-source-transaction", Payload: json.RawMessage(`{"stream_generation":"stream-1","commit_lsn":"20"}`)}
	update, err := PendingCycleSynchronizedCRUDOperation(insert, push, materialize, "update", "runtime_value", "pending", "pending-updated", "insert-server-version")
	if err != nil {
		t.Fatalf("derive pending-cycle update operations: %v", err)
	}
	deleteStep, err := PendingCycleSynchronizedCRUDOperation(insert, push, materialize, "delete", "runtime_value", "", "", "update-server-version")
	if err != nil {
		t.Fatalf("derive pending-cycle delete operations: %v", err)
	}
	for name, operation := range map[string]Operation{"update": update.LocalWrite, "delete": deleteStep.LocalWrite} {
		var payload map[string]json.RawMessage
		if err := json.Unmarshal(operation.Payload, &payload); err != nil {
			t.Fatalf("decode %s operation: %v", name, err)
		}
		var operationName string
		if json.Unmarshal(payload["operation"], &operationName) != nil || operationName != name || string(payload["pk"]) != `{"runtime_id":"row-a"}` {
			t.Fatalf("%s operation changed runtime identity", name)
		}
	}
	for name, step := range map[string]PendingCycleNativeCRUDStep{"update": update, "delete": deleteStep} {
		var payload struct {
			Request struct {
				Mutations []struct {
					Op          string                     `json:"op"`
					PK          map[string]json.RawMessage `json:"pk"`
					BaseVersion string                     `json:"base_version"`
					Columns     map[string]json.RawMessage `json:"columns"`
				} `json:"mutations"`
			} `json:"request"`
			CommitLSN string `json:"commit_lsn"`
		}
		if err := json.Unmarshal(step.ApplicationPush.Payload, &payload); err != nil || len(payload.Request.Mutations) != 1 {
			t.Fatalf("decode %s push: %v", name, err)
		}
		mutation := payload.Request.Mutations[0]
		if mutation.Op != name || string(mutation.PK["id"]) != `"pending-row"` || mutation.BaseVersion == "" {
			t.Fatalf("%s push changed authored identity", name)
		}
		if name == "update" && string(mutation.Columns["value"]) != `"pending-updated"` {
			t.Fatal("update push did not carry the updated value")
		}
		if name == "delete" && mutation.Columns != nil {
			t.Fatal("delete push carried columns")
		}
		var process struct {
			CommitLSN string `json:"commit_lsn"`
		}
		if err := json.Unmarshal(step.Materialize.Payload, &process); err != nil || process.CommitLSN == "20" || process.CommitLSN != payload.CommitLSN {
			t.Fatalf("%s materialization did not bind its generated push", name)
		}
	}
	faulted, err := PendingCycleTemporaryUnavailablePush(update.ApplicationPush)
	if err != nil || faulted.WireFault == nil || faulted.WireFault.Mode != "temporary_unavailable" {
		t.Fatalf("derive pending-cycle temporary-unavailable push: %v", err)
	}
}
