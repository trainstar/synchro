package scenarios

import (
	"encoding/json"
	"testing"
)

func TestNativeCRUDPlanCoversEveryRegisteredTable(t *testing.T) {
	plan, err := NewNativeCRUDPlan(testNativeCRUDSchema(), "stream-1", "user-a", "client-a-crud")
	if err != nil {
		t.Fatalf("create native CRUD plan: %v", err)
	}
	if len(plan.Targets()) != 2 {
		t.Fatalf("native CRUD plan targets = %d, want 2", len(plan.Targets()))
	}
	insert, err := plan.Step("insert", nil, 20)
	if err != nil {
		t.Fatalf("create native CRUD insert: %v", err)
	}
	if len(insert.LocalWrites) != 2 {
		t.Fatalf("native CRUD insert writes = %d, want 2", len(insert.LocalWrites))
	}
	var push struct {
		Request struct {
			Mutations []struct {
				Table       string  `json:"table"`
				Operation   string  `json:"op"`
				BaseVersion *string `json:"base_version"`
			} `json:"mutations"`
		} `json:"request"`
	}
	if err := json.Unmarshal(insert.ApplicationPush.Payload, &push); err != nil || len(push.Request.Mutations) != 2 {
		t.Fatalf("decode native CRUD insert push: %v", err)
	}
	for index, tableID := range []string{"documents", "items"} {
		mutation := push.Request.Mutations[index]
		if mutation.Table != tableID || mutation.Operation != "insert" || mutation.BaseVersion != nil {
			t.Fatalf("native CRUD insert mutation %d = %#v", index, mutation)
		}
	}
	versions := map[string]string{"documents": "document-version", "items": "item-version"}
	for _, operation := range []string{"update", "delete"} {
		step, err := plan.Step(operation, versions, map[string]uint64{"update": 22, "delete": 24}[operation])
		if err != nil {
			t.Fatalf("create native CRUD %s: %v", operation, err)
		}
		if len(step.LocalWrites) != 2 {
			t.Fatalf("native CRUD %s writes = %d, want 2", operation, len(step.LocalWrites))
		}
	}
}

func TestValidateNativeCRUDEvidenceRejectsResponseAndPersistenceMutants(t *testing.T) {
	if err := ValidateNativeCRUDEvidence(validNativeCRUDEvidence()); err != nil {
		t.Fatalf("validate native CRUD evidence: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*NativeCRUDEvidence)
	}{
		{
			name: "registered table omitted",
			mutate: func(evidence *NativeCRUDEvidence) {
				evidence.AfterUpdateResponse.Rows = evidence.AfterUpdateResponse.Rows[:1]
			},
		},
		{
			name: "local insert bypasses queue",
			mutate: func(evidence *NativeCRUDEvidence) {
				evidence.AfterInsertWrite.PendingChangeCount = 0
			},
		},
		{
			name: "terminal update outcome is lost",
			mutate: func(evidence *NativeCRUDEvidence) {
				evidence.AfterUpdateResponse.MutationOutcomeCount--
			},
		},
		{
			name: "accepted update reuses version",
			mutate: func(evidence *NativeCRUDEvidence) {
				evidence.AfterUpdateResponse.Rows[0].ServerVersion = evidence.AfterUpdateWrite.Rows[0].ServerVersion
			},
		},
		{
			name: "delete response restores row",
			mutate: func(evidence *NativeCRUDEvidence) {
				evidence.AfterDeleteResponse.Rows[0].Present = true
				evidence.AfterDeleteResponse.Rows[0].Value = json.RawMessage(`"restored"`)
			},
		},
		{
			name: "restart changes durable outcome",
			mutate: func(evidence *NativeCRUDEvidence) {
				evidence.AfterDeleteRestart.MutationOutcomeCount--
			},
		},
		{
			name: "restart reuses process",
			mutate: func(evidence *NativeCRUDEvidence) {
				evidence.AfterUpdateRestart.ProcessID = evidence.AfterUpdateResponse.ProcessID
			},
		},
		{
			name: "custom upload endpoint",
			mutate: func(evidence *NativeCRUDEvidence) {
				evidence.Responses[0].Transport[0].OperationClass = "upload"
			},
		},
		{
			name: "push response becomes retryable",
			mutate: func(evidence *NativeCRUDEvidence) {
				evidence.Responses[1].Transport[0].Retryable = true
			},
		},
		{
			name: "push omits one registered table",
			mutate: func(evidence *NativeCRUDEvidence) {
				evidence.Responses[2].Transport[0].MutationCount--
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			evidence := validNativeCRUDEvidence()
			test.mutate(&evidence)
			if err := ValidateNativeCRUDEvidence(evidence); err == nil {
				t.Fatal("native CRUD mutant passed")
			}
		})
	}
}

func TestBindNativeCRUDTargetPreservesRuntimeIdentityAndValues(t *testing.T) {
	plan, err := NewNativeCRUDPlan(testNativeCRUDSchema(), "stream-1", "user-a", "client-a-crud")
	if err != nil {
		t.Fatalf("create native CRUD plan: %v", err)
	}
	target := plan.Targets()[0]
	insert := Operation{ContractOperation: "local", Name: "write", Payload: json.RawMessage(`{"authenticated_user_id":"user-a","client_id":"client-a-crud","mutation_id":"insert","table_id":"runtime_documents","pk":{"runtime_id":"runtime-row"},"authored_schema":{"version":1,"hash":"schema-a"},"operation":"insert","client_version":"2026-08-11T00:30:01.000000Z","columns":{"runtime_value":"native-crud-documents-value-insert"}}`)}
	update := Operation{ContractOperation: "local", Name: "write", Payload: json.RawMessage(`{"authenticated_user_id":"user-a","client_id":"client-a-crud","mutation_id":"update","table_id":"runtime_documents","pk":{"runtime_id":"runtime-row"},"authored_schema":{"version":1,"hash":"schema-a"},"operation":"update","base_version":"server-version","client_version":"2026-08-11T00:30:02.000000Z","columns":{"runtime_value":"native-crud-documents-value-update"}}`)}
	bound, err := BindNativeCRUDTarget(target, insert, update)
	if err != nil {
		t.Fatalf("bind native CRUD target: %v", err)
	}
	if bound.TableID != "documents" || bound.TableName != "runtime_documents" || bound.PrimaryKeyField != "runtime_id" || bound.RecordID != "runtime-row" || bound.ValueField != "runtime_value" {
		t.Fatalf("bound native CRUD target = %#v", bound)
	}
}

func testNativeCRUDSchema() NativeCRUDSchema {
	return NativeCRUDSchema{
		Version: 1,
		Hash:    "schema-a",
		Tables: []NativeCRUDSchemaTable{
			{TableID: "items", PrimaryKeyFieldID: "id", Fields: []NativeCRUDSchemaField{{FieldID: "id", Type: "string", PrimaryKey: true}, {FieldID: "value", Type: "string", Writable: true}}},
			{TableID: "documents", PrimaryKeyFieldID: "id", Fields: []NativeCRUDSchemaField{{FieldID: "id", Type: "string", PrimaryKey: true}, {FieldID: "value", Type: "string", Writable: true}}},
		},
	}
}

func validNativeCRUDEvidence() NativeCRUDEvidence {
	targets := []NativeCRUDTarget{
		{TableID: "documents", TableName: "runtime_documents", PrimaryKeyField: "id", RecordID: "document-row", ValueField: "value", InitialValue: json.RawMessage(`"document-insert"`), UpdatedValue: json.RawMessage(`"document-update"`)},
		{TableID: "items", TableName: "runtime_items", PrimaryKeyField: "id", RecordID: "item-row", ValueField: "value", InitialValue: json.RawMessage(`"item-insert"`), UpdatedValue: json.RawMessage(`"item-update"`)},
	}
	before := NativeCRUDState{
		ProcessID: "process-1", DatabaseIdentityFingerprint: "database-a", Rows: []NativeCRUDRowState{{TableID: "documents"}, {TableID: "items"}},
	}
	insertWrite := before
	insertWrite.ApplicationRowCount = 2
	insertWrite.PendingChangeCount = 2
	insertWrite.MutationLedgerCount = 2
	insertWrite.Rows = []NativeCRUDRowState{
		{TableID: "documents", Present: true, Value: json.RawMessage(`"document-insert"`), Mutation: &NativeCRUDMutation{Operation: "insert", Status: "pending", ClientVersion: "insert-client"}},
		{TableID: "items", Present: true, Value: json.RawMessage(`"item-insert"`), Mutation: &NativeCRUDMutation{Operation: "insert", Status: "pending", ClientVersion: "insert-client"}},
	}
	insertResponse := insertWrite
	insertResponse.PendingChangeCount = 0
	insertResponse.MutationOutcomeCount = 2
	insertResponse.RowMetadataCount = 2
	insertResponse.Rows = []NativeCRUDRowState{
		{TableID: "documents", Present: true, Value: json.RawMessage(`"document-insert"`), ServerVersion: "document-insert-version", RowChecksum: "document-insert-checksum"},
		{TableID: "items", Present: true, Value: json.RawMessage(`"item-insert"`), ServerVersion: "item-insert-version", RowChecksum: "item-insert-checksum"},
	}
	insertRestart := cloneNativeCRUDState(insertResponse, "process-2")
	updateWrite := cloneNativeCRUDState(insertRestart, "process-2")
	updateWrite.PendingChangeCount = 2
	updateWrite.MutationLedgerCount = 4
	updateWrite.Rows = []NativeCRUDRowState{
		{TableID: "documents", Present: true, Value: json.RawMessage(`"document-update"`), Mutation: &NativeCRUDMutation{Operation: "update", Status: "pending", ClientVersion: "update-client"}, ServerVersion: "document-insert-version", RowChecksum: "document-insert-checksum"},
		{TableID: "items", Present: true, Value: json.RawMessage(`"item-update"`), Mutation: &NativeCRUDMutation{Operation: "update", Status: "pending", ClientVersion: "update-client"}, ServerVersion: "item-insert-version", RowChecksum: "item-insert-checksum"},
	}
	updateResponse := updateWrite
	updateResponse.PendingChangeCount = 0
	updateResponse.MutationOutcomeCount = 4
	updateResponse.Rows = []NativeCRUDRowState{
		{TableID: "documents", Present: true, Value: json.RawMessage(`"document-update"`), ServerVersion: "document-update-version", RowChecksum: "document-update-checksum"},
		{TableID: "items", Present: true, Value: json.RawMessage(`"item-update"`), ServerVersion: "item-update-version", RowChecksum: "item-update-checksum"},
	}
	updateRestart := cloneNativeCRUDState(updateResponse, "process-3")
	deleteWrite := cloneNativeCRUDState(updateRestart, "process-3")
	deleteWrite.ApplicationRowCount = 0
	deleteWrite.PendingChangeCount = 2
	deleteWrite.MutationLedgerCount = 6
	deleteWrite.Rows = []NativeCRUDRowState{
		{TableID: "documents", Mutation: &NativeCRUDMutation{Operation: "delete", Status: "pending", ClientVersion: "delete-client"}, ServerVersion: "document-update-version", RowChecksum: "document-update-checksum"},
		{TableID: "items", Mutation: &NativeCRUDMutation{Operation: "delete", Status: "pending", ClientVersion: "delete-client"}, ServerVersion: "item-update-version", RowChecksum: "item-update-checksum"},
	}
	deleteResponse := deleteWrite
	deleteResponse.PendingChangeCount = 0
	deleteResponse.MutationOutcomeCount = 6
	deleteResponse.Rows = []NativeCRUDRowState{
		{TableID: "documents", ServerVersion: "document-delete-version", RowChecksum: "document-delete-checksum"},
		{TableID: "items", ServerVersion: "item-delete-version", RowChecksum: "item-delete-checksum"},
	}
	deleteRestart := cloneNativeCRUDState(deleteResponse, "process-4")
	responses := make([]NativeCRUDResponse, 0, 3)
	for _, operation := range []string{"insert", "update", "delete"} {
		responses = append(responses, NativeCRUDResponse{Operation: operation, Completion: "idle", Transport: []NativeCRUDTransport{{OperationClass: "push", StatusCode: 200, RetryablePresent: true, MutationCount: 2, MutationCountPresent: true}}})
	}
	return NativeCRUDEvidence{
		Targets: targets, Before: before, AfterInsertWrite: insertWrite, AfterInsertResponse: insertResponse, AfterInsertRestart: insertRestart,
		AfterUpdateWrite: updateWrite, AfterUpdateResponse: updateResponse, AfterUpdateRestart: updateRestart,
		AfterDeleteWrite: deleteWrite, AfterDeleteResponse: deleteResponse, AfterDeleteRestart: deleteRestart, Responses: responses,
	}
}

func cloneNativeCRUDState(state NativeCRUDState, processID string) NativeCRUDState {
	state.ProcessID = processID
	state.Rows = append([]NativeCRUDRowState(nil), state.Rows...)
	return state
}
