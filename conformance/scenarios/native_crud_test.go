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
				evidence.Responses[1].Transport[0].RetryablePresent = true
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

func TestQueueSuccessorControlRejectsIdentityAndContentMutants(t *testing.T) {
	if err := ValidateNativeQueueSuccessorEvidence(validNativeQueueSuccessorEvidence()); err != nil {
		t.Fatalf("validate native queue successor evidence: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*NativeQueueSuccessorEvidence)
	}{
		{
			name: "changed intent reuses original identity",
			mutate: func(evidence *NativeQueueSuccessorEvidence) {
				evidence.Rows[0].Successor.MutationID = evidence.Rows[0].BeforeRestart.MutationID
			},
		},
		{
			name: "restart removes authored content",
			mutate: func(evidence *NativeQueueSuccessorEvidence) {
				evidence.Rows[0].AfterRestart.AuthoredFields = nil
			},
		},
		{
			name: "successor removes original content",
			mutate: func(evidence *NativeQueueSuccessorEvidence) {
				evidence.Rows[0].OriginalAfterChange.AuthoredFields = nil
			},
		},
		{
			name: "successor loses dependency",
			mutate: func(evidence *NativeQueueSuccessorEvidence) {
				evidence.Rows[0].Successor.DependsOnMutationID = nil
			},
		},
		{
			name: "linked successor keeps the original value",
			mutate: func(evidence *NativeQueueSuccessorEvidence) {
				evidence.Rows[0].Successor.AuthoredFields = append([]NativeQueuedField(nil), evidence.Rows[0].BeforeRestart.AuthoredFields...)
			},
		},
		{
			name: "successor writes the changed value to another field",
			mutate: func(evidence *NativeQueueSuccessorEvidence) {
				evidence.Rows[0].Successor.AuthoredFields = []NativeQueuedField{{FieldID: "other", LogicalType: "string", Value: json.RawMessage(`"updated"`)}}
			},
		},
		{
			name: "original lacks the authored initial value",
			mutate: func(evidence *NativeQueueSuccessorEvidence) {
				evidence.Rows[0].Target.InitialValue = json.RawMessage(`"not-authored"`)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			evidence := validNativeQueueSuccessorEvidence()
			test.mutate(&evidence)
			if err := ValidateNativeQueueSuccessorEvidence(evidence); err == nil {
				t.Fatal("queue successor mutant passed")
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

func validNativeQueueSuccessorEvidence() NativeQueueSuccessorEvidence {
	original := NativeQueuedMutation{
		MutationID: "original", LocalOrder: 1, TableID: "items", TableName: "runtime_items", RecordID: "row-a",
		PrimaryKeyFieldID: "id", PrimaryKeyLogicalType: "string", Operation: "insert",
		AuthoredSchemaVersion: 1, AuthoredSchemaHash: "schema-a", ClientVersion: "2026-08-11T00:30:01.000000Z",
		Status: "pending", SourceKind: "application",
		AuthoredFields: []NativeQueuedField{{FieldID: "value", LogicalType: "string", Value: json.RawMessage(`"initial"`)}},
	}
	afterRestart := original
	afterRestart.AuthoredFields = append([]NativeQueuedField(nil), original.AuthoredFields...)
	afterChange := original
	normalized := "normalized"
	afterChange.Status = "superseded_before_send"
	afterChange.NormalizedMutationID = &normalized
	dependency := original.MutationID
	successor := NativeQueuedMutation{
		MutationID: "successor", LocalOrder: 2, TableID: "items", TableName: "runtime_items", RecordID: "row-a",
		PrimaryKeyFieldID: "id", PrimaryKeyLogicalType: "string", Operation: "update",
		AuthoredSchemaVersion: 1, AuthoredSchemaHash: "schema-a", ClientVersion: "2026-08-11T00:30:02.000000Z",
		Status: "superseded_before_send", SourceKind: "application", DependsOnMutationID: &dependency, NormalizedMutationID: &normalized,
		AuthoredFields: []NativeQueuedField{{FieldID: "value", LogicalType: "string", Value: json.RawMessage(`"updated"`)}},
	}
	target := NativeCRUDTarget{
		TableID: "items", TableName: "runtime_items", PrimaryKeyField: "id", RecordID: "row-a", ValueField: "value",
		InitialValue: json.RawMessage(`"initial"`), UpdatedValue: json.RawMessage(`"updated"`),
	}
	return NativeQueueSuccessorEvidence{Rows: []NativeQueueSuccessorRow{{
		Target: target, BeforeRestart: original, AfterRestart: afterRestart, OriginalAfterChange: afterChange, Successor: successor,
	}}}
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
		responses = append(responses, NativeCRUDResponse{Operation: operation, Completion: "idle", Transport: []NativeCRUDTransport{{OperationClass: "push", StatusCode: 200, MutationCount: 2, MutationCountPresent: true}}})
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

func TestRequireLocalWriteRowAcceptsBothColumnFormsAndRejectsInvalidEvidence(t *testing.T) {
	row := map[string]json.RawMessage{
		"id": json.RawMessage(`"row"`), "value": json.RawMessage(`"local"`), "note": json.RawMessage(`null`),
	}
	for name, columns := range map[string]string{
		"map":   `{"value":"local","note":null}`,
		"array": `[{"field_id":"value","value":"local"},{"field_id":"note","value":null}]`,
	} {
		t.Run(name, func(t *testing.T) {
			write := Operation{ContractOperation: "local", Name: "write", Payload: json.RawMessage(
				`{"pk":{"id":"row"},"columns":` + columns + `}`,
			)}
			for name, evidence := range map[string]struct {
				rows    []map[string]json.RawMessage
				wantErr bool
			}{
				"valid": {rows: []map[string]json.RawMessage{
					{"id": json.RawMessage(`"other"`), "value": json.RawMessage(`"other"`)}, row,
				}},
				"wrong value": {rows: []map[string]json.RawMessage{
					{"id": row["id"], "value": json.RawMessage(`"server"`), "note": row["note"]},
				}, wantErr: true},
				"missing column": {rows: []map[string]json.RawMessage{
					{"id": row["id"], "value": row["value"]},
				}, wantErr: true},
				"missing row": {rows: []map[string]json.RawMessage{
					{"id": json.RawMessage(`"other"`), "value": row["value"], "note": row["note"]},
				}, wantErr: true},
				"duplicate row": {rows: []map[string]json.RawMessage{row, row}, wantErr: true},
			} {
				t.Run(name, func(t *testing.T) {
					if err := RequireLocalWriteRow(write, evidence.rows); (err != nil) != evidence.wantErr {
						t.Fatalf("row check error = %v, want error = %t", err, evidence.wantErr)
					}
				})
			}
		})
	}
	for name, columns := range map[string]string{
		"absent":             "",
		"null":               `,"columns":null`,
		"empty map":          `,"columns":{}`,
		"empty array":        `,"columns":[]`,
		"scalar":             `,"columns":"local"`,
		"empty map field":    `,"columns":{"":"local"}`,
		"empty array field":  `,"columns":[{"field_id":"","value":"local"}]`,
		"missing field":      `,"columns":[{"value":"local"}]`,
		"nonstring field":    `,"columns":[{"field_id":1,"value":"local"}]`,
		"missing value":      `,"columns":[{"field_id":"value"}]`,
		"wrong value key":    `,"columns":[{"field_id":"value","other":"local"}]`,
		"malformed value":    `,"columns":{"value":}`,
		"scalar array entry": `,"columns":["local"]`,
		"null array entry":   `,"columns":[null]`,
		"extra array field":  `,"columns":[{"field_id":"value","value":"local","extra":1}]`,
		"duplicate array ID": `,"columns":[{"field_id":"value","value":"local"},{"field_id":"value","value":"local"}]`,
	} {
		t.Run(name, func(t *testing.T) {
			write := Operation{ContractOperation: "local", Name: "write", Payload: json.RawMessage(
				`{"pk":{"id":"row"}` + columns + `}`,
			)}
			if err := RequireLocalWriteRow(write, []map[string]json.RawMessage{row}); err == nil {
				t.Fatal("invalid columns passed the row check")
			}
		})
	}
}

func TestLocalWriteKeptBySchemaRequiresSomeButNotAllColumns(t *testing.T) {
	publish := Operation{ContractOperation: "model", Name: "publish-schema", Payload: json.RawMessage(
		`{"tables":[{"table_id":"other","fields":[{"field_id":"note"}]},{"table_id":"items","fields":[{"field_id":"id"},{"field_id":"kept"}]}]}`,
	)}
	write := func(columns string) Operation {
		return Operation{ContractOperation: "local", Name: "write", Payload: json.RawMessage(
			`{"table_id":"items","pk":{"field_id":"id","value":"row"},"columns":` + columns + `}`,
		)}
	}

	kept, err := LocalWriteKeptBySchema(write(`[{"field_id":"kept","value":"local"},{"field_id":"note","value":"removed"}]`), publish)
	if err != nil {
		t.Fatalf("keep declared column: %v", err)
	}
	var payload struct {
		Columns []struct {
			FieldID string `json:"field_id"`
			Value   string `json:"value"`
		} `json:"columns"`
	}
	if err := json.Unmarshal(kept.Payload, &payload); err != nil || len(payload.Columns) != 1 ||
		payload.Columns[0].FieldID != "kept" || payload.Columns[0].Value != "local" {
		t.Fatalf("kept write = %s, want only the declared column", kept.Payload)
	}
	// Another table that declares the removed field must not keep it.
	for name, columns := range map[string]string{
		"none": `[{"field_id":"note","value":"removed"}]`,
		"all":  `[{"field_id":"kept","value":"local"}]`,
	} {
		if _, err := LocalWriteKeptBySchema(write(columns), publish); err == nil {
			t.Fatalf("write that keeps %s of its columns was accepted", name)
		}
	}
}
