package kotlin

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestAcceptedMutationOutcomesRejectInvalidAndUnboundedMaps(t *testing.T) {
	outcome := `{"mutation_id":"m1","status":"applied","outcome_schema":{"version":1,"hash":"` + strings.Repeat("a", 64) + `"},"server_version":"v1"}`
	encoded, _ := json.Marshal(map[string]string{"m1": outcome})
	var valid acceptedMutationOutcomes
	if err := json.Unmarshal(encoded, &valid); err != nil || valid["m1"] != outcome {
		t.Fatalf("stored bytes changed: %v", err)
	}
	for _, invalid := range []string{
		strings.Replace(outcome, `"applied"`, `"merged"`, 1),
		strings.Replace(outcome, `"applied"`, `"rejected"`, 1),
		strings.Replace(outcome, `,"server_version":"v1"`, ``, 1),
		strings.Replace(outcome, `"server_version":"v1"`, `"server_version":""`, 1),
		strings.Replace(outcome, `"server_version":"v1"`, `"server_version":null`, 1),
		strings.Replace(outcome, `"server_version":"v1"`, `"server_version":42`, 1),
	} {
		raw, _ := json.Marshal(map[string]string{"m1": invalid})
		var values acceptedMutationOutcomes
		if json.Unmarshal(raw, &values) == nil {
			t.Fatal("invalid accepted status or server version passed")
		}
	}
	large, _ := json.Marshal(map[string]string{"m1": outcome + strings.Repeat(" ", 65_536)})
	wrong, _ := json.Marshal(map[string]string{"m1": strings.Replace(outcome, `"m1"`, `"m2"`, 1)})
	for _, raw := range [][]byte{[]byte(`null`), []byte(`[]`), []byte(`{"m1":null}`), []byte(`{"m1":42}`), []byte(`{"m1":"{}","m1":"{}"}`), []byte(`{"m1":"not-json"}`), wrong, large} {
		var values acceptedMutationOutcomes
		if json.Unmarshal(raw, &values) == nil {
			t.Fatalf("invalid accepted outcome map passed: %.80s", raw)
		}
	}
	tooMany := make(map[string]string, maximumRecords+1)
	for index := 0; index <= maximumRecords; index++ {
		tooMany[strings.Repeat("x", index+1)] = outcome
	}
	raw, _ := json.Marshal(tooMany)
	var values acceptedMutationOutcomes
	if json.Unmarshal(raw, &values) == nil {
		t.Fatal("over-bound map passed")
	}
}

func TestSchemaProofSingleWritesPreserveIdentityAcrossSealing(t *testing.T) {
	localBase, acceptedBase, m1, batch := "local-base", "accepted-base", "actual-m1", "m2-batch"
	ordinal := int64(0)
	original := retainedMutation{MutationID: "original-m2", LocalOrder: 2, TableID: "items", TableName: "cf_items", RecordID: "row", PrimaryKeyFieldID: "id", PrimaryKeyLogicalType: "string", Operation: "update", ClientVersion: "2026-10-09T00:00:03.000000Z", SourceKind: "application", AuthoredSchema: schemaRef{Version: 2, Hash: strings.Repeat("b", 64)}, BaseVersion: &localBase, DependsOnMutationID: &m1, Status: "pending", AuthoredFields: []retainedField{{FieldID: "value", Value: []byte(`"42"`)}, {FieldID: "note", Value: []byte(`"later-note"`)}}}
	sealed := original
	sealed.BaseVersion = &acceptedBase
	sealed.DependsOnMutationID = nil
	sealed.Status = "sealed"
	sealed.SealedBatchID = &batch
	sealed.SealedOrdinal = &ordinal
	if err := requireSchemaProofSuccessorTransition(original, sealed, acceptedBase); err != nil {
		t.Fatal(err)
	}
	mutation := wireMutation{MutationID: "original-m2", Table: "items", Operation: "update", PrimaryKey: map[string]json.RawMessage{"id": []byte(`"row"`)}, AuthoredSchema: sealed.AuthoredSchema, BaseVersion: &acceptedBase, ClientVersion: "2026-10-09T00:00:03.000000Z"}
	for _, value := range []string{`"42"`, `"4\u0032"`} {
		mutation.Columns = map[string]json.RawMessage{"value": []byte(value), "note": []byte(`"later-note"`)}
		if err := requireSchemaProofMutation(sealed, mutation); err != nil {
			t.Fatalf("matching string fields rejected: %v", err)
		}
	}
	for _, test := range []struct{ name, value string }{
		{"changed", `"43"`},
		{"null", `null`},
		{"number", `42`},
		{"boolean", `true`},
		{"object", `{}`},
		{"array", `[]`},
		{"malformed", `"42`},
		{"trailing JSON", `"42" "42"`},
		{"absent", ``},
	} {
		t.Run("wire string "+test.name, func(t *testing.T) {
			mutation.Columns = map[string]json.RawMessage{"value": []byte(test.value), "note": []byte(`"later-note"`)}
			if requireSchemaProofMutation(sealed, mutation) == nil {
				t.Fatal("invalid wire field passed")
			}
		})
		if test.name == "changed" {
			continue
		}
		t.Run("authored string "+test.name, func(t *testing.T) {
			intent := sealed
			intent.AuthoredFields = []retainedField{{FieldID: "value", Value: []byte(test.value)}, sealed.AuthoredFields[1]}
			mutation.Columns = map[string]json.RawMessage{"value": []byte(`"42"`), "note": []byte(`"later-note"`)}
			if requireSchemaProofMutation(intent, mutation) == nil {
				t.Fatal("invalid authored field passed")
			}
		})
	}
	emptyIntent := sealed
	emptyIntent.AuthoredFields = []retainedField{{FieldID: "value", Value: []byte(`""`)}, sealed.AuthoredFields[1]}
	mutation.Columns = map[string]json.RawMessage{"value": []byte(`""`), "note": []byte(`"later-note"`)}
	if err := requireSchemaProofMutation(emptyIntent, mutation); err != nil {
		t.Fatal(err)
	}
	mutation.Columns["value"] = []byte(`null`)
	if requireSchemaProofMutation(emptyIntent, mutation) == nil {
		t.Fatal("null wire field matched an empty string")
	}
	emptyIntent.AuthoredFields[0].Value = []byte(`null`)
	mutation.Columns["value"] = []byte(`""`)
	if requireSchemaProofMutation(emptyIntent, mutation) == nil {
		t.Fatal("null authored field matched an empty string")
	}
	direct := original
	direct.DependsOnMutationID = nil
	directSealed := sealed
	directSealed.BaseVersion = &localBase
	if err := requireSchemaProofOriginal(direct, []retainedMutation{directSealed}); err != nil {
		t.Fatal(err)
	}
	if err := requireSchemaProofOriginal(directSealed, []retainedMutation{directSealed}); err != nil {
		t.Fatal(err)
	}
	if requireSchemaProofOriginal(direct, []retainedMutation{sealed}) == nil {
		t.Fatal("M1 base refreshed after sealing")
	}
	if requireSchemaProofOriginal(direct, nil) == nil {
		t.Fatal("missing original passed")
	}
	if requireSchemaProofOriginal(direct, []retainedMutation{directSealed, directSealed}) == nil {
		t.Fatal("duplicate original passed")
	}
	mutation.Columns = map[string]json.RawMessage{"value": []byte(`"42"`), "note": []byte(`"later-note"`)}
	mutation.MutationID = "other"
	if requireSchemaProofMutation(sealed, mutation) == nil {
		t.Fatal("push substituted the original identity")
	}
	for _, test := range []struct {
		name   string
		mutate func(*retainedMutation)
	}{
		{"successor source", func(v *retainedMutation) { v.SourceKind = "normalized" }},
		{"successor later order", func(v *retainedMutation) { v.LocalOrder = 3 }},
		{"successor earlier order", func(v *retainedMutation) { v.LocalOrder = 1 }},
		{"successor table", func(v *retainedMutation) { v.TableID = "other" }},
		{"successor table name", func(v *retainedMutation) { v.TableName = "other" }},
		{"successor record", func(v *retainedMutation) { v.RecordID = "other" }},
		{"successor primary field", func(v *retainedMutation) { v.PrimaryKeyFieldID = "other" }},
		{"successor primary type", func(v *retainedMutation) { v.PrimaryKeyLogicalType = "uuid" }},
		{"successor operation", func(v *retainedMutation) { v.Operation = "insert" }},
		{"successor client version", func(v *retainedMutation) { v.ClientVersion = "other" }},
		{"successor schema", func(v *retainedMutation) { v.AuthoredSchema.Version = 1 }},
		{"successor fields", func(v *retainedMutation) { v.AuthoredFields = original.AuthoredFields[:1] }},
		{"successor identity", func(v *retainedMutation) { v.MutationID = "other" }},
		{"successor field order", func(v *retainedMutation) {
			v.AuthoredFields = []retainedField{original.AuthoredFields[1], original.AuthoredFields[0]}
		}},
		{"successor lineage", func(v *retainedMutation) { v.NormalizedMutationID = &m1 }},
		{"successor dependency", func(v *retainedMutation) { v.DependsOnMutationID = &m1 }},
		{"successor base", func(v *retainedMutation) { v.BaseVersion = &localBase }},
		{"successor state", func(v *retainedMutation) { v.Status = "pending" }},
		{"successor missing batch", func(v *retainedMutation) { v.SealedBatchID = nil }},
		{"successor ordinal", func(v *retainedMutation) { other := int64(1); v.SealedOrdinal = &other }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := sealed
			test.mutate(&changed)
			if requireSchemaProofSuccessorTransition(original, changed, acceptedBase) == nil {
				t.Fatal("invalid singleton successor transition passed")
			}
		})
	}
	alreadySealed := original
	alreadySealed.Status = "sealed"
	alreadySealed.SealedBatchID = &batch
	alreadySealed.SealedOrdinal = &ordinal
	if requireSchemaProofSuccessorTransition(alreadySealed, sealed, acceptedBase) == nil {
		t.Fatal("already sealed M2 rebased in place")
	}
	changedMembership := directSealed
	otherBatch := "other-batch"
	changedMembership.SealedBatchID = &otherBatch
	if requireSchemaProofOriginal(directSealed, []retainedMutation{changedMembership}) == nil {
		t.Fatal("sealed M1 membership changed")
	}
}

func TestSchemaProofComparisonsRejectCorruption(t *testing.T) {
	source, target := schemaRef{Version: 1, Hash: strings.Repeat("a", 64)}, schemaRef{Version: 2, Hash: strings.Repeat("b", 64)}
	old, issued := "old-cursor", "actual-replacement"
	intent := retainedMutation{MutationID: "original", LocalOrder: 1, AuthoredSchema: source, RecordID: "row", BaseVersion: &old, SourceKind: "local", Status: "pending", AuthoredFields: []retainedField{{FieldID: "value", LogicalType: "string", Value: json.RawMessage(`"42"`)}}}
	before := schemaProofSnapshot{Result: Result{PhysicalSchema: []byte(`[{"s1":true}]`)}, Schema: &source, ScopeStates: []scopeStateRecord{{ScopeID: "scope", Cursor: &old}}, RetainedMutations: []retainedMutation{intent}}
	prepared := before
	journal := migrationJournalCapture{Source: source, Target: target, Phase: "prepared"}
	if err := validateSchemaProofActivation(before, prepared, journal, issued, true); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*schemaProofSnapshot)
	}{
		{"activation cursor", func(v *schemaProofSnapshot) { v.ScopeStates = []scopeStateRecord{{ScopeID: "scope", Cursor: &issued}} }},
		{"physical schema", func(v *schemaProofSnapshot) { v.PhysicalSchema = []byte(`[{"s2":true}]`) }},
		{"queued intent", func(v *schemaProofSnapshot) {
			changed := intent
			changed.BaseVersion = &issued
			v.RetainedMutations = []retainedMutation{changed}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := prepared
			test.mutate(&changed)
			if validateSchemaProofActivation(before, changed, journal, issued, true) == nil {
				t.Fatal("corrupt prepared capture passed")
			}
		})
	}
	committed := before
	committed.Schema = &target
	committed.PhysicalSchema = []byte(`[{"s2":true}]`)
	committed.ScopeStates = []scopeStateRecord{{ScopeID: "scope", Cursor: &issued}}
	journal.Phase = "ddl_applied"
	if err := validateSchemaProofActivation(before, committed, journal, issued, false); err != nil {
		t.Fatal(err)
	}
	committed.ScopeStates = []scopeStateRecord{{ScopeID: "scope", Cursor: &old}}
	if validateSchemaProofActivation(before, committed, journal, issued, false) == nil {
		t.Fatal("committed activation retained old cursor")
	}
	flag := false
	fixture := &scenarios.NativeLocalFixture{TableName: "schema_proof_local", ID: "sentinel", Value: "preserve-local"}
	capture := schemaProofSnapshot{Result: Result{MigrationJournal: []byte(`null`), MigrationJournalTruncated: &flag, PhysicalSchema: []byte(`[]`), PhysicalSchemaTruncated: &flag, CaptureOverflowed: &flag}, ApplicationRows: []map[string]json.RawMessage{{"id": []byte(`"sentinel"`), "value": []byte(`"preserve-local"`)}}}
	if err := requireSchemaProofSentinel(fixture, capture); err != nil {
		t.Fatal(err)
	}
	capture.ApplicationRows[0]["value"] = []byte(`"changed"`)
	if requireSchemaProofSentinel(fixture, capture) == nil {
		t.Fatal("changed sentinel passed")
	}
	server := blackbox.NativeCaptureFacts{StateFacts: scenarios.StateFacts{Rows: []scenarios.RowFact{{TableID: "items", CanonicalWireJSON: `"row"`, Version: "v1", Checksum: strings.Repeat("c", 64)}}, MutationOutcomes: []scenarios.MutationOutcomeIdentityFact{{UserID: "user-a", ClientID: "client-a", MutationID: "actual-m1"}}}}
	if err := compareSchemaProofServer(server, server); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*blackbox.NativeCaptureFacts)
	}{
		{"row version", func(v *blackbox.NativeCaptureFacts) {
			v.StateFacts.Rows = append([]scenarios.RowFact(nil), v.StateFacts.Rows...)
			v.StateFacts.Rows[0].Version = "v2"
		}},
		{"row checksum", func(v *blackbox.NativeCaptureFacts) {
			v.StateFacts.Rows = append([]scenarios.RowFact(nil), v.StateFacts.Rows...)
			v.StateFacts.Rows[0].Checksum = strings.Repeat("d", 64)
		}},
		{"history identity", func(v *blackbox.NativeCaptureFacts) {
			v.StateFacts.MutationOutcomes = append([]scenarios.MutationOutcomeIdentityFact(nil), v.StateFacts.MutationOutcomes...)
			v.StateFacts.MutationOutcomes[0].MutationID = "other"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := server
			test.mutate(&changed)
			if compareSchemaProofServer(server, changed) == nil {
				t.Fatal("changed historical server state passed")
			}
		})
	}
}

func TestSchemaProofPhysicalComparisonChecksEveryColumn(t *testing.T) {
	scenario := loadSchemaCheckScenario(t)
	var manifest, authoredWrite json.RawMessage
	for _, step := range scenario.Steps {
		if step.ID == "STEP-PERF-SCHEMA-CHECK-CLASS2-PUBLISH-001" {
			manifest = step.Operation.Payload
		}
		if step.ID == "STEP-PERF-SCHEMA-CHECK-PROOF-COMMITTED-M2-WRITE-001" {
			authoredWrite = step.Operation.Payload
		}
	}
	runtimeWrite := json.RawMessage(`{"table_id":"cf_items","pk":{"id":"row"},"columns":{"value":"42","note":"later-note"}}`)
	columns := []physicalSchemaColumn{
		{TableName: "cf_items", Name: "id", Type: "TEXT", PrimaryKeyPosition: 1},
		{TableName: "cf_items", Name: "value", Type: "TEXT", NotNull: true},
		{TableName: "cf_items", Name: "note", Type: "TEXT"},
		{TableName: "cf_items", Name: "owner_id", Type: "TEXT", NotNull: true},
		{TableName: "cf_items", Name: "updated_at", Type: "TEXT", NotNull: true},
		{TableName: "cf_items", Name: "deleted_at", Type: "TEXT"},
		{TableName: "cf_global_items", Name: "id", Type: "TEXT", PrimaryKeyPosition: 1},
	}
	encoded, _ := json.Marshal(columns)
	if err := compareSchemaProofPhysical(manifest, authoredWrite, runtimeWrite, encoded); err != nil {
		t.Fatal(err)
	}
	var setup struct {
		InitialSchema json.RawMessage `json:"initial_schema"`
	}
	if err := json.Unmarshal(scenario.Model.Setup[0].Payload, &setup); err != nil {
		t.Fatal(err)
	}
	for _, step := range scenario.Steps {
		if step.ID == "STEP-PERF-SCHEMA-CHECK-PROOF-PREPARED-WRITE-001" {
			initialColumns, _ := json.Marshal(append(append([]physicalSchemaColumn(nil), columns[:2]...), columns[3:]...))
			initialWrite := json.RawMessage(`{"table_id":"cf_items","pk":{"id":"row"},"columns":{"value":"42"}}`)
			if err := compareSchemaProofPhysical(setup.InitialSchema, step.Operation.Payload, initialWrite, initialColumns); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, test := range []struct {
		name   string
		mutate func([]physicalSchemaColumn) []physicalSchemaColumn
	}{
		{"missing column", func(v []physicalSchemaColumn) []physicalSchemaColumn { return v[1:] }},
		{"type", func(v []physicalSchemaColumn) []physicalSchemaColumn { v[1].Type = "INTEGER"; return v }},
		{"nullability", func(v []physicalSchemaColumn) []physicalSchemaColumn { v[2].NotNull = true; return v }},
		{"primary key", func(v []physicalSchemaColumn) []physicalSchemaColumn { v[0].PrimaryKeyPosition = 0; return v }},
		{"primary key pragma nullability", func(v []physicalSchemaColumn) []physicalSchemaColumn { v[0].NotNull = true; return v }},
		{"table mapping", func(v []physicalSchemaColumn) []physicalSchemaColumn { v[0].TableName = "items"; return v }},
		{"column mapping", func(v []physicalSchemaColumn) []physicalSchemaColumn { v[1].Name = "other"; return v }},
		{"missing support column", func(v []physicalSchemaColumn) []physicalSchemaColumn { return append(v[:3], v[4:]...) }},
		{"owner type", func(v []physicalSchemaColumn) []physicalSchemaColumn { v[3].Type = "INTEGER"; return v }},
		{"owner nullability", func(v []physicalSchemaColumn) []physicalSchemaColumn { v[3].NotNull = false; return v }},
		{"support primary key", func(v []physicalSchemaColumn) []physicalSchemaColumn { v[3].PrimaryKeyPosition = 1; return v }},
		{"updated nullability", func(v []physicalSchemaColumn) []physicalSchemaColumn { v[4].NotNull = false; return v }},
		{"deleted nullability", func(v []physicalSchemaColumn) []physicalSchemaColumn { v[5].NotNull = true; return v }},
		{"duplicate subject", func(v []physicalSchemaColumn) []physicalSchemaColumn { return append(v, v[0]) }},
		{"invalid unrelated column", func(v []physicalSchemaColumn) []physicalSchemaColumn { v[6].Name = ""; return v }},
		{"extra column", func(v []physicalSchemaColumn) []physicalSchemaColumn {
			return append(v, physicalSchemaColumn{TableName: "cf_items", Name: "extra", Type: "TEXT"})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			mutated := test.mutate(append([]physicalSchemaColumn(nil), columns...))
			encoded, _ := json.Marshal(mutated)
			if compareSchemaProofPhysical(manifest, authoredWrite, runtimeWrite, encoded) == nil {
				t.Fatal("corrupt complete physical schema passed")
			}
		})
	}
	for _, test := range []struct{ name, write string }{
		{"wrong table", `{"table_id":"other","pk":{"id":"row"},"columns":{"value":"42","note":"later-note"}}`},
		{"wrong primary name", `{"table_id":"cf_items","pk":{"other":"row"},"columns":{"value":"42","note":"later-note"}}`},
		{"renamed value", `{"table_id":"cf_items","pk":{"id":"row"},"columns":{"other":"42","note":"later-note"}}`},
		{"renamed note", `{"table_id":"cf_items","pk":{"id":"row"},"columns":{"value":"42","other":"later-note"}}`},
		{"swapped fields", `{"table_id":"cf_items","pk":{"id":"row"},"columns":{"value":"later-note","note":"42"}}`},
		{"null value", `{"table_id":"cf_items","pk":{"id":"row"},"columns":{"value":null,"note":"later-note"}}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if compareSchemaProofPhysical(manifest, authoredWrite, []byte(test.write), encoded) == nil {
				t.Fatal("invalid fixed fixture binding passed")
			}
		})
	}
}

func TestSchemaProofStoredOutcomeRequiresActualOriginalIdentity(t *testing.T) {
	id := "original-m1"
	schema := strings.Repeat("a", 64)
	outcome := `{"mutation_id":"` + id + `","status":"applied","outcome_schema":{"version":1,"hash":"` + schema + `"},"server_version":"accepted-base","server_row":{"value":"41"}}`
	push := schemaProofPush{Status: 200, Request: []byte(`{"batch_id":"actual-batch","mutations":[{"mutation_id":"` + id + `"}]}`), Response: []byte(`{"accepted":[` + outcome + `],"rejected":[]}`)}
	flag := false
	ledger := 1
	capture := schemaProofSnapshot{Result: Result{RetainedMutations: []byte(`[]`), AcceptedMutationOutcomes: acceptedMutationOutcomes{id: outcome}, AcceptedMutationOutcomesTruncated: &flag, MutationLedgerCount: &ledger}}
	if err := requireSchemaProofStoredOutcome(capture, push); err != nil {
		t.Fatal(err)
	}
	original := retainedMutation{TableName: "physical_items", RecordID: "proof-row"}
	metadata := rowMetadataRecord{TableName: "physical_items", RecordID: "proof-row", ServerVersion: "accepted-base"}
	capture.RowMetadata, _ = json.Marshal([]rowMetadataRecord{{TableName: "other", RecordID: "proof-row", ServerVersion: "decoy-version"}, metadata})
	if err := requireSchemaProofRowMetadata(capture, original, push); err != nil {
		t.Fatal(err)
	}
	latest := push
	latest.Response = []byte(strings.Replace(string(push.Response), "accepted-base", "latest-m2-version", 1))
	latestMetadata := metadata
	latestMetadata.ServerVersion = "latest-m2-version"
	latestCapture := capture
	latestCapture.RowMetadata, _ = json.Marshal([]rowMetadataRecord{latestMetadata})
	if err := requireSchemaProofRowMetadata(latestCapture, original, latest); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		records []rowMetadataRecord
	}{
		{"missing", []rowMetadataRecord{}},
		{"ambiguous", []rowMetadataRecord{metadata, metadata}},
		{"wrong table", []rowMetadataRecord{{TableName: "other", RecordID: "proof-row", ServerVersion: "accepted-base"}}},
		{"wrong row", []rowMetadataRecord{{TableName: "physical_items", RecordID: "other", ServerVersion: "accepted-base"}}},
		{"wrong version", []rowMetadataRecord{{TableName: "physical_items", RecordID: "proof-row", ServerVersion: "other"}}},
		{"stale M1 version", []rowMetadataRecord{metadata}},
	} {
		t.Run("metadata "+test.name, func(t *testing.T) {
			changed := capture
			changed.RowMetadata, _ = json.Marshal(test.records)
			expected := push
			if test.name == "stale M1 version" {
				expected = latest
			}
			if requireSchemaProofRowMetadata(changed, original, expected) == nil {
				t.Fatal("invalid proof row metadata passed")
			}
		})
	}
	for _, raw := range []string{`null`, `{}`, `[{"server_version":42}]`} {
		changed := capture
		changed.RowMetadata = []byte(raw)
		if requireSchemaProofRowMetadata(changed, original, push) == nil {
			t.Fatal("invalid proof metadata array passed")
		}
	}
	changed := capture
	changed.AcceptedMutationOutcomes = acceptedMutationOutcomes{"other-original": strings.Replace(outcome, id, "other-original", 1)}
	if requireSchemaProofStoredOutcome(changed, push) == nil {
		t.Fatal("count-only accepted identity passed")
	}
	changed = capture
	changed.AcceptedMutationOutcomes = acceptedMutationOutcomes{id: strings.Replace(outcome, `"41"`, `"42"`, 1)}
	if requireSchemaProofStoredOutcome(changed, push) == nil {
		t.Fatal("changed historical stored outcome passed")
	}
	changed = capture
	truncated := true
	changed.AcceptedMutationOutcomesTruncated = &truncated
	if requireSchemaProofStoredOutcome(changed, push) == nil {
		t.Fatal("truncated accepted outcomes passed")
	}
	changed = capture
	changed.AcceptedMutationOutcomes = acceptedMutationOutcomes{id: outcome, "extra": strings.Replace(outcome, id, "extra", 1)}
	if requireSchemaProofStoredOutcome(changed, push) == nil {
		t.Fatal("extra accepted record passed exact ledger closure")
	}
	changed = capture
	changed.Result.RetainedMutations = []byte(`[{"mutation_id":"original-m1"}]`)
	if requireSchemaProofStoredOutcome(changed, push) == nil {
		t.Fatal("accepted original also appeared in retained records")
	}
}

func TestSchemaProofRecoveryWireUsesRecoveredCursorForFirstRequests(t *testing.T) {
	scenario := loadSchemaCheckScenario(t)
	steps, err := kotlinScenarioStepMap(scenario, schemaCheckScenarioID, 64)
	if err != nil {
		t.Fatal(err)
	}
	target := schemaRef{Version: 2, Hash: strings.Repeat("b", 64)}
	cursor, oldCursor := "installed-replacement", "before-call-cursor"
	recovered := schemaProofSnapshot{Schema: &target, ScopeStates: []scopeStateRecord{{ScopeID: "scope", Cursor: &cursor}}}
	complete, incomplete := true, false
	fingerprint := cursorFingerprint(cursor)
	connect := TransportObservation{OperationClass: "connect", StatusCode: 200, CursorFingerprints: []string{fingerprint}, CursorFingerprintsComplete: &complete, RequestFacts: &TransportRequestFacts{SchemaVersion: target.Version, SchemaHash: target.Hash}, ConnectResponseFacts: &TransportConnectResponseFacts{Action: "none", SchemaVersion: target.Version, SchemaHash: target.Hash}}
	first := TransportObservation{OperationClass: "pull", StatusCode: 200, CursorFingerprints: []string{fingerprint}, CursorFingerprintsComplete: &complete, RequestFacts: &TransportRequestFacts{SchemaVersion: target.Version, SchemaHash: target.Hash}}
	terminal := first
	terminal.CursorFingerprints = []string{cursorFingerprint("later-page-cursor")}
	beforeStates, _ := json.Marshal([]scopeStateRecord{{ScopeID: "scope", Cursor: &oldCursor}})
	call := SynchronizationResult{before: &Result{ScopeStates: beforeStates}, transportObservations: []TransportObservation{connect, {OperationClass: "push", StatusCode: 200}, first, terminal}}
	if err := validateSchemaProofRecoveryWire(scenario, steps, "PREPARED", call, target, recovered); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*SynchronizationResult)
	}{
		{"connect S1", func(v *SynchronizationResult) {
			v.transportObservations[0].RequestFacts = &TransportRequestFacts{SchemaVersion: 1, SchemaHash: strings.Repeat("a", 64)}
		}},
		{"connect missing request", func(v *SynchronizationResult) { v.transportObservations[0].RequestFacts = nil }},
		{"connect old cursor", func(v *SynchronizationResult) {
			v.transportObservations[0].CursorFingerprints = []string{cursorFingerprint(oldCursor)}
		}},
		{"connect incomplete cursor", func(v *SynchronizationResult) { v.transportObservations[0].CursorFingerprintsComplete = &incomplete }},
		{"connect missing completeness", func(v *SynchronizationResult) { v.transportObservations[0].CursorFingerprintsComplete = nil }},
		{"connect extra cursor", func(v *SynchronizationResult) {
			v.transportObservations[0].CursorFingerprints = []string{fingerprint, cursorFingerprint(oldCursor)}
		}},
		{"first pull S1", func(v *SynchronizationResult) {
			v.transportObservations[2].RequestFacts = &TransportRequestFacts{SchemaVersion: 1, SchemaHash: strings.Repeat("a", 64)}
		}},
		{"first pull failed", func(v *SynchronizationResult) { v.transportObservations[2].StatusCode = 503 }},
		{"first pull old cursor", func(v *SynchronizationResult) {
			v.transportObservations[2].CursorFingerprints = []string{cursorFingerprint(oldCursor)}
		}},
		{"first pull incomplete cursor", func(v *SynchronizationResult) { v.transportObservations[2].CursorFingerprintsComplete = &incomplete }},
		{"first pull absent cursor", func(v *SynchronizationResult) { v.transportObservations[2].CursorFingerprints = nil }},
		{"first pull missing request", func(v *SynchronizationResult) { v.transportObservations[2].RequestFacts = nil }},
		{"terminal failure", func(v *SynchronizationResult) { v.transportObservations[3].StatusCode = 503 }},
		{"no pull", func(v *SynchronizationResult) {
			v.transportObservations = v.transportObservations[:3]
			v.transportObservations[2].OperationClass = "push"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := call
			changed.transportObservations = append([]TransportObservation(nil), call.transportObservations...)
			test.mutate(&changed)
			if validateSchemaProofRecoveryWire(scenario, steps, "PREPARED", changed, target, recovered) == nil {
				t.Fatal("invalid recovery request passed")
			}
		})
	}
	recovered.ScopeStates = nil
	if validateSchemaProofRecoveryWire(scenario, steps, "PREPARED", call, target, recovered) == nil {
		t.Fatal("absent recovered cursor passed")
	}
}

func TestSchemaCheckBindingsFollowAuthoredWireCompletions(t *testing.T) {
	scenario := loadSchemaCheckScenario(t)
	steps, err := kotlinScenarioStepMap(scenario, schemaCheckScenarioID, 64)
	if err != nil {
		t.Fatalf("validate schema-check scenario: %v", err)
	}
	publicCount, err := validateSchemaCheckBindings(scenario, steps)
	if err != nil {
		t.Fatalf("validate schema-check bindings: %v", err)
	}
	if publicCount != 36 {
		t.Fatalf("schema-check measured and prewarm calls = %d, want 36", publicCount)
	}
	for _, step := range scenario.Steps {
		if step.NativeBinding.Kind != "public-call" || strings.HasPrefix(string(step.ID), "STEP-PERF-SCHEMA-CHECK-PROOF-") {
			continue
		}
		wire, err := schemaCheckWireExpectation(scenario, step.ID)
		if err != nil {
			t.Fatalf("read schema-check wire expectation %s: %v", step.ID, err)
		}
		if step.NativeBinding.Completion != schemaCheckNativeCompletion(wire) {
			t.Fatalf("schema-check step %s completion = %q, want authored completion %q", step.ID, step.NativeBinding.Completion, schemaCheckNativeCompletion(wire))
		}
	}
}

func TestSchemaCheckUnsupportedWireDerivesErrorCompletion(t *testing.T) {
	scenario := loadSchemaCheckScenario(t)
	for _, id := range []scenarios.StepID{
		"STEP-PERF-SCHEMA-CHECK-016",
		"STEP-PERF-SCHEMA-CHECK-017",
		"STEP-PERF-SCHEMA-CHECK-018",
	} {
		step, found := schemaCheckStep(scenario, id)
		if !found || step.NativeBinding == nil {
			t.Fatalf("schema-check step %s is absent", id)
		}
		wire, err := schemaCheckWireExpectation(scenario, id)
		if err != nil {
			t.Fatalf("read schema-check wire expectation %s: %v", id, err)
		}
		if wire.Action != "unsupported" || wire.HTTPStatus != 200 {
			t.Fatalf("schema-check step %s does not carry the authored unsupported 200 wire case", id)
		}
		if got := schemaCheckNativeCompletion(wire); got != "error" || step.NativeBinding.Completion != got {
			t.Fatalf("schema-check step %s completion = %q, want error from unsupported wire action", id, step.NativeBinding.Completion)
		}
	}
}

func TestSchemaCheckBindingRejectsCompletionNotDerivedFromWire(t *testing.T) {
	scenario := loadSchemaCheckScenario(t)
	for index := range scenario.Steps {
		if scenario.Steps[index].ID != "STEP-PERF-SCHEMA-CHECK-016" {
			continue
		}
		binding := *scenario.Steps[index].NativeBinding
		binding.Completion = "idle"
		scenario.Steps[index].NativeBinding = &binding
	}
	steps, err := kotlinScenarioStepMap(scenario, schemaCheckScenarioID, 64)
	if err != nil {
		t.Fatalf("validate mutated schema-check scenario: %v", err)
	}
	if _, err := validateSchemaCheckBindings(scenario, steps); err == nil {
		t.Fatal("schema-check binding with an idle completion for unsupported wire action passed validation")
	}
}

func loadSchemaCheckScenario(t *testing.T) scenarios.Scenario {
	t.Helper()
	scenario, err := scenarios.LoadFile(context.Background(), filepath.Join("..", ".."), "conformance/scenarios/performance/schema-check-001.json")
	if err != nil {
		t.Fatalf("load schema-check scenario: %v", err)
	}
	return scenario
}

func schemaCheckStep(scenario scenarios.Scenario, id scenarios.StepID) (scenarios.Step, bool) {
	for _, step := range scenario.Steps {
		if step.ID == id {
			return step, true
		}
	}
	return scenarios.Step{}, false
}

func TestSchemaCheckDispatchRejectsWrongActionOldCursorAndIncompleteCapture(t *testing.T) {
	scenario := loadSchemaCheckScenario(t)
	step, found := schemaCheckStep(scenario, "STEP-PERF-SCHEMA-CHECK-007")
	if !found {
		t.Fatal("authored Class 2 step is absent")
	}
	wire, err := schemaCheckWireExpectation(scenario, step.ID)
	if err != nil {
		t.Fatal(err)
	}
	source, target := schemaRef{Version: 1, Hash: strings.Repeat("a", 64)}, schemaRef{Version: 2, Hash: strings.Repeat("b", 64)}
	scope, oldCursor, finalCursor := "runtime-scope", "old-cursor", "later-pull-cursor"
	issued := cursorFingerprint("issued-replacement")
	complete, notTruncated := true, false
	encode := func(value any) json.RawMessage {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	newCall := func() SynchronizationResult {
		before := Result{Schema: encode(source), ScopeStates: encode([]scopeStateRecord{{ScopeID: scope, Cursor: &oldCursor}}), Events: []byte("[]"), TransportObservations: &TransportObservationSnapshot{SequenceCheckpoint: 4}, MigrationJournal: []byte("null"), MigrationJournalTruncated: &notTruncated, PhysicalSchema: []byte("[]"), PhysicalSchemaTruncated: &notTruncated, CaptureOverflowed: &notTruncated}
		after := Result{Schema: encode(target), ScopeStates: encode([]scopeStateRecord{{ScopeID: scope, Cursor: &finalCursor}}), TransportObservations: &TransportObservationSnapshot{SequenceCheckpoint: 6}, MigrationJournal: []byte("null"), MigrationJournalTruncated: &notTruncated, PhysicalSchema: []byte("[]"), PhysicalSchemaTruncated: &notTruncated, CaptureOverflowed: &notTruncated, Events: encode([]any{
			map[string]any{"type": "schema_applying", "source_schema": source, "target_schema": target, "schema_action": wire.Action},
			map[string]any{"type": "schema_applied", "source_schema": source, "target_schema": target, "schema_action": wire.Action},
		})}
		return SynchronizationResult{Completion: "idle", before: &before, after: &after, transportObservations: []TransportObservation{
			{Sequence: 5, OperationClass: "connect", StatusCode: 200, DurationNanoseconds: 1, CursorFingerprints: []string{cursorFingerprint(oldCursor)}, CursorFingerprintsComplete: &complete, RequestFacts: &TransportRequestFacts{SchemaVersion: source.Version, SchemaHash: source.Hash}, ConnectResponseFacts: &TransportConnectResponseFacts{Action: wire.Action, SchemaVersion: target.Version, SchemaHash: target.Hash, AffectedScopeFingerprints: []string{}, AffectedScopesComplete: true, ScopeCursorUpdates: map[string]*string{cursorFingerprint(scope): &issued}, ScopeCursorUpdatesComplete: true}},
			{Sequence: 6, OperationClass: "pull", StatusCode: 200, DurationNanoseconds: 1, CursorFingerprints: []string{issued}, CursorFingerprintsComplete: &complete, RequestFacts: &TransportRequestFacts{SchemaVersion: target.Version, SchemaHash: target.Hash}},
		}}
	}
	if err := validateSchemaCheckDispatch(step, wire.Action, newCall(), target, scope, false); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*SynchronizationResult)
	}{
		{"wrong action", func(call *SynchronizationResult) { call.transportObservations[0].ConnectResponseFacts.Action = "none" }},
		{"old cursor on target pull", func(call *SynchronizationResult) {
			call.transportObservations[1].CursorFingerprints = []string{cursorFingerprint(oldCursor)}
		}},
		{"missing migration journal", func(call *SynchronizationResult) { call.before.MigrationJournal = nil }},
		{"truncated migration journal", func(call *SynchronizationResult) { call.before.MigrationJournalTruncated = &complete }},
		{"truncated physical schema", func(call *SynchronizationResult) { call.after.PhysicalSchemaTruncated = &complete }},
		{"overall capture overflow", func(call *SynchronizationResult) { call.after.CaptureOverflowed = &complete }},
		{"missing aggregate overflow", func(call *SynchronizationResult) { call.before.CaptureOverflowed = nil }},
		{"uncaptured row metadata", func(call *SynchronizationResult) { count := maximumRecords + 1; call.after.RowMetadataCount = &count }},
		{"full event ring", func(call *SynchronizationResult) {
			var events []map[string]any
			if err := json.Unmarshal(call.after.Events, &events); err != nil {
				t.Fatal(err)
			}
			for len(events) < maximumRecords {
				events = append(events, map[string]any{"type": "state_changed"})
			}
			call.after.Events = encode(events)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			call := newCall()
			test.mutate(&call)
			if err := validateSchemaCheckDispatch(step, wire.Action, call, target, scope, false); err == nil {
				t.Fatal("invalid schema dispatch passed")
			}
		})
	}
}

func TestMigrationCaptureProtocolPreservesKotlinStoredFields(t *testing.T) {
	for _, target := range []string{"migration_prepared", "migration_committed"} {
		if validTransportOperation(target) {
			t.Fatal("migration checkpoint became an HTTP operation")
		}
		for _, operation := range []string{"arm-transport-pause", "await-transport-pause"} {
			request := Request{SchemaVersion: 1, SessionID: "session-1", Operation: operation, TransportOperation: target}
			if err := validateRequest(request); err != nil {
				t.Fatal(err)
			}
			request.TransportOperation = "migration_unknown"
			if err := validateRequest(request); err == nil {
				t.Fatal("unknown migration checkpoint accepted")
			}
		}
	}
	stored := map[string]string{"journal_version": "2", "migration_plan_version": "2", "reset_materialization": "0", "target_manifest_json": "{}", "affected_scopes_json": "[]", "scope_cursor_updates_json": "{}", "target_tables_json": "[]", "migration_plan_json": " {\"kotlin_operations\":[]} ", "migration_plan_hash": strings.Repeat("a", 64)}
	encode := func(value any) json.RawMessage {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	journal := migrationJournalCapture{Source: schemaRef{}, Target: schemaRef{Version: 1, Hash: strings.Repeat("b", 64)}, Action: "replace", Phase: "ddl_applied", Stored: stored}
	notTruncated := false
	result := Result{ProcessID: "1234", DatabaseIdentityFingerprint: strings.Repeat("a", 64), TransportObservations: &TransportObservationSnapshot{Observations: []TransportObservation{}}, MigrationJournal: encode(journal), MigrationJournalTruncated: &notTruncated, PhysicalSchema: []byte(`[{"table_name":"items","name":"id","type":"TEXT","not_null":false,"primary_key_position":1}]`), PhysicalSchemaTruncated: &notTruncated, CaptureOverflowed: &notTruncated}
	decoded, err := decodeResult(encode(result))
	if err != nil {
		t.Fatal(err)
	}
	inspection, err := decodeMigrationJournal(decoded.MigrationJournal)
	if err != nil || inspection == nil || inspection.Stored["migration_plan_json"] != stored["migration_plan_json"] {
		t.Fatalf("stored bytes changed: %v", err)
	}
	if err := decoded.requireCompleteMigrationCapture(); err != nil {
		t.Fatal(err)
	}
	legacy := result
	legacy.MigrationJournal = nil
	legacy.MigrationJournalTruncated = nil
	legacy.PhysicalSchema = nil
	legacy.PhysicalSchemaTruncated = nil
	legacy.CaptureOverflowed = nil
	if _, err := decodeResult(encode(legacy)); err != nil {
		t.Fatal(err)
	}
	if err := legacy.requireCompleteMigrationCapture(); err == nil {
		t.Fatal("missing migration capture proved complete")
	}
	for _, test := range []struct {
		name   string
		mutate func(*Result)
	}{
		{"missing flag", func(value *Result) { value.MigrationJournalTruncated = nil }},
		{"wrong physical column type", func(value *Result) {
			value.PhysicalSchema = []byte(`[{"table_name":"items","name":"id","type":"TEXT","not_null":false,"primary_key_position":"1"}]`)
		}},
		{"unknown journal key", func(value *Result) {
			changed := journal
			changed.Stored = map[string]string{}
			for key, text := range stored {
				changed.Stored[key] = text
			}
			changed.Stored["is_schema_reset"] = "0"
			value.MigrationJournal = encode(changed)
		}},
		{"oversized stored bytes", func(value *Result) {
			changed := journal
			changed.Stored = map[string]string{}
			for key, text := range stored {
				changed.Stored[key] = text
			}
			changed.Stored["migration_plan_json"] = strings.Repeat("x", 65_536)
			value.MigrationJournal = encode(changed)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := result
			test.mutate(&changed)
			if _, err := decodeResult(encode(changed)); err == nil {
				t.Fatal("invalid migration capture decoded")
			}
		})
	}
}
