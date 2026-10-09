package swift

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestWireExpectationRejectsMissingAndMismatchedObservations(t *testing.T) {
	code, wrongCode := "temporary_unavailable", "auth_required"
	scenario := scenarios.Scenario{WireExpectations: []scenarios.WireExpectation{
		{StepID: "STEP-ERROR-001", HTTPStatus: 503, Retryable: true, ErrorCode: &code},
	}}
	retryable, terminal := true, false
	observed := transportObservation{OperationClass: "pull", StatusCode: 503, Retryable: &retryable, ErrorCode: &code}
	result := SynchronizationResult{transportObservations: []transportObservation{observed}}
	if err := validateSwiftWireExpectation(scenario, "STEP-ERROR-001", "pull", result); err != nil {
		t.Fatalf("matching observed error failed: %v", err)
	}
	for _, test := range []struct {
		name   string
		change func(*transportObservation)
	}{
		{"status", func(value *transportObservation) { value.StatusCode = 200 }},
		{"retryability", func(value *transportObservation) { value.Retryable = &terminal }},
		{"missing retryability", func(value *transportObservation) { value.Retryable = nil }},
		{"canonical code", func(value *transportObservation) { value.ErrorCode = &wrongCode }},
		{"missing code", func(value *transportObservation) { value.ErrorCode = nil }},
		{"missing operation", func(value *transportObservation) { value.OperationClass = "connect" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := observed
			test.change(&changed)
			result := SynchronizationResult{transportObservations: []transportObservation{changed}}
			if err := validateSwiftWireExpectation(scenario, "STEP-ERROR-001", "pull", result); err == nil {
				t.Fatal("mismatched observed wire result passed")
			}
		})
	}
	if err := validateSwiftWireExpectation(scenario, "STEP-ABSENT-001", "pull", result); err == nil {
		t.Fatal("missing authored expectation passed")
	}
	if err := validateSwiftWireExpectation(scenario, "STEP-ERROR-001", "pull", SynchronizationResult{}); err == nil {
		t.Fatal("missing executed exchange passed")
	}
}

func TestSeededEmptyStartupDirectBindingGroupsRemainClosed(t *testing.T) {
	root := filepath.Join("..", "..")
	scenario, err := scenarios.LoadFile(context.Background(), root, "conformance/scenarios/performance/seeded-empty-startup-001.json")
	if err != nil {
		t.Fatalf("load seeded-startup scenario: %v", err)
	}
	steps, err := swiftScenarioStepMap(scenario, seededEmptyStartupScenarioID, 29)
	if err != nil {
		t.Fatalf("validate seeded-startup scenario: %v", err)
	}
	for _, number := range []int{3, 6, 9, 11, 13, 15} {
		id := scenarios.StepID(fmt.Sprintf("STEP-PERF-SEEDED-EMPTY-STARTUP-%03d", number))
		step := steps[id]
		if step.NativeBinding == nil || step.NativeBinding.Kind != "public-call" || step.NativeBinding.Method != "start" || step.NativeBinding.Completion != "idle" {
			t.Fatalf("seeded-startup step %s is not a synchronous start binding", id)
		}
		if scenarios.OperationKey(step.Operation) != "connect/send" {
			t.Fatalf("seeded-startup step %s operation = %s", id, scenarios.OperationKey(step.Operation))
		}
	}
}

func TestSteadyPullBaselineAcceptsAuthoredRebuildPageSequence(t *testing.T) {
	status := 200
	result := SynchronizationResult{
		Completion: "idle",
		transportObservations: []transportObservation{
			{OperationClass: "connect", StatusCode: status},
			{OperationClass: "rebuild", StatusCode: status},
			{OperationClass: "rebuild", StatusCode: status},
			{OperationClass: "pull", StatusCode: status},
		},
	}
	scenario := scenarios.Scenario{WireExpectations: []scenarios.WireExpectation{
		{StepID: "STEP-PERF-STEADY-PULL-BASELINE-REQUEST-001", HTTPStatus: status},
		{StepID: "STEP-PERF-STEADY-PULL-001", HTTPStatus: status},
	}}
	if err := validateSwiftSteadyPullBaselineWires(scenario, result); err != nil {
		t.Fatalf("validate authored steady-pull baseline page sequence: %v", err)
	}
}

func TestSteadyPullBaselineRejectsUnexpectedWireOutcome(t *testing.T) {
	result := SynchronizationResult{
		Completion: "idle",
		transportObservations: []transportObservation{
			{OperationClass: "connect", StatusCode: 200},
			{OperationClass: "rebuild", StatusCode: 500, ErrorCode: pointerString("invalid_response")},
			{OperationClass: "pull", StatusCode: 200},
		},
	}
	scenario := scenarios.Scenario{WireExpectations: []scenarios.WireExpectation{
		{StepID: "STEP-PERF-STEADY-PULL-BASELINE-REQUEST-001", HTTPStatus: 200},
		{StepID: "STEP-PERF-STEADY-PULL-001", HTTPStatus: 200},
	}}
	if err := validateSwiftSteadyPullBaselineWires(scenario, result); err == nil {
		t.Fatal("unexpected steady-pull rebuild response passed authored wire validation")
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
	before := runnerResult{Schema: &source, ScopeStates: []scopeStateRecord{{ScopeID: "scope", Cursor: &old}}, RetainedMutations: []retainedMutation{intent}, PhysicalSchema: []byte(`[{"s1":true}]`)}
	prepared := before
	journal := migrationJournalCapture{Source: source, Target: target, Phase: "prepared"}
	if err := validateSchemaProofActivation(before, prepared, journal, issued, true); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*runnerResult)
	}{
		{"activation cursor", func(v *runnerResult) { v.ScopeStates = []scopeStateRecord{{ScopeID: "scope", Cursor: &issued}} }},
		{"physical schema", func(v *runnerResult) { v.PhysicalSchema = []byte(`[{"s2":true}]`) }},
		{"queued intent", func(v *runnerResult) {
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
	journal.Phase = "applied"
	if err := validateSchemaProofActivation(before, committed, journal, issued, false); err != nil {
		t.Fatal(err)
	}
	committed.ScopeStates = []scopeStateRecord{{ScopeID: "scope", Cursor: &old}}
	if validateSchemaProofActivation(before, committed, journal, issued, false) == nil {
		t.Fatal("committed activation retained old cursor")
	}
	flag := false
	fixture := &scenarios.NativeLocalFixture{TableName: "schema_proof_local", ID: "sentinel", Value: "preserve-local"}
	capture := runnerResult{MigrationJournal: []byte(`null`), MigrationJournalTruncated: &flag, PhysicalSchema: []byte(`[]`), PhysicalSchemaTruncated: &flag, CaptureOverflowed: &flag, ApplicationRows: []map[string]json.RawMessage{{"id": []byte(`"sentinel"`), "value": []byte(`"preserve-local"`)}}}
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
	scenario, err := scenarios.LoadFile(context.Background(), filepath.Join("..", ".."), "conformance/scenarios/performance/schema-check-001.json")
	if err != nil {
		t.Fatal(err)
	}
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
	capture := runnerResult{RetainedMutations: []retainedMutation{}, AcceptedMutationOutcomes: acceptedMutationOutcomes{id: outcome}, AcceptedMutationOutcomesTruncated: &flag, MutationLedgerCount: &ledger}
	if err := requireSchemaProofStoredOutcome(capture, push); err != nil {
		t.Fatal(err)
	}
	original := retainedMutation{TableName: "physical_items", RecordID: "proof-row"}
	metadata := rowMetadataRecord{TableName: "physical_items", RecordID: "proof-row", ServerVersion: "accepted-base"}
	capture.RowMetadataRecords = []rowMetadataRecord{{TableName: "other", RecordID: "proof-row", ServerVersion: "decoy-version"}, metadata}
	if err := requireSchemaProofRowMetadata(capture, original, push); err != nil {
		t.Fatal(err)
	}
	latest := push
	latest.Response = []byte(strings.Replace(string(push.Response), "accepted-base", "latest-m2-version", 1))
	latestMetadata := metadata
	latestMetadata.ServerVersion = "latest-m2-version"
	latestCapture := capture
	latestCapture.RowMetadataRecords = []rowMetadataRecord{latestMetadata}
	if err := requireSchemaProofRowMetadata(latestCapture, original, latest); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		records []rowMetadataRecord
	}{
		{"missing", nil},
		{"ambiguous", []rowMetadataRecord{metadata, metadata}},
		{"wrong table", []rowMetadataRecord{{TableName: "other", RecordID: "proof-row", ServerVersion: "accepted-base"}}},
		{"wrong row", []rowMetadataRecord{{TableName: "physical_items", RecordID: "other", ServerVersion: "accepted-base"}}},
		{"wrong version", []rowMetadataRecord{{TableName: "physical_items", RecordID: "proof-row", ServerVersion: "other"}}},
		{"stale M1 version", []rowMetadataRecord{metadata}},
	} {
		t.Run("metadata "+test.name, func(t *testing.T) {
			changed := capture
			changed.RowMetadataRecords = test.records
			expected := push
			if test.name == "stale M1 version" {
				expected = latest
			}
			if requireSchemaProofRowMetadata(changed, original, expected) == nil {
				t.Fatal("invalid proof row metadata passed")
			}
		})
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
	changed.RetainedMutations = []retainedMutation{{MutationID: id}}
	if requireSchemaProofStoredOutcome(changed, push) == nil {
		t.Fatal("accepted original also appeared in retained records")
	}
}

func TestSchemaProofRecoveryWireUsesRecoveredCursorForFirstRequests(t *testing.T) {
	scenario, err := scenarios.LoadFile(context.Background(), filepath.Join("..", ".."), "conformance/scenarios/performance/schema-check-001.json")
	if err != nil {
		t.Fatal(err)
	}
	steps, err := swiftScenarioStepMap(scenario, schemaCheckScenarioID, 64)
	if err != nil {
		t.Fatal(err)
	}
	target := schemaRef{Version: 2, Hash: strings.Repeat("b", 64)}
	cursor, oldCursor := "installed-replacement", "before-call-cursor"
	recovered := runnerResult{Schema: &target, ScopeStates: []scopeStateRecord{{ScopeID: "scope", Cursor: &cursor}}}
	complete, incomplete := true, false
	fingerprint := cursorFingerprint(cursor)
	connect := transportObservation{OperationClass: "connect", StatusCode: 200, CursorFingerprints: []string{fingerprint}, CursorFingerprintsComplete: &complete, RequestFacts: &transportRequestFacts{SchemaVersion: target.Version, SchemaHash: target.Hash}, ConnectResponseFacts: &transportConnectResponseFacts{Action: "none", SchemaVersion: target.Version, SchemaHash: target.Hash}}
	first := transportObservation{OperationClass: "pull", StatusCode: 200, CursorFingerprints: []string{fingerprint}, CursorFingerprintsComplete: &complete, RequestFacts: &transportRequestFacts{SchemaVersion: target.Version, SchemaHash: target.Hash}}
	terminal := first
	terminal.CursorFingerprints = []string{cursorFingerprint("later-page-cursor")}
	call := SynchronizationResult{before: &runnerResult{ScopeStates: []scopeStateRecord{{ScopeID: "scope", Cursor: &oldCursor}}}, transportObservations: []transportObservation{connect, {OperationClass: "push", StatusCode: 200}, first, terminal}}
	if err := validateSchemaProofRecoveryWire(scenario, steps, "PREPARED", call, target, recovered); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*SynchronizationResult)
	}{
		{"connect S1", func(v *SynchronizationResult) {
			v.transportObservations[0].RequestFacts = &transportRequestFacts{SchemaVersion: 1, SchemaHash: strings.Repeat("a", 64)}
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
			v.transportObservations[2].RequestFacts = &transportRequestFacts{SchemaVersion: 1, SchemaHash: strings.Repeat("a", 64)}
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
			changed.transportObservations = append([]transportObservation(nil), call.transportObservations...)
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
	root := filepath.Join("..", "..")
	scenario, err := scenarios.LoadFile(context.Background(), root, "conformance/scenarios/performance/schema-check-001.json")
	if err != nil {
		t.Fatalf("load schema-check scenario: %v", err)
	}
	steps, err := swiftScenarioStepMap(scenario, schemaCheckScenarioID, 64)
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
	root := filepath.Join("..", "..")
	scenario, err := scenarios.LoadFile(context.Background(), root, "conformance/scenarios/performance/schema-check-001.json")
	if err != nil {
		t.Fatalf("load schema-check scenario: %v", err)
	}
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
	root := filepath.Join("..", "..")
	scenario, err := scenarios.LoadFile(context.Background(), root, "conformance/scenarios/performance/schema-check-001.json")
	if err != nil {
		t.Fatalf("load schema-check scenario: %v", err)
	}
	for index := range scenario.Steps {
		if scenario.Steps[index].ID != "STEP-PERF-SCHEMA-CHECK-016" {
			continue
		}
		binding := *scenario.Steps[index].NativeBinding
		binding.Completion = "idle"
		scenario.Steps[index].NativeBinding = &binding
	}
	steps, err := swiftScenarioStepMap(scenario, schemaCheckScenarioID, 64)
	if err != nil {
		t.Fatalf("validate mutated schema-check scenario: %v", err)
	}
	if _, err := validateSchemaCheckBindings(scenario, steps); err == nil {
		t.Fatal("schema-check binding with an idle completion for unsupported wire action passed validation")
	}
}

func schemaCheckStep(scenario scenarios.Scenario, id scenarios.StepID) (scenarios.Step, bool) {
	for _, step := range scenario.Steps {
		if step.ID == id {
			return step, true
		}
	}
	return scenarios.Step{}, false
}

func TestSchemaCheckDispatchRejectsWrongActionAndOldCursor(t *testing.T) {
	scenario, err := scenarios.LoadFile(context.Background(), filepath.Join("..", ".."), "conformance/scenarios/performance/schema-check-001.json")
	if err != nil {
		t.Fatal(err)
	}
	step, found := schemaCheckStep(scenario, "STEP-PERF-SCHEMA-CHECK-007")
	if !found {
		t.Fatal("authored Class 2 step is absent")
	}
	wire, err := schemaCheckWireExpectation(scenario, step.ID)
	if err != nil {
		t.Fatal(err)
	}
	source := schemaRef{Version: 1, Hash: strings.Repeat("a", 64)}
	target := schemaRef{Version: 2, Hash: strings.Repeat("b", 64)}
	scope, oldCursor := "runtime-scope", "old-cursor"
	issued, finalCursor := cursorFingerprint("issued-replacement"), "later-pull-cursor"
	complete := true
	notTruncated := false
	newCall := func() SynchronizationResult {
		action := wire.Action
		before := runnerResult{Schema: &source, ScopeStates: []scopeStateRecord{{ScopeID: scope, Cursor: &oldCursor}}, Events: []eventRecord{}, TransportObservations: &transportObservationSnapshot{SequenceCheckpoint: 4}, MigrationJournal: []byte("null"), MigrationJournalTruncated: &notTruncated, PhysicalSchema: []byte("[]"), PhysicalSchemaTruncated: &notTruncated, CaptureOverflowed: &notTruncated}
		after := runnerResult{Schema: &target, ScopeStates: []scopeStateRecord{{ScopeID: scope, Cursor: &finalCursor}}, TransportObservations: &transportObservationSnapshot{SequenceCheckpoint: 6}, MigrationJournal: []byte("null"), MigrationJournalTruncated: &notTruncated, PhysicalSchema: []byte("[]"), PhysicalSchemaTruncated: &notTruncated, CaptureOverflowed: &notTruncated, Events: []eventRecord{
			{Type: "schema_applying", SourceSchema: &source, TargetSchema: &target, SchemaAction: &action},
			{Type: "schema_applied", SourceSchema: &source, TargetSchema: &target, SchemaAction: &action},
		}}
		return SynchronizationResult{Completion: "idle", before: &before, after: &after, transportObservations: []transportObservation{
			{Sequence: 5, OperationClass: "connect", StatusCode: 200, DurationNanoseconds: 1, CursorFingerprints: []string{cursorFingerprint(oldCursor)}, CursorFingerprintsComplete: &complete,
				RequestFacts:         &transportRequestFacts{SchemaVersion: source.Version, SchemaHash: source.Hash},
				ConnectResponseFacts: &transportConnectResponseFacts{Action: action, SchemaVersion: target.Version, SchemaHash: target.Hash, AffectedScopeFingerprints: []string{}, AffectedScopesComplete: true, ScopeCursorUpdates: map[string]*string{cursorFingerprint(scope): &issued}, ScopeCursorUpdatesComplete: true}},
			{Sequence: 6, OperationClass: "pull", StatusCode: 200, DurationNanoseconds: 1, CursorFingerprints: []string{issued}, CursorFingerprintsComplete: &complete, RequestFacts: &transportRequestFacts{SchemaVersion: target.Version, SchemaHash: target.Hash}},
		}}
	}
	if err := validateSchemaCheckDispatch(step, wire.Action, newCall(), target, scope, false); err != nil {
		t.Fatalf("valid replacement rejected: %v", err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*SynchronizationResult)
	}{
		{"wrong action", func(call *SynchronizationResult) { call.transportObservations[0].ConnectResponseFacts.Action = "none" }},
		{"old cursor on target pull", func(call *SynchronizationResult) {
			call.transportObservations[1].CursorFingerprints = []string{cursorFingerprint(oldCursor)}
		}},
		{"old token relabeled as replacement", func(call *SynchronizationResult) {
			old := cursorFingerprint(oldCursor)
			call.transportObservations[0].ConnectResponseFacts.ScopeCursorUpdates[cursorFingerprint(scope)] = &old
		}},
		{"incomplete response", func(call *SynchronizationResult) {
			call.transportObservations[0].ConnectResponseFacts.ScopeCursorUpdatesComplete = false
		}},
		{"wrong event source", func(call *SynchronizationResult) { call.after.Events[0].SourceSchema = &target }},
		{"transport overflow", func(call *SynchronizationResult) { call.after.TransportObservations.Overflowed = true }},
		{"missing migration journal", func(call *SynchronizationResult) { call.before.MigrationJournal = nil }},
		{"truncated migration journal", func(call *SynchronizationResult) { call.before.MigrationJournalTruncated = &complete }},
		{"truncated physical schema", func(call *SynchronizationResult) { call.after.PhysicalSchemaTruncated = &complete }},
		{"overall captured-state overflow", func(call *SynchronizationResult) { call.after.CaptureOverflowed = &complete }},
		{"missing aggregate overflow", func(call *SynchronizationResult) { call.before.CaptureOverflowed = nil }},
		{"unrelated scope rows truncated", func(call *SynchronizationResult) { call.after.ScopeRowsTruncated = &complete }},
		{"full event ring", func(call *SynchronizationResult) {
			for len(call.after.Events) < maximumRunnerRecords {
				call.after.Events = append(call.after.Events, eventRecord{Type: "state_changed"})
			}
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
	call := newCall()
	fingerprint := cursorFingerprint(scope)
	call.transportObservations = append(call.transportObservations, transportObservation{Sequence: 7, OperationClass: "rebuild", StatusCode: 200, RequestFacts: &transportRequestFacts{SchemaVersion: target.Version, SchemaHash: target.Hash, ScopeFingerprint: &fingerprint}})
	call.after.TransportObservations.SequenceCheckpoint = 7
	if err := validateSchemaCheckDispatch(step, wire.Action, call, target, scope, false); err == nil || !strings.Contains(err.Error(), "unnecessary rebuild") {
		t.Fatalf("unaffected rebuild error=%v", err)
	}
	call = newCall()
	call.after.Schema = &source
	call.after.Events = []eventRecord{}
	call.transportObservations[0].ConnectResponseFacts = &transportConnectResponseFacts{Action: "none", SchemaVersion: source.Version, SchemaHash: source.Hash, AffectedScopeFingerprints: []string{}, AffectedScopesComplete: true, ScopeCursorUpdates: map[string]*string{}, ScopeCursorUpdatesComplete: true}
	call.transportObservations[1].RequestFacts = &transportRequestFacts{SchemaVersion: source.Version, SchemaHash: source.Hash}
	call.transportObservations[1].CursorFingerprints = []string{cursorFingerprint(oldCursor)}
	call.transportObservations = append(call.transportObservations, transportObservation{Sequence: 7, OperationClass: "rebuild", StatusCode: 200, RequestFacts: &transportRequestFacts{SchemaVersion: source.Version, SchemaHash: source.Hash, ScopeFingerprint: &fingerprint}, RebuildResponseFacts: &transportRebuildResponseFacts{ScopeFingerprint: fingerprint, HasFinalScopeCursor: true, FinalScopeCursorFingerprint: &issued}})
	call.after.TransportObservations.SequenceCheckpoint = 7
	membershipStep := step
	measurement := *step.MeasurementSample
	measurement.Parameters = []byte(`{"schema_case":"class_1"}`)
	membershipStep.MeasurementSample = &measurement
	if err := validateSchemaCheckDispatch(membershipStep, "none", call, source, scope, false); err != nil {
		t.Fatalf("Class 1 membership recovery rejected: %v", err)
	}
}
