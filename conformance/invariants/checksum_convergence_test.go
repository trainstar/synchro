package invariants

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
	"github.com/trainstar/synchro/conformance/vectors"
)

const (
	checksumTableID       = "00000000-0000-4000-8000-000000000030"
	checksumPKFieldID     = "00000000-0000-4000-8000-000000000031"
	checksumValueFieldID  = "00000000-0000-4000-8000-000000000033"
	checksumServerVersion = "00000000-0000-4000-8000-000000000050"
	checksumSecondVersion = "00000000-0000-4000-8000-000000000051"
	checksumManifestJSON  = `{"schema_version":1,"schema_hash":"dec0f17c4a7ed5522fb5e135c896d61dc722feacbddfa52a69917043ce415c8b","parent_schema":null,"transition_class":"initial","compatibility_floor":1,"tables":[{"table_id":"00000000-0000-4000-8000-000000000030","relation_id":"00000000-0000-4000-8000-000000000034","name":"items","composition":"single_scope","primary_key_field_id":"00000000-0000-4000-8000-000000000031","lifecycle":{"created_at_field_id":null,"updated_at_field_id":null,"deleted_at_field_id":null},"fields":[{"field_id":"00000000-0000-4000-8000-000000000031","name":"id","type":"string","nullable":false,"writable":false},{"field_id":"00000000-0000-4000-8000-000000000033","name":"value","type":"string","nullable":false,"writable":true}],"indexes":[]}]}`
)

func TestCheckChecksumConvergenceAcceptsOneChangePullForPopulatedScope(t *testing.T) {
	observation := checksumConvergenceFixture(t)
	response := decodeFixtureObject(t, observation.WireExchanges[0].ResponseBody)
	if len(response["changes"].([]any)) != 1 || len(observation.Clients[0].ScopeRows) != 2 {
		t.Fatal("fixture must return one change for a populated two-row scope")
	}
	violations, err := CheckChecksumConvergence([]Observation{observation})
	assertNoViolations(t, violations, err)
}

func TestCheckChecksumConvergenceRejectsChangedRowOnlyScopeDigest(t *testing.T) {
	observation := checksumConvergenceFixture(t)
	changedRow := observation.Clients[0].Rows[0]
	changedDigest, err := vectors.RowDigest(*observation.Manifest, changedRow.TableID, changedRow.Row, changedRow.ServerVersion)
	if err != nil {
		t.Fatalf("compute authored changed-row digest: %v", err)
	}
	changedIdentity, err := vectors.RowIdentity(*observation.Manifest, changedRow.TableID, changedRow.Row.PK)
	if err != nil {
		t.Fatalf("compute authored changed-row identity: %v", err)
	}
	changedRowOnlyDigest, err := vectors.ScopeDigest(observation.Manifest.Hash(), "scope-authored", []vectors.DigestEntry{{
		RowIdentity: changedIdentity,
		RowDigest:   changedDigest,
	}})
	if err != nil {
		t.Fatalf("compute authored changed-row-only scope digest: %v", err)
	}
	response := decodeFixtureObject(t, observation.WireExchanges[0].ResponseBody)
	response["checksums"].(map[string]any)["scope-authored"] = checksumFixture(hex.EncodeToString(changedRowOnlyDigest[:]))
	observation.WireExchanges[0].ResponseBody = marshalFixture(t, response)

	violations, err := CheckChecksumConvergence([]Observation{observation})
	assertCaughtRule(t, violations, err, RuleChecksumWireScopeDigestMismatch)
}

func TestCheckChecksumConvergenceCatchesEachRule(t *testing.T) {
	tests := []struct {
		name   string
		ruleID RuleID
		mutate func(*testing.T, *Observation)
	}{
		{name: "unexpected status", ruleID: RuleChecksumUnexpectedStatus, mutate: func(_ *testing.T, observation *Observation) {
			observation.WireExchanges[0].ResponseStatus = 503
		}},
		{name: "wire shape", ruleID: RuleChecksumWireShapeInvalid, mutate: func(t *testing.T, observation *Observation) {
			setFixturePullMember(t, &observation.WireExchanges[0], "changes", []any{})
		}},
		{name: "wire checksum metadata", ruleID: RuleChecksumWireMetadataInvalid, mutate: func(t *testing.T, observation *Observation) {
			response := decodeFixtureObject(t, observation.WireExchanges[0].ResponseBody)
			change := response["changes"].([]any)[0].(map[string]any)
			change["row_checksum"].(map[string]any)["algorithm"] = "sha1"
			observation.WireExchanges[0].ResponseBody = marshalFixture(t, response)
		}},
		{name: "wire row digest", ruleID: RuleChecksumWireRowDigestMismatch, mutate: func(t *testing.T, observation *Observation) {
			response := decodeFixtureObject(t, observation.WireExchanges[0].ResponseBody)
			change := response["changes"].([]any)[0].(map[string]any)
			change["row_checksum"].(map[string]any)["digest"] = strings.Repeat("0", 64)
			observation.WireExchanges[0].ResponseBody = marshalFixture(t, response)
		}},
		{name: "wire scope digest", ruleID: RuleChecksumWireScopeDigestMismatch, mutate: func(t *testing.T, observation *Observation) {
			response := decodeFixtureObject(t, observation.WireExchanges[0].ResponseBody)
			response["checksums"].(map[string]any)["scope-authored"].(map[string]any)["digest"] = strings.Repeat("0", 64)
			observation.WireExchanges[0].ResponseBody = marshalFixture(t, response)
		}},
		{name: "wire scope rows incomplete", ruleID: RuleChecksumWireScopeRowsIncomplete, mutate: func(_ *testing.T, observation *Observation) {
			observation.Clients[0].Complete = false
		}},
		{name: "manifest missing", ruleID: RuleChecksumManifestMissing, mutate: func(_ *testing.T, observation *Observation) {
			observation.Manifest = nil
			observation.WireExchanges = nil
		}},
		{name: "row input invalid", ruleID: RuleChecksumRowInputInvalid, mutate: func(_ *testing.T, observation *Observation) {
			observation.Clients[0].Rows[0].ServerVersion = ""
			observation.Clients[0].Scopes = nil
			observation.Clients[0].ScopeRows = nil
			observation.Clients[0].State.Checkpoints = nil
		}},
		{name: "row digest missing", ruleID: RuleChecksumRowDigestMissing, mutate: func(_ *testing.T, observation *Observation) {
			observation.Clients[0].Rows[0].StoredDigest = nil
		}},
		{name: "row digest mismatch", ruleID: RuleChecksumRowDigestMismatch, mutate: func(_ *testing.T, observation *Observation) {
			flipFixtureDigest(observation.Clients[0].Rows[0].StoredDigest)
		}},
		{name: "row identity duplicate", ruleID: RuleChecksumRowIdentityDuplicate, mutate: func(_ *testing.T, observation *Observation) {
			observation.Clients[0].Rows = append(observation.Clients[0].Rows, observation.Clients[0].Rows[0])
		}},
		{name: "scope row digest mismatch", ruleID: RuleChecksumScopeRowDigestMismatch, mutate: func(_ *testing.T, observation *Observation) {
			observation.Clients[0].ScopeRows[0].Entry.RowDigest[0] ^= 0xff
		}},
		{name: "scope input invalid", ruleID: RuleChecksumScopeInputInvalid, mutate: func(_ *testing.T, observation *Observation) {
			observation.Clients[0].ScopeRows = append(observation.Clients[0].ScopeRows, observation.Clients[0].ScopeRows[0])
		}},
		{name: "authoritative digest missing", ruleID: RuleChecksumAuthoritativeDigestMissing, mutate: func(_ *testing.T, observation *Observation) {
			observation.Clients[0].Scopes[0].AuthoritativeDigest = nil
		}},
		{name: "authoritative digest mismatch", ruleID: RuleChecksumAuthoritativeDigestMismatch, mutate: func(_ *testing.T, observation *Observation) {
			flipFixtureDigest(observation.Clients[0].Scopes[0].AuthoritativeDigest)
		}},
		{name: "local digest missing", ruleID: RuleChecksumLocalDigestMissing, mutate: func(_ *testing.T, observation *Observation) {
			observation.Clients[0].Scopes[0].LocalDigest = nil
		}},
		{name: "local digest mismatch", ruleID: RuleChecksumLocalDigestMismatch, mutate: func(_ *testing.T, observation *Observation) {
			flipFixtureDigest(observation.Clients[0].Scopes[0].LocalDigest)
		}},
		{name: "scope observation missing", ruleID: RuleChecksumScopeObservationMissing, mutate: func(_ *testing.T, observation *Observation) {
			observation.Clients[0].Scopes = nil
		}},
		{name: "scope row unknown", ruleID: RuleChecksumScopeRowUnknown, mutate: func(t *testing.T, observation *Observation) {
			observation.Clients[0].ScopeRows[0].Entry.RowIdentity = []byte("unknown-authored-row")
			remainingDigest, err := vectors.ScopeDigest(
				observation.Manifest.Hash(),
				"scope-authored",
				[]vectors.DigestEntry{observation.Clients[0].ScopeRows[1].Entry},
			)
			if err != nil {
				t.Fatalf("compute authored remaining scope digest: %v", err)
			}
			observation.Clients[0].Scopes[0].AuthoritativeDigest = &remainingDigest
			localDigest := remainingDigest
			observation.Clients[0].Scopes[0].LocalDigest = &localDigest
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observation := checksumConvergenceFixture(t)
			if !checksumWireRule(test.ruleID) {
				observation.WireExchanges = nil
			}
			test.mutate(t, &observation)
			violations, err := CheckChecksumConvergence([]Observation{observation})
			assertCaughtRule(t, violations, err, test.ruleID)
		})
	}
}

func checksumWireRule(ruleID RuleID) bool {
	switch ruleID {
	case RuleChecksumUnexpectedStatus,
		RuleChecksumWireShapeInvalid,
		RuleChecksumWireMetadataInvalid,
		RuleChecksumWireRowDigestMismatch,
		RuleChecksumWireScopeDigestMismatch,
		RuleChecksumWireScopeRowsIncomplete:
		return true
	default:
		return false
	}
}

func checksumConvergenceFixture(t *testing.T) Observation {
	t.Helper()
	manifest, tableID, primaryKeyFieldID, row, serverVersion := checksumFixtureInput(t)
	rowDigest, err := vectors.RowDigest(manifest, tableID, row, serverVersion)
	if err != nil {
		t.Fatalf("compute authored row digest: %v", err)
	}
	identity, err := vectors.RowIdentity(manifest, tableID, row.PK)
	if err != nil {
		t.Fatalf("compute authored row identity: %v", err)
	}
	entry := vectors.DigestEntry{RowIdentity: identity, RowDigest: rowDigest}
	secondRow := vectors.Row{
		PK: json.RawMessage(`"row-existing"`),
		Fields: []vectors.RowField{
			{FieldID: checksumPKFieldID, Value: json.RawMessage(`"row-existing"`)},
			{FieldID: checksumValueFieldID, Value: json.RawMessage(`"value-existing"`)},
		},
	}
	secondRowDigest, err := vectors.RowDigest(manifest, tableID, secondRow, checksumSecondVersion)
	if err != nil {
		t.Fatalf("compute authored existing-row digest: %v", err)
	}
	secondIdentity, err := vectors.RowIdentity(manifest, tableID, secondRow.PK)
	if err != nil {
		t.Fatalf("compute authored existing-row identity: %v", err)
	}
	secondEntry := vectors.DigestEntry{RowIdentity: secondIdentity, RowDigest: secondRowDigest}
	scopeDigest, err := vectors.ScopeDigest(manifest.Hash(), "scope-authored", []vectors.DigestEntry{entry, secondEntry})
	if err != nil {
		t.Fatalf("compute authored scope digest: %v", err)
	}
	authoritativeDigest := scopeDigest
	localDigest := scopeDigest
	rowObject := make(map[string]any, len(row.Fields))
	for _, field := range row.Fields {
		var value any
		if err := json.Unmarshal(field.Value, &value); err != nil {
			t.Fatalf("decode authored row field: %v", err)
		}
		rowObject[field.FieldID] = value
	}
	var primaryKey any
	if err := json.Unmarshal(row.PK, &primaryKey); err != nil {
		t.Fatalf("decode authored primary key: %v", err)
	}
	request := map[string]any{
		"client_id": "client-authored",
		"scopes":    map[string]any{"scope-authored": map[string]any{"cursor": "cursor-authored"}},
	}
	response := map[string]any{
		"changes": []any{map[string]any{
			"scope": "scope-authored", "table": tableID,
			"pk": map[string]any{primaryKeyFieldID: primaryKey}, "row": rowObject,
			"server_version": serverVersion, "row_checksum": checksumFixture(hex.EncodeToString(rowDigest[:])),
		}},
		"scope_cursors": map[string]any{"scope-authored": "cursor-terminal"},
		"scope_updates": map[string]any{"add": []any{}, "remove": []any{}},
		"rebuild":       []any{}, "has_more": false,
		"checksums": map[string]any{"scope-authored": checksumFixture(hex.EncodeToString(scopeDigest[:]))},
	}
	return Observation{
		Sequence: 21,
		Manifest: &manifest,
		Clients: []ClientObservation{{
			State: scenarios.ClientDurabilityFact{
				UserID: "user-authored", ClientID: "client-authored",
				Checkpoints: []scenarios.CheckpointFact{{ScopeID: "scope-authored", HasCursor: true, HasChecksum: true, Verified: true}},
			},
			Rows: []ClientRowObservation{
				{TableID: tableID, Row: row, ServerVersion: serverVersion, StoredDigest: &rowDigest},
				{TableID: tableID, Row: secondRow, ServerVersion: checksumSecondVersion, StoredDigest: &secondRowDigest},
			},
			Scopes: []ClientScopeObservation{{
				ScopeID: "scope-authored", AuthoritativeDigest: &authoritativeDigest, LocalDigest: &localDigest, Generation: 4,
			}},
			ScopeRows: []ClientScopeRowObservation{
				{ScopeID: "scope-authored", Entry: entry, Generation: 4},
				{ScopeID: "scope-authored", Entry: secondEntry, Generation: 4},
			},
			Complete: true,
		}},
		WireExchanges: []WireExchangeObservation{{
			Sequence: 1, OperationClass: "pull", RequestBody: marshalFixture(t, request), ResponseStatus: 200,
			ResponseBody: marshalFixture(t, response), ExpectChecksumConvergence: true,
		}},
	}
}

func checksumFixtureInput(t *testing.T) (vectors.Manifest, string, string, vectors.Row, string) {
	t.Helper()
	manifest, err := vectors.ParseManifest(json.RawMessage(checksumManifestJSON))
	if err != nil {
		t.Fatalf("parse authored manifest: %v", err)
	}
	row := vectors.Row{
		PK: json.RawMessage(`"row-authored"`),
		Fields: []vectors.RowField{
			{FieldID: checksumPKFieldID, Value: json.RawMessage(`"row-authored"`)},
			{FieldID: checksumValueFieldID, Value: json.RawMessage(`"value-authored"`)},
		},
	}
	return manifest, checksumTableID, checksumPKFieldID, row, checksumServerVersion
}

func flipFixtureDigest(digest *[32]byte) {
	digest[0] ^= 0xff
}
