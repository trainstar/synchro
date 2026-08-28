package blackbox

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestResolveNativeIdentityAliasesRequiresCompleteConsistentEvidence(t *testing.T) {
	stepA := scenarios.StepID("STEP-A")
	stepB := scenarios.StepID("STEP-B")
	expectationA := scenarios.ExpectationID("EXPECT-A")
	aliases := []scenarios.NativeIdentityAlias{
		{Kind: "scope", Alias: "scope-a", Value: json.RawMessage(`"authored-a"`), StepIDs: []scenarios.StepID{stepA}, ExpectationIDs: []scenarios.ExpectationID{expectationA}},
		{Kind: "scope", Alias: "scope-b", Value: json.RawMessage(`"authored-b"`), StepIDs: []scenarios.StepID{stepB}},
		{Kind: "checksum", Alias: "checksum-a", Value: json.RawMessage(`"authored-checksum"`), ExpectationIDs: []scenarios.ExpectationID{expectationA}},
	}
	valid := []NativeIdentityObservation{
		{Kind: "scope", Alias: "scope-a", StepID: &stepA, RuntimeValue: json.RawMessage(`"runtime-a"`)},
		{Kind: "scope", Alias: "scope-a", ExpectationID: &expectationA, RuntimeValue: json.RawMessage(`"runtime-a"`)},
		{Kind: "scope", Alias: "scope-b", StepID: &stepB, RuntimeValue: json.RawMessage(`"runtime-b"`)},
		{Kind: "checksum", Alias: "checksum-a", ExpectationID: &expectationA, RuntimeValue: json.RawMessage(`"runtime-checksum"`)},
	}
	resolutions, err := ResolveNativeIdentityAliases(aliases, valid)
	if err != nil {
		t.Fatalf("resolve valid identity evidence: %v", err)
	}
	if len(resolutions) != len(aliases) || resolutions[0].Alias != "scope-a" || string(resolutions[0].AuthoredValue) != `"authored-a"` || string(resolutions[0].RuntimeValue) != `"runtime-a"` {
		t.Fatalf("identity resolutions = %#v", resolutions)
	}

	tests := []struct {
		name         string
		observations []NativeIdentityObservation
	}{
		{
			name: "unresolved alias",
			observations: append(append([]NativeIdentityObservation(nil), valid...),
				NativeIdentityObservation{Kind: "scope", Alias: "scope-c", StepID: &stepA, RuntimeValue: json.RawMessage(`"runtime-c"`)}),
		},
		{
			name:         "missing runtime observation",
			observations: valid[:3],
		},
		{
			name: "inconsistent equal alias",
			observations: []NativeIdentityObservation{
				{Kind: "scope", Alias: "scope-a", StepID: &stepA, RuntimeValue: json.RawMessage(`"runtime-a"`)},
				{Kind: "scope", Alias: "scope-a", ExpectationID: &expectationA, RuntimeValue: json.RawMessage(`"changed"`)},
				{Kind: "scope", Alias: "scope-b", StepID: &stepB, RuntimeValue: json.RawMessage(`"runtime-b"`)},
				{Kind: "checksum", Alias: "checksum-a", ExpectationID: &expectationA, RuntimeValue: json.RawMessage(`"runtime-checksum"`)},
			},
		},
		{
			name: "collapsed distinct aliases",
			observations: []NativeIdentityObservation{
				{Kind: "scope", Alias: "scope-a", StepID: &stepA, RuntimeValue: json.RawMessage(`"collapsed"`)},
				{Kind: "scope", Alias: "scope-a", ExpectationID: &expectationA, RuntimeValue: json.RawMessage(`"collapsed"`)},
				{Kind: "scope", Alias: "scope-b", StepID: &stepB, RuntimeValue: json.RawMessage(`"collapsed"`)},
				{Kind: "checksum", Alias: "checksum-a", ExpectationID: &expectationA, RuntimeValue: json.RawMessage(`"runtime-checksum"`)},
			},
		},
		{
			name: "wrong kind",
			observations: []NativeIdentityObservation{
				{Kind: "table", Alias: "scope-a", StepID: &stepA, RuntimeValue: json.RawMessage(`"runtime-a"`)},
				{Kind: "scope", Alias: "scope-a", ExpectationID: &expectationA, RuntimeValue: json.RawMessage(`"runtime-a"`)},
				{Kind: "scope", Alias: "scope-b", StepID: &stepB, RuntimeValue: json.RawMessage(`"runtime-b"`)},
				{Kind: "checksum", Alias: "checksum-a", ExpectationID: &expectationA, RuntimeValue: json.RawMessage(`"runtime-checksum"`)},
			},
		},
		{
			name: "ownerless observation",
			observations: []NativeIdentityObservation{
				{Kind: "scope", Alias: "scope-a", RuntimeValue: json.RawMessage(`"runtime-a"`)},
				{Kind: "scope", Alias: "scope-b", StepID: &stepB, RuntimeValue: json.RawMessage(`"runtime-b"`)},
				{Kind: "checksum", Alias: "checksum-a", ExpectationID: &expectationA, RuntimeValue: json.RawMessage(`"runtime-checksum"`)},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ResolveNativeIdentityAliases(aliases, test.observations); !errors.Is(err, ErrNativeIdentityEvidence) {
				t.Fatalf("resolution error = %v, want native identity evidence error", err)
			}
		})
	}
}

func TestResolveNativeIdentityAliasesRejectsUnknownKindsAndMalformedSchemas(t *testing.T) {
	stepID := scenarios.StepID("STEP-A")
	unknown := []scenarios.NativeIdentityAlias{{Kind: "unknown", Alias: "value-a", Value: json.RawMessage(`"authored"`), StepIDs: []scenarios.StepID{stepID}}}
	if _, err := ResolveNativeIdentityAliases(unknown, nil); !errors.Is(err, ErrNativeIdentityEvidence) {
		t.Fatalf("unknown identity kind error = %v", err)
	}

	malformed := []scenarios.NativeIdentityAlias{{Kind: "schema", Alias: "schema-a", Value: json.RawMessage(`{"version":1}`), StepIDs: []scenarios.StepID{stepID}}}
	if _, err := ResolveNativeIdentityAliases(malformed, nil); !errors.Is(err, ErrNativeIdentityEvidence) {
		t.Fatalf("malformed authored schema error = %v", err)
	}

	valid := []scenarios.NativeIdentityAlias{{Kind: "schema", Alias: "schema-a", Value: json.RawMessage(`{"version":1,"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`), StepIDs: []scenarios.StepID{stepID}}}
	observed := []NativeIdentityObservation{{Kind: "schema", Alias: "schema-a", StepID: &stepID, RuntimeValue: json.RawMessage(`{"version":2,"hash":"short"}`)}}
	if _, err := ResolveNativeIdentityAliases(valid, observed); !errors.Is(err, ErrNativeIdentityEvidence) {
		t.Fatalf("malformed runtime schema error = %v", err)
	}

	duplicate := []scenarios.NativeIdentityAlias{{Kind: "schema", Alias: "schema-a", Value: json.RawMessage(`{"version":1,"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}`), StepIDs: []scenarios.StepID{stepID}}}
	if _, err := ResolveNativeIdentityAliases(duplicate, nil); !errors.Is(err, ErrNativeIdentityEvidence) {
		t.Fatalf("duplicate schema member error = %v", err)
	}
}

func TestResolveNativeIdentityAliasesRejectsWrongScalarShapes(t *testing.T) {
	stepID := scenarios.StepID("STEP-A")
	tests := []scenarios.NativeIdentityAlias{
		{Kind: "client-generation", Alias: "generation-a", Value: json.RawMessage(`"1"`), StepIDs: []scenarios.StepID{stepID}},
		{Kind: "scope-set-version", Alias: "scope-set-a", Value: json.RawMessage(`-1`), StepIDs: []scenarios.StepID{stepID}},
		{Kind: "checksum", Alias: "checksum-a", Value: json.RawMessage(`{"value":"checksum"}`), StepIDs: []scenarios.StepID{stepID}},
		{Kind: "scope", Alias: "scope-a", Value: json.RawMessage(`""`), StepIDs: []scenarios.StepID{stepID}},
	}
	for _, alias := range tests {
		if _, err := ResolveNativeIdentityAliases([]scenarios.NativeIdentityAlias{alias}, nil); !errors.Is(err, ErrNativeIdentityEvidence) {
			t.Fatalf("%s shape error = %v", alias.Kind, err)
		}
	}
}

func TestNativeControllerIdentityValuesExposeRuntimeSemanticHandles(t *testing.T) {
	authoredSchema := nativeSchemaReference{Version: 1, Hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	runtimeSchema := nativeSchemaReference{Version: 7, Hash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
	controller := &NativeController{
		installation: &nativeInstallationBinding{
			runtimeSchemas: map[string]nativeSchemaReference{nativeSchemaKey(authoredSchema): runtimeSchema},
			tables: map[string]nativeTableBinding{
				"items": {
					AuthoredID:      "items",
					RuntimeID:       "00000000-0000-4000-8000-000000000002",
					RuntimeName:     "cf_items",
					AuthoredPrimary: "id",
					RuntimePrimary:  "00000000-0000-4000-8000-000000000001",
					FieldNames:      map[string]string{"id": "cf_id"},
				},
			},
			scopes: map[string]string{"scope-a": "cf:global"},
		},
		records: map[string]*nativeRecordBinding{
			"items\x00\"row-a\"": {
				Table: nativeTableBinding{
					AuthoredPrimary: "id",
					FieldNames:      map[string]string{"id": "cf_id"},
				},
				RecordID:        "row-a",
				RuntimeRecordID: "00000000-0000-4000-8000-000000000003",
			},
		},
		transactions: map[string]*nativeTransactionBinding{
			"stream-a\x0010": {
				AuthoredBatchID:     "00000000-0000-4000-8000-000000000010",
				AuthoredMutationIDs: []string{"00000000-0000-4000-8000-000000000011"},
				RuntimeBatchID:      "00000000-0000-4000-8000-000000000012",
				RuntimeMutationIDs:  []string{"00000000-0000-4000-8000-000000000013"},
			},
		},
	}
	aliases := []scenarios.NativeIdentityAlias{
		{Kind: "schema", Alias: "schema-a", Value: json.RawMessage(`{"version":1,"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`)},
		{Kind: "table", Alias: "table-a", Value: json.RawMessage(`"items"`)},
		{Kind: "primary-key", Alias: "primary-a", Value: json.RawMessage(`"id"`)},
		{Kind: "primary-key", Alias: "record-a", Value: json.RawMessage(`"row-a"`)},
		{Kind: "scope", Alias: "scope-a", Value: json.RawMessage(`"scope-a"`)},
		{Kind: "batch-id", Alias: "batch-a", Value: json.RawMessage(`"00000000-0000-4000-8000-000000000010"`)},
		{Kind: "mutation-id", Alias: "mutation-a", Value: json.RawMessage(`"00000000-0000-4000-8000-000000000011"`)},
		{Kind: "row-version", Alias: "version-a", Value: json.RawMessage(`"v1"`)},
	}

	values, err := controller.IdentityValues(aliases)
	if err != nil {
		t.Fatalf("resolve controller identities: %v", err)
	}
	want := []NativeIdentityValue{
		{Kind: "schema", Alias: "schema-a", RuntimeValue: json.RawMessage(`{"version":7,"hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}`)},
		{Kind: "table", Alias: "table-a", RuntimeValue: json.RawMessage(`"00000000-0000-4000-8000-000000000002"`), ApplicationIdentifier: "cf_items"},
		{Kind: "primary-key", Alias: "primary-a", RuntimeValue: json.RawMessage(`"00000000-0000-4000-8000-000000000001"`), ApplicationIdentifier: "cf_id"},
		{Kind: "primary-key", Alias: "record-a", RuntimeValue: json.RawMessage(`"00000000-0000-4000-8000-000000000003"`), ApplicationIdentifier: "cf_id"},
		{Kind: "scope", Alias: "scope-a", RuntimeValue: json.RawMessage(`"cf:global"`)},
		{Kind: "batch-id", Alias: "batch-a", RuntimeValue: json.RawMessage(`"00000000-0000-4000-8000-000000000012"`)},
		{Kind: "mutation-id", Alias: "mutation-a", RuntimeValue: json.RawMessage(`"00000000-0000-4000-8000-000000000013"`)},
	}
	if !reflect.DeepEqual(values, want) {
		t.Fatalf("controller identity values = %#v, want %#v", values, want)
	}
}

func TestNativeControllerIdentityValuesRejectMissingSemanticBinding(t *testing.T) {
	controller := &NativeController{installation: &nativeInstallationBinding{scopes: map[string]string{}}}
	_, err := controller.IdentityValues([]scenarios.NativeIdentityAlias{{
		Kind:  "scope",
		Alias: "scope-a",
		Value: json.RawMessage(`"scope-a"`),
	}})
	if !errors.Is(err, ErrNativeIdentityEvidence) {
		t.Fatalf("missing semantic binding error = %v", err)
	}
}

func TestNativeControllerIdentityValuesRejectAmbiguousPrimaryKeyBinding(t *testing.T) {
	controller := &NativeController{
		installation: &nativeInstallationBinding{tables: map[string]nativeTableBinding{
			"items": {
				AuthoredPrimary: "row-a",
				RuntimePrimary:  "00000000-0000-4000-8000-000000000001",
				FieldNames:      map[string]string{"row-a": "cf_id"},
			},
		}},
		records: map[string]*nativeRecordBinding{
			"items\x00\"row-a\"": {
				Table: nativeTableBinding{
					AuthoredPrimary: "row-a",
					FieldNames:      map[string]string{"row-a": "cf_id"},
				},
				RecordID:        "row-a",
				RuntimeRecordID: "00000000-0000-4000-8000-000000000002",
			},
		},
	}
	_, err := controller.IdentityValues([]scenarios.NativeIdentityAlias{{
		Kind:  "primary-key",
		Alias: "row-a",
		Value: json.RawMessage(`"row-a"`),
	}})
	if !errors.Is(err, ErrNativeIdentityEvidence) {
		t.Fatalf("ambiguous primary-key binding error = %v", err)
	}
}
