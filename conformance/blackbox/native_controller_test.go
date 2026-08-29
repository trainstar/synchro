package blackbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestNativeControllerApplyRejectsWorkloadMacro(t *testing.T) {
	controller := &NativeController{harness: &Harness{}}
	operation := scenarios.Operation{
		ContractOperation: "workload",
		Name:              "prepare",
		Payload:           []byte(`{"profile":"scope_topology","scope_fanout":1,"impact_rows":1}`),
	}
	_, err := controller.ApplyStep(context.Background(), operation)
	if err == nil || !strings.Contains(err.Error(), "does not execute workload macros") {
		t.Fatalf("ApplyStep error = %v, want workload macro boundary error", err)
	}
}

func TestNativeControllerAssignmentPreservesSharedScope(t *testing.T) {
	controller := &NativeController{installation: &nativeInstallationBinding{
		scopes:        map[string]string{"scope-a": "cf:global"},
		runtimeScopes: map[string]string{"cf:global": "scope-a"},
	}}
	operation := scenarios.Operation{
		ContractOperation: "model",
		Name:              "set-client-assignments",
		Payload:           json.RawMessage(`{"user_id":"user-a","client_id":"client-a","assignments":[{"scope_id":"scope-a"}]}`),
	}
	for range 2 {
		observation, usesDefaultSharedScope, err := controller.setClientAssignments(operation)
		if err != nil {
			t.Fatalf("set client assignments: %v", err)
		}
		if observation.Disposition != "success" {
			t.Fatalf("assignment disposition = %q", observation.Disposition)
		}
		if !usesDefaultSharedScope {
			t.Fatal("shared assignment did not retain the default shared scope")
		}
	}
	if got := controller.installation.scopes["scope-a"]; got != "cf:global" {
		t.Fatalf("runtime scope = %q, want cf:global", got)
	}
	if got := controller.installation.runtimeScopes["cf:global"]; got != "scope-a" {
		t.Fatalf("reverse runtime scope = %q, want scope-a", got)
	}
	if len(controller.installation.clients) != 1 || controller.installation.clients[0] != (nativeInstalledClient{UserID: "user-a", ClientID: "client-a"}) {
		t.Fatalf("installed clients = %#v", controller.installation.clients)
	}
}

func TestNativeControllerAssignmentBindsUnresolvedPrivateScope(t *testing.T) {
	controller := &NativeController{installation: &nativeInstallationBinding{
		scopes:        map[string]string{},
		runtimeScopes: map[string]string{},
	}}
	operation := scenarios.Operation{
		ContractOperation: "model",
		Name:              "set-client-assignments",
		Payload:           json.RawMessage(`{"user_id":"user-a","client_id":"client-a","assignments":[{"scope_id":"scope-a"}]}`),
	}

	observation, usesDefaultSharedScope, err := controller.setClientAssignments(operation)
	if err != nil {
		t.Fatalf("set client assignments: %v", err)
	}
	if observation.Disposition != "success" || usesDefaultSharedScope {
		t.Fatalf("private assignment result = %#v, shared = %t", observation, usesDefaultSharedScope)
	}
	if got := controller.installation.scopes["scope-a"]; got != "user:user-a" {
		t.Fatalf("runtime scope = %q, want user:user-a", got)
	}
}

func TestNativeControllerAssignmentDoesNotStageSharedScope(t *testing.T) {
	controller := &NativeController{installation: &nativeInstallationBinding{
		scopes:        map[string]string{},
		runtimeScopes: map[string]string{},
	}}
	operation := scenarios.Operation{
		ContractOperation: "model",
		Name:              "set-client-assignments",
		Payload:           json.RawMessage(`{"user_id":"user-a","client_id":"client-a","assignments":[{"scope_id":"scope-b"}]}`),
	}

	_, retainsSharedScope, err := controller.setClientAssignments(operation)
	if err != nil {
		t.Fatalf("set client assignments: %v", err)
	}
	if retainsSharedScope {
		t.Fatal("un-staged assignment retained a shared scope")
	}
	if got := controller.installation.scopes[nativeStagedSharedAuthoredScope]; got != "user:user-a" {
		t.Fatalf("runtime scope = %q, want user:user-a", got)
	}
}

func TestNativeControllerAssignmentBindsStagedSharedScope(t *testing.T) {
	controller := &NativeController{installation: &nativeInstallationBinding{
		scopes:        map[string]string{nativeStagedSharedAuthoredScope: nativeStagedSharedRuntimeScope},
		runtimeScopes: map[string]string{nativeStagedSharedRuntimeScope: nativeStagedSharedAuthoredScope},
	}}
	operation := scenarios.Operation{
		ContractOperation: "model",
		Name:              "set-client-assignments",
		Payload:           json.RawMessage(`{"user_id":"user-a","client_id":"client-a","assignments":[{"scope_id":"scope-b"}]}`),
	}

	_, retainsSharedScope, err := controller.setClientAssignments(operation)
	if err != nil {
		t.Fatalf("set client assignments: %v", err)
	}
	if !retainsSharedScope {
		t.Fatal("staged shared assignment did not retain shared scopes")
	}
}

func TestNativeInstallDetectsAuthoredPrivateAssignments(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    bool
	}{
		{name: "assigned", payload: `{"clients":[{"user_id":"user-a","client_id":"client-a","assigned_scope_ids":["scope-a"]}]}`, want: true},
		{name: "staged shared", payload: `{"clients":[{"user_id":"user-a","client_id":"client-a","assigned_scope_ids":["scope-b"]}]}`, want: true},
		{name: "unassigned", payload: `{"clients":[{"user_id":"user-a","client_id":"client-a","assigned_scope_ids":[]}]}`},
		{name: "no clients", payload: `{"clients":[]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var payload nativeInstallPayload
			if err := json.Unmarshal([]byte(test.payload), &payload); err != nil {
				t.Fatalf("decode install payload: %v", err)
			}
			if got := nativeInstallRequiresPrivateScopeAssignments(payload); got != test.want {
				t.Fatalf("private assignment detection = %t, want %t", got, test.want)
			}
		})
	}
}

func TestNativeStageRegistersStagedSharedScope(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    bool
	}{
		{name: "private only", payload: `{"affected_scopes":["scope-a"]}`},
		{name: "shared only", payload: `{"affected_scopes":["scope-b"]}`},
		{name: "staged shared", payload: `{"affected_scopes":["scope-a","scope-b"]}`, want: true},
		{name: "additional scope", payload: `{"affected_scopes":["scope-a","scope-b","scope-c"]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := nativeStageRegistersSharedScope(scenarios.Operation{Payload: json.RawMessage(test.payload)})
			if err != nil {
				t.Fatalf("select staged shared registration: %v", err)
			}
			if got != test.want {
				t.Fatalf("staged shared registration = %t, want %t", got, test.want)
			}
		})
	}
}

func TestNativeInstallationBindsDeferredMembershipScopesOnlyWithPolicy(t *testing.T) {
	payload := nativeInstallPayload{}
	payload.InitialSchema.Schema = nativeSchemaReference{Version: 1, Hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	payload.InitialSchema.Tables = []nativeAuthoredTable{{
		TableID:           "items",
		RelationID:        "public.items",
		Name:              "items",
		PrimaryKeyFieldID: "id",
		Fields: []nativeAuthoredField{
			{FieldID: "id", Name: "id", Type: "string", PrimaryKey: true},
			{FieldID: "value", Name: "value", Type: "string", Writable: true},
		},
	}}
	payload.InitialRegistry.RegistryGeneration = 1
	payload.InitialRegistry.Relations = []nativeAuthoredRelation{{
		Relation:               "public.items",
		RegistrationKind:       "synced",
		TableID:                "items",
		PrimaryKeyFieldID:      "id",
		PrimaryKeyPortableType: "string",
	}, {
		Relation:           "public.item_impacts",
		RegistrationKind:   "capture_dependency",
		CaptureKeyFieldIDs: []string{"scope_key"},
		CapturedFieldIDs:   []string{"scope_key"},
	}}
	rule := nativeScopeRule{Relation: "public.items"}
	rule.Evaluations = append(rule.Evaluations, struct {
		Row struct {
			TableID           string `json:"table_id"`
			CanonicalWireJSON string `json:"canonical_wire_json"`
		} `json:"row"`
		Scopes []string `json:"scopes"`
	}{})
	rule.Evaluations[0].Row.TableID = "items"
	rule.Evaluations[0].Row.CanonicalWireJSON = `"row-a"`
	rule.Evaluations[0].Scopes = []string{"scope-a"}
	payload.InitialRegistry.ScopeRules = []nativeScopeRule{rule}
	payload.EmptyScopes = append(payload.EmptyScopes, struct {
		ScopeID string `json:"scope_id"`
	}{ScopeID: "scope-a"}, struct {
		ScopeID string `json:"scope_id"`
	}{ScopeID: "scope-c"}, struct {
		ScopeID string `json:"scope_id"`
	}{ScopeID: nativeStagedSharedAuthoredScope})
	runtime := nativeRuntimeManifest{
		SchemaVersion: 7,
		SchemaHash:    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Manifest: nativeRuntimeManifestBody{Tables: []nativeRuntimeManifestTable{{
			Name:              "cf_items",
			ID:                "00000000-0000-4000-8000-000000000002",
			RelationID:        "public.cf_items",
			PrimaryKeyFieldID: "00000000-0000-4000-8000-000000000003",
			Fields: []nativeRuntimeManifestField{
				{ID: "00000000-0000-4000-8000-000000000003", Name: "id", Type: "string"},
				{ID: "00000000-0000-4000-8000-000000000004", Name: "value", Type: "string", Writable: true},
			},
		}}},
	}

	binding, err := bindNativeInstallation(payload, runtime, 9)
	if err != nil {
		t.Fatalf("bind native installation: %v", err)
	}
	if len(binding.relations) != 1 || binding.relations["public.items"] != "items" || binding.captureDependencies["public.item_impacts"].RuntimeName != nativeCaptureDependencyFixture {
		t.Fatalf("registry bindings = %#v, %#v", binding.relations, binding.captureDependencies)
	}
	if _, found := binding.scopes["scope-a"]; found {
		t.Fatal("membership scope was bound before its assignment operation")
	}
	if got := binding.scopes["scope-c"]; got != "cf:global" {
		t.Fatalf("unreferenced empty scope = %q, want cf:global", got)
	}
	if got := binding.scopes[nativeStagedSharedAuthoredScope]; got != "user:scope-b" {
		t.Fatalf("second unreferenced empty scope = %q, want user:scope-b", got)
	}
	controller := &NativeController{installation: binding}
	if err := controller.bindStagedSharedScope(); err != nil {
		t.Fatalf("bind staged shared scope: %v", err)
	}
	if got := binding.scopes[nativeStagedSharedAuthoredScope]; got != nativeStagedSharedRuntimeScope {
		t.Fatalf("staged shared scope = %q, want %q", got, nativeStagedSharedRuntimeScope)
	}

	payload.WritePolicies = []nativeWritePolicy{{UserID: "user-a", TableID: "items", Allowed: true}}
	policyBinding, err := bindNativeInstallation(payload, runtime, 9)
	if err != nil {
		t.Fatalf("bind native policy installation: %v", err)
	}
	if got := policyBinding.scopes["scope-a"]; got != "user:user-a" {
		t.Fatalf("policy-bound membership scope = %q, want user:user-a", got)
	}
}

func TestNativeControllerApplicationWriteMapsInstalledRuntimeIdentities(t *testing.T) {
	controller := &NativeController{installation: &nativeInstallationBinding{tables: map[string]nativeTableBinding{
		"items": {
			AuthoredID:      "items",
			RuntimeName:     "cf_items",
			AuthoredPrimary: "item-id",
			FieldNames: map[string]string{
				"item-id": "id",
				"value":   "value",
			},
		},
	}}}
	operation := scenarios.Operation{
		ContractOperation: "local",
		Name:              "write",
		Payload:           json.RawMessage(`{"authenticated_user_id":"user-a","client_id":"client-a","mutation_id":"00000000-0000-4000-8000-000000000001","table_id":"items","pk":{"item-id":"row-a"},"authored_schema":{"version":1,"hash":"721d2c95e6f34cd9733feea9f5118fba391eee10d07663dad066cfc59439fa44"},"operation":"insert","client_version":"2026-08-11T00:00:00.000000Z","columns":{"value":"pending"}}`),
	}

	mapped, err := controller.ApplicationWrite(operation)
	if err != nil {
		t.Fatalf("map native application write: %v", err)
	}
	var payload struct {
		TableID string            `json:"table_id"`
		PK      map[string]string `json:"pk"`
		Columns map[string]string `json:"columns"`
	}
	if err := json.Unmarshal(mapped.Payload, &payload); err != nil {
		t.Fatalf("decode mapped application write: %v", err)
	}
	wantPrimary := nativeRuntimeUUID("items", `"row-a"`)
	if payload.TableID != "cf_items" || payload.PK["id"] != wantPrimary || payload.Columns["value"] != "pending" || payload.Columns["owner_id"] != "user-a" || payload.Columns["updated_at"] != "2026-08-11T00:00:00.000000Z" {
		t.Fatalf("mapped application write = %#v, want installed application identities", payload)
	}
	var authored struct {
		TableID string            `json:"table_id"`
		PK      map[string]string `json:"pk"`
	}
	if err := json.Unmarshal(operation.Payload, &authored); err != nil || authored.TableID != "items" || authored.PK["item-id"] != "row-a" {
		t.Fatalf("authored application write changed: %#v, %v", authored, err)
	}
	fieldValueOperation := operation
	fieldValueOperation.Payload = json.RawMessage(`{"authenticated_user_id":"user-a","client_id":"client-a","mutation_id":"00000000-0000-4000-8000-000000000002","table_id":"items","pk":{"field_id":"item-id","value":"row-b"},"authored_schema":{"version":1,"hash":"721d2c95e6f34cd9733feea9f5118fba391eee10d07663dad066cfc59439fa44"},"operation":"insert","client_version":"2026-08-11T00:00:00.000000Z","columns":[{"field_id":"value","value":"pending"}]}`)
	fieldValueMapped, err := controller.ApplicationWrite(fieldValueOperation)
	if err != nil {
		t.Fatalf("map field-value native application write: %v", err)
	}
	var fieldValuePayload struct {
		PK map[string]string `json:"pk"`
	}
	if err := json.Unmarshal(fieldValueMapped.Payload, &fieldValuePayload); err != nil || fieldValuePayload.PK["id"] != nativeRuntimeUUID("items", `"row-b"`) {
		t.Fatalf("mapped field-value application write = %#v, %v", fieldValuePayload, err)
	}
}

func TestNativeControllerMapsSchemaQueueApplicationWrite(t *testing.T) {
	authored := nativeAuthoredTable{
		TableID:           "items",
		Name:              "items",
		RelationID:        "public.items",
		PrimaryKeyFieldID: "id",
		Fields: []nativeAuthoredField{
			{FieldID: "id", Name: "id", Type: "string", PrimaryKey: true},
			{FieldID: "value", Name: "value", Type: "string"},
			{FieldID: "obsolete_value", Name: "obsolete_value", Type: "string"},
		},
	}
	runtime := nativeRuntimeManifestTable{
		ID:                "runtime-items",
		Name:              "cf_schema_queue",
		RelationID:        "runtime-relation",
		PrimaryKeyFieldID: "runtime-id",
		Fields: []nativeRuntimeManifestField{
			{ID: "runtime-id", Name: "id", Type: "string"},
			{ID: "runtime-value", Name: "authored_mutation", Type: "json"},
			{ID: "runtime-obsolete", Name: "legacy_value", Type: "string"},
		},
	}
	if !nativeRuntimeTableSupports(runtime, authored) {
		t.Fatal("schema-queue runtime table does not support the authored contract")
	}
	binding, err := bindNativeTable(authored, runtime)
	if err != nil {
		t.Fatalf("bind schema-queue table: %v", err)
	}
	controller := &NativeController{installation: &nativeInstallationBinding{tables: map[string]nativeTableBinding{"items": binding}}}
	operation := scenarios.Operation{
		ContractOperation: "local",
		Name:              "write",
		Payload:           json.RawMessage(`{"authenticated_user_id":"user-a","client_id":"client-a","mutation_id":"00000000-0000-4000-8000-000000000001","table_id":"items","pk":{"id":"row-a"},"authored_schema":{"version":1,"hash":"721d2c95e6f34cd9733feea9f5118fba391eee10d07663dad066cfc59439fa44"},"operation":"insert","client_version":"2026-08-11T00:00:00.000000Z","columns":{"value":"pending"}}`),
	}
	mapped, err := controller.ApplicationWrite(operation)
	if err != nil {
		t.Fatalf("map schema-queue application write: %v", err)
	}
	var payload struct {
		TableID string            `json:"table_id"`
		Columns map[string]string `json:"columns"`
	}
	if err := json.Unmarshal(mapped.Payload, &payload); err != nil {
		t.Fatalf("decode mapped schema-queue write: %v", err)
	}
	if payload.TableID != "cf_schema_queue" || payload.Columns["authored_mutation"] != `"pending"` || payload.Columns["legacy_value"] != "" || payload.Columns["owner_id"] != "user-a" || payload.Columns["updated_at"] != "2026-08-11T00:00:00.000000Z" {
		t.Fatalf("mapped schema-queue write = %#v, want physical queue identities", payload)
	}
}

func TestSchemaTransitionColumnValidation(t *testing.T) {
	tests := []struct {
		value string
		want  bool
	}{
		{value: "legacy_value", want: true},
		{value: "queue_value_9", want: true},
		{value: ""},
		{value: "9queue_value"},
		{value: "queue-value"},
		{value: "queue_value;drop"},
	}
	for _, test := range tests {
		if got := validSchemaTransitionColumn(test.value); got != test.want {
			t.Fatalf("validSchemaTransitionColumn(%q) = %t, want %t", test.value, got, test.want)
		}
	}
}

func TestNativeControllerBindsAcceptedApplicationPushToWALIdentity(t *testing.T) {
	table := nativeTableBinding{
		AuthoredID:       "items",
		AuthoredRelation: "public.items",
		RuntimeName:      "cf_items",
		AuthoredPrimary:  "id",
		RuntimePrimary:   "runtime-id",
		Fields:           map[string]string{"id": "runtime-id", "value": "runtime-value"},
	}
	controller := &NativeController{
		installation: &nativeInstallationBinding{
			authoredStream: "stream-1",
			tables:         map[string]nativeTableBinding{"items": table},
			relations:      map[string]string{"public.items": "items"},
			rowScopes:      map[string][]string{nativeRecordKey("items", `"pending-row"`): {"scope-a"}},
		},
		transactions: make(map[string]*nativeTransactionBinding),
	}
	operation := scenarios.Operation{
		ContractOperation: "push",
		Name:              "submit",
		Payload: json.RawMessage(`{
			"authenticated_user_id":"user-a",
			"request":{
				"client_id":"client-a",
				"client_generation":1,
				"batch_id":"00000000-0000-4000-8000-000000004002",
				"schema":{"version":1,"hash":"721d2c95e6f34cd9733feea9f5118fba391eee10d07663dad066cfc59439fa44"},
				"mutations":[{
					"mutation_id":"00000000-0000-4000-8000-000000004001",
					"table":"items",
					"pk":{"id":"pending-row"},
					"authored_schema":{"version":1,"hash":"721d2c95e6f34cd9733feea9f5118fba391eee10d07663dad066cfc59439fa44"},
					"op":"insert",
					"client_version":"2026-08-11T00:00:00.000000Z",
					"columns":{"value":"pending"}
				}]
			},
			"delivery":"apply",
			"commit_lsn":"20",
			"end_lsn":"21"
		}`),
	}

	if err := controller.BindApplicationPush(operation); err != nil {
		t.Fatalf("bind native application push: %v", err)
	}
	transaction := controller.transactions[nativeTransactionKey("stream-1", "20")]
	if transaction == nil || !transaction.ApplicationPush || len(transaction.Events) != 1 {
		t.Fatalf("application push transaction = %#v, want one bound event", transaction)
	}
	event := transaction.Events[0]
	if event.Table.AuthoredID != "items" || event.RecordID != "pending-row" || event.RuntimeRecordID != nativeRuntimeUUID("items", `"pending-row"`) || event.After == nil || string(event.After.Fields["value"]) != `"pending"` || len(event.AuthoredScopes) != 1 || event.AuthoredScopes[0] != "scope-a" {
		t.Fatalf("application push event = %#v, want authored runtime binding", event)
	}
}

func TestNativeArtifactStageRejectsUnsupportedOperationKey(t *testing.T) {
	artifact := &NativeArtifact{harness: &Harness{}}
	_, err := artifact.StageStep(context.Background(), scenarios.Operation{
		ContractOperation: "workload",
		Name:              "prepare",
		Payload:           []byte(`{"profile":"scope_topology","scope_fanout":1,"impact_rows":1}`),
	})
	if err == nil || !strings.Contains(err.Error(), `stage operation "workload/prepare" is unsupported`) {
		t.Fatalf("StageStep error = %v, want unsupported operation key", err)
	}
}

func TestNativeArtifactCloseRemovesOnlyUnchangedOwnedFiles(t *testing.T) {
	directory := t.TempDir()
	ownedPath := filepath.Join(directory, "owned.sqlite")
	unrelatedPath := filepath.Join(directory, "unrelated.sqlite")
	ownedData := []byte("owned portable seed")
	if err := os.WriteFile(ownedPath, ownedData, 0o600); err != nil {
		t.Fatalf("write owned artifact: %v", err)
	}
	if err := os.WriteFile(unrelatedPath, []byte("unrelated"), 0o600); err != nil {
		t.Fatalf("write unrelated artifact: %v", err)
	}
	digest := sha256.Sum256(ownedData)
	artifact := &NativeArtifact{
		harness:          &Harness{},
		stagingDirectory: directory,
		staged: map[string]*nativeStagedArtifact{
			"target": {path: ownedPath, sha256: hex.EncodeToString(digest[:])},
		},
	}
	if err := artifact.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Lstat(ownedPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("owned artifact remains after Close: %v", err)
	}
	if _, err := os.Lstat(unrelatedPath); err != nil {
		t.Fatalf("Close changed unrelated file: %v", err)
	}
}

func TestNativeArtifactCloseRefusesChangedOwnedFile(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "changed.sqlite")
	if err := os.WriteFile(path, []byte("changed"), 0o600); err != nil {
		t.Fatalf("write changed artifact: %v", err)
	}
	artifact := &NativeArtifact{
		harness:          &Harness{},
		stagingDirectory: directory,
		staged: map[string]*nativeStagedArtifact{
			"target": {path: path, sha256: strings.Repeat("0", 64)},
		},
	}
	if err := artifact.Close(context.Background()); err == nil || !strings.Contains(err.Error(), "refused a changed file") {
		t.Fatalf("Close error = %v, want changed-file refusal", err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("Close removed changed artifact: %v", err)
	}
}

func TestDecodeNativeCaptureDependencyImageRejectsSyncedRowAndUnexpectedField(t *testing.T) {
	dependency := nativeCaptureDependencyBinding{
		RuntimeName:    nativeCaptureDependencyFixture,
		CapturedFields: map[string]struct{}{"scope_key": {}},
	}
	valid := nativeCaptureDependencyImageWire(t, `{
		"identity":{"kind":"capture_dependency","synced_row":null,"capture_key":{"canonical_key_bytes":"impact-key"}},
		"fields":[{"field":"scope_key","type":"string","wire_json":"\"scope-a\""}],
		"version":"impact-version","checksum":null,"deleted":false
	}`)
	image, err := decodeNativeCaptureDependencyImage(&valid, dependency)
	if err != nil || image.CaptureKey != "impact-key" || string(image.Fields["scope_key"]) != `"scope-a"` {
		t.Fatalf("decode valid capture dependency image = %#v, %v", image, err)
	}

	for _, invalid := range []string{
		`{
			"identity":{"kind":"capture_dependency","synced_row":{"table_id":"items","primary_key_field_id":"id","portable_type":"string","canonical_wire_json":"\"item-a\""},"capture_key":{"canonical_key_bytes":"impact-key"}},
			"fields":[{"field":"scope_key","type":"string","wire_json":"\"scope-a\""}],
			"version":"impact-version","checksum":null,"deleted":false
		}`,
		`{
			"identity":{"kind":"capture_dependency","synced_row":null,"capture_key":{"canonical_key_bytes":"impact-key"}},
			"fields":[{"field":"outside","type":"string","wire_json":"\"scope-a\""}],
			"version":"impact-version","checksum":null,"deleted":false
		}`,
	} {
		wire := nativeCaptureDependencyImageWire(t, invalid)
		if _, err := decodeNativeCaptureDependencyImage(&wire, dependency); err == nil {
			t.Fatalf("accepted invalid capture dependency image: %s", invalid)
		}
	}
}

func TestBindNativeTransactionAllowsEmptyEventsButRejectsInvalidIdentity(t *testing.T) {
	installation := &nativeInstallationBinding{authoredStream: "stream-1"}
	transaction, err := bindNativeTransaction(nativeCommitPayload{
		StreamGeneration: "stream-1",
		CommitLSN:        "10",
		EndLSN:           "11",
	}, installation)
	if err != nil || len(transaction.Events) != 0 {
		t.Fatalf("bind event-free transaction = %#v, %v", transaction, err)
	}
	for _, payload := range []nativeCommitPayload{
		{StreamGeneration: "stream-2", CommitLSN: "10", EndLSN: "11"},
		{StreamGeneration: "stream-1", CommitLSN: "11", EndLSN: "10"},
	} {
		if _, err := bindNativeTransaction(payload, installation); err == nil {
			t.Fatalf("accepted invalid event-free transaction: %#v", payload)
		}
	}
}

func TestNativeCaptureDependencySourceStatementUsesFixture(t *testing.T) {
	dependency := nativeCaptureDependencyBinding{
		RuntimeName:    nativeCaptureDependencyFixture,
		CapturedFields: map[string]struct{}{"scope_key": {}},
	}
	statement, arguments, err := nativeSourceStatement(nativeEventBinding{
		Operation:  "insert",
		Dependency: &dependency,
		After: &nativeAuthoredImage{
			CaptureKey: "impact-key-a",
			Fields:     map[string]json.RawMessage{"scope_key": json.RawMessage(`"scope-a"`)},
		},
	}, nil)
	if err != nil || statement != "INSERT INTO cf_item_impacts (id, scope_key) VALUES ($1, $2)" || len(arguments) != 2 || arguments[0] != "impact-key-a" || arguments[1] != "scope-a" {
		t.Fatalf("capture dependency source statement = %q, %#v, %v", statement, arguments, err)
	}
	// The extension builds the capture key from the registered key column, so
	// the runtime key names that column and carries the authored identity.
	if key := nativeCaptureDependencyKey(nativeAuthoredImage{CaptureKey: "impact-key-a"}); key != `{"id":"impact-key-a"}` {
		t.Fatalf("capture dependency runtime key = %q", key)
	}
}

func nativeCaptureDependencyImageWire(t *testing.T, raw string) nativeAuthoredImageWire {
	t.Helper()
	var wire nativeAuthoredImageWire
	if err := json.Unmarshal([]byte(raw), &wire); err != nil {
		t.Fatalf("decode capture dependency test image: %v", err)
	}
	return wire
}
