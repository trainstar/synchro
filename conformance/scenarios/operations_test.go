package scenarios

import (
	"reflect"
	"testing"
)

func TestClosedOperationClasses(t *testing.T) {
	want := []string{
		"artifact/install-portable-seed",
		"connect/send",
		"local/apply-pull-page",
		"local/apply-rebuild-page",
		"local/begin-rebuild",
		"local/finalize-rebuild",
		"local/write",
		"model/activate-registry-membership-generation",
		"model/commit-source-transaction",
		"model/compact-scope",
		"model/expire-client-generation",
		"model/install-current-contract",
		"model/publish-schema",
		"model/set-client-assignments",
		"model/stage-registry-membership-generation",
		"process/acknowledge-contiguous-prefix",
		"process/materialize-source-transaction",
		"process/repair-and-retry-source-transaction",
		"process/response-loss",
		"process/restart-client",
		"process/restart-wal-worker",
		"pull/request-page",
		"push/submit",
		"rebuild/request-page",
		"workload/prepare",
	}
	if got := OperationKeys(); !reflect.DeepEqual(got, want) {
		t.Fatalf("OperationKeys() = %v, want %v", got, want)
	}

	for _, key := range want {
		class, found := LookupOperationClass(key)
		if !found {
			t.Fatalf("LookupOperationClass(%q) did not find a closed operation", key)
		}
		wantClass := OperationClassReference
		if key == "workload/prepare" {
			wantClass = OperationClassModelRunnerMacro
		}
		if class != wantClass {
			t.Fatalf("LookupOperationClass(%q) = %q, want %q", key, class, wantClass)
		}
	}

	if class, found := LookupOperationClass("local/start-sync"); found || class != "" {
		t.Fatalf("LookupOperationClass accepted a removed operation: %q, %v", class, found)
	}
}

func TestConnectSeedReceiptsUseTheClosedPayloadMember(t *testing.T) {
	valid := Operation{
		ContractOperation: "connect",
		Name:              "send",
		Payload: []byte(`{
			"user_id":"user-a",
			"client_id":"client-a",
			"runtime_version":3,
			"protocol_version":3,
			"client_generation":1,
			"schema_reset":false,
			"schema":{"version":1,"hash":"hash"},
			"scope_set_version":1,
			"known_scopes":[],
			"seed_receipts":{"scope-a":"local_seed_receipt"}
		}`),
	}
	if err := ValidateOperation(valid); err != nil {
		t.Fatalf("connect payload rejected seed_receipts: %v", err)
	}

	invalid := valid
	invalid.Payload = []byte(`{
		"user_id":"user-a",
		"client_id":"client-a",
		"runtime_version":3,
		"protocol_version":3,
		"client_generation":1,
		"schema_reset":false,
		"schema":{"version":1,"hash":"hash"},
		"scope_set_version":1,
		"known_scopes":[],
		"seed_receipt":"local_seed_receipt"
	}`)
	if err := ValidateOperation(invalid); err == nil {
		t.Fatal("connect payload accepted an undeclared seed receipt member")
	}
}

func TestValidateOperationRejectsUnknownNestedMembers(t *testing.T) {
	tests := []struct {
		name      string
		operation Operation
	}{
		{
			name: "schema reference",
			operation: Operation{ContractOperation: "connect", Name: "send", Payload: []byte(`{
				"user_id":"user-a","client_id":"client-a","runtime_version":3,"protocol_version":3,
				"schema_reset":false,"schema":{"version":1,"hash":"hash","unknown":true},
				"scope_set_version":1,"known_scopes":[]
			}`)},
		},
		{
			name: "source event image",
			operation: Operation{ContractOperation: "model", Name: "commit-source-transaction", Payload: []byte(`{
				"stream_generation":"stream-1","commit_lsn":"1","end_lsn":"2","events":[{
					"event_ordinal":1,"relation":"public.items","operation":"insert","before":null,
					"after":{"identity":{"kind":"synced","synced_row":{"canonical_identity_bytes":"id","table_id":"items","primary_key_field_id":"id","portable_type":"string","canonical_wire_json":"\"row\""},"capture_key":null},"fields":[],"version":"v1","checksum":null,"deleted":false,"unknown":true}
				}]
			}`)},
		},
		{
			name: "push mutation",
			operation: Operation{ContractOperation: "push", Name: "submit", Payload: []byte(`{
				"authenticated_user_id":"user-a","request":{"client_id":"client-a","client_generation":1,"batch_id":"batch","schema":{"version":1,"hash":"hash"},"mutations":[{"mutation_id":"mutation","table":"items","pk":{"id":"row"},"authored_schema":{"version":1,"hash":"hash"},"op":"insert","client_version":"version","unknown":true}]},"delivery":"apply","commit_lsn":"1","end_lsn":"2"
			}`)},
		},
		{
			name: "initial registry physical",
			operation: Operation{ContractOperation: "model", Name: "install-current-contract", Payload: []byte(`{
				"installation":{"installed":true,"schema_name":"synchro","extension_version":"0.3.0","protocol_version":3,"minimum_client_runtime":3,"stale_client_interval_milliseconds":1,"endpoints":[],"capabilities":[]},
				"initial_schema":{"schema":{"version":1,"hash":"hash"},"body":"body","transition_class":"initial","compatibility_floor":1,"tables":[],"affected_scopes":[]},
				"initial_registry":{"registry_generation":1,"relations":[{"relation":"public.items","registration_kind":"synced","table_id":"items","physical":{"schema":"public","name":"items","oid":1,"replica_identity":"default","unknown":true},"primary_key_field_id":"id","primary_key_physical_column":"id","primary_key_portable_type":"string","capture_key_field_ids":[],"captured_field_ids":["id"],"membership_function":"scope","positive_fanout_bound":1,"dependency_impact_function":null,"dependency_captured_field_ids":[],"positive_dependency_row_bound":null}],"capture_dependencies":[],"scope_rules":[],"dependency_impacts":[]},
				"stream":{"stream_generation":"stream-1","database":"synchro","worker_id":"worker","slot_id":"slot"},"empty_scopes":[],"clients":[],"write_policies":[],"configured_limits":{"max_scope_fanout":1,"max_impact_rows":1,"pull_maximum":1,"rebuild_maximum":1,"compaction_batch_maximum":1,"backfill_batch_maximum":1}
			}`)},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateOperation(test.operation); err == nil {
				t.Fatal("accepted an unknown nested operation member")
			}
		})
	}
}
