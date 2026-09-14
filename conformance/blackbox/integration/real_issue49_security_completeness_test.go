package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

// TestRealIssue49SecurityAdapterAuthorityAndScopeBoundary proves
// SYNC-BOUNDARY-001 and SYNC-SCOPE-005 for every identity-bearing endpoint.
func TestRealIssue49SecurityAdapterAuthorityAndScopeBoundary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-security-boundary")
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")

	type endpoint struct {
		name      string
		function  string
		path      string
		request   map[string]any
		wantAfter int
	}
	endpoints := []endpoint{
		{
			name:     "connect",
			function: "synchro.synchro_connect(pg_catalog.text,pg_catalog.jsonb)",
			path:     "/sync/connect",
			request: map[string]any{
				"client_id":         "issue49-security-revoked-connect",
				"platform":          "conformance",
				"app_version":       "0.3.0",
				"protocol_version":  3,
				"schema":            map[string]any{"version": 0, "hash": ""},
				"scope_set_version": 0,
				"known_scopes":      map[string]any{},
			},
			wantAfter: http.StatusOK,
		},
		{
			name:      "pull",
			function:  "synchro.synchro_pull(pg_catalog.text,pg_catalog.jsonb)",
			path:      "/sync/pull",
			request:   realPullPayload(client, issue49CloneScopes(client.Scopes), 100),
			wantAfter: http.StatusOK,
		},
		{
			name:     "push",
			function: "synchro.synchro_push(pg_catalog.text,pg_catalog.jsonb)",
			path:     "/sync/push",
			request: phase4PushPayload(client, "00000000-0000-4000-8a01-000000000001", []map[string]any{
				phase4InsertMutation(
					client,
					table,
					ownerField,
					"00000000-0000-4000-8a01-000000000002",
					"00000000-0000-4000-8a01-000000000003",
					"issue49-security-delegated-write",
				),
			}),
			wantAfter: http.StatusOK,
		},
		{
			name:     "rebuild",
			function: "synchro.synchro_rebuild(pg_catalog.text,pg_catalog.jsonb)",
			path:     "/sync/rebuild",
			request: map[string]any{
				"client_id":         client.ID,
				"client_generation": client.Generation,
				"schema":            client.Schema,
				"scope":             "user:diagnostic-user",
				"rebuild_id":        "00000000-0000-4000-8a01-000000000004",
				"cursor":            nil,
				"limit":             100,
			},
			wantAfter: http.StatusOK,
		},
	}
	deniedStatuses := make(map[string]int, len(endpoints))
	restoredStatuses := make(map[string]int, len(endpoints))
	for _, endpoint := range endpoints {
		revoke := "REVOKE EXECUTE ON FUNCTION " + endpoint.function + " FROM synchro_adapter"
		grant := "GRANT EXECUTE ON FUNCTION " + endpoint.function + " TO synchro_adapter"
		if _, err := admin.ExecContext(ctx, revoke); err != nil {
			t.Fatalf("revoke canonical %s function: %v", endpoint.name, err)
		}
		status, _ := postSync(t, ctx, harness.AdapterURL(), token, endpoint.path, endpoint.request)
		deniedStatuses[endpoint.name] = status
		if _, err := admin.ExecContext(ctx, grant); err != nil {
			t.Fatalf("restore canonical %s function: %v", endpoint.name, err)
		}
		status, restored := postSync(t, ctx, harness.AdapterURL(), token, endpoint.path, endpoint.request)
		restoredStatuses[endpoint.name] = status
		if endpoint.name == "push" && status == http.StatusOK {
			waitForRealWALRecords(t, ctx, harness, "cf_items", "00000000-0000-4000-8a01-000000000003")
		}
		if endpoint.name == "rebuild" && status == http.StatusOK {
			cursor, ok := restored["final_scope_cursor"].(string)
			if !ok || cursor == "" {
				t.Fatalf("restored rebuild did not return a final scope cursor: %#v", restored)
			}
			client.Scopes["user:diagnostic-user"] = map[string]any{"cursor": cursor}
		}
	}

	t.Run("assertion", func(t *testing.T) {
		for _, endpoint := range endpoints {
			if deniedStatuses[endpoint.name] != http.StatusInternalServerError || restoredStatuses[endpoint.name] != endpoint.wantAfter {
				t.Fatalf(
					"canonical %s delegation boundary failed: denied=%d restored=%d",
					endpoint.name,
					deniedStatuses[endpoint.name],
					restoredStatuses[endpoint.name],
				)
			}
		}
		rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8a01-000000000005")
		acknowledgeRealClientCursors(t, ctx, harness, token, client)
		before := observeCheckpointMap(t, ctx, harness, client.ID)
		unknownScopes := issue49CloneScopes(client.Scopes)
		unknownScopes["security49:client-authored"] = client.Scopes["user:diagnostic-user"]
		unknownStatus, unknownResponse := postSync(
			t, ctx, harness.AdapterURL(), token, "/sync/pull", realPullPayload(client, unknownScopes, 100),
		)
		predicateRequest := realPullPayload(client, issue49CloneScopes(client.Scopes), 100)
		predicateRequest["predicate"] = "owner_id = current_user"
		predicateStatus, predicateResponse := postSync(t, ctx, harness.AdapterURL(), token, "/sync/pull", predicateRequest)
		SQLRequest := realPullPayload(client, issue49CloneScopes(client.Scopes), 100)
		SQLRequest["scope_sql"] = "SELECT scope_id FROM private_assignments"
		SQLStatus, SQLResponse := postSync(t, ctx, harness.AdapterURL(), token, "/sync/pull", SQLRequest)
		rebuildRequest := endpoints[3].request
		rebuildRequest["scope"] = "security49:client-authored"
		rebuildStatus, rebuildResponse := postSync(t, ctx, harness.AdapterURL(), token, "/sync/rebuild", rebuildRequest)
		after := observeCheckpointMap(t, ctx, harness, client.ID)
		assertIssue49ProtocolError(t, unknownStatus, unknownResponse, http.StatusBadRequest, "invalid_request", false)
		assertIssue49ProtocolError(t, predicateStatus, predicateResponse, http.StatusBadRequest, "invalid_request", false)
		assertIssue49ProtocolError(t, SQLStatus, SQLResponse, http.StatusBadRequest, "invalid_request", false)
		assertIssue49ProtocolError(t, rebuildStatus, rebuildResponse, http.StatusBadRequest, "invalid_request", false)
		if !issue49CheckpointMapsEqual(before, after) {
			t.Fatalf("client-authored replication input changed durable progress: before=%#v after=%#v", before, after)
		}
	})
}

// TestRealIssue49SecurityRegistryIdentityAndKeys proves SYNC-REGISTRY-001 and
// SYNC-REGISTRY-002 across lookup identity, registration, drift, and DML.
func TestRealIssue49SecurityRegistryIdentityAndKeys(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)

	var registeredOID int64
	var physicalSchema, physicalRelation, replicaIdentity, portableType string
	if err := admin.QueryRowContext(ctx, `
		SELECT registry.physical_relation_oid::bigint,
		       registry.physical_schema::text,
		       registry.physical_relation::text,
		       registry.replica_identity::text,
		       registry.pk_portable_type
		FROM synchro.sync_registry registry
		JOIN synchro.sync_registry_generations generation
		  ON generation.generation = registry.registry_generation
		WHERE generation.state = 'active' AND registry.table_name = 'cf_items'`).Scan(
		&registeredOID,
		&physicalSchema,
		&physicalRelation,
		&replicaIdentity,
		&portableType,
	); err != nil {
		t.Fatalf("load active registered identity: %v", err)
	}
	if _, err := admin.ExecContext(ctx, `
		CREATE SCHEMA security49_shadow;
		CREATE TABLE security49_shadow.cf_items (
			id uuid PRIMARY KEY,
			owner_id text NOT NULL,
			value text NOT NULL,
			updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
			deleted_at timestamptz
		);
		CREATE TABLE security49_shadow.composite_key (
			id bigint NOT NULL,
			part bigint NOT NULL,
			updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
			deleted_at timestamptz,
			PRIMARY KEY (id, part)
		);
		CREATE TABLE security49_shadow.nullable_key (
			id bigint,
			updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
			deleted_at timestamptz,
			UNIQUE (id)
		);
		CREATE TABLE security49_shadow.nonportable_key (
			id numeric PRIMARY KEY,
			updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
			deleted_at timestamptz
		);
		CREATE TABLE security49_shadow.full_identity (
			id bigint PRIMARY KEY,
			updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
			deleted_at timestamptz
		);
		ALTER TABLE security49_shadow.full_identity REPLICA IDENTITY FULL`); err != nil {
		t.Fatalf("create registry rejection controls: %v", err)
	}

	connection, err := admin.Conn(ctx)
	if err != nil {
		t.Fatalf("open hostile search-path connection: %v", err)
	}
	if _, err := connection.ExecContext(ctx, "SET search_path = security49_shadow, public, pg_catalog"); err != nil {
		_ = connection.Close()
		t.Fatalf("set hostile registry search path: %v", err)
	}
	shadowHealth := loadIssue49Health(t, ctx, connection)
	_ = connection.Close()

	registrationErrors := make(map[string]bool)
	registrationStatements := map[string]string{
		"bare_name": `SELECT synchro.synchro_register_table(
			'cf_items', 'public.cf_items_membership', 'single_scope',
			'id', 'updated_at', 'deleted_at', 'enabled')`,
		"composite": `SELECT synchro.synchro_register_table(
			'security49_shadow.composite_key', 'public.cf_items_membership', 'single_scope',
			'id', 'updated_at', 'deleted_at', 'enabled')`,
		"nullable": `SELECT synchro.synchro_register_table(
			'security49_shadow.nullable_key', 'public.cf_items_membership', 'single_scope',
			'id', 'updated_at', 'deleted_at', 'enabled')`,
		"nonportable": `SELECT synchro.synchro_register_table(
			'security49_shadow.nonportable_key', 'public.cf_items_membership', 'single_scope',
			'id', 'updated_at', 'deleted_at', 'enabled')`,
		"replica_identity": `SELECT synchro.synchro_register_table(
			'security49_shadow.full_identity', 'public.cf_items_membership', 'single_scope',
			'id', 'updated_at', 'deleted_at', 'enabled')`,
	}
	for name, statement := range registrationStatements {
		_, err := admin.ExecContext(ctx, statement)
		registrationErrors[name] = err != nil
	}

	recordID := "00000000-0000-4000-8a02-000000000001"
	changedID := "00000000-0000-4000-8a02-000000000002"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'security49-key-guard')",
		recordID,
	); err != nil {
		t.Fatalf("create registered key update control: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", recordID)
	beforeKeyUpdate := observeIssue49WALStages(t, ctx, admin, []string{recordID, changedID})
	keyUpdateErr := harness.Source().ExecContext(ctx, "UPDATE cf_items SET id = $2 WHERE id = $1", recordID, changedID)
	afterKeyUpdate := observeIssue49WALStages(t, ctx, admin, []string{recordID, changedID})

	if _, err := admin.ExecContext(ctx, "ALTER TABLE public.cf_items REPLICA IDENTITY FULL"); err != nil {
		t.Fatalf("inject active replica-identity drift: %v", err)
	}
	replicaDrift := loadIssue49Health(t, ctx, admin)
	if _, err := admin.ExecContext(ctx, "ALTER TABLE public.cf_items REPLICA IDENTITY DEFAULT"); err != nil {
		t.Fatalf("restore active replica identity: %v", err)
	}
	waitForIssue49CanonicalHealth(t, ctx, admin, true)

	if _, err := admin.ExecContext(ctx, `
		ALTER TABLE public.cf_items RENAME TO cf_items_registered_oid;
		CREATE TABLE public.cf_items (
			id uuid PRIMARY KEY,
			owner_id text NOT NULL,
			value text NOT NULL,
			updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
			deleted_at timestamptz
		)`); err != nil {
		t.Fatalf("inject active relation OID drift: %v", err)
	}
	var replacementOID, persistedOID int64
	if err := admin.QueryRowContext(ctx, `
		SELECT 'public.cf_items'::regclass::oid::bigint,
		       registry.physical_relation_oid::bigint
		FROM synchro.sync_registry registry
		JOIN synchro.sync_registry_generations generation
		  ON generation.generation = registry.registry_generation
		WHERE generation.state = 'active' AND registry.table_name = 'cf_items'`).Scan(
		&replacementOID,
		&persistedOID,
	); err != nil {
		t.Fatalf("observe active relation replacement: %v", err)
	}
	OIDDrift := loadIssue49Health(t, ctx, admin)

	t.Run("assertion", func(t *testing.T) {
		if registeredOID <= 0 || physicalSchema != "public" || physicalRelation != "cf_items" || replicaIdentity != "d" || portableType != "string" {
			t.Fatalf(
				"active registry identity is incomplete: oid=%d schema=%q relation=%q replica=%q portable=%q",
				registeredOID,
				physicalSchema,
				physicalRelation,
				replicaIdentity,
				portableType,
			)
		}
		if shadowHealth["ready"] != true || issue49HealthChecks(t, shadowHealth)["relation_identity"] != "ok" {
			t.Fatalf("hostile search_path rebound the registered relation: %#v", shadowHealth)
		}
		for name := range registrationStatements {
			if !registrationErrors[name] {
				t.Fatalf("nonconforming registry input %q was accepted", name)
			}
		}
		if keyUpdateErr == nil || beforeKeyUpdate != afterKeyUpdate {
			t.Fatalf("primary-key update crossed the fence boundary: err=%v before=%#v after=%#v", keyUpdateErr, beforeKeyUpdate, afterKeyUpdate)
		}
		if replicaDrift["ready"] != false || issue49HealthChecks(t, replicaDrift)["relation_identity"] != "failed" {
			t.Fatalf("replica-identity drift remained active and ready: %#v", replicaDrift)
		}
		if registeredOID != persistedOID || replacementOID == persistedOID || OIDDrift["ready"] != false ||
			issue49HealthChecks(t, OIDDrift)["relation_identity"] != "failed" {
			t.Fatalf(
				"relation OID drift rebound or remained ready: registered=%d replacement=%d persisted=%d health=%#v",
				registeredOID,
				replacementOID,
				persistedOID,
				OIDDrift,
			)
		}
	})
}

// TestRealIssue49SecurityCaptureHealthFailsClosed proves SYNC-WAL-008,
// SYNC-HEALTH-001, and SYNC-HEALTH-002 for every canonical readiness class.
func TestRealIssue49SecurityCaptureHealthFailsClosed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	waitForIssue49CanonicalHealth(t, ctx, admin, true)
	baseline := loadIssue49Health(t, ctx, admin)
	baselineStatus, baselineBody := getIssue49Readiness(t, ctx, harness.AdapterURL())

	recordID := "00000000-0000-4000-8a03-000000000001"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'security49-health-boundary')",
		recordID,
	); err != nil {
		t.Fatalf("create health progress boundary: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", recordID)

	faults := []struct {
		name       string
		wantCheck  string
		statements []string
	}{
		{
			name:      "extension objects",
			wantCheck: "extension_objects_stale",
			statements: []string{
				"UPDATE synchro.sync_extension_build SET installed_fingerprint = repeat('0', 64)",
			},
		},
		{
			name:      "registry generation",
			wantCheck: "registry_generation",
			statements: []string{
				"UPDATE synchro.sync_runtime_state SET stream_generation = 'security49-invalid-generation' WHERE singleton",
			},
		},
		{
			name:      "schema generation",
			wantCheck: "schema_generation",
			statements: []string{
				"ALTER TABLE synchro.sync_schema_manifest DISABLE TRIGGER USER",
				"UPDATE synchro.sync_schema_manifest SET canonical_manifest_body = '{}' WHERE schema_version = (SELECT max(schema_version) FROM synchro.sync_schema_manifest)",
			},
		},
		{
			name:      "relation identity",
			wantCheck: "relation_identity",
			statements: []string{
				"ALTER TABLE public.cf_items REPLICA IDENTITY FULL",
			},
		},
		{
			name:      "publication set",
			wantCheck: "publication",
			statements: []string{
				"ALTER PUBLICATION " + security49QuoteIdentifier(harness.Names().Publication) + " DROP TABLE public.cf_items",
			},
		},
		{
			name:      "capture triggers",
			wantCheck: "capture_triggers",
			statements: []string{
				"ALTER TABLE public.cf_items DISABLE TRIGGER synchro_capture_fence",
			},
		},
		{
			name:      "replication slot",
			wantCheck: "replication_slot",
			statements: []string{
				"UPDATE synchro.sync_runtime_state SET active_slot_name = 'security49_missing_slot' WHERE singleton",
			},
		},
		{
			name:      "stream reset",
			wantCheck: "stream_reset",
			statements: []string{`
				INSERT INTO synchro.sync_stream_resets (
					reset_id, operation_kind, source_stream_generation,
					target_stream_generation, source_registry_generation,
					old_slot_name, candidate_slot_name, database_oid,
					database_name, plugin, lifecycle
				)
				SELECT '00000000-0000-4000-8a03-000000000002'::uuid,
				       'stream_reset', runtime.stream_generation,
				       runtime.stream_generation || '-security49', generation.generation,
				       runtime.active_slot_name, 'security49_candidate',
				       database.oid, database.datname, 'pgoutput', 'preparing'
				FROM synchro.sync_runtime_state runtime
				JOIN synchro.sync_registry_generations generation ON generation.state = 'active'
				JOIN pg_catalog.pg_database database ON database.datname = current_database()
				WHERE runtime.singleton`,
			},
		},
		{
			name:      "poison",
			wantCheck: "poison",
			statements: []string{`
				INSERT INTO synchro.sync_wal_poison (
					stream_generation, commit_lsn, failure_class, failure_detail
				)
				SELECT stream_generation, pg_catalog.pg_current_wal_lsn(),
				       'decode_failed', 'bounded_decode_failure'
				FROM synchro.sync_runtime_state WHERE singleton`,
			},
		},
		{
			name:      "contiguous progress",
			wantCheck: "materialization_progress",
			statements: []string{
				"UPDATE synchro.sync_wal_progress SET acknowledged_end_lsn = NULL WHERE singleton",
			},
		},
		{
			name:      "worker state",
			wantCheck: "worker",
			statements: []string{
				"UPDATE synchro.sync_wal_worker_state SET state = 'stopped' WHERE worker_id = 'synchro_wal_consumer'",
			},
		},
		{
			name:      "heartbeat observation",
			wantCheck: "heartbeat",
			statements: []string{
				"UPDATE synchro.sync_wal_worker_state SET heartbeat_at = clock_timestamp() - interval '1 day' WHERE worker_id = 'synchro_wal_consumer'",
			},
		},
		{
			name:      "oldest commit observation",
			wantCheck: "wal_time_lag",
			statements: []string{
				"UPDATE synchro.sync_wal_worker_state SET oldest_unmaterialized_commit_timestamp = clock_timestamp() - interval '1 day' WHERE worker_id = 'synchro_wal_consumer'",
			},
		},
	}
	faultResults := make(map[string]map[string]any, len(faults))
	for _, fault := range faults {
		faultResults[fault.name] = security49HealthDuringTransaction(t, ctx, admin, fault.statements)
	}

	limitSettings := map[string]string{
		"synchro.max_worker_heartbeat_age_seconds": "heartbeat",
		"synchro.max_wal_lag_bytes":                "wal_byte_lag",
		"synchro.max_wal_lag_seconds":              "wal_time_lag",
	}
	limitResults := make(map[string]security49PublicHealth, len(limitSettings))
	for setting, check := range limitSettings {
		security49SetSystemHealthLimit(t, ctx, admin, setting, 0)
		limitResults[check] = security49ObservePublicHealth(t, ctx, admin, harness.AdapterURL(), check)
		security49SetSystemHealthLimit(t, ctx, admin, setting, security49DefaultHealthLimit(setting))
		waitForIssue49CanonicalHealth(t, ctx, admin, true)
	}

	t.Run("assertion", func(t *testing.T) {
		if baselineStatus != http.StatusOK || !bytes.Equal(baselineBody, []byte(`{"ready":true}`)) || baseline["ready"] != true {
			t.Fatalf("healthy public readiness is not canonical: status=%d body=%q detail=%#v", baselineStatus, baselineBody, baseline)
		}
		baselineChecks := issue49HealthChecks(t, baseline)
		if len(baselineChecks) != len(issue49HealthCheckNames) {
			t.Fatalf("canonical readiness check set is incomplete: %#v", baselineChecks)
		}
		for _, name := range issue49HealthCheckNames {
			if baselineChecks[name] != "ok" {
				t.Fatalf("healthy canonical check %q = %q", name, baselineChecks[name])
			}
		}
		security49AssertFiniteHealthObservations(t, baseline)
		for _, fault := range faults {
			result := faultResults[fault.name]
			checks := issue49HealthChecks(t, result)
			if result["ready"] != false || checks[fault.wantCheck] == "ok" {
				t.Fatalf("health fault %q did not fail closed at %q: %#v", fault.name, fault.wantCheck, result)
			}
			security49AssertTriStateChecks(t, checks)
		}
		for check, result := range limitResults {
			if result.status != http.StatusServiceUnavailable || !bytes.Equal(result.body, []byte(`{"ready":false}`)) ||
				result.detail["ready"] != false || issue49HealthChecks(t, result.detail)[check] != "failed" {
				t.Fatalf("invalid finite limit for %q did not fail closed: %#v", check, result)
			}
			if bytes.Contains(result.body, []byte("checks")) || bytes.Contains(result.body, []byte("observations")) {
				t.Fatalf("public readiness exposed detailed state for %q: %q", check, result.body)
			}
		}
	})
}

// TestRealIssue49SecurityDatabaseAuthority proves SYNC-DBAUTH-001 and
// SYNC-DBAUTH-002 from installed catalog authority and runtime behavior.
func TestRealIssue49SecurityDatabaseAuthority(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	environment, err := blackbox.LoadEnvironment()
	if err != nil {
		t.Fatalf("load database authority environment: %v", err)
	}
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)

	var restrictedGroups, publicAuthority, directMetadataAuthority, unexpectedFunctionAuthority int64
	if err := admin.QueryRowContext(ctx, `
		SELECT count(*)
		FROM pg_catalog.pg_roles
		WHERE rolname = ANY($1)
		  AND NOT rolcanlogin AND NOT rolreplication
		  AND NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolbypassrls`,
		[]string{"synchro_owner", "synchro_adapter", "synchro_seed", "synchro_monitor", "synchro_operator", "synchro_worker"},
	).Scan(&restrictedGroups); err != nil {
		t.Fatalf("inspect fixed authority groups: %v", err)
	}
	if err := admin.QueryRowContext(ctx, security49PublicAuthoritySQL).Scan(&publicAuthority); err != nil {
		t.Fatalf("inspect PUBLIC extension authority: %v", err)
	}
	if err := admin.QueryRowContext(ctx, `
		SELECT count(*) FROM (
			SELECT 1
			FROM pg_catalog.pg_class relation
			JOIN pg_catalog.pg_namespace namespace ON namespace.oid = relation.relnamespace
			CROSS JOIN unnest(ARRAY['synchro_adapter', 'synchro_seed', 'synchro_monitor']) runtime(role_name)
			WHERE namespace.nspname = 'synchro' AND relation.relkind IN ('r', 'p')
			  AND pg_catalog.has_table_privilege(
				runtime.role_name, relation.oid,
				'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
			  )
			UNION ALL
			SELECT 1
			FROM pg_catalog.pg_class sequence
			JOIN pg_catalog.pg_namespace namespace ON namespace.oid = sequence.relnamespace
			CROSS JOIN unnest(ARRAY['synchro_adapter', 'synchro_seed', 'synchro_monitor']) runtime(role_name)
			WHERE namespace.nspname = 'synchro' AND sequence.relkind = 'S'
			  AND pg_catalog.has_sequence_privilege(runtime.role_name, sequence.oid, 'USAGE,SELECT,UPDATE')
		) exposed`).Scan(&directMetadataAuthority); err != nil {
		t.Fatalf("inspect direct metadata authority: %v", err)
	}
	if err := admin.QueryRowContext(ctx, security49UnexpectedFunctionAuthoritySQL).Scan(&unexpectedFunctionAuthority); err != nil {
		t.Fatalf("inspect runtime function authority: %v", err)
	}

	var unsafeFunctions int64
	if err := admin.QueryRowContext(ctx, `
		SELECT count(*)
		FROM pg_catalog.pg_proc procedure
		JOIN pg_catalog.pg_namespace namespace ON namespace.oid = procedure.pronamespace
		JOIN pg_catalog.pg_roles owner ON owner.oid = procedure.proowner
		WHERE namespace.nspname = 'synchro'
		  AND (owner.rolname <> 'synchro_owner'
		       OR NOT procedure.prosecdef
		       OR NOT COALESCE(procedure.proconfig, '{}'::text[]) @> ARRAY['search_path=pg_catalog, synchro'])`).Scan(&unsafeFunctions); err != nil {
		t.Fatalf("inspect privileged function definitions: %v", err)
	}

	memberships := map[string]string{
		environment.Adapter.Username:  "synchro_adapter",
		environment.Operator.Username: "synchro_operator",
		environment.Worker.Username:   "synchro_worker",
	}
	membershipValid := true
	for login, group := range memberships {
		var exact bool
		if err := admin.QueryRowContext(ctx, `
			SELECT count(*) = 1 AND COALESCE(bool_and(granted.rolname = $2), false)
			FROM pg_catalog.pg_auth_members membership
			JOIN pg_catalog.pg_roles granted ON granted.oid = membership.roleid
			JOIN pg_catalog.pg_roles member ON member.oid = membership.member
			WHERE member.rolname = $1`, login, group).Scan(&exact); err != nil {
			t.Fatalf("inspect runtime membership for %q: %v", login, err)
		}
		membershipValid = membershipValid && exact
	}
	var crossRoleReachable, workerBoundary, soleReplicationLogin, workerRequiresSetRole bool
	if err := admin.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM unnest(ARRAY['synchro_adapter', 'synchro_seed', 'synchro_monitor', 'synchro_operator', 'synchro_worker']) source(role_name)
			CROSS JOIN unnest(ARRAY['synchro_owner', 'synchro_adapter', 'synchro_seed', 'synchro_monitor', 'synchro_operator', 'synchro_worker']) target(role_name)
			WHERE source.role_name <> target.role_name
			  AND pg_catalog.pg_has_role(source.role_name, target.role_name, 'MEMBER')
		),
		(SELECT rolcanlogin AND rolreplication AND NOT rolinherit
		        AND NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolbypassrls
		 FROM pg_catalog.pg_roles WHERE rolname = $1),
		(SELECT count(*) = 1 AND bool_and(rolname = $1)
		 FROM pg_catalog.pg_roles WHERE rolcanlogin AND rolreplication AND NOT rolsuper),
		NOT pg_catalog.has_table_privilege($1, 'synchro.sync_wal_progress', 'SELECT')
		AND pg_catalog.has_table_privilege('synchro_worker', 'synchro.sync_wal_progress', 'SELECT')`,
		environment.Worker.Username,
	).Scan(&crossRoleReachable, &workerBoundary, &soleReplicationLogin, &workerRequiresSetRole); err != nil {
		t.Fatalf("inspect role separation boundary: %v", err)
	}

	var workerHBA, workerHBAReject bool
	if err := admin.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_catalog.pg_hba_file_rules
			WHERE $1 = ANY(database) AND $2 = ANY(user_name)
			  AND auth_method IN ('scram-sha-256', 'cert') AND error IS NULL
		), EXISTS (
			SELECT 1 FROM pg_catalog.pg_hba_file_rules
			WHERE 'all' = ANY(database)
			  AND ($2 = ANY(user_name) OR 'all' = ANY(user_name))
			  AND auth_method = 'reject' AND error IS NULL
		)`, harness.Names().Database, environment.Worker.Username).Scan(&workerHBA, &workerHBAReject); err != nil {
		t.Fatalf("inspect worker HBA boundary: %v", err)
	}

	capabilities := []struct {
		role     string
		function string
	}{
		{"synchro_adapter", "synchro.synchro_connect(pg_catalog.text,pg_catalog.jsonb)"},
		{"synchro_adapter", "synchro.synchro_push(pg_catalog.text,pg_catalog.jsonb)"},
		{"synchro_adapter", "synchro.synchro_pull(pg_catalog.text,pg_catalog.jsonb)"},
		{"synchro_adapter", "synchro.synchro_rebuild(pg_catalog.text,pg_catalog.jsonb)"},
		{"synchro_adapter", "synchro.synchro_readiness()"},
		{"synchro_seed", "synchro.synchro_schema_manifest()"},
		{"synchro_seed", "synchro.synchro_portable_seed_manifest(pg_catalog.int4)"},
		{"synchro_monitor", "synchro.synchro_readiness()"},
		{"synchro_monitor", "synchro.synchro_health_detail()"},
		{"synchro_operator", "synchro.synchro_health_detail()"},
	}
	capabilitiesValid := true
	for _, capability := range capabilities {
		var allowed bool
		if err := admin.QueryRowContext(
			ctx,
			"SELECT pg_catalog.has_function_privilege($1, $2, 'EXECUTE')",
			capability.role,
			capability.function,
		).Scan(&allowed); err != nil {
			t.Fatalf("inspect %s function capability: %v", capability.role, err)
		}
		capabilitiesValid = capabilitiesValid && allowed
	}
	nonSuperuserChecks := security49ExerciseRuntimeFunctions(t, ctx, admin)
	secretFilesExternal := security49SecretFilesAreExternal(t, environment)

	t.Run("assertion", func(t *testing.T) {
		if restrictedGroups != 6 || publicAuthority != 0 || directMetadataAuthority != 0 || unexpectedFunctionAuthority != 0 || unsafeFunctions != 0 {
			t.Fatalf(
				"installed least-privilege catalog is invalid: groups=%d public=%d metadata=%d functions=%d unsafe=%d",
				restrictedGroups,
				publicAuthority,
				directMetadataAuthority,
				unexpectedFunctionAuthority,
				unsafeFunctions,
			)
		}
		if !membershipValid || crossRoleReachable || !workerBoundary || !soleReplicationLogin || !workerRequiresSetRole {
			t.Fatalf(
				"runtime role separation failed: memberships=%t cross_role=%t worker=%t sole=%t set_role=%t",
				membershipValid,
				crossRoleReachable,
				workerBoundary,
				soleReplicationLogin,
				workerRequiresSetRole,
			)
		}
		if !workerHBA || !workerHBAReject || !capabilitiesValid || !nonSuperuserChecks || !secretFilesExternal {
			t.Fatalf(
				"runtime authority controls failed: hba=%t reject=%t capabilities=%t execution=%t external_secrets=%t",
				workerHBA,
				workerHBAReject,
				capabilitiesValid,
				nonSuperuserChecks,
				secretFilesExternal,
			)
		}
	})
}

// TestRealIssue49SecurityOperationalRedaction proves SYNC-LOGGING-001 across
// process logs, readiness, diagnostics, quarantine, metrics, and traces.
func TestRealIssue49SecurityOperationalRedaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	environment, err := blackbox.LoadEnvironment()
	if err != nil {
		t.Fatalf("load redaction environment: %v", err)
	}
	harness, token := provisionRealProofHarness(t, ctx)
	clientCanary := "security49-client-8f36c4"
	client := connectRealProtocolClient(t, ctx, harness, token, clientCanary)
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")
	recordCanary := "00000000-0000-4000-8a05-000000000001"
	valueCanary := "security49-row-value-d430ef"
	mutationCanary := "00000000-0000-4000-8a05-000000000002"
	status, _ := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		"00000000-0000-4000-8a05-000000000003",
		[]map[string]any{phase4InsertMutation(client, table, ownerField, mutationCanary, recordCanary, valueCanary)},
	))
	if status == http.StatusOK {
		waitForRealWALRecords(t, ctx, harness, "cf_items", recordCanary)
	}

	scopeCanary := "security49:scope-f97483"
	scopeRequest := realPullPayload(client, issue49CloneScopes(client.Scopes), 10)
	scopeRequest["predicate"] = scopeCanary
	_, _ = postSync(t, ctx, harness.AdapterURL(), token, "/sync/pull", scopeRequest)
	readyStatus, readyBody := getIssue49Readiness(t, ctx, harness.AdapterURL())
	for readyStatus == http.StatusServiceUnavailable && bytes.Equal(readyBody, []byte(`{"ready":false}`)) {
		select {
		case <-ctx.Done():
			t.Fatalf("operational redaction fixture did not become ready: %s", harness.FailureDiagnostics())
		case <-time.After(50 * time.Millisecond):
		}
		readyStatus, readyBody = getIssue49Readiness(t, ctx, harness.AdapterURL())
	}
	admin := openIssue49Admin(t, ctx, harness)
	health := loadIssue49Health(t, ctx, admin)
	wALDiagnostic, diagnosticErr := harness.Operator().WALDiagnostics(ctx)
	if diagnosticErr != nil {
		t.Fatalf("load bounded WAL diagnostic: %v", diagnosticErr)
	}
	var quarantine string
	if err := admin.QueryRowContext(ctx, `
		SELECT COALESCE(string_agg(failure_detail, '' ORDER BY id), '')
		FROM synchro.sync_wal_poison`).Scan(&quarantine); err != nil {
		t.Fatalf("load bounded quarantine text: %v", err)
	}
	metricsStatus, metricsBody := security49GetBounded(t, ctx, harness.AdapterURL()+"/metrics")
	tracesStatus, tracesBody := security49GetBounded(t, ctx, harness.AdapterURL()+"/traces")
	diagnostics := harness.FailureDiagnostics()
	healthJSON, err := json.Marshal(health)
	if err != nil {
		t.Fatalf("encode detailed health observation: %v", err)
	}
	credentialCanaries := security49LoadCredentialCanaries(t, environment)
	canaries := append([]string{
		clientCanary,
		recordCanary,
		valueCanary,
		mutationCanary,
		scopeCanary,
		token,
	}, credentialCanaries...)
	outputs := [][]byte{
		[]byte(diagnostics),
		readyBody,
		healthJSON,
		[]byte(wALDiagnostic),
		[]byte(quarantine),
		metricsBody,
		tracesBody,
	}
	logDisclosure, err := harness.StopAdapterAndObserveLogDisclosure(ctx, canaries)
	if err != nil {
		t.Fatalf("observe operational logs: %v", err)
	}

	t.Run("assertion", func(t *testing.T) {
		if logDisclosure {
			t.Fatal("operational logs disclosed protected data")
		}
		if status != http.StatusOK || readyStatus != http.StatusOK || !bytes.Equal(readyBody, []byte(`{"ready":true}`)) {
			t.Fatalf("redaction exercise did not reach healthy operational output: push=%d ready=%d body=%q", status, readyStatus, readyBody)
		}
		if metricsStatus != http.StatusNotFound || tracesStatus != http.StatusNotFound {
			t.Fatalf("unconfigured metric or trace output became public: metrics=%d traces=%d", metricsStatus, tracesStatus)
		}
		if len(quarantine) > 512 || len(diagnostics) > 8704 {
			t.Fatalf("operational failure output is unbounded: quarantine=%d diagnostics=%d", len(quarantine), len(diagnostics))
		}
		for _, canary := range canaries {
			if canary == "" {
				continue
			}
			for _, output := range outputs {
				if bytes.Contains(output, []byte(canary)) {
					t.Fatal("operational output disclosed protected data")
				}
			}
		}
	})
}

// TestRealIssue49SecurityInstallationAuthority proves SYNC-INSTALL-001 and
// SYNC-INSTALL-002 from the packaged artifact and clean PostgreSQL 18 state.
func TestRealIssue49SecurityInstallationAuthority(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	environment, err := blackbox.LoadEnvironment()
	if err != nil {
		t.Fatalf("load installation authority environment: %v", err)
	}
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)

	var serverMajor, otherVersions, updatePaths int
	var extensionVersion, extensionSchema string
	if err := admin.QueryRowContext(ctx, `
		SELECT current_setting('server_version_num')::integer / 10000,
		       extension.extversion, namespace.nspname
		FROM pg_catalog.pg_extension extension
		JOIN pg_catalog.pg_namespace namespace ON namespace.oid = extension.extnamespace
		WHERE extension.extname = 'synchro_pg'`).Scan(&serverMajor, &extensionVersion, &extensionSchema); err != nil {
		t.Fatalf("inspect clean extension installation: %v", err)
	}
	if err := admin.QueryRowContext(ctx, `
		SELECT count(*) FROM pg_catalog.pg_available_extension_versions
		WHERE name = 'synchro_pg' AND version <> '0.3.0'`).Scan(&otherVersions); err != nil {
		t.Fatalf("inspect extension baseline versions: %v", err)
	}
	if err := admin.QueryRowContext(ctx, "SELECT count(*) FROM pg_catalog.pg_extension_update_paths('synchro_pg')").Scan(&updatePaths); err != nil {
		t.Fatalf("inspect extension update paths: %v", err)
	}
	trackedSQL, packagedSQL := security49InstallationFiles(t, environment.ExtensionArtifact, "synchro_pg--0.3.0.sql")
	trackedControl, packagedControl := security49InstallationFiles(t, environment.ExtensionArtifact, "synchro_pg.control")
	control := string(packagedControl)
	nonSuperuserChecks := security49ExerciseRuntimeFunctions(t, ctx, admin)

	t.Run("assertion", func(t *testing.T) {
		if serverMajor != 18 || extensionVersion != "0.3.0" || extensionSchema != "synchro" || otherVersions != 0 || updatePaths != 0 {
			t.Fatalf(
				"clean PostgreSQL 18 baseline is invalid: major=%d version=%q schema=%q other=%d paths=%d",
				serverMajor,
				extensionVersion,
				extensionSchema,
				otherVersions,
				updatePaths,
			)
		}
		if !bytes.Equal(trackedSQL, packagedSQL) {
			t.Fatal("packaged extension SQL differs from its tracked pgrx output")
		}
		if !bytes.Equal(trackedControl, packagedControl) {
			t.Fatal("packaged extension control metadata differs from its tracked pgrx output")
		}
		for _, clause := range []string{
			"default_version = '0.3.0'",
			"relocatable = false",
			"schema = 'synchro'",
		} {
			if !strings.Contains(control, clause) {
				t.Fatalf("extension control metadata omits %q", clause)
			}
		}
		if !nonSuperuserChecks {
			t.Fatal("canonical contract and readiness functions failed under runtime group roles")
		}
	})
}

type security49PublicHealth struct {
	status int
	body   []byte
	detail map[string]any
}

func security49HealthDuringTransaction(t *testing.T, ctx context.Context, database *sql.DB, statements []string) map[string]any {
	t.Helper()
	transaction, err := database.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin health fault transaction: %v", err)
	}
	defer transaction.Rollback()
	for _, statement := range statements {
		if _, err := transaction.ExecContext(ctx, statement); err != nil {
			t.Fatalf("apply health fault: %v", err)
		}
	}
	detail := loadIssue49Health(t, ctx, transaction)
	if err := transaction.Rollback(); err != nil {
		t.Fatalf("roll back health fault: %v", err)
	}
	return detail
}

func security49SetSystemHealthLimit(t *testing.T, ctx context.Context, database *sql.DB, setting string, value int) {
	t.Helper()
	if _, err := database.ExecContext(ctx, fmt.Sprintf("ALTER SYSTEM SET %s = '%d'", setting, value)); err != nil {
		t.Fatalf("set finite health limit %s: %v", setting, err)
	}
	if _, err := database.ExecContext(ctx, "SELECT pg_catalog.pg_reload_conf()"); err != nil {
		t.Fatalf("reload finite health limit %s: %v", setting, err)
	}
}

func security49DefaultHealthLimit(setting string) int {
	switch setting {
	case "synchro.max_wal_lag_bytes":
		return 67_108_864
	default:
		return 30
	}
}

func security49ObservePublicHealth(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	adapterURL string,
	wantCheck string,
) security49PublicHealth {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var result security49PublicHealth
	for time.Now().Before(deadline) {
		result.status, result.body = getIssue49Readiness(t, ctx, adapterURL)
		result.detail = loadIssue49Health(t, ctx, database)
		checks := issue49HealthChecks(t, result.detail)
		if checks[wantCheck] == "failed" && result.detail["ready"] == false &&
			result.status == http.StatusServiceUnavailable && bytes.Equal(result.body, []byte(`{"ready":false}`)) {
			return result
		}
		time.Sleep(25 * time.Millisecond)
	}
	return result
}

func security49AssertFiniteHealthObservations(t *testing.T, detail map[string]any) {
	t.Helper()
	observations, ok := detail["observations"].(map[string]any)
	if !ok {
		t.Fatalf("canonical health observations are missing: %#v", detail)
	}
	for _, name := range []string{"heartbeat_age_seconds", "wal_lag_bytes", "wal_lag_seconds"} {
		value, ok := observations[name].(float64)
		if !ok || value < 0 || value != value || value > 1.7976931348623157e308 {
			t.Fatalf("health observation %q is not known, finite, and nonnegative: %#v", name, observations[name])
		}
	}
}

func security49AssertTriStateChecks(t *testing.T, checks map[string]string) {
	t.Helper()
	if len(checks) != len(issue49HealthCheckNames) {
		t.Fatalf("health fault changed the canonical check set: %#v", checks)
	}
	for name, state := range checks {
		if state != "ok" && state != "failed" && state != "unknown" {
			t.Fatalf("health check %q has invalid state %q", name, state)
		}
	}
}

func security49QuoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func security49ExerciseRuntimeFunctions(t *testing.T, ctx context.Context, database *sql.DB) bool {
	t.Helper()
	checks := []struct {
		role  string
		query string
	}{
		{"synchro_adapter", "SELECT synchro.synchro_contract_info() IS NOT NULL"},
		{"synchro_adapter", "SELECT synchro.synchro_readiness() IS NOT NULL"},
		{"synchro_seed", "SELECT synchro.synchro_schema_manifest() IS NOT NULL"},
		{"synchro_monitor", "SELECT synchro.synchro_health_detail() IS NOT NULL"},
		{"synchro_operator", "SELECT synchro.synchro_health_detail() IS NOT NULL"},
	}
	for _, check := range checks {
		transaction, err := database.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin non-superuser function check: %v", err)
		}
		if _, err := transaction.ExecContext(ctx, "SET LOCAL ROLE "+security49QuoteIdentifier(check.role)); err != nil {
			_ = transaction.Rollback()
			t.Fatalf("activate runtime group %s: %v", check.role, err)
		}
		var valid bool
		err = transaction.QueryRowContext(ctx, check.query).Scan(&valid)
		_ = transaction.Rollback()
		if err != nil || !valid {
			return false
		}
	}
	return true
}

func security49SecretFilesAreExternal(t *testing.T, environment blackbox.EnvironmentConfig) bool {
	t.Helper()
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate security proof source")
	}
	repository := filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "..", "..", ".."))
	paths := []string{
		environment.Admin.PasswordFile,
		environment.Adapter.PasswordFile,
		environment.Observer.PasswordFile,
		environment.Worker.PasswordFile,
		environment.Operator.PasswordFile,
		environment.JWTSecretFile,
	}
	for _, path := range paths {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return false
		}
		relative, err := filepath.Rel(repository, absolute)
		if err != nil || relative == "." || relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return false
		}
		info, err := os.Stat(absolute)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
			return false
		}
	}
	return true
}

func security49LoadCredentialCanaries(t *testing.T, environment blackbox.EnvironmentConfig) []string {
	t.Helper()
	paths := []string{
		environment.Admin.PasswordFile,
		environment.Adapter.PasswordFile,
		environment.Observer.PasswordFile,
		environment.Worker.PasswordFile,
		environment.Operator.PasswordFile,
		environment.JWTSecretFile,
	}
	values := make([]string, 0, len(paths))
	for _, path := range paths {
		value, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read protected credential canary: %v", err)
		}
		values = append(values, strings.TrimSpace(string(value)))
	}
	return values
}

func security49GetBounded(t *testing.T, ctx context.Context, URL string) (int, []byte) {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, URL, nil)
	if err != nil {
		t.Fatalf("create bounded operational request: %v", err)
	}
	response, err := (&http.Client{Timeout: 30 * time.Second}).Do(request)
	if err != nil {
		t.Fatalf("send bounded operational request: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	if err != nil || len(body) > 4096 {
		t.Fatalf("read bounded operational response: size=%d err=%v", len(body), err)
	}
	return response.StatusCode, body
}

func security49InstallationFiles(t *testing.T, artifactRoot, name string) ([]byte, []byte) {
	t.Helper()
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate installation proof source")
	}
	repository := filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "..", "..", ".."))
	trackedPath := filepath.Join(repository, "extensions", "synchro-pg")
	if strings.HasSuffix(name, ".sql") {
		trackedPath = filepath.Join(trackedPath, "sql", name)
	} else {
		trackedPath = filepath.Join(trackedPath, name)
	}
	tracked, err := os.ReadFile(trackedPath)
	if err != nil {
		t.Fatalf("read tracked extension file %s: %v", name, err)
	}
	var candidates []string
	err = filepath.WalkDir(artifactRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() && entry.Name() == name {
			candidates = append(candidates, path)
		}
		return nil
	})
	sort.Strings(candidates)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("locate packaged extension file %s: count=%d err=%v", name, len(candidates), err)
	}
	packaged, err := os.ReadFile(candidates[0])
	if err != nil {
		t.Fatalf("read packaged extension file %s: %v", name, err)
	}
	return tracked, packaged
}

const security49PublicAuthoritySQL = `
	SELECT count(*) FROM (
		SELECT 1
		FROM pg_catalog.pg_namespace namespace
		CROSS JOIN LATERAL pg_catalog.aclexplode(
			COALESCE(namespace.nspacl, pg_catalog.acldefault('n', namespace.nspowner))
		) acl
		WHERE namespace.nspname IN ('synchro', 'synchro_projection') AND acl.grantee = 0
		UNION ALL
		SELECT 1
		FROM pg_catalog.pg_class relation
		JOIN pg_catalog.pg_namespace namespace ON namespace.oid = relation.relnamespace
		CROSS JOIN LATERAL pg_catalog.aclexplode(
			COALESCE(relation.relacl, pg_catalog.acldefault(
				CASE WHEN relation.relkind = 'S' THEN 'S'::"char" ELSE 'r'::"char" END,
				relation.relowner
			))
		) acl
		WHERE namespace.nspname IN ('synchro', 'synchro_projection') AND acl.grantee = 0
		UNION ALL
		SELECT 1
		FROM pg_catalog.pg_proc procedure
		JOIN pg_catalog.pg_namespace namespace ON namespace.oid = procedure.pronamespace
		CROSS JOIN LATERAL pg_catalog.aclexplode(
			COALESCE(procedure.proacl, pg_catalog.acldefault('f', procedure.proowner))
		) acl
		WHERE namespace.nspname = 'synchro' AND acl.grantee = 0
		UNION ALL
		SELECT 1
		FROM pg_catalog.pg_type type
		JOIN pg_catalog.pg_namespace namespace ON namespace.oid = type.typnamespace
		LEFT JOIN pg_catalog.pg_class composite ON composite.oid = type.typrelid
		CROSS JOIN LATERAL pg_catalog.aclexplode(
			COALESCE(type.typacl, pg_catalog.acldefault('T', type.typowner))
		) acl
		WHERE namespace.nspname = 'synchro'
		  AND (type.typrelid = 0 OR composite.relkind = 'c')
		  AND NOT (type.typelem <> 0 AND type.typlen = -1)
		  AND acl.grantee = 0
	) exposed`

const security49UnexpectedFunctionAuthoritySQL = `
	WITH allowed(role_name, function_name) AS (
		VALUES
		('synchro_adapter', 'synchro_contract_info'),
		('synchro_adapter', 'synchro_connect'),
		('synchro_adapter', 'synchro_pull'),
		('synchro_adapter', 'synchro_push'),
		('synchro_adapter', 'synchro_rebuild'),
		('synchro_adapter', 'synchro_schema_manifest'),
		('synchro_adapter', 'synchro_tables'),
		('synchro_adapter', 'synchro_readiness'),
		('synchro_adapter', 'synchro_build_fingerprint'),
		('synchro_seed', 'synchro_schema_manifest'),
		('synchro_seed', 'synchro_portable_seed_manifest'),
		('synchro_seed', 'synchro_portable_seed_scope'),
		('synchro_monitor', 'synchro_readiness'),
		('synchro_monitor', 'synchro_health_detail'),
		('synchro_operator', 'synchro_register_table'),
		('synchro_operator', 'synchro_register_capture_dependency'),
		('synchro_operator', 'synchro_prepare_projection_view'),
		('synchro_operator', 'synchro_register_membership_dependency'),
		('synchro_operator', 'synchro_unregister_table'),
		('synchro_operator', 'synchro_register_shared_scope'),
		('synchro_operator', 'synchro_unregister_shared_scope'),
		('synchro_operator', 'synchro_grant_user_scope'),
		('synchro_operator', 'synchro_revoke_user_scope'),
		('synchro_operator', 'synchro_backfill_bucket_edges'),
		('synchro_operator', 'synchro_compact'),
		('synchro_operator', 'synchro_inject_client_retention_expiry'),
		('synchro_operator', 'synchro_retry_wal_poison'),
		('synchro_operator', 'synchro_health_detail'),
		('synchro_operator', 'synchro_debug'),
		('synchro_operator', 'synchro_primary_key_guard'),
		('synchro_operator', 'synchro_capture_fence'),
		('synchro_operator', 'synchro_prepare_stream_reset'),
		('synchro_operator', 'synchro_lock_stream_reset_sources'),
		('synchro_operator', 'synchro_mark_stream_reset_snapshot'),
		('synchro_operator', 'synchro_stage_stream_reset'),
		('synchro_operator', 'synchro_activate_stream_reset'),
		('synchro_operator', 'synchro_abort_stream_reset'),
		('synchro_operator', 'synchro_complete_stream_reset_cleanup'),
		('synchro_operator', 'synchro_prepare_projection_bootstrap'),
		('synchro_operator', 'synchro_stage_projection_bootstrap'),
		('synchro_operator', 'synchro_emit_projection_bootstrap_barrier'),
		('synchro_operator', 'synchro_request_projection_bootstrap_barrier'),
		('synchro_operator', 'synchro_activate_projection_bootstrap'),
		('synchro_operator', 'synchro_projection_bootstrap_status'),
		('synchro_operator', 'synchro_abort_projection_bootstrap'),
		('synchro_operator', 'synchro_complete_projection_bootstrap_cleanup'),
		('synchro_operator', 'synchro_projection_bootstrap_slot_drop_state'),
		('synchro_worker', 'synchro_projection_bootstrap_active_stream'),
		('synchro_worker', 'synchro_projection_bootstrap_main_boundary'),
		('synchro_worker', 'synchro_projection_bootstrap_slot_absent'),
		('synchro_worker', 'synchro_projection_bootstrap_slot_drop_state'),
		('synchro_worker', 'synchro_projection_bootstrap_next_aborted_slot'),
		('synchro_worker', 'synchro_projection_bootstrap_is_activated'),
		('synchro_worker', 'synchro_projection_bootstrap_interrupted')
	), direct_grants AS (
		SELECT grantee.rolname AS role_name, procedure.proname AS function_name
		FROM pg_catalog.pg_proc procedure
		JOIN pg_catalog.pg_namespace namespace ON namespace.oid = procedure.pronamespace
		CROSS JOIN LATERAL pg_catalog.aclexplode(
			COALESCE(procedure.proacl, pg_catalog.acldefault('f', procedure.proowner))
		) acl
		JOIN pg_catalog.pg_roles grantee ON grantee.oid = acl.grantee
		WHERE namespace.nspname = 'synchro'
		  AND acl.privilege_type = 'EXECUTE'
		  AND grantee.rolname IN (
			'synchro_adapter', 'synchro_seed', 'synchro_monitor', 'synchro_operator', 'synchro_worker'
		  )
	)
	SELECT count(*)
	FROM direct_grants grant_entry
	LEFT JOIN allowed USING (role_name, function_name)
	WHERE allowed.role_name IS NULL`
