package integration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

// registryKeyTypeCase is one key column type. An empty portableType means
// that the registry must reject the type.
type registryKeyTypeCase struct {
	label        string
	sqlType      string
	portableType string
}

// registryKeyTypeKind is one registration kind. The setup is a format string
// with the relation name as %[1]s, the key column type as %[2]s, and the
// primary key attributes as %[3]s.
type registryKeyTypeKind struct {
	prefix   string
	setup    string
	register string
}

var registryKeyTypeCases = []registryKeyTypeCase{
	{label: "smallint", sqlType: "smallint", portableType: "int"},
	{label: "integer", sqlType: "integer", portableType: "int"},
	{label: "bigint", sqlType: "bigint", portableType: "int64"},
	{label: "text", sqlType: "text", portableType: "string"},
	{label: "varchar", sqlType: "character varying(16)", portableType: "string"},
	{label: "bpchar", sqlType: "character(8)", portableType: "string"},
	{label: "uuid", sqlType: "uuid", portableType: "string"},
	{label: "inet", sqlType: "inet", portableType: "string"},
	{label: "cidr", sqlType: "cidr", portableType: "string"},
	{label: "macaddr", sqlType: "macaddr", portableType: "string"},
	{label: "macaddr8", sqlType: "macaddr8", portableType: "string"},
	{label: "int4range", sqlType: "int4range", portableType: "string"},
	{label: "int8range", sqlType: "int8range", portableType: "string"},
	{label: "numrange", sqlType: "numrange", portableType: "string"},
	{label: "int4multirange", sqlType: "int4multirange", portableType: "string"},
	{label: "int8multirange", sqlType: "int8multirange", portableType: "string"},
	{label: "nummultirange", sqlType: "nummultirange", portableType: "string"},
	{label: "interval", sqlType: "interval"},
	{label: "daterange", sqlType: "daterange"},
	{label: "tsrange", sqlType: "tsrange"},
	{label: "tstzrange", sqlType: "tstzrange"},
	{label: "datemultirange", sqlType: "datemultirange"},
	{label: "tsmultirange", sqlType: "tsmultirange"},
	{label: "tstzmultirange", sqlType: "tstzmultirange"},
	{label: "floatrange", sqlType: "key_types.floatrange"},
	{label: "labelrange", sqlType: "key_types.labelrange"},
}

var registryKeyTypeKinds = []registryKeyTypeKind{
	{
		prefix: "synced_",
		setup: `
			CREATE TABLE key_types.%[1]s (
				id %[2]s PRIMARY KEY%[3]s,
				owner_id text NOT NULL,
				value text NOT NULL,
				updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
				deleted_at timestamptz
			);
			ALTER TABLE key_types.%[1]s ENABLE ROW LEVEL SECURITY;
			CREATE POLICY synchro_owner_all ON key_types.%[1]s
				AS PERMISSIVE FOR ALL TO synchro_owner USING (true) WITH CHECK (true);
			GRANT SELECT, INSERT, UPDATE ON TABLE key_types.%[1]s TO synchro_owner;
			GRANT SELECT ON TABLE key_types.%[1]s TO synchro_worker;
			CREATE FUNCTION key_types.%[1]s_membership(p_id %[2]s)
			RETURNS SETOF text
			LANGUAGE SQL STABLE SECURITY INVOKER
			SET search_path = pg_catalog, synchro
			BEGIN ATOMIC SELECT 'user:key-types'::text; END;
			REVOKE ALL ON FUNCTION key_types.%[1]s_membership FROM PUBLIC;
			GRANT EXECUTE ON FUNCTION key_types.%[1]s_membership TO synchro_owner, synchro_worker`,
		register: `SELECT synchro.synchro_register_table(
			'key_types.' || $1::text, 'key_types.' || $1::text || '_membership', 'single_scope',
			'id', 'updated_at', 'deleted_at', 'enabled')`,
	},
	{
		prefix: "capture_",
		setup: `
			CREATE TABLE key_types.%[1]s (
				id %[2]s PRIMARY KEY%[3]s,
				scope_key text NOT NULL
			);
			ALTER TABLE key_types.%[1]s ENABLE ROW LEVEL SECURITY;
			CREATE POLICY synchro_owner_all ON key_types.%[1]s
				AS PERMISSIVE FOR ALL TO synchro_owner USING (true) WITH CHECK (true);
			GRANT SELECT ON TABLE key_types.%[1]s TO synchro_owner, synchro_worker`,
		register: `SELECT synchro.synchro_register_capture_dependency(
			'key_types.' || $1::text, ARRAY['id']::text[], ARRAY['scope_key']::text[])`,
	},
}

// TestRealRegistryAcceptsOnlyKeyTypesWithOneTextForm proves SYNC-REGISTRY-002
// for the registered key type list.
func TestRealRegistryAcceptsOnlyKeyTypesWithOneTextForm(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	waitForIssue49CanonicalHealth(t, ctx, admin, true)

	if _, err := admin.ExecContext(ctx, `
		CREATE SCHEMA key_types;
		GRANT USAGE ON SCHEMA key_types TO synchro_owner, synchro_worker;
		CREATE TYPE key_types.floatrange AS RANGE (subtype = float8);
		CREATE DOMAIN key_types.labelrange AS text`); err != nil {
		t.Fatalf("create key type schema: %v", err)
	}
	for _, keyType := range registryKeyTypeCases {
		for _, kind := range registryKeyTypeKinds {
			relation := kind.prefix + keyType.label
			if _, err := admin.ExecContext(ctx, fmt.Sprintf(kind.setup, relation, keyType.sqlType, "")); err != nil {
				t.Fatalf("create key type relation %s: %v", relation, err)
			}
		}
	}

	t.Run("assertion", func(t *testing.T) {
		for _, keyType := range registryKeyTypeCases {
			for _, kind := range registryKeyTypeKinds {
				relation := kind.prefix + keyType.label
				before := latestRegistryGeneration(t, ctx, admin)
				_, registerErr := admin.ExecContext(ctx, kind.register, relation)
				after := latestRegistryGeneration(t, ctx, admin)
				rows, portableType := registryKeyTypeRows(t, ctx, admin, after, relation)
				if keyType.portableType == "" {
					if registerErr == nil || after != before || rows != 0 {
						t.Errorf(
							"rejected key type %s registered for %s: error=%v generation=%d->%d rows=%d",
							keyType.sqlType, relation, registerErr, before, after, rows,
						)
					}
					continue
				}
				if registerErr != nil || after <= before || rows != 1 || portableType != keyType.portableType {
					t.Errorf(
						"accepted key type %s did not register for %s: error=%v generation=%d->%d rows=%d portable=%q want %q",
						keyType.sqlType, relation, registerErr, before, after, rows, portableType, keyType.portableType,
					)
				}
			}
		}
	})
}

// TestRealRegistryRejectsDeferrablePrimaryKey proves SYNC-REGISTRY-002 for a
// deferrable primary key. The uuid case of
// TestRealRegistryAcceptsOnlyKeyTypesWithOneTextForm is the accepted control.
func TestRealRegistryRejectsDeferrablePrimaryKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	admin := openIssue49Admin(t, ctx, harness)
	waitForIssue49CanonicalHealth(t, ctx, admin, true)

	if _, err := admin.ExecContext(ctx, `
		CREATE SCHEMA key_types;
		GRANT USAGE ON SCHEMA key_types TO synchro_owner, synchro_worker`); err != nil {
		t.Fatalf("create deferrable key schema: %v", err)
	}
	for _, kind := range registryKeyTypeKinds {
		relation := kind.prefix + "deferrable"
		if _, err := admin.ExecContext(ctx, fmt.Sprintf(kind.setup, relation, "uuid", " DEFERRABLE")); err != nil {
			t.Fatalf("create deferrable key relation %s: %v", relation, err)
		}
	}

	t.Run("assertion", func(t *testing.T) {
		for _, kind := range registryKeyTypeKinds {
			relation := kind.prefix + "deferrable"
			before := latestRegistryGeneration(t, ctx, admin)
			_, registerErr := admin.ExecContext(ctx, kind.register, relation)
			after := latestRegistryGeneration(t, ctx, admin)
			rows, _ := registryKeyTypeRows(t, ctx, admin, after, relation)
			if registerErr == nil || after != before || rows != 0 {
				t.Errorf(
					"deferrable primary key registered for %s: error=%v generation=%d->%d rows=%d",
					relation, registerErr, before, after, rows,
				)
			}
		}
	})
}

func latestRegistryGeneration(t *testing.T, ctx context.Context, admin *sql.DB) int64 {
	t.Helper()
	var generation int64
	if err := admin.QueryRowContext(ctx, "SELECT max(generation) FROM synchro.sync_registry_generations").Scan(&generation); err != nil {
		t.Fatalf("load latest registry generation: %v", err)
	}
	return generation
}

func registryKeyTypeRows(t *testing.T, ctx context.Context, admin *sql.DB, generation int64, relation string) (int64, string) {
	t.Helper()
	var rows int64
	var portableType string
	if err := admin.QueryRowContext(ctx, `
		SELECT count(*), COALESCE(max(registry.pk_portable_type), '')
		FROM synchro.sync_registry registry
		WHERE registry.registry_generation = $1
		  AND registry.physical_schema = 'key_types'
		  AND registry.physical_relation = $2::text::name`,
		generation, relation,
	).Scan(&rows, &portableType); err != nil {
		t.Fatalf("load key type registration %s: %v", relation, err)
	}
	return rows, portableType
}
