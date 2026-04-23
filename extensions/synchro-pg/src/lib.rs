use std::ffi::CString;

use pgrx::prelude::*;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

mod bgworker;
mod bucketing;
mod client;
mod compaction;
mod cursor_token;
mod materialize;
mod portable_seed;
mod pull;
mod push;
mod rebuild;
mod registry;
mod schema;
mod wal_decoder;

pgrx::pg_module_magic!();

// ---------------------------------------------------------------------------
// Infrastructure tables (included in generated extension SQL)
// ---------------------------------------------------------------------------

pgrx::extension_sql!(
    r#"
CREATE TABLE IF NOT EXISTS sync_registry (
    table_name TEXT PRIMARY KEY,
    bucket_sql TEXT NOT NULL,
    pk_column TEXT NOT NULL DEFAULT 'id',
    pk_type TEXT NOT NULL DEFAULT 'uuid',
    updated_at_col TEXT NOT NULL DEFAULT 'updated_at',
    deleted_at_col TEXT NOT NULL DEFAULT 'deleted_at',
    push_policy TEXT NOT NULL DEFAULT 'enabled',
    sync_columns TEXT[] NOT NULL DEFAULT '{}',
    exclude_columns TEXT[] NOT NULL DEFAULT '{}',
    has_updated_at BOOLEAN NOT NULL DEFAULT true,
    has_deleted_at BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE sync_registry
    ADD COLUMN IF NOT EXISTS sync_columns TEXT[] NOT NULL DEFAULT '{}';

CREATE TABLE IF NOT EXISTS sync_changelog (
    seq BIGSERIAL PRIMARY KEY,
    bucket_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    record_id TEXT NOT NULL,
    operation SMALLINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sync_changelog_bucket_seq ON sync_changelog (bucket_id, seq);
CREATE INDEX IF NOT EXISTS idx_sync_changelog_record ON sync_changelog (table_name, record_id);

CREATE TABLE IF NOT EXISTS sync_clients (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    client_name TEXT,
    platform TEXT NOT NULL DEFAULT '',
    app_version TEXT NOT NULL DEFAULT '',
    bucket_subs TEXT[] NOT NULL DEFAULT '{}',
    scope_set_version BIGINT NOT NULL DEFAULT 1,
    last_sync_at TIMESTAMPTZ,
    last_pull_at TIMESTAMPTZ,
    last_push_at TIMESTAMPTZ,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, client_id)
);
ALTER TABLE sync_clients
    ADD COLUMN IF NOT EXISTS scope_set_version BIGINT NOT NULL DEFAULT 1;
CREATE INDEX IF NOT EXISTS idx_sync_clients_user_id ON sync_clients (user_id);

CREATE TABLE IF NOT EXISTS sync_shared_scopes (
    scope_id TEXT PRIMARY KEY,
    portable BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sync_bucket_edges (
    table_name TEXT NOT NULL,
    record_id TEXT NOT NULL,
    bucket_id TEXT NOT NULL,
    checksum INTEGER,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (table_name, record_id, bucket_id)
);
CREATE INDEX IF NOT EXISTS idx_sync_bucket_edges_bucket ON sync_bucket_edges (bucket_id, table_name, record_id);

CREATE TABLE IF NOT EXISTS sync_rule_failures (
    id BIGSERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    record_id TEXT NOT NULL,
    operation SMALLINT NOT NULL,
    error_text TEXT NOT NULL,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sync_rule_failures_created ON sync_rule_failures (created_at);

CREATE TABLE IF NOT EXISTS sync_schema_manifest (
    schema_version BIGINT PRIMARY KEY,
    schema_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_sync_schema_manifest_hash ON sync_schema_manifest (schema_hash);

CREATE TABLE IF NOT EXISTS sync_client_checkpoints (
    user_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    bucket_id TEXT NOT NULL,
    checkpoint BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, client_id, bucket_id)
);

CREATE TABLE IF NOT EXISTS sync_runtime_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    stream_generation TEXT NOT NULL,
    cursor_secret TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO sync_runtime_state (singleton, stream_generation, cursor_secret)
VALUES (
    true,
    gen_random_uuid()::text,
    replace(gen_random_uuid()::text, '-', '') || replace(gen_random_uuid()::text, '-', '')
)
ON CONFLICT (singleton) DO NOTHING;
"#,
    name = "create_infrastructure_tables",
    bootstrap
);

// ---------------------------------------------------------------------------
// GUC settings (readable from all modules via crate::*)
// ---------------------------------------------------------------------------

/// Name of the logical replication slot. Defaults to "synchro_slot" when NULL.
pub(crate) static REPLICATION_SLOT_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

/// Name of the WAL publication. Defaults to "synchro_pub" when NULL.
pub(crate) static PUBLICATION_NAME_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

/// Database the WAL background worker should connect to.
pub(crate) static DATABASE_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

/// Whether to auto-start the WAL background worker on server boot.
pub(crate) static AUTO_START_GUC: GucSetting<bool> = GucSetting::<bool>::new(true);

/// Clock skew tolerance for LWW conflict resolution (milliseconds).
pub(crate) static CLOCK_SKEW_TOLERANCE_MS_GUC: GucSetting<i32> = GucSetting::<i32>::new(500);

/// Extension initialization. Called when the shared library is loaded.
///
/// Registers all GUCs and conditionally starts the WAL background worker.
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    GucRegistry::define_string_guc(
        c"synchro.replication_slot",
        c"Name of the logical replication slot used by synchro.",
        c"Name of the logical replication slot used by the synchro WAL consumer. Defaults to synchro_slot.",
        &REPLICATION_SLOT_GUC,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"synchro.publication_name",
        c"Name of the WAL publication used by synchro.",
        c"Name of the PostgreSQL publication used by the synchro WAL consumer. Defaults to synchro_pub.",
        &PUBLICATION_NAME_GUC,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"synchro.database",
        c"Database name for the WAL background worker.",
        c"Database the WAL consumer connects to. Defaults to postgres.",
        &DATABASE_GUC,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"synchro.auto_start",
        c"Whether to auto-start the synchro WAL background worker.",
        c"When true, the WAL consumer background worker starts on server boot.",
        &AUTO_START_GUC,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"synchro.clock_skew_tolerance_ms",
        c"Clock skew tolerance for LWW conflict resolution (ms).",
        c"Milliseconds of clock skew tolerance for last-write-wins conflict resolution.",
        &CLOCK_SKEW_TOLERANCE_MS_GUC,
        0,
        60000,
        GucContext::Userset,
        GucFlags::default(),
    );

    let in_shared_preload = unsafe { pg_sys::process_shared_preload_libraries_in_progress };
    if AUTO_START_GUC.get() && in_shared_preload {
        bgworker::register_bgworker();
    }
}

// ---------------------------------------------------------------------------
// Test helpers and integration tests
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;
    use serde_json::json;
    use serde_json::Value;

    // -----------------------------------------------------------------------
    // Shared test setup
    // -----------------------------------------------------------------------

    /// Create test tables and register them for sync.
    fn setup_test_tables() {
        // orders: standard table with timestamps, user_id for bucketing
        Spi::run(
            "CREATE TABLE IF NOT EXISTS test_orders (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id TEXT NOT NULL,
                title TEXT NOT NULL DEFAULT '',
                amount NUMERIC DEFAULT 0,
                internal_notes TEXT DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                deleted_at TIMESTAMPTZ
            )",
        )
        .unwrap();

        // products: read-only reference data
        Spi::run(
            "CREATE TABLE IF NOT EXISTS test_products (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT NOT NULL DEFAULT '',
                price NUMERIC DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                deleted_at TIMESTAMPTZ
            )",
        )
        .unwrap();

        // bare_items: no timestamps (no updated_at, no deleted_at)
        Spi::run(
            "CREATE TABLE IF NOT EXISTS test_bare_items (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT NOT NULL DEFAULT ''
            )",
        )
        .unwrap();

        // Register tables for sync. Bucket SQL must cast $1 to the PK type
        // explicitly because SPI prepared statements pass text parameters and
        // PG does not implicit-cast text to uuid.
        Spi::run(
            "SELECT synchro_register_table(
                'test_orders',
                $$SELECT ARRAY['user:' || user_id] FROM test_orders WHERE id = $1::uuid$$,
                'id', 'updated_at', 'deleted_at', 'enabled',
                ARRAY['internal_notes']
            )",
        )
        .unwrap();

        Spi::run(
            "SELECT synchro_register_table(
                'test_products',
                $$SELECT ARRAY['global'] FROM test_products WHERE id = $1::uuid$$,
                'id', 'updated_at', 'deleted_at', 'read_only'
            )",
        )
        .unwrap();

        Spi::run(
            "SELECT synchro_register_table(
                'test_bare_items',
                $$SELECT ARRAY['global'] FROM test_bare_items WHERE id = $1::uuid$$,
                'id', 'updated_at', 'deleted_at', 'enabled'
            )",
        )
        .unwrap();
    }

    fn setup_sync_columns_table() {
        setup_test_tables();

        Spi::run(
            "CREATE TABLE IF NOT EXISTS test_sync_columns_items (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id TEXT NOT NULL,
                title TEXT NOT NULL DEFAULT '',
                search_vector TEXT DEFAULT '',
                internal_notes TEXT DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                deleted_at TIMESTAMPTZ
            )",
        )
        .unwrap();
        Spi::run(
            "CREATE INDEX IF NOT EXISTS idx_test_sync_columns_items_title
             ON test_sync_columns_items (title)",
        )
        .unwrap();
        Spi::run(
            "CREATE INDEX IF NOT EXISTS idx_test_sync_columns_items_search_vector
             ON test_sync_columns_items (search_vector)",
        )
        .unwrap();
        Spi::run(
            "SELECT synchro_register_table(
                'test_sync_columns_items',
                $$SELECT ARRAY['user:' || user_id] FROM test_sync_columns_items WHERE id = $1::uuid$$,
                'id',
                'updated_at',
                'deleted_at',
                'enabled',
                ARRAY[]::text[],
                ARRAY['id', 'user_id', 'title', 'updated_at', 'deleted_at']
            )",
        )
        .unwrap();
    }

    fn setup_portable_type_contract_table() {
        setup_test_tables();

        Spi::run(
            "CREATE TABLE IF NOT EXISTS test_portable_type_contract (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id TEXT NOT NULL,
                label TEXT NOT NULL DEFAULT '',
                col_smallint SMALLINT,
                col_integer INTEGER,
                col_bigint BIGINT,
                col_numeric NUMERIC(5,1),
                col_real REAL,
                col_double DOUBLE PRECISION,
                col_timestamp TIMESTAMPTZ,
                col_interval INTERVAL,
                col_json JSONB,
                col_blob BYTEA,
                col_text_array TEXT[],
                col_int_array INTEGER[],
                col_inet INET,
                col_point POINT,
                col_int4range INT4RANGE,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                deleted_at TIMESTAMPTZ
            )",
        )
        .unwrap();

        Spi::run(
            "SELECT synchro_register_table(
                p_table_name := 'test_portable_type_contract',
                p_bucket_sql := $$SELECT ARRAY['user:' || user_id] FROM test_portable_type_contract WHERE id = $1::uuid$$,
                p_pk_column := 'id',
                p_updated_at_col := 'updated_at',
                p_deleted_at_col := 'deleted_at',
                p_push_policy := 'enabled',
                p_sync_columns := ARRAY[
                    'id',
                    'user_id',
                    'label',
                    'col_smallint',
                    'col_integer',
                    'col_bigint',
                    'col_numeric',
                    'col_real',
                    'col_double',
                    'col_timestamp',
                    'col_interval',
                    'col_json',
                    'col_blob',
                    'col_text_array',
                    'col_int_array',
                    'col_inet',
                    'col_point',
                    'col_int4range',
                    'updated_at',
                    'deleted_at'
                ]
            )",
        )
        .unwrap();
    }

    /// Register a test client and return the raw JSONB response.
    fn register_client(user_id: &str, client_id: &str) -> Value {
        connect_client(
            user_id,
            json!({
                "client_id": client_id,
                "platform": "test",
                "app_version": "1.0.0",
                "protocol_version": 2,
                "schema": { "version": 0, "hash": "" },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        )
    }

    /// Execute a canonical connect request and return the raw JSONB response.
    fn connect_client(user_id: &str, request: Value) -> Value {
        let row: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_connect($1, $2::jsonb)",
            &[user_id.into(), request.to_string().into()],
        )
        .unwrap();
        row.unwrap().0
    }

    fn register_shared_scope(scope_id: &str, portable: bool) {
        Spi::run_with_args(
            "SELECT synchro_register_shared_scope($1, $2)",
            &[scope_id.into(), portable.into()],
        )
        .unwrap();
    }

    fn push_client(user_id: &str, client_id: &str, batch_id: &str, mutations: Value) -> Value {
        let (schema_version, schema_hash) = latest_schema_ref();
        let row: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2::jsonb)",
            &[
                user_id.into(),
                json!({
                    "client_id": client_id,
                    "batch_id": batch_id,
                    "schema": { "version": schema_version, "hash": schema_hash },
                    "mutations": mutations
                })
                .to_string()
                .into(),
            ],
        )
        .unwrap();
        row.unwrap().0
    }

    fn pull_client(
        user_id: &str,
        client_id: &str,
        scope_set_version: i64,
        scopes: Value,
        limit: i32,
    ) -> Value {
        let (schema_version, schema_hash) = latest_schema_ref();
        let request = json!({
            "client_id": client_id,
            "schema": { "version": schema_version, "hash": schema_hash },
            "scope_set_version": scope_set_version,
            "scopes": scopes,
            "limit": limit,
        });

        let row: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2::jsonb)",
            &[user_id.into(), request.to_string().into()],
        )
        .unwrap();
        row.unwrap().0
    }

    fn rebuild_client(
        user_id: &str,
        client_id: &str,
        scope: &str,
        cursor: Option<&str>,
        limit: i32,
    ) -> Value {
        let row: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_rebuild($1, $2::jsonb)",
            &[
                user_id.into(),
                json!({
                    "client_id": client_id,
                    "scope": scope,
                    "cursor": cursor,
                    "limit": limit
                })
                .to_string()
                .into(),
            ],
        )
        .unwrap();
        row.unwrap().0
    }

    fn client_scope_ids(user_id: &str, client_id: &str) -> Vec<String> {
        Spi::get_one_with_args(
            "SELECT bucket_subs FROM sync_clients WHERE user_id = $1 AND client_id = $2",
            &[user_id.into(), client_id.into()],
        )
        .unwrap()
        .unwrap_or_default()
    }

    /// Return the latest schema version and hash persisted by the extension.
    fn latest_schema_ref() -> (i64, String) {
        let row: Option<pgrx::JsonB> = Spi::get_one(
            "SELECT jsonb_build_object(
                'version', schema_version,
                'hash', schema_hash
             )
             FROM sync_schema_manifest
             ORDER BY schema_version DESC
             LIMIT 1",
        )
        .unwrap();
        let row = row.expect("schema manifest row");
        let version = row.0["version"].as_i64().unwrap_or(0);
        let hash = row.0["hash"].as_str().unwrap_or_default().to_string();
        (version, hash)
    }

    fn issued_scope_cursor(scope_id: &str, checkpoint: i64) -> String {
        Spi::connect(|client| crate::cursor_token::issue_scope_cursor(client, scope_id, checkpoint))
            .expect("issue scope cursor")
    }

    fn scope_cursor_ref(scope_id: &str, checkpoint: i64) -> Value {
        json!({ "cursor": issued_scope_cursor(scope_id, checkpoint) })
    }

    /// Insert a changelog entry directly for test fixtures.
    fn insert_changelog(bucket_id: &str, table_name: &str, record_id: &str, operation: i16) {
        Spi::run_with_args(
            "INSERT INTO sync_changelog (bucket_id, table_name, record_id, operation) \
             VALUES ($1, $2, $3, $4)",
            &[
                bucket_id.into(),
                table_name.into(),
                record_id.into(),
                operation.into(),
            ],
        )
        .unwrap();
    }

    /// Insert a bucket edge directly for test fixtures.
    fn insert_edge(table_name: &str, record_id: &str, bucket_id: &str) {
        Spi::run_with_args(
            "INSERT INTO sync_bucket_edges (table_name, record_id, bucket_id, checksum) \
             VALUES ($1, $2, $3, 12345) \
             ON CONFLICT DO NOTHING",
            &[table_name.into(), record_id.into(), bucket_id.into()],
        )
        .unwrap();
    }

    // -----------------------------------------------------------------------
    // Smoke test
    // -----------------------------------------------------------------------

    #[pg_test]
    fn test_extension_loads() {
        assert!(true);
    }

    // -----------------------------------------------------------------------
    // Registration (5 tests)
    // -----------------------------------------------------------------------

    #[pg_test]
    fn test_register_table_basic() {
        setup_test_tables();
        let count: Option<i64> =
            Spi::get_one("SELECT count(*) FROM sync_registry WHERE table_name = 'test_orders'")
                .unwrap();
        assert_eq!(count, Some(1));
    }

    #[pg_test]
    fn test_register_table_creates_publication() {
        setup_test_tables();
        let in_pub: Option<bool> = Spi::get_one(
            "SELECT EXISTS (
                SELECT 1 FROM pg_publication_tables
                WHERE pubname = 'synchro_pub' AND tablename = 'test_orders'
            )",
        )
        .unwrap();
        assert_eq!(in_pub, Some(true));
    }

    #[pg_test]
    fn test_register_table_schema_manifest() {
        setup_test_tables();
        let version: Option<i64> =
            Spi::get_one("SELECT MAX(schema_version) FROM sync_schema_manifest").unwrap();
        assert!(version.unwrap_or(0) > 0);
    }

    #[pg_test]
    fn test_register_table_for_all_publication() {
        Spi::run("DROP PUBLICATION IF EXISTS synchro_pub").unwrap();
        Spi::run("CREATE PUBLICATION synchro_pub FOR ALL TABLES").unwrap();
        Spi::run(
            "CREATE TABLE IF NOT EXISTS test_for_all_publication_items (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id TEXT NOT NULL,
                title TEXT NOT NULL DEFAULT '',
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                deleted_at TIMESTAMPTZ
            )",
        )
        .unwrap();

        Spi::run(
            "SELECT synchro_register_table(
                'test_for_all_publication_items',
                $$SELECT ARRAY['user:' || user_id] FROM test_for_all_publication_items WHERE id = $1::uuid$$,
                'id', 'updated_at', 'deleted_at', 'read_only'
            )",
        )
        .unwrap();

        let in_registry: Option<bool> = Spi::get_one(
            "SELECT EXISTS (
                SELECT 1 FROM sync_registry
                WHERE table_name = 'test_for_all_publication_items'
            )",
        )
        .unwrap();
        assert_eq!(in_registry, Some(true));

        let pub_all_tables: Option<bool> =
            Spi::get_one("SELECT puballtables FROM pg_publication WHERE pubname = 'synchro_pub'")
                .unwrap();
        assert_eq!(pub_all_tables, Some(true));
    }

    #[pg_test]
    fn test_register_table_rejects_conflicting_column_selection() {
        setup_test_tables();

        let result = std::panic::catch_unwind(|| {
            Spi::run(
                "SELECT synchro_register_table(
                    'test_orders',
                    $$SELECT ARRAY['user:' || user_id] FROM test_orders WHERE id = $1::uuid$$,
                    'id',
                    'updated_at',
                    'deleted_at',
                    'enabled',
                    ARRAY['internal_notes'],
                    ARRAY['id', 'user_id', 'title', 'updated_at', 'deleted_at']
                )",
            )
            .unwrap();
        });

        assert!(
            result.is_err(),
            "registration should reject simultaneous sync_columns and exclude_columns"
        );
    }

    #[pg_test]
    fn test_unregister_table_for_all_publication() {
        setup_test_tables();
        Spi::run("DROP PUBLICATION IF EXISTS synchro_pub").unwrap();
        Spi::run("CREATE PUBLICATION synchro_pub FOR ALL TABLES").unwrap();

        Spi::run("SELECT synchro_unregister_table('test_orders')").unwrap();

        let in_registry: Option<bool> = Spi::get_one(
            "SELECT EXISTS (
                SELECT 1 FROM sync_registry
                WHERE table_name = 'test_orders'
            )",
        )
        .unwrap();
        assert_eq!(in_registry, Some(false));

        let pub_all_tables: Option<bool> =
            Spi::get_one("SELECT puballtables FROM pg_publication WHERE pubname = 'synchro_pub'")
                .unwrap();
        assert_eq!(pub_all_tables, Some(true));
    }

    #[pg_test]
    fn test_unregister_cleanup() {
        setup_test_tables();
        // Insert an edge for test_bare_items.
        insert_edge("test_bare_items", "fake-id", "global");

        Spi::run("SELECT synchro_unregister_table('test_bare_items')").unwrap();

        let reg_count: Option<i64> =
            Spi::get_one("SELECT count(*) FROM sync_registry WHERE table_name = 'test_bare_items'")
                .unwrap();
        assert_eq!(reg_count, Some(0));

        let edge_count: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM sync_bucket_edges WHERE table_name = 'test_bare_items'",
        )
        .unwrap();
        assert_eq!(edge_count, Some(0));
    }

    #[pg_test]
    fn test_register_client_bucket_subs() {
        setup_test_tables();
        let resp = register_client("user1", "client1");
        let server_time = resp.get("server_time").and_then(|v| v.as_str()).unwrap();
        assert!(chrono::DateTime::parse_from_rfc3339(server_time).is_ok());
        assert_eq!(
            resp["schema"]
                .get("version")
                .and_then(|v| v.as_i64())
                .unwrap_or(0)
                > 0,
            true
        );
        let added_scopes = resp["scopes"]["add"].as_array().unwrap();
        assert!(added_scopes
            .iter()
            .any(|scope| scope["id"].as_str() == Some("user:user1")));

        // Verify bucket_subs in sync_clients.
        let subs: Option<Vec<String>> = Spi::get_one_with_args(
            "SELECT bucket_subs FROM sync_clients WHERE user_id = $1 AND client_id = $2",
            &["user1".into(), "client1".into()],
        )
        .unwrap();
        let subs = subs.unwrap();
        assert!(subs.contains(&"user:user1".to_string()));
        assert_eq!(subs.len(), 1);
    }

    #[pg_test]
    fn test_connect_includes_registered_shared_scopes() {
        setup_test_tables();
        register_shared_scope("catalog", true);

        let first = connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 2,
                "schema": {
                    "version": 0,
                    "hash": ""
                },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        );

        assert_eq!(first["scope_set_version"].as_i64(), Some(1));
        let first_added_scopes = first["scopes"]["add"].as_array().unwrap();
        let first_added_ids: Vec<&str> = first_added_scopes
            .iter()
            .filter_map(|scope| scope["id"].as_str())
            .collect();
        assert!(first_added_ids.contains(&"user:user1"));
        assert!(first_added_ids.contains(&"catalog"));

        register_shared_scope("runtime-only", false);

        let (schema_version, schema_hash) = latest_schema_ref();
        let second = connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 2,
                "schema": {
                    "version": schema_version,
                    "hash": schema_hash
                },
                "scope_set_version": 1,
                "known_scopes": {
                    "user:user1": scope_cursor_ref("user:user1", 1),
                    "catalog": scope_cursor_ref("catalog", 1)
                }
            }),
        );

        assert_eq!(second["scope_set_version"].as_i64(), Some(2));
        let second_added_scopes = second["scopes"]["add"].as_array().unwrap();
        let second_added_ids: Vec<&str> = second_added_scopes
            .iter()
            .filter_map(|scope| scope["id"].as_str())
            .collect();
        assert!(second_added_ids.contains(&"runtime-only"));
    }

    #[pg_test]
    fn test_connect_returns_replace_and_scope_adds_for_fresh_client() {
        setup_test_tables();

        let resp = connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 2,
                "schema": {
                    "version": 0,
                    "hash": ""
                },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        );

        assert_eq!(resp["protocol_version"].as_u64(), Some(2));
        assert_eq!(resp["scope_set_version"].as_i64(), Some(1));
        assert_eq!(resp["schema"]["action"].as_str(), Some("replace"));
        assert!(resp.get("schema_definition").is_some());

        let added_scopes = resp["scopes"]["add"].as_array().unwrap();
        assert_eq!(added_scopes.len(), 1);
        let added_ids: Vec<&str> = added_scopes
            .iter()
            .filter_map(|scope| scope["id"].as_str())
            .collect();
        assert!(added_ids.contains(&"user:user1"));
        assert_eq!(resp["scopes"]["remove"].as_array().unwrap().len(), 0);
    }

    #[pg_test]
    fn test_portable_seed_export_is_side_effect_free() {
        setup_test_tables();
        register_shared_scope("global", true);

        Spi::run(
            "INSERT INTO test_products (id, name, price)
             VALUES ('37373737-3737-3737-3737-373737373737', 'Seed Push Up', 0)",
        )
        .unwrap();
        insert_edge(
            "test_products",
            "37373737-3737-3737-3737-373737373737",
            "global",
        );
        insert_changelog(
            "global",
            "test_products",
            "37373737-3737-3737-3737-373737373737",
            1,
        );

        let clients_before: Option<i64> =
            Spi::get_one("SELECT count(*) FROM sync_clients").unwrap();
        assert_eq!(clients_before, Some(0));

        let manifest: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_portable_seed_manifest()").unwrap();
        let manifest = manifest.unwrap().0;
        let scopes = manifest["portable_scopes"].as_array().unwrap();
        assert_eq!(scopes.len(), 1);
        assert_eq!(scopes[0]["id"].as_str(), Some("global"));
        assert!(scopes[0]["cursor"].as_str().is_some());
        assert!(scopes[0]["checksum"].as_str().is_some());

        let page: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_portable_seed_scope($1, $2, $3)",
            &["global".into(), "".into(), 100.into()],
        )
        .unwrap();
        let page = page.unwrap().0;
        assert_eq!(page["scope"].as_str(), Some("global"));
        assert_eq!(page["has_more"].as_bool(), Some(false));
        let records = page["records"].as_array().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["table"].as_str(), Some("test_products"));
        assert_eq!(
            records[0]["record_id"].as_str(),
            Some("37373737-3737-3737-3737-373737373737")
        );
        assert_eq!(records[0]["row"]["name"].as_str(), Some("Seed Push Up"));

        let clients_after: Option<i64> = Spi::get_one("SELECT count(*) FROM sync_clients").unwrap();
        assert_eq!(clients_after, Some(0));
    }

    #[pg_test]
    fn test_portable_seed_manifest_excludes_non_portable_shared_scopes() {
        setup_test_tables();
        register_shared_scope("catalog", true);
        register_shared_scope("runtime-only", false);

        let manifest: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_portable_seed_manifest()").unwrap();
        let manifest = manifest.unwrap().0;
        let scopes = manifest["portable_scopes"].as_array().unwrap();
        let scope_ids: Vec<&str> = scopes
            .iter()
            .filter_map(|scope| scope["id"].as_str())
            .collect();

        assert!(scope_ids.contains(&"catalog"));
        assert!(!scope_ids.contains(&"runtime-only"));
    }

    #[pg_test]
    fn test_connect_returns_none_when_schema_and_scopes_match() {
        setup_test_tables();
        let (schema_version, schema_hash) = latest_schema_ref();

        let resp = connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 2,
                "schema": {
                    "version": schema_version,
                    "hash": schema_hash
                },
                "scope_set_version": 1,
                "known_scopes": {
                    "user:user1": scope_cursor_ref("user:user1", 1)
                }
            }),
        );

        assert_eq!(resp["schema"]["action"].as_str(), Some("none"));
        assert!(resp.get("schema_definition").is_none());
        assert_eq!(resp["scopes"]["add"].as_array().unwrap().len(), 0);
        assert_eq!(resp["scopes"]["remove"].as_array().unwrap().len(), 0);
        assert_eq!(resp["scope_set_version"].as_i64(), Some(1));
    }

    #[pg_test]
    fn test_connect_server_only_change_keeps_schema_none() {
        setup_test_tables();
        let (schema_version, schema_hash) = latest_schema_ref();

        Spi::run(
            "ALTER TABLE test_orders
             ADD CONSTRAINT test_orders_ship_address_len
             CHECK (char_length(coalesce(title, '')) >= 0)",
        )
        .unwrap();

        let resp = connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 2,
                "schema": {
                    "version": schema_version,
                    "hash": schema_hash
                },
                "scope_set_version": 1,
                "known_scopes": {
                    "user:user1": scope_cursor_ref("user:user1", 1)
                }
            }),
        );

        assert_eq!(resp["schema"]["action"].as_str(), Some("none"));
        assert!(resp.get("schema_definition").is_none());
        assert_eq!(resp["scope_set_version"].as_i64(), Some(1));
    }

    #[pg_test]
    fn test_connect_removes_client_invented_scope_from_known_scopes() {
        setup_test_tables();
        let (schema_version, schema_hash) = latest_schema_ref();

        let resp = connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 2,
                "schema": {
                    "version": schema_version,
                    "hash": schema_hash
                },
                "scope_set_version": 1,
                "known_scopes": {
                    "user:user1": scope_cursor_ref("user:user1", 1),
                    "client:invented": scope_cursor_ref("client:invented", 1)
                }
            }),
        );

        assert_eq!(resp["schema"]["action"].as_str(), Some("none"));
        assert_eq!(resp["scope_set_version"].as_i64(), Some(1));
        assert_eq!(resp["scopes"]["add"].as_array().unwrap().len(), 0);

        let removed = resp["scopes"]["remove"].as_array().unwrap();
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].as_str(), Some("client:invented"));

        let subs: Option<Vec<String>> = Spi::get_one_with_args(
            "SELECT bucket_subs FROM sync_clients WHERE user_id = $1 AND client_id = $2",
            &["user1".into(), "client1".into()],
        )
        .unwrap();
        let subs = subs.unwrap();
        assert!(subs.contains(&"user:user1".to_string()));
        assert!(!subs.contains(&"client:invented".to_string()));
    }

    #[pg_test]
    fn test_connect_rejects_unsupported_protocol_version() {
        setup_test_tables();

        let resp = connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 99,
                "schema": {
                    "version": 0,
                    "hash": ""
                },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        );

        assert_eq!(resp["error"]["code"].as_str(), Some("upgrade_required"));
        assert_eq!(resp["error"]["retryable"].as_bool(), Some(false));
    }

    #[pg_test]
    fn test_pull_returns_upsert_and_scope_cursor() {
        setup_test_tables();
        connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 2,
                "schema": { "version": 0, "hash": "" },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        );

        Spi::run(
            "INSERT INTO test_orders (id, user_id, title, amount)
             VALUES ('11111111-1111-1111-1111-111111111111', 'user1', 'Morning Run', 10)",
        )
        .unwrap();
        insert_edge(
            "test_orders",
            "11111111-1111-1111-1111-111111111111",
            "user:user1",
        );
        insert_changelog(
            "user:user1",
            "test_orders",
            "11111111-1111-1111-1111-111111111111",
            1,
        );

        let (schema_version, schema_hash) = latest_schema_ref();
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2::jsonb)",
            &[
                "user1".into(),
                json!({
                    "client_id": "client1",
                    "schema": { "version": schema_version, "hash": schema_hash },
                    "scope_set_version": 1,
                    "scopes": {
                        "user:user1": scope_cursor_ref("user:user1", 0)
                    },
                    "limit": 100
                })
                .to_string()
                .into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        let changes = resp["changes"].as_array().unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0]["scope"].as_str(), Some("user:user1"));
        assert_eq!(changes[0]["table"].as_str(), Some("test_orders"));
        assert_eq!(changes[0]["op"].as_str(), Some("upsert"));
        assert_eq!(changes[0]["row"]["title"].as_str(), Some("Morning Run"));
        assert_eq!(resp["scope_set_version"].as_i64(), Some(1));
        assert!(resp["scope_cursors"]["user:user1"].as_str().is_some());
        assert_eq!(resp["rebuild"].as_array().unwrap().len(), 0);
        assert!(resp["checksums"]["user:user1"].as_str().is_some());
    }

    #[pg_test]
    fn test_pull_delete_includes_tombstone_row() {
        setup_test_tables();
        connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 2,
                "schema": { "version": 0, "hash": "" },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        );

        Spi::run(
            "INSERT INTO test_orders (id, user_id, title, updated_at, deleted_at)
             VALUES (
                '12121212-1212-1212-1212-121212121212',
                'user1',
                'Soft Deleted',
                '2026-01-04T00:00:00Z'::timestamptz,
                '2026-01-04T00:00:00Z'::timestamptz
             )",
        )
        .unwrap();
        insert_changelog(
            "user:user1",
            "test_orders",
            "12121212-1212-1212-1212-121212121212",
            3,
        );

        let (schema_version, schema_hash) = latest_schema_ref();
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2::jsonb)",
            &[
                "user1".into(),
                json!({
                    "client_id": "client1",
                    "schema": { "version": schema_version, "hash": schema_hash },
                    "scope_set_version": 1,
                    "scopes": {
                        "user:user1": scope_cursor_ref("user:user1", 0)
                    },
                    "limit": 100
                })
                .to_string()
                .into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        let changes = resp["changes"].as_array().unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0]["op"].as_str(), Some("delete"));
        assert_eq!(
            changes[0]["row"]["deleted_at"].as_str(),
            Some("2026-01-04T00:00:00.000Z")
        );
    }

    #[pg_test]
    fn test_pull_requests_rebuild_for_scope_without_cursor() {
        setup_test_tables();
        connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 2,
                "schema": { "version": 0, "hash": "" },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        );

        Spi::run(
            "INSERT INTO test_orders (id, user_id, title, amount)
             VALUES ('22222222-2222-2222-2222-222222222222', 'user1', 'Needs Rebuild', 11)",
        )
        .unwrap();
        insert_edge(
            "test_orders",
            "22222222-2222-2222-2222-222222222222",
            "user:user1",
        );
        insert_changelog(
            "user:user1",
            "test_orders",
            "22222222-2222-2222-2222-222222222222",
            1,
        );

        let (schema_version, schema_hash) = latest_schema_ref();
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2::jsonb)",
            &[
                "user1".into(),
                json!({
                    "client_id": "client1",
                    "schema": { "version": schema_version, "hash": schema_hash },
                    "scope_set_version": 1,
                    "scopes": {
                        "user:user1": { "cursor": null }
                    },
                    "limit": 100
                })
                .to_string()
                .into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        assert_eq!(resp["changes"].as_array().unwrap().len(), 0);
        assert_eq!(resp["rebuild"].as_array().unwrap().len(), 1);
        assert_eq!(resp["rebuild"][0].as_str(), Some("user:user1"));
    }

    #[pg_test]
    fn test_rebuild_returns_final_cursor_and_checksum() {
        setup_test_tables();
        register_shared_scope("global", true);
        connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 2,
                "schema": { "version": 0, "hash": "" },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        );

        Spi::run(
            "INSERT INTO test_products (id, name, price)
             VALUES ('33333333-3333-3333-3333-333333333333', 'Push Up', 0)",
        )
        .unwrap();
        insert_edge(
            "test_products",
            "33333333-3333-3333-3333-333333333333",
            "global",
        );
        insert_changelog(
            "global",
            "test_products",
            "33333333-3333-3333-3333-333333333333",
            1,
        );

        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_rebuild($1, $2::jsonb)",
            &[
                "user1".into(),
                json!({
                    "client_id": "client1",
                    "scope": "global",
                    "cursor": null,
                    "limit": 100
                })
                .to_string()
                .into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        assert_eq!(resp["scope"].as_str(), Some("global"));
        assert_eq!(resp["has_more"].as_bool(), Some(false));
        assert!(resp["final_scope_cursor"].as_str().is_some());
        assert!(resp["checksum"].as_str().is_some());
        assert!(resp["cursor"].is_null());

        let records = resp["records"].as_array().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["table"].as_str(), Some("test_products"));
        assert_eq!(records[0]["row"]["name"].as_str(), Some("Push Up"));
    }

    #[pg_test]
    fn test_push_mixed_accept_and_terminal_reject() {
        setup_test_tables();
        connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 2,
                "schema": { "version": 0, "hash": "" },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        );

        let (schema_version, schema_hash) = latest_schema_ref();
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2::jsonb)",
            &[
                "user1".into(),
                json!({
                    "client_id": "client1",
                    "batch_id": "batch-1",
                    "schema": { "version": schema_version, "hash": schema_hash },
                    "mutations": [
                        {
                            "mutation_id": "m1",
                            "table": "test_orders",
                            "op": "insert",
                            "pk": { "id": "44444444-4444-4444-4444-444444444444" },
                            "client_version": "2026-01-01T00:00:00Z",
                            "columns": {
                                "user_id": "user1",
                                "title": "Bench Press",
                                "amount": 25
                            }
                        },
                        {
                            "mutation_id": "m2",
                            "table": "test_products",
                            "op": "insert",
                            "pk": { "id": "55555555-5555-5555-5555-555555555555" },
                            "client_version": "2026-01-01T00:00:01Z",
                            "columns": {
                                "name": "Read Only",
                                "price": 12
                            }
                        }
                    ]
                })
                .to_string()
                .into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        let accepted = resp["accepted"].as_array().unwrap();
        let rejected = resp["rejected"].as_array().unwrap();
        assert_eq!(accepted.len(), 1);
        assert_eq!(rejected.len(), 1);

        assert_eq!(accepted[0]["mutation_id"].as_str(), Some("m1"));
        assert_eq!(accepted[0]["status"].as_str(), Some("applied"));
        assert_eq!(
            accepted[0]["server_row"]["title"].as_str(),
            Some("Bench Press")
        );
        assert!(accepted[0]["server_version"].as_str().is_some());

        assert_eq!(rejected[0]["mutation_id"].as_str(), Some("m2"));
        assert_eq!(rejected[0]["status"].as_str(), Some("rejected_terminal"));
        assert_eq!(rejected[0]["code"].as_str(), Some("policy_rejected"));
    }

    #[pg_test]
    fn test_push_delete_returns_canonical_tombstone_row() {
        setup_test_tables();
        connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 2,
                "schema": { "version": 0, "hash": "" },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        );

        Spi::run(
            "INSERT INTO test_orders (id, user_id, title, updated_at)
             VALUES (
                '56565656-5656-5656-5656-565656565656',
                'user1',
                'Delete Me',
                '2026-01-03T00:00:00Z'::timestamptz
             )",
        )
        .unwrap();

        let (schema_version, schema_hash) = latest_schema_ref();
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2::jsonb)",
            &[
                "user1".into(),
                json!({
                    "client_id": "client1",
                    "batch_id": "batch-delete-1",
                    "schema": { "version": schema_version, "hash": schema_hash },
                    "mutations": [
                        {
                            "mutation_id": "m-delete-1",
                            "table": "test_orders",
                            "op": "delete",
                            "pk": { "id": "56565656-5656-5656-5656-565656565656" }
                        }
                    ]
                })
                .to_string()
                .into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        let accepted = resp["accepted"].as_array().unwrap();
        assert_eq!(accepted.len(), 1);
        assert_eq!(accepted[0]["status"].as_str(), Some("applied"));
        assert_eq!(
            accepted[0]["server_row"]["deleted_at"]
                .as_str()
                .map(|value| value.ends_with('Z')),
            Some(true)
        );
    }

    #[pg_test]
    fn test_push_update_typed_payloads() {
        setup_test_tables();

        Spi::run(
            "CREATE TABLE IF NOT EXISTS test_profiles (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id TEXT NOT NULL,
                balance NUMERIC(15,2) NOT NULL DEFAULT 0,
                is_active BOOLEAN NOT NULL DEFAULT true,
                preferences JSONB DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                deleted_at TIMESTAMPTZ
            )",
        )
        .unwrap();

        Spi::run(
            "SELECT synchro_register_table(
                'test_profiles',
                $$SELECT ARRAY['user:' || user_id] FROM test_profiles WHERE id = $1::uuid$$,
                'id', 'updated_at', 'deleted_at', 'enabled'
            )",
        )
        .unwrap();

        connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 2,
                "schema": { "version": 0, "hash": "" },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        );

        Spi::run(
            "INSERT INTO test_profiles (id, user_id, balance, is_active, preferences, updated_at)
             VALUES (
                '90909090-9090-9090-9090-909090909090',
                'user1',
                0,
                true,
                '{\"theme\":\"light\"}'::jsonb,
                '2025-01-01T00:00:00Z'::timestamptz
             )",
        )
        .unwrap();

        let (schema_version, schema_hash) = latest_schema_ref();
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2::jsonb)",
            &[
                "user1".into(),
                json!({
                    "client_id": "client1",
                    "batch_id": "batch-profile-1",
                    "schema": { "version": schema_version, "hash": schema_hash },
                    "mutations": [
                        {
                            "mutation_id": "m-profile-1",
                            "table": "test_profiles",
                            "op": "update",
                            "pk": { "id": "90909090-9090-9090-9090-909090909090" },
                            "client_version": "2026-01-03T00:00:00Z",
                            "columns": {
                                "user_id": "user1",
                                "balance": "42.50",
                                "is_active": false,
                                "preferences": { "theme": "dark" }
                            }
                        }
                    ]
                })
                .to_string()
                .into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        let accepted = resp["accepted"].as_array().unwrap();
        assert_eq!(accepted.len(), 1);
        assert_eq!(accepted[0]["status"].as_str(), Some("applied"));

        let row = Spi::get_one::<String>(
            "SELECT format('%s|%s|%s', balance::text, is_active::text, preferences->>'theme')
             FROM test_profiles
             WHERE id = '90909090-9090-9090-9090-909090909090'",
        )
        .unwrap()
        .expect("updated row should exist");
        assert_eq!(row, "42.50|false|dark");
    }

    #[pg_test]
    fn test_push_requires_client_version_for_update() {
        setup_test_tables();
        connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 2,
                "schema": { "version": 0, "hash": "" },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        );

        let (schema_version, schema_hash) = latest_schema_ref();
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2::jsonb)",
            &[
                "user1".into(),
                json!({
                    "client_id": "client1",
                    "batch_id": "batch-missing-client-version",
                    "schema": { "version": schema_version, "hash": schema_hash },
                    "mutations": [
                        {
                            "mutation_id": "m-missing-client-version",
                            "table": "test_orders",
                            "op": "update",
                            "pk": { "id": "99990000-0000-0000-0000-000000000001" },
                            "columns": {
                                "title": "Should Fail"
                            }
                        }
                    ]
                })
                .to_string()
                .into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        assert_eq!(resp["error"]["code"].as_str(), Some("invalid_request"));
        assert!(resp["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("client_version"));
    }

    #[pg_test]
    fn test_push_rejects_malformed_client_version_timestamp() {
        setup_test_tables();
        connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 2,
                "schema": { "version": 0, "hash": "" },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        );

        let (schema_version, schema_hash) = latest_schema_ref();
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2::jsonb)",
            &[
                "user1".into(),
                json!({
                    "client_id": "client1",
                    "batch_id": "batch-invalid-client-version",
                    "schema": { "version": schema_version, "hash": schema_hash },
                    "mutations": [
                        {
                            "mutation_id": "m-invalid-client-version",
                            "table": "test_orders",
                            "op": "update",
                            "pk": { "id": "99990000-0000-0000-0000-000000000001" },
                            "client_version": "not-a-timestamp",
                            "columns": {
                                "title": "Should Fail"
                            }
                        }
                    ]
                })
                .to_string()
                .into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        assert_eq!(resp["error"]["code"].as_str(), Some("invalid_request"));
        assert!(resp["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("invalid client_version"));
    }

    #[pg_test]
    fn test_push_rejects_malformed_base_version_timestamp() {
        setup_test_tables();
        connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 2,
                "schema": { "version": 0, "hash": "" },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        );

        let (schema_version, schema_hash) = latest_schema_ref();
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2::jsonb)",
            &[
                "user1".into(),
                json!({
                    "client_id": "client1",
                    "batch_id": "batch-invalid-base-version",
                    "schema": { "version": schema_version, "hash": schema_hash },
                    "mutations": [
                        {
                            "mutation_id": "m-invalid-base-version",
                            "table": "test_orders",
                            "op": "delete",
                            "pk": { "id": "99990000-0000-0000-0000-000000000001" },
                            "base_version": "still-not-a-timestamp"
                        }
                    ]
                })
                .to_string()
                .into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        assert_eq!(resp["error"]["code"].as_str(), Some("invalid_request"));
        assert!(resp["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("invalid base_version"));
    }

    #[pg_test]
    fn test_push_update_uses_client_version_for_lww() {
        setup_test_tables();
        connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 2,
                "schema": { "version": 0, "hash": "" },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        );

        Spi::run(
            "INSERT INTO test_orders (id, user_id, title, updated_at)
             VALUES (
                '91919191-0000-0000-0000-000000000002',
                'user1',
                'Old Title',
                '2020-01-01T00:00:00Z'::timestamptz
             )",
        )
        .unwrap();

        let (schema_version, schema_hash) = latest_schema_ref();
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2::jsonb)",
            &[
                "user1".into(),
                json!({
                    "client_id": "client1",
                    "batch_id": "batch-client-version-lww",
                    "schema": { "version": schema_version, "hash": schema_hash },
                    "mutations": [
                        {
                            "mutation_id": "m-client-version-lww",
                            "table": "test_orders",
                            "op": "update",
                            "pk": { "id": "91919191-0000-0000-0000-000000000002" },
                            "client_version": "2025-01-01T00:00:00Z",
                            "columns": {
                                "title": "Updated via client_version"
                            }
                        }
                    ]
                })
                .to_string()
                .into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        let accepted = resp["accepted"].as_array().unwrap();
        assert_eq!(accepted.len(), 1);
        assert_eq!(accepted[0]["status"].as_str(), Some("applied"));

        let title: Option<String> = Spi::get_one(
            "SELECT title FROM test_orders WHERE id = '91919191-0000-0000-0000-000000000002'",
        )
        .unwrap();
        assert_eq!(title, Some("Updated via client_version".to_string()));

        let updated_at_utc: Option<String> = Spi::get_one(
            "SELECT to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
             FROM test_orders
             WHERE id = '91919191-0000-0000-0000-000000000002'",
        )
        .unwrap();
        assert_eq!(updated_at_utc, Some("2025-01-01T00:00:00Z".to_string()));
    }

    #[pg_test]
    fn test_push_insert_persists_client_version_as_updated_at() {
        setup_test_tables();
        register_client("u1", "c1");

        let resp = push_client(
            "u1",
            "c1",
            "batch-insert-client-version",
            json!([{
                "mutation_id": "m-insert-client-version",
                "table": "test_orders",
                "op": "insert",
                "pk": { "id": "92929292-0000-0000-0000-000000000002" },
                "client_version": "2028-01-01T00:00:00Z",
                "columns": {
                    "user_id": "u1",
                    "title": "Inserted via client_version"
                }
            }]),
        );

        let accepted = resp["accepted"].as_array().unwrap();
        assert_eq!(accepted.len(), 1);
        assert_eq!(accepted[0]["status"].as_str(), Some("applied"));

        let updated_at_utc: Option<String> = Spi::get_one(
            "SELECT to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
             FROM test_orders
             WHERE id = '92929292-0000-0000-0000-000000000002'",
        )
        .unwrap();
        assert_eq!(updated_at_utc, Some("2028-01-01T00:00:00Z".to_string()));
    }

    #[pg_test]
    fn test_push_materializes_bucket_edges_and_changelog() {
        setup_test_tables();
        connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 2,
                "schema": { "version": 0, "hash": "" },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        );

        let (schema_version, schema_hash) = latest_schema_ref();
        let record_id = "91919191-9191-9191-9191-919191919191";
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2::jsonb)",
            &[
                "user1".into(),
                json!({
                    "client_id": "client1",
                    "batch_id": "batch-materialize-1",
                    "schema": { "version": schema_version, "hash": schema_hash },
                    "mutations": [
                        {
                            "mutation_id": "m-materialize-1",
                            "table": "test_orders",
                            "op": "insert",
                            "pk": { "id": record_id },
                            "client_version": "2026-01-04T00:00:00Z",
                            "columns": {
                                "user_id": "user1",
                                "title": "Materialized Insert",
                                "amount": 15
                            }
                        }
                    ]
                })
                .to_string()
                .into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;
        assert_eq!(resp["accepted"][0]["status"].as_str(), Some("applied"));

        let bucket_edge_count: Option<i64> = Spi::get_one_with_args(
            "SELECT count(*) FROM sync_bucket_edges
             WHERE table_name = 'test_orders'
               AND record_id = $1
               AND bucket_id = 'user:user1'",
            &[record_id.into()],
        )
        .unwrap();
        assert_eq!(bucket_edge_count, Some(1));

        let changelog_count: Option<i64> = Spi::get_one_with_args(
            "SELECT count(*) FROM sync_changelog
             WHERE table_name = 'test_orders'
               AND record_id = $1
               AND bucket_id = 'user:user1'",
            &[record_id.into()],
        )
        .unwrap();
        assert_eq!(changelog_count, Some(1));
    }

    #[pg_test]
    fn test_push_canonicalizes_uuid_record_ids_for_sync_metadata() {
        setup_test_tables();
        connect_client(
            "user1",
            json!({
                "client_id": "client1",
                "platform": "ios",
                "app_version": "1.0.0",
                "protocol_version": 2,
                "schema": { "version": 0, "hash": "" },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        );

        let (schema_version, schema_hash) = latest_schema_ref();
        let upper_record_id = "91919191-9191-9191-9191-9191919191AB";
        let lower_record_id = upper_record_id.to_ascii_lowercase();

        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2::jsonb)",
            &[
                "user1".into(),
                json!({
                    "client_id": "client1",
                    "batch_id": "batch-canonicalize-1",
                    "schema": { "version": schema_version, "hash": schema_hash },
                    "mutations": [
                        {
                            "mutation_id": "m-canonicalize-1",
                            "table": "test_orders",
                            "op": "insert",
                            "pk": { "id": upper_record_id },
                            "client_version": "2026-01-04T00:00:00Z",
                            "columns": {
                                "user_id": "user1",
                                "title": "Canonicalized Insert",
                                "amount": 15
                            }
                        }
                    ]
                })
                .to_string()
                .into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;
        assert_eq!(resp["accepted"][0]["status"].as_str(), Some("applied"));

        let edge_ids: Vec<String> = Spi::connect(|client| {
            let rows = client
                .select(
                    "SELECT record_id FROM sync_bucket_edges
                     WHERE table_name = 'test_orders'
                     ORDER BY record_id",
                    None,
                    &[],
                )
                .unwrap();
            let mut ids = Vec::new();
            for row in rows {
                if let Some(record_id) = row.get_by_name::<String, &str>("record_id").unwrap() {
                    ids.push(record_id);
                }
            }
            Ok::<Vec<String>, pgrx::spi::Error>(ids)
        })
        .unwrap();
        assert_eq!(edge_ids, vec![lower_record_id.clone()]);

        let changelog_ids: Vec<String> = Spi::connect(|client| {
            let rows = client
                .select(
                    "SELECT record_id FROM sync_changelog
                     WHERE table_name = 'test_orders'
                     ORDER BY record_id",
                    None,
                    &[],
                )
                .unwrap();
            let mut ids = Vec::new();
            for row in rows {
                if let Some(record_id) = row.get_by_name::<String, &str>("record_id").unwrap() {
                    ids.push(record_id);
                }
            }
            Ok::<Vec<String>, pgrx::spi::Error>(ids)
        })
        .unwrap();
        assert_eq!(changelog_ids, vec![lower_record_id]);
    }

    fn setup_pull_fixtures() {
        setup_test_tables();
        register_client("u1", "c1");

        Spi::run(
            "INSERT INTO test_orders (id, user_id, title, internal_notes) VALUES
             ('a1111111-1111-1111-1111-111111111111', 'u1', 'Order 1', 'secret1'),
             ('a2222222-2222-2222-2222-222222222222', 'u1', 'Order 2', 'secret2')",
        )
        .unwrap();

        insert_changelog(
            "user:u1",
            "test_orders",
            "a1111111-1111-1111-1111-111111111111",
            1,
        );
        insert_changelog(
            "user:u1",
            "test_orders",
            "a2222222-2222-2222-2222-222222222222",
            1,
        );

        insert_edge(
            "test_orders",
            "a1111111-1111-1111-1111-111111111111",
            "user:u1",
        );
        insert_edge(
            "test_orders",
            "a2222222-2222-2222-2222-222222222222",
            "user:u1",
        );
    }

    #[pg_test]
    fn test_push_create_strips_protected() {
        setup_test_tables();
        register_client("u1", "c1");

        let resp = push_client(
            "u1",
            "c1",
            "batch-protected",
            json!([{
                "mutation_id": "m-protected",
                "table": "test_orders",
                "op": "insert",
                "pk": { "id": "22222222-2222-2222-2222-222222222222" },
                "client_version": "2026-01-01T00:00:00Z",
                "columns": {
                    "user_id": "u1",
                    "title": "Protected Test",
                    "updated_at": "1999-01-01T00:00:00Z",
                    "created_at": "1999-01-01T00:00:00Z"
                }
            }]),
        );
        assert_eq!(resp["accepted"][0]["status"].as_str(), Some("applied"));

        let year: Option<i32> = Spi::get_one(
            "SELECT EXTRACT(YEAR FROM updated_at)::int FROM test_orders
             WHERE id = '22222222-2222-2222-2222-222222222222'",
        )
        .unwrap();
        assert_ne!(year, Some(1999));
    }

    #[pg_test]
    fn test_push_create_strips_exclude() {
        setup_test_tables();
        register_client("u1", "c1");

        let resp = push_client(
            "u1",
            "c1",
            "batch-exclude",
            json!([{
                "mutation_id": "m-exclude",
                "table": "test_orders",
                "op": "insert",
                "pk": { "id": "33333333-3333-3333-3333-333333333333" },
                "client_version": "2026-01-01T00:00:00Z",
                "columns": {
                    "user_id": "u1",
                    "title": "Exclude Test",
                    "internal_notes": "secret stuff"
                }
            }]),
        );
        assert_eq!(resp["accepted"][0]["status"].as_str(), Some("applied"));

        let notes: Option<String> = Spi::get_one(
            "SELECT internal_notes FROM test_orders
             WHERE id = '33333333-3333-3333-3333-333333333333'",
        )
        .unwrap();
        assert_eq!(notes, Some("".to_string()));
    }

    #[pg_test]
    fn test_push_create_strips_non_synced_columns() {
        setup_sync_columns_table();
        register_client("u1", "c1");

        let resp = push_client(
            "u1",
            "c1",
            "batch-sync-columns",
            json!([{
                "mutation_id": "m-sync-columns",
                "table": "test_sync_columns_items",
                "op": "insert",
                "pk": { "id": "55555555-5555-5555-5555-555555555555" },
                "client_version": "2026-01-01T00:00:00Z",
                "columns": {
                    "user_id": "u1",
                    "title": "Sync columns",
                    "search_vector": "fts",
                    "internal_notes": "secret"
                }
            }]),
        );
        assert_eq!(resp["accepted"][0]["status"].as_str(), Some("applied"));

        let row: Option<pgrx::JsonB> = Spi::get_one(
            "SELECT jsonb_build_object(
                'title', title,
                'search_vector', search_vector,
                'internal_notes', internal_notes
             )
             FROM test_sync_columns_items
             WHERE id = '55555555-5555-5555-5555-555555555555'",
        )
        .unwrap();
        let row = row.expect("inserted row should exist").0;
        assert_eq!(row["title"].as_str(), Some("Sync columns"));
        assert_eq!(row["search_vector"].as_str(), Some(""));
        assert_eq!(row["internal_notes"].as_str(), Some(""));
    }

    #[pg_test]
    fn test_push_create_duplicate_conflict() {
        setup_test_tables();
        register_client("u1", "c1");

        Spi::run(
            "INSERT INTO test_orders (id, user_id, title)
             VALUES ('44444444-4444-4444-4444-444444444444', 'u1', 'Existing')",
        )
        .unwrap();

        let resp = push_client(
            "u1",
            "c1",
            "batch-duplicate",
            json!([{
                "mutation_id": "m-duplicate",
                "table": "test_orders",
                "op": "insert",
                "pk": { "id": "44444444-4444-4444-4444-444444444444" },
                "client_version": "2026-01-01T00:00:00Z",
                "columns": {
                    "user_id": "u1",
                    "title": "Duplicate"
                }
            }]),
        );

        let rejected = resp["rejected"].as_array().unwrap();
        assert_eq!(rejected.len(), 1);
        assert_eq!(rejected[0]["status"].as_str(), Some("conflict"));
        assert_eq!(rejected[0]["code"].as_str(), Some("version_conflict"));
    }

    #[pg_test]
    fn test_push_update_lww_server_wins() {
        setup_test_tables();
        register_client("u1", "c1");

        Spi::run(
            "INSERT INTO test_orders (id, user_id, title, updated_at)
             VALUES ('66666666-6666-6666-6666-666666666666', 'u1', 'Recent',
             '2099-01-01T00:00:00Z')",
        )
        .unwrap();

        let resp = push_client(
            "u1",
            "c1",
            "batch-server-wins",
            json!([{
                "mutation_id": "m-server-wins",
                "table": "test_orders",
                "op": "update",
                "pk": { "id": "66666666-6666-6666-6666-666666666666" },
                "client_version": "2020-01-01T00:00:00Z",
                "columns": {
                    "title": "Stale"
                }
            }]),
        );

        let rejected = resp["rejected"].as_array().unwrap();
        assert_eq!(rejected[0]["status"].as_str(), Some("conflict"));
        assert_eq!(rejected[0]["code"].as_str(), Some("version_conflict"));

        let title: Option<String> = Spi::get_one(
            "SELECT title FROM test_orders WHERE id = '66666666-6666-6666-6666-666666666666'",
        )
        .unwrap();
        assert_eq!(title, Some("Recent".to_string()));
    }

    #[pg_test]
    fn test_push_update_lww_base_unchanged() {
        setup_test_tables();
        register_client("u1", "c1");

        Spi::run(
            "INSERT INTO test_orders (id, user_id, title, updated_at)
             VALUES ('88888888-8888-8888-8888-888888888888', 'u1', 'Base Test',
             '2025-06-15T12:00:00Z')",
        )
        .unwrap();

        let resp = push_client(
            "u1",
            "c1",
            "batch-base-match",
            json!([{
                "mutation_id": "m-base-match",
                "table": "test_orders",
                "op": "update",
                "pk": { "id": "88888888-8888-8888-8888-888888888888" },
                "client_version": "2020-01-01T00:00:00Z",
                "base_version": "2025-06-15T12:00:00Z",
                "columns": {
                    "title": "Base Match"
                }
            }]),
        );
        assert_eq!(resp["accepted"][0]["status"].as_str(), Some("applied"));
    }

    #[pg_test]
    fn test_push_soft_delete_idempotent() {
        setup_test_tables();
        register_client("u1", "c1");

        Spi::run(
            "INSERT INTO test_orders (id, user_id, title, deleted_at)
             VALUES ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'u1', 'Already Deleted', now())",
        )
        .unwrap();

        let resp = push_client(
            "u1",
            "c1",
            "batch-delete-idempotent",
            json!([{
                "mutation_id": "m-delete-idempotent",
                "table": "test_orders",
                "op": "delete",
                "pk": { "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" }
            }]),
        );
        assert_eq!(resp["accepted"][0]["status"].as_str(), Some("applied"));
    }

    #[pg_test]
    fn test_push_hard_delete() {
        setup_test_tables();
        register_client("u1", "c1");

        Spi::run(
            "INSERT INTO test_bare_items (id, name)
             VALUES ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'Bare Item')",
        )
        .unwrap();

        let resp = push_client(
            "u1",
            "c1",
            "batch-hard-delete",
            json!([{
                "mutation_id": "m-hard-delete",
                "table": "test_bare_items",
                "op": "delete",
                "pk": { "id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb" }
            }]),
        );
        assert_eq!(resp["accepted"][0]["status"].as_str(), Some("applied"));

        let count: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM test_bare_items WHERE id = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'",
        )
        .unwrap();
        assert_eq!(count, Some(0));
    }

    #[pg_test]
    fn test_push_resurrection() {
        setup_test_tables();
        register_client("u1", "c1");

        Spi::run(
            "INSERT INTO test_orders (id, user_id, title, deleted_at)
             VALUES ('cccccccc-cccc-cccc-cccc-cccccccccccc', 'u1', 'Dead', now())",
        )
        .unwrap();

        let resp = push_client(
            "u1",
            "c1",
            "batch-resurrection",
            json!([{
                "mutation_id": "m-resurrection",
                "table": "test_orders",
                "op": "insert",
                "pk": { "id": "cccccccc-cccc-cccc-cccc-cccccccccccc" },
                "client_version": "2099-01-01T00:00:00Z",
                "columns": {
                    "user_id": "u1",
                    "title": "Risen"
                }
            }]),
        );
        assert_eq!(resp["accepted"][0]["status"].as_str(), Some("applied"));

        let row = Spi::get_two::<String, bool>(
            "SELECT title, deleted_at IS NULL FROM test_orders
             WHERE id = 'cccccccc-cccc-cccc-cccc-cccccccccccc'",
        )
        .unwrap();
        assert_eq!(row.0, Some("Risen".to_string()));
        assert_eq!(row.1, Some(true));
    }

    #[pg_test]
    fn test_push_read_only_rejected() {
        setup_test_tables();
        register_client("u1", "c1");

        let resp = push_client(
            "u1",
            "c1",
            "batch-read-only",
            json!([{
                "mutation_id": "m-read-only",
                "table": "test_products",
                "op": "insert",
                "pk": { "id": "dddddddd-dddd-dddd-dddd-dddddddddddd" },
                "client_version": "2026-01-01T00:00:00Z",
                "columns": {
                    "name": "Hack Product"
                }
            }]),
        );

        let rejected = resp["rejected"].as_array().unwrap();
        assert_eq!(rejected.len(), 1);
        assert_eq!(rejected[0]["status"].as_str(), Some("rejected_terminal"));
        assert_eq!(rejected[0]["code"].as_str(), Some("policy_rejected"));
    }

    #[pg_test]
    fn test_backfill_bucket_edges_populates_existing_rows() {
        setup_test_tables();
        Spi::run(
            "INSERT INTO test_products (id, name, price)
             VALUES ('13131313-1313-1313-1313-131313131313', 'Backfill Product', 12)",
        )
        .unwrap();

        let resp: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_backfill_bucket_edges()").unwrap();
        let resp = resp.unwrap().0;
        assert!(resp["edges"].as_i64().unwrap_or(0) > 0);

        let edge_count: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM sync_bucket_edges
             WHERE table_name = 'test_products'
               AND record_id = '13131313-1313-1313-1313-131313131313'
               AND bucket_id = 'global'",
        )
        .unwrap();
        assert_eq!(edge_count, Some(1));
    }

    #[pg_test]
    fn test_push_write_protect_transforms() {
        setup_test_tables();
        register_client("u1", "c1");

        Spi::run(
            "CREATE OR REPLACE FUNCTION synchro_write_protect(
                p_user_id TEXT, p_table_name TEXT, p_operation TEXT, p_record JSONB
            ) RETURNS JSONB AS $$
            BEGIN
                IF p_operation = 'insert' THEN
                    p_record := jsonb_set(p_record, '{user_id}', to_jsonb(p_user_id));
                END IF;
                RETURN p_record;
            END;
            $$ LANGUAGE plpgsql",
        )
        .unwrap();

        let resp = push_client(
            "u1",
            "c1",
            "batch-write-protect",
            json!([{
                "mutation_id": "m-write-protect",
                "table": "test_orders",
                "op": "insert",
                "pk": { "id": "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee" },
                "client_version": "2026-01-01T00:00:00Z",
                "columns": {
                    "title": "Protected"
                }
            }]),
        );
        assert_eq!(resp["accepted"][0]["status"].as_str(), Some("applied"));

        let uid: Option<String> = Spi::get_one(
            "SELECT user_id FROM test_orders WHERE id = 'eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee'",
        )
        .unwrap();
        assert_eq!(uid, Some("u1".to_string()));

        Spi::run("DROP FUNCTION IF EXISTS synchro_write_protect").unwrap();
    }

    #[pg_test]
    fn test_push_write_protect_restrips() {
        setup_test_tables();
        register_client("u1", "c1");

        Spi::run(
            "CREATE OR REPLACE FUNCTION synchro_write_protect(
                p_user_id TEXT, p_table_name TEXT, p_operation TEXT, p_record JSONB
            ) RETURNS JSONB AS $$
            BEGIN
                p_record := jsonb_set(
                    p_record,
                    '{updated_at}',
                    to_jsonb('1999-01-01T00:00:00Z'::text)
                );
                RETURN p_record;
            END;
            $$ LANGUAGE plpgsql",
        )
        .unwrap();

        let resp = push_client(
            "u1",
            "c1",
            "batch-write-protect-restrip",
            json!([{
                "mutation_id": "m-write-protect-restrip",
                "table": "test_orders",
                "op": "insert",
                "pk": { "id": "ffffffff-ffff-ffff-ffff-ffffffffffff" },
                "client_version": "2026-01-01T00:00:00Z",
                "columns": {
                    "user_id": "u1",
                    "title": "Restrip"
                }
            }]),
        );
        assert_eq!(resp["accepted"][0]["status"].as_str(), Some("applied"));

        let year: Option<i32> = Spi::get_one(
            "SELECT EXTRACT(YEAR FROM updated_at)::int FROM test_orders
             WHERE id = 'ffffffff-ffff-ffff-ffff-ffffffffffff'",
        )
        .unwrap();
        assert_ne!(year, Some(1999));

        Spi::run("DROP FUNCTION IF EXISTS synchro_write_protect").unwrap();
    }

    #[pg_test]
    fn test_push_invalid_columns_ignored() {
        setup_test_tables();
        register_client("u1", "c1");

        let resp = push_client(
            "u1",
            "c1",
            "batch-invalid-column",
            json!([{
                "mutation_id": "m-invalid-column",
                "table": "test_orders",
                "op": "insert",
                "pk": { "id": "11111111-aaaa-bbbb-cccc-111111111111" },
                "client_version": "2026-01-01T00:00:00Z",
                "columns": {
                    "user_id": "u1",
                    "title": "Valid",
                    "nonexistent_col": "bogus"
                }
            }]),
        );
        assert_eq!(resp["accepted"][0]["status"].as_str(), Some("applied"));
    }

    #[pg_test]
    fn test_push_rls_context_set() {
        setup_test_tables();
        register_client("u1", "c1");

        Spi::run("ALTER TABLE test_orders ENABLE ROW LEVEL SECURITY").unwrap();
        Spi::run("ALTER TABLE test_orders FORCE ROW LEVEL SECURITY").unwrap();
        Spi::run(
            "CREATE POLICY test_rls ON test_orders
             USING (user_id = current_setting('app.user_id', true))
             WITH CHECK (user_id = current_setting('app.user_id', true))",
        )
        .unwrap();

        let resp = push_client(
            "u1",
            "c1",
            "batch-rls",
            json!([{
                "mutation_id": "m-rls",
                "table": "test_orders",
                "op": "insert",
                "pk": { "id": "22222222-aaaa-bbbb-cccc-222222222222" },
                "client_version": "2026-01-01T00:00:00Z",
                "columns": {
                    "user_id": "u1",
                    "title": "RLS OK"
                }
            }]),
        );
        assert_eq!(resp["accepted"][0]["status"].as_str(), Some("applied"));

        Spi::run("DROP POLICY test_rls ON test_orders").unwrap();
        Spi::run("ALTER TABLE test_orders DISABLE ROW LEVEL SECURITY").unwrap();
    }

    #[pg_test]
    fn test_pull_deduplication() {
        setup_pull_fixtures();
        insert_changelog(
            "user:u1",
            "test_orders",
            "a1111111-1111-1111-1111-111111111111",
            2,
        );

        let resp = pull_client(
            "u1",
            "c1",
            1,
            json!({ "user:u1": scope_cursor_ref("user:u1", 0) }),
            100,
        );

        let hits = resp["changes"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|change| change["pk"]["id"].as_str() == Some("a1111111-1111-1111-1111-111111111111"))
            .count();
        assert_eq!(hits, 1);
    }

    #[pg_test]
    fn test_pull_pagination_has_more() {
        setup_pull_fixtures();

        let resp = pull_client(
            "u1",
            "c1",
            1,
            json!({ "user:u1": scope_cursor_ref("user:u1", 0) }),
            1,
        );
        assert_eq!(resp["has_more"].as_bool(), Some(true));
    }

    #[pg_test]
    fn test_pull_exclude_columns_stripped() {
        setup_pull_fixtures();

        let resp = pull_client(
            "u1",
            "c1",
            1,
            json!({ "user:u1": scope_cursor_ref("user:u1", 0) }),
            100,
        );

        for change in resp["changes"].as_array().unwrap() {
            if change["table"].as_str() == Some("test_orders") {
                assert!(change["row"].get("internal_notes").is_none());
            }
        }
    }

    #[pg_test]
    fn test_pull_sync_columns_strip_non_synced_fields() {
        setup_sync_columns_table();
        register_client("u1", "c1");

        Spi::run(
            "INSERT INTO test_sync_columns_items (id, user_id, title, search_vector, internal_notes)
             VALUES (
                '44444444-4444-4444-4444-444444444444',
                'u1',
                'Projection test',
                'fts data',
                'server secret'
             )",
        )
        .unwrap();
        insert_edge(
            "test_sync_columns_items",
            "44444444-4444-4444-4444-444444444444",
            "user:u1",
        );
        insert_changelog(
            "user:u1",
            "test_sync_columns_items",
            "44444444-4444-4444-4444-444444444444",
            1,
        );

        let resp = pull_client(
            "u1",
            "c1",
            1,
            json!({ "user:u1": scope_cursor_ref("user:u1", 0) }),
            100,
        );

        let change = resp["changes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|change| change["table"].as_str() == Some("test_sync_columns_items"))
            .expect("test_sync_columns_items should be present in pull response");
        let row = &change["row"];
        assert_eq!(row["title"].as_str(), Some("Projection test"));
        assert!(row.get("search_vector").is_none());
        assert!(row.get("internal_notes").is_none());
    }

    #[pg_test]
    fn test_pull_bucket_isolation() {
        setup_test_tables();
        register_client("u1", "c1");
        register_client("u2", "c2");

        Spi::run(
            "INSERT INTO test_orders (id, user_id, title) VALUES
             ('a00000a1-1111-1111-1111-111111111111', 'u1', 'User1 Order'),
             ('a00000a2-2222-2222-2222-222222222222', 'u2', 'User2 Order')",
        )
        .unwrap();

        insert_changelog(
            "user:u1",
            "test_orders",
            "a00000a1-1111-1111-1111-111111111111",
            1,
        );
        insert_changelog(
            "user:u2",
            "test_orders",
            "a00000a2-2222-2222-2222-222222222222",
            1,
        );
        insert_edge(
            "test_orders",
            "a00000a1-1111-1111-1111-111111111111",
            "user:u1",
        );
        insert_edge(
            "test_orders",
            "a00000a2-2222-2222-2222-222222222222",
            "user:u2",
        );

        let resp = pull_client(
            "u1",
            "c1",
            1,
            json!({ "user:u1": scope_cursor_ref("user:u1", 0) }),
            100,
        );

        for change in resp["changes"].as_array().unwrap() {
            assert_ne!(
                change["pk"]["id"].as_str(),
                Some("a00000a2-2222-2222-2222-222222222222")
            );
        }
    }

    #[pg_test]
    fn test_pull_scope_updates_added() {
        setup_test_tables();
        register_client("u1", "c1");

        let resp = pull_client("u1", "c1", 0, json!({}), 100);
        let added = resp["scope_updates"]["add"].as_array().unwrap();
        assert!(
            added.iter().any(|scope| scope["id"].as_str() == Some("user:u1")),
            "user:u1 should be present in scope updates"
        );
    }

    #[pg_test]
    fn test_pull_scope_updates_removed() {
        setup_test_tables();
        register_client("u1", "c1");

        let resp = pull_client(
            "u1",
            "c1",
            0,
            json!({ "team:old": scope_cursor_ref("team:old", 0) }),
            100,
        );
        let removed = resp["scope_updates"]["remove"].as_array().unwrap();
        assert!(
            removed.iter().any(|scope| scope.as_str() == Some("team:old")),
            "team:old should be removed"
        );
    }

    #[pg_test]
    fn test_pull_scope_updates_unchanged() {
        setup_test_tables();
        register_client("u1", "c1");

        let scopes = client_scope_ids("u1", "c1")
            .into_iter()
            .map(|scope_id| {
                let cursor = issued_scope_cursor(&scope_id, 0);
                (scope_id, json!({ "cursor": cursor }))
            })
            .collect::<serde_json::Map<String, Value>>();
        let resp = pull_client("u1", "c1", 1, Value::Object(scopes), 100);

        assert_eq!(resp["scope_updates"]["add"].as_array().unwrap().len(), 0);
        assert_eq!(resp["scope_updates"]["remove"].as_array().unwrap().len(), 0);
    }

    #[pg_test]
    fn test_rebuild_cursor_pagination() {
        setup_test_tables();
        register_client("u1", "c1");

        for i in 1..=3 {
            let id = format!("b000000{i}-0000-0000-0000-000000000000");
            Spi::run_with_args(
                "INSERT INTO test_orders (id, user_id, title) VALUES ($1::uuid, 'u1', $2)",
                &[id.as_str().into(), format!("Rebuild {i}").as_str().into()],
            )
            .unwrap();
            insert_edge("test_orders", &id, "user:u1");
        }

        let first = rebuild_client("u1", "c1", "user:u1", None, 2);
        assert_eq!(first["has_more"].as_bool(), Some(true));
        let cursor = first["cursor"].as_str().unwrap();
        assert!(!cursor.is_empty());

        let second = rebuild_client("u1", "c1", "user:u1", Some(cursor), 2);
        assert!(!second["records"].as_array().unwrap().is_empty());
    }

    #[pg_test]
    fn test_rebuild_filters_soft_deleted() {
        setup_test_tables();
        register_client("u1", "c1");

        Spi::run(
            "INSERT INTO test_orders (id, user_id, title, deleted_at) VALUES
             ('bde10000-1111-1111-1111-111111111111', 'u1', 'Deleted', now())",
        )
        .unwrap();
        insert_edge(
            "test_orders",
            "bde10000-1111-1111-1111-111111111111",
            "user:u1",
        );

        let resp = rebuild_client("u1", "c1", "user:u1", None, 100);
        let deleted = resp["records"]
            .as_array()
            .unwrap()
            .iter()
            .any(|record| record["pk"]["id"].as_str() == Some("bde10000-1111-1111-1111-111111111111"));
        assert!(!deleted);
    }

    #[pg_test]
    fn test_rebuild_persists_final_scope_cursor() {
        setup_pull_fixtures();

        let resp = rebuild_client("u1", "c1", "user:u1", None, 1000);
        assert_eq!(resp["has_more"].as_bool(), Some(false));
        let final_scope_cursor = resp["final_scope_cursor"].as_str().unwrap();

        let stored: Option<i64> = Spi::get_one_with_args(
            "SELECT checkpoint FROM sync_client_checkpoints
             WHERE user_id = $1 AND client_id = $2 AND bucket_id = 'user:u1'",
            &["u1".into(), "c1".into()],
        )
        .unwrap();
        let parsed_checkpoint = Spi::connect(|client| {
            match crate::cursor_token::parse_scope_cursor(client, "user:u1", final_scope_cursor)
                .expect("final scope cursor should decode for rebuilt scope")
            {
                crate::cursor_token::ParsedScopeCursor::Current(checkpoint) => {
                    Ok::<i64, pgrx::spi::Error>(checkpoint)
                }
                crate::cursor_token::ParsedScopeCursor::Stale => {
                    panic!("rebuilt final scope cursor must not be stale")
                }
            }
        })
        .unwrap();
        assert_eq!(stored, Some(parsed_checkpoint));
    }

    #[pg_test]
    fn test_rotated_cursor_secret_marks_existing_cursor_stale() {
        setup_test_tables();

        let issued = issued_scope_cursor("global", 0);

        Spi::run(
            "UPDATE sync_runtime_state
             SET cursor_secret = 'rotated-secret-for-test',
                 updated_at = now()
             WHERE singleton = true",
        )
        .unwrap();

        let parsed = Spi::connect(|client| {
            crate::cursor_token::parse_scope_cursor(client, "global", &issued)
        })
        .unwrap();

        assert!(matches!(
            parsed,
            crate::cursor_token::ParsedScopeCursor::Stale
        ));
    }

    #[pg_test]
    fn test_rebuild_unsubscribed_errors() {
        setup_test_tables();
        register_client("u1", "c1");

        let resp = rebuild_client("u1", "c1", "team:other", None, 100);
        assert_eq!(resp["error"]["code"].as_str(), Some("invalid_request"));
    }

    // -----------------------------------------------------------------------
    // Compaction (5 tests)
    // -----------------------------------------------------------------------

    #[pg_test]
    fn test_compact_deactivates_stale() {
        setup_test_tables();
        register_client("u1", "c1");

        // Set client's last_sync_at to 30 days ago.
        Spi::run_with_args(
            "UPDATE sync_clients SET last_sync_at = now() - interval '30 days' \
             WHERE user_id = $1 AND client_id = $2",
            &["u1".into(), "c1".into()],
        )
        .unwrap();

        let resp: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_compact('7 days', 10000)").unwrap();
        let resp = resp.unwrap().0;

        assert!(resp["deactivated_clients"].as_i64().unwrap() >= 1);
    }

    #[pg_test]
    fn test_compact_deletes_below_safe() {
        setup_test_tables();
        // No clients registered, so safe_seq should be MAX(seq).
        insert_changelog("global", "test_products", "compact-1", 1);
        insert_changelog("global", "test_products", "compact-2", 1);

        let before: Option<i64> = Spi::get_one("SELECT count(*) FROM sync_changelog").unwrap();

        let resp: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_compact('7 days', 10000)").unwrap();
        let resp = resp.unwrap().0;

        let deleted = resp["deleted_entries"].as_i64().unwrap_or(0);
        // With no active clients, all entries should be deleted.
        assert!(deleted >= before.unwrap_or(0));
    }

    #[pg_test]
    fn test_compact_preserves_above_safe() {
        setup_test_tables();
        register_client("u1", "c1");

        // Insert entries.
        insert_changelog("user:u1", "test_orders", "preserve-1", 1);
        insert_changelog("user:u1", "test_orders", "preserve-2", 1);

        // Client has never pulled and seeded bucket checkpoints remain at 0.
        // safe_seq should be 0, so nothing gets deleted.
        let resp: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_compact('7 days', 10000)").unwrap();
        let resp = resp.unwrap().0;

        let deleted = resp["deleted_entries"].as_i64().unwrap_or(0);
        assert_eq!(
            deleted, 0,
            "no entries should be deleted when active client at checkpoint 0"
        );
    }

    #[pg_test]
    fn test_compact_safe_seq_uses_bucket_checkpoints() {
        setup_test_tables();
        register_client("u1", "c1");

        // Insert entries and advance only the per-bucket checkpoint.
        insert_changelog("user:u1", "test_orders", "both-src-1", 1);
        insert_changelog("user:u1", "test_orders", "both-src-2", 1);

        // Advance the bucket checkpoint to seq 1.
        Spi::run_with_args(
            "UPDATE sync_client_checkpoints SET checkpoint = 1 \
             WHERE user_id = $1 AND client_id = $2 AND bucket_id = 'user:u1'",
            &["u1".into(), "c1".into()],
        )
        .unwrap();

        // Compaction should use the active bucket checkpoint as safe_seq.
        let resp: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_compact('7 days', 10000)").unwrap();
        let resp = resp.unwrap().0;

        assert_eq!(resp["safe_seq"].as_i64(), Some(1));
    }

    #[pg_test]
    fn test_compact_no_active_clients() {
        setup_test_tables();

        insert_changelog("global", "test_products", "no-clients-1", 1);

        let resp: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_compact('7 days', 10000)").unwrap();
        let resp = resp.unwrap().0;

        // With no clients, all entries should be compactable.
        assert!(resp["deleted_entries"].as_i64().unwrap_or(0) >= 1);
    }

    // -----------------------------------------------------------------------
    // Schema/Debug (3 smoke tests)
    // -----------------------------------------------------------------------

    #[pg_test]
    fn test_schema_returns_columns() {
        setup_test_tables();

        let resp: Option<pgrx::JsonB> = Spi::get_one("SELECT synchro_schema_manifest()").unwrap();
        let resp = resp.unwrap().0;

        let tables = resp["manifest"]["tables"].as_array().unwrap();
        assert!(!tables.is_empty());
        // At least one table should have columns.
        let has_columns = tables.iter().any(|t| {
            t["columns"]
                .as_array()
                .map(|c| !c.is_empty())
                .unwrap_or(false)
        });
        assert!(has_columns, "schema should include column definitions");
    }

    #[pg_test]
    fn test_schema_manifest_validates_against_core_contract() {
        setup_test_tables();

        let resp: Option<pgrx::JsonB> = Spi::get_one("SELECT synchro_schema_manifest()").unwrap();
        let resp = resp.unwrap().0;

        assert!(resp.get("schema_version").is_some());
        assert!(resp.get("schema_hash").is_some());

        let manifest: synchro_core::contract::SchemaManifest =
            serde_json::from_value(resp["manifest"].clone()).unwrap();
        manifest.validate().unwrap();

        let orders = manifest
            .tables
            .iter()
            .find(|table| table.name == "test_orders")
            .expect("test_orders should be present in schema manifest");
        assert_eq!(orders.primary_key.as_ref(), Some(&vec!["id".to_string()]));
        assert_eq!(orders.updated_at_column.as_deref(), Some("updated_at"));
        assert_eq!(orders.deleted_at_column.as_deref(), Some("deleted_at"));

        let columns = orders.columns.as_ref().expect("columns should be present");
        assert!(columns
            .iter()
            .any(|column| { column.name == "user_id" && column.type_name == "string" }));
        assert!(columns
            .iter()
            .any(|column| { column.name == "updated_at" && column.type_name == "datetime" }));
        assert!(
            columns.iter().all(|column| column.name != "internal_notes"),
            "exclude_columns must not leak into the portable manifest"
        );
        assert!(columns.iter().all(|column| {
            synchro_core::contract::is_canonical_portable_type_name(&column.type_name)
        }));
    }

    #[pg_test]
    fn test_contract_info_reports_canonical_versions() {
        let resp: Option<pgrx::JsonB> = Spi::get_one("SELECT synchro_contract_info()").unwrap();
        let resp = resp.unwrap().0;

        assert_eq!(
            resp["sql_contract_version"].as_i64(),
            Some(crate::client::SQL_CONTRACT_VERSION as i64)
        );
        assert_eq!(
            resp["protocol_version"].as_i64(),
            Some(crate::client::PROTOCOL_VERSION as i64)
        );
        assert!(resp["extension_version"].as_str().is_some());
    }

    #[pg_test]
    fn test_schema_surfaces_use_sync_columns_as_canonical_shape() {
        setup_sync_columns_table();

        let manifest_resp: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_schema_manifest()").unwrap();
        let manifest_resp = manifest_resp.unwrap().0;
        let manifest: synchro_core::contract::SchemaManifest =
            serde_json::from_value(manifest_resp["manifest"].clone()).unwrap();
        manifest.validate().unwrap();

        let table = manifest
            .tables
            .iter()
            .find(|table| table.name == "test_sync_columns_items")
            .expect("portable schema manifest should include test_sync_columns_items");
        let columns = table.columns.as_ref().expect("columns should be present");
        assert!(columns.iter().any(|column| column.name == "title"));
        assert!(columns.iter().all(|column| column.name != "search_vector"));
        assert!(columns.iter().all(|column| column.name != "internal_notes"));

        let indexes = table.indexes.as_ref().expect("indexes should be present");
        assert!(indexes
            .iter()
            .any(|index| index.name == "idx_test_sync_columns_items_title"));
        assert!(indexes
            .iter()
            .all(|index| index.name != "idx_test_sync_columns_items_search_vector"));
    }

    #[pg_test]
    fn test_schema_surfaces_omit_missing_timestamp_columns() {
        setup_test_tables();

        let manifest_resp: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_schema_manifest()").unwrap();
        let manifest_resp = manifest_resp.unwrap().0;
        let manifest: synchro_core::contract::SchemaManifest =
            serde_json::from_value(manifest_resp["manifest"].clone()).unwrap();
        let bare_items = manifest
            .tables
            .iter()
            .find(|table| table.name == "test_bare_items")
            .expect("portable schema should include test_bare_items");
        assert!(bare_items.updated_at_column.is_none());
        assert!(bare_items.deleted_at_column.is_none());
    }

    #[pg_test]
    fn test_schema_manifest_emits_canonical_portable_types() {
        setup_portable_type_contract_table();

        let manifest_resp: Option<pgrx::JsonB> =
            Spi::get_one("SELECT synchro_schema_manifest()").unwrap();
        let manifest_resp = manifest_resp.unwrap().0;
        let manifest: synchro_core::contract::SchemaManifest =
            serde_json::from_value(manifest_resp["manifest"].clone()).unwrap();
        manifest.validate().unwrap();

        let table = manifest
            .tables
            .iter()
            .find(|table| table.name == "test_portable_type_contract")
            .expect("portable schema manifest should include test_portable_type_contract");
        let columns = table.columns.as_ref().expect("columns should be present");
        let types: std::collections::HashMap<_, _> = columns
            .iter()
            .map(|column| (column.name.as_str(), column.type_name.as_str()))
            .collect();

        assert_eq!(types.get("id"), Some(&"string"));
        assert_eq!(types.get("user_id"), Some(&"string"));
        assert_eq!(types.get("label"), Some(&"string"));
        assert_eq!(types.get("col_smallint"), Some(&"int"));
        assert_eq!(types.get("col_integer"), Some(&"int"));
        assert_eq!(types.get("col_bigint"), Some(&"int64"));
        assert_eq!(types.get("col_numeric"), Some(&"float"));
        assert_eq!(types.get("col_real"), Some(&"float"));
        assert_eq!(types.get("col_double"), Some(&"float"));
        assert_eq!(types.get("col_timestamp"), Some(&"datetime"));
        assert_eq!(types.get("col_interval"), Some(&"string"));
        assert_eq!(types.get("col_json"), Some(&"json"));
        assert_eq!(types.get("col_blob"), Some(&"bytes"));
        assert_eq!(types.get("col_text_array"), Some(&"json"));
        assert_eq!(types.get("col_int_array"), Some(&"json"));
        assert_eq!(types.get("col_inet"), Some(&"string"));
        assert_eq!(types.get("col_point"), Some(&"string"));
        assert_eq!(types.get("col_int4range"), Some(&"string"));
        assert_eq!(types.get("updated_at"), Some(&"datetime"));
        assert_eq!(types.get("deleted_at"), Some(&"datetime"));
        assert!(columns.iter().all(|column| {
            synchro_core::contract::is_canonical_portable_type_name(&column.type_name)
        }));
    }

    #[pg_test]
    fn test_schema_manifest_hash_ignores_bucket_sql_only_changes() {
        setup_test_tables();

        let before: Option<pgrx::JsonB> = Spi::get_one("SELECT synchro_schema_manifest()").unwrap();
        let before = before.unwrap().0;

        Spi::run(
            "SELECT synchro_register_table(
                'test_orders',
                $$SELECT ARRAY['alt:' || user_id] FROM test_orders WHERE id = $1::uuid$$,
                'id', 'updated_at', 'deleted_at', 'enabled',
                ARRAY['internal_notes']
            )",
        )
        .unwrap();

        let after: Option<pgrx::JsonB> = Spi::get_one("SELECT synchro_schema_manifest()").unwrap();
        let after = after.unwrap().0;

        assert_eq!(before["schema_version"], after["schema_version"]);
        assert_eq!(before["schema_hash"], after["schema_hash"]);
    }

    #[pg_test]
    fn test_debug_returns_state() {
        setup_test_tables();
        register_client("u1", "c1");

        let resp: Option<pgrx::JsonB> =
            Spi::get_one_with_args("SELECT synchro_debug($1, $2)", &["u1".into(), "c1".into()])
                .unwrap();
        let resp = resp.unwrap().0;

        assert!(resp.get("client").is_some());
        assert!(resp.get("buckets").is_some());
        let server_time = resp.get("server_time").and_then(|v| v.as_str()).unwrap();
        assert!(chrono::DateTime::parse_from_rfc3339(server_time).is_ok());
    }

    #[pg_test]
    fn test_tables_returns_registry() {
        setup_test_tables();

        let resp: Option<pgrx::JsonB> = Spi::get_one("SELECT synchro_tables()").unwrap();
        let resp = resp.unwrap().0;

        let tables = resp["tables"].as_array().unwrap();
        assert!(!tables.is_empty());
        let has_orders = tables
            .iter()
            .any(|t| t["table_name"].as_str() == Some("test_orders"));
        assert!(has_orders, "tables should include test_orders");
    }

    // -----------------------------------------------------------------------
    // Structured Error Responses (3 tests)
    // -----------------------------------------------------------------------

    #[pg_test]
    fn test_schema_mismatch_returns_jsonb() {
        setup_test_tables();
        register_client("u1", "c1");

        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2::jsonb)",
            &[
                "u1".into(),
                json!({
                    "client_id": "c1",
                    "batch_id": "batch-schema-mismatch",
                    "schema": { "version": 999, "hash": "wrong_hash" },
                    "mutations": []
                })
                .to_string()
                .into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        assert_eq!(resp["error"]["code"].as_str(), Some("schema_mismatch"));
        assert_eq!(resp["error"]["retryable"].as_bool(), Some(false));
        assert!(resp["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("client schema"));
    }

    #[pg_test]
    fn test_client_not_found_returns_jsonb() {
        setup_test_tables();

        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2::jsonb)",
            &[
                "nonexistent_user".into(),
                json!({
                    "client_id": "nonexistent_client",
                    "schema": { "version": 0, "hash": "" },
                    "scope_set_version": 0,
                    "scopes": {},
                    "limit": 100
                })
                .to_string()
                .into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        assert_eq!(resp["error"]["code"].as_str(), Some("invalid_request"));
        assert_eq!(
            resp["error"]["message"].as_str(),
            Some("client is not registered")
        );
        assert_eq!(resp["error"]["retryable"].as_bool(), Some(false));
    }

    #[pg_test]
    fn test_schema_mismatch_no_manifest() {
        // Before any tables are registered, schema manifest is empty.
        // Connect should accept any client schema when the server manifest is empty.
        Spi::run(
            "CREATE TABLE IF NOT EXISTS test_empty_table (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT DEFAULT ''
            )",
        )
        .unwrap();

        let resp = connect_client(
            "empty_user",
            json!({
                "client_id": "empty_client",
                "platform": "test",
                "app_version": "1.0.0",
                "protocol_version": 2,
                "schema": { "version": 1, "hash": "somehash" },
                "scope_set_version": 0,
                "known_scopes": {}
            }),
        );

        // Should succeed, not error.
        assert!(resp.get("error").is_none());
    }

    // -----------------------------------------------------------------------
    // GUC Integration (1 test)
    // -----------------------------------------------------------------------

    #[pg_test]
    fn test_clock_skew_guc_affects_lww() {
        setup_test_tables();
        register_client("u1", "c1");

        // Insert with known timestamp.
        Spi::run(
            "INSERT INTO test_orders (id, user_id, title, updated_at) VALUES
             ('00c5e571-1111-1111-1111-111111111111', 'u1', 'GUC Test',
              '2025-06-15T12:00:00.000Z')",
        )
        .unwrap();

        let resp1 = push_client(
            "u1",
            "c1",
            "batch-skew-default",
            json!([
                {
                    "mutation_id": "m-skew-default",
                    "table": "test_orders",
                    "op": "update",
                    "pk": { "id": "00c5e571-1111-1111-1111-111111111111" },
                    "client_version": "2025-06-15T11:59:59.800Z",
                    "columns": { "title": "Within 500ms" }
                }
            ]),
        );
        let status1 = resp1["accepted"][0]["status"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(status1, "applied", "client should win with 500ms tolerance");

        // Now set tolerance to 0ms.
        Spi::run("SET synchro.clock_skew_tolerance_ms = 0").unwrap();

        // Re-read the record's updated_at (it was updated by the previous push).
        let _updated_at: Option<String> = Spi::get_one(
            "SELECT updated_at::text FROM test_orders \
             WHERE id = '00c5e571-1111-1111-1111-111111111111'",
        )
        .unwrap();

        // Push again with a client timestamp behind the (now-updated) server timestamp.
        // With 0ms tolerance, the 200ms gap means server wins.
        let resp2 = push_client(
            "u1",
            "c1",
            "batch-skew-zero",
            json!([
                {
                    "mutation_id": "m-skew-zero",
                    "table": "test_orders",
                    "op": "update",
                    "pk": { "id": "00c5e571-1111-1111-1111-111111111111" },
                    "client_version": "2020-01-01T00:00:00Z",
                    "columns": { "title": "With 0ms" }
                }
            ]),
        );
        let status2 = resp2["rejected"][0]["status"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(status2, "conflict", "server should win with 0ms tolerance");

        // Reset GUC.
        Spi::run("RESET synchro.clock_skew_tolerance_ms").unwrap();
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // The extension registers a Postmaster-context GUC (synchro.auto_start)
        // and a background worker, both of which require the shared library to
        // be loaded at server startup, not via CREATE EXTENSION.
        vec![
            "shared_preload_libraries = 'synchro_pg'",
            "synchro.auto_start = off",
        ]
    }
}
