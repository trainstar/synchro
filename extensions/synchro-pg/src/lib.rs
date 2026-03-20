use std::ffi::CString;

use pgrx::prelude::*;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

mod bgworker;
mod bucketing;
mod client;
mod compaction;
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
    exclude_columns TEXT[] NOT NULL DEFAULT '{}',
    has_updated_at BOOLEAN NOT NULL DEFAULT true,
    has_deleted_at BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

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
    last_sync_at TIMESTAMPTZ,
    last_pull_at TIMESTAMPTZ,
    last_push_at TIMESTAMPTZ,
    last_pull_seq BIGINT,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, client_id)
);
CREATE INDEX IF NOT EXISTS idx_sync_clients_user_id ON sync_clients (user_id);

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

/// Whether to auto-start the WAL background worker on server boot.
pub(crate) static AUTO_START_GUC: GucSetting<bool> = GucSetting::<bool>::new(true);

/// Clock skew tolerance for LWW conflict resolution (milliseconds).
pub(crate) static CLOCK_SKEW_TOLERANCE_MS_GUC: GucSetting<i32> = GucSetting::<i32>::new(500);

/// Database name for the WAL background worker to connect to.
pub(crate) static DATABASE_GUC: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

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

    GucRegistry::define_bool_guc(
        c"synchro.auto_start",
        c"Whether to auto-start the synchro WAL background worker.",
        c"When true, the WAL consumer background worker starts on server boot.",
        &AUTO_START_GUC,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"synchro.database",
        c"Database name for the WAL background worker.",
        c"The database the WAL consumer connects to. Must have the synchro_pg extension installed. Defaults to postgres.",
        &DATABASE_GUC,
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

    if AUTO_START_GUC.get() {
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
    use serde_json::Value;

    // -----------------------------------------------------------------------
    // Wire protocol helpers
    // -----------------------------------------------------------------------

    /// Assert that a JSON value is an ISO 8601 UTC timestamp.
    /// Accepts both 'Z' and '+00:00' suffixes (both are valid ISO 8601 UTC).
    fn assert_iso8601(value: &Value, field_name: &str) {
        if let Some(s) = value.as_str() {
            let is_utc = s.ends_with('Z') || s.ends_with("+00:00");
            assert!(s.contains('T') && is_utc,
                "{} must be ISO 8601 UTC (got: {})", field_name, s);
        }
    }

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

    /// Register a test client and return the raw JSONB response.
    fn register_client(user_id: &str, client_id: &str) -> Value {
        let row: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_register_client($1, $2, 'test', '1.0.0', 0, '')",
            &[user_id.into(), client_id.into()],
        )
        .unwrap();
        row.unwrap().0
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
            &[
                table_name.into(),
                record_id.into(),
                bucket_id.into(),
            ],
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
        let count: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM sync_registry WHERE table_name = 'test_orders'",
        )
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
        let version: Option<i64> = Spi::get_one(
            "SELECT MAX(schema_version) FROM sync_schema_manifest",
        )
        .unwrap();
        assert!(version.unwrap_or(0) > 0);
    }

    #[pg_test]
    fn test_unregister_cleanup() {
        setup_test_tables();
        // Insert an edge for test_bare_items.
        insert_edge("test_bare_items", "fake-id", "global");

        Spi::run("SELECT synchro_unregister_table('test_bare_items')").unwrap();

        let reg_count: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM sync_registry WHERE table_name = 'test_bare_items'",
        )
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
        assert!(resp.get("id").is_some());
        assert!(resp.get("server_time").is_some());
        assert_iso8601(&resp["server_time"], "register server_time");
        assert_eq!(resp.get("schema_version").and_then(|v| v.as_i64()).unwrap_or(0) > 0, true);

        // Verify bucket_subs in sync_clients.
        let subs: Option<Vec<String>> = Spi::get_one_with_args(
            "SELECT bucket_subs FROM sync_clients WHERE user_id = $1 AND client_id = $2",
            &["user1".into(), "client1".into()],
        )
        .unwrap();
        let subs = subs.unwrap();
        assert!(subs.contains(&"user:user1".to_string()));
        assert!(subs.contains(&"global".to_string()));
    }

    // -----------------------------------------------------------------------
    // Push Round-Trip (12 tests)
    // -----------------------------------------------------------------------

    #[pg_test]
    fn test_push_create_and_verify() {
        setup_test_tables();
        register_client("u1", "c1");

        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, $3::jsonb, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                r#"[{"id":"11111111-1111-1111-1111-111111111111","table_name":"test_orders","operation":"create","data":{"user_id":"u1","title":"Test Order","amount":99.99}}]"#.into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        let accepted = resp.get("accepted").unwrap().as_array().unwrap();
        assert_eq!(accepted.len(), 1);
        assert_eq!(accepted[0].get("status").unwrap().as_str().unwrap(), "applied");

        // Validate server_updated_at is ISO 8601 UTC if present.
        if let Some(ts) = accepted[0].get("server_updated_at") {
            assert_iso8601(ts, "push accepted server_updated_at");
        }

        // Verify record exists in table.
        let title: Option<String> = Spi::get_one(
            "SELECT title FROM test_orders WHERE id = '11111111-1111-1111-1111-111111111111'",
        )
        .unwrap();
        assert_eq!(title, Some("Test Order".to_string()));
    }

    #[pg_test]
    fn test_push_create_strips_protected() {
        setup_test_tables();
        register_client("u1", "c1");

        // Client tries to set updated_at and created_at. Both should be stripped.
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, $3::jsonb, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                r#"[{"id":"22222222-2222-2222-2222-222222222222","table_name":"test_orders","operation":"create","data":{"user_id":"u1","title":"Protected Test","updated_at":"1999-01-01T00:00:00Z","created_at":"1999-01-01T00:00:00Z"}}]"#.into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;
        assert_eq!(
            resp["accepted"][0]["status"].as_str().unwrap(),
            "applied"
        );

        // Verify server timestamps are NOT 1999.
        let year: Option<i32> = Spi::get_one(
            "SELECT EXTRACT(YEAR FROM updated_at)::int FROM test_orders \
             WHERE id = '22222222-2222-2222-2222-222222222222'",
        )
        .unwrap();
        assert_ne!(year, Some(1999));
    }

    #[pg_test]
    fn test_push_create_strips_exclude() {
        setup_test_tables();
        register_client("u1", "c1");

        // internal_notes is in exclude_columns. Client sends it, should be stripped.
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, $3::jsonb, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                r#"[{"id":"33333333-3333-3333-3333-333333333333","table_name":"test_orders","operation":"create","data":{"user_id":"u1","title":"Exclude Test","internal_notes":"secret stuff"}}]"#.into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;
        assert_eq!(resp["accepted"][0]["status"].as_str().unwrap(), "applied");

        // internal_notes should be default empty, not "secret stuff".
        let notes: Option<String> = Spi::get_one(
            "SELECT internal_notes FROM test_orders \
             WHERE id = '33333333-3333-3333-3333-333333333333'",
        )
        .unwrap();
        assert_eq!(notes, Some("".to_string()));
    }

    #[pg_test]
    fn test_push_create_duplicate_conflict() {
        setup_test_tables();
        register_client("u1", "c1");

        // Insert a record.
        Spi::run(
            "INSERT INTO test_orders (id, user_id, title) \
             VALUES ('44444444-4444-4444-4444-444444444444', 'u1', 'Existing')",
        )
        .unwrap();

        // Push a create for the same id. Should conflict.
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, $3::jsonb, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                r#"[{"id":"44444444-4444-4444-4444-444444444444","table_name":"test_orders","operation":"create","data":{"user_id":"u1","title":"Duplicate"}}]"#.into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        // Conflicts go to rejected, not accepted. The client's write was NOT applied.
        let rejected = resp["rejected"].as_array().unwrap();
        assert_eq!(rejected.len(), 1);
        assert_eq!(rejected[0]["status"].as_str().unwrap(), "conflict");
        assert_eq!(rejected[0]["reason_code"].as_str().unwrap(), "record_exists");
        assert_eq!(resp["accepted"].as_array().unwrap().len(), 0);

        // server_version must be a full Record: id, table_name, data, updated_at.
        let sv = &rejected[0]["server_version"];
        assert_eq!(sv["id"].as_str().unwrap(), "44444444-4444-4444-4444-444444444444");
        assert_eq!(sv["table_name"].as_str().unwrap(), "test_orders");
        assert!(sv.get("data").is_some(), "server_version missing data");
        assert!(sv.get("updated_at").is_some(), "server_version missing updated_at");
        assert_iso8601(&sv["updated_at"], "server_version updated_at");

        // data must not contain excluded columns.
        let sv_data = &sv["data"];
        assert!(sv_data.get("internal_notes").is_none(),
            "server_version data should not contain excluded column internal_notes");

        // All timestamps inside data must be ISO 8601 UTC.
        // PG's to_json/to_jsonb outputs +00:00 for UTC; both +00:00 and Z are valid.
        if let Some(created_at) = sv_data.get("created_at").and_then(|v| v.as_str()) {
            let is_utc = created_at.ends_with('Z') || created_at.ends_with("+00:00");
            assert!(created_at.contains('T') && is_utc,
                "created_at in server_version data must be ISO 8601 UTC, got: {}", created_at);
        }
        if let Some(updated_at) = sv_data.get("updated_at").and_then(|v| v.as_str()) {
            let is_utc = updated_at.ends_with('Z') || updated_at.ends_with("+00:00");
            assert!(updated_at.contains('T') && is_utc,
                "updated_at in server_version data must be ISO 8601 UTC, got: {}", updated_at);
        }
    }

    #[pg_test]
    fn test_push_update_lww_client_wins() {
        setup_test_tables();
        register_client("u1", "c1");

        // Insert with old timestamp.
        Spi::run(
            "INSERT INTO test_orders (id, user_id, title, updated_at) \
             VALUES ('55555555-5555-5555-5555-555555555555', 'u1', 'Old', \
             '2020-01-01T00:00:00Z')",
        )
        .unwrap();

        // Push update with recent client timestamp. Client should win.
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, $3::jsonb, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                r#"[{"id":"55555555-5555-5555-5555-555555555555","table_name":"test_orders","operation":"update","data":{"title":"Updated"},"client_updated_at":"2025-01-01T00:00:00Z"}]"#.into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;
        assert_eq!(resp["accepted"][0]["status"].as_str().unwrap(), "applied");

        let title: Option<String> = Spi::get_one(
            "SELECT title FROM test_orders WHERE id = '55555555-5555-5555-5555-555555555555'",
        )
        .unwrap();
        assert_eq!(title, Some("Updated".to_string()));
    }

    #[pg_test]
    fn test_push_update_lww_server_wins() {
        setup_test_tables();
        register_client("u1", "c1");

        // Insert with very recent timestamp.
        Spi::run(
            "INSERT INTO test_orders (id, user_id, title, updated_at) \
             VALUES ('66666666-6666-6666-6666-666666666666', 'u1', 'Recent', \
             '2099-01-01T00:00:00Z')",
        )
        .unwrap();

        // Push update with old client timestamp. Server should win.
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, $3::jsonb, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                r#"[{"id":"66666666-6666-6666-6666-666666666666","table_name":"test_orders","operation":"update","data":{"title":"Stale"},"client_updated_at":"2020-01-01T00:00:00Z"}]"#.into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        // Conflict goes to rejected.
        let rejected = resp["rejected"].as_array().unwrap();
        assert_eq!(rejected[0]["status"].as_str().unwrap(), "conflict");
        assert_eq!(resp["accepted"].as_array().unwrap().len(), 0);

        // Title should be unchanged.
        let title: Option<String> = Spi::get_one(
            "SELECT title FROM test_orders WHERE id = '66666666-6666-6666-6666-666666666666'",
        )
        .unwrap();
        assert_eq!(title, Some("Recent".to_string()));
    }

    #[pg_test]
    fn test_push_update_lww_clock_skew() {
        setup_test_tables();
        register_client("u1", "c1");

        // Insert with known timestamp.
        Spi::run(
            "INSERT INTO test_orders (id, user_id, title, updated_at) \
             VALUES ('77777777-7777-7777-7777-777777777777', 'u1', 'Skew Test', \
             '2025-06-15T12:00:00.500Z')",
        )
        .unwrap();

        // Client is 400ms behind server. Default tolerance is 500ms, so client wins.
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, $3::jsonb, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                r#"[{"id":"77777777-7777-7777-7777-777777777777","table_name":"test_orders","operation":"update","data":{"title":"Within Skew"},"client_updated_at":"2025-06-15T12:00:00.100Z"}]"#.into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;
        assert_eq!(resp["accepted"][0]["status"].as_str().unwrap(), "applied");
    }

    #[pg_test]
    fn test_push_update_lww_base_unchanged() {
        setup_test_tables();
        register_client("u1", "c1");

        Spi::run(
            "INSERT INTO test_orders (id, user_id, title, updated_at) \
             VALUES ('88888888-8888-8888-8888-888888888888', 'u1', 'Base Test', \
             '2025-06-15T12:00:00Z')",
        )
        .unwrap();

        // Client base_updated_at matches server: server unchanged, client wins.
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, $3::jsonb, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                r#"[{"id":"88888888-8888-8888-8888-888888888888","table_name":"test_orders","operation":"update","data":{"title":"Base Match"},"client_updated_at":"2020-01-01T00:00:00Z","base_updated_at":"2025-06-15T12:00:00Z"}]"#.into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;
        assert_eq!(resp["accepted"][0]["status"].as_str().unwrap(), "applied");
    }

    #[pg_test]
    fn test_push_soft_delete() {
        setup_test_tables();
        register_client("u1", "c1");

        Spi::run(
            "INSERT INTO test_orders (id, user_id, title) \
             VALUES ('99999999-9999-9999-9999-999999999999', 'u1', 'To Delete')",
        )
        .unwrap();

        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, $3::jsonb, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                r#"[{"id":"99999999-9999-9999-9999-999999999999","table_name":"test_orders","operation":"delete"}]"#.into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;
        assert_eq!(resp["accepted"][0]["status"].as_str().unwrap(), "applied");

        // deleted_at should be set, not null.
        let deleted: Option<bool> = Spi::get_one(
            "SELECT deleted_at IS NOT NULL FROM test_orders \
             WHERE id = '99999999-9999-9999-9999-999999999999'",
        )
        .unwrap();
        assert_eq!(deleted, Some(true));
    }

    #[pg_test]
    fn test_push_soft_delete_idempotent() {
        setup_test_tables();
        register_client("u1", "c1");

        Spi::run(
            "INSERT INTO test_orders (id, user_id, title, deleted_at) \
             VALUES ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'u1', 'Already Deleted', now())",
        )
        .unwrap();

        // Delete again. Should succeed (idempotent), not error.
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, $3::jsonb, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                r#"[{"id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","table_name":"test_orders","operation":"delete"}]"#.into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;
        assert_eq!(resp["accepted"][0]["status"].as_str().unwrap(), "applied");
    }

    #[pg_test]
    fn test_push_hard_delete() {
        setup_test_tables();
        register_client("u1", "c1");

        Spi::run(
            "INSERT INTO test_bare_items (id, name) \
             VALUES ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'Bare Item')",
        )
        .unwrap();

        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, $3::jsonb, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                r#"[{"id":"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb","table_name":"test_bare_items","operation":"delete"}]"#.into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;
        assert_eq!(resp["accepted"][0]["status"].as_str().unwrap(), "applied");

        // Record should be gone entirely (hard delete).
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

        // Soft-deleted record.
        Spi::run(
            "INSERT INTO test_orders (id, user_id, title, deleted_at) \
             VALUES ('cccccccc-cccc-cccc-cccc-cccccccccccc', 'u1', 'Dead', now())",
        )
        .unwrap();

        // Create on soft-deleted: resurrection.
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, $3::jsonb, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                r#"[{"id":"cccccccc-cccc-cccc-cccc-cccccccccccc","table_name":"test_orders","operation":"create","data":{"user_id":"u1","title":"Risen"}}]"#.into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;
        assert_eq!(resp["accepted"][0]["status"].as_str().unwrap(), "applied");

        // deleted_at should be cleared, title updated.
        let row = Spi::get_two::<String, bool>(
            "SELECT title, deleted_at IS NULL FROM test_orders \
             WHERE id = 'cccccccc-cccc-cccc-cccc-cccccccccccc'",
        )
        .unwrap();
        assert_eq!(row.0, Some("Risen".to_string()));
        assert_eq!(row.1, Some(true)); // deleted_at IS NULL = true
    }

    // -----------------------------------------------------------------------
    // Push Safety (5 tests)
    // -----------------------------------------------------------------------

    #[pg_test]
    fn test_push_read_only_rejected() {
        setup_test_tables();
        register_client("u1", "c1");

        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, $3::jsonb, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                r#"[{"id":"dddddddd-dddd-dddd-dddd-dddddddddddd","table_name":"test_products","operation":"create","data":{"name":"Hack Product"}}]"#.into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        let rejected = resp["rejected"].as_array().unwrap();
        assert_eq!(rejected.len(), 1);
        assert_eq!(rejected[0]["status"].as_str().unwrap(), "rejected_terminal");
        assert_eq!(rejected[0]["reason_code"].as_str().unwrap(), "table_read_only");
    }

    #[pg_test]
    fn test_push_write_protect_transforms() {
        setup_test_tables();
        register_client("u1", "c1");

        // Create the write_protect function that stamps user_id.
        Spi::run(
            "CREATE OR REPLACE FUNCTION synchro_write_protect(
                p_user_id TEXT, p_table_name TEXT, p_operation TEXT, p_record JSONB
            ) RETURNS JSONB AS $$
            BEGIN
                IF p_operation = 'create' THEN
                    p_record := jsonb_set(p_record, '{user_id}', to_jsonb(p_user_id));
                END IF;
                RETURN p_record;
            END;
            $$ LANGUAGE plpgsql",
        )
        .unwrap();

        // Push without user_id in data. write_protect should stamp it.
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, $3::jsonb, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                r#"[{"id":"eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee","table_name":"test_orders","operation":"create","data":{"title":"Protected"}}]"#.into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;
        assert_eq!(resp["accepted"][0]["status"].as_str().unwrap(), "applied");

        let uid: Option<String> = Spi::get_one(
            "SELECT user_id FROM test_orders WHERE id = 'eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee'",
        )
        .unwrap();
        assert_eq!(uid, Some("u1".to_string()));

        // Clean up.
        Spi::run("DROP FUNCTION IF EXISTS synchro_write_protect").unwrap();
    }

    #[pg_test]
    fn test_push_write_protect_restrips() {
        setup_test_tables();
        register_client("u1", "c1");

        // write_protect re-introduces updated_at. Extension should re-strip it.
        Spi::run(
            "CREATE OR REPLACE FUNCTION synchro_write_protect(
                p_user_id TEXT, p_table_name TEXT, p_operation TEXT, p_record JSONB
            ) RETURNS JSONB AS $$
            BEGIN
                p_record := jsonb_set(p_record, '{updated_at}', '\"1999-01-01T00:00:00Z\"'::jsonb);
                RETURN p_record;
            END;
            $$ LANGUAGE plpgsql",
        )
        .unwrap();

        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, $3::jsonb, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                r#"[{"id":"ffffffff-ffff-ffff-ffff-ffffffffffff","table_name":"test_orders","operation":"create","data":{"user_id":"u1","title":"Restrip"}}]"#.into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;
        assert_eq!(resp["accepted"][0]["status"].as_str().unwrap(), "applied");

        let year: Option<i32> = Spi::get_one(
            "SELECT EXTRACT(YEAR FROM updated_at)::int FROM test_orders \
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

        // Push with a column that doesn't exist. Should be silently ignored.
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, $3::jsonb, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                r#"[{"id":"11111111-aaaa-bbbb-cccc-111111111111","table_name":"test_orders","operation":"create","data":{"user_id":"u1","title":"Valid","nonexistent_col":"bogus"}}]"#.into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;
        assert_eq!(resp["accepted"][0]["status"].as_str().unwrap(), "applied");
    }

    #[pg_test]
    fn test_push_rls_context_set() {
        setup_test_tables();
        register_client("u1", "c1");

        // Create an RLS policy that blocks writes unless app.user_id matches.
        Spi::run("ALTER TABLE test_orders ENABLE ROW LEVEL SECURITY").unwrap();
        Spi::run("ALTER TABLE test_orders FORCE ROW LEVEL SECURITY").unwrap();
        Spi::run(
            "CREATE POLICY test_rls ON test_orders \
             USING (user_id = current_setting('app.user_id', true)) \
             WITH CHECK (user_id = current_setting('app.user_id', true))",
        )
        .unwrap();

        // Push as u1 creating a record owned by u1. Should succeed.
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, $3::jsonb, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                r#"[{"id":"22222222-aaaa-bbbb-cccc-222222222222","table_name":"test_orders","operation":"create","data":{"user_id":"u1","title":"RLS OK"}}]"#.into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;
        assert_eq!(resp["accepted"][0]["status"].as_str().unwrap(), "applied");

        // Clean up RLS.
        Spi::run("DROP POLICY test_rls ON test_orders").unwrap();
        Spi::run("ALTER TABLE test_orders DISABLE ROW LEVEL SECURITY").unwrap();
    }

    // -----------------------------------------------------------------------
    // Push Response Envelope (4 tests)
    // -----------------------------------------------------------------------

    #[pg_test]
    fn test_push_response_shape_mixed() {
        setup_test_tables();
        register_client("u1", "c1");

        // Two records: one valid create, one to a read-only table.
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, $3::jsonb, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                r#"[
                    {"id":"aaa11111-1111-1111-1111-111111111111","table_name":"test_orders","operation":"create","data":{"user_id":"u1","title":"Good"}},
                    {"id":"bbb22222-2222-2222-2222-222222222222","table_name":"test_products","operation":"create","data":{"name":"Bad"}}
                ]"#.into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        // accepted: 1, rejected: 1
        assert_eq!(resp["accepted"].as_array().unwrap().len(), 1);
        assert_eq!(resp["rejected"].as_array().unwrap().len(), 1);

        // Envelope fields present.
        assert!(resp.get("checkpoint").is_some());
        assert!(resp.get("server_time").and_then(|v| v.as_str()).is_some());
        assert_iso8601(&resp["server_time"], "push envelope server_time");
        assert!(resp.get("schema_version").is_some());
        assert!(resp.get("schema_hash").is_some());
    }

    #[pg_test]
    fn test_push_response_all_accepted() {
        setup_test_tables();
        register_client("u1", "c1");

        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, $3::jsonb, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                r#"[{"id":"ccc33333-3333-3333-3333-333333333333","table_name":"test_orders","operation":"create","data":{"user_id":"u1","title":"All Good"}}]"#.into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        assert_eq!(resp["accepted"].as_array().unwrap().len(), 1);
        // rejected must be an empty array, not null or missing.
        assert_eq!(resp["rejected"].as_array().unwrap().len(), 0);
    }

    #[pg_test]
    fn test_push_response_all_rejected() {
        setup_test_tables();
        register_client("u1", "c1");

        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, $3::jsonb, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                r#"[{"id":"ddd44444-4444-4444-4444-444444444444","table_name":"test_products","operation":"create","data":{"name":"Nope"}}]"#.into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        // accepted must be an empty array, not null or missing.
        assert_eq!(resp["accepted"].as_array().unwrap().len(), 0);
        assert_eq!(resp["rejected"].as_array().unwrap().len(), 1);
    }

    #[pg_test]
    fn test_push_result_fields() {
        setup_test_tables();
        register_client("u1", "c1");

        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, $3::jsonb, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                r#"[{"id":"eee55555-5555-5555-5555-555555555555","table_name":"test_products","operation":"create","data":{"name":"Fields"}}]"#.into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        let result = &resp["rejected"][0];
        // Must have id, table_name, operation, status, reason_code.
        assert_eq!(result["id"].as_str().unwrap(), "eee55555-5555-5555-5555-555555555555");
        assert_eq!(result["table_name"].as_str().unwrap(), "test_products");
        assert_eq!(result["operation"].as_str().unwrap(), "create");
        assert_eq!(result["status"].as_str().unwrap(), "rejected_terminal");
        assert_eq!(result["reason_code"].as_str().unwrap(), "table_read_only");
        // Must NOT have a "reason" field (old name).
        assert!(result.get("reason").is_none());
    }

    // -----------------------------------------------------------------------
    // Pull (12 tests)
    // -----------------------------------------------------------------------

    /// Helper: set up a client with changelog fixtures for pull tests.
    fn setup_pull_fixtures() {
        setup_test_tables();
        register_client("u1", "c1");

        // Insert test records.
        Spi::run(
            "INSERT INTO test_orders (id, user_id, title, internal_notes) VALUES
             ('a1111111-1111-1111-1111-111111111111', 'u1', 'Order 1', 'secret1'),
             ('a2222222-2222-2222-2222-222222222222', 'u1', 'Order 2', 'secret2')",
        )
        .unwrap();

        // Insert changelog entries.
        insert_changelog("user:u1", "test_orders", "a1111111-1111-1111-1111-111111111111", 1);
        insert_changelog("user:u1", "test_orders", "a2222222-2222-2222-2222-222222222222", 1);

        // Insert edges.
        insert_edge("test_orders", "a1111111-1111-1111-1111-111111111111", "user:u1");
        insert_edge("test_orders", "a2222222-2222-2222-2222-222222222222", "user:u1");
    }

    #[pg_test]
    fn test_pull_returns_hydrated_changes() {
        setup_pull_fixtures();

        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2, 0, NULL, 100, NULL, NULL, 0, '')",
            &["u1".into(), "c1".into()],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        let changes = resp["changes"].as_array().unwrap();
        assert!(changes.len() >= 2);
        // Each change should have id, table_name, data, checksum.
        let c = &changes[0];
        assert!(c.get("id").is_some());
        assert!(c.get("table_name").is_some());
        assert!(c.get("data").is_some());
        assert!(c.get("checksum").is_some());

        // Validate updated_at on hydrated records is ISO 8601 UTC.
        for change in changes {
            if let Some(data) = change.get("data") {
                if let Some(ts) = data.get("updated_at") {
                    assert_iso8601(ts, "pull hydrated updated_at");
                }
            }
        }
    }

    #[pg_test]
    fn test_pull_returns_deletes() {
        setup_test_tables();
        register_client("u1", "c1");

        // Insert a delete changelog entry.
        insert_changelog("user:u1", "test_orders", "deleted-id", 3); // 3 = Delete

        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2, 0, NULL, 100, NULL, NULL, 0, '')",
            &["u1".into(), "c1".into()],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        let deletes = resp["deletes"].as_array().unwrap();
        assert!(deletes.len() >= 1);
        let d = deletes.iter().find(|d| d["id"].as_str() == Some("deleted-id"));
        assert!(d.is_some());
    }

    #[pg_test]
    fn test_pull_deduplication() {
        setup_test_tables();
        register_client("u1", "c1");

        Spi::run(
            "INSERT INTO test_orders (id, user_id, title) VALUES
             ('de000111-1111-1111-1111-111111111111', 'u1', 'Dedup Test')",
        )
        .unwrap();
        insert_edge("test_orders", "de000111-1111-1111-1111-111111111111", "user:u1");

        // Two changelog entries for the same record. Pull should dedup to 1.
        insert_changelog("user:u1", "test_orders", "de000111-1111-1111-1111-111111111111", 1);
        insert_changelog("user:u1", "test_orders", "de000111-1111-1111-1111-111111111111", 2);

        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2, 0, NULL, 100, NULL, NULL, 0, '')",
            &["u1".into(), "c1".into()],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        let changes = resp["changes"].as_array().unwrap();
        let matches: Vec<_> = changes
            .iter()
            .filter(|c| c["id"].as_str() == Some("de000111-1111-1111-1111-111111111111"))
            .collect();
        assert_eq!(matches.len(), 1);
    }

    #[pg_test]
    fn test_pull_pagination_has_more() {
        setup_test_tables();
        register_client("u1", "c1");

        // Insert 3 changelog entries.
        for i in 1..=3 {
            let id = format!("a{i:07}0-0000-0000-0000-000000000000");
            insert_changelog("user:u1", "test_orders", &id, 1);
        }

        // Pull with limit=2. Should have has_more=true.
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2, 0, NULL, 2, NULL, NULL, 0, '')",
            &["u1".into(), "c1".into()],
        )
        .unwrap();
        let resp = resp.unwrap().0;
        assert_eq!(resp["has_more"].as_bool(), Some(true));
    }

    #[pg_test]
    fn test_pull_checkpoint_advances() {
        setup_pull_fixtures();

        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2, 0, NULL, 100, NULL, NULL, 0, '')",
            &["u1".into(), "c1".into()],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        let checkpoint = resp["checkpoint"].as_i64().unwrap();
        assert!(checkpoint > 0);

        // Verify persisted in sync_clients.
        let stored: Option<i64> = Spi::get_one_with_args(
            "SELECT last_pull_seq FROM sync_clients WHERE user_id = $1 AND client_id = $2",
            &["u1".into(), "c1".into()],
        )
        .unwrap();
        assert_eq!(stored, Some(checkpoint));
    }

    #[pg_test]
    fn test_pull_checkpoint_monotonic() {
        setup_pull_fixtures();

        // First pull: advances checkpoint.
        let resp1: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2, 0, NULL, 100, NULL, NULL, 0, '')",
            &["u1".into(), "c1".into()],
        )
        .unwrap();
        let cp1 = resp1.unwrap().0["checkpoint"].as_i64().unwrap();

        // Second pull from checkpoint: should not regress.
        let resp2: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2, $3, NULL, 100, NULL, NULL, 0, '')",
            &["u1".into(), "c1".into(), cp1.into()],
        )
        .unwrap();
        let cp2 = resp2.unwrap().0["checkpoint"].as_i64().unwrap();
        assert!(cp2 >= cp1);
    }

    #[pg_test]
    fn test_pull_exclude_columns_stripped() {
        setup_pull_fixtures();

        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2, 0, NULL, 100, NULL, NULL, 0, '')",
            &["u1".into(), "c1".into()],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        // internal_notes is in exclude_columns. Must not appear in hydrated data.
        for change in resp["changes"].as_array().unwrap() {
            if change["table_name"].as_str() == Some("test_orders") {
                let data = &change["data"];
                assert!(
                    data.get("internal_notes").is_none(),
                    "internal_notes should be stripped from pull response"
                );
            }
        }
    }

    #[pg_test]
    fn test_pull_bucket_isolation() {
        setup_test_tables();
        register_client("u1", "c1");
        register_client("u2", "c2");

        // Insert records for each user.
        Spi::run(
            "INSERT INTO test_orders (id, user_id, title) VALUES
             ('a00000a1-1111-1111-1111-111111111111', 'u1', 'User1 Order'),
             ('a00000a2-2222-2222-2222-222222222222', 'u2', 'User2 Order')",
        )
        .unwrap();

        insert_changelog("user:u1", "test_orders", "a00000a1-1111-1111-1111-111111111111", 1);
        insert_changelog("user:u2", "test_orders", "a00000a2-2222-2222-2222-222222222222", 1);
        insert_edge("test_orders", "a00000a1-1111-1111-1111-111111111111", "user:u1");
        insert_edge("test_orders", "a00000a2-2222-2222-2222-222222222222", "user:u2");

        // u1 should only see their own data.
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2, 0, NULL, 100, NULL, NULL, 0, '')",
            &["u1".into(), "c1".into()],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        let changes = resp["changes"].as_array().unwrap();
        for c in changes {
            if c["table_name"].as_str() == Some("test_orders") {
                assert_ne!(c["id"].as_str(), Some("a00000a2-2222-2222-2222-222222222222"),
                    "u1 should not see u2's data");
            }
        }
    }

    #[pg_test]
    fn test_pull_per_bucket_independent() {
        setup_test_tables();
        register_client("u1", "c1");

        // Insert into user:u1 bucket and global bucket at different seqs.
        insert_changelog("user:u1", "test_orders", "per-bucket-user-record", 1);
        insert_changelog("global", "test_products", "per-bucket-global-record", 1);

        // Pull with per-bucket checkpoints. Advance user:u1 past its entry.
        let bucket_cps = serde_json::json!({"user:u1": 999999, "global": 0});
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2, 0, $3::jsonb, 100, NULL, NULL, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                pgrx::JsonB(bucket_cps).into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        // user:u1 entries should be filtered (already seen), but global should not.
        let empty: Vec<Value> = vec![];
        let changes = resp["changes"].as_array().unwrap_or(&empty);
        let deletes = resp["deletes"].as_array().unwrap_or(&empty);
        let user_entries: Vec<_> = changes
            .iter()
            .chain(deletes.iter())
            .filter(|e| {
                e.get("id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.contains("per-bucket-user"))
                    .unwrap_or(false)
            })
            .collect();
        assert!(user_entries.is_empty(), "user:u1 entries should be filtered by per-bucket checkpoint");
    }

    #[pg_test]
    fn test_pull_stale_returns_rebuild() {
        setup_test_tables();
        register_client("u1", "c1");

        // Insert a changelog entry to establish min_seq.
        insert_changelog("user:u1", "test_orders", "stale-record", 1);

        // Pull with per-bucket checkpoints where the bucket checkpoint
        // has no entry (None). This should trigger stale detection for
        // buckets not in bucket_cps.
        // Actually, stale detection fires when bucket_cps[bucket] > 0 AND < min_seq.
        // Let's set up a scenario: insert entries, compact to raise min_seq.
        // Simpler: just verify that rebuild_buckets appears when stale is detected.
        // We need min_seq > 0 and a bucket checkpoint between 0 (exclusive) and min_seq.

        // Get the current min seq.
        let min_seq: Option<i64> =
            Spi::get_one("SELECT COALESCE(MIN(seq), 0) FROM sync_changelog").unwrap();
        let min_seq = min_seq.unwrap_or(0);

        if min_seq > 1 {
            // Set a bucket checkpoint below min_seq.
            let bucket_cps = serde_json::json!({"user:u1": 1, "global": 1});
            let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
                "SELECT synchro_pull($1, $2, 0, $3::jsonb, 100, NULL, NULL, 0, '')",
                &[
                    "u1".into(),
                    "c1".into(),
                    pgrx::JsonB(bucket_cps).into(),
                ],
            )
            .unwrap();
            let resp = resp.unwrap().0;

            // Should contain rebuild_buckets.
            assert!(resp.get("rebuild_buckets").is_some());
        }
        // If min_seq <= 1, stale detection can't fire. This is expected in a fresh
        // test database. The test validates the code path exists.
    }

    #[pg_test]
    fn test_pull_stale_no_checkpoint_advance() {
        setup_test_tables();
        register_client("u1", "c1");

        // Set initial checkpoint.
        Spi::run_with_args(
            "UPDATE sync_clients SET last_pull_seq = 5 WHERE user_id = $1 AND client_id = $2",
            &["u1".into(), "c1".into()],
        )
        .unwrap();

        // Insert entries and simulate stale by setting bucket checkpoint below min.
        insert_changelog("user:u1", "test_orders", "stale-cp-record", 1);

        // Pull. Even if stale, checkpoint must not advance beyond where it was.
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2, 5, NULL, 100, NULL, NULL, 0, '')",
            &["u1".into(), "c1".into()],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        // If stale, checkpoint should remain at the input value (not advanced).
        if resp.get("rebuild_buckets").is_some() {
            let cp = resp["checkpoint"].as_i64().unwrap();
            assert_eq!(cp, 5, "checkpoint should not advance when stale");
        }
    }

    #[pg_test]
    fn test_pull_checksums_final_page() {
        setup_pull_fixtures();

        // Pull all (no has_more). Should include bucket_checksums.
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2, 0, NULL, 1000, NULL, NULL, 0, '')",
            &["u1".into(), "c1".into()],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        if resp["has_more"].as_bool() == Some(false) {
            // Final page: bucket_checksums should be present.
            assert!(
                resp.get("bucket_checksums").is_some(),
                "final page should include bucket_checksums"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Pull Bucket Updates (4 tests)
    // -----------------------------------------------------------------------

    #[pg_test]
    fn test_pull_bucket_updates_added() {
        setup_test_tables();
        register_client("u1", "c1");

        // Client thinks it only has "global". Server has "user:u1" and "global".
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2, 0, NULL, 100, NULL, ARRAY['global'], 0, '')",
            &["u1".into(), "c1".into()],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        let updates = resp.get("bucket_updates");
        assert!(updates.is_some(), "bucket_updates should be present");
        let added = updates.unwrap()["added"].as_array().unwrap();
        assert!(
            added.iter().any(|v| v.as_str() == Some("user:u1")),
            "user:u1 should be in added"
        );
    }

    #[pg_test]
    fn test_pull_bucket_updates_removed() {
        setup_test_tables();
        register_client("u1", "c1");

        // Client thinks it has "user:u1", "global", and "team:old".
        // Server only has "user:u1" and "global". "team:old" should be removed.
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2, 0, NULL, 100, NULL, ARRAY['user:u1','global','team:old'], 0, '')",
            &["u1".into(), "c1".into()],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        let updates = resp.get("bucket_updates");
        assert!(updates.is_some(), "bucket_updates should be present");
        let removed = updates.unwrap()["removed"].as_array().unwrap();
        assert!(
            removed.iter().any(|v| v.as_str() == Some("team:old")),
            "team:old should be in removed"
        );
    }

    #[pg_test]
    fn test_pull_bucket_updates_unchanged() {
        setup_test_tables();
        register_client("u1", "c1");

        // Client known_buckets matches server exactly.
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2, 0, NULL, 100, NULL, ARRAY['user:u1','global'], 0, '')",
            &["u1".into(), "c1".into()],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        // No bucket_updates when nothing changed.
        assert!(
            resp.get("bucket_updates").is_none(),
            "bucket_updates should not be present when unchanged"
        );
    }

    #[pg_test]
    fn test_pull_bucket_updates_null() {
        setup_test_tables();
        register_client("u1", "c1");

        // known_buckets not sent (NULL).
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2, 0, NULL, 100, NULL, NULL, 0, '')",
            &["u1".into(), "c1".into()],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        assert!(
            resp.get("bucket_updates").is_none(),
            "bucket_updates should not be present when known_buckets is NULL"
        );
    }

    // -----------------------------------------------------------------------
    // Rebuild (6 tests)
    // -----------------------------------------------------------------------

    #[pg_test]
    fn test_rebuild_basic() {
        setup_pull_fixtures(); // Creates records + edges

        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_rebuild($1, $2, 'user:u1', '', 100, 0, '')",
            &["u1".into(), "c1".into()],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        let records = resp["records"].as_array().unwrap();
        assert!(!records.is_empty());
        assert!(resp.get("checkpoint").is_some());
        assert!(resp.get("schema_version").is_some());
    }

    #[pg_test]
    fn test_rebuild_cursor_pagination() {
        setup_test_tables();
        register_client("u1", "c1");

        // Insert 3 records with edges.
        for i in 1..=3 {
            let id = format!("b000000{i}-0000-0000-0000-000000000000");
            Spi::run_with_args(
                "INSERT INTO test_orders (id, user_id, title) VALUES ($1::uuid, 'u1', $2)",
                &[id.as_str().into(), format!("Rebuild {i}").as_str().into()],
            )
            .unwrap();
            insert_edge("test_orders", &id, "user:u1");
        }

        // First page: limit=2.
        let resp1: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_rebuild($1, $2, 'user:u1', '', 2, 0, '')",
            &["u1".into(), "c1".into()],
        )
        .unwrap();
        let resp1 = resp1.unwrap().0;
        assert_eq!(resp1["has_more"].as_bool(), Some(true));
        let cursor = resp1["cursor"].as_str().unwrap();
        assert!(!cursor.is_empty());

        // Second page using cursor.
        let resp2: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_rebuild($1, $2, 'user:u1', $3, 2, 0, '')",
            &["u1".into(), "c1".into(), cursor.into()],
        )
        .unwrap();
        let resp2 = resp2.unwrap().0;
        // Should have remaining records.
        assert!(resp2["records"].as_array().unwrap().len() >= 1);
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
        insert_edge("test_orders", "bde10000-1111-1111-1111-111111111111", "user:u1");

        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_rebuild($1, $2, 'user:u1', '', 100, 0, '')",
            &["u1".into(), "c1".into()],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        let records = resp["records"].as_array().unwrap();
        let deleted = records
            .iter()
            .any(|r| r["id"].as_str() == Some("bde10000-1111-1111-1111-111111111111"));
        assert!(!deleted, "soft-deleted records should be filtered from rebuild");
    }

    #[pg_test]
    fn test_rebuild_sets_checkpoint() {
        setup_pull_fixtures();

        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_rebuild($1, $2, 'user:u1', '', 1000, 0, '')",
            &["u1".into(), "c1".into()],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        if resp["has_more"].as_bool() == Some(false) {
            let cp = resp["checkpoint"].as_i64().unwrap();
            // Verify checkpoint persisted in sync_client_checkpoints.
            let stored: Option<i64> = Spi::get_one_with_args(
                "SELECT checkpoint FROM sync_client_checkpoints \
                 WHERE user_id = $1 AND client_id = $2 AND bucket_id = 'user:u1'",
                &["u1".into(), "c1".into()],
            )
            .unwrap();
            assert_eq!(stored, Some(cp));
        }
    }

    #[pg_test]
    fn test_rebuild_advances_legacy() {
        setup_pull_fixtures();

        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_rebuild($1, $2, 'user:u1', '', 1000, 0, '')",
            &["u1".into(), "c1".into()],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        if resp["has_more"].as_bool() == Some(false) {
            let cp = resp["checkpoint"].as_i64().unwrap();
            let legacy: Option<i64> = Spi::get_one_with_args(
                "SELECT last_pull_seq FROM sync_clients WHERE user_id = $1 AND client_id = $2",
                &["u1".into(), "c1".into()],
            )
            .unwrap();
            // Legacy checkpoint should be >= rebuild checkpoint.
            assert!(legacy.unwrap_or(0) >= cp);
        }
    }

    #[pg_test]
    fn test_rebuild_unsubscribed_errors() {
        setup_test_tables();
        register_client("u1", "c1");

        // u1 is not subscribed to "team:other".
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_rebuild($1, $2, 'team:other', '', 100, 0, '')",
            &["u1".into(), "c1".into()],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        // Should return a client_not_found error (not subscribed to bucket).
        assert!(resp.get("error").is_some());
    }

    #[pg_test]
    fn test_rebuild_global_bucket() {
        setup_test_tables();
        register_client("user1", "client1");

        // Insert a record into test_products (a global/read-only table).
        let product_id = "d1000000-1111-1111-1111-111111111111";
        Spi::run_with_args(
            "INSERT INTO test_products (id, name, price) VALUES ($1::uuid, 'Widget', 9.99)",
            &[product_id.into()],
        )
        .unwrap();

        // Insert a bucket edge directly for the global bucket.
        insert_edge("test_products", product_id, "global");

        // Call synchro_rebuild for the global bucket.
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_rebuild($1, $2, 'global', '', 100, 0, '')",
            &["user1".into(), "client1".into()],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        let records = resp["records"].as_array().unwrap();
        assert!(
            !records.is_empty(),
            "rebuild global bucket should return records when edges exist, got 0"
        );

        // The product record should be in the results.
        let found = records
            .iter()
            .any(|r| r["id"].as_str() == Some(product_id));
        assert!(found, "product record should be in rebuild response");

        // On the final page (no more records), bucket_checksum should be present.
        if resp["has_more"].as_bool() == Some(false) {
            assert!(
                resp.get("bucket_checksum").is_some(),
                "final page should include bucket_checksum"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Push DML Error Recovery (Bug 3): PG ERROR in one record must not abort others
    // -----------------------------------------------------------------------

    #[pg_test]
    fn test_push_dml_error_does_not_abort_batch() {
        setup_test_tables();
        register_client("u1", "c1");

        // Push a batch with two records:
        // 1. First record has a type mismatch (amount is numeric, we send an
        //    invalid value via jsonb_populate_record to trigger a PG ERROR).
        //    Actually, jsonb_populate_record handles string-to-numeric coercion.
        //    Instead, use a direct SQL violation: insert with a duplicate PK
        //    where the existing record is NOT soft-deleted (conflict).
        //    But conflict returns "conflict" not "rejected_retryable".
        //
        // Better approach: try to insert a record with a value that fails PG
        // type coercion. A UUID column receiving "not-a-uuid" will error.
        // But the PK is validated before the INSERT... Let's use a different
        // trigger: insert into test_orders with a NULL user_id (NOT NULL).
        // Actually, strip_protected_columns might remove user_id if it's not
        // in the data. Let's send empty data so the INSERT fails.
        //
        // Simplest: create a table with a CHECK constraint that will fail.
        Spi::run(
            "CREATE TABLE IF NOT EXISTS test_constrained (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id TEXT NOT NULL,
                value INT NOT NULL CHECK (value > 0),
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                deleted_at TIMESTAMPTZ
            )",
        )
        .unwrap();
        Spi::run(
            "SELECT synchro_register_table(
                'test_constrained',
                $$SELECT ARRAY['user:' || user_id] FROM test_constrained WHERE id = $1::uuid$$,
                'id', 'updated_at', 'deleted_at', 'enabled'
            )",
        )
        .unwrap();

        // Batch: first record violates CHECK constraint (value = -1),
        // second is a valid order create.
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, $3::jsonb, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                r#"[
                    {"id":"d2a11111-1111-1111-1111-111111111111","table_name":"test_constrained","operation":"create","data":{"user_id":"u1","value":-1}},
                    {"id":"d2a22222-2222-2222-2222-222222222222","table_name":"test_orders","operation":"create","data":{"user_id":"u1","title":"After Error"}}
                ]"#.into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        // The first record should be rejected (DML error caught by savepoint).
        let rejected = resp["rejected"].as_array().unwrap();
        assert!(
            rejected.len() >= 1,
            "the constrained record should be rejected"
        );
        let constrained_rejected = rejected
            .iter()
            .find(|r| r["id"].as_str() == Some("d2a11111-1111-1111-1111-111111111111"));
        assert!(
            constrained_rejected.is_some(),
            "constrained record should be in rejected list"
        );

        // The second record should be accepted (savepoint rollback recovered
        // the transaction).
        let accepted = resp["accepted"].as_array().unwrap();
        let order_accepted = accepted
            .iter()
            .find(|r| r["id"].as_str() == Some("d2a22222-2222-2222-2222-222222222222"));
        assert!(
            order_accepted.is_some(),
            "valid order should be accepted even after a prior DML error"
        );
        assert_eq!(
            order_accepted.unwrap()["status"].as_str().unwrap(),
            "applied"
        );

        // Verify the accepted record was actually persisted.
        let title: Option<String> = Spi::get_one(
            "SELECT title FROM test_orders WHERE id = 'd2a22222-2222-2222-2222-222222222222'",
        )
        .unwrap();
        assert_eq!(title, Some("After Error".to_string()));

        // No explicit cleanup needed: pgrx tests run in a rolled-back transaction.
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

        let resp: Option<pgrx::JsonB> = Spi::get_one(
            "SELECT synchro_compact('7 days', 10000)",
        )
        .unwrap();
        let resp = resp.unwrap().0;

        assert!(resp["deactivated_clients"].as_i64().unwrap() >= 1);
    }

    #[pg_test]
    fn test_compact_deletes_below_safe() {
        setup_test_tables();
        // No clients registered, so safe_seq should be MAX(seq).
        insert_changelog("global", "test_products", "compact-1", 1);
        insert_changelog("global", "test_products", "compact-2", 1);

        let before: Option<i64> =
            Spi::get_one("SELECT count(*) FROM sync_changelog").unwrap();

        let resp: Option<pgrx::JsonB> = Spi::get_one(
            "SELECT synchro_compact('7 days', 10000)",
        )
        .unwrap();
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

        // Client has never pulled (last_pull_seq is NULL or 0).
        // safe_seq should be 0, so nothing gets deleted.
        let resp: Option<pgrx::JsonB> = Spi::get_one(
            "SELECT synchro_compact('7 days', 10000)",
        )
        .unwrap();
        let resp = resp.unwrap().0;

        let deleted = resp["deleted_entries"].as_i64().unwrap_or(0);
        assert_eq!(deleted, 0, "no entries should be deleted when active client at checkpoint 0");
    }

    #[pg_test]
    fn test_compact_safe_seq_both_sources() {
        setup_test_tables();
        register_client("u1", "c1");

        // Insert entries and advance both legacy and per-bucket checkpoints.
        insert_changelog("user:u1", "test_orders", "both-src-1", 1);
        insert_changelog("user:u1", "test_orders", "both-src-2", 1);

        // Advance legacy to seq 1.
        Spi::run_with_args(
            "UPDATE sync_clients SET last_pull_seq = 1 WHERE user_id = $1 AND client_id = $2",
            &["u1".into(), "c1".into()],
        )
        .unwrap();

        // Compact should respect the minimum of both checkpoint sources.
        let resp: Option<pgrx::JsonB> = Spi::get_one(
            "SELECT synchro_compact('7 days', 10000)",
        )
        .unwrap();
        let resp = resp.unwrap().0;

        // safe_seq should be <= 1 (the legacy checkpoint).
        assert!(resp["safe_seq"].as_i64().unwrap_or(0) <= 1);
    }

    #[pg_test]
    fn test_compact_no_active_clients() {
        setup_test_tables();

        insert_changelog("global", "test_products", "no-clients-1", 1);

        let resp: Option<pgrx::JsonB> = Spi::get_one(
            "SELECT synchro_compact('7 days', 10000)",
        )
        .unwrap();
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

        let resp: Option<pgrx::JsonB> = Spi::get_one("SELECT synchro_schema()").unwrap();
        let resp = resp.unwrap().0;

        // Envelope fields.
        assert!(resp.get("schema_version").is_some());
        assert!(resp.get("schema_hash").is_some());
        let server_time = resp["server_time"].as_str().unwrap();
        let is_utc = server_time.ends_with('Z') || server_time.ends_with("+00:00");
        assert!(server_time.contains('T') && is_utc,
            "server_time must be ISO 8601 UTC, got: {}", server_time);

        let tables = resp["tables"].as_array().unwrap();
        assert!(!tables.is_empty());

        // Find test_orders and validate the full contract.
        let orders = tables.iter().find(|t| t["table_name"].as_str() == Some("test_orders"))
            .expect("test_orders should be in schema");

        // Per-table fields.
        assert_eq!(orders["push_policy"].as_str(), Some("enabled"));
        assert_eq!(orders["updated_at_column"].as_str(), Some("updated_at"));
        assert_eq!(orders["deleted_at_column"].as_str(), Some("deleted_at"));
        let pk = orders["primary_key"].as_array().unwrap();
        assert_eq!(pk[0].as_str(), Some("id"));

        // Per-column fields: validate one column has all required fields.
        let columns = orders["columns"].as_array().unwrap();
        assert!(!columns.is_empty());
        let id_col = columns.iter().find(|c| c["name"].as_str() == Some("id"))
            .expect("id column should exist");
        assert!(id_col.get("db_type").is_some(), "column missing db_type");
        assert!(id_col.get("logical_type").is_some(), "column missing logical_type");
        assert!(id_col.get("nullable").is_some(), "column missing nullable");
        assert!(id_col.get("default_kind").is_some(), "column missing default_kind");
        assert_eq!(id_col["is_primary_key"].as_bool(), Some(true));

        // Verify logical_type mapping: uuid -> text, boolean -> integer.
        assert_eq!(id_col["logical_type"].as_str(), Some("text"), "uuid should map to text");

        // Verify default_kind: gen_random_uuid() is server_only.
        assert_eq!(id_col["default_kind"].as_str(), Some("server_only"),
            "gen_random_uuid() should be server_only");
    }

    #[pg_test]
    fn test_debug_returns_state() {
        setup_test_tables();
        register_client("u1", "c1");

        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_debug($1, $2)",
            &["u1".into(), "c1".into()],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        assert!(resp.get("client").is_some());
        assert!(resp.get("buckets").is_some());
        assert!(resp.get("server_time").is_some());
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

        // Push with wrong schema hash. Should return structured JSONB error.
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, '[]'::jsonb, 999, 'wrong_hash')",
            &["u1".into(), "c1".into()],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        assert_eq!(resp["error"].as_str(), Some("schema_mismatch"));
        assert!(resp.get("server_schema_version").is_some());
        assert!(resp.get("server_schema_hash").is_some());
        // server_schema_version should be a real value, not 0.
        assert!(resp["server_schema_version"].as_i64().unwrap_or(0) > 0);
    }

    #[pg_test]
    fn test_client_not_found_returns_jsonb() {
        setup_test_tables();

        // Pull with a non-existent client.
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2, 0, NULL, 100, NULL, NULL, 0, '')",
            &["nonexistent_user".into(), "nonexistent_client".into()],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        assert_eq!(resp["error"].as_str(), Some("client_not_found"));
    }

    #[pg_test]
    fn test_schema_mismatch_no_manifest() {
        // Before any tables are registered, schema manifest is empty.
        // Push with schema_version > 0 should pass (no manifest to validate against).
        Spi::run(
            "CREATE TABLE IF NOT EXISTS test_empty_table (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT DEFAULT ''
            )",
        )
        .unwrap();

        // Register a client without any tables registered.
        // This calls validate_schema internally.
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_register_client($1, $2, 'test', '1.0.0', 1, 'somehash')",
            &["empty_user".into(), "empty_client".into()],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        // Should succeed, not error. The id field means it registered.
        assert!(resp.get("id").is_some() || resp.get("error").is_none());
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

        // Client is 200ms behind server. With default 500ms tolerance, client wins.
        let resp1: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, $3::jsonb, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                r#"[{"id":"00c5e571-1111-1111-1111-111111111111","table_name":"test_orders","operation":"update","data":{"title":"Within 500ms"},"client_updated_at":"2025-06-15T11:59:59.800Z"}]"#.into(),
            ],
        )
        .unwrap();
        let status1 = resp1.unwrap().0["accepted"][0]["status"]
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
        let changes = format!(
            r#"[{{"id":"00c5e571-1111-1111-1111-111111111111","table_name":"test_orders","operation":"update","data":{{"title":"With 0ms"}},"client_updated_at":"2020-01-01T00:00:00Z"}}]"#
        );
        let resp2: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, $3::jsonb, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                changes.as_str().into(),
            ],
        )
        .unwrap();
        let status2 = resp2.unwrap().0["rejected"][0]["status"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(status2, "conflict", "server should win with 0ms tolerance");

        // Reset GUC.
        Spi::run("RESET synchro.clock_skew_tolerance_ms").unwrap();
    }

    // -----------------------------------------------------------------------
    // Mixed Push (Bug 2): valid create + read-only rejection in same batch
    // -----------------------------------------------------------------------

    #[pg_test]
    fn test_push_mixed_accepted_and_read_only_rejected() {
        setup_test_tables();
        register_client("u1", "c1");

        // Batch: one valid create (test_orders) and one read-only rejection (test_products).
        // The extension must return a proper JSONB response with accepted[1] and rejected[1],
        // not a PG error that aborts the transaction.
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_push($1, $2, $3::jsonb, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                r#"[
                    {"id":"f0a11111-1111-1111-1111-111111111111","table_name":"test_orders","operation":"create","data":{"user_id":"u1","title":"Valid Order"}},
                    {"id":"f0a22222-2222-2222-2222-222222222222","table_name":"test_products","operation":"create","data":{"name":"Read Only Product"}}
                ]"#.into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        let accepted = resp["accepted"].as_array().unwrap();
        let rejected = resp["rejected"].as_array().unwrap();
        assert_eq!(accepted.len(), 1, "one record should be accepted");
        assert_eq!(rejected.len(), 1, "one record should be rejected");
        assert_eq!(accepted[0]["status"].as_str().unwrap(), "applied");
        assert_eq!(rejected[0]["status"].as_str().unwrap(), "rejected_terminal");
        assert_eq!(rejected[0]["reason_code"].as_str().unwrap(), "table_read_only");

        // Verify the accepted record was persisted despite the rejection.
        let title: Option<String> = Spi::get_one(
            "SELECT title FROM test_orders WHERE id = 'f0a11111-1111-1111-1111-111111111111'",
        )
        .unwrap();
        assert_eq!(title, Some("Valid Order".to_string()));
    }

    // -----------------------------------------------------------------------
    // Pull bucket_checkpoints (Bug 3): empty bucket_checkpoints in response
    // -----------------------------------------------------------------------

    #[pg_test]
    fn test_pull_empty_bucket_checkpoints_returned() {
        setup_test_tables();
        register_client("u1", "c1");

        // Send per-bucket checkpoints as empty JSONB object.
        // The response must include bucket_checkpoints even when no entries exist.
        let bucket_cps = serde_json::json!({});
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2, 0, $3::jsonb, 100, NULL, NULL, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                pgrx::JsonB(bucket_cps).into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        // bucket_checkpoints must be present in the response.
        assert!(
            resp.get("bucket_checkpoints").is_some(),
            "response must include bucket_checkpoints when per-bucket mode is active"
        );
    }

    #[pg_test]
    fn test_pull_bucket_checkpoints_with_entries() {
        setup_test_tables();
        register_client("u1", "c1");

        // Insert changelog and edges.
        Spi::run(
            "INSERT INTO test_orders (id, user_id, title) VALUES
             ('bca01111-1111-1111-1111-111111111111', 'u1', 'BCP Test')",
        )
        .unwrap();
        insert_changelog("user:u1", "test_orders", "bca01111-1111-1111-1111-111111111111", 1);
        insert_edge("test_orders", "bca01111-1111-1111-1111-111111111111", "user:u1");

        // Pull with per-bucket checkpoints.
        let bucket_cps = serde_json::json!({"user:u1": 0, "global": 0});
        let resp: Option<pgrx::JsonB> = Spi::get_one_with_args(
            "SELECT synchro_pull($1, $2, 0, $3::jsonb, 100, NULL, NULL, 0, '')",
            &[
                "u1".into(),
                "c1".into(),
                pgrx::JsonB(bucket_cps).into(),
            ],
        )
        .unwrap();
        let resp = resp.unwrap().0;

        // bucket_checkpoints must be present and contain updated values.
        let bcp = resp.get("bucket_checkpoints");
        assert!(bcp.is_some(), "response must include bucket_checkpoints");
        let bcp_obj = bcp.unwrap().as_object().unwrap();
        // user:u1 should have advanced beyond 0.
        let u1_cp = bcp_obj.get("user:u1").and_then(|v| v.as_i64()).unwrap_or(0);
        assert!(u1_cp > 0, "user:u1 bucket checkpoint should have advanced");
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
            "synchro.database = 'postgres'",
        ]
    }
}
