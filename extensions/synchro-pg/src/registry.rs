use pgrx::prelude::*;
use serde::{Deserialize, Serialize};

/// Push policy for a registered table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PushPolicy {
    Enabled,
    ReadOnly,
}

impl PushPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::ReadOnly => "read_only",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "enabled" => Some(Self::Enabled),
            "read_only" => Some(Self::ReadOnly),
            _ => None,
        }
    }
}

/// In-memory representation of a registered table (cached by bgworker).
#[derive(Debug, Clone)]
pub struct TableRegistration {
    pub table_name: String,
    pub bucket_sql: String,
    pub pk_column: String,
    pub updated_at_col: String,
    pub deleted_at_col: String,
    pub push_policy: PushPolicy,
    pub exclude_columns: Vec<String>,
    pub has_updated_at: bool,
    pub has_deleted_at: bool,
}

/// Register a table for synchronization.
///
/// Upserts into `sync_registry`, introspects the table via `pg_catalog`,
/// validates the bucket SQL, and adds the table to the WAL publication.
#[pg_extern]
fn synchro_register_table(
    p_table_name: &str,
    p_bucket_sql: &str,
    p_pk_column: default!(&str, "'id'"),
    p_updated_at_col: default!(&str, "'updated_at'"),
    p_deleted_at_col: default!(&str, "'deleted_at'"),
    p_push_policy: default!(&str, "'enabled'"),
    p_exclude_columns: default!(Vec<String>, "'{}'"),
) {
    // Validate push_policy.
    if PushPolicy::parse(p_push_policy).is_none() {
        pgrx::error!(
            "invalid push_policy: {:?}, expected 'enabled' or 'read_only'",
            p_push_policy
        );
    }
    let policy = PushPolicy::parse(p_push_policy).unwrap();

    // Validate that the target table exists.
    let table_exists: bool = Spi::get_one_with_args(
        "SELECT EXISTS (
            SELECT 1 FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = $1
              AND n.nspname = ANY(current_schemas(false))
              AND c.relkind IN ('r', 'p')
        )",
        vec![(PgBuiltInOids::TEXTOID.oid(), p_table_name.into_datum())],
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    if !table_exists {
        pgrx::error!("table {:?} does not exist", p_table_name);
    }

    // Validate bucket_sql by executing with a dummy UUID.
    if let Err(e) = Spi::run(&format!(
        "DO $$ DECLARE _result TEXT[];
        BEGIN
            EXECUTE $sql${bucket_sql}$sql$ USING '00000000-0000-0000-0000-000000000000'::text INTO _result;
        END $$",
        bucket_sql = p_bucket_sql
    )) {
        pgrx::error!("bucket_sql validation failed: {}", e);
    }

    // Introspect timestamp columns.
    let has_updated_at: bool = Spi::get_one_with_args(
        "SELECT EXISTS (
            SELECT 1 FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = $1
              AND n.nspname = ANY(current_schemas(false))
              AND a.attname = $2
              AND NOT a.attisdropped
        )",
        vec![
            (PgBuiltInOids::TEXTOID.oid(), p_table_name.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), p_updated_at_col.into_datum()),
        ],
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    let has_deleted_at: bool = Spi::get_one_with_args(
        "SELECT EXISTS (
            SELECT 1 FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = $1
              AND n.nspname = ANY(current_schemas(false))
              AND a.attname = $2
              AND NOT a.attisdropped
        )",
        vec![
            (PgBuiltInOids::TEXTOID.oid(), p_table_name.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), p_deleted_at_col.into_datum()),
        ],
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    // Upsert into sync_registry.
    let exclude_arr = p_exclude_columns;
    if let Err(e) = Spi::run_with_args(
        "INSERT INTO sync_registry (
            table_name, bucket_sql, pk_column, updated_at_col, deleted_at_col,
            push_policy, exclude_columns, has_updated_at, has_deleted_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (table_name) DO UPDATE SET
            bucket_sql = EXCLUDED.bucket_sql,
            pk_column = EXCLUDED.pk_column,
            updated_at_col = EXCLUDED.updated_at_col,
            deleted_at_col = EXCLUDED.deleted_at_col,
            push_policy = EXCLUDED.push_policy,
            exclude_columns = EXCLUDED.exclude_columns,
            has_updated_at = EXCLUDED.has_updated_at,
            has_deleted_at = EXCLUDED.has_deleted_at,
            updated_at = now()",
        Some(vec![
            (PgBuiltInOids::TEXTOID.oid(), p_table_name.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), p_bucket_sql.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), p_pk_column.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), p_updated_at_col.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), p_deleted_at_col.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), policy.as_str().into_datum()),
            (PgBuiltInOids::TEXTARRAYOID.oid(), exclude_arr.into_datum()),
            (PgBuiltInOids::BOOLOID.oid(), has_updated_at.into_datum()),
            (PgBuiltInOids::BOOLOID.oid(), has_deleted_at.into_datum()),
        ]),
    ) {
        pgrx::error!("failed to upsert sync_registry: {}", e);
    }

    // Add table to WAL publication.
    let pub_name = "synchro_pub";
    let pub_exists: bool = Spi::get_one_with_args(
        "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)",
        vec![(PgBuiltInOids::TEXTOID.oid(), pub_name.into_datum())],
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    if !pub_exists {
        let _ = Spi::run(&format!(
            "CREATE PUBLICATION {pub_name} FOR TABLE {table_ident}",
            table_ident = quote_identifier(p_table_name),
        ));
    } else {
        let in_pub: bool = Spi::get_one_with_args(
            "SELECT EXISTS (
                SELECT 1 FROM pg_publication_tables
                WHERE pubname = $1 AND tablename = $2
            )",
            vec![
                (PgBuiltInOids::TEXTOID.oid(), pub_name.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), p_table_name.into_datum()),
            ],
        )
        .unwrap_or(Some(false))
        .unwrap_or(false);

        if !in_pub {
            let _ = Spi::run(&format!(
                "ALTER PUBLICATION {pub_name} ADD TABLE {table_ident}",
                table_ident = quote_identifier(p_table_name),
            ));
        }
    }

    // Recompute schema manifest.
    recompute_schema_manifest();

    // Notify background worker of registry change.
    let _ = Spi::run("NOTIFY synchro_registry_changed");
}

/// Unregister a table from synchronization.
#[pg_extern]
fn synchro_unregister_table(p_table_name: &str) {
    let _ = Spi::run_with_args(
        "DELETE FROM sync_registry WHERE table_name = $1",
        Some(vec![(PgBuiltInOids::TEXTOID.oid(), p_table_name.into_datum())]),
    );

    // Remove from publication.
    let pub_name = "synchro_pub";
    let in_pub: bool = Spi::get_one_with_args(
        "SELECT EXISTS (
            SELECT 1 FROM pg_publication_tables
            WHERE pubname = $1 AND tablename = $2
        )",
        vec![
            (PgBuiltInOids::TEXTOID.oid(), pub_name.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), p_table_name.into_datum()),
        ],
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    if in_pub {
        let _ = Spi::run(&format!(
            "ALTER PUBLICATION {pub_name} DROP TABLE {table_ident}",
            table_ident = quote_identifier(p_table_name),
        ));
    }

    recompute_schema_manifest();
    let _ = Spi::run("NOTIFY synchro_registry_changed");
}

/// Recompute the schema manifest hash from all registered tables.
fn recompute_schema_manifest() {
    let _ = Spi::run(
        "WITH registry_hash AS (
            SELECT md5(string_agg(
                table_name || ':' || bucket_sql || ':' || pk_column || ':' ||
                updated_at_col || ':' || deleted_at_col || ':' || push_policy || ':' ||
                array_to_string(exclude_columns, ','),
                '|' ORDER BY table_name
            )) AS hash
            FROM sync_registry
        )
        INSERT INTO sync_schema_manifest (schema_version, schema_hash)
        SELECT
            COALESCE((SELECT MAX(schema_version) FROM sync_schema_manifest), 0) + 1,
            COALESCE(rh.hash, '')
        FROM registry_hash rh
        WHERE NOT EXISTS (
            SELECT 1 FROM sync_schema_manifest sm
            WHERE sm.schema_hash = COALESCE(rh.hash, '')
        )",
    );
}

/// Double-quote a SQL identifier, escaping internal double quotes.
fn quote_identifier(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

/// Load all registered tables from sync_registry into memory.
/// Used by the background worker on startup and after NOTIFY.
pub fn load_registry() -> Result<Vec<TableRegistration>, spi::Error> {
    let mut tables = Vec::new();

    Spi::connect(|client| {
        let query = "SELECT table_name, bucket_sql, pk_column, updated_at_col,
                            deleted_at_col, push_policy, exclude_columns,
                            has_updated_at, has_deleted_at
                     FROM sync_registry
                     ORDER BY table_name";
        let tup_table = client.select(query, None, None)?;

        for row in tup_table {
            let table_name: String = row.get_by_name("table_name")?.unwrap_or_default();
            let bucket_sql: String = row.get_by_name("bucket_sql")?.unwrap_or_default();
            let pk_column: String = row.get_by_name("pk_column")?.unwrap_or_default();
            let updated_at_col: String = row.get_by_name("updated_at_col")?.unwrap_or_default();
            let deleted_at_col: String = row.get_by_name("deleted_at_col")?.unwrap_or_default();
            let push_policy_str: String = row.get_by_name("push_policy")?.unwrap_or_default();
            let exclude_columns: Vec<String> =
                row.get_by_name("exclude_columns")?.unwrap_or_default();
            let has_updated_at: bool = row.get_by_name("has_updated_at")?.unwrap_or(false);
            let has_deleted_at: bool = row.get_by_name("has_deleted_at")?.unwrap_or(false);

            tables.push(TableRegistration {
                table_name,
                bucket_sql,
                pk_column,
                updated_at_col,
                deleted_at_col,
                push_policy: PushPolicy::parse(&push_policy_str).unwrap_or(PushPolicy::Enabled),
                exclude_columns,
                has_updated_at,
                has_deleted_at,
            });
        }

        Ok(tables)
    })
}
