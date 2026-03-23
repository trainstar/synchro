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
    pub pk_type: String,
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
    let policy = match PushPolicy::parse(p_push_policy) {
        Some(p) => p,
        None => {
            pgrx::error!(
                "invalid push_policy: {:?}, expected 'enabled' or 'read_only'",
                p_push_policy
            );
        }
    };

    // Validate that the target table exists.
    let table_exists: bool = Spi::get_one_with_args(
        "SELECT EXISTS (
            SELECT 1 FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = $1
              AND n.nspname = ANY(current_schemas(false))
              AND c.relkind IN ('r', 'p')
        )",
        &[p_table_name.into()],
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    if !table_exists {
        pgrx::error!("table {:?} does not exist", p_table_name);
    }

    // Validate bucket_sql by executing it with a dummy UUID. The query must
    // accept a TEXT parameter ($1) and be valid SQL. We execute it via SPI
    // select (not a DO block, which cannot accept SPI parameters). An empty
    // result set is expected (no row matches the dummy UUID).
    if let Err(e) = Spi::connect(|client| {
        client.select(
            p_bucket_sql,
            None,
            &["00000000-0000-0000-0000-000000000000".into()],
        )?;
        Ok::<_, pgrx::spi::Error>(())
    }) {
        pgrx::error!("bucket_sql validation failed: {}", e);
    }

    // Introspect PK column type for cast generation in push/pull queries.
    let pk_type: String = Spi::get_one_with_args(
        "SELECT format_type(a.atttypid, a.atttypmod) FROM pg_catalog.pg_attribute a \
         JOIN pg_catalog.pg_class c ON c.oid = a.attrelid \
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relname = $1 AND a.attname = $2 \
         AND n.nspname = ANY(current_schemas(false)) AND NOT a.attisdropped",
        &[p_table_name.into(), p_pk_column.into()],
    )
    .unwrap_or(Some("text".to_string()))
    .unwrap_or_else(|| "text".to_string());

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
        &[p_table_name.into(), p_updated_at_col.into()],
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
        &[p_table_name.into(), p_deleted_at_col.into()],
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    // Upsert into sync_registry.
    let exclude_arr = p_exclude_columns;
    if let Err(e) = Spi::run_with_args(
        "INSERT INTO sync_registry (
            table_name, bucket_sql, pk_column, pk_type, updated_at_col, deleted_at_col,
            push_policy, exclude_columns, has_updated_at, has_deleted_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (table_name) DO UPDATE SET
            bucket_sql = EXCLUDED.bucket_sql,
            pk_column = EXCLUDED.pk_column,
            pk_type = EXCLUDED.pk_type,
            updated_at_col = EXCLUDED.updated_at_col,
            deleted_at_col = EXCLUDED.deleted_at_col,
            push_policy = EXCLUDED.push_policy,
            exclude_columns = EXCLUDED.exclude_columns,
            has_updated_at = EXCLUDED.has_updated_at,
            has_deleted_at = EXCLUDED.has_deleted_at,
            updated_at = now()",
        &[
            p_table_name.into(),
            p_bucket_sql.into(),
            p_pk_column.into(),
            pk_type.as_str().into(),
            p_updated_at_col.into(),
            p_deleted_at_col.into(),
            policy.as_str().into(),
            exclude_arr.into(),
            has_updated_at.into(),
            has_deleted_at.into(),
        ],
    ) {
        pgrx::error!("failed to upsert sync_registry: {}", e);
    }

    // Add table to WAL publication using PG's format() for safe identifier quoting.
    let pub_name = crate::PUBLICATION_NAME_GUC
        .get()
        .and_then(|cs| cs.to_str().ok().map(String::from))
        .unwrap_or_else(|| "synchro_pub".to_string());
    let pub_exists: bool = Spi::get_one_with_args(
        "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)",
        &[pub_name.as_str().into()],
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    if !pub_exists {
        // Build DDL safely using format(%I) for identifier quoting.
        let ddl: Option<String> = Spi::get_one_with_args(
            "SELECT format('CREATE PUBLICATION %I FOR TABLE %I', $1, $2)",
            &[pub_name.as_str().into(), p_table_name.into()],
        )
        .unwrap_or(None);

        if let Some(sql) = ddl {
            if let Err(e) = Spi::run(&sql) {
                pgrx::error!("failed to create publication: {}", e);
            }
        }
    } else {
        let in_pub: bool = Spi::get_one_with_args(
            "SELECT EXISTS (
                SELECT 1 FROM pg_publication_tables
                WHERE pubname = $1 AND tablename = $2
            )",
            &[pub_name.as_str().into(), p_table_name.into()],
        )
        .unwrap_or(Some(false))
        .unwrap_or(false);

        if !in_pub {
            let ddl: Option<String> = Spi::get_one_with_args(
                "SELECT format('ALTER PUBLICATION %I ADD TABLE %I', $1, $2)",
                &[pub_name.as_str().into(), p_table_name.into()],
            )
            .unwrap_or(None);

            if let Some(sql) = ddl {
                if let Err(e) = Spi::run(&sql) {
                    pgrx::error!("failed to add table to publication: {}", e);
                }
            }
        }
    }

    // Recompute schema manifest.
    recompute_schema_manifest();

    // Notify background worker of registry change.
    let _ = Spi::run("NOTIFY synchro_registry_changed");
}

/// Unregister a table from synchronization.
///
/// Removes the registry entry, cleans up bucket edges, and removes
/// the table from the WAL publication.
#[pg_extern]
fn synchro_unregister_table(p_table_name: &str) {
    // Delete registry entry.
    let _ = Spi::run_with_args(
        "DELETE FROM sync_registry WHERE table_name = $1",
        &[p_table_name.into()],
    );

    // Clean up bucket edges for this table.
    let _ = Spi::run_with_args(
        "DELETE FROM sync_bucket_edges WHERE table_name = $1",
        &[p_table_name.into()],
    );

    // Remove from publication.
    let pub_name = crate::PUBLICATION_NAME_GUC
        .get()
        .and_then(|cs| cs.to_str().ok().map(String::from))
        .unwrap_or_else(|| "synchro_pub".to_string());
    let in_pub: bool = Spi::get_one_with_args(
        "SELECT EXISTS (
            SELECT 1 FROM pg_publication_tables
            WHERE pubname = $1 AND tablename = $2
        )",
        &[pub_name.as_str().into(), p_table_name.into()],
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    if in_pub {
        let ddl: Option<String> = Spi::get_one_with_args(
            "SELECT format('ALTER PUBLICATION %I DROP TABLE %I', $1, $2)",
            &[pub_name.as_str().into(), p_table_name.into()],
        )
        .unwrap_or(None);

        if let Some(sql) = ddl {
            let _ = Spi::run(&sql);
        }
    }

    recompute_schema_manifest();
    let _ = Spi::run("NOTIFY synchro_registry_changed");
}

/// Recompute the schema manifest hash from all registered tables.
///
/// Uses ON CONFLICT on schema_hash to avoid TOCTOU races on schema_version.
fn recompute_schema_manifest() {
    let _ = Spi::connect_mut(|client| {
        let manifest = crate::schema::build_schema_manifest(client);
        if let Err(err) = manifest.validate() {
            pgrx::error!("failed to validate canonical schema manifest: {}", err);
        }

        let manifest_json = match serde_json::to_string(&manifest) {
            Ok(json) => json,
            Err(err) => pgrx::error!("failed to serialize canonical schema manifest: {}", err),
        };

        let schema_hash: String =
            Spi::get_one_with_args("SELECT md5($1)", &[manifest_json.as_str().into()])
                .unwrap_or(None)
                .unwrap_or_default();

        let _ = client.update(
            "INSERT INTO sync_schema_manifest (schema_version, schema_hash)
             SELECT
                 COALESCE((SELECT MAX(schema_version) FROM sync_schema_manifest), 0) + 1,
                 $1
             WHERE NOT EXISTS (
                 SELECT 1 FROM sync_schema_manifest sm
                 WHERE sm.schema_hash = $1
             )",
            None,
            &[schema_hash.as_str().into()],
        )?;

        Ok::<(), spi::Error>(())
    });
}

/// Load all registered tables from sync_registry into memory.
/// Used by the background worker on startup and after NOTIFY.
pub fn load_registry() -> Result<Vec<TableRegistration>, spi::Error> {
    let mut tables = Vec::new();

    Spi::connect(|client| {
        let query = "SELECT table_name, bucket_sql, pk_column, pk_type, updated_at_col,
                            deleted_at_col, push_policy, exclude_columns,
                            has_updated_at, has_deleted_at
                     FROM sync_registry
                     ORDER BY table_name";
        let tup_table = client.select(query, None, &[])?;

        for row in tup_table {
            let table_name: String = row.get_by_name("table_name")?.unwrap_or_default();
            let bucket_sql: String = row.get_by_name("bucket_sql")?.unwrap_or_default();
            let pk_column: String = row.get_by_name("pk_column")?.unwrap_or_default();
            let pk_type: String = row
                .get_by_name("pk_type")?
                .unwrap_or_else(|| "text".to_string());
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
                pk_type,
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
