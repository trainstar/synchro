use pgrx::prelude::*;
use pgrx::spi::SpiClient;
use synchro_core::contract::{
    normalize_portable_type_name, ColumnSchema, IndexSchema, SchemaManifest, TableSchema,
};

/// Return the full schema contract for all registered tables.
///
/// Includes column metadata, types, nullability, defaults, and primary keys.
/// Used by client SDKs to build local database schemas.
#[pg_extern]
fn synchro_schema() -> pgrx::JsonB {
    Spi::connect(|client| {
        let (schema_version, schema_hash) = crate::pull::get_latest_schema(client);
        let tables = build_table_schemas(client);

        pgrx::JsonB(serde_json::json!({
            "schema_version": schema_version,
            "schema_hash": schema_hash,
            "server_time": server_time_str(client),
            "tables": tables,
        }))
    })
}

/// Return the canonical portable schema manifest for registered synced tables.
///
/// This is the vNext-oriented schema surface intended for adapter and client
/// contract work. The older `synchro_schema()` JSON remains for legacy
/// compatibility during the audit sequence.
#[pg_extern]
fn synchro_schema_manifest() -> pgrx::JsonB {
    Spi::connect(|client| {
        let (schema_version, schema_hash) = crate::pull::get_latest_schema(client);
        let manifest = build_schema_manifest(client);

        pgrx::JsonB(serde_json::json!({
            "schema_version": schema_version,
            "schema_hash": schema_hash,
            "server_time": server_time_str(client),
            "manifest": manifest,
        }))
    })
}

/// Return a simplified list of registered tables with sync metadata.
#[pg_extern]
fn synchro_tables() -> pgrx::JsonB {
    Spi::connect(|client| {
        let (schema_version, schema_hash) = crate::pull::get_latest_schema(client);

        let tup_table = match client.select(
            "SELECT table_name, push_policy, pk_column, updated_at_col, \
             deleted_at_col, sync_columns, exclude_columns, has_updated_at, has_deleted_at \
             FROM sync_registry ORDER BY table_name",
            None,
            &[],
        ) {
            Ok(t) => t,
            Err(e) => pgrx::error!("querying sync_registry: {}", e),
        };

        let mut tables: Vec<serde_json::Value> = Vec::new();
        for row in tup_table {
            let table_name: String = row
                .get_by_name::<String, &str>("table_name")
                .unwrap_or(None)
                .unwrap_or_default();
            let push_policy: String = row
                .get_by_name::<String, &str>("push_policy")
                .unwrap_or(None)
                .unwrap_or_default();
            let pk_column: String = row
                .get_by_name::<String, &str>("pk_column")
                .unwrap_or(None)
                .unwrap_or_default();
            let updated_at_col: String = row
                .get_by_name::<String, &str>("updated_at_col")
                .unwrap_or(None)
                .unwrap_or_default();
            let deleted_at_col: String = row
                .get_by_name::<String, &str>("deleted_at_col")
                .unwrap_or(None)
                .unwrap_or_default();
            let sync_columns: Vec<String> = row
                .get_by_name::<Vec<String>, &str>("sync_columns")
                .unwrap_or(None)
                .unwrap_or_default();
            let exclude_columns: Vec<String> = row
                .get_by_name::<Vec<String>, &str>("exclude_columns")
                .unwrap_or(None)
                .unwrap_or_default();
            let has_updated_at: bool = row
                .get_by_name::<bool, &str>("has_updated_at")
                .unwrap_or(None)
                .unwrap_or(false);
            let has_deleted_at: bool = row
                .get_by_name::<bool, &str>("has_deleted_at")
                .unwrap_or(None)
                .unwrap_or(false);

            tables.push(serde_json::json!({
                "table_name": table_name,
                "push_policy": push_policy,
                "dependencies": [],
                "pk_column": pk_column,
                "updated_at_column": optional_sync_column_name(has_updated_at, &updated_at_col),
                "deleted_at_column": optional_sync_column_name(has_deleted_at, &deleted_at_col),
                "sync_columns": sync_columns,
                "exclude_columns": exclude_columns,
            }));
        }

        pgrx::JsonB(serde_json::json!({
            "tables": tables,
            "server_time": server_time_str(client),
            "schema_version": schema_version,
            "schema_hash": schema_hash,
        }))
    })
}

/// Return debug state for a specific client.
///
/// Includes client info, bucket checkpoints, member counts, checksums,
/// and changelog statistics.
#[pg_extern]
fn synchro_debug(p_user_id: &str, p_client_id: &str) -> pgrx::JsonB {
    Spi::connect(|client| {
        // Load client info.
        let client_info = load_client_debug(client, p_user_id, p_client_id);

        // Load bucket details.
        let bucket_subs: Vec<String> = client_info
            .get("bucket_subs")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let buckets = load_bucket_details(client, p_user_id, p_client_id, &bucket_subs);

        // Changelog stats.
        let changelog_stats = load_changelog_stats(client);

        pgrx::JsonB(serde_json::json!({
            "client": client_info,
            "buckets": buckets,
            "changelog_stats": changelog_stats,
            "server_time": server_time_str(client),
        }))
    })
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn server_time_str(client: &SpiClient<'_>) -> String {
    let _ = client;
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn build_table_schemas(client: &SpiClient<'_>) -> Vec<serde_json::Value> {
    let mut tables: Vec<serde_json::Value> = Vec::new();
    let registrations = match crate::registry::load_registry_from_client(client) {
        Ok(registrations) => registrations,
        Err(_) => return vec![],
    };

    for registration in registrations {
        let columns = introspect_columns(
            client,
            &registration.table_name,
            &registration.pk_column,
            &registration.sync_columns,
        );
        let table_name = registration.table_name.clone();
        let pk_column = registration.pk_column.clone();
        let updated_at_column =
            optional_sync_column_name(registration.has_updated_at, &registration.updated_at_col);
        let deleted_at_column =
            optional_sync_column_name(registration.has_deleted_at, &registration.deleted_at_col);
        let push_policy = registration.push_policy.as_str();

        tables.push(serde_json::json!({
            "table_name": table_name,
            "push_policy": push_policy,
            "dependencies": [],
            "updated_at_column": updated_at_column,
            "deleted_at_column": deleted_at_column,
            "primary_key": [pk_column],
            "columns": columns,
        }));
    }
    tables
}

pub(crate) fn build_schema_manifest(client: &SpiClient<'_>) -> SchemaManifest {
    let mut tables = Vec::new();
    let registrations = match crate::registry::load_registry_from_client(client) {
        Ok(registrations) => registrations,
        Err(_) => return SchemaManifest { tables: vec![] },
    };

    for registration in registrations {
        tables.push(TableSchema {
            name: registration.table_name.clone(),
            primary_key: Some(vec![registration.pk_column.clone()]),
            updated_at_column: optional_sync_column_name(
                registration.has_updated_at,
                &registration.updated_at_col,
            ),
            deleted_at_column: optional_sync_column_name(
                registration.has_deleted_at,
                &registration.deleted_at_col,
            ),
            composition: None,
            columns: Some(introspect_manifest_columns(
                client,
                &registration.table_name,
                &registration.pk_column,
                &registration.sync_columns,
            )),
            indexes: Some(introspect_manifest_indexes(
                client,
                &registration.table_name,
                &registration.sync_columns,
            )),
        });
    }

    let manifest = SchemaManifest { tables };
    if let Err(err) = manifest.validate() {
        pgrx::error!("building portable schema manifest: {}", err);
    }
    manifest
}

fn introspect_columns(
    client: &SpiClient<'_>,
    table_name: &str,
    pk_column: &str,
    sync_columns: &[String],
) -> Vec<serde_json::Value> {
    let tup = match client.select(
        "SELECT jsonb_build_object(
             'name', a.attname,
             'db_type', pg_catalog.format_type(a.atttypid, a.atttypmod),
             'nullable', NOT a.attnotnull,
             'default_sql', COALESCE(pg_catalog.pg_get_expr(ad.adbin, ad.adrelid), ''),
             'default_kind', CASE
                 WHEN ad.adbin IS NULL THEN 'none'
                 ELSE 'server_expression'
             END,
             'is_primary_key', a.attname = $2
         ) AS column_json \
         FROM pg_catalog.pg_attribute a \
         JOIN pg_catalog.pg_class c ON c.oid = a.attrelid \
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum \
         WHERE c.relname = $1 AND n.nspname = ANY(current_schemas(false)) \
         AND a.attnum > 0 AND NOT a.attisdropped \
         ORDER BY a.attnum",
        None,
        &[table_name.into(), pk_column.into()],
    ) {
        Ok(t) => t,
        Err(_) => return vec![],
    };

    let sync_column_set: std::collections::HashSet<&str> =
        sync_columns.iter().map(|column| column.as_str()).collect();
    let mut columns: Vec<serde_json::Value> = Vec::new();
    for row in tup {
        if let Some(json) = row.get::<pgrx::JsonB>(1).unwrap_or(None) {
            let mut column = json.0;
            let is_synced = column
                .get("name")
                .and_then(|value| value.as_str())
                .map(|name| sync_column_set.contains(name))
                .unwrap_or(false);
            if !is_synced {
                continue;
            }
            if let Some(obj) = column.as_object_mut() {
                let db_type = obj
                    .get("db_type")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default();
                obj.insert(
                    "logical_type".into(),
                    serde_json::json!(legacy_logical_type(db_type)),
                );
            }
            columns.push(column);
        }
    }
    columns
}

fn introspect_manifest_columns(
    client: &SpiClient<'_>,
    table_name: &str,
    pk_column: &str,
    sync_columns: &[String],
) -> Vec<ColumnSchema> {
    introspect_columns(client, table_name, pk_column, sync_columns)
        .into_iter()
        .filter_map(|column| {
            let name = column.get("name").and_then(|value| value.as_str())?;
            let db_type = column.get("db_type").and_then(|value| value.as_str())?;
            let nullable = column.get("nullable").and_then(|value| value.as_bool())?;

            Some(ColumnSchema {
                name: name.to_string(),
                type_name: portable_type_name(db_type),
                nullable,
            })
        })
        .collect()
}

fn introspect_manifest_indexes(
    client: &SpiClient<'_>,
    table_name: &str,
    sync_columns: &[String],
) -> Vec<IndexSchema> {
    let tup = match client.select(
        "SELECT idx.indexname::text AS name, \
         array_agg(att.attname::text ORDER BY key_pos.ordinality) AS columns \
         FROM pg_indexes idx \
         JOIN pg_class c ON c.relname = idx.tablename \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         JOIN pg_index pi ON pi.indexrelid = to_regclass(idx.indexname) \
         JOIN unnest(pi.indkey) WITH ORDINALITY AS key_pos(attnum, ordinality) ON true \
         JOIN pg_attribute att ON att.attrelid = c.oid AND att.attnum = key_pos.attnum \
         WHERE idx.tablename = $1 \
           AND n.nspname = ANY(current_schemas(false)) \
           AND NOT pi.indisprimary \
           AND att.attnum > 0 \
         GROUP BY idx.indexname \
         ORDER BY idx.indexname",
        None,
        &[table_name.into()],
    ) {
        Ok(t) => t,
        Err(_) => return vec![],
    };

    let sync_column_set: std::collections::HashSet<&str> =
        sync_columns.iter().map(|column| column.as_str()).collect();
    let mut indexes = Vec::new();
    for row in tup {
        let name: String = row
            .get_by_name::<String, &str>("name")
            .unwrap_or(None)
            .unwrap_or_default();
        let columns: Vec<String> = row
            .get_by_name::<Vec<String>, &str>("columns")
            .unwrap_or(None)
            .unwrap_or_default();
        if columns
            .iter()
            .any(|column| !sync_column_set.contains(column.as_str()))
        {
            continue;
        }
        if !name.is_empty() && !columns.is_empty() {
            indexes.push(IndexSchema { name, columns });
        }
    }

    indexes
}

fn optional_sync_column_name(enabled: bool, column_name: &str) -> Option<String> {
    if !enabled {
        return None;
    }

    let trimmed = column_name.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn portable_type_name(db_type: &str) -> String {
    let normalized = db_type.trim().to_ascii_lowercase();

    if let Some(type_name) = normalize_portable_type_name(normalized.as_str()) {
        return type_name.to_string();
    }

    if normalized.starts_with("numeric(") || normalized.starts_with("decimal(") {
        return "float".into();
    }
    if normalized.starts_with("character varying")
        || normalized.starts_with("varchar(")
        || normalized.starts_with("character(")
    {
        return "string".into();
    }

    db_type.to_string()
}

fn legacy_logical_type(db_type: &str) -> &'static str {
    let normalized = db_type.to_ascii_lowercase();
    if normalized.contains("timestamp") {
        return "datetime";
    }
    if normalized == "date" {
        return "date";
    }
    if normalized == "boolean" {
        return "boolean";
    }
    if normalized == "uuid"
        || normalized == "text"
        || normalized.starts_with("character varying")
        || normalized.starts_with("character(")
        || normalized.starts_with("character ")
        || normalized == "character"
        || normalized == "interval"
        || normalized == "inet"
        || normalized == "cidr"
        || normalized == "macaddr"
        || normalized == "macaddr8"
        || normalized == "xml"
        || normalized == "point"
        || normalized == "line"
        || normalized == "lseg"
        || normalized == "box"
        || normalized == "path"
        || normalized == "polygon"
        || normalized == "circle"
        || normalized.ends_with("range")
    {
        return "string";
    }
    if normalized == "smallint" || normalized == "integer" {
        return "int";
    }
    if normalized == "bigint" {
        return "int64";
    }
    if normalized.starts_with("numeric") || normalized == "real" || normalized == "double precision"
    {
        return "float";
    }
    if normalized == "json" || normalized == "jsonb" || normalized.ends_with("[]") {
        return "json";
    }
    if normalized == "bytea" {
        return "bytes";
    }
    "string"
}

fn load_client_debug(client: &SpiClient<'_>, user_id: &str, client_id: &str) -> serde_json::Value {
    let tup = match client.select(
        "SELECT id::text, client_id, user_id, platform, app_version, is_active, \
         last_sync_at::text, last_pull_at::text, last_pull_seq, bucket_subs \
         FROM sync_clients WHERE user_id = $1 AND client_id = $2",
        None,
        &[user_id.into(), client_id.into()],
    ) {
        Ok(t) => t,
        Err(e) => pgrx::error!("querying client: {}", e),
    };

    if let Some(row) = tup.into_iter().next() {
        let id: String = row
            .get_by_name::<String, &str>("id")
            .unwrap_or(None)
            .unwrap_or_default();
        let cid: String = row
            .get_by_name::<String, &str>("client_id")
            .unwrap_or(None)
            .unwrap_or_default();
        let platform: String = row
            .get_by_name::<String, &str>("platform")
            .unwrap_or(None)
            .unwrap_or_default();
        let app_version: String = row
            .get_by_name::<String, &str>("app_version")
            .unwrap_or(None)
            .unwrap_or_default();
        let is_active: bool = row
            .get_by_name::<bool, &str>("is_active")
            .unwrap_or(None)
            .unwrap_or(false);
        let last_sync_at: Option<String> = row
            .get_by_name::<String, &str>("last_sync_at")
            .unwrap_or(None);
        let last_pull_at: Option<String> = row
            .get_by_name::<String, &str>("last_pull_at")
            .unwrap_or(None);
        let last_pull_seq: i64 = row
            .get_by_name::<i64, &str>("last_pull_seq")
            .unwrap_or(None)
            .unwrap_or(0);
        let bucket_subs: Vec<String> = row
            .get_by_name::<Vec<String>, &str>("bucket_subs")
            .unwrap_or(None)
            .unwrap_or_default();

        serde_json::json!({
            "id": id,
            "client_id": cid,
            "platform": platform,
            "app_version": app_version,
            "is_active": is_active,
            "last_sync_at": last_sync_at,
            "last_pull_at": last_pull_at,
            "legacy_checkpoint": last_pull_seq,
            "bucket_subs": bucket_subs,
        })
    } else {
        pgrx::error!("client not found: {}/{}", user_id, client_id);
    }
}

fn load_bucket_details(
    client: &SpiClient<'_>,
    user_id: &str,
    client_id: &str,
    bucket_subs: &[String],
) -> Vec<serde_json::Value> {
    // Load checkpoints.
    let cp_tup = match client.select(
        "SELECT bucket_id, checkpoint FROM sync_client_checkpoints \
         WHERE user_id = $1 AND client_id = $2",
        None,
        &[user_id.into(), client_id.into()],
    ) {
        Ok(t) => t,
        Err(_) => return vec![],
    };

    let mut checkpoints: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for row in cp_tup {
        let bid: String = row
            .get_by_name::<String, &str>("bucket_id")
            .unwrap_or(None)
            .unwrap_or_default();
        let cp: i64 = row
            .get_by_name::<i64, &str>("checkpoint")
            .unwrap_or(None)
            .unwrap_or(0);
        checkpoints.insert(bid, cp);
    }

    let mut buckets: Vec<serde_json::Value> = Vec::new();
    for bid in bucket_subs {
        // Member count.
        let member_count: i64 = match client.select(
            "SELECT COUNT(*) AS cnt FROM sync_bucket_edges WHERE bucket_id = $1",
            None,
            &[bid.as_str().into()],
        ) {
            Ok(tup) => tup.first().get_one::<i64>().ok().flatten().unwrap_or(0),
            Err(_) => 0,
        };

        // Checksum.
        let checksum: Option<i32> = match client.select(
            "SELECT BIT_XOR(checksum) AS cs FROM sync_bucket_edges \
             WHERE bucket_id = $1 AND checksum IS NOT NULL",
            None,
            &[bid.as_str().into()],
        ) {
            Ok(tup) => tup.first().get_one::<i32>().ok().flatten(),
            Err(_) => None,
        };

        buckets.push(serde_json::json!({
            "bucket_id": bid,
            "checkpoint": checkpoints.get(bid).copied().unwrap_or(0),
            "member_count": member_count,
            "checksum": checksum,
        }));
    }
    buckets
}

fn load_changelog_stats(_client: &SpiClient<'_>) -> serde_json::Value {
    // Single query for all three stats (consistent snapshot).
    match Spi::get_three_with_args::<i64, i64, i64>(
        "SELECT COALESCE(MIN(seq), 0), COALESCE(MAX(seq), 0), COUNT(*) FROM sync_changelog",
        &[],
    ) {
        Ok((min_seq, max_seq, total)) => serde_json::json!({
            "min_seq": min_seq.unwrap_or(0),
            "max_seq": max_seq.unwrap_or(0),
            "total_entries": total.unwrap_or(0),
        }),
        Err(_) => serde_json::json!({
            "min_seq": 0,
            "max_seq": 0,
            "total_entries": 0,
        }),
    }
}
