use pgrx::prelude::*;
use pgrx::spi::SpiClient;

/// Return the full schema contract for all registered tables.
///
/// Includes column metadata, types, nullability, defaults, and primary keys.
/// Used by client SDKs to build local database schemas.
#[pg_extern]
fn synchro_schema() -> pgrx::JsonB {
    Spi::connect(|client| {
        let (schema_version, schema_hash) = crate::pull::get_latest_schema(&client);
        let tables = build_table_schemas(&client);

        pgrx::JsonB(serde_json::json!({
            "schema_version": schema_version,
            "schema_hash": schema_hash,
            "server_time": server_time_str(&client),
            "tables": tables,
        }))
    })
}

/// Return a simplified list of registered tables with sync metadata.
#[pg_extern]
fn synchro_tables() -> pgrx::JsonB {
    Spi::connect(|client| {
        let (schema_version, schema_hash) = crate::pull::get_latest_schema(&client);

        let tup_table = match client.select(
            "SELECT table_name, push_policy, pk_column, updated_at_col, \
             deleted_at_col, exclude_columns \
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
            let exclude_columns: Vec<String> = row
                .get_by_name::<Vec<String>, &str>("exclude_columns")
                .unwrap_or(None)
                .unwrap_or_default();

            tables.push(serde_json::json!({
                "table_name": table_name,
                "push_policy": push_policy,
                "pk_column": pk_column,
                "updated_at_column": updated_at_col,
                "deleted_at_column": deleted_at_col,
                "exclude_columns": exclude_columns,
            }));
        }

        pgrx::JsonB(serde_json::json!({
            "tables": tables,
            "server_time": server_time_str(&client),
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
        let client_info = load_client_debug(&client, p_user_id, p_client_id);

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

        let buckets = load_bucket_details(&client, p_user_id, p_client_id, &bucket_subs);

        // Changelog stats.
        let changelog_stats = load_changelog_stats(&client);

        pgrx::JsonB(serde_json::json!({
            "client": client_info,
            "buckets": buckets,
            "changelog_stats": changelog_stats,
            "server_time": server_time_str(&client),
        }))
    })
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn server_time_str(client: &SpiClient<'_>) -> String {
    let tup = match client.select("SELECT now()::text AS t", None, &[]) {
        Ok(t) => t,
        Err(_) => return String::new(),
    };
    tup.first()
        .get_one::<String>()
        .ok()
        .flatten()
        .unwrap_or_default()
}

fn build_table_schemas(client: &SpiClient<'_>) -> Vec<serde_json::Value> {
    let reg_tup = match client.select(
        "SELECT table_name, push_policy, pk_column, updated_at_col, \
         deleted_at_col, exclude_columns \
         FROM sync_registry ORDER BY table_name",
        None,
        &[],
    ) {
        Ok(t) => t,
        Err(_) => return vec![],
    };

    let mut table_names: Vec<(String, String, String, String, String, Vec<String>)> = Vec::new();
    for row in reg_tup {
        let tn: String = row.get_by_name("table_name").unwrap_or(None).unwrap_or_default();
        let pp: String = row.get_by_name("push_policy").unwrap_or(None).unwrap_or_default();
        let pk: String = row.get_by_name("pk_column").unwrap_or(None).unwrap_or_default();
        let ua: String = row.get_by_name("updated_at_col").unwrap_or(None).unwrap_or_default();
        let da: String = row.get_by_name("deleted_at_col").unwrap_or(None).unwrap_or_default();
        let ec: Vec<String> = row.get_by_name("exclude_columns").unwrap_or(None).unwrap_or_default();
        table_names.push((tn, pp, pk, ua, da, ec));
    }

    let mut tables: Vec<serde_json::Value> = Vec::new();
    for (table_name, push_policy, pk_col, _ua, _da, _ec) in &table_names {
        let columns = introspect_columns(client, table_name, pk_col);

        tables.push(serde_json::json!({
            "table_name": table_name,
            "push_policy": push_policy,
            "columns": columns,
        }));
    }
    tables
}

fn introspect_columns(
    client: &SpiClient<'_>,
    table_name: &str,
    pk_column: &str,
) -> Vec<serde_json::Value> {
    let tup = match client.select(
        "SELECT a.attname AS name, \
         pg_catalog.format_type(a.atttypid, a.atttypmod) AS db_type, \
         NOT a.attnotnull AS nullable, \
         COALESCE(pg_catalog.pg_get_expr(ad.adbin, ad.adrelid), '') AS default_sql \
         FROM pg_catalog.pg_attribute a \
         JOIN pg_catalog.pg_class c ON c.oid = a.attrelid \
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum \
         WHERE c.relname = $1 AND n.nspname = ANY(current_schemas(false)) \
         AND a.attnum > 0 AND NOT a.attisdropped \
         ORDER BY a.attnum",
        None,
        &[table_name.into()],
    ) {
        Ok(t) => t,
        Err(_) => return vec![],
    };

    let mut columns: Vec<serde_json::Value> = Vec::new();
    for row in tup {
        let name: String = row.get_by_name::<String, &str>("name").unwrap_or(None).unwrap_or_default();
        let db_type: String = row.get_by_name::<String, &str>("db_type").unwrap_or(None).unwrap_or_default();
        let nullable: bool = row.get_by_name::<bool, &str>("nullable").unwrap_or(None).unwrap_or(true);
        let default_sql: String = row.get_by_name::<String, &str>("default_sql").unwrap_or(None).unwrap_or_default();

        let is_pk = name == pk_column;

        columns.push(serde_json::json!({
            "name": name,
            "db_type": db_type,
            "nullable": nullable,
            "default_sql": default_sql,
            "is_primary_key": is_pk,
        }));
    }
    columns
}

fn load_client_debug(
    client: &SpiClient<'_>,
    user_id: &str,
    client_id: &str,
) -> serde_json::Value {
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

    for row in tup {
        let id: String = row.get_by_name::<String, &str>("id").unwrap_or(None).unwrap_or_default();
        let cid: String = row.get_by_name::<String, &str>("client_id").unwrap_or(None).unwrap_or_default();
        let platform: String = row.get_by_name::<String, &str>("platform").unwrap_or(None).unwrap_or_default();
        let app_version: String = row.get_by_name::<String, &str>("app_version").unwrap_or(None).unwrap_or_default();
        let is_active: bool = row.get_by_name::<bool, &str>("is_active").unwrap_or(None).unwrap_or(false);
        let last_sync_at: Option<String> = row.get_by_name::<String, &str>("last_sync_at").unwrap_or(None);
        let last_pull_at: Option<String> = row.get_by_name::<String, &str>("last_pull_at").unwrap_or(None);
        let last_pull_seq: i64 = row.get_by_name::<i64, &str>("last_pull_seq").unwrap_or(None).unwrap_or(0);
        let bucket_subs: Vec<String> = row.get_by_name::<Vec<String>, &str>("bucket_subs").unwrap_or(None).unwrap_or_default();

        return serde_json::json!({
            "id": id,
            "client_id": cid,
            "platform": platform,
            "app_version": app_version,
            "is_active": is_active,
            "last_sync_at": last_sync_at,
            "last_pull_at": last_pull_at,
            "legacy_checkpoint": last_pull_seq,
            "bucket_subs": bucket_subs,
        });
    }

    pgrx::error!("client not found: {}/{}", user_id, client_id);
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
        let bid: String = row.get_by_name::<String, &str>("bucket_id").unwrap_or(None).unwrap_or_default();
        let cp: i64 = row.get_by_name::<i64, &str>("checkpoint").unwrap_or(None).unwrap_or(0);
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

fn load_changelog_stats(client: &SpiClient<'_>) -> serde_json::Value {
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
