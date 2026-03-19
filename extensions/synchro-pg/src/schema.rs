use pgrx::prelude::*;
use pgrx::spi::SpiClient;

/// Return the full schema contract for all registered tables.
///
/// Includes column metadata, types, nullability, defaults, and primary keys.
/// Used by client SDKs to build local database schemas.
#[pg_extern]
fn synchro_schema() -> pgrx::JsonB {
    Spi::connect_mut(|client| {
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
    Spi::connect_mut(|client| {
        let (schema_version, schema_hash) = crate::pull::get_latest_schema(&client);

        let tup_table = match client.select(
            "SELECT table_name, push_policy, pk_column, updated_at_col, \
             deleted_at_col, exclude_columns, has_updated_at, has_deleted_at \
             FROM sync_registry ORDER BY table_name",
            None,
            &[],
        ) {
            Ok(t) => t,
            Err(e) => pgrx::error!("querying sync_registry: {}", e),
        };

        let mut tables: Vec<serde_json::Value> = Vec::new();
        for row in tup_table {
            let table_name: String = row.get_by_name("table_name").unwrap_or(None).unwrap_or_default();
            let push_policy: String = row.get_by_name("push_policy").unwrap_or(None).unwrap_or_default();
            let pk_column: String = row.get_by_name("pk_column").unwrap_or(None).unwrap_or_default();
            let updated_at_col: String = row.get_by_name("updated_at_col").unwrap_or(None).unwrap_or_default();
            let deleted_at_col: String = row.get_by_name("deleted_at_col").unwrap_or(None).unwrap_or_default();
            let exclude_columns: Vec<String> = row.get_by_name("exclude_columns").unwrap_or(None).unwrap_or_default();
            let has_updated_at: bool = row.get_by_name("has_updated_at").unwrap_or(None).unwrap_or(false);
            let has_deleted_at: bool = row.get_by_name("has_deleted_at").unwrap_or(None).unwrap_or(false);

            let mut table = serde_json::json!({
                "table_name": table_name,
                "push_policy": push_policy,
                "primary_key": [pk_column],
                "exclude_columns": exclude_columns,
            });

            if has_updated_at {
                table["updated_at_column"] = serde_json::json!(updated_at_col);
            }
            if has_deleted_at {
                table["deleted_at_column"] = serde_json::json!(deleted_at_col);
            }

            tables.push(table);
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
    Spi::connect_mut(|client| {
        let client_info = load_client_debug(&client, p_user_id, p_client_id);

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
    let sql = format!("SELECT {} AS t", crate::push::iso8601_now());
    let tup = match client.select(&sql, None, &[]) {
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
         deleted_at_col, exclude_columns, has_updated_at, has_deleted_at \
         FROM sync_registry ORDER BY table_name",
        None,
        &[],
    ) {
        Ok(t) => t,
        Err(_) => return vec![],
    };

    let mut reg_entries: Vec<(String, String, String, String, String, Vec<String>, bool, bool)> = Vec::new();
    for row in reg_tup {
        let tn: String = row.get_by_name("table_name").unwrap_or(None).unwrap_or_default();
        let pp: String = row.get_by_name("push_policy").unwrap_or(None).unwrap_or_default();
        let pk: String = row.get_by_name("pk_column").unwrap_or(None).unwrap_or_default();
        let ua: String = row.get_by_name("updated_at_col").unwrap_or(None).unwrap_or_default();
        let da: String = row.get_by_name("deleted_at_col").unwrap_or(None).unwrap_or_default();
        let ec: Vec<String> = row.get_by_name("exclude_columns").unwrap_or(None).unwrap_or_default();
        let has_ua: bool = row.get_by_name("has_updated_at").unwrap_or(None).unwrap_or(false);
        let has_da: bool = row.get_by_name("has_deleted_at").unwrap_or(None).unwrap_or(false);
        reg_entries.push((tn, pp, pk, ua, da, ec, has_ua, has_da));
    }

    let mut tables: Vec<serde_json::Value> = Vec::new();
    for (table_name, push_policy, pk_col, ua_col, da_col, _ec, has_ua, has_da) in &reg_entries {
        let columns = introspect_columns(client, table_name, pk_col);

        let mut table = serde_json::json!({
            "table_name": table_name,
            "push_policy": push_policy,
            "primary_key": [pk_col],
            "columns": columns,
        });

        if *has_ua {
            table["updated_at_column"] = serde_json::json!(ua_col);
        }
        if *has_da {
            table["deleted_at_column"] = serde_json::json!(da_col);
        }

        tables.push(table);
    }
    tables
}

/// Map a PG type name to a platform-agnostic logical type.
///
/// Logical types tell client SDKs what SQLite/CoreData/Room type to use
/// without parsing PG-specific type names.
fn logical_type(db_type: &str) -> &'static str {
    let lower = db_type.to_lowercase();
    let base = lower.split('(').next().unwrap_or(&lower).trim();

    match base {
        "uuid" => "text",
        "text" | "varchar" | "character varying" | "char" | "character" | "name" => "text",
        "boolean" | "bool" => "integer",
        "smallint" | "int2" => "integer",
        "integer" | "int" | "int4" => "integer",
        "bigint" | "int8" => "integer",
        "real" | "float4" => "real",
        "double precision" | "float8" => "real",
        "numeric" | "decimal" => "real",
        "date" => "text",
        "timestamp without time zone" | "timestamp" => "text",
        "timestamp with time zone" | "timestamptz" => "text",
        "interval" => "text",
        "jsonb" | "json" => "text",
        "bytea" => "blob",
        "text[]" | "character varying[]" => "text",
        "integer[]" | "int[]" | "smallint[]" | "bigint[]" => "text",
        "inet" | "cidr" | "macaddr" | "macaddr8" => "text",
        "point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle" => "text",
        "xml" => "text",
        "int4range" | "int8range" | "numrange" | "tsrange" | "tstzrange" | "daterange" => "text",
        _ => "text",
    }
}

/// Classify a column default as portable, server-only, or none.
///
/// Portable defaults can be replicated on the client (literal values, simple
/// expressions). Server-only defaults depend on server state (sequences,
/// gen_random_uuid, now()).
fn default_kind(default_sql: &str) -> &'static str {
    if default_sql.is_empty() {
        return "none";
    }

    let lower = default_sql.to_lowercase();

    // Server-only: anything that depends on server state.
    if lower.contains("nextval(")
        || lower.contains("gen_random_uuid()")
        || lower.contains("uuid_generate_")
        || lower.contains("now()")
        || lower.contains("current_timestamp")
        || lower.contains("current_date")
        || lower.contains("current_time")
        || lower.contains("clock_timestamp()")
        || lower.contains("statement_timestamp()")
        || lower.contains("transaction_timestamp()")
    {
        return "server_only";
    }

    // Portable: literal values and simple expressions.
    "portable"
}

fn introspect_columns(
    client: &SpiClient<'_>,
    table_name: &str,
    pk_column: &str,
) -> Vec<serde_json::Value> {
    // Open the relation via the relcache. This works reliably in pgrx test
    // contexts where pg_catalog.pg_attribute rows may not yet be visible for
    // tables created in the same transaction. The relcache is populated by
    // DDL itself and does not depend on catalog snapshot visibility.
    let rel = match pgrx::PgRelation::open_with_name_and_share_lock(table_name) {
        Ok(r) => r,
        Err(e) => {
            log!("synchro_schema: cannot open relation {}: {}", table_name, e);
            return vec![];
        }
    };
    let tupdesc = pgrx::PgTupleDesc::from_relation(&rel);

    // Collect column metadata from the TupleDesc (name, type oid, type mod,
    // nullable, has_default). Skip dropped columns.
    struct ColMeta {
        name: String,
        attnum: i16,
        type_oid: u32,
        type_mod: i32,
        nullable: bool,
    }
    let mut col_metas: Vec<ColMeta> = Vec::with_capacity(tupdesc.len());
    for att in tupdesc.iter() {
        if att.is_dropped() {
            continue;
        }
        col_metas.push(ColMeta {
            name: att.name().to_string(),
            attnum: att.num(),
            type_oid: u32::from(att.type_oid().value()),
            type_mod: att.type_mod(),
            nullable: !att.attnotnull,
        });
    }

    if col_metas.is_empty() {
        return vec![];
    }

    // Resolve type OIDs + typmod to type names via format_type() in one query.
    let pairs: Vec<String> = col_metas
        .iter()
        .map(|c| format!("({},{})", c.type_oid, c.type_mod))
        .collect();
    let type_sql = format!(
        "SELECT format_type(v.oid::oid, v.tmod) AS type_name \
         FROM (VALUES {}) AS v(oid, tmod)",
        pairs.join(","),
    );
    let mut type_names: Vec<String> = Vec::with_capacity(col_metas.len());
    match client.select(&type_sql, None, &[]) {
        Ok(tup) => {
            for row in tup {
                let tn: String = row
                    .get_by_name::<String, &str>("type_name")
                    .unwrap_or(None)
                    .unwrap_or_default();
                type_names.push(tn);
            }
        }
        Err(_) => {
            type_names.resize(col_metas.len(), String::new());
        }
    }

    // Extract default expressions from the TupleDesc's constraint data.
    // The relcache stores defaults in TupleConstr.defval[], which contains
    // the serialized node tree (adbin). We deparse each via the C-level
    // deparse_expression() function. This avoids querying pg_attrdef,
    // which may not be visible in pgrx test contexts for tables created
    // in the same transaction.
    let mut default_map: std::collections::HashMap<i16, String> =
        std::collections::HashMap::new();

    unsafe {
        let tupdesc_ptr = tupdesc.as_ptr();
        let constr = (*tupdesc_ptr).constr;
        if !constr.is_null() {
            let num_defval = (*constr).num_defval as usize;
            let defval_ptr = (*constr).defval;
            if !defval_ptr.is_null() && num_defval > 0 {
                let defvals = std::slice::from_raw_parts(defval_ptr, num_defval);

                // Build a deparse context for this relation.
                let relname_cstr = std::ffi::CString::new(table_name)
                    .unwrap_or_else(|_| std::ffi::CString::new("").unwrap());
                let dpcontext = pgrx::pg_sys::deparse_context_for(
                    relname_cstr.as_ptr(),
                    rel.oid(),
                );

                for dv in defvals {
                    if dv.adbin.is_null() {
                        continue;
                    }
                    // Parse the node tree text back to a Node.
                    let node = pgrx::pg_sys::stringToNode(dv.adbin);
                    if node.is_null() {
                        continue;
                    }
                    // Deparse to readable SQL.
                    let expr_ptr = pgrx::pg_sys::deparse_expression(
                        node as *mut pgrx::pg_sys::Node,
                        dpcontext,
                        false,
                        false,
                    );
                    if !expr_ptr.is_null() {
                        let expr_cstr = std::ffi::CStr::from_ptr(expr_ptr);
                        if let Ok(expr_str) = expr_cstr.to_str() {
                            if !expr_str.is_empty() {
                                default_map.insert(dv.adnum, expr_str.to_string());
                            }
                        }
                    }
                }
            }
        }
    }

    // Assemble the column JSON objects.
    let mut columns: Vec<serde_json::Value> = Vec::with_capacity(col_metas.len());
    for (idx, cm) in col_metas.iter().enumerate() {
        let db_type_str = type_names.get(idx).cloned().unwrap_or_default();
        let default_sql_str = default_map.get(&cm.attnum).cloned().unwrap_or_default();

        let is_pk = cm.name == pk_column;
        let ltype = logical_type(&db_type_str);
        let dkind = default_kind(&default_sql_str);

        let mut col = serde_json::json!({
            "name": cm.name,
            "db_type": db_type_str,
            "logical_type": ltype,
            "nullable": cm.nullable,
            "is_primary_key": is_pk,
            "default_kind": dkind,
        });

        if !default_sql_str.is_empty() {
            col["default_sql"] = serde_json::json!(default_sql_str);
        }

        columns.push(col);
    }
    columns
}

fn load_client_debug(
    client: &SpiClient<'_>,
    user_id: &str,
    client_id: &str,
) -> serde_json::Value {
    let sql = format!(
        "SELECT id::text, client_id, user_id, platform, app_version, is_active, \
         {} AS last_sync_at, \
         {} AS last_pull_at, \
         last_pull_seq, bucket_subs \
         FROM sync_clients WHERE user_id = $1 AND client_id = $2",
        crate::push::iso8601_sql("last_sync_at"),
        crate::push::iso8601_sql("last_pull_at"),
    );
    let tup = match client.select(&sql, None, &[user_id.into(), client_id.into()]) {
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
        let member_count: i64 = match client.select(
            "SELECT COUNT(*) AS cnt FROM sync_bucket_edges WHERE bucket_id = $1",
            None,
            &[bid.as_str().into()],
        ) {
            Ok(tup) => tup.first().get_one::<i64>().ok().flatten().unwrap_or(0),
            Err(_) => 0,
        };

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
