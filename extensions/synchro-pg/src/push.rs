use pgrx::prelude::*;
use pgrx::spi::SpiClient;
use synchro_core::conflict::{Conflict, LwwResolver};
use synchro_core::protocol::{
    Operation, PUSH_STATUS_APPLIED, PUSH_STATUS_CONFLICT, PUSH_STATUS_REJECTED_RETRYABLE,
    PUSH_STATUS_REJECTED_TERMINAL,
};

use crate::client::validate_schema;
use crate::pull::{get_latest_schema, load_client_buckets, pg_quote_ident};
use crate::registry::{PushPolicy, TableRegistration};

// Clock skew tolerance is read from the synchro.clock_skew_tolerance_ms GUC
// (registered in lib.rs). Accessed via crate::CLOCK_SKEW_TOLERANCE_MS_GUC.

/// Push client changes to the server.
///
/// Returns JSONB with per-record results: status, reason, server timestamps.
/// Each record is processed independently within the same transaction.
/// RLS context is set via SET LOCAL app.user_id.
#[pg_extern]
fn synchro_push(
    p_user_id: &str,
    p_client_id: &str,
    p_changes: pgrx::JsonB,
    p_schema_version: default!(i64, "0"),
    p_schema_hash: default!(&str, "''"),
) -> pgrx::JsonB {
    // Validate schema if provided.
    if p_schema_version > 0 || !p_schema_hash.is_empty() {
        if let Err(err_json) = validate_schema(p_schema_version, p_schema_hash) {
            return err_json;
        }
    }

    // Parse changes array.
    let changes: Vec<serde_json::Value> = match p_changes.0 {
        serde_json::Value::Array(arr) => arr,
        _ => pgrx::error!("p_changes must be a JSON array"),
    };

    Spi::connect_mut(|client| {
        // Set RLS context (propagate error, do not discard).
        client
            .update(
                "SELECT set_config('app.user_id', $1, true)",
                None,
                &[p_user_id.into()],
            )
            .unwrap_or_else(|e| pgrx::error!("setting RLS context: {}", e));

        // Validate client exists.
        if let Err(err_json) = load_client_buckets(client, p_user_id, p_client_id) {
            return err_json;
        }

        // Load registry.
        let registry = load_registry_inner(client);

        // Check if write_protect function exists.
        let has_write_protect = check_write_protect_exists(client);

        // Process each change, annotating results with record identity.
        let mut results: Vec<serde_json::Value> = Vec::with_capacity(changes.len());
        for change in &changes {
            let id = change.get("id").and_then(|v| v.as_str()).unwrap_or("");
            let table_name = change.get("table_name").and_then(|v| v.as_str()).unwrap_or("");
            let operation = change.get("operation").and_then(|v| v.as_str()).unwrap_or("");

            let mut result = process_record(
                client,
                p_user_id,
                change,
                &registry,
                has_write_protect,
            );

            // Annotate with record identity for the wire protocol.
            if let Some(obj) = result.as_object_mut() {
                obj.insert("id".into(), serde_json::json!(id));
                obj.insert("table_name".into(), serde_json::json!(table_name));
                obj.insert("operation".into(), serde_json::json!(operation));
            }

            results.push(result);
        }

        // Split results into accepted (applied/conflict) and rejected.
        let mut accepted: Vec<serde_json::Value> = Vec::new();
        let mut rejected: Vec<serde_json::Value> = Vec::new();
        for result in results {
            let status = result.get("status").and_then(|v| v.as_str()).unwrap_or("");
            match status {
                "applied" | "conflict" => accepted.push(result),
                _ => rejected.push(result),
            }
        }

        // Update client sync timestamp and get checkpoint + server time.
        let mut checkpoint: i64 = 0;
        let mut server_time = String::new();
        if let Ok(tup) = client.update(
            "UPDATE sync_clients SET last_sync_at = now() \
             WHERE user_id = $1 AND client_id = $2 \
             RETURNING COALESCE(last_pull_seq, 0) AS checkpoint, \
             now()::timestamptz::text AS server_time",
            None,
            &[p_user_id.into(), p_client_id.into()],
        ) {
            for row in tup {
                checkpoint = row
                    .get_by_name::<i64, &str>("checkpoint")
                    .unwrap_or(None)
                    .unwrap_or(0);
                server_time = row
                    .get_by_name::<String, &str>("server_time")
                    .unwrap_or(None)
                    .unwrap_or_default();
            }
        }

        // Get schema info.
        let (schema_version, schema_hash) = get_latest_schema(client);

        pgrx::JsonB(serde_json::json!({
            "accepted": accepted,
            "rejected": rejected,
            "checkpoint": checkpoint,
            "server_time": server_time,
            "schema_version": schema_version,
            "schema_hash": schema_hash,
        }))
    })
}

// ---------------------------------------------------------------------------
// Per-record processing
// ---------------------------------------------------------------------------

fn process_record(
    client: &mut SpiClient<'_>,
    user_id: &str,
    change: &serde_json::Value,
    registry: &[TableRegistration],
    has_write_protect: bool,
) -> serde_json::Value {
    // Parse record fields.
    let id = match change.get("id").and_then(|v| v.as_str()) {
        Some(s) => s,
        None => return reject_terminal("invalid_data", "missing 'id' field"),
    };
    let table_name = match change.get("table_name").and_then(|v| v.as_str()) {
        Some(s) => s,
        None => return reject_terminal("invalid_data", "missing 'table_name' field"),
    };
    let op_str = match change.get("operation").and_then(|v| v.as_str()) {
        Some(s) => s,
        None => return reject_terminal("invalid_data", "missing 'operation' field"),
    };
    let operation = match Operation::parse(op_str) {
        Some(op) => op,
        None => {
            return reject_terminal(
                "invalid_operation",
                &format!("unknown operation: {}", op_str),
            )
        }
    };

    // Find table in registry.
    let table_reg = match registry.iter().find(|t| t.table_name == table_name) {
        Some(t) => t,
        None => {
            return reject_terminal(
                "table_not_registered",
                &format!("table {:?} not registered", table_name),
            )
        }
    };

    // Check push policy.
    if table_reg.push_policy == PushPolicy::ReadOnly {
        return reject_terminal(
            "table_read_only",
            &format!("table {:?} is read-only", table_name),
        );
    }

    // Get client data.
    let mut data = match change.get("data") {
        Some(d) if d.is_object() => d.clone(),
        _ => serde_json::json!({}),
    };

    // Strip protected columns (pk, timestamps, created_at) and exclude_columns.
    strip_protected_columns(&mut data, table_reg);

    // Call write_protect function if it exists.
    if has_write_protect {
        match call_write_protect(client, user_id, table_name, op_str, &data) {
            Ok(transformed) => {
                data = transformed;
                // Re-strip protected columns in case write_protect re-introduced them.
                strip_protected_columns(&mut data, table_reg);
            }
            Err(msg) => return reject_terminal("write_protect_rejected", &msg),
        }
    }

    // Load the actual column set from pg_catalog to validate client-supplied column names.
    let valid_columns = get_table_columns(client, table_name);

    // Parse client timestamps for conflict resolution.
    let client_updated_at = change
        .get("client_updated_at")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc));

    let base_updated_at = change
        .get("base_updated_at")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc));

    // Dispatch by operation.
    match operation {
        Operation::Insert => push_create(
            client,
            user_id,
            id,
            table_reg,
            &data,
            &valid_columns,
            client_updated_at,
            base_updated_at,
        ),
        Operation::Update => push_update(
            client,
            user_id,
            id,
            table_reg,
            &data,
            &valid_columns,
            client_updated_at,
            base_updated_at,
        ),
        Operation::Delete => push_delete(client, id, table_reg),
    }
}

// ---------------------------------------------------------------------------
// CREATE
// ---------------------------------------------------------------------------

fn push_create(
    client: &mut SpiClient<'_>,
    user_id: &str,
    id: &str,
    table_reg: &TableRegistration,
    data: &serde_json::Value,
    valid_columns: &std::collections::HashSet<String>,
    client_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    base_updated_at: Option<chrono::DateTime<chrono::Utc>>,
) -> serde_json::Value {
    // Check if record already exists.
    let existing = load_existing_record(client, id, table_reg);

    if let Some(ref ex) = existing {
        if ex.deleted_at.is_none() {
            return conflict_result("record_exists", "record already exists", ex);
        }
        // Exists but soft-deleted: resurrection via update path.
        return push_update_inner(
            client,
            user_id,
            id,
            table_reg,
            data,
            valid_columns,
            client_updated_at,
            base_updated_at,
            existing,
            true,
        );
    }

    // Insert new record.
    let columns = allowed_columns(data, table_reg, valid_columns);
    if columns.is_empty() {
        return reject_terminal("invalid_data", "no allowed columns for insert");
    }

    // Build INSERT SQL. Column names are validated against pg_catalog.
    let col_list: String = std::iter::once(pg_quote_ident(&table_reg.pk_column))
        .chain(columns.iter().map(|c| pg_quote_ident(c)))
        .collect::<Vec<_>>()
        .join(", ");

    // Use jsonb_populate_record for automatic type coercion (handles numeric, uuid, etc.).
    let select_list: String = std::iter::once(format!("$1::{}", table_reg.pk_type))
        .chain(columns.iter().map(|c| format!("r.{}", pg_quote_ident(c))))
        .collect::<Vec<_>>()
        .join(", ");

    let returning = if table_reg.has_updated_at {
        format!(
            " RETURNING {}::text AS updated_at",
            pg_quote_ident(&table_reg.updated_at_col)
        )
    } else {
        String::new()
    };

    let sql = format!(
        "INSERT INTO {} ({}) SELECT {} FROM jsonb_populate_record(NULL::{}, $2) r{}",
        pg_quote_ident(&table_reg.table_name),
        col_list,
        select_list,
        pg_quote_ident(&table_reg.table_name),
        returning,
    );

    let data_jsonb = pgrx::JsonB(data.clone());
    match client.update(&sql, None, &[id.into(), data_jsonb.into()]) {
        Ok(tup) => {
            let server_updated_at = if table_reg.has_updated_at {
                tup.first().get_one::<String>().ok().flatten()
            } else {
                None
            };
            applied_result(server_updated_at.as_deref(), None)
        }
        Err(e) => reject_retryable("create_failed", &format!("{}", e)),
    }
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

fn push_update(
    client: &mut SpiClient<'_>,
    user_id: &str,
    id: &str,
    table_reg: &TableRegistration,
    data: &serde_json::Value,
    valid_columns: &std::collections::HashSet<String>,
    client_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    base_updated_at: Option<chrono::DateTime<chrono::Utc>>,
) -> serde_json::Value {
    let existing = load_existing_record(client, id, table_reg);
    push_update_inner(
        client,
        user_id,
        id,
        table_reg,
        data,
        valid_columns,
        client_updated_at,
        base_updated_at,
        existing,
        false,
    )
}

fn push_update_inner(
    client: &mut SpiClient<'_>,
    _user_id: &str,
    id: &str,
    table_reg: &TableRegistration,
    data: &serde_json::Value,
    valid_columns: &std::collections::HashSet<String>,
    client_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    base_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    existing: Option<ExistingRecord>,
    is_resurrection: bool,
) -> serde_json::Value {
    let existing = match existing {
        Some(ex) => ex,
        None => return reject_terminal("record_not_found", "record does not exist"),
    };

    // If soft-deleted and not a resurrection: conflict.
    if existing.deleted_at.is_some() && !is_resurrection {
        return conflict_result("record_deleted", "record has been deleted", &existing);
    }

    // Conflict resolution via LWW (if table has updated_at).
    if table_reg.has_updated_at {
        if let Some(server_time) = &existing.updated_at {
            if let Some(server_utc) = parse_pg_timestamptz(server_time) {
                let client_time = client_updated_at.unwrap_or(chrono::Utc::now());

                let conflict = Conflict {
                    table: table_reg.table_name.clone(),
                    record_id: id.to_string(),
                    client_id: String::new(),
                    user_id: String::new(),
                    client_time,
                    server_time: server_utc,
                    base_version: base_updated_at,
                };

                let tolerance = chrono::Duration::milliseconds(
                    crate::CLOCK_SKEW_TOLERANCE_MS_GUC.get() as i64,
                );
                let resolver = LwwResolver::new(tolerance);
                let resolution = resolver.resolve(&conflict);

                if resolution.winner == "server" {
                    return conflict_result(
                        "server_won_conflict",
                        &resolution.reason,
                        &existing,
                    );
                }
            }
        }
    }

    // Build UPDATE SQL.
    let columns = allowed_columns(data, table_reg, valid_columns);
    if columns.is_empty() && !is_resurrection {
        return applied_result(existing.updated_at.as_deref(), None);
    }

    let server_updated_at = if !columns.is_empty() {
        let set_clause: String = columns
            .iter()
            .map(|c| {
                format!(
                    "{} = ($1::jsonb ->> {})",
                    pg_quote_ident(c),
                    pg_quote_literal(c),
                )
            })
            .collect::<Vec<_>>()
            .join(", ");

        let returning = if table_reg.has_updated_at {
            format!(
                " RETURNING {}::text AS updated_at",
                pg_quote_ident(&table_reg.updated_at_col)
            )
        } else {
            String::new()
        };

        let sql = format!(
            "UPDATE {} SET {} WHERE {} = $2::{}{}",
            pg_quote_ident(&table_reg.table_name),
            set_clause,
            pg_quote_ident(&table_reg.pk_column),
            table_reg.pk_type,
            returning,
        );

        let data_jsonb = pgrx::JsonB(data.clone());
        match client.update(&sql, None, &[data_jsonb.into(), id.into()]) {
            Ok(tup) => {
                if table_reg.has_updated_at {
                    tup.first().get_one::<String>().ok().flatten()
                } else {
                    None
                }
            }
            Err(e) => return reject_retryable("update_failed", &format!("{}", e)),
        }
    } else {
        existing.updated_at.clone()
    };

    // Resurrection: clear deleted_at.
    if is_resurrection && table_reg.has_deleted_at {
        let resurrection_sql = if table_reg.has_updated_at {
            format!(
                "UPDATE {} SET {} = NULL WHERE {} = $1::{} RETURNING {}::text AS updated_at",
                pg_quote_ident(&table_reg.table_name),
                pg_quote_ident(&table_reg.deleted_at_col),
                pg_quote_ident(&table_reg.pk_column),
                table_reg.pk_type,
                pg_quote_ident(&table_reg.updated_at_col),
            )
        } else {
            format!(
                "UPDATE {} SET {} = NULL WHERE {} = $1::{}",
                pg_quote_ident(&table_reg.table_name),
                pg_quote_ident(&table_reg.deleted_at_col),
                pg_quote_ident(&table_reg.pk_column),
                table_reg.pk_type,
            )
        };

        match client.update(&resurrection_sql, None, &[id.into()]) {
            Ok(tup) => {
                let ua = if table_reg.has_updated_at {
                    tup.first().get_one::<String>().ok().flatten()
                } else {
                    None
                };
                return applied_result(ua.as_deref(), None);
            }
            Err(e) => return reject_retryable("resurrection_failed", &format!("{}", e)),
        }
    }

    applied_result(server_updated_at.as_deref(), None)
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

fn push_delete(
    client: &mut SpiClient<'_>,
    id: &str,
    table_reg: &TableRegistration,
) -> serde_json::Value {
    let existing = load_existing_record(client, id, table_reg);

    if table_reg.has_deleted_at {
        push_soft_delete(client, id, table_reg, existing)
    } else {
        push_hard_delete(client, id, table_reg)
    }
}

fn push_soft_delete(
    client: &mut SpiClient<'_>,
    id: &str,
    table_reg: &TableRegistration,
    existing: Option<ExistingRecord>,
) -> serde_json::Value {
    match &existing {
        None => return reject_terminal("record_not_found", "record does not exist"),
        Some(ex) if ex.deleted_at.is_some() => {
            // Already soft-deleted: idempotent success.
            return applied_result(None, ex.deleted_at.as_deref());
        }
        _ => {}
    }

    let sql = format!(
        "UPDATE {} SET {} = now() WHERE {} = $1::{} RETURNING {}::text AS deleted_at",
        pg_quote_ident(&table_reg.table_name),
        pg_quote_ident(&table_reg.deleted_at_col),
        pg_quote_ident(&table_reg.pk_column),
        table_reg.pk_type,
        pg_quote_ident(&table_reg.deleted_at_col),
    );

    match client.update(&sql, None, &[id.into()]) {
        Ok(tup) => {
            let deleted_at = tup.first().get_one::<String>().ok().flatten();
            applied_result(None, deleted_at.as_deref())
        }
        Err(e) => reject_retryable("delete_failed", &format!("{}", e)),
    }
}

fn push_hard_delete(
    client: &mut SpiClient<'_>,
    id: &str,
    table_reg: &TableRegistration,
) -> serde_json::Value {
    let sql = format!(
        "DELETE FROM {} WHERE {} = $1::{}",
        pg_quote_ident(&table_reg.table_name),
        pg_quote_ident(&table_reg.pk_column),
        table_reg.pk_type,
    );

    match client.update(&sql, None, &[id.into()]) {
        Ok(_) => applied_result(None, None),
        Err(e) => reject_retryable("delete_failed", &format!("{}", e)),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

struct ExistingRecord {
    updated_at: Option<String>,
    deleted_at: Option<String>,
    data: Option<String>,
}

fn load_existing_record(
    client: &SpiClient<'_>,
    id: &str,
    table_reg: &TableRegistration,
) -> Option<ExistingRecord> {
    let updated_at_expr = if table_reg.has_updated_at {
        format!("{}::text", pg_quote_ident(&table_reg.updated_at_col))
    } else {
        "NULL::text".into()
    };
    let deleted_at_expr = if table_reg.has_deleted_at {
        format!("{}::text", pg_quote_ident(&table_reg.deleted_at_col))
    } else {
        "NULL::text".into()
    };

    let sql = format!(
        "SELECT {} AS updated_at, {} AS deleted_at, row_to_json(t)::text AS data \
         FROM {} t WHERE {} = $1::{}",
        updated_at_expr,
        deleted_at_expr,
        pg_quote_ident(&table_reg.table_name),
        pg_quote_ident(&table_reg.pk_column),
        table_reg.pk_type,
    );

    let tup_table = match client.select(&sql, None, &[id.into()]) {
        Ok(t) => t,
        Err(_) => return None,
    };

    for row in tup_table {
        let updated_at: Option<String> = row
            .get_by_name::<String, &str>("updated_at")
            .unwrap_or(None);
        let deleted_at: Option<String> = row
            .get_by_name::<String, &str>("deleted_at")
            .unwrap_or(None);
        let data: Option<String> = row
            .get_by_name::<String, &str>("data")
            .unwrap_or(None);
        return Some(ExistingRecord {
            updated_at,
            deleted_at,
            data,
        });
    }

    None
}

/// Load the actual column names for a table.
/// Uses SELECT * LIMIT 0 to get column names from the result descriptor,
/// which avoids catalog cache visibility issues in nested SPI connections.
fn get_table_columns(
    client: &SpiClient<'_>,
    table_name: &str,
) -> std::collections::HashSet<String> {
    let describe_sql = format!(
        "SELECT * FROM {} LIMIT 0",
        crate::pull::pg_quote_ident(table_name)
    );
    let tup_table = match client.select(&describe_sql, None, &[]) {
        Ok(t) => t,
        Err(e) => pgrx::error!("introspecting table columns: {}", e),
    };

    let mut cols = std::collections::HashSet::new();
    let ncols = tup_table.columns().unwrap_or(0);
    for i in 1..=ncols {
        if let Ok(name) = tup_table.column_name(i) {
            cols.insert(name);
        }
    }
    cols
}

/// Get allowed columns: client data keys that are real table columns,
/// minus protected columns (pk, timestamps, created_at) and exclude_columns.
fn allowed_columns(
    data: &serde_json::Value,
    table_reg: &TableRegistration,
    valid_columns: &std::collections::HashSet<String>,
) -> Vec<String> {
    let obj = match data.as_object() {
        Some(o) => o,
        None => return vec![],
    };

    let mut protected = std::collections::HashSet::new();
    protected.insert(table_reg.pk_column.as_str());
    if table_reg.has_updated_at {
        protected.insert(table_reg.updated_at_col.as_str());
    }
    if table_reg.has_deleted_at {
        protected.insert(table_reg.deleted_at_col.as_str());
    }
    // Protect created_at by convention (server-authoritative).
    protected.insert("created_at");

    let excluded: std::collections::HashSet<&str> = table_reg
        .exclude_columns
        .iter()
        .map(|s| s.as_str())
        .collect();

    obj.keys()
        .filter(|k| {
            let k_str = k.as_str();
            valid_columns.contains(k_str)
                && !protected.contains(k_str)
                && !excluded.contains(k_str)
        })
        .cloned()
        .collect()
}

/// Strip protected and excluded columns from client data in place.
fn strip_protected_columns(data: &mut serde_json::Value, table_reg: &TableRegistration) {
    if let Some(obj) = data.as_object_mut() {
        obj.remove(&table_reg.pk_column);
        if table_reg.has_updated_at {
            obj.remove(&table_reg.updated_at_col);
        }
        if table_reg.has_deleted_at {
            obj.remove(&table_reg.deleted_at_col);
        }
        obj.remove("created_at");
        for col in &table_reg.exclude_columns {
            obj.remove(col);
        }
    }
}

/// Check if synchro_write_protect() function exists.
fn check_write_protect_exists(client: &SpiClient<'_>) -> bool {
    let tup = match client.select(
        "SELECT EXISTS (
            SELECT 1 FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE p.proname = 'synchro_write_protect'
            AND n.nspname = ANY(current_schemas(false))
        ) AS exists",
        None,
        &[],
    ) {
        Ok(t) => t,
        Err(_) => return false,
    };
    tup.first().get_one::<bool>().ok().flatten().unwrap_or(false)
}

/// Call the developer-defined synchro_write_protect() function.
fn call_write_protect(
    client: &mut SpiClient<'_>,
    user_id: &str,
    table_name: &str,
    operation: &str,
    data: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let data_jsonb = pgrx::JsonB(data.clone());

    let result: Option<pgrx::JsonB> = match client.update(
        "SELECT synchro_write_protect($1, $2, $3, $4) AS result",
        None,
        &[
            user_id.into(),
            table_name.into(),
            operation.into(),
            data_jsonb.into(),
        ],
    ) {
        Ok(tup) => tup.first().get_one().ok().flatten(),
        Err(e) => return Err(format!("{}", e)),
    };

    match result {
        Some(jsonb) => Ok(jsonb.0),
        None => Err("write_protect returned NULL".into()),
    }
}

/// Load registry within an existing SPI context.
fn load_registry_inner(client: &SpiClient<'_>) -> Vec<TableRegistration> {
    let tup_table = match client.select(
        "SELECT table_name, bucket_sql, pk_column, pk_type, updated_at_col, \
         deleted_at_col, push_policy, exclude_columns, has_updated_at, has_deleted_at \
         FROM sync_registry ORDER BY table_name",
        None,
        &[],
    ) {
        Ok(t) => t,
        Err(e) => pgrx::error!("loading registry: {}", e),
    };

    let mut tables = Vec::new();
    for row in tup_table {
        let table_name: String = row.get_by_name("table_name").unwrap_or(None).unwrap_or_default();
        let bucket_sql: String = row.get_by_name("bucket_sql").unwrap_or(None).unwrap_or_default();
        let pk_column: String = row.get_by_name("pk_column").unwrap_or(None).unwrap_or_default();
        let pk_type: String = row.get_by_name("pk_type").unwrap_or(None).unwrap_or_else(|| "text".to_string());
        let updated_at_col: String = row.get_by_name("updated_at_col").unwrap_or(None).unwrap_or_default();
        let deleted_at_col: String = row.get_by_name("deleted_at_col").unwrap_or(None).unwrap_or_default();
        let push_policy_str: String = row.get_by_name("push_policy").unwrap_or(None).unwrap_or_default();
        let exclude_columns: Vec<String> = row.get_by_name("exclude_columns").unwrap_or(None).unwrap_or_default();
        let has_updated_at: bool = row.get_by_name("has_updated_at").unwrap_or(None).unwrap_or(false);
        let has_deleted_at: bool = row.get_by_name("has_deleted_at").unwrap_or(None).unwrap_or(false);

        tables.push(TableRegistration {
            table_name, bucket_sql, pk_column, pk_type, updated_at_col, deleted_at_col,
            push_policy: PushPolicy::parse(&push_policy_str).unwrap_or(PushPolicy::Enabled),
            exclude_columns, has_updated_at, has_deleted_at,
        });
    }
    tables
}

// ---------------------------------------------------------------------------
// Response builders
// ---------------------------------------------------------------------------

fn applied_result(
    server_updated_at: Option<&str>,
    server_deleted_at: Option<&str>,
) -> serde_json::Value {
    let mut r = serde_json::json!({
        "status": PUSH_STATUS_APPLIED,
    });
    if let Some(ua) = server_updated_at {
        r["server_updated_at"] = serde_json::json!(ua);
    }
    if let Some(da) = server_deleted_at {
        r["server_deleted_at"] = serde_json::json!(da);
    }
    r
}

fn conflict_result(
    reason_code: &str,
    message: &str,
    existing: &ExistingRecord,
) -> serde_json::Value {
    let mut r = serde_json::json!({
        "status": PUSH_STATUS_CONFLICT,
        "reason_code": reason_code,
        "message": message,
    });
    if let Some(ua) = &existing.updated_at {
        r["server_updated_at"] = serde_json::json!(ua);
    }
    // Include server version data so clients can perform merge resolution.
    if let Some(data_str) = &existing.data {
        if let Ok(data) = serde_json::from_str::<serde_json::Value>(data_str) {
            r["server_version"] = data;
        }
    }
    r
}

fn reject_terminal(reason_code: &str, message: &str) -> serde_json::Value {
    serde_json::json!({
        "status": PUSH_STATUS_REJECTED_TERMINAL,
        "reason_code": reason_code,
        "message": message,
    })
}

fn reject_retryable(reason_code: &str, message: &str) -> serde_json::Value {
    serde_json::json!({
        "status": PUSH_STATUS_REJECTED_RETRYABLE,
        "reason_code": reason_code,
        "message": message,
    })
}

/// Parse a PostgreSQL timestamptz text representation into a chrono DateTime<Utc>.
///
/// PostgreSQL outputs timestamptz::text in formats like:
///   "2025-06-15 12:00:00+00"
///   "2025-06-15 12:00:00.123+00"
///   "2099-01-01 00:00:00-05"
///
/// This differs from RFC3339 which uses 'T' separator and full timezone offset (+00:00).
/// This function handles both PostgreSQL format and RFC3339 for robustness.
fn parse_pg_timestamptz(s: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    // Try RFC3339 first (handles client-supplied timestamps).
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&chrono::Utc));
    }

    // Try PostgreSQL's default timestamptz text output format.
    // Format: "YYYY-MM-DD HH:MM:SS.fff+ZZ" or "YYYY-MM-DD HH:MM:SS+ZZ"
    let formats = [
        "%Y-%m-%d %H:%M:%S%.f%#z",
        "%Y-%m-%d %H:%M:%S%#z",
    ];
    for fmt in &formats {
        if let Ok(dt) = chrono::DateTime::parse_from_str(s, fmt) {
            return Some(dt.with_timezone(&chrono::Utc));
        }
    }

    None
}

/// Quote a SQL string literal (escape single quotes).
fn pg_quote_literal(s: &str) -> String {
    let escaped = s.replace('\'', "''");
    format!("'{}'", escaped)
}
