use pgrx::prelude::*;
use pgrx::spi::SpiClient;
use synchro_core::contract::{ProtocolErrorCode, RebuildRecord, RebuildRequest, RebuildResponse};
use synchro_core::limits::clamp_rebuild_limit;

use crate::client::{protocol_error_response, validate_schema};
use crate::pull::{
    compute_bucket_checksums, get_latest_schema, hydrate_records, load_client_buckets,
    record_server_version, vnext_pk_value,
};
use crate::registry::load_registry;

/// Paginated bucket rebuild.
///
/// Returns all current records in a bucket via sync_bucket_edges,
/// using keyset (cursor) pagination. Each page returns hydrated records
/// with a cursor for the next page. On the final page, includes the
/// bucket checksum and sets the client's checkpoint for this bucket.
#[pg_extern]
fn synchro_rebuild(
    p_user_id: &str,
    p_client_id: &str,
    p_bucket_id: &str,
    p_cursor: default!(&str, "''"),
    p_limit: default!(i32, "100"),
    p_schema_version: default!(i64, "0"),
    p_schema_hash: default!(&str, "''"),
) -> pgrx::JsonB {
    // Validate schema if provided.
    if p_schema_version > 0 || !p_schema_hash.is_empty() {
        if let Err(err_json) = validate_schema(p_schema_version, p_schema_hash) {
            return err_json;
        }
    }

    let limit = clamp_rebuild_limit(p_limit);

    Spi::connect_mut(|client| {
        // Set RLS context.
        let _ = client.update(
            "SELECT set_config('app.user_id', $1, true)",
            None,
            &[p_user_id.into()],
        );

        // Validate client exists and is subscribed to this bucket.
        let bucket_subs = match load_client_buckets(client, p_user_id, p_client_id) {
            Ok(subs) => subs,
            Err(err_json) => return err_json,
        };
        if !bucket_subs.contains(&p_bucket_id.to_string()) {
            return pgrx::JsonB(serde_json::json!({"error": "client_not_found"}));
        }

        // Load registry for hydration.
        let registry = match load_registry() {
            Ok(r) => r,
            Err(e) => pgrx::error!("failed to load registry: {}", e),
        };

        let (schema_version, schema_hash) = get_latest_schema(client);

        // Capture rebuild checkpoint: MAX(seq) at this moment.
        let rebuild_checkpoint: i64 = {
            let tup = client
                .select(
                    "SELECT COALESCE(MAX(seq), 0) AS cp FROM sync_changelog",
                    None,
                    &[],
                )
                .unwrap_or_else(|e| pgrx::error!("querying max seq: {}", e));
            let mut cp = 0i64;
            for row in tup {
                cp = row
                    .get_by_name::<i64, &str>("cp")
                    .unwrap_or(None)
                    .unwrap_or(0);
            }
            cp
        };

        // Parse cursor: "table_name|record_id" or empty.
        let cursor_parts = parse_cursor(p_cursor);

        // Query bucket edges with keyset pagination.
        let edges = query_edges(client, p_bucket_id, cursor_parts.as_ref(), limit + 1);

        // Detect has_more.
        let has_more = edges.len() > limit as usize;
        let edges: Vec<(String, String)> = if has_more {
            edges[..limit as usize].to_vec()
        } else {
            edges
        };

        if edges.is_empty() {
            // Final page (or empty bucket). Set checkpoint and advance legacy.
            complete_rebuild(
                client,
                p_user_id,
                p_client_id,
                p_bucket_id,
                rebuild_checkpoint,
            );

            let bucket_checksums = compute_bucket_checksums(client, &[p_bucket_id.to_string()]);
            let bucket_checksum = bucket_checksums.get(p_bucket_id).copied();

            return pgrx::JsonB(serde_json::json!({
                "records": [],
                "cursor": "",
                "checkpoint": rebuild_checkpoint,
                "has_more": false,
                "bucket_checksum": bucket_checksum,
                "schema_version": schema_version,
                "schema_hash": schema_hash,
            }));
        }

        // Group edges by table for batch hydration.
        let mut by_table: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for (table_name, record_id) in &edges {
            by_table
                .entry(table_name.clone())
                .or_default()
                .push(record_id.clone());
        }

        // Hydrate records per table, filter out soft-deleted.
        let mut records: Vec<serde_json::Value> = Vec::new();
        for (table_name, record_ids) in &by_table {
            let id_refs: Vec<&str> = record_ids.iter().map(|s| s.as_str()).collect();
            let hydrated = hydrate_records(client, table_name, &id_refs, &registry);

            for mut record in hydrated {
                // Rebuild is snapshot mode: filter out soft-deleted records.
                let is_deleted = record.get("deleted_at").and_then(|v| v.as_str()).is_some();
                if is_deleted {
                    continue;
                }

                record
                    .as_object_mut()
                    .unwrap()
                    .insert("bucket_id".into(), serde_json::json!(p_bucket_id));
                records.push(record);
            }
        }

        // Compute next cursor from last edge.
        let next_cursor = if has_more {
            if let Some((t, r)) = edges.last() {
                format!("{}|{}", t, r)
            } else {
                String::new()
            }
        } else {
            String::new()
        };

        // On final page: compute bucket checksum and set checkpoint.
        let bucket_checksum = if !has_more {
            complete_rebuild(
                client,
                p_user_id,
                p_client_id,
                p_bucket_id,
                rebuild_checkpoint,
            );
            let checksums = compute_bucket_checksums(client, &[p_bucket_id.to_string()]);
            checksums.get(p_bucket_id).copied()
        } else {
            None
        };

        let mut response = serde_json::json!({
            "records": records,
            "cursor": next_cursor,
            "checkpoint": rebuild_checkpoint,
            "has_more": has_more,
            "schema_version": schema_version,
            "schema_hash": schema_hash,
        });

        if let Some(cs) = bucket_checksum {
            response["bucket_checksum"] = serde_json::json!(cs);
        }

        pgrx::JsonB(response)
    })
}

/// Rebuild a single vNext scope using paginated snapshot records.
#[pg_extern]
fn synchro_rebuild_vnext(p_user_id: &str, p_request: pgrx::JsonB) -> pgrx::JsonB {
    let request: RebuildRequest = match serde_json::from_value(p_request.0) {
        Ok(request) => request,
        Err(err) => {
            return protocol_error_response(
                ProtocolErrorCode::InvalidRequest,
                format!("invalid rebuild request: {err}"),
                false,
            );
        }
    };

    let limit = clamp_rebuild_limit(request.limit);

    Spi::connect_mut(|client| {
        let _ = client.update(
            "SELECT set_config('app.user_id', $1, true)",
            None,
            &[p_user_id.into()],
        );

        let bucket_subs = match load_client_buckets(client, p_user_id, &request.client_id) {
            Ok(subs) => subs,
            Err(_) => {
                return protocol_error_response(
                    ProtocolErrorCode::InvalidRequest,
                    "client is not registered",
                    false,
                );
            }
        };
        if !bucket_subs.contains(&request.scope) {
            return protocol_error_response(
                ProtocolErrorCode::InvalidRequest,
                format!("client is not subscribed to scope {}", request.scope),
                false,
            );
        }

        let registry = match load_registry() {
            Ok(registry) => registry,
            Err(err) => pgrx::error!("failed to load registry: {}", err),
        };
        let rebuild_checkpoint: i64 = {
            let tup = client
                .select(
                    "SELECT COALESCE(MAX(seq), 0) AS cp FROM sync_changelog",
                    None,
                    &[],
                )
                .unwrap_or_else(|err| pgrx::error!("querying max seq: {}", err));
            let mut checkpoint = 0i64;
            for row in tup {
                checkpoint = row
                    .get_by_name::<i64, &str>("cp")
                    .unwrap_or(None)
                    .unwrap_or(0);
            }
            checkpoint
        };

        let cursor_parts = request.cursor.as_deref().and_then(parse_cursor);
        let edges = query_edges(client, &request.scope, cursor_parts.as_ref(), limit + 1);
        let has_more = edges.len() > limit as usize;
        let edges: Vec<(String, String)> = if has_more {
            edges[..limit as usize].to_vec()
        } else {
            edges
        };

        if edges.is_empty() {
            complete_rebuild(
                client,
                p_user_id,
                &request.client_id,
                &request.scope,
                rebuild_checkpoint,
            );

            let checksum = compute_bucket_checksums(client, std::slice::from_ref(&request.scope))
                .get(&request.scope)
                .copied()
                .unwrap_or(0)
                .to_string();
            let response = RebuildResponse {
                scope: request.scope.clone(),
                records: Vec::new(),
                cursor: None,
                has_more: false,
                final_scope_cursor: Some(rebuild_checkpoint.to_string()),
                checksum: Some(checksum),
            };
            if let Err(err) = response.validate() {
                pgrx::error!("invalid vNext rebuild response: {}", err);
            }
            return pgrx::JsonB(serde_json::to_value(response).unwrap());
        }

        let mut by_table: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for (table_name, record_id) in &edges {
            by_table
                .entry(table_name.clone())
                .or_default()
                .push(record_id.clone());
        }

        let mut records = Vec::new();
        for (table_name, record_ids) in &by_table {
            let id_refs: Vec<&str> = record_ids
                .iter()
                .map(|record_id| record_id.as_str())
                .collect();
            let hydrated = hydrate_records(client, table_name, &id_refs, &registry);

            for record in hydrated {
                let is_deleted = record
                    .get("deleted_at")
                    .and_then(|value| value.as_str())
                    .is_some();
                if is_deleted {
                    continue;
                }

                let record_id = record["id"].as_str().unwrap_or_default().to_string();
                records.push(RebuildRecord {
                    table: table_name.clone(),
                    pk: vnext_pk_value(&registry, table_name, &record_id),
                    row: Some(record["data"].clone()),
                    server_version: record_server_version(&record, rebuild_checkpoint),
                });
            }
        }

        let next_cursor = if has_more {
            edges
                .last()
                .map(|(table_name, record_id)| format!("{table_name}|{record_id}"))
        } else {
            None
        };

        let (final_scope_cursor, checksum) = if has_more {
            (None, None)
        } else {
            complete_rebuild(
                client,
                p_user_id,
                &request.client_id,
                &request.scope,
                rebuild_checkpoint,
            );
            let checksum = compute_bucket_checksums(client, std::slice::from_ref(&request.scope))
                .get(&request.scope)
                .copied()
                .unwrap_or(0)
                .to_string();
            (Some(rebuild_checkpoint.to_string()), Some(checksum))
        };

        let response = RebuildResponse {
            scope: request.scope.clone(),
            records,
            cursor: next_cursor,
            has_more,
            final_scope_cursor,
            checksum,
        };
        if let Err(err) = response.validate() {
            pgrx::error!("invalid vNext rebuild response: {}", err);
        }

        pgrx::JsonB(serde_json::to_value(response).unwrap())
    })
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn parse_cursor(cursor: &str) -> Option<(String, String)> {
    if cursor.is_empty() {
        return None;
    }
    cursor
        .split_once('|')
        .map(|(t, id)| (t.to_string(), id.to_string()))
}

fn query_edges(
    client: &SpiClient<'_>,
    bucket_id: &str,
    cursor: Option<&(String, String)>,
    limit: i32,
) -> Vec<(String, String)> {
    let tup_table = if let Some((cursor_table, cursor_id)) = cursor {
        client.select(
            "SELECT table_name, record_id FROM sync_bucket_edges \
             WHERE bucket_id = $1 AND (table_name, record_id) > ($2, $3) \
             ORDER BY table_name, record_id LIMIT $4",
            None,
            &[
                bucket_id.into(),
                cursor_table.as_str().into(),
                cursor_id.as_str().into(),
                limit.into(),
            ],
        )
    } else {
        client.select(
            "SELECT table_name, record_id FROM sync_bucket_edges \
             WHERE bucket_id = $1 \
             ORDER BY table_name, record_id LIMIT $2",
            None,
            &[bucket_id.into(), limit.into()],
        )
    };

    let tup_table = match tup_table {
        Ok(t) => t,
        Err(e) => pgrx::error!("querying bucket edges: {}", e),
    };

    let mut edges = Vec::new();
    for row in tup_table {
        let table: String = match row.get_by_name::<String, &str>("table_name") {
            Ok(Some(v)) if !v.is_empty() => v,
            _ => continue,
        };
        let record: String = match row.get_by_name::<String, &str>("record_id") {
            Ok(Some(v)) if !v.is_empty() => v,
            _ => continue,
        };
        edges.push((table, record));
    }
    edges
}

/// Complete a rebuild: set bucket checkpoint (unconditional), advance legacy
/// checkpoint, sync per-bucket checkpoints, reactivate client.
fn complete_rebuild(
    client: &mut SpiClient<'_>,
    user_id: &str,
    client_id: &str,
    bucket_id: &str,
    checkpoint: i64,
) {
    // Set the bucket checkpoint unconditionally.
    let _ = client.update(
        "INSERT INTO sync_client_checkpoints \
         (user_id, client_id, bucket_id, checkpoint) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (user_id, client_id, bucket_id) DO UPDATE \
         SET checkpoint = EXCLUDED.checkpoint, updated_at = now()",
        None,
        &[
            user_id.into(),
            client_id.into(),
            bucket_id.into(),
            checkpoint.into(),
        ],
    );

    // Advance legacy checkpoint (monotonic).
    let _ = client.update(
        "UPDATE sync_clients \
         SET last_pull_seq = $3, last_pull_at = now(), last_sync_at = now() \
         WHERE user_id = $1 AND client_id = $2 \
         AND (last_pull_seq IS NULL OR last_pull_seq < $3)",
        None,
        &[user_id.into(), client_id.into(), checkpoint.into()],
    );

    // Sync all per-bucket checkpoints behind this value.
    let _ = client.update(
        "UPDATE sync_client_checkpoints \
         SET checkpoint = $3, updated_at = now() \
         WHERE user_id = $1 AND client_id = $2 AND checkpoint < $3",
        None,
        &[user_id.into(), client_id.into(), checkpoint.into()],
    );
}
