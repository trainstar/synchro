use pgrx::prelude::*;
use pgrx::spi::SpiClient;
use synchro_core::protocol::clamp_rebuild_limit;

use crate::client::validate_schema;
use crate::pull::{
    compute_bucket_checksums, get_latest_schema, hydrate_records, load_client_buckets,
};
use crate::registry::load_registry;

/// Paginated bucket rebuild.
///
/// Returns all current records in a bucket via sync_bucket_edges,
/// using keyset (cursor) pagination. Each page returns hydrated records
/// with a cursor for the next page. On the final page, includes the
/// bucket checksum and a checkpoint (MAX seq) for the client to adopt.
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
        validate_schema(p_schema_version, p_schema_hash);
    }

    // Validate client exists.
    let _ = load_client_buckets(p_user_id, p_client_id);

    let limit = clamp_rebuild_limit(p_limit);

    let registry = match load_registry() {
        Ok(r) => r,
        Err(e) => pgrx::error!("failed to load registry: {}", e),
    };

    let (schema_version, schema_hash) = get_latest_schema();

    Spi::connect_mut(|client| {
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
                cp = row.get_by_name("cp").unwrap_or(None).unwrap_or(0);
            }
            cp
        };

        // Parse cursor: "table_name|record_id" or empty.
        let (cursor_table, cursor_id) = parse_cursor(p_cursor);

        // Query bucket edges with keyset pagination.
        let edges = if let (Some(ct), Some(ci)) = (&cursor_table, &cursor_id) {
            query_edges_with_cursor(client, p_bucket_id, ct, ci, limit + 1)
        } else {
            query_edges(client, p_bucket_id, limit + 1)
        };

        // Detect has_more.
        let has_more = edges.len() > limit as usize;
        let edges: Vec<(String, String)> = if has_more {
            edges[..limit as usize].to_vec()
        } else {
            edges
        };

        if edges.is_empty() {
            // Final page (or empty bucket). Set checkpoint.
            set_rebuild_checkpoint(client, p_user_id, p_client_id, p_bucket_id, rebuild_checkpoint);

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
                // Filter out soft-deleted records (rebuild is snapshot mode).
                let is_deleted = record
                    .get("deleted_at")
                    .and_then(|v| v.as_str())
                    .is_some();
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
            set_rebuild_checkpoint(
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

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn parse_cursor(cursor: &str) -> (Option<String>, Option<String>) {
    if cursor.is_empty() {
        return (None, None);
    }
    match cursor.split_once('|') {
        Some((table, id)) => (Some(table.to_string()), Some(id.to_string())),
        None => (None, None),
    }
}

fn query_edges(
    client: &SpiClient<'_>,
    bucket_id: &str,
    limit: i32,
) -> Vec<(String, String)> {
    let tup_table = match client.select(
        "SELECT table_name, record_id FROM sync_bucket_edges \
         WHERE bucket_id = $1 \
         ORDER BY table_name, record_id \
         LIMIT $2",
        None,
        &[bucket_id.into(), limit.into()],
    ) {
        Ok(t) => t,
        Err(e) => pgrx::error!("querying bucket edges: {}", e),
    };

    let mut edges = Vec::new();
    for row in tup_table {
        let table: String = row
            .get_by_name::<String, &str>("table_name")
            .unwrap_or(None)
            .unwrap_or_default();
        let record: String = row
            .get_by_name::<String, &str>("record_id")
            .unwrap_or(None)
            .unwrap_or_default();
        edges.push((table, record));
    }
    edges
}

fn query_edges_with_cursor(
    client: &SpiClient<'_>,
    bucket_id: &str,
    cursor_table: &str,
    cursor_id: &str,
    limit: i32,
) -> Vec<(String, String)> {
    let tup_table = match client.select(
        "SELECT table_name, record_id FROM sync_bucket_edges \
         WHERE bucket_id = $1 \
         AND (table_name, record_id) > ($2, $3) \
         ORDER BY table_name, record_id \
         LIMIT $4",
        None,
        &[
            bucket_id.into(),
            cursor_table.into(),
            cursor_id.into(),
            limit.into(),
        ],
    ) {
        Ok(t) => t,
        Err(e) => pgrx::error!("querying bucket edges with cursor: {}", e),
    };

    let mut edges = Vec::new();
    for row in tup_table {
        let table: String = row
            .get_by_name::<String, &str>("table_name")
            .unwrap_or(None)
            .unwrap_or_default();
        let record: String = row
            .get_by_name::<String, &str>("record_id")
            .unwrap_or(None)
            .unwrap_or_default();
        edges.push((table, record));
    }
    edges
}

/// Set the client's checkpoint for a bucket after rebuild completes.
/// Uses unconditional upsert (no WHERE checkpoint < constraint).
fn set_rebuild_checkpoint(
    client: &mut SpiClient<'_>,
    user_id: &str,
    client_id: &str,
    bucket_id: &str,
    checkpoint: i64,
) {
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
}
