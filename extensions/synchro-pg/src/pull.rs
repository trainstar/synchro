use pgrx::prelude::*;
use pgrx::spi::SpiClient;
use synchro_core::change::ChangeOperation;
use synchro_core::checksum::compute_record_checksum;
use synchro_core::contract::{
    ChangeRecord, ChecksumMode, ProtocolErrorCode, PullRequest, PullResponse, ScopeCursorRef,
    VNextOperation,
};
use synchro_core::dedup::{
    deduplicate_entries, ChangelogEntry as DedupEntry, RecordRef as DedupRecordRef,
};
use synchro_core::limits::clamp_pull_limit;

use crate::client::{
    build_scope_delta, protocol_error_response, validate_schema, validate_schema_vnext,
};
use crate::registry::TableRegistration;

/// Pull changes for a client.
///
/// Returns JSONB matching Go PullResponse: changes, deletes, checkpoint,
/// bucket_checkpoints, has_more, bucket_checksums, schema_version, schema_hash.
///
/// Supports two modes:
///   - Legacy (single checkpoint): when p_bucket_checkpoints is NULL
///   - Per-bucket checkpoints: when p_bucket_checkpoints is provided
#[allow(clippy::too_many_arguments)]
#[pg_extern]
fn synchro_pull(
    p_user_id: &str,
    p_client_id: &str,
    p_checkpoint: default!(i64, "0"),
    p_bucket_checkpoints: default!(Option<pgrx::JsonB>, "NULL"),
    p_limit: default!(i32, "100"),
    p_tables: default!(Option<Vec<String>>, "NULL"),
    p_known_buckets: default!(Option<Vec<String>>, "NULL"),
    p_schema_version: default!(i64, "0"),
    p_schema_hash: default!(&str, "''"),
) -> pgrx::JsonB {
    // Validate schema if provided.
    if p_schema_version > 0 || !p_schema_hash.is_empty() {
        if let Err(err_json) = validate_schema(p_schema_version, p_schema_hash) {
            return err_json;
        }
    }

    let limit = clamp_pull_limit(p_limit);

    // Determine pull mode.
    let per_bucket = p_bucket_checkpoints.is_some();
    let bucket_cps: std::collections::HashMap<String, i64> = match &p_bucket_checkpoints {
        Some(jsonb) => serde_json::from_value(jsonb.0.clone())
            .unwrap_or_else(|e| pgrx::error!("invalid bucket_checkpoints JSON: {}", e)),
        None => std::collections::HashMap::new(),
    };

    // All reads and writes in a single SPI transaction to avoid TOCTOU races.
    Spi::connect_mut(|client| {
        // Set RLS context.
        let _ = client.update(
            "SELECT set_config('app.user_id', $1, true)",
            None,
            &[p_user_id.into()],
        );

        // Load client bucket subscriptions.
        let bucket_subs = match load_client_buckets(client, p_user_id, p_client_id) {
            Ok(subs) => subs,
            Err(err_json) => return err_json,
        };

        // Compare known_buckets against server bucket subscriptions.
        let bucket_updates: Option<serde_json::Value> =
            p_known_buckets.as_ref().and_then(|known| {
                let known_set: std::collections::HashSet<&str> =
                    known.iter().map(|s| s.as_str()).collect();
                let server_set: std::collections::HashSet<&str> =
                    bucket_subs.iter().map(|s| s.as_str()).collect();

                let added: Vec<&str> = server_set.difference(&known_set).copied().collect();
                let removed: Vec<&str> = known_set.difference(&server_set).copied().collect();

                if !added.is_empty() || !removed.is_empty() {
                    Some(serde_json::json!({
                        "added": added,
                        "removed": removed,
                    }))
                } else {
                    None
                }
            });

        // Get schema info.
        let (schema_version, schema_hash) = get_latest_schema(client);

        // Detect stale buckets (only in per-bucket checkpoint mode).
        // In legacy mode bucket_cps is empty, so stale detection is not applicable.
        let stale_buckets = if per_bucket {
            detect_stale_buckets(client, &bucket_subs, &bucket_cps)
        } else {
            vec![]
        };

        // If stale buckets detected, return immediately with rebuild instruction.
        // Do NOT query changelog or advance checkpoints (Go behavior).
        if !stale_buckets.is_empty() {
            let mut resp = serde_json::json!({
                "changes": [],
                "deletes": [],
                "checkpoint": p_checkpoint,
                "has_more": false,
                "rebuild_buckets": stale_buckets,
                "schema_version": schema_version,
                "schema_hash": schema_hash,
            });
            if per_bucket {
                resp["bucket_checkpoints"] = serde_json::json!(bucket_cps);
            }
            if let Some(ref updates) = bucket_updates {
                resp["bucket_updates"] = updates.clone();
            }
            return pgrx::JsonB(resp);
        }

        // Load registry for hydration.
        let registry = load_registry_inner(client);

        // Determine effective checkpoint.
        let effective_cp = if per_bucket {
            bucket_subs
                .iter()
                .map(|b| bucket_cps.get(b).copied().unwrap_or(0))
                .min()
                .unwrap_or(0)
        } else {
            p_checkpoint
        };

        // Query changelog entries.
        let tables_param: Option<Vec<String>> = p_tables;
        let raw_entries =
            query_changelog(client, &bucket_subs, effective_cp, limit + 1, &tables_param);

        // Filter per-bucket (only entries beyond each bucket's checkpoint).
        let filtered: Vec<RawChangelogEntry> = if per_bucket {
            raw_entries
                .into_iter()
                .filter(|e| {
                    let bcp = bucket_cps.get(&e.bucket_id).copied().unwrap_or(0);
                    e.seq > bcp
                })
                .collect()
        } else {
            raw_entries
        };

        // Detect has_more.
        let has_more = filtered.len() > limit as usize;
        let entries: Vec<RawChangelogEntry> = if has_more {
            filtered[..limit as usize].to_vec()
        } else {
            filtered
        };

        if entries.is_empty() {
            let checkpoint_val = if per_bucket {
                effective_cp
            } else {
                p_checkpoint
            };
            let mut resp = serde_json::json!({
                "changes": [],
                "deletes": [],
                "checkpoint": checkpoint_val,
                "has_more": false,
                "schema_version": schema_version,
                "schema_hash": schema_hash,
            });
            if per_bucket {
                resp["bucket_checkpoints"] = serde_json::json!(bucket_cps);
            }
            resp["bucket_checksums"] =
                serde_json::json!(compute_bucket_checksums(client, &bucket_subs));
            if let Some(ref updates) = bucket_updates {
                resp["bucket_updates"] = updates.clone();
            }
            return pgrx::JsonB(resp);
        }

        // Deduplicate: keep latest entry per (table_name, record_id).
        let dedup_input: Vec<DedupEntry> = entries
            .iter()
            .map(|e| DedupEntry {
                seq: e.seq,
                bucket_id: e.bucket_id.clone(),
                table_name: e.table_name.clone(),
                record_id: e.record_id.clone(),
                operation: e.operation,
            })
            .collect();
        let deduped = deduplicate_entries(&dedup_input);

        // Separate deletes from changes, group changes by table.
        let mut deletes: Vec<serde_json::Value> = Vec::new();
        let mut changes_by_table: std::collections::HashMap<String, Vec<(String, String)>> =
            std::collections::HashMap::new();

        for r in &deduped {
            if r.operation == ChangeOperation::Delete {
                deletes.push(serde_json::json!({
                    "id": r.record_id,
                    "table_name": r.table_name,
                }));
            } else {
                changes_by_table
                    .entry(r.table_name.clone())
                    .or_default()
                    .push((r.record_id.clone(), r.bucket_id.clone()));
            }
        }

        // Hydrate records per table.
        let mut changes: Vec<serde_json::Value> = Vec::new();
        for (table_name, id_bucket_pairs) in &changes_by_table {
            let ids: Vec<&str> = id_bucket_pairs.iter().map(|(id, _)| id.as_str()).collect();
            let bucket_map: std::collections::HashMap<&str, &str> = id_bucket_pairs
                .iter()
                .map(|(id, b)| (id.as_str(), b.as_str()))
                .collect();

            let hydrated = hydrate_records(client, table_name, &ids, &registry);
            for mut record in hydrated {
                if let Some(id) = record.get("id").and_then(|v| v.as_str()).map(String::from) {
                    if let Some(bid) = bucket_map.get(id.as_str()) {
                        record
                            .as_object_mut()
                            .unwrap()
                            .insert("bucket_id".into(), serde_json::json!(bid));
                    }
                }
                changes.push(record);
            }
        }

        // Compute new checkpoint.
        let new_checkpoint = entries.last().map(|e| e.seq).unwrap_or(p_checkpoint);

        // Compute new per-bucket checkpoints.
        let mut new_bucket_cps = bucket_cps.clone();
        if per_bucket {
            for e in &entries {
                let current = new_bucket_cps.get(&e.bucket_id).copied().unwrap_or(0);
                if e.seq > current {
                    new_bucket_cps.insert(e.bucket_id.clone(), e.seq);
                }
            }
        }

        // Compute bucket checksums (final page only).
        let bucket_checksums = if !has_more {
            Some(compute_bucket_checksums(client, &bucket_subs))
        } else {
            None
        };

        // Advance checkpoints.
        advance_checkpoints(
            client,
            p_user_id,
            p_client_id,
            new_checkpoint,
            if per_bucket {
                Some(&new_bucket_cps)
            } else {
                None
            },
        );

        // Build response.
        let mut response = serde_json::json!({
            "changes": changes,
            "deletes": deletes,
            "checkpoint": new_checkpoint,
            "has_more": has_more,
            "schema_version": schema_version,
            "schema_hash": schema_hash,
        });

        if per_bucket {
            response["bucket_checkpoints"] = serde_json::json!(new_bucket_cps);
        }
        if let Some(checksums) = bucket_checksums {
            response["bucket_checksums"] = serde_json::json!(checksums);
        }
        if let Some(ref updates) = bucket_updates {
            response["bucket_updates"] = updates.clone();
        }

        pgrx::JsonB(response)
    })
}

/// Pull vNext scoped changes for a client using per-scope cursors.
#[pg_extern]
fn synchro_pull_vnext(p_user_id: &str, p_request: pgrx::JsonB) -> pgrx::JsonB {
    let request: PullRequest = match serde_json::from_value(p_request.0) {
        Ok(request) => request,
        Err(err) => {
            return protocol_error_response(
                ProtocolErrorCode::InvalidRequest,
                format!("invalid pull request: {err}"),
                false,
            );
        }
    };

    if let Err(err_json) = validate_schema_vnext(&request.schema) {
        return err_json;
    }

    let limit = clamp_pull_limit(request.limit);

    Spi::connect_mut(|client| {
        let _ = client.update(
            "SELECT set_config('app.user_id', $1, true)",
            None,
            &[p_user_id.into()],
        );

        let client_state =
            match crate::client::load_client_connect_state(client, p_user_id, &request.client_id) {
                Ok(state) => state,
                Err(err_json) => return err_json,
            };

        let server_scopes = client_state.bucket_subs;
        let scope_set_version = client_state.scope_set_version;
        let scope_updates = build_scope_delta(&request.scopes, &server_scopes);
        let active_scopes: Vec<String> = server_scopes
            .iter()
            .filter(|scope_id: &&String| request.scopes.contains_key(scope_id.as_str()))
            .cloned()
            .collect();
        let scope_cps = match parse_scope_checkpoints(&request.scopes, &active_scopes) {
            Ok(cursors) => cursors,
            Err(err_json) => return err_json,
        };

        let stale_scopes = detect_stale_buckets(client, &active_scopes, &scope_cps);
        if !stale_scopes.is_empty() {
            let response = PullResponse {
                changes: Vec::new(),
                scope_set_version,
                scope_cursors: std::collections::BTreeMap::new(),
                scope_updates,
                rebuild: stale_scopes,
                has_more: false,
                checksums: maybe_scope_checksums(
                    client,
                    &active_scopes,
                    request.checksum_mode,
                    false,
                ),
            };

            if let Err(err) = response.validate_for_request(&request) {
                pgrx::error!("invalid vNext pull response: {}", err);
            }

            return pgrx::JsonB(serde_json::to_value(response).unwrap());
        }

        if active_scopes.is_empty() {
            let response = PullResponse {
                changes: Vec::new(),
                scope_set_version,
                scope_cursors: std::collections::BTreeMap::new(),
                scope_updates,
                rebuild: Vec::new(),
                has_more: false,
                checksums: maybe_scope_checksums(
                    client,
                    &active_scopes,
                    request.checksum_mode,
                    false,
                ),
            };

            if let Err(err) = response.validate_for_request(&request) {
                pgrx::error!("invalid vNext pull response: {}", err);
            }

            return pgrx::JsonB(serde_json::to_value(response).unwrap());
        }

        let effective_cp = active_scopes
            .iter()
            .map(|scope_id| scope_cps.get(scope_id).copied().unwrap_or(0))
            .min()
            .unwrap_or(0);
        let raw_entries = query_changelog(client, &active_scopes, effective_cp, limit + 1, &None);
        let filtered: Vec<RawChangelogEntry> = raw_entries
            .into_iter()
            .filter(|entry| {
                let scope_cp = scope_cps.get(&entry.bucket_id).copied().unwrap_or(0);
                entry.seq > scope_cp
            })
            .collect();

        let has_more = filtered.len() > limit as usize;
        let entries: Vec<RawChangelogEntry> = if has_more {
            filtered[..limit as usize].to_vec()
        } else {
            filtered
        };

        let registry = load_registry_inner(client);
        let dedup_input: Vec<DedupEntry> = entries
            .iter()
            .map(|entry| DedupEntry {
                seq: entry.seq,
                bucket_id: entry.bucket_id.clone(),
                table_name: entry.table_name.clone(),
                record_id: entry.record_id.clone(),
                operation: entry.operation,
            })
            .collect();
        let deduped = deduplicate_entries(&dedup_input);
        let hydrated = hydrate_vnext_records(client, &registry, &deduped);

        let mut changes = Vec::new();
        let mut scope_cursors = std::collections::BTreeMap::new();
        for entry in &deduped {
            scope_cursors.insert(entry.bucket_id.clone(), entry.seq.to_string());
            let pk = vnext_pk_value(&registry, &entry.table_name, &entry.record_id);
            let hydrated_record =
                hydrated.get(&(entry.table_name.clone(), entry.record_id.clone()));
            match entry.operation {
                ChangeOperation::Delete => changes.push(ChangeRecord {
                    scope: entry.bucket_id.clone(),
                    table: entry.table_name.clone(),
                    op: VNextOperation::Delete,
                    pk,
                    row: hydrated_record.map(|record| record["data"].clone()),
                    server_version: hydrated_record
                        .map(|record| record_server_version(record, entry.seq))
                        .unwrap_or_else(|| entry.seq.to_string()),
                }),
                ChangeOperation::Insert | ChangeOperation::Update => {
                    if let Some(record) = hydrated_record {
                        changes.push(ChangeRecord {
                            scope: entry.bucket_id.clone(),
                            table: entry.table_name.clone(),
                            op: VNextOperation::Upsert,
                            pk,
                            row: Some(record["data"].clone()),
                            server_version: record_server_version(record, entry.seq),
                        });
                    }
                }
            }
        }

        let response = PullResponse {
            changes,
            scope_set_version,
            scope_cursors,
            scope_updates,
            rebuild: Vec::new(),
            has_more,
            checksums: maybe_scope_checksums(
                client,
                &active_scopes,
                request.checksum_mode,
                has_more,
            ),
        };

        if let Err(err) = response.validate_for_request(&request) {
            pgrx::error!("invalid vNext pull response: {}", err);
        }

        pgrx::JsonB(serde_json::to_value(response).unwrap())
    })
}

// ---------------------------------------------------------------------------
// Internal helpers (pub(crate) for use by rebuild.rs)
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct RawChangelogEntry {
    seq: i64,
    bucket_id: String,
    table_name: String,
    record_id: String,
    operation: ChangeOperation,
}

pub(crate) fn load_client_buckets(
    client: &SpiClient<'_>,
    user_id: &str,
    client_id: &str,
) -> Result<Vec<String>, pgrx::JsonB> {
    let tup_table = client
        .select(
            "SELECT bucket_subs FROM sync_clients \
             WHERE user_id = $1 AND client_id = $2 AND is_active = true",
            None,
            &[user_id.into(), client_id.into()],
        )
        .unwrap_or_else(|e| pgrx::error!("querying client: {}", e));

    for row in tup_table {
        let subs: Option<Vec<String>> = row
            .get_by_name::<Vec<String>, &str>("bucket_subs")
            .unwrap_or(None);
        if let Some(s) = subs {
            return Ok(s);
        }
    }
    Err(pgrx::JsonB(
        serde_json::json!({"error": "client_not_found"}),
    ))
}

pub(crate) fn get_latest_schema(client: &SpiClient<'_>) -> (i64, String) {
    let tup_table = match client.select(
        "SELECT schema_version, schema_hash FROM sync_schema_manifest \
         ORDER BY schema_version DESC LIMIT 1",
        None,
        &[],
    ) {
        Ok(t) => t,
        Err(_) => return (0, String::new()),
    };

    if let Some(row) = tup_table.into_iter().next() {
        let v: i64 = row
            .get_by_name::<i64, &str>("schema_version")
            .unwrap_or(None)
            .unwrap_or(0);
        let h: String = row
            .get_by_name::<String, &str>("schema_hash")
            .unwrap_or(None)
            .unwrap_or_default();
        (v, h)
    } else {
        (0, String::new())
    }
}

fn detect_stale_buckets(
    client: &SpiClient<'_>,
    bucket_subs: &[String],
    bucket_cps: &std::collections::HashMap<String, i64>,
) -> Vec<String> {
    let tup_table = match client.select(
        "SELECT COALESCE(MIN(seq), 0) AS min_seq FROM sync_changelog",
        None,
        &[],
    ) {
        Ok(t) => t,
        Err(_) => return vec![],
    };

    let mut min_seq: i64 = 0;
    for row in tup_table {
        min_seq = row
            .get_by_name::<i64, &str>("min_seq")
            .unwrap_or(None)
            .unwrap_or(0);
    }

    if min_seq == 0 {
        return vec![];
    }

    let mut stale = Vec::new();
    for bid in bucket_subs {
        match bucket_cps.get(bid) {
            None => stale.push(bid.clone()),
            Some(&cp) if cp > 0 && cp < min_seq => stale.push(bid.clone()),
            _ => {}
        }
    }
    stale
}

fn parse_scope_checkpoints(
    scopes: &std::collections::BTreeMap<String, ScopeCursorRef>,
    active_scopes: &[String],
) -> Result<std::collections::HashMap<String, i64>, pgrx::JsonB> {
    let mut checkpoints = std::collections::HashMap::new();

    for scope_id in active_scopes {
        let cursor = scopes
            .get(scope_id)
            .and_then(|scope_ref| scope_ref.cursor.as_ref());
        let Some(cursor) = cursor else {
            continue;
        };

        let checkpoint = cursor.parse::<i64>().map_err(|_| {
            protocol_error_response(
                ProtocolErrorCode::InvalidRequest,
                format!("scope {scope_id} cursor must be a decimal sequence value"),
                false,
            )
        })?;
        checkpoints.insert(scope_id.clone(), checkpoint);
    }

    Ok(checkpoints)
}

fn maybe_scope_checksums(
    client: &SpiClient<'_>,
    scope_ids: &[String],
    checksum_mode: Option<ChecksumMode>,
    has_more: bool,
) -> Option<std::collections::BTreeMap<String, String>> {
    if has_more || matches!(checksum_mode.unwrap_or_default(), ChecksumMode::None) {
        return None;
    }

    let checksums = compute_bucket_checksums(client, scope_ids);
    let mut response = std::collections::BTreeMap::new();
    for scope_id in scope_ids {
        let checksum = checksums.get(scope_id).copied().unwrap_or(0);
        response.insert(scope_id.clone(), checksum.to_string());
    }

    Some(response)
}

/// Load registry within an existing SPI context.
fn load_registry_inner(client: &SpiClient<'_>) -> Vec<TableRegistration> {
    crate::registry::load_registry_from_client(client)
        .unwrap_or_else(|e| pgrx::error!("loading registry: {}", e))
}

fn query_changelog(
    client: &SpiClient<'_>,
    bucket_ids: &[String],
    after_seq: i64,
    limit: i32,
    tables: &Option<Vec<String>>,
) -> Vec<RawChangelogEntry> {
    let query = if tables.is_some() {
        "SELECT seq, bucket_id, table_name, record_id, operation \
         FROM sync_changelog \
         WHERE bucket_id = ANY($1) AND seq > $2 AND table_name = ANY($4) \
         ORDER BY seq LIMIT $3"
    } else {
        "SELECT seq, bucket_id, table_name, record_id, operation \
         FROM sync_changelog \
         WHERE bucket_id = ANY($1) AND seq > $2 \
         ORDER BY seq LIMIT $3"
    };

    let bucket_arr = bucket_ids.to_vec();
    let result = if let Some(table_filter) = tables {
        client.select(
            query,
            None,
            &[
                bucket_arr.into(),
                after_seq.into(),
                limit.into(),
                table_filter.clone().into(),
            ],
        )
    } else {
        client.select(
            query,
            None,
            &[bucket_arr.into(), after_seq.into(), limit.into()],
        )
    };

    let tup_table = match result {
        Ok(t) => t,
        Err(e) => pgrx::error!("querying changelog: {}", e),
    };

    let mut entries = Vec::new();
    for row in tup_table {
        let seq: i64 = row
            .get_by_name::<i64, &str>("seq")
            .unwrap_or(None)
            .unwrap_or(0);
        let bucket_id: String = match row.get_by_name::<String, &str>("bucket_id") {
            Ok(Some(v)) if !v.is_empty() => v,
            _ => continue, // skip entries with NULL/empty required fields
        };
        let table_name: String = match row.get_by_name::<String, &str>("table_name") {
            Ok(Some(v)) if !v.is_empty() => v,
            _ => continue,
        };
        let record_id: String = match row.get_by_name::<String, &str>("record_id") {
            Ok(Some(v)) if !v.is_empty() => v,
            _ => continue,
        };
        let op_i16: i16 = row
            .get_by_name::<i16, &str>("operation")
            .unwrap_or(None)
            .unwrap_or(0);
        let operation = match ChangeOperation::from_i16(op_i16) {
            Some(op) => op,
            None => {
                log!(
                    "unknown operation {} in changelog seq {}, skipping",
                    op_i16,
                    seq
                );
                continue;
            }
        };

        entries.push(RawChangelogEntry {
            seq,
            bucket_id,
            table_name,
            record_id,
            operation,
        });
    }
    entries
}

fn hydrate_vnext_records(
    client: &SpiClient<'_>,
    registry: &[TableRegistration],
    entries: &[DedupRecordRef],
) -> std::collections::HashMap<(String, String), serde_json::Value> {
    let mut changes_by_table: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for entry in entries {
        changes_by_table
            .entry(entry.table_name.clone())
            .or_default()
            .push(entry.record_id.clone());
    }

    let mut hydrated = std::collections::HashMap::new();
    for (table_name, ids) in changes_by_table {
        let id_refs: Vec<&str> = ids.iter().map(|id| id.as_str()).collect();
        for record in hydrate_records(client, &table_name, &id_refs, registry) {
            let record_id = record["id"].as_str().unwrap_or_default().to_string();
            hydrated.insert((table_name.clone(), record_id), record);
        }
    }

    hydrated
}

/// Hydrate records from a single table.
///
/// Returns JSON objects with: id, table_name, data, updated_at, deleted_at,
/// checksum. Exclude columns are stripped via JSONB subtraction in SQL.
pub(crate) fn hydrate_records(
    client: &SpiClient<'_>,
    table_name: &str,
    ids: &[&str],
    registry: &[TableRegistration],
) -> Vec<serde_json::Value> {
    let table_reg = match registry.iter().find(|t| t.table_name == table_name) {
        Some(t) => t,
        None => pgrx::error!(
            "table {:?} not found in registry during hydration",
            table_name
        ),
    };

    if ids.is_empty() {
        return vec![];
    }

    // Build timestamp expressions.
    let updated_at_expr = if table_reg.has_updated_at {
        format!(
            "to_char(timezone('UTC', t.{}), 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')",
            pg_quote_ident(&table_reg.updated_at_col)
        )
    } else {
        "NULL::text".into()
    };

    let deleted_at_expr = if table_reg.has_deleted_at {
        format!(
            "to_char(timezone('UTC', t.{}), 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')",
            pg_quote_ident(&table_reg.deleted_at_col)
        )
    } else {
        "NULL::text".into()
    };

    // Build hydration query.
    let query = format!(
        "SELECT {pk}::text AS id, \
         ({projection})::text AS data, \
         {updated_at} AS updated_at, \
         {deleted_at} AS deleted_at \
         FROM {table} t \
         WHERE {pk}::text = ANY($1)",
        pk = pg_quote_ident(&table_reg.pk_column),
        projection = synced_row_projection_sql(table_reg, "t"),
        updated_at = updated_at_expr,
        deleted_at = deleted_at_expr,
        table = pg_quote_ident(table_name),
    );

    let id_arr: Vec<String> = ids.iter().map(|s| s.to_string()).collect();
    let tup_table = match client.select(&query, None, &[id_arr.into()]) {
        Ok(t) => t,
        Err(e) => pgrx::error!("hydration failed for table {}: {}", table_name, e),
    };

    let mut records = Vec::new();
    for row in tup_table {
        let id: String = row
            .get_by_name::<String, &str>("id")
            .unwrap_or(None)
            .unwrap_or_default();
        let data_str: String = row
            .get_by_name::<String, &str>("data")
            .unwrap_or(None)
            .unwrap_or_default();
        let updated_at: Option<String> = row
            .get_by_name::<String, &str>("updated_at")
            .unwrap_or(None);
        let deleted_at: Option<String> = row
            .get_by_name::<String, &str>("deleted_at")
            .unwrap_or(None);

        let checksum = compute_record_checksum(&data_str) as i32;
        let mut data: serde_json::Value =
            serde_json::from_str(&data_str).unwrap_or(serde_json::Value::Null);

        if let Some(obj) = data.as_object_mut() {
            if table_reg.has_updated_at {
                if let Some(updated_at) = obj
                    .get(&table_reg.updated_at_col)
                    .and_then(|value| value.as_str())
                {
                    obj.insert(
                        table_reg.updated_at_col.clone(),
                        serde_json::json!(crate::push::canonical_timestamp(updated_at)),
                    );
                }
            }
            if table_reg.has_deleted_at {
                if let Some(deleted_at) = obj
                    .get(&table_reg.deleted_at_col)
                    .and_then(|value| value.as_str())
                {
                    obj.insert(
                        table_reg.deleted_at_col.clone(),
                        serde_json::json!(crate::push::canonical_timestamp(deleted_at)),
                    );
                }
            }
        }

        let mut record = serde_json::json!({
            "id": id,
            "table_name": table_name,
            "data": data,
            "checksum": checksum,
        });

        if let Some(ua) = updated_at {
            record["updated_at"] = serde_json::json!(ua);
        }
        if let Some(da) = deleted_at {
            record["deleted_at"] = serde_json::json!(da);
        }

        records.push(record);
    }
    records
}

pub(crate) fn synced_row_projection_sql(table_reg: &TableRegistration, row_alias: &str) -> String {
    let qualified = |column: &str| format!("{row_alias}.{}", pg_quote_ident(column));
    let pairs = table_reg
        .sync_columns
        .iter()
        .map(|column| format!("'{}', {}", column.replace('\'', "''"), qualified(column)))
        .collect::<Vec<_>>()
        .join(", ");

    format!("jsonb_build_object({pairs})")
}

pub(crate) fn compute_bucket_checksums(
    client: &SpiClient<'_>,
    bucket_ids: &[String],
) -> std::collections::HashMap<String, i32> {
    let bucket_arr = bucket_ids.to_vec();
    let tup_table = match client.select(
        "SELECT bucket_id, BIT_XOR(checksum) AS checksum \
         FROM sync_bucket_edges \
         WHERE bucket_id = ANY($1) AND checksum IS NOT NULL \
         GROUP BY bucket_id",
        None,
        &[bucket_arr.into()],
    ) {
        Ok(t) => t,
        Err(e) => {
            log!("computing bucket checksums: {}", e);
            return std::collections::HashMap::new();
        }
    };

    let mut checksums = std::collections::HashMap::new();
    for row in tup_table {
        let bid: String = row
            .get_by_name::<String, &str>("bucket_id")
            .unwrap_or(None)
            .unwrap_or_default();
        let cs: i32 = row
            .get_by_name::<i32, &str>("checksum")
            .unwrap_or(None)
            .unwrap_or(0);
        checksums.insert(bid, cs);
    }
    checksums
}

pub(crate) fn vnext_pk_value(
    registry: &[TableRegistration],
    table_name: &str,
    record_id: &str,
) -> serde_json::Value {
    let pk_column = registry
        .iter()
        .find(|table| table.table_name == table_name)
        .map(|table| table.pk_column.clone())
        .unwrap_or_else(|| "id".to_string());
    serde_json::json!({ pk_column: record_id })
}

pub(crate) fn record_server_version(record: &serde_json::Value, fallback_seq: i64) -> String {
    record
        .get("deleted_at")
        .and_then(|value| value.as_str())
        .map(|value| value.to_string())
        .or_else(|| {
            record
                .get("updated_at")
                .and_then(|value| value.as_str())
                .map(|value| value.to_string())
        })
        .unwrap_or_else(|| fallback_seq.to_string())
}

fn advance_checkpoints(
    client: &mut SpiClient<'_>,
    user_id: &str,
    client_id: &str,
    checkpoint: i64,
    bucket_checkpoints: Option<&std::collections::HashMap<String, i64>>,
) {
    // Advance legacy checkpoint (monotonic).
    let _ = client.update(
        "UPDATE sync_clients \
         SET last_pull_seq = $3, last_pull_at = now(), last_sync_at = now() \
         WHERE user_id = $1 AND client_id = $2 \
         AND (last_pull_seq IS NULL OR last_pull_seq < $3)",
        None,
        &[user_id.into(), client_id.into(), checkpoint.into()],
    );

    // Sync all existing per-bucket checkpoints that are behind the legacy CP.
    // Matches Go AdvanceCheckpoint which propagates to sync_client_checkpoints.
    let _ = client.update(
        "UPDATE sync_client_checkpoints \
         SET checkpoint = $3, updated_at = now() \
         WHERE user_id = $1 AND client_id = $2 AND checkpoint < $3",
        None,
        &[user_id.into(), client_id.into(), checkpoint.into()],
    );

    // Advance individual per-bucket checkpoints.
    if let Some(cps) = bucket_checkpoints {
        for (bucket_id, &seq) in cps {
            let _ = client.update(
                "INSERT INTO sync_client_checkpoints \
                 (user_id, client_id, bucket_id, checkpoint) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (user_id, client_id, bucket_id) DO UPDATE \
                 SET checkpoint = EXCLUDED.checkpoint, updated_at = now() \
                 WHERE sync_client_checkpoints.checkpoint < EXCLUDED.checkpoint",
                None,
                &[
                    user_id.into(),
                    client_id.into(),
                    bucket_id.as_str().into(),
                    seq.into(),
                ],
            );
        }
    }
}

/// Double-quote a SQL identifier, escaping internal double quotes.
pub(crate) fn pg_quote_ident(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}
