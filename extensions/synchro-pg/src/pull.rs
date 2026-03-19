use pgrx::prelude::*;
use pgrx::spi::SpiClient;
use synchro_core::checksum::compute_record_checksum;
use synchro_core::dedup::{deduplicate_entries, ChangelogEntry as DedupEntry};
use synchro_core::protocol::{clamp_pull_limit, Operation};

use crate::client::validate_schema;
use crate::registry::TableRegistration;

/// Pull changes for a client.
///
/// Returns JSONB matching Go PullResponse: changes, deletes, checkpoint,
/// bucket_checkpoints, has_more, bucket_checksums, schema_version, schema_hash.
///
/// Supports two modes:
///   - Legacy (single checkpoint): when p_bucket_checkpoints is NULL
///   - Per-bucket checkpoints: when p_bucket_checkpoints is provided
#[pg_extern]
fn synchro_pull(
    p_user_id: &str,
    p_client_id: &str,
    p_checkpoint: default!(i64, "0"),
    p_bucket_checkpoints: default!(Option<pgrx::JsonB>, "NULL"),
    p_limit: default!(i32, "100"),
    p_tables: default!(Option<Vec<String>>, "NULL"),
    _p_known_buckets: default!(Option<Vec<String>>, "NULL"),
    p_schema_version: default!(i64, "0"),
    p_schema_hash: default!(&str, "''"),
) -> pgrx::JsonB {
    // Validate schema if provided.
    if p_schema_version > 0 || !p_schema_hash.is_empty() {
        validate_schema(p_schema_version, p_schema_hash);
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
        let bucket_subs = load_client_buckets(client, p_user_id, p_client_id);

        // Get schema info.
        let (schema_version, schema_hash) = get_latest_schema(client);

        // Detect stale buckets.
        let stale_buckets = detect_stale_buckets(client, &bucket_subs, &bucket_cps);

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
        let raw_entries = query_changelog(
            client,
            &bucket_subs,
            effective_cp,
            limit + 1,
            &tables_param,
        );

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
            let checkpoint_val = if per_bucket { effective_cp } else { p_checkpoint };
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
            if r.operation == Operation::Delete {
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

        pgrx::JsonB(response)
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
    operation: Operation,
}

pub(crate) fn load_client_buckets(
    client: &SpiClient<'_>,
    user_id: &str,
    client_id: &str,
) -> Vec<String> {
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
            return s;
        }
    }
    pgrx::error!("client not found or inactive: {}/{}", user_id, client_id);
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

    for row in tup_table {
        let v: i64 = row
            .get_by_name::<i64, &str>("schema_version")
            .unwrap_or(None)
            .unwrap_or(0);
        let h: String = row
            .get_by_name::<String, &str>("schema_hash")
            .unwrap_or(None)
            .unwrap_or_default();
        return (v, h);
    }
    (0, String::new())
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

/// Load registry within an existing SPI context.
fn load_registry_inner(client: &SpiClient<'_>) -> Vec<TableRegistration> {
    let query = "SELECT table_name, bucket_sql, pk_column, updated_at_col,
                        deleted_at_col, push_policy, exclude_columns,
                        has_updated_at, has_deleted_at
                 FROM sync_registry
                 ORDER BY table_name";
    let tup_table = match client.select(query, None, &[]) {
        Ok(t) => t,
        Err(e) => pgrx::error!("loading registry: {}", e),
    };

    let mut tables = Vec::new();
    for row in tup_table {
        let table_name: String = row.get_by_name("table_name").unwrap_or(None).unwrap_or_default();
        let bucket_sql: String = row.get_by_name("bucket_sql").unwrap_or(None).unwrap_or_default();
        let pk_column: String = row.get_by_name("pk_column").unwrap_or(None).unwrap_or_default();
        let updated_at_col: String = row
            .get_by_name("updated_at_col")
            .unwrap_or(None)
            .unwrap_or_default();
        let deleted_at_col: String = row
            .get_by_name("deleted_at_col")
            .unwrap_or(None)
            .unwrap_or_default();
        let push_policy_str: String = row
            .get_by_name("push_policy")
            .unwrap_or(None)
            .unwrap_or_default();
        let exclude_columns: Vec<String> = row
            .get_by_name("exclude_columns")
            .unwrap_or(None)
            .unwrap_or_default();
        let has_updated_at: bool = row.get_by_name("has_updated_at").unwrap_or(None).unwrap_or(false);
        let has_deleted_at: bool = row.get_by_name("has_deleted_at").unwrap_or(None).unwrap_or(false);

        tables.push(TableRegistration {
            table_name,
            bucket_sql,
            pk_column,
            updated_at_col,
            deleted_at_col,
            push_policy: crate::registry::PushPolicy::parse(&push_policy_str)
                .unwrap_or(crate::registry::PushPolicy::Enabled),
            exclude_columns,
            has_updated_at,
            has_deleted_at,
        });
    }
    tables
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
        let operation = match Operation::from_i16(op_i16) {
            Some(op) => op,
            None => {
                log!("unknown operation {} in changelog seq {}, skipping", op_i16, seq);
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
        None => pgrx::error!("table {:?} not found in registry during hydration", table_name),
    };

    if ids.is_empty() {
        return vec![];
    }

    // Build exclude expression for JSONB subtraction.
    let exclude_expr = if table_reg.exclude_columns.is_empty() {
        String::new()
    } else {
        table_reg
            .exclude_columns
            .iter()
            .map(|c| format!(" - '{}'", c.replace('\'', "''")))
            .collect::<String>()
    };

    // Build timestamp expressions.
    let updated_at_expr = if table_reg.has_updated_at {
        format!(
            "t.{}::timestamptz::text",
            pg_quote_ident(&table_reg.updated_at_col)
        )
    } else {
        "NULL::text".into()
    };

    let deleted_at_expr = if table_reg.has_deleted_at {
        format!(
            "t.{}::timestamptz::text",
            pg_quote_ident(&table_reg.deleted_at_col)
        )
    } else {
        "NULL::text".into()
    };

    // Build hydration query.
    let query = format!(
        "SELECT {pk}::text AS id, \
         (to_jsonb(t){exclude})::text AS data, \
         {updated_at} AS updated_at, \
         {deleted_at} AS deleted_at \
         FROM {table} t \
         WHERE {pk}::text = ANY($1)",
        pk = pg_quote_ident(&table_reg.pk_column),
        exclude = exclude_expr,
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
        let data: serde_json::Value =
            serde_json::from_str(&data_str).unwrap_or(serde_json::Value::Null);

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
