use pgrx::prelude::*;
use pgrx::spi::SpiClient;
use serde::Serialize;
use synchro_core::limits::clamp_rebuild_limit;

use crate::client::protocol_error_response;
use crate::cursor_token::issue_scope_cursor;
use crate::pull::{compute_bucket_checksums, get_latest_schema, hydrate_records};
use crate::rebuild::parse_cursor;
use crate::registry::load_registry;
use synchro_core::contract::ProtocolErrorCode;

#[derive(Debug, Clone, Serialize)]
struct PortableSeedScope {
    id: String,
    cursor: String,
    checksum: String,
}

#[derive(Debug, Clone, Serialize)]
struct PortableSeedRecord {
    table: String,
    record_id: String,
    checksum: Option<i32>,
    row: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
struct PortableSeedManifest {
    schema_version: i64,
    schema_hash: String,
    server_time: chrono::DateTime<chrono::Utc>,
    portable_scopes: Vec<PortableSeedScope>,
}

#[derive(Debug, Clone, Serialize)]
struct PortableSeedPage {
    scope: String,
    records: Vec<PortableSeedRecord>,
    cursor: Option<String>,
    has_more: bool,
}

#[pg_extern]
fn synchro_register_shared_scope(p_scope_id: &str, p_portable: default!(bool, "false")) {
    validate_shared_scope_id(p_scope_id);

    Spi::connect_mut(|client| {
        let _ = client
            .update(
                "INSERT INTO sync_shared_scopes (scope_id, portable)
                 VALUES ($1, $2)
                 ON CONFLICT (scope_id) DO UPDATE
                 SET portable = EXCLUDED.portable,
                     updated_at = now()",
                None,
                &[p_scope_id.into(), p_portable.into()],
            )
            .unwrap_or_else(|err| pgrx::error!("registering shared scope: {}", err));

        let _ = client
            .update(
                "UPDATE sync_clients
                 SET bucket_subs = array_append(bucket_subs, $1),
                     scope_set_version = scope_set_version + 1,
                     updated_at = now()
                 WHERE is_active = true
                   AND NOT ($1 = ANY(bucket_subs))",
                None,
                &[p_scope_id.into()],
            )
            .unwrap_or_else(|err| pgrx::error!("applying shared scope to clients: {}", err));

        let _ = client
            .update(
                "INSERT INTO sync_client_checkpoints (user_id, client_id, bucket_id, checkpoint)
                 SELECT user_id, client_id, $1, 0
                 FROM sync_clients
                 WHERE is_active = true
                 ON CONFLICT (user_id, client_id, bucket_id) DO NOTHING",
                None,
                &[p_scope_id.into()],
            )
            .unwrap_or_else(|err| pgrx::error!("seeding shared checkpoints: {}", err));
    });
}

#[pg_extern]
fn synchro_unregister_shared_scope(p_scope_id: &str) {
    validate_shared_scope_id(p_scope_id);

    Spi::connect_mut(|client| {
        let _ = client
            .update(
                "DELETE FROM sync_shared_scopes WHERE scope_id = $1",
                None,
                &[p_scope_id.into()],
            )
            .unwrap_or_else(|err| pgrx::error!("unregistering shared scope: {}", err));

        let _ = client
            .update(
                "UPDATE sync_clients
                 SET bucket_subs = array_remove(bucket_subs, $1),
                     scope_set_version = scope_set_version + 1,
                     updated_at = now()
                 WHERE is_active = true
                   AND $1 = ANY(bucket_subs)",
                None,
                &[p_scope_id.into()],
            )
            .unwrap_or_else(|err| pgrx::error!("removing shared scope from clients: {}", err));

        let _ = client
            .update(
                "DELETE FROM sync_client_checkpoints WHERE bucket_id = $1",
                None,
                &[p_scope_id.into()],
            )
            .unwrap_or_else(|err| pgrx::error!("removing shared checkpoints: {}", err));
    });
}

#[pg_extern]
fn synchro_portable_seed_manifest() -> pgrx::JsonB {
    Spi::connect(|client| {
        let (schema_version, schema_hash) = get_latest_schema(client);
        let portable_scopes = load_portable_shared_scopes(client);
        let seed_checkpoint = portable_seed_checkpoint(client);
        let checksums = compute_bucket_checksums(client, &portable_scopes);

        let response = PortableSeedManifest {
            schema_version,
            schema_hash,
            server_time: chrono::Utc::now(),
            portable_scopes: portable_scopes
                .into_iter()
                .map(|scope_id| PortableSeedScope {
                    id: scope_id.clone(),
                    cursor: issue_scope_cursor(client, &scope_id, seed_checkpoint)
                        .unwrap_or_else(|err| pgrx::error!("issuing portable seed cursor: {}", err)),
                    checksum: checksums.get(&scope_id).copied().unwrap_or(0).to_string(),
                })
                .collect(),
        };

        pgrx::JsonB(serde_json::to_value(response).unwrap())
    })
}

#[pg_extern]
fn synchro_portable_seed_scope(
    p_scope_id: &str,
    p_cursor: default!(&str, "''"),
    p_limit: default!(i32, "1000"),
) -> pgrx::JsonB {
    validate_shared_scope_id(p_scope_id);
    let limit = clamp_rebuild_limit(p_limit);

    Spi::connect(|client| {
        if !portable_shared_scope_exists(client, p_scope_id) {
            return protocol_error_response(
                ProtocolErrorCode::InvalidRequest,
                format!("scope {p_scope_id} is not portable"),
                false,
            );
        }

        let registry = match load_registry() {
            Ok(registry) => registry,
            Err(err) => pgrx::error!("loading registry for portable seed export: {}", err),
        };

        let cursor_parts = parse_cursor(p_cursor);
        let edges = query_edges_with_checksum(client, p_scope_id, cursor_parts.as_ref(), limit + 1);
        let has_more = edges.len() > limit as usize;
        let edges: Vec<(String, String, Option<i32>)> = if has_more {
            edges[..limit as usize].to_vec()
        } else {
            edges
        };

        let mut by_table: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        let mut checksum_by_record = std::collections::HashMap::new();
        for (table_name, record_id, checksum) in &edges {
            by_table
                .entry(table_name.clone())
                .or_default()
                .push(record_id.clone());
            checksum_by_record.insert((table_name.clone(), record_id.clone()), *checksum);
        }

        let mut records = Vec::new();
        for (table_name, record_ids) in &by_table {
            let id_refs: Vec<&str> = record_ids
                .iter()
                .map(|record_id| record_id.as_str())
                .collect();
            for record in hydrate_records(client, table_name, &id_refs, &registry) {
                let is_deleted = record
                    .get("deleted_at")
                    .and_then(|value| value.as_str())
                    .is_some();
                if is_deleted {
                    continue;
                }

                let record_id = record["id"].as_str().unwrap_or_default().to_string();
                records.push(PortableSeedRecord {
                    table: table_name.clone(),
                    record_id: record_id.clone(),
                    checksum: checksum_by_record
                        .get(&(table_name.clone(), record_id.clone()))
                        .copied()
                        .flatten(),
                    row: record["data"].clone(),
                });
            }
        }

        let response = PortableSeedPage {
            scope: p_scope_id.to_string(),
            records,
            cursor: if has_more {
                edges
                    .last()
                    .map(|(table_name, record_id, _)| format!("{table_name}|{record_id}"))
            } else {
                None
            },
            has_more,
        };

        pgrx::JsonB(serde_json::to_value(response).unwrap())
    })
}

pub(crate) fn load_portable_shared_scopes(client: &SpiClient<'_>) -> Vec<String> {
    let tup_table = client
        .select(
            "SELECT scope_id
             FROM sync_shared_scopes
             WHERE portable = true
             ORDER BY scope_id",
            None,
            &[],
        )
        .unwrap_or_else(|err| pgrx::error!("loading portable shared scopes: {}", err));

    let mut scopes = Vec::new();
    for row in tup_table {
        let scope_id: String = row
            .get_by_name::<String, &str>("scope_id")
            .unwrap_or(None)
            .unwrap_or_default();
        if !scope_id.is_empty() {
            scopes.push(scope_id);
        }
    }
    scopes
}

fn portable_shared_scope_exists(client: &SpiClient<'_>, scope_id: &str) -> bool {
    client
        .select(
            "SELECT EXISTS (
                SELECT 1
                FROM sync_shared_scopes
                WHERE scope_id = $1
                  AND portable = true
            ) AS exists",
            None,
            &[scope_id.into()],
        )
        .unwrap_or_else(|err| pgrx::error!("checking portable shared scope existence: {}", err))
        .next()
        .and_then(|row| row.get_by_name::<bool, &str>("exists").unwrap_or(None))
        .unwrap_or(false)
}

fn validate_shared_scope_id(scope_id: &str) {
    let trimmed = scope_id.trim();
    if trimmed.is_empty() {
        pgrx::error!("shared scope_id must not be empty");
    }
    if trimmed.starts_with("user:") {
        pgrx::error!("shared scope_id must not use the reserved user: prefix");
    }
}

fn portable_seed_checkpoint(client: &SpiClient<'_>) -> i64 {
    let tup = client
        .select(
            "SELECT COALESCE(MAX(seq), 0) AS checkpoint FROM sync_changelog",
            None,
            &[],
        )
        .unwrap_or_else(|err| pgrx::error!("loading portable seed checkpoint: {}", err));
    tup.into_iter()
        .next()
        .and_then(|row| row.get_by_name::<i64, &str>("checkpoint").unwrap_or(None))
        .unwrap_or(0)
}

fn query_edges_with_checksum(
    client: &SpiClient<'_>,
    scope_id: &str,
    cursor: Option<&(String, String)>,
    limit: i32,
) -> Vec<(String, String, Option<i32>)> {
    let tup_table = if let Some((cursor_table, cursor_id)) = cursor {
        client.select(
            "SELECT table_name, record_id, checksum
             FROM sync_bucket_edges
             WHERE bucket_id = $1
               AND (table_name, record_id) > ($2, $3)
             ORDER BY table_name, record_id
             LIMIT $4",
            None,
            &[
                scope_id.into(),
                cursor_table.as_str().into(),
                cursor_id.as_str().into(),
                limit.into(),
            ],
        )
    } else {
        client.select(
            "SELECT table_name, record_id, checksum
             FROM sync_bucket_edges
             WHERE bucket_id = $1
             ORDER BY table_name, record_id
             LIMIT $2",
            None,
            &[scope_id.into(), limit.into()],
        )
    }
    .unwrap_or_else(|err| pgrx::error!("querying portable scope edges: {}", err));

    let mut edges = Vec::new();
    for row in tup_table {
        let table_name: String = row
            .get_by_name::<String, &str>("table_name")
            .unwrap_or(None)
            .unwrap_or_default();
        let record_id: String = row
            .get_by_name::<String, &str>("record_id")
            .unwrap_or(None)
            .unwrap_or_default();
        let checksum: Option<i32> = row.get_by_name::<i32, &str>("checksum").unwrap_or(None);
        if table_name.is_empty() || record_id.is_empty() {
            continue;
        }
        edges.push((table_name, record_id, checksum));
    }
    edges
}
