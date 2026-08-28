use std::collections::{BTreeMap, HashSet};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use pgrx::prelude::*;
use pgrx::spi::SpiClient;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use synchro_core::checksum::{
    row_digest, row_identity, scope_digest, CanonicalRow, ChecksumObject, RowIdentity, SchemaHash,
    ScopeDigestEntry, Sha256Digest,
};
use synchro_core::limits::MAX_REBUILD_LIMIT;

use crate::client::protocol_error_response;
use crate::pull::{
    canonical_table, canonicalize_synced_row_data, contract_pk_value, row_primary_key_json,
};
use crate::registry::{load_registry_generation_from_client, TableRegistration};
use crate::seed_token::{self, SeedContinuationPayload, SeedPagePayload, SeedSnapshotBoundary};
use crate::spi_helpers::{
    current_utc_timestamp, decode_digest, is_lower_hex, is_lower_uuid, required_positive_i64,
    required_text,
};
use crate::stream_position::{parse_lsn, StreamPosition};
use synchro_core::contract::ProtocolErrorCode;

const EXPORT_STATE_SETTING: &str = "synchro.seed_export_state";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PortableSeedScope {
    id: String,
    registry_generation: i64,
    membership_generation: i64,
    retention_generation: i64,
    cardinality: i64,
    checksum: ChecksumObject,
    continuation: String,
    page_token: String,
}

#[derive(Debug, Clone, Serialize)]
struct PortableSeedRecord {
    table: String,
    pk: serde_json::Value,
    row_checksum: ChecksumObject,
    server_version: String,
    row: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
struct PortableSeedPage {
    scope: String,
    records: Vec<PortableSeedRecord>,
    page_token: Option<String>,
    has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PortableSeedManifest {
    export_id: String,
    export_manifest_hash: String,
    schema_version: i64,
    schema_hash: String,
    stream_generation: String,
    snapshot_boundary: SeedSnapshotBoundary,
    page_limit: i64,
    portable_scopes: Vec<PortableSeedScope>,
}

#[derive(Debug, Clone)]
struct ExportBoundary {
    stream_generation: String,
    position: StreamPosition,
    registry_generation: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExportScope {
    id: String,
    registry_generation: i64,
    membership_generation: i64,
    retention_generation: i64,
    stream_generation: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExportState {
    transaction_id: String,
    export_id: String,
    transaction_nonce: String,
    export_manifest_hash: String,
    schema_hash: String,
    stream_generation: String,
    boundary: SeedSnapshotBoundary,
    page_limit: i64,
    registry_generation: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExportScopeState {
    scope: ExportScope,
    cardinality: i64,
    checksum: ChecksumObject,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExportSessionState {
    key_id: String,
    state: ExportState,
    manifest: PortableSeedManifest,
    scopes: BTreeMap<String, ExportScopeState>,
}

#[derive(Debug, Clone)]
struct SeedRow {
    table: TableRegistration,
    record_id: String,
    row_identity: RowIdentity,
    row: serde_json::Value,
    checksum: Sha256Digest,
    server_version: String,
}

#[derive(Debug, Clone)]
struct SeedKey {
    key_id: String,
    secret: String,
}

#[pg_extern]
fn synchro_register_shared_scope(p_scope_id: &str, p_portable: default!(bool, "false")) {
    validate_shared_scope_id(p_scope_id);

    Spi::connect_mut(|client| {
        let _ = client
            .update(
                "INSERT INTO sync_scope_state (scope_id, stream_generation)
                 SELECT $1, stream_generation
                 FROM sync_runtime_state
                 WHERE singleton = true
                 ON CONFLICT (scope_id) DO NOTHING",
                None,
                &[p_scope_id.into()],
            )
            .unwrap_or_else(|err| pgrx::error!("registering shared scope state: {}", err));
        let _ = client
            .update(
                "INSERT INTO sync_shared_scopes (scope_id, portable)
                 VALUES ($1, $2)
                 ON CONFLICT (scope_id) DO UPDATE
                 SET portable = EXCLUDED.portable,
                     updated_at = now()
                  WHERE sync_shared_scopes.portable IS DISTINCT FROM EXCLUDED.portable",
                None,
                &[p_scope_id.into(), p_portable.into()],
            )
            .unwrap_or_else(|err| pgrx::error!("registering shared scope: {}", err));

        let _ = client
            .update(
                "WITH updated AS (
                     UPDATE sync_clients
                     SET bucket_subs = array_append(bucket_subs, $1),
                         scope_set_version = scope_set_version + 1,
                         updated_at = now()
                     WHERE is_active = true
                       AND NOT ($1 = ANY(bucket_subs))
                     RETURNING user_id, client_id, client_generation, scope_set_version
                 )
                 INSERT INTO sync_client_scope_history (
                     user_id, client_id, client_generation, scope_id,
                     scope_set_version, assigned, assignment_source,
                     membership_generation, retention_generation
                 )
                 SELECT updated.user_id, updated.client_id, updated.client_generation,
                        $1, updated.scope_set_version, true, 'shared',
                        state.membership_generation, state.retention_generation
                 FROM updated
                 JOIN sync_scope_state state ON state.scope_id = $1",
                None,
                &[p_scope_id.into()],
            )
            .unwrap_or_else(|err| pgrx::error!("applying shared scope to clients: {}", err));

        let _ = client
            .update(
                "INSERT INTO sync_client_checkpoints (
                     user_id, client_id, bucket_id, stream_generation, position_kind
                 )
                 SELECT c.user_id, c.client_id, $1, rs.stream_generation, 'generation_start'
                 FROM sync_clients c
                 CROSS JOIN sync_runtime_state rs
                 WHERE c.is_active = true AND rs.singleton = true
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
                "WITH updated AS (
                     UPDATE sync_clients
                     SET bucket_subs = array_remove(bucket_subs, $1),
                         scope_set_version = scope_set_version + 1,
                         updated_at = now()
                     WHERE is_active = true
                       AND $1 = ANY(bucket_subs)
                     RETURNING user_id, client_id, client_generation, scope_set_version
                 )
                 INSERT INTO sync_client_scope_history (
                     user_id, client_id, client_generation, scope_id,
                     scope_set_version, assigned, assignment_source,
                     membership_generation, retention_generation
                 )
                 SELECT updated.user_id, updated.client_id, updated.client_generation,
                        $1, updated.scope_set_version, false, 'shared',
                        state.membership_generation, state.retention_generation
                 FROM updated
                 JOIN sync_scope_state state ON state.scope_id = $1",
                None,
                &[p_scope_id.into()],
            )
            .unwrap_or_else(|err| pgrx::error!("removing shared scope from clients: {}", err));
    });
}

#[pg_extern]
fn synchro_portable_seed_manifest(p_page_limit: default!(i32, "1000")) -> pgrx::JsonB {
    if p_page_limit <= 0 || p_page_limit > MAX_REBUILD_LIMIT {
        return protocol_error_response(
            ProtocolErrorCode::InvalidRequest,
            format!("portable seed limit must be between 1 and {MAX_REBUILD_LIMIT}"),
            false,
        );
    }
    let page_limit = i64::from(p_page_limit);

    Spi::connect_mut(|client| {
        verify_export_transaction(client)
            .unwrap_or_else(|error| pgrx::error!("portable seed transaction is invalid: {error}"));

        if export_state_exists(client) {
            let state = load_export_state(client)
                .unwrap_or_else(|error| pgrx::error!("loading portable seed state: {error}"));
            if state.page_limit != page_limit {
                return protocol_error_response(
                    ProtocolErrorCode::InvalidRequest,
                    "portable seed page limit is fixed for the export",
                    false,
                );
            }
            return load_manifest_from_state(client)
                .unwrap_or_else(|error| pgrx::error!("loading portable seed manifest: {error}"));
        }

        let boundary = load_export_boundary(client)
            .unwrap_or_else(|error| pgrx::error!("loading portable seed boundary: {error}"));
        let (schema_version, schema_hash) =
            load_export_schema(client, boundary.registry_generation)
                .unwrap_or_else(|error| pgrx::error!("loading portable seed schema: {error}"));
        let registry = load_registry_generation_from_client(client, boundary.registry_generation)
            .unwrap_or_else(|error| pgrx::error!("loading portable seed registry: {error}"));
        let scopes = load_export_scopes(client, &boundary)
            .unwrap_or_else(|error| pgrx::error!("loading portable seed scopes: {error}"));

        let mut scope_rows = Vec::with_capacity(scopes.len());
        for scope in &scopes {
            let rows = load_seed_rows(client, scope, &boundary, &registry, &schema_hash)
                .unwrap_or_else(|error| pgrx::error!("verifying portable seed scope: {error}"));
            let checksum =
                compute_scope_checksum(&schema_hash, &scope.id, &rows).unwrap_or_else(|error| {
                    pgrx::error!("computing portable seed scope digest: {error}")
                });
            scope_rows.push((scope.clone(), rows, checksum));
        }

        let export_id = load_export_id(client).unwrap_or_else(|error| {
            pgrx::error!("creating portable seed export identity: {error}")
        });
        let transaction_nonce = load_transaction_nonce(client).unwrap_or_else(|error| {
            pgrx::error!("creating portable seed transaction nonce: {error}")
        });
        let transaction_nonce_text = URL_SAFE_NO_PAD.encode(&transaction_nonce);
        let boundary_wire = SeedSnapshotBoundary::from_position(&boundary.position);
        let body = export_manifest_body(
            &export_id,
            schema_version,
            &schema_hash,
            &boundary,
            &boundary_wire,
            page_limit,
            &scope_rows,
        );
        let export_manifest_hash = compute_export_manifest_hash(&body)
            .unwrap_or_else(|error| pgrx::error!("hashing portable seed manifest: {error}"));

        let page_key = load_seed_key(client, "seed_page", None)
            .unwrap_or_else(|error| pgrx::error!("loading portable seed page key: {error}"));
        let continuation_key =
            load_seed_key(client, "seed_continuation", None).unwrap_or_else(|error| {
                pgrx::error!("loading portable seed continuation key: {error}")
            });

        let mut response_scopes = Vec::with_capacity(scope_rows.len());
        let mut state_scopes = Vec::with_capacity(scope_rows.len());
        for (scope, rows, checksum) in &scope_rows {
            let continuation = seed_token::issue_continuation(
                &SeedContinuationPayload {
                    kind: "portable_seed_continuation".to_string(),
                    version: 1,
                    key_id: continuation_key.key_id.clone(),
                    export_id: export_id.clone(),
                    export_manifest_hash: export_manifest_hash.clone(),
                    schema_hash: schema_hash.clone(),
                    scope_id: scope.id.clone(),
                    registry_generation: scope.registry_generation.to_string(),
                    membership_generation: scope.membership_generation.to_string(),
                    retention_generation: scope.retention_generation.to_string(),
                    stream_generation: scope.stream_generation.clone(),
                    snapshot_boundary: boundary_wire.clone(),
                    cardinality: rows.len().to_string(),
                    checksum: *checksum,
                    issued_at: current_utc_timestamp(client, "portable seed timestamp", "")
                        .unwrap_or_else(|error| {
                            pgrx::error!("reading portable seed time: {error}")
                        }),
                },
                &continuation_key.secret,
            )
            .unwrap_or_else(|error| pgrx::error!("issuing portable seed receipt: {error}"));
            let page_token = seed_token::issue_page(
                &SeedPagePayload {
                    kind: "portable_seed_page".to_string(),
                    version: 1,
                    key_id: page_key.key_id.clone(),
                    export_id: export_id.clone(),
                    transaction_nonce: transaction_nonce_text.clone(),
                    export_manifest_hash: export_manifest_hash.clone(),
                    schema_hash: schema_hash.clone(),
                    scope_id: scope.id.clone(),
                    registry_generation: scope.registry_generation.to_string(),
                    membership_generation: scope.membership_generation.to_string(),
                    retention_generation: scope.retention_generation.to_string(),
                    stream_generation: scope.stream_generation.clone(),
                    snapshot_boundary: boundary_wire.clone(),
                    next_row_ordinal: "0".to_string(),
                    page_limit: page_limit.to_string(),
                },
                &page_key.secret,
            )
            .unwrap_or_else(|error| pgrx::error!("issuing portable seed page token: {error}"));

            response_scopes.push(PortableSeedScope {
                id: scope.id.clone(),
                registry_generation: scope.registry_generation,
                membership_generation: scope.membership_generation,
                retention_generation: scope.retention_generation,
                cardinality: rows.len() as i64,
                checksum: *checksum,
                continuation: continuation.clone(),
                page_token: page_token.clone(),
            });
            state_scopes.push((scope.clone(), rows.len() as i64, *checksum));
        }

        let response = PortableSeedManifest {
            export_id: export_id.clone(),
            export_manifest_hash: export_manifest_hash.clone(),
            schema_version,
            schema_hash: schema_hash.clone(),
            stream_generation: boundary.stream_generation.clone(),
            snapshot_boundary: boundary_wire.clone(),
            page_limit,
            portable_scopes: response_scopes,
        };
        create_export_state(
            client,
            &ExportState {
                transaction_id: current_export_transaction_id(client).unwrap_or_else(|error| {
                    pgrx::error!("loading portable seed transaction identity: {error}")
                }),
                export_id,
                transaction_nonce: transaction_nonce_text,
                export_manifest_hash,
                schema_hash,
                stream_generation: boundary.stream_generation,
                boundary: boundary_wire,
                page_limit,
                registry_generation: boundary.registry_generation,
            },
            &response,
            &state_scopes,
            &page_key,
        )
        .unwrap_or_else(|error| pgrx::error!("storing portable seed state: {error}"));
        pgrx::JsonB(serde_json::to_value(response).unwrap())
    })
}

#[pg_extern]
fn synchro_portable_seed_scope(
    p_scope_id: &str,
    p_page_token: &str,
    p_continuation_receipt: &str,
    p_expected_row_ordinal: i64,
    p_limit: default!(i32, "1000"),
) -> pgrx::JsonB {
    validate_shared_scope_id(p_scope_id);
    if p_limit <= 0 || p_limit > MAX_REBUILD_LIMIT {
        return protocol_error_response(
            ProtocolErrorCode::InvalidRequest,
            format!("portable seed limit must be between 1 and {MAX_REBUILD_LIMIT}"),
            false,
        );
    }
    let limit = i64::from(p_limit);
    if p_expected_row_ordinal < 0 {
        return protocol_error_response(
            ProtocolErrorCode::InvalidRequest,
            "portable seed expected row ordinal is invalid",
            false,
        );
    }

    Spi::connect(|client| {
        if !export_state_exists(client) {
            return protocol_error_response(
                ProtocolErrorCode::InvalidRequest,
                "portable seed export transaction is unavailable",
                false,
            );
        }
        if p_page_token.is_empty() {
            return protocol_error_response(
                ProtocolErrorCode::InvalidRequest,
                "portable seed page token is required",
                false,
            );
        }
        if p_continuation_receipt.is_empty() {
            return protocol_error_response(
                ProtocolErrorCode::InvalidRequest,
                "portable seed continuation receipt is required",
                false,
            );
        }
        if !portable_shared_scope_exists(client, p_scope_id) {
            return protocol_error_response(
                ProtocolErrorCode::InvalidRequest,
                format!("scope {p_scope_id} is not portable"),
                false,
            );
        }

        if let Err(error) = verify_export_transaction(client) {
            return protocol_error_response(ProtocolErrorCode::InvalidRequest, error, false);
        }
        let state = match load_export_state(client) {
            Ok(state) => state,
            Err(error) => {
                return protocol_error_response(ProtocolErrorCode::InvalidRequest, error, false)
            }
        };
        let scope_state = match load_export_scope_state(client, p_scope_id) {
            Ok(scope) => scope,
            Err(error) => {
                return protocol_error_response(ProtocolErrorCode::InvalidRequest, error, false)
            }
        };
        if state.page_limit != limit {
            return protocol_error_response(
                ProtocolErrorCode::InvalidRequest,
                "portable seed page limit does not match the export",
                false,
            );
        }

        let page_payload = match parse_and_verify_page_token(client, p_page_token) {
            Ok(payload) => payload,
            Err(error) => {
                return protocol_error_response(ProtocolErrorCode::InvalidRequest, error, false)
            }
        };
        if let Err(error) = validate_page_binding(
            &state,
            &scope_state,
            &page_payload,
            p_scope_id,
            p_expected_row_ordinal,
            limit,
        ) {
            return protocol_error_response(ProtocolErrorCode::InvalidRequest, error, false);
        }
        let receipt_payload = match parse_and_verify_continuation(client, p_continuation_receipt) {
            Ok(payload) => payload,
            Err(error) => {
                return protocol_error_response(ProtocolErrorCode::InvalidRequest, error, false)
            }
        };
        if let Err(error) =
            validate_export_continuation_binding(&state, &scope_state, &receipt_payload, p_scope_id)
        {
            return protocol_error_response(ProtocolErrorCode::InvalidRequest, error, false);
        }
        let start = match parse_unsigned_ordinal(&page_payload.next_row_ordinal) {
            Ok(value) => value,
            Err(error) => {
                return protocol_error_response(ProtocolErrorCode::InvalidRequest, error, false)
            }
        };

        let boundary = ExportBoundary {
            stream_generation: state.stream_generation.clone(),
            position: stream_position_from_wire(&state.boundary)
                .unwrap_or_else(|error| pgrx::error!("portable seed boundary is invalid: {error}")),
            registry_generation: state.registry_generation,
        };
        let registry = load_registry_generation_from_client(client, state.registry_generation)
            .unwrap_or_else(|error| pgrx::error!("loading portable seed registry: {error}"));
        let rows = load_seed_rows(
            client,
            &scope_state.scope,
            &boundary,
            &registry,
            &state.schema_hash,
        )
        .unwrap_or_else(|error| pgrx::error!("verifying portable seed page rows: {error}"));
        let checksum = compute_scope_checksum(&state.schema_hash, p_scope_id, &rows)
            .unwrap_or_else(|error| pgrx::error!("verifying portable seed page scope: {error}"));
        if rows.len() as i64 != scope_state.cardinality
            || checksum.digest() != scope_state.checksum.digest()
        {
            pgrx::error!("portable seed scope changed inside its export transaction");
        }
        if start > rows.len() as u64 {
            return protocol_error_response(
                ProtocolErrorCode::InvalidRequest,
                "portable seed page ordinal is outside the export",
                false,
            );
        }
        let start_index = start as usize;
        let end_index = start_index
            .checked_add(limit as usize)
            .unwrap_or_else(|| pgrx::error!("portable seed page ordinal overflow"))
            .min(rows.len());
        let has_more = end_index < rows.len();
        let records = rows[start_index..end_index]
            .iter()
            .map(seed_record)
            .collect::<Vec<_>>();
        let page_token = if has_more {
            let page_key = load_seed_key(client, "seed_page", None)
                .unwrap_or_else(|error| pgrx::error!("loading portable seed page key: {error}"));
            Some(
                seed_token::issue_page(
                    &SeedPagePayload {
                        kind: "portable_seed_page".to_string(),
                        version: 1,
                        key_id: page_key.key_id,
                        export_id: state.export_id.clone(),
                        transaction_nonce: state.transaction_nonce.clone(),
                        export_manifest_hash: state.export_manifest_hash.clone(),
                        schema_hash: state.schema_hash.clone(),
                        scope_id: p_scope_id.to_string(),
                        registry_generation: scope_state.scope.registry_generation.to_string(),
                        membership_generation: scope_state.scope.membership_generation.to_string(),
                        retention_generation: scope_state.scope.retention_generation.to_string(),
                        stream_generation: scope_state.scope.stream_generation.clone(),
                        snapshot_boundary: state.boundary.clone(),
                        next_row_ordinal: end_index.to_string(),
                        page_limit: limit.to_string(),
                    },
                    &page_key.secret,
                )
                .unwrap_or_else(|error| pgrx::error!("issuing portable seed page token: {error}")),
            )
        } else {
            None
        };

        pgrx::JsonB(
            serde_json::to_value(PortableSeedPage {
                scope: p_scope_id.to_string(),
                records,
                page_token,
                has_more,
            })
            .unwrap(),
        )
    })
}

fn verify_export_transaction(client: &SpiClient<'_>) -> Result<(), String> {
    let row = client
        .select(
            "SELECT current_setting('transaction_isolation') AS isolation,
                    current_setting('transaction_read_only') AS read_only,
                    current_setting('transaction_deferrable') AS deferrable",
            None,
            &[],
        )
        .map_err(|error| format!("reading transaction characteristics: {error}"))?
        .next()
        .ok_or_else(|| "transaction characteristics are unavailable".to_string())?;
    let isolation = required_text(&row, "isolation", "")?;
    let read_only = required_text(&row, "read_only", "")?;
    let deferrable = required_text(&row, "deferrable", "")?;
    if isolation != "serializable" || read_only != "on" || deferrable != "on" {
        return Err("portable seed requires SERIALIZABLE READ ONLY DEFERRABLE".to_string());
    }
    Ok(())
}

fn export_state_exists(client: &SpiClient<'_>) -> bool {
    client
        .select(
            "SELECT NULLIF(current_setting($1, true), '') IS NOT NULL AS present",
            None,
            &[EXPORT_STATE_SETTING.into()],
        )
        .ok()
        .and_then(|rows| rows.into_iter().next())
        .and_then(|row| row.get_by_name::<bool, &str>("present").ok().flatten())
        .unwrap_or(false)
}

fn load_export_boundary(client: &SpiClient<'_>) -> Result<ExportBoundary, String> {
    let row = client
        .select(
            "SELECT stream_generation,
                    materialized_commit_lsn::text AS materialized_commit_lsn,
                    registry_generation
             FROM sync_wal_progress
             WHERE singleton = true",
            None,
            &[],
        )
        .map_err(|error| format!("reading materialization progress: {error}"))?
        .next()
        .ok_or_else(|| "materialization progress is missing".to_string())?;
    let stream_generation = required_text(&row, "stream_generation", "")?;
    let registry_generation = required_positive_i64(&row, "registry_generation")?;
    let commit_lsn = row
        .get_by_name::<String, &str>("materialized_commit_lsn")
        .map_err(|error| format!("reading materialized boundary: {error}"))?;
    let position = match commit_lsn {
        Some(value) => StreamPosition::transaction_end(&value)?,
        None => StreamPosition::GenerationStart,
    };
    Ok(ExportBoundary {
        stream_generation,
        position,
        registry_generation,
    })
}

fn load_export_schema(
    client: &SpiClient<'_>,
    registry_generation: i64,
) -> Result<(i64, String), String> {
    let row = client
        .select(
            "SELECT schema_version, schema_hash, canonical_manifest_body
             FROM sync_schema_manifest
             WHERE registry_generation <= $1
             ORDER BY registry_generation DESC, schema_version DESC
             LIMIT 1",
            None,
            &[registry_generation.into()],
        )
        .map_err(|error| format!("reading immutable schema manifest: {error}"))?
        .next()
        .ok_or_else(|| "immutable schema manifest is missing".to_string())?;
    let schema_version = required_positive_i64(&row, "schema_version")?;
    let schema_hash = required_text(&row, "schema_hash", "")?;
    if !is_lower_hex(&schema_hash, 64) {
        return Err("immutable schema hash is invalid".to_string());
    }
    let body = required_text(&row, "canonical_manifest_body", "")?;
    let parsed: serde_json::Value = serde_json::from_str(&body)
        .map_err(|_| "immutable schema manifest is invalid".to_string())?;
    let canonical = serde_json_canonicalizer::to_vec(&parsed)
        .map_err(|error| format!("canonicalizing immutable schema manifest: {error}"))?;
    if canonical != body.as_bytes() {
        return Err("immutable schema manifest is not canonical".to_string());
    }
    let mut hasher = Sha256::new();
    hasher.update(b"synchro:v3:schema-manifest:v1\0");
    hasher.update(&canonical);
    if format!("{:x}", hasher.finalize()) != schema_hash {
        return Err("immutable schema manifest hash does not match".to_string());
    }
    Ok((schema_version, schema_hash))
}

fn load_export_scopes(
    client: &SpiClient<'_>,
    boundary: &ExportBoundary,
) -> Result<Vec<ExportScope>, String> {
    let rows = client
        .select(
            "SELECT shared.scope_id,
                    state.stream_generation,
                    state.membership_generation,
                    state.retention_generation
             FROM sync_shared_scopes shared
             JOIN sync_scope_state state ON state.scope_id = shared.scope_id
             WHERE shared.portable = true
             ORDER BY shared.scope_id",
            None,
            &[],
        )
        .map_err(|error| format!("reading portable scope declarations: {error}"))?;
    let mut scopes = Vec::with_capacity(rows.len());
    let mut seen = HashSet::with_capacity(rows.len());
    for row in rows {
        let id = required_text(&row, "scope_id", "")?;
        if !seen.insert(id.clone()) {
            return Err("portable scope declarations contain a duplicate".to_string());
        }
        let stream_generation = required_text(&row, "stream_generation", "")?;
        if stream_generation != boundary.stream_generation {
            return Err("portable scope has the wrong stream generation".to_string());
        }
        let membership_generation = required_positive_i64(&row, "membership_generation")?;
        let retention_generation = required_positive_i64(&row, "retention_generation")?;
        scopes.push(ExportScope {
            id,
            registry_generation: boundary.registry_generation,
            membership_generation,
            retention_generation,
            stream_generation,
        });
    }
    Ok(scopes)
}

fn load_seed_rows(
    client: &SpiClient<'_>,
    scope: &ExportScope,
    boundary: &ExportBoundary,
    registry: &[TableRegistration],
    schema_hash: &str,
) -> Result<Vec<SeedRow>, String> {
    let rows = client
        .select(
            "SELECT edge.relation_id::text AS edge_relation_id,
                    edge.table_name,
                    edge.record_id,
                    edge.checksum AS edge_checksum,
                    edge.row_version::text AS edge_row_version,
                    captured.relation_id::text AS captured_relation_id,
                    captured.row_data,
                    captured.row_version::text AS captured_row_version,
                    captured.checksum AS captured_checksum,
                    captured.deleted,
                     captured.source_stream_generation,
                     captured.source_commit_lsn::text AS source_commit_lsn,
                     captured.source_event_ordinal,
                     captured.source_reset_id::text AS source_reset_id,
                     source_reset.target_stream_generation AS reset_stream_generation,
                     source_reset.lifecycle AS reset_lifecycle,
                     captured.registry_generation
              FROM sync_bucket_edges edge
              LEFT JOIN sync_captured_rows captured
                ON captured.relation_id = edge.relation_id
               AND captured.record_id = edge.record_id
              LEFT JOIN sync_stream_resets source_reset
                ON source_reset.reset_id = captured.source_reset_id
             WHERE edge.bucket_id = $1",
            None,
            &[scope.id.as_str().into()],
        )
        .map_err(|error| format!("reading portable scope projections: {error}"))?;
    let expected_schema_hash = SchemaHash::from_lower_hex(schema_hash)
        .map_err(|error| format!("portable seed schema hash is invalid: {error}"))?;
    let mut result = Vec::with_capacity(rows.len());
    let mut identities = HashSet::with_capacity(rows.len());
    for row in rows {
        let relation_id = required_text(&row, "edge_relation_id", "")?;
        let captured_relation_id = required_text(&row, "captured_relation_id", "")?;
        if relation_id != captured_relation_id {
            return Err("portable scope edge and captured relation differ".to_string());
        }
        let table_name = required_text(&row, "table_name", "")?;
        let record_id = required_text(&row, "record_id", "")?;
        let table = registry
            .iter()
            .find(|table| table.relation_id == relation_id && table.table_name == table_name)
            .ok_or_else(|| "portable scope edge is not in the export registry".to_string())?;
        if table.registry_generation != scope.registry_generation {
            return Err("portable scope edge has the wrong registry generation".to_string());
        }
        let captured_generation = required_positive_i64(&row, "registry_generation")?;
        if captured_generation != scope.registry_generation {
            return Err("portable captured row has the wrong registry generation".to_string());
        }
        let source_generation = required_text(&row, "source_stream_generation", "")?;
        if source_generation != boundary.stream_generation {
            return Err("portable captured row has the wrong stream generation".to_string());
        }
        let source_reset_id = row
            .get_by_name::<String, &str>("source_reset_id")
            .map_err(|error| format!("reading portable reset provenance: {error}"))?;
        if source_reset_id.is_some() {
            let reset_stream_generation = required_text(&row, "reset_stream_generation", "")?;
            let reset_lifecycle = required_text(&row, "reset_lifecycle", "")?;
            let source_lsn = row
                .get_by_name::<String, &str>("source_commit_lsn")
                .map_err(|error| format!("reading portable source LSN: {error}"))?;
            let source_ordinal = row
                .get_by_name::<i64, &str>("source_event_ordinal")
                .map_err(|error| format!("reading portable source ordinal: {error}"))?;
            if reset_stream_generation != boundary.stream_generation
                || !matches!(reset_lifecycle.as_str(), "activated" | "cleanup_complete")
                || source_lsn.is_some()
                || source_ordinal.is_some()
            {
                return Err("portable reset baseline binding is invalid".to_string());
            }
        } else {
            let source_lsn = required_text(&row, "source_commit_lsn", "")?;
            let source_lsn_value = parse_lsn(&source_lsn)
                .ok_or_else(|| "portable captured row has an invalid source LSN".to_string())?;
            if row
                .get_by_name::<i64, &str>("source_event_ordinal")
                .map_err(|error| format!("reading portable source ordinal: {error}"))?
                .filter(|ordinal| *ordinal >= 0)
                .is_none()
            {
                return Err("portable captured row has an invalid source ordinal".to_string());
            }
            let Some(boundary_lsn) = boundary
                .position
                .commit_lsn()
                .as_deref()
                .and_then(parse_lsn)
            else {
                return Err(
                    "portable seed contains rows beyond the generation boundary".to_string()
                );
            };
            if source_lsn_value > boundary_lsn {
                return Err("portable captured row is outside the snapshot boundary".to_string());
            }
        }

        let edge_checksum = decode_digest(
            row.get_by_name::<Vec<u8>, &str>("edge_checksum")
                .map_err(|error| format!("reading portable edge checksum: {error}"))?
                .ok_or_else(|| "portable edge checksum is missing".to_string())?,
            "portable seed checksum must contain 32 octets",
        )?;
        let captured_checksum = decode_digest(
            row.get_by_name::<Vec<u8>, &str>("captured_checksum")
                .map_err(|error| format!("reading portable captured checksum: {error}"))?
                .ok_or_else(|| "portable captured checksum is missing".to_string())?,
            "portable seed checksum must contain 32 octets",
        )?;
        if edge_checksum != captured_checksum {
            return Err("portable edge and captured checksums differ".to_string());
        }
        let edge_version = row
            .get_by_name::<String, &str>("edge_row_version")
            .map_err(|error| format!("reading portable edge version: {error}"))?;
        let server_version = row
            .get_by_name::<String, &str>("captured_row_version")
            .map_err(|error| format!("reading portable captured version: {error}"))?
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "portable captured row version is missing".to_string())?;
        if edge_version
            .as_deref()
            .is_some_and(|version| version != server_version)
        {
            return Err("portable edge and captured versions differ".to_string());
        }
        let deleted = row
            .get_by_name::<bool, &str>("deleted")
            .map_err(|error| format!("reading portable captured deletion state: {error}"))?
            .ok_or_else(|| "portable captured deletion state is missing".to_string())?;
        if deleted {
            return Err("portable scope edge references a tombstone".to_string());
        }
        let row = row
            .get_by_name::<pgrx::JsonB, &str>("row_data")
            .map_err(|error| format!("reading portable captured row: {error}"))?
            .map(|value| value.0)
            .filter(serde_json::Value::is_object)
            .ok_or_else(|| "portable captured row is missing".to_string())?;
        let mut row = row;
        canonicalize_synced_row_data(table, &mut row)
            .map_err(|error| format!("portable captured row is not canonical: {error}"))?;
        let primary_key = row_primary_key_json(table, &record_id)?;
        if row.get(&table.primary_key_field_id) != Some(&primary_key) {
            return Err("portable row primary key differs from its identity".to_string());
        }
        let canonical = canonical_table(table)?;
        let row_identity = row_identity(
            &canonical,
            &serde_json::to_string(&primary_key)
                .map_err(|error| format!("encoding portable primary key: {error}"))?,
        )
        .map_err(|error| format!("computing portable row identity: {error}"))?;
        if !identities.insert(row_identity.clone()) {
            return Err("portable scope contains a duplicate logical row identity".to_string());
        }
        let canonical_row = CanonicalRow::from_json(
            serde_json::to_string(&primary_key)
                .map_err(|error| format!("encoding portable primary key: {error}"))?,
            &serde_json::to_string(&row)
                .map_err(|error| format!("encoding portable row: {error}"))?,
        )
        .map_err(|error| format!("validating portable row: {error}"))?;
        let computed = row_digest(
            expected_schema_hash,
            &canonical,
            &canonical_row,
            &server_version,
        )
        .map_err(|error| format!("computing portable row checksum: {error}"))?;
        if computed != captured_checksum {
            return Err("portable captured row checksum does not match".to_string());
        }
        result.push(SeedRow {
            table: table.clone(),
            record_id,
            row_identity,
            row,
            checksum: computed,
            server_version,
        });
    }
    result.sort_by(|left, right| {
        left.row_identity
            .as_bytes()
            .cmp(right.row_identity.as_bytes())
    });
    Ok(result)
}

fn compute_scope_checksum(
    schema_hash: &str,
    scope_id: &str,
    rows: &[SeedRow],
) -> Result<ChecksumObject, String> {
    let schema_hash = SchemaHash::from_lower_hex(schema_hash)
        .map_err(|error| format!("portable scope schema hash is invalid: {error}"))?;
    let entries = rows
        .iter()
        .map(|row| ScopeDigestEntry::new(row.row_identity.clone(), row.checksum))
        .collect::<Vec<_>>();
    scope_digest(schema_hash, scope_id, &entries)
        .map(ChecksumObject::new)
        .map_err(|error| format!("computing portable scope checksum: {error}"))
}

fn export_manifest_body(
    export_id: &str,
    schema_version: i64,
    schema_hash: &str,
    boundary: &ExportBoundary,
    boundary_wire: &SeedSnapshotBoundary,
    page_limit: i64,
    scopes: &[(ExportScope, Vec<SeedRow>, ChecksumObject)],
) -> serde_json::Value {
    serde_json::json!({
        "export_id": export_id,
        "schema_version": schema_version,
        "schema_hash": schema_hash,
        "stream_generation": boundary.stream_generation,
        "snapshot_boundary": boundary_wire,
        "page_limit": page_limit,
        "portable_scopes": scopes.iter().map(|(scope, rows, checksum)| serde_json::json!({
            "id": scope.id,
            "registry_generation": scope.registry_generation,
            "membership_generation": scope.membership_generation,
            "retention_generation": scope.retention_generation,
            "cardinality": rows.len(),
            "checksum": checksum,
        })).collect::<Vec<_>>(),
    })
}

fn compute_export_manifest_hash(body: &serde_json::Value) -> Result<String, String> {
    let canonical = serde_json_canonicalizer::to_vec(body)
        .map_err(|error| format!("canonicalizing portable seed manifest: {error}"))?;
    let mut hasher = Sha256::new();
    hasher.update(b"synchro:v3:seed-export-manifest:v1\0");
    hasher.update(canonical);
    Ok(format!("{:x}", hasher.finalize()))
}

fn create_export_state(
    client: &SpiClient<'_>,
    state: &ExportState,
    manifest: &PortableSeedManifest,
    scopes: &[(ExportScope, i64, ChecksumObject)],
    key: &SeedKey,
) -> Result<(), String> {
    let mut scope_states = BTreeMap::new();
    for (scope, cardinality, checksum) in scopes {
        if scope_states
            .insert(
                scope.id.clone(),
                ExportScopeState {
                    scope: scope.clone(),
                    cardinality: *cardinality,
                    checksum: *checksum,
                },
            )
            .is_some()
        {
            return Err("portable seed export has duplicate scope state".to_string());
        }
    }
    let session = ExportSessionState {
        key_id: key.key_id.clone(),
        state: state.clone(),
        manifest: manifest.clone(),
        scopes: scope_states,
    };
    let token = seed_token::issue_export_session(&session, &key.secret)?;
    client
        .select(
            "SELECT set_config($1, $2, true)",
            None,
            &[EXPORT_STATE_SETTING.into(), token.as_str().into()],
        )
        .map_err(|error| format!("storing portable seed export state: {error}"))?;
    Ok(())
}

fn load_export_state(client: &SpiClient<'_>) -> Result<ExportState, String> {
    load_export_session(client).map(|session| session.state)
}

fn load_manifest_from_state(client: &SpiClient<'_>) -> Result<pgrx::JsonB, String> {
    let manifest = load_export_session(client)?.manifest;
    serde_json::to_value(manifest)
        .map(pgrx::JsonB)
        .map_err(|error| format!("encoding portable seed manifest: {error}"))
}

fn load_export_scope_state(
    client: &SpiClient<'_>,
    scope_id: &str,
) -> Result<ExportScopeState, String> {
    load_export_session(client)?
        .scopes
        .remove(scope_id)
        .ok_or_else(|| "portable seed scope state is missing".to_string())
}

fn load_export_session(client: &SpiClient<'_>) -> Result<ExportSessionState, String> {
    let token = client
        .select(
            "SELECT NULLIF(current_setting($1, true), '') AS state",
            None,
            &[EXPORT_STATE_SETTING.into()],
        )
        .map_err(|error| format!("reading portable seed export state: {error}"))?
        .next()
        .and_then(|row| row.get_by_name::<String, &str>("state").ok().flatten())
        .ok_or_else(|| "portable seed export state is missing".to_string())?;
    let key_id = token_key_id(&token, "portable seed export session")?;
    let key = load_seed_key(client, "seed_page", Some(&key_id))?;
    let session: ExportSessionState = seed_token::verify_export_session(&token, &key.secret)?;
    if session.key_id != key.key_id
        || session.state.transaction_id != current_export_transaction_id(client)?
        || !is_lower_uuid(&session.state.export_id)
        || decode_nonce(&session.state.transaction_nonce).is_err()
        || !is_lower_hex(&session.state.export_manifest_hash, 64)
        || !is_lower_hex(&session.state.schema_hash, 64)
        || session.state.stream_generation.is_empty()
        || session.state.page_limit <= 0
        || session.state.registry_generation <= 0
    {
        return Err("portable seed export state is invalid".to_string());
    }
    session.state.boundary.validate()?;
    if session.manifest.export_id != session.state.export_id
        || session.manifest.export_manifest_hash != session.state.export_manifest_hash
        || session.manifest.schema_hash != session.state.schema_hash
        || session.manifest.stream_generation != session.state.stream_generation
        || session.manifest.snapshot_boundary != session.state.boundary
        || session.manifest.page_limit != session.state.page_limit
        || session.manifest.portable_scopes.len() != session.scopes.len()
    {
        return Err("portable seed export manifest state is invalid".to_string());
    }
    for (scope_id, scope) in &session.scopes {
        if scope_id != &scope.scope.id
            || scope.cardinality < 0
            || scope.scope.registry_generation != session.state.registry_generation
            || scope.scope.membership_generation <= 0
            || scope.scope.retention_generation <= 0
            || scope.scope.stream_generation != session.state.stream_generation
        {
            return Err("portable seed export scope state is invalid".to_string());
        }
    }
    Ok(session)
}

fn current_export_transaction_id(client: &SpiClient<'_>) -> Result<String, String> {
    let transaction_id = client
        .select(
            "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
            None,
            &[],
        )
        .map_err(|error| format!("reading portable seed transaction identity: {error}"))?
        .next()
        .and_then(|row| {
            row.get_by_name::<String, &str>("transaction_id")
                .ok()
                .flatten()
        })
        .ok_or_else(|| "portable seed transaction identity is missing".to_string())?;
    transaction_id
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .map(|_| transaction_id)
        .ok_or_else(|| "portable seed transaction identity is invalid".to_string())
}

fn parse_and_verify_page_token(
    client: &SpiClient<'_>,
    token: &str,
) -> Result<SeedPagePayload, String> {
    let key_id = token_key_id(token, "portable seed page token")?;
    let key = load_seed_key(client, "seed_page", Some(&key_id))?;
    seed_token::verify_page(token, &key.secret)
}

fn parse_and_verify_continuation(
    client: &SpiClient<'_>,
    token: &str,
) -> Result<SeedContinuationPayload, String> {
    let key_id = token_key_id(token, "portable seed continuation receipt")?;
    let key = load_seed_key(client, "seed_continuation", Some(&key_id))?;
    seed_token::verify_continuation(token, &key.secret)
}

/// Validate portable seed receipts for first authenticated connect.
///
/// A stale or unverifiable receipt set returns an empty position map, so
/// every seeded scope degrades to a null cursor and a required rebuild.
/// Only a server-side fault produces an error response.
pub(crate) fn validate_seed_receipts(
    client: &SpiClient<'_>,
    receipts: &BTreeMap<String, String>,
    current_schema_hash: &str,
) -> Result<BTreeMap<String, StreamPosition>, pgrx::JsonB> {
    match validate_seed_receipts_inner(client, receipts, current_schema_hash) {
        Ok(Some(positions)) => Ok(positions),
        Ok(None) => Ok(BTreeMap::new()),
        Err(_) => Err(protocol_error_response(
            ProtocolErrorCode::InvalidRequest,
            "invalid portable seed receipts",
            false,
        )),
    }
}

fn validate_seed_receipts_inner(
    client: &SpiClient<'_>,
    receipts: &BTreeMap<String, String>,
    current_schema_hash: &str,
) -> Result<Option<BTreeMap<String, StreamPosition>>, String> {
    let rows = client
        .select(
            "SELECT shared.scope_id, state.stream_generation,
                    state.membership_generation, state.retention_generation,
                    progress.registry_generation
             FROM sync_shared_scopes shared
             JOIN sync_scope_state state ON state.scope_id = shared.scope_id
             JOIN sync_wal_progress progress
               ON progress.singleton = true
              AND progress.stream_generation = state.stream_generation
             WHERE shared.portable = true
             ORDER BY shared.scope_id",
            None,
            &[],
        )
        .map_err(|error| format!("loading portable seed receipt scopes: {error}"))?;
    if rows.len() != receipts.len() {
        return Ok(None);
    }

    let materialized = crate::stream_position::load_materialized_boundary(client)?;
    let mut positions = BTreeMap::new();
    let mut export_binding: Option<(String, String, SeedSnapshotBoundary)> = None;
    for row in rows {
        let scope_id = required_text(&row, "scope_id", "")?;
        let Some(receipt) = receipts.get(&scope_id) else {
            return Ok(None);
        };
        let Ok(payload) = parse_and_verify_continuation(client, receipt) else {
            return Ok(None);
        };
        let stream_generation = required_text(&row, "stream_generation", "")?;
        let membership_generation = required_positive_i64(&row, "membership_generation")?;
        let retention_generation = required_positive_i64(&row, "retention_generation")?;
        let registry_generation = required_positive_i64(&row, "registry_generation")?;
        if payload.scope_id != scope_id
            || payload.schema_hash != current_schema_hash
            || payload.stream_generation != stream_generation
            || payload.stream_generation != materialized.stream_generation
            || payload.registry_generation != registry_generation.to_string()
            || payload.membership_generation != membership_generation.to_string()
            || payload.retention_generation != retention_generation.to_string()
        {
            return Ok(None);
        }

        let binding = (
            payload.export_id.clone(),
            payload.export_manifest_hash.clone(),
            payload.snapshot_boundary.clone(),
        );
        if export_binding
            .as_ref()
            .is_some_and(|expected| expected != &binding)
        {
            return Ok(None);
        }
        export_binding.get_or_insert(binding);

        let Ok(position) = stream_position_from_wire(&payload.snapshot_boundary) else {
            return Ok(None);
        };
        if position > materialized.position {
            return Ok(None);
        }
        positions.insert(scope_id, position);
    }
    Ok(Some(positions))
}

fn token_key_id(token: &str, name: &str) -> Result<String, String> {
    token
        .split('.')
        .nth(1)
        .and_then(|encoded| URL_SAFE_NO_PAD.decode(encoded).ok())
        .and_then(|payload| serde_json::from_slice::<serde_json::Value>(&payload).ok())
        .and_then(|payload| {
            payload
                .get("key_id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .ok_or_else(|| format!("{name} is invalid"))
}

fn validate_page_binding(
    state: &ExportState,
    scope: &ExportScopeState,
    payload: &SeedPagePayload,
    scope_id: &str,
    expected_row_ordinal: i64,
    limit: i64,
) -> Result<(), String> {
    if payload.export_id != state.export_id
        || payload.transaction_nonce != state.transaction_nonce
        || payload.export_manifest_hash != state.export_manifest_hash
        || payload.schema_hash != state.schema_hash
        || payload.scope_id != scope_id
        || payload.registry_generation != scope.scope.registry_generation.to_string()
        || payload.membership_generation != scope.scope.membership_generation.to_string()
        || payload.retention_generation != scope.scope.retention_generation.to_string()
        || payload.stream_generation != scope.scope.stream_generation
        || payload.snapshot_boundary != state.boundary
        || payload.next_row_ordinal != expected_row_ordinal.to_string()
        || payload.page_limit != limit.to_string()
    {
        return Err("portable seed page token binding is invalid".to_string());
    }
    if scope.scope.stream_generation != state.stream_generation {
        return Err("portable seed scope stream binding is invalid".to_string());
    }
    Ok(())
}

fn validate_export_continuation_binding(
    state: &ExportState,
    scope: &ExportScopeState,
    payload: &SeedContinuationPayload,
    scope_id: &str,
) -> Result<(), String> {
    if payload.export_id != state.export_id
        || payload.export_manifest_hash != state.export_manifest_hash
        || payload.schema_hash != state.schema_hash
        || payload.scope_id != scope_id
        || payload.registry_generation != scope.scope.registry_generation.to_string()
        || payload.membership_generation != scope.scope.membership_generation.to_string()
        || payload.retention_generation != scope.scope.retention_generation.to_string()
        || payload.stream_generation != scope.scope.stream_generation
        || payload.snapshot_boundary != state.boundary
        || payload.cardinality != scope.cardinality.to_string()
        || payload.checksum != scope.checksum
    {
        return Err("portable seed continuation receipt binding is invalid".to_string());
    }
    if scope.scope.stream_generation != state.stream_generation {
        return Err("portable seed scope stream binding is invalid".to_string());
    }
    Ok(())
}

fn seed_record(row: &SeedRow) -> PortableSeedRecord {
    PortableSeedRecord {
        table: row.table.table_id.clone(),
        pk: contract_pk_value(
            std::slice::from_ref(&row.table),
            &row.table.table_name,
            &row.record_id,
        ),
        row_checksum: ChecksumObject::new(row.checksum),
        server_version: row.server_version.clone(),
        row: row.row.clone(),
    }
}

fn load_seed_key(
    client: &SpiClient<'_>,
    purpose: &str,
    key_id: Option<&str>,
) -> Result<SeedKey, String> {
    let rows = if let Some(key_id) = key_id {
        client.select(
            "SELECT key_id, secret
                 FROM sync_token_keys
                 WHERE purpose = $1
                   AND key_id = $2
                   AND state IN ('active', 'verify_only')",
            None,
            &[purpose.into(), key_id.into()],
        )
    } else {
        client.select(
            "SELECT key_id, secret
             FROM sync_token_keys
             WHERE purpose = $1 AND state = 'active'",
            None,
            &[purpose.into()],
        )
    }
    .map_err(|error| format!("loading portable seed token key: {error}"))?;
    if rows.len() != 1 {
        return Err("portable seed token key is unavailable".to_string());
    }
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| "portable seed token key is unavailable".to_string())?;
    let key_id = required_text(&row, "key_id", "")?;
    let secret = required_text(&row, "secret", "")?;
    if secret.len() < 64 {
        return Err("portable seed token key is invalid".to_string());
    }
    Ok(SeedKey { key_id, secret })
}

fn load_export_id(client: &SpiClient<'_>) -> Result<String, String> {
    let row = client
        .select(
            "SELECT lower(gen_random_uuid()::text) AS export_id",
            None,
            &[],
        )
        .map_err(|error| format!("creating portable seed export ID: {error}"))?
        .next()
        .ok_or_else(|| "portable seed export ID is missing".to_string())?;
    let export_id = required_text(&row, "export_id", "")?;
    if !is_lower_uuid(&export_id) {
        return Err("portable seed export ID is invalid".to_string());
    }
    Ok(export_id)
}

fn load_transaction_nonce(client: &SpiClient<'_>) -> Result<Vec<u8>, String> {
    let row = client
        .select(
            "SELECT uuid_send(gen_random_uuid()) || uuid_send(gen_random_uuid()) AS transaction_nonce",
            None,
            &[],
        )
        .map_err(|error| format!("creating portable seed transaction nonce: {error}"))?
        .next()
        .ok_or_else(|| "portable seed transaction nonce is missing".to_string())?;
    let nonce = row
        .get_by_name::<Vec<u8>, &str>("transaction_nonce")
        .map_err(|error| format!("reading portable seed transaction nonce: {error}"))?
        .ok_or_else(|| "portable seed transaction nonce is missing".to_string())?;
    if nonce.len() != 32 {
        return Err("portable seed transaction nonce is invalid".to_string());
    }
    Ok(nonce)
}

fn stream_position_from_wire(boundary: &SeedSnapshotBoundary) -> Result<StreamPosition, String> {
    match (
        boundary.position_kind.as_str(),
        boundary.commit_lsn.as_deref(),
    ) {
        ("generation_start", None) => Ok(StreamPosition::GenerationStart),
        ("transaction_end", Some(commit_lsn)) => StreamPosition::transaction_end(commit_lsn),
        _ => Err("portable seed snapshot boundary is invalid".to_string()),
    }
}

fn parse_unsigned_ordinal(value: &str) -> Result<u64, String> {
    if value.is_empty()
        || (value != "0" && value.starts_with('0'))
        || !value.bytes().all(|b| b.is_ascii_digit())
    {
        return Err("portable seed page ordinal is invalid".to_string());
    }
    value
        .parse::<u64>()
        .map_err(|_| "portable seed page ordinal is invalid".to_string())
}

fn decode_nonce(value: &str) -> Result<Vec<u8>, String> {
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| "portable seed transaction nonce is invalid".to_string())?;
    if bytes.len() != 32 {
        return Err("portable seed transaction nonce is invalid".to_string());
    }
    Ok(bytes)
}

fn portable_shared_scope_exists(client: &SpiClient<'_>, scope_id: &str) -> bool {
    client
        .select(
            "SELECT EXISTS (
                 SELECT 1
                 FROM sync_shared_scopes
                 WHERE scope_id = $1 AND portable = true
             ) AS exists",
            None,
            &[scope_id.into()],
        )
        .ok()
        .and_then(|rows| rows.into_iter().next())
        .and_then(|row| row.get_by_name::<bool, &str>("exists").ok().flatten())
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
