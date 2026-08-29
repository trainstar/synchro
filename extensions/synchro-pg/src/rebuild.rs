use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiHeapTupleData};
use synchro_core::checksum::{
    row_identity, scope_digest, ChecksumObject, RowIdentity, SchemaHash, ScopeDigestEntry,
    Sha256Digest,
};
use synchro_core::contract::{
    ErrorBody, ErrorResponse, ProtocolErrorCode, RebuildRecord, RebuildRequest, RebuildResponse,
};
use synchro_core::limits::MAX_REBUILD_LIMIT;

use crate::client::{
    acquire_client_identity_lock, load_client_connect_state, protocol_error_response,
    validate_schema_ref,
};
use crate::cursor_token::{issue_scope_cursor, ScopeCursorContext};
use crate::pull::{canonical_table, contract_pk_value, row_primary_key_json, synced_row_digest};
use crate::rebuild_token::{
    issue_rebuild_continuation, parse_rebuild_continuation, RebuildContinuation,
    RebuildContinuationInput,
};
use crate::registry::{load_registry_from_client, TableRegistration};
use crate::spi_helpers::{current_utc_timestamp, decode_digest, required_text};
use crate::stream_position::{load_materialized_boundary, StreamBoundary, StreamPosition};

const SESSION_COLUMNS: &str = "
    session_id::text AS session_id,
    user_id,
    client_id,
    rebuild_id::text AS rebuild_id,
    scope_id,
    client_generation,
    schema_version,
    schema_hash,
    stream_generation,
    membership_generation,
    retention_generation,
    boundary_position_kind,
    boundary_commit_lsn::text AS boundary_commit_lsn,
    boundary_event_ordinal,
    boundary_effect_ordinal,
    accepted_write_epoch,
    page_limit,
    snapshot_checksum,
    staged_row_count,
    to_char(expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS expires_at,
    expires_at <= now() AS expired";

#[derive(Clone)]
struct RebuildClientState {
    bucket_subs: Vec<String>,
    client_generation: i64,
    accepted_write_epoch: i64,
}

#[derive(Clone)]
struct ScopeBinding {
    stream_generation: String,
    membership_generation: i64,
    retention_generation: i64,
}

#[derive(Clone)]
struct RebuildSession {
    session_id: String,
    user_id: String,
    client_id: String,
    rebuild_id: String,
    scope_id: String,
    client_generation: i64,
    schema_version: i64,
    schema_hash: String,
    stream_generation: String,
    membership_generation: i64,
    retention_generation: i64,
    boundary: StreamPosition,
    accepted_write_epoch: i64,
    page_limit: i64,
    snapshot_checksum: ChecksumObject,
    staged_row_count: i64,
    expires_at: String,
    expired: bool,
}

#[derive(Clone)]
struct StagedRecord {
    table: String,
    row_identity: Vec<u8>,
    pk: serde_json::Value,
    row: serde_json::Value,
    row_checksum: Sha256Digest,
    server_version: String,
}

/// Rebuild one assigned scope from an immutable, durable projection snapshot.
#[pg_extern(name = "synchro_rebuild")]
fn synchro_rebuild_contract(p_user_id: &str, p_request: pgrx::JsonB) -> pgrx::JsonB {
    if p_user_id.is_empty() {
        return protocol_error_response(
            ProtocolErrorCode::AuthRequired,
            "authentication is required",
            false,
        );
    }

    let request: RebuildRequest = match serde_json::from_value(p_request.0) {
        Ok(request) => request,
        Err(_) => return invalid_request_response(),
    };
    if request.validate().is_err() || request.limit > i64::from(MAX_REBUILD_LIMIT) {
        return invalid_request_response();
    }

    Spi::connect_mut(|client| {
        let _ = client.update(
            "SELECT set_config('app.user_id', $1, true)",
            None,
            &[p_user_id.into()],
        );
        acquire_client_identity_lock(client, p_user_id, &request.client_id);
        client
            .update("LOCK TABLE sync_wal_progress IN SHARE MODE", None, &[])
            .unwrap_or_else(|error| pgrx::error!("locking rebuild boundary: {error}"));
        lock_rebuild_identity(client, p_user_id, &request.client_id, &request.rebuild_id);

        let connected = match load_client_connect_state(client, p_user_id, &request.client_id) {
            Ok(state) => state,
            Err(error) => return error,
        };
        if request.client_generation != connected.client_generation {
            return crate::client::client_generation_expired_response(connected.client_generation);
        }
        if let Err(error) = validate_schema_ref(client, &request.schema) {
            return error;
        }
        let client_state = match load_rebuild_client_state(client, p_user_id, &request.client_id) {
            Ok(Some(state)) => state,
            Ok(None) => return invalid_request_response(),
            Err(error) => return integrity_failure_with_log("loading client state", &error),
        };
        if request.client_generation != client_state.client_generation {
            return crate::client::client_generation_expired_response(
                client_state.client_generation,
            );
        }

        let continuation = match request.cursor.as_deref() {
            Some(cursor) => match parse_rebuild_continuation(client, cursor) {
                Ok(continuation)
                    if continuation.matches_request(
                        p_user_id,
                        &request.client_id,
                        &request.scope,
                        &request.rebuild_id,
                        request.limit,
                    ) =>
                {
                    Some(continuation)
                }
                _ => return invalid_request_response(),
            },
            None => None,
        };

        let session = match continuation.as_ref() {
            Some(continuation) => match load_session_by_id(client, &continuation.session_id) {
                Ok(Some(session)) => Some(session),
                Ok(None) => return rebuild_restart_response(&request.scope),
                Err(_) => return integrity_failure_response(),
            },
            None => match load_session_by_rebuild(
                client,
                p_user_id,
                &request.client_id,
                &request.rebuild_id,
            ) {
                Ok(session) => session,
                Err(_) => return integrity_failure_response(),
            },
        };

        if let Some(session) = session {
            if !session_identity_matches_request(&session, p_user_id, &request) {
                return invalid_request_response();
            }
            if let Some(continuation) = continuation.as_ref() {
                if !continuation_matches_session(continuation, &session) {
                    return invalid_request_response();
                }
            }
            if !client_state.bucket_subs.contains(&request.scope) {
                return rebuild_restart_response(&request.scope);
            }
            let boundary = match load_materialized_boundary(client) {
                Ok(boundary) => boundary,
                Err(_) => return integrity_failure_response(),
            };
            let scope_binding = match load_scope_binding(client, &request.scope) {
                Ok(Some(binding)) => binding,
                Ok(None) => return rebuild_restart_response(&request.scope),
                Err(_) => return integrity_failure_response(),
            };
            if !session_is_current(&session, &request, &client_state, &scope_binding, &boundary) {
                return rebuild_restart_response(&request.scope);
            }
            if crate::pull::client_has_pending_fence(client, p_user_id, &request.client_id) {
                return protocol_error_response(
                    ProtocolErrorCode::CapturePending,
                    "accepted writes are pending capture",
                    true,
                );
            }
            let row_ordinal = continuation
                .as_ref()
                .map(|continuation| continuation.next_row_ordinal)
                .unwrap_or(0);
            return match load_stored_page(client, &session, row_ordinal) {
                Ok(Some(response)) => response_json(response),
                Ok(None) if continuation.is_some() => {
                    match create_page(client, p_user_id, &session, row_ordinal) {
                        Ok(response) => response_json(response),
                        Err(_) => integrity_failure_response(),
                    }
                }
                Ok(None) => integrity_failure_response(),
                Err(_) => integrity_failure_response(),
            };
        }

        if continuation.is_some() || !client_state.bucket_subs.contains(&request.scope) {
            return invalid_request_response();
        }
        if crate::pull::client_has_pending_fence(client, p_user_id, &request.client_id) {
            return protocol_error_response(
                ProtocolErrorCode::CapturePending,
                "accepted writes are pending capture",
                true,
            );
        }
        let boundary = match load_materialized_boundary(client) {
            Ok(boundary) => boundary,
            Err(error) => return integrity_failure_with_log("loading boundary", &error),
        };
        let scope_binding = match load_scope_binding(client, &request.scope) {
            Ok(Some(binding)) => binding,
            Ok(None) => return integrity_failure_response(),
            Err(error) => return integrity_failure_with_log("loading scope state", &error),
        };
        if scope_binding.stream_generation != boundary.stream_generation {
            return integrity_failure_response();
        }
        let registry = match load_registry_from_client(client) {
            Ok(registry) => registry,
            Err(error) => return integrity_failure_with_log("loading registry", &error),
        };
        let (staged_records, checksum) = match stage_records(
            client,
            &registry,
            &request.scope,
            &request.schema.hash,
            &boundary,
        ) {
            Ok(staged) => staged,
            Err(error) => return integrity_failure_with_log("staging snapshot", &error),
        };
        let session = match create_session(
            client,
            p_user_id,
            &request,
            &client_state,
            &scope_binding,
            &boundary,
            checksum,
            &staged_records,
        ) {
            Ok(session) => session,
            Err(error) => return integrity_failure_with_log("creating session", &error),
        };
        insert_staged_records(client, &session, &staged_records)
            .unwrap_or_else(|error| pgrx::error!("staging rebuild snapshot: {error}"));
        let response = create_page(client, p_user_id, &session, 0)
            .unwrap_or_else(|error| pgrx::error!("creating first rebuild page: {error}"));
        response_json(response)
    })
}

fn lock_rebuild_identity(
    client: &mut SpiClient<'_>,
    user_id: &str,
    client_id: &str,
    rebuild_id: &str,
) {
    client
        .update(
            "SELECT pg_advisory_xact_lock(
                 hashtextextended(jsonb_build_array($1::text, $2::text, $3::text)::text, 0)
             )",
            None,
            &[user_id.into(), client_id.into(), rebuild_id.into()],
        )
        .unwrap_or_else(|error| pgrx::error!("locking rebuild identity: {error}"));
}

fn load_rebuild_client_state(
    client: &SpiClient<'_>,
    user_id: &str,
    client_id: &str,
) -> Result<Option<RebuildClientState>, String> {
    let rows = client
        .select(
            "SELECT bucket_subs, client_generation, accepted_write_epoch
             FROM sync_clients
             WHERE user_id = $1 AND client_id = $2 AND is_active = true
             FOR SHARE",
            None,
            &[user_id.into(), client_id.into()],
        )
        .map_err(|error| format!("locking rebuild client state: {error}"))?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(None);
    };
    let bucket_subs = row
        .get_by_name::<Vec<String>, &str>("bucket_subs")
        .map_err(|error| format!("reading rebuild scope assignments: {error}"))?
        .ok_or_else(|| "rebuild scope assignments are missing".to_string())?;
    let client_generation = row
        .get_by_name::<i64, &str>("client_generation")
        .map_err(|error| format!("reading rebuild client generation: {error}"))?
        .filter(|generation| *generation > 0)
        .ok_or_else(|| "rebuild client generation is invalid".to_string())?;
    let accepted_write_epoch = row
        .get_by_name::<i64, &str>("accepted_write_epoch")
        .map_err(|error| format!("reading rebuild accepted-write epoch: {error}"))?
        .filter(|epoch| *epoch > 0)
        .ok_or_else(|| "rebuild accepted-write epoch is invalid".to_string())?;
    Ok(Some(RebuildClientState {
        bucket_subs,
        client_generation,
        accepted_write_epoch,
    }))
}

fn load_scope_binding(
    client: &SpiClient<'_>,
    scope_id: &str,
) -> Result<Option<ScopeBinding>, String> {
    let rows = client
        .select(
            "SELECT stream_generation, membership_generation, retention_generation
             FROM sync_scope_state
             WHERE scope_id = $1
             FOR SHARE",
            None,
            &[scope_id.into()],
        )
        .map_err(|error| format!("locking rebuild scope state: {error}"))?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(None);
    };
    let stream_generation = row
        .get_by_name::<String, &str>("stream_generation")
        .map_err(|error| format!("reading rebuild scope stream generation: {error}"))?
        .filter(|generation| !generation.is_empty())
        .ok_or_else(|| "rebuild scope stream generation is missing".to_string())?;
    let membership_generation = row
        .get_by_name::<i64, &str>("membership_generation")
        .map_err(|error| format!("reading rebuild membership generation: {error}"))?
        .filter(|generation| *generation > 0)
        .ok_or_else(|| "rebuild membership generation is invalid".to_string())?;
    let retention_generation = row
        .get_by_name::<i64, &str>("retention_generation")
        .map_err(|error| format!("reading rebuild retention generation: {error}"))?
        .filter(|generation| *generation > 0)
        .ok_or_else(|| "rebuild retention generation is invalid".to_string())?;
    Ok(Some(ScopeBinding {
        stream_generation,
        membership_generation,
        retention_generation,
    }))
}

fn load_session_by_id(
    client: &SpiClient<'_>,
    session_id: &str,
) -> Result<Option<RebuildSession>, String> {
    let query = format!(
        "SELECT {SESSION_COLUMNS}
         FROM sync_rebuild_sessions
         WHERE session_id = $1::uuid
         FOR SHARE"
    );
    let rows = client
        .select(&query, None, &[session_id.into()])
        .map_err(|error| format!("loading rebuild session: {error}"))?;
    rows.into_iter().next().map(parse_session).transpose()
}

fn load_session_by_rebuild(
    client: &SpiClient<'_>,
    user_id: &str,
    client_id: &str,
    rebuild_id: &str,
) -> Result<Option<RebuildSession>, String> {
    let query = format!(
        "SELECT {SESSION_COLUMNS}
         FROM sync_rebuild_sessions
         WHERE user_id = $1 AND client_id = $2 AND rebuild_id = $3::uuid
         FOR SHARE"
    );
    let rows = client
        .select(
            &query,
            None,
            &[user_id.into(), client_id.into(), rebuild_id.into()],
        )
        .map_err(|error| format!("loading rebuild session: {error}"))?;
    rows.into_iter().next().map(parse_session).transpose()
}

fn parse_session(row: SpiHeapTupleData<'_>) -> Result<RebuildSession, String> {
    let session_id = required_text(&row, "session_id", "rebuild ")?;
    let user_id = required_text(&row, "user_id", "rebuild ")?;
    let client_id = required_text(&row, "client_id", "rebuild ")?;
    let rebuild_id = required_text(&row, "rebuild_id", "rebuild ")?;
    let scope_id = required_text(&row, "scope_id", "rebuild ")?;
    let client_generation = required_positive_i64(&row, "client_generation")?;
    let schema_version = required_positive_i64(&row, "schema_version")?;
    let schema_hash = required_text(&row, "schema_hash", "rebuild ")?;
    let stream_generation = required_text(&row, "stream_generation", "rebuild ")?;
    let membership_generation = required_positive_i64(&row, "membership_generation")?;
    let retention_generation = required_positive_i64(&row, "retention_generation")?;
    let boundary_kind = required_text(&row, "boundary_position_kind", "rebuild ")?;
    let boundary_commit_lsn = row
        .get_by_name::<String, &str>("boundary_commit_lsn")
        .map_err(|error| format!("reading rebuild boundary commit LSN: {error}"))?;
    let boundary_event_ordinal = row
        .get_by_name::<i64, &str>("boundary_event_ordinal")
        .map_err(|error| format!("reading rebuild boundary event ordinal: {error}"))?;
    let boundary_effect_ordinal = row
        .get_by_name::<i32, &str>("boundary_effect_ordinal")
        .map_err(|error| format!("reading rebuild boundary effect ordinal: {error}"))?;
    let boundary = StreamPosition::from_sql_parts(
        &boundary_kind,
        boundary_commit_lsn.as_deref(),
        boundary_event_ordinal,
        boundary_effect_ordinal,
    )?;
    if !matches!(
        &boundary,
        StreamPosition::GenerationStart | StreamPosition::TransactionEnd { .. }
    ) {
        return Err("rebuild boundary is not a snapshot boundary".to_string());
    }
    let checksum = row
        .get_by_name::<Vec<u8>, &str>("snapshot_checksum")
        .map_err(|error| format!("reading rebuild snapshot checksum: {error}"))?
        .ok_or_else(|| "rebuild snapshot checksum is missing".to_string())?;
    let snapshot_checksum = ChecksumObject::new(decode_digest(
        checksum,
        "rebuild snapshot checksum must contain exactly 32 octets",
    )?);
    let staged_row_count = row
        .get_by_name::<i64, &str>("staged_row_count")
        .map_err(|error| format!("reading rebuild staged row count: {error}"))?
        .filter(|count| *count >= 0)
        .ok_or_else(|| "rebuild staged row count is invalid".to_string())?;
    Ok(RebuildSession {
        session_id,
        user_id,
        client_id,
        rebuild_id,
        scope_id,
        client_generation,
        schema_version,
        schema_hash,
        stream_generation,
        membership_generation,
        retention_generation,
        boundary,
        accepted_write_epoch: required_positive_i64(&row, "accepted_write_epoch")?,
        page_limit: required_positive_i64(&row, "page_limit")?,
        snapshot_checksum,
        staged_row_count,
        expires_at: required_text(&row, "expires_at", "rebuild ")?,
        expired: row
            .get_by_name::<bool, &str>("expired")
            .map_err(|error| format!("reading rebuild session expiry: {error}"))?
            .ok_or_else(|| "rebuild session expiry is missing".to_string())?,
    })
}

fn session_identity_matches_request(
    session: &RebuildSession,
    user_id: &str,
    request: &RebuildRequest,
) -> bool {
    session.user_id == user_id
        && session.client_id == request.client_id
        && session.rebuild_id == request.rebuild_id
        && session.scope_id == request.scope
        && session.page_limit == request.limit
}

fn continuation_matches_session(
    continuation: &RebuildContinuation,
    session: &RebuildSession,
) -> bool {
    continuation.session_id == session.session_id
        && continuation.rebuild_id == session.rebuild_id
        && continuation.stream_generation == session.stream_generation
        && continuation.client_generation == session.client_generation
        && continuation.scope_id == session.scope_id
        && continuation.schema_hash == session.schema_hash
        && continuation.membership_generation == session.membership_generation
        && continuation.retention_generation == session.retention_generation
        && continuation.snapshot_boundary == session.boundary
        && continuation.page_limit == session.page_limit
        && continuation.accepted_write_epoch == session.accepted_write_epoch
        && continuation.expires_at == session.expires_at
        && continuation.next_row_ordinal < session.staged_row_count
}

fn session_is_current(
    session: &RebuildSession,
    request: &RebuildRequest,
    client: &RebuildClientState,
    scope: &ScopeBinding,
    boundary: &StreamBoundary,
) -> bool {
    !session.expired
        && session.client_generation == client.client_generation
        && session.client_generation == request.client_generation
        && session.schema_version == request.schema.version
        && session.schema_hash == request.schema.hash
        && session.accepted_write_epoch == client.accepted_write_epoch
        && session.stream_generation == boundary.stream_generation
        && session.stream_generation == scope.stream_generation
        && session.membership_generation == scope.membership_generation
        && session.retention_generation == scope.retention_generation
}

/// registry_generation_lineage returns the target generation and every ancestor
/// reachable through parent_generation.
///
/// A rebuild binds captured rows at the active registry generation. Registering
/// a relation again, which is how a membership rule changes, activates a new
/// generation without rewriting sync_captured_rows. Only schema publication
/// re-stamps those rows, through migrate_schema_digests. A row that still
/// carries an older generation therefore has an unchanged table definition, and
/// its content remains valid for the rebuild. Restricting acceptance to the
/// ancestry keeps a row from an unrelated lineage out.
fn registry_generation_lineage(
    client: &SpiClient<'_>,
    target_generation: i64,
) -> Result<std::collections::HashSet<i64>, String> {
    let mut lineage = std::collections::HashSet::new();
    let mut current = target_generation;
    while current > 0 {
        if !lineage.insert(current) {
            return Err("rebuild registry lineage is invalid".to_string());
        }
        if lineage.len() > 10_000 {
            return Err("rebuild registry lineage is invalid".to_string());
        }
        let parent = client
            .select(
                "SELECT parent_generation
                 FROM synchro.sync_registry_generations
                 WHERE generation = $1",
                None,
                &[current.into()],
            )
            .map_err(|_| "loading rebuild registry lineage failed".to_string())?
            .first()
            .get_by_name::<i64, &str>("parent_generation")
            .map_err(|_| "loading rebuild registry lineage failed".to_string())?;
        match parent {
            Some(value) if value > 0 && value < current => current = value,
            _ => break,
        }
    }
    Ok(lineage)
}

fn stage_records(
    client: &SpiClient<'_>,
    registry: &[TableRegistration],
    scope_id: &str,
    schema_hash: &str,
    boundary: &StreamBoundary,
) -> Result<(Vec<StagedRecord>, ChecksumObject), String> {
    let active_generation = registry
        .first()
        .map(|table| table.registry_generation)
        .unwrap_or_default();
    let lineage = registry_generation_lineage(client, active_generation)?;
    let rows = client
        .select(
            "SELECT edge.relation_id::text AS relation_id,
                    edge.table_name,
                    edge.record_id,
                    edge.checksum AS edge_checksum,
                    edge.row_version::text AS edge_row_version,
                    captured.row_data,
                    captured.row_version::text AS captured_row_version,
                    captured.checksum AS captured_checksum,
                    captured.deleted AS captured_deleted,
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
             WHERE edge.bucket_id = $1
             ORDER BY edge.relation_id, edge.record_id",
            None,
            &[scope_id.into()],
        )
        .map_err(|error| format!("loading rebuild source projections: {error}"))?;

    let mut staged = Vec::with_capacity(rows.len());
    for row in rows {
        let relation_id = required_text(&row, "relation_id", "rebuild ")?;
        let table_name = required_text(&row, "table_name", "rebuild ")?;
        let record_id = required_text(&row, "record_id", "rebuild ")?;
        let table = registry
            .iter()
            .find(|table| table.relation_id == relation_id)
            .ok_or_else(|| "rebuild edge relation is not registered".to_string())?;
        if table.table_name != table_name {
            return Err("rebuild edge table identity is invalid".to_string());
        }
        let edge_checksum = row
            .get_by_name::<Vec<u8>, &str>("edge_checksum")
            .map_err(|error| format!("reading rebuild edge checksum: {error}"))?
            .ok_or_else(|| "rebuild edge checksum is missing".to_string())?;
        let captured_checksum = row
            .get_by_name::<Vec<u8>, &str>("captured_checksum")
            .map_err(|error| format!("reading rebuild captured checksum: {error}"))?
            .ok_or_else(|| "rebuild captured row is missing".to_string())?;
        let row_checksum = decode_digest(
            captured_checksum,
            "rebuild captured checksum must contain exactly 32 octets",
        )?;
        if edge_checksum.as_slice() != row_checksum.as_bytes() {
            return Err("rebuild edge checksum differs from captured row".to_string());
        }
        let edge_version = row
            .get_by_name::<String, &str>("edge_row_version")
            .map_err(|error| format!("reading rebuild edge version: {error}"))?
            .filter(|version| !version.is_empty())
            .ok_or_else(|| "rebuild edge version is missing".to_string())?;
        let server_version = row
            .get_by_name::<String, &str>("captured_row_version")
            .map_err(|error| format!("reading rebuild captured version: {error}"))?
            .filter(|version| !version.is_empty())
            .ok_or_else(|| "rebuild captured version is missing".to_string())?;
        if edge_version != server_version {
            return Err("rebuild edge version differs from captured row".to_string());
        }
        let deleted = row
            .get_by_name::<bool, &str>("captured_deleted")
            .map_err(|error| format!("reading rebuild captured deletion state: {error}"))?
            .ok_or_else(|| "rebuild captured deletion state is missing".to_string())?;
        if deleted {
            return Err("rebuild edge references a captured tombstone".to_string());
        }
        let source_stream_generation = required_text(&row, "source_stream_generation", "rebuild ")?;
        let captured_generation = required_positive_i64(&row, "registry_generation")?;
        if source_stream_generation != boundary.stream_generation
            || !lineage.contains(&captured_generation)
        {
            return Err("rebuild captured row binding is invalid".to_string());
        }
        let source_reset_id = row
            .get_by_name::<String, &str>("source_reset_id")
            .map_err(|error| format!("reading rebuild reset provenance: {error}"))?;
        if source_reset_id.is_some() {
            let reset_stream_generation =
                required_text(&row, "reset_stream_generation", "rebuild ")?;
            let reset_lifecycle = required_text(&row, "reset_lifecycle", "rebuild ")?;
            let source_commit_lsn = row
                .get_by_name::<String, &str>("source_commit_lsn")
                .map_err(|error| format!("reading rebuild source LSN: {error}"))?;
            let source_event_ordinal = row
                .get_by_name::<i64, &str>("source_event_ordinal")
                .map_err(|error| format!("reading rebuild source event ordinal: {error}"))?;
            if reset_stream_generation != boundary.stream_generation
                || !matches!(reset_lifecycle.as_str(), "activated" | "cleanup_complete")
                || source_commit_lsn.is_some()
                || source_event_ordinal.is_some()
            {
                return Err("rebuild reset baseline binding is invalid".to_string());
            }
        } else {
            let source_commit_lsn = required_text(&row, "source_commit_lsn", "rebuild ")?;
            row.get_by_name::<i64, &str>("source_event_ordinal")
                .map_err(|error| format!("reading rebuild source event ordinal: {error}"))?
                .filter(|ordinal| *ordinal >= 0)
                .ok_or_else(|| "rebuild source event ordinal is invalid".to_string())?;
            let source_position = StreamPosition::transaction_end(&source_commit_lsn)?;
            if !matches!(&boundary.position, StreamPosition::TransactionEnd { .. })
                || source_position > boundary.position
            {
                return Err("rebuild captured row is outside the snapshot boundary".to_string());
            }
        }
        let row = row
            .get_by_name::<pgrx::JsonB, &str>("row_data")
            .map_err(|error| format!("reading rebuild captured row: {error}"))?
            .map(|value| value.0)
            .filter(serde_json::Value::is_object)
            .ok_or_else(|| "rebuild captured row is missing".to_string())?;
        let computed = synced_row_digest(client, table, &row, &record_id, &server_version)?;
        if computed != row_checksum {
            return Err("rebuild captured row checksum does not match".to_string());
        }
        let primary_key = row_primary_key_json(table, &record_id)?;
        let canonical_table = canonical_table(table)?;
        let row_identity = row_identity(
            &canonical_table,
            &serde_json::to_string(&primary_key)
                .map_err(|error| format!("encoding rebuild primary key: {error}"))?,
        )
        .map_err(|error| format!("encoding rebuild row identity: {error}"))?;
        staged.push(StagedRecord {
            table: table.table_id.clone(),
            row_identity: row_identity.into_bytes(),
            pk: contract_pk_value(registry, &table.table_name, &record_id),
            row,
            row_checksum,
            server_version,
        });
    }
    staged.sort_by(|left, right| {
        left.table
            .as_bytes()
            .cmp(right.table.as_bytes())
            .then_with(|| left.row_identity.cmp(&right.row_identity))
    });
    let schema_hash = SchemaHash::from_lower_hex(schema_hash)
        .map_err(|error| format!("rebuild schema hash is invalid: {error}"))?;
    let scope_entries = staged
        .iter()
        .map(|record| {
            let identity = RowIdentity::from_bytes(record.row_identity.clone())
                .map_err(|error| format!("rebuild staged row identity is invalid: {error}"))?;
            Ok(ScopeDigestEntry::new(identity, record.row_checksum))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let checksum = scope_digest(schema_hash, scope_id, &scope_entries)
        .map_err(|error| format!("computing rebuild snapshot checksum: {error}"))?;
    Ok((staged, ChecksumObject::new(checksum)))
}

#[allow(clippy::too_many_arguments)]
fn create_session(
    client: &SpiClient<'_>,
    user_id: &str,
    request: &RebuildRequest,
    client_state: &RebuildClientState,
    scope_binding: &ScopeBinding,
    boundary: &StreamBoundary,
    checksum: ChecksumObject,
    staged_records: &[StagedRecord],
) -> Result<RebuildSession, String> {
    let staged_row_count = i64::try_from(staged_records.len())
        .map_err(|_| "rebuild staged row count exceeds int64".to_string())?;
    let boundary_lsn = boundary.position.commit_lsn();
    let rows = client
        .select(
            "INSERT INTO sync_rebuild_sessions (
                 user_id, client_id, rebuild_id, scope_id, client_generation,
                 schema_version, schema_hash, stream_generation,
                 membership_generation, retention_generation,
                 boundary_position_kind, boundary_commit_lsn,
                 boundary_event_ordinal, boundary_effect_ordinal,
                 accepted_write_epoch, page_limit, snapshot_checksum, staged_row_count
             ) VALUES (
                 $1, $2, $3::uuid, $4, $5,
                 $6, $7, $8,
                 $9, $10,
                 $11, $12::pg_lsn,
                 NULL, NULL,
                 $13, $14, $15, $16
             )
             RETURNING
                 session_id::text AS session_id,
                 user_id,
                 client_id,
                 rebuild_id::text AS rebuild_id,
                 scope_id,
                 client_generation,
                 schema_version,
                 schema_hash,
                 stream_generation,
                 membership_generation,
                 retention_generation,
                 boundary_position_kind,
                 boundary_commit_lsn::text AS boundary_commit_lsn,
                 boundary_event_ordinal,
                 boundary_effect_ordinal,
                 accepted_write_epoch,
                 page_limit,
                 snapshot_checksum,
                 staged_row_count,
                 to_char(expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS expires_at,
                 false AS expired",
            None,
            &[
                user_id.into(),
                request.client_id.as_str().into(),
                request.rebuild_id.as_str().into(),
                request.scope.as_str().into(),
                request.client_generation.into(),
                request.schema.version.into(),
                request.schema.hash.as_str().into(),
                boundary.stream_generation.as_str().into(),
                scope_binding.membership_generation.into(),
                scope_binding.retention_generation.into(),
                boundary.position.kind().into(),
                boundary_lsn.as_deref().into(),
                client_state.accepted_write_epoch.into(),
                request.limit.into(),
                checksum.digest().as_bytes().to_vec().into(),
                staged_row_count.into(),
            ],
        )
        .map_err(|error| format!("creating rebuild session: {error}"))?;
    rows.into_iter()
        .next()
        .ok_or_else(|| "rebuild session insert returned no row".to_string())
        .and_then(parse_session)
}

fn insert_staged_records(
    client: &mut SpiClient<'_>,
    session: &RebuildSession,
    records: &[StagedRecord],
) -> Result<(), String> {
    if records.is_empty() {
        return Ok(());
    }
    let staged = records
        .iter()
        .enumerate()
        .map(|(ordinal, record)| {
            Ok(serde_json::json!({
                "row_ordinal": i64::try_from(ordinal)
                    .map_err(|_| "rebuild row ordinal exceeds int64")?,
                "table_id": record.table,
                "row_identity_hex": encode_hex(&record.row_identity),
                "primary_key": record.pk,
                "row_data": record.row,
                "row_checksum_hex": record.row_checksum.to_lower_hex(),
                "server_version": record.server_version,
            }))
        })
        .collect::<Result<Vec<_>, String>>()?;
    client
        .update(
            "INSERT INTO sync_rebuild_staged_rows (
                 session_id, row_ordinal, table_id, row_identity,
                 primary_key, row_data, row_checksum, server_version
             )
             SELECT $1::uuid,
                    stage.row_ordinal,
                    stage.table_id,
                    decode(stage.row_identity_hex, 'hex'),
                    stage.primary_key,
                    stage.row_data,
                    decode(stage.row_checksum_hex, 'hex'),
                    stage.server_version
             FROM jsonb_to_recordset($2::jsonb) AS stage(
                 row_ordinal bigint,
                 table_id text,
                 row_identity_hex text,
                 primary_key jsonb,
                 row_data jsonb,
                 row_checksum_hex text,
                 server_version text
             )",
            None,
            &[
                session.session_id.as_str().into(),
                pgrx::JsonB(serde_json::Value::Array(staged)).into(),
            ],
        )
        .map_err(|error| format!("staging rebuild rows: {error}"))?;
    Ok(())
}

fn load_stored_page(
    client: &SpiClient<'_>,
    session: &RebuildSession,
    row_ordinal: i64,
) -> Result<Option<RebuildResponse>, String> {
    let rows = client
        .select(
            "SELECT response
             FROM sync_rebuild_pages
             WHERE session_id = $1::uuid AND next_row_ordinal = $2",
            None,
            &[session.session_id.as_str().into(), row_ordinal.into()],
        )
        .map_err(|error| format!("loading stored rebuild page: {error}"))?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(None);
    };
    let response = row
        .get_by_name::<pgrx::JsonB, &str>("response")
        .map_err(|error| format!("reading stored rebuild page: {error}"))?
        .ok_or_else(|| "stored rebuild page is missing".to_string())?;
    let response: RebuildResponse = serde_json::from_value(response.0)
        .map_err(|error| format!("stored rebuild page is invalid: {error}"))?;
    if response.scope != session.scope_id {
        return Err("stored rebuild page has the wrong scope".to_string());
    }
    response
        .validate()
        .map_err(|error| format!("stored rebuild page violates the contract: {error}"))?;
    Ok(Some(response))
}

fn create_page(
    client: &mut SpiClient<'_>,
    user_id: &str,
    session: &RebuildSession,
    row_ordinal: i64,
) -> Result<RebuildResponse, String> {
    if row_ordinal < 0 || row_ordinal > session.staged_row_count {
        return Err("rebuild page ordinal is outside the staged snapshot".to_string());
    }
    let remaining = session.staged_row_count - row_ordinal;
    let count = remaining.min(session.page_limit);
    let rows = client
        .select(
            "SELECT row_ordinal, table_id, primary_key, row_data, row_checksum, server_version
             FROM sync_rebuild_staged_rows
             WHERE session_id = $1::uuid AND row_ordinal >= $2
             ORDER BY row_ordinal
             LIMIT $3",
            None,
            &[
                session.session_id.as_str().into(),
                row_ordinal.into(),
                count.into(),
            ],
        )
        .map_err(|error| format!("reading staged rebuild page: {error}"))?;
    if rows.len() != usize::try_from(count).map_err(|_| "rebuild page count is invalid")? {
        return Err("staged rebuild rows are incomplete".to_string());
    }
    let mut records = Vec::with_capacity(rows.len());
    for (offset, row) in rows.into_iter().enumerate() {
        let ordinal = row
            .get_by_name::<i64, &str>("row_ordinal")
            .map_err(|error| format!("reading staged rebuild ordinal: {error}"))?
            .ok_or_else(|| "staged rebuild ordinal is missing".to_string())?;
        if ordinal
            != row_ordinal + i64::try_from(offset).map_err(|_| "rebuild offset is invalid")?
        {
            return Err("staged rebuild ordinals are not contiguous".to_string());
        }
        let checksum = row
            .get_by_name::<Vec<u8>, &str>("row_checksum")
            .map_err(|error| format!("reading staged rebuild checksum: {error}"))?
            .ok_or_else(|| "staged rebuild checksum is missing".to_string())?;
        records.push(RebuildRecord {
            table: required_text(&row, "table_id", "rebuild ")?,
            pk: row
                .get_by_name::<pgrx::JsonB, &str>("primary_key")
                .map_err(|error| format!("reading staged rebuild primary key: {error}"))?
                .map(|value| value.0)
                .ok_or_else(|| "staged rebuild primary key is missing".to_string())?,
            row: row
                .get_by_name::<pgrx::JsonB, &str>("row_data")
                .map_err(|error| format!("reading staged rebuild row: {error}"))?
                .map(|value| value.0)
                .ok_or_else(|| "staged rebuild row is missing".to_string())?,
            row_checksum: ChecksumObject::new(decode_digest(
                checksum,
                "staged rebuild checksum must contain exactly 32 octets",
            )?),
            server_version: required_text(&row, "server_version", "rebuild ")?,
        });
    }
    let next_row_ordinal = row_ordinal
        .checked_add(count)
        .ok_or_else(|| "rebuild page ordinal overflow".to_string())?;
    let has_more = next_row_ordinal < session.staged_row_count;
    let (cursor, final_scope_cursor, checksum) = if has_more {
        let continuation = RebuildContinuation::new(RebuildContinuationInput {
            stream_generation: session.stream_generation.clone(),
            user_binding: user_id.to_string(),
            client_binding: session.client_id.clone(),
            client_generation: session.client_generation,
            scope_id: session.scope_id.clone(),
            schema_hash: session.schema_hash.clone(),
            membership_generation: session.membership_generation,
            retention_generation: session.retention_generation,
            session_id: session.session_id.clone(),
            rebuild_id: session.rebuild_id.clone(),
            snapshot_boundary: session.boundary.clone(),
            next_row_ordinal,
            page_limit: session.page_limit,
            accepted_write_epoch: session.accepted_write_epoch,
            issued_at: current_utc_timestamp(client, "rebuild token issue time", "rebuild ")?,
            expires_at: session.expires_at.clone(),
        });
        (
            Some(issue_rebuild_continuation(client, &continuation)?),
            None,
            None,
        )
    } else {
        let cursor_context = ScopeCursorContext::new(
            user_id,
            &session.client_id,
            session.client_generation,
            &session.scope_id,
            &session.schema_hash,
        )?;
        (
            None,
            Some(issue_scope_cursor(
                client,
                &cursor_context,
                &session.boundary,
            )?),
            Some(session.snapshot_checksum),
        )
    };
    let response = RebuildResponse {
        scope: session.scope_id.clone(),
        records,
        cursor,
        has_more,
        final_scope_cursor,
        checksum,
    };
    response
        .validate()
        .map_err(|error| format!("rebuild page violates the contract: {error}"))?;
    store_page(client, session, row_ordinal, &response)?;
    Ok(response)
}

fn store_page(
    client: &mut SpiClient<'_>,
    session: &RebuildSession,
    row_ordinal: i64,
    response: &RebuildResponse,
) -> Result<(), String> {
    let response = serde_json::to_value(response)
        .map_err(|error| format!("encoding rebuild page for replay: {error}"))?;
    client
        .update(
            "INSERT INTO sync_rebuild_pages (session_id, next_row_ordinal, response)
             VALUES ($1::uuid, $2, $3)",
            None,
            &[
                session.session_id.as_str().into(),
                row_ordinal.into(),
                pgrx::JsonB(response).into(),
            ],
        )
        .map_err(|error| format!("storing rebuild page replay: {error}"))?;
    Ok(())
}

fn required_positive_i64(row: &SpiHeapTupleData<'_>, name: &str) -> Result<i64, String> {
    row.get_by_name::<i64, &str>(name)
        .map_err(|error| format!("reading rebuild {name}: {error}"))?
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("rebuild {name} is invalid"))
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(HEX[usize::from(byte >> 4)] as char);
        value.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    value
}

fn response_json(response: RebuildResponse) -> pgrx::JsonB {
    pgrx::JsonB(
        serde_json::to_value(response)
            .unwrap_or_else(|error| pgrx::error!("encoding rebuild response: {error}")),
    )
}

fn invalid_request_response() -> pgrx::JsonB {
    protocol_error_response(
        ProtocolErrorCode::InvalidRequest,
        "invalid rebuild request",
        false,
    )
}

fn integrity_failure_response() -> pgrx::JsonB {
    protocol_error_response(
        ProtocolErrorCode::SyncIntegrityFailure,
        "rebuild state is inconsistent",
        false,
    )
}

fn integrity_failure_with_log(operation: &str, error: &impl std::fmt::Display) -> pgrx::JsonB {
    pgrx::warning!("rebuild {} failed: {}", operation, error);
    integrity_failure_response()
}

fn rebuild_restart_response(scope_id: &str) -> pgrx::JsonB {
    let response = ErrorResponse {
        error: ErrorBody {
            code: ProtocolErrorCode::RebuildRestartRequired,
            message: "rebuild session must restart".to_string(),
            retryable: false,
            current_schema: None,
            received_schema: None,
            current_client_generation: None,
            scope_id: Some(scope_id.to_string()),
            required_protocol_version: None,
            received_protocol_version: None,
            minimum_client_version: None,
            received_client_version: None,
            reason: None,
            field: None,
            minimum: None,
            maximum: None,
        },
    };
    response
        .validate()
        .unwrap_or_else(|error| pgrx::error!("encoding rebuild restart response: {error}"));
    pgrx::JsonB(
        serde_json::to_value(response)
            .unwrap_or_else(|error| pgrx::error!("encoding rebuild restart response: {error}")),
    )
}
