use pgrx::prelude::*;
use pgrx::spi::SpiClient;
use synchro_core::change::ChangeOperation;
use synchro_core::checksum::{
    row_digest, row_identity, CanonicalField, CanonicalRow, CanonicalTable, ChecksumObject,
    FieldSpec, PortableType, SchemaHash, ScopeDigestEntry, Sha256Digest,
};
use synchro_core::contract::{
    ChangeRecord, Operation, ProtocolErrorCode, PullRequest, PullResponse,
};
use synchro_core::limits::MAX_PULL_LIMIT;

use crate::client::{
    acquire_client_identity_lock, build_scope_delta, protocol_error_response, validate_schema_ref,
};
use crate::cursor_token::{
    issue_scope_cursor, parse_scope_cursor, ParsedScopeCursor, ScopeCursorContext,
};
use crate::registry::qualified_relation_name;
use crate::registry::TableRegistration;
use crate::stream_position::{load_materialized_boundary, StreamBoundary, StreamPosition};

/// Pull scoped changes for a client using per-scope cursors.
#[pg_extern(name = "synchro_pull")]
fn synchro_pull_contract(p_user_id: &str, p_request: pgrx::JsonB) -> pgrx::JsonB {
    if p_user_id.is_empty() {
        return protocol_error_response(
            ProtocolErrorCode::AuthRequired,
            "authentication is required",
            false,
        );
    }

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

    if let Err(error) = request.validate() {
        return protocol_error_response(
            ProtocolErrorCode::InvalidRequest,
            format!("invalid pull request: {error}"),
            false,
        );
    }
    if request.limit > i64::from(MAX_PULL_LIMIT) {
        return protocol_error_response(
            ProtocolErrorCode::InvalidRequest,
            format!("pull limit exceeds maximum {MAX_PULL_LIMIT}"),
            false,
        );
    }
    let limit = request.limit as usize;

    Spi::connect_mut(|client| {
        let _ = client.update(
            "SELECT set_config('app.user_id', $1, true)",
            None,
            &[p_user_id.into()],
        );

        acquire_client_identity_lock(client, p_user_id, &request.client_id);
        client
            .update("LOCK TABLE sync_wal_progress IN SHARE MODE", None, &[])
            .unwrap_or_else(|error| pgrx::error!("locking materialization boundary: {}", error));
        let boundary = match load_materialized_boundary(client) {
            Ok(boundary) => boundary,
            Err(error) => {
                return protocol_error_response(
                    ProtocolErrorCode::SyncIntegrityFailure,
                    error,
                    false,
                )
            }
        };

        let client_state =
            match crate::client::load_client_connect_state(client, p_user_id, &request.client_id) {
                Ok(state) => state,
                Err(err_json) => return err_json,
            };
        if request.client_generation != client_state.client_generation {
            return crate::client::client_generation_expired_response(
                client_state.client_generation,
            );
        }
        if let Err(error) = validate_schema_ref(client, &request.schema) {
            return error;
        }
        if client_has_pending_fence(client, p_user_id, &request.client_id) {
            return protocol_error_response(
                ProtocolErrorCode::CapturePending,
                "accepted writes are pending capture",
                true,
            );
        }

        let server_scopes = client_state.bucket_subs;
        let scope_set_version = client_state.scope_set_version;
        if request.scope_set_version > scope_set_version {
            return protocol_error_response(
                ProtocolErrorCode::InvalidRequest,
                "scope_set_version is ahead of the server",
                false,
            );
        }
        if !crate::client::scopes_were_assigned(
            client,
            p_user_id,
            &request.client_id,
            request.client_generation,
            request.scopes.keys(),
        ) {
            return protocol_error_response(
                ProtocolErrorCode::InvalidRequest,
                "pull contains an unknown scope",
                false,
            );
        }
        let scope_updates = build_scope_delta(&request.scopes, &server_scopes);
        let active_scopes_before_update = request.scopes.keys().cloned().collect();
        let active_scopes: Vec<String> = server_scopes
            .iter()
            .filter(|scope_id: &&String| request.scopes.contains_key(scope_id.as_str()))
            .cloned()
            .collect();
        if let Err(error) = lock_scope_states(client, &active_scopes) {
            return protocol_error_response(ProtocolErrorCode::SyncIntegrityFailure, error, false);
        }
        let (scope_positions, parsed_stale_scopes) =
            match parse_scope_positions(client, p_user_id, &request, &active_scopes, &boundary) {
                Ok(cursors) => cursors,
                Err(err_json) => return err_json,
            };
        let mut stale_scopes = scope_updates
            .add
            .iter()
            .map(|scope| scope.id.clone())
            .chain(parsed_stale_scopes)
            .collect::<Vec<_>>();
        stale_scopes.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        stale_scopes.dedup();
        let stale_lookup: std::collections::HashSet<&str> = stale_scopes
            .iter()
            .map(|scope_id| scope_id.as_str())
            .collect();
        let current_scopes: Vec<String> = active_scopes
            .iter()
            .filter(|scope_id| !stale_lookup.contains(scope_id.as_str()))
            .cloned()
            .collect();

        if current_scopes.is_empty() {
            let response = PullResponse {
                changes: Vec::new(),
                scope_set_version,
                scope_cursors: std::collections::BTreeMap::new(),
                scope_updates,
                rebuild: stale_scopes,
                has_more: false,
                checksums: match final_scope_checksums(client, &server_scopes, false) {
                    Ok(checksums) => checksums,
                    Err(error) => {
                        return protocol_error_response(
                            ProtocolErrorCode::SyncIntegrityFailure,
                            error,
                            false,
                        )
                    }
                },
            };

            if let Err(err) = response.validate_for_active_scopes(&active_scopes_before_update) {
                pgrx::error!("invalid pull response: {}", err);
            }

            return pgrx::JsonB(serde_json::to_value(response).unwrap());
        }

        let registry = load_registry_inner(client);
        let per_scope_limit = i64::try_from(limit + 1).unwrap_or(i64::MAX);
        let mut streams = std::collections::BTreeMap::new();
        for scope_id in &current_scopes {
            let position = scope_positions
                .get(scope_id)
                .expect("current scope has a parsed position");
            let candidates = match query_scope_candidates(
                client,
                &registry,
                scope_id,
                position,
                &boundary,
                per_scope_limit,
            ) {
                Ok(candidates) => candidates,
                Err(error) => {
                    return protocol_error_response(
                        ProtocolErrorCode::SyncIntegrityFailure,
                        error,
                        false,
                    )
                }
            };
            streams.insert(scope_id.clone(), candidates);
        }

        let mut merged = streams
            .values()
            .flat_map(|candidates| candidates.iter().cloned())
            .collect::<Vec<_>>();
        merged.sort_by(compare_candidates);
        let has_more = merged.len() > limit;
        merged.truncate(limit);
        let entries = merged
            .iter()
            .map(|candidate| candidate.entry.clone())
            .collect::<Vec<_>>();
        let changes = match build_contract_changes(client, &registry, &entries) {
            Ok(changes) => changes,
            Err(message) => {
                return protocol_error_response(
                    ProtocolErrorCode::SyncIntegrityFailure,
                    message,
                    false,
                );
            }
        };

        let mut selected_by_scope = std::collections::HashMap::<&str, Vec<&PullCandidate>>::new();
        for candidate in &merged {
            selected_by_scope
                .entry(candidate.entry.bucket_id.as_str())
                .or_default()
                .push(candidate);
        }
        let mut scope_cursors = std::collections::BTreeMap::new();
        for scope_id in &current_scopes {
            let selected = selected_by_scope
                .get(scope_id.as_str())
                .map(Vec::as_slice)
                .unwrap_or_default();
            let remaining = streams
                .get(scope_id)
                .is_some_and(|candidates| candidates.len() > selected.len());
            let safe_position = if remaining {
                selected.last().map(|candidate| &candidate.entry.position)
            } else {
                Some(&boundary.position)
            };
            if let Some(safe_position) = safe_position {
                let context = ScopeCursorContext::new(
                    p_user_id,
                    &request.client_id,
                    request.client_generation,
                    scope_id,
                    &request.schema.hash,
                )
                .unwrap_or_else(|error| pgrx::error!("building scope cursor context: {error}"));
                let next_cursor = issue_scope_cursor(client, &context, safe_position)
                    .unwrap_or_else(|err| pgrx::error!("issuing scope cursor: {}", err));
                scope_cursors.insert(scope_id.clone(), next_cursor);
            }
        }

        let response = PullResponse {
            changes,
            scope_set_version,
            scope_cursors,
            scope_updates,
            rebuild: stale_scopes,
            has_more,
            checksums: match final_scope_checksums(client, &server_scopes, has_more) {
                Ok(checksums) => checksums,
                Err(error) => {
                    return protocol_error_response(
                        ProtocolErrorCode::SyncIntegrityFailure,
                        error,
                        false,
                    )
                }
            },
        };

        if let Err(err) = response.validate_for_active_scopes(&active_scopes_before_update) {
            pgrx::error!("invalid pull response: {}", err);
        }

        if let Err(error) = acknowledge_scope_positions(
            client,
            p_user_id,
            &request.client_id,
            &scope_positions,
            &boundary.stream_generation,
        ) {
            pgrx::error!("acknowledging scope positions: {}", error);
        }

        pgrx::JsonB(serde_json::to_value(response).unwrap())
    })
}

pub(crate) fn client_has_pending_fence(
    client: &SpiClient<'_>,
    user_id: &str,
    client_id: &str,
) -> bool {
    client
        .select(
            "SELECT EXISTS (
                 SELECT 1
                 FROM sync_write_fences
                 WHERE user_id = $1
                   AND client_id = $2
                   AND mutation_id IS NOT NULL
                   AND coverage = 'pending'
             ) AS pending",
            None,
            &[user_id.into(), client_id.into()],
        )
        .unwrap_or_else(|error| pgrx::error!("checking pending write fences: {}", error))
        .first()
        .get_by_name::<bool, &str>("pending")
        .unwrap_or_else(|error| pgrx::error!("reading pending write-fence state: {}", error))
        .unwrap_or(true)
}

// ---------------------------------------------------------------------------
// Internal helpers (pub(crate) for use by rebuild.rs)
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct RawChangelogEntry {
    bucket_id: String,
    table_name: String,
    record_id: String,
    operation: ChangeOperation,
    position: StreamPosition,
    relation_id: String,
    row_version: Option<String>,
    projection_image: Option<String>,
    projection_record_id: Option<String>,
    projection_row: Option<serde_json::Value>,
    projection_row_version: Option<String>,
    projection_registry_generation: Option<i64>,
    projection_checksum: Option<Sha256Digest>,
    projection_deleted: Option<bool>,
}

#[derive(Clone)]
struct PullCandidate {
    entry: RawChangelogEntry,
    logical_table_id: String,
    typed_primary_key: Vec<u8>,
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

fn parse_scope_positions(
    client: &SpiClient<'_>,
    user_id: &str,
    request: &PullRequest,
    active_scopes: &[String],
    boundary: &StreamBoundary,
) -> Result<
    (
        std::collections::HashMap<String, StreamPosition>,
        Vec<String>,
    ),
    pgrx::JsonB,
> {
    let mut positions = std::collections::HashMap::new();
    let mut stale_scopes = Vec::new();

    for scope_id in active_scopes {
        let cursor = request
            .scopes
            .get(scope_id)
            .and_then(|scope_ref| scope_ref.cursor.as_ref());
        let Some(cursor) = cursor else {
            stale_scopes.push(scope_id.clone());
            continue;
        };

        let context = ScopeCursorContext::new(
            user_id,
            &request.client_id,
            request.client_generation,
            scope_id,
            &request.schema.hash,
        )
        .map_err(|_| {
            protocol_error_response(
                ProtocolErrorCode::InvalidRequest,
                "scope cursor context is invalid",
                false,
            )
        })?;
        match parse_scope_cursor(client, &context, cursor) {
            Ok(ParsedScopeCursor::Current(position)) if position <= boundary.position => {
                positions.insert(scope_id.clone(), position);
            }
            Ok(ParsedScopeCursor::Current(_)) => stale_scopes.push(scope_id.clone()),
            Ok(ParsedScopeCursor::Stale) => stale_scopes.push(scope_id.clone()),
            Err(err) => {
                return Err(protocol_error_response(
                    ProtocolErrorCode::InvalidRequest,
                    format!("scope {scope_id} cursor is invalid: {err}"),
                    false,
                ));
            }
        }
    }

    Ok((positions, stale_scopes))
}

fn acknowledge_scope_positions(
    client: &mut SpiClient<'_>,
    user_id: &str,
    client_id: &str,
    positions: &std::collections::HashMap<String, StreamPosition>,
    stream_generation: &str,
) -> Result<(), String> {
    let mut scopes = positions.keys().collect::<Vec<_>>();
    scopes.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    let mut advanced = false;
    for scope_id in scopes {
        let presented = &positions[scope_id];
        let stored_rows = client
            .select(
                "SELECT stream_generation, position_kind, commit_lsn::text AS commit_lsn,
                        event_ordinal, effect_ordinal
                 FROM sync_client_checkpoints
                 WHERE user_id = $1 AND client_id = $2 AND bucket_id = $3
                 FOR UPDATE",
                None,
                &[user_id.into(), client_id.into(), scope_id.as_str().into()],
            )
            .map_err(|error| format!("locking scope checkpoint: {error}"))?;
        let stored = if let Some(row) = stored_rows.into_iter().next() {
            let stored_generation = row
                .get_by_name::<String, &str>("stream_generation")
                .map_err(|error| format!("reading checkpoint stream generation: {error}"))?
                .ok_or_else(|| "checkpoint stream generation is missing".to_string())?;
            if stored_generation != stream_generation {
                return Err("checkpoint has the wrong stream generation".to_string());
            }
            let kind = row
                .get_by_name::<String, &str>("position_kind")
                .map_err(|error| format!("reading checkpoint position kind: {error}"))?
                .ok_or_else(|| "checkpoint position kind is missing".to_string())?;
            let commit_lsn = row
                .get_by_name::<String, &str>("commit_lsn")
                .map_err(|error| format!("reading checkpoint commit LSN: {error}"))?;
            let event_ordinal = row
                .get_by_name::<i64, &str>("event_ordinal")
                .map_err(|error| format!("reading checkpoint event ordinal: {error}"))?;
            let effect_ordinal = row
                .get_by_name::<i32, &str>("effect_ordinal")
                .map_err(|error| format!("reading checkpoint effect ordinal: {error}"))?;
            StreamPosition::from_sql_parts(
                &kind,
                commit_lsn.as_deref(),
                event_ordinal,
                effect_ordinal,
            )?
        } else {
            StreamPosition::GenerationStart
        };
        if presented <= &stored {
            continue;
        }

        let commit_lsn = presented.commit_lsn();
        client
            .update(
                "INSERT INTO sync_client_checkpoints (
                     user_id, client_id, bucket_id, stream_generation,
                     position_kind, commit_lsn, event_ordinal, effect_ordinal
                 ) VALUES ($1, $2, $3, $4, $5, $6::pg_lsn, $7, $8)
                 ON CONFLICT (user_id, client_id, bucket_id) DO UPDATE
                 SET stream_generation = EXCLUDED.stream_generation,
                     position_kind = EXCLUDED.position_kind,
                     commit_lsn = EXCLUDED.commit_lsn,
                     event_ordinal = EXCLUDED.event_ordinal,
                     effect_ordinal = EXCLUDED.effect_ordinal,
                     updated_at = now()",
                None,
                &[
                    user_id.into(),
                    client_id.into(),
                    scope_id.as_str().into(),
                    stream_generation.into(),
                    presented.kind().into(),
                    commit_lsn.as_deref().into(),
                    presented.event_ordinal().into(),
                    presented.effect_ordinal().into(),
                ],
            )
            .map_err(|error| format!("persisting scope checkpoint: {error}"))?;
        advanced = true;
    }
    if advanced {
        client
            .update(
                "UPDATE sync_clients
                 SET last_acknowledged_at = now(), updated_at = now()
                 WHERE user_id = $1 AND client_id = $2",
                None,
                &[user_id.into(), client_id.into()],
            )
            .map_err(|error| format!("updating client acknowledgement time: {error}"))?;
    }
    Ok(())
}

fn final_scope_checksums(
    client: &SpiClient<'_>,
    scope_ids: &[String],
    has_more: bool,
) -> Result<Option<std::collections::BTreeMap<String, ChecksumObject>>, String> {
    if has_more {
        return Ok(None);
    }

    let checksums = compute_bucket_checksums(client, scope_ids)?;
    let mut response = std::collections::BTreeMap::new();
    for scope_id in scope_ids {
        let checksum = checksums
            .get(scope_id)
            .copied()
            .ok_or_else(|| format!("scope digest is missing for scope {scope_id}"))?;
        response.insert(scope_id.clone(), checksum);
    }

    Ok(Some(response))
}

pub(crate) fn canonicalize_synced_row_data(
    table_reg: &TableRegistration,
    data: &mut serde_json::Value,
) -> Result<(), String> {
    if let Some(obj) = data.as_object_mut() {
        for field in &table_reg.fields {
            if let Some(value) = obj.remove(&field.physical_column) {
                if obj.contains_key(&field.field_id) {
                    return Err(format!(
                        "row contains both physical and logical field {:?}",
                        field.physical_column
                    ));
                }
                obj.insert(field.field_id.clone(), value);
            }
        }

        for field in &table_reg.fields {
            let Some(value) = obj.get_mut(&field.field_id) else {
                continue;
            };
            if field.portable_type == "json" {
                if let Some(json_text) = value.as_str() {
                    let parsed: serde_json::Value =
                        serde_json::from_str(json_text).map_err(|error| {
                            format!("field {:?} has invalid JSON: {error}", field.field_id)
                        })?;
                    let canonical = serde_json_canonicalizer::to_vec(&parsed).map_err(|error| {
                        format!(
                            "field {:?} JSON is not canonicalizable: {error}",
                            field.field_id
                        )
                    })?;
                    *value = serde_json::Value::String(String::from_utf8(canonical).map_err(
                        |error| format!("field {:?} JSON is not UTF-8: {error}", field.field_id),
                    )?);
                }
            } else if field.portable_type == "float" {
                let canonical = serde_json_canonicalizer::to_vec(value).map_err(|error| {
                    format!(
                        "field {:?} float is not canonicalizable: {error}",
                        field.field_id
                    )
                })?;
                *value = serde_json::from_slice(&canonical).map_err(|error| {
                    format!("field {:?} float is invalid: {error}", field.field_id)
                })?;
            }
        }
        Ok(())
    } else {
        Err("wire row is not a JSON object".to_string())
    }
}

pub(crate) fn canonical_table(table_reg: &TableRegistration) -> Result<CanonicalTable, String> {
    let fields = table_reg
        .fields
        .iter()
        .map(|field| {
            let portable_type = PortableType::parse(&field.portable_type)
                .map_err(|error| format!("field {:?} type is invalid: {error}", field.field_id))?;
            let spec = FieldSpec::new(
                portable_type,
                field.nullable,
                field.decimal_precision.map(|value| value as u32),
                field.decimal_scale.map(|value| value as u32),
            )
            .map_err(|error| {
                format!(
                    "field {:?} specification is invalid: {error}",
                    field.field_id
                )
            })?;
            CanonicalField::new(field.field_id.clone(), spec)
                .map_err(|error| format!("field {:?} identity is invalid: {error}", field.field_id))
        })
        .collect::<Result<Vec<_>, _>>()?;
    CanonicalTable::new(
        table_reg.table_id.clone(),
        table_reg.primary_key_field_id.clone(),
        fields,
    )
    .map_err(|error| format!("table {:?} is invalid: {error}", table_reg.table_name))
}

pub(crate) fn schema_hash_for_generation(
    client: &SpiClient<'_>,
    registry_generation: i64,
) -> Result<SchemaHash, String> {
    let hash = client
        .select(
            "SELECT schema_hash
             FROM (
                 SELECT schema_hash, registry_generation, schema_version
                 FROM synchro.sync_schema_manifest
                 WHERE registry_generation <= $1
                 UNION ALL
                 SELECT target_schema_hash AS schema_hash,
                        target_registry_generation AS registry_generation,
                        target_schema_version AS schema_version
                 FROM synchro.sync_stream_resets
                 WHERE operation_kind = 'projection_bootstrap'
                   AND target_registry_generation = $1
                   AND lifecycle IN ('preparing', 'baseline_staged', 'catching_up', 'activated')
             ) schema_reference
             ORDER BY registry_generation DESC, schema_version DESC
             LIMIT 1",
            None,
            &[registry_generation.into()],
        )
        .map_err(|_| "loading immutable schema hash failed".to_string())?
        .first()
        .get_by_name::<String, &str>("schema_hash")
        .map_err(|_| "reading immutable schema hash failed".to_string())?
        .ok_or_else(|| "immutable schema hash is missing".to_string())?;
    SchemaHash::from_lower_hex(&hash)
        .map_err(|error| format!("immutable schema hash is invalid: {error}"))
}

pub(crate) fn row_primary_key_json(
    table_reg: &TableRegistration,
    record_id: &str,
) -> Result<serde_json::Value, String> {
    match table_reg.pk_portable_type.as_str() {
        "string" => Ok(serde_json::Value::String(record_id.to_string())),
        "int" => serde_json::from_str(record_id)
            .map_err(|error| format!("record identity is not a canonical int: {error}")),
        "int64" => Ok(serde_json::Value::String(record_id.to_string())),
        portable_type => Err(format!("unsupported primary-key type {portable_type:?}")),
    }
}

pub(crate) fn typed_primary_key_bytes(
    table_reg: &TableRegistration,
    record_id: &str,
) -> Result<Vec<u8>, String> {
    let primary_key_json = row_primary_key_json(table_reg, record_id)?;
    row_identity(
        &canonical_table(table_reg)?,
        &serde_json::to_string(&primary_key_json)
            .map_err(|error| format!("encoding primary key: {error}"))?,
    )
    .map(|identity| identity.into_bytes())
    .map_err(|error| format!("encoding row identity: {error}"))
}

pub(crate) fn synced_row_digest(
    client: &SpiClient<'_>,
    table_reg: &TableRegistration,
    data: &serde_json::Value,
    record_id: &str,
    server_version: &str,
) -> Result<Sha256Digest, String> {
    let mut canonical = data.clone();
    canonicalize_synced_row_data(table_reg, &mut canonical)?;
    let table = canonical_table(table_reg)?;
    let primary_key_json = row_primary_key_json(table_reg, record_id)?;
    let row = CanonicalRow::from_json(
        serde_json::to_string(&primary_key_json)
            .map_err(|error| format!("encoding primary key: {error}"))?,
        &serde_json::to_string(&canonical)
            .map_err(|error| format!("encoding wire row: {error}"))?,
    )
    .map_err(|error| format!("canonical row is invalid: {error}"))?;
    let schema_hash = schema_hash_for_generation(client, table_reg.registry_generation)?;
    row_digest(schema_hash, &table, &row, server_version)
        .map_err(|error| format!("computing row digest: {error}"))
}

/// Load registry within an existing SPI context.
fn load_registry_inner(client: &SpiClient<'_>) -> Vec<TableRegistration> {
    crate::registry::load_registry_from_client(client)
        .unwrap_or_else(|e| pgrx::error!("loading registry: {}", e))
}

fn query_scope_candidates(
    client: &SpiClient<'_>,
    registry: &[TableRegistration],
    scope_id: &str,
    after: &StreamPosition,
    boundary: &StreamBoundary,
    limit: i64,
) -> Result<Vec<PullCandidate>, String> {
    let malformed = client
        .select(
            "SELECT EXISTS (
                 SELECT 1
                 FROM sync_changelog
                 WHERE bucket_id = $1
                   AND (stream_generation IS NULL
                        OR stream_generation = $2 AND (
                            commit_lsn IS NULL
                            OR event_ordinal IS NULL
                            OR event_ordinal < 0
                            OR effect_ordinal IS NULL
                            OR effect_ordinal < 0
                            OR relation_id IS NULL
                            OR row_version IS NULL
                            OR NOT EXISTS (
                                SELECT 1
                                FROM sync_wal_events event
                                JOIN sync_wal_transactions transaction
                                  ON transaction.stream_generation = event.stream_generation
                                 AND transaction.commit_lsn = event.commit_lsn
                                WHERE event.stream_generation = sync_changelog.stream_generation
                                  AND event.commit_lsn = sync_changelog.commit_lsn
                                  AND event.event_ordinal = sync_changelog.event_ordinal
                                  AND event.relation_id = sync_changelog.relation_id
                            )
                        ))
             ) AS malformed",
            None,
            &[scope_id.into(), boundary.stream_generation.as_str().into()],
        )
        .map_err(|error| format!("validating scope changelog: {error}"))?
        .first()
        .get_by_name::<bool, &str>("malformed")
        .map_err(|error| format!("reading scope changelog validity: {error}"))?
        .unwrap_or(true);
    if malformed {
        return Err("scope changelog contains a malformed stream position".to_string());
    }
    let StreamPosition::TransactionEnd { .. } = boundary.position else {
        return Ok(Vec::new());
    };
    let after_lsn = after.commit_lsn().unwrap_or_else(|| "0/0".to_string());
    let after_event = after.event_ordinal().unwrap_or(0);
    let after_effect = after.effect_ordinal().unwrap_or(0);
    let boundary_lsn = boundary
        .position
        .commit_lsn()
        .ok_or_else(|| "materialization boundary has no commit LSN".to_string())?;
    let tup_table = client
        .select(
            "WITH eligible AS (
                 SELECT c.bucket_id, c.table_name, c.record_id, c.operation,
                        c.commit_lsn::text AS commit_lsn,
                        c.event_ordinal, c.effect_ordinal,
                        c.relation_id::text AS relation_id,
                        c.row_version::text AS row_version, c.projection_image,
                        p.record_id AS projection_record_id, p.row_data AS projection_row,
                        p.registry_generation AS projection_registry_generation,
                        p.row_version::text AS projection_row_version,
                        p.checksum AS projection_checksum, p.deleted AS projection_deleted
                 FROM sync_changelog c
                 LEFT JOIN sync_captured_projections p
                   ON p.stream_generation = c.stream_generation
                  AND p.commit_lsn = c.commit_lsn
                  AND p.event_ordinal = c.event_ordinal
                  AND p.relation_id = c.relation_id
                  AND p.image_kind = c.projection_image
                 WHERE c.bucket_id = $1
                   AND c.stream_generation = $2
                   AND c.commit_lsn IS NOT NULL
                   AND c.event_ordinal IS NOT NULL
                   AND c.effect_ordinal IS NOT NULL
                   AND c.relation_id IS NOT NULL
                   AND c.commit_lsn <= $7::pg_lsn
                   AND (
                       $3 = 'generation_start'
                       OR ($3 = 'transaction_end' AND c.commit_lsn > $4::pg_lsn)
                       OR ($3 = 'effect' AND
                           (c.commit_lsn, c.event_ordinal, c.effect_ordinal) >
                           ($4::pg_lsn, $5::bigint, $6::integer))
                   )
             ), retained AS (
                 SELECT DISTINCT ON (relation_id, record_id) *
                 FROM eligible
                 ORDER BY relation_id, record_id,
                          commit_lsn::pg_lsn DESC, event_ordinal DESC, effect_ordinal DESC
             )
             SELECT * FROM retained
             ORDER BY commit_lsn::pg_lsn, event_ordinal, effect_ordinal
             LIMIT $8",
            None,
            &[
                scope_id.into(),
                boundary.stream_generation.as_str().into(),
                after.kind().into(),
                after_lsn.as_str().into(),
                after_event.into(),
                after_effect.into(),
                boundary_lsn.as_str().into(),
                limit.into(),
            ],
        )
        .map_err(|error| format!("querying scope changelog: {error}"))?;

    let mut entries = Vec::new();
    for row in tup_table {
        let bucket_id: String = match row.get_by_name::<String, &str>("bucket_id") {
            Ok(Some(v)) if !v.is_empty() => v,
            _ => return Err("changelog bucket is malformed".to_string()),
        };
        let table_name: String = match row.get_by_name::<String, &str>("table_name") {
            Ok(Some(v)) if !v.is_empty() => v,
            _ => return Err("changelog table is malformed".to_string()),
        };
        let record_id: String = match row.get_by_name::<String, &str>("record_id") {
            Ok(Some(v)) if !v.is_empty() => v,
            _ => return Err("changelog record identity is malformed".to_string()),
        };
        let op_i16: i16 = row
            .get_by_name::<i16, &str>("operation")
            .map_err(|_| "changelog operation is malformed".to_string())?
            .ok_or_else(|| "changelog operation is null".to_string())?;
        let operation = match ChangeOperation::from_i16(op_i16) {
            Some(op) => op,
            None => return Err(format!("changelog operation {op_i16} is invalid")),
        };
        let commit_lsn = row
            .get_by_name::<String, &str>("commit_lsn")
            .map_err(|_| "changelog commit LSN is malformed".to_string())?
            .ok_or_else(|| "changelog commit LSN is null".to_string())?;
        let event_ordinal = row
            .get_by_name::<i64, &str>("event_ordinal")
            .map_err(|_| "changelog event ordinal is malformed".to_string())?
            .ok_or_else(|| "changelog event ordinal is null".to_string())?;
        let effect_ordinal = row
            .get_by_name::<i32, &str>("effect_ordinal")
            .map_err(|_| "changelog effect ordinal is malformed".to_string())?
            .ok_or_else(|| "changelog effect ordinal is null".to_string())?;
        let position = StreamPosition::effect(&commit_lsn, event_ordinal, effect_ordinal)?;
        let relation_id = row
            .get_by_name::<String, &str>("relation_id")
            .map_err(|_| "changelog relation identity is malformed".to_string())?
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "changelog relation identity is null".to_string())?;
        let row_version = row
            .get_by_name::<String, &str>("row_version")
            .map_err(|_| "changelog row version is malformed".to_string())?;
        if row_version.as_deref().is_none_or(str::is_empty) {
            return Err("changelog row version is missing".to_string());
        }
        let projection_image = row
            .get_by_name::<String, &str>("projection_image")
            .map_err(|_| "changelog projection image is malformed".to_string())?;
        if let Some(image) = projection_image.as_deref() {
            if !matches!(image, "before" | "after") {
                return Err("changelog projection image is invalid".to_string());
            }
        }
        let projection_record_id = row
            .get_by_name::<String, &str>("projection_record_id")
            .map_err(|_| "captured projection record identity is malformed".to_string())?;
        let projection_row = row
            .get_by_name::<pgrx::JsonB, &str>("projection_row")
            .map_err(|_| "captured projection row is malformed".to_string())?
            .map(|value| value.0);
        let projection_registry_generation = row
            .get_by_name::<i64, &str>("projection_registry_generation")
            .map_err(|_| "captured projection registry generation is malformed".to_string())?;
        let projection_checksum = row
            .get_by_name::<Vec<u8>, &str>("projection_checksum")
            .map_err(|_| "captured projection digest is malformed".to_string())?
            .map(|bytes| decode_digest(bytes, "captured projection digest"))
            .transpose()?;
        let projection_row_version = row
            .get_by_name::<String, &str>("projection_row_version")
            .map_err(|_| "captured projection row version is malformed".to_string())?;
        let projection_deleted = row
            .get_by_name::<bool, &str>("projection_deleted")
            .map_err(|_| "captured projection deletion state is malformed".to_string())?;

        let entry = RawChangelogEntry {
            bucket_id,
            table_name,
            record_id,
            operation,
            position,
            relation_id,
            row_version,
            projection_image,
            projection_record_id,
            projection_row,
            projection_registry_generation,
            projection_row_version,
            projection_checksum,
            projection_deleted,
        };
        let table = registry
            .iter()
            .find(|table| table.relation_id == entry.relation_id)
            .ok_or_else(|| "pull effect relation is not registered".to_string())?;
        if table.table_name != entry.table_name {
            return Err("pull effect table identity is inconsistent".to_string());
        }
        let typed_primary_key = typed_primary_key_bytes(table, &entry.record_id)?;
        entries.push(PullCandidate {
            entry,
            logical_table_id: table.table_id.clone(),
            typed_primary_key,
        });
    }
    entries.sort_by(compare_candidates);
    Ok(entries)
}

pub(crate) fn lock_scope_states(
    client: &SpiClient<'_>,
    scope_ids: &[String],
) -> Result<(), String> {
    if scope_ids.is_empty() {
        return Ok(());
    }
    let rows = client
        .select(
            "SELECT scope_id
             FROM sync_scope_state
             WHERE scope_id = ANY($1)
             ORDER BY scope_id
             FOR SHARE",
            None,
            &[scope_ids.to_vec().into()],
        )
        .map_err(|error| format!("locking scope state: {error}"))?;
    if rows.len() != scope_ids.len() {
        return Err("assigned scope generation state is missing".to_string());
    }
    Ok(())
}

fn compare_candidates(left: &PullCandidate, right: &PullCandidate) -> std::cmp::Ordering {
    left.entry
        .position
        .cmp(&right.entry.position)
        .then_with(|| {
            left.entry
                .bucket_id
                .as_bytes()
                .cmp(right.entry.bucket_id.as_bytes())
        })
        .then_with(|| {
            left.logical_table_id
                .as_bytes()
                .cmp(right.logical_table_id.as_bytes())
        })
        .then_with(|| left.typed_primary_key.cmp(&right.typed_primary_key))
        .then_with(|| {
            operation_rank(left.entry.operation).cmp(&operation_rank(right.entry.operation))
        })
}

const fn operation_rank(operation: ChangeOperation) -> u8 {
    match operation {
        ChangeOperation::Delete => 0,
        ChangeOperation::Insert | ChangeOperation::Update => 1,
    }
}

fn build_contract_changes(
    client: &SpiClient<'_>,
    registry: &[TableRegistration],
    entries: &[RawChangelogEntry],
) -> Result<Vec<ChangeRecord>, String> {
    let mut changes = Vec::with_capacity(entries.len());
    for entry in entries {
        let table = registry
            .iter()
            .find(|table| table.table_name == entry.table_name)
            .ok_or_else(|| "pull effect table is not registered".to_string())?;
        let server_version = entry
            .row_version
            .clone()
            .ok_or_else(|| "pull effect has no opaque server version".to_string())?;
        if entry.projection_image.is_some()
            && entry.projection_row_version.as_deref() != Some(server_version.as_str())
        {
            return Err("pull effect and captured projection versions differ".to_string());
        }
        let pk = contract_pk_value(registry, &entry.table_name, &entry.record_id);
        match entry.operation {
            ChangeOperation::Insert | ChangeOperation::Update => {
                if entry.projection_image.as_deref() != Some("after")
                    || entry.projection_record_id.as_deref() != Some(entry.record_id.as_str())
                    || entry.projection_deleted != Some(false)
                {
                    return Err("pull upsert has no valid captured projection".to_string());
                }
                let row = entry
                    .projection_row
                    .clone()
                    .ok_or_else(|| "pull upsert projection is missing".to_string())?;
                let checksum = entry
                    .projection_checksum
                    .ok_or_else(|| "pull upsert row digest is missing".to_string())?;
                let projection_generation =
                    entry.projection_registry_generation.ok_or_else(|| {
                        "pull upsert projection registry generation is missing".to_string()
                    })?;
                let projection_table = crate::registry::load_registry_generation_from_client(
                    client,
                    projection_generation,
                )
                .map_err(|_| "pull upsert projection registry is unavailable".to_string())?
                .into_iter()
                .find(|candidate| candidate.table_name == entry.table_name)
                .ok_or_else(|| "pull upsert projection table is unavailable".to_string())?;
                let computed = synced_row_digest(
                    client,
                    &projection_table,
                    &row,
                    &entry.record_id,
                    &server_version,
                )?;
                if computed != checksum {
                    return Err("pull upsert projection row digest does not match".to_string());
                }
                changes.push(ChangeRecord {
                    scope: entry.bucket_id.clone(),
                    table: table.table_id.clone(),
                    op: Operation::Upsert,
                    pk,
                    row: Some(row),
                    row_checksum: Some(ChecksumObject::new(checksum)),
                    server_version,
                });
            }
            ChangeOperation::Delete => {
                let (row, row_checksum) = if entry.projection_image.is_some() {
                    if entry.projection_image.as_deref() != Some("after")
                        || entry.projection_record_id.as_deref() != Some(entry.record_id.as_str())
                        || entry.projection_deleted != Some(true)
                    {
                        return Err("pull delete has an invalid captured tombstone".to_string());
                    }
                    let row = entry
                        .projection_row
                        .clone()
                        .ok_or_else(|| "pull delete tombstone is missing".to_string())?;
                    let checksum = entry
                        .projection_checksum
                        .ok_or_else(|| "pull delete row digest is missing".to_string())?;
                    let projection_generation =
                        entry.projection_registry_generation.ok_or_else(|| {
                            "pull delete projection registry generation is missing".to_string()
                        })?;
                    let projection_table = crate::registry::load_registry_generation_from_client(
                        client,
                        projection_generation,
                    )
                    .map_err(|_| "pull delete projection registry is unavailable".to_string())?
                    .into_iter()
                    .find(|candidate| candidate.table_name == entry.table_name)
                    .ok_or_else(|| "pull delete projection table is unavailable".to_string())?;
                    let computed = synced_row_digest(
                        client,
                        &projection_table,
                        &row,
                        &entry.record_id,
                        &server_version,
                    )?;
                    if computed != checksum {
                        return Err("pull delete projection row digest does not match".to_string());
                    }
                    (Some(row), Some(ChecksumObject::new(checksum)))
                } else {
                    (None, None)
                };
                changes.push(ChangeRecord {
                    scope: entry.bucket_id.clone(),
                    table: table.table_id.clone(),
                    op: Operation::Delete,
                    pk,
                    row,
                    row_checksum,
                    server_version,
                });
            }
        }
    }
    Ok(changes)
}

/// Hydrate records from a single table.
///
/// Returns JSON objects with the logical wire row, row digest, and server version.
pub(crate) fn hydrate_records(
    client: &SpiClient<'_>,
    table_name: &str,
    ids: &[&str],
    registry: &[TableRegistration],
) -> Result<Vec<serde_json::Value>, String> {
    let table_reg = registry
        .iter()
        .find(|table| table.table_name == table_name)
        .ok_or_else(|| format!("table {table_name:?} is not registered during hydration"))?;

    if ids.is_empty() {
        return Ok(vec![]);
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
         {deleted_at} AS deleted_at, \
          v.row_version::text AS row_version \
         FROM {table} t \
         LEFT JOIN sync_row_versions v \
           ON v.relation_id = $2::uuid AND v.record_id = t.{pk}::text \
         WHERE t.{pk}::text = ANY($1)",
        pk = pg_quote_ident(&table_reg.pk_column),
        projection = synced_row_projection_sql(table_reg, "t"),
        updated_at = updated_at_expr,
        deleted_at = deleted_at_expr,
        table = qualified_relation_name(&table_reg.physical_schema, &table_reg.physical_relation,),
    );

    let id_arr: Vec<String> = ids.iter().map(|s| s.to_string()).collect();
    let tup_table = client
        .select(
            &query,
            None,
            &[id_arr.into(), table_reg.relation_id.as_str().into()],
        )
        .map_err(|error| format!("hydration failed for table {table_name}: {error}"))?;

    let mut records = Vec::new();
    for row in tup_table {
        let id: String = row
            .get_by_name::<String, &str>("id")
            .map_err(|error| format!("reading hydrated row identity: {error}"))?
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "hydrated row identity is missing".to_string())?;
        let data_str: String = row
            .get_by_name::<String, &str>("data")
            .map_err(|error| format!("reading hydrated row data: {error}"))?
            .ok_or_else(|| format!("row {table_name}.{id} has no hydrated data"))?;
        let updated_at: Option<String> = row
            .get_by_name::<String, &str>("updated_at")
            .map_err(|error| format!("reading hydrated update time: {error}"))?;
        let deleted_at: Option<String> = row
            .get_by_name::<String, &str>("deleted_at")
            .map_err(|error| format!("reading hydrated deletion time: {error}"))?;
        let row_version = row
            .get_by_name::<String, &str>("row_version")
            .map_err(|error| format!("reading hydrated row version: {error}"))?
            .filter(|value| !value.is_empty())
            .ok_or_else(|| format!("row {table_name}.{id} has no server version"))?;

        let mut data: serde_json::Value = serde_json::from_str(&data_str)
            .map_err(|error| format!("row {table_name}.{id} is invalid JSON: {error}"))?;
        canonicalize_synced_row_data(table_reg, &mut data)
            .map_err(|error| format!("row {table_name}.{id} is not canonical: {error}"))?;
        let digest = synced_row_digest(client, table_reg, &data, &id, &row_version)
            .map_err(|error| format!("row {table_name}.{id} digest failed: {error}"))?;
        let row_checksum = ChecksumObject::new(digest);

        let mut record = serde_json::json!({
            "id": id,
            "table_name": table_name,
            "data": data,
            "row_checksum": row_checksum,
            "server_version": row_version,
        });

        if let Some(ua) = updated_at {
            record["updated_at"] = serde_json::json!(ua);
        }
        if let Some(da) = deleted_at {
            record["deleted_at"] = serde_json::json!(da);
        }
        records.push(record);
    }
    Ok(records)
}

pub(crate) fn synced_row_projection_sql(table_reg: &TableRegistration, row_alias: &str) -> String {
    let qualified = |column: &str| format!("{row_alias}.{}", pg_quote_ident(column));
    let pairs = table_reg
        .fields
        .iter()
        .map(|field| {
            let column = qualified(&field.physical_column);
            let expression = match field.portable_type.as_str() {
                "string" => format!("to_jsonb(({column})::text)"),
                "int" => format!("to_jsonb(({column})::int4)"),
                "int64" => format!("to_jsonb(({column})::text)"),
                "decimal" => format!("to_jsonb(trim_scale(({column})::numeric)::text)"),
                "float" => format!("to_jsonb(({column})::float8)"),
                "boolean" => format!("to_jsonb(({column})::boolean)"),
                "datetime" => format!(
                    "to_jsonb(to_char(timezone('UTC', {column}), 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'))"
                ),
                "date" => format!("to_jsonb(to_char({column}, 'YYYY-MM-DD'))"),
                "time" => format!("to_jsonb(to_char({column}, 'HH24:MI:SS.US'))"),
                "json" => format!("to_jsonb(({column})::text)"),
                "bytes" => format!(
                    "to_jsonb(translate(rtrim(replace(encode(({column})::bytea, 'base64'), E'\\n', ''), '='), '+/', '-_'))"
                ),
                other => pgrx::error!("unsupported portable type {other:?}"),
            };
            format!(
                "'{}', {}",
                field.field_id.replace('\'', "''"),
                expression
            )
        })
        .collect::<Vec<_>>()
        .join(", ");

    format!("jsonb_build_object({pairs})")
}

pub(crate) fn compute_bucket_checksums(
    client: &SpiClient<'_>,
    bucket_ids: &[String],
) -> Result<std::collections::HashMap<String, ChecksumObject>, String> {
    let bucket_arr = bucket_ids.to_vec();
    let tup_table = match client.select(
        "SELECT bucket_id, relation_id::text AS relation_id, table_name, record_id, checksum \
         FROM sync_bucket_edges \
         WHERE bucket_id = ANY($1) \
         ORDER BY bucket_id, table_name, record_id",
        None,
        &[bucket_arr.into()],
    ) {
        Ok(t) => t,
        Err(_) => return Err("loading scope edges failed".to_string()),
    };

    let registry = crate::registry::load_registry_from_client(client)
        .map_err(|_| "loading active registry for scope digest failed".to_string())?;
    let (schema_version, schema_hash) = get_latest_schema(client);
    if schema_version <= 0 || schema_hash.is_empty() {
        return Err("current immutable schema is missing".to_string());
    }
    let schema_hash = SchemaHash::from_lower_hex(&schema_hash)
        .map_err(|error| format!("current immutable schema hash is invalid: {error}"))?;
    let mut entries_by_scope: std::collections::BTreeMap<String, Vec<ScopeDigestEntry>> =
        bucket_ids
            .iter()
            .cloned()
            .map(|scope_id| (scope_id, Vec::new()))
            .collect();
    for row in tup_table {
        let bid: String = row
            .get_by_name::<String, &str>("bucket_id")
            .map_err(|_| "scope edge bucket is malformed".to_string())?
            .ok_or_else(|| "scope edge bucket is null".to_string())?;
        let table_name = row
            .get_by_name::<String, &str>("table_name")
            .map_err(|_| "scope edge table is malformed".to_string())?
            .ok_or_else(|| "scope edge table is null".to_string())?;
        let relation_id = row
            .get_by_name::<String, &str>("relation_id")
            .map_err(|_| "scope edge relation identity is malformed".to_string())?
            .ok_or_else(|| "scope edge relation identity is null".to_string())?;
        let record_id = row
            .get_by_name::<String, &str>("record_id")
            .map_err(|_| "scope edge record identity is malformed".to_string())?
            .ok_or_else(|| "scope edge record identity is null".to_string())?;
        let digest = decode_digest(
            row.get_by_name::<Vec<u8>, &str>("checksum")
                .map_err(|_| "scope edge row digest is malformed".to_string())?
                .ok_or_else(|| "scope edge row digest is null".to_string())?,
            "scope edge row digest",
        )?;
        let table = registry
            .iter()
            .find(|table| table.relation_id == relation_id && table.table_name == table_name)
            .ok_or_else(|| {
                format!(
                    "scope edge relation {relation_id:?} and table {table_name:?} are not registered together"
                )
            })?;
        let primary_key_json = row_primary_key_json(table, &record_id)?;
        let canonical_table = canonical_table(table)?;
        let identity = row_identity(
            &canonical_table,
            &serde_json::to_string(&primary_key_json)
                .map_err(|error| format!("encoding scope row identity: {error}"))?,
        )
        .map_err(|error| format!("computing scope row identity: {error}"))?;
        entries_by_scope
            .get_mut(&bid)
            .ok_or_else(|| format!("scope edge belongs to an unknown scope {bid:?}"))?
            .push(ScopeDigestEntry::new(identity, digest));
    }
    entries_by_scope
        .into_iter()
        .map(|(scope_id, entries)| {
            let digest = synchro_core::checksum::scope_digest(schema_hash, &scope_id, &entries)
                .map_err(|error| format!("computing scope {scope_id:?} digest: {error}"))?;
            Ok((scope_id, ChecksumObject::new(digest)))
        })
        .collect()
}

pub(crate) fn contract_pk_value(
    registry: &[TableRegistration],
    table_name: &str,
    record_id: &str,
) -> serde_json::Value {
    let table = registry
        .iter()
        .find(|table| table.table_name == table_name)
        .unwrap_or_else(|| pgrx::error!("table {:?} is not registered", table_name));
    let value = row_primary_key_json(table, record_id)
        .unwrap_or_else(|error| pgrx::error!("invalid primary key for {}: {}", table_name, error));
    let mut object = serde_json::Map::new();
    object.insert(table.primary_key_field_id.clone(), value);
    serde_json::Value::Object(object)
}

fn decode_digest(bytes: Vec<u8>, name: &str) -> Result<Sha256Digest, String> {
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| format!("{name} must contain exactly 32 octets"))?;
    Ok(Sha256Digest::from_bytes(bytes))
}

/// Double-quote a SQL identifier, escaping internal double quotes.
pub(crate) fn pg_quote_ident(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}
