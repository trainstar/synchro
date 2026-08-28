use std::collections::{BTreeMap, HashSet};

use chrono::{SecondsFormat, Timelike, Utc};
use pgrx::prelude::*;
use pgrx::spi::SpiClient;
use synchro_core::contract::{
    ConnectRequest, ConnectResponse, ContractViolation, ErrorBody, ErrorResponse,
    ProtocolErrorCode, SchemaAction, SchemaDescriptor, SchemaRef, ScopeAssignment,
    ScopeAssignmentDelta, ScopeCursorRef,
};

pub(crate) const SQL_CONTRACT_VERSION: i32 = 1;
pub(crate) const PROTOCOL_VERSION: u32 = 3;
const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;

#[derive(Debug, Clone)]
pub(crate) struct ClientConnectState {
    pub(crate) bucket_subs: Vec<String>,
    pub(crate) scope_set_version: i64,
    pub(crate) client_generation: i64,
}

#[derive(Debug)]
struct StoredClientState {
    bucket_subs: Vec<String>,
    scope_set_version: i64,
    client_generation: i64,
    generation_expired: bool,
}

#[derive(Debug)]
struct EnsuredClientState {
    state: ClientConnectState,
    generation_renewed: bool,
}

struct ScopeCursorUpdateInput<'a> {
    user_id: &'a str,
    request: &'a ConnectRequest,
    assigned_scopes: &'a [String],
    delta: &'a ScopeAssignmentDelta,
    schema_action: SchemaAction,
    affected_scopes: Option<&'a [String]>,
    generation_renewed: bool,
    client_generation: i64,
    current_schema_hash: &'a str,
}

#[derive(serde::Serialize)]
struct ContractInfo {
    extension_version: &'static str,
    sql_contract_version: i32,
    protocol_version: u32,
    library_build_fingerprint: &'static str,
    installed_build_fingerprint: Option<String>,
    extension_objects_current: bool,
}

#[pg_extern]
fn synchro_contract_info() -> pgrx::JsonB {
    let installed_build_fingerprint = crate::build_fingerprint::installed_fingerprint();
    pgrx::JsonB(
        serde_json::to_value(ContractInfo {
            extension_version: env!("CARGO_PKG_VERSION"),
            sql_contract_version: SQL_CONTRACT_VERSION,
            protocol_version: PROTOCOL_VERSION,
            library_build_fingerprint: crate::build_fingerprint::library_fingerprint(),
            extension_objects_current: installed_build_fingerprint.as_deref().is_some_and(
                |installed| installed == crate::build_fingerprint::library_fingerprint(),
            ),
            installed_build_fingerprint,
        })
        .unwrap(),
    )
}

pub(crate) fn load_client_connect_state(
    client: &SpiClient<'_>,
    user_id: &str,
    client_id: &str,
) -> Result<ClientConnectState, pgrx::JsonB> {
    if client_is_retired(client, user_id, client_id) {
        return Err(client_retired_response());
    }

    let rows = client
        .select(
            "SELECT bucket_subs, scope_set_version, client_generation
             FROM sync_clients
             WHERE user_id = $1 AND client_id = $2 AND is_active = true",
            None,
            &[user_id.into(), client_id.into()],
        )
        .unwrap_or_else(|err| pgrx::error!("loading sync client state: {}", err));

    if let Some(row) = rows.into_iter().next() {
        let mut bucket_subs: Vec<String> = row
            .get_by_name::<Vec<String>, &str>("bucket_subs")
            .unwrap_or(None)
            .unwrap_or_default();
        sort_scope_ids(&mut bucket_subs);
        let scope_set_version: i64 = row
            .get_by_name::<i64, &str>("scope_set_version")
            .unwrap_or(None)
            .unwrap_or(1);
        let client_generation: i64 = row
            .get_by_name::<i64, &str>("client_generation")
            .unwrap_or(None)
            .unwrap_or(1);

        Ok(ClientConnectState {
            bucket_subs,
            scope_set_version,
            client_generation,
        })
    } else {
        Err(protocol_error_response(
            ProtocolErrorCode::InvalidRequest,
            "client is not registered",
            false,
        ))
    }
}

/// Canonical connect/bootstrap handshake.
///
/// This keeps `user_id` as a separate extension parameter while using the
/// shared-core canonical connect request and response shapes internally.
#[pg_extern]
fn synchro_connect(p_user_id: &str, p_request: pgrx::JsonB) -> pgrx::JsonB {
    if p_user_id.is_empty() {
        return protocol_error_response(
            ProtocolErrorCode::AuthRequired,
            "authentication is required",
            false,
        );
    }

    let request: ConnectRequest = match serde_json::from_value(p_request.0) {
        Ok(request) => request,
        Err(_) => {
            return protocol_error_response(
                ProtocolErrorCode::InvalidRequest,
                "invalid connect request",
                false,
            );
        }
    };

    if request.protocol_version != PROTOCOL_VERSION {
        return upgrade_required_response(request.protocol_version);
    }
    if let Err(error) = request.validate() {
        return match error {
            ContractViolation::InvalidSchemaReference
            | ContractViolation::InvalidFreshSchemaReference => {
                invalid_schema_reference_response(request.schema.clone())
            }
            _ => protocol_error_response(
                ProtocolErrorCode::InvalidRequest,
                "invalid connect request",
                false,
            ),
        };
    }

    Spi::connect_mut(|client| {
        let _ = client.update(
            "SELECT set_config('app.user_id', $1, true)",
            None,
            &[p_user_id.into()],
        );

        acquire_client_identity_lock(client, p_user_id, &request.client_id);
        if client_is_retired(client, p_user_id, &request.client_id) {
            return client_retired_response();
        }

        let prior = load_stored_client_state(client, p_user_id, &request.client_id);
        if request.schema.is_fresh_sentinel() && prior.is_some() {
            return invalid_schema_reference_response(request.schema.clone());
        }
        if prior.is_some() && request.client_generation.is_none() {
            return invalid_schema_reference_response(request.schema.clone());
        }

        crate::schema::ensure_schema_manifest(client);
        let current_manifest = crate::schema::load_latest_schema_manifest(client);
        let server_scopes = load_authoritative_scopes(client, p_user_id);
        if prior.is_none()
            && request
                .known_scopes
                .keys()
                .any(|scope| !server_scopes.contains(scope))
        {
            return protocol_error_response(
                ProtocolErrorCode::InvalidRequest,
                "connect contains an unknown scope",
                false,
            );
        }
        let (schema_action, schema_reason, affected_scopes) = if request.schema_reset == Some(true)
        {
            if server_scopes.is_empty() {
                (SchemaAction::Replace, None, None)
            } else {
                (
                    SchemaAction::RebuildLocal,
                    None,
                    Some(server_scopes.clone()),
                )
            }
        } else if request.schema.is_fresh_sentinel() {
            (SchemaAction::Replace, None, None)
        } else {
            let decision = crate::schema::resolve_schema_lineage(client, &request.schema);
            if decision.action == SchemaAction::Unsupported {
                (decision.action, decision.reason, None)
            } else {
                let affected = intersect_scopes(&server_scopes, &decision.affected_scopes);
                if affected.is_empty() {
                    (decision.action, None, None)
                } else {
                    (SchemaAction::RebuildLocal, None, Some(affected))
                }
            }
        };
        if let Err(response) = prevalidate_scope_cursor_replacements(
            client,
            p_user_id,
            &request,
            prior.as_ref(),
            &server_scopes,
            schema_action,
        ) {
            return response;
        }
        let seed_positions = match request.seed_receipts.as_ref() {
            Some(receipts) if prior.is_none() && request.client_generation.is_none() => {
                match crate::portable_seed::validate_seed_receipts(
                    client,
                    receipts,
                    &current_manifest.schema_hash,
                ) {
                    Ok(positions) => positions,
                    Err(response) => return response,
                }
            }
            Some(_) => {
                return protocol_error_response(
                    ProtocolErrorCode::InvalidRequest,
                    "portable seed receipts are valid only on first connect",
                    false,
                )
            }
            None => BTreeMap::new(),
        };
        let ensured =
            match ensure_client_connect_state(client, p_user_id, &request, prior, &server_scopes) {
                Ok(state) => state,
                Err(response) => return response,
            };

        if request.scope_set_version > ensured.state.scope_set_version {
            return protocol_error_response(
                ProtocolErrorCode::InvalidRequest,
                "invalid connect request",
                false,
            );
        }
        if !scopes_were_assigned(
            client,
            p_user_id,
            &request.client_id,
            ensured.state.client_generation,
            request.known_scopes.keys(),
        ) {
            return protocol_error_response(
                ProtocolErrorCode::InvalidRequest,
                "connect contains an unknown scope",
                false,
            );
        }

        let assigned_scopes = &ensured.state.bucket_subs;

        let mut scopes = build_scope_delta(&request.known_scopes, assigned_scopes);
        for scope in &mut scopes.add {
            let Some(position) = seed_positions.get(&scope.id) else {
                continue;
            };
            scope.cursor = Some(issue_seed_scope_cursor(
                client,
                p_user_id,
                &request.client_id,
                ensured.state.client_generation,
                &scope.id,
                &current_manifest.schema_hash,
                position,
            ));
        }
        let mut scope_cursor_updates = match build_scope_cursor_updates(
            client,
            ScopeCursorUpdateInput {
                user_id: p_user_id,
                request: &request,
                assigned_scopes,
                delta: &scopes,
                schema_action,
                affected_scopes: affected_scopes.as_deref(),
                generation_renewed: ensured.generation_renewed,
                client_generation: ensured.state.client_generation,
                current_schema_hash: &current_manifest.schema_hash,
            },
        ) {
            Ok(updates) => updates,
            Err(response) => return response,
        };
        for (scope_id, position) in &seed_positions {
            if !request.known_scopes.contains_key(scope_id) || !assigned_scopes.contains(scope_id) {
                continue;
            }
            scope_cursor_updates.insert(
                scope_id.clone(),
                Some(issue_seed_scope_cursor(
                    client,
                    p_user_id,
                    &request.client_id,
                    ensured.state.client_generation,
                    scope_id,
                    &current_manifest.schema_hash,
                    position,
                )),
            );
        }

        let response = ConnectResponse {
            server_time: canonical_server_time(),
            protocol_version: PROTOCOL_VERSION,
            client_generation: ensured.state.client_generation,
            scope_set_version: ensured.state.scope_set_version,
            schema: SchemaDescriptor {
                version: current_manifest.schema_version,
                hash: current_manifest.schema_hash.clone(),
                action: schema_action,
                reason: schema_reason,
            },
            scopes,
            scope_cursor_updates,
            schema_definition: schema_action
                .requires_schema_definition()
                .then_some(current_manifest),
            affected_scopes,
        };

        if let Err(err) = response.validate() {
            pgrx::error!("invalid connect response: {}", err);
        }

        pgrx::JsonB(canonical_connect_response_value(&response).unwrap())
    })
}

fn issue_seed_scope_cursor(
    client: &SpiClient<'_>,
    user_id: &str,
    client_id: &str,
    client_generation: i64,
    scope_id: &str,
    schema_hash: &str,
    position: &crate::stream_position::StreamPosition,
) -> String {
    let context = crate::cursor_token::ScopeCursorContext::new(
        user_id,
        client_id,
        client_generation,
        scope_id,
        schema_hash,
    )
    .unwrap_or_else(|error| pgrx::error!("building seed scope cursor: {}", error));
    crate::cursor_token::issue_scope_cursor(client, &context, position)
        .unwrap_or_else(|error| pgrx::error!("issuing seed scope cursor: {}", error))
}

fn canonical_server_time() -> chrono::DateTime<Utc> {
    let now = Utc::now();
    now.with_nanosecond((now.nanosecond() / 1_000) * 1_000)
        .expect("truncated nanoseconds remain valid")
}

fn canonical_connect_response_value(
    response: &ConnectResponse,
) -> Result<serde_json::Value, serde_json::Error> {
    let mut value = serde_json::to_value(response)?;
    value["server_time"] = serde_json::Value::String(
        response
            .server_time
            .to_rfc3339_opts(SecondsFormat::Micros, true),
    );
    Ok(value)
}

pub(crate) fn validate_schema_ref(
    client: &SpiClient<'_>,
    schema: &SchemaRef,
) -> Result<(), pgrx::JsonB> {
    if schema.validate_normal().is_err() {
        return Err(protocol_error_response(
            ProtocolErrorCode::InvalidRequest,
            "invalid schema reference",
            false,
        ));
    }

    let manifest = crate::schema::load_latest_schema_manifest(client);
    let current = SchemaRef {
        version: manifest.schema_version,
        hash: manifest.schema_hash,
    };
    if *schema == current {
        return Ok(());
    }

    Err(schema_mismatch_response(current, schema.clone()))
}

pub(crate) fn protocol_error_response(
    code: ProtocolErrorCode,
    message: impl Into<String>,
    retryable: bool,
) -> pgrx::JsonB {
    encode_error(error_body(code, message, retryable))
}

fn upgrade_required_response(received_protocol_version: u32) -> pgrx::JsonB {
    let mut error = error_body(
        ProtocolErrorCode::UpgradeRequired,
        "unsupported protocol version",
        false,
    );
    error.required_protocol_version = Some(PROTOCOL_VERSION);
    error.received_protocol_version = Some(received_protocol_version);
    encode_validated_error(error)
}

fn invalid_schema_reference_response(received_schema: SchemaRef) -> pgrx::JsonB {
    let mut error = error_body(
        ProtocolErrorCode::InvalidSchemaReference,
        "invalid schema reference",
        false,
    );
    error.received_schema = Some(received_schema);
    encode_validated_error(error)
}

fn schema_mismatch_response(current_schema: SchemaRef, received_schema: SchemaRef) -> pgrx::JsonB {
    let mut error = error_body(
        ProtocolErrorCode::SchemaMismatch,
        "schema does not match the current server schema",
        false,
    );
    error.current_schema = Some(current_schema);
    error.received_schema = Some(received_schema);
    encode_validated_error(error)
}

pub(crate) fn client_generation_expired_response(current_generation: i64) -> pgrx::JsonB {
    let mut error = error_body(
        ProtocolErrorCode::ClientGenerationExpired,
        "client generation has expired",
        false,
    );
    error.current_client_generation = Some(current_generation);
    encode_validated_error(error)
}

fn client_retired_response() -> pgrx::JsonB {
    encode_validated_error(error_body(
        ProtocolErrorCode::ClientRetired,
        "client identity is retired",
        false,
    ))
}

fn error_body(code: ProtocolErrorCode, message: impl Into<String>, retryable: bool) -> ErrorBody {
    ErrorBody {
        code,
        message: message.into(),
        retryable,
        current_schema: None,
        received_schema: None,
        current_client_generation: None,
        scope_id: None,
        required_protocol_version: None,
        received_protocol_version: None,
        minimum_client_version: None,
        received_client_version: None,
        reason: None,
        field: None,
        minimum: None,
        maximum: None,
    }
}

fn encode_validated_error(error: ErrorBody) -> pgrx::JsonB {
    if let Err(validation_error) = error.validate() {
        pgrx::error!("invalid protocol error response: {}", validation_error);
    }
    encode_error(error)
}

fn encode_error(error: ErrorBody) -> pgrx::JsonB {
    pgrx::JsonB(serde_json::to_value(ErrorResponse { error }).unwrap())
}

pub(crate) fn acquire_client_identity_lock(
    client: &mut SpiClient<'_>,
    user_id: &str,
    client_id: &str,
) {
    client
        .update(
            "SELECT pg_advisory_xact_lock(
                 hashtextextended(jsonb_build_array($1::text, $2::text)::text, 0)
             )",
            None,
            &[user_id.into(), client_id.into()],
        )
        .unwrap_or_else(|err| pgrx::error!("locking sync client identity: {}", err));
}

fn prevalidate_scope_cursor_replacements(
    client: &SpiClient<'_>,
    user_id: &str,
    request: &ConnectRequest,
    prior: Option<&StoredClientState>,
    assigned_scopes: &[String],
    schema_action: SchemaAction,
) -> Result<(), pgrx::JsonB> {
    if request.schema.is_fresh_sentinel()
        || request.schema_reset == Some(true)
        || !matches!(
            schema_action,
            SchemaAction::Replace | SchemaAction::RebuildLocal
        )
    {
        return Ok(());
    }
    let Some(prior) = prior else {
        return Ok(());
    };
    if request.client_generation != Some(prior.client_generation) {
        return Ok(());
    }

    let assigned: HashSet<&str> = assigned_scopes.iter().map(String::as_str).collect();

    for (scope_id, cursor_ref) in &request.known_scopes {
        if !assigned.contains(scope_id.as_str()) {
            continue;
        }
        let Some(cursor) = cursor_ref.cursor.as_deref() else {
            continue;
        };
        let context = crate::cursor_token::ScopeCursorContext::new(
            user_id,
            &request.client_id,
            prior.client_generation,
            scope_id,
            &request.schema.hash,
        )
        .map_err(|_| {
            protocol_error_response(
                ProtocolErrorCode::InvalidRequest,
                "invalid scope cursor",
                false,
            )
        })?;
        crate::cursor_token::parse_scope_cursor(client, &context, cursor).map_err(|_| {
            protocol_error_response(
                ProtocolErrorCode::InvalidRequest,
                "invalid scope cursor",
                false,
            )
        })?;
    }
    Ok(())
}

fn client_is_retired(client: &SpiClient<'_>, user_id: &str, client_id: &str) -> bool {
    client
        .select(
            "SELECT EXISTS (
                 SELECT 1 FROM sync_client_retirements
                 WHERE user_id = $1 AND client_id = $2
             ) AS retired",
            None,
            &[user_id.into(), client_id.into()],
        )
        .unwrap_or_else(|err| pgrx::error!("checking client retirement: {}", err))
        .first()
        .get_one::<bool>()
        .unwrap_or_else(|err| pgrx::error!("reading client retirement state: {}", err))
        .unwrap_or(false)
}

fn load_stored_client_state(
    client: &SpiClient<'_>,
    user_id: &str,
    client_id: &str,
) -> Option<StoredClientState> {
    let rows = client
        .select(
            "SELECT bucket_subs, scope_set_version, client_generation,
                    generation_expires_at IS NOT NULL
                    OR NOT is_active
                    OR COALESCE(last_acknowledged_at, generation_created_at)
                       <= now() - interval '30 days' AS generation_expired
             FROM sync_clients
             WHERE user_id = $1 AND client_id = $2
             FOR UPDATE",
            None,
            &[user_id.into(), client_id.into()],
        )
        .unwrap_or_else(|err| pgrx::error!("loading durable sync client: {}", err));
    let row = rows.into_iter().next()?;
    let mut bucket_subs = row
        .get_by_name::<Vec<String>, &str>("bucket_subs")
        .unwrap_or(None)
        .unwrap_or_default();
    sort_scope_ids(&mut bucket_subs);
    Some(StoredClientState {
        bucket_subs,
        scope_set_version: row
            .get_by_name::<i64, &str>("scope_set_version")
            .unwrap_or(None)
            .unwrap_or(1),
        client_generation: row
            .get_by_name::<i64, &str>("client_generation")
            .unwrap_or(None)
            .unwrap_or(1),
        generation_expired: row
            .get_by_name::<bool, &str>("generation_expired")
            .unwrap_or(None)
            .unwrap_or(false),
    })
}

fn load_authoritative_scopes(client: &SpiClient<'_>, user_id: &str) -> Vec<String> {
    let rows = client
        .select("SELECT scope_id FROM sync_shared_scopes", None, &[])
        .unwrap_or_else(|err| pgrx::error!("loading authoritative client scopes: {}", err));
    let mut scopes = vec![format!("user:{user_id}")];
    for row in rows {
        let scope_id = row
            .get_by_name::<String, &str>("scope_id")
            .unwrap_or_else(|err| pgrx::error!("reading authoritative client scope: {}", err))
            .unwrap_or_else(|| pgrx::error!("authoritative client scope is missing"));
        scopes.push(scope_id);
    }
    sort_scope_ids(&mut scopes);
    scopes
}

fn ensure_client_connect_state(
    client: &mut SpiClient<'_>,
    user_id: &str,
    request: &ConnectRequest,
    prior: Option<StoredClientState>,
    server_scopes: &[String],
) -> Result<EnsuredClientState, pgrx::JsonB> {
    let (
        client_generation,
        generation_renewed,
        prior_scope_set_version,
        prior_scopes,
        client_was_new,
    ) = match prior {
        None => {
            if request.client_generation.is_some() {
                return Err(protocol_error_response(
                    ProtocolErrorCode::InvalidRequest,
                    "invalid connect request",
                    false,
                ));
            }
            (1, false, 1, Vec::new(), true)
        }
        Some(prior) => {
            if request.client_generation != Some(prior.client_generation) {
                return Err(protocol_error_response(
                    ProtocolErrorCode::InvalidRequest,
                    "invalid connect request",
                    false,
                ));
            }
            if prior.generation_expired {
                let next = prior
                    .client_generation
                    .checked_add(1)
                    .filter(|generation| *generation <= MAX_SAFE_INTEGER)
                    .unwrap_or_else(|| pgrx::error!("client generation allocation overflow"));
                (
                    next,
                    true,
                    prior.scope_set_version,
                    prior.bucket_subs,
                    false,
                )
            } else {
                (
                    prior.client_generation,
                    false,
                    prior.scope_set_version,
                    prior.bucket_subs,
                    false,
                )
            }
        }
    };

    let scope_set_version = if client_was_new {
        1
    } else if prior_scopes != server_scopes {
        prior_scope_set_version
            .checked_add(1)
            .filter(|version| *version <= MAX_SAFE_INTEGER)
            .unwrap_or_else(|| pgrx::error!("scope set version allocation overflow"))
    } else {
        prior_scope_set_version
    };

    client
        .update(
            "INSERT INTO sync_clients (
                 user_id, client_id, platform, app_version, bucket_subs,
                 scope_set_version, client_generation, accepted_write_epoch,
                 generation_created_at, generation_expires_at,
                 last_acknowledged_at, is_active
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, 1, now(), NULL, NULL, true)
             ON CONFLICT (user_id, client_id) DO UPDATE SET
                 platform = EXCLUDED.platform,
                 app_version = EXCLUDED.app_version,
                 bucket_subs = EXCLUDED.bucket_subs,
                 scope_set_version = EXCLUDED.scope_set_version,
                 client_generation = EXCLUDED.client_generation,
                 generation_created_at = CASE
                     WHEN sync_clients.client_generation <> EXCLUDED.client_generation
                         THEN now()
                     ELSE sync_clients.generation_created_at
                 END,
                 generation_expires_at = CASE
                     WHEN sync_clients.client_generation <> EXCLUDED.client_generation
                         THEN NULL
                     ELSE sync_clients.generation_expires_at
                 END,
                 last_acknowledged_at = CASE
                     WHEN sync_clients.client_generation <> EXCLUDED.client_generation
                         THEN NULL
                     ELSE sync_clients.last_acknowledged_at
                 END,
                 is_active = true,
                 updated_at = now()",
            None,
            &[
                user_id.into(),
                request.client_id.as_str().into(),
                request.platform.as_str().into(),
                request.app_version.as_str().into(),
                server_scopes.to_vec().into(),
                scope_set_version.into(),
                client_generation.into(),
            ],
        )
        .unwrap_or_else(|err| pgrx::error!("persisting sync client state: {}", err));

    if generation_renewed {
        client
            .update(
                "DELETE FROM sync_client_checkpoints
                 WHERE user_id = $1 AND client_id = $2",
                None,
                &[user_id.into(), request.client_id.as_str().into()],
            )
            .unwrap_or_else(|err| pgrx::error!("invalidating expired client checkpoints: {}", err));
    }
    client
        .update(
            "INSERT INTO sync_scope_state (scope_id, stream_generation)
             SELECT scope_id, rs.stream_generation
             FROM unnest($1::text[]) AS scope_id
             CROSS JOIN sync_runtime_state rs
             WHERE rs.singleton = true
             ON CONFLICT (scope_id) DO NOTHING",
            None,
            &[server_scopes.to_vec().into()],
        )
        .unwrap_or_else(|err| pgrx::error!("persisting scope generation state: {}", err));
    let (assigned, removed) = if client_was_new || generation_renewed {
        (server_scopes.to_vec(), Vec::new())
    } else {
        (
            server_scopes
                .iter()
                .filter(|scope| !prior_scopes.contains(scope))
                .cloned()
                .collect(),
            prior_scopes
                .iter()
                .filter(|scope| !server_scopes.contains(scope))
                .cloned()
                .collect(),
        )
    };
    persist_scope_history(
        client,
        user_id,
        &request.client_id,
        client_generation,
        scope_set_version,
        &assigned,
        true,
    );
    persist_scope_history(
        client,
        user_id,
        &request.client_id,
        client_generation,
        scope_set_version,
        &removed,
        false,
    );
    client
        .update(
            "INSERT INTO sync_client_checkpoints (
                 user_id, client_id, bucket_id, stream_generation, position_kind
             )
             SELECT $1, $2, scope_id, rs.stream_generation, 'generation_start'
             FROM unnest($3::text[]) AS scope_id
             CROSS JOIN sync_runtime_state rs
             WHERE rs.singleton = true
             ON CONFLICT (user_id, client_id, bucket_id) DO NOTHING",
            None,
            &[
                user_id.into(),
                request.client_id.as_str().into(),
                server_scopes.to_vec().into(),
            ],
        )
        .unwrap_or_else(|err| pgrx::error!("persisting sync client checkpoints: {}", err));

    Ok(EnsuredClientState {
        state: ClientConnectState {
            bucket_subs: server_scopes.to_vec(),
            scope_set_version,
            client_generation,
        },
        generation_renewed,
    })
}

fn persist_scope_history(
    client: &mut SpiClient<'_>,
    user_id: &str,
    client_id: &str,
    client_generation: i64,
    scope_set_version: i64,
    scopes: &[String],
    assigned: bool,
) {
    if scopes.is_empty() {
        return;
    }
    client
        .update(
            "INSERT INTO sync_client_scope_history (
                 user_id, client_id, client_generation, scope_id,
                 scope_set_version, assigned, assignment_source,
                 membership_generation, retention_generation
             )
             SELECT $1, $2, $3, scope.scope_id, $4, $5,
                    CASE WHEN scope.scope_id = 'user:' || $1
                         THEN 'identity' ELSE 'shared' END,
                    state.membership_generation, state.retention_generation
             FROM unnest($6::text[]) AS scope(scope_id)
             JOIN sync_scope_state state ON state.scope_id = scope.scope_id",
            None,
            &[
                user_id.into(),
                client_id.into(),
                client_generation.into(),
                scope_set_version.into(),
                assigned.into(),
                scopes.to_vec().into(),
            ],
        )
        .unwrap_or_else(|error| pgrx::error!("persisting scope assignment history: {}", error));
}

pub(crate) fn scopes_were_assigned<'a>(
    client: &SpiClient<'_>,
    user_id: &str,
    client_id: &str,
    client_generation: i64,
    scopes: impl Iterator<Item = &'a String>,
) -> bool {
    let scopes = scopes.cloned().collect::<Vec<_>>();
    if scopes.is_empty() {
        return true;
    }
    let count = client
        .select(
            "SELECT count(DISTINCT scope_id) AS count
             FROM sync_client_scope_history
             WHERE user_id = $1
               AND client_id = $2
               AND client_generation = $3
               AND scope_id = ANY($4)",
            None,
            &[
                user_id.into(),
                client_id.into(),
                client_generation.into(),
                scopes.clone().into(),
            ],
        )
        .unwrap_or_else(|error| pgrx::error!("validating scope assignment history: {}", error))
        .first()
        .get_by_name::<i64, &str>("count")
        .unwrap_or_else(|error| pgrx::error!("reading scope assignment history: {}", error))
        .unwrap_or(0);
    count == i64::try_from(scopes.len()).unwrap_or(i64::MAX)
}

fn build_scope_cursor_updates(
    client: &SpiClient<'_>,
    input: ScopeCursorUpdateInput<'_>,
) -> Result<BTreeMap<String, Option<String>>, pgrx::JsonB> {
    let added: HashSet<&str> = input
        .delta
        .add
        .iter()
        .map(|scope| scope.id.as_str())
        .collect();
    let removed: HashSet<&str> = input.delta.remove.iter().map(String::as_str).collect();
    let affected: HashSet<&str> = input
        .affected_scopes
        .unwrap_or_default()
        .iter()
        .map(String::as_str)
        .collect();
    let assigned: HashSet<&str> = input.assigned_scopes.iter().map(String::as_str).collect();
    let schema_changed = matches!(
        input.schema_action,
        SchemaAction::Replace | SchemaAction::RebuildLocal
    ) && !input.request.schema.is_fresh_sentinel();
    let reset = input.request.schema_reset == Some(true);

    let mut updates = BTreeMap::new();
    for (scope_id, cursor_ref) in &input.request.known_scopes {
        if !assigned.contains(scope_id.as_str())
            || added.contains(scope_id.as_str())
            || removed.contains(scope_id.as_str())
        {
            continue;
        }
        if input.generation_renewed || reset || affected.contains(scope_id.as_str()) {
            updates.insert(scope_id.clone(), None);
            continue;
        }
        if !schema_changed {
            continue;
        }

        let Some(cursor) = cursor_ref.cursor.as_deref() else {
            updates.insert(scope_id.clone(), None);
            continue;
        };
        let presented_context = crate::cursor_token::ScopeCursorContext::new(
            input.user_id,
            &input.request.client_id,
            input.client_generation,
            scope_id,
            &input.request.schema.hash,
        )
        .map_err(|_| {
            protocol_error_response(
                ProtocolErrorCode::InvalidRequest,
                "invalid scope cursor",
                false,
            )
        })?;
        match crate::cursor_token::parse_scope_cursor(client, &presented_context, cursor) {
            Ok(crate::cursor_token::ParsedScopeCursor::Current(position)) => {
                let replacement_context = crate::cursor_token::ScopeCursorContext::new(
                    input.user_id,
                    &input.request.client_id,
                    input.client_generation,
                    scope_id,
                    input.current_schema_hash,
                )
                .unwrap_or_else(|err| pgrx::error!("building replacement cursor: {}", err));
                let replacement = crate::cursor_token::issue_scope_cursor(
                    client,
                    &replacement_context,
                    &position,
                )
                .unwrap_or_else(|err| pgrx::error!("issuing replacement scope cursor: {}", err));
                updates.insert(scope_id.clone(), Some(replacement));
            }
            Ok(crate::cursor_token::ParsedScopeCursor::Stale) => {
                updates.insert(scope_id.clone(), None);
            }
            Err(_) => {
                return Err(protocol_error_response(
                    ProtocolErrorCode::InvalidRequest,
                    "invalid scope cursor",
                    false,
                ));
            }
        }
    }
    Ok(updates)
}

fn intersect_scopes(assigned: &[String], affected: &[String]) -> Vec<String> {
    let affected: HashSet<&str> = affected.iter().map(String::as_str).collect();
    assigned
        .iter()
        .filter(|scope| affected.contains(scope.as_str()))
        .cloned()
        .collect()
}

fn sort_scope_ids(scopes: &mut Vec<String>) {
    scopes.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    scopes.dedup();
}

pub(crate) fn build_scope_delta(
    known_scopes: &BTreeMap<String, ScopeCursorRef>,
    server_scopes: &[String],
) -> ScopeAssignmentDelta {
    let known_ids: HashSet<&str> = known_scopes.keys().map(String::as_str).collect();
    let server_ids: HashSet<&str> = server_scopes.iter().map(String::as_str).collect();

    let add = server_scopes
        .iter()
        .filter(|scope_id| !known_ids.contains(scope_id.as_str()))
        .map(|scope_id| ScopeAssignment {
            id: scope_id.clone(),
            cursor: None,
        })
        .collect();
    let remove = known_scopes
        .keys()
        .filter(|scope_id| !server_ids.contains(scope_id.as_str()))
        .cloned()
        .collect();

    ScopeAssignmentDelta { add, remove }
}
