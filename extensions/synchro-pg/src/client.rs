use pgrx::prelude::*;
use pgrx::spi::SpiClient;
use synchro_core::contract::{
    ConnectRequest, ConnectResponse, ErrorBody, ErrorResponse, ProtocolErrorCode, SchemaAction,
    SchemaDescriptor, SchemaRef, ScopeAssignment, ScopeAssignmentDelta, ScopeCursorRef,
};

#[derive(Debug, Clone)]
pub(crate) struct ClientConnectState {
    pub(crate) bucket_subs: Vec<String>,
    pub(crate) scope_set_version: i64,
}

pub(crate) fn load_client_connect_state(
    client: &SpiClient<'_>,
    user_id: &str,
    client_id: &str,
) -> Result<ClientConnectState, pgrx::JsonB> {
    let rows = client
        .select(
            "SELECT bucket_subs, scope_set_version FROM sync_clients \
             WHERE user_id = $1 AND client_id = $2 AND is_active = true",
            None,
            &[user_id.into(), client_id.into()],
        )
        .unwrap_or_else(|err| pgrx::error!("loading sync client: {}", err));

    if let Some(row) = rows.into_iter().next() {
        let bucket_subs: Vec<String> = row
            .get_by_name::<Vec<String>, &str>("bucket_subs")
            .unwrap_or(None)
            .unwrap_or_default();
        let scope_set_version: i64 = row
            .get_by_name::<i64, &str>("scope_set_version")
            .unwrap_or(None)
            .unwrap_or(1);

        Ok(ClientConnectState {
            bucket_subs,
            scope_set_version,
        })
    } else {
        Err(pgrx::JsonB(
            serde_json::json!({"error": "client_not_found"}),
        ))
    }
}

/// Register a client for synchronization.
///
/// Upserts into sync_clients with default bucket subscriptions (user:{user_id}
/// plus every registered shared runtime scope). Returns JSONB matching the Go
/// RegisterResponse.
#[pg_extern]
fn synchro_register_client(
    p_user_id: &str,
    p_client_id: &str,
    p_platform: default!(&str, "''"),
    p_app_version: default!(&str, "''"),
    p_schema_version: default!(i64, "0"),
    p_schema_hash: default!(&str, "''"),
) -> pgrx::JsonB {
    // Validate schema version/hash if provided.
    if p_schema_version > 0 || !p_schema_hash.is_empty() {
        if let Err(err_json) = validate_schema(p_schema_version, p_schema_hash) {
            return err_json;
        }
    }

    let user_bucket = format!("user:{p_user_id}");

    let row: Option<pgrx::JsonB> = Spi::get_one_with_args(
        "WITH desired_scopes AS (
            SELECT ARRAY[$5::text]::text[]
                   || COALESCE(
                        ARRAY(
                            SELECT scope_id
                            FROM sync_shared_scopes
                            ORDER BY scope_id
                        ),
                        ARRAY[]::text[]
                   ) AS bucket_subs
        ),
        upserted AS (
            INSERT INTO sync_clients (
                user_id, client_id, platform, app_version,
                bucket_subs, is_active
            )
            SELECT $1, $2, $3, $4, ds.bucket_subs, true
            FROM desired_scopes ds
            ON CONFLICT (user_id, client_id) DO UPDATE SET
                platform = EXCLUDED.platform,
                app_version = EXCLUDED.app_version,
                bucket_subs = EXCLUDED.bucket_subs,
                scope_set_version = CASE
                    WHEN sync_clients.bucket_subs IS DISTINCT FROM EXCLUDED.bucket_subs
                        THEN sync_clients.scope_set_version + 1
                    ELSE sync_clients.scope_set_version
                END,
                is_active = true,
                updated_at = now()
            RETURNING id, last_sync_at, last_pull_seq, bucket_subs, scope_set_version
        ),
        seeded_checkpoints AS (
            INSERT INTO sync_client_checkpoints (user_id, client_id, bucket_id, checkpoint)
            SELECT $1, $2, bucket_id, 0
            FROM upserted u, unnest(u.bucket_subs) AS bucket_id
            ON CONFLICT (user_id, client_id, bucket_id) DO NOTHING
        ),
        client_buckets AS (
            SELECT bucket_id
            FROM upserted u, unnest(u.bucket_subs) AS bucket_id
        ),
        schema AS (
            SELECT schema_version, schema_hash
            FROM sync_schema_manifest
            ORDER BY schema_version DESC
            LIMIT 1
        ),
        bucket_cps AS (
            SELECT jsonb_object_agg(cb.bucket_id, COALESCE(scc.checkpoint, 0)) AS cps
            FROM client_buckets cb
            LEFT JOIN sync_client_checkpoints scc
              ON scc.user_id = $1
             AND scc.client_id = $2
             AND scc.bucket_id = cb.bucket_id
        )
        SELECT jsonb_build_object(
            'id', u.id::text,
            'server_time', to_char(timezone('UTC', now()), 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),
            'last_sync_at', CASE
                WHEN u.last_sync_at IS NULL THEN NULL
                ELSE to_char(u.last_sync_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')
            END,
            'checkpoint', COALESCE(u.last_pull_seq, 0),
            'bucket_checkpoints', COALESCE(bc.cps, '{}'::jsonb),
            'schema_version', COALESCE(s.schema_version, 0),
            'schema_hash', COALESCE(s.schema_hash, '')
        )
        FROM upserted u
        LEFT JOIN schema s ON true
        LEFT JOIN bucket_cps bc ON true",
        &[
            p_user_id.into(),
            p_client_id.into(),
            p_platform.into(),
            p_app_version.into(),
            user_bucket.as_str().into(),
        ],
    )
    .unwrap_or(None);

    match row {
        Some(json) => json,
        None => pgrx::error!("client registration returned no result"),
    }
}

/// vNext connect/bootstrap handshake.
///
/// This keeps `user_id` as a separate extension parameter while using the
/// shared-core vNext connect request and response shapes internally.
#[pg_extern]
fn synchro_connect(p_user_id: &str, p_request: pgrx::JsonB) -> pgrx::JsonB {
    let request: ConnectRequest = match serde_json::from_value(p_request.0) {
        Ok(request) => request,
        Err(err) => {
            return protocol_error_response(
                ProtocolErrorCode::InvalidRequest,
                format!("invalid connect request: {err}"),
                false,
            );
        }
    };

    if request.protocol_version != 1 {
        return protocol_error_response(
            ProtocolErrorCode::UpgradeRequired,
            "unsupported protocol version",
            false,
        );
    }

    Spi::connect_mut(|client| {
        let _ = client.update(
            "SELECT set_config('app.user_id', $1, true)",
            None,
            &[p_user_id.into()],
        );

        let client_state = ensure_client_connect_state(
            client,
            p_user_id,
            &request.client_id,
            &request.platform,
            &request.app_version,
        );

        let (schema_version, schema_hash) = crate::pull::get_latest_schema(client);
        let schema_definition = crate::schema::build_schema_manifest(client);
        let schema_action =
            if request.schema.version == schema_version && request.schema.hash == schema_hash {
                SchemaAction::None
            } else {
                SchemaAction::Replace
            };

        let scopes = build_scope_delta(&request.known_scopes, &client_state.bucket_subs);
        let response = ConnectResponse {
            server_time: chrono::Utc::now(),
            protocol_version: 1,
            scope_set_version: client_state.scope_set_version,
            schema: SchemaDescriptor {
                version: schema_version,
                hash: schema_hash,
                action: schema_action,
            },
            scopes,
            schema_definition: if schema_action.requires_schema_definition() {
                Some(schema_definition)
            } else {
                None
            },
        };

        if let Err(err) = response.validate() {
            pgrx::error!("invalid vNext connect response: {}", err);
        }

        pgrx::JsonB(serde_json::to_value(response).unwrap())
    })
}

/// Validate client schema version/hash against the server manifest.
///
/// Returns Ok(()) if valid, or Err(JsonB) with a structured error response
/// for schema mismatches. Business conditions are returned as JSONB, not
/// PG exceptions.
pub fn validate_schema(schema_version: i64, schema_hash: &str) -> Result<(), pgrx::JsonB> {
    if schema_version == 0 && schema_hash.is_empty() {
        return Ok(());
    }

    let server_hash: Option<String> = Spi::get_one_with_args(
        "SELECT schema_hash FROM sync_schema_manifest WHERE schema_version = $1",
        &[schema_version.into()],
    )
    .unwrap_or(None);

    match server_hash {
        Some(ref h) if h == schema_hash => Ok(()),
        Some(_) => {
            // Hash mismatch for this version.
            let (sv, sh) = latest_server_schema();
            Err(pgrx::JsonB(serde_json::json!({
                "error": "schema_mismatch",
                "server_schema_version": sv,
                "server_schema_hash": sh,
            })))
        }
        None => {
            // Version not found. Only error if the manifest has entries.
            let has_any: bool = Spi::get_one("SELECT EXISTS (SELECT 1 FROM sync_schema_manifest)")
                .unwrap_or(Some(false))
                .unwrap_or(false);

            if has_any {
                let (sv, sh) = latest_server_schema();
                Err(pgrx::JsonB(serde_json::json!({
                    "error": "schema_mismatch",
                    "server_schema_version": sv,
                    "server_schema_hash": sh,
                })))
            } else {
                // No schema manifest entries yet, skip validation.
                Ok(())
            }
        }
    }
}

pub(crate) fn validate_schema_vnext(schema: &SchemaRef) -> Result<(), pgrx::JsonB> {
    if schema.version == 0 && schema.hash.is_empty() {
        return Ok(());
    }

    let (server_version, server_hash) = latest_server_schema();
    if server_version == 0 && server_hash.is_empty() {
        return Ok(());
    }

    if schema.version == server_version && schema.hash == server_hash {
        return Ok(());
    }

    Err(protocol_error_response(
        ProtocolErrorCode::SchemaMismatch,
        format!(
            "client schema {}:{} does not match server schema {}:{}",
            schema.version, schema.hash, server_version, server_hash
        ),
        false,
    ))
}

pub(crate) fn protocol_error_response(
    code: ProtocolErrorCode,
    message: impl Into<String>,
    retryable: bool,
) -> pgrx::JsonB {
    pgrx::JsonB(
        serde_json::to_value(ErrorResponse {
            error: ErrorBody {
                code,
                message: message.into(),
                retryable,
            },
        })
        .unwrap(),
    )
}

/// Get the latest server schema version and hash.
fn latest_server_schema() -> (i64, String) {
    Spi::connect(|client| {
        let tup = match client.select(
            "SELECT schema_version, schema_hash FROM sync_schema_manifest \
             ORDER BY schema_version DESC LIMIT 1",
            None,
            &[],
        ) {
            Ok(t) => t,
            Err(_) => return (0, String::new()),
        };
        if let Some(row) = tup.into_iter().next() {
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
    })
}

fn ensure_client_connect_state(
    client: &mut SpiClient<'_>,
    user_id: &str,
    client_id: &str,
    platform: &str,
    app_version: &str,
) -> ClientConnectState {
    let user_bucket = format!("user:{user_id}");

    let _ = client
        .update(
            "WITH desired_scopes AS (
                SELECT ARRAY[$5::text]::text[]
                       || COALESCE(
                            ARRAY(
                                SELECT scope_id
                                FROM sync_shared_scopes
                                ORDER BY scope_id
                            ),
                            ARRAY[]::text[]
                       ) AS bucket_subs
            ),
            upserted AS (
                INSERT INTO sync_clients (
                user_id, client_id, platform, app_version,
                bucket_subs, is_active
                )
                SELECT $1, $2, $3, $4, ds.bucket_subs, true
                FROM desired_scopes ds
                ON CONFLICT (user_id, client_id) DO UPDATE SET
                    platform = EXCLUDED.platform,
                    app_version = EXCLUDED.app_version,
                    bucket_subs = EXCLUDED.bucket_subs,
                    scope_set_version = CASE
                        WHEN sync_clients.bucket_subs IS DISTINCT FROM EXCLUDED.bucket_subs
                            THEN sync_clients.scope_set_version + 1
                        ELSE sync_clients.scope_set_version
                    END,
                    is_active = true,
                    updated_at = now()
                RETURNING user_id, client_id, bucket_subs
            )
            INSERT INTO sync_client_checkpoints (user_id, client_id, bucket_id, checkpoint)
            SELECT u.user_id, u.client_id, bucket_id, 0
            FROM upserted u, unnest(u.bucket_subs) AS bucket_id
            ON CONFLICT (user_id, client_id, bucket_id) DO NOTHING",
            None,
            &[
                user_id.into(),
                client_id.into(),
                platform.into(),
                app_version.into(),
                user_bucket.as_str().into(),
            ],
        )
        .unwrap_or_else(|err| pgrx::error!("upserting sync client: {}", err));

    load_client_connect_state(client, user_id, client_id)
        .unwrap_or_else(|_| pgrx::error!("client registration returned no client row"))
}

pub(crate) fn build_scope_delta(
    known_scopes: &std::collections::BTreeMap<String, ScopeCursorRef>,
    server_scopes: &[String],
) -> ScopeAssignmentDelta {
    let known_ids: std::collections::HashSet<&str> = known_scopes
        .keys()
        .map(|scope_id| scope_id.as_str())
        .collect();
    let server_ids: std::collections::HashSet<&str> = server_scopes
        .iter()
        .map(|scope_id| scope_id.as_str())
        .collect();

    let mut add = Vec::new();
    for scope_id in server_scopes {
        if !known_ids.contains(scope_id.as_str()) {
            add.push(ScopeAssignment {
                id: scope_id.clone(),
                cursor: None,
            });
        }
    }

    let mut remove = Vec::new();
    for scope_id in known_scopes.keys() {
        if !server_ids.contains(scope_id.as_str()) {
            remove.push(scope_id.clone());
        }
    }

    ScopeAssignmentDelta { add, remove }
}
