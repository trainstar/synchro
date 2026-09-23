use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use pgrx::spi::SpiClient;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::spi_helpers::{
    current_utc_timestamp, is_lower_hex, required_positive_i64, required_text,
};
use crate::stream_position::StreamPosition;

type HmacSha256 = Hmac<Sha256>;

const TOKEN_PREFIX: &str = "ic1";
const TOKEN_KIND: &str = "incremental_cursor";
const TOKEN_VERSION: i32 = 1;
const TOKEN_PURPOSE: &str = "incremental_cursor";
const SIGNING_DOMAIN: &[u8] = b"synchro:v3:incremental-cursor:v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParsedScopeCursor {
    Current(StreamPosition),
    Stale,
}

#[derive(Debug, Clone)]
pub(crate) struct ScopeCursorContext {
    user_binding: String,
    client_binding: String,
    client_generation: i64,
    scope_id: String,
    schema_hash: String,
}

impl ScopeCursorContext {
    pub(crate) fn new(
        user_binding: &str,
        client_binding: &str,
        client_generation: i64,
        scope_id: &str,
        schema_hash: &str,
    ) -> Result<Self, String> {
        if user_binding.is_empty()
            || client_binding.is_empty()
            || client_generation <= 0
            || scope_id.is_empty()
            || !is_lower_hex(schema_hash, 64)
        {
            return Err("incremental cursor context is invalid".to_string());
        }
        Ok(Self {
            user_binding: user_binding.to_string(),
            client_binding: client_binding.to_string(),
            client_generation,
            scope_id: scope_id.to_string(),
            schema_hash: schema_hash.to_string(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopeCursorPayload {
    kind: String,
    token_version: i32,
    key_id: String,
    stream_generation: String,
    position: StreamPosition,
    user_binding: String,
    client_binding: String,
    client_generation: i64,
    scope_id: String,
    schema_hash: String,
    membership_generation: i64,
    retention_generation: i64,
    issued_at: String,
}

struct RuntimeState {
    stream_generation: String,
}

struct ScopeState {
    stream_generation: String,
    membership_generation: i64,
    retention_generation: i64,
    floor: StreamPosition,
}

struct TokenKey {
    key_id: String,
    secret: String,
}

pub(crate) fn issue_scope_cursor(
    client: &SpiClient<'_>,
    context: &ScopeCursorContext,
    position: &StreamPosition,
) -> Result<String, String> {
    position.validate()?;
    let runtime = load_runtime_state(client)?;
    let scope = load_scope_state(client, &context.scope_id)?;
    if scope.stream_generation != runtime.stream_generation {
        return Err("scope state has the wrong stream generation".to_string());
    }
    if position < &scope.floor {
        return Err("scope cursor position is below the retention floor".to_string());
    }
    let key = load_active_key(client)?;
    let payload = ScopeCursorPayload {
        kind: TOKEN_KIND.to_string(),
        token_version: TOKEN_VERSION,
        key_id: key.key_id,
        stream_generation: runtime.stream_generation,
        position: position.clone(),
        user_binding: context.user_binding.clone(),
        client_binding: context.client_binding.clone(),
        client_generation: context.client_generation,
        scope_id: context.scope_id.clone(),
        schema_hash: context.schema_hash.clone(),
        membership_generation: scope.membership_generation,
        retention_generation: scope.retention_generation,
        issued_at: current_utc_timestamp(client, "incremental cursor issue time", "")?,
    };
    let payload = canonical_payload(&payload)?;
    let signature = sign(&key.secret, &payload)?;
    Ok(format!(
        "{TOKEN_PREFIX}.{}.{}",
        URL_SAFE_NO_PAD.encode(payload),
        URL_SAFE_NO_PAD.encode(signature)
    ))
}

pub(crate) fn parse_scope_cursor(
    client: &SpiClient<'_>,
    context: &ScopeCursorContext,
    token: &str,
) -> Result<ParsedScopeCursor, String> {
    let (payload_bytes, signature) = decode_envelope(token)?;
    let payload: ScopeCursorPayload = serde_json::from_slice(&payload_bytes)
        .map_err(|_| "incremental cursor payload is invalid".to_string())?;
    if canonical_payload(&payload)? != payload_bytes || !payload_is_structurally_valid(&payload) {
        return Err("incremental cursor payload is invalid".to_string());
    }
    let key = load_verification_key(client, &payload.key_id)?;
    verify(&key.secret, &payload_bytes, &signature)?;
    if payload.user_binding != context.user_binding
        || payload.client_binding != context.client_binding
        || payload.client_generation != context.client_generation
        || payload.scope_id != context.scope_id
        || payload.schema_hash != context.schema_hash
    {
        return Err("incremental cursor binding is invalid".to_string());
    }

    let runtime = load_runtime_state(client)?;
    let scope = load_scope_state(client, &context.scope_id)?;
    if payload.stream_generation != runtime.stream_generation
        || scope.stream_generation != payload.stream_generation
        || scope.membership_generation != payload.membership_generation
        || scope.retention_generation != payload.retention_generation
        || payload.position < scope.floor
    {
        return Ok(ParsedScopeCursor::Stale);
    }
    Ok(ParsedScopeCursor::Current(payload.position))
}

fn payload_is_structurally_valid(payload: &ScopeCursorPayload) -> bool {
    payload.kind == TOKEN_KIND
        && payload.token_version == TOKEN_VERSION
        && !payload.key_id.is_empty()
        && !payload.stream_generation.is_empty()
        && payload.position.validate().is_ok()
        && !payload.user_binding.is_empty()
        && !payload.client_binding.is_empty()
        && payload.client_generation > 0
        && !payload.scope_id.is_empty()
        && is_lower_hex(&payload.schema_hash, 64)
        && payload.membership_generation > 0
        && payload.retention_generation > 0
        && chrono::DateTime::parse_from_rfc3339(&payload.issued_at).is_ok()
}

fn canonical_payload(payload: &ScopeCursorPayload) -> Result<Vec<u8>, String> {
    serde_json_canonicalizer::to_vec(payload)
        .map_err(|error| format!("canonicalizing incremental cursor: {error}"))
}

fn decode_envelope(token: &str) -> Result<(Vec<u8>, Vec<u8>), String> {
    let mut parts = token.split('.');
    let prefix = parts.next();
    let payload = parts.next();
    let signature = parts.next();
    if prefix != Some(TOKEN_PREFIX)
        || payload.is_none()
        || signature.is_none()
        || parts.next().is_some()
    {
        return Err("incremental cursor envelope is invalid".to_string());
    }
    let payload = URL_SAFE_NO_PAD
        .decode(payload.unwrap_or_default())
        .map_err(|_| "incremental cursor payload encoding is invalid".to_string())?;
    let signature = URL_SAFE_NO_PAD
        .decode(signature.unwrap_or_default())
        .map_err(|_| "incremental cursor signature encoding is invalid".to_string())?;
    if signature.len() != 32 {
        return Err("incremental cursor signature is invalid".to_string());
    }
    Ok((payload, signature))
}

fn sign(secret: &str, payload: &[u8]) -> Result<Vec<u8>, String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|error| format!("initializing incremental cursor HMAC: {error}"))?;
    mac.update(SIGNING_DOMAIN);
    mac.update(&[0]);
    mac.update(payload);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn verify(secret: &str, payload: &[u8], signature: &[u8]) -> Result<(), String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|error| format!("initializing incremental cursor HMAC: {error}"))?;
    mac.update(SIGNING_DOMAIN);
    mac.update(&[0]);
    mac.update(payload);
    mac.verify_slice(signature)
        .map_err(|_| "incremental cursor signature is invalid".to_string())
}

fn load_active_key(client: &SpiClient<'_>) -> Result<TokenKey, String> {
    load_key(client, None, "state = 'active'")
}

fn load_verification_key(client: &SpiClient<'_>, key_id: &str) -> Result<TokenKey, String> {
    load_key(client, Some(key_id), "state IN ('active', 'verify_only')")
}

fn load_key(
    client: &SpiClient<'_>,
    key_id: Option<&str>,
    state_predicate: &str,
) -> Result<TokenKey, String> {
    let key_filter = if key_id.is_some() {
        "AND key_id = $1"
    } else {
        ""
    };
    let query = format!(
        "SELECT key_id, secret
         FROM sync_token_keys
         WHERE purpose = '{TOKEN_PURPOSE}' AND {state_predicate} {key_filter}"
    );
    let args = key_id.map_or_else(Vec::new, |key_id| vec![key_id.into()]);
    let rows = client
        .select(&query, None, &args)
        .map_err(|error| format!("loading incremental cursor key: {error}"))?;
    if rows.len() != 1 {
        return Err("incremental cursor key is unavailable".to_string());
    }
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| "incremental cursor key is unavailable".to_string())?;
    let key_id = row
        .get_by_name::<String, &str>("key_id")
        .map_err(|error| format!("reading incremental cursor key ID: {error}"))?
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "incremental cursor key ID is missing".to_string())?;
    let secret = row
        .get_by_name::<String, &str>("secret")
        .map_err(|error| format!("reading incremental cursor key: {error}"))?
        .filter(|value| value.len() >= 64)
        .ok_or_else(|| "incremental cursor key is invalid".to_string())?;
    Ok(TokenKey { key_id, secret })
}

fn load_runtime_state(client: &SpiClient<'_>) -> Result<RuntimeState, String> {
    let rows = client
        .select(
            "SELECT stream_generation FROM sync_runtime_state WHERE singleton = true",
            None,
            &[],
        )
        .map_err(|error| format!("loading runtime state: {error}"))?;
    let stream_generation = rows
        .into_iter()
        .next()
        .ok_or_else(|| "runtime state is missing".to_string())?
        .get_by_name::<String, &str>("stream_generation")
        .map_err(|error| format!("reading stream generation: {error}"))?
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "stream generation is missing".to_string())?;
    Ok(RuntimeState { stream_generation })
}

fn load_scope_state(client: &SpiClient<'_>, scope_id: &str) -> Result<ScopeState, String> {
    let rows = client
        .select(
            "SELECT stream_generation, membership_generation, retention_generation,
                    floor_position_kind, floor_commit_lsn::text AS floor_commit_lsn,
                    floor_event_ordinal, floor_effect_ordinal
             FROM sync_scope_state
             WHERE scope_id = $1",
            None,
            &[scope_id.into()],
        )
        .map_err(|error| format!("loading scope state: {error}"))?;
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| "scope state is missing".to_string())?;
    let stream_generation = required_text(&row, "stream_generation", "")?;
    let membership_generation = required_positive_i64(&row, "membership_generation")?;
    let retention_generation = required_positive_i64(&row, "retention_generation")?;
    let kind = required_text(&row, "floor_position_kind", "")?;
    let commit_lsn = row
        .get_by_name::<String, &str>("floor_commit_lsn")
        .map_err(|error| format!("reading retention floor commit LSN: {error}"))?;
    let event_ordinal = row
        .get_by_name::<i64, &str>("floor_event_ordinal")
        .map_err(|error| format!("reading retention floor event ordinal: {error}"))?;
    let effect_ordinal = row
        .get_by_name::<i32, &str>("floor_effect_ordinal")
        .map_err(|error| format!("reading retention floor effect ordinal: {error}"))?;
    let floor = StreamPosition::from_sql_parts(
        &kind,
        commit_lsn.as_deref(),
        event_ordinal,
        effect_ordinal,
    )?;
    Ok(ScopeState {
        stream_generation,
        membership_generation,
        retention_generation,
        floor,
    })
}
