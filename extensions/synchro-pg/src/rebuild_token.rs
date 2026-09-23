use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use pgrx::spi::SpiClient;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::stream_position::StreamPosition;

type HmacSha256 = Hmac<Sha256>;

const TOKEN_VERSION: &str = "v3";
const TOKEN_KIND: &str = "rebuild";
const TOKEN_PURPOSE: &str = "rebuild_cursor";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RebuildContinuation {
    pub(crate) kind: String,
    pub(crate) token_version: i32,
    pub(crate) key_id: String,
    pub(crate) stream_generation: String,
    pub(crate) user_binding: String,
    pub(crate) client_binding: String,
    pub(crate) client_generation: i64,
    pub(crate) scope_id: String,
    pub(crate) schema_hash: String,
    pub(crate) membership_generation: i64,
    pub(crate) retention_generation: i64,
    pub(crate) session_id: String,
    pub(crate) rebuild_id: String,
    pub(crate) snapshot_boundary: StreamPosition,
    pub(crate) next_row_ordinal: i64,
    pub(crate) page_limit: i64,
    pub(crate) accepted_write_epoch: i64,
    pub(crate) issued_at: String,
    pub(crate) expires_at: String,
}

pub(crate) struct RebuildContinuationInput {
    pub(crate) stream_generation: String,
    pub(crate) user_binding: String,
    pub(crate) client_binding: String,
    pub(crate) client_generation: i64,
    pub(crate) scope_id: String,
    pub(crate) schema_hash: String,
    pub(crate) membership_generation: i64,
    pub(crate) retention_generation: i64,
    pub(crate) session_id: String,
    pub(crate) rebuild_id: String,
    pub(crate) snapshot_boundary: StreamPosition,
    pub(crate) next_row_ordinal: i64,
    pub(crate) page_limit: i64,
    pub(crate) accepted_write_epoch: i64,
    pub(crate) issued_at: String,
    pub(crate) expires_at: String,
}

impl RebuildContinuation {
    pub(crate) fn new(input: RebuildContinuationInput) -> Self {
        Self {
            kind: TOKEN_KIND.to_string(),
            token_version: 3,
            key_id: String::new(),
            stream_generation: input.stream_generation,
            user_binding: input.user_binding,
            client_binding: input.client_binding,
            client_generation: input.client_generation,
            scope_id: input.scope_id,
            schema_hash: input.schema_hash,
            membership_generation: input.membership_generation,
            retention_generation: input.retention_generation,
            session_id: input.session_id,
            rebuild_id: input.rebuild_id,
            snapshot_boundary: input.snapshot_boundary,
            next_row_ordinal: input.next_row_ordinal,
            page_limit: input.page_limit,
            accepted_write_epoch: input.accepted_write_epoch,
            issued_at: input.issued_at,
            expires_at: input.expires_at,
        }
    }

    pub(crate) fn matches_request(
        &self,
        user_id: &str,
        client_id: &str,
        scope_id: &str,
        rebuild_id: &str,
        page_limit: i64,
    ) -> bool {
        self.user_binding == user_id
            && self.client_binding == client_id
            && self.scope_id == scope_id
            && self.rebuild_id == rebuild_id
            && self.page_limit == page_limit
    }

    fn is_structurally_valid(&self) -> bool {
        matches!(
            &self.snapshot_boundary,
            StreamPosition::GenerationStart | StreamPosition::TransactionEnd { .. }
        ) && self.kind == TOKEN_KIND
            && self.token_version == 3
            && !self.key_id.is_empty()
            && !self.stream_generation.is_empty()
            && !self.user_binding.is_empty()
            && !self.client_binding.is_empty()
            && self.client_generation > 0
            && !self.scope_id.is_empty()
            && !self.schema_hash.is_empty()
            && self.membership_generation > 0
            && self.retention_generation > 0
            && !self.session_id.is_empty()
            && !self.rebuild_id.is_empty()
            && self.next_row_ordinal >= 0
            && self.page_limit > 0
            && self.accepted_write_epoch > 0
            && !self.issued_at.is_empty()
            && !self.expires_at.is_empty()
    }
}

pub(crate) fn issue_rebuild_continuation(
    client: &SpiClient<'_>,
    continuation: &RebuildContinuation,
) -> Result<String, String> {
    let key = load_active_key(client)?;
    let mut payload = continuation.clone();
    payload.key_id = key.key_id;
    if !payload.is_structurally_valid() {
        return Err("rebuild continuation payload is invalid".to_string());
    }
    let payload = canonical_payload(&payload)?;
    let payload_segment = URL_SAFE_NO_PAD.encode(payload);
    let signed = signed_input(&payload_segment);
    let signature = sign(&key.secret, signed.as_bytes())?;
    Ok(format!(
        "{TOKEN_VERSION}.{TOKEN_KIND}.{payload_segment}.{signature}"
    ))
}

pub(crate) fn parse_rebuild_continuation(
    client: &SpiClient<'_>,
    token: &str,
) -> Result<RebuildContinuation, ()> {
    let mut parts = token.split('.');
    let (Some(version), Some(kind), Some(payload_segment), Some(signature_segment)) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(());
    };
    if parts.next().is_some() || version != TOKEN_VERSION || kind != TOKEN_KIND {
        return Err(());
    }

    let signature = URL_SAFE_NO_PAD.decode(signature_segment).map_err(|_| ())?;
    if signature.len() != 32 {
        return Err(());
    }
    let payload = URL_SAFE_NO_PAD.decode(payload_segment).map_err(|_| ())?;
    let continuation: RebuildContinuation = serde_json::from_slice(&payload).map_err(|_| ())?;
    if canonical_payload(&continuation).map_err(|_| ())? != payload
        || !continuation.is_structurally_valid()
    {
        return Err(());
    }
    let key = load_verification_key(client, &continuation.key_id).map_err(|_| ())?;
    let signed = signed_input(payload_segment);
    let mut mac = HmacSha256::new_from_slice(key.secret.as_bytes()).map_err(|_| ())?;
    mac.update(signed.as_bytes());
    mac.verify_slice(&signature).map_err(|_| ())?;
    Ok(continuation)
}

struct TokenKey {
    key_id: String,
    secret: String,
}

fn canonical_payload(continuation: &RebuildContinuation) -> Result<Vec<u8>, String> {
    serde_json_canonicalizer::to_vec(continuation)
        .map_err(|error| format!("canonicalizing rebuild continuation payload: {error}"))
}

fn load_active_key(client: &SpiClient<'_>) -> Result<TokenKey, String> {
    load_key(client, None)
}

fn load_verification_key(client: &SpiClient<'_>, key_id: &str) -> Result<TokenKey, String> {
    load_key(client, Some(key_id))
}

fn load_key(client: &SpiClient<'_>, key_id: Option<&str>) -> Result<TokenKey, String> {
    let (query, args) = match key_id {
        Some(key_id) => (
            "SELECT key_id, secret
             FROM sync_token_keys
             WHERE purpose = $1
               AND key_id = $2
               AND state IN ('active', 'verify_only')",
            vec![TOKEN_PURPOSE.into(), key_id.into()],
        ),
        None => (
            "SELECT key_id, secret
             FROM sync_token_keys
             WHERE purpose = $1 AND state = 'active'",
            vec![TOKEN_PURPOSE.into()],
        ),
    };
    let rows = client
        .select(query, None, &args)
        .map_err(|error| format!("loading rebuild token key: {error}"))?;
    if rows.len() != 1 {
        return Err("rebuild token key is unavailable".to_string());
    }
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| "rebuild token key is unavailable".to_string())?;
    let key_id = row
        .get_by_name::<String, &str>("key_id")
        .map_err(|error| format!("reading rebuild token key ID: {error}"))?
        .filter(|key_id| !key_id.is_empty())
        .ok_or_else(|| "rebuild token key ID is missing".to_string())?;
    let secret = row
        .get_by_name::<String, &str>("secret")
        .map_err(|error| format!("reading rebuild token key: {error}"))?
        .filter(|secret| secret.len() >= 64)
        .ok_or_else(|| "rebuild token key is invalid".to_string())?;
    Ok(TokenKey { key_id, secret })
}

fn signed_input(payload_segment: &str) -> String {
    format!("{TOKEN_VERSION}.{TOKEN_KIND}.{payload_segment}")
}

fn sign(secret: &str, input: &[u8]) -> Result<String, String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|error| format!("initializing rebuild token hmac: {error}"))?;
    mac.update(input);
    Ok(URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
}
