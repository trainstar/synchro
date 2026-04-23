use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use pgrx::spi::SpiClient;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

const TOKEN_VERSION: &str = "v2";

#[derive(Debug)]
enum CursorDecodeError {
    Malformed(String),
    SignatureMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParsedScopeCursor {
    Current(i64),
    Stale,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScopeCursorPayload {
    scope_id: String,
    stream_generation: String,
    checkpoint: i64,
}

pub(crate) fn issue_scope_cursor(
    client: &SpiClient<'_>,
    scope_id: &str,
    checkpoint: i64,
) -> Result<String, String> {
    let state = load_runtime_state(client)?;
    let payload = ScopeCursorPayload {
        scope_id: scope_id.to_string(),
        stream_generation: state.stream_generation,
        checkpoint,
    };
    encode_scope_cursor(&payload, state.cursor_secret.as_bytes())
}

pub(crate) fn parse_scope_cursor(
    client: &SpiClient<'_>,
    expected_scope_id: &str,
    token: &str,
) -> Result<ParsedScopeCursor, String> {
    let state = load_runtime_state(client)?;
    let payload = match decode_scope_cursor(token, state.cursor_secret.as_bytes()) {
        Ok(payload) => payload,
        Err(CursorDecodeError::SignatureMismatch) => return Ok(ParsedScopeCursor::Stale),
        Err(CursorDecodeError::Malformed(message)) => {
            if token.starts_with(TOKEN_VERSION) {
                return Err(message);
            }
            return Ok(ParsedScopeCursor::Stale);
        }
    };
    if payload.scope_id != expected_scope_id {
        return Err(format!(
            "cursor scope mismatch: expected {}, got {}",
            expected_scope_id, payload.scope_id
        ));
    }
    if payload.stream_generation != state.stream_generation {
        return Ok(ParsedScopeCursor::Stale);
    }
    Ok(ParsedScopeCursor::Current(payload.checkpoint))
}

#[derive(Debug, Clone)]
struct RuntimeState {
    stream_generation: String,
    cursor_secret: String,
}

fn load_runtime_state(client: &SpiClient<'_>) -> Result<RuntimeState, String> {
    let rows = client
        .select(
            "SELECT stream_generation, cursor_secret FROM sync_runtime_state WHERE singleton = true",
            None,
            &[],
        )
        .map_err(|err| format!("loading runtime state: {err}"))?;

    if let Some(row) = rows.into_iter().next() {
        let stream_generation = row
            .get_by_name::<String, &str>("stream_generation")
            .map_err(|err| format!("reading stream_generation: {err}"))?
            .unwrap_or_default();
        let cursor_secret = row
            .get_by_name::<String, &str>("cursor_secret")
            .map_err(|err| format!("reading cursor_secret: {err}"))?
            .unwrap_or_default();

        if stream_generation.is_empty() || cursor_secret.is_empty() {
            return Err("runtime state is incomplete".to_string());
        }

        Ok(RuntimeState {
            stream_generation,
            cursor_secret,
        })
    } else {
        Err("runtime state row is missing".to_string())
    }
}

fn encode_scope_cursor(payload: &ScopeCursorPayload, secret: &[u8]) -> Result<String, String> {
    let payload_bytes =
        serde_json::to_vec(payload).map_err(|err| format!("encoding cursor payload: {err}"))?;
    let payload_segment = URL_SAFE_NO_PAD.encode(payload_bytes);
    let signature_segment = sign_segment(secret, payload_segment.as_bytes())?;
    Ok(format!(
        "{}.{}.{}",
        TOKEN_VERSION, payload_segment, signature_segment
    ))
}

fn decode_scope_cursor(token: &str, secret: &[u8]) -> Result<ScopeCursorPayload, CursorDecodeError> {
    let mut parts = token.split('.');
    let version = parts
        .next()
        .ok_or_else(|| CursorDecodeError::Malformed("cursor token is missing version".to_string()))?;
    let payload_segment = parts
        .next()
        .ok_or_else(|| CursorDecodeError::Malformed("cursor token is missing payload".to_string()))?;
    let signature_segment = parts
        .next()
        .ok_or_else(|| CursorDecodeError::Malformed("cursor token is missing signature".to_string()))?;
    if parts.next().is_some() {
        return Err(CursorDecodeError::Malformed(
            "cursor token has too many segments".to_string(),
        ));
    }
    if version != TOKEN_VERSION {
        return Err(CursorDecodeError::Malformed(format!(
            "unsupported cursor token version {}",
            version
        )));
    }

    let signature_bytes = URL_SAFE_NO_PAD
        .decode(signature_segment)
        .map_err(|err| CursorDecodeError::Malformed(format!("decoding cursor signature: {err}")))?;
    let mut mac = HmacSha256::new_from_slice(secret)
        .map_err(|err| CursorDecodeError::Malformed(format!("initializing cursor token hmac: {err}")))?;
    mac.update(payload_segment.as_bytes());
    mac.verify_slice(&signature_bytes)
        .map_err(|_| CursorDecodeError::SignatureMismatch)?;

    let payload_bytes = URL_SAFE_NO_PAD
        .decode(payload_segment)
        .map_err(|err| CursorDecodeError::Malformed(format!("decoding cursor payload: {err}")))?;
    serde_json::from_slice(&payload_bytes)
        .map_err(|err| CursorDecodeError::Malformed(format!("decoding cursor token payload: {err}")))
}

fn sign_segment(secret: &[u8], payload_segment: &[u8]) -> Result<String, String> {
    let mut mac = HmacSha256::new_from_slice(secret)
        .map_err(|err| format!("initializing cursor token hmac: {err}"))?;
    mac.update(payload_segment);
    Ok(URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
}
