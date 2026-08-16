use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::stream_position::StreamPosition;
use synchro_core::checksum::ChecksumObject;

type HmacSha256 = Hmac<Sha256>;

const PAGE_PREFIX: &str = "sp1";
const CONTINUATION_PREFIX: &str = "sc1";
const EXPORT_SESSION_PREFIX: &str = "ss1";
const PAGE_DOMAIN: &[u8] = b"synchro:v3:seed-page:v1\0";
const CONTINUATION_DOMAIN: &[u8] = b"synchro:v3:seed-continuation:v1\0";
const EXPORT_SESSION_DOMAIN: &[u8] = b"synchro:v3:seed-export-session:v1\0";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SeedSnapshotBoundary {
    pub(crate) position_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) commit_lsn: Option<String>,
}

impl SeedSnapshotBoundary {
    pub(crate) fn from_position(position: &StreamPosition) -> Self {
        Self {
            position_kind: position.kind().to_string(),
            commit_lsn: position.commit_lsn(),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        match (self.position_kind.as_str(), self.commit_lsn.as_deref()) {
            ("generation_start", None) => Ok(()),
            ("transaction_end", Some(commit_lsn))
                if crate::stream_position::parse_lsn(commit_lsn).is_some() =>
            {
                Ok(())
            }
            _ => Err("seed snapshot boundary is invalid".to_string()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SeedPagePayload {
    pub(crate) kind: String,
    pub(crate) version: i32,
    pub(crate) key_id: String,
    pub(crate) export_id: String,
    pub(crate) transaction_nonce: String,
    pub(crate) export_manifest_hash: String,
    pub(crate) schema_hash: String,
    pub(crate) scope_id: String,
    pub(crate) registry_generation: String,
    pub(crate) membership_generation: String,
    pub(crate) retention_generation: String,
    pub(crate) stream_generation: String,
    pub(crate) snapshot_boundary: SeedSnapshotBoundary,
    pub(crate) next_row_ordinal: String,
    pub(crate) page_limit: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SeedContinuationPayload {
    pub(crate) kind: String,
    pub(crate) version: i32,
    pub(crate) key_id: String,
    pub(crate) export_id: String,
    pub(crate) export_manifest_hash: String,
    pub(crate) schema_hash: String,
    pub(crate) scope_id: String,
    pub(crate) registry_generation: String,
    pub(crate) membership_generation: String,
    pub(crate) retention_generation: String,
    pub(crate) stream_generation: String,
    pub(crate) snapshot_boundary: SeedSnapshotBoundary,
    pub(crate) cardinality: String,
    pub(crate) checksum: ChecksumObject,
    pub(crate) issued_at: String,
}

pub(crate) fn issue_page(payload: &SeedPagePayload, secret: &str) -> Result<String, String> {
    validate_page_payload(payload)?;
    let payload_bytes = canonical_payload(payload)?;
    let mac = sign(PAGE_DOMAIN, secret, &payload_bytes)?;
    Ok(format!(
        "{PAGE_PREFIX}.{}.{}",
        URL_SAFE_NO_PAD.encode(payload_bytes),
        URL_SAFE_NO_PAD.encode(mac)
    ))
}

pub(crate) fn verify_page(token: &str, secret: &str) -> Result<SeedPagePayload, String> {
    let payload_bytes = verify_envelope(token, PAGE_PREFIX, PAGE_DOMAIN, secret)?;
    let payload: SeedPagePayload = serde_json::from_slice(&payload_bytes)
        .map_err(|_| "portable seed page token is invalid".to_string())?;
    if canonical_payload(&payload)? != payload_bytes {
        return Err("portable seed page token is not canonical".to_string());
    }
    validate_page_payload(&payload)?;
    Ok(payload)
}

pub(crate) fn issue_export_session<T: Serialize>(
    payload: &T,
    secret: &str,
) -> Result<String, String> {
    let payload_bytes = canonical_payload(payload)?;
    let mac = sign(EXPORT_SESSION_DOMAIN, secret, &payload_bytes)?;
    Ok(format!(
        "{EXPORT_SESSION_PREFIX}.{}.{}",
        URL_SAFE_NO_PAD.encode(payload_bytes),
        URL_SAFE_NO_PAD.encode(mac)
    ))
}

pub(crate) fn verify_export_session<T>(token: &str, secret: &str) -> Result<T, String>
where
    T: DeserializeOwned + Serialize,
{
    let payload_bytes =
        verify_envelope(token, EXPORT_SESSION_PREFIX, EXPORT_SESSION_DOMAIN, secret)?;
    let payload: T = serde_json::from_slice(&payload_bytes)
        .map_err(|_| "portable seed export session is invalid".to_string())?;
    if canonical_payload(&payload)? != payload_bytes {
        return Err("portable seed export session is not canonical".to_string());
    }
    Ok(payload)
}

pub(crate) fn issue_continuation(
    payload: &SeedContinuationPayload,
    secret: &str,
) -> Result<String, String> {
    validate_continuation_payload(payload)?;
    let payload_bytes = canonical_payload(payload)?;
    let mac = sign(CONTINUATION_DOMAIN, secret, &payload_bytes)?;
    Ok(format!(
        "{CONTINUATION_PREFIX}.{}.{}",
        URL_SAFE_NO_PAD.encode(payload_bytes),
        URL_SAFE_NO_PAD.encode(mac)
    ))
}

pub(crate) fn verify_continuation(
    token: &str,
    secret: &str,
) -> Result<SeedContinuationPayload, String> {
    let payload_bytes = verify_envelope(token, CONTINUATION_PREFIX, CONTINUATION_DOMAIN, secret)?;
    let payload: SeedContinuationPayload = serde_json::from_slice(&payload_bytes)
        .map_err(|_| "portable seed continuation receipt is invalid".to_string())?;
    if canonical_payload(&payload)? != payload_bytes {
        return Err("portable seed continuation receipt is not canonical".to_string());
    }
    validate_continuation_payload(&payload)?;
    Ok(payload)
}

fn canonical_payload<T: Serialize>(payload: &T) -> Result<Vec<u8>, String> {
    serde_json_canonicalizer::to_vec(payload)
        .map_err(|error| format!("canonicalizing seed token payload: {error}"))
}

fn verify_envelope(
    token: &str,
    prefix: &str,
    domain: &[u8],
    secret: &str,
) -> Result<Vec<u8>, String> {
    let mut parts = token.split('.');
    let token_prefix = parts.next();
    let encoded_payload = parts.next();
    let encoded_mac = parts.next();
    if token_prefix != Some(prefix)
        || encoded_payload.is_none()
        || encoded_mac.is_none()
        || parts.next().is_some()
    {
        return Err("portable seed token envelope is invalid".to_string());
    }
    let payload = URL_SAFE_NO_PAD
        .decode(encoded_payload.unwrap_or_default())
        .map_err(|_| "portable seed token payload encoding is invalid".to_string())?;
    let mac = URL_SAFE_NO_PAD
        .decode(encoded_mac.unwrap_or_default())
        .map_err(|_| "portable seed token MAC encoding is invalid".to_string())?;
    if mac.len() != 32 {
        return Err("portable seed token MAC is invalid".to_string());
    }
    let mut verifier = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|error| format!("initializing seed token HMAC: {error}"))?;
    verifier.update(domain);
    verifier.update(&payload);
    verifier
        .verify_slice(&mac)
        .map_err(|_| "portable seed token MAC is invalid".to_string())?;
    Ok(payload)
}

fn sign(domain: &[u8], secret: &str, payload: &[u8]) -> Result<Vec<u8>, String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|error| format!("initializing seed token HMAC: {error}"))?;
    mac.update(domain);
    mac.update(payload);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn validate_page_payload(payload: &SeedPagePayload) -> Result<(), String> {
    if payload.kind != "portable_seed_page"
        || payload.version != 1
        || payload.key_id.is_empty()
        || !is_lower_uuid(&payload.export_id)
        || !is_base64url_32(&payload.transaction_nonce)
        || !is_lower_sha256(&payload.export_manifest_hash)
        || !is_lower_sha256(&payload.schema_hash)
        || payload.scope_id.is_empty()
        || payload.stream_generation.is_empty()
        || !is_unsigned_decimal(&payload.registry_generation)
        || !is_unsigned_decimal(&payload.membership_generation)
        || !is_unsigned_decimal(&payload.retention_generation)
        || !is_unsigned_decimal(&payload.next_row_ordinal)
        || !is_unsigned_decimal(&payload.page_limit)
        || payload.page_limit == "0"
    {
        return Err("portable seed page token payload is invalid".to_string());
    }
    payload.snapshot_boundary.validate()
}

fn validate_continuation_payload(payload: &SeedContinuationPayload) -> Result<(), String> {
    if payload.kind != "portable_seed_continuation"
        || payload.version != 1
        || payload.key_id.is_empty()
        || !is_lower_uuid(&payload.export_id)
        || !is_lower_sha256(&payload.export_manifest_hash)
        || !is_lower_sha256(&payload.schema_hash)
        || payload.scope_id.is_empty()
        || payload.stream_generation.is_empty()
        || !is_unsigned_decimal(&payload.registry_generation)
        || !is_unsigned_decimal(&payload.membership_generation)
        || !is_unsigned_decimal(&payload.retention_generation)
        || !is_unsigned_decimal(&payload.cardinality)
        || !is_canonical_datetime(&payload.issued_at)
    {
        return Err("portable seed continuation receipt payload is invalid".to_string());
    }
    payload.snapshot_boundary.validate()
}

fn is_unsigned_decimal(value: &str) -> bool {
    !value.is_empty()
        && (value == "0" || !value.starts_with('0'))
        && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_lower_uuid(value: &str) -> bool {
    value.len() == 36
        && value.as_bytes().iter().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                *byte == b'-'
            } else {
                byte.is_ascii_digit() || (b'a'..=b'f').contains(byte)
            }
        })
}

fn is_base64url_32(value: &str) -> bool {
    URL_SAFE_NO_PAD
        .decode(value)
        .map(|bytes| bytes.len() == 32 && URL_SAFE_NO_PAD.encode(bytes) == value)
        .unwrap_or(false)
}

fn is_canonical_datetime(value: &str) -> bool {
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|parsed| {
            parsed
                .with_timezone(&chrono::Utc)
                .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                .to_string()
                == value
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use synchro_core::checksum::Sha256Digest;

    const TEST_SECRET: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn corrupt_mac(token: &str) -> String {
        let mut parts = token.split('.').map(str::to_string).collect::<Vec<_>>();
        assert_eq!(parts.len(), 3);
        let payload = parts[1].clone();
        let mut mac = URL_SAFE_NO_PAD.decode(&parts[2]).expect("decode test MAC");
        assert_eq!(mac.len(), 32);
        mac[0] ^= 1;
        parts[2] = URL_SAFE_NO_PAD.encode(mac);
        assert_eq!(parts[1], payload);
        parts.join(".")
    }

    fn test_boundary() -> SeedSnapshotBoundary {
        SeedSnapshotBoundary {
            position_kind: "generation_start".to_string(),
            commit_lsn: None,
        }
    }

    #[test]
    fn page_token_rejects_mac_only_corruption() {
        let payload = SeedPagePayload {
            kind: "portable_seed_page".to_string(),
            version: 1,
            key_id: "seed-page-v1".to_string(),
            export_id: "00000000-0000-0000-0000-000000000001".to_string(),
            transaction_nonce: URL_SAFE_NO_PAD.encode([7_u8; 32]),
            export_manifest_hash: "1".repeat(64),
            schema_hash: "2".repeat(64),
            scope_id: "catalog".to_string(),
            registry_generation: "1".to_string(),
            membership_generation: "2".to_string(),
            retention_generation: "3".to_string(),
            stream_generation: "stream-1".to_string(),
            snapshot_boundary: test_boundary(),
            next_row_ordinal: "0".to_string(),
            page_limit: "100".to_string(),
        };
        let token = issue_page(&payload, TEST_SECRET).expect("issue page token");
        let corrupted = corrupt_mac(&token);

        assert!(verify_page(&token, TEST_SECRET).is_ok());
        assert!(verify_page(&corrupted, TEST_SECRET).is_err());
    }

    #[test]
    fn continuation_receipt_rejects_mac_only_corruption() {
        let checksum = ChecksumObject::new(
            Sha256Digest::from_lower_hex(&"3".repeat(64)).expect("test checksum"),
        );
        let payload = SeedContinuationPayload {
            kind: "portable_seed_continuation".to_string(),
            version: 1,
            key_id: "seed-continuation-v1".to_string(),
            export_id: "00000000-0000-0000-0000-000000000001".to_string(),
            export_manifest_hash: "1".repeat(64),
            schema_hash: "2".repeat(64),
            scope_id: "catalog".to_string(),
            registry_generation: "1".to_string(),
            membership_generation: "2".to_string(),
            retention_generation: "3".to_string(),
            stream_generation: "stream-1".to_string(),
            snapshot_boundary: test_boundary(),
            cardinality: "0".to_string(),
            checksum,
            issued_at: "2026-08-15T00:00:00.000000Z".to_string(),
        };
        let token = issue_continuation(&payload, TEST_SECRET).expect("issue continuation");
        let corrupted = corrupt_mac(&token);

        assert!(verify_continuation(&token, TEST_SECRET).is_ok());
        assert!(verify_continuation(&corrupted, TEST_SECRET).is_err());
    }
}
