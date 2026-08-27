use pgrx::spi::{SpiClient, SpiHeapTupleData};
use synchro_core::checksum::Sha256Digest;

pub(crate) fn required_text(
    row: &SpiHeapTupleData<'_>,
    name: &str,
    error_prefix: &str,
) -> Result<String, String> {
    row.get_by_name::<String, &str>(name)
        .map_err(|error| format!("reading {error_prefix}{name}: {error}"))?
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{error_prefix}{name} is missing"))
}

pub(crate) fn decode_digest(value: Vec<u8>, invalid_message: &str) -> Result<Sha256Digest, String> {
    let bytes: [u8; 32] = value.try_into().map_err(|_| invalid_message.to_string())?;
    Ok(Sha256Digest::from_bytes(bytes))
}

pub(crate) fn current_utc_timestamp(
    client: &SpiClient<'_>,
    context: &str,
    text_error_prefix: &str,
) -> Result<String, String> {
    let row = client
        .select(
            "SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS issued_at",
            None,
            &[],
        )
        .map_err(|error| format!("reading {context}: {error}"))?
        .next()
        .ok_or_else(|| format!("{context} is missing"))?;
    required_text(&row, "issued_at", text_error_prefix)
}

pub(crate) fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) fn is_lower_uuid(value: &str) -> bool {
    value.len() == 36
        && value.as_bytes().iter().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                *byte == b'-'
            } else {
                byte.is_ascii_digit() || (b'a'..=b'f').contains(byte)
            }
        })
}
