use pgrx::spi::{SpiClient, SpiHeapTupleData};
use synchro_core::checksum::Sha256Digest;

pub(crate) const JSONB_BATCH_BYTES: usize = 16 * 1024 * 1024;

pub(crate) fn jsonb_batches<'a, T>(
    items: &'a [T],
    maximum_rows: usize,
    payload: impl Fn(&'a T) -> &'a serde_json::Value,
) -> Result<Vec<&'a [T]>, String> {
    struct EncodedSize(usize);

    impl std::io::Write for EncodedSize {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("encoded batch size overflowed"))?;
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    if maximum_rows == 0 {
        return Err("JSON batch row bound must be positive".to_string());
    }
    let mut batches = Vec::new();
    let mut start = 0;
    let mut bytes = 0usize;
    for (index, item) in items.iter().enumerate() {
        let mut encoded = EncodedSize(0);
        serde_json::to_writer(&mut encoded, payload(item))
            .map_err(|error| format!("measuring JSON batch payload: {error}"))?;
        // A single large row keeps its existing limit. Only grouping uses this byte target.
        if index > start
            && (index - start == maximum_rows
                || bytes >= JSONB_BATCH_BYTES
                || encoded.0 > JSONB_BATCH_BYTES - bytes)
        {
            batches.push(&items[start..index]);
            start = index;
            bytes = 0;
        }
        bytes += encoded.0;
    }
    if start < items.len() {
        batches.push(&items[start..]);
    }
    Ok(batches)
}

// Separate payload datums preserve PostgreSQL's existing per-row JSONB size limit.
pub(crate) fn jsonb_payload_parameters(
    rows: &[serde_json::Value],
    payload_key: &str,
) -> Result<(pgrx::JsonB, Vec<pgrx::JsonB>), String> {
    let mut metadata = Vec::<serde_json::Value>::with_capacity(rows.len());
    let mut payloads = Vec::with_capacity(rows.len());
    for (index, row) in rows.iter().enumerate() {
        let row = row
            .as_object()
            .ok_or_else(|| "JSON batch row is not an object".to_string())?;
        let payload = row
            .get(payload_key)
            .ok_or_else(|| "JSON batch payload is missing".to_string())?;
        let mut entry = row
            .iter()
            .filter(|(key, _)| key.as_str() != payload_key)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<serde_json::Map<_, _>>();
        if entry
            .insert("payload_index".to_string(), (index + 1).into())
            .is_some()
        {
            return Err("JSON batch payload index is duplicated".to_string());
        }
        metadata.push(entry.into());
        payloads.push(pgrx::JsonB(payload.clone()));
    }
    Ok((pgrx::JsonB(metadata.into()), payloads))
}

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

pub(crate) fn required_positive_i64(row: &SpiHeapTupleData<'_>, name: &str) -> Result<i64, String> {
    row.get_by_name::<i64, &str>(name)
        .map_err(|error| format!("reading {name}: {error}"))?
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{name} is invalid"))
}

pub(crate) fn required_record_id(row: &SpiHeapTupleData<'_>) -> Result<String, String> {
    row.get_by_name::<String, &str>("record_id")
        .map_err(|error| format!("reading record identity: {error}"))?
        .ok_or_else(|| "record identity is missing".to_string())
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

            #[cfg(test)]
            mod tests {
                use super::{jsonb_batches, jsonb_payload_parameters, JSONB_BATCH_BYTES};
                use serde_json::json;

                #[test]
                fn payload_batches_respect_encoded_bytes_and_keep_large_rows() {
                    let escaped = json!("\n".repeat(JSONB_BATCH_BYTES / 4));
                    let rows = vec![escaped.clone(), escaped];
                    let batches = jsonb_batches(&rows, 500, |row| row).unwrap();
                    assert_eq!(
                        batches.iter().map(|batch| batch.len()).collect::<Vec<_>>(),
                        [1, 1]
                    );

                    let rows = vec![json!("x".repeat(JSONB_BATCH_BYTES - 2)), json!(null)];
                    let batches = jsonb_batches(&rows, 500, |row| row).unwrap();
                    assert_eq!(
                        batches.iter().map(|batch| batch.len()).collect::<Vec<_>>(),
                        [1, 1]
                    );

                    let rows = vec![json!("x".repeat(JSONB_BATCH_BYTES)), json!(null)];
                    let batches = jsonb_batches(&rows, 500, |row| row).unwrap();
                    assert_eq!(
                        batches.iter().map(|batch| batch.len()).collect::<Vec<_>>(),
                        [1, 1]
                    );
                    assert_eq!(batches[0][0], rows[0]);
                }

                #[test]
                fn payload_batches_preserve_rows_and_native_parameter_binding() {
                    let rows = (0..5)
                        .map(|index| json!({"record_id": index, "row_data": {"value": index}}))
                        .collect::<Vec<_>>();
                    let batches = jsonb_batches(&rows, 2, |row| row).unwrap();
                    assert_eq!(
                        batches.iter().map(|batch| batch.len()).collect::<Vec<_>>(),
                        [2, 2, 1]
                    );
                    for batch in batches {
                        let (metadata, payloads) =
                            jsonb_payload_parameters(batch, "row_data").unwrap();
                        let metadata = metadata.0.as_array().unwrap();
                        assert_eq!(metadata.len(), batch.len());
                        assert_eq!(payloads.len(), batch.len());
                        for (index, original) in batch.iter().enumerate() {
                            assert_eq!(metadata[index]["record_id"], original["record_id"]);
                            assert_eq!(metadata[index]["payload_index"], index + 1);
                            assert!(metadata[index].get("row_data").is_none());
                            assert_eq!(payloads[index].0, original["row_data"]);
                        }
                    }
                    assert!(jsonb_batches(&rows, 0, |row| row).is_err());
                    assert!(jsonb_batches::<serde_json::Value>(&[], 1, |row| row)
                        .unwrap()
                        .is_empty());
                    assert!(jsonb_payload_parameters(&[json!({})], "row_data").is_err());
                    assert!(jsonb_payload_parameters(
                        &[json!({"payload_index": 0, "row_data": {}})],
                        "row_data",
                    )
                    .is_err());
                }
            }
        })
}
