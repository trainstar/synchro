use crc32fast::Hasher;
use serde_json::Value;

/// Computes a deterministic CRC32 (IEEE / ISO 3309) checksum for a JSON string.
///
/// The input is parsed into a `serde_json::Value`, then re-serialized. Because
/// `serde_json` serializes object keys in insertion order, and `Value::Object`
/// uses a `BTreeMap` (sorted keys) when the `preserve_order` feature is **not**
/// enabled (the default), this produces canonical output regardless of the
/// original key order.
///
/// If the input is not valid JSON, the raw bytes are hashed instead.
///
/// Matches the Go `ComputeRecordChecksum` (crc32.ChecksumIEEE on
/// `json.Marshal` canonical output) and the Swift/Kotlin client CRC32
/// implementations.
pub fn compute_record_checksum(json_str: &str) -> u32 {
    match serde_json::from_str::<Value>(json_str) {
        Ok(val) => {
            // Re-serialize to get canonical (sorted-key) JSON.
            match serde_json::to_vec(&val) {
                Ok(canonical) => ieee_crc32(&canonical),
                Err(_) => ieee_crc32(json_str.as_bytes()),
            }
        }
        Err(_) => ieee_crc32(json_str.as_bytes()),
    }
}

/// Computes a deterministic CRC32 checksum from an already-parsed JSON value.
pub fn compute_record_checksum_from_value(val: &Value) -> u32 {
    // serde_json::to_vec on a Value with BTreeMap keys produces sorted output.
    match serde_json::to_vec(val) {
        Ok(canonical) => ieee_crc32(&canonical),
        Err(_) => 0,
    }
}

/// Raw CRC32 IEEE computation.
fn ieee_crc32(data: &[u8]) -> u32 {
    let mut h = Hasher::new();
    h.update(data);
    h.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn canonical_key_order() {
        // Keys in different orders must produce the same checksum.
        let a = r#"{"b":2,"a":1}"#;
        let b = r#"{"a":1,"b":2}"#;
        assert_eq!(compute_record_checksum(a), compute_record_checksum(b));
    }

    #[test]
    fn invalid_json_hashes_raw() {
        let raw = "not json at all";
        let cs = compute_record_checksum(raw);
        assert_eq!(cs, ieee_crc32(raw.as_bytes()));
    }

    #[test]
    fn empty_object() {
        let cs = compute_record_checksum("{}");
        assert_ne!(cs, 0);
    }

    #[test]
    fn from_value_matches_from_string() {
        let val = json!({"name": "Alice", "age": 30});
        let json_str = serde_json::to_string(&val).unwrap();
        assert_eq!(
            compute_record_checksum(&json_str),
            compute_record_checksum_from_value(&val)
        );
    }

    // Test vector: verify Go compatibility.
    // Go: json.Marshal(map[string]any{"id":"abc","name":"test"}) produces
    //     {"id":"abc","name":"test"} (sorted keys).
    // CRC32 IEEE of that byte sequence.
    #[test]
    fn known_test_vector() {
        let json_str = r#"{"id":"abc","name":"test"}"#;
        let cs = compute_record_checksum(json_str);
        // Compute expected from raw bytes directly.
        let expected = ieee_crc32(json_str.as_bytes());
        assert_eq!(cs, expected);
    }

    #[test]
    fn signed_cast_round_trip() {
        // The wire protocol uses i32. Verify the cast round-trips.
        let cs = compute_record_checksum(r#"{"x":1}"#);
        let signed = cs as i32;
        let back = signed as u32;
        assert_eq!(cs, back);
    }

    // Go-verified test vectors. These checksums were produced by:
    //   go: json.Marshal(map) -> crc32.ChecksumIEEE(canonical)
    // and must match exactly for cross-platform consistency.

    #[test]
    fn go_vector_sorted_keys() {
        assert_eq!(compute_record_checksum(r#"{"id":"abc","name":"test"}"#), 1221324213);
    }

    #[test]
    fn go_vector_unsorted_keys_canonicalize() {
        // Different key order, same canonical output.
        assert_eq!(compute_record_checksum(r#"{"name":"test","id":"abc"}"#), 1221324213);
    }

    #[test]
    fn go_vector_empty_object() {
        assert_eq!(compute_record_checksum("{}"), 2745614147);
    }

    #[test]
    fn go_vector_numeric_values() {
        assert_eq!(compute_record_checksum(r#"{"a":1,"b":2,"c":3}"#), 785669035);
    }

    #[test]
    fn go_vector_mixed_types() {
        assert_eq!(
            compute_record_checksum(
                r#"{"user_id":"u1","email":"test@example.com","active":true}"#
            ),
            830595651
        );
    }

    #[test]
    fn go_vector_signed_negative() {
        // {} produces u32=2745614147 which wraps to i32=-1549353149.
        let cs = compute_record_checksum("{}");
        assert_eq!(cs as i32, -1549353149);
    }
}
