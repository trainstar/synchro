//! Canonical Protocol 3 push fingerprint encodings.

use std::fmt;

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::checksum::Sha256Digest;
use crate::contract::{ContractViolation, Mutation, Operation, PushRequest, SchemaRef};

const BATCH_FINGERPRINT_DOMAIN: &[u8] = b"synchro:v3:push-batch-fingerprint:v1\0";
const MUTATION_FINGERPRINT_DOMAIN: &[u8] = b"synchro:v3:push-mutation-fingerprint:v1\0";
const MAX_NORMALIZED_MUTATION_BYTES: usize = 65_536;
const MAX_JSON_DEPTH: usize = 128;
const MAX_JSON_VALUES_AND_NAMES: usize = 1_000_000;

/// Reports a Protocol 3 push fingerprint contract violation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FingerprintError {
    /// The request or mutation does not satisfy the public wire contract.
    Contract(ContractViolation),
    /// The authenticated identity is empty or is not I-JSON text.
    InvalidAuthenticatedUserId,
    /// The client identity is empty or is not I-JSON text.
    InvalidClientId,
    /// A normalized value is outside the I-JSON limits required by RFC 8785.
    InvalidIJson,
    /// One normalized mutation exceeds the Protocol 3 byte limit.
    NormalizedMutationTooLarge,
    /// RFC 8785 canonicalization failed.
    Canonicalization(String),
}

impl fmt::Display for FingerprintError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(error) => write!(formatter, "invalid push fingerprint input: {error}"),
            Self::InvalidAuthenticatedUserId => {
                formatter.write_str("authenticated user ID is invalid")
            }
            Self::InvalidClientId => formatter.write_str("client ID is invalid"),
            Self::InvalidIJson => formatter.write_str("fingerprint input is not valid I-JSON"),
            Self::NormalizedMutationTooLarge => {
                formatter.write_str("normalized mutation exceeds 65536 bytes")
            }
            Self::Canonicalization(error) => {
                write!(
                    formatter,
                    "canonicalize RFC 8785 fingerprint input: {error}"
                )
            }
        }
    }
}

impl std::error::Error for FingerprintError {}

/// Returns the RFC 8785 encoding of one normalized mutation.
pub fn normalized_mutation(mutation: &Mutation) -> Result<Vec<u8>, FingerprintError> {
    let (_, canonical) = canonical_normalized_mutation(mutation)?;
    Ok(canonical)
}

/// Returns the RFC 8785 encoding of one normalized batch.
pub fn normalized_batch(
    authenticated_user_id: &str,
    request: &PushRequest,
) -> Result<Vec<u8>, FingerprintError> {
    canonical_normalized_batch(authenticated_user_id, request)
}

/// Returns the exact batch-fingerprint hash input.
pub fn batch_fingerprint_preimage(
    authenticated_user_id: &str,
    request: &PushRequest,
) -> Result<Vec<u8>, FingerprintError> {
    let canonical = canonical_normalized_batch(authenticated_user_id, request)?;
    let mut preimage = Vec::with_capacity(BATCH_FINGERPRINT_DOMAIN.len() + canonical.len());
    preimage.extend_from_slice(BATCH_FINGERPRINT_DOMAIN);
    preimage.extend_from_slice(&canonical);
    Ok(preimage)
}

/// Computes the canonical scoped batch fingerprint.
pub fn batch_fingerprint(
    authenticated_user_id: &str,
    request: &PushRequest,
) -> Result<Sha256Digest, FingerprintError> {
    Ok(sha256_digest(&batch_fingerprint_preimage(
        authenticated_user_id,
        request,
    )?))
}

/// Returns the exact mutation-fingerprint hash input.
pub fn mutation_fingerprint_preimage(
    authenticated_user_id: &str,
    client_id: &str,
    mutation: &Mutation,
) -> Result<Vec<u8>, FingerprintError> {
    validate_authenticated_user_id(authenticated_user_id)?;
    validate_client_id(client_id)?;
    let (normalized, _) = canonical_normalized_mutation(mutation)?;
    let scope = Value::Array(vec![
        Value::String("mutation-scope-v1".into()),
        Value::String(authenticated_user_id.into()),
        Value::String(client_id.into()),
        normalized,
    ]);
    let canonical = canonicalize(&scope)?;
    let mut preimage = Vec::with_capacity(MUTATION_FINGERPRINT_DOMAIN.len() + canonical.len());
    preimage.extend_from_slice(MUTATION_FINGERPRINT_DOMAIN);
    preimage.extend_from_slice(&canonical);
    Ok(preimage)
}

/// Computes the canonical scoped mutation fingerprint.
pub fn mutation_fingerprint(
    authenticated_user_id: &str,
    client_id: &str,
    mutation: &Mutation,
) -> Result<Sha256Digest, FingerprintError> {
    Ok(sha256_digest(&mutation_fingerprint_preimage(
        authenticated_user_id,
        client_id,
        mutation,
    )?))
}

fn canonical_normalized_batch(
    authenticated_user_id: &str,
    request: &PushRequest,
) -> Result<Vec<u8>, FingerprintError> {
    validate_authenticated_user_id(authenticated_user_id)?;
    request.validate().map_err(FingerprintError::Contract)?;
    validate_client_id(&request.client_id)?;

    let mut mutations = Vec::with_capacity(request.mutations.len());
    for mutation in &request.mutations {
        let (normalized, _) = canonical_normalized_mutation(mutation)?;
        mutations.push(normalized);
    }

    let batch = Value::Array(vec![
        Value::String("batch-v1".into()),
        Value::String(authenticated_user_id.into()),
        Value::String(request.client_id.clone()),
        Value::String(request.client_generation.to_string()),
        Value::String(request.batch_id.clone()),
        schema_reference_value(&request.schema),
        Value::Array(mutations),
    ]);
    canonicalize(&batch)
}

fn canonical_normalized_mutation(
    mutation: &Mutation,
) -> Result<(Value, Vec<u8>), FingerprintError> {
    mutation.validate().map_err(FingerprintError::Contract)?;

    let primary_key = mutation
        .pk
        .as_object()
        .expect("Mutation::validate accepts only one-field primary-key objects");
    let (field_id, value) = primary_key
        .iter()
        .next()
        .expect("Mutation::validate accepts nonempty primary-key objects");
    let primary_key = Value::Array(vec![Value::String(field_id.clone()), value.clone()]);

    let base = match &mutation.base_version {
        Some(version) => Value::Array(vec![Value::from(1), Value::String(version.clone())]),
        None => Value::Array(vec![Value::from(0)]),
    };

    let columns = match &mutation.columns {
        Some(columns) => {
            let mut fields = columns
                .as_object()
                .expect("Mutation::validate accepts only object columns")
                .iter()
                .collect::<Vec<_>>();
            fields.sort_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
            let pairs = fields
                .into_iter()
                .map(|(field_id, value)| {
                    Value::Array(vec![Value::String(field_id.clone()), value.clone()])
                })
                .collect();
            Value::Array(vec![Value::from(1), Value::Array(pairs)])
        }
        None => Value::Array(vec![Value::from(0)]),
    };

    let normalized = Value::Array(vec![
        Value::String("mutation-v1".into()),
        Value::String(mutation.mutation_id.clone()),
        Value::String(mutation.table.clone()),
        primary_key,
        schema_reference_value(&mutation.authored_schema),
        Value::String(operation_name(mutation.op).into()),
        base,
        Value::String(mutation.client_version.clone()),
        columns,
    ]);
    let canonical = canonicalize(&normalized)?;
    if canonical.len() > MAX_NORMALIZED_MUTATION_BYTES {
        return Err(FingerprintError::NormalizedMutationTooLarge);
    }
    Ok((normalized, canonical))
}

fn schema_reference_value(schema: &SchemaRef) -> Value {
    Value::Array(vec![
        Value::String(schema.version.to_string()),
        Value::String(schema.hash.clone()),
    ])
}

fn operation_name(operation: Operation) -> &'static str {
    match operation {
        Operation::Insert => "insert",
        Operation::Upsert => "upsert",
        Operation::Update => "update",
        Operation::Delete => "delete",
    }
}

fn validate_authenticated_user_id(value: &str) -> Result<(), FingerprintError> {
    if value.is_empty() || !is_i_json_string(value) {
        return Err(FingerprintError::InvalidAuthenticatedUserId);
    }
    Ok(())
}

fn validate_client_id(value: &str) -> Result<(), FingerprintError> {
    if value.is_empty() || !is_i_json_string(value) {
        return Err(FingerprintError::InvalidClientId);
    }
    Ok(())
}

fn canonicalize(value: &Value) -> Result<Vec<u8>, FingerprintError> {
    let mut values_and_names = 0;
    validate_i_json(value, 0, &mut values_and_names)?;
    serde_json_canonicalizer::to_vec(value)
        .map_err(|error| FingerprintError::Canonicalization(error.to_string()))
}

fn validate_i_json(
    value: &Value,
    depth: usize,
    values_and_names: &mut usize,
) -> Result<(), FingerprintError> {
    *values_and_names += 1;
    if *values_and_names > MAX_JSON_VALUES_AND_NAMES {
        return Err(FingerprintError::InvalidIJson);
    }

    match value {
        Value::Null | Value::Bool(_) => Ok(()),
        Value::Number(_) => Ok(()),
        Value::String(value) if is_i_json_string(value) => Ok(()),
        Value::String(_) => Err(FingerprintError::InvalidIJson),
        Value::Array(values) => {
            if depth >= MAX_JSON_DEPTH {
                return Err(FingerprintError::InvalidIJson);
            }
            for value in values {
                validate_i_json(value, depth + 1, values_and_names)?;
            }
            Ok(())
        }
        Value::Object(entries) => {
            if depth >= MAX_JSON_DEPTH {
                return Err(FingerprintError::InvalidIJson);
            }
            for (name, value) in entries {
                *values_and_names += 1;
                if !is_i_json_string(name) {
                    return Err(FingerprintError::InvalidIJson);
                }
                validate_i_json(value, depth + 1, values_and_names)?;
            }
            Ok(())
        }
    }
}

fn is_i_json_string(value: &str) -> bool {
    !value.chars().any(is_unicode_noncharacter)
}

fn is_unicode_noncharacter(character: char) -> bool {
    matches!(character as u32, 0xfdd0..=0xfdef)
        || matches!(character as u32 & 0xffff, 0xfffe | 0xffff)
}

fn sha256_digest(input: &[u8]) -> Sha256Digest {
    let digest = Sha256::digest(input);
    let mut bytes = [0_u8; 32];
    bytes.copy_from_slice(&digest);
    Sha256Digest::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::fmt;

    use serde::de::{self, MapAccess, SeqAccess, Visitor};
    use serde::{Deserialize, Deserializer};
    use serde_json::{Map, Number};

    use super::*;

    const MAX_BATCH_REQUEST_BYTES: usize = 1_048_576;
    const HASH: &str = "a97280b716fe0f8a9553ba7c3b31b00dd03f7c7aacf0ff01a703d73182f3df31";

    fn vector_file() -> String {
        let path = std::env::var_os("SYNCHRO_REPO_ROOT")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
            .join("conformance/vectors/canonical-v1.json");
        std::fs::read_to_string(path).expect("canonical vector file must be readable")
    }

    #[derive(Deserialize)]
    struct VectorDocument {
        vectors: Vec<Vector>,
    }

    #[derive(Deserialize)]
    struct Vector {
        vector_id: String,
        kind: String,
        valid: bool,
        input: Value,
        expected: Expected,
    }

    #[derive(Deserialize)]
    struct Expected {
        canonical_bytes_hex: Option<String>,
        expected_sha256: Option<String>,
    }

    #[test]
    fn batch_fingerprints_match_all_authored_vectors() {
        let document: VectorDocument = serde_json::from_str(&vector_file()).unwrap();
        for vector in document
            .vectors
            .into_iter()
            .filter(|vector| vector.kind == "batch_fingerprint")
        {
            let result = fingerprint_batch_vector(&vector);
            assert_vector_result(&vector, result);
        }
    }

    #[test]
    fn mutation_fingerprints_match_all_authored_vectors() {
        let document: VectorDocument = serde_json::from_str(&vector_file()).unwrap();
        for vector in document
            .vectors
            .into_iter()
            .filter(|vector| vector.kind == "mutation_fingerprint")
        {
            let result = fingerprint_mutation_vector(&vector);
            assert_vector_result(&vector, result);
        }
    }

    #[test]
    fn fingerprints_bind_identity_values_and_mutation_order() {
        let first = mutation("00000000-0000-4000-8000-000000000101", "first");
        let second = mutation("00000000-0000-4000-8000-000000000102", "second");

        let canonical_mutation = mutation_fingerprint("user-a", "client-a", &first).unwrap();
        assert_ne!(
            canonical_mutation,
            mutation_fingerprint("user-b", "client-a", &first).unwrap()
        );
        assert_ne!(
            canonical_mutation,
            mutation_fingerprint("user-a", "client-b", &first).unwrap()
        );

        let mut null_column = first.clone();
        null_column.columns = Some(serde_json::json!({ "fld_title": null }));
        assert_ne!(
            canonical_mutation,
            mutation_fingerprint("user-a", "client-a", &null_column).unwrap()
        );

        let ordered_request = request(vec![first.clone(), second.clone()]);
        let reversed = request(vec![second, first]);
        assert_ne!(
            batch_fingerprint("user-a", &ordered_request).unwrap(),
            batch_fingerprint("user-a", &reversed).unwrap()
        );

        let mut changed_generation = ordered_request;
        changed_generation.client_generation = 2;
        assert_ne!(
            batch_fingerprint("user-a", &changed_generation).unwrap(),
            batch_fingerprint("user-b", &changed_generation).unwrap()
        );
    }

    #[test]
    fn normalized_entry_points_return_the_canonical_payloads() {
        let first = mutation("00000000-0000-4000-8000-000000000101", "first");
        assert_eq!(
            normalized_mutation(&first).unwrap(),
            br#"["mutation-v1","00000000-0000-4000-8000-000000000101","tbl_records",["fld_id","record-1"],["1","a97280b716fe0f8a9553ba7c3b31b00dd03f7c7aacf0ff01a703d73182f3df31"],"insert",[0],"2024-02-29T12:34:56.123456Z",[1,[["fld_title","first"]]]]"#
        );

        let request = request(vec![first]);
        let normalized = normalized_batch("user-a", &request).unwrap();
        let preimage = batch_fingerprint_preimage("user-a", &request).unwrap();
        assert_eq!(
            preimage.strip_prefix(BATCH_FINGERPRINT_DOMAIN).unwrap(),
            normalized
        );
        assert_eq!(
            normalized,
            br#"["batch-v1","user-a","client-a","1","00000000-0000-4000-8000-000000000100",["1","a97280b716fe0f8a9553ba7c3b31b00dd03f7c7aacf0ff01a703d73182f3df31"],[["mutation-v1","00000000-0000-4000-8000-000000000101","tbl_records",["fld_id","record-1"],["1","a97280b716fe0f8a9553ba7c3b31b00dd03f7c7aacf0ff01a703d73182f3df31"],"insert",[0],"2024-02-29T12:34:56.123456Z",[1,[["fld_title","first"]]]]]]"#
        );
    }

    #[test]
    fn fingerprint_scope_rejects_empty_and_noncharacter_identities() {
        let first = mutation("00000000-0000-4000-8000-000000000101", "first");
        let mut request = request(vec![first.clone()]);
        for user in ["", "\u{fdd0}", "\u{fffe}", "\u{1fffe}"] {
            assert!(batch_fingerprint_preimage(user, &request).is_err());
            assert!(mutation_fingerprint_preimage(user, "client-a", &first).is_err());
        }
        for client in ["", "\u{fdd0}", "\u{fffe}", "\u{1fffe}"] {
            assert!(mutation_fingerprint_preimage("user-a", client, &first).is_err());
            request.client_id = client.into();
            assert!(batch_fingerprint_preimage("user-a", &request).is_err());
        }
    }

    #[test]
    fn fingerprint_i_json_validation_enforces_counts_and_depth() {
        let mut count = 2;
        validate_i_json(&Value::Null, 0, &mut count).expect("one value must be valid");
        assert_eq!(count, 3);

        let mut count = MAX_JSON_VALUES_AND_NAMES - 1;
        validate_i_json(&Value::Null, 0, &mut count).expect("limit must be inclusive");
        assert_eq!(count, MAX_JSON_VALUES_AND_NAMES);
        assert!(validate_i_json(&Value::Null, 0, &mut count).is_err());

        let array = Value::Array(vec![Value::Null]);
        let mut count = 0;
        validate_i_json(&array, MAX_JSON_DEPTH - 1, &mut count)
            .expect("last supported nesting level must be valid");
        let mut count = 0;
        assert!(validate_i_json(&array, MAX_JSON_DEPTH, &mut count).is_err());

        let object = serde_json::json!({"name": [null]});
        let mut count = 0;
        validate_i_json(&object, 0, &mut count).expect("object must be valid I-JSON");
        assert_eq!(count, 4);

        let mut nested_array = Value::Null;
        for _ in 0..MAX_JSON_DEPTH {
            nested_array = Value::Array(vec![nested_array]);
        }
        let mut count = 0;
        validate_i_json(&nested_array, 0, &mut count).expect("maximum array depth must be valid");
        nested_array = Value::Array(vec![nested_array]);
        let mut count = 0;
        assert!(validate_i_json(&nested_array, 0, &mut count).is_err());

        let mut nested_object = Value::Null;
        for _ in 0..MAX_JSON_DEPTH {
            nested_object = Value::Object(Map::from_iter([("name".into(), nested_object)]));
        }
        let mut count = 0;
        validate_i_json(&nested_object, 0, &mut count).expect("maximum object depth must be valid");
        nested_object = Value::Object(Map::from_iter([("name".into(), nested_object)]));
        let mut count = 0;
        assert!(validate_i_json(&nested_object, 0, &mut count).is_err());

        let object = Value::Object(Map::from_iter([("name".into(), Value::Null)]));
        let mut count = MAX_JSON_VALUES_AND_NAMES - 3;
        validate_i_json(&object, 0, &mut count).expect("object count limit must be inclusive");
        assert_eq!(count, MAX_JSON_VALUES_AND_NAMES);
        let mut count = MAX_JSON_VALUES_AND_NAMES - 2;
        assert!(validate_i_json(&object, 0, &mut count).is_err());

        let invalid_name = Value::Object(Map::from_iter([("\u{fdd0}".into(), Value::Null)]));
        let mut count = 0;
        assert!(validate_i_json(&invalid_name, 0, &mut count).is_err());
        let mut count = 0;
        assert!(validate_i_json(&Value::String("\u{fdd0}".into()), 0, &mut count,).is_err());
        assert!(is_i_json_string("plain"));
        for character in ['\u{fdd0}', '\u{fffe}', '\u{1fffe}', '\u{10ffff}'] {
            assert!(!is_i_json_string(&character.to_string()));
        }
    }

    fn fingerprint_batch_vector(vector: &Vector) -> Result<(Vec<u8>, Sha256Digest), String> {
        let input = object(&vector.input)?;
        let authenticated_user_id = required_string(input, "authenticated_user_id")?;
        let batch_json = required_string(input, "batch_json")?;
        let request = legacy_batch(&batch_json)?;
        let preimage = batch_fingerprint_preimage(&authenticated_user_id, &request)
            .map_err(|error| error.to_string())?;
        let digest = batch_fingerprint(&authenticated_user_id, &request)
            .map_err(|error| error.to_string())?;
        Ok((preimage, digest))
    }

    fn fingerprint_mutation_vector(vector: &Vector) -> Result<(Vec<u8>, Sha256Digest), String> {
        let input = object(&vector.input)?;
        let authenticated_user_id = required_string(input, "authenticated_user_id")?;
        let client_id = required_string(input, "client_id")?;
        let mutation_json = required_string(input, "mutation_json")?;
        let mutation = legacy_mutation_json(&mutation_json)?;
        let preimage = mutation_fingerprint_preimage(&authenticated_user_id, &client_id, &mutation)
            .map_err(|error| error.to_string())?;
        let digest = mutation_fingerprint(&authenticated_user_id, &client_id, &mutation)
            .map_err(|error| error.to_string())?;
        Ok((preimage, digest))
    }

    fn assert_vector_result(vector: &Vector, result: Result<(Vec<u8>, Sha256Digest), String>) {
        if !vector.valid {
            assert!(result.is_err(), "{} must fail", vector.vector_id);
            return;
        }

        let (preimage, digest) = result.unwrap_or_else(|error| {
            panic!("{} must succeed: {error}", vector.vector_id);
        });
        assert_eq!(
            encode_lower_hex(&preimage),
            vector
                .expected
                .canonical_bytes_hex
                .as_deref()
                .expect("valid vector has canonical bytes"),
            "{} preimage",
            vector.vector_id
        );
        assert_eq!(
            digest.to_lower_hex(),
            vector
                .expected
                .expected_sha256
                .as_deref()
                .expect("valid vector has a digest"),
            "{} digest",
            vector.vector_id
        );
    }

    fn legacy_batch(raw: &str) -> Result<PushRequest, String> {
        if raw.len() > MAX_BATCH_REQUEST_BYTES {
            return Err("batch request exceeds byte limit".into());
        }
        let value = strict_json_value(raw)?;
        let batch_object = object(&value)?;
        require_keys(
            batch_object,
            &[
                "client_id",
                "client_generation",
                "batch_id",
                "request_schema",
                "mutations",
            ],
            &[],
        )?;
        let mutations = batch_object
            .get("mutations")
            .and_then(Value::as_array)
            .ok_or_else(|| "mutations is not an array".to_owned())?
            .iter()
            .cloned()
            .map(legacy_mutation_value)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(PushRequest {
            client_id: required_string(batch_object, "client_id")?,
            client_generation: required_i64(batch_object, "client_generation")?,
            batch_id: required_string(batch_object, "batch_id")?,
            schema: schema_reference(
                batch_object
                    .get("request_schema")
                    .ok_or_else(|| "request_schema is missing".to_owned())?,
            )?,
            mutations,
        })
    }

    fn legacy_mutation_json(raw: &str) -> Result<Mutation, String> {
        legacy_mutation_value(strict_json_value(raw)?)
    }

    fn legacy_mutation_value(value: Value) -> Result<Mutation, String> {
        let mutation_object = object(&value)?;
        require_keys(
            mutation_object,
            &[
                "mutation_id",
                "table_id",
                "pk",
                "authored_schema",
                "operation",
                "client_version",
            ],
            &["base_version", "columns"],
        )?;

        let pk = object(
            mutation_object
                .get("pk")
                .ok_or_else(|| "pk is missing".to_owned())?,
        )?;
        require_keys(pk, &["field_id", "value"], &[])?;
        let primary_key = Value::Object(Map::from_iter([(
            required_string(pk, "field_id")?,
            pk.get("value")
                .cloned()
                .ok_or_else(|| "pk.value is missing".to_owned())?,
        )]));

        let base_version = match mutation_object.get("base_version") {
            Some(Value::String(value)) => Some(value.clone()),
            Some(_) => return Err("base_version is not a string".into()),
            None => None,
        };
        let columns = match mutation_object.get("columns") {
            Some(Value::Array(values)) => Some(legacy_columns(values)?),
            Some(_) => return Err("columns is not an array".into()),
            None => None,
        };

        Ok(Mutation {
            mutation_id: required_string(mutation_object, "mutation_id")?,
            table: required_string(mutation_object, "table_id")?,
            pk: primary_key,
            authored_schema: schema_reference(
                mutation_object
                    .get("authored_schema")
                    .ok_or_else(|| "authored_schema is missing".to_owned())?,
            )?,
            op: serde_json::from_value(
                mutation_object
                    .get("operation")
                    .cloned()
                    .ok_or_else(|| "operation is missing".to_owned())?,
            )
            .map_err(|error| format!("operation is invalid: {error}"))?,
            base_version,
            client_version: required_string(mutation_object, "client_version")?,
            columns,
        })
    }

    fn legacy_columns(values: &[Value]) -> Result<Value, String> {
        let mut columns = Map::new();
        for value in values {
            let column = object(value)?;
            require_keys(column, &["field_id", "value"], &[])?;
            let field_id = required_string(column, "field_id")?;
            let value = column
                .get("value")
                .cloned()
                .ok_or_else(|| "column.value is missing".to_owned())?;
            if columns.insert(field_id.clone(), value).is_some() {
                return Err(format!("duplicate column field ID {field_id:?}"));
            }
        }
        Ok(Value::Object(columns))
    }

    fn schema_reference(value: &Value) -> Result<SchemaRef, String> {
        serde_json::from_value(value.clone()).map_err(|error| format!("schema is invalid: {error}"))
    }

    fn object(value: &Value) -> Result<&Map<String, Value>, String> {
        value
            .as_object()
            .ok_or_else(|| "value is not an object".to_owned())
    }

    fn require_keys(
        object: &Map<String, Value>,
        required: &[&str],
        optional: &[&str],
    ) -> Result<(), String> {
        if object.len() < required.len()
            || required.iter().any(|name| !object.contains_key(*name))
            || object.keys().any(|name| {
                !required.contains(&name.as_str()) && !optional.contains(&name.as_str())
            })
        {
            return Err("object members are invalid".into());
        }
        Ok(())
    }

    fn required_string(object: &Map<String, Value>, name: &str) -> Result<String, String> {
        object
            .get(name)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .ok_or_else(|| format!("{name} is not a nonempty string"))
    }

    fn required_i64(object: &Map<String, Value>, name: &str) -> Result<i64, String> {
        object
            .get(name)
            .and_then(Value::as_i64)
            .ok_or_else(|| format!("{name} is not an integer"))
    }

    fn strict_json_value(raw: &str) -> Result<Value, String> {
        serde_json::from_str::<UniqueValue>(raw)
            .map(|value| value.0)
            .map_err(|error| format!("invalid JSON: {error}"))
    }

    struct UniqueValue(Value);

    impl<'de> Deserialize<'de> for UniqueValue {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct UniqueValueVisitor;

            impl<'de> Visitor<'de> for UniqueValueVisitor {
                type Value = UniqueValue;

                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str("a JSON value with unique object members")
                }

                fn visit_unit<E>(self) -> Result<Self::Value, E>
                where
                    E: de::Error,
                {
                    Ok(UniqueValue(Value::Null))
                }

                fn visit_none<E>(self) -> Result<Self::Value, E>
                where
                    E: de::Error,
                {
                    Ok(UniqueValue(Value::Null))
                }

                fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
                where
                    E: de::Error,
                {
                    Ok(UniqueValue(Value::Bool(value)))
                }

                fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
                where
                    E: de::Error,
                {
                    Ok(UniqueValue(Value::Number(Number::from(value))))
                }

                fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
                where
                    E: de::Error,
                {
                    Ok(UniqueValue(Value::Number(Number::from(value))))
                }

                fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
                where
                    E: de::Error,
                {
                    Number::from_f64(value)
                        .map(|number| UniqueValue(Value::Number(number)))
                        .ok_or_else(|| E::custom("JSON number is not finite"))
                }

                fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
                where
                    E: de::Error,
                {
                    Ok(UniqueValue(Value::String(value.to_owned())))
                }

                fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
                where
                    E: de::Error,
                {
                    Ok(UniqueValue(Value::String(value)))
                }

                fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
                where
                    A: SeqAccess<'de>,
                {
                    let mut values = Vec::new();
                    while let Some(value) = sequence.next_element::<UniqueValue>()? {
                        values.push(value.0);
                    }
                    Ok(UniqueValue(Value::Array(values)))
                }

                fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where
                    A: MapAccess<'de>,
                {
                    let mut object = Map::new();
                    let mut names = HashSet::new();
                    while let Some((name, value)) = map.next_entry::<String, UniqueValue>()? {
                        if !names.insert(name.clone()) {
                            return Err(de::Error::custom("duplicate JSON object member"));
                        }
                        object.insert(name, value.0);
                    }
                    Ok(UniqueValue(Value::Object(object)))
                }
            }

            deserializer.deserialize_any(UniqueValueVisitor)
        }
    }

    fn mutation(id: &str, title: &str) -> Mutation {
        Mutation {
            mutation_id: id.into(),
            table: "tbl_records".into(),
            pk: serde_json::json!({ "fld_id": "record-1" }),
            authored_schema: SchemaRef {
                version: 1,
                hash: HASH.into(),
            },
            op: Operation::Insert,
            base_version: None,
            client_version: "2024-02-29T12:34:56.123456Z".into(),
            columns: Some(serde_json::json!({ "fld_title": title })),
        }
    }

    fn request(mutations: Vec<Mutation>) -> PushRequest {
        PushRequest {
            client_id: "client-a".into(),
            client_generation: 1,
            batch_id: "00000000-0000-4000-8000-000000000100".into(),
            schema: SchemaRef {
                version: 1,
                hash: HASH.into(),
            },
            mutations,
        }
    }

    fn encode_lower_hex(value: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut output = String::with_capacity(value.len() * 2);
        for byte in value {
            output.push(HEX[usize::from(byte >> 4)] as char);
            output.push(HEX[usize::from(byte & 0x0f)] as char);
        }
        output
    }
}
