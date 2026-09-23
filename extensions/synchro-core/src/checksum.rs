//! Protocol version 3 canonical typed encodings and integrity digests.

use std::collections::{BTreeMap, HashSet};
use std::fmt;

use chrono::NaiveDate;
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;
use serde_json::Value;
use sha2::{Digest, Sha256};

const ROW_ID_DOMAIN: &[u8] = b"synchro:v3:row-identity:v1\0";
const ROW_DIGEST_DOMAIN: &[u8] = b"synchro:v3:row-digest:v1\0";
const SCOPE_DIGEST_DOMAIN: &[u8] = b"synchro:v3:scope-digest:v1\0";
const SHA256_HEX_LENGTH: usize = 64;
const MAX_JSON_DEPTH: usize = 128;
const MAX_JSON_VALUES_AND_NAMES: usize = 1_000_000;
const MAX_SAFE_JSON_INTEGER: f64 = 9_007_199_254_740_991.0;

/// Reports a canonical encoding or digest contract violation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DigestError {
    message: String,
}

impl DigestError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for DigestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for DigestError {}

/// The raw 32-octet SHA-256 result used by protocol version 3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Sha256Digest([u8; 32]);

impl Sha256Digest {
    /// Creates a digest from its raw SHA-256 octets.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Decodes exactly 64 lowercase hexadecimal characters.
    pub fn from_lower_hex(value: &str) -> Result<Self, DigestError> {
        let bytes = decode_lower_sha256(value)?;
        Ok(Self(bytes))
    }

    /// Returns the raw SHA-256 octets.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Encodes the digest as exactly 64 lowercase hexadecimal characters.
    pub fn to_lower_hex(self) -> String {
        encode_lower_hex(&self.0)
    }
}

/// The immutable raw schema hash used by row and scope digests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SchemaHash([u8; 32]);

impl SchemaHash {
    /// Creates a schema hash from its verified raw SHA-256 octets.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Decodes an exact lowercase hexadecimal schema hash.
    pub fn from_lower_hex(value: &str) -> Result<Self, DigestError> {
        Ok(Self(decode_lower_sha256(value)?))
    }

    /// Returns the raw schema hash octets.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Encodes the schema hash as exactly 64 lowercase hexadecimal characters.
    pub fn to_lower_hex(self) -> String {
        encode_lower_hex(&self.0)
    }
}

/// A supported protocol version 3 portable logical type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PortableType {
    String,
    Int,
    Int64,
    Decimal,
    Float,
    Boolean,
    Datetime,
    Date,
    Time,
    Json,
    Bytes,
}

impl PortableType {
    /// Parses one exact protocol portable type name.
    pub fn parse(value: &str) -> Result<Self, DigestError> {
        match value {
            "string" => Ok(Self::String),
            "int" => Ok(Self::Int),
            "int64" => Ok(Self::Int64),
            "decimal" => Ok(Self::Decimal),
            "float" => Ok(Self::Float),
            "boolean" => Ok(Self::Boolean),
            "datetime" => Ok(Self::Datetime),
            "date" => Ok(Self::Date),
            "time" => Ok(Self::Time),
            "json" => Ok(Self::Json),
            "bytes" => Ok(Self::Bytes),
            _ => Err(DigestError::new(format!(
                "unsupported portable type {value:?}"
            ))),
        }
    }

    /// Returns the protocol type name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Int => "int",
            Self::Int64 => "int64",
            Self::Decimal => "decimal",
            Self::Float => "float",
            Self::Boolean => "boolean",
            Self::Datetime => "datetime",
            Self::Date => "date",
            Self::Time => "time",
            Self::Json => "json",
            Self::Bytes => "bytes",
        }
    }

    const fn tag(self) -> u8 {
        match self {
            Self::String => 0x01,
            Self::Int => 0x02,
            Self::Int64 => 0x03,
            Self::Decimal => 0x04,
            Self::Float => 0x05,
            Self::Boolean => 0x06,
            Self::Datetime => 0x07,
            Self::Date => 0x08,
            Self::Time => 0x09,
            Self::Json => 0x0a,
            Self::Bytes => 0x0b,
        }
    }
}

/// The immutable portable type and nullability for one logical field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldSpec {
    portable_type: PortableType,
    nullable: bool,
    precision: Option<u32>,
    scale: Option<u32>,
}

impl FieldSpec {
    /// Creates a validated protocol field specification.
    pub fn new(
        portable_type: PortableType,
        nullable: bool,
        precision: Option<u32>,
        scale: Option<u32>,
    ) -> Result<Self, DigestError> {
        match portable_type {
            PortableType::Decimal => {
                let (precision, scale) = precision.zip(scale).ok_or_else(|| {
                    DigestError::new("decimal field requires precision and scale")
                })?;
                if precision == 0 || scale > precision {
                    return Err(DigestError::new("decimal precision or scale is invalid"));
                }
            }
            _ if precision.is_some() || scale.is_some() => {
                return Err(DigestError::new("non-decimal field has precision or scale"));
            }
            _ => {}
        }

        Ok(Self {
            portable_type,
            nullable,
            precision,
            scale,
        })
    }

    /// Returns the exact portable type.
    pub const fn portable_type(&self) -> PortableType {
        self.portable_type
    }

    /// Reports whether SQL null is valid for this field.
    pub const fn nullable(&self) -> bool {
        self.nullable
    }

    /// Returns the decimal precision when the type is decimal.
    pub const fn precision(&self) -> Option<u32> {
        self.precision
    }

    /// Returns the decimal scale when the type is decimal.
    pub const fn scale(&self) -> Option<u32> {
        self.scale
    }
}

/// One immutable field ID and its protocol field specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalField {
    field_id: String,
    spec: FieldSpec,
}

impl CanonicalField {
    /// Creates one field for a canonical table.
    pub fn new(field_id: impl Into<String>, spec: FieldSpec) -> Result<Self, DigestError> {
        let field_id = field_id.into();
        require_nonempty_text(&field_id, "field_id")?;
        Ok(Self { field_id, spec })
    }

    /// Returns the immutable logical field ID.
    pub fn field_id(&self) -> &str {
        &self.field_id
    }

    /// Returns the field specification.
    pub fn spec(&self) -> &FieldSpec {
        &self.spec
    }
}

/// The integrity-relevant immutable table state from a schema manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalTable {
    table_id: String,
    primary_key_field_id: String,
    fields: Vec<CanonicalField>,
}

impl CanonicalTable {
    /// Creates a validated logical table for canonical row encoding.
    pub fn new(
        table_id: impl Into<String>,
        primary_key_field_id: impl Into<String>,
        fields: Vec<CanonicalField>,
    ) -> Result<Self, DigestError> {
        let table_id = table_id.into();
        let primary_key_field_id = primary_key_field_id.into();
        require_nonempty_text(&table_id, "table_id")?;
        require_nonempty_text(&primary_key_field_id, "primary_key_field_id")?;
        if fields.is_empty() {
            return Err(DigestError::new("table has no fields"));
        }

        let mut seen = HashSet::with_capacity(fields.len());
        for field in &fields {
            if !seen.insert(field.field_id.as_str()) {
                return Err(DigestError::new(format!(
                    "table has duplicate field_id {:?}",
                    field.field_id
                )));
            }
        }

        let primary = fields
            .iter()
            .find(|field| field.field_id == primary_key_field_id)
            .ok_or_else(|| DigestError::new("primary_key_field_id is unknown"))?;
        if primary.spec.nullable
            || !matches!(
                primary.spec.portable_type,
                PortableType::String | PortableType::Int | PortableType::Int64
            )
        {
            return Err(DigestError::new(
                "primary key type or nullability is invalid",
            ));
        }

        Ok(Self {
            table_id,
            primary_key_field_id,
            fields,
        })
    }

    /// Returns the immutable logical table ID.
    pub fn table_id(&self) -> &str {
        &self.table_id
    }

    /// Returns the immutable logical primary-key field ID.
    pub fn primary_key_field_id(&self) -> &str {
        &self.primary_key_field_id
    }

    /// Returns the complete synchronized field set.
    pub fn fields(&self) -> &[CanonicalField] {
        &self.fields
    }

    fn field(&self, field_id: &str) -> Option<&CanonicalField> {
        self.fields.iter().find(|field| field.field_id == field_id)
    }

    fn primary_key_field(&self) -> &CanonicalField {
        self.field(&self.primary_key_field_id)
            .expect("CanonicalTable validates its primary key field")
    }
}

/// One raw JSON value addressed by immutable logical field ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowField {
    field_id: String,
    value_json: String,
}

impl RowField {
    /// Creates one raw JSON row field.
    pub fn new(
        field_id: impl Into<String>,
        value_json: impl Into<String>,
    ) -> Result<Self, DigestError> {
        let field_id = field_id.into();
        let value_json = value_json.into();
        require_nonempty_text(&field_id, "row field_id")?;
        validate_json_document(&value_json, false)?;
        Ok(Self {
            field_id,
            value_json,
        })
    }

    /// Returns the immutable logical field ID.
    pub fn field_id(&self) -> &str {
        &self.field_id
    }

    /// Returns the raw canonical wire JSON value.
    pub fn value_json(&self) -> &str {
        &self.value_json
    }
}

/// A complete authoritative row with its separate primary-key wire value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalRow {
    primary_key_json: String,
    fields: Vec<RowField>,
}

impl CanonicalRow {
    /// Creates a row from a separate primary key and complete field values.
    pub fn new(
        primary_key_json: impl Into<String>,
        fields: Vec<RowField>,
    ) -> Result<Self, DigestError> {
        let primary_key_json = primary_key_json.into();
        validate_json_document(&primary_key_json, false)?;
        let mut seen = HashSet::with_capacity(fields.len());
        for field in &fields {
            if !seen.insert(field.field_id.as_str()) {
                return Err(DigestError::new(format!(
                    "row has duplicate field_id {:?}",
                    field.field_id
                )));
            }
        }
        Ok(Self {
            primary_key_json,
            fields,
        })
    }

    /// Parses an object row without accepting duplicate JSON member names.
    pub fn from_json(
        primary_key_json: impl Into<String>,
        row_json: &str,
    ) -> Result<Self, DigestError> {
        validate_json_document(row_json, false)?;
        let raw_fields = serde_json::from_str::<BTreeMap<String, Box<RawValue>>>(row_json)
            .map_err(|error| DigestError::new(format!("row is not a JSON object: {error}")))?;
        let fields = raw_fields
            .into_iter()
            .map(|(field_id, value)| RowField::new(field_id, value.get().to_owned()))
            .collect::<Result<Vec<_>, _>>()?;
        Self::new(primary_key_json, fields)
    }

    /// Returns the separate primary-key wire JSON value.
    pub fn primary_key_json(&self) -> &str {
        &self.primary_key_json
    }

    /// Returns the complete row fields.
    pub fn fields(&self) -> &[RowField] {
        &self.fields
    }
}

/// A validated canonical row identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RowIdentity(Vec<u8>);

impl RowIdentity {
    /// Validates and stores an encoded row identity.
    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> Result<Self, DigestError> {
        let bytes = bytes.into();
        validate_row_identity(&bytes)?;
        Ok(Self(bytes))
    }

    /// Decodes lowercase hexadecimal row-identity octets.
    pub fn from_lower_hex(value: &str) -> Result<Self, DigestError> {
        Self::from_bytes(decode_lower_hex(value)?)
    }

    /// Returns the encoded row identity octets.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Returns the encoded row identity as lowercase hexadecimal.
    pub fn to_lower_hex(&self) -> String {
        encode_lower_hex(&self.0)
    }

    /// Consumes the identity and returns its encoded octets.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

/// A canonical row identity and its verified row digest for scope hashing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeDigestEntry {
    pub row_identity: RowIdentity,
    pub row_digest: Sha256Digest,
}

impl ScopeDigestEntry {
    /// Creates one validated scope entry.
    pub const fn new(row_identity: RowIdentity, row_digest: Sha256Digest) -> Self {
        Self {
            row_identity,
            row_digest,
        }
    }
}

/// The exact protocol version 3 checksum object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChecksumObject {
    digest: Sha256Digest,
}

impl ChecksumObject {
    /// Creates the exact SHA-256 hexadecimal checksum object.
    pub const fn new(digest: Sha256Digest) -> Self {
        Self { digest }
    }

    /// Parses and validates the exact protocol checksum object shape.
    pub fn from_json(value: &str) -> Result<Self, DigestError> {
        serde_json::from_str(value)
            .map_err(|error| DigestError::new(format!("invalid checksum object: {error}")))
    }

    /// Returns the checksum digest.
    pub const fn digest(&self) -> Sha256Digest {
        self.digest
    }

    /// Returns the fixed wire algorithm.
    pub const fn algorithm(&self) -> &'static str {
        "sha256"
    }

    /// Returns the fixed wire object version.
    pub const fn version(&self) -> u8 {
        1
    }

    /// Returns the fixed digest encoding.
    pub const fn encoding(&self) -> &'static str {
        "hex"
    }
}

impl Serialize for ChecksumObject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut object = serializer.serialize_struct("ChecksumObject", 4)?;
        object.serialize_field("algorithm", self.algorithm())?;
        object.serialize_field("version", &self.version())?;
        object.serialize_field("encoding", self.encoding())?;
        object.serialize_field("digest", &self.digest.to_lower_hex())?;
        object.end()
    }
}

impl<'de> Deserialize<'de> for ChecksumObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireChecksumObject {
            algorithm: String,
            version: u8,
            encoding: String,
            digest: String,
        }

        let wire = WireChecksumObject::deserialize(deserializer)?;
        if wire.algorithm != "sha256" {
            return Err(serde::de::Error::custom(
                "checksum algorithm must be sha256",
            ));
        }
        if wire.version != 1 {
            return Err(serde::de::Error::custom("checksum version must be 1"));
        }
        if wire.encoding != "hex" {
            return Err(serde::de::Error::custom("checksum encoding must be hex"));
        }
        let digest = Sha256Digest::from_lower_hex(&wire.digest)
            .map_err(|error| serde::de::Error::custom(error.to_string()))?;
        Ok(Self::new(digest))
    }
}

/// Encodes a validated canonical wire value as a protocol typed value.
pub fn encode_typed_value(spec: &FieldSpec, raw_json: &str) -> Result<Vec<u8>, DigestError> {
    let raw_json = raw_json.trim();
    if raw_json.is_empty() {
        return Err(DigestError::new("typed value is empty"));
    }

    if raw_json == "null" {
        if !spec.nullable {
            return Err(DigestError::new("non-nullable field contains null"));
        }
        return Ok(vec![spec.portable_type.tag(), 0x00]);
    }

    let payload = typed_payload(spec, raw_json)?;
    let mut encoded = Vec::with_capacity(payload.len() + 2);
    encoded.extend_from_slice(&[spec.portable_type.tag(), 0x01]);
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

/// Computes the canonical row identity from the table's typed primary key.
pub fn row_identity(
    table: &CanonicalTable,
    primary_key_json: &str,
) -> Result<RowIdentity, DigestError> {
    let encoded_primary_key =
        encode_typed_value(table.primary_key_field().spec(), primary_key_json)
            .map_err(|error| DigestError::new(format!("encode primary key: {error}")))?;
    if encoded_primary_key.get(1) != Some(&0x01) {
        return Err(DigestError::new("primary key is null"));
    }

    let mut identity = Vec::with_capacity(
        ROW_ID_DOMAIN.len()
            + table.table_id.len()
            + table.primary_key_field_id.len()
            + encoded_primary_key.len()
            + 16,
    );
    identity.extend_from_slice(ROW_ID_DOMAIN);
    append_text(&mut identity, &table.table_id)?;
    append_text(&mut identity, &table.primary_key_field_id)?;
    identity.extend_from_slice(&encoded_primary_key);
    RowIdentity::from_bytes(identity)
}

/// Returns the exact SHA-256 preimage for a canonical row digest.
pub fn row_digest_preimage(
    schema_hash: SchemaHash,
    table: &CanonicalTable,
    row: &CanonicalRow,
    server_version: &str,
) -> Result<Vec<u8>, DigestError> {
    require_nonempty_text(server_version, "server_version")?;
    let row_body = encode_row_body(table, row)?;
    let identity = row_identity(table, row.primary_key_json())?;

    let mut preimage = Vec::with_capacity(
        ROW_DIGEST_DOMAIN.len()
            + schema_hash.as_bytes().len()
            + identity.as_bytes().len()
            + row_body.len()
            + server_version.len()
            + 24,
    );
    preimage.extend_from_slice(ROW_DIGEST_DOMAIN);
    preimage.extend_from_slice(schema_hash.as_bytes());
    append_blob(&mut preimage, identity.as_bytes())?;
    append_blob(&mut preimage, &row_body)?;
    append_text(&mut preimage, server_version)?;
    Ok(preimage)
}

/// Computes the canonical SHA-256 row digest.
pub fn row_digest(
    schema_hash: SchemaHash,
    table: &CanonicalTable,
    row: &CanonicalRow,
    server_version: &str,
) -> Result<Sha256Digest, DigestError> {
    let preimage = row_digest_preimage(schema_hash, table, row, server_version)?;
    Ok(sha256_digest(&preimage))
}

/// Returns the exact SHA-256 preimage for one ordered scope digest.
pub fn scope_digest_preimage(
    schema_hash: SchemaHash,
    scope_id: &str,
    entries: &[ScopeDigestEntry],
) -> Result<Vec<u8>, DigestError> {
    require_nonempty_text(scope_id, "scope_id")?;
    let ordered = ordered_scope_entries(entries)?;
    let mut preimage = Vec::with_capacity(
        SCOPE_DIGEST_DOMAIN.len()
            + schema_hash.as_bytes().len()
            + scope_id.len()
            + ordered
                .iter()
                .map(|entry| entry.row_identity.as_bytes().len() + 40)
                .sum::<usize>()
            + 16,
    );
    preimage.extend_from_slice(SCOPE_DIGEST_DOMAIN);
    preimage.extend_from_slice(schema_hash.as_bytes());
    append_text(&mut preimage, scope_id)?;
    append_u64(
        &mut preimage,
        u64::try_from(ordered.len())
            .map_err(|_| DigestError::new("scope cardinality exceeds u64"))?,
    );
    for entry in ordered {
        append_blob(&mut preimage, entry.row_identity.as_bytes())?;
        preimage.extend_from_slice(entry.row_digest.as_bytes());
    }
    Ok(preimage)
}

/// Computes the standard ordered streaming SHA-256 scope digest.
pub fn scope_digest(
    schema_hash: SchemaHash,
    scope_id: &str,
    entries: &[ScopeDigestEntry],
) -> Result<Sha256Digest, DigestError> {
    let preimage = scope_digest_preimage(schema_hash, scope_id, entries)?;
    let mut hasher = Sha256::new();
    hasher.update(preimage);
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 32];
    bytes.copy_from_slice(&digest);
    Ok(Sha256Digest::from_bytes(bytes))
}

fn encode_row_body(table: &CanonicalTable, row: &CanonicalRow) -> Result<Vec<u8>, DigestError> {
    if row.fields.len() != table.fields.len() {
        return Err(DigestError::new("row field count does not match table"));
    }
    let mut encoded_fields = Vec::with_capacity(row.fields.len());
    let mut encoded_row_primary_key = None;
    let mut seen = HashSet::with_capacity(row.fields.len());

    for row_field in &row.fields {
        if !seen.insert(row_field.field_id.as_str()) {
            return Err(DigestError::new(format!(
                "row has duplicate field_id {:?}",
                row_field.field_id
            )));
        }
        let field = table.field(&row_field.field_id).ok_or_else(|| {
            DigestError::new(format!("row has unknown field_id {:?}", row_field.field_id))
        })?;
        let encoded = encode_typed_value(field.spec(), &row_field.value_json).map_err(|error| {
            DigestError::new(format!(
                "encode row field {:?}: {error}",
                row_field.field_id
            ))
        })?;
        if row_field.field_id == table.primary_key_field_id {
            encoded_row_primary_key = Some(encoded.clone());
        }
        encoded_fields.push((&row_field.field_id, encoded));
    }

    for field in &table.fields {
        if !seen.contains(field.field_id.as_str()) {
            return Err(DigestError::new(format!(
                "row omits field_id {:?}",
                field.field_id
            )));
        }
    }

    let separate_primary_key =
        encode_typed_value(table.primary_key_field().spec(), &row.primary_key_json)
            .map_err(|error| DigestError::new(format!("encode separate primary key: {error}")))?;
    if encoded_row_primary_key.as_deref() != Some(separate_primary_key.as_slice()) {
        return Err(DigestError::new(
            "row primary key does not match separate primary key",
        ));
    }

    encoded_fields.sort_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
    let field_count = u32::try_from(encoded_fields.len())
        .map_err(|_| DigestError::new("row field count exceeds u32"))?;
    let mut body = Vec::new();
    append_u32(&mut body, field_count);
    for (field_id, value) in encoded_fields {
        append_text(&mut body, field_id)?;
        body.extend_from_slice(&value);
    }
    Ok(body)
}

fn ordered_scope_entries(
    entries: &[ScopeDigestEntry],
) -> Result<Vec<&ScopeDigestEntry>, DigestError> {
    let mut ordered = entries.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| {
        left.row_identity
            .as_bytes()
            .cmp(right.row_identity.as_bytes())
    });
    for pair in ordered.windows(2) {
        if pair[0].row_identity == pair[1].row_identity {
            return Err(DigestError::new("scope contains a duplicate row identity"));
        }
    }
    Ok(ordered)
}

fn typed_payload(spec: &FieldSpec, raw_json: &str) -> Result<Vec<u8>, DigestError> {
    match spec.portable_type {
        PortableType::String => {
            let value = decode_json_string(raw_json)?;
            let mut payload = Vec::new();
            append_blob(&mut payload, value.as_bytes())?;
            Ok(payload)
        }
        PortableType::Int => {
            if !is_canonical_integer(raw_json) {
                return Err(DigestError::new("int wire value is not canonical"));
            }
            let value = raw_json
                .parse::<i32>()
                .map_err(|_| DigestError::new("int wire value is outside int32"))?;
            Ok(value.to_be_bytes().to_vec())
        }
        PortableType::Int64 => {
            let value = decode_json_string(raw_json)?;
            if !is_canonical_integer(&value) {
                return Err(DigestError::new("int64 wire value is not canonical"));
            }
            let value = value
                .parse::<i64>()
                .map_err(|_| DigestError::new("int64 wire value is outside int64"))?;
            Ok(value.to_be_bytes().to_vec())
        }
        PortableType::Decimal => {
            let value = decode_json_string(raw_json)?;
            if !is_canonical_decimal(&value) {
                return Err(DigestError::new("decimal wire value is not canonical"));
            }
            validate_decimal_bounds(
                &value,
                spec.precision.expect("decimal FieldSpec has precision"),
                spec.scale.expect("decimal FieldSpec has scale"),
            )?;
            let mut payload = Vec::new();
            append_blob(&mut payload, value.as_bytes())?;
            Ok(payload)
        }
        PortableType::Float => {
            let value = parse_json_value(raw_json, false)?;
            let number = value
                .as_f64()
                .filter(|number| number.is_finite())
                .ok_or_else(|| DigestError::new("float wire value is not finite binary64"))?;
            let canonical = serde_json_canonicalizer::to_vec(&value).map_err(|error| {
                DigestError::new(format!("canonicalize float wire value: {error}"))
            })?;
            if canonical != raw_json.as_bytes() {
                return Err(DigestError::new("float wire value is not canonical"));
            }
            let bits = if number == 0.0 { 0.0 } else { number }.to_bits();
            Ok(bits.to_be_bytes().to_vec())
        }
        PortableType::Boolean => match raw_json {
            "false" => Ok(vec![0x00]),
            "true" => Ok(vec![0x01]),
            _ => Err(DigestError::new("boolean wire value is not true or false")),
        },
        PortableType::Datetime => {
            let value = decode_json_string(raw_json)?;
            validate_datetime(&value)?;
            let mut payload = Vec::new();
            append_blob(&mut payload, value.as_bytes())?;
            Ok(payload)
        }
        PortableType::Date => {
            let value = decode_json_string(raw_json)?;
            validate_date(&value)?;
            let mut payload = Vec::new();
            append_blob(&mut payload, value.as_bytes())?;
            Ok(payload)
        }
        PortableType::Time => {
            let value = decode_json_string(raw_json)?;
            validate_time(&value)?;
            let mut payload = Vec::new();
            append_blob(&mut payload, value.as_bytes())?;
            Ok(payload)
        }
        PortableType::Json => {
            let value = decode_json_string(raw_json)?;
            let canonical = canonicalize_json(&value)?;
            if canonical != value.as_bytes() {
                return Err(DigestError::new("nested JSON wire value is not canonical"));
            }
            let mut payload = Vec::new();
            append_blob(&mut payload, &canonical)?;
            Ok(payload)
        }
        PortableType::Bytes => {
            let value = decode_json_string(raw_json)?;
            let decoded = decode_base64url(&value)?;
            let mut payload = Vec::new();
            append_blob(&mut payload, &decoded)?;
            Ok(payload)
        }
    }
}

fn decode_json_string(raw_json: &str) -> Result<String, DigestError> {
    let value = parse_json_value(raw_json, false)?;
    value
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| DigestError::new("wire value is not a JSON string"))
}

fn canonicalize_json(value: &str) -> Result<Vec<u8>, DigestError> {
    let parsed = parse_json_value(value, true)?;
    serde_json_canonicalizer::to_vec(&parsed)
        .map_err(|error| DigestError::new(format!("canonicalize RFC 8785 JSON: {error}")))
}

fn parse_json_value(raw_json: &str, i_json: bool) -> Result<Value, DigestError> {
    let raw_json = raw_json.trim();
    if raw_json.is_empty() {
        return Err(DigestError::new("JSON value is empty"));
    }
    validate_json_document(raw_json, i_json)?;
    serde_json::from_str(raw_json)
        .map_err(|error| DigestError::new(format!("decode JSON value: {error}")))
}

fn validate_json_document(raw_json: &str, i_json: bool) -> Result<(), DigestError> {
    let mut deserializer = serde_json::Deserializer::from_str(raw_json);
    let value = StrictJson::deserialize(&mut deserializer)
        .map_err(|error| DigestError::new(format!("decode JSON value: {error}")))?;
    deserializer
        .end()
        .map_err(|error| DigestError::new(format!("decode trailing JSON: {error}")))?;
    if i_json {
        let mut values_and_names = 0;
        validate_i_json(&value, 0, &mut values_and_names)?;
    }
    Ok(())
}

#[derive(Debug)]
enum StrictJson {
    Null,
    Boolean,
    Number(f64),
    String(String),
    Array(Vec<StrictJson>),
    Object(Vec<(String, StrictJson)>),
}

impl<'de> Deserialize<'de> for StrictJson {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StrictJsonVisitor;

        impl<'de> Visitor<'de> for StrictJsonVisitor {
            type Value = StrictJson;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("one JSON value")
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(StrictJson::Null)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(StrictJson::Null)
            }

            fn visit_bool<E>(self, _: bool) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(StrictJson::Boolean)
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(StrictJson::Number(value as f64))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(StrictJson::Number(value as f64))
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(StrictJson::Number(value))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(StrictJson::String(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(StrictJson::String(value.to_owned()))
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some(value) = sequence.next_element::<StrictJson>()? {
                    values.push(value);
                }
                Ok(StrictJson::Array(values))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut entries = Vec::new();
                let mut seen = HashSet::new();
                while let Some((key, value)) = map.next_entry::<String, StrictJson>()? {
                    if !seen.insert(key.clone()) {
                        return Err(serde::de::Error::custom("duplicate JSON object member"));
                    }
                    entries.push((key, value));
                }
                Ok(StrictJson::Object(entries))
            }
        }

        deserializer.deserialize_any(StrictJsonVisitor)
    }
}

fn validate_i_json(
    value: &StrictJson,
    depth: usize,
    values_and_names: &mut usize,
) -> Result<(), DigestError> {
    *values_and_names += 1;
    if *values_and_names > MAX_JSON_VALUES_AND_NAMES {
        return Err(DigestError::new(
            "JSON exceeds the value and member name limit",
        ));
    }

    match value {
        StrictJson::Null | StrictJson::Boolean => Ok(()),
        StrictJson::Number(number) => {
            if !number.is_finite() {
                return Err(DigestError::new("JSON number is outside finite binary64"));
            }
            if number.fract() == 0.0 && number.abs() > MAX_SAFE_JSON_INTEGER {
                return Err(DigestError::new("JSON integer is outside the safe range"));
            }
            Ok(())
        }
        StrictJson::String(value) => validate_i_json_string(value),
        StrictJson::Array(values) => {
            if depth >= MAX_JSON_DEPTH {
                return Err(DigestError::new("JSON exceeds the nesting limit"));
            }
            for value in values {
                validate_i_json(value, depth + 1, values_and_names)?;
            }
            Ok(())
        }
        StrictJson::Object(entries) => {
            if depth >= MAX_JSON_DEPTH {
                return Err(DigestError::new("JSON exceeds the nesting limit"));
            }
            for (name, value) in entries {
                *values_and_names += 1;
                validate_i_json_string(name)?;
                validate_i_json(value, depth + 1, values_and_names)?;
            }
            Ok(())
        }
    }
}

fn validate_i_json_string(value: &str) -> Result<(), DigestError> {
    if value.chars().any(is_unicode_noncharacter) {
        return Err(DigestError::new(
            "I-JSON string contains a Unicode noncharacter",
        ));
    }
    Ok(())
}

fn is_unicode_noncharacter(character: char) -> bool {
    matches!(character as u32, 0xfdd0..=0xfdef)
        || matches!(character as u32 & 0xffff, 0xfffe | 0xffff)
}

fn is_canonical_integer(value: &str) -> bool {
    if value == "0" {
        return true;
    }
    let digits = value.strip_prefix('-').unwrap_or(value);
    !digits.is_empty()
        && digits.as_bytes()[0] != b'0'
        && digits.bytes().all(|byte| byte.is_ascii_digit())
        && !(value.starts_with('-') && digits == "0")
}

fn is_canonical_decimal(value: &str) -> bool {
    if value == "0" {
        return true;
    }
    let unsigned = value.strip_prefix('-').unwrap_or(value);
    let (integer, fraction) = match unsigned.split_once('.') {
        Some((integer, fraction)) => (integer, Some(fraction)),
        None => (unsigned, None),
    };
    if integer.is_empty() || !integer.bytes().all(|byte| byte.is_ascii_digit()) {
        return false;
    }
    match fraction {
        None => integer.as_bytes()[0] != b'0',
        Some(fraction) => {
            !fraction.is_empty()
                && fraction.bytes().all(|byte| byte.is_ascii_digit())
                && fraction.as_bytes().last() != Some(&b'0')
                && (integer == "0" || integer.as_bytes()[0] != b'0')
        }
    }
}

fn validate_decimal_bounds(value: &str, precision: u32, scale: u32) -> Result<(), DigestError> {
    let unsigned = value.strip_prefix('-').unwrap_or(value);
    let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let integer_digits = integer.trim_start_matches('0').len() as u32;
    let fraction_digits = fraction.len() as u32;
    if integer_digits > precision - scale
        || fraction_digits > scale
        || integer_digits + fraction_digits > precision
    {
        return Err(DigestError::new(
            "decimal wire value exceeds declared precision or scale",
        ));
    }
    Ok(())
}

fn validate_datetime(value: &str) -> Result<(), DigestError> {
    if value.len() != 27 || value.as_bytes()[10] != b'T' || value.as_bytes()[26] != b'Z' {
        return Err(DigestError::new(
            "datetime wire value has an invalid format",
        ));
    }
    validate_date(&value[..10])?;
    validate_time(&value[11..26])
}

fn validate_date(value: &str) -> Result<(), DigestError> {
    if value.len() != 10
        || value.as_bytes()[4] != b'-'
        || value.as_bytes()[7] != b'-'
        || !value
            .bytes()
            .enumerate()
            .filter(|(index, _)| !matches!(index, 4 | 7))
            .all(|(_, byte)| byte.is_ascii_digit())
    {
        return Err(DigestError::new("date wire value has an invalid format"));
    }
    let year = value[..4]
        .parse::<i32>()
        .map_err(|_| DigestError::new("date wire value is invalid"))?;
    let month = value[5..7]
        .parse::<u32>()
        .map_err(|_| DigestError::new("date wire value is invalid"))?;
    let day = value[8..10]
        .parse::<u32>()
        .map_err(|_| DigestError::new("date wire value is invalid"))?;
    if NaiveDate::from_ymd_opt(year, month, day).is_none() {
        return Err(DigestError::new("date wire value is invalid"));
    }
    Ok(())
}

fn validate_time(value: &str) -> Result<(), DigestError> {
    if value.len() != 15
        || value.as_bytes()[2] != b':'
        || value.as_bytes()[5] != b':'
        || value.as_bytes()[8] != b'.'
        || !value
            .bytes()
            .enumerate()
            .filter(|(index, _)| !matches!(index, 2 | 5 | 8))
            .all(|(_, byte)| byte.is_ascii_digit())
    {
        return Err(DigestError::new("time wire value has an invalid format"));
    }
    let hours = value[..2]
        .parse::<u8>()
        .map_err(|_| DigestError::new("time wire value is invalid"))?;
    let minutes = value[3..5]
        .parse::<u8>()
        .map_err(|_| DigestError::new("time wire value is invalid"))?;
    let seconds = value[6..8]
        .parse::<u8>()
        .map_err(|_| DigestError::new("time wire value is invalid"))?;
    if hours > 23 || minutes > 59 || seconds > 59 {
        return Err(DigestError::new("time wire value is invalid"));
    }
    Ok(())
}

fn decode_base64url(value: &str) -> Result<Vec<u8>, DigestError> {
    let input = value.as_bytes();
    if input.len() % 4 == 1 {
        return Err(DigestError::new(
            "bytes wire value is not canonical base64url",
        ));
    }
    let mut output = Vec::with_capacity(input.len() / 4 * 3 + 2);
    let complete = input.len() / 4 * 4;
    for chunk in input[..complete].as_chunks::<4>().0 {
        let a = base64url_value(chunk[0])?;
        let b = base64url_value(chunk[1])?;
        let c = base64url_value(chunk[2])?;
        let d = base64url_value(chunk[3])?;
        output.push((a << 2) | (b >> 4));
        output.push((b << 4) | (c >> 2));
        output.push((c << 6) | d);
    }
    match input.len() - complete {
        0 => {}
        2 => {
            let a = base64url_value(input[complete])?;
            let b = base64url_value(input[complete + 1])?;
            if b & 0x0f != 0 {
                return Err(DigestError::new(
                    "bytes wire value is not canonical base64url",
                ));
            }
            output.push((a << 2) | (b >> 4));
        }
        3 => {
            let a = base64url_value(input[complete])?;
            let b = base64url_value(input[complete + 1])?;
            let c = base64url_value(input[complete + 2])?;
            if c & 0x03 != 0 {
                return Err(DigestError::new(
                    "bytes wire value is not canonical base64url",
                ));
            }
            output.push((a << 2) | (b >> 4));
            output.push((b << 4) | (c >> 2));
        }
        _ => unreachable!("base64url remainder is in 0..4"),
    }
    Ok(output)
}

fn base64url_value(value: u8) -> Result<u8, DigestError> {
    match value {
        b'A'..=b'Z' => Ok(value - b'A'),
        b'a'..=b'z' => Ok(value - b'a' + 26),
        b'0'..=b'9' => Ok(value - b'0' + 52),
        b'-' => Ok(62),
        b'_' => Ok(63),
        _ => Err(DigestError::new(
            "bytes wire value is not canonical base64url",
        )),
    }
}

fn validate_row_identity(identity: &[u8]) -> Result<(), DigestError> {
    let mut position = 0;
    if !consume_exact(identity, &mut position, ROW_ID_DOMAIN) {
        return Err(DigestError::new("row identity has an invalid domain"));
    }
    consume_nonempty_text(identity, &mut position, "table_id")?;
    consume_nonempty_text(identity, &mut position, "primary_key_field_id")?;
    let tag = *identity
        .get(position)
        .ok_or_else(|| DigestError::new("row identity primary key is truncated"))?;
    position += 1;
    let presence = *identity
        .get(position)
        .ok_or_else(|| DigestError::new("row identity primary key is truncated"))?;
    position += 1;
    if presence != 0x01 {
        return Err(DigestError::new(
            "row identity primary key has invalid presence",
        ));
    }
    match tag {
        0x01 => {
            let text = consume_blob(identity, &mut position, "primary key")?;
            std::str::from_utf8(text)
                .map_err(|_| DigestError::new("row identity primary key is not valid UTF-8"))?;
        }
        0x02 => consume_fixed(identity, &mut position, 4, "int primary key")?,
        0x03 => consume_fixed(identity, &mut position, 8, "int64 primary key")?,
        _ => {
            return Err(DigestError::new(
                "row identity primary key has an invalid type tag",
            ))
        }
    }
    if position != identity.len() {
        return Err(DigestError::new("row identity has trailing bytes"));
    }
    Ok(())
}

fn consume_exact(input: &[u8], position: &mut usize, expected: &[u8]) -> bool {
    match input.get(*position..position.saturating_add(expected.len())) {
        Some(actual) if actual == expected => {
            *position += expected.len();
            true
        }
        _ => false,
    }
}

fn consume_nonempty_text(
    input: &[u8],
    position: &mut usize,
    name: &str,
) -> Result<(), DigestError> {
    let text = consume_blob(input, position, name)?;
    if text.is_empty() {
        return Err(DigestError::new(format!("row identity {name} is empty")));
    }
    std::str::from_utf8(text)
        .map_err(|_| DigestError::new(format!("row identity {name} is not valid UTF-8")))?;
    Ok(())
}

fn consume_blob<'a>(
    input: &'a [u8],
    position: &mut usize,
    name: &str,
) -> Result<&'a [u8], DigestError> {
    let length_bytes = input
        .get(*position..position.saturating_add(8))
        .ok_or_else(|| DigestError::new(format!("row identity {name} length is truncated")))?;
    let length = u64::from_be_bytes(length_bytes.try_into().expect("slice length is eight"));
    *position += 8;
    let available = input.len() - *position;
    if length > u64::try_from(available).expect("usize fits u64 on supported targets") {
        return Err(DigestError::new(format!(
            "row identity {name} length exceeds available bytes"
        )));
    }
    let length = usize::try_from(length).expect("validated length fits usize");
    let end = *position + length;
    let payload = &input[*position..end];
    *position = end;
    Ok(payload)
}

fn consume_fixed(
    input: &[u8],
    position: &mut usize,
    length: usize,
    name: &str,
) -> Result<(), DigestError> {
    if input.len().saturating_sub(*position) < length {
        return Err(DigestError::new(format!(
            "row identity {name} payload is truncated"
        )));
    }
    *position += length;
    Ok(())
}

fn require_nonempty_text(value: &str, name: &str) -> Result<(), DigestError> {
    if value.is_empty() {
        return Err(DigestError::new(format!("{name} is empty")));
    }
    Ok(())
}

fn append_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn append_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn append_blob(output: &mut Vec<u8>, value: &[u8]) -> Result<(), DigestError> {
    let length = u64::try_from(value.len()).map_err(|_| DigestError::new("blob exceeds u64"))?;
    append_u64(output, length);
    output.extend_from_slice(value);
    Ok(())
}

fn append_text(output: &mut Vec<u8>, value: &str) -> Result<(), DigestError> {
    append_blob(output, value.as_bytes())
}

fn sha256_digest(input: &[u8]) -> Sha256Digest {
    let digest = Sha256::digest(input);
    let mut bytes = [0_u8; 32];
    bytes.copy_from_slice(&digest);
    Sha256Digest::from_bytes(bytes)
}

fn decode_lower_sha256(value: &str) -> Result<[u8; 32], DigestError> {
    if value.len() != SHA256_HEX_LENGTH {
        return Err(DigestError::new("digest must contain 64 characters"));
    }
    let bytes = decode_lower_hex(value)?;
    let mut digest = [0_u8; 32];
    digest.copy_from_slice(&bytes);
    Ok(digest)
}

fn decode_lower_hex(value: &str) -> Result<Vec<u8>, DigestError> {
    if !value.len().is_multiple_of(2) {
        return Err(DigestError::new("hexadecimal value has odd length"));
    }
    let mut output = Vec::with_capacity(value.len() / 2);
    for pair in value.as_bytes().as_chunks::<2>().0 {
        let high = lower_hex_value(pair[0])?;
        let low = lower_hex_value(pair[1])?;
        output.push(high << 4 | low);
    }
    Ok(output)
}

fn lower_hex_value(value: u8) -> Result<u8, DigestError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(DigestError::new("value is not lowercase hexadecimal")),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn authored_vectors() -> Vec<Value> {
        let path = std::env::var_os("SYNCHRO_REPO_ROOT")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
            .join("conformance/vectors/canonical-v1.json");
        let authored =
            std::fs::read_to_string(path).expect("canonical vector file must be readable");
        let vectors: Value =
            serde_json::from_str(&authored).expect("canonical vector file must be valid JSON");
        vectors["vectors"]
            .as_array()
            .expect("canonical vectors must be an array")
            .clone()
    }

    fn field_spec(value: &Value) -> FieldSpec {
        let portable_type =
            PortableType::parse(value["type"].as_str().expect("field type must be a string"))
                .expect("authored field type must be portable");
        FieldSpec::new(
            portable_type,
            value["nullable"]
                .as_bool()
                .expect("field nullability must be boolean"),
            value["precision"].as_u64().map(|value| value as u32),
            value["scale"].as_u64().map(|value| value as u32),
        )
        .expect("authored field specification must be valid")
    }

    fn table_from_manifest(manifest_json: &str, table_id: &str) -> CanonicalTable {
        let manifest: Value = serde_json::from_str(manifest_json).expect("manifest must parse");
        let table = manifest["tables"]
            .as_array()
            .expect("manifest tables must be an array")
            .iter()
            .find(|table| table["table_id"] == table_id)
            .expect("manifest table must exist");
        let fields = table["fields"]
            .as_array()
            .expect("manifest fields must be an array")
            .iter()
            .map(|field| {
                CanonicalField::new(
                    field["field_id"]
                        .as_str()
                        .expect("field ID must be a string"),
                    field_spec(field),
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .expect("manifest fields must be valid");
        CanonicalTable::new(
            table["table_id"]
                .as_str()
                .expect("table ID must be a string"),
            table["primary_key_field_id"]
                .as_str()
                .expect("primary key field ID must be a string"),
            fields,
        )
        .expect("manifest table must be valid")
    }

    fn assert_expected_bytes(vector: &Value, bytes: &[u8]) {
        let expected = vector["expected"]["canonical_bytes_hex"]
            .as_str()
            .expect("valid vector requires canonical bytes");
        assert_eq!(encode_lower_hex(bytes), expected, "{}", vector["vector_id"]);
        let expected_hash = vector["expected"]["expected_bytes_sha256"]
            .as_str()
            .expect("valid vector requires canonical byte digest");
        assert_eq!(sha256_digest(bytes).to_lower_hex(), expected_hash);
    }

    #[test]
    fn typed_values_match_authored_canonical_vectors() {
        for vector in authored_vectors()
            .into_iter()
            .filter(|vector| vector["kind"] == "typed_value")
        {
            let input = &vector["input"];
            let result = encode_typed_value(
                &field_spec(&input["field_spec"]),
                input["raw_json"]
                    .as_str()
                    .expect("raw JSON must be a string"),
            );
            if vector["valid"].as_bool().expect("valid must be boolean") {
                assert_expected_bytes(&vector, &result.expect("valid vector must encode"));
            } else {
                assert!(result.is_err(), "{}", vector["vector_id"]);
            }
        }
    }

    #[test]
    fn row_identity_and_digest_match_authored_canonical_vectors() {
        for vector in authored_vectors()
            .into_iter()
            .filter(|vector| vector["kind"] == "row_identity" || vector["kind"] == "row_digest")
        {
            let input = &vector["input"];
            let table = table_from_manifest(
                input["manifest_json"]
                    .as_str()
                    .expect("manifest JSON must be a string"),
                input["table_id"]
                    .as_str()
                    .expect("table ID must be a string"),
            );
            let result = if vector["kind"] == "row_identity" {
                row_identity(
                    &table,
                    input["pk_json"].as_str().expect("PK JSON must be a string"),
                )
                .map(|identity| (identity.into_bytes(), None))
            } else {
                let row = CanonicalRow::from_json(
                    input["pk_json"].as_str().expect("PK JSON must be a string"),
                    input["row_json"]
                        .as_str()
                        .expect("row JSON must be a string"),
                );
                row.and_then(|row| {
                    let schema_hash = SchemaHash::from_lower_hex(
                        &serde_json::from_str::<Value>(
                            input["manifest_json"]
                                .as_str()
                                .expect("manifest JSON must be a string"),
                        )
                        .expect("manifest must parse")["schema_hash"]
                            .as_str()
                            .expect("schema hash must be a string"),
                    )?;
                    let preimage = row_digest_preimage(
                        schema_hash,
                        &table,
                        &row,
                        input["server_version"]
                            .as_str()
                            .expect("server version must be a string"),
                    )?;
                    let digest = row_digest(
                        schema_hash,
                        &table,
                        &row,
                        input["server_version"]
                            .as_str()
                            .expect("server version must be a string"),
                    )?;
                    Ok((preimage, Some(digest)))
                })
            };
            if vector["valid"].as_bool().expect("valid must be boolean") {
                let (bytes, digest) = result.expect("valid vector must encode");
                assert_expected_bytes(&vector, &bytes);
                if let Some(digest) = digest {
                    assert_eq!(
                        digest.to_lower_hex(),
                        vector["expected"]["expected_sha256"]
                            .as_str()
                            .expect("digest vector requires SHA-256"),
                    );
                }
            } else {
                assert!(result.is_err(), "{}", vector["vector_id"]);
            }
        }
    }

    #[test]
    fn scope_digests_match_authored_canonical_vectors() {
        for vector in authored_vectors()
            .into_iter()
            .filter(|vector| vector["kind"] == "scope_digest")
        {
            let input = &vector["input"];
            let result: Result<(Vec<u8>, Sha256Digest), DigestError> = (|| {
                let schema_hash = SchemaHash::from_lower_hex(
                    input["schema_hash"]
                        .as_str()
                        .expect("schema hash must be a string"),
                )?;
                let entries = input["entries"]
                    .as_array()
                    .expect("entries must be an array")
                    .iter()
                    .map(|entry| {
                        Ok(ScopeDigestEntry::new(
                            RowIdentity::from_lower_hex(
                                entry["row_identity_hex"]
                                    .as_str()
                                    .expect("row identity must be a string"),
                            )?,
                            Sha256Digest::from_lower_hex(
                                entry["row_digest_hex"]
                                    .as_str()
                                    .expect("row digest must be a string"),
                            )?,
                        ))
                    })
                    .collect::<Result<Vec<_>, DigestError>>()?;
                let scope_id = input["scope_id"]
                    .as_str()
                    .expect("scope ID must be a string");
                let preimage = scope_digest_preimage(schema_hash, scope_id, &entries)?;
                let digest = scope_digest(schema_hash, scope_id, &entries)?;
                Ok((preimage, digest))
            })();
            if vector["valid"].as_bool().expect("valid must be boolean") {
                let (preimage, digest) = result.expect("valid vector must encode");
                assert_expected_bytes(&vector, &preimage);
                assert_eq!(
                    digest.to_lower_hex(),
                    vector["expected"]["expected_sha256"]
                        .as_str()
                        .expect("scope digest vector requires SHA-256"),
                );
            } else {
                assert!(result.is_err(), "{}", vector["vector_id"]);
            }
        }
    }

    #[test]
    fn checksum_object_requires_the_exact_wire_shape() {
        let digest = Sha256Digest::from_lower_hex(
            "bed4f60fde120d654081554f43b1a2a561a654f2ef11a8d05f2efea522a5a1e1",
        )
        .expect("authored digest must decode");
        let object = ChecksumObject::new(digest);
        assert_eq!(
            serde_json::to_string(&object).expect("checksum object must serialize"),
            "{\"algorithm\":\"sha256\",\"version\":1,\"encoding\":\"hex\",\"digest\":\"bed4f60fde120d654081554f43b1a2a561a654f2ef11a8d05f2efea522a5a1e1\"}"
        );
        assert_eq!(
            ChecksumObject::from_json(
                "{\"algorithm\":\"sha256\",\"version\":1,\"encoding\":\"hex\",\"digest\":\"bed4f60fde120d654081554f43b1a2a561a654f2ef11a8d05f2efea522a5a1e1\"}"
            )
            .expect("exact checksum object must parse")
            .digest(),
            digest
        );
        for invalid in [
            "{\"algorithm\":\"sha-256\",\"version\":1,\"encoding\":\"hex\",\"digest\":\"bed4f60fde120d654081554f43b1a2a561a654f2ef11a8d05f2efea522a5a1e1\"}",
            "{\"algorithm\":\"sha256\",\"version\":2,\"encoding\":\"hex\",\"digest\":\"bed4f60fde120d654081554f43b1a2a561a654f2ef11a8d05f2efea522a5a1e1\"}",
            "{\"algorithm\":\"sha256\",\"version\":1,\"encoding\":\"base64\",\"digest\":\"bed4f60fde120d654081554f43b1a2a561a654f2ef11a8d05f2efea522a5a1e1\"}",
            "{\"algorithm\":\"sha256\",\"version\":1,\"encoding\":\"hex\",\"digest\":\"BED4F60FDE120D654081554F43B1A2A561A654F2EF11A8D05F2EFEA522A5A1E1\"}",
            "{\"algorithm\":\"sha256\",\"version\":1,\"encoding\":\"hex\",\"digest\":\"00\"}",
            "{\"algorithm\":\"sha256\",\"version\":1,\"encoding\":\"hex\",\"digest\":\"bed4f60fde120d654081554f43b1a2a561a654f2ef11a8d05f2efea522a5a1e1\",\"extra\":true}",
        ] {
            assert!(ChecksumObject::from_json(invalid).is_err());
        }
    }

    #[test]
    fn changing_bound_row_and_scope_inputs_changes_the_digest() {
        let vector = authored_vectors()
            .into_iter()
            .find(|vector| vector["vector_id"] == "VEC-ROW-DIGEST-FULL-ROW-001")
            .expect("full row vector must exist");
        let input = &vector["input"];
        let manifest: Value = serde_json::from_str(
            input["manifest_json"]
                .as_str()
                .expect("manifest JSON must be a string"),
        )
        .expect("manifest must parse");
        let schema_hash = SchemaHash::from_lower_hex(
            manifest["schema_hash"]
                .as_str()
                .expect("schema hash must be a string"),
        )
        .expect("schema hash must decode");
        let table = table_from_manifest(
            input["manifest_json"]
                .as_str()
                .expect("manifest JSON must be a string"),
            "tbl_records",
        );
        let row = CanonicalRow::from_json(
            input["pk_json"].as_str().expect("PK JSON must be a string"),
            input["row_json"]
                .as_str()
                .expect("row JSON must be a string"),
        )
        .expect("full row must parse");
        let original = row_digest(schema_hash, &table, &row, "server-version-001")
            .expect("full row must digest");
        assert_ne!(
            original,
            row_digest(schema_hash, &table, &row, "server-version-002")
                .expect("changed version must digest"),
        );
        assert_ne!(
            original,
            row_digest(
                SchemaHash::from_bytes([0_u8; 32]),
                &table,
                &row,
                "server-version-001"
            )
            .expect("changed schema must digest"),
        );

        let identity = row_identity(&table, row.primary_key_json()).expect("identity must encode");
        let first_scope = scope_digest(
            schema_hash,
            "scope-a",
            &[ScopeDigestEntry::new(identity.clone(), original)],
        )
        .expect("scope must digest");
        assert_ne!(
            first_scope,
            scope_digest(
                schema_hash,
                "scope-b",
                &[ScopeDigestEntry::new(identity.clone(), original)],
            )
            .expect("changed scope ID must digest"),
        );
        assert_ne!(
            first_scope,
            scope_digest(schema_hash, "scope-a", &[]).expect("changed cardinality must digest"),
        );
        assert!(scope_digest(
            schema_hash,
            "scope-a",
            &[
                ScopeDigestEntry::new(identity.clone(), original),
                ScopeDigestEntry::new(identity, original),
            ],
        )
        .is_err(),);
    }

    #[test]
    fn portable_type_names_and_digest_hex_round_trip_exactly() {
        let types = [
            (PortableType::String, "string"),
            (PortableType::Int, "int"),
            (PortableType::Int64, "int64"),
            (PortableType::Decimal, "decimal"),
            (PortableType::Float, "float"),
            (PortableType::Boolean, "boolean"),
            (PortableType::Datetime, "datetime"),
            (PortableType::Date, "date"),
            (PortableType::Time, "time"),
            (PortableType::Json, "json"),
            (PortableType::Bytes, "bytes"),
        ];
        for (portable_type, name) in types {
            assert_eq!(portable_type.as_str(), name);
            assert_eq!(
                PortableType::parse(name).expect("type name must parse"),
                portable_type
            );
        }

        let hex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
        let schema_hash = SchemaHash::from_lower_hex(hex).expect("schema hash must decode");
        assert_eq!(schema_hash.to_lower_hex(), hex);
        assert_eq!(
            schema_hash.as_bytes(),
            &std::array::from_fn(|index| index as u8)
        );
    }

    #[test]
    fn i_json_validation_enforces_counts_depth_and_noncharacters() {
        let mut count = 2;
        validate_i_json(&StrictJson::Null, 0, &mut count).expect("one value must be valid");
        assert_eq!(count, 3);

        let mut count = MAX_JSON_VALUES_AND_NAMES - 1;
        validate_i_json(&StrictJson::Null, 0, &mut count).expect("limit must be inclusive");
        assert_eq!(count, MAX_JSON_VALUES_AND_NAMES);
        assert!(validate_i_json(&StrictJson::Null, 0, &mut count).is_err());

        let array = StrictJson::Array(vec![StrictJson::Null]);
        let mut count = 0;
        validate_i_json(&array, MAX_JSON_DEPTH - 1, &mut count)
            .expect("last supported nesting level must be valid");
        let mut count = 0;
        assert!(validate_i_json(&array, MAX_JSON_DEPTH, &mut count).is_err());

        let object = StrictJson::Object(vec![(
            "name".into(),
            StrictJson::Array(vec![StrictJson::Null]),
        )]);
        let mut count = 0;
        validate_i_json(&object, 0, &mut count).expect("object must be valid I-JSON");
        assert_eq!(count, 4);

        let mut count = 0;
        validate_i_json(&StrictJson::Number(MAX_SAFE_JSON_INTEGER), 0, &mut count)
            .expect("largest safe integer must be valid");
        let mut count = 0;
        assert!(validate_i_json(
            &StrictJson::Number(MAX_SAFE_JSON_INTEGER + 1.0),
            0,
            &mut count,
        )
        .is_err());

        let mut nested_array = StrictJson::Null;
        for _ in 0..MAX_JSON_DEPTH {
            nested_array = StrictJson::Array(vec![nested_array]);
        }
        let mut count = 0;
        validate_i_json(&nested_array, 0, &mut count).expect("maximum array depth must be valid");
        nested_array = StrictJson::Array(vec![nested_array]);
        let mut count = 0;
        assert!(validate_i_json(&nested_array, 0, &mut count).is_err());

        let mut nested_object = StrictJson::Null;
        for _ in 0..MAX_JSON_DEPTH {
            nested_object = StrictJson::Object(vec![("name".into(), nested_object)]);
        }
        let mut count = 0;
        validate_i_json(&nested_object, 0, &mut count).expect("maximum object depth must be valid");
        nested_object = StrictJson::Object(vec![("name".into(), nested_object)]);
        let mut count = 0;
        assert!(validate_i_json(&nested_object, 0, &mut count).is_err());

        let object = StrictJson::Object(vec![("name".into(), StrictJson::Null)]);
        let mut count = MAX_JSON_VALUES_AND_NAMES - 3;
        validate_i_json(&object, 0, &mut count).expect("object count limit must be inclusive");
        assert_eq!(count, MAX_JSON_VALUES_AND_NAMES);
        let mut count = MAX_JSON_VALUES_AND_NAMES - 2;
        assert!(validate_i_json(&object, 0, &mut count).is_err());

        for character in ['\u{fdd0}', '\u{fffe}', '\u{1fffe}', '\u{10ffff}'] {
            assert!(validate_i_json_string(&character.to_string()).is_err());
        }
        validate_i_json_string("\u{fdcf}\u{1fffd}").expect("nearby characters are valid");
    }

    #[test]
    fn decimal_validation_covers_canonical_and_declared_boundaries() {
        for valid in ["0", "1", "-1", "0.1", "1.01", "10.01", "-999.99"] {
            assert!(is_canonical_decimal(valid), "{valid}");
        }
        for invalid in ["", "-0", "00", "01", ".1", "1.", "1.0", "1.10", "01.1"] {
            assert!(!is_canonical_decimal(invalid), "{invalid}");
        }

        for valid in ["0", "0.01", "1", "999", "999.99", "-999.99"] {
            validate_decimal_bounds(valid, 5, 2).unwrap_or_else(|error| panic!("{valid}: {error}"));
        }
        for invalid in ["1000", "1000.0", "1.234", "9999.99"] {
            assert!(validate_decimal_bounds(invalid, 5, 2).is_err(), "{invalid}");
        }
    }

    #[test]
    fn temporal_validation_requires_every_separator_and_range() {
        let datetime = "2024-02-29T23:59:58.123456Z";
        validate_datetime(datetime).expect("leap-day datetime must be valid");
        for index in [4, 7, 10, 13, 16, 19, 26] {
            let mut invalid = datetime.as_bytes().to_vec();
            invalid[index] = b'X';
            assert!(validate_datetime(std::str::from_utf8(&invalid).unwrap()).is_err());
        }

        let date = "2024-02-29";
        validate_date(date).expect("leap day must be valid");
        for index in [4, 7] {
            let mut invalid = date.as_bytes().to_vec();
            invalid[index] = b'X';
            assert!(validate_date(std::str::from_utf8(&invalid).unwrap()).is_err());
        }
        for invalid in ["2023-02-29", "2024-00-01", "2024-13-01", "2024-01-00"] {
            assert!(validate_date(invalid).is_err(), "{invalid}");
        }

        let time = "23:59:58.123456";
        validate_time(time).expect("last full second must be valid");
        for index in [2, 5, 8] {
            let mut invalid = time.as_bytes().to_vec();
            invalid[index] = b'X';
            assert!(validate_time(std::str::from_utf8(&invalid).unwrap()).is_err());
        }
        for invalid in ["24:00:00.000000", "00:60:00.000000", "00:00:60.000000"] {
            assert!(validate_time(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn base64url_and_hex_decoders_cover_alphabet_and_quantum_boundaries() {
        let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        for (expected, encoded) in alphabet.iter().copied().enumerate() {
            assert_eq!(base64url_value(encoded).unwrap(), expected as u8);
        }
        for invalid in [b'=', b'+', b'/', b' '] {
            assert!(base64url_value(invalid).is_err());
        }

        for (encoded, expected) in [
            ("", &b""[..]),
            ("AA", &b"\0"[..]),
            ("_w", &[0xff][..]),
            ("AAA", &[0, 0][..]),
            ("__8", &[0xff, 0xff][..]),
            ("AAAA", &[0, 0, 0][..]),
            ("____", &[0xff, 0xff, 0xff][..]),
            ("AQIDBAU", &[1, 2, 3, 4, 5][..]),
            ("Zm9vYmFy", &b"foobar"[..]),
        ] {
            assert_eq!(decode_base64url(encoded).unwrap(), expected, "{encoded}");
        }
        for invalid in ["A", "_x", "__9", "AA=", "AA+"] {
            assert!(decode_base64url(invalid).is_err(), "{invalid}");
        }

        assert_eq!(decode_lower_hex("00abff").unwrap(), [0x00, 0xab, 0xff]);
        assert!(decode_lower_hex("0").is_err());
        assert!(decode_lower_hex("AB").is_err());
    }

    #[test]
    fn row_identity_parser_rejects_wrong_domains_and_truncation() {
        let mut position = 0;
        assert!(consume_exact(b"prefix-rest", &mut position, b"prefix-"));
        assert_eq!(position, 7);
        let mut position = 0;
        assert!(!consume_exact(b"wrong", &mut position, b"right"));
        assert_eq!(position, 0);

        let mut position = 0;
        consume_fixed(&[0, 1], &mut position, 2, "value").expect("exact payload must fit");
        assert_eq!(position, 2);
        let mut position = 0;
        assert!(consume_fixed(&[0], &mut position, 2, "value").is_err());

        let vector = authored_vectors()
            .into_iter()
            .find(|vector| vector["kind"] == "row_identity" && vector["valid"] == true)
            .expect("valid row identity vector must exist");
        let input = &vector["input"];
        let table = table_from_manifest(
            input["manifest_json"].as_str().unwrap(),
            input["table_id"].as_str().unwrap(),
        );
        let identity = row_identity(&table, input["pk_json"].as_str().unwrap()).unwrap();
        assert_eq!(
            identity.to_lower_hex(),
            encode_lower_hex(identity.as_bytes())
        );
        let mut wrong_domain = identity.as_bytes().to_vec();
        wrong_domain[0] ^= 1;
        assert!(RowIdentity::from_bytes(wrong_domain).is_err());
        assert!(
            RowIdentity::from_bytes(&identity.as_bytes()[..identity.as_bytes().len() - 1]).is_err()
        );
    }
}
