use crate::checksum::ChecksumObject;
use chrono::{DateTime, SecondsFormat, Utc};
use serde::de::{self, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};
use std::collections::{BTreeMap, HashSet};
use std::convert::TryFrom;
use std::fmt;

const PROTOCOL_VERSION: u32 = 3;
const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;
const MAX_PUSH_MUTATIONS: usize = 1_000;
const MAX_PUSH_COLUMNS: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Insert,
    Upsert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeOperationConversionError {
    pub operation: Operation,
}

impl fmt::Display for ChangeOperationConversionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the operation is not a local change operation")
    }
}

impl std::error::Error for ChangeOperationConversionError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContractViolation {
    InvalidProtocolVersion,
    InvalidSchemaReference,
    InvalidFreshSchemaReference,
    InvalidPositiveSafeInteger,
    InvalidNonnegativeSafeInteger,
    InvalidUuid,
    InvalidSemver,
    EmptyRequiredField,
    InvalidOpaqueValue,
    InvalidScopeAssignment,
    DuplicateScope,
    ConflictingScopeAssignment,
    InvalidSchemaManifest,
    InvalidSchemaActionFields,
    InvalidSchemaDefinition,
    InvalidAffectedScopes,
    InvalidPrimaryKey,
    InvalidColumns,
    InvalidRow,
    InvalidMutationClientVersion,
    InvalidPushOperation,
    MissingMutationBaseVersion,
    UnexpectedMutationBaseVersion,
    MissingMutationColumns,
    UnexpectedMutationColumns,
    DuplicateMutationId,
    InvalidPushOutcome,
    InvalidPushOutcomePartition,
    FinalPullChecksumsMissing,
    NonterminalPullChecksumsPresent,
    InvalidPullChange,
    PartialRebuildHasFinalCursor,
    PartialRebuildHasChecksum,
    PartialRebuildCursorMissing,
    FinalRebuildCursorMissing,
    FinalRebuildCursorPresent,
    FinalRebuildChecksumMissing,
    InvalidRebuildRecord,
    InvalidErrorBody,
}

impl fmt::Display for ContractViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidProtocolVersion => "protocol version is invalid",
            Self::InvalidSchemaReference => "schema reference is invalid",
            Self::InvalidFreshSchemaReference => "fresh schema reference is invalid",
            Self::InvalidPositiveSafeInteger => "positive safe integer is invalid",
            Self::InvalidNonnegativeSafeInteger => "nonnegative safe integer is invalid",
            Self::InvalidUuid => "UUID is invalid",
            Self::InvalidSemver => "Semantic Version is invalid",
            Self::EmptyRequiredField => "required field is empty",
            Self::InvalidOpaqueValue => "opaque value is invalid",
            Self::InvalidScopeAssignment => "scope assignment is invalid",
            Self::DuplicateScope => "scope is duplicated",
            Self::ConflictingScopeAssignment => "scope assignment conflicts",
            Self::InvalidSchemaManifest => "schema manifest is invalid",
            Self::InvalidSchemaActionFields => "schema action fields are invalid",
            Self::InvalidSchemaDefinition => "schema definition is invalid",
            Self::InvalidAffectedScopes => "affected scopes are invalid",
            Self::InvalidPrimaryKey => "primary key is invalid",
            Self::InvalidColumns => "columns are invalid",
            Self::InvalidRow => "row is invalid",
            Self::InvalidMutationClientVersion => "mutation client version is invalid",
            Self::InvalidPushOperation => "push operation is invalid",
            Self::MissingMutationBaseVersion => "mutation base version is missing",
            Self::UnexpectedMutationBaseVersion => "mutation base version is unexpected",
            Self::MissingMutationColumns => "mutation columns are missing",
            Self::UnexpectedMutationColumns => "mutation columns are unexpected",
            Self::DuplicateMutationId => "mutation ID is duplicated",
            Self::InvalidPushOutcome => "push outcome is invalid",
            Self::InvalidPushOutcomePartition => "push outcome partition is invalid",
            Self::FinalPullChecksumsMissing => "final pull checksums are missing",
            Self::NonterminalPullChecksumsPresent => "nonterminal pull checksums are present",
            Self::InvalidPullChange => "pull change is invalid",
            Self::PartialRebuildHasFinalCursor => "partial rebuild has final cursor",
            Self::PartialRebuildHasChecksum => "partial rebuild has checksum",
            Self::PartialRebuildCursorMissing => "partial rebuild cursor is missing",
            Self::FinalRebuildCursorMissing => "final rebuild cursor is missing",
            Self::FinalRebuildCursorPresent => "final rebuild cursor is present",
            Self::FinalRebuildChecksumMissing => "final rebuild checksum is missing",
            Self::InvalidRebuildRecord => "rebuild record is invalid",
            Self::InvalidErrorBody => "error body is invalid",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for ContractViolation {}

impl From<crate::change::ChangeOperation> for Operation {
    fn from(value: crate::change::ChangeOperation) -> Self {
        match value {
            crate::change::ChangeOperation::Insert => Self::Insert,
            crate::change::ChangeOperation::Update => Self::Update,
            crate::change::ChangeOperation::Delete => Self::Delete,
        }
    }
}

impl TryFrom<Operation> for crate::change::ChangeOperation {
    type Error = ChangeOperationConversionError;

    fn try_from(value: Operation) -> Result<Self, Self::Error> {
        match value {
            Operation::Insert => Ok(crate::change::ChangeOperation::Insert),
            Operation::Update => Ok(crate::change::ChangeOperation::Update),
            Operation::Delete => Ok(crate::change::ChangeOperation::Delete),
            Operation::Upsert => Err(ChangeOperationConversionError { operation: value }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaAction {
    None,
    Replace,
    RebuildLocal,
    Unsupported,
}

impl SchemaAction {
    pub const fn requires_schema_definition(self) -> bool {
        matches!(self, Self::Replace | Self::RebuildLocal)
    }

    pub const fn requires_local_rebuild(self) -> bool {
        matches!(self, Self::RebuildLocal)
    }

    pub const fn is_compatible(self) -> bool {
        !matches!(self, Self::Unsupported)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaUnsupportedReason {
    UnknownSchemaLineage,
    IncompatibleSchemaTransition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationStatus {
    Applied,
    Conflict,
    RejectedTerminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationRejectionCode {
    VersionConflict,
    RowAlreadyExists,
    RowDeleted,
    RowNotFound,
    SchemaIncompatible,
    TableNotSynced,
    PolicyRejected,
    ValidationFailed,
}

impl MutationRejectionCode {
    const fn is_conflict(self) -> bool {
        matches!(
            self,
            Self::VersionConflict | Self::RowAlreadyExists | Self::RowDeleted | Self::RowNotFound
        )
    }

    const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::SchemaIncompatible
                | Self::TableNotSynced
                | Self::PolicyRejected
                | Self::ValidationFailed
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolErrorCode {
    InvalidRequest,
    InvalidSchemaReference,
    UpgradeRequired,
    AuthRequired,
    IdempotencyConflict,
    ClientRetired,
    ClientGenerationExpired,
    RebuildRestartRequired,
    SchemaMismatch,
    RetryLater,
    SyncIntegrityFailure,
    CapturePending,
    TemporaryUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TemporaryUnavailableReason {
    CaptureBlocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompositionClass {
    SingleScope,
    MultiScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaRef {
    pub version: i64,
    pub hash: String,
}

impl SchemaRef {
    pub fn is_fresh_sentinel(&self) -> bool {
        self.version == 0 && self.hash.is_empty()
    }

    pub fn validate_normal(&self) -> Result<(), ContractViolation> {
        if !is_positive_safe_integer(self.version) || !is_lower_sha256(&self.hash) {
            return Err(ContractViolation::InvalidSchemaReference);
        }
        Ok(())
    }

    pub fn validate_connect(&self) -> Result<(), ContractViolation> {
        if self.is_fresh_sentinel() {
            return Ok(());
        }
        self.validate_normal()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeCursorRef {
    pub cursor: Option<String>,
}

impl ScopeCursorRef {
    fn validate(&self) -> Result<(), ContractViolation> {
        validate_optional_opaque(self.cursor.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeAssignment {
    pub id: String,
    pub cursor: Option<String>,
}

impl ScopeAssignment {
    fn validate(&self) -> Result<(), ContractViolation> {
        require_nonempty(&self.id)?;
        validate_optional_opaque(self.cursor.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeAssignmentDelta {
    pub add: Vec<ScopeAssignment>,
    pub remove: Vec<String>,
}

impl ScopeAssignmentDelta {
    pub fn validate(&self) -> Result<(), ContractViolation> {
        let mut added = HashSet::with_capacity(self.add.len());
        for scope in &self.add {
            scope.validate()?;
            if !added.insert(scope.id.as_str()) {
                return Err(ContractViolation::DuplicateScope);
            }
        }

        let mut removed = HashSet::with_capacity(self.remove.len());
        for scope_id in &self.remove {
            require_nonempty(scope_id)?;
            if !removed.insert(scope_id.as_str()) {
                return Err(ContractViolation::DuplicateScope);
            }
            if added.contains(scope_id.as_str()) {
                return Err(ContractViolation::ConflictingScopeAssignment);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaDescriptor {
    pub version: i64,
    pub hash: String,
    pub action: SchemaAction,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub reason: Option<SchemaUnsupportedReason>,
}

impl SchemaDescriptor {
    fn validate(&self) -> Result<(), ContractViolation> {
        SchemaRef {
            version: self.version,
            hash: self.hash.clone(),
        }
        .validate_normal()?;
        if matches!(self.action, SchemaAction::Unsupported) != self.reason.is_some() {
            return Err(ContractViolation::InvalidSchemaActionFields);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnSchema {
    pub field_id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
    pub nullable: bool,
    pub writable: bool,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub precision: Option<i32>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub scale: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexSchema {
    pub index_id: String,
    pub name: String,
    pub field_ids: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleSchema {
    pub created_at_field_id: Option<String>,
    pub updated_at_field_id: Option<String>,
    pub deleted_at_field_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableSchema {
    pub table_id: String,
    pub relation_id: String,
    pub name: String,
    pub primary_key_field_id: String,
    pub lifecycle: LifecycleSchema,
    pub composition: CompositionClass,
    pub fields: Vec<ColumnSchema>,
    pub indexes: Vec<IndexSchema>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaTransitionClass {
    Initial,
    #[serde(rename = "class_2")]
    Class2,
    #[serde(rename = "class_3")]
    Class3,
    #[serde(rename = "class_4")]
    Class4,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaManifest {
    pub schema_version: i64,
    pub schema_hash: String,
    pub parent_schema: Option<SchemaRef>,
    pub transition_class: SchemaTransitionClass,
    pub compatibility_floor: i64,
    pub tables: Vec<TableSchema>,
}

pub fn normalize_portable_type_name(type_name: &str) -> Option<&'static str> {
    let normalized = type_name.trim().to_ascii_lowercase();
    if normalized.ends_with("[]") {
        return Some("json");
    }
    if normalized.starts_with("numeric(") || normalized.starts_with("decimal(") {
        return Some("decimal");
    }
    if normalized.starts_with("character varying")
        || normalized.starts_with("varchar(")
        || normalized.starts_with("character(")
    {
        return Some("string");
    }
    if normalized.ends_with("range") {
        return Some("string");
    }
    match normalized.as_str() {
        "string" | "text" | "uuid" | "varchar" | "character" | "interval" | "inet" | "cidr"
        | "macaddr" | "macaddr8" | "xml" | "point" | "line" | "lseg" | "box" | "path"
        | "polygon" | "circle" => Some("string"),
        "int" | "int32" | "smallint" | "integer" => Some("int"),
        "int64" | "bigint" => Some("int64"),
        "decimal" | "numeric" => Some("decimal"),
        "float" | "float64" | "real" | "double precision" => Some("float"),
        "boolean" | "bool" => Some("boolean"),
        "datetime" | "timestamp" | "timestamp with time zone" | "timestamp without time zone" => {
            Some("datetime")
        }
        "date" => Some("date"),
        "time" | "time without time zone" => Some("time"),
        "json" | "jsonb" => Some("json"),
        "bytes" | "blob" | "bytea" => Some("bytes"),
        _ => None,
    }
}

pub fn is_canonical_portable_type_name(type_name: &str) -> bool {
    matches!(
        type_name,
        "string"
            | "int"
            | "int64"
            | "decimal"
            | "float"
            | "boolean"
            | "datetime"
            | "date"
            | "time"
            | "json"
            | "bytes"
    )
}

impl SchemaManifest {
    pub fn validate(&self) -> Result<(), ContractViolation> {
        SchemaRef {
            version: self.schema_version,
            hash: self.schema_hash.clone(),
        }
        .validate_normal()
        .map_err(|_| ContractViolation::InvalidSchemaManifest)?;

        let is_initial = matches!(self.transition_class, SchemaTransitionClass::Initial);
        if is_initial != self.parent_schema.is_none() {
            return Err(ContractViolation::InvalidSchemaManifest);
        }
        if let Some(parent) = &self.parent_schema {
            parent
                .validate_normal()
                .map_err(|_| ContractViolation::InvalidSchemaManifest)?;
            if parent.version >= self.schema_version {
                return Err(ContractViolation::InvalidSchemaManifest);
            }
        }
        if !is_positive_safe_integer(self.compatibility_floor)
            || self.compatibility_floor > self.schema_version
            || (!matches!(self.transition_class, SchemaTransitionClass::Class2)
                && self.compatibility_floor != self.schema_version)
        {
            return Err(ContractViolation::InvalidSchemaManifest);
        }

        let mut table_ids = HashSet::with_capacity(self.tables.len());
        let mut relation_ids = HashSet::with_capacity(self.tables.len());
        let mut table_names = HashSet::with_capacity(self.tables.len());
        for table in &self.tables {
            require_nonempty(&table.table_id)?;
            require_nonempty(&table.relation_id)?;
            require_nonempty(&table.name)?;
            require_nonempty(&table.primary_key_field_id)?;
            if !table_ids.insert(table.table_id.as_str())
                || !relation_ids.insert(table.relation_id.as_str())
                || !table_names.insert(table.name.as_str())
            {
                return Err(ContractViolation::InvalidSchemaManifest);
            }

            let mut field_ids = HashSet::with_capacity(table.fields.len());
            let mut field_names = HashSet::with_capacity(table.fields.len());
            for field in &table.fields {
                require_nonempty(&field.field_id)?;
                require_nonempty(&field.name)?;
                if !field_ids.insert(field.field_id.as_str())
                    || !field_names.insert(field.name.as_str())
                    || !is_canonical_portable_type_name(&field.type_name)
                {
                    return Err(ContractViolation::InvalidSchemaManifest);
                }
                match (field.type_name.as_str(), field.precision, field.scale) {
                    ("decimal", Some(precision), Some(scale))
                        if precision > 0 && scale >= 0 && scale <= precision => {}
                    ("decimal", _, _) => return Err(ContractViolation::InvalidSchemaManifest),
                    (_, None, None) => {}
                    _ => return Err(ContractViolation::InvalidSchemaManifest),
                }
            }

            let primary_key = table
                .fields
                .iter()
                .find(|field| field.field_id == table.primary_key_field_id)
                .ok_or(ContractViolation::InvalidSchemaManifest)?;
            if primary_key.nullable
                || primary_key.writable
                || !matches!(primary_key.type_name.as_str(), "string" | "int" | "int64")
            {
                return Err(ContractViolation::InvalidSchemaManifest);
            }

            for field_id in [
                table.lifecycle.created_at_field_id.as_deref(),
                table.lifecycle.updated_at_field_id.as_deref(),
                table.lifecycle.deleted_at_field_id.as_deref(),
            ]
            .into_iter()
            .flatten()
            {
                let lifecycle_field = table
                    .fields
                    .iter()
                    .find(|field| field.field_id == field_id)
                    .ok_or(ContractViolation::InvalidSchemaManifest)?;
                if lifecycle_field.type_name != "datetime" || lifecycle_field.writable {
                    return Err(ContractViolation::InvalidSchemaManifest);
                }
            }

            let mut index_ids = HashSet::with_capacity(table.indexes.len());
            let mut index_names = HashSet::with_capacity(table.indexes.len());
            for index in &table.indexes {
                require_nonempty(&index.index_id)?;
                require_nonempty(&index.name)?;
                if index.field_ids.is_empty()
                    || !index_ids.insert(index.index_id.as_str())
                    || !index_names.insert(index.name.as_str())
                {
                    return Err(ContractViolation::InvalidSchemaManifest);
                }
                let mut indexed_fields = HashSet::with_capacity(index.field_ids.len());
                for field_id in &index.field_ids {
                    if field_id.is_empty()
                        || !field_ids.contains(field_id.as_str())
                        || !indexed_fields.insert(field_id.as_str())
                    {
                        return Err(ContractViolation::InvalidSchemaManifest);
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectRequest {
    pub client_id: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub client_generation: Option<i64>,
    pub platform: String,
    pub app_version: String,
    pub protocol_version: u32,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub schema_reset: Option<bool>,
    pub schema: SchemaRef,
    pub scope_set_version: i64,
    pub known_scopes: BTreeMap<String, ScopeCursorRef>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub seed_receipts: Option<BTreeMap<String, String>>,
}

impl ConnectRequest {
    pub fn validate(&self) -> Result<(), ContractViolation> {
        require_nonempty(&self.client_id)?;
        require_nonempty(&self.platform)?;
        if !is_semver(&self.app_version) {
            return Err(ContractViolation::InvalidSemver);
        }
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(ContractViolation::InvalidProtocolVersion);
        }
        if let Some(generation) = self.client_generation {
            validate_positive_safe_integer(generation)?;
        }
        validate_nonnegative_safe_integer(self.scope_set_version)?;
        self.schema.validate_connect()?;
        if self.schema.is_fresh_sentinel() {
            if self.client_generation.is_some()
                || self.schema_reset == Some(true)
                || self.scope_set_version != 0
                || !self.known_scopes.is_empty()
            {
                return Err(ContractViolation::InvalidFreshSchemaReference);
            }
        } else if self.schema_reset == Some(true) && self.client_generation.is_none() {
            return Err(ContractViolation::InvalidFreshSchemaReference);
        }
        validate_scope_map(&self.known_scopes)?;
        if let Some(seed_receipts) = &self.seed_receipts {
            if seed_receipts.is_empty() {
                return Err(ContractViolation::InvalidScopeAssignment);
            }
            for (scope_id, receipt) in seed_receipts {
                require_nonempty(scope_id)?;
                if receipt.is_empty() {
                    return Err(ContractViolation::InvalidOpaqueValue);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectResponse {
    pub server_time: DateTime<Utc>,
    pub protocol_version: u32,
    pub client_generation: i64,
    pub scope_set_version: i64,
    pub schema: SchemaDescriptor,
    pub scopes: ScopeAssignmentDelta,
    pub scope_cursor_updates: BTreeMap<String, Option<String>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub schema_definition: Option<SchemaManifest>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub affected_scopes: Option<Vec<String>>,
}

impl ConnectResponse {
    pub fn validate(&self) -> Result<(), ContractViolation> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(ContractViolation::InvalidProtocolVersion);
        }
        validate_positive_safe_integer(self.client_generation)?;
        validate_nonnegative_safe_integer(self.scope_set_version)?;
        self.schema.validate()?;
        self.scopes.validate()?;
        validate_scope_cursor_updates(&self.scope_cursor_updates, &self.scopes)?;

        let has_definition = self.schema_definition.is_some();
        if self.schema.action.requires_schema_definition() != has_definition {
            return Err(ContractViolation::InvalidSchemaActionFields);
        }
        if let Some(definition) = &self.schema_definition {
            definition.validate()?;
            if definition.schema_version != self.schema.version
                || definition.schema_hash != self.schema.hash
            {
                return Err(ContractViolation::InvalidSchemaDefinition);
            }
        }

        match self.schema.action {
            SchemaAction::RebuildLocal => {
                let affected = self
                    .affected_scopes
                    .as_deref()
                    .ok_or(ContractViolation::InvalidSchemaActionFields)?;
                validate_sorted_scope_list(affected, true)?;
            }
            _ if self.affected_scopes.is_some() => {
                return Err(ContractViolation::InvalidSchemaActionFields)
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mutation {
    pub mutation_id: String,
    pub table: String,
    #[serde(deserialize_with = "deserialize_strict_json_value")]
    pub pk: Value,
    pub authored_schema: SchemaRef,
    pub op: Operation,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub base_version: Option<String>,
    pub client_version: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_strict_json_value"
    )]
    pub columns: Option<Value>,
}

impl Mutation {
    pub fn validate(&self) -> Result<(), ContractViolation> {
        validate_uuid(&self.mutation_id)?;
        require_nonempty(&self.table)?;
        validate_one_field_pk(&self.pk)?;
        self.authored_schema.validate_normal()?;
        if !is_canonical_utc_microsecond(&self.client_version) {
            return Err(ContractViolation::InvalidMutationClientVersion);
        }

        match self.op {
            Operation::Insert => {
                if self.base_version.is_some() {
                    return Err(ContractViolation::UnexpectedMutationBaseVersion);
                }
                validate_columns(self.columns.as_ref())
                    .map_err(|_| ContractViolation::MissingMutationColumns)?;
            }
            Operation::Update => {
                if self.base_version.as_deref().is_none_or(str::is_empty) {
                    return Err(ContractViolation::MissingMutationBaseVersion);
                }
                validate_columns(self.columns.as_ref())
                    .map_err(|_| ContractViolation::MissingMutationColumns)?;
            }
            Operation::Delete => {
                if self.base_version.as_deref().is_none_or(str::is_empty) {
                    return Err(ContractViolation::MissingMutationBaseVersion);
                }
                if self.columns.is_some() {
                    return Err(ContractViolation::UnexpectedMutationColumns);
                }
            }
            Operation::Upsert => return Err(ContractViolation::InvalidPushOperation),
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PushRequest {
    pub client_id: String,
    pub client_generation: i64,
    pub batch_id: String,
    pub schema: SchemaRef,
    pub mutations: Vec<Mutation>,
}

impl PushRequest {
    pub fn validate(&self) -> Result<(), ContractViolation> {
        require_nonempty(&self.client_id)?;
        validate_positive_safe_integer(self.client_generation)?;
        validate_uuid(&self.batch_id)?;
        self.schema.validate_normal()?;
        if self.mutations.is_empty() || self.mutations.len() > MAX_PUSH_MUTATIONS {
            return Err(ContractViolation::InvalidPushOperation);
        }
        let mut mutation_ids = HashSet::with_capacity(self.mutations.len());
        for mutation in &self.mutations {
            mutation.validate()?;
            if !mutation_ids.insert(mutation.mutation_id.as_str()) {
                return Err(ContractViolation::DuplicateMutationId);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedMutation {
    pub mutation_id: String,
    pub table: String,
    #[serde(deserialize_with = "deserialize_strict_json_value")]
    pub pk: Value,
    pub outcome_schema: SchemaRef,
    pub status: MutationStatus,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_strict_json_value"
    )]
    pub server_row: Option<Value>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub row_checksum: Option<ChecksumObject>,
    pub server_version: String,
}

impl AcceptedMutation {
    fn validate(&self) -> Result<(), ContractViolation> {
        validate_uuid(&self.mutation_id)?;
        require_nonempty(&self.table)?;
        validate_one_field_pk(&self.pk)?;
        self.outcome_schema.validate_normal()?;
        if self.status != MutationStatus::Applied || self.server_version.is_empty() {
            return Err(ContractViolation::InvalidPushOutcome);
        }
        validate_optional_row_checksum_pair(self.server_row.as_ref(), self.row_checksum.as_ref())
            .map_err(|_| ContractViolation::InvalidPushOutcome)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RejectedMutation {
    pub mutation_id: String,
    pub table: String,
    #[serde(deserialize_with = "deserialize_strict_json_value")]
    pub pk: Value,
    pub outcome_schema: SchemaRef,
    pub status: MutationStatus,
    pub code: MutationRejectionCode,
    pub message: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub retryable: Option<bool>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_strict_json_value"
    )]
    pub server_row: Option<Value>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub row_checksum: Option<ChecksumObject>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub server_version: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub authored_schema: Option<SchemaRef>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub current_schema: Option<SchemaRef>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub incompatible_field_ids: Option<Vec<String>>,
}

impl RejectedMutation {
    fn validate(&self) -> Result<(), ContractViolation> {
        validate_uuid(&self.mutation_id)?;
        require_nonempty(&self.table)?;
        validate_one_field_pk(&self.pk)?;
        self.outcome_schema.validate_normal()?;
        require_nonempty(&self.message)?;

        match self.status {
            MutationStatus::Conflict if self.code.is_conflict() => {
                if self.retryable.is_some()
                    || self.authored_schema.is_some()
                    || self.current_schema.is_some()
                    || self.incompatible_field_ids.is_some()
                {
                    return Err(ContractViolation::InvalidPushOutcome);
                }
                validate_optional_row_checksum_pair(
                    self.server_row.as_ref(),
                    self.row_checksum.as_ref(),
                )
                .map_err(|_| ContractViolation::InvalidPushOutcome)?;
                if self.server_row.is_some()
                    && self.server_version.as_deref().is_none_or(str::is_empty)
                {
                    return Err(ContractViolation::InvalidPushOutcome);
                }
                validate_optional_opaque(self.server_version.as_deref())
            }
            MutationStatus::RejectedTerminal if self.code.is_terminal() => {
                if self.server_row.is_some()
                    || self.row_checksum.is_some()
                    || self.server_version.is_some()
                {
                    return Err(ContractViolation::InvalidPushOutcome);
                }
                if self.code == MutationRejectionCode::SchemaIncompatible {
                    if self.retryable != Some(false) {
                        return Err(ContractViolation::InvalidPushOutcome);
                    }
                    self.authored_schema
                        .as_ref()
                        .ok_or(ContractViolation::InvalidPushOutcome)?
                        .validate_normal()?;
                    self.current_schema
                        .as_ref()
                        .ok_or(ContractViolation::InvalidPushOutcome)?
                        .validate_normal()?;
                    validate_sorted_field_ids(
                        self.incompatible_field_ids
                            .as_deref()
                            .ok_or(ContractViolation::InvalidPushOutcome)?,
                    )?;
                } else if self.retryable.is_some()
                    || self.authored_schema.is_some()
                    || self.current_schema.is_some()
                    || self.incompatible_field_ids.is_some()
                {
                    return Err(ContractViolation::InvalidPushOutcome);
                }
                Ok(())
            }
            _ => Err(ContractViolation::InvalidPushOutcome),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PushResponse {
    pub batch_id: String,
    pub server_time: DateTime<Utc>,
    pub accepted: Vec<AcceptedMutation>,
    pub rejected: Vec<RejectedMutation>,
}

impl PushResponse {
    pub fn validate(&self) -> Result<(), ContractViolation> {
        validate_uuid(&self.batch_id)?;
        let mut mutation_ids = HashSet::with_capacity(self.accepted.len() + self.rejected.len());
        for accepted in &self.accepted {
            accepted.validate()?;
            if !mutation_ids.insert(accepted.mutation_id.as_str()) {
                return Err(ContractViolation::InvalidPushOutcomePartition);
            }
        }
        for rejected in &self.rejected {
            rejected.validate()?;
            if !mutation_ids.insert(rejected.mutation_id.as_str()) {
                return Err(ContractViolation::InvalidPushOutcomePartition);
            }
        }
        Ok(())
    }

    pub fn validate_for_request(&self, request: &PushRequest) -> Result<(), ContractViolation> {
        request.validate()?;
        self.validate()?;
        if self.batch_id != request.batch_id {
            return Err(ContractViolation::InvalidPushOutcomePartition);
        }

        let accepted = self
            .accepted
            .iter()
            .map(|outcome| (outcome.mutation_id.as_str(), outcome))
            .collect::<BTreeMap<_, _>>();
        let rejected = self
            .rejected
            .iter()
            .map(|outcome| (outcome.mutation_id.as_str(), outcome))
            .collect::<BTreeMap<_, _>>();
        if accepted.len() + rejected.len() != request.mutations.len() {
            return Err(ContractViolation::InvalidPushOutcomePartition);
        }

        let mut expected_accepted = Vec::new();
        let mut expected_rejected = Vec::new();
        for mutation in &request.mutations {
            match (
                accepted.get(mutation.mutation_id.as_str()),
                rejected.get(mutation.mutation_id.as_str()),
            ) {
                (Some(outcome), None) => {
                    if outcome.table != mutation.table || outcome.pk != mutation.pk {
                        return Err(ContractViolation::InvalidPushOutcomePartition);
                    }
                    if matches!(mutation.op, Operation::Insert | Operation::Update)
                        && outcome.server_row.is_none()
                    {
                        return Err(ContractViolation::InvalidPushOutcomePartition);
                    }
                    expected_accepted.push(mutation.mutation_id.as_str());
                }
                (None, Some(outcome)) => {
                    if outcome.table != mutation.table || outcome.pk != mutation.pk {
                        return Err(ContractViolation::InvalidPushOutcomePartition);
                    }
                    expected_rejected.push(mutation.mutation_id.as_str());
                }
                _ => return Err(ContractViolation::InvalidPushOutcomePartition),
            }
        }
        if self
            .accepted
            .iter()
            .map(|outcome| outcome.mutation_id.as_str())
            .ne(expected_accepted)
            || self
                .rejected
                .iter()
                .map(|outcome| outcome.mutation_id.as_str())
                .ne(expected_rejected)
        {
            return Err(ContractViolation::InvalidPushOutcomePartition);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullRequest {
    pub client_id: String,
    pub client_generation: i64,
    pub schema: SchemaRef,
    pub scope_set_version: i64,
    pub scopes: BTreeMap<String, ScopeCursorRef>,
    pub limit: i64,
}

impl PullRequest {
    pub fn validate(&self) -> Result<(), ContractViolation> {
        require_nonempty(&self.client_id)?;
        validate_positive_safe_integer(self.client_generation)?;
        self.schema.validate_normal()?;
        validate_nonnegative_safe_integer(self.scope_set_version)?;
        validate_scope_map(&self.scopes)?;
        validate_positive_safe_integer(self.limit)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangeRecord {
    pub scope: String,
    pub table: String,
    pub op: Operation,
    #[serde(deserialize_with = "deserialize_strict_json_value")]
    pub pk: Value,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_strict_json_value"
    )]
    pub row: Option<Value>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub row_checksum: Option<ChecksumObject>,
    pub server_version: String,
}

impl ChangeRecord {
    fn validate(&self) -> Result<(), ContractViolation> {
        require_nonempty(&self.scope)?;
        require_nonempty(&self.table)?;
        validate_one_field_pk(&self.pk)?;
        require_nonempty(&self.server_version)?;
        match self.op {
            Operation::Upsert => {
                validate_optional_row_checksum_pair(self.row.as_ref(), self.row_checksum.as_ref())
                    .map_err(|_| ContractViolation::InvalidPullChange)?;
                if self.row.is_none() {
                    return Err(ContractViolation::InvalidPullChange);
                }
            }
            Operation::Delete => {
                validate_optional_row_checksum_pair(self.row.as_ref(), self.row_checksum.as_ref())
                    .map_err(|_| ContractViolation::InvalidPullChange)?;
            }
            _ => return Err(ContractViolation::InvalidPullChange),
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullResponse {
    pub changes: Vec<ChangeRecord>,
    pub scope_set_version: i64,
    pub scope_cursors: BTreeMap<String, String>,
    pub scope_updates: ScopeAssignmentDelta,
    pub rebuild: Vec<String>,
    pub has_more: bool,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub checksums: Option<BTreeMap<String, ChecksumObject>>,
}

impl PullResponse {
    pub fn requests_rebuild(&self) -> bool {
        !self.rebuild.is_empty()
    }

    pub fn validate(&self) -> Result<(), ContractViolation> {
        validate_nonnegative_safe_integer(self.scope_set_version)?;
        self.scope_updates.validate()?;
        let removed = self
            .scope_updates
            .remove
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        let added = self
            .scope_updates
            .add
            .iter()
            .map(|scope| scope.id.as_str())
            .collect::<HashSet<_>>();
        let rebuild = validate_scope_set(&self.rebuild)?;

        if self
            .scope_updates
            .add
            .iter()
            .any(|scope| scope.cursor.is_some())
            || rebuild.iter().any(|scope_id| removed.contains(scope_id))
        {
            return Err(ContractViolation::InvalidScopeAssignment);
        }
        for (scope_id, cursor) in &self.scope_cursors {
            require_nonempty(scope_id)?;
            require_nonempty(cursor)?;
            if removed.contains(scope_id.as_str()) || rebuild.contains(scope_id.as_str()) {
                return Err(ContractViolation::InvalidScopeAssignment);
            }
        }
        if added.iter().any(|scope_id| !rebuild.contains(scope_id)) {
            return Err(ContractViolation::InvalidScopeAssignment);
        }
        for change in &self.changes {
            change.validate()?;
            if removed.contains(change.scope.as_str()) || rebuild.contains(change.scope.as_str()) {
                return Err(ContractViolation::InvalidScopeAssignment);
            }
        }

        match (self.has_more, &self.checksums) {
            (true, Some(_)) => return Err(ContractViolation::NonterminalPullChecksumsPresent),
            (false, None) => return Err(ContractViolation::FinalPullChecksumsMissing),
            (_, None) => return Ok(()),
            (false, Some(checksums)) => {
                for scope_id in checksums.keys() {
                    require_nonempty(scope_id)?;
                    if removed.contains(scope_id.as_str()) {
                        return Err(ContractViolation::InvalidScopeAssignment);
                    }
                }
                if rebuild
                    .iter()
                    .any(|scope_id| !checksums.contains_key(*scope_id))
                {
                    return Err(ContractViolation::InvalidScopeAssignment);
                }
            }
        }
        Ok(())
    }

    pub fn validate_for_active_scopes(
        &self,
        active_scopes_before_update: &HashSet<String>,
    ) -> Result<(), ContractViolation> {
        self.validate()?;
        if self.has_more {
            return Ok(());
        }
        let mut expected = active_scopes_before_update.clone();
        for scope_id in &self.scope_updates.remove {
            expected.remove(scope_id);
        }
        expected.extend(self.scope_updates.add.iter().map(|scope| scope.id.clone()));
        let checksums = self
            .checksums
            .as_ref()
            .expect("validate requires terminal checksums");
        if checksums.len() != expected.len()
            || checksums
                .keys()
                .any(|scope_id| !expected.contains(scope_id))
        {
            return Err(ContractViolation::InvalidScopeAssignment);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebuildRequest {
    pub client_id: String,
    pub client_generation: i64,
    pub schema: SchemaRef,
    pub scope: String,
    pub rebuild_id: String,
    pub cursor: Option<String>,
    pub limit: i64,
}

impl RebuildRequest {
    pub fn validate(&self) -> Result<(), ContractViolation> {
        require_nonempty(&self.client_id)?;
        validate_positive_safe_integer(self.client_generation)?;
        self.schema.validate_normal()?;
        require_nonempty(&self.scope)?;
        validate_uuid(&self.rebuild_id)?;
        validate_optional_opaque(self.cursor.as_deref())?;
        validate_positive_safe_integer(self.limit)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebuildRecord {
    pub table: String,
    #[serde(deserialize_with = "deserialize_strict_json_value")]
    pub pk: Value,
    #[serde(deserialize_with = "deserialize_strict_json_value")]
    pub row: Value,
    pub row_checksum: ChecksumObject,
    pub server_version: String,
}

impl RebuildRecord {
    fn validate(&self) -> Result<(), ContractViolation> {
        require_nonempty(&self.table)?;
        validate_one_field_pk(&self.pk)?;
        validate_row(&self.row)?;
        require_nonempty(&self.server_version)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebuildResponse {
    pub scope: String,
    pub records: Vec<RebuildRecord>,
    pub cursor: Option<String>,
    pub has_more: bool,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub final_scope_cursor: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub checksum: Option<ChecksumObject>,
}

impl RebuildResponse {
    pub fn is_final_page(&self) -> bool {
        !self.has_more && self.final_scope_cursor.is_some()
    }

    pub fn validate(&self) -> Result<(), ContractViolation> {
        require_nonempty(&self.scope)?;
        for record in &self.records {
            record.validate()?;
        }
        if self.has_more {
            if self.cursor.as_deref().is_none_or(str::is_empty) {
                return Err(ContractViolation::PartialRebuildCursorMissing);
            }
            if self.final_scope_cursor.is_some() {
                return Err(ContractViolation::PartialRebuildHasFinalCursor);
            }
            if self.checksum.is_some() {
                return Err(ContractViolation::PartialRebuildHasChecksum);
            }
        } else {
            if self.cursor.is_some() {
                return Err(ContractViolation::FinalRebuildCursorPresent);
            }
            if self.final_scope_cursor.as_deref().is_none_or(str::is_empty) {
                return Err(ContractViolation::FinalRebuildCursorMissing);
            }
            if self.checksum.is_none() {
                return Err(ContractViolation::FinalRebuildChecksumMissing);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrorBody {
    pub code: ProtocolErrorCode,
    pub message: String,
    pub retryable: bool,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub current_schema: Option<SchemaRef>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub received_schema: Option<SchemaRef>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub current_client_generation: Option<i64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub scope_id: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub required_protocol_version: Option<u32>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub received_protocol_version: Option<u32>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub minimum_client_version: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub received_client_version: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub reason: Option<TemporaryUnavailableReason>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub field: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub minimum: Option<i64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_optional_non_null"
    )]
    pub maximum: Option<i64>,
}

impl ErrorBody {
    pub fn validate(&self) -> Result<(), ContractViolation> {
        require_nonempty(&self.message)?;
        let expected_retryable = matches!(
            self.code,
            ProtocolErrorCode::RetryLater
                | ProtocolErrorCode::CapturePending
                | ProtocolErrorCode::TemporaryUnavailable
        );
        if self.retryable != expected_retryable {
            return Err(ContractViolation::InvalidErrorBody);
        }

        let no_context = || {
            self.current_schema.is_none()
                && self.received_schema.is_none()
                && self.current_client_generation.is_none()
                && self.scope_id.is_none()
                && self.required_protocol_version.is_none()
                && self.received_protocol_version.is_none()
                && self.minimum_client_version.is_none()
                && self.received_client_version.is_none()
                && self.reason.is_none()
                && self.field.is_none()
                && self.minimum.is_none()
                && self.maximum.is_none()
        };
        match self.code {
            ProtocolErrorCode::InvalidSchemaReference => {
                if self.received_schema.is_none()
                    || self.current_schema.is_some()
                    || self.current_client_generation.is_some()
                    || self.scope_id.is_some()
                    || self.required_protocol_version.is_some()
                    || self.received_protocol_version.is_some()
                    || self.minimum_client_version.is_some()
                    || self.received_client_version.is_some()
                    || self.reason.is_some()
                    || self.field.is_some()
                    || self.minimum.is_some()
                    || self.maximum.is_some()
                {
                    return Err(ContractViolation::InvalidErrorBody);
                }
            }
            ProtocolErrorCode::UpgradeRequired => {
                let protocol_pair = self.required_protocol_version.is_some()
                    && self.received_protocol_version.is_some()
                    && self.minimum_client_version.is_none()
                    && self.received_client_version.is_none();
                let version_pair = self.required_protocol_version.is_none()
                    && self.received_protocol_version.is_none()
                    && self
                        .minimum_client_version
                        .as_deref()
                        .is_some_and(is_semver)
                    && self
                        .received_client_version
                        .as_deref()
                        .is_some_and(is_semver);
                if !(protocol_pair || version_pair)
                    || self.current_schema.is_some()
                    || self.received_schema.is_some()
                    || self.current_client_generation.is_some()
                    || self.scope_id.is_some()
                    || self.reason.is_some()
                    || self.field.is_some()
                    || self.minimum.is_some()
                    || self.maximum.is_some()
                {
                    return Err(ContractViolation::InvalidErrorBody);
                }
            }
            ProtocolErrorCode::ClientGenerationExpired => {
                if !self
                    .current_client_generation
                    .is_some_and(is_positive_safe_integer)
                    || !context_only(self, ContextField::ClientGeneration)
                {
                    return Err(ContractViolation::InvalidErrorBody);
                }
            }
            ProtocolErrorCode::RebuildRestartRequired => {
                if self.scope_id.as_deref().is_none_or(str::is_empty)
                    || !context_only(self, ContextField::ScopeId)
                {
                    return Err(ContractViolation::InvalidErrorBody);
                }
            }
            ProtocolErrorCode::SchemaMismatch => {
                if self
                    .current_schema
                    .as_ref()
                    .is_none_or(|schema| schema.validate_normal().is_err())
                    || self
                        .received_schema
                        .as_ref()
                        .is_none_or(|schema| schema.validate_normal().is_err())
                    || !context_only(self, ContextField::SchemaPair)
                {
                    return Err(ContractViolation::InvalidErrorBody);
                }
            }
            ProtocolErrorCode::TemporaryUnavailable => {
                if self.reason.is_some() && !context_only(self, ContextField::Reason) {
                    return Err(ContractViolation::InvalidErrorBody);
                }
                if self.reason.is_none() && !no_context() {
                    return Err(ContractViolation::InvalidErrorBody);
                }
            }
            ProtocolErrorCode::InvalidRequest
                if self.field.is_some() || self.minimum.is_some() || self.maximum.is_some() =>
            {
                if self.field.as_deref().is_none_or(str::is_empty)
                    || !self.minimum.is_some_and(is_positive_safe_integer)
                    || !self.maximum.is_some_and(is_positive_safe_integer)
                    || self.minimum > self.maximum
                    || !context_only(self, ContextField::Limit)
                {
                    return Err(ContractViolation::InvalidErrorBody);
                }
            }
            _ if !no_context() => return Err(ContractViolation::InvalidErrorBody),
            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum ContextField {
    ClientGeneration,
    ScopeId,
    SchemaPair,
    Reason,
    Limit,
}

fn context_only(error: &ErrorBody, expected: ContextField) -> bool {
    match expected {
        ContextField::ClientGeneration => {
            error.current_schema.is_none()
                && error.received_schema.is_none()
                && error.scope_id.is_none()
                && error.required_protocol_version.is_none()
                && error.received_protocol_version.is_none()
                && error.minimum_client_version.is_none()
                && error.received_client_version.is_none()
                && error.reason.is_none()
                && error.field.is_none()
                && error.minimum.is_none()
                && error.maximum.is_none()
        }
        ContextField::ScopeId => {
            error.current_schema.is_none()
                && error.received_schema.is_none()
                && error.current_client_generation.is_none()
                && error.required_protocol_version.is_none()
                && error.received_protocol_version.is_none()
                && error.minimum_client_version.is_none()
                && error.received_client_version.is_none()
                && error.reason.is_none()
                && error.field.is_none()
                && error.minimum.is_none()
                && error.maximum.is_none()
        }
        ContextField::SchemaPair => {
            error.current_client_generation.is_none()
                && error.scope_id.is_none()
                && error.required_protocol_version.is_none()
                && error.received_protocol_version.is_none()
                && error.minimum_client_version.is_none()
                && error.received_client_version.is_none()
                && error.reason.is_none()
                && error.field.is_none()
                && error.minimum.is_none()
                && error.maximum.is_none()
        }
        ContextField::Reason => {
            error.current_schema.is_none()
                && error.received_schema.is_none()
                && error.current_client_generation.is_none()
                && error.scope_id.is_none()
                && error.required_protocol_version.is_none()
                && error.received_protocol_version.is_none()
                && error.minimum_client_version.is_none()
                && error.received_client_version.is_none()
                && error.field.is_none()
                && error.minimum.is_none()
                && error.maximum.is_none()
        }
        ContextField::Limit => {
            error.current_schema.is_none()
                && error.received_schema.is_none()
                && error.current_client_generation.is_none()
                && error.scope_id.is_none()
                && error.required_protocol_version.is_none()
                && error.received_protocol_version.is_none()
                && error.minimum_client_version.is_none()
                && error.received_client_version.is_none()
                && error.reason.is_none()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrorResponse {
    pub error: ErrorBody,
}

impl ErrorResponse {
    pub fn validate(&self) -> Result<(), ContractViolation> {
        self.error.validate()
    }
}

fn is_positive_safe_integer(value: i64) -> bool {
    (1..=MAX_SAFE_INTEGER).contains(&value)
}

fn validate_positive_safe_integer(value: i64) -> Result<(), ContractViolation> {
    if is_positive_safe_integer(value) {
        Ok(())
    } else {
        Err(ContractViolation::InvalidPositiveSafeInteger)
    }
}

fn validate_nonnegative_safe_integer(value: i64) -> Result<(), ContractViolation> {
    if (0..=MAX_SAFE_INTEGER).contains(&value) {
        Ok(())
    } else {
        Err(ContractViolation::InvalidNonnegativeSafeInteger)
    }
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_uuid(value: &str) -> Result<(), ContractViolation> {
    let bytes = value.as_bytes();
    if bytes.len() != 36
        || [8, 13, 18, 23]
            .into_iter()
            .any(|index| bytes[index] != b'-')
        || bytes.iter().enumerate().any(|(index, byte)| {
            !matches!(index, 8 | 13 | 18 | 23)
                && !byte.is_ascii_digit()
                && !matches!(byte, b'a'..=b'f')
        })
        || bytes
            .iter()
            .enumerate()
            .filter(|(index, _)| !matches!(index, 8 | 13 | 18 | 23))
            .all(|(_, byte)| *byte == b'0')
    {
        return Err(ContractViolation::InvalidUuid);
    }
    Ok(())
}

fn require_nonempty(value: &str) -> Result<(), ContractViolation> {
    if value.is_empty() {
        Err(ContractViolation::EmptyRequiredField)
    } else {
        Ok(())
    }
}

fn validate_optional_opaque(value: Option<&str>) -> Result<(), ContractViolation> {
    if value.is_some_and(str::is_empty) {
        Err(ContractViolation::InvalidOpaqueValue)
    } else {
        Ok(())
    }
}

fn validate_scope_map(scopes: &BTreeMap<String, ScopeCursorRef>) -> Result<(), ContractViolation> {
    for (scope_id, cursor) in scopes {
        require_nonempty(scope_id)?;
        cursor.validate()?;
    }
    Ok(())
}

fn validate_scope_cursor_updates(
    updates: &BTreeMap<String, Option<String>>,
    assignments: &ScopeAssignmentDelta,
) -> Result<(), ContractViolation> {
    let added = assignments
        .add
        .iter()
        .map(|scope| scope.id.as_str())
        .collect::<HashSet<_>>();
    let removed = assignments
        .remove
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    for (scope_id, cursor) in updates {
        require_nonempty(scope_id)?;
        validate_optional_opaque(cursor.as_deref())?;
        if added.contains(scope_id.as_str()) || removed.contains(scope_id.as_str()) {
            return Err(ContractViolation::InvalidScopeAssignment);
        }
    }
    Ok(())
}

fn validate_scope_set(scopes: &[String]) -> Result<HashSet<&str>, ContractViolation> {
    let mut unique = HashSet::with_capacity(scopes.len());
    for scope_id in scopes {
        require_nonempty(scope_id)?;
        if !unique.insert(scope_id.as_str()) {
            return Err(ContractViolation::DuplicateScope);
        }
    }
    Ok(unique)
}

fn validate_sorted_scope_list(scopes: &[String], nonempty: bool) -> Result<(), ContractViolation> {
    if nonempty && scopes.is_empty() {
        return Err(ContractViolation::InvalidAffectedScopes);
    }
    for pair in scopes.windows(2) {
        if pair[0].as_bytes() >= pair[1].as_bytes() {
            return Err(ContractViolation::InvalidAffectedScopes);
        }
    }
    for scope_id in scopes {
        if scope_id.is_empty() {
            return Err(ContractViolation::InvalidAffectedScopes);
        }
    }
    Ok(())
}

fn validate_sorted_field_ids(field_ids: &[String]) -> Result<(), ContractViolation> {
    for field_id in field_ids {
        require_nonempty(field_id)?;
    }
    for pair in field_ids.windows(2) {
        if pair[0].as_bytes() >= pair[1].as_bytes() {
            return Err(ContractViolation::InvalidPushOutcome);
        }
    }
    Ok(())
}

fn validate_one_field_pk(value: &Value) -> Result<(), ContractViolation> {
    let object = value
        .as_object()
        .ok_or(ContractViolation::InvalidPrimaryKey)?;
    if object.len() != 1 {
        return Err(ContractViolation::InvalidPrimaryKey);
    }
    let (field_id, value) = object.iter().next().expect("one primary-key field exists");
    if field_id.is_empty() || value.is_null() {
        return Err(ContractViolation::InvalidPrimaryKey);
    }
    Ok(())
}

fn validate_columns(value: Option<&Value>) -> Result<(), ContractViolation> {
    let object = value
        .and_then(Value::as_object)
        .ok_or(ContractViolation::InvalidColumns)?;
    if object.is_empty()
        || object.len() > MAX_PUSH_COLUMNS
        || object.keys().any(|field_id| field_id.is_empty())
    {
        return Err(ContractViolation::InvalidColumns);
    }
    Ok(())
}

fn validate_row(value: &Value) -> Result<(), ContractViolation> {
    let object = value.as_object().ok_or(ContractViolation::InvalidRow)?;
    if object.is_empty() || object.keys().any(|field_id| field_id.is_empty()) {
        return Err(ContractViolation::InvalidRow);
    }
    Ok(())
}

fn validate_optional_row_checksum_pair(
    row: Option<&Value>,
    checksum: Option<&ChecksumObject>,
) -> Result<(), ContractViolation> {
    match (row, checksum) {
        (Some(row), Some(_)) => validate_row(row),
        (None, None) => Ok(()),
        _ => Err(ContractViolation::InvalidRow),
    }
}

fn is_canonical_utc_microsecond(value: &str) -> bool {
    DateTime::parse_from_rfc3339(value)
        .map(|time| {
            time.with_timezone(&Utc)
                .to_rfc3339_opts(SecondsFormat::Micros, true)
                == value
        })
        .unwrap_or(false)
}

fn is_semver(value: &str) -> bool {
    crate::version::Semver::parse(value).is_ok()
}

fn deserialize_optional_non_null<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)?
        .map(Some)
        .ok_or_else(|| {
            de::Error::custom("optional protocol members must be omitted instead of null")
        })
}

fn deserialize_strict_json_value<'de, D>(deserializer: D) -> Result<Value, D::Error>
where
    D: Deserializer<'de>,
{
    StrictJsonValue::deserialize(deserializer).map(|value| value.0)
}

fn deserialize_optional_strict_json_value<'de, D>(
    deserializer: D,
) -> Result<Option<Value>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<StrictJsonValue>::deserialize(deserializer)?
        .map(|value| Some(value.0))
        .ok_or_else(|| {
            de::Error::custom("optional protocol members must be omitted instead of null")
        })
}

struct StrictJsonValue(Value);

impl<'de> Deserialize<'de> for StrictJsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StrictJsonValueVisitor;

        impl<'de> Visitor<'de> for StrictJsonValueVisitor {
            type Value = StrictJsonValue;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON value with unique object members")
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(StrictJsonValue(Value::Null))
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(StrictJsonValue(Value::Null))
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(StrictJsonValue(Value::Bool(value)))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(StrictJsonValue(Value::Number(Number::from(value))))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(StrictJsonValue(Value::Number(Number::from(value))))
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Number::from_f64(value)
                    .map(|number| StrictJsonValue(Value::Number(number)))
                    .ok_or_else(|| E::custom("JSON number is not finite"))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(StrictJsonValue(Value::String(value.to_owned())))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(StrictJsonValue(Value::String(value)))
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some(value) = sequence.next_element::<StrictJsonValue>()? {
                    values.push(value.0);
                }
                Ok(StrictJsonValue(Value::Array(values)))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut object = Map::new();
                while let Some((key, value)) = map.next_entry::<String, StrictJsonValue>()? {
                    if object.insert(key, value.0).is_some() {
                        return Err(de::Error::custom("duplicate JSON object member"));
                    }
                }
                Ok(StrictJsonValue(Value::Object(object)))
            }
        }

        deserializer.deserialize_any(StrictJsonValueVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HASH_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HASH_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const BATCH_ID: &str = "018f2b5e-7c42-7a1d-9d31-8a95bd674001";
    const MUTATION_ID: &str = "018f2b5e-7c42-7a1d-9d31-8a95bd674011";

    fn schema() -> SchemaRef {
        SchemaRef {
            version: 8,
            hash: HASH_A.into(),
        }
    }

    fn checksum() -> ChecksumObject {
        ChecksumObject::from_json(&format!(
            r#"{{"algorithm":"sha256","version":1,"encoding":"hex","digest":"{HASH_B}"}}"#
        ))
        .unwrap()
    }

    fn server_time() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-07-18T14:00:00.000000Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn mutation(op: Operation) -> Mutation {
        Mutation {
            mutation_id: MUTATION_ID.into(),
            table: "tbl_documents".into(),
            pk: serde_json::json!({ "fld_documents_id": "doc-1" }),
            authored_schema: schema(),
            op,
            base_version: matches!(op, Operation::Update | Operation::Delete)
                .then(|| "opaque-base-version".into()),
            client_version: "2026-07-18T13:59:01.000000Z".into(),
            columns: matches!(op, Operation::Insert | Operation::Update)
                .then(|| serde_json::json!({ "fld_documents_title": "Title" })),
        }
    }

    fn push_request() -> PushRequest {
        PushRequest {
            client_id: "ios-device-123".into(),
            client_generation: 4,
            batch_id: BATCH_ID.into(),
            schema: schema(),
            mutations: vec![mutation(Operation::Insert)],
        }
    }

    fn accepted_mutation() -> AcceptedMutation {
        AcceptedMutation {
            mutation_id: MUTATION_ID.into(),
            table: "tbl_documents".into(),
            pk: serde_json::json!({ "fld_documents_id": "doc-1" }),
            outcome_schema: schema(),
            status: MutationStatus::Applied,
            server_row: Some(serde_json::json!({
                "fld_documents_id": "doc-1",
                "fld_documents_title": "Title"
            })),
            row_checksum: Some(checksum()),
            server_version: "opaque-server-version".into(),
        }
    }

    fn rejected_mutation(status: MutationStatus, code: MutationRejectionCode) -> RejectedMutation {
        RejectedMutation {
            mutation_id: MUTATION_ID.into(),
            table: "tbl_documents".into(),
            pk: serde_json::json!({ "fld_documents_id": "doc-1" }),
            outcome_schema: schema(),
            status,
            code,
            message: "mutation rejected".into(),
            retryable: None,
            server_row: None,
            row_checksum: None,
            server_version: None,
            authored_schema: None,
            current_schema: None,
            incompatible_field_ids: None,
        }
    }

    fn continuing_connect_request() -> ConnectRequest {
        ConnectRequest {
            client_id: "ios-device-123".into(),
            client_generation: Some(4),
            platform: "ios".into(),
            app_version: "3.0.1-beta.2+20260718".into(),
            protocol_version: PROTOCOL_VERSION,
            schema_reset: None,
            schema: schema(),
            scope_set_version: 13,
            known_scopes: BTreeMap::from([(
                "documents_shared".into(),
                ScopeCursorRef {
                    cursor: Some("opaque-cursor".into()),
                },
            )]),
            seed_receipts: Some(BTreeMap::from([(
                "documents_shared".into(),
                "opaque-receipt".into(),
            )])),
        }
    }

    fn connect_response(action: SchemaAction) -> ConnectResponse {
        ConnectResponse {
            server_time: server_time(),
            protocol_version: PROTOCOL_VERSION,
            client_generation: 4,
            scope_set_version: 13,
            schema: SchemaDescriptor {
                version: 8,
                hash: HASH_A.into(),
                action,
                reason: matches!(action, SchemaAction::Unsupported)
                    .then_some(SchemaUnsupportedReason::IncompatibleSchemaTransition),
            },
            scopes: ScopeAssignmentDelta {
                add: vec![],
                remove: vec![],
            },
            scope_cursor_updates: BTreeMap::new(),
            schema_definition: action.requires_schema_definition().then(minimal_manifest),
            affected_scopes: matches!(action, SchemaAction::RebuildLocal)
                .then(|| vec!["documents_shared".into()]),
        }
    }

    fn terminal_pull() -> PullResponse {
        PullResponse {
            changes: vec![],
            scope_set_version: 13,
            scope_cursors: BTreeMap::new(),
            scope_updates: ScopeAssignmentDelta {
                add: vec![],
                remove: vec![],
            },
            rebuild: vec![],
            has_more: false,
            checksums: Some(BTreeMap::new()),
        }
    }

    fn rebuild_record() -> RebuildRecord {
        RebuildRecord {
            table: "tbl_documents".into(),
            pk: serde_json::json!({ "fld_documents_id": "doc-1" }),
            row: serde_json::json!({ "fld_documents_id": "doc-1" }),
            row_checksum: checksum(),
            server_version: "opaque-server-version".into(),
        }
    }

    #[test]
    fn schema_reference_accepts_only_exact_normal_or_fresh_forms() {
        assert!(schema().validate_normal().is_ok());
        assert!(SchemaRef {
            version: 0,
            hash: "".into()
        }
        .validate_connect()
        .is_ok());
        assert!(SchemaRef {
            version: 0,
            hash: HASH_A.into()
        }
        .validate_connect()
        .is_err());
    }

    #[test]
    fn connect_request_validates_fresh_and_continuing_clients() {
        let fresh = ConnectRequest {
            client_id: "ios-device-123".into(),
            client_generation: None,
            platform: "ios".into(),
            app_version: "3.0.1-beta.2+20260718".into(),
            protocol_version: 3,
            schema_reset: None,
            schema: SchemaRef {
                version: 0,
                hash: "".into(),
            },
            scope_set_version: 0,
            known_scopes: BTreeMap::new(),
            seed_receipts: None,
        };
        assert_eq!(fresh.validate(), Ok(()));

        let invalid = ConnectRequest {
            client_generation: Some(1),
            ..fresh
        };
        assert_eq!(
            invalid.validate(),
            Err(ContractViolation::InvalidFreshSchemaReference)
        );
    }

    #[test]
    fn exact_wire_objects_reject_unknown_and_null_optional_members() {
        let unknown = serde_json::json!({
            "client_id": "ios-device-123",
            "client_generation": 4,
            "batch_id": BATCH_ID,
            "schema": { "version": 8, "hash": HASH_A },
            "mutations": [],
            "unexpected": true
        });
        assert!(serde_json::from_value::<PushRequest>(unknown).is_err());

        let null_optional = serde_json::json!({
            "client_id": "ios-device-123",
            "client_generation": 4,
            "platform": "ios",
            "app_version": "3.0.1",
            "protocol_version": 3,
            "schema_reset": null,
            "schema": { "version": 8, "hash": HASH_A },
            "scope_set_version": 0,
            "known_scopes": {}
        });
        assert!(serde_json::from_value::<ConnectRequest>(null_optional).is_err());
    }

    #[test]
    fn push_request_rejects_invalid_uuid_duplicate_identity_and_operation_shapes() {
        let mut request = push_request();
        request.batch_id = "018F2B5E-7C42-7A1D-9D31-8A95BD674001".into();
        assert_eq!(request.validate(), Err(ContractViolation::InvalidUuid));

        let mut request = push_request();
        request.mutations.push(mutation(Operation::Insert));
        assert_eq!(
            request.validate(),
            Err(ContractViolation::DuplicateMutationId)
        );

        let mut update = mutation(Operation::Update);
        update.base_version = None;
        assert_eq!(
            update.validate(),
            Err(ContractViolation::MissingMutationBaseVersion)
        );

        let mut delete = mutation(Operation::Delete);
        delete.columns = Some(serde_json::json!({ "fld_documents_title": "Title" }));
        assert_eq!(
            delete.validate(),
            Err(ContractViolation::UnexpectedMutationColumns)
        );
    }

    #[test]
    fn push_mutation_rejects_noncanonical_client_version_and_invalid_primary_key() {
        let mut value = mutation(Operation::Insert);
        value.client_version = "2026-07-18T13:59:01Z".into();
        assert_eq!(
            value.validate(),
            Err(ContractViolation::InvalidMutationClientVersion)
        );

        let mut value = mutation(Operation::Insert);
        value.pk = serde_json::json!({ "a": "one", "b": "two" });
        assert_eq!(value.validate(), Err(ContractViolation::InvalidPrimaryKey));
    }

    #[test]
    fn push_response_validates_complete_unique_partition_against_request() {
        let request = push_request();
        let response = PushResponse {
            batch_id: BATCH_ID.into(),
            server_time: server_time(),
            accepted: vec![AcceptedMutation {
                mutation_id: MUTATION_ID.into(),
                table: "tbl_documents".into(),
                pk: serde_json::json!({ "fld_documents_id": "doc-1" }),
                outcome_schema: schema(),
                status: MutationStatus::Applied,
                server_row: Some(serde_json::json!({
                    "fld_documents_id": "doc-1",
                    "fld_documents_title": "Title"
                })),
                row_checksum: Some(checksum()),
                server_version: "opaque-server-version".into(),
            }],
            rejected: vec![],
        };
        assert_eq!(response.validate_for_request(&request), Ok(()));

        let incomplete = PushResponse {
            accepted: vec![],
            ..response
        };
        assert_eq!(
            incomplete.validate_for_request(&request),
            Err(ContractViolation::InvalidPushOutcomePartition)
        );
    }

    #[test]
    fn schema_incompatible_outcome_requires_its_exact_fields() {
        let outcome = RejectedMutation {
            mutation_id: MUTATION_ID.into(),
            table: "tbl_documents".into(),
            pk: serde_json::json!({ "fld_documents_id": "doc-1" }),
            outcome_schema: schema(),
            status: MutationStatus::RejectedTerminal,
            code: MutationRejectionCode::SchemaIncompatible,
            message: "authored mutation cannot be represented".into(),
            retryable: Some(false),
            server_row: None,
            row_checksum: None,
            server_version: None,
            authored_schema: Some(SchemaRef {
                version: 7,
                hash: HASH_B.into(),
            }),
            current_schema: Some(schema()),
            incompatible_field_ids: Some(vec!["fld_documents_legacy_summary".into()]),
        };
        assert_eq!(outcome.validate(), Ok(()));
    }

    #[test]
    fn connect_response_enforces_schema_action_fields_and_sorted_scopes() {
        let response = ConnectResponse {
            server_time: server_time(),
            protocol_version: 3,
            client_generation: 4,
            scope_set_version: 13,
            schema: SchemaDescriptor {
                version: 8,
                hash: HASH_A.into(),
                action: SchemaAction::RebuildLocal,
                reason: None,
            },
            scopes: ScopeAssignmentDelta {
                add: vec![],
                remove: vec![],
            },
            scope_cursor_updates: BTreeMap::new(),
            schema_definition: Some(minimal_manifest()),
            affected_scopes: Some(vec!["documents_shared".into()]),
        };
        assert_eq!(response.validate(), Ok(()));

        let missing_affected = ConnectResponse {
            affected_scopes: None,
            ..response
        };
        assert_eq!(
            missing_affected.validate(),
            Err(ContractViolation::InvalidSchemaActionFields)
        );
    }

    #[test]
    fn pull_response_enforces_terminal_checksum_presence() {
        let partial = PullResponse {
            changes: vec![],
            scope_set_version: 1,
            scope_cursors: BTreeMap::new(),
            scope_updates: ScopeAssignmentDelta {
                add: vec![],
                remove: vec![],
            },
            rebuild: vec![],
            has_more: true,
            checksums: Some(BTreeMap::new()),
        };
        assert_eq!(
            partial.validate(),
            Err(ContractViolation::NonterminalPullChecksumsPresent)
        );

        let terminal = PullResponse {
            has_more: false,
            checksums: None,
            ..partial
        };
        assert_eq!(
            terminal.validate(),
            Err(ContractViolation::FinalPullChecksumsMissing)
        );
    }

    #[test]
    fn pull_change_and_rebuild_record_use_structured_checksums() {
        let change: ChangeRecord = serde_json::from_value(serde_json::json!({
            "scope": "documents_shared",
            "table": "tbl_documents",
            "op": "upsert",
            "pk": { "fld_documents_id": "doc-1" },
            "row": { "fld_documents_id": "doc-1" },
            "row_checksum": {
                "algorithm": "sha256",
                "version": 1,
                "encoding": "hex",
                "digest": HASH_B
            },
            "server_version": "opaque-server-version"
        }))
        .unwrap();
        assert_eq!(change.validate(), Ok(()));

        let numeric_checksum = serde_json::json!({
            "table": "tbl_documents",
            "pk": { "fld_documents_id": "doc-1" },
            "row": { "fld_documents_id": "doc-1" },
            "row_checksum": 42,
            "server_version": "opaque-server-version"
        });
        assert!(serde_json::from_value::<RebuildRecord>(numeric_checksum).is_err());
    }

    #[test]
    fn rebuild_response_enforces_intermediate_and_final_presence_matrix() {
        let partial = RebuildResponse {
            scope: "documents_shared".into(),
            records: vec![],
            cursor: Some("opaque-rebuild-continuation".into()),
            has_more: true,
            final_scope_cursor: None,
            checksum: None,
        };
        assert_eq!(partial.validate(), Ok(()));

        let final_page = RebuildResponse {
            cursor: None,
            has_more: false,
            final_scope_cursor: Some("opaque-final-cursor".into()),
            checksum: Some(checksum()),
            ..partial
        };
        assert_eq!(final_page.validate(), Ok(()));
    }

    #[test]
    fn manifest_rejects_noncanonical_field_definitions() {
        let mut manifest = minimal_manifest();
        manifest.tables[0].fields[0].type_name = "integer".into();
        assert_eq!(
            manifest.validate(),
            Err(ContractViolation::InvalidSchemaManifest)
        );
    }

    #[test]
    fn manifest_rejects_writable_primary_key_and_lifecycle_fields() {
        let mut manifest = minimal_manifest();
        manifest.tables[0].fields[0].writable = true;
        assert_eq!(
            manifest.validate(),
            Err(ContractViolation::InvalidSchemaManifest)
        );

        let mut manifest = minimal_manifest();
        manifest.tables[0].fields.push(ColumnSchema {
            field_id: "fld_documents_updated_at".into(),
            name: "updated_at".into(),
            type_name: "datetime".into(),
            nullable: false,
            writable: false,
            precision: None,
            scale: None,
        });
        manifest.tables[0].lifecycle.updated_at_field_id = Some("fld_documents_updated_at".into());
        assert_eq!(manifest.validate(), Ok(()));

        let mut manifest = minimal_manifest();
        manifest.tables[0].fields.push(ColumnSchema {
            field_id: "fld_documents_updated_at".into(),
            name: "updated_at".into(),
            type_name: "string".into(),
            nullable: false,
            writable: false,
            precision: None,
            scale: None,
        });
        manifest.tables[0].lifecycle.updated_at_field_id = Some("fld_documents_updated_at".into());
        assert_eq!(
            manifest.validate(),
            Err(ContractViolation::InvalidSchemaManifest)
        );

        manifest.tables[0].fields[1].type_name = "datetime".into();
        manifest.tables[0].fields[1].writable = true;
        assert_eq!(
            manifest.validate(),
            Err(ContractViolation::InvalidSchemaManifest)
        );
    }

    #[test]
    fn manifest_decodes_required_nullable_lifecycle_fields() {
        let manifest = minimal_manifest();
        let encoded = serde_json::to_value(manifest).unwrap();
        assert_eq!(
            encoded["tables"][0]["lifecycle"]["created_at_field_id"],
            Value::Null
        );
        let decoded: SchemaManifest = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded.validate(), Ok(()));
    }

    #[test]
    fn error_body_accepts_complete_current_error_codes() {
        let error = ErrorResponse {
            error: ErrorBody {
                code: ProtocolErrorCode::SchemaMismatch,
                message: "schema is not current".into(),
                retryable: false,
                current_schema: Some(schema()),
                received_schema: Some(SchemaRef {
                    version: 7,
                    hash: HASH_B.into(),
                }),
                current_client_generation: None,
                scope_id: None,
                required_protocol_version: None,
                received_protocol_version: None,
                minimum_client_version: None,
                received_client_version: None,
                reason: None,
                field: None,
                minimum: None,
                maximum: None,
            },
        };
        assert_eq!(error.validate(), Ok(()));
    }

    #[test]
    fn strict_dynamic_values_reject_duplicate_members() {
        let duplicate_pk = format!(
            r#"{{"mutation_id":"{MUTATION_ID}","table":"tbl_documents","pk":{{"fld_documents_id":"one","fld_documents_id":"two"}},"authored_schema":{{"version":8,"hash":"{HASH_A}"}},"op":"insert","client_version":"2026-07-18T13:59:01.000000Z","columns":{{"fld_documents_title":"Title"}}}}"#
        );
        assert!(serde_json::from_str::<Mutation>(&duplicate_pk).is_err());
    }

    #[test]
    fn change_operation_conversion_remains_explicit() {
        for (wire, operation) in [
            (crate::change::ChangeOperation::Insert, Operation::Insert),
            (crate::change::ChangeOperation::Update, Operation::Update),
            (crate::change::ChangeOperation::Delete, Operation::Delete),
        ] {
            assert_eq!(Operation::from(wire), operation);
            assert_eq!(
                crate::change::ChangeOperation::try_from(operation),
                Ok(wire)
            );
        }
        assert_eq!(
            crate::change::ChangeOperation::try_from(Operation::Upsert),
            Err(ChangeOperationConversionError {
                operation: Operation::Upsert,
            })
        );
    }

    #[test]
    fn schema_action_and_rejection_classifications_are_exact() {
        for (action, definition, rebuild, compatible) in [
            (SchemaAction::None, false, false, true),
            (SchemaAction::Replace, true, false, true),
            (SchemaAction::RebuildLocal, true, true, true),
            (SchemaAction::Unsupported, false, false, false),
        ] {
            assert_eq!(action.requires_schema_definition(), definition);
            assert_eq!(action.requires_local_rebuild(), rebuild);
            assert_eq!(action.is_compatible(), compatible);
        }

        for code in [
            MutationRejectionCode::VersionConflict,
            MutationRejectionCode::RowAlreadyExists,
            MutationRejectionCode::RowDeleted,
            MutationRejectionCode::RowNotFound,
        ] {
            assert!(code.is_conflict());
            assert!(!code.is_terminal());
        }
        for code in [
            MutationRejectionCode::SchemaIncompatible,
            MutationRejectionCode::TableNotSynced,
            MutationRejectionCode::PolicyRejected,
            MutationRejectionCode::ValidationFailed,
        ] {
            assert!(!code.is_conflict());
            assert!(code.is_terminal());
        }
    }

    #[test]
    fn scope_assignment_validation_rejects_each_invalid_shape() {
        assert!(ScopeCursorRef { cursor: None }.validate().is_ok());
        assert!(ScopeCursorRef {
            cursor: Some("opaque".into())
        }
        .validate()
        .is_ok());
        assert!(ScopeCursorRef {
            cursor: Some(String::new())
        }
        .validate()
        .is_err());

        let valid = ScopeAssignment {
            id: "scope-a".into(),
            cursor: None,
        };
        assert!(valid.validate().is_ok());
        assert!(ScopeAssignment {
            id: String::new(),
            ..valid.clone()
        }
        .validate()
        .is_err());
        assert!(ScopeAssignment {
            cursor: Some(String::new()),
            ..valid.clone()
        }
        .validate()
        .is_err());

        let delta = ScopeAssignmentDelta {
            add: vec![valid.clone()],
            remove: vec!["scope-b".into()],
        };
        assert_eq!(delta.validate(), Ok(()));
        for invalid in [
            ScopeAssignmentDelta {
                add: vec![valid.clone(), valid.clone()],
                remove: vec![],
            },
            ScopeAssignmentDelta {
                add: vec![],
                remove: vec!["scope-a".into(), "scope-a".into()],
            },
            ScopeAssignmentDelta {
                add: vec![valid.clone()],
                remove: vec!["scope-a".into()],
            },
            ScopeAssignmentDelta {
                add: vec![ScopeAssignment {
                    id: String::new(),
                    cursor: None,
                }],
                remove: vec![],
            },
            ScopeAssignmentDelta {
                add: vec![],
                remove: vec![String::new()],
            },
        ] {
            assert!(invalid.validate().is_err());
        }
    }

    #[test]
    fn portable_type_normalization_covers_every_supported_family() {
        for (input, expected) in [
            ("text", "string"),
            ("UUID", "string"),
            ("varchar(20)", "string"),
            ("character varying(20)", "string"),
            ("int32", "int"),
            ("integer", "int"),
            ("bigint", "int64"),
            ("numeric", "decimal"),
            ("decimal(10,2)", "decimal"),
            ("double precision", "float"),
            ("bool", "boolean"),
            ("timestamp with time zone", "datetime"),
            ("date", "date"),
            ("time without time zone", "time"),
            ("jsonb", "json"),
            ("bytea", "bytes"),
            ("text[]", "json"),
            ("int4range", "string"),
        ] {
            assert_eq!(
                normalize_portable_type_name(input),
                Some(expected),
                "{input}"
            );
        }
        assert_eq!(normalize_portable_type_name("unknown"), None);
        for canonical in [
            "string", "int", "int64", "decimal", "float", "boolean", "datetime", "date", "time",
            "json", "bytes",
        ] {
            assert!(is_canonical_portable_type_name(canonical));
        }
        for noncanonical in ["text", "integer", "bool", "jsonb", "unknown", ""] {
            assert!(!is_canonical_portable_type_name(noncanonical));
        }
    }

    #[test]
    fn manifest_validation_covers_lineage_fields_lifecycle_and_indexes() {
        let base = minimal_manifest();
        assert_eq!(base.validate(), Ok(()));

        let mut initial = base.clone();
        initial.parent_schema = None;
        initial.transition_class = SchemaTransitionClass::Initial;
        assert_eq!(initial.validate(), Ok(()));

        let mut class_two = base.clone();
        class_two.transition_class = SchemaTransitionClass::Class2;
        class_two.compatibility_floor = 7;
        assert_eq!(class_two.validate(), Ok(()));

        let mut invalid_manifests = Vec::new();
        let mut value = base.clone();
        value.parent_schema = None;
        invalid_manifests.push(value);
        let mut value = initial.clone();
        value.parent_schema = Some(schema());
        invalid_manifests.push(value);
        let mut value = base.clone();
        value.parent_schema.as_mut().unwrap().version = value.schema_version;
        invalid_manifests.push(value);
        for floor in [0, 7, 9] {
            let mut value = base.clone();
            value.compatibility_floor = floor;
            invalid_manifests.push(value);
        }

        for field in ["table_id", "relation_id", "name", "primary_key_field_id"] {
            let mut value = base.clone();
            match field {
                "table_id" => value.tables[0].table_id.clear(),
                "relation_id" => value.tables[0].relation_id.clear(),
                "name" => value.tables[0].name.clear(),
                _ => value.tables[0].primary_key_field_id.clear(),
            }
            invalid_manifests.push(value);
        }

        for duplicate in ["table_id", "relation_id", "name"] {
            let mut value = base.clone();
            let mut table = value.tables[0].clone();
            table.table_id = "tbl_other".into();
            table.relation_id = "rel_other".into();
            table.name = "other".into();
            match duplicate {
                "table_id" => table.table_id = value.tables[0].table_id.clone(),
                "relation_id" => table.relation_id = value.tables[0].relation_id.clone(),
                _ => table.name = value.tables[0].name.clone(),
            }
            value.tables.push(table);
            invalid_manifests.push(value);
        }

        for field in ["field_id", "name", "type_name"] {
            let mut value = base.clone();
            match field {
                "field_id" => value.tables[0].fields[0].field_id.clear(),
                "name" => value.tables[0].fields[0].name.clear(),
                _ => value.tables[0].fields[0].type_name = "text".into(),
            }
            invalid_manifests.push(value);
        }

        for duplicate in ["field_id", "name"] {
            let mut value = base.clone();
            let mut field = value.tables[0].fields[0].clone();
            field.field_id = "fld_other".into();
            field.name = "other".into();
            if duplicate == "field_id" {
                field.field_id = value.tables[0].fields[0].field_id.clone();
            } else {
                field.name = value.tables[0].fields[0].name.clone();
            }
            value.tables[0].fields.push(field);
            invalid_manifests.push(value);
        }

        let mut decimal = base.clone();
        decimal.tables[0].fields.push(ColumnSchema {
            field_id: "fld_amount".into(),
            name: "amount".into(),
            type_name: "decimal".into(),
            nullable: false,
            writable: true,
            precision: Some(5),
            scale: Some(2),
        });
        assert_eq!(decimal.validate(), Ok(()));
        for (precision, scale) in [
            (None, None),
            (Some(0), Some(0)),
            (Some(5), Some(-1)),
            (Some(5), Some(6)),
        ] {
            let mut value = decimal.clone();
            value.tables[0].fields[1].precision = precision;
            value.tables[0].fields[1].scale = scale;
            invalid_manifests.push(value);
        }
        let mut value = base.clone();
        value.tables[0].fields[0].precision = Some(1);
        invalid_manifests.push(value);
        let mut value = base.clone();
        value.tables[0].fields[0].nullable = true;
        invalid_manifests.push(value);
        let mut value = base.clone();
        value.tables[0].fields[0].type_name = "boolean".into();
        invalid_manifests.push(value);

        for lifecycle in ["empty", "unknown"] {
            let mut value = base.clone();
            value.tables[0].lifecycle.created_at_field_id = Some(if lifecycle == "empty" {
                String::new()
            } else {
                "fld_unknown".into()
            });
            invalid_manifests.push(value);
        }

        let mut indexed = base.clone();
        indexed.tables[0].indexes.push(IndexSchema {
            index_id: "idx_documents_id".into(),
            name: "documents_id_idx".into(),
            field_ids: vec!["fld_documents_id".into()],
            unique: true,
        });
        assert_eq!(indexed.validate(), Ok(()));
        for kind in [
            "empty",
            "unknown",
            "duplicate-field",
            "duplicate-id",
            "duplicate-name",
        ] {
            let mut value = indexed.clone();
            match kind {
                "empty" => value.tables[0].indexes[0].field_ids.clear(),
                "unknown" => value.tables[0].indexes[0].field_ids = vec!["fld_unknown".into()],
                "duplicate-field" => value.tables[0].indexes[0]
                    .field_ids
                    .push("fld_documents_id".into()),
                "duplicate-id" => {
                    let index = value.tables[0].indexes[0].clone();
                    value.tables[0].indexes.push(index);
                }
                _ => {
                    let mut index = value.tables[0].indexes[0].clone();
                    index.index_id = "idx_other".into();
                    value.tables[0].indexes.push(index);
                }
            }
            invalid_manifests.push(value);
        }

        for manifest in invalid_manifests {
            assert!(manifest.validate().is_err(), "{manifest:?}");
        }
    }

    #[test]
    fn accepted_and_rejected_outcomes_enforce_complete_shapes() {
        let accepted = accepted_mutation();
        assert_eq!(accepted.validate(), Ok(()));
        for invalid in [
            AcceptedMutation {
                status: MutationStatus::Conflict,
                ..accepted.clone()
            },
            AcceptedMutation {
                server_version: String::new(),
                ..accepted.clone()
            },
            AcceptedMutation {
                row_checksum: None,
                ..accepted.clone()
            },
            AcceptedMutation {
                server_row: None,
                ..accepted.clone()
            },
        ] {
            assert!(invalid.validate().is_err());
        }

        for code in [
            MutationRejectionCode::VersionConflict,
            MutationRejectionCode::RowAlreadyExists,
            MutationRejectionCode::RowDeleted,
            MutationRejectionCode::RowNotFound,
        ] {
            assert_eq!(
                rejected_mutation(MutationStatus::Conflict, code).validate(),
                Ok(())
            );
        }
        for code in [
            MutationRejectionCode::TableNotSynced,
            MutationRejectionCode::PolicyRejected,
            MutationRejectionCode::ValidationFailed,
        ] {
            assert_eq!(
                rejected_mutation(MutationStatus::RejectedTerminal, code).validate(),
                Ok(())
            );
        }
        assert!(rejected_mutation(
            MutationStatus::Conflict,
            MutationRejectionCode::ValidationFailed
        )
        .validate()
        .is_err());
        assert!(rejected_mutation(
            MutationStatus::RejectedTerminal,
            MutationRejectionCode::VersionConflict
        )
        .validate()
        .is_err());

        let mut conflict = rejected_mutation(
            MutationStatus::Conflict,
            MutationRejectionCode::VersionConflict,
        );
        conflict.server_row = accepted.server_row.clone();
        conflict.row_checksum = accepted.row_checksum;
        conflict.server_version = Some("opaque-server-version".into());
        assert_eq!(conflict.validate(), Ok(()));
        for kind in [
            "retryable",
            "authored",
            "current",
            "fields",
            "checksum",
            "version",
        ] {
            let mut value = conflict.clone();
            match kind {
                "retryable" => value.retryable = Some(false),
                "authored" => value.authored_schema = Some(schema()),
                "current" => value.current_schema = Some(schema()),
                "fields" => value.incompatible_field_ids = Some(vec!["fld_old".into()]),
                "checksum" => value.row_checksum = None,
                _ => value.server_version = None,
            }
            assert!(value.validate().is_err(), "{kind}");
        }
    }

    #[test]
    fn schema_and_connect_boundaries_are_independently_enforced() {
        for descriptor in [
            SchemaDescriptor {
                version: 0,
                hash: HASH_A.into(),
                action: SchemaAction::None,
                reason: None,
            },
            SchemaDescriptor {
                version: 8,
                hash: HASH_A.into(),
                action: SchemaAction::Unsupported,
                reason: None,
            },
            SchemaDescriptor {
                version: 8,
                hash: HASH_A.into(),
                action: SchemaAction::None,
                reason: Some(SchemaUnsupportedReason::UnknownSchemaLineage),
            },
        ] {
            assert!(descriptor.validate().is_err());
        }

        let fresh = ConnectRequest {
            client_id: "ios-device-123".into(),
            client_generation: None,
            platform: "ios".into(),
            app_version: "3.0.1".into(),
            protocol_version: PROTOCOL_VERSION,
            schema_reset: None,
            schema: SchemaRef {
                version: 0,
                hash: String::new(),
            },
            scope_set_version: 0,
            known_scopes: BTreeMap::new(),
            seed_receipts: None,
        };
        assert_eq!(fresh.validate(), Ok(()));
        let mut fresh_invalid = Vec::new();
        let mut value = fresh.clone();
        value.client_generation = Some(1);
        fresh_invalid.push(value);
        let mut value = fresh.clone();
        value.schema_reset = Some(true);
        fresh_invalid.push(value);
        let mut value = fresh.clone();
        value.scope_set_version = 1;
        fresh_invalid.push(value);
        let mut value = fresh.clone();
        value.known_scopes.insert(
            "scope-a".into(),
            ScopeCursorRef {
                cursor: Some("opaque".into()),
            },
        );
        fresh_invalid.push(value);
        for value in fresh_invalid {
            assert_eq!(
                value.validate(),
                Err(ContractViolation::InvalidFreshSchemaReference)
            );
        }

        let continuing = continuing_connect_request();
        assert_eq!(continuing.validate(), Ok(()));
        let mut reset_with_generation = continuing.clone();
        reset_with_generation.schema_reset = Some(true);
        assert_eq!(reset_with_generation.validate(), Ok(()));
        let mut no_generation_without_reset = continuing.clone();
        no_generation_without_reset.client_generation = None;
        assert_eq!(no_generation_without_reset.validate(), Ok(()));
        let mut reset_without_generation = continuing.clone();
        reset_without_generation.schema_reset = Some(true);
        reset_without_generation.client_generation = None;
        assert_eq!(
            reset_without_generation.validate(),
            Err(ContractViolation::InvalidFreshSchemaReference)
        );
        let mut invalid_cursor = continuing.clone();
        invalid_cursor
            .known_scopes
            .get_mut("documents_shared")
            .unwrap()
            .cursor = Some(String::new());
        assert!(invalid_cursor.validate().is_err());
        let mut empty_receipts = continuing.clone();
        empty_receipts.seed_receipts = Some(BTreeMap::new());
        assert!(empty_receipts.validate().is_err());
        let mut empty_receipt = continuing;
        empty_receipt.seed_receipts = Some(BTreeMap::from([("scope-a".into(), String::new())]));
        assert!(empty_receipt.validate().is_err());
    }

    #[test]
    fn connect_response_enforces_definition_identity_and_affected_scopes() {
        for action in [
            SchemaAction::None,
            SchemaAction::Replace,
            SchemaAction::RebuildLocal,
            SchemaAction::Unsupported,
        ] {
            assert_eq!(connect_response(action).validate(), Ok(()), "{action:?}");
        }

        let mut mismatched_version = connect_response(SchemaAction::Replace);
        mismatched_version
            .schema_definition
            .as_mut()
            .unwrap()
            .schema_version = 9;
        assert!(mismatched_version.validate().is_err());
        let mut mismatched_hash = connect_response(SchemaAction::Replace);
        mismatched_hash
            .schema_definition
            .as_mut()
            .unwrap()
            .schema_hash = HASH_B.into();
        assert!(mismatched_hash.validate().is_err());

        let mut affected_on_none = connect_response(SchemaAction::None);
        affected_on_none.affected_scopes = Some(vec!["documents_shared".into()]);
        assert!(affected_on_none.validate().is_err());
        let mut missing_affected = connect_response(SchemaAction::RebuildLocal);
        missing_affected.affected_scopes = None;
        assert!(missing_affected.validate().is_err());

        let mut class_two_floor_above_version = minimal_manifest();
        class_two_floor_above_version.transition_class = SchemaTransitionClass::Class2;
        class_two_floor_above_version.compatibility_floor = 9;
        assert!(class_two_floor_above_version.validate().is_err());
    }

    #[test]
    fn terminal_rejections_forbid_all_server_and_schema_extras() {
        let base = rejected_mutation(
            MutationStatus::RejectedTerminal,
            MutationRejectionCode::PolicyRejected,
        );
        for kind in [
            "row",
            "checksum",
            "version",
            "retryable",
            "authored",
            "current",
            "fields",
        ] {
            let mut value = base.clone();
            match kind {
                "row" => value.server_row = Some(serde_json::json!({"id": "doc-1"})),
                "checksum" => value.row_checksum = Some(checksum()),
                "version" => value.server_version = Some("opaque".into()),
                "retryable" => value.retryable = Some(false),
                "authored" => value.authored_schema = Some(schema()),
                "current" => value.current_schema = Some(schema()),
                _ => value.incompatible_field_ids = Some(vec!["fld_old".into()]),
            }
            assert!(value.validate().is_err(), "{kind}");
        }
    }

    #[test]
    fn push_response_checks_each_outcome_and_partition_dimension() {
        let request = push_request();
        let response = PushResponse {
            batch_id: BATCH_ID.into(),
            server_time: server_time(),
            accepted: vec![accepted_mutation()],
            rejected: vec![],
        };
        assert_eq!(response.validate(), Ok(()));
        assert_eq!(response.validate_for_request(&request), Ok(()));

        let mut invalid_accepted = response.clone();
        invalid_accepted.accepted[0].status = MutationStatus::Conflict;
        assert!(invalid_accepted.validate().is_err());
        let mut duplicate = response.clone();
        duplicate.accepted.push(accepted_mutation());
        assert!(duplicate.validate().is_err());
        let mut duplicate_rejected = PushResponse {
            accepted: vec![],
            rejected: vec![rejected_mutation(
                MutationStatus::Conflict,
                MutationRejectionCode::VersionConflict,
            )],
            ..response.clone()
        };
        duplicate_rejected
            .rejected
            .push(duplicate_rejected.rejected[0].clone());
        assert!(duplicate_rejected.validate().is_err());

        for kind in [
            "accepted-table",
            "accepted-pk",
            "accepted-row",
            "accepted-checksum",
        ] {
            let mut value = response.clone();
            match kind {
                "accepted-table" => value.accepted[0].table = "tbl_other".into(),
                "accepted-pk" => {
                    value.accepted[0].pk = serde_json::json!({"fld_documents_id": "other"})
                }
                "accepted-row" => value.accepted[0].server_row = None,
                _ => value.accepted[0].row_checksum = None,
            }
            assert!(value.validate_for_request(&request).is_err(), "{kind}");
        }

        let mut rejected_request = push_request();
        rejected_request.mutations[0].op = Operation::Delete;
        rejected_request.mutations[0].base_version = Some("opaque".into());
        rejected_request.mutations[0].columns = None;
        let rejected_response = PushResponse {
            accepted: vec![],
            rejected: vec![rejected_mutation(
                MutationStatus::Conflict,
                MutationRejectionCode::VersionConflict,
            )],
            ..response.clone()
        };
        assert_eq!(
            rejected_response.validate_for_request(&rejected_request),
            Ok(())
        );
        for kind in ["rejected-table", "rejected-pk"] {
            let mut value = rejected_response.clone();
            if kind == "rejected-table" {
                value.rejected[0].table = "tbl_other".into();
            } else {
                value.rejected[0].pk = serde_json::json!({"fld_documents_id": "other"});
            }
            assert!(value.validate_for_request(&rejected_request).is_err());
        }

        let mut two_request = push_request();
        let mut second = mutation(Operation::Delete);
        second.mutation_id = "018f2b5e-7c42-7a1d-9d31-8a95bd674012".into();
        two_request.mutations.push(second.clone());
        let mut second_rejection = rejected_mutation(
            MutationStatus::Conflict,
            MutationRejectionCode::VersionConflict,
        );
        second_rejection.mutation_id = second.mutation_id;
        let mixed = PushResponse {
            accepted: vec![accepted_mutation()],
            rejected: vec![second_rejection],
            ..response
        };
        assert_eq!(mixed.validate_for_request(&two_request), Ok(()));

        let mut two_accepted_request = push_request();
        let mut second_insert = mutation(Operation::Insert);
        second_insert.mutation_id = "018f2b5e-7c42-7a1d-9d31-8a95bd674013".into();
        two_accepted_request.mutations.push(second_insert.clone());
        let mut second_accepted = accepted_mutation();
        second_accepted.mutation_id = second_insert.mutation_id;
        let two_accepted = PushResponse {
            batch_id: BATCH_ID.into(),
            server_time: server_time(),
            accepted: vec![accepted_mutation(), second_accepted],
            rejected: vec![],
        };
        assert_eq!(
            two_accepted.validate_for_request(&two_accepted_request),
            Ok(())
        );
        let mut reversed_request = two_accepted_request;
        reversed_request.mutations.reverse();
        assert!(two_accepted
            .validate_for_request(&reversed_request)
            .is_err());
    }

    #[test]
    fn pull_and_rebuild_requests_and_records_reject_each_invalid_boundary() {
        let pull = PullRequest {
            client_id: "ios-device-123".into(),
            client_generation: 4,
            schema: schema(),
            scope_set_version: 13,
            scopes: BTreeMap::from([(
                "documents_shared".into(),
                ScopeCursorRef {
                    cursor: Some("opaque".into()),
                },
            )]),
            limit: 100,
        };
        assert_eq!(pull.validate(), Ok(()));
        for kind in [
            "client",
            "generation",
            "schema",
            "scope-version",
            "scope",
            "limit",
        ] {
            let mut value = pull.clone();
            match kind {
                "client" => value.client_id.clear(),
                "generation" => value.client_generation = 0,
                "schema" => value.schema.version = 0,
                "scope-version" => value.scope_set_version = -1,
                "scope" => {
                    value.scopes.get_mut("documents_shared").unwrap().cursor = Some(String::new())
                }
                _ => value.limit = 0,
            }
            assert!(value.validate().is_err(), "{kind}");
        }

        let change = ChangeRecord {
            scope: "documents_shared".into(),
            table: "tbl_documents".into(),
            op: Operation::Delete,
            pk: serde_json::json!({"fld_documents_id": "doc-1"}),
            row: None,
            row_checksum: None,
            server_version: "opaque".into(),
        };
        assert_eq!(change.validate(), Ok(()));
        let mut invalid_change = change;
        invalid_change.scope.clear();
        assert!(invalid_change.validate().is_err());

        let request = RebuildRequest {
            client_id: "ios-device-123".into(),
            client_generation: 4,
            schema: schema(),
            scope: "documents_shared".into(),
            rebuild_id: BATCH_ID.into(),
            cursor: Some("opaque".into()),
            limit: 100,
        };
        assert_eq!(request.validate(), Ok(()));
        for kind in [
            "client",
            "generation",
            "schema",
            "scope",
            "id",
            "cursor",
            "limit",
        ] {
            let mut value = request.clone();
            match kind {
                "client" => value.client_id.clear(),
                "generation" => value.client_generation = 0,
                "schema" => value.schema.hash.clear(),
                "scope" => value.scope.clear(),
                "id" => value.rebuild_id = "bad".into(),
                "cursor" => value.cursor = Some(String::new()),
                _ => value.limit = 0,
            }
            assert!(value.validate().is_err(), "{kind}");
        }

        let record = rebuild_record();
        assert_eq!(record.validate(), Ok(()));
        for kind in ["table", "pk", "row", "version"] {
            let mut value = record.clone();
            match kind {
                "table" => value.table.clear(),
                "pk" => value.pk = serde_json::json!({}),
                "row" => value.row = Value::Null,
                _ => value.server_version.clear(),
            }
            assert!(value.validate().is_err(), "{kind}");
        }
    }

    #[test]
    fn pull_scope_matrix_and_rebuild_page_matrix_are_exact() {
        let mut pull = terminal_pull();
        assert!(!pull.requests_rebuild());
        pull.rebuild.push("scope-rebuild".into());
        pull.checksums
            .as_mut()
            .unwrap()
            .insert("scope-rebuild".into(), checksum());
        assert!(pull.requests_rebuild());
        assert_eq!(pull.validate(), Ok(()));

        let mut invalid = terminal_pull();
        invalid.scope_updates.add.push(ScopeAssignment {
            id: "scope-a".into(),
            cursor: Some("not-allowed".into()),
        });
        invalid.rebuild.push("scope-a".into());
        invalid
            .checksums
            .as_mut()
            .unwrap()
            .insert("scope-a".into(), checksum());
        assert!(invalid.validate().is_err());

        let mut invalid = terminal_pull();
        invalid.scope_updates.remove.push("scope-a".into());
        invalid.rebuild.push("scope-a".into());
        invalid
            .checksums
            .as_mut()
            .unwrap()
            .insert("scope-a".into(), checksum());
        assert!(invalid.validate().is_err());

        let mut invalid = terminal_pull();
        invalid.scope_updates.remove.push("scope-a".into());
        invalid
            .scope_cursors
            .insert("scope-a".into(), "opaque".into());
        assert!(invalid.validate().is_err());
        let mut invalid = terminal_pull();
        invalid.rebuild.push("scope-a".into());
        invalid
            .scope_cursors
            .insert("scope-a".into(), "opaque".into());
        invalid
            .checksums
            .as_mut()
            .unwrap()
            .insert("scope-a".into(), checksum());
        assert!(invalid.validate().is_err());

        let change = ChangeRecord {
            scope: "scope-a".into(),
            table: "tbl_documents".into(),
            op: Operation::Delete,
            pk: serde_json::json!({"fld_documents_id": "doc-1"}),
            row: None,
            row_checksum: None,
            server_version: "opaque".into(),
        };
        let mut invalid = terminal_pull();
        invalid.scope_updates.remove.push("scope-a".into());
        invalid.changes.push(change.clone());
        assert!(invalid.validate().is_err());
        let mut invalid = terminal_pull();
        invalid.rebuild.push("scope-a".into());
        invalid.changes.push(change);
        invalid
            .checksums
            .as_mut()
            .unwrap()
            .insert("scope-a".into(), checksum());
        assert!(invalid.validate().is_err());

        let mut invalid = terminal_pull();
        invalid.scope_updates.add.push(ScopeAssignment {
            id: "scope-a".into(),
            cursor: None,
        });
        invalid
            .checksums
            .as_mut()
            .unwrap()
            .insert("scope-a".into(), checksum());
        assert!(invalid.validate().is_err());

        let active = HashSet::from(["scope-a".to_owned()]);
        let mut exact = terminal_pull();
        exact
            .checksums
            .as_mut()
            .unwrap()
            .insert("scope-a".into(), checksum());
        assert_eq!(exact.validate_for_active_scopes(&active), Ok(()));
        let mut missing = exact.clone();
        missing.checksums.as_mut().unwrap().clear();
        assert!(missing.validate_for_active_scopes(&active).is_err());
        let mut wrong = exact;
        wrong.checksums.as_mut().unwrap().remove("scope-a");
        wrong
            .checksums
            .as_mut()
            .unwrap()
            .insert("scope-b".into(), checksum());
        assert!(wrong.validate_for_active_scopes(&active).is_err());

        let partial = RebuildResponse {
            scope: "documents_shared".into(),
            records: vec![],
            cursor: Some("opaque".into()),
            has_more: true,
            final_scope_cursor: None,
            checksum: None,
        };
        assert!(!partial.is_final_page());
        assert_eq!(partial.validate(), Ok(()));
        let final_page = RebuildResponse {
            cursor: None,
            has_more: false,
            final_scope_cursor: Some("final".into()),
            checksum: Some(checksum()),
            ..partial.clone()
        };
        assert!(final_page.is_final_page());
        assert_eq!(final_page.validate(), Ok(()));
        let nonfinal = RebuildResponse {
            final_scope_cursor: None,
            checksum: None,
            ..final_page.clone()
        };
        assert!(!nonfinal.is_final_page());

        for kind in [
            "scope",
            "partial-cursor",
            "partial-final",
            "partial-checksum",
            "final-cursor",
            "final-continuation",
            "final-checksum",
            "record",
        ] {
            let mut value = if kind.starts_with("partial") {
                partial.clone()
            } else {
                final_page.clone()
            };
            match kind {
                "scope" => value.scope.clear(),
                "partial-cursor" => value.cursor = None,
                "partial-final" => value.final_scope_cursor = Some("final".into()),
                "partial-checksum" => value.checksum = Some(checksum()),
                "final-cursor" => value.final_scope_cursor = None,
                "final-continuation" => value.cursor = Some("opaque".into()),
                "final-checksum" => value.checksum = None,
                _ => {
                    let mut record = rebuild_record();
                    record.table.clear();
                    value.records.push(record);
                }
            }
            assert!(value.validate().is_err(), "{kind}");
        }
    }

    #[test]
    fn primitive_contract_validators_cover_boundaries() {
        assert!(validate_positive_safe_integer(1).is_ok());
        assert!(validate_positive_safe_integer(MAX_SAFE_INTEGER).is_ok());
        assert!(validate_positive_safe_integer(0).is_err());
        assert!(validate_positive_safe_integer(MAX_SAFE_INTEGER + 1).is_err());
        assert!(validate_nonnegative_safe_integer(0).is_ok());
        assert!(validate_nonnegative_safe_integer(-1).is_err());

        assert!(is_lower_sha256(HASH_A));
        assert!(!is_lower_sha256(&HASH_A.to_uppercase()));
        assert!(!is_lower_sha256(&HASH_A[..63]));
        assert!(!is_lower_sha256(&format!("{}g", &HASH_A[..63])));

        for valid in [BATCH_ID, "00000000-0000-4000-8000-000000000000"] {
            assert!(validate_uuid(valid).is_ok());
        }
        for invalid in [
            "",
            "018f2b5e7c427a1d9d318a95bd674001",
            "018F2B5E-7C42-7A1D-9D31-8A95BD674001",
            "018f2b5e_7c42-7a1d-9d31-8a95bd674001",
            "g18f2b5e-7c42-7a1d-9d31-8a95bd674001",
            "00000000-0000-0000-0000-000000000000",
        ] {
            assert!(validate_uuid(invalid).is_err(), "{invalid}");
        }

        let assignments = ScopeAssignmentDelta {
            add: vec![ScopeAssignment {
                id: "scope-added".into(),
                cursor: None,
            }],
            remove: vec!["scope-removed".into()],
        };
        assert!(validate_scope_cursor_updates(
            &BTreeMap::from([("scope-stable".into(), Some("opaque".into()))]),
            &assignments,
        )
        .is_ok());
        for updates in [
            BTreeMap::from([("scope-added".into(), Some("opaque".into()))]),
            BTreeMap::from([("scope-removed".into(), Some("opaque".into()))]),
            BTreeMap::from([(String::new(), Some("opaque".into()))]),
            BTreeMap::from([("scope-stable".into(), Some(String::new()))]),
        ] {
            assert!(validate_scope_cursor_updates(&updates, &assignments).is_err());
        }

        assert!(validate_scope_set(&["a".into(), "b".into()]).is_ok());
        assert!(validate_scope_set(&[String::new()]).is_err());
        assert!(validate_scope_set(&["a".into(), "a".into()]).is_err());
        assert!(validate_sorted_scope_list(&["a".into(), "b".into()], true).is_ok());
        assert!(validate_sorted_scope_list(&[], true).is_err());
        assert!(validate_sorted_scope_list(&["b".into(), "a".into()], false).is_err());
        assert!(validate_sorted_scope_list(&["a".into(), "a".into()], false).is_err());
        assert!(validate_sorted_field_ids(&["a".into(), "b".into()]).is_ok());
        assert!(validate_sorted_field_ids(&[]).is_ok());
        assert!(validate_sorted_field_ids(&["b".into(), "a".into()]).is_err());
        assert!(validate_sorted_field_ids(&["a".into(), "a".into()]).is_err());

        assert!(validate_one_field_pk(&serde_json::json!({"id": 1})).is_ok());
        for invalid in [
            Value::Null,
            serde_json::json!({}),
            serde_json::json!({"": 1}),
            serde_json::json!({"a": 1, "b": 2}),
        ] {
            assert!(validate_one_field_pk(&invalid).is_err());
        }
        assert!(validate_row(&serde_json::json!({"id": 1})).is_ok());
        assert!(validate_row(&serde_json::json!({})).is_err());
        assert!(validate_row(&serde_json::json!({"": 1})).is_err());
        assert!(validate_row(&Value::Null).is_err());
        assert!(validate_row(&serde_json::json!([])).is_err());

        let timestamp = "2026-07-18T13:59:01.000000Z";
        assert!(is_canonical_utc_microsecond(timestamp));
        for index in [4, 7, 10, 13, 16, 19, 26] {
            let mut invalid = timestamp.as_bytes().to_vec();
            invalid[index] = b'X';
            assert!(!is_canonical_utc_microsecond(
                std::str::from_utf8(&invalid).unwrap()
            ));
        }
        for invalid in ["2026-07-18T13:59:01Z", "2026-13-18T13:59:01.000000Z"] {
            assert!(!is_canonical_utc_microsecond(invalid));
        }

        for valid in [
            "0.0.0",
            "1.2.3",
            "1.2.3-0",
            "1.2.3-alpha.1",
            "1.2.3+build-1",
        ] {
            assert!(is_semver(valid), "{valid}");
        }
        for invalid in [
            "",
            "1.2",
            "1.2.3.4",
            "01.2.3",
            "1.02.3",
            "1.2.03",
            "1.2.3-01",
            "1.2.3-",
            "1.2.3+",
            "1.2.3+one+two",
            "1.2.3-alpha..1",
            "1.2.3-β",
        ] {
            assert!(!is_semver(invalid), "{invalid}");
        }
    }

    fn minimal_manifest() -> SchemaManifest {
        SchemaManifest {
            schema_version: 8,
            schema_hash: HASH_A.into(),
            parent_schema: Some(SchemaRef {
                version: 7,
                hash: HASH_B.into(),
            }),
            transition_class: SchemaTransitionClass::Class3,
            compatibility_floor: 8,
            tables: vec![TableSchema {
                table_id: "tbl_documents".into(),
                relation_id: "rel_documents".into(),
                name: "documents".into(),
                primary_key_field_id: "fld_documents_id".into(),
                lifecycle: LifecycleSchema {
                    created_at_field_id: None,
                    updated_at_field_id: None,
                    deleted_at_field_id: None,
                },
                composition: CompositionClass::SingleScope,
                fields: vec![ColumnSchema {
                    field_id: "fld_documents_id".into(),
                    name: "id".into(),
                    type_name: "string".into(),
                    nullable: false,
                    writable: false,
                    precision: None,
                    scale: None,
                }],
                indexes: vec![],
            }],
        }
    }
}
