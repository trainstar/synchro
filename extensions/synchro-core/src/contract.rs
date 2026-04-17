use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::convert::TryFrom;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VNextOperation {
    Insert,
    Upsert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyOperationConversionError {
    pub operation: VNextOperation,
}

impl std::fmt::Display for LegacyOperationConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "vNext operation {:?} cannot be represented by the legacy operation model",
            self.operation
        )
    }
}

impl std::error::Error for LegacyOperationConversionError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContractViolation {
    EmptyScopeId,
    DuplicateAddedScope {
        scope_id: String,
    },
    DuplicateRemovedScope {
        scope_id: String,
    },
    ConflictingScopeAssignment {
        scope_id: String,
    },
    SchemaDefinitionMismatch {
        action: SchemaAction,
        has_schema_definition: bool,
    },
    EmptyTableName,
    DuplicateTableName {
        table_name: String,
    },
    EmptyColumnName {
        table_name: String,
    },
    DuplicateColumnName {
        table_name: String,
        column_name: String,
    },
    UnsupportedColumnType {
        table_name: String,
        column_name: String,
        type_name: String,
    },
    UnknownPrimaryKeyColumn {
        table_name: String,
        column_name: String,
    },
    UnknownUpdatedAtColumn {
        table_name: String,
        column_name: String,
    },
    UnknownDeletedAtColumn {
        table_name: String,
        column_name: String,
    },
    EmptyIndexName {
        table_name: String,
    },
    DuplicateIndexName {
        table_name: String,
        index_name: String,
    },
    UnknownIndexColumn {
        table_name: String,
        index_name: String,
        column_name: String,
    },
    RequiredChecksumsMissing,
    FinalRebuildCursorMissing,
    FinalRebuildChecksumMissing,
    PartialRebuildHasFinalCursor,
    PartialRebuildHasChecksum,
}

impl std::fmt::Display for ContractViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyScopeId => f.write_str("scope id must not be empty"),
            Self::DuplicateAddedScope { scope_id } => {
                write!(f, "scope add list contains duplicate scope id {scope_id}")
            }
            Self::DuplicateRemovedScope { scope_id } => {
                write!(f, "scope remove list contains duplicate scope id {scope_id}")
            }
            Self::ConflictingScopeAssignment { scope_id } => {
                write!(f, "scope id {scope_id} appears in both add and remove")
            }
            Self::SchemaDefinitionMismatch {
                action,
                has_schema_definition,
            } => write!(
                f,
                "schema action {:?} is inconsistent with schema_definition presence {}",
                action, has_schema_definition
            ),
            Self::EmptyTableName => f.write_str("table name must not be empty"),
            Self::DuplicateTableName { table_name } => {
                write!(f, "schema manifest contains duplicate table {table_name}")
            }
            Self::EmptyColumnName { table_name } => {
                write!(f, "schema manifest table {table_name} contains an empty column name")
            }
            Self::DuplicateColumnName {
                table_name,
                column_name,
            } => write!(
                f,
                "schema manifest table {table_name} contains duplicate column {column_name}"
            ),
            Self::UnsupportedColumnType {
                table_name,
                column_name,
                type_name,
            } => write!(
                f,
                "schema manifest table {table_name} column {column_name} uses unsupported type {type_name}"
            ),
            Self::UnknownPrimaryKeyColumn {
                table_name,
                column_name,
            } => write!(
                f,
                "schema manifest table {table_name} primary key references unknown column {column_name}"
            ),
            Self::UnknownUpdatedAtColumn {
                table_name,
                column_name,
            } => write!(
                f,
                "schema manifest table {table_name} updated_at_column references unknown column {column_name}"
            ),
            Self::UnknownDeletedAtColumn {
                table_name,
                column_name,
            } => write!(
                f,
                "schema manifest table {table_name} deleted_at_column references unknown column {column_name}"
            ),
            Self::EmptyIndexName { table_name } => {
                write!(f, "schema manifest table {table_name} contains an empty index name")
            }
            Self::DuplicateIndexName {
                table_name,
                index_name,
            } => write!(
                f,
                "schema manifest table {table_name} contains duplicate index {index_name}"
            ),
            Self::UnknownIndexColumn {
                table_name,
                index_name,
                column_name,
            } => write!(
                f,
                "schema manifest table {table_name} index {index_name} references unknown column {column_name}"
            ),
            Self::RequiredChecksumsMissing => {
                f.write_str("pull response is missing required checksums")
            }
            Self::FinalRebuildCursorMissing => {
                f.write_str("final rebuild page must include final_scope_cursor")
            }
            Self::FinalRebuildChecksumMissing => {
                f.write_str("final rebuild page must include checksum")
            }
            Self::PartialRebuildHasFinalCursor => {
                f.write_str("non-final rebuild page must not include final_scope_cursor")
            }
            Self::PartialRebuildHasChecksum => {
                f.write_str("non-final rebuild page must not include checksum")
            }
        }
    }
}

impl std::error::Error for ContractViolation {}

impl From<crate::change::ChangeOperation> for VNextOperation {
    fn from(value: crate::change::ChangeOperation) -> Self {
        match value {
            crate::change::ChangeOperation::Insert => Self::Insert,
            crate::change::ChangeOperation::Update => Self::Update,
            crate::change::ChangeOperation::Delete => Self::Delete,
        }
    }
}

impl TryFrom<VNextOperation> for crate::change::ChangeOperation {
    type Error = LegacyOperationConversionError;

    fn try_from(value: VNextOperation) -> Result<Self, Self::Error> {
        match value {
            VNextOperation::Insert => Ok(crate::change::ChangeOperation::Insert),
            VNextOperation::Update => Ok(crate::change::ChangeOperation::Update),
            VNextOperation::Delete => Ok(crate::change::ChangeOperation::Delete),
            VNextOperation::Upsert => Err(LegacyOperationConversionError { operation: value }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaAction {
    None,
    Fetch,
    Replace,
    RebuildLocal,
    Unsupported,
}

impl SchemaAction {
    pub fn requires_schema_definition(self) -> bool {
        matches!(self, Self::Replace | Self::RebuildLocal)
    }

    pub fn requires_local_rebuild(self) -> bool {
        matches!(self, Self::RebuildLocal)
    }

    pub fn is_compatible(self) -> bool {
        !matches!(self, Self::Unsupported)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ChecksumMode {
    #[default]
    None,
    Requested,
    Required,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationStatus {
    Applied,
    Conflict,
    RejectedTerminal,
    RejectedRetryable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationRejectionCode {
    VersionConflict,
    PolicyRejected,
    ValidationFailed,
    TableNotSynced,
    UnknownScopeEffect,
    ServerRetryable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolErrorCode {
    InvalidRequest,
    UpgradeRequired,
    AuthRequired,
    SchemaMismatch,
    RetryLater,
    TemporaryUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompositionClass {
    SingleScope,
    MultiScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaRef {
    pub version: i64,
    pub hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeCursorRef {
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeAssignment {
    pub id: String,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeAssignmentDelta {
    pub add: Vec<ScopeAssignment>,
    pub remove: Vec<String>,
}

impl ScopeAssignmentDelta {
    pub fn validate(&self) -> Result<(), ContractViolation> {
        let mut added = std::collections::HashSet::with_capacity(self.add.len());
        for scope in &self.add {
            if scope.id.is_empty() {
                return Err(ContractViolation::EmptyScopeId);
            }
            if !added.insert(scope.id.as_str()) {
                return Err(ContractViolation::DuplicateAddedScope {
                    scope_id: scope.id.clone(),
                });
            }
        }

        let mut removed = std::collections::HashSet::with_capacity(self.remove.len());
        for scope_id in &self.remove {
            if scope_id.is_empty() {
                return Err(ContractViolation::EmptyScopeId);
            }
            if !removed.insert(scope_id.as_str()) {
                return Err(ContractViolation::DuplicateRemovedScope {
                    scope_id: scope_id.clone(),
                });
            }
            if added.contains(scope_id.as_str()) {
                return Err(ContractViolation::ConflictingScopeAssignment {
                    scope_id: scope_id.clone(),
                });
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaDescriptor {
    pub version: i64,
    pub hash: String,
    pub action: SchemaAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexSchema {
    pub name: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at_column: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_at_column: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub composition: Option<CompositionClass>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<ColumnSchema>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub indexes: Option<Vec<IndexSchema>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaManifest {
    pub tables: Vec<TableSchema>,
}

pub fn normalize_portable_type_name(type_name: &str) -> Option<&'static str> {
    let normalized = type_name.trim().to_ascii_lowercase();

    if normalized.ends_with("[]") {
        return Some("json");
    }
    if normalized.starts_with("numeric(") || normalized.starts_with("decimal(") {
        return Some("float");
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
        "float" | "float64" | "numeric" | "decimal" | "real" | "double precision" => Some("float"),
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
        let mut table_names = std::collections::HashSet::with_capacity(self.tables.len());
        for table in &self.tables {
            if table.name.is_empty() {
                return Err(ContractViolation::EmptyTableName);
            }
            if !table_names.insert(table.name.as_str()) {
                return Err(ContractViolation::DuplicateTableName {
                    table_name: table.name.clone(),
                });
            }

            let mut column_names = std::collections::HashSet::new();
            if let Some(columns) = &table.columns {
                for column in columns {
                    if column.name.is_empty() {
                        return Err(ContractViolation::EmptyColumnName {
                            table_name: table.name.clone(),
                        });
                    }
                    if !is_canonical_portable_type_name(column.type_name.as_str()) {
                        return Err(ContractViolation::UnsupportedColumnType {
                            table_name: table.name.clone(),
                            column_name: column.name.clone(),
                            type_name: column.type_name.clone(),
                        });
                    }
                    if !column_names.insert(column.name.as_str()) {
                        return Err(ContractViolation::DuplicateColumnName {
                            table_name: table.name.clone(),
                            column_name: column.name.clone(),
                        });
                    }
                }
            }

            if let Some(primary_key) = &table.primary_key {
                for column_name in primary_key {
                    if !column_names.is_empty() && !column_names.contains(column_name.as_str()) {
                        return Err(ContractViolation::UnknownPrimaryKeyColumn {
                            table_name: table.name.clone(),
                            column_name: column_name.clone(),
                        });
                    }
                }
            }
            if let Some(column_name) = &table.updated_at_column {
                if !column_names.is_empty() && !column_names.contains(column_name.as_str()) {
                    return Err(ContractViolation::UnknownUpdatedAtColumn {
                        table_name: table.name.clone(),
                        column_name: column_name.clone(),
                    });
                }
            }
            if let Some(column_name) = &table.deleted_at_column {
                if !column_names.is_empty() && !column_names.contains(column_name.as_str()) {
                    return Err(ContractViolation::UnknownDeletedAtColumn {
                        table_name: table.name.clone(),
                        column_name: column_name.clone(),
                    });
                }
            }

            let mut index_names = std::collections::HashSet::new();
            if let Some(indexes) = &table.indexes {
                for index in indexes {
                    if index.name.is_empty() {
                        return Err(ContractViolation::EmptyIndexName {
                            table_name: table.name.clone(),
                        });
                    }
                    if !index_names.insert(index.name.as_str()) {
                        return Err(ContractViolation::DuplicateIndexName {
                            table_name: table.name.clone(),
                            index_name: index.name.clone(),
                        });
                    }
                    for column_name in &index.columns {
                        if !column_names.is_empty() && !column_names.contains(column_name.as_str())
                        {
                            return Err(ContractViolation::UnknownIndexColumn {
                                table_name: table.name.clone(),
                                index_name: index.name.clone(),
                                column_name: column_name.clone(),
                            });
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectRequest {
    pub client_id: String,
    pub platform: String,
    pub app_version: String,
    pub protocol_version: u32,
    pub schema: SchemaRef,
    pub scope_set_version: i64,
    pub known_scopes: BTreeMap<String, ScopeCursorRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectResponse {
    pub server_time: DateTime<Utc>,
    pub protocol_version: u32,
    pub scope_set_version: i64,
    pub schema: SchemaDescriptor,
    pub scopes: ScopeAssignmentDelta,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_definition: Option<SchemaManifest>,
}

impl ConnectResponse {
    pub fn validate(&self) -> Result<(), ContractViolation> {
        let has_schema_definition = self.schema_definition.is_some();
        if self.schema.action.requires_schema_definition() != has_schema_definition {
            return Err(ContractViolation::SchemaDefinitionMismatch {
                action: self.schema.action,
                has_schema_definition,
            });
        }
        self.scopes.validate()?;
        if let Some(schema_definition) = &self.schema_definition {
            schema_definition.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Mutation {
    pub mutation_id: String,
    pub table: String,
    pub op: VNextOperation,
    pub pk: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub columns: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PushRequest {
    pub client_id: String,
    pub batch_id: String,
    pub schema: SchemaRef,
    pub mutations: Vec<Mutation>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AcceptedMutation {
    pub mutation_id: String,
    pub table: String,
    pub pk: Value,
    pub status: MutationStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_row: Option<Value>,
    pub server_version: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RejectedMutation {
    pub mutation_id: String,
    pub table: String,
    pub pk: Value,
    pub status: MutationStatus,
    pub code: MutationRejectionCode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_row: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PushResponse {
    pub server_time: DateTime<Utc>,
    pub accepted: Vec<AcceptedMutation>,
    pub rejected: Vec<RejectedMutation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequest {
    pub client_id: String,
    pub schema: SchemaRef,
    pub scope_set_version: i64,
    pub scopes: BTreeMap<String, ScopeCursorRef>,
    pub limit: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum_mode: Option<ChecksumMode>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangeRecord {
    pub scope: String,
    pub table: String,
    pub op: VNextOperation,
    pub pk: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row: Option<Value>,
    pub server_version: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PullResponse {
    pub changes: Vec<ChangeRecord>,
    pub scope_set_version: i64,
    pub scope_cursors: BTreeMap<String, String>,
    pub scope_updates: ScopeAssignmentDelta,
    pub rebuild: Vec<String>,
    pub has_more: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksums: Option<BTreeMap<String, String>>,
}

impl PullResponse {
    pub fn requests_rebuild(&self) -> bool {
        !self.rebuild.is_empty()
    }

    pub fn validate_for_request(&self, request: &PullRequest) -> Result<(), ContractViolation> {
        self.scope_updates.validate()?;
        if request.checksum_mode.unwrap_or_default() == ChecksumMode::Required
            && self.checksums.is_none()
        {
            return Err(ContractViolation::RequiredChecksumsMissing);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebuildRequest {
    pub client_id: String,
    pub scope: String,
    pub cursor: Option<String>,
    pub limit: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RebuildRecord {
    pub table: String,
    pub pk: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row: Option<Value>,
    pub server_version: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RebuildResponse {
    pub scope: String,
    pub records: Vec<RebuildRecord>,
    pub cursor: Option<String>,
    pub has_more: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_scope_cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
}

impl RebuildResponse {
    pub fn is_final_page(&self) -> bool {
        !self.has_more && self.final_scope_cursor.is_some()
    }

    pub fn validate(&self) -> Result<(), ContractViolation> {
        if self.has_more {
            if self.final_scope_cursor.is_some() {
                return Err(ContractViolation::PartialRebuildHasFinalCursor);
            }
            if self.checksum.is_some() {
                return Err(ContractViolation::PartialRebuildHasChecksum);
            }
        } else {
            if self.final_scope_cursor.is_none() {
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
pub struct ErrorBody {
    pub code: ProtocolErrorCode,
    pub message: String,
    pub retryable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: ErrorBody,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(path: &str) -> Value {
        let raw = match path {
            "../../../conformance/protocol/connect-none.json" => {
                include_str!("../../../conformance/protocol/connect-none.json")
            }
            "../../../conformance/protocol/connect-rebuild-local.json" => {
                include_str!("../../../conformance/protocol/connect-rebuild-local.json")
            }
            "../../../conformance/protocol/connect-unsupported.json" => {
                include_str!("../../../conformance/protocol/connect-unsupported.json")
            }
            "../../../conformance/protocol/error-upgrade-required.json" => {
                include_str!("../../../conformance/protocol/error-upgrade-required.json")
            }
            "../../../conformance/protocol/pull-required-checksums.json" => {
                include_str!("../../../conformance/protocol/pull-required-checksums.json")
            }
            "../../../conformance/mutations/push-mixed-accept-reject.json" => {
                include_str!("../../../conformance/mutations/push-mixed-accept-reject.json")
            }
            "../../../conformance/scopes/pull-delta-and-rebuild.json" => {
                include_str!("../../../conformance/scopes/pull-delta-and-rebuild.json")
            }
            "../../../conformance/scopes/rebuild-single-scope.json" => {
                include_str!("../../../conformance/scopes/rebuild-single-scope.json")
            }
            "../../../conformance/schema/additive-column-rebuild-local.json" => {
                include_str!("../../../conformance/schema/additive-column-rebuild-local.json")
            }
            "../../../conformance/schema/schema-manifest-portable.json" => {
                include_str!("../../../conformance/schema/schema-manifest-portable.json")
            }
            "../../../conformance/traces/rebuild-cycle.json" => {
                include_str!("../../../conformance/traces/rebuild-cycle.json")
            }
            _ => panic!("unknown fixture path: {path}"),
        };
        serde_json::from_str(raw).expect("fixture json should decode")
    }

    #[test]
    fn connect_none_fixture_decodes() {
        let doc = fixture("../../../conformance/protocol/connect-none.json");
        let request: ConnectRequest =
            serde_json::from_value(doc["input"]["request"].clone()).unwrap();
        let response: ConnectResponse =
            serde_json::from_value(doc["expected"]["response"].clone()).unwrap();

        assert_eq!(request.protocol_version, 1);
        response.validate().unwrap();
        assert_eq!(response.schema.action, SchemaAction::None);
        assert!(response.schema.action.is_compatible());
        assert!(!response.schema.action.requires_schema_definition());
        assert!(response.schema_definition.is_none());
        assert!(response.scopes.add.is_empty());
    }

    #[test]
    fn connect_rebuild_local_fixture_decodes() {
        let doc = fixture("../../../conformance/protocol/connect-rebuild-local.json");
        let response: ConnectResponse =
            serde_json::from_value(doc["expected"]["response"].clone()).unwrap();

        response.validate().unwrap();
        assert_eq!(response.schema.action, SchemaAction::RebuildLocal);
        assert!(response.schema.action.requires_schema_definition());
        assert!(response.schema.action.requires_local_rebuild());
        assert!(response.schema_definition.is_some());
        assert_eq!(response.scopes.add.len(), 2);
    }

    #[test]
    fn connect_unsupported_fixture_decodes() {
        let doc = fixture("../../../conformance/protocol/connect-unsupported.json");
        let response: ConnectResponse =
            serde_json::from_value(doc["expected"]["response"].clone()).unwrap();

        response.validate().unwrap();
        assert_eq!(response.schema.action, SchemaAction::Unsupported);
        assert!(!response.schema.action.is_compatible());
        assert!(response.scopes.add.is_empty());
        assert!(response.schema_definition.is_none());
    }

    #[test]
    fn push_fixture_decodes() {
        let doc = fixture("../../../conformance/mutations/push-mixed-accept-reject.json");
        let request: PushRequest = serde_json::from_value(doc["input"]["request"].clone()).unwrap();
        let response: PushResponse =
            serde_json::from_value(doc["expected"]["response"].clone()).unwrap();

        assert_eq!(request.mutations[0].op, VNextOperation::Insert);
        assert_eq!(request.mutations[1].op, VNextOperation::Delete);
        assert_eq!(response.accepted[0].status, MutationStatus::Applied);
        assert_eq!(response.rejected[0].status, MutationStatus::Conflict);
        assert_eq!(
            response.rejected[0].code,
            MutationRejectionCode::VersionConflict
        );
    }

    #[test]
    fn pull_fixture_decodes() {
        let doc = fixture("../../../conformance/scopes/pull-delta-and-rebuild.json");
        let request: PullRequest = serde_json::from_value(doc["input"]["request"].clone()).unwrap();
        let response: PullResponse =
            serde_json::from_value(doc["expected"]["response"].clone()).unwrap();

        response.validate_for_request(&request).unwrap();
        assert_eq!(request.checksum_mode, Some(ChecksumMode::Requested));
        assert_eq!(response.scope_set_version, 14);
        assert_eq!(response.changes[0].op, VNextOperation::Upsert);
        assert!(response.requests_rebuild());
    }

    #[test]
    fn pull_required_checksums_fixture_decodes() {
        let doc = fixture("../../../conformance/protocol/pull-required-checksums.json");
        let request: PullRequest = serde_json::from_value(doc["input"]["request"].clone()).unwrap();
        let response: PullResponse =
            serde_json::from_value(doc["expected"]["response"].clone()).unwrap();

        response.validate_for_request(&request).unwrap();
        assert_eq!(request.checksum_mode, Some(ChecksumMode::Required));
        assert!(response.checksums.is_some());
        assert!(!response.requests_rebuild());
    }

    #[test]
    fn rebuild_fixture_decodes() {
        let doc = fixture("../../../conformance/scopes/rebuild-single-scope.json");
        let request: RebuildRequest =
            serde_json::from_value(doc["input"]["request"].clone()).unwrap();
        let pages: Vec<RebuildResponse> =
            serde_json::from_value(doc["expected"]["pages"].clone()).unwrap();

        assert_eq!(request.scope, "exercises_public");
        pages[0].validate().unwrap();
        pages[1].validate().unwrap();
        assert!(pages[0].has_more);
        assert!(!pages[0].is_final_page());
        assert!(pages[1].is_final_page());
        assert_eq!(pages[1].final_scope_cursor.as_deref(), Some("c_202"));
        assert_eq!(pages[1].checksum.as_deref(), Some("cs_0f22"));
    }

    #[test]
    fn schema_fixture_decodes() {
        let doc = fixture("../../../conformance/schema/additive-column-rebuild-local.json");
        let expected = &doc["expected"];

        let action: SchemaAction =
            serde_json::from_value(expected["connect_schema_action"].clone()).unwrap();
        assert_eq!(action, SchemaAction::RebuildLocal);
    }

    #[test]
    fn schema_manifest_fixture_decodes() {
        let doc = fixture("../../../conformance/schema/schema-manifest-portable.json");
        let manifest: SchemaManifest = serde_json::from_value(doc["manifest"].clone()).unwrap();

        manifest.validate().unwrap();
        assert_eq!(manifest.tables.len(), 2);
        assert_eq!(
            manifest.tables[0].composition,
            Some(CompositionClass::SingleScope)
        );
        assert_eq!(
            manifest.tables[1].composition,
            Some(CompositionClass::MultiScope)
        );
        assert!(manifest.tables[0].columns.is_some());
        assert_eq!(
            manifest.tables[0].updated_at_column.as_deref(),
            Some("updated_at")
        );
        assert_eq!(
            manifest.tables[0].deleted_at_column.as_deref(),
            Some("deleted_at")
        );
    }

    #[test]
    fn error_envelope_fixture_decodes() {
        let doc = fixture("../../../conformance/protocol/error-upgrade-required.json");
        let response: ErrorResponse =
            serde_json::from_value(doc["expected"]["response"].clone()).unwrap();

        assert_eq!(response.error.code, ProtocolErrorCode::UpgradeRequired);
        assert!(!response.error.retryable);
    }

    #[test]
    fn rebuild_trace_fixture_still_matches_state_contract() {
        let doc = fixture("../../../conformance/traces/rebuild-cycle.json");
        let events = doc["expected_transitions"].as_array().unwrap();
        assert_eq!(events.len(), 4);
        assert_eq!(events[0].as_str(), Some("ready -> rebuilding"));
    }

    #[test]
    fn legacy_operation_converts_to_vnext() {
        let converted = VNextOperation::from(crate::change::ChangeOperation::Insert);
        assert_eq!(converted, VNextOperation::Insert);
    }

    #[test]
    fn vnext_upsert_rejects_legacy_conversion() {
        let converted = crate::change::ChangeOperation::try_from(VNextOperation::Upsert);
        assert!(converted.is_err());
    }

    #[test]
    fn connect_validate_rejects_missing_schema_definition() {
        let response = ConnectResponse {
            server_time: Utc::now(),
            protocol_version: 1,
            scope_set_version: 7,
            schema: SchemaDescriptor {
                version: 8,
                hash: "abc123".into(),
                action: SchemaAction::Replace,
            },
            scopes: ScopeAssignmentDelta {
                add: vec![],
                remove: vec![],
            },
            schema_definition: None,
        };

        assert_eq!(
            response.validate(),
            Err(ContractViolation::SchemaDefinitionMismatch {
                action: SchemaAction::Replace,
                has_schema_definition: false,
            })
        );
    }

    #[test]
    fn scope_assignment_delta_rejects_duplicates_and_overlap() {
        let duplicate_add = ScopeAssignmentDelta {
            add: vec![
                ScopeAssignment {
                    id: "scope_a".into(),
                    cursor: None,
                },
                ScopeAssignment {
                    id: "scope_a".into(),
                    cursor: Some("c_1".into()),
                },
            ],
            remove: vec![],
        };
        assert_eq!(
            duplicate_add.validate(),
            Err(ContractViolation::DuplicateAddedScope {
                scope_id: "scope_a".into(),
            })
        );

        let overlap = ScopeAssignmentDelta {
            add: vec![ScopeAssignment {
                id: "scope_a".into(),
                cursor: None,
            }],
            remove: vec!["scope_a".into()],
        };
        assert_eq!(
            overlap.validate(),
            Err(ContractViolation::ConflictingScopeAssignment {
                scope_id: "scope_a".into(),
            })
        );
    }

    #[test]
    fn schema_manifest_rejects_unknown_index_column() {
        let manifest = SchemaManifest {
            tables: vec![TableSchema {
                name: "workouts".into(),
                primary_key: Some(vec!["id".into()]),
                updated_at_column: None,
                deleted_at_column: None,
                composition: Some(CompositionClass::SingleScope),
                columns: Some(vec![ColumnSchema {
                    name: "id".into(),
                    type_name: "string".into(),
                    nullable: false,
                }]),
                indexes: Some(vec![IndexSchema {
                    name: "idx_workouts_user".into(),
                    columns: vec!["user_id".into()],
                }]),
            }],
        };

        assert_eq!(
            manifest.validate(),
            Err(ContractViolation::UnknownIndexColumn {
                table_name: "workouts".into(),
                index_name: "idx_workouts_user".into(),
                column_name: "user_id".into(),
            })
        );
    }

    #[test]
    fn schema_manifest_rejects_missing_primary_key_column() {
        let manifest = SchemaManifest {
            tables: vec![TableSchema {
                name: "documents".into(),
                primary_key: Some(vec!["id".into()]),
                updated_at_column: None,
                deleted_at_column: None,
                composition: Some(CompositionClass::SingleScope),
                columns: Some(vec![ColumnSchema {
                    name: "title".into(),
                    type_name: "string".into(),
                    nullable: false,
                }]),
                indexes: None,
            }],
        };

        assert_eq!(
            manifest.validate(),
            Err(ContractViolation::UnknownPrimaryKeyColumn {
                table_name: "documents".into(),
                column_name: "id".into(),
            })
        );
    }

    #[test]
    fn schema_manifest_rejects_missing_timestamp_columns() {
        let updated_manifest = SchemaManifest {
            tables: vec![TableSchema {
                name: "documents".into(),
                primary_key: Some(vec!["id".into()]),
                updated_at_column: Some("updated_at".into()),
                deleted_at_column: None,
                composition: Some(CompositionClass::SingleScope),
                columns: Some(vec![ColumnSchema {
                    name: "id".into(),
                    type_name: "string".into(),
                    nullable: false,
                }]),
                indexes: None,
            }],
        };

        assert_eq!(
            updated_manifest.validate(),
            Err(ContractViolation::UnknownUpdatedAtColumn {
                table_name: "documents".into(),
                column_name: "updated_at".into(),
            })
        );

        let deleted_manifest = SchemaManifest {
            tables: vec![TableSchema {
                name: "documents".into(),
                primary_key: Some(vec!["id".into()]),
                updated_at_column: None,
                deleted_at_column: Some("deleted_at".into()),
                composition: Some(CompositionClass::SingleScope),
                columns: Some(vec![ColumnSchema {
                    name: "id".into(),
                    type_name: "string".into(),
                    nullable: false,
                }]),
                indexes: None,
            }],
        };

        assert_eq!(
            deleted_manifest.validate(),
            Err(ContractViolation::UnknownDeletedAtColumn {
                table_name: "documents".into(),
                column_name: "deleted_at".into(),
            })
        );
    }

    #[test]
    fn schema_manifest_rejects_noncanonical_column_type() {
        let manifest = SchemaManifest {
            tables: vec![TableSchema {
                name: "documents".into(),
                primary_key: Some(vec!["id".into()]),
                updated_at_column: None,
                deleted_at_column: None,
                composition: Some(CompositionClass::SingleScope),
                columns: Some(vec![
                    ColumnSchema {
                        name: "id".into(),
                        type_name: "string".into(),
                        nullable: false,
                    },
                    ColumnSchema {
                        name: "score".into(),
                        type_name: "integer".into(),
                        nullable: false,
                    },
                ]),
                indexes: None,
            }],
        };

        assert_eq!(
            manifest.validate(),
            Err(ContractViolation::UnsupportedColumnType {
                table_name: "documents".into(),
                column_name: "score".into(),
                type_name: "integer".into(),
            })
        );
    }

    #[test]
    fn normalize_portable_type_name_handles_pattern_aliases_and_arrays() {
        assert_eq!(normalize_portable_type_name("numeric(5,1)"), Some("float"));
        assert_eq!(normalize_portable_type_name("varchar(255)"), Some("string"));
        assert_eq!(normalize_portable_type_name("text[]"), Some("json"));
        assert_eq!(normalize_portable_type_name("integer[]"), Some("json"));
        assert_eq!(normalize_portable_type_name("interval"), Some("string"));
        assert_eq!(normalize_portable_type_name("inet"), Some("string"));
        assert_eq!(normalize_portable_type_name("int4range"), Some("string"));
    }

    #[test]
    fn pull_validate_requires_checksums_when_mode_is_required() {
        let request = PullRequest {
            client_id: "client_1".into(),
            schema: SchemaRef {
                version: 1,
                hash: "hash_1".into(),
            },
            scope_set_version: 1,
            scopes: BTreeMap::new(),
            limit: 100,
            checksum_mode: Some(ChecksumMode::Required),
        };
        let response = PullResponse {
            changes: vec![],
            scope_set_version: 1,
            scope_cursors: BTreeMap::new(),
            scope_updates: ScopeAssignmentDelta {
                add: vec![],
                remove: vec![],
            },
            rebuild: vec![],
            has_more: false,
            checksums: None,
        };

        assert_eq!(
            response.validate_for_request(&request),
            Err(ContractViolation::RequiredChecksumsMissing)
        );
    }

    #[test]
    fn rebuild_validate_rejects_partial_final_cursor_and_final_missing_checksum() {
        let partial = RebuildResponse {
            scope: "scope_a".into(),
            records: vec![],
            cursor: Some("rb_001".into()),
            has_more: true,
            final_scope_cursor: Some("c_1".into()),
            checksum: None,
        };
        assert_eq!(
            partial.validate(),
            Err(ContractViolation::PartialRebuildHasFinalCursor)
        );

        let final_page = RebuildResponse {
            scope: "scope_a".into(),
            records: vec![],
            cursor: None,
            has_more: false,
            final_scope_cursor: Some("c_1".into()),
            checksum: None,
        };
        assert_eq!(
            final_page.validate(),
            Err(ContractViolation::FinalRebuildChecksumMissing)
        );
    }
}
