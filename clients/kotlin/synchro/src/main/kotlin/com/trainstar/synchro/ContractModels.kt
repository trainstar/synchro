package com.trainstar.synchro

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject

class ContractException(message: String) : IllegalArgumentException(message)

@Serializable
enum class Operation {
    @SerialName("insert") INSERT,
    @SerialName("upsert") UPSERT,
    @SerialName("update") UPDATE,
    @SerialName("delete") DELETE,
}

@Serializable
enum class SchemaAction {
    @SerialName("none") NONE,
    @SerialName("fetch") FETCH,
    @SerialName("replace") REPLACE,
    @SerialName("rebuild_local") REBUILD_LOCAL,
    @SerialName("unsupported") UNSUPPORTED;

    fun requiresSchemaDefinition(): Boolean = this == REPLACE || this == REBUILD_LOCAL
    fun requiresLocalRebuild(): Boolean = this == REBUILD_LOCAL
    fun isCompatible(): Boolean = this != UNSUPPORTED
}

@Serializable
enum class ChecksumMode {
    @SerialName("none") NONE,
    @SerialName("requested") REQUESTED,
    @SerialName("required") REQUIRED,
}

@Serializable
enum class MutationStatus {
    @SerialName("applied") APPLIED,
    @SerialName("conflict") CONFLICT,
    @SerialName("rejected_terminal") REJECTED_TERMINAL,
    @SerialName("rejected_retryable") REJECTED_RETRYABLE,
}

@Serializable
enum class MutationRejectionCode {
    @SerialName("version_conflict") VERSION_CONFLICT,
    @SerialName("policy_rejected") POLICY_REJECTED,
    @SerialName("validation_failed") VALIDATION_FAILED,
    @SerialName("table_not_synced") TABLE_NOT_SYNCED,
    @SerialName("unknown_scope_effect") UNKNOWN_SCOPE_EFFECT,
    @SerialName("server_retryable") SERVER_RETRYABLE,
}

@Serializable
enum class ProtocolErrorCode {
    @SerialName("invalid_request") INVALID_REQUEST,
    @SerialName("upgrade_required") UPGRADE_REQUIRED,
    @SerialName("auth_required") AUTH_REQUIRED,
    @SerialName("schema_mismatch") SCHEMA_MISMATCH,
    @SerialName("retry_later") RETRY_LATER,
    @SerialName("temporary_unavailable") TEMPORARY_UNAVAILABLE,
}

@Serializable
enum class CompositionClass {
    @SerialName("single_scope") SINGLE_SCOPE,
    @SerialName("multi_scope") MULTI_SCOPE,
}

@Serializable
data class SchemaRef(
    val version: Long,
    val hash: String,
)

@Serializable
data class ScopeCursorRef(
    val cursor: String? = null,
)

@Serializable
data class ScopeAssignment(
    val id: String,
    val cursor: String? = null,
)

@Serializable
data class ScopeAssignmentDelta(
    val add: List<ScopeAssignment>,
    val remove: List<String>,
) {
    fun validate() {
        val added = mutableSetOf<String>()
        for (scope in add) {
            require(scope.id.isNotEmpty()) { "scope id must not be empty" }
            if (!added.add(scope.id)) {
                throw ContractException("duplicate added scope ${scope.id}")
            }
        }

        val removed = mutableSetOf<String>()
        for (scopeId in remove) {
            require(scopeId.isNotEmpty()) { "scope id must not be empty" }
            if (!removed.add(scopeId)) {
                throw ContractException("duplicate removed scope $scopeId")
            }
            if (added.contains(scopeId)) {
                throw ContractException("conflicting scope assignment $scopeId")
            }
        }
    }
}

@Serializable
data class SchemaDescriptor(
    val version: Long,
    val hash: String,
    val action: SchemaAction,
)

@Serializable
data class ColumnSchema(
    val name: String,
    @SerialName("type") val typeName: String,
    val nullable: Boolean,
)

@Serializable
data class IndexSchema(
    val name: String,
    val columns: List<String>,
)

@Serializable
data class TableSchema(
    val name: String,
    @SerialName("primary_key") val primaryKey: List<String>? = null,
    @SerialName("updated_at_column") val updatedAtColumn: String? = null,
    @SerialName("deleted_at_column") val deletedAtColumn: String? = null,
    val composition: CompositionClass? = null,
    val columns: List<ColumnSchema>? = null,
    val indexes: List<IndexSchema>? = null,
)

@Serializable
data class SchemaManifest(
    val tables: List<TableSchema>,
) {
    fun validate() {
        val tableNames = mutableSetOf<String>()
        for (table in tables) {
            require(table.name.isNotEmpty()) { "table name must not be empty" }
            if (!tableNames.add(table.name)) {
                throw ContractException("duplicate table ${table.name}")
            }

            val columnNames = mutableSetOf<String>()
            for (column in table.columns.orEmpty()) {
                require(column.name.isNotEmpty()) { "column name must not be empty for ${table.name}" }
                if (!columnNames.add(column.name)) {
                    throw ContractException("duplicate column ${table.name}.${column.name}")
                }
            }
            table.primaryKey?.forEach { columnName ->
                if (columnName !in columnNames) {
                    throw ContractException("unknown primary key column ${table.name}.$columnName")
                }
            }
            table.updatedAtColumn?.let { columnName ->
                if (columnName !in columnNames) {
                    throw ContractException("unknown updated_at column ${table.name}.$columnName")
                }
            }
            table.deletedAtColumn?.let { columnName ->
                if (columnName !in columnNames) {
                    throw ContractException("unknown deleted_at column ${table.name}.$columnName")
                }
            }

            val indexNames = mutableSetOf<String>()
            for (index in table.indexes.orEmpty()) {
                require(index.name.isNotEmpty()) { "index name must not be empty for ${table.name}" }
                if (!indexNames.add(index.name)) {
                    throw ContractException("duplicate index ${table.name}.${index.name}")
                }
                if (columnNames.isNotEmpty()) {
                    for (columnName in index.columns) {
                        if (!columnNames.contains(columnName)) {
                            throw ContractException("unknown index column ${table.name}.${index.name} -> $columnName")
                        }
                    }
                }
            }
        }
    }
}

@Serializable
data class ConnectRequest(
    @SerialName("client_id") val clientID: String,
    val platform: String,
    @SerialName("app_version") val appVersion: String,
    @SerialName("protocol_version") val protocolVersion: Int,
    val schema: SchemaRef,
    @SerialName("scope_set_version") val scopeSetVersion: Long,
    @SerialName("known_scopes") val knownScopes: Map<String, ScopeCursorRef>,
)

@Serializable
data class ConnectResponse(
    @SerialName("server_time") val serverTime: String,
    @SerialName("protocol_version") val protocolVersion: Int,
    @SerialName("scope_set_version") val scopeSetVersion: Long,
    val schema: SchemaDescriptor,
    val scopes: ScopeAssignmentDelta,
    @SerialName("schema_definition") val schemaDefinition: SchemaManifest? = null,
) {
    fun validate() {
        if (schema.action.requiresSchemaDefinition() != (schemaDefinition != null)) {
            throw ContractException(
                "schema action ${schema.action} is inconsistent with schema_definition presence ${schemaDefinition != null}"
            )
        }
        scopes.validate()
        schemaDefinition?.validate()
    }
}

@Serializable
data class Mutation(
    @SerialName("mutation_id") val mutationID: String,
    val table: String,
    val op: Operation,
    val pk: JsonObject,
    @SerialName("base_version") val baseVersion: String? = null,
    @SerialName("client_version") val clientVersion: String? = null,
    val columns: JsonObject? = null,
)

@Serializable
data class PushRequest(
    @SerialName("client_id") val clientID: String,
    @SerialName("batch_id") val batchID: String,
    val schema: SchemaRef,
    val mutations: List<Mutation>,
)

@Serializable
data class AcceptedMutation(
    @SerialName("mutation_id") val mutationID: String,
    val table: String,
    val pk: JsonObject,
    val status: MutationStatus,
    @SerialName("server_row") val serverRow: JsonObject? = null,
    @SerialName("server_version") val serverVersion: String,
)

@Serializable
data class RejectedMutation(
    @SerialName("mutation_id") val mutationID: String,
    val table: String,
    val pk: JsonObject,
    val status: MutationStatus,
    val code: MutationRejectionCode,
    val message: String? = null,
    @SerialName("server_row") val serverRow: JsonObject? = null,
    @SerialName("server_version") val serverVersion: String? = null,
)

@Serializable
data class PushResponse(
    @SerialName("server_time") val serverTime: String,
    val accepted: List<AcceptedMutation>,
    val rejected: List<RejectedMutation>,
)

@Serializable
data class PullRequest(
    @SerialName("client_id") val clientID: String,
    val schema: SchemaRef,
    @SerialName("scope_set_version") val scopeSetVersion: Long,
    val scopes: Map<String, ScopeCursorRef>,
    val limit: Int,
    @SerialName("checksum_mode") val checksumMode: ChecksumMode? = null,
)

@Serializable
data class ChangeRecord(
    val scope: String,
    val table: String,
    val op: Operation,
    val pk: JsonObject,
    val row: JsonObject? = null,
    @SerialName("server_version") val serverVersion: String,
)

@Serializable
data class PullResponse(
    val changes: List<ChangeRecord>,
    @SerialName("scope_set_version") val scopeSetVersion: Long,
    @SerialName("scope_cursors") val scopeCursors: Map<String, String>,
    @SerialName("scope_updates") val scopeUpdates: ScopeAssignmentDelta,
    val rebuild: List<String>,
    @SerialName("has_more") val hasMore: Boolean,
    val checksums: Map<String, String>? = null,
) {
    fun requestsRebuild(): Boolean = rebuild.isNotEmpty()

    fun validate(request: PullRequest) {
        scopeUpdates.validate()
        if (request.checksumMode == ChecksumMode.REQUIRED && checksums == null) {
            throw ContractException("required checksums missing")
        }
    }
}

@Serializable
data class RebuildRequest(
    @SerialName("client_id") val clientID: String,
    val scope: String,
    val cursor: String? = null,
    val limit: Int,
)

@Serializable
data class RebuildRecord(
    val table: String,
    val pk: JsonObject,
    val row: JsonObject? = null,
    @SerialName("server_version") val serverVersion: String,
)

@Serializable
data class RebuildResponse(
    val scope: String,
    val records: List<RebuildRecord>,
    val cursor: String? = null,
    @SerialName("has_more") val hasMore: Boolean,
    @SerialName("final_scope_cursor") val finalScopeCursor: String? = null,
    val checksum: String? = null,
) {
    fun isFinalPage(): Boolean = !hasMore && finalScopeCursor != null

    fun validate() {
        if (hasMore) {
            if (finalScopeCursor != null) {
                throw ContractException("partial rebuild must not include final scope cursor")
            }
            if (checksum != null) {
                throw ContractException("partial rebuild must not include checksum")
            }
        } else {
            if (finalScopeCursor == null) {
                throw ContractException("final rebuild page must include final scope cursor")
            }
            if (checksum == null) {
                throw ContractException("final rebuild page must include checksum")
            }
        }
    }
}

@Serializable
data class ErrorBody(
    val code: ProtocolErrorCode,
    val message: String,
    val retryable: Boolean,
)

@Serializable
data class ErrorResponse(
    val error: ErrorBody,
)
