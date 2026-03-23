package com.trainstar.synchro

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject

class VNextContractException(message: String) : IllegalArgumentException(message)

@Serializable
enum class VNextOperation {
    @SerialName("insert") INSERT,
    @SerialName("upsert") UPSERT,
    @SerialName("update") UPDATE,
    @SerialName("delete") DELETE,
}

@Serializable
enum class VNextSchemaAction {
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
enum class VNextChecksumMode {
    @SerialName("none") NONE,
    @SerialName("requested") REQUESTED,
    @SerialName("required") REQUIRED,
}

@Serializable
enum class VNextMutationStatus {
    @SerialName("applied") APPLIED,
    @SerialName("conflict") CONFLICT,
    @SerialName("rejected_terminal") REJECTED_TERMINAL,
    @SerialName("rejected_retryable") REJECTED_RETRYABLE,
}

@Serializable
enum class VNextMutationRejectionCode {
    @SerialName("version_conflict") VERSION_CONFLICT,
    @SerialName("policy_rejected") POLICY_REJECTED,
    @SerialName("validation_failed") VALIDATION_FAILED,
    @SerialName("table_not_synced") TABLE_NOT_SYNCED,
    @SerialName("unknown_scope_effect") UNKNOWN_SCOPE_EFFECT,
    @SerialName("server_retryable") SERVER_RETRYABLE,
}

@Serializable
enum class VNextProtocolErrorCode {
    @SerialName("invalid_request") INVALID_REQUEST,
    @SerialName("upgrade_required") UPGRADE_REQUIRED,
    @SerialName("auth_required") AUTH_REQUIRED,
    @SerialName("schema_mismatch") SCHEMA_MISMATCH,
    @SerialName("retry_later") RETRY_LATER,
    @SerialName("temporary_unavailable") TEMPORARY_UNAVAILABLE,
}

@Serializable
enum class VNextCompositionClass {
    @SerialName("single_scope") SINGLE_SCOPE,
    @SerialName("multi_scope") MULTI_SCOPE,
}

@Serializable
data class VNextSchemaRef(
    val version: Long,
    val hash: String,
)

@Serializable
data class VNextScopeCursorRef(
    val cursor: String? = null,
)

@Serializable
data class VNextScopeAssignment(
    val id: String,
    val cursor: String? = null,
)

@Serializable
data class VNextScopeAssignmentDelta(
    val add: List<VNextScopeAssignment>,
    val remove: List<String>,
) {
    fun validate() {
        val added = mutableSetOf<String>()
        for (scope in add) {
            require(scope.id.isNotEmpty()) { "scope id must not be empty" }
            if (!added.add(scope.id)) {
                throw VNextContractException("duplicate added scope ${scope.id}")
            }
        }

        val removed = mutableSetOf<String>()
        for (scopeId in remove) {
            require(scopeId.isNotEmpty()) { "scope id must not be empty" }
            if (!removed.add(scopeId)) {
                throw VNextContractException("duplicate removed scope $scopeId")
            }
            if (added.contains(scopeId)) {
                throw VNextContractException("conflicting scope assignment $scopeId")
            }
        }
    }
}

@Serializable
data class VNextSchemaDescriptor(
    val version: Long,
    val hash: String,
    val action: VNextSchemaAction,
)

@Serializable
data class VNextColumnSchema(
    val name: String,
    @SerialName("type") val typeName: String,
    val nullable: Boolean,
)

@Serializable
data class VNextIndexSchema(
    val name: String,
    val columns: List<String>,
)

@Serializable
data class VNextTableSchema(
    val name: String,
    @SerialName("primary_key") val primaryKey: List<String>? = null,
    @SerialName("updated_at_column") val updatedAtColumn: String? = null,
    @SerialName("deleted_at_column") val deletedAtColumn: String? = null,
    val composition: VNextCompositionClass? = null,
    val columns: List<VNextColumnSchema>? = null,
    val indexes: List<VNextIndexSchema>? = null,
)

@Serializable
data class VNextSchemaManifest(
    val tables: List<VNextTableSchema>,
) {
    fun validate() {
        val tableNames = mutableSetOf<String>()
        for (table in tables) {
            require(table.name.isNotEmpty()) { "table name must not be empty" }
            if (!tableNames.add(table.name)) {
                throw VNextContractException("duplicate table ${table.name}")
            }

            val columnNames = mutableSetOf<String>()
            for (column in table.columns.orEmpty()) {
                require(column.name.isNotEmpty()) { "column name must not be empty for ${table.name}" }
                if (!columnNames.add(column.name)) {
                    throw VNextContractException("duplicate column ${table.name}.${column.name}")
                }
            }
            table.primaryKey?.forEach { columnName ->
                if (columnName !in columnNames) {
                    throw VNextContractException("unknown primary key column ${table.name}.$columnName")
                }
            }
            table.updatedAtColumn?.let { columnName ->
                if (columnName !in columnNames) {
                    throw VNextContractException("unknown updated_at column ${table.name}.$columnName")
                }
            }
            table.deletedAtColumn?.let { columnName ->
                if (columnName !in columnNames) {
                    throw VNextContractException("unknown deleted_at column ${table.name}.$columnName")
                }
            }

            val indexNames = mutableSetOf<String>()
            for (index in table.indexes.orEmpty()) {
                require(index.name.isNotEmpty()) { "index name must not be empty for ${table.name}" }
                if (!indexNames.add(index.name)) {
                    throw VNextContractException("duplicate index ${table.name}.${index.name}")
                }
                if (columnNames.isNotEmpty()) {
                    for (columnName in index.columns) {
                        if (!columnNames.contains(columnName)) {
                            throw VNextContractException("unknown index column ${table.name}.${index.name} -> $columnName")
                        }
                    }
                }
            }
        }
    }
}

@Serializable
data class VNextConnectRequest(
    @SerialName("client_id") val clientID: String,
    val platform: String,
    @SerialName("app_version") val appVersion: String,
    @SerialName("protocol_version") val protocolVersion: Int,
    val schema: VNextSchemaRef,
    @SerialName("scope_set_version") val scopeSetVersion: Long,
    @SerialName("known_scopes") val knownScopes: Map<String, VNextScopeCursorRef>,
)

@Serializable
data class VNextConnectResponse(
    @SerialName("server_time") val serverTime: String,
    @SerialName("protocol_version") val protocolVersion: Int,
    @SerialName("scope_set_version") val scopeSetVersion: Long,
    val schema: VNextSchemaDescriptor,
    val scopes: VNextScopeAssignmentDelta,
    @SerialName("schema_definition") val schemaDefinition: VNextSchemaManifest? = null,
) {
    fun validate() {
        if (schema.action.requiresSchemaDefinition() != (schemaDefinition != null)) {
            throw VNextContractException(
                "schema action ${schema.action} is inconsistent with schema_definition presence ${schemaDefinition != null}"
            )
        }
        scopes.validate()
        schemaDefinition?.validate()
    }
}

@Serializable
data class VNextMutation(
    @SerialName("mutation_id") val mutationID: String,
    val table: String,
    val op: VNextOperation,
    val pk: JsonObject,
    @SerialName("base_version") val baseVersion: String? = null,
    val columns: JsonObject? = null,
)

@Serializable
data class VNextPushRequest(
    @SerialName("client_id") val clientID: String,
    @SerialName("batch_id") val batchID: String,
    val schema: VNextSchemaRef,
    val mutations: List<VNextMutation>,
)

@Serializable
data class VNextAcceptedMutation(
    @SerialName("mutation_id") val mutationID: String,
    val table: String,
    val pk: JsonObject,
    val status: VNextMutationStatus,
    @SerialName("server_row") val serverRow: JsonObject? = null,
    @SerialName("server_version") val serverVersion: String,
)

@Serializable
data class VNextRejectedMutation(
    @SerialName("mutation_id") val mutationID: String,
    val table: String,
    val pk: JsonObject,
    val status: VNextMutationStatus,
    val code: VNextMutationRejectionCode,
    val message: String? = null,
    @SerialName("server_row") val serverRow: JsonObject? = null,
    @SerialName("server_version") val serverVersion: String? = null,
)

@Serializable
data class VNextPushResponse(
    @SerialName("server_time") val serverTime: String,
    val accepted: List<VNextAcceptedMutation>,
    val rejected: List<VNextRejectedMutation>,
)

@Serializable
data class VNextPullRequest(
    @SerialName("client_id") val clientID: String,
    val schema: VNextSchemaRef,
    @SerialName("scope_set_version") val scopeSetVersion: Long,
    val scopes: Map<String, VNextScopeCursorRef>,
    val limit: Int,
    @SerialName("checksum_mode") val checksumMode: VNextChecksumMode? = null,
)

@Serializable
data class VNextChangeRecord(
    val scope: String,
    val table: String,
    val op: VNextOperation,
    val pk: JsonObject,
    val row: JsonObject? = null,
    @SerialName("server_version") val serverVersion: String,
)

@Serializable
data class VNextPullResponse(
    val changes: List<VNextChangeRecord>,
    @SerialName("scope_set_version") val scopeSetVersion: Long,
    @SerialName("scope_cursors") val scopeCursors: Map<String, String>,
    @SerialName("scope_updates") val scopeUpdates: VNextScopeAssignmentDelta,
    val rebuild: List<String>,
    @SerialName("has_more") val hasMore: Boolean,
    val checksums: Map<String, String>? = null,
) {
    fun requestsRebuild(): Boolean = rebuild.isNotEmpty()

    fun validate(request: VNextPullRequest) {
        scopeUpdates.validate()
        if (request.checksumMode == VNextChecksumMode.REQUIRED && checksums == null) {
            throw VNextContractException("required checksums missing")
        }
    }
}

@Serializable
data class VNextRebuildRequest(
    @SerialName("client_id") val clientID: String,
    val scope: String,
    val cursor: String? = null,
    val limit: Int,
)

@Serializable
data class VNextRebuildRecord(
    val table: String,
    val pk: JsonObject,
    val row: JsonObject? = null,
    @SerialName("server_version") val serverVersion: String,
)

@Serializable
data class VNextRebuildResponse(
    val scope: String,
    val records: List<VNextRebuildRecord>,
    val cursor: String? = null,
    @SerialName("has_more") val hasMore: Boolean,
    @SerialName("final_scope_cursor") val finalScopeCursor: String? = null,
    val checksum: String? = null,
) {
    fun isFinalPage(): Boolean = !hasMore && finalScopeCursor != null

    fun validate() {
        if (hasMore) {
            if (finalScopeCursor != null) {
                throw VNextContractException("partial rebuild must not include final scope cursor")
            }
            if (checksum != null) {
                throw VNextContractException("partial rebuild must not include checksum")
            }
        } else {
            if (finalScopeCursor == null) {
                throw VNextContractException("final rebuild page must include final scope cursor")
            }
            if (checksum == null) {
                throw VNextContractException("final rebuild page must include checksum")
            }
        }
    }
}

@Serializable
data class VNextErrorBody(
    val code: VNextProtocolErrorCode,
    val message: String,
    val retryable: Boolean,
)

@Serializable
data class VNextErrorResponse(
    val error: VNextErrorBody,
)
