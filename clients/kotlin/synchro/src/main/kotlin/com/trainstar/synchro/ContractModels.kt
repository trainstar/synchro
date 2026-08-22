package com.trainstar.synchro

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject
import java.util.UUID

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
    @SerialName("replace") REPLACE,
    @SerialName("rebuild_local") REBUILD_LOCAL,
    @SerialName("unsupported") UNSUPPORTED;

    fun requiresSchemaDefinition(): Boolean = this == REPLACE || this == REBUILD_LOCAL
    fun requiresLocalRebuild(): Boolean = this == REBUILD_LOCAL
    fun isCompatible(): Boolean = this != UNSUPPORTED
}

@Serializable
enum class MutationStatus {
    @SerialName("applied") APPLIED,
    @SerialName("conflict") CONFLICT,
    @SerialName("rejected_terminal") REJECTED_TERMINAL,
}

@Serializable
enum class MutationRejectionCode {
    @SerialName("version_conflict") VERSION_CONFLICT,
    @SerialName("row_already_exists") ROW_ALREADY_EXISTS,
    @SerialName("row_deleted") ROW_DELETED,
    @SerialName("row_not_found") ROW_NOT_FOUND,
    @SerialName("schema_incompatible") SCHEMA_INCOMPATIBLE,
    @SerialName("policy_rejected") POLICY_REJECTED,
    @SerialName("validation_failed") VALIDATION_FAILED,
    @SerialName("table_not_synced") TABLE_NOT_SYNCED,
}

@Serializable
enum class ProtocolErrorCode {
    @SerialName("invalid_request") INVALID_REQUEST,
    @SerialName("invalid_schema_reference") INVALID_SCHEMA_REFERENCE,
    @SerialName("upgrade_required") UPGRADE_REQUIRED,
    @SerialName("auth_required") AUTH_REQUIRED,
    @SerialName("idempotency_conflict") IDEMPOTENCY_CONFLICT,
    @SerialName("client_retired") CLIENT_RETIRED,
    @SerialName("client_generation_expired") CLIENT_GENERATION_EXPIRED,
    @SerialName("rebuild_restart_required") REBUILD_RESTART_REQUIRED,
    @SerialName("schema_mismatch") SCHEMA_MISMATCH,
    @SerialName("retry_later") RETRY_LATER,
    @SerialName("sync_integrity_failure") SYNC_INTEGRITY_FAILURE,
    @SerialName("capture_pending") CAPTURE_PENDING,
    @SerialName("temporary_unavailable") TEMPORARY_UNAVAILABLE,
}

@Serializable
enum class SchemaUnsupportedReason {
    @SerialName("unknown_schema_lineage") UNKNOWN_SCHEMA_LINEAGE,
    @SerialName("incompatible_schema_transition") INCOMPATIBLE_SCHEMA_TRANSITION,
}

@Serializable
enum class TemporaryUnavailableReason {
    @SerialName("capture_blocked") CAPTURE_BLOCKED,
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
) {
    fun validate(allowFresh: Boolean = false) {
        if (allowFresh && version == 0L && hash.isEmpty()) return
        if (version <= 0L || version > 9_007_199_254_740_991L ||
            hash.length != 64 || hash.any { it !in '0'..'9' && it !in 'a'..'f' }
        ) {
            throw ContractException("invalid schema reference")
        }
    }
}

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
    val reason: SchemaUnsupportedReason? = null,
)

@Serializable
data class ChecksumObject(
    val algorithm: String,
    val version: Int,
    val encoding: String,
    val digest: String,
) {
    fun validate() {
        if (algorithm != "sha256" || version != 1 || encoding != "hex" ||
            digest.length != 64 || digest.any { it !in '0'..'9' && it !in 'a'..'f' }
        ) {
            throw ContractException("invalid checksum object")
        }
    }
}

@Serializable
data class ColumnSchema(
    @SerialName("field_id") val fieldID: String,
    val name: String,
    @SerialName("type") val typeName: String,
    val nullable: Boolean,
    val writable: Boolean,
    val precision: Int? = null,
    val scale: Int? = null,
)

@Serializable
data class IndexSchema(
    @SerialName("index_id") val indexID: String,
    val name: String,
    @SerialName("field_ids") val fieldIDs: List<String>,
    val unique: Boolean,
)

@Serializable
data class LifecycleSchema(
    @SerialName("created_at_field_id") val createdAtFieldID: String? = null,
    @SerialName("updated_at_field_id") val updatedAtFieldID: String? = null,
    @SerialName("deleted_at_field_id") val deletedAtFieldID: String? = null,
)

@Serializable
data class TableSchema(
    @SerialName("table_id") val tableID: String,
    @SerialName("relation_id") val relationID: String,
    val name: String,
    @SerialName("primary_key_field_id") val primaryKeyFieldID: String,
    val lifecycle: LifecycleSchema,
    val composition: CompositionClass,
    val fields: List<ColumnSchema>,
    val indexes: List<IndexSchema>,
)

@Serializable
data class SchemaManifest(
    @SerialName("schema_version") val schemaVersion: Long,
    @SerialName("schema_hash") val schemaHash: String,
    @SerialName("parent_schema") val parentSchema: SchemaRef? = null,
    @SerialName("transition_class") val transitionClass: String,
    @SerialName("compatibility_floor") val compatibilityFloor: Long,
    val tables: List<TableSchema>,
) {
    private val supportedFieldTypes: Set<String>
        get() = setOf(
            "string", "int", "int64", "decimal", "float", "boolean",
            "datetime", "date", "time", "json", "bytes",
        )

    fun validate() {
        SchemaRef(schemaVersion, schemaHash).validate()
        if (compatibilityFloor <= 0L || compatibilityFloor > 9_007_199_254_740_991L) {
            throw ContractException("invalid schema manifest compatibility floor")
        }
        when (transitionClass) {
            "initial" -> if (parentSchema != null || compatibilityFloor != schemaVersion) {
                throw ContractException("invalid initial schema manifest lineage")
            }
            "class_2" -> {
                val parent = parentSchema ?: throw ContractException("schema manifest parent is missing")
                parent.validate()
                if (parent.version >= schemaVersion || compatibilityFloor > parent.version) {
                    throw ContractException("invalid Class 2 schema manifest lineage")
                }
            }
            "class_3", "class_4" -> {
                val parent = parentSchema ?: throw ContractException("schema manifest parent is missing")
                parent.validate()
                if (parent.version >= schemaVersion || compatibilityFloor != schemaVersion) {
                    throw ContractException("invalid schema manifest lineage boundary")
                }
            }
            else -> throw ContractException("invalid schema manifest transition class")
        }
        if (tables.isEmpty()) throw ContractException("schema manifest contains no tables")

        val tableIDs = mutableSetOf<String>()
        val relationIDs = mutableSetOf<String>()
        val tableNames = mutableSetOf<String>()
        for (table in tables) {
            if (table.tableID.isEmpty() || table.relationID.isEmpty() || table.name.isEmpty()) {
                throw ContractException("schema manifest table identity must not be empty")
            }
            if (!tableIDs.add(table.tableID) || !relationIDs.add(table.relationID)) {
                throw ContractException("duplicate schema manifest table identity")
            }
            if (!tableNames.add(table.name)) {
                throw ContractException("duplicate table ${table.name}")
            }

            if (table.fields.isEmpty()) throw ContractException("table ${table.name} has no fields")
            val fieldIDs = mutableSetOf<String>()
            val fieldNames = mutableSetOf<String>()
            val fieldsByID = mutableMapOf<String, ColumnSchema>()
            for (field in table.fields) {
                if (field.fieldID.isEmpty() || field.name.isEmpty()) {
                    throw ContractException("field identity must not be empty for ${table.name}")
                }
                if (!fieldIDs.add(field.fieldID) || !fieldNames.add(field.name)) {
                    throw ContractException("duplicate field ${table.name}.${field.name}")
                }
                if (field.typeName !in supportedFieldTypes) {
                    throw ContractException("unsupported field type ${table.name}.${field.name}: ${field.typeName}")
                }
                if (field.typeName == "decimal") {
                    if (field.precision == null || field.scale == null ||
                        field.precision <= 0 || field.scale < 0 || field.scale > field.precision
                    ) {
                        throw ContractException("invalid decimal metadata for ${table.name}.${field.name}")
                    }
                } else if (field.precision != null || field.scale != null) {
                    throw ContractException("decimal metadata on non-decimal field ${table.name}.${field.name}")
                }
                fieldsByID[field.fieldID] = field
            }
            val primaryKey = fieldsByID[table.primaryKeyFieldID]
                ?: throw ContractException("unknown primary key field ${table.name}.${table.primaryKeyFieldID}")
            if (primaryKey.nullable || primaryKey.writable || primaryKey.typeName !in setOf("string", "int", "int64")) {
                throw ContractException("invalid primary key field for ${table.name}")
            }

            listOfNotNull(
                table.lifecycle.createdAtFieldID,
                table.lifecycle.updatedAtFieldID,
                table.lifecycle.deletedAtFieldID,
            ).forEach { fieldID ->
                val field = fieldsByID[fieldID]
                if (field == null || field.typeName != "datetime" || field.writable) {
                    throw ContractException("invalid lifecycle field for ${table.name}")
                }
            }

            val indexIDs = mutableSetOf<String>()
            val indexNames = mutableSetOf<String>()
            for (index in table.indexes) {
                if (index.indexID.isEmpty() || index.name.isEmpty() || index.fieldIDs.isEmpty()) {
                    throw ContractException("index identity must not be empty for ${table.name}")
                }
                if (!indexIDs.add(index.indexID) || !indexNames.add(index.name)) {
                    throw ContractException("duplicate index ${table.name}.${index.name}")
                }
                val indexedFields = mutableSetOf<String>()
                for (fieldID in index.fieldIDs) {
                    if (!fieldIDs.contains(fieldID)) {
                        throw ContractException("unknown index field ${table.name}.${index.name} -> $fieldID")
                    }
                    if (!indexedFields.add(fieldID)) {
                        throw ContractException("duplicate index field ${table.name}.${index.name} -> $fieldID")
                    }
                }
            }
        }
    }
}

@Serializable
data class ConnectRequest(
    @SerialName("client_id") val clientID: String,
    @SerialName("client_generation") val clientGeneration: Long? = null,
    val platform: String,
    @SerialName("app_version") val appVersion: String,
    @SerialName("protocol_version") val protocolVersion: Int,
    @SerialName("schema_reset") val schemaReset: Boolean? = null,
    val schema: SchemaRef,
    @SerialName("scope_set_version") val scopeSetVersion: Long,
    @SerialName("known_scopes") val knownScopes: Map<String, ScopeCursorRef>,
    @SerialName("seed_receipts") val seedReceipts: Map<String, String>? = null,
)

@Serializable
data class ConnectResponse(
    @SerialName("server_time") val serverTime: String,
    @SerialName("protocol_version") val protocolVersion: Int,
    @SerialName("client_generation") val clientGeneration: Long,
    @SerialName("scope_set_version") val scopeSetVersion: Long,
    val schema: SchemaDescriptor,
    val scopes: ScopeAssignmentDelta,
    @SerialName("scope_cursor_updates") val scopeCursorUpdates: Map<String, String?>,
    @SerialName("schema_definition") val schemaDefinition: SchemaManifest? = null,
    @SerialName("affected_scopes") val affectedScopes: List<String>? = null,
) {
    fun validate(
        existingScopes: Map<String, ScopeCursorRef>? = null,
        requestScopeSetVersion: Long? = null,
        seedReceiptScopes: Set<String> = emptySet(),
    ) {
        val existingScopeIDs = existingScopes?.keys
        if (protocolVersion != 3) {
            throw ContractException("unsupported protocol version $protocolVersion")
        }
        if (clientGeneration <= 0L || scopeSetVersion < 0L) {
            throw ContractException("invalid connect response counters")
        }
        SchemaRef(schema.version, schema.hash).validate()
        if (schema.action.requiresSchemaDefinition() != (schemaDefinition != null)) {
            throw ContractException(
                "schema action ${schema.action} is inconsistent with schema_definition presence ${schemaDefinition != null}"
            )
        }
        scopes.validate()
        schemaDefinition?.validate()
        if (schemaDefinition != null &&
            (schemaDefinition.schemaVersion != schema.version || schemaDefinition.schemaHash != schema.hash)
        ) {
            throw ContractException("schema definition does not match schema descriptor")
        }
        if (schemaDefinition != null && Integrity.schemaManifestHash(schemaDefinition) != schemaDefinition.schemaHash) {
            throw ContractException("schema definition hash does not match its canonical body")
        }

        if (requestScopeSetVersion != null) {
            val assignmentChanged = scopes.add.isNotEmpty() || scopes.remove.isNotEmpty()
            if (scopeSetVersion < requestScopeSetVersion ||
                (assignmentChanged && scopeSetVersion == requestScopeSetVersion)
            ) {
                throw ContractException(
                    "scope_set_version regressed from $requestScopeSetVersion to $scopeSetVersion"
                )
            }
        }

        if (existingScopeIDs != null) {
            val added = scopes.add.map { it.id }.toSet()
            val removed = scopes.remove.toSet()
            if (added.any { it in existingScopeIDs } || !existingScopeIDs.containsAll(removed)) {
                throw ContractException("connect assignment delta does not match existing scopes")
            }
            if (!existingScopeIDs.containsAll(seedReceiptScopes)) {
                throw ContractException("portable seed receipt does not identify a known scope")
            }
        }

        for (scopeID in scopeCursorUpdates.keys) {
            if (scopeID.isEmpty() || scopes.add.any { it.id == scopeID } || scopeID in scopes.remove) {
                throw ContractException("invalid scope cursor update $scopeID")
            }
            if (existingScopeIDs != null && scopeID !in existingScopeIDs) {
                throw ContractException("scope cursor update is not for an existing scope $scopeID")
            }
        }
        val removedScopes = scopes.remove.toSet()
        for (scopeID in seedReceiptScopes) {
            val hasCursorUpdate = scopeCursorUpdates.containsKey(scopeID)
            val isRemoved = scopeID in removedScopes
            if (hasCursorUpdate == isRemoved) {
                throw ContractException("portable seed scope $scopeID lacks one explicit resolution")
            }
        }
        if ((schema.action == SchemaAction.REPLACE || schema.action == SchemaAction.REBUILD_LOCAL) &&
            existingScopes != null
        ) {
            for ((scopeID, scope) in existingScopes) {
                if (scope.cursor != null && scopeID !in scopes.remove && !scopeCursorUpdates.containsKey(scopeID)) {
                    throw ContractException("missing scope cursor update for $scopeID")
                }
            }
        }
        when (schema.action) {
            SchemaAction.NONE, SchemaAction.REPLACE -> {
                if (affectedScopes != null || schema.reason != null) throw ContractException("invalid response shape for ${schema.action}")
            }
            SchemaAction.REBUILD_LOCAL -> {
                val affected = affectedScopes
                if (affected.isNullOrEmpty() || affected.any { it.isEmpty() } || affected.toSet().size != affected.size) {
                    throw ContractException("rebuild_local requires unique affected_scopes")
                }
                if (existingScopeIDs != null) {
                    val assigned = (existingScopeIDs - scopes.remove.toSet()) + scopes.add.map { it.id }
                    if (!affected.toSet().all { it in assigned }) {
                        throw ContractException("affected scope is not assigned")
                    }
                }
            }
            SchemaAction.UNSUPPORTED -> {
                if (affectedScopes != null || schema.reason == null) {
                    throw ContractException("unsupported schema action has invalid response shape")
                }
            }
        }
    }
}

@Serializable
data class Mutation(
    @SerialName("mutation_id") val mutationID: String,
    val table: String,
    val op: Operation,
    val pk: JsonObject,
    @SerialName("authored_schema") val authoredSchema: SchemaRef,
    @SerialName("base_version") val baseVersion: String? = null,
    @SerialName("client_version") val clientVersion: String,
    val columns: JsonObject? = null,
)

@Serializable
data class PushRequest(
    @SerialName("client_id") val clientID: String,
    @SerialName("client_generation") val clientGeneration: Long,
    @SerialName("batch_id") val batchID: String,
    val schema: SchemaRef,
    val mutations: List<Mutation>,
) {
    /**
     * Validates the immutable envelope and mutation shapes.
     * A retained authored table can be absent from the current schema.
     */
    fun validate(syncedTables: List<LocalSchemaTable>? = null) {
        if (clientID.isEmpty() || clientGeneration <= 0L || !isCanonicalUUID(batchID) || mutations.isEmpty()) {
            throw ContractException("invalid push envelope")
        }
        schema.validate()
        val tables = syncedTables?.associateBy { it.tableID }.orEmpty()
        val mutationIDs = mutableSetOf<String>()
        for (mutation in mutations) {
            if (mutation.table.isEmpty() || !isCanonicalUUID(mutation.mutationID) || !mutationIDs.add(mutation.mutationID)) {
                throw ContractException("invalid or duplicate mutation ID")
            }
            mutation.authoredSchema.validate()
            Integrity.validateCanonicalClientVersion(mutation.clientVersion)
            val table = tables[mutation.table]
            if (mutation.pk.size != 1 || mutation.pk.keys.singleOrNull().isNullOrEmpty()) {
                throw ContractException("invalid primary key for ${mutation.table}")
            }
            if (table != null) {
                if (mutation.pk.keys.single() != table.primaryKeyFieldID) {
                    throw ContractException("invalid primary key for ${mutation.table}")
                }
                val primaryKey = table.columns.singleOrNull { it.fieldID == table.primaryKeyFieldID }
                    ?: throw ContractException("primary key field is missing for ${mutation.table}")
                if (!validPrimaryKeyValue(mutation.pk.getValue(table.primaryKeyFieldID), primaryKey.logicalType)) {
                    throw ContractException("primary key has the wrong type")
                }
            }
            val columns = mutation.columns ?: JsonObject(emptyMap())
            if (table != null && mutation.authoredSchema == schema) {
                val writableIDs = table.columns.filter { it.writable }.map { it.fieldID }.toSet()
                if (!columns.keys.all { it in writableIDs }) {
                    throw ContractException("mutation contains an unknown or non-writable field")
                }
            }
            when (mutation.op) {
                Operation.INSERT -> if (mutation.baseVersion != null || columns.isEmpty()) {
                    throw ContractException("insert shape is invalid")
                }
                Operation.UPDATE -> if (mutation.baseVersion.isNullOrEmpty() || columns.isEmpty()) {
                    throw ContractException("update shape is invalid")
                }
                Operation.DELETE -> if (mutation.baseVersion.isNullOrEmpty() || mutation.columns != null) {
                    throw ContractException("delete shape is invalid")
                }
                Operation.UPSERT -> throw ContractException("upsert is not a push operation")
            }
        }
    }

    private fun isCanonicalUUID(value: String): Boolean =
        runCatching { UUID.fromString(value).toString() == value }.getOrDefault(false)

    private fun validPrimaryKeyValue(value: kotlinx.serialization.json.JsonElement, logicalType: String): Boolean {
        val primitive = value as? kotlinx.serialization.json.JsonPrimitive ?: return false
        return when (logicalType) {
            "string" -> primitive.isString
            "int" -> !primitive.isString && primitive.content.matches(Regex("0|-?[1-9][0-9]*")) && primitive.content.toIntOrNull() != null
            "int64" -> primitive.isString && primitive.content.matches(Regex("0|-?[1-9][0-9]*")) && primitive.content.toLongOrNull() != null
            else -> false
        }
    }
}

@Serializable
data class AcceptedMutation(
    @SerialName("mutation_id") val mutationID: String,
    val table: String,
    val pk: JsonObject,
    @SerialName("outcome_schema") val outcomeSchema: SchemaRef,
    val status: MutationStatus,
    @SerialName("server_row") val serverRow: JsonObject? = null,
    @SerialName("row_checksum") val rowChecksum: ChecksumObject? = null,
    @SerialName("server_version") val serverVersion: String,
)

@Serializable
data class RejectedMutation(
    @SerialName("mutation_id") val mutationID: String,
    val table: String,
    val pk: JsonObject,
    @SerialName("outcome_schema") val outcomeSchema: SchemaRef,
    val status: MutationStatus,
    val code: MutationRejectionCode,
    val message: String,
    val retryable: Boolean? = null,
    @SerialName("server_row") val serverRow: JsonObject? = null,
    @SerialName("row_checksum") val rowChecksum: ChecksumObject? = null,
    @SerialName("server_version") val serverVersion: String? = null,
    @SerialName("authored_schema") val authoredSchema: SchemaRef? = null,
    @SerialName("current_schema") val currentSchema: SchemaRef? = null,
    @SerialName("incompatible_field_ids") val incompatibleFieldIDs: List<String>? = null,
)

@Serializable
data class PushResponse(
    @SerialName("batch_id") val batchID: String,
    @SerialName("server_time") val serverTime: String,
    val accepted: List<AcceptedMutation>,
    val rejected: List<RejectedMutation>,
) {
    fun validate(request: PushRequest? = null) {
        val mutationIDs = mutableSetOf<String>()
        accepted.forEach { outcome ->
            validateAcceptedShape(outcome)
            if (!mutationIDs.add(outcome.mutationID)) throw ContractException("invalid accepted mutation outcome")
        }
        rejected.forEach { outcome ->
            validateRejectedShape(outcome)
            if (!mutationIDs.add(outcome.mutationID)) throw ContractException("invalid rejected mutation outcome")
        }
        if (request != null) {
            request.validate()
            if (batchID != request.batchID) throw ContractException("push response batch ID does not match request")
            val requested = request.mutations.associateBy { it.mutationID }
            if (mutationIDs != requested.keys) {
                throw ContractException("push response IDs do not exactly match request IDs")
            }
            val expectedAccepted = mutableListOf<String>()
            val expectedRejected = mutableListOf<String>()
            request.mutations.forEach { mutation ->
                val acceptedOutcome = accepted.firstOrNull { it.mutationID == mutation.mutationID }
                val rejectedOutcome = rejected.firstOrNull { it.mutationID == mutation.mutationID }
                when {
                    acceptedOutcome != null && rejectedOutcome == null -> {
                        validateOutcome(acceptedOutcome, mutation)
                        expectedAccepted += mutation.mutationID
                    }
                    rejectedOutcome != null && acceptedOutcome == null -> {
                        validateOutcome(rejectedOutcome, mutation)
                        expectedRejected += mutation.mutationID
                    }
                    else -> throw ContractException("push response outcome partition is invalid")
                }
            }
            if (accepted.map { it.mutationID } != expectedAccepted || rejected.map { it.mutationID } != expectedRejected) {
                throw ContractException("push response does not preserve request-relative outcome order")
            }
        }
    }

    private fun validateAcceptedShape(outcome: AcceptedMutation) {
        if (!isCanonicalUUID(outcome.mutationID) || outcome.table.isEmpty() || outcome.pk.size != 1 ||
            outcome.pk.keys.singleOrNull().isNullOrEmpty() || outcome.status != MutationStatus.APPLIED ||
            outcome.serverVersion.isEmpty()
        ) {
            throw ContractException("invalid accepted mutation outcome")
        }
        outcome.outcomeSchema.validate()
        outcome.rowChecksum?.validate()
        if ((outcome.serverRow == null) != (outcome.rowChecksum == null)) {
            throw ContractException("accepted row and checksum must be paired")
        }
    }

    private fun validateOutcome(outcome: AcceptedMutation, request: Mutation) {
        if (outcome.table != request.table || outcome.pk != request.pk) {
            throw ContractException("accepted outcome does not match request")
        }
        val hasRow = outcome.serverRow != null
        val hasChecksum = outcome.rowChecksum != null
        when (request.op) {
            Operation.INSERT, Operation.UPDATE -> if (!hasRow || !hasChecksum) {
                throw ContractException("accepted insert or update lacks its row or checksum")
            }
            Operation.DELETE -> if (hasRow != hasChecksum) {
                throw ContractException("accepted delete row and checksum must be paired")
            }
            Operation.UPSERT -> throw ContractException("accepted outcome targets an unsupported push operation")
        }
    }

    private fun validateRejectedShape(outcome: RejectedMutation) {
        if (!isCanonicalUUID(outcome.mutationID) || outcome.table.isEmpty() || outcome.pk.size != 1 ||
            outcome.pk.keys.singleOrNull().isNullOrEmpty() || outcome.message.isEmpty()
        ) {
            throw ContractException("invalid rejected mutation outcome")
        }
        outcome.outcomeSchema.validate()
        outcome.rowChecksum?.validate()
        val conflictCodes = setOf(
            MutationRejectionCode.VERSION_CONFLICT,
            MutationRejectionCode.ROW_ALREADY_EXISTS,
            MutationRejectionCode.ROW_DELETED,
            MutationRejectionCode.ROW_NOT_FOUND,
        )
        val terminalCodes = setOf(
            MutationRejectionCode.SCHEMA_INCOMPATIBLE,
            MutationRejectionCode.POLICY_REJECTED,
            MutationRejectionCode.VALIDATION_FAILED,
            MutationRejectionCode.TABLE_NOT_SYNCED,
        )
        when (outcome.status) {
            MutationStatus.CONFLICT -> {
                if (outcome.code !in conflictCodes || outcome.retryable != null ||
                    outcome.authoredSchema != null || outcome.currentSchema != null || outcome.incompatibleFieldIDs != null
                ) {
                    throw ContractException("conflict outcome has invalid fields")
                }
                if ((outcome.serverRow == null) != (outcome.rowChecksum == null)) {
                    throw ContractException("conflict row and checksum must be paired")
                }
                if (outcome.serverRow != null && outcome.serverVersion.isNullOrEmpty()) {
                    throw ContractException("conflict row has no server version")
                }
                if (outcome.serverVersion?.isEmpty() == true) {
                    throw ContractException("conflict outcome has an empty server version")
                }
            }
            MutationStatus.REJECTED_TERMINAL -> {
                if (outcome.code !in terminalCodes || outcome.serverRow != null || outcome.rowChecksum != null ||
                    outcome.serverVersion != null
                ) {
                    throw ContractException("terminal outcome contains invalid authoritative metadata")
                }
                if (outcome.code == MutationRejectionCode.SCHEMA_INCOMPATIBLE) {
                    if (outcome.retryable != false || outcome.authoredSchema == null || outcome.currentSchema == null ||
                        outcome.incompatibleFieldIDs == null
                    ) {
                        throw ContractException("schema incompatible outcome is incomplete")
                    }
                    outcome.authoredSchema.validate()
                    outcome.currentSchema.validate()
                    validateIncompatibleFieldIDs(outcome.incompatibleFieldIDs)
                } else if (outcome.retryable != null || outcome.authoredSchema != null || outcome.currentSchema != null ||
                    outcome.incompatibleFieldIDs != null
                ) {
                    throw ContractException("terminal outcome has schema-incompatible fields")
                }
            }
            MutationStatus.APPLIED -> throw ContractException("rejected outcome has applied status")
        }
    }

    private fun validateOutcome(outcome: RejectedMutation, request: Mutation) {
        if (outcome.table != request.table || outcome.pk != request.pk) {
            throw ContractException("rejected outcome does not match request")
        }
        if (outcome.code == MutationRejectionCode.SCHEMA_INCOMPATIBLE) {
            if (outcome.authoredSchema != request.authoredSchema || outcome.currentSchema != outcome.outcomeSchema) {
                throw ContractException("schema incompatible outcome does not bind the request")
            }
            if (outcome.incompatibleFieldIDs.isNullOrEmpty() && request.op != Operation.DELETE) {
                throw ContractException("schema incompatible outcome has no incompatible fields")
            }
        }
    }

    private fun validateIncompatibleFieldIDs(fieldIDs: List<String>) {
        var previous: ByteArray? = null
        fieldIDs.forEach { fieldID ->
            if (fieldID.isEmpty() || !Integrity.isValidText(fieldID)) {
                throw ContractException("schema incompatible outcome has an invalid field ID")
            }
            val current = fieldID.toByteArray(Charsets.UTF_8)
            if (previous != null && compareUnsignedUTF8(previous!!, current) >= 0) {
                throw ContractException("schema incompatible field IDs are not unsigned-UTF8 sorted")
            }
            previous = current
        }
    }

    private fun compareUnsignedUTF8(left: ByteArray, right: ByteArray): Int {
        val count = minOf(left.size, right.size)
        for (index in 0 until count) {
            val comparison = (left[index].toInt() and 0xff).compareTo(right[index].toInt() and 0xff)
            if (comparison != 0) return comparison
        }
        return left.size.compareTo(right.size)
    }

    private fun isCanonicalUUID(value: String): Boolean =
        runCatching { UUID.fromString(value).toString() == value }.getOrDefault(false)
}

@Serializable
data class PullRequest(
    @SerialName("client_id") val clientID: String,
    @SerialName("client_generation") val clientGeneration: Long,
    val schema: SchemaRef,
    @SerialName("scope_set_version") val scopeSetVersion: Long,
    val scopes: Map<String, ScopeCursorRef>,
    val limit: Int,
)

@Serializable
data class ChangeRecord(
    val scope: String,
    val table: String,
    val op: Operation,
    val pk: JsonObject,
    val row: JsonObject? = null,
    @SerialName("row_checksum") val rowChecksum: ChecksumObject? = null,
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
    val checksums: Map<String, ChecksumObject>? = null,
) {
    fun requestsRebuild(): Boolean = rebuild.isNotEmpty()

    fun validate() {
        scopeUpdates.validate()
        changes.forEach { change ->
            if (change.op != Operation.UPSERT && change.op != Operation.DELETE) {
                throw ContractException("invalid pull operation ${change.op}")
            }
        }
        if (hasMore && checksums != null) {
            throw ContractException("nonterminal pull page must omit checksums")
        }
        if (!hasMore && checksums == null) {
            throw ContractException("final pull page must include checksums")
        }
    }

    fun validate(activeScopes: Set<String>, requestScopeSetVersion: Long? = null) {
        validate()
        if (requestScopeSetVersion != null) {
            val assignmentChanged = scopeUpdates.add.isNotEmpty() || scopeUpdates.remove.isNotEmpty()
            if (scopeSetVersion < requestScopeSetVersion ||
                (assignmentChanged && scopeSetVersion == requestScopeSetVersion)
            ) {
                throw ContractException(
                    "scope_set_version regressed from $requestScopeSetVersion to $scopeSetVersion"
                )
            }
        }
        val added = scopeUpdates.add.map { it.id }.toSet()
        val removed = scopeUpdates.remove.toSet()
        if (!activeScopes.containsAll(removed) || added.any(activeScopes::contains) ||
            scopeUpdates.add.any { it.cursor != null }
        ) {
            throw ContractException("pull assignment delta does not match active scopes")
        }

        val expectedScopes = (activeScopes - removed) + added
        val rebuildScopes = rebuild.toSet()
        if (changes.any { it.scope !in expectedScopes } ||
            scopeCursors.keys.any { it !in expectedScopes } ||
            rebuildScopes.any { it !in expectedScopes } ||
            !rebuildScopes.containsAll(added)
        ) {
            throw ContractException("pull members do not match assigned scopes")
        }
        if (!hasMore && checksums?.keys != expectedScopes) {
            throw ContractException("terminal pull checksum map does not match active scopes")
        }
        if (scopeCursors.keys.any { it in rebuildScopes }) {
            throw ContractException("rebuild scope received an incremental cursor")
        }
    }
}

@Serializable
data class RebuildRequest(
    @SerialName("client_id") val clientID: String,
    @SerialName("client_generation") val clientGeneration: Long,
    val schema: SchemaRef,
    val scope: String,
    @SerialName("rebuild_id") val rebuildID: String,
    val cursor: String? = null,
    val limit: Int,
)

@Serializable
data class RebuildRecord(
    val table: String,
    val pk: JsonObject,
    val row: JsonObject,
    @SerialName("row_checksum") val rowChecksum: ChecksumObject,
    @SerialName("server_version") val serverVersion: String,
)

@Serializable
data class RebuildResponse(
    val scope: String,
    val records: List<RebuildRecord>,
    val cursor: String? = null,
    @SerialName("has_more") val hasMore: Boolean,
    @SerialName("final_scope_cursor") val finalScopeCursor: String? = null,
    val checksum: ChecksumObject? = null,
) {
    fun isFinalPage(): Boolean = !hasMore && finalScopeCursor != null

    fun validate() {
        if (scope.isEmpty()) {
            throw ContractException("rebuild response scope must be nonempty")
        }
        if (hasMore) {
            if (cursor == null) {
                throw ContractException("partial rebuild must include a cursor")
            }
            if (finalScopeCursor != null) {
                throw ContractException("partial rebuild must not include final scope cursor")
            }
            if (checksum != null) {
                throw ContractException("partial rebuild must not include checksum")
            }
        } else {
            if (cursor != null) {
                throw ContractException("final rebuild must omit its continuation cursor")
            }
            if (finalScopeCursor == null) {
                throw ContractException("final rebuild page must include final scope cursor")
            }
            if (checksum == null) {
                throw ContractException("final rebuild page must include checksum")
            }
        }
    }

    fun validate(request: RebuildRequest) {
        validate()
        if (scope != request.scope) {
            throw ContractException("rebuild response scope does not match request scope")
        }
    }
}

@Serializable
data class ErrorBody(
    val code: ProtocolErrorCode,
    val message: String,
    val retryable: Boolean,
    @SerialName("current_schema") val currentSchema: SchemaRef? = null,
    @SerialName("received_schema") val receivedSchema: SchemaRef? = null,
    @SerialName("current_client_generation") val currentClientGeneration: Long? = null,
    @SerialName("scope_id") val scopeID: String? = null,
    @SerialName("required_protocol_version") val requiredProtocolVersion: Int? = null,
    @SerialName("received_protocol_version") val receivedProtocolVersion: Int? = null,
    @SerialName("minimum_client_version") val minimumClientVersion: String? = null,
    @SerialName("received_client_version") val receivedClientVersion: String? = null,
    val reason: TemporaryUnavailableReason? = null,
    val field: String? = null,
    val minimum: Long? = null,
    val maximum: Long? = null,
)

@Serializable
data class ErrorResponse(
    val error: ErrorBody,
)
