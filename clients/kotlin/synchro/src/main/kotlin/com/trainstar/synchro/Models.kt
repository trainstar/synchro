package com.trainstar.synchro

import kotlinx.serialization.*
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.*

// MARK: - Push

@Serializable
data class PushRecord(
    val id: String,
    @SerialName("table_name") val tableName: String,
    val operation: String,
    val data: Map<String, @Serializable(with = AnyCodableSerializer::class) AnyCodable>? = null,
    @SerialName("client_updated_at") val clientUpdatedAt: String,
    @SerialName("base_updated_at") val baseUpdatedAt: String? = null,
    @Transient val localRevision: Long = 0,
)

// MARK: - Schema

@Serializable
data class SchemaResponse(
    @SerialName("schema_version") val schemaVersion: Long,
    @SerialName("schema_hash") val schemaHash: String,
    @SerialName("server_time") val serverTime: String,
    val manifest: SchemaManifest
) {
    fun localTables(): List<LocalSchemaTable> = manifest.localTables()
}

// MARK: - Table Meta

// MARK: - SDK Types

/** The exact protocol 3 native-client lifecycle vocabulary. */
enum class SyncLifecycleState(val wireName: String) {
    UNINITIALIZED("uninitialized"),
    LOCAL_READY("local_ready"),
    CONNECTING("connecting"),
    SCHEMA_APPLYING("schema_applying"),
    READY("ready"),
    PUSHING("pushing"),
    PULLING("pulling"),
    REBUILDING("rebuilding"),
    BACKOFF("backoff"),
    ERROR("error"),
    STOPPED("stopped"),
}

/** A bounded, stable diagnostic for a durable client failure. */
enum class SyncOperationKind(val wireName: String) {
    OPENING("opening"),
    CONNECTING("connecting"),
    SCHEMA("schema"),
    PUSHING("pushing"),
    PULLING("pulling"),
    REBUILDING("rebuilding"),
    LIFECYCLE("lifecycle"),
    DATABASE("database");

    companion object {
        fun fromWireName(value: String): SyncOperationKind? =
            entries.firstOrNull { it.wireName == value }
    }
}

enum class SyncRecoveryAction(val wireName: String) {
    RETRY("retry"),
    SCHEMA_RESET("schema_reset"),
    NONE("none");

    companion object {
        fun fromWireName(value: String): SyncRecoveryAction? =
            entries.firstOrNull { it.wireName == value }
    }
}

enum class SyncFailureCode(val wireName: String) {
    AUTHENTICATION_REQUIRED("auth_required"),
    CLIENT_RETIRED("client_retired"),
    IDEMPOTENCY_CONFLICT("idempotency_conflict"),
    INVALID_REQUEST("invalid_request"),
    INVALID_RESPONSE("invalid_response"),
    INVALID_SCHEMA_REFERENCE("invalid_schema_reference"),
    INVALID_STATE_TRANSITION("invalid_state_transition"),
    LOCAL_DATABASE("local_database"),
    SCHEMA_APPLICATION_FAILED("schema_application_failed"),
    SYNC_INTEGRITY_FAILURE("sync_integrity_failure"),
    UNSUPPORTED_SCHEMA("unsupported_schema"),
    UPGRADE_REQUIRED("upgrade_required"),
    SCHEMA_MISMATCH("schema_mismatch"),
    SERVER_ERROR("server_error"),
    NETWORK_ERROR("network_error"),
    DATABASE_ERROR("database_error"),
    LOCAL_FAILURE("local_failure");

    companion object {
        fun fromWireName(value: String): SyncFailureCode? =
            entries.firstOrNull { it.wireName == value }
    }
}

data class SyncFailure(
    val operation: SyncOperationKind,
    val code: SyncFailureCode,
    val retryable: Boolean,
    val message: String,
    val recoveryAction: SyncRecoveryAction,
    val metadata: Map<String, String> = emptyMap(),
) {
    init {
        require(message.isNotEmpty() && message.length <= 256) {
            "sync failure message is invalid"
        }
        require(metadata.size <= 8 && metadata.all { (key, value) ->
            key.isNotEmpty() && key.length <= 64 && value.length <= 128
        }) {
            "sync failure metadata is invalid"
        }
    }
}

sealed class SyncStatus(open val state: SyncLifecycleState) {
    data object Uninitialized : SyncStatus(SyncLifecycleState.UNINITIALIZED)
    data object LocalReady : SyncStatus(SyncLifecycleState.LOCAL_READY)
    data object Connecting : SyncStatus(SyncLifecycleState.CONNECTING)
    data object SchemaApplying : SyncStatus(SyncLifecycleState.SCHEMA_APPLYING)
    data object Ready : SyncStatus(SyncLifecycleState.READY)
    data object Pushing : SyncStatus(SyncLifecycleState.PUSHING)
    data object Pulling : SyncStatus(SyncLifecycleState.PULLING)
    data object Rebuilding : SyncStatus(SyncLifecycleState.REBUILDING)
    data class Backoff(
        val retryAt: java.time.Instant,
        val operation: String,
    ) : SyncStatus(SyncLifecycleState.BACKOFF)

    data class Error(val failure: SyncFailure) : SyncStatus(SyncLifecycleState.ERROR)

    data object Stopped : SyncStatus(SyncLifecycleState.STOPPED)
}

data class SyncStateChangeEvent(
    val from: SyncLifecycleState,
    val to: SyncLifecycleState,
)

data class SyncBackoffEvent(
    val operation: SyncOperationKind,
    val attempt: Long,
    val retryAt: java.time.Instant,
)

data class SyncSchemaEvent(
    val source: SchemaRef,
    val target: SchemaRef,
    val action: SchemaAction,
)

data class SyncMutationEvent(
    val mutationID: String,
    val tableID: String,
    val status: MutationStatus,
    val rejectionCode: MutationRejectionCode?,
)

data class SyncRebuildEvent(
    val scopeID: String,
    val rebuildID: String,
)

sealed interface SyncEvent {
    data class StateChanged(val change: SyncStateChangeEvent) : SyncEvent

    data class Backoff(val backoff: SyncBackoffEvent) : SyncEvent

    data class SchemaApplying(val schema: SyncSchemaEvent) : SyncEvent

    data class SchemaApplied(val schema: SyncSchemaEvent) : SyncEvent

    data class MutationAccepted(val mutation: SyncMutationEvent) : SyncEvent

    data class MutationRejected(val mutation: SyncMutationEvent) : SyncEvent

    data class RebuildRequested(val rebuild: SyncRebuildEvent) : SyncEvent

    data class RebuildCompleted(val rebuild: SyncRebuildEvent) : SyncEvent

    data class Failure(val failure: SyncFailure) : SyncEvent
}

enum class LocalMutationStatus {
    PENDING,
    SEALED,
    SUPERSEDED_BEFORE_SEND,
    CANCELLED_BEFORE_SEND,
    BLOCKED_BY_PREDECESSOR,
}

data class AuthoredMutationField(
    val fieldID: String,
    val logicalType: String,
    val value: AnyCodable,
)

data class PendingMutationInspection(
    val mutationID: String,
    val localOrder: Long,
    val tableID: String,
    val tableName: String,
    val recordID: String,
    val primaryKeyFieldID: String,
    val primaryKeyLogicalType: String,
    val operation: Operation,
    val authoredSchema: SchemaRef,
    val baseVersion: String?,
    val clientVersion: String,
    val status: LocalMutationStatus,
    val sourceKind: String,
    val dependsOnMutationID: String?,
    val normalizedMutationID: String?,
    val sealedBatchID: String?,
    val sealedOrdinal: Int?,
    val authoredFields: List<AuthoredMutationField>,
)

data class RejectedMutationInspection(
    val mutationID: String,
    val tableName: String,
    val recordID: String,
    val status: MutationStatus,
    val code: MutationRejectionCode,
    val message: String?,
    val serverRowJSON: String?,
    val serverVersion: String?,
    val mutationJSON: String,
    val rejectionJSON: String,
    val createdAt: String,
    val updatedAt: String,
)

data class ConflictEvent(
    val table: String,
    val recordID: String,
    val clientData: Map<String, AnyCodable>?,
    val serverData: Map<String, AnyCodable>?
)

data class ExecResult(val rowsAffected: Int)

data class SQLStatement(
    val sql: String,
    val params: Array<out Any?>? = null
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SQLStatement) return false
        if (sql != other.sql) return false
        if (params == null && other.params == null) return true
        if (params == null || other.params == null) return false
        return params.contentEquals(other.params)
    }

    override fun hashCode(): Int {
        var result = sql.hashCode()
        result = 31 * result + (params?.contentHashCode() ?: 0)
        return result
    }
}

data class ColumnDef(
    val name: String,
    val type: String,
    val nullable: Boolean = true,
    val defaultValue: String? = null,
    val primaryKey: Boolean = false
)

data class TableOptions(
    val ifNotExists: Boolean = true,
    val withoutRowid: Boolean = false
)

enum class CheckpointMode {
    PASSIVE, FULL, RESTART, TRUNCATE
}

fun interface Cancellable {
    fun cancel()
}

// MARK: - AnyCodable

class AnyCodable(val value: Any?) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is AnyCodable) return false
        return when {
            value == null && other.value == null -> true
            value is Boolean && other.value is Boolean -> value == other.value
            value is Number && other.value is Number -> {
                // Compare via Double for fractional values, via Long for integers.
                // Both checks required: toLong truncates fractions, toDouble loses precision on large longs.
                if (value is Double || value is Float || other.value is Double || other.value is Float) {
                    value.toDouble() == other.value.toDouble()
                } else {
                    value.toLong() == other.value.toLong()
                }
            }
            value is String && other.value is String -> value == other.value
            value is List<*> && other.value is List<*> -> value == other.value
            value is Map<*, *> && other.value is Map<*, *> -> value == other.value
            else -> false
        }
    }

    override fun hashCode(): Int = value?.hashCode() ?: 0

    override fun toString(): String = "AnyCodable($value)"
}

object AnyCodableSerializer : KSerializer<AnyCodable> {
    override val descriptor: SerialDescriptor =
        PrimitiveSerialDescriptor("AnyCodable", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: AnyCodable) {
        val jsonEncoder = encoder as? JsonEncoder
            ?: throw SerializationException("AnyCodable can only be serialized with JSON")
        val element = toJsonElement(value.value)
        jsonEncoder.encodeJsonElement(element)
    }

    override fun deserialize(decoder: Decoder): AnyCodable {
        val jsonDecoder = decoder as? JsonDecoder
            ?: throw SerializationException("AnyCodable can only be deserialized with JSON")
        val element = jsonDecoder.decodeJsonElement()
        return AnyCodable(fromJsonElement(element))
    }

    private fun toJsonElement(value: Any?): JsonElement = when (value) {
        null -> JsonNull
        is Boolean -> JsonPrimitive(value)
        is Int -> JsonPrimitive(value)
        is Long -> JsonPrimitive(value)
        is Double -> JsonPrimitive(value)
        is Float -> JsonPrimitive(value.toDouble())
        is String -> JsonPrimitive(value)
        is List<*> -> JsonArray(value.map { toJsonElement(it) })
        is Map<*, *> -> JsonObject(value.entries.associate { (k, v) ->
            k.toString() to toJsonElement(v)
        })
        is AnyCodable -> toJsonElement(value.value)
        else -> JsonPrimitive(value.toString())
    }

    private fun fromJsonElement(element: JsonElement): Any? = when (element) {
        is JsonNull -> null
        is JsonPrimitive -> {
            when {
                element.isString -> element.content
                element.content == "true" || element.content == "false" -> element.boolean
                element.content.contains('.') -> element.double
                else -> element.long
            }
        }
        is JsonArray -> element.map { fromJsonElement(it) }
        is JsonObject -> element.mapValues { fromJsonElement(it.value) }
    }
}
