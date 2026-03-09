package com.trainstar.synchro

import kotlinx.serialization.*
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.*

// MARK: - Register

@Serializable
data class RegisterRequest(
    @SerialName("client_id") val clientID: String,
    @SerialName("client_name") val clientName: String? = null,
    val platform: String,
    @SerialName("app_version") val appVersion: String,
    @SerialName("schema_version") val schemaVersion: Long,
    @SerialName("schema_hash") val schemaHash: String
)

@Serializable
data class RegisterResponse(
    val id: String,
    @SerialName("server_time") val serverTime: String,
    @SerialName("last_sync_at") val lastSyncAt: String? = null,
    val checkpoint: Long,
    @SerialName("schema_version") val schemaVersion: Long,
    @SerialName("schema_hash") val schemaHash: String
)

// MARK: - Pull

@Serializable
data class PullRequest(
    @SerialName("client_id") val clientID: String,
    val checkpoint: Long,
    val tables: List<String>? = null,
    val limit: Int? = null,
    @SerialName("known_buckets") val knownBuckets: List<String>? = null,
    @SerialName("schema_version") val schemaVersion: Long,
    @SerialName("schema_hash") val schemaHash: String
)

@Serializable
data class PullResponse(
    val changes: List<Record>,
    val deletes: List<DeleteEntry>,
    val checkpoint: Long,
    @SerialName("has_more") val hasMore: Boolean,
    @SerialName("snapshot_required") val snapshotRequired: Boolean? = null,
    @SerialName("snapshot_reason") val snapshotReason: String? = null,
    @SerialName("bucket_updates") val bucketUpdates: BucketUpdate? = null,
    @SerialName("schema_version") val schemaVersion: Long,
    @SerialName("schema_hash") val schemaHash: String
)

@Serializable
data class Record(
    val id: String,
    @SerialName("table_name") val tableName: String,
    val data: Map<String, @Serializable(with = AnyCodableSerializer::class) AnyCodable>,
    @SerialName("updated_at") val updatedAt: String,
    @SerialName("deleted_at") val deletedAt: String? = null
)

@Serializable
data class DeleteEntry(
    val id: String,
    @SerialName("table_name") val tableName: String
)

@Serializable
data class BucketUpdate(
    val added: List<String>? = null,
    val removed: List<String>? = null
)

// MARK: - Push

@Serializable
data class PushRequest(
    @SerialName("client_id") val clientID: String,
    val changes: List<PushRecord>,
    @SerialName("schema_version") val schemaVersion: Long,
    @SerialName("schema_hash") val schemaHash: String
)

@Serializable
data class PushRecord(
    val id: String,
    @SerialName("table_name") val tableName: String,
    val operation: String,
    val data: Map<String, @Serializable(with = AnyCodableSerializer::class) AnyCodable>? = null,
    @SerialName("client_updated_at") val clientUpdatedAt: String,
    @SerialName("base_updated_at") val baseUpdatedAt: String? = null
)

@Serializable
data class PushResponse(
    val accepted: List<PushResult>,
    val rejected: List<PushResult>,
    val checkpoint: Long,
    @SerialName("server_time") val serverTime: String,
    @SerialName("schema_version") val schemaVersion: Long,
    @SerialName("schema_hash") val schemaHash: String
)

@Serializable
data class PushResult(
    val id: String,
    @SerialName("table_name") val tableName: String,
    val operation: String,
    val status: String,
    @SerialName("reason_code") val reasonCode: String? = null,
    val message: String? = null,
    @SerialName("server_version") val serverVersion: Record? = null,
    @SerialName("server_updated_at") val serverUpdatedAt: String? = null,
    @SerialName("server_deleted_at") val serverDeletedAt: String? = null
) {
    constructor(
        id: String,
        tableName: String,
        operation: String,
        status: String,
        reason: String? = null,
        serverVersion: Record? = null,
        serverUpdatedAt: String? = null,
        serverDeletedAt: String? = null
    ) : this(
        id = id,
        tableName = tableName,
        operation = operation,
        status = status,
        reasonCode = null,
        message = reason,
        serverVersion = serverVersion,
        serverUpdatedAt = serverUpdatedAt,
        serverDeletedAt = serverDeletedAt
    )
}

// MARK: - Snapshot

@Serializable
data class SnapshotRequest(
    @SerialName("client_id") val clientID: String,
    val cursor: SnapshotCursor? = null,
    val limit: Int? = null,
    @SerialName("schema_version") val schemaVersion: Long,
    @SerialName("schema_hash") val schemaHash: String
)

@Serializable
data class SnapshotCursor(
    val checkpoint: Long,
    @SerialName("table_idx") val tableIndex: Int,
    @SerialName("after_id") val afterID: String
)

@Serializable
data class SnapshotResponse(
    val records: List<Record>,
    val cursor: SnapshotCursor? = null,
    val checkpoint: Long,
    @SerialName("has_more") val hasMore: Boolean,
    @SerialName("schema_version") val schemaVersion: Long,
    @SerialName("schema_hash") val schemaHash: String
)

// MARK: - Schema

@Serializable
data class SchemaResponse(
    @SerialName("schema_version") val schemaVersion: Long,
    @SerialName("schema_hash") val schemaHash: String,
    @SerialName("server_time") val serverTime: String,
    val tables: List<SchemaTable>
)

@Serializable
data class SchemaTable(
    @SerialName("table_name") val tableName: String,
    @SerialName("push_policy") val pushPolicy: String,
    @SerialName("parent_table") val parentTable: String? = null,
    @SerialName("parent_fk_col") val parentFKCol: String? = null,
    val dependencies: List<String>? = null,
    @SerialName("updated_at_column") val updatedAtColumn: String,
    @SerialName("deleted_at_column") val deletedAtColumn: String,
    @SerialName("primary_key") val primaryKey: List<String>,
    @SerialName("bucket_by_column") val bucketByColumn: String? = null,
    @SerialName("bucket_prefix") val bucketPrefix: String? = null,
    @SerialName("global_when_bucket_null") val globalWhenBucketNull: Boolean? = null,
    @SerialName("allow_global_read") val allowGlobalRead: Boolean? = null,
    @SerialName("bucket_function") val bucketFunction: String? = null,
    val columns: List<SchemaColumn>
)

@Serializable
data class SchemaColumn(
    val name: String,
    @SerialName("db_type") val dbType: String,
    @SerialName("logical_type") val logicalType: String,
    val nullable: Boolean,
    @SerialName("default_sql") val defaultSQL: String? = null,
    @SerialName("default_kind") val defaultKind: String = "none",
    @SerialName("sqlite_default_sql") val sqliteDefaultSQL: String? = null,
    @SerialName("is_primary_key") val isPrimaryKey: Boolean
)

// MARK: - Table Meta

@Serializable
data class TableMetaResponse(
    val tables: List<TableMeta>,
    @SerialName("server_time") val serverTime: String,
    @SerialName("schema_version") val schemaVersion: Long,
    @SerialName("schema_hash") val schemaHash: String
)

@Serializable
data class TableMeta(
    @SerialName("table_name") val tableName: String,
    @SerialName("push_policy") val pushPolicy: String,
    val dependencies: List<String>,
    @SerialName("parent_table") val parentTable: String? = null,
    @SerialName("parent_fk_col") val parentFKCol: String? = null,
    @SerialName("updated_at_column") val updatedAtColumn: String? = null,
    @SerialName("deleted_at_column") val deletedAtColumn: String? = null,
    @SerialName("bucket_by_column") val bucketByColumn: String? = null,
    @SerialName("bucket_prefix") val bucketPrefix: String? = null,
    @SerialName("global_when_bucket_null") val globalWhenBucketNull: Boolean? = null,
    @SerialName("allow_global_read") val allowGlobalRead: Boolean? = null,
    @SerialName("bucket_function") val bucketFunction: String? = null
)

// MARK: - SDK Types

sealed class SyncStatus {
    data object Idle : SyncStatus()
    data object Syncing : SyncStatus()
    data class Error(val retryAt: java.time.Instant?) : SyncStatus()
    data object Stopped : SyncStatus()
}

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

// MARK: - Push Status Constants

object PushStatus {
    const val APPLIED = "applied"
    const val CONFLICT = "conflict"
    const val REJECTED_TERMINAL = "rejected_terminal"
    const val REJECTED_RETRYABLE = "rejected_retryable"
}

// MARK: - Schema Mismatch Body (internal)

@Serializable
internal data class SchemaMismatchBody(
    val code: String? = null,
    val message: String? = null,
    @SerialName("server_schema_version") val serverSchemaVersion: Long? = null,
    @SerialName("server_schema_hash") val serverSchemaHash: String? = null
)
