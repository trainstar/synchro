package com.trainstar.synchro

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

class PushProcessor(
    private val database: SynchroDatabase,
    private val changeTracker: ChangeTracker
) {
    data class PushOutcome(
        val response: PushResponse,
        val conflicts: List<ConflictEvent>,
        val hasRetryableRejections: Boolean
    )

    suspend fun processPush(
        httpClient: HttpClient,
        clientID: String,
        schemaVersion: Long,
        schemaHash: String,
        syncedTables: List<LocalSchemaTable>,
        batchSize: Int = 100
    ): PushOutcome? {
        val pending = changeTracker.pendingChanges(limit = batchSize)
        if (pending.isEmpty()) return null

        val pushRecords = changeTracker.hydratePendingForPush(pending, syncedTables)
        if (pushRecords.isEmpty()) {
            changeTracker.removePending(pending)
            return null
        }

        val mutations = buildMutations(pushRecords, syncedTables)
        if (mutations.isEmpty()) {
            changeTracker.removePending(pending)
            return null
        }

        val request = PushRequest(
            clientID = clientID,
            batchID = buildBatchID(mutations),
            schema = SchemaRef(version = schemaVersion, hash = schemaHash),
            mutations = mutations
        )

        val response = httpClient.push(request)

        val conflicts = applyAccepted(response.accepted, syncedTables)
        val rejectedOutcome = applyRejectedOutcome(response.rejected, syncedTables)

        return PushOutcome(
            response = response,
            conflicts = conflicts + rejectedOutcome.conflicts,
            hasRetryableRejections = rejectedOutcome.hasRetryableRejections
        )
    }

    private fun buildMutations(
        pushRecords: List<PushRecord>,
        syncedTables: List<LocalSchemaTable>
    ): List<Mutation> {
        val tableMap = syncedTables.associateBy { it.tableName }
        return pushRecords.mapNotNull { record ->
            val schema = tableMap[record.tableName] ?: return@mapNotNull null
            val pkColumn = schema.primaryKey.firstOrNull() ?: "id"
            Mutation(
                mutationID = mutationID(record),
                table = record.tableName,
                op = mutationOperation(record.operation),
                pk = JsonObject(mapOf(pkColumn to JsonPrimitive(record.id))),
                baseVersion = record.baseUpdatedAt,
                clientVersion = record.clientUpdatedAt,
                columns = record.data?.let(::anyMapToJsonObject)
            )
        }
    }

    private fun mutationOperation(operation: String): Operation = when (operation) {
        "create" -> Operation.INSERT
        "update" -> Operation.UPDATE
        "delete" -> Operation.DELETE
        else -> throw SynchroError.InvalidResponse("unknown local operation $operation")
    }

    private fun mutationID(record: PushRecord): String =
        "${record.tableName}:${record.id}:${record.operation}:${record.clientUpdatedAt}"

    private fun buildBatchID(mutations: List<Mutation>): String {
        val first = mutations.firstOrNull()?.mutationID ?: return "empty"
        val last = mutations.lastOrNull()?.mutationID ?: return first
        return "$first|$last"
    }

    // MARK: - Internal (visible for testing)

    fun applyAccepted(accepted: List<AcceptedMutation>, syncedTables: List<LocalSchemaTable>): List<ConflictEvent> {
        if (accepted.isEmpty()) return emptyList()
        val tableMap = syncedTables.associateBy { it.tableName }

        database.writeSyncLockedTransaction { db ->
            for (mutation in accepted) {
                val schema = tableMap[mutation.table] ?: continue
                val recordID = recordID(mutation.pk, schema)

                mutation.serverRow?.let { row ->
                    upsertServerRow(
                        db = db,
                        tableName = mutation.table,
                        schema = schema,
                        recordID = recordID,
                        row = jsonObjectToAnyMap(row)
                    )
                }

                val delStmt = db.compileStatement(
                    "DELETE FROM _synchro_pending_changes WHERE table_name = ? AND record_id = ?"
                )
                try {
                    delStmt.bindString(1, mutation.table)
                    delStmt.bindString(2, recordID)
                    delStmt.executeUpdateDelete()
                } finally {
                    delStmt.close()
                }
            }
        }

        return emptyList()
    }

    data class RejectedOutcome(
        val conflicts: List<ConflictEvent>,
        val hasRetryableRejections: Boolean
    )

    fun applyRejected(rejected: List<RejectedMutation>, syncedTables: List<LocalSchemaTable>): List<ConflictEvent> {
        return applyRejectedOutcome(rejected, syncedTables).conflicts
    }

    @JvmName("applyRejectedOutcomeLocalSchema")
    private fun applyRejectedOutcome(
        rejected: List<RejectedMutation>,
        syncedTables: List<LocalSchemaTable>
    ): RejectedOutcome {
        if (rejected.isEmpty()) return RejectedOutcome(emptyList(), false)
        val tableMap = syncedTables.associateBy { it.tableName }
        val conflicts = mutableListOf<ConflictEvent>()
        var hasRetryableRejections = false

        database.writeSyncLockedTransaction { db ->
            for (mutation in rejected) {
                if (mutation.status == MutationStatus.REJECTED_RETRYABLE) {
                    hasRetryableRejections = true
                    continue
                }

                val schema = tableMap[mutation.table] ?: continue
                val recordID = recordID(mutation.pk, schema)

                mutation.serverRow?.let { row ->
                    upsertServerRow(
                        db = db,
                        tableName = mutation.table,
                        schema = schema,
                        recordID = recordID,
                        row = jsonObjectToAnyMap(row)
                    )
                }

                val delStmt = db.compileStatement(
                    "DELETE FROM _synchro_pending_changes WHERE table_name = ? AND record_id = ?"
                )
                try {
                    delStmt.bindString(1, mutation.table)
                    delStmt.bindString(2, recordID)
                    delStmt.executeUpdateDelete()
                } finally {
                    delStmt.close()
                }

                if (mutation.status == MutationStatus.CONFLICT) {
                    conflicts.add(
                        ConflictEvent(
                            table = mutation.table,
                            recordID = recordID,
                            clientData = null,
                            serverData = mutation.serverRow?.let(::jsonObjectToAnyMap)
                        )
                    )
                }
            }
        }

        return RejectedOutcome(conflicts, hasRetryableRejections)
    }

    private fun upsertServerRow(
        db: android.database.sqlite.SQLiteDatabase,
        tableName: String,
        schema: LocalSchemaTable,
        recordID: String,
        row: Map<String, AnyCodable>
    ) {
        val pkCol = schema.primaryKey.firstOrNull() ?: "id"
        val columns = schema.columns.map { it.name }
        val quoted = SQLiteHelpers.quoteIdentifier(tableName)
        val quotedPK = SQLiteHelpers.quoteIdentifier(pkCol)
        val quotedColumns = columns.joinToString(", ") { SQLiteHelpers.quoteIdentifier(it) }
        val placeholders = SQLiteHelpers.placeholders(columns.size)
        val updateClauses = columns
            .filter { it != pkCol }
            .joinToString(", ") { "${SQLiteHelpers.quoteIdentifier(it)} = excluded.${SQLiteHelpers.quoteIdentifier(it)}" }
        val conflictAction = if (updateClauses.isEmpty()) "DO NOTHING" else "DO UPDATE SET $updateClauses"
        val dbValues = columns.map { col ->
            val anyCodable = row[col]
            when {
                anyCodable != null -> SQLiteHelpers.databaseValue(anyCodable)
                col == pkCol -> recordID
                else -> null
            }
        }

        executeWithTypedBindings(
            db,
            "INSERT INTO $quoted ($quotedColumns) VALUES ($placeholders) ON CONFLICT ($quotedPK) $conflictAction",
            dbValues
        )
    }

    private fun recordID(pk: JsonObject, schema: LocalSchemaTable): String {
        val pkCol = schema.primaryKey.firstOrNull() ?: "id"
        val value = pk[pkCol] ?: throw SynchroError.InvalidResponse("missing primary key $pkCol for table ${schema.tableName}")
        return when (val decoded = fromJsonElement(value)) {
            is String -> decoded
            is Int -> decoded.toString()
            is Long -> decoded.toString()
            is Double -> decoded.toString()
            is Boolean -> if (decoded) "true" else "false"
            else -> throw SynchroError.InvalidResponse("unsupported primary key value for table ${schema.tableName}")
        }
    }

    private fun anyMapToJsonObject(value: Map<String, AnyCodable>): JsonObject {
        return JsonObject(value.mapValues { (_, anyCodable) -> toJsonElement(anyCodable.value) })
    }

    private fun jsonObjectToAnyMap(value: JsonObject): Map<String, AnyCodable> {
        return value.mapValues { (_, element) -> AnyCodable(fromJsonElement(element)) }
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
        is Map<*, *> -> JsonObject(value.entries.associate { (k, v) -> k.toString() to toJsonElement(v) })
        is AnyCodable -> toJsonElement(value.value)
        else -> JsonPrimitive(value.toString())
    }

    private fun fromJsonElement(element: JsonElement): Any? = when (element) {
        JsonNull -> null
        is JsonPrimitive -> when {
            element.isString -> element.content
            element.content == "true" -> true
            element.content == "false" -> false
            element.content.contains('.') -> element.content.toDoubleOrNull()
            else -> element.content.toLongOrNull() ?: element.content
        }
        is JsonArray -> element.map { fromJsonElement(it) }
        is JsonObject -> element.mapValues { (_, value) -> fromJsonElement(value) }
    }
}
