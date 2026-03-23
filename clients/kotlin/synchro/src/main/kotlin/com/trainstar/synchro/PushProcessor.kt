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
        val response: VNextPushResponse,
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

        val request = VNextPushRequest(
            clientID = clientID,
            batchID = buildBatchID(mutations),
            schema = VNextSchemaRef(version = schemaVersion, hash = schemaHash),
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
    ): List<VNextMutation> {
        val tableMap = syncedTables.associateBy { it.tableName }
        return pushRecords.mapNotNull { record ->
            val schema = tableMap[record.tableName] ?: return@mapNotNull null
            val pkColumn = schema.primaryKey.firstOrNull() ?: "id"
            VNextMutation(
                mutationID = mutationID(record),
                table = record.tableName,
                op = mutationOperation(record.operation),
                pk = JsonObject(mapOf(pkColumn to JsonPrimitive(record.id))),
                baseVersion = record.baseUpdatedAt,
                columns = record.data?.let(::anyMapToJsonObject)
            )
        }
    }

    private fun mutationOperation(operation: String): VNextOperation = when (operation) {
        "create" -> VNextOperation.INSERT
        "update" -> VNextOperation.UPDATE
        "delete" -> VNextOperation.DELETE
        else -> throw SynchroError.InvalidResponse("unknown local operation $operation")
    }

    private fun mutationID(record: PushRecord): String =
        "${record.tableName}:${record.id}:${record.operation}:${record.clientUpdatedAt}"

    private fun buildBatchID(mutations: List<VNextMutation>): String {
        val first = mutations.firstOrNull()?.mutationID ?: return "empty"
        val last = mutations.lastOrNull()?.mutationID ?: return first
        return "$first|$last"
    }

    // MARK: - Internal (visible for testing)

    fun applyLegacyAccepted(accepted: List<PushResult>, syncedTables: List<LocalSchemaTable>): List<ConflictEvent> {
        if (accepted.isEmpty()) return emptyList()
        val tableMap = syncedTables.associateBy { it.tableName }

        database.writeTransaction { db ->
            SynchroMeta.setSyncLock(db, true)
            try {
                for (result in accepted) {
                    // Remove from pending queue
                    val delStmt = db.compileStatement(
                        "DELETE FROM _synchro_pending_changes WHERE table_name = ? AND record_id = ?"
                    )
                    try {
                        delStmt.bindString(1, result.tableName)
                        delStmt.bindString(2, result.id)
                        delStmt.executeUpdateDelete()
                    } finally {
                        delStmt.close()
                    }

                    val schema = tableMap[result.tableName] ?: continue
                    val pkCol = schema.primaryKey.firstOrNull() ?: "id"
                    val quoted = SQLiteHelpers.quoteIdentifier(result.tableName)
                    val quotedPK = SQLiteHelpers.quoteIdentifier(pkCol)

                    // RYOW: apply server timestamps to local records using schema column names
                    if (result.serverUpdatedAt != null) {
                        val quotedUpdatedAt = SQLiteHelpers.quoteIdentifier(schema.updatedAtColumn)
                        val stmt = db.compileStatement(
                            "UPDATE $quoted SET $quotedUpdatedAt = ? WHERE $quotedPK = ?"
                        )
                        try {
                            stmt.bindString(1, result.serverUpdatedAt)
                            stmt.bindString(2, result.id)
                            stmt.executeUpdateDelete()
                        } finally {
                            stmt.close()
                        }
                    }
                    if (result.serverDeletedAt != null) {
                        val quotedDeletedAt = SQLiteHelpers.quoteIdentifier(schema.deletedAtColumn)
                        val stmt = db.compileStatement(
                            "UPDATE $quoted SET $quotedDeletedAt = ? WHERE $quotedPK = ?"
                        )
                        try {
                            stmt.bindString(1, result.serverDeletedAt)
                            stmt.bindString(2, result.id)
                            stmt.executeUpdateDelete()
                        } finally {
                            stmt.close()
                        }
                    }
                }
            } finally {
                SynchroMeta.setSyncLock(db, false)
            }
        }

        return emptyList()
    }

    fun applyAccepted(accepted: List<VNextAcceptedMutation>, syncedTables: List<LocalSchemaTable>): List<ConflictEvent> {
        if (accepted.isEmpty()) return emptyList()
        val tableMap = syncedTables.associateBy { it.tableName }

        database.writeTransaction { db ->
            SynchroMeta.setSyncLock(db, true)
            try {
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
            } finally {
                SynchroMeta.setSyncLock(db, false)
            }
        }

        return emptyList()
    }

    data class RejectedOutcome(
        val conflicts: List<ConflictEvent>,
        val hasRetryableRejections: Boolean
    )

    fun applyLegacyRejected(rejected: List<PushResult>, syncedTables: List<LocalSchemaTable>): List<ConflictEvent> {
        return applyLegacyRejectedOutcome(rejected, syncedTables).conflicts
    }

    fun applyRejected(rejected: List<VNextRejectedMutation>, syncedTables: List<LocalSchemaTable>): List<ConflictEvent> {
        return applyRejectedOutcome(rejected, syncedTables).conflicts
    }

    private fun applyLegacyRejectedOutcome(rejected: List<PushResult>, syncedTables: List<LocalSchemaTable>): RejectedOutcome {
        if (rejected.isEmpty()) return RejectedOutcome(emptyList(), false)
        val tableMap = syncedTables.associateBy { it.tableName }
        val conflicts = mutableListOf<ConflictEvent>()
        var hasRetryableRejections = false

        database.writeTransaction { db ->
            SynchroMeta.setSyncLock(db, true)
            try {
                for (result in rejected) {
                    if (result.status == PushStatus.REJECTED_RETRYABLE) {
                        hasRetryableRejections = true
                        continue
                    }

                    val delStmt = db.compileStatement(
                        "DELETE FROM _synchro_pending_changes WHERE table_name = ? AND record_id = ?"
                    )
                    try {
                        delStmt.bindString(1, result.tableName)
                        delStmt.bindString(2, result.id)
                        delStmt.executeUpdateDelete()
                    } finally {
                        delStmt.close()
                    }

                    // Apply server version if provided
                    val serverVersion = result.serverVersion
                    val schema = tableMap[result.tableName]
                    if (serverVersion != null && schema != null) {
                        val pkCol = schema.primaryKey.firstOrNull() ?: "id"
                        val columns = schema.columns.map { it.name }
                        val quoted = SQLiteHelpers.quoteIdentifier(result.tableName)
                        val quotedPK = SQLiteHelpers.quoteIdentifier(pkCol)

                        val dbValues = columns.map { col ->
                            val anyCodable = serverVersion.data[col]
                            if (anyCodable != null) {
                                SQLiteHelpers.databaseValue(anyCodable)
                            } else if (col == pkCol) {
                                serverVersion.id
                            } else {
                                null
                            }
                        }

                        val quotedColumns = columns.joinToString(", ") { SQLiteHelpers.quoteIdentifier(it) }
                        val placeholders = SQLiteHelpers.placeholders(columns.size)
                        val updateClauses = columns
                            .filter { it != pkCol }
                            .joinToString(", ") { "${SQLiteHelpers.quoteIdentifier(it)} = excluded.${SQLiteHelpers.quoteIdentifier(it)}" }

                        executeWithTypedBindings(
                            db,
                            "INSERT INTO $quoted ($quotedColumns) VALUES ($placeholders) ON CONFLICT ($quotedPK) DO UPDATE SET $updateClauses",
                            dbValues
                        )
                    }

                    if (result.status == PushStatus.CONFLICT) {
                        conflicts.add(
                            ConflictEvent(
                                table = result.tableName,
                                recordID = result.id,
                                clientData = null,
                                serverData = result.serverVersion?.data
                            )
                        )
                    }
                }
            } finally {
                SynchroMeta.setSyncLock(db, false)
            }
        }

        return RejectedOutcome(conflicts, hasRetryableRejections)
    }

    @JvmName("applyRejectedOutcomeVNextLocalSchema")
    private fun applyRejectedOutcome(
        rejected: List<VNextRejectedMutation>,
        syncedTables: List<LocalSchemaTable>
    ): RejectedOutcome {
        if (rejected.isEmpty()) return RejectedOutcome(emptyList(), false)
        val tableMap = syncedTables.associateBy { it.tableName }
        val conflicts = mutableListOf<ConflictEvent>()
        var hasRetryableRejections = false

        database.writeTransaction { db ->
            SynchroMeta.setSyncLock(db, true)
            try {
                for (mutation in rejected) {
                    if (mutation.status == VNextMutationStatus.REJECTED_RETRYABLE) {
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

                    if (mutation.status == VNextMutationStatus.CONFLICT) {
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
            } finally {
                SynchroMeta.setSyncLock(db, false)
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
