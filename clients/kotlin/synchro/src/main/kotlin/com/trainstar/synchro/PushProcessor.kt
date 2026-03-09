package com.trainstar.synchro

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
        syncedTables: List<SchemaTable>,
        batchSize: Int = 100
    ): PushOutcome? {
        val pending = changeTracker.pendingChanges(limit = batchSize)
        if (pending.isEmpty()) return null

        val pushRecords = changeTracker.hydratePendingForPush(pending, syncedTables)
        if (pushRecords.isEmpty()) {
            changeTracker.removePending(pending)
            return null
        }

        val request = PushRequest(
            clientID = clientID,
            changes = pushRecords,
            schemaVersion = schemaVersion,
            schemaHash = schemaHash
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

    // MARK: - Internal (visible for testing)

    fun applyAccepted(accepted: List<PushResult>, syncedTables: List<SchemaTable>): List<ConflictEvent> {
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

    data class RejectedOutcome(
        val conflicts: List<ConflictEvent>,
        val hasRetryableRejections: Boolean
    )

    fun applyRejected(rejected: List<PushResult>, syncedTables: List<SchemaTable>): List<ConflictEvent> {
        return applyRejectedOutcome(rejected, syncedTables).conflicts
    }

    private fun applyRejectedOutcome(rejected: List<PushResult>, syncedTables: List<SchemaTable>): RejectedOutcome {
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
}
