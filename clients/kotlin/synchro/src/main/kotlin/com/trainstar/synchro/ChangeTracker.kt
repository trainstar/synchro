package com.trainstar.synchro

import android.database.sqlite.SQLiteDatabase
import android.util.Base64
import kotlinx.serialization.Serializable

/**
 * A pending change is an immutable mutation-ledger record.
 *
 * The legacy property names remain for source compatibility with older SDK
 * callers. They map to protocol v3 base and client versions.
 */
@Serializable
data class PendingChange(
    val mutationID: String,
    val localOrder: Long,
    val tableID: String,
    val recordID: String,
    val tableName: String,
    val pkFieldID: String,
    val pkLogicalType: String,
    val operation: String,
    val authoredSchemaVersion: Long,
    val authoredSchemaHash: String,
    val baseUpdatedAt: String?,
    val clientUpdatedAt: String,
    val lifecycleState: String,
    val sourceKind: String,
    val dependsOnMutationID: String?,
    val normalizedMutationID: String?,
    val sealedBatchID: String?,
    val sealedOrdinal: Int?,
    val localRevision: Long = localOrder,
)

internal data class LedgerValue(
    val fieldID: String,
    val logicalType: String,
    val valueKind: String,
    val integerValue: Long?,
    val realValue: Double?,
    val textValue: String?,
    val blobValue: ByteArray?,
) {
    fun asAnyCodable(): AnyCodable = AnyCodable(
        when (valueKind) {
            "null" -> null
            "boolean" -> integerValue != 0L
            "integer" -> when (logicalType) {
                "int64" -> integerValue?.toString()
                    ?: throw SynchroError.InvalidResponse("stored int64 mutation value is invalid")
                else -> integerValue
            }
            "real" -> realValue
            "text" -> textValue
            "blob" -> blobValue?.let {
                Base64.encodeToString(it, Base64.URL_SAFE or Base64.NO_WRAP or Base64.NO_PADDING)
            }
            else -> throw SynchroError.InvalidResponse("stored mutation value has an invalid kind")
        }
    )
}

internal class ChangeTracker(private val database: SynchroDatabase) {

    internal fun inspectPendingMutations(): List<PendingMutationInspection> =
        inspectMutations(includeServerRejected = false)

    /**
     * Returns every mutation the client retains, including one the server
     * rejected. A rejected mutation leaves the pending set, so a caller that
     * needs the complete retained ledger reads this instead.
     */
    internal fun inspectRetainedMutations(): List<PendingMutationInspection> =
        inspectMutations(includeServerRejected = true)

    private fun inspectMutations(includeServerRejected: Boolean): List<PendingMutationInspection> =
        database.readTransaction { db ->
            val rejectedState = if (includeServerRejected) ", 'rejected_terminal'" else ""
            queryChanges(
                db,
                """
                SELECT $CHANGE_COLUMNS
                FROM _synchro_pending_changes
                WHERE lifecycle_state IN (
                    'captured', 'sealed', 'legacy_blocked', 'blocked_by_predecessor',
                    'superseded_before_send', 'cancelled_before_send'$rejectedState
                )
                ORDER BY local_order
                """.trimIndent(),
                emptyArray(),
            ).map { change ->
                PendingMutationInspection(
                    mutationID = change.mutationID,
                    localOrder = change.localOrder,
                    tableID = change.tableID,
                    tableName = change.tableName,
                    recordID = change.recordID,
                    primaryKeyFieldID = change.pkFieldID,
                    primaryKeyLogicalType = change.pkLogicalType,
                    operation = change.operation.asOperation(),
                    authoredSchema = SchemaRef(change.authoredSchemaVersion, change.authoredSchemaHash),
                    baseVersion = change.baseUpdatedAt,
                    clientVersion = change.clientUpdatedAt,
                    status = change.lifecycleState.asLocalMutationStatus(),
                    sourceKind = change.sourceKind,
                    dependsOnMutationID = change.dependsOnMutationID,
                    normalizedMutationID = change.normalizedMutationID,
                    sealedBatchID = change.sealedBatchID,
                    sealedOrdinal = change.sealedOrdinal,
                    authoredFields = valuesForMutation(db, change.mutationID).map { value ->
                        AuthoredMutationField(value.fieldID, value.logicalType, value.asAnyCodable())
                    },
                )
            }
        }

    /** Returns only unsealed records that can become a new batch. */
    fun pendingChanges(limit: Int = 100): List<PendingChange> =
        database.readTransaction { db -> pendingChanges(db, limit) }

    internal fun pendingChanges(db: SQLiteDatabase, limit: Int): List<PendingChange> =
        queryChanges(
            db,
            """
            SELECT $CHANGE_COLUMNS
            FROM _synchro_pending_changes
            WHERE lifecycle_state = 'captured'
            ORDER BY local_order
            LIMIT ?
            """.trimIndent(),
            arrayOf(limit.toString()),
        )

    /**
     * A row chain is keyed by immutable protocol identity.
     * Physical table names can change while queued intent remains valid.
     */
    internal fun capturedChain(
        db: SQLiteDatabase,
        tableID: String,
        pkFieldID: String,
        pkLogicalType: String,
        recordID: String,
    ): List<PendingChange> =
        queryChanges(
            db,
            """
            SELECT $CHANGE_COLUMNS
            FROM _synchro_pending_changes
            WHERE table_id = ? AND pk_field_id = ? AND pk_logical_type = ?
              AND record_id = ? AND lifecycle_state = 'captured'
            ORDER BY local_order
            """.trimIndent(),
            arrayOf(tableID, pkFieldID, pkLogicalType, recordID),
        )

    internal fun changeByID(db: SQLiteDatabase, mutationID: String): PendingChange? =
        queryChanges(
            db,
            "SELECT $CHANGE_COLUMNS FROM _synchro_pending_changes WHERE mutation_id = ?",
            arrayOf(mutationID),
        ).singleOrNull()

    internal fun valuesForMutation(db: SQLiteDatabase, mutationID: String): List<LedgerValue> {
        val values = mutableListOf<LedgerValue>()
        db.rawQuery(
            """
            SELECT field_id, logical_type, value_kind, value_integer, value_real, value_text, value_blob
            FROM _synchro_mutation_values
            WHERE mutation_id = ?
            ORDER BY field_id
            """.trimIndent(),
            arrayOf(mutationID),
        ).use { cursor ->
            while (cursor.moveToNext()) {
                values += LedgerValue(
                    fieldID = cursor.getString(0),
                    logicalType = cursor.getString(1),
                    valueKind = cursor.getString(2),
                    integerValue = if (cursor.isNull(3)) null else cursor.getLong(3),
                    realValue = if (cursor.isNull(4)) null else cursor.getDouble(4),
                    textValue = if (cursor.isNull(5)) null else cursor.getString(5),
                    blobValue = if (cursor.isNull(6)) null else cursor.getBlob(6),
                )
            }
        }
        return values
    }

    /**
     * This compatibility method reads only immutable field-value records.
     * It never reads the current application row.
     */
    fun hydratePendingForPush(
        pending: List<PendingChange>,
        syncedTables: List<LocalSchemaTable>,
    ): List<PushRecord> = database.readTransaction { db ->
        hydratePendingForPush(db, pending, syncedTables)
    }

    internal fun hydratePendingForPush(
        db: SQLiteDatabase,
        pending: List<PendingChange>,
        syncedTables: List<LocalSchemaTable>,
    ): List<PushRecord> {
        val tables = syncedTables.associateBy { it.tableID }
        return pending.map { change ->
            val table = tables[change.tableID]
            val names = table?.columns?.associate { it.fieldID to it.name }.orEmpty()
            val data = if (change.operation == "delete") {
                null
            } else {
                valuesForMutation(db, change.mutationID).associate { value ->
                    (names[value.fieldID] ?: value.fieldID) to value.asAnyCodable()
                }
            }
            PushRecord(
                id = change.recordID,
                tableName = change.tableName,
                operation = change.operation,
                data = data,
                clientUpdatedAt = change.clientUpdatedAt,
                baseUpdatedAt = change.baseUpdatedAt,
                localRevision = change.localOrder,
            )
        }
    }

    /** Explicit cleanup for tests and local database reset tools. */
    internal fun clearAllInTransaction(db: SQLiteDatabase) {
        db.execSQL("DELETE FROM _synchro_mutation_values")
        db.execSQL("DELETE FROM _synchro_push_batch_members")
        db.execSQL("DELETE FROM _synchro_pending_changes")
    }

    /** Explicit cleanup for tests and local database reset tools. */
    internal fun clearAllForTesting() {
        database.writeTransaction { db -> clearAllInTransaction(db) }
    }

    /** Explicit cleanup for tests and local database reset tools. */
    internal fun clearTableForTesting(table: String) {
        database.writeTransaction { db ->
            db.execSQL(
                "DELETE FROM _synchro_push_batch_members WHERE mutation_id IN (SELECT mutation_id FROM _synchro_pending_changes WHERE table_name = ?)",
                arrayOf(table),
            )
            db.execSQL(
                "DELETE FROM _synchro_mutation_values WHERE mutation_id IN (SELECT mutation_id FROM _synchro_pending_changes WHERE table_name = ?)",
                arrayOf(table),
            )
            db.execSQL("DELETE FROM _synchro_pending_changes WHERE table_name = ?", arrayOf(table))
        }
    }

    fun hasPendingChanges(): Boolean = pendingChangeCount() > 0

    fun hasCapturedChanges(): Boolean = database.readTransaction { db ->
        db.rawQuery(
            "SELECT EXISTS(SELECT 1 FROM _synchro_pending_changes WHERE lifecycle_state = 'captured')",
            null,
        ).use { cursor -> cursor.moveToFirst() && cursor.getInt(0) == 1 }
    }

    /** A completed or blocked record remains inspectable but is not pending work. */
    fun pendingChangeCount(): Int = database.readTransaction { db ->
        db.rawQuery(
            "SELECT COUNT(*) FROM _synchro_pending_changes WHERE lifecycle_state IN ('captured', 'sealed')",
            null,
        ).use { cursor -> if (cursor.moveToFirst()) cursor.getInt(0) else 0 }
    }

    private fun queryChanges(db: SQLiteDatabase, sql: String, args: Array<String>): List<PendingChange> {
        val changes = mutableListOf<PendingChange>()
        db.rawQuery(sql, args).use { cursor ->
            while (cursor.moveToNext()) {
                changes += PendingChange(
                    mutationID = cursor.getString(0),
                    localOrder = cursor.getLong(1),
                    tableID = cursor.getString(2),
                    recordID = cursor.getString(3),
                    tableName = cursor.getString(4),
                    pkFieldID = cursor.getString(5),
                    pkLogicalType = cursor.getString(6),
                    operation = cursor.getString(7),
                    authoredSchemaVersion = cursor.getLong(8),
                    authoredSchemaHash = cursor.getString(9),
                    baseUpdatedAt = if (cursor.isNull(10)) null else cursor.getString(10),
                    clientUpdatedAt = cursor.getString(11),
                    lifecycleState = cursor.getString(12),
                    sourceKind = cursor.getString(13),
                    dependsOnMutationID = if (cursor.isNull(14)) null else cursor.getString(14),
                    normalizedMutationID = if (cursor.isNull(15)) null else cursor.getString(15),
                    sealedBatchID = if (cursor.isNull(16)) null else cursor.getString(16),
                    sealedOrdinal = if (cursor.isNull(17)) null else cursor.getInt(17),
                )
            }
        }
        return changes
    }

    private fun String.asOperation(): Operation = when (this) {
        "insert" -> Operation.INSERT
        "update" -> Operation.UPDATE
        "delete" -> Operation.DELETE
        else -> throw SynchroError.InvalidResponse("stored mutation has an invalid operation")
    }

    private fun String.asLocalMutationStatus(): LocalMutationStatus = when (this) {
        "captured" -> LocalMutationStatus.PENDING
        "sealed" -> LocalMutationStatus.SEALED
        "superseded_before_send" -> LocalMutationStatus.SUPERSEDED_BEFORE_SEND
        "cancelled_before_send" -> LocalMutationStatus.CANCELLED_BEFORE_SEND
        "legacy_blocked", "blocked_by_predecessor" -> LocalMutationStatus.BLOCKED_BY_PREDECESSOR
        "rejected_terminal" -> LocalMutationStatus.SERVER_REJECTED
        else -> throw SynchroError.InvalidResponse("stored mutation has an invalid inspectable status")
    }

    private companion object {
        const val CHANGE_COLUMNS = """
            mutation_id, local_order, table_id, record_id, table_name, pk_field_id, pk_logical_type,
            operation, authored_schema_version, authored_schema_hash, base_version, client_version,
            lifecycle_state, source_kind, depends_on_mutation_id, normalized_mutation_id,
            sealed_batch_id, sealed_ordinal
        """
    }
}
