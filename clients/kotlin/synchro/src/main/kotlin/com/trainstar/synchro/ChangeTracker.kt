package com.trainstar.synchro

import android.util.Base64

data class PendingChange(
    val recordID: String,
    val tableName: String,
    val operation: String,
    val baseUpdatedAt: String?,
    val clientUpdatedAt: String
)

class ChangeTracker(private val database: SynchroDatabase) {

    fun pendingChanges(limit: Int = 100): List<PendingChange> {
        return database.readTransaction { db ->
            val changes = mutableListOf<PendingChange>()
            db.rawQuery(
                """
                SELECT record_id, table_name, operation, base_updated_at, client_updated_at
                FROM _synchro_pending_changes
                ORDER BY client_updated_at ASC
                LIMIT ?
                """.trimIndent(),
                arrayOf(limit.toString())
            ).use { cursor ->
                while (cursor.moveToNext()) {
                    changes.add(
                        PendingChange(
                            recordID = cursor.getString(0),
                            tableName = cursor.getString(1),
                            operation = cursor.getString(2),
                            baseUpdatedAt = if (cursor.isNull(3)) null else cursor.getString(3),
                            clientUpdatedAt = cursor.getString(4)
                        )
                    )
                }
            }
            changes
        }
    }

    fun hydratePendingForPush(
        pending: List<PendingChange>,
        syncedTables: List<SchemaTable>
    ): List<PushRecord> {
        val tableMap = syncedTables.associateBy { it.tableName }

        return database.readTransaction { db ->
            val records = mutableListOf<PushRecord>()
            for (change in pending) {
                val clientUpdatedAt = change.clientUpdatedAt
                val baseUpdatedAt = change.baseUpdatedAt

                if (change.operation == "delete") {
                    records.add(
                        PushRecord(
                            id = change.recordID,
                            tableName = change.tableName,
                            operation = "delete",
                            data = null,
                            clientUpdatedAt = clientUpdatedAt,
                            baseUpdatedAt = baseUpdatedAt
                        )
                    )
                    continue
                }

                val schema = tableMap[change.tableName] ?: continue
                val pkCol = schema.primaryKey.firstOrNull() ?: "id"
                val quoted = SQLiteHelpers.quoteIdentifier(change.tableName)
                val quotedPK = SQLiteHelpers.quoteIdentifier(pkCol)

                db.rawQuery(
                    "SELECT * FROM $quoted WHERE $quotedPK = ?",
                    arrayOf(change.recordID)
                ).use { cursor ->
                    if (!cursor.moveToFirst()) return@use

                    val data = mutableMapOf<String, AnyCodable>()
                    for (col in schema.columns) {
                        val colIdx = cursor.getColumnIndex(col.name)
                        if (colIdx < 0) {
                            data[col.name] = AnyCodable(null)
                            continue
                        }
                        data[col.name] = when {
                            cursor.isNull(colIdx) -> AnyCodable(null)
                            cursor.getType(colIdx) == android.database.Cursor.FIELD_TYPE_INTEGER -> {
                                val v = cursor.getLong(colIdx)
                                if (col.logicalType == "boolean") {
                                    AnyCodable(v != 0L)
                                } else {
                                    AnyCodable(v)
                                }
                            }
                            cursor.getType(colIdx) == android.database.Cursor.FIELD_TYPE_FLOAT -> {
                                AnyCodable(cursor.getDouble(colIdx))
                            }
                            cursor.getType(colIdx) == android.database.Cursor.FIELD_TYPE_STRING -> {
                                AnyCodable(cursor.getString(colIdx))
                            }
                            cursor.getType(colIdx) == android.database.Cursor.FIELD_TYPE_BLOB -> {
                                AnyCodable(Base64.encodeToString(cursor.getBlob(colIdx), Base64.NO_WRAP))
                            }
                            else -> AnyCodable(null)
                        }
                    }

                    records.add(
                        PushRecord(
                            id = change.recordID,
                            tableName = change.tableName,
                            operation = change.operation,
                            data = data,
                            clientUpdatedAt = clientUpdatedAt,
                            baseUpdatedAt = baseUpdatedAt
                        )
                    )
                }
            }
            records
        }
    }

    fun removePending(entries: List<PendingChange>) {
        if (entries.isEmpty()) return
        database.writeTransaction { db ->
            for (entry in entries) {
                db.execSQL(
                    "DELETE FROM _synchro_pending_changes WHERE table_name = ? AND record_id = ?",
                    arrayOf(entry.tableName, entry.recordID)
                )
            }
        }
    }

    fun removePendingByIDs(entries: List<Pair<String, String>>) {
        if (entries.isEmpty()) return
        database.writeTransaction { db ->
            for ((tableName, recordID) in entries) {
                db.execSQL(
                    "DELETE FROM _synchro_pending_changes WHERE table_name = ? AND record_id = ?",
                    arrayOf(tableName, recordID)
                )
            }
        }
    }

    fun clearTable(table: String) {
        database.writeTransaction { db ->
            db.execSQL(
                "DELETE FROM _synchro_pending_changes WHERE table_name = ?",
                arrayOf(table)
            )
        }
    }

    fun clearAll() {
        database.writeTransaction { db ->
            clearAllInTransaction(db)
        }
    }

    internal fun clearAllInTransaction(db: android.database.sqlite.SQLiteDatabase) {
        db.execSQL("DELETE FROM _synchro_pending_changes")
    }

    fun hasPendingChanges(): Boolean = pendingChangeCount() > 0

    fun pendingChangeCount(): Int {
        return database.readTransaction { db ->
            db.rawQuery("SELECT COUNT(*) FROM _synchro_pending_changes", null).use { cursor ->
                if (cursor.moveToFirst()) cursor.getInt(0) else 0
            }
        }
    }
}
