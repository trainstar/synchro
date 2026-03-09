package com.trainstar.synchro

import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteStatement
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField

class PullProcessor(private val database: SynchroDatabase) {

    private val isoFormatter: DateTimeFormatter = DateTimeFormatterBuilder()
        .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
        .optionalStart()
        .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
        .optionalEnd()
        .appendPattern("'Z'")
        .toFormatter()

    fun applyChanges(changes: List<Record>, syncedTables: List<SchemaTable>) {
        if (changes.isEmpty()) return
        val tableMap = syncedTables.associateBy { it.tableName }

        database.writeTransaction { db ->
            SynchroMeta.setSyncLock(db, true)
            try {
                for (record in changes) {
                    val schema = tableMap[record.tableName] ?: continue
                    upsertRecord(db, record, schema)
                }
            } finally {
                SynchroMeta.setSyncLock(db, false)
            }
        }
    }

    fun applyDeletes(deletes: List<DeleteEntry>, syncedTables: List<SchemaTable>) {
        if (deletes.isEmpty()) return
        val tableMap = syncedTables.associateBy { it.tableName }

        database.writeTransaction { db ->
            SynchroMeta.setSyncLock(db, true)
            try {
                applyDeletesInTransaction(db, deletes, tableMap)
            } finally {
                SynchroMeta.setSyncLock(db, false)
            }
        }
    }

    /**
     * Atomically applies changes and deletes from a single pull page within
     * one write transaction and one sync lock cycle. This prevents partial
     * state if the process is killed between applying changes and deletes.
     */
    fun applyPullPage(
        changes: List<Record>,
        deletes: List<DeleteEntry>,
        syncedTables: List<SchemaTable>
    ) {
        if (changes.isEmpty() && deletes.isEmpty()) return
        val tableMap = syncedTables.associateBy { it.tableName }

        database.writeTransaction { db ->
            SynchroMeta.setSyncLock(db, true)
            try {
                for (record in changes) {
                    val schema = tableMap[record.tableName] ?: continue
                    upsertRecord(db, record, schema)
                }
                applyDeletesInTransaction(db, deletes, tableMap)
            } finally {
                SynchroMeta.setSyncLock(db, false)
            }
        }
    }

    private fun applyDeletesInTransaction(
        db: SQLiteDatabase,
        deletes: List<DeleteEntry>,
        tableMap: Map<String, SchemaTable>
    ) {
        for (entry in deletes) {
            val schema = tableMap[entry.tableName] ?: continue
            val pkCol = schema.primaryKey.firstOrNull() ?: "id"
            val quoted = SQLiteHelpers.quoteIdentifier(entry.tableName)
            val quotedPK = SQLiteHelpers.quoteIdentifier(pkCol)
            val quotedDeletedAt = SQLiteHelpers.quoteIdentifier(schema.deletedAtColumn)

            val stmt = db.compileStatement(
                "UPDATE $quoted SET $quotedDeletedAt = ${SQLiteHelpers.timestampNow()} WHERE $quotedPK = ? AND $quotedDeletedAt IS NULL"
            )
            try {
                stmt.bindString(1, entry.id)
                stmt.executeUpdateDelete()
            } finally {
                stmt.close()
            }
        }
    }

    fun applySnapshotPage(records: List<Record>, syncedTables: List<SchemaTable>) {
        if (records.isEmpty()) return
        val tableMap = syncedTables.associateBy { it.tableName }

        database.writeTransaction { db ->
            SynchroMeta.setSyncLock(db, true)
            try {
                for (record in records) {
                    val schema = tableMap[record.tableName] ?: continue
                    insertOrReplace(db, record, schema)
                }
            } finally {
                SynchroMeta.setSyncLock(db, false)
            }
        }
    }

    fun updateCheckpoint(checkpoint: Long) {
        database.writeTransaction { db ->
            val current = SynchroMeta.getInt64(db, MetaKey.CHECKPOINT)
            if (checkpoint > current) {
                SynchroMeta.setInt64(db, MetaKey.CHECKPOINT, checkpoint)
            }
        }
    }

    fun updateKnownBuckets(bucketUpdates: BucketUpdate?) {
        val updates = bucketUpdates ?: return
        database.writeTransaction { db ->
            val existing = SynchroMeta.get(db, MetaKey.KNOWN_BUCKETS) ?: "[]"
            val buckets = try {
                kotlinx.serialization.json.Json.decodeFromString<List<String>>(existing).toMutableList()
            } catch (_: Exception) {
                mutableListOf()
            }

            updates.added?.forEach { b ->
                if (b !in buckets) buckets.add(b)
            }
            updates.removed?.forEach { b ->
                buckets.remove(b)
            }

            val jsonArray = kotlinx.serialization.json.buildJsonArray {
                for (b in buckets) add(kotlinx.serialization.json.JsonPrimitive(b))
            }
            val encoded = jsonArray.toString()
            SynchroMeta.set(db, MetaKey.KNOWN_BUCKETS, encoded)
        }
    }

    // MARK: - Private

    private fun upsertRecord(db: SQLiteDatabase, record: Record, schema: SchemaTable) {
        val pkCol = schema.primaryKey.firstOrNull() ?: "id"
        val quoted = SQLiteHelpers.quoteIdentifier(record.tableName)
        val quotedPK = SQLiteHelpers.quoteIdentifier(pkCol)
        val quotedUpdatedAt = SQLiteHelpers.quoteIdentifier(schema.updatedAtColumn)

        // RYOW dedup: skip if local updated_at is >= server
        db.rawQuery(
            "SELECT $quotedUpdatedAt FROM $quoted WHERE $quotedPK = ?",
            arrayOf(record.id)
        ).use { cursor ->
            if (cursor.moveToFirst()) {
                val localUpdatedAt = cursor.getString(0)
                if (localUpdatedAt != null) {
                    try {
                        val localInstant = parseISO8601(localUpdatedAt)
                        val serverInstant = parseISO8601(record.updatedAt)
                        if (localInstant != null && serverInstant != null && !localInstant.isBefore(serverInstant)) {
                            return
                        }
                    } catch (_: Exception) {
                        // Parse failure, proceed with upsert
                    }
                }
            }
        }

        val columns = schema.columns.map { it.name }
        val dbValues = buildDatabaseValues(columns, pkCol, record.id, record.data)

        val quotedColumns = columns.joinToString(", ") { SQLiteHelpers.quoteIdentifier(it) }
        val placeholders = SQLiteHelpers.placeholders(columns.size)
        val updateClauses = columns
            .filter { it != pkCol }
            .joinToString(", ") { "${SQLiteHelpers.quoteIdentifier(it)} = excluded.${SQLiteHelpers.quoteIdentifier(it)}" }

        val sql = "INSERT INTO $quoted ($quotedColumns) VALUES ($placeholders) ON CONFLICT ($quotedPK) DO UPDATE SET $updateClauses"
        executeWithTypedBindings(db, sql, dbValues)
    }

    private fun insertOrReplace(db: SQLiteDatabase, record: Record, schema: SchemaTable) {
        val columns = schema.columns.map { it.name }
        val pkCol = schema.primaryKey.firstOrNull() ?: "id"
        val quoted = SQLiteHelpers.quoteIdentifier(record.tableName)

        val dbValues = buildDatabaseValues(columns, pkCol, record.id, record.data)

        val quotedColumns = columns.joinToString(", ") { SQLiteHelpers.quoteIdentifier(it) }
        val placeholders = SQLiteHelpers.placeholders(columns.size)

        executeWithTypedBindings(
            db,
            "INSERT OR REPLACE INTO $quoted ($quotedColumns) VALUES ($placeholders)",
            dbValues
        )
    }

    private fun buildDatabaseValues(
        columns: List<String>,
        pkCol: String,
        recordID: String,
        data: Map<String, AnyCodable>
    ): List<Any?> {
        return columns.map { col ->
            val anyCodable = data[col]
            if (anyCodable != null) {
                SQLiteHelpers.databaseValue(anyCodable)
            } else if (col == pkCol) {
                recordID
            } else {
                null
            }
        }
    }

    private fun parseISO8601(dateStr: String): Instant? {
        return try {
            Instant.parse(dateStr)
        } catch (_: Exception) {
            try {
                java.time.LocalDateTime.parse(dateStr, isoFormatter)
                    .toInstant(java.time.ZoneOffset.UTC)
            } catch (_: Exception) {
                null
            }
        }
    }
}

/**
 * Executes a SQL statement with properly typed parameter bindings.
 * Unlike `execSQL(String, Object[])` which converts everything to strings,
 * this uses `compileStatement` with typed bind methods to correctly handle
 * null, Long, Double, String, ByteArray, and Boolean values.
 */
internal fun executeWithTypedBindings(db: SQLiteDatabase, sql: String, values: List<Any?>) {
    val stmt = db.compileStatement(sql)
    try {
        bindTypedValues(stmt, values)
        stmt.executeUpdateDelete()
    } finally {
        stmt.close()
    }
}

internal fun bindTypedValues(stmt: SQLiteStatement, values: List<Any?>) {
    for (i in values.indices) {
        val bindIndex = i + 1
        when (val value = values[i]) {
            null -> stmt.bindNull(bindIndex)
            is Long -> stmt.bindLong(bindIndex, value)
            is Int -> stmt.bindLong(bindIndex, value.toLong())
            is Double -> stmt.bindDouble(bindIndex, value)
            is Float -> stmt.bindDouble(bindIndex, value.toDouble())
            is ByteArray -> stmt.bindBlob(bindIndex, value)
            is Boolean -> stmt.bindLong(bindIndex, if (value) 1L else 0L)
            else -> stmt.bindString(bindIndex, value.toString())
        }
    }
}
