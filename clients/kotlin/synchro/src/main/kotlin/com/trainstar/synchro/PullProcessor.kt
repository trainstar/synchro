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

    fun updateBucketCheckpoints(bucketCheckpoints: Map<String, Long>?) {
        val checkpoints = bucketCheckpoints ?: return
        if (checkpoints.isEmpty()) return
        database.writeTransaction { db ->
            for ((bucketId, checkpoint) in checkpoints) {
                SynchroMeta.setBucketCheckpoint(db, bucketId, checkpoint)
            }
        }
    }

    /**
     * Applies a page of rebuild records for a single bucket. Upserts each record
     * into its table and tracks bucket membership in _synchro_bucket_members.
     */
    fun applyRebuildPage(
        bucketId: String,
        records: List<Record>,
        syncedTables: List<SchemaTable>
    ) {
        if (records.isEmpty()) return
        val tableMap = syncedTables.associateBy { it.tableName }

        database.writeTransaction { db ->
            SynchroMeta.setSyncLock(db, true)
            try {
                for (record in records) {
                    val schema = tableMap[record.tableName] ?: continue
                    upsertRecord(db, record, schema)
                    // Use server-provided checksum if available, fall back to local CRC32.
                    val checksum = record.checksum?.toLong() ?: crc32Checksum(record.data)
                    SynchroMeta.setBucketMember(db, bucketId, record.tableName, record.id, checksum)
                }
            } finally {
                SynchroMeta.setSyncLock(db, false)
            }
        }
    }

    /**
     * Clears all bucket membership records for the given bucket. Called before
     * starting a bucket rebuild to remove stale membership data.
     */
    fun clearBucketMembers(bucketId: String) {
        database.writeTransaction { db ->
            SynchroMeta.clearBucketMembers(db, bucketId)
        }
    }

    /**
     * Sets the checkpoint for a specific bucket after a successful rebuild.
     */
    fun updateBucketCheckpoint(bucketId: String, checkpoint: Long) {
        database.writeTransaction { db ->
            SynchroMeta.setBucketCheckpoint(db, bucketId, checkpoint)
        }
    }

    /**
     * Tracks bucket membership for records that carry a bucket_id.
     * When overrideBucketId is provided (during rebuild), all records are
     * assigned to that bucket regardless of their record-level bucket_id.
     */
    fun trackBucketMembership(records: List<Record>, overrideBucketId: String? = null) {
        val entries: List<Pair<Record, String>> = if (overrideBucketId != null) {
            records.map { it to overrideBucketId }
        } else {
            records.mapNotNull { record ->
                val bucketId = record.bucketId ?: return@mapNotNull null
                record to bucketId
            }
        }
        if (entries.isEmpty()) return

        database.writeTransaction { db ->
            for ((record, bucketId) in entries) {
                // Use server-provided checksum if available, fall back to local CRC32.
                val checksum = record.checksum?.toLong() ?: crc32Checksum(record.data)
                SynchroMeta.setBucketMember(db, bucketId, record.tableName, record.id, checksum)
            }
        }
    }

    /**
     * Returns all stored bucket checkpoints.
     */
    fun getAllBucketCheckpoints(): Map<String, Long> {
        return database.readTransaction { db ->
            SynchroMeta.getAllBucketCheckpoints(db)
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

    /**
     * Computes the aggregate checksum for a bucket by XOR-ing all stored
     * per-record checksums from `_synchro_bucket_members`.
     */
    fun computeBucketChecksum(bucketId: String): Int {
        return database.readTransaction { db ->
            var xor = 0
            db.rawQuery(
                "SELECT checksum FROM _synchro_bucket_members WHERE bucket_id = ?",
                arrayOf(bucketId)
            ).use { cursor ->
                while (cursor.moveToNext()) {
                    if (!cursor.isNull(0)) {
                        xor = xor xor cursor.getInt(0)
                    }
                }
            }
            xor
        }
    }

    /**
     * Computes CRC32 (IEEE) checksum from record data. Used as fallback when
     * the server does not provide a checksum (backwards compatibility).
     * Serializes with sorted keys to match the server's canonical JSON.
     */
    private fun crc32Checksum(data: Map<String, AnyCodable>): Long {
        return try {
            // Sort keys for deterministic output matching server's json.Marshal.
            val sorted = data.toSortedMap()
            val obj = kotlinx.serialization.json.buildJsonObject {
                for ((key, value) in sorted) {
                    put(key, toJsonElement(value.value))
                }
            }
            val json = obj.toString()
            val crc = java.util.zip.CRC32()
            crc.update(json.toByteArray(Charsets.UTF_8))
            crc.value
        } catch (_: Exception) {
            0L
        }
    }

    private fun toJsonElement(value: Any?): kotlinx.serialization.json.JsonElement = when (value) {
        null -> kotlinx.serialization.json.JsonNull
        is Boolean -> kotlinx.serialization.json.JsonPrimitive(value)
        is Int -> kotlinx.serialization.json.JsonPrimitive(value)
        is Long -> kotlinx.serialization.json.JsonPrimitive(value)
        is Double -> kotlinx.serialization.json.JsonPrimitive(value)
        is Float -> kotlinx.serialization.json.JsonPrimitive(value.toDouble())
        is String -> kotlinx.serialization.json.JsonPrimitive(value)
        is List<*> -> kotlinx.serialization.json.JsonArray(value.map { toJsonElement(it) })
        is Map<*, *> -> kotlinx.serialization.json.buildJsonObject {
            for ((k, v) in value) put(k.toString(), toJsonElement(v))
        }
        is AnyCodable -> toJsonElement(value.value)
        else -> kotlinx.serialization.json.JsonPrimitive(value.toString())
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
