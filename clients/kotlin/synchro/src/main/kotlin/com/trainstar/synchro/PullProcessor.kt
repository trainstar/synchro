package com.trainstar.synchro

import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteProgram
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
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

    fun applyChanges(changes: List<Record>, syncedTables: List<LocalSchemaTable>) {
        if (changes.isEmpty()) return
        val tableMap = syncedTables.associateBy { it.tableName }

        database.writeSyncLockedTransaction { db ->
            for (record in changes) {
                val schema = tableMap[record.tableName] ?: continue
                upsertRecord(db, record, schema)
            }
        }
    }

    fun applyDeletes(deletes: List<DeleteEntry>, syncedTables: List<LocalSchemaTable>) {
        if (deletes.isEmpty()) return
        val tableMap = syncedTables.associateBy { it.tableName }

        database.writeSyncLockedTransaction { db ->
            applyDeletesInTransaction(db, deletes, tableMap)
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
        syncedTables: List<LocalSchemaTable>
    ) {
        if (changes.isEmpty() && deletes.isEmpty()) return
        val tableMap = syncedTables.associateBy { it.tableName }

        database.writeSyncLockedTransaction { db ->
            for (record in changes) {
                val schema = tableMap[record.tableName] ?: continue
                upsertRecord(db, record, schema)
            }
            applyDeletesInTransaction(db, deletes, tableMap)
        }
    }

    private fun applyDeletesInTransaction(
        db: SQLiteDatabase,
        deletes: List<DeleteEntry>,
        tableMap: Map<String, LocalSchemaTable>
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

    fun updateCheckpoint(checkpoint: Long) {
        database.writeTransaction { db ->
            val current = SynchroMeta.getInt64(db, MetaKey.CHECKPOINT)
            if (checkpoint > current) {
                SynchroMeta.setInt64(db, MetaKey.CHECKPOINT, checkpoint)
            }
        }
    }

    fun applyScopeChanges(
        changes: List<ChangeRecord>,
        syncedTables: List<LocalSchemaTable>,
        scopeCursors: Map<String, String>,
        checksums: Map<String, String>?
    ) {
        val checksumMap = checksums ?: emptyMap()
        if (changes.isEmpty() && scopeCursors.isEmpty() && checksumMap.isEmpty()) return
        val tableMap = syncedTables.associateBy { it.tableName }

        database.writeSyncLockedTransaction { db ->
            for (change in changes) {
                val schema = tableMap[change.table] ?: continue
                val recordId = scopeRecordID(change.pk, schema)

                when (change.op) {
                    Operation.DELETE -> applyScopeDeleteChange(db, change, recordId, schema)
                    Operation.INSERT, Operation.UPSERT, Operation.UPDATE -> {
                        val record = scopeRecord(change, schema)
                        upsertRecord(db, record, schema)
                        val generation = SynchroMeta.getScopeGeneration(db, change.scope)
                        SynchroMeta.upsertScopeRow(
                            db,
                            change.scope,
                            change.table,
                            recordId,
                            requiredScopeRowChecksum(change.rowChecksum, change.table, recordId),
                            generation
                        )
                    }
                }
            }

            val scopeIds = (scopeCursors.keys + checksumMap.keys).toSet()
            for (scopeId in scopeIds) {
                val existingScope = SynchroMeta.getScope(db, scopeId) ?: continue
                val nextCursor = scopeCursors[scopeId] ?: existingScope.cursor
                val localChecksum = SynchroMeta.getScopeLocalChecksum(db, scopeId)
                val serverChecksum = checksumMap[scopeId]
                if (serverChecksum != null) {
                    val expectedChecksum = parseScopeChecksum(scopeId, serverChecksum)
                    if (localChecksum == expectedChecksum) {
                        SynchroMeta.upsertScope(
                            db,
                            scopeId = scopeId,
                            cursor = nextCursor,
                            checksum = serverChecksum,
                            generation = existingScope.generation,
                            localChecksum = localChecksum
                        )
                    } else {
                        SynchroMeta.upsertScope(
                            db,
                            scopeId = scopeId,
                            cursor = null,
                            checksum = null,
                            generation = existingScope.generation,
                            localChecksum = localChecksum
                        )
                    }
                    continue
                }
                SynchroMeta.upsertScope(
                    db,
                    scopeId = scopeId,
                    cursor = nextCursor,
                    checksum = existingScope.checksum,
                    generation = existingScope.generation,
                    localChecksum = localChecksum
                )
            }
        }
    }

    fun beginScopeRebuild(scopeId: String): Long {
        return database.writeTransaction { db ->
            SynchroMeta.bumpScopeGeneration(db, scopeId)
        }
    }

    fun applyScopeRebuildPage(scopeId: String, generation: Long, records: List<RebuildRecord>, syncedTables: List<LocalSchemaTable>) {
        if (records.isEmpty()) return
        val tableMap = syncedTables.associateBy { it.tableName }

        database.writeSyncLockedTransaction { db ->
            for (record in records) {
                val schema = tableMap[record.table] ?: continue
                val recordId = scopeRecordID(record.pk, schema)
                val scopedRecord = scopeRecord(record, schema)
                upsertRecord(db, scopedRecord, schema)
                SynchroMeta.upsertScopeRow(
                    db,
                    scopeId,
                    record.table,
                    recordId,
                    requiredScopeRowChecksum(record.rowChecksum, record.table, recordId),
                    generation
                )
            }
        }
    }

    fun finalizeScopeRebuild(scopeId: String, generation: Long, finalCursor: String, checksum: String, syncedTables: List<LocalSchemaTable>) {
        val tableMap = syncedTables.associateBy { it.tableName }

        database.writeSyncLockedTransaction { db ->
            val staleRows = SynchroMeta.getStaleScopeRows(db, scopeId, generation)
            SynchroMeta.deleteStaleScopeRows(db, scopeId, generation)

            for ((tableName, recordId) in staleRows) {
                val schema = tableMap[tableName] ?: continue
                removeLocalRowIfUnreferenced(db, tableName, recordId, schema)
            }

            val localChecksum = SynchroMeta.getScopeLocalChecksum(db, scopeId)
            val expectedChecksum = parseScopeChecksum(scopeId, checksum)
            if (localChecksum != expectedChecksum) {
                throw SynchroError.InvalidResponse("scope checksum mismatch after rebuild for $scopeId")
            }

            SynchroMeta.upsertScope(
                db,
                scopeId,
                finalCursor,
                checksum,
                generation,
                localChecksum
            )
        }
    }

    fun removeScope(scopeId: String, syncedTables: List<LocalSchemaTable>) {
        val tableMap = syncedTables.associateBy { it.tableName }

        database.writeSyncLockedTransaction { db ->
            val scopeRows = SynchroMeta.getScopeRows(db, scopeId)
            SynchroMeta.deleteScopeRows(db, scopeId)
            SynchroMeta.deleteScope(db, scopeId)

            for ((tableName, recordId) in scopeRows) {
                val schema = tableMap[tableName] ?: continue
                removeLocalRowIfUnreferenced(db, tableName, recordId, schema)
            }
        }
    }

    fun clearAllScopeState() {
        database.writeTransaction { db ->
            SynchroMeta.clearAllScopes(db)
            SynchroMeta.clearAllScopeRows(db)
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
        syncedTables: List<LocalSchemaTable>
    ) {
        if (records.isEmpty()) return
        val tableMap = syncedTables.associateBy { it.tableName }

        database.writeSyncLockedTransaction { db ->
            for (record in records) {
                val schema = tableMap[record.tableName] ?: continue
                upsertRecord(db, record, schema)
                val checksum = requiredRecordChecksum(record)
                SynchroMeta.setBucketMember(db, bucketId, record.tableName, record.id, checksum)
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

    fun deleteBucketOrphanedRecords(bucketId: String, syncedTables: List<LocalSchemaTable>) {
        val tableMap = syncedTables.associateBy { it.tableName }

        database.writeSyncLockedTransaction { db ->
            val members = SynchroMeta.getBucketMembers(db, bucketId)
            SynchroMeta.clearBucketMembers(db, bucketId)

            for ((tableName, recordId) in members) {
                val schema = tableMap[tableName] ?: continue
                removeLocalRowIfUnreferenced(db, tableName, recordId, schema)
            }
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
                val checksum = requiredRecordChecksum(record)
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

    private fun requiredRecordChecksum(record: Record): Long {
        return record.checksum?.toLong()
            ?: throw SynchroError.InvalidResponse(
                "missing record checksum for bucket membership ${record.tableName}/${record.id}"
            )
    }

    private fun requiredScopeRowChecksum(checksum: Int?, tableName: String, recordId: String): Int {
        return checksum
            ?: throw SynchroError.InvalidResponse(
                "missing scope row checksum for $tableName/$recordId"
            )
    }

    private fun parseScopeChecksum(scopeId: String, checksum: String): Int {
        return checksum.toIntOrNull()
            ?: throw SynchroError.InvalidResponse("invalid checksum for scope $scopeId")
    }

    // MARK: - Private

    private fun upsertRecord(db: SQLiteDatabase, record: Record, schema: LocalSchemaTable) {
        val pkCol = schema.primaryKey.firstOrNull() ?: "id"
        val quoted = SQLiteHelpers.quoteIdentifier(record.tableName)
        val quotedPK = SQLiteHelpers.quoteIdentifier(pkCol)
        val quotedUpdatedAt = SQLiteHelpers.quoteIdentifier(schema.updatedAtColumn)
        val quotedDeletedAt = SQLiteHelpers.quoteIdentifier(schema.deletedAtColumn)

        db.rawQuery(
            "SELECT $quotedUpdatedAt, $quotedDeletedAt FROM $quoted WHERE $quotedPK = ?",
            arrayOf(record.id)
        ).use { cursor ->
            if (cursor.moveToFirst()) {
                val localUpdatedAt = if (cursor.isNull(0)) null else cursor.getString(0)
                val localDeletedAt = if (cursor.isNull(1)) null else cursor.getString(1)
                val localVersion = effectiveSyncTimestamp(localUpdatedAt, localDeletedAt)
                val serverVersion = record.deletedAt ?: record.updatedAt
                if (localVersion != null) {
                    try {
                        val serverInstant = parseISO8601(serverVersion)
                        if (serverInstant != null && !localVersion.isBefore(serverInstant)) {
                            return
                        }
                    } catch (_: Exception) {
                        // Parse failure, proceed with upsert.
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

    private fun scopeRecord(change: ChangeRecord, schema: LocalSchemaTable): Record {
        val row = change.row ?: throw SynchroError.InvalidResponse("missing row for ${change.table} ${change.op}")
        val recordId = scopeRecordID(change.pk, schema)
        return scopeRecord(change.table, recordId, jsonObjectToAnyMap(row), change.rowChecksum, schema)
    }

    private fun scopeRecord(rebuild: RebuildRecord, schema: LocalSchemaTable): Record {
        val row = rebuild.row ?: throw SynchroError.InvalidResponse("missing rebuild row for ${rebuild.table}")
        val recordId = scopeRecordID(rebuild.pk, schema)
        return scopeRecord(rebuild.table, recordId, jsonObjectToAnyMap(row), rebuild.rowChecksum, schema)
    }

    private fun scopeRecord(
        tableName: String,
        recordId: String,
        row: Map<String, AnyCodable>,
        checksum: Int?,
        schema: LocalSchemaTable
    ): Record {
        val updatedAt = row[schema.updatedAtColumn]?.value as? String
            ?: throw SynchroError.InvalidResponse("missing ${schema.updatedAtColumn} for $tableName")
        val deletedAt = row[schema.deletedAtColumn]?.value as? String

        return Record(
            id = recordId,
            tableName = tableName,
            data = row,
            updatedAt = updatedAt,
            deletedAt = deletedAt,
            bucketId = null,
            checksum = checksum
        )
    }

    private fun scopeRecordID(pk: JsonObject, schema: LocalSchemaTable): String {
        val primaryKey = schema.primaryKey.singleOrNull()
            ?: throw SynchroError.InvalidResponse("composite primary keys are not supported for ${schema.tableName}")
        val value = pk[primaryKey]?.let(::fromJsonElement)
            ?: throw SynchroError.InvalidResponse("missing primary key $primaryKey for ${schema.tableName}")
        return value.toString()
    }

    private fun effectiveSyncTimestamp(updatedAt: String?, deletedAt: String?): Instant? {
        if (deletedAt != null) {
            parseISO8601(deletedAt)?.let { return it }
        }
        if (updatedAt != null) {
            parseISO8601(updatedAt)?.let { return it }
        }
        return null
    }

    private fun jsonObjectToAnyMap(value: JsonObject): Map<String, AnyCodable> {
        return value.mapValues { (_, element) -> AnyCodable(fromJsonElement(element)) }
    }

    private fun fromJsonElement(element: JsonElement): Any? = when (element) {
        is JsonNull -> null
        is JsonPrimitive -> when {
            element.isString -> element.content
            element.content == "true" -> true
            element.content == "false" -> false
            element.content.contains('.') -> element.content.toDoubleOrNull() ?: element.content
            else -> element.content.toLongOrNull() ?: element.content
        }
        is JsonArray -> element.map { fromJsonElement(it) }
        is JsonObject -> element.mapValues { (_, value) -> fromJsonElement(value) }
    }

    private fun applyScopeDeleteChange(
        db: SQLiteDatabase,
        change: ChangeRecord,
        recordId: String,
        schema: LocalSchemaTable
    ) {
        change.row?.let { row ->
            val deletedAt = row[schema.deletedAtColumn]
            if (deletedAt == null || deletedAt is JsonNull) {
                throw SynchroError.InvalidResponse(
                    "delete change for ${change.table} $recordId included a row without ${schema.deletedAtColumn}"
                )
            }
            val record = scopeRecord(change, schema)
            upsertRecord(db, record, schema)
        }

        SynchroMeta.deleteScopeRow(db, change.scope, change.table, recordId)

        if (change.row == null) {
            removeLocalRowIfUnreferenced(db, change.table, recordId, schema)
        }
    }

    private fun removeLocalRowIfUnreferenced(
        db: SQLiteDatabase,
        tableName: String,
        recordId: String,
        schema: LocalSchemaTable
    ) {
        if (SynchroMeta.hasScopeRows(db, tableName, recordId) || SynchroMeta.hasBucketMembers(db, tableName, recordId)) {
            return
        }

        val pkCol = schema.primaryKey.firstOrNull() ?: "id"
        val quoted = SQLiteHelpers.quoteIdentifier(tableName)
        val quotedPK = SQLiteHelpers.quoteIdentifier(pkCol)

        val stmt = db.compileStatement(
            "DELETE FROM $quoted WHERE $quotedPK = ?"
        )
        try {
            stmt.bindString(1, recordId)
            stmt.executeUpdateDelete()
        } finally {
            stmt.close()
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

internal fun bindTypedValues(stmt: SQLiteProgram, values: List<Any?>) {
    for (i in values.indices) {
        val bindIndex = i + 1
        when (val value = sqliteBindValue(values[i], i)) {
            null -> stmt.bindNull(bindIndex)
            is Long -> stmt.bindLong(bindIndex, value)
            is Double -> stmt.bindDouble(bindIndex, value)
            is ByteArray -> stmt.bindBlob(bindIndex, value)
            is String -> stmt.bindString(bindIndex, value)
            else -> throw IllegalArgumentException(
                "Unsupported SQL bind value at index $i: ${value::class.java.name}"
            )
        }
    }
}
