package com.trainstar.synchro

import android.database.sqlite.SQLiteDatabase
import java.time.Instant
import java.time.format.DateTimeFormatter

enum class MetaKey(val key: String) {
    CHECKPOINT("checkpoint"),
    SCHEMA_VERSION("schema_version"),
    SCHEMA_HASH("schema_hash"),
    LOCAL_SCHEMA("local_schema"),
    CLIENT_SERVER_ID("client_server_id"),
    SCOPE_SET_VERSION("scope_set_version"),
    KNOWN_BUCKETS("known_buckets"),
    SNAPSHOT_COMPLETE("snapshot_complete"),
    SYNC_LOCK("sync_lock")
}

data class LocalScopeState(
    val scopeID: String,
    val cursor: String?,
    val checksum: String?,
    val generation: Long,
    val localChecksum: Int,
)

data class LocalRejectedMutation(
    val mutationID: String,
    val tableName: String,
    val recordID: String,
    val status: String,
    val code: String,
    val message: String?,
    val serverRowJson: String?,
    val serverVersion: String?,
    val createdAt: String,
    val updatedAt: String,
)

object SynchroMeta {
    fun get(db: SQLiteDatabase, key: MetaKey): String? {
        db.rawQuery(
            "SELECT value FROM _synchro_meta WHERE key = ?",
            arrayOf(key.key)
        ).use { cursor ->
            return if (cursor.moveToFirst()) cursor.getString(0) else null
        }
    }

    fun set(db: SQLiteDatabase, key: MetaKey, value: String) {
        db.execSQL(
            """
            INSERT INTO _synchro_meta (key, value) VALUES (?, ?)
            ON CONFLICT (key) DO UPDATE SET value = excluded.value
            """.trimIndent(),
            arrayOf(key.key, value)
        )
    }

    fun getInt64(db: SQLiteDatabase, key: MetaKey): Long {
        val str = get(db, key) ?: return 0L
        return str.toLongOrNull() ?: 0L
    }

    fun setInt64(db: SQLiteDatabase, key: MetaKey, value: Long) {
        set(db, key, value.toString())
    }

    fun setSyncLock(db: SQLiteDatabase, locked: Boolean) {
        set(db, MetaKey.SYNC_LOCK, if (locked) "1" else "0")
    }

    fun isSyncLocked(db: SQLiteDatabase): Boolean {
        return get(db, MetaKey.SYNC_LOCK) == "1"
    }

    // MARK: - Bucket Checkpoints

    fun getBucketCheckpoint(db: SQLiteDatabase, bucketId: String): Long {
        db.rawQuery(
            "SELECT checkpoint FROM _synchro_bucket_checkpoints WHERE bucket_id = ?",
            arrayOf(bucketId)
        ).use { cursor ->
            return if (cursor.moveToFirst()) cursor.getLong(0) else 0L
        }
    }

    fun setBucketCheckpoint(db: SQLiteDatabase, bucketId: String, checkpoint: Long) {
        db.execSQL(
            """
            INSERT INTO _synchro_bucket_checkpoints (bucket_id, checkpoint) VALUES (?, ?)
            ON CONFLICT (bucket_id) DO UPDATE SET checkpoint = excluded.checkpoint
            """.trimIndent(),
            arrayOf(bucketId, checkpoint.toString())
        )
    }

    fun getAllBucketCheckpoints(db: SQLiteDatabase): Map<String, Long> {
        val result = mutableMapOf<String, Long>()
        db.rawQuery("SELECT bucket_id, checkpoint FROM _synchro_bucket_checkpoints", null).use { cursor ->
            while (cursor.moveToNext()) {
                result[cursor.getString(0)] = cursor.getLong(1)
            }
        }
        return result
    }

    fun deleteBucketCheckpoint(db: SQLiteDatabase, bucketId: String) {
        db.execSQL(
            "DELETE FROM _synchro_bucket_checkpoints WHERE bucket_id = ?",
            arrayOf(bucketId)
        )
    }

    fun clearAllBucketCheckpoints(db: SQLiteDatabase) {
        db.execSQL("DELETE FROM _synchro_bucket_checkpoints")
    }

    // MARK: - Bucket Members

    fun setBucketMember(db: SQLiteDatabase, bucketId: String, tableName: String, recordId: String, checksum: Long?) {
        if (checksum != null) {
            db.execSQL(
                """
                INSERT INTO _synchro_bucket_members (bucket_id, table_name, record_id, checksum) VALUES (?, ?, ?, ?)
                ON CONFLICT (bucket_id, table_name, record_id) DO UPDATE SET checksum = excluded.checksum
                """.trimIndent(),
                arrayOf(bucketId, tableName, recordId, checksum.toString())
            )
        } else {
            db.execSQL(
                """
                INSERT INTO _synchro_bucket_members (bucket_id, table_name, record_id, checksum) VALUES (?, ?, ?, NULL)
                ON CONFLICT (bucket_id, table_name, record_id) DO UPDATE SET checksum = excluded.checksum
                """.trimIndent(),
                arrayOf(bucketId, tableName, recordId)
            )
        }
    }

    fun clearBucketMembers(db: SQLiteDatabase, bucketId: String) {
        db.execSQL(
            "DELETE FROM _synchro_bucket_members WHERE bucket_id = ?",
            arrayOf(bucketId)
        )
    }

    fun getBucketMembers(db: SQLiteDatabase, bucketId: String): List<Pair<String, String>> {
        val result = mutableListOf<Pair<String, String>>()
        db.rawQuery(
            "SELECT table_name, record_id FROM _synchro_bucket_members WHERE bucket_id = ?",
            arrayOf(bucketId)
        ).use { cursor ->
            while (cursor.moveToNext()) {
                result.add(Pair(cursor.getString(0), cursor.getString(1)))
            }
        }
        return result
    }

    fun hasBucketMembers(db: SQLiteDatabase, tableName: String, recordId: String): Boolean {
        db.rawQuery(
            "SELECT 1 FROM _synchro_bucket_members WHERE table_name = ? AND record_id = ? LIMIT 1",
            arrayOf(tableName, recordId)
        ).use { cursor ->
            return cursor.moveToFirst()
        }
    }

    // MARK: - Scope State

    fun getAllScopes(db: SQLiteDatabase): List<LocalScopeState> {
        val result = mutableListOf<LocalScopeState>()
        db.rawQuery(
            "SELECT scope_id, cursor, checksum, generation, local_checksum FROM _synchro_scopes ORDER BY scope_id",
            null
        ).use { cursor ->
            while (cursor.moveToNext()) {
                result.add(
                    LocalScopeState(
                        scopeID = cursor.getString(0),
                        cursor = if (cursor.isNull(1)) null else cursor.getString(1),
                        checksum = if (cursor.isNull(2)) null else cursor.getString(2),
                        generation = cursor.getLong(3),
                        localChecksum = cursor.getInt(4),
                    )
                )
            }
        }
        return result
    }

    fun getScope(db: SQLiteDatabase, scopeId: String): LocalScopeState? {
        db.rawQuery(
            "SELECT scope_id, cursor, checksum, generation, local_checksum FROM _synchro_scopes WHERE scope_id = ?",
            arrayOf(scopeId)
        ).use { cursor ->
            if (!cursor.moveToFirst()) {
                return null
            }
            return LocalScopeState(
                scopeID = cursor.getString(0),
                cursor = if (cursor.isNull(1)) null else cursor.getString(1),
                checksum = if (cursor.isNull(2)) null else cursor.getString(2),
                generation = cursor.getLong(3),
                localChecksum = cursor.getInt(4)
            )
        }
    }

    fun getScopeGeneration(db: SQLiteDatabase, scopeId: String): Long {
        db.rawQuery(
            "SELECT generation FROM _synchro_scopes WHERE scope_id = ?",
            arrayOf(scopeId)
        ).use { cursor ->
            return if (cursor.moveToFirst()) cursor.getLong(0) else 0L
        }
    }

    fun upsertScope(
        db: SQLiteDatabase,
        scopeId: String,
        cursor: String?,
        checksum: String?,
        generation: Long? = null,
        localChecksum: Int? = null
    ) {
        val effectiveGeneration = generation ?: getScopeGeneration(db, scopeId)
        val effectiveLocalChecksum = localChecksum ?: getScopeLocalChecksum(db, scopeId)
        db.execSQL(
            """
            INSERT INTO _synchro_scopes (scope_id, cursor, checksum, generation, local_checksum) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (scope_id) DO UPDATE SET
                cursor = excluded.cursor,
                checksum = excluded.checksum,
                generation = excluded.generation,
                local_checksum = excluded.local_checksum
            """.trimIndent(),
            arrayOf(scopeId, cursor, checksum, effectiveGeneration.toString(), effectiveLocalChecksum.toString())
        )
    }

    fun bumpScopeGeneration(db: SQLiteDatabase, scopeId: String): Long {
        val nextGeneration = getScopeGeneration(db, scopeId) + 1
        upsertScope(
            db,
            scopeId,
            cursor = null,
            checksum = null,
            generation = nextGeneration,
            localChecksum = 0
        )
        return nextGeneration
    }

    fun deleteScope(db: SQLiteDatabase, scopeId: String) {
        db.execSQL("DELETE FROM _synchro_scopes WHERE scope_id = ?", arrayOf(scopeId))
    }

    fun clearAllScopes(db: SQLiteDatabase) {
        db.execSQL("DELETE FROM _synchro_scopes")
    }

    fun invalidateAllScopes(db: SQLiteDatabase) {
        db.execSQL("UPDATE _synchro_scopes SET cursor = NULL, checksum = NULL, generation = 0, local_checksum = 0")
        clearAllScopeRows(db)
    }

    fun clearAllScopeRows(db: SQLiteDatabase) {
        db.execSQL("DELETE FROM _synchro_scope_rows")
        db.execSQL("UPDATE _synchro_scopes SET local_checksum = 0")
    }

    // MARK: - Rejected Mutations

    fun upsertRejectedMutation(
        db: SQLiteDatabase,
        mutationID: String,
        tableName: String,
        recordId: String,
        status: String,
        code: String,
        message: String?,
        serverRowJson: String?,
        serverVersion: String?
    ) {
        val now = timestampNow()
        db.execSQL(
            """
            INSERT INTO _synchro_rejected_mutations
                (mutation_id, table_name, record_id, status, code, message, server_row_json, server_version, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (mutation_id) DO UPDATE SET
                table_name = excluded.table_name,
                record_id = excluded.record_id,
                status = excluded.status,
                code = excluded.code,
                message = excluded.message,
                server_row_json = excluded.server_row_json,
                server_version = excluded.server_version,
                updated_at = excluded.updated_at
            """.trimIndent(),
            arrayOf(
                mutationID,
                tableName,
                recordId,
                status,
                code,
                message,
                serverRowJson,
                serverVersion,
                now,
                now,
            )
        )
    }

    fun listRejectedMutations(db: SQLiteDatabase): List<LocalRejectedMutation> {
        val result = mutableListOf<LocalRejectedMutation>()
        db.rawQuery(
            """
            SELECT mutation_id, table_name, record_id, status, code, message, server_row_json, server_version, created_at, updated_at
            FROM _synchro_rejected_mutations
            ORDER BY created_at, mutation_id
            """.trimIndent(),
            null
        ).use { cursor ->
            while (cursor.moveToNext()) {
                result.add(
                    LocalRejectedMutation(
                        mutationID = cursor.getString(0),
                        tableName = cursor.getString(1),
                        recordID = cursor.getString(2),
                        status = cursor.getString(3),
                        code = cursor.getString(4),
                        message = if (cursor.isNull(5)) null else cursor.getString(5),
                        serverRowJson = if (cursor.isNull(6)) null else cursor.getString(6),
                        serverVersion = if (cursor.isNull(7)) null else cursor.getString(7),
                        createdAt = cursor.getString(8),
                        updatedAt = cursor.getString(9),
                    )
                )
            }
        }
        return result
    }

    fun clearRejectedMutations(db: SQLiteDatabase) {
        db.execSQL("DELETE FROM _synchro_rejected_mutations")
    }

    // MARK: - Scope Rows

    fun upsertScopeRow(
        db: SQLiteDatabase,
        scopeId: String,
        tableName: String,
        recordId: String,
        checksum: Int,
        generation: Long
    ) {
        val existingChecksum = getScopeRowChecksum(db, scopeId, tableName, recordId) ?: 0
        val nextLocalChecksum = xorChecksum(
            xorChecksum(getScopeLocalChecksum(db, scopeId), existingChecksum),
            checksum
        )
        db.execSQL(
            """
            INSERT INTO _synchro_scope_rows (scope_id, table_name, record_id, checksum, generation) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (scope_id, table_name, record_id) DO UPDATE SET
                checksum = excluded.checksum,
                generation = excluded.generation
            """.trimIndent(),
            arrayOf(scopeId, tableName, recordId, checksum.toString(), generation.toString())
        )
        setScopeLocalChecksum(db, scopeId, nextLocalChecksum)
    }

    fun deleteScopeRow(db: SQLiteDatabase, scopeId: String, tableName: String, recordId: String) {
        val existingChecksum = getScopeRowChecksum(db, scopeId, tableName, recordId) ?: 0
        db.execSQL(
            "DELETE FROM _synchro_scope_rows WHERE scope_id = ? AND table_name = ? AND record_id = ?",
            arrayOf(scopeId, tableName, recordId)
        )
        setScopeLocalChecksum(db, scopeId, xorChecksum(getScopeLocalChecksum(db, scopeId), existingChecksum))
    }

    fun deleteScopeRows(db: SQLiteDatabase, scopeId: String) {
        db.execSQL("DELETE FROM _synchro_scope_rows WHERE scope_id = ?", arrayOf(scopeId))
        setScopeLocalChecksum(db, scopeId, 0)
    }

    fun getScopeRows(db: SQLiteDatabase, scopeId: String): List<Pair<String, String>> {
        val result = mutableListOf<Pair<String, String>>()
        db.rawQuery(
            "SELECT table_name, record_id FROM _synchro_scope_rows WHERE scope_id = ?",
            arrayOf(scopeId)
        ).use { cursor ->
            while (cursor.moveToNext()) {
                result.add(Pair(cursor.getString(0), cursor.getString(1)))
            }
        }
        return result
    }

    fun getStaleScopeRows(db: SQLiteDatabase, scopeId: String, generation: Long): List<Pair<String, String>> {
        val result = mutableListOf<Pair<String, String>>()
        db.rawQuery(
            "SELECT table_name, record_id FROM _synchro_scope_rows WHERE scope_id = ? AND generation <> ?",
            arrayOf(scopeId, generation.toString())
        ).use { cursor ->
            while (cursor.moveToNext()) {
                result.add(Pair(cursor.getString(0), cursor.getString(1)))
            }
        }
        return result
    }

    fun deleteStaleScopeRows(db: SQLiteDatabase, scopeId: String, generation: Long) {
        db.execSQL(
            "DELETE FROM _synchro_scope_rows WHERE scope_id = ? AND generation <> ?",
            arrayOf(scopeId, generation.toString())
        )
        recomputeScopeLocalChecksum(db, scopeId)
    }

    fun hasScopeRows(db: SQLiteDatabase, tableName: String, recordId: String): Boolean {
        db.rawQuery(
            "SELECT 1 FROM _synchro_scope_rows WHERE table_name = ? AND record_id = ? LIMIT 1",
            arrayOf(tableName, recordId)
        ).use { cursor ->
            return cursor.moveToFirst()
        }
    }

    fun getScopeLocalChecksum(db: SQLiteDatabase, scopeId: String): Int {
        db.rawQuery(
            "SELECT local_checksum FROM _synchro_scopes WHERE scope_id = ?",
            arrayOf(scopeId)
        ).use { cursor ->
            return if (cursor.moveToFirst()) cursor.getInt(0) else 0
        }
    }

    private fun setScopeLocalChecksum(db: SQLiteDatabase, scopeId: String, checksum: Int) {
        db.execSQL(
            "UPDATE _synchro_scopes SET local_checksum = ? WHERE scope_id = ?",
            arrayOf(checksum.toString(), scopeId)
        )
    }

    private fun getScopeRowChecksum(
        db: SQLiteDatabase,
        scopeId: String,
        tableName: String,
        recordId: String
    ): Int? {
        db.rawQuery(
            "SELECT checksum FROM _synchro_scope_rows WHERE scope_id = ? AND table_name = ? AND record_id = ?",
            arrayOf(scopeId, tableName, recordId)
        ).use { cursor ->
            return if (cursor.moveToFirst()) cursor.getInt(0) else null
        }
    }

    private fun recomputeScopeLocalChecksum(db: SQLiteDatabase, scopeId: String) {
        var aggregate = 0
        db.rawQuery(
            "SELECT checksum FROM _synchro_scope_rows WHERE scope_id = ?",
            arrayOf(scopeId)
        ).use { cursor ->
            while (cursor.moveToNext()) {
                aggregate = xorChecksum(aggregate, cursor.getInt(0))
            }
        }
        setScopeLocalChecksum(db, scopeId, aggregate)
    }

    private fun xorChecksum(lhs: Int, rhs: Int): Int = lhs xor rhs

    private fun timestampNow(): String =
        DateTimeFormatter.ISO_INSTANT.format(Instant.now())
}
