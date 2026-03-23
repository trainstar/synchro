package com.trainstar.synchro

import android.database.sqlite.SQLiteDatabase

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

    // MARK: - Scope State

    fun getAllScopes(db: SQLiteDatabase): List<LocalScopeState> {
        val result = mutableListOf<LocalScopeState>()
        db.rawQuery(
            "SELECT scope_id, cursor, checksum, generation FROM _synchro_scopes ORDER BY scope_id",
            null
        ).use { cursor ->
            while (cursor.moveToNext()) {
                result.add(
                    LocalScopeState(
                        scopeID = cursor.getString(0),
                        cursor = if (cursor.isNull(1)) null else cursor.getString(1),
                        checksum = if (cursor.isNull(2)) null else cursor.getString(2),
                        generation = cursor.getLong(3),
                    )
                )
            }
        }
        return result
    }

    fun getScopeGeneration(db: SQLiteDatabase, scopeId: String): Long {
        db.rawQuery(
            "SELECT generation FROM _synchro_scopes WHERE scope_id = ?",
            arrayOf(scopeId)
        ).use { cursor ->
            return if (cursor.moveToFirst()) cursor.getLong(0) else 0L
        }
    }

    fun upsertScope(db: SQLiteDatabase, scopeId: String, cursor: String?, checksum: String?, generation: Long? = null) {
        val effectiveGeneration = generation ?: getScopeGeneration(db, scopeId)
        db.execSQL(
            """
            INSERT INTO _synchro_scopes (scope_id, cursor, checksum, generation) VALUES (?, ?, ?, ?)
            ON CONFLICT (scope_id) DO UPDATE SET
                cursor = excluded.cursor,
                checksum = excluded.checksum,
                generation = excluded.generation
            """.trimIndent(),
            arrayOf(scopeId, cursor, checksum, effectiveGeneration.toString())
        )
    }

    fun bumpScopeGeneration(db: SQLiteDatabase, scopeId: String): Long {
        val nextGeneration = getScopeGeneration(db, scopeId) + 1
        upsertScope(db, scopeId, cursor = null, checksum = null, generation = nextGeneration)
        return nextGeneration
    }

    fun deleteScope(db: SQLiteDatabase, scopeId: String) {
        db.execSQL("DELETE FROM _synchro_scopes WHERE scope_id = ?", arrayOf(scopeId))
    }

    fun clearAllScopes(db: SQLiteDatabase) {
        db.execSQL("DELETE FROM _synchro_scopes")
    }

    fun invalidateAllScopes(db: SQLiteDatabase) {
        db.execSQL("UPDATE _synchro_scopes SET cursor = NULL, checksum = NULL, generation = 0")
        clearAllScopeRows(db)
    }

    fun clearAllScopeRows(db: SQLiteDatabase) {
        db.execSQL("DELETE FROM _synchro_scope_rows")
    }

    // MARK: - Scope Rows

    fun upsertScopeRow(db: SQLiteDatabase, scopeId: String, tableName: String, recordId: String, generation: Long) {
        db.execSQL(
            """
            INSERT INTO _synchro_scope_rows (scope_id, table_name, record_id, generation) VALUES (?, ?, ?, ?)
            ON CONFLICT (scope_id, table_name, record_id) DO UPDATE SET generation = excluded.generation
            """.trimIndent(),
            arrayOf(scopeId, tableName, recordId, generation.toString())
        )
    }

    fun deleteScopeRow(db: SQLiteDatabase, scopeId: String, tableName: String, recordId: String) {
        db.execSQL(
            "DELETE FROM _synchro_scope_rows WHERE scope_id = ? AND table_name = ? AND record_id = ?",
            arrayOf(scopeId, tableName, recordId)
        )
    }

    fun deleteScopeRows(db: SQLiteDatabase, scopeId: String) {
        db.execSQL("DELETE FROM _synchro_scope_rows WHERE scope_id = ?", arrayOf(scopeId))
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
    }

    fun hasScopeRows(db: SQLiteDatabase, tableName: String, recordId: String): Boolean {
        db.rawQuery(
            "SELECT 1 FROM _synchro_scope_rows WHERE table_name = ? AND record_id = ? LIMIT 1",
            arrayOf(tableName, recordId)
        ).use { cursor ->
            return cursor.moveToFirst()
        }
    }
}
