package com.trainstar.synchro

import android.database.sqlite.SQLiteDatabase

enum class MetaKey(val key: String) {
    CHECKPOINT("checkpoint"),
    SCHEMA_VERSION("schema_version"),
    SCHEMA_HASH("schema_hash"),
    CLIENT_SERVER_ID("client_server_id"),
    KNOWN_BUCKETS("known_buckets"),
    SNAPSHOT_COMPLETE("snapshot_complete"),
    SYNC_LOCK("sync_lock")
}

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
}
