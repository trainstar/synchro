package com.trainstar.synchro

import android.content.Context
import android.database.Cursor
import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteOpenHelper
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArraySet

typealias Row = Map<String, Any?>

class SynchroDatabase(context: Context, dbPath: String) :
    SQLiteOpenHelper(context, dbPath, null, 1) {

    val path: String = dbPath
    private val changeNotifier = ChangeNotifier()

    init {
        // Enable WAL mode
        writableDatabase.enableWriteAheadLogging()
    }

    override fun onCreate(db: SQLiteDatabase) {
        db.execSQL("""
            CREATE TABLE IF NOT EXISTS _synchro_pending_changes (
                record_id TEXT NOT NULL,
                table_name TEXT NOT NULL,
                operation TEXT NOT NULL,
                base_updated_at TEXT,
                client_updated_at TEXT NOT NULL,
                PRIMARY KEY (table_name, record_id)
            )
        """.trimIndent())

        db.execSQL("""
            CREATE TABLE IF NOT EXISTS _synchro_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """.trimIndent())

        db.execSQL("""
            CREATE TABLE IF NOT EXISTS _synchro_bucket_checkpoints (
                bucket_id TEXT PRIMARY KEY,
                checkpoint INTEGER NOT NULL DEFAULT 0
            )
        """.trimIndent())

        db.execSQL("""
            CREATE TABLE IF NOT EXISTS _synchro_bucket_members (
                bucket_id TEXT NOT NULL,
                table_name TEXT NOT NULL,
                record_id TEXT NOT NULL,
                checksum INTEGER,
                PRIMARY KEY (bucket_id, table_name, record_id)
            )
        """.trimIndent())

        db.execSQL("INSERT OR IGNORE INTO _synchro_meta (key, value) VALUES ('sync_lock', '0')")
        db.execSQL("INSERT OR IGNORE INTO _synchro_meta (key, value) VALUES ('checkpoint', '0')")
    }

    override fun onUpgrade(db: SQLiteDatabase, oldVersion: Int, newVersion: Int) {
        // No upgrades yet
    }

    /**
     * Ensures the bucket checkpoint and bucket member tables exist.
     * Safe to call on databases created before bucket support was added,
     * since it uses IF NOT EXISTS.
     */
    fun ensureBucketTables() {
        val db = writableDatabase
        db.execSQL("""
            CREATE TABLE IF NOT EXISTS _synchro_bucket_checkpoints (
                bucket_id TEXT PRIMARY KEY,
                checkpoint INTEGER NOT NULL DEFAULT 0
            )
        """.trimIndent())
        db.execSQL("""
            CREATE TABLE IF NOT EXISTS _synchro_bucket_members (
                bucket_id TEXT NOT NULL,
                table_name TEXT NOT NULL,
                record_id TEXT NOT NULL,
                checksum INTEGER,
                PRIMARY KEY (bucket_id, table_name, record_id)
            )
        """.trimIndent())
    }

    // MARK: - Queries

    fun query(sql: String, params: Array<out Any?>? = null): List<Row> {
        val stringParams = bindableParams(params)
        readableDatabase.rawQuery(sql, stringParams).use { cursor ->
            val rows = mutableListOf<Row>()
            while (cursor.moveToNext()) {
                rows.add(cursorToRow(cursor))
            }
            return rows
        }
    }

    fun queryOne(sql: String, params: Array<out Any?>? = null): Row? {
        val stringParams = bindableParams(params)
        readableDatabase.rawQuery(sql, stringParams).use { cursor ->
            return if (cursor.moveToFirst()) cursorToRow(cursor) else null
        }
    }

    /**
     * Converts params to String array suitable for rawQuery.
     * Android's rawQuery only accepts String[], so we must convert types properly:
     * - null stays null (rawQuery binds NULL for null entries)
     * - Boolean → "1" or "0" (matches SQLite INTEGER convention)
     * - everything else → toString()
     */
    private fun bindableParams(params: Array<out Any?>?): Array<String?>? {
        return params?.map { value ->
            when (value) {
                null -> null
                is Boolean -> if (value) "1" else "0"
                else -> value.toString()
            }
        }?.toTypedArray()
    }

    fun execute(sql: String, params: Array<out Any?>? = null): ExecResult {
        val db = writableDatabase
        val stmt = db.compileStatement(sql)
        if (params != null) {
            for (i in params.indices) {
                val value = params[i]
                val bindIndex = i + 1 // SQLite bind indices are 1-based
                when (value) {
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
        val changes = try {
            stmt.executeUpdateDelete()
        } finally {
            stmt.close()
        }
        notifyTablesChanged(sql)
        return ExecResult(rowsAffected = changes)
    }

    // MARK: - Transactions

    fun <T> readTransaction(block: (SQLiteDatabase) -> T): T {
        val db = readableDatabase
        db.beginTransaction()
        try {
            val result = block(db)
            db.setTransactionSuccessful()
            return result
        } finally {
            db.endTransaction()
        }
    }

    fun <T> writeTransaction(block: (SQLiteDatabase) -> T): T {
        val db = writableDatabase
        db.beginTransaction()
        try {
            val result = block(db)
            db.setTransactionSuccessful()
            return result
        } finally {
            db.endTransaction()
        }
    }

    // MARK: - Batch

    fun executeBatch(statements: List<SQLStatement>): Int {
        val db = writableDatabase
        val affectedTables = mutableSetOf<String>()
        db.beginTransaction()
        try {
            var total = 0
            for (stmt in statements) {
                if (stmt.params != null && stmt.params.isNotEmpty()) {
                    db.execSQL(stmt.sql, stmt.params)
                } else {
                    db.execSQL(stmt.sql)
                }
                total += queryChangesCount(db)
                affectedTables.addAll(extractTablesFromSQL(stmt.sql))
            }
            db.setTransactionSuccessful()
            return total
        } finally {
            db.endTransaction()
            if (affectedTables.isNotEmpty()) {
                changeNotifier.notifyTables(affectedTables)
            }
        }
    }

    // MARK: - Schema (local-only tables)

    fun createTable(name: String, columns: List<ColumnDef>, options: TableOptions? = null) {
        val ifNotExists = options?.ifNotExists ?: true
        val withoutRowid = options?.withoutRowid ?: false
        val quotedName = SQLiteHelpers.quoteIdentifier(name)

        val colDefs = columns.map { col ->
            buildString {
                append("${SQLiteHelpers.quoteIdentifier(col.name)} ${col.type}")
                if (col.primaryKey) append(" PRIMARY KEY")
                if (!col.nullable) append(" NOT NULL")
                if (col.defaultValue != null) append(" DEFAULT ${col.defaultValue}")
            }
        }

        var sql = "CREATE TABLE"
        if (ifNotExists) sql += " IF NOT EXISTS"
        sql += " $quotedName (${colDefs.joinToString(", ")})"
        if (withoutRowid) sql += " WITHOUT ROWID"

        writableDatabase.execSQL(sql)
    }

    fun alterTable(name: String, addColumns: List<ColumnDef>) {
        val quotedName = SQLiteHelpers.quoteIdentifier(name)
        val db = writableDatabase
        for (col in addColumns) {
            var def = "ALTER TABLE $quotedName ADD COLUMN ${SQLiteHelpers.quoteIdentifier(col.name)} ${col.type}"
            if (col.defaultValue != null) {
                if (!col.nullable) def += " NOT NULL"
                def += " DEFAULT ${col.defaultValue}"
            } else if (!col.nullable) {
                def += " NOT NULL DEFAULT ''"
            }
            db.execSQL(def)
        }
    }

    fun createIndex(table: String, columns: List<String>, unique: Boolean = false) {
        val quotedTable = SQLiteHelpers.quoteIdentifier(table)
        val quotedCols = columns.joinToString(", ") { SQLiteHelpers.quoteIdentifier(it) }
        val indexName = SQLiteHelpers.quoteIdentifier("idx_${table}_${columns.joinToString("_")}")
        val uniqueStr = if (unique) "UNIQUE " else ""
        val sql = "CREATE ${uniqueStr}INDEX IF NOT EXISTS $indexName ON $quotedTable ($quotedCols)"
        writableDatabase.execSQL(sql)
    }

    // MARK: - Observation

    fun onChange(tables: List<String>, callback: () -> Unit): Cancellable {
        return changeNotifier.onChange(tables, callback)
    }

    fun watch(
        sql: String,
        params: Array<out Any?>? = null,
        tables: List<String>,
        callback: (List<Row>) -> Unit
    ): Cancellable {
        // Immediate re-query
        val rows = query(sql, params)
        callback(rows)

        // Re-query on each notification
        return changeNotifier.onChange(tables) {
            val updatedRows = query(sql, params)
            callback(updatedRows)
        }
    }

    // MARK: - WAL Checkpoint

    fun checkpoint(mode: CheckpointMode = CheckpointMode.PASSIVE) {
        val modeStr = when (mode) {
            CheckpointMode.PASSIVE -> "PASSIVE"
            CheckpointMode.FULL -> "FULL"
            CheckpointMode.RESTART -> "RESTART"
            CheckpointMode.TRUNCATE -> "TRUNCATE"
        }
        writableDatabase.rawQuery("PRAGMA wal_checkpoint($modeStr)", null).use { it.moveToFirst() }
    }

    // MARK: - Internal

    internal fun notifyTablesChanged(sql: String) {
        val tables = extractTablesFromSQL(sql)
        if (tables.isNotEmpty()) {
            changeNotifier.notifyTables(tables)
        }
    }

    internal fun notifyTablesChanged(tables: Set<String>) {
        changeNotifier.notifyTables(tables)
    }

    private fun queryChangesCount(db: SQLiteDatabase): Int {
        db.rawQuery("SELECT changes()", null).use { cursor ->
            return if (cursor.moveToFirst()) cursor.getInt(0) else 0
        }
    }

    private fun cursorToRow(cursor: Cursor): Row {
        val row = mutableMapOf<String, Any?>()
        for (i in 0 until cursor.columnCount) {
            val name = cursor.getColumnName(i)
            row[name] = when (cursor.getType(i)) {
                Cursor.FIELD_TYPE_NULL -> null
                Cursor.FIELD_TYPE_INTEGER -> cursor.getLong(i)
                Cursor.FIELD_TYPE_FLOAT -> cursor.getDouble(i)
                Cursor.FIELD_TYPE_STRING -> cursor.getString(i)
                Cursor.FIELD_TYPE_BLOB -> cursor.getBlob(i)
                else -> null
            }
        }
        return row
    }

    private fun extractTablesFromSQL(sql: String): Set<String> {
        val tables = mutableSetOf<String>()
        // Simple heuristic: extract table name from common SQL patterns
        val patterns = listOf(
            Regex("""INSERT\s+(?:OR\s+\w+\s+)?INTO\s+["']?(\w+)["']?""", RegexOption.IGNORE_CASE),
            Regex("""UPDATE\s+["']?(\w+)["']?""", RegexOption.IGNORE_CASE),
            Regex("""DELETE\s+FROM\s+["']?(\w+)["']?""", RegexOption.IGNORE_CASE),
        )
        for (pattern in patterns) {
            pattern.findAll(sql).forEach { match ->
                tables.add(match.groupValues[1])
            }
        }
        return tables
    }
}

// MARK: - ChangeNotifier

internal class ChangeNotifier {
    private val listeners = ConcurrentHashMap<String, CopyOnWriteArraySet<() -> Unit>>()

    fun onChange(tables: List<String>, callback: () -> Unit): Cancellable {
        for (table in tables) {
            listeners.getOrPut(table) { CopyOnWriteArraySet() }.add(callback)
        }
        return Cancellable {
            for (table in tables) {
                listeners[table]?.remove(callback)
            }
        }
    }

    fun notifyTables(tables: Set<String>) {
        val callbacks = mutableSetOf<() -> Unit>()
        for (table in tables) {
            listeners[table]?.let { callbacks.addAll(it) }
        }
        for (cb in callbacks) {
            try {
                cb()
            } catch (_: Exception) {
                // One observer must not prevent others from being notified
            }
        }
    }
}
