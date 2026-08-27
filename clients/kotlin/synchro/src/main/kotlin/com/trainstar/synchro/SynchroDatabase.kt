package com.trainstar.synchro

import android.content.Context
import android.database.Cursor
import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteOpenHelper
import android.database.sqlite.SQLiteCursor
import com.trainstar.synchro.inspection.ProvenanceMaintenanceWorkInspection
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

typealias Row = Map<String, Any?>

private class ProvenanceMaintenanceTransaction(
    val database: SQLiteDatabase,
    val cursor: AtomicLong,
) {
    var affectedRows = 0L
    var failed = false
}

/** Tracks scope-row maintenance only while its enclosing write transaction is active. */
internal object ProvenanceMaintenanceWork {
    private val transactions = ThreadLocal<ArrayDeque<ProvenanceMaintenanceTransaction>>()

    fun begin(database: SQLiteDatabase, cursor: AtomicLong) {
        val active = transactions.get() ?: ArrayDeque<ProvenanceMaintenanceTransaction>().also(transactions::set)
        active.addLast(ProvenanceMaintenanceTransaction(database, cursor))
    }

    fun record(database: SQLiteDatabase, affectedRows: Int) {
        require(affectedRows >= 0) { "scope-row affected count is invalid" }
        val transaction = transactions.get()?.lastOrNull { it.database === database } ?: return
        transaction.affectedRows = saturatingAdd(transaction.affectedRows, affectedRows.toLong())
    }

    fun end(database: SQLiteDatabase, committed: Boolean) {
        val active = transactions.get() ?: return
        val transaction = active.removeLastOrNull()
            ?: throw IllegalStateException("scope-row maintenance transaction is missing")
        check(transaction.database === database) { "scope-row maintenance transaction order is invalid" }
        val succeeded = committed && !transaction.failed
        val parent = active.lastOrNull { it.database === database }
        if (parent != null) {
            if (succeeded) {
                parent.affectedRows = saturatingAdd(parent.affectedRows, transaction.affectedRows)
            } else {
                parent.failed = true
            }
        } else if (succeeded && transaction.affectedRows > 0L) {
            transaction.cursor.updateAndGet { current -> saturatingAdd(current, transaction.affectedRows) }
        }
        if (active.isEmpty()) transactions.remove()
    }

    private fun saturatingAdd(left: Long, right: Long): Long =
        if (Long.MAX_VALUE - left < right) Long.MAX_VALUE else left + right
}

/**
 * Owns the raw SQLite helper. The wrapper deliberately does not inherit
 * SQLiteOpenHelper, so published consumers cannot obtain a writable handle.
 */
internal class SynchroDatabase private constructor(context: Context, dbPath: String) {

    val path: String = dbPath
    private val changeNotifier = ChangeNotifier()
    private val writeOwnership = ReentrantLock(true)
    private val applicationTransactionDepth = ThreadLocal<Int>()
    private val provenanceMaintenanceCursor = AtomicLong()
    private val helper = object : SQLiteOpenHelper(context, dbPath, null, DATABASE_VERSION) {
        override fun onCreate(db: SQLiteDatabase) = createDatabase(db)

        override fun onUpgrade(db: SQLiteDatabase, oldVersion: Int, newVersion: Int) =
            upgradeDatabase(db, oldVersion)
    }
    private val readableDatabase: SQLiteDatabase
        get() = helper.readableDatabase
    private val writableDatabase: SQLiteDatabase
        get() = helper.writableDatabase

    init {
        // Initialization and migrations precede sampling, so their scope-row work is not tracked.
        writableDatabase.enableWriteAheadLogging()
    }

    internal fun close() = helper.close()

    internal fun inspectProvenanceMaintenanceWork(): ProvenanceMaintenanceWorkInspection =
        ProvenanceMaintenanceWorkInspection(provenanceMaintenanceCursor.get())

    internal fun <T> stateInspectionTransaction(
        block: (SQLiteDatabase, ProvenanceMaintenanceWorkInspection) -> T,
    ): T = writeOwnership.withLock {
        readTransaction { db -> block(db, inspectProvenanceMaintenanceWork()) }
    }

    private fun createDatabase(db: SQLiteDatabase) {
        db.execSQL("""
            CREATE TABLE IF NOT EXISTS _synchro_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """.trimIndent())

        createScopeTables(db)

        db.execSQL("""
            CREATE TABLE IF NOT EXISTS _synchro_rejected_mutations (
                mutation_id TEXT PRIMARY KEY,
                table_name TEXT NOT NULL,
                record_id TEXT NOT NULL,
                status TEXT NOT NULL,
                code TEXT NOT NULL,
                message TEXT,
                server_row_json TEXT,
                server_version TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                mutation_json TEXT,
                rejection_json TEXT
            )
        """.trimIndent())

        db.execSQL("""
            CREATE INDEX IF NOT EXISTS idx_synchro_rejected_mutations_record
            ON _synchro_rejected_mutations (table_name, record_id)
        """.trimIndent())

        db.execSQL("INSERT OR IGNORE INTO _synchro_meta (key, value) VALUES ('sync_lock', '0')")
        db.execSQL("INSERT OR IGNORE INTO _synchro_meta (key, value) VALUES ('checkpoint', '0')")
        createMutationLedgerTables(db)
        migrateRejectedMutations(db)
        createProtocolThreeTables(db)
        createBackoffTable(db)
        createClientStateTable(db)
        createMigrationJournalTable(db)
    }

    private fun upgradeDatabase(db: SQLiteDatabase, oldVersion: Int) {
        if (oldVersion < 2) {
            ensureLegacyScopeTables(db)
            migrateScopeIntegrity(db)
        }
        if (oldVersion < 3) {
            migrateRejectedMutations(db)
        }
        if (oldVersion < 4) {
            invalidateProtocolState(db)
            db.execSQL("DROP TABLE IF EXISTS _synchro_bucket_members")
            db.execSQL("DROP TABLE IF EXISTS _synchro_bucket_checkpoints")
            createProtocolThreeTables(db)
        }
        if (oldVersion < 5) {
            migratePendingLocalRevision(db)
        }
        if (oldVersion < 6) {
            createSealedPushBatchTable(db)
        }
        if (oldVersion < 7) {
            migrateMutationLedger(db)
        }
        if (oldVersion < 8) {
            createRebuildPageReceiptTable(db)
        }
        if (oldVersion < 9) {
            createBackoffTable(db)
        }
        if (oldVersion < 10) {
            createClientStateTable(db)
        }
        if (oldVersion < 11) {
            createMigrationJournalTable(db)
        }
        if (oldVersion < 12) {
            migrateClientStateFailureMetadata(db)
        }
        if (oldVersion < 13) {
            migrateScopeTextAffinity(db)
        }
    }

    private fun createScopeTables(db: SQLiteDatabase) {
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _synchro_scopes (
                scope_id TEXT PRIMARY KEY,
                cursor TEXT,
                checksum TEXT,
                generation INTEGER NOT NULL DEFAULT 0,
                local_checksum TEXT NOT NULL DEFAULT ''
            )
            """.trimIndent(),
        )
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _synchro_scope_rows (
                scope_id TEXT NOT NULL,
                table_name TEXT NOT NULL,
                record_id TEXT NOT NULL,
                checksum TEXT NOT NULL,
                generation INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (scope_id, table_name, record_id)
            )
            """.trimIndent(),
        )
        db.execSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_synchro_scope_rows_record
            ON _synchro_scope_rows (table_name, record_id)
            """.trimIndent(),
        )
    }

    private fun ensureLegacyScopeTables(db: SQLiteDatabase) {
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _synchro_scopes (
                scope_id TEXT PRIMARY KEY,
                cursor TEXT,
                checksum TEXT,
                generation INTEGER NOT NULL DEFAULT 0,
                local_checksum INTEGER NOT NULL DEFAULT 0
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _synchro_scope_rows (
                scope_id TEXT NOT NULL,
                table_name TEXT NOT NULL,
                record_id TEXT NOT NULL,
                checksum INTEGER NOT NULL DEFAULT 0,
                generation INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (scope_id, table_name, record_id)
            )
            """.trimIndent()
        )
    }

    private fun invalidateProtocolState(db: SQLiteDatabase) {
        if (hasTable(db, "_synchro_scopes")) {
            db.execSQL(
                "UPDATE _synchro_scopes SET cursor = NULL, checksum = NULL, generation = 0, local_checksum = ''"
            )
        }
        if (hasTable(db, "_synchro_scope_rows")) {
            db.execSQL("DELETE FROM _synchro_scope_rows")
        }
        if (hasTable(db, "_synchro_rebuild_attempts")) {
            db.execSQL("DELETE FROM _synchro_rebuild_attempts")
        }
    }

    private fun migrateScopeIntegrity(db: SQLiteDatabase) {
        if (!hasTable(db, "_synchro_scopes") || !hasTable(db, "_synchro_scope_rows")) return
        var schemaChanged = false
        if (!hasColumn(db, "_synchro_scopes", "local_checksum")) {
            db.execSQL("ALTER TABLE _synchro_scopes ADD COLUMN local_checksum INTEGER NOT NULL DEFAULT 0")
            schemaChanged = true
        }
        if (!hasColumn(db, "_synchro_scope_rows", "checksum")) {
            db.execSQL("ALTER TABLE _synchro_scope_rows ADD COLUMN checksum INTEGER NOT NULL DEFAULT 0")
            schemaChanged = true
        }
        if (schemaChanged) {
            db.execSQL("UPDATE _synchro_scopes SET cursor = NULL, checksum = NULL, generation = 0, local_checksum = 0")
            db.execSQL("DELETE FROM _synchro_scope_rows")
        }
    }

    private fun migrateRejectedMutations(db: SQLiteDatabase) {
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _synchro_rejected_mutations (
                mutation_id TEXT PRIMARY KEY,
                table_name TEXT NOT NULL,
                record_id TEXT NOT NULL,
                status TEXT NOT NULL,
                code TEXT NOT NULL,
                message TEXT,
                server_row_json TEXT,
                server_version TEXT,
                mutation_json TEXT,
                rejection_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_synchro_rejected_mutations_record
            ON _synchro_rejected_mutations (table_name, record_id)
            """.trimIndent()
        )
        if (!hasColumn(db, "_synchro_rejected_mutations", "mutation_json")) {
            db.execSQL("ALTER TABLE _synchro_rejected_mutations ADD COLUMN mutation_json TEXT")
        }
        if (!hasColumn(db, "_synchro_rejected_mutations", "rejection_json")) {
            db.execSQL("ALTER TABLE _synchro_rejected_mutations ADD COLUMN rejection_json TEXT")
        }
    }

    private fun migratePendingLocalRevision(db: SQLiteDatabase) {
        if (!hasColumn(db, "_synchro_pending_changes", "local_revision")) {
            db.execSQL(
                "ALTER TABLE _synchro_pending_changes ADD COLUMN local_revision INTEGER NOT NULL DEFAULT 0"
            )
        }
    }

    private fun migrateClientStateFailureMetadata(db: SQLiteDatabase) {
        if (!hasTable(db, "_synchro_client_state")) {
            createClientStateTable(db)
            return
        }
        if (!hasColumn(db, "_synchro_client_state", "error_message")) {
            db.execSQL("ALTER TABLE _synchro_client_state ADD COLUMN error_message TEXT")
        }
        if (!hasColumn(db, "_synchro_client_state", "error_recovery_action")) {
            db.execSQL("ALTER TABLE _synchro_client_state ADD COLUMN error_recovery_action TEXT")
        }
        db.execSQL(
            """
            UPDATE _synchro_client_state
            SET error_operation = CASE error_operation
                    WHEN 'schema_applying' THEN 'schema'
                    WHEN 'uninitialized' THEN 'lifecycle'
                    WHEN 'local_ready' THEN 'lifecycle'
                    WHEN 'ready' THEN 'lifecycle'
                    WHEN 'backoff' THEN 'lifecycle'
                    WHEN 'error' THEN 'lifecycle'
                    WHEN 'stopped' THEN 'lifecycle'
                    ELSE error_operation
                END,
                error_message = COALESCE(
                    error_message,
                    CASE error_code
                        WHEN 'upgrade_required' THEN 'The installed schema requires an explicit synchronized reset.'
                        ELSE 'The sync operation failed contract validation.'
                    END
                ),
                error_recovery_action = COALESCE(
                    error_recovery_action,
                    CASE error_code
                        WHEN 'upgrade_required' THEN 'schema_reset'
                        ELSE 'retry'
                    END
                ),
                error_diagnostics = COALESCE(error_diagnostics, '{}')
            WHERE error_operation IS NOT NULL
              AND error_code IS NOT NULL
              AND error_retryable IN (0, 1)
            """.trimIndent(),
        )
    }

    private fun migrateScopeTextAffinity(db: SQLiteDatabase) {
        check(hasTable(db, "_synchro_scopes") && hasTable(db, "_synchro_scope_rows")) {
            "scope state is missing before the version 13 migration"
        }
        db.execSQL(
            """
            CREATE TABLE _synchro_scopes_v13 (
                scope_id TEXT PRIMARY KEY,
                cursor TEXT,
                checksum TEXT,
                generation INTEGER NOT NULL DEFAULT 0,
                local_checksum TEXT NOT NULL DEFAULT ''
            )
            """.trimIndent(),
        )
        db.execSQL(
            """
            INSERT INTO _synchro_scopes_v13
                (scope_id, cursor, checksum, generation, local_checksum)
            SELECT scope_id, cursor, CAST(checksum AS TEXT), generation, CAST(local_checksum AS TEXT)
            FROM _synchro_scopes
            """.trimIndent(),
        )
        db.execSQL(
            """
            CREATE TABLE _synchro_scope_rows_v13 (
                scope_id TEXT NOT NULL,
                table_name TEXT NOT NULL,
                record_id TEXT NOT NULL,
                checksum TEXT NOT NULL,
                generation INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (scope_id, table_name, record_id)
            )
            """.trimIndent(),
        )
        db.execSQL(
            """
            INSERT INTO _synchro_scope_rows_v13
                (scope_id, table_name, record_id, checksum, generation)
            SELECT scope_id, table_name, record_id, CAST(checksum AS TEXT), generation
            FROM _synchro_scope_rows
            """.trimIndent(),
        )
        db.execSQL("DROP TABLE _synchro_scope_rows")
        db.execSQL("DROP TABLE _synchro_scopes")
        createScopeTables(db)
        db.execSQL(
            """
            INSERT INTO _synchro_scopes
                (scope_id, cursor, checksum, generation, local_checksum)
            SELECT scope_id, cursor, checksum, generation, local_checksum
            FROM _synchro_scopes_v13
            """.trimIndent(),
        )
        db.execSQL(
            """
            INSERT INTO _synchro_scope_rows
                (scope_id, table_name, record_id, checksum, generation)
            SELECT scope_id, table_name, record_id, checksum, generation
            FROM _synchro_scope_rows_v13
            """.trimIndent(),
        )
        db.execSQL("DROP TABLE _synchro_scope_rows_v13")
        db.execSQL("DROP TABLE _synchro_scopes_v13")
    }

    private fun hasTable(db: SQLiteDatabase, tableName: String): Boolean {
        db.rawQuery(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            arrayOf(tableName),
        ).use { cursor -> return cursor.moveToFirst() }
    }

    private fun hasColumn(db: SQLiteDatabase, tableName: String, columnName: String): Boolean {
        db.rawQuery("PRAGMA table_info(${SQLiteHelpers.quoteIdentifier(tableName)})", null).use { cursor ->
            val nameIndex = cursor.getColumnIndex("name")
            while (cursor.moveToNext()) {
                if (cursor.getString(nameIndex) == columnName) return true
            }
        }
        return false
    }

    private fun createProtocolThreeTables(db: SQLiteDatabase) {
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _synchro_row_versions (
                table_name TEXT NOT NULL,
                record_id TEXT NOT NULL,
                server_version TEXT NOT NULL,
                row_checksum TEXT,
                PRIMARY KEY (table_name, record_id)
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _synchro_seed_receipts (
                scope_id TEXT PRIMARY KEY,
                receipt TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                schema_hash TEXT NOT NULL,
                cardinality INTEGER NOT NULL,
                checksum TEXT NOT NULL
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _synchro_rebuild_attempts (
                scope_id TEXT PRIMARY KEY,
                rebuild_id TEXT NOT NULL,
                client_generation INTEGER NOT NULL,
                schema_version INTEGER NOT NULL,
                schema_hash TEXT NOT NULL,
                generation INTEGER NOT NULL,
                cursor TEXT,
                page_limit INTEGER NOT NULL
            )
            """.trimIndent()
        )
        createRebuildPageReceiptTable(db)
        createSealedPushBatchTable(db)
    }

    private fun createRebuildPageReceiptTable(db: SQLiteDatabase) {
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _synchro_rebuild_page_receipts (
                scope_id TEXT NOT NULL,
                rebuild_id TEXT NOT NULL,
                request_cursor_is_null INTEGER NOT NULL CHECK (request_cursor_is_null IN (0, 1)),
                request_cursor TEXT NOT NULL,
                request_json TEXT NOT NULL,
                response_json TEXT NOT NULL,
                is_final INTEGER NOT NULL CHECK (is_final IN (0, 1)),
                final_scope_cursor TEXT,
                final_checksum TEXT,
                PRIMARY KEY (scope_id, rebuild_id, request_cursor_is_null, request_cursor),
                CHECK (
                    (is_final = 0 AND final_scope_cursor IS NULL AND final_checksum IS NULL) OR
                    (is_final = 1 AND final_scope_cursor IS NOT NULL AND final_checksum IS NOT NULL)
                )
            )
            """.trimIndent()
        )
    }

    private fun createMutationLedgerTables(db: SQLiteDatabase) {
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _synchro_pending_changes (
                local_order INTEGER PRIMARY KEY AUTOINCREMENT,
                mutation_id TEXT NOT NULL UNIQUE,
                table_id TEXT NOT NULL,
                table_name TEXT NOT NULL,
                record_id TEXT NOT NULL,
                pk_field_id TEXT NOT NULL,
                pk_logical_type TEXT NOT NULL,
                operation TEXT NOT NULL CHECK (operation IN ('insert', 'update', 'delete')),
                authored_schema_version INTEGER NOT NULL,
                authored_schema_hash TEXT NOT NULL,
                base_version TEXT,
                client_version TEXT NOT NULL,
                lifecycle_state TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                depends_on_mutation_id TEXT,
                normalized_mutation_id TEXT,
                sealed_batch_id TEXT,
                sealed_ordinal INTEGER,
                accepted_outcome_json TEXT,
                rejected_outcome_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                CHECK ((sealed_batch_id IS NULL AND sealed_ordinal IS NULL) OR
                       (sealed_batch_id IS NOT NULL AND sealed_ordinal IS NOT NULL))
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _synchro_mutation_values (
                mutation_id TEXT NOT NULL,
                field_id TEXT NOT NULL,
                logical_type TEXT NOT NULL,
                value_kind TEXT NOT NULL CHECK (value_kind IN ('null', 'boolean', 'integer', 'real', 'text', 'blob')),
                value_integer INTEGER,
                value_real REAL,
                value_text TEXT,
                value_blob BLOB,
                PRIMARY KEY (mutation_id, field_id),
                CHECK (
                    (value_kind = 'null' AND value_integer IS NULL AND value_real IS NULL AND value_text IS NULL AND value_blob IS NULL) OR
                    (value_kind IN ('boolean', 'integer') AND value_integer IS NOT NULL AND value_real IS NULL AND value_text IS NULL AND value_blob IS NULL) OR
                    (value_kind = 'real' AND value_integer IS NULL AND value_real IS NOT NULL AND value_text IS NULL AND value_blob IS NULL) OR
                    (value_kind = 'text' AND value_integer IS NULL AND value_real IS NULL AND value_text IS NOT NULL AND value_blob IS NULL) OR
                    (value_kind = 'blob' AND value_integer IS NULL AND value_real IS NULL AND value_text IS NULL AND value_blob IS NOT NULL)
                )
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _synchro_push_batch_members (
                batch_id TEXT NOT NULL,
                mutation_id TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                PRIMARY KEY (batch_id, mutation_id),
                UNIQUE (batch_id, ordinal)
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _synchro_schema_archives (
                schema_version INTEGER NOT NULL,
                schema_hash TEXT NOT NULL,
                manifest_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (schema_version, schema_hash)
            )
            """.trimIndent()
        )
        db.execSQL(
            "CREATE INDEX IF NOT EXISTS idx_synchro_pending_state_order ON _synchro_pending_changes (lifecycle_state, local_order)"
        )
        db.execSQL(
            "CREATE INDEX IF NOT EXISTS idx_synchro_pending_row_order ON _synchro_pending_changes (table_name, record_id, local_order)"
        )
        db.execSQL(
            "CREATE INDEX IF NOT EXISTS idx_synchro_pending_dependency ON _synchro_pending_changes (depends_on_mutation_id, lifecycle_state)"
        )
        db.execSQL(
            "CREATE INDEX IF NOT EXISTS idx_synchro_pending_sealed ON _synchro_pending_changes (sealed_batch_id, sealed_ordinal)"
        )
        db.execSQL(
            "CREATE INDEX IF NOT EXISTS idx_synchro_batch_members_order ON _synchro_push_batch_members (batch_id, ordinal)"
        )
    }

    private fun migrateMutationLedger(db: SQLiteDatabase) {
        val pendingExists = hasTable(db, "_synchro_pending_changes")
        if (pendingExists && !hasColumn(db, "_synchro_pending_changes", "mutation_id")) {
            dropCaptureTriggers(db)
            db.execSQL("ALTER TABLE _synchro_pending_changes RENAME TO _synchro_pending_changes_v6")
            createMutationLedgerTables(db)
            db.execSQL(
                """
                INSERT INTO _synchro_pending_changes (
                    mutation_id, table_id, table_name, record_id, pk_field_id, pk_logical_type,
                    operation, authored_schema_version, authored_schema_hash, base_version,
                    client_version, lifecycle_state, source_kind, created_at, updated_at
                )
                SELECT
                    $SQLITE_UUID,
                    'legacy_unknown', table_name, record_id, 'legacy_unknown', 'legacy_unknown',
                    CASE operation WHEN 'create' THEN 'insert' WHEN 'update' THEN 'update' ELSE 'delete' END,
                    0, '', base_updated_at, client_updated_at,
                    'legacy_blocked', 'legacy_unsealed', client_updated_at, client_updated_at
                FROM _synchro_pending_changes_v6
                """.trimIndent()
            )
            db.execSQL("DROP TABLE _synchro_pending_changes_v6")
            restoreCaptureTriggers(db)
        } else {
            createMutationLedgerTables(db)
        }
        migrateRejectedMutations(db)
    }

    private fun dropCaptureTriggers(db: SQLiteDatabase) {
        val names = mutableListOf<String>()
        db.rawQuery(
            "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name LIKE '_synchro_cdc_%'",
            null,
        ).use { cursor -> while (cursor.moveToNext()) names += cursor.getString(0) }
        names.forEach { name -> db.execSQL("DROP TRIGGER IF EXISTS ${SQLiteHelpers.quoteIdentifier(name)}") }
    }

    private fun restoreCaptureTriggers(db: SQLiteDatabase) {
        val schemaVersion = SynchroMeta.getInt64(db, MetaKey.SCHEMA_VERSION)
        val schemaHash = SynchroMeta.get(db, MetaKey.SCHEMA_HASH).orEmpty()
        if (schemaVersion <= 0 || schemaHash.isEmpty()) return
        val encoded = SynchroMeta.get(db, MetaKey.LOCAL_SCHEMA)
            ?: throw SynchroError.InvalidResponse("verified local schema metadata is missing")
        val tables = try {
            Json { ignoreUnknownKeys = true }.decodeFromString<List<LocalSchemaTable>>(encoded)
        } catch (_: IllegalArgumentException) {
            throw SynchroError.InvalidResponse("verified local schema metadata is invalid")
        }
        db.execSQL(
            """
            INSERT INTO _synchro_schema_archives (schema_version, schema_hash, manifest_json, created_at)
            VALUES (?, ?, ?, substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z')
            ON CONFLICT (schema_version, schema_hash) DO NOTHING
            """.trimIndent(),
            arrayOf(schemaVersion, schemaHash, encoded),
        )
        tables.forEach { table ->
            SQLiteSchema.generateCDCTriggers(table).forEach { trigger -> db.execSQL(trigger) }
        }
    }

    private fun createSealedPushBatchTable(db: SQLiteDatabase) {
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _synchro_push_batches (
                batch_id TEXT PRIMARY KEY,
                request_json TEXT NOT NULL,
                pending_json TEXT NOT NULL,
                schema_json TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at TEXT NOT NULL,
                completed_at TEXT
            )
            """.trimIndent()
        )
    }

    private fun createBackoffTable(db: SQLiteDatabase) {
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _synchro_backoff (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                resume_state TEXT NOT NULL CHECK (resume_state IN ('connecting', 'pushing', 'pulling', 'rebuilding')),
                work_identity TEXT NOT NULL,
                retry_classification TEXT NOT NULL CHECK (retry_classification IN ('network', 'http_429', 'http_503')),
                attempt_count INTEGER NOT NULL CHECK (attempt_count > 0),
                next_retry_at_ms INTEGER NOT NULL
            )
            """.trimIndent()
        )
    }

    private fun createClientStateTable(db: SQLiteDatabase) {
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _synchro_client_state (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                lifecycle_state TEXT NOT NULL,
                error_operation TEXT,
                error_code TEXT,
                error_retryable INTEGER,
                error_message TEXT,
                error_recovery_action TEXT,
                error_diagnostics TEXT,
                error_acknowledged INTEGER NOT NULL DEFAULT 0 CHECK (error_acknowledged IN (0, 1)),
                updated_at TEXT NOT NULL,
                CHECK (
                    (error_operation IS NULL AND error_code IS NULL AND error_retryable IS NULL AND
                        error_message IS NULL AND error_recovery_action IS NULL AND error_diagnostics IS NULL) OR
                    (error_operation IS NOT NULL AND error_code IS NOT NULL AND error_retryable IN (0, 1) AND
                        error_message IS NOT NULL AND error_recovery_action IN ('retry', 'schema_reset', 'none') AND
                        error_diagnostics IS NOT NULL)
                )
            )
            """.trimIndent(),
        )
        db.execSQL(
            """
            INSERT OR IGNORE INTO _synchro_client_state
                (singleton, lifecycle_state, updated_at)
            VALUES (1, 'uninitialized', substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z')
            """.trimIndent(),
        )
    }

    private fun createMigrationJournalTable(db: SQLiteDatabase) {
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _synchro_migration_journal (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                journal_version INTEGER NOT NULL,
                source_schema_version INTEGER NOT NULL,
                source_schema_hash TEXT NOT NULL,
                target_schema_version INTEGER NOT NULL,
                target_schema_hash TEXT NOT NULL,
                action TEXT NOT NULL CHECK (action IN ('replace', 'rebuild_local')),
                affected_scopes_json TEXT NOT NULL,
                scope_cursor_updates_json TEXT NOT NULL,
                target_manifest_json TEXT NOT NULL,
                target_tables_json TEXT NOT NULL,
                migration_plan_version INTEGER NOT NULL,
                migration_plan_json TEXT NOT NULL,
                migration_plan_hash TEXT NOT NULL,
                reset_materialization INTEGER NOT NULL CHECK (reset_materialization IN (0, 1)),
                phase TEXT NOT NULL CHECK (phase IN ('prepared', 'ddl_applied', 'awaiting_rebuild')),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """.trimIndent(),
        )
    }

    companion object {
        /** Kotlin-only internal construction keeps the raw helper out of the AAR API. */
        @JvmSynthetic
        internal fun open(context: Context, dbPath: String): SynchroDatabase = SynchroDatabase(context, dbPath)

        internal const val DATABASE_VERSION: Int = 13

        /** SQLite creates the UUID in the same application-write transaction as capture. */
        const val SQLITE_UUID: String =
            "lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || " +
                "substr(lower(hex(randomblob(2))), 2) || '-' || " +
                "substr('89ab', (random() & 3) + 1, 1) || substr(lower(hex(randomblob(2))), 2) || '-' || " +
                "lower(hex(randomblob(6)))"
    }

    // MARK: - Queries

    internal fun query(sql: String, params: Array<out Any?>? = null): List<Row> {
        return queryWithTypedBindings(readableDatabase, sql, params)
    }

    internal fun queryOne(sql: String, params: Array<out Any?>? = null): Row? {
        return queryOneWithTypedBindings(readableDatabase, sql, params)
    }

    fun applicationQuery(sql: String, params: Array<out Any?>? = null): List<Row> {
        ApplicationSql.authorizeRead(sql)
        return queryWithTypedBindings(readableDatabase, sql, params)
    }

    fun applicationQueryOne(sql: String, params: Array<out Any?>? = null): Row? {
        ApplicationSql.authorizeRead(sql)
        return queryOneWithTypedBindings(readableDatabase, sql, params)
    }

    fun applicationExecute(sql: String, params: Array<out Any?>? = null): ExecResult {
        var changedTables = emptySet<String>()
        val result = writeTransaction { db ->
            val statement = ApplicationSql.authorizeWrite(sql)
            ApplicationWriteGuard.requireWritableTarget(db, requireNotNull(statement.writeTarget))
            val compiled = db.compileStatement(sql)
            val changes = try {
                bindTypedValues(compiled, params?.toList() ?: emptyList())
                compiled.executeUpdateDelete()
            } finally {
                compiled.close()
            }
            changedTables = setOf(requireNotNull(statement.writeTarget))
            ExecResult(rowsAffected = changes)
        }
        notifyTablesChanged(changedTables)
        return result
    }

    fun <T> applicationReadTransaction(block: (ApplicationReadTransaction) -> T): T =
        readTransaction { db -> block(ApplicationReadTransaction(db)) }

    fun <T> applicationTransaction(block: (ApplicationTransaction) -> T): T {
        var changedTables = emptySet<String>()
        val result = writeTransaction { db ->
            val transaction = ApplicationTransaction(db)
            val previousDepth = applicationTransactionDepth.get() ?: 0
            applicationTransactionDepth.set(previousDepth + 1)
            try {
                val value = block(transaction)
                changedTables = transaction.changedTables()
                value
            } finally {
                if (previousDepth == 0) {
                    applicationTransactionDepth.remove()
                } else {
                    applicationTransactionDepth.set(previousDepth)
                }
            }
        }
        notifyTablesChanged(changedTables)
        return result
    }

    internal fun requireOutsideApplicationTransaction(operation: String) {
        check((applicationTransactionDepth.get() ?: 0) == 0) {
            "$operation cannot run inside an application transaction"
        }
    }

    // MARK: - Transactions

    internal fun <T> readTransaction(block: (SQLiteDatabase) -> T): T {
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

    /** Serializes client-owned DDL and every application write around trigger validation. */
    internal fun <T> writeTransaction(block: (SQLiteDatabase) -> T): T = writeOwnership.withLock {
        val db = writableDatabase
        db.beginTransaction()
        ProvenanceMaintenanceWork.begin(db, provenanceMaintenanceCursor)
        var committed = false
        try {
            val result = block(db)
            db.setTransactionSuccessful()
            committed = true
            return result
        } finally {
            try {
                db.endTransaction()
            } catch (error: Throwable) {
                committed = false
                throw error
            } finally {
                ProvenanceMaintenanceWork.end(db, committed)
            }
        }
    }

    internal fun <T> writeSyncLockedTransaction(block: (SQLiteDatabase) -> T): T {
        return writeTransaction { db ->
            SynchroMeta.setSyncLock(db, true)
            try {
                val result = block(db)
                SynchroMeta.setSyncLock(db, false)
                result
            } catch (error: Throwable) {
                try {
                    SynchroMeta.setSyncLock(db, false)
                } catch (unlockError: Throwable) {
                    error.addSuppressed(unlockError)
                }
                throw error
            }
        }
    }

    // MARK: - Batch

    fun applicationExecuteBatch(statements: List<SQLStatement>): Int = applicationTransaction { transaction ->
        statements.sumOf { statement -> transaction.execute(statement.sql, statement.params).rowsAffected }
    }

    // MARK: - Schema (local-only tables)

    fun createLocalOnlyTable(name: String, columns: List<ColumnDef>, options: TableOptions? = null) {
        ApplicationSql.requireApplicationObject(name)
        validateLocalOnlyColumns(columns)
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

        writeTransaction { db ->
            requireLocalOnlyTable(db, name)
            db.execSQL(sql)
        }
    }

    fun alterLocalOnlyTable(name: String, addColumns: List<ColumnDef>) {
        ApplicationSql.requireApplicationObject(name)
        validateLocalOnlyColumns(addColumns)
        val quotedName = SQLiteHelpers.quoteIdentifier(name)
        writeTransaction { db ->
            requireLocalOnlyTable(db, name, mustExist = true)
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
    }

    fun createLocalOnlyIndex(table: String, columns: List<String>, unique: Boolean = false) {
        ApplicationSql.requireApplicationObject(table)
        columns.forEach(ApplicationSql::requireApplicationObject)
        require(columns.isNotEmpty()) { "An index requires at least one column" }
        val quotedTable = SQLiteHelpers.quoteIdentifier(table)
        val quotedCols = columns.joinToString(", ") { SQLiteHelpers.quoteIdentifier(it) }
        val indexName = SQLiteHelpers.quoteIdentifier("idx_${table}_${columns.joinToString("_")}")
        val uniqueStr = if (unique) "UNIQUE " else ""
        val sql = "CREATE ${uniqueStr}INDEX IF NOT EXISTS $indexName ON $quotedTable ($quotedCols)"
        writeTransaction { db ->
            requireLocalOnlyTable(db, table, mustExist = true)
            db.execSQL(sql)
        }
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

    private fun requireLocalOnlyTable(
        db: SQLiteDatabase,
        tableName: String,
        mustExist: Boolean = false,
    ) {
        ApplicationSql.requireApplicationObject(tableName)
        requireSimpleApplicationIdentifier(tableName, "table name")
        val synced = SynchroMeta.get(db, MetaKey.LOCAL_SCHEMA)?.let { encoded ->
            try {
                Json { ignoreUnknownKeys = false }.decodeFromString<List<LocalSchemaTable>>(encoded)
            } catch (_: Exception) {
                throw SynchroError.InvalidResponse("stored local schema metadata is invalid")
            }
        }.orEmpty()
        if (synced.any { it.tableName.equals(tableName, ignoreCase = true) }) {
            throw IllegalArgumentException("Synced tables are managed by Synchro")
        }
        if (mustExist) {
            val exists = db.rawQuery(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
                arrayOf(tableName),
            ).use { it.moveToFirst() }
            if (!exists) throw IllegalArgumentException("Local-only table does not exist")
        }
    }

    private fun validateLocalOnlyColumns(columns: List<ColumnDef>) {
        require(columns.isNotEmpty()) { "A table requires at least one column" }
        val names = mutableSetOf<String>()
        columns.forEach { column ->
            ApplicationSql.requireApplicationObject(column.name)
            requireSimpleApplicationIdentifier(column.name, "column name")
            require(names.add(column.name.lowercase())) { "Duplicate column name" }
            validateSQLiteType(column.type)
            column.defaultValue?.let(::validateSQLiteDefault)
        }
    }

    private fun requireSimpleApplicationIdentifier(value: String, subject: String) {
        require(value.isNotEmpty() && value.all { it == '_' || it.isLetterOrDigit() }) {
            "Invalid local-only $subject"
        }
    }

    /** Local-only DDL receives a type token, not an arbitrary SQL fragment. */
    private fun validateSQLiteType(value: String) {
        val type = value.trim()
        require(type.isNotEmpty() && type.all { it == '_' || it.isLetterOrDigit() }) {
            "Invalid local-only column type"
        }
    }

    /** Local-only defaults are scalar constants only. */
    private fun validateSQLiteDefault(value: String) {
        val trimmed = value.trim()
        require(trimmed.isNotEmpty()) { "Invalid local-only default" }
        if (trimmed.equals("NULL", ignoreCase = true)) return
        if (trimmed.length >= 2 && trimmed.first() == '\'' && trimmed.last() == '\'') {
            var index = 1
            while (index < trimmed.length - 1) {
                if (trimmed[index] == '\'') {
                    require(index + 1 < trimmed.length - 1 && trimmed[index + 1] == '\'') {
                        "Invalid local-only default"
                    }
                    index += 2
                } else {
                    index += 1
                }
            }
            return
        }
        require(trimmed.all { it.isDigit() || it in "+-.eE" }) { "Invalid local-only default" }
        require(trimmed.toDoubleOrNull()?.isFinite() == true) { "Invalid local-only default" }
    }

    internal fun notifyTablesChanged(tables: Set<String>) {
        if (tables.isEmpty()) return
        changeNotifier.notifyTables(expandTriggerChanges(writableDatabase, tables))
    }

    private fun expandTriggerChanges(db: SQLiteDatabase, tables: Set<String>): Set<String> {
        if (tables.isEmpty()) return tables
        val placeholders = SQLiteHelpers.placeholders(tables.size)
        val capturesMutation = db.rawQuery(
            "SELECT 1 FROM sqlite_master WHERE type = 'trigger' AND tbl_name IN ($placeholders) AND name LIKE '_synchro_cdc_%' LIMIT 1",
            tables.toTypedArray(),
        ).use { it.moveToFirst() }
        return if (capturesMutation) tables + "_synchro_pending_changes" else tables
    }
}

/** A read-only application transaction. It never exposes SQLiteDatabase. */
class ApplicationReadTransaction internal constructor(
    private val database: SQLiteDatabase,
) {
    fun query(sql: String, params: Array<out Any?>? = null): List<Row> {
        ApplicationSql.authorizeRead(sql)
        return queryWithTypedBindings(database, sql, params)
    }

    fun queryOne(sql: String, params: Array<out Any?>? = null): Row? {
        ApplicationSql.authorizeRead(sql)
        return queryOneWithTypedBindings(database, sql, params)
    }
}

/**
 * An application transaction contains only guarded local SQL. The internal
 * SQLite handle remains private, so an application cannot change metadata or
 * remove capture triggers.
 */
class ApplicationTransaction internal constructor(
    private val database: SQLiteDatabase,
) {
    private val changed = linkedSetOf<String>()
    private var triggerSetValidated = false

    fun query(sql: String, params: Array<out Any?>? = null): List<Row> {
        ApplicationSql.authorizeRead(sql)
        return queryWithTypedBindings(database, sql, params)
    }

    fun queryOne(sql: String, params: Array<out Any?>? = null): Row? {
        ApplicationSql.authorizeRead(sql)
        return queryOneWithTypedBindings(database, sql, params)
    }

    fun execute(sql: String, params: Array<out Any?>? = null): ExecResult {
        val statement = ApplicationSql.authorizeWrite(sql)
        val target = requireNotNull(statement.writeTarget)
        ApplicationWriteGuard.requireWritableTarget(
            database,
            target,
            validateTriggerSet = !triggerSetValidated,
        )
        triggerSetValidated = true
        val compiled = database.compileStatement(sql)
        val changes = try {
            bindTypedValues(compiled, params?.toList() ?: emptyList())
            compiled.executeUpdateDelete()
        } finally {
            compiled.close()
        }
        changed += target
        return ExecResult(rowsAffected = changes)
    }

    fun executeBatch(statements: List<SQLStatement>): Int =
        statements.sumOf { statement -> execute(statement.sql, statement.params).rowsAffected }

    internal fun changedTables(): Set<String> = changed.toSet()
}

/**
 * The public write surface accepts real application tables only. A synced
 * table must retain the complete generated capture-trigger set before SQLite
 * receives an application DML statement.
 */
internal object ApplicationWriteGuard {
    fun requireWritableTarget(
        db: SQLiteDatabase,
        target: String,
        validateTriggerSet: Boolean = true,
    ) {
        ApplicationSql.requireApplicationObject(target)
        val exists = db.rawQuery(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ? COLLATE NOCASE",
            arrayOf(target),
        ).use { cursor ->
            cursor.moveToFirst()
        }
        if (!exists) throw IllegalArgumentException("Application writes require an application table")
        if (validateTriggerSet) {
            requireExactAllowedTriggerSet(db, loadSyncedTables(db))
        }
    }

    internal fun loadSyncedTables(db: SQLiteDatabase): List<LocalSchemaTable> {
        val encoded = SynchroMeta.get(db, MetaKey.LOCAL_SCHEMA) ?: return emptyList()
        return try {
            Json { ignoreUnknownKeys = false }.decodeFromString(encoded)
        } catch (_: Exception) {
            throw SynchroError.InvalidResponse("stored local schema metadata is invalid")
        }
    }

    /**
     * SQLite has no public authorizer on Android. Therefore every trigger in the
     * database is either the exact generated capture trigger or an application
     * write is rejected before SQLite executes it.
     */
    internal fun requireExactAllowedTriggerSet(db: SQLiteDatabase, syncedTables: List<LocalSchemaTable>) {
        val expected = linkedMapOf<String, ExpectedTrigger>()
        syncedTables.forEach { table ->
            SQLiteSchema.expectedCDCTriggerSQL(table).forEach { (name, sql) ->
                if (expected.put(name, ExpectedTrigger(table.tableName, sql)) != null) {
                    throw SynchroError.InvalidResponse("synced schema has duplicate capture triggers")
                }
            }
        }
        val actual = linkedMapOf<String, ActualTrigger>()
        db.rawQuery(
            "SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger' ORDER BY name",
            null,
        ).use { cursor ->
            while (cursor.moveToNext()) {
                val name = cursor.getString(0)
                if (actual.put(name, ActualTrigger(
                        tableName = cursor.getString(1),
                        sql = if (cursor.isNull(2)) null else cursor.getString(2),
                    )) != null
                ) {
                    throw SynchroError.InvalidResponse("SQLite has duplicate trigger metadata")
                }
            }
        }
        if (actual.keys != expected.keys) {
            throw IllegalStateException("Application writes require the exact Synchro trigger set")
        }
        expected.forEach { (name, required) ->
            val installed = actual.getValue(name)
            if (!installed.tableName.equals(required.tableName, ignoreCase = true) ||
                installed.sql == null ||
                SQLiteSchema.canonicalDDL(installed.sql) != SQLiteSchema.canonicalDDL(required.sql)
            ) {
                throw IllegalStateException("Application writes require exact Synchro trigger definitions")
            }
        }
    }

    private data class ExpectedTrigger(val tableName: String, val sql: String)

    private data class ActualTrigger(val tableName: String, val sql: String?)
}

internal fun queryWithTypedBindings(
    db: SQLiteDatabase,
    sql: String,
    params: Array<out Any?>? = null
): List<Row> {
    db.rawQueryWithFactory({ _, driver, editTable, query ->
        bindTypedValues(query, params?.toList() ?: emptyList())
        SQLiteCursor(driver, editTable, query)
    }, sql, emptyArray(), "").use { cursor ->
        val rows = mutableListOf<Row>()
        while (cursor.moveToNext()) {
            rows.add(cursorToRow(cursor))
        }
        return rows
    }
}

internal fun queryOneWithTypedBindings(
    db: SQLiteDatabase,
    sql: String,
    params: Array<out Any?>? = null
): Row? {
    db.rawQueryWithFactory({ _, driver, editTable, query ->
        bindTypedValues(query, params?.toList() ?: emptyList())
        SQLiteCursor(driver, editTable, query)
    }, sql, emptyArray(), "").use { cursor ->
        return if (cursor.moveToFirst()) cursorToRow(cursor) else null
    }
}

internal fun sqliteBindValue(value: Any?, index: Int): Any? {
    return when (value) {
        null -> null
        is String -> value
        is ByteArray -> value
        is Boolean -> if (value) 1L else 0L
        is Byte -> value.toLong()
        is Short -> value.toLong()
        is Int -> value.toLong()
        is Long -> value
        is Float -> {
            require(value.isFinite()) { "Invalid SQL bind number at index $index" }
            value.toDouble()
        }
        is Double -> {
            require(value.isFinite()) { "Invalid SQL bind number at index $index" }
            value
        }
        else -> throw IllegalArgumentException(
            "Unsupported SQL bind value at index $index: ${value::class.java.name}"
        )
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
