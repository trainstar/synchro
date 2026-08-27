package com.trainstar.synchro

import android.content.Context
import android.database.sqlite.SQLiteDatabase
import androidx.test.core.app.ApplicationProvider
import java.util.UUID
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28])
class DatabaseMigrationTests {
    private val databases = TestDatabaseTracker()

    @After
    fun tearDown() = databases.closeAll()

    @Test
    fun versionSixUpgradeRebuildsQueueAndPreservesSealedRequestJSON() = runTest {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val path = context.getDatabasePath("synchro_mutation_ledger_${UUID.randomUUID()}.sqlite").absolutePath
        val batchID = "00000000-0000-4000-8000-000000000001"
        val mutationID = "00000000-0000-4000-8000-000000000002"
        val requestJSON = Json { encodeDefaults = true }.encodeToString(
            PushRequest(
                clientID = "device-1",
                clientGeneration = 1,
                batchID = batchID,
                schema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
                mutations = listOf(
                    Mutation(
                        mutationID = mutationID,
                        table = "orders",
                        op = Operation.INSERT,
                        pk = JsonObject(mapOf("id" to JsonPrimitive("sealed"))),
                        authoredSchema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
                        clientVersion = "2026-01-01T00:00:00.000000Z",
                        columns = JsonObject(mapOf("title" to JsonPrimitive("sealed value"))),
                    ),
                ),
            ),
        )
        val legacy = SQLiteDatabase.openOrCreateDatabase(path, null)
        legacy.execSQL(
            """
            CREATE TABLE _synchro_pending_changes (
                record_id TEXT NOT NULL, table_name TEXT NOT NULL, operation TEXT NOT NULL,
                base_updated_at TEXT, client_updated_at TEXT NOT NULL, local_revision INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (table_name, record_id)
            )
            """.trimIndent(),
        )
        legacy.execSQL("CREATE TABLE _synchro_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        legacy.execSQL("INSERT INTO _synchro_meta VALUES ('sync_lock', '0')")
        createLegacyScopeTables(legacy)
        legacy.execSQL(
            """
            CREATE TABLE _synchro_push_batches (
                batch_id TEXT PRIMARY KEY, request_json TEXT NOT NULL, pending_json TEXT NOT NULL,
                schema_json TEXT NOT NULL, state TEXT NOT NULL, created_at TEXT NOT NULL, completed_at TEXT
            )
            """.trimIndent(),
        )
        legacy.execSQL(
            "INSERT INTO _synchro_pending_changes VALUES ('unsealed', 'orders', 'create', NULL, '2026-01-01T00:00:00.000000Z', 3)",
        )
        legacy.execSQL(
            "INSERT INTO _synchro_push_batches VALUES (?, ?, '[]', '[]', 'pending', '2026-01-01T00:00:00.000000Z', NULL)",
            arrayOf(batchID, requestJSON),
        )
        legacy.execSQL("PRAGMA user_version = 6")
        legacy.close()

        val database = databases.open(context, path)
        assertEquals(
            SynchroDatabase.DATABASE_VERSION.toLong(),
            database.queryOne("PRAGMA user_version")?.get("user_version"),
        )
        val migrated = database.queryOne(
            "SELECT lifecycle_state, source_kind, table_id, pk_field_id FROM _synchro_pending_changes WHERE record_id = 'unsealed'",
        )!!
        assertEquals("legacy_blocked", migrated["lifecycle_state"])
        assertEquals("legacy_unsealed", migrated["source_kind"])
        assertEquals("legacy_unknown", migrated["table_id"])
        assertEquals("legacy_unknown", migrated["pk_field_id"])
        assertTrue(database.query("SELECT * FROM _synchro_mutation_values WHERE mutation_id = (SELECT mutation_id FROM _synchro_pending_changes WHERE record_id = 'unsealed')").isEmpty())
        assertEquals(requestJSON, database.queryOne("SELECT request_json FROM _synchro_push_batches WHERE batch_id = ?", arrayOf(batchID))?.get("request_json"))
        assertEquals(
            1,
            database.query("SELECT name FROM sqlite_master WHERE type = 'table' AND name = '_synchro_backoff'").size,
        )
        assertEquals(
            1,
            database.query("SELECT name FROM sqlite_master WHERE type = 'table' AND name = '_synchro_client_state'").size,
        )
        assertEquals(
            1,
            database.query("SELECT name FROM sqlite_master WHERE type = 'table' AND name = '_synchro_migration_journal'").size,
        )

        val server = MockWebServer()
        server.enqueue(MockResponse().setResponseCode(503).setHeader("Retry-After", "1").setBody(RETRYABLE_503_ERROR_JSON))
        server.start()
        try {
            val processor = PushProcessor(database, ChangeTracker(database))
            val http = HttpClient(
                SynchroConfig(
                    dbPath = path,
                    serverURL = server.url("/").toString().trimEnd('/'),
                    authProvider = { "token" },
                    clientID = "device-1",
                    appVersion = "1.0.0",
                ),
            )
            assertTrue(
                runCatching {
                    processor.processPush(http, "device-1", 1, 1, PROTOCOL_TEST_SCHEMA_HASH, listOf(legacyLocalTable()))
                }.exceptionOrNull() is RetryableError,
            )
            assertEquals(requestJSON, server.takeRequest().body.readUtf8())
            assertEquals(1, database.query("SELECT * FROM _synchro_push_batch_members WHERE batch_id = ?", arrayOf(batchID)).size)
            assertEquals("legacy_sealed", database.queryOne("SELECT source_kind FROM _synchro_pending_changes WHERE mutation_id = ?", arrayOf(mutationID))?.get("source_kind"))
        } finally {
            server.shutdown()
        }
    }

    @Test
    fun versionSevenUpgradePreservesRebuildAttemptAndAddsRequiredTables() {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val path = context.getDatabasePath("synchro_rebuild_receipts_${UUID.randomUUID()}.sqlite").absolutePath
        val rebuildID = "00000000-0000-4000-8000-000000000003"
        val legacy = SQLiteDatabase.openOrCreateDatabase(path, null)
        createLegacyScopeTables(legacy)
        legacy.execSQL("CREATE TABLE retained_application_state (value TEXT NOT NULL)")
        legacy.execSQL("INSERT INTO retained_application_state VALUES ('preserved')")
        legacy.execSQL(
            """
            CREATE TABLE _synchro_rebuild_attempts (
                scope_id TEXT PRIMARY KEY,
                rebuild_id TEXT NOT NULL,
                client_generation INTEGER NOT NULL,
                schema_version INTEGER NOT NULL,
                schema_hash TEXT NOT NULL,
                generation INTEGER NOT NULL,
                cursor TEXT,
                page_limit INTEGER NOT NULL
            )
            """.trimIndent(),
        )
        legacy.execSQL(
            "INSERT INTO _synchro_rebuild_attempts VALUES ('orders:user1', ?, 1, 1, ?, 4, 'page-2', 100)",
            arrayOf(rebuildID, PROTOCOL_TEST_SCHEMA_HASH),
        )
        legacy.execSQL("PRAGMA user_version = 7")
        legacy.close()

        val database = databases.open(context, path)

        assertEquals(
            SynchroDatabase.DATABASE_VERSION.toLong(),
            database.queryOne("PRAGMA user_version")?.get("user_version"),
        )
        assertEquals(
            "preserved",
            database.queryOne("SELECT value FROM retained_application_state")?.get("value"),
        )
        assertEquals(
            "page-2",
            database.queryOne(
                "SELECT cursor FROM _synchro_rebuild_attempts WHERE rebuild_id = ?",
                arrayOf(rebuildID),
            )?.get("cursor"),
        )
        assertEquals(
            1,
            database.query(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = '_synchro_rebuild_page_receipts'",
            ).size,
        )
        assertEquals(
            listOf(
                "scope_id",
                "rebuild_id",
                "request_cursor_is_null",
                "request_cursor",
                "request_json",
                "response_json",
                "is_final",
                "final_scope_cursor",
                "final_checksum",
            ),
            database.query("PRAGMA table_info(_synchro_rebuild_page_receipts)").map { it["name"] },
        )
        assertEquals(
            listOf(
                "singleton",
                "resume_state",
                "work_identity",
                "retry_classification",
                "attempt_count",
                "next_retry_at_ms",
            ),
            database.query("PRAGMA table_info(_synchro_backoff)").map { it["name"] },
        )
    }

    @Test
    fun versionElevenUpgradePreservesAndCompletesBlockingFailureMetadata() {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val path = context.getDatabasePath("synchro_failure_metadata_${UUID.randomUUID()}.sqlite").absolutePath
        val legacy = SQLiteDatabase.openOrCreateDatabase(path, null)
        createLegacyScopeTables(legacy)
        legacy.execSQL(
            """
            CREATE TABLE _synchro_client_state (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                lifecycle_state TEXT NOT NULL,
                error_operation TEXT,
                error_code TEXT,
                error_retryable INTEGER,
                error_diagnostics TEXT,
                error_acknowledged INTEGER NOT NULL DEFAULT 0 CHECK (error_acknowledged IN (0, 1)),
                updated_at TEXT NOT NULL,
                CHECK (
                    (error_operation IS NULL AND error_code IS NULL AND error_retryable IS NULL AND error_diagnostics IS NULL) OR
                    (error_operation IS NOT NULL AND error_code IS NOT NULL AND error_retryable IN (0, 1))
                )
            )
            """.trimIndent(),
        )
        legacy.execSQL(
            """
            INSERT INTO _synchro_client_state
                (singleton, lifecycle_state, error_operation, error_code, error_retryable,
                 error_diagnostics, error_acknowledged, updated_at)
            VALUES (1, 'error', 'schema_applying', 'invalid_response', 0,
                    '{"reason":"unknown_schema_lineage"}', 0, '2026-01-01T00:00:00.000Z')
            """.trimIndent(),
        )
        legacy.execSQL("PRAGMA user_version = 11")
        legacy.close()

        val database = databases.open(context, path)

        assertEquals(
            SynchroDatabase.DATABASE_VERSION.toLong(),
            database.queryOne("PRAGMA user_version")?.get("user_version"),
        )
        assertEquals(
            listOf(
                "singleton",
                "lifecycle_state",
                "error_operation",
                "error_code",
                "error_retryable",
                "error_diagnostics",
                "error_acknowledged",
                "updated_at",
                "error_message",
                "error_recovery_action",
            ),
            database.query("PRAGMA table_info(_synchro_client_state)").map { it["name"] },
        )
        val state = database.readTransaction { db -> SynchroMeta.getClientState(db) }
        assertEquals(SyncLifecycleState.ERROR, state.lifecycleState)
        assertEquals(SyncOperationKind.SCHEMA, state.failure?.operation)
        assertEquals(SyncFailureCode.INVALID_RESPONSE, state.failure?.code)
        assertEquals(
            "The sync operation failed contract validation.",
            state.failure?.message,
        )
        assertEquals(SyncRecoveryAction.RETRY, state.failure?.recoveryAction)
        assertEquals(mapOf("reason" to "unknown_schema_lineage"), state.failure?.metadata)
    }

    @Test
    fun versionTwelveUpgradeConvertsScopeAffinityAndPreservesState() {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val path = context.getDatabasePath("synchro_scope_affinity_${UUID.randomUUID()}.sqlite").absolutePath
        val legacy = SQLiteDatabase.openOrCreateDatabase(path, null)
        createLegacyScopeTables(legacy)
        legacy.execSQL("INSERT INTO _synchro_scopes VALUES ('scope-1', 'cursor-1', 'checksum-1', 4, 7)")
        legacy.execSQL("INSERT INTO _synchro_scope_rows VALUES ('scope-1', 'orders', 'record-1', 9, 4)")
        legacy.execSQL("PRAGMA user_version = 12")
        legacy.close()

        val database = databases.open(context, path)
        val scopeTypes = database.query("PRAGMA table_info(_synchro_scopes)")
            .associate { it.getValue("name") as String to it.getValue("type") as String }
        val scopeRowTypes = database.query("PRAGMA table_info(_synchro_scope_rows)")
            .associate { it.getValue("name") as String to it.getValue("type") as String }
        assertEquals("TEXT", scopeTypes.getValue("local_checksum"))
        assertEquals("TEXT", scopeRowTypes.getValue("checksum"))
        val scope = database.queryOne(
            "SELECT cursor, checksum, generation, local_checksum FROM _synchro_scopes WHERE scope_id = 'scope-1'",
        )!!
        assertEquals("cursor-1", scope["cursor"])
        assertEquals("checksum-1", scope["checksum"])
        assertEquals(4L, scope["generation"])
        assertEquals("7", scope["local_checksum"])
        val scopeRow = database.queryOne(
            "SELECT checksum, generation FROM _synchro_scope_rows WHERE record_id = 'record-1'",
        )!!
        assertEquals("9", scopeRow["checksum"])
        assertEquals(4L, scopeRow["generation"])
        assertEquals(
            SynchroDatabase.DATABASE_VERSION.toLong(),
            database.queryOne("PRAGMA user_version")?.get("user_version"),
        )
        assertEquals(
            1,
            database.query(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'idx_synchro_scope_rows_record'",
            ).size,
        )
    }

    @Test
    fun versionTwelveUpgradeRollsBackWhenScopeStateIsIncomplete() {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val name = "synchro_scope_affinity_failure_${UUID.randomUUID()}.sqlite"
        val path = context.getDatabasePath(name).absolutePath
        try {
            SQLiteDatabase.openOrCreateDatabase(path, null).use { legacy ->
                legacy.execSQL(
                    "CREATE TABLE _synchro_scopes (scope_id TEXT PRIMARY KEY, cursor TEXT, checksum TEXT, generation INTEGER NOT NULL DEFAULT 0, local_checksum INTEGER NOT NULL DEFAULT 0)",
                )
                legacy.execSQL("INSERT INTO _synchro_scopes VALUES ('scope-1', 'cursor-1', 'checksum-1', 4, 7)")
                legacy.execSQL("PRAGMA user_version = 12")
            }

            val error = runCatching { SynchroDatabase.open(context, path) }.exceptionOrNull()
            assertTrue(error is IllegalStateException)

            SQLiteDatabase.openDatabase(path, null, SQLiteDatabase.OPEN_READONLY).use { legacy ->
                legacy.rawQuery("PRAGMA user_version", null).use { cursor ->
                    assertTrue(cursor.moveToFirst())
                    assertEquals(12, cursor.getInt(0))
                }
                legacy.rawQuery(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE '_synchro_%_v13'",
                    null,
                ).use { cursor -> assertEquals(0, cursor.count) }
                legacy.rawQuery("SELECT local_checksum FROM _synchro_scopes WHERE scope_id = 'scope-1'", null).use { cursor ->
                    assertTrue(cursor.moveToFirst())
                    assertEquals(7L, cursor.getLong(0))
                }
            }
        } finally {
            context.deleteDatabase(name)
        }
    }

    @Test
    fun migrationScopeRowDeletionDoesNotContaminateSampledWorkDelta() {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val path = context.getDatabasePath("synchro_migration_work_${UUID.randomUUID()}.sqlite").absolutePath
        val initial = SynchroDatabase.open(context, path)
        try {
            initial.writeTransaction { db ->
                SynchroMeta.upsertScope(db, "scope", "cursor", "checksum")
                SynchroMeta.upsertScopeRow(db, "scope", "orders", "before-migration", "checksum", 0)
            }
        } finally {
            initial.close()
        }
        SQLiteDatabase.openDatabase(path, null, SQLiteDatabase.OPEN_READWRITE).use { legacy ->
            legacy.execSQL("PRAGMA user_version = 3")
        }

        val migrated = databases.open(context, path)
        val baseline = migrated.inspectProvenanceMaintenanceWork().cursor

        assertEquals(0L, baseline)
        assertTrue(migrated.readTransaction { db -> SynchroMeta.listScopeRows(db, 1).isEmpty() })
        migrated.writeTransaction { db ->
            SynchroMeta.upsertScopeRow(db, "scope", "orders", "sampled", "checksum", 0)
        }
        assertEquals(1L, migrated.inspectProvenanceMaintenanceWork().cursor - baseline)
    }

    private fun createLegacyScopeTables(database: SQLiteDatabase) {
        database.execSQL(
            "CREATE TABLE _synchro_scopes (scope_id TEXT PRIMARY KEY, cursor TEXT, checksum TEXT, generation INTEGER NOT NULL DEFAULT 0, local_checksum INTEGER NOT NULL DEFAULT 0)",
        )
        database.execSQL(
            "CREATE TABLE _synchro_scope_rows (scope_id TEXT NOT NULL, table_name TEXT NOT NULL, record_id TEXT NOT NULL, checksum INTEGER NOT NULL DEFAULT 0, generation INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (scope_id, table_name, record_id))",
        )
        database.execSQL("CREATE INDEX idx_synchro_scope_rows_record ON _synchro_scope_rows (table_name, record_id)")
    }

    private fun legacyLocalTable(): LocalSchemaTable = LocalSchemaTable(
        tableID = "orders",
        relationID = "orders",
        tableName = "orders",
        primaryKeyFieldID = "id",
        updatedAtColumn = "updated_at",
        deletedAtColumn = "deleted_at",
        primaryKey = listOf("id"),
        columns = listOf(
            LocalSchemaColumn("id", "string", false, isPrimaryKey = true),
            LocalSchemaColumn("title", "string", true, isPrimaryKey = false),
        ),
    )
}
