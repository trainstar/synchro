package com.trainstar.synchro

import android.content.Context
import android.app.Activity
import android.database.sqlite.SQLiteDatabase
import androidx.test.core.app.ApplicationProvider
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.Robolectric
import org.robolectric.annotation.Config
import java.io.File
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28])
class SynchroClientTests {

    private fun makeConfig(): SynchroConfig {
        val dbName = "synchro_client_test_${UUID.randomUUID()}.sqlite"
        return SynchroConfig(
            dbPath = dbName,
            serverURL = "http://localhost:8080",
            authProvider = { "test-token" },
            clientID = "test-device",
            appVersion = "1.0.0"
        )
    }

    private fun makeClient(): SynchroClient {
        val config = makeConfig()
        val context = ApplicationProvider.getApplicationContext<Context>()
        return SynchroClient(config, context)
    }

    private fun makeSeedConfig(databasePath: String, seedPath: String): SynchroConfig = SynchroConfig(
        dbPath = databasePath,
        serverURL = "http://localhost:8080",
        authProvider = { "test-token" },
        clientID = "test-device",
        appVersion = "1.0.0",
        seedDatabasePath = seedPath,
    )

    private fun <T> withInternalDatabase(dbPath: String, block: (SynchroDatabase) -> T): T {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val database = SynchroDatabase.open(context, dbPath)
        return try {
            block(database)
        } finally {
            database.close()
        }
    }

    @Test
    fun nativeLifecycleSuppressesRotationBackgroundAndDuplicateForeground() {
        val events = mutableListOf<String>()
        var changingConfigurations = false
        val observer = NativeApplicationLifecycleObserver(
            onForeground = { events += "foreground" },
            onBackground = { events += "background" },
            isChangingConfigurations = { changingConfigurations },
        )
        val oldActivity = Robolectric.buildActivity(Activity::class.java).get()
        val replacement = Robolectric.buildActivity(Activity::class.java).get()

        observer.onActivityStarted(oldActivity)
        changingConfigurations = true
        observer.onActivityStopped(oldActivity)
        observer.onActivityStarted(replacement)
        changingConfigurations = false
        observer.onActivityStopped(replacement)

        assertEquals(listOf("foreground", "background"), events)
    }

    @Test
    fun nativeLifecycleTracksMultipleActivitiesAndForegroundBackgroundCycles() {
        val events = mutableListOf<String>()
        val observer = NativeApplicationLifecycleObserver(
            onForeground = { events += "foreground" },
            onBackground = { events += "background" },
        )
        val first = Robolectric.buildActivity(Activity::class.java).get()
        val second = Robolectric.buildActivity(Activity::class.java).get()

        observer.onActivityStarted(first)
        observer.onActivityStarted(second)
        observer.onActivityStopped(first)
        observer.onActivityStopped(second)
        observer.onActivityStarted(first)
        observer.onActivityStopped(first)

        assertEquals(
            listOf("foreground", "background", "foreground", "background"),
            events,
        )
    }

    @Test
    fun testSeedInstallationValidatesAndPublishesDatabase() {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val seedName = "synchro_seed_${UUID.randomUUID()}.sqlite"
        val destinationName = "synchro_destination_${UUID.randomUUID()}.sqlite"
        createProducerLikeSeed(context, seedName)

        val destination = context.getDatabasePath(destinationName)
        val client = SynchroClient(
            makeSeedConfig(destinationName, context.getDatabasePath(seedName).absolutePath),
            context,
        )
        try {
            assertTrue(destination.exists())
            assertEquals("Seeded Address", client.queryOne("SELECT ship_address FROM orders WHERE id = 'seed-1'")?.get("ship_address"))
            withInternalDatabase(destinationName) { database ->
                assertNull(database.queryOne("SELECT value FROM _synchro_meta WHERE key = 'client_id'"))
                assertEquals(
                    SynchroDatabase.DATABASE_VERSION.toLong(),
                    database.queryOne("PRAGMA user_version")?.get("user_version"),
                )
                assertEquals(1, database.query("SELECT scope_id FROM _synchro_seed_receipts").size)
                assertNull(database.queryOne("SELECT cursor FROM _synchro_scopes")?.get("cursor"))
            }
        } finally {
            client.close()
            context.deleteDatabase(seedName)
            context.deleteDatabase(destinationName)
        }
    }

    @Test
    fun testCorruptSeedDoesNotPublishDatabase() {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val seedName = "synchro_corrupt_seed_${UUID.randomUUID()}.sqlite"
        val destinationName = "synchro_destination_${UUID.randomUUID()}.sqlite"
        val seed = context.getDatabasePath(seedName)
        seed.parentFile?.mkdirs()
        seed.writeBytes("not a SQLite database".toByteArray())

        assertThrows(Exception::class.java) {
            SynchroClient(makeSeedConfig(destinationName, seed.absolutePath), context)
        }

        assertNoPublishedOrTemporaryDatabase(context, destinationName)
        seed.delete()
    }

    @Test
    fun testSeedMissingRequiredSynchroStateDoesNotPublishDatabase() {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val seedName = "synchro_incomplete_seed_${UUID.randomUUID()}.sqlite"
        val destinationName = "synchro_destination_${UUID.randomUUID()}.sqlite"
        createProducerLikeSeed(context, seedName)
        val seed = context.getDatabasePath(seedName)
        mutateSeed(seed) { it.execSQL("DROP TABLE _synchro_seed_receipts") }

        assertThrows(Exception::class.java) {
            SynchroClient(makeSeedConfig(destinationName, seed.absolutePath), context)
        }

        assertNoPublishedOrTemporaryDatabase(context, destinationName)
        context.deleteDatabase(seedName)
    }

    @Test
    fun testSemanticSeedCorruptionDoesNotPublishDatabase() {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val zeroChecksum = Json.encodeToString(
            ChecksumObject.serializer(),
            ChecksumObject("sha256", 1, "hex", "0".repeat(64)),
        )
        val mutants = linkedMapOf<String, (SQLiteDatabase) -> Unit>(
            "incomplete snapshot" to { db ->
                db.execSQL("UPDATE _synchro_meta SET value = '0' WHERE key = 'snapshot_complete'")
            },
            "client binding" to { db ->
                db.execSQL("INSERT INTO _synchro_meta (key, value) VALUES ('client_id', 'bound-client')")
            },
            "pending work" to { db ->
                db.execSQL(
                    """
                    INSERT INTO _synchro_pending_changes
                        (record_id, table_name, operation, base_updated_at, client_updated_at, local_revision)
                    VALUES ('seed-1', 'orders', 'update', 'server-version-1', '2026-01-01T01:00:00.000000Z', 1)
                    """.trimIndent(),
                )
            },
            "schema reference" to { db ->
                db.execSQL("UPDATE _synchro_meta SET value = ? WHERE key = 'schema_hash'", arrayOf("0".repeat(64)))
            },
            "local schema" to { db ->
                db.execSQL("UPDATE _synchro_meta SET value = '[]' WHERE key = 'local_schema'")
            },
            "receipt schema" to { db ->
                db.execSQL("UPDATE _synchro_seed_receipts SET schema_version = schema_version + 1")
            },
            "runtime scope cursor" to { db ->
                db.execSQL("UPDATE _synchro_scopes SET cursor = 'unbound-runtime-cursor'")
            },
            "receipt cardinality" to { db ->
                db.execSQL("UPDATE _synchro_seed_receipts SET cardinality = cardinality + 1")
            },
            "missing scope provenance" to { db ->
                db.execSQL("DELETE FROM _synchro_scope_rows")
            },
            "row checksum" to { db ->
                db.execSQL("UPDATE _synchro_row_versions SET row_checksum = ?", arrayOf(zeroChecksum))
            },
            "materialized row" to { db ->
                db.execSQL("DROP TRIGGER _synchro_cdc_insert_orders")
                db.execSQL("DROP TRIGGER _synchro_cdc_update_orders")
                db.execSQL("DROP TRIGGER _synchro_cdc_delete_orders")
                db.execSQL("DROP TRIGGER _synchro_cdc_pk_guard_orders")
                db.execSQL("UPDATE orders SET ship_address = 'corrupt without digest update'")
            },
            "scope digest" to { db ->
                db.execSQL(
                    "UPDATE _synchro_scopes SET checksum = ?, local_checksum = ?",
                    arrayOf(zeroChecksum, zeroChecksum),
                )
                db.execSQL("UPDATE _synchro_seed_receipts SET checksum = ?", arrayOf(zeroChecksum))
            },
            "missing row version" to { db ->
                db.execSQL("DELETE FROM _synchro_row_versions")
            },
        )

        for ((name, mutation) in mutants) {
            val seedName = "synchro_seed_mutant_${UUID.randomUUID()}.sqlite"
            val destinationName = "synchro_destination_mutant_${UUID.randomUUID()}.sqlite"
            createProducerLikeSeed(context, seedName)
            val seed = context.getDatabasePath(seedName)
            mutateSeed(seed, mutation)

            var installed: SynchroClient? = null
            val failure = runCatching {
                installed = SynchroClient(makeSeedConfig(destinationName, seed.absolutePath), context)
            }.exceptionOrNull()
            installed?.close()

            assertNotNull("semantic seed mutant was accepted: $name", failure)
            assertNoPublishedOrTemporaryDatabase(context, destinationName)
            context.deleteDatabase(seedName)
            context.deleteDatabase(destinationName)
        }
    }

    private fun createProducerLikeSeed(context: Context, databaseName: String) {
        val manifest = protocolOrdersSchemaManifest().copy(schemaHash = PROTOCOL_TEST_SCHEMA_HASH)
        val localTable = manifest.localTables().single()
        val scopeID = "orders:portable"
        val row = buildJsonObject {
            put("field-id", "seed-1")
            put("field-ship-address", "Seeded Address")
            put("field-user-id", "portable")
            put("field-updated-at", "2026-01-01T00:00:00.000000Z")
            put("field-deleted-at", JsonNull)
        }
        val primaryKey = buildJsonObject { put("field-id", "seed-1") }
        val serverVersion = "server-version-1"
        val rowDigest = Integrity.rowDigest(
            PROTOCOL_TEST_SCHEMA_HASH,
            localTable,
            primaryKey,
            row,
            serverVersion,
        )
        val scopeChecksum = Integrity.scopeDigest(
            PROTOCOL_TEST_SCHEMA_HASH,
            scopeID,
            listOf(rowDigest.identity to rowDigest.checksum),
        )
        val checksumJSON = Json.encodeToString(ChecksumObject.serializer(), scopeChecksum)

        val seed = SynchroDatabase.open(context, databaseName)
        SchemaManager(seed).reconcileLocalSchema(
            schemaVersion = manifest.schemaVersion,
            schemaHash = manifest.schemaHash,
            tables = listOf(localTable),
        )
        seed.writeSyncLockedTransaction { db ->
            db.execSQL(
                """
                INSERT INTO orders (id, ship_address, user_id, updated_at, deleted_at)
                VALUES (?, ?, ?, ?, ?)
                """.trimIndent(),
                arrayOf("seed-1", "Seeded Address", "portable", "2026-01-01T00:00:00.000000Z", null),
            )
            SynchroMeta.upsertRowVersion(db, "orders", "seed-1", serverVersion, rowDigest.checksum)
            SynchroMeta.upsertScope(db, scopeID, null, checksumJSON, 0L, checksumJSON)
            SynchroMeta.upsertScopeRow(db, scopeID, "orders", "seed-1", rowDigest.checksum.digest, 0L)
            db.execSQL(
                """
                INSERT INTO _synchro_seed_receipts
                    (scope_id, receipt, schema_version, schema_hash, cardinality, checksum)
                VALUES (?, ?, ?, ?, ?, ?)
                """.trimIndent(),
                arrayOf(scopeID, "sc1.producer-like-receipt.signature", 1L, PROTOCOL_TEST_SCHEMA_HASH, 1L, checksumJSON),
            )
            SynchroMeta.set(db, MetaKey.SCHEMA_MANIFEST, Json.encodeToString(SchemaManifest.serializer(), manifest))
            SynchroMeta.setInt64(db, MetaKey.SCOPE_SET_VERSION, 7L)
            SynchroMeta.set(db, MetaKey.SNAPSHOT_COMPLETE, "1")
        }

        seed.writeTransaction { db ->
            db.execSQL("DROP TABLE _synchro_pending_changes")
            db.execSQL("DROP TABLE _synchro_mutation_values")
            db.execSQL("DROP TABLE _synchro_push_batch_members")
            db.execSQL("DROP TABLE _synchro_schema_archives")
            db.execSQL("DROP TABLE _synchro_rebuild_page_receipts")
            db.execSQL("DROP TABLE _synchro_backoff")
            db.execSQL("DROP TABLE _synchro_rejected_mutations")
            db.execSQL(
                """
                CREATE TABLE _synchro_pending_changes (
                    record_id TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    base_updated_at TEXT,
                    client_updated_at TEXT NOT NULL,
                    local_revision INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (table_name, record_id)
                )
                """.trimIndent(),
            )
            db.execSQL(
                """
                CREATE TABLE _synchro_rejected_mutations (
                    mutation_id TEXT PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    record_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    code TEXT NOT NULL,
                    message TEXT,
                    server_row_json TEXT,
                    server_version TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """.trimIndent(),
            )
            db.execSQL("CREATE TABLE grdb_migrations (identifier TEXT NOT NULL PRIMARY KEY)")
            db.version = 6
        }
        seed.close()
    }

    private fun mutateSeed(file: File, mutation: (SQLiteDatabase) -> Unit) {
        val database = SQLiteDatabase.openDatabase(file.absolutePath, null, SQLiteDatabase.OPEN_READWRITE)
        try {
            mutation(database)
        } finally {
            database.close()
        }
    }

    private fun assertNoPublishedOrTemporaryDatabase(context: Context, databaseName: String) {
        val destination = context.getDatabasePath(databaseName)
        assertFalse(destination.exists())
        listOf("-journal", "-wal", "-shm").forEach { suffix ->
            assertFalse(File(destination.path + suffix).exists())
        }
        val temporaryPrefix = ".${destination.name}.seed-"
        assertTrue(destination.parentFile?.listFiles().orEmpty().none { it.name.startsWith(temporaryPrefix) })
    }

    @Test
    fun testClientInitCreatesDatabase() {
        val client = makeClient()

        withInternalDatabase(client.path) { database ->
            val rows = database.query("SELECT name FROM sqlite_master WHERE type='table'")
            val tableNames = rows.map { it["name"] as String }
            assertTrue(tableNames.contains("_synchro_pending_changes"))
            assertTrue(tableNames.contains("_synchro_meta"))
        }

        client.close()
    }

    @Test
    fun publicClientRestoresBlockingFailureAfterAbruptReopen() {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val server = MockWebServer()
        server.enqueue(MockResponse().setResponseCode(500).setBody("{\"error\":\"fatal bootstrap\"}"))
        server.start()
        val dbName = "synchro_client_failure_${UUID.randomUUID()}.sqlite"
        val config = SynchroConfig(
            dbPath = dbName,
            serverURL = server.url("/").toString().trimEnd('/'),
            authProvider = { "test-token" },
            clientID = "test-device",
            appVersion = "1.0.0",
        )
        val first = SynchroClient(config, context)
        try {
            val failure = runCatching { runBlocking { first.start() } }.exceptionOrNull()
            assertTrue(failure is SynchroError.ServerError)
        } finally {
            first.close()
        }

        val reopened = SynchroClient(config, context)
        try {
            val status = reopened.getSyncStatus()
            assertTrue(status is SyncStatus.Error)
            assertEquals(SyncFailureCode.SERVER_ERROR, (status as SyncStatus.Error).failure.code)
            val failure = runCatching { runBlocking { reopened.start() } }.exceptionOrNull()
            assertTrue(failure is SynchroError.BlockingFailure)
            assertEquals(1, server.requestCount)
        } finally {
            reopened.close()
            server.shutdown()
            context.deleteDatabase(dbName)
        }
    }

    @Test
    fun testCloseWaitsForCallerOwnedSyncBeforeClosingDatabase() {
        val server = MockWebServer()
        server.start()
        val context = ApplicationProvider.getApplicationContext<Context>()
        val config = SynchroConfig(
            dbPath = "synchro_client_close_${UUID.randomUUID()}.sqlite",
            serverURL = server.url("/").toString().trimEnd('/'),
            authProvider = { "test-token" },
            clientID = "test-device",
            appVersion = "1.0.0",
            syncInterval = 999.0,
        )
        val preparedDatabase = SynchroDatabase.open(context, config.dbPath)
        SchemaManager(preparedDatabase).reconcileLocalSchema(
            schemaVersion = 1,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            tables = emptyList(),
        )
        preparedDatabase.close()

        val client = SynchroClient(config, context)
        val scopeID = "caller-owned-scope"
        val emptyChecksum = Json.encodeToString(
            ChecksumObject.serializer(),
            protocolEmptyScopeChecksum(scopeID),
        )
        server.enqueue(
            MockResponse().setBody(
                """
                {
                    "server_time":"2026-01-01T12:00:00.000Z",
                    "protocol_version":3,
                    "client_generation":1,
                    "scope_set_version":1,
                    "schema":{"version":1,"hash":"$PROTOCOL_TEST_SCHEMA_HASH","action":"none"},
                    "scopes":{"add":[{"id":"$scopeID","cursor":"cursor-1"}],"remove":[]},
                    "scope_cursor_updates":{}
                }
                """.trimIndent(),
            ),
        )
        server.enqueue(
            MockResponse()
                .setBody(
                    """
                    {
                        "changes":[],
                        "scope_set_version":1,
                        "scope_cursors":{"$scopeID":"cursor-1"},
                        "scope_updates":{"add":[],"remove":[]},
                        "rebuild":[],
                        "has_more":false,
                        "checksums":{"$scopeID":$emptyChecksum}
                    }
                    """.trimIndent(),
                ),
        )

        val started = CountDownLatch(1)
        var closed = false
        try {
            val startJob = CoroutineScope(Dispatchers.Default).launch {
                runCatching { client.start() }
                started.countDown()
            }
            assertEquals("/sync/connect", server.takeRequest(2, TimeUnit.SECONDS)?.path)
            assertEquals("/sync/pull", server.takeRequest(2, TimeUnit.SECONDS)?.path)
            assertTrue("start must complete before caller-owned sync", started.await(2, TimeUnit.SECONDS))

            server.enqueue(
                MockResponse()
                    .setBody("{}")
                    .setBodyDelay(1, TimeUnit.SECONDS),
            )
            val syncJob = CoroutineScope(Dispatchers.Default).launch {
                runCatching { client.syncNow() }
            }
            assertEquals("/sync/pull", server.takeRequest(2, TimeUnit.SECONDS)?.path)

            client.close()
            closed = true

            assertTrue("close must cancel and drain the engine-owned syncNow job", syncJob.isCompleted)
            assertTrue("close must wait for the managed job", startJob.isCompleted)
        } finally {
            if (!closed) client.close()
            runCatching { server.shutdown() }
        }
    }

    @Test
    fun testClosePermanentlyRejectsFutureSyncOperations() {
        val client = makeClient()
        client.close()

        assertThrows(SynchroError.NotStarted::class.java) {
            runBlocking { client.start() }
        }
        assertThrows(SynchroError.NotStarted::class.java) {
            runBlocking { client.syncNow() }
        }
    }

    @Test
    fun testCoreSQL() {
        val client = makeClient()

        client.createTable("local_notes", listOf(
            ColumnDef(name = "id", type = "TEXT", nullable = false, primaryKey = true),
            ColumnDef(name = "body", type = "TEXT"),
        ))

        val result = client.execute(
            "INSERT INTO local_notes (id, body) VALUES (?, ?)",
            arrayOf("n1", "hello")
        )
        assertEquals(1, result.rowsAffected)

        val rows = client.query("SELECT * FROM local_notes WHERE id = ?", arrayOf("n1"))
        assertEquals(1, rows.size)
        assertEquals("hello", rows[0]["body"])

        val one = client.queryOne("SELECT * FROM local_notes WHERE id = ?", arrayOf("n1"))
        assertNotNull(one)

        val nullResult = client.execute(
            "INSERT INTO local_notes (id, body) VALUES (?, ?)",
            arrayOf("n2", null)
        )
        assertEquals(1, nullResult.rowsAffected)

        val nullRows = client.query(
            "SELECT id FROM local_notes WHERE id = ? AND body IS ?",
            arrayOf("n2", null)
        )
        assertEquals(1, nullRows.size)

        client.close()
    }

    @Test
    fun testBatchExecution() {
        val client = makeClient()

        client.createTable("orders", listOf(
            ColumnDef(name = "id", type = "TEXT", nullable = false, primaryKey = true),
            ColumnDef(name = "value", type = "INTEGER"),
        ))

        val total = client.executeBatch(listOf(
            SQLStatement("INSERT INTO orders (id, value) VALUES (?, ?)", arrayOf("a", 1)),
            SQLStatement("INSERT INTO orders (id, value) VALUES (?, ?)", arrayOf("b", 2)),
            SQLStatement("INSERT INTO orders (id, value) VALUES (?, ?)", arrayOf("c", null)),
        ))
        assertEquals(3, total)

        val rows = client.query("SELECT COUNT(*) as cnt FROM orders")
        assertEquals(3L, rows[0]["cnt"])

        val nullRows = client.query("SELECT id FROM orders WHERE value IS ?", arrayOf(null))
        assertEquals("c", nullRows.first()["id"])

        client.close()
    }

    @Test
    fun testCreateIndex() {
        val client = makeClient()

        client.createTable("orders", listOf(
            ColumnDef(name = "id", type = "TEXT", nullable = false, primaryKey = true),
            ColumnDef(name = "category", type = "TEXT"),
        ))

        client.createIndex("orders", listOf("category"), unique = false)

        withInternalDatabase(client.path) { database ->
            val indexes = database.query("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='orders'")
            val names = indexes.map { it["name"] as String }
            assertTrue(names.contains("idx_orders_category"))
        }

        client.close()
    }

    @Test
    fun testOnChange() {
        val client = makeClient()

        client.createTable("events", listOf(
            ColumnDef(name = "id", type = "TEXT", nullable = false, primaryKey = true),
            ColumnDef(name = "name", type = "TEXT"),
        ))

        val latch = CountDownLatch(1)
        val cancellable = client.onChange(listOf("events")) {
            latch.countDown()
        }

        client.execute("INSERT INTO events (id, name) VALUES (?, ?)", arrayOf("e1", "test"))

        assertTrue(latch.await(2, TimeUnit.SECONDS))
        cancellable.cancel()
        client.close()
    }

    @Test
    fun testWatch() {
        val client = makeClient()

        client.createTable("counters", listOf(
            ColumnDef(name = "id", type = "TEXT", nullable = false, primaryKey = true),
            ColumnDef(name = "value", type = "INTEGER"),
        ))

        client.execute("INSERT INTO counters (id, value) VALUES (?, ?)", arrayOf("c1", 0))

        val latch = CountDownLatch(2) // initial + after update
        val receivedRows = mutableListOf<List<Row>>()

        val cancellable = client.watch(
            "SELECT * FROM counters WHERE id = ?",
            arrayOf("c1"),
            listOf("counters")
        ) { rows ->
            receivedRows.add(rows)
            latch.countDown()
        }

        // Trigger an update
        Thread {
            Thread.sleep(300)
            client.execute("UPDATE counters SET value = ? WHERE id = ?", arrayOf(42, "c1"))
        }.start()

        assertTrue(latch.await(3, TimeUnit.SECONDS))
        assertTrue(receivedRows.size >= 2)

        // Last callback should have the updated value
        val lastRows = receivedRows.last()
        if (lastRows.isNotEmpty()) {
            assertEquals(42L, lastRows[0]["value"])
        }

        cancellable.cancel()
        client.close()
    }

    @Test
    fun testWatchPreservesNullBindSlots() {
        val client = makeClient()

        client.createTable("nullable_counters", listOf(
            ColumnDef(name = "id", type = "TEXT", nullable = false, primaryKey = true),
            ColumnDef(name = "note", type = "TEXT"),
        ))

        client.execute(
            "INSERT INTO nullable_counters (id, note) VALUES (?, ?)",
            arrayOf("c1", null)
        )

        val latch = CountDownLatch(1)
        val cancellable = client.watch(
            "SELECT id FROM nullable_counters WHERE id = ? AND note IS ?",
            arrayOf("c1", null),
            listOf("nullable_counters")
        ) { rows ->
            if (rows.firstOrNull()?.get("id") == "c1") {
                latch.countDown()
            }
        }

        assertTrue(latch.await(2, TimeUnit.SECONDS))
        cancellable.cancel()
        client.close()
    }

    // MARK: - Schema

    @Test
    fun testAlterTable() {
        val client = makeClient()

        client.createTable("people", listOf(
            ColumnDef(name = "id", type = "TEXT", nullable = false, primaryKey = true),
            ColumnDef(name = "name", type = "TEXT"),
        ))

        client.alterTable("people", listOf(
            ColumnDef(name = "age", type = "INTEGER"),
        ))

        client.execute("INSERT INTO people (id, name, age) VALUES (?, ?, ?)", arrayOf("p1", "Alice", 30))
        val row = client.queryOne("SELECT age FROM people WHERE id = ?", arrayOf("p1"))
        assertEquals(30L, row?.get("age"))

        client.close()
    }

    @Test
    fun testTransactions() {
        val client = makeClient()

        client.createTable("txtest", listOf(
            ColumnDef(name = "id", type = "TEXT", nullable = false, primaryKey = true),
            ColumnDef(name = "val", type = "TEXT"),
        ))

        // Write transaction
        val written = client.transaction { transaction ->
            transaction.execute("INSERT INTO txtest (id, val) VALUES (?, ?)", arrayOf("t1", "hello"))
                .rowsAffected
        }
        assertEquals(1, written)

        // Read transaction
        val value = client.readTransaction { transaction ->
            transaction.queryOne("SELECT val FROM txtest WHERE id = ?", arrayOf("t1"))?.get("val")
        }
        assertEquals("hello", value)

        client.close()
    }

    @Test
    fun testMetaTablesInitialized() {
        val client = makeClient()

        assertThrows(IllegalArgumentException::class.java) {
            client.queryOne("SELECT value FROM _synchro_meta WHERE key = 'sync_lock'")
        }

        client.close()
    }

    @Test
    fun testReopenPreservesCurrentScopeState() {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val dbName = "synchro_scope_preserve_${UUID.randomUUID()}.sqlite"

        val db = SynchroDatabase.open(context, dbName)
        db.writeTransaction { rawDb ->
            rawDb.execSQL(
                """
                INSERT INTO _synchro_scopes (scope_id, cursor, checksum, generation, local_checksum)
                VALUES (?, ?, ?, ?, ?)
                """.trimIndent(),
                arrayOf("global", "opaque_cursor_token", "7", "0", "7")
            )
            rawDb.execSQL(
                """
                INSERT INTO _synchro_scope_rows (scope_id, table_name, record_id, checksum, generation)
                VALUES (?, ?, ?, ?, ?)
                """.trimIndent(),
                arrayOf("global", "categories", "seed-category", "7", "0")
            )
        }
        db.close()

        val reopened = SynchroDatabase.open(context, dbName)
        try {
            val scope = reopened.readTransaction { rawDb ->
                SynchroMeta.getScope(rawDb, "global")
            }
            assertEquals("opaque_cursor_token", scope?.cursor)
            assertEquals("7", scope?.checksum)
            assertEquals(0L, scope?.generation)
            assertEquals(scope?.checksum, scope?.localChecksum)

            val scopeRows = reopened.readTransaction { rawDb ->
                SynchroMeta.getScopeRows(rawDb, "global")
            }
            assertEquals(listOf("categories" to "seed-category"), scopeRows)
        } finally {
            reopened.close()
        }
    }
}
