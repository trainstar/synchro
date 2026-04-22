package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.After
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28])
class SyncEngineTests {

    private var server: MockWebServer? = null
    private val databases = TestDatabaseTracker()

    @After
    fun tearDown() {
        server?.shutdown()
        databases.closeAll()
    }

    // MARK: - Unit Tests

    @Test
    fun testCallbackRegistrationAndCancellation() {
        val (engine, _) = makeSyncEngine()

        val statusUpdates = mutableListOf<String>()
        val cancellable1 = engine.onStatusChange { status ->
            when (status) {
                is SyncStatus.Idle -> statusUpdates.add("idle")
                is SyncStatus.Syncing -> statusUpdates.add("syncing")
                is SyncStatus.Error -> statusUpdates.add("error")
                is SyncStatus.Stopped -> statusUpdates.add("stopped")
            }
        }

        val conflictEvents = mutableListOf<String>()
        val cancellable2 = engine.onConflict { event ->
            conflictEvents.add(event.recordID)
        }

        // Stop triggers a status update
        engine.stop()
        assertEquals(listOf("stopped"), statusUpdates)

        // Cancel callbacks
        cancellable1.cancel()
        cancellable2.cancel()

        // After cancel, no more updates
        statusUpdates.clear()
        engine.stop()
        assertTrue(statusUpdates.isEmpty())
    }

    @Test
    fun testMultipleCallbacksIndependentCancellation() {
        val (engine, _) = makeSyncEngine()

        val updates1 = mutableListOf<String>()
        val updates2 = mutableListOf<String>()

        val cancellable1 = engine.onStatusChange { updates1.add("hit") }
        @Suppress("UNUSED_VARIABLE")
        val cancellable2 = engine.onStatusChange { updates2.add("hit") }

        engine.stop()
        assertEquals(1, updates1.size)
        assertEquals(1, updates2.size)

        // Cancel only first
        cancellable1.cancel()
        updates1.clear()
        updates2.clear()

        engine.stop()
        assertEquals(0, updates1.size)
        assertEquals(1, updates2.size)
    }

    // MARK: - Behavioral Sync Tests

    @Test
    fun testStartInitializesAndSyncs() = runTest {
        val callLog = mutableListOf<String>()

        val (engine, db) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> {
                    callLog.add("connect")
                    mockResponse(connectJSON)
                }
                path.endsWith("/sync/rebuild") -> {
                    callLog.add("rebuild")
                    mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                }
                path.endsWith("/sync/pull") -> {
                    callLog.add("pull")
                    mockResponse(scopePullJSON(cursor = "scope_cursor_2"))
                }
                else -> mockResponse("""{"error":"unexpected: $path"}""", 500)
            }
        }

        try {
            engine.start()

            assertEquals(listOf("connect", "rebuild", "pull"), callLog)

            val scopeSetVersion = db.readTransaction { conn -> SynchroMeta.getInt64(conn, MetaKey.SCOPE_SET_VERSION) }
            assertEquals(1L, scopeSetVersion)

            val scopes = db.readTransaction { conn -> SynchroMeta.getAllScopes(conn) }
            assertEquals(1, scopes.size)
            assertEquals(scopeID, scopes[0].scopeID)
            assertEquals("scope_cursor_2", scopes[0].cursor)
            assertEquals("sum_2", scopes[0].checksum)

            val tables = db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'")
            assertEquals(1, tables.size)

            val triggers = db.query("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_synchro_cdc_%orders'")
            assertEquals(3, triggers.size)
        } finally {
            engine.stop()
        }
    }

    @Test
    fun testPushAcceptedAppliesRYOW() = runTest {
        var pushCalled = false

        val (engine, db) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> mockResponse(connectJSON)
                path.endsWith("/sync/rebuild") -> mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                path.endsWith("/sync/push") -> {
                    pushCalled = true
                    val body = Json.decodeFromString<JsonObject>(request.body.readUtf8())
                    val mutations = body["mutations"] as kotlinx.serialization.json.JsonArray
                    val accepted = mutations.map { change ->
                        val c = change as JsonObject
                        val pk = c["pk"] as JsonObject
                        val columns = c["columns"] as? JsonObject ?: JsonObject(emptyMap())
                        val id = pk["id"]!!
                        val serverRow = buildJsonObject {
                            put("id", id)
                            for ((key, value) in columns) put(key, value)
                            put("updated_at", JsonPrimitive("2026-01-01T14:00:00.000Z"))
                            put("deleted_at", JsonNull)
                        }
                        """{"mutation_id":${c["mutation_id"]},"table":${c["table"]},"pk":$pk,"status":"applied","server_row":$serverRow,"server_version":"2026-01-01T14:00:00.000Z"}"""
                    }
                    mockResponse("""{"server_time":"2026-01-01T14:00:00.000Z","accepted":[${accepted.joinToString(",")}],"rejected":[]}""")
                }
                path.endsWith("/sync/pull") -> mockResponse(scopePullJSON(cursor = "scope_cursor_2"))
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        try {
            engine.start()

            db.execute(
                "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arrayOf("w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z")
            )
            val tracker = ChangeTracker(db)
            assertTrue(tracker.hasPendingChanges())

            engine.syncNow()

            assertTrue(pushCalled)
            assertFalse(tracker.hasPendingChanges())

            val row = db.queryOne("SELECT updated_at FROM orders WHERE id = ?", arrayOf("w1"))
            assertEquals("2026-01-01T14:00:00.000Z", row?.get("updated_at"))
        } finally {
            engine.stop()
        }
    }

    @Test
    fun testPullAppliesServerRecord() = runTest {
        val (engine, db) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> mockResponse(connectJSON)
                path.endsWith("/sync/rebuild") -> mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                path.endsWith("/sync/pull") -> {
                    mockResponse("""
                        {
                            "changes": [
                                {
                                    "scope": "$scopeID",
                                    "table": "orders",
                                    "op": "upsert",
                                    "pk": {"id": "w1"},
                                    "row": {"id": "w1", "ship_address": "Server Address", "user_id": "u1", "updated_at": "2026-01-01T12:00:00.000Z", "deleted_at": null},
                                    "server_version": "sv_1"
                                }
                            ],
                            "scope_set_version": 1,
                            "scope_cursors": {"$scopeID": "scope_cursor_2"},
                            "scope_updates": {"add": [], "remove": []},
                            "rebuild": [],
                            "has_more": false,
                            "checksums": {"$scopeID": "sum_2"}
                        }
                    """.trimIndent())
                }
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        try {
            engine.start()

            val row = db.queryOne("SELECT ship_address FROM orders WHERE id = ?", arrayOf("w1"))
            assertEquals("Server Address", row?.get("ship_address"))

            val tracker = ChangeTracker(db)
            assertFalse(tracker.hasPendingChanges())
        } finally {
            engine.stop()
        }
    }

    @Test
    fun testScopeRemovalDeletesLocalRowWithoutQueueingPendingDelete() = runTest {
        var pullCallCount = 0

        val (engine, db) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> mockResponse(connectJSON)
                path.endsWith("/sync/rebuild") -> mockResponse(
                    rebuildJSON(
                        records = """
                            [
                                {
                                    "table": "orders",
                                    "pk": {"id": "w1"},
                                    "row": {"id": "w1", "ship_address": "Seeded", "user_id": "u1", "updated_at": "2026-01-01T12:00:00.000Z", "deleted_at": null},
                                    "server_version": "sv_1"
                                }
                            ]
                        """.trimIndent(),
                        finalCursor = "scope_cursor_1"
                    )
                )
                path.endsWith("/sync/pull") -> {
                    pullCallCount++
                    if (pullCallCount == 1) {
                        mockResponse(scopePullJSON(cursor = "scope_cursor_2"))
                    } else {
                        mockResponse(
                            """
                                {
                                    "changes": [],
                                    "scope_set_version": 2,
                                    "scope_cursors": {"$scopeID": "scope_cursor_3"},
                                    "scope_updates": {"add": [], "remove": ["$scopeID"]},
                                    "rebuild": [],
                                    "has_more": false,
                                    "checksums": {}
                                }
                            """.trimIndent()
                        )
                    }
                }
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        try {
            engine.start()
            engine.syncNow()

            val row = db.queryOne("SELECT id FROM orders WHERE id = ?", arrayOf("w1"))
            assertNull(row)

            val tracker = ChangeTracker(db)
            assertFalse(tracker.hasPendingChanges())
        } finally {
            engine.stop()
        }
    }

    @Test
    fun testPullPagesUntilComplete() = runTest {
        var pullCallCount = 0

        val (engine, db) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> mockResponse(connectJSON)
                path.endsWith("/sync/rebuild") -> mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                path.endsWith("/sync/pull") -> {
                    pullCallCount++
                    if (pullCallCount == 1) {
                        mockResponse("""
                            {
                                "changes": [{"scope":"$scopeID","table":"orders","op":"upsert","pk":{"id":"w1"},"row":{"id":"w1","ship_address":"Address 1","user_id":"u1","updated_at":"2026-01-01T12:00:00.000Z","deleted_at":null},"server_version":"sv_1"}],
                                "scope_set_version":1,
                                "scope_cursors":{"$scopeID":"scope_cursor_mid"},
                                "scope_updates":{"add":[],"remove":[]},
                                "rebuild":[],
                                "has_more":true
                            }
                        """.trimIndent())
                    } else {
                        mockResponse("""
                            {
                                "changes": [{"scope":"$scopeID","table":"orders","op":"upsert","pk":{"id":"w2"},"row":{"id":"w2","ship_address":"Address 2","user_id":"u1","updated_at":"2026-01-01T13:00:00.000Z","deleted_at":null},"server_version":"sv_2"}],
                                "scope_set_version":1,
                                "scope_cursors":{"$scopeID":"scope_cursor_2"},
                                "scope_updates":{"add":[],"remove":[]},
                                "rebuild":[],
                                "has_more":false,
                                "checksums":{"$scopeID":"sum_2"}
                            }
                        """.trimIndent())
                    }
                }
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        try {
            engine.start()

            assertEquals(2, pullCallCount)

            val count = db.query("SELECT id FROM orders")
            assertEquals(2, count.size)

            val scopes = db.readTransaction { conn -> SynchroMeta.getAllScopes(conn) }
            assertEquals("scope_cursor_2", scopes.first().cursor)
        } finally {
            engine.stop()
        }
    }

    @Test
    fun testSyncRetriesOnRetryableError() = runTest {
        var pushCallCount = 0

        val (engine, db) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> mockResponse(connectJSON)
                path.endsWith("/sync/rebuild") -> mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                path.endsWith("/sync/push") -> {
                    pushCallCount++
                    if (pushCallCount == 1) {
                        MockResponse().setBody("""{"error":"unavailable"}""")
                            .setResponseCode(503)
                            .setHeader("Retry-After", "0.01")
                    } else {
                        val body = Json.decodeFromString<JsonObject>(request.body.readUtf8())
                        val mutations = body["mutations"] as kotlinx.serialization.json.JsonArray
                        val accepted = mutations.map { change ->
                            val c = change as JsonObject
                            val pk = c["pk"] as JsonObject
                            val columns = c["columns"] as? JsonObject ?: JsonObject(emptyMap())
                            val id = pk["id"]!!
                            val serverRow = buildJsonObject {
                                put("id", id)
                                for ((key, value) in columns) put(key, value)
                                put("updated_at", JsonPrimitive("2026-01-01T14:00:00.000Z"))
                                put("deleted_at", JsonNull)
                            }
                            """{"mutation_id":${c["mutation_id"]},"table":${c["table"]},"pk":$pk,"status":"applied","server_row":$serverRow,"server_version":"2026-01-01T14:00:00.000Z"}"""
                        }
                        mockResponse("""{"server_time":"2026-01-01T14:00:00.000Z","accepted":[${accepted.joinToString(",")}],"rejected":[]}""")
                    }
                }
                path.endsWith("/sync/pull") -> mockResponse(scopePullJSON(cursor = "scope_cursor_2"))
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        try {
            engine.start()

            db.execute(
                "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arrayOf("w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z")
            )

            engine.syncNow()

            assertEquals(2, pushCallCount)
            val tracker = ChangeTracker(db)
            assertFalse(tracker.hasPendingChanges())
        } finally {
            engine.stop()
        }
    }

    @Test
    fun testRetryableStartupFailureDoesNotRequireAppRestart() = runTest {
        var pullCallCount = 0

        val (engine, db) = makeIntegrationEnv(maxRetryAttempts = 0) { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> mockResponse(connectJSON)
                path.endsWith("/sync/rebuild") -> mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                path.endsWith("/sync/pull") -> {
                    pullCallCount++
                    if (pullCallCount == 1) {
                        mockResponse("""{"error":"temporarily unavailable"}""", 503)
                            .addHeader("Retry-After", "0.01")
                    } else {
                        mockResponse(scopePullJSON(cursor = "scope_cursor_2"))
                    }
                }
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        val statuses = mutableListOf<String>()
        engine.onStatusChange { status ->
            when (status) {
                is SyncStatus.Idle -> statuses.add("idle")
                is SyncStatus.Syncing -> statuses.add("syncing")
                is SyncStatus.Error -> statuses.add("error")
                is SyncStatus.Stopped -> statuses.add("stopped")
            }
        }

        val initialSyncCompleted = CountDownLatch(1)

        try {
            engine.start(SyncOptions(initialSyncCompleted = { initialSyncCompleted.countDown() }))

            try {
                engine.start()
                fail("Expected AlreadyStarted while engine owns startup retry")
            } catch (e: SynchroError.AlreadyStarted) {
            }

            assertTrue(initialSyncCompleted.await(2, TimeUnit.SECONDS))

            assertEquals(2, pullCallCount)
            assertTrue(statuses.contains("error"))
            assertEquals("idle", statuses.last())

            val scope = db.readTransaction { conn ->
                SynchroMeta.getAllScopes(conn).firstOrNull()
            }
            assertEquals("scope_cursor_2", scope?.cursor)
        } finally {
            engine.stop()
        }
    }

    @Test
    fun testNonRetryableStartupFailureStillThrowsAndAllowsRestart() = runTest {
        var returnSuccess = false

        val (engine, _) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> {
                    if (returnSuccess) {
                        mockResponse(connectJSON)
                    } else {
                        mockResponse("""{"error":"fatal bootstrap"}""", 500)
                    }
                }
                path.endsWith("/sync/rebuild") -> mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                path.endsWith("/sync/pull") -> mockResponse(scopePullJSON(cursor = "scope_cursor_2"))
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        try {
            try {
                engine.start()
                fail("Expected non-retryable startup failure")
            } catch (e: Exception) {
                assertFalse(e is RetryableError)
            }

            returnSuccess = true
            engine.start()
        } finally {
            engine.stop()
        }
    }

    @Test
    fun testStatusTransitionsDuringSyncCycle() = runTest {
        val (engine, _) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> mockResponse(connectJSON)
                path.endsWith("/sync/rebuild") -> mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                path.endsWith("/sync/pull") -> mockResponse(scopePullJSON(cursor = "scope_cursor_2"))
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        val statuses = mutableListOf<String>()
        engine.onStatusChange { status ->
            when (status) {
                is SyncStatus.Idle -> statuses.add("idle")
                is SyncStatus.Syncing -> statuses.add("syncing")
                is SyncStatus.Error -> statuses.add("error")
                is SyncStatus.Stopped -> statuses.add("stopped")
            }
        }

        try {
            engine.start()

            // start() sets idle after register, then syncing+idle for initial sync cycle
            assertEquals(listOf("idle", "syncing", "idle"), statuses)

            // syncNow triggers another cycle
            statuses.clear()
            engine.syncNow()
            assertEquals(listOf("syncing", "idle"), statuses)

            // stop sets stopped
            statuses.clear()
            engine.stop()
            assertEquals(listOf("stopped"), statuses)
        } finally {
            engine.stop()
        }
    }

    @Test
    fun testConflictCallbackFiresDuringSyncCycle() = runTest {
        val receivedConflicts = mutableListOf<ConflictEvent>()

        val (engine, db) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> mockResponse(connectJSON)
                path.endsWith("/sync/rebuild") -> mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                path.endsWith("/sync/push") -> {
                    val body = Json.decodeFromString<JsonObject>(request.body.readUtf8())
                    val mutations = body["mutations"] as kotlinx.serialization.json.JsonArray
                    val rejected = mutations.map { change ->
                        val c = change as JsonObject
                        val pk = c["pk"] as JsonObject
                        """{"mutation_id":${c["mutation_id"]},"table":${c["table"]},"pk":$pk,"status":"conflict","code":"version_conflict","message":"server version is newer","server_row":{"id":${pk["id"]},"ship_address":"Server Wins Address","user_id":"u1","updated_at":"2026-01-01T15:00:00.000Z","deleted_at":null},"server_version":"2026-01-01T15:00:00.000Z"}"""
                    }
                    mockResponse("""{"server_time":"2026-01-01T15:00:00.000Z","accepted":[],"rejected":[${rejected.joinToString(",")}]}""")
                }
                path.endsWith("/sync/pull") -> mockResponse(scopePullJSON(cursor = "scope_cursor_2"))
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        engine.onConflict { event -> receivedConflicts.add(event) }

        try {
            engine.start()

            db.execute(
                "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arrayOf("w1", "Client Address", "u1", "2026-01-01T10:00:00.000Z")
            )

            engine.syncNow()

            assertEquals(1, receivedConflicts.size)
            assertEquals("orders", receivedConflicts[0].table)
            assertEquals("w1", receivedConflicts[0].recordID)
            assertEquals(AnyCodable("Server Wins Address"), receivedConflicts[0].serverData?.get("ship_address"))

            val row = db.queryOne("SELECT ship_address FROM orders WHERE id = ?", arrayOf("w1"))
            assertEquals("Server Wins Address", row?.get("ship_address"))

            val tracker = ChangeTracker(db)
            assertFalse(tracker.hasPendingChanges())
        } finally {
            engine.stop()
        }
    }

    // MARK: - Helpers

    private fun makeSyncEngine(): Pair<SyncEngine, SynchroDatabase> {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val dbName = "synchro_test_${UUID.randomUUID()}.sqlite"
        val config = SynchroConfig(
            dbPath = dbName,
            serverURL = "http://test.local",
            authProvider = { "token" },
            clientID = "test",
            appVersion = "1.0.0",
            maxRetryAttempts = 3
        )
        val db = databases.open(context, dbName)
        val httpClient = HttpClient(config)
        val schemaManager = SchemaManager(db)
        val changeTracker = ChangeTracker(db)
        val pullProcessor = PullProcessor(db)
        val pushProcessor = PushProcessor(db, changeTracker)

        val engine = SyncEngine(config, db, httpClient, schemaManager, changeTracker, pullProcessor, pushProcessor)
        return Pair(engine, db)
    }

    private fun makeIntegrationEnv(
        maxRetryAttempts: Int = 3,
        handler: (RecordedRequest) -> MockResponse
    ): Pair<SyncEngine, SynchroDatabase> {
        server = MockWebServer()
        server!!.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = handler(request)
        }
        server!!.start()

        val context = ApplicationProvider.getApplicationContext<Context>()
        val dbName = "synchro_test_${UUID.randomUUID()}.sqlite"
        val config = SynchroConfig(
            dbPath = dbName,
            serverURL = server!!.url("/").toString().trimEnd('/'),
            authProvider = { "token" },
            clientID = "test-device",
            appVersion = "1.0.0",
            syncInterval = 999.0,
            maxRetryAttempts = maxRetryAttempts
        )
        val db = databases.open(context, dbName)
        val httpClient = HttpClient(config, OkHttpClient())
        val schemaManager = SchemaManager(db)
        val changeTracker = ChangeTracker(db)
        val pullProcessor = PullProcessor(db)
        val pushProcessor = PushProcessor(db, changeTracker)

        val engine = SyncEngine(config, db, httpClient, schemaManager, changeTracker, pullProcessor, pushProcessor)
        return Pair(engine, db)
    }

    // MARK: - Mock JSON Helpers

    private val scopeID = "orders_user:u1"

    private val connectJSON = """
        {
            "server_time": "2026-01-01T12:00:00.000Z",
            "protocol_version": 1,
            "scope_set_version": 1,
            "schema": {
                "version": 1,
                "hash": "test",
                "action": "replace"
            },
            "scopes": {
                "add": [
                    {
                        "id": "$scopeID",
                        "cursor": null
                    }
                ],
                "remove": []
            },
            "schema_definition": {
                "tables": [
                    {
                        "name": "orders",
                        "primary_key": ["id"],
                        "updated_at_column": "updated_at",
                        "deleted_at_column": "deleted_at",
                        "composition": "single_scope",
                        "columns": [
                            {"name":"id","type":"string","nullable":false},
                            {"name":"ship_address","type":"string","nullable":true},
                            {"name":"user_id","type":"string","nullable":false},
                            {"name":"updated_at","type":"datetime","nullable":false},
                            {"name":"deleted_at","type":"datetime","nullable":true}
                        ]
                    }
                ]
            }
        }
    """.trimIndent()

    private fun rebuildJSON(
        records: String = "[]",
        cursor: String? = null,
        hasMore: Boolean = false,
        finalCursor: String? = null,
        checksum: String = "sum_1"
    ): String = """
        {
            "scope": "$scopeID",
            "records": $records,
            "cursor": ${cursor?.let { "\"$it\"" } ?: "null"},
            "has_more": $hasMore,
            "final_scope_cursor": ${finalCursor?.let { "\"$it\"" } ?: "null"},
            "checksum": ${if (hasMore) "null" else "\"$checksum\""}
        }
    """.trimIndent()

    private fun scopePullJSON(
        cursor: String,
        changes: String = "[]",
        hasMore: Boolean = false,
        rebuild: String = "[]"
    ): String = """
        {
            "changes": $changes,
            "scope_set_version": 1,
            "scope_cursors": {"$scopeID": "$cursor"},
            "scope_updates": {"add": [], "remove": []},
            "rebuild": $rebuild,
            "has_more": $hasMore,
            "checksums": {"$scopeID": "sum_2"}
        }
    """.trimIndent()

    private fun mockResponse(body: String, statusCode: Int = 200): MockResponse =
        MockResponse().setBody(body).setResponseCode(statusCode)
}
