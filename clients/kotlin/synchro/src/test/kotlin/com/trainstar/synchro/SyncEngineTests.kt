package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
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

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28])
class SyncEngineTests {

    private var server: MockWebServer? = null

    @After
    fun tearDown() {
        server?.shutdown()
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
                path.endsWith("/sync/schema") -> {
                    callLog.add("schema")
                    mockResponse(schemaJSON)
                }
                path.endsWith("/sync/register") -> {
                    callLog.add("register")
                    mockResponse(registerJSON)
                }
                path.endsWith("/sync/pull") -> {
                    callLog.add("pull")
                    mockResponse(pullJSON(10))
                }
                else -> mockResponse("""{"error":"unexpected: $path"}""", 500)
            }
        }

        try {
            engine.start()

            assertEquals(listOf("schema", "register", "pull"), callLog)

            val checkpoint = db.readTransaction { conn -> SynchroMeta.getInt64(conn, MetaKey.CHECKPOINT) }
            assertEquals(10L, checkpoint)

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
                path.endsWith("/sync/schema") -> mockResponse(schemaJSON)
                path.endsWith("/sync/register") -> mockResponse(registerJSON)
                path.endsWith("/sync/push") -> {
                    pushCalled = true
                    val body = Json.decodeFromString<JsonObject>(request.body.readUtf8())
                    val changes = body["changes"] as kotlinx.serialization.json.JsonArray
                    val accepted = changes.map { change ->
                        val c = change as JsonObject
                        """{"id":${c["id"]},"table_name":${c["table_name"]},"operation":${c["operation"]},"status":"applied","server_updated_at":"2026-01-01T14:00:00.000Z"}"""
                    }
                    mockResponse("""{"accepted":[${accepted.joinToString(",")}],"rejected":[],"checkpoint":20,"server_time":"2026-01-01T14:00:00.000Z","schema_version":1,"schema_hash":"test"}""")
                }
                path.endsWith("/sync/pull") -> mockResponse(pullJSON(20))
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
                path.endsWith("/sync/schema") -> mockResponse(schemaJSON)
                path.endsWith("/sync/register") -> mockResponse(registerJSON)
                path.endsWith("/sync/pull") -> {
                    mockResponse("""
                        {
                            "changes": [
                                {
                                    "id": "w1", "table_name": "orders",
                                    "data": {"id": "w1", "ship_address": "Server Address", "user_id": "u1", "updated_at": "2026-01-01T12:00:00.000Z"},
                                    "updated_at": "2026-01-01T12:00:00.000Z"
                                }
                            ],
                            "deletes": [],
                            "checkpoint": 15, "has_more": false,
                            "schema_version": 1, "schema_hash": "test"
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
    fun testPullPagesUntilComplete() = runTest {
        var pullCallCount = 0

        val (engine, db) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/schema") -> mockResponse(schemaJSON)
                path.endsWith("/sync/register") -> mockResponse(registerJSON)
                path.endsWith("/sync/pull") -> {
                    pullCallCount++
                    if (pullCallCount == 1) {
                        mockResponse("""
                            {
                                "changes": [{"id":"w1","table_name":"orders","data":{"id":"w1","ship_address":"Address 1","user_id":"u1","updated_at":"2026-01-01T12:00:00.000Z"},"updated_at":"2026-01-01T12:00:00.000Z"}],
                                "deletes":[],"checkpoint":5,"has_more":true,"schema_version":1,"schema_hash":"test"
                            }
                        """.trimIndent())
                    } else {
                        mockResponse("""
                            {
                                "changes": [{"id":"w2","table_name":"orders","data":{"id":"w2","ship_address":"Address 2","user_id":"u1","updated_at":"2026-01-01T13:00:00.000Z"},"updated_at":"2026-01-01T13:00:00.000Z"}],
                                "deletes":[],"checkpoint":10,"has_more":false,"schema_version":1,"schema_hash":"test"
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

            val checkpoint = db.readTransaction { conn -> SynchroMeta.getInt64(conn, MetaKey.CHECKPOINT) }
            assertEquals(10L, checkpoint)
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
                path.endsWith("/sync/schema") -> mockResponse(schemaJSON)
                path.endsWith("/sync/register") -> mockResponse(registerJSON)
                path.endsWith("/sync/push") -> {
                    pushCallCount++
                    if (pushCallCount == 1) {
                        MockResponse().setBody("""{"error":"unavailable"}""")
                            .setResponseCode(503)
                            .setHeader("Retry-After", "0.01")
                    } else {
                        val body = Json.decodeFromString<JsonObject>(request.body.readUtf8())
                        val changes = body["changes"] as kotlinx.serialization.json.JsonArray
                        val accepted = changes.map { change ->
                            val c = change as JsonObject
                            """{"id":${c["id"]},"table_name":${c["table_name"]},"operation":${c["operation"]},"status":"applied","server_updated_at":"2026-01-01T14:00:00.000Z"}"""
                        }
                        mockResponse("""{"accepted":[${accepted.joinToString(",")}],"rejected":[],"checkpoint":20,"server_time":"2026-01-01T14:00:00.000Z","schema_version":1,"schema_hash":"test"}""")
                    }
                }
                path.endsWith("/sync/pull") -> mockResponse(pullJSON(20))
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
    fun testStatusTransitionsDuringSyncCycle() = runTest {
        val (engine, _) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/schema") -> mockResponse(schemaJSON)
                path.endsWith("/sync/register") -> mockResponse(registerJSON)
                path.endsWith("/sync/pull") -> mockResponse(pullJSON(10))
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
                path.endsWith("/sync/schema") -> mockResponse(schemaJSON)
                path.endsWith("/sync/register") -> mockResponse(registerJSON)
                path.endsWith("/sync/push") -> {
                    val body = Json.decodeFromString<JsonObject>(request.body.readUtf8())
                    val changes = body["changes"] as kotlinx.serialization.json.JsonArray
                    val rejected = changes.map { change ->
                        val c = change as JsonObject
                        """{"id":${c["id"]},"table_name":${c["table_name"]},"operation":${c["operation"]},"status":"conflict","message":"server version is newer","server_version":{"id":${c["id"]},"table_name":${c["table_name"]},"data":{"id":${c["id"]},"ship_address":"Server Wins Address","user_id":"u1","updated_at":"2026-01-01T15:00:00.000Z"},"updated_at":"2026-01-01T15:00:00.000Z"}}"""
                    }
                    mockResponse("""{"accepted":[],"rejected":[${rejected.joinToString(",")}],"checkpoint":20,"server_time":"2026-01-01T15:00:00.000Z","schema_version":1,"schema_hash":"test"}""")
                }
                path.endsWith("/sync/pull") -> mockResponse(pullJSON(20))
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
        val db = SynchroDatabase(context, dbName)
        val httpClient = HttpClient(config)
        val schemaManager = SchemaManager(db)
        val changeTracker = ChangeTracker(db)
        val pullProcessor = PullProcessor(db)
        val pushProcessor = PushProcessor(db, changeTracker)

        val engine = SyncEngine(config, db, httpClient, schemaManager, changeTracker, pullProcessor, pushProcessor)
        return Pair(engine, db)
    }

    private fun makeIntegrationEnv(handler: (RecordedRequest) -> MockResponse): Pair<SyncEngine, SynchroDatabase> {
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
            maxRetryAttempts = 3
        )
        val db = SynchroDatabase(context, dbName)
        val httpClient = HttpClient(config, OkHttpClient())
        val schemaManager = SchemaManager(db)
        val changeTracker = ChangeTracker(db)
        val pullProcessor = PullProcessor(db)
        val pushProcessor = PushProcessor(db, changeTracker)

        val engine = SyncEngine(config, db, httpClient, schemaManager, changeTracker, pullProcessor, pushProcessor)
        return Pair(engine, db)
    }

    // MARK: - Mock JSON Helpers

    private val schemaJSON = """
        {
            "schema_version": 1, "schema_hash": "test",
            "server_time": "2026-01-01T12:00:00.000Z",
            "tables": [{
                "table_name": "orders",
                "push_policy": "owner_only",
                "updated_at_column": "updated_at",
                "deleted_at_column": "deleted_at",
                "primary_key": ["id"],
                "columns": [
                    {"name":"id","db_type":"uuid","logical_type":"string","nullable":false,"default_kind":"none","is_primary_key":true},
                    {"name":"ship_address","db_type":"text","logical_type":"string","nullable":true,"default_kind":"none","is_primary_key":false},
                    {"name":"user_id","db_type":"uuid","logical_type":"string","nullable":false,"default_kind":"none","is_primary_key":false},
                    {"name":"updated_at","db_type":"timestamp","logical_type":"datetime","nullable":false,"default_kind":"none","is_primary_key":false},
                    {"name":"deleted_at","db_type":"timestamp","logical_type":"datetime","nullable":true,"default_kind":"none","is_primary_key":false}
                ]
            }]
        }
    """.trimIndent()

    private val registerJSON = """
        {
            "id": "server-client-1",
            "server_time": "2026-01-01T12:00:00.000Z",
            "checkpoint": 0,
            "schema_version": 1,
            "schema_hash": "test"
        }
    """.trimIndent()

    private fun pullJSON(checkpoint: Int): String = """
        {
            "changes": [],
            "deletes": [],
            "checkpoint": $checkpoint,
            "has_more": false,
            "schema_version": 1,
            "schema_hash": "test"
        }
    """.trimIndent()

    private fun mockResponse(body: String, statusCode: Int = 200): MockResponse =
        MockResponse().setBody(body).setResponseCode(statusCode)
}
