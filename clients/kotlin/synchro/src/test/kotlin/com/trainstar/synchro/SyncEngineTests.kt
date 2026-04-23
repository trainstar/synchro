package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
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
            assertEquals("0", scopes[0].checksum)

            val tables = db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'")
            assertEquals(1, tables.size)

            val triggers = db.query("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_synchro_cdc_%orders'")
            assertEquals(3, triggers.size)
        } finally {
            engine.stop()
        }
    }

    @Test
    fun testWarmStartUsesExactlyOneConnectAndOnePullRequest() = runTest {
        val callLog = mutableListOf<String>()

        val (engine, db) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> {
                    callLog.add("connect")
                    mockResponse(connectResumeJSON)
                }
                path.endsWith("/sync/pull") -> {
                    callLog.add("pull")
                    mockResponse(scopePullJSON(cursor = "scope_cursor_2", checksum = "0"))
                }
                path.endsWith("/sync/push") -> {
                    callLog.add("push")
                    mockResponse("""{"error":"unexpected push"}""", 500)
                }
                path.endsWith("/sync/rebuild") -> {
                    callLog.add("rebuild")
                    mockResponse("""{"error":"unexpected rebuild"}""", 500)
                }
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        try {
            val schemaManager = SchemaManager(db)
            schemaManager.reconcileLocalSchema(
                schemaVersion = 1,
                schemaHash = "test",
                tables = listOf(ordersLocalSchemaTable(includeNotes = false))
            )
            db.writeSyncLockedTransaction { conn ->
                SynchroMeta.upsertScope(
                    conn,
                    scopeId = scopeID,
                    cursor = "scope_cursor_1",
                    checksum = "0"
                )
            }

            engine.start()

            assertEquals(listOf("connect", "pull"), callLog)
        } finally {
            engine.stop()
        }
    }

    @Test
    fun testSteadyStatePullOnlyCycleUsesSinglePullRequest() = runTest {
        var connectCallCount = 0
        var pullCallCount = 0
        var pushCallCount = 0
        var rebuildCallCount = 0

        val (engine, db) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> {
                    connectCallCount++
                    mockResponse(connectResumeJSON)
                }
                path.endsWith("/sync/pull") -> {
                    pullCallCount++
                    val cursor = if (pullCallCount == 1) "scope_cursor_2" else "scope_cursor_3"
                    mockResponse(scopePullJSON(cursor = cursor, checksum = "0"))
                }
                path.endsWith("/sync/push") -> {
                    pushCallCount++
                    mockResponse("""{"error":"unexpected push"}""", 500)
                }
                path.endsWith("/sync/rebuild") -> {
                    rebuildCallCount++
                    mockResponse("""{"error":"unexpected rebuild"}""", 500)
                }
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        try {
            val schemaManager = SchemaManager(db)
            schemaManager.reconcileLocalSchema(
                schemaVersion = 1,
                schemaHash = "test",
                tables = listOf(ordersLocalSchemaTable(includeNotes = false))
            )
            db.writeTransaction { conn ->
                SynchroMeta.upsertScope(
                    conn,
                    scopeId = scopeID,
                    cursor = "scope_cursor_1",
                    checksum = "0"
                )
            }

            engine.start()
            connectCallCount = 0
            pullCallCount = 0
            pushCallCount = 0
            rebuildCallCount = 0

            engine.syncNow()

            assertEquals(0, connectCallCount)
            assertEquals(0, rebuildCallCount)
            assertEquals(0, pushCallCount)
            assertEquals(1, pullCallCount)
        } finally {
            engine.stop()
        }
    }

    @Test
    fun testSteadyStatePushPlusPullCycleUsesTwoRequests() = runTest {
        val callLog = mutableListOf<String>()

        val (engine, db) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> {
                    callLog.add("connect")
                    mockResponse(connectResumeJSON)
                }
                path.endsWith("/sync/push") -> {
                    callLog.add("push")
                    val body = Json.decodeFromString<JsonObject>(request.body.readUtf8())
                    val mutations = body["mutations"]!!.jsonArray
                    val accepted = mutations.map { change ->
                        val c = change.jsonObject
                        val pk = c["pk"]!!.jsonObject
                        val columns = c["columns"]?.jsonObject ?: JsonObject(emptyMap())
                        val id = pk["id"]!!
                        val serverRow = buildJsonObject {
                            put("id", id)
                            for ((key, value) in columns) put(key, value)
                            put("updated_at", JsonPrimitive("2026-01-01T14:00:00.000Z"))
                            put("deleted_at", JsonNull)
                        }
                        """{"mutation_id":${c["mutation_id"]},"table":${c["table"]},"pk":$pk,"status":"applied","server_row":$serverRow,"server_version":"opaque_server_version_after_push"}"""
                    }
                    mockResponse("""{"server_time":"2026-01-01T14:00:00.000Z","accepted":[${accepted.joinToString(",")}],"rejected":[]}""")
                }
                path.endsWith("/sync/pull") -> {
                    callLog.add("pull")
                    mockResponse(scopePullJSON(cursor = "scope_cursor_2", checksum = "0"))
                }
                path.endsWith("/sync/rebuild") -> {
                    callLog.add("rebuild")
                    mockResponse("""{"error":"unexpected rebuild"}""", 500)
                }
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        try {
            val schemaManager = SchemaManager(db)
            schemaManager.reconcileLocalSchema(
                schemaVersion = 1,
                schemaHash = "test",
                tables = listOf(ordersLocalSchemaTable(includeNotes = false))
            )
            db.writeTransaction { conn ->
                SynchroMeta.upsertScope(
                    conn,
                    scopeId = scopeID,
                    cursor = "scope_cursor_1",
                    checksum = "0"
                )
            }

            engine.start()
            callLog.clear()

            db.execute(
                "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arrayOf("w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z")
            )

            engine.syncNow()

            assertEquals(listOf("push", "pull"), callLog)
        } finally {
            engine.stop()
        }
    }

    @Test
    fun testConnectRebuildLocalReconcilesSchemaAndRebuildsExistingScope() = runTest {
        val callLog = mutableListOf<String>()

        val (engine, db) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> {
                    callLog.add("connect")
                    mockResponse(connectRebuildLocalJSON)
                }
                path.endsWith("/sync/rebuild") -> {
                    callLog.add("rebuild")
                    mockResponse(
                        rebuildJSON(
                            records = """
                                [
                                    {
                                        "table": "orders",
                                        "pk": {"id": "w1"},
                                        "row": {"id": "w1", "ship_address": "Rebuilt Address", "user_id": "u1", "notes": "schema rebuild local", "updated_at": "2026-01-01T12:00:00.000Z", "deleted_at": null},
                                        "row_checksum": 11,
                                        "server_version": "opaque_server_version_rebuild"
                                    }
                                ]
                            """.trimIndent(),
                            finalCursor = "scope_cursor_rebuilt",
                            checksum = "11"
                        )
                    )
                }
                path.endsWith("/sync/pull") -> {
                    callLog.add("pull")
                    mockResponse(
                        """
                            {
                                "changes": [],
                                "scope_set_version": 2,
                                "scope_cursors": {"$scopeID": "scope_cursor_after_rebuild"},
                                "scope_updates": {"add": [], "remove": []},
                                "rebuild": [],
                                "has_more": false,
                                "checksums": {"$scopeID": "11"}
                            }
                        """.trimIndent()
                    )
                }
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        try {
            val schemaManager = SchemaManager(db)
            schemaManager.reconcileLocalSchema(
                schemaVersion = 1,
                schemaHash = "old_hash",
                tables = listOf(ordersLocalSchemaTable(includeNotes = false))
            )
            db.writeSyncLockedTransaction { conn ->
                SynchroMeta.upsertScope(
                    conn,
                    scopeId = scopeID,
                    cursor = "scope_cursor_old",
                    checksum = "3"
                )
                conn.execSQL(
                    "INSERT INTO orders (id, ship_address, user_id, updated_at, deleted_at) VALUES (?, ?, ?, ?, ?)",
                    arrayOf("w1", "Old Address", "u1", "2026-01-01T10:00:00.000Z", null)
                )
            }

            val tracker = ChangeTracker(db)
            assertFalse(tracker.hasPendingChanges())

            engine.start()

            assertEquals(listOf("connect", "rebuild", "pull"), callLog)

            val columnNames = mutableSetOf<String>()
            db.readTransaction { conn ->
                conn.rawQuery("PRAGMA table_info(orders)", null).use { cursor ->
                    val nameIndex = cursor.getColumnIndex("name")
                    while (cursor.moveToNext()) {
                        columnNames.add(cursor.getString(nameIndex))
                    }
                }
            }
            assertTrue(columnNames.contains("notes"))

            val row = db.queryOne("SELECT ship_address, notes FROM orders WHERE id = ?", arrayOf("w1"))
            assertEquals("Rebuilt Address", row?.get("ship_address"))
            assertEquals("schema rebuild local", row?.get("notes"))

            val scope = db.readTransaction { conn ->
                SynchroMeta.getScope(conn, scopeID)
            }
            assertEquals("scope_cursor_after_rebuild", scope?.cursor)
            assertEquals("11", scope?.checksum)
            assertEquals(11, scope?.localChecksum)

            val schemaVersion = db.readTransaction { conn ->
                SynchroMeta.getInt64(conn, MetaKey.SCHEMA_VERSION)
            }
            assertEquals(2L, schemaVersion)
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
                                    "row_checksum": 7,
                                    "server_version": "sv_1"
                                }
                            ],
                            "scope_set_version": 1,
                            "scope_cursors": {"$scopeID": "scope_cursor_2"},
                            "scope_updates": {"add": [], "remove": []},
                            "rebuild": [],
                            "has_more": false,
                            "checksums": {"$scopeID": "7"}
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
                                    "row_checksum": 7,
                                    "server_version": "sv_1"
                                }
                            ]
                        """.trimIndent(),
                        finalCursor = "scope_cursor_1",
                        checksum = "7"
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
                                "changes": [{"scope":"$scopeID","table":"orders","op":"upsert","pk":{"id":"w1"},"row":{"id":"w1","ship_address":"Address 1","user_id":"u1","updated_at":"2026-01-01T12:00:00.000Z","deleted_at":null},"row_checksum":7,"server_version":"sv_1"}],
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
                                "changes": [{"scope":"$scopeID","table":"orders","op":"upsert","pk":{"id":"w2"},"row":{"id":"w2","ship_address":"Address 2","user_id":"u1","updated_at":"2026-01-01T13:00:00.000Z","deleted_at":null},"row_checksum":9,"server_version":"sv_2"}],
                                "scope_set_version":1,
                                "scope_cursors":{"$scopeID":"scope_cursor_2"},
                                "scope_updates":{"add":[],"remove":[]},
                                "rebuild":[],
                                "has_more":false,
                                "checksums":{"$scopeID":"14"}
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
    fun testTerminalPullChecksumMismatchForcesImmediateRebuild() = runTest {
        var pullCallCount = 0
        var rebuildCallCount = 0

        val scopeRecord = """
            {
                "table": "orders",
                "pk": {"id": "w1"},
                "row": {"id": "w1", "ship_address": "Recovered Address", "user_id": "u1", "updated_at": "2026-01-01T12:00:00.000Z", "deleted_at": null},
                "row_checksum": 7,
                "server_version": "sv_1"
            }
        """.trimIndent()

        val (engine, db) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> mockResponse(connectJSON)
                path.endsWith("/sync/rebuild") -> {
                    rebuildCallCount++
                    if (rebuildCallCount == 1) {
                        mockResponse(
                            rebuildJSON(
                                records = "[$scopeRecord]",
                                finalCursor = "scope_cursor_1",
                                checksum = "7"
                            )
                        )
                    } else {
                        mockResponse(
                            rebuildJSON(
                                records = "[$scopeRecord]",
                                finalCursor = "scope_cursor_rebuilt",
                                checksum = "7"
                            )
                        )
                    }
                }
                path.endsWith("/sync/pull") -> {
                    pullCallCount++
                    if (pullCallCount == 1) {
                        mockResponse(scopePullJSON(cursor = "scope_cursor_2", checksum = "7"))
                    } else {
                        mockResponse(
                            """
                                {
                                    "changes": [],
                                    "scope_set_version": 1,
                                    "scope_cursors": {},
                                    "scope_updates": {"add": [], "remove": []},
                                    "rebuild": [],
                                    "has_more": false,
                                    "checksums": {"$scopeID": "7"}
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

            db.writeSyncLockedTransaction { conn ->
                SynchroMeta.deleteScopeRow(conn, scopeID, "orders", "w1")
                conn.execSQL("DELETE FROM orders WHERE id = ?", arrayOf("w1"))
            }

            val tracker = ChangeTracker(db)
            assertFalse(tracker.hasPendingChanges())

            engine.syncNow()

            assertEquals(2, pullCallCount)
            assertEquals(2, rebuildCallCount)

            val row = db.queryOne("SELECT ship_address FROM orders WHERE id = ?", arrayOf("w1"))
            assertEquals("Recovered Address", row?.get("ship_address"))

            val scope = db.readTransaction { conn ->
                SynchroMeta.getScope(conn, scopeID)
            }
            assertEquals("scope_cursor_rebuilt", scope?.cursor)
            assertEquals("7", scope?.checksum)
            assertEquals(7, scope?.localChecksum)
        } finally {
            engine.stop()
        }
    }

    @Test
    fun testQueuedMutationSurvivesRestartAndPushesExactlyOnce() = runTest {
        val dbName = "restart_${UUID.randomUUID()}.sqlite"
        val clientID = "restart-device"
        val orderID = "restart-order"
        var connectCallCount = 0
        var rebuildCallCount = 0
        var pushCallCount = 0
        var resumedKnownCursor: String? = null

        val handler: (RecordedRequest) -> MockResponse = { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> {
                    connectCallCount++
                    if (connectCallCount == 1) {
                        mockResponse(connectJSON)
                    } else {
                        val body = Json.decodeFromString<JsonObject>(request.body.readUtf8())
                        val knownScopes = body["known_scopes"] as? JsonObject
                        resumedKnownCursor = knownScopes?.get(scopeID)
                            ?.jsonObject
                            ?.get("cursor")
                            ?.jsonPrimitive
                            ?.contentOrNull
                        mockResponse(connectResumeJSON)
                    }
                }
                path.endsWith("/sync/rebuild") -> {
                    rebuildCallCount++
                    mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                }
                path.endsWith("/sync/push") -> {
                    pushCallCount++
                    val body = Json.decodeFromString<JsonObject>(request.body.readUtf8())
                    val mutations = body["mutations"]!!.jsonArray
                    assertEquals(1, mutations.size)
                    val mutation = mutations.first().jsonObject
                    val pk = mutation["pk"]!!.jsonObject
                    assertEquals(orderID, pk["id"]!!.jsonPrimitive.content)
                    val columns = mutation["columns"] as? JsonObject ?: JsonObject(emptyMap())
                    val serverRow = buildJsonObject {
                        put("id", JsonPrimitive(orderID))
                        for ((key, value) in columns) put(key, value)
                        put("updated_at", JsonPrimitive("2026-01-01T15:00:00.000Z"))
                        put("deleted_at", JsonNull)
                    }
                    mockResponse(
                        """
                            {
                                "server_time": "2026-01-01T15:00:00.000Z",
                                "accepted": [{
                                    "mutation_id": ${mutation["mutation_id"]},
                                    "table": "orders",
                                    "pk": $pk,
                                    "status": "applied",
                                    "server_row": $serverRow,
                                    "server_version": "2026-01-01T15:00:00.000Z"
                                }],
                                "rejected": []
                            }
                        """.trimIndent()
                    )
                }
                path.endsWith("/sync/pull") -> mockResponse(scopePullJSON(cursor = "scope_cursor_2", checksum = "0"))
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        val (engine1, db1) = makeIntegrationEnv(dbName = dbName, clientID = clientID, handler = handler)
        engine1.start()

        db1.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            arrayOf(orderID, "Queued After First Start", "u1", "2026-01-01T10:00:00.000Z")
        )
        val tracker1 = ChangeTracker(db1)
        assertTrue(tracker1.hasPendingChanges())

        engine1.stop()
        db1.close()

        val (engine2, db2) = makeIntegrationEnv(dbName = dbName, clientID = clientID, handler = handler)
        try {
            engine2.start()

            assertEquals(1, rebuildCallCount)
            assertEquals(1, pushCallCount)
            assertEquals("scope_cursor_2", resumedKnownCursor)

            val tracker2 = ChangeTracker(db2)
            assertFalse(tracker2.hasPendingChanges())

            val row = db2.queryOne("SELECT updated_at FROM orders WHERE id = ?", arrayOf(orderID))
            assertEquals("2026-01-01T15:00:00.000Z", row?.get("updated_at"))
        } finally {
            engine2.stop()
            db2.close()
        }
    }

    @Test
    fun testScopeCursorAndChecksumSurviveRestartAndResumeWithoutRebuild() = runTest {
        val dbName = "resume_${UUID.randomUUID()}.sqlite"
        val clientID = "resume-device"
        var connectCallCount = 0
        var rebuildCallCount = 0
        var resumedKnownCursor: String? = null

        val handler: (RecordedRequest) -> MockResponse = { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> {
                    connectCallCount++
                    if (connectCallCount == 1) {
                        mockResponse(connectJSON)
                    } else {
                        val body = Json.decodeFromString<JsonObject>(request.body.readUtf8())
                        val knownScopes = body["known_scopes"] as? JsonObject
                        resumedKnownCursor = knownScopes?.get(scopeID)
                            ?.jsonObject
                            ?.get("cursor")
                            ?.jsonPrimitive
                            ?.contentOrNull
                        mockResponse(connectResumeJSON)
                    }
                }
                path.endsWith("/sync/rebuild") -> {
                    rebuildCallCount++
                    mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                }
                path.endsWith("/sync/pull") -> {
                    val cursor = if (connectCallCount == 1) "scope_cursor_2" else "scope_cursor_3"
                    mockResponse(scopePullJSON(cursor = cursor, checksum = "0"))
                }
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        val (engine1, db1) = makeIntegrationEnv(dbName = dbName, clientID = clientID, handler = handler)
        engine1.start()
        engine1.stop()
        db1.close()

        val (engine2, db2) = makeIntegrationEnv(dbName = dbName, clientID = clientID, handler = handler)
        try {
            engine2.start()

            assertEquals(1, rebuildCallCount)
            assertEquals("scope_cursor_2", resumedKnownCursor)

            val scope = db2.readTransaction { conn -> SynchroMeta.getScope(conn, scopeID) }
            assertEquals("scope_cursor_3", scope?.cursor)
            assertEquals("0", scope?.checksum)
            assertEquals(0, scope?.localChecksum)
        } finally {
            engine2.stop()
            db2.close()
        }
    }

    @Test
    fun testPartialRebuildStateSurvivesRestartAndCompletesCleanly() = runTest {
        val dbName = "rebuild_restart_${UUID.randomUUID()}.sqlite"
        val clientID = "rebuild-restart-device"
        var connectCallCount = 0
        var rebuildCallCount = 0
        var restartedKnownCursor: String? = null

        val rebuildRecordOne = """
            {
                "table": "orders",
                "pk": {"id": "w1"},
                "row": {"id": "w1", "ship_address": "Address 1", "user_id": "u1", "updated_at": "2026-01-01T12:00:00.000Z", "deleted_at": null},
                "row_checksum": 7,
                "server_version": "sv_1"
            }
        """.trimIndent()

        val rebuildRecordTwo = """
            {
                "table": "orders",
                "pk": {"id": "w2"},
                "row": {"id": "w2", "ship_address": "Address 2", "user_id": "u1", "updated_at": "2026-01-01T13:00:00.000Z", "deleted_at": null},
                "row_checksum": 9,
                "server_version": "sv_2"
            }
        """.trimIndent()

        val handler: (RecordedRequest) -> MockResponse = { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> {
                    connectCallCount++
                    if (connectCallCount == 1) {
                        mockResponse(connectJSON)
                    } else {
                        val body = Json.decodeFromString<JsonObject>(request.body.readUtf8())
                        val knownScopes = body["known_scopes"] as? JsonObject
                        restartedKnownCursor = knownScopes?.get(scopeID)
                            ?.jsonObject
                            ?.get("cursor")
                            ?.jsonPrimitive
                            ?.contentOrNull
                        mockResponse(connectResumeJSON)
                    }
                }
                path.endsWith("/sync/rebuild") -> {
                    rebuildCallCount++
                    when (rebuildCallCount) {
                        1 -> mockResponse(
                            rebuildJSON(
                                records = "[$rebuildRecordOne]",
                                cursor = "page_1",
                                hasMore = true
                            )
                        )
                        2 -> mockResponse("""{"error":"interrupted"}""", 500)
                        else -> mockResponse(
                            rebuildJSON(
                                records = "[$rebuildRecordOne,$rebuildRecordTwo]",
                                finalCursor = "scope_cursor_recovered",
                                checksum = "14"
                            )
                        )
                    }
                }
                path.endsWith("/sync/pull") -> mockResponse(scopePullJSON(cursor = "scope_cursor_recovered", checksum = "14"))
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        val (engine1, db1) = makeIntegrationEnv(dbName = dbName, clientID = clientID, handler = handler)
        try {
            engine1.start()
            fail("expected partial rebuild interruption to fail startup")
        } catch (_: Exception) {
        }

        val partialRows = db1.query("SELECT id FROM orders ORDER BY id")
        assertEquals(listOf("w1"), partialRows.map { it["id"] as String })

        val interruptedScope = db1.readTransaction { conn ->
            SynchroMeta.getScope(conn, scopeID)
        }
        assertNull(interruptedScope?.cursor)
        assertEquals(1L, interruptedScope?.generation)
        assertEquals(7, interruptedScope?.localChecksum)

        engine1.stop()
        db1.close()

        val (engine2, db2) = makeIntegrationEnv(dbName = dbName, clientID = clientID, handler = handler)
        try {
            engine2.start()

            assertNull(restartedKnownCursor)
            assertEquals(3, rebuildCallCount)

            val rows = db2.query("SELECT id FROM orders ORDER BY id")
            assertEquals(listOf("w1", "w2"), rows.map { it["id"] as String })

            val recoveredScope = db2.readTransaction { conn ->
                SynchroMeta.getScope(conn, scopeID)
            }
            assertEquals("scope_cursor_recovered", recoveredScope?.cursor)
            assertEquals("14", recoveredScope?.checksum)
            assertEquals(2L, recoveredScope?.generation)
            assertEquals(14, recoveredScope?.localChecksum)

            val tracker = ChangeTracker(db2)
            assertFalse(tracker.hasPendingChanges())
        } finally {
            engine2.stop()
            db2.close()
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
    fun testRetryablePushFailurePreservesQueueAcrossRestart() = runTest {
        val dbName = "retryable_push_restart_${UUID.randomUUID()}.sqlite"
        val clientID = "retryable-push-restart-device"
        var pushCallCount = 0
        var connectCallCount = 0
        var shouldFailNextPush = false

        val handler: (RecordedRequest) -> MockResponse = { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> {
                    connectCallCount++
                    mockResponse(if (connectCallCount == 1) connectJSON else connectResumeJSON)
                }
                path.endsWith("/sync/rebuild") -> mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                path.endsWith("/sync/push") -> {
                    pushCallCount++
                    if (shouldFailNextPush) {
                        shouldFailNextPush = false
                        MockResponse()
                            .setBody("""{"error":"temporarily unavailable"}""")
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

        val (engine1, db1) = makeIntegrationEnv(
            dbName = dbName,
            clientID = clientID,
            maxRetryAttempts = 0,
            handler = handler
        )
        val schemaManager1 = SchemaManager(db1)
        schemaManager1.reconcileLocalSchema(
            schemaVersion = 1,
            schemaHash = "test",
            tables = listOf(ordersLocalSchemaTable(includeNotes = false))
        )

        try {
            engine1.start()
            db1.execute(
                "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arrayOf("w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z")
            )
            shouldFailNextPush = true
            try {
                engine1.syncNow()
                fail("expected retryable push failure to abort the first sync")
            } catch (e: RetryableError) {
            }

            val tracker = ChangeTracker(db1)
            assertTrue(tracker.hasPendingChanges())
            val rejectedBeforeRestart = db1.readTransaction { conn -> SynchroMeta.listRejectedMutations(conn) }
            assertTrue(rejectedBeforeRestart.isEmpty())
        } finally {
            engine1.stop()
            db1.close()
        }

        val (engine2, db2) = makeIntegrationEnv(
            dbName = dbName,
            clientID = clientID,
            maxRetryAttempts = 0,
            handler = handler
        )
        val schemaManager2 = SchemaManager(db2)
        schemaManager2.reconcileLocalSchema(
            schemaVersion = 1,
            schemaHash = "test",
            tables = listOf(ordersLocalSchemaTable(includeNotes = false))
        )
        try {
            engine2.start()

            val tracker = ChangeTracker(db2)
            assertFalse(tracker.hasPendingChanges())
            assertEquals(2, pushCallCount)

            val localRow = db2.queryOne(
                "SELECT ship_address, updated_at FROM orders WHERE id = ?",
                arrayOf("w1")
            )
            assertEquals("123 Main St", localRow?.get("ship_address"))
            assertEquals("2026-01-01T14:00:00.000Z", localRow?.get("updated_at"))

            val rejectedAfterRestart = db2.readTransaction { conn -> SynchroMeta.listRejectedMutations(conn) }
            assertTrue(rejectedAfterRestart.isEmpty())
        } finally {
            engine2.stop()
            db2.close()
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
    fun testConnectUnsupportedFailsExplicitly() = runTest {
        val (engine, _) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> {
                    mockResponse(
                        """
                        {
                          "server_time":"2026-01-01T12:00:00.000Z",
                          "protocol_version":2,
                          "scope_set_version":1,
                          "schema":{"version":2,"hash":"unsupported_hash","action":"unsupported"},
                          "scopes":{"add":[],"remove":[]}
                        }
                        """.trimIndent()
                    )
                }
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        try {
            try {
                engine.start()
                fail("Expected unsupported connect schema action failure")
            } catch (error: SynchroError.InvalidResponse) {
                assertTrue(error.message?.contains("unsupported connect schema action") == true)
            }
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

    @Test
    fun testRejectedMutationsRemainInspectableAcrossRestart() = runTest {
        val dbName = "rejection_persistence_${UUID.randomUUID()}.sqlite"
        val clientID = "rejection-persistence-device"
        var connectCallCount = 0

        val handler: (RecordedRequest) -> MockResponse = { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> {
                    connectCallCount += 1
                    mockResponse(if (connectCallCount == 1) connectJSON else connectResumeJSON)
                }
                path.endsWith("/sync/rebuild") -> mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                path.endsWith("/sync/push") -> {
                    val body = Json.decodeFromString<JsonObject>(request.body.readUtf8())
                    val mutations = body["mutations"] as kotlinx.serialization.json.JsonArray
                    val rejected = mutations.map { change ->
                        val c = change as JsonObject
                        val pk = c["pk"] as JsonObject
                        """{"mutation_id":${c["mutation_id"]},"table":${c["table"]},"pk":$pk,"status":"rejected_terminal","code":"policy_rejected","message":"explicit rejection for inspection","server_row":null,"server_version":"sv::reject::1"}"""
                    }
                    mockResponse("""{"server_time":"2026-01-01T15:00:00.000Z","accepted":[],"rejected":[${rejected.joinToString(",")}]}""")
                }
                path.endsWith("/sync/pull") -> mockResponse(scopePullJSON(cursor = "scope_cursor_2"))
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        val (engine1, db1) = makeIntegrationEnv(dbName = dbName, clientID = clientID, handler = handler)
        try {
            engine1.start()
            db1.execute(
                "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arrayOf("w1", "Client Address", "u1", "2026-01-01T10:00:00.000Z")
            )
            engine1.syncNow()

            val rejectedBeforeRestart = db1.readTransaction { conn -> SynchroMeta.listRejectedMutations(conn) }
            assertEquals(1, rejectedBeforeRestart.size)
            assertTrue(rejectedBeforeRestart[0].mutationID.startsWith("orders:w1:"))
            assertEquals("policy_rejected", rejectedBeforeRestart[0].code)
        } finally {
            engine1.stop()
            db1.close()
        }

        val (engine2, db2) = makeIntegrationEnv(dbName = dbName, clientID = clientID, handler = handler)
        try {
            engine2.start()
            val rejectedAfterRestart = db2.readTransaction { conn -> SynchroMeta.listRejectedMutations(conn) }
            assertEquals(1, rejectedAfterRestart.size)
            assertTrue(rejectedAfterRestart[0].mutationID.startsWith("orders:w1:"))
            assertEquals("explicit rejection for inspection", rejectedAfterRestart[0].message)
            assertEquals("sv::reject::1", rejectedAfterRestart[0].serverVersion)
            db2.execute("DELETE FROM _synchro_rejected_mutations")
            val cleared = db2.query("SELECT mutation_id FROM _synchro_rejected_mutations")
            assertTrue(cleared.isEmpty())
        } finally {
            engine2.stop()
            db2.close()
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
        dbName: String = "synchro_test_${UUID.randomUUID()}.sqlite",
        clientID: String = "test-device",
        maxRetryAttempts: Int = 3,
        handler: (RecordedRequest) -> MockResponse
    ): Pair<SyncEngine, SynchroDatabase> {
        server?.shutdown()
        server = MockWebServer()
        server!!.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = handler(request)
        }
        server!!.start()

        val context = ApplicationProvider.getApplicationContext<Context>()
        val config = SynchroConfig(
            dbPath = dbName,
            serverURL = server!!.url("/").toString().trimEnd('/'),
            authProvider = { "token" },
            clientID = clientID,
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
            "protocol_version": 2,
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

    private val connectResumeJSON = """
        {
            "server_time": "2026-01-01T12:00:00.000Z",
            "protocol_version": 2,
            "scope_set_version": 1,
            "schema": {
                "version": 1,
                "hash": "test",
                "action": "none"
            },
            "scopes": {
                "add": [],
                "remove": []
            }
        }
    """.trimIndent()

    private val connectRebuildLocalJSON = """
        {
            "server_time": "2026-01-01T12:00:00.000Z",
            "protocol_version": 2,
            "scope_set_version": 2,
            "schema": {
                "version": 2,
                "hash": "test_v2",
                "action": "rebuild_local"
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
                            {"name":"notes","type":"string","nullable":true},
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
        checksum: String = "0"
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
        rebuild: String = "[]",
        checksum: String = "0"
    ): String = """
        {
            "changes": $changes,
            "scope_set_version": 1,
            "scope_cursors": {"$scopeID": "$cursor"},
            "scope_updates": {"add": [], "remove": []},
            "rebuild": $rebuild,
            "has_more": $hasMore,
            "checksums": {"$scopeID": "$checksum"}
        }
    """.trimIndent()

    private fun mockResponse(body: String, statusCode: Int = 200): MockResponse =
        MockResponse().setBody(body).setResponseCode(statusCode)

    private fun ordersLocalSchemaTable(includeNotes: Boolean): LocalSchemaTable {
        val columns = mutableListOf(
            LocalSchemaColumn(name = "id", logicalType = "string", nullable = false, isPrimaryKey = true),
            LocalSchemaColumn(name = "ship_address", logicalType = "string", nullable = true, isPrimaryKey = false),
            LocalSchemaColumn(name = "user_id", logicalType = "string", nullable = false, isPrimaryKey = false),
            LocalSchemaColumn(name = "updated_at", logicalType = "datetime", nullable = false, isPrimaryKey = false),
            LocalSchemaColumn(name = "deleted_at", logicalType = "datetime", nullable = true, isPrimaryKey = false),
        )
        if (includeNotes) {
            columns.add(
                3,
                LocalSchemaColumn(name = "notes", logicalType = "string", nullable = true, isPrimaryKey = false)
            )
        }
        return LocalSchemaTable(
            tableName = "orders",
            updatedAtColumn = "updated_at",
            deletedAtColumn = "deleted_at",
            composition = CompositionClass.SINGLE_SCOPE,
            primaryKey = listOf("id"),
            columns = columns,
        )
    }
}
