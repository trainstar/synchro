package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
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
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.coroutines.CoroutineContext

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
    fun testCallbackRegistrationAndCancellation() = runBlocking {
        val (engine, _) = makeSyncEngine()

        val statusUpdates = mutableListOf<String>()
        val cancellable1 = engine.onStatusChange { status ->
            when (status) {
                is SyncStatus.Error -> statusUpdates.add("error")
                is SyncStatus.Stopped -> statusUpdates.add("stopped")
                else -> Unit
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
    fun testMultipleCallbacksIndependentCancellation() = runBlocking {
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

    @Test
    fun testImmediateStopStartKeepsNewLifecycleOwnedAfterOldStartFailure() = runTest {
        var connectCallCount = 0
        var pullCallCount = 0
        val errorEntered = CountDownLatch(1)
        val releaseError = CountDownLatch(1)
        val callbackCallCount = AtomicInteger()

        val (engine, _) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> {
                    connectCallCount++
                    mockResponse(if (connectCallCount == 1) connectJSON else connectResumeJSON)
                }
                path.endsWith("/sync/rebuild") -> mockResponse(rebuildJSON(finalCursor = "scope_cursor_initial"))
                path.endsWith("/sync/pull") -> {
                    pullCallCount++
                    if (pullCallCount == 1) {
                        mockResponse("""{"error":"startup failure"}""", 500)
                    } else {
                        mockResponse(scopePullJSON(cursor = "scope_cursor_after_restart"))
                    }
                }
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        val callback = engine.onStatusChange { status ->
            if (status is SyncStatus.Error && callbackCallCount.compareAndSet(0, 1)) {
                errorEntered.countDown()
                releaseError.await(5, TimeUnit.SECONDS)
            }
        }
        val oldStart = CoroutineScope(Dispatchers.Default).launch {
            runCatching { engine.start() }
        }

        try {
            assertEquals("/sync/connect", server!!.takeRequest(2, TimeUnit.SECONDS)?.path)
            assertEquals("/sync/rebuild", server!!.takeRequest(2, TimeUnit.SECONDS)?.path)
            assertEquals("/sync/pull", server!!.takeRequest(2, TimeUnit.SECONDS)?.path)
            assertTrue("old startup must reach its failure callback", errorEntered.await(2, TimeUnit.SECONDS))

            engine.stop()
            releaseError.countDown()
            val newStart = CoroutineScope(Dispatchers.Default).launch {
                runCatching { engine.retry() }
            }
            newStart.join()
            assertTrue("new lifecycle must start after stop", newStart.isCompleted)

            oldStart.join()
            assertEquals(2, connectCallCount)
            assertEquals("new lifecycle must complete its initial pull", 2, pullCallCount)
        } finally {
            releaseError.countDown()
            callback.cancel()
            engine.stop()
        }
    }

    @Test
    fun stopBarrierRemainsActiveUntilStoppedStatusPublishes() = runTest {
        val (engine, _) = makeIntegrationEnv { request ->
            when {
                request.path.orEmpty().endsWith("/sync/connect") -> mockResponse(connectJSON)
                request.path.orEmpty().endsWith("/sync/rebuild") -> mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                request.path.orEmpty().endsWith("/sync/pull") -> mockResponse(scopePullJSON(cursor = "scope_cursor_2"))
                else -> mockResponse("{\"error\":\"unexpected\"}", 500)
            }
        }
        var startDuringStoppedPublication: Throwable? = null
        val callback = engine.onStatusChange { status ->
            if (status is SyncStatus.Stopped) {
                startDuringStoppedPublication = runCatching {
                    runBlocking { engine.start() }
                }.exceptionOrNull()
            }
        }
        try {
            engine.stop()
            assertTrue(startDuringStoppedPublication is SynchroError.NotStarted)

            callback.cancel()
            engine.start()
            assertTrue(engine.getSyncStatus() is SyncStatus.Ready)
        } finally {
            callback.cancel()
            engine.stop()
        }
    }

    @Test
    fun concurrentSecondStopWaitsForStoppedPublication() {
        val (engine, _) = makeSyncEngine()
        val publicationEntered = CountDownLatch(1)
        val releasePublication = CountDownLatch(1)
        val stoppedCallbacks = AtomicInteger()
        val registration = engine.onStatusChange { status ->
            if (status is SyncStatus.Stopped && stoppedCallbacks.getAndIncrement() == 0) {
                publicationEntered.countDown()
                releasePublication.await(5, TimeUnit.SECONDS)
            }
        }
        val firstStop = thread { runBlocking { engine.stop() } }

        try {
            assertTrue(publicationEntered.await(2, TimeUnit.SECONDS))
            val secondReturned = CountDownLatch(1)
            val secondStop = thread {
                runBlocking { engine.stop() }
                secondReturned.countDown()
            }

            assertFalse(
                "a concurrent stop returned before the first stopped publication completed",
                secondReturned.await(200, TimeUnit.MILLISECONDS),
            )
            releasePublication.countDown()
            firstStop.join(2_000)
            secondStop.join(2_000)
            assertFalse(firstStop.isAlive)
            assertFalse(secondStop.isAlive)
        } finally {
            releasePublication.countDown()
            firstStop.join(2_000)
            registration.cancel()
            runBlocking { engine.stop() }
        }
    }

    @Test
    fun lifecycleCallInsideApplicationTransactionFailsBeforeLockAcquisition() {
        val (engine, database) = makeSyncEngine()

        try {
            val error = assertThrows(IllegalStateException::class.java) {
                database.applicationTransaction {
                    runBlocking { engine.stop() }
                }
            }
            assertTrue(error.message.orEmpty().contains("application transaction"))
        } finally {
            runBlocking { engine.stop() }
        }
    }

    @Test
    fun stopClearsDebounceJobInstalledDuringSchedulingRace() {
        val (engine, _) = makeSyncEngine()
        val dispatchEntered = CountDownLatch(1)
        val releaseDispatch = CountDownLatch(1)
        val dispatchCount = AtomicInteger()
        val dispatcher = object : CoroutineDispatcher() {
            override fun dispatch(context: CoroutineContext, block: Runnable) {
                if (dispatchCount.getAndIncrement() == 0) {
                    dispatchEntered.countDown()
                    releaseDispatch.await(5, TimeUnit.SECONDS)
                }
                block.run()
            }
        }
        val scopeField = SyncEngine::class.java.getDeclaredField("scope").apply { isAccessible = true }
        val debounceField = SyncEngine::class.java.getDeclaredField("debounceJob").apply { isAccessible = true }
        val schedule = SyncEngine::class.java.getDeclaredMethod("scheduleDebouncedPush").apply { isAccessible = true }
        scopeField.set(engine, CoroutineScope(SupervisorJob() + dispatcher))
        val scheduler = thread { schedule.invoke(engine) }

        try {
            assertTrue(dispatchEntered.await(2, TimeUnit.SECONDS))
            val stopReturned = CountDownLatch(1)
            val stop = thread {
                runBlocking { engine.stop() }
                stopReturned.countDown()
            }
            stopReturned.await(200, TimeUnit.MILLISECONDS)
            releaseDispatch.countDown()
            scheduler.join(2_000)
            stop.join(2_000)

            assertFalse(scheduler.isAlive)
            assertFalse(stop.isAlive)
            assertNull(debounceField.get(engine))
        } finally {
            releaseDispatch.countDown()
            scheduler.join(2_000)
            runBlocking { engine.stop() }
        }
    }

    @Test
    fun lifecycleRejectsAnUnlistedSelfTransition() {
        val (engine, db) = makeSyncEngine()
        try {
            db.writeTransaction { connection ->
                SynchroMeta.transitionClientLifecycleState(connection, SyncLifecycleState.LOCAL_READY)
            }
            val error = assertThrows(SynchroError.InvalidStateTransition::class.java) {
                db.writeTransaction { connection ->
                    SynchroMeta.transitionClientLifecycleState(connection, SyncLifecycleState.LOCAL_READY)
                }
            }
            assertEquals(SyncLifecycleState.LOCAL_READY, error.from)
            assertEquals(SyncLifecycleState.LOCAL_READY, error.to)
        } finally {
            runBlocking { engine.stop() }
        }
    }

    @Test
    fun startupFailurePublishesAndPersistsOneBlockingError() = runTest {
        val (engine, db) = makeIntegrationEnv { mockResponse("{\"error\":\"fatal bootstrap\"}", 500) }
        val errors = AtomicInteger()
        val callback = engine.onStatusChange { status ->
            if (status is SyncStatus.Error) errors.incrementAndGet()
        }
        try {
            assertTrue(runCatching { engine.start() }.exceptionOrNull() is SynchroError.ServerError)
            assertEquals(1, errors.get())
            val durable = db.readTransaction { connection -> SynchroMeta.getClientState(connection) }
            assertEquals(SyncLifecycleState.ERROR, durable.lifecycleState)
            assertEquals(SyncFailureCode.SERVER_ERROR, durable.failure?.code)
            assertEquals(SyncOperationKind.CONNECTING, durable.failure?.operation)
            assertEquals("The server rejected the sync operation.", durable.failure?.message)
            assertEquals(SyncRecoveryAction.RETRY, durable.failure?.recoveryAction)
            assertEquals(mapOf("http_status" to "500"), durable.failure?.metadata)
        } finally {
            callback.cancel()
            engine.stop()
        }
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
            assertEquals(emptyScopeChecksumJSON(), scopes[0].checksum)

            val tables = db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'")
            assertEquals(1, tables.size)

            val triggers = db.query(
                "SELECT name, sql FROM sqlite_master WHERE type='trigger' AND name LIKE '_synchro_cdc_%orders' ORDER BY name",
            )
            assertEquals(
                setOf(
                    "_synchro_cdc_delete_orders",
                    "_synchro_cdc_insert_orders",
                    "_synchro_cdc_pk_guard_orders",
                    "_synchro_cdc_update_orders",
                ),
                triggers.map { it.getValue("name") }.toSet(),
            )
            val guardSQL = triggers.single { it["name"] == "_synchro_cdc_pk_guard_orders" }["sql"] as String
            assertTrue(guardSQL.contains("BEFORE UPDATE OF \"id\""))
            assertTrue(guardSQL.contains("RAISE(ABORT, 'synced primary key cannot change')"))
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
                    mockResponse(scopePullJSON(cursor = "scope_cursor_2"))
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
            installTestSchema(
                db,
                schemaVersion = 1,
                schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
                tables = listOf(ordersLocalSchemaTable(includeNotes = false))
            )
            db.writeSyncLockedTransaction { conn ->
                SynchroMeta.upsertScope(
                    conn,
                    scopeId = scopeID,
                    cursor = "scope_cursor_1",
                    checksum = emptyScopeChecksumJSON()
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
                    mockResponse(scopePullJSON(cursor = cursor))
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
            installTestSchema(
                db,
                schemaVersion = 1,
                schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
                tables = listOf(ordersLocalSchemaTable(includeNotes = false))
            )
            db.writeTransaction { conn ->
                SynchroMeta.upsertScope(
                    conn,
                    scopeId = scopeID,
                    cursor = "scope_cursor_1",
                    checksum = emptyScopeChecksumJSON()
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
                        acceptedPushOutcomeJSON(
                            mutation = change.jsonObject,
                            serverVersion = "opaque_server_version_after_push",
                        )
                    }
                    mockResponse("""{"batch_id":${body["batch_id"]},"server_time":"2026-01-01T14:00:00.000Z","accepted":[${accepted.joinToString(",")}],"rejected":[]}""")
                }
                path.endsWith("/sync/pull") -> {
                    callLog.add("pull")
                    mockResponse(scopePullJSON(cursor = "scope_cursor_2"))
                }
                path.endsWith("/sync/rebuild") -> {
                    callLog.add("rebuild")
                    mockResponse("""{"error":"unexpected rebuild"}""", 500)
                }
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        try {
            installTestSchema(
                db,
                schemaVersion = 1,
                schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
                tables = listOf(ordersLocalSchemaTable(includeNotes = false))
            )
            db.writeTransaction { conn ->
                SynchroMeta.upsertScope(
                    conn,
                    scopeId = scopeID,
                    cursor = "scope_cursor_1",
                    checksum = emptyScopeChecksumJSON()
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
    fun testDebouncedPushSharesCycleGateWithExplicitSync() = runTest {
        val pushStarted = CountDownLatch(1)
        val pushCount = AtomicInteger()
        val (engine, db) = makeIntegrationEnv(pushDebounce = 0.01) { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> mockResponse(connectResumeJSON)
                path.endsWith("/sync/push") -> {
                    pushCount.incrementAndGet()
                    pushStarted.countDown()
                    Thread.sleep(100)
                    val body = Json.decodeFromString<JsonObject>(request.body.readUtf8())
                    val accepted = body.getValue("mutations").jsonArray.map { mutation ->
                        acceptedPushOutcomeJSON(
                            mutation = mutation.jsonObject,
                            serverVersion = "debounced-server-version",
                        )
                    }
                    mockResponse(
                        """{"batch_id":${body["batch_id"]},"server_time":"2026-01-01T14:00:00.000Z","accepted":[${accepted.joinToString(",")}],"rejected":[]}""",
                    )
                }
                path.endsWith("/sync/pull") -> mockResponse(scopePullJSON(cursor = "scope_cursor_debounced"))
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        try {
            installTestSchema(
                db,
                schemaVersion = 1,
                schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
                tables = listOf(ordersLocalSchemaTable(includeNotes = false)),
            )
            db.writeTransaction { connection ->
                SynchroMeta.upsertScope(
                    connection,
                    scopeId = scopeID,
                    cursor = "scope_cursor_1",
                    checksum = emptyScopeChecksumJSON(),
                )
            }

            engine.start()
            db.applicationExecute(
                "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arrayOf("w1", "debounced", "u1", "2026-01-01T10:00:00.000Z"),
            )
            assertTrue(pushStarted.await(1, TimeUnit.SECONDS))
            engine.syncNow()

            val tracker = ChangeTracker(db)
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2)
            while (tracker.hasPendingChanges() && System.nanoTime() < deadline) {
                Thread.sleep(20)
            }
            assertFalse(tracker.hasPendingChanges())
            assertEquals(1, pushCount.get())
        } finally {
            engine.stop()
        }
    }

    @Test
    fun testConnectRebuildLocalReconcilesSchemaAndRebuildsExistingScope() = runTest {
        val callLog = mutableListOf<String>()
        val rebuiltRecord = protocolRecord(
            schema = protocolOrdersSchema(includeNotes = true),
            id = "w1",
            shipAddress = "Rebuilt Address",
            userID = "u1",
            notes = "schema rebuild local",
            updatedAt = "2026-01-01T12:00:00.000000Z",
            serverVersion = "opaque_server_version_rebuild",
            schemaHash = connectRebuildLocalSchemaHash,
        )
        val rebuiltChecksum = protocolScopeChecksum(rebuiltRecord)

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
                            records = "[${protocolRebuildRecordJSON(rebuiltRecord)}]",
                            finalCursor = "scope_cursor_rebuilt",
                            checksum = rebuiltChecksum
                        )
                    )
                }
                path.endsWith("/sync/pull") -> {
                    callLog.add("pull")
                    mockResponse(scopePullJSON(
                        cursor = "scope_cursor_after_rebuild",
                        scopeSetVersion = 2,
                        checksum = rebuiltChecksum,
                    ))
                }
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        try {
            installTestSchema(
                db,
                schemaVersion = 1,
                schemaHash = "old_hash",
                tables = listOf(ordersLocalSchemaTable(includeNotes = false))
            )
            db.writeSyncLockedTransaction { conn ->
                SynchroMeta.upsertScope(
                    conn,
                    scopeId = scopeID,
                    cursor = "scope_cursor_old",
                    checksum = emptyScopeChecksumJSON()
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
            assertEquals(checksumJSON(rebuiltChecksum), scope?.checksum)
            assertEquals(scope?.checksum, scope?.localChecksum)

            val schemaVersion = db.readTransaction { conn ->
                SynchroMeta.getInt64(conn, MetaKey.SCHEMA_VERSION)
            }
            assertEquals(2L, schemaVersion)
        } finally {
            engine.stop()
        }
    }

    @Test
    fun testConnectSchemaAndBindingInstallationRollsBackTogether() = runTest {
        val (engine, db) = makeIntegrationEnv { mockResponse("{}", 500) }
        installTestSchema(
            db,
            schemaVersion = 1,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            tables = listOf(ordersLocalSchemaTable(includeNotes = false)),
        )
        db.writeTransaction { connection ->
            SynchroMeta.upsertScope(
                connection,
                scopeId = scopeID,
                cursor = "scope_cursor_old",
                checksum = null,
            )
            SynchroMeta.setInt64(connection, MetaKey.SCOPE_SET_VERSION, 1)
            SynchroMeta.setInt64(connection, MetaKey.CLIENT_GENERATION, 1)
            connection.execSQL(
                """
                CREATE TRIGGER fail_connect_binding_install
                BEFORE INSERT ON _synchro_scopes
                WHEN NEW.scope_id = 'orders:added'
                BEGIN
                    SELECT RAISE(ABORT, 'forced connect binding failure');
                END
                """.trimIndent()
            )
        }

        val response = Json { ignoreUnknownKeys = true }
            .decodeFromString<ConnectResponse>(connectRebuildLocalJSON)
            .copy(
                scopes = ScopeAssignmentDelta(
                    add = listOf(ScopeAssignment("orders:added", null)),
                    remove = emptyList(),
                )
            )

        try {
            engine.installConnectResponse(response)
            fail("expected connect installation to fail")
        } catch (_: Exception) {
        }

        val columnNames = db.readTransaction { connection ->
            buildSet {
                connection.rawQuery("PRAGMA table_info(orders)", null).use { cursor ->
                    val nameIndex = cursor.getColumnIndex("name")
                    while (cursor.moveToNext()) add(cursor.getString(nameIndex))
                }
            }
        }
        assertFalse(columnNames.contains("notes"))
        db.readTransaction { connection ->
            assertNull(SynchroMeta.get(connection, MetaKey.CLIENT_ID))
            assertEquals(1L, SynchroMeta.getInt64(connection, MetaKey.SCHEMA_VERSION))
            assertEquals(PROTOCOL_TEST_SCHEMA_HASH, SynchroMeta.get(connection, MetaKey.SCHEMA_HASH))
            assertEquals(1L, SynchroMeta.getInt64(connection, MetaKey.SCOPE_SET_VERSION))
            assertEquals(1L, SynchroMeta.getInt64(connection, MetaKey.CLIENT_GENERATION))
            assertEquals("scope_cursor_old", SynchroMeta.getScope(connection, scopeID)?.cursor)
            assertNull(SynchroMeta.getScope(connection, "orders:added"))
        }
    }

    @Test
    fun firstSeedConnectSendsInstalledStateAndConvertsEveryReceipt() = runTest {
        val secondScopeID = "orders:portable-shared"
        var connectBody: JsonObject? = null
        val (engine, database) = makeIntegrationEnv { request ->
            when {
                request.path!!.endsWith("/sync/connect") -> {
                    connectBody = Json.parseToJsonElement(request.body.readUtf8()).jsonObject
                    mockResponse(
                        """
                        {
                            "server_time":"2026-01-01T12:00:00.000000Z",
                            "protocol_version":3,
                            "client_generation":1,
                            "scope_set_version":7,
                            "schema":{"version":1,"hash":"$PROTOCOL_TEST_SCHEMA_HASH","action":"none"},
                            "scopes":{"add":[],"remove":[]},
                            "scope_cursor_updates":{
                                "$scopeID":"seed-runtime-cursor-1",
                                "$secondScopeID":"seed-runtime-cursor-2"
                            }
                        }
                        """.trimIndent(),
                    )
                }
                request.path!!.endsWith("/sync/pull") -> mockResponse(
                    """
                    {
                        "changes":[],
                        "scope_set_version":7,
                        "scope_cursors":{
                            "$scopeID":"seed-runtime-cursor-1-next",
                            "$secondScopeID":"seed-runtime-cursor-2-next"
                        },
                        "scope_updates":{"add":[],"remove":[]},
                        "rebuild":[],
                        "has_more":false,
                        "checksums":{
                            "$scopeID":${checksumJSON(protocolEmptyScopeChecksum(scopeID))},
                            "$secondScopeID":${checksumJSON(protocolEmptyScopeChecksum(secondScopeID))}
                        }
                    }
                    """.trimIndent(),
                )
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }
        installTestSchema(
            database,
            schemaVersion = 1,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            tables = listOf(protocolOrdersSchema()),
        )
        database.writeTransaction { db ->
            installEmptySeedScope(db, scopeID, "receipt-one")
            installEmptySeedScope(db, secondScopeID, "receipt-two")
            SynchroMeta.setInt64(db, MetaKey.SCOPE_SET_VERSION, 7)
            SynchroMeta.set(db, MetaKey.SNAPSHOT_COMPLETE, "1")
        }

        try {
            engine.start()

            val request = requireNotNull(connectBody)
            assertNull(request["client_generation"])
            assertEquals(1L, request.getValue("schema").jsonObject.getValue("version").jsonPrimitive.content.toLong())
            assertEquals(
                PROTOCOL_TEST_SCHEMA_HASH,
                request.getValue("schema").jsonObject.getValue("hash").jsonPrimitive.content,
            )
            assertEquals(7L, request.getValue("scope_set_version").jsonPrimitive.content.toLong())
            val knownScopes = request.getValue("known_scopes").jsonObject
            assertEquals(setOf(scopeID, secondScopeID), knownScopes.keys)
            assertTrue(knownScopes.getValue(scopeID).jsonObject.getValue("cursor") is JsonNull)
            assertTrue(knownScopes.getValue(secondScopeID).jsonObject.getValue("cursor") is JsonNull)
            assertEquals(
                mapOf(scopeID to "receipt-one", secondScopeID to "receipt-two"),
                request.getValue("seed_receipts").jsonObject.mapValues { it.value.jsonPrimitive.content },
            )

            database.readTransaction { db ->
                assertEquals("test-device", SynchroMeta.get(db, MetaKey.CLIENT_ID))
                assertTrue(SynchroMeta.getSeedReceipts(db).isEmpty())
                assertEquals("seed-runtime-cursor-1-next", SynchroMeta.getScope(db, scopeID)?.cursor)
                assertEquals("seed-runtime-cursor-2-next", SynchroMeta.getScope(db, secondScopeID)?.cursor)
            }
        } finally {
            engine.stop()
        }
    }

    @Test
    fun unresolvedSeedReceiptKeepsDatabaseUnboundAndRetainsReceipt() = runTest {
        val (engine, database) = makeIntegrationEnv { request ->
            if (request.path!!.endsWith("/sync/connect")) {
                mockResponse(connectNoneJSON(1, 1, PROTOCOL_TEST_SCHEMA_HASH))
            } else {
                mockResponse("""{"error":"network must stop after connect"}""", 500)
            }
        }
        installTestSchema(
            database,
            schemaVersion = 1,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            tables = listOf(protocolOrdersSchema()),
        )
        database.writeTransaction { db ->
            installEmptySeedScope(db, scopeID, "receipt-unresolved")
            SynchroMeta.setInt64(db, MetaKey.SCOPE_SET_VERSION, 1)
            SynchroMeta.set(db, MetaKey.SNAPSHOT_COMPLETE, "1")
        }

        val failure = runCatching { engine.start() }.exceptionOrNull()
        assertNotNull(failure)
        database.readTransaction { db ->
            assertNull(SynchroMeta.get(db, MetaKey.CLIENT_ID))
            assertEquals(mapOf(scopeID to "receipt-unresolved"), SynchroMeta.getSeedReceipts(db))
            assertEquals(0L, SynchroMeta.getInt64(db, MetaKey.CLIENT_GENERATION))
            assertNull(SynchroMeta.getScope(db, scopeID)?.cursor)
        }
    }

    @Test
    fun seedReceiptResolutionsHandleValidStaleAndUnassignedScopesWithProtectedIntent() = runTest {
        val validScope = "orders:seed-valid"
        val staleScope = "orders:seed-stale"
        val removedScope = "orders:seed-removed"
        val (engine, database) = makeIntegrationEnv { mockResponse("{}", 500) }
        val schema = protocolOrdersSchema()
        installTestSchema(
            database,
            schemaVersion = 1,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            tables = listOf(schema),
        )
        val validChecksum = installSeedScope(
            database,
            schema,
            validScope,
            "receipt-valid",
            listOf("valid-row" to "Valid seed"),
        )
        installSeedScope(
            database,
            schema,
            staleScope,
            "receipt-stale",
            listOf("stale-row" to "Stale seed"),
        )
        installSeedScope(
            database,
            schema,
            removedScope,
            "receipt-removed",
            listOf("removed-protected" to "Protected seed", "removed-cache" to "Unchanged cache"),
        )
        database.writeTransaction { db ->
            SynchroMeta.setInt64(db, MetaKey.SCOPE_SET_VERSION, 7)
            SynchroMeta.set(db, MetaKey.SNAPSHOT_COMPLETE, "1")
        }
        database.execute("UPDATE orders SET ship_address = ? WHERE id = ?", arrayOf("Valid local intent", "valid-row"))
        database.execute("UPDATE orders SET ship_address = ? WHERE id = ?", arrayOf("Stale local intent", "stale-row"))
        database.execute("UPDATE orders SET ship_address = ? WHERE id = ?", arrayOf("Removed local intent", "removed-protected"))

        val response = ConnectResponse(
            serverTime = "2026-01-01T12:00:00.000000Z",
            protocolVersion = 3,
            clientGeneration = 1,
            scopeSetVersion = 8,
            schema = SchemaDescriptor(1, PROTOCOL_TEST_SCHEMA_HASH, SchemaAction.NONE),
            scopes = ScopeAssignmentDelta(emptyList(), listOf(removedScope)),
            scopeCursorUpdates = mapOf(validScope to "valid-runtime-cursor", staleScope to null),
        )
        engine.installConnectResponse(response)

        database.readTransaction { db ->
            assertEquals("test-device", SynchroMeta.get(db, MetaKey.CLIENT_ID))
            assertTrue(SynchroMeta.getSeedReceipts(db).isEmpty())
            assertEquals("valid-runtime-cursor", SynchroMeta.getScope(db, validScope)?.cursor)
            assertNull(SynchroMeta.getScope(db, staleScope)?.cursor)
            assertEquals(1L, SynchroMeta.getScope(db, staleScope)?.generation)
            assertNull(SynchroMeta.getScope(db, removedScope))
        }
        assertEquals("Valid local intent", database.queryOne("SELECT ship_address FROM orders WHERE id = 'valid-row'")?.get("ship_address"))
        assertEquals("Stale local intent", database.queryOne("SELECT ship_address FROM orders WHERE id = 'stale-row'")?.get("ship_address"))
        assertEquals("Removed local intent", database.queryOne("SELECT ship_address FROM orders WHERE id = 'removed-protected'")?.get("ship_address"))
        assertNull(database.queryOne("SELECT id FROM orders WHERE id = 'removed-cache'"))
        assertEquals(3, database.query("SELECT mutation_id FROM _synchro_pending_changes").size)
        assertEquals(
            validChecksum,
            database.queryOne(
                "SELECT checksum FROM _synchro_scope_rows WHERE scope_id = ? AND record_id = ?",
                arrayOf(validScope, "valid-row"),
            )?.get("checksum"),
        )
    }

    @Test
    fun firstStartedClientIDBindsDatabaseBeforeLaterWork() = runTest {
        val dbName = "client_binding_${UUID.randomUUID()}.sqlite"
        val firstClientID = "first-client"
        val (firstEngine, firstDatabase) = makeIntegrationEnv(
            dbName = dbName,
            clientID = firstClientID,
        ) { request ->
            when {
                request.path!!.endsWith("/sync/connect") -> mockResponse(connectJSON)
                request.path!!.endsWith("/sync/rebuild") -> mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                request.path!!.endsWith("/sync/pull") -> mockResponse(scopePullJSON(cursor = "scope_cursor_2"))
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }
        try {
            firstEngine.start()
            assertEquals(
                firstClientID,
                firstDatabase.readTransaction { SynchroMeta.get(it, MetaKey.CLIENT_ID) },
            )
        } finally {
            firstEngine.stop()
            firstDatabase.close()
        }

        val (secondEngine, secondDatabase) = makeIntegrationEnv(
            dbName = dbName,
            clientID = "different-client",
        ) { mockResponse("""{"error":"network must not run"}""", 500) }
        try {
            val failure = runCatching { secondEngine.start() }.exceptionOrNull()
            assertTrue(failure is SynchroError.InvalidResponse)
            assertEquals(0, server!!.requestCount)
            assertEquals(
                firstClientID,
                secondDatabase.readTransaction { SynchroMeta.get(it, MetaKey.CLIENT_ID) },
            )
        } finally {
            secondEngine.stop()
            secondDatabase.close()
        }
    }

    @Test
    fun connectInstallAndMatchingBackoffResolveInOneTransaction() = runTest {
        val (engine, database) = makeIntegrationEnv { mockResponse("{}", 500) }
        installTestSchema(
            database,
            schemaVersion = 1,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            tables = listOf(protocolOrdersSchema()),
        )
        database.writeTransaction { connection ->
            SynchroMeta.setInt64(connection, MetaKey.CLIENT_GENERATION, 1)
            SynchroMeta.setInt64(connection, MetaKey.SCOPE_SET_VERSION, 1)
        }
        val request = ConnectRequest(
            clientID = "test-device",
            clientGeneration = 1,
            platform = "android",
            appVersion = "1.0.0",
            protocolVersion = 3,
            schema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
            scopeSetVersion = 1,
            knownScopes = emptyMap(),
        )
        val requestJSON = Json { encodeDefaults = true }.encodeToString(request)
        val response = Json.decodeFromString<ConnectResponse>(
            connectNoneJSON(2, 1, PROTOCOL_TEST_SCHEMA_HASH),
        )
        installDurableBackoff(database, RetryOperation.CONNECTING, requestJSON)
        database.execute(
            """
            CREATE TRIGGER fail_connect_backoff_resolution
            BEFORE DELETE ON _synchro_backoff
            BEGIN
                SELECT RAISE(ABORT, 'forced backoff resolution failure');
            END
            """.trimIndent(),
        )

        assertThrows(android.database.sqlite.SQLiteException::class.java) {
            kotlinx.coroutines.runBlocking {
                engine.installConnectResponse(response, requestJSON)
            }
        }
        assertEquals(1L, database.readTransaction { SynchroMeta.getInt64(it, MetaKey.CLIENT_GENERATION) })
        assertNotNull(DurableBackoffStore.load(database))

        database.execute("DROP TRIGGER fail_connect_backoff_resolution")
        engine.installConnectResponse(response, requestJSON)

        assertEquals(2L, database.readTransaction { SynchroMeta.getInt64(it, MetaKey.CLIENT_GENERATION) })
        assertNull(DurableBackoffStore.load(database))
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
                        acceptedPushOutcomeJSON(
                            mutation = change as JsonObject,
                            serverVersion = "2026-01-01T14:00:00.000000Z",
                        )
                    }
                    mockResponse("""{"batch_id":${body["batch_id"]},"server_time":"2026-01-01T14:00:00.000Z","accepted":[${accepted.joinToString(",")}],"rejected":[]}""")
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
            assertEquals("2026-01-01T14:00:00.000000Z", row?.get("updated_at"))
        } finally {
            engine.stop()
        }
    }

    @Test
    fun testPullAppliesServerRecord() = runTest {
        val serverRecord = protocolRecord(
            id = "w1",
            shipAddress = "Server Address",
            userID = "u1",
            updatedAt = "2026-01-01T12:00:00.000000Z",
            serverVersion = "sv_1",
        )
        val (engine, db) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> mockResponse(connectJSON)
                path.endsWith("/sync/rebuild") -> mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                path.endsWith("/sync/pull") -> {
                    mockResponse(scopePullJSON(
                        cursor = "scope_cursor_2",
                        changes = "[${protocolChangeRecordJSON(serverRecord)}]",
                        checksum = protocolScopeChecksum(serverRecord),
                    ))
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
        val seededRecord = protocolRecord(
            id = "w1",
            shipAddress = "Seeded",
            userID = "u1",
            updatedAt = "2026-01-01T12:00:00.000000Z",
            serverVersion = "sv_1",
        )

        val (engine, db) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> mockResponse(connectJSON)
                path.endsWith("/sync/rebuild") -> mockResponse(
                    rebuildJSON(
                        records = "[${protocolRebuildRecordJSON(seededRecord)}]",
                        finalCursor = "scope_cursor_1",
                        checksum = protocolScopeChecksum(seededRecord)
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
                                    "scope_cursors": {},
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
        val firstRecord = protocolRecord(
            id = "w1",
            shipAddress = "Address 1",
            userID = "u1",
            updatedAt = "2026-01-01T12:00:00.000000Z",
            serverVersion = "sv_1",
        )
        val secondRecord = protocolRecord(
            id = "w2",
            shipAddress = "Address 2",
            userID = "u1",
            updatedAt = "2026-01-01T13:00:00.000000Z",
            serverVersion = "sv_2",
        )

        val (engine, db) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> mockResponse(connectJSON)
                path.endsWith("/sync/rebuild") -> mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                path.endsWith("/sync/pull") -> {
                    pullCallCount++
                    if (pullCallCount == 1) {
                        mockResponse(scopePullJSON(
                            cursor = "scope_cursor_mid",
                            changes = "[${protocolChangeRecordJSON(firstRecord)}]",
                            hasMore = true,
                        ))
                    } else {
                        mockResponse(scopePullJSON(
                            cursor = "scope_cursor_2",
                            changes = "[${protocolChangeRecordJSON(secondRecord)}]",
                            checksum = protocolScopeChecksum(firstRecord, secondRecord),
                        ))
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

        val scopeRecord = protocolRecord(
            id = "w1",
            shipAddress = "Recovered Address",
            userID = "u1",
            updatedAt = "2026-01-01T12:00:00.000000Z",
            serverVersion = "sv_1",
        )
        val scopeChecksum = protocolScopeChecksum(scopeRecord)

        val (engine, db) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> mockResponse(connectJSON)
                path.endsWith("/sync/rebuild") -> {
                    rebuildCallCount++
                    if (rebuildCallCount == 1) {
                        mockResponse(
                            rebuildJSON(
                                records = "[${protocolRebuildRecordJSON(scopeRecord)}]",
                                finalCursor = "scope_cursor_1",
                                checksum = scopeChecksum
                            )
                        )
                    } else {
                        mockResponse(
                            rebuildJSON(
                                records = "[${protocolRebuildRecordJSON(scopeRecord)}]",
                                finalCursor = "scope_cursor_rebuilt",
                                checksum = scopeChecksum
                            )
                        )
                    }
                }
                path.endsWith("/sync/pull") -> {
                    pullCallCount++
                    if (pullCallCount == 1) {
                        mockResponse(scopePullJSON(cursor = "scope_cursor_2", checksum = scopeChecksum))
                    } else {
                        mockResponse(scopePullJSON(
                            cursor = "scope_cursor_2",
                            checksum = scopeChecksum,
                        ))
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
            assertEquals(checksumJSON(scopeChecksum), scope?.checksum)
            assertEquals(scope?.checksum, scope?.localChecksum)
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
                    assertEquals(orderID, pk["field-id"]!!.jsonPrimitive.content)
                    mockResponse(
                        """
                            {
                                "batch_id": ${body["batch_id"]},
                                "server_time": "2026-01-01T15:00:00.000Z",
                                "accepted": [${acceptedPushOutcomeJSON(
                                    mutation,
                                    "2026-01-01T15:00:00.000000Z",
                                    serverUpdatedAt = "2026-01-01T15:00:00.000000Z",
                                )}],
                                "rejected": []
                            }
                        """.trimIndent()
                    )
                }
                path.endsWith("/sync/pull") -> mockResponse(scopePullJSON(cursor = "scope_cursor_2"))
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
            assertEquals("2026-01-01T15:00:00.000000Z", row?.get("updated_at"))
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
                    mockResponse(scopePullJSON(cursor = cursor))
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
            assertEquals(emptyScopeChecksumJSON(), scope?.checksum)
            assertEquals(scope?.checksum, scope?.localChecksum)
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

        val rebuildRecordOne = protocolRecord(
            id = "w1",
            shipAddress = "Address 1",
            userID = "u1",
            updatedAt = "2026-01-01T12:00:00.000000Z",
            serverVersion = "sv_1",
        )
        val rebuildRecordTwo = protocolRecord(
            id = "w2",
            shipAddress = "Address 2",
            userID = "u1",
            updatedAt = "2026-01-01T13:00:00.000000Z",
            serverVersion = "sv_2",
        )
        val recoveredChecksum = protocolScopeChecksum(rebuildRecordOne, rebuildRecordTwo)

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
                                records = "[${protocolRebuildRecordJSON(rebuildRecordOne)}]",
                                cursor = "page_1",
                                hasMore = true
                            )
                        )
                        2 -> mockResponse("""{"error":"interrupted"}""", 500)
                        else -> mockResponse(
                            rebuildJSON(
                                records = "[${protocolRebuildRecordJSON(rebuildRecordOne)},${protocolRebuildRecordJSON(rebuildRecordTwo)}]",
                                finalCursor = "scope_cursor_recovered",
                                checksum = recoveredChecksum
                            )
                        )
                    }
                }
                path.endsWith("/sync/pull") -> mockResponse(scopePullJSON(cursor = "scope_cursor_recovered", checksum = recoveredChecksum))
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
        assertEquals("", interruptedScope?.localChecksum)

        engine1.stop()
        db1.close()

        val (engine2, db2) = makeIntegrationEnv(dbName = dbName, clientID = clientID, handler = handler)
        try {
            engine2.retry()

            assertNull(restartedKnownCursor)
            assertEquals(3, rebuildCallCount)

            val rows = db2.query("SELECT id FROM orders ORDER BY id")
            assertEquals(listOf("w1", "w2"), rows.map { it["id"] as String })

            val recoveredScope = db2.readTransaction { conn ->
                SynchroMeta.getScope(conn, scopeID)
            }
            assertEquals("scope_cursor_recovered", recoveredScope?.cursor)
            assertEquals(checksumJSON(recoveredChecksum), recoveredScope?.checksum)
            assertEquals(1L, recoveredScope?.generation)
            assertEquals(recoveredScope?.checksum, recoveredScope?.localChecksum)

            val tracker = ChangeTracker(db2)
            assertFalse(tracker.hasPendingChanges())
        } finally {
            engine2.stop()
            db2.close()
        }
    }

    @Test
    fun testFinalRebuildReceiptFinalizesAfterRestartWithoutRequestingPage() = runTest {
        val dbName = "rebuild_finality_restart_${UUID.randomUUID()}.sqlite"
        val clientID = "rebuild-finality-device"
        val finalCursor = "scope_cursor_final"
        val rebuiltRecord = protocolRecord(
            id = "w1",
            shipAddress = "Applied before finality",
            userID = "u1",
            updatedAt = "2026-01-01T12:00:00.000000Z",
            serverVersion = "server-version-final",
        )
        val rebuiltChecksum = protocolScopeChecksum(rebuiltRecord)
        val responseJSON = rebuildJSON(
            records = "[${protocolRebuildRecordJSON(rebuiltRecord)}]",
            finalCursor = finalCursor,
            checksum = rebuiltChecksum,
        )
        val response = Json { ignoreUnknownKeys = true }.decodeFromString<RebuildResponse>(responseJSON)

        val (engine1, db1) = makeIntegrationEnv(dbName = dbName, clientID = clientID) { request ->
            mockResponse("""{"error":"unexpected: ${request.path}"}""", 500)
        }
        installTestSchema(
            db1,
            schemaVersion = 1,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            tables = listOf(protocolOrdersSchema()),
        )
        db1.writeTransaction { connection ->
            SynchroMeta.upsertScope(connection, scopeID, cursor = null, checksum = null)
            SynchroMeta.setInt64(connection, MetaKey.CLIENT_GENERATION, 1)
            SynchroMeta.setInt64(connection, MetaKey.SCOPE_SET_VERSION, 1)
        }
        val processor = PullProcessor(db1)
        val attempt = processor.beginScopeRebuild(
            scopeID,
            clientGeneration = 1,
            schemaVersion = 1,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            pageLimit = 100,
        )
        val request = RebuildRequest(
            clientID = clientID,
            clientGeneration = attempt.clientGeneration,
            schema = SchemaRef(attempt.schemaVersion, attempt.schemaHash),
            scope = scopeID,
            rebuildID = attempt.rebuildID,
            cursor = attempt.cursor,
            limit = attempt.pageLimit,
        )
        processor.applyScopeRebuildPage(
            attempt,
            request,
            rebuildRequestJSON(request),
            response,
            responseJSON,
            listOf(protocolOrdersSchema()),
        )
        assertNull(db1.readTransaction { connection -> SynchroMeta.getScope(connection, scopeID)?.cursor })
        assertEquals(1, db1.query("SELECT * FROM _synchro_rebuild_page_receipts").size)
        engine1.stop()
        db1.close()

        var rebuildRequestCount = 0
        val (engine2, db2) = makeIntegrationEnv(dbName = dbName, clientID = clientID) { recorded ->
            when {
                recorded.path!!.endsWith("/sync/connect") -> mockResponse(connectResumeJSON)
                recorded.path!!.endsWith("/sync/rebuild") -> {
                    rebuildRequestCount++
                    mockResponse("""{"error":"rebuild page must not be requested"}""", 500)
                }
                recorded.path!!.endsWith("/sync/pull") -> mockResponse(
                    scopePullJSON(finalCursor, checksum = rebuiltChecksum),
                )
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }
        try {
            engine2.start()

            assertEquals(0, rebuildRequestCount)
            assertEquals(
                "Applied before finality",
                db2.queryOne("SELECT ship_address FROM orders WHERE id = ?", arrayOf("w1"))?.get("ship_address"),
            )
            val recoveredScope = db2.readTransaction { connection -> SynchroMeta.getScope(connection, scopeID) }
            assertEquals(finalCursor, recoveredScope?.cursor)
            assertEquals(checksumJSON(rebuiltChecksum), recoveredScope?.checksum)
            assertNull(db2.readTransaction { connection -> SynchroMeta.getRebuildAttempt(connection, scopeID) })
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
                        MockResponse().setBody(RETRYABLE_503_ERROR_JSON)
                            .setResponseCode(503)
                            .setHeader("Retry-After", "0.01")
                    } else {
                        val body = Json.decodeFromString<JsonObject>(request.body.readUtf8())
                        val mutations = body["mutations"] as kotlinx.serialization.json.JsonArray
                        val accepted = mutations.map { change ->
                            acceptedPushOutcomeJSON(
                                mutation = change as JsonObject,
                                serverVersion = "2026-01-01T14:00:00.000000Z",
                            )
                        }
                        mockResponse("""{"batch_id":${body["batch_id"]},"server_time":"2026-01-01T14:00:00.000Z","accepted":[${accepted.joinToString(",")}],"rejected":[]}""")
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
    fun testPushBackoffPersistsBeforeDelayWithSealedBatchIdentity() = runTest {
        val timing = BlockingRetryTiming(1_000L)
        var failPush = false
        val failedRequestJSON = mutableListOf<String>()
        val events = java.util.Collections.synchronizedList(mutableListOf<SyncEvent>())
        val (engine, db) = makeIntegrationEnv(
            maxRetryAttempts = 1,
            retryTiming = timing,
        ) { request ->
            when {
                request.path!!.endsWith("/sync/connect") -> mockResponse(connectJSON)
                request.path!!.endsWith("/sync/rebuild") -> mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                request.path!!.endsWith("/sync/push") -> {
                    val body = request.body.readUtf8()
                    if (failPush) {
                        failedRequestJSON += body
                        MockResponse().setResponseCode(503).setHeader("Retry-After", "60")
                            .setBody(RETRYABLE_503_ERROR_JSON)
                    } else {
                        mockResponse("""{"batch_id":"unused","server_time":"2026-01-01T14:00:00.000Z","accepted":[],"rejected":[]}""")
                    }
                }
                request.path!!.endsWith("/sync/pull") -> mockResponse(scopePullJSON(cursor = "scope_cursor_2"))
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }
        val registration = engine.onEvent(events::add)

        try {
            engine.start()
            db.execute(
                "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arrayOf("backoff-push", "Retry Street", "u1", "2026-01-01T10:00:00.000000Z"),
            )
            failPush = true

            val syncJob = CoroutineScope(Dispatchers.Default).launch {
                runCatching { engine.syncNow() }
            }
            assertTrue(timing.sleepStarted.await(2, TimeUnit.SECONDS))

            val backoff = requireNotNull(DurableBackoffStore.load(db))
            val batch = requireNotNull(
                db.queryOne("SELECT batch_id, request_json FROM _synchro_push_batches WHERE state = 'pending'")
            )
            assertEquals(RetryOperation.PUSHING, backoff.resumeState)
            assertEquals(batch["batch_id"], backoff.workIdentity)
            assertEquals(RetryClassification.HTTP_503, backoff.retryClassification)
            assertEquals(1L, backoff.attemptCount)
            assertEquals(61_000L, backoff.nextRetryAtMs)
            assertEquals(failedRequestJSON.single(), batch["request_json"])
            val backoffEvent = events.filterIsInstance<SyncEvent.Backoff>().last()
            assertEquals(SyncOperationKind.PUSHING, backoffEvent.backoff.operation)
            assertEquals(1L, backoffEvent.backoff.attempt)
            assertEquals(java.time.Instant.ofEpochMilli(61_000L), backoffEvent.backoff.retryAt)

            timing.releaseAt(61_000L)
            syncJob.join()
            assertEquals(2, failedRequestJSON.size)
            assertEquals(failedRequestJSON[0], failedRequestJSON[1])
        } finally {
            registration.cancel()
            timing.releaseAt(61_000L)
            engine.stop()
        }
    }

    @Test
    fun testRestartWaitsForDurableDeadlineThenReconnectsAndClearsBackoff() = runTest {
        val timing = BlockingRetryTiming(1_000L)
        val initialSyncCompleted = CountDownLatch(1)
        var resumedPullJSON: String? = null
        val (engine, db) = makeIntegrationEnv(retryTiming = timing) { request ->
            when {
                request.path!!.endsWith("/sync/connect") -> mockResponse(connectResumeJSON)
                request.path!!.endsWith("/sync/pull") -> {
                    resumedPullJSON = request.body.readUtf8()
                    mockResponse(scopePullJSON(cursor = "scope_cursor_2"))
                }
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }
        installTestSchema(
            db,
            schemaVersion = 1,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            tables = listOf(protocolOrdersSchema()),
        )
        db.writeTransaction { connection ->
            SynchroMeta.upsertScope(
                connection,
                scopeID,
                cursor = "scope_cursor_1",
                checksum = emptyScopeChecksumJSON(),
            )
            SynchroMeta.setInt64(connection, MetaKey.CLIENT_GENERATION, 1)
            SynchroMeta.setInt64(connection, MetaKey.SCOPE_SET_VERSION, 1)
        }
        val exactPullRequestJSON = Json.encodeToString(
            PullRequest.serializer(),
            PullRequest(
                clientID = "test-device",
                clientGeneration = 1,
                schema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
                scopeSetVersion = 1,
                scopes = mapOf(scopeID to ScopeCursorRef("scope_cursor_1")),
                limit = 100,
            ),
        )
        db.execute(
            """
            INSERT INTO _synchro_backoff (
                singleton, resume_state, work_identity, retry_classification,
                attempt_count, next_retry_at_ms
            ) VALUES (1, 'pulling', ?, 'network', 2, 61000)
            """.trimIndent(),
            arrayOf(exactPullRequestJSON),
        )

        try {
            engine.start(SyncOptions(initialSyncCompleted = { initialSyncCompleted.countDown() }))
            assertTrue(timing.sleepStarted.await(2, TimeUnit.SECONDS))
            assertEquals(0, server!!.requestCount)
            assertNotNull(DurableBackoffStore.load(db))

            timing.releaseAt(61_000L)
            assertTrue(initialSyncCompleted.await(2, TimeUnit.SECONDS))
            assertEquals(2, server!!.requestCount)
            assertEquals(exactPullRequestJSON, resumedPullJSON)
            assertNull(DurableBackoffStore.load(db))
        } finally {
            timing.releaseAt(61_000L)
            engine.stop()
        }
    }

    @Test
    fun testStopPreservesDurableBackoff() = runTest {
        val timing = BlockingRetryTiming(1_000L)
        val (engine, db) = makeIntegrationEnv(retryTiming = timing) {
            mockResponse("""{"error":"network must not start before deadline"}""", 500)
        }
        db.execute(
            """
            INSERT INTO _synchro_backoff (
                singleton, resume_state, work_identity, retry_classification,
                attempt_count, next_retry_at_ms
            ) VALUES (1, 'connecting', '{"request":"exact-connect"}', 'http_429', 3, 61000)
            """.trimIndent(),
        )

        engine.start()
        assertTrue(timing.sleepStarted.await(2, TimeUnit.SECONDS))
        engine.stop()

        val retained = requireNotNull(DurableBackoffStore.load(db))
        assertEquals(RetryOperation.CONNECTING, retained.resumeState)
        assertEquals("{\"request\":\"exact-connect\"}", retained.workIdentity)
        assertEquals(3L, retained.attemptCount)
        assertEquals(0, server!!.requestCount)
    }

    @Test
    fun testRebuildBackoffStoresExactRequestAndRetainsAttempt() = runTest {
        val timing = BlockingRetryTiming(5_000L)
        var failRebuild = false
        val failedRequestJSON = mutableListOf<String>()
        val (engine, db) = makeIntegrationEnv(
            maxRetryAttempts = 1,
            retryTiming = timing,
        ) { request ->
            when {
                request.path!!.endsWith("/sync/connect") -> mockResponse(connectJSON)
                request.path!!.endsWith("/sync/rebuild") -> {
                    val body = request.body.readUtf8()
                    if (failRebuild) {
                        failedRequestJSON += body
                        MockResponse().setResponseCode(503).setHeader("Retry-After", "30")
                            .setBody(RETRYABLE_503_ERROR_JSON)
                    } else {
                        mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                    }
                }
                request.path!!.endsWith("/sync/pull") -> mockResponse(scopePullJSON(cursor = "scope_cursor_2"))
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        try {
            engine.start()
            val receiptCountBeforeFailure = db.query("SELECT * FROM _synchro_rebuild_page_receipts").size
            db.execute("UPDATE _synchro_scopes SET cursor = NULL, checksum = NULL WHERE scope_id = ?", arrayOf(scopeID))
            failRebuild = true

            val syncJob = CoroutineScope(Dispatchers.Default).launch {
                runCatching { engine.syncNow() }
            }
            assertTrue(timing.sleepStarted.await(2, TimeUnit.SECONDS))

            val backoff = requireNotNull(DurableBackoffStore.load(db))
            val requestJSON = failedRequestJSON.single()
            val request = Json.decodeFromString<RebuildRequest>(requestJSON)
            val attempt = db.readTransaction { connection ->
                SynchroMeta.getRebuildAttempt(connection, scopeID)
            }
            assertEquals(RetryOperation.REBUILDING, backoff.resumeState)
            assertEquals(requestJSON, backoff.workIdentity)
            assertEquals(request.rebuildID, attempt?.rebuildID)
            assertEquals(receiptCountBeforeFailure, db.query("SELECT * FROM _synchro_rebuild_page_receipts").size)

            timing.releaseAt(35_000L)
            syncJob.join()
            assertEquals(2, failedRequestJSON.size)
            assertEquals(failedRequestJSON[0], failedRequestJSON[1])
        } finally {
            timing.releaseAt(35_000L)
            engine.stop()
        }
    }

    @Test
    fun testRebuildRestartRequiredReplacesOnlyThatAttempt() = runTest {
        val otherScopeID = "orders_user:other"
        var pullCallCount = 0
        var rebuildCallCount = 0
        val rebuildIDs = mutableListOf<String>()
        lateinit var databaseForHandler: SynchroDatabase

        val (engine, db) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> mockResponse(connectJSON)
                path.endsWith("/sync/rebuild") -> {
                    rebuildCallCount++
                    val body = Json.decodeFromString<JsonObject>(request.body.readUtf8())
                    rebuildIDs += body.getValue("rebuild_id").jsonPrimitive.content
                    when (rebuildCallCount) {
                        1 -> mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                        2 -> {
                            databaseForHandler.writeSyncLockedTransaction { connection ->
                                connection.execSQL(
                                    "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                                    arrayOf("preserved", "keep this row", "u1", "2026-01-01T10:00:00.000Z"),
                                )
                                connection.execSQL(
                                    """
                                    INSERT INTO _synchro_pending_changes (
                                        mutation_id, table_id, table_name, record_id, pk_field_id, pk_logical_type,
                                        operation, authored_schema_version, authored_schema_hash, base_version,
                                        client_version, lifecycle_state, source_kind, created_at, updated_at
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    """.trimIndent(),
                                    arrayOf(
                                        UUID.randomUUID().toString(), "table-orders", "orders", "preserved", "field-id", "string",
                                        "insert", 1, PROTOCOL_TEST_SCHEMA_HASH, null,
                                        "2026-01-01T10:00:00.000000Z", "sealed", "test", "2026-01-01T10:00:00.000000Z", "2026-01-01T10:00:00.000000Z",
                                    ),
                                )
                            }
                            mockResponse(rebuildJSON(cursor = "page-2", hasMore = true))
                        }
                        3 -> {
                            mockResponse(
                                """{"error":{"code":"rebuild_restart_required","message":"stale rebuild","retryable":false,"scope_id":"$scopeID"}}""",
                                409,
                            )
                        }
                        else -> mockResponse(rebuildJSON(finalCursor = "scope_cursor_restarted"))
                    }
                }
                path.endsWith("/sync/pull") -> {
                    pullCallCount++
                    if (pullCallCount == 1) {
                        mockResponse(scopePullJSON(cursor = "scope_cursor_2"))
                    } else {
                        mockResponse(
                            """
                            {
                                "changes": [],
                                "scope_set_version": 1,
                                "scope_cursors": {"$otherScopeID":"other_scope_cursor_next"},
                                "scope_updates": {"add": [], "remove": []},
                                "rebuild": ["$scopeID"],
                                "has_more": false,
                                "checksums": {
                                    "$scopeID": ${emptyScopeChecksumJSON()},
                                    "$otherScopeID": ${checksumJSON(protocolEmptyScopeChecksum(otherScopeID))}
                                }
                            }
                            """.trimIndent(),
                        )
                    }
                }
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }
        databaseForHandler = db

        try {
            engine.start()
            db.writeTransaction { connection ->
                SynchroMeta.upsertScope(
                    connection,
                    otherScopeID,
                    cursor = "other_scope_cursor_old",
                    checksum = checksumJSON(protocolEmptyScopeChecksum(otherScopeID)),
                )
            }
            engine.syncNow()

            assertEquals(4, rebuildCallCount)
            assertEquals(rebuildIDs[1], rebuildIDs[2])
            assertNotEquals(rebuildIDs[2], rebuildIDs[3])
            assertEquals("keep this row", db.queryOne("SELECT ship_address FROM orders WHERE id = ?", arrayOf("preserved"))?.get("ship_address"))
            assertTrue(ChangeTracker(db).hasPendingChanges())
            assertEquals(
                "other_scope_cursor_next",
                db.readTransaction { connection -> SynchroMeta.getScope(connection, otherScopeID)?.cursor },
            )
            assertTrue(
                db.query(
                    "SELECT * FROM _synchro_rebuild_page_receipts WHERE rebuild_id = ?",
                    arrayOf(rebuildIDs[1]),
                ).isEmpty(),
            )
            assertNull(db.readTransaction { conn -> SynchroMeta.getRebuildAttempt(conn, scopeID) })
        } finally {
            engine.stop()
        }
    }

    @Test
    fun testFinalChecksumMismatchStartsNewRebuildAttemptWithoutInstallingFinalState() = runTest {
        val rebuiltRecord = protocolRecord(
            id = "w1",
            shipAddress = "checksum recovery",
            userID = "u1",
            updatedAt = "2026-01-01T12:00:00.000000Z",
            serverVersion = "server-version-checksum",
        )
        val rebuiltChecksum = protocolScopeChecksum(rebuiltRecord)
        var rebuildRequestCount = 0
        val rebuildIDs = mutableListOf<String>()
        var stateBeforeReplacement: LocalScopeState? = null
        lateinit var databaseForHandler: SynchroDatabase

        val (engine, db) = makeIntegrationEnv { request ->
            when {
                request.path!!.endsWith("/sync/connect") -> mockResponse(connectJSON)
                request.path!!.endsWith("/sync/rebuild") -> {
                    rebuildRequestCount++
                    val body = Json.decodeFromString<JsonObject>(request.body.readUtf8())
                    rebuildIDs += body.getValue("rebuild_id").jsonPrimitive.content
                    if (rebuildRequestCount == 1) {
                        mockResponse(
                            rebuildJSON(
                                records = "[${protocolRebuildRecordJSON(rebuiltRecord)}]",
                                finalCursor = "scope_cursor_bad_checksum",
                                checksum = emptyScopeChecksum(),
                            ),
                        )
                    } else {
                        stateBeforeReplacement = databaseForHandler.readTransaction { connection ->
                            SynchroMeta.getScope(connection, scopeID)
                        }
                        mockResponse(
                            rebuildJSON(
                                records = "[${protocolRebuildRecordJSON(rebuiltRecord)}]",
                                finalCursor = "scope_cursor_recovered",
                                checksum = rebuiltChecksum,
                            ),
                        )
                    }
                }
                request.path!!.endsWith("/sync/pull") -> mockResponse(
                    scopePullJSON("scope_cursor_recovered", checksum = rebuiltChecksum),
                )
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }
        databaseForHandler = db

        try {
            engine.start()

            assertEquals(2, rebuildRequestCount)
            assertNotEquals(rebuildIDs[0], rebuildIDs[1])
            assertNull(stateBeforeReplacement?.cursor)
            assertNull(stateBeforeReplacement?.checksum)
            val scope = db.readTransaction { connection -> SynchroMeta.getScope(connection, scopeID) }
            assertEquals("scope_cursor_recovered", scope?.cursor)
            assertEquals(checksumJSON(rebuiltChecksum), scope?.checksum)
        } finally {
            engine.stop()
        }
    }

    @Test
    fun testGenerationExpiryReconnectsRenewsAndNeverResendsRetiredBatch() = runTest {
        var connectCallCount = 0
        var pushCallCount = 0
        val cycleCalls = mutableListOf<String>()
        val pushBodies = mutableListOf<JsonObject>()

        val (engine, db) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> {
                    connectCallCount += 1
                    cycleCalls += "connect"
                    mockResponse(
                        if (connectCallCount == 1) connectJSON
                        else connectNoneJSON(2, 1, PROTOCOL_TEST_SCHEMA_HASH, invalidateScope = true),
                    )
                }
                path.endsWith("/sync/rebuild") -> mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                path.endsWith("/sync/push") -> {
                    pushCallCount += 1
                    cycleCalls += "push"
                    val body = Json.decodeFromString<JsonObject>(request.body.readUtf8())
                    pushBodies += body
                    if (pushCallCount == 1) {
                        mockResponse(
                            """
                            {"error":{"code":"client_generation_expired","message":"generation expired","retryable":false,"current_client_generation":2}}
                            """.trimIndent(),
                            409,
                        )
                    } else {
                        val accepted = body.getValue("mutations").jsonArray.map { mutation ->
                            acceptedPushOutcomeJSON(mutation.jsonObject, "server-generation-2")
                        }
                        mockResponse(
                            """{"batch_id":${body["batch_id"]},"server_time":"2026-01-01T14:00:00.000Z","accepted":[${accepted.joinToString(",")}],"rejected":[]}""",
                        )
                    }
                }
                path.endsWith("/sync/pull") -> {
                    cycleCalls += "pull"
                    mockResponse(scopePullJSON(cursor = "scope_cursor_2"))
                }
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        try {
            engine.start()
            cycleCalls.clear()
            db.execute(
                "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arrayOf("w1", "generation renewal", "u1", "2026-01-01T10:00:00.000000Z"),
            )

            engine.syncNow()

            assertEquals(listOf("push", "connect", "push", "pull"), cycleCalls)
            assertEquals(2, pushBodies.size)
            assertNotEquals(pushBodies[0]["batch_id"], pushBodies[1]["batch_id"])
            assertEquals(pushBodies[0]["mutations"], pushBodies[1]["mutations"])
            assertEquals("1", pushBodies[0].getValue("client_generation").jsonPrimitive.content)
            assertEquals("2", pushBodies[1].getValue("client_generation").jsonPrimitive.content)
            val oldBatchID = pushBodies[0].getValue("batch_id").jsonPrimitive.content
            val newBatchID = pushBodies[1].getValue("batch_id").jsonPrimitive.content
            val states = db.query("SELECT batch_id, state FROM _synchro_push_batches")
                .associate { it.getValue("batch_id") as String to it.getValue("state") }
            assertEquals("superseded", states[oldBatchID])
            assertEquals("completed", states[newBatchID])
            assertEquals(2L, db.readTransaction { SynchroMeta.getInt64(it, MetaKey.CLIENT_GENERATION) })
            assertFalse(ChangeTracker(db).hasPendingChanges())
        } finally {
            engine.stop()
        }
    }

    @Test
    fun testRebuildGenerationExpiryReconnectsAndRestartsRebuild() = runTest {
        var connectCallCount = 0
        var rebuildCallCount = 0
        var pullCallCount = 0

        val (engine, db) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> {
                    connectCallCount += 1
                    when (connectCallCount) {
                        1 -> mockResponse(connectJSON)
                        2 -> MockResponse().setResponseCode(503).setHeader("Retry-After", "0.01")
                            .setBody(RETRYABLE_503_ERROR_JSON)
                        else -> mockResponse(
                            connectNoneJSON(2, 1, PROTOCOL_TEST_SCHEMA_HASH, invalidateScope = true)
                        )
                    }
                }
                path.endsWith("/sync/rebuild") -> {
                    rebuildCallCount += 1
                    if (rebuildCallCount <= 2) {
                        mockResponse(
                            """
                            {"error":{"code":"client_generation_expired","message":"generation expired","retryable":false,"current_client_generation":2}}
                            """.trimIndent(),
                            409,
                        )
                    } else {
                        mockResponse(rebuildJSON(finalCursor = "scope_cursor_generation_2"))
                    }
                }
                path.endsWith("/sync/pull") -> {
                    pullCallCount += 1
                    mockResponse(scopePullJSON(cursor = "scope_cursor_after_generation_2"))
                }
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        try {
            engine.start()

            assertEquals(4, connectCallCount)
            assertEquals(3, rebuildCallCount)
            assertEquals(1, pullCallCount)
            assertEquals(2L, db.readTransaction { SynchroMeta.getInt64(it, MetaKey.CLIENT_GENERATION) })
        } finally {
            engine.stop()
        }
    }

    @Test
    fun testPullSchemaMismatchReconnectsBeforeRetry() = runTest {
        val nextHash = replacementSchemaHash
        val nextChecksum = Integrity.scopeDigest(nextHash, scopeID, emptyList())
        var connectCallCount = 0
        var pullCallCount = 0

        val (engine, db) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> {
                    connectCallCount += 1
                    mockResponse(
                        if (connectCallCount == 1) connectJSON
                        else connectReplacementJSON(1, 2, nextHash, "scope_cursor_schema_2"),
                    )
                }
                path.endsWith("/sync/rebuild") -> mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                path.endsWith("/sync/pull") -> {
                    pullCallCount += 1
                    when (pullCallCount) {
                        1 -> mockResponse(scopePullJSON(cursor = "scope_cursor_2"))
                        2 -> mockResponse(
                            """
                            {"error":{"code":"schema_mismatch","message":"schema changed","retryable":false,"current_schema":{"version":2,"hash":"$nextHash"},"received_schema":{"version":1,"hash":"$PROTOCOL_TEST_SCHEMA_HASH"}}}
                            """.trimIndent(),
                            422,
                        )
                        else -> mockResponse(
                            scopePullJSON(cursor = "scope_cursor_schema_3", checksum = nextChecksum)
                        )
                    }
                }
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        try {
            engine.start()
            engine.syncNow()

            assertEquals(2, connectCallCount)
            assertEquals(3, pullCallCount)
            assertEquals(2L, db.readTransaction { SynchroMeta.getInt64(it, MetaKey.SCHEMA_VERSION) })
            assertEquals(nextHash, db.readTransaction { SynchroMeta.get(it, MetaKey.SCHEMA_HASH) })
        } finally {
            engine.stop()
        }
    }

    @Test
    fun testSchemaMismatchReconnectsInstallsSchemaAndRenewsAuthoredMutation() = runTest {
        val nextHash = replacementSchemaHash
        val nextEmptyChecksum = Integrity.scopeDigest(nextHash, scopeID, emptyList())
        var connectCallCount = 0
        var pushCallCount = 0
        var pullCallCount = 0
        val pushBodies = mutableListOf<JsonObject>()

        val (engine, db) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> {
                    connectCallCount += 1
                    mockResponse(
                        if (connectCallCount == 1) connectJSON
                        else connectReplacementJSON(1, 2, nextHash, "scope_cursor_schema_2"),
                    )
                }
                path.endsWith("/sync/rebuild") -> mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                path.endsWith("/sync/push") -> {
                    pushCallCount += 1
                    val body = Json.decodeFromString<JsonObject>(request.body.readUtf8())
                    pushBodies += body
                    if (pushCallCount == 1) {
                        mockResponse(
                            """
                            {"error":{"code":"schema_mismatch","message":"schema changed","retryable":false,"current_schema":{"version":2,"hash":"$nextHash"},"received_schema":{"version":1,"hash":"$PROTOCOL_TEST_SCHEMA_HASH"}}}
                            """.trimIndent(),
                            422,
                        )
                    } else {
                        val accepted = body.getValue("mutations").jsonArray.map { mutation ->
                            acceptedPushOutcomeJSON(
                                mutation.jsonObject,
                                "server-schema-2",
                                schemaVersion = 2,
                                schemaHash = nextHash,
                            )
                        }
                        mockResponse(
                            """{"batch_id":${body["batch_id"]},"server_time":"2026-01-01T14:00:00.000Z","accepted":[${accepted.joinToString(",")}],"rejected":[]}""",
                        )
                    }
                }
                path.endsWith("/sync/pull") -> {
                    pullCallCount += 1
                    if (pullCallCount == 1) {
                        mockResponse(scopePullJSON(cursor = "scope_cursor_2"))
                    } else {
                        mockResponse(scopePullJSON(cursor = "scope_cursor_3", checksum = nextEmptyChecksum))
                    }
                }
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        try {
            engine.start()
            db.execute(
                "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arrayOf("w1", "schema renewal", "u1", "2026-01-01T10:00:00.000000Z"),
            )

            engine.syncNow()

            assertEquals(2, pushBodies.size)
            assertNotEquals(pushBodies[0]["batch_id"], pushBodies[1]["batch_id"])
            assertEquals(pushBodies[0]["mutations"], pushBodies[1]["mutations"])
            assertEquals("1", pushBodies[0].getValue("schema").jsonObject.getValue("version").jsonPrimitive.content)
            assertEquals("2", pushBodies[1].getValue("schema").jsonObject.getValue("version").jsonPrimitive.content)
            val renewedMutation = pushBodies[1].getValue("mutations").jsonArray.single().jsonObject
            assertEquals(
                "1",
                renewedMutation.getValue("authored_schema").jsonObject.getValue("version").jsonPrimitive.content,
            )
            assertEquals(2L, db.readTransaction { SynchroMeta.getInt64(it, MetaKey.SCHEMA_VERSION) })
            assertEquals(nextHash, db.readTransaction { SynchroMeta.get(it, MetaKey.SCHEMA_HASH) })
            assertFalse(ChangeTracker(db).hasPendingChanges())
        } finally {
            engine.stop()
        }
    }

    @Test
    fun testRenewalRequiredBatchSurvivesRestartAndUsesOnlyItsSuccessor() = runTest {
        val dbName = "renewal_restart_${UUID.randomUUID()}.sqlite"
        val clientID = "renewal-restart-device"
        var connectCallCount = 0
        var pushCallCount = 0
        val pushBodies = mutableListOf<JsonObject>()

        val handler: (RecordedRequest) -> MockResponse = { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> {
                    connectCallCount += 1
                    when (connectCallCount) {
                        1 -> mockResponse(connectJSON)
                        2 -> MockResponse().setResponseCode(503).setHeader("Retry-After", "0.01")
                            .setBody(RETRYABLE_503_ERROR_JSON)
                        else -> mockResponse(
                            connectNoneJSON(2, 1, PROTOCOL_TEST_SCHEMA_HASH, invalidateScope = true)
                        )
                    }
                }
                path.endsWith("/sync/rebuild") -> mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                path.endsWith("/sync/push") -> {
                    pushCallCount += 1
                    val body = Json.decodeFromString<JsonObject>(request.body.readUtf8())
                    pushBodies += body
                    if (pushCallCount == 1) {
                        mockResponse(
                            """
                            {"error":{"code":"client_generation_expired","message":"generation expired","retryable":false,"current_client_generation":2}}
                            """.trimIndent(),
                            409,
                        )
                    } else {
                        val accepted = body.getValue("mutations").jsonArray.map { mutation ->
                            acceptedPushOutcomeJSON(mutation.jsonObject, "server-after-restart")
                        }
                        mockResponse(
                            """{"batch_id":${body["batch_id"]},"server_time":"2026-01-01T14:00:00.000Z","accepted":[${accepted.joinToString(",")}],"rejected":[]}""",
                        )
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
            handler = handler,
        )
        try {
            engine1.start()
            db1.execute(
                "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arrayOf("w1", "restart renewal", "u1", "2026-01-01T10:00:00.000000Z"),
            )
            assertTrue(runCatching { engine1.syncNow() }.exceptionOrNull() is RetryableError)
            assertEquals(
                "renewal_required",
                db1.queryOne("SELECT state FROM _synchro_push_batches")?.get("state"),
            )
            assertEquals(1, pushBodies.size)
        } finally {
            engine1.stop()
            db1.close()
        }

        val (engine2, db2) = makeIntegrationEnv(
            dbName = dbName,
            clientID = clientID,
            maxRetryAttempts = 0,
            handler = handler,
        )
        try {
            val initialSyncCompleted = CountDownLatch(1)
            engine2.start(SyncOptions(initialSyncCompleted = { initialSyncCompleted.countDown() }))
            assertTrue(initialSyncCompleted.await(2, TimeUnit.SECONDS))

            assertEquals(2, pushBodies.size)
            assertNotEquals(pushBodies[0]["batch_id"], pushBodies[1]["batch_id"])
            assertEquals(pushBodies[0]["mutations"], pushBodies[1]["mutations"])
            val oldBatchID = pushBodies[0].getValue("batch_id").jsonPrimitive.content
            val newBatchID = pushBodies[1].getValue("batch_id").jsonPrimitive.content
            val states = db2.query("SELECT batch_id, state FROM _synchro_push_batches")
                .associate { it.getValue("batch_id") as String to it.getValue("state") }
            assertEquals("superseded", states[oldBatchID])
            assertEquals("completed", states[newBatchID])
            assertFalse(ChangeTracker(db2).hasPendingChanges())
        } finally {
            engine2.stop()
            db2.close()
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
                            .setBody(RETRYABLE_503_ERROR_JSON)
                            .setResponseCode(503)
                            .setHeader("Retry-After", "0.01")
                    } else {
                        val body = Json.decodeFromString<JsonObject>(request.body.readUtf8())
                        val mutations = body["mutations"] as kotlinx.serialization.json.JsonArray
                        val accepted = mutations.map { change ->
                            acceptedPushOutcomeJSON(
                                mutation = change as JsonObject,
                                serverVersion = "2026-01-01T14:00:00.000000Z",
                            )
                        }
                        mockResponse("""{"batch_id":${body["batch_id"]},"server_time":"2026-01-01T14:00:00.000Z","accepted":[${accepted.joinToString(",")}],"rejected":[]}""")
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
        installTestSchema(
            db1,
            schemaVersion = 1,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
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
        installTestSchema(
            db2,
            schemaVersion = 1,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            tables = listOf(ordersLocalSchemaTable(includeNotes = false))
        )
        try {
            val initialSyncCompleted = CountDownLatch(1)
            engine2.start(SyncOptions(initialSyncCompleted = { initialSyncCompleted.countDown() }))
            assertTrue(initialSyncCompleted.await(2, TimeUnit.SECONDS))

            val tracker = ChangeTracker(db2)
            assertFalse(tracker.hasPendingChanges())
            assertEquals(2, pushCallCount)

            val localRow = db2.queryOne(
                "SELECT ship_address, updated_at FROM orders WHERE id = ?",
                arrayOf("w1")
            )
            assertEquals("123 Main St", localRow?.get("ship_address"))
            assertEquals("2026-01-01T14:00:00.000000Z", localRow?.get("updated_at"))

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
                        mockResponse(RETRYABLE_503_ERROR_JSON, 503)
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
                is SyncStatus.Backoff -> statuses.add("backoff")
                is SyncStatus.Ready -> statuses.add("ready")
                else -> Unit
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
            assertTrue(statuses.contains("backoff"))
            assertEquals("ready", statuses.last())

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
            engine.retry()
        } finally {
            engine.stop()
        }
    }

    @Test
    fun testConnectUnsupportedFailsExplicitly() = runTest {
        val events = java.util.Collections.synchronizedList(mutableListOf<SyncEvent>())
        val (engine, db) = makeIntegrationEnv { request ->
            val path = request.path ?: ""
            when {
                path.endsWith("/sync/connect") -> {
                    mockResponse(
                        """
                        {
                          "server_time":"2026-01-01T12:00:00.000Z",
                          "protocol_version":3,
                          "client_generation":1,
                          "scope_set_version":1,
                          "schema":{"version":2,"hash":"${"f".repeat(64)}","action":"unsupported","reason":"unknown_schema_lineage"},
                          "scopes":{"add":[],"remove":[]},
                          "scope_cursor_updates":{}
                        }
                        """.trimIndent()
                    )
                }
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }
        val registration = engine.onEvent(events::add)

        try {
            try {
                engine.start()
                fail("Expected unsupported connect schema action failure")
            } catch (error: SynchroError.UnsupportedSchema) {
                assertEquals(SchemaUnsupportedReason.UNKNOWN_SCHEMA_LINEAGE, error.reason)
            }
            val durable = db.readTransaction { connection -> SynchroMeta.getClientState(connection) }
            assertEquals(SyncLifecycleState.ERROR, durable.lifecycleState)
            assertEquals(SyncFailureCode.UNSUPPORTED_SCHEMA, durable.failure?.code)
            assertEquals(SyncOperationKind.SCHEMA, durable.failure?.operation)
            assertFalse(durable.failure?.retryable ?: true)
            assertEquals(
                "The installed schema requires an explicit synchronized reset.",
                durable.failure?.message,
            )
            assertEquals(SyncRecoveryAction.SCHEMA_RESET, durable.failure?.recoveryAction)
            assertEquals(
                mapOf("reason" to "unknown_schema_lineage"),
                durable.failure?.metadata,
            )
            val failureEvent = events.filterIsInstance<SyncEvent.Failure>().single()
            assertEquals(durable.failure, failureEvent.failure)
            assertTrue(engine.getSyncStatus() is SyncStatus.Error)
            val retryFailure = try {
                engine.retry()
                null
            } catch (error: Exception) {
                error
            }
            assertTrue(retryFailure is SynchroError.InvalidResponse)
        } finally {
            registration.cancel()
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
            statuses.add(status.state.wireName)
        }

        try {
            engine.start()

            assertTrue(statuses.contains("local_ready"))
            assertTrue(statuses.contains("connecting"))
            assertTrue(statuses.contains("pulling"))
            assertEquals("ready", statuses.last())

            // syncNow triggers another cycle
            statuses.clear()
            engine.syncNow()
            assertEquals(listOf("pulling", "ready"), statuses)

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
                        val record = protocolRecord(
                            id = c.getValue("pk").jsonObject.getValue("field-id").jsonPrimitive.content,
                            shipAddress = "Server Wins Address",
                            userID = "u1",
                            updatedAt = "2026-01-01T15:00:00.000000Z",
                            serverVersion = "2026-01-01T15:00:00.000000Z",
                        )
                        rejectedPushOutcomeJSON(
                            mutation = c,
                            status = MutationStatus.CONFLICT,
                            code = MutationRejectionCode.VERSION_CONFLICT,
                            message = "server version is newer",
                            serverRow = requireNotNull(record.change.row),
                            serverVersion = record.change.serverVersion,
                        )
                    }
                    mockResponse("""{"batch_id":${body["batch_id"]},"server_time":"2026-01-01T15:00:00.000Z","accepted":[],"rejected":[${rejected.joinToString(",")}]}""")
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
                        rejectedPushOutcomeJSON(
                            mutation = c,
                            status = MutationStatus.REJECTED_TERMINAL,
                            code = MutationRejectionCode.POLICY_REJECTED,
                            message = "explicit rejection for inspection",
                        )
                    }
                    mockResponse("""{"batch_id":${body["batch_id"]},"server_time":"2026-01-01T15:00:00.000Z","accepted":[],"rejected":[${rejected.joinToString(",")}]}""")
                }
                path.endsWith("/sync/pull") -> mockResponse(scopePullJSON(cursor = "scope_cursor_2"))
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }

        lateinit var rejectedMutationID: String
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
            UUID.fromString(rejectedBeforeRestart[0].mutationID)
            assertEquals("policy_rejected", rejectedBeforeRestart[0].code)
            rejectedMutationID = rejectedBeforeRestart[0].mutationID
        } finally {
            engine1.stop()
            db1.close()
        }

        val (engine2, db2) = makeIntegrationEnv(dbName = dbName, clientID = clientID, handler = handler)
        try {
            engine2.start()
            val rejectedAfterRestart = db2.readTransaction { conn -> SynchroMeta.listRejectedMutations(conn) }
            assertEquals(1, rejectedAfterRestart.size)
            assertEquals(rejectedMutationID, rejectedAfterRestart[0].mutationID)
            assertEquals("explicit rejection for inspection", rejectedAfterRestart[0].message)
            assertNull(rejectedAfterRestart[0].serverVersion)
            db2.execute("DELETE FROM _synchro_rejected_mutations")
            val cleared = db2.query("SELECT mutation_id FROM _synchro_rejected_mutations")
            assertTrue(cleared.isEmpty())
        } finally {
            engine2.stop()
            db2.close()
        }
    }

    @Test
    fun explicitSchemaResetPreservesLocalOnlyDataQueueAndOutcomes() = runTest {
        val targetDraft = protocolOrdersSchemaManifest(
            includeNotes = true,
            schemaVersion = 2,
            parentSchema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
            transitionClass = "class_3",
            compatibilityFloor = 2,
        )
        val target = targetDraft.copy(schemaHash = Integrity.schemaManifestHash(targetDraft))
        val resetResponse = Json { encodeDefaults = true }.encodeToString(
            ConnectResponse.serializer(),
            ConnectResponse(
                serverTime = "2026-01-01T12:00:00.000Z",
                protocolVersion = 3,
                clientGeneration = 1,
                scopeSetVersion = 0,
                schema = SchemaDescriptor(2, target.schemaHash, SchemaAction.REPLACE),
                scopes = ScopeAssignmentDelta(emptyList(), emptyList()),
                scopeCursorUpdates = emptyMap(),
                schemaDefinition = target,
            ),
        )
        var resetRequest: JsonObject? = null
        val (engine, db) = makeIntegrationEnv { request ->
            if (!request.path.orEmpty().endsWith("/sync/connect")) {
                mockResponse("""{"error":"unexpected"}""", 500)
            } else {
                val body = Json.decodeFromString<JsonObject>(request.body.readUtf8())
                if (body["schema_reset"]?.jsonPrimitive?.content == "true") {
                    resetRequest = body
                    mockResponse(resetResponse)
                } else {
                    mockResponse(
                        """
                        {
                          "server_time":"2026-01-01T12:00:00.000Z",
                          "protocol_version":3,
                          "client_generation":1,
                          "scope_set_version":0,
                          "schema":{"version":2,"hash":"${target.schemaHash}","action":"unsupported","reason":"unknown_schema_lineage"},
                          "scopes":{"add":[],"remove":[]},
                          "scope_cursor_updates":{}
                        }
                        """.trimIndent(),
                    )
                }
            }
        }
        val rejectedID = UUID.randomUUID().toString()
        try {
            installTestSchema(
                db,
                schemaVersion = 1,
                schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
                tables = listOf(protocolOrdersSchema()),
            )
            db.createLocalOnlyTable(
                "drafts",
                listOf(
                    ColumnDef("id", "TEXT", nullable = false, primaryKey = true),
                    ColumnDef("body", "TEXT", nullable = false),
                ),
            )
            db.execute("INSERT INTO drafts (id, body) VALUES ('draft', 'keep')")
            db.execute(
                "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arrayOf("queued", "Queue Street", "u1", "2026-01-01T00:00:00.000000Z"),
            )
            db.execute(
                "UPDATE _synchro_pending_changes SET lifecycle_state = 'blocked_by_predecessor' WHERE record_id = 'queued'",
            )
            db.writeTransaction { connection ->
                SynchroMeta.upsertRejectedMutation(
                    db = connection,
                    mutationID = rejectedID,
                    tableName = "orders",
                    recordId = "rejected",
                    status = "rejected_terminal",
                    code = "policy_rejected",
                    message = "keep outcome",
                    serverRowJson = null,
                    serverVersion = null,
                    mutationJSON = "{}",
                    rejectionJSON = "{}",
                )
            }

            val failure = try {
                engine.start()
                null
            } catch (error: Exception) {
                error
            }
            assertTrue(failure is SynchroError.UnsupportedSchema)

            engine.resetSchema()

            assertEquals("true", resetRequest?.get("schema_reset")?.jsonPrimitive?.content)
            assertEquals("1", resetRequest?.get("client_generation")?.jsonPrimitive?.content)
            assertEquals("keep", db.queryOne("SELECT body FROM drafts WHERE id = 'draft'")?.get("body"))
            assertEquals(1L, db.queryOne("SELECT COUNT(*) AS count FROM _synchro_pending_changes")?.get("count"))
            assertEquals(
                rejectedID,
                db.readTransaction { connection -> SynchroMeta.listRejectedMutations(connection).single().mutationID },
            )
            assertTrue(db.query("SELECT id FROM orders").isEmpty())
            assertTrue(db.query("PRAGMA table_info(orders)").map { it.getValue("name") }.contains("notes"))
            assertTrue(db.readTransaction { connection -> SynchroMeta.getClientState(connection).failure == null })
            assertTrue(engine.getSyncStatus() is SyncStatus.Ready)
        } finally {
            engine.stop()
        }
    }

    @Test
    fun schemaResetRejectsRetainedScopeCursor() = runTest {
        val targetDraft = protocolOrdersSchemaManifest(
            includeNotes = true,
            schemaVersion = 2,
            parentSchema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
            transitionClass = "class_3",
            compatibilityFloor = 2,
        )
        val target = targetDraft.copy(schemaHash = Integrity.schemaManifestHash(targetDraft))
        val (engine, db) = makeIntegrationEnv { mockResponse("{}", 500) }
        try {
            db.writeTransaction { connection ->
                SynchroMeta.upsertScope(connection, scopeID, "old-cursor", null)
            }
            val response = ConnectResponse(
                serverTime = "2026-01-01T12:00:00.000Z",
                protocolVersion = 3,
                clientGeneration = 1,
                scopeSetVersion = 0,
                schema = SchemaDescriptor(2, target.schemaHash, SchemaAction.REBUILD_LOCAL),
                scopes = ScopeAssignmentDelta(emptyList(), emptyList()),
                scopeCursorUpdates = mapOf(scopeID to "reused-cursor"),
                affectedScopes = listOf(scopeID),
                schemaDefinition = target,
            )

            val failure = try {
                engine.installConnectResponse(response, schemaReset = true)
                null
            } catch (error: Exception) {
                error
            }

            assertTrue(failure is SynchroError.InvalidResponse)
            assertEquals(
                "old-cursor",
                db.readTransaction { connection -> SynchroMeta.getScope(connection, scopeID)?.cursor },
            )
        } finally {
            engine.stop()
        }
    }

    @Test
    fun backgroundDefersAutomaticPushForegroundResumesAndStopRemainsStopped() = runTest {
        val pushObserved = CountDownLatch(1)
        val pushCount = AtomicInteger()
        val (engine, db) = makeIntegrationEnv(pushDebounce = 0.01) { request ->
            when {
                request.path.orEmpty().endsWith("/sync/connect") -> mockResponse(connectJSON)
                request.path.orEmpty().endsWith("/sync/rebuild") -> mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                request.path.orEmpty().endsWith("/sync/pull") -> mockResponse(scopePullJSON(cursor = "scope_cursor_2"))
                request.path.orEmpty().endsWith("/sync/push") -> {
                    pushCount.incrementAndGet()
                    pushObserved.countDown()
                    val body = Json.decodeFromString<JsonObject>(request.body.readUtf8())
                    val accepted = body.getValue("mutations").jsonArray.map { mutation ->
                        acceptedPushOutcomeJSON(
                            mutation = mutation.jsonObject,
                            serverVersion = "foreground-server-version",
                        )
                    }
                    mockResponse(
                        """{"batch_id":${body["batch_id"]},"server_time":"2026-01-01T14:00:00.000Z","accepted":[${accepted.joinToString(",")}],"rejected":[]}""",
                    )
                }
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }
        try {
            engine.start()
            engine.onApplicationBackground()
            db.execute(
                "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arrayOf("background", "Deferred Street", "u1", "2026-01-01T00:00:00.000000Z"),
            )
            assertFalse(pushObserved.await(250, TimeUnit.MILLISECONDS))

            engine.onApplicationForeground()
            assertTrue(pushObserved.await(2, TimeUnit.SECONDS))

            engine.stop()
            val completedPushes = pushCount.get()
            engine.onApplicationForeground()
            Thread.sleep(100)
            assertEquals(completedPushes, pushCount.get())
            assertTrue(engine.getSyncStatus() is SyncStatus.Stopped)
        } finally {
            engine.stop()
        }
    }

    @Test
    fun syncEventsPublishExactStateSchemaRebuildAndMutationOutcomes() = runTest {
        val events = java.util.Collections.synchronizedList(mutableListOf<SyncEvent>())
        val (engine, db) = makeIntegrationEnv { request ->
            when {
                request.path.orEmpty().endsWith("/sync/connect") -> mockResponse(connectJSON)
                request.path.orEmpty().endsWith("/sync/rebuild") -> mockResponse(rebuildJSON(finalCursor = "scope_cursor_1"))
                request.path.orEmpty().endsWith("/sync/pull") -> mockResponse(scopePullJSON(cursor = "scope_cursor_2"))
                request.path.orEmpty().endsWith("/sync/push") -> {
                    val body = Json.decodeFromString<JsonObject>(request.body.readUtf8())
                    val mutations = body.getValue("mutations").jsonArray.map { it.jsonObject }
                    val accepted = mutations.take(1).map { mutation ->
                        acceptedPushOutcomeJSON(
                            mutation = mutation,
                            serverVersion = "event-server-version",
                        )
                    }
                    val rejected = mutations.drop(1).map { mutation ->
                        rejectedPushOutcomeJSON(
                            mutation = mutation,
                            status = MutationStatus.REJECTED_TERMINAL,
                            code = MutationRejectionCode.POLICY_REJECTED,
                            message = "event rejection",
                        )
                    }
                    mockResponse(
                        """{"batch_id":${body["batch_id"]},"server_time":"2026-01-01T14:00:00.000Z","accepted":[${accepted.joinToString(",")}],"rejected":[${rejected.joinToString(",")}]}""",
                    )
                }
                else -> mockResponse("""{"error":"unexpected"}""", 500)
            }
        }
        val registration = engine.onEvent(events::add)
        try {
            engine.start()
            db.execute(
                "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arrayOf("event", "Event Street", "u1", "2026-01-01T00:00:00.000000Z"),
            )
            db.execute(
                "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arrayOf("event-rejected", "Rejected Street", "u1", "2026-01-01T00:00:00.000000Z"),
            )
            engine.syncNow()

            val observed = events.toList()
            val kinds = observed.map { event ->
                when (event) {
                    is SyncEvent.StateChanged -> "state_changed"
                    is SyncEvent.Backoff -> "backoff"
                    is SyncEvent.SchemaApplying -> "schema_applying"
                    is SyncEvent.SchemaApplied -> "schema_applied"
                    is SyncEvent.MutationAccepted -> "mutation_accepted"
                    is SyncEvent.MutationRejected -> "mutation_rejected"
                    is SyncEvent.RebuildRequested -> "rebuild_requested"
                    is SyncEvent.RebuildCompleted -> "rebuild_completed"
                    is SyncEvent.Failure -> "failure"
                }
            }
            assertTrue(observed.any {
                it is SyncEvent.StateChanged &&
                    it.change.from == SyncLifecycleState.UNINITIALIZED &&
                    it.change.to == SyncLifecycleState.LOCAL_READY
            })

            val applying = observed.filterIsInstance<SyncEvent.SchemaApplying>().single()
            assertEquals(SchemaRef(0, ""), applying.schema.source)
            assertEquals(SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH), applying.schema.target)
            assertEquals(SchemaAction.REPLACE, applying.schema.action)
            val applied = observed.filterIsInstance<SyncEvent.SchemaApplied>().single()
            assertEquals(applying.schema, applied.schema)
            assertTrue(kinds.indexOf("schema_applying") < kinds.indexOf("schema_applied"))

            val rebuildRequested = observed.filterIsInstance<SyncEvent.RebuildRequested>().single()
            val rebuildCompleted = observed.filterIsInstance<SyncEvent.RebuildCompleted>().single()
            assertEquals(scopeID, rebuildRequested.rebuild.scopeID)
            assertTrue(rebuildRequested.rebuild.rebuildID.isNotEmpty())
            assertEquals(rebuildRequested.rebuild, rebuildCompleted.rebuild)
            assertTrue(kinds.indexOf("rebuild_requested") < kinds.indexOf("rebuild_completed"))

            val accepted = observed.filterIsInstance<SyncEvent.MutationAccepted>().single()
            assertEquals("table-orders", accepted.mutation.tableID)
            assertEquals(MutationStatus.APPLIED, accepted.mutation.status)
            assertNull(accepted.mutation.rejectionCode)
            assertTrue(accepted.mutation.mutationID.isNotEmpty())

            val rejected = observed.filterIsInstance<SyncEvent.MutationRejected>().single()
            assertEquals("table-orders", rejected.mutation.tableID)
            assertEquals(MutationStatus.REJECTED_TERMINAL, rejected.mutation.status)
            assertEquals(MutationRejectionCode.POLICY_REJECTED, rejected.mutation.rejectionCode)
            assertTrue(rejected.mutation.mutationID.isNotEmpty())
            assertTrue(kinds.contains("state_changed"))
        } finally {
            registration.cancel()
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
        dbName: String = "synchro_test_${UUID.randomUUID()}.sqlite",
        clientID: String = "test-device",
        maxRetryAttempts: Int = 3,
        pushDebounce: Double = 0.5,
        retryTiming: RetryTiming? = null,
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
            pushDebounce = pushDebounce,
            maxRetryAttempts = maxRetryAttempts
        )
        val db = databases.open(context, dbName)
        val httpClient = HttpClient(config, OkHttpClient())
        val schemaManager = SchemaManager(db)
        val changeTracker = ChangeTracker(db)
        val pullProcessor = PullProcessor(db)
        val pushProcessor = PushProcessor(db, changeTracker)

        val engine = SyncEngine(config, db, httpClient, schemaManager, changeTracker, pullProcessor, pushProcessor)
        retryTiming?.let { engine.retryTiming = it }
        return Pair(engine, db)
    }

    private class BlockingRetryTiming(initialTimeMillis: Long) : RetryTiming {
        @Volatile
        private var currentTimeMillis = initialTimeMillis
        private val release = CompletableDeferred<Unit>()
        val sleepStarted = CountDownLatch(1)

        override fun currentTimeMillis(): Long = currentTimeMillis
        override fun jitterFraction(): Double = 0.0

        override suspend fun sleep(delayMillis: Long) {
            sleepStarted.countDown()
            release.await()
        }

        fun releaseAt(timeMillis: Long) {
            currentTimeMillis = timeMillis
            release.complete(Unit)
        }
    }

    private fun installEmptySeedScope(db: android.database.sqlite.SQLiteDatabase, scopeID: String, receipt: String) {
        val checksum = protocolEmptyScopeChecksum(scopeID)
        val encoded = checksumJSON(checksum)
        SynchroMeta.upsertScope(db, scopeID, null, encoded, 0L, encoded)
        db.execSQL(
            """
            INSERT INTO _synchro_seed_receipts
                (scope_id, receipt, schema_version, schema_hash, cardinality, checksum)
            VALUES (?, ?, ?, ?, 0, ?)
            """.trimIndent(),
            arrayOf(scopeID, receipt, 1L, PROTOCOL_TEST_SCHEMA_HASH, encoded),
        )
    }

    private fun installSeedScope(
        database: SynchroDatabase,
        schema: LocalSchemaTable,
        scopeID: String,
        receipt: String,
        rows: List<Pair<String, String>>,
    ): String {
        val digests = rows.map { (recordID, address) ->
            val primaryKey = buildJsonObject { put("field-id", JsonPrimitive(recordID)) }
            val row = buildJsonObject {
                put("field-id", JsonPrimitive(recordID))
                put("field-ship-address", JsonPrimitive(address))
                put("field-user-id", JsonPrimitive("portable"))
                put("field-updated-at", JsonPrimitive("2026-01-01T00:00:00.000000Z"))
                put("field-deleted-at", JsonNull)
            }
            recordID to Integrity.rowDigest(
                PROTOCOL_TEST_SCHEMA_HASH,
                schema,
                primaryKey,
                row,
                "server-version-$recordID",
            )
        }
        val scopeChecksum = Integrity.scopeDigest(
            PROTOCOL_TEST_SCHEMA_HASH,
            scopeID,
            digests.map { it.second.identity to it.second.checksum },
        )
        val encodedScopeChecksum = checksumJSON(scopeChecksum)
        database.writeSyncLockedTransaction { db ->
            for ((record, digest) in digests) {
                val address = rows.single { it.first == record }.second
                db.execSQL(
                    """
                    INSERT INTO orders (id, ship_address, user_id, updated_at, deleted_at)
                    VALUES (?, ?, 'portable', '2026-01-01T00:00:00.000000Z', NULL)
                    """.trimIndent(),
                    arrayOf(record, address),
                )
                SynchroMeta.upsertRowVersion(
                    db,
                    "orders",
                    record,
                    "server-version-$record",
                    digest.checksum,
                )
                SynchroMeta.upsertScopeRow(db, scopeID, "orders", record, digest.checksum.digest, 0L)
            }
            SynchroMeta.upsertScope(db, scopeID, null, encodedScopeChecksum, 0L, encodedScopeChecksum)
            db.execSQL(
                """
                INSERT INTO _synchro_seed_receipts
                    (scope_id, receipt, schema_version, schema_hash, cardinality, checksum)
                VALUES (?, ?, 1, ?, ?, ?)
                """.trimIndent(),
                arrayOf(scopeID, receipt, PROTOCOL_TEST_SCHEMA_HASH, rows.size.toLong(), encodedScopeChecksum),
            )
        }
        return digests.singleOrNull()?.second?.checksum?.digest.orEmpty()
    }

    // MARK: - Mock JSON Helpers

    private val scopeID = "orders_user:u1"

    private data class ProtocolRecord(
        val schema: LocalSchemaTable,
        val change: ChangeRecord,
        val schemaHash: String,
    )

    private fun protocolOrdersSchema(includeNotes: Boolean = false): LocalSchemaTable {
        val columns = mutableListOf(
            LocalSchemaColumn("field-id", "id", "string", false, false, isPrimaryKey = true),
            LocalSchemaColumn("field-ship-address", "ship_address", "string", true, true, isPrimaryKey = false),
            LocalSchemaColumn("field-user-id", "user_id", "string", false, true, isPrimaryKey = false),
            LocalSchemaColumn("field-updated-at", "updated_at", "datetime", false, false, isPrimaryKey = false),
            LocalSchemaColumn("field-deleted-at", "deleted_at", "datetime", true, false, isPrimaryKey = false),
        )
        if (includeNotes) {
            columns.add(3, LocalSchemaColumn("field-notes", "notes", "string", true, true, isPrimaryKey = false))
        }
        return LocalSchemaTable(
            tableID = "table-orders",
            relationID = "relation-orders",
            tableName = "orders",
            primaryKeyFieldID = "field-id",
            updatedAtFieldID = "field-updated-at",
            deletedAtFieldID = "field-deleted-at",
            updatedAtColumn = "updated_at",
            deletedAtColumn = "deleted_at",
            composition = CompositionClass.SINGLE_SCOPE,
            primaryKey = listOf("id"),
            columns = columns,
        )
    }

    private fun protocolRecord(
        schema: LocalSchemaTable = protocolOrdersSchema(),
        id: String,
        shipAddress: String,
        userID: String,
        updatedAt: String,
        serverVersion: String,
        notes: String? = null,
        schemaHash: String = PROTOCOL_TEST_SCHEMA_HASH,
    ): ProtocolRecord {
        val pk = buildJsonObject { put("field-id", JsonPrimitive(id)) }
        val row = buildJsonObject {
            put("field-id", JsonPrimitive(id))
            put("field-ship-address", JsonPrimitive(shipAddress))
            put("field-user-id", JsonPrimitive(userID))
            if (schema.columns.any { it.fieldID == "field-notes" }) {
                put("field-notes", notes?.let(::JsonPrimitive) ?: JsonNull)
            }
            put("field-updated-at", JsonPrimitive(updatedAt))
            put("field-deleted-at", JsonNull)
        }
        val change = makeChangeRecord(scopeID, schema, Operation.UPSERT, pk, row, serverVersion)
        return ProtocolRecord(
            schema,
            change.copy(rowChecksum = Integrity.rowDigest(schemaHash, schema, pk, row, serverVersion).checksum),
            schemaHash,
        )
    }

    private fun protocolChangeRecordJSON(record: ProtocolRecord): String =
        Json.encodeToString(ChangeRecord.serializer(), record.change)

    private fun protocolRebuildRecordJSON(record: ProtocolRecord): String =
        Json.encodeToString(
            RebuildRecord.serializer(),
            RebuildRecord(
                table = record.change.table,
                pk = record.change.pk,
                row = requireNotNull(record.change.row),
                rowChecksum = requireNotNull(record.change.rowChecksum),
                serverVersion = record.change.serverVersion,
            ),
        )

    private fun protocolScopeChecksum(vararg records: ProtocolRecord): ChecksumObject =
        Integrity.scopeDigest(
            records.firstOrNull()?.schemaHash ?: PROTOCOL_TEST_SCHEMA_HASH,
            scopeID,
            records.map { record ->
                Integrity.rowDigest(
                    record.schemaHash,
                    record.schema,
                    record.change.pk,
                    requireNotNull(record.change.row),
                    record.change.serverVersion,
                ).let { it.identity to it.checksum }
            },
        )

    private fun emptyScopeChecksum(): ChecksumObject = protocolEmptyScopeChecksum(scopeID)

    private fun checksumJSON(checksum: ChecksumObject): String =
        Json.encodeToString(ChecksumObject.serializer(), checksum)

    @OptIn(ExperimentalSerializationApi::class)
    private fun rebuildRequestJSON(request: RebuildRequest): String = Json {
        encodeDefaults = true
        explicitNulls = false
    }.encodeToString(request)

    private fun emptyScopeChecksumJSON(): String = checksumJSON(emptyScopeChecksum())

    private fun acceptedPushOutcomeJSON(
        mutation: JsonObject,
        serverVersion: String,
        serverUpdatedAt: String = "2026-01-01T14:00:00.000000Z",
        schemaVersion: Long = 1,
        schemaHash: String = PROTOCOL_TEST_SCHEMA_HASH,
    ): String {
        val schema = protocolOrdersSchema()
        val pk = mutation.getValue("pk").jsonObject
        val columns = mutation["columns"]?.jsonObject ?: JsonObject(emptyMap())
        val id = pk.getValue("field-id")
        val serverRow = buildJsonObject {
            put("field-id", id)
            put("field-ship-address", columns["field-ship-address"] ?: JsonNull)
            put("field-user-id", columns["field-user-id"] ?: JsonPrimitive("u1"))
            put("field-updated-at", JsonPrimitive(serverUpdatedAt))
            put("field-deleted-at", JsonNull)
        }
        val outcome = AcceptedMutation(
            mutationID = mutation.getValue("mutation_id").jsonPrimitive.content,
            table = schema.tableID,
            pk = pk,
            outcomeSchema = SchemaRef(schemaVersion, schemaHash),
            status = MutationStatus.APPLIED,
            serverRow = serverRow,
            rowChecksum = Integrity.rowDigest(schemaHash, schema, pk, serverRow, serverVersion).checksum,
            serverVersion = serverVersion,
        )
        return Json.encodeToString(
            AcceptedMutation.serializer(),
            outcome,
        )
    }

    private fun rejectedPushOutcomeJSON(
        mutation: JsonObject,
        status: MutationStatus,
        code: MutationRejectionCode,
        message: String,
        serverRow: JsonObject? = null,
        serverVersion: String? = null,
    ): String = Json.encodeToString(
        RejectedMutation.serializer(),
        makeRejectedMutation(
            mutationID = mutation.getValue("mutation_id").jsonPrimitive.content,
            schema = protocolOrdersSchema(),
            pk = mutation.getValue("pk").jsonObject,
            status = status,
            code = code,
            message = message,
            serverRow = serverRow,
            serverVersion = serverVersion,
        ),
    )

    private val connectJSON = """
        {
            "server_time": "2026-01-01T12:00:00.000Z",
            "protocol_version": 3,
            "client_generation": 1,
            "scope_set_version": 1,
            "schema": {
                "version": 1,
                "hash": "$PROTOCOL_TEST_SCHEMA_HASH",
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
            "scope_cursor_updates": {},
            "schema_definition": {
                "schema_version": 1,
                "schema_hash": "$PROTOCOL_TEST_SCHEMA_HASH",
                "parent_schema": null,
                "transition_class": "initial",
                "compatibility_floor": 1,
                "tables": [
                    {
                        "table_id": "table-orders",
                        "relation_id": "relation-orders",
                        "name": "orders",
                        "primary_key_field_id": "field-id",
                        "lifecycle": {
                            "created_at_field_id": null,
                            "updated_at_field_id": "field-updated-at",
                            "deleted_at_field_id": "field-deleted-at"
                        },
                        "composition": "single_scope",
                        "fields": [
                            {"field_id":"field-id","name":"id","type":"string","nullable":false,"writable":false},
                            {"field_id":"field-ship-address","name":"ship_address","type":"string","nullable":true,"writable":true},
                            {"field_id":"field-user-id","name":"user_id","type":"string","nullable":false,"writable":true},
                            {"field_id":"field-updated-at","name":"updated_at","type":"datetime","nullable":false,"writable":false},
                            {"field_id":"field-deleted-at","name":"deleted_at","type":"datetime","nullable":true,"writable":false}
                        ],
                        "indexes": []
                    }
                ]
            }
        }
    """.trimIndent()

    private val connectResumeJSON = """
        {
            "server_time": "2026-01-01T12:00:00.000Z",
            "protocol_version": 3,
            "client_generation": 1,
            "scope_set_version": 1,
            "schema": {
                "version": 1,
                "hash": "$PROTOCOL_TEST_SCHEMA_HASH",
                "action": "none"
            },
            "scopes": {
                "add": [],
                "remove": []
            },
            "scope_cursor_updates": {}
        }
    """.trimIndent()

    private fun connectNoneJSON(
        clientGeneration: Long,
        schemaVersion: Long,
        schemaHash: String,
        scopeCursor: String? = null,
        invalidateScope: Boolean = false,
    ): String = """
        {
            "server_time": "2026-01-01T12:00:00.000Z",
            "protocol_version": 3,
            "client_generation": $clientGeneration,
            "scope_set_version": 1,
            "schema": {
                "version": $schemaVersion,
                "hash": "$schemaHash",
                "action": "none"
            },
            "scopes": {"add": [], "remove": []},
            "scope_cursor_updates": ${when {
                invalidateScope -> "{\"$scopeID\":null}"
                scopeCursor != null -> "{\"$scopeID\":\"$scopeCursor\"}"
                else -> "{}"
            }}
        }
    """.trimIndent()

    private fun connectReplacementJSON(
        clientGeneration: Long,
        schemaVersion: Long,
        schemaHash: String,
        scopeCursor: String,
    ): String = """
        {
            "server_time": "2026-01-01T12:00:00.000Z",
            "protocol_version": 3,
            "client_generation": $clientGeneration,
            "scope_set_version": 1,
            "schema": {
                "version": $schemaVersion,
                "hash": "$schemaHash",
                "action": "replace"
            },
            "scopes": {"add": [], "remove": []},
            "scope_cursor_updates": {"$scopeID": "$scopeCursor"},
            "schema_definition": {
                "schema_version": $schemaVersion,
                "schema_hash": "$schemaHash",
                "parent_schema": {"version":1,"hash":"$PROTOCOL_TEST_SCHEMA_HASH"},
                "transition_class": "class_3",
                "compatibility_floor": 2,
                "tables": [
                    {
                        "table_id": "table-orders",
                        "relation_id": "relation-orders",
                        "name": "orders",
                        "primary_key_field_id": "field-id",
                        "lifecycle": {
                            "created_at_field_id": null,
                            "updated_at_field_id": "field-updated-at",
                            "deleted_at_field_id": "field-deleted-at"
                        },
                        "composition": "single_scope",
                        "fields": [
                            {"field_id":"field-id","name":"id","type":"string","nullable":false,"writable":false},
                            {"field_id":"field-ship-address","name":"ship_address","type":"string","nullable":true,"writable":true},
                            {"field_id":"field-user-id","name":"user_id","type":"string","nullable":false,"writable":true},
                            {"field_id":"field-updated-at","name":"updated_at","type":"datetime","nullable":false,"writable":false},
                            {"field_id":"field-deleted-at","name":"deleted_at","type":"datetime","nullable":true,"writable":false}
                        ],
                        "indexes": []
                    }
                ]
            }
        }
    """.trimIndent()

    private val connectRebuildLocalJSON = """
        {
            "server_time": "2026-01-01T12:00:00.000Z",
            "protocol_version": 3,
            "client_generation": 1,
            "scope_set_version": 2,
            "schema": {
                "version": 2,
                "hash": "$connectRebuildLocalSchemaHash",
                "action": "rebuild_local"
            },
            "scopes": {
                "add": [],
                "remove": []
            },
            "scope_cursor_updates": {"$scopeID": null},
            "schema_definition": {
                "schema_version": 2,
                "schema_hash": "$connectRebuildLocalSchemaHash",
                "parent_schema": {"version":1,"hash":"$PROTOCOL_TEST_SCHEMA_HASH"},
                "transition_class": "class_3",
                "compatibility_floor": 2,
                "tables": [
                    {
                        "table_id": "table-orders",
                        "relation_id": "relation-orders",
                        "name": "orders",
                        "primary_key_field_id": "field-id",
                        "lifecycle": {
                            "created_at_field_id": null,
                            "updated_at_field_id": "field-updated-at",
                            "deleted_at_field_id": "field-deleted-at"
                        },
                        "composition": "single_scope",
                        "fields": [
                            {"field_id":"field-id","name":"id","type":"string","nullable":false,"writable":false},
                            {"field_id":"field-ship-address","name":"ship_address","type":"string","nullable":true,"writable":true},
                            {"field_id":"field-user-id","name":"user_id","type":"string","nullable":false,"writable":true},
                            {"field_id":"field-notes","name":"notes","type":"string","nullable":true,"writable":true},
                            {"field_id":"field-updated-at","name":"updated_at","type":"datetime","nullable":false,"writable":false},
                            {"field_id":"field-deleted-at","name":"deleted_at","type":"datetime","nullable":true,"writable":false}
                        ],
                        "indexes": []
                    }
                ]
            },
            "affected_scopes": ["$scopeID"]
        }
    """.trimIndent()

    private val connectRebuildLocalSchemaHash: String
        get() = Integrity.schemaManifestHash(
            protocolOrdersSchemaManifest(
                includeNotes = true,
                schemaVersion = 2,
                parentSchema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
                transitionClass = "class_3",
                compatibilityFloor = 2,
            )
        )

    private val replacementSchemaHash: String
        get() = Integrity.schemaManifestHash(
            protocolOrdersSchemaManifest(
                schemaVersion = 2,
                parentSchema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
                transitionClass = "class_3",
                compatibilityFloor = 2,
            )
        )

    private fun rebuildJSON(
        records: String = "[]",
        cursor: String? = null,
        hasMore: Boolean = false,
        finalCursor: String? = null,
        checksum: ChecksumObject = emptyScopeChecksum()
    ): String = """
        {
            "scope": "$scopeID",
            "records": $records,
            "cursor": ${cursor?.let { "\"$it\"" } ?: "null"},
            "has_more": $hasMore,
            "final_scope_cursor": ${finalCursor?.let { "\"$it\"" } ?: "null"},
            "checksum": ${if (hasMore) "null" else checksumJSON(checksum)}
        }
    """.trimIndent()

    private fun scopePullJSON(
        cursor: String,
        changes: String = "[]",
        hasMore: Boolean = false,
        rebuild: String = "[]",
        checksum: ChecksumObject = emptyScopeChecksum(),
        scopeSetVersion: Int = 1,
    ): String = """
        {
            "changes": $changes,
            "scope_set_version": $scopeSetVersion,
            "scope_cursors": {"$scopeID": "$cursor"},
            "scope_updates": {"add": [], "remove": []},
            "rebuild": $rebuild,
            "has_more": $hasMore${if (hasMore) "" else ",\n            \"checksums\": {\"$scopeID\": ${checksumJSON(checksum)}}"}
        }
    """.trimIndent()

    private fun mockResponse(body: String, statusCode: Int = 200): MockResponse =
        MockResponse().setBody(body).setResponseCode(statusCode)

    private fun ordersLocalSchemaTable(includeNotes: Boolean): LocalSchemaTable {
        return protocolOrdersSchema(includeNotes)
    }
}
