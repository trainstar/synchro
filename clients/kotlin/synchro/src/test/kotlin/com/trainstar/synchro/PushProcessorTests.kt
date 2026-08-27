package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import java.util.UUID
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonPrimitive
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.After
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Assert.assertThrows
import org.junit.Assert.fail
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28])
class PushProcessorTests {
    private val databases = TestDatabaseTracker()
    private val wireJSON = Json { encodeDefaults = true }

    private val table = SchemaTable(
        tableName = "orders",
        updatedAtColumn = "updated_at",
        deletedAtColumn = "deleted_at",
        primaryKey = listOf("id"),
        columns = listOf(
            SchemaColumn("id", logicalType = "string", nullable = false, isPrimaryKey = true),
            SchemaColumn("title", logicalType = "string"),
            SchemaColumn("enabled", logicalType = "boolean"),
            SchemaColumn("quantity", logicalType = "int"),
            SchemaColumn("large_quantity", logicalType = "int64"),
            SchemaColumn("amount", logicalType = "decimal", precision = 5, scale = 2),
            SchemaColumn("document", logicalType = "json"),
            SchemaColumn("score", logicalType = "float"),
            SchemaColumn("payload", logicalType = "bytes"),
            SchemaColumn("updated_at", logicalType = "datetime", nullable = false),
            SchemaColumn("deleted_at", logicalType = "datetime"),
        ),
    )
    private val localTable = table.localSchema

    private fun environment(): Triple<SynchroDatabase, ChangeTracker, PushProcessor> {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val database = databases.create(context)
        installTestSchema(
            database,
            SchemaResponse(1, PROTOCOL_TEST_SCHEMA_HASH, "2026-01-01T00:00:00.000000Z", listOf(table)),
        )
        val tracker = ChangeTracker(database)
        return Triple(database, tracker, PushProcessor(database, tracker))
    }

    private fun http(server: MockWebServer): HttpClient = HttpClient(
        SynchroConfig(
            dbPath = "unused",
            serverURL = server.url("/").toString().trimEnd('/'),
            authProvider = { "test-token" },
            clientID = "device-1",
            appVersion = "1.0.0",
        ),
    )

    private suspend fun sealWithRetryableFailure(
        processor: PushProcessor,
        server: MockWebServer,
    ) {
        try {
            processor.processPush(http(server), "device-1", 1, 1, PROTOCOL_TEST_SCHEMA_HASH, listOf(localTable))
            fail("expected retryable push failure")
        } catch (_: RetryableError) {
        }
    }

    private fun accepted(mutationID: String, title: String, serverVersion: String = "sv-1"): AcceptedMutation =
        makeAcceptedMutation(
            mutationID = mutationID,
            schema = localTable,
            pk = JsonObject(mapOf("id" to JsonPrimitive("o1"))),
            status = MutationStatus.APPLIED,
            serverRow = JsonObject(
                mapOf(
                    "id" to JsonPrimitive("o1"),
                    "title" to JsonPrimitive(title),
                    "enabled" to JsonNull,
                    "quantity" to JsonNull,
                    "large_quantity" to JsonNull,
                    "amount" to JsonNull,
                    "document" to JsonNull,
                    "score" to JsonNull,
                    "payload" to JsonNull,
                    "updated_at" to JsonPrimitive("2026-01-01T01:00:00.000000Z"),
                    "deleted_at" to JsonNull,
                ),
            ),
            serverVersion = serverVersion,
        )

    private fun acceptedFor(
        mutationID: String,
        recordID: String,
        title: String,
        serverVersion: String,
        schema: LocalSchemaTable = localTable,
        schemaRef: SchemaRef = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
    ): AcceptedMutation {
        val pk = JsonObject(mapOf(schema.primaryKeyFieldID to JsonPrimitive(recordID)))
        val row = JsonObject(
            schema.columns.associate { column ->
                column.fieldID to when (column.fieldID) {
                    schema.primaryKeyFieldID -> JsonPrimitive(recordID)
                    "title" -> JsonPrimitive(title)
                    "enabled", "quantity", "large_quantity", "amount", "document", "score", "payload" -> JsonNull
                    "updated_at" -> JsonPrimitive("2026-01-01T01:00:00.000000Z")
                    "deleted_at" -> JsonNull
                    else -> JsonNull
                }
            },
        )
        return AcceptedMutation(
            mutationID = mutationID,
            table = schema.tableID,
            pk = pk,
            outcomeSchema = schemaRef,
            status = MutationStatus.APPLIED,
            serverRow = row,
            rowChecksum = Integrity.rowDigest(schemaRef.hash, schema, pk, row, serverVersion).checksum,
            serverVersion = serverVersion,
        )
    }

    private fun projectionTable(
        titleName: String,
        enabledName: String,
        countName: String,
        payloadName: String,
    ): LocalSchemaTable = LocalSchemaTable(
        tableID = "table-projection-orders",
        relationID = "relation-projection-orders",
        tableName = "projection_orders",
        primaryKeyFieldID = "field-id",
        updatedAtFieldID = "field-updated-at",
        deletedAtFieldID = "field-deleted-at",
        updatedAtColumn = "updated_at",
        deletedAtColumn = "deleted_at",
        composition = CompositionClass.SINGLE_SCOPE,
        primaryKey = listOf("id"),
        columns = listOf(
            LocalSchemaColumn("field-id", "id", "string", false, false, isPrimaryKey = true),
            LocalSchemaColumn("field-title", titleName, "string", true, true, isPrimaryKey = false),
            LocalSchemaColumn("field-enabled", enabledName, "boolean", true, true, isPrimaryKey = false),
            LocalSchemaColumn("field-count", countName, "int64", true, true, isPrimaryKey = false),
            LocalSchemaColumn("field-payload", payloadName, "bytes", true, true, isPrimaryKey = false),
            LocalSchemaColumn("field-updated-at", "updated_at", "datetime", false, false, isPrimaryKey = false),
            LocalSchemaColumn("field-deleted-at", "deleted_at", "datetime", true, false, isPrimaryKey = false),
        ),
    )

    private fun installServerRow(database: SynchroDatabase, title: String, serverVersion: String) {
        database.writeSyncLockedTransaction { connection ->
            connection.execSQL(
                "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
                arrayOf("o1", title, "2026-01-01T00:00:00.000000Z"),
            )
            SynchroMeta.upsertRowVersion(connection, "orders", "o1", serverVersion, null)
        }
    }

    @After
    fun tearDown() = databases.closeAll()

    @Test
    fun sealingUsesCapturedValuesNotTheMutableApplicationRow() = runTest {
        val (database, _, processor) = environment()
        database.execute(
            "INSERT INTO orders (id, title, enabled, updated_at) VALUES (?, ?, ?, ?)",
            arrayOf("o1", "captured", 1L, "2026-01-01T00:00:00.000000Z"),
        )
        database.writeSyncLockedTransaction {
            it.execSQL("UPDATE orders SET title = 'mutable value' WHERE id = 'o1'")
        }
        val server = MockWebServer()
        server.enqueue(MockResponse().setResponseCode(503).setHeader("Retry-After", "1").setBody(RETRYABLE_503_ERROR_JSON))
        server.start()
        try {
            sealWithRetryableFailure(processor, server)
            val request = wireJSON.decodeFromString<PushRequest>(server.takeRequest().body.readUtf8())
            assertEquals("captured", (request.mutations.single().columns?.get("title") as JsonPrimitive).content)
            assertNotEquals("mutable value", (request.mutations.single().columns?.get("title") as JsonPrimitive).content)
        } finally {
            server.shutdown()
        }
    }

    @Test
    fun sealingPreservesEveryPortableValueAsItsExactWireType() = runTest {
        val (database, _, processor) = environment()
        database.execute(
            """
            INSERT INTO orders
                (id, title, enabled, quantity, large_quantity, amount, document, score, payload, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """.trimIndent(),
            arrayOf(
                "o1", null, 1L, 17L, 9_007_199_254_740_991L, "123.45", "{\"a\":1,\"b\":true}",
                1.25, byteArrayOf(0, 1, 2), "2026-01-01T00:00:00.000000Z",
            ),
        )
        val server = MockWebServer()
        server.enqueue(MockResponse().setResponseCode(503).setHeader("Retry-After", "1").setBody(RETRYABLE_503_ERROR_JSON))
        server.start()
        try {
            sealWithRetryableFailure(processor, server)
            val columns = wireJSON.decodeFromString<PushRequest>(server.takeRequest().body.readUtf8()).mutations.single().columns!!
            assertEquals(JsonNull, columns["title"])
            assertEquals("true", (columns.getValue("enabled") as JsonPrimitive).content)
            assertFalse((columns.getValue("enabled") as JsonPrimitive).isString)
            assertEquals("17", (columns.getValue("quantity") as JsonPrimitive).content)
            assertFalse((columns.getValue("quantity") as JsonPrimitive).isString)
            assertEquals("9007199254740991", (columns.getValue("large_quantity") as JsonPrimitive).content)
            assertTrue((columns.getValue("large_quantity") as JsonPrimitive).isString)
            assertEquals("123.45", (columns.getValue("amount") as JsonPrimitive).content)
            assertTrue((columns.getValue("amount") as JsonPrimitive).isString)
            assertEquals("{\"a\":1,\"b\":true}", (columns.getValue("document") as JsonPrimitive).content)
            assertTrue((columns.getValue("document") as JsonPrimitive).isString)
            assertEquals("1.25", (columns.getValue("score") as JsonPrimitive).content)
            assertFalse((columns.getValue("score") as JsonPrimitive).isString)
            assertEquals("AAEC", (columns.getValue("payload") as JsonPrimitive).content)
            assertTrue((columns.getValue("payload") as JsonPrimitive).isString)
        } finally {
            server.shutdown()
        }
    }

    @Test
    fun strictPortableValidationFailsBeforeBatchPersistenceOrNetwork() = runTest {
        val (database, _, processor) = environment()
        database.execute(
            "INSERT INTO orders (id, amount, updated_at) VALUES (?, ?, ?)",
            arrayOf("o1", "1.00", "2026-01-01T00:00:00.000000Z"),
        )
        val server = MockWebServer()
        server.start()
        try {
            val failure = runCatching {
                processor.processPush(http(server), "device-1", 1, 1, PROTOCOL_TEST_SCHEMA_HASH, listOf(localTable))
            }.exceptionOrNull()
            assertTrue(failure is SynchroError.InvalidResponse)
            assertEquals(0, server.requestCount)
            assertTrue(database.query("SELECT batch_id FROM _synchro_push_batches").isEmpty())
            assertEquals(
                "captured",
                database.queryOne("SELECT lifecycle_state FROM _synchro_pending_changes")?.get("lifecycle_state"),
            )
        } finally {
            server.shutdown()
        }
    }

    @Test
    fun crossSchemaSameRowChainDoesNotNormalizeOrSendItsSuccessor() = runTest {
        val (database, tracker, processor) = environment()
        database.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            arrayOf("o1", "authored-v1", "2026-01-01T00:00:00.000000Z"),
        )
        val first = tracker.pendingChanges().single()
        val nextHash = "1".repeat(64)
        installTestSchema(database, 2, nextHash, listOf(localTable))
        database.execute("UPDATE orders SET title = ? WHERE id = ?", arrayOf("authored-v2", "o1"))
        val second = tracker.pendingChanges().last()
        assertEquals(first.mutationID, second.dependsOnMutationID)

        val server = MockWebServer()
        server.enqueue(MockResponse().setResponseCode(503).setHeader("Retry-After", "1").setBody(RETRYABLE_503_ERROR_JSON))
        server.start()
        try {
            val failure = runCatching {
                processor.processPush(http(server), "device-1", 1, 2, nextHash, listOf(localTable))
            }.exceptionOrNull()
            assertTrue(failure is RetryableError)
            val request = wireJSON.decodeFromString<PushRequest>(server.takeRequest().body.readUtf8())
            assertEquals(listOf(first.mutationID), request.mutations.map { it.mutationID })
            assertEquals("authored-v1", request.mutations.single().columns?.get("title")?.jsonPrimitive?.content)
            val ledger = database.query(
                "SELECT mutation_id, lifecycle_state, source_kind FROM _synchro_pending_changes ORDER BY local_order",
            )
            assertEquals(listOf("sealed", "captured"), ledger.map { it.getValue("lifecycle_state") })
            assertEquals(listOf("capture", "capture"), ledger.map { it.getValue("source_kind") })
        } finally {
            server.shutdown()
        }
    }

    @Test
    fun acceptedOutcomeUsesSealedHistoricalSchemaAndReappliesTypedPatchesInOrder() = runTest {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val database = databases.create(context)
        val oldTable = projectionTable("legacy_title", "legacy_enabled", "legacy_count", "legacy_payload")
        val currentTable = projectionTable("title", "enabled", "large_count", "payload")
        val oldHash = PROTOCOL_TEST_SCHEMA_HASH
        val currentHash = "1".repeat(64)
        installTestSchema(database, 1, oldHash, listOf(oldTable))
        val tracker = ChangeTracker(database)
        val processor = PushProcessor(database, tracker)

        database.execute(
            """
            INSERT INTO projection_orders
                (id, legacy_title, legacy_enabled, legacy_count, legacy_payload, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """.trimIndent(),
            arrayOf(
                "o1", "captured", 0L, 7L, byteArrayOf(9),
                "2026-01-01T00:00:00.000000Z",
            ),
        )

        var requestCount = 0
        val requestBodies = mutableListOf<String>()
        val server = MockWebServer()
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                requestCount += 1
                val body = request.body.readUtf8()
                requestBodies += body
                if (requestCount == 1) {
                    return MockResponse().setResponseCode(503).setHeader("Retry-After", "1")
                        .setBody(RETRYABLE_503_ERROR_JSON)
                }
                val sealed = wireJSON.decodeFromString<PushRequest>(body)
                val pk = JsonObject(mapOf("field-id" to JsonPrimitive("o1")))
                val row = JsonObject(
                    mapOf(
                        "field-id" to JsonPrimitive("o1"),
                        "field-title" to JsonPrimitive("server"),
                        "field-enabled" to JsonPrimitive(true),
                        "field-count" to JsonPrimitive(Long.MAX_VALUE.toString()),
                        "field-payload" to JsonPrimitive("AAEC"),
                        "field-updated-at" to JsonPrimitive("2026-01-01T01:00:00.000000Z"),
                        "field-deleted-at" to JsonNull,
                    ),
                )
                val version = "server-v1"
                val accepted = AcceptedMutation(
                    mutationID = sealed.mutations.single().mutationID,
                    table = oldTable.tableID,
                    pk = pk,
                    outcomeSchema = SchemaRef(1, oldHash),
                    status = MutationStatus.APPLIED,
                    serverRow = row,
                    rowChecksum = Integrity.rowDigest(oldHash, oldTable, pk, row, version).checksum,
                    serverVersion = version,
                )
                return MockResponse().setBody(
                    wireJSON.encodeToString(
                        PushResponse(
                            batchID = sealed.batchID,
                            serverTime = "2026-01-01T01:00:00.000000Z",
                            accepted = listOf(accepted),
                            rejected = emptyList(),
                        ),
                    ),
                )
            }
        }
        server.start()
        try {
            val firstFailure = runCatching {
                processor.processPush(http(server), "device-1", 1, 1, oldHash, listOf(oldTable))
            }.exceptionOrNull()
            assertTrue(firstFailure is RetryableError)

            installTestSchema(database, 2, currentHash, listOf(currentTable))
            database.writeTransaction {
                it.execSQL(
                    "DELETE FROM _synchro_schema_archives WHERE schema_version = 1 AND schema_hash = ?",
                    arrayOf(oldHash),
                )
            }
            database.execute("UPDATE projection_orders SET title = ? WHERE id = ?", arrayOf("local-one", "o1"))
            database.execute("UPDATE projection_orders SET title = ? WHERE id = ?", arrayOf("local-two", "o1"))

            processor.processPush(http(server), "device-1", 1, 2, currentHash, listOf(currentTable))

            assertEquals(requestBodies[0], requestBodies[1])
            val row = database.queryOne(
                "SELECT title, enabled, large_count, payload FROM projection_orders WHERE id = ?",
                arrayOf("o1"),
            )!!
            assertEquals("local-two", row["title"])
            assertEquals(1L, row["enabled"])
            assertEquals(Long.MAX_VALUE, row["large_count"])
            assertArrayEquals(byteArrayOf(0, 1, 2), row["payload"] as ByteArray)
            assertEquals(
                "completed",
                database.queryOne("SELECT state FROM _synchro_push_batches")?.get("state"),
            )
        } finally {
            server.shutdown()
        }
    }

    @Test
    fun unsafeCurrentProjectionRetainsOutcomeAndInvalidatesAffectedScope() {
        val (database, tracker, processor) = environment()
        database.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            arrayOf("o1", "local", "2026-01-01T00:00:00.000000Z"),
        )
        val source = tracker.pendingChanges().single()
        database.writeTransaction { db ->
            SynchroMeta.upsertScope(db, "orders:user-1", "cursor-1", "checksum-1")
            SynchroMeta.upsertScopeRow(db, "orders:user-1", "orders", "o1", "row-checksum", 0)
        }
        val unsafeCurrent = localTable.copy(
            columns = localTable.columns.map { column ->
                if (column.fieldID == "title") column.copy(name = "title_number", logicalType = "int") else column
            },
        )

        processor.applyAccepted(listOf(accepted(source.mutationID, "server", "server-v1")), listOf(unsafeCurrent))

        assertEquals(
            "accepted",
            database.queryOne("SELECT lifecycle_state FROM _synchro_pending_changes")?.get("lifecycle_state"),
        )
        assertTrue(
            database.queryOne("SELECT accepted_outcome_json FROM _synchro_pending_changes")
                ?.get("accepted_outcome_json") is String,
        )
        assertEquals("local", database.queryOne("SELECT title FROM orders WHERE id = 'o1'")?.get("title"))
        val scope = database.readTransaction { SynchroMeta.getScope(it, "orders:user-1") }!!
        assertNull(scope.cursor)
        assertEquals(1L, scope.generation)
    }

    @Test
    fun rowChecksumFailureRollsBackEveryOutcomeInTheTransaction() {
        val (database, tracker, processor) = environment()
        database.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            arrayOf("o1", "local-one", "2026-01-01T00:00:00.000000Z"),
        )
        database.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            arrayOf("o2", "local-two", "2026-01-01T00:00:00.000000Z"),
        )
        val changes = tracker.pendingChanges()
        val first = acceptedFor(changes[0].mutationID, "o1", "server-one", "server-v1")
        val second = acceptedFor(changes[1].mutationID, "o2", "server-two", "server-v2")
            .copy(rowChecksum = ChecksumObject("sha256", 1, "hex", "f".repeat(64)))

        val failure = runCatching { processor.applyAccepted(listOf(first, second), listOf(localTable)) }.exceptionOrNull()
        assertTrue(failure is SynchroError.InvalidResponse)
        assertEquals(
            listOf("captured", "captured"),
            database.query("SELECT lifecycle_state FROM _synchro_pending_changes ORDER BY local_order")
                .map { it.getValue("lifecycle_state") },
        )
        assertEquals(
            listOf("local-one", "local-two"),
            database.query("SELECT title FROM orders ORDER BY id").map { it.getValue("title") },
        )
        assertTrue(database.query("SELECT * FROM _synchro_row_versions").isEmpty())
    }

    @Test
    fun primaryKeyUpdateRollsBackTheApplicationRowAndLedger() {
        val (database, _, _) = environment()
        database.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            arrayOf("o1", "local", "2026-01-01T00:00:00.000000Z"),
        )
        val rowsBefore = database.query("SELECT * FROM orders ORDER BY id")
        val ledgerBefore = database.query("SELECT * FROM _synchro_pending_changes ORDER BY local_order")
        val valuesBefore = database.query("SELECT * FROM _synchro_mutation_values ORDER BY mutation_id, field_id")

        assertThrows(android.database.sqlite.SQLiteException::class.java) {
            database.execute("UPDATE orders SET id = ? WHERE id = ?", arrayOf("o2", "o1"))
        }

        assertEquals(rowsBefore, database.query("SELECT * FROM orders ORDER BY id"))
        assertEquals(ledgerBefore, database.query("SELECT * FROM _synchro_pending_changes ORDER BY local_order"))
        assertEquals(valuesBefore, database.query("SELECT * FROM _synchro_mutation_values ORDER BY mutation_id, field_id"))
    }

    @Test
    fun normalizationSupersedesSourcesAndMergesInsertUpdates() = runTest {
        val (database, _, processor) = environment()
        database.execute(
            "INSERT INTO orders (id, title, enabled, updated_at) VALUES (?, ?, ?, ?)",
            arrayOf("o1", "first", 0L, "2026-01-01T00:00:00.000000Z"),
        )
        database.execute("UPDATE orders SET title = ?, enabled = ? WHERE id = ?", arrayOf("last", 1L, "o1"))
        val server = MockWebServer()
        server.enqueue(MockResponse().setResponseCode(503).setHeader("Retry-After", "1").setBody(RETRYABLE_503_ERROR_JSON))
        server.start()
        try {
            sealWithRetryableFailure(processor, server)
            val request = wireJSON.decodeFromString<PushRequest>(server.takeRequest().body.readUtf8())
            assertEquals(1, request.mutations.size)
            assertEquals(Operation.INSERT, request.mutations.single().op)
            assertEquals("last", (request.mutations.single().columns?.get("title") as JsonPrimitive).content)
            assertEquals("true", (request.mutations.single().columns?.get("enabled") as JsonPrimitive).content)
            val records = database.query(
                "SELECT mutation_id, lifecycle_state, normalized_mutation_id FROM _synchro_pending_changes ORDER BY local_order",
            )
            assertEquals(listOf("superseded_before_send", "superseded_before_send", "sealed"), records.map { it.getValue("lifecycle_state") })
            assertEquals(records[0]["normalized_mutation_id"], records[1]["normalized_mutation_id"])
            assertEquals(records[2]["mutation_id"], records[0]["normalized_mutation_id"])
        } finally {
            server.shutdown()
        }
    }

    @Test
    fun insertDeleteCancelsDurablyWithoutNetworkMutation() = runTest {
        val (database, tracker, processor) = environment()
        database.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            arrayOf("o1", "temporary", "2026-01-01T00:00:00.000000Z"),
        )
        database.execute("DELETE FROM orders WHERE id = ?", arrayOf("o1"))
        val server = MockWebServer()
        server.start()
        try {
            assertNull(processor.processPush(http(server), "device-1", 1, 1, PROTOCOL_TEST_SCHEMA_HASH, listOf(localTable)))
            assertFalse(tracker.hasPendingChanges())
            val records = database.query("SELECT lifecycle_state, normalized_mutation_id FROM _synchro_pending_changes ORDER BY local_order")
            assertEquals(listOf("cancelled_before_send", "cancelled_before_send"), records.map { it.getValue("lifecycle_state") })
            assertEquals(records[0]["normalized_mutation_id"], records[1]["normalized_mutation_id"])
        } finally {
            server.shutdown()
        }
    }

    @Test
    fun sealedPredecessorPreventsSuccessorTransmissionAndOnlyRebasesUnsealedSuccessor() = runTest {
        val (database, tracker, processor) = environment()
        database.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            arrayOf("o1", "first", "2026-01-01T00:00:00.000000Z"),
        )
        val server = MockWebServer()
        server.enqueue(MockResponse().setResponseCode(503).setHeader("Retry-After", "1").setBody(RETRYABLE_503_ERROR_JSON))
        server.enqueue(MockResponse().setResponseCode(503).setHeader("Retry-After", "1").setBody(RETRYABLE_503_ERROR_JSON))
        server.start()
        try {
            sealWithRetryableFailure(processor, server)
            val predecessor = database.queryOne("SELECT mutation_id FROM _synchro_pending_changes")?.get("mutation_id") as String
            database.execute("UPDATE orders SET title = ? WHERE id = ?", arrayOf("successor", "o1"))
            val successor = tracker.pendingChanges().single()
            assertEquals(predecessor, successor.dependsOnMutationID)
            sealWithRetryableFailure(processor, server)
            val first = server.takeRequest().body.readUtf8()
            val second = server.takeRequest().body.readUtf8()
            assertEquals(first, second)
            assertEquals(1, wireJSON.decodeFromString<PushRequest>(second).mutations.size)

            processor.applyAccepted(listOf(accepted(predecessor, "first", "sv-accepted")), listOf(localTable))
            val refreshed = tracker.pendingChanges().single()
            assertEquals("sv-accepted", refreshed.baseUpdatedAt)
            assertNull(refreshed.dependsOnMutationID)
            assertEquals("accepted", database.queryOne("SELECT lifecycle_state FROM _synchro_pending_changes WHERE mutation_id = ?", arrayOf(predecessor))?.get("lifecycle_state"))
        } finally {
            server.shutdown()
        }
    }

    @Test
    fun sealedPredecessorCaptureOmitsStaleBaseAndAcceptanceRefreshesOnlyTheUnsealedSuccessor() = runTest {
        val (database, tracker, processor) = environment()
        installServerRow(database, "server", "sv-start")
        database.execute("UPDATE orders SET title = ? WHERE id = ?", arrayOf("predecessor", "o1"))
        val predecessor = tracker.pendingChanges().single()
        assertEquals("sv-start", predecessor.baseUpdatedAt)

        val server = MockWebServer()
        server.enqueue(MockResponse().setResponseCode(503).setHeader("Retry-After", "1").setBody(RETRYABLE_503_ERROR_JSON))
        server.start()
        try {
            sealWithRetryableFailure(processor, server)
            database.execute("UPDATE orders SET title = ? WHERE id = ?", arrayOf("successor", "o1"))
            val successor = tracker.pendingChanges().single()
            assertEquals(predecessor.mutationID, successor.dependsOnMutationID)
            assertNull(successor.baseUpdatedAt)

            processor.applyAccepted(listOf(accepted(predecessor.mutationID, "predecessor", "sv-accepted")), listOf(localTable))
            val refreshed = tracker.pendingChanges().single()
            assertEquals("sv-accepted", refreshed.baseUpdatedAt)
            assertNull(refreshed.dependsOnMutationID)
            assertEquals("captured", refreshed.lifecycleState)
        } finally {
            server.shutdown()
        }
    }

    @Test
    fun acceptedDeleteFencePreservesLaterProjectionAndStoresReturnedVersion() {
        val (database, tracker, processor) = environment()
        installServerRow(database, "server", "sv-start")
        database.execute("DELETE FROM orders WHERE id = ?", arrayOf("o1"))
        val predecessor = tracker.pendingChanges().single()
        database.execute("UPDATE orders SET title = ? WHERE id = ?", arrayOf("later local", "o1"))

        processor.applyAccepted(
            listOf(
                AcceptedMutation(
                    mutationID = predecessor.mutationID,
                    table = localTable.tableID,
                    pk = JsonObject(mapOf("id" to JsonPrimitive("o1"))),
                    outcomeSchema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
                    status = MutationStatus.APPLIED,
                    serverVersion = "delete-fence",
                ),
            ),
            listOf(localTable),
            mapOf(predecessor.mutationID to predecessor),
        )

        assertEquals("later local", database.queryOne("SELECT title FROM orders WHERE id = 'o1'")?.get("title"))
        assertEquals("delete-fence", database.readTransaction { SynchroMeta.getRowVersion(it, "orders", "o1") })
        assertEquals("delete-fence", tracker.pendingChanges().single().baseUpdatedAt)
    }

    @Test
    fun rejectedDeleteFencePreservesLaterProjectionAndStoresReturnedVersion() {
        val (database, tracker, processor) = environment()
        installServerRow(database, "server", "sv-start")
        database.execute("DELETE FROM orders WHERE id = ?", arrayOf("o1"))
        val predecessor = tracker.pendingChanges().single()
        database.execute("UPDATE orders SET title = ? WHERE id = ?", arrayOf("later local", "o1"))
        val successor = tracker.pendingChanges().last()

        processor.applyRejected(
            listOf(
                RejectedMutation(
                    mutationID = predecessor.mutationID,
                    table = localTable.tableID,
                    pk = JsonObject(mapOf("id" to JsonPrimitive("o1"))),
                    outcomeSchema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
                    status = MutationStatus.CONFLICT,
                    code = MutationRejectionCode.ROW_DELETED,
                    message = "row was deleted",
                    serverVersion = "delete-fence",
                ),
            ),
            listOf(localTable),
            mapOf(predecessor.mutationID to predecessor),
        )

        assertEquals("later local", database.queryOne("SELECT title FROM orders WHERE id = 'o1'")?.get("title"))
        assertEquals("delete-fence", database.readTransaction { SynchroMeta.getRowVersion(it, "orders", "o1") })
        val blocked = database.queryOne(
            "SELECT lifecycle_state, base_version FROM _synchro_pending_changes WHERE mutation_id = ?",
            arrayOf(successor.mutationID),
        )!!
        assertEquals("blocked_by_predecessor", blocked["lifecycle_state"])
        assertNull(blocked["base_version"])
    }

    @Test
    fun rejectedSealedPredecessorRetainsNullBaseAndBlocksItsSuccessor() = runTest {
        val (database, tracker, processor) = environment()
        installServerRow(database, "server", "sv-start")
        database.execute("UPDATE orders SET title = ? WHERE id = ?", arrayOf("predecessor", "o1"))
        val predecessor = tracker.pendingChanges().single()

        val server = MockWebServer()
        server.enqueue(MockResponse().setResponseCode(503).setHeader("Retry-After", "1").setBody(RETRYABLE_503_ERROR_JSON))
        server.start()
        try {
            sealWithRetryableFailure(processor, server)
            database.execute("UPDATE orders SET title = ? WHERE id = ?", arrayOf("successor", "o1"))
            val successor = tracker.pendingChanges().single()
            assertNull(successor.baseUpdatedAt)

            processor.applyRejected(
                listOf(
                    RejectedMutation(
                        mutationID = predecessor.mutationID,
                        table = localTable.tableID,
                        pk = JsonObject(mapOf("id" to JsonPrimitive("o1"))),
                        outcomeSchema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
                        status = MutationStatus.REJECTED_TERMINAL,
                        code = MutationRejectionCode.POLICY_REJECTED,
                        message = "not allowed",
                    ),
                ),
                listOf(localTable),
            )
            val blocked = database.queryOne(
                "SELECT lifecycle_state, base_version, depends_on_mutation_id FROM _synchro_pending_changes WHERE mutation_id = ?",
                arrayOf(successor.mutationID),
            )!!
            assertEquals("blocked_by_predecessor", blocked["lifecycle_state"])
            assertNull(blocked["base_version"])
            assertEquals(predecessor.mutationID, blocked["depends_on_mutation_id"])
        } finally {
            server.shutdown()
        }
    }

    @Test
    fun acceptedPredecessorNeverRebasesAnAlreadySealedSuccessor() {
        val (database, tracker, processor) = environment()
        database.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            arrayOf("o1", "first", "2026-01-01T00:00:00.000000Z"),
        )
        val predecessor = tracker.pendingChanges().single()
        database.execute("UPDATE orders SET title = ? WHERE id = ?", arrayOf("successor", "o1"))
        val successor = tracker.pendingChanges().last()
        database.writeTransaction {
            it.execSQL(
                """
                UPDATE _synchro_pending_changes
                SET lifecycle_state = 'sealed', sealed_batch_id = 'synthetic-successor-batch', sealed_ordinal = 0
                WHERE mutation_id = ?
                """.trimIndent(),
                arrayOf(successor.mutationID),
            )
        }

        processor.applyAccepted(listOf(accepted(predecessor.mutationID, "first", "sv-predecessor")), listOf(localTable))
        val sealed = database.queryOne(
            "SELECT lifecycle_state, base_version, depends_on_mutation_id FROM _synchro_pending_changes WHERE mutation_id = ?",
            arrayOf(successor.mutationID),
        )!!
        assertEquals("sealed", sealed["lifecycle_state"])
        assertNull(sealed["base_version"])
        assertEquals(predecessor.mutationID, sealed["depends_on_mutation_id"])
    }

    @Test
    fun exactAcceptedReplayRetainsTheTerminalRecordWithoutReapplyingTheRow() {
        val (database, tracker, processor) = environment()
        database.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            arrayOf("o1", "local", "2026-01-01T00:00:00.000000Z"),
        )
        val outcome = accepted(tracker.pendingChanges().single().mutationID, "canonical", "sv-canonical")
        processor.applyAccepted(listOf(outcome), listOf(localTable))
        val storedOutcome = database.queryOne(
            "SELECT accepted_outcome_json FROM _synchro_pending_changes WHERE mutation_id = ?",
            arrayOf(outcome.mutationID),
        )?.get("accepted_outcome_json")
        database.writeSyncLockedTransaction {
            it.execSQL("UPDATE orders SET title = 'later local projection' WHERE id = 'o1'")
        }
        processor.applyAccepted(listOf(outcome), listOf(localTable))
        assertEquals(
            "later local projection",
            database.queryOne("SELECT title FROM orders WHERE id = 'o1'")?.get("title"),
        )
        assertEquals(storedOutcome, database.queryOne(
            "SELECT accepted_outcome_json FROM _synchro_pending_changes WHERE mutation_id = ?",
            arrayOf(outcome.mutationID),
        )?.get("accepted_outcome_json"))
    }

    @Test
    fun reconciliationRetainsExactLedgerAndCompleteTerminalRejection() {
        val (database, tracker, processor) = environment()
        database.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            arrayOf("o1", "blocked", "2026-01-01T00:00:00.000000Z"),
        )
        val mutationID = tracker.pendingChanges().single().mutationID
        val rejection = RejectedMutation(
            mutationID = mutationID,
            table = localTable.tableID,
            pk = JsonObject(mapOf("id" to JsonPrimitive("o1"))),
            outcomeSchema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
            status = MutationStatus.REJECTED_TERMINAL,
            code = MutationRejectionCode.SCHEMA_INCOMPATIBLE,
            message = "retained field is incompatible",
            retryable = false,
            authoredSchema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
            currentSchema = SchemaRef(2, "1".repeat(64)),
            incompatibleFieldIDs = listOf("title"),
        )
        processor.applyRejected(listOf(rejection), listOf(localTable))

        assertFalse(tracker.hasPendingChanges())
        assertEquals("rejected_terminal", database.queryOne("SELECT lifecycle_state FROM _synchro_pending_changes WHERE mutation_id = ?", arrayOf(mutationID))?.get("lifecycle_state"))
        val rejected = database.readTransaction { SynchroMeta.listRejectedMutations(it).single() }
        assertTrue(rejected.mutationJSON!!.contains(mutationID))
        assertTrue(rejected.mutationJSON!!.contains("title"))
        assertTrue(rejected.rejectionJSON!!.contains("schema_incompatible"))
        assertTrue(rejected.rejectionJSON!!.contains("incompatible_field_ids"))

        processor.applyRejected(listOf(rejection), listOf(localTable))
        assertEquals("rejected_terminal", database.queryOne(
            "SELECT lifecycle_state FROM _synchro_pending_changes WHERE mutation_id = ?",
            arrayOf(mutationID),
        )?.get("lifecycle_state"))
        val different = rejection.copy(message = "different outcome")
        assertTrue(runCatching { processor.applyRejected(listOf(different), listOf(localTable)) }.isFailure)
    }

    @Test
    fun conflictOutcomeAndExactReplayRemainInspectable() {
        val (database, tracker, processor) = environment()
        database.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            arrayOf("o1", "local", "2026-01-01T00:00:00.000000Z"),
        )
        val mutationID = tracker.pendingChanges().single().mutationID
        val row = JsonObject(
            mapOf(
                "id" to JsonPrimitive("o1"),
                "title" to JsonPrimitive("server"),
                "enabled" to JsonNull,
                "quantity" to JsonNull,
                "large_quantity" to JsonNull,
                "amount" to JsonNull,
                "document" to JsonNull,
                "score" to JsonNull,
                "payload" to JsonNull,
                "updated_at" to JsonPrimitive("2026-01-01T01:00:00.000000Z"),
                "deleted_at" to JsonNull,
            ),
        )
        val outcome = makeRejectedMutation(
            mutationID = mutationID,
            schema = localTable,
            pk = JsonObject(mapOf("id" to JsonPrimitive("o1"))),
            status = MutationStatus.CONFLICT,
            code = MutationRejectionCode.VERSION_CONFLICT,
            message = "server changed",
            serverRow = row,
            serverVersion = "sv-conflict",
        )
        processor.applyRejected(listOf(outcome), listOf(localTable))
        processor.applyRejected(listOf(outcome), listOf(localTable))
        assertEquals("conflict", database.queryOne(
            "SELECT lifecycle_state FROM _synchro_pending_changes WHERE mutation_id = ?",
            arrayOf(mutationID),
        )?.get("lifecycle_state"))
        val persisted = database.readTransaction { SynchroMeta.listRejectedMutations(it).single() }
        assertTrue(persisted.mutationJSON!!.contains(mutationID))
        assertTrue(persisted.rejectionJSON!!.contains("version_conflict"))
    }

    @Test
    fun terminalRejectionRecursivelyBlocksEveryDependentWithoutDeletingIntent() {
        val (database, tracker, processor) = environment()
        database.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            arrayOf("o1", "first", "2026-01-01T00:00:00.000000Z"),
        )
        val predecessor = tracker.pendingChanges().single()
        database.execute("UPDATE orders SET title = ? WHERE id = ?", arrayOf("dependent", "o1"))
        database.execute("UPDATE orders SET title = ? WHERE id = ?", arrayOf("descendant", "o1"))
        val successors = tracker.pendingChanges().drop(1)
        val rejection = RejectedMutation(
            mutationID = predecessor.mutationID,
            table = localTable.tableID,
            pk = JsonObject(mapOf("id" to JsonPrimitive("o1"))),
            outcomeSchema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
            status = MutationStatus.REJECTED_TERMINAL,
            code = MutationRejectionCode.POLICY_REJECTED,
            message = "not allowed",
        )
        processor.applyRejected(listOf(rejection), listOf(localTable))
        assertEquals(
            listOf("blocked_by_predecessor", "blocked_by_predecessor"),
            successors.map { successor ->
                database.queryOne(
                    "SELECT lifecycle_state FROM _synchro_pending_changes WHERE mutation_id = ?",
                    arrayOf(successor.mutationID),
                )?.get("lifecycle_state")
            },
        )
        assertEquals(3L, database.queryOne("SELECT COUNT(*) AS count FROM _synchro_pending_changes")?.get("count"))
    }

    @Test
    fun malformedSchemaIncompatibleOutcomeLeavesTheSealedBatchUntouched() = runTest {
        val (database, tracker, processor) = environment()
        database.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            arrayOf("o1", "local", "2026-01-01T00:00:00.000000Z"),
        )
        val mutationID = tracker.pendingChanges().single().mutationID
        val server = MockWebServer()
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val sealed = wireJSON.decodeFromString<PushRequest>(request.body.readUtf8())
                val malformed = RejectedMutation(
                    mutationID = mutationID,
                    table = localTable.tableID,
                    pk = JsonObject(mapOf("id" to JsonPrimitive("o1"))),
                    outcomeSchema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
                    status = MutationStatus.REJECTED_TERMINAL,
                    code = MutationRejectionCode.SCHEMA_INCOMPATIBLE,
                    message = "missing required schema bindings",
                    retryable = false,
                )
                return MockResponse().setBody(
                    wireJSON.encodeToString(
                        PushResponse(
                            batchID = sealed.batchID,
                            serverTime = "2026-01-01T01:00:00.000000Z",
                            accepted = emptyList(),
                            rejected = listOf(malformed),
                        ),
                    ),
                )
            }
        }
        server.start()
        try {
            assertTrue(
                runCatching {
                    processor.processPush(http(server), "device-1", 1, 1, PROTOCOL_TEST_SCHEMA_HASH, listOf(localTable))
                }.isFailure,
            )
            val ledger = database.queryOne(
                "SELECT lifecycle_state, accepted_outcome_json, rejected_outcome_json FROM _synchro_pending_changes",
            )!!
            assertEquals("sealed", ledger["lifecycle_state"])
            assertNull(ledger["accepted_outcome_json"])
            assertNull(ledger["rejected_outcome_json"])
            assertEquals("pending", database.queryOne("SELECT state FROM _synchro_push_batches")?.get("state"))
            assertTrue(database.query("SELECT * FROM _synchro_rejected_mutations").isEmpty())
            assertEquals("local", database.queryOne("SELECT title FROM orders WHERE id = 'o1'")?.get("title"))
        } finally {
            server.shutdown()
        }
    }

    @Test
    fun responseOutcomeOrderMismatchFailsBeforeAnyOutcomeIsRecorded() = runTest {
        val (database, _, processor) = environment()
        database.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            arrayOf("o1", "local-one", "2026-01-01T00:00:00.000000Z"),
        )
        database.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            arrayOf("o2", "local-two", "2026-01-01T00:00:00.000000Z"),
        )
        val server = MockWebServer()
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val sealed = wireJSON.decodeFromString<PushRequest>(request.body.readUtf8())
                val accepted = sealed.mutations.mapIndexed { index, mutation ->
                    val recordID = (mutation.pk.getValue("id") as JsonPrimitive).content
                    acceptedFor(mutation.mutationID, recordID, "server-$index", "server-$index")
                }.reversed()
                return MockResponse().setBody(
                    wireJSON.encodeToString(
                        PushResponse(
                            batchID = sealed.batchID,
                            serverTime = "2026-01-01T01:00:00.000000Z",
                            accepted = accepted,
                            rejected = emptyList(),
                        ),
                    ),
                )
            }
        }
        server.start()
        try {
            assertTrue(
                runCatching {
                    processor.processPush(http(server), "device-1", 1, 1, PROTOCOL_TEST_SCHEMA_HASH, listOf(localTable))
                }.isFailure,
            )
            val ledger = database.query(
                "SELECT lifecycle_state, accepted_outcome_json, rejected_outcome_json FROM _synchro_pending_changes ORDER BY local_order",
            )
            assertEquals(listOf("sealed", "sealed"), ledger.map { it.getValue("lifecycle_state") })
            assertTrue(ledger.all { it["accepted_outcome_json"] == null && it["rejected_outcome_json"] == null })
            assertEquals(
                listOf("local-one", "local-two"),
                database.query("SELECT title FROM orders ORDER BY id").map { it.getValue("title") },
            )
            assertEquals("pending", database.queryOne("SELECT state FROM _synchro_push_batches")?.get("state"))
        } finally {
            server.shutdown()
        }
    }

    @Test
    fun renewalRejectsAnUnchangedBindingAndKeepsTheRetiredBatchUnsendable() = runTest {
        val (database, _, processor) = environment()
        database.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            arrayOf("o1", "local", "2026-01-01T00:00:00.000000Z"),
        )
        val server = MockWebServer()
        server.enqueue(
            MockResponse().setResponseCode(409).setBody(
                """
                {"error":{"code":"client_generation_expired","message":"generation expired","retryable":false,"current_client_generation":2}}
                """.trimIndent(),
            ),
        )
        server.start()
        try {
            assertTrue(
                runCatching {
                    processor.processPush(http(server), "device-1", 1, 1, PROTOCOL_TEST_SCHEMA_HASH, listOf(localTable))
                }.exceptionOrNull() is PushRenewalRequiredException,
            )
            assertTrue(
                runCatching {
                    processor.renewRequiredBatches("device-1", 1, 1, PROTOCOL_TEST_SCHEMA_HASH, listOf(localTable))
                }.exceptionOrNull() is SynchroError.InvalidResponse,
            )
            assertEquals(
                listOf("renewal_required"),
                database.query("SELECT state FROM _synchro_push_batches").map { it.getValue("state") },
            )
            assertEquals(
                "sealed",
                database.queryOne("SELECT lifecycle_state FROM _synchro_pending_changes")?.get("lifecycle_state"),
            )
            assertEquals(1, server.requestCount)
        } finally {
            server.shutdown()
        }
    }

    @Test
    fun sealedRequestSurvivesRestartWithExactStoredJSON() = runTest {
        val (database, _, processor) = environment()
        database.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            arrayOf("o1", "restart", "2026-01-01T00:00:00.000000Z"),
        )
        val requests = mutableListOf<String>()
        var first = true
        val server = MockWebServer()
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                requests += request.body.readUtf8()
                if (first) {
                    first = false
                    return MockResponse().setResponseCode(503).setHeader("Retry-After", "1").setBody(RETRYABLE_503_ERROR_JSON)
                }
                val sealed = wireJSON.decodeFromString<PushRequest>(requests.last())
                return MockResponse().setBody(
                    wireJSON.encodeToString(
                        PushResponse(
                            batchID = sealed.batchID,
                            serverTime = "2026-01-01T01:00:00.000000Z",
                            accepted = listOf(accepted(sealed.mutations.single().mutationID, "restart", "sv-restart")),
                            rejected = emptyList(),
                        ),
                    ),
                )
            }
        }
        server.start()
        try {
            sealWithRetryableFailure(processor, server)
            val path = database.path
            database.close()
            val reopened = databases.open(ApplicationProvider.getApplicationContext<Context>(), path)
            val restarted = PushProcessor(reopened, ChangeTracker(reopened))
            restarted.processPush(http(server), "device-1", 1, 1, PROTOCOL_TEST_SCHEMA_HASH, listOf(localTable))
            assertEquals(2, requests.size)
            assertEquals(requests[0], requests[1])
            assertEquals("accepted", reopened.queryOne("SELECT lifecycle_state FROM _synchro_pending_changes")?.get("lifecycle_state"))
        } finally {
            server.shutdown()
        }
    }

    @Test
    fun pushOutcomeAndMatchingBackoffResolveInOneTransaction() = runTest {
        val (database, _, processor) = environment()
        database.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            arrayOf("o1", "transactional", "2026-01-01T00:00:00.000000Z"),
        )
        val server = MockWebServer()
        server.enqueue(
            MockResponse().setResponseCode(503).setHeader("Retry-After", "1")
                .setBody(RETRYABLE_503_ERROR_JSON)
        )
        server.start()
        try {
            sealWithRetryableFailure(processor, server)
            val request = wireJSON.decodeFromString<PushRequest>(server.takeRequest().body.readUtf8())
            val responseJSON = wireJSON.encodeToString(
                PushResponse(
                    batchID = request.batchID,
                    serverTime = "2026-01-01T01:00:00.000000Z",
                    accepted = listOf(accepted(request.mutations.single().mutationID, "server")),
                    rejected = emptyList(),
                ),
            )
            installDurableBackoff(database, RetryOperation.PUSHING, request.batchID)
            database.execute(
                """
                CREATE TRIGGER fail_push_backoff_resolution
                BEFORE DELETE ON _synchro_backoff
                BEGIN
                    SELECT RAISE(ABORT, 'forced backoff resolution failure');
                END
                """.trimIndent(),
            )
            server.enqueue(MockResponse().setBody(responseJSON))

            val failure = runCatching {
                processor.processPush(
                    http(server),
                    "device-1",
                    1,
                    1,
                    PROTOCOL_TEST_SCHEMA_HASH,
                    listOf(localTable),
                )
            }.exceptionOrNull()
            assertTrue(failure is android.database.sqlite.SQLiteException)
            assertEquals(
                "pending",
                database.queryOne("SELECT state FROM _synchro_push_batches WHERE batch_id = ?", arrayOf(request.batchID))
                    ?.get("state"),
            )
            assertEquals(
                "sealed",
                database.queryOne("SELECT lifecycle_state FROM _synchro_pending_changes")?.get("lifecycle_state"),
            )
            assertNotNull(DurableBackoffStore.load(database))

            database.execute("DROP TRIGGER fail_push_backoff_resolution")
            server.enqueue(MockResponse().setBody(responseJSON))
            processor.processPush(
                http(server),
                "device-1",
                1,
                1,
                PROTOCOL_TEST_SCHEMA_HASH,
                listOf(localTable),
            )

            assertEquals(
                "completed",
                database.queryOne("SELECT state FROM _synchro_push_batches WHERE batch_id = ?", arrayOf(request.batchID))
                    ?.get("state"),
            )
            assertNull(DurableBackoffStore.load(database))
        } finally {
            server.shutdown()
        }
    }
}
