package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import java.util.UUID
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.ExperimentalSerializationApi
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
import okhttp3.mockwebserver.SocketPolicy
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

    private fun installServerRow(
        database: SynchroDatabase,
        title: String,
        serverVersion: String,
        recordID: String = "o1",
    ) {
        database.writeSyncLockedTransaction { connection ->
            connection.execSQL(
                "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
                arrayOf(recordID, title, "2026-01-01T00:00:00.000000Z"),
            )
            SynchroMeta.upsertRowVersion(connection, "orders", recordID, serverVersion, null)
        }
    }

    @OptIn(ExperimentalSerializationApi::class)
    private val pushJSON = Json {
        ignoreUnknownKeys = false
        encodeDefaults = true
        explicitNulls = false
    }

    private fun insertOrder(database: SynchroDatabase, id: String, title: String, score: Double? = null) {
        if (score == null) {
            database.execute(
                "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
                arrayOf(id, title, "2026-01-01T00:00:00.000000Z"),
            )
        } else {
            database.execute(
                "INSERT INTO orders (id, title, score, updated_at) VALUES (?, ?, ?, ?)",
                arrayOf(id, title, score, "2026-01-01T00:00:00.000000Z"),
            )
        }
    }

    private fun ledgerID(database: SynchroDatabase, recordID: String, operation: String = "insert"): String =
        database.queryOne(
            "SELECT mutation_id FROM _synchro_pending_changes WHERE record_id = ? AND operation = ?",
            arrayOf(recordID, operation),
        )!!.getValue("mutation_id") as String

    private fun lifecycleState(database: SynchroDatabase, mutationID: String): Any? =
        database.queryOne(
            "SELECT lifecycle_state FROM _synchro_pending_changes WHERE mutation_id = ?",
            arrayOf(mutationID),
        )?.get("lifecycle_state")

    private fun octets(text: String): Int = text.toByteArray(Charsets.UTF_8).size

    private fun canonicalOctets(body: String): Int = octets(Integrity.canonicalJSON(Json.parseToJsonElement(body)))

    /** Measures the complete request with the largest generation and schema version that a successor can carry. */
    private fun reservedOctets(request: PushRequest, clientID: String): Pair<Int, Int> {
        val body = pushJSON.encodeToString(
            request.copy(
                clientID = clientID,
                clientGeneration = 9_007_199_254_740_991L,
                schema = SchemaRef(9_007_199_254_740_991L, request.schema.hash),
            ),
        )
        return octets(body) to canonicalOctets(body)
    }

    private fun retryableResponse(): MockResponse =
        MockResponse().setResponseCode(503).setHeader("Retry-After", "1").setBody(RETRYABLE_503_ERROR_JSON)

    /** Seals one insert in a new database and returns the sent request. */
    private suspend fun sealedInsert(id: String = "o0", title: String = "", score: Double? = null): PushRequest {
        val (database, _, processor) = environment()
        insertOrder(database, id, title, score)
        val server = MockWebServer()
        server.enqueue(retryableResponse())
        server.start()
        try {
            sealWithRetryableFailure(processor, server)
            return pushJSON.decodeFromString<PushRequest>(server.takeRequest().body.readUtf8())
        } finally {
            server.shutdown()
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
    fun historicalSchemaValidationCacheDoesNotSurviveFailedTransport() = runTest {
        val (database, _, processor) = environment()
        database.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            arrayOf("o1", "first", "2026-01-01T00:00:00.000000Z"),
        )
        database.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            arrayOf("o2", "second", "2026-01-01T00:00:00.000000Z"),
        )
        val currentHash = "1".repeat(64)
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
            val original = wireJSON.decodeFromString<PushRequest>(server.takeRequest().body.readUtf8())
            assertEquals(2, original.mutations.size)

            installTestSchema(database, 2, currentHash, listOf(localTable))
            assertTrue(processor.renewRequiredBatches("device-1", 2, 2, currentHash, listOf(localTable)))
            server.enqueue(
                MockResponse()
                    .setBody("partial response")
                    .setSocketPolicy(SocketPolicy.DISCONNECT_DURING_RESPONSE_BODY),
            )

            val transportFailure = runCatching {
                processor.processPush(http(server), "device-1", 2, 2, currentHash, listOf(localTable))
            }.exceptionOrNull()
            assertTrue(transportFailure is RetryableError)
            val renewed = wireJSON.decodeFromString<PushRequest>(server.takeRequest().body.readUtf8())
            assertEquals(SchemaRef(2, currentHash), renewed.schema)
            assertEquals(2, renewed.mutations.size)
            assertTrue(renewed.mutations.all { it.authoredSchema == SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH) })

            database.execute("UPDATE _synchro_push_batches SET schema_json = '[]' WHERE state = 'superseded'")
            database.execute(
                "DELETE FROM _synchro_schema_archives WHERE schema_version = ? AND schema_hash = ?",
                arrayOf(1L, PROTOCOL_TEST_SCHEMA_HASH),
            )

            val missingHistory = runCatching {
                processor.processPush(http(server), "device-1", 2, 2, currentHash, listOf(localTable))
            }.exceptionOrNull()
            assertTrue(missingHistory is SynchroError.InvalidResponse)
            assertEquals(2, server.requestCount)
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

    @Test
    fun pushLimitCompositionEqualsTheCompleteRequestEncoding() = runTest {
        val (database, _, processor) = environment()
        installServerRow(database, "server one", "sv-1")
        installServerRow(database, "server two", "sv-2", recordID = "o2")
        database.execute(
            "INSERT INTO orders (id, title, score, document, updated_at) VALUES (?, ?, ?, ?, ?)",
            arrayOf(
                "o3",
                "quote \" backslash \\ slash / tab \t line \n control \u0001 \u007f \u00e9 \uD83D\uDE00 \u2028",
                1e-7,
                """{"a":"${"\u00fc"}","b":[1.5,0.1]}""",
                "2026-01-01T00:00:00.000000Z",
            ),
        )
        database.execute("UPDATE orders SET title = ?, score = ? WHERE id = ?", arrayOf("\u00fc \"updated\"", 12345.678, "o1"))
        database.execute("DELETE FROM orders WHERE id = ?", arrayOf("o2"))
        val server = MockWebServer()
        server.enqueue(retryableResponse())
        server.start()
        try {
            sealWithRetryableFailure(processor, server)
            val body = server.takeRequest().body.readUtf8()
            val request = pushJSON.decodeFromString<PushRequest>(body)
            assertEquals(body, pushJSON.encodeToString(request))
            assertEquals(
                setOf(Operation.INSERT, Operation.UPDATE, Operation.DELETE),
                request.mutations.map { it.op }.toSet(),
            )

            val composed = request.mutations.fold(PushLimits.envelope(pushJSON, request)) { size, mutation ->
                size.adding(PushLimits.mutation(pushJSON, mutation))
            }

            assertEquals(octets(body).toLong(), composed.body)
            assertEquals(canonicalOctets(body).toLong(), composed.canonical)
            assertNotEquals(composed.body, composed.canonical)
        } finally {
            server.shutdown()
        }
    }

    @Test
    fun pendingEntriesOverTheRequestLimitSealInOrderedBatchesWithinBothMeasures() = runTest {
        // The canonical form writes 1e20 with 15 more octets than the body form.
        val score = 1e20
        val probe = sealedInsert(score = score)
        val element = PushLimits.mutation(pushJSON, probe.mutations.single())
        val reserved = PushLimits.reservedEnvelope(pushJSON, probe.clientID, probe.batchID, probe.schema.hash)
        assertEquals(reserved.body, reserved.canonical)
        assertTrue(element.canonical > element.body)
        // The body measure fits bodyFit rows in one request, but the canonical measure fits one row fewer.
        val bodyFit = 20
        val title = ((PushLimits.MAX_REQUEST_OCTETS - reserved.body - bodyFit * element.body - (bodyFit - 1)) / bodyFit).toInt()
        val (database, _, processor) = environment()
        val titles = (0 until 2 * bodyFit).associate { index -> "${'a' + index / 10}${index % 10}" to "${index % 10}".repeat(title) }
        titles.forEach { (id, text) -> insertOrder(database, id, text, score) }
        val bodies = mutableListOf<String>()
        val server = MockWebServer()
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val body = request.body.readUtf8()
                bodies += body
                val sealed = pushJSON.decodeFromString<PushRequest>(body)
                val accepted = sealed.mutations.map { mutation ->
                    val recordID = mutation.pk.getValue("id").jsonPrimitive.content
                    acceptedFor(mutation.mutationID, recordID, titles.getValue(recordID), "sv-${mutation.mutationID}")
                }
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
            var pushes = 0
            while (
                processor.processPush(http(server), "device-1", 1, 1, PROTOCOL_TEST_SCHEMA_HASH, listOf(localTable)) != null
            ) {
                pushes += 1
                assertTrue(pushes <= titles.size)
            }

            assertTrue(bodies.size >= 2)
            assertTrue(bodies.sumOf { canonicalOctets(it) } > PushLimits.MAX_REQUEST_OCTETS)
            assertEquals(bodyFit - 1, pushJSON.decodeFromString<PushRequest>(bodies.first()).mutations.size)
            bodies.forEach { body ->
                assertTrue(octets(body) <= PushLimits.MAX_REQUEST_OCTETS)
                assertTrue(canonicalOctets(body) <= PushLimits.MAX_REQUEST_OCTETS)
            }
            val sealedIDs = bodies.flatMap { body ->
                pushJSON.decodeFromString<PushRequest>(body).mutations.map { it.mutationID }
            }
            val ledgerIDs = database.query("SELECT mutation_id FROM _synchro_pending_changes ORDER BY local_order")
                .map { it.getValue("mutation_id") }
            assertEquals(titles.size, ledgerIDs.size)
            assertEquals(ledgerIDs, sealedIDs)
        } finally {
            server.shutdown()
        }
    }

    @Test
    fun normalizedLimitIsInclusiveAndAnOversizeMutationLeavesTheQueue() = runTest {
        val overhead = PushLimits.mutation(pushJSON, sealedInsert().mutations.single()).normalized
        val atLimit = PushLimits.MAX_NORMALIZED_MUTATION_OCTETS - overhead
        val (database, tracker, processor) = environment()
        insertOrder(database, "o1", "a".repeat(atLimit))
        insertOrder(database, "o2", "b".repeat(atLimit + 1))
        val currentHash = "1".repeat(64)
        installTestSchema(database, 2, currentHash, listOf(localTable))
        database.execute("UPDATE orders SET title = ? WHERE id = ?", arrayOf("dependent", "o2"))
        insertOrder(database, "o3", "later")
        val exact = ledgerID(database, "o1")
        val oversize = ledgerID(database, "o2")
        val dependent = ledgerID(database, "o2", "update")
        val later = ledgerID(database, "o3")
        val server = MockWebServer()
        server.enqueue(retryableResponse())
        server.start()
        try {
            assertTrue(
                runCatching {
                    processor.processPush(http(server), "device-1", 1, 2, currentHash, listOf(localTable))
                }.exceptionOrNull() is RetryableError,
            )
            val request = pushJSON.decodeFromString<PushRequest>(server.takeRequest().body.readUtf8())

            assertEquals(listOf(exact, later), request.mutations.map { it.mutationID })
            assertEquals(
                PushLimits.MAX_NORMALIZED_MUTATION_OCTETS,
                PushLimits.mutation(pushJSON, request.mutations.first()).normalized,
            )
            assertEquals("exceeds_push_limit", lifecycleState(database, oversize))
            assertEquals("blocked_by_predecessor", lifecycleState(database, dependent))
            assertTrue(
                database.query(
                    "SELECT 1 FROM _synchro_push_batch_members WHERE mutation_id = ?",
                    arrayOf(oversize),
                ).isEmpty(),
            )
            val retained = tracker.inspectRetainedMutations().currentRecords().single { it.mutationID == oversize }
            assertEquals(LocalMutationStatus.EXCEEDS_PUSH_LIMIT, retained.status)
            assertEquals(
                AnyCodable("b".repeat(atLimit + 1)),
                retained.authoredFields.single { it.fieldID == "title" }.value,
            )
            assertEquals(4, tracker.retainedMutationCount())
            assertFalse(tracker.inspectPendingMutations().currentRecords().any { it.mutationID == oversize })
            assertTrue(database.readTransaction { SynchroMeta.listRejectedMutations(it) }.isEmpty())
        } finally {
            server.shutdown()
        }
    }

    @Test
    fun authoredColumnLimitIsInclusive() = runTest {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val database = databases.create(context)
        val fields = (0..PushLimits.MAX_AUTHORED_COLUMNS).map { "c$it" }
        val wide = SchemaTable(
            tableName = "wide",
            updatedAtColumn = "updated_at",
            deletedAtColumn = "deleted_at",
            primaryKey = listOf("id"),
            columns = listOf(SchemaColumn("id", logicalType = "string", nullable = false, isPrimaryKey = true)) +
                fields.map { SchemaColumn(it, logicalType = "string") } +
                listOf(
                    SchemaColumn("updated_at", logicalType = "datetime", nullable = false),
                    SchemaColumn("deleted_at", logicalType = "datetime"),
                ),
        ).localSchema
        val hash = "2".repeat(64)
        installTestSchema(database, 1, hash, listOf(wide))
        val processor = PushProcessor(database, ChangeTracker(database))
        fun insertWide(id: String, columns: List<String>) {
            database.execute(
                "INSERT INTO wide (id, ${columns.joinToString()}, updated_at) VALUES (?, ${columns.joinToString { "?" }}, ?)",
                arrayOf(id, *columns.toTypedArray(), "2026-01-01T00:00:00.000000Z"),
            )
        }
        insertWide("w1", fields)
        insertWide("w2", fields.dropLast(1))
        val server = MockWebServer()
        server.enqueue(retryableResponse())
        server.start()
        try {
            assertTrue(
                runCatching {
                    processor.processPush(http(server), "device-1", 1, 1, hash, listOf(wide))
                }.exceptionOrNull() is RetryableError,
            )
            val request = pushJSON.decodeFromString<PushRequest>(server.takeRequest().body.readUtf8())

            assertEquals(listOf(ledgerID(database, "w2")), request.mutations.map { it.mutationID })
            assertEquals(PushLimits.MAX_AUTHORED_COLUMNS, request.mutations.single().columns?.size)
            assertEquals("exceeds_push_limit", lifecycleState(database, ledgerID(database, "w1")))
        } finally {
            server.shutdown()
        }
    }

    @Test
    fun sealingContinuesWhenEveryFirstPassCandidateExceedsALimit() = runTest {
        val (database, _, processor) = environment()
        insertOrder(database, "o1", "x".repeat(PushLimits.MAX_NORMALIZED_MUTATION_OCTETS))
        insertOrder(database, "o2", "y".repeat(PushLimits.MAX_NORMALIZED_MUTATION_OCTETS))
        insertOrder(database, "o3", "fits")
        val server = MockWebServer()
        server.enqueue(retryableResponse())
        server.start()
        try {
            assertTrue(
                runCatching {
                    processor.processPush(
                        http(server), "device-1", 1, 1, PROTOCOL_TEST_SCHEMA_HASH, listOf(localTable), batchSize = 2,
                    )
                }.exceptionOrNull() is RetryableError,
            )
            val request = pushJSON.decodeFromString<PushRequest>(server.takeRequest().body.readUtf8())

            assertEquals(listOf(ledgerID(database, "o3")), request.mutations.map { it.mutationID })
            assertEquals(
                listOf("exceeds_push_limit", "exceeds_push_limit"),
                listOf("o1", "o2").map { lifecycleState(database, ledgerID(database, it)) },
            )
            assertEquals(1, server.requestCount)
        } finally {
            server.shutdown()
        }
    }

    @Test
    fun reservedEnvelopeKeepsARenewedSuccessorWithinTheRequestLimit() = runTest {
        val probe = sealedInsert()
        val probeEnvelope = PushLimits.envelope(pushJSON, probe)
        val probeElement = PushLimits.mutation(pushJSON, probe.mutations.single())
        val envelope = maxOf(probeEnvelope.body, probeEnvelope.canonical)
        val element = maxOf(probeElement.body, probeElement.canonical).toLong()
        val title = 60_000
        // Each reserve adds 15 octets. The last row fits only when the sealer omits a reserve.
        // Then the renewed successor is larger than the limit.
        val slack = 20L
        val fullRows = ((PushLimits.MAX_REQUEST_OCTETS - slack - envelope - element) / (element + title + 1)).toInt()
        val lastTitle = PushLimits.MAX_REQUEST_OCTETS - slack - envelope - fullRows * (element + title + 1) - element
        val (database, _, processor) = environment()
        (0 until fullRows).forEach { index -> insertOrder(database, "r${'a' + index}", "x".repeat(title)) }
        insertOrder(database, "zz", "x".repeat(lastTitle.toInt()))
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
            val original = pushJSON.decodeFromString<PushRequest>(server.takeRequest().body.readUtf8())
            assertEquals(fullRows, original.mutations.size)

            val renewedHash = "3".repeat(64)
            installTestSchema(database, PushLimits.MAX_PROTOCOL_INTEGER, renewedHash, listOf(localTable))
            assertTrue(
                processor.renewRequiredBatches(
                    "device-1",
                    PushLimits.MAX_PROTOCOL_INTEGER,
                    PushLimits.MAX_PROTOCOL_INTEGER,
                    renewedHash,
                    listOf(localTable),
                ),
            )
            val successorJSON = database.queryOne(
                "SELECT request_json FROM _synchro_push_batches WHERE state = 'pending'",
            )!!.getValue("request_json") as String
            val successor = pushJSON.decodeFromString<PushRequest>(successorJSON)

            assertEquals(PushLimits.MAX_PROTOCOL_INTEGER, successor.clientGeneration)
            assertEquals(SchemaRef(PushLimits.MAX_PROTOCOL_INTEGER, renewedHash), successor.schema)
            assertEquals(original.mutations.map { it.mutationID }, successor.mutations.map { it.mutationID })
            assertTrue(octets(successorJSON) <= PushLimits.MAX_REQUEST_OCTETS)
            assertTrue(canonicalOctets(successorJSON) <= PushLimits.MAX_REQUEST_OCTETS)
        } finally {
            server.shutdown()
        }
    }

    @Test
    fun invalidStoredValueKeepsTheInvalidResponseErrorAtSeal() = runTest {
        val (database, _, processor) = environment()
        insertOrder(database, "o1", "valid")
        // The UTF-8 octets ED A0 80 decode to an unpaired surrogate.
        database.execute("UPDATE _synchro_mutation_values SET value_text = CAST(x'EDA080' AS TEXT) WHERE field_id = 'title'")
        val server = MockWebServer()
        server.start()
        try {
            val failure = runCatching {
                processor.processPush(http(server), "device-1", 1, 1, PROTOCOL_TEST_SCHEMA_HASH, listOf(localTable))
            }.exceptionOrNull()

            assertTrue(failure is SynchroError.InvalidResponse)
            assertEquals("captured", lifecycleState(database, ledgerID(database, "o1")))
            assertEquals(0, server.requestCount)
        } finally {
            server.shutdown()
        }
    }

    @Test
    fun slashOnlyTextAtTheNormalizedLimitSealsAloneBelowTheRequestLimit() = runTest {
        val overhead = PushLimits.mutation(pushJSON, sealedInsert().mutations.single()).normalized
        val (database, _, processor) = environment()
        insertOrder(database, "o1", "/".repeat(PushLimits.MAX_NORMALIZED_MUTATION_OCTETS - overhead))
        val server = MockWebServer()
        server.enqueue(retryableResponse())
        server.start()
        try {
            sealWithRetryableFailure(processor, server)
            val body = server.takeRequest().body.readUtf8()
            val request = pushJSON.decodeFromString<PushRequest>(body)

            assertEquals(
                PushLimits.MAX_NORMALIZED_MUTATION_OCTETS,
                PushLimits.mutation(pushJSON, request.mutations.single()).normalized,
            )
            assertTrue(octets(body) < PushLimits.MAX_REQUEST_OCTETS)
            assertTrue(canonicalOctets(body) < PushLimits.MAX_REQUEST_OCTETS)
        } finally {
            server.shutdown()
        }
    }

    /**
     * A client identity near the request limit is the only way that an individually valid
     * mutation cannot fit alone. The same queue fits at the limit and fails one octet above it.
     */
    @Test
    fun firstCandidateMustFitBothRequestMeasuresWithTheReservedEnvelope() = runTest {
        val requestLimit = 1_048_576
        val title = "a".repeat(60_000)
        // The body writes 1.0E-7 with two more octets. RFC 8785 writes 1e20 with 15 more octets.
        listOf(true to 1e-7, false to 1e20).forEach { (bodyDominant, score) ->
            val dominant = { octets: Pair<Int, Int> -> if (bodyDominant) octets.first else octets.second }
            val other = { octets: Pair<Int, Int> -> if (bodyDominant) octets.second else octets.first }
            val base = reservedOctets(sealedInsert("o1", title, score), clientID = "")
            assertTrue(dominant(base) > other(base))

            listOf(0, 1).forEach { extra ->
                val clientID = "c".repeat(requestLimit - dominant(base) + extra)
                val (database, tracker, processor) = environment()
                insertOrder(database, "o1", title, score)
                // A newer authored schema keeps the dependent update separate from the insert.
                val currentHash = "1".repeat(64)
                installTestSchema(database, 2, currentHash, listOf(localTable))
                database.execute("UPDATE orders SET title = ? WHERE id = ?", arrayOf("dependent", "o1"))
                insertOrder(database, "o2", "small", 0.5)
                val big = ledgerID(database, "o1")
                val dependent = ledgerID(database, "o1", "update")
                val small = ledgerID(database, "o2")
                val server = MockWebServer()
                server.enqueue(retryableResponse())
                server.start()
                try {
                    assertTrue(
                        runCatching {
                            processor.processPush(http(server), clientID, 1, 2, currentHash, listOf(localTable))
                        }.exceptionOrNull() is RetryableError,
                    )
                    val request = pushJSON.decodeFromString<PushRequest>(server.takeRequest().body.readUtf8())

                    if (extra == 0) {
                        assertEquals(listOf(big), request.mutations.map { it.mutationID })
                        val sealed = reservedOctets(request, clientID)
                        assertEquals(requestLimit, dominant(sealed))
                        assertTrue(other(sealed) < requestLimit)
                        assertEquals("captured", lifecycleState(database, small))
                        return@forEach
                    }
                    assertEquals(listOf(small), request.mutations.map { it.mutationID })
                    assertEquals("exceeds_push_limit", lifecycleState(database, big))
                    assertEquals("blocked_by_predecessor", lifecycleState(database, dependent))
                    assertTrue(
                        database.query(
                            "SELECT 1 FROM _synchro_push_batch_members WHERE mutation_id = ?",
                            arrayOf(big),
                        ).isEmpty(),
                    )
                    val retained = tracker.inspectRetainedMutations().currentRecords().single { it.mutationID == big }
                    assertEquals(LocalMutationStatus.EXCEEDS_PUSH_LIMIT, retained.status)
                    assertEquals(AnyCodable(title), retained.authoredFields.single { it.fieldID == "title" }.value)
                    assertEquals(AnyCodable(score), retained.authoredFields.single { it.fieldID == "score" }.value)
                    val retainedMutation = Mutation(
                        mutationID = retained.mutationID,
                        table = retained.tableID,
                        op = retained.operation,
                        pk = JsonObject(mapOf(retained.primaryKeyFieldID to JsonPrimitive(retained.recordID))),
                        authoredSchema = retained.authoredSchema,
                        baseVersion = retained.baseVersion,
                        clientVersion = retained.clientVersion,
                        columns = JsonObject(
                            retained.authoredFields.associate { field ->
                                field.fieldID to Json.encodeToJsonElement(AnyCodableSerializer, field.value)
                            },
                        ),
                    )
                    val measure = PushLimits.mutation(pushJSON, retainedMutation)
                    assertTrue(measure.normalized <= 65_536)
                    assertTrue(measure.authoredColumns <= 256)
                    val alone = reservedOctets(request.copy(mutations = listOf(retainedMutation)), clientID)
                    assertEquals(requestLimit + 1, dominant(alone))
                    assertTrue(other(alone) <= requestLimit)
                } finally {
                    server.shutdown()
                }
            }
        }
    }

    /**
     * An envelope exactly at the request limit does not exceed it. No mutation element fits with it,
     * so each candidate follows the singleton terminal policy and no request is sent.
     */
    @Test
    fun envelopeExactlyAtTheRequestLimitLeavesEachCandidateIndividuallyUnsendable() = runTest {
        val empty = PushRequest(
            clientID = "",
            clientGeneration = 1,
            batchID = UUID.randomUUID().toString(),
            schema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
            mutations = emptyList(),
        )
        val base = reservedOctets(empty, clientID = "")
        assertEquals(base.first, base.second)
        val clientID = "c".repeat(1_048_576 - base.first)
        assertEquals(1_048_576 to 1_048_576, reservedOctets(empty, clientID))
        val (database, tracker, processor) = environment()
        insertOrder(database, "o1", "x")
        val insert = ledgerID(database, "o1")
        val server = MockWebServer()
        // A request that should not exist gets a prompt answer, so the count assertion reports it.
        server.enqueue(retryableResponse())
        server.start()
        try {
            val result = runCatching {
                processor.processPush(http(server), clientID, 1, 1, PROTOCOL_TEST_SCHEMA_HASH, listOf(localTable))
            }

            assertEquals(0, server.requestCount)
            assertNull(result.getOrThrow())
            assertEquals("exceeds_push_limit", lifecycleState(database, insert))
            assertEquals(
                LocalMutationStatus.EXCEEDS_PUSH_LIMIT,
                tracker.inspectRetainedMutations().currentRecords().single { it.mutationID == insert }.status,
            )
            assertTrue(database.query("SELECT 1 FROM _synchro_push_batches").isEmpty())
        } finally {
            server.shutdown()
        }
    }
}
