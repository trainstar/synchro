@file:OptIn(com.trainstar.synchro.inspection.SynchroProofApi::class)

package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import com.trainstar.synchro.inspection.SynchroInspection
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import okhttp3.mockwebserver.SocketPolicy
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
import org.robolectric.annotation.SQLiteMode
import java.io.File
import java.util.UUID
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28])
@SQLiteMode(SQLiteMode.Mode.NATIVE)
class Issue49RequirementProofTests {
    private data class PullEnvironment(
        val databaseName: String,
        val database: SynchroDatabase,
        val processor: PullProcessor,
        val table: LocalSchemaTable,
    )

    private data class TableFixture(
        val name: String,
        val seededID: String,
        val insertedID: String,
        val updateColumn: String,
        val cloneSQL: String,
    )

    private data class QueueObservation(
        val mutationID: String,
        val localOrder: Long,
        val tableName: String,
        val recordID: String,
        val operation: Operation,
        val baseVersion: String?,
        val authoredFields: List<AuthoredMutationField>,
    )

    private val context = ApplicationProvider.getApplicationContext<Context>()
    private val wireJSON = Json { encodeDefaults = true }
    private val proofSchemaTable = SchemaTable(
        tableName = "orders",
        pushPolicy = "owner_only",
        updatedAtColumn = "updated_at",
        deletedAtColumn = "deleted_at",
        primaryKey = listOf("id"),
        columns = listOf(
            SchemaColumn("id", dbType = "uuid", logicalType = "string", nullable = false, isPrimaryKey = true),
            SchemaColumn("ship_address", dbType = "text", logicalType = "string"),
            SchemaColumn("user_id", dbType = "uuid", logicalType = "string", nullable = false),
            SchemaColumn("updated_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false),
            SchemaColumn("deleted_at", dbType = "timestamp with time zone", logicalType = "datetime"),
        ),
    )
    private val fixtures = listOf(
        TableFixture(
            "regions",
            "10000000-0000-0000-0000-000000000001",
            "20000000-0000-0000-0000-000000000001",
            "name",
            """
                INSERT INTO regions
                SELECT created_at, updated_at, ?, description, deleted_at, name
                FROM regions WHERE id = ?
            """.trimIndent(),
        ),
        TableFixture(
            "nations",
            "10000000-0000-0000-0000-000000000002",
            "20000000-0000-0000-0000-000000000002",
            "name",
            """
                INSERT INTO nations
                SELECT iso_code, metadata, deleted_at, updated_at, name, created_at, region_id, ?
                FROM nations WHERE id = ?
            """.trimIndent(),
        ),
        TableFixture(
            "suppliers",
            "10000000-0000-0000-0000-000000000003",
            "20000000-0000-0000-0000-000000000003",
            "name",
            """
                INSERT INTO suppliers
                SELECT address, created_at, updated_at, phone, name, website, deleted_at,
                       rating, tags, ?, nation_id, is_active
                FROM suppliers WHERE id = ?
            """.trimIndent(),
        ),
        TableFixture(
            "parts",
            "10000000-0000-0000-0000-000000000004",
            "20000000-0000-0000-0000-000000000004",
            "name",
            """
                INSERT INTO parts
                SELECT ?, name, description, brand, tags, created_at, specifications, updated_at,
                       weight_kg, retail_price, size_cm, deleted_at, part_type, manufacturer
                FROM parts WHERE id = ?
            """.trimIndent(),
        ),
        TableFixture(
            "part_suppliers",
            "10000000-0000-0000-0000-000000000005",
            "20000000-0000-0000-0000-000000000005",
            "notes",
            """
                INSERT INTO part_suppliers
                SELECT part_id, notes, deleted_at, supply_cost, updated_at, created_at,
                       lead_time_days, supplier_id, available_quantity, ?
                FROM part_suppliers WHERE id = ?
            """.trimIndent(),
        ),
        TableFixture(
            "categories",
            "10000000-0000-0000-0000-000000000006",
            "20000000-0000-0000-0000-000000000006",
            "name",
            """
                INSERT INTO categories
                SELECT ?, updated_at, parent_id, deleted_at, created_at, metadata, sort_order, name
                FROM categories WHERE id = ?
            """.trimIndent(),
        ),
    )

    @Test
    fun registeredLocalSQLCrudProducesCanonicalMutationsAndRejectsIncompleteObservations() {
        for (operation in listOf(Operation.INSERT, Operation.UPDATE, Operation.DELETE)) {
            val databaseName = temporaryDatabaseName("crud-${operation.name.lowercase()}")
            val client = SynchroClient(config(databaseName, canonicalSeedPath()), context)
            try {
                for (fixture in fixtures) {
                    when (operation) {
                        Operation.INSERT -> assertEquals(
                            1,
                            client.execute(fixture.cloneSQL, arrayOf(fixture.insertedID, fixture.seededID)).rowsAffected,
                        )
                        Operation.UPDATE -> assertEquals(
                            1,
                            client.execute(
                                "UPDATE ${fixture.name} SET ${fixture.updateColumn} = ? WHERE id = ?",
                                arrayOf("Issue 49 update", fixture.seededID),
                            ).rowsAffected,
                        )
                        Operation.DELETE -> {
                            client.execute("DELETE FROM ${fixture.name} WHERE id = ?", arrayOf(fixture.seededID))
                            assertNotNull(
                                client.queryOne(
                                    "SELECT deleted_at FROM ${fixture.name} WHERE id = ? AND deleted_at IS NOT NULL",
                                    arrayOf(fixture.seededID),
                                ),
                            )
                        }
                        Operation.UPSERT -> throw AssertionError("The proof does not request upsert")
                    }
                }

                client.createTable(
                    "local_notes",
                    listOf(
                        ColumnDef("id", "TEXT", nullable = false, primaryKey = true),
                        ColumnDef("body", "TEXT"),
                    ),
                )
                assertEquals(
                    1,
                    client.execute(
                        "INSERT INTO local_notes (id, body) VALUES (?, ?)",
                        arrayOf("note-1", "local"),
                    ).rowsAffected,
                )

                val observed = client.inspectPendingMutations()
                assertTrue(queueMatches(observed, operation))
                assertEquals(fixtures.size, client.pendingChangeCount())

                val missingTableMutant = observed.dropLast(1)
                assertFalse(queueMatches(missingTableMutant, operation))

                val wrongOperation = if (operation == Operation.INSERT) Operation.UPDATE else Operation.INSERT
                assertFalse(queueMatches(observed, wrongOperation))
            } finally {
                client.close()
                context.deleteDatabase(databaseName)
            }
        }
    }

    @Test
    fun opaqueBaseVersionAndAuthoredQueueSurviveRestartAndRejectRewriting() {
        val databaseName = temporaryDatabaseName("durable-queue")
        val config = config(databaseName, canonicalSeedPath())
        val category = fixtures.last()
        val first = SynchroClient(config, context)
        val authoritativeVersion = checkNotNull(
            SynchroInspection(first).rowMetadata(category.name, category.seededID)?.serverVersion,
        )
        assertTrue(authoritativeVersion.isNotEmpty())

        assertEquals(
            1,
            first.execute(
                "UPDATE categories SET name = ?, updated_at = ? WHERE id = ?",
                arrayOf("Offline category", "2026-02-02T00:00:00.000000Z", category.seededID),
            ).rowsAffected,
        )
        val beforeRestart = first.inspectPendingMutations().single()
        assertEquals(authoritativeVersion, beforeRestart.baseVersion)
        assertEquals(Operation.UPDATE, beforeRestart.operation)
        assertTrue(beforeRestart.authoredFields.any { it.value.value == "Offline category" })
        first.close()

        val reopened = SynchroClient(config, context)
        try {
            val afterRestart = reopened.inspectPendingMutations().single()
            assertEquals(observation(beforeRestart), observation(afterRestart))
            assertEquals(
                "Offline category",
                reopened.queryOne("SELECT name FROM categories WHERE id = ?", arrayOf(category.seededID))?.get("name"),
            )

            val rewrittenVersionMutant = observation(afterRestart).copy(
                baseVersion = "$authoritativeVersion-rewritten",
            )
            assertNotEquals(observation(beforeRestart), rewrittenVersionMutant)

            val regeneratedIdentityMutant = observation(afterRestart).copy(
                mutationID = UUID.randomUUID().toString(),
            )
            assertNotEquals(observation(beforeRestart), regeneratedIdentityMutant)

            assertThrows(Exception::class.java) {
                reopened.query("SELECT * FROM _synchro_pending_changes")
            }
            assertThrows(Exception::class.java) {
                reopened.execute("DELETE FROM _synchro_pending_changes")
            }
            assertEquals(listOf(observation(beforeRestart)), reopened.inspectPendingMutations().map(::observation))
        } finally {
            reopened.close()
            context.deleteDatabase(databaseName)
        }
    }

    @Test
    fun portableSeedRejectsQueuedIntentWithoutPublishingMutant() {
        val mutableSeedName = temporaryDatabaseName("mutated-seed")
        val rejectedDestinationName = temporaryDatabaseName("rejected-seed")
        val baseline = SynchroClient(config(mutableSeedName, canonicalSeedPath()), context)
        assertEquals(
            "Seed Category",
            baseline.queryOne(
                "SELECT name FROM categories WHERE id = ?",
                arrayOf(fixtures.last().seededID),
            )?.get("name"),
        )
        assertTrue(baseline.inspectPendingMutations().isEmpty())

        assertEquals(
            1,
            baseline.execute(
                "UPDATE categories SET name = ? WHERE id = ?",
                arrayOf("Unauthorized queued seed intent", fixtures.last().seededID),
            ).rowsAffected,
        )
        assertEquals(1, baseline.inspectPendingMutations().size)
        baseline.close()

        assertThrows(Exception::class.java) {
            SynchroClient(
                config(rejectedDestinationName, context.getDatabasePath(mutableSeedName).absolutePath),
                context,
            )
        }
        assertNoDatabaseFamily(rejectedDestinationName)
        context.deleteDatabase(mutableSeedName)
        context.deleteDatabase(rejectedDestinationName)
    }

    @Test
    fun pullApplyCommitsOpaqueCursorAndChecksumsAtomicallyWithoutEcho() {
        val scopeID = "orders:atomic"
        val cursor = "c1.A_-~%2F.雪"
        val success = makePullEnvironment("atomic-success")
        success.database.writeTransaction {
            SynchroMeta.upsertScope(it, scopeID, "opaque-old-cursor", null, 7)
        }
        val row = proofRow("row-1", "server", "2026-03-01T00:00:00.000000Z")
        val change = makeChangeRecord(
            scope = scopeID,
            schema = success.table,
            op = Operation.UPSERT,
            pk = buildJsonObject { put("id", "row-1") },
            row = row,
            serverVersion = "opaque-server-v1",
        )
        val rowDigest = Integrity.rowDigest(
            PROTOCOL_TEST_SCHEMA_HASH,
            success.table,
            buildJsonObject { put("id", "row-1") },
            row,
            "opaque-server-v1",
        )
        val scopeDigest = Integrity.scopeDigest(
            PROTOCOL_TEST_SCHEMA_HASH,
            scopeID,
            listOf(rowDigest.identity to rowDigest.checksum),
        )
        success.processor.applyScopeChanges(
            listOf(change),
            listOf(success.table),
            mapOf(scopeID to cursor),
            mapOf(scopeID to scopeDigest),
            PROTOCOL_TEST_SCHEMA_HASH,
        )
        assertEquals("server", success.database.queryOne(
            "SELECT ship_address FROM orders WHERE id = ?",
            arrayOf("row-1"),
        )?.get("ship_address"))
        assertEquals(0, ChangeTracker(success.database).pendingChangeCount())
        success.database.readTransaction {
            assertEquals(cursor, SynchroMeta.getScope(it, scopeID)?.cursor)
            assertEquals("opaque-server-v1", SynchroMeta.getRowVersion(it, "orders", "row-1"))
        }
        success.database.close()
        val reopened = SynchroDatabase.open(context, success.databaseName)
        reopened.readTransaction {
            assertEquals(cursor, SynchroMeta.getScope(it, scopeID)?.cursor)
            assertEquals("opaque-server-v1", SynchroMeta.getRowVersion(it, "orders", "row-1"))
        }
        assertEquals(0, ChangeTracker(reopened).pendingChangeCount())
        reopened.close()
        context.deleteDatabase(success.databaseName)

        val mutant = makePullEnvironment("atomic-mutant")
        mutant.database.writeTransaction {
            SynchroMeta.upsertScope(it, scopeID, "opaque-old-cursor", null, 7)
        }
        val corrupted = change.copy(
            rowChecksum = ChecksumObject("sha256", 1, "hex", "f".repeat(64)),
        )
        assertThrows(Exception::class.java) {
            mutant.processor.applyScopeChanges(
                listOf(corrupted),
                listOf(mutant.table),
                mapOf(scopeID to cursor),
                mapOf(scopeID to scopeDigest),
                PROTOCOL_TEST_SCHEMA_HASH,
            )
        }
        assertNull(mutant.database.queryOne("SELECT id FROM orders WHERE id = ?", arrayOf("row-1")))
        assertEquals(0, ChangeTracker(mutant.database).pendingChangeCount())
        assertEquals(
            "opaque-old-cursor",
            mutant.database.readTransaction { SynchroMeta.getScope(it, scopeID)?.cursor },
        )
        mutant.database.close()
        val reopenedMutant = SynchroDatabase.open(context, mutant.databaseName)
        assertNull(reopenedMutant.queryOne("SELECT id FROM orders WHERE id = ?", arrayOf("row-1")))
        assertEquals(
            "opaque-old-cursor",
            reopenedMutant.readTransaction { SynchroMeta.getScope(it, scopeID)?.cursor },
        )
        reopenedMutant.close()
        context.deleteDatabase(mutant.databaseName)

        val health = makePullEnvironment("cursor-health-mutant")
        health.database.writeTransaction {
            SynchroMeta.upsertScope(it, scopeID, "healthy-old-cursor", null, 7)
        }
        val healthChange = makeChangeRecord(
            scope = scopeID,
            schema = health.table,
            op = Operation.UPSERT,
            pk = buildJsonObject { put("id", "row-1") },
            row = row,
            serverVersion = "opaque-server-v1",
        )
        health.processor.applyScopeChanges(
            listOf(healthChange),
            listOf(health.table),
            mapOf(scopeID to cursor),
            mapOf(scopeID to protocolEmptyScopeChecksum(scopeID)),
            PROTOCOL_TEST_SCHEMA_HASH,
        )
        assertEquals(
            "server",
            health.database.queryOne(
                "SELECT ship_address FROM orders WHERE id = ?",
                arrayOf("row-1"),
            )?.get("ship_address"),
        )
        assertNull(health.database.readTransaction { SynchroMeta.getScope(it, scopeID)?.cursor })
        assertNull(health.database.readTransaction { SynchroMeta.getScope(it, scopeID)?.checksum })
        assertEquals(0, ChangeTracker(health.database).pendingChangeCount())
        health.database.close()
        val reopenedHealth = SynchroDatabase.open(context, health.databaseName)
        assertNull(reopenedHealth.readTransaction { SynchroMeta.getScope(it, scopeID)?.cursor })
        assertEquals(
            "opaque-server-v1",
            reopenedHealth.readTransaction { SynchroMeta.getRowVersion(it, health.table.tableName, "row-1") },
        )
        reopenedHealth.close()
        context.deleteDatabase(health.databaseName)
    }

    @Test
    fun rebuildRestartReplayAndFinalizationPreserveUnrelatedAndProtectedRows() {
        val targetScope = "orders:target"
        val otherScope = "orders:other"
        val environment = makePullEnvironment("rebuild")
        val database = environment.database
        val table = environment.table
        for (recordID in listOf("orphan", "shared", "protected", "local-only")) {
            insertProofRow(database, recordID, "local-$recordID")
        }
        database.writeTransaction { connection ->
            SynchroMeta.upsertScope(connection, targetScope, "target-old", null, 3)
            SynchroMeta.upsertScope(connection, otherScope, "other-stable", null, 8)
            for (recordID in listOf("orphan", "shared", "protected")) {
                SynchroMeta.upsertScopeRow(
                    connection,
                    targetScope,
                    table.tableName,
                    recordID,
                    "0".repeat(64),
                    3,
                )
            }
            SynchroMeta.upsertScopeRow(
                connection,
                otherScope,
                table.tableName,
                "shared",
                "1".repeat(64),
                8,
            )
        }
        database.execute(
            "UPDATE orders SET ship_address = ? WHERE id = ?",
            arrayOf("protected-local-intent", "protected"),
        )
        val protectedMutation = ChangeTracker(database).pendingChanges().single()
        val attempt = environment.processor.beginScopeRebuild(
            targetScope,
            4,
            1,
            PROTOCOL_TEST_SCHEMA_HASH,
            100,
        )
        assertNull(database.queryOne("SELECT id FROM orders WHERE id = 'orphan'"))
        for (retained in listOf("shared", "protected", "local-only")) {
            assertNotNull(database.queryOne("SELECT id FROM orders WHERE id = ?", arrayOf(retained)))
        }
        assertEquals(
            listOf(protectedMutation.mutationID),
            ChangeTracker(database).pendingChanges().map { it.mutationID },
        )
        assertEquals(
            "other-stable",
            database.readTransaction { SynchroMeta.getScope(it, otherScope)?.cursor },
        )
        database.close()

        val reopened = SynchroDatabase.open(context, environment.databaseName)
        val restarted = PullProcessor(reopened)
        assertEquals(attempt, reopened.readTransaction { SynchroMeta.getRebuildAttempt(it, targetScope) })
        val serverRow = proofRow("protected", "server-protected", "2026-03-02T00:00:00.000000Z")
        val rowDigest = Integrity.rowDigest(
            PROTOCOL_TEST_SCHEMA_HASH,
            table,
            buildJsonObject { put("id", "protected") },
            serverRow,
            "opaque-rebuild-v1",
        )
        val record = RebuildRecord(
            table.tableID,
            buildJsonObject { put("id", "protected") },
            serverRow,
            rowDigest.checksum,
            "opaque-rebuild-v1",
        )
        val firstRequest = rebuildRequest(attempt)
        val firstResponse = RebuildResponse(
            targetScope,
            listOf(record),
            "page-two",
            true,
            null,
            null,
        )
        val continued = restarted.applyScopeRebuildPage(
            attempt,
            firstRequest,
            wireJSON.encodeToString(firstRequest),
            firstResponse,
            wireJSON.encodeToString(firstResponse),
            listOf(table),
        )
        val replayed = restarted.applyScopeRebuildPage(
            attempt,
            firstRequest,
            wireJSON.encodeToString(firstRequest),
            firstResponse,
            wireJSON.encodeToString(firstResponse),
            listOf(table),
        )
        assertEquals(continued, replayed)
        assertEquals(1, reopened.query("SELECT * FROM _synchro_rebuild_page_receipts").size)

        val finalRequest = rebuildRequest(continued)
        val wrongFinal = RebuildResponse(
            targetScope,
            emptyList(),
            null,
            false,
            "target-final",
            protocolEmptyScopeChecksum(targetScope),
        )
        assertThrows(Exception::class.java) {
            restarted.applyScopeRebuildPage(
                continued,
                finalRequest,
                wireJSON.encodeToString(finalRequest),
                wrongFinal,
                wireJSON.encodeToString(wrongFinal),
                listOf(table),
            )
        }
        assertEquals(continued, reopened.readTransaction { SynchroMeta.getRebuildAttempt(it, targetScope) })
        assertNull(reopened.readTransaction { SynchroMeta.getScope(it, targetScope)?.cursor })

        val finalChecksum = Integrity.scopeDigest(
            PROTOCOL_TEST_SCHEMA_HASH,
            targetScope,
            listOf(rowDigest.identity to rowDigest.checksum),
        )
        val finalResponse = wrongFinal.copy(checksum = finalChecksum)
        restarted.applyScopeRebuildPage(
            continued,
            finalRequest,
            wireJSON.encodeToString(finalRequest),
            finalResponse,
            wireJSON.encodeToString(finalResponse),
            listOf(table),
        )
        assertEquals(
            "protected-local-intent",
            reopened.queryOne("SELECT ship_address FROM orders WHERE id = 'protected'")?.get("ship_address"),
        )
        assertNotNull(reopened.queryOne("SELECT id FROM orders WHERE id = 'shared'"))
        assertNotNull(reopened.queryOne("SELECT id FROM orders WHERE id = 'local-only'"))
        assertEquals(
            "target-final",
            reopened.readTransaction { SynchroMeta.getScope(it, targetScope)?.cursor },
        )
        assertEquals(
            "other-stable",
            reopened.readTransaction { SynchroMeta.getScope(it, otherScope)?.cursor },
        )
        assertEquals(
            listOf(protectedMutation.mutationID),
            ChangeTracker(reopened).pendingChanges().map { it.mutationID },
        )
        reopened.close()
        val finalReopen = SynchroDatabase.open(context, environment.databaseName)
        assertNull(finalReopen.readTransaction { SynchroMeta.getRebuildAttempt(it, targetScope) })
        assertEquals(
            "target-final",
            finalReopen.readTransaction { SynchroMeta.getScope(it, targetScope)?.cursor },
        )
        finalReopen.close()
        context.deleteDatabase(environment.databaseName)
    }

    @Test
    fun queueNormalizationAndSealedRetriesKeepExactBytesAcrossResponseFaultsAndRestart() = runTest {
        val environment = makePullEnvironment("sealed-retry")
        val database = environment.database
        val table = environment.table
        val tracker = ChangeTracker(database)
        var processor = PushProcessor(database, tracker)
        database.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            arrayOf("queue-row", "first", "proof-user", "2026-03-03T00:00:00.000000Z"),
        )
        database.execute(
            "UPDATE orders SET ship_address = ? WHERE id = ?",
            arrayOf("normalized", "queue-row"),
        )

        val bodies = Collections.synchronizedList(mutableListOf<String>())
        val responseIndex = AtomicInteger()
        val server = MockWebServer()
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                val body = request.body.readUtf8()
                bodies += body
                return when (responseIndex.getAndIncrement()) {
                    0 -> MockResponse()
                        .setResponseCode(429)
                        .setHeader("Retry-After", "1")
                        .setBody(RETRYABLE_429_ERROR_JSON)
                    1 -> MockResponse()
                        .setResponseCode(503)
                        .setHeader("Retry-After", "1")
                        .setBody(RETRYABLE_503_ERROR_JSON)
                    2 -> MockResponse()
                        .setBody("partial response")
                        .setSocketPolicy(SocketPolicy.DISCONNECT_DURING_RESPONSE_BODY)
                    else -> {
                        val push = wireJSON.decodeFromString<PushRequest>(body)
                        val accepted = makeAcceptedMutation(
                            mutationID = push.mutations.single().mutationID,
                            schema = table,
                            pk = buildJsonObject { put("id", "queue-row") },
                            status = MutationStatus.APPLIED,
                            serverRow = proofRow(
                                "queue-row",
                                "normalized",
                                "2026-03-03T01:00:00.000000Z",
                            ),
                            serverVersion = "opaque-accepted-v2",
                        )
                        MockResponse()
                            .setResponseCode(200)
                            .setHeader("Content-Type", "application/json")
                            .setBody(wireJSON.encodeToString(PushResponse(
                                push.batchID,
                                "2026-03-03T01:00:00.000000Z",
                                listOf(accepted),
                                emptyList(),
                            )))
                    }
                }
            }
        }
        server.start()
        val http = proofHttp(server)
        try {
            repeat(2) {
                val failure = runCatching {
                    processor.processPush(
                        http,
                        "issue-49-client",
                        1,
                        1,
                        PROTOCOL_TEST_SCHEMA_HASH,
                        listOf(table),
                    )
                }.exceptionOrNull()
                assertTrue(failure is RetryableError)
            }
            val sealedRequest = wireJSON.decodeFromString<PushRequest>(bodies[0])
            assertEquals(1, sealedRequest.mutations.size)
            assertEquals(Operation.INSERT, sealedRequest.mutations.single().op)
            assertEquals(
                "normalized",
                sealedRequest.mutations.single().columns?.get("ship_address")?.jsonPrimitive?.content,
            )
            assertEquals(bodies[0], bodies[1])
            assertEquals(
                listOf("superseded_before_send", "superseded_before_send", "sealed"),
                database.query(
                    "SELECT lifecycle_state FROM _synchro_pending_changes ORDER BY local_order",
                ).map { it.getValue("lifecycle_state") },
            )

            database.execute(
                "UPDATE orders SET ship_address = ? WHERE id = ?",
                arrayOf("successor", "queue-row"),
            )
            val lostResponse = runCatching {
                processor.processPush(
                    http,
                    "issue-49-client",
                    1,
                    1,
                    PROTOCOL_TEST_SCHEMA_HASH,
                    listOf(table),
                )
            }.exceptionOrNull()
            assertTrue(lostResponse is RetryableError)
            assertEquals(bodies[0], bodies[2])
            database.close()

            val reopened = SynchroDatabase.open(context, environment.databaseName)
            val reopenedTracker = ChangeTracker(reopened)
            processor = PushProcessor(reopened, reopenedTracker)
            processor.processPush(
                http,
                "issue-49-client",
                1,
                1,
                PROTOCOL_TEST_SCHEMA_HASH,
                listOf(table),
            )
            assertEquals(bodies[0], bodies[3])
            val successor = reopenedTracker.pendingChanges().single()
            assertNotEquals(sealedRequest.mutations.single().mutationID, successor.mutationID)
            assertNull(successor.dependsOnMutationID)
            assertEquals("opaque-accepted-v2", successor.baseUpdatedAt)
            val acceptedLedger = checkNotNull(reopened.queryOne(
                "SELECT lifecycle_state, accepted_outcome_json FROM _synchro_pending_changes WHERE mutation_id = ?",
                arrayOf(sealedRequest.mutations.single().mutationID),
            ))
            assertEquals("accepted", acceptedLedger["lifecycle_state"])
            assertNotNull(acceptedLedger["accepted_outcome_json"])
            assertEquals(
                "successor",
                reopened.queryOne(
                    "SELECT ship_address FROM orders WHERE id = ?",
                    arrayOf("queue-row"),
                )?.get("ship_address"),
            )
            reopened.close()

            val cancel = makePullEnvironment("cancel-chain")
            cancel.database.execute(
                "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arrayOf("temporary", "temporary", "proof-user", "2026-03-03T00:00:00.000000Z"),
            )
            cancel.database.execute("DELETE FROM orders WHERE id = ?", arrayOf("temporary"))
            val cancelTracker = ChangeTracker(cancel.database)
            val cancelledPush = PushProcessor(cancel.database, cancelTracker).processPush(
                http,
                "issue-49-client",
                1,
                1,
                PROTOCOL_TEST_SCHEMA_HASH,
                listOf(cancel.table),
            )
            assertNull(cancelledPush)
            assertFalse(cancelTracker.hasPendingChanges())
            assertEquals(
                listOf("cancelled_before_send", "cancelled_before_send"),
                cancel.database.query(
                    "SELECT lifecycle_state FROM _synchro_pending_changes ORDER BY local_order",
                ).map { it.getValue("lifecycle_state") },
            )
            cancel.database.close()
            context.deleteDatabase(cancel.databaseName)
        } finally {
            server.shutdown()
            context.deleteDatabase(environment.databaseName)
        }
    }

    @Test
    fun rejectedOutcomeAndBlockedSuccessorRemainLinkedAndInspectableAcrossRestart() {
        val environment = makePullEnvironment("rejected-successor")
        val database = environment.database
        val table = environment.table
        val tracker = ChangeTracker(database)
        val processor = PushProcessor(database, tracker)
        database.writeSyncLockedTransaction {
            it.execSQL(
                "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arrayOf("rejected-row", "server-base", "proof-user", "2026-03-04T00:00:00.000000Z"),
            )
            SynchroMeta.upsertRowVersion(it, table.tableName, "rejected-row", "opaque-base-v1", null)
        }
        database.execute(
            "UPDATE orders SET ship_address = ? WHERE id = ?",
            arrayOf("first-local", "rejected-row"),
        )
        val capturedPredecessor = tracker.pendingChanges().single()
        val batchID = UUID.randomUUID().toString().lowercase()
        database.writeTransaction {
            it.execSQL(
                "UPDATE _synchro_pending_changes SET lifecycle_state = 'sealed', sealed_batch_id = ?, sealed_ordinal = 0 WHERE mutation_id = ?",
                arrayOf(batchID, capturedPredecessor.mutationID),
            )
        }
        val predecessor = capturedPredecessor.copy(
            lifecycleState = "sealed",
            sealedBatchID = batchID,
            sealedOrdinal = 0,
        )
        database.execute(
            "UPDATE orders SET ship_address = ? WHERE id = ?",
            arrayOf("successor-local", "rejected-row"),
        )
        val authoritativeRow = proofRow(
            "rejected-row",
            "server-conflict",
            "2026-03-04T01:00:00.000000Z",
        )
        val rejection = makeRejectedMutation(
            mutationID = predecessor.mutationID,
            schema = table,
            pk = buildJsonObject { put("id", "rejected-row") },
            status = MutationStatus.CONFLICT,
            code = MutationRejectionCode.VERSION_CONFLICT,
            message = "conflict",
            serverRow = authoritativeRow,
            serverVersion = "opaque-conflict-v2",
        )
        val wrongIdentityMutant = makeRejectedMutation(
            mutationID = UUID.randomUUID().toString().lowercase(),
            schema = table,
            pk = buildJsonObject { put("id", "rejected-row") },
            status = MutationStatus.CONFLICT,
            code = MutationRejectionCode.VERSION_CONFLICT,
            message = "conflict",
            serverRow = authoritativeRow,
            serverVersion = "opaque-conflict-v2",
        )
        assertThrows(Exception::class.java) {
            processor.applyRejected(
                listOf(wrongIdentityMutant),
                listOf(table),
                mapOf(predecessor.mutationID to predecessor),
            )
        }
        assertEquals(
            listOf("sealed", "captured"),
            database.query(
                "SELECT lifecycle_state FROM _synchro_pending_changes ORDER BY local_order",
            ).map { it.getValue("lifecycle_state") },
        )
        assertTrue(database.readTransaction { SynchroMeta.listRejectedMutations(it).isEmpty() })

        processor.applyRejected(
            listOf(rejection),
            listOf(table),
            mapOf(predecessor.mutationID to predecessor),
        )
        val successor = checkNotNull(database.queryOne(
            "SELECT mutation_id, lifecycle_state, depends_on_mutation_id, base_version FROM _synchro_pending_changes WHERE mutation_id <> ?",
            arrayOf(predecessor.mutationID),
        ))
        assertEquals("blocked_by_predecessor", successor["lifecycle_state"])
        assertEquals(predecessor.mutationID, successor["depends_on_mutation_id"])
        assertNull(successor["base_version"])
        assertEquals(
            "successor-local",
            database.queryOne(
                "SELECT ship_address FROM orders WHERE id = ?",
                arrayOf("rejected-row"),
            )?.get("ship_address"),
        )
        database.close()

        val reopened = SynchroDatabase.open(context, environment.databaseName)
        val rejections = reopened.readTransaction { SynchroMeta.listRejectedMutations(it) }
        assertEquals(1, rejections.size)
        assertEquals(predecessor.mutationID, rejections.single().mutationID)
        assertEquals("conflict", rejections.single().status)
        assertEquals("version_conflict", rejections.single().code)
        assertEquals("opaque-conflict-v2", rejections.single().serverVersion)
        val reopenedSuccessor = checkNotNull(reopened.queryOne(
            "SELECT mutation_id, lifecycle_state, depends_on_mutation_id, base_version FROM _synchro_pending_changes WHERE mutation_id <> ?",
            arrayOf(predecessor.mutationID),
        ))
        val rejectedLedger = checkNotNull(reopened.queryOne(
            "SELECT lifecycle_state, rejected_outcome_json FROM _synchro_pending_changes WHERE mutation_id = ?",
            arrayOf(predecessor.mutationID),
        ))
        assertEquals("conflict", rejectedLedger["lifecycle_state"])
        assertNotNull(rejectedLedger["rejected_outcome_json"])
        assertNotEquals(predecessor.mutationID, reopenedSuccessor["mutation_id"])
        assertEquals("blocked_by_predecessor", reopenedSuccessor["lifecycle_state"])
        assertEquals(predecessor.mutationID, reopenedSuccessor["depends_on_mutation_id"])
        assertNull(reopenedSuccessor["base_version"])
        reopened.close()
        context.deleteDatabase(environment.databaseName)
    }

    @Test
    fun stateTransitionGraphAndBlockingFailureRejectUnlistedOrMalformedState() {
        val allowed = mapOf(
            SyncLifecycleState.UNINITIALIZED to setOf(
                SyncLifecycleState.LOCAL_READY,
                SyncLifecycleState.ERROR,
                SyncLifecycleState.STOPPED,
            ),
            SyncLifecycleState.LOCAL_READY to setOf(
                SyncLifecycleState.CONNECTING,
                SyncLifecycleState.ERROR,
                SyncLifecycleState.STOPPED,
            ),
            SyncLifecycleState.CONNECTING to setOf(
                SyncLifecycleState.SCHEMA_APPLYING,
                SyncLifecycleState.READY,
                SyncLifecycleState.BACKOFF,
                SyncLifecycleState.ERROR,
                SyncLifecycleState.STOPPED,
            ),
            SyncLifecycleState.SCHEMA_APPLYING to setOf(
                SyncLifecycleState.READY,
                SyncLifecycleState.REBUILDING,
                SyncLifecycleState.ERROR,
                SyncLifecycleState.STOPPED,
            ),
            SyncLifecycleState.READY to setOf(
                SyncLifecycleState.CONNECTING,
                SyncLifecycleState.PUSHING,
                SyncLifecycleState.PULLING,
                SyncLifecycleState.REBUILDING,
                SyncLifecycleState.ERROR,
                SyncLifecycleState.STOPPED,
            ),
            SyncLifecycleState.PUSHING to setOf(
                SyncLifecycleState.PUSHING,
                SyncLifecycleState.READY,
                SyncLifecycleState.PULLING,
                SyncLifecycleState.CONNECTING,
                SyncLifecycleState.BACKOFF,
                SyncLifecycleState.ERROR,
                SyncLifecycleState.STOPPED,
            ),
            SyncLifecycleState.PULLING to setOf(
                SyncLifecycleState.PULLING,
                SyncLifecycleState.READY,
                SyncLifecycleState.REBUILDING,
                SyncLifecycleState.CONNECTING,
                SyncLifecycleState.BACKOFF,
                SyncLifecycleState.ERROR,
                SyncLifecycleState.STOPPED,
            ),
            SyncLifecycleState.REBUILDING to setOf(
                SyncLifecycleState.REBUILDING,
                SyncLifecycleState.READY,
                SyncLifecycleState.CONNECTING,
                SyncLifecycleState.BACKOFF,
                SyncLifecycleState.ERROR,
                SyncLifecycleState.STOPPED,
            ),
            SyncLifecycleState.BACKOFF to setOf(
                SyncLifecycleState.CONNECTING,
                SyncLifecycleState.PUSHING,
                SyncLifecycleState.PULLING,
                SyncLifecycleState.REBUILDING,
                SyncLifecycleState.ERROR,
                SyncLifecycleState.STOPPED,
            ),
            SyncLifecycleState.ERROR to setOf(
                SyncLifecycleState.LOCAL_READY,
                SyncLifecycleState.STOPPED,
            ),
            SyncLifecycleState.STOPPED to setOf(SyncLifecycleState.LOCAL_READY),
        )
        val databaseName = temporaryDatabaseName("lifecycle")
        val database = SynchroDatabase.open(context, databaseName)
        for (current in SyncLifecycleState.entries) {
            for (next in SyncLifecycleState.entries) {
                forceLifecycleState(database, current)
                val before = database.readTransaction { SynchroMeta.getClientState(it) }
                val failure = runCatching {
                    database.writeTransaction {
                        SynchroMeta.transitionClientLifecycleState(it, next)
                    }
                }.exceptionOrNull()
                if (next in allowed.getValue(current)) {
                    assertNull("${current.wireName} -> ${next.wireName}", failure)
                    assertEquals(
                        next,
                        database.readTransaction { SynchroMeta.getClientState(it).lifecycleState },
                    )
                } else {
                    assertTrue(
                        "${current.wireName} -> ${next.wireName}",
                        failure is SynchroError.InvalidStateTransition,
                    )
                    assertEquals(before, database.readTransaction { SynchroMeta.getClientState(it) })
                }
            }
        }
        val unlistedEdgeMutant = allowed.toMutableMap()
        unlistedEdgeMutant[SyncLifecycleState.STOPPED] =
            unlistedEdgeMutant.getValue(SyncLifecycleState.STOPPED) + SyncLifecycleState.PULLING
        forceLifecycleState(database, SyncLifecycleState.STOPPED)
        assertThrows(SynchroError.InvalidStateTransition::class.java) {
            database.writeTransaction {
                SynchroMeta.transitionClientLifecycleState(it, SyncLifecycleState.PULLING)
            }
        }
        assertTrue(SyncLifecycleState.PULLING in unlistedEdgeMutant.getValue(SyncLifecycleState.STOPPED))

        forceLifecycleState(database, SyncLifecycleState.PULLING)
        database.writeTransaction {
            SynchroMeta.transitionClientLifecycleState(
                it,
                SyncLifecycleState.LOCAL_READY,
                processRecovery = true,
            )
        }
        assertEquals(
            SyncLifecycleState.LOCAL_READY,
            database.readTransaction { SynchroMeta.getClientState(it).lifecycleState },
        )
        forceLifecycleState(database, SyncLifecycleState.PULLING)
        assertThrows(SynchroError.InvalidStateTransition::class.java) {
            database.writeTransaction {
                SynchroMeta.transitionClientLifecycleState(it, SyncLifecycleState.LOCAL_READY)
            }
        }

        forceLifecycleState(database, SyncLifecycleState.LOCAL_READY)
        val failure = SyncFailure(
            SyncOperationKind.PULLING,
            SyncFailureCode.SYNC_INTEGRITY_FAILURE,
            false,
            "invalid terminal checksum",
            SyncRecoveryAction.SCHEMA_RESET,
            mapOf("scope" to "fingerprint"),
        )
        database.writeTransaction { SynchroMeta.recordBlockingError(it, failure) }
        database.close()
        val reopened = SynchroDatabase.open(context, databaseName)
        val durable = reopened.readTransaction { SynchroMeta.getClientState(it) }
        assertEquals(SyncLifecycleState.ERROR, durable.lifecycleState)
        assertEquals(failure, durable.failure)
        reopened.writeTransaction {
            it.execSQL(
                "UPDATE _synchro_client_state SET error_operation = 'unknown' WHERE singleton = 1",
            )
        }
        assertThrows(SynchroError.InvalidResponse::class.java) {
            reopened.readTransaction { SynchroMeta.getClientState(it) }
        }
        reopened.close()
        context.deleteDatabase(databaseName)
    }

    private fun makePullEnvironment(label: String): PullEnvironment {
        val databaseName = temporaryDatabaseName(label)
        val database = SynchroDatabase.open(context, databaseName)
        val table = proofSchemaTable.localSchema
        installTestSchema(
            database,
            SchemaResponse(
                1,
                PROTOCOL_TEST_SCHEMA_HASH,
                "2026-03-01T00:00:00.000000Z",
                listOf(proofSchemaTable),
            ),
        )
        return PullEnvironment(databaseName, database, PullProcessor(database), table)
    }

    private fun proofRow(recordID: String, value: String, versionTime: String) = buildJsonObject {
        put("id", recordID)
        put("ship_address", value)
        put("user_id", "proof-user")
        put("updated_at", versionTime)
        put("deleted_at", JsonNull)
    }

    private fun insertProofRow(database: SynchroDatabase, recordID: String, value: String) {
        database.writeSyncLockedTransaction {
            it.execSQL(
                "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arrayOf(recordID, value, "proof-user", "2026-03-01T00:00:00.000000Z"),
            )
        }
    }

    private fun rebuildRequest(attempt: LocalRebuildAttempt) = RebuildRequest(
        clientID = "issue-49-client",
        clientGeneration = attempt.clientGeneration,
        schema = SchemaRef(attempt.schemaVersion, attempt.schemaHash),
        scope = attempt.scopeID,
        rebuildID = attempt.rebuildID,
        cursor = attempt.cursor,
        limit = attempt.pageLimit,
    )

    private fun proofHttp(server: MockWebServer) = HttpClient(
        SynchroConfig(
            dbPath = "unused",
            serverURL = server.url("/").toString().trimEnd('/'),
            authProvider = { "token" },
            clientID = "issue-49-client",
            appVersion = "1.0.0",
        ),
    )

    private fun forceLifecycleState(database: SynchroDatabase, state: SyncLifecycleState) {
        database.writeTransaction {
            it.execSQL(
                """
                UPDATE _synchro_client_state
                SET lifecycle_state = ?, error_operation = NULL, error_code = NULL,
                    error_retryable = NULL, error_message = NULL, error_recovery_action = NULL,
                    error_diagnostics = NULL, error_acknowledged = 0
                WHERE singleton = 1
                """.trimIndent(),
                arrayOf(state.wireName),
            )
        }
    }

    private fun queueMatches(observed: List<PendingMutationInspection>, operation: Operation): Boolean {
        val expectedRecords = fixtures.mapTo(mutableSetOf()) { fixture ->
            "${fixture.name}:${if (operation == Operation.INSERT) fixture.insertedID else fixture.seededID}"
        }
        val actualRecords = observed.mapTo(mutableSetOf()) { "${it.tableName}:${it.recordID}" }
        return observed.size == fixtures.size &&
            actualRecords == expectedRecords &&
            observed.all { it.operation == operation && it.status == LocalMutationStatus.PENDING }
    }

    private fun observation(mutation: PendingMutationInspection) = QueueObservation(
        mutation.mutationID,
        mutation.localOrder,
        mutation.tableName,
        mutation.recordID,
        mutation.operation,
        mutation.baseVersion,
        mutation.authoredFields,
    )

    private fun config(databaseName: String, seedPath: String) = SynchroConfig(
        dbPath = databaseName,
        serverURL = "http://127.0.0.1:1",
        authProvider = { "unused" },
        clientID = "issue-49-native-proof",
        appVersion = "1.0.0",
        seedDatabasePath = seedPath,
    )

    private fun canonicalSeedPath(): String {
        val root = File(checkNotNull(System.getProperty("user.dir")))
        val path = listOf(
            File(root, "../../react-native/example/seed.db"),
            File(root, "../react-native/example/seed.db"),
            File(root, "clients/react-native/example/seed.db"),
        ).map(File::getCanonicalFile).firstOrNull(File::isFile)
        checkNotNull(path) { "canonical seed database is unavailable" }
        return path.absolutePath
    }

    private fun temporaryDatabaseName(label: String) =
        "synchro_issue_49_${label}_${UUID.randomUUID()}.sqlite"

    private fun assertNoDatabaseFamily(databaseName: String) {
        val path = context.getDatabasePath(databaseName).absolutePath
        for (suffix in listOf("", "-journal", "-wal", "-shm")) {
            assertFalse(File(path + suffix).exists())
        }
    }
}
