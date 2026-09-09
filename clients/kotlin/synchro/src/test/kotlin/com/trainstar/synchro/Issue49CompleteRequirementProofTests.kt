package com.trainstar.synchro

import android.content.Context
import android.database.sqlite.SQLiteDatabase
import androidx.test.core.app.ApplicationProvider
import java.io.File
import java.util.Collections
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import okhttp3.mockwebserver.SocketPolicy
import org.junit.After
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

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28])
@SQLiteMode(SQLiteMode.Mode.NATIVE)
class Issue49CompleteRequirementProofTests {
    private data class Environment(
        val databaseName: String,
        val database: SynchroDatabase,
        val table: LocalSchemaTable,
    )

    private data class PortableSeed(
        val databaseName: String,
        val path: String,
        val scopeID: String,
        val table: LocalSchemaTable,
        val rowDigest: Integrity.RowDigest,
        val receipt: String,
    )

    private val context = ApplicationProvider.getApplicationContext<Context>()
    private val databases = TestDatabaseTracker()
    private val servers = mutableListOf<MockWebServer>()

    @OptIn(ExperimentalSerializationApi::class)
    private val wireJSON = Json {
        ignoreUnknownKeys = false
        encodeDefaults = true
        explicitNulls = false
    }

    private val ordersTable = protocolOrdersSchemaManifest().localTables().single()

    @After
    fun tearDown() {
        servers.forEach { runCatching { it.shutdown() } }
        servers.clear()
        databases.closeAll()
    }

    @Test
    fun acceptedApplyAndWalEchoRemainSingleAndRejectAChangedEchoAtomically() {
        val scopeID = "orders:echo"
        val environment = environment("apply-echo")
        val database = environment.database
        val tracker = ChangeTracker(database)
        database.writeTransaction { db ->
            SynchroMeta.upsertScope(db, scopeID, "cursor-before-write", null)
        }
        database.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            arrayOf("echo-row", "local", "u1", "2026-04-01T00:00:00.000000Z"),
        )
        val pending = tracker.pendingChanges().single()
        val serverRow = orderRow(
            "echo-row",
            "server-canonical",
            "2026-04-01T01:00:00.000000Z",
        )
        val accepted = makeAcceptedMutation(
            mutationID = pending.mutationID,
            schema = ordersTable,
            pk = orderPK("echo-row"),
            status = MutationStatus.APPLIED,
            serverRow = serverRow,
            serverVersion = "opaque-server-version-1",
        )
        PushProcessor(database, tracker).applyAccepted(
            accepted = listOf(accepted),
            syncedTables = listOf(ordersTable),
            sentPending = mapOf(pending.mutationID to pending),
        )

        val echo = ChangeRecord(
            scope = scopeID,
            table = ordersTable.tableID,
            op = Operation.UPSERT,
            pk = orderPK("echo-row"),
            row = serverRow,
            rowChecksum = accepted.rowChecksum,
            serverVersion = accepted.serverVersion,
        )
        val scopeChecksum = scopeChecksum(scopeID, listOf(echo))
        val pull = PullProcessor(database)
        pull.applyScopeChanges(
            changes = listOf(echo),
            syncedTables = listOf(ordersTable),
            scopeCursors = mapOf(scopeID to "cursor-after-echo"),
            checksums = mapOf(scopeID to scopeChecksum),
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
        )

        assertEquals(0, tracker.pendingChangeCount())
        assertEquals(
            listOf("accepted"),
            database.query("SELECT lifecycle_state FROM _synchro_pending_changes").map { it.getValue("lifecycle_state") },
        )
        assertEquals(
            "server-canonical",
            database.queryOne("SELECT ship_address FROM orders WHERE id = 'echo-row'")?.get("ship_address"),
        )

        val replayBaseline = durableSnapshot(database)
        pull.applyScopeChanges(
            changes = listOf(echo),
            syncedTables = listOf(ordersTable),
            scopeCursors = mapOf(scopeID to "cursor-after-echo"),
            checksums = mapOf(scopeID to scopeChecksum),
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
        )
        assertNoDurableProgress(replayBaseline, database)

        val changedEcho = echo.copy(
            row = orderRow("echo-row", "tampered-echo", "2026-04-01T01:00:00.000000Z"),
        )
        val beforeMutant = durableSnapshot(database)
        assertThrows(SynchroError.InvalidResponse::class.java) {
            pull.applyScopeChanges(
                changes = listOf(changedEcho),
                syncedTables = listOf(ordersTable),
                scopeCursors = mapOf(scopeID to "cursor-mutant"),
                checksums = mapOf(scopeID to scopeChecksum),
                schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            )
        }
        assertNoDurableProgress(beforeMutant, database)
    }

    @Test
    fun rebuildAndAssignmentCleanupRemainScopeLocalAndPreservePendingIntent() {
        val targetScope = "orders:target"
        val otherScope = "orders:other"
        val removedScope = "orders:removed"
        val environment = environment("rebuild-isolation")
        val database = environment.database
        listOf("orphan", "shared", "protected", "local-only").forEach { recordID ->
            insertOrderWithoutCapture(database, recordID, "local-$recordID")
        }
        database.writeTransaction { db ->
            SynchroMeta.upsertScope(db, targetScope, "target-old", null, 3)
            SynchroMeta.upsertScope(db, otherScope, "other-stable", null, 8)
            listOf("orphan", "shared", "protected").forEach { recordID ->
                SynchroMeta.upsertScopeRow(db, targetScope, "orders", recordID, "0".repeat(64), 3)
            }
            SynchroMeta.upsertScopeRow(db, otherScope, "orders", "shared", "1".repeat(64), 8)
        }
        database.execute(
            "UPDATE orders SET ship_address = ? WHERE id = ?",
            arrayOf("protected-local-intent", "protected"),
        )
        val pendingID = ChangeTracker(database).pendingChanges().single().mutationID
        val processor = PullProcessor(database)
        val attempt = processor.beginScopeRebuild(
            targetScope,
            clientGeneration = 4,
            schemaVersion = 1,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            pageLimit = 100,
        )

        assertNull(database.queryOne("SELECT id FROM orders WHERE id = 'orphan'"))
        listOf("shared", "protected", "local-only").forEach { recordID ->
            assertNotNull(database.queryOne("SELECT id FROM orders WHERE id = ?", arrayOf(recordID)))
        }
        database.close()

        val reopened = databases.open(context, environment.databaseName)
        val restarted = PullProcessor(reopened)
        assertEquals(attempt, reopened.readTransaction { SynchroMeta.getRebuildAttempt(it, targetScope) })
        val request = rebuildRequest(attempt)
        val wrongFinal = RebuildResponse(
            scope = targetScope,
            records = emptyList(),
            hasMore = false,
            finalScopeCursor = "target-final",
            checksum = ChecksumObject("sha256", 1, "hex", "f".repeat(64)),
        )
        val beforeWrongFinal = durableSnapshot(reopened)
        assertThrows(RebuildChecksumMismatchException::class.java) {
            restarted.applyScopeRebuildPage(
                attempt,
                request,
                wireJSON.encodeToString(request),
                wrongFinal,
                wireJSON.encodeToString(wrongFinal),
                listOf(ordersTable),
            )
        }
        assertNoDurableProgress(beforeWrongFinal, reopened)

        val finalResponse = wrongFinal.copy(checksum = protocolEmptyScopeChecksum(targetScope))
        restarted.applyScopeRebuildPage(
            attempt,
            request,
            wireJSON.encodeToString(request),
            finalResponse,
            wireJSON.encodeToString(finalResponse),
            listOf(ordersTable),
        )
        assertEquals("target-final", reopened.readTransaction { SynchroMeta.getScope(it, targetScope)?.cursor })
        assertEquals("other-stable", reopened.readTransaction { SynchroMeta.getScope(it, otherScope)?.cursor })
        assertNotNull(reopened.queryOne("SELECT id FROM orders WHERE id = 'shared'"))
        assertNotNull(reopened.queryOne("SELECT id FROM orders WHERE id = 'local-only'"))
        assertEquals(
            "protected-local-intent",
            reopened.queryOne("SELECT ship_address FROM orders WHERE id = 'protected'")?.get("ship_address"),
        )

        reopened.writeTransaction { db ->
            SynchroMeta.upsertScope(db, removedScope, "remove-old", null)
            SynchroMeta.upsertScopeRow(db, removedScope, "orders", "protected", "2".repeat(64), 0)
        }
        val removalMutant = ChangeRecord(
            scope = removedScope,
            table = ordersTable.tableID,
            op = Operation.UPSERT,
            pk = orderPK("protected"),
            row = orderRow("protected", "mutant", "2026-04-02T00:00:00.000000Z"),
            rowChecksum = ChecksumObject("sha256", 1, "hex", "f".repeat(64)),
            serverVersion = "mutant-version",
        )
        val beforeRemovalMutant = durableSnapshot(reopened)
        assertThrows(SynchroError.InvalidResponse::class.java) {
            restarted.applyScopeChanges(
                changes = listOf(removalMutant),
                syncedTables = listOf(ordersTable),
                scopeCursors = emptyMap(),
                checksums = null,
                schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
                scopeUpdates = ScopeAssignmentDelta(emptyList(), listOf(removedScope)),
                scopeSetVersion = 9,
            )
        }
        assertNoDurableProgress(beforeRemovalMutant, reopened)

        restarted.applyScopeChanges(
            changes = emptyList(),
            syncedTables = listOf(ordersTable),
            scopeCursors = emptyMap(),
            checksums = null,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            scopeUpdates = ScopeAssignmentDelta(emptyList(), listOf(removedScope)),
            scopeSetVersion = 9,
        )
        assertNull(reopened.readTransaction { SynchroMeta.getScope(it, removedScope) })
        assertNotNull(reopened.queryOne("SELECT id FROM orders WHERE id = 'protected'"))
        assertEquals(listOf(pendingID), ChangeTracker(reopened).pendingChanges().map { it.mutationID })
    }

    @Test
    fun sqliteApplyFailureCannotAdvanceTheScopeCursorOrLeavePartialRows() {
        val scopeID = "orders:atomic-failure"
        val environment = environment("cursor-atomicity")
        val database = environment.database
        database.writeTransaction { db ->
            SynchroMeta.upsertScope(db, scopeID, "cursor-before", null)
            db.execSQL(
                """
                CREATE TRIGGER reject_issue_49_apply
                BEFORE INSERT ON orders
                WHEN NEW.id = 'fault-row'
                BEGIN
                    SELECT RAISE(ABORT, 'injected apply failure');
                END
                """.trimIndent(),
            )
        }
        val change = canonicalChange(scopeID, "fault-row", "server", "atomic-version")
        val checksum = scopeChecksum(scopeID, listOf(change))
        val beforeFault = durableSnapshot(database)
        assertThrows(Exception::class.java) {
            PullProcessor(database).applyScopeChanges(
                changes = listOf(change),
                syncedTables = listOf(ordersTable),
                scopeCursors = mapOf(scopeID to "cursor-after"),
                checksums = mapOf(scopeID to checksum),
                schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            )
        }
        assertNoDurableProgress(beforeFault, database)
        assertNull(database.queryOne("SELECT id FROM orders WHERE id = 'fault-row'"))
        assertEquals("cursor-before", database.readTransaction { SynchroMeta.getScope(it, scopeID)?.cursor })

        database.writeTransaction { it.execSQL("DROP TRIGGER reject_issue_49_apply") }
        PullProcessor(database).applyScopeChanges(
            changes = listOf(change),
            syncedTables = listOf(ordersTable),
            scopeCursors = mapOf(scopeID to "cursor-after"),
            checksums = mapOf(scopeID to checksum),
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
        )
        database.close()

        val reopened = databases.open(context, environment.databaseName)
        assertNotNull(reopened.queryOne("SELECT id FROM orders WHERE id = 'fault-row'"))
        assertEquals("cursor-after", reopened.readTransaction { SynchroMeta.getScope(it, scopeID)?.cursor })
        assertEquals("atomic-version", reopened.readTransaction { SynchroMeta.getRowVersion(it, "orders", "fault-row") })
    }

    @Test
    fun typedRowsChecksumsVersionsAndTerminalMapsRejectEveryMutatedEncoding() {
        val scopeID = "typed:integrity"
        val table = typedTable()
        val environment = environment("typed-integrity", table)
        val database = environment.database
        database.writeTransaction { SynchroMeta.upsertScope(it, scopeID, "typed-old", null) }
        val pk = buildJsonObject { put("field-id", "typed-row") }
        val validRow = typedRow("typed-row")
        val checksum = Integrity.rowDigest(PROTOCOL_TEST_SCHEMA_HASH, table, pk, validRow, "typed-version").checksum
        val validChange = ChangeRecord(
            scopeID,
            table.tableID,
            Operation.UPSERT,
            pk,
            validRow,
            checksum,
            "typed-version",
        )
        val rowMutants = linkedMapOf<String, JsonObject>(
            "unknown field" to JsonObject(validRow + ("field-unknown" to JsonPrimitive("unknown"))),
            "omitted field" to JsonObject(validRow - "field-title"),
            "physical alias" to JsonObject((validRow - "field-title") + ("title" to JsonPrimitive("alias"))),
            "alternate case" to JsonObject((validRow - "field-title") + ("FIELD-TITLE" to JsonPrimitive("case"))),
            "string as number" to replace(validRow, "field-title", JsonPrimitive(7)),
            "int as string" to replace(validRow, "field-count", JsonPrimitive("7")),
            "int64 as number" to replace(validRow, "field-large-count", JsonPrimitive(8)),
            "boolean as SQLite integer" to replace(validRow, "field-enabled", JsonPrimitive(1)),
            "decimal as JSON number" to replace(validRow, "field-amount", JsonPrimitive(12.3)),
            "JSON as object" to replace(validRow, "field-document", buildJsonObject { put("a", 1) }),
            "bytes as padded base64" to replace(validRow, "field-payload", JsonPrimitive("AAEC=")),
            "datetime as number" to replace(validRow, "field-updated-at", JsonPrimitive(1_775_001_600)),
        )
        for ((name, row) in rowMutants) {
            val before = durableSnapshot(database, listOf(table.tableName))
            val failure = runCatching {
                PullProcessor(database).applyScopeChanges(
                    changes = listOf(validChange.copy(row = row)),
                    syncedTables = listOf(table),
                    scopeCursors = mapOf(scopeID to "cursor-$name"),
                    checksums = null,
                    schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
                )
            }.exceptionOrNull()
            assertTrue("accepted typed-row mutant: $name", failure is SynchroError.InvalidResponse)
            assertNoDurableProgress(before, database, listOf(table.tableName))
        }

        val identityMutants = listOf(
            validChange.copy(pk = buildJsonObject { put("field-id", "different-row") }),
            validChange.copy(serverVersion = "different-version"),
            validChange.copy(rowChecksum = ChecksumObject("sha256", 1, "hex", "f".repeat(64))),
        )
        identityMutants.forEach { mutant ->
            val before = durableSnapshot(database, listOf(table.tableName))
            assertThrows(SynchroError.InvalidResponse::class.java) {
                PullProcessor(database).applyScopeChanges(
                    changes = listOf(mutant),
                    syncedTables = listOf(table),
                    scopeCursors = mapOf(scopeID to "identity-mutant"),
                    checksums = null,
                    schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
                )
            }
            assertNoDurableProgress(before, database, listOf(table.tableName))
        }

        val beforeDuplicate = durableSnapshot(database, listOf(table.tableName))
        assertThrows(IllegalArgumentException::class.java) {
            Integrity.validateCanonicalWireJSON("{\"field-id\":\"typed-row\",\"field-id\":\"duplicate\"}")
        }
        assertNoDurableProgress(beforeDuplicate, database, listOf(table.tableName))

        val incompleteMaps = listOf(
            PullResponse(
                emptyList(),
                1,
                emptyMap(),
                ScopeAssignmentDelta(emptyList(), emptyList()),
                emptyList(),
                false,
                emptyMap(),
            ),
            PullResponse(
                emptyList(),
                1,
                emptyMap(),
                ScopeAssignmentDelta(emptyList(), emptyList()),
                emptyList(),
                false,
                mapOf(
                    scopeID to protocolEmptyScopeChecksum(scopeID),
                    "typed:extra" to protocolEmptyScopeChecksum("typed:extra"),
                ),
            ),
        )
        incompleteMaps.forEach { response ->
            val before = durableSnapshot(database, listOf(table.tableName))
            assertThrows(ContractException::class.java) { response.validate(setOf(scopeID), 1) }
            assertNoDurableProgress(before, database, listOf(table.tableName))
        }

        PullProcessor(database).applyScopeChanges(
            changes = listOf(validChange),
            syncedTables = listOf(table),
            scopeCursors = mapOf(scopeID to "must-not-install"),
            checksums = mapOf(scopeID to protocolEmptyScopeChecksum(scopeID)),
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
        )
        val invalidated = database.readTransaction { SynchroMeta.getScope(it, scopeID)!! }
        assertNull(invalidated.cursor)
        assertNull(invalidated.checksum)
        assertNotEquals("must-not-install", invalidated.cursor)
    }

    @Test
    fun queueTerminalStatesAndRetryFaultsPreserveTheSealedRequestAndLedger() = runTest {
        val environment = environment("queue-terminal")
        var database = environment.database
        var tracker = ChangeTracker(database)
        var processor = PushProcessor(database, tracker)
        database.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            arrayOf("queue-row", "first", "u1", "2026-04-04T00:00:00.000000Z"),
        )
        database.execute("UPDATE orders SET ship_address = ? WHERE id = ?", arrayOf("normalized", "queue-row"))

        val requestBodies = Collections.synchronizedList(mutableListOf<String>())
        val responseIndex = AtomicInteger()
        val acceptedOutcome = AtomicReference<AcceptedMutation>()
        val retryServer = server { request ->
            val body = request.body.readUtf8()
            requestBodies += body
            when (responseIndex.getAndIncrement()) {
                0 -> retryResponse(429, RETRYABLE_429_ERROR_JSON)
                1 -> retryResponse(503, RETRYABLE_503_ERROR_JSON)
                2 -> MockResponse()
                    .setBody("truncated")
                    .setSocketPolicy(SocketPolicy.DISCONNECT_DURING_RESPONSE_BODY)
                else -> {
                    val push = wireJSON.decodeFromString<PushRequest>(body)
                    val accepted = makeAcceptedMutation(
                        mutationID = push.mutations.single().mutationID,
                        schema = ordersTable,
                        pk = orderPK("queue-row"),
                        status = MutationStatus.APPLIED,
                        serverRow = orderRow("queue-row", "normalized", "2026-04-04T01:00:00.000000Z"),
                        serverVersion = "queue-server-version",
                    )
                    acceptedOutcome.set(accepted)
                    jsonResponse(PushResponse(push.batchID, "2026-04-04T01:00:00.000000Z", listOf(accepted), emptyList()))
                }
            }
        }
        val http = http(retryServer)
        assertTrue(pushFailure(processor, http, environment.table) is RetryableError)
        val sealedBaseline = durableSnapshot(database)
        assertEquals("pending", database.queryOne("SELECT state FROM _synchro_push_batches")?.get("state"))
        assertTrue(pushFailure(processor, http, environment.table) is RetryableError)
        assertNoDurableProgress(sealedBaseline, database)
        assertTrue(pushFailure(processor, http, environment.table) is RetryableError)
        assertNoDurableProgress(sealedBaseline, database)
        assertEquals(3, requestBodies.size)
        assertTrue(requestBodies.all { it == requestBodies.first() })

        database.close()
        database = databases.open(context, environment.databaseName)
        tracker = ChangeTracker(database)
        processor = PushProcessor(database, tracker)
        assertNoDurableProgress(sealedBaseline, database)

        database.execute("UPDATE orders SET ship_address = ? WHERE id = ?", arrayOf("successor", "queue-row"))
        processor.processPush(
            http,
            "issue-49-client",
            1,
            1,
            PROTOCOL_TEST_SCHEMA_HASH,
            listOf(environment.table),
        )
        assertEquals(requestBodies.first(), requestBodies.last())
        assertEquals(
            listOf("superseded_before_send", "superseded_before_send", "accepted", "captured"),
            database.query("SELECT lifecycle_state FROM _synchro_pending_changes ORDER BY local_order")
                .map { it.getValue("lifecycle_state") },
        )
        assertEquals("completed", database.queryOne("SELECT state FROM _synchro_push_batches")?.get("state"))
        assertEquals("successor", database.queryOne("SELECT ship_address FROM orders WHERE id = 'queue-row'")?.get("ship_address"))

        val changedOutcome = acceptedOutcome.get().let { accepted ->
            val changedRow = orderRow("queue-row", "different-terminal", "2026-04-04T02:00:00.000000Z")
            accepted.copy(
                serverRow = changedRow,
                rowChecksum = Integrity.rowDigest(
                    PROTOCOL_TEST_SCHEMA_HASH,
                    ordersTable,
                    orderPK("queue-row"),
                    changedRow,
                    accepted.serverVersion,
                ).checksum,
            )
        }
        val beforeChangedTerminal = durableSnapshot(database)
        assertThrows(SynchroError.InvalidResponse::class.java) {
            processor.applyAccepted(listOf(changedOutcome), listOf(ordersTable))
        }
        assertNoDurableProgress(beforeChangedTerminal, database)

        val cancelled = environment("queue-cancelled")
        cancelled.database.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            arrayOf("cancelled", "temporary", "u1", "2026-04-04T00:00:00.000000Z"),
        )
        cancelled.database.execute("DELETE FROM orders WHERE id = 'cancelled'")
        assertNull(
            PushProcessor(cancelled.database, ChangeTracker(cancelled.database)).processPush(
                http,
                "issue-49-client",
                1,
                1,
                PROTOCOL_TEST_SCHEMA_HASH,
                listOf(cancelled.table),
            ),
        )
        assertEquals(
            listOf("cancelled_before_send", "cancelled_before_send"),
            cancelled.database.query("SELECT lifecycle_state FROM _synchro_pending_changes ORDER BY local_order")
                .map { it.getValue("lifecycle_state") },
        )

        val conflictStates = applyConflictWithBlockedSuccessor()
        val rejectedState = applyTerminalRejection()
        assertEquals(listOf("conflict", "blocked_by_predecessor"), conflictStates)
        assertEquals("rejected_terminal", rejectedState)
    }

    @Test
    fun interruptedSchemaMigrationRejectsJournalTamperingBeforeAnySchemaOrCursorProgress() {
        val scopeID = "orders:migration"
        val environment = environment("migration-interruption")
        val database = environment.database
        database.writeTransaction { SynchroMeta.upsertScope(it, scopeID, "schema-old-cursor", null) }
        val target = targetManifest()
        val manager = SchemaManager(database)
        manager.prepareConnectMigration(
            response = migrationResponse(target, mapOf(scopeID to "schema-new-cursor")),
            targetTables = target.localTables(),
            resetMaterialization = false,
        )
        val originalHash = database.queryOne(
            "SELECT migration_plan_hash FROM _synchro_migration_journal WHERE singleton = 1",
        )?.get("migration_plan_hash") as String
        database.close()

        val reopened = databases.open(context, environment.databaseName)
        reopened.writeTransaction { db ->
            db.execSQL(
                "UPDATE _synchro_migration_journal SET migration_plan_hash = ? WHERE singleton = 1",
                arrayOf("f".repeat(64)),
            )
        }
        val beforeRecovery = durableSnapshot(reopened)
        assertThrows(SynchroError.InvalidResponse::class.java) {
            SchemaManager(reopened).recoverPendingMigration()
        }
        assertNoDurableProgress(beforeRecovery, reopened)
        assertEquals(1L, reopened.readTransaction { SynchroMeta.getInt64(it, MetaKey.SCHEMA_VERSION) })
        assertEquals("schema-old-cursor", reopened.readTransaction { SynchroMeta.getScope(it, scopeID)?.cursor })
        assertFalse(reopened.query("PRAGMA table_info(orders)").any { it["name"] == "notes" })

        reopened.writeTransaction { db ->
            db.execSQL(
                "UPDATE _synchro_migration_journal SET migration_plan_hash = ? WHERE singleton = 1",
                arrayOf(originalHash),
            )
        }
        SchemaManager(reopened).recoverPendingMigration()
        assertEquals(2L, reopened.readTransaction { SynchroMeta.getInt64(it, MetaKey.SCHEMA_VERSION) })
        assertEquals(target.schemaHash, reopened.readTransaction { SynchroMeta.get(it, MetaKey.SCHEMA_HASH) })
        assertEquals("schema-new-cursor", reopened.readTransaction { SynchroMeta.getScope(it, scopeID)?.cursor })
        assertTrue(reopened.query("PRAGMA table_info(orders)").any { it["name"] == "notes" })
    }

    @Test
    fun portableSeedContinuesThroughNormalSyncAndTamperingNeverPublishesAClientDatabase() = runTest {
        val tamperedSeed = createPortableSeed("seed-tampered")
        SQLiteDatabase.openDatabase(tamperedSeed.path, null, SQLiteDatabase.OPEN_READWRITE).use { raw ->
            raw.execSQL("DROP TRIGGER _synchro_cdc_update_orders")
            raw.execSQL("UPDATE orders SET ship_address = 'tampered without digest update' WHERE id = 'seed-row'")
        }
        val rejectedDestination = databaseName("seed-rejected")
        assertThrows(Exception::class.java) {
            SynchroClient(seedConfig(rejectedDestination, tamperedSeed.path, "http://127.0.0.1:1"), context)
        }
        assertNoDatabaseFamily(rejectedDestination)

        val seed = createPortableSeed("seed-valid")
        val newChange = canonicalChange(
            seed.scopeID,
            "continued-row",
            "continued",
            "continued-version",
        )
        val finalChecksum = Integrity.scopeDigest(
            PROTOCOL_TEST_SCHEMA_HASH,
            seed.scopeID,
            listOf(
                seed.rowDigest.identity to seed.rowDigest.checksum,
                Integrity.rowDigest(
                    PROTOCOL_TEST_SCHEMA_HASH,
                    seed.table,
                    newChange.pk,
                    requireNotNull(newChange.row),
                    newChange.serverVersion,
                ).let { it.identity to it.checksum },
            ),
        )
        val requests = Collections.synchronizedList(mutableListOf<Pair<String, String>>())
        val syncServer = server { request ->
            val body = request.body.readUtf8()
            requests += request.path.orEmpty() to body
            when {
                request.path.orEmpty().endsWith("/sync/connect") -> jsonResponse(
                    ConnectResponse(
                        serverTime = "2026-04-06T00:00:00.000000Z",
                        protocolVersion = 3,
                        clientGeneration = 1,
                        scopeSetVersion = 7,
                        schema = SchemaDescriptor(1, PROTOCOL_TEST_SCHEMA_HASH, SchemaAction.NONE),
                        scopes = ScopeAssignmentDelta(emptyList(), emptyList()),
                        scopeCursorUpdates = mapOf(seed.scopeID to "seed-base-cursor"),
                    ),
                )
                request.path.orEmpty().endsWith("/sync/pull") -> jsonResponse(
                    PullResponse(
                        changes = listOf(newChange),
                        scopeSetVersion = 7,
                        scopeCursors = mapOf(seed.scopeID to "seed-next-cursor"),
                        scopeUpdates = ScopeAssignmentDelta(emptyList(), emptyList()),
                        rebuild = emptyList(),
                        hasMore = false,
                        checksums = mapOf(seed.scopeID to finalChecksum),
                    ),
                )
                else -> MockResponse().setResponseCode(500)
            }
        }
        val destination = databaseName("seed-continuation")
        val client = SynchroClient(seedConfig(destination, seed.path, syncServer.url("/").toString().trimEnd('/')), context)
        try {
            client.start()
            val connect = wireJSON.decodeFromString<ConnectRequest>(
                requests.single { it.first.endsWith("/sync/connect") }.second,
            )
            assertEquals(mapOf(seed.scopeID to seed.receipt), connect.seedReceipts)
            assertEquals(null, connect.knownScopes.getValue(seed.scopeID).cursor)
            val pull = wireJSON.decodeFromString<PullRequest>(
                requests.single { it.first.endsWith("/sync/pull") }.second,
            )
            assertEquals("seed-base-cursor", pull.scopes.getValue(seed.scopeID).cursor)
            assertEquals("Seeded Address", client.queryOne("SELECT ship_address FROM orders WHERE id = 'seed-row'")?.get("ship_address"))
            assertEquals("continued", client.queryOne("SELECT ship_address FROM orders WHERE id = 'continued-row'")?.get("ship_address"))
            withDatabase(destination) { database ->
                assertTrue(database.query("SELECT * FROM _synchro_seed_receipts").isEmpty())
                assertEquals("seed-next-cursor", database.readTransaction { SynchroMeta.getScope(it, seed.scopeID)?.cursor })
            }
        } finally {
            client.stop()
            client.close()
            context.deleteDatabase(destination)
            context.deleteDatabase(seed.databaseName)
            context.deleteDatabase(tamperedSeed.databaseName)
        }
    }

    @Test
    fun missingScopeCursorAutomaticallyRebuildsWhileInvalidFinalityMakesNoVerifiedProgress() = runTest {
        val scopeID = "orders:auto-rebuild"
        val failing = environment("automatic-rebuild-failure")
        failing.database.writeTransaction { SynchroMeta.upsertScope(it, scopeID, null, null) }
        val failingServer = server { request ->
            when {
                request.path.orEmpty().endsWith("/sync/connect") -> jsonResponse(connectNone(scopeSetVersion = 0))
                request.path.orEmpty().endsWith("/sync/rebuild") -> jsonResponse(
                    RebuildResponse(
                        scope = "orders:wrong-scope",
                        records = emptyList(),
                        hasMore = false,
                        finalScopeCursor = "must-not-install",
                        checksum = protocolEmptyScopeChecksum("orders:wrong-scope"),
                    ),
                )
                else -> MockResponse().setResponseCode(500)
            }
        }
        val failingEngine = engine(failing.databaseName, failing.database, failingServer)
        val failure = runCatching { failingEngine.start() }.exceptionOrNull()
        assertNotNull(failure)
        assertNull(failing.database.readTransaction { SynchroMeta.getScope(it, scopeID)?.cursor })
        assertNull(failing.database.readTransaction { SynchroMeta.getScope(it, scopeID)?.checksum })
        assertTrue(failing.database.query("SELECT * FROM _synchro_rebuild_page_receipts").isEmpty())
        assertTrue(failing.database.query("SELECT * FROM orders").isEmpty())
        runCatching { failingEngine.stop() }

        val success = environment("automatic-rebuild-success")
        success.database.writeTransaction { SynchroMeta.upsertScope(it, scopeID, null, null) }
        val paths = Collections.synchronizedList(mutableListOf<String>())
        val successServer = server { request ->
            paths += request.path.orEmpty()
            when {
                request.path.orEmpty().endsWith("/sync/connect") -> jsonResponse(connectNone(scopeSetVersion = 0))
                request.path.orEmpty().endsWith("/sync/rebuild") -> jsonResponse(
                    RebuildResponse(
                        scope = scopeID,
                        records = emptyList(),
                        hasMore = false,
                        finalScopeCursor = "rebuilt-cursor",
                        checksum = protocolEmptyScopeChecksum(scopeID),
                    ),
                )
                request.path.orEmpty().endsWith("/sync/pull") -> jsonResponse(
                    PullResponse(
                        changes = emptyList(),
                        scopeSetVersion = 0,
                        scopeCursors = mapOf(scopeID to "post-rebuild-cursor"),
                        scopeUpdates = ScopeAssignmentDelta(emptyList(), emptyList()),
                        rebuild = emptyList(),
                        hasMore = false,
                        checksums = mapOf(scopeID to protocolEmptyScopeChecksum(scopeID)),
                    ),
                )
                else -> MockResponse().setResponseCode(500)
            }
        }
        val successEngine = engine(success.databaseName, success.database, successServer)
        try {
            successEngine.start()
            val rebuildIndex = paths.indexOfFirst { it.endsWith("/sync/rebuild") }
            val pullIndex = paths.indexOfFirst { it.endsWith("/sync/pull") }
            assertTrue(rebuildIndex >= 0)
            assertTrue(pullIndex > rebuildIndex)
            assertEquals(
                "post-rebuild-cursor",
                success.database.readTransaction { SynchroMeta.getScope(it, scopeID)?.cursor },
            )
            assertNull(success.database.readTransaction { SynchroMeta.getRebuildAttempt(it, scopeID) })
        } finally {
            successEngine.stop()
        }
    }

    @Test
    fun historicalOutcomeReplayUsesTheSealedSchemaAndRejectsAnyChangedReplay() = runTest {
        val oldTable = projectionTable("legacy_title", "legacy_enabled")
        val currentTable = projectionTable("title", "enabled")
        val oldHash = PROTOCOL_TEST_SCHEMA_HASH
        val currentHash = "1".repeat(64)
        val environment = environment("historical-outcome", oldTable, oldHash)
        var database = environment.database
        database.execute(
            "INSERT INTO projection_orders (id, legacy_title, legacy_enabled, updated_at) VALUES (?, ?, ?, ?)",
            arrayOf("history-row", "authored", 0L, "2026-04-08T00:00:00.000000Z"),
        )
        var processor = PushProcessor(database, ChangeTracker(database))
        val responseIndex = AtomicInteger()
        val validOutcome = AtomicReference<AcceptedMutation>()
        val historyServer = server { request ->
            val push = wireJSON.decodeFromString<PushRequest>(request.body.readUtf8())
            when (responseIndex.getAndIncrement()) {
                0 -> retryResponse(503, RETRYABLE_503_ERROR_JSON)
                else -> {
                    val row = projectionRow(oldTable, "history-row", "server", true)
                    val accepted = AcceptedMutation(
                        mutationID = push.mutations.single().mutationID,
                        table = oldTable.tableID,
                        pk = buildJsonObject { put("field-id", "history-row") },
                        outcomeSchema = SchemaRef(1, oldHash),
                        status = MutationStatus.APPLIED,
                        serverRow = row,
                        rowChecksum = Integrity.rowDigest(
                            oldHash,
                            oldTable,
                            buildJsonObject { put("field-id", "history-row") },
                            row,
                            "historical-version",
                        ).checksum,
                        serverVersion = "historical-version",
                    )
                    validOutcome.set(accepted)
                    val outcome = if (responseIndex.get() == 2) {
                        accepted.copy(rowChecksum = ChecksumObject("sha256", 1, "hex", "f".repeat(64)))
                    } else {
                        accepted
                    }
                    jsonResponse(PushResponse(push.batchID, "2026-04-08T01:00:00.000000Z", listOf(outcome), emptyList()))
                }
            }
        }
        val http = http(historyServer)
        assertTrue(pushFailure(processor, http, oldTable, oldHash) is RetryableError)
        installTestSchema(database, 2, currentHash, listOf(currentTable))
        database.execute("UPDATE projection_orders SET title = ? WHERE id = ?", arrayOf("later-local", "history-row"))

        val beforeWrongHistorical = durableSnapshot(database, listOf("projection_orders"))
        val wrongHistoricalFailure = runCatching {
            processor.processPush(http, "issue-49-client", 1, 2, currentHash, listOf(currentTable))
        }.exceptionOrNull()
        assertTrue(wrongHistoricalFailure is SynchroError.InvalidResponse)
        assertNoDurableProgress(beforeWrongHistorical, database, listOf("projection_orders"))

        processor.processPush(http, "issue-49-client", 1, 2, currentHash, listOf(currentTable))
        assertEquals("later-local", database.queryOne("SELECT title FROM projection_orders WHERE id = 'history-row'")?.get("title"))
        assertEquals(1L, database.queryOne("SELECT enabled FROM projection_orders WHERE id = 'history-row'")?.get("enabled"))
        assertEquals("completed", database.queryOne("SELECT state FROM _synchro_push_batches")?.get("state"))

        database.close()
        database = databases.open(context, environment.databaseName)
        processor = PushProcessor(database, ChangeTracker(database))
        val replayBaseline = durableSnapshot(database, listOf("projection_orders"))
        processor.applyAccepted(listOf(validOutcome.get()), listOf(currentTable))
        assertNoDurableProgress(replayBaseline, database, listOf("projection_orders"))

        val changedRow = projectionRow(oldTable, "history-row", "changed-replay", true)
        val changedReplay = validOutcome.get().copy(
            serverRow = changedRow,
            rowChecksum = Integrity.rowDigest(
                oldHash,
                oldTable,
                buildJsonObject { put("field-id", "history-row") },
                changedRow,
                "historical-version",
            ).checksum,
        )
        val beforeChangedReplay = durableSnapshot(database, listOf("projection_orders"))
        assertThrows(SynchroError.InvalidResponse::class.java) {
            processor.applyAccepted(listOf(changedReplay), listOf(currentTable))
        }
        assertNoDurableProgress(beforeChangedReplay, database, listOf("projection_orders"))
    }

    private fun environment(
        label: String,
        table: LocalSchemaTable = ordersTable,
        schemaHash: String = PROTOCOL_TEST_SCHEMA_HASH,
    ): Environment {
        val name = databaseName(label)
        val database = databases.open(context, name)
        installTestSchema(database, 1, schemaHash, listOf(table))
        return Environment(name, database, table)
    }

    private fun orderPK(recordID: String) = buildJsonObject { put("field-id", recordID) }

    private fun orderRow(recordID: String, address: String, updatedAt: String) = buildJsonObject {
        put("field-id", recordID)
        put("field-ship-address", address)
        put("field-user-id", "u1")
        put("field-updated-at", updatedAt)
        put("field-deleted-at", JsonNull)
    }

    private fun canonicalChange(scopeID: String, recordID: String, address: String, serverVersion: String): ChangeRecord {
        val pk = orderPK(recordID)
        val row = orderRow(recordID, address, "2026-04-01T00:00:00.000000Z")
        return ChangeRecord(
            scopeID,
            ordersTable.tableID,
            Operation.UPSERT,
            pk,
            row,
            Integrity.rowDigest(PROTOCOL_TEST_SCHEMA_HASH, ordersTable, pk, row, serverVersion).checksum,
            serverVersion,
        )
    }

    private fun scopeChecksum(scopeID: String, changes: List<ChangeRecord>): ChecksumObject =
        Integrity.scopeDigest(
            PROTOCOL_TEST_SCHEMA_HASH,
            scopeID,
            changes.map { change ->
                Integrity.rowDigest(
                    PROTOCOL_TEST_SCHEMA_HASH,
                    ordersTable,
                    change.pk,
                    requireNotNull(change.row),
                    change.serverVersion,
                ).let { it.identity to it.checksum }
            },
        )

    private fun insertOrderWithoutCapture(database: SynchroDatabase, recordID: String, address: String) {
        database.writeSyncLockedTransaction { db ->
            db.execSQL(
                "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arrayOf(recordID, address, "u1", "2026-04-01T00:00:00.000000Z"),
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

    private fun typedTable() = LocalSchemaTable(
        tableID = "table-typed-orders",
        relationID = "relation-typed-orders",
        tableName = "typed_orders",
        primaryKeyFieldID = "field-id",
        updatedAtFieldID = "field-updated-at",
        deletedAtFieldID = "field-deleted-at",
        updatedAtColumn = "updated_at",
        deletedAtColumn = "deleted_at",
        composition = CompositionClass.SINGLE_SCOPE,
        primaryKey = listOf("id"),
        columns = listOf(
            LocalSchemaColumn("field-id", "id", "string", false, false, isPrimaryKey = true),
            LocalSchemaColumn("field-title", "title", "string", false, true, isPrimaryKey = false),
            LocalSchemaColumn("field-count", "count", "int", true, true, isPrimaryKey = false),
            LocalSchemaColumn("field-large-count", "large_count", "int64", true, true, isPrimaryKey = false),
            LocalSchemaColumn("field-enabled", "enabled", "boolean", true, true, isPrimaryKey = false),
            LocalSchemaColumn(
                "field-amount",
                "amount",
                "decimal",
                true,
                true,
                precision = 5,
                scale = 2,
                isPrimaryKey = false,
            ),
            LocalSchemaColumn("field-document", "document", "json", true, true, isPrimaryKey = false),
            LocalSchemaColumn("field-score", "score", "float", true, true, isPrimaryKey = false),
            LocalSchemaColumn("field-payload", "payload", "bytes", true, true, isPrimaryKey = false),
            LocalSchemaColumn("field-updated-at", "updated_at", "datetime", false, false, isPrimaryKey = false),
            LocalSchemaColumn("field-deleted-at", "deleted_at", "datetime", true, false, isPrimaryKey = false),
        ),
    )

    private fun typedRow(recordID: String) = buildJsonObject {
        put("field-id", recordID)
        put("field-title", "canonical")
        put("field-count", 7)
        put("field-large-count", "8")
        put("field-enabled", true)
        put("field-amount", "12.3")
        put("field-document", "{\"a\":1}")
        put("field-score", 1.25)
        put("field-payload", "AAEC")
        put("field-updated-at", "2026-04-03T00:00:00.000000Z")
        put("field-deleted-at", JsonNull)
    }

    private fun replace(row: JsonObject, key: String, value: kotlinx.serialization.json.JsonElement): JsonObject =
        JsonObject(row.toMutableMap().also { it[key] = value })

    private suspend fun pushFailure(
        processor: PushProcessor,
        http: HttpClient,
        table: LocalSchemaTable,
        schemaHash: String = PROTOCOL_TEST_SCHEMA_HASH,
    ): Throwable? = runCatching {
        processor.processPush(http, "issue-49-client", 1, 1, schemaHash, listOf(table))
    }.exceptionOrNull()

    private fun applyConflictWithBlockedSuccessor(): List<String> {
        val environment = environment("queue-conflict")
        val database = environment.database
        insertOrderWithoutCapture(database, "conflict-row", "server-base")
        database.writeTransaction {
            SynchroMeta.upsertRowVersion(it, "orders", "conflict-row", "conflict-base", null)
        }
        database.execute("UPDATE orders SET ship_address = 'first-local' WHERE id = 'conflict-row'")
        val tracker = ChangeTracker(database)
        val captured = tracker.pendingChanges().single()
        val batchID = UUID.randomUUID().toString()
        database.writeTransaction { db ->
            db.execSQL(
                "UPDATE _synchro_pending_changes SET lifecycle_state = 'sealed', sealed_batch_id = ?, sealed_ordinal = 0 WHERE mutation_id = ?",
                arrayOf(batchID, captured.mutationID),
            )
        }
        val sealed = captured.copy(lifecycleState = "sealed", sealedBatchID = batchID, sealedOrdinal = 0)
        database.execute("UPDATE orders SET ship_address = 'later-local' WHERE id = 'conflict-row'")
        val serverRow = orderRow("conflict-row", "server-conflict", "2026-04-05T00:00:00.000000Z")
        val rejection = makeRejectedMutation(
            mutationID = sealed.mutationID,
            schema = ordersTable,
            pk = orderPK("conflict-row"),
            status = MutationStatus.CONFLICT,
            code = MutationRejectionCode.VERSION_CONFLICT,
            message = "conflict",
            serverRow = serverRow,
            serverVersion = "conflict-server-version",
        )
        val processor = PushProcessor(database, tracker)
        val beforeWrongID = durableSnapshot(database)
        assertThrows(SynchroError.InvalidResponse::class.java) {
            processor.applyRejected(
                listOf(rejection.copy(mutationID = UUID.randomUUID().toString())),
                listOf(ordersTable),
                mapOf(sealed.mutationID to sealed),
            )
        }
        assertNoDurableProgress(beforeWrongID, database)
        processor.applyRejected(listOf(rejection), listOf(ordersTable), mapOf(sealed.mutationID to sealed))
        database.close()
        val reopened = databases.open(context, environment.databaseName)
        assertEquals(
            sealed.mutationID,
            reopened.readTransaction { SynchroMeta.listRejectedMutations(it).single().mutationID },
        )
        return reopened.query("SELECT lifecycle_state FROM _synchro_pending_changes ORDER BY local_order")
            .map { it.getValue("lifecycle_state") as String }
    }

    private fun applyTerminalRejection(): String {
        val environment = environment("queue-rejected")
        val database = environment.database
        database.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            arrayOf("rejected-row", "local", "u1", "2026-04-05T00:00:00.000000Z"),
        )
        val tracker = ChangeTracker(database)
        val source = tracker.pendingChanges().single()
        val rejection = makeRejectedMutation(
            mutationID = source.mutationID,
            schema = ordersTable,
            pk = orderPK("rejected-row"),
            status = MutationStatus.REJECTED_TERMINAL,
            code = MutationRejectionCode.POLICY_REJECTED,
            message = "policy",
        )
        val processor = PushProcessor(database, tracker)
        val beforeWrongTable = durableSnapshot(database)
        assertThrows(SynchroError.InvalidResponse::class.java) {
            processor.applyRejected(listOf(rejection.copy(table = "wrong-table")), listOf(ordersTable))
        }
        assertNoDurableProgress(beforeWrongTable, database)
        processor.applyRejected(listOf(rejection), listOf(ordersTable))
        database.close()
        val reopened = databases.open(context, environment.databaseName)
        assertEquals(
            source.mutationID,
            reopened.readTransaction { SynchroMeta.listRejectedMutations(it).single().mutationID },
        )
        return reopened.queryOne("SELECT lifecycle_state FROM _synchro_pending_changes")?.get("lifecycle_state") as String
    }

    private fun targetManifest(): SchemaManifest {
        val draft = protocolOrdersSchemaManifest(
            includeNotes = true,
            schemaVersion = 2,
            parentSchema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
            transitionClass = "class_3",
            compatibilityFloor = 2,
        )
        return draft.copy(schemaHash = Integrity.schemaManifestHash(draft))
    }

    private fun migrationResponse(target: SchemaManifest, cursors: Map<String, String?>) = ConnectResponse(
        serverTime = "2026-04-05T00:00:00.000000Z",
        protocolVersion = 3,
        clientGeneration = 1,
        scopeSetVersion = 0,
        schema = SchemaDescriptor(target.schemaVersion, target.schemaHash, SchemaAction.REPLACE),
        scopes = ScopeAssignmentDelta(emptyList(), emptyList()),
        scopeCursorUpdates = cursors,
        schemaDefinition = target,
    )

    private fun createPortableSeed(label: String): PortableSeed {
        val databaseName = databaseName(label)
        val database = SynchroDatabase.open(context, databaseName)
        val scopeID = "orders:portable"
        val row = orderRow("seed-row", "Seeded Address", "2026-04-06T00:00:00.000000Z")
        val pk = orderPK("seed-row")
        val digest = Integrity.rowDigest(PROTOCOL_TEST_SCHEMA_HASH, ordersTable, pk, row, "seed-server-version")
        val scopeChecksum = Integrity.scopeDigest(
            PROTOCOL_TEST_SCHEMA_HASH,
            scopeID,
            listOf(digest.identity to digest.checksum),
        )
        val checksumJSON = wireJSON.encodeToString(scopeChecksum)
        val receipt = "sc1.issue-49-portable-receipt.signature"
        try {
            installTestSchema(database, 1, PROTOCOL_TEST_SCHEMA_HASH, listOf(ordersTable))
            database.writeSyncLockedTransaction { db ->
                db.execSQL(
                    "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                    arrayOf("seed-row", "Seeded Address", "u1", "2026-04-06T00:00:00.000000Z"),
                )
                SynchroMeta.upsertRowVersion(db, "orders", "seed-row", "seed-server-version", digest.checksum)
                SynchroMeta.upsertScope(db, scopeID, null, checksumJSON, 0, checksumJSON)
                SynchroMeta.upsertScopeRow(db, scopeID, "orders", "seed-row", digest.checksum.digest, 0)
                db.execSQL(
                    """
                    INSERT INTO _synchro_seed_receipts
                        (scope_id, receipt, schema_version, schema_hash, cardinality, checksum)
                    VALUES (?, ?, 1, ?, 1, ?)
                    """.trimIndent(),
                    arrayOf(scopeID, receipt, PROTOCOL_TEST_SCHEMA_HASH, checksumJSON),
                )
                val manifest = protocolOrdersSchemaManifest().copy(schemaHash = PROTOCOL_TEST_SCHEMA_HASH)
                SynchroMeta.set(db, MetaKey.SCHEMA_MANIFEST, wireJSON.encodeToString(manifest))
                SynchroMeta.setInt64(db, MetaKey.SCOPE_SET_VERSION, 7)
                SynchroMeta.set(db, MetaKey.SNAPSHOT_COMPLETE, "1")
            }
        } finally {
            database.close()
        }
        return PortableSeed(databaseName, context.getDatabasePath(databaseName).absolutePath, scopeID, ordersTable, digest, receipt)
    }

    private fun seedConfig(databaseName: String, seedPath: String, serverURL: String) = SynchroConfig(
        dbPath = databaseName,
        serverURL = serverURL,
        authProvider = { "token" },
        clientID = "issue-49-client",
        appVersion = "1.0.0",
        syncInterval = 999.0,
        seedDatabasePath = seedPath,
    )

    private fun assertNoDatabaseFamily(databaseName: String) {
        val destination = context.getDatabasePath(databaseName)
        listOf("", "-journal", "-wal", "-shm").forEach { suffix ->
            assertFalse(File(destination.path + suffix).exists())
        }
        val prefix = ".${destination.name}.seed-"
        assertTrue(destination.parentFile?.listFiles().orEmpty().none { it.name.startsWith(prefix) })
    }

    private fun connectNone(scopeSetVersion: Long) = ConnectResponse(
        serverTime = "2026-04-07T00:00:00.000000Z",
        protocolVersion = 3,
        clientGeneration = 1,
        scopeSetVersion = scopeSetVersion,
        schema = SchemaDescriptor(1, PROTOCOL_TEST_SCHEMA_HASH, SchemaAction.NONE),
        scopes = ScopeAssignmentDelta(emptyList(), emptyList()),
        scopeCursorUpdates = emptyMap(),
    )

    private fun engine(databaseName: String, database: SynchroDatabase, server: MockWebServer): SyncEngine {
        val config = SynchroConfig(
            dbPath = databaseName,
            serverURL = server.url("/").toString().trimEnd('/'),
            authProvider = { "token" },
            clientID = "issue-49-client",
            appVersion = "1.0.0",
            syncInterval = 999.0,
            maxRetryAttempts = 0,
        )
        val tracker = ChangeTracker(database)
        return SyncEngine(
            config,
            database,
            HttpClient(config, OkHttpClient()),
            SchemaManager(database),
            tracker,
            PullProcessor(database),
            PushProcessor(database, tracker),
        )
    }

    private fun projectionTable(titleName: String, enabledName: String) = LocalSchemaTable(
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
            LocalSchemaColumn("field-updated-at", "updated_at", "datetime", false, false, isPrimaryKey = false),
            LocalSchemaColumn("field-deleted-at", "deleted_at", "datetime", true, false, isPrimaryKey = false),
        ),
    )

    private fun projectionRow(table: LocalSchemaTable, recordID: String, title: String, enabled: Boolean) =
        JsonObject(
            table.columns.associate { column ->
                column.fieldID to when (column.fieldID) {
                    "field-id" -> JsonPrimitive(recordID)
                    "field-title" -> JsonPrimitive(title)
                    "field-enabled" -> JsonPrimitive(enabled)
                    "field-updated-at" -> JsonPrimitive("2026-04-08T01:00:00.000000Z")
                    "field-deleted-at" -> JsonNull
                    else -> error("unexpected projection field")
                }
            },
        )

    private fun server(handler: (RecordedRequest) -> MockResponse): MockWebServer = MockWebServer().also { server ->
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = handler(request)
        }
        server.start()
        servers += server
    }

    private fun http(server: MockWebServer): HttpClient {
        val config = SynchroConfig(
            dbPath = "unused",
            serverURL = server.url("/").toString().trimEnd('/'),
            authProvider = { "token" },
            clientID = "issue-49-client",
            appVersion = "1.0.0",
        )
        return HttpClient(config, OkHttpClient())
    }

    private inline fun <reified T> jsonResponse(value: T): MockResponse = MockResponse()
        .setResponseCode(200)
        .setHeader("Content-Type", "application/json")
        .setBody(wireJSON.encodeToString(value))

    private fun retryResponse(status: Int, body: String): MockResponse = MockResponse()
        .setResponseCode(status)
        .setHeader("Retry-After", "1")
        .setHeader("Content-Type", "application/json")
        .setBody(body)

    private fun <T> withDatabase(databaseName: String, block: (SynchroDatabase) -> T): T {
        val database = SynchroDatabase.open(context, databaseName)
        return try {
            block(database)
        } finally {
            database.close()
        }
    }

    private fun durableSnapshot(
        database: SynchroDatabase,
        applicationTables: List<String> = listOf("orders"),
    ): Map<String, List<String>> {
        val queries = linkedMapOf<String, String>()
        applicationTables.forEach { table ->
            queries["application:$table"] = "SELECT * FROM ${SQLiteHelpers.quoteIdentifier(table)} ORDER BY 1"
        }
        queries += linkedMapOf(
            "meta" to "SELECT key, value FROM _synchro_meta ORDER BY key",
            "scopes" to "SELECT scope_id, cursor, checksum, generation, local_checksum FROM _synchro_scopes ORDER BY scope_id",
            "scope-rows" to "SELECT scope_id, table_name, record_id, checksum, generation FROM _synchro_scope_rows ORDER BY scope_id, table_name, record_id",
            "row-versions" to "SELECT table_name, record_id, server_version, row_checksum FROM _synchro_row_versions ORDER BY table_name, record_id",
            "pending" to "SELECT local_order, mutation_id, table_id, table_name, record_id, pk_field_id, pk_logical_type, operation, authored_schema_version, authored_schema_hash, base_version, client_version, lifecycle_state, source_kind, depends_on_mutation_id, normalized_mutation_id, sealed_batch_id, sealed_ordinal, accepted_outcome_json, rejected_outcome_json FROM _synchro_pending_changes ORDER BY local_order",
            "mutation-values" to "SELECT mutation_id, field_id, logical_type, value_kind, value_integer, value_real, value_text, value_blob FROM _synchro_mutation_values ORDER BY mutation_id, field_id",
            "batches" to "SELECT batch_id, request_json, pending_json, schema_json, state, completed_at FROM _synchro_push_batches ORDER BY batch_id",
            "batch-members" to "SELECT batch_id, mutation_id, ordinal FROM _synchro_push_batch_members ORDER BY batch_id, ordinal",
            "rejected" to "SELECT mutation_id, table_name, record_id, status, code, message, server_row_json, server_version, mutation_json, rejection_json FROM _synchro_rejected_mutations ORDER BY mutation_id",
            "rebuild-attempts" to "SELECT scope_id, rebuild_id, client_generation, schema_version, schema_hash, generation, cursor, page_limit FROM _synchro_rebuild_attempts ORDER BY scope_id",
            "rebuild-receipts" to "SELECT scope_id, rebuild_id, request_cursor_is_null, request_cursor, request_json, response_json, is_final, final_scope_cursor, final_checksum FROM _synchro_rebuild_page_receipts ORDER BY scope_id, rebuild_id, request_cursor_is_null, request_cursor",
            "seed-receipts" to "SELECT scope_id, receipt, schema_version, schema_hash, cardinality, checksum FROM _synchro_seed_receipts ORDER BY scope_id",
            "backoff" to "SELECT singleton, resume_state, work_identity, retry_classification, attempt_count, next_retry_at_ms FROM _synchro_backoff ORDER BY singleton",
            "migration" to "SELECT singleton, journal_version, source_schema_version, source_schema_hash, target_schema_version, target_schema_hash, action, affected_scopes_json, scope_cursor_updates_json, target_manifest_json, target_tables_json, migration_plan_version, migration_plan_json, migration_plan_hash, reset_materialization, phase FROM _synchro_migration_journal ORDER BY singleton",
        )
        return queries.mapValues { (_, query) ->
            database.query(query).map { row ->
                row.entries.joinToString("|") { (key, value) -> "$key=${snapshotValue(value)}" }
            }
        }
    }

    private fun snapshotValue(value: Any?): String = when (value) {
        null -> "<null>"
        is ByteArray -> value.joinToString("") { byte -> "%02x".format(byte) }
        else -> value.toString()
    }

    private fun assertNoDurableProgress(
        expected: Map<String, List<String>>,
        database: SynchroDatabase,
        applicationTables: List<String> = listOf("orders"),
    ) {
        assertEquals(expected, durableSnapshot(database, applicationTables))
    }

    private fun databaseName(label: String) = "synchro_issue_49_complete_${label}_${UUID.randomUUID()}.sqlite"
}
