@file:OptIn(com.trainstar.synchro.inspection.SynchroProofApi::class)

package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import com.trainstar.synchro.inspection.RebuildAttemptInspection
import com.trainstar.synchro.inspection.RebuildReceiptInspection
import com.trainstar.synchro.inspection.ScopeRowInspection
import com.trainstar.synchro.inspection.ScopeStateInspection
import com.trainstar.synchro.inspection.SynchroInspection
import com.trainstar.synchro.inspection.TransportObservationCollector
import com.trainstar.synchro.inspection.MigrationCheckpoint
import com.trainstar.synchro.inspection.withTransportObservation
import kotlinx.coroutines.async
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.test.runTest
import java.io.IOException
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.AbstractQueuedSynchronizer
import java.util.concurrent.locks.LockSupport
import java.util.UUID
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertSame
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

private const val GUARD_SECONDS = 60L

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28])
class InspectionTests {
    @Test
    fun acceptedOutcomeCapturePreservesStoredIdentityRawJSONAndBounds() {
        val config = prepareClientConfig()
        val client = SynchroClient(config, context)
        val database = SynchroDatabase.open(context, config.dbPath)
        try {
            for (id in listOf("o1", "o2")) client.execute("INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
                arrayOf(id, "authored", "2026-01-01T00:00:00.000000Z"))
            val ids = client.inspectRetainedMutations().map { it.mutationID }
            assertEquals(2, ids.size)
            val first = " { \"mutation_id\": \"${ids[0]}\", \"marker\": \"é\" }\n"
            val second = "{\"marker\":\"second\", \"mutation_id\":\"${ids[1]}\"}"
            database.writeTransaction { db ->
                for ((id, outcome) in ids.zip(listOf(first, second))) {
                    db.execSQL("UPDATE _synchro_pending_changes SET lifecycle_state = 'accepted', accepted_outcome_json = ? WHERE mutation_id = ?",
                        arrayOf(outcome, id))
                }
            }
            val before = database.query("SELECT * FROM _synchro_pending_changes ORDER BY local_order")
            val inspection = SynchroInspection(client)
            var rows: List<Row> = emptyList()
            val snapshot = inspection.captureSnapshot(8) { _, transaction ->
                rows = transaction.query("SELECT id FROM orders ORDER BY id")
                assertThrows(Exception::class.java) { transaction.query("UPDATE orders SET title = 'not read only'") }
            }
            assertEquals(listOf(mapOf("id" to "o1"), mapOf("id" to "o2")), rows)
            assertEquals(mapOf(ids[0] to first, ids[1] to second), snapshot.capture.acceptedMutationOutcomes)
            assertFalse(snapshot.capture.acceptedMutationOutcomesTruncated)
            assertEquals(2, snapshot.capture.mutationLedgerCount)
            assertEquals(0, snapshot.pendingChangeCount)
            assertEquals(emptyList<Any>(), snapshot.retainedMutations)
            assertEquals(emptyList<Any>(), client.inspectRetainedMutations())
            assertEquals(before, database.query("SELECT * FROM _synchro_pending_changes ORDER BY local_order"))
            assertTrue(inspection.captureState(1).acceptedMutationOutcomesTruncated)
            assertEquals(emptyMap<String, String>(), inspection.captureState(1).acceptedMutationOutcomes)
            val padding = 65_536 - ids.sumOf { it.toByteArray(Charsets.UTF_8).size } -
                first.toByteArray(Charsets.UTF_8).size - second.toByteArray(Charsets.UTF_8).size
            val exact = first + " ".repeat(padding)
            database.execute("UPDATE _synchro_pending_changes SET accepted_outcome_json = ? WHERE mutation_id = ?", arrayOf(exact, ids[0]))
            assertFalse(inspection.captureState(8).acceptedMutationOutcomesTruncated)
            assertEquals(exact, inspection.captureState(8).acceptedMutationOutcomes[ids[0]])
            for (oversized in listOf(exact + " ", "x".repeat(4_194_304))) {
                database.execute("UPDATE _synchro_pending_changes SET accepted_outcome_json = ? WHERE mutation_id = ?", arrayOf(oversized, ids[0]))
                val capture = inspection.captureState(8)
                assertTrue(capture.acceptedMutationOutcomesTruncated)
                assertTrue(capture.overflowed)
                assertEquals(emptyMap<String, String>(), capture.acceptedMutationOutcomes)
            }
            for (invalid in listOf(null, "{}".toByteArray(Charsets.UTF_8))) {
                database.execute("UPDATE _synchro_pending_changes SET accepted_outcome_json = ? WHERE mutation_id = ?", arrayOf(invalid, ids[0]))
                assertThrows(SynchroError.InvalidResponse::class.java) { inspection.captureState(8) }
            }
        } finally {
            database.close()
            client.close()
            context.deleteDatabase(config.dbPath)
        }
    }
    @Test
    fun recoveredStartupPauseStopAndCloseDrainWithoutTransport() = runBlocking {
        for (closing in listOf(false, true)) supervisorScope {
            val collector = TransportObservationCollector()
            val config = SynchroConfig(dbPath = "startup_pause_${UUID.randomUUID()}.sqlite", serverURL = "http://test.local",
                authProvider = { "token" }, clientID = "inspection", appVersion = "1.0.0").withTransportObservation(collector)
            val client = SynchroClient(config, context)
            val database = SynchroDatabase.open(context, config.dbPath)
            val draft = protocolOrdersSchemaManifest()
            val manifest = draft.copy(schemaHash = Integrity.schemaManifestHash(draft))
            SchemaManager(database).prepareConnectMigration(ConnectResponse(
                serverTime = "2026-01-01T00:00:00.000000Z", protocolVersion = 3, clientGeneration = 1, scopeSetVersion = 0,
                schema = SchemaDescriptor(1, manifest.schemaHash, SchemaAction.REPLACE),
                scopes = ScopeAssignmentDelta(emptyList(), emptyList()), scopeCursorUpdates = emptyMap(), schemaDefinition = manifest,
            ), manifest.localTables(), false)
            collector.armPause(MigrationCheckpoint.COMMITTED)
            val startup = async(Dispatchers.IO) { runCatching { client.start() } }
            var shutdown: kotlinx.coroutines.Deferred<Unit>? = null
            try {
                collector.awaitPause(MigrationCheckpoint.COMMITTED, 2_000)
                assertEquals(SchemaRef(manifest.schemaVersion, manifest.schemaHash), SynchroInspection(client).captureState(32).schema)
                shutdown = async(Dispatchers.IO) { if (closing) client.close() else client.stop() }
                withTimeout(2_000) { shutdown.await(); startup.await() }
                assertEquals(SyncStatus.Stopped, client.getSyncStatus())
                assertEquals(0L, collector.snapshot().sequenceCheckpoint)
            } finally {
                startup.cancelAndJoin()
                shutdown?.cancelAndJoin()
                database.close()
                if (!closing) client.close()
                context.deleteDatabase(config.dbPath)
            }
        }
    }

    @Test
    fun migrationCaptureReadsRawBindingsAndPhysicalColumnsWithoutChangingIntent() = runTest {
        val collector = TransportObservationCollector()
        val config = SynchroConfig(dbPath = "migration_capture_${UUID.randomUUID()}.sqlite", serverURL = "http://test.local",
            authProvider = { "token" }, clientID = "inspection", appVersion = "1.0.0").withTransportObservation(collector)
        val client = SynchroClient(config, context)
        val database = SynchroDatabase.open(context, config.dbPath)
        try {
            val source = protocolOrdersSchemaManifest()
            installTestSchema(database, 1, source.schemaHash, source.localTables())
            database.execute("CREATE TABLE local_settings (value TEXT)")
            database.execute("INSERT INTO local_settings VALUES ('sentinel')")
            database.execute("INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES ('o1', 'first', 'u1', '2026-01-01T00:00:00.000000Z')")
            database.execute("UPDATE orders SET ship_address = 'later' WHERE id = 'o1'")
            val draft = protocolOrdersSchemaManifest(includeNotes = true, schemaVersion = 2,
                parentSchema = SchemaRef(1, source.schemaHash), transitionClass = "class_2")
            val target = draft.copy(schemaHash = Integrity.schemaManifestHash(draft))
            val manager = SchemaManager(database)
            val response = ConnectResponse(
                serverTime = "2026-01-01T00:00:00.000000Z", protocolVersion = 3, clientGeneration = 1, scopeSetVersion = 0,
                schema = SchemaDescriptor(2, target.schemaHash, SchemaAction.REPLACE),
                scopes = ScopeAssignmentDelta(emptyList(), emptyList()), scopeCursorUpdates = emptyMap(), schemaDefinition = target,
            )
            val tracker = ChangeTracker(database)
            val engine = SyncEngine(config, database, HttpClient(config), manager, tracker,
                PullProcessor(database), PushProcessor(database, tracker))
            collector.armPause(MigrationCheckpoint.PREPARED)
            val installation = async { engine.installConnectResponse(response) }
            collector.awaitPause(MigrationCheckpoint.PREPARED, 2_000)
            assertFalse(installation.isCompleted)
            val before = database.query("SELECT * FROM _synchro_migration_journal")
            val inspection = SynchroInspection(client)
            val prepared = inspection.captureSnapshot(32) { _, transaction ->
                assertEquals("sentinel", transaction.query("SELECT value FROM local_settings").single()["value"])
                assertThrows(Exception::class.java) { transaction.query("UPDATE local_settings SET value = 'changed'") }
            }
            val journal = requireNotNull(prepared.capture.migrationJournal)
            assertEquals(SchemaRef(1, source.schemaHash), journal.source)
            assertEquals(SchemaRef(2, target.schemaHash), journal.target)
            assertEquals("prepared", journal.phase)
            assertEquals(before.single()["migration_plan_json"], journal.stored["migration_plan_json"])
            assertEquals(before.single()["target_manifest_json"], journal.stored["target_manifest_json"])
            assertEquals(before.single()["journal_version"].toString(), journal.stored["journal_version"])
            database.execute("UPDATE _synchro_migration_journal SET migration_plan_version = 'invalid'")
            assertThrows(SynchroError.InvalidResponse::class.java) { inspection.captureState(32) }
            database.execute("UPDATE _synchro_migration_journal SET migration_plan_version = ?", arrayOf(journal.stored.getValue("migration_plan_version").toLong()))
            for ((column, version) in listOf("source_schema_version" to journal.source.version, "target_schema_version" to journal.target.version)) {
                database.execute("UPDATE _synchro_migration_journal SET $column = ?", arrayOf("x".repeat(4_194_304)))
                val omitted = inspection.captureState(32)
                assertEquals(null, omitted.migrationJournal)
                assertTrue(omitted.migrationJournalTruncated)
                database.execute("UPDATE _synchro_migration_journal SET $column = ?", arrayOf(version))
            }
            database.execute("UPDATE _synchro_migration_journal SET target_manifest_json = ?", arrayOf(byteArrayOf(0xff.toByte())))
            assertThrows(SynchroError.InvalidResponse::class.java) { inspection.captureState(32) }
            database.execute("UPDATE _synchro_migration_journal SET target_manifest_json = ?", arrayOf(journal.stored.getValue("target_manifest_json")))
            assertFalse(prepared.capture.physicalSchema.any { it.name == "notes" || it.tableName == "local_settings" })
            assertEquals(2, prepared.capture.mutationLedgerCount)
            assertEquals(prepared, inspection.captureSnapshot(32) { _, _ -> })
            assertEquals(before, database.query("SELECT * FROM _synchro_migration_journal"))
            collector.armPause(MigrationCheckpoint.COMMITTED)
            collector.resumePause()
            collector.awaitPause(MigrationCheckpoint.COMMITTED, 2_000)
            assertFalse(installation.isCompleted)
            val committed = inspection.captureSnapshot(32) { _, _ -> }
            assertEquals("ddl_applied", committed.capture.migrationJournal?.phase)
            assertEquals(journal.target, committed.capture.schema)
            assertTrue(committed.capture.physicalSchema.any { it.name == "notes" && !it.notNull })
            assertEquals(prepared.retainedMutations, committed.retainedMutations)
            val bounded = inspection.captureState(0)
            assertTrue(bounded.migrationJournalTruncated)
            assertTrue(bounded.physicalSchemaTruncated)
            assertTrue(bounded.overflowed)
            assertEquals(null, bounded.migrationJournal)
            database.execute("UPDATE _synchro_migration_journal SET migration_plan_json = ?", arrayOf("x".repeat(4_194_304)))
            assertTrue(inspection.captureState(32).migrationJournalTruncated)
            assertEquals(null, inspection.captureState(32).migrationJournal)
            assertTrue(inspection.captureState(32).physicalSchemaTruncated)
            assertEquals(emptyList<Any>(), inspection.captureState(32).physicalSchema)
            database.execute("UPDATE _synchro_migration_journal SET migration_plan_json = ?", arrayOf(journal.stored.getValue("migration_plan_json")))
            collector.resumePause()
            installation.await()
            assertTrue(installation.isCompleted)
        } finally {
            collector.cancelPauseBarrier()
            database.close()
            client.close()
            context.deleteDatabase(config.dbPath)
        }
    }

    @Test
    fun migrationPhysicalCaptureDetectsPrematureTargetAndLeftoverSourceTables() {
        val config = SynchroConfig(dbPath = "migration_union_${UUID.randomUUID()}.sqlite", serverURL = "http://test.local",
            authProvider = { "token" }, clientID = "inspection", appVersion = "1.0.0")
        val client = SynchroClient(config, context)
        val database = SynchroDatabase.open(context, config.dbPath)
        try {
            val sourceDraft = protocolOrdersSchemaManifest().let { manifest ->
                manifest.copy(tables = manifest.tables + manifest.tables[0].copy(
                    tableID = "table-retired", relationID = "relation-retired", name = "retired"))
            }
            val source = sourceDraft.copy(schemaHash = Integrity.schemaManifestHash(sourceDraft))
            val manager = SchemaManager(database)
            manager.prepareConnectMigration(ConnectResponse(
                serverTime = "2026-01-01T00:00:00.000000Z", protocolVersion = 3, clientGeneration = 1, scopeSetVersion = 0,
                schema = SchemaDescriptor(1, source.schemaHash, SchemaAction.REPLACE),
                scopes = ScopeAssignmentDelta(emptyList(), emptyList()), scopeCursorUpdates = emptyMap(), schemaDefinition = source,
            ), source.localTables(), false)
            val fresh = SynchroInspection(client).captureState(32)
            assertEquals(SchemaRef(0, ""), fresh.migrationJournal?.source)
            assertEquals(emptyList<Any>(), fresh.physicalSchema)
            assertFalse(fresh.physicalSchemaTruncated)
            database.writeSyncLockedTransaction { manager.applyPreparedMigrationInTransaction(it) }
            manager.completeMigrationIfReady(authoritativeAssignmentsInstalled = true)
            database.execute("CREATE TABLE unrelated_local (id TEXT)")
            val targetDraft = protocolOrdersSchemaManifest(schemaVersion = 2, parentSchema = SchemaRef(1, source.schemaHash),
                transitionClass = "class_4", compatibilityFloor = 2).let { manifest ->
                manifest.copy(tables = manifest.tables + manifest.tables[0].copy(
                    tableID = "table-added", relationID = "relation-added", name = "added"))
            }
            val target = targetDraft.copy(schemaHash = Integrity.schemaManifestHash(targetDraft))
            manager.prepareConnectMigration(ConnectResponse(
                serverTime = "2026-01-01T00:00:00.000000Z", protocolVersion = 3, clientGeneration = 1, scopeSetVersion = 0,
                schema = SchemaDescriptor(2, target.schemaHash, SchemaAction.REPLACE),
                scopes = ScopeAssignmentDelta(emptyList(), emptyList()), scopeCursorUpdates = emptyMap(), schemaDefinition = target,
            ), target.localTables(), true)
            val inspection = SynchroInspection(client)
            val prepared = inspection.captureState(32)
            assertFalse(prepared.physicalSchemaTruncated)
            assertTrue(prepared.physicalSchema.any { it.tableName == "retired" })
            assertFalse(prepared.physicalSchema.any { it.tableName == "added" || it.tableName == "unrelated_local" })
            database.execute("CREATE TABLE added (id TEXT)")
            val premature = inspection.captureState(32)
            assertTrue(premature.physicalSchema.any { it.tableName == "added" })
            assertFalse(premature.physicalSchema.any { it.tableName == "unrelated_local" })
            database.execute("DROP TABLE added")
            val archived = database.queryOne("SELECT manifest_json FROM _synchro_schema_archives WHERE schema_version = 1")!!["manifest_json"] as String
            database.execute("UPDATE _synchro_schema_archives SET schema_hash = ? WHERE schema_version = 1", arrayOf("c".repeat(64)))
            assertThrows(SynchroError.InvalidResponse::class.java) { inspection.captureState(32) }
            database.execute("UPDATE _synchro_schema_archives SET schema_hash = ? WHERE schema_version = 1", arrayOf(source.schemaHash))
            database.execute("UPDATE _synchro_migration_journal SET target_schema_hash = ?", arrayOf("d".repeat(64)))
            assertThrows(SynchroError.InvalidResponse::class.java) { inspection.captureState(32) }
            database.execute("UPDATE _synchro_migration_journal SET target_schema_hash = ?", arrayOf(target.schemaHash))
            database.execute("UPDATE _synchro_schema_archives SET manifest_json = '{}' WHERE schema_version = 1")
            assertThrows(SynchroError.InvalidResponse::class.java) { inspection.captureState(32) }
            database.execute("UPDATE _synchro_schema_archives SET manifest_json = ? WHERE schema_version = 1", arrayOf(archived + " ".repeat(4_194_304)))
            val oversized = inspection.captureState(32)
            assertTrue(oversized.migrationJournal != null)
            assertTrue(oversized.physicalSchemaTruncated)
            assertEquals(emptyList<Any>(), oversized.physicalSchema)
            database.execute("UPDATE _synchro_schema_archives SET manifest_json = ? WHERE schema_version = 1", arrayOf(archived))
            database.writeSyncLockedTransaction { manager.applyPreparedMigrationInTransaction(it) }
            val committed = inspection.captureState(32)
            assertFalse(committed.physicalSchemaTruncated)
            assertTrue(committed.physicalSchema.any { it.tableName == "added" })
            assertFalse(committed.physicalSchema.any { it.tableName == "retired" || it.tableName == "unrelated_local" })
            database.execute("CREATE TABLE retired (id TEXT)")
            val leftover = inspection.captureState(32)
            assertTrue(leftover.physicalSchema.any { it.tableName == "retired" })
            assertFalse(leftover.physicalSchema.any { it.tableName == "unrelated_local" })
            database.execute("DROP TABLE retired")
        } finally {
            database.close()
            client.close()
            context.deleteDatabase(config.dbPath)
        }
    }

    private data class RebuildReceiptProof(
        val rebuildIDFingerprint: String,
        val pageCount: Int,
        val returnedRecordCount: Int,
        val requestChainValid: Boolean,
        val recordsInCanonicalOrder: Boolean,
        val rowChecksumsValid: Boolean,
        val scopeChecksumValid: Boolean,
        val finalChecksumMatchesLocal: Boolean,
    )

    private fun rebuildReceiptProof(value: RebuildReceiptInspection): RebuildReceiptProof =
        RebuildReceiptProof(
            rebuildIDFingerprint = value.rebuildIDFingerprint,
            pageCount = value.pageCount,
            returnedRecordCount = value.returnedRecordCount,
            requestChainValid = value.requestChainExpected == value.requestChainObserved,
            recordsInCanonicalOrder = value.recordIdentitiesHex.size == value.recordIdentitiesHex.toSet().size &&
                value.recordIdentitiesHex == value.recordIdentitiesHex.sorted(),
            rowChecksumsValid = value.receivedRowChecksums == value.computedRowChecksums,
            scopeChecksumValid = value.computedScopeChecksum != null && value.computedScopeChecksum == value.finalScopeChecksum,
            finalChecksumMatchesLocal = value.finalScopeChecksum != null &&
                value.finalScopeChecksum == value.storedScopeChecksum &&
                value.finalScopeChecksum == value.localScopeChecksum,
        )

    private val context = ApplicationProvider.getApplicationContext<Context>()
    @OptIn(ExperimentalSerializationApi::class)
    private val wireJSON = Json {
        encodeDefaults = true
        explicitNulls = false
    }

    private val table = SchemaTable(
        tableName = "orders",
        updatedAtColumn = "updated_at",
        deletedAtColumn = "deleted_at",
        primaryKey = listOf("id"),
        columns = listOf(
            SchemaColumn("id", logicalType = "string", nullable = false, isPrimaryKey = true),
            SchemaColumn("title", logicalType = "string"),
            SchemaColumn("updated_at", logicalType = "datetime", nullable = false),
            SchemaColumn("deleted_at", logicalType = "datetime"),
        ),
    )

    @Test
    fun pendingInspectionSurvivesRestartAndUsesAuthoredValues() {
        val config = prepareClientConfig()
        val firstClient = SynchroClient(config, context)
        try {
            assertSame(SyncStatus.LocalReady, firstClient.getSyncStatus())
            firstClient.execute(
                "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
                arrayOf("o1", "first authored", "2026-01-01T00:00:00.000000Z"),
            )
            firstClient.execute(
                "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
                arrayOf("o2", "second authored", "2026-01-01T00:00:01.000000Z"),
            )
        } finally {
            firstClient.close()
        }

        val rawDatabase = SynchroDatabase.open(context, config.dbPath)
        try {
            rawDatabase.writeTransaction { db ->
                db.execSQL(
                    "UPDATE _synchro_pending_changes SET lifecycle_state = 'superseded_before_send' WHERE record_id = 'o1'",
                )
                db.execSQL("UPDATE _synchro_meta SET value = '1' WHERE key = 'sync_lock'")
                db.execSQL("UPDATE orders SET title = 'current row value'")
                db.execSQL("UPDATE _synchro_meta SET value = '0' WHERE key = 'sync_lock'")
            }
        } finally {
            rawDatabase.close()
        }

        val restartedClient = SynchroClient(config, context)
        try {
            val inspections = restartedClient.inspectPendingMutations()
            assertEquals(listOf("o1", "o2"), inspections.map { it.recordID })
            assertEquals(inspections.map { it.localOrder }.sorted(), inspections.map { it.localOrder })
            assertEquals(LocalMutationStatus.SUPERSEDED_BEFORE_SEND, inspections[0].status)
            assertEquals(LocalMutationStatus.PENDING, inspections[1].status)
            assertEquals(Operation.INSERT, inspections[0].operation)
            assertEquals(SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH), inspections[0].authoredSchema)
            assertEquals(
                AnyCodable("first authored"),
                inspections[0].authoredFields.single { it.fieldID == "title" }.value,
            )
            assertEquals(
                AnyCodable("second authored"),
                inspections[1].authoredFields.single { it.fieldID == "title" }.value,
            )
            assertTrue(inspections.none { inspection ->
                inspection.authoredFields.any { it.value == AnyCodable("current row value") }
            })
        } finally {
            restartedClient.close()
            context.deleteDatabase(config.dbPath)
        }
    }

    @Test
    fun publicInitializationRestoresExactDurableLifecycleAndWork() {
        val freshConfig = SynchroConfig(
            dbPath = "synchro_initial_fresh_${UUID.randomUUID()}.sqlite",
            serverURL = "http://localhost:8080",
            authProvider = { "test-token" },
            clientID = "fresh-device",
            appVersion = "1.0.0",
        )
        val fresh = SynchroClient(freshConfig, context)
        try {
            assertSame(SyncStatus.LocalReady, fresh.getSyncStatus())
        } finally {
            fresh.close()
            context.deleteDatabase(freshConfig.dbPath)
        }

        val stoppedConfig = prepareClientConfig()
        withInternalDatabase(stoppedConfig) { database ->
            database.writeTransaction { db ->
                SynchroMeta.transitionClientLifecycleState(db, SyncLifecycleState.STOPPED)
            }
        }
        val stopped = SynchroClient(stoppedConfig, context)
        try {
            assertSame(SyncStatus.Stopped, stopped.getSyncStatus())
        } finally {
            stopped.close()
            context.deleteDatabase(stoppedConfig.dbPath)
        }

        val failureConfig = prepareClientConfig()
        val failure = SyncFailure(
            operation = SyncOperationKind.CONNECTING,
            code = SyncFailureCode.UPGRADE_REQUIRED,
            retryable = false,
            message = "The installed schema requires an explicit synchronized reset.",
            recoveryAction = SyncRecoveryAction.SCHEMA_RESET,
        )
        withInternalDatabase(failureConfig) { database ->
            database.writeTransaction { db ->
                SynchroMeta.transitionClientLifecycleState(db, SyncLifecycleState.LOCAL_READY)
                SynchroMeta.recordBlockingError(db, failure)
            }
        }
        val blocked = SynchroClient(failureConfig, context)
        try {
            val status = blocked.getSyncStatus()
            assertTrue(status is SyncStatus.Error)
            assertEquals(failure, (status as SyncStatus.Error).failure)
        } finally {
            blocked.close()
            context.deleteDatabase(failureConfig.dbPath)
        }

        val backoffConfig = prepareClientConfig()
        val exactRequest = "{\"client_id\":\"inspection-device\",\"scope\":\"orders:user\"}"
        withInternalDatabase(backoffConfig) { database ->
            database.writeTransaction { db ->
                for (state in listOf(
                    SyncLifecycleState.LOCAL_READY,
                    SyncLifecycleState.CONNECTING,
                    SyncLifecycleState.READY,
                    SyncLifecycleState.PULLING,
                    SyncLifecycleState.BACKOFF,
                )) {
                    SynchroMeta.transitionClientLifecycleState(db, state)
                }
            }
            DurableBackoffStore.persist(
                database = database,
                error = RetryableError(
                    underlying = SynchroError.NetworkError(IOException("offline")),
                    retryAfter = null,
                    interruptedOperation = RetryOperation.PULLING,
                    workIdentity = exactRequest,
                    retryClassification = RetryClassification.NETWORK,
                ),
                currentTimeMillis = 1_000L,
                fallbackDelaySeconds = { 2.0 },
            )
        }
        val withBackoff = SynchroClient(backoffConfig, context)
        try {
            assertSame(SyncStatus.LocalReady, withBackoff.getSyncStatus())
            withInternalDatabase(backoffConfig) { database ->
                assertEquals(
                    SyncLifecycleState.LOCAL_READY,
                    database.readTransaction { db -> SynchroMeta.getClientState(db).lifecycleState },
                )
                val retained = requireNotNull(DurableBackoffStore.load(database))
                assertEquals(RetryOperation.PULLING, retained.resumeState)
                assertEquals(exactRequest, retained.workIdentity)
                assertEquals(1L, retained.attemptCount)
                assertEquals(3_000L, retained.nextRetryAtMs)
            }
        } finally {
            withBackoff.close()
            context.deleteDatabase(backoffConfig.dbPath)
        }
    }

    @Test
    fun rejectedInspectionSurvivesRestartAndClearRetainsQueueIntent() {
        val config = prepareClientConfig()
        val mutationJSON = "{\"mutation_id\":\"m1\",\"columns\":{\"title\":\"authored\"}}"
        val rejectionJSON = "{\"mutation_id\":\"m1\",\"status\":\"rejected_terminal\",\"code\":\"policy_rejected\"}"
        lateinit var exactMutationJSON: String
        lateinit var exactRejectionJSON: String
        val firstClient = SynchroClient(config, context)
        var firstClientClosed = false
        try {
            firstClient.execute(
                "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
                arrayOf("o1", "authored", "2026-01-01T00:00:00.000000Z"),
            )
            val mutationID = firstClient.inspectPendingMutations().single().mutationID
            exactMutationJSON = mutationJSON.replace("m1", mutationID)
            exactRejectionJSON = rejectionJSON.replace("m1", mutationID)
            firstClient.close()
            firstClientClosed = true
            val rawDatabase = SynchroDatabase.open(context, config.dbPath)
            rawDatabase.writeTransaction { db ->
                db.execSQL(
                    "UPDATE _synchro_pending_changes SET lifecycle_state = 'rejected_terminal' WHERE mutation_id = ?",
                    arrayOf(mutationID),
                )
                SynchroMeta.upsertRejectedMutation(
                    db = db,
                    mutationID = mutationID,
                    tableName = "orders",
                    recordId = "o1",
                    status = "rejected_terminal",
                    code = "policy_rejected",
                    message = "not allowed",
                    serverRowJson = null,
                    serverVersion = null,
                    mutationJSON = exactMutationJSON,
                    rejectionJSON = exactRejectionJSON,
                )
            }
            rawDatabase.close()
        } finally {
            if (!firstClientClosed) firstClient.close()
        }

        val restartedClient = SynchroClient(config, context)
        try {
            val rejected = restartedClient.inspectRejectedMutations().single()
            assertEquals(MutationStatus.REJECTED_TERMINAL, rejected.status)
            assertEquals(MutationRejectionCode.POLICY_REJECTED, rejected.code)
            assertEquals("not allowed", rejected.message)
            assertEquals(exactMutationJSON, rejected.mutationJSON)
            assertEquals(exactRejectionJSON, rejected.rejectionJSON)
            val queueBeforeClear = internalQuery(config,
                "SELECT mutation_id, lifecycle_state FROM _synchro_pending_changes ORDER BY local_order",
            )
            val valuesBeforeClear = internalQuery(config,
                "SELECT mutation_id, field_id, logical_type, value_kind, value_text FROM _synchro_mutation_values ORDER BY mutation_id, field_id",
            )

            restartedClient.clearRejectedMutations()

            assertTrue(restartedClient.inspectRejectedMutations().isEmpty())
            assertEquals(queueBeforeClear, internalQuery(config,
                "SELECT mutation_id, lifecycle_state FROM _synchro_pending_changes ORDER BY local_order",
            ))
            assertEquals(valuesBeforeClear, internalQuery(config,
                "SELECT mutation_id, field_id, logical_type, value_kind, value_text FROM _synchro_mutation_values ORDER BY mutation_id, field_id",
            ))
        } finally {
            restartedClient.close()
        }

        val afterClearRestart = SynchroClient(config, context)
        try {
            assertTrue(afterClearRestart.inspectRejectedMutations().isEmpty())
            assertEquals("rejected_terminal", internalQueryOne(
                config,
                "SELECT lifecycle_state FROM _synchro_pending_changes",
            )?.get("lifecycle_state"))
            assertTrue(internalQuery(config, "SELECT field_id FROM _synchro_mutation_values").isNotEmpty())
        } finally {
            afterClearRestart.close()
            context.deleteDatabase(config.dbPath)
        }
    }

    @Test
    fun atomicClientStateCaptureKeepsLedgerHistorySeparateFromRetainedMutations() {
        val config = prepareClientConfig()
        val client = SynchroClient(config, context)
        try {
            client.execute(
                "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
                arrayOf("retained", "retained", "2026-01-01T00:00:00.000000Z"),
            )
            client.execute(
                "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
                arrayOf("historical", "historical", "2026-01-01T00:00:01.000000Z"),
            )
        } finally {
            client.close()
        }
        val historicalID = requireNotNull(internalQueryOne(config,
            "SELECT mutation_id FROM _synchro_pending_changes WHERE record_id = 'historical'",
        )?.get("mutation_id") as? String)
        val outcome = " { \"mutation_id\": \"$historicalID\" }\n"
        withInternalDatabase(config) { database ->
            database.execute(
                "UPDATE _synchro_pending_changes SET lifecycle_state = 'accepted', accepted_outcome_json = ? WHERE record_id = ?",
                arrayOf(outcome, "historical"),
            )
        }

        val reopened = SynchroClient(config, context)
        try {
            val capture = SynchroInspection(reopened).captureState(maximumRecords = 10)
            assertEquals(2, capture.mutationLedgerCount)
            assertEquals(1, capture.mutationOutcomeCount)
            assertEquals(mapOf(historicalID to outcome), capture.acceptedMutationOutcomes)
            assertEquals(1, reopened.retainedMutationCount())
            assertEquals(listOf("retained"), reopened.inspectRetainedMutations().map { it.recordID })
        } finally {
            reopened.close()
            context.deleteDatabase(config.dbPath)
        }
    }

    @Test
    fun atomicClientStateCaptureReturnsBoundedDurableStateAndExactCounts() {
        val config = prepareClientConfig()
        val rebuildID = "00000000-0000-4000-8000-000000000001"
        withInternalDatabase(config) { database ->
            database.writeTransaction { db ->
                SynchroMeta.upsertScope(
                    db,
                    scopeId = "orders:user-1",
                    cursor = "cursor-1",
                    checksum = "scope-checksum",
                    generation = 3,
                    localChecksum = "local-checksum",
                )
                SynchroMeta.upsertScopeRow(
                    db,
                    scopeId = "orders:user-1",
                    tableName = "orders",
                    recordId = "order-1",
                    checksum = "row-checksum",
                    generation = 3,
                )
                SynchroMeta.upsertRebuildAttempt(
                    db,
                    LocalRebuildAttempt(
                        scopeID = "orders:user-1",
                        rebuildID = rebuildID,
                        clientGeneration = 4,
                        schemaVersion = 1,
                        schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
                        generation = 3,
                        cursor = "rebuild-page-2",
                        pageLimit = 100,
                    ),
                )
            }
        }

        val client = SynchroClient(config, context)
        try {
            val proof = SynchroInspection(client)
            val inspection = proof.captureState(maximumRecords = 4)

            assertEquals(SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH), inspection.schema)
            assertEquals(
                listOf(
                    ScopeStateInspection(
                        scopeID = "orders:user-1",
                        cursor = "cursor-1",
                        checksum = "scope-checksum",
                        localChecksum = "local-checksum",
                        generation = 3,
                    ),
                ),
                inspection.scopeStates,
            )
            assertEquals(
                listOf(ScopeRowInspection("orders:user-1", "orders", "order-1", "row-checksum", 3)),
                inspection.scopeRows,
            )
            assertEquals(
                listOf(
                    RebuildAttemptInspection(
                        scopeID = "orders:user-1",
                        rebuildID = rebuildID,
                        clientGeneration = 4,
                        schemaVersion = 1,
                        schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
                        generation = 3,
                        cursor = "rebuild-page-2",
                        pageLimit = 100,
                    ),
                ),
                inspection.rebuildAttempts,
            )
            assertEquals(0L, inspection.provenanceMaintenanceWorkCursor)
            assertFalse(inspection.scopeStatesTruncated)
            assertFalse(inspection.scopeRowsTruncated)
            assertFalse(inspection.rebuildAttemptsTruncated)
            assertFalse(inspection.rebuildReceiptsTruncated)
            assertFalse(inspection.rowMetadataTruncated)
            assertFalse(inspection.overflowed)
            assertEquals(0, inspection.applicationRowCount)
            assertEquals(0, inspection.mutationLedgerCount)
            assertEquals(0, inspection.mutationOutcomeCount)
            assertEquals(0, inspection.sealedBatchCount)
            assertEquals(0, inspection.rejectedMutationCount)
            assertEquals(1, inspection.scopeStateCount)
            assertEquals(1, inspection.scopeRowCount)
            assertEquals(1, inspection.provenanceCount)
            assertEquals(0, inspection.rowMetadataCount)
            assertEquals(1, inspection.rebuildAttemptCount)
            assertEquals(0, inspection.rebuildReceiptCount)
            assertEquals(inspection.schema, proof.currentSchema())
            assertEquals(inspection.scopeStates, proof.scopeStates())
            assertEquals(inspection.scopeRows, proof.scopeRows())
            assertEquals(inspection.rebuildAttempts, proof.rebuildAttempts())
        } finally {
            client.close()
            context.deleteDatabase(config.dbPath)
        }
    }

    @Test
    fun snapshotReadsCountsDetailsAndApplicationRowsTogether() {
        val config = prepareClientConfig()
        val client = SynchroClient(config, context)
        try {
            client.execute(
                "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
                arrayOf("o1", "first", "2026-01-01T00:00:00.000000Z"),
            )
            client.execute(
                "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
                arrayOf("o2", "second", "2026-01-01T00:00:01.000000Z"),
            )
            val proof = SynchroInspection(client)
            var rows: List<Row> = emptyList()
            val snapshot = proof.captureSnapshot(maximumRecords = 8) { capture, transaction ->
                assertEquals(2, capture.applicationRowCount)
                rows = transaction.query("SELECT id, title FROM orders ORDER BY id")
            }

            assertEquals(listOf(mapOf("id" to "o1", "title" to "first"), mapOf("id" to "o2", "title" to "second")), rows)
            assertEquals(2, snapshot.capture.mutationLedgerCount)
            val retained = requireNotNull(snapshot.retainedMutations).currentRecords()
            assertEquals(listOf("o1", "o2"), retained.map { it.recordID })
            assertEquals(listOf(LocalMutationStatus.PENDING, LocalMutationStatus.PENDING), retained.map { it.status })
            assertEquals(
                listOf(AnyCodable("first"), AnyCodable("second")),
                retained.map { mutation -> mutation.authoredFields.single { it.fieldID == "title" }.value },
            )
            assertEquals(2, snapshot.pendingChangeCount)
            assertEquals(emptyList<RetainedRejectionInspection>(), snapshot.rejectedMutations)
            assertEquals(null, snapshot.blockingFailure)
            assertEquals(snapshot, proof.captureSnapshot(maximumRecords = 8) { _, _ -> })

            val bounded = proof.captureSnapshot(maximumRecords = 1) { _, _ -> }
            assertEquals(null, bounded.retainedMutations)
            assertEquals(emptyList<RetainedRejectionInspection>(), bounded.rejectedMutations)
        } finally {
            client.close()
            context.deleteDatabase(config.dbPath)
        }
    }

    /**
     * Queues a transition on the fair client write lock directly behind the snapshot. The lock
     * grants waiters in order, so the transition commits after the first transaction of the
     * snapshot and before any later transaction of the same snapshot.
     */
    @Test
    fun snapshotKeepsOneStateWhenATransitionFollowsItsFirstTransaction() {
        val config = prepareClientConfig()
        val client = SynchroClient(config, context)
        val database = SynchroClient::class.java.getDeclaredField("database").apply { isAccessible = true }.get(client) as SynchroDatabase
        val threads = Executors.newFixedThreadPool(3)
        val release = CountDownLatch(1)
        try {
            client.execute(
                "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
                arrayOf("o1", "before", "2026-01-01T00:00:00.000000Z"),
            )
            val acceptedID = client.inspectRetainedMutations().first().mutationID
            client.execute("UPDATE orders SET title = 'updated-before' WHERE id = 'o1'")
            val beforeOutcome = " { \"mutation_id\": \"$acceptedID\", \"marker\": \"before\" }\n"
            val afterOutcome = beforeOutcome + " \n"
            database.writeTransaction { db ->
                db.execSQL("UPDATE _synchro_pending_changes SET lifecycle_state = 'accepted', accepted_outcome_json = ? WHERE mutation_id = ?", arrayOf(beforeOutcome, acceptedID))
            }
            val holding = CountDownLatch(1)
            val holder = threads.submit {
                client.writeTransaction {
                    holding.countDown()
                    release.await()
                }
            }
            assertTrue(holding.await(GUARD_SECONDS, TimeUnit.SECONDS))

            var rows: List<Row> = emptyList()
            val snapshotThread = AtomicReference<Thread>()
            val snapshot = threads.submit(Callable {
                snapshotThread.set(Thread.currentThread())
                SynchroInspection(client).captureSnapshot(maximumRecords = 8) { _, transaction ->
                    rows = transaction.query("SELECT id FROM orders ORDER BY id")
                }
            })
            awaitQueuedOnLock(snapshotThread)
            val transitionThread = AtomicReference<Thread>()
            val transition = threads.submit {
                transitionThread.set(Thread.currentThread())
                database.writeTransaction { db ->
                    client.execute("INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
                        arrayOf("o2", "after", "2026-01-02T00:00:00.000000Z"))
                    db.execSQL("UPDATE _synchro_pending_changes SET accepted_outcome_json = ? WHERE mutation_id = ?",
                        arrayOf(afterOutcome, acceptedID))
                }
            }
            awaitQueuedOnLock(transitionThread)
            release.countDown()
            holder.get(GUARD_SECONDS, TimeUnit.SECONDS)
            val captured = snapshot.get(GUARD_SECONDS, TimeUnit.SECONDS)
            transition.get(GUARD_SECONDS, TimeUnit.SECONDS)

            assertEquals(listOf(mapOf("id" to "o1")), rows)
            assertEquals(1, captured.capture.applicationRowCount)
            assertEquals(2, captured.capture.mutationLedgerCount)
            assertEquals(mapOf(acceptedID to beforeOutcome), captured.capture.acceptedMutationOutcomes)
            assertEquals(listOf("o1"), requireNotNull(captured.retainedMutations).map { it.recordID })
            assertEquals(1, captured.pendingChangeCount)

            var laterRows: List<Row> = emptyList()
            val later = SynchroInspection(client).captureSnapshot(maximumRecords = 8) { _, transaction ->
                laterRows = transaction.query("SELECT id FROM orders ORDER BY id")
            }
            assertEquals(listOf(mapOf("id" to "o1"), mapOf("id" to "o2")), laterRows)
            assertEquals(2, later.capture.applicationRowCount)
            assertEquals(mapOf(acceptedID to afterOutcome), later.capture.acceptedMutationOutcomes)
            assertEquals(listOf("o1", "o2"), requireNotNull(later.retainedMutations).map { it.recordID })
        } finally {
            release.countDown()
            threads.shutdown()
            assertTrue(threads.awaitTermination(GUARD_SECONDS, TimeUnit.SECONDS))
            client.close()
            context.deleteDatabase(config.dbPath)
        }
    }

    /** Waits until the thread is parked on a lock. A thread parks on the client write lock here. */
    private fun awaitQueuedOnLock(thread: AtomicReference<Thread>) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(GUARD_SECONDS)
        while (true) {
            val queued = thread.get()?.let { LockSupport.getBlocker(it) is AbstractQueuedSynchronizer } == true
            if (queued) return
            check(System.nanoTime() < deadline) { "thread did not queue on the client write lock" }
            Thread.yield()
        }
    }

    @Test
    fun atomicClientStateCaptureIncludesUnscopedMetadataAndReportsTruncation() {
        val config = prepareClientConfig()
        withInternalDatabase(config) { database ->
            database.writeTransaction { db ->
                SynchroMeta.upsertRowVersion(db, "orders", "unscoped-a", "version-a", null)
                SynchroMeta.upsertRowVersion(db, "orders", "unscoped-b", "version-b", null)
            }
        }

        val client = SynchroClient(config, context)
        try {
            val proof = SynchroInspection(client)
            val bounded = proof.captureState(maximumRecords = 1)
            assertTrue(bounded.scopeRows.isEmpty())
            assertEquals(2, bounded.rowMetadataCount)
            assertEquals(listOf("unscoped-a"), bounded.rowMetadata.map { it.recordID })
            assertTrue(bounded.rowMetadataTruncated)
            assertTrue(bounded.overflowed)

            val complete = proof.captureState(maximumRecords = 4)
            assertEquals(listOf("unscoped-a", "unscoped-b"), complete.rowMetadata.map { it.recordID })
            assertFalse(complete.rowMetadataTruncated)
            assertFalse(complete.overflowed)
        } finally {
            client.close()
            context.deleteDatabase(config.dbPath)
        }
    }

    @Test
    fun rebuildReceiptProofAcceptsValidEmptyTerminalReceipt() {
        val fixture = makeRebuildReceiptFixture(recordCount = 0)
        try {
            val proof = SynchroInspection(fixture.client).rebuildReceipts().single().let(::rebuildReceiptProof)

            assertEquals(TransportObservationCollector.cursorFingerprint(PROOF_REBUILD_ID), proof.rebuildIDFingerprint)
            assertEquals(1, proof.pageCount)
            assertEquals(0, proof.returnedRecordCount)
            assertTrue(proof.requestChainValid)
            assertTrue(proof.recordsInCanonicalOrder)
            assertTrue(proof.rowChecksumsValid)
            assertTrue(proof.scopeChecksumValid)
            assertTrue(proof.finalChecksumMatchesLocal)
        } finally {
            closeRebuildReceiptFixture(fixture)
        }
    }

    @Test
    fun rebuildReceiptProofTreatsEmptyLocalChecksumAsAbsent() {
        val fixture = makeRebuildReceiptFixture(recordCount = 0)
        try {
            withInternalDatabase(fixture.config) { database ->
                database.writeTransaction { db ->
                    SynchroMeta.setScopeLocalChecksum(db, PROOF_SCOPE_ID, "")
                }
            }

            val receipt = SynchroInspection(fixture.client)
                .captureState(maximumRecords = 1)
                .rebuildReceipts
                .single()
            assertEquals(null, receipt.localScopeChecksum)
            assertEquals(receipt.finalScopeChecksum, receipt.storedScopeChecksum)
        } finally {
            closeRebuildReceiptFixture(fixture)
        }
    }

    @Test
    fun rebuildReceiptProofAcceptsValidTwoPageReceipts() {
        val fixture = makeRebuildReceiptFixture(recordCount = 3)
        try {
            val inspection = SynchroInspection(fixture.client)
            val proof = inspection.rebuildReceipts().single().let(::rebuildReceiptProof)
            val capture = inspection.captureState(maximumRecords = 1)

            assertEquals(TransportObservationCollector.cursorFingerprint(PROOF_REBUILD_ID), proof.rebuildIDFingerprint)
            assertEquals(2, proof.pageCount)
            assertEquals(3, proof.returnedRecordCount)
            assertTrue(proof.requestChainValid)
            assertTrue(proof.recordsInCanonicalOrder)
            assertTrue(proof.rowChecksumsValid)
            assertTrue(proof.scopeChecksumValid)
            assertTrue(proof.finalChecksumMatchesLocal)
            assertEquals(2, capture.rebuildReceiptCount)
            assertEquals(1, capture.rebuildReceipts.size)
            assertFalse(capture.rebuildReceiptsTruncated)
        } finally {
            closeRebuildReceiptFixture(fixture)
        }
    }

    @Test
    fun rebuildReceiptProofRejectsUnknownResponseMember() {
        val fixture = makeRebuildReceiptFixture(recordCount = 3)
        val storedMember = "receipt_content_must_remain_private"
        val storedContent = "receipt-content-must-remain-private"
        try {
            updateReceiptSource(fixture.config, null) { source ->
                source.removeSuffix("}") + ",\"$storedMember\":\"$storedContent\"}"
            }

            val error = assertThrows(SynchroError.InvalidResponse::class.java) {
                SynchroInspection(fixture.client).rebuildReceipts()
            }
            assertTrue(error.details.contains("rebuild response receipt exact shape check failed"))
            assertFalse(error.details.contains(storedMember))
            assertFalse(error.details.contains(storedContent))
        } finally {
            closeRebuildReceiptFixture(fixture)
        }
    }

    @Test
    fun rebuildReceiptProofDetectsBrokenCursorChain() {
        val fixture = makeRebuildReceiptFixture(recordCount = 3)
        try {
            updateReceiptResponse(fixture.config, null) { it.copy(cursor = "unconsumed") }

            val proof = SynchroInspection(fixture.client).rebuildReceipts().single().let(::rebuildReceiptProof)
            assertFalse(proof.requestChainValid)
            assertTrue(proof.recordsInCanonicalOrder)
            assertTrue(proof.rowChecksumsValid)
        } finally {
            closeRebuildReceiptFixture(fixture)
        }
    }

    @Test
    fun rebuildReceiptProofDetectsNoncanonicalRecordOrderIndependently() {
        val fixture = makeRebuildReceiptFixture(recordCount = 3)
        try {
            updateReceiptResponse(fixture.config, null) { response ->
                response.copy(records = response.records.reversed())
            }

            val proof = SynchroInspection(fixture.client).rebuildReceipts().single().let(::rebuildReceiptProof)
            assertTrue(proof.requestChainValid)
            assertFalse(proof.recordsInCanonicalOrder)
            assertTrue(proof.rowChecksumsValid)
            assertTrue(proof.scopeChecksumValid)
            assertTrue(proof.finalChecksumMatchesLocal)
        } finally {
            closeRebuildReceiptFixture(fixture)
        }
    }

    @Test
    fun rebuildReceiptProofDetectsWrongRowChecksumIndependently() {
        val fixture = makeRebuildReceiptFixture(recordCount = 3)
        try {
            updateReceiptResponse(fixture.config, null) { response ->
                response.copy(
                    records = response.records.toMutableList().also { records ->
                        records[0] = records[0].copy(
                            rowChecksum = records[0].rowChecksum.copy(digest = "f".repeat(64)),
                        )
                    },
                )
            }

            val proof = SynchroInspection(fixture.client).rebuildReceipts().single().let(::rebuildReceiptProof)
            assertTrue(proof.requestChainValid)
            assertTrue(proof.recordsInCanonicalOrder)
            assertFalse(proof.rowChecksumsValid)
            assertTrue(proof.scopeChecksumValid)
            assertTrue(proof.finalChecksumMatchesLocal)
        } finally {
            closeRebuildReceiptFixture(fixture)
        }
    }

    @Test
    fun rebuildReceiptProofDetectsWrongScopeChecksumIndependently() {
        val fixture = makeRebuildReceiptFixture(recordCount = 3)
        val forged = ChecksumObject("sha256", 1, "hex", "e".repeat(64))
        try {
            updateReceiptResponse(fixture.config, PROOF_SECOND_CURSOR) { response ->
                response.copy(checksum = forged)
            }
            withInternalDatabase(fixture.config) { database ->
                database.writeTransaction { db ->
                    db.execSQL(
                        """
                        UPDATE _synchro_rebuild_page_receipts
                        SET final_checksum = ?
                        WHERE scope_id = ? AND rebuild_id = ? AND request_cursor_is_null = 0 AND request_cursor = ?
                        """.trimIndent(),
                        arrayOf(wireJSON.encodeToString(forged), PROOF_SCOPE_ID, PROOF_REBUILD_ID, PROOF_SECOND_CURSOR),
                    )
                }
            }

            val proof = SynchroInspection(fixture.client).rebuildReceipts().single().let(::rebuildReceiptProof)
            assertTrue(proof.requestChainValid)
            assertTrue(proof.recordsInCanonicalOrder)
            assertTrue(proof.rowChecksumsValid)
            assertFalse(proof.scopeChecksumValid)
            assertFalse(proof.finalChecksumMatchesLocal)
        } finally {
            closeRebuildReceiptFixture(fixture)
        }
    }

    private fun prepareClientConfig(): SynchroConfig {
        val dbPath = "synchro_inspection_${UUID.randomUUID()}.sqlite"
        val database = SynchroDatabase.open(context, dbPath)
        try {
            installTestSchema(
                database,
                SchemaResponse(
                    1,
                    PROTOCOL_TEST_SCHEMA_HASH,
                    "2026-01-01T00:00:00.000000Z",
                    listOf(table),
                ),
            )
        } finally {
            database.close()
        }
        return SynchroConfig(
            dbPath = dbPath,
            serverURL = "http://localhost:8080",
            authProvider = { "test-token" },
            clientID = "inspection-device",
            appVersion = "1.0.0",
        )
    }

    private data class RebuildReceiptFixture(
        val client: SynchroClient,
        val config: SynchroConfig,
    )

    private fun makeRebuildReceiptFixture(recordCount: Int): RebuildReceiptFixture {
        require(recordCount == 0 || recordCount == 3)
        val config = prepareClientConfig()
        val localTable = table.localSchema
        val recordsWithDigests = (1..recordCount).map { index ->
            val id = "o$index"
            val serverVersion = "2026-01-01T00:00:0$index.000000Z"
            val pk = buildJsonObject { put("id", id) }
            val row = buildJsonObject {
                put("id", id)
                put("title", "title-$id")
                put("updated_at", serverVersion)
                put("deleted_at", JsonNull)
            }
            val digest = Integrity.rowDigest(PROTOCOL_TEST_SCHEMA_HASH, localTable, pk, row, serverVersion)
            RebuildRecord(
                table = localTable.tableID,
                pk = pk,
                row = row,
                rowChecksum = digest.checksum,
                serverVersion = serverVersion,
            ) to digest
        }.sortedWith { left, right -> compareUnsigned(left.second.identity, right.second.identity) }
        val records = recordsWithDigests.map { it.first }
        val finalChecksum = Integrity.scopeDigest(
            PROTOCOL_TEST_SCHEMA_HASH,
            PROOF_SCOPE_ID,
            recordsWithDigests.map { it.second.identity to it.second.checksum },
        )
        withInternalDatabase(config) { database ->
            database.writeTransaction { db ->
                SynchroMeta.upsertScope(
                    db,
                    scopeId = PROOF_SCOPE_ID,
                    cursor = PROOF_FINAL_CURSOR,
                    checksum = wireJSON.encodeToString(finalChecksum),
                    generation = 1,
                    localChecksum = wireJSON.encodeToString(finalChecksum),
                )
                if (recordCount == 0) {
                    insertRebuildReceipt(
                        db = db,
                        requestCursor = null,
                        records = emptyList(),
                        responseCursor = null,
                        hasMore = false,
                        finalChecksum = finalChecksum,
                    )
                } else {
                    insertRebuildReceipt(
                        db = db,
                        requestCursor = null,
                        records = records.take(2),
                        responseCursor = PROOF_SECOND_CURSOR,
                        hasMore = true,
                        finalChecksum = null,
                    )
                    insertRebuildReceipt(
                        db = db,
                        requestCursor = PROOF_SECOND_CURSOR,
                        records = records.drop(2),
                        responseCursor = null,
                        hasMore = false,
                        finalChecksum = finalChecksum,
                    )
                }
            }
        }
        return RebuildReceiptFixture(SynchroClient(config, context), config)
    }

    private fun insertRebuildReceipt(
        db: android.database.sqlite.SQLiteDatabase,
        requestCursor: String?,
        records: List<RebuildRecord>,
        responseCursor: String?,
        hasMore: Boolean,
        finalChecksum: ChecksumObject?,
    ) {
        val request = RebuildRequest(
            clientID = "inspection-device",
            clientGeneration = 1,
            schema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
            scope = PROOF_SCOPE_ID,
            rebuildID = PROOF_REBUILD_ID,
            cursor = requestCursor,
            limit = 2,
        )
        val response = RebuildResponse(
            scope = PROOF_SCOPE_ID,
            records = records,
            cursor = responseCursor,
            hasMore = hasMore,
            finalScopeCursor = if (hasMore) null else PROOF_FINAL_CURSOR,
            checksum = finalChecksum,
        )
        SynchroMeta.insertRebuildPageReceipt(
            db = db,
            scopeId = PROOF_SCOPE_ID,
            rebuildId = PROOF_REBUILD_ID,
            requestCursor = requestCursor,
            requestJSON = wireJSON.encodeToString(request),
            responseJSON = wireJSON.encodeToString(response),
            finalScopeCursor = response.finalScopeCursor,
            finalChecksumJSON = finalChecksum?.let(wireJSON::encodeToString),
        )
    }

    private fun updateReceiptResponse(
        config: SynchroConfig,
        requestCursor: String?,
        update: (RebuildResponse) -> RebuildResponse,
    ) {
        updateReceiptSource(config, requestCursor) { source ->
            wireJSON.encodeToString(update(wireJSON.decodeFromString<RebuildResponse>(source)))
        }
    }

    private fun updateReceiptSource(
        config: SynchroConfig,
        requestCursor: String?,
        update: (String) -> String,
    ) {
        withInternalDatabase(config) { database ->
            database.writeTransaction { db ->
                val source = db.rawQuery(
                    """
                    SELECT response_json FROM _synchro_rebuild_page_receipts
                    WHERE scope_id = ? AND rebuild_id = ? AND request_cursor_is_null = ? AND request_cursor = ?
                    """.trimIndent(),
                    arrayOf(
                        PROOF_SCOPE_ID,
                        PROOF_REBUILD_ID,
                        if (requestCursor == null) "1" else "0",
                        requestCursor ?: "",
                    ),
                ).use { cursor ->
                    check(cursor.moveToFirst())
                    cursor.getString(0)
                }
                db.execSQL(
                    """
                    UPDATE _synchro_rebuild_page_receipts SET response_json = ?
                    WHERE scope_id = ? AND rebuild_id = ? AND request_cursor_is_null = ? AND request_cursor = ?
                    """.trimIndent(),
                    arrayOf(
                        update(source),
                        PROOF_SCOPE_ID,
                        PROOF_REBUILD_ID,
                        if (requestCursor == null) 1 else 0,
                        requestCursor ?: "",
                    ),
                )
            }
        }
    }

    private fun closeRebuildReceiptFixture(fixture: RebuildReceiptFixture) {
        fixture.client.close()
        context.deleteDatabase(fixture.config.dbPath)
    }

    private fun compareUnsigned(left: ByteArray, right: ByteArray): Int {
        for (index in 0 until minOf(left.size, right.size)) {
            val difference = (left[index].toInt() and 0xff) - (right[index].toInt() and 0xff)
            if (difference != 0) return difference
        }
        return left.size - right.size
    }

    private fun internalQuery(config: SynchroConfig, sql: String): List<Row> =
        withInternalDatabase(config) { database -> database.query(sql) }

    private fun internalQueryOne(config: SynchroConfig, sql: String): Row? =
        withInternalDatabase(config) { database -> database.queryOne(sql) }

    private fun <T> withInternalDatabase(config: SynchroConfig, block: (SynchroDatabase) -> T): T {
        val database = SynchroDatabase.open(context, config.dbPath)
        return try {
            block(database)
        } finally {
            database.close()
        }
    }

    private companion object {
        const val PROOF_SCOPE_ID = "proof-scope"
        const val PROOF_REBUILD_ID = "proof-rebuild"
        const val PROOF_SECOND_CURSOR = "page-2"
        const val PROOF_FINAL_CURSOR = "scope-final"
    }
}
