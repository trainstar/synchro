package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import java.io.IOException
import java.util.UUID
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28])
class LifecycleDurabilityTests {
    private val context = ApplicationProvider.getApplicationContext<Context>()

    @Test
    fun preparedMigrationJournalRecoversAfterDatabaseReopen() {
        val dbName = databaseName()
        try {
            val target = targetManifest()
            val first = SynchroDatabase.open(context, dbName)
            try {
                val schemaManager = SchemaManager(first)
                schemaManager.reconcileLocalSchema(
                    schemaVersion = 1,
                    schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
                    tables = protocolOrdersSchemaManifest().localTables(),
                )
                val journal = schemaManager.prepareConnectMigration(
                    response = migrationResponse(target),
                    targetTables = target.localTables(),
                    resetMaterialization = false,
                )
                assertNotNull(journal)
                assertEquals(
                    "prepared",
                    first.queryOne("SELECT phase FROM _synchro_migration_journal")?.get("phase"),
                )
            } finally {
                first.close()
            }

            val recovered = SynchroDatabase.open(context, dbName)
            try {
                SchemaManager(recovered).recoverPendingMigration()
                assertEquals(
                    2L,
                    recovered.readTransaction { db -> SynchroMeta.getInt64(db, MetaKey.SCHEMA_VERSION) },
                )
                assertEquals(
                    target.schemaHash,
                    recovered.readTransaction { db -> SynchroMeta.get(db, MetaKey.SCHEMA_HASH) },
                )
                assertTrue(
                    recovered.query("PRAGMA table_info(orders)").map { it.getValue("name") }.contains("notes"),
                )
                assertEquals(
                    "ddl_applied",
                    recovered.queryOne("SELECT phase FROM _synchro_migration_journal")?.get("phase"),
                )
            } finally {
                recovered.close()
            }
        } finally {
            context.deleteDatabase(dbName)
        }
    }

    @Test
    fun typedBlockingErrorSurvivesReopenUntilExplicitAcknowledgement() {
        val dbName = databaseName()
        try {
            val first = SynchroDatabase.open(context, dbName)
            try {
                first.writeTransaction { db ->
                    SynchroMeta.transitionClientLifecycleState(db, SyncLifecycleState.LOCAL_READY)
                    SynchroMeta.recordBlockingError(
                        db,
                        SyncFailure(
                            operation = SyncOperationKind.CONNECTING,
                            code = SyncFailureCode.UPGRADE_REQUIRED,
                            retryable = false,
                            message = "The installed schema requires an explicit synchronized reset.",
                            recoveryAction = SyncRecoveryAction.SCHEMA_RESET,
                            metadata = mapOf("reason" to "unknown_schema_lineage"),
                        ),
                    )
                }
            } finally {
                first.close()
            }

            val reopened = SynchroDatabase.open(context, dbName)
            try {
                val persisted = reopened.readTransaction { db -> SynchroMeta.getClientState(db) }
                assertEquals(SyncLifecycleState.ERROR, persisted.lifecycleState)
                assertEquals(SyncFailureCode.UPGRADE_REQUIRED, persisted.failure?.code)
                assertEquals(SyncOperationKind.CONNECTING, persisted.failure?.operation)
                assertEquals(
                    "The installed schema requires an explicit synchronized reset.",
                    persisted.failure?.message,
                )
                assertEquals(SyncRecoveryAction.SCHEMA_RESET, persisted.failure?.recoveryAction)
                assertEquals(mapOf("reason" to "unknown_schema_lineage"), persisted.failure?.metadata)
                assertFalse(persisted.errorAcknowledged)

                reopened.writeTransaction { db -> SynchroMeta.acknowledgeBlockingError(db) }
                val acknowledged = reopened.readTransaction { db -> SynchroMeta.getClientState(db) }
                assertEquals(SyncLifecycleState.LOCAL_READY, acknowledged.lifecycleState)
                assertEquals(SyncFailureCode.UPGRADE_REQUIRED, acknowledged.failure?.code)
                assertEquals(SyncRecoveryAction.SCHEMA_RESET, acknowledged.failure?.recoveryAction)
                assertEquals(mapOf("reason" to "unknown_schema_lineage"), acknowledged.failure?.metadata)
                assertTrue(acknowledged.errorAcknowledged)
            } finally {
                reopened.close()
            }
        } finally {
            context.deleteDatabase(dbName)
        }
    }

    @Test
    fun durableRetryRecordPreservesExactIdentityAcrossReopen() {
        val dbName = databaseName()
        val exactRequest = "{\"client_id\":\"client\",\"scope\":\"orders:user\"}"
        try {
            val first = SynchroDatabase.open(context, dbName)
            try {
                DurableBackoffStore.persist(
                    database = first,
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
            } finally {
                first.close()
            }

            val reopened = SynchroDatabase.open(context, dbName)
            try {
                val retry = requireNotNull(DurableBackoffStore.load(reopened))
                assertEquals(RetryOperation.PULLING, retry.resumeState)
                assertEquals(exactRequest, retry.workIdentity)
                assertEquals(1L, retry.attemptCount)
                assertEquals(3_000L, retry.nextRetryAtMs)
            } finally {
                reopened.close()
            }
        } finally {
            context.deleteDatabase(dbName)
        }
    }

    @Test
    fun resetMigrationPreservesLocalOnlyDataIntentAndOutcomesAcrossReopen() {
        val dbName = databaseName()
        val rejectedID = UUID.randomUUID().toString()
        try {
            val target = targetManifest()
            val first = SynchroDatabase.open(context, dbName)
            try {
                val schemaManager = SchemaManager(first)
                schemaManager.reconcileLocalSchema(
                    schemaVersion = 1,
                    schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
                    tables = protocolOrdersSchemaManifest().localTables(),
                )
                first.createLocalOnlyTable(
                    "drafts",
                    listOf(
                        ColumnDef("id", "TEXT", nullable = false, primaryKey = true),
                        ColumnDef("body", "TEXT", nullable = false),
                    ),
                )
                first.execute("INSERT INTO drafts (id, body) VALUES ('d1', 'preserve me')")
                first.execute(
                    "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                    arrayOf("queued", "Queued address", "u1", "2026-01-01T00:00:00.000000Z"),
                )
                first.writeTransaction { db ->
                    SynchroMeta.upsertRejectedMutation(
                        db = db,
                        mutationID = rejectedID,
                        tableName = "orders",
                        recordId = "rejected",
                        status = "rejected_terminal",
                        code = "policy_rejected",
                        message = "retained outcome",
                        serverRowJson = null,
                        serverVersion = null,
                        mutationJSON = "{}",
                        rejectionJSON = "{}",
                    )
                }
                schemaManager.prepareConnectMigration(
                    response = migrationResponse(target),
                    targetTables = target.localTables(),
                    resetMaterialization = true,
                )
                first.writeSyncLockedTransaction { db ->
                    schemaManager.applyPreparedMigrationInTransaction(db)
                }
            } finally {
                first.close()
            }

            val reopened = SynchroDatabase.open(context, dbName)
            try {
                SchemaManager(reopened).recoverPendingMigration()
                assertEquals("preserve me", reopened.queryOne("SELECT body FROM drafts WHERE id = 'd1'")?.get("body"))
                assertEquals(1L, reopened.queryOne("SELECT COUNT(*) AS count FROM _synchro_pending_changes")?.get("count"))
                assertEquals(
                    rejectedID,
                    reopened.readTransaction { db -> SynchroMeta.listRejectedMutations(db).single().mutationID },
                )
                assertTrue(reopened.query("SELECT id FROM orders").isEmpty())
                assertTrue(
                    reopened.query("PRAGMA table_info(orders)").map { it.getValue("name") }.contains("notes"),
                )
            } finally {
                reopened.close()
            }
        } finally {
            context.deleteDatabase(dbName)
        }
    }

    @Test
    fun schemaResetRenewsSealedIntentAndDropsStaleBackoffAfterAbruptReopen() = runBlocking {
        val dbName = databaseName()
        val server = MockWebServer()
        server.enqueue(
            MockResponse()
                .setResponseCode(503)
                .setHeader("Retry-After", "1")
                .setBody(RETRYABLE_503_ERROR_JSON),
        )
        server.start()
        val target = targetManifest()
        val config = SynchroConfig(
            dbPath = dbName,
            serverURL = server.url("/").toString().trimEnd('/'),
            authProvider = { "token" },
            clientID = "reset-device",
            appVersion = "1.0.0",
        )
        lateinit var originalBatchID: String
        lateinit var originalRequestJSON: String
        try {
            val first = SynchroDatabase.open(context, dbName)
            try {
                val sourceTables = protocolOrdersSchemaManifest().localTables()
                SchemaManager(first).reconcileLocalSchema(1, PROTOCOL_TEST_SCHEMA_HASH, sourceTables)
                first.applicationExecute(
                    "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                    arrayOf("queued", "Queued", "u1", "2026-01-01T00:00:00.000000Z"),
                )
                val pushProcessor = PushProcessor(first, ChangeTracker(first))
                val pushFailure = runCatching {
                    pushProcessor.processPush(
                        HttpClient(config),
                        config.clientID,
                        1,
                        1,
                        PROTOCOL_TEST_SCHEMA_HASH,
                        sourceTables,
                    )
                }.exceptionOrNull()
                assertTrue(pushFailure is RetryableError)
                val batch = requireNotNull(
                    first.queryOne("SELECT batch_id, request_json FROM _synchro_push_batches WHERE state = 'pending'"),
                )
                originalBatchID = batch.getValue("batch_id") as String
                originalRequestJSON = batch.getValue("request_json") as String
                installDurableBackoff(first, RetryOperation.PUSHING, originalBatchID)

                val engine = SyncEngine(
                    config,
                    first,
                    HttpClient(config),
                    SchemaManager(first),
                    ChangeTracker(first),
                    PullProcessor(first),
                    pushProcessor,
                )
                val response = migrationResponse(target)
                engine.installConnectResponse(response, schemaReset = true)
                assertEquals(
                    "reset_renewal_required",
                    first.queryOne("SELECT state FROM _synchro_push_batches WHERE batch_id = ?", arrayOf(originalBatchID))?.get("state"),
                )
                assertEquals(originalRequestJSON, first.queryOne(
                    "SELECT request_json FROM _synchro_push_batches WHERE batch_id = ?",
                    arrayOf(originalBatchID),
                )?.get("request_json"))
                assertTrue(first.query("SELECT * FROM _synchro_backoff").isEmpty())

                installDurableBackoff(first, RetryOperation.PULLING, "obsolete-pull")
                engine.installConnectResponse(response, schemaReset = true)
                assertTrue(first.query("SELECT * FROM _synchro_backoff").isEmpty())

                installDurableBackoff(first, RetryOperation.REBUILDING, "obsolete-rebuild")
                engine.installConnectResponse(response, schemaReset = true)
                assertTrue(first.query("SELECT * FROM _synchro_backoff").isEmpty())
            } finally {
                first.close()
            }

            val reopened = SynchroDatabase.open(context, dbName)
            try {
                val renewed = PushProcessor(reopened, ChangeTracker(reopened)).renewRequiredBatches(
                    clientID = config.clientID,
                    clientGeneration = 1,
                    schemaVersion = target.schemaVersion,
                    schemaHash = target.schemaHash,
                    syncedTables = target.localTables(),
                )
                assertTrue(renewed)
                assertEquals(
                    "superseded",
                    reopened.queryOne("SELECT state FROM _synchro_push_batches WHERE batch_id = ?", arrayOf(originalBatchID))?.get("state"),
                )
                assertEquals(originalRequestJSON, reopened.queryOne(
                    "SELECT request_json FROM _synchro_push_batches WHERE batch_id = ?",
                    arrayOf(originalBatchID),
                )?.get("request_json"))
                val successor = requireNotNull(
                    reopened.queryOne("SELECT batch_id, request_json FROM _synchro_push_batches WHERE state = 'pending'"),
                )
                val successorRequest = Json.decodeFromString<PushRequest>(successor.getValue("request_json") as String)
                assertEquals(target.schemaVersion, successorRequest.schema.version)
                assertEquals(target.schemaHash, successorRequest.schema.hash)
                assertEquals(1, successorRequest.mutations.size)
                assertEquals(
                    successor.getValue("batch_id"),
                    reopened.queryOne("SELECT sealed_batch_id FROM _synchro_pending_changes")?.get("sealed_batch_id"),
                )
            } finally {
                reopened.close()
            }
        } finally {
            server.shutdown()
            context.deleteDatabase(dbName)
        }
    }

    @Test
    fun migrationRecoveryRejectsEveryMutatedClientOwnedPhysicalObject() {
        assertRecoveryRejectsPhysicalMutation("column type") { database, table ->
            replaceOrdersPhysicalTable(database, table, "id BLOB PRIMARY KEY")
        }
        assertRecoveryRejectsPhysicalMutation("column nullability") { database, table ->
            replaceOrdersPhysicalTable(database, table, "id TEXT PRIMARY KEY", "user_id TEXT")
        }
        assertRecoveryRejectsPhysicalMutation("primary key") { database, table ->
            replaceOrdersPhysicalTable(database, table, "id TEXT")
        }
        assertRecoveryRejectsPhysicalMutation("trigger definition") { database, _ ->
            database.execute("DROP TRIGGER _synchro_cdc_insert_orders")
            database.execute(
                """
                CREATE TRIGGER _synchro_cdc_insert_orders
                AFTER INSERT ON orders
                BEGIN
                    SELECT 1;
                END
                """.trimIndent(),
            )
            assertTrue(
                database.queryOne("SELECT sql FROM sqlite_master WHERE name = '_synchro_cdc_insert_orders'")
                    ?.get("sql")?.toString()?.contains("SELECT 1;") == true,
            )
        }
        assertRecoveryRejectsPhysicalMutation("index definition") { database, _ ->
            database.execute("DROP INDEX idx_orders_user")
            database.execute("CREATE INDEX idx_orders_user ON orders (notes)")
        }
        assertRecoveryRejectsPhysicalMutation("unexpected index") { database, _ ->
            database.execute("CREATE INDEX unexpected_orders_index ON orders (notes)")
        }
        assertRecoveryRejectsPhysicalMutation("unexpected trigger") { database, _ ->
            database.execute(
                """
                CREATE TRIGGER unexpected_orders_trigger
                AFTER INSERT ON orders
                BEGIN
                    SELECT 1;
                END
                """.trimIndent(),
            )
        }
    }

    private fun assertRecoveryRejectsPhysicalMutation(
        name: String,
        mutate: (SynchroDatabase, LocalSchemaTable) -> Unit,
    ) {
        val dbName = databaseName()
        try {
            val target = targetManifestWithIndex()
            val database = SynchroDatabase.open(context, dbName)
            try {
                val manager = SchemaManager(database)
                manager.reconcileLocalSchema(
                    schemaVersion = 1,
                    schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
                    tables = protocolOrdersSchemaManifest().localTables(),
                )
                manager.prepareConnectMigration(
                    response = migrationResponse(target),
                    targetTables = target.localTables(),
                    resetMaterialization = false,
                )
                database.writeSyncLockedTransaction { db ->
                    manager.applyPreparedMigrationInTransaction(db)
                }
                assertEquals(
                    "ddl_applied",
                    database.queryOne("SELECT phase FROM _synchro_migration_journal")?.get("phase"),
                )
                assertEquals(
                    target.schemaVersion,
                    database.readTransaction { db -> SynchroMeta.getInt64(db, MetaKey.SCHEMA_VERSION) },
                )
                assertEquals(
                    target.schemaHash,
                    database.readTransaction { db -> SynchroMeta.get(db, MetaKey.SCHEMA_HASH) },
                )

                mutate(database, target.localTables().single())
                val triggerValidationFailure = runCatching {
                    database.readTransaction { db ->
                        ApplicationWriteGuard.requireExactAllowedTriggerSet(db, target.localTables())
                    }
                }.exceptionOrNull()
                if (name == "trigger definition") {
                    assertTrue(
                        "trigger validation accepted a modified definition: $triggerValidationFailure",
                        triggerValidationFailure is IllegalStateException,
                    )
                }
                val failure = runCatching { manager.recoverPendingMigration() }.exceptionOrNull()
                assertTrue("accepted mutated $name: $failure", failure is SynchroError.InvalidResponse)
            } finally {
                database.close()
            }
        } finally {
            context.deleteDatabase(dbName)
        }
    }

    private fun replaceOrdersPhysicalTable(
        database: SynchroDatabase,
        table: LocalSchemaTable,
        idDefinition: String,
        userIDDefinition: String = "user_id TEXT NOT NULL",
    ) {
        database.writeTransaction { db ->
            SQLiteSchema.expectedCDCTriggerSQL(table).keys.forEach { trigger ->
                db.execSQL("DROP TRIGGER IF EXISTS ${SQLiteHelpers.quoteIdentifier(trigger)}")
            }
            db.execSQL("DROP TABLE orders")
            db.execSQL(
                """
                CREATE TABLE orders (
                    $idDefinition,
                    ship_address TEXT,
                    $userIDDefinition,
                    notes TEXT,
                    updated_at TEXT NOT NULL,
                    deleted_at TEXT
                )
                """.trimIndent(),
            )
            SQLiteSchema.generateCDCTriggers(table).forEach(db::execSQL)
            db.execSQL(SQLiteSchema.generateCreateIndexSQL(table, table.indexes.single()))
        }
    }

    private fun migrationResponse(target: SchemaManifest): ConnectResponse = ConnectResponse(
        serverTime = "2026-01-01T00:00:00.000Z",
        protocolVersion = 3,
        clientGeneration = 1,
        scopeSetVersion = 0,
        schema = SchemaDescriptor(target.schemaVersion, target.schemaHash, SchemaAction.REPLACE),
        scopes = ScopeAssignmentDelta(emptyList(), emptyList()),
        scopeCursorUpdates = emptyMap(),
        schemaDefinition = target,
    )

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

    private fun targetManifestWithIndex(): SchemaManifest {
        val base = targetManifest()
        val indexedTable = base.tables.single().copy(
            indexes = listOf(
                IndexSchema(
                    indexID = "idx-orders-user",
                    name = "idx_orders_user",
                    fieldIDs = listOf("field-user-id"),
                    unique = false,
                ),
            ),
        )
        val draft = base.copy(schemaHash = "0".repeat(64), tables = listOf(indexedTable))
        return draft.copy(schemaHash = Integrity.schemaManifestHash(draft))
    }

    private fun databaseName(): String = "synchro_lifecycle_${UUID.randomUUID()}.sqlite"
}
