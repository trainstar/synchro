package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.junit.After
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28])
class PullProcessorTests {
    private val databases = TestDatabaseTracker()

    private val testTable = SchemaTable(
        tableName = "orders",
        pushPolicy = "owner_only",
        updatedAtColumn = "updated_at",
        deletedAtColumn = "deleted_at",
        primaryKey = listOf("id"),
        columns = listOf(
            SchemaColumn(name = "id", dbType = "uuid", logicalType = "string", nullable = false, isPrimaryKey = true),
            SchemaColumn(name = "ship_address", dbType = "text", logicalType = "string", nullable = true, isPrimaryKey = false),
            SchemaColumn(name = "updated_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
            SchemaColumn(name = "deleted_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = true, isPrimaryKey = false),
        )
    )
    private val localTestTable = testTable.localSchema
    private val typedTable = SchemaTable(
        tableName = "typed_orders",
        updatedAtColumn = "updated_at",
        deletedAtColumn = "deleted_at",
        primaryKey = listOf("id"),
        columns = listOf(
            SchemaColumn(name = "id", logicalType = "string", nullable = false, isPrimaryKey = true),
            SchemaColumn(name = "payload", logicalType = "bytes"),
            SchemaColumn(name = "large_count", logicalType = "int64"),
            SchemaColumn(name = "enabled", logicalType = "boolean"),
            SchemaColumn(name = "updated_at", logicalType = "datetime", nullable = false),
            SchemaColumn(name = "deleted_at", logicalType = "datetime"),
        ),
    )

    private fun makeTestEnv(table: SchemaTable = testTable): Pair<SynchroDatabase, PullProcessor> {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val db = databases.create(context)
        val schema = SchemaResponse(
            schemaVersion = 1, schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            serverTime = "2026-01-01T12:00:00.000000Z", tables = listOf(table)
        )
        installTestSchema(db, schema)
        return Pair(db, PullProcessor(db))
    }

    private fun insertOrder(
        db: SynchroDatabase,
        id: String,
        shipAddress: String = "123 Main St",
        updatedAt: String,
        deletedAt: String? = null
    ) {
        db.writeSyncLockedTransaction { conn ->
            val stmt = conn.compileStatement(
                "INSERT INTO orders (id, ship_address, updated_at, deleted_at) VALUES (?, ?, ?, ?)"
            )
            try {
                stmt.bindString(1, id)
                stmt.bindString(2, shipAddress)
                stmt.bindString(3, updatedAt)
                if (deletedAt == null) {
                    stmt.bindNull(4)
                } else {
                    stmt.bindString(4, deletedAt)
                }
                stmt.executeInsert()
            } finally {
                stmt.close()
            }
        }
    }

    private fun addScopeRow(
        db: SynchroDatabase,
        scopeId: String,
        recordId: String,
        checksum: String = "{\"algorithm\":\"sha256\",\"version\":1,\"encoding\":\"hex\",\"digest\":\"0000000000000000000000000000000000000000000000000000000000000000\"}",
        generation: Long = 0
    ) {
        db.writeTransaction { conn ->
            SynchroMeta.upsertScope(conn, scopeId, "10", null, generation)
            SynchroMeta.upsertScopeRow(conn, scopeId, "orders", recordId, checksum, generation)
        }
    }

    private fun pendingChangeCount(db: SynchroDatabase): Int {
        return ChangeTracker(db).pendingChangeCount()
    }

    private fun orderRow(
        id: String,
        shipAddress: String,
        updatedAt: String = "2026-01-04T00:00:00.000000Z",
        deletedAt: String? = null,
    ) = buildJsonObject {
        put("id", id)
        put("ship_address", shipAddress)
        put("updated_at", updatedAt)
        if (deletedAt == null) put("deleted_at", JsonNull) else put("deleted_at", deletedAt)
    }

    private fun rebuildRecord(id: String, shipAddress: String): RebuildRecord {
        val serverVersion = "server-$id"
        val row = orderRow(id, shipAddress)
        return RebuildRecord(
            table = localTestTable.tableID,
            pk = buildJsonObject { put("id", id) },
            row = row,
            rowChecksum = Integrity.rowDigest(
                PROTOCOL_TEST_SCHEMA_HASH,
                localTestTable,
                buildJsonObject { put("id", id) },
                row,
                serverVersion,
            ).checksum,
            serverVersion = serverVersion,
        )
    }

    private fun rebuildRequestJSON(request: RebuildRequest): String =
        Json { encodeDefaults = true }.encodeToString(request)

    private fun rebuildResponseJSON(response: RebuildResponse): String =
        Json { encodeDefaults = true }.encodeToString(response)

    private fun scopeChecksum(
        scopeID: String,
        records: List<Pair<String, ChecksumObject>>,
    ): ChecksumObject = Integrity.scopeDigest(
        PROTOCOL_TEST_SCHEMA_HASH,
        scopeID,
        records.map { (recordID, checksum) ->
            Integrity.rowIdentity(
                localTestTable,
                buildJsonObject { put(localTestTable.primaryKeyFieldID, recordID) },
            ) to checksum
        },
    )

    private fun insertPendingChange(
        db: SynchroDatabase,
        recordID: String,
        state: String,
        mutationID: String = java.util.UUID.randomUUID().toString(),
    ) {
        db.writeTransaction { conn ->
            val sealed = state == "sealed"
            conn.execSQL(
                """
                INSERT INTO _synchro_pending_changes (
                    mutation_id, table_id, table_name, record_id, pk_field_id, pk_logical_type,
                    operation, authored_schema_version, authored_schema_hash, base_version, client_version,
                    lifecycle_state, source_kind, sealed_batch_id, sealed_ordinal, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """.trimIndent(),
                arrayOf(
                    mutationID,
                    localTestTable.tableID,
                    localTestTable.tableName,
                    recordID,
                    localTestTable.primaryKeyFieldID,
                    "string",
                    "update",
                    1,
                    PROTOCOL_TEST_SCHEMA_HASH,
                    "base-$recordID",
                    "2026-01-01T00:00:00.000000Z",
                    state,
                    "test",
                    if (sealed) "batch-$mutationID" else null,
                    if (sealed) 0 else null,
                    "2026-01-01T00:00:00.000000Z",
                    "2026-01-01T00:00:00.000000Z",
                ),
            )
        }
    }

    private fun installReceiptBackedScope(
        db: SynchroDatabase,
        scopeID: String,
        recordID: String,
        shipAddress: String,
    ): String {
        val updatedAt = "2026-01-04T00:00:00.000000Z"
        val serverVersion = "server-$recordID"
        val row = orderRow(recordID, shipAddress, updatedAt)
        val rowDigest = Integrity.rowDigest(
            PROTOCOL_TEST_SCHEMA_HASH,
            localTestTable,
            buildJsonObject { put(localTestTable.primaryKeyFieldID, recordID) },
            row,
            serverVersion,
        )
        val scopeChecksum = scopeChecksum(scopeID, listOf(recordID to rowDigest.checksum))
        val checksumJSON = Json.encodeToString(ChecksumObject.serializer(), scopeChecksum)

        insertOrder(db, recordID, shipAddress, updatedAt)
        db.writeSyncLockedTransaction { conn ->
            SynchroMeta.upsertRowVersion(conn, "orders", recordID, serverVersion, rowDigest.checksum)
            SynchroMeta.upsertScopeRow(conn, scopeID, "orders", recordID, rowDigest.checksum.digest, 0L)
            SynchroMeta.upsertScope(conn, scopeID, null, checksumJSON, 0L, checksumJSON)
            conn.execSQL(
                """
                INSERT INTO _synchro_seed_receipts
                    (scope_id, receipt, schema_version, schema_hash, cardinality, checksum)
                VALUES (?, ?, 1, ?, 1, ?)
                """.trimIndent(),
                arrayOf(scopeID, "receipt-$scopeID", PROTOCOL_TEST_SCHEMA_HASH, checksumJSON),
            )
        }
        return checksumJSON
    }

    @After
    fun tearDown() {
        databases.closeAll()
    }

    @Test
    fun scopeRowMaintenanceCursorCountsCommittedAffectedRowsOnly() {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val db = databases.create(context)
        val checksum = "checksum"

        assertEquals(0L, db.provenanceMaintenanceWorkCursor())
        db.writeTransaction { conn ->
            SynchroMeta.upsertScopeRow(conn, "scope", "orders", "one", checksum, 0L)
            SynchroMeta.upsertScopeRow(conn, "scope", "orders", "two", checksum, 0L)
            SynchroMeta.updateScopeRowChecksum(conn, "scope", "orders", "one", "updated")
            SynchroMeta.deleteScopeRow(conn, "scope", "orders", "two")
            SynchroMeta.deleteScopeRows(conn, "scope")
            SynchroMeta.upsertScopeRow(conn, "scope", "orders", "stale", checksum, 0L)
            SynchroMeta.upsertScopeRow(conn, "scope", "orders", "current", checksum, 1L)
            SynchroMeta.deleteStaleScopeRows(conn, "scope", 1L)
            SynchroMeta.clearAllScopeRows(conn)
        }

        assertEquals(9L, db.provenanceMaintenanceWorkCursor())
        assertTrue(db.readTransaction { conn -> SynchroMeta.listScopeRows(conn, 10).isEmpty() })

        assertThrows(IllegalStateException::class.java) {
            db.writeTransaction { conn ->
                SynchroMeta.upsertScopeRow(conn, "scope", "orders", "rolled-back", checksum, 1L)
                SynchroMeta.clearAllScopeRows(conn)
                throw IllegalStateException("rollback")
            }
        }

        assertEquals(9L, db.provenanceMaintenanceWorkCursor())
        assertTrue(db.readTransaction { conn -> SynchroMeta.listScopeRows(conn, 10).isEmpty() })
    }

    @Test
    fun testUpdateCheckpointAdvancesForward() {
        val (db, processor) = makeTestEnv()

        processor.updateCheckpoint(100)
        val cp1 = db.readTransaction { conn -> SynchroMeta.getInt64(conn, MetaKey.CHECKPOINT) }
        assertEquals(100L, cp1)

        // Should not go backward
        processor.updateCheckpoint(50)
        val cp2 = db.readTransaction { conn -> SynchroMeta.getInt64(conn, MetaKey.CHECKPOINT) }
        assertEquals(100L, cp2)

        // Should advance forward
        processor.updateCheckpoint(200)
        val cp3 = db.readTransaction { conn -> SynchroMeta.getInt64(conn, MetaKey.CHECKPOINT) }
        assertEquals(200L, cp3)
    }

    @Test
    fun testApplyScopeDeletePreservesCanonicalDeletedAt() {
        val (db, processor) = makeTestEnv()

        db.writeTransaction { conn ->
            SynchroMeta.setSyncLock(conn, true)
            SynchroMeta.upsertScope(conn, "orders:user1", "10", null)
            SynchroMeta.upsertScopeRow(conn, "orders:user1", "orders", "w1", "{\"algorithm\":\"sha256\",\"version\":1,\"encoding\":\"hex\",\"digest\":\"0000000000000000000000000000000000000000000000000000000000000000\"}", 0)
        }
        db.execute(
            "INSERT INTO orders (id, ship_address, updated_at) VALUES (?, ?, ?)",
            arrayOf("w1", "123 Main St", "2026-01-01T10:00:00.000Z")
        )
        db.writeTransaction { conn ->
            SynchroMeta.setSyncLock(conn, false)
        }

        val change = makeChangeRecord(
            scope = "orders:user1",
            schema = localTestTable,
            op = Operation.DELETE,
            pk = buildJsonObject { put("id", "w1") },
            row = buildJsonObject {
                put("id", "w1")
                put("ship_address", "123 Main St")
                put("updated_at", "2026-01-04T00:00:00.000000Z")
                put("deleted_at", "2026-01-04T00:00:00.000000Z")
            },
            serverVersion = "2026-01-04T00:00:00.000Z"
        )

        processor.applyScopeChanges(
            changes = listOf(change),
            syncedTables = listOf(localTestTable),
            scopeCursors = mapOf("orders:user1" to "11"),
            checksums = null,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
        )

        val row = db.queryOne("SELECT deleted_at FROM orders WHERE id = ?", arrayOf("w1"))
        assertEquals("2026-01-04T00:00:00.000000Z", row?.get("deleted_at"))
    }

    @Test
    fun testApplyScopeDeleteUsesDeletedAtAsEffectiveVersion() {
        val (db, processor) = makeTestEnv()

        addScopeRow(db, "orders:user1", "w1")
        insertOrder(db, "w1", updatedAt = "2026-01-03T00:00:00.000Z")

        val change = makeChangeRecord(
            scope = "orders:user1",
            schema = localTestTable,
            op = Operation.DELETE,
            pk = buildJsonObject { put("id", "w1") },
            row = buildJsonObject {
                put("id", "w1")
                put("ship_address", "123 Main St")
                put("updated_at", "2026-01-03T00:00:00.000000Z")
                put("deleted_at", "2026-01-04T00:00:00.000000Z")
            },
            serverVersion = "2026-01-04T00:00:00.000Z"
        )

        processor.applyScopeChanges(
            changes = listOf(change),
            syncedTables = listOf(localTestTable),
            scopeCursors = mapOf("orders:user1" to "11"),
            checksums = null,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
        )

        val row = db.queryOne("SELECT updated_at, deleted_at FROM orders WHERE id = ?", arrayOf("w1"))
        assertEquals("2026-01-03T00:00:00.000000Z", row?.get("updated_at"))
        assertEquals("2026-01-04T00:00:00.000000Z", row?.get("deleted_at"))
    }

    @Test
    fun testPullRejectsPushOperationsWithoutChangingRows() {
        val (db, processor) = makeTestEnv()
        val row = buildJsonObject {
            put("id", "w1")
            put("ship_address", "server")
            put("updated_at", "2026-01-01T12:00:00.000000Z")
            put("deleted_at", JsonNull)
        }
        val change = makeChangeRecord(
            scope = "orders:user1",
            schema = localTestTable,
            op = Operation.INSERT,
            pk = buildJsonObject { put("id", "w1") },
            row = row,
            serverVersion = "server-version",
        )

        assertThrows(SynchroError.InvalidResponse::class.java) {
            processor.applyScopeChanges(
                listOf(change),
                listOf(localTestTable),
                emptyMap(),
                null,
                PROTOCOL_TEST_SCHEMA_HASH,
            )
        }
        assertNull(db.queryOne("SELECT id FROM orders WHERE id = ?", arrayOf("w1")))
    }

    @Test
    fun testPullRejectsRowPrimaryKeyDifferentFromResponsePrimaryKey() {
        val (db, processor) = makeTestEnv()
        val row = buildJsonObject {
            put("id", "row-id")
            put("ship_address", "server")
            put("updated_at", "2026-01-01T12:00:00.000000Z")
            put("deleted_at", JsonNull)
        }
        val checksum = Integrity.rowDigest(
            PROTOCOL_TEST_SCHEMA_HASH,
            localTestTable,
            buildJsonObject { put("id", "row-id") },
            row,
            "server-version",
        ).checksum
        val change = ChangeRecord(
            scope = "orders:user1",
            table = localTestTable.tableID,
            op = Operation.UPSERT,
            pk = buildJsonObject { put("id", "response-id") },
            row = row,
            rowChecksum = checksum,
            serverVersion = "server-version",
        )

        assertThrows(SynchroError.InvalidResponse::class.java) {
            processor.applyScopeChanges(
                listOf(change),
                listOf(localTestTable),
                emptyMap(),
                null,
                PROTOCOL_TEST_SCHEMA_HASH,
            )
        }
        assertTrue(db.query("SELECT id FROM orders").isEmpty())
    }

    @Test
    fun testPullPageRollsBackAssignmentCleanupWhenChecksumValidationFails() {
        val (db, processor) = makeTestEnv()
        val removedScope = "orders:removed"
        val retainedScope = "orders:retained"
        addScopeRow(db, removedScope, "w1")
        insertOrder(db, "w1", updatedAt = "2026-01-01T12:00:00.000000Z")
        db.writeTransaction { connection ->
            SynchroMeta.upsertScope(connection, retainedScope, "10", null)
            SynchroMeta.setInt64(connection, MetaKey.SCOPE_SET_VERSION, 1)
        }
        val invalidChecksum = ChecksumObject("md5", 1, "hex", "0".repeat(64))

        assertThrows(ContractException::class.java) {
            processor.applyScopeChanges(
                changes = emptyList(),
                syncedTables = listOf(localTestTable),
                scopeCursors = emptyMap(),
                checksums = mapOf(retainedScope to invalidChecksum),
                schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
                scopeUpdates = ScopeAssignmentDelta(emptyList(), listOf(removedScope)),
                scopeSetVersion = 2,
            )
        }

        assertNotNull(db.queryOne("SELECT id FROM orders WHERE id = ?", arrayOf("w1")))
        db.readTransaction { connection ->
            assertNotNull(SynchroMeta.getScope(connection, removedScope))
            assertEquals(1L, SynchroMeta.getInt64(connection, MetaKey.SCOPE_SET_VERSION))
        }
    }

    @Test
    fun testTerminalPullKeepsServerRebuildScopeWithoutUsableCursor() {
        val (db, processor) = makeTestEnv()
        val scopeId = "orders:rebuild"
        db.writeTransaction { connection ->
            SynchroMeta.upsertScope(
                connection,
                scopeId = scopeId,
                cursor = "stale-cursor",
                checksum = "stale-checksum",
            )
        }

        processor.applyScopeChanges(
            changes = emptyList(),
            syncedTables = listOf(localTestTable),
            scopeCursors = emptyMap(),
            checksums = mapOf(scopeId to protocolEmptyScopeChecksum(scopeId)),
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            rebuildScopes = setOf(scopeId),
        )

        val scope = db.readTransaction { connection -> SynchroMeta.getScope(connection, scopeId) }
        assertNull(scope?.cursor)
        assertNull(scope?.checksum)
        assertNotEquals("", scope?.localChecksum)
    }

    @Test
    fun testConnectAssignmentRollsBackGenerationAndCleanupTogether() {
        val (db, processor) = makeTestEnv()
        val removedScope = "orders:removed"
        val addedScope = "orders:added"
        addScopeRow(db, removedScope, "w1")
        insertOrder(db, "w1", updatedAt = "2026-01-01T12:00:00.000000Z")
        db.writeTransaction { connection ->
            SynchroMeta.setInt64(connection, MetaKey.SCOPE_SET_VERSION, 1)
            SynchroMeta.setInt64(connection, MetaKey.CLIENT_GENERATION, 1)
            connection.execSQL(
                """
                INSERT INTO _synchro_seed_receipts
                    (scope_id, receipt, schema_version, schema_hash, cardinality, checksum)
                VALUES (?, ?, ?, ?, ?, ?)
                """.trimIndent(),
                arrayOf(removedScope, "receipt", 1, PROTOCOL_TEST_SCHEMA_HASH, 1, "checksum"),
            )
            connection.execSQL(
                """
                CREATE TRIGGER fail_connected_scope
                BEFORE INSERT ON _synchro_scopes
                WHEN NEW.scope_id = '$addedScope'
                BEGIN
                    SELECT RAISE(ABORT, 'forced connected assignment failure');
                END
                """.trimIndent()
            )
        }

        assertThrows(Exception::class.java) {
            processor.installConnectedAssignment(
                ScopeAssignmentDelta(
                    add = listOf(ScopeAssignment(addedScope, null)),
                    remove = listOf(removedScope),
                ),
                scopeSetVersion = 2,
                clientGeneration = 2,
                syncedTables = listOf(localTestTable),
            )
        }

        assertNotNull(db.queryOne("SELECT id FROM orders WHERE id = ?", arrayOf("w1")))
        db.readTransaction { connection ->
            assertNotNull(SynchroMeta.getScope(connection, removedScope))
            assertNull(SynchroMeta.getScope(connection, addedScope))
            assertEquals(1L, SynchroMeta.getInt64(connection, MetaKey.SCOPE_SET_VERSION))
            assertEquals(1L, SynchroMeta.getInt64(connection, MetaKey.CLIENT_GENERATION))
            assertEquals(mapOf(removedScope to "receipt"), SynchroMeta.getSeedReceipts(connection))
        }
    }

    @Test
    fun testApplyScopeDeleteWithoutRowRemovesOrphanedRecordAndLeavesQueueEmpty() {
        val (db, processor) = makeTestEnv()

        addScopeRow(db, "orders:user1", "w1")
        insertOrder(db, "w1", updatedAt = "2026-01-03T00:00:00.000Z")

        val change = makeChangeRecord(
            scope = "orders:user1",
            schema = localTestTable,
            op = Operation.DELETE,
            pk = buildJsonObject { put("id", "w1") },
            row = null,
            serverVersion = "2026-01-04T00:00:00.000Z"
        )

        processor.applyScopeChanges(
            changes = listOf(change),
            syncedTables = listOf(localTestTable),
            scopeCursors = mapOf("orders:user1" to "11"),
            checksums = null,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
        )

        val row = db.queryOne("SELECT id FROM orders WHERE id = ?", arrayOf("w1"))
        assertNull(row)
        assertEquals(0, pendingChangeCount(db))
    }

    @Test
    fun testApplyScopeDeleteRejectsRowWithoutDeletedAt() {
        val (db, processor) = makeTestEnv()

        addScopeRow(db, "orders:user1", "w1")
        insertOrder(db, "w1", updatedAt = "2026-01-03T00:00:00.000Z")

        val change = makeChangeRecord(
            scope = "orders:user1",
            schema = localTestTable,
            op = Operation.DELETE,
            pk = buildJsonObject { put("id", "w1") },
            row = buildJsonObject {
                put("id", "w1")
                put("ship_address", "123 Main St")
                put("updated_at", "2026-01-04T00:00:00.000000Z")
                put("deleted_at", JsonNull)
            },
            serverVersion = "2026-01-04T00:00:00.000Z"
        )

        try {
            processor.applyScopeChanges(
                changes = listOf(change),
                syncedTables = listOf(localTestTable),
                scopeCursors = mapOf("orders:user1" to "11"),
                checksums = null,
                schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            )
            fail("Expected invalid delete tombstone payload to be rejected")
        } catch (error: SynchroError.InvalidResponse) {
            assertTrue(error.message!!.contains("delete change"))
        }
    }

    @Test
    fun testFinalizeScopeRebuildRemovesOrphanedRecordAndLeavesQueueEmpty() {
        val (db, processor) = makeTestEnv()

        addScopeRow(db, "orders:user1", "w1", generation = 1)
        insertOrder(db, "w1", updatedAt = "2026-01-03T00:00:00.000Z")

        val attempt = LocalRebuildAttempt(
            scopeID = "orders:user1",
            rebuildID = java.util.UUID.randomUUID().toString(),
            clientGeneration = 1,
            schemaVersion = 1,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            generation = 2,
            cursor = null,
            pageLimit = 100,
        )
        processor.finalizeScopeRebuild(
            attempt = attempt,
            finalCursor = "scope_cursor_20",
            checksum = protocolEmptyScopeChecksum(attempt.scopeID),
            syncedTables = listOf(localTestTable)
        )

        val row = db.queryOne("SELECT id FROM orders WHERE id = ?", arrayOf("w1"))
        assertNull(row)
        assertEquals(0, pendingChangeCount(db))
    }

    @Test
    fun pullApplyAndMatchingBackoffResolveInOneTransaction() {
        val (database, processor) = makeTestEnv()
        database.writeTransaction { connection ->
            SynchroMeta.setInt64(connection, MetaKey.SCOPE_SET_VERSION, 1)
        }
        val requestJSON = Json.encodeToString(
            PullRequest.serializer(),
            PullRequest(
                clientID = "test-client",
                clientGeneration = 1,
                schema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
                scopeSetVersion = 1,
                scopes = emptyMap(),
                limit = 100,
            ),
        )
        installDurableBackoff(database, RetryOperation.PULLING, requestJSON)
        database.execute(
            """
            CREATE TRIGGER fail_pull_backoff_resolution
            BEFORE DELETE ON _synchro_backoff
            BEGIN
                SELECT RAISE(ABORT, 'forced backoff resolution failure');
            END
            """.trimIndent(),
        )

        assertThrows(android.database.sqlite.SQLiteException::class.java) {
            processor.applyScopeChangesResolvingBackoff(
                changes = emptyList(),
                syncedTables = listOf(localTestTable),
                scopeCursors = emptyMap(),
                checksums = emptyMap(),
                schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
                scopeUpdates = ScopeAssignmentDelta(emptyList(), emptyList()),
                scopeSetVersion = 2,
                rebuildScopes = emptySet(),
                requestJSON = requestJSON,
            )
        }
        assertEquals(1L, database.readTransaction { SynchroMeta.getInt64(it, MetaKey.SCOPE_SET_VERSION) })
        assertNotNull(DurableBackoffStore.load(database))

        database.execute("DROP TRIGGER fail_pull_backoff_resolution")
        processor.applyScopeChangesResolvingBackoff(
            changes = emptyList(),
            syncedTables = listOf(localTestTable),
            scopeCursors = emptyMap(),
            checksums = emptyMap(),
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            scopeUpdates = ScopeAssignmentDelta(emptyList(), emptyList()),
            scopeSetVersion = 2,
            rebuildScopes = emptySet(),
            requestJSON = requestJSON,
        )
        assertEquals(2L, database.readTransaction { SynchroMeta.getInt64(it, MetaKey.SCOPE_SET_VERSION) })
        assertNull(DurableBackoffStore.load(database))

        installDurableBackoff(database, RetryOperation.PULLING, "$requestJSON ")
        processor.applyScopeChangesResolvingBackoff(
            changes = emptyList(),
            syncedTables = listOf(localTestTable),
            scopeCursors = emptyMap(),
            checksums = emptyMap(),
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            scopeUpdates = ScopeAssignmentDelta(emptyList(), emptyList()),
            scopeSetVersion = 3,
            rebuildScopes = emptySet(),
            requestJSON = requestJSON,
        )
        assertEquals("$requestJSON ", DurableBackoffStore.load(database)?.workIdentity)
    }

    @Test
    fun rebuildAttemptStartResetsOnlyTargetProvenanceBeforeAnyRequest() {
        val (database, processor) = makeTestEnv()
        val targetScope = "orders:target"
        val otherScope = "orders:other"
        addScopeRow(database, targetScope, "orphan", generation = 3)
        addScopeRow(database, targetScope, "shared", generation = 3)
        addScopeRow(database, targetScope, "protected", generation = 3)
        addScopeRow(database, otherScope, "shared", generation = 8)
        insertOrder(database, "orphan", updatedAt = "2026-01-03T00:00:00.000000Z")
        insertOrder(database, "shared", updatedAt = "2026-01-03T00:00:00.000000Z")
        insertOrder(database, "protected", shipAddress = "local", updatedAt = "2026-01-03T00:00:00.000000Z")
        insertOrder(database, "local-only", updatedAt = "2026-01-03T00:00:00.000000Z")
        insertPendingChange(database, "protected", "captured")
        val pendingBefore = pendingChangeCount(database)

        val attempt = processor.beginScopeRebuild(
            targetScope,
            clientGeneration = 1,
            schemaVersion = 1,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            pageLimit = 100,
        )

        assertTrue(
            database.query("SELECT record_id FROM _synchro_scope_rows WHERE scope_id = ?", arrayOf(targetScope))
                .isEmpty(),
        )
        assertEquals(
            listOf("shared"),
            database.query("SELECT record_id FROM _synchro_scope_rows WHERE scope_id = ?", arrayOf(otherScope))
                .map { it.getValue("record_id") },
        )
        assertNull(database.queryOne("SELECT id FROM orders WHERE id = 'orphan'"))
        assertNotNull(database.queryOne("SELECT id FROM orders WHERE id = 'shared'"))
        assertNotNull(database.queryOne("SELECT id FROM orders WHERE id = 'protected'"))
        assertNotNull(database.queryOne("SELECT id FROM orders WHERE id = 'local-only'"))
        assertEquals(pendingBefore, pendingChangeCount(database))
        assertEquals(attempt, database.readTransaction { SynchroMeta.getRebuildAttempt(it, targetScope) })
        assertEquals(4L, attempt.generation)
        assertNull(database.readTransaction { SynchroMeta.getScope(it, targetScope) }?.cursor)
        assertFalse(database.readTransaction { SynchroMeta.isSyncLocked(it) })
    }

    @Test
    fun rebuildPageAndMatchingBackoffCommitTogetherAndRecoverAppliedReceipt() {
        val (database, processor) = makeTestEnv()
        val targetScope = "orders:rebuild"
        database.writeTransaction { SynchroMeta.upsertScope(it, targetScope, null, null) }
        val attempt = processor.beginScopeRebuild(
            targetScope,
            clientGeneration = 1,
            schemaVersion = 1,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            pageLimit = 100,
        )
        val request = RebuildRequest(
            clientID = "test-client",
            clientGeneration = 1,
            schema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
            scope = targetScope,
            rebuildID = attempt.rebuildID,
            cursor = null,
            limit = 100,
        )
        val requestJSON = rebuildRequestJSON(request)
        val response = RebuildResponse(
            scope = targetScope,
            records = emptyList(),
            cursor = "next-page",
            hasMore = true,
        )
        installDurableBackoff(database, RetryOperation.REBUILDING, requestJSON)
        database.execute(
            """
            CREATE TRIGGER fail_rebuild_backoff_resolution
            BEFORE DELETE ON _synchro_backoff
            BEGIN
                SELECT RAISE(ABORT, 'forced backoff resolution failure');
            END
            """.trimIndent(),
        )

        assertThrows(android.database.sqlite.SQLiteException::class.java) {
            processor.applyScopeRebuildPage(
                attempt,
                request,
                requestJSON,
                response,
                rebuildResponseJSON(response),
                listOf(localTestTable),
            )
        }
        assertNull(database.readTransaction { SynchroMeta.getRebuildAttempt(it, targetScope) }?.cursor)
        assertTrue(database.query("SELECT * FROM _synchro_rebuild_page_receipts").isEmpty())
        assertNotNull(DurableBackoffStore.load(database))
        database.execute("DROP TRIGGER fail_rebuild_backoff_resolution")

        val continued = processor.applyScopeRebuildPage(
            attempt,
            request,
            requestJSON,
            response,
            rebuildResponseJSON(response),
            listOf(localTestTable),
        )

        assertEquals("next-page", continued.cursor)
        assertNull(DurableBackoffStore.load(database))
        installDurableBackoff(database, RetryOperation.REBUILDING, requestJSON)

        assertTrue(processor.resolveAppliedRebuildBackoff(continued, request, requestJSON))
        assertNull(DurableBackoffStore.load(database))
    }

    @Test
    fun scopeRemovalDeletesAttemptReceiptsAndMatchingBackoffAtomically() {
        val (database, processor) = makeTestEnv()
        val targetScope = "orders:removed"
        database.writeTransaction { SynchroMeta.upsertScope(it, targetScope, null, null) }
        val attempt = processor.beginScopeRebuild(
            targetScope,
            clientGeneration = 1,
            schemaVersion = 1,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            pageLimit = 100,
        )
        val request = RebuildRequest(
            clientID = "test-client",
            clientGeneration = 1,
            schema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
            scope = targetScope,
            rebuildID = attempt.rebuildID,
            cursor = null,
            limit = 100,
        )
        val requestJSON = rebuildRequestJSON(request)
        database.writeTransaction {
            SynchroMeta.insertRebuildPageReceipt(
                it,
                targetScope,
                attempt.rebuildID,
                null,
                requestJSON,
                "{}",
                null,
                null,
            )
        }
        installDurableBackoff(database, RetryOperation.REBUILDING, requestJSON)
        database.execute(
            """
            CREATE TRIGGER fail_scope_backoff_resolution
            BEFORE DELETE ON _synchro_backoff
            BEGIN
                SELECT RAISE(ABORT, 'forced backoff resolution failure');
            END
            """.trimIndent(),
        )

        assertThrows(android.database.sqlite.SQLiteException::class.java) {
            processor.removeScope(targetScope, listOf(localTestTable))
        }
        assertNotNull(database.readTransaction { SynchroMeta.getScope(it, targetScope) })
        assertNotNull(database.readTransaction { SynchroMeta.getRebuildAttempt(it, targetScope) })
        assertEquals(1, database.query("SELECT * FROM _synchro_rebuild_page_receipts").size)
        assertNotNull(DurableBackoffStore.load(database))

        database.execute("DROP TRIGGER fail_scope_backoff_resolution")
        processor.removeScope(targetScope, listOf(localTestTable))

        assertNull(database.readTransaction { SynchroMeta.getScope(it, targetScope) })
        assertNull(database.readTransaction { SynchroMeta.getRebuildAttempt(it, targetScope) })
        assertTrue(database.query("SELECT * FROM _synchro_rebuild_page_receipts").isEmpty())
        assertNull(DurableBackoffStore.load(database))
    }

    @Test
    fun scopeRemovalPreservesRebuildBackoffForAnotherScope() {
        val (database, processor) = makeTestEnv()
        val removedScope = "orders:removed"
        val retainedScope = "orders:retained"
        database.writeTransaction {
            SynchroMeta.upsertScope(it, removedScope, null, null)
            SynchroMeta.upsertScope(it, retainedScope, null, null)
        }
        val requestJSON = rebuildRequestJSON(
            RebuildRequest(
                clientID = "test-client",
                clientGeneration = 1,
                schema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
                scope = retainedScope,
                rebuildID = java.util.UUID.randomUUID().toString(),
                cursor = null,
                limit = 100,
            ),
        )
        installDurableBackoff(database, RetryOperation.REBUILDING, requestJSON)

        processor.removeScope(removedScope, listOf(localTestTable))

        assertEquals(requestJSON, DurableBackoffStore.load(database)?.workIdentity)
    }

    @Test
    fun testFinalizeScopeRebuildKeepsRecordBackedByAnotherScope() {
        val (db, processor) = makeTestEnv()

        addScopeRow(db, "orders:user1", "w1", generation = 1)
        addScopeRow(db, "orders:shared", "w1", generation = 4)
        insertOrder(db, "w1", updatedAt = "2026-01-03T00:00:00.000Z")

        val attempt = LocalRebuildAttempt(
            scopeID = "orders:user1",
            rebuildID = java.util.UUID.randomUUID().toString(),
            clientGeneration = 1,
            schemaVersion = 1,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            generation = 2,
            cursor = null,
            pageLimit = 100,
        )
        processor.finalizeScopeRebuild(
            attempt = attempt,
            finalCursor = "scope_cursor_20",
            checksum = protocolEmptyScopeChecksum(attempt.scopeID),
            syncedTables = listOf(localTestTable)
        )

        val row = db.queryOne("SELECT id FROM orders WHERE id = ?", arrayOf("w1"))
        assertNotNull(row)
        assertEquals(0, pendingChangeCount(db))
    }

    @Test
    fun testRemoveScopeRemovesOrphanedRecordAndLeavesQueueEmpty() {
        val (db, processor) = makeTestEnv()

        addScopeRow(db, "orders:user1", "w1")
        insertOrder(db, "w1", updatedAt = "2026-01-03T00:00:00.000Z")

        processor.removeScope("orders:user1", listOf(localTestTable))

        val row = db.queryOne("SELECT id FROM orders WHERE id = ?", arrayOf("w1"))
        assertNull(row)
        assertEquals(0, pendingChangeCount(db))
    }

    @Test
    fun testProtectedUpsertPreservesEveryProtectedLifecycleProjection() {
        val protectedStates = listOf(
            "captured",
            "sealed",
            "blocked_by_predecessor",
            "legacy_blocked",
            "rejected_terminal",
        )

        protectedStates.forEach { state ->
            val (db, processor) = makeTestEnv()
            val recordID = "protected-$state"
            val scopeID = "orders:$state"
            addScopeRow(db, scopeID, recordID)
            insertOrder(db, recordID, shipAddress = "local-$state", updatedAt = "2026-01-03T00:00:00.000000Z")
            insertPendingChange(db, recordID, state)
            val change = makeChangeRecord(
                scope = scopeID,
                schema = localTestTable,
                op = Operation.UPSERT,
                pk = buildJsonObject { put("id", recordID) },
                row = orderRow(recordID, "server-$state"),
                serverVersion = "server-$state",
            )

            processor.applyScopeChanges(
                changes = listOf(change),
                syncedTables = listOf(localTestTable),
                scopeCursors = emptyMap(),
                checksums = null,
                schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            )

            assertEquals(
                "local-$state",
                db.queryOne("SELECT ship_address FROM orders WHERE id = ?", arrayOf(recordID))?.get("ship_address"),
            )
            assertEquals(
                "server-$state",
                db.readTransaction { conn -> SynchroMeta.getRowVersion(conn, "orders", recordID) },
            )
            assertEquals(
                change.rowChecksum!!.digest,
                db.queryOne(
                    "SELECT checksum FROM _synchro_scope_rows WHERE scope_id = ? AND table_name = ? AND record_id = ?",
                    arrayOf(scopeID, "orders", recordID),
                )?.get("checksum"),
            )
        }
    }

    @Test
    fun testRejectedTerminalDoesNotProtectAfterLaterAuthoritativeReplacement() {
        val (db, processor) = makeTestEnv()
        val recordID = "terminal-replaced"
        addScopeRow(db, "orders:user1", recordID)
        insertOrder(db, recordID, shipAddress = "local", updatedAt = "2026-01-03T00:00:00.000000Z")
        insertPendingChange(db, recordID, "rejected_terminal")
        insertPendingChange(db, recordID, "accepted")
        val change = makeChangeRecord(
            scope = "orders:user1",
            schema = localTestTable,
            op = Operation.UPSERT,
            pk = buildJsonObject { put("id", recordID) },
            row = orderRow(recordID, "server"),
            serverVersion = "server-replacement",
        )

        processor.applyScopeChanges(
            changes = listOf(change),
            syncedTables = listOf(localTestTable),
            scopeCursors = emptyMap(),
            checksums = null,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
        )

        assertEquals(
            "server",
            db.queryOne("SELECT ship_address FROM orders WHERE id = ?", arrayOf(recordID))?.get("ship_address"),
        )
    }

    @Test
    fun testRowlessDeleteProtectsPendingRowAndRemovesUnprotectedRow() {
        val (db, processor) = makeTestEnv()
        val scopeID = "orders:user1"
        addScopeRow(db, scopeID, "protected")
        addScopeRow(db, scopeID, "unprotected")
        insertOrder(db, "protected", shipAddress = "local", updatedAt = "2026-01-03T00:00:00.000000Z")
        insertOrder(db, "unprotected", updatedAt = "2026-01-03T00:00:00.000000Z")
        insertPendingChange(db, "protected", "captured")

        processor.applyScopeChanges(
            changes = listOf(
                makeChangeRecord(
                    scopeID,
                    localTestTable,
                    Operation.DELETE,
                    buildJsonObject { put("id", "protected") },
                    null,
                    "server-protected",
                ),
                makeChangeRecord(
                    scopeID,
                    localTestTable,
                    Operation.DELETE,
                    buildJsonObject { put("id", "unprotected") },
                    null,
                    "server-unprotected",
                ),
            ),
            syncedTables = listOf(localTestTable),
            scopeCursors = mapOf(scopeID to "11"),
            checksums = mapOf(scopeID to protocolEmptyScopeChecksum(scopeID)),
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
        )

        assertNotNull(db.queryOne("SELECT id FROM orders WHERE id = ?", arrayOf("protected")))
        assertNull(db.queryOne("SELECT id FROM orders WHERE id = ?", arrayOf("unprotected")))
        assertEquals(
            0,
            db.query("SELECT record_id FROM _synchro_scope_rows WHERE scope_id = ?", arrayOf(scopeID)).size,
        )
    }

    @Test
    fun testTombstoneProtectsPendingRowAndAppliesUnprotectedTombstone() {
        val (db, processor) = makeTestEnv()
        val scopeID = "orders:user1"
        addScopeRow(db, scopeID, "protected")
        addScopeRow(db, scopeID, "unprotected")
        insertOrder(db, "protected", shipAddress = "local", updatedAt = "2026-01-03T00:00:00.000000Z")
        insertOrder(db, "unprotected", updatedAt = "2026-01-03T00:00:00.000000Z")
        insertPendingChange(db, "protected", "captured")
        val deletedAt = "2026-01-04T00:00:00.000000Z"

        processor.applyScopeChanges(
            changes = listOf(
                makeChangeRecord(
                    scopeID,
                    localTestTable,
                    Operation.DELETE,
                    buildJsonObject { put("id", "protected") },
                    orderRow("protected", "server", deletedAt = deletedAt),
                    "server-protected",
                ),
                makeChangeRecord(
                    scopeID,
                    localTestTable,
                    Operation.DELETE,
                    buildJsonObject { put("id", "unprotected") },
                    orderRow("unprotected", "server", deletedAt = deletedAt),
                    "server-unprotected",
                ),
            ),
            syncedTables = listOf(localTestTable),
            scopeCursors = mapOf(scopeID to "11"),
            checksums = mapOf(scopeID to protocolEmptyScopeChecksum(scopeID)),
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
        )

        assertEquals(
            "local",
            db.queryOne("SELECT ship_address FROM orders WHERE id = ?", arrayOf("protected"))?.get("ship_address"),
        )
        assertNull(
            db.queryOne("SELECT deleted_at FROM orders WHERE id = ?", arrayOf("protected"))?.get("deleted_at"),
        )
        assertEquals(
            deletedAt,
            db.queryOne("SELECT deleted_at FROM orders WHERE id = ?", arrayOf("unprotected"))?.get("deleted_at"),
        )
        assertEquals(
            0,
            db.query("SELECT record_id FROM _synchro_scope_rows WHERE scope_id = ?", arrayOf(scopeID)).size,
        )
    }

    @Test
    fun testScopeRemovalProtectsPendingRowAndRemovesUnprotectedRow() {
        val (db, processor) = makeTestEnv()
        val scopeID = "orders:user1"
        addScopeRow(db, scopeID, "protected")
        addScopeRow(db, scopeID, "unprotected")
        insertOrder(db, "protected", shipAddress = "local", updatedAt = "2026-01-03T00:00:00.000000Z")
        insertOrder(db, "unprotected", updatedAt = "2026-01-03T00:00:00.000000Z")
        insertPendingChange(db, "protected", "captured")

        processor.removeScope(scopeID, listOf(localTestTable))

        assertNotNull(db.queryOne("SELECT id FROM orders WHERE id = ?", arrayOf("protected")))
        assertNull(db.queryOne("SELECT id FROM orders WHERE id = ?", arrayOf("unprotected")))
        assertNull(db.queryOne("SELECT scope_id FROM _synchro_scopes WHERE scope_id = ?", arrayOf(scopeID)))
    }

    @Test
    fun testRebuildPageAndPruningProtectPendingRowsAndRemoveUnprotectedRows() {
        val (db, processor) = makeTestEnv()
        val scopeID = "orders:user1"
        addScopeRow(db, scopeID, "stale-protected", generation = 1)
        addScopeRow(db, scopeID, "stale-unprotected", generation = 1)
        insertOrder(db, "stale-protected", shipAddress = "local", updatedAt = "2026-01-03T00:00:00.000000Z")
        insertOrder(db, "stale-unprotected", updatedAt = "2026-01-03T00:00:00.000000Z")
        insertOrder(db, "page-protected", shipAddress = "local", updatedAt = "2026-01-03T00:00:00.000000Z")
        insertPendingChange(db, "stale-protected", "captured")
        insertPendingChange(db, "page-protected", "captured")
        val attempt = processor.beginScopeRebuild(
            scopeID,
            clientGeneration = 1,
            schemaVersion = 1,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            pageLimit = 100,
        )
        val protectedPage = rebuildRecord("page-protected", "server")
        val unprotectedPage = rebuildRecord("page-unprotected", "server")
        val checksum = scopeChecksum(
            scopeID,
            listOf(
                "page-protected" to protectedPage.rowChecksum,
                "page-unprotected" to unprotectedPage.rowChecksum,
            ),
        )
        val request = RebuildRequest(
            clientID = "test-client",
            clientGeneration = attempt.clientGeneration,
            schema = SchemaRef(attempt.schemaVersion, attempt.schemaHash),
            scope = scopeID,
            rebuildID = attempt.rebuildID,
            cursor = attempt.cursor,
            limit = attempt.pageLimit,
        )
        val response = RebuildResponse(
            scope = scopeID,
            records = listOf(protectedPage, unprotectedPage),
            cursor = null,
            hasMore = false,
            finalScopeCursor = "scope-20",
            checksum = checksum,
        )

        val finalAttempt = processor.applyScopeRebuildPage(
            attempt = attempt,
            request = request,
            requestJSON = rebuildRequestJSON(request),
            response = response,
            responseJSON = rebuildResponseJSON(response),
            syncedTables = listOf(localTestTable),
        )

        assertEquals(
            "local",
            db.queryOne("SELECT ship_address FROM orders WHERE id = ?", arrayOf("page-protected"))?.get("ship_address"),
        )
        assertEquals(
            "server",
            db.queryOne("SELECT ship_address FROM orders WHERE id = ?", arrayOf("page-unprotected"))?.get("ship_address"),
        )

        processor.finalizeScopeRebuild(
            attempt = finalAttempt,
            finalCursor = "scope-20",
            checksum = checksum,
            syncedTables = listOf(localTestTable),
        )

        assertNotNull(db.queryOne("SELECT id FROM orders WHERE id = ?", arrayOf("stale-protected")))
        assertNull(db.queryOne("SELECT id FROM orders WHERE id = ?", arrayOf("stale-unprotected")))
    }

    @Test
    fun testIntermediateRebuildReceiptSurvivesRestartAndSkipsExactReplay() {
        val (db, processor) = makeTestEnv()
        val context = ApplicationProvider.getApplicationContext<Context>()
        val scopeID = "orders:user1"
        db.writeTransaction { connection ->
            SynchroMeta.upsertScope(connection, scopeID, cursor = null, checksum = null)
            connection.execSQL("CREATE TABLE rebuild_apply_events (count INTEGER NOT NULL)")
            connection.execSQL("INSERT INTO rebuild_apply_events (count) VALUES (0)")
            connection.execSQL(
                """
                CREATE TRIGGER count_rebuild_apply
                AFTER INSERT ON orders
                BEGIN
                    UPDATE rebuild_apply_events SET count = count + 1;
                END
                """.trimIndent(),
            )
            connection.execSQL(
                """
                CREATE TRIGGER count_rebuild_reapply
                AFTER UPDATE ON orders
                BEGIN
                    UPDATE rebuild_apply_events SET count = count + 1;
                END
                """.trimIndent(),
            )
        }
        val attempt = processor.beginScopeRebuild(
            scopeID,
            clientGeneration = 1,
            schemaVersion = 1,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            pageLimit = 100,
        )
        val request = RebuildRequest(
            clientID = "test-client",
            clientGeneration = attempt.clientGeneration,
            schema = SchemaRef(attempt.schemaVersion, attempt.schemaHash),
            scope = scopeID,
            rebuildID = attempt.rebuildID,
            cursor = attempt.cursor,
            limit = attempt.pageLimit,
        )
        val response = RebuildResponse(
            scope = scopeID,
            records = listOf(rebuildRecord("receipt-row", "server")),
            cursor = "opaque-next-token",
            hasMore = true,
            finalScopeCursor = null,
            checksum = null,
        )
        val requestJSON = rebuildRequestJSON(request)
        val responseJSON = rebuildResponseJSON(response)
        val continuedAttempt = processor.applyScopeRebuildPage(
            attempt,
            request,
            requestJSON,
            response,
            responseJSON,
            listOf(localTestTable),
        )
        assertEquals("opaque-next-token", continuedAttempt.cursor)
        assertEquals(1L, db.queryOne("SELECT count FROM rebuild_apply_events")?.get("count"))

        db.close()
        val recoveredDatabase = databases.open(context, db.path)
        val recoveredProcessor = PullProcessor(recoveredDatabase)
        val recoveredAttempt = recoveredProcessor.beginScopeRebuild(
            scopeID,
            clientGeneration = 1,
            schemaVersion = 1,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            pageLimit = 100,
        )
        assertEquals(continuedAttempt, recoveredAttempt)

        val replayedAttempt = recoveredProcessor.applyScopeRebuildPage(
            attempt,
            request,
            requestJSON,
            response,
            responseJSON,
            listOf(localTestTable),
        )
        assertEquals(continuedAttempt, replayedAttempt)
        assertEquals(1L, recoveredDatabase.queryOne("SELECT count FROM rebuild_apply_events")?.get("count"))
        assertEquals(1, recoveredDatabase.query("SELECT * FROM _synchro_rebuild_page_receipts").size)

        assertThrows(SynchroError.InvalidResponse::class.java) {
            recoveredProcessor.applyScopeRebuildPage(
                attempt,
                request,
                requestJSON,
                response,
                " $responseJSON",
                listOf(localTestTable),
            )
        }

        val restartedAttempt = recoveredProcessor.restartScopeRebuild(
            scopeID,
            clientGeneration = 1,
            schemaVersion = 1,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            pageLimit = 100,
        )
        assertNotEquals(attempt.rebuildID, restartedAttempt.rebuildID)
        assertNull(restartedAttempt.cursor)
        assertTrue(
            recoveredDatabase.query(
                "SELECT * FROM _synchro_rebuild_page_receipts WHERE rebuild_id = ?",
                arrayOf(attempt.rebuildID),
            ).isEmpty(),
        )
    }

    @Test
    fun testTerminalScopeChecksumUsesStoredDigestForProtectedRow() {
        val (db, processor) = makeTestEnv()
        val scopeID = "orders:user1"
        val recordID = "protected"
        addScopeRow(db, scopeID, recordID)
        insertOrder(db, recordID, shipAddress = "local", updatedAt = "2026-01-03T00:00:00.000000Z")
        insertPendingChange(db, recordID, "captured")
        val change = makeChangeRecord(
            scope = scopeID,
            schema = localTestTable,
            op = Operation.UPSERT,
            pk = buildJsonObject { put("id", recordID) },
            row = orderRow(recordID, "server"),
            serverVersion = "server-version",
        )
        val checksum = scopeChecksum(scopeID, listOf(recordID to change.rowChecksum!!))

        processor.applyScopeChanges(
            changes = listOf(change),
            syncedTables = listOf(localTestTable),
            scopeCursors = mapOf(scopeID to "20"),
            checksums = mapOf(scopeID to checksum),
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
        )

        assertEquals(
            "local",
            db.queryOne("SELECT ship_address FROM orders WHERE id = ?", arrayOf(recordID))?.get("ship_address"),
        )
        val scope = db.queryOne("SELECT cursor, checksum FROM _synchro_scopes WHERE scope_id = ?", arrayOf(scopeID))
        assertEquals("20", scope?.get("cursor"))
        assertTrue((scope?.get("checksum") as String).contains(checksum.digest))
    }

    @Test
    fun testPreConnectSeedVerificationInvalidatesOnlyCorruptReceiptScope() {
        val (db, processor) = makeTestEnv()
        val corruptScope = "orders:corrupt"
        val healthyScope = "orders:healthy"
        installReceiptBackedScope(db, corruptScope, "corrupt-row", "Corrupt")
        val healthyChecksum = installReceiptBackedScope(db, healthyScope, "healthy-row", "Healthy")

        db.writeSyncLockedTransaction { conn ->
            SynchroMeta.deleteScopeRow(conn, corruptScope, "orders", "corrupt-row")
            conn.execSQL("DELETE FROM orders WHERE id = ?", arrayOf("corrupt-row"))
        }

        assertEquals(setOf(corruptScope), processor.reconcileSeedReceiptsBeforeConnect())

        db.readTransaction { conn ->
            assertEquals(mapOf(healthyScope to "receipt-$healthyScope"), SynchroMeta.getSeedReceipts(conn))
            val corrupt = SynchroMeta.getScope(conn, corruptScope)
            assertNull(corrupt?.cursor)
            assertNull(corrupt?.checksum)
            assertEquals("", corrupt?.localChecksum)
            val healthy = SynchroMeta.getScope(conn, healthyScope)
            assertNull(healthy?.cursor)
            assertEquals(healthyChecksum, healthy?.checksum)
            assertEquals(healthyChecksum, healthy?.localChecksum)
        }
        assertNotNull(db.queryOne("SELECT id FROM orders WHERE id = ?", arrayOf("healthy-row")))
    }

    @Test
    fun testPreConnectSeedVerificationPreservesProtectedIntent() {
        val (db, processor) = makeTestEnv()
        val scopeID = "orders:protected"
        installReceiptBackedScope(db, scopeID, "protected-row", "Seed value")
        db.execute(
            "UPDATE orders SET ship_address = ? WHERE id = ?",
            arrayOf("Local intent", "protected-row"),
        )

        assertTrue(processor.reconcileSeedReceiptsBeforeConnect().isEmpty())
        assertEquals(1, pendingChangeCount(db))
        assertEquals(
            "Local intent",
            db.queryOne("SELECT ship_address FROM orders WHERE id = ?", arrayOf("protected-row"))?.get("ship_address"),
        )
        db.readTransaction { conn ->
            assertEquals(mapOf(scopeID to "receipt-$scopeID"), SynchroMeta.getSeedReceipts(conn))
            assertNotNull(SynchroMeta.getScope(conn, scopeID)?.checksum)
        }
    }

    @Test
    fun testTypedPullApplyStoresBytesAsBlobAndRejectsMalformedBase64url() {
        val localTypedTable = typedTable.localSchema
        val (db, processor) = makeTestEnv(typedTable)
        val row = buildJsonObject {
            put("id", "typed-1")
            put("payload", "AP8")
            put("large_count", "9223372036854775807")
            put("enabled", true)
            put("updated_at", "2026-01-04T00:00:00.000000Z")
            put("deleted_at", JsonNull)
        }

        processor.applyScopeChanges(
            changes = listOf(
                makeChangeRecord(
                    scope = "typed:user1",
                    schema = localTypedTable,
                    op = Operation.UPSERT,
                    pk = buildJsonObject { put("id", "typed-1") },
                    row = row,
                    serverVersion = "typed-version",
                ),
            ),
            syncedTables = listOf(localTypedTable),
            scopeCursors = emptyMap(),
            checksums = null,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
        )

        val values = db.readTransaction { conn ->
            conn.rawQuery(
                "SELECT payload, large_count, enabled FROM typed_orders WHERE id = ?",
                arrayOf("typed-1"),
            ).use { cursor ->
                assertTrue(cursor.moveToFirst())
                Triple(cursor.getBlob(0), cursor.getLong(1), cursor.getLong(2))
            }
        }
        assertArrayEquals(byteArrayOf(0, -1), values.first)
        assertEquals(Long.MAX_VALUE, values.second)
        assertEquals(1L, values.third)

        val malformed = buildJsonObject {
            put("id", "typed-invalid")
            put("payload", "AQ=")
            put("large_count", "1")
            put("enabled", false)
            put("updated_at", "2026-01-04T00:00:00.000000Z")
            put("deleted_at", JsonNull)
        }
        val invalidChange = ChangeRecord(
            scope = "typed:user1",
            table = localTypedTable.tableID,
            op = Operation.UPSERT,
            pk = buildJsonObject { put("id", "typed-invalid") },
            row = malformed,
            rowChecksum = ChecksumObject("sha256", 1, "hex", "0".repeat(64)),
            serverVersion = "typed-invalid",
        )

        assertThrows(SynchroError.InvalidResponse::class.java) {
            processor.applyScopeChanges(
                changes = listOf(invalidChange),
                syncedTables = listOf(localTypedTable),
                scopeCursors = emptyMap(),
                checksums = null,
                schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
            )
        }
        assertNull(db.queryOne("SELECT id FROM typed_orders WHERE id = ?", arrayOf("typed-invalid")))
    }

}
