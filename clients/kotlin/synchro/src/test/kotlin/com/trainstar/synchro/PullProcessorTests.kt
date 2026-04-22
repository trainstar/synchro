package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
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

    private fun makeTestEnv(): Pair<SynchroDatabase, PullProcessor> {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val db = databases.create(context)
        val manager = SchemaManager(db)
        val schema = SchemaResponse(
            schemaVersion = 1, schemaHash = "test",
            serverTime = "2026-01-01T12:00:00.000Z", tables = listOf(testTable)
        )
        manager.createSyncedTables(schema)
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
        checksum: Int = 7,
        generation: Long = 0
    ) {
        db.writeTransaction { conn ->
            SynchroMeta.upsertScope(conn, scopeId, "10", null, generation)
            SynchroMeta.upsertScopeRow(conn, scopeId, "orders", recordId, checksum, generation)
        }
    }

    private fun addBucketMember(db: SynchroDatabase, bucketId: String, recordId: String) {
        db.writeTransaction { conn ->
            SynchroMeta.setBucketMember(conn, bucketId, "orders", recordId, 1)
        }
    }

    private fun pendingChangeCount(db: SynchroDatabase): Int {
        return ChangeTracker(db).pendingChangeCount()
    }

    @After
    fun tearDown() {
        databases.closeAll()
    }

    @Test
    fun testApplyChangesInsertsRecords() {
        val (db, processor) = makeTestEnv()

        val record = Record(
            id = "w1",
            tableName = "orders",
            data = mapOf(
                "id" to AnyCodable("w1"),
                "ship_address" to AnyCodable("123 Main St"),
                "updated_at" to AnyCodable("2026-01-01T12:00:00.000Z"),
            ),
            updatedAt = "2026-01-01T12:00:00.000Z"
        )

        processor.applyChanges(listOf(record), listOf(localTestTable))

        val row = db.queryOne("SELECT * FROM orders WHERE id = ?", arrayOf("w1"))
        assertNotNull(row)
        assertEquals("123 Main St", row?.get("ship_address"))
    }

    @Test
    fun testApplyChangesUpdatesExisting() {
        val (db, processor) = makeTestEnv()

        // Insert with sync lock to avoid trigger
        db.writeTransaction { conn ->
            SynchroMeta.setSyncLock(conn, true)
        }
        db.execute(
            "INSERT INTO orders (id, ship_address, updated_at) VALUES (?, ?, ?)",
            arrayOf("w1", "Old Address", "2026-01-01T10:00:00.000Z")
        )
        db.writeTransaction { conn ->
            SynchroMeta.setSyncLock(conn, false)
        }

        val record = Record(
            id = "w1",
            tableName = "orders",
            data = mapOf(
                "id" to AnyCodable("w1"),
                "ship_address" to AnyCodable("New Address"),
                "updated_at" to AnyCodable("2026-01-01T12:00:00.000Z"),
            ),
            updatedAt = "2026-01-01T12:00:00.000Z"
        )

        processor.applyChanges(listOf(record), listOf(localTestTable))

        val row = db.queryOne("SELECT ship_address FROM orders WHERE id = ?", arrayOf("w1"))
        assertEquals("New Address", row?.get("ship_address"))
    }

    @Test
    fun testApplyDeletesSetsDeletedAt() {
        val (db, processor) = makeTestEnv()

        db.writeTransaction { conn -> SynchroMeta.setSyncLock(conn, true) }
        db.execute(
            "INSERT INTO orders (id, ship_address, updated_at) VALUES (?, ?, ?)",
            arrayOf("w1", "123 Main St", "2026-01-01T10:00:00.000Z")
        )
        db.writeTransaction { conn -> SynchroMeta.setSyncLock(conn, false) }

        processor.applyDeletes(
            listOf(DeleteEntry(id = "w1", tableName = "orders")),
            listOf(localTestTable)
        )

        val row = db.queryOne("SELECT deleted_at FROM orders WHERE id = ?", arrayOf("w1"))
        assertNotNull(row)
        assertNotNull(row?.get("deleted_at"))
    }

    @Test
    fun testApplyChangesSkipsRYOW() {
        val (db, processor) = makeTestEnv()

        // Insert with a newer timestamp locally
        db.writeTransaction { conn -> SynchroMeta.setSyncLock(conn, true) }
        db.execute(
            "INSERT INTO orders (id, ship_address, updated_at) VALUES (?, ?, ?)",
            arrayOf("w1", "Local Address", "2026-01-01T15:00:00.000Z")
        )
        db.writeTransaction { conn -> SynchroMeta.setSyncLock(conn, false) }

        // Try to apply an older server record
        val record = Record(
            id = "w1",
            tableName = "orders",
            data = mapOf(
                "id" to AnyCodable("w1"),
                "ship_address" to AnyCodable("Server Address"),
                "updated_at" to AnyCodable("2026-01-01T12:00:00.000Z"),
            ),
            updatedAt = "2026-01-01T12:00:00.000Z"
        )

        processor.applyChanges(listOf(record), listOf(localTestTable))

        // Should keep local name (RYOW dedup)
        val row = db.queryOne("SELECT ship_address FROM orders WHERE id = ?", arrayOf("w1"))
        assertEquals("Local Address", row?.get("ship_address"))
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
    fun testApplyChangesAcceptsNewerServerVersion() {
        val (db, processor) = makeTestEnv()

        // Insert local record at T1
        db.writeTransaction { conn -> SynchroMeta.setSyncLock(conn, true) }
        db.execute(
            "INSERT INTO orders (id, ship_address, updated_at) VALUES (?, ?, ?)",
            arrayOf("w1", "Local Address", "2026-01-01T10:00:00.000Z")
        )
        db.writeTransaction { conn -> SynchroMeta.setSyncLock(conn, false) }

        // Apply server record at T2 > T1 — should overwrite local
        val record = Record(
            id = "w1",
            tableName = "orders",
            data = mapOf(
                "id" to AnyCodable("w1"),
                "ship_address" to AnyCodable("Server Address"),
                "updated_at" to AnyCodable("2026-01-01T15:00:00.000Z"),
            ),
            updatedAt = "2026-01-01T15:00:00.000Z"
        )

        processor.applyChanges(listOf(record), listOf(localTestTable))

        val row = db.queryOne("SELECT ship_address, updated_at FROM orders WHERE id = ?", arrayOf("w1"))
        assertEquals("Server Address", row?.get("ship_address"))
        assertEquals("2026-01-01T15:00:00.000Z", row?.get("updated_at"))
    }

    @Test
    fun testSyncLockPreventsTriggering() {
        val (db, processor) = makeTestEnv()
        val tracker = ChangeTracker(db)

        val record = Record(
            id = "w1",
            tableName = "orders",
            data = mapOf(
                "id" to AnyCodable("w1"),
                "ship_address" to AnyCodable("Pull Applied Address"),
                "updated_at" to AnyCodable("2026-01-01T12:00:00.000Z"),
            ),
            updatedAt = "2026-01-01T12:00:00.000Z"
        )

        processor.applyChanges(listOf(record), listOf(localTestTable))

        // Pending queue should be empty (sync_lock was on during apply)
        val pending = tracker.pendingChanges()
        assertEquals(0, pending.size)
    }

    @Test
    fun testApplyScopeDeletePreservesCanonicalDeletedAt() {
        val (db, processor) = makeTestEnv()

        db.writeTransaction { conn ->
            SynchroMeta.setSyncLock(conn, true)
            SynchroMeta.upsertScope(conn, "orders:user1", "10", null)
            SynchroMeta.upsertScopeRow(conn, "orders:user1", "orders", "w1", 7, 0)
        }
        db.execute(
            "INSERT INTO orders (id, ship_address, updated_at) VALUES (?, ?, ?)",
            arrayOf("w1", "123 Main St", "2026-01-01T10:00:00.000Z")
        )
        db.writeTransaction { conn ->
            SynchroMeta.setSyncLock(conn, false)
        }

        val change = ChangeRecord(
            scope = "orders:user1",
            table = "orders",
            op = Operation.DELETE,
            pk = buildJsonObject { put("id", "w1") },
            row = buildJsonObject {
                put("id", "w1")
                put("ship_address", "123 Main St")
                put("updated_at", "2026-01-04T00:00:00.000Z")
                put("deleted_at", "2026-01-04T00:00:00.000Z")
            },
            serverVersion = "2026-01-04T00:00:00.000Z"
        )

        processor.applyScopeChanges(
            changes = listOf(change),
            syncedTables = listOf(localTestTable),
            scopeCursors = mapOf("orders:user1" to "11"),
            checksums = null
        )

        val row = db.queryOne("SELECT deleted_at FROM orders WHERE id = ?", arrayOf("w1"))
        assertEquals("2026-01-04T00:00:00.000Z", row?.get("deleted_at"))
    }

    @Test
    fun testApplyScopeDeleteUsesDeletedAtAsEffectiveVersion() {
        val (db, processor) = makeTestEnv()

        addScopeRow(db, "orders:user1", "w1")
        insertOrder(db, "w1", updatedAt = "2026-01-03T00:00:00.000Z")

        val change = ChangeRecord(
            scope = "orders:user1",
            table = "orders",
            op = Operation.DELETE,
            pk = buildJsonObject { put("id", "w1") },
            row = buildJsonObject {
                put("id", "w1")
                put("ship_address", "123 Main St")
                put("updated_at", "2026-01-03T00:00:00.000Z")
                put("deleted_at", "2026-01-04T00:00:00.000Z")
            },
            serverVersion = "2026-01-04T00:00:00.000Z"
        )

        processor.applyScopeChanges(
            changes = listOf(change),
            syncedTables = listOf(localTestTable),
            scopeCursors = mapOf("orders:user1" to "11"),
            checksums = null
        )

        val row = db.queryOne("SELECT updated_at, deleted_at FROM orders WHERE id = ?", arrayOf("w1"))
        assertEquals("2026-01-03T00:00:00.000Z", row?.get("updated_at"))
        assertEquals("2026-01-04T00:00:00.000Z", row?.get("deleted_at"))
    }

    @Test
    fun testApplyScopeDeleteWithoutRowRemovesOrphanedRecordAndLeavesQueueEmpty() {
        val (db, processor) = makeTestEnv()

        addScopeRow(db, "orders:user1", "w1")
        insertOrder(db, "w1", updatedAt = "2026-01-03T00:00:00.000Z")

        val change = ChangeRecord(
            scope = "orders:user1",
            table = "orders",
            op = Operation.DELETE,
            pk = buildJsonObject { put("id", "w1") },
            row = null,
            serverVersion = "2026-01-04T00:00:00.000Z"
        )

        processor.applyScopeChanges(
            changes = listOf(change),
            syncedTables = listOf(localTestTable),
            scopeCursors = mapOf("orders:user1" to "11"),
            checksums = null
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

        val change = ChangeRecord(
            scope = "orders:user1",
            table = "orders",
            op = Operation.DELETE,
            pk = buildJsonObject { put("id", "w1") },
            row = buildJsonObject {
                put("id", "w1")
                put("ship_address", "123 Main St")
                put("updated_at", "2026-01-04T00:00:00.000Z")
                put("deleted_at", JsonNull)
            },
            serverVersion = "2026-01-04T00:00:00.000Z"
        )

        try {
            processor.applyScopeChanges(
                changes = listOf(change),
                syncedTables = listOf(localTestTable),
                scopeCursors = mapOf("orders:user1" to "11"),
                checksums = null
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

        processor.finalizeScopeRebuild(
            scopeId = "orders:user1",
            generation = 2,
            finalCursor = "scope_cursor_20",
            checksum = "0",
            syncedTables = listOf(localTestTable)
        )

        val row = db.queryOne("SELECT id FROM orders WHERE id = ?", arrayOf("w1"))
        assertNull(row)
        assertEquals(0, pendingChangeCount(db))
    }

    @Test
    fun testFinalizeScopeRebuildKeepsRecordBackedByAnotherScope() {
        val (db, processor) = makeTestEnv()

        addScopeRow(db, "orders:user1", "w1", generation = 1)
        addScopeRow(db, "orders:shared", "w1", generation = 4)
        insertOrder(db, "w1", updatedAt = "2026-01-03T00:00:00.000Z")

        processor.finalizeScopeRebuild(
            scopeId = "orders:user1",
            generation = 2,
            finalCursor = "scope_cursor_20",
            checksum = "0",
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
    fun testRemoveScopeKeepsRecordBackedByBucketMembership() {
        val (db, processor) = makeTestEnv()

        addScopeRow(db, "orders:user1", "w1")
        addBucketMember(db, "bucket-1", "w1")
        insertOrder(db, "w1", updatedAt = "2026-01-03T00:00:00.000Z")

        processor.removeScope("orders:user1", listOf(localTestTable))

        val row = db.queryOne("SELECT id FROM orders WHERE id = ?", arrayOf("w1"))
        assertNotNull(row)
        assertEquals(0, pendingChangeCount(db))
    }

    @Test
    fun testDeleteBucketOrphanedRecordsRemovesUnreferencedRowsAndLeavesQueueEmpty() {
        val (db, processor) = makeTestEnv()

        addBucketMember(db, "bucket-1", "w1")
        insertOrder(db, "w1", updatedAt = "2026-01-03T00:00:00.000Z")

        processor.deleteBucketOrphanedRecords("bucket-1", listOf(localTestTable))

        val row = db.queryOne("SELECT id FROM orders WHERE id = ?", arrayOf("w1"))
        assertNull(row)
        assertEquals(0, pendingChangeCount(db))
    }

    @Test
    fun testTrackBucketMembershipRejectsMissingChecksum() {
        val (_, processor) = makeTestEnv()

        val record = Record(
            id = "w1",
            tableName = "orders",
            data = mapOf(
                "id" to AnyCodable("w1"),
                "user_id" to AnyCodable("user-1"),
                "updated_at" to AnyCodable("2026-01-03T00:00:00.000Z"),
            ),
            updatedAt = "2026-01-03T00:00:00.000Z",
            deletedAt = null,
            bucketId = "bucket-1",
            checksum = null,
        )

        val error = assertThrows(SynchroError.InvalidResponse::class.java) {
            processor.trackBucketMembership(listOf(record))
        }
        assertTrue(error.details.contains("missing record checksum"))
    }
}
