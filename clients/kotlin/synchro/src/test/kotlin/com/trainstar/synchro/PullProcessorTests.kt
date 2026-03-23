package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
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
            SynchroMeta.upsertScopeRow(conn, "orders:user1", "orders", "w1", 0)
        }
        db.execute(
            "INSERT INTO orders (id, ship_address, updated_at) VALUES (?, ?, ?)",
            arrayOf("w1", "123 Main St", "2026-01-01T10:00:00.000Z")
        )
        db.writeTransaction { conn ->
            SynchroMeta.setSyncLock(conn, false)
        }

        val change = VNextChangeRecord(
            scope = "orders:user1",
            table = "orders",
            op = VNextOperation.DELETE,
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

        db.writeTransaction { conn ->
            SynchroMeta.setSyncLock(conn, true)
            SynchroMeta.upsertScope(conn, "orders:user1", "10", null)
            SynchroMeta.upsertScopeRow(conn, "orders:user1", "orders", "w1", 0)
        }
        db.execute(
            "INSERT INTO orders (id, ship_address, updated_at) VALUES (?, ?, ?)",
            arrayOf("w1", "123 Main St", "2026-01-03T00:00:00.000Z")
        )
        db.writeTransaction { conn ->
            SynchroMeta.setSyncLock(conn, false)
        }

        val change = VNextChangeRecord(
            scope = "orders:user1",
            table = "orders",
            op = VNextOperation.DELETE,
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
}
