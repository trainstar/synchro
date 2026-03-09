package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
import java.util.UUID

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28])
class PushProcessorTests {

    private val testTable = SchemaTable(
        tableName = "orders",
        pushPolicy = "owner_only",
        updatedAtColumn = "updated_at",
        deletedAtColumn = "deleted_at",
        primaryKey = listOf("id"),
        columns = listOf(
            SchemaColumn(name = "id", dbType = "uuid", logicalType = "string", nullable = false, isPrimaryKey = true),
            SchemaColumn(name = "ship_address", dbType = "text", logicalType = "string", nullable = true, isPrimaryKey = false),
            SchemaColumn(name = "user_id", dbType = "uuid", logicalType = "string", nullable = false, isPrimaryKey = false),
            SchemaColumn(name = "updated_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
            SchemaColumn(name = "deleted_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = true, isPrimaryKey = false),
        )
    )

    private val customTable = SchemaTable(
        tableName = "custom_items",
        pushPolicy = "owner_only",
        updatedAtColumn = "modified_at",
        deletedAtColumn = "removed_at",
        primaryKey = listOf("item_id"),
        columns = listOf(
            SchemaColumn(name = "item_id", dbType = "uuid", logicalType = "string", nullable = false, isPrimaryKey = true),
            SchemaColumn(name = "title", dbType = "text", logicalType = "string", nullable = true, isPrimaryKey = false),
            SchemaColumn(name = "modified_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
            SchemaColumn(name = "removed_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = true, isPrimaryKey = false),
        )
    )

    private fun makeTestEnv(table: SchemaTable? = null): Triple<SynchroDatabase, ChangeTracker, PushProcessor> {
        val t = table ?: testTable
        val context = ApplicationProvider.getApplicationContext<Context>()
        val dbName = "synchro_test_${UUID.randomUUID()}.sqlite"
        val db = SynchroDatabase(context, dbName)
        val manager = SchemaManager(db)
        val schema = SchemaResponse(
            schemaVersion = 1, schemaHash = "test",
            serverTime = "2026-01-01T12:00:00.000Z", tables = listOf(t)
        )
        manager.createSyncedTables(schema)
        val tracker = ChangeTracker(db)
        val processor = PushProcessor(db, tracker)
        return Triple(db, tracker, processor)
    }

    // MARK: - Hydration Tests

    @Test
    fun testHydratePendingForPush() {
        val (db, tracker, _) = makeTestEnv()

        db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            arrayOf("w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z")
        )

        val pending = tracker.pendingChanges()
        assertEquals(1, pending.size)

        val pushRecords = tracker.hydratePendingForPush(pending, listOf(testTable))
        assertEquals(1, pushRecords.size)
        assertEquals("w1", pushRecords[0].id)
        assertEquals("create", pushRecords[0].operation)
        assertNotNull(pushRecords[0].data)
        assertEquals(AnyCodable("123 Main St"), pushRecords[0].data?.get("ship_address"))
    }

    @Test
    fun testHydrateDeleteHasNilData() {
        val (db, tracker, _) = makeTestEnv()

        db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            arrayOf("w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z")
        )
        tracker.clearAll()

        db.execute("DELETE FROM orders WHERE id = ?", arrayOf("w1"))

        val pending = tracker.pendingChanges()
        assertEquals(1, pending.size)
        assertEquals("delete", pending[0].operation)

        val pushRecords = tracker.hydratePendingForPush(pending, listOf(testTable))
        assertEquals(1, pushRecords.size)
        assertNull(pushRecords[0].data)
    }

    @Test
    fun testRemovePending() {
        val (db, tracker, _) = makeTestEnv()

        db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            arrayOf("w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z")
        )

        val pending = tracker.pendingChanges()
        assertEquals(1, pending.size)

        tracker.removePending(pending)
        assertFalse(tracker.hasPendingChanges())
    }

    @Test
    fun testHydrateWithCustomPrimaryKey() {
        val (db, tracker, _) = makeTestEnv(customTable)

        db.execute(
            "INSERT INTO custom_items (item_id, title, modified_at) VALUES (?, ?, ?)",
            arrayOf("ci1", "My Item", "2026-01-01T10:00:00.000Z")
        )

        val pending = tracker.pendingChanges()
        assertEquals(1, pending.size)
        assertEquals("ci1", pending[0].recordID)

        val pushRecords = tracker.hydratePendingForPush(pending, listOf(customTable))
        assertEquals(1, pushRecords.size)
        assertEquals("ci1", pushRecords[0].id)
        assertEquals(AnyCodable("My Item"), pushRecords[0].data?.get("title"))
        assertEquals(AnyCodable("ci1"), pushRecords[0].data?.get("item_id"))
    }

    @Test
    fun testHydrateMultiplePendingChanges() {
        val (db, tracker, _) = makeTestEnv()

        db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            arrayOf("w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z")
        )
        db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            arrayOf("w2", "456 Oak Ave", "u1", "2026-01-01T10:00:00.000Z")
        )

        val pending = tracker.pendingChanges()
        assertEquals(2, pending.size)

        val pushRecords = tracker.hydratePendingForPush(pending, listOf(testTable))
        assertEquals(2, pushRecords.size)

        val ids = pushRecords.map { it.id }.toSet()
        assertTrue(ids.contains("w1"))
        assertTrue(ids.contains("w2"))
    }

    @Test
    fun testHydrateLimitsPendingCount() {
        val (db, tracker, _) = makeTestEnv()

        for (i in 1..5) {
            db.execute(
                "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arrayOf("w$i", "$i Main St", "u1", "2026-01-01T10:00:00.000Z")
            )
        }

        val pending = tracker.pendingChanges(limit = 3)
        assertEquals(3, pending.size)
    }

    // MARK: - applyAccepted Tests

    @Test
    fun testApplyAcceptedRemovesPendingAndAppliesRYOW() {
        val (db, tracker, processor) = makeTestEnv()

        db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            arrayOf("w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z")
        )

        assertTrue(tracker.hasPendingChanges())

        val accepted = listOf(
            PushResult(
                id = "w1",
                tableName = "orders",
                operation = "create",
                status = PushStatus.APPLIED,
                serverUpdatedAt = "2026-01-01T12:00:00.000Z"
            )
        )

        processor.applyAccepted(accepted, listOf(testTable))

        // Pending should be drained
        assertFalse(tracker.hasPendingChanges())

        // RYOW: local updated_at should match server timestamp
        val row = db.queryOne("SELECT updated_at FROM orders WHERE id = ?", arrayOf("w1"))
        assertEquals("2026-01-01T12:00:00.000Z", row?.get("updated_at"))
    }

    @Test
    fun testApplyAcceptedRYOWWithCustomColumns() {
        val (db, tracker, processor) = makeTestEnv(customTable)

        db.execute(
            "INSERT INTO custom_items (item_id, title, modified_at) VALUES (?, ?, ?)",
            arrayOf("ci1", "My Item", "2026-01-01T10:00:00.000Z")
        )

        assertTrue(tracker.hasPendingChanges())

        val accepted = listOf(
            PushResult(
                id = "ci1",
                tableName = "custom_items",
                operation = "create",
                status = PushStatus.APPLIED,
                serverUpdatedAt = "2026-01-01T14:00:00.000Z"
            )
        )

        processor.applyAccepted(accepted, listOf(customTable))

        // RYOW should write to "modified_at", not "updated_at"
        val row = db.queryOne("SELECT modified_at FROM custom_items WHERE item_id = ?", arrayOf("ci1"))
        assertEquals("2026-01-01T14:00:00.000Z", row?.get("modified_at"))
    }

    @Test
    fun testApplyAcceptedDeleteRYOW() {
        val (db, tracker, processor) = makeTestEnv()

        db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            arrayOf("w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z")
        )
        tracker.clearAll()
        db.execute("DELETE FROM orders WHERE id = ?", arrayOf("w1"))

        assertTrue(tracker.hasPendingChanges())

        val accepted = listOf(
            PushResult(
                id = "w1",
                tableName = "orders",
                operation = "delete",
                status = PushStatus.APPLIED,
                serverDeletedAt = "2026-01-01T12:00:00.000Z"
            )
        )

        processor.applyAccepted(accepted, listOf(testTable))

        assertFalse(tracker.hasPendingChanges())

        val row = db.queryOne("SELECT deleted_at FROM orders WHERE id = ?", arrayOf("w1"))
        assertEquals("2026-01-01T12:00:00.000Z", row?.get("deleted_at"))
    }

    @Test
    fun testApplyAcceptedDoesNotTriggerCDC() {
        val (db, tracker, processor) = makeTestEnv()

        db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            arrayOf("w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z")
        )

        val accepted = listOf(
            PushResult(
                id = "w1",
                tableName = "orders",
                operation = "create",
                status = PushStatus.APPLIED,
                serverUpdatedAt = "2026-01-01T12:00:00.000Z"
            )
        )

        processor.applyAccepted(accepted, listOf(testTable))

        // Pending queue should be empty — sync_lock prevented the RYOW update from re-queuing
        assertFalse(tracker.hasPendingChanges())
    }

    // MARK: - applyRejected Tests

    @Test
    fun testApplyRejectedAppliesServerVersion() {
        val (db, tracker, processor) = makeTestEnv()

        db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            arrayOf("w1", "Client Address", "u1", "2026-01-01T10:00:00.000Z")
        )

        assertTrue(tracker.hasPendingChanges())

        val serverVersion = Record(
            id = "w1",
            tableName = "orders",
            data = mapOf(
                "id" to AnyCodable("w1"),
                "ship_address" to AnyCodable("Server Address"),
                "user_id" to AnyCodable("u1"),
                "updated_at" to AnyCodable("2026-01-01T11:00:00.000Z"),
            ),
            updatedAt = "2026-01-01T11:00:00.000Z"
        )

        val rejected = listOf(
            PushResult(
                id = "w1",
                tableName = "orders",
                operation = "update",
                status = PushStatus.CONFLICT,
                reason = "server version is newer",
                serverVersion = serverVersion
            )
        )

        val conflicts = processor.applyRejected(rejected, listOf(testTable))

        // Pending should be drained
        assertFalse(tracker.hasPendingChanges())

        // Local record should have server's data
        val row = db.queryOne("SELECT ship_address, updated_at FROM orders WHERE id = ?", arrayOf("w1"))
        assertEquals("Server Address", row?.get("ship_address"))
        assertEquals("2026-01-01T11:00:00.000Z", row?.get("updated_at"))

        // Should fire conflict event
        assertEquals(1, conflicts.size)
        assertEquals("orders", conflicts[0].table)
        assertEquals("w1", conflicts[0].recordID)
        assertEquals(AnyCodable("Server Address"), conflicts[0].serverData?.get("ship_address"))
    }

    @Test
    fun testApplyRejectedWithoutServerVersion() {
        val (db, tracker, processor) = makeTestEnv()

        db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            arrayOf("w1", "Client Address", "u1", "2026-01-01T10:00:00.000Z")
        )

        val rejected = listOf(
            PushResult(
                id = "w1",
                tableName = "orders",
                operation = "update",
                status = PushStatus.REJECTED_TERMINAL,
                reason = "ownership violation"
            )
        )

        val conflicts = processor.applyRejected(rejected, listOf(testTable))

        // Pending drained
        assertFalse(tracker.hasPendingChanges())

        // Local record unchanged (no server version to apply)
        val row = db.queryOne("SELECT ship_address FROM orders WHERE id = ?", arrayOf("w1"))
        assertEquals("Client Address", row?.get("ship_address"))

        // Error status, not conflict — no conflict event
        assertEquals(0, conflicts.size)
    }

    @Test
    fun testApplyRejectedDoesNotTriggerCDC() {
        val (db, tracker, processor) = makeTestEnv()

        db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            arrayOf("w1", "Client Address", "u1", "2026-01-01T10:00:00.000Z")
        )

        val serverVersion = Record(
            id = "w1",
            tableName = "orders",
            data = mapOf(
                "id" to AnyCodable("w1"),
                "ship_address" to AnyCodable("Server Address"),
                "user_id" to AnyCodable("u1"),
                "updated_at" to AnyCodable("2026-01-01T11:00:00.000Z"),
            ),
            updatedAt = "2026-01-01T11:00:00.000Z"
        )

        val rejected = listOf(
            PushResult(
                id = "w1",
                tableName = "orders",
                operation = "update",
                status = PushStatus.CONFLICT,
                reason = "server version is newer",
                serverVersion = serverVersion
            )
        )

        processor.applyRejected(rejected, listOf(testTable))

        // sync_lock should have prevented CDC triggers from re-queuing
        assertFalse(tracker.hasPendingChanges())
    }
}
