package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import org.junit.After
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28])
class ChangeTrackerTests {
    private val databases = TestDatabaseTracker()

    private val testSchema = SchemaResponse(
        schemaVersion = 1,
        schemaHash = "test",
        serverTime = "2026-01-01T12:00:00.000Z",
        tables = listOf(
            SchemaTable(
                tableName = "orders",
                pushPolicy = "owner_only",
                updatedAtColumn = "updated_at",
                deletedAtColumn = "deleted_at",
                primaryKey = listOf("id"),
                columns = listOf(
                    SchemaColumn(name = "id", dbType = "uuid", logicalType = "string", nullable = false, isPrimaryKey = true),
                    SchemaColumn(name = "ship_address", dbType = "text", logicalType = "string", nullable = true, isPrimaryKey = false),
                    SchemaColumn(name = "user_id", dbType = "uuid", logicalType = "string", nullable = false, isPrimaryKey = false),
                    SchemaColumn(name = "created_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                    SchemaColumn(name = "updated_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                    SchemaColumn(name = "deleted_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = true, isPrimaryKey = false),
                )
            )
        )
    )

    private fun makeTestEnv(): Triple<SynchroDatabase, ChangeTracker, SchemaManager> {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val db = databases.create(context)
        val tracker = ChangeTracker(db)
        val manager = SchemaManager(db)
        manager.createSyncedTables(testSchema)
        return Triple(db, tracker, manager)
    }

    @After
    fun tearDown() {
        databases.closeAll()
    }

    @Test
    fun testInsertTriggerCreatesEntry() {
        val (db, tracker, _) = makeTestEnv()

        db.execute(
            "INSERT INTO orders (id, ship_address, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            arrayOf("w1", "123 Main St", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
        )

        val pending = tracker.pendingChanges()
        assertEquals(1, pending.size)
        assertEquals("w1", pending[0].recordID)
        assertEquals("orders", pending[0].tableName)
        assertEquals("create", pending[0].operation)
    }

    @Test
    fun testUpdateTriggerCreatesEntry() {
        val (db, tracker, _) = makeTestEnv()

        db.execute(
            "INSERT INTO orders (id, ship_address, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            arrayOf("w1", "123 Main St", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
        )
        tracker.clearAll()

        db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", arrayOf("456 Oak Ave", "w1"))

        val pending = tracker.pendingChanges()
        assertEquals(1, pending.size)
        assertEquals("update", pending[0].operation)
        assertNotNull(pending[0].baseUpdatedAt)
    }

    @Test
    fun testDeleteTriggerConvertsSoftDelete() {
        val (db, tracker, _) = makeTestEnv()

        db.execute(
            "INSERT INTO orders (id, ship_address, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            arrayOf("w1", "123 Main St", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
        )
        tracker.clearAll()

        // Hard DELETE should be intercepted by BEFORE DELETE trigger
        db.execute("DELETE FROM orders WHERE id = ?", arrayOf("w1"))

        // Record should still exist with deleted_at set
        val row = db.queryOne("SELECT deleted_at FROM orders WHERE id = ?", arrayOf("w1"))
        assertNotNull(row)
        assertNotNull(row?.get("deleted_at"))

        // Pending queue should have a delete operation
        val pending = tracker.pendingChanges()
        assertEquals(1, pending.size)
        assertEquals("delete", pending[0].operation)
    }

    @Test
    fun testDedupCreateThenUpdate() {
        val (db, tracker, _) = makeTestEnv()

        db.execute(
            "INSERT INTO orders (id, ship_address, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            arrayOf("w1", "123 Main St", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
        )
        db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", arrayOf("789 Updated Blvd", "w1"))

        val pending = tracker.pendingChanges()
        assertEquals(1, pending.size)
        // create + update = still create
        assertEquals("create", pending[0].operation)
    }

    @Test
    fun testDedupCreateThenDeleteRemovesEntry() {
        val (db, tracker, _) = makeTestEnv()

        db.execute(
            "INSERT INTO orders (id, ship_address, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            arrayOf("w1", "123 Main St", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
        )
        db.execute("DELETE FROM orders WHERE id = ?", arrayOf("w1"))

        // create + delete = removed entirely (never reached server)
        val pending = tracker.pendingChanges()
        assertEquals(0, pending.size)
    }

    @Test
    fun testSyncLockPreventsTracking() {
        val (db, tracker, _) = makeTestEnv()

        db.writeTransaction { dbConn ->
            SynchroMeta.setSyncLock(dbConn, true)
        }

        db.execute(
            "INSERT INTO orders (id, ship_address, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            arrayOf("w1", "123 Main St", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
        )

        val pending = tracker.pendingChanges()
        assertEquals(0, pending.size)

        db.writeTransaction { dbConn ->
            SynchroMeta.setSyncLock(dbConn, false)
        }
    }

    @Test
    fun testClearAll() {
        val (db, tracker, _) = makeTestEnv()

        db.execute(
            "INSERT INTO orders (id, ship_address, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            arrayOf("w1", "123 Main St", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
        )

        assertTrue(tracker.hasPendingChanges())
        tracker.clearAll()
        assertFalse(tracker.hasPendingChanges())
    }

    @Test
    fun testDedupUpdateThenDelete() {
        val (db, tracker, _) = makeTestEnv()

        // Insert and clear (simulate already-pushed create)
        db.execute(
            "INSERT INTO orders (id, ship_address, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            arrayOf("w1", "123 Main St", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
        )
        tracker.clearAll()

        // Update then delete — should dedup to "delete"
        db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", arrayOf("111 Renamed St", "w1"))
        db.execute("DELETE FROM orders WHERE id = ?", arrayOf("w1"))

        val pending = tracker.pendingChanges()
        assertEquals(1, pending.size)
        assertEquals("delete", pending[0].operation)
        // base_updated_at should be preserved from the update
        assertNotNull(pending[0].baseUpdatedAt)
    }

    @Test
    fun testDedupUpdateThenUpdate() {
        val (db, tracker, _) = makeTestEnv()

        // Insert and clear (simulate already-pushed create)
        db.execute(
            "INSERT INTO orders (id, ship_address, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            arrayOf("w1", "123 Main St", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
        )
        tracker.clearAll()

        // Two sequential updates — should dedup to single "update"
        db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", arrayOf("222 First St", "w1"))
        db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", arrayOf("333 Second Ave", "w1"))

        val pending = tracker.pendingChanges()
        assertEquals(1, pending.size)
        assertEquals("update", pending[0].operation)
        assertNotNull(pending[0].baseUpdatedAt)
    }
}
