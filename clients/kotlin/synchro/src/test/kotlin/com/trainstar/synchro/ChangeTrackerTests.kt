package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import java.util.UUID
import org.junit.After
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
class ChangeTrackerTests {
    private val databases = TestDatabaseTracker()

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
            SchemaColumn("amount", logicalType = "decimal", precision = 6, scale = 2),
            SchemaColumn("document", logicalType = "json"),
            SchemaColumn("score", logicalType = "float"),
            SchemaColumn("payload", logicalType = "bytes"),
            SchemaColumn("updated_at", logicalType = "datetime", nullable = false),
            SchemaColumn("deleted_at", logicalType = "datetime"),
        ),
    )

    private fun environment(): Pair<SynchroDatabase, ChangeTracker> {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val database = databases.create(context)
        SchemaManager(database).createSyncedTables(
            SchemaResponse(1, PROTOCOL_TEST_SCHEMA_HASH, "2026-01-01T00:00:00.000000Z", listOf(table)),
        )
        return database to ChangeTracker(database)
    }

    @After
    fun tearDown() = databases.closeAll()

    @Test
    fun captureAppendsImmutableTypedValues() {
        val (database, tracker) = environment()
        database.execute(
            """
            INSERT INTO orders (id, title, enabled, quantity, large_quantity, amount, document, score, payload, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """.trimIndent(),
            arrayOf(
                "o1", null, 1L, 7L, 9_007_199_254_740_991L, "123.45", "{\"a\":1,\"b\":true}",
                2.5, byteArrayOf(1, 2, 3), "2026-01-01T00:00:00.000000Z",
            ),
        )
        database.execute("UPDATE orders SET title = ?, score = ? WHERE id = ?", arrayOf("after", 3.5, "o1"))
        database.execute("UPDATE orders SET title = ? WHERE id = ?", arrayOf("later", "o1"))

        val ledger = database.query(
            "SELECT mutation_id, local_order, operation, depends_on_mutation_id FROM _synchro_pending_changes ORDER BY local_order",
        )
        assertEquals(3, ledger.size)
        assertEquals(listOf("insert", "update", "update"), ledger.map { it.getValue("operation") })
        ledger.forEach { assertEquals(it.getValue("mutation_id"), UUID.fromString(it.getValue("mutation_id") as String).toString()) }
        assertNotNull(ledger[1]["depends_on_mutation_id"])
        assertEquals(ledger[1]["mutation_id"], ledger[2]["depends_on_mutation_id"])

        val insertID = ledger.first().getValue("mutation_id") as String
        val updateID = ledger[1].getValue("mutation_id") as String
        val insertValues = database.query(
            "SELECT field_id, logical_type, value_kind, value_integer, value_real, value_blob FROM _synchro_mutation_values WHERE mutation_id = ? ORDER BY field_id",
            arrayOf(insertID),
        )
        assertEquals(8, insertValues.size)
        assertEquals("null", insertValues.single { it["field_id"] == "title" }["value_kind"])
        assertEquals("boolean", insertValues.single { it["field_id"] == "enabled" }["value_kind"])
        assertEquals("integer", insertValues.single { it["field_id"] == "quantity" }["value_kind"])
        assertEquals("integer", insertValues.single { it["field_id"] == "large_quantity" }["value_kind"])
        assertEquals("text", insertValues.single { it["field_id"] == "amount" }["value_kind"])
        assertEquals("text", insertValues.single { it["field_id"] == "document" }["value_kind"])
        assertEquals("real", insertValues.single { it["field_id"] == "score" }["value_kind"])
        assertEquals("blob", insertValues.single { it["field_id"] == "payload" }["value_kind"])

        val updateFields = database.query(
            "SELECT field_id FROM _synchro_mutation_values WHERE mutation_id = ? ORDER BY field_id",
            arrayOf(updateID),
        ).map { it.getValue("field_id") }
        assertEquals(listOf("score", "title"), updateFields)
        val hydrated = tracker.hydratePendingForPush(listOf(tracker.pendingChanges().first()), listOf(table.localSchema)).single()
        assertEquals(AnyCodable(7L), hydrated.data?.get("quantity"))
        assertEquals(AnyCodable("9007199254740991"), hydrated.data?.get("large_quantity"))
        assertEquals(AnyCodable("123.45"), hydrated.data?.get("amount"))
        assertEquals(AnyCodable("{\"a\":1,\"b\":true}"), hydrated.data?.get("document"))
        assertEquals(AnyCodable(true), hydrated.data?.get("enabled"))
        assertEquals(AnyCodable(2.5), hydrated.data?.get("score"))
        assertEquals(AnyCodable("AQID"), hydrated.data?.get("payload"))
        assertEquals(AnyCodable(null), hydrated.data?.get("title"))
        assertEquals(3, tracker.pendingChangeCount())
    }

    @Test
    fun deleteAppendsAColumnFreeIntentWithoutCoalescingSources() {
        val (database, _) = environment()
        database.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            arrayOf("o1", "first", "2026-01-01T00:00:00.000000Z"),
        )
        database.execute("UPDATE orders SET title = ? WHERE id = ?", arrayOf("second", "o1"))
        database.execute("DELETE FROM orders WHERE id = ?", arrayOf("o1"))

        val changes = database.query("SELECT mutation_id, operation FROM _synchro_pending_changes ORDER BY local_order")
        assertEquals(listOf("insert", "update", "delete"), changes.map { it.getValue("operation") })
        val deleteID = changes.last().getValue("mutation_id") as String
        assertTrue(database.query("SELECT field_id FROM _synchro_mutation_values WHERE mutation_id = ?", arrayOf(deleteID)).isEmpty())
        assertNotNull(database.queryOne("SELECT deleted_at FROM orders WHERE id = ?", arrayOf("o1"))?.get("deleted_at"))
    }

    @Test
    fun syncLockAndMissingArchivePreventCaptureAtomically() {
        val (database, tracker) = environment()
        database.writeTransaction { SynchroMeta.setSyncLock(it, true) }
        database.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
            arrayOf("locked", "no intent", "2026-01-01T00:00:00.000000Z"),
        )
        assertFalse(tracker.hasPendingChanges())

        database.writeTransaction {
            SynchroMeta.setSyncLock(it, false)
            it.execSQL("DELETE FROM _synchro_schema_archives")
        }
        assertTrue(
            runCatching {
                database.execute(
                    "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
                    arrayOf("blocked", "no row", "2026-01-01T00:00:00.000000Z"),
                )
            }.isFailure,
        )
        assertTrue(database.query("SELECT id FROM orders WHERE id = 'blocked'").isEmpty())
        assertEquals(0L, database.queryOne("SELECT COUNT(*) AS count FROM _synchro_pending_changes")?.get("count"))
    }
}
