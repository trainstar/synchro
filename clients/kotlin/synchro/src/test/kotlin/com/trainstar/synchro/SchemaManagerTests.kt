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
class SchemaManagerTests {

    private fun makeTestDB(): SynchroDatabase {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val dbName = "synchro_test_${UUID.randomUUID()}.sqlite"
        return SynchroDatabase(context, dbName)
    }

    @Test
    fun testCreateSyncedTables() {
        val db = makeTestDB()
        val manager = SchemaManager(db)
        val schema = SchemaResponse(
            schemaVersion = 1,
            schemaHash = "abc123",
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
                        SchemaColumn(name = "updated_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "deleted_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = true, isPrimaryKey = false),
                    )
                )
            )
        )

        manager.createSyncedTables(schema)

        // Verify table exists
        val rows = db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'")
        assertEquals(1, rows.size)

        // Verify triggers exist
        val triggers = db.query("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_synchro_cdc_%orders'")
        assertEquals(3, triggers.size)
    }

    @Test
    fun testMigrateSchemaAddsColumn() {
        val db = makeTestDB()
        val manager = SchemaManager(db)

        val v1 = SchemaResponse(
            schemaVersion = 1,
            schemaHash = "v1",
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
                        SchemaColumn(name = "updated_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "deleted_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = true, isPrimaryKey = false),
                    )
                )
            )
        )
        manager.createSyncedTables(v1)

        val v2 = SchemaResponse(
            schemaVersion = 2,
            schemaHash = "v2",
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
                        SchemaColumn(name = "description", dbType = "text", logicalType = "string", nullable = true, isPrimaryKey = false),
                        SchemaColumn(name = "updated_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "deleted_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = true, isPrimaryKey = false),
                    )
                )
            )
        )
        manager.migrateSchema(v2)

        // Verify new column exists by inserting and reading back
        db.execute(
            "INSERT INTO orders (id, description, updated_at) VALUES ('test-1', 'hello', '2026-01-01T00:00:00Z')"
        )
        val row = db.queryOne("SELECT description FROM orders WHERE id = ?", arrayOf("test-1"))
        assertNotNull(row)
        assertEquals("hello", row?.get("description"))
    }

    @Test
    fun testDropSyncedTables() {
        val db = makeTestDB()
        val manager = SchemaManager(db)
        val schema = SchemaResponse(
            schemaVersion = 1,
            schemaHash = "abc123",
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
                        SchemaColumn(name = "updated_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "deleted_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = true, isPrimaryKey = false),
                    )
                )
            )
        )

        manager.createSyncedTables(schema)

        // Verify table and triggers exist
        assertEquals(1, db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'").size)
        assertEquals(3, db.query("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_synchro_cdc_%orders'").size)

        // Drop
        manager.dropSyncedTables(schema)

        // Verify table and triggers are gone
        assertEquals(0, db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'").size)
        assertEquals(0, db.query("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_synchro_cdc_%orders'").size)
    }
}
