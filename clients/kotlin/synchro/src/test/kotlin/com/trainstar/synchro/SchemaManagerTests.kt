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
class SchemaManagerTests {
    private val databases = TestDatabaseTracker()

    private fun makeTestDB(): SynchroDatabase {
        val context = ApplicationProvider.getApplicationContext<Context>()
        return databases.create(context)
    }

    private fun makeManifest(tables: List<TableSchema>): SchemaManifest =
        SchemaManifest(tables)

    @After
    fun tearDown() {
        databases.closeAll()
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
    fun testReconcileLocalSchemaFromPortableManifest() {
        val db = makeTestDB()
        val manager = SchemaManager(db)
        val manifest = makeManifest(
            listOf(
                TableSchema(
                    name = "workouts",
                    primaryKey = listOf("id"),
                    updatedAtColumn = "updated_at",
                    deletedAtColumn = "deleted_at",
                    composition = CompositionClass.SINGLE_SCOPE,
                    columns = listOf(
                        ColumnSchema(name = "id", typeName = "string", nullable = false),
                        ColumnSchema(name = "name", typeName = "string", nullable = false),
                        ColumnSchema(name = "updated_at", typeName = "datetime", nullable = false),
                        ColumnSchema(name = "deleted_at", typeName = "datetime", nullable = true),
                    ),
                    indexes = null,
                )
            )
        )

        manager.reconcileLocalSchema(schemaVersion = 7, schemaHash = "portable-v1", tables = manifest.localTables())

        assertEquals(1, db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='workouts'").size)
        assertEquals(3, db.query("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_synchro_cdc_%workouts'").size)

        db.readTransaction { rawDb ->
            assertEquals(7L, SynchroMeta.getInt64(rawDb, MetaKey.SCHEMA_VERSION))
            assertEquals("portable-v1", SynchroMeta.get(rawDb, MetaKey.SCHEMA_HASH))
        }
    }

    @Test
    fun testReconcileLocalSchemaMigratesAdditiveManifestChange() {
        val db = makeTestDB()
        val manager = SchemaManager(db)

        val v1 = makeManifest(
            listOf(
                TableSchema(
                    name = "workouts",
                    primaryKey = listOf("id"),
                    updatedAtColumn = "updated_at",
                    deletedAtColumn = "deleted_at",
                    composition = CompositionClass.SINGLE_SCOPE,
                    columns = listOf(
                        ColumnSchema(name = "id", typeName = "string", nullable = false),
                        ColumnSchema(name = "name", typeName = "string", nullable = false),
                        ColumnSchema(name = "updated_at", typeName = "datetime", nullable = false),
                        ColumnSchema(name = "deleted_at", typeName = "datetime", nullable = true),
                    ),
                    indexes = null,
                )
            )
        )
        manager.reconcileLocalSchema(schemaVersion = 1, schemaHash = "portable-v1", tables = v1.localTables())

        db.execute("INSERT INTO workouts (id, name, updated_at) VALUES ('w-1', 'Morning Run', '2026-01-01T00:00:00Z')")

        val v2 = makeManifest(
            listOf(
                TableSchema(
                    name = "workouts",
                    primaryKey = listOf("id"),
                    updatedAtColumn = "updated_at",
                    deletedAtColumn = "deleted_at",
                    composition = CompositionClass.SINGLE_SCOPE,
                    columns = listOf(
                        ColumnSchema(name = "id", typeName = "string", nullable = false),
                        ColumnSchema(name = "name", typeName = "string", nullable = false),
                        ColumnSchema(name = "notes", typeName = "string", nullable = true),
                        ColumnSchema(name = "updated_at", typeName = "datetime", nullable = false),
                        ColumnSchema(name = "deleted_at", typeName = "datetime", nullable = true),
                    ),
                    indexes = null,
                )
            )
        )
        manager.reconcileLocalSchema(schemaVersion = 2, schemaHash = "portable-v2", tables = v2.localTables())

        val row = db.queryOne("SELECT name, notes FROM workouts WHERE id = ?", arrayOf("w-1"))
        assertNotNull(row)
        assertEquals("Morning Run", row?.get("name"))
        assertNull(row?.get("notes"))
    }

    @Test
    fun testPortableManifestAliasTypesNormalizeWithoutTriggeringRebuild() {
        val db = makeTestDB()
        val manager = SchemaManager(db)

        val canonical = makeManifest(
            listOf(
                TableSchema(
                    name = "metrics",
                    primaryKey = listOf("id"),
                    updatedAtColumn = "updated_at",
                    deletedAtColumn = "deleted_at",
                    composition = CompositionClass.SINGLE_SCOPE,
                    columns = listOf(
                        ColumnSchema(name = "id", typeName = "string", nullable = false),
                        ColumnSchema(name = "score", typeName = "int", nullable = true),
                        ColumnSchema(name = "updated_at", typeName = "datetime", nullable = false),
                        ColumnSchema(name = "deleted_at", typeName = "datetime", nullable = true),
                    ),
                    indexes = null,
                )
            )
        )
        manager.reconcileLocalSchema(schemaVersion = 1, schemaHash = "canonical-v1", tables = canonical.localTables())
        db.execute("INSERT INTO metrics (id, score, updated_at) VALUES ('m-1', 7, '2026-01-01T00:00:00Z')")
        db.writeTransaction { rawDb ->
            SynchroMeta.set(rawDb, MetaKey.SNAPSHOT_COMPLETE, "1")
        }

        val aliasManifest = makeManifest(
            listOf(
                TableSchema(
                    name = "metrics",
                    primaryKey = listOf("id"),
                    updatedAtColumn = "updated_at",
                    deletedAtColumn = "deleted_at",
                    composition = CompositionClass.SINGLE_SCOPE,
                    columns = listOf(
                        ColumnSchema(name = "id", typeName = "uuid", nullable = false),
                        ColumnSchema(name = "score", typeName = "integer", nullable = true),
                        ColumnSchema(name = "updated_at", typeName = "timestamp", nullable = false),
                        ColumnSchema(name = "deleted_at", typeName = "timestamp", nullable = true),
                    ),
                    indexes = null,
                )
            )
        )

        val normalizedTables = aliasManifest.localTables()
        assertEquals(listOf("string", "int", "datetime", "datetime"), normalizedTables[0].columns.map { it.logicalType })

        manager.reconcileLocalSchema(schemaVersion = 2, schemaHash = "alias-v2", tables = normalizedTables)

        val row = db.queryOne("SELECT score FROM metrics WHERE id = ?", arrayOf("m-1"))
        assertNotNull(row)
        assertEquals(7L, row?.get("score"))

        val snapshotComplete = db.readTransaction { rawDb ->
            SynchroMeta.get(rawDb, MetaKey.SNAPSHOT_COMPLETE)
        }
        assertEquals("1", snapshotComplete)
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

    @Test
    fun testLocalOnlyTablesSurviveSchemaMigration() {
        val db = makeTestDB()
        val manager = SchemaManager(db)

        // Create a local-only table with data using raw SQL
        db.writeTransaction { rawDb ->
            rawDb.execSQL("CREATE TABLE app_settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            rawDb.execSQL("INSERT INTO app_settings (key, value) VALUES ('theme', 'dark')")
            rawDb.execSQL("INSERT INTO app_settings (key, value) VALUES ('locale', 'en')")
        }

        // Create synced table via schema v1
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

        // Migrate to v2 -- server schema does NOT include app_settings
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
                        SchemaColumn(name = "notes", dbType = "text", logicalType = "string", nullable = true, isPrimaryKey = false),
                        SchemaColumn(name = "updated_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "deleted_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = true, isPrimaryKey = false),
                    )
                )
            )
        )
        manager.migrateSchema(v2)

        // Verify local-only table still exists
        val tableRows = db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='app_settings'")
        assertEquals(1, tableRows.size)

        // Verify data is preserved
        val settings = db.query("SELECT key, value FROM app_settings ORDER BY key")
        assertEquals(2, settings.size)
        assertEquals("locale", settings[0]["key"])
        assertEquals("en", settings[0]["value"])
        assertEquals("theme", settings[1]["key"])
        assertEquals("dark", settings[1]["value"])
    }

    @Test
    fun testSyncedTableExtraColumnsSurviveMigration() {
        val db = makeTestDB()
        val manager = SchemaManager(db)

        // Create synced table via schema v1
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
                        SchemaColumn(name = "title", dbType = "text", logicalType = "string", nullable = true, isPrimaryKey = false),
                        SchemaColumn(name = "updated_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "deleted_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = true, isPrimaryKey = false),
                    )
                )
            )
        )
        manager.createSyncedTables(v1)

        // Add a local-only column via raw SQL and insert data
        db.writeTransaction { rawDb ->
            rawDb.execSQL("ALTER TABLE orders ADD COLUMN extra_data TEXT")
        }
        db.execute("INSERT INTO orders (id, title, extra_data, updated_at) VALUES ('ord-1', 'Order 1', 'local-cache', '2026-01-01T00:00:00Z')")

        // Migrate to v2 -- server schema does NOT include extra_data
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
                        SchemaColumn(name = "title", dbType = "text", logicalType = "string", nullable = true, isPrimaryKey = false),
                        SchemaColumn(name = "updated_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "deleted_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = true, isPrimaryKey = false),
                    )
                )
            )
        )
        manager.migrateSchema(v2)

        // Verify extra_data column still exists and data is preserved
        val row = db.queryOne("SELECT extra_data FROM orders WHERE id = ?", arrayOf("ord-1"))
        assertNotNull(row)
        assertEquals("local-cache", row?.get("extra_data"))
    }

    @Test
    fun testServerAddsNewColumnNonDestructive() {
        val db = makeTestDB()
        val manager = SchemaManager(db)

        // Create v1 schema and insert data
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
                        SchemaColumn(name = "title", dbType = "text", logicalType = "string", nullable = true, isPrimaryKey = false),
                        SchemaColumn(name = "updated_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "deleted_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = true, isPrimaryKey = false),
                    )
                )
            )
        )
        manager.createSyncedTables(v1)
        db.execute("INSERT INTO orders (id, title, updated_at) VALUES ('ord-1', 'First Order', '2026-01-01T00:00:00Z')")

        // Migrate to v2 that adds a new column
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
                        SchemaColumn(name = "title", dbType = "text", logicalType = "string", nullable = true, isPrimaryKey = false),
                        SchemaColumn(name = "priority", dbType = "integer", logicalType = "int", nullable = true, isPrimaryKey = false),
                        SchemaColumn(name = "updated_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "deleted_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = true, isPrimaryKey = false),
                    )
                )
            )
        )
        manager.migrateSchema(v2)

        // Verify old data is preserved
        val row = db.queryOne("SELECT id, title FROM orders WHERE id = ?", arrayOf("ord-1"))
        assertNotNull(row)
        assertEquals("First Order", row?.get("title"))

        // Verify new column exists (nullable, so old row has NULL)
        val rowWithPriority = db.queryOne("SELECT priority FROM orders WHERE id = ?", arrayOf("ord-1"))
        assertNotNull(rowWithPriority)
        assertNull(rowWithPriority?.get("priority"))

        // Verify new column is usable
        db.execute("INSERT INTO orders (id, title, priority, updated_at) VALUES ('ord-2', 'Second Order', 5, '2026-01-02T00:00:00Z')")
        val newRow = db.queryOne("SELECT priority FROM orders WHERE id = ?", arrayOf("ord-2"))
        assertNotNull(newRow)
        assertEquals(5L, newRow?.get("priority"))
    }

    @Test
    fun testServerAddsNewTable() {
        val db = makeTestDB()
        val manager = SchemaManager(db)

        // Create v1 with one table
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
                        SchemaColumn(name = "title", dbType = "text", logicalType = "string", nullable = true, isPrimaryKey = false),
                        SchemaColumn(name = "updated_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "deleted_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = true, isPrimaryKey = false),
                    )
                )
            )
        )
        manager.createSyncedTables(v1)
        db.execute("INSERT INTO orders (id, title, updated_at) VALUES ('ord-1', 'Order One', '2026-01-01T00:00:00Z')")

        // Migrate to v2 that adds a second table
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
                        SchemaColumn(name = "title", dbType = "text", logicalType = "string", nullable = true, isPrimaryKey = false),
                        SchemaColumn(name = "updated_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "deleted_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = true, isPrimaryKey = false),
                    )
                ),
                SchemaTable(
                    tableName = "items",
                    pushPolicy = "owner_only",
                    updatedAtColumn = "updated_at",
                    deletedAtColumn = "deleted_at",
                    primaryKey = listOf("id"),
                    columns = listOf(
                        SchemaColumn(name = "id", dbType = "uuid", logicalType = "string", nullable = false, isPrimaryKey = true),
                        SchemaColumn(name = "order_id", dbType = "uuid", logicalType = "string", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "product_name", dbType = "text", logicalType = "string", nullable = true, isPrimaryKey = false),
                        SchemaColumn(name = "updated_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "deleted_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = true, isPrimaryKey = false),
                    )
                )
            )
        )
        manager.migrateSchema(v2)

        // Verify first table is unchanged
        val orderRow = db.queryOne("SELECT title FROM orders WHERE id = ?", arrayOf("ord-1"))
        assertNotNull(orderRow)
        assertEquals("Order One", orderRow?.get("title"))

        // Verify second table exists
        val itemsTables = db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='items'")
        assertEquals(1, itemsTables.size)

        // Verify second table has triggers
        val itemsTriggers = db.query("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_synchro_cdc_%items'")
        assertEquals(3, itemsTriggers.size)

        // Verify second table is usable
        db.execute("INSERT INTO items (id, order_id, product_name, updated_at) VALUES ('itm-1', 'ord-1', 'Widget', '2026-01-01T00:00:00Z')")
        val itemRow = db.queryOne("SELECT product_name FROM items WHERE id = ?", arrayOf("itm-1"))
        assertNotNull(itemRow)
        assertEquals("Widget", itemRow?.get("product_name"))
    }

    @Test
    fun testServerRemovesColumnNonDestructive() {
        val db = makeTestDB()
        val manager = SchemaManager(db)

        // Create v1 with description column
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
                        SchemaColumn(name = "description", dbType = "text", logicalType = "string", nullable = true, isPrimaryKey = false),
                        SchemaColumn(name = "updated_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "deleted_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = true, isPrimaryKey = false),
                    )
                )
            )
        )
        manager.createSyncedTables(v1)
        db.execute("INSERT INTO orders (id, description, updated_at) VALUES ('ord-1', 'Important order', '2026-01-01T00:00:00Z')")

        // Migrate to v2 that removes "description"
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
                        SchemaColumn(name = "updated_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "deleted_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = true, isPrimaryKey = false),
                    )
                )
            )
        )
        manager.migrateSchema(v2)

        // Verify "description" column still exists locally (non-destructive)
        val row = db.queryOne("SELECT description FROM orders WHERE id = ?", arrayOf("ord-1"))
        assertNotNull(row)
        assertEquals("Important order", row?.get("description"))
    }

    @Test
    fun testServerRemovesTableNonDestructive() {
        val db = makeTestDB()
        val manager = SchemaManager(db)

        // Create v1 with two tables
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
                ),
                SchemaTable(
                    tableName = "items",
                    pushPolicy = "owner_only",
                    updatedAtColumn = "updated_at",
                    deletedAtColumn = "deleted_at",
                    primaryKey = listOf("id"),
                    columns = listOf(
                        SchemaColumn(name = "id", dbType = "uuid", logicalType = "string", nullable = false, isPrimaryKey = true),
                        SchemaColumn(name = "product_name", dbType = "text", logicalType = "string", nullable = true, isPrimaryKey = false),
                        SchemaColumn(name = "updated_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "deleted_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = true, isPrimaryKey = false),
                    )
                )
            )
        )
        manager.createSyncedTables(v1)
        db.execute("INSERT INTO items (id, product_name, updated_at) VALUES ('itm-1', 'Gadget', '2026-01-01T00:00:00Z')")

        // Migrate to v2 with only "orders" -- server removes "items"
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
                        SchemaColumn(name = "updated_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "deleted_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = true, isPrimaryKey = false),
                    )
                )
            )
        )
        manager.migrateSchema(v2)

        // Verify "items" table still exists locally with data
        val tableRows = db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='items'")
        assertEquals(1, tableRows.size)

        val itemRow = db.queryOne("SELECT product_name FROM items WHERE id = ?", arrayOf("itm-1"))
        assertNotNull(itemRow)
        assertEquals("Gadget", itemRow?.get("product_name"))
    }

    @Test
    fun testPreExistingTablesFromSeedReconciled() {
        val db = makeTestDB()
        val manager = SchemaManager(db)

        // Manually create a table matching a server schema table but missing one column (stale seed)
        db.writeTransaction { rawDb ->
            rawDb.execSQL("CREATE TABLE orders (id TEXT PRIMARY KEY, updated_at TEXT NOT NULL, deleted_at TEXT)")
            rawDb.execSQL("INSERT INTO orders (id, updated_at) VALUES ('ord-1', '2026-01-01T00:00:00Z')")
        }

        // Call migrateSchema with full schema that includes a column the seed is missing
        val schema = SchemaResponse(
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
                        SchemaColumn(name = "title", dbType = "text", logicalType = "string", nullable = true, isPrimaryKey = false),
                        SchemaColumn(name = "updated_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "deleted_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = true, isPrimaryKey = false),
                    )
                )
            )
        )
        manager.migrateSchema(schema)

        // Verify the missing column was added
        val columns = mutableListOf<String>()
        db.readTransaction { rawDb ->
            rawDb.rawQuery("PRAGMA table_info(orders)", null).use { cursor ->
                val nameIdx = cursor.getColumnIndex("name")
                while (cursor.moveToNext()) {
                    columns.add(cursor.getString(nameIdx))
                }
            }
        }
        assertTrue("title column should exist", columns.contains("title"))

        // Verify triggers are installed
        val triggers = db.query("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_synchro_cdc_%orders'")
        assertEquals(3, triggers.size)

        // Verify existing data is preserved
        val row = db.queryOne("SELECT id, updated_at FROM orders WHERE id = ?", arrayOf("ord-1"))
        assertNotNull(row)
        assertEquals("ord-1", row?.get("id"))

        // Verify new column is usable on existing row (should be NULL)
        val rowWithTitle = db.queryOne("SELECT title FROM orders WHERE id = ?", arrayOf("ord-1"))
        assertNotNull(rowWithTitle)
        assertNull(rowWithTitle?.get("title"))
    }

    @Test
    fun testColumnTypeIncompatibilityTriggersRebuild() {
        val db = makeTestDB()
        val manager = SchemaManager(db)

        // Create v1 where "score" is TEXT type (logicalType "string")
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
                        SchemaColumn(name = "score", dbType = "text", logicalType = "string", nullable = true, isPrimaryKey = false),
                        SchemaColumn(name = "updated_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "deleted_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = true, isPrimaryKey = false),
                    )
                )
            )
        )
        manager.createSyncedTables(v1)

        // Insert data and set a non-zero checkpoint to detect rebuild
        db.execute("INSERT INTO orders (id, score, updated_at) VALUES ('ord-1', 'high', '2026-01-01T00:00:00Z')")
        db.writeTransaction { rawDb ->
            SynchroMeta.setInt64(rawDb, MetaKey.CHECKPOINT, 42L)
        }

        // Verify checkpoint is set before migration
        val checkpointBefore = db.readTransaction { rawDb -> SynchroMeta.getInt64(rawDb, MetaKey.CHECKPOINT) }
        assertEquals(42L, checkpointBefore)

        // Migrate to v2 where "score" is logicalType "int" (INTEGER)
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
                        SchemaColumn(name = "score", dbType = "integer", logicalType = "int", nullable = true, isPrimaryKey = false),
                        SchemaColumn(name = "updated_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "deleted_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = true, isPrimaryKey = false),
                    )
                )
            )
        )
        manager.migrateSchema(v2)

        // Verify destructive rebuild occurred: checkpoint reset to 0
        val checkpointAfter = db.readTransaction { rawDb -> SynchroMeta.getInt64(rawDb, MetaKey.CHECKPOINT) }
        assertEquals(0L, checkpointAfter)

        // Verify table was recreated (old data is gone due to destructive rebuild)
        val rows = db.query("SELECT * FROM orders")
        assertEquals(0, rows.size)

        // Verify table exists with correct schema and triggers
        val tableRows = db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'")
        assertEquals(1, tableRows.size)

        val triggers = db.query("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_synchro_cdc_%orders'")
        assertEquals(3, triggers.size)

        // Verify "score" column now accepts integers
        db.execute("INSERT INTO orders (id, score, updated_at) VALUES ('ord-2', 99, '2026-01-02T00:00:00Z')")
        val newRow = db.queryOne("SELECT score FROM orders WHERE id = ?", arrayOf("ord-2"))
        assertNotNull(newRow)
        assertEquals(99L, newRow?.get("score"))
    }
}
