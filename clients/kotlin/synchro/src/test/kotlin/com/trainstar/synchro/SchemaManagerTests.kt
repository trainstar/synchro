package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import org.junit.After
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
import org.robolectric.annotation.SQLiteMode

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

    private fun assertCDCTriggers(db: SynchroDatabase, tableName: String) {
        val triggers = db.query(
            "SELECT name, sql FROM sqlite_master WHERE type='trigger' AND name LIKE '_synchro_cdc_%$tableName' ORDER BY name",
        )
        assertEquals(
            setOf(
                "_synchro_cdc_delete_$tableName",
                "_synchro_cdc_insert_$tableName",
                "_synchro_cdc_pk_guard_$tableName",
                "_synchro_cdc_update_$tableName",
            ),
            triggers.map { it.getValue("name") }.toSet(),
        )
        val guardSQL = triggers.single { it["name"] == "_synchro_cdc_pk_guard_$tableName" }["sql"] as String
        assertTrue(guardSQL.contains("BEFORE UPDATE OF"))
        assertTrue(guardSQL.contains("NEW.\"id\" IS NOT OLD.\"id\""))
        assertTrue(guardSQL.contains("RAISE(ABORT, 'synced primary key cannot change')"))
    }

    @After
    fun tearDown() {
        databases.closeAll()
    }

    @Test
    @SQLiteMode(SQLiteMode.Mode.NATIVE)
    fun appliedRecoveryRejectsDuplicateSourceIdentifiersAfterReopenWithoutDurableProgress() {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val sourceDraft = protocolOrdersSchemaManifest().let { manifest ->
            manifest.copy(tables = manifest.tables.map { table ->
                table.copy(indexes = listOf(IndexSchema("idx-owner", "idx_orders_owner", listOf("field-user-id"), false)))
            })
        }
        val source = sourceDraft.copy(schemaHash = Integrity.schemaManifestHash(sourceDraft))
        val sourceTables = source.localTables()
        val sourceTable = sourceTables.single()
        val targetDraft = protocolOrdersSchemaManifest(
            includeNotes = true, schemaVersion = 2,
            parentSchema = SchemaRef(1, source.schemaHash), transitionClass = "class_2",
        ).let { manifest ->
            manifest.copy(tables = manifest.tables.map { it.copy(indexes = source.tables.single().indexes) })
        }
        val target = targetDraft.copy(schemaHash = Integrity.schemaManifestHash(targetDraft))
        val journalJSON = Json { encodeDefaults = true }
        for (corruptedTables in listOf(
            sourceTables + sourceTable,
            listOf(sourceTable.copy(columns = sourceTable.columns + sourceTable.columns.first())),
            listOf(sourceTable.copy(indexes = sourceTable.indexes + sourceTable.indexes.single())),
        )) {
            val dbName = "synchro_source_archive_${java.util.UUID.randomUUID()}.sqlite"
            val database = databases.open(context, dbName)
            installTestSchema(database, 1, source.schemaHash, sourceTables)
            database.execute("CREATE TABLE drafts (id TEXT PRIMARY KEY, body TEXT)")
            database.execute("INSERT INTO drafts VALUES ('draft', 'local')")
            database.execute("INSERT INTO orders (id, user_id, updated_at) VALUES ('queued', 'owner', '2026-01-01T00:00:00.000000Z')")
            val manager = SchemaManager(database)
            manager.prepareConnectMigration(schemaMigrationResponse(target), target.localTables(), false)
            manager.recoverPendingMigration()
            assertEquals("ddl_applied", database.queryOne("SELECT phase FROM _synchro_migration_journal")?.get("phase"))
            database.writeTransaction { db ->
                db.execSQL(
                    "UPDATE _synchro_schema_archives SET manifest_json = ? WHERE schema_version = ? AND schema_hash = ?",
                    arrayOf(journalJSON.encodeToString(corruptedTables), 1, source.schemaHash),
                )
            }
            database.close()

            val reopened = databases.open(context, dbName)
            val stateQueries = listOf(
                "SELECT * FROM _synchro_meta ORDER BY key",
                "SELECT * FROM _synchro_migration_journal",
                "SELECT * FROM _synchro_schema_archives ORDER BY schema_version, schema_hash",
                "SELECT * FROM _synchro_pending_changes ORDER BY local_order",
                "SELECT * FROM orders ORDER BY id",
                "SELECT * FROM drafts ORDER BY id",
                "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY name",
            )
            val before = stateQueries.map { reopened.query(it) }
            assertThrows(SynchroError.InvalidResponse::class.java) { SchemaManager(reopened).recoverPendingMigration() }
            assertEquals(before, stateQueries.map { reopened.query(it) })
            reopened.writeTransaction { db ->
                db.execSQL(
                    "UPDATE _synchro_schema_archives SET manifest_json = ? WHERE schema_version = ? AND schema_hash = ?",
                    arrayOf(journalJSON.encodeToString(sourceTables), 1, source.schemaHash),
                )
            }
            SchemaManager(reopened).recoverPendingMigration()
            reopened.close()
        }
    }

    @Test
    @SQLiteMode(SQLiteMode.Mode.NATIVE)
    fun nullableRelaxationPreservesRowsAndDurableIntentWithoutCapture() {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val dbName = "synchro_nullable_views_${java.util.UUID.randomUUID()}.sqlite"
        val db = databases.open(context, dbName)
        val sourceDraft = protocolOrdersSchemaManifest().let { manifest ->
            manifest.copy(tables = manifest.tables.map { table ->
                table.copy(indexes = listOf(IndexSchema("idx-owner", "idx_orders_owner", listOf("field-user-id"), false)))
            })
        }
        val source = sourceDraft.copy(schemaHash = Integrity.schemaManifestHash(sourceDraft))
        installTestSchema(db, 1, source.schemaHash, source.localTables())
        db.execute("CREATE TABLE drafts (body TEXT)")
        db.execute("INSERT INTO drafts VALUES ('local')")
        db.execute("INSERT INTO orders (id, user_id, updated_at) VALUES ('queued', 'owner', '2026-01-01T00:00:00.000000Z')")
        db.execute("CREATE VIEW order_cache AS SELECT id, user_id FROM orders")
        db.execute("CREATE VIEW nested_order_cache AS SELECT id, user_id FROM order_cache")
        db.execute("CREATE VIEW draft_cache AS SELECT body FROM drafts")
        db.execute(
            "CREATE TRIGGER order_cache_write INSTEAD OF UPDATE OF user_id ON NESTED_ORDER_CACHE " +
                "BEGIN INSERT INTO drafts (body) VALUES (NEW.user_id); END",
        )
        val sourceTable = source.localTables().single()
        val sourceRow = JsonObject(mapOf(
            "field-id" to JsonPrimitive("queued"),
            "field-ship-address" to JsonNull,
            "field-user-id" to JsonPrimitive("owner"),
            "field-updated-at" to JsonPrimitive("2026-01-01T00:00:00.000000Z"),
            "field-deleted-at" to JsonNull,
        ))
        val sourceDigest = Integrity.rowDigest(
            source.schemaHash, sourceTable, JsonObject(mapOf("field-id" to JsonPrimitive("queued"))), sourceRow, "v1",
        )
        val sourceScopeChecksum = Json.encodeToString(
            Integrity.scopeDigest(source.schemaHash, "orders:test", listOf(sourceDigest.identity to sourceDigest.checksum)),
        )
        db.writeTransaction { raw ->
            SynchroMeta.upsertScope(raw, "orders:test", "c1", sourceScopeChecksum, 3, sourceScopeChecksum)
            SynchroMeta.upsertRowVersion(raw, "orders", "queued", "v1", sourceDigest.checksum)
            SynchroMeta.upsertScopeRow(raw, "orders:test", "orders", "queued", sourceDigest.checksum.digest, 3)
        }
        val pending = db.query("SELECT * FROM _synchro_pending_changes ORDER BY local_order")
        val mutationValues = db.query("SELECT * FROM _synchro_mutation_values ORDER BY mutation_id, field_id")
        val versions = db.query("SELECT * FROM _synchro_row_versions")
        val scopes = db.query("SELECT * FROM _synchro_scopes")
        val provenance = db.query("SELECT * FROM _synchro_scope_rows")
        val draft = source.copy(
            schemaVersion = 2,
            parentSchema = SchemaRef(1, source.schemaHash),
            transitionClass = "class_2",
            tables = source.tables.map { table ->
                table.copy(fields = table.fields.map { if (it.name == "user_id") it.copy(nullable = true) else it })
            },
        )
        val target = draft.copy(schemaHash = Integrity.schemaManifestHash(draft))
        val manager = SchemaManager(db)
        manager.prepareConnectMigration(
            schemaMigrationResponse(target).copy(scopeCursorUpdates = mapOf("orders:test" to "c2")),
            target.localTables(), false,
        )
        db.execute("CREATE VIEW invalid_cache AS SELECT missing_field FROM orders")
        val rollbackQueries = listOf(
            "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY name",
            "SELECT * FROM _synchro_meta ORDER BY key",
            "SELECT * FROM _synchro_migration_journal",
            "SELECT * FROM _synchro_schema_archives ORDER BY schema_version, schema_hash",
            "SELECT * FROM _synchro_pending_changes ORDER BY local_order",
            "SELECT * FROM orders ORDER BY id",
            "SELECT * FROM drafts",
        )
        val beforeRollback = rollbackQueries.map { db.query(it) }
        assertThrows(android.database.sqlite.SQLiteException::class.java) { manager.recoverPendingMigration() }
        assertEquals(beforeRollback, rollbackQueries.map { db.query(it) })
        db.execute("DROP VIEW invalid_cache")
        val viewObjectsQuery = "SELECT type, name, tbl_name, sql FROM sqlite_master WHERE type = 'view' OR " +
            "tbl_name COLLATE NOCASE IN (SELECT name FROM sqlite_master WHERE type = 'view') ORDER BY name"
        val viewObjects = db.query(viewObjectsQuery)
        db.close()

        val resumed = databases.open(context, dbName)
        SchemaManager(resumed).recoverPendingMigration()
        resumed.close()
        val recovered = databases.open(context, dbName)
        SchemaManager(recovered).recoverPendingMigration()
        assertEquals(viewObjects, recovered.query(viewObjectsQuery))
        assertEquals("owner", recovered.queryOne("SELECT user_id FROM nested_order_cache WHERE id = 'queued'")?.get("user_id"))
        assertEquals(pending, recovered.query("SELECT * FROM _synchro_pending_changes ORDER BY local_order"))
        assertEquals(mutationValues, recovered.query("SELECT * FROM _synchro_mutation_values ORDER BY mutation_id, field_id"))
        assertEquals(versions, recovered.query("SELECT * FROM _synchro_row_versions"))
        assertEquals(
            scopes.map { it + mapOf("cursor" to "c2", "checksum" to null, "local_checksum" to "") },
            recovered.query("SELECT * FROM _synchro_scopes"),
        )
        assertEquals(provenance, recovered.query("SELECT * FROM _synchro_scope_rows"))
        assertEquals("local", recovered.queryOne("SELECT body FROM draft_cache")?.get("body"))
        assertCDCTriggers(recovered, "orders")
        assertEquals("user_id", recovered.query("PRAGMA index_info(idx_orders_owner)").single()["name"])
        recovered.writeTransaction { raw ->
            raw.execSQL("UPDATE nested_order_cache SET user_id = 'view-trigger' WHERE id = 'queued'")
        }
        assertEquals(1, recovered.query("SELECT body FROM drafts WHERE body = 'view-trigger'").size)
        assertEquals(pending, recovered.query("SELECT * FROM _synchro_pending_changes ORDER BY local_order"))
        recovered.execute("DROP TRIGGER order_cache_write")
        recovered.execute("UPDATE orders SET user_id = NULL WHERE id = 'queued'")
        assertNull(recovered.queryOne("SELECT user_id FROM nested_order_cache WHERE id = 'queued'")?.get("user_id"))
        val captured = recovered.query("SELECT * FROM _synchro_pending_changes ORDER BY local_order")
        assertEquals(pending, captured.take(pending.size))
        assertEquals(pending.size + 1, captured.size)
    }

    @Test
    fun requiredAdditionWithoutDefaultUsesNullableStorageUntilRebuild() {
        val db = makeTestDB()
        val source = protocolOrdersSchemaManifest()
        installTestSchema(db, 1, source.schemaHash, source.localTables())
        db.execute("INSERT INTO orders (id, user_id, updated_at) VALUES ('retained', 'owner', '2026-01-01T00:00:00.000000Z')")
        val draft = protocolOrdersSchemaManifest(
            includeNotes = true,
            schemaVersion = 2,
            parentSchema = SchemaRef(1, source.schemaHash),
            transitionClass = "class_3",
            compatibilityFloor = 2,
        ).let { manifest ->
            manifest.copy(tables = manifest.tables.map { table ->
                table.copy(fields = table.fields.map { if (it.name == "notes") it.copy(nullable = false) else it })
            })
        }
        val target = draft.copy(schemaHash = Integrity.schemaManifestHash(draft))
        val manager = SchemaManager(db)
        manager.prepareConnectMigration(schemaMigrationResponse(target), target.localTables(), false)
        manager.recoverPendingMigration()
        manager.recoverPendingMigration()
        assertNull(db.queryOne("SELECT notes FROM orders WHERE id = 'retained'")?.get("notes"))
        assertFalse(manager.loadStoredLocalSchema()!!.single().columns.single { it.name == "notes" }.nullable)
        assertEquals(0L, db.query("PRAGMA table_info(orders)").single { it["name"] == "notes" }["notnull"])
        val table = target.localTables().single()
        val nullRequiredRow = JsonObject(table.columns.associate { column ->
            column.fieldID to when (column.name) {
                "id" -> JsonPrimitive("retained")
                "user_id" -> JsonPrimitive("owner")
                "updated_at" -> JsonPrimitive("2026-01-01T00:00:00.000000Z")
                else -> JsonNull
            }
        })
        Integrity.rowDigest(
            target.schemaHash, table, JsonObject(mapOf(table.primaryKeyFieldID to JsonPrimitive("retained"))),
            JsonObject(nullRequiredRow + ("field-notes" to JsonPrimitive("materialized"))), "v1",
        )
        assertThrows(IllegalArgumentException::class.java) {
            Integrity.rowDigest(target.schemaHash, table, JsonObject(mapOf(table.primaryKeyFieldID to JsonPrimitive("retained"))), nullRequiredRow, "v1")
        }
    }

    private fun schemaMigrationResponse(target: SchemaManifest) = ConnectResponse(
        serverTime = "2026-01-01T00:00:00.000000Z",
        protocolVersion = 3,
        clientGeneration = 1,
        scopeSetVersion = 0,
        schema = SchemaDescriptor(target.schemaVersion, target.schemaHash, SchemaAction.REPLACE),
        scopes = ScopeAssignmentDelta(emptyList(), emptyList()),
        scopeCursorUpdates = emptyMap(),
        schemaDefinition = target,
    )

    @Test
    fun testCreateSyncedTables() {
        val db = makeTestDB()
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

        installTestSchema(db, schema)

        // Verify table exists
        val rows = db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'")
        assertEquals(1, rows.size)

        // Verify triggers exist
        assertCDCTriggers(db, "orders")
    }

    @Test
    fun testReconcileLocalSchemaFromPortableManifest() {
        val db = makeTestDB()
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

        installTestSchema(db, schemaVersion = 7, schemaHash = "portable-v1", tables = manifest.localTables())

        assertEquals(1, db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='workouts'").size)
        assertCDCTriggers(db, "workouts")

        db.readTransaction { rawDb ->
            assertEquals(7L, SynchroMeta.getInt64(rawDb, MetaKey.SCHEMA_VERSION))
            assertEquals("portable-v1", SynchroMeta.get(rawDb, MetaKey.SCHEMA_HASH))
        }
    }

    @Test
    fun testReconcileLocalSchemaMigratesAdditiveManifestChange() {
        val db = makeTestDB()

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
        installTestSchema(db, schemaVersion = 1, schemaHash = "portable-v1", tables = v1.localTables())

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
        installTestSchema(db, schemaVersion = 2, schemaHash = "portable-v2", tables = v2.localTables())

        val row = db.queryOne("SELECT name, notes FROM workouts WHERE id = ?", arrayOf("w-1"))
        assertNotNull(row)
        assertEquals("Morning Run", row?.get("name"))
        assertNull(row?.get("notes"))
    }

    @Test
    fun testMigrateSchemaAddsColumn() {
        val db = makeTestDB()

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
        installTestSchema(db, v1)

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
        installTestSchema(db, v2)

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

        installTestSchema(db, schema)

        // Verify table and triggers exist
        assertEquals(1, db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'").size)
        assertCDCTriggers(db, "orders")

        // Drop
        db.writeTransaction { rawDb ->
            for (table in schema.localTables().reversed()) {
                SQLiteSchema.expectedCDCTriggerSQL(table).keys.forEach { trigger ->
                    rawDb.execSQL("DROP TRIGGER IF EXISTS ${SQLiteHelpers.quoteIdentifier(trigger)}")
                }
                rawDb.execSQL("DROP TABLE IF EXISTS ${SQLiteHelpers.quoteIdentifier(table.tableName)}")
            }
        }

        // Verify table and triggers are gone
        assertEquals(0, db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'").size)
        assertEquals(0, db.query("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_synchro_cdc_%orders'").size)
    }

    @Test
    fun testLocalOnlyTablesSurviveSchemaMigration() {
        val db = makeTestDB()

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
        installTestSchema(db, v1)

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
        installTestSchema(db, v2)

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
        installTestSchema(db, v1)

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
        installTestSchema(db, v2)

        // Verify extra_data column still exists and data is preserved
        val row = db.queryOne("SELECT extra_data FROM orders WHERE id = ?", arrayOf("ord-1"))
        assertNotNull(row)
        assertEquals("local-cache", row?.get("extra_data"))
    }

    @Test
    fun testServerAddsNewColumnNonDestructive() {
        val db = makeTestDB()

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
        installTestSchema(db, v1)
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
        installTestSchema(db, v2)

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
        installTestSchema(db, v1)
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
        installTestSchema(db, v2)

        // Verify first table is unchanged
        val orderRow = db.queryOne("SELECT title FROM orders WHERE id = ?", arrayOf("ord-1"))
        assertNotNull(orderRow)
        assertEquals("Order One", orderRow?.get("title"))

        // Verify second table exists
        val itemsTables = db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='items'")
        assertEquals(1, itemsTables.size)

        // Verify second table has triggers
        assertCDCTriggers(db, "items")

        // Verify second table is usable
        db.execute("INSERT INTO items (id, order_id, product_name, updated_at) VALUES ('itm-1', 'ord-1', 'Widget', '2026-01-01T00:00:00Z')")
        val itemRow = db.queryOne("SELECT product_name FROM items WHERE id = ?", arrayOf("itm-1"))
        assertNotNull(itemRow)
        assertEquals("Widget", itemRow?.get("product_name"))
    }

    @Test
    fun testServerRemovesColumnNonDestructive() {
        val db = makeTestDB()

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
        installTestSchema(db, v1)
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
        installTestSchema(db, v2)

        // Verify "description" column still exists locally (non-destructive)
        val row = db.queryOne("SELECT description FROM orders WHERE id = ?", arrayOf("ord-1"))
        assertNotNull(row)
        assertEquals("Important order", row?.get("description"))
    }

    @Test
    fun testServerRemovesTableNonDestructive() {
        val db = makeTestDB()

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
        installTestSchema(db, v1)
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
        installTestSchema(db, v2)

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
        installTestSchema(db, schema)

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
        assertCDCTriggers(db, "orders")

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
    fun testIncompatibleTypeRejectsWholeMultiTableMigrationWithoutStateChange() {
        val db = makeTestDB()

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
                ),
                SchemaTable(
                    tableName = "profiles",
                    pushPolicy = "owner_only",
                    updatedAtColumn = "updated_at",
                    deletedAtColumn = "deleted_at",
                    primaryKey = listOf("id"),
                    columns = listOf(
                        SchemaColumn(name = "id", dbType = "uuid", logicalType = "string", nullable = false, isPrimaryKey = true),
                        SchemaColumn(name = "display_name", dbType = "text", logicalType = "string", nullable = true, isPrimaryKey = false),
                        SchemaColumn(name = "updated_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "deleted_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = true, isPrimaryKey = false),
                    )
                )
            )
        )
        installTestSchema(db, v1)

        db.writeTransaction { rawDb ->
            rawDb.execSQL("ALTER TABLE orders ADD COLUMN local_note TEXT")
            rawDb.execSQL("CREATE TABLE app_settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            rawDb.execSQL("INSERT INTO app_settings (key, value) VALUES ('theme', 'dark')")
            SynchroMeta.setInt64(rawDb, MetaKey.CHECKPOINT, 42L)
            SynchroMeta.set(rawDb, MetaKey.SNAPSHOT_COMPLETE, "1")
        }
        db.execute(
            "INSERT INTO orders (id, score, local_note, updated_at) VALUES (?, ?, ?, ?)",
            arrayOf("ord-1", "high", "local-order-data", "2026-01-01T00:00:00Z"),
        )
        db.execute(
            "INSERT INTO profiles (id, display_name, updated_at) VALUES (?, ?, ?)",
            arrayOf("profile-1", "Ada", "2026-01-01T00:00:00Z"),
        )
        db.writeTransaction { rawDb ->
            rawDb.execSQL(
                "INSERT INTO _synchro_row_versions (table_name, record_id, server_version, row_checksum) VALUES (?, ?, ?, ?)",
                arrayOf("orders", "ord-1", "server-v1", "row-checksum-v1"),
            )
            rawDb.execSQL(
                "INSERT INTO _synchro_scopes (scope_id, cursor, checksum, generation, local_checksum) VALUES (?, ?, ?, ?, ?)",
                arrayOf("orders:user-1", "cursor-v1", "scope-checksum-v1", 3, "local-checksum-v1"),
            )
            rawDb.execSQL(
                "INSERT INTO _synchro_scope_rows (scope_id, table_name, record_id, checksum, generation) VALUES (?, ?, ?, ?, ?)",
                arrayOf("orders:user-1", "orders", "ord-1", "row-checksum-v1", 3),
            )
        }

        val stateQueries = listOf(
            "SELECT type, name, tbl_name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name",
            "SELECT * FROM orders ORDER BY id",
            "SELECT * FROM profiles ORDER BY id",
            "SELECT * FROM app_settings ORDER BY key",
            "SELECT * FROM _synchro_meta ORDER BY key",
            "SELECT * FROM _synchro_schema_archives ORDER BY schema_version, schema_hash",
            "SELECT * FROM _synchro_pending_changes ORDER BY local_order",
            "SELECT * FROM _synchro_mutation_values ORDER BY mutation_id, field_id",
            "SELECT * FROM _synchro_row_versions ORDER BY table_name, record_id",
            "SELECT * FROM _synchro_scopes ORDER BY scope_id",
            "SELECT * FROM _synchro_scope_rows ORDER BY scope_id, table_name, record_id",
        )
        val stateBefore = stateQueries.associateWith(db::query)

        val v2 = SchemaResponse(
            schemaVersion = 2,
            schemaHash = "v2",
            serverTime = "2026-01-01T12:00:00.000Z",
            tables = listOf(
                SchemaTable(
                    tableName = "profiles",
                    pushPolicy = "owner_only",
                    updatedAtColumn = "updated_at",
                    deletedAtColumn = "deleted_at",
                    primaryKey = listOf("id"),
                    columns = listOf(
                        SchemaColumn(name = "id", dbType = "uuid", logicalType = "string", nullable = false, isPrimaryKey = true),
                        SchemaColumn(name = "display_name", dbType = "text", logicalType = "string", nullable = true, isPrimaryKey = false),
                        SchemaColumn(name = "nickname", dbType = "text", logicalType = "string", nullable = true, isPrimaryKey = false),
                        SchemaColumn(name = "updated_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "deleted_at", dbType = "timestamp with time zone", logicalType = "datetime", nullable = true, isPrimaryKey = false),
                    )
                ),
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
        val error = assertThrows(SynchroError.InvalidResponse::class.java) {
            installTestSchema(
                db,
                schemaVersion = v2.schemaVersion,
                schemaHash = v2.schemaHash,
                tables = v2.localTables(),
                scopeCursorUpdates = mapOf("orders:user-1" to null),
                affectedScopes = listOf("orders:user-1"),
            )
        }
        assertTrue(error.details.contains("SQLite type"))

        stateQueries.forEach { query ->
            assertEquals("migration changed state for: $query", stateBefore.getValue(query), db.query(query))
        }
        assertEquals(0, db.query("SELECT name FROM pragma_table_info('profiles') WHERE name = 'nickname'").size)
        assertEquals("v1", db.readTransaction { SynchroMeta.get(it, MetaKey.SCHEMA_HASH) })
        assertEquals(2, db.query("SELECT mutation_id FROM _synchro_pending_changes").size)
        assertEquals("local-order-data", db.queryOne("SELECT local_note FROM orders WHERE id = 'ord-1'")?.get("local_note"))
    }

    @Test
    fun testPrimaryKeyShapeChangeIsRejectedWithoutReplacingRowsOrMetadata() {
        val db = makeTestDB()
        val v1 = SchemaResponse(
            schemaVersion = 1,
            schemaHash = "pk-v1",
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
                        SchemaColumn(name = "alternate_id", dbType = "uuid", logicalType = "string", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "updated_at", dbType = "timestamp", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "deleted_at", dbType = "timestamp", logicalType = "datetime", nullable = true, isPrimaryKey = false),
                    ),
                )
            ),
        )
        installTestSchema(db, v1)
        db.execute(
            "INSERT INTO orders (id, alternate_id, updated_at) VALUES (?, ?, ?)",
            arrayOf("order-1", "alternate-1", "2026-01-01T00:00:00Z"),
        )
        val rowsBefore = db.query("SELECT * FROM orders")
        val metadataBefore = db.query("SELECT * FROM _synchro_meta ORDER BY key")
        val queueBefore = db.query("SELECT * FROM _synchro_pending_changes ORDER BY local_order")

        val v2 = SchemaResponse(
            schemaVersion = 2,
            schemaHash = "pk-v2",
            serverTime = "2026-01-01T12:00:00.000Z",
            tables = listOf(
                SchemaTable(
                    tableName = "orders",
                    pushPolicy = "owner_only",
                    updatedAtColumn = "updated_at",
                    deletedAtColumn = "deleted_at",
                    primaryKey = listOf("alternate_id"),
                    columns = listOf(
                        SchemaColumn(name = "id", dbType = "uuid", logicalType = "string", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "alternate_id", dbType = "uuid", logicalType = "string", nullable = false, isPrimaryKey = true),
                        SchemaColumn(name = "updated_at", dbType = "timestamp", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "deleted_at", dbType = "timestamp", logicalType = "datetime", nullable = true, isPrimaryKey = false),
                    ),
                )
            ),
        )

        val error = assertThrows(SynchroError.InvalidResponse::class.java) {
            installTestSchema(db, v2)
        }
        assertTrue(error.details.contains("primary key"))
        assertEquals(rowsBefore, db.query("SELECT * FROM orders"))
        assertEquals(metadataBefore, db.query("SELECT * FROM _synchro_meta ORDER BY key"))
        assertEquals(queueBefore, db.query("SELECT * FROM _synchro_pending_changes ORDER BY local_order"))
        assertEquals(0, db.query("SELECT 1 FROM _synchro_schema_archives WHERE schema_hash = 'pk-v2'").size)
    }

    @Test
    fun testAdditiveDdlFailureRollsBackEarlierDdlAndTargetMetadata() {
        val db = makeTestDB()
        val baseTables = makeManifest(
            listOf(
                TableSchema(
                    name = "orders", primaryKey = listOf("id"), updatedAtColumn = "updated_at",
                    deletedAtColumn = "deleted_at", composition = CompositionClass.SINGLE_SCOPE,
                    columns = listOf(
                        ColumnSchema(name = "id", typeName = "string", nullable = false),
                        ColumnSchema(name = "updated_at", typeName = "datetime", nullable = false),
                        ColumnSchema(name = "deleted_at", typeName = "datetime", nullable = true),
                    ), indexes = null,
                ),
                TableSchema(
                    name = "items", primaryKey = listOf("id"), updatedAtColumn = "updated_at",
                    deletedAtColumn = "deleted_at", composition = CompositionClass.SINGLE_SCOPE,
                    columns = listOf(
                        ColumnSchema(name = "id", typeName = "string", nullable = false),
                        ColumnSchema(name = "updated_at", typeName = "datetime", nullable = false),
                        ColumnSchema(name = "deleted_at", typeName = "datetime", nullable = true),
                    ), indexes = null,
                ),
            )
        ).localTables()
        installTestSchema(db, 1, "ddl-v1", baseTables)

        val validColumn = LocalSchemaColumn(
            fieldID = "field-orders-notes", name = "notes", logicalType = "string",
            nullable = true, writable = true, isPrimaryKey = false,
        )
        val invalidColumn = LocalSchemaColumn(
            fieldID = "field-items-broken", name = "broken", logicalType = "string",
            nullable = false, writable = true, sqliteDefaultSQL = "'", isPrimaryKey = false,
        )
        val targetTables = listOf(
            baseTables[0].copy(columns = baseTables[0].columns + validColumn),
            baseTables[1].copy(columns = baseTables[1].columns + invalidColumn),
        )
        val schemaBefore = db.query(
            "SELECT type, name, tbl_name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        )
        val metadataBefore = db.query("SELECT * FROM _synchro_meta ORDER BY key")
        val archivesBefore = db.query("SELECT * FROM _synchro_schema_archives ORDER BY schema_version, schema_hash")

        assertThrows(android.database.sqlite.SQLiteException::class.java) {
            installTestSchema(db, 2, "ddl-v2", targetTables)
        }

        assertEquals(
            schemaBefore,
            db.query("SELECT type, name, tbl_name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"),
        )
        assertEquals(metadataBefore, db.query("SELECT * FROM _synchro_meta ORDER BY key"))
        assertEquals(archivesBefore, db.query("SELECT * FROM _synchro_schema_archives ORDER BY schema_version, schema_hash"))
        assertEquals(0, db.query("SELECT name FROM pragma_table_info('orders') WHERE name = 'notes'").size)
        assertEquals(0, db.query("SELECT name FROM pragma_table_info('items') WHERE name = 'broken'").size)
    }

    @Test
    fun testConnectScopeCursorUpdatesAndAffectedScopesAreApplied() {
        val db = makeTestDB()
        val schema = SchemaResponse(
            schemaVersion = 1,
            schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
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
                        SchemaColumn(name = "updated_at", dbType = "timestamp", logicalType = "datetime", nullable = false, isPrimaryKey = false),
                        SchemaColumn(name = "deleted_at", dbType = "timestamp", logicalType = "datetime", nullable = true, isPrimaryKey = false),
                    ),
                )
            ),
        )
        val tables = schema.localTables()
        installTestSchema(db, 1, PROTOCOL_TEST_SCHEMA_HASH, tables)
        db.writeTransaction { connection ->
            SynchroMeta.upsertScope(connection, "orders:existing", "old", "old")
            SynchroMeta.upsertScope(connection, "orders:affected", "old", "old")
        }

        installTestSchema(
            db,
            schemaVersion = 2,
            schemaHash = "1".repeat(64),
            tables = tables,
            scopeCursorUpdates = mapOf("orders:existing" to "new-current-schema", "orders:affected" to null),
            affectedScopes = listOf("orders:affected"),
        )

        val existing = db.readTransaction { SynchroMeta.getScope(it, "orders:existing") }
        val affected = db.readTransaction { SynchroMeta.getScope(it, "orders:affected") }
        assertEquals("new-current-schema", existing?.cursor)
        assertNull(existing?.checksum)
        assertNull(affected?.cursor)
        assertEquals(1L, affected?.generation)
    }

    @Test
    fun testConnectCursorUpdateRecomputesRetainedProvenanceForTargetFieldIDs() {
        fun localTable(fieldSuffix: String): LocalSchemaTable {
            val columns = listOf(
                LocalSchemaColumn(
                    fieldID = "field-id-$fieldSuffix", name = "id", logicalType = "string",
                    nullable = false, writable = false, isPrimaryKey = true,
                ),
                LocalSchemaColumn(
                    fieldID = "field-title-$fieldSuffix", name = "title", logicalType = "string",
                    nullable = false, writable = true, isPrimaryKey = false,
                ),
                LocalSchemaColumn(
                    fieldID = "field-updated-$fieldSuffix", name = "updated_at", logicalType = "datetime",
                    nullable = false, writable = false, isPrimaryKey = false,
                ),
                LocalSchemaColumn(
                    fieldID = "field-deleted-$fieldSuffix", name = "deleted_at", logicalType = "datetime",
                    nullable = true, writable = false, isPrimaryKey = false,
                ),
            )
            return LocalSchemaTable(
                tableID = "table-orders",
                relationID = "relation-orders",
                tableName = "orders",
                primaryKeyFieldID = "field-id-$fieldSuffix",
                updatedAtFieldID = "field-updated-$fieldSuffix",
                deletedAtFieldID = "field-deleted-$fieldSuffix",
                updatedAtColumn = "updated_at",
                deletedAtColumn = "deleted_at",
                composition = CompositionClass.SINGLE_SCOPE,
                primaryKey = listOf("id"),
                columns = columns,
            )
        }

        val db = makeTestDB()
        val oldTable = localTable("old")
        val targetTable = localTable("target")
        val oldHash = "0".repeat(64)
        val targetHash = "1".repeat(64)
        val scopeID = "orders:user-1"
        val serverVersion = "server-version-1"
        installTestSchema(db, 1, oldHash, listOf(oldTable))

        val oldRow = JsonObject(
            mapOf(
                "field-id-old" to JsonPrimitive("r1"),
                "field-title-old" to JsonPrimitive("retained"),
                "field-updated-old" to JsonPrimitive("2026-01-01T00:00:00.000000Z"),
                "field-deleted-old" to JsonNull,
            )
        )
        val oldDigest = Integrity.rowDigest(
            oldHash,
            oldTable,
            JsonObject(mapOf("field-id-old" to JsonPrimitive("r1"))),
            oldRow,
            serverVersion,
        )
        val protectedScopeID = "orders:user-2"
        val additionalDigests = listOf("edited", "deleted", "other-retained").associateWith { recordID ->
            Integrity.rowDigest(
                oldHash, oldTable, JsonObject(mapOf("field-id-old" to JsonPrimitive(recordID))),
                JsonObject(oldRow + ("field-id-old" to JsonPrimitive(recordID))), serverVersion,
            )
        }
        val sourceScopeChecksum = Json.encodeToString(
            Integrity.scopeDigest(oldHash, protectedScopeID, additionalDigests.values.map { it.identity to it.checksum }),
        )
        db.writeSyncLockedTransaction { connection ->
            connection.execSQL(
                "INSERT INTO orders (id, title, updated_at, deleted_at) VALUES (?, ?, ?, NULL)",
                arrayOf("r1", "retained", "2026-01-01T00:00:00.000000Z"),
            )
            SynchroMeta.upsertRowVersion(
                connection,
                "orders",
                "r1",
                serverVersion,
                oldDigest.checksum,
            )
            SynchroMeta.upsertScope(connection, scopeID, "old-cursor", null)
            SynchroMeta.upsertScopeRow(
                connection,
                scopeID,
                "orders",
                "r1",
                oldDigest.checksum.digest,
                0,
            )
            SynchroMeta.upsertScope(connection, protectedScopeID, "protected-old-cursor", sourceScopeChecksum, 3, sourceScopeChecksum)
            additionalDigests.forEach { (recordID, digest) ->
                connection.execSQL(
                    "INSERT INTO orders (id, title, updated_at, deleted_at) VALUES (?, ?, ?, NULL)",
                    arrayOf(recordID, "retained", "2026-01-01T00:00:00.000000Z"),
                )
                SynchroMeta.upsertRowVersion(connection, "orders", recordID, serverVersion, digest.checksum)
                SynchroMeta.upsertScopeRow(connection, protectedScopeID, "orders", recordID, digest.checksum.digest, 3)
            }
        }
        db.execute("UPDATE orders SET title = 'visible-local-edit' WHERE id = 'edited'")
        db.execute("DELETE FROM orders WHERE id = 'deleted'")
        val pending = db.query("SELECT * FROM _synchro_pending_changes ORDER BY local_order")
        assertEquals(listOf("update", "delete"), pending.map { it["operation"] })
        val mutationValues = db.query("SELECT * FROM _synchro_mutation_values ORDER BY mutation_id, field_id")
        val versions = db.query("SELECT * FROM _synchro_row_versions ORDER BY table_name, record_id")
        val protectedProvenanceQuery = "SELECT * FROM _synchro_scope_rows WHERE scope_id = ? AND record_id IN ('edited', 'deleted') ORDER BY record_id"
        val protectedProvenance = db.query(protectedProvenanceQuery, arrayOf(protectedScopeID))
        val visibleRows = db.query("SELECT * FROM orders ORDER BY id")
        assertEquals("visible-local-edit", db.queryOne("SELECT title FROM orders WHERE id = 'edited'")?.get("title"))
        assertNotNull(db.queryOne("SELECT deleted_at FROM orders WHERE id = 'deleted'")?.get("deleted_at"))

        installTestSchema(
            db,
            schemaVersion = 2,
            schemaHash = targetHash,
            tables = listOf(targetTable),
            scopeCursorUpdates = mapOf(scopeID to "target-cursor", protectedScopeID to "protected-target-cursor"),
        )

        val targetRow = JsonObject(
            mapOf(
                "field-id-target" to JsonPrimitive("r1"),
                "field-title-target" to JsonPrimitive("retained"),
                "field-updated-target" to JsonPrimitive("2026-01-01T00:00:00.000000Z"),
                "field-deleted-target" to JsonNull,
            )
        )
        val targetDigest = Integrity.rowDigest(
            targetHash,
            targetTable,
            JsonObject(mapOf("field-id-target" to JsonPrimitive("r1"))),
            targetRow,
            serverVersion,
        )
        val targetScopeDigest = Integrity.scopeDigest(
            targetHash,
            scopeID,
            listOf(targetDigest.identity to targetDigest.checksum),
        )
        assertNotEquals(oldDigest.checksum, targetDigest.checksum)

        val storedRows = db.readTransaction { SynchroMeta.getScopeRowChecksums(it, scopeID) }
        val storedScope = db.readTransaction { SynchroMeta.getScope(it, scopeID) }
        assertEquals(targetDigest.checksum.digest, storedRows.single().checksum)
        assertEquals("target-cursor", storedScope?.cursor)
        assertEquals(
            targetScopeDigest,
            Json.decodeFromString<ChecksumObject>(storedScope!!.localChecksum),
        )
        val unprotectedDigest = Integrity.rowDigest(
            targetHash, targetTable, JsonObject(mapOf("field-id-target" to JsonPrimitive("other-retained"))),
            JsonObject(targetRow + ("field-id-target" to JsonPrimitive("other-retained"))), serverVersion,
        )
        val mixedScopeRows = db.readTransaction { SynchroMeta.getScopeRowChecksums(it, protectedScopeID) }
        assertEquals(unprotectedDigest.checksum.digest, mixedScopeRows.single { it.recordID == "other-retained" }.checksum)
        assertEquals(protectedProvenance, db.query(protectedProvenanceQuery, arrayOf(protectedScopeID)))
        assertEquals(pending, db.query("SELECT * FROM _synchro_pending_changes ORDER BY local_order"))
        assertEquals(mutationValues, db.query("SELECT * FROM _synchro_mutation_values ORDER BY mutation_id, field_id"))
        assertEquals(versions, db.query("SELECT * FROM _synchro_row_versions ORDER BY table_name, record_id"))
        assertEquals(visibleRows, db.query("SELECT * FROM orders ORDER BY id"))
        val protectedScope = db.readTransaction { SynchroMeta.getScope(it, protectedScopeID) }
        assertEquals("protected-target-cursor", protectedScope?.cursor)
        assertEquals(3L, protectedScope?.generation)
        assertNull(protectedScope?.checksum)
        assertEquals("", protectedScope?.localChecksum)
        assertEquals(0L, storedScope.generation)
        assertEquals(0, db.query("SELECT * FROM _synchro_rebuild_attempts").size)
    }

    @Test
    fun testSchemaResetKeepsOnlyProtectedRowsThatTheTightenedTargetCanHold() {
        val db = makeTestDB()
        val sourceDraft = protocolOrdersSchemaManifest(includeNotes = true)
        val source = sourceDraft.copy(schemaHash = Integrity.schemaManifestHash(sourceDraft))
        installTestSchema(db, 1, source.schemaHash, source.localTables())
        val draft = protocolOrdersSchemaManifest(
            includeNotes = true,
            schemaVersion = 2,
            parentSchema = SchemaRef(1, source.schemaHash),
            transitionClass = "class_4",
            compatibilityFloor = 2,
        ).let { manifest ->
            manifest.copy(
                tables = manifest.tables.map { table ->
                    table.copy(fields = table.fields.map { if (it.fieldID == "field-notes") it.copy(nullable = false) else it })
                },
            )
        }
        val target = draft.copy(schemaHash = Integrity.schemaManifestHash(draft))
        db.execute(
            """
            INSERT INTO orders (id, ship_address, user_id, updated_at, notes) VALUES
                ('fits', 'kept address', 'u1', '2026-01-01T00:00:00.000000Z', 'kept note'),
                ('null-note', 'local address', 'u1', '2026-01-01T00:00:00.000000Z', NULL)
            """.trimIndent(),
        )
        val pendingBefore = ChangeTracker(db).inspectPendingMutations()
        assertEquals(setOf("fits", "null-note"), pendingBefore.map { it.recordID }.toSet())

        val manager = SchemaManager(db)
        manager.prepareConnectMigration(
            response = ConnectResponse(
                serverTime = "2026-01-02T00:00:00.000000Z",
                protocolVersion = 3,
                clientGeneration = 1,
                scopeSetVersion = 0,
                schema = SchemaDescriptor(target.schemaVersion, target.schemaHash, SchemaAction.REPLACE),
                scopes = ScopeAssignmentDelta(emptyList(), emptyList()),
                scopeCursorUpdates = emptyMap(),
                schemaDefinition = target,
            ),
            targetTables = target.localTables(),
            resetMaterialization = true,
        )
        db.writeSyncLockedTransaction { manager.applyPreparedMigrationInTransaction(it) }

        val rows = db.query("SELECT id, ship_address, notes FROM orders ORDER BY id")
        assertEquals(listOf("fits"), rows.map { it["id"] })
        assertEquals("kept address", rows.single()["ship_address"])
        assertEquals("kept note", rows.single()["notes"])
        // The NULL note cannot exist in the target shape. Its row waits for the
        // rebuild to install the server row, and its intent stays inspectable.
        assertEquals(pendingBefore, ChangeTracker(db).inspectPendingMutations())
    }
}
