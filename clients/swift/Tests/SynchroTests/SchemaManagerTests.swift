import XCTest
import GRDB
@testable import Synchro

final class SchemaManagerTests: XCTestCase {
    private func makeTestDB() throws -> SynchroDatabase {
        let tmpDir = NSTemporaryDirectory()
        let path = (tmpDir as NSString).appendingPathComponent("synchro_test_\(UUID().uuidString).sqlite")
        return try SynchroDatabase(path: path)
    }

    // MARK: - Helper builders

    private func makeColumn(
        name: String,
        dbType: String = "text",
        logicalType: String = "string",
        nullable: Bool = true,
        isPrimaryKey: Bool = false
    ) -> SchemaColumn {
        SchemaColumn(name: name, dbType: dbType, logicalType: logicalType, nullable: nullable, isPrimaryKey: isPrimaryKey)
    }

    private func makeTable(
        name: String,
        columns: [SchemaColumn]
    ) -> SchemaTable {
        SchemaTable(
            tableName: name,
            pushPolicy: "owner_only",
            updatedAtColumn: "updated_at",
            deletedAtColumn: "deleted_at",
            primaryKey: ["id"],
            columns: columns
        )
    }

    private func makeSchema(version: Int64, hash: String, tables: [SchemaTable]) -> SchemaResponse {
        SchemaResponse(schemaVersion: version, schemaHash: hash, serverTime: Date(), tables: tables)
    }

    private func makeManifest(tables: [VNextTableSchema]) -> VNextSchemaManifest {
        VNextSchemaManifest(tables: tables)
    }

    private func columnNames(db: SynchroDatabase, table: String) throws -> Set<String> {
        let rows = try db.query("PRAGMA table_info(\(table))", params: nil)
        return Set(rows.map { $0["name"] as String })
    }

    private func triggerCount(db: SynchroDatabase, table: String) throws -> Int {
        let triggers = try db.query(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '%\(table)'",
            params: nil
        )
        return triggers.count
    }

    private func tableExists(db: SynchroDatabase, name: String) throws -> Bool {
        let rows = try db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='\(name)'", params: nil)
        return rows.count == 1
    }

    // MARK: - Standard columns used across tests

    private var standardColumns: [SchemaColumn] {
        [
            makeColumn(name: "id", dbType: "uuid", logicalType: "string", nullable: false, isPrimaryKey: true),
            makeColumn(name: "updated_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: false),
            makeColumn(name: "deleted_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: true),
        ]
    }

    // MARK: - 1. testCreateSyncedTables

    func testCreateSyncedTables() throws {
        let db = try makeTestDB()
        let manager = SchemaManager(database: db)
        let schema = SchemaResponse(
            schemaVersion: 1,
            schemaHash: "abc123",
            serverTime: Date(),
            tables: [
                SchemaTable(
                    tableName: "orders",
                    pushPolicy: "owner_only",
                    updatedAtColumn: "updated_at",
                    deletedAtColumn: "deleted_at",
                    primaryKey: ["id"],
                    columns: [
                        SchemaColumn(name: "id", dbType: "uuid", logicalType: "string", nullable: false, isPrimaryKey: true),
                        SchemaColumn(name: "ship_address", dbType: "text", logicalType: "string", nullable: true, isPrimaryKey: false),
                        SchemaColumn(name: "updated_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: false, isPrimaryKey: false),
                        SchemaColumn(name: "deleted_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: true, isPrimaryKey: false),
                    ]
                )
            ]
        )

        try manager.createSyncedTables(schema: schema)

        // Verify table exists
        let rows = try db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'", params: nil)
        XCTAssertEqual(rows.count, 1)

        // Verify triggers exist
        let triggers = try db.query("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_synchro_cdc_%orders'", params: nil)
        XCTAssertEqual(triggers.count, 3)
    }

    func testReconcileLocalSchemaFromPortableManifest() throws {
        let db = try makeTestDB()
        let manager = SchemaManager(database: db)
        let manifest = makeManifest(tables: [
            VNextTableSchema(
                name: "workouts",
                primaryKey: ["id"],
                updatedAtColumn: "updated_at",
                deletedAtColumn: "deleted_at",
                composition: .singleScope,
                columns: [
                    VNextColumnSchema(name: "id", type: "uuid", nullable: false),
                    VNextColumnSchema(name: "name", type: "text", nullable: false),
                    VNextColumnSchema(name: "updated_at", type: "timestamp", nullable: false),
                    VNextColumnSchema(name: "deleted_at", type: "timestamp", nullable: true),
                ],
                indexes: nil
            )
        ])

        let tables = try manifest.localTables()
        try manager.reconcileLocalSchema(schemaVersion: 7, schemaHash: "portable-v1", tables: tables)

        XCTAssertTrue(try tableExists(db: db, name: "workouts"))
        XCTAssertEqual(try triggerCount(db: db, table: "workouts"), 3)

        let schemaState = try db.readTransaction { grdb in
            (
                try SynchroMeta.getInt64(grdb, key: .schemaVersion),
                try SynchroMeta.get(grdb, key: .schemaHash)
            )
        }
        XCTAssertEqual(schemaState.0, 7)
        XCTAssertEqual(schemaState.1, "portable-v1")
    }

    func testReconcileLocalSchemaMigratesAdditiveManifestChange() throws {
        let db = try makeTestDB()
        let manager = SchemaManager(database: db)

        let v1 = makeManifest(tables: [
            VNextTableSchema(
                name: "workouts",
                primaryKey: ["id"],
                updatedAtColumn: "updated_at",
                deletedAtColumn: "deleted_at",
                composition: .singleScope,
                columns: [
                    VNextColumnSchema(name: "id", type: "uuid", nullable: false),
                    VNextColumnSchema(name: "name", type: "text", nullable: false),
                    VNextColumnSchema(name: "updated_at", type: "timestamp", nullable: false),
                    VNextColumnSchema(name: "deleted_at", type: "timestamp", nullable: true),
                ],
                indexes: nil
            )
        ])
        try manager.reconcileLocalSchema(schemaVersion: 1, schemaHash: "portable-v1", tables: try v1.localTables())

        _ = try db.execute(
            "INSERT INTO workouts (id, name, updated_at) VALUES ('w-1', 'Morning Run', '2026-01-01T00:00:00Z')",
            params: nil
        )

        let v2 = makeManifest(tables: [
            VNextTableSchema(
                name: "workouts",
                primaryKey: ["id"],
                updatedAtColumn: "updated_at",
                deletedAtColumn: "deleted_at",
                composition: .singleScope,
                columns: [
                    VNextColumnSchema(name: "id", type: "uuid", nullable: false),
                    VNextColumnSchema(name: "name", type: "text", nullable: false),
                    VNextColumnSchema(name: "notes", type: "text", nullable: true),
                    VNextColumnSchema(name: "updated_at", type: "timestamp", nullable: false),
                    VNextColumnSchema(name: "deleted_at", type: "timestamp", nullable: true),
                ],
                indexes: nil
            )
        ])
        try manager.reconcileLocalSchema(schemaVersion: 2, schemaHash: "portable-v2", tables: try v2.localTables())

        XCTAssertTrue(try columnNames(db: db, table: "workouts").contains("notes"))
        let row = try db.queryOne("SELECT name, notes FROM workouts WHERE id = ?", params: ["w-1"])
        XCTAssertNotNil(row)
        XCTAssertEqual(row?["name"] as? String, "Morning Run")
        XCTAssertNil(row?["notes"])
    }

    // MARK: - 2. testMigrateSchemaAddsColumn

    func testMigrateSchemaAddsColumn() throws {
        let db = try makeTestDB()
        let manager = SchemaManager(database: db)

        let v1 = SchemaResponse(
            schemaVersion: 1,
            schemaHash: "v1",
            serverTime: Date(),
            tables: [
                SchemaTable(
                    tableName: "orders",
                    pushPolicy: "owner_only",
                    updatedAtColumn: "updated_at",
                    deletedAtColumn: "deleted_at",
                    primaryKey: ["id"],
                    columns: [
                        SchemaColumn(name: "id", dbType: "uuid", logicalType: "string", nullable: false, isPrimaryKey: true),
                        SchemaColumn(name: "updated_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: false, isPrimaryKey: false),
                        SchemaColumn(name: "deleted_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: true, isPrimaryKey: false),
                    ]
                )
            ]
        )
        try manager.createSyncedTables(schema: v1)

        let v2 = SchemaResponse(
            schemaVersion: 2,
            schemaHash: "v2",
            serverTime: Date(),
            tables: [
                SchemaTable(
                    tableName: "orders",
                    pushPolicy: "owner_only",
                    updatedAtColumn: "updated_at",
                    deletedAtColumn: "deleted_at",
                    primaryKey: ["id"],
                    columns: [
                        SchemaColumn(name: "id", dbType: "uuid", logicalType: "string", nullable: false, isPrimaryKey: true),
                        SchemaColumn(name: "description", dbType: "text", logicalType: "string", nullable: true, isPrimaryKey: false),
                        SchemaColumn(name: "updated_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: false, isPrimaryKey: false),
                        SchemaColumn(name: "deleted_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: true, isPrimaryKey: false),
                    ]
                )
            ]
        )
        try manager.migrateSchema(newSchema: v2)

        // Verify new column exists
        let row = try db.execute("INSERT INTO orders (id, description, updated_at) VALUES ('test-1', 'hello', '2026-01-01T00:00:00Z')", params: nil)
        XCTAssertEqual(row.rowsAffected, 1)
    }

    // MARK: - 3. testDropSyncedTables

    func testDropSyncedTables() throws {
        let db = try makeTestDB()
        let manager = SchemaManager(database: db)
        let schema = SchemaResponse(
            schemaVersion: 1,
            schemaHash: "abc123",
            serverTime: Date(),
            tables: [
                SchemaTable(
                    tableName: "orders",
                    pushPolicy: "owner_only",
                    updatedAtColumn: "updated_at",
                    deletedAtColumn: "deleted_at",
                    primaryKey: ["id"],
                    columns: [
                        SchemaColumn(name: "id", dbType: "uuid", logicalType: "string", nullable: false, isPrimaryKey: true),
                        SchemaColumn(name: "ship_address", dbType: "text", logicalType: "string", nullable: true, isPrimaryKey: false),
                        SchemaColumn(name: "updated_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: false, isPrimaryKey: false),
                        SchemaColumn(name: "deleted_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: true, isPrimaryKey: false),
                    ]
                )
            ]
        )

        try manager.createSyncedTables(schema: schema)

        // Verify table and triggers exist
        let tables = try db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'", params: nil)
        XCTAssertEqual(tables.count, 1)
        let triggers = try db.query("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_synchro_cdc_%orders'", params: nil)
        XCTAssertEqual(triggers.count, 3)

        // Drop
        try manager.dropSyncedTables(schema: schema)

        // Verify table and triggers are gone
        let tablesAfter = try db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'", params: nil)
        XCTAssertEqual(tablesAfter.count, 0)
        let triggersAfter = try db.query("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_synchro_cdc_%orders'", params: nil)
        XCTAssertEqual(triggersAfter.count, 0)
    }

    // MARK: - 4. testLocalOnlyTablesSurviveSchemaMigration

    func testLocalOnlyTablesSurviveSchemaMigration() throws {
        let db = try makeTestDB()
        let manager = SchemaManager(database: db)

        // Create a synced table via schema
        let v1 = makeSchema(version: 1, hash: "v1", tables: [
            makeTable(name: "orders", columns: standardColumns),
        ])
        try manager.createSyncedTables(schema: v1)

        // Create a local-only table with data using raw SQL
        _ = try db.execute(
            "CREATE TABLE app_settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
            params: nil
        )
        _ = try db.execute(
            "INSERT INTO app_settings (key, value) VALUES ('theme', 'dark')",
            params: nil
        )
        _ = try db.execute(
            "INSERT INTO app_settings (key, value) VALUES ('lang', 'en')",
            params: nil
        )

        // Migrate to v2 -- server schema does NOT include app_settings
        let v2 = makeSchema(version: 2, hash: "v2", tables: [
            makeTable(name: "orders", columns: standardColumns),
        ])
        try manager.migrateSchema(newSchema: v2)

        // Verify local-only table still exists
        XCTAssertTrue(try tableExists(db: db, name: "app_settings"))

        // Verify data is preserved
        let rows = try db.query("SELECT key, value FROM app_settings ORDER BY key", params: nil)
        XCTAssertEqual(rows.count, 2)
        XCTAssertEqual(rows[0]["key"] as String, "lang")
        XCTAssertEqual(rows[0]["value"] as String, "en")
        XCTAssertEqual(rows[1]["key"] as String, "theme")
        XCTAssertEqual(rows[1]["value"] as String, "dark")
    }

    // MARK: - 5. testSyncedTableExtraColumnsSurviveMigration

    func testSyncedTableExtraColumnsSurviveMigration() throws {
        let db = try makeTestDB()
        let manager = SchemaManager(database: db)

        // Create synced table with server schema v1
        let v1 = makeSchema(version: 1, hash: "v1", tables: [
            makeTable(name: "orders", columns: standardColumns),
        ])
        try manager.createSyncedTables(schema: v1)

        // Client adds an extra local-only column via raw SQL
        _ = try db.execute("ALTER TABLE orders ADD COLUMN extra_data TEXT", params: nil)
        _ = try db.execute(
            "INSERT INTO orders (id, updated_at, extra_data) VALUES ('o1', '2026-01-01T00:00:00Z', 'local-stuff')",
            params: nil
        )

        // Migrate to v2 -- server schema does NOT include extra_data
        let v2 = makeSchema(version: 2, hash: "v2", tables: [
            makeTable(name: "orders", columns: standardColumns),
        ])
        try manager.migrateSchema(newSchema: v2)

        // Verify extra_data column still exists
        let cols = try columnNames(db: db, table: "orders")
        XCTAssertTrue(cols.contains("extra_data"), "extra_data column should survive migration")

        // Verify data is preserved
        let rows = try db.query("SELECT id, extra_data FROM orders WHERE id = 'o1'", params: nil)
        XCTAssertEqual(rows.count, 1)
        XCTAssertEqual(rows[0]["extra_data"] as String, "local-stuff")
    }

    // MARK: - 6. testServerAddsNewColumnNonDestructive

    func testServerAddsNewColumnNonDestructive() throws {
        let db = try makeTestDB()
        let manager = SchemaManager(database: db)

        // Create v1 schema, insert data
        let v1 = makeSchema(version: 1, hash: "v1", tables: [
            makeTable(name: "orders", columns: standardColumns),
        ])
        try manager.createSyncedTables(schema: v1)

        _ = try db.execute(
            "INSERT INTO orders (id, updated_at) VALUES ('o1', '2026-01-01T00:00:00Z')",
            params: nil
        )
        _ = try db.execute(
            "INSERT INTO orders (id, updated_at) VALUES ('o2', '2026-01-02T00:00:00Z')",
            params: nil
        )

        // Migrate to v2 that adds a "description" column
        var v2Columns = standardColumns
        v2Columns.insert(
            makeColumn(name: "description", dbType: "text", logicalType: "string", nullable: true),
            at: 1
        )
        let v2 = makeSchema(version: 2, hash: "v2", tables: [
            makeTable(name: "orders", columns: v2Columns),
        ])
        try manager.migrateSchema(newSchema: v2)

        // Verify new column exists
        let cols = try columnNames(db: db, table: "orders")
        XCTAssertTrue(cols.contains("description"), "description column should be added by migration")

        // Verify old data is preserved
        let rows = try db.query("SELECT id FROM orders ORDER BY id", params: nil)
        XCTAssertEqual(rows.count, 2)
        XCTAssertEqual(rows[0]["id"] as String, "o1")
        XCTAssertEqual(rows[1]["id"] as String, "o2")

        // Verify new column is usable
        let result = try db.execute(
            "UPDATE orders SET description = 'test-desc' WHERE id = 'o1'",
            params: nil
        )
        XCTAssertEqual(result.rowsAffected, 1)
    }

    // MARK: - 7. testServerAddsNewTable

    func testServerAddsNewTable() throws {
        let db = try makeTestDB()
        let manager = SchemaManager(database: db)

        // Create v1 with one table
        let v1 = makeSchema(version: 1, hash: "v1", tables: [
            makeTable(name: "orders", columns: standardColumns),
        ])
        try manager.createSyncedTables(schema: v1)

        _ = try db.execute(
            "INSERT INTO orders (id, updated_at) VALUES ('o1', '2026-01-01T00:00:00Z')",
            params: nil
        )

        // Migrate to v2 that adds a second table "items"
        let itemColumns: [SchemaColumn] = [
            makeColumn(name: "id", dbType: "uuid", logicalType: "string", nullable: false, isPrimaryKey: true),
            makeColumn(name: "order_id", dbType: "uuid", logicalType: "string", nullable: false),
            makeColumn(name: "quantity", dbType: "integer", logicalType: "int", nullable: false),
            makeColumn(name: "updated_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: false),
            makeColumn(name: "deleted_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: true),
        ]
        let v2 = makeSchema(version: 2, hash: "v2", tables: [
            makeTable(name: "orders", columns: standardColumns),
            makeTable(name: "items", columns: itemColumns),
        ])
        try manager.migrateSchema(newSchema: v2)

        // Verify first table is unchanged
        XCTAssertTrue(try tableExists(db: db, name: "orders"))
        let orderRows = try db.query("SELECT id FROM orders", params: nil)
        XCTAssertEqual(orderRows.count, 1)
        XCTAssertEqual(orderRows[0]["id"] as String, "o1")

        // Verify second table exists
        XCTAssertTrue(try tableExists(db: db, name: "items"))

        // Verify triggers exist for the new table
        let triggers = try triggerCount(db: db, table: "items")
        XCTAssertEqual(triggers, 3, "items table should have 3 CDC triggers")

        // Verify new table is usable
        let result = try db.execute(
            "INSERT INTO items (id, order_id, quantity, updated_at) VALUES ('i1', 'o1', 5, '2026-01-01T00:00:00Z')",
            params: nil
        )
        XCTAssertEqual(result.rowsAffected, 1)
    }

    // MARK: - 8. testServerRemovesColumnNonDestructive

    func testServerRemovesColumnNonDestructive() throws {
        let db = try makeTestDB()
        let manager = SchemaManager(database: db)

        // Create v1 with columns [id, description, updated_at, deleted_at]
        let v1Columns: [SchemaColumn] = [
            makeColumn(name: "id", dbType: "uuid", logicalType: "string", nullable: false, isPrimaryKey: true),
            makeColumn(name: "description", dbType: "text", logicalType: "string", nullable: true),
            makeColumn(name: "updated_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: false),
            makeColumn(name: "deleted_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: true),
        ]
        let v1 = makeSchema(version: 1, hash: "v1", tables: [
            makeTable(name: "orders", columns: v1Columns),
        ])
        try manager.createSyncedTables(schema: v1)

        _ = try db.execute(
            "INSERT INTO orders (id, description, updated_at) VALUES ('o1', 'important note', '2026-01-01T00:00:00Z')",
            params: nil
        )

        // Migrate to v2 that removes "description" from server schema
        let v2 = makeSchema(version: 2, hash: "v2", tables: [
            makeTable(name: "orders", columns: standardColumns),
        ])
        try manager.migrateSchema(newSchema: v2)

        // Verify "description" column still exists locally
        let cols = try columnNames(db: db, table: "orders")
        XCTAssertTrue(cols.contains("description"), "description column should be preserved when server removes it")

        // Verify data is preserved
        let rows = try db.query("SELECT id, description FROM orders WHERE id = 'o1'", params: nil)
        XCTAssertEqual(rows.count, 1)
        XCTAssertEqual(rows[0]["description"] as String, "important note")
    }

    // MARK: - 9. testServerRemovesTableNonDestructive

    func testServerRemovesTableNonDestructive() throws {
        let db = try makeTestDB()
        let manager = SchemaManager(database: db)

        // Create v1 with two tables: orders and items
        let itemColumns: [SchemaColumn] = [
            makeColumn(name: "id", dbType: "uuid", logicalType: "string", nullable: false, isPrimaryKey: true),
            makeColumn(name: "order_id", dbType: "uuid", logicalType: "string", nullable: false),
            makeColumn(name: "updated_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: false),
            makeColumn(name: "deleted_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: true),
        ]
        let v1 = makeSchema(version: 1, hash: "v1", tables: [
            makeTable(name: "orders", columns: standardColumns),
            makeTable(name: "items", columns: itemColumns),
        ])
        try manager.createSyncedTables(schema: v1)

        _ = try db.execute(
            "INSERT INTO orders (id, updated_at) VALUES ('o1', '2026-01-01T00:00:00Z')",
            params: nil
        )
        _ = try db.execute(
            "INSERT INTO items (id, order_id, updated_at) VALUES ('i1', 'o1', '2026-01-01T00:00:00Z')",
            params: nil
        )

        // Migrate to v2 with only "orders" -- server removes "items"
        let v2 = makeSchema(version: 2, hash: "v2", tables: [
            makeTable(name: "orders", columns: standardColumns),
        ])
        try manager.migrateSchema(newSchema: v2)

        // Verify "items" table still exists locally with data
        XCTAssertTrue(try tableExists(db: db, name: "items"))
        let itemRows = try db.query("SELECT id, order_id FROM items", params: nil)
        XCTAssertEqual(itemRows.count, 1)
        XCTAssertEqual(itemRows[0]["id"] as String, "i1")
        XCTAssertEqual(itemRows[0]["order_id"] as String, "o1")

        // Verify orders table is also intact
        let orderRows = try db.query("SELECT id FROM orders", params: nil)
        XCTAssertEqual(orderRows.count, 1)
    }

    // MARK: - 10. testPreExistingTablesFromSeedReconciled

    func testPreExistingTablesFromSeedReconciled() throws {
        let db = try makeTestDB()
        let manager = SchemaManager(database: db)

        // Manually create a table that matches a server schema table but is missing one column.
        // Simulates a stale seed database that has "orders" with [id, updated_at, deleted_at]
        // but the server schema now also requires "description".
        _ = try db.execute(
            "CREATE TABLE orders (id TEXT PRIMARY KEY, updated_at TEXT NOT NULL, deleted_at TEXT)",
            params: nil
        )
        _ = try db.execute(
            "INSERT INTO orders (id, updated_at) VALUES ('seed-1', '2025-12-01T00:00:00Z')",
            params: nil
        )
        _ = try db.execute(
            "INSERT INTO orders (id, updated_at) VALUES ('seed-2', '2025-12-02T00:00:00Z')",
            params: nil
        )

        // Server schema includes the "description" column the seed is missing
        let serverColumns: [SchemaColumn] = [
            makeColumn(name: "id", dbType: "uuid", logicalType: "string", nullable: false, isPrimaryKey: true),
            makeColumn(name: "description", dbType: "text", logicalType: "string", nullable: true),
            makeColumn(name: "updated_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: false),
            makeColumn(name: "deleted_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: true),
        ]
        let schema = makeSchema(version: 1, hash: "v1", tables: [
            makeTable(name: "orders", columns: serverColumns),
        ])
        try manager.migrateSchema(newSchema: schema)

        // Verify the missing column was added
        let cols = try columnNames(db: db, table: "orders")
        XCTAssertTrue(cols.contains("description"), "migration should add missing column to pre-existing table")

        // Verify triggers were installed
        let triggers = try triggerCount(db: db, table: "orders")
        XCTAssertEqual(triggers, 3, "CDC triggers should be installed on pre-existing table")

        // Verify existing seed data is preserved
        let rows = try db.query("SELECT id FROM orders ORDER BY id", params: nil)
        XCTAssertEqual(rows.count, 2)
        XCTAssertEqual(rows[0]["id"] as String, "seed-1")
        XCTAssertEqual(rows[1]["id"] as String, "seed-2")
    }

    // MARK: - 11. testColumnTypeIncompatibilityTriggersRebuild

    func testColumnTypeIncompatibilityTriggersRebuild() throws {
        let db = try makeTestDB()
        let manager = SchemaManager(database: db)

        // Create v1 where "score" is TEXT (logicalType "string")
        let v1Columns: [SchemaColumn] = [
            makeColumn(name: "id", dbType: "uuid", logicalType: "string", nullable: false, isPrimaryKey: true),
            makeColumn(name: "score", dbType: "text", logicalType: "string", nullable: true),
            makeColumn(name: "updated_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: false),
            makeColumn(name: "deleted_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: true),
        ]
        let v1 = makeSchema(version: 1, hash: "v1", tables: [
            makeTable(name: "orders", columns: v1Columns),
        ])
        try manager.createSyncedTables(schema: v1)

        // Insert data and set a non-zero checkpoint to detect reset
        _ = try db.execute(
            "INSERT INTO orders (id, score, updated_at) VALUES ('o1', 'high', '2026-01-01T00:00:00Z')",
            params: nil
        )
        _ = try db.execute(
            "INSERT OR REPLACE INTO _synchro_meta (key, value) VALUES ('checkpoint', '42')",
            params: nil
        )

        // Verify checkpoint is non-zero before migration
        let beforeRow = try db.queryOne("SELECT value FROM _synchro_meta WHERE key = 'checkpoint'", params: nil)
        XCTAssertEqual(beforeRow?["value"] as? String, "42")

        // Migrate to v2 where "score" is now INTEGER (logicalType "int")
        let v2Columns: [SchemaColumn] = [
            makeColumn(name: "id", dbType: "uuid", logicalType: "string", nullable: false, isPrimaryKey: true),
            makeColumn(name: "score", dbType: "integer", logicalType: "int", nullable: true),
            makeColumn(name: "updated_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: false),
            makeColumn(name: "deleted_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: true),
        ]
        let v2 = makeSchema(version: 2, hash: "v2", tables: [
            makeTable(name: "orders", columns: v2Columns),
        ])
        try manager.migrateSchema(newSchema: v2)

        // Verify checkpoint was reset to 0 (destructive rebuild occurred)
        let afterRow = try db.queryOne("SELECT value FROM _synchro_meta WHERE key = 'checkpoint'", params: nil)
        XCTAssertEqual(afterRow?["value"] as? String, "0", "checkpoint should be reset to 0 after destructive rebuild")

        // Verify table was recreated with the new column type
        XCTAssertTrue(try tableExists(db: db, name: "orders"))
        let cols = try columnNames(db: db, table: "orders")
        XCTAssertTrue(cols.contains("score"), "score column should exist after rebuild")

        // Verify old data is gone (table was recreated)
        let rows = try db.query("SELECT id FROM orders", params: nil)
        XCTAssertEqual(rows.count, 0, "old data should be gone after destructive rebuild")

        // Verify triggers were reinstalled
        let triggers = try triggerCount(db: db, table: "orders")
        XCTAssertEqual(triggers, 3, "CDC triggers should be reinstalled after rebuild")

        // Verify the new type works (insert an integer value)
        let result = try db.execute(
            "INSERT INTO orders (id, score, updated_at) VALUES ('o2', 99, '2026-02-01T00:00:00Z')",
            params: nil
        )
        XCTAssertEqual(result.rowsAffected, 1)
    }
}
