import XCTest
import GRDB
import os
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

    private func makeManifest(tables: [TableSchema]) -> SchemaManifest {
        SchemaManifest(tables: tables)
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

    private func textRows(db: SynchroDatabase, sql: String) throws -> [String] {
        try db.query(sql, params: nil).map { $0["value"] as String }
    }

    // MARK: - Standard columns used across tests

    private var standardColumns: [SchemaColumn] {
        [
            makeColumn(name: "id", dbType: "uuid", logicalType: "string", nullable: false, isPrimaryKey: true),
            // A synced table with no writable column accepts no insert, so the
            // fixture carries one authored column like every real registration.
            makeColumn(name: "title", dbType: "text", logicalType: "string", nullable: true),
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
            TableSchema(
                name: "workouts",
                primaryKey: ["id"],
                updatedAtColumn: "updated_at",
                deletedAtColumn: "deleted_at",
                composition: .singleScope,
                columns: [
                    ColumnSchema(name: "id", type: "string", nullable: false),
                    ColumnSchema(name: "name", type: "string", nullable: false),
                    ColumnSchema(name: "updated_at", type: "datetime", nullable: false),
                    ColumnSchema(name: "deleted_at", type: "datetime", nullable: true),
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
            TableSchema(
                name: "workouts",
                primaryKey: ["id"],
                updatedAtColumn: "updated_at",
                deletedAtColumn: "deleted_at",
                composition: .singleScope,
                columns: [
                    ColumnSchema(name: "id", type: "string", nullable: false),
                    ColumnSchema(name: "name", type: "string", nullable: false),
                    ColumnSchema(name: "updated_at", type: "datetime", nullable: false),
                    ColumnSchema(name: "deleted_at", type: "datetime", nullable: true),
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
            TableSchema(
                name: "workouts",
                primaryKey: ["id"],
                updatedAtColumn: "updated_at",
                deletedAtColumn: "deleted_at",
                composition: .singleScope,
                columns: [
                    ColumnSchema(name: "id", type: "string", nullable: false),
                    ColumnSchema(name: "name", type: "string", nullable: false),
                    ColumnSchema(name: "notes", type: "string", nullable: true),
                    ColumnSchema(name: "updated_at", type: "datetime", nullable: false),
                    ColumnSchema(name: "deleted_at", type: "datetime", nullable: true),
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
        try db.writeTransaction { connection in
            try connection.execute(sql: "ALTER TABLE orders ADD COLUMN extra_data TEXT")
        }
        _ = try db.execute(
            "INSERT INTO orders (id, title, updated_at, extra_data) VALUES ('o1', 'first', '2026-01-01T00:00:00Z', 'local-stuff')",
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
            "INSERT INTO orders (id, title, updated_at) VALUES ('o1', 'first', '2026-01-01T00:00:00Z')",
            params: nil
        )
        _ = try db.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES ('o2', 'second', '2026-01-02T00:00:00Z')",
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

        let metadata = try db.readTransaction { connection in
            (
                try SynchroMeta.getInt64(connection, key: .schemaVersion),
                try SynchroMeta.get(connection, key: .schemaHash)
            )
        }
        XCTAssertEqual(metadata.0, 2)
        XCTAssertEqual(metadata.1, "v2")
        XCTAssertEqual(
            try db.query(
                "SELECT schema_version FROM _synchro_schema_archive WHERE schema_version = 2 AND schema_hash = 'v2'",
                params: nil
            ).count,
            1
        )
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
            "INSERT INTO orders (id, title, updated_at) VALUES ('o1', 'first', '2026-01-01T00:00:00Z')",
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
            "INSERT INTO orders (id, title, updated_at) VALUES ('o1', 'first', '2026-01-01T00:00:00Z')",
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
            "CREATE TABLE orders (id TEXT PRIMARY KEY, title TEXT, updated_at TEXT NOT NULL, deleted_at TEXT)",
            params: nil
        )
        _ = try db.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES ('seed-1', 'first', '2025-12-01T00:00:00Z')",
            params: nil
        )
        _ = try db.execute(
            "INSERT INTO orders (id, title, updated_at) VALUES ('seed-2', 'second', '2025-12-02T00:00:00Z')",
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

    // MARK: - 11. Unsupported schema transitions

    func testIncompatibleTypeAmongMultipleTablesRejectsWithoutStateChange() throws {
        let db = try makeTestDB()
        let manager = SchemaManager(database: db)

        let orderColumns: [SchemaColumn] = [
            makeColumn(name: "id", dbType: "uuid", logicalType: "string", nullable: false, isPrimaryKey: true),
            makeColumn(name: "title", dbType: "text", logicalType: "string", nullable: true),
            makeColumn(name: "updated_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: false),
            makeColumn(name: "deleted_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: true),
        ]
        let itemColumns: [SchemaColumn] = [
            makeColumn(name: "id", dbType: "uuid", logicalType: "string", nullable: false, isPrimaryKey: true),
            makeColumn(name: "score", dbType: "text", logicalType: "string", nullable: true),
            makeColumn(name: "updated_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: false),
            makeColumn(name: "deleted_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: true),
        ]
        let v1 = makeSchema(version: 1, hash: "v1", tables: [
            makeTable(name: "orders", columns: orderColumns),
            makeTable(name: "items", columns: itemColumns),
        ])
        try manager.createSyncedTables(schema: v1)

        try db.writeTransaction { connection in
            try connection.execute(sql: "ALTER TABLE orders ADD COLUMN local_note TEXT")
        }
        _ = try db.execute(
            "CREATE TABLE app_settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
            params: nil
        )
        _ = try db.execute(
            "INSERT INTO app_settings (key, value) VALUES ('theme', 'dark')",
            params: nil
        )
        _ = try db.execute(
            "INSERT INTO orders (id, title, local_note, updated_at) VALUES ('o1', 'order', 'local', '2026-01-01T00:00:00Z')",
            params: nil
        )
        _ = try db.execute(
            "INSERT INTO items (id, score, updated_at) VALUES ('i1', 'high', '2026-01-01T00:00:00Z')",
            params: nil
        )
        try db.writeSyncLockedTransaction { connection in
            try SynchroMeta.setInt64(connection, key: .checkpoint, value: 42)
            try SynchroMeta.set(connection, key: .snapshotComplete, value: "1")
            try SynchroMeta.upsertRowVersion(
                connection,
                tableName: "items",
                recordID: "i1",
                serverVersion: "server-v1",
                rowChecksum: nil
            )
            try SynchroMeta.upsertScope(
                connection,
                scopeID: "items:all",
                cursor: "cursor-v1",
                checksum: "scope-v1"
            )
            try SynchroMeta.upsertScopeRow(
                connection,
                scopeID: "items:all",
                tableName: "items",
                recordID: "i1",
                checksum: "row-v1",
                generation: 0
            )
            try SynchroMeta.upsertRejectedMutation(
                connection,
                mutationID: "rejected-1",
                tableName: "items",
                recordID: "i1",
                status: "rejected",
                code: "conflict",
                message: nil,
                serverRow: nil,
                serverVersion: "server-v1",
                mutationJSON: "{}",
                rejectedJSON: "{}"
            )
        }

        let metadataBefore = try textRows(
            db: db,
            sql: "SELECT key || '|' || value AS value FROM _synchro_meta ORDER BY key"
        )
        let archiveBefore = try textRows(
            db: db,
            sql: "SELECT CAST(schema_version AS TEXT) || '|' || schema_hash || '|' || schema_json AS value FROM _synchro_schema_archive ORDER BY schema_version, schema_hash"
        )
        let queueBefore = try textRows(
            db: db,
            sql: "SELECT mutation_id || '|' || table_name || '|' || record_id || '|' || lifecycle_state AS value FROM _synchro_pending_changes ORDER BY local_order"
        )
        let mutationValuesBefore = try textRows(
            db: db,
            sql: "SELECT mutation_id || '|' || field_id || '|' || value_kind || '|' || COALESCE(value_text, CAST(value_integer AS TEXT), CAST(value_real AS TEXT), hex(value_blob), 'NULL') AS value FROM _synchro_mutation_values ORDER BY mutation_id, field_id"
        )
        let schemaVersionBefore = try XCTUnwrap(
            db.queryOne("PRAGMA schema_version", params: nil)?["schema_version"] as? Int64
        )
        let totalChangesBefore = try XCTUnwrap(
            db.queryOne("SELECT total_changes() AS value", params: nil)?["value"] as? Int64
        )

        var additiveOrderColumns = orderColumns
        additiveOrderColumns.insert(
            makeColumn(name: "server_note", dbType: "text", logicalType: "string", nullable: true),
            at: 2
        )
        let incompatibleItemColumns: [SchemaColumn] = [
            makeColumn(name: "id", dbType: "uuid", logicalType: "string", nullable: false, isPrimaryKey: true),
            makeColumn(name: "score", dbType: "integer", logicalType: "int", nullable: true),
            makeColumn(name: "updated_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: false),
            makeColumn(name: "deleted_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: true),
        ]
        let v2 = makeSchema(version: 2, hash: "v2", tables: [
            makeTable(name: "orders", columns: additiveOrderColumns),
            makeTable(name: "items", columns: incompatibleItemColumns),
        ])
        XCTAssertThrowsError(try manager.migrateSchema(newSchema: v2))

        XCTAssertFalse(try columnNames(db: db, table: "orders").contains("server_note"))
        XCTAssertEqual(
            try textRows(db: db, sql: "SELECT id || '|' || title || '|' || local_note AS value FROM orders"),
            ["o1|order|local"]
        )
        XCTAssertEqual(
            try textRows(db: db, sql: "SELECT id || '|' || score AS value FROM items"),
            ["i1|high"]
        )
        XCTAssertEqual(
            try textRows(db: db, sql: "SELECT key || '|' || value AS value FROM app_settings"),
            ["theme|dark"]
        )
        XCTAssertEqual(try textRows(db: db, sql: "SELECT table_name || '|' || record_id || '|' || server_version AS value FROM _synchro_row_versions"), ["items|i1|server-v1"])
        XCTAssertEqual(try textRows(db: db, sql: "SELECT scope_id || '|' || cursor || '|' || checksum AS value FROM _synchro_scopes"), ["items:all|cursor-v1|scope-v1"])
        XCTAssertEqual(try textRows(db: db, sql: "SELECT scope_id || '|' || table_name || '|' || record_id || '|' || checksum AS value FROM _synchro_scope_rows"), ["items:all|items|i1|row-v1"])
        XCTAssertEqual(try textRows(db: db, sql: "SELECT mutation_id || '|' || status || '|' || code AS value FROM _synchro_rejected_mutations"), ["rejected-1|rejected|conflict"])
        XCTAssertEqual(try textRows(db: db, sql: "SELECT key || '|' || value AS value FROM _synchro_meta ORDER BY key"), metadataBefore)
        XCTAssertEqual(try textRows(db: db, sql: "SELECT CAST(schema_version AS TEXT) || '|' || schema_hash || '|' || schema_json AS value FROM _synchro_schema_archive ORDER BY schema_version, schema_hash"), archiveBefore)
        XCTAssertEqual(try textRows(db: db, sql: "SELECT mutation_id || '|' || table_name || '|' || record_id || '|' || lifecycle_state AS value FROM _synchro_pending_changes ORDER BY local_order"), queueBefore)
        XCTAssertEqual(try textRows(db: db, sql: "SELECT mutation_id || '|' || field_id || '|' || value_kind || '|' || COALESCE(value_text, CAST(value_integer AS TEXT), CAST(value_real AS TEXT), hex(value_blob), 'NULL') AS value FROM _synchro_mutation_values ORDER BY mutation_id, field_id"), mutationValuesBefore)
        XCTAssertEqual(try db.queryOne("PRAGMA schema_version", params: nil)?["schema_version"] as? Int64, schemaVersionBefore)
        XCTAssertEqual(try db.queryOne("SELECT total_changes() AS value", params: nil)?["value"] as? Int64, totalChangesBefore)
    }

    func testPrimaryKeyShapeChangeRejectsWithoutStateChange() throws {
        let db = try makeTestDB()
        let manager = SchemaManager(database: db)
        let v1Columns = standardColumns + [makeColumn(name: "alternate_id", nullable: false)]
        try manager.createSyncedTables(schema: makeSchema(version: 1, hash: "v1", tables: [
            makeTable(name: "orders", columns: v1Columns),
        ]))
        _ = try db.execute(
            "INSERT INTO orders (id, alternate_id, updated_at) VALUES ('o1', 'alternate-1', '2026-01-01T00:00:00Z')",
            params: nil
        )

        var targetColumns = v1Columns.map {
            makeColumn(
                name: $0.name,
                logicalType: $0.logicalType,
                nullable: $0.nullable,
                isPrimaryKey: $0.name == "alternate_id"
            )
        }
        targetColumns[0] = makeColumn(name: "id", nullable: false)
        let targetTable = SchemaTable(
            tableName: "orders",
            updatedAtColumn: "updated_at",
            deletedAtColumn: "deleted_at",
            primaryKey: ["alternate_id"],
            columns: targetColumns
        )
        let metadataBefore = try textRows(db: db, sql: "SELECT key || '|' || value AS value FROM _synchro_meta ORDER BY key")
        let queueBefore = try textRows(db: db, sql: "SELECT mutation_id || '|' || record_id AS value FROM _synchro_pending_changes")

        XCTAssertThrowsError(
            try manager.migrateLocalSchema(newTables: [targetTable])
        )

        XCTAssertEqual(try textRows(db: db, sql: "SELECT id || '|' || alternate_id AS value FROM orders"), ["o1|alternate-1"])
        XCTAssertEqual(try textRows(db: db, sql: "SELECT key || '|' || value AS value FROM _synchro_meta ORDER BY key"), metadataBefore)
        XCTAssertEqual(try textRows(db: db, sql: "SELECT mutation_id || '|' || record_id AS value FROM _synchro_pending_changes"), queueBefore)
        let primaryKey = try textRows(
            db: db,
            sql: "SELECT name AS value FROM pragma_table_info('orders') WHERE pk > 0 ORDER BY pk"
        )
        XCTAssertEqual(primaryKey, ["id"])
    }

    func testAdditiveDDLFailureRollsBackEarlierDDLAndMetadata() throws {
        let db = try makeTestDB()
        let manager = SchemaManager(database: db)
        let v1 = makeSchema(version: 1, hash: "v1", tables: [
            makeTable(name: "orders", columns: standardColumns),
            makeTable(name: "items", columns: standardColumns),
        ])
        try manager.createSyncedTables(schema: v1)
        let metadataBefore = try textRows(db: db, sql: "SELECT key || '|' || value AS value FROM _synchro_meta ORDER BY key")
        let archiveBefore = try textRows(db: db, sql: "SELECT CAST(schema_version AS TEXT) || '|' || schema_hash AS value FROM _synchro_schema_archive ORDER BY schema_version")

        var orderColumns = standardColumns
        orderColumns.insert(makeColumn(name: "first_addition"), at: 1)
        var itemColumns = standardColumns
        itemColumns.insert(
            SchemaColumn(
                name: "invalid_addition",
                logicalType: "string",
                nullable: false,
                sqliteDefaultSQL: "'unterminated",
                isPrimaryKey: false
            ),
            at: 1
        )

        XCTAssertThrowsError(
            try manager.migrateLocalSchema(newTables: [
                makeTable(name: "orders", columns: orderColumns),
                makeTable(name: "items", columns: itemColumns),
            ])
        )

        XCTAssertFalse(try columnNames(db: db, table: "orders").contains("first_addition"))
        XCTAssertFalse(try columnNames(db: db, table: "items").contains("invalid_addition"))
        XCTAssertEqual(try textRows(db: db, sql: "SELECT key || '|' || value AS value FROM _synchro_meta ORDER BY key"), metadataBefore)
        XCTAssertEqual(try textRows(db: db, sql: "SELECT CAST(schema_version AS TEXT) || '|' || schema_hash AS value FROM _synchro_schema_archive ORDER BY schema_version"), archiveBefore)
    }

    func testConnectScopeCursorUpdatesAndAffectedScopesAreApplied() throws {
        let db = try makeTestDB()
        let manager = SchemaManager(database: db)
        let manifest = makeManifest(tables: [
            TableSchema(
                name: "orders",
                primaryKey: ["id"],
                updatedAtColumn: "updated_at",
                deletedAtColumn: "deleted_at",
                composition: .singleScope,
                columns: [
                    ColumnSchema(name: "id", type: "string", nullable: false),
                    ColumnSchema(name: "updated_at", type: "datetime", nullable: false),
                    ColumnSchema(name: "deleted_at", type: "datetime", nullable: true),
                ],
                indexes: nil
            )
        ])
        let tables = try manifest.localTables()
        try manager.reconcileLocalSchema(schemaVersion: 1, schemaHash: protocolTestSchemaHash, tables: tables)
        try db.writeTransaction { connection in
            try SynchroMeta.upsertScope(connection, scopeID: "orders:existing", cursor: "old", checksum: "old")
            try SynchroMeta.upsertScope(connection, scopeID: "orders:affected", cursor: "old", checksum: "old")
        }

        try manager.reconcileLocalSchema(
            schemaVersion: 2,
            schemaHash: String(repeating: "1", count: 64),
            tables: tables,
            scopeCursorUpdates: ["orders:existing": "new-current-schema", "orders:affected": nil],
            affectedScopes: ["orders:affected"]
        )

        let existing = try db.readTransaction { try SynchroMeta.getScope($0, scopeID: "orders:existing") }
        let affected = try db.readTransaction { try SynchroMeta.getScope($0, scopeID: "orders:affected") }
        XCTAssertEqual(existing?.cursor, "new-current-schema")
        XCTAssertNil(existing?.checksum)
        XCTAssertNil(affected?.cursor)
        XCTAssertEqual(affected?.generation, 1)
    }

    func testConnectCursorUpdateRecomputesRetainedProvenanceForTargetFieldIDs() throws {
        func localTable(fieldSuffix: String) -> LocalSchemaTable {
            let columns = [
                LocalSchemaColumn(
                    fieldID: "field-id-\(fieldSuffix)", name: "id", logicalType: "string",
                    nullable: false, writable: false, precision: nil, scale: nil,
                    sqliteDefaultSQL: nil, isPrimaryKey: true
                ),
                LocalSchemaColumn(
                    fieldID: "field-title-\(fieldSuffix)", name: "title", logicalType: "string",
                    nullable: false, writable: true, precision: nil, scale: nil,
                    sqliteDefaultSQL: nil, isPrimaryKey: false
                ),
                LocalSchemaColumn(
                    fieldID: "field-updated-\(fieldSuffix)", name: "updated_at", logicalType: "datetime",
                    nullable: false, writable: false, precision: nil, scale: nil,
                    sqliteDefaultSQL: nil, isPrimaryKey: false
                ),
                LocalSchemaColumn(
                    fieldID: "field-deleted-\(fieldSuffix)", name: "deleted_at", logicalType: "datetime",
                    nullable: true, writable: false, precision: nil, scale: nil,
                    sqliteDefaultSQL: nil, isPrimaryKey: false
                ),
            ]
            return LocalSchemaTable(
                tableID: "table-orders",
                relationID: "relation-orders",
                tableName: "orders",
                primaryKeyFieldID: "field-id-\(fieldSuffix)",
                createdAtFieldID: nil,
                updatedAtFieldID: "field-updated-\(fieldSuffix)",
                deletedAtFieldID: "field-deleted-\(fieldSuffix)",
                updatedAtColumn: "updated_at",
                deletedAtColumn: "deleted_at",
                composition: .singleScope,
                primaryKey: ["id"],
                columns: columns
            )
        }

        let db = try makeTestDB()
        let manager = SchemaManager(database: db)
        let oldTable = localTable(fieldSuffix: "old")
        let targetTable = localTable(fieldSuffix: "target")
        let oldHash = String(repeating: "0", count: 64)
        let targetHash = String(repeating: "1", count: 64)
        let scopeID = "orders:user-1"
        let serverVersion = "server-version-1"
        try manager.reconcileLocalSchema(schemaVersion: 1, schemaHash: oldHash, tables: [oldTable])

        let oldRow: [String: AnyCodable] = [
            "field-id-old": AnyCodable("r1"),
            "field-title-old": AnyCodable("retained"),
            "field-updated-old": AnyCodable("2026-01-01T00:00:00.000000Z"),
            "field-deleted-old": AnyCodable(NSNull()),
        ]
        let oldPK = ["field-id-old": AnyCodable("r1")]
        let oldDigest = try Integrity.rowDigest(
            schemaHash: oldHash,
            table: oldTable,
            pk: oldPK,
            row: oldRow,
            serverVersion: serverVersion
        )
        try db.writeSyncLockedTransaction { connection in
            try connection.execute(
                sql: "INSERT INTO orders (id, title, updated_at, deleted_at) VALUES (?, ?, ?, NULL)",
                arguments: ["r1", "retained", "2026-01-01T00:00:00.000000Z"]
            )
            try SynchroMeta.upsertRowVersion(
                connection,
                tableName: "orders",
                recordID: "r1",
                serverVersion: serverVersion,
                rowChecksum: oldDigest.checksum
            )
            try SynchroMeta.upsertScope(
                connection,
                scopeID: scopeID,
                cursor: "old-cursor",
                checksum: nil
            )
            try SynchroMeta.upsertScopeRow(
                connection,
                scopeID: scopeID,
                tableName: "orders",
                recordID: "r1",
                checksum: oldDigest.checksum.digest,
                generation: 0
            )
        }

        try manager.reconcileLocalSchema(
            schemaVersion: 2,
            schemaHash: targetHash,
            tables: [targetTable],
            scopeCursorUpdates: [scopeID: "target-cursor"]
        )

        let targetRow: [String: AnyCodable] = [
            "field-id-target": AnyCodable("r1"),
            "field-title-target": AnyCodable("retained"),
            "field-updated-target": AnyCodable("2026-01-01T00:00:00.000000Z"),
            "field-deleted-target": AnyCodable(NSNull()),
        ]
        let targetDigest = try Integrity.rowDigest(
            schemaHash: targetHash,
            table: targetTable,
            pk: ["field-id-target": AnyCodable("r1")],
            row: targetRow,
            serverVersion: serverVersion
        )
        let targetScopeDigest = try Integrity.scopeDigest(
            schemaHash: targetHash,
            scopeID: scopeID,
            entries: [(identity: targetDigest.identity, digest: targetDigest.checksum)]
        )
        XCTAssertNotEqual(oldDigest.checksum, targetDigest.checksum)

        let storedRows = try db.readTransaction {
            try SynchroMeta.getScopeRowChecksums($0, scopeID: scopeID)
        }
        let storedScope = try db.readTransaction { try SynchroMeta.getScope($0, scopeID: scopeID) }
        XCTAssertEqual(storedRows.single?.checksum, targetDigest.checksum.digest)
        XCTAssertEqual(storedScope?.cursor, "target-cursor")
        let localChecksum = try JSONDecoder.synchroDecoder().decode(
            ChecksumObject.self,
            from: Data(try XCTUnwrap(storedScope?.localChecksum).utf8)
        )
        XCTAssertEqual(localChecksum, targetScopeDigest)
    }

    func testPreparedMigrationProtectsTargetTableDuringConcurrentDDL() throws {
        let database = try makeTestDB()
        defer { try? database.close() }
        let manager = SchemaManager(database: database)
        var sourceManifest = protocolOrdersSchemaManifest(includeNotes: false)
        sourceManifest.schemaHash = try Integrity.schemaManifestHash(sourceManifest)
        try manager.createSyncedTables(schema: SchemaResponse(
            schemaVersion: sourceManifest.schemaVersion,
            schemaHash: sourceManifest.schemaHash,
            serverTime: Date(),
            manifest: sourceManifest
        ))

        var targetManifest = protocolOrdersSchemaManifest(
            includeNotes: false,
            schemaVersion: 2,
            parentSchema: SchemaRef(version: 1, hash: sourceManifest.schemaHash),
            transitionClass: "class_2",
            compatibilityFloor: 1
        )
        targetManifest.tables.append(TableSchema(
            tableID: "table-future-items",
            relationID: "relation-future-items",
            name: "future_items",
            primaryKeyFieldID: "field-future-id",
            lifecycle: LifecycleSchema(
                createdAtFieldID: nil,
                updatedAtFieldID: "field-future-updated-at",
                deletedAtFieldID: "field-future-deleted-at"
            ),
            composition: .singleScope,
            fields: [
                ColumnSchema(fieldID: "field-future-id", name: "id", type: "string", nullable: false, writable: false, precision: nil, scale: nil),
                ColumnSchema(fieldID: "field-future-updated-at", name: "updated_at", type: "datetime", nullable: false, writable: false, precision: nil, scale: nil),
                ColumnSchema(fieldID: "field-future-deleted-at", name: "deleted_at", type: "datetime", nullable: true, writable: false, precision: nil, scale: nil),
            ],
            indexes: []
        ))
        targetManifest.schemaHash = try Integrity.schemaManifestHash(targetManifest)
        _ = try manager.prepareMigration(
            targetManifest: targetManifest,
            action: .replace,
            affectedScopes: [],
            scopeCursorUpdates: [:],
            schemaReset: false
        )

        let migrationEntered = expectation(description: "migration holds the synchronized writer")
        let migrationFinished = expectation(description: "migration completes")
        let releaseMigration = DispatchSemaphore(value: 0)
        let migrationResult = OSAllocatedUnfairLock(
            initialState: Optional<Result<SchemaMigrationJournal, Error>>.none
        )
        DispatchQueue.global().async {
            let result = Result {
                try database.writeSyncLockedTransaction { connection in
                    migrationEntered.fulfill()
                    _ = releaseMigration.wait(timeout: .now() + 2)
                    return try manager.applyPreparedMigrationInTransaction(connection)
                }
            }
            migrationResult.withLock { $0 = result }
            migrationFinished.fulfill()
        }
        wait(for: [migrationEntered], timeout: 1)

        let applicationFinished = expectation(description: "application DDL is rejected before the writer is released")
        let applicationResult = OSAllocatedUnfairLock(
            initialState: Optional<Result<Void, Error>>.none
        )
        DispatchQueue.global().async {
            let result: Result<Void, Error> = Result {
                try database.applicationWritePreparedStatement(
                    "CREATE TABLE future_items (id TEXT PRIMARY KEY)"
                ) { statement in
                    try statement.execute()
                }
            }
            applicationResult.withLock { $0 = result }
            applicationFinished.fulfill()
        }

        let applicationWait = XCTWaiter.wait(for: [applicationFinished], timeout: 1)
        releaseMigration.signal()
        wait(for: [migrationFinished], timeout: 2)

        XCTAssertEqual(applicationWait, .completed)
        guard let applicationResult = applicationResult.withLock({ $0 }) else {
            return XCTFail("Application DDL did not produce a result")
        }
        if case .success = applicationResult {
            XCTFail("Application DDL changed a target synchronized table")
        }
        let applied = try XCTUnwrap(migrationResult.withLock { $0 }).get()
        XCTAssertEqual(applied.phase, .applied)
        XCTAssertTrue(try tableExists(db: database, name: "future_items"))
    }

    func testPreparedMigrationRecoversAfterAbruptReopenWithoutLosingApplicationState() throws {
        let path = (NSTemporaryDirectory() as NSString)
            .appendingPathComponent("synchro_prepared_migration_\(UUID().uuidString).sqlite")
        var sourceManifest = protocolOrdersSchemaManifest(includeNotes: false)
        sourceManifest.schemaHash = try Integrity.schemaManifestHash(sourceManifest)
        var targetManifest = protocolOrdersSchemaManifest(
            includeNotes: true,
            schemaVersion: 2,
            parentSchema: SchemaRef(version: 1, hash: sourceManifest.schemaHash),
            transitionClass: "class_2",
            compatibilityFloor: 1
        )
        targetManifest.schemaHash = try Integrity.schemaManifestHash(targetManifest)

        let database = try SynchroDatabase(path: path)
        let manager = SchemaManager(database: database)
        try manager.createSyncedTables(schema: SchemaResponse(
            schemaVersion: sourceManifest.schemaVersion,
            schemaHash: sourceManifest.schemaHash,
            serverTime: Date(),
            manifest: sourceManifest
        ))
        _ = try database.execute(
            "CREATE TABLE local_settings (key TEXT PRIMARY KEY, value TEXT)",
            params: nil
        )
        _ = try database.execute(
            "INSERT INTO local_settings (key, value) VALUES ('theme', 'dark')",
            params: nil
        )
        _ = try database.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES ('o1', 'offline', 'u1', '2026-01-01T00:00:00.000000Z')",
            params: nil
        )
        let pendingBefore = try ChangeTracker(database: database).inspectPendingMutations()

        _ = try manager.prepareMigration(
            targetManifest: targetManifest,
            action: .replace,
            affectedScopes: [],
            scopeCursorUpdates: [:],
            schemaReset: false
        )
        XCTAssertFalse(try database.query("PRAGMA table_info(orders)", params: nil)
            .contains { ($0["name"] as String?) == "notes" })
        let recoveredDatabase = try SynchroDatabase(path: path)
        defer {
            try? recoveredDatabase.close()
            try? database.close()
        }
        let recoveredManager = SchemaManager(database: recoveredDatabase)
        let recovered = try XCTUnwrap(recoveredManager.recoverMigrationIfNeeded())

        XCTAssertEqual(recovered.phase, .applied)
        XCTAssertTrue(try recoveredDatabase.query("PRAGMA table_info(orders)", params: nil)
            .contains { ($0["name"] as String?) == "notes" })
        XCTAssertEqual(
            try recoveredDatabase.queryOne(
                "SELECT value FROM local_settings WHERE key = 'theme'",
                params: nil
            )?["value"] as String?,
            "dark"
        )
        XCTAssertEqual(
            try ChangeTracker(database: recoveredDatabase).inspectPendingMutations(),
            pendingBefore
        )
        XCTAssertNil(try recoveredManager.activeMigration())
    }

    func testAppliedMigrationAbruptReopenDoesNotRepeatDDLOrScopeInvalidation() throws {
        let path = (NSTemporaryDirectory() as NSString)
            .appendingPathComponent("synchro_applied_migration_\(UUID().uuidString).sqlite")
        let scopeID = "orders:user-1"
        var sourceManifest = protocolOrdersSchemaManifest(includeNotes: false)
        sourceManifest.schemaHash = try Integrity.schemaManifestHash(sourceManifest)
        var targetManifest = protocolOrdersSchemaManifest(
            includeNotes: true,
            schemaVersion: 2,
            parentSchema: SchemaRef(version: 1, hash: sourceManifest.schemaHash),
            transitionClass: "class_3",
            compatibilityFloor: 2
        )
        targetManifest.schemaHash = try Integrity.schemaManifestHash(targetManifest)

        let database = try SynchroDatabase(path: path)
        let manager = SchemaManager(database: database)
        try manager.createSyncedTables(schema: SchemaResponse(
            schemaVersion: sourceManifest.schemaVersion,
            schemaHash: sourceManifest.schemaHash,
            serverTime: Date(),
            manifest: sourceManifest
        ))
        try database.writeTransaction { connection in
            try SynchroMeta.upsertScope(
                connection,
                scopeID: scopeID,
                cursor: "old-cursor",
                checksum: "old-checksum"
            )
        }
        _ = try manager.prepareMigration(
            targetManifest: targetManifest,
            action: .rebuildLocal,
            affectedScopes: [scopeID],
            scopeCursorUpdates: [scopeID: nil],
            schemaReset: false
        )
        let applied = try database.writeSyncLockedTransaction { connection in
            try manager.applyPreparedMigrationInTransaction(connection)
        }
        XCTAssertEqual(applied.phase, .applied)
        let generationAfterApply = try database.readTransaction {
            try SynchroMeta.getScopeGeneration($0, scopeID: scopeID)
        }
        let recoveredDatabase = try SynchroDatabase(path: path)
        defer {
            try? recoveredDatabase.close()
            try? database.close()
        }
        let recoveredManager = SchemaManager(database: recoveredDatabase)
        let recovered = try XCTUnwrap(recoveredManager.recoverMigrationIfNeeded())

        XCTAssertEqual(recovered.phase, .applied)
        XCTAssertEqual(
            try recoveredDatabase.readTransaction {
                try SynchroMeta.getScopeGeneration($0, scopeID: scopeID)
            },
            generationAfterApply
        )
        XCTAssertNil(try recoveredDatabase.readTransaction {
            try SynchroMeta.getScope($0, scopeID: scopeID)?.cursor
        })
        XCTAssertNotNil(try recoveredManager.activeMigration())
        XCTAssertEqual(
            try recoveredDatabase.query("PRAGMA table_info(orders)", params: nil)
                .filter { ($0["name"] as String?) == "notes" }
                .count,
            1
        )
    }
}

private extension Array {
    var single: Element? { count == 1 ? self[0] : nil }
}
