import XCTest
import GRDB
@testable import Synchro

final class SchemaManagerTests: XCTestCase {
    private func makeTestDB() throws -> SynchroDatabase {
        let tmpDir = NSTemporaryDirectory()
        let path = (tmpDir as NSString).appendingPathComponent("synchro_test_\(UUID().uuidString).sqlite")
        return try SynchroDatabase(path: path)
    }

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
}
