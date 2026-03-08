import XCTest
@testable import Synchro

final class SQLiteSchemaTests: XCTestCase {
    func testLogicalTypeMapping() {
        XCTAssertEqual(SQLiteSchema.sqliteType(for: "string"), "TEXT")
        XCTAssertEqual(SQLiteSchema.sqliteType(for: "int"), "INTEGER")
        XCTAssertEqual(SQLiteSchema.sqliteType(for: "int64"), "INTEGER")
        XCTAssertEqual(SQLiteSchema.sqliteType(for: "float"), "REAL")
        XCTAssertEqual(SQLiteSchema.sqliteType(for: "boolean"), "INTEGER")
        XCTAssertEqual(SQLiteSchema.sqliteType(for: "datetime"), "TEXT")
        XCTAssertEqual(SQLiteSchema.sqliteType(for: "date"), "TEXT")
        XCTAssertEqual(SQLiteSchema.sqliteType(for: "time"), "TEXT")
        XCTAssertEqual(SQLiteSchema.sqliteType(for: "json"), "TEXT")
        XCTAssertEqual(SQLiteSchema.sqliteType(for: "bytes"), "BLOB")
        XCTAssertEqual(SQLiteSchema.sqliteType(for: "unknown"), "TEXT")
    }

    func testGenerateCreateTableSQL() {
        let table = SchemaTable(
            tableName: "workouts",
            pushPolicy: "owner_only",
            updatedAtColumn: "updated_at",
            deletedAtColumn: "deleted_at",
            primaryKey: ["id"],
            columns: [
                SchemaColumn(name: "id", dbType: "uuid", logicalType: "string", nullable: false, isPrimaryKey: true),
                SchemaColumn(name: "name", dbType: "text", logicalType: "string", nullable: false, isPrimaryKey: false),
                SchemaColumn(name: "user_id", dbType: "uuid", logicalType: "string", nullable: false, isPrimaryKey: false),
                SchemaColumn(name: "created_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: false, isPrimaryKey: false),
                SchemaColumn(name: "updated_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: false, isPrimaryKey: false),
                SchemaColumn(name: "deleted_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: true, isPrimaryKey: false),
            ]
        )

        let sql = SQLiteSchema.generateCreateTableSQL(table: table)

        XCTAssertTrue(sql.contains("CREATE TABLE IF NOT EXISTS \"workouts\""))
        XCTAssertTrue(sql.contains("\"id\" TEXT PRIMARY KEY"))
        XCTAssertTrue(sql.contains("\"name\" TEXT NOT NULL"))
        XCTAssertTrue(sql.contains("\"deleted_at\" TEXT"))
        XCTAssertFalse(sql.contains("\"deleted_at\" TEXT NOT NULL"))
    }

    func testGenerateCDCTriggers() {
        let table = SchemaTable(
            tableName: "workouts",
            pushPolicy: "owner_only",
            updatedAtColumn: "updated_at",
            deletedAtColumn: "deleted_at",
            primaryKey: ["id"],
            columns: [
                SchemaColumn(name: "id", dbType: "uuid", logicalType: "string", nullable: false, isPrimaryKey: true),
            ]
        )

        let triggers = SQLiteSchema.generateCDCTriggers(table: table)

        // 3 DROP + 3 CREATE = 6 statements
        XCTAssertEqual(triggers.count, 6)

        // Check trigger names
        XCTAssertTrue(triggers[0].contains("DROP TRIGGER IF EXISTS _synchro_cdc_insert_workouts"))
        XCTAssertTrue(triggers[3].contains("CREATE TRIGGER _synchro_cdc_insert_workouts"))
        XCTAssertTrue(triggers[4].contains("CREATE TRIGGER _synchro_cdc_update_workouts"))
        XCTAssertTrue(triggers[5].contains("CREATE TRIGGER _synchro_cdc_delete_workouts"))

        // Check sync_lock check
        XCTAssertTrue(triggers[3].contains("sync_lock"))
        XCTAssertTrue(triggers[4].contains("sync_lock"))
        XCTAssertTrue(triggers[5].contains("sync_lock"))

        // Check BEFORE DELETE has RAISE(IGNORE)
        XCTAssertTrue(triggers[5].contains("RAISE(IGNORE)"))
    }
}
