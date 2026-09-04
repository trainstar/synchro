import XCTest
import GRDB
@testable import Synchro

/// The capture context matches physical table and column names while the
/// mutation ledger still records wire field IDs, so this schema keeps wire
/// IDs that differ from every physical name. Issue #42.
final class AuthoredCaptureTests: XCTestCase {
    private let authoredTable = LocalSchemaTable(
        tableID: "table-authored-rows",
        relationID: "relation-authored-rows",
        tableName: "authored_rows",
        primaryKeyFieldID: "field-id",
        createdAtFieldID: nil,
        updatedAtFieldID: "field-updated-at",
        deletedAtFieldID: "field-deleted-at",
        updatedAtColumn: "updated_at",
        deletedAtColumn: "deleted_at",
        composition: nil,
        primaryKey: ["id"],
        columns: [
            LocalSchemaColumn(fieldID: "field-id", name: "id", logicalType: "string", nullable: false, writable: false, precision: nil, scale: nil, sqliteDefaultSQL: nil, isPrimaryKey: true),
            LocalSchemaColumn(fieldID: "field-body", name: "body", logicalType: "string", nullable: true, writable: true, precision: nil, scale: nil, sqliteDefaultSQL: nil, isPrimaryKey: false),
            LocalSchemaColumn(fieldID: "field-default", name: "default_value", logicalType: "string", nullable: false, writable: true, precision: nil, scale: nil, sqliteDefaultSQL: "'default'", isPrimaryKey: false),
            LocalSchemaColumn(fieldID: "field-support", name: "support_value", logicalType: "string", nullable: false, writable: true, precision: nil, scale: nil, sqliteDefaultSQL: "''", isPrimaryKey: false),
            LocalSchemaColumn(fieldID: "field-updated-at", name: "updated_at", logicalType: "datetime", nullable: false, writable: false, precision: nil, scale: nil, sqliteDefaultSQL: nil, isPrimaryKey: false),
            LocalSchemaColumn(fieldID: "field-deleted-at", name: "deleted_at", logicalType: "datetime", nullable: true, writable: false, precision: nil, scale: nil, sqliteDefaultSQL: nil, isPrimaryKey: false),
        ]
    )

    private func makeEnvironment() throws -> SynchroDatabase {
        let path = (NSTemporaryDirectory() as NSString)
            .appendingPathComponent("synchro_authored_\(UUID().uuidString).sqlite")
        let db = try SynchroDatabase(path: path)
        let manager = SchemaManager(database: db)
        let table = authoredTable
        try db.writeTransaction { connection in
            try manager.createSyncedTablesInTransaction(connection, tables: [table], installTriggers: true)
            try SynchroMeta.setInt64(connection, key: .schemaVersion, value: 1)
            try SynchroMeta.set(connection, key: .schemaHash, value: protocolTestSchemaHash)
            try SynchroMeta.archiveSchema(connection, version: 1, hash: protocolTestSchemaHash, tables: [table])
        }
        db.updateApplicationSyncedTables([table])
        return db
    }

    private func assertLedger(
        _ db: SynchroDatabase,
        expectedOperations: [String],
        expectedFields: [[String]],
        file: StaticString = #filePath,
        line: UInt = #line
    ) throws {
        try db.readTransaction { connection in
            let ledger = try Row.fetchAll(
                connection,
                sql: "SELECT mutation_id, operation FROM _synchro_pending_changes ORDER BY local_order"
            )
            XCTAssertEqual(ledger.map { $0["operation"] as String }, expectedOperations, file: file, line: line)
            var fields: [[String]] = []
            for mutation in ledger {
                let values = try String.fetchAll(
                    connection,
                    sql: "SELECT field_id FROM _synchro_mutation_values WHERE mutation_id = ? ORDER BY field_id",
                    arguments: [mutation["mutation_id"] as String]
                )
                fields.append(values)
            }
            XCTAssertEqual(fields, expectedFields, file: file, line: line)
            let contextRows = try Int.fetchOne(connection, sql: "SELECT COUNT(*) FROM _synchro_capture_context")
            let fieldRows = try Int.fetchOne(connection, sql: "SELECT COUNT(*) FROM _synchro_capture_fields")
            XCTAssertEqual(contextRows, 0, file: file, line: line)
            XCTAssertEqual(fieldRows, 0, file: file, line: line)
        }
    }

    func testOmittedDefaultRemainsAbsentFromTheCapturedInsert() throws {
        let db = try makeEnvironment()
        try db.applicationAuthoredWriteTransaction(
            tableName: "authored_rows",
            operation: "insert",
            columnNames: ["body"]
        ) { transaction in
            try transaction.execute(
                "INSERT INTO authored_rows (id, body, updated_at) VALUES (?, ?, ?)",
                params: ["row-1", "authored", "2026-01-01T00:00:00.000000Z"]
            )
        }
        let stored = try db.readTransaction { connection in
            try String.fetchOne(connection, sql: "SELECT default_value FROM authored_rows")
        }
        XCTAssertEqual(stored, "default")
        try assertLedger(db, expectedOperations: ["insert"], expectedFields: [["field-body"]])
    }

    func testExplicitDefaultValuedWriteRemainsAuthored() throws {
        let db = try makeEnvironment()
        try db.applicationAuthoredWriteTransaction(
            tableName: "authored_rows",
            operation: "insert",
            columnNames: ["default_value"]
        ) { transaction in
            try transaction.execute(
                "INSERT INTO authored_rows (id, default_value, updated_at) VALUES (?, ?, ?)",
                params: ["row-1", "default", "2026-01-01T00:00:00.000000Z"]
            )
        }
        try assertLedger(db, expectedOperations: ["insert"], expectedFields: [["field-default"]])
    }

    func testSupportColumnInjectionRemainsAbsentFromTheCapturedInsert() throws {
        let db = try makeEnvironment()
        try db.applicationAuthoredWriteTransaction(
            tableName: "authored_rows",
            operation: "insert",
            columnNames: ["body"]
        ) { transaction in
            try transaction.execute(
                "INSERT INTO authored_rows (id, body, support_value, updated_at) VALUES (?, ?, ?, ?)",
                params: ["row-1", "authored", "runtime-support", "2026-01-01T00:00:00.000000Z"]
            )
        }
        let stored = try db.readTransaction { connection in
            try String.fetchOne(connection, sql: "SELECT support_value FROM authored_rows")
        }
        XCTAssertEqual(stored, "runtime-support")
        try assertLedger(db, expectedOperations: ["insert"], expectedFields: [["field-body"]])
    }

    func testUpdateCapturesOnlyChangedAuthoredColumns() throws {
        let db = try makeEnvironment()
        try db.applicationAuthoredWriteTransaction(
            tableName: "authored_rows",
            operation: "insert",
            columnNames: ["body"]
        ) { transaction in
            try transaction.execute(
                "INSERT INTO authored_rows (id, body, updated_at) VALUES (?, ?, ?)",
                params: ["row-1", "before", "2026-01-01T00:00:00.000000Z"]
            )
        }
        try db.applicationAuthoredWriteTransaction(
            tableName: "authored_rows",
            operation: "update",
            columnNames: ["body"]
        ) { transaction in
            try transaction.execute(
                "UPDATE authored_rows SET body = ?, support_value = ? WHERE id = ?",
                params: ["after", "runtime-support", "row-1"]
            )
        }
        try assertLedger(
            db,
            expectedOperations: ["insert", "update"],
            expectedFields: [["field-body"], ["field-body"]]
        )
        try db.applicationAuthoredWriteTransaction(
            tableName: "authored_rows",
            operation: "update",
            columnNames: ["body"]
        ) { transaction in
            try transaction.execute(
                "UPDATE authored_rows SET support_value = ? WHERE id = ?",
                params: ["another-support", "row-1"]
            )
        }
        try assertLedger(
            db,
            expectedOperations: ["insert", "update"],
            expectedFields: [["field-body"], ["field-body"]]
        )
    }

    func testInsertWithoutAnAuthoredWritableFieldAborts() throws {
        let db = try makeEnvironment()
        XCTAssertThrowsError(
            try db.applicationAuthoredWriteTransaction(
                tableName: "authored_rows",
                operation: "insert",
                columnNames: ["id"]
            ) { transaction in
                try transaction.execute(
                    "INSERT INTO authored_rows (id, updated_at) VALUES (?, ?)",
                    params: ["row-1", "2026-01-01T00:00:00.000000Z"]
                )
            }
        )
        let rows = try db.readTransaction { connection in
            try Int.fetchOne(connection, sql: "SELECT COUNT(*) FROM authored_rows")
        }
        XCTAssertEqual(rows, 0)
        try assertLedger(db, expectedOperations: [], expectedFields: [])
    }
}
