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

    private struct RollbackProbe: Error {}

    private func reopen(_ path: String) throws -> SynchroDatabase {
        let db = try SynchroDatabase(path: path)
        db.updateApplicationSyncedTables([authoredTable])
        return db
    }

    /// Removes the files only after a successful close, and reports every cleanup failure.
    private func closeAndRemove(_ db: SynchroDatabase) {
        do {
            try db.close()
            for suffix in ["", "-journal", "-wal", "-shm"]
            where FileManager.default.fileExists(atPath: db.path + suffix) {
                try FileManager.default.removeItem(atPath: db.path + suffix)
            }
        } catch {
            XCTFail("cleanup of \(db.path) failed: \(error)")
        }
    }

    private func storedRow(_ db: SynchroDatabase, id: String) throws -> [String?]? {
        try db.readTransaction { connection in
            try Row.fetchOne(
                connection,
                sql: "SELECT body, default_value, support_value FROM authored_rows WHERE id = ?",
                arguments: [id]
            ).map { row -> [String?] in [row["body"], row["default_value"], row["support_value"]] }
        }
    }

    /// Each entry is "operation field kind text" in capture order.
    private func capturedValues(_ db: SynchroDatabase) throws -> [String] {
        try db.readTransaction { connection in
            try Row.fetchAll(
                connection,
                sql: """
                    SELECT change.operation, value.field_id, value.value_kind, value.value_text
                    FROM _synchro_pending_changes AS change
                    JOIN _synchro_mutation_values AS value ON value.mutation_id = change.mutation_id
                    ORDER BY change.local_order, value.field_id
                    """
            ).map { row in
                let text: String? = row["value_text"]
                return "\(row["operation"] as String) \(row["field_id"] as String) "
                    + "\(row["value_kind"] as String) \(text ?? "NULL")"
            }
        }
    }

    private func transactionFailure(_ error: Error) -> String? {
        guard case let .databaseError(underlying)? = error as? SynchroError,
              let failure = underlying as? ApplicationTransactionError else { return nil }
        switch failure {
        case .writableQuery:
            return "writableQuery"
        case .captureContextCleanupFailed:
            return "captureContextCleanupFailed"
        case .captureContextRemains:
            return "captureContextRemains"
        }
    }

    private func insertRow(_ db: SynchroDatabase) throws {
        _ = try db.execute(
            "INSERT INTO authored_rows (id, body, updated_at) VALUES (?, ?, ?)",
            params: ["row-1", "before", "2026-01-01T00:00:00.000000Z"]
        )
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

    func testOrdinaryWritesCaptureOnlyStatementColumns() throws {
        let db = try makeEnvironment()
        _ = try db.execute(
            "INSERT INTO authored_rows (id, body, updated_at) VALUES (?, ?, ?)",
            params: ["row-1", "before", "2026-01-01T00:00:00.000000Z"]
        )
        _ = try db.execute(
            "UPDATE authored_rows SET support_value = ?, updated_at = ? WHERE id = ?",
            params: ["runtime-support", "2026-01-02T00:00:00.000000Z", "row-1"]
        )

        try assertLedger(
            db,
            expectedOperations: ["insert", "update"],
            expectedFields: [["field-body"], ["field-support"]]
        )
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

    /// D-01: an insert that authors only its key is a key-only create. The
    /// row keeps its local defaults, and the insert captures no authored field.
    func testKeyOnlyInsertCapturesAnInsertWithNoAuthoredField() throws {
        let db = try makeEnvironment()
        defer { closeAndRemove(db) }
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
        XCTAssertEqual(try storedRow(db, id: "row-1"), [nil, "default", ""])
        try assertLedger(db, expectedOperations: ["insert"], expectedFields: [[]])
    }

    /// An insert without its own capture context did not come through the SDK.
    /// Its authored fields are unknown, so it aborts before any row or intent.
    func testInsertWithoutItsOwnCaptureContextAborts() throws {
        let db = try makeEnvironment()
        defer { closeAndRemove(db) }
        XCTAssertThrowsError(try db.writeTransaction { connection in
            try connection.execute(
                sql: "INSERT INTO authored_rows (id, body, updated_at) VALUES (?, ?, ?)",
                arguments: ["row-1", "unauthored", "2026-01-01T00:00:00.000000Z"]
            )
        }) { error in
            XCTAssertTrue(
                String(describing: error).contains("synced insert has no authored capture context"),
                "\(error)"
            )
        }
        XCTAssertNil(try storedRow(db, id: "row-1"))
        try assertLedger(db, expectedOperations: [], expectedFields: [])
    }

    /// Issue #219: an ordinary statement installs no UPDATE context, so a
    /// synced-row change made by a local-table trigger is captured.
    func testOrdinaryLocalTriggerUpdateCapturesChangedWritableFields() throws {
        var db = try makeEnvironment()
        var isOpen = true
        defer { if isOpen { closeAndRemove(db) } }
        try insertRow(db)
        _ = try db.execute("CREATE TABLE local_commands (id TEXT PRIMARY KEY, body TEXT NOT NULL)", params: nil)
        _ = try db.execute(
            """
            CREATE TRIGGER local_commands_apply
            AFTER UPDATE OF body ON local_commands
            BEGIN
                UPDATE authored_rows SET body = NEW.body WHERE id = NEW.id;
            END
            """,
            params: nil
        )
        _ = try db.execute("INSERT INTO local_commands (id, body) VALUES ('row-1', 'queued')", params: nil)

        XCTAssertThrowsError(try db.applicationWriteTransaction { transaction in
            try transaction.execute(
                "UPDATE local_commands SET body = ? WHERE id = ?",
                params: ["rolled-back", "row-1"]
            )
            let inside = try transaction.queryOne(
                "SELECT COUNT(*) AS count FROM _synchro_pending_changes WHERE operation = 'update'"
            )
            XCTAssertEqual(inside?["count"] as Int64?, 1)
            throw RollbackProbe()
        }) { error in
            XCTAssertTrue(error is RollbackProbe)
        }
        XCTAssertEqual(try storedRow(db, id: "row-1"), ["before", "default", ""])
        try assertLedger(db, expectedOperations: ["insert"], expectedFields: [["field-body"]])

        _ = try db.execute(
            "UPDATE local_commands SET body = ? WHERE id = ?",
            params: ["after", "row-1"]
        )
        XCTAssertEqual(try storedRow(db, id: "row-1"), ["after", "default", ""])
        try assertLedger(
            db,
            expectedOperations: ["insert", "update"],
            expectedFields: [["field-body"], ["field-body"]]
        )
        XCTAssertEqual(
            try capturedValues(db),
            ["insert field-body text before", "update field-body text after"]
        )

        isOpen = false
        try db.close()
        db = try reopen(db.path)
        isOpen = true
        XCTAssertEqual(try storedRow(db, id: "row-1"), ["after", "default", ""])
        XCTAssertEqual(
            try capturedValues(db),
            ["insert field-body text before", "update field-body text after"]
        )
        _ = try db.execute(
            "UPDATE local_commands SET body = ? WHERE id = ?",
            params: ["reopened", "row-1"]
        )
        try assertLedger(
            db,
            expectedOperations: ["insert", "update", "update"],
            expectedFields: [["field-body"], ["field-body"], ["field-body"]]
        )
        XCTAssertEqual(
            try capturedValues(db),
            [
                "insert field-body text before",
                "update field-body text after",
                "update field-body text reopened",
            ]
        )
    }

    /// An explicit empty mask still installs a context row. Absence therefore
    /// cannot be inferred from the field rows.
    func testActiveEmptyMaskCapturesNoUpdateAndKeepsPrimaryKeyGuard() throws {
        let db = try makeEnvironment()
        defer { closeAndRemove(db) }
        try insertRow(db)

        _ = try db.applicationAuthoredWriteTransaction(
            tableName: "authored_rows",
            operation: "update",
            columnNames: []
        ) { transaction in
            try transaction.execute(
                "UPDATE authored_rows SET body = ?, support_value = ? WHERE id = ?",
                params: ["masked", "masked-support", "row-1"]
            )
        }
        XCTAssertEqual(try storedRow(db, id: "row-1"), ["masked", "default", "masked-support"])
        try assertLedger(db, expectedOperations: ["insert"], expectedFields: [["field-body"]])

        XCTAssertThrowsError(try db.applicationAuthoredWriteTransaction(
            tableName: "authored_rows",
            operation: "update",
            columnNames: []
        ) { transaction in
            try transaction.execute(
                "UPDATE authored_rows SET id = ? WHERE id = ?",
                params: ["row-2", "row-1"]
            )
        }) { error in
            XCTAssertEqual((error as? DatabaseError)?.extendedResultCode, .SQLITE_CONSTRAINT_TRIGGER)
        }
        XCTAssertEqual(try storedRow(db, id: "row-1"), ["masked", "default", "masked-support"])
        XCTAssertNil(try storedRow(db, id: "row-2"))
        try assertLedger(db, expectedOperations: ["insert"], expectedFields: [["field-body"]])
    }

    /// A context for another table is active. Absence therefore cannot be
    /// inferred from the updated table alone.
    func testOtherTableMaskKeepsUpdateCaptureMasked() throws {
        let db = try makeEnvironment()
        defer { closeAndRemove(db) }
        try insertRow(db)
        _ = try db.execute("CREATE TABLE local_commands (id TEXT PRIMARY KEY, body TEXT)", params: nil)

        _ = try db.applicationAuthoredWriteTransaction(
            tableName: "local_commands",
            operation: "update",
            columnNames: ["body"]
        ) { transaction in
            try transaction.execute(
                "UPDATE authored_rows SET body = ?, support_value = ? WHERE id = ?",
                params: ["masked", "masked-support", "row-1"]
            )
        }
        XCTAssertEqual(try storedRow(db, id: "row-1"), ["masked", "default", "masked-support"])
        try assertLedger(db, expectedOperations: ["insert"], expectedFields: [["field-body"]])
    }

    func testInferredInsertContextEndsWithItsStatement() throws {
        let db = try makeEnvironment()
        defer { closeAndRemove(db) }

        try db.applicationWriteTransaction { transaction in
            try transaction.execute(
                "INSERT INTO authored_rows (id, body, updated_at) VALUES (?, ?, ?)",
                params: ["row-1", "before", "2026-01-01T00:00:00.000000Z"]
            )
            try transaction.execute(
                "UPDATE authored_rows SET support_value = ? WHERE id = ?",
                params: ["runtime-support", "row-1"]
            )
        }
        XCTAssertEqual(try storedRow(db, id: "row-1"), ["before", "default", "runtime-support"])
        try assertLedger(
            db,
            expectedOperations: ["insert", "update"],
            expectedFields: [["field-body"], ["field-support"]]
        )
        XCTAssertEqual(
            try capturedValues(db),
            ["insert field-body text before", "update field-support text runtime-support"]
        )
    }

    func testCaughtStatementErrorClearsContextBeforeLaterCapture() throws {
        let db = try makeEnvironment()
        defer { closeAndRemove(db) }
        try insertRow(db)

        try db.applicationWriteTransaction { transaction in
            XCTAssertThrowsError(try transaction.execute(
                "INSERT INTO authored_rows (id, body, updated_at) VALUES (?, ?, ?)",
                params: ["row-1", "duplicate", "2026-01-01T00:00:00.000000Z"]
            )) { error in
                XCTAssertEqual((error as? DatabaseError)?.extendedResultCode, .SQLITE_CONSTRAINT_PRIMARYKEY)
            }
            try transaction.execute(
                "UPDATE authored_rows SET body = ?, support_value = ? WHERE id = ?",
                params: ["after", "runtime-support", "row-1"]
            )
        }
        XCTAssertEqual(try storedRow(db, id: "row-1"), ["after", "default", "runtime-support"])
        try assertLedger(
            db,
            expectedOperations: ["insert", "update"],
            expectedFields: [["field-body"], ["field-body", "field-support"]]
        )
        XCTAssertEqual(
            try capturedValues(db),
            [
                "insert field-body text before",
                "update field-body text after",
                "update field-support text runtime-support",
            ]
        )
    }

    /// A local trigger on the internal connection makes context cleanup fail
    /// without a production hook. The callback catches every statement error,
    /// so only the commit check can stop residual context and untracked changes.
    func testContextCleanupFailureCannotCommitCaughtWrites() throws {
        var db = try makeEnvironment()
        var isOpen = true
        defer { if isOpen { closeAndRemove(db) } }
        try insertRow(db)
        try db.writeTransaction { connection in
            try connection.execute(sql: """
                CREATE TRIGGER local_fail_context_clear
                BEFORE DELETE ON _synchro_capture_context
                BEGIN
                    SELECT RAISE(ABORT, 'local context clear failure');
                END
                """)
        }

        XCTAssertThrowsError(try db.applicationWriteTransaction { transaction in
            XCTAssertThrowsError(try transaction.execute(
                "INSERT INTO authored_rows (id, body, updated_at) VALUES (?, ?, ?)",
                params: ["row-1", "duplicate", "2026-01-01T00:00:00.000000Z"]
            )) { error in
                guard case let .databaseError(underlying)? = error as? SynchroError,
                      case let .captureContextCleanupFailed(cleanup, write)? =
                        underlying as? ApplicationTransactionError else {
                    return XCTFail("expected a cleanup failure, got \(error)")
                }
                XCTAssertEqual((cleanup as? DatabaseError)?.extendedResultCode, .SQLITE_CONSTRAINT_TRIGGER)
                XCTAssertEqual((write as? DatabaseError)?.extendedResultCode, .SQLITE_CONSTRAINT_PRIMARYKEY)
            }
            XCTAssertThrowsError(try transaction.execute(
                "INSERT INTO authored_rows (id, body, updated_at) VALUES (?, ?, ?)",
                params: ["row-2", "uncommitted", "2026-01-01T00:00:00.000000Z"]
            )) { error in
                guard case let .databaseError(underlying)? = error as? SynchroError,
                      case let .captureContextCleanupFailed(cleanup, write)? =
                        underlying as? ApplicationTransactionError else {
                    return XCTFail("expected a cleanup failure, got \(error)")
                }
                XCTAssertEqual((cleanup as? DatabaseError)?.extendedResultCode, .SQLITE_CONSTRAINT_TRIGGER)
                XCTAssertNil(write)
            }
            // The residual context masks this change, so a commit would leave it untracked.
            try transaction.execute(
                "UPDATE authored_rows SET body = ? WHERE id = ?",
                params: ["untracked", "row-1"]
            )
        }) { error in
            XCTAssertEqual(transactionFailure(error), "captureContextRemains")
        }

        XCTAssertEqual(try storedRow(db, id: "row-1"), ["before", "default", ""])
        XCTAssertNil(try storedRow(db, id: "row-2"))
        try assertLedger(db, expectedOperations: ["insert"], expectedFields: [["field-body"]])

        try db.writeTransaction { connection in
            try connection.execute(sql: "DROP TRIGGER local_fail_context_clear")
        }
        isOpen = false
        try db.close()
        db = try reopen(db.path)
        isOpen = true
        XCTAssertEqual(try storedRow(db, id: "row-1"), ["before", "default", ""])
        XCTAssertNil(try storedRow(db, id: "row-2"))
        try assertLedger(db, expectedOperations: ["insert"], expectedFields: [["field-body"]])
        _ = try db.execute(
            "UPDATE authored_rows SET body = ? WHERE id = ?",
            params: ["after", "row-1"]
        )
        XCTAssertEqual(
            try capturedValues(db),
            ["insert field-body text before", "update field-body text after"]
        )
    }

    func testTransactionQueriesRejectWritableStatementsBeforeExecution() throws {
        let db = try makeEnvironment()
        defer { closeAndRemove(db) }
        try insertRow(db)

        try db.applicationWriteTransaction { transaction in
            let rows = try transaction.query(
                "SELECT id, body FROM authored_rows WHERE id = ?",
                params: ["row-1"]
            )
            XCTAssertEqual(rows.map { [$0["id"] as String, $0["body"] as String] }, [["row-1", "before"]])
            XCTAssertEqual(
                try transaction.queryOne(
                    "SELECT default_value FROM authored_rows WHERE id = ?",
                    params: ["row-1"]
                )?["default_value"] as String?,
                "default"
            )
            XCTAssertThrowsError(try transaction.query(
                "UPDATE authored_rows SET body = ? WHERE id = ? RETURNING body",
                params: ["query-update", "row-1"]
            )) { error in
                XCTAssertEqual(transactionFailure(error), "writableQuery")
            }
            XCTAssertThrowsError(try transaction.queryOne(
                "INSERT INTO authored_rows (id, body, updated_at) VALUES (?, ?, ?) RETURNING id",
                params: ["row-2", "query-insert", "2026-01-01T00:00:00.000000Z"]
            )) { error in
                XCTAssertEqual(transactionFailure(error), "writableQuery")
            }
            XCTAssertEqual(
                try transaction.queryOne(
                    "SELECT COUNT(*) AS count FROM _synchro_pending_changes"
                )?["count"] as Int64?,
                1
            )
        }
        XCTAssertEqual(try storedRow(db, id: "row-1"), ["before", "default", ""])
        XCTAssertNil(try storedRow(db, id: "row-2"))
        try assertLedger(db, expectedOperations: ["insert"], expectedFields: [["field-body"]])
    }

    /// An application COMMIT would commit before the managed context check,
    /// and that check cannot roll back a committed change.
    func testManagedWritesRejectTransactionControlAndRollBack() throws {
        let db = try makeEnvironment()
        defer { closeAndRemove(db) }
        try insertRow(db)
        let transactionControl = [
            "COMMIT",
            "END",
            "ROLLBACK",
            "BEGIN IMMEDIATE",
            "SAVEPOINT application_savepoint",
            "RELEASE application_savepoint",
            "ROLLBACK TO application_savepoint",
        ]
        func assertTransactionControlDenied(_ transaction: ApplicationTransaction) {
            for sql in transactionControl {
                XCTAssertThrowsError(try transaction.execute(sql), sql) { error in
                    XCTAssertEqual((error as? DatabaseError)?.resultCode, .SQLITE_AUTH, sql)
                }
                XCTAssertThrowsError(try transaction.query(sql), sql) { error in
                    XCTAssertEqual((error as? DatabaseError)?.resultCode, .SQLITE_AUTH, sql)
                }
                XCTAssertThrowsError(try transaction.queryOne(sql), sql) { error in
                    XCTAssertEqual((error as? DatabaseError)?.resultCode, .SQLITE_AUTH, sql)
                }
            }
        }

        XCTAssertThrowsError(try db.applicationWriteTransaction { transaction in
            try transaction.execute(
                "UPDATE authored_rows SET body = ? WHERE id = ?",
                params: ["ordinary-uncommitted", "row-1"]
            )
            assertTransactionControlDenied(transaction)
            XCTAssertEqual(
                try transaction.queryOne("SELECT body FROM authored_rows WHERE id = 'row-1'")?["body"] as String?,
                "ordinary-uncommitted"
            )
            throw RollbackProbe()
        }) { error in
            XCTAssertTrue(error is RollbackProbe, "\(error)")
        }
        XCTAssertEqual(try storedRow(db, id: "row-1"), ["before", "default", ""])
        try assertLedger(db, expectedOperations: ["insert"], expectedFields: [["field-body"]])

        XCTAssertThrowsError(try db.applicationAuthoredWriteTransaction(
            tableName: "authored_rows",
            operation: "update",
            columnNames: ["body"]
        ) { transaction in
            try transaction.execute(
                "UPDATE authored_rows SET body = ? WHERE id = ?",
                params: ["authored-uncommitted", "row-1"]
            )
            assertTransactionControlDenied(transaction)
            XCTAssertEqual(
                try transaction.queryOne("SELECT COUNT(*) AS count FROM _synchro_capture_context")?["count"] as Int64?,
                1
            )
            throw RollbackProbe()
        }) { error in
            XCTAssertTrue(error is RollbackProbe, "\(error)")
        }
        XCTAssertEqual(try storedRow(db, id: "row-1"), ["before", "default", ""])
        try assertLedger(db, expectedOperations: ["insert"], expectedFields: [["field-body"]])

        // GRDB compiles BEGIN and COMMIT for this write, so the scope closed on both failure paths.
        _ = try db.execute(
            "UPDATE authored_rows SET body = ? WHERE id = ?",
            params: ["after", "row-1"]
        )
        XCTAssertEqual(
            try capturedValues(db),
            ["insert field-body text before", "update field-body text after"]
        )
    }
}
