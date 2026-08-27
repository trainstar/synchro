import XCTest
import os
@preconcurrency import GRDB
@testable import Synchro

final class SynchroClientTests: XCTestCase {
    private func makeConfig() -> SynchroConfig {
        let tmpDir = NSTemporaryDirectory()
        let path = (tmpDir as NSString).appendingPathComponent("synchro_client_test_\(UUID().uuidString).sqlite")
        return SynchroConfig(
            dbPath: path,
            serverURL: URL(string: "http://localhost:8080")!,
            authProvider: { "test-token" },
            clientID: "test-device",
            appVersion: "1.0.0"
        )
    }

    private func makeSeedConfig(databasePath: String, seedPath: String) -> SynchroConfig {
        SynchroConfig(
            dbPath: databasePath,
            serverURL: URL(string: "http://localhost:8080")!,
            authProvider: { "test-token" },
            clientID: "test-device",
            appVersion: "1.0.0",
            seedDatabasePath: seedPath
        )
    }

    private func makePortableSeed() throws -> String {
        let seedPath = (NSTemporaryDirectory() as NSString).appendingPathComponent("synchro_portable_seed_\(UUID().uuidString).sqlite")
        let database = try SynchroDatabase(path: seedPath)
        var manifest = protocolOrdersSchemaManifest()
        manifest.schemaHash = try Integrity.schemaManifestHash(manifest)
        let schema = SchemaResponse(
            schemaVersion: manifest.schemaVersion,
            schemaHash: manifest.schemaHash,
            serverTime: Date(),
            manifest: manifest
        )
        let schemaManager = SchemaManager(database: database)
        try schemaManager.createSyncedTables(schema: schema)

        let table = try XCTUnwrap(try manifest.localTables().first)
        let scopeID = "global"
        let recordID = "seed-order"
        let serverVersion = "2026-01-01T00:00:00.000000Z"
        let row: [String: AnyCodable] = [
            "field-id": AnyCodable(recordID),
            "field-ship-address": AnyCodable("Seed street"),
            "field-user-id": AnyCodable("seed-user"),
            "field-updated-at": AnyCodable(serverVersion),
            "field-deleted-at": AnyCodable(NSNull()),
        ]
        let rowDigest = try Integrity.rowDigest(
            schemaHash: manifest.schemaHash,
            table: table,
            pk: [table.primaryKeyFieldID: AnyCodable(recordID)],
            row: row,
            serverVersion: serverVersion
        )
        let scopeChecksum = try Integrity.scopeDigest(
            schemaHash: manifest.schemaHash,
            scopeID: scopeID,
            entries: [(identity: rowDigest.identity, digest: rowDigest.checksum)]
        )
        let scopeChecksumJSON = try checksumJSONString(scopeChecksum)
        let manifestJSON = try jsonString(manifest)

        try database.writeSyncLockedTransaction { db in
            try db.execute(
                sql: """
                    INSERT INTO orders (id, ship_address, user_id, updated_at, deleted_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                arguments: [recordID, "Seed street", "seed-user", serverVersion, nil]
            )
            try SynchroMeta.upsertRowVersion(
                db,
                tableName: table.tableName,
                recordID: recordID,
                serverVersion: serverVersion,
                rowChecksum: rowDigest.checksum
            )
            try SynchroMeta.upsertScope(
                db,
                scopeID: scopeID,
                cursor: nil,
                checksum: scopeChecksumJSON,
                generation: 0,
                localChecksum: scopeChecksumJSON
            )
            try SynchroMeta.upsertScopeRow(
                db,
                scopeID: scopeID,
                tableName: table.tableName,
                recordID: recordID,
                checksum: rowDigest.checksum.digest,
                generation: 0
            )
            try db.execute(
                sql: """
                    INSERT INTO _synchro_seed_receipts
                        (scope_id, receipt, schema_version, schema_hash, cardinality, checksum)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                arguments: [scopeID, "seed-receipt", manifest.schemaVersion, manifest.schemaHash, 1, scopeChecksumJSON]
            )
            try SynchroMeta.set(db, key: .schemaManifest, value: manifestJSON)
            try SynchroMeta.setInt64(db, key: .scopeSetVersion, value: 0)
            try SynchroMeta.set(db, key: .snapshotComplete, value: "1")
        }

        try demotePortableSeedToVersionEight(database)
        try database.close()
        try removeSQLiteSidecars(at: seedPath)
        return seedPath
    }

    private func demotePortableSeedToVersionEight(_ database: SynchroDatabase) throws {
        try database.writeTransaction { db in
            for trigger in [
                "_synchro_cdc_insert_orders",
                "_synchro_cdc_update_orders",
                "_synchro_cdc_delete_orders",
            ] {
                try db.execute(sql: "DROP TRIGGER IF EXISTS \(SQLiteHelpers.quoteIdentifier(trigger))")
            }
            try db.execute(sql: "DROP TABLE _synchro_mutation_values")
            try db.execute(sql: "DROP TABLE _synchro_push_batch_members")
            try db.execute(sql: "DROP TABLE _synchro_schema_archive")
            try db.execute(sql: "DROP TABLE _synchro_rebuild_page_receipts")
            try db.execute(sql: "DROP TABLE _synchro_backoff")
            try db.execute(sql: "DROP TABLE _synchro_blocking_error")
            try db.execute(sql: "DROP TABLE _synchro_schema_migration")
            try db.execute(sql: "DROP TABLE _synchro_pending_changes")
            try db.execute(sql: """
                CREATE TABLE _synchro_pending_changes (
                    record_id TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    base_updated_at TEXT,
                    client_updated_at TEXT NOT NULL,
                    local_revision INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (table_name, record_id)
                )
                """)
            try db.execute(sql: "DROP TABLE _synchro_rejected_mutations")
            try db.execute(sql: """
                CREATE TABLE _synchro_rejected_mutations (
                    mutation_id TEXT PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    record_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    code TEXT NOT NULL,
                    message TEXT,
                    server_row_json TEXT,
                    server_version TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """)
            try db.execute(sql: """
                CREATE INDEX idx_synchro_rejected_mutations_record
                ON _synchro_rejected_mutations (table_name, record_id)
                """)
            try db.execute(
                sql: "DELETE FROM grdb_migrations WHERE identifier IN (?, ?, ?, ?, ?)",
                arguments: [
                    "synchro_v9_mutation_ledger",
                    "synchro_v10_rebuild_page_receipts",
                    "synchro_v11_durable_backoff",
                    "synchro_v12_gate2_recovery",
                    "synchro_v13_scope_text_affinity",
                ]
            )
        }
    }

    private func assertSeedRejection(
        _ mutate: (GRDB.Database) throws -> Void,
        file: StaticString = #filePath,
        line: UInt = #line
    ) throws {
        let seedPath = try makePortableSeed()
        let destinationPath = (NSTemporaryDirectory() as NSString).appendingPathComponent("synchro_destination_\(UUID().uuidString).sqlite")
        defer {
            try? removeDatabaseFamily(at: seedPath)
            try? removeDatabaseFamily(at: destinationPath)
        }

        let seed = try DatabaseQueue(path: seedPath)
        try seed.write(mutate)
        try seed.close()
        try removeSQLiteSidecars(at: seedPath)
        let sourceBeforeInstall = try Data(contentsOf: URL(fileURLWithPath: seedPath))

        XCTAssertThrowsError(
            try SynchroClient(config: makeSeedConfig(databasePath: destinationPath, seedPath: seedPath)),
            file: file,
            line: line
        )
        XCTAssertEqual(
            try Data(contentsOf: URL(fileURLWithPath: seedPath)),
            sourceBeforeInstall,
            file: file,
            line: line
        )
        assertNoDatabaseFamily(at: destinationPath, file: file, line: line)
    }

    private func jsonString<Value: Encodable>(_ value: Value) throws -> String {
        let data = try JSONEncoder.synchroEncoder().encode(value)
        return try XCTUnwrap(String(data: data, encoding: .utf8))
    }

    private func checksumJSONString(_ checksum: ChecksumObject) throws -> String {
        try jsonString(checksum)
    }

    private func removeDatabaseFamily(at path: String) throws {
        let fileManager = FileManager.default
        if fileManager.fileExists(atPath: path) {
            try fileManager.removeItem(atPath: path)
        }
        try removeSQLiteSidecars(at: path)
    }

    private func removeSQLiteSidecars(at path: String) throws {
        let fileManager = FileManager.default
        for suffix in ["-journal", "-wal", "-shm"] {
            let sidecar = path + suffix
            if fileManager.fileExists(atPath: sidecar) {
                try fileManager.removeItem(atPath: sidecar)
            }
        }
    }

    private func assertNoDatabaseFamily(
        at path: String,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        for suffix in ["", "-journal", "-wal", "-shm"] {
            XCTAssertFalse(FileManager.default.fileExists(atPath: path + suffix), file: file, line: line)
        }
    }

    private func assertNoSQLiteSidecars(
        at path: String,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        for suffix in ["-journal", "-wal", "-shm"] {
            XCTAssertFalse(FileManager.default.fileExists(atPath: path + suffix), file: file, line: line)
        }
    }

    func testSeedInstallationValidatesAndPublishesDatabase() async throws {
        let seedPath = try makePortableSeed()
        let databasePath = (NSTemporaryDirectory() as NSString).appendingPathComponent("synchro_destination_\(UUID().uuidString).sqlite")
        defer {
            try? removeDatabaseFamily(at: seedPath)
            try? removeDatabaseFamily(at: databasePath)
        }
        let sourceBeforeInstall = try Data(contentsOf: URL(fileURLWithPath: seedPath))

        let client = try SynchroClient(config: makeSeedConfig(databasePath: databasePath, seedPath: seedPath))
        XCTAssertTrue(FileManager.default.fileExists(atPath: databasePath))
        XCTAssertEqual(try client.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: ["seed-order"])?["ship_address"] as? String, "Seed street")
        let migrations = try client.query("SELECT identifier FROM grdb_migrations", params: nil)
        let identifiers = Set(migrations.compactMap { $0["identifier"] as? String })
        XCTAssertTrue(identifiers.contains("synchro_v9_mutation_ledger"))
        XCTAssertTrue(identifiers.contains("synchro_v10_rebuild_page_receipts"))
        XCTAssertTrue(identifiers.contains("synchro_v11_durable_backoff"))
        XCTAssertTrue(identifiers.contains("synchro_v12_gate2_recovery"))
        XCTAssertTrue(identifiers.contains("synchro_v13_scope_text_affinity"))
        XCTAssertEqual(try Data(contentsOf: URL(fileURLWithPath: seedPath)), sourceBeforeInstall)
        assertNoSQLiteSidecars(at: seedPath)
        try await client.close()
    }

    func testCorruptSeedDoesNotPublishDatabase() throws {
        let seedPath = (NSTemporaryDirectory() as NSString).appendingPathComponent("synchro_corrupt_seed_\(UUID().uuidString).sqlite")
        let databasePath = (NSTemporaryDirectory() as NSString).appendingPathComponent("synchro_destination_\(UUID().uuidString).sqlite")
        try Data("not a SQLite database".utf8).write(to: URL(fileURLWithPath: seedPath))

        XCTAssertThrowsError(try SynchroClient(config: makeSeedConfig(databasePath: databasePath, seedPath: seedPath)))
        XCTAssertFalse(FileManager.default.fileExists(atPath: databasePath))
        XCTAssertFalse(FileManager.default.fileExists(atPath: databasePath + "-journal"))
        XCTAssertFalse(FileManager.default.fileExists(atPath: databasePath + "-wal"))
        XCTAssertFalse(FileManager.default.fileExists(atPath: databasePath + "-shm"))
    }

    func testSeedMissingRequiredSynchroStateDoesNotPublishDatabase() throws {
        try assertSeedRejection { db in
            try db.execute(sql: "DROP TABLE _synchro_seed_receipts")
        }
    }

    func testSeedRejectsSchemaManifestMetadataMismatchWithoutPublication() throws {
        try assertSeedRejection { db in
            try SynchroMeta.set(db, key: .schemaHash, value: String(repeating: "f", count: 64))
        }
    }

    func testSeedRejectsScopeReceiptMismatchWithoutPublication() throws {
        try assertSeedRejection { db in
            try db.execute(sql: "UPDATE _synchro_seed_receipts SET cardinality = cardinality + 1")
        }
    }

    func testSeedRejectsRowChecksumCorruptionWithoutPublication() throws {
        try assertSeedRejection { db in
            let checksum = try self.jsonString(ChecksumObject(
                algorithm: "sha256",
                version: 1,
                encoding: "hex",
                digest: String(repeating: "0", count: 64)
            ))
            try db.execute(sql: "UPDATE _synchro_row_versions SET row_checksum = ?", arguments: [checksum])
        }
    }

    func testSeedRejectsScopeProvenanceCorruptionWithoutPublication() throws {
        try assertSeedRejection { db in
            try db.execute(sql: "DELETE FROM _synchro_scope_rows")
        }
    }

    func testClientInitCreatesDatabase() async throws {
        let config = makeConfig()
        let client = try SynchroClient(config: config)

        let rows = try client.query("SELECT name FROM sqlite_master WHERE type='table'", params: nil)
        let tableNames = rows.map { $0["name"] as! String }
        XCTAssertTrue(tableNames.contains("_synchro_pending_changes"))
        XCTAssertTrue(tableNames.contains("_synchro_meta"))

        try await client.close()
    }

    func testClosePermanentlyRejectsFutureSyncOperations() async throws {
        let client = try SynchroClient(config: makeConfig())
        try await client.close()

        do {
            try await client.start()
            XCTFail("start must reject after close")
        } catch let error as SynchroError {
            guard case .notStarted = error else {
                XCTFail("start returned the wrong error after close")
                return
            }
        }

        do {
            try await client.syncNow()
            XCTFail("syncNow must reject after close")
        } catch let error as SynchroError {
            guard case .notStarted = error else {
                XCTFail("syncNow returned the wrong error after close")
                return
            }
        }
    }

    func testCoreSQL() async throws {
        let config = makeConfig()
        let client = try SynchroClient(config: config)

        try client.createTable("local_notes", columns: [
            ColumnDef(name: "id", type: "TEXT", nullable: false, primaryKey: true),
            ColumnDef(name: "body", type: "TEXT"),
        ])

        let result = try client.execute(
            "INSERT INTO local_notes (id, body) VALUES (?, ?)",
            params: ["n1", "hello"]
        )
        XCTAssertEqual(result.rowsAffected, 1)

        let rows = try client.query("SELECT * FROM local_notes WHERE id = ?", params: ["n1"])
        XCTAssertEqual(rows.count, 1)
        XCTAssertEqual(rows[0]["body"] as? String, "hello")

        let one = try client.queryOne("SELECT * FROM local_notes WHERE id = ?", params: ["n1"])
        XCTAssertNotNil(one)

        let nullBody: String? = nil
        let nullResult = try client.execute(
            "INSERT INTO local_notes (id, body) VALUES (?, ?)",
            params: ["n2", nullBody]
        )
        XCTAssertEqual(nullResult.rowsAffected, 1)

        let nullRows = try client.query(
            "SELECT id FROM local_notes WHERE id = ? AND body IS ?",
            params: ["n2", nullBody]
        )
        XCTAssertEqual(nullRows.count, 1)

        try await client.close()
    }

    func testBatchExecution() async throws {
        let config = makeConfig()
        let client = try SynchroClient(config: config)

        try client.createTable("orders", columns: [
            ColumnDef(name: "id", type: "TEXT", nullable: false, primaryKey: true),
            ColumnDef(name: "value", type: "INTEGER"),
        ])

        let total = try client.executeBatch([
            SQLStatement(sql: "INSERT INTO orders (id, value) VALUES (?, ?)", params: ["a", 1]),
            SQLStatement(sql: "INSERT INTO orders (id, value) VALUES (?, ?)", params: ["b", 2]),
            SQLStatement(sql: "INSERT INTO orders (id, value) VALUES (?, ?)", params: ["c", nil as Int?]),
        ])
        XCTAssertEqual(total, 3)

        let rows = try client.query("SELECT COUNT(*) as cnt FROM orders", params: nil)
        XCTAssertEqual(rows[0]["cnt"] as? Int64, 3)

        let nullRows = try client.query("SELECT id FROM orders WHERE value IS ?", params: [nil as Int?])
        XCTAssertEqual(nullRows.first?["id"] as? String, "c")

        try await client.close()
    }

    func testMetaTablesInitialized() async throws {
        let config = makeConfig()
        let client = try SynchroClient(config: config)

        let lockRow = try client.queryOne("SELECT value FROM _synchro_meta WHERE key = 'sync_lock'", params: nil)
        XCTAssertEqual(lockRow?["value"] as? String, "0")

        let cpRow = try client.queryOne("SELECT value FROM _synchro_meta WHERE key = 'checkpoint'", params: nil)
        XCTAssertEqual(cpRow?["value"] as? String, "0")

        try await client.close()
    }

    func testCreateIndex() async throws {
        let config = makeConfig()
        let client = try SynchroClient(config: config)

        try client.createTable("orders", columns: [
            ColumnDef(name: "id", type: "TEXT", nullable: false, primaryKey: true),
            ColumnDef(name: "category", type: "TEXT"),
        ])

        try client.createIndex("orders", columns: ["category"], unique: false)

        let indexes = try client.query("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='orders'")
        let names = indexes.map { $0["name"] as String }
        XCTAssertTrue(names.contains("idx_orders_category"))

        try await client.close()
    }

    func testOnChange() async throws {
        let config = makeConfig()
        let client = try SynchroClient(config: config)

        try client.createTable("events", columns: [
            ColumnDef(name: "id", type: "TEXT", nullable: false, primaryKey: true),
            ColumnDef(name: "name", type: "TEXT"),
        ])

        let expectation = XCTestExpectation(description: "onChange fires")
        let cancellable = client.onChange(tables: ["events"]) {
            expectation.fulfill()
        }

        _ = try client.execute("INSERT INTO events (id, name) VALUES (?, ?)", params: ["e1", "test"])

        await fulfillment(of: [expectation], timeout: 2.0)
        cancellable.cancel()
        try await client.close()
    }

    func testWatch() throws {
        let config = makeConfig()
        let client = try SynchroClient(config: config)
        addTeardownBlock { try? await client.close() }

        try client.createTable("counters", columns: [
            ColumnDef(name: "id", type: "TEXT", nullable: false, primaryKey: true),
            ColumnDef(name: "value", type: "INTEGER"),
        ])

        // Insert initial row
        _ = try client.execute("INSERT INTO counters (id, value) VALUES (?, ?)", params: ["c1", 0])

        let expectation = XCTestExpectation(description: "watch fires with updated data")
        expectation.expectedFulfillmentCount = 2 // initial + after update

        let receivedRows = OSAllocatedUnfairLock(initialState: [[GRDB.Row]]())
        let cancellable = client.watch(
            "SELECT * FROM counters WHERE id = ?",
            params: ["c1"],
            tables: ["counters"]
        ) { rows in
            receivedRows.withLock { $0.append(rows) }
            expectation.fulfill()
        }
        // Trigger an update
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.3) {
            _ = try? client.execute("UPDATE counters SET value = ? WHERE id = ?", params: [42, "c1"])
        }

        wait(for: [expectation], timeout: 3.0)

        let receivedRowsSnapshot = receivedRows.withLock { $0 }
        XCTAssertGreaterThanOrEqual(receivedRowsSnapshot.count, 2)
        // Last callback should have the updated value
        if let lastRows = receivedRowsSnapshot.last, let row = lastRows.first {
            XCTAssertEqual(row["value"] as Int, 42)
        }

        cancellable.cancel()
    }

    func testWatchPreservesNullBindSlots() throws {
        let config = makeConfig()
        let client = try SynchroClient(config: config)
        addTeardownBlock { try? await client.close() }

        try client.createTable("nullable_counters", columns: [
            ColumnDef(name: "id", type: "TEXT", nullable: false, primaryKey: true),
            ColumnDef(name: "note", type: "TEXT"),
        ])

        let nullNote: String? = nil
        _ = try client.execute(
            "INSERT INTO nullable_counters (id, note) VALUES (?, ?)",
            params: ["c1", nullNote]
        )

        let observed = OSAllocatedUnfairLock(initialState: false)
        let cancellable = client.watch(
            "SELECT id FROM nullable_counters WHERE id = ? AND note IS ?",
            params: ["c1", nullNote],
            tables: ["nullable_counters"]
        ) { rows in
            _ = rows
            observed.withLock { $0 = true }
        }

        _ = try client.execute(
            "UPDATE nullable_counters SET note = ? WHERE id = ?",
            params: [nullNote, "c1"]
        )

        let deadline = Date().addingTimeInterval(2.0)
        while !observed.withLock({ $0 }) && Date() < deadline {
            // GRDB delivers observation callbacks asynchronously on the main run loop.
            RunLoop.current.run(until: Date().addingTimeInterval(0.01))
        }
        XCTAssertTrue(observed.withLock { $0 })
        cancellable.cancel()
    }

    // MARK: - Schema

    func testAlterTable() async throws {
        let config = makeConfig()
        let client = try SynchroClient(config: config)

        try client.createTable("people", columns: [
            ColumnDef(name: "id", type: "TEXT", nullable: false, primaryKey: true),
            ColumnDef(name: "name", type: "TEXT"),
        ])

        try client.alterTable("people", addColumns: [
            ColumnDef(name: "age", type: "INTEGER"),
        ])

        _ = try client.execute("INSERT INTO people (id, name, age) VALUES (?, ?, ?)", params: ["p1", "Alice", 30])
        let row = try client.queryOne("SELECT age FROM people WHERE id = ?", params: ["p1"])
        XCTAssertEqual(row?["age"] as Int?, 30)

        try await client.close()
    }

    func testTransactions() async throws {
        let config = makeConfig()
        let client = try SynchroClient(config: config)

        try client.createTable("txtest", columns: [
            ColumnDef(name: "id", type: "TEXT", nullable: false, primaryKey: true),
            ColumnDef(name: "val", type: "TEXT"),
        ])

        // Write transaction
        let written = try client.writeTransaction { db -> Int in
            try db.execute(
                "INSERT INTO txtest (id, val) VALUES (?, ?)",
                params: ["t1", "hello"]
            ).rowsAffected
        }
        XCTAssertEqual(written, 1)

        // Read transaction
        let value = try client.readTransaction { db -> String? in
            let row = try GRDB.Row.fetchOne(db, sql: "SELECT val FROM txtest WHERE id = ?", arguments: ["t1"])
            return row?["val"]
        }
        XCTAssertEqual(value, "hello")

        enum IntentionalRollback: Error {
            case rollback
        }

        XCTAssertThrowsError(try client.writeTransaction { db -> Void in
            try db.execute(
                "INSERT INTO txtest (id, val) VALUES (?, ?)",
                params: ["t2", "rolled-back"]
            )
            throw IntentionalRollback.rollback
        })

        let rolledBackRow = try client.queryOne("SELECT val FROM txtest WHERE id = ?", params: ["t2"])
        XCTAssertNil(rolledBackRow)

        try await client.close()
    }

}
