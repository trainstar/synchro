import XCTest
import GRDB
@testable import Synchro

final class PullProcessorTests: XCTestCase {
    private let testTable = SchemaTable(
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

    private let bytesTable = SchemaTable(
        tableName: "binary_rows",
        updatedAtColumn: "updated_at",
        deletedAtColumn: "deleted_at",
        primaryKey: ["id"],
        columns: [
            SchemaColumn(name: "id", logicalType: "string", nullable: false, isPrimaryKey: true),
            SchemaColumn(name: "payload", logicalType: "bytes", nullable: false, isPrimaryKey: false),
            SchemaColumn(name: "updated_at", logicalType: "datetime", nullable: false, isPrimaryKey: false),
            SchemaColumn(name: "deleted_at", logicalType: "datetime", nullable: true, isPrimaryKey: false),
        ]
    )

    private func makeTestEnv() throws -> (SynchroDatabase, PullProcessor) {
        let tmpDir = NSTemporaryDirectory()
        let path = (tmpDir as NSString).appendingPathComponent("synchro_test_\(UUID().uuidString).sqlite")
        let db = try SynchroDatabase(path: path)
        let manager = SchemaManager(database: db)
        let schema = SchemaResponse(schemaVersion: 1, schemaHash: protocolTestSchemaHash, serverTime: Date(), tables: [testTable])
        try manager.createSyncedTables(schema: schema)
        return (db, PullProcessor(database: db))
    }

    private func insertOrder(
        _ db: SynchroDatabase,
        id: String,
        shipAddress: String = "123 Main St",
        updatedAt: String,
        deletedAt: String? = nil
    ) throws {
        try db.writeSyncLockedTransaction { conn in
            try conn.execute(
                sql: "INSERT INTO orders (id, ship_address, updated_at, deleted_at) VALUES (?, ?, ?, ?)",
                arguments: [id, shipAddress, updatedAt, deletedAt]
            )
        }
    }

    private func addScopeRow(
        _ db: SynchroDatabase,
        scopeID: String,
        recordID: String,
        tableName: String = "orders",
        checksum: String = "{\"algorithm\":\"sha256\",\"digest\":\"0000000000000000000000000000000000000000000000000000000000000000\",\"encoding\":\"hex\",\"version\":1}",
        generation: Int64 = 0
    ) throws {
        try db.writeTransaction { conn in
            try SynchroMeta.upsertScope(conn, scopeID: scopeID, cursor: "10", checksum: nil, generation: generation)
            try SynchroMeta.upsertScopeRow(
                conn,
                scopeID: scopeID,
                tableName: tableName,
                recordID: recordID,
                checksum: checksum,
                generation: generation
            )
        }
    }

    private func pendingChangeCount(_ db: SynchroDatabase) throws -> Int {
        try ChangeTracker(database: db).pendingChangeCount()
    }

    private func makeTestEnv(schema table: LocalSchemaTable) throws -> (SynchroDatabase, PullProcessor) {
        let tmpDir = NSTemporaryDirectory()
        let path = (tmpDir as NSString).appendingPathComponent("synchro_test_\(UUID().uuidString).sqlite")
        let db = try SynchroDatabase(path: path)
        let manager = SchemaManager(database: db)
        let schema = SchemaResponse(schemaVersion: 1, schemaHash: protocolTestSchemaHash, serverTime: Date(), tables: [table])
        try manager.createSyncedTables(schema: schema)
        return (db, PullProcessor(database: db))
    }

    private func addPendingIntent(
        _ db: SynchroDatabase,
        table: LocalSchemaTable,
        recordID: String,
        state: String
    ) throws {
        let mutationID = UUID().uuidString.lowercased()
        let timestamp = "2026-01-01T00:00:00.000000Z"
        try db.writeTransaction { connection in
            try connection.execute(
                sql: """
                    INSERT INTO _synchro_pending_changes
                        (mutation_id, capture_uuid, table_id, table_name, record_id, pk_field_id, pk_logical_type,
                         operation, authored_schema_version, authored_schema_hash, base_version, client_version,
                         lifecycle_state, source_kind, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'update', 1, ?, ?, ?, ?, 'test', ?, ?)
                    """,
                arguments: [
                    mutationID, mutationID, table.tableID, table.tableName, recordID,
                    table.primaryKeyFieldID, "string", protocolTestSchemaHash, "server-version-1",
                    timestamp, state, timestamp, timestamp,
                ]
            )
        }
    }

    private func scopeChecksum(
        scopeID: String,
        schema: LocalSchemaTable,
        row: [String: AnyCodable],
        serverVersion: String
    ) throws -> ChecksumObject {
        let pk = [schema.primaryKeyFieldID: row[schema.primaryKeyFieldID]!]
        let rowChecksum = try Integrity.rowDigest(
            schemaHash: protocolTestSchemaHash,
            table: schema,
            pk: pk,
            row: row,
            serverVersion: serverVersion
        )
        return try Integrity.scopeDigest(
            schemaHash: protocolTestSchemaHash,
            scopeID: scopeID,
            entries: [(identity: rowChecksum.identity, digest: rowChecksum.checksum)]
        )
    }

    private func rebuildRequestBody(_ request: RebuildRequest) throws -> Data {
        try JSONEncoder.synchroEncoder().encode(request)
    }

    private func rebuildResponseBody(_ response: RebuildResponse) throws -> Data {
        try JSONEncoder.synchroEncoder().encode(response)
    }

    func testUpdateCheckpointAdvancesForward() throws {
        let (db, processor) = try makeTestEnv()

        try processor.updateCheckpoint(100)
        let cp1 = try db.readTransaction { conn in
            try SynchroMeta.getInt64(conn, key: .checkpoint)
        }
        XCTAssertEqual(cp1, 100)

        // Should not go backward
        try processor.updateCheckpoint(50)
        let cp2 = try db.readTransaction { conn in
            try SynchroMeta.getInt64(conn, key: .checkpoint)
        }
        XCTAssertEqual(cp2, 100)

        // Should advance forward
        try processor.updateCheckpoint(200)
        let cp3 = try db.readTransaction { conn in
            try SynchroMeta.getInt64(conn, key: .checkpoint)
        }
        XCTAssertEqual(cp3, 200)
    }

    func testProtectedUpsertPreservesProjectionAndRefreshesServerMetadata() throws {
        for state in ["unsealed", "sealed", "blocked_by_predecessor", "legacy_blocked"] {
            let (db, processor) = try makeTestEnv()
            let scopeID = "orders:user1"
            let recordID = "protected-\(state)"
            try addScopeRow(db, scopeID: scopeID, recordID: recordID)
            try insertOrder(db, id: recordID, shipAddress: "local", updatedAt: "2026-01-01T00:00:00.000000Z")
            try addPendingIntent(db, table: testTable, recordID: recordID, state: state)

            let serverVersion = "2026-01-02T00:00:00.000000Z"
            let serverRow: [String: AnyCodable] = [
                "id": AnyCodable(recordID),
                "ship_address": AnyCodable("server"),
                "updated_at": AnyCodable(serverVersion),
                "deleted_at": AnyCodable(NSNull()),
            ]
            let change = try makeChangeRecord(
                scope: scopeID,
                schema: testTable,
                op: .upsert,
                pk: ["id": AnyCodable(recordID)],
                row: serverRow,
                serverVersion: serverVersion
            )

            try processor.applyScopeChanges(
                changes: [change],
                syncedTables: [testTable.localSchema],
                scopeCursors: [scopeID: "11"],
                checksums: nil,
                schemaHash: protocolTestSchemaHash
            )

            let row = try db.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: [recordID])
            XCTAssertEqual(row?["ship_address"] as String?, "local")
            let version = try db.queryOne(
                "SELECT server_version FROM _synchro_row_versions WHERE table_name = ? AND record_id = ?",
                params: [testTable.tableName, recordID]
            )
            XCTAssertEqual(version?["server_version"] as String?, serverVersion)
            let scopeRow = try db.queryOne(
                "SELECT checksum FROM _synchro_scope_rows WHERE scope_id = ? AND record_id = ?",
                params: [scopeID, recordID]
            )
            XCTAssertEqual(scopeRow?["checksum"] as String?, change.rowChecksum?.digest)

            try db.writeTransaction { connection in
                try connection.execute(
                    sql: "UPDATE _synchro_pending_changes SET lifecycle_state = 'accepted' WHERE table_name = ? AND record_id = ?",
                    arguments: [testTable.tableName, recordID]
                )
            }
            try processor.removeScope(scopeID: scopeID, syncedTables: [testTable.localSchema])
            XCTAssertNil(try db.queryOne("SELECT id FROM orders WHERE id = ?", params: [recordID]))
        }
    }

    func testProtectedRowlessDeleteRemovesProvenanceAndUnprotectedRow() throws {
        let (db, processor) = try makeTestEnv()
        let scopeID = "orders:user1"
        try addScopeRow(db, scopeID: scopeID, recordID: "protected")
        try addScopeRow(db, scopeID: scopeID, recordID: "unprotected")
        try insertOrder(db, id: "protected", shipAddress: "local", updatedAt: "2026-01-01T00:00:00.000000Z")
        try insertOrder(db, id: "unprotected", shipAddress: "local", updatedAt: "2026-01-01T00:00:00.000000Z")
        try addPendingIntent(db, table: testTable, recordID: "protected", state: "sealed")

        let changes = try ["protected", "unprotected"].map { recordID in
            try makeChangeRecord(
                scope: scopeID,
                schema: testTable,
                op: .delete,
                pk: ["id": AnyCodable(recordID)],
                row: nil,
                serverVersion: "2026-01-02T00:00:00.000000Z"
            )
        }
        try processor.applyScopeChanges(
            changes: changes,
            syncedTables: [testTable.localSchema],
            scopeCursors: [scopeID: "11"],
            checksums: nil,
            schemaHash: protocolTestSchemaHash
        )

        XCTAssertNotNil(try db.queryOne("SELECT id FROM orders WHERE id = 'protected'", params: nil))
        XCTAssertNil(try db.queryOne("SELECT id FROM orders WHERE id = 'unprotected'", params: nil))
        XCTAssertEqual(
            try db.query("SELECT record_id FROM _synchro_scope_rows WHERE scope_id = ?", params: [scopeID]).count,
            0
        )
    }

    func testPullRejectsPushOperationsWithoutChangingRows() throws {
        let (db, processor) = try makeTestEnv()
        let row: [String: AnyCodable] = [
            "id": AnyCodable("w1"),
            "ship_address": AnyCodable("server"),
            "updated_at": AnyCodable("2026-01-01T12:00:00.000000Z"),
            "deleted_at": AnyCodable(NSNull()),
        ]
        let change = try makeChangeRecord(
            scope: "orders:user1",
            schema: testTable.localSchema,
            op: .insert,
            pk: ["id": AnyCodable("w1")],
            row: row,
            serverVersion: "server-version"
        )

        XCTAssertThrowsError(try processor.applyScopeChanges(
            changes: [change],
            syncedTables: [testTable.localSchema],
            scopeCursors: [:],
            checksums: nil,
            schemaHash: protocolTestSchemaHash
        ))
        XCTAssertNil(try db.queryOne("SELECT id FROM orders WHERE id = ?", params: ["w1"]))
    }

    func testPullRejectsRowPrimaryKeyDifferentFromResponsePrimaryKey() throws {
        let (db, processor) = try makeTestEnv()
        let schema = testTable.localSchema
        let row: [String: AnyCodable] = [
            "id": AnyCodable("row-id"),
            "ship_address": AnyCodable("server"),
            "updated_at": AnyCodable("2026-01-01T12:00:00.000000Z"),
            "deleted_at": AnyCodable(NSNull()),
        ]
        let checksum = try Integrity.rowDigest(
            schemaHash: protocolTestSchemaHash,
            table: schema,
            pk: ["id": AnyCodable("row-id")],
            row: row,
            serverVersion: "server-version"
        ).checksum
        let change = ChangeRecord(
            scope: "orders:user1",
            table: schema.tableID,
            op: .upsert,
            pk: ["id": AnyCodable("response-id")],
            row: row,
            rowChecksum: checksum,
            serverVersion: "server-version"
        )

        XCTAssertThrowsError(try processor.applyScopeChanges(
            changes: [change],
            syncedTables: [schema],
            scopeCursors: [:],
            checksums: nil,
            schemaHash: protocolTestSchemaHash
        ))
        XCTAssertTrue(try db.query("SELECT id FROM orders", params: nil).isEmpty)
    }

    func testPullPageRollsBackAssignmentCleanupWhenChecksumValidationFails() throws {
        let (db, processor) = try makeTestEnv()
        let removedScope = "orders:removed"
        let retainedScope = "orders:retained"
        try addScopeRow(db, scopeID: removedScope, recordID: "w1")
        try insertOrder(db, id: "w1", updatedAt: "2026-01-01T12:00:00.000000Z")
        try db.writeTransaction { conn in
            try SynchroMeta.upsertScope(conn, scopeID: retainedScope, cursor: "10", checksum: nil)
            try SynchroMeta.setInt64(conn, key: .scopeSetVersion, value: 1)
        }
        let invalidChecksum = ChecksumObject(
            algorithm: "md5",
            version: 1,
            encoding: "hex",
            digest: String(repeating: "0", count: 64)
        )

        XCTAssertThrowsError(try processor.applyScopeChanges(
            changes: [],
            syncedTables: [testTable.localSchema],
            scopeCursors: [:],
            checksums: [retainedScope: invalidChecksum],
            schemaHash: protocolTestSchemaHash,
            scopeUpdates: ScopeAssignmentDelta(add: [], remove: [removedScope]),
            scopeSetVersion: 2
        ))

        XCTAssertNotNil(try db.queryOne("SELECT id FROM orders WHERE id = ?", params: ["w1"]))
        try db.readTransaction { conn in
            XCTAssertNotNil(try SynchroMeta.getScope(conn, scopeID: removedScope))
            XCTAssertEqual(try SynchroMeta.getInt64(conn, key: .scopeSetVersion), 1)
        }
    }

    func testTerminalPullKeepsServerRebuildScopeWithoutUsableCursor() throws {
        let (db, processor) = try makeTestEnv()
        let scopeID = "orders:rebuild"
        try db.writeTransaction { conn in
            try SynchroMeta.upsertScope(
                conn,
                scopeID: scopeID,
                cursor: "stale-cursor",
                checksum: "stale-checksum"
            )
        }

        try processor.applyScopeChanges(
            changes: [],
            syncedTables: [testTable.localSchema],
            scopeCursors: [:],
            checksums: [scopeID: protocolEmptyScopeChecksum(scopeID: scopeID)],
            schemaHash: protocolTestSchemaHash,
            rebuildScopes: [scopeID]
        )

        let scope = try db.readTransaction { conn in
            try SynchroMeta.getScope(conn, scopeID: scopeID)
        }
        XCTAssertNil(scope?.cursor)
        XCTAssertNil(scope?.checksum)
        XCTAssertNotEqual(scope?.localChecksum, "")
    }

    func testTerminalPullRejectsStructurallyValidWrongChecksum() throws {
        let (db, processor) = try makeTestEnv()
        let scopeID = "orders:user1"
        try db.writeTransaction { conn in
            try SynchroMeta.upsertScope(
                conn,
                scopeID: scopeID,
                cursor: "old-cursor",
                checksum: "old-checksum"
            )
        }
        let wrongChecksum = ChecksumObject(
            algorithm: "sha256",
            version: 1,
            encoding: "hex",
            digest: String(repeating: "f", count: 64)
        )

        try processor.applyScopeChanges(
            changes: [],
            syncedTables: [testTable.localSchema],
            scopeCursors: [scopeID: "untrusted-terminal-cursor"],
            checksums: [scopeID: wrongChecksum],
            schemaHash: protocolTestSchemaHash
        )

        let scope = try db.readTransaction { conn in
            try SynchroMeta.getScope(conn, scopeID: scopeID)
        }
        XCTAssertNil(scope?.cursor)
        XCTAssertNil(scope?.checksum)
        XCTAssertNotEqual(scope?.localChecksum, "")
    }

    func testConnectAssignmentRollsBackGenerationAndCleanupTogether() throws {
        let (db, processor) = try makeTestEnv()
        let removedScope = "orders:removed"
        let addedScope = "orders:added"
        try addScopeRow(db, scopeID: removedScope, recordID: "w1")
        try insertOrder(db, id: "w1", updatedAt: "2026-01-01T12:00:00.000000Z")
        try db.writeTransaction { conn in
            try SynchroMeta.setInt64(conn, key: .scopeSetVersion, value: 1)
            try SynchroMeta.setInt64(conn, key: .clientGeneration, value: 1)
            try conn.execute(
                sql: """
                    INSERT INTO _synchro_seed_receipts
                        (scope_id, receipt, schema_version, schema_hash, cardinality, checksum)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                arguments: [removedScope, "receipt", 1, protocolTestSchemaHash, 1, "checksum"]
            )
            try conn.execute(sql: """
                CREATE TRIGGER fail_connected_scope
                BEFORE INSERT ON _synchro_scopes
                WHEN NEW.scope_id = 'orders:added'
                BEGIN
                    SELECT RAISE(ABORT, 'forced connected assignment failure');
                END
                """)
        }

        XCTAssertThrowsError(try processor.installConnectedAssignment(
            ScopeAssignmentDelta(
                add: [ScopeAssignment(id: addedScope, cursor: nil)],
                remove: [removedScope]
            ),
            scopeSetVersion: 2,
            clientGeneration: 2,
            syncedTables: [testTable.localSchema]
        ))

        XCTAssertNotNil(try db.queryOne("SELECT id FROM orders WHERE id = ?", params: ["w1"]))
        try db.readTransaction { conn in
            XCTAssertNotNil(try SynchroMeta.getScope(conn, scopeID: removedScope))
            XCTAssertNil(try SynchroMeta.getScope(conn, scopeID: addedScope))
            XCTAssertEqual(try SynchroMeta.getInt64(conn, key: .scopeSetVersion), 1)
            XCTAssertEqual(try SynchroMeta.getInt64(conn, key: .clientGeneration), 1)
            XCTAssertEqual(try SynchroMeta.getSeedReceipts(conn), [removedScope: "receipt"])
        }
    }

    func testSeedReceiptWithoutCursorDispositionLeavesStateUntouched() throws {
        let (db, processor) = try makeTestEnv()
        let scopeID = "orders:seed"
        try db.writeTransaction { conn in
            try SynchroMeta.upsertScope(
                conn,
                scopeID: scopeID,
                cursor: nil,
                checksum: nil
            )
            try SynchroMeta.setInt64(conn, key: .scopeSetVersion, value: 0)
            try SynchroMeta.setInt64(conn, key: .clientGeneration, value: 0)
            try conn.execute(
                sql: """
                    INSERT INTO _synchro_seed_receipts
                        (scope_id, receipt, schema_version, schema_hash, cardinality, checksum)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                arguments: [scopeID, "receipt", 1, protocolTestSchemaHash, 0, "checksum"]
            )
        }

        XCTAssertThrowsError(try processor.installConnectedAssignment(
            ScopeAssignmentDelta(add: [], remove: []),
            scopeSetVersion: 1,
            clientGeneration: 1,
            syncedTables: [testTable.localSchema]
        ))

        try db.readTransaction { conn in
            XCTAssertNotNil(try SynchroMeta.getScope(conn, scopeID: scopeID))
            XCTAssertEqual(try SynchroMeta.getSeedReceipts(conn), [scopeID: "receipt"])
            XCTAssertEqual(try SynchroMeta.getInt64(conn, key: .scopeSetVersion), 0)
            XCTAssertEqual(try SynchroMeta.getInt64(conn, key: .clientGeneration), 0)
        }
    }

    func testExplicitSeedCursorDispositionClearsReceiptAfterConnectedAssignment() throws {
        let (db, processor) = try makeTestEnv()
        let scopeID = "orders:seed"
        try db.writeTransaction { conn in
            try SynchroMeta.upsertScope(
                conn,
                scopeID: scopeID,
                cursor: nil,
                checksum: nil
            )
            try conn.execute(
                sql: """
                    INSERT INTO _synchro_seed_receipts
                        (scope_id, receipt, schema_version, schema_hash, cardinality, checksum)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                arguments: [scopeID, "receipt", 1, protocolTestSchemaHash, 0, "checksum"]
            )
        }

        try processor.installConnectedAssignment(
            ScopeAssignmentDelta(add: [], remove: []),
            scopeSetVersion: 1,
            clientGeneration: 1,
            syncedTables: [testTable.localSchema],
            scopeCursorUpdates: [scopeID: "seed-cursor"]
        )

        try db.readTransaction { conn in
            XCTAssertNotNil(try SynchroMeta.getScope(conn, scopeID: scopeID))
            XCTAssertTrue(try SynchroMeta.getSeedReceipts(conn).isEmpty)
            XCTAssertEqual(try SynchroMeta.getInt64(conn, key: .scopeSetVersion), 1)
            XCTAssertEqual(try SynchroMeta.getInt64(conn, key: .clientGeneration), 1)
        }
    }

    func testSeedReceiptRemovalPreservesProtectedLocalIntent() throws {
        let (db, processor) = try makeTestEnv()
        let scopeID = "orders:seed"
        try addScopeRow(db, scopeID: scopeID, recordID: "protected")
        try insertOrder(db, id: "protected", updatedAt: "2026-01-01T12:00:00.000000Z")
        try addPendingIntent(db, table: testTable, recordID: "protected", state: "unsealed")
        try db.writeTransaction { conn in
            try conn.execute(
                sql: """
                    INSERT INTO _synchro_seed_receipts
                        (scope_id, receipt, schema_version, schema_hash, cardinality, checksum)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                arguments: [scopeID, "receipt", 1, protocolTestSchemaHash, 1, "checksum"]
            )
        }

        try processor.installConnectedAssignment(
            ScopeAssignmentDelta(add: [], remove: [scopeID]),
            scopeSetVersion: 1,
            clientGeneration: 1,
            syncedTables: [testTable.localSchema]
        )

        XCTAssertNotNil(try db.queryOne("SELECT id FROM orders WHERE id = ?", params: ["protected"]))
        XCTAssertEqual(try pendingChangeCount(db), 1)
        try db.readTransaction { conn in
            XCTAssertNil(try SynchroMeta.getScope(conn, scopeID: scopeID))
            XCTAssertTrue(try SynchroMeta.getSeedReceipts(conn).isEmpty)
        }
    }

    func testProtectedTombstonePreservesProjectionAndRemovesProvenance() throws {
        let (db, processor) = try makeTestEnv()
        let scopeID = "orders:user1"
        try addScopeRow(db, scopeID: scopeID, recordID: "protected")
        try addScopeRow(db, scopeID: "orders:cleanup", recordID: "unprotected")
        try insertOrder(db, id: "protected", shipAddress: "local", updatedAt: "2026-01-01T00:00:00.000000Z")
        try insertOrder(db, id: "unprotected", shipAddress: "local", updatedAt: "2026-01-01T00:00:00.000000Z")
        try addPendingIntent(db, table: testTable, recordID: "protected", state: "blocked_by_predecessor")

        let deletedAt = "2026-01-02T00:00:00.000000Z"
        let tombstone = [
            "id": AnyCodable("protected"),
            "ship_address": AnyCodable("server"),
            "updated_at": AnyCodable("2026-01-01T00:00:00.000000Z"),
            "deleted_at": AnyCodable(deletedAt),
        ]
        let change = try makeChangeRecord(
            scope: scopeID,
            schema: testTable,
            op: .delete,
            pk: ["id": AnyCodable("protected")],
            row: tombstone,
            serverVersion: deletedAt
        )
        try processor.applyScopeChanges(
            changes: [change],
            syncedTables: [testTable.localSchema],
            scopeCursors: [scopeID: "11"],
            checksums: nil,
            schemaHash: protocolTestSchemaHash
        )

        let protected = try db.queryOne("SELECT ship_address, deleted_at FROM orders WHERE id = 'protected'", params: nil)
        XCTAssertEqual(protected?["ship_address"] as String?, "local")
        XCTAssertNil(protected?["deleted_at"] as String?)
        XCTAssertNil(try db.queryOne("SELECT record_id FROM _synchro_scope_rows WHERE scope_id = ?", params: [scopeID]))

        try processor.removeScope(scopeID: "orders:cleanup", syncedTables: [testTable.localSchema])
        XCTAssertNil(try db.queryOne("SELECT id FROM orders WHERE id = 'unprotected'", params: nil))
    }

    func testAssignmentRemovalPreservesProtectedProjectionAndCleansUnprotectedRows() throws {
        let (db, processor) = try makeTestEnv()
        let scopeID = "orders:user1"
        try addScopeRow(db, scopeID: scopeID, recordID: "protected")
        try addScopeRow(db, scopeID: scopeID, recordID: "unprotected")
        try insertOrder(db, id: "protected", shipAddress: "local", updatedAt: "2026-01-01T00:00:00.000000Z")
        try insertOrder(db, id: "unprotected", shipAddress: "local", updatedAt: "2026-01-01T00:00:00.000000Z")
        try addPendingIntent(db, table: testTable, recordID: "protected", state: "legacy_blocked")

        try processor.removeScope(scopeID: scopeID, syncedTables: [testTable.localSchema])

        XCTAssertNotNil(try db.queryOne("SELECT id FROM orders WHERE id = 'protected'", params: nil))
        XCTAssertNil(try db.queryOne("SELECT id FROM orders WHERE id = 'unprotected'", params: nil))
        XCTAssertEqual(try db.query("SELECT * FROM _synchro_scope_rows", params: nil).count, 0)
    }

    func testPullApplyClearsMatchingDurableBackoffWithCommittedState() throws {
        let (db, processor) = try makeTestEnv()
        let scopeID = "orders:user1"
        let request = PullRequest(
            clientID: "test-client",
            clientGeneration: 1,
            schema: SchemaRef(version: 1, hash: protocolTestSchemaHash),
            scopeSetVersion: 1,
            scopes: [scopeID: ScopeCursorRef(cursor: "scope-before")],
            limit: 100
        )
        let requestJSON = try XCTUnwrap(
            String(data: JSONEncoder.synchroEncoder().encode(request), encoding: .utf8)
        )
        try db.writeTransaction { connection in
            try SynchroMeta.upsertScope(
                connection,
                scopeID: scopeID,
                cursor: "scope-before",
                checksum: nil
            )
            try SynchroMeta.upsertBackoffRecord(
                connection,
                record: LocalBackoffRecord(
                    resumeState: .pulling,
                    workIdentity: requestJSON,
                    retryClassification: .network,
                    attemptCount: 1,
                    nextRetryAtMS: 1
                )
            )
        }

        try processor.applyScopeChanges(
            changes: [],
            syncedTables: [testTable.localSchema],
            scopeCursors: [scopeID: "scope-after"],
            checksums: nil,
            schemaHash: protocolTestSchemaHash,
            completedPullRequestJSON: requestJSON
        )

        XCTAssertEqual(
            try db.readTransaction { try SynchroMeta.getScope($0, scopeID: scopeID)?.cursor },
            "scope-after"
        )
        XCTAssertNil(try db.readTransaction { try SynchroMeta.getBackoffRecord($0) })

        let path = db.path
        try db.close()
        let recovered = try SynchroDatabase(path: path)
        defer { try? recovered.close() }
        XCTAssertEqual(
            try recovered.readTransaction { try SynchroMeta.getScope($0, scopeID: scopeID)?.cursor },
            "scope-after"
        )
        XCTAssertNil(try recovered.readTransaction { try SynchroMeta.getBackoffRecord($0) })
    }

    func testRebuildPageApplyClearsMatchingDurableBackoffWithCommittedState() throws {
        let (db, processor) = try makeTestEnv()
        let scopeID = "orders:user1"
        try db.writeTransaction { connection in
            try SynchroMeta.upsertScope(connection, scopeID: scopeID, cursor: nil, checksum: nil)
        }
        let attempt = try processor.beginScopeRebuild(
            scopeID: scopeID,
            clientGeneration: 1,
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            pageLimit: 100,
            syncedTables: [testTable.localSchema]
        )
        let request = RebuildRequest(
            clientID: "test-client",
            clientGeneration: attempt.clientGeneration,
            schema: SchemaRef(version: attempt.schemaVersion, hash: attempt.schemaHash),
            scope: scopeID,
            rebuildID: attempt.rebuildID,
            cursor: attempt.cursor,
            limit: attempt.pageLimit
        )
        let requestBody = try rebuildRequestBody(request)
        let requestJSON = try XCTUnwrap(String(data: requestBody, encoding: .utf8))
        let response = RebuildResponse(
            scope: scopeID,
            records: [],
            cursor: "page-two",
            hasMore: true,
            finalScopeCursor: nil,
            checksum: nil
        )
        try db.writeTransaction { connection in
            try SynchroMeta.upsertBackoffRecord(
                connection,
                record: LocalBackoffRecord(
                    resumeState: .rebuilding,
                    workIdentity: requestJSON,
                    retryClassification: .network,
                    attemptCount: 1,
                    nextRetryAtMS: 1
                )
            )
        }

        let continuedAttempt = try processor.applyScopeRebuildPage(
            attempt: attempt,
            request: request,
            requestBody: requestBody,
            response: response,
            responseBody: try rebuildResponseBody(response),
            syncedTables: [testTable.localSchema]
        )

        XCTAssertEqual(continuedAttempt.cursor, "page-two")
        XCTAssertNil(try db.readTransaction { try SynchroMeta.getBackoffRecord($0) })

        let path = db.path
        try db.close()
        let recovered = try SynchroDatabase(path: path)
        defer { try? recovered.close() }
        XCTAssertEqual(
            try recovered.readTransaction { try SynchroMeta.getRebuildAttempt($0, scopeID: scopeID)?.cursor },
            "page-two"
        )
        XCTAssertNil(try recovered.readTransaction { try SynchroMeta.getBackoffRecord($0) })
    }

    func testScopeRemovalDeletesActiveRebuildArtifactsAndMatchingBackoff() throws {
        let (db, processor) = try makeTestEnv()
        let scopeID = "orders:user1"
        try db.writeTransaction { connection in
            try SynchroMeta.upsertScope(connection, scopeID: scopeID, cursor: nil, checksum: nil)
        }
        let attempt = try processor.beginScopeRebuild(
            scopeID: scopeID,
            clientGeneration: 1,
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            pageLimit: 100,
            syncedTables: [testTable.localSchema]
        )
        let request = RebuildRequest(
            clientID: "test-client",
            clientGeneration: attempt.clientGeneration,
            schema: SchemaRef(version: attempt.schemaVersion, hash: attempt.schemaHash),
            scope: scopeID,
            rebuildID: attempt.rebuildID,
            cursor: attempt.cursor,
            limit: attempt.pageLimit
        )
        let requestJSON = try XCTUnwrap(
            String(data: try rebuildRequestBody(request), encoding: .utf8)
        )
        let response = RebuildResponse(
            scope: scopeID,
            records: [],
            cursor: "page-two",
            hasMore: true,
            finalScopeCursor: nil,
            checksum: nil
        )
        let responseJSON = try XCTUnwrap(
            String(data: try rebuildResponseBody(response), encoding: .utf8)
        )
        try db.writeTransaction { connection in
            try SynchroMeta.insertRebuildPageReceipt(
                connection,
                scopeID: scopeID,
                rebuildID: attempt.rebuildID,
                requestCursor: request.cursor,
                requestJSON: requestJSON,
                responseJSON: responseJSON,
                finalScopeCursor: nil,
                finalChecksumJSON: nil
            )
            try SynchroMeta.upsertBackoffRecord(
                connection,
                record: LocalBackoffRecord(
                    resumeState: .rebuilding,
                    workIdentity: requestJSON,
                    retryClassification: .network,
                    attemptCount: 1,
                    nextRetryAtMS: 1
                )
            )
        }

        try processor.removeScope(scopeID: scopeID, syncedTables: [testTable.localSchema])

        XCTAssertNil(try db.readTransaction { try SynchroMeta.getScope($0, scopeID: scopeID) })
        XCTAssertNil(try db.readTransaction { try SynchroMeta.getRebuildAttempt($0, scopeID: scopeID) })
        XCTAssertEqual(
            try db.query(
                "SELECT * FROM _synchro_rebuild_page_receipts WHERE scope_id = ?",
                params: [scopeID]
            ).count,
            0
        )
        XCTAssertNil(try db.readTransaction { try SynchroMeta.getBackoffRecord($0) })
    }

    func testRebuildStartResetsOnlyTargetScopeProvenance() throws {
        let (db, processor) = try makeTestEnv()
        let scopeID = "orders:target"
        let otherScopeID = "orders:other"
        try addScopeRow(db, scopeID: scopeID, recordID: "target-only")
        try addScopeRow(db, scopeID: scopeID, recordID: "shared")
        try addScopeRow(db, scopeID: scopeID, recordID: "protected")
        try addScopeRow(db, scopeID: otherScopeID, recordID: "shared")
        for recordID in ["target-only", "shared", "protected", "local-only"] {
            try insertOrder(
                db,
                id: recordID,
                updatedAt: "2026-01-01T00:00:00.000000Z"
            )
        }
        try addPendingIntent(db, table: testTable, recordID: "protected", state: "sealed")

        let attempt = try processor.beginScopeRebuild(
            scopeID: scopeID,
            clientGeneration: 1,
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            pageLimit: 100,
            syncedTables: [testTable.localSchema]
        )

        XCTAssertNil(try db.queryOne("SELECT id FROM orders WHERE id = ?", params: ["target-only"]))
        XCTAssertNotNil(try db.queryOne("SELECT id FROM orders WHERE id = ?", params: ["shared"]))
        XCTAssertNotNil(try db.queryOne("SELECT id FROM orders WHERE id = ?", params: ["protected"]))
        XCTAssertNotNil(try db.queryOne("SELECT id FROM orders WHERE id = ?", params: ["local-only"]))
        XCTAssertEqual(
            try db.query(
                "SELECT record_id FROM _synchro_scope_rows WHERE scope_id = ?",
                params: [scopeID]
            ).count,
            0
        )
        XCTAssertEqual(
            try db.query(
                "SELECT record_id FROM _synchro_scope_rows WHERE scope_id = ?",
                params: [otherScopeID]
            ).map { $0["record_id"] as String? },
            ["shared"]
        )
        XCTAssertEqual(try pendingChangeCount(db), 1)
        XCTAssertEqual(
            try db.readTransaction { try SynchroMeta.getRebuildAttempt($0, scopeID: scopeID) },
            attempt
        )
    }

    func testRebuildStartRollsBackProvenanceResetWhenAttemptCannotPersist() throws {
        let (db, processor) = try makeTestEnv()
        let scopeID = "orders:target"
        try addScopeRow(db, scopeID: scopeID, recordID: "target-only")
        try insertOrder(
            db,
            id: "target-only",
            updatedAt: "2026-01-01T00:00:00.000000Z"
        )
        try db.writeTransaction { connection in
            try connection.execute(sql: """
                CREATE TRIGGER fail_rebuild_attempt_insert
                BEFORE INSERT ON _synchro_rebuild_attempts
                BEGIN
                    SELECT RAISE(ABORT, 'forced rebuild attempt failure');
                END
                """)
        }

        XCTAssertThrowsError(try processor.beginScopeRebuild(
            scopeID: scopeID,
            clientGeneration: 1,
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            pageLimit: 100,
            syncedTables: [testTable.localSchema]
        ))

        XCTAssertNotNil(try db.queryOne("SELECT id FROM orders WHERE id = ?", params: ["target-only"]))
        XCTAssertEqual(
            try db.query(
                "SELECT record_id FROM _synchro_scope_rows WHERE scope_id = ?",
                params: [scopeID]
            ).map { $0["record_id"] as String? },
            ["target-only"]
        )
        XCTAssertNil(try db.readTransaction { try SynchroMeta.getRebuildAttempt($0, scopeID: scopeID) })
    }

    func testRebuildPageAndPruningPreserveProtectedProjectionAndCleanStaleRows() throws {
        let (db, processor) = try makeTestEnv()
        let scopeID = "orders:user1"
        try addScopeRow(db, scopeID: scopeID, recordID: "protected", generation: 0)
        try addScopeRow(db, scopeID: scopeID, recordID: "stale", generation: 0)
        try insertOrder(db, id: "protected", shipAddress: "local", updatedAt: "2026-01-01T00:00:00.000000Z")
        try insertOrder(db, id: "stale", shipAddress: "local", updatedAt: "2026-01-01T00:00:00.000000Z")
        try addPendingIntent(db, table: testTable, recordID: "protected", state: "sealed")

        let attempt = try processor.beginScopeRebuild(
            scopeID: scopeID,
            clientGeneration: 1,
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            pageLimit: 100,
            syncedTables: [testTable.localSchema]
        )
        let serverVersion = "2026-01-02T00:00:00.000000Z"
        let serverRow: [String: AnyCodable] = [
            "id": AnyCodable("protected"),
            "ship_address": AnyCodable("server"),
            "updated_at": AnyCodable(serverVersion),
            "deleted_at": AnyCodable(NSNull()),
        ]
        let rowDigest = try Integrity.rowDigest(
            schemaHash: protocolTestSchemaHash,
            table: testTable,
            pk: ["id": AnyCodable("protected")],
            row: serverRow,
            serverVersion: serverVersion
        )
        let record = RebuildRecord(
            table: testTable.tableID,
            pk: ["id": AnyCodable("protected")],
            row: serverRow,
            rowChecksum: rowDigest.checksum,
            serverVersion: serverVersion
        )
        let firstRequest = RebuildRequest(
            clientID: "test-client",
            clientGeneration: attempt.clientGeneration,
            schema: SchemaRef(version: attempt.schemaVersion, hash: attempt.schemaHash),
            scope: scopeID,
            rebuildID: attempt.rebuildID,
            cursor: attempt.cursor,
            limit: attempt.pageLimit
        )
        let firstResponse = RebuildResponse(
            scope: scopeID,
            records: [record],
            cursor: "page-2",
            hasMore: true,
            finalScopeCursor: nil,
            checksum: nil
        )
        let continuedAttempt = try processor.applyScopeRebuildPage(
            attempt: attempt,
            request: firstRequest,
            requestBody: try rebuildRequestBody(firstRequest),
            response: firstResponse,
            responseBody: try rebuildResponseBody(firstResponse),
            syncedTables: [testTable.localSchema]
        )
        let checksum = try Integrity.scopeDigest(
            schemaHash: protocolTestSchemaHash,
            scopeID: scopeID,
            entries: [(identity: rowDigest.identity, digest: rowDigest.checksum)]
        )
        let finalRequest = RebuildRequest(
            clientID: "test-client",
            clientGeneration: continuedAttempt.clientGeneration,
            schema: SchemaRef(version: continuedAttempt.schemaVersion, hash: continuedAttempt.schemaHash),
            scope: scopeID,
            rebuildID: continuedAttempt.rebuildID,
            cursor: continuedAttempt.cursor,
            limit: continuedAttempt.pageLimit
        )
        let finalResponse = RebuildResponse(
            scope: scopeID,
            records: [],
            cursor: nil,
            hasMore: false,
            finalScopeCursor: "scope_cursor_20",
            checksum: checksum
        )
        let finalAttempt = try processor.applyScopeRebuildPage(
            attempt: continuedAttempt,
            request: finalRequest,
            requestBody: try rebuildRequestBody(finalRequest),
            response: finalResponse,
            responseBody: try rebuildResponseBody(finalResponse),
            syncedTables: [testTable.localSchema]
        )
        try processor.finalizeScopeRebuild(
            attempt: finalAttempt,
            finalCursor: "scope_cursor_20",
            checksum: checksum,
            syncedTables: [testTable.localSchema]
        )

        let protected = try db.queryOne("SELECT ship_address FROM orders WHERE id = 'protected'", params: nil)
        XCTAssertEqual(protected?["ship_address"] as String?, "local")
        XCTAssertNil(try db.queryOne("SELECT id FROM orders WHERE id = 'stale'", params: nil))
        XCTAssertEqual(
            try db.query("SELECT record_id FROM _synchro_scope_rows WHERE scope_id = ?", params: [scopeID]).map { $0["record_id"] as String? },
            ["protected"]
        )
    }

    func testRebuildPageBatchesMixedProtectionAcrossChunkBoundary() throws {
        let (db, processor) = try makeTestEnv()
        let scopeID = "orders:user1"
        try db.writeTransaction { connection in
            try SynchroMeta.upsertScope(connection, scopeID: scopeID, cursor: nil, checksum: nil)
        }
        let attempt = try processor.beginScopeRebuild(
            scopeID: scopeID,
            clientGeneration: 1,
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            pageLimit: 402,
            syncedTables: [testTable.localSchema]
        )
        let serverVersion = "2026-01-02T00:00:00.000000Z"
        let records = try (0..<402).map { index in
            let recordID = String(format: "row-%03d", index)
            let row: [String: AnyCodable] = [
                "id": AnyCodable(recordID),
                "ship_address": AnyCodable("server-\(index)"),
                "updated_at": AnyCodable(serverVersion),
                "deleted_at": AnyCodable(NSNull()),
            ]
            let rowChecksum = try Integrity.rowDigest(
                schemaHash: protocolTestSchemaHash,
                table: testTable.localSchema,
                pk: ["id": AnyCodable(recordID)],
                row: row,
                serverVersion: serverVersion
            ).checksum
            return RebuildRecord(
                table: testTable.tableID,
                pk: ["id": AnyCodable(recordID)],
                row: row,
                rowChecksum: rowChecksum,
                serverVersion: serverVersion
            )
        }
        for index in 398...401 {
            try insertOrder(
                db,
                id: String(format: "row-%03d", index),
                shipAddress: "local-\(index)",
                updatedAt: "2026-01-01T00:00:00.000000Z"
            )
        }
        try addPendingIntent(db, table: testTable, recordID: "row-399", state: "sealed")
        try addPendingIntent(db, table: testTable, recordID: "row-401", state: "accepted")
        try db.writeTransaction { connection in
            try SynchroMeta.upsertRejectedMutation(
                connection,
                mutationID: UUID().uuidString.lowercased(),
                tableName: testTable.tableName,
                recordID: "row-400",
                status: "rejected_terminal",
                code: "policy_rejected",
                message: "not allowed",
                serverRow: nil,
                serverVersion: nil
            )
            try SynchroMeta.upsertRejectedMutation(
                connection,
                mutationID: UUID().uuidString.lowercased(),
                tableName: testTable.tableName,
                recordID: "row-398",
                status: "rejected_terminal",
                code: "policy_rejected",
                message: "canonical row supplied",
                serverRow: nil,
                serverVersion: "server-version"
            )
        }
        let request = RebuildRequest(
            clientID: "test-client",
            clientGeneration: attempt.clientGeneration,
            schema: SchemaRef(version: attempt.schemaVersion, hash: attempt.schemaHash),
            scope: scopeID,
            rebuildID: attempt.rebuildID,
            cursor: attempt.cursor,
            limit: attempt.pageLimit
        )
        let response = RebuildResponse(
            scope: scopeID,
            records: records,
            cursor: "page-2",
            hasMore: true,
            finalScopeCursor: nil,
            checksum: nil
        )

        let continuedAttempt = try processor.applyScopeRebuildPage(
            attempt: attempt,
            request: request,
            requestBody: try rebuildRequestBody(request),
            response: response,
            responseBody: try rebuildResponseBody(response),
            syncedTables: [testTable.localSchema]
        )

        XCTAssertEqual(continuedAttempt.cursor, "page-2")
        XCTAssertEqual(
            try db.queryOne("SELECT ship_address FROM orders WHERE id = 'row-398'", params: nil)?["ship_address"] as String?,
            "server-398"
        )
        XCTAssertEqual(
            try db.queryOne("SELECT ship_address FROM orders WHERE id = 'row-399'", params: nil)?["ship_address"] as String?,
            "local-399"
        )
        XCTAssertEqual(
            try db.queryOne("SELECT ship_address FROM orders WHERE id = 'row-400'", params: nil)?["ship_address"] as String?,
            "local-400"
        )
        XCTAssertEqual(
            try db.queryOne("SELECT ship_address FROM orders WHERE id = 'row-401'", params: nil)?["ship_address"] as String?,
            "server-401"
        )
        XCTAssertEqual(
            try db.query("SELECT record_id FROM _synchro_scope_rows WHERE scope_id = ?", params: [scopeID]).count,
            records.count
        )
        for recordID in ["row-399", "row-400"] {
            XCTAssertEqual(
                try db.queryOne(
                    "SELECT server_version FROM _synchro_row_versions WHERE table_name = ? AND record_id = ?",
                    params: [testTable.tableName, recordID]
                )?["server_version"] as String?,
                serverVersion
            )
        }
    }

    func testIntermediateRebuildReceiptSurvivesRestartAndSkipsExactReplay() throws {
        let (db, processor) = try makeTestEnv()
        let path = db.path
        let scopeID = "orders:user1"
        try db.writeTransaction { connection in
            try SynchroMeta.upsertScope(connection, scopeID: scopeID, cursor: nil, checksum: nil)
            try connection.execute(sql: "CREATE TABLE rebuild_apply_events (count INTEGER NOT NULL)")
            try connection.execute(sql: "INSERT INTO rebuild_apply_events (count) VALUES (0)")
            try connection.execute(sql: """
                CREATE TRIGGER count_rebuild_apply
                AFTER INSERT ON orders
                BEGIN
                    UPDATE rebuild_apply_events SET count = count + 1;
                END
                """)
        }
        let attempt = try processor.beginScopeRebuild(
            scopeID: scopeID,
            clientGeneration: 1,
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            pageLimit: 100,
            syncedTables: [testTable.localSchema]
        )
        let serverVersion = "server-version-1"
        let row: [String: AnyCodable] = [
            "id": AnyCodable("r1"),
            "ship_address": AnyCodable("server"),
            "updated_at": AnyCodable("2026-01-01T12:00:00.000000Z"),
            "deleted_at": AnyCodable(NSNull()),
        ]
        let rowChecksum = try Integrity.rowDigest(
            schemaHash: protocolTestSchemaHash,
            table: testTable.localSchema,
            pk: ["id": AnyCodable("r1")],
            row: row,
            serverVersion: serverVersion
        ).checksum
        let request = RebuildRequest(
            clientID: "test-client",
            clientGeneration: attempt.clientGeneration,
            schema: SchemaRef(version: attempt.schemaVersion, hash: attempt.schemaHash),
            scope: scopeID,
            rebuildID: attempt.rebuildID,
            cursor: attempt.cursor,
            limit: attempt.pageLimit
        )
        let response = RebuildResponse(
            scope: scopeID,
            records: [RebuildRecord(
                table: testTable.localSchema.tableID,
                pk: ["id": AnyCodable("r1")],
                row: row,
                rowChecksum: rowChecksum,
                serverVersion: serverVersion
            )],
            cursor: "opaque-next-token",
            hasMore: true,
            finalScopeCursor: nil,
            checksum: nil
        )
        let continuedAttempt = try processor.applyScopeRebuildPage(
            attempt: attempt,
            request: request,
            requestBody: try rebuildRequestBody(request),
            response: response,
            responseBody: try rebuildResponseBody(response),
            syncedTables: [testTable.localSchema]
        )
        XCTAssertEqual(continuedAttempt.cursor, "opaque-next-token")
        XCTAssertEqual(try db.queryOne("SELECT count FROM rebuild_apply_events", params: nil)?["count"] as Int64?, 1)

        try db.close()
        let recoveredDatabase = try SynchroDatabase(path: path)
        defer { try? recoveredDatabase.close() }
        let recoveredProcessor = PullProcessor(database: recoveredDatabase)
        let recoveredAttempt = try recoveredProcessor.beginScopeRebuild(
            scopeID: scopeID,
            clientGeneration: 1,
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            pageLimit: 100,
            syncedTables: [testTable.localSchema]
        )
        XCTAssertEqual(recoveredAttempt, continuedAttempt)

        let replayedAttempt = try recoveredProcessor.applyScopeRebuildPage(
            attempt: attempt,
            request: request,
            requestBody: try rebuildRequestBody(request),
            response: response,
            responseBody: try rebuildResponseBody(response),
            syncedTables: [testTable.localSchema]
        )
        XCTAssertEqual(replayedAttempt, continuedAttempt)
        XCTAssertEqual(
            try recoveredDatabase.queryOne("SELECT count FROM rebuild_apply_events", params: nil)?["count"] as Int64?,
            1
        )
        XCTAssertEqual(
            try recoveredDatabase.query("SELECT * FROM _synchro_rebuild_page_receipts", params: nil).count,
            1
        )

        let canonicalResponseBody = try rebuildResponseBody(response)
        let byteDifferentResponseBody = Data(
            (" " + String(decoding: canonicalResponseBody, as: UTF8.self)).utf8
        )
        try Integrity.validateCanonicalWireJSON(byteDifferentResponseBody)
        XCTAssertThrowsError(try recoveredProcessor.applyScopeRebuildPage(
            attempt: attempt,
            request: request,
            requestBody: try rebuildRequestBody(request),
            response: response,
            responseBody: byteDifferentResponseBody,
            syncedTables: [testTable.localSchema]
        ))

        let restartedAttempt = try recoveredProcessor.restartScopeRebuild(
            scopeID: scopeID,
            clientGeneration: 1,
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            pageLimit: 100,
            syncedTables: [testTable.localSchema]
        )
        XCTAssertNotEqual(restartedAttempt.rebuildID, attempt.rebuildID)
        XCTAssertNil(restartedAttempt.cursor)
        XCTAssertEqual(
            try recoveredDatabase.query(
                "SELECT * FROM _synchro_rebuild_page_receipts WHERE rebuild_id = ?",
                params: [attempt.rebuildID]
            ).count,
            0
        )
    }

    func testTerminalChecksumUsesValidatedServerDigestForTerminalRejectedProjection() throws {
        let (db, processor) = try makeTestEnv()
        let scopeID = "orders:user1"
        let serverVersion = "2026-01-02T00:00:00.000000Z"
        let serverRow: [String: AnyCodable] = [
            "id": AnyCodable("w1"),
            "ship_address": AnyCodable("server"),
            "updated_at": AnyCodable(serverVersion),
            "deleted_at": AnyCodable(NSNull()),
        ]
        let rowDigest = try Integrity.rowDigest(
            schemaHash: protocolTestSchemaHash,
            table: testTable,
            pk: ["id": AnyCodable("w1")],
            row: serverRow,
            serverVersion: serverVersion
        )
        let scopeDigest = try scopeChecksum(
            scopeID: scopeID,
            schema: testTable,
            row: serverRow,
            serverVersion: serverVersion
        )
        try addScopeRow(db, scopeID: scopeID, recordID: "w1", checksum: rowDigest.checksum.digest)
        try insertOrder(db, id: "w1", shipAddress: "local", updatedAt: "2026-01-01T00:00:00.000000Z")
        try db.writeTransaction { conn in
            try SynchroMeta.upsertRowVersion(
                conn,
                tableName: testTable.tableName,
                recordID: "w1",
                serverVersion: serverVersion,
                rowChecksum: rowDigest.checksum
            )
            try SynchroMeta.upsertRejectedMutation(
                conn,
                mutationID: UUID().uuidString.lowercased(),
                tableName: testTable.tableName,
                recordID: "w1",
                status: "rejected_terminal",
                code: "policy_rejected",
                message: "not allowed",
                serverRow: nil,
                serverVersion: nil
            )
        }

        try processor.applyScopeChanges(
            changes: [],
            syncedTables: [testTable.localSchema],
            scopeCursors: [scopeID: "11"],
            checksums: [scopeID: scopeDigest],
            schemaHash: protocolTestSchemaHash
        )

        XCTAssertEqual(try db.queryOne("SELECT ship_address FROM orders WHERE id = 'w1'", params: nil)?["ship_address"] as String?, "local")
        XCTAssertEqual(
            try db.queryOne("SELECT checksum FROM _synchro_scopes WHERE scope_id = ?", params: [scopeID])?["checksum"] as String?,
            try String(data: JSONEncoder.synchroEncoder().encode(scopeDigest), encoding: .utf8)
        )
    }

    func testPullRoundTripsBytesAsBlob() throws {
        let (db, processor) = try makeTestEnv(schema: bytesTable)
        let scopeID = "binary:user1"
        try addScopeRow(db, scopeID: scopeID, recordID: "b1", tableName: bytesTable.tableName)
        let serverVersion = "2026-01-02T00:00:00.000000Z"
        let encoded = "AAH_"
        let row: [String: AnyCodable] = [
            "id": AnyCodable("b1"),
            "payload": AnyCodable(encoded),
            "updated_at": AnyCodable(serverVersion),
            "deleted_at": AnyCodable(NSNull()),
        ]
        let change = try makeChangeRecord(
            scope: scopeID,
            schema: bytesTable,
            op: .upsert,
            pk: ["id": AnyCodable("b1")],
            row: row,
            serverVersion: serverVersion
        )
        try processor.applyScopeChanges(
            changes: [change],
            syncedTables: [bytesTable.localSchema],
            scopeCursors: [scopeID: "11"],
            checksums: nil,
            schemaHash: protocolTestSchemaHash
        )

        let stored = try db.queryOne("SELECT payload FROM binary_rows WHERE id = 'b1'", params: nil)?["payload"] as Data?
        XCTAssertEqual(stored, Data([0x00, 0x01, 0xff]))
    }

    func testApplyScopeDeletePreservesCanonicalDeletedAt() throws {
        let (db, processor) = try makeTestEnv()

        try db.writeTransaction { conn in
            try SynchroMeta.setSyncLock(conn, locked: true)
            try SynchroMeta.upsertScope(conn, scopeID: "orders:user1", cursor: "10", checksum: nil)
            try SynchroMeta.upsertScopeRow(
                conn,
                scopeID: "orders:user1",
                tableName: "orders",
                recordID: "w1",
                checksum: "{\"algorithm\":\"sha256\",\"digest\":\"0000000000000000000000000000000000000000000000000000000000000000\",\"encoding\":\"hex\",\"version\":1}",
                generation: 0
            )
        }
        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, updated_at) VALUES (?, ?, ?)",
            params: ["w1", "123 Main St", "2026-01-01T10:00:00.000Z"]
        )
        try db.writeTransaction { conn in
            try SynchroMeta.setSyncLock(conn, locked: false)
        }

        let change = try makeChangeRecord(
            scope: "orders:user1",
            schema: testTable,
            op: .delete,
            pk: ["id": AnyCodable("w1")],
            row: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("123 Main St"),
                "updated_at": AnyCodable("2026-01-04T00:00:00.000000Z"),
                "deleted_at": AnyCodable("2026-01-04T00:00:00.000000Z"),
            ],
            serverVersion: "2026-01-04T00:00:00.000Z"
        )

        try processor.applyScopeChanges(
            changes: [change],
            syncedTables: [testTable.localSchema],
            scopeCursors: ["orders:user1": "11"],
            checksums: nil,
            schemaHash: protocolTestSchemaHash
        )

        let row = try db.queryOne("SELECT deleted_at FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["deleted_at"] as String?, "2026-01-04T00:00:00.000000Z")
    }

    func testApplyScopeDeleteUsesDeletedAtAsEffectiveVersion() throws {
        let (db, processor) = try makeTestEnv()

        try addScopeRow(db, scopeID: "orders:user1", recordID: "w1")
        try insertOrder(db, id: "w1", updatedAt: "2026-01-03T00:00:00.000Z")

        let change = try makeChangeRecord(
            scope: "orders:user1",
            schema: testTable,
            op: .delete,
            pk: ["id": AnyCodable("w1")],
            row: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("123 Main St"),
                "updated_at": AnyCodable("2026-01-03T00:00:00.000000Z"),
                "deleted_at": AnyCodable("2026-01-04T00:00:00.000000Z"),
            ],
            serverVersion: "2026-01-04T00:00:00.000Z"
        )

        try processor.applyScopeChanges(
            changes: [change],
            syncedTables: [testTable.localSchema],
            scopeCursors: ["orders:user1": "11"],
            checksums: nil,
            schemaHash: protocolTestSchemaHash
        )

        let row = try db.queryOne(
            "SELECT updated_at, deleted_at FROM orders WHERE id = ?",
            params: ["w1"]
        )
        XCTAssertEqual(row?["updated_at"] as String?, "2026-01-03T00:00:00.000000Z")
        XCTAssertEqual(row?["deleted_at"] as String?, "2026-01-04T00:00:00.000000Z")
    }

    func testApplyScopeDeleteWithoutRowRemovesOrphanedRecordAndLeavesQueueEmpty() throws {
        let (db, processor) = try makeTestEnv()

        try addScopeRow(db, scopeID: "orders:user1", recordID: "w1")
        try insertOrder(db, id: "w1", updatedAt: "2026-01-03T00:00:00.000Z")

        let change = try makeChangeRecord(
            scope: "orders:user1",
            schema: testTable,
            op: .delete,
            pk: ["id": AnyCodable("w1")],
            row: nil,
            serverVersion: "2026-01-04T00:00:00.000Z"
        )

        try processor.applyScopeChanges(
            changes: [change],
            syncedTables: [testTable.localSchema],
            scopeCursors: ["orders:user1": "11"],
            checksums: nil,
            schemaHash: protocolTestSchemaHash
        )

        let row = try db.queryOne("SELECT id FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertNil(row)
        XCTAssertEqual(try pendingChangeCount(db), 0)
    }

    func testApplyScopeDeleteRejectsRowWithoutDeletedAt() throws {
        let (db, processor) = try makeTestEnv()

        try addScopeRow(db, scopeID: "orders:user1", recordID: "w1")
        try insertOrder(db, id: "w1", updatedAt: "2026-01-03T00:00:00.000Z")

        let change = try makeChangeRecord(
            scope: "orders:user1",
            schema: testTable,
            op: .delete,
            pk: ["id": AnyCodable("w1")],
            row: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("123 Main St"),
                "updated_at": AnyCodable("2026-01-04T00:00:00.000000Z"),
                "deleted_at": AnyCodable(NSNull())
            ],
            serverVersion: "2026-01-04T00:00:00.000Z"
        )

        XCTAssertThrowsError(
            try processor.applyScopeChanges(
                changes: [change],
                syncedTables: [testTable.localSchema],
                scopeCursors: ["orders:user1": "11"],
                checksums: nil,
                schemaHash: protocolTestSchemaHash
            )
        )
    }

    func testFinalizeScopeRebuildRemovesOrphanedRecordAndLeavesQueueEmpty() throws {
        let (db, processor) = try makeTestEnv()

        try addScopeRow(db, scopeID: "orders:user1", recordID: "w1", generation: 1)
        try insertOrder(db, id: "w1", updatedAt: "2026-01-03T00:00:00.000Z")

        let attempt = LocalRebuildAttempt(
            scopeID: "orders:user1",
            rebuildID: UUID().uuidString.lowercased(),
            clientGeneration: 1,
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            generation: 2,
            cursor: nil,
            pageLimit: 100
        )
        try processor.finalizeScopeRebuild(
            attempt: attempt,
            finalCursor: "scope_cursor_20",
            checksum: protocolEmptyScopeChecksum(scopeID: attempt.scopeID),
            syncedTables: [testTable.localSchema]
        )

        let row = try db.queryOne("SELECT id FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertNil(row)
        XCTAssertEqual(try pendingChangeCount(db), 0)
    }

    func testFinalizeScopeRebuildKeepsRecordBackedByAnotherScope() throws {
        let (db, processor) = try makeTestEnv()

        try addScopeRow(db, scopeID: "orders:user1", recordID: "w1", generation: 1)
        try addScopeRow(db, scopeID: "orders:shared", recordID: "w1", generation: 4)
        try insertOrder(db, id: "w1", updatedAt: "2026-01-03T00:00:00.000Z")

        let attempt = LocalRebuildAttempt(
            scopeID: "orders:user1",
            rebuildID: UUID().uuidString.lowercased(),
            clientGeneration: 1,
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            generation: 2,
            cursor: nil,
            pageLimit: 100
        )
        try processor.finalizeScopeRebuild(
            attempt: attempt,
            finalCursor: "scope_cursor_20",
            checksum: protocolEmptyScopeChecksum(scopeID: attempt.scopeID),
            syncedTables: [testTable.localSchema]
        )

        let row = try db.queryOne("SELECT id FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertNotNil(row)
        XCTAssertEqual(try pendingChangeCount(db), 0)
    }

    func testRemoveScopeRemovesOrphanedRecordAndLeavesQueueEmpty() throws {
        let (db, processor) = try makeTestEnv()

        try addScopeRow(db, scopeID: "orders:user1", recordID: "w1")
        try insertOrder(db, id: "w1", updatedAt: "2026-01-03T00:00:00.000Z")

        try processor.removeScope(scopeID: "orders:user1", syncedTables: [testTable.localSchema])

        let row = try db.queryOne("SELECT id FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertNil(row)
        XCTAssertEqual(try pendingChangeCount(db), 0)
    }

}
