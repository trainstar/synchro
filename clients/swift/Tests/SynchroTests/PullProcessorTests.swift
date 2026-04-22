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

    private func makeTestEnv() throws -> (SynchroDatabase, PullProcessor) {
        let tmpDir = NSTemporaryDirectory()
        let path = (tmpDir as NSString).appendingPathComponent("synchro_test_\(UUID().uuidString).sqlite")
        let db = try SynchroDatabase(path: path)
        let manager = SchemaManager(database: db)
        let schema = SchemaResponse(schemaVersion: 1, schemaHash: "test", serverTime: Date(), tables: [testTable])
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
        checksum: Int32 = 7,
        generation: Int64 = 0
    ) throws {
        try db.writeTransaction { conn in
            try SynchroMeta.upsertScope(conn, scopeID: scopeID, cursor: "10", checksum: nil, generation: generation)
            try SynchroMeta.upsertScopeRow(
                conn,
                scopeID: scopeID,
                tableName: "orders",
                recordID: recordID,
                checksum: checksum,
                generation: generation
            )
        }
    }

    private func addBucketMember(_ db: SynchroDatabase, bucketID: String, recordID: String) throws {
        try db.writeTransaction { conn in
            try SynchroMeta.upsertBucketMember(
                conn,
                bucketID: bucketID,
                tableName: "orders",
                recordID: recordID,
                checksum: 1
            )
        }
    }

    private func pendingChangeCount(_ db: SynchroDatabase) throws -> Int {
        try ChangeTracker(database: db).pendingChangeCount()
    }

    func testApplyChangesInsertsRecords() throws {
        let (db, processor) = try makeTestEnv()

        let record = Record(
            id: "w1",
            tableName: "orders",
            data: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("123 Main St"),
                "updated_at": AnyCodable("2026-01-01T12:00:00.000Z"),
            ],
            updatedAt: ISO8601DateFormatter().date(from: "2026-01-01T12:00:00Z")!
        )

        try processor.applyChanges(changes: [record], syncedTables: [testTable])

        let row = try db.queryOne("SELECT * FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertNotNil(row)
        XCTAssertEqual(row?["ship_address"] as String?, "123 Main St")
    }

    func testApplyChangesUpdatesExisting() throws {
        let (db, processor) = try makeTestEnv()

        // Disable sync lock to allow insert trigger
        try db.writeTransaction { conn in
            try SynchroMeta.setSyncLock(conn, locked: true)
        }
        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, updated_at) VALUES (?, ?, ?)",
            params: ["w1", "Old Address", "2026-01-01T10:00:00.000Z"]
        )
        try db.writeTransaction { conn in
            try SynchroMeta.setSyncLock(conn, locked: false)
        }

        let record = Record(
            id: "w1",
            tableName: "orders",
            data: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("New Address"),
                "updated_at": AnyCodable("2026-01-01T12:00:00.000Z"),
            ],
            updatedAt: ISO8601DateFormatter().date(from: "2026-01-01T12:00:00Z")!
        )

        try processor.applyChanges(changes: [record], syncedTables: [testTable])

        let row = try db.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["ship_address"] as String?, "New Address")
    }

    func testApplyDeletesSetsDeletedAt() throws {
        let (db, processor) = try makeTestEnv()

        try db.writeTransaction { conn in
            try SynchroMeta.setSyncLock(conn, locked: true)
        }
        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, updated_at) VALUES (?, ?, ?)",
            params: ["w1", "123 Main St", "2026-01-01T10:00:00.000Z"]
        )
        try db.writeTransaction { conn in
            try SynchroMeta.setSyncLock(conn, locked: false)
        }

        try processor.applyDeletes(
            deletes: [DeleteEntry(id: "w1", tableName: "orders")],
            syncedTables: [testTable]
        )

        let row = try db.queryOne("SELECT deleted_at FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertNotNil(row)
        let deletedAt: String? = row?["deleted_at"]
        XCTAssertNotNil(deletedAt)
    }

    func testApplyChangesSkipsRYOW() throws {
        let (db, processor) = try makeTestEnv()

        // Insert with a newer timestamp locally
        try db.writeTransaction { conn in
            try SynchroMeta.setSyncLock(conn, locked: true)
        }
        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, updated_at) VALUES (?, ?, ?)",
            params: ["w1", "Local Address", "2026-01-01T15:00:00.000Z"]
        )
        try db.writeTransaction { conn in
            try SynchroMeta.setSyncLock(conn, locked: false)
        }

        // Try to apply an older server record
        let record = Record(
            id: "w1",
            tableName: "orders",
            data: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("Server Address"),
                "updated_at": AnyCodable("2026-01-01T12:00:00.000Z"),
            ],
            updatedAt: ISO8601DateFormatter().date(from: "2026-01-01T12:00:00Z")!
        )

        try processor.applyChanges(changes: [record], syncedTables: [testTable])

        // Should keep local name (RYOW dedup)
        let row = try db.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["ship_address"] as String?, "Local Address")
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

    func testApplyChangesAcceptsNewerServerVersion() throws {
        let (db, processor) = try makeTestEnv()

        // Insert local record at T1
        try db.writeTransaction { conn in
            try SynchroMeta.setSyncLock(conn, locked: true)
        }
        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, updated_at) VALUES (?, ?, ?)",
            params: ["w1", "Local Address", "2026-01-01T10:00:00.000Z"]
        )
        try db.writeTransaction { conn in
            try SynchroMeta.setSyncLock(conn, locked: false)
        }

        // Apply server record at T2 > T1 — should overwrite local
        let record = Record(
            id: "w1",
            tableName: "orders",
            data: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("Server Address"),
                "updated_at": AnyCodable("2026-01-01T15:00:00.000Z"),
            ],
            updatedAt: ISO8601DateFormatter().date(from: "2026-01-01T15:00:00Z")!
        )

        try processor.applyChanges(changes: [record], syncedTables: [testTable])

        let row = try db.queryOne("SELECT ship_address, updated_at FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["ship_address"] as String?, "Server Address")
        XCTAssertEqual(row?["updated_at"] as String?, "2026-01-01T15:00:00.000Z")
    }

    func testSyncLockPreventsTriggering() throws {
        let (db, processor) = try makeTestEnv()
        let tracker = ChangeTracker(database: db)

        let record = Record(
            id: "w1",
            tableName: "orders",
            data: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("Pull Applied Address"),
                "updated_at": AnyCodable("2026-01-01T12:00:00.000Z"),
            ],
            updatedAt: ISO8601DateFormatter().date(from: "2026-01-01T12:00:00Z")!
        )

        try processor.applyChanges(changes: [record], syncedTables: [testTable])

        // Pending queue should be empty (sync_lock was on during apply)
        let pending = try tracker.pendingChanges()
        XCTAssertEqual(pending.count, 0)
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
                checksum: 7,
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

        let change = ChangeRecord(
            scope: "orders:user1",
            table: "orders",
            op: .delete,
            pk: ["id": AnyCodable("w1")],
            row: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("123 Main St"),
                "updated_at": AnyCodable("2026-01-04T00:00:00.000Z"),
                "deleted_at": AnyCodable("2026-01-04T00:00:00.000Z"),
            ],
            serverVersion: "2026-01-04T00:00:00.000Z"
        )

        try processor.applyScopeChanges(
            changes: [change],
            syncedTables: [testTable.localSchema],
            scopeCursors: ["orders:user1": "11"],
            checksums: nil
        )

        let row = try db.queryOne("SELECT deleted_at FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["deleted_at"] as String?, "2026-01-04T00:00:00.000Z")
    }

    func testApplyScopeDeleteUsesDeletedAtAsEffectiveVersion() throws {
        let (db, processor) = try makeTestEnv()

        try addScopeRow(db, scopeID: "orders:user1", recordID: "w1")
        try insertOrder(db, id: "w1", updatedAt: "2026-01-03T00:00:00.000Z")

        let change = ChangeRecord(
            scope: "orders:user1",
            table: "orders",
            op: .delete,
            pk: ["id": AnyCodable("w1")],
            row: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("123 Main St"),
                "updated_at": AnyCodable("2026-01-03T00:00:00.000Z"),
                "deleted_at": AnyCodable("2026-01-04T00:00:00.000Z"),
            ],
            serverVersion: "2026-01-04T00:00:00.000Z"
        )

        try processor.applyScopeChanges(
            changes: [change],
            syncedTables: [testTable.localSchema],
            scopeCursors: ["orders:user1": "11"],
            checksums: nil
        )

        let row = try db.queryOne(
            "SELECT updated_at, deleted_at FROM orders WHERE id = ?",
            params: ["w1"]
        )
        XCTAssertEqual(row?["updated_at"] as String?, "2026-01-03T00:00:00.000Z")
        XCTAssertEqual(row?["deleted_at"] as String?, "2026-01-04T00:00:00.000Z")
    }

    func testApplyScopeDeleteWithoutRowRemovesOrphanedRecordAndLeavesQueueEmpty() throws {
        let (db, processor) = try makeTestEnv()

        try addScopeRow(db, scopeID: "orders:user1", recordID: "w1")
        try insertOrder(db, id: "w1", updatedAt: "2026-01-03T00:00:00.000Z")

        let change = ChangeRecord(
            scope: "orders:user1",
            table: "orders",
            op: .delete,
            pk: ["id": AnyCodable("w1")],
            row: nil,
            serverVersion: "2026-01-04T00:00:00.000Z"
        )

        try processor.applyScopeChanges(
            changes: [change],
            syncedTables: [testTable.localSchema],
            scopeCursors: ["orders:user1": "11"],
            checksums: nil
        )

        let row = try db.queryOne("SELECT id FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertNil(row)
        XCTAssertEqual(try pendingChangeCount(db), 0)
    }

    func testApplyScopeDeleteRejectsRowWithoutDeletedAt() throws {
        let (db, processor) = try makeTestEnv()

        try addScopeRow(db, scopeID: "orders:user1", recordID: "w1")
        try insertOrder(db, id: "w1", updatedAt: "2026-01-03T00:00:00.000Z")

        let change = ChangeRecord(
            scope: "orders:user1",
            table: "orders",
            op: .delete,
            pk: ["id": AnyCodable("w1")],
            row: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("123 Main St"),
                "updated_at": AnyCodable("2026-01-04T00:00:00.000Z"),
                "deleted_at": AnyCodable(NSNull())
            ],
            serverVersion: "2026-01-04T00:00:00.000Z"
        )

        XCTAssertThrowsError(
            try processor.applyScopeChanges(
                changes: [change],
                syncedTables: [testTable.localSchema],
                scopeCursors: ["orders:user1": "11"],
                checksums: nil
            )
        )
    }

    func testFinalizeScopeRebuildRemovesOrphanedRecordAndLeavesQueueEmpty() throws {
        let (db, processor) = try makeTestEnv()

        try addScopeRow(db, scopeID: "orders:user1", recordID: "w1", generation: 1)
        try insertOrder(db, id: "w1", updatedAt: "2026-01-03T00:00:00.000Z")

        try processor.finalizeScopeRebuild(
            scopeID: "orders:user1",
            generation: 2,
            finalCursor: "scope_cursor_20",
            checksum: "0",
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

        try processor.finalizeScopeRebuild(
            scopeID: "orders:user1",
            generation: 2,
            finalCursor: "scope_cursor_20",
            checksum: "0",
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

    func testRemoveScopeKeepsRecordBackedByBucketMembership() throws {
        let (db, processor) = try makeTestEnv()

        try addScopeRow(db, scopeID: "orders:user1", recordID: "w1")
        try addBucketMember(db, bucketID: "bucket-1", recordID: "w1")
        try insertOrder(db, id: "w1", updatedAt: "2026-01-03T00:00:00.000Z")

        try processor.removeScope(scopeID: "orders:user1", syncedTables: [testTable.localSchema])

        let row = try db.queryOne("SELECT id FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertNotNil(row)
        XCTAssertEqual(try pendingChangeCount(db), 0)
    }

    func testDeleteBucketOrphanedRecordsRemovesUnreferencedRowsAndLeavesQueueEmpty() throws {
        let (db, processor) = try makeTestEnv()

        try addBucketMember(db, bucketID: "bucket-1", recordID: "w1")
        try insertOrder(db, id: "w1", updatedAt: "2026-01-03T00:00:00.000Z")

        try processor.deleteBucketOrphanedRecords(bucketID: "bucket-1", syncedTables: [testTable.localSchema])

        let row = try db.queryOne("SELECT id FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertNil(row)
        XCTAssertEqual(try pendingChangeCount(db), 0)
    }

    func testTrackBucketMembershipRejectsMissingChecksum() throws {
        let (_, processor) = try makeTestEnv()

        let record = Record(
            id: "w1",
            tableName: "orders",
            data: [
                "id": AnyCodable("w1"),
                "user_id": AnyCodable("user-1"),
                "updated_at": AnyCodable("2026-01-03T00:00:00.000Z"),
            ],
            updatedAt: Date(),
            deletedAt: nil,
            bucketID: "bucket-1",
            checksum: nil
        )

        XCTAssertThrowsError(try processor.trackBucketMembership(records: [record])) { error in
            guard case let SynchroError.invalidResponse(message) = error else {
                return XCTFail("expected invalidResponse, got \(error)")
            }
            XCTAssertTrue(message.contains("missing record checksum"))
        }
    }
}
