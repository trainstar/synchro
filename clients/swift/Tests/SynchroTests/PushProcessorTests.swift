import XCTest
import GRDB
@testable import Synchro

final class PushProcessorTests: XCTestCase {
    private let testTable = SchemaTable(
        tableName: "orders",
        pushPolicy: "owner_only",
        updatedAtColumn: "updated_at",
        deletedAtColumn: "deleted_at",
        primaryKey: ["id"],
        columns: [
            SchemaColumn(name: "id", dbType: "uuid", logicalType: "string", nullable: false, isPrimaryKey: true),
            SchemaColumn(name: "ship_address", dbType: "text", logicalType: "string", nullable: true, isPrimaryKey: false),
            SchemaColumn(name: "user_id", dbType: "uuid", logicalType: "string", nullable: false, isPrimaryKey: false),
            SchemaColumn(name: "updated_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: false, isPrimaryKey: false),
            SchemaColumn(name: "deleted_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: true, isPrimaryKey: false),
        ]
    )

    private let customTable = SchemaTable(
        tableName: "custom_items",
        pushPolicy: "owner_only",
        updatedAtColumn: "modified_at",
        deletedAtColumn: "removed_at",
        primaryKey: ["item_id"],
        columns: [
            SchemaColumn(name: "item_id", dbType: "uuid", logicalType: "string", nullable: false, isPrimaryKey: true),
            SchemaColumn(name: "title", dbType: "text", logicalType: "string", nullable: true, isPrimaryKey: false),
            SchemaColumn(name: "modified_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: false, isPrimaryKey: false),
            SchemaColumn(name: "removed_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: true, isPrimaryKey: false),
        ]
    )

    private func makeTestEnv(table: SchemaTable? = nil) throws -> (SynchroDatabase, ChangeTracker, PushProcessor) {
        let t = table ?? testTable
        let tmpDir = NSTemporaryDirectory()
        let path = (tmpDir as NSString).appendingPathComponent("synchro_test_\(UUID().uuidString).sqlite")
        let db = try SynchroDatabase(path: path)
        let manager = SchemaManager(database: db)
        let schema = SchemaResponse(schemaVersion: 1, schemaHash: "test", serverTime: Date(), tables: [t])
        try manager.createSyncedTables(schema: schema)
        let tracker = ChangeTracker(database: db)
        let processor = PushProcessor(database: db, changeTracker: tracker)
        return (db, tracker, processor)
    }

    // MARK: - Hydration Tests

    func testHydratePendingForPush() throws {
        let (db, tracker, _) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z"]
        )

        let pending = try tracker.pendingChanges()
        XCTAssertEqual(pending.count, 1)

        let pushRecords = try tracker.hydratePendingForPush(pending: pending, syncedTables: [testTable])
        XCTAssertEqual(pushRecords.count, 1)
        XCTAssertEqual(pushRecords[0].id, "w1")
        XCTAssertEqual(pushRecords[0].operation, "create")
        XCTAssertNotNil(pushRecords[0].data)
        XCTAssertEqual(pushRecords[0].data?["ship_address"], AnyCodable("123 Main St"))
    }

    func testHydrateDeleteHasNilData() throws {
        let (db, tracker, _) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z"]
        )
        try tracker.clearAll()

        _ = try db.execute("DELETE FROM orders WHERE id = ?", params: ["w1"])

        let pending = try tracker.pendingChanges()
        XCTAssertEqual(pending.count, 1)
        XCTAssertEqual(pending[0].operation, "delete")

        let pushRecords = try tracker.hydratePendingForPush(pending: pending, syncedTables: [testTable])
        XCTAssertEqual(pushRecords.count, 1)
        XCTAssertNil(pushRecords[0].data)
    }

    func testRemovePending() throws {
        let (db, tracker, _) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z"]
        )

        let pending = try tracker.pendingChanges()
        XCTAssertEqual(pending.count, 1)

        try tracker.removePending(entries: pending)
        XCTAssertFalse(try tracker.hasPendingChanges())
    }

    func testHydrateWithCustomPrimaryKey() throws {
        let (db, tracker, _) = try makeTestEnv(table: customTable)

        _ = try db.execute(
            "INSERT INTO custom_items (item_id, title, modified_at) VALUES (?, ?, ?)",
            params: ["ci1", "My Item", "2026-01-01T10:00:00.000Z"]
        )

        let pending = try tracker.pendingChanges()
        XCTAssertEqual(pending.count, 1)
        XCTAssertEqual(pending[0].recordID, "ci1")

        let pushRecords = try tracker.hydratePendingForPush(pending: pending, syncedTables: [customTable])
        XCTAssertEqual(pushRecords.count, 1)
        XCTAssertEqual(pushRecords[0].id, "ci1")
        XCTAssertEqual(pushRecords[0].data?["title"], AnyCodable("My Item"))
        XCTAssertEqual(pushRecords[0].data?["item_id"], AnyCodable("ci1"))
    }

    func testHydrateMultiplePendingChanges() throws {
        let (db, tracker, _) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z"]
        )
        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w2", "456 Oak Ave", "u1", "2026-01-01T10:00:00.000Z"]
        )

        let pending = try tracker.pendingChanges()
        XCTAssertEqual(pending.count, 2)

        let pushRecords = try tracker.hydratePendingForPush(pending: pending, syncedTables: [testTable])
        XCTAssertEqual(pushRecords.count, 2)

        let ids = Set(pushRecords.map { $0.id })
        XCTAssertTrue(ids.contains("w1"))
        XCTAssertTrue(ids.contains("w2"))
    }

    func testHydrateLimitsPendingCount() throws {
        let (db, tracker, _) = try makeTestEnv()

        for i in 1...5 {
            _ = try db.execute(
                "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                params: ["w\(i)", "Address \(i)", "u1", "2026-01-01T10:00:00.000Z"]
            )
        }

        let pending = try tracker.pendingChanges(limit: 3)
        XCTAssertEqual(pending.count, 3)
    }

    // MARK: - applyAccepted Tests

    func testApplyAcceptedRemovesPendingAndAppliesRYOW() throws {
        let (db, tracker, processor) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z"]
        )

        // Verify pending entry exists
        XCTAssertTrue(try tracker.hasPendingChanges())

        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        let serverTime = formatter.date(from: "2026-01-01T12:00:00.000Z")!

        let accepted = [AcceptedMutation(
            mutationID: "m1",
            table: "orders",
            pk: ["id": AnyCodable("w1")],
            status: .applied,
            serverRow: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("123 Main St"),
                "user_id": AnyCodable("u1"),
                "updated_at": AnyCodable("2026-01-01T12:00:00.000Z"),
                "deleted_at": AnyCodable(NSNull())
            ],
            serverVersion: formatter.string(from: serverTime)
        )]

        _ = try processor.applyAccepted(accepted: accepted, syncedTables: [testTable])

        // Pending should be drained
        XCTAssertFalse(try tracker.hasPendingChanges())

        // RYOW: local updated_at should match server timestamp
        let row = try db.queryOne("SELECT updated_at FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["updated_at"] as String?, "2026-01-01T12:00:00.000Z")
    }

    func testApplyAcceptedRYOWWithCustomColumns() throws {
        let (db, tracker, processor) = try makeTestEnv(table: customTable)

        _ = try db.execute(
            "INSERT INTO custom_items (item_id, title, modified_at) VALUES (?, ?, ?)",
            params: ["ci1", "My Item", "2026-01-01T10:00:00.000Z"]
        )

        XCTAssertTrue(try tracker.hasPendingChanges())

        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        let serverTime = formatter.date(from: "2026-01-01T14:00:00.000Z")!

        let accepted = [AcceptedMutation(
            mutationID: "m1",
            table: "custom_items",
            pk: ["item_id": AnyCodable("ci1")],
            status: .applied,
            serverRow: [
                "item_id": AnyCodable("ci1"),
                "title": AnyCodable("My Item"),
                "modified_at": AnyCodable("2026-01-01T14:00:00.000Z"),
                "removed_at": AnyCodable(NSNull())
            ],
            serverVersion: formatter.string(from: serverTime)
        )]

        _ = try processor.applyAccepted(accepted: accepted, syncedTables: [customTable])

        // RYOW should write to "modified_at", not "updated_at"
        let row = try db.queryOne("SELECT modified_at FROM custom_items WHERE item_id = ?", params: ["ci1"])
        XCTAssertEqual(row?["modified_at"] as String?, "2026-01-01T14:00:00.000Z")
    }

    func testApplyAcceptedDeleteRYOW() throws {
        let (db, tracker, processor) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z"]
        )
        try tracker.clearAll()
        _ = try db.execute("DELETE FROM orders WHERE id = ?", params: ["w1"])

        XCTAssertTrue(try tracker.hasPendingChanges())

        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        let serverTime = formatter.date(from: "2026-01-01T12:00:00.000Z")!

        let accepted = [AcceptedMutation(
            mutationID: "m1",
            table: "orders",
            pk: ["id": AnyCodable("w1")],
            status: .applied,
            serverRow: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("123 Main St"),
                "user_id": AnyCodable("u1"),
                "updated_at": AnyCodable("2026-01-01T10:00:00.000Z"),
                "deleted_at": AnyCodable("2026-01-01T12:00:00.000Z")
            ],
            serverVersion: formatter.string(from: serverTime)
        )]

        _ = try processor.applyAccepted(accepted: accepted, syncedTables: [testTable])

        XCTAssertFalse(try tracker.hasPendingChanges())

        let row = try db.queryOne("SELECT deleted_at FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["deleted_at"] as String?, "2026-01-01T12:00:00.000Z")
    }

    func testApplyAcceptedSupportsOpaqueServerVersion() throws {
        let (db, tracker, processor) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z"]
        )

        let accepted = [AcceptedMutation(
            mutationID: "m1",
            table: "orders",
            pk: ["id": AnyCodable("w1")],
            status: .applied,
            serverRow: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("Opaque Version Address"),
                "user_id": AnyCodable("u1"),
                "updated_at": AnyCodable("2026-01-01T12:00:00.000Z"),
                "deleted_at": AnyCodable(NSNull())
            ],
            serverVersion: "sv::opaque::1"
        )]

        _ = try processor.applyAccepted(accepted: accepted, syncedTables: [testTable])

        XCTAssertFalse(try tracker.hasPendingChanges())
        let row = try db.queryOne("SELECT ship_address, updated_at FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["ship_address"] as String?, "Opaque Version Address")
        XCTAssertEqual(row?["updated_at"] as String?, "2026-01-01T12:00:00.000Z")
    }

    func testApplyAcceptedDoesNotTriggerCDC() throws {
        let (db, tracker, processor) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z"]
        )

        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]

        let accepted = [AcceptedMutation(
            mutationID: "m1",
            table: "orders",
            pk: ["id": AnyCodable("w1")],
            status: .applied,
            serverRow: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("123 Main St"),
                "user_id": AnyCodable("u1"),
                "updated_at": AnyCodable("2026-01-01T12:00:00.000Z"),
                "deleted_at": AnyCodable(NSNull())
            ],
            serverVersion: "2026-01-01T12:00:00.000Z"
        )]

        _ = try processor.applyAccepted(accepted: accepted, syncedTables: [testTable])

        // Pending queue should be empty — sync_lock prevented the RYOW update from re-queuing
        XCTAssertFalse(try tracker.hasPendingChanges())
    }

    func testApplyAcceptedAppliesCanonicalServerRow() throws {
        let (db, tracker, processor) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "Client Address", "u1", "2026-01-01T10:00:00.000Z"]
        )

        let accepted = [AcceptedMutation(
            mutationID: "m1",
            table: "orders",
            pk: ["id": AnyCodable("w1")],
            status: .applied,
            serverRow: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("Canonical Address"),
                "user_id": AnyCodable("u1"),
                "updated_at": AnyCodable("2026-01-01T12:00:00.000Z"),
                "deleted_at": AnyCodable(NSNull())
            ],
            serverVersion: "2026-01-01T12:00:00.000Z"
        )]

        _ = try processor.applyAccepted(accepted: accepted, syncedTables: [testTable])

        XCTAssertFalse(try tracker.hasPendingChanges())

        let row = try db.queryOne("SELECT ship_address, updated_at FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["ship_address"] as String?, "Canonical Address")
        XCTAssertEqual(row?["updated_at"] as String?, "2026-01-01T12:00:00.000Z")
    }

    // MARK: - applyRejected Tests

    func testApplyRejectedAppliesServerVersion() throws {
        let (db, tracker, processor) = try makeTestEnv()

        // Insert local record
        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "Client Address", "u1", "2026-01-01T10:00:00.000Z"]
        )

        XCTAssertTrue(try tracker.hasPendingChanges())

        let serverVersion = Record(
            id: "w1",
            tableName: "orders",
            data: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("Server Address"),
                "user_id": AnyCodable("u1"),
                "updated_at": AnyCodable("2026-01-01T11:00:00.000Z"),
            ],
            updatedAt: ISO8601DateFormatter().date(from: "2026-01-01T11:00:00Z")!
        )

        let rejected = [RejectedMutation(
            mutationID: "m1",
            table: "orders",
            pk: ["id": AnyCodable("w1")],
            status: .conflict,
            code: .versionConflict,
            message: "server version is newer",
            serverRow: serverVersion.data,
            serverVersion: "2026-01-01T11:00:00.000Z"
        )]

        let conflicts = try processor.applyRejected(rejected: rejected, syncedTables: [testTable])

        // Pending should be drained
        XCTAssertFalse(try tracker.hasPendingChanges())

        // Local record should have server's data
        let row = try db.queryOne("SELECT ship_address, updated_at FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["ship_address"] as String?, "Server Address")
        XCTAssertEqual(row?["updated_at"] as String?, "2026-01-01T11:00:00.000Z")

        // Should fire conflict event
        XCTAssertEqual(conflicts.count, 1)
        XCTAssertEqual(conflicts[0].table, "orders")
        XCTAssertEqual(conflicts[0].recordID, "w1")
        XCTAssertEqual(conflicts[0].serverData?["ship_address"], AnyCodable("Server Address"))

        let storedRejections = try db.readTransaction { db in
            try SynchroMeta.listRejectedMutations(db)
        }
        XCTAssertEqual(storedRejections.count, 1)
        XCTAssertEqual(storedRejections[0].mutationID, "m1")
        XCTAssertEqual(storedRejections[0].status, MutationStatus.conflict.rawValue)
        XCTAssertEqual(storedRejections[0].code, MutationRejectionCode.versionConflict.rawValue)
        XCTAssertEqual(storedRejections[0].serverVersion, "2026-01-01T11:00:00.000Z")
    }

    func testApplyRejectedWithoutServerVersion() throws {
        let (db, tracker, processor) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "Client Address", "u1", "2026-01-01T10:00:00.000Z"]
        )

        let rejected = [RejectedMutation(
            mutationID: "m1",
            table: "orders",
            pk: ["id": AnyCodable("w1")],
            status: .rejectedTerminal,
            code: .policyRejected,
            message: "ownership violation",
            serverRow: nil,
            serverVersion: nil
        )]

        let conflicts = try processor.applyRejected(rejected: rejected, syncedTables: [testTable])

        // Pending drained
        XCTAssertFalse(try tracker.hasPendingChanges())

        // Local record unchanged (no server version to apply)
        let row = try db.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["ship_address"] as String?, "Client Address")

        // Error status, not conflict — no conflict event
        XCTAssertEqual(conflicts.count, 0)

        let storedRejections = try db.readTransaction { db in
            try SynchroMeta.listRejectedMutations(db)
        }
        XCTAssertEqual(storedRejections.count, 1)
        XCTAssertEqual(storedRejections[0].mutationID, "m1")
        XCTAssertEqual(storedRejections[0].status, MutationStatus.rejectedTerminal.rawValue)
        XCTAssertEqual(storedRejections[0].code, MutationRejectionCode.policyRejected.rawValue)
        XCTAssertEqual(storedRejections[0].message, "ownership violation")
    }

    func testApplyRejectedConflictAppliesCanonicalServerRow() throws {
        let (db, tracker, processor) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "Client Address", "u1", "2026-01-01T10:00:00.000Z"]
        )

        let rejected = [RejectedMutation(
            mutationID: "m1",
            table: "orders",
            pk: ["id": AnyCodable("w1")],
            status: .conflict,
            code: .versionConflict,
            message: "server version is newer",
            serverRow: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("Server Address"),
                "user_id": AnyCodable("u1"),
                "updated_at": AnyCodable("2026-01-01T11:00:00.000Z"),
                "deleted_at": AnyCodable(NSNull())
            ],
            serverVersion: "2026-01-01T11:00:00.000Z"
        )]

        let conflicts = try processor.applyRejected(rejected: rejected, syncedTables: [testTable])

        XCTAssertFalse(try tracker.hasPendingChanges())

        let row = try db.queryOne("SELECT ship_address, updated_at FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["ship_address"] as String?, "Server Address")
        XCTAssertEqual(row?["updated_at"] as String?, "2026-01-01T11:00:00.000Z")
        XCTAssertEqual(conflicts.count, 1)
        XCTAssertEqual(conflicts[0].serverData?["ship_address"], AnyCodable("Server Address"))
    }

    func testApplyRejectedDoesNotTriggerCDC() throws {
        let (db, tracker, processor) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "Client Address", "u1", "2026-01-01T10:00:00.000Z"]
        )

        let serverVersion = Record(
            id: "w1",
            tableName: "orders",
            data: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("Server Address"),
                "user_id": AnyCodable("u1"),
                "updated_at": AnyCodable("2026-01-01T11:00:00.000Z"),
            ],
            updatedAt: ISO8601DateFormatter().date(from: "2026-01-01T11:00:00Z")!
        )

        let rejected = [RejectedMutation(
            mutationID: "m1",
            table: "orders",
            pk: ["id": AnyCodable("w1")],
            status: .conflict,
            code: .versionConflict,
            message: "server version is newer",
            serverRow: serverVersion.data,
            serverVersion: "2026-01-01T11:00:00.000Z"
        )]

        _ = try processor.applyRejected(rejected: rejected, syncedTables: [testTable])

        // sync_lock should have prevented CDC triggers from re-queuing
        XCTAssertFalse(try tracker.hasPendingChanges())
    }
}
