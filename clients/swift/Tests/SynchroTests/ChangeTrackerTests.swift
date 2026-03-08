import XCTest
import GRDB
@testable import Synchro

final class ChangeTrackerTests: XCTestCase {
    private func makeTestEnv() throws -> (SynchroDatabase, ChangeTracker, SchemaManager) {
        let tmpDir = NSTemporaryDirectory()
        let path = (tmpDir as NSString).appendingPathComponent("synchro_test_\(UUID().uuidString).sqlite")
        let db = try SynchroDatabase(path: path)
        let tracker = ChangeTracker(database: db)
        let manager = SchemaManager(database: db)

        let schema = SchemaResponse(
            schemaVersion: 1,
            schemaHash: "test",
            serverTime: Date(),
            tables: [
                SchemaTable(
                    tableName: "workouts",
                    pushPolicy: "owner_only",
                    updatedAtColumn: "updated_at",
                    deletedAtColumn: "deleted_at",
                    primaryKey: ["id"],
                    columns: [
                        SchemaColumn(name: "id", dbType: "uuid", logicalType: "string", nullable: false, isPrimaryKey: true),
                        SchemaColumn(name: "name", dbType: "text", logicalType: "string", nullable: true, isPrimaryKey: false),
                        SchemaColumn(name: "user_id", dbType: "uuid", logicalType: "string", nullable: false, isPrimaryKey: false),
                        SchemaColumn(name: "created_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: false, isPrimaryKey: false),
                        SchemaColumn(name: "updated_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: false, isPrimaryKey: false),
                        SchemaColumn(name: "deleted_at", dbType: "timestamp with time zone", logicalType: "datetime", nullable: true, isPrimaryKey: false),
                    ]
                )
            ]
        )
        try manager.createSyncedTables(schema: schema)
        return (db, tracker, manager)
    }

    func testInsertTriggerCreatesEntry() throws {
        let (db, tracker, _) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO workouts (id, name, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            params: ["w1", "Push Day", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"]
        )

        let pending = try tracker.pendingChanges()
        XCTAssertEqual(pending.count, 1)
        XCTAssertEqual(pending[0].recordID, "w1")
        XCTAssertEqual(pending[0].tableName, "workouts")
        XCTAssertEqual(pending[0].operation, "create")
    }

    func testUpdateTriggerCreatesEntry() throws {
        let (db, tracker, _) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO workouts (id, name, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            params: ["w1", "Push Day", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"]
        )
        try tracker.clearAll()

        _ = try db.execute(
            "UPDATE workouts SET name = ? WHERE id = ?",
            params: ["Pull Day", "w1"]
        )

        let pending = try tracker.pendingChanges()
        XCTAssertEqual(pending.count, 1)
        XCTAssertEqual(pending[0].operation, "update")
        XCTAssertNotNil(pending[0].baseUpdatedAt)
    }

    func testDeleteTriggerConvertsSoftDelete() throws {
        let (db, tracker, _) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO workouts (id, name, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            params: ["w1", "Push Day", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"]
        )
        try tracker.clearAll()

        // Hard DELETE should be intercepted by BEFORE DELETE trigger
        _ = try db.execute("DELETE FROM workouts WHERE id = ?", params: ["w1"])

        // Record should still exist with deleted_at set
        let row = try db.queryOne("SELECT deleted_at FROM workouts WHERE id = ?", params: ["w1"])
        XCTAssertNotNil(row)
        let deletedAt: String? = row?["deleted_at"]
        XCTAssertNotNil(deletedAt)

        // Pending queue should have a delete operation (from UPDATE trigger fired by BEFORE DELETE)
        let pending = try tracker.pendingChanges()
        XCTAssertEqual(pending.count, 1)
        XCTAssertEqual(pending[0].operation, "delete")
    }

    func testDedupCreateThenUpdate() throws {
        let (db, tracker, _) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO workouts (id, name, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            params: ["w1", "Push Day", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"]
        )
        _ = try db.execute(
            "UPDATE workouts SET name = ? WHERE id = ?",
            params: ["Updated Push Day", "w1"]
        )

        let pending = try tracker.pendingChanges()
        XCTAssertEqual(pending.count, 1)
        // create + update = still create
        XCTAssertEqual(pending[0].operation, "create")
    }

    func testDedupCreateThenDeleteRemovesEntry() throws {
        let (db, tracker, _) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO workouts (id, name, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            params: ["w1", "Push Day", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"]
        )
        _ = try db.execute("DELETE FROM workouts WHERE id = ?", params: ["w1"])

        // create + delete = removed entirely (never reached server)
        let pending = try tracker.pendingChanges()
        XCTAssertEqual(pending.count, 0)
    }

    func testSyncLockPreventsTracking() throws {
        let (db, tracker, _) = try makeTestEnv()

        try db.writeTransaction { dbConn in
            try SynchroMeta.setSyncLock(dbConn, locked: true)
        }

        _ = try db.execute(
            "INSERT INTO workouts (id, name, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            params: ["w1", "Push Day", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"]
        )

        let pending = try tracker.pendingChanges()
        XCTAssertEqual(pending.count, 0)

        try db.writeTransaction { dbConn in
            try SynchroMeta.setSyncLock(dbConn, locked: false)
        }
    }

    func testClearAll() throws {
        let (db, tracker, _) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO workouts (id, name, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            params: ["w1", "Push Day", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"]
        )

        XCTAssertTrue(try tracker.hasPendingChanges())
        try tracker.clearAll()
        XCTAssertFalse(try tracker.hasPendingChanges())
    }

    func testDedupUpdateThenDelete() throws {
        let (db, tracker, _) = try makeTestEnv()

        // Insert and clear (simulate already-pushed create)
        _ = try db.execute(
            "INSERT INTO workouts (id, name, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            params: ["w1", "Push Day", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"]
        )
        try tracker.clearAll()

        // Update then delete — should dedup to "delete"
        _ = try db.execute("UPDATE workouts SET name = ? WHERE id = ?", params: ["Renamed", "w1"])
        _ = try db.execute("DELETE FROM workouts WHERE id = ?", params: ["w1"])

        let pending = try tracker.pendingChanges()
        XCTAssertEqual(pending.count, 1)
        XCTAssertEqual(pending[0].operation, "delete")
        // base_updated_at should be preserved from the update (not nil — record existed on server)
        XCTAssertNotNil(pending[0].baseUpdatedAt)
    }

    func testDedupUpdateThenUpdate() throws {
        let (db, tracker, _) = try makeTestEnv()

        // Insert and clear (simulate already-pushed create)
        _ = try db.execute(
            "INSERT INTO workouts (id, name, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            params: ["w1", "Push Day", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"]
        )
        try tracker.clearAll()

        // Two sequential updates — should dedup to single "update"
        _ = try db.execute("UPDATE workouts SET name = ? WHERE id = ?", params: ["First Edit", "w1"])
        _ = try db.execute("UPDATE workouts SET name = ? WHERE id = ?", params: ["Second Edit", "w1"])

        let pending = try tracker.pendingChanges()
        XCTAssertEqual(pending.count, 1)
        XCTAssertEqual(pending[0].operation, "update")
        // base_updated_at should be from the original record (before first update)
        XCTAssertNotNil(pending[0].baseUpdatedAt)
    }
}
