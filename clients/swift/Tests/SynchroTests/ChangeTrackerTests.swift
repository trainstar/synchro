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
            schemaHash: protocolTestSchemaHash,
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

    private func markOrderAsSynced(_ db: SynchroDatabase, tracker: ChangeTracker) throws {
        try db.writeTransaction { connection in
            try SynchroMeta.upsertRowVersion(
                connection,
                tableName: "orders",
                recordID: "w1",
                serverVersion: "server-version-1",
                rowChecksum: nil
            )
        }
        try tracker.clearAll()
    }

    private func makeTypedTestEnv() throws -> (SynchroDatabase, ChangeTracker, LocalSchemaTable) {
        let path = (NSTemporaryDirectory() as NSString).appendingPathComponent("synchro_typed_\(UUID().uuidString).sqlite")
        let db = try SynchroDatabase(path: path)
        let manager = SchemaManager(database: db)
        let table = SchemaTable(
            tableName: "typed_values",
            updatedAtColumn: "updated_at",
            deletedAtColumn: "deleted_at",
            primaryKey: ["id"],
            columns: [
                SchemaColumn(name: "id", logicalType: "string", nullable: false, isPrimaryKey: true),
                SchemaColumn(name: "int64_value", logicalType: "int64", nullable: false, isPrimaryKey: false),
                SchemaColumn(name: "int_value", logicalType: "int", nullable: false, isPrimaryKey: false),
                SchemaColumn(
                    name: "decimal_value",
                    logicalType: "decimal",
                    nullable: false,
                    precision: 6,
                    scale: 2,
                    isPrimaryKey: false
                ),
                SchemaColumn(name: "json_value", logicalType: "json", nullable: false, isPrimaryKey: false),
                SchemaColumn(name: "nullable_value", logicalType: "string", nullable: true, isPrimaryKey: false),
                SchemaColumn(name: "bool_value", logicalType: "boolean", nullable: false, isPrimaryKey: false),
                SchemaColumn(name: "float_value", logicalType: "float", nullable: false, isPrimaryKey: false),
                SchemaColumn(name: "blob_value", logicalType: "bytes", nullable: false, isPrimaryKey: false),
                SchemaColumn(name: "updated_at", logicalType: "datetime", nullable: false, isPrimaryKey: false),
                SchemaColumn(name: "deleted_at", logicalType: "datetime", nullable: true, isPrimaryKey: false),
            ]
        )
        try manager.createSyncedTables(
            schema: SchemaResponse(schemaVersion: 1, schemaHash: protocolTestSchemaHash, serverTime: Date(), tables: [table])
        )
        return (db, ChangeTracker(database: db), table)
    }

    func testInsertTriggerCreatesEntry() throws {
        let (db, tracker, _) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"]
        )

        let pending = try tracker.pendingChanges()
        XCTAssertEqual(pending.count, 1)
        XCTAssertEqual(pending[0].recordID, "w1")
        XCTAssertEqual(pending[0].tableName, "orders")
        XCTAssertEqual(pending[0].operation, "insert")
    }

    func testUpdateTriggerCreatesEntry() throws {
        let (db, tracker, _) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"]
        )
        try markOrderAsSynced(db, tracker: tracker)

        _ = try db.execute(
            "UPDATE orders SET ship_address = ? WHERE id = ?",
            params: ["456 Oak Ave", "w1"]
        )

        let pending = try tracker.pendingChanges()
        XCTAssertEqual(pending.count, 1)
        XCTAssertEqual(pending[0].operation, "update")
        XCTAssertNotNil(pending[0].baseUpdatedAt)
    }

    func testDeleteTriggerConvertsSoftDelete() throws {
        let (db, tracker, _) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"]
        )
        try markOrderAsSynced(db, tracker: tracker)

        // Hard DELETE should be intercepted by BEFORE DELETE trigger
        _ = try db.execute("DELETE FROM orders WHERE id = ?", params: ["w1"])

        // Record should still exist with deleted_at set
        let row = try db.queryOne("SELECT deleted_at FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertNotNil(row)
        let deletedAt: String? = row?["deleted_at"]
        XCTAssertNotNil(deletedAt)

        // Pending queue should have a delete operation (from UPDATE trigger fired by BEFORE DELETE)
        let pending = try tracker.pendingChanges()
        XCTAssertEqual(pending.count, 1)
        XCTAssertEqual(pending[0].operation, "delete")
        XCTAssertTrue(pending[0].fieldValuesByID.isEmpty)
    }

    func testDeleteTriggerCapturesHardDeleteWhenTableHasNoSoftDeleteField() throws {
        let path = (NSTemporaryDirectory() as NSString)
            .appendingPathComponent("synchro_hard_delete_\(UUID().uuidString).sqlite")
        let db = try SynchroDatabase(path: path)
        let tracker = ChangeTracker(database: db)
        let manager = SchemaManager(database: db)
        let table = LocalSchemaTable(
            tableID: "hard_items",
            relationID: "public.hard_items",
            tableName: "hard_items",
            primaryKeyFieldID: "id",
            createdAtFieldID: nil,
            updatedAtFieldID: nil,
            deletedAtFieldID: nil,
            updatedAtColumn: "",
            deletedAtColumn: "",
            composition: .singleScope,
            primaryKey: ["id"],
            columns: [
                LocalSchemaColumn(
                    fieldID: "id", name: "id", logicalType: "string", nullable: false,
                    writable: false, precision: nil, scale: nil, sqliteDefaultSQL: nil, isPrimaryKey: true
                ),
                LocalSchemaColumn(
                    fieldID: "value", name: "value", logicalType: "string", nullable: false,
                    writable: true, precision: nil, scale: nil, sqliteDefaultSQL: nil, isPrimaryKey: false
                ),
            ]
        )
        try manager.reconcileLocalSchema(
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            tables: [table]
        )
        _ = try db.execute(
            "INSERT INTO hard_items (id, value) VALUES (?, ?)",
            params: ["row-1", "value"]
        )
        try db.writeTransaction { connection in
            try SynchroMeta.upsertRowVersion(
                connection,
                tableName: table.tableName,
                recordID: "row-1",
                serverVersion: "server-version-1",
                rowChecksum: nil
            )
        }
        try tracker.clearAll()

        _ = try db.execute("DELETE FROM hard_items WHERE id = ?", params: ["row-1"])

        XCTAssertNil(try db.queryOne("SELECT id FROM hard_items WHERE id = ?", params: ["row-1"]))
        let pending = try tracker.pendingChanges()
        XCTAssertEqual(pending.count, 1)
        XCTAssertEqual(pending[0].operation, "delete")
        XCTAssertEqual(pending[0].baseUpdatedAt, "server-version-1")
        XCTAssertTrue(pending[0].fieldValuesByID.isEmpty)
    }

    func testDedupCreateThenUpdate() throws {
        let (db, tracker, _) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"]
        )
        _ = try db.execute(
            "UPDATE orders SET ship_address = ? WHERE id = ?",
            params: ["789 Updated Blvd", "w1"]
        )

        let pending = try tracker.pendingChanges()
        XCTAssertEqual(pending.count, 1)
        // insert + update normalizes to one insert.
        XCTAssertEqual(pending[0].operation, "insert")
        let sourceStates = try db.query(
            "SELECT mutation_id, lifecycle_state, normalized_mutation_id FROM _synchro_pending_changes WHERE table_name = 'orders' ORDER BY local_order",
            params: nil
        )
        XCTAssertEqual(sourceStates.count, 3)
        XCTAssertEqual(sourceStates[0]["lifecycle_state"] as String?, "superseded_before_send")
        XCTAssertEqual(sourceStates[1]["lifecycle_state"] as String?, "superseded_before_send")
        XCTAssertEqual(sourceStates[2]["lifecycle_state"] as String?, "unsealed")
        XCTAssertEqual(sourceStates[0]["normalized_mutation_id"] as String?, sourceStates[2]["mutation_id"] as String?)
    }

    func testDedupCreateThenDeleteRemovesEntry() throws {
        let (db, tracker, _) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"]
        )
        _ = try db.execute("DELETE FROM orders WHERE id = ?", params: ["w1"])

        // create + delete = removed entirely (never reached server)
        let pending = try tracker.pendingChanges()
        XCTAssertEqual(pending.count, 0)
        let cancelled = try db.query(
            "SELECT lifecycle_state, normalized_mutation_id FROM _synchro_pending_changes WHERE table_name = 'orders' ORDER BY local_order",
            params: nil
        )
        XCTAssertEqual(cancelled.count, 2)
        XCTAssertTrue(cancelled.allSatisfy { ($0["lifecycle_state"] as String?) == "cancelled_before_send" })
        XCTAssertNotNil(cancelled[0]["normalized_mutation_id"] as String?)
        XCTAssertEqual(cancelled[0]["normalized_mutation_id"] as String?, cancelled[1]["normalized_mutation_id"] as String?)
    }

    func testSyncLockPreventsTracking() throws {
        let (db, tracker, _) = try makeTestEnv()

        try db.writeTransaction { dbConn in
            try SynchroMeta.setSyncLock(dbConn, locked: true)
        }

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"]
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
            "INSERT INTO orders (id, ship_address, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"]
        )

        XCTAssertTrue(try tracker.hasPendingChanges())
        try tracker.clearAll()
        XCTAssertFalse(try tracker.hasPendingChanges())
    }

    func testDedupUpdateThenDelete() throws {
        let (db, tracker, _) = try makeTestEnv()

        // Insert and clear (simulate already-pushed create)
        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"]
        )
        try markOrderAsSynced(db, tracker: tracker)

        // Update then delete — should dedup to "delete"
        _ = try db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["555 Renamed Dr", "w1"])
        _ = try db.execute("DELETE FROM orders WHERE id = ?", params: ["w1"])

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
            "INSERT INTO orders (id, ship_address, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"]
        )
        try markOrderAsSynced(db, tracker: tracker)

        // Two sequential updates — should dedup to single "update"
        _ = try db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["100 First Ave", "w1"])
        _ = try db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["200 Second Blvd", "w1"])

        let pending = try tracker.pendingChanges()
        XCTAssertEqual(pending.count, 1)
        XCTAssertEqual(pending[0].operation, "update")
        // base_updated_at should be from the original record (before first update)
        XCTAssertNotNil(pending[0].baseUpdatedAt)
    }

    func testUnhydratablePendingChangeFailsWithoutDeletingIntent() throws {
        let (db, tracker, _) = try makeTestEnv()
        try db.writeTransaction { connection in
            try connection.execute(
                sql: """
                    INSERT INTO _synchro_pending_changes
                        (mutation_id, capture_uuid, table_id, table_name, record_id, pk_field_id, pk_logical_type,
                         operation, authored_schema_version, authored_schema_hash, client_version, lifecycle_state,
                         source_kind, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'unsealed', 'test', ?, ?)
                    """,
                arguments: [
                    UUID().uuidString.lowercased(), UUID().uuidString.lowercased(), "orders", "orders", "missing-1", "id", "string",
                    "insert", 1, protocolTestSchemaHash, "2026-01-01T00:00:00.000000Z", "2026-01-01T00:00:00.000000Z", "2026-01-01T00:00:00.000000Z",
                ]
            )
        }

        let pending = try tracker.pendingChanges()
        XCTAssertThrowsError(try tracker.hydratePendingForPush(pending: pending, syncedTables: [])) { error in
            guard case SynchroError.invalidResponse = error else {
                return XCTFail("expected explicit hydration failure, got \(error)")
            }
        }
        XCTAssertEqual(try tracker.pendingChangeCount(), 1)
    }

    func testTypedCaptureRetainsImmutableValuesAndWireForms() throws {
        let (db, tracker, table) = try makeTypedTestEnv()
        let blob = Data([0x00, 0x01, 0x02])
        _ = try db.execute(
            """
            INSERT INTO typed_values
                (id, int64_value, int_value, decimal_value, json_value, nullable_value, bool_value, float_value, blob_value, updated_at)
            VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)
            """,
            params: ["t1", "9223372036854775807", 7, "12.3400", "{\"b\":2,\"a\":1}", 1, 3.5, blob, "2026-01-01T00:00:00Z"]
        )

        let pending = try tracker.pendingChanges()
        XCTAssertEqual(pending.count, 1)
        let hydrated = try tracker.hydratePendingForPush(pending: pending, syncedTables: [table])
        let fields = try XCTUnwrap(hydrated.first?.fieldValuesByID)
        XCTAssertEqual(fields["int64_value"]?.wireValue, AnyCodable("9223372036854775807"))
        XCTAssertEqual(fields["int_value"]?.wireValue, AnyCodable(Int64(7)))
        XCTAssertEqual(fields["decimal_value"]?.wireValue, AnyCodable("12.3400"))
        XCTAssertEqual(fields["json_value"]?.wireValue, AnyCodable("{\"b\":2,\"a\":1}"))
        XCTAssertEqual(fields["nullable_value"]?.wireValue, AnyCodable(NSNull()))
        XCTAssertEqual(fields["bool_value"]?.wireValue, AnyCodable(true))
        XCTAssertEqual(fields["float_value"]?.wireValue, AnyCodable(3.5))
        XCTAssertEqual(fields["blob_value"]?.wireValue, AnyCodable("AAEC"))

        try db.writeSyncLockedTransaction { connection in
            try connection.execute(
                sql: "UPDATE typed_values SET int64_value = ?, decimal_value = ?, json_value = ?, blob_value = ? WHERE id = ?",
                arguments: ["1", "changed", "changed", Data([0xff]), "t1"]
            )
        }
        let afterRowMutation = try tracker.pendingChanges()
        let retained = try tracker.hydratePendingForPush(pending: afterRowMutation, syncedTables: [table])
        let retainedFields = try XCTUnwrap(retained.first?.fieldValuesByID)
        XCTAssertEqual(retainedFields["int64_value"]?.wireValue, AnyCodable("9223372036854775807"))
        XCTAssertEqual(retainedFields["decimal_value"]?.wireValue, AnyCodable("12.3400"))
        XCTAssertEqual(retainedFields["blob_value"]?.wireValue, AnyCodable("AAEC"))
    }

    func testSchemaArchiveMismatchRollsBackApplicationWriteAndCapture() throws {
        let (db, tracker, _) = try makeTestEnv()
        try db.writeTransaction { connection in
            try connection.execute(sql: "DELETE FROM _synchro_schema_archive")
        }
        XCTAssertThrowsError(
            try db.execute(
                "INSERT INTO orders (id, ship_address, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                params: ["archive-mismatch", "blocked", "u1", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"]
            )
        )
        XCTAssertNil(try db.queryOne("SELECT id FROM orders WHERE id = 'archive-mismatch'", params: nil))
        XCTAssertEqual(try tracker.pendingChangeCount(), 0)
    }
}
