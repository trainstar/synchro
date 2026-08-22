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
        let schema = SchemaResponse(schemaVersion: 1, schemaHash: protocolTestSchemaHash, serverTime: Date(), tables: [t])
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
        XCTAssertEqual(pushRecords[0].operation, "insert")
        XCTAssertNotNil(pushRecords[0].data)
        XCTAssertEqual(pushRecords[0].data?["ship_address"], AnyCodable("123 Main St"))
    }

    func testHydrateDeleteHasNilData() throws {
        let (db, tracker, _) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z"]
        )
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

        _ = try db.execute("DELETE FROM orders WHERE id = ?", params: ["w1"])

        let pending = try tracker.pendingChanges()
        XCTAssertEqual(pending.count, 1)
        XCTAssertEqual(pending[0].operation, "delete")

        let pushRecords = try tracker.hydratePendingForPush(pending: pending, syncedTables: [testTable])
        XCTAssertEqual(pushRecords.count, 1)
        XCTAssertNil(pushRecords[0].data)
    }

    func testResponseLossReplaysSealedBatchAndPreservesSuccessor() async throws {
        let (db, tracker, processor) = try makeTestEnv()
        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "original", "u1", "2026-01-01T10:00:00.000Z"]
        )
        try db.writeTransaction { connection in
            try SynchroMeta.upsertRowVersion(
                connection,
                tableName: "orders",
                recordID: "w1",
                serverVersion: "opaque-v1",
                rowChecksum: nil
            )
        }
        try tracker.clearAll()
        _ = try db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["first", "w1"])

        let sessionConfiguration = URLSessionConfiguration.ephemeral
        sessionConfiguration.protocolClasses = [MockURLProtocol.self]
        let session = URLSession(configuration: sessionConfiguration)
        defer {
            MockURLProtocol.requestHandler = nil
            session.invalidateAndCancel()
        }
        let config = SynchroConfig(
            dbPath: db.path,
            serverURL: URL(string: "http://test.local")!,
            authProvider: { "test-token" },
            clientID: "test-device",
            appVersion: "1.0.0"
        )
        let httpClient = HttpClient(config: config, session: session)
        let decoder = JSONDecoder.synchroDecoder()
        let encoder = JSONEncoder.synchroEncoder()
        var requests: [PushRequest] = []
        var requestBodies: [Data] = []
        var loseFirstResponse = true
        MockURLProtocol.requestHandler = { request in
            let body = try XCTUnwrap(request.bodyData())
            requestBodies.append(body)
            let pushRequest = try decoder.decode(PushRequest.self, from: body)
            requests.append(pushRequest)
            if loseFirstResponse {
                loseFirstResponse = false
                throw URLError(.networkConnectionLost)
            }
            let serverVersion = "opaque-v2"
            let serverRow: [String: AnyCodable] = [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("first"),
                "user_id": AnyCodable("u1"),
                "updated_at": AnyCodable("2026-01-01T11:00:00.000000Z"),
                "deleted_at": AnyCodable(NSNull())
            ]
            let accepted = try makeAcceptedMutation(
                mutationID: pushRequest.mutations[0].mutationID,
                schema: self.testTable,
                pk: ["id": AnyCodable("w1")],
                status: .applied,
                serverRow: serverRow,
                serverVersion: serverVersion
            )
            let response = PushResponse(
                batchID: pushRequest.batchID,
                serverTime: "2026-01-01T11:00:00.000000Z",
                accepted: [accepted],
                rejected: []
            )
            let httpResponse = HTTPURLResponse(
                url: request.url!,
                statusCode: 200,
                httpVersion: nil,
                headerFields: ["Content-Type": "application/json"]
            )!
            return (httpResponse, try encoder.encode(response))
        }

        do {
            _ = try await processor.processPush(
                httpClient: httpClient,
                clientID: "test-device",
                clientGeneration: 1,
                schemaVersion: 1,
                schemaHash: protocolTestSchemaHash,
                syncedTables: [testTable]
            )
            XCTFail("expected response loss")
        } catch is RetryableError {
        }

        let sealed = try db.queryOne(
            "SELECT batch_id, request_json, state FROM _synchro_push_batches WHERE state = 'pending'",
            params: nil
        )
        XCTAssertNotNil(sealed)
        _ = try db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["second", "w1"])
        let path = db.path
        try db.close()

        let reopenedDatabase = try SynchroDatabase(path: path)
        defer { try? reopenedDatabase.close() }
        let restartedTracker = ChangeTracker(database: reopenedDatabase)
        let restartedProcessor = PushProcessor(database: reopenedDatabase, changeTracker: restartedTracker)
        _ = try await restartedProcessor.processPush(
            httpClient: httpClient,
            clientID: "test-device",
            clientGeneration: 1,
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            syncedTables: [testTable]
        )

        XCTAssertEqual(requests.count, 2)
        XCTAssertEqual(requests[0], requests[1])
        XCTAssertEqual(requestBodies.count, 2)
        XCTAssertEqual(requestBodies[0], requestBodies[1])
        let completed = try reopenedDatabase.queryOne(
            "SELECT state, completed_at FROM _synchro_push_batches WHERE batch_id = ?",
            params: [requests[0].batchID]
        )
        XCTAssertEqual(completed?["state"] as String?, "completed")
        XCTAssertNotNil(completed?["completed_at"] as String?)
        let successor = try restartedTracker.pendingChanges()
        XCTAssertEqual(successor.count, 1)
        XCTAssertEqual(successor[0].operation, "update")
        XCTAssertEqual(successor[0].baseUpdatedAt, "opaque-v2")
        let row = try reopenedDatabase.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["ship_address"] as String?, "second")
    }

    func testPushCompletionClearsMatchingDurableBackoffWithCommittedState() async throws {
        let (db, _, processor) = try makeTestEnv()
        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "local", "u1", "2026-01-01T10:00:00.000000Z"]
        )

        let sessionConfiguration = URLSessionConfiguration.ephemeral
        sessionConfiguration.protocolClasses = [MockURLProtocol.self]
        let session = URLSession(configuration: sessionConfiguration)
        defer {
            MockURLProtocol.requestHandler = nil
            session.invalidateAndCancel()
        }
        let config = SynchroConfig(
            dbPath: db.path,
            serverURL: URL(string: "http://test.local")!,
            authProvider: { "test-token" },
            clientID: "test-device",
            appVersion: "1.0.0"
        )
        let httpClient = HttpClient(config: config, session: session)
        let decoder = JSONDecoder.synchroDecoder()
        let encoder = JSONEncoder.synchroEncoder()
        MockURLProtocol.requestHandler = { request in
            let body = try XCTUnwrap(request.bodyData())
            let pushRequest = try decoder.decode(PushRequest.self, from: body)
            try db.writeTransaction { connection in
                try SynchroMeta.upsertBackoffRecord(
                    connection,
                    record: LocalBackoffRecord(
                        resumeState: .pushing,
                        workIdentity: pushRequest.batchID,
                        retryClassification: .network,
                        attemptCount: 1,
                        nextRetryAtMS: 1
                    )
                )
            }
            let serverVersion = "opaque-server-version"
            let serverRow: [String: AnyCodable] = [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("local"),
                "user_id": AnyCodable("u1"),
                "updated_at": AnyCodable("2026-01-01T11:00:00.000000Z"),
                "deleted_at": AnyCodable(NSNull()),
            ]
            let accepted = try makeAcceptedMutation(
                mutationID: try XCTUnwrap(pushRequest.mutations.first?.mutationID),
                schema: self.testTable,
                pk: ["id": AnyCodable("w1")],
                status: .applied,
                serverRow: serverRow,
                serverVersion: serverVersion
            )
            let response = PushResponse(
                batchID: pushRequest.batchID,
                serverTime: "2026-01-01T11:00:00.000000Z",
                accepted: [accepted],
                rejected: []
            )
            let httpResponse = HTTPURLResponse(
                url: request.url!,
                statusCode: 200,
                httpVersion: nil,
                headerFields: ["Content-Type": "application/json"]
            )!
            return (httpResponse, try encoder.encode(response))
        }

        _ = try await processor.processPush(
            httpClient: httpClient,
            clientID: "test-device",
            clientGeneration: 1,
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            syncedTables: [testTable.localSchema]
        )

        XCTAssertEqual(
            try db.queryOne(
                "SELECT state FROM _synchro_push_batches",
                params: nil
            )?["state"] as String?,
            "completed"
        )
        XCTAssertNil(try db.readTransaction { try SynchroMeta.getBackoffRecord($0) })

        let path = db.path
        try db.close()
        let recovered = try SynchroDatabase(path: path)
        defer { try? recovered.close() }
        XCTAssertEqual(
            try recovered.queryOne(
                "SELECT state FROM _synchro_push_batches",
                params: nil
            )?["state"] as String?,
            "completed"
        )
        XCTAssertNil(try recovered.readTransaction { try SynchroMeta.getBackoffRecord($0) })
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
        XCTAssertNil(pushRecords[0].data?["item_id"])
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

        let accepted = [try makeAcceptedMutation(
            mutationID: "m1",
            schema: testTable,
            pk: ["id": AnyCodable("w1")],
            status: .applied,
            serverRow: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("123 Main St"),
                "user_id": AnyCodable("u1"),
                "updated_at": AnyCodable("2026-01-01T12:00:00.000000Z"),
                "deleted_at": AnyCodable(NSNull())
            ],
            serverVersion: formatter.string(from: serverTime)
        )]

        _ = try processor.applyAccepted(accepted: accepted, syncedTables: [testTable])

        // Pending should be drained
        XCTAssertFalse(try tracker.hasPendingChanges())

        // RYOW: local updated_at should match server timestamp
        let row = try db.queryOne("SELECT updated_at FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["updated_at"] as String?, "2026-01-01T12:00:00.000000Z")
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

        let accepted = [try makeAcceptedMutation(
            mutationID: "m1",
            schema: customTable,
            pk: ["item_id": AnyCodable("ci1")],
            status: .applied,
            serverRow: [
                "item_id": AnyCodable("ci1"),
                "title": AnyCodable("My Item"),
                "modified_at": AnyCodable("2026-01-01T14:00:00.000000Z"),
                "removed_at": AnyCodable(NSNull())
            ],
            serverVersion: formatter.string(from: serverTime)
        )]

        _ = try processor.applyAccepted(accepted: accepted, syncedTables: [customTable])

        // RYOW should write to "modified_at", not "updated_at"
        let row = try db.queryOne("SELECT modified_at FROM custom_items WHERE item_id = ?", params: ["ci1"])
        XCTAssertEqual(row?["modified_at"] as String?, "2026-01-01T14:00:00.000000Z")
    }

    func testApplyAcceptedDeleteRYOW() throws {
        let (db, tracker, processor) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z"]
        )
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
        _ = try db.execute("DELETE FROM orders WHERE id = ?", params: ["w1"])

        XCTAssertTrue(try tracker.hasPendingChanges())

        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        let serverTime = formatter.date(from: "2026-01-01T12:00:00.000Z")!

        let accepted = [try makeAcceptedMutation(
            mutationID: "m1",
            schema: testTable,
            pk: ["id": AnyCodable("w1")],
            status: .applied,
            serverRow: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("123 Main St"),
                "user_id": AnyCodable("u1"),
                "updated_at": AnyCodable("2026-01-01T10:00:00.000000Z"),
                "deleted_at": AnyCodable("2026-01-01T12:00:00.000000Z")
            ],
            serverVersion: formatter.string(from: serverTime)
        )]

        _ = try processor.applyAccepted(accepted: accepted, syncedTables: [testTable])

        XCTAssertFalse(try tracker.hasPendingChanges())

        let row = try db.queryOne("SELECT deleted_at FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["deleted_at"] as String?, "2026-01-01T12:00:00.000000Z")
    }

    func testApplyAcceptedSupportsOpaqueServerVersion() throws {
        let (db, tracker, processor) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z"]
        )

        let accepted = [try makeAcceptedMutation(
            mutationID: "m1",
            schema: testTable,
            pk: ["id": AnyCodable("w1")],
            status: .applied,
            serverRow: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("Opaque Version Address"),
                "user_id": AnyCodable("u1"),
                "updated_at": AnyCodable("2026-01-01T12:00:00.000000Z"),
                "deleted_at": AnyCodable(NSNull())
            ],
            serverVersion: "sv::opaque::1"
        )]

        _ = try processor.applyAccepted(accepted: accepted, syncedTables: [testTable])

        XCTAssertFalse(try tracker.hasPendingChanges())
        let row = try db.queryOne("SELECT ship_address, updated_at FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["ship_address"] as String?, "Opaque Version Address")
        XCTAssertEqual(row?["updated_at"] as String?, "2026-01-01T12:00:00.000000Z")
    }

    func testApplyAcceptedDoesNotTriggerCDC() throws {
        let (db, tracker, processor) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z"]
        )

        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]

        let accepted = [try makeAcceptedMutation(
            mutationID: "m1",
            schema: testTable,
            pk: ["id": AnyCodable("w1")],
            status: .applied,
            serverRow: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("123 Main St"),
                "user_id": AnyCodable("u1"),
                "updated_at": AnyCodable("2026-01-01T12:00:00.000000Z"),
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

        let accepted = [try makeAcceptedMutation(
            mutationID: "m1",
            schema: testTable,
            pk: ["id": AnyCodable("w1")],
            status: .applied,
            serverRow: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("Canonical Address"),
                "user_id": AnyCodable("u1"),
                "updated_at": AnyCodable("2026-01-01T12:00:00.000000Z"),
                "deleted_at": AnyCodable(NSNull())
            ],
            serverVersion: "2026-01-01T12:00:00.000Z"
        )]

        _ = try processor.applyAccepted(accepted: accepted, syncedTables: [testTable])

        XCTAssertFalse(try tracker.hasPendingChanges())

        let row = try db.queryOne("SELECT ship_address, updated_at FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["ship_address"] as String?, "Canonical Address")
        XCTAssertEqual(row?["updated_at"] as String?, "2026-01-01T12:00:00.000000Z")
    }

    func testApplyAcceptedPreservesNewerLocalMutationAndRow() throws {
        let (db, tracker, processor) = try makeTestEnv()
        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "Initial", "u1", "2026-01-01T10:00:00.000000Z"]
        )
        try db.writeTransaction { connection in
            try connection.execute(
                sql: "UPDATE _synchro_pending_changes SET client_version = ? WHERE table_name = ? AND record_id = ?",
                arguments: ["2026-01-01T10:00:01.000000Z", "orders", "w1"]
            )
        }
        let sent = try tracker.pendingChanges()[0]
        _ = try db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["Newer local", "w1"])
        try db.writeTransaction { connection in
            try connection.execute(
                sql: "UPDATE _synchro_pending_changes SET client_version = ? WHERE table_name = ? AND record_id = ?",
                arguments: ["2026-01-01T10:00:02.000000Z", "orders", "w1"]
            )
        }

        let accepted = [try makeAcceptedMutation(
            mutationID: "m-newer-accepted",
            schema: testTable,
            pk: ["id": AnyCodable("w1")],
            status: .applied,
            serverRow: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("Server result"),
                "user_id": AnyCodable("u1"),
                "updated_at": AnyCodable("2026-01-01T11:00:00.000000Z"),
                "deleted_at": AnyCodable(NSNull()),
            ],
            serverVersion: "sv-accepted"
        )]
        _ = try processor.applyAccepted(
            accepted: accepted,
            syncedTables: [testTable],
            sentPending: [accepted[0].mutationID: sent]
        )

        let row = try db.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["ship_address"] as String?, "Newer local")
        XCTAssertEqual(try tracker.pendingChangeCount(), 1)
        XCTAssertEqual(try db.readTransaction { try SynchroMeta.getRowVersion($0, tableName: "orders", recordID: "w1") }, "sv-accepted")
    }

    func testAcceptedPredecessorRebasesUnsealedUpdateSuccessor() throws {
        let (db, tracker, processor) = try makeTestEnv()
        try db.writeSyncLockedTransaction { connection in
            try connection.execute(
                sql: "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arguments: ["w1", "Server base", "u1", "2026-01-01T10:00:00.000000Z"]
            )
            try SynchroMeta.upsertRowVersion(
                connection,
                tableName: "orders",
                recordID: "w1",
                serverVersion: "sv-old",
                rowChecksum: nil
            )
        }
        _ = try db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["First local", "w1"])
        let sent = try XCTUnwrap(try tracker.pendingChanges().first)
        _ = try db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["Successor local", "w1"])

        let accepted = try makeAcceptedMutation(
            mutationID: "m-accepted-predecessor",
            schema: testTable,
            pk: ["id": AnyCodable("w1")],
            status: .applied,
            serverRow: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("First local"),
                "user_id": AnyCodable("u1"),
                "updated_at": AnyCodable("2026-01-01T11:00:00.000000Z"),
                "deleted_at": AnyCodable(NSNull()),
            ],
            serverVersion: "sv-accepted"
        )
        _ = try processor.applyAccepted(
            accepted: [accepted],
            syncedTables: [testTable],
            sentPending: [accepted.mutationID: sent]
        )

        let retained = try XCTUnwrap(try tracker.pendingChanges().first)
        XCTAssertEqual(retained.baseUpdatedAt, "sv-accepted")
        let hydrated = try tracker.hydratePendingForPush(pending: [retained], syncedTables: [testTable])
        XCTAssertEqual(hydrated.first?.baseUpdatedAt, "sv-accepted")
    }

    func testAcceptedDeleteFencePreservesLaterProjectionAndStoresReturnedVersion() throws {
        let (db, tracker, processor) = try makeTestEnv()
        try db.writeSyncLockedTransaction { connection in
            try connection.execute(
                sql: "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arguments: ["w1", "server", "u1", "2026-01-01T10:00:00.000000Z"]
            )
            try SynchroMeta.upsertRowVersion(
                connection,
                tableName: "orders",
                recordID: "w1",
                serverVersion: "sv-start",
                rowChecksum: nil
            )
        }
        _ = try db.execute("DELETE FROM orders WHERE id = ?", params: ["w1"])
        let predecessor = try XCTUnwrap(try tracker.pendingChanges().first)
        try db.writeTransaction { connection in
            try tracker.markPendingAsSealed(
                connection,
                batchID: UUID().uuidString.lowercased(),
                pending: [predecessor]
            )
        }
        _ = try db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["later local", "w1"])

        let accepted = AcceptedMutation(
            mutationID: predecessor.mutationID,
            table: testTable.tableID,
            pk: ["id": AnyCodable("w1")],
            outcomeSchema: SchemaRef(version: 1, hash: protocolTestSchemaHash),
            status: .applied,
            serverRow: nil,
            rowChecksum: nil,
            serverVersion: "delete-fence"
        )
        _ = try processor.applyAccepted(
            accepted: [accepted],
            syncedTables: [testTable],
            sentPending: [predecessor.mutationID: predecessor]
        )

        let row = try db.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["ship_address"] as String?, "later local")
        XCTAssertEqual(
            try db.readTransaction { try SynchroMeta.getRowVersion($0, tableName: "orders", recordID: "w1") },
            "delete-fence"
        )
        XCTAssertEqual(try tracker.pendingChanges().first?.baseUpdatedAt, "delete-fence")
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
        let sent = try XCTUnwrap(try tracker.pendingChanges().first)

        let serverRow = [
            "id": AnyCodable("w1"),
            "ship_address": AnyCodable("Server Address"),
            "user_id": AnyCodable("u1"),
            "updated_at": AnyCodable("2026-01-01T11:00:00.000000Z"),
            "deleted_at": AnyCodable(NSNull()),
        ]

        let rejected = [try makeRejectedMutation(
            mutationID: sent.mutationID,
            schema: testTable,
            pk: ["id": AnyCodable("w1")],
            status: .conflict,
            code: .versionConflict,
            message: "server version is newer",
            serverRow: serverRow,
            serverVersion: "2026-01-01T11:00:00.000Z"
        )]

        let conflicts = try processor.applyRejected(
            rejected: rejected,
            syncedTables: [testTable],
            sentPending: [sent.mutationID: sent]
        )

        // Pending should be drained
        XCTAssertFalse(try tracker.hasPendingChanges())

        // Local record should have server's data
        let row = try db.queryOne("SELECT ship_address, updated_at FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["ship_address"] as String?, "Server Address")
        XCTAssertEqual(row?["updated_at"] as String?, "2026-01-01T11:00:00.000000Z")

        // Should fire conflict event
        XCTAssertEqual(conflicts.count, 1)
        XCTAssertEqual(conflicts[0].table, "orders")
        XCTAssertEqual(conflicts[0].recordID, "w1")
        XCTAssertEqual(conflicts[0].serverData?["ship_address"], AnyCodable("Server Address"))

        let storedRejections = try db.readTransaction { db in
            try SynchroMeta.listRejectedMutations(db)
        }
        XCTAssertEqual(storedRejections.count, 1)
        XCTAssertEqual(storedRejections[0].mutationID, sent.mutationID)
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
        let sent = try XCTUnwrap(try tracker.pendingChanges().first)

        let rejected = [try makeRejectedMutation(
            mutationID: sent.mutationID,
            schema: testTable,
            pk: ["id": AnyCodable("w1")],
            status: .rejectedTerminal,
            code: .policyRejected,
            message: "ownership violation",
            serverRow: nil,
            serverVersion: nil
        )]

        let conflicts = try processor.applyRejected(
            rejected: rejected,
            syncedTables: [testTable],
            sentPending: [sent.mutationID: sent]
        )

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
        XCTAssertEqual(storedRejections[0].mutationID, sent.mutationID)
        XCTAssertEqual(storedRejections[0].status, MutationStatus.rejectedTerminal.rawValue)
        XCTAssertEqual(storedRejections[0].code, MutationRejectionCode.policyRejected.rawValue)
        XCTAssertEqual(storedRejections[0].message, "ownership violation")
    }

    func testSchemaIncompatibleRejectionRetainsCompleteOriginalAndExactOutcomeJSON() throws {
        let (db, tracker, processor) = try makeTestEnv()
        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "Authored address", "u1", "2026-01-01T10:00:00.000Z"]
        )
        let sent = try XCTUnwrap(try tracker.pendingChanges().first)
        let authoredSchema = SchemaRef(version: 1, hash: protocolTestSchemaHash)
        let currentSchema = SchemaRef(version: 2, hash: String(repeating: "1", count: 64))
        var rejected = try makeRejectedMutation(
            mutationID: sent.mutationID,
            schema: testTable,
            pk: ["id": AnyCodable("w1")],
            status: .rejectedTerminal,
            code: .schemaIncompatible,
            message: "field was removed",
            authoredSchema: authoredSchema,
            currentSchema: currentSchema,
            incompatibleFieldIDs: ["removed-field-id"]
        )
        rejected.retryable = false
        _ = try processor.applyRejected(
            rejected: [rejected],
            syncedTables: [testTable],
            sentPending: [sent.mutationID: sent]
        )

        let stored = try XCTUnwrap(try db.readTransaction { connection in
            try SynchroMeta.listRejectedMutations(connection).first
        })
        let mutationJSON = try XCTUnwrap(stored.mutationJSON)
        let retainedMutation = try JSONDecoder.synchroDecoder().decode(Mutation.self, from: Data(mutationJSON.utf8))
        XCTAssertEqual(retainedMutation.mutationID, sent.mutationID)
        XCTAssertEqual(retainedMutation.authoredSchema, authoredSchema)
        XCTAssertEqual(retainedMutation.columns?["ship_address"], AnyCodable("Authored address"))

        let rejectedJSON = try XCTUnwrap(stored.rejectedJSON)
        let expectedRejectedJSON = String(
            data: try JSONEncoder.synchroEncoder().encode(rejected),
            encoding: .utf8
        )
        XCTAssertEqual(rejectedJSON, expectedRejectedJSON)
        let retainedRejected = try JSONDecoder.synchroDecoder().decode(RejectedMutation.self, from: Data(rejectedJSON.utf8))
        XCTAssertEqual(retainedRejected.authoredSchema, authoredSchema)
        XCTAssertEqual(retainedRejected.currentSchema, currentSchema)
        XCTAssertEqual(retainedRejected.incompatibleFieldIDs, ["removed-field-id"])
        XCTAssertEqual(retainedRejected.retryable, false)
    }

    func testApplyRejectedConflictAppliesCanonicalServerRow() throws {
        let (db, tracker, processor) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "Client Address", "u1", "2026-01-01T10:00:00.000Z"]
        )

        let rejected = [try makeRejectedMutation(
            mutationID: "m1",
            schema: testTable,
            pk: ["id": AnyCodable("w1")],
            status: .conflict,
            code: .versionConflict,
            message: "server version is newer",
            serverRow: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("Server Address"),
                "user_id": AnyCodable("u1"),
                "updated_at": AnyCodable("2026-01-01T11:00:00.000000Z"),
                "deleted_at": AnyCodable(NSNull())
            ],
            serverVersion: "2026-01-01T11:00:00.000Z"
        )]

        let conflicts = try processor.applyRejected(rejected: rejected, syncedTables: [testTable])

        XCTAssertFalse(try tracker.hasPendingChanges())

        let row = try db.queryOne("SELECT ship_address, updated_at FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["ship_address"] as String?, "Server Address")
        XCTAssertEqual(row?["updated_at"] as String?, "2026-01-01T11:00:00.000000Z")
        XCTAssertEqual(conflicts.count, 1)
        XCTAssertEqual(conflicts[0].serverData?["ship_address"], AnyCodable("Server Address"))
    }

    func testApplyRejectedPreservesNewerLocalMutationAndRow() throws {
        let (db, tracker, processor) = try makeTestEnv()
        try db.writeSyncLockedTransaction { connection in
            try connection.execute(
                sql: "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arguments: ["w1", "Initial", "u1", "2026-01-01T10:00:00.000000Z"]
            )
            try SynchroMeta.upsertRowVersion(
                connection,
                tableName: "orders",
                recordID: "w1",
                serverVersion: "base-version",
                rowChecksum: nil
            )
        }
        _ = try db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["First local", "w1"])
        let sent = try tracker.pendingChanges()[0]
        try db.writeTransaction { connection in
            try tracker.markPendingAsSealed(connection, batchID: UUID().uuidString.lowercased(), pending: [sent])
        }
        _ = try db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["Newer local", "w1"])

        let rejected = [try makeRejectedMutation(
            mutationID: sent.mutationID,
            schema: testTable,
            pk: ["id": AnyCodable("w1")],
            status: .conflict,
            code: .versionConflict,
            message: "conflict",
            serverRow: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("Server result"),
                "user_id": AnyCodable("u1"),
                "updated_at": AnyCodable("2026-01-01T11:00:00.000000Z"),
                "deleted_at": AnyCodable(NSNull()),
            ],
            serverVersion: "sv-rejected"
        )]
        _ = try processor.applyRejected(
            rejected: rejected,
            syncedTables: [testTable],
            sentPending: [sent.mutationID: sent]
        )

        let row = try db.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["ship_address"] as String?, "Newer local")
        XCTAssertEqual(try tracker.pendingChangeCount(), 1)
        let retained = try db.queryOne(
            "SELECT lifecycle_state, dependency_mutation_id, base_version FROM _synchro_pending_changes WHERE mutation_id <> ? AND table_name = 'orders' ORDER BY local_order DESC LIMIT 1",
            params: [sent.mutationID]
        )
        XCTAssertEqual(retained?["lifecycle_state"] as String?, "blocked_by_predecessor")
        XCTAssertEqual(retained?["dependency_mutation_id"] as String?, sent.mutationID)
        XCTAssertEqual(retained?["base_version"] as String?, nil)
        XCTAssertEqual(
            try db.readTransaction { try SynchroMeta.getRowVersion($0, tableName: "orders", recordID: "w1") },
            "sv-rejected"
        )
        let rejections = try db.readTransaction { try SynchroMeta.listRejectedMutations($0) }
        XCTAssertEqual(rejections.count, 1)
    }

    func testRejectedPredecessorDoesNotRebaseUpdateSuccessor() throws {
        let (db, tracker, processor) = try makeTestEnv()
        try db.writeSyncLockedTransaction { connection in
            try connection.execute(
                sql: "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arguments: ["w1", "Server base", "u1", "2026-01-01T10:00:00.000000Z"]
            )
            try SynchroMeta.upsertRowVersion(
                connection,
                tableName: "orders",
                recordID: "w1",
                serverVersion: "sv-old",
                rowChecksum: nil
            )
        }
        _ = try db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["First local", "w1"])
        let sent = try XCTUnwrap(try tracker.pendingChanges().first)
        try db.writeTransaction { connection in
            try tracker.markPendingAsSealed(connection, batchID: UUID().uuidString.lowercased(), pending: [sent])
        }
        _ = try db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["Successor local", "w1"])

        let rejected = try makeRejectedMutation(
            mutationID: sent.mutationID,
            schema: testTable,
            pk: ["id": AnyCodable("w1")],
            status: .conflict,
            code: .versionConflict,
            message: "conflict",
            serverRow: [
                "id": AnyCodable("w1"),
                "ship_address": AnyCodable("Server result"),
                "user_id": AnyCodable("u1"),
                "updated_at": AnyCodable("2026-01-01T11:00:00.000000Z"),
                "deleted_at": AnyCodable(NSNull()),
            ],
            serverVersion: "sv-rejected"
        )
        _ = try processor.applyRejected(
            rejected: [rejected],
            syncedTables: [testTable],
            sentPending: [sent.mutationID: sent]
        )

        let retained = try XCTUnwrap(try db.queryOne(
            "SELECT lifecycle_state, base_version, dependency_mutation_id FROM _synchro_pending_changes WHERE mutation_id <> ? ORDER BY local_order DESC LIMIT 1",
            params: [sent.mutationID]
        ))
        XCTAssertEqual(retained["lifecycle_state"] as String?, "blocked_by_predecessor")
        XCTAssertEqual(retained["base_version"] as String?, nil)
        XCTAssertEqual(retained["dependency_mutation_id"] as String?, sent.mutationID)
    }

    func testRejectedDeleteFencePreservesLaterProjectionAndStoresReturnedVersion() throws {
        let (db, tracker, processor) = try makeTestEnv()
        try db.writeSyncLockedTransaction { connection in
            try connection.execute(
                sql: "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                arguments: ["w1", "server", "u1", "2026-01-01T10:00:00.000000Z"]
            )
            try SynchroMeta.upsertRowVersion(
                connection,
                tableName: "orders",
                recordID: "w1",
                serverVersion: "sv-start",
                rowChecksum: nil
            )
        }
        _ = try db.execute("DELETE FROM orders WHERE id = ?", params: ["w1"])
        let predecessor = try XCTUnwrap(try tracker.pendingChanges().first)
        try db.writeTransaction { connection in
            try tracker.markPendingAsSealed(
                connection,
                batchID: UUID().uuidString.lowercased(),
                pending: [predecessor]
            )
        }
        _ = try db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["later local", "w1"])
        let successorID = try XCTUnwrap(
            try db.queryOne(
                "SELECT mutation_id FROM _synchro_pending_changes WHERE mutation_id <> ? ORDER BY local_order DESC LIMIT 1",
                params: [predecessor.mutationID]
            )?["mutation_id"] as String?
        )

        let rejected = RejectedMutation(
            mutationID: predecessor.mutationID,
            table: testTable.tableID,
            pk: ["id": AnyCodable("w1")],
            outcomeSchema: SchemaRef(version: 1, hash: protocolTestSchemaHash),
            status: .conflict,
            code: .rowDeleted,
            message: "row was deleted",
            retryable: nil,
            serverRow: nil,
            rowChecksum: nil,
            serverVersion: "delete-fence",
            authoredSchema: nil,
            currentSchema: nil,
            incompatibleFieldIDs: nil
        )
        _ = try processor.applyRejected(
            rejected: [rejected],
            syncedTables: [testTable],
            sentPending: [predecessor.mutationID: predecessor]
        )

        let row = try db.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["ship_address"] as String?, "later local")
        XCTAssertEqual(
            try db.readTransaction { try SynchroMeta.getRowVersion($0, tableName: "orders", recordID: "w1") },
            "delete-fence"
        )
        let blocked = try db.queryOne(
            "SELECT lifecycle_state, base_version FROM _synchro_pending_changes WHERE mutation_id = ?",
            params: [successorID]
        )
        XCTAssertEqual(blocked?["lifecycle_state"] as String?, "blocked_by_predecessor")
        XCTAssertNil(blocked?["base_version"] as String?)
    }

    func testApplyRejectedDoesNotTriggerCDC() throws {
        let (db, tracker, processor) = try makeTestEnv()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "Client Address", "u1", "2026-01-01T10:00:00.000Z"]
        )

        let serverRow = [
            "id": AnyCodable("w1"),
            "ship_address": AnyCodable("Server Address"),
            "user_id": AnyCodable("u1"),
            "updated_at": AnyCodable("2026-01-01T11:00:00.000000Z"),
            "deleted_at": AnyCodable(NSNull()),
        ]

        let rejected = [try makeRejectedMutation(
            mutationID: "m1",
            schema: testTable,
            pk: ["id": AnyCodable("w1")],
            status: .conflict,
            code: .versionConflict,
            message: "server version is newer",
            serverRow: serverRow,
            serverVersion: "2026-01-01T11:00:00.000Z"
        )]

        _ = try processor.applyRejected(rejected: rejected, syncedTables: [testTable])

        // sync_lock should have prevented CDC triggers from re-queuing
        XCTAssertFalse(try tracker.hasPendingChanges())
    }
}
