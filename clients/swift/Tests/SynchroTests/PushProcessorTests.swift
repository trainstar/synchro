import XCTest
import GRDB
import os
@testable import Synchro

final class PushProcessorTests: XCTestCase {
    private let testTable = SchemaTable(
        tableName: "orders",
        updatedAtColumn: "updated_at",
        deletedAtColumn: "deleted_at",
        primaryKey: ["id"],
        columns: [
            SchemaColumn(name: "id", logicalType: "string", nullable: false, isPrimaryKey: true),
            SchemaColumn(name: "ship_address", logicalType: "string", nullable: true, isPrimaryKey: false),
            SchemaColumn(name: "user_id", logicalType: "string", nullable: false, isPrimaryKey: false),
            SchemaColumn(name: "updated_at", logicalType: "datetime", nullable: false, isPrimaryKey: false),
            SchemaColumn(name: "deleted_at", logicalType: "datetime", nullable: true, isPrimaryKey: false),
        ]
    )

    private let customTable = SchemaTable(
        tableName: "custom_items",
        updatedAtColumn: "modified_at",
        deletedAtColumn: "removed_at",
        primaryKey: ["item_id"],
        columns: [
            SchemaColumn(name: "item_id", logicalType: "string", nullable: false, isPrimaryKey: true),
            SchemaColumn(name: "title", logicalType: "string", nullable: true, isPrimaryKey: false),
            SchemaColumn(name: "modified_at", logicalType: "datetime", nullable: false, isPrimaryKey: false),
            SchemaColumn(name: "removed_at", logicalType: "datetime", nullable: true, isPrimaryKey: false),
        ]
    )

    private let notesTable = SchemaTable(
        tableName: "notes",
        updatedAtColumn: "updated_at",
        deletedAtColumn: "deleted_at",
        primaryKey: ["id"],
        columns: [
            SchemaColumn(name: "id", logicalType: "string", nullable: false, isPrimaryKey: true),
            SchemaColumn(name: "body", logicalType: "string", nullable: true),
            SchemaColumn(name: "score", logicalType: "float", nullable: true),
            SchemaColumn(name: "updated_at", logicalType: "datetime", nullable: false),
            SchemaColumn(name: "deleted_at", logicalType: "datetime", nullable: true),
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

    func testPushRetryRejectsHistoricalSchemaConflictAfterResponseLoss() async throws {
        let path = (NSTemporaryDirectory() as NSString)
            .appendingPathComponent("synchro_history_\(UUID().uuidString).sqlite")
        let db = try SynchroDatabase(path: path)
        let schema = SchemaResponse(
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            serverTime: Date(),
            tables: [testTable, customTable]
        )
        let tables = try schema.localTables()
        try SchemaManager(database: db).createSyncedTables(schema: schema)
        let processor = PushProcessor(database: db, changeTracker: ChangeTracker(database: db))
        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?), (?, ?, ?, ?)",
            params: [
                "w1", "First", "u1", "2026-01-01T10:00:00.000Z",
                "w2", "Second", "u1", "2026-01-01T10:00:00.000Z",
            ]
        )
        _ = try db.execute(
            "INSERT INTO custom_items (item_id, title, modified_at) VALUES (?, ?, ?)",
            params: ["other-row", "Other table", "2026-01-01T10:00:00.000Z"]
        )
        let historyReads = OSAllocatedUnfairLock(initialState: [String]())
        try db.writeTransaction { connection in
            connection.trace { event in
                guard case let .statement(statement) = event else { return }
                let sql = statement.sql
                if sql.hasPrefix("SELECT request_json, schema_json FROM _synchro_push_batches")
                    || sql.hasPrefix("SELECT schema_json FROM _synchro_schema_archive") {
                    historyReads.withLock { $0.append(sql) }
                }
            }
        }
        defer {
            try? db.writeTransaction { $0.trace(nil) }
            try? db.close()
        }

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
        var requestCount = 0
        var requestBodies: [Data] = []
        MockURLProtocol.requestHandler = { request in
            requestCount += 1
            requestBodies.append(try XCTUnwrap(request.bodyData()))
            throw URLError(.networkConnectionLost)
        }

        var operationReads: [[String]] = []
        for _ in 0..<2 {
            historyReads.withLock { $0.removeAll() }
            do {
                _ = try await processor.processPush(
                    httpClient: httpClient,
                    clientID: "test-device",
                    clientGeneration: 1,
                    schemaVersion: 1,
                    schemaHash: protocolTestSchemaHash,
                    syncedTables: tables
                )
                XCTFail("expected response loss")
            } catch is RetryableError {
            }
            operationReads.append(historyReads.withLock { $0 })
        }
        XCTAssertEqual(requestCount, 2)
        XCTAssertEqual(requestBodies[0], requestBodies[1])
        let request = try JSONDecoder.synchroDecoder().decode(PushRequest.self, from: requestBodies[0])
        XCTAssertEqual(request.mutations.count, 3)
        XCTAssertEqual(Set(request.mutations.map(\.table)), Set(tables.map(\.tableID)))
        for reads in operationReads {
            XCTAssertEqual(reads.filter { $0.contains("FROM _synchro_push_batches") }.count, 1)
            XCTAssertEqual(reads.filter { $0.contains("FROM _synchro_schema_archive") }.count, 1)
        }

        let conflictingSchema = try JSONEncoder.synchroEncoder().encode([customTable])
        try db.writeTransaction { connection in
            try connection.execute(
                sql: """
                    UPDATE _synchro_schema_archive
                    SET schema_json = ?
                    WHERE schema_version = 1 AND schema_hash = ?
                    """,
                arguments: [String(decoding: conflictingSchema, as: UTF8.self), protocolTestSchemaHash]
            )
        }

        do {
            _ = try await processor.processPush(
                httpClient: httpClient,
                clientID: "test-device",
                clientGeneration: 1,
                schemaVersion: 1,
                schemaHash: protocolTestSchemaHash,
                syncedTables: tables
            )
            XCTFail("expected historical schema conflict")
        } catch SynchroError.invalidResponse {
        }
        XCTAssertEqual(requestCount, 2)
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

    func testBindingRenewalClearsOnlySupersededBatchBackoff() async throws {
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
        var serverGeneration = 2
        MockURLProtocol.requestHandler = { request in
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 409,
                httpVersion: nil,
                headerFields: ["Content-Type": "application/json"]
            )!
            let body = try JSONSerialization.data(withJSONObject: [
                "error": [
                    "code": "client_generation_expired",
                    "message": "generation expired",
                    "retryable": false,
                    "current_client_generation": serverGeneration,
                ] as [String: Any]
            ])
            return (response, body)
        }

        do {
            _ = try await processor.processPush(
                httpClient: httpClient,
                clientID: "test-device",
                clientGeneration: 1,
                schemaVersion: 1,
                schemaHash: protocolTestSchemaHash,
                syncedTables: [testTable.localSchema]
            )
            XCTFail("expected binding renewal")
        } catch is BindingRenewalError {
        }

        let oldBatchID = try XCTUnwrap(
            db.queryOne(
                "SELECT batch_id FROM _synchro_push_batches WHERE state = 'renewal_required'",
                params: nil
            )?["batch_id"] as String?
        )
        try db.writeTransaction { connection in
            try SynchroMeta.upsertBackoffRecord(
                connection,
                record: LocalBackoffRecord(
                    resumeState: .pushing,
                    workIdentity: oldBatchID,
                    retryClassification: .network,
                    attemptCount: 1,
                    nextRetryAtMS: 1
                )
            )
        }

        XCTAssertTrue(
            try processor.renewSealedBatchesAfterBindingChange(
                clientID: "test-device",
                clientGeneration: 2,
                schemaVersion: 1,
                schemaHash: protocolTestSchemaHash,
                syncedTables: [testTable.localSchema]
            )
        )
        XCTAssertNil(try db.readTransaction { try SynchroMeta.getBackoffRecord($0) })
        let states = try db.query("SELECT state FROM _synchro_push_batches", params: nil)
            .compactMap { $0["state"] as String? }
        XCTAssertEqual(Set(states), ["pending", "superseded"])

        serverGeneration = 3
        do {
            _ = try await processor.processPush(
                httpClient: httpClient,
                clientID: "test-device",
                clientGeneration: 2,
                schemaVersion: 1,
                schemaHash: protocolTestSchemaHash,
                syncedTables: [testTable.localSchema]
            )
            XCTFail("expected second binding renewal")
        } catch is BindingRenewalError {
        }
        try db.writeTransaction { connection in
            try SynchroMeta.upsertBackoffRecord(
                connection,
                record: LocalBackoffRecord(
                    resumeState: .pushing,
                    workIdentity: "unrelated-batch",
                    retryClassification: .network,
                    attemptCount: 1,
                    nextRetryAtMS: 1
                )
            )
        }

        XCTAssertTrue(
            try processor.renewSealedBatchesAfterBindingChange(
                clientID: "test-device",
                clientGeneration: 3,
                schemaVersion: 1,
                schemaHash: protocolTestSchemaHash,
                syncedTables: [testTable.localSchema]
            )
        )
        XCTAssertEqual(
            try db.readTransaction { try SynchroMeta.getBackoffRecord($0) }?.workIdentity,
            "unrelated-batch"
        )
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

    // MARK: - Push limits

    func testPushRequestOctetsComposeFromEnvelopeAndMutationElements() async throws {
        let (db, tracker, processor) = try makeTestEnv(table: notesTable)
        _ = try db.execute(
            "INSERT INTO notes (id, body, score, updated_at) VALUES (?, ?, ?, ?), (?, ?, ?, ?)",
            params: ["n2", "old", 1.0, "2026-01-01T10:00:00.000Z", "n3", "old", 2.0, "2026-01-01T10:00:00.000Z"]
        )
        try db.writeTransaction { connection in
            for recordID in ["n2", "n3"] {
                try SynchroMeta.upsertRowVersion(
                    connection,
                    tableName: "notes",
                    recordID: recordID,
                    serverVersion: "opaque-\(recordID)",
                    rowChecksum: nil
                )
            }
        }
        try tracker.clearAll()
        _ = try db.execute(
            "INSERT INTO notes (id, body, score, updated_at) VALUES (?, ?, ?, ?)",
            params: [
                "n1",
                "quote \" back \\ slash / tab \t line \n control \u{01} é 😀 \u{2028}",
                1.5e-7,
                "2026-01-01T10:00:00.000Z",
            ]
        )
        _ = try db.execute("UPDATE notes SET body = ?, score = ? WHERE id = ?", params: ["ünïcödé / ✓", 1e21, "n2"])
        _ = try db.execute("DELETE FROM notes WHERE id = ?", params: ["n3"])

        let body = try await sealBatchWithLostResponse(db, processor: processor, syncedTables: [notesTable])

        let encoder = JSONEncoder.synchroEncoder()
        let request = try JSONDecoder.synchroDecoder().decode(PushRequest.self, from: body)
        XCTAssertEqual(try encoder.encode(request), body)
        XCTAssertEqual(request.mutations.map(\.op), [.insert, .update, .delete])
        var envelope = request
        envelope.mutations = []
        var composed = try PushLimits.requestOctets(envelope, encoder: encoder)
        for (index, mutation) in request.mutations.enumerated() {
            composed = composed.appending(
                try PushLimits.measure(mutation, encoder: encoder).element,
                afterElement: index > 0
            )
        }
        let complete = try requestOctets(body)
        XCTAssertEqual(composed, complete)
        XCTAssertNotEqual(complete.body, complete.canonical)
        XCTAssertEqual(try PushLimits.measure(request.mutations[2], encoder: encoder).authoredColumns, 0)
    }

    func testSealSplitsPendingEntriesAtCanonicalRequestOctetLimit() async throws {
        let (db, _, processor) = try makeTestEnv(table: notesTable)
        let encoder = JSONEncoder.synchroEncoder()
        let decoder = JSONDecoder.synchroDecoder()
        let reserve = try PushLimits.envelopeReserve(
            clientID: "test-device",
            batchID: UUID().uuidString.lowercased(),
            schemaHash: protocolTestSchemaHash,
            encoder: encoder
        )
        // The body writes the score 1e20 as 1e+20 and RFC 8785 writes all 21 digits.
        // All rows fit in one request by body octets but not by canonical octets.
        let emptyInsert = try PushLimits.measure(notesInsert(recordID: "n00", body: ""), encoder: encoder)
        let maxText = PushLimits.maxNormalizedMutationOctets - emptyInsert.normalizedJSON.count
        let available = PushLimits.maxRequestOctets - reserve.body + 1
        let elementStep = emptyInsert.element.body + 1
        let count = (available + elementStep + maxText - 1) / (elementStep + maxText)
        let totalText = available - count * elementStep
        for index in 0..<count {
            let length = totalText / count + (index < totalText % count ? 1 : 0)
            _ = try db.execute(
                "INSERT INTO notes (id, body, score, updated_at) VALUES (?, ?, ?, ?)",
                params: [
                    String(format: "n%02d", index),
                    String(repeating: "a", count: length),
                    1e20,
                    "2026-01-01T10:00:00.000Z",
                ]
            )
        }
        let localOrder = try db.query(
            "SELECT mutation_id FROM _synchro_pending_changes ORDER BY local_order",
            params: nil
        ).map { $0["mutation_id"] as String }
        XCTAssertEqual(localOrder.count, count)

        let (httpClient, session) = makeMockPushClient(dbPath: db.path)
        defer {
            MockURLProtocol.requestHandler = nil
            session.invalidateAndCancel()
        }
        var bodies: [Data] = []
        MockURLProtocol.requestHandler = { [notesTable] request in
            let body = try XCTUnwrap(request.bodyData())
            bodies.append(body)
            let pushRequest = try decoder.decode(PushRequest.self, from: body)
            let accepted = try pushRequest.mutations.map { mutation in
                var serverRow = try XCTUnwrap(mutation.columns)
                serverRow["id"] = try XCTUnwrap(mutation.pk["id"])
                serverRow["updated_at"] = AnyCodable("2026-01-01T11:00:00.000000Z")
                serverRow["deleted_at"] = AnyCodable(NSNull())
                return try makeAcceptedMutation(
                    mutationID: mutation.mutationID,
                    schema: notesTable,
                    pk: mutation.pk,
                    status: .applied,
                    serverRow: serverRow,
                    serverVersion: "opaque-\(mutation.mutationID)"
                )
            }
            let response = PushResponse(
                batchID: pushRequest.batchID,
                serverTime: "2026-01-01T11:00:00.000000Z",
                accepted: accepted,
                rejected: []
            )
            let httpResponse = HTTPURLResponse(
                url: request.url!,
                statusCode: 200,
                httpVersion: nil,
                headerFields: ["Content-Type": "application/json"]
            )!
            // The client accepts only canonical numbers in a response.
            return (httpResponse, try PushLimits.canonicalJSON(encoder.encode(response)))
        }
        for _ in localOrder {
            let outcome = try await processor.processPush(
                httpClient: httpClient,
                clientID: "test-device",
                clientGeneration: 1,
                schemaVersion: 1,
                schemaHash: protocolTestSchemaHash,
                syncedTables: [notesTable]
            )
            if outcome == nil { break }
        }

        XCTAssertGreaterThanOrEqual(bodies.count, 2)
        var sealedIDs: [String] = []
        var singleRequest = reserve
        for body in bodies {
            let octets = try requestOctets(body)
            XCTAssertLessThanOrEqual(octets.body, PushLimits.maxRequestOctets)
            XCTAssertLessThanOrEqual(octets.canonical, PushLimits.maxRequestOctets)
            for mutation in try decoder.decode(PushRequest.self, from: body).mutations {
                singleRequest = singleRequest.appending(
                    try PushLimits.measure(mutation, encoder: encoder).element,
                    afterElement: !sealedIDs.isEmpty
                )
                sealedIDs.append(mutation.mutationID)
            }
        }
        XCTAssertLessThanOrEqual(singleRequest.body, PushLimits.maxRequestOctets)
        XCTAssertGreaterThan(singleRequest.canonical, PushLimits.maxRequestOctets)
        XCTAssertEqual(sealedIDs, localOrder)
        XCTAssertEqual(
            try db.queryOne("SELECT COUNT(*) AS count FROM _synchro_push_batch_members", params: nil)?["count"] as Int?,
            localOrder.count
        )
        XCTAssertEqual(
            try db.queryOne(
                "SELECT COUNT(*) AS count FROM _synchro_pending_changes WHERE lifecycle_state = 'accepted'",
                params: nil
            )?["count"] as Int?,
            localOrder.count
        )
    }

    func testSealMovesOversizeMutationToLocalTerminalStateAndSealsLaterRow() async throws {
        let (db, tracker, processor) = try makeTestEnv()
        let encoder = JSONEncoder.synchroEncoder()
        try insertOrder(db, id: "w-fit", address: ordersAddress(recordID: "w-fit", normalizedOctets: 65_536))
        let oversizeAddress = try ordersAddress(recordID: "w-big", normalizedOctets: 65_537)
        try insertOrder(db, id: "w-big", address: oversizeAddress)
        try insertOrder(db, id: "w-end", address: "later row")
        _ = try db.execute(
            "UPDATE orders SET deleted_at = ? WHERE id = ?",
            params: ["2026-01-01T10:30:00.000Z", "w-big"]
        )
        _ = try db.execute("UPDATE orders SET ship_address = ? WHERE id = ?", params: ["after delete", "w-big"])
        let fitID = try mutationID(db, recordID: "w-fit", operation: "insert")
        let oversizeID = try mutationID(db, recordID: "w-big", operation: "insert")
        let laterID = try mutationID(db, recordID: "w-end", operation: "insert")
        let dependentIDs = try db.query(
            "SELECT mutation_id FROM _synchro_pending_changes WHERE record_id = ? AND operation <> 'insert'",
            params: ["w-big"]
        ).map { $0["mutation_id"] as String }
        XCTAssertEqual(dependentIDs.count, 2)

        let request = try JSONDecoder.synchroDecoder().decode(
            PushRequest.self,
            from: try await sealBatchWithLostResponse(db, processor: processor, syncedTables: [testTable])
        )

        XCTAssertEqual(request.mutations.map(\.mutationID), [fitID, laterID])
        XCTAssertEqual(try PushLimits.measure(request.mutations[0], encoder: encoder).normalizedJSON.count, 65_536)
        XCTAssertEqual(try lifecycleState(db, mutationID: oversizeID), "exceeds_push_limit")
        for dependentID in dependentIDs {
            XCTAssertEqual(try lifecycleState(db, mutationID: dependentID), "blocked_by_predecessor")
        }
        let retained = try XCTUnwrap(tracker.inspectRetainedMutations().first { $0.mutationID == oversizeID })
        XCTAssertEqual(retained.status, .exceedsPushLimit)
        XCTAssertEqual(
            retained.authoredFields.first { $0.fieldID == "ship_address" }?.value,
            AnyCodable(oversizeAddress)
        )
        let retainedMutation = Mutation(
            mutationID: retained.mutationID,
            table: retained.tableID,
            op: retained.operation,
            pk: [retained.primaryKeyFieldID: AnyCodable(retained.recordID)],
            authoredSchema: retained.authoredSchema,
            baseVersion: retained.baseVersion,
            clientVersion: retained.clientVersion,
            columns: Dictionary(uniqueKeysWithValues: retained.authoredFields.map { ($0.fieldID, $0.value) })
        )
        XCTAssertEqual(try PushLimits.measure(retainedMutation, encoder: encoder).normalizedJSON.count, 65_537)
        XCTAssertFalse(try tracker.inspectPendingMutations().contains { $0.mutationID == oversizeID })
        XCTAssertTrue(try db.readTransaction { try SynchroMeta.listRejectedMutations($0) }.isEmpty)
        XCTAssertNil(try db.queryOne(
            "SELECT batch_id FROM _synchro_push_batch_members WHERE mutation_id = ?",
            params: [oversizeID]
        ))
    }

    func testSealMovesMutationAboveAuthoredColumnLimitToLocalTerminalState() async throws {
        let authoredColumns = (0...PushLimits.maxAuthoredColumns).map { SchemaColumn(name: String(format: "c%03d", $0)) }
        let wideTable = SchemaTable(
            tableName: "wide_items",
            updatedAtColumn: "updated_at",
            deletedAtColumn: "deleted_at",
            primaryKey: ["id"],
            columns: [SchemaColumn(name: "id", nullable: false, isPrimaryKey: true)]
                + authoredColumns
                + [
                    SchemaColumn(name: "updated_at", logicalType: "datetime", nullable: false),
                    SchemaColumn(name: "deleted_at", logicalType: "datetime", nullable: true),
                ]
        )
        let (db, _, processor) = try makeTestEnv(table: wideTable)
        func insert(id: String, columns: ArraySlice<SchemaColumn>) throws {
            let names = ["id"] + columns.map(\.name) + ["updated_at"]
            let values = [id] + columns.map { _ in "v" } + ["2026-01-01T10:00:00.000Z"]
            _ = try db.execute(
                "INSERT INTO wide_items (\(names.joined(separator: ", "))) VALUES (\(names.map { _ in "?" }.joined(separator: ", ")))",
                params: values
            )
        }
        try insert(id: "w257", columns: authoredColumns[...])
        try insert(id: "w256", columns: authoredColumns.dropLast())
        let overLimitID = try mutationID(db, recordID: "w257", operation: "insert")
        let atLimitID = try mutationID(db, recordID: "w256", operation: "insert")

        let request = try JSONDecoder.synchroDecoder().decode(
            PushRequest.self,
            from: try await sealBatchWithLostResponse(db, processor: processor, syncedTables: [wideTable])
        )

        XCTAssertEqual(request.mutations.map(\.mutationID), [atLimitID])
        XCTAssertEqual(request.mutations[0].columns?.count, PushLimits.maxAuthoredColumns)
        XCTAssertEqual(try lifecycleState(db, mutationID: overLimitID), "exceeds_push_limit")
    }

    func testSealReadsNextCandidatesWhenEveryCandidateExceedsLimits() async throws {
        let (db, _, processor) = try makeTestEnv()
        let oversizeAddress = String(repeating: "a", count: PushLimits.maxNormalizedMutationOctets)
        try insertOrder(db, id: "w1", address: oversizeAddress)
        try insertOrder(db, id: "w2", address: oversizeAddress)
        try insertOrder(db, id: "w3", address: "fits")
        let oversizeIDs = try ["w1", "w2"].map { try mutationID(db, recordID: $0, operation: "insert") }
        let fitID = try mutationID(db, recordID: "w3", operation: "insert")

        let request = try JSONDecoder.synchroDecoder().decode(
            PushRequest.self,
            from: try await sealBatchWithLostResponse(
                db,
                processor: processor,
                syncedTables: [testTable],
                batchSize: oversizeIDs.count
            )
        )

        XCTAssertEqual(request.mutations.map(\.mutationID), [fitID])
        for oversizeID in oversizeIDs {
            XCTAssertEqual(try lifecycleState(db, mutationID: oversizeID), "exceeds_push_limit")
        }
    }

    func testBatchSealedNearRequestLimitRenewsWithinLimitsAtMaximumGeneration() async throws {
        let (db, _, processor) = try makeTestEnv()
        let encoder = JSONEncoder.synchroEncoder()
        let decoder = JSONDecoder.synchroDecoder()
        let envelope = try PushLimits.requestOctets(
            PushRequest(
                clientID: "test-device",
                clientGeneration: 1,
                batchID: UUID().uuidString.lowercased(),
                schema: SchemaRef(version: 1, hash: protocolTestSchemaHash),
                mutations: []
            ),
            encoder: encoder
        )
        let emptyInsert = try PushLimits.measure(ordersInsert(recordID: "r00", address: ""), encoder: encoder)
        XCTAssertEqual(envelope.body, envelope.canonical)
        XCTAssertEqual(emptyInsert.element.body, emptyInsert.element.canonical)
        // All rows in one generation 1 request give exactly the request limit.
        // Each element is the empty insert element plus its address length.
        let maxAddress = PushLimits.maxNormalizedMutationOctets - emptyInsert.normalizedJSON.count
        let available = PushLimits.maxRequestOctets - envelope.body + 1
        let elementStep = emptyInsert.element.body + 1
        let count = (available + elementStep + maxAddress - 1) / (elementStep + maxAddress)
        let totalAddress = available - count * elementStep
        XCTAssertLessThanOrEqual(count, 100)
        var lengths: [Int] = []
        for index in 0..<count {
            lengths.append(totalAddress / count + (index < totalAddress % count ? 1 : 0))
            try insertOrder(db, id: String(format: "r%02d", index), address: String(repeating: "a", count: lengths[index]))
        }
        let lastID = try mutationID(db, recordID: String(format: "r%02d", count - 1), operation: "insert")

        let (httpClient, session) = makeMockPushClient(dbPath: db.path)
        defer {
            MockURLProtocol.requestHandler = nil
            session.invalidateAndCancel()
        }
        MockURLProtocol.requestHandler = { request in
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 409,
                httpVersion: nil,
                headerFields: ["Content-Type": "application/json"]
            )!
            let body = try JSONSerialization.data(withJSONObject: [
                "error": [
                    "code": "client_generation_expired",
                    "message": "generation expired",
                    "retryable": false,
                    "current_client_generation": PushLimits.maxProtocolInteger,
                ] as [String: Any]
            ])
            return (response, body)
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
            XCTFail("expected binding renewal")
        } catch is BindingRenewalError {
        }

        // The envelope reserve keeps the last row out of the generation 1 batch.
        let sealedJSON = try batchJSON(db, state: "renewal_required")
        let sealed = try decoder.decode(PushRequest.self, from: sealedJSON)
        XCTAssertEqual(sealed.mutations.count, count - 1)
        XCTAssertEqual(try lifecycleState(db, mutationID: lastID), "unsealed")
        XCTAssertEqual(
            try requestOctets(sealedJSON).body + 1 + emptyInsert.element.body + lengths[count - 1],
            PushLimits.maxRequestOctets
        )

        XCTAssertTrue(
            try processor.renewSealedBatchesAfterBindingChange(
                clientID: "test-device",
                clientGeneration: PushLimits.maxProtocolInteger,
                schemaVersion: 1,
                schemaHash: protocolTestSchemaHash,
                syncedTables: [testTable]
            )
        )
        let successorJSON = try batchJSON(db, state: "pending")
        let successor = try decoder.decode(PushRequest.self, from: successorJSON)
        XCTAssertEqual(successor.clientGeneration, PushLimits.maxProtocolInteger)
        XCTAssertEqual(successor.mutations, sealed.mutations)
        let successorOctets = try requestOctets(successorJSON)
        XCTAssertLessThanOrEqual(successorOctets.body, PushLimits.maxRequestOctets)
        XCTAssertLessThanOrEqual(successorOctets.canonical, PushLimits.maxRequestOctets)
    }

    func testInsertAtNormalizedLimitWithOnlySlashTextSealsAlone() async throws {
        let (db, _, processor) = try makeTestEnv()
        let address = try ordersAddress(recordID: "w1", normalizedOctets: 65_536, fill: "/")
        try insertOrder(db, id: "w1", address: address)
        let insertID = try mutationID(db, recordID: "w1", operation: "insert")

        let body = try await sealBatchWithLostResponse(db, processor: processor, syncedTables: [testTable])

        let request = try JSONDecoder.synchroDecoder().decode(PushRequest.self, from: body)
        XCTAssertEqual(request.mutations.map(\.mutationID), [insertID])
        let measure = try PushLimits.measure(request.mutations[0], encoder: JSONEncoder.synchroEncoder())
        XCTAssertEqual(measure.normalizedJSON.count, 65_536)
        XCTAssertGreaterThan(measure.element.body, 2 * address.utf8.count)
        let octets = try requestOctets(body)
        XCTAssertLessThan(octets.body, PushLimits.maxRequestOctets)
        XCTAssertLessThan(octets.canonical, PushLimits.maxRequestOctets)
    }

    /// A client identity near the request limit is the only way that an individually valid
    /// mutation cannot fit alone. The same queue fits at the limit and fails one octet above it.
    func testFirstCandidateMustFitBothRequestMeasuresWithTheReservedEnvelope() async throws {
        let requestLimit = 1_048_576
        // Slash text writes two body octets for each character. 1e20 writes 16 more canonical octets.
        let cases: [(dominant: KeyPath<PushLimits.Octets, Int>, other: KeyPath<PushLimits.Octets, Int>, text: String, score: Double)] = [
            (\.body, \.canonical, String(repeating: "/", count: 60_000), 1.5),
            (\.canonical, \.body, String(repeating: "a", count: 60_000), 1e20),
        ]
        for testCase in cases {
            let (probeDB, _, probeProcessor) = try makeTestEnv(table: notesTable)
            defer { closeAndRemove(probeDB) }
            try insertNote(probeDB, id: "n-big", body: testCase.text, score: testCase.score)
            let probe = try JSONDecoder.synchroDecoder().decode(
                PushRequest.self,
                from: try await sealBatchWithLostResponse(probeDB, processor: probeProcessor, syncedTables: [notesTable])
            )
            let base = try reservedRequestOctets(probe, clientID: "")
            XCTAssertGreaterThan(base[keyPath: testCase.dominant], base[keyPath: testCase.other])

            for extra in [0, 1] {
                let clientID = String(repeating: "c", count: requestLimit - base[keyPath: testCase.dominant] + extra)
                let (db, tracker, processor) = try makeTestEnv(table: notesTable)
                defer { closeAndRemove(db) }
                try insertNote(db, id: "n-big", body: testCase.text, score: testCase.score)
                // A soft delete and a later update depend on the large insert.
                _ = try db.execute(
                    "UPDATE notes SET deleted_at = ? WHERE id = ?",
                    params: ["2026-01-01T10:30:00.000Z", "n-big"]
                )
                _ = try db.execute("UPDATE notes SET body = ? WHERE id = ?", params: ["after delete", "n-big"])
                try insertNote(db, id: "n-small", body: "small", score: 1.5)
                let bigID = try mutationID(db, recordID: "n-big", operation: "insert")
                let smallID = try mutationID(db, recordID: "n-small", operation: "insert")
                let dependentIDs = try db.query(
                    "SELECT mutation_id FROM _synchro_pending_changes WHERE record_id = ? AND operation <> 'insert'",
                    params: ["n-big"]
                ).map { $0["mutation_id"] as String }
                XCTAssertEqual(dependentIDs.count, 2)

                let request = try JSONDecoder.synchroDecoder().decode(
                    PushRequest.self,
                    from: try await sealBatchWithLostResponse(
                        db,
                        processor: processor,
                        syncedTables: [notesTable],
                        clientID: clientID
                    )
                )

                if extra == 0 {
                    XCTAssertEqual(request.mutations.map(\.mutationID), [bigID])
                    let sealed = try reservedRequestOctets(request, clientID: clientID)
                    XCTAssertEqual(sealed[keyPath: testCase.dominant], requestLimit)
                    XCTAssertLessThan(sealed[keyPath: testCase.other], requestLimit)
                    XCTAssertEqual(try lifecycleState(db, mutationID: smallID), "unsealed")
                    continue
                }
                XCTAssertEqual(request.mutations.map(\.mutationID), [smallID])
                XCTAssertEqual(try lifecycleState(db, mutationID: bigID), "exceeds_push_limit")
                for dependentID in dependentIDs {
                    XCTAssertEqual(try lifecycleState(db, mutationID: dependentID), "blocked_by_predecessor")
                }
                XCTAssertNil(try db.queryOne(
                    "SELECT batch_id FROM _synchro_push_batch_members WHERE mutation_id = ?",
                    params: [bigID]
                ))
                let retained = try XCTUnwrap(tracker.inspectRetainedMutations().first { $0.mutationID == bigID })
                XCTAssertEqual(retained.status, .exceedsPushLimit)
                XCTAssertEqual(retained.authoredFields.first { $0.fieldID == "body" }?.value, AnyCodable(testCase.text))
                XCTAssertEqual(retained.authoredFields.first { $0.fieldID == "score" }?.value, AnyCodable(testCase.score))
                let retainedMutation = Mutation(
                    mutationID: retained.mutationID,
                    table: retained.tableID,
                    op: retained.operation,
                    pk: [retained.primaryKeyFieldID: AnyCodable(retained.recordID)],
                    authoredSchema: retained.authoredSchema,
                    baseVersion: retained.baseVersion,
                    clientVersion: retained.clientVersion,
                    columns: Dictionary(uniqueKeysWithValues: retained.authoredFields.map { ($0.fieldID, $0.value) })
                )
                let measure = try PushLimits.measure(retainedMutation, encoder: JSONEncoder.synchroEncoder())
                XCTAssertLessThanOrEqual(measure.normalizedJSON.count, 65_536)
                XCTAssertLessThanOrEqual(measure.authoredColumns, 256)
                var alone = request
                alone.mutations = [retainedMutation]
                let aloneOctets = try reservedRequestOctets(alone, clientID: clientID)
                XCTAssertEqual(aloneOctets[keyPath: testCase.dominant], requestLimit + 1)
                XCTAssertLessThanOrEqual(aloneOctets[keyPath: testCase.other], requestLimit)
            }
        }
    }

    /// An envelope exactly at the request limit does not exceed it. No mutation element fits with it,
    /// so each candidate follows the singleton terminal policy and no request is sent.
    func testEnvelopeExactlyAtTheRequestLimitLeavesEachCandidateIndividuallyUnsendable() async throws {
        let empty = PushRequest(
            clientID: "",
            clientGeneration: 1,
            batchID: UUID().uuidString.lowercased(),
            schema: SchemaRef(version: 1, hash: protocolTestSchemaHash),
            mutations: []
        )
        let base = try reservedRequestOctets(empty, clientID: "")
        XCTAssertEqual(base.body, base.canonical)
        let clientID = String(repeating: "c", count: 1_048_576 - base.body)
        XCTAssertEqual(
            try reservedRequestOctets(empty, clientID: clientID),
            PushLimits.Octets(body: 1_048_576, canonical: 1_048_576)
        )
        let (db, tracker, processor) = try makeTestEnv(table: notesTable)
        defer { closeAndRemove(db) }
        try insertNote(db, id: "n1", body: "x", score: 1.5)
        let insertID = try mutationID(db, recordID: "n1", operation: "insert")
        let (httpClient, session) = makeMockPushClient(dbPath: db.path)
        defer {
            MockURLProtocol.requestHandler = nil
            session.invalidateAndCancel()
        }
        var requestCount = 0
        MockURLProtocol.requestHandler = { _ in
            requestCount += 1
            throw URLError(.networkConnectionLost)
        }

        var outcome: PushProcessor.PushOutcome?
        var pushError: Error?
        do {
            outcome = try await processor.processPush(
                httpClient: httpClient,
                clientID: clientID,
                clientGeneration: 1,
                schemaVersion: 1,
                schemaHash: protocolTestSchemaHash,
                syncedTables: [notesTable]
            )
        } catch {
            pushError = error
        }

        XCTAssertEqual(requestCount, 0)
        XCTAssertNil(pushError)
        XCTAssertNil(outcome)
        XCTAssertEqual(try lifecycleState(db, mutationID: insertID), "exceeds_push_limit")
        XCTAssertEqual(
            try tracker.inspectRetainedMutations().first { $0.mutationID == insertID }?.status,
            .exceedsPushLimit
        )
        XCTAssertNil(try db.queryOne("SELECT batch_id FROM _synchro_push_batches", params: nil))
    }

    /// The fixed-envelope failure rolls back the seal transaction, so every action,
    /// authored value, and dependency keeps its state.
    func testOversizedEnvelopeFailsBeforeSealingAndKeepsEveryAction() async throws {
        let empty = PushRequest(
            clientID: "",
            clientGeneration: 1,
            batchID: UUID().uuidString.lowercased(),
            schema: SchemaRef(version: 1, hash: protocolTestSchemaHash),
            mutations: []
        )
        let base = try reservedRequestOctets(empty, clientID: "")
        let clientID = String(repeating: "c", count: 1_048_577 - base.body)
        XCTAssertEqual(
            try reservedRequestOctets(empty, clientID: clientID),
            PushLimits.Octets(body: 1_048_577, canonical: 1_048_577)
        )
        let (db, _, processor) = try makeTestEnv(table: notesTable)
        defer { closeAndRemove(db) }
        try insertNote(db, id: "n1", body: "first", score: 1.5)
        // A soft delete and a later update depend on the insert.
        _ = try db.execute("UPDATE notes SET deleted_at = ? WHERE id = ?", params: ["2026-01-01T10:30:00.000Z", "n1"])
        _ = try db.execute("UPDATE notes SET body = ? WHERE id = ?", params: ["after delete", "n1"])
        try insertNote(db, id: "n2", body: "unrelated", score: 1.5)
        let before = try durableActionRows(db)
        XCTAssertEqual(before[0].count, 4)
        let (httpClient, session) = makeMockPushClient(dbPath: db.path)
        defer {
            MockURLProtocol.requestHandler = nil
            session.invalidateAndCancel()
        }
        var requestCount = 0
        MockURLProtocol.requestHandler = { _ in
            requestCount += 1
            throw URLError(.networkConnectionLost)
        }

        var thrown: Error?
        do {
            _ = try await processor.processPush(
                httpClient: httpClient,
                clientID: clientID,
                clientGeneration: 1,
                schemaVersion: 1,
                schemaHash: protocolTestSchemaHash,
                syncedTables: [notesTable]
            )
        } catch {
            thrown = error
        }

        XCTAssertEqual(requestCount, 0)
        guard let synchroError = thrown as? SynchroError, case let .blocked(failure) = synchroError else {
            XCTFail("processPush result: \(String(describing: thrown))")
            return
        }
        XCTAssertEqual(failure.operation, .pushing)
        XCTAssertEqual(failure.code, .invalidRequest)
        XCTAssertFalse(failure.retryable)
        XCTAssertEqual(failure.recoveryAction, .none)
        XCTAssertTrue(failure.metadata.isEmpty)
        XCTAssertEqual(try durableActionRows(db), before)
    }

    private func makeMockPushClient(dbPath: String) -> (HttpClient, URLSession) {
        let sessionConfiguration = URLSessionConfiguration.ephemeral
        sessionConfiguration.protocolClasses = [MockURLProtocol.self]
        let session = URLSession(configuration: sessionConfiguration)
        let config = SynchroConfig(
            dbPath: dbPath,
            serverURL: URL(string: "http://test.local")!,
            authProvider: { "test-token" },
            clientID: "test-device",
            appVersion: "1.0.0"
        )
        return (HttpClient(config: config, session: session), session)
    }

    /// Seals one batch through the production push path and keeps it pending.
    private func sealBatchWithLostResponse(
        _ db: SynchroDatabase,
        processor: PushProcessor,
        syncedTables: [LocalSchemaTable],
        batchSize: Int = 100,
        clientID: String = "test-device"
    ) async throws -> Data {
        let (httpClient, session) = makeMockPushClient(dbPath: db.path)
        defer {
            MockURLProtocol.requestHandler = nil
            session.invalidateAndCancel()
        }
        var sentBody: Data?
        MockURLProtocol.requestHandler = { request in
            sentBody = request.bodyData()
            throw URLError(.networkConnectionLost)
        }
        do {
            _ = try await processor.processPush(
                httpClient: httpClient,
                clientID: clientID,
                clientGeneration: 1,
                schemaVersion: 1,
                schemaHash: protocolTestSchemaHash,
                syncedTables: syncedTables,
                batchSize: batchSize
            )
            XCTFail("expected response loss")
        } catch is RetryableError {
        }
        let stored = try batchJSON(db, state: "pending")
        XCTAssertEqual(sentBody, stored)
        return stored
    }

    private func batchJSON(_ db: SynchroDatabase, state: String) throws -> Data {
        let requestJSON = try XCTUnwrap(
            db.queryOne(
                "SELECT request_json FROM _synchro_push_batches WHERE state = ?",
                params: [state]
            )?["request_json"] as String?
        )
        return Data(requestJSON.utf8)
    }

    private func requestOctets(_ json: Data) throws -> PushLimits.Octets {
        PushLimits.Octets(body: json.count, canonical: try PushLimits.canonicalJSON(json).count)
    }

    /// Measures the complete request with the largest generation and schema version that a successor can carry.
    private func reservedRequestOctets(_ request: PushRequest, clientID: String) throws -> PushLimits.Octets {
        var reserved = request
        reserved.clientID = clientID
        reserved.clientGeneration = 9_007_199_254_740_991
        reserved.schema.version = 9_007_199_254_740_991
        return try requestOctets(JSONEncoder.synchroEncoder().encode(reserved))
    }

    /// Closes a database that this test acquired and removes its file and sidecars.
    /// Each failure fails the test. A database that did not close keeps its files.
    private func closeAndRemove(_ db: SynchroDatabase, file: StaticString = #filePath, line: UInt = #line) {
        let path = db.path
        do {
            try db.close()
        } catch {
            XCTFail("closing test database failed: \(error)", file: file, line: line)
            return
        }
        for suffix in ["", "-journal", "-wal", "-shm"] where FileManager.default.fileExists(atPath: path + suffix) {
            do {
                try FileManager.default.removeItem(atPath: path + suffix)
            } catch {
                XCTFail("removing test database file failed: \(error)", file: file, line: line)
            }
        }
    }

    private func insertNote(_ db: SynchroDatabase, id: String, body: String, score: Double) throws {
        _ = try db.execute(
            "INSERT INTO notes (id, body, score, updated_at) VALUES (?, ?, ?, ?)",
            params: [id, body, score, "2026-01-01T10:00:00.000Z"]
        )
    }

    private func ordersInsert(recordID: String, address: String) -> Mutation {
        Mutation(
            mutationID: UUID().uuidString.lowercased(),
            table: testTable.tableID,
            op: .insert,
            pk: ["id": AnyCodable(recordID)],
            authoredSchema: SchemaRef(version: 1, hash: protocolTestSchemaHash),
            baseVersion: nil,
            clientVersion: "2026-01-01T10:00:00.000000Z",
            columns: ["ship_address": AnyCodable(address), "user_id": AnyCodable("u1")]
        )
    }

    private func notesInsert(recordID: String, body: String) -> Mutation {
        Mutation(
            mutationID: UUID().uuidString.lowercased(),
            table: notesTable.tableID,
            op: .insert,
            pk: ["id": AnyCodable(recordID)],
            authoredSchema: SchemaRef(version: 1, hash: protocolTestSchemaHash),
            baseVersion: nil,
            clientVersion: "2026-01-01T10:00:00.000000Z",
            columns: ["body": AnyCodable(body), "score": AnyCodable(1e20)]
        )
    }

    /// Gives an address that makes a captured `orders` insert have exactly the given normalized octets.
    private func ordersAddress(recordID: String, normalizedOctets: Int, fill: Character = "a") throws -> String {
        let empty = try PushLimits.measure(
            ordersInsert(recordID: recordID, address: ""),
            encoder: JSONEncoder.synchroEncoder()
        )
        return String(repeating: fill, count: normalizedOctets - empty.normalizedJSON.count)
    }

    private func insertOrder(_ db: SynchroDatabase, id: String, address: String) throws {
        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: [id, address, "u1", "2026-01-01T10:00:00.000Z"]
        )
    }

    private func mutationID(_ db: SynchroDatabase, recordID: String, operation: String) throws -> String {
        try XCTUnwrap(
            db.queryOne(
                "SELECT mutation_id FROM _synchro_pending_changes WHERE record_id = ? AND operation = ?",
                params: [recordID, operation]
            )?["mutation_id"] as String?
        )
    }

    private func lifecycleState(_ db: SynchroDatabase, mutationID: String) throws -> String? {
        try db.queryOne(
            "SELECT lifecycle_state FROM _synchro_pending_changes WHERE mutation_id = ?",
            params: [mutationID]
        )?["lifecycle_state"] as String?
    }
}

/// Returns every durable action, authored value, sealed batch, and batch member row in key order.
func durableActionRows(_ db: SynchroDatabase) throws -> [[Row]] {
    try db.readTransaction { connection in
        try [
            "SELECT * FROM _synchro_pending_changes ORDER BY mutation_id",
            "SELECT * FROM _synchro_mutation_values ORDER BY mutation_id, field_id",
            "SELECT * FROM _synchro_push_batches ORDER BY batch_id",
            "SELECT * FROM _synchro_push_batch_members ORDER BY batch_id, ordinal",
        ].map { try Row.fetchAll(connection, sql: $0) }
    }
}
