import XCTest
import GRDB
@testable import Synchro

final class SyncEngineTests: XCTestCase {
    func testCallbackRegistrationAndCancellation() throws {
        let (engine, _) = try makeSyncEngine()

        var statusUpdates: [String] = []
        let cancellable1 = engine.onStatusChange { status in
            switch status {
            case .idle: statusUpdates.append("idle")
            case .syncing: statusUpdates.append("syncing")
            case .error: statusUpdates.append("error")
            case .stopped: statusUpdates.append("stopped")
            }
        }

        var conflictEvents: [String] = []
        let cancellable2 = engine.onConflict { event in
            conflictEvents.append(event.recordID)
        }

        // Stop triggers a status update
        engine.stop()
        XCTAssertEqual(statusUpdates, ["stopped"])

        // Cancel callbacks
        cancellable1.cancel()
        cancellable2.cancel()

        // After cancel, no more updates
        statusUpdates.removeAll()
        engine.stop()
        XCTAssertTrue(statusUpdates.isEmpty)
    }

    func testMultipleCallbacksIndependentCancellation() throws {
        let (engine, _) = try makeSyncEngine()

        var updates1: [String] = []
        var updates2: [String] = []

        let cancellable1 = engine.onStatusChange { _ in
            updates1.append("hit")
        }
        let _ = engine.onStatusChange { _ in
            updates2.append("hit")
        }

        engine.stop()
        XCTAssertEqual(updates1.count, 1)
        XCTAssertEqual(updates2.count, 1)

        // Cancel only first
        cancellable1.cancel()
        updates1.removeAll()
        updates2.removeAll()

        engine.stop()
        XCTAssertEqual(updates1.count, 0, "Cancelled callback should not fire")
        XCTAssertEqual(updates2.count, 1, "Uncancelled callback should still fire")
    }

    // MARK: - Behavioral Sync Tests

    override func tearDown() {
        MockURLProtocol.requestHandler = nil
        super.tearDown()
    }

    func testStartInitializesAndSyncs() async throws {
        var callLog: [String] = []

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/schema") {
                callLog.append("schema")
                return try self.mockResponse(json: self.schemaJSON)
            } else if path.hasSuffix("/sync/register") {
                callLog.append("register")
                return try self.mockResponse(json: self.registerJSON)
            } else if path.hasSuffix("/sync/snapshot") {
                callLog.append("snapshot")
                return try self.mockResponse(json: self.snapshotJSON(checkpoint: 0, records: []))
            } else if path.hasSuffix("/sync/pull") {
                callLog.append("pull")
                return try self.mockResponse(json: self.pullJSON(checkpoint: 10))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected: \(path)"])
        }

        let (engine, db) = try makeIntegrationEnv()
        defer { engine.stop() }

        try await engine.start()

        // Schema fetched, client registered, initial pull completed (no push — no pending)
        XCTAssertEqual(callLog, ["schema", "register", "snapshot", "pull"])

        // Checkpoint advanced from pull
        let checkpoint = try db.readTransaction { db in try SynchroMeta.getInt64(db, key: .checkpoint) }
        XCTAssertEqual(checkpoint, 10)

        // Synced tables were created
        let tables = try db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'", params: nil)
        XCTAssertEqual(tables.count, 1)

        // CDC triggers were created
        let triggers = try db.query("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_synchro_cdc_%orders'", params: nil)
        XCTAssertEqual(triggers.count, 3)
    }

    func testPushAcceptedAppliesRYOW() async throws {
        var pushCalled = false

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/schema") {
                return try self.mockResponse(json: self.schemaJSON)
            } else if path.hasSuffix("/sync/register") {
                return try self.mockResponse(json: self.registerJSON)
            } else if path.hasSuffix("/sync/snapshot") {
                return try self.mockResponse(json: self.snapshotJSON(checkpoint: 0, records: []))
            } else if path.hasSuffix("/sync/push") {
                pushCalled = true
                // Accept the pushed record with a server timestamp
                let body = try JSONSerialization.jsonObject(with: request.bodyData()!) as! [String: Any]
                let changes = body["changes"] as! [[String: Any]]
                let accepted: [[String: Any]] = changes.map { change in
                    [
                        "id": change["id"]!,
                        "table_name": change["table_name"]!,
                        "operation": change["operation"]!,
                        "status": "applied",
                        "server_updated_at": "2026-01-01T14:00:00.000Z",
                    ] as [String: Any]
                }
                let json: [String: Any] = [
                    "accepted": accepted, "rejected": [] as [Any],
                    "checkpoint": 20, "server_time": "2026-01-01T14:00:00.000Z",
                    "schema_version": 1, "schema_hash": "test",
                ]
                return try self.mockResponse(json: json)
            } else if path.hasSuffix("/sync/pull") {
                return try self.mockResponse(json: self.pullJSON(checkpoint: 20))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv()
        defer { engine.stop() }

        try await engine.start()

        // Insert a record — CDC trigger fires, pending created
        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z"]
        )
        let tracker = ChangeTracker(database: db)
        XCTAssertTrue(try tracker.hasPendingChanges())

        // Sync — pushes the record, server accepts with RYOW timestamp
        try await engine.syncNow()

        // Push was called
        XCTAssertTrue(pushCalled)

        // Pending drained
        XCTAssertFalse(try tracker.hasPendingChanges())

        // RYOW: local updated_at matches server timestamp
        let row = try db.queryOne("SELECT updated_at FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["updated_at"] as String?, "2026-01-01T14:00:00.000Z")
    }

    func testPullAppliesServerRecord() async throws {
        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/schema") {
                return try self.mockResponse(json: self.schemaJSON)
            } else if path.hasSuffix("/sync/register") {
                return try self.mockResponse(json: self.registerJSON)
            } else if path.hasSuffix("/sync/snapshot") {
                return try self.mockResponse(json: self.snapshotJSON(checkpoint: 0, records: []))
            } else if path.hasSuffix("/sync/pull") {
                // Return a server record in the initial pull
                let json: [String: Any] = [
                    "changes": [
                        [
                            "id": "w1", "table_name": "orders",
                            "data": [
                                "id": "w1", "ship_address": "Server Address",
                                "user_id": "u1", "updated_at": "2026-01-01T12:00:00.000Z",
                            ] as [String: Any],
                            "updated_at": "2026-01-01T12:00:00.000Z",
                        ] as [String: Any]
                    ],
                    "deletes": [] as [Any],
                    "checkpoint": 15, "has_more": false,
                    "schema_version": 1, "schema_hash": "test",
                ]
                return try self.mockResponse(json: json)
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv()
        defer { engine.stop() }

        try await engine.start()

        // Server record should be in local DB
        let row = try db.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["ship_address"] as String?, "Server Address")

        // No pending changes (pull applies under sync_lock)
        let tracker = ChangeTracker(database: db)
        XCTAssertFalse(try tracker.hasPendingChanges())
    }

    func testPullPagesUntilComplete() async throws {
        var pullCallCount = 0

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/schema") {
                return try self.mockResponse(json: self.schemaJSON)
            } else if path.hasSuffix("/sync/register") {
                return try self.mockResponse(json: self.registerJSON)
            } else if path.hasSuffix("/sync/snapshot") {
                return try self.mockResponse(json: self.snapshotJSON(checkpoint: 0, records: []))
            } else if path.hasSuffix("/sync/pull") {
                pullCallCount += 1
                if pullCallCount == 1 {
                    // First page: has_more=true
                    let json: [String: Any] = [
                        "changes": [
                            ["id": "w1", "table_name": "orders",
                             "data": ["id": "w1", "ship_address": "Address 1", "user_id": "u1",
                                      "updated_at": "2026-01-01T12:00:00.000Z"] as [String: Any],
                             "updated_at": "2026-01-01T12:00:00.000Z"] as [String: Any]
                        ],
                        "deletes": [] as [Any],
                        "checkpoint": 5, "has_more": true,
                        "schema_version": 1, "schema_hash": "test",
                    ]
                    return try self.mockResponse(json: json)
                } else {
                    // Second page: has_more=false
                    let json: [String: Any] = [
                        "changes": [
                            ["id": "w2", "table_name": "orders",
                             "data": ["id": "w2", "ship_address": "Address 2", "user_id": "u1",
                                      "updated_at": "2026-01-01T13:00:00.000Z"] as [String: Any],
                             "updated_at": "2026-01-01T13:00:00.000Z"] as [String: Any]
                        ],
                        "deletes": [] as [Any],
                        "checkpoint": 10, "has_more": false,
                        "schema_version": 1, "schema_hash": "test",
                    ]
                    return try self.mockResponse(json: json)
                }
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv()
        defer { engine.stop() }

        try await engine.start()

        // Two pull requests were made
        XCTAssertEqual(pullCallCount, 2)

        // Both records applied
        let count = try db.query("SELECT id FROM orders", params: nil)
        XCTAssertEqual(count.count, 2)

        // Checkpoint is from the final page
        let checkpoint = try db.readTransaction { db in try SynchroMeta.getInt64(db, key: .checkpoint) }
        XCTAssertEqual(checkpoint, 10)
    }

    func testSyncRetriesOnRetryableError() async throws {
        var pushCallCount = 0

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/schema") {
                return try self.mockResponse(json: self.schemaJSON)
            } else if path.hasSuffix("/sync/register") {
                return try self.mockResponse(json: self.registerJSON)
            } else if path.hasSuffix("/sync/snapshot") {
                return try self.mockResponse(json: self.snapshotJSON(checkpoint: 0, records: []))
            } else if path.hasSuffix("/sync/push") {
                pushCallCount += 1
                if pushCallCount == 1 {
                    // First attempt: 503 with short retry-after
                    let data = try JSONSerialization.data(withJSONObject: ["error": "unavailable"])
                    let response = HTTPURLResponse(url: request.url!, statusCode: 503, httpVersion: nil,
                                                   headerFields: ["Retry-After": "0.01"])!
                    return (response, data)
                } else {
                    // Second attempt: success
                    let body = try JSONSerialization.jsonObject(with: request.bodyData()!) as! [String: Any]
                    let changes = body["changes"] as! [[String: Any]]
                    let accepted: [[String: Any]] = changes.map { [
                        "id": $0["id"]!, "table_name": $0["table_name"]!,
                        "operation": $0["operation"]!, "status": "applied",
                        "server_updated_at": "2026-01-01T14:00:00.000Z",
                    ] as [String: Any] }
                    let json: [String: Any] = [
                        "accepted": accepted, "rejected": [] as [Any],
                        "checkpoint": 20, "server_time": "2026-01-01T14:00:00.000Z",
                        "schema_version": 1, "schema_hash": "test",
                    ]
                    return try self.mockResponse(json: json)
                }
            } else if path.hasSuffix("/sync/pull") {
                return try self.mockResponse(json: self.pullJSON(checkpoint: 20))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv()
        defer { engine.stop() }

        try await engine.start()

        // Insert a record
        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z"]
        )

        // Sync — first push fails (503), retry succeeds
        try await engine.syncNow()

        // Push was retried
        XCTAssertEqual(pushCallCount, 2)

        // Pending drained despite initial failure
        let tracker = ChangeTracker(database: db)
        XCTAssertFalse(try tracker.hasPendingChanges())
    }

    func testStatusTransitionsDuringSyncCycle() async throws {
        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/schema") {
                return try self.mockResponse(json: self.schemaJSON)
            } else if path.hasSuffix("/sync/register") {
                return try self.mockResponse(json: self.registerJSON)
            } else if path.hasSuffix("/sync/snapshot") {
                return try self.mockResponse(json: self.snapshotJSON(checkpoint: 0, records: []))
            } else if path.hasSuffix("/sync/pull") {
                return try self.mockResponse(json: self.pullJSON(checkpoint: 10))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, _) = try makeIntegrationEnv()
        defer { engine.stop() }

        var statuses: [String] = []
        let _ = engine.onStatusChange { status in
            switch status {
            case .idle: statuses.append("idle")
            case .syncing: statuses.append("syncing")
            case .error: statuses.append("error")
            case .stopped: statuses.append("stopped")
            }
        }

        try await engine.start()

        // start() sets idle after register, then syncing+idle for initial sync cycle
        XCTAssertEqual(statuses, ["idle", "syncing", "idle"])

        // syncNow triggers another cycle
        statuses.removeAll()
        try await engine.syncNow()
        XCTAssertEqual(statuses, ["syncing", "idle"])

        // stop sets stopped
        statuses.removeAll()
        engine.stop()
        XCTAssertEqual(statuses, ["stopped"])
    }

    func testSnapshotFlowRebuildsTables() async throws {
        var pullCallCount = 0
        var snapshotCallCount = 0
        var snapshotApproved = false

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/schema") {
                return try self.mockResponse(json: self.schemaJSON)
            } else if path.hasSuffix("/sync/register") {
                return try self.mockResponse(json: self.registerJSON)
            } else if path.hasSuffix("/sync/pull") {
                pullCallCount += 1
                if pullCallCount == 1 {
                    // Initial pull during start() — return snapshot_required
                    let json: [String: Any] = [
                        "changes": [] as [Any], "deletes": [] as [Any],
                        "checkpoint": 0, "has_more": false,
                        "snapshot_required": true,
                        "schema_version": 1, "schema_hash": "test",
                    ]
                    return try self.mockResponse(json: json)
                } else {
                    XCTFail("Pull should not be called again after snapshot")
                    return try self.mockResponse(json: self.pullJSON(checkpoint: 0))
                }
            } else if path.hasSuffix("/sync/snapshot") {
                snapshotCallCount += 1
                if snapshotCallCount == 1 {
                    // Initial bootstrap snapshot is empty.
                    let json = self.snapshotJSON(checkpoint: 0, records: [])
                    return try self.mockResponse(json: json)
                } else if snapshotCallCount == 2 {
                    // Rebuild snapshot first page
                    let json: [String: Any] = [
                        "records": [
                            ["id": "w1", "table_name": "orders",
                             "data": ["id": "w1", "ship_address": "Rebuilt Address", "user_id": "u1",
                                      "updated_at": "2026-01-01T12:00:00.000Z"] as [String: Any],
                             "updated_at": "2026-01-01T12:00:00.000Z"] as [String: Any]
                        ],
                        "cursor": ["checkpoint": 50, "table_idx": 0, "after_id": "w1"] as [String: Any],
                        "checkpoint": 50, "has_more": true,
                        "schema_version": 1, "schema_hash": "test",
                    ]
                    return try self.mockResponse(json: json)
                } else {
                    // Final snapshot page
                    let json: [String: Any] = [
                        "records": [] as [Any],
                        "checkpoint": 100, "has_more": false,
                        "schema_version": 1, "schema_hash": "test",
                    ]
                    return try self.mockResponse(json: json)
                }
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected: \(request.url!.path)"])
        }

        let (engine, db) = try makeIntegrationEnv()
        defer { engine.stop() }

        // Register snapshot approval callback
        let _ = engine.onSnapshotRequired {
            snapshotApproved = true
            return true
        }

        try await engine.start()

        // Snapshot callback was invoked
        XCTAssertTrue(snapshotApproved)

        // Snapshot paged through (2 calls: first with data, second empty)
        XCTAssertEqual(snapshotCallCount, 3)

        // Rebuilt record exists in local DB
        let row = try db.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["ship_address"] as String?, "Rebuilt Address")

        // Checkpoint set from final snapshot page
        let checkpoint = try db.readTransaction { db in try SynchroMeta.getInt64(db, key: .checkpoint) }
        XCTAssertEqual(checkpoint, 100)

        // No pending changes (snapshot applied under sync_lock)
        let tracker = ChangeTracker(database: db)
        XCTAssertFalse(try tracker.hasPendingChanges())
    }

    func testConflictCallbackFiresDuringSyncCycle() async throws {
        var receivedConflicts: [ConflictEvent] = []

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/schema") {
                return try self.mockResponse(json: self.schemaJSON)
            } else if path.hasSuffix("/sync/register") {
                return try self.mockResponse(json: self.registerJSON)
            } else if path.hasSuffix("/sync/snapshot") {
                return try self.mockResponse(json: self.snapshotJSON(checkpoint: 0, records: []))
            } else if path.hasSuffix("/sync/push") {
                // Reject the push with a conflict + server version
                let body = try JSONSerialization.jsonObject(with: request.bodyData()!) as! [String: Any]
                let changes = body["changes"] as! [[String: Any]]
                let rejected: [[String: Any]] = changes.map { change in
                    [
                        "id": change["id"]!,
                        "table_name": change["table_name"]!,
                        "operation": change["operation"]!,
                        "status": "conflict",
                        "reason": "server version is newer",
                        "server_version": [
                            "id": change["id"]!,
                            "table_name": change["table_name"]!,
                            "data": [
                                "id": change["id"]!,
                                "ship_address": "Server Wins",
                                "user_id": "u1",
                                "updated_at": "2026-01-01T15:00:00.000Z",
                            ] as [String: Any],
                            "updated_at": "2026-01-01T15:00:00.000Z",
                        ] as [String: Any],
                    ] as [String: Any]
                }
                let json: [String: Any] = [
                    "accepted": [] as [Any], "rejected": rejected,
                    "checkpoint": 20, "server_time": "2026-01-01T15:00:00.000Z",
                    "schema_version": 1, "schema_hash": "test",
                ]
                return try self.mockResponse(json: json)
            } else if path.hasSuffix("/sync/pull") {
                return try self.mockResponse(json: self.pullJSON(checkpoint: 20))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv()
        defer { engine.stop() }

        // Register conflict callback
        let _ = engine.onConflict { event in
            receivedConflicts.append(event)
        }

        try await engine.start()

        // Insert a record that will conflict
        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "Client Address", "u1", "2026-01-01T10:00:00.000Z"]
        )

        // Sync — push is rejected with conflict, server version applied
        try await engine.syncNow()

        // Conflict callback was fired
        XCTAssertEqual(receivedConflicts.count, 1)
        XCTAssertEqual(receivedConflicts[0].table, "orders")
        XCTAssertEqual(receivedConflicts[0].recordID, "w1")
        XCTAssertEqual(receivedConflicts[0].serverData?["ship_address"], AnyCodable("Server Wins"))

        // Server version was applied locally
        let row = try db.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["ship_address"] as String?, "Server Wins")

        // Pending drained
        let tracker = ChangeTracker(database: db)
        XCTAssertFalse(try tracker.hasPendingChanges())
    }

    // MARK: - Helpers

    private func makeSyncEngine() throws -> (SyncEngine, SynchroDatabase) {
        let tmpDir = NSTemporaryDirectory()
        let path = (tmpDir as NSString).appendingPathComponent("synchro_test_\(UUID().uuidString).sqlite")
        let config = SynchroConfig(
            dbPath: path,
            serverURL: URL(string: "http://test.local")!,
            authProvider: { "token" },
            clientID: "test",
            appVersion: "1.0.0",
            maxRetryAttempts: 3
        )
        let db = try SynchroDatabase(path: path)
        let httpClient = HttpClient(config: config)
        let schemaManager = SchemaManager(database: db)
        let changeTracker = ChangeTracker(database: db)
        let pullProcessor = PullProcessor(database: db)
        let pushProcessor = PushProcessor(database: db, changeTracker: changeTracker)

        let engine = SyncEngine(
            config: config,
            database: db,
            httpClient: httpClient,
            schemaManager: schemaManager,
            changeTracker: changeTracker,
            pullProcessor: pullProcessor,
            pushProcessor: pushProcessor
        )
        return (engine, db)
    }

    private func makeIntegrationEnv() throws -> (SyncEngine, SynchroDatabase) {
        let sessionConfig = URLSessionConfiguration.ephemeral
        sessionConfig.protocolClasses = [MockURLProtocol.self]
        let session = URLSession(configuration: sessionConfig)

        let tmpDir = NSTemporaryDirectory()
        let path = (tmpDir as NSString).appendingPathComponent("synchro_test_\(UUID().uuidString).sqlite")
        let config = SynchroConfig(
            dbPath: path,
            serverURL: URL(string: "http://test.local")!,
            authProvider: { "token" },
            clientID: "test-device",
            appVersion: "1.0.0",
            syncInterval: 999,
            maxRetryAttempts: 3
        )
        let db = try SynchroDatabase(path: path)
        let httpClient = HttpClient(config: config, session: session)
        let schemaManager = SchemaManager(database: db)
        let changeTracker = ChangeTracker(database: db)
        let pullProcessor = PullProcessor(database: db)
        let pushProcessor = PushProcessor(database: db, changeTracker: changeTracker)

        let engine = SyncEngine(
            config: config,
            database: db,
            httpClient: httpClient,
            schemaManager: schemaManager,
            changeTracker: changeTracker,
            pullProcessor: pullProcessor,
            pushProcessor: pushProcessor
        )
        return (engine, db)
    }

    // MARK: - Mock JSON Helpers

    private var schemaJSON: [String: Any] {
        [
            "schema_version": 1, "schema_hash": "test",
            "server_time": "2026-01-01T12:00:00.000Z",
            "tables": [
                [
                    "table_name": "orders",
                    "push_policy": "owner_only",
                    "updated_at_column": "updated_at",
                    "deleted_at_column": "deleted_at",
                    "primary_key": ["id"],
                    "columns": [
                        ["name": "id", "db_type": "uuid", "logical_type": "string", "nullable": false, "default_kind": "none", "is_primary_key": true] as [String: Any],
                        ["name": "ship_address", "db_type": "text", "logical_type": "string", "nullable": true, "default_kind": "none", "is_primary_key": false] as [String: Any],
                        ["name": "user_id", "db_type": "uuid", "logical_type": "string", "nullable": false, "default_kind": "none", "is_primary_key": false] as [String: Any],
                        ["name": "updated_at", "db_type": "timestamp", "logical_type": "datetime", "nullable": false, "default_kind": "none", "is_primary_key": false] as [String: Any],
                        ["name": "deleted_at", "db_type": "timestamp", "logical_type": "datetime", "nullable": true, "default_kind": "none", "is_primary_key": false] as [String: Any],
                    ]
                ] as [String: Any]
            ]
        ]
    }

    private var registerJSON: [String: Any] {
        [
            "id": "server-client-1",
            "server_time": "2026-01-01T12:00:00.000Z",
            "checkpoint": 0,
            "schema_version": 1,
            "schema_hash": "test",
        ]
    }

    private func pullJSON(checkpoint: Int) -> [String: Any] {
        [
            "changes": [] as [Any],
            "deletes": [] as [Any],
            "checkpoint": checkpoint,
            "has_more": false,
            "schema_version": 1,
            "schema_hash": "test",
        ]
    }

    private func snapshotJSON(checkpoint: Int, records: [[String: Any]]) -> [String: Any] {
        [
            "records": records,
            "checkpoint": checkpoint,
            "has_more": false,
            "schema_version": 1,
            "schema_hash": "test",
        ]
    }

    private func mockResponse(statusCode: Int = 200, json: [String: Any]) throws -> (HTTPURLResponse, Data) {
        let data = try JSONSerialization.data(withJSONObject: json)
        let response = HTTPURLResponse(url: URL(string: "http://test.local")!, statusCode: statusCode, httpVersion: nil, headerFields: nil)!
        return (response, data)
    }
}
