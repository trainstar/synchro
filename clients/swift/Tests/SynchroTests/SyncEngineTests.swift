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
            if path.hasSuffix("/sync/connect") {
                callLog.append("connect")
                return try self.mockResponse(json: self.connectJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                callLog.append("rebuild")
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_1"))
            } else if path.hasSuffix("/sync/pull") {
                callLog.append("pull")
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_2"))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected: \(path)"])
        }

        let (engine, db) = try makeIntegrationEnv()
        defer { engine.stop() }

        try await engine.start()

        XCTAssertEqual(callLog, ["connect", "rebuild", "pull"])

        let scopeSetVersion = try db.readTransaction { db in
            try SynchroMeta.getInt64(db, key: .scopeSetVersion)
        }
        XCTAssertEqual(scopeSetVersion, 1)

        let scopes = try db.readTransaction { db in
            try SynchroMeta.getAllScopes(db)
        }
        XCTAssertEqual(scopes.count, 1)
        XCTAssertEqual(scopes[0].scopeID, self.scopeID)
        XCTAssertEqual(scopes[0].cursor, "scope_cursor_2")
        XCTAssertEqual(scopes[0].checksum, "sum_2")

        let tables = try db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'", params: nil)
        XCTAssertEqual(tables.count, 1)

        let triggers = try db.query("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_synchro_cdc_%orders'", params: nil)
        XCTAssertEqual(triggers.count, 3)
    }

    func testPushAcceptedAppliesRYOW() async throws {
        var pushCalled = false

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_1"))
            } else if path.hasSuffix("/sync/push") {
                pushCalled = true
                let body = try JSONSerialization.jsonObject(with: request.bodyData()!) as! [String: Any]
                let mutations = body["mutations"] as! [[String: Any]]
                let accepted: [[String: Any]] = mutations.map { mutation in
                    let pk = mutation["pk"] as! [String: Any]
                    let columns = mutation["columns"] as? [String: Any] ?? [:]
                    var serverRow = columns
                    serverRow["id"] = pk["id"]
                    serverRow["updated_at"] = "2026-01-01T14:00:00.000Z"
                    serverRow["deleted_at"] = NSNull()
                    return [
                        "mutation_id": mutation["mutation_id"]!,
                        "table": mutation["table"]!,
                        "pk": pk,
                        "status": "applied",
                        "server_row": serverRow,
                        "server_version": "2026-01-01T14:00:00.000Z",
                    ] as [String: Any]
                }
                let json: [String: Any] = [
                    "server_time": "2026-01-01T14:00:00.000Z",
                    "accepted": accepted,
                    "rejected": [] as [Any]
                ]
                return try self.mockResponse(json: json)
            } else if path.hasSuffix("/sync/pull") {
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_2"))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv()
        defer { engine.stop() }

        try await engine.start()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z"]
        )
        let tracker = ChangeTracker(database: db)
        XCTAssertTrue(try tracker.hasPendingChanges())

        try await engine.syncNow()

        XCTAssertTrue(pushCalled)
        XCTAssertFalse(try tracker.hasPendingChanges())
        let row = try db.queryOne("SELECT updated_at FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["updated_at"] as String?, "2026-01-01T14:00:00.000Z")
    }

    func testPullAppliesServerRecord() async throws {
        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_1"))
            } else if path.hasSuffix("/sync/pull") {
                let json: [String: Any] = [
                    "changes": [
                        [
                            "scope": self.scopeID,
                            "table": "orders",
                            "op": "upsert",
                            "pk": ["id": "w1"] as [String: Any],
                            "row": [
                                "id": "w1", "ship_address": "Server Address",
                                "user_id": "u1",
                                "updated_at": "2026-01-01T12:00:00.000Z",
                                "deleted_at": NSNull()
                            ] as [String: Any],
                            "server_version": "sv_1",
                        ] as [String: Any]
                    ],
                    "scope_set_version": 1,
                    "scope_cursors": [self.scopeID: "scope_cursor_2"],
                    "scope_updates": ["add": [] as [Any], "remove": [] as [Any]],
                    "rebuild": [] as [Any],
                    "has_more": false,
                    "checksums": [self.scopeID: "sum_2"]
                ]
                return try self.mockResponse(json: json)
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv()
        defer { engine.stop() }

        try await engine.start()

        let row = try db.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["ship_address"] as String?, "Server Address")

        let tracker = ChangeTracker(database: db)
        XCTAssertFalse(try tracker.hasPendingChanges())
    }

    func testPullPagesUntilComplete() async throws {
        var pullCallCount = 0

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_1"))
            } else if path.hasSuffix("/sync/pull") {
                pullCallCount += 1
                if pullCallCount == 1 {
                    let json: [String: Any] = [
                        "changes": [
                            [
                                "scope": self.scopeID,
                                "table": "orders",
                                "op": "upsert",
                                "pk": ["id": "w1"] as [String: Any],
                                "row": [
                                    "id": "w1",
                                    "ship_address": "Address 1",
                                    "user_id": "u1",
                                    "updated_at": "2026-01-01T12:00:00.000Z",
                                    "deleted_at": NSNull()
                                ] as [String: Any],
                                "server_version": "sv_1",
                            ] as [String: Any]
                        ],
                        "scope_set_version": 1,
                        "scope_cursors": [self.scopeID: "scope_cursor_mid"],
                        "scope_updates": ["add": [] as [Any], "remove": [] as [Any]],
                        "rebuild": [] as [Any],
                        "has_more": true
                    ]
                    return try self.mockResponse(json: json)
                } else {
                    let json: [String: Any] = [
                        "changes": [
                            [
                                "scope": self.scopeID,
                                "table": "orders",
                                "op": "upsert",
                                "pk": ["id": "w2"] as [String: Any],
                                "row": [
                                    "id": "w2",
                                    "ship_address": "Address 2",
                                    "user_id": "u1",
                                    "updated_at": "2026-01-01T13:00:00.000Z",
                                    "deleted_at": NSNull()
                                ] as [String: Any],
                                "server_version": "sv_2",
                            ] as [String: Any]
                        ],
                        "scope_set_version": 1,
                        "scope_cursors": [self.scopeID: "scope_cursor_2"],
                        "scope_updates": ["add": [] as [Any], "remove": [] as [Any]],
                        "rebuild": [] as [Any],
                        "has_more": false,
                        "checksums": [self.scopeID: "sum_2"]
                    ]
                    return try self.mockResponse(json: json)
                }
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv()
        defer { engine.stop() }

        try await engine.start()

        XCTAssertEqual(pullCallCount, 2)
        let count = try db.query("SELECT id FROM orders", params: nil)
        XCTAssertEqual(count.count, 2)

        let scopes = try db.readTransaction { db in
            try SynchroMeta.getAllScopes(db)
        }
        XCTAssertEqual(scopes.first?.cursor, "scope_cursor_2")
    }

    func testSyncRetriesOnRetryableError() async throws {
        var pushCallCount = 0

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_1"))
            } else if path.hasSuffix("/sync/push") {
                pushCallCount += 1
                if pushCallCount == 1 {
                    let data = try JSONSerialization.data(withJSONObject: ["error": "unavailable"])
                    let response = HTTPURLResponse(url: request.url!, statusCode: 503, httpVersion: nil,
                                                   headerFields: ["Retry-After": "0.01"])!
                    return (response, data)
                } else {
                    // Second attempt: success
                    let body = try JSONSerialization.jsonObject(with: request.bodyData()!) as! [String: Any]
                    let mutations = body["mutations"] as! [[String: Any]]
                    let accepted: [[String: Any]] = mutations.map {
                        let pk = $0["pk"] as! [String: Any]
                        let columns = $0["columns"] as? [String: Any] ?? [:]
                        var serverRow = columns
                        serverRow["id"] = pk["id"]
                        serverRow["updated_at"] = "2026-01-01T14:00:00.000Z"
                        serverRow["deleted_at"] = NSNull()
                        return [
                            "mutation_id": $0["mutation_id"]!,
                            "table": $0["table"]!,
                            "pk": pk,
                            "status": "applied",
                            "server_row": serverRow,
                            "server_version": "2026-01-01T14:00:00.000Z",
                        ] as [String: Any]
                    }
                    let json: [String: Any] = [
                        "server_time": "2026-01-01T14:00:00.000Z",
                        "accepted": accepted,
                        "rejected": [] as [Any],
                    ]
                    return try self.mockResponse(json: json)
                }
            } else if path.hasSuffix("/sync/pull") {
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_2"))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv()
        defer { engine.stop() }

        try await engine.start()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z"]
        )

        try await engine.syncNow()

        XCTAssertEqual(pushCallCount, 2)
        let tracker = ChangeTracker(database: db)
        XCTAssertFalse(try tracker.hasPendingChanges())
    }

    func testRetryableStartupFailureDoesNotRequireAppRestart() async throws {
        var pullCallCount = 0

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_1"))
            } else if path.hasSuffix("/sync/pull") {
                pullCallCount += 1
                if pullCallCount == 1 {
                    let data = try JSONSerialization.data(withJSONObject: ["error": "temporarily unavailable"])
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 503,
                        httpVersion: nil,
                        headerFields: ["Retry-After": "0.01"]
                    )!
                    return (response, data)
                }
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_2"))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv(maxRetryAttempts: 0)
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

        let initialSyncCompleted = expectation(description: "initial sync completed after internal retry")
        try await engine.start(options: SyncOptions(initialSyncCompleted: {
            initialSyncCompleted.fulfill()
        }))

        do {
            try await engine.start()
            XCTFail("Expected alreadyStarted while engine owns startup retry")
        } catch SynchroError.alreadyStarted {
        } catch {
            XCTFail("Expected alreadyStarted, got \(error)")
        }

        await fulfillment(of: [initialSyncCompleted], timeout: 1.0)

        XCTAssertEqual(pullCallCount, 2)
        XCTAssertTrue(statuses.contains("error"))
        XCTAssertEqual(statuses.last, "idle")

        let scope = try db.readTransaction { db in
            try SynchroMeta.getAllScopes(db).first
        }
        XCTAssertEqual(scope?.cursor, "scope_cursor_2")
    }

    func testNonRetryableStartupFailureStillThrowsAndAllowsRestart() async throws {
        var returnSuccess = false

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                if returnSuccess {
                    return try self.mockResponse(json: self.connectJSON)
                }
                return try self.mockResponse(statusCode: 500, json: ["error": "fatal bootstrap"])
            } else if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_1"))
            } else if path.hasSuffix("/sync/pull") {
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_2"))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, _) = try makeIntegrationEnv()
        defer { engine.stop() }

        do {
            try await engine.start()
            XCTFail("Expected non-retryable startup failure")
        } catch {
            XCTAssertFalse(error is RetryableError)
        }

        returnSuccess = true
        try await engine.start()
    }

    func testStatusTransitionsDuringSyncCycle() async throws {
        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_1"))
            } else if path.hasSuffix("/sync/pull") {
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_2"))
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

        XCTAssertEqual(statuses, ["idle", "syncing", "idle"])

        statuses.removeAll()
        try await engine.syncNow()
        XCTAssertEqual(statuses, ["syncing", "idle"])

        statuses.removeAll()
        engine.stop()
        XCTAssertEqual(statuses, ["stopped"])
    }

    func testConflictCallbackFiresDuringSyncCycle() async throws {
        var receivedConflicts: [ConflictEvent] = []

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_1"))
            } else if path.hasSuffix("/sync/push") {
                let body = try JSONSerialization.jsonObject(with: request.bodyData()!) as! [String: Any]
                let mutations = body["mutations"] as! [[String: Any]]
                let rejected: [[String: Any]] = mutations.map { mutation in
                    let pk = mutation["pk"] as! [String: Any]
                    return [
                        "mutation_id": mutation["mutation_id"]!,
                        "table": mutation["table"]!,
                        "pk": pk,
                        "status": "conflict",
                        "code": "version_conflict",
                        "message": "server version is newer",
                        "server_row": [
                            "id": pk["id"]!,
                            "ship_address": "Server Wins",
                            "user_id": "u1",
                            "updated_at": "2026-01-01T15:00:00.000Z",
                            "deleted_at": NSNull()
                        ] as [String: Any],
                        "server_version": "2026-01-01T15:00:00.000Z",
                    ] as [String: Any]
                }
                let json: [String: Any] = [
                    "server_time": "2026-01-01T15:00:00.000Z",
                    "accepted": [] as [Any],
                    "rejected": rejected,
                ]
                return try self.mockResponse(json: json)
            } else if path.hasSuffix("/sync/pull") {
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_2"))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv()
        defer { engine.stop() }

        let _ = engine.onConflict { event in
            receivedConflicts.append(event)
        }

        try await engine.start()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "Client Address", "u1", "2026-01-01T10:00:00.000Z"]
        )

        try await engine.syncNow()

        XCTAssertEqual(receivedConflicts.count, 1)
        XCTAssertEqual(receivedConflicts[0].table, "orders")
        XCTAssertEqual(receivedConflicts[0].recordID, "w1")
        XCTAssertEqual(receivedConflicts[0].serverData?["ship_address"], AnyCodable("Server Wins"))

        let row = try db.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["ship_address"] as String?, "Server Wins")

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

    private func makeIntegrationEnv(maxRetryAttempts: Int = 3) throws -> (SyncEngine, SynchroDatabase) {
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
            maxRetryAttempts: maxRetryAttempts
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

    private let scopeID = "orders_user:u1"

    private var connectJSON: [String: Any] {
        [
            "server_time": "2026-01-01T12:00:00.000Z",
            "protocol_version": 1,
            "scope_set_version": 1,
            "schema": [
                "version": 1,
                "hash": "test",
                "action": "replace"
            ],
            "scopes": [
                "add": [
                    [
                        "id": scopeID,
                        "cursor": NSNull()
                    ] as [String: Any]
                ],
                "remove": [] as [Any]
            ],
            "schema_definition": [
                "tables": [
                    [
                        "name": "orders",
                        "primary_key": ["id"],
                        "updated_at_column": "updated_at",
                        "deleted_at_column": "deleted_at",
                        "composition": "single_scope",
                        "columns": [
                            ["name": "id", "type": "string", "nullable": false] as [String: Any],
                            ["name": "ship_address", "type": "string", "nullable": true] as [String: Any],
                            ["name": "user_id", "type": "string", "nullable": false] as [String: Any],
                            ["name": "updated_at", "type": "datetime", "nullable": false] as [String: Any],
                            ["name": "deleted_at", "type": "datetime", "nullable": true] as [String: Any],
                        ]
                    ] as [String: Any]
                ]
            ] as [String: Any]
        ]
    }

    private func rebuildJSON(
        records: [[String: Any]] = [],
        cursor: String? = nil,
        hasMore: Bool = false,
        finalCursor: String? = nil,
        checksum: String = "sum_1"
    ) -> [String: Any] {
        [
            "scope": scopeID,
            "records": records,
            "cursor": cursor ?? NSNull(),
            "has_more": hasMore,
            "final_scope_cursor": finalCursor ?? NSNull(),
            "checksum": hasMore ? NSNull() : checksum,
        ]
    }

    private func scopePullJSON(
        cursor: String,
        changes: [[String: Any]] = [],
        hasMore: Bool = false,
        rebuild: [String] = []
    ) -> [String: Any] {
        [
            "changes": changes,
            "scope_set_version": 1,
            "scope_cursors": [scopeID: cursor],
            "scope_updates": [
                "add": [] as [Any],
                "remove": [] as [Any],
            ] as [String: Any],
            "rebuild": rebuild,
            "has_more": hasMore,
            "checksums": [scopeID: "sum_2"]
        ]
    }

    private func mockResponse(statusCode: Int = 200, json: [String: Any]) throws -> (HTTPURLResponse, Data) {
        let data = try JSONSerialization.data(withJSONObject: json)
        let response = HTTPURLResponse(url: URL(string: "http://test.local")!, statusCode: statusCode, httpVersion: nil, headerFields: nil)!
        return (response, data)
    }
}
