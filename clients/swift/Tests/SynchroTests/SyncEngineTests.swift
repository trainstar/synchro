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
        XCTAssertEqual(scopes[0].checksum, "0")

        let tables = try db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'", params: nil)
        XCTAssertEqual(tables.count, 1)

        let triggers = try db.query("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_synchro_cdc_%orders'", params: nil)
        XCTAssertEqual(triggers.count, 3)
    }

    func testWarmStartUsesExactlyOneConnectAndOnePullRequest() async throws {
        var callLog: [String] = []

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                callLog.append("connect")
                return try self.mockResponse(json: self.connectResumeJSON)
            } else if path.hasSuffix("/sync/pull") {
                callLog.append("pull")
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_2", checksum: "0"))
            } else if path.hasSuffix("/sync/push") {
                callLog.append("push")
                return try self.mockResponse(statusCode: 500, json: ["error": "unexpected push"])
            } else if path.hasSuffix("/sync/rebuild") {
                callLog.append("rebuild")
                return try self.mockResponse(statusCode: 500, json: ["error": "unexpected rebuild"])
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv()
        defer { engine.stop() }

        let schemaManager = SchemaManager(database: db)
        try schemaManager.reconcileLocalSchema(
            schemaVersion: 1,
            schemaHash: "test",
            tables: [ordersLocalSchemaTable(includeNotes: false)]
        )
        try db.writeSyncLockedTransaction { db in
            try SynchroMeta.upsertScope(
                db,
                scopeID: self.scopeID,
                cursor: "scope_cursor_1",
                checksum: "0"
            )
        }

        try await engine.start()

        XCTAssertEqual(callLog, ["connect", "pull"])
    }

    func testSteadyStatePullOnlyCycleUsesSinglePullRequest() async throws {
        var connectCallCount = 0
        var pullCallCount = 0
        var pushCallCount = 0
        var rebuildCallCount = 0

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                connectCallCount += 1
                return try self.mockResponse(json: self.connectResumeJSON)
            } else if path.hasSuffix("/sync/pull") {
                pullCallCount += 1
                let cursor = pullCallCount == 1 ? "scope_cursor_2" : "scope_cursor_3"
                return try self.mockResponse(json: self.scopePullJSON(cursor: cursor, checksum: "0"))
            } else if path.hasSuffix("/sync/push") {
                pushCallCount += 1
                return try self.mockResponse(statusCode: 500, json: ["error": "unexpected push"])
            } else if path.hasSuffix("/sync/rebuild") {
                rebuildCallCount += 1
                return try self.mockResponse(statusCode: 500, json: ["error": "unexpected rebuild"])
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv()
        defer { engine.stop() }

        let schemaManager = SchemaManager(database: db)
        try schemaManager.reconcileLocalSchema(
            schemaVersion: 1,
            schemaHash: "test",
            tables: [ordersLocalSchemaTable(includeNotes: false)]
        )
        try db.writeTransaction { db in
            try SynchroMeta.upsertScope(
                db,
                scopeID: self.scopeID,
                cursor: "scope_cursor_1",
                checksum: "0"
            )
        }

        try await engine.start()
        connectCallCount = 0
        pullCallCount = 0
        pushCallCount = 0
        rebuildCallCount = 0

        try await engine.syncNow()

        XCTAssertEqual(connectCallCount, 0)
        XCTAssertEqual(rebuildCallCount, 0)
        XCTAssertEqual(pushCallCount, 0)
        XCTAssertEqual(pullCallCount, 1)
    }

    func testSteadyStatePushPlusPullCycleUsesTwoRequests() async throws {
        var callLog: [String] = []

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                callLog.append("connect")
                return try self.mockResponse(json: self.connectResumeJSON)
            } else if path.hasSuffix("/sync/push") {
                callLog.append("push")
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
                        "server_version": "opaque_server_version_after_push",
                    ] as [String: Any]
                }
                return try self.mockResponse(json: [
                    "server_time": "2026-01-01T14:00:00.000Z",
                    "accepted": accepted,
                    "rejected": [] as [Any],
                ])
            } else if path.hasSuffix("/sync/pull") {
                callLog.append("pull")
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_2", checksum: "0"))
            } else if path.hasSuffix("/sync/rebuild") {
                callLog.append("rebuild")
                return try self.mockResponse(statusCode: 500, json: ["error": "unexpected rebuild"])
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv()
        defer { engine.stop() }

        let schemaManager = SchemaManager(database: db)
        try schemaManager.reconcileLocalSchema(
            schemaVersion: 1,
            schemaHash: "test",
            tables: [ordersLocalSchemaTable(includeNotes: false)]
        )
        try db.writeTransaction { db in
            try SynchroMeta.upsertScope(
                db,
                scopeID: self.scopeID,
                cursor: "scope_cursor_1",
                checksum: "0"
            )
        }

        try await engine.start()
        callLog.removeAll()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z"]
        )

        try await engine.syncNow()

        XCTAssertEqual(callLog, ["push", "pull"])
    }

    func testConnectRebuildLocalReconcilesSchemaAndRebuildsExistingScope() async throws {
        var callLog: [String] = []

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                callLog.append("connect")
                return try self.mockResponse(json: self.connectRebuildLocalJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                callLog.append("rebuild")
                return try self.mockResponse(json: self.rebuildJSON(
                    records: [[
                        "table": "orders",
                        "pk": ["id": "w1"] as [String: Any],
                        "row": [
                            "id": "w1",
                            "ship_address": "Rebuilt Address",
                            "user_id": "u1",
                            "notes": "schema rebuild local",
                            "updated_at": "2026-01-01T12:00:00.000Z",
                            "deleted_at": NSNull(),
                        ] as [String: Any],
                        "row_checksum": 11,
                        "server_version": "opaque_server_version_rebuild",
                    ]],
                    finalCursor: "scope_cursor_rebuilt",
                    checksum: "11"
                ))
            } else if path.hasSuffix("/sync/pull") {
                callLog.append("pull")
                return try self.mockResponse(json: [
                    "changes": [] as [Any],
                    "scope_set_version": 2,
                    "scope_cursors": [self.scopeID: "scope_cursor_after_rebuild"],
                    "scope_updates": [
                        "add": [] as [Any],
                        "remove": [] as [Any],
                    ] as [String: Any],
                    "rebuild": [] as [Any],
                    "has_more": false,
                    "checksums": [self.scopeID: "11"],
                ])
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv()
        defer { engine.stop() }

        let schemaManager = SchemaManager(database: db)
        try schemaManager.reconcileLocalSchema(
            schemaVersion: 1,
            schemaHash: "old_hash",
            tables: [ordersLocalSchemaTable(includeNotes: false)]
        )
        try db.writeSyncLockedTransaction { db in
            try SynchroMeta.upsertScope(
                db,
                scopeID: self.scopeID,
                cursor: "scope_cursor_old",
                checksum: "3"
            )
            try db.execute(
                sql: "INSERT INTO orders (id, ship_address, user_id, updated_at, deleted_at) VALUES (?, ?, ?, ?, ?)",
                arguments: ["w1", "Old Address", "u1", "2026-01-01T10:00:00.000Z", nil]
            )
        }

        let tracker = ChangeTracker(database: db)
        XCTAssertFalse(try tracker.hasPendingChanges())

        try await engine.start()

        XCTAssertEqual(callLog, ["connect", "rebuild", "pull"])

        let columns = try db.query("PRAGMA table_info(orders)", params: nil)
        let columnNames = Set(columns.compactMap { $0["name"] as? String })
        XCTAssertTrue(columnNames.contains("notes"))

        let row = try db.queryOne(
            "SELECT ship_address, notes FROM orders WHERE id = ?",
            params: ["w1"]
        )
        XCTAssertEqual(row?["ship_address"] as? String, "Rebuilt Address")
        XCTAssertEqual(row?["notes"] as? String, "schema rebuild local")

        let scope = try db.readTransaction { db in
            try SynchroMeta.getScope(db, scopeID: self.scopeID)
        }
        XCTAssertEqual(scope?.cursor, "scope_cursor_after_rebuild")
        XCTAssertEqual(scope?.checksum, "11")
        XCTAssertEqual(scope?.localChecksum, 11)

        let schemaVersion = try db.readTransaction { db in
            try SynchroMeta.getInt64(db, key: .schemaVersion)
        }
        XCTAssertEqual(schemaVersion, 2)
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
                            "row_checksum": 7,
                            "server_version": "sv_1",
                        ] as [String: Any]
                    ],
                    "scope_set_version": 1,
                    "scope_cursors": [self.scopeID: "scope_cursor_2"],
                    "scope_updates": ["add": [] as [Any], "remove": [] as [Any]],
                    "rebuild": [] as [Any],
                    "has_more": false,
                    "checksums": [self.scopeID: "7"]
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

    func testScopeRemovalDeletesLocalRowWithoutQueueingPendingDelete() async throws {
        var pullCallCount = 0

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(
                    records: [[
                        "table": "orders",
                        "pk": ["id": "w1"] as [String: Any],
                        "row": [
                            "id": "w1",
                            "ship_address": "Seeded",
                            "user_id": "u1",
                            "updated_at": "2026-01-01T12:00:00.000Z",
                            "deleted_at": NSNull()
                        ] as [String: Any],
                        "row_checksum": 7,
                        "server_version": "sv_1",
                    ]],
                    finalCursor: "scope_cursor_1",
                    checksum: "7"
                ))
            } else if path.hasSuffix("/sync/pull") {
                pullCallCount += 1
                if pullCallCount == 1 {
                    return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_2"))
                }
                return try self.mockResponse(json: [
                    "changes": [] as [Any],
                    "scope_set_version": 2,
                    "scope_cursors": [self.scopeID: "scope_cursor_3"],
                    "scope_updates": [
                        "add": [] as [Any],
                        "remove": [self.scopeID]
                    ] as [String: Any],
                    "rebuild": [] as [Any],
                    "has_more": false,
                    "checksums": [:] as [String: String]
                ])
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv()
        defer { engine.stop() }

        try await engine.start()
        try await engine.syncNow()

        let row = try db.queryOne("SELECT id FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertNil(row)

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
                                "row_checksum": 7,
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
                                "row_checksum": 9,
                                "server_version": "sv_2",
                            ] as [String: Any]
                        ],
                        "scope_set_version": 1,
                        "scope_cursors": [self.scopeID: "scope_cursor_2"],
                        "scope_updates": ["add": [] as [Any], "remove": [] as [Any]],
                        "rebuild": [] as [Any],
                        "has_more": false,
                        "checksums": [self.scopeID: "14"]
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

    func testTerminalPullChecksumMismatchForcesImmediateRebuild() async throws {
        var pullCallCount = 0
        var rebuildCallCount = 0

        let scopeRecord: [String: Any] = [
            "table": "orders",
            "pk": ["id": "w1"] as [String: Any],
            "row": [
                "id": "w1",
                "ship_address": "Recovered Address",
                "user_id": "u1",
                "updated_at": "2026-01-01T12:00:00.000Z",
                "deleted_at": NSNull()
            ] as [String: Any],
            "row_checksum": 7,
            "server_version": "sv_1",
        ]

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                rebuildCallCount += 1
                if rebuildCallCount == 1 {
                    return try self.mockResponse(json: self.rebuildJSON(
                        records: [scopeRecord],
                        finalCursor: "scope_cursor_1",
                        checksum: "7"
                    ))
                }
                return try self.mockResponse(json: self.rebuildJSON(
                    records: [scopeRecord],
                    finalCursor: "scope_cursor_rebuilt",
                    checksum: "7"
                ))
            } else if path.hasSuffix("/sync/pull") {
                pullCallCount += 1
                if pullCallCount == 1 {
                    return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_2", checksum: "7"))
                }
                return try self.mockResponse(json: [
                    "changes": [] as [Any],
                    "scope_set_version": 1,
                    "scope_cursors": [:] as [String: String],
                    "scope_updates": [
                        "add": [] as [Any],
                        "remove": [] as [Any],
                    ] as [String: Any],
                    "rebuild": [] as [Any],
                    "has_more": false,
                    "checksums": [self.scopeID: "7"]
                ])
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv()
        defer { engine.stop() }

        try await engine.start()

        try db.writeSyncLockedTransaction { conn in
            try SynchroMeta.deleteScopeRow(conn, scopeID: self.scopeID, tableName: "orders", recordID: "w1")
            try conn.execute(sql: "DELETE FROM orders WHERE id = ?", arguments: ["w1"])
        }

        let tracker = ChangeTracker(database: db)
        XCTAssertFalse(try tracker.hasPendingChanges())

        try await engine.syncNow()

        XCTAssertEqual(pullCallCount, 2)
        XCTAssertEqual(rebuildCallCount, 2)

        let row = try db.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["ship_address"] as String?, "Recovered Address")

        let scope = try db.readTransaction { db in
            try SynchroMeta.getScope(db, scopeID: self.scopeID)
        }
        XCTAssertEqual(scope?.cursor, "scope_cursor_rebuilt")
        XCTAssertEqual(scope?.checksum, "7")
        XCTAssertEqual(scope?.localChecksum, 7)
    }

    func testQueuedMutationSurvivesRestartAndPushesExactlyOnce() async throws {
        let dbPath = tempDBPath()
        let clientID = "restart-device"
        let orderID = "restart-order"
        var connectCallCount = 0
        var rebuildCallCount = 0
        var pushCallCount = 0
        var resumedKnownCursor: String?

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            switch path {
            case _ where path.hasSuffix("/sync/connect"):
                connectCallCount += 1
                if connectCallCount == 1 {
                    return try self.mockResponse(json: self.connectJSON)
                }

                let body = try XCTUnwrap(request.bodyData())
                let json = try XCTUnwrap(JSONSerialization.jsonObject(with: body) as? [String: Any])
                let knownScopes = json["known_scopes"] as? [String: Any]
                resumedKnownCursor = (knownScopes?[self.scopeID] as? [String: Any])?["cursor"] as? String
                return try self.mockResponse(json: self.connectResumeJSON)

            case _ where path.hasSuffix("/sync/rebuild"):
                rebuildCallCount += 1
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_1"))

            case _ where path.hasSuffix("/sync/push"):
                pushCallCount += 1
                let json = try XCTUnwrap(
                    JSONSerialization.jsonObject(with: try XCTUnwrap(request.bodyData())) as? [String: Any]
                )
                let mutations = try XCTUnwrap(json["mutations"] as? [[String: Any]])
                XCTAssertEqual(mutations.count, 1)
                let mutation = mutations[0]
                let pk = try XCTUnwrap(mutation["pk"] as? [String: Any])
                XCTAssertEqual(pk["id"] as? String, orderID)
                let columns = mutation["columns"] as? [String: Any] ?? [:]
                var serverRow = columns
                serverRow["id"] = orderID
                serverRow["updated_at"] = "2026-01-01T15:00:00.000Z"
                serverRow["deleted_at"] = NSNull()
                return try self.mockResponse(json: [
                    "server_time": "2026-01-01T15:00:00.000Z",
                    "accepted": [[
                        "mutation_id": mutation["mutation_id"] as Any,
                        "table": "orders",
                        "pk": pk,
                        "status": "applied",
                        "server_row": serverRow,
                        "server_version": "2026-01-01T15:00:00.000Z",
                    ]],
                    "rejected": [] as [Any],
                ])

            case _ where path.hasSuffix("/sync/pull"):
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_2", checksum: "0"))

            default:
                return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
            }
        }

        let (engine1, db1) = try makeIntegrationEnv(dbPath: dbPath, clientID: clientID)
        try await engine1.start()

        _ = try db1.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: [orderID, "Queued After First Start", "u1", "2026-01-01T10:00:00.000Z"]
        )
        let tracker1 = ChangeTracker(database: db1)
        XCTAssertTrue(try tracker1.hasPendingChanges())

        engine1.stop()
        try db1.close()

        let (engine2, db2) = try makeIntegrationEnv(dbPath: dbPath, clientID: clientID)
        defer {
            engine2.stop()
            try? db2.close()
        }

        try await engine2.start()

        XCTAssertEqual(rebuildCallCount, 1)
        XCTAssertEqual(pushCallCount, 1)
        XCTAssertEqual(resumedKnownCursor, "scope_cursor_2")

        let tracker2 = ChangeTracker(database: db2)
        XCTAssertFalse(try tracker2.hasPendingChanges())

        let row = try db2.queryOne(
            "SELECT updated_at FROM orders WHERE id = ?",
            params: [orderID]
        )
        XCTAssertEqual(row?["updated_at"] as? String, "2026-01-01T15:00:00.000Z")
    }

    func testScopeCursorAndChecksumSurviveRestartAndResumeWithoutRebuild() async throws {
        let dbPath = tempDBPath()
        let clientID = "resume-device"
        var connectCallCount = 0
        var rebuildCallCount = 0
        var resumedKnownCursor: String?

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            switch path {
            case _ where path.hasSuffix("/sync/connect"):
                connectCallCount += 1
                if connectCallCount == 1 {
                    return try self.mockResponse(json: self.connectJSON)
                }

                let json = try XCTUnwrap(
                    JSONSerialization.jsonObject(with: try XCTUnwrap(request.bodyData())) as? [String: Any]
                )
                let knownScopes = json["known_scopes"] as? [String: Any]
                resumedKnownCursor = (knownScopes?[self.scopeID] as? [String: Any])?["cursor"] as? String
                return try self.mockResponse(json: self.connectResumeJSON)

            case _ where path.hasSuffix("/sync/rebuild"):
                rebuildCallCount += 1
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_1"))

            case _ where path.hasSuffix("/sync/pull"):
                let cursor = connectCallCount == 1 ? "scope_cursor_2" : "scope_cursor_3"
                return try self.mockResponse(json: self.scopePullJSON(cursor: cursor, checksum: "0"))

            default:
                return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
            }
        }

        let (engine1, db1) = try makeIntegrationEnv(dbPath: dbPath, clientID: clientID)
        try await engine1.start()
        engine1.stop()
        try db1.close()

        let (engine2, db2) = try makeIntegrationEnv(dbPath: dbPath, clientID: clientID)
        defer {
            engine2.stop()
            try? db2.close()
        }
        try await engine2.start()

        XCTAssertEqual(rebuildCallCount, 1)
        XCTAssertEqual(resumedKnownCursor, "scope_cursor_2")

        let scope = try db2.readTransaction { db in
            try SynchroMeta.getScope(db, scopeID: self.scopeID)
        }
        XCTAssertEqual(scope?.cursor, "scope_cursor_3")
        XCTAssertEqual(scope?.checksum, "0")
        XCTAssertEqual(scope?.localChecksum, 0)
    }

    func testPartialRebuildStateSurvivesRestartAndCompletesCleanly() async throws {
        let dbPath = tempDBPath()
        let clientID = "rebuild-restart-device"
        var connectCallCount = 0
        var rebuildCallCount = 0
        var restartedKnownCursor: String?

        let rebuildRecordOne: [String: Any] = [
            "table": "orders",
            "pk": ["id": "w1"] as [String: Any],
            "row": [
                "id": "w1",
                "ship_address": "Address 1",
                "user_id": "u1",
                "updated_at": "2026-01-01T12:00:00.000Z",
                "deleted_at": NSNull(),
            ] as [String: Any],
            "row_checksum": 7,
            "server_version": "sv_1",
        ]

        let rebuildRecordTwo: [String: Any] = [
            "table": "orders",
            "pk": ["id": "w2"] as [String: Any],
            "row": [
                "id": "w2",
                "ship_address": "Address 2",
                "user_id": "u1",
                "updated_at": "2026-01-01T13:00:00.000Z",
                "deleted_at": NSNull(),
            ] as [String: Any],
            "row_checksum": 9,
            "server_version": "sv_2",
        ]

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            switch path {
            case _ where path.hasSuffix("/sync/connect"):
                connectCallCount += 1
                if connectCallCount == 1 {
                    return try self.mockResponse(json: self.connectJSON)
                }

                let json = try XCTUnwrap(
                    JSONSerialization.jsonObject(with: try XCTUnwrap(request.bodyData())) as? [String: Any]
                )
                let knownScopes = json["known_scopes"] as? [String: Any]
                restartedKnownCursor = (knownScopes?[self.scopeID] as? [String: Any])?["cursor"] as? String
                return try self.mockResponse(json: self.connectResumeJSON)

            case _ where path.hasSuffix("/sync/rebuild"):
                rebuildCallCount += 1
                switch rebuildCallCount {
                case 1:
                    return try self.mockResponse(json: self.rebuildJSON(
                        records: [rebuildRecordOne],
                        cursor: "page_1",
                        hasMore: true
                    ))
                case 2:
                    return try self.mockResponse(statusCode: 500, json: ["error": "interrupted"])
                default:
                    return try self.mockResponse(json: self.rebuildJSON(
                        records: [rebuildRecordOne, rebuildRecordTwo],
                        finalCursor: "scope_cursor_recovered",
                        checksum: "14"
                    ))
                }

            case _ where path.hasSuffix("/sync/pull"):
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_recovered", checksum: "14"))

            default:
                return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
            }
        }

        let (engine1, db1) = try makeIntegrationEnv(dbPath: dbPath, clientID: clientID)
        do {
            try await engine1.start()
            XCTFail("expected partial rebuild interruption to fail startup")
        } catch {
            // expected
        }

        let partiallyApplied = try db1.query("SELECT id FROM orders ORDER BY id", params: nil)
        XCTAssertEqual(partiallyApplied.map { $0["id"] as! String }, ["w1"])

        let interruptedScope = try db1.readTransaction { db in
            try SynchroMeta.getScope(db, scopeID: self.scopeID)
        }
        XCTAssertNil(interruptedScope?.cursor)
        XCTAssertEqual(interruptedScope?.generation, 1)
        XCTAssertEqual(interruptedScope?.localChecksum, 7)

        engine1.stop()
        try db1.close()

        let (engine2, db2) = try makeIntegrationEnv(dbPath: dbPath, clientID: clientID)
        defer {
            engine2.stop()
            try? db2.close()
        }

        try await engine2.start()

        XCTAssertNil(restartedKnownCursor)
        XCTAssertEqual(rebuildCallCount, 3)

        let rows = try db2.query("SELECT id FROM orders ORDER BY id", params: nil)
        XCTAssertEqual(rows.map { $0["id"] as! String }, ["w1", "w2"])

        let recoveredScope = try db2.readTransaction { db in
            try SynchroMeta.getScope(db, scopeID: self.scopeID)
        }
        XCTAssertEqual(recoveredScope?.cursor, "scope_cursor_recovered")
        XCTAssertEqual(recoveredScope?.checksum, "14")
        XCTAssertEqual(recoveredScope?.generation, 2)
        XCTAssertEqual(recoveredScope?.localChecksum, 14)

        let tracker = ChangeTracker(database: db2)
        XCTAssertFalse(try tracker.hasPendingChanges())
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

    func testRetryablePushFailurePreservesQueueAcrossRestart() async throws {
        let dbPath = tempDBPath()
        let clientID = "retryable-push-restart-device"
        var pushCallCount = 0
        var connectCallCount = 0
        var shouldFailNextPush = false

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                connectCallCount += 1
                return try self.mockResponse(json: connectCallCount == 1 ? self.connectJSON : self.connectResumeJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_1"))
            } else if path.hasSuffix("/sync/push") {
                pushCallCount += 1
                if shouldFailNextPush {
                    shouldFailNextPush = false
                    let data = try JSONSerialization.data(withJSONObject: ["error": "temporarily unavailable"])
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 503,
                        httpVersion: nil,
                        headerFields: ["Retry-After": "0.01"]
                    )!
                    return (response, data)
                }

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
            } else if path.hasSuffix("/sync/pull") {
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_2"))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine1, db1) = try makeIntegrationEnv(dbPath: dbPath, clientID: clientID, maxRetryAttempts: 0)
        let schemaManager1 = SchemaManager(database: db1)
        try schemaManager1.reconcileLocalSchema(
            schemaVersion: 1,
            schemaHash: "test",
            tables: [ordersLocalSchemaTable(includeNotes: false)]
        )
        do {
            try await engine1.start()
            _ = try db1.execute(
                "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                params: ["w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z"]
            )
            shouldFailNextPush = true
            try await engine1.syncNow()
            XCTFail("expected retryable push failure to abort the first sync")
        } catch {
            XCTAssertTrue(error is RetryableError)
        }

        let tracker1 = ChangeTracker(database: db1)
        XCTAssertTrue(try tracker1.hasPendingChanges())
        let rejectedBeforeRestart = try db1.readTransaction { db in
            try SynchroMeta.listRejectedMutations(db)
        }
        XCTAssertTrue(rejectedBeforeRestart.isEmpty)
        engine1.stop()
        try db1.close()

        let (engine2, db2) = try makeIntegrationEnv(dbPath: dbPath, clientID: clientID, maxRetryAttempts: 0)
        defer {
            engine2.stop()
            try? db2.close()
        }
        let schemaManager2 = SchemaManager(database: db2)
        try schemaManager2.reconcileLocalSchema(
            schemaVersion: 1,
            schemaHash: "test",
            tables: [ordersLocalSchemaTable(includeNotes: false)]
        )

        try await engine2.start()

        let tracker2 = ChangeTracker(database: db2)
        XCTAssertFalse(try tracker2.hasPendingChanges())
        XCTAssertEqual(pushCallCount, 2)

        let localRow = try db2.queryOne(
            "SELECT ship_address, updated_at FROM orders WHERE id = ?",
            params: ["w1"]
        )
        XCTAssertEqual(localRow?["ship_address"] as? String, "123 Main St")
        XCTAssertEqual(localRow?["updated_at"] as? String, "2026-01-01T14:00:00.000Z")

        let rejectedAfterRestart = try db2.readTransaction { db in
            try SynchroMeta.listRejectedMutations(db)
        }
        XCTAssertTrue(rejectedAfterRestart.isEmpty)
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

    func testConnectUnsupportedFailsExplicitly() async throws {
        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: [
                    "server_time": "2026-01-01T12:00:00.000Z",
                    "protocol_version": 2,
                    "scope_set_version": 1,
                    "schema": [
                        "version": 2,
                        "hash": "unsupported_hash",
                        "action": "unsupported",
                    ],
                    "scopes": [
                        "add": [] as [Any],
                        "remove": [] as [Any],
                    ] as [String: Any],
                ])
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, _) = try makeIntegrationEnv()
        defer { engine.stop() }

        do {
            try await engine.start()
            XCTFail("Expected unsupported connect schema action failure")
        } catch SynchroError.invalidResponse(let message) {
            XCTAssertTrue(message.contains("unsupported connect schema action"))
        } catch {
            XCTFail("Expected invalidResponse, got \(error)")
        }
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

    func testRejectedMutationsRemainInspectableAcrossRestart() async throws {
        let dbPath = tempDBPath()
        let clientID = "rejection-persistence-device"
        var connectCallCount = 0

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            switch path {
            case _ where path.hasSuffix("/sync/connect"):
                connectCallCount += 1
                return try self.mockResponse(json: connectCallCount == 1 ? self.connectJSON : self.connectResumeJSON)
            case _ where path.hasSuffix("/sync/rebuild"):
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_1"))
            case _ where path.hasSuffix("/sync/push"):
                let body = try JSONSerialization.jsonObject(with: request.bodyData()!) as! [String: Any]
                let mutations = body["mutations"] as! [[String: Any]]
                let rejected: [[String: Any]] = mutations.map { mutation in
                    let pk = mutation["pk"] as! [String: Any]
                    return [
                        "mutation_id": mutation["mutation_id"]!,
                        "table": mutation["table"]!,
                        "pk": pk,
                        "status": "rejected_terminal",
                        "code": "policy_rejected",
                        "message": "explicit rejection for inspection",
                        "server_row": NSNull(),
                        "server_version": "sv::reject::1",
                    ] as [String: Any]
                }
                return try self.mockResponse(json: [
                    "server_time": "2026-01-01T15:00:00.000Z",
                    "accepted": [] as [Any],
                    "rejected": rejected,
                ])
            case _ where path.hasSuffix("/sync/pull"):
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_2"))
            default:
                return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
            }
        }

        let (engine1, db1) = try makeIntegrationEnv(dbPath: dbPath, clientID: clientID)
        try await engine1.start()
        _ = try db1.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "Client Address", "u1", "2026-01-01T10:00:00.000Z"]
        )
        try await engine1.syncNow()

        let rejectedBeforeRestart = try db1.readTransaction { db in
            try SynchroMeta.listRejectedMutations(db)
        }
        XCTAssertEqual(rejectedBeforeRestart.count, 1)
        XCTAssertTrue(rejectedBeforeRestart[0].mutationID.hasPrefix("orders:w1:"))
        XCTAssertEqual(rejectedBeforeRestart[0].code, "policy_rejected")

        engine1.stop()
        try db1.close()

        let (engine2, db2) = try makeIntegrationEnv(dbPath: dbPath, clientID: clientID)
        defer {
            engine2.stop()
            try? db2.close()
        }
        try await engine2.start()

        let rejectedAfterRestart = try db2.readTransaction { db in
            try SynchroMeta.listRejectedMutations(db)
        }
        XCTAssertEqual(rejectedAfterRestart.count, 1)
        XCTAssertTrue(rejectedAfterRestart[0].mutationID.hasPrefix("orders:w1:"))
        XCTAssertEqual(rejectedAfterRestart[0].message, "explicit rejection for inspection")
        XCTAssertEqual(rejectedAfterRestart[0].serverVersion, "sv::reject::1")

        _ = try db2.execute("DELETE FROM _synchro_rejected_mutations", params: nil)
        let cleared = try db2.query("SELECT mutation_id FROM _synchro_rejected_mutations", params: nil)
        XCTAssertTrue(cleared.isEmpty)
    }

    // MARK: - Helpers

    private func makeSyncEngine() throws -> (SyncEngine, SynchroDatabase) {
        let path = tempDBPath()
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

    private func makeIntegrationEnv(
        dbPath: String? = nil,
        clientID: String = "test-device",
        maxRetryAttempts: Int = 3
    ) throws -> (SyncEngine, SynchroDatabase) {
        let sessionConfig = URLSessionConfiguration.ephemeral
        sessionConfig.protocolClasses = [MockURLProtocol.self]
        let session = URLSession(configuration: sessionConfig)

        let path = dbPath ?? tempDBPath()
        let config = SynchroConfig(
            dbPath: path,
            serverURL: URL(string: "http://test.local")!,
            authProvider: { "token" },
            clientID: clientID,
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

    private func tempDBPath() -> String {
        let tmpDir = NSTemporaryDirectory()
        return (tmpDir as NSString).appendingPathComponent("synchro_test_\(UUID().uuidString).sqlite")
    }

    private var connectJSON: [String: Any] {
        [
            "server_time": "2026-01-01T12:00:00.000Z",
            "protocol_version": 2,
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

    private var connectRebuildLocalJSON: [String: Any] {
        [
            "server_time": "2026-01-01T12:00:00.000Z",
            "protocol_version": 2,
            "scope_set_version": 2,
            "schema": [
                "version": 2,
                "hash": "test_v2",
                "action": "rebuild_local"
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
                            ["name": "notes", "type": "string", "nullable": true] as [String: Any],
                            ["name": "updated_at", "type": "datetime", "nullable": false] as [String: Any],
                            ["name": "deleted_at", "type": "datetime", "nullable": true] as [String: Any],
                        ]
                    ] as [String: Any]
                ]
            ] as [String: Any]
        ]
    }

    private var connectResumeJSON: [String: Any] {
        [
            "server_time": "2026-01-01T12:00:00.000Z",
            "protocol_version": 2,
            "scope_set_version": 1,
            "schema": [
                "version": 1,
                "hash": "test",
                "action": "none",
            ],
            "scopes": [
                "add": [] as [Any],
                "remove": [] as [Any],
            ] as [String: Any],
        ]
    }

    private func rebuildJSON(
        records: [[String: Any]] = [],
        cursor: String? = nil,
        hasMore: Bool = false,
        finalCursor: String? = nil,
        checksum: String = "0"
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
        rebuild: [String] = [],
        checksum: String = "0"
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
            "checksums": [scopeID: checksum]
        ]
    }

    private func mockResponse(statusCode: Int = 200, json: [String: Any]) throws -> (HTTPURLResponse, Data) {
        let data = try JSONSerialization.data(withJSONObject: json)
        let response = HTTPURLResponse(url: URL(string: "http://test.local")!, statusCode: statusCode, httpVersion: nil, headerFields: nil)!
        return (response, data)
    }

    private func ordersLocalSchemaTable(includeNotes: Bool) -> LocalSchemaTable {
        var columns = [
            LocalSchemaColumn(name: "id", logicalType: "string", nullable: false, sqliteDefaultSQL: nil, isPrimaryKey: true),
            LocalSchemaColumn(name: "ship_address", logicalType: "string", nullable: true, sqliteDefaultSQL: nil, isPrimaryKey: false),
            LocalSchemaColumn(name: "user_id", logicalType: "string", nullable: false, sqliteDefaultSQL: nil, isPrimaryKey: false),
            LocalSchemaColumn(name: "updated_at", logicalType: "datetime", nullable: false, sqliteDefaultSQL: nil, isPrimaryKey: false),
            LocalSchemaColumn(name: "deleted_at", logicalType: "datetime", nullable: true, sqliteDefaultSQL: nil, isPrimaryKey: false),
        ]
        if includeNotes {
            columns.insert(
                LocalSchemaColumn(name: "notes", logicalType: "string", nullable: true, sqliteDefaultSQL: nil, isPrimaryKey: false),
                at: 3
            )
        }
        return LocalSchemaTable(
            tableName: "orders",
            updatedAtColumn: "updated_at",
            deletedAtColumn: "deleted_at",
            composition: .singleScope,
            primaryKey: ["id"],
            columns: columns
        )
    }
}
