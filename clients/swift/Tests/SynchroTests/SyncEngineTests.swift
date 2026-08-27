import XCTest
import GRDB
import os
@testable @_spi(Inspection) import Synchro

final class SyncEngineTests: XCTestCase {
    func testCallbackRegistrationAndCancellation() async throws {
        let (engine, _) = try makeSyncEngine()

        var statusUpdates: [String] = []
        let cancellable1 = engine.onStatusChange { status in
            statusUpdates.append(status.rawValue)
        }

        var conflictEvents: [String] = []
        let cancellable2 = engine.onConflict { event in
            conflictEvents.append(event.recordID)
        }

        // Stop triggers a status update
        await engine.stop()
        XCTAssertEqual(statusUpdates, ["stopped"])

        // Cancel callbacks
        cancellable1.cancel()
        cancellable2.cancel()

        // After cancel, no more updates
        statusUpdates.removeAll()
        await engine.stop()
        XCTAssertTrue(statusUpdates.isEmpty)
    }

    func testMultipleCallbacksIndependentCancellation() async throws {
        let (engine, _) = try makeSyncEngine()

        var updates1: [String] = []
        var updates2: [String] = []

        let cancellable1 = engine.onStatusChange { _ in
            updates1.append("hit")
        }
        let _ = engine.onStatusChange { _ in
            updates2.append("hit")
        }

        await engine.stop()
        XCTAssertEqual(updates1.count, 1)
        XCTAssertEqual(updates2.count, 1)

        // Cancel only first
        cancellable1.cancel()
        updates1.removeAll()
        updates2.removeAll()

        await engine.stop()
        XCTAssertEqual(updates1.count, 0, "Cancelled callback should not fire")
        XCTAssertEqual(updates2.count, 0, "An idempotent stop does not publish another transition")
    }

    func testConcurrentSyncNowCallersEachCompleteTheirOwnCycle() async throws {
        let pullCount = OSAllocatedUnfairLock(initialState: 0)
        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectJSON)
            }
            if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_concurrent"))
            }
            if path.hasSuffix("/sync/pull") {
                let count = pullCount.withLock { count in
                    count += 1
                    return count
                }
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_explicit_\(count)"))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let collector = TransportObservationCollector()
        let (engine, _) = try makeIntegrationEnv(transportObservationCollector: collector)
        addTeardownBlock { await engine.stop() }
        try await engine.start()
        pullCount.withLock { $0 = 0 }
        try collector.armPause(for: .pull)

        let first = Task { try await engine.syncNow() }
        try await collector.awaitPause(for: .pull, timeout: 1)
        try collector.armPause(for: .pull)
        let second = Task { try await engine.syncNow() }
        try collector.resumePause()

        try await collector.awaitPause(for: .pull, timeout: 1)
        try collector.resumePause()
        try await first.value
        try await second.value
        XCTAssertEqual(pullCount.withLock { $0 }, 2)
    }

    func testStopCancelsInFlightCycleWork() async throws {
        let cycleFinished = XCTestExpectation(description: "cancelled cycle finished")
        let pullCount = OSAllocatedUnfairLock(initialState: 0)

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectJSON)
            }
            if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_stop"))
            }
            if path.hasSuffix("/sync/pull") {
                let count = pullCount.withLock { count in
                    count += 1
                    return count
                }
                let cursor = count == 1 ? "scope_cursor_stop_initial" : "scope_cursor_stop_explicit"
                return try self.mockResponse(json: self.scopePullJSON(cursor: cursor))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let collector = TransportObservationCollector()
        let (engine, database) = try makeIntegrationEnv(transportObservationCollector: collector)
        try await engine.start()
        try collector.armPause(for: .pull)
        let cycle = Task {
            defer { cycleFinished.fulfill() }
            try? await engine.syncNow()
        }
        try await collector.awaitPause(for: .pull, timeout: 1)
        await engine.stop()
        await fulfillment(of: [cycleFinished], timeout: 1)
        _ = await cycle.result
        XCTAssertEqual(pullCount.withLock { $0 }, 2)
        let cursor = try database.readTransaction { db in
            try SynchroMeta.getScope(db, scopeID: self.scopeID)?.cursor
        }
        XCTAssertEqual(cursor, "scope_cursor_stop_initial")
    }

    func testShutdownCancelsCallerOwnedHTTPWorkBeforeDatabaseClose() async throws {
        let pullStarted = XCTestExpectation(description: "caller-owned pull started")
        let shutdownStarted = XCTestExpectation(description: "shutdown started")
        let shutdownFinished = XCTestExpectation(description: "shutdown finished")
        let releasePull = DispatchSemaphore(value: 0)
        let closeFinished = OSAllocatedUnfairLock(initialState: false)
        let pullCount = OSAllocatedUnfairLock(initialState: 0)
        let blockPull = OSAllocatedUnfairLock(initialState: false)

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectJSON)
            }
            if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_close"))
            }
            if path.hasSuffix("/sync/pull") {
                let count = pullCount.withLock { count in
                    count += 1
                    return count
                }
                if blockPull.withLock({ $0 }) {
                    pullStarted.fulfill()
                    _ = releasePull.wait(timeout: .now() + 10)
                }
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_close_\(count)"))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, database) = try makeIntegrationEnv()
        try await engine.start()
        blockPull.withLock { $0 = true }
        let callerCycle = Task { try? await engine.syncNow() }
        await fulfillment(of: [pullStarted], timeout: 1)

        let closeTask = Task {
            shutdownStarted.fulfill()
            await engine.shutdown()
            closeFinished.withLock { $0 = true }
            shutdownFinished.fulfill()
        }
        await fulfillment(of: [shutdownStarted], timeout: 1)
        await fulfillment(of: [shutdownFinished], timeout: 1)
        let finishedBeforeRelease = closeFinished.withLock { $0 }
        releasePull.signal()
        await closeTask.value
        XCTAssertTrue(finishedBeforeRelease)
        XCTAssertNoThrow(try database.close())
        _ = await callerCycle.result
    }

    func testImmediateStopStartKeepsNewLifecycleOwnership() async throws {
        let oldConnectStarted = XCTestExpectation(description: "old connect started")
        let releaseOldConnect = DispatchSemaphore(value: 0)
        let connectCount = OSAllocatedUnfairLock(initialState: 0)

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                let count = connectCount.withLock { count in
                    count += 1
                    return count
                }
                if count == 1 {
                    oldConnectStarted.fulfill()
                    _ = releaseOldConnect.wait(timeout: .now() + 2)
                }
                return try self.mockResponse(json: self.connectJSON)
            }
            if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_new"))
            }
            if path.hasSuffix("/sync/pull") {
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_new"))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, _) = try makeIntegrationEnv()
        let oldStart = Task { () -> Bool in
            do {
                try await engine.start()
                return true
            } catch {
                return false
            }
        }
        await fulfillment(of: [oldConnectStarted], timeout: 1)
        await engine.stop()

        let newStart = Task { try await engine.start() }
        try await newStart.value
        releaseOldConnect.signal()
        let oldStartSucceeded = await oldStart.value
        XCTAssertFalse(oldStartSucceeded)
        await engine.stop()
    }

    func testOldBindingInstallationCannotMarkNewGenerationReady() async throws {
        let oldConnectStarted = XCTestExpectation(description: "old connect started")
        let releaseOldConnect = DispatchSemaphore(value: 0)
        let connectCount = OSAllocatedUnfairLock(initialState: 0)
        let pullCount = OSAllocatedUnfairLock(initialState: 0)

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                let count = connectCount.withLock { count in
                    count += 1
                    return count
                }
                if count == 1 {
                    oldConnectStarted.fulfill()
                    _ = releaseOldConnect.wait(timeout: .now() + 2)
                }
                return try self.mockResponse(json: self.connectJSON)
            }
            if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_binding"))
            }
            if path.hasSuffix("/sync/pull") {
                let count = pullCount.withLock { value in
                    value += 1
                    return value
                }
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_binding_\(count)"))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, _) = try makeIntegrationEnv()
        let oldStart = Task { () -> Bool in
            do {
                try await engine.start()
                return true
            } catch {
                return false
            }
        }
        await fulfillment(of: [oldConnectStarted], timeout: 1)
        await engine.stop()
        let newStart = Task { try await engine.start() }
        try await newStart.value
        releaseOldConnect.signal()
        let oldStartSucceeded = await oldStart.value
        XCTAssertFalse(oldStartSucceeded)
        try await engine.syncNow()
        await engine.stop()
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
        addTeardownBlock { await engine.stop() }

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
        XCTAssertEqual(try decodedChecksum(scopes[0].checksum), emptyScopeChecksum)

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
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_2"))
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
        addTeardownBlock { await engine.stop() }

        let schemaManager = SchemaManager(database: db)
        try schemaManager.reconcileLocalSchema(
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            tables: [ordersLocalSchemaTable(includeNotes: false)]
        )
        try db.writeSyncLockedTransaction { db in
            try SynchroMeta.upsertScope(
                db,
                scopeID: self.scopeID,
                cursor: "scope_cursor_1",
                checksum: try checksumJSONString(emptyScopeChecksum)
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
                return try self.mockResponse(json: self.scopePullJSON(cursor: cursor))
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
        addTeardownBlock { await engine.stop() }

        let schemaManager = SchemaManager(database: db)
        try schemaManager.reconcileLocalSchema(
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            tables: [ordersLocalSchemaTable(includeNotes: false)]
        )
        try db.writeTransaction { db in
            try SynchroMeta.upsertScope(
                db,
                scopeID: self.scopeID,
                cursor: "scope_cursor_1",
                checksum: try checksumJSONString(emptyScopeChecksum)
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
                let accepted: [[String: Any]] = try mutations.map { mutation in
                    try self.acceptedPushOutcome(
                        mutation: mutation,
                        updatedAt: "2026-01-01T14:00:00.000000Z",
                        serverVersion: "opaque_server_version_after_push"
                    )
                }
                return try self.mockResponse(json: [
                    "batch_id": body["batch_id"]!,
                    "server_time": "2026-01-01T14:00:00.000Z",
                    "accepted": accepted,
                    "rejected": [] as [Any],
                ])
            } else if path.hasSuffix("/sync/pull") {
                callLog.append("pull")
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_2"))
            } else if path.hasSuffix("/sync/rebuild") {
                callLog.append("rebuild")
                return try self.mockResponse(statusCode: 500, json: ["error": "unexpected rebuild"])
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv()
        addTeardownBlock { await engine.stop() }

        let schemaManager = SchemaManager(database: db)
        try schemaManager.reconcileLocalSchema(
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            tables: [ordersLocalSchemaTable(includeNotes: false)]
        )
        try db.writeTransaction { db in
            try SynchroMeta.upsertScope(
                db,
                scopeID: self.scopeID,
                cursor: "scope_cursor_1",
                checksum: try checksumJSONString(emptyScopeChecksum)
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

    func testDebouncedPushSharesCycleGateWithExplicitSync() async throws {
        let pushCount = OSAllocatedUnfairLock(initialState: 0)

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectResumeJSON)
            } else if path.hasSuffix("/sync/push") {
                pushCount.withLock { count in
                    count += 1
                }
                let body = try JSONSerialization.jsonObject(with: request.bodyData()!) as! [String: Any]
                let mutations = body["mutations"] as! [[String: Any]]
                let accepted = try mutations.map { mutation in
                    try self.acceptedPushOutcome(
                        mutation: mutation,
                        updatedAt: "2026-01-01T14:00:00.000000Z",
                        serverVersion: "debounced-server-version"
                    )
                }
                return try self.mockResponse(json: [
                    "batch_id": body["batch_id"]!,
                    "server_time": "2026-01-01T14:00:00.000Z",
                    "accepted": accepted,
                    "rejected": [] as [Any],
                ])
            } else if path.hasSuffix("/sync/pull") {
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_debounced"))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let collector = TransportObservationCollector()
        let (engine, db) = try makeIntegrationEnv(
            pushDebounce: 0.01,
            transportObservationCollector: collector
        )
        addTeardownBlock {
            try? collector.resumePause()
            await engine.stop()
        }
        try SchemaManager(database: db).reconcileLocalSchema(
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            tables: [ordersLocalSchemaTable(includeNotes: false)]
        )
        try db.writeTransaction { connection in
            try SynchroMeta.upsertScope(
                connection,
                scopeID: self.scopeID,
                cursor: "scope_cursor_1",
                checksum: try self.checksumJSONString(self.emptyScopeChecksum)
            )
        }

        try await engine.start()
        try collector.armPause(for: .push)
        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "debounced", "u1", "2026-01-01T10:00:00.000Z"]
        )
        try await collector.awaitPause(for: .push, timeout: 1)
        let explicitSync = Task { try await engine.syncNow() }
        try collector.resumePause()
        try await explicitSync.value

        let tracker = ChangeTracker(database: db)
        let deadline = Date().addingTimeInterval(2)
        while try tracker.hasPendingChanges(), Date() < deadline {
            try await Task.sleep(nanoseconds: 20_000_000)
        }
        XCTAssertFalse(try tracker.hasPendingChanges())
        XCTAssertEqual(pushCount.withLock { $0 }, 1)
    }

    func testConnectRebuildLocalReconcilesSchemaAndRebuildsExistingScope() async throws {
        var callLog: [String] = []
        let schemaHash = connectRebuildLocalSchemaHash
        let rebuiltRecord = try authoritativeRecord(
            id: "w1",
            shipAddress: "Rebuilt Address",
            notes: "schema rebuild local",
            includeNotes: true,
            updatedAt: "2026-01-01T12:00:00.000000Z",
            serverVersion: "opaque_server_version_rebuild",
            schemaHash: schemaHash
        )
        let rebuiltScopeChecksum = try authoritativeScopeChecksum([rebuiltRecord], schemaHash: schemaHash)

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                callLog.append("connect")
                return try self.mockResponse(json: self.connectRebuildLocalJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                callLog.append("rebuild")
                return try self.mockResponse(json: self.rebuildJSON(
                    records: [rebuiltRecord.json],
                    finalCursor: "scope_cursor_rebuilt",
                    checksum: rebuiltScopeChecksum
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
                    "checksums": [self.scopeID: try self.checksumJSONObject(rebuiltScopeChecksum)],
                ])
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv()
        addTeardownBlock { await engine.stop() }

        let schemaManager = SchemaManager(database: db)
        try schemaManager.reconcileLocalSchema(
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            tables: [ordersLocalSchemaTable(includeNotes: false)]
        )
        try db.writeSyncLockedTransaction { db in
            try SynchroMeta.upsertScope(
                db,
                scopeID: self.scopeID,
                cursor: "scope_cursor_old",
                checksum: try checksumJSONString(emptyScopeChecksum)
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
        XCTAssertEqual(try decodedChecksum(scope?.checksum), rebuiltScopeChecksum)
        XCTAssertEqual(try decodedChecksum(scope?.localChecksum), rebuiltScopeChecksum)

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
                let accepted: [[String: Any]] = try mutations.map { mutation in
                    try self.acceptedPushOutcome(
                        mutation: mutation,
                        updatedAt: "2026-01-01T14:00:00.000000Z",
                        serverVersion: "2026-01-01T14:00:00.000000Z"
                    )
                }
                let json: [String: Any] = [
                    "batch_id": body["batch_id"]!,
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
        addTeardownBlock { await engine.stop() }

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
        XCTAssertEqual(row?["updated_at"] as String?, "2026-01-01T14:00:00.000000Z")
    }

    func testPullAppliesServerRecord() async throws {
        let record = try authoritativeRecord(
            id: "w1",
            shipAddress: "Server Address",
            updatedAt: "2026-01-01T12:00:00.000000Z",
            serverVersion: "sv_1"
        )
        let scopeChecksum = try authoritativeScopeChecksum([record])
        var change = record.json
        change["scope"] = scopeID
        change["op"] = "upsert"

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_1"))
            } else if path.hasSuffix("/sync/pull") {
                let json: [String: Any] = [
                    "changes": [
                        change
                    ],
                    "scope_set_version": 1,
                    "scope_cursors": [self.scopeID: "scope_cursor_2"],
                    "scope_updates": ["add": [] as [Any], "remove": [] as [Any]],
                    "rebuild": [] as [Any],
                    "has_more": false,
                    "checksums": [self.scopeID: try self.checksumJSONObject(scopeChecksum)]
                ]
                return try self.mockResponse(json: json)
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv()
        addTeardownBlock { await engine.stop() }

        try await engine.start()

        let row = try db.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertEqual(row?["ship_address"] as String?, "Server Address")

        let tracker = ChangeTracker(database: db)
        XCTAssertFalse(try tracker.hasPendingChanges())
    }

    func testScopeRemovalDeletesLocalRowWithoutQueueingPendingDelete() async throws {
        var pullCallCount = 0
        let record = try authoritativeRecord(
            id: "w1",
            shipAddress: "Seeded",
            updatedAt: "2026-01-01T12:00:00.000000Z",
            serverVersion: "sv_1"
        )
        let scopeChecksum = try authoritativeScopeChecksum([record])

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(
                    records: [record.json],
                    finalCursor: "scope_cursor_1",
                    checksum: scopeChecksum
                ))
            } else if path.hasSuffix("/sync/pull") {
                pullCallCount += 1
                if pullCallCount == 1 {
                    return try self.mockResponse(json: self.scopePullJSON(
                        cursor: "scope_cursor_2",
                        checksum: scopeChecksum
                    ))
                }
                return try self.mockResponse(json: [
                    "changes": [] as [Any],
                    "scope_set_version": 2,
                    "scope_cursors": [:] as [String: Any],
                    "scope_updates": [
                        "add": [] as [Any],
                        "remove": [self.scopeID]
                    ] as [String: Any],
                    "rebuild": [] as [Any],
                    "has_more": false,
                    "checksums": [:] as [String: Any]
                ])
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv()
        addTeardownBlock { await engine.stop() }

        try await engine.start()
        try await engine.syncNow()

        let row = try db.queryOne("SELECT id FROM orders WHERE id = ?", params: ["w1"])
        XCTAssertNil(row)

        let tracker = ChangeTracker(database: db)
        XCTAssertFalse(try tracker.hasPendingChanges())
    }

    func testPullPagesUntilComplete() async throws {
        var pullCallCount = 0
        let recordOne = try authoritativeRecord(
            id: "w1",
            shipAddress: "Address 1",
            updatedAt: "2026-01-01T12:00:00.000000Z",
            serverVersion: "sv_1"
        )
        let recordTwo = try authoritativeRecord(
            id: "w2",
            shipAddress: "Address 2",
            updatedAt: "2026-01-01T13:00:00.000000Z",
            serverVersion: "sv_2"
        )
        let scopeChecksum = try authoritativeScopeChecksum([recordOne, recordTwo])
        var changeOne = recordOne.json
        changeOne["scope"] = scopeID
        changeOne["op"] = "upsert"
        var changeTwo = recordTwo.json
        changeTwo["scope"] = scopeID
        changeTwo["op"] = "upsert"

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
                            changeOne
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
                            changeTwo
                        ],
                        "scope_set_version": 1,
                        "scope_cursors": [self.scopeID: "scope_cursor_2"],
                        "scope_updates": ["add": [] as [Any], "remove": [] as [Any]],
                        "rebuild": [] as [Any],
                        "has_more": false,
                        "checksums": [self.scopeID: try self.checksumJSONObject(scopeChecksum)]
                    ]
                    return try self.mockResponse(json: json)
                }
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv()
        addTeardownBlock { await engine.stop() }

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

        let scopeRecord = try authoritativeRecord(
            id: "w1",
            shipAddress: "Recovered Address",
            updatedAt: "2026-01-01T12:00:00.000000Z",
            serverVersion: "sv_1"
        )
        let scopeChecksum = try authoritativeScopeChecksum([scopeRecord])

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                rebuildCallCount += 1
                if rebuildCallCount == 1 {
                    return try self.mockResponse(json: self.rebuildJSON(
                        records: [scopeRecord.json],
                        finalCursor: "scope_cursor_1",
                        checksum: scopeChecksum
                    ))
                }
                return try self.mockResponse(json: self.rebuildJSON(
                    records: [scopeRecord.json],
                    finalCursor: "scope_cursor_rebuilt",
                    checksum: scopeChecksum
                ))
            } else if path.hasSuffix("/sync/pull") {
                pullCallCount += 1
                if pullCallCount == 1 {
                    return try self.mockResponse(json: self.scopePullJSON(
                        cursor: "scope_cursor_2",
                        checksum: scopeChecksum
                    ))
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
                    "checksums": [self.scopeID: try self.checksumJSONObject(scopeChecksum)]
                ])
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv()
        addTeardownBlock { await engine.stop() }

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
        XCTAssertEqual(try decodedChecksum(scope?.checksum), scopeChecksum)
        XCTAssertEqual(try decodedChecksum(scope?.localChecksum), scopeChecksum)
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
                XCTAssertEqual(pk["field-id"] as? String, orderID)
                let accepted = try self.acceptedPushOutcome(
                    mutation: mutation,
                    updatedAt: "2026-01-01T15:00:00.000000Z",
                    serverVersion: "2026-01-01T15:00:00.000000Z"
                )
                return try self.mockResponse(json: [
                    "batch_id": json["batch_id"]!,
                    "server_time": "2026-01-01T15:00:00.000Z",
                    "accepted": [accepted],
                    "rejected": [] as [Any],
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
            params: [orderID, "Queued After First Start", "u1", "2026-01-01T10:00:00.000Z"]
        )
        let tracker1 = ChangeTracker(database: db1)
        XCTAssertTrue(try tracker1.hasPendingChanges())

        await engine1.stop()
        try db1.close()

        let (engine2, db2) = try makeIntegrationEnv(dbPath: dbPath, clientID: clientID)
        addTeardownBlock {
            await engine2.stop()
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
        XCTAssertEqual(row?["updated_at"] as? String, "2026-01-01T15:00:00.000000Z")
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
                return try self.mockResponse(json: self.scopePullJSON(cursor: cursor))

            default:
                return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
            }
        }

        let (engine1, db1) = try makeIntegrationEnv(dbPath: dbPath, clientID: clientID)
        try await engine1.start()
        await engine1.stop()
        try db1.close()

        let (engine2, db2) = try makeIntegrationEnv(dbPath: dbPath, clientID: clientID)
        addTeardownBlock {
            await engine2.stop()
            try? db2.close()
        }
        try await engine2.start()

        XCTAssertEqual(rebuildCallCount, 1)
        XCTAssertEqual(resumedKnownCursor, "scope_cursor_2")

        let scope = try db2.readTransaction { db in
            try SynchroMeta.getScope(db, scopeID: self.scopeID)
        }
        XCTAssertEqual(scope?.cursor, "scope_cursor_3")
        XCTAssertEqual(try decodedChecksum(scope?.checksum), emptyScopeChecksum)
        XCTAssertEqual(try decodedChecksum(scope?.localChecksum), emptyScopeChecksum)
    }

    func testPartialRebuildStateSurvivesRestartAndCompletesCleanly() async throws {
        let dbPath = tempDBPath()
        let clientID = "rebuild-restart-device"
        var connectCallCount = 0
        var rebuildCallCount = 0
        var restartedKnownCursor: String?

        let rebuildRecordOne = try authoritativeRecord(
            id: "w1",
            shipAddress: "Address 1",
            updatedAt: "2026-01-01T12:00:00.000000Z",
            serverVersion: "sv_1"
        )
        let rebuildRecordTwo = try authoritativeRecord(
            id: "w2",
            shipAddress: "Address 2",
            updatedAt: "2026-01-01T13:00:00.000000Z",
            serverVersion: "sv_2"
        )
        let recoveredScopeChecksum = try authoritativeScopeChecksum([rebuildRecordOne, rebuildRecordTwo])

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
                        records: [rebuildRecordOne.json],
                        cursor: "page_1",
                        hasMore: true
                    ))
                case 2:
                    return try self.mockResponse(statusCode: 500, json: ["error": "interrupted"])
                default:
                    return try self.mockResponse(json: self.rebuildJSON(
                        records: [rebuildRecordOne.json, rebuildRecordTwo.json],
                        finalCursor: "scope_cursor_recovered",
                        checksum: recoveredScopeChecksum
                    ))
                }

            case _ where path.hasSuffix("/sync/pull"):
                return try self.mockResponse(json: self.scopePullJSON(
                    cursor: "scope_cursor_recovered",
                    checksum: recoveredScopeChecksum
                ))

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
        XCTAssertEqual(interruptedScope?.localChecksum, "")

        await engine1.stop()
        try db1.close()

        let (engine2, db2) = try makeIntegrationEnv(dbPath: dbPath, clientID: clientID)
        addTeardownBlock {
            await engine2.stop()
            try? db2.close()
        }

        try await engine2.retryAfterError()

        XCTAssertNil(restartedKnownCursor)
        XCTAssertEqual(rebuildCallCount, 3)

        let rows = try db2.query("SELECT id FROM orders ORDER BY id", params: nil)
        XCTAssertEqual(rows.map { $0["id"] as! String }, ["w1", "w2"])

        let recoveredScope = try db2.readTransaction { db in
            try SynchroMeta.getScope(db, scopeID: self.scopeID)
        }
        XCTAssertEqual(recoveredScope?.cursor, "scope_cursor_recovered")
        XCTAssertEqual(try decodedChecksum(recoveredScope?.checksum), recoveredScopeChecksum)
        XCTAssertEqual(recoveredScope?.generation, 1)
        XCTAssertEqual(try decodedChecksum(recoveredScope?.localChecksum), recoveredScopeChecksum)

        let tracker = ChangeTracker(database: db2)
        XCTAssertFalse(try tracker.hasPendingChanges())
    }

    func testFinalRebuildReceiptFinalizesAfterRestartWithoutRequestingPage() async throws {
        let dbPath = tempDBPath()
        let finalCursor = "scope_cursor_final"
        let rebuiltRecord = try authoritativeRecord(
            id: "w1",
            shipAddress: "Applied before finality",
            updatedAt: "2026-01-01T12:00:00.000000Z",
            serverVersion: "server-version-final"
        )
        let rebuiltChecksum = try authoritativeScopeChecksum([rebuiltRecord])
        let rebuildRecord = try JSONDecoder.synchroDecoder().decode(
            RebuildRecord.self,
            from: JSONSerialization.data(withJSONObject: rebuiltRecord.json)
        )

        let (engine1, db1) = try makeIntegrationEnv(dbPath: dbPath)
        try SchemaManager(database: db1).reconcileLocalSchema(
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            tables: [ordersLocalSchemaTable(includeNotes: false)]
        )
        try db1.writeTransaction { connection in
            try SynchroMeta.upsertScope(connection, scopeID: scopeID, cursor: nil, checksum: nil)
        }
        let processor = PullProcessor(database: db1)
        let attempt = try processor.beginScopeRebuild(
            scopeID: scopeID,
            clientGeneration: 1,
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            pageLimit: 100,
            syncedTables: [ordersLocalSchemaTable(includeNotes: false)]
        )
        let request = RebuildRequest(
            clientID: "test-device",
            clientGeneration: attempt.clientGeneration,
            schema: SchemaRef(version: attempt.schemaVersion, hash: attempt.schemaHash),
            scope: scopeID,
            rebuildID: attempt.rebuildID,
            cursor: attempt.cursor,
            limit: attempt.pageLimit
        )
        let finalResponse = RebuildResponse(
            scope: scopeID,
            records: [rebuildRecord],
            cursor: nil,
            hasMore: false,
            finalScopeCursor: finalCursor,
            checksum: rebuiltChecksum
        )
        _ = try processor.applyScopeRebuildPage(
            attempt: attempt,
            request: request,
            requestBody: try rebuildRequestBody(request),
            response: finalResponse,
            responseBody: try rebuildResponseBody(finalResponse),
            syncedTables: [ordersLocalSchemaTable(includeNotes: false)]
        )
        XCTAssertNil(try db1.readTransaction { try SynchroMeta.getScope($0, scopeID: scopeID)?.cursor })
        XCTAssertEqual(try db1.query("SELECT * FROM _synchro_rebuild_page_receipts", params: nil).count, 1)
        await engine1.stop()
        try db1.close()

        var rebuildRequestCount = 0
        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectResumeJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                rebuildRequestCount += 1
                return try self.mockResponse(statusCode: 500, json: ["error": "rebuild page must not be requested"])
            } else if path.hasSuffix("/sync/pull") {
                return try self.mockResponse(json: self.scopePullJSON(
                    cursor: finalCursor,
                    checksum: rebuiltChecksum
                ))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine2, db2) = try makeIntegrationEnv(dbPath: dbPath)
        addTeardownBlock {
            await engine2.stop()
            try? db2.close()
        }
        try await engine2.start()

        XCTAssertEqual(rebuildRequestCount, 0)
        XCTAssertEqual(
            try db2.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: ["w1"])?["ship_address"] as String?,
            "Applied before finality"
        )
        let recoveredScope = try db2.readTransaction { connection in
            try SynchroMeta.getScope(connection, scopeID: scopeID)
        }
        XCTAssertEqual(recoveredScope?.cursor, finalCursor)
        XCTAssertEqual(try decodedChecksum(recoveredScope?.checksum), rebuiltChecksum)
        XCTAssertNil(try db2.readTransaction { try SynchroMeta.getRebuildAttempt($0, scopeID: scopeID) })
    }

    func testRebuildRestartRequiredRestartsOnlyAffectedScope() async throws {
        let otherScopeID = "orders:other"
        var rebuildRequestCount = 0
        var rebuildIDs: [String] = []
        var pullRequestCount = 0
        var databaseForHandler: SynchroDatabase!

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                rebuildRequestCount += 1
                let body = try XCTUnwrap(
                    JSONSerialization.jsonObject(with: try XCTUnwrap(request.bodyData())) as? [String: Any]
                )
                rebuildIDs.append(try XCTUnwrap(body["rebuild_id"] as? String))
                switch rebuildRequestCount {
                case 1:
                    return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_1"))
                case 2:
                    try databaseForHandler.writeSyncLockedTransaction { connection in
                        try connection.execute(
                            sql: "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                            arguments: ["preserved", "keep this row", "u1", "2026-01-01T10:00:00.000000Z"]
                        )
                        let mutationID = UUID().uuidString.lowercased()
                        try connection.execute(
                            sql: """
                                INSERT INTO _synchro_pending_changes
                                    (mutation_id, capture_uuid, table_id, table_name, record_id, pk_field_id,
                                     pk_logical_type, operation, authored_schema_version, authored_schema_hash,
                                     base_version, client_version, lifecycle_state, source_kind, created_at, updated_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?, 'insert', ?, ?, NULL, ?, 'sealed', 'test', ?, ?)
                                """,
                            arguments: [
                                mutationID, mutationID, "table-orders", "orders", "preserved", "field-id", "string",
                                1, protocolTestSchemaHash, "2026-01-01T10:00:00.000000Z",
                                "2026-01-01T10:00:00.000000Z", "2026-01-01T10:00:00.000000Z",
                            ]
                        )
                    }
                    return try self.mockResponse(statusCode: 409, json: [
                        "error": [
                            "code": "rebuild_restart_required",
                            "message": "stale rebuild",
                            "retryable": false,
                            "scope_id": self.scopeID,
                        ],
                    ])
                default:
                    return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_restarted"))
                }
            } else if path.hasSuffix("/sync/pull") {
                pullRequestCount += 1
                if pullRequestCount == 1 {
                    return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_2"))
                }
                return try self.mockResponse(json: [
                    "changes": [] as [Any],
                    "scope_set_version": 1,
                    "scope_cursors": [otherScopeID: "other_scope_cursor_next"],
                    "scope_updates": ["add": [] as [Any], "remove": [] as [Any]],
                    "rebuild": [self.scopeID],
                    "has_more": false,
                    "checksums": [
                        self.scopeID: try self.checksumJSONObject(self.emptyScopeChecksum),
                        otherScopeID: try self.checksumJSONObject(protocolEmptyScopeChecksum(scopeID: otherScopeID)),
                    ],
                ])
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv()
        databaseForHandler = db
        addTeardownBlock { await engine.stop() }
        try await engine.start()
        try db.writeTransaction { connection in
            try SynchroMeta.upsertScope(
                connection,
                scopeID: otherScopeID,
                cursor: "other_scope_cursor_old",
                checksum: try checksumJSONString(protocolEmptyScopeChecksum(scopeID: otherScopeID))
            )
        }

        try await engine.syncNow()

        XCTAssertEqual(rebuildRequestCount, 3)
        XCTAssertNotEqual(rebuildIDs[1], rebuildIDs[2])
        XCTAssertEqual(
            try db.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: ["preserved"])?["ship_address"] as String?,
            "keep this row"
        )
        XCTAssertTrue(try ChangeTracker(database: db).hasPendingChanges())
        let otherScope = try db.readTransaction { connection in
            try SynchroMeta.getScope(connection, scopeID: otherScopeID)
        }
        XCTAssertEqual(otherScope?.cursor, "other_scope_cursor_next")
        XCTAssertNil(try db.readTransaction { try SynchroMeta.getRebuildAttempt($0, scopeID: scopeID) })
    }

    func testFinalChecksumMismatchStartsNewRebuildAttempt() async throws {
        let rebuiltRecord = try authoritativeRecord(
            id: "w1",
            shipAddress: "checksum recovery",
            updatedAt: "2026-01-01T12:00:00.000000Z",
            serverVersion: "server-version-checksum"
        )
        let rebuiltChecksum = try authoritativeScopeChecksum([rebuiltRecord])
        var rebuildRequestCount = 0
        var rebuildIDs: [String] = []
        var stateBeforeReplacement: LocalScopeState?
        var databaseForHandler: SynchroDatabase!

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                rebuildRequestCount += 1
                let body = try XCTUnwrap(
                    JSONSerialization.jsonObject(with: try XCTUnwrap(request.bodyData())) as? [String: Any]
                )
                rebuildIDs.append(try XCTUnwrap(body["rebuild_id"] as? String))
                if rebuildRequestCount == 1 {
                    return try self.mockResponse(json: self.rebuildJSON(
                        records: [rebuiltRecord.json],
                        finalCursor: "scope_cursor_bad_checksum",
                        checksum: self.emptyScopeChecksum
                    ))
                }
                stateBeforeReplacement = try databaseForHandler.readTransaction { connection in
                    try SynchroMeta.getScope(connection, scopeID: self.scopeID)
                }
                return try self.mockResponse(json: self.rebuildJSON(
                    records: [rebuiltRecord.json],
                    finalCursor: "scope_cursor_recovered",
                    checksum: rebuiltChecksum
                ))
            } else if path.hasSuffix("/sync/pull") {
                return try self.mockResponse(json: self.scopePullJSON(
                    cursor: "scope_cursor_recovered",
                    checksum: rebuiltChecksum
                ))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv()
        databaseForHandler = db
        addTeardownBlock { await engine.stop() }
        try await engine.start()

        XCTAssertEqual(rebuildRequestCount, 2)
        XCTAssertNotEqual(rebuildIDs[0], rebuildIDs[1])
        XCTAssertNil(stateBeforeReplacement?.cursor)
        XCTAssertNil(stateBeforeReplacement?.checksum)
        let scope = try db.readTransaction { connection in
            try SynchroMeta.getScope(connection, scopeID: scopeID)
        }
        XCTAssertEqual(scope?.cursor, "scope_cursor_recovered")
        XCTAssertEqual(try decodedChecksum(scope?.checksum), rebuiltChecksum)
    }

    func testFirstStartBindsClientAndRejectsMismatchBeforeHTTP() async throws {
        let dbPath = tempDBPath()
        let requestCount = OSAllocatedUnfairLock(initialState: 0)
        MockURLProtocol.requestHandler = { request in
            requestCount.withLock { $0 += 1 }
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectJSON)
            }
            if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_1"))
            }
            if path.hasSuffix("/sync/pull") {
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_2"))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (firstEngine, firstDatabase) = try makeIntegrationEnv(
            dbPath: dbPath,
            clientID: "client-one"
        )
        try await firstEngine.start()
        XCTAssertEqual(
            try firstDatabase.readTransaction { try SynchroMeta.get($0, key: .clientServerID) },
            "client-one"
        )
        await firstEngine.stop()
        try firstDatabase.close()

        let (secondEngine, secondDatabase) = try makeIntegrationEnv(
            dbPath: dbPath,
            clientID: "client-two"
        )
        addTeardownBlock {
            await secondEngine.stop()
            try? secondDatabase.close()
        }
        do {
            try await secondEngine.start()
            XCTFail("Expected client binding rejection")
        } catch let error as SynchroError {
            guard case .invalidResponse = error else {
                XCTFail("Expected client binding rejection, got \(error)")
                return
            }
        }

        XCTAssertEqual(requestCount.withLock { $0 }, 3)
        XCTAssertEqual(
            try secondDatabase.readTransaction { try SynchroMeta.get($0, key: .clientServerID) },
            "client-one"
        )
        XCTAssertEqual(
            try secondDatabase.query("SELECT * FROM _synchro_pending_changes", params: nil).count,
            0
        )
    }

    func testSeedConnectSendsInstalledStateAndBindsAfterDisposition() async throws {
        let seedScopeID = "global"
        let seedReceipt = "seed-receipt"
        let scopeSetVersion: Int64 = 7
        let requestBody = OSAllocatedUnfairLock(initialState: String?.none)

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                requestBody.withLock { state in
                    state = String(data: request.bodyData()!, encoding: .utf8)
                }
                return try self.mockResponse(json: [
                    "server_time": "2026-01-01T12:00:00.000Z",
                    "protocol_version": 3,
                    "client_generation": 1,
                    "scope_set_version": scopeSetVersion,
                    "schema": [
                        "version": 1,
                        "hash": protocolTestSchemaHash,
                        "action": "none",
                    ],
                    "scopes": [
                        "add": [] as [Any],
                        "remove": [] as [Any],
                    ],
                    "scope_cursor_updates": [seedScopeID: "seed-cursor"],
                ])
            }
            if path.hasSuffix("/sync/pull") {
                let checksum = try self.checksumJSONObject(protocolEmptyScopeChecksum(scopeID: seedScopeID))
                return try self.mockResponse(json: [
                    "changes": [] as [Any],
                    "scope_set_version": scopeSetVersion,
                    "scope_cursors": [seedScopeID: "seed-cursor-after"],
                    "scope_updates": [
                        "add": [] as [Any],
                        "remove": [] as [Any],
                    ],
                    "rebuild": [] as [Any],
                    "has_more": false,
                    "checksums": [seedScopeID: checksum],
                ])
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv(clientID: "seed-client")
        addTeardownBlock {
            await engine.stop()
            try? db.close()
        }
        try installPortableSeedBootstrapState(
            in: db,
            scopeID: seedScopeID,
            receipt: seedReceipt,
            scopeSetVersion: scopeSetVersion
        )

        try await engine.start()

        let body = try XCTUnwrap(requestBody.withLock { $0 })
        let request = try XCTUnwrap(
            JSONSerialization.jsonObject(with: Data(body.utf8)) as? [String: Any]
        )
        let schema = try XCTUnwrap(request["schema"] as? [String: Any])
        let knownScopes = try XCTUnwrap(request["known_scopes"] as? [String: Any])
        let globalScope = try XCTUnwrap(knownScopes[seedScopeID] as? [String: Any])
        XCTAssertEqual(schema["version"] as? Int, 1)
        XCTAssertEqual(schema["hash"] as? String, protocolTestSchemaHash)
        XCTAssertEqual(request["scope_set_version"] as? Int, Int(scopeSetVersion))
        XCTAssertNil(request["client_generation"])
        XCTAssertTrue(globalScope["cursor"] is NSNull)
        XCTAssertEqual(request["seed_receipts"] as? [String: String], [seedScopeID: seedReceipt])

        try db.readTransaction { connection in
            XCTAssertEqual(try SynchroMeta.get(connection, key: .clientServerID), "seed-client")
            XCTAssertTrue(try SynchroMeta.getSeedReceipts(connection).isEmpty)
            XCTAssertEqual(try SynchroMeta.getScope(connection, scopeID: seedScopeID)?.cursor, "seed-cursor-after")
            XCTAssertEqual(try SynchroMeta.getInt64(connection, key: .clientGeneration), 1)
        }
    }

    func testSeedConnectWithoutDispositionKeepsReceiptsAndBindingUnset() async throws {
        let seedScopeID = "global"
        let seedReceipt = "seed-receipt"
        let scopeSetVersion: Int64 = 7

        MockURLProtocol.requestHandler = { request in
            guard request.url!.path.hasSuffix("/sync/connect") else {
                return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
            }
            return try self.mockResponse(json: [
                "server_time": "2026-01-01T12:00:00.000Z",
                "protocol_version": 3,
                "client_generation": 1,
                "scope_set_version": scopeSetVersion,
                "schema": [
                    "version": 1,
                    "hash": protocolTestSchemaHash,
                    "action": "none",
                ],
                "scopes": [
                    "add": [] as [Any],
                    "remove": [] as [Any],
                ],
                "scope_cursor_updates": [:] as [String: Any],
            ])
        }

        let (engine, db) = try makeIntegrationEnv(clientID: "seed-client")
        addTeardownBlock {
            await engine.stop()
            try? db.close()
        }
        try installPortableSeedBootstrapState(
            in: db,
            scopeID: seedScopeID,
            receipt: seedReceipt,
            scopeSetVersion: scopeSetVersion
        )

        do {
            try await engine.start()
            XCTFail("seed connect without a disposition must fail")
        } catch {
        }

        try db.readTransaction { connection in
            XCTAssertNil(try SynchroMeta.get(connection, key: .clientServerID))
            XCTAssertEqual(try SynchroMeta.getSeedReceipts(connection), [seedScopeID: seedReceipt])
            XCTAssertNil(try SynchroMeta.getScope(connection, scopeID: seedScopeID)?.cursor)
            XCTAssertEqual(try SynchroMeta.getInt64(connection, key: .clientGeneration), 0)
        }
    }

    func testInvalidSeedReceiptIsRemovedBeforeConnectWithoutAffectingOtherScopes() async throws {
        let validScopeID = "global"
        let invalidScopeID = "orders:corrupt"
        let validReceipt = "valid-seed-receipt"
        let invalidReceipt = "invalid-seed-receipt"
        let requestBody = OSAllocatedUnfairLock(initialState: Data?.none)

        MockURLProtocol.requestHandler = { request in
            guard request.url!.path.hasSuffix("/sync/connect") else {
                return try self.mockResponse(statusCode: 400, json: ["error": "unexpected"])
            }
            requestBody.withLock { $0 = request.bodyData() }
            return try self.mockResponse(statusCode: 400, json: ["error": "stop after request capture"])
        }

        let (engine, db) = try makeIntegrationEnv(clientID: "seed-client", maxRetryAttempts: 0)
        addTeardownBlock {
            await engine.stop()
            try? db.close()
        }
        try installPortableSeedBootstrapState(
            in: db,
            scopeID: validScopeID,
            receipt: validReceipt,
            scopeSetVersion: 0
        )

        let validChecksum = try checksumJSONString(protocolEmptyScopeChecksum(scopeID: validScopeID))
        let invalidChecksum = validChecksum
        try db.writeTransaction { connection in
            try SynchroMeta.upsertScope(
                connection,
                scopeID: invalidScopeID,
                cursor: nil,
                checksum: invalidChecksum,
                generation: 0,
                localChecksum: invalidChecksum
            )
            try connection.execute(
                sql: """
                    INSERT INTO _synchro_seed_receipts
                        (scope_id, receipt, schema_version, schema_hash, cardinality, checksum)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                arguments: [invalidScopeID, invalidReceipt, 1, protocolTestSchemaHash, 0, invalidChecksum]
            )
        }

        do {
            try await engine.start()
            XCTFail("the captured connection must fail")
        } catch {
        }

        let requestData = try XCTUnwrap(requestBody.withLock { $0 })
        let request = try XCTUnwrap(
            JSONSerialization.jsonObject(with: requestData) as? [String: Any]
        )
        XCTAssertEqual(request["seed_receipts"] as? [String: String], [validScopeID: validReceipt])

        let knownScopes = try XCTUnwrap(request["known_scopes"] as? [String: Any])
        let invalidScope = try XCTUnwrap(knownScopes[invalidScopeID] as? [String: Any])
        XCTAssertTrue(invalidScope["cursor"] is NSNull)

        try db.readTransaction { connection in
            XCTAssertEqual(
                try SynchroMeta.getSeedReceipts(connection),
                [validScopeID: validReceipt]
            )
            XCTAssertEqual(
                try SynchroMeta.getScope(connection, scopeID: validScopeID)?.checksum,
                validChecksum
            )
            XCTAssertEqual(
                try SynchroMeta.getScope(connection, scopeID: validScopeID)?.generation,
                0
            )
            let invalidScope = try SynchroMeta.getScope(connection, scopeID: invalidScopeID)
            XCTAssertNil(invalidScope?.cursor)
            XCTAssertNil(invalidScope?.checksum)
            XCTAssertEqual(invalidScope?.localChecksum, "")
            XCTAssertEqual(invalidScope?.generation, 1)
            XCTAssertNil(try SynchroMeta.get(connection, key: .clientServerID))
            XCTAssertEqual(try SynchroMeta.getInt64(connection, key: .clientGeneration), 0)
        }
    }

    func testPortableSeedBootstrapLeavesClientBindingUnset() async throws {
        MockURLProtocol.requestHandler = { request in
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }
        let (engine, db) = try makeIntegrationEnv(clientID: "seed-client")
        addTeardownBlock {
            await engine.stop()
            try? db.close()
        }
        try db.writeTransaction { connection in
            try connection.execute(
                sql: """
                    INSERT INTO _synchro_seed_receipts
                        (scope_id, receipt, schema_version, schema_hash, cardinality, checksum)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                arguments: ["orders:seed", "seed-receipt", 1, protocolTestSchemaHash, 0, "seed-checksum"]
            )
        }

        do {
            try await engine.start()
            XCTFail("Expected connection failure")
        } catch {
        }
        XCTAssertNil(try db.readTransaction { try SynchroMeta.get($0, key: .clientServerID) })
    }

    func testConnectedStateInstallClearsMatchingDurableBackoffWithCommittedState() async throws {
        let (engine, db) = try makeIntegrationEnv()
        addTeardownBlock { await engine.stop() }
        let request = ConnectRequest(
            clientID: "test-device",
            clientGeneration: nil,
            platform: "ios",
            appVersion: "1.0.0",
            protocolVersion: 3,
            schemaReset: nil,
            schema: SchemaRef(version: 0, hash: ""),
            scopeSetVersion: 0,
            knownScopes: [:],
            seedReceipts: nil
        )
        let requestBody = try JSONEncoder.synchroEncoder().encode(request)
        let requestJSON = try XCTUnwrap(String(data: requestBody, encoding: .utf8))
        let response = try JSONDecoder.synchroDecoder().decode(
            ConnectResponse.self,
            from: JSONSerialization.data(withJSONObject: connectJSON)
        )
        try db.writeTransaction { connection in
            try SynchroMeta.upsertBackoffRecord(
                connection,
                record: LocalBackoffRecord(
                    resumeState: .connecting,
                    workIdentity: requestJSON,
                    retryClassification: .network,
                    attemptCount: 1,
                    nextRetryAtMS: 1
                )
            )
        }

        try await engine.installConnectedState(
            response,
            completedConnectRequestBody: requestBody
        )

        XCTAssertEqual(try db.readTransaction { try SynchroMeta.getInt64($0, key: .clientGeneration) }, 1)
        XCTAssertNil(try db.readTransaction { try SynchroMeta.getBackoffRecord($0) })

        let path = db.path
        try db.close()
        let recovered = try SynchroDatabase(path: path)
        defer { try? recovered.close() }
        XCTAssertEqual(try recovered.readTransaction { try SynchroMeta.getInt64($0, key: .clientGeneration) }, 1)
        XCTAssertNil(try recovered.readTransaction { try SynchroMeta.getBackoffRecord($0) })
    }

    func testRetryBackoffPersistsBeforeDelay() async throws {
        var pullRequestBody: String?

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_1"))
            } else if path.hasSuffix("/sync/pull") {
                pullRequestBody = String(data: request.bodyData()!, encoding: .utf8)
                let data = try JSONSerialization.data(withJSONObject: self.retryableTemporaryUnavailableError())
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 503,
                    httpVersion: nil,
                    headerFields: ["Retry-After": "60"]
                )!
                return (response, data)
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv(maxRetryAttempts: 0)
        addTeardownBlock { await engine.stop() }

        try await engine.start()

        let backoff = try db.readTransaction { db in
            try SynchroMeta.getBackoffRecord(db)
        }
        XCTAssertEqual(backoff?.resumeState, .pulling)
        XCTAssertEqual(backoff?.workIdentity, pullRequestBody)
        XCTAssertEqual(backoff?.retryClassification, .http503)
        XCTAssertEqual(backoff?.attemptCount, 1)
        XCTAssertGreaterThan(backoff?.nextRetryAtMS ?? 0, Int64(Date().timeIntervalSince1970 * 1_000))
    }

    func testRestartArmsDurableBackoffDeadlineBeforeReconnect() async throws {
        let dbPath = tempDBPath()
        let clientID = "durable-backoff-restart-device"
        let database = try SynchroDatabase(path: dbPath)
        let connectRequest = ConnectRequest(
            clientID: clientID,
            clientGeneration: nil,
            platform: "ios",
            appVersion: "1.0.0",
            protocolVersion: 3,
            schemaReset: nil,
            schema: SchemaRef(version: 0, hash: ""),
            scopeSetVersion: 0,
            knownScopes: [:],
            seedReceipts: nil
        )
        let requestJSON = String(data: try JSONEncoder.synchroEncoder().encode(connectRequest), encoding: .utf8)!
        let deadline = Int64(Date().timeIntervalSince1970 * 1_000) + 150
        try database.writeTransaction { db in
            try SynchroMeta.upsertBackoffRecord(
                db,
                record: LocalBackoffRecord(
                    resumeState: .connecting,
                    workIdentity: requestJSON,
                    retryClassification: .network,
                    attemptCount: 1,
                    nextRetryAtMS: deadline
                )
            )
        }
        try database.close()

        var firstConnectAt: Date?
        let reconnectStarted = expectation(description: "reconnect begins after the stored deadline")
        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                firstConnectAt = Date()
                reconnectStarted.fulfill()
                return try self.mockResponse(json: self.connectJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_1"))
            } else if path.hasSuffix("/sync/pull") {
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_2"))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, recoveredDatabase) = try makeIntegrationEnv(dbPath: dbPath, clientID: clientID)
        addTeardownBlock {
            await engine.stop()
            try? recoveredDatabase.close()
        }

        let start = Date()
        try await engine.start()

        XCTAssertLessThan(Date().timeIntervalSince(start), 0.1)
        XCTAssertNil(firstConnectAt)
        await fulfillment(of: [reconnectStarted], timeout: 1.0)

        XCTAssertNotNil(firstConnectAt)
        XCTAssertGreaterThanOrEqual(firstConnectAt!.timeIntervalSince(start), 0.1)
        try await waitForBackoffClear(in: recoveredDatabase)
        XCTAssertNil(try recoveredDatabase.readTransaction { db in
            try SynchroMeta.getBackoffRecord(db)
        })
    }

    func testRecoveredPullBackoffReplaysBeforePendingPush() async throws {
        let dbPath = tempDBPath()
        let clientID = "recovered-pull-device"
        let database = try SynchroDatabase(path: dbPath)
        let schemaManager = SchemaManager(database: database)
        try schemaManager.reconcileLocalSchema(
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            tables: [ordersLocalSchemaTable(includeNotes: false)]
        )
        try database.writeTransaction { db in
            try SynchroMeta.upsertScope(
                db,
                scopeID: scopeID,
                cursor: "scope_cursor_1",
                checksum: try checksumJSONString(emptyScopeChecksum)
            )
            try SynchroMeta.setInt64(db, key: .scopeSetVersion, value: 1)
            try SynchroMeta.setInt64(db, key: .clientGeneration, value: 1)
        }
        _ = try database.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "offline", "u1", "2026-01-01T10:00:00.000Z"]
        )
        let request = PullRequest(
            clientID: clientID,
            clientGeneration: 1,
            schema: SchemaRef(version: 1, hash: protocolTestSchemaHash),
            scopeSetVersion: 1,
            scopes: [scopeID: ScopeCursorRef(cursor: "scope_cursor_1")],
            limit: 100
        )
        let requestJSON = String(data: try JSONEncoder.synchroEncoder().encode(request), encoding: .utf8)!
        try database.writeTransaction { db in
            try SynchroMeta.upsertBackoffRecord(
                db,
                record: LocalBackoffRecord(
                    resumeState: .pulling,
                    workIdentity: requestJSON,
                    retryClassification: .network,
                    attemptCount: 1,
                    nextRetryAtMS: Int64(Date().timeIntervalSince1970 * 1_000) - 1
                )
            )
        }
        try database.close()

        var callLog: [String] = []
        var replayedRequestJSON: String?
        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                callLog.append("connect")
                return try self.mockResponse(json: self.connectResumeJSON)
            } else if path.hasSuffix("/sync/push") {
                callLog.append("push")
                return try self.mockResponse(statusCode: 500, json: ["error": "push ran before pull replay"])
            } else if path.hasSuffix("/sync/rebuild") {
                callLog.append("rebuild")
                return try self.mockResponse(statusCode: 500, json: ["error": "unexpected rebuild"])
            } else if path.hasSuffix("/sync/pull") {
                callLog.append("pull")
                replayedRequestJSON = String(data: request.bodyData()!, encoding: .utf8)
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_2"))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, recoveredDatabase) = try makeIntegrationEnv(dbPath: dbPath, clientID: clientID)
        addTeardownBlock {
            await engine.stop()
            try? recoveredDatabase.close()
        }
        try await engine.start()

        XCTAssertEqual(callLog, ["connect", "pull"])
        XCTAssertEqual(replayedRequestJSON, requestJSON)
        XCTAssertNil(try recoveredDatabase.readTransaction { db in
            try SynchroMeta.getBackoffRecord(db)
        })
    }

    func testRecoveredRebuildBackoffReplaysBeforePendingPush() async throws {
        let dbPath = tempDBPath()
        let clientID = "recovered-rebuild-device"
        let database = try SynchroDatabase(path: dbPath)
        let schemaManager = SchemaManager(database: database)
        try schemaManager.reconcileLocalSchema(
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            tables: [ordersLocalSchemaTable(includeNotes: false)]
        )
        try database.writeTransaction { db in
            try SynchroMeta.upsertScope(
                db,
                scopeID: scopeID,
                cursor: nil,
                checksum: nil
            )
            try SynchroMeta.setInt64(db, key: .scopeSetVersion, value: 1)
            try SynchroMeta.setInt64(db, key: .clientGeneration, value: 1)
        }
        let pullProcessor = PullProcessor(database: database)
        let attempt = try pullProcessor.beginScopeRebuild(
            scopeID: scopeID,
            clientGeneration: 1,
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            pageLimit: 100,
            syncedTables: [ordersLocalSchemaTable(includeNotes: false)]
        )
        _ = try database.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "offline", "u1", "2026-01-01T10:00:00.000Z"]
        )
        let request = RebuildRequest(
            clientID: clientID,
            clientGeneration: attempt.clientGeneration,
            schema: SchemaRef(version: attempt.schemaVersion, hash: attempt.schemaHash),
            scope: attempt.scopeID,
            rebuildID: attempt.rebuildID,
            cursor: attempt.cursor,
            limit: attempt.pageLimit
        )
        let requestJSON = String(data: try JSONEncoder.synchroEncoder().encode(request), encoding: .utf8)!
        try database.writeTransaction { db in
            try SynchroMeta.upsertBackoffRecord(
                db,
                record: LocalBackoffRecord(
                    resumeState: .rebuilding,
                    workIdentity: requestJSON,
                    retryClassification: .network,
                    attemptCount: 1,
                    nextRetryAtMS: Int64(Date().timeIntervalSince1970 * 1_000) - 1
                )
            )
        }
        try database.close()

        var callLog: [String] = []
        var replayedRequestJSON: String?
        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                callLog.append("connect")
                return try self.mockResponse(json: self.connectResumeJSON)
            } else if path.hasSuffix("/sync/push") {
                callLog.append("push")
                return try self.mockResponse(statusCode: 500, json: ["error": "push ran before rebuild replay"])
            } else if path.hasSuffix("/sync/rebuild") {
                callLog.append("rebuild")
                replayedRequestJSON = String(data: request.bodyData()!, encoding: .utf8)
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_2"))
            } else if path.hasSuffix("/sync/pull") {
                callLog.append("pull")
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_3"))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, recoveredDatabase) = try makeIntegrationEnv(dbPath: dbPath, clientID: clientID)
        addTeardownBlock {
            await engine.stop()
            try? recoveredDatabase.close()
        }
        try await engine.start()

        XCTAssertEqual(callLog, ["connect", "rebuild", "pull"])
        XCTAssertEqual(replayedRequestJSON, requestJSON)
        XCTAssertNil(try recoveredDatabase.readTransaction { db in
            try SynchroMeta.getBackoffRecord(db)
        })
    }

    func testStopRetainsDurableBackoffRecord() async throws {
        var pullCallCount = 0

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_1"))
            } else if path.hasSuffix("/sync/pull") {
                pullCallCount += 1
                let data = try JSONSerialization.data(withJSONObject: self.retryableTemporaryUnavailableError())
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 503,
                    httpVersion: nil,
                    headerFields: ["Retry-After": "60"]
                )!
                return (response, data)
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv(maxRetryAttempts: 0)
        try await engine.start()
        let beforeStop = try db.readTransaction { db in
            try SynchroMeta.getBackoffRecord(db)
        }

        await engine.stop()

        let afterStop = try db.readTransaction { db in
            try SynchroMeta.getBackoffRecord(db)
        }
        XCTAssertEqual(afterStop, beforeStop)
        XCTAssertEqual(pullCallCount, 1)
    }

    func testPushBackoffStoresSealedBatchIdentity() async throws {
        var failPush = false
        var failedPushRequest: String?

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_1"))
            } else if path.hasSuffix("/sync/push") {
                let body = try JSONSerialization.jsonObject(with: request.bodyData()!) as! [String: Any]
                if failPush {
                    failedPushRequest = String(data: request.bodyData()!, encoding: .utf8)
                    let data = try JSONSerialization.data(withJSONObject: self.retryableTemporaryUnavailableError())
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 503,
                        httpVersion: nil,
                        headerFields: ["Retry-After": "60"]
                    )!
                    return (response, data)
                }
                let mutations = body["mutations"] as! [[String: Any]]
                let accepted = try mutations.map {
                    try self.acceptedPushOutcome(
                        mutation: $0,
                        updatedAt: "2026-01-01T14:00:00.000000Z",
                        serverVersion: "2026-01-01T14:00:00.000000Z"
                    )
                }
                return try self.mockResponse(json: [
                    "batch_id": body["batch_id"]!,
                    "server_time": "2026-01-01T14:00:00.000Z",
                    "accepted": accepted,
                    "rejected": [] as [Any],
                ])
            } else if path.hasSuffix("/sync/pull") {
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_2"))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv(maxRetryAttempts: 0)
        addTeardownBlock { await engine.stop() }
        try await engine.start()
        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z"]
        )
        failPush = true

        do {
            try await engine.syncNow()
            XCTFail("Expected retryable push failure")
        } catch is RetryableError {
        }

        let backoff = try db.readTransaction { db in
            try SynchroMeta.getBackoffRecord(db)
        }
        let sealedBatch = try db.queryOne(
            "SELECT batch_id, request_json FROM _synchro_push_batches WHERE state = 'pending'",
            params: nil
        )
        XCTAssertEqual(backoff?.resumeState, .pushing)
        XCTAssertEqual(backoff?.workIdentity, sealedBatch?["batch_id"] as String?)
        XCTAssertEqual(sealedBatch?["request_json"] as String?, failedPushRequest)
    }

    func testRebuildBackoffStoresExactRequestJSON() async throws {
        var rebuildRequestBody: String?

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                rebuildRequestBody = String(data: request.bodyData()!, encoding: .utf8)
                let data = try JSONSerialization.data(withJSONObject: self.retryableTemporaryUnavailableError())
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 503,
                    httpVersion: nil,
                    headerFields: ["Retry-After": "60"]
                )!
                return (response, data)
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv(maxRetryAttempts: 0)
        addTeardownBlock { await engine.stop() }
        try await engine.start()

        let backoff = try db.readTransaction { db in
            try SynchroMeta.getBackoffRecord(db)
        }
        XCTAssertEqual(backoff?.resumeState, .rebuilding)
        XCTAssertEqual(backoff?.workIdentity, rebuildRequestBody)
        XCTAssertEqual(backoff?.retryClassification, .http503)
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
                    let data = try JSONSerialization.data(withJSONObject: self.retryableTemporaryUnavailableError())
                    let response = HTTPURLResponse(url: request.url!, statusCode: 503, httpVersion: nil,
                                                   headerFields: ["Retry-After": "0.01"])!
                    return (response, data)
                } else {
                    // Second attempt: success
                    let body = try JSONSerialization.jsonObject(with: request.bodyData()!) as! [String: Any]
                    let mutations = body["mutations"] as! [[String: Any]]
                    let accepted: [[String: Any]] = try mutations.map {
                        try self.acceptedPushOutcome(
                            mutation: $0,
                            updatedAt: "2026-01-01T14:00:00.000000Z",
                            serverVersion: "2026-01-01T14:00:00.000000Z"
                        )
                    }
                    let json: [String: Any] = [
                        "batch_id": body["batch_id"]!,
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
        addTeardownBlock { await engine.stop() }

        try await engine.start()

        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "123 Main St", "u1", "2026-01-01T10:00:00.000Z"]
        )

        try await engine.syncNow()

        XCTAssertEqual(pushCallCount, 2)
        let tracker = ChangeTracker(database: db)
        XCTAssertFalse(try tracker.hasPendingChanges())
        XCTAssertNil(try db.readTransaction { db in
            try SynchroMeta.getBackoffRecord(db)
        })
    }

    func testRetryablePushFailurePreservesQueueAcrossRestart() async throws {
        let dbPath = tempDBPath()
        let clientID = "retryable-push-restart-device"
        var pushCallCount = 0
        var connectCallCount = 0
        var shouldFailNextPush = false

        var initialConnect = connectJSON
        initialConnect["schema"] = [
            "version": 1,
            "hash": protocolTestSchemaHash,
            "action": "none",
        ] as [String: Any]
        initialConnect.removeValue(forKey: "schema_definition")

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                connectCallCount += 1
                return try self.mockResponse(json: connectCallCount == 1 ? initialConnect : self.connectResumeJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_1"))
            } else if path.hasSuffix("/sync/push") {
                pushCallCount += 1
                if shouldFailNextPush {
                    shouldFailNextPush = false
                    let data = try JSONSerialization.data(withJSONObject: self.retryableTemporaryUnavailableError())
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
                let accepted: [[String: Any]] = try mutations.map {
                    try self.acceptedPushOutcome(
                        mutation: $0,
                        updatedAt: "2026-01-01T14:00:00.000000Z",
                        serverVersion: "2026-01-01T14:00:00.000000Z"
                    )
                }
                let json: [String: Any] = [
                    "batch_id": body["batch_id"]!,
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
            schemaHash: protocolTestSchemaHash,
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
        await engine1.stop()
        try db1.close()

        let (engine2, db2) = try makeIntegrationEnv(dbPath: dbPath, clientID: clientID, maxRetryAttempts: 0)
        addTeardownBlock {
            await engine2.stop()
            try? db2.close()
        }
        let schemaManager2 = SchemaManager(database: db2)
        try schemaManager2.reconcileLocalSchema(
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            tables: [ordersLocalSchemaTable(includeNotes: false)]
        )

        try await engine2.start()
        try await waitForBackoffClear(in: db2)

        let tracker2 = ChangeTracker(database: db2)
        XCTAssertFalse(try tracker2.hasPendingChanges())
        XCTAssertEqual(pushCallCount, 2)

        let localRow = try db2.queryOne(
            "SELECT ship_address, updated_at FROM orders WHERE id = ?",
            params: ["w1"]
        )
        XCTAssertEqual(localRow?["ship_address"] as? String, "123 Main St")
        XCTAssertEqual(localRow?["updated_at"] as? String, "2026-01-01T14:00:00.000000Z")

        let rejectedAfterRestart = try db2.readTransaction { db in
            try SynchroMeta.listRejectedMutations(db)
        }
        XCTAssertTrue(rejectedAfterRestart.isEmpty)
    }

    func testRenewalRequiredBatchSurvivesRetryableReconnectFailure() async throws {
        var connectCallCount = 0
        var pushCallCount = 0

        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                connectCallCount += 1
                if connectCallCount == 1 {
                    return try self.mockResponse(json: self.connectJSON)
                }
                if connectCallCount == 2 {
                    let data = try JSONSerialization.data(withJSONObject: self.retryableTemporaryUnavailableError())
                    let response = HTTPURLResponse(
                        url: request.url!,
                        statusCode: 503,
                        httpVersion: nil,
                        headerFields: ["Retry-After": "0.01"]
                    )!
                    return (response, data)
                }
                return try self.mockResponse(json: self.connectGenerationRenewedJSON)
            } else if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_1"))
            } else if path.hasSuffix("/sync/push") {
                pushCallCount += 1
                let body = try JSONSerialization.jsonObject(with: request.bodyData()!) as! [String: Any]
                if pushCallCount == 1 {
                    return try self.mockResponse(
                        statusCode: 409,
                        json: [
                            "error": [
                                "code": "client_generation_expired",
                                "message": "generation expired",
                                "retryable": false,
                                "current_client_generation": 2,
                            ] as [String: Any]
                        ]
                    )
                }
                let mutations = body["mutations"] as! [[String: Any]]
                let accepted = try mutations.map {
                    try self.acceptedPushOutcome(
                        mutation: $0,
                        updatedAt: "2026-01-01T14:00:00.000000Z",
                        serverVersion: "server-generation-2"
                    )
                }
                return try self.mockResponse(json: [
                    "batch_id": body["batch_id"]!,
                    "server_time": "2026-01-01T14:00:00.000Z",
                    "accepted": accepted,
                    "rejected": [] as [Any],
                ])
            } else if path.hasSuffix("/sync/pull") {
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_2"))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, db) = try makeIntegrationEnv(maxRetryAttempts: 1)
        addTeardownBlock { await engine.stop() }
        try await engine.start()
        _ = try db.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["w1", "generation renewal", "u1", "2026-01-01T10:00:00.000000Z"]
        )

        try await engine.syncNow()

        XCTAssertEqual(connectCallCount, 3)
        XCTAssertEqual(pushCallCount, 2)
        XCTAssertFalse(try ChangeTracker(database: db).hasPendingChanges())
        let states = try db.query("SELECT state FROM _synchro_push_batches ORDER BY created_at, batch_id", params: nil)
            .compactMap { $0["state"] as String? }
        XCTAssertEqual(Set(states), ["completed", "superseded"])
        XCTAssertEqual(try db.readTransaction { try SynchroMeta.getInt64($0, key: .clientGeneration) }, 2)
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
                    let data = try JSONSerialization.data(withJSONObject: self.retryableTemporaryUnavailableError())
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
        addTeardownBlock { await engine.stop() }

        var statuses: [String] = []
        let _ = engine.onStatusChange { status in
            statuses.append(status.rawValue)
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

        await fulfillment(of: [initialSyncCompleted], timeout: 3.0)

        XCTAssertEqual(pullCallCount, 2)
        XCTAssertTrue(statuses.contains("backoff"))
        XCTAssertEqual(statuses.last, "ready")

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
        addTeardownBlock { await engine.stop() }

        do {
            try await engine.start()
            XCTFail("Expected non-retryable startup failure")
        } catch {
            XCTAssertFalse(error is RetryableError)
        }

        returnSuccess = true
        try await engine.retryAfterError()
    }

    func testConnectUnsupportedFailsExplicitly() async throws {
        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: [
                    "server_time": "2026-01-01T12:00:00.000Z",
                    "protocol_version": 3,
                    "client_generation": 1,
                    "scope_set_version": 1,
                    "schema": [
                        "version": 2,
                        "hash": String(repeating: "f", count: 64),
                        "action": "unsupported",
                        "reason": "unknown_schema_lineage",
                    ],
                    "scopes": [
                        "add": [] as [Any],
                        "remove": [] as [Any],
                    ] as [String: Any],
                    "scope_cursor_updates": [:] as [String: Any],
                ])
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, _) = try makeIntegrationEnv()
        addTeardownBlock { await engine.stop() }

        do {
            try await engine.start()
            XCTFail("Expected unsupported connect schema action failure")
        } catch SynchroError.unsupportedSchema(let reason) {
            XCTAssertEqual(reason, .unknownSchemaLineage)
        } catch {
            XCTFail("Expected unsupportedSchema, got \(error)")
        }
    }

    func testConnectSchemaAndBindingInstallationRollsBackTogether() async throws {
        let (engine, db) = try makeIntegrationEnv()
        let schemaManager = SchemaManager(database: db)
        try schemaManager.reconcileLocalSchema(
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            tables: [ordersLocalSchemaTable(includeNotes: false)]
        )
        try db.writeTransaction { conn in
            try SynchroMeta.upsertScope(
                conn,
                scopeID: scopeID,
                cursor: "scope_cursor_old",
                checksum: nil
            )
            try SynchroMeta.setInt64(conn, key: .scopeSetVersion, value: 1)
            try SynchroMeta.setInt64(conn, key: .clientGeneration, value: 1)
            try conn.execute(sql: """
                CREATE TRIGGER fail_connect_binding_install
                BEFORE INSERT ON _synchro_scopes
                WHEN NEW.scope_id = 'orders:added'
                BEGIN
                    SELECT RAISE(ABORT, 'forced connect binding failure');
                END
                """)
        }

        let responseData = try JSONSerialization.data(withJSONObject: connectRebuildLocalJSON)
        var response = try JSONDecoder.synchroDecoder().decode(ConnectResponse.self, from: responseData)
        response.scopes.add = [ScopeAssignment(id: "orders:added", cursor: nil)]

        do {
            try await engine.installConnectedState(response)
            XCTFail("expected connect installation to fail")
        } catch {
        }

        let columnNames = try db.query("PRAGMA table_info(orders)", params: nil).compactMap { row in
            row["name"] as String?
        }
        XCTAssertFalse(columnNames.contains("notes"))
        try db.readTransaction { conn in
            XCTAssertEqual(try SynchroMeta.getInt64(conn, key: .schemaVersion), 1)
            XCTAssertEqual(try SynchroMeta.get(conn, key: .schemaHash), protocolTestSchemaHash)
            XCTAssertEqual(try SynchroMeta.getInt64(conn, key: .scopeSetVersion), 1)
            XCTAssertEqual(try SynchroMeta.getInt64(conn, key: .clientGeneration), 1)
            XCTAssertEqual(try SynchroMeta.getScope(conn, scopeID: scopeID)?.cursor, "scope_cursor_old")
            XCTAssertNil(try SynchroMeta.getScope(conn, scopeID: "orders:added"))
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
        addTeardownBlock { await engine.stop() }

        var statuses: [String] = []
        let _ = engine.onStatusChange { status in
            statuses.append(status.rawValue)
        }

        try await engine.start()

        XCTAssertEqual(statuses.last, "ready")
        XCTAssertTrue(statuses.contains("connecting"))
        XCTAssertTrue(statuses.contains("rebuilding"))
        XCTAssertTrue(statuses.contains("pulling"))

        statuses.removeAll()
        try await engine.syncNow()
        XCTAssertEqual(statuses, ["pulling", "ready"])

        statuses.removeAll()
        await engine.stop()
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
                let rejected: [[String: Any]] = try mutations.map { mutation in
                    let pk = mutation["pk"] as! [String: Any]
                    let id = pk["field-id"] as! String
                    let record = try self.authoritativeRecord(
                        id: id,
                        shipAddress: "Server Wins",
                        updatedAt: "2026-01-01T15:00:00.000000Z",
                        serverVersion: "2026-01-01T15:00:00.000000Z"
                    )
                    return [
                        "mutation_id": mutation["mutation_id"]!,
                        "table": mutation["table"]!,
                        "pk": pk,
                        "outcome_schema": ["version": 1, "hash": protocolTestSchemaHash],
                        "status": "conflict",
                        "code": "version_conflict",
                        "message": "server version is newer",
                        "server_row": record.json["row"]!,
                        "row_checksum": record.json["row_checksum"]!,
                        "server_version": "2026-01-01T15:00:00.000000Z",
                    ] as [String: Any]
                }
                let json: [String: Any] = [
                    "batch_id": body["batch_id"]!,
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
        addTeardownBlock { await engine.stop() }

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
                        "outcome_schema": ["version": 1, "hash": protocolTestSchemaHash],
                        "status": "rejected_terminal",
                        "code": "policy_rejected",
                        "message": "explicit rejection for inspection",
                    ] as [String: Any]
                }
                return try self.mockResponse(json: [
                    "batch_id": body["batch_id"]!,
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
        XCTAssertNotNil(UUID(uuidString: rejectedBeforeRestart[0].mutationID))
        XCTAssertEqual(rejectedBeforeRestart[0].code, "policy_rejected")
        let rejectedMutationID = rejectedBeforeRestart[0].mutationID

        await engine1.stop()
        try db1.close()

        let (engine2, db2) = try makeIntegrationEnv(dbPath: dbPath, clientID: clientID)
        addTeardownBlock {
            await engine2.stop()
            try? db2.close()
        }
        try await engine2.start()

        let rejectedAfterRestart = try db2.readTransaction { db in
            try SynchroMeta.listRejectedMutations(db)
        }
        XCTAssertEqual(rejectedAfterRestart.count, 1)
        XCTAssertEqual(rejectedAfterRestart[0].mutationID, rejectedMutationID)
        XCTAssertEqual(rejectedAfterRestart[0].message, "explicit rejection for inspection")
        XCTAssertNil(rejectedAfterRestart[0].serverVersion)

        try db2.writeTransaction { connection in
            try SynchroMeta.clearRejectedMutations(connection)
        }
        let cleared = try db2.query("SELECT mutation_id FROM _synchro_rejected_mutations", params: nil)
        XCTAssertTrue(cleared.isEmpty)
    }

    func testSyncNowDuringBackoffRejectsWithoutEnteringError() async throws {
        let pullCount = OSAllocatedUnfairLock(initialState: 0)
        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectJSON)
            }
            if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_backoff"))
            }
            if path.hasSuffix("/sync/pull") {
                pullCount.withLock { $0 += 1 }
                let data = try JSONSerialization.data(withJSONObject: self.retryableTemporaryUnavailableError())
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 503,
                    httpVersion: nil,
                    headerFields: ["Retry-After": "60"]
                )!
                return (response, data)
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, database) = try makeIntegrationEnv(maxRetryAttempts: 0)
        addTeardownBlock { await engine.stop() }
        try await engine.start()
        let before = try database.readTransaction { try SynchroMeta.getBackoffRecord($0) }

        do {
            try await engine.syncNow()
            XCTFail("Expected syncNow to reject while backoff owns the retry")
        } catch SynchroError.notStarted {
        } catch {
            XCTFail("Expected notStarted, got \(error)")
        }

        XCTAssertEqual(engine.getSyncStatus(), .backoff)
        XCTAssertEqual(try database.readTransaction { try SynchroMeta.getBackoffRecord($0) }, before)
        XCTAssertNil(try engine.getBlockingFailure())
        XCTAssertEqual(pullCount.withLock { $0 }, 1)
    }

    func testHugeFiniteRetryAfterSaturatesDurableBackoff() async throws {
        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectJSON)
            }
            if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_huge_backoff"))
            }
            if path.hasSuffix("/sync/pull") {
                let data = try JSONSerialization.data(withJSONObject: self.retryableTemporaryUnavailableError())
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 503,
                    httpVersion: nil,
                    headerFields: ["Retry-After": "1" + String(repeating: "0", count: 308)]
                )!
                return (response, data)
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let (engine, database) = try makeIntegrationEnv(maxRetryAttempts: 0)
        addTeardownBlock { await engine.stop() }
        try await engine.start()

        let backoff = try XCTUnwrap(database.readTransaction {
            try SynchroMeta.getBackoffRecord($0)
        })
        XCTAssertEqual(backoff.nextRetryAtMS, Int64.max)
        XCTAssertEqual(engine.getSyncStatus(), .backoff)
        XCTAssertNil(try engine.getBlockingFailure())
    }

    func testCanonicalProtocolErrorsMapToTypedBlockingFailures() async throws {
        let cases: [(Int, ProtocolErrorCode, SyncFailureCode, SyncRecoveryAction)] = [
            (400, .invalidRequest, .invalidRequest, .retry),
            (400, .invalidSchemaReference, .invalidSchemaReference, .retry),
            (401, .authRequired, .authenticationRequired, .none),
            (409, .idempotencyConflict, .idempotencyConflict, .none),
            (409, .clientRetired, .clientRetired, .none),
            (500, .syncIntegrityFailure, .syncIntegrityFailure, .none),
        ]

        for (status, protocolCode, failureCode, recoveryAction) in cases {
            MockURLProtocol.requestHandler = { request in
                try self.mockResponse(statusCode: status, json: [
                    "error": [
                        "code": protocolCode.rawValue,
                        "message": "canonical protocol failure",
                        "retryable": false,
                    ] as [String: Any],
                ])
            }
            let (engine, database) = try makeIntegrationEnv()

            do {
                try await engine.start()
                XCTFail("Expected canonical protocol failure")
            } catch let SynchroError.protocolError(receivedStatus, receivedCode, _) {
                XCTAssertEqual(receivedStatus, status)
                XCTAssertEqual(receivedCode, protocolCode)
            } catch {
                XCTFail("Expected protocolError, got \(error)")
            }

            XCTAssertEqual(engine.getSyncStatus(), .error)
            let failure = try XCTUnwrap(engine.getBlockingFailure())
            XCTAssertEqual(failure.operation, .connecting)
            XCTAssertEqual(failure.code, failureCode)
            XCTAssertEqual(failure.retryable, false)
            XCTAssertEqual(failure.recoveryAction, recoveryAction)
            XCTAssertEqual(failure.metadata["http_status"], String(status))
            await engine.stop()
            try database.close()
        }
    }

    func testMalformedDurableFailureBecomesPublicErrorState() async throws {
        let requestCount = OSAllocatedUnfairLock(initialState: 0)
        MockURLProtocol.requestHandler = { request in
            requestCount.withLock { $0 += 1 }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }
        let (engine, database) = try makeIntegrationEnv()
        addTeardownBlock { await engine.stop() }
        try database.writeTransaction { connection in
            try connection.execute(sql: """
                INSERT INTO _synchro_blocking_error
                    (singleton, operation, code, retryable, message, recovery_action, metadata_json, created_at)
                VALUES (1, 'opening', 'invalid_response', 0, 'corrupt', 'retry', '{', '2026-01-01T00:00:00Z')
                """)
        }

        do {
            try await engine.start()
            XCTFail("Expected a typed blocking failure")
        } catch let SynchroError.blocked(failure) {
            XCTAssertEqual(failure.operation, .opening)
            XCTAssertEqual(failure.code, .invalidResponse)
            XCTAssertEqual(failure.recoveryAction, .retry)
        } catch {
            XCTFail("Malformed durable state escaped start: \(error)")
        }

        XCTAssertEqual(engine.getSyncStatus(), .error)
        let persisted = try XCTUnwrap(engine.getBlockingFailure())
        XCTAssertEqual(persisted.operation, .opening)
        XCTAssertEqual(persisted.code, .invalidResponse)
        XCTAssertEqual(persisted.recoveryAction, .retry)
        XCTAssertEqual(requestCount.withLock { $0 }, 0)
    }

    func testStopInvalidatesStartDuringMigrationRecoveryPreflight() async throws {
        let (engine, database) = try makeIntegrationEnv()
        let manager = SchemaManager(database: database)
        var sourceManifest = protocolOrdersSchemaManifest(includeNotes: false)
        sourceManifest.schemaHash = try Integrity.schemaManifestHash(sourceManifest)
        var targetManifest = protocolOrdersSchemaManifest(
            includeNotes: true,
            schemaVersion: 2,
            parentSchema: SchemaRef(version: 1, hash: sourceManifest.schemaHash),
            transitionClass: "class_2",
            compatibilityFloor: 1
        )
        targetManifest.schemaHash = try Integrity.schemaManifestHash(targetManifest)
        try manager.createSyncedTables(schema: SchemaResponse(
            schemaVersion: sourceManifest.schemaVersion,
            schemaHash: sourceManifest.schemaHash,
            serverTime: Date(),
            manifest: sourceManifest
        ))
        _ = try manager.prepareMigration(
            targetManifest: targetManifest,
            action: .replace,
            affectedScopes: [],
            scopeCursorUpdates: [:],
            schemaReset: false
        )
        await engine.stop()

        let requestCount = OSAllocatedUnfairLock(initialState: 0)
        MockURLProtocol.requestHandler = { request in
            requestCount.withLock { $0 += 1 }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }
        let writerEntered = expectation(description: "writer blocks migration recovery")
        let writerFinished = expectation(description: "writer releases migration recovery")
        let releaseWriter = DispatchSemaphore(value: 0)
        let writerResult = OSAllocatedUnfairLock(initialState: Optional<Result<Void, Error>>.none)
        DispatchQueue.global().async {
            let result: Result<Void, Error> = Result {
                try database.writeTransaction { _ in
                    writerEntered.fulfill()
                    _ = releaseWriter.wait(timeout: .now() + 2)
                }
            }
            writerResult.withLock { $0 = result }
            writerFinished.fulfill()
        }
        await fulfillment(of: [writerEntered], timeout: 1)

        let localReady = expectation(description: "start reserves its lifecycle before recovery")
        let statuses = OSAllocatedUnfairLock(initialState: [SyncStatus]())
        let statusToken = engine.onStatusChange { status in
            statuses.withLock { $0.append(status) }
            if status == .localReady {
                localReady.fulfill()
            }
        }
        let startTask = Task { try await engine.start() }
        await fulfillment(of: [localReady], timeout: 1)

        DispatchQueue.global().asyncAfter(deadline: .now() + 0.05) {
            releaseWriter.signal()
        }
        await engine.stop()
        await fulfillment(of: [writerFinished], timeout: 1)
        statusToken.cancel()

        switch await startTask.result {
        case .success:
            XCTFail("Stale start completed after stop invalidated its generation")
        case .failure(let error):
            XCTAssertTrue(error is CancellationError)
        }
        try XCTUnwrap(writerResult.withLock { $0 }).get()
        XCTAssertEqual(statuses.withLock { $0 }, [.localReady, .stopped])
        XCTAssertEqual(engine.getSyncStatus(), .stopped)
        XCTAssertEqual(requestCount.withLock { $0 }, 0)
        XCTAssertTrue(try database.query("PRAGMA table_info(orders)", params: nil)
            .contains { ($0["name"] as String?) == "notes" })
        XCTAssertNil(try manager.activeMigration())
    }

    func testBackgroundStopsNetworkAndForegroundResumesDurableWork() async throws {
        let connectCount = OSAllocatedUnfairLock(initialState: 0)
        let pullCount = OSAllocatedUnfairLock(initialState: 0)
        let pushCount = OSAllocatedUnfairLock(initialState: 0)
        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                let count = connectCount.withLock { count in
                    count += 1
                    return count
                }
                return try self.mockResponse(json: count == 1 ? self.connectJSON : self.connectResumeJSON)
            }
            if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_background"))
            }
            if path.hasSuffix("/sync/push") {
                pushCount.withLock { $0 += 1 }
                let body = try JSONSerialization.jsonObject(with: request.bodyData()!) as! [String: Any]
                let mutations = body["mutations"] as! [[String: Any]]
                let accepted = try mutations.map {
                    try self.acceptedPushOutcome(
                        mutation: $0,
                        updatedAt: "2026-01-01T14:00:00.000000Z",
                        serverVersion: "foreground-server-version"
                    )
                }
                return try self.mockResponse(json: [
                    "batch_id": body["batch_id"]!,
                    "server_time": "2026-01-01T14:00:00.000Z",
                    "accepted": accepted,
                    "rejected": [] as [Any],
                ])
            }
            if path.hasSuffix("/sync/pull") {
                let count = pullCount.withLock { count in
                    count += 1
                    return count
                }
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_foreground_\(count)"))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }

        let collector = TransportObservationCollector()
        let (engine, database) = try makeIntegrationEnv(transportObservationCollector: collector)
        addTeardownBlock { await engine.stop() }
        try await engine.start()
        _ = try database.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["queued-order", "before-background", "u1", "2026-01-01T09:00:00.000Z"]
        )
        XCTAssertTrue(try ChangeTracker(database: database).hasPendingChanges())
        await engine.enterBackground()
        XCTAssertEqual(engine.getSyncStatus(), .stopped)
        let requestsBeforeOfflineWrite = (
            connectCount.withLock { $0 },
            pullCount.withLock { $0 },
            pushCount.withLock { $0 }
        )
        let changeObserved = expectation(description: "offline write observers completed")
        let observation = database.onChange(tables: ["orders"]) {
            changeObserved.fulfill()
        }

        _ = try database.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["foreground-order", "offline", "u1", "2026-01-01T10:00:00.000Z"]
        )
        await fulfillment(of: [changeObserved], timeout: 1)
        observation.cancel()
        XCTAssertTrue(try ChangeTracker(database: database).hasPendingChanges())
        try collector.armPause(for: .connect)
        let foreground = Task { try await engine.enterForeground() }
        try await collector.awaitPause(for: .connect, timeout: 1)
        XCTAssertEqual(connectCount.withLock { $0 }, requestsBeforeOfflineWrite.0 + 1)
        XCTAssertEqual(pullCount.withLock { $0 }, requestsBeforeOfflineWrite.1)
        XCTAssertEqual(pushCount.withLock { $0 }, requestsBeforeOfflineWrite.2)
        try collector.resumePause()
        try await foreground.value

        XCTAssertEqual(engine.getSyncStatus(), .ready)
        XCTAssertEqual(connectCount.withLock { $0 }, 2)
        XCTAssertEqual(pushCount.withLock { $0 }, 1)
        XCTAssertFalse(try ChangeTracker(database: database).hasPendingChanges())
    }

    func testPreparedMigrationRecoversBeforeFirstNetworkRequestAfterAbruptReopen() async throws {
        let path = tempDBPath()
        let originalDatabase = try SynchroDatabase(path: path)
        let originalManager = SchemaManager(database: originalDatabase)
        var sourceManifest = protocolOrdersSchemaManifest(includeNotes: false)
        sourceManifest.schemaHash = try Integrity.schemaManifestHash(sourceManifest)
        var targetManifest = protocolOrdersSchemaManifest(
            includeNotes: true,
            schemaVersion: 2,
            parentSchema: SchemaRef(version: 1, hash: sourceManifest.schemaHash),
            transitionClass: "class_2",
            compatibilityFloor: 1
        )
        targetManifest.schemaHash = try Integrity.schemaManifestHash(targetManifest)
        try originalManager.createSyncedTables(schema: SchemaResponse(
            schemaVersion: sourceManifest.schemaVersion,
            schemaHash: sourceManifest.schemaHash,
            serverTime: Date(),
            manifest: sourceManifest
        ))
        _ = try originalManager.prepareMigration(
            targetManifest: targetManifest,
            action: .replace,
            affectedScopes: [],
            scopeCursorUpdates: [:],
            schemaReset: false
        )
        let targetVersion = targetManifest.schemaVersion
        let targetHash = targetManifest.schemaHash

        let (engine, recoveredDatabase) = try makeIntegrationEnv(dbPath: path)
        addTeardownBlock {
            await engine.stop()
            try? recoveredDatabase.close()
            try? originalDatabase.close()
        }
        let connectObservedTarget = OSAllocatedUnfairLock(initialState: false)
        MockURLProtocol.requestHandler = { request in
            guard request.url!.path.hasSuffix("/sync/connect") else {
                return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
            }
            let schemaState = try recoveredDatabase.readTransaction { connection in
                (
                    try SynchroMeta.getInt64(connection, key: .schemaVersion),
                    try SynchroMeta.get(connection, key: .schemaHash),
                    try SynchroMeta.getSchemaMigrationJournal(connection)
                )
            }
            let hasTargetColumn = try recoveredDatabase.query("PRAGMA table_info(orders)", params: nil)
                .contains { ($0["name"] as String?) == "notes" }
            connectObservedTarget.withLock {
                $0 = schemaState.0 == targetVersion
                    && schemaState.1 == targetHash
                    && schemaState.2 == nil
                    && hasTargetColumn
            }
            return try self.mockResponse(json: [
                "server_time": "2026-01-01T12:00:00.000Z",
                "protocol_version": 3,
                "client_generation": 1,
                "scope_set_version": 0,
                "schema": [
                    "version": targetVersion,
                    "hash": targetHash,
                    "action": "none",
                ] as [String: Any],
                "scopes": ["add": [] as [Any], "remove": [] as [Any]],
                "scope_cursor_updates": [:] as [String: Any],
            ])
        }

        try await engine.start()

        XCTAssertTrue(connectObservedTarget.withLock { $0 })
        XCTAssertEqual(engine.getSyncStatus(), .ready)
        XCTAssertNil(try SchemaManager(database: recoveredDatabase).activeMigration())
    }

    func testTypedEventsFollowCommittedSchemaAndRebuildOrder() async throws {
        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                return try self.mockResponse(json: self.connectJSON)
            }
            if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(finalCursor: "scope_cursor_events"))
            }
            if path.hasSuffix("/sync/pull") {
                return try self.mockResponse(json: self.scopePullJSON(cursor: "scope_cursor_events_final"))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }
        let (engine, _) = try makeIntegrationEnv()
        addTeardownBlock { await engine.stop() }
        let events = OSAllocatedUnfairLock(initialState: [String]())
        let eventToken = engine.onEvent { event in
            let name: String
            switch event {
            case .stateChanged(let change):
                name = "state:\(change.from.rawValue)->\(change.to.rawValue)"
            case .schemaApplying(let schema):
                name = "schema_applying:\(schema.action.rawValue)"
            case .schemaApplied(let schema):
                name = "schema_applied:\(schema.action.rawValue)"
            case .rebuildRequested(let rebuild):
                name = "rebuild_requested:\(rebuild.scopeID)"
            case .rebuildCompleted(let rebuild):
                name = "rebuild_completed:\(rebuild.scopeID)"
            case .backoff:
                name = "backoff"
            case .mutationAccepted:
                name = "mutation_accepted"
            case .mutationRejected:
                name = "mutation_rejected"
            case .failure:
                name = "failure"
            }
            events.withLock { $0.append(name) }
        }

        try await engine.start()
        eventToken.cancel()

        XCTAssertEqual(events.withLock { $0 }, [
            "state:local_ready->connecting",
            "state:connecting->schema_applying",
            "schema_applying:replace",
            "schema_applied:replace",
            "state:schema_applying->ready",
            "state:ready->rebuilding",
            "state:rebuilding->rebuilding",
            "rebuild_requested:\(scopeID)",
            "rebuild_completed:\(scopeID)",
            "state:rebuilding->ready",
            "state:ready->pulling",
            "state:pulling->ready",
        ])
    }

    func testSchemaResetPreservesLocalOnlyDataQueueSealedBatchAndRejection() async throws {
        let (engine, database) = try makeIntegrationEnv()
        let manager = SchemaManager(database: database)
        let sourceSchema = SchemaRef(version: 1, hash: protocolTestSchemaHash)
        var targetManifest = protocolOrdersSchemaManifest(
            includeNotes: true,
            schemaVersion: 2,
            parentSchema: sourceSchema,
            transitionClass: "class_4",
            compatibilityFloor: 2
        )
        targetManifest.schemaHash = try Integrity.schemaManifestHash(targetManifest)
        let targetSchema = SchemaRef(version: 2, hash: targetManifest.schemaHash)
        let targetManifestJSON = try JSONSerialization.jsonObject(
            with: JSONEncoder.synchroEncoder().encode(targetManifest)
        ) as! [String: Any]
        let targetRecord = try authoritativeRecord(
            id: "reset-order",
            shipAddress: "offline",
            notes: nil,
            includeNotes: true,
            updatedAt: "2026-01-01T14:00:00.000000Z",
            serverVersion: "reset-server-version",
            schemaHash: targetManifest.schemaHash
        )
        let targetScopeChecksum = try authoritativeScopeChecksum(
            [targetRecord],
            schemaHash: targetManifest.schemaHash
        )
        try manager.reconcileLocalSchema(
            schemaVersion: sourceSchema.version,
            schemaHash: sourceSchema.hash,
            tables: [ordersLocalSchemaTable(includeNotes: false)]
        )
        try database.writeTransaction { connection in
            try SynchroMeta.upsertScope(
                connection,
                scopeID: self.scopeID,
                cursor: "scope_cursor_before_reset",
                checksum: try self.checksumJSONString(self.emptyScopeChecksum)
            )
            try SynchroMeta.setInt64(connection, key: .scopeSetVersion, value: 1)
            try SynchroMeta.setInt64(connection, key: .clientGeneration, value: 1)
        }

        let connectCount = OSAllocatedUnfairLock(initialState: 0)
        let pushCount = OSAllocatedUnfairLock(initialState: 0)
        let pullCount = OSAllocatedUnfairLock(initialState: 0)
        MockURLProtocol.requestHandler = { request in
            let path = request.url!.path
            if path.hasSuffix("/sync/connect") {
                let count = connectCount.withLock { count in
                    count += 1
                    return count
                }
                if count == 1 {
                    return try self.mockResponse(json: self.connectResumeJSON)
                }
                if count == 2 {
                    return try self.mockResponse(json: [
                        "server_time": "2026-01-01T12:00:00.000Z",
                        "protocol_version": 3,
                        "client_generation": 1,
                        "scope_set_version": 1,
                        "schema": [
                            "version": targetSchema.version,
                            "hash": targetSchema.hash,
                            "action": "unsupported",
                            "reason": "unknown_schema_lineage",
                        ] as [String: Any],
                        "scopes": ["add": [] as [Any], "remove": [] as [Any]],
                        "scope_cursor_updates": [:] as [String: Any],
                    ])
                }
                let body = try JSONSerialization.jsonObject(with: request.bodyData()!) as! [String: Any]
                XCTAssertEqual(body["schema_reset"] as? Bool, true)
                let installed = body["schema"] as? [String: Any]
                XCTAssertEqual(installed?["version"] as? Int64, sourceSchema.version)
                XCTAssertEqual(installed?["hash"] as? String, sourceSchema.hash)
                return try self.mockResponse(json: [
                    "server_time": "2026-01-01T12:00:00.000Z",
                    "protocol_version": 3,
                    "client_generation": 1,
                    "scope_set_version": 1,
                    "schema": [
                        "version": targetSchema.version,
                        "hash": targetSchema.hash,
                        "action": "rebuild_local",
                    ] as [String: Any],
                    "scopes": ["add": [] as [Any], "remove": [] as [Any]],
                    "scope_cursor_updates": [self.scopeID: NSNull()],
                    "schema_definition": targetManifestJSON,
                    "affected_scopes": [self.scopeID],
                ])
            }
            if path.hasSuffix("/sync/push") {
                let count = pushCount.withLock { count in
                    count += 1
                    return count
                }
                if count == 1 {
                    return try self.mockResponse(statusCode: 422, json: [
                        "error": [
                            "code": "schema_mismatch",
                            "message": "schema changed",
                            "retryable": false,
                            "current_schema": ["version": targetSchema.version, "hash": targetSchema.hash],
                            "received_schema": ["version": sourceSchema.version, "hash": sourceSchema.hash],
                        ] as [String: Any],
                    ])
                }
                let body = try JSONSerialization.jsonObject(with: request.bodyData()!) as! [String: Any]
                let mutation = try XCTUnwrap((body["mutations"] as? [[String: Any]])?.first)
                return try self.mockResponse(json: [
                    "batch_id": body["batch_id"]!,
                    "server_time": "2026-01-01T14:00:00.000Z",
                    "accepted": [[
                        "mutation_id": mutation["mutation_id"]!,
                        "table": mutation["table"]!,
                        "pk": mutation["pk"]!,
                        "outcome_schema": ["version": targetSchema.version, "hash": targetSchema.hash],
                        "status": "applied",
                        "server_row": targetRecord.json["row"]!,
                        "row_checksum": targetRecord.json["row_checksum"]!,
                        "server_version": "reset-server-version",
                    ] as [String: Any]],
                    "rejected": [] as [Any],
                ])
            }
            if path.hasSuffix("/sync/rebuild") {
                return try self.mockResponse(json: self.rebuildJSON(
                    records: [targetRecord.json],
                    finalCursor: "scope_cursor_after_reset_rebuild",
                    checksum: targetScopeChecksum
                ))
            }
            if path.hasSuffix("/sync/pull") {
                let count = pullCount.withLock { count in
                    count += 1
                    return count
                }
                let checksum = count == 1 ? self.emptyScopeChecksum : targetScopeChecksum
                return try self.mockResponse(json: self.scopePullJSON(
                    cursor: "scope_cursor_reset_pull_\(count)",
                    checksum: checksum
                ))
            }
            return try self.mockResponse(statusCode: 500, json: ["error": "unexpected"])
        }
        addTeardownBlock { await engine.stop() }

        try await engine.start()
        _ = try database.execute(
            "CREATE TABLE local_settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
            params: nil
        )
        _ = try database.execute(
            "INSERT INTO local_settings (key, value) VALUES ('theme', 'dark')",
            params: nil
        )
        _ = try database.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["rejected-order", "retained", "u1", "2026-01-01T09:00:00.000Z"]
        )
        let rejectionTracker = ChangeTracker(database: database)
        let rejectedPending = try XCTUnwrap(
            try rejectionTracker.pendingChanges().first(where: { $0.recordID == "rejected-order" })
        )
        let rejectionID = rejectedPending.mutationID
        let rejected = try makeRejectedMutation(
            mutationID: rejectionID,
            schema: ordersLocalSchemaTable(includeNotes: false),
            pk: ["field-id": AnyCodable("rejected-order")],
            status: .rejectedTerminal,
            code: .policyRejected,
            message: "retained outcome"
        )
        _ = try PushProcessor(database: database, changeTracker: rejectionTracker).applyRejected(
            rejected: [rejected],
            syncedTables: [ordersLocalSchemaTable(includeNotes: false)],
            sentPending: [rejectionID: rejectedPending]
        )
        _ = try database.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
            params: ["reset-order", "offline", "u1", "2026-01-01T10:00:00.000Z"]
        )

        do {
            try await engine.syncNow()
            XCTFail("Expected unsupported schema before explicit reset")
        } catch SynchroError.unsupportedSchema {
        } catch {
            XCTFail("Expected unsupportedSchema, got \(error)")
        }
        XCTAssertEqual(engine.getSyncStatus(), .error)
        XCTAssertTrue(try ChangeTracker(database: database).hasPendingChanges())
        XCTAssertEqual(
            try database.query("SELECT state FROM _synchro_push_batches", params: nil)
                .compactMap { $0["state"] as String? },
            ["renewal_required"]
        )

        try await engine.resetSchemaAndStart()

        XCTAssertEqual(engine.getSyncStatus(), .ready)
        XCTAssertFalse(try ChangeTracker(database: database).hasPendingChanges())
        XCTAssertNil(try engine.getBlockingFailure())
        XCTAssertEqual(
            try database.queryOne(
                "SELECT value FROM local_settings WHERE key = 'theme'",
                params: nil
            )?["value"] as String?,
            "dark"
        )
        XCTAssertEqual(
            try database.readTransaction { try SynchroMeta.listRejectedMutations($0).map(\.mutationID) },
            [rejectionID]
        )
        XCTAssertEqual(
            Set(try database.query("SELECT state FROM _synchro_push_batches", params: nil)
                .compactMap { $0["state"] as String? }),
            ["superseded", "completed"]
        )
        XCTAssertTrue(try database.query("PRAGMA table_info(orders)", params: nil)
            .contains { ($0["name"] as String?) == "notes" })
        XCTAssertEqual(connectCount.withLock { $0 }, 3)
        XCTAssertEqual(pushCount.withLock { $0 }, 2)
    }

    // MARK: - Helpers

    private func installPortableSeedBootstrapState(
        in database: SynchroDatabase,
        scopeID: String,
        receipt: String,
        scopeSetVersion: Int64
    ) throws {
        let tables = [ordersLocalSchemaTable(includeNotes: false)]
        let schemaManager = SchemaManager(database: database)
        try schemaManager.reconcileLocalSchema(
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            tables: tables
        )
        let checksum = protocolEmptyScopeChecksum(scopeID: scopeID)
        let checksumJSON = try checksumJSONString(checksum)
        try database.writeTransaction { db in
            try SynchroMeta.upsertScope(
                db,
                scopeID: scopeID,
                cursor: nil,
                checksum: checksumJSON,
                generation: 0,
                localChecksum: checksumJSON
            )
            try db.execute(
                sql: """
                    INSERT INTO _synchro_seed_receipts
                        (scope_id, receipt, schema_version, schema_hash, cardinality, checksum)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                arguments: [scopeID, receipt, 1, protocolTestSchemaHash, 0, checksumJSON]
            )
            try SynchroMeta.setInt64(db, key: .scopeSetVersion, value: scopeSetVersion)
            try SynchroMeta.setInt64(db, key: .clientGeneration, value: 0)
            try SynchroMeta.set(db, key: .snapshotComplete, value: "1")
        }
    }

    private func waitForBackoffClear(in database: SynchroDatabase) async throws {
        let deadline = Date().addingTimeInterval(3)
        while Date() < deadline {
            if try database.readTransaction({ db in
                try SynchroMeta.getBackoffRecord(db) == nil
            }) {
                return
            }
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        XCTFail("timed out waiting for durable backoff recovery")
    }

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
        maxRetryAttempts: Int = 3,
        pushDebounce: TimeInterval = 0.5,
        transportObservationCollector: TransportObservationCollector? = nil
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
            pushDebounce: pushDebounce,
            maxRetryAttempts: maxRetryAttempts,
            transportObservationCollector: transportObservationCollector
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
            "protocol_version": 3,
            "client_generation": 1,
            "scope_set_version": 1,
            "schema": [
                "version": 1,
                "hash": protocolTestSchemaHash,
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
            "scope_cursor_updates": [:] as [String: Any],
            "schema_definition": [
                "schema_version": 1,
                "schema_hash": protocolTestSchemaHash,
                "parent_schema": NSNull(),
                "transition_class": "initial",
                "compatibility_floor": 1,
                "tables": [
                    [
                        "table_id": "table-orders",
                        "relation_id": "relation-orders",
                        "name": "orders",
                        "primary_key_field_id": "field-id",
                        "lifecycle": [
                            "created_at_field_id": NSNull(),
                            "updated_at_field_id": "field-updated-at",
                            "deleted_at_field_id": "field-deleted-at",
                        ],
                        "composition": "single_scope",
                        "fields": [
                            ["field_id": "field-id", "name": "id", "type": "string", "nullable": false, "writable": false] as [String: Any],
                            ["field_id": "field-ship-address", "name": "ship_address", "type": "string", "nullable": true, "writable": true] as [String: Any],
                            ["field_id": "field-user-id", "name": "user_id", "type": "string", "nullable": false, "writable": true] as [String: Any],
                            ["field_id": "field-updated-at", "name": "updated_at", "type": "datetime", "nullable": false, "writable": false] as [String: Any],
                            ["field_id": "field-deleted-at", "name": "deleted_at", "type": "datetime", "nullable": true, "writable": false] as [String: Any],
                        ],
                        "indexes": []
                    ] as [String: Any]
                ]
            ] as [String: Any]
        ]
    }

    private var connectRebuildLocalJSON: [String: Any] {
        [
            "server_time": "2026-01-01T12:00:00.000Z",
            "protocol_version": 3,
            "client_generation": 1,
            "scope_set_version": 2,
            "schema": [
                "version": 2,
                "hash": connectRebuildLocalSchemaHash,
                "action": "rebuild_local"
            ],
            "scopes": [
                "add": [] as [Any],
                "remove": [] as [Any]
            ],
            "scope_cursor_updates": [scopeID: NSNull()] as [String: Any],
            "schema_definition": [
                "schema_version": 2,
                "schema_hash": connectRebuildLocalSchemaHash,
                "parent_schema": ["version": 1, "hash": protocolTestSchemaHash],
                "transition_class": "class_3",
                "compatibility_floor": 2,
                "tables": [
                    [
                        "table_id": "table-orders",
                        "relation_id": "relation-orders",
                        "name": "orders",
                        "primary_key_field_id": "field-id",
                        "lifecycle": [
                            "created_at_field_id": NSNull(),
                            "updated_at_field_id": "field-updated-at",
                            "deleted_at_field_id": "field-deleted-at",
                        ],
                        "composition": "single_scope",
                        "fields": [
                            ["field_id": "field-id", "name": "id", "type": "string", "nullable": false, "writable": false] as [String: Any],
                            ["field_id": "field-ship-address", "name": "ship_address", "type": "string", "nullable": true, "writable": true] as [String: Any],
                            ["field_id": "field-user-id", "name": "user_id", "type": "string", "nullable": false, "writable": true] as [String: Any],
                            ["field_id": "field-notes", "name": "notes", "type": "string", "nullable": true, "writable": true] as [String: Any],
                            ["field_id": "field-updated-at", "name": "updated_at", "type": "datetime", "nullable": false, "writable": false] as [String: Any],
                            ["field_id": "field-deleted-at", "name": "deleted_at", "type": "datetime", "nullable": true, "writable": false] as [String: Any],
                        ],
                        "indexes": []
                    ] as [String: Any]
                ]
            ] as [String: Any],
            "affected_scopes": [scopeID],
        ]
    }

    private var connectRebuildLocalSchemaHash: String {
        try! Integrity.schemaManifestHash(protocolOrdersSchemaManifest(
            includeNotes: true,
            schemaVersion: 2,
            parentSchema: SchemaRef(version: 1, hash: protocolTestSchemaHash),
            transitionClass: "class_3",
            compatibilityFloor: 2
        ))
    }

    private var connectResumeJSON: [String: Any] {
        [
            "server_time": "2026-01-01T12:00:00.000Z",
            "protocol_version": 3,
            "client_generation": 1,
            "scope_set_version": 1,
            "schema": [
                "version": 1,
                "hash": protocolTestSchemaHash,
                "action": "none",
            ],
            "scopes": [
                "add": [] as [Any],
                "remove": [] as [Any],
            ] as [String: Any],
            "scope_cursor_updates": [:] as [String: Any],
        ]
    }

    private var connectGenerationRenewedJSON: [String: Any] {
        [
            "server_time": "2026-01-01T12:00:00.000Z",
            "protocol_version": 3,
            "client_generation": 2,
            "scope_set_version": 1,
            "schema": [
                "version": 1,
                "hash": protocolTestSchemaHash,
                "action": "none",
            ],
            "scopes": [
                "add": [] as [Any],
                "remove": [] as [Any],
            ] as [String: Any],
            "scope_cursor_updates": [scopeID: NSNull()] as [String: Any],
        ]
    }

    private func rebuildJSON(
        records: [[String: Any]] = [],
        cursor: String? = nil,
        hasMore: Bool = false,
        finalCursor: String? = nil,
        checksum: ChecksumObject? = nil
    ) throws -> [String: Any] {
        [
            "scope": scopeID,
            "records": records,
            "cursor": cursor ?? NSNull(),
            "has_more": hasMore,
            "final_scope_cursor": finalCursor ?? NSNull(),
            "checksum": hasMore ? NSNull() : try checksumJSONObject(checksum ?? emptyScopeChecksum),
        ]
    }

    private func scopePullJSON(
        cursor: String,
        changes: [[String: Any]] = [],
        hasMore: Bool = false,
        rebuild: [String] = [],
        checksum: ChecksumObject? = nil
    ) throws -> [String: Any] {
        var response: [String: Any] = [
            "changes": changes,
            "scope_set_version": 1,
            "scope_cursors": [scopeID: cursor],
            "scope_updates": [
                "add": [] as [Any],
                "remove": [] as [Any],
            ] as [String: Any],
            "rebuild": rebuild,
            "has_more": hasMore,
        ]
        if !hasMore {
            response["checksums"] = [scopeID: try checksumJSONObject(checksum ?? emptyScopeChecksum)]
        }
        return response
    }

    private struct AuthoritativeRecordFixture {
        let json: [String: Any]
        let identity: Data
        let checksum: ChecksumObject
    }

    private var emptyScopeChecksum: ChecksumObject {
        protocolEmptyScopeChecksum(scopeID: scopeID)
    }

    private func authoritativeRecord(
        id: String,
        shipAddress: String,
        userID: String = "u1",
        notes: String? = nil,
        includeNotes: Bool = false,
        updatedAt: String,
        deletedAt: String? = nil,
        serverVersion: String,
        schemaHash: String = protocolTestSchemaHash
    ) throws -> AuthoritativeRecordFixture {
        let schema = ordersLocalSchemaTable(includeNotes: includeNotes)
        let pk = ["field-id": AnyCodable(id)]
        var row: [String: AnyCodable] = [
            "field-id": AnyCodable(id),
            "field-ship-address": AnyCodable(shipAddress),
            "field-user-id": AnyCodable(userID),
            "field-updated-at": AnyCodable(updatedAt),
            "field-deleted-at": deletedAt.map(AnyCodable.init) ?? AnyCodable(NSNull()),
        ]
        if includeNotes {
            row["field-notes"] = notes.map(AnyCodable.init) ?? AnyCodable(NSNull())
        }
        let digest = try Integrity.rowDigest(
            schemaHash: schemaHash,
            table: schema,
            pk: pk,
            row: row,
            serverVersion: serverVersion
        )
        return AuthoritativeRecordFixture(
            json: [
                "table": schema.tableID,
                "pk": anyMap(pk),
                "row": anyMap(row),
                "row_checksum": try checksumJSONObject(digest.checksum),
                "server_version": serverVersion,
            ],
            identity: digest.identity,
            checksum: digest.checksum
        )
    }

    private func authoritativeScopeChecksum(
        _ records: [AuthoritativeRecordFixture],
        schemaHash: String = protocolTestSchemaHash
    ) throws -> ChecksumObject {
        try Integrity.scopeDigest(
            schemaHash: schemaHash,
            scopeID: scopeID,
            entries: records.map { (identity: $0.identity, digest: $0.checksum) }
        )
    }

    private func acceptedPushOutcome(
        mutation: [String: Any],
        updatedAt: String,
        serverVersion: String
    ) throws -> [String: Any] {
        let pk = mutation["pk"] as! [String: Any]
        let columns = mutation["columns"] as? [String: Any] ?? [:]
        var serverRow = columns
        serverRow["field-id"] = pk["field-id"]
        serverRow["field-updated-at"] = updatedAt
        serverRow["field-deleted-at"] = NSNull()
        let rowChecksum = try Integrity.rowDigest(
            schemaHash: protocolTestSchemaHash,
            table: ordersLocalSchemaTable(includeNotes: false),
            pk: anyCodableMap(pk),
            row: anyCodableMap(serverRow),
            serverVersion: serverVersion
        ).checksum
        return [
            "mutation_id": mutation["mutation_id"]!,
            "table": mutation["table"]!,
            "pk": pk,
            "outcome_schema": ["version": 1, "hash": protocolTestSchemaHash],
            "status": "applied",
            "server_row": serverRow,
            "row_checksum": try checksumJSONObject(rowChecksum),
            "server_version": serverVersion,
        ]
    }

    private func checksumJSONObject(_ checksum: ChecksumObject) throws -> [String: Any] {
        let data = try JSONEncoder.synchroEncoder().encode(checksum)
        return try JSONSerialization.jsonObject(with: data) as! [String: Any]
    }

    private func checksumJSONString(_ checksum: ChecksumObject) throws -> String {
        let data = try JSONEncoder.synchroEncoder().encode(checksum)
        return String(data: data, encoding: .utf8)!
    }

    private func rebuildRequestBody(_ request: RebuildRequest) throws -> Data {
        try JSONEncoder.synchroEncoder().encode(request)
    }

    private func rebuildResponseBody(_ response: RebuildResponse) throws -> Data {
        try JSONEncoder.synchroEncoder().encode(response)
    }

    private func decodedChecksum(_ json: String?) throws -> ChecksumObject? {
        guard let json else { return nil }
        return try JSONDecoder.synchroDecoder().decode(ChecksumObject.self, from: Data(json.utf8))
    }

    private func anyMap(_ values: [String: AnyCodable]) -> [String: Any] {
        values.mapValues(\.value)
    }

    private func anyCodableMap(_ values: [String: Any]) -> [String: AnyCodable] {
        values.mapValues(AnyCodable.init)
    }

    private func retryableTemporaryUnavailableError() -> [String: Any] {
        [
            "error": [
                "code": "temporary_unavailable",
                "message": "temporary service outage",
                "retryable": true,
            ] as [String: Any],
        ]
    }

    private func mockResponse(statusCode: Int = 200, json: [String: Any]) throws -> (HTTPURLResponse, Data) {
        let data = try JSONSerialization.data(withJSONObject: json)
        let response = HTTPURLResponse(url: URL(string: "http://test.local")!, statusCode: statusCode, httpVersion: nil, headerFields: nil)!
        return (response, data)
    }

    private func ordersLocalSchemaTable(includeNotes: Bool) -> LocalSchemaTable {
        var columns = [
            LocalSchemaColumn(fieldID: "field-id", name: "id", logicalType: "string", nullable: false, writable: false, precision: nil, scale: nil, sqliteDefaultSQL: nil, isPrimaryKey: true),
            LocalSchemaColumn(fieldID: "field-ship-address", name: "ship_address", logicalType: "string", nullable: true, writable: true, precision: nil, scale: nil, sqliteDefaultSQL: nil, isPrimaryKey: false),
            LocalSchemaColumn(fieldID: "field-user-id", name: "user_id", logicalType: "string", nullable: false, writable: true, precision: nil, scale: nil, sqliteDefaultSQL: nil, isPrimaryKey: false),
            LocalSchemaColumn(fieldID: "field-updated-at", name: "updated_at", logicalType: "datetime", nullable: false, writable: false, precision: nil, scale: nil, sqliteDefaultSQL: nil, isPrimaryKey: false),
            LocalSchemaColumn(fieldID: "field-deleted-at", name: "deleted_at", logicalType: "datetime", nullable: true, writable: false, precision: nil, scale: nil, sqliteDefaultSQL: nil, isPrimaryKey: false),
        ]
        if includeNotes {
            columns.insert(
                LocalSchemaColumn(fieldID: "field-notes", name: "notes", logicalType: "string", nullable: true, writable: true, precision: nil, scale: nil, sqliteDefaultSQL: nil, isPrimaryKey: false),
                at: 3
            )
        }
        return LocalSchemaTable(
            tableID: "table-orders",
            relationID: "relation-orders",
            tableName: "orders",
            primaryKeyFieldID: "field-id",
            createdAtFieldID: nil,
            updatedAtFieldID: "field-updated-at",
            deletedAtFieldID: "field-deleted-at",
            updatedAtColumn: "updated_at",
            deletedAtColumn: "deleted_at",
            composition: .singleScope,
            primaryKey: ["id"],
            columns: columns
        )
    }
}
