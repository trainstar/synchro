import XCTest
import CryptoKit
import Foundation
import GRDB
#if canImport(CommonCrypto)
import CommonCrypto
#endif
@testable @_spi(Inspection) import Synchro

final class IntegrationTests: XCTestCase {
    private var serverURL: URL!
    private var jwtSecret: String!

    override func setUpWithError() throws {
        try super.setUpWithError()
        let urlString = try XCTUnwrap(
            ProcessInfo.processInfo.environment["SYNCHRO_TEST_URL"],
            "SYNCHRO_TEST_URL must be set for integration tests"
        )
        let secret = try XCTUnwrap(
            ProcessInfo.processInfo.environment["SYNCHRO_TEST_JWT_SECRET"],
            "SYNCHRO_TEST_JWT_SECRET must be set for integration tests"
        )
        serverURL = try XCTUnwrap(
            URL(string: urlString),
            "SYNCHRO_TEST_URL must be a valid URL"
        )
        jwtSecret = secret
    }

    private func signTestJWT(userID: String) -> String {
        let header = #"{"alg":"HS256","typ":"JWT"}"#
        let now = Int(Date().timeIntervalSince1970)
        let exp = now + 3600
        let payload = #"{"sub":"\#(userID)","iat":\#(now),"exp":\#(exp)}"#

        let headerB64 = base64URLEncode(Data(header.utf8))
        let payloadB64 = base64URLEncode(Data(payload.utf8))
        let signingInput = "\(headerB64).\(payloadB64)"
        let signature = hmacSHA256(key: Data(jwtSecret.utf8), data: Data(signingInput.utf8))
        return "\(signingInput).\(base64URLEncode(signature))"
    }

    private func base64URLEncode(_ data: Data) -> String {
        data.base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }

    private func hmacSHA256(key: Data, data: Data) -> Data {
        var digest = [UInt8](repeating: 0, count: Int(CC_SHA256_DIGEST_LENGTH))
        key.withUnsafeBytes { keyBytes in
            data.withUnsafeBytes { dataBytes in
                CCHmac(
                    CCHmacAlgorithm(kCCHmacAlgSHA256),
                    keyBytes.baseAddress, key.count,
                    dataBytes.baseAddress, data.count,
                    &digest
                )
            }
        }
        return Data(digest)
    }

    private func tempDBPath() -> String {
        NSTemporaryDirectory() + UUID().uuidString.lowercased() + ".sqlite"
    }

    private func makeConfig(
        userID: String,
        clientID: String = UUID().uuidString.lowercased(),
        dbPath: String,
        syncInterval: TimeInterval = 999,
        pushDebounce: TimeInterval = 0.5,
        transportObservationCollector: TransportObservationCollector? = nil
    ) -> SynchroConfig {
        let token = signTestJWT(userID: userID)
        if let transportObservationCollector {
            return SynchroConfig(
                dbPath: dbPath,
                serverURL: serverURL,
                authProvider: { token },
                clientID: clientID,
                appVersion: "1.0.0",
                syncInterval: syncInterval,
                pushDebounce: pushDebounce,
                maxRetryAttempts: 1,
                transportObservationCollector: transportObservationCollector
            )
        }
        return SynchroConfig(
            dbPath: dbPath,
            serverURL: serverURL,
            authProvider: { token },
            clientID: clientID,
            appVersion: "1.0.0",
            syncInterval: syncInterval,
            pushDebounce: pushDebounce,
            maxRetryAttempts: 1
        )
    }

    private func makeBadTokenConfig(clientID: String = UUID().uuidString.lowercased()) -> SynchroConfig {
        SynchroConfig(
            dbPath: tempDBPath(),
            serverURL: serverURL,
            authProvider: { "bad.token" },
            clientID: clientID,
            appVersion: "1.0.0",
            syncInterval: 999,
            maxRetryAttempts: 1
        )
    }

    private func makeConnectRequest(clientID: String) -> ConnectRequest {
        ConnectRequest(
            clientID: clientID,
            platform: "ios",
            appVersion: "1.0.0",
            protocolVersion: 3,
            schema: .init(version: 0, hash: ""),
            scopeSetVersion: 0,
            knownScopes: [:]
        )
    }

    private func seedOrder(_ client: SynchroClient, userID: String, customerID: String, orderID: String, shipAddress: String, updatedAt: String) throws {
        _ = try client.execute(
            "INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, ?, ?)",
            params: [customerID, userID, "Integration Customer", updatedAt, updatedAt]
        )
        _ = try client.execute(
            "INSERT INTO orders (id, customer_id, user_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, ?, 'pending', 0, 'USD', ?, ?, ?)",
            params: [orderID, customerID, userID, shipAddress, updatedAt, updatedAt]
        )
    }


    private func stopAndClose(_ client: SynchroClient?) async {
        await client?.stop()
        try? await client?.close()
    }

    private func syncAndWaitForScheduledRetry(_ client: SynchroClient) async throws {
        do {
            try await client.syncNow()
        } catch let error as RetryableError where error.classification == .http503 {
            // A real server answers with retryable 503 capture_pending until WAL
            // capture reaches accepted writes. The error names no protocol code,
            // and a retryable 503 admits only capture_pending and
            // temporary_unavailable, so every other failure propagates.
            try await waitForCondition(timeoutNanoseconds: 15_000_000_000) {
                client.getSyncStatus() == .ready
            }
        }
    }

    private func waitForCondition(
        timeoutNanoseconds: UInt64 = 5_000_000_000,
        intervalNanoseconds: UInt64 = 250_000_000,
        condition: @escaping () async throws -> Bool
    ) async throws {
        let deadline = DispatchTime.now().uptimeNanoseconds + timeoutNanoseconds
        while true {
            if try await condition() {
                return
            }
            if DispatchTime.now().uptimeNanoseconds >= deadline {
                XCTFail("timed out waiting for sync condition")
                return
            }
            try await Task.sleep(nanoseconds: intervalNanoseconds)
        }
    }

    func testAuthFailure() async throws {
        let config = makeBadTokenConfig()
        let http = HttpClient(config: config)

        do {
            _ = try await http.connect(request: makeConnectRequest(clientID: config.clientID))
            XCTFail("Expected auth failure")
        } catch let error as SynchroError {
            switch error {
            case .protocolError(let status, let code, _):
                XCTAssertEqual(status, 401)
                XCTAssertEqual(code, .authRequired)
            default:
                XCTFail("Expected authRequired protocol error, got \(error)")
            }
        }
    }

    func testPushPullBetweenTwoClients() async throws {
        let userID = UUID().uuidString.lowercased()
        let clientAConfig = makeConfig(userID: userID, dbPath: tempDBPath(), syncInterval: 0.1)
        let clientBConfig = makeConfig(userID: userID, dbPath: tempDBPath())
        let customerID = UUID().uuidString.lowercased()
        let orderID = UUID().uuidString.lowercased()

        let clientA = try SynchroClient(config: clientAConfig)
        let clientB = try SynchroClient(config: clientBConfig)
        addTeardownBlock {
            await self.stopAndClose(clientA)
            await self.stopAndClose(clientB)
        }

        try await clientA.start()
        try seedOrder(clientA, userID: userID, customerID: customerID, orderID: orderID, shipAddress: #"{"street":"123 Main St"}"#, updatedAt: "2026-01-01T00:00:00.000Z")
        try await syncAndWaitForScheduledRetry(clientA)

        try await clientB.start()
        try await waitForCondition {
            try await self.syncAndWaitForScheduledRetry(clientB)
            let row = try clientB.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: [orderID])
            return (row?["ship_address"] as? String) == #"{"street":"123 Main St"}"#
        }
    }

    func testFreshClientBootstrapsExistingServerState() async throws {
        let userID = UUID().uuidString.lowercased()
        let writerConfig = makeConfig(userID: userID, dbPath: tempDBPath(), syncInterval: 0.1)
        let readerConfig = makeConfig(userID: userID, dbPath: tempDBPath())
        let customerID = UUID().uuidString.lowercased()
        let orderID = UUID().uuidString.lowercased()

        let writer = try SynchroClient(config: writerConfig)
        addTeardownBlock { await self.stopAndClose(writer) }

        try await writer.start()
        try seedOrder(writer, userID: userID, customerID: customerID, orderID: orderID, shipAddress: #"{"street":"Bootstrap Ave"}"#, updatedAt: "2026-01-02T00:00:00.000Z")
        try await syncAndWaitForScheduledRetry(writer)
        await writer.stop()
        try await writer.close()

        let reader = try SynchroClient(config: readerConfig)
        addTeardownBlock { await self.stopAndClose(reader) }
        try await reader.start()
        try await waitForCondition {
            try await self.syncAndWaitForScheduledRetry(reader)
            let row = try reader.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: [orderID])
            return (row?["ship_address"] as? String) == #"{"street":"Bootstrap Ave"}"#
        }
    }

    func testSoftDeletePropagatesBetweenClients() async throws {
        let userID = UUID().uuidString.lowercased()
        let clientAConfig = makeConfig(userID: userID, dbPath: tempDBPath(), syncInterval: 0.1)
        let clientBConfig = makeConfig(userID: userID, dbPath: tempDBPath())
        let customerID = UUID().uuidString.lowercased()
        let orderID = UUID().uuidString.lowercased()

        let clientA = try SynchroClient(config: clientAConfig)
        let clientB = try SynchroClient(config: clientBConfig)
        addTeardownBlock {
            await self.stopAndClose(clientA)
            await self.stopAndClose(clientB)
        }

        try await clientA.start()
        try seedOrder(clientA, userID: userID, customerID: customerID, orderID: orderID, shipAddress: #"{"street":"Delete Me"}"#, updatedAt: "2026-01-03T00:00:00.000Z")
        try await syncAndWaitForScheduledRetry(clientA)

        try await clientB.start()
        try await waitForCondition {
            let row = try clientB.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: [orderID])
            return (row?["ship_address"] as? String) == #"{"street":"Delete Me"}"#
        }

        _ = try clientA.execute(
            "UPDATE orders SET deleted_at = ?, updated_at = ? WHERE id = ?",
            params: ["2026-01-04T00:00:00.000Z", "2026-01-04T00:00:00.000Z", orderID]
        )
        try await syncAndWaitForScheduledRetry(clientA)
        let expectedDeletedAt = try clientA.queryOne(
            "SELECT deleted_at FROM orders WHERE id = ?",
            params: [orderID]
        )?["deleted_at"] as? String
        XCTAssertNotNil(expectedDeletedAt)
        try await waitForCondition {
            try await self.syncAndWaitForScheduledRetry(clientB)
            let row = try clientB.queryOne("SELECT deleted_at FROM orders WHERE id = ?", params: [orderID])
            return (row?["deleted_at"] as? String) == expectedDeletedAt
        }
    }

    func testLocalTriggerUpdatePropagatesToWarmPeer() async throws {
        let userID = UUID().uuidString.lowercased()
        let customerID = UUID().uuidString.lowercased()
        let orderID = UUID().uuidString.lowercased()
        let beforeTrigger = #"{"street":"Before Trigger"}"#
        let triggerValue = #"{"street":"Trigger Ave"}"#
        let shipAddress: (SynchroClient) throws -> String? = { client in
            try client.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: [orderID])?["ship_address"] as String?
        }
        let clientA = try SynchroClient(config: makeConfig(userID: userID, dbPath: tempDBPath()))
        addTeardownBlock {
            await clientA.stop()
            try await clientA.close()
        }
        let clientB = try SynchroClient(config: makeConfig(userID: userID, dbPath: tempDBPath()))
        addTeardownBlock {
            await clientB.stop()
            try await clientB.close()
        }

        try await clientA.start()
        try seedOrder(clientA, userID: userID, customerID: customerID, orderID: orderID, shipAddress: beforeTrigger, updatedAt: "2026-01-05T00:00:00.000Z")
        try await waitForCondition(timeoutNanoseconds: 30_000_000_000) {
            try await self.syncAndWaitForScheduledRetry(clientA)
            return try clientA.pendingChangeCount() == 0
        }

        try await clientB.start()
        try await waitForCondition(timeoutNanoseconds: 30_000_000_000) {
            try await self.syncAndWaitForScheduledRetry(clientB)
            return try shipAddress(clientB) == beforeTrigger
        }
        XCTAssertEqual(try shipAddress(clientB), beforeTrigger)

        _ = try clientA.execute("CREATE TABLE local_address_commands (id TEXT PRIMARY KEY, ship_address TEXT NOT NULL)")
        _ = try clientA.execute("""
            CREATE TRIGGER local_address_apply AFTER UPDATE OF ship_address ON local_address_commands
            BEGIN
                UPDATE orders SET ship_address = NEW.ship_address, updated_at = '2026-01-06T00:00:00.000Z' WHERE id = NEW.id;
            END
            """)
        _ = try clientA.execute("INSERT INTO local_address_commands (id, ship_address) VALUES (?, ?)", params: [orderID, "queued"])
        _ = try clientA.execute(
            "UPDATE local_address_commands SET ship_address = ? WHERE id = ?",
            params: [triggerValue, orderID]
        )
        XCTAssertEqual(try shipAddress(clientA), triggerValue)

        try await waitForCondition(timeoutNanoseconds: 30_000_000_000) {
            try await self.syncAndWaitForScheduledRetry(clientA)
            return try clientA.pendingChangeCount() == 0
        }
        try await waitForCondition(timeoutNanoseconds: 30_000_000_000) {
            try await self.syncAndWaitForScheduledRetry(clientB)
            return try shipAddress(clientB) == triggerValue
        }
        XCTAssertEqual(try shipAddress(clientB), triggerValue)
    }

    func testConcurrentSyncNowCallersEachCompleteTheirOwnCycleAgainstExtension() async throws {
        let collector = TransportObservationCollector()
        let config = makeConfig(
            userID: UUID().uuidString.lowercased(),
            dbPath: tempDBPath(),
            transportObservationCollector: collector
        )
        let client = try SynchroClient(config: config)
        addTeardownBlock { await self.stopAndClose(client) }

        try await client.start()
        let checkpoint = collector.snapshot().sequenceCheckpoint
        try collector.armPause(for: .pull)
        let first = Task { try await client.syncNow() }
        try await collector.awaitPause(for: .pull, timeout: 5)
        try collector.armPause(for: .pull)
        let second = Task { try await client.syncNow() }
        try collector.resumePause()
        try await collector.awaitPause(for: .pull, timeout: 5)
        try collector.resumePause()
        try await first.value
        try await second.value

        XCTAssertEqual(
            collector.snapshot(after: checkpoint).observations.filter { $0.operationClass == .pull }.count,
            2
        )
    }

    func testStopCancelsInFlightCycleWorkAgainstExtension() async throws {
        let collector = TransportObservationCollector()
        let config = makeConfig(
            userID: UUID().uuidString.lowercased(),
            dbPath: tempDBPath(),
            transportObservationCollector: collector
        )
        let client = try SynchroClient(config: config)
        addTeardownBlock { await self.stopAndClose(client) }

        try await client.start()
        let checkpoint = collector.snapshot().sequenceCheckpoint
        try collector.armPause(for: .pull)
        let cycle = Task { try await client.syncNow() }
        try await collector.awaitPause(for: .pull, timeout: 5)
        await client.stop()

        if case .success = await cycle.result {
            XCTFail("stopped cycle completed")
        }
        XCTAssertEqual(client.getSyncStatus(), .stopped)
        XCTAssertEqual(
            collector.snapshot(after: checkpoint).observations.filter { $0.operationClass == .pull }.count,
            1
        )
    }

    func testDebouncedPushSharesCycleGateWithExplicitSyncAgainstExtension() async throws {
        let collector = TransportObservationCollector()
        let userID = UUID().uuidString.lowercased()
        let config = makeConfig(
            userID: userID,
            dbPath: tempDBPath(),
            pushDebounce: 0.01,
            transportObservationCollector: collector
        )
        let client = try SynchroClient(config: config)
        addTeardownBlock { await self.stopAndClose(client) }

        try await client.start()
        let checkpoint = collector.snapshot().sequenceCheckpoint
        try collector.armPause(for: .push)
        try seedOrder(
            client,
            userID: userID,
            customerID: UUID().uuidString.lowercased(),
            orderID: UUID().uuidString.lowercased(),
            shipAddress: #"{"street":"Debounced"}"#,
            updatedAt: "2026-01-05T00:00:00.000Z"
        )
        try await collector.awaitPause(for: .push, timeout: 5)
        let explicitSync = Task { try await client.syncNow() }
        try collector.resumePause()
        try await explicitSync.value
        try await waitForCondition {
            try client.pendingChangeCount() == 0
        }

        let pushes = collector.snapshot(after: checkpoint).observations.filter { $0.operationClass == .push }
        XCTAssertEqual(pushes.count, 1)
        XCTAssertEqual(pushes.first?.statusCode, 200)
        XCTAssertEqual(pushes.first?.requestFacts?.mutationCount, 2)
    }

    func testBackgroundStopsNetworkAndForegroundResumesDurableWorkAgainstExtension() async throws {
        let collector = TransportObservationCollector()
        let userID = UUID().uuidString.lowercased()
        let config = makeConfig(
            userID: userID,
            dbPath: tempDBPath(),
            transportObservationCollector: collector
        )
        let client = try SynchroClient(config: config)
        addTeardownBlock { await self.stopAndClose(client) }

        try await client.start()
        await client.enterBackground()
        XCTAssertEqual(client.getSyncStatus(), .stopped)
        let checkpoint = collector.snapshot().sequenceCheckpoint
        try seedOrder(
            client,
            userID: userID,
            customerID: UUID().uuidString.lowercased(),
            orderID: UUID().uuidString.lowercased(),
            shipAddress: #"{"street":"Foreground"}"#,
            updatedAt: "2026-01-06T00:00:00.000Z"
        )
        try await Task.sleep(nanoseconds: 100_000_000)
        XCTAssertTrue(collector.snapshot(after: checkpoint).observations.isEmpty)

        try collector.armPause(for: .connect)
        let foreground = Task { try await client.enterForeground() }
        try await collector.awaitPause(for: .connect, timeout: 5)
        let paused = collector.snapshot(after: checkpoint).observations
        XCTAssertEqual(paused.map(\.operationClass), [.connect])
        try collector.resumePause()
        try await foreground.value
        try await waitForCondition {
            try client.pendingChangeCount() == 0
        }

        XCTAssertEqual(client.getSyncStatus(), .ready)
        XCTAssertEqual(
            collector.snapshot(after: checkpoint).observations.filter { $0.operationClass == .push }.count,
            1
        )
    }

    func testRealRebuildObservationProvidesBoundedFacts() async throws {
        let collector = TransportObservationCollector()
        let config = makeConfig(
            userID: UUID().uuidString.lowercased(),
            dbPath: tempDBPath(),
            transportObservationCollector: collector
        )
        let client = try SynchroClient(config: config)
        addTeardownBlock { await self.stopAndClose(client) }

        try collector.armPause(for: .rebuild)
        let start = Task { try await client.start() }
        try await collector.awaitPause(for: .rebuild, timeout: 5)
        let observation = try XCTUnwrap(
            collector.snapshot().observations.last(where: { $0.operationClass == .rebuild })
        )
        XCTAssertEqual(observation.statusCode, 200)
        XCTAssertNotNil(observation.requestFacts?.scopeFingerprint)
        XCTAssertNotNil(observation.requestFacts?.rebuildIDFingerprint)
        XCTAssertNotNil(observation.rebuildResponseFacts)
        XCTAssertNil(observation.pullResponseFacts)
        try collector.resumePause()
        try await start.value
    }

    func testRealConnectResponsePauseResumesUnchanged() async throws {
        let collector = TransportObservationCollector()
        let config = makeConfig(
            userID: UUID().uuidString.lowercased(),
            dbPath: tempDBPath(),
            transportObservationCollector: collector
        )
        let client = try SynchroClient(config: config)
        addTeardownBlock { await self.stopAndClose(client) }

        try collector.armPause(for: .connect)
        let start = Task { try await client.start() }
        try await collector.awaitPause(for: .connect, timeout: 5)
        let paused = collector.snapshot().observations
        XCTAssertEqual(paused.count, 1)
        XCTAssertEqual(paused.first?.operationClass, .connect)
        XCTAssertEqual(paused.first?.statusCode, 200)
        XCTAssertEqual(paused.first?.requestFacts?.protocolVersion, 3)
        XCTAssertNil(paused.first?.pullResponseFacts)
        XCTAssertNil(paused.first?.rebuildResponseFacts)
        try collector.resumePause()
        try await start.value
    }

    func testRealConnectPauseCancellationReleasesResponse() async throws {
        let collector = TransportObservationCollector()
        let config = makeConfig(
            userID: UUID().uuidString.lowercased(),
            dbPath: tempDBPath(),
            transportObservationCollector: collector
        )
        let client = try SynchroClient(config: config)
        addTeardownBlock { await self.stopAndClose(client) }

        try collector.armPause(for: .connect)
        let start = Task { try await client.start() }
        try await collector.awaitPause(for: .connect, timeout: 5)
        collector.cancelPauseBarrier()
        if case .success = await start.result {
            XCTFail("cancelled connect completed")
        }
        XCTAssertEqual(collector.snapshot().observations.count, 1)
    }

    func testPushDrainsQueueAboveRequestLimitInSeveralBatchesAgainstExtension() async throws {
        let userID = UUID().uuidString.lowercased()
        let writer = try SynchroClient(config: makeConfig(userID: userID, dbPath: tempDBPath()))
        let reader = try SynchroClient(config: makeConfig(userID: userID, dbPath: tempDBPath()))
        addTeardownBlock {
            await self.stopAndClose(writer)
            await self.stopAndClose(reader)
        }
        try await writer.start()
        await writer.enterBackground()
        let names = Dictionary(uniqueKeysWithValues: (0..<20).map { index in
            (UUID().uuidString.lowercased(), String(format: "%02d", index) + String(repeating: "n", count: 60_000))
        })
        _ = try writer.executeBatch(names.map { customerID, name in
            customerInsert(customerID: customerID, userID: userID, name: name)
        })
        let queueCanonicalOctets = try writer.inspectPendingMutations().reduce(0) { total, pending in
            total + (try pushMeasure(pending).element.canonical)
        }
        XCTAssertGreaterThan(queueCanonicalOctets, PushLimits.maxRequestOctets)

        try await writer.enterForeground()
        try await waitForCondition(timeoutNanoseconds: 30_000_000_000) {
            try writer.pendingChangeCount() == 0
        }

        let capture = try writer.inspectClientStateCapture(maximumRecords: 1)
        XCTAssertGreaterThanOrEqual(capture.sealedBatchCount, 2)
        XCTAssertEqual(capture.mutationOutcomeCount, names.count)
        XCTAssertEqual(capture.rejectedMutationCount, 0)
        XCTAssertTrue(try writer.inspectRetainedMutations().isEmpty)
        XCTAssertEqual(try customerNames(writer, userID: userID), names)
        try await reader.start()
        try await waitForCondition(timeoutNanoseconds: 30_000_000_000) {
            try await self.syncAndWaitForScheduledRetry(reader)
            return try self.customerNames(reader, userID: userID) == names
        }
    }

    func testPushBatchAtRequestLimitAcceptsResponseAboveItAgainstExtension() async throws {
        let userID = UUID().uuidString.lowercased()
        let clientID = UUID().uuidString.lowercased()
        // The long debounce keeps local writes queued until syncNow sends them.
        let writer = try SynchroClient(config: makeConfig(
            userID: userID, clientID: clientID, dbPath: tempDBPath(), pushDebounce: 999
        ))
        let reader = try SynchroClient(config: makeConfig(userID: userID, dbPath: tempDBPath()))
        addTeardownBlock {
            await self.stopAndClose(writer)
            await self.stopAndClose(reader)
        }
        try await writer.start()
        try await syncAndWaitForScheduledRetry(writer)

        let rowCount = 80
        let probeID = UUID().uuidString.lowercased()
        _ = try writer.executeBatch([customerInsert(customerID: probeID, userID: userID, name: "")])
        let probe = try XCTUnwrap(writer.inspectPendingMutations().first { $0.recordID == probeID })
        let element = try pushMeasure(probe).element
        let reserve = try PushLimits.envelopeReserve(
            clientID: clientID,
            batchID: UUID().uuidString.lowercased(),
            schemaHash: probe.authoredSchema.hash,
            atomic: false,
            encoder: JSONEncoder.synchroEncoder()
        )
        // Each row differs from the probe only by fixed-width identifiers and its ASCII
        // name, so the names fill the batch to exactly the client request measure.
        let nameOctets = PushLimits.maxRequestOctets - (rowCount - 1) - max(
            reserve.body + rowCount * element.body,
            reserve.canonical + rowCount * element.canonical
        )
        var names = [probeID: ""]
        for index in 1..<rowCount {
            let length = nameOctets / (rowCount - 1) + (index <= nameOctets % (rowCount - 1) ? 1 : 0)
            names[UUID().uuidString.lowercased()] = String(
                repeating: String(UnicodeScalar(UInt8(97 + index % 26))), count: length
            )
        }
        _ = try writer.executeBatch(names.filter { $0.key != probeID }.map { customerID, name in
            customerInsert(customerID: customerID, userID: userID, name: name)
        })
        let queued = try writer.inspectPendingMutations().enumerated().reduce(reserve) { octets, entry in
            octets.appending(try pushMeasure(entry.element).element, afterElement: entry.offset > 0)
        }
        XCTAssertEqual(max(queued.body, queued.canonical), PushLimits.maxRequestOctets)

        try await syncAndWaitForScheduledRetry(writer)

        XCTAssertEqual(try writer.pendingChangeCount(), 0)
        let capture = try writer.inspectClientStateCapture(maximumRecords: 1)
        XCTAssertEqual(capture.sealedBatchCount, 1)
        XCTAssertEqual(capture.mutationOutcomeCount, rowCount)
        XCTAssertEqual(capture.rejectedMutationCount, 0)
        XCTAssertTrue(try writer.inspectRetainedMutations().isEmpty)
        let request = try XCTUnwrap(writer.queryOne(
            "SELECT length(CAST(request_json AS BLOB)) AS octets FROM _synchro_push_batches", params: nil
        )?["octets"] as Int?)
        XCTAssertLessThanOrEqual(request, PushLimits.maxRequestOctets)
        // The stored outcomes are exact slices of the push response, so their sum is a
        // lower bound of the response octets.
        let outcomes = try XCTUnwrap(writer.queryOne(
            """
            SELECT COUNT(*) AS count, SUM(length(CAST(accepted_json AS BLOB))) AS octets
            FROM _synchro_pending_changes WHERE lifecycle_state = 'accepted'
            """,
            params: nil
        ))
        XCTAssertEqual(outcomes["count"] as Int?, rowCount)
        let responseLowerBound = try XCTUnwrap(outcomes["octets"] as Int?)
        print("request octets = \(request), accepted outcome octets = \(responseLowerBound)")
        XCTAssertGreaterThan(responseLowerBound, PushLimits.maxRequestOctets)
        XCTAssertEqual(try customerNames(writer, userID: userID), names)
        try await reader.start()
        try await waitForCondition(timeoutNanoseconds: 30_000_000_000) {
            try await self.syncAndWaitForScheduledRetry(reader)
            return try self.customerNames(reader, userID: userID) == names
        }
    }

    func testPushAppliesNormalizedMutationLimitAgainstExtension() async throws {
        let userID = UUID().uuidString.lowercased()
        let writer = try SynchroClient(config: makeConfig(userID: userID, dbPath: tempDBPath()))
        let reader = try SynchroClient(config: makeConfig(userID: userID, dbPath: tempDBPath()))
        addTeardownBlock {
            await self.stopAndClose(writer)
            await self.stopAndClose(reader)
        }
        try await writer.start()
        await writer.enterBackground()
        let probeID = UUID().uuidString.lowercased()
        _ = try writer.executeBatch([customerInsert(customerID: probeID, userID: userID, name: "")])
        let probe = try XCTUnwrap(writer.inspectPendingMutations().first { $0.recordID == probeID })
        XCTAssertEqual(probe.authoredFields.filter { $0.value == AnyCodable("") }.count, 1)
        let emptyNameOctets = try pushMeasure(probe).normalizedJSON.count
        let fitID = UUID().uuidString.lowercased()
        let oversizeID = UUID().uuidString.lowercased()
        let laterID = UUID().uuidString.lowercased()
        let fitName = String(repeating: "f", count: PushLimits.maxNormalizedMutationOctets - emptyNameOctets)
        let oversizeName = String(repeating: "o", count: PushLimits.maxNormalizedMutationOctets + 1 - emptyNameOctets)
        _ = try writer.executeBatch([
            customerInsert(customerID: fitID, userID: userID, name: fitName),
            customerInsert(customerID: oversizeID, userID: userID, name: oversizeName),
            customerInsert(customerID: laterID, userID: userID, name: "later row"),
        ])
        let pending = try writer.inspectPendingMutations()
        let fit = try XCTUnwrap(pending.first { $0.recordID == fitID })
        let oversize = try XCTUnwrap(pending.first { $0.recordID == oversizeID })
        XCTAssertEqual(try pushMeasure(fit).normalizedJSON.count, PushLimits.maxNormalizedMutationOctets)
        XCTAssertEqual(try pushMeasure(oversize).normalizedJSON.count, PushLimits.maxNormalizedMutationOctets + 1)

        try await writer.enterForeground()
        try await waitForCondition(timeoutNanoseconds: 30_000_000_000) {
            try writer.pendingChangeCount() == 0
        }

        let retained = try writer.inspectRetainedMutations()
        XCTAssertEqual(retained.map(\.mutationID), [oversize.mutationID])
        XCTAssertEqual(retained.first?.status, .exceedsPushLimit)
        XCTAssertTrue(try writer.inspectRejectedMutations().isEmpty)
        XCTAssertEqual(
            try writer.queryOne(
                "SELECT COUNT(*) AS count FROM _synchro_push_batch_members WHERE mutation_id = ?",
                params: [oversize.mutationID]
            )?["count"] as Int?,
            0
        )
        let expected = [probeID: "", fitID: fitName, laterID: "later row"]
        try await reader.start()
        try await waitForCondition(timeoutNanoseconds: 30_000_000_000) {
            try await self.syncAndWaitForScheduledRetry(reader)
            return try self.customerNames(reader, userID: userID) == expected
        }
    }

    /// A connect that the extension rejects must leave local durable state and
    /// queued intent unchanged. The client contract requires a 400 response to
    /// preserve unresolved local state. Only the blocking-failure record changes.
    func testRejectedConnectPreservesLocalStateAndQueuedIntent() async throws {
        let userID = UUID().uuidString.lowercased()
        let clientID = UUID().uuidString.lowercased()
        let dbPath = tempDBPath()
        let customerID = UUID().uuidString.lowercased()

        let writer = try SynchroClient(config: makeConfig(userID: userID, clientID: clientID, dbPath: dbPath))
        try await writer.start()
        await writer.enterBackground()
        _ = try writer.executeBatch([customerInsert(customerID: customerID, userID: userID, name: "queued before rejection")])
        XCTAssertEqual(try writer.inspectPendingMutations().map(\.recordID), [customerID])
        await writer.stop()
        try await writer.close()
        let connectedGeneration = try localMeta(dbPath, key: "client_generation")

        // A scope that the server never assigned makes the extension reject connect
        // after it has loaded the prior client state.
        let unassignedScope = "user:\(UUID().uuidString.lowercased())"
        try await DatabaseQueue(path: dbPath).write { db in
            try db.execute(sql: "INSERT INTO _synchro_scopes (scope_id) VALUES (?)", arguments: [unassignedScope])
        }
        let beforeRejection = try localDurableState(dbPath)

        let collector = TransportObservationCollector()
        let rejected = try SynchroClient(config: makeConfig(
            userID: userID,
            clientID: clientID,
            dbPath: dbPath,
            transportObservationCollector: collector
        ))
        do {
            try await rejected.start()
            XCTFail("connect with an unassigned scope started")
        } catch {}
        XCTAssertEqual(
            collector.snapshot().observations.map { "\($0.operationClass.rawValue) \($0.statusCode) \($0.errorCode ?? "none")" },
            ["connect 400 invalid_request"]
        )
        let failure = try XCTUnwrap(rejected.getBlockingFailure())
        XCTAssertEqual(failure.code, .invalidRequest)
        await rejected.stop()
        try await rejected.close()
        XCTAssertEqual(try localDurableState(dbPath), beforeRejection)

        try await DatabaseQueue(path: dbPath).write { db in
            try db.execute(sql: "DELETE FROM _synchro_scopes WHERE scope_id = ?", arguments: [unassignedScope])
        }
        let resumed = try SynchroClient(config: makeConfig(userID: userID, clientID: clientID, dbPath: dbPath))
        addTeardownBlock {
            await resumed.stop()
            try await resumed.close()
        }
        XCTAssertEqual(resumed.getSyncStatus(), .error)
        try await resumed.retryAfterError()
        try await waitForCondition(timeoutNanoseconds: 30_000_000_000) {
            try await self.syncAndWaitForScheduledRetry(resumed)
            return try resumed.pendingChangeCount() == 0
        }
        XCTAssertNil(try resumed.getBlockingFailure())
        XCTAssertEqual(try localMeta(dbPath, key: "client_generation"), connectedGeneration)

        let reader = try SynchroClient(config: makeConfig(userID: userID, dbPath: tempDBPath()))
        addTeardownBlock {
            await reader.stop()
            try await reader.close()
        }
        try await reader.start()
        try await waitForCondition(timeoutNanoseconds: 30_000_000_000) {
            try await self.syncAndWaitForScheduledRetry(reader)
            return try self.customerNames(reader, userID: userID) == [customerID: "queued before rejection"]
        }
    }

    private func localMeta(_ dbPath: String, key: String) throws -> String? {
        try DatabaseQueue(path: dbPath).read { db in
            try String.fetchOne(db, sql: "SELECT value FROM _synchro_meta WHERE key = ?", arguments: [key])
        }
    }

    /// Every local row except the blocking-failure record, in a stable order.
    private func localDurableState(_ dbPath: String) throws -> [String] {
        try DatabaseQueue(path: dbPath).read { db in
            let tables = try String.fetchAll(
                db,
                sql: "SELECT name FROM sqlite_master WHERE type = 'table' AND name <> '_synchro_blocking_error' ORDER BY name"
            )
            return try tables.flatMap { table in
                let columns = try String.fetchAll(db, sql: "SELECT name FROM pragma_table_info(?) ORDER BY cid", arguments: [table])
                let row = columns.map { "quote(\"\($0)\")" }.joined(separator: " || '|' || ")
                return try String.fetchAll(db, sql: "SELECT \(row) FROM \"\(table)\"").map { "\(table) \($0)" }.sorted()
            }
        }
    }

    func testRealFloatWireValuesSurviveRebuildPullAndLaterWork() async throws {
        let cases = try floatWireCases()
        let userID = UUID().uuidString.lowercased()
        let token = signTestJWT(userID: userID)
        let writer = try SynchroClient(config: makeConfig(userID: userID, dbPath: tempDBPath()))
        addTeardownBlock {
            await writer.stop()
            try await writer.close()
        }
        let collector = TransportObservationCollector(capacity: 4096)
        // A page limit below the row count requires rebuild continuation.
        let reader = try SynchroClient(config: SynchroConfig(
            dbPath: tempDBPath(),
            serverURL: serverURL,
            authProvider: { token },
            clientID: UUID().uuidString.lowercased(),
            appVersion: "1.0.0",
            syncInterval: 999,
            maxRetryAttempts: 1,
            pullPageSize: 4,
            transportObservationCollector: collector
        ))
        addTeardownBlock {
            await reader.stop()
            try await reader.close()
        }
        let ids = cases.map { _ in UUID().uuidString.lowercased() }

        try await writer.start()
        _ = try writer.executeBatch(try zip(ids, cases).map { id, testCase in
            SQLStatement(
                sql: "INSERT INTO type_zoo (id, user_id, col_text, col_double, created_at, updated_at) VALUES (?, ?, 'float-wire', ?, ?, ?)",
                params: [id, userID, try XCTUnwrap(Double(testCase.source)), "2026-01-09T00:00:00.000Z", "2026-01-09T00:00:00.000Z"]
            )
        })
        try await waitForCondition(timeoutNanoseconds: 30_000_000_000) {
            try await self.syncFloatWire(writer)
            return try writer.pendingChangeCount() == 0
        }
        try await reader.start()
        try await waitForFloatWireRows(reader, userID: userID, ids: ids, cases: cases, text: "float-wire")
        let scopeFingerprint = SHA256.hash(data: Data("user:\(userID)".utf8)).map { String(format: "%02x", $0) }.joined()
        let snapshot = collector.snapshot()
        XCTAssertFalse(snapshot.overflowed)
        let pages = snapshot.observations.compactMap(\.rebuildResponseFacts).filter { $0.scopeFingerprint == scopeFingerprint }
        XCTAssertGreaterThan(pages.count, 1)
        XCTAssertEqual(pages.map(\.recordCount).reduce(0, +), ids.count)
        XCTAssertTrue(pages.dropLast().allSatisfy { $0.hasMore && $0.hasCursor })
        XCTAssertEqual(pages.last?.hasMore, false)

        _ = try writer.executeBatch(ids.map { id in
            SQLStatement(
                sql: "UPDATE type_zoo SET col_text = 'float-wire-later', updated_at = ? WHERE id = ?",
                params: ["2026-01-10T00:00:00.000Z", id]
            )
        })
        try await waitForCondition(timeoutNanoseconds: 30_000_000_000) {
            try await self.syncFloatWire(writer)
            return try writer.pendingChangeCount() == 0
        }
        try await waitForFloatWireRows(reader, userID: userID, ids: ids, cases: cases, text: "float-wire-later")
    }

    /// A rejected response can stop the engine, so a later call reports only cancellation.
    /// The recorded blocking failure keeps the original cause visible.
    private func syncFloatWire(_ client: SynchroClient) async throws {
        do {
            try await syncAndWaitForScheduledRetry(client)
        } catch {
            XCTFail("float wire sync failed: \(error), blocking failure: \(String(describing: try client.getBlockingFailure()))")
            throw error
        }
    }

    /// One shared source binary64 value and its RFC 8785 text.
    private struct FloatWireCase: Decodable {
        let source: String
        let canonical: String
    }

    private func floatWireCases() throws -> [FloatWireCase] {
        struct Document: Decodable {
            let version: Int
            let cases: [FloatWireCase]
        }
        var directory = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
        for _ in 0..<8 {
            let candidate = directory.appendingPathComponent("conformance/protocol/float-wire-boundaries-v1.json")
            if FileManager.default.fileExists(atPath: candidate.path) {
                let document = try JSONDecoder().decode(Document.self, from: Data(contentsOf: candidate))
                XCTAssertEqual(document.version, 1)
                XCTAssertFalse(document.cases.isEmpty)
                return document.cases
            }
            directory.deleteLastPathComponent()
        }
        throw NSError(domain: "IntegrationTests", code: 1, userInfo: [NSLocalizedDescriptionKey: "shared float wire cases not found"])
    }

    /// Syncs until every row has the wanted text, then compares each stored value with its canonical binary64.
    private func waitForFloatWireRows(
        _ client: SynchroClient,
        userID: String,
        ids: [String],
        cases: [FloatWireCase],
        text: String
    ) async throws {
        try await waitForCondition(timeoutNanoseconds: 30_000_000_000) {
            try await self.syncFloatWire(client)
            let rows = try client.query("SELECT col_text FROM type_zoo WHERE user_id = ?", params: [userID])
            return rows.count == ids.count && rows.allSatisfy { ($0["col_text"] as String?) == text }
        }
        for (id, testCase) in zip(ids, cases) {
            let row = try XCTUnwrap(client.queryOne("SELECT col_double FROM type_zoo WHERE id = ?", params: [id]))
            let value: Double = try XCTUnwrap(row["col_double"])
            let expected = try XCTUnwrap(Double(testCase.canonical))
            XCTAssertEqual(value.bitPattern, expected.bitPattern, "\(testCase.source) must arrive as \(testCase.canonical)")
        }
    }

    func testAtomicGroupAppliesEveryMutationAgainstExtension() async throws {
        let userID = UUID().uuidString.lowercased()
        let writer = try SynchroClient(config: makeConfig(userID: userID, dbPath: tempDBPath()))
        let reader = try SynchroClient(config: makeConfig(userID: userID, dbPath: tempDBPath()))
        addTeardownBlock {
            await self.stopAndClose(writer)
            await self.stopAndClose(reader)
        }
        try await writer.start()
        await writer.enterBackground()
        let names = Dictionary(uniqueKeysWithValues: (0..<3).map { index in
            (UUID().uuidString.lowercased(), "atomic member \(index)")
        })
        try writer.atomicWriteTransaction { transaction in
            for (customerID, name) in names {
                let insert = customerInsert(customerID: customerID, userID: userID, name: name)
                try transaction.execute(insert.sql, params: insert.params)
            }
        }

        try await writer.enterForeground()
        try await waitForCondition(timeoutNanoseconds: 30_000_000_000) {
            try writer.pendingChangeCount() == 0
        }

        let sealed = try writer.query("SELECT request_json FROM _synchro_push_batches", params: nil)
        XCTAssertEqual(sealed.count, 1)
        let request = try JSONDecoder.synchroDecoder().decode(
            PushRequest.self,
            from: Data(try XCTUnwrap(sealed.first?["request_json"] as String?).utf8)
        )
        XCTAssertEqual(request.atomic, true)
        XCTAssertEqual(request.mutations.count, names.count)
        XCTAssertTrue(try writer.inspectRejectedMutations().isEmpty)
        XCTAssertTrue(try writer.inspectRetainedMutations().isEmpty)
        XCTAssertEqual(try customerNames(writer, userID: userID), names)
        try await reader.start()
        try await waitForCondition(timeoutNanoseconds: 60_000_000_000) {
            try await self.syncAllowingCaptureRetry(reader)
            return try self.customerNames(reader, userID: userID) == names
        }
    }

    func testAtomicGroupConflictKeepsServerRowAndLocalMembersAgainstExtension() async throws {
        let userID = UUID().uuidString.lowercased()
        let writer = try SynchroClient(config: makeConfig(userID: userID, dbPath: tempDBPath()))
        let other = try SynchroClient(config: makeConfig(userID: userID, dbPath: tempDBPath()))
        let reader = try SynchroClient(config: makeConfig(userID: userID, dbPath: tempDBPath()))
        addTeardownBlock {
            await self.stopAndClose(writer)
            await self.stopAndClose(other)
            await self.stopAndClose(reader)
        }
        let conflictedID = UUID().uuidString.lowercased()
        let localID = UUID().uuidString.lowercased()
        try await writer.start()
        _ = try writer.executeBatch([customerInsert(customerID: conflictedID, userID: userID, name: "original")])
        try await waitForCondition(timeoutNanoseconds: 60_000_000_000) {
            try await self.syncAllowingCaptureRetry(writer)
            return try writer.pendingChangeCount() == 0 && writer.getSyncStatus() == .ready
        }
        await writer.enterBackground()

        try await other.start()
        try await waitForCondition(timeoutNanoseconds: 60_000_000_000) {
            try await self.syncAllowingCaptureRetry(other)
            return try self.customerNames(other, userID: userID) == [conflictedID: "original"]
        }
        _ = try other.execute(
            "UPDATE customers SET name = ?, updated_at = ? WHERE id = ?",
            params: ["server row", "2026-01-09T00:00:00.000Z", conflictedID]
        )
        try await waitForCondition(timeoutNanoseconds: 60_000_000_000) {
            try await self.syncAllowingCaptureRetry(other)
            return try other.pendingChangeCount() == 0
        }

        try writer.atomicWriteTransaction { transaction in
            let insert = customerInsert(customerID: localID, userID: userID, name: "local member")
            try transaction.execute(insert.sql, params: insert.params)
            try transaction.execute(
                "UPDATE customers SET name = ?, updated_at = ? WHERE id = ?",
                params: ["stale edit", "2026-01-10T00:00:00.000Z", conflictedID]
            )
        }
        try await writer.enterForeground()
        try await waitForCondition(timeoutNanoseconds: 30_000_000_000) {
            try writer.pendingChangeCount() == 0
        }

        let rejected = Dictionary(uniqueKeysWithValues: try writer.inspectRejectedMutations().map { ($0.recordID, $0) })
        XCTAssertEqual(rejected.count, 2)
        XCTAssertEqual(rejected[localID]?.status, .rejectedTerminal)
        XCTAssertEqual(rejected[localID]?.code, .atomicBatchRejected)
        XCTAssertEqual(rejected[conflictedID]?.status, .conflict)
        XCTAssertEqual(rejected[conflictedID]?.code, .versionConflict)
        XCTAssertEqual(
            try customerNames(writer, userID: userID),
            [conflictedID: "server row", localID: "local member"]
        )
        try await reader.start()
        try await waitForCondition(timeoutNanoseconds: 60_000_000_000) {
            try await self.syncAllowingCaptureRetry(reader)
            return try self.customerNames(reader, userID: userID) == [conflictedID: "server row"]
        }
    }

    /// The fixture trigger gives the customer insert an order with the same ID. Thus the grouped
    /// order insert conflicts, and after the group rollback no row exists for the order.
    func testAtomicGroupConflictWithoutServerRowRemovesTheLocalRowAgainstExtension() async throws {
        let userID = UUID().uuidString.lowercased()
        let writer = try SynchroClient(config: makeConfig(userID: userID, dbPath: tempDBPath()))
        addTeardownBlock {
            await self.stopAndClose(writer)
        }
        let rowID = UUID().uuidString.lowercased()
        let updatedAt = "2026-01-11T00:00:00.000Z"
        try await writer.start()
        await writer.enterBackground()
        try writer.atomicWriteTransaction { transaction in
            try transaction.execute(
                "INSERT INTO customers (id, user_id, name, balance, is_active, market_segment, created_at, updated_at) VALUES (?, ?, 'shadow parent', 0, 1, 'test-shadow-order', ?, ?)",
                params: [rowID, userID, updatedAt, updatedAt]
            )
            try transaction.execute(
                "INSERT INTO orders (id, customer_id, user_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, ?, 'pending', 0, 'USD', ?, ?, ?)",
                params: [rowID, rowID, userID, #"{"street":"Shadow Way"}"#, updatedAt, updatedAt]
            )
        }
        try await writer.enterForeground()
        try await waitForCondition(timeoutNanoseconds: 30_000_000_000) {
            try writer.pendingChangeCount() == 0
        }

        let rejected = Dictionary(uniqueKeysWithValues: try writer.inspectRejectedMutations().map { ($0.tableName, $0) })
        XCTAssertEqual(rejected.count, 2)
        XCTAssertEqual(rejected["customers"]?.code, .atomicBatchRejected)
        let order = try XCTUnwrap(rejected["orders"])
        XCTAssertEqual(order.status, .conflict)
        XCTAssertEqual(order.code, .rowAlreadyExists)
        XCTAssertNil(order.serverRowJSON)
        XCTAssertNil(order.serverVersion)
        XCTAssertTrue(try writer.query("SELECT id FROM orders WHERE id = ?", params: [rowID]).isEmpty)
        XCTAssertEqual(try customerNames(writer, userID: userID), [rowID: "shadow parent"])
    }

    /// A pull after an accepted push can end in a scheduled retry until WAL
    /// capture completes, and capture can lag for tens of seconds. The engine
    /// owns that retry and the reconnect after it, and it refuses a caller
    /// sync until it is ready again. The caller polls its own observable
    /// outcome instead.
    private func syncAllowingCaptureRetry(_ client: SynchroClient) async throws {
        guard client.getSyncStatus() == .ready else { return }
        do {
            try await client.syncNow()
        } catch let error as RetryableError where error.classification == .http503 {
            // Only capture_pending and temporary_unavailable use a retryable 503.
        } catch SynchroError.notStarted where client.getSyncStatus() != .ready {}
    }

    private func customerInsert(customerID: String, userID: String, name: String) -> SQLStatement {
        SQLStatement(
            sql: "INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, ?, ?)",
            params: [customerID, userID, name, "2026-01-08T00:00:00.000Z", "2026-01-08T00:00:00.000Z"]
        )
    }

    private func customerNames(_ client: SynchroClient, userID: String) throws -> [String: String] {
        let rows = try client.query("SELECT id, name FROM customers WHERE user_id = ?", params: [userID])
        return Dictionary(uniqueKeysWithValues: rows.map { ($0["id"] as String, $0["name"] as String) })
    }

    /// Measures the push form of a pending mutation as the push path sends it.
    private func pushMeasure(_ pending: PendingMutationInspection) throws -> PushLimits.MutationMeasure {
        try PushLimits.measure(
            Mutation(
                mutationID: pending.mutationID,
                table: pending.tableID,
                op: pending.operation,
                pk: [pending.primaryKeyFieldID: AnyCodable(pending.recordID)],
                authoredSchema: pending.authoredSchema,
                baseVersion: pending.baseVersion,
                clientVersion: pending.clientVersion,
                columns: Dictionary(uniqueKeysWithValues: pending.authoredFields.map { ($0.fieldID, $0.value) })
            ),
            encoder: JSONEncoder.synchroEncoder()
        )
    }
}
