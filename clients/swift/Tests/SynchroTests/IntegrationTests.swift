import XCTest
import Foundation
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
                syncInterval: 999,
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
            syncInterval: 999,
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
        let clientAConfig = makeConfig(userID: userID, dbPath: tempDBPath())
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
        try await clientA.syncNow()

        try await clientB.start()
        try await waitForCondition {
            try await clientB.syncNow()
            let row = try clientB.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: [orderID])
            return (row?["ship_address"] as? String) == #"{"street":"123 Main St"}"#
        }
    }

    func testFreshClientBootstrapsExistingServerState() async throws {
        let userID = UUID().uuidString.lowercased()
        let writerConfig = makeConfig(userID: userID, dbPath: tempDBPath())
        let readerConfig = makeConfig(userID: userID, dbPath: tempDBPath())
        let customerID = UUID().uuidString.lowercased()
        let orderID = UUID().uuidString.lowercased()

        let writer = try SynchroClient(config: writerConfig)
        addTeardownBlock { await self.stopAndClose(writer) }

        try await writer.start()
        try seedOrder(writer, userID: userID, customerID: customerID, orderID: orderID, shipAddress: #"{"street":"Bootstrap Ave"}"#, updatedAt: "2026-01-02T00:00:00.000Z")
        try await writer.syncNow()
        await writer.stop()
        try await writer.close()

        let reader = try SynchroClient(config: readerConfig)
        addTeardownBlock { await self.stopAndClose(reader) }
        try await reader.start()
        try await waitForCondition {
            try await reader.syncNow()
            let row = try reader.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: [orderID])
            return (row?["ship_address"] as? String) == #"{"street":"Bootstrap Ave"}"#
        }
    }

    func testSoftDeletePropagatesBetweenClients() async throws {
        let userID = UUID().uuidString.lowercased()
        let clientAConfig = makeConfig(userID: userID, dbPath: tempDBPath())
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
        try await clientA.syncNow()

        try await clientB.start()
        try await waitForCondition {
            let row = try clientB.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: [orderID])
            return (row?["ship_address"] as? String) == #"{"street":"Delete Me"}"#
        }

        _ = try clientA.execute(
            "UPDATE orders SET deleted_at = ?, updated_at = ? WHERE id = ?",
            params: ["2026-01-04T00:00:00.000Z", "2026-01-04T00:00:00.000Z", orderID]
        )
        try await clientA.syncNow()
        let expectedDeletedAt = try clientA.queryOne(
            "SELECT deleted_at FROM orders WHERE id = ?",
            params: [orderID]
        )?["deleted_at"] as? String
        XCTAssertNotNil(expectedDeletedAt)
        try await waitForCondition {
            try await clientB.syncNow()
            let row = try clientB.queryOne("SELECT deleted_at FROM orders WHERE id = ?", params: [orderID])
            return (row?["deleted_at"] as? String) == expectedDeletedAt
        }
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

}
