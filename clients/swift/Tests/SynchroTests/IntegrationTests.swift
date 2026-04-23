import XCTest
import Foundation
#if canImport(CommonCrypto)
import CommonCrypto
#endif
@testable import Synchro

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

    private func makeConfig(userID: String, clientID: String = UUID().uuidString.lowercased(), dbPath: String) -> SynchroConfig {
        let token = signTestJWT(userID: userID)
        return SynchroConfig(
            dbPath: dbPath,
            serverURL: serverURL,
            authProvider: { token },
            clientID: clientID,
            appVersion: "1.0.0",
            syncInterval: 999,
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
            protocolVersion: 2,
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
            "INSERT INTO orders (id, customer_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, 'pending', 0, 'USD', ?, ?, ?)",
            params: [orderID, customerID, shipAddress, updatedAt, updatedAt]
        )
    }


    private func stopAndClose(_ client: SynchroClient?) {
        client?.stop()
        try? client?.close()
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
            case .serverError(let status, _):
                XCTAssertEqual(status, 401)
            default:
                XCTFail("Expected serverError(401), got \(error)")
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
        defer {
            stopAndClose(clientA)
            stopAndClose(clientB)
        }

        try await clientA.start()
        try seedOrder(clientA, userID: userID, customerID: customerID, orderID: orderID, shipAddress: "123 Main St", updatedAt: "2026-01-01T00:00:00.000Z")
        try await clientA.syncNow()

        try await clientB.start()
        try await waitForCondition {
            try await clientB.syncNow()
            let row = try clientB.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: [orderID])
            return (row?["ship_address"] as? String) == "123 Main St"
        }
    }

    func testFreshClientBootstrapsExistingServerState() async throws {
        let userID = UUID().uuidString.lowercased()
        let writerConfig = makeConfig(userID: userID, dbPath: tempDBPath())
        let readerConfig = makeConfig(userID: userID, dbPath: tempDBPath())
        let customerID = UUID().uuidString.lowercased()
        let orderID = UUID().uuidString.lowercased()

        let writer = try SynchroClient(config: writerConfig)
        defer { stopAndClose(writer) }

        try await writer.start()
        try seedOrder(writer, userID: userID, customerID: customerID, orderID: orderID, shipAddress: "Bootstrap Ave", updatedAt: "2026-01-02T00:00:00.000Z")
        try await writer.syncNow()
        writer.stop()
        try writer.close()

        let reader = try SynchroClient(config: readerConfig)
        defer { stopAndClose(reader) }
        try await reader.start()
        try await waitForCondition {
            try await reader.syncNow()
            let row = try reader.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: [orderID])
            return (row?["ship_address"] as? String) == "Bootstrap Ave"
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
        defer {
            stopAndClose(clientA)
            stopAndClose(clientB)
        }

        try await clientA.start()
        try seedOrder(clientA, userID: userID, customerID: customerID, orderID: orderID, shipAddress: "Delete Me", updatedAt: "2026-01-03T00:00:00.000Z")
        try await clientA.syncNow()

        try await clientB.start()
        try await waitForCondition {
            let row = try clientB.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: [orderID])
            return (row?["ship_address"] as? String) == "Delete Me"
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

}
