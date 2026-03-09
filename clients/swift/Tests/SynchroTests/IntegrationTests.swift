import XCTest
import Foundation
#if canImport(CommonCrypto)
import CommonCrypto
#endif
@testable import Synchro

/// Integration tests that run against a real synchrod server with WAL consumer.
/// Requires SYNCHRO_TEST_URL and SYNCHRO_TEST_JWT_SECRET environment variables.
/// Skips when env vars are not set (same pattern as Go integration tests).
final class IntegrationTests: XCTestCase {

    private var serverURL: URL!
    private var jwtSecret: String!

    override func setUp() {
        super.setUp()
        guard let url = ProcessInfo.processInfo.environment["SYNCHRO_TEST_URL"],
              let secret = ProcessInfo.processInfo.environment["SYNCHRO_TEST_JWT_SECRET"] else {
            return
        }
        serverURL = URL(string: url)!
        jwtSecret = secret
    }

    private func skipIfNoServer() throws {
        if serverURL == nil || jwtSecret == nil {
            throw XCTSkip("SYNCHRO_TEST_URL or SYNCHRO_TEST_JWT_SECRET not set")
        }
    }

    // MARK: - JWT Helper

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

    // MARK: - Helpers

    private func makeHttpClient(userID: String, clientID: String? = nil, appVersion: String = "1.0.0") -> (HttpClient, String) {
        let cid = clientID ?? UUID().uuidString.lowercased()
        let token = signTestJWT(userID: userID)
        let config = SynchroConfig(
            dbPath: NSTemporaryDirectory() + UUID().uuidString.lowercased() + ".sqlite",
            serverURL: serverURL,
            authProvider: { token },
            clientID: cid,
            appVersion: appVersion,
            syncInterval: 999,
            maxRetryAttempts: 1
        )
        return (HttpClient(config: config), cid)
    }

    /// Polls pull until a record with the given ID appears, or timeout expires.
    /// WAL processing is async — after a push, the WAL consumer needs time to
    /// decode the event, assign buckets, and write changelog entries.
    private func pollForRecord(
        http: HttpClient,
        clientID: String,
        checkpoint: Int64,
        schemaVersion: Int64,
        schemaHash: String,
        recordID: String,
        timeout: TimeInterval = 10
    ) async throws -> PullResponse {
        let deadline = Date().addingTimeInterval(timeout)
        var lastResponse: PullResponse?

        while Date() < deadline {
            let resp = try await http.pull(request: PullRequest(
                clientID: clientID,
                checkpoint: checkpoint,
                tables: nil,
                limit: 100,
                knownBuckets: nil,
                schemaVersion: schemaVersion,
                schemaHash: schemaHash
            ))
            lastResponse = resp
            if resp.changes.contains(where: { $0.id == recordID }) {
                return resp
            }
            try await Task.sleep(nanoseconds: 250_000_000) // 250ms
        }

        // Final attempt
        if let last = lastResponse {
            return last
        }
        return try await http.pull(request: PullRequest(
            clientID: clientID,
            checkpoint: checkpoint,
            tables: nil,
            limit: 100,
            knownBuckets: nil,
            schemaVersion: schemaVersion,
            schemaHash: schemaHash
        ))
    }

    private func register(http: HttpClient, clientID: String) async throws -> RegisterResponse {
        try await http.register(request: RegisterRequest(
            clientID: clientID,
            clientName: nil,
            platform: "test",
            appVersion: "1.0.0",
            schemaVersion: 0,
            schemaHash: ""
        ))
    }

    private func bootstrapSnapshot(
        http: HttpClient,
        clientID: String,
        schemaVersion: Int64,
        schemaHash: String
    ) async throws {
        var cursor: SnapshotCursor?

        while true {
            let resp = try await http.snapshot(request: SnapshotRequest(
                clientID: clientID,
                cursor: cursor,
                limit: 100,
                schemaVersion: schemaVersion,
                schemaHash: schemaHash
            ))
            if !resp.hasMore {
                return
            }
            cursor = resp.cursor
        }
    }

    private func pushOrder(
        http: HttpClient,
        clientID: String,
        orderID: String,
        userID: String,
        shipAddress: String,
        schemaVersion: Int64,
        schemaHash: String,
        clientUpdatedAt: Date = Date()
    ) async throws -> PushResponse {
        try await http.push(request: PushRequest(
            clientID: clientID,
            changes: [
                PushRecord(
                    id: orderID,
                    tableName: "orders",
                    operation: "create",
                    data: [
                        "id": AnyCodable(orderID),
                        "user_id": AnyCodable(userID),
                        "ship_address": AnyCodable(shipAddress),
                    ],
                    clientUpdatedAt: clientUpdatedAt,
                    baseUpdatedAt: nil
                ),
            ],
            schemaVersion: schemaVersion,
            schemaHash: schemaHash
        ))
    }

    // MARK: - Test 1: Push and Pull Round Trip

    func testPushAndPullRoundTrip() async throws {
        try skipIfNoServer()

        let userID = UUID().uuidString.lowercased()
        let (http, clientID) = makeHttpClient(userID: userID)

        let reg = try await register(http: http, clientID: clientID)
        try await bootstrapSnapshot(
            http: http,
            clientID: clientID,
            schemaVersion: reg.schemaVersion,
            schemaHash: reg.schemaHash
        )

        // Push a record
        let orderID = UUID().uuidString.lowercased()
        let pushResp = try await pushOrder(
            http: http, clientID: clientID, orderID: orderID, userID: userID,
            shipAddress: "123 Main St",
            schemaVersion: reg.schemaVersion, schemaHash: reg.schemaHash
        )
        XCTAssertEqual(pushResp.accepted.count, 1)
        XCTAssertEqual(pushResp.accepted.first?.status, "applied")

        // Pull it back — WAL consumer processes the INSERT asynchronously
        let pullResp = try await pollForRecord(
            http: http, clientID: clientID, checkpoint: 0,
            schemaVersion: reg.schemaVersion, schemaHash: reg.schemaHash,
            recordID: orderID
        )

        let found = pullResp.changes.first(where: { $0.id == orderID })
        XCTAssertNotNil(found, "pushed order should appear in pull response")
        XCTAssertEqual(found?.tableName, "orders")

        // Verify the data content
        let shipAddress = found?.data["ship_address"]
        XCTAssertNotNil(shipAddress, "pulled record should contain 'ship_address' field")
    }

    // MARK: - Test 2: Pull Checkpoint Advancement

    func testPullCheckpointAdvancement() async throws {
        try skipIfNoServer()

        let userID = UUID().uuidString.lowercased()
        let (http, clientID) = makeHttpClient(userID: userID)

        let reg = try await register(http: http, clientID: clientID)
        try await bootstrapSnapshot(
            http: http,
            clientID: clientID,
            schemaVersion: reg.schemaVersion,
            schemaHash: reg.schemaHash
        )

        // Push 3 records
        var orderIDs: [String] = []
        for i in 0..<3 {
            let orderID = UUID().uuidString.lowercased()
            orderIDs.append(orderID)
            let resp = try await pushOrder(
                http: http, clientID: clientID, orderID: orderID, userID: userID,
                shipAddress: "Checkpoint Address \(i)",
                schemaVersion: reg.schemaVersion, schemaHash: reg.schemaHash
            )
            XCTAssertEqual(resp.accepted.count, 1)
        }

        // Wait for WAL consumer to process all 3
        _ = try await pollForRecord(
            http: http, clientID: clientID, checkpoint: 0,
            schemaVersion: reg.schemaVersion, schemaHash: reg.schemaHash,
            recordID: orderIDs.last!
        )

        // Now pull with limit=1 repeatedly and verify checkpoint advances
        var checkpoint: Int64 = 0
        var allPulledIDs: [String] = []
        var iterations = 0

        repeat {
            let pullResp = try await http.pull(request: PullRequest(
                clientID: clientID,
                checkpoint: checkpoint,
                tables: nil,
                limit: 1,
                knownBuckets: nil,
                schemaVersion: reg.schemaVersion,
                schemaHash: reg.schemaHash
            ))

            for change in pullResp.changes {
                allPulledIDs.append(change.id)
            }

            XCTAssertGreaterThan(pullResp.checkpoint, checkpoint,
                "checkpoint must advance on each pull page")
            checkpoint = pullResp.checkpoint
            iterations += 1

            if !pullResp.hasMore { break }
        } while iterations < 20 // Safety limit

        // All 3 orders should have been pulled
        for orderID in orderIDs {
            XCTAssertTrue(allPulledIDs.contains(where: { $0 == orderID }),
                "order \(orderID) should appear in paginated pull")
        }
    }

    // MARK: - Test 3: Conflict Resolution (LWW)

    func testConflictResolution() async throws {
        try skipIfNoServer()

        let userID = UUID().uuidString.lowercased()
        let (http, clientID) = makeHttpClient(userID: userID)

        let reg = try await register(http: http, clientID: clientID)
        try await bootstrapSnapshot(
            http: http,
            clientID: clientID,
            schemaVersion: reg.schemaVersion,
            schemaHash: reg.schemaHash
        )

        // Push a create
        let orderID = UUID().uuidString.lowercased()
        let createResp = try await pushOrder(
            http: http, clientID: clientID, orderID: orderID, userID: userID,
            shipAddress: "456 Conflict Ave",
            schemaVersion: reg.schemaVersion, schemaHash: reg.schemaHash
        )
        XCTAssertEqual(createResp.accepted.count, 1)
        XCTAssertEqual(createResp.accepted.first?.status, "applied")

        // Push a conflicting update with old timestamps.
        // baseUpdatedAt doesn't match server's updated_at → conflict detected.
        // clientUpdatedAt is ancient → server version is newer → server wins via LWW.
        let ancientDate = Date(timeIntervalSince1970: 946684800) // 2000-01-01
        let conflictResp = try await http.push(request: PushRequest(
            clientID: clientID,
            changes: [
                PushRecord(
                    id: orderID,
                    tableName: "orders",
                    operation: "update",
                    data: [
                        "id": AnyCodable(orderID),
                        "user_id": AnyCodable(userID),
                        "ship_address": AnyCodable("789 Conflict Blvd"),
                    ],
                    clientUpdatedAt: ancientDate,
                    baseUpdatedAt: ancientDate
                ),
            ],
            schemaVersion: reg.schemaVersion,
            schemaHash: reg.schemaHash
        ))

        // Server wins — update should be in rejected with status "conflict"
        XCTAssertEqual(conflictResp.rejected.count, 1, "conflicting update should be rejected")
        XCTAssertEqual(conflictResp.rejected.first?.status, "conflict")
        XCTAssertEqual(conflictResp.rejected.first?.id, orderID)

        // Server version should be returned so client can reconcile
        XCTAssertNotNil(conflictResp.rejected.first?.serverVersion,
            "rejected conflict should include server version for reconciliation")
    }

    // MARK: - Test 4: Soft Delete Sync

    func testSoftDeleteSync() async throws {
        try skipIfNoServer()

        let userID = UUID().uuidString.lowercased()
        let (http, clientID) = makeHttpClient(userID: userID)

        let reg = try await register(http: http, clientID: clientID)
        try await bootstrapSnapshot(
            http: http,
            clientID: clientID,
            schemaVersion: reg.schemaVersion,
            schemaHash: reg.schemaHash
        )

        // Create a record
        let orderID = UUID().uuidString.lowercased()
        let createResp = try await pushOrder(
            http: http, clientID: clientID, orderID: orderID, userID: userID,
            shipAddress: "999 Deletable Ln",
            schemaVersion: reg.schemaVersion, schemaHash: reg.schemaHash
        )
        XCTAssertEqual(createResp.accepted.count, 1)

        // Wait for WAL consumer to process the create
        let pullAfterCreate = try await pollForRecord(
            http: http, clientID: clientID, checkpoint: 0,
            schemaVersion: reg.schemaVersion, schemaHash: reg.schemaHash,
            recordID: orderID
        )
        let checkpointAfterCreate = pullAfterCreate.checkpoint

        // Soft-delete it
        let deleteResp = try await http.push(request: PushRequest(
            clientID: clientID,
            changes: [
                PushRecord(
                    id: orderID,
                    tableName: "orders",
                    operation: "delete",
                    data: [
                        "id": AnyCodable(orderID),
                        "user_id": AnyCodable(userID),
                    ],
                    clientUpdatedAt: Date(),
                    baseUpdatedAt: nil
                ),
            ],
            schemaVersion: reg.schemaVersion,
            schemaHash: reg.schemaHash
        ))
        XCTAssertEqual(deleteResp.accepted.count, 1, "delete should be accepted")
        XCTAssertNotNil(deleteResp.accepted.first?.serverDeletedAt,
            "delete result should include server deleted_at timestamp")

        // Pull after the delete — should see the delete in the deletes list or
        // the record should have deleted_at set. Poll until we see the delete
        // changelog entry from WAL.
        let deadline = Date().addingTimeInterval(10)
        var foundDelete = false
        var checkpoint = checkpointAfterCreate

        while Date() < deadline && !foundDelete {
            let pullResp = try await http.pull(request: PullRequest(
                clientID: clientID,
                checkpoint: checkpoint,
                tables: nil,
                limit: 100,
                knownBuckets: nil,
                schemaVersion: reg.schemaVersion,
                schemaHash: reg.schemaHash
            ))

            // Check deletes list
            if pullResp.deletes.contains(where: { $0.id == orderID }) {
                foundDelete = true
                break
            }

            // Check if it appears as a change with deleted_at set
            if let record = pullResp.changes.first(where: { $0.id == orderID }),
               record.deletedAt != nil {
                foundDelete = true
                break
            }

            checkpoint = pullResp.checkpoint
            if !pullResp.hasMore {
                try await Task.sleep(nanoseconds: 250_000_000)
            }
        }

        XCTAssertTrue(foundDelete, "soft-deleted record should appear in pull as deleted")
    }

    // MARK: - Test 5: Auth Failure

    func testAuthFailure() async throws {
        try skipIfNoServer()

        let config = SynchroConfig(
            dbPath: NSTemporaryDirectory() + UUID().uuidString.lowercased() + ".sqlite",
            serverURL: serverURL,
            authProvider: { "invalid-jwt-token" },
            clientID: UUID().uuidString.lowercased(),
            appVersion: "1.0.0",
            syncInterval: 999,
            maxRetryAttempts: 1
        )
        let http = HttpClient(config: config)

        do {
            _ = try await http.fetchSchema()
            XCTFail("expected auth failure with invalid JWT")
        } catch let error as SynchroError {
            if case .serverError(let status, _) = error {
                XCTAssertEqual(status, 401)
            } else {
                XCTFail("expected serverError(401), got \(error)")
            }
        }

        // Also verify no data leaks — register should fail too
        do {
            _ = try await http.register(request: RegisterRequest(
                clientID: config.clientID, clientName: nil, platform: "test",
                appVersion: "1.0.0", schemaVersion: 0, schemaHash: ""
            ))
            XCTFail("expected auth failure on register")
        } catch let error as SynchroError {
            if case .serverError(let status, _) = error {
                XCTAssertEqual(status, 401)
            } else {
                XCTFail("expected serverError(401), got \(error)")
            }
        }
    }

    // MARK: - Test 7: Version Mismatch

    func testVersionMismatch() async throws {
        try skipIfNoServer()

        let userID = UUID().uuidString.lowercased()
        let (http, _) = makeHttpClient(userID: userID, appVersion: "0.0.1")

        do {
            _ = try await http.register(request: RegisterRequest(
                clientID: UUID().uuidString.lowercased(),
                clientName: nil,
                platform: "test",
                appVersion: "0.0.1",
                schemaVersion: 0,
                schemaHash: ""
            ))
            XCTFail("expected upgrade required error")
        } catch let error as SynchroError {
            if case .upgradeRequired = error {
                // Expected — server requires >= 1.0.0, client sent 0.0.1
            } else {
                XCTFail("expected upgradeRequired, got \(error)")
            }
        }
    }

    // MARK: - Test 6: Schema Drift Detection

    private func skipIfNoDatabase() throws {
        guard ProcessInfo.processInfo.environment["TEST_DATABASE_URL"] != nil else {
            throw XCTSkip("TEST_DATABASE_URL not set")
        }
    }

    private func runSQL(_ sql: String) throws {
        guard let dbURL = ProcessInfo.processInfo.environment["TEST_DATABASE_URL"] else {
            throw XCTSkip("TEST_DATABASE_URL not set")
        }
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = ["psql", dbURL, "-c", sql]
        let pipe = Pipe()
        process.standardOutput = pipe
        process.standardError = pipe
        try process.run()
        process.waitUntilExit()
        guard process.terminationStatus == 0 else {
            let output = String(data: pipe.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
            XCTFail("psql failed: \(output)")
            return
        }
    }

    func testSchemaDriftDetection() async throws {
        try skipIfNoServer()
        try skipIfNoDatabase()

        // Cleanup: drop the drift column even if test fails
        addTeardownBlock { [self] in
            try? runSQL("ALTER TABLE orders DROP COLUMN IF EXISTS _drift")
        }

        let userID = UUID().uuidString.lowercased()
        let (http, clientID) = makeHttpClient(userID: userID)

        // Register and capture the current schema version/hash
        let reg = try await register(http: http, clientID: clientID)
        let oldVersion = reg.schemaVersion
        let oldHash = reg.schemaHash

        // Alter the DB schema — this changes the pg_catalog metadata that the
        // schema hash is computed from, without any server code change
        try runSQL("ALTER TABLE orders ADD COLUMN _drift TEXT")

        // Push with the stale schema should get 409 schemaMismatch
        let orderID = UUID().uuidString.lowercased()
        do {
            _ = try await pushOrder(
                http: http, clientID: clientID, orderID: orderID, userID: userID,
                shipAddress: "Should Be Rejected",
                schemaVersion: oldVersion, schemaHash: oldHash
            )
            XCTFail("expected schemaMismatch error after schema drift")
        } catch let error as SynchroError {
            guard case .schemaMismatch(let serverVersion, let serverHash) = error else {
                XCTFail("expected schemaMismatch, got \(error)")
                return
            }
            // Server should report a newer version and a different hash
            XCTAssertGreaterThan(serverVersion, oldVersion,
                "server schema version should advance after ALTER TABLE")
            XCTAssertNotEqual(serverHash, oldHash,
                "server schema hash should differ after column addition")
        }
    }

    // MARK: - Test 8: Multi-User Isolation

    func testMultiUserIsolation() async throws {
        try skipIfNoServer()

        let userA = UUID().uuidString.lowercased()
        let userB = UUID().uuidString.lowercased()
        let (httpA, clientA) = makeHttpClient(userID: userA)
        let (httpB, clientB) = makeHttpClient(userID: userB)

        let regA = try await register(http: httpA, clientID: clientA)
        let regB = try await register(http: httpB, clientID: clientB)
        try await bootstrapSnapshot(
            http: httpA,
            clientID: clientA,
            schemaVersion: regA.schemaVersion,
            schemaHash: regA.schemaHash
        )
        try await bootstrapSnapshot(
            http: httpB,
            clientID: clientB,
            schemaVersion: regB.schemaVersion,
            schemaHash: regB.schemaHash
        )

        // User A pushes
        let orderA = UUID().uuidString.lowercased()
        let pushA = try await pushOrder(
            http: httpA, clientID: clientA, orderID: orderA, userID: userA,
            shipAddress: "User A Private Address",
            schemaVersion: regA.schemaVersion, schemaHash: regA.schemaHash
        )
        XCTAssertEqual(pushA.accepted.count, 1)

        // User B pushes
        let orderB = UUID().uuidString.lowercased()
        let pushB = try await pushOrder(
            http: httpB, clientID: clientB, orderID: orderB, userID: userB,
            shipAddress: "User B Private Address",
            schemaVersion: regB.schemaVersion, schemaHash: regB.schemaHash
        )
        XCTAssertEqual(pushB.accepted.count, 1)

        // Wait for User B's order to appear in User B's pull (proves WAL processed it)
        let pullB = try await pollForRecord(
            http: httpB, clientID: clientB, checkpoint: 0,
            schemaVersion: regB.schemaVersion, schemaHash: regB.schemaHash,
            recordID: orderB
        )

        // User B should see their own order
        XCTAssertTrue(pullB.changes.contains(where: { $0.id == orderB }),
            "User B should see their own order")

        // User B should NOT see User A's order — bucket isolation
        XCTAssertFalse(pullB.changes.contains(where: { $0.id == orderA }),
            "User B must not see User A's order (bucket isolation)")

        // Wait for User A's order to appear in User A's pull
        let pullA = try await pollForRecord(
            http: httpA, clientID: clientA, checkpoint: 0,
            schemaVersion: regA.schemaVersion, schemaHash: regA.schemaHash,
            recordID: orderA
        )

        // User A should see their own order
        XCTAssertTrue(pullA.changes.contains(where: { $0.id == orderA }),
            "User A should see their own order")

        // User A should NOT see User B's order
        XCTAssertFalse(pullA.changes.contains(where: { $0.id == orderB }),
            "User A must not see User B's order (bucket isolation)")
    }
}
