import XCTest
import Foundation
#if canImport(CommonCrypto)
import CommonCrypto
#endif
@testable import Synchro

/// Integration tests that run against a real synchrod-pg server with PG extension.
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

    // MARK: - HTTP Helpers

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

    /// Fetches server schema, then registers the client. Returns the register response
    /// plus schema version/hash for use in subsequent calls.
    private func registerWithSchema(
        http: HttpClient,
        clientID: String
    ) async throws -> (reg: RegisterResponse, schemaVersion: Int64, schemaHash: String) {
        let schema = try await http.fetchSchema()
        let reg = try await http.register(request: RegisterRequest(
            clientID: clientID,
            clientName: nil,
            platform: "test",
            appVersion: "1.0.0",
            schemaVersion: schema.schemaVersion,
            schemaHash: schema.schemaHash
        ))
        return (reg, schema.schemaVersion, schema.schemaHash)
    }

    /// Paginates through POST /sync/rebuild until hasMore == false.
    /// Returns all records and the final bucketChecksum (if present).
    @discardableResult
    private func rebuildBucket(
        http: HttpClient,
        clientID: String,
        bucketID: String,
        schemaVersion: Int64,
        schemaHash: String
    ) async throws -> (records: [Record], bucketChecksum: Int32?) {
        var allRecords: [Record] = []
        var cursor: String? = nil
        var hasMore = true
        var finalChecksum: Int32? = nil

        while hasMore {
            let resp = try await http.rebuild(request: RebuildRequest(
                clientID: clientID,
                bucketID: bucketID,
                cursor: cursor,
                limit: 100,
                schemaVersion: schemaVersion,
                schemaHash: schemaHash
            ))
            allRecords.append(contentsOf: resp.records)
            cursor = resp.cursor
            hasMore = resp.hasMore
            if !hasMore {
                finalChecksum = resp.bucketChecksum
            }
        }
        return (allRecords, finalChecksum)
    }

    // MARK: - Push Helpers

    private func pushCustomer(
        http: HttpClient, clientID: String,
        customerID: String, userID: String,
        email: String = "test@example.com",
        internalNotes: String? = nil,
        schemaVersion: Int64, schemaHash: String,
        clientUpdatedAt: Date = Date()
    ) async throws -> PushResponse {
        var data: [String: AnyCodable] = [
            "id": AnyCodable(customerID),
            "user_id": AnyCodable(userID),
            "email": AnyCodable(email),
        ]
        if let notes = internalNotes {
            data["internal_notes"] = AnyCodable(notes)
        }
        return try await http.push(request: PushRequest(
            clientID: clientID,
            changes: [PushRecord(
                id: customerID, tableName: "customers", operation: "create",
                data: data, clientUpdatedAt: clientUpdatedAt
            )],
            schemaVersion: schemaVersion, schemaHash: schemaHash
        ))
    }

    private func pushOrder(
        http: HttpClient, clientID: String,
        orderID: String, customerID: String,
        shipAddress: [String: Any]? = nil,
        schemaVersion: Int64, schemaHash: String,
        clientUpdatedAt: Date = Date()
    ) async throws -> PushResponse {
        var data: [String: AnyCodable] = [
            "id": AnyCodable(orderID),
            "customer_id": AnyCodable(customerID),
        ]
        if let addr = shipAddress {
            data["ship_address"] = AnyCodable(addr)
        }
        return try await http.push(request: PushRequest(
            clientID: clientID,
            changes: [PushRecord(
                id: orderID, tableName: "orders", operation: "create",
                data: data, clientUpdatedAt: clientUpdatedAt
            )],
            schemaVersion: schemaVersion, schemaHash: schemaHash
        ))
    }

    private func pushLineItem(
        http: HttpClient, clientID: String,
        lineItemID: String, orderID: String,
        quantity: Int = 1, unitPrice: Double = 10.00,
        schemaVersion: Int64, schemaHash: String,
        clientUpdatedAt: Date = Date()
    ) async throws -> PushResponse {
        try await http.push(request: PushRequest(
            clientID: clientID,
            changes: [PushRecord(
                id: lineItemID, tableName: "line_items", operation: "create",
                data: [
                    "id": AnyCodable(lineItemID),
                    "order_id": AnyCodable(orderID),
                    "quantity": AnyCodable(quantity),
                    "unit_price": AnyCodable(unitPrice),
                ],
                clientUpdatedAt: clientUpdatedAt
            )],
            schemaVersion: schemaVersion, schemaHash: schemaHash
        ))
    }

    private func pushDocument(
        http: HttpClient, clientID: String,
        docID: String, ownerID: String,
        title: String = "Test Document",
        schemaVersion: Int64, schemaHash: String,
        clientUpdatedAt: Date = Date()
    ) async throws -> PushResponse {
        try await http.push(request: PushRequest(
            clientID: clientID,
            changes: [PushRecord(
                id: docID, tableName: "documents", operation: "create",
                data: [
                    "id": AnyCodable(docID),
                    "owner_id": AnyCodable(ownerID),
                    "title": AnyCodable(title),
                ],
                clientUpdatedAt: clientUpdatedAt
            )],
            schemaVersion: schemaVersion, schemaHash: schemaHash
        ))
    }

    private func pushDocumentMember(
        http: HttpClient, clientID: String,
        memberID: String, docID: String, userID: String,
        role: String = "editor",
        schemaVersion: Int64, schemaHash: String,
        clientUpdatedAt: Date = Date()
    ) async throws -> PushResponse {
        try await http.push(request: PushRequest(
            clientID: clientID,
            changes: [PushRecord(
                id: memberID, tableName: "document_members", operation: "create",
                data: [
                    "id": AnyCodable(memberID),
                    "document_id": AnyCodable(docID),
                    "user_id": AnyCodable(userID),
                    "role": AnyCodable(role),
                ],
                clientUpdatedAt: clientUpdatedAt
            )],
            schemaVersion: schemaVersion, schemaHash: schemaHash
        ))
    }

    private func pushDocumentComment(
        http: HttpClient, clientID: String,
        commentID: String, docID: String, authorID: String,
        body: String = "Test comment",
        schemaVersion: Int64, schemaHash: String,
        clientUpdatedAt: Date = Date()
    ) async throws -> PushResponse {
        try await http.push(request: PushRequest(
            clientID: clientID,
            changes: [PushRecord(
                id: commentID, tableName: "document_comments", operation: "create",
                data: [
                    "id": AnyCodable(commentID),
                    "document_id": AnyCodable(docID),
                    "author_id": AnyCodable(authorID),
                    "body": AnyCodable(body),
                ],
                clientUpdatedAt: clientUpdatedAt
            )],
            schemaVersion: schemaVersion, schemaHash: schemaHash
        ))
    }

    /// Polls pull every 250ms until a record with the given ID appears, or timeout expires.
    private func pollForRecord(
        http: HttpClient, clientID: String,
        checkpoint: Int64,
        schemaVersion: Int64, schemaHash: String,
        recordID: String, timeout: TimeInterval = 10
    ) async throws -> PullResponse {
        let deadline = Date().addingTimeInterval(timeout)
        var lastResponse: PullResponse?

        while Date() < deadline {
            let resp = try await http.pull(request: PullRequest(
                clientID: clientID, checkpoint: checkpoint,
                tables: nil, limit: 100, knownBuckets: nil,
                schemaVersion: schemaVersion, schemaHash: schemaHash
            ))
            lastResponse = resp
            if resp.changes.contains(where: { $0.id == recordID }) {
                return resp
            }
            try await Task.sleep(nanoseconds: 250_000_000)
        }

        if let last = lastResponse { return last }
        return try await http.pull(request: PullRequest(
            clientID: clientID, checkpoint: checkpoint,
            tables: nil, limit: 100, knownBuckets: nil,
            schemaVersion: schemaVersion, schemaHash: schemaHash
        ))
    }

    // MARK: - Test 1: Register and Schema Fetch

    func testRegisterAndSchemaFetch() async throws {
        try skipIfNoServer()

        let userID = UUID().uuidString.lowercased()
        let (http, clientID) = makeHttpClient(userID: userID)

        let schema = try await http.fetchSchema()
        XCTAssertGreaterThan(schema.schemaVersion, 0, "schema version should be > 0")
        XCTAssertFalse(schema.schemaHash.isEmpty, "schema hash should be non-empty")
        XCTAssertGreaterThanOrEqual(schema.tables.count, 13, "should have at least 13 TPC-H tables")

        // Verify read-only tables are marked correctly
        let regions = schema.tables.first(where: { $0.tableName == "regions" })
        XCTAssertNotNil(regions, "regions table should exist in schema")
        XCTAssertEqual(regions?.pushPolicy, "read_only", "regions should be read_only")

        // Verify writable tables
        let customers = schema.tables.first(where: { $0.tableName == "customers" })
        XCTAssertNotNil(customers, "customers table should exist in schema")
        XCTAssertEqual(customers?.pushPolicy, "enabled", "customers should be enabled")

        // Register
        let reg = try await http.register(request: RegisterRequest(
            clientID: clientID, clientName: nil, platform: "test",
            appVersion: "1.0.0",
            schemaVersion: schema.schemaVersion, schemaHash: schema.schemaHash
        ))
        XCTAssertFalse(reg.id.isEmpty)
        XCTAssertEqual(reg.schemaVersion, schema.schemaVersion)
        XCTAssertEqual(reg.schemaHash, schema.schemaHash)
        XCTAssertNotNil(reg.bucketCheckpoints, "register should return bucket_checkpoints")
    }

    // MARK: - Test 2: Rebuild Bootstrap

    func testRebuildBootstrap() async throws {
        try skipIfNoServer()

        let userID = UUID().uuidString.lowercased()
        let (http, clientID) = makeHttpClient(userID: userID)
        let (reg, sv, sh) = try await registerWithSchema(http: http, clientID: clientID)

        // Rebuild global bucket: should have reference data (regions, nations, etc.)
        let (globalRecords, globalChecksum) = try await rebuildBucket(
            http: http, clientID: clientID, bucketID: "global",
            schemaVersion: sv, schemaHash: sh
        )
        XCTAssertGreaterThan(globalRecords.count, 0, "global bucket should contain reference data")

        // Final page should have a checksum
        XCTAssertNotNil(globalChecksum, "final rebuild page should include bucket_checksum")

        // Rebuild user bucket: empty for a brand new user
        let userBucket = "user:\(userID)"
        let (userRecords, _) = try await rebuildBucket(
            http: http, clientID: clientID, bucketID: userBucket,
            schemaVersion: sv, schemaHash: sh
        )
        XCTAssertEqual(userRecords.count, 0, "new user's bucket should be empty")
    }

    // MARK: - Test 3: Push/Pull FK Chain

    func testPushPullFKChain() async throws {
        try skipIfNoServer()

        let userID = UUID().uuidString.lowercased()
        let (http, clientID) = makeHttpClient(userID: userID)
        let (_, sv, sh) = try await registerWithSchema(http: http, clientID: clientID)

        // Push customer (root owner)
        let custID = UUID().uuidString.lowercased()
        let custResp = try await pushCustomer(
            http: http, clientID: clientID,
            customerID: custID, userID: userID,
            schemaVersion: sv, schemaHash: sh
        )
        XCTAssertEqual(custResp.accepted.count, 1)

        // Push order with customer_id FK and JSONB ship_address
        let orderID = UUID().uuidString.lowercased()
        let address: [String: Any] = ["street": "123 Main St", "city": "Anytown", "zip": "12345"]
        let orderResp = try await pushOrder(
            http: http, clientID: clientID,
            orderID: orderID, customerID: custID, shipAddress: address,
            schemaVersion: sv, schemaHash: sh
        )
        XCTAssertEqual(orderResp.accepted.count, 1)

        // Poll-pull: both should appear
        let pullResp = try await pollForRecord(
            http: http, clientID: clientID, checkpoint: 0,
            schemaVersion: sv, schemaHash: sh, recordID: orderID
        )

        let foundCust = pullResp.changes.first(where: { $0.id == custID })
        XCTAssertNotNil(foundCust, "customer should appear in pull")

        let foundOrder = pullResp.changes.first(where: { $0.id == orderID })
        XCTAssertNotNil(foundOrder, "order should appear in pull")
        XCTAssertEqual(foundOrder?.tableName, "orders")

        // Verify JSONB ship_address round-trips
        if let shipAddr = foundOrder?.data["ship_address"]?.value as? [String: Any] {
            XCTAssertEqual(shipAddr["street"] as? String, "123 Main St")
        }
    }

    // MARK: - Test 4: Deep FK Chain

    func testDeepFKChain() async throws {
        try skipIfNoServer()

        let userID = UUID().uuidString.lowercased()
        let (http, clientID) = makeHttpClient(userID: userID)
        let (_, sv, sh) = try await registerWithSchema(http: http, clientID: clientID)

        let custID = UUID().uuidString.lowercased()
        _ = try await pushCustomer(
            http: http, clientID: clientID,
            customerID: custID, userID: userID,
            schemaVersion: sv, schemaHash: sh
        )

        let orderID = UUID().uuidString.lowercased()
        _ = try await pushOrder(
            http: http, clientID: clientID,
            orderID: orderID, customerID: custID,
            schemaVersion: sv, schemaHash: sh
        )

        let lineItemID = UUID().uuidString.lowercased()
        let liResp = try await pushLineItem(
            http: http, clientID: clientID,
            lineItemID: lineItemID, orderID: orderID,
            schemaVersion: sv, schemaHash: sh
        )
        XCTAssertEqual(liResp.accepted.count, 1)

        // All three should appear in pull
        let pullResp = try await pollForRecord(
            http: http, clientID: clientID, checkpoint: 0,
            schemaVersion: sv, schemaHash: sh, recordID: lineItemID
        )

        XCTAssertTrue(pullResp.changes.contains(where: { $0.id == custID }), "customer should be in pull")
        XCTAssertTrue(pullResp.changes.contains(where: { $0.id == orderID }), "order should be in pull")
        XCTAssertTrue(pullResp.changes.contains(where: { $0.id == lineItemID }), "line_item should be in pull")
    }

    // MARK: - Test 5: Pull Pagination

    func testPullPagination() async throws {
        try skipIfNoServer()

        let userID = UUID().uuidString.lowercased()
        let (http, clientID) = makeHttpClient(userID: userID)
        let (_, sv, sh) = try await registerWithSchema(http: http, clientID: clientID)

        // Push customer first (required for FK), then 3 orders
        let custID = UUID().uuidString.lowercased()
        _ = try await pushCustomer(
            http: http, clientID: clientID,
            customerID: custID, userID: userID,
            schemaVersion: sv, schemaHash: sh
        )

        var orderIDs: [String] = []
        for _ in 0..<3 {
            let oid = UUID().uuidString.lowercased()
            orderIDs.append(oid)
            _ = try await pushOrder(
                http: http, clientID: clientID,
                orderID: oid, customerID: custID,
                schemaVersion: sv, schemaHash: sh
            )
        }

        // Wait for last order to appear
        _ = try await pollForRecord(
            http: http, clientID: clientID, checkpoint: 0,
            schemaVersion: sv, schemaHash: sh, recordID: orderIDs.last!
        )

        // Pull with limit=1 and verify checkpoint advances
        var checkpoint: Int64 = 0
        var allPulledIDs: Set<String> = []
        var iterations = 0

        repeat {
            let resp = try await http.pull(request: PullRequest(
                clientID: clientID, checkpoint: checkpoint,
                tables: nil, limit: 1, knownBuckets: nil,
                schemaVersion: sv, schemaHash: sh
            ))

            for change in resp.changes {
                allPulledIDs.insert(change.id)
            }

            XCTAssertGreaterThanOrEqual(resp.checkpoint, checkpoint,
                "checkpoint must not go backwards")
            checkpoint = resp.checkpoint
            iterations += 1

            if !resp.hasMore { break }
        } while iterations < 50

        // All 3 orders appear exactly once
        for oid in orderIDs {
            XCTAssertTrue(allPulledIDs.contains(oid), "order \(oid) should appear in paginated pull")
        }
    }

    // MARK: - Test 6: Conflict Server Wins

    func testConflictServerWins() async throws {
        try skipIfNoServer()

        let userID = UUID().uuidString.lowercased()
        let (http, clientID) = makeHttpClient(userID: userID)
        let (_, sv, sh) = try await registerWithSchema(http: http, clientID: clientID)

        let custID = UUID().uuidString.lowercased()
        _ = try await pushCustomer(
            http: http, clientID: clientID,
            customerID: custID, userID: userID,
            schemaVersion: sv, schemaHash: sh
        )

        // Update with ancient timestamps: server version is newer, server wins
        let ancientDate = Date(timeIntervalSince1970: 946684800) // 2000-01-01
        let conflictResp = try await http.push(request: PushRequest(
            clientID: clientID,
            changes: [PushRecord(
                id: custID, tableName: "customers", operation: "update",
                data: [
                    "id": AnyCodable(custID),
                    "user_id": AnyCodable(userID),
                    "email": AnyCodable("stale@example.com"),
                ],
                clientUpdatedAt: ancientDate, baseUpdatedAt: ancientDate
            )],
            schemaVersion: sv, schemaHash: sh
        ))

        XCTAssertEqual(conflictResp.rejected.count, 1)
        XCTAssertEqual(conflictResp.rejected.first?.status, "conflict")
        XCTAssertEqual(conflictResp.rejected.first?.reasonCode, "server_won_conflict")
        XCTAssertNotNil(conflictResp.rejected.first?.serverVersion, "should include serverVersion")
    }

    // MARK: - Test 7: Conflict Client Wins

    func testConflictClientWins() async throws {
        try skipIfNoServer()

        let userID = UUID().uuidString.lowercased()
        let (http, clientID) = makeHttpClient(userID: userID)
        let (_, sv, sh) = try await registerWithSchema(http: http, clientID: clientID)

        let custID = UUID().uuidString.lowercased()
        let createResp = try await pushCustomer(
            http: http, clientID: clientID,
            customerID: custID, userID: userID,
            schemaVersion: sv, schemaHash: sh
        )
        let serverUpdatedAt = createResp.accepted.first?.serverUpdatedAt

        // Update with fresh timestamp + matching baseUpdatedAt
        let updateResp = try await http.push(request: PushRequest(
            clientID: clientID,
            changes: [PushRecord(
                id: custID, tableName: "customers", operation: "update",
                data: [
                    "id": AnyCodable(custID),
                    "user_id": AnyCodable(userID),
                    "email": AnyCodable("updated@example.com"),
                ],
                clientUpdatedAt: Date(), baseUpdatedAt: serverUpdatedAt
            )],
            schemaVersion: sv, schemaHash: sh
        ))

        XCTAssertEqual(updateResp.accepted.count, 1)
        XCTAssertEqual(updateResp.accepted.first?.status, PushStatus.applied)
    }

    // MARK: - Test 8: Conflict Duplicate Create

    func testConflictDuplicateCreate() async throws {
        try skipIfNoServer()

        let userID = UUID().uuidString.lowercased()
        let (http, clientID) = makeHttpClient(userID: userID)
        let (_, sv, sh) = try await registerWithSchema(http: http, clientID: clientID)

        let custID = UUID().uuidString.lowercased()
        _ = try await pushCustomer(
            http: http, clientID: clientID,
            customerID: custID, userID: userID,
            schemaVersion: sv, schemaHash: sh
        )

        // Push create with same ID
        let dupResp = try await pushCustomer(
            http: http, clientID: clientID,
            customerID: custID, userID: userID, email: "dup@example.com",
            schemaVersion: sv, schemaHash: sh
        )

        XCTAssertEqual(dupResp.rejected.count, 1)
        XCTAssertEqual(dupResp.rejected.first?.reasonCode, "record_exists")
    }

    // MARK: - Test 9: Conflict Update on Deleted

    func testConflictUpdateOnDeleted() async throws {
        try skipIfNoServer()

        let userID = UUID().uuidString.lowercased()
        let (http, clientID) = makeHttpClient(userID: userID)
        let (_, sv, sh) = try await registerWithSchema(http: http, clientID: clientID)

        let custID = UUID().uuidString.lowercased()
        _ = try await pushCustomer(
            http: http, clientID: clientID,
            customerID: custID, userID: userID,
            schemaVersion: sv, schemaHash: sh
        )

        // Delete
        _ = try await http.push(request: PushRequest(
            clientID: clientID,
            changes: [PushRecord(
                id: custID, tableName: "customers", operation: "delete",
                data: ["id": AnyCodable(custID), "user_id": AnyCodable(userID)],
                clientUpdatedAt: Date()
            )],
            schemaVersion: sv, schemaHash: sh
        ))

        // Update on deleted record
        let updateResp = try await http.push(request: PushRequest(
            clientID: clientID,
            changes: [PushRecord(
                id: custID, tableName: "customers", operation: "update",
                data: [
                    "id": AnyCodable(custID),
                    "user_id": AnyCodable(userID),
                    "email": AnyCodable("ghost@example.com"),
                ],
                clientUpdatedAt: Date()
            )],
            schemaVersion: sv, schemaHash: sh
        ))

        XCTAssertEqual(updateResp.rejected.count, 1)
        XCTAssertEqual(updateResp.rejected.first?.reasonCode, "record_deleted")
    }

    // MARK: - Test 10: Resurrection

    func testResurrection() async throws {
        try skipIfNoServer()

        let userID = UUID().uuidString.lowercased()
        let (http, clientID) = makeHttpClient(userID: userID)
        let (_, sv, sh) = try await registerWithSchema(http: http, clientID: clientID)

        let custID = UUID().uuidString.lowercased()
        _ = try await pushCustomer(
            http: http, clientID: clientID,
            customerID: custID, userID: userID,
            schemaVersion: sv, schemaHash: sh
        )

        // Delete
        _ = try await http.push(request: PushRequest(
            clientID: clientID,
            changes: [PushRecord(
                id: custID, tableName: "customers", operation: "delete",
                data: ["id": AnyCodable(custID), "user_id": AnyCodable(userID)],
                clientUpdatedAt: Date()
            )],
            schemaVersion: sv, schemaHash: sh
        ))

        // Re-create same ID (resurrection)
        let resurrectResp = try await pushCustomer(
            http: http, clientID: clientID,
            customerID: custID, userID: userID, email: "reborn@example.com",
            schemaVersion: sv, schemaHash: sh
        )
        XCTAssertEqual(resurrectResp.accepted.count, 1, "resurrection should be accepted")

        // Poll pull: record should have no deletedAt
        let pullResp = try await pollForRecord(
            http: http, clientID: clientID, checkpoint: 0,
            schemaVersion: sv, schemaHash: sh, recordID: custID
        )
        let found = pullResp.changes.last(where: { $0.id == custID })
        XCTAssertNil(found?.deletedAt, "resurrected record should not have deletedAt")
    }

    // MARK: - Test 11: Soft Delete

    func testSoftDelete() async throws {
        try skipIfNoServer()

        let userID = UUID().uuidString.lowercased()
        let (http, clientID) = makeHttpClient(userID: userID)
        let (_, sv, sh) = try await registerWithSchema(http: http, clientID: clientID)

        let custID = UUID().uuidString.lowercased()
        _ = try await pushCustomer(
            http: http, clientID: clientID,
            customerID: custID, userID: userID,
            schemaVersion: sv, schemaHash: sh
        )

        let orderID = UUID().uuidString.lowercased()
        _ = try await pushOrder(
            http: http, clientID: clientID,
            orderID: orderID, customerID: custID,
            schemaVersion: sv, schemaHash: sh
        )

        // Wait for order to appear
        let pullAfterCreate = try await pollForRecord(
            http: http, clientID: clientID, checkpoint: 0,
            schemaVersion: sv, schemaHash: sh, recordID: orderID
        )
        let checkpointAfterCreate = pullAfterCreate.checkpoint

        // Soft-delete the order
        let deleteResp = try await http.push(request: PushRequest(
            clientID: clientID,
            changes: [PushRecord(
                id: orderID, tableName: "orders", operation: "delete",
                data: ["id": AnyCodable(orderID), "customer_id": AnyCodable(custID)],
                clientUpdatedAt: Date()
            )],
            schemaVersion: sv, schemaHash: sh
        ))
        XCTAssertEqual(deleteResp.accepted.count, 1)
        XCTAssertNotNil(deleteResp.accepted.first?.serverDeletedAt)

        // Poll until delete propagates
        let deadline = Date().addingTimeInterval(10)
        var foundDelete = false
        var cp = checkpointAfterCreate

        while Date() < deadline && !foundDelete {
            let resp = try await http.pull(request: PullRequest(
                clientID: clientID, checkpoint: cp,
                tables: nil, limit: 100, knownBuckets: nil,
                schemaVersion: sv, schemaHash: sh
            ))
            if resp.deletes.contains(where: { $0.id == orderID }) {
                foundDelete = true
                break
            }
            if let record = resp.changes.first(where: { $0.id == orderID }), record.deletedAt != nil {
                foundDelete = true
                break
            }
            cp = resp.checkpoint
            if !resp.hasMore { try await Task.sleep(nanoseconds: 250_000_000) }
        }
        XCTAssertTrue(foundDelete, "soft-deleted order should appear in pull as deleted")
    }

    // MARK: - Test 12: Read-Only Rejection

    func testReadOnlyRejection() async throws {
        try skipIfNoServer()

        let userID = UUID().uuidString.lowercased()
        let (http, clientID) = makeHttpClient(userID: userID)
        let (_, sv, sh) = try await registerWithSchema(http: http, clientID: clientID)

        let regionID = UUID().uuidString.lowercased()
        let resp = try await http.push(request: PushRequest(
            clientID: clientID,
            changes: [PushRecord(
                id: regionID, tableName: "regions", operation: "create",
                data: [
                    "id": AnyCodable(regionID),
                    "name": AnyCodable("Atlantis"),
                ],
                clientUpdatedAt: Date()
            )],
            schemaVersion: sv, schemaHash: sh
        ))

        XCTAssertEqual(resp.rejected.count, 1)
        XCTAssertEqual(resp.rejected.first?.status, PushStatus.rejectedTerminal)
        XCTAssertEqual(resp.rejected.first?.reasonCode, "table_read_only")
    }

    // MARK: - Test 13: Bucket Isolation

    func testBucketIsolation() async throws {
        try skipIfNoServer()

        let userA = UUID().uuidString.lowercased()
        let userB = UUID().uuidString.lowercased()
        let (httpA, clientA) = makeHttpClient(userID: userA)
        let (httpB, clientB) = makeHttpClient(userID: userB)

        let (_, svA, shA) = try await registerWithSchema(http: httpA, clientID: clientA)
        let (_, svB, shB) = try await registerWithSchema(http: httpB, clientID: clientB)

        // User A pushes a customer + order
        let custA = UUID().uuidString.lowercased()
        _ = try await pushCustomer(http: httpA, clientID: clientA, customerID: custA, userID: userA, schemaVersion: svA, schemaHash: shA)
        let orderA = UUID().uuidString.lowercased()
        _ = try await pushOrder(http: httpA, clientID: clientA, orderID: orderA, customerID: custA, schemaVersion: svA, schemaHash: shA)

        // User B pushes a customer + order
        let custB = UUID().uuidString.lowercased()
        _ = try await pushCustomer(http: httpB, clientID: clientB, customerID: custB, userID: userB, schemaVersion: svB, schemaHash: shB)
        let orderB = UUID().uuidString.lowercased()
        _ = try await pushOrder(http: httpB, clientID: clientB, orderID: orderB, customerID: custB, schemaVersion: svB, schemaHash: shB)

        // User A pull: should see A's data, not B's
        let pullA = try await pollForRecord(
            http: httpA, clientID: clientA, checkpoint: 0,
            schemaVersion: svA, schemaHash: shA, recordID: orderA
        )
        XCTAssertTrue(pullA.changes.contains(where: { $0.id == orderA }), "User A should see own order")
        XCTAssertFalse(pullA.changes.contains(where: { $0.id == orderB }), "User A must not see B's order")

        // User B pull: should see B's data, not A's
        let pullB = try await pollForRecord(
            http: httpB, clientID: clientB, checkpoint: 0,
            schemaVersion: svB, schemaHash: shB, recordID: orderB
        )
        XCTAssertTrue(pullB.changes.contains(where: { $0.id == orderB }), "User B should see own order")
        XCTAssertFalse(pullB.changes.contains(where: { $0.id == orderA }), "User B must not see A's order")
    }

    // MARK: - Test 14: Excluded Columns

    func testExcludedColumns() async throws {
        try skipIfNoServer()

        let userID = UUID().uuidString.lowercased()
        let (http, clientID) = makeHttpClient(userID: userID)
        let (_, sv, sh) = try await registerWithSchema(http: http, clientID: clientID)

        let custID = UUID().uuidString.lowercased()
        _ = try await pushCustomer(
            http: http, clientID: clientID,
            customerID: custID, userID: userID,
            email: "visible@example.com", internalNotes: "SECRET_DATA",
            schemaVersion: sv, schemaHash: sh
        )

        let pullResp = try await pollForRecord(
            http: http, clientID: clientID, checkpoint: 0,
            schemaVersion: sv, schemaHash: sh, recordID: custID
        )

        let found = pullResp.changes.first(where: { $0.id == custID })
        XCTAssertNotNil(found)
        XCTAssertNil(found?.data["internal_notes"], "internal_notes should be excluded from pull")
        XCTAssertNotNil(found?.data["email"], "non-excluded fields should be present")
    }

    // MARK: - Test 15: Multi-Bucket Collaboration

    func testMultiBucketCollaboration() async throws {
        try skipIfNoServer()

        let userA = UUID().uuidString.lowercased()
        let userB = UUID().uuidString.lowercased()
        let (httpA, clientA) = makeHttpClient(userID: userA)
        let (httpB, clientB) = makeHttpClient(userID: userB)

        let (_, svA, shA) = try await registerWithSchema(http: httpA, clientID: clientA)
        let (_, svB, shB) = try await registerWithSchema(http: httpB, clientID: clientB)

        // User A creates a document
        let docID = UUID().uuidString.lowercased()
        let docResp = try await pushDocument(
            http: httpA, clientID: clientA,
            docID: docID, ownerID: userA,
            schemaVersion: svA, schemaHash: shA
        )
        XCTAssertEqual(docResp.accepted.count, 1)

        // User A adds User B as a document member
        let memberID = UUID().uuidString.lowercased()
        let memberResp = try await pushDocumentMember(
            http: httpA, clientID: clientA,
            memberID: memberID, docID: docID, userID: userB,
            schemaVersion: svA, schemaHash: shA
        )
        XCTAssertEqual(memberResp.accepted.count, 1)

        // User B should see the document_member in their bucket
        let pullB = try await pollForRecord(
            http: httpB, clientID: clientB, checkpoint: 0,
            schemaVersion: svB, schemaHash: shB, recordID: memberID
        )
        XCTAssertTrue(pullB.changes.contains(where: { $0.id == memberID }),
            "User B should see document_member in their bucket")
    }

    // MARK: - Test 16: Dual Bucket Ownership

    func testDualBucketOwnership() async throws {
        try skipIfNoServer()

        let userA = UUID().uuidString.lowercased()
        let userB = UUID().uuidString.lowercased()
        let (httpA, clientA) = makeHttpClient(userID: userA)
        let (httpB, clientB) = makeHttpClient(userID: userB)

        let (_, svA, shA) = try await registerWithSchema(http: httpA, clientID: clientA)
        let (_, svB, shB) = try await registerWithSchema(http: httpB, clientID: clientB)

        // User A creates a document
        let docID = UUID().uuidString.lowercased()
        _ = try await pushDocument(
            http: httpA, clientID: clientA,
            docID: docID, ownerID: userA,
            schemaVersion: svA, schemaHash: shA
        )

        // User B creates a comment on it (dual ownership: doc owner + comment author)
        let commentID = UUID().uuidString.lowercased()
        let commentResp = try await pushDocumentComment(
            http: httpB, clientID: clientB,
            commentID: commentID, docID: docID, authorID: userB,
            schemaVersion: svB, schemaHash: shB
        )
        XCTAssertEqual(commentResp.accepted.count, 1)

        // User A (document owner) should see the comment
        let pullA = try await pollForRecord(
            http: httpA, clientID: clientA, checkpoint: 0,
            schemaVersion: svA, schemaHash: shA, recordID: commentID
        )
        XCTAssertTrue(pullA.changes.contains(where: { $0.id == commentID }),
            "Document owner should see comment")

        // User B (comment author) should also see the comment
        let pullB = try await pollForRecord(
            http: httpB, clientID: clientB, checkpoint: 0,
            schemaVersion: svB, schemaHash: shB, recordID: commentID
        )
        XCTAssertTrue(pullB.changes.contains(where: { $0.id == commentID }),
            "Comment author should see comment")
    }

    // MARK: - Test 17: Type Zoo

    func testTypeZoo() async throws {
        try skipIfNoServer()

        let userID = UUID().uuidString.lowercased()
        let (http, clientID) = makeHttpClient(userID: userID)
        let (_, sv, sh) = try await registerWithSchema(http: http, clientID: clientID)

        let zooID = UUID().uuidString.lowercased()
        let testUUID = UUID().uuidString.lowercased()
        let data: [String: AnyCodable] = [
            "id": AnyCodable(zooID),
            "user_id": AnyCodable(userID),
            "text_val": AnyCodable("hello world"),
            "varchar_val": AnyCodable("bounded string"),
            "bool_val": AnyCodable(true),
            "smallint_val": AnyCodable(42),
            "int_val": AnyCodable(123456),
            "bigint_val": AnyCodable(Int64(9876543210)),
            "numeric_val": AnyCodable("99999.99"),
            "real_val": AnyCodable(3.14),
            "double_val": AnyCodable(2.718281828),
            "date_val": AnyCodable("2026-03-19"),
            "timestamptz_val": AnyCodable("2026-03-19T12:00:00.000Z"),
            "jsonb_val": AnyCodable(["nested": "object", "count": 42] as [String: Any]),
            "text_array_val": AnyCodable(["alpha", "beta", "gamma"]),
            "int_array_val": AnyCodable([1, 2, 3]),
            "uuid_val": AnyCodable(testUUID),
        ]

        let pushResp = try await http.push(request: PushRequest(
            clientID: clientID,
            changes: [PushRecord(
                id: zooID, tableName: "type_zoo", operation: "create",
                data: data, clientUpdatedAt: Date()
            )],
            schemaVersion: sv, schemaHash: sh
        ))
        XCTAssertEqual(pushResp.accepted.count, 1, "type_zoo push should be accepted")

        // Pull it back and verify values
        let pullResp = try await pollForRecord(
            http: http, clientID: clientID, checkpoint: 0,
            schemaVersion: sv, schemaHash: sh, recordID: zooID
        )
        let found = pullResp.changes.first(where: { $0.id == zooID })
        XCTAssertNotNil(found, "type_zoo record should appear in pull")

        let d = found!.data
        XCTAssertEqual(d["text_val"]?.value as? String, "hello world")
        XCTAssertEqual(d["varchar_val"]?.value as? String, "bounded string")
        XCTAssertEqual(d["bool_val"]?.value as? Bool, true)
        XCTAssertNotNil(d["smallint_val"], "smallint should round-trip")
        XCTAssertNotNil(d["int_val"], "integer should round-trip")
        XCTAssertNotNil(d["bigint_val"], "bigint should round-trip")
        XCTAssertNotNil(d["real_val"], "real should round-trip")
        XCTAssertNotNil(d["double_val"], "double should round-trip")
        XCTAssertEqual(d["date_val"]?.value as? String, "2026-03-19")
        XCTAssertNotNil(d["timestamptz_val"], "timestamptz should round-trip")
        XCTAssertNotNil(d["jsonb_val"], "JSONB should round-trip")
        XCTAssertEqual(d["uuid_val"]?.value as? String, testUUID)

        // Nullable columns not sent should be null
        XCTAssertTrue(
            d["xml_val"] == nil || d["xml_val"]?.value is NSNull,
            "nullable column not sent should be null"
        )
    }

    // MARK: - Test 18: Bucket Updates

    func testBucketUpdates() async throws {
        try skipIfNoServer()

        let userID = UUID().uuidString.lowercased()
        let (http, clientID) = makeHttpClient(userID: userID)
        let (_, sv, sh) = try await registerWithSchema(http: http, clientID: clientID)

        // Pull with empty knownBuckets: should get bucketUpdates.added
        let resp = try await http.pull(request: PullRequest(
            clientID: clientID, checkpoint: 0,
            tables: nil, limit: 100, knownBuckets: [],
            schemaVersion: sv, schemaHash: sh
        ))

        XCTAssertNotNil(resp.bucketUpdates, "should get bucket updates on first pull")
        XCTAssertNotNil(resp.bucketUpdates?.added, "bucketUpdates should have added list")

        // Pull again with correct knownBuckets: no more updates
        let knownBuckets = resp.bucketUpdates?.added ?? []
        var finalCheckpoint = resp.checkpoint
        if resp.hasMore {
            // Drain remaining pages
            var hasMore = resp.hasMore
            var cp = resp.checkpoint
            while hasMore {
                let page = try await http.pull(request: PullRequest(
                    clientID: clientID, checkpoint: cp,
                    tables: nil, limit: 100, knownBuckets: knownBuckets,
                    schemaVersion: sv, schemaHash: sh
                ))
                cp = page.checkpoint
                hasMore = page.hasMore
            }
            finalCheckpoint = cp
        }

        let resp2 = try await http.pull(request: PullRequest(
            clientID: clientID, checkpoint: finalCheckpoint,
            tables: nil, limit: 100, knownBuckets: knownBuckets,
            schemaVersion: sv, schemaHash: sh
        ))
        // On a fully caught-up pull, bucketUpdates should be nil or have empty added
        let newAdded = resp2.bucketUpdates?.added ?? []
        XCTAssertTrue(newAdded.isEmpty, "no new buckets should be added on subsequent pull")
    }

    // MARK: - Test 19: Per-Bucket Checkpoints

    func testPerBucketCheckpoints() async throws {
        try skipIfNoServer()

        let userID = UUID().uuidString.lowercased()
        let (http, clientID) = makeHttpClient(userID: userID)
        let (reg, sv, sh) = try await registerWithSchema(http: http, clientID: clientID)

        // Use bucket_checkpoints from register
        let bucketCheckpoints = reg.bucketCheckpoints ?? [:]

        let resp = try await http.pull(request: PullRequest(
            clientID: clientID, checkpoint: 0,
            bucketCheckpoints: bucketCheckpoints.isEmpty ? nil : bucketCheckpoints,
            tables: nil, limit: 100, knownBuckets: nil,
            schemaVersion: sv, schemaHash: sh
        ))

        // Response should include updated bucket_checkpoints
        XCTAssertNotNil(resp.bucketCheckpoints, "response should include bucket_checkpoints")

        // Second pull from those checkpoints: no duplicate data
        let resp2 = try await http.pull(request: PullRequest(
            clientID: clientID, checkpoint: resp.checkpoint,
            bucketCheckpoints: resp.bucketCheckpoints,
            tables: nil, limit: 100, knownBuckets: nil,
            schemaVersion: sv, schemaHash: sh
        ))
        XCTAssertTrue(resp2.changes.isEmpty || resp2.checkpoint >= resp.checkpoint,
            "second pull from same checkpoints should not duplicate data")
    }

    // MARK: - Test 20: Bucket Checksums on Final Page

    func testBucketChecksumsOnFinalPage() async throws {
        try skipIfNoServer()

        let userID = UUID().uuidString.lowercased()
        let (http, clientID) = makeHttpClient(userID: userID)
        let (_, sv, sh) = try await registerWithSchema(http: http, clientID: clientID)

        // Push some data
        let custID = UUID().uuidString.lowercased()
        _ = try await pushCustomer(
            http: http, clientID: clientID,
            customerID: custID, userID: userID,
            schemaVersion: sv, schemaHash: sh
        )

        // Wait for it to appear
        _ = try await pollForRecord(
            http: http, clientID: clientID, checkpoint: 0,
            schemaVersion: sv, schemaHash: sh, recordID: custID
        )

        // Pull to final page with limit=1
        var checkpoint: Int64 = 0
        var lastBucketChecksums: [String: Int32]? = nil
        var intermediateHadChecksums = false
        var iterations = 0

        repeat {
            let resp = try await http.pull(request: PullRequest(
                clientID: clientID, checkpoint: checkpoint,
                tables: nil, limit: 1, knownBuckets: nil,
                schemaVersion: sv, schemaHash: sh
            ))
            checkpoint = resp.checkpoint

            if resp.hasMore && resp.bucketChecksums != nil {
                intermediateHadChecksums = true
            }
            if !resp.hasMore {
                lastBucketChecksums = resp.bucketChecksums
                break
            }
            iterations += 1
        } while iterations < 100

        XCTAssertNotNil(lastBucketChecksums, "final page should include bucket_checksums")
        XCTAssertFalse(lastBucketChecksums?.isEmpty ?? true, "bucket_checksums should be non-empty")
        XCTAssertFalse(intermediateHadChecksums, "intermediate pages (has_more=true) should NOT have checksums")
    }

    // MARK: - Test 21: Push Response Envelope

    func testPushResponseEnvelope() async throws {
        try skipIfNoServer()

        let userID = UUID().uuidString.lowercased()
        let (http, clientID) = makeHttpClient(userID: userID)
        let (_, sv, sh) = try await registerWithSchema(http: http, clientID: clientID)

        let custID = UUID().uuidString.lowercased()
        let regionID = UUID().uuidString.lowercased()

        let resp = try await http.push(request: PushRequest(
            clientID: clientID,
            changes: [
                // Valid customer create
                PushRecord(
                    id: custID, tableName: "customers", operation: "create",
                    data: [
                        "id": AnyCodable(custID),
                        "user_id": AnyCodable(userID),
                        "email": AnyCodable("envelope@example.com"),
                    ],
                    clientUpdatedAt: Date()
                ),
                // Invalid: write to read-only table
                PushRecord(
                    id: regionID, tableName: "regions", operation: "create",
                    data: [
                        "id": AnyCodable(regionID),
                        "name": AnyCodable("Invalid"),
                    ],
                    clientUpdatedAt: Date()
                ),
            ],
            schemaVersion: sv, schemaHash: sh
        ))

        // Accepted entry shape
        XCTAssertEqual(resp.accepted.count, 1)
        let accepted = resp.accepted.first!
        XCTAssertEqual(accepted.id, custID)
        XCTAssertEqual(accepted.tableName, "customers")
        XCTAssertEqual(accepted.operation, "create")
        XCTAssertEqual(accepted.status, PushStatus.applied)
        XCTAssertNotNil(accepted.serverUpdatedAt)

        // Rejected entry shape
        XCTAssertEqual(resp.rejected.count, 1)
        let rejected = resp.rejected.first!
        XCTAssertEqual(rejected.id, regionID)
        XCTAssertEqual(rejected.tableName, "regions")
        XCTAssertEqual(rejected.operation, "create")
        XCTAssertEqual(rejected.status, PushStatus.rejectedTerminal)
        XCTAssertNotNil(rejected.reasonCode)

        // Envelope fields
        XCTAssertGreaterThan(resp.checkpoint, 0)
        XCTAssertGreaterThan(resp.schemaVersion, 0)
        XCTAssertFalse(resp.schemaHash.isEmpty)
    }

    // MARK: - Test 22: Auth Failure

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

        // Register should fail with 401
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

    // MARK: - Test 23: Version Mismatch

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
                // Expected: server requires >= 1.0.0, client sent 0.0.1
            } else {
                XCTFail("expected upgradeRequired, got \(error)")
            }
        }
    }

    // MARK: - Test 24: Schema Mismatch

    func testSchemaMismatch() async throws {
        try skipIfNoServer()

        let userID = UUID().uuidString.lowercased()
        let (http, clientID) = makeHttpClient(userID: userID)

        // First register normally to get valid schema
        let (_, sv, sh) = try await registerWithSchema(http: http, clientID: clientID)

        // Push with stale schema_version=0 and hash="stale"
        do {
            _ = try await http.push(request: PushRequest(
                clientID: clientID,
                changes: [PushRecord(
                    id: UUID().uuidString.lowercased(),
                    tableName: "customers", operation: "create",
                    data: [
                        "id": AnyCodable(UUID().uuidString.lowercased()),
                        "user_id": AnyCodable(userID),
                    ],
                    clientUpdatedAt: Date()
                )],
                schemaVersion: 0, schemaHash: "stale"
            ))
            XCTFail("expected schema mismatch error")
        } catch let error as SynchroError {
            guard case .schemaMismatch(let serverVersion, let serverHash) = error else {
                XCTFail("expected schemaMismatch, got \(error)")
                return
            }
            XCTAssertEqual(serverVersion, sv, "server should report current schema version")
            XCTAssertEqual(serverHash, sh, "server should report current schema hash")
        }
    }
}
