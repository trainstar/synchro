import XCTest
import Foundation
@testable import Synchro

// MARK: - Mock URLProtocol

final class MockURLProtocol: URLProtocol {
    static var requestHandler: ((URLRequest) throws -> (HTTPURLResponse, Data))?

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let handler = MockURLProtocol.requestHandler else {
            client?.urlProtocolDidFinishLoading(self)
            return
        }
        do {
            let (response, data) = try handler(request)
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: data)
            client?.urlProtocolDidFinishLoading(self)
        } catch {
            client?.urlProtocol(self, didFailWithError: error)
        }
    }

    override func stopLoading() {}
}

extension URLRequest {
    func bodyData() -> Data? {
        if let httpBody {
            return httpBody
        }
        guard let stream = httpBodyStream else { return nil }
        stream.open()
        defer { stream.close() }
        var data = Data()
        let bufferSize = 4096
        let buffer = UnsafeMutablePointer<UInt8>.allocate(capacity: bufferSize)
        defer { buffer.deallocate() }
        while stream.hasBytesAvailable {
            let read = stream.read(buffer, maxLength: bufferSize)
            if read <= 0 { break }
            data.append(buffer, count: read)
        }
        return data
    }
}

final class HttpClientTests: XCTestCase {
    private var session: URLSession!
    private var httpClient: HttpClient!

    override func setUp() {
        super.setUp()
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [MockURLProtocol.self]
        session = URLSession(configuration: config)

        let synchroConfig = SynchroConfig(
            dbPath: "",
            serverURL: URL(string: "http://test.local")!,
            authProvider: { "test-token" },
            clientID: "test-device",
            appVersion: "1.0.0"
        )
        httpClient = HttpClient(config: synchroConfig, session: session)
    }

    override func tearDown() {
        MockURLProtocol.requestHandler = nil
        super.tearDown()
    }

    func testRegisterSuccess() async throws {
        let responseBody: [String: Any] = [
            "id": "server-id-123",
            "server_time": "2026-01-01T12:00:00.000Z",
            "checkpoint": 0,
            "schema_version": 1,
            "schema_hash": "abc123",
        ]

        MockURLProtocol.requestHandler = { request in
            XCTAssertEqual(request.httpMethod, "POST")
            XCTAssertTrue(request.url!.path.hasSuffix("/sync/register"))
            XCTAssertEqual(request.value(forHTTPHeaderField: "Authorization"), "Bearer test-token")
            XCTAssertEqual(request.value(forHTTPHeaderField: "X-App-Version"), "1.0.0")
            XCTAssertEqual(request.value(forHTTPHeaderField: "Content-Type"), "application/json")

            // Verify request body
            let body = try JSONSerialization.jsonObject(with: request.bodyData()!) as! [String: Any]
            XCTAssertEqual(body["client_id"] as? String, "test-device")
            XCTAssertEqual(body["platform"] as? String, "ios")

            let data = try JSONSerialization.data(withJSONObject: responseBody)
            let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!
            return (response, data)
        }

        let req = RegisterRequest(clientID: "test-device", platform: "ios", appVersion: "1.0.0", schemaVersion: 0, schemaHash: "")
        let resp = try await httpClient.register(request: req)
        XCTAssertEqual(resp.id, "server-id-123")
        XCTAssertEqual(resp.schemaVersion, 1)
        XCTAssertEqual(resp.schemaHash, "abc123")
    }

    func testFetchSchemaSuccess() async throws {
        let responseBody: [String: Any] = [
            "schema_version": 3,
            "schema_hash": "def456",
            "server_time": "2026-01-01T12:00:00.000Z",
            "tables": [
                [
                    "table_name": "workouts",
                    "push_policy": "owner_only",
                    "updated_at_column": "updated_at",
                    "deleted_at_column": "deleted_at",
                    "primary_key": ["id"],
                    "columns": [
                        ["name": "id", "db_type": "uuid", "logical_type": "string", "nullable": false, "is_primary_key": true]
                    ]
                ] as [String : Any]
            ]
        ]

        MockURLProtocol.requestHandler = { request in
            XCTAssertEqual(request.httpMethod, "GET")
            XCTAssertTrue(request.url!.path.hasSuffix("/sync/schema"))
            let data = try JSONSerialization.data(withJSONObject: responseBody)
            let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
            return (response, data)
        }

        let resp = try await httpClient.fetchSchema()
        XCTAssertEqual(resp.schemaVersion, 3)
        XCTAssertEqual(resp.tables.count, 1)
        XCTAssertEqual(resp.tables[0].tableName, "workouts")
    }

    func testSchemaMismatch409() async throws {
        let responseBody: [String: Any] = [
            "code": "schema_mismatch",
            "message": "client schema does not match server schema",
            "server_schema_version": 5,
            "server_schema_hash": "newHash",
        ]

        MockURLProtocol.requestHandler = { request in
            let data = try JSONSerialization.data(withJSONObject: responseBody)
            let response = HTTPURLResponse(url: request.url!, statusCode: 409, httpVersion: nil, headerFields: nil)!
            return (response, data)
        }

        let req = PullRequest(clientID: "test", checkpoint: 0, schemaVersion: 1, schemaHash: "old")
        do {
            _ = try await httpClient.pull(request: req)
            XCTFail("Expected schemaMismatch error")
        } catch let error as SynchroError {
            switch error {
            case .schemaMismatch(let version, let hash):
                XCTAssertEqual(version, 5)
                XCTAssertEqual(hash, "newHash")
            default:
                XCTFail("Expected schemaMismatch, got \(error)")
            }
        }
    }

    func testUpgradeRequired426() async throws {
        let responseBody = ["error": "client upgrade required"]

        MockURLProtocol.requestHandler = { request in
            let data = try JSONSerialization.data(withJSONObject: responseBody)
            let response = HTTPURLResponse(url: request.url!, statusCode: 426, httpVersion: nil, headerFields: nil)!
            return (response, data)
        }

        let req = RegisterRequest(clientID: "test", platform: "ios", appVersion: "0.1.0", schemaVersion: 0, schemaHash: "")
        do {
            _ = try await httpClient.register(request: req)
            XCTFail("Expected upgradeRequired error")
        } catch let error as SynchroError {
            switch error {
            case .upgradeRequired(let current, _):
                XCTAssertEqual(current, "1.0.0")
            default:
                XCTFail("Expected upgradeRequired, got \(error)")
            }
        }
    }

    func testRetryAfter429() async throws {
        let responseBody: [String: Any] = ["error": "rate limited", "retry_after": 10]

        MockURLProtocol.requestHandler = { request in
            let data = try JSONSerialization.data(withJSONObject: responseBody)
            let response = HTTPURLResponse(url: request.url!, statusCode: 429, httpVersion: nil, headerFields: ["Retry-After": "10"])!
            return (response, data)
        }

        let req = PushRequest(clientID: "test", changes: [], schemaVersion: 1, schemaHash: "abc")
        do {
            _ = try await httpClient.push(request: req)
            XCTFail("Expected retryable error")
        } catch let error as RetryableError {
            XCTAssertEqual(error.retryAfter, 10)
            switch error.underlying {
            case .serverError(let status, _):
                XCTAssertEqual(status, 429)
            default:
                XCTFail("Expected serverError")
            }
        }
    }

    func testRetryAfter503() async throws {
        let responseBody: [String: Any] = ["error": "service temporarily unavailable", "retry_after": 5]

        MockURLProtocol.requestHandler = { request in
            let data = try JSONSerialization.data(withJSONObject: responseBody)
            let response = HTTPURLResponse(url: request.url!, statusCode: 503, httpVersion: nil, headerFields: ["Retry-After": "5"])!
            return (response, data)
        }

        let req = PullRequest(clientID: "test", checkpoint: 0, schemaVersion: 1, schemaHash: "abc")
        do {
            _ = try await httpClient.pull(request: req)
            XCTFail("Expected retryable error")
        } catch let error as RetryableError {
            XCTAssertEqual(error.retryAfter, 5)
        }
    }

    func testServerError500() async throws {
        let responseBody = ["error": "internal server error"]

        MockURLProtocol.requestHandler = { request in
            let data = try JSONSerialization.data(withJSONObject: responseBody)
            let response = HTTPURLResponse(url: request.url!, statusCode: 500, httpVersion: nil, headerFields: nil)!
            return (response, data)
        }

        let req = PullRequest(clientID: "test", checkpoint: 0, schemaVersion: 1, schemaHash: "abc")
        do {
            _ = try await httpClient.pull(request: req)
            XCTFail("Expected serverError")
        } catch let error as SynchroError {
            switch error {
            case .serverError(let status, let msg):
                XCTAssertEqual(status, 500)
                XCTAssertEqual(msg, "internal server error")
            default:
                XCTFail("Expected serverError, got \(error)")
            }
        }
    }

    func testPullRequestEncoding() async throws {
        let pullResponseBody: [String: Any] = [
            "changes": [] as [Any],
            "deletes": [] as [Any],
            "checkpoint": 42,
            "has_more": false,
            "schema_version": 1,
            "schema_hash": "abc",
        ]

        var capturedBody: [String: Any]?

        MockURLProtocol.requestHandler = { request in
            capturedBody = try JSONSerialization.jsonObject(with: request.bodyData()!) as? [String: Any]
            let data = try JSONSerialization.data(withJSONObject: pullResponseBody)
            let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
            return (response, data)
        }

        let req = PullRequest(
            clientID: "dev-1",
            checkpoint: 100,
            tables: ["workouts"],
            limit: 50,
            knownBuckets: ["user:123", "global"],
            schemaVersion: 7,
            schemaHash: "hash7"
        )
        _ = try await httpClient.pull(request: req)

        XCTAssertEqual(capturedBody?["client_id"] as? String, "dev-1")
        XCTAssertEqual(capturedBody?["checkpoint"] as? Int, 100)
        XCTAssertEqual(capturedBody?["tables"] as? [String], ["workouts"])
        XCTAssertEqual(capturedBody?["limit"] as? Int, 50)
        XCTAssertEqual(capturedBody?["known_buckets"] as? [String], ["user:123", "global"])
        XCTAssertEqual(capturedBody?["schema_version"] as? Int, 7)
        XCTAssertEqual(capturedBody?["schema_hash"] as? String, "hash7")
    }

    func testResyncRequestEncoding() async throws {
        let resyncResponseBody: [String: Any] = [
            "records": [] as [Any],
            "checkpoint": 50,
            "has_more": true,
            "schema_version": 1,
            "schema_hash": "abc",
        ]

        var capturedBody: [String: Any]?

        MockURLProtocol.requestHandler = { request in
            XCTAssertEqual(request.httpMethod, "POST")
            XCTAssertTrue(request.url!.path.hasSuffix("/sync/resync"))
            capturedBody = try JSONSerialization.jsonObject(with: request.bodyData()!) as? [String: Any]
            let data = try JSONSerialization.data(withJSONObject: resyncResponseBody)
            let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
            return (response, data)
        }

        let req = ResyncRequest(
            clientID: "dev-1",
            cursor: ResyncCursor(checkpoint: 10, tableIndex: 0, afterID: "w5"),
            limit: 100,
            schemaVersion: 3,
            schemaHash: "hash3"
        )
        let resp = try await httpClient.resync(request: req)

        XCTAssertEqual(capturedBody?["client_id"] as? String, "dev-1")
        XCTAssertEqual(capturedBody?["limit"] as? Int, 100)
        XCTAssertEqual(capturedBody?["schema_version"] as? Int, 3)
        let cursor = capturedBody?["cursor"] as? [String: Any]
        XCTAssertEqual(cursor?["checkpoint"] as? Int, 10)
        XCTAssertEqual(cursor?["table_idx"] as? Int, 0)
        XCTAssertEqual(cursor?["after_id"] as? String, "w5")
        XCTAssertEqual(resp.checkpoint, 50)
        XCTAssertTrue(resp.hasMore)
    }

    func testFetchTablesSuccess() async throws {
        let responseBody: [String: Any] = [
            "server_time": "2026-01-01T12:00:00.000Z",
            "schema_version": 2,
            "schema_hash": "xyz",
            "tables": [
                [
                    "table_name": "workouts",
                    "push_policy": "owner_only",
                    "dependencies": [] as [String],
                ] as [String : Any]
            ]
        ]

        MockURLProtocol.requestHandler = { request in
            XCTAssertEqual(request.httpMethod, "GET")
            XCTAssertTrue(request.url!.path.hasSuffix("/sync/tables"))
            let data = try JSONSerialization.data(withJSONObject: responseBody)
            let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
            return (response, data)
        }

        let resp = try await httpClient.fetchTables()
        XCTAssertEqual(resp.schemaVersion, 2)
        XCTAssertEqual(resp.tables.count, 1)
        XCTAssertEqual(resp.tables[0].tableName, "workouts")
        XCTAssertEqual(resp.tables[0].pushPolicy, "owner_only")
    }

    func testPushRequestEncoding() async throws {
        let pushResponseBody: [String: Any] = [
            "accepted": [] as [Any],
            "rejected": [] as [Any],
            "checkpoint": 0,
            "server_time": "2026-01-01T12:00:00.000Z",
            "schema_version": 1,
            "schema_hash": "abc",
        ]

        var capturedBody: [String: Any]?

        MockURLProtocol.requestHandler = { request in
            capturedBody = try JSONSerialization.jsonObject(with: request.bodyData()!) as? [String: Any]
            let data = try JSONSerialization.data(withJSONObject: pushResponseBody)
            let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
            return (response, data)
        }

        let req = PushRequest(
            clientID: "dev-1",
            changes: [
                PushRecord(
                    id: "rec-1",
                    tableName: "workouts",
                    operation: "create",
                    data: ["name": AnyCodable("Push Day")],
                    clientUpdatedAt: ISO8601DateFormatter().date(from: "2026-01-01T12:00:00Z")!
                )
            ],
            schemaVersion: 7,
            schemaHash: "hash7"
        )
        _ = try await httpClient.push(request: req)

        XCTAssertEqual(capturedBody?["client_id"] as? String, "dev-1")
        let changes = capturedBody?["changes"] as? [[String: Any]]
        XCTAssertEqual(changes?.count, 1)
        XCTAssertEqual(changes?[0]["id"] as? String, "rec-1")
        XCTAssertEqual(changes?[0]["operation"] as? String, "create")
    }
}
