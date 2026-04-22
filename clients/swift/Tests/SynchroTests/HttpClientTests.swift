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

    func testFetchSchemaSuccess() async throws {
        let responseBody: [String: Any] = [
            "schema_version": 3,
            "schema_hash": "def456",
            "server_time": "2026-01-01T12:00:00.000Z",
            "manifest": [
                "tables": [
                    [
                        "name": "orders",
                        "primary_key": ["id"],
                        "updated_at_column": "updated_at",
                        "deleted_at_column": "deleted_at",
                        "columns": [
                            ["name": "id", "type": "string", "nullable": false],
                            ["name": "updated_at", "type": "datetime", "nullable": false],
                            ["name": "deleted_at", "type": "datetime", "nullable": true]
                        ]
                    ] as [String: Any]
                ]
            ],
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
        XCTAssertEqual(resp.tables[0].tableName, "orders")
    }

    func testConnectSuccess() async throws {
        let responseBody: [String: Any] = [
            "server_time": "2026-03-20T18:22:11Z",
            "protocol_version": 1,
            "scope_set_version": 13,
            "schema": [
                "version": 8,
                "hash": "8b21d2a1",
                "action": "none",
            ],
            "scopes": [
                "add": [],
                "remove": [],
            ],
        ]

        MockURLProtocol.requestHandler = { request in
            XCTAssertEqual(request.httpMethod, "POST")
            XCTAssertTrue(request.url!.path.hasSuffix("/sync/connect"))
            let body = try JSONSerialization.jsonObject(with: request.bodyData()!) as! [String: Any]
            XCTAssertEqual(body["client_id"] as? String, "test-device")
            XCTAssertEqual(body["protocol_version"] as? Int, 1)

            let data = try JSONSerialization.data(withJSONObject: responseBody)
            let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!
            return (response, data)
        }

        let req = ConnectRequest(
            clientID: "test-device",
            platform: "ios",
            appVersion: "1.0.0",
            protocolVersion: 1,
            schema: .init(version: 8, hash: "8b21d2a1"),
            scopeSetVersion: 13,
            knownScopes: [:]
        )
        let resp = try await httpClient.connect(request: req)
        XCTAssertEqual(resp.schema.action, .none)
        try resp.validate()
    }

    func testPullEncoding() async throws {
        let responseBody: [String: Any] = [
            "changes": [],
            "scope_set_version": 13,
            "scope_cursors": [
                "workouts_user:u_123": "c_890",
            ],
            "scope_updates": [
                "add": [],
                "remove": [],
            ],
            "rebuild": [],
            "has_more": false,
            "checksums": [
                "workouts_user:u_123": "cs_a19d",
            ],
        ]

        MockURLProtocol.requestHandler = { request in
            XCTAssertEqual(request.httpMethod, "POST")
            XCTAssertTrue(request.url!.path.hasSuffix("/sync/pull"))
            let body = try JSONSerialization.jsonObject(with: request.bodyData()!) as! [String: Any]
            XCTAssertEqual(body["client_id"] as? String, "test-device")
            XCTAssertEqual(body["scope_set_version"] as? Int64, 13)
            XCTAssertEqual(body["checksum_mode"] as? String, "required")

            let data = try JSONSerialization.data(withJSONObject: responseBody)
            let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!
            return (response, data)
        }

        let req = PullRequest(
            clientID: "test-device",
            schema: .init(version: 8, hash: "8b21d2a1"),
            scopeSetVersion: 13,
            scopes: ["workouts_user:u_123": .init(cursor: "c_890")],
            limit: 100,
            checksumMode: .required
        )
        let resp = try await httpClient.pull(request: req)
        try resp.validate(for: req)
        XCTAssertEqual(resp.scopeSetVersion, 13)
        XCTAssertEqual(resp.scopeCursors["workouts_user:u_123"], "c_890")
    }

    func testSchemaMismatch422() async throws {
        let responseBody: [String: Any] = [
            "error": [
                "code": "schema_mismatch",
                "message": "client schema does not match server schema",
                "retryable": false,
            ],
        ]

        MockURLProtocol.requestHandler = { request in
            let data = try JSONSerialization.data(withJSONObject: responseBody)
            let response = HTTPURLResponse(url: request.url!, statusCode: 422, httpVersion: nil, headerFields: nil)!
            return (response, data)
        }

        let req = PullRequest(
            clientID: "test",
            schema: .init(version: 1, hash: "old"),
            scopeSetVersion: 0,
            scopes: [:],
            limit: 100,
            checksumMode: ChecksumMode.none
        )
        do {
            _ = try await httpClient.pull(request: req)
            XCTFail("Expected schemaMismatch error")
        } catch let error as SynchroError {
            switch error {
            case .schemaMismatch(let version, let hash):
                XCTAssertEqual(version, 0)
                XCTAssertEqual(hash, "")
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

        let req = ConnectRequest(
            clientID: "test",
            platform: "ios",
            appVersion: "0.1.0",
            protocolVersion: 1,
            schema: .init(version: 0, hash: ""),
            scopeSetVersion: 0,
            knownScopes: [:]
        )
        do {
            _ = try await httpClient.connect(request: req)
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

        let req = PushRequest(
            clientID: "test",
            batchID: "batch-1",
            schema: .init(version: 1, hash: "abc"),
            mutations: []
        )
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

        let req = PullRequest(
            clientID: "test",
            schema: .init(version: 1, hash: "abc"),
            scopeSetVersion: 0,
            scopes: [:],
            limit: 100,
            checksumMode: ChecksumMode.none
        )
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

        let req = PullRequest(
            clientID: "test",
            schema: .init(version: 1, hash: "abc"),
            scopeSetVersion: 0,
            scopes: [:],
            limit: 100,
            checksumMode: ChecksumMode.none
        )
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

    func testFetchTablesSuccess() async throws {
        let responseBody: [String: Any] = [
            "server_time": "2026-01-01T12:00:00.000Z",
            "schema_version": 2,
            "schema_hash": "xyz",
            "tables": [
                [
                    "table_name": "orders",
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
        XCTAssertEqual(resp.tables[0].tableName, "orders")
        XCTAssertEqual(resp.tables[0].pushPolicy, "owner_only")
    }

    func testPushRequestEncoding() async throws {
        let pushResponseBody: [String: Any] = [
            "accepted": [] as [Any],
            "rejected": [] as [Any],
            "server_time": "2026-01-01T12:00:00.000Z",
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
            batchID: "batch-7",
            schema: .init(version: 7, hash: "hash7"),
            mutations: [
                Mutation(
                    mutationID: "m-1",
                    table: "orders",
                    op: .insert,
                    pk: ["id": AnyCodable("rec-1")],
                    baseVersion: nil,
                    clientVersion: "2026-01-01T12:00:00.000Z",
                    columns: ["ship_address": AnyCodable("123 Main St")]
                )
            ]
        )
        _ = try await httpClient.push(request: req)

        XCTAssertEqual(capturedBody?["client_id"] as? String, "dev-1")
        XCTAssertEqual(capturedBody?["batch_id"] as? String, "batch-7")
        let mutations = capturedBody?["mutations"] as? [[String: Any]]
        XCTAssertEqual(mutations?.count, 1)
        XCTAssertEqual(mutations?[0]["mutation_id"] as? String, "m-1")
        XCTAssertEqual(mutations?[0]["table"] as? String, "orders")
        XCTAssertEqual(mutations?[0]["op"] as? String, "insert")
        XCTAssertEqual(mutations?[0]["client_version"] as? String, "2026-01-01T12:00:00.000Z")
    }
}
