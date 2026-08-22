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
            "schema_version": 1,
            "schema_hash": protocolTestSchemaHash,
            "server_time": "2026-01-01T12:00:00.000Z",
            "manifest": [
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
                            ["field_id": "field-id", "name": "id", "type": "string", "nullable": false, "writable": false],
                            ["field_id": "field-ship-address", "name": "ship_address", "type": "string", "nullable": true, "writable": true],
                            ["field_id": "field-user-id", "name": "user_id", "type": "string", "nullable": false, "writable": true],
                            ["field_id": "field-updated-at", "name": "updated_at", "type": "datetime", "nullable": false, "writable": false],
                            ["field_id": "field-deleted-at", "name": "deleted_at", "type": "datetime", "nullable": true, "writable": false]
                        ],
                        "indexes": []
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
        XCTAssertEqual(resp.schemaVersion, 1)
        XCTAssertEqual(resp.tables.count, 1)
        XCTAssertEqual(resp.tables[0].tableName, "orders")
    }

    func testConnectSuccess() async throws {
        let responseBody: [String: Any] = [
            "server_time": "2026-03-20T18:22:11Z",
            "protocol_version": 3,
            "client_generation": 4,
            "scope_set_version": 13,
            "schema": [
                "version": 8,
                "hash": String(repeating: "8", count: 64),
                "action": "none",
            ],
            "scopes": [
                "add": [],
                "remove": [],
            ],
            "scope_cursor_updates": [:] as [String: String],
        ]

        MockURLProtocol.requestHandler = { request in
            XCTAssertEqual(request.httpMethod, "POST")
            XCTAssertTrue(request.url!.path.hasSuffix("/sync/connect"))
            let body = try JSONSerialization.jsonObject(with: request.bodyData()!) as! [String: Any]
            XCTAssertEqual(body["client_id"] as? String, "test-device")
            XCTAssertEqual(body["protocol_version"] as? Int, 3)

            let data = try JSONSerialization.data(withJSONObject: responseBody)
            let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!
            return (response, data)
        }

        let req = ConnectRequest(
            clientID: "test-device",
            clientGeneration: 4,
            platform: "ios",
            appVersion: "1.0.0",
            protocolVersion: 3,
            schema: .init(version: 8, hash: "8b21d2a1"),
            scopeSetVersion: 13,
            knownScopes: [:]
        )
        let resp = try await httpClient.connect(request: req)
        XCTAssertEqual(resp.schema.action, .none)
        try resp.validate()
    }

    func testConnectRejectsNoncanonicalSuccessJSON() async throws {
        let responseBody = Data("""
        {"server_time":"2026-03-20T18:22:11Z","protocol_version":3,"client_generation":4.0,"scope_set_version":13,"schema":{"version":8,"hash":"\(String(repeating: "8", count: 64))","action":"none"},"scopes":{"add":[],"remove":[]},"scope_cursor_updates":{}}
        """.utf8)
        MockURLProtocol.requestHandler = { request in
            let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
            return (response, responseBody)
        }

        let request = ConnectRequest(
            clientID: "test-device",
            platform: "ios",
            appVersion: "1.0.0",
            protocolVersion: 3,
            schema: .init(version: 8, hash: String(repeating: "8", count: 64)),
            scopeSetVersion: 13,
            knownScopes: [:]
        )
        do {
            _ = try await httpClient.connect(request: request)
            XCTFail("Expected invalid response")
        } catch let error as SynchroError {
            guard case .invalidResponse = error else {
                return XCTFail("Expected invalid response, got \(error)")
            }
        }
    }

    func testPullEncoding() async throws {
        let responseBody: [String: Any] = [
                "changes": [],
                "scope_set_version": 13,
                "scope_cursors": [
                "workouts_user:u_123": "workouts_user_u_123_890.sig",
            ],
            "scope_updates": [
                "add": [],
                "remove": [],
            ],
            "rebuild": [],
            "has_more": false,
            "checksums": [
                "workouts_user:u_123": [
                    "algorithm": "sha256",
                    "version": 1,
                    "encoding": "hex",
                    "digest": String(repeating: "1", count: 64),
                ],
            ],
        ]

        MockURLProtocol.requestHandler = { request in
            XCTAssertEqual(request.httpMethod, "POST")
            XCTAssertTrue(request.url!.path.hasSuffix("/sync/pull"))
            let body = try JSONSerialization.jsonObject(with: request.bodyData()!) as! [String: Any]
            XCTAssertEqual(body["client_id"] as? String, "test-device")
            XCTAssertEqual(body["scope_set_version"] as? Int64, 13)
            XCTAssertNil(body["checksum_mode"])

            let data = try JSONSerialization.data(withJSONObject: responseBody)
            let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: ["Content-Type": "application/json"])!
            return (response, data)
        }

        let req = PullRequest(
            clientID: "test-device",
            clientGeneration: 4,
            schema: .init(version: 8, hash: "8b21d2a1"),
            scopeSetVersion: 13,
            scopes: ["workouts_user:u_123": .init(cursor: "workouts_user_u_123_890.sig")],
            limit: 100
        )
        let resp = try await httpClient.pull(request: req)
        try resp.validate()
        XCTAssertEqual(resp.scopeSetVersion, 13)
        XCTAssertEqual(resp.scopeCursors["workouts_user:u_123"], "workouts_user_u_123_890.sig")
    }

    func testSchemaMismatch422() async throws {
        let currentSchema = SchemaRef(version: 2, hash: String(repeating: "b", count: 64))
        let receivedSchema = SchemaRef(version: 1, hash: String(repeating: "a", count: 64))
        let responseBody: [String: Any] = [
            "error": [
                "code": "schema_mismatch",
                "message": "client schema does not match server schema",
                "retryable": false,
                "current_schema": ["version": currentSchema.version, "hash": currentSchema.hash],
                "received_schema": ["version": receivedSchema.version, "hash": receivedSchema.hash],
            ],
        ]

        MockURLProtocol.requestHandler = { request in
            let data = try JSONSerialization.data(withJSONObject: responseBody)
            let response = HTTPURLResponse(url: request.url!, statusCode: 422, httpVersion: nil, headerFields: nil)!
            return (response, data)
        }

        let req = PullRequest(
            clientID: "test",
            clientGeneration: 1,
            schema: .init(version: 1, hash: "old"),
            scopeSetVersion: 0,
            scopes: [:],
            limit: 100
        )
        do {
            _ = try await httpClient.pull(request: req)
            XCTFail("Expected schemaMismatch error")
        } catch let error as BindingRenewalError {
            XCTAssertEqual(
                error,
                .schemaMismatch(currentSchema: currentSchema, receivedSchema: receivedSchema)
            )
        }
    }

    func testRebuildRestartRequired409IsTyped() async throws {
        MockURLProtocol.requestHandler = { request in
            let data = try JSONSerialization.data(withJSONObject: [
                "error": [
                    "code": "rebuild_restart_required",
                    "message": "rebuild continuation expired",
                    "retryable": false,
                    "scope_id": "orders:user1",
                ],
            ])
            let response = HTTPURLResponse(url: request.url!, statusCode: 409, httpVersion: nil, headerFields: nil)!
            return (response, data)
        }
        let request = RebuildRequest(
            clientID: "test-device",
            clientGeneration: 1,
            schema: SchemaRef(version: 1, hash: String(repeating: "a", count: 64)),
            scope: "orders:user1",
            rebuildID: "00000000-0000-4000-8000-000000000001",
            cursor: "opaque-token",
            limit: 100
        )

        do {
            _ = try await httpClient.rebuild(request: request)
            XCTFail("Expected rebuild restart requirement")
        } catch let error as RebuildRestartRequiredError {
            XCTAssertEqual(error.scopeID, "orders:user1")
        }
    }

    func testRebuildRestartRequired409RejectsInvalidEnvelope() async throws {
        let invalidErrors: [[String: Any]] = [
            [
                "code": "rebuild_restart_required",
                "message": "incorrectly retryable",
                "retryable": true,
                "scope_id": "orders:user1",
            ],
            [
                "code": "rebuild_restart_required",
                "message": "scope absent",
                "retryable": false,
                "scope_id": "",
            ],
            [
                "code": "rebuild_restart_required",
                "message": "retryability absent",
                "scope_id": "orders:user1",
            ],
        ]
        let request = RebuildRequest(
            clientID: "test-device",
            clientGeneration: 1,
            schema: SchemaRef(version: 1, hash: String(repeating: "a", count: 64)),
            scope: "orders:user1",
            rebuildID: "00000000-0000-4000-8000-000000000001",
            cursor: "opaque-token",
            limit: 100
        )

        for errorBody in invalidErrors {
            MockURLProtocol.requestHandler = { request in
                let data = try JSONSerialization.data(withJSONObject: ["error": errorBody])
                let response = HTTPURLResponse(url: request.url!, statusCode: 409, httpVersion: nil, headerFields: nil)!
                return (response, data)
            }
            do {
                _ = try await httpClient.rebuild(request: request)
                XCTFail("Expected invalid response")
            } catch let error as SynchroError {
                guard case .invalidResponse = error else {
                    return XCTFail("Expected invalid response, got \(error)")
                }
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
            protocolVersion: 3,
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
        let responseBody: [String: Any] = [
            "error": [
                "code": "retry_later",
                "message": "rate limited",
                "retryable": true,
            ] as [String: Any],
        ]

        MockURLProtocol.requestHandler = { request in
            let data = try JSONSerialization.data(withJSONObject: responseBody)
            let response = HTTPURLResponse(url: request.url!, statusCode: 429, httpVersion: nil, headerFields: ["Retry-After": "10"])!
            return (response, data)
        }

        let req = PushRequest(
            clientID: "test",
            clientGeneration: 1,
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
        let responseBody: [String: Any] = [
            "error": [
                "code": "temporary_unavailable",
                "message": "service temporarily unavailable",
                "retryable": true,
            ] as [String: Any],
        ]

        MockURLProtocol.requestHandler = { request in
            let data = try JSONSerialization.data(withJSONObject: responseBody)
            let response = HTTPURLResponse(url: request.url!, statusCode: 503, httpVersion: nil, headerFields: ["Retry-After": "5"])!
            return (response, data)
        }

        let req = PullRequest(
            clientID: "test",
            clientGeneration: 1,
            schema: .init(version: 1, hash: "abc"),
            scopeSetVersion: 0,
            scopes: [:],
            limit: 100
        )
        do {
            _ = try await httpClient.pull(request: req)
            XCTFail("Expected retryable error")
        } catch let error as RetryableError {
            XCTAssertEqual(error.retryAfter, 5)
        }
    }

    func testHugeFiniteRetryAfterIsPreservedWithoutOverflow() async throws {
        MockURLProtocol.requestHandler = { request in
            let data = try JSONSerialization.data(withJSONObject: [
                "error": [
                    "code": "temporary_unavailable",
                    "message": "service temporarily unavailable",
                    "retryable": true,
                ] as [String: Any],
            ])
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 503,
                httpVersion: nil,
                headerFields: ["Retry-After": "100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"]
            )!
            return (response, data)
        }
        let request = PullRequest(
            clientID: "test",
            clientGeneration: 1,
            schema: .init(version: 1, hash: "abc"),
            scopeSetVersion: 0,
            scopes: [:],
            limit: 100
        )

        do {
            _ = try await httpClient.pull(request: request)
            XCTFail("Expected retryable error")
        } catch let error as RetryableError {
            XCTAssertEqual(error.retryAfter, 1e308)
            XCTAssertEqual(
                RetryTiming.deadline(nowMS: 1_000, delaySeconds: try XCTUnwrap(error.retryAfter)),
                Int64.max
            )
        }
    }

    func testCanonicalProtocolErrorsPreserveStatusAndCode() async throws {
        let cases: [(status: Int, code: ProtocolErrorCode)] = [
            (400, .invalidRequest),
            (400, .invalidSchemaReference),
            (401, .authRequired),
            (409, .idempotencyConflict),
            (409, .clientRetired),
            (500, .syncIntegrityFailure),
        ]
        let request = PullRequest(
            clientID: "test",
            clientGeneration: 1,
            schema: .init(version: 1, hash: "abc"),
            scopeSetVersion: 0,
            scopes: [:],
            limit: 100
        )

        for testCase in cases {
            MockURLProtocol.requestHandler = { request in
                let data = try JSONSerialization.data(withJSONObject: [
                    "error": [
                        "code": testCase.code.rawValue,
                        "message": "canonical protocol rejection",
                        "retryable": false,
                    ] as [String: Any],
                ])
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: testCase.status,
                    httpVersion: nil,
                    headerFields: nil
                )!
                return (response, data)
            }
            do {
                _ = try await httpClient.pull(request: request)
                XCTFail("Expected protocol error")
            } catch let error as SynchroError {
                guard case let .protocolError(status, code, message) = error else {
                    return XCTFail("Expected protocolError, got \(error)")
                }
                XCTAssertEqual(status, testCase.status)
                XCTAssertEqual(code, testCase.code)
                XCTAssertEqual(message, "canonical protocol rejection")
            }
        }
    }

    func testRetryableServiceResponsesRejectMalformedEnvelopesWithoutRetry() async throws {
        let request = PullRequest(
            clientID: "test",
            clientGeneration: 1,
            schema: .init(version: 1, hash: "abc"),
            scopeSetVersion: 0,
            scopes: [:],
            limit: 100
        )
        let cases: [(status: Int, header: String?, body: [String: Any])] = [
            (
                429,
                "1",
                ["error": ["code": "temporary_unavailable", "message": "wrong code", "retryable": true]]
            ),
            (
                429,
                "1",
                ["error": ["code": "retry_later", "message": "wrong retryability", "retryable": false]]
            ),
            (
                429,
                nil,
                ["error": ["code": "retry_later", "message": "missing retry header", "retryable": true]]
            ),
            (
                503,
                "not-a-delay",
                ["error": ["code": "capture_pending", "message": "invalid retry header", "retryable": true]]
            ),
            (
                503,
                "1",
                ["error": ["code": "retry_later", "message": "wrong code", "retryable": true]]
            ),
            (
                503,
                "1",
                ["error": ["code": "temporary_unavailable", "message": "wrong retryability", "retryable": false]]
            ),
            (
                503,
                "1",
                ["error": "malformed envelope"]
            ),
        ]

        for malformed in cases {
            MockURLProtocol.requestHandler = { request in
                let data = try JSONSerialization.data(withJSONObject: malformed.body)
                let headers = malformed.header.map { ["Retry-After": $0] }
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: malformed.status,
                    httpVersion: nil,
                    headerFields: headers
                )!
                return (response, data)
            }

            do {
                _ = try await httpClient.pull(request: request)
                XCTFail("Expected invalid response")
            } catch is RetryableError {
                XCTFail("Malformed retry response must not enter backoff")
            } catch let error as SynchroError {
                guard case .invalidResponse = error else {
                    XCTFail("Expected invalid response, got \(error)")
                    continue
                }
            }
        }
    }

    func testServerError500() async throws {
        let responseBody: [String: Any] = [
            "error": [
                "code": "sync_integrity_failure",
                "message": "internal server integrity error",
                "retryable": false,
            ] as [String: Any],
        ]

        MockURLProtocol.requestHandler = { request in
            let data = try JSONSerialization.data(withJSONObject: responseBody)
            let response = HTTPURLResponse(url: request.url!, statusCode: 500, httpVersion: nil, headerFields: nil)!
            return (response, data)
        }

        let req = PullRequest(
            clientID: "test",
            clientGeneration: 1,
            schema: .init(version: 1, hash: "abc"),
            scopeSetVersion: 0,
            scopes: [:],
            limit: 100
        )
        do {
            _ = try await httpClient.pull(request: req)
            XCTFail("Expected protocolError")
        } catch let error as SynchroError {
            switch error {
            case .protocolError(let status, let code, let message):
                XCTAssertEqual(status, 500)
                XCTAssertEqual(code, .syncIntegrityFailure)
                XCTAssertEqual(message, "internal server integrity error")
            default:
                XCTFail("Expected protocolError, got \(error)")
            }
        }
    }

    func testPushRequestEncoding() async throws {
        let pushResponseBody: [String: Any] = [
            "batch_id": "00000000-0000-4000-8000-000000000007",
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
            clientGeneration: 4,
            batchID: "00000000-0000-4000-8000-000000000007",
            schema: .init(version: 7, hash: "hash7"),
            mutations: [
                Mutation(
                    mutationID: "m-1",
                    table: "orders",
                    op: .insert,
                    pk: ["id": AnyCodable("rec-1")],
                    authoredSchema: .init(version: 7, hash: "hash7"),
                    baseVersion: nil,
                    clientVersion: "2026-01-01T12:00:00.000000Z",
                    columns: ["ship_address": AnyCodable("123 Main St")]
                )
            ]
        )
        _ = try await httpClient.push(request: req)

        XCTAssertEqual(capturedBody?["client_id"] as? String, "dev-1")
        XCTAssertEqual(capturedBody?["batch_id"] as? String, "00000000-0000-4000-8000-000000000007")
        let mutations = capturedBody?["mutations"] as? [[String: Any]]
        XCTAssertEqual(mutations?.count, 1)
        XCTAssertEqual(mutations?[0]["mutation_id"] as? String, "m-1")
        XCTAssertEqual(mutations?[0]["table"] as? String, "orders")
        XCTAssertEqual(mutations?[0]["op"] as? String, "insert")
        XCTAssertEqual(mutations?[0]["client_version"] as? String, "2026-01-01T12:00:00.000000Z")
    }
}
