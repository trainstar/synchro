import Foundation
import XCTest
@testable @_spi(Inspection) import Synchro

final class TransportObservationTests: XCTestCase {
    private var session: URLSession!

    override func setUp() {
        super.setUp()
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [MockURLProtocol.self]
        session = URLSession(configuration: configuration)
    }

    override func tearDown() {
        MockURLProtocol.requestHandler = nil
        session.invalidateAndCancel()
        session = nil
        super.tearDown()
    }

    func testOperationClassificationIsClosed() {
        let cases: [(String, TransportOperationClass)] = [
            ("/sync/connect", .connect),
            ("/sync/pull", .pull),
            ("/sync/push", .push),
            ("/sync/checkpoint", .checkpoint),
            ("/sync/schema", .schemas),
            ("/sync/rebuild", .rebuild),
            ("/sync/unknown", .other),
            ("/health", .other),
        ]

        for (path, expected) in cases {
            XCTAssertEqual(TransportOperationClass.classify(path: path), expected)
        }
        XCTAssertEqual(Set(TransportOperationClass.allCases).count, 7)
    }

    func testRetryAttemptsProduceIndividualPullObservationsWithoutCursorLeakage() async throws {
        let collector = TransportObservationCollector(capacity: 8)
        let client = makeClient(collector: collector)
        var attempt = 0
        MockURLProtocol.requestHandler = { request in
            attempt += 1
            if attempt == 1 {
                let body = try JSONSerialization.data(withJSONObject: [
                    "error": [
                        "code": "temporary_unavailable",
                        "message": "retry",
                        "retryable": true,
                    ],
                ])
                return (
                    HTTPURLResponse(
                        url: request.url!,
                        statusCode: 503,
                        httpVersion: nil,
                        headerFields: ["Retry-After": "0"]
                    )!,
                    body
                )
            }
            let body = try JSONSerialization.data(withJSONObject: [
                "changes": [],
                "scope_set_version": 1,
                "scope_cursors": ["scope-sensitive": "next"],
                "scope_updates": ["add": [], "remove": []],
                "rebuild": [],
                "has_more": false,
                "checksums": [:],
            ])
            return (
                HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!,
                body
            )
        }
        let request = PullRequest(
            clientID: "client",
            clientGeneration: 1,
            schema: SchemaRef(version: 1, hash: String(repeating: "a", count: 64)),
            scopeSetVersion: 1,
            scopes: ["scope-sensitive": ScopeCursorRef(cursor: "abc")],
            limit: 100
        )

        do {
            _ = try await client.pull(request: request)
            XCTFail("Expected retryable response")
        } catch is RetryableError {
            // The caller performs the next durable retry attempt.
        }
        _ = try await client.pull(request: request)

        let snapshot = collector.snapshot()
        XCTAssertFalse(snapshot.overflowed)
        XCTAssertEqual(snapshot.sequenceCheckpoint, 2)
        XCTAssertEqual(snapshot.observations.map(\.sequence), [1, 2])
        XCTAssertEqual(snapshot.observations.map(\.operationClass), [.pull, .pull])
        XCTAssertEqual(snapshot.observations.map(\.statusCode), [503, 200])
        XCTAssertEqual(snapshot.observations.map(\.errorCode), ["temporary_unavailable", nil])
        XCTAssertEqual(snapshot.observations.map(\.retryable), [true, nil])
        XCTAssertTrue(snapshot.observations.allSatisfy { $0.durationNanoseconds > 0 })
        XCTAssertEqual(
            snapshot.observations.map(\.cursorFingerprints),
            [
                ["ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"],
                ["ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"],
            ]
        )
        XCTAssertTrue(snapshot.observations.allSatisfy { $0.cursorFingerprintsComplete == true })
        let pullFacts = try XCTUnwrap(snapshot.observations[1].requestFacts)
        XCTAssertEqual(pullFacts, TransportRequestFacts(
            clientGeneration: 1,
            schemaVersion: 1,
            schemaHash: String(repeating: "a", count: 64),
            scopeSetVersion: 1,
            scopeCount: 1,
            limit: 100
        ))
        XCTAssertNil(pullFacts.protocolVersion)
        XCTAssertNil(pullFacts.rebuildIDFingerprint)
        XCTAssertNil(pullFacts.cursorFingerprint)
        XCTAssertNil(pullFacts.cursorPresent)
        XCTAssertNil(pullFacts.mutationCount)
        XCTAssertEqual(snapshot.observations[1].pullResponseFacts, TransportPullResponseFacts(
            changeCount: 0,
            hasMore: false,
            rebuildScopeCount: 0,
            checksumCount: 0,
            scopeCursorFingerprints: [
                TransportObservationCollector.cursorFingerprint("next"),
            ],
            scopeCursorFingerprintsComplete: true
        ))
        let encoded = try XCTUnwrap(String(data: JSONEncoder().encode(snapshot), encoding: .utf8))
        XCTAssertFalse(encoded.contains("abc"))
        XCTAssertFalse(encoded.contains("scope-sensitive"))
        XCTAssertFalse(encoded.contains("\"client\""))
        XCTAssertFalse(encoded.contains("\"rebuild\""))
        XCTAssertFalse(encoded.contains("\"abc\""))
        XCTAssertFalse(encoded.contains("\"mutation_count\""))
        for key in [
            "sequence_checkpoint",
            "overflowed",
            "operation_class",
            "status_code",
            "duration_nanoseconds",
            "cursor_fingerprints",
            "cursor_fingerprints_complete",
        ] {
            XCTAssertTrue(encoded.contains("\"\(key)\""))
        }
    }

    func testFailedRebuildRecordsRequestFactsWithoutResponseFacts() async throws {
        let collector = TransportObservationCollector(capacity: 4)
        let client = makeClient(collector: collector)
        MockURLProtocol.requestHandler = { request in
            (
                HTTPURLResponse(url: request.url!, statusCode: 503, httpVersion: nil, headerFields: nil)!,
                Data("{}".utf8)
            )
        }
        let request = RebuildRequest(
            clientID: "client-sensitive",
            clientGeneration: 7,
            schema: SchemaRef(version: 3, hash: String(repeating: "c", count: 64)),
            scope: "scope-sensitive",
            rebuildID: "00000000-0000-4000-8000-000000000002",
            cursor: nil,
            limit: 25
        )

        do {
            _ = try await client.rebuild(request: request)
            XCTFail("Expected failed rebuild")
        } catch {
            // The observation proves the failed transport attempt.
        }

        let observation = try XCTUnwrap(collector.snapshot().observations.first)
        XCTAssertEqual(observation.statusCode, 503)
        XCTAssertNotNil(observation.requestFacts)
        XCTAssertNil(observation.rebuildResponseFacts)
        XCTAssertNil(observation.pullResponseFacts)
    }

    func testSuccessfulRebuildRecordsExactResponseBodyDigest() async throws {
        let collector = TransportObservationCollector(capacity: 4)
        let client = makeClient(collector: collector)
        let body = Data(#"{"cursor":"opaque-cursor","has_more":true,"records":[],"scope":"scope-a"}"#.utf8)
        MockURLProtocol.requestHandler = { request in
            (
                HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!,
                body
            )
        }
        let request = RebuildRequest(
            clientID: "client",
            clientGeneration: 1,
            schema: SchemaRef(version: 1, hash: String(repeating: "a", count: 64)),
            scope: "scope-a",
            rebuildID: "00000000-0000-4000-8000-000000000002",
            cursor: nil,
            limit: 1
        )

        _ = try await client.rebuild(request: request)

        let observation = try XCTUnwrap(collector.snapshot().observations.first)
        let facts = try XCTUnwrap(observation.rebuildResponseFacts)
        XCTAssertEqual(
            facts.responseBodySHA256,
            "22cf3cb46da3cd46c4daf9999268bdcce423f922baa88eedd41acb6c99ad076a"
        )
    }

    func testConnectInspectionBoundsParsedCollectionsAndPreservesNullUpdates() async throws {
        let collector = TransportObservationCollector()
        let client = makeClient(collector: collector)
        let scopes = (0..<17).map { String(format: "scope-%02d", $0) }
        let updates: [String: Any] = Dictionary(uniqueKeysWithValues: scopes.map {
            ($0, $0 == scopes[0] ? NSNull() : "token-\($0)" as Any)
        })
        let body = try JSONSerialization.data(withJSONObject: [
            "server_time": "2026-10-09T00:00:00Z", "protocol_version": 3,
            "client_generation": 1, "scope_set_version": 1,
            "schema": ["version": 2, "hash": String(repeating: "a", count: 64), "action": "replace"],
            "scopes": ["add": [], "remove": []],
            "affected_scopes": Array(scopes.reversed()), "scope_cursor_updates": updates,
        ])
        MockURLProtocol.requestHandler = { request in
            (HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!, body)
        }
        var knownScopes = Dictionary(uniqueKeysWithValues: scopes.map { ($0, ScopeCursorRef(cursor: "token-\($0)")) })
        knownScopes["without-cursor"] = ScopeCursorRef(cursor: nil)
        let request = ConnectRequest(
            clientID: "client", clientGeneration: 1, platform: "ios", appVersion: "1.0.0",
            protocolVersion: 3, schema: SchemaRef(version: 1, hash: String(repeating: "b", count: 64)),
            scopeSetVersion: 1, knownScopes: knownScopes
        )
        // The missing schema definition prevents this inspection test from proving a sync flow.
        do {
            _ = try await client.connect(request: request)
            XCTFail("Expected an invalid connect response")
        } catch {}

        let observation = try XCTUnwrap(collector.snapshot().observations.first)
        let facts = try XCTUnwrap(observation.connectResponseFacts)
        XCTAssertEqual(facts.action, "replace")
        XCTAssertEqual(facts.schemaVersion, 2)
        XCTAssertEqual(facts.schemaHash, String(repeating: "a", count: 64))
        XCTAssertEqual(facts.affectedScopeFingerprints, scopes.prefix(16).map(TransportObservationCollector.cursorFingerprint).sorted())
        XCTAssertFalse(facts.affectedScopesComplete)
        XCTAssertFalse(facts.scopeCursorUpdatesComplete)
        XCTAssertEqual(facts.scopeCursorUpdates.count, 16)
        let nullScope = TransportObservationCollector.cursorFingerprint(scopes[0])
        XCTAssertTrue(facts.scopeCursorUpdates[nullScope] == .some(nil))
        XCTAssertNil(facts.scopeCursorUpdates[TransportObservationCollector.cursorFingerprint(scopes[16])])
        XCTAssertEqual(observation.cursorFingerprints, Array(scopes.map { TransportObservationCollector.cursorFingerprint("token-\($0)") }.sorted().prefix(16)))
        XCTAssertEqual(observation.cursorFingerprintsComplete, false)
        let encoded = try JSONEncoder().encode(observation)
        let object = try XCTUnwrap(JSONSerialization.jsonObject(with: encoded) as? [String: Any])
        let encodedFacts = try XCTUnwrap(object["connect_response_facts"] as? [String: Any])
        let encodedUpdates = try XCTUnwrap(encodedFacts["scope_cursor_updates"] as? [String: Any])
        XCTAssertTrue(encodedUpdates[nullScope] is NSNull)
        XCTAssertEqual(encodedUpdates[TransportObservationCollector.cursorFingerprint(scopes[1])] as? String,
                       TransportObservationCollector.cursorFingerprint("token-\(scopes[1])"))
        XCTAssertEqual(try JSONDecoder().decode(TransportObservation.self, from: encoded), observation)
        XCTAssertFalse(String(decoding: encoded, as: UTF8.self).contains("scope-"))
        XCTAssertFalse(String(decoding: encoded, as: UTF8.self).contains("token-"))

        var omittedResponse = try XCTUnwrap(JSONSerialization.jsonObject(with: body) as? [String: Any])
        omittedResponse.removeValue(forKey: "affected_scopes")
        let omittedBody = try JSONSerialization.data(withJSONObject: omittedResponse)
        MockURLProtocol.requestHandler = { request in
            (HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!, omittedBody)
        }
        let omittedResult = try? await client.connect(request: request)
        XCTAssertNil(omittedResult)
        let omittedObservation = try XCTUnwrap(collector.snapshot().observations.last)
        let omittedEncoded = try JSONEncoder().encode(omittedObservation)
        let omittedObject = try XCTUnwrap(JSONSerialization.jsonObject(with: omittedEncoded) as? [String: Any])
        let omittedFacts = try XCTUnwrap(omittedObject["connect_response_facts"] as? [String: Any])
        XCTAssertEqual(omittedFacts["affected_scope_fingerprints"] as? [String], [])
        XCTAssertEqual(omittedFacts["affected_scopes_complete"] as? Bool, true)
    }

    func testNetworkFailureUsesStatusZero() async throws {
        let collector = TransportObservationCollector(capacity: 4)
        let client = makeClient(collector: collector)
        MockURLProtocol.requestHandler = { _ in
            throw URLError(.notConnectedToInternet)
        }

        do {
            _ = try await client.fetchSchema()
            XCTFail("Expected network error")
        } catch is SynchroError {
            // The observation is the asserted result.
        }

        let observation = try XCTUnwrap(collector.snapshot().observations.first)
        XCTAssertEqual(observation.operationClass, .schemas)
        XCTAssertEqual(observation.statusCode, 0)
        XCTAssertNil(observation.cursorFingerprints)
    }

    func testBoundedSnapshotReportsRequestedRangeOverflow() {
        let collector = TransportObservationCollector(capacity: 2)
        for status in [200, 201, 202] {
            collector.record(
                operationClass: .other,
                statusCode: status,
                durationNanoseconds: 1,
                cursorFingerprints: nil,
                cursorFingerprintsComplete: nil
            )
        }

        let missingRange = collector.snapshot(after: 0)
        XCTAssertTrue(missingRange.overflowed)
        XCTAssertEqual(missingRange.observations.map(\.sequence), [2, 3])
        XCTAssertEqual(missingRange.sequenceCheckpoint, 3)

        let retainedRange = collector.snapshot(after: 1)
        XCTAssertFalse(retainedRange.overflowed)
        XCTAssertEqual(retainedRange.observations.map(\.sequence), [2, 3])
    }

    func testTelemetryIsDisabledByDefault() {
        let config = SynchroConfig(
            dbPath: "",
            serverURL: URL(string: "https://example.test")!,
            authProvider: { "token" },
            clientID: "client",
            appVersion: "1.0.0"
        )

        XCTAssertNil(config.transportObservationCollector)
    }

    func testPausedBarrierCanQueueTheNextOperation() async throws {
        let collector = TransportObservationCollector(capacity: 4)
        try collector.armPause(for: .connect)

        let firstPause = Task { try await collector.pauseIfArmed(for: .connect) }
        try await collector.awaitPause(for: .connect, timeout: 1)
        try collector.armPause(for: .pull)
        try collector.resumePause()
        try await firstPause.value

        let secondPause = Task { try await collector.pauseIfArmed(for: .pull) }
        try await collector.awaitPause(for: .pull, timeout: 1)
        try collector.resumePause()
        try await secondPause.value
    }

    func testWrongAwaitOperationFailsClosed() async throws {
        let collector = TransportObservationCollector()
        try collector.armPause(for: .pull)

        do {
            try await collector.awaitPause(for: .push, timeout: 1)
            XCTFail("Expected wrong operation failure")
        } catch let error as TransportPauseBarrierError {
            XCTAssertEqual(error, .wrongOperation)
        }
        do {
            try collector.armPause(for: .pull)
            XCTFail("Expected closed barrier failure")
        } catch let error as TransportPauseBarrierError {
            XCTAssertEqual(error, .wrongOperation)
        }
    }

    func testDoubleArmAndResumeWithoutPauseFailClosed() throws {
        let doubleArmCollector = TransportObservationCollector()
        try doubleArmCollector.armPause(for: .pull)
        do {
            try doubleArmCollector.armPause(for: .pull)
            XCTFail("Expected double-arm failure")
        } catch let error as TransportPauseBarrierError {
            XCTAssertEqual(error, .alreadyArmed)
        }

        let resumeCollector = TransportObservationCollector()
        do {
            try resumeCollector.resumePause()
            XCTFail("Expected resume failure")
        } catch let error as TransportPauseBarrierError {
            XCTAssertEqual(error, .notPaused)
        }
        do {
            try resumeCollector.armPause(for: .pull)
            XCTFail("Expected closed barrier failure")
        } catch let error as TransportPauseBarrierError {
            XCTAssertEqual(error, .notPaused)
        }
    }

    func testPauseWaitTimeoutFailsClosed() async throws {
        let collector = TransportObservationCollector()
        try collector.armPause(for: .pull)

        do {
            try await collector.awaitPause(for: .pull, timeout: 0.01)
            XCTFail("Expected timeout")
        } catch let error as TransportPauseBarrierError {
            XCTAssertEqual(error, .timedOut)
        }
        do {
            try collector.resumePause()
            XCTFail("Expected closed barrier failure")
        } catch let error as TransportPauseBarrierError {
            XCTAssertEqual(error, .timedOut)
        }
    }

    func testCancelledPauseWaitFailsClosed() async throws {
        let collector = TransportObservationCollector()
        try collector.armPause(for: .pull)
        let waitTask = Task {
            try await collector.awaitPause(for: .pull, timeout: 10)
        }
        waitTask.cancel()

        do {
            try await waitTask.value
            XCTFail("Expected cancellation")
        } catch let error as TransportPauseBarrierError {
            XCTAssertEqual(error, .cancelled)
        }
        do {
            try collector.resumePause()
            XCTFail("Expected closed barrier failure")
        } catch let error as TransportPauseBarrierError {
            XCTAssertEqual(error, .cancelled)
        }
    }

    private func makeClient(collector: TransportObservationCollector) -> HttpClient {
        let config = SynchroConfig(
            dbPath: "",
            serverURL: URL(string: "http://test.local")!,
            authProvider: { "token" },
            clientID: "client",
            appVersion: "1.0.0",
            transportObservationCollector: collector
        )
        return HttpClient(config: config, session: session)
    }
}
