import XCTest
import Foundation
@testable import Synchro

final class ContractTests: XCTestCase {
    private let decoder = JSONDecoder.synchroDecoder()

    func testConnectNoneFixtureDecodesAndValidates() throws {
        let response: ConnectResponse = try decodeFixtureValue(
            path: "conformance/protocol/connect-none.json",
            jsonPath: ["expected", "response"]
        )

        XCTAssertEqual(response.schema.action, .none)
        XCTAssertNil(response.schemaDefinition)
        XCTAssertEqual(response.scopeSetVersion, 13)
        try response.validate()
    }

    func testConnectRebuildLocalFixtureDecodesAndValidates() throws {
        let response: ConnectResponse = try decodeFixtureValue(
            path: "conformance/protocol/connect-rebuild-local.json",
            jsonPath: ["expected", "response"]
        )

        XCTAssertEqual(response.schema.action, .rebuildLocal)
        XCTAssertNotNil(response.schemaDefinition)
        XCTAssertEqual(response.scopes.add.count, 1)
        XCTAssertEqual(response.scopeCursorUpdates["exercises_public"]!, nil)
        try response.validate(
            existingScopes: ["exercises_public": ScopeCursorRef(cursor: "historical-cursor")],
            requestScopeSetVersion: 12
        )

        var missingCursorUpdate = response
        missingCursorUpdate.scopeCursorUpdates = [:]
        XCTAssertThrowsError(try missingCursorUpdate.validate(
            existingScopes: ["exercises_public": ScopeCursorRef(cursor: "historical-cursor")],
            requestScopeSetVersion: 12
        ))
    }

    func testConnectRejectsStaleAndRehashedInvalidManifest() throws {
        var stale: ConnectResponse = try decodeFixtureValue(
            path: "conformance/protocol/connect-rebuild-local.json",
            jsonPath: ["expected", "response"]
        )
        var staleManifest = try XCTUnwrap(stale.schemaDefinition)
        staleManifest.tables[0].name = "changed"
        stale.schemaDefinition = staleManifest
        XCTAssertThrowsError(try stale.validate())

        var invalid: ConnectResponse = try decodeFixtureValue(
            path: "conformance/protocol/connect-rebuild-local.json",
            jsonPath: ["expected", "response"]
        )
        var invalidManifest = try XCTUnwrap(invalid.schemaDefinition)
        invalidManifest.compatibilityFloor = 1
        let invalidHash = try Integrity.schemaManifestHash(invalidManifest)
        invalidManifest.schemaHash = invalidHash
        invalid.schema.hash = invalidHash
        invalid.schemaDefinition = invalidManifest
        XCTAssertThrowsError(try invalid.validate())
    }

    func testConnectUnsupportedFixtureDecodesAndValidates() throws {
        let response: ConnectResponse = try decodeFixtureValue(
            path: "conformance/protocol/connect-unsupported.json",
            jsonPath: ["expected", "response"]
        )

        XCTAssertEqual(response.schema.action, .unsupported)
        XCTAssertNil(response.schemaDefinition)
        XCTAssertTrue(response.scopes.add.isEmpty)
        try response.validate()
    }

    func testConnectRejectsProtocolMismatch() throws {
        var response: ConnectResponse = try decodeFixtureValue(
            path: "conformance/protocol/connect-none.json",
            jsonPath: ["expected", "response"]
        )
        response.protocolVersion = 2
        XCTAssertThrowsError(try response.validate()) { error in
            guard case ContractViolation.invalidProtocolVersion(2) = error else {
                return XCTFail("expected protocol mismatch, got \(error)")
            }
        }
    }

    func testConnectRejectsRegressingOrUnadvancedAssignmentVersion() throws {
        var response: ConnectResponse = try decodeFixtureValue(
            path: "conformance/protocol/connect-none.json",
            jsonPath: ["expected", "response"]
        )
        let existingScopes = [
            "workouts_user:u_123": ScopeCursorRef(cursor: "cursor-a"),
            "exercises_public": ScopeCursorRef(cursor: "cursor-b"),
        ]
        try response.validate(existingScopes: existingScopes, requestScopeSetVersion: 13)

        response.scopeSetVersion = 12
        XCTAssertThrowsError(try response.validate(
            existingScopes: existingScopes,
            requestScopeSetVersion: 13
        ))

        response.scopeSetVersion = 13
        response.scopes.add = [ScopeAssignment(id: "new-scope", cursor: nil)]
        XCTAssertThrowsError(try response.validate(
            existingScopes: existingScopes,
            requestScopeSetVersion: 13
        ))
    }

    func testPushRejectsBatchAndOutcomeIdentityMismatch() throws {
        let schema = SchemaRef(version: 1, hash: protocolTestSchemaHash)
        let requestMutation = Mutation(
            mutationID: "00000000-0000-5000-8000-000000000001",
            table: "table-orders",
            op: .insert,
            pk: ["field-id": AnyCodable("r1")],
            authoredSchema: schema,
            baseVersion: nil,
            clientVersion: "2026-01-01T00:00:00.000000Z",
            columns: ["field-title": AnyCodable("Title")]
        )
        let request = PushRequest(
            clientID: "client-1",
            clientGeneration: 1,
            batchID: "00000000-0000-5000-8000-000000000002",
            schema: schema,
            mutations: [requestMutation]
        )
        let outcome = AcceptedMutation(
            mutationID: requestMutation.mutationID,
            table: requestMutation.table,
            pk: requestMutation.pk,
            outcomeSchema: schema,
            status: .applied,
            serverRow: ["field-id": AnyCodable("r1"), "field-title": AnyCodable("Title")],
            rowChecksum: validChecksum,
            serverVersion: "opaque-version"
        )
        var response = PushResponse(batchID: "00000000-0000-5000-8000-000000000003", serverTime: "2026-01-01T00:00:00.000000Z", accepted: [outcome], rejected: [])
        XCTAssertThrowsError(try response.validate(for: request))

        response.batchID = request.batchID
        response.accepted[0].table = "table-other"
        XCTAssertThrowsError(try response.validate(for: request))
    }

    func testPushOutcomeShapeUsesRequestedOperation() throws {
        let checksum = validChecksum
        let row = ["field-id": AnyCodable("r1"), "field-title": AnyCodable("Title")]

        for operation in [Synchro.Operation.insert, .update] {
            let request = makePushRequest(operation: operation)
            let valid = AcceptedMutation(
                mutationID: request.mutations[0].mutationID,
                table: request.mutations[0].table,
                pk: request.mutations[0].pk,
                outcomeSchema: request.schema,
                status: .applied,
                serverRow: row,
                rowChecksum: checksum,
                serverVersion: "server-version"
            )
            try PushResponse(
                batchID: request.batchID,
                serverTime: "2026-01-01T00:00:00.000000Z",
                accepted: [valid],
                rejected: []
            ).validate(for: request)

            var missingRow = valid
            missingRow.serverRow = nil
            missingRow.rowChecksum = nil
            XCTAssertThrowsError(
                try PushResponse(
                    batchID: request.batchID,
                    serverTime: "2026-01-01T00:00:00.000000Z",
                    accepted: [missingRow],
                    rejected: []
                ).validate(for: request)
            )
        }

        let deleteRequest = makePushRequest(operation: .delete)
        var deleteOutcome = AcceptedMutation(
            mutationID: deleteRequest.mutations[0].mutationID,
            table: deleteRequest.mutations[0].table,
            pk: deleteRequest.mutations[0].pk,
            outcomeSchema: deleteRequest.schema,
            status: .applied,
            serverRow: nil,
            rowChecksum: nil,
            serverVersion: "delete-fence"
        )
        try PushResponse(
            batchID: deleteRequest.batchID,
            serverTime: "2026-01-01T00:00:00.000000Z",
            accepted: [deleteOutcome],
            rejected: []
        ).validate(for: deleteRequest)
        deleteOutcome.serverRow = row
        XCTAssertThrowsError(
            try PushResponse(
                batchID: deleteRequest.batchID,
                serverTime: "2026-01-01T00:00:00.000000Z",
                accepted: [deleteOutcome],
                rejected: []
            ).validate(for: deleteRequest)
        )
    }

    func testPushRejectedOutcomeShapeAndCodeMatchStatus() throws {
        let request = makePushRequest(operation: .update)
        let mutation = request.mutations[0]
        var conflict = RejectedMutation(
            mutationID: mutation.mutationID,
            table: mutation.table,
            pk: mutation.pk,
            outcomeSchema: request.schema,
            status: .conflict,
            code: .versionConflict,
            message: "conflict",
            retryable: nil,
            serverRow: nil,
            rowChecksum: nil,
            serverVersion: nil
        )
        try PushResponse(
            batchID: request.batchID,
            serverTime: "2026-01-01T00:00:00.000000Z",
            accepted: [],
            rejected: [conflict]
        ).validate(for: request)

        conflict.serverVersion = "fence-version"
        try PushResponse(
            batchID: request.batchID,
            serverTime: "2026-01-01T00:00:00.000000Z",
            accepted: [],
            rejected: [conflict]
        ).validate(for: request)

        conflict.serverRow = ["field-id": AnyCodable("r1"), "field-title": AnyCodable("server")]
        conflict.rowChecksum = nil
        XCTAssertThrowsError(try rejectedResponse(conflict, request: request).validate(for: request))
        conflict.rowChecksum = validChecksum
        conflict.serverVersion = nil
        XCTAssertThrowsError(try rejectedResponse(conflict, request: request).validate(for: request))

        conflict.serverRow = nil
        conflict.rowChecksum = nil
        conflict.serverVersion = nil
        conflict.code = .policyRejected
        XCTAssertThrowsError(try rejectedResponse(conflict, request: request).validate(for: request))

        var terminal = conflict
        terminal.status = .rejectedTerminal
        terminal.code = .policyRejected
        try rejectedResponse(terminal, request: request).validate(for: request)
        terminal.code = .versionConflict
        XCTAssertThrowsError(try rejectedResponse(terminal, request: request).validate(for: request))
        terminal.code = .policyRejected
        terminal.serverVersion = "not-permitted"
        XCTAssertThrowsError(try rejectedResponse(terminal, request: request).validate(for: request))
    }

    func testSchemaIncompatibleDeleteAllowsEmptyFieldIDs() throws {
        let deleteRequest = makePushRequest(operation: .delete)
        let deleteMutation = deleteRequest.mutations[0]
        let deleteOutcome = RejectedMutation(
            mutationID: deleteMutation.mutationID,
            table: deleteMutation.table,
            pk: deleteMutation.pk,
            outcomeSchema: deleteRequest.schema,
            status: .rejectedTerminal,
            code: .schemaIncompatible,
            message: "table removed",
            retryable: false,
            serverRow: nil,
            rowChecksum: nil,
            serverVersion: nil,
            authoredSchema: deleteMutation.authoredSchema,
            currentSchema: deleteRequest.schema,
            incompatibleFieldIDs: []
        )
        try rejectedResponse(deleteOutcome, request: deleteRequest).validate(for: deleteRequest)

        let updateRequest = makePushRequest(operation: .update)
        let updateMutation = updateRequest.mutations[0]
        let updateOutcome = RejectedMutation(
            mutationID: updateMutation.mutationID,
            table: updateMutation.table,
            pk: updateMutation.pk,
            outcomeSchema: updateRequest.schema,
            status: .rejectedTerminal,
            code: .schemaIncompatible,
            message: "field removed",
            retryable: false,
            serverRow: nil,
            rowChecksum: nil,
            serverVersion: nil,
            authoredSchema: updateMutation.authoredSchema,
            currentSchema: updateRequest.schema,
            incompatibleFieldIDs: []
        )
        XCTAssertThrowsError(try rejectedResponse(updateOutcome, request: updateRequest).validate(for: updateRequest))
    }

    func testPullRequiredChecksumsFixtureDecodesAndValidates() throws {
        let response: PullResponse = try decodeFixtureValue(
            path: "conformance/protocol/pull-required-checksums.json",
            jsonPath: ["expected", "response"]
        )

        XCTAssertEqual(response.scopeSetVersion, 13)
        XCTAssertEqual(response.scopeCursors["workouts_user:u_123"], "workouts_user_u_123_890.sig")
        try response.validate()
        try response.validate(
            activeScopes: ["workouts_user:u_123"],
            requestScopeSetVersion: 13
        )

        var regressing = response
        regressing.scopeSetVersion = 12
        XCTAssertThrowsError(try regressing.validate(
            activeScopes: ["workouts_user:u_123"],
            requestScopeSetVersion: 13
        ))
    }

    func testPullValidationRejectsInvalidOperationAndScopeBindings() throws {
        let scopeID = "scope-a"
        let checksum = ChecksumObject(
            algorithm: "sha256",
            version: 1,
            encoding: "hex",
            digest: String(repeating: "0", count: 64)
        )
        let change = ChangeRecord(
            scope: scopeID,
            table: "items",
            op: .insert,
            pk: ["id": AnyCodable("row-a")],
            row: nil,
            rowChecksum: nil,
            serverVersion: "server-version"
        )
        let invalidOperation = PullResponse(
            changes: [change],
            scopeSetVersion: 1,
            scopeCursors: [scopeID: "cursor-a"],
            scopeUpdates: ScopeAssignmentDelta(add: [], remove: []),
            rebuild: [],
            hasMore: false,
            checksums: [scopeID: checksum]
        )
        XCTAssertThrowsError(try invalidOperation.validate(activeScopes: [scopeID]))

        var invalidScope = invalidOperation
        invalidScope.changes[0].op = .delete
        invalidScope.changes[0].scope = "scope-b"
        XCTAssertThrowsError(try invalidScope.validate(activeScopes: [scopeID]))

        var invalidAssignment = invalidOperation
        invalidAssignment.changes = []
        invalidAssignment.scopeUpdates = ScopeAssignmentDelta(
            add: [ScopeAssignment(id: "scope-b", cursor: "forged-cursor")],
            remove: []
        )
        invalidAssignment.checksums?["scope-b"] = checksum
        XCTAssertThrowsError(try invalidAssignment.validate(activeScopes: [scopeID]))
    }

    func testRebuildFixturePagesDecodeAndValidate() throws {
        let request: RebuildRequest = try decodeFixtureValue(
            path: "conformance/scopes/rebuild-single-scope.json",
            jsonPath: ["input", "request"]
        )
        let pages: [RebuildResponse] = try decodeFixtureValue(
            path: "conformance/scopes/rebuild-single-scope.json",
            jsonPath: ["expected", "pages"]
        )

        XCTAssertEqual(pages.count, 2)
        XCTAssertTrue(pages[1].isFinalPage())
        try pages[0].validate(for: request)
        try pages[1].validate(for: request)

        var misbound = pages[0]
        misbound.scope = "another-scope"
        XCTAssertThrowsError(try misbound.validate(for: request))
    }

    func testPortableSchemaManifestFixtureDecodesAndValidates() throws {
        let manifest: SchemaManifest = try decodeFixtureValue(
            path: "conformance/schema/schema-manifest-portable.json",
            jsonPath: ["manifest"]
        )

        XCTAssertEqual(manifest.tables.count, 2)
        XCTAssertEqual(manifest.tables[1].composition, .multiScope)
        XCTAssertEqual(manifest.tables[0].lifecycle.updatedAtFieldID, "fld_workouts_updated_at")
        XCTAssertEqual(manifest.tables[0].lifecycle.deletedAtFieldID, "fld_workouts_deleted_at")
        try manifest.validate()
    }

    func testPortableSchemaManifestRejectsSemanticMutants() throws {
        let manifest: SchemaManifest = try decodeFixtureValue(
            path: "conformance/schema/schema-manifest-portable.json",
            jsonPath: ["manifest"]
        )
        var invalid = [SchemaManifest]()

        var decimal = manifest
        decimal.tables[0].fields[1].type = "decimal"
        invalid.append(decimal)

        var primaryKey = manifest
        primaryKey.tables[0].fields[0].writable = true
        invalid.append(primaryKey)

        var lifecycle = manifest
        lifecycle.tables[0].fields[2].type = "string"
        invalid.append(lifecycle)

        var relationIdentity = manifest
        relationIdentity.tables[1].relationID = relationIdentity.tables[0].relationID
        invalid.append(relationIdentity)

        var index = manifest
        index.tables[0].indexes[0].fieldIDs = []
        invalid.append(index)

        for mutant in invalid {
            XCTAssertThrowsError(try mutant.validate())
        }
    }

    func testPortableSchemaManifestConvertsToLocalSchemaTables() throws {
        let manifest: SchemaManifest = try decodeFixtureValue(
            path: "conformance/schema/schema-manifest-portable.json",
            jsonPath: ["manifest"]
        )

        let tables = try manifest.localTables()

        XCTAssertEqual(tables.count, 2)
        XCTAssertEqual(tables[0].tableName, "workouts")
        XCTAssertEqual(tables[0].primaryKey, ["id"])
        XCTAssertEqual(tables[0].updatedAtColumn, "updated_at")
        XCTAssertEqual(tables[0].deletedAtColumn, "deleted_at")
        XCTAssertTrue(tables[0].columns.contains { $0.name == "id" && $0.isPrimaryKey })
        XCTAssertTrue(tables[1].columns.contains { $0.name == "user_id" && !$0.isPrimaryKey })
    }

    func testPortableSchemaManifestFixtureUsesCanonicalTypeNames() throws {
        let manifest: SchemaManifest = try decodeFixtureValue(
            path: "conformance/schema/schema-manifest-portable.json",
            jsonPath: ["manifest"]
        )

        let allowed: Set<String> = ["string", "int", "int64", "decimal", "float", "boolean", "datetime", "date", "time", "json", "bytes"]
        let emittedTypes = Set(manifest.tables.flatMap(\.fields).map(\.type))

        XCTAssertFalse(emittedTypes.isEmpty)
        XCTAssertTrue(emittedTypes.isSubset(of: allowed), "fixture emitted non-canonical portable types: \(emittedTypes.subtracting(allowed))")
    }

    func testPortableSchemaManifestRejectsUnknownFieldType() throws {
        var manifest: SchemaManifest = try decodeFixtureValue(
            path: "conformance/schema/schema-manifest-portable.json",
            jsonPath: ["manifest"]
        )
        manifest.tables[0].fields[0].type = "uuid"

        XCTAssertThrowsError(try manifest.localTables()) { error in
            guard case ContractViolation.unsupportedColumnType(_, _, "uuid") = error else {
                return XCTFail("expected unsupported field type, got \(error)")
            }
        }
    }

    func testUpgradeRequiredErrorFixtureDecodes() throws {
        let errorResponse: ErrorResponse = try decodeFixtureValue(
            path: "conformance/protocol/error-upgrade-required.json",
            jsonPath: ["expected", "response"]
        )

        XCTAssertEqual(errorResponse.error.code, .upgradeRequired)
        XCTAssertFalse(errorResponse.error.retryable)
    }

    func testRetryAfterFixtureMatchesAuthoredGrammar() throws {
        let data = try Data(contentsOf: fixtureURL(path: "conformance/protocol/retry-after-v1.json"))
        let fixture = try decoder.decode(RetryAfterFixture.self, from: data)
        let now = try XCTUnwrap(ISO8601DateFormatter().date(from: "2026-01-01T00:00:00Z"))

        XCTAssertEqual(fixture.version, 1)
        XCTAssertEqual(fixture.grammar.deltaSeconds, "(?:0|[1-9][0-9]*)(?:\\.[0-9]+)?")
        XCTAssertEqual(fixture.grammar.httpDate, "IMF-fixdate")
        for testCase in fixture.cases {
            XCTAssertEqual(
                HttpClient.parseRetryAfter(testCase.value, now: now) != nil,
                testCase.valid,
                "Retry-After case \(testCase.value.debugDescription)"
            )
        }
    }

    private func decodeFixtureValue<T: Decodable>(path: String, jsonPath: [String]) throws -> T {
        let fixtureURL = try fixtureURL(path: path)
        let data = try Data(contentsOf: fixtureURL)
        let object = try JSONSerialization.jsonObject(with: data)
        let nested = try value(at: jsonPath, in: object)
        let nestedData = try JSONSerialization.data(withJSONObject: nested)
        return try decoder.decode(T.self, from: nestedData)
    }

    private var validChecksum: ChecksumObject {
        ChecksumObject(
            algorithm: "sha256",
            version: 1,
            encoding: "hex",
            digest: String(repeating: "a", count: 64)
        )
    }

    private func makePushRequest(operation: Synchro.Operation) -> PushRequest {
        let schema = SchemaRef(version: 1, hash: protocolTestSchemaHash)
        let baseVersion: String?
        let columns: [String: AnyCodable]?
        switch operation {
        case .insert:
            baseVersion = nil
            columns = ["field-title": AnyCodable("Title")]
        case .update, .upsert:
            baseVersion = "base-version"
            columns = ["field-title": AnyCodable("Title")]
        case .delete:
            baseVersion = "base-version"
            columns = nil
        }
        let mutation = Mutation(
            mutationID: "00000000-0000-5000-8000-000000000001",
            table: "table-orders",
            op: operation,
            pk: ["field-id": AnyCodable("r1")],
            authoredSchema: schema,
            baseVersion: baseVersion,
            clientVersion: "2026-01-01T00:00:00.000000Z",
            columns: columns
        )
        return PushRequest(
            clientID: "client-1",
            clientGeneration: 1,
            batchID: "00000000-0000-5000-8000-000000000002",
            schema: schema,
            mutations: [mutation]
        )
    }

    private func rejectedResponse(_ outcome: RejectedMutation, request: PushRequest) -> PushResponse {
        PushResponse(
            batchID: request.batchID,
            serverTime: "2026-01-01T00:00:00.000000Z",
            accepted: [],
            rejected: [outcome]
        )
    }

    private func fixtureURL(path: String) throws -> URL {
        var current = URL(fileURLWithPath: #filePath)
        for _ in 0..<8 {
            let candidate = current.deletingLastPathComponent().appendingPathComponent(path)
            if FileManager.default.fileExists(atPath: candidate.path) {
                return candidate
            }
            current = current.deletingLastPathComponent()
        }
        throw NSError(domain: "ContractTests", code: 1, userInfo: [NSLocalizedDescriptionKey: "fixture not found: \(path)"])
    }

    private func value(at jsonPath: [String], in root: Any) throws -> Any {
        var current = root
        for key in jsonPath {
            guard let object = current as? [String: Any], let next = object[key] else {
                throw NSError(domain: "ContractTests", code: 2, userInfo: [NSLocalizedDescriptionKey: "missing json path component \(key)"])
            }
            current = next
        }
        return current
    }
}

private struct RetryAfterFixture: Decodable {
    let version: Int
    let grammar: RetryAfterGrammar
    let cases: [RetryAfterCase]
}

private struct RetryAfterGrammar: Decodable {
    let deltaSeconds: String
    let httpDate: String

    enum CodingKeys: String, CodingKey {
        case deltaSeconds = "delta_seconds"
        case httpDate = "http_date"
    }
}

private struct RetryAfterCase: Decodable {
    let value: String
    let valid: Bool
}
