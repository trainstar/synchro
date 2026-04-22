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
        XCTAssertEqual(response.scopes.add.count, 2)
        try response.validate()
    }

    func testPullRequiredChecksumsFixtureDecodesAndValidates() throws {
        let request: PullRequest = try decodeFixtureValue(
            path: "conformance/protocol/pull-required-checksums.json",
            jsonPath: ["input", "request"]
        )
        let response: PullResponse = try decodeFixtureValue(
            path: "conformance/protocol/pull-required-checksums.json",
            jsonPath: ["expected", "response"]
        )

        XCTAssertEqual(request.checksumMode, .required)
        XCTAssertEqual(response.scopeSetVersion, 13)
        XCTAssertEqual(response.scopeCursors["workouts_user:u_123"], "c_890")
        try response.validate(for: request)
    }

    func testRebuildFixturePagesDecodeAndValidate() throws {
        let pages: [RebuildResponse] = try decodeFixtureValue(
            path: "conformance/scopes/rebuild-single-scope.json",
            jsonPath: ["expected", "pages"]
        )

        XCTAssertEqual(pages.count, 2)
        XCTAssertTrue(pages[1].isFinalPage())
        try pages[0].validate()
        try pages[1].validate()
    }

    func testPortableSchemaManifestFixtureDecodesAndValidates() throws {
        let manifest: SchemaManifest = try decodeFixtureValue(
            path: "conformance/schema/schema-manifest-portable.json",
            jsonPath: ["manifest"]
        )

        XCTAssertEqual(manifest.tables.count, 2)
        XCTAssertEqual(manifest.tables[1].composition, .multiScope)
        XCTAssertEqual(manifest.tables[0].updatedAtColumn, "updated_at")
        XCTAssertEqual(manifest.tables[0].deletedAtColumn, "deleted_at")
        try manifest.validate()
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

        let allowed: Set<String> = ["string", "int", "int64", "float", "boolean", "datetime", "date", "time", "json", "bytes"]
        let emittedTypes = Set(manifest.tables.flatMap { $0.columns ?? [] }.map(\.type))

        XCTAssertFalse(emittedTypes.isEmpty)
        XCTAssertTrue(emittedTypes.isSubset(of: allowed), "fixture emitted non-canonical portable types: \(emittedTypes.subtracting(allowed))")
    }

    func testSQLiteSchemaNormalizesPatternAliases() {
        XCTAssertEqual(SQLiteSchema.normalizedLogicalType("numeric(5,1)"), "float")
        XCTAssertEqual(SQLiteSchema.normalizedLogicalType("varchar(255)"), "string")
        XCTAssertEqual(SQLiteSchema.normalizedLogicalType("text[]"), "json")
        XCTAssertEqual(SQLiteSchema.normalizedLogicalType("interval"), "string")
        XCTAssertEqual(SQLiteSchema.normalizedLogicalType("int4range"), "string")
    }

    func testUpgradeRequiredErrorFixtureDecodes() throws {
        let errorResponse: ErrorResponse = try decodeFixtureValue(
            path: "conformance/protocol/error-upgrade-required.json",
            jsonPath: ["expected", "response"]
        )

        XCTAssertEqual(errorResponse.error.code, .upgradeRequired)
        XCTAssertFalse(errorResponse.error.retryable)
    }

    private func decodeFixtureValue<T: Decodable>(path: String, jsonPath: [String]) throws -> T {
        let fixtureURL = try fixtureURL(path: path)
        let data = try Data(contentsOf: fixtureURL)
        let object = try JSONSerialization.jsonObject(with: data)
        let nested = try value(at: jsonPath, in: object)
        let nestedData = try JSONSerialization.data(withJSONObject: nested)
        return try decoder.decode(T.self, from: nestedData)
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
