import CryptoKit
import XCTest
@testable import Synchro

final class IntegrityTests: XCTestCase {
    func testSchemaManifestHashMatchesAuthoredContract() throws {
        let expected = "5dc97fc5ea571dd7555d877e08cecc102113c6efd63976d37d498341c8b32d51"
        var manifest = SchemaManifest(
            schemaVersion: 42,
            schemaHash: expected,
            parentSchema: SchemaRef(
                version: 41,
                hash: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
            ),
            transitionClass: "class_3",
            compatibilityFloor: 42,
            tables: [
                TableSchema(
                    tableID: "tbl_documents",
                    relationID: "rel_documents",
                    name: "documents",
                    primaryKeyFieldID: "fld_documents_id",
                    lifecycle: LifecycleSchema(
                        createdAtFieldID: nil,
                        updatedAtFieldID: nil,
                        deletedAtFieldID: nil
                    ),
                    composition: .multiScope,
                    fields: [
                        ColumnSchema(
                            fieldID: "fld_documents_id",
                            name: "id",
                            type: "string",
                            nullable: false,
                            writable: false,
                            precision: nil,
                            scale: nil
                        ),
                        ColumnSchema(
                            fieldID: "fld_documents_amount",
                            name: "amount",
                            type: "decimal",
                            nullable: false,
                            writable: true,
                            precision: 18,
                            scale: 4
                        ),
                    ],
                    indexes: [
                        IndexSchema(
                            indexID: "idx_documents_amount",
                            name: "idx_documents_amount",
                            fieldIDs: ["fld_documents_amount"],
                            unique: false
                        )
                    ]
                )
            ]
        )

        XCTAssertEqual(try Integrity.schemaManifestHash(manifest), expected)
        manifest.tables[0].name = "changed_documents"
        XCTAssertNotEqual(try Integrity.schemaManifestHash(manifest), expected)
    }

    func testAuthoredManifestFixturesHaveValidHashes() throws {
        let decoder = JSONDecoder.synchroDecoder()
        let portableDocument = try XCTUnwrap(
            JSONSerialization.jsonObject(
                with: Data(contentsOf: fixtureURL(path: "conformance/schema/schema-manifest-portable.json"))
            ) as? [String: Any]
        )
        let portable = try decoder.decode(
            SchemaManifest.self,
            from: JSONSerialization.data(withJSONObject: try XCTUnwrap(portableDocument["manifest"]))
        )
        XCTAssertEqual(
            portable.schemaHash,
            try Integrity.schemaManifestHash(portable),
            "conformance/schema/schema-manifest-portable.json"
        )

        let connectDocument = try XCTUnwrap(
            JSONSerialization.jsonObject(
                with: Data(contentsOf: fixtureURL(path: "conformance/protocol/connect-rebuild-local.json"))
            ) as? [String: Any]
        )
        let expected = try XCTUnwrap(connectDocument["expected"] as? [String: Any])
        let connect = try decoder.decode(
            ConnectResponse.self,
            from: JSONSerialization.data(withJSONObject: try XCTUnwrap(expected["response"]))
        )
        let connectManifest = try XCTUnwrap(connect.schemaDefinition)
        XCTAssertEqual(
            connectManifest.schemaHash,
            try Integrity.schemaManifestHash(connectManifest),
            "conformance/protocol/connect-rebuild-local.json"
        )
    }

    func testCanonicalWireJSONValidation() throws {
        XCTAssertNoThrow(try Integrity.validateCanonicalWireJSON(Data("{\"b\":1,\"a\":0.000001,\"text\":\"\\ufdd0\"}".utf8)))
        for source in [
            "{\"value\":-0}",
            "{\"value\":1.0}",
            "{\"value\":1e-6}",
            "{\"value\":1,\"value\":1}",
        ] {
            XCTAssertThrowsError(try Integrity.validateCanonicalWireJSON(Data(source.utf8)), source)
        }
        let deep = String(repeating: "[", count: 130) + "0" + String(repeating: "]", count: 130)
        XCTAssertThrowsError(try Integrity.validateCanonicalWireJSON(Data(deep.utf8)))
        XCTAssertNoThrow(try Integrity.validateCanonicalWireJSON(Data("{\"float\":9007199254740992}".utf8)))
    }

    func testDecimalCapacityAndUnicodeDomains() throws {
        let decimal = LocalSchemaColumn(
            fieldID: "field-decimal",
            name: "decimal_value",
            logicalType: "decimal",
            nullable: false,
            writable: true,
            precision: 6,
            scale: 2,
            sqliteDefaultSQL: nil,
            isPrimaryKey: false
        )
        XCTAssertNoThrow(try Integrity.encodedTypedValue(json: "\"1234.56\"", field: decimal))
        XCTAssertThrowsError(try Integrity.encodedTypedValue(json: "\"123456\"", field: decimal))
        XCTAssertThrowsError(try Integrity.encodedTypedValue(json: "\"١.٢\"", field: decimal))

        let noncharacter = String(UnicodeScalar(0xfdd0)!)
        let pk = ["field-id": AnyCodable("row-1")]
        XCTAssertNoThrow(try Integrity.rowDigest(
            schemaHash: String(repeating: "b", count: 64),
            table: canonicalTable,
            pk: pk,
            row: [
                "field-id": AnyCodable("row-1"),
                "field-json": AnyCodable("{\"n\":0}"),
                "field-text": AnyCodable(noncharacter),
            ],
            serverVersion: "version-1"
        ))
        XCTAssertNoThrow(try Integrity.rowDigest(
            schemaHash: String(repeating: "b", count: 64),
            table: canonicalTable,
            pk: pk,
            row: [
                "field-id": AnyCodable("row-1"),
                "field-json": AnyCodable("{\"value\":\"\(noncharacter)\"}"),
                "field-text": AnyCodable("valid"),
            ],
            serverVersion: "version-1"
        ))
    }

    func testRowDigestUsesCanonicalAndSafeJSONNumbers() throws {
        let pk = ["field-id": AnyCodable("row-1")]
        for source in [
            "{\"n\":0.000001}",
            "{\"n\":1e-7}",
        ] {
            let digest = try Integrity.rowDigest(
                schemaHash: String(repeating: "b", count: 64),
                table: canonicalTable,
                pk: pk,
                row: [
                    "field-id": AnyCodable("row-1"),
                    "field-json": AnyCodable(source),
                    "field-text": AnyCodable("valid"),
                ],
                serverVersion: "version-1"
            )
            XCTAssertEqual(digest.checksum.digest.count, 64)
        }

        for source in [
            "{\"n\":1e-6}",
            "{\"n\":9007199254740992}",
            "{\"n\":100000000000000000000}",
            "{\"n\":1e+21}",
        ] {
            XCTAssertThrowsError(try Integrity.rowDigest(
                schemaHash: String(repeating: "b", count: 64),
                table: canonicalTable,
                pk: pk,
                row: [
                    "field-id": AnyCodable("row-1"),
                    "field-json": AnyCodable(source),
                    "field-text": AnyCodable("valid"),
                ],
                serverVersion: "version-1"
            ))
        }
    }

    func testAuthoredChecksumVectors() throws {
        let data = try Data(contentsOf: fixtureURL(path: "conformance/vectors/canonical-v1.json"))
        let document = try XCTUnwrap(JSONSerialization.jsonObject(with: data) as? [String: Any])
        let vectors = try XCTUnwrap(document["vectors"] as? [[String: Any]])
        let checksumKinds = Set(["typed_value", "row_identity", "row_digest", "scope_digest"])
        var executed = 0

        for vector in vectors {
            guard let kind = vector["kind"] as? String, checksumKinds.contains(kind) else {
                continue
            }
            executed += 1
            let valid = try XCTUnwrap(vector["valid"] as? Bool)
            let vectorID = try XCTUnwrap(vector["vector_id"] as? String)
            let result = Result { try execute(vector: vector, kind: kind) }
            if valid {
                guard case let .success(output) = result else {
                    if case let .failure(error) = result {
                        XCTFail("valid authored vector failed: \(vectorID): \(error)")
                    }
                    continue
                }
                let expected = try XCTUnwrap(vector["expected"] as? [String: Any])
                XCTAssertEqual(output.preimage.hex, try XCTUnwrap(expected["canonical_bytes_hex"] as? String), vectorID)
                XCTAssertEqual(
                    Data(SHA256.hash(data: output.preimage)).hex,
                    try XCTUnwrap(expected["expected_bytes_sha256"] as? String),
                    vectorID
                )
                XCTAssertEqual(output.digest, expected["expected_sha256"] as? String, vectorID)
            } else if case .success = result {
                XCTFail("invalid authored vector was accepted: \(vectorID)")
            }
        }

        XCTAssertEqual(executed, 90)
    }

    func testScopeDigestRejectsMalformedRowIdentities() throws {
        let identity = try Integrity.rowIdentity(
            table: table,
            pk: ["field-id": AnyCodable(7)]
        )
        let digest = ChecksumObject(
            algorithm: "sha256",
            version: 1,
            encoding: "hex",
            digest: String(repeating: "a", count: 64)
        )

        XCTAssertNoThrow(try Integrity.scopeDigest(
            schemaHash: String(repeating: "b", count: 64),
            scopeID: "scope-1",
            entries: [(identity, digest)]
        ))

        var invalidTag = identity
        invalidTag[invalidTag.count - 6] = 0xff
        let malformed = [
            invalidTag,
            identity.dropLast(),
            identity + Data([0]),
        ]
        for value in malformed {
            XCTAssertThrowsError(try Integrity.scopeDigest(
                schemaHash: String(repeating: "b", count: 64),
                scopeID: "scope-1",
                entries: [(Data(value), digest)]
            ))
        }
    }

    private var table: LocalSchemaTable {
        LocalSchemaTable(
            tableID: "table-1",
            relationID: "relation-1",
            tableName: "records",
            primaryKeyFieldID: "field-id",
            createdAtFieldID: nil,
            updatedAtFieldID: nil,
            deletedAtFieldID: nil,
            updatedAtColumn: "",
            deletedAtColumn: "",
            composition: .singleScope,
            primaryKey: ["id"],
            columns: [
                LocalSchemaColumn(
                    fieldID: "field-id",
                    name: "id",
                    logicalType: "int",
                    nullable: false,
                    writable: false,
                    precision: nil,
                    scale: nil,
                    sqliteDefaultSQL: nil,
                    isPrimaryKey: true
                ),
            ]
        )
    }

    private var canonicalTable: LocalSchemaTable {
        LocalSchemaTable(
            tableID: "table-canonical",
            relationID: "relation-canonical",
            tableName: "canonical_records",
            primaryKeyFieldID: "field-id",
            createdAtFieldID: nil,
            updatedAtFieldID: nil,
            deletedAtFieldID: nil,
            updatedAtColumn: "",
            deletedAtColumn: "",
            composition: .singleScope,
            primaryKey: ["id"],
            columns: [
                column(fieldID: "field-id", name: "id", type: "string", writable: false, isPrimaryKey: true),
                column(fieldID: "field-json", name: "json_value", type: "json"),
                column(fieldID: "field-text", name: "text_value", type: "string"),
            ]
        )
    }

    private func column(
        fieldID: String,
        name: String,
        type: String,
        writable: Bool = true,
        isPrimaryKey: Bool = false
    ) -> LocalSchemaColumn {
        LocalSchemaColumn(
            fieldID: fieldID,
            name: name,
            logicalType: type,
            nullable: false,
            writable: writable,
            precision: nil,
            scale: nil,
            sqliteDefaultSQL: nil,
            isPrimaryKey: isPrimaryKey
        )
    }

    private func execute(vector: [String: Any], kind: String) throws -> VectorExecution {
        let input = try XCTUnwrap(vector["input"] as? [String: Any])
        if kind == "typed_value" {
            let spec = try XCTUnwrap(input["field_spec"] as? [String: Any])
            let field = LocalSchemaColumn(
                fieldID: "vector-field",
                name: "vector_value",
                logicalType: try XCTUnwrap(spec["type"] as? String),
                nullable: try XCTUnwrap(spec["nullable"] as? Bool),
                writable: true,
                precision: (spec["precision"] as? NSNumber)?.intValue,
                scale: (spec["scale"] as? NSNumber)?.intValue,
                sqliteDefaultSQL: nil,
                isPrimaryKey: false
            )
            let source = try XCTUnwrap(input["raw_json"] as? String)
            return VectorExecution(preimage: try Integrity.encodedTypedValue(json: source, field: field), digest: nil)
        }

        if kind == "row_digest" {
            let manifestJSON = try XCTUnwrap(input["manifest_json"] as? String)
            let manifest = try JSONDecoder().decode(SchemaManifest.self, from: Data(manifestJSON.utf8))
            let tableID = try XCTUnwrap(input["table_id"] as? String)
            let table = try XCTUnwrap(try manifest.localTables().first(where: { $0.tableID == tableID }))
            let pkSource = try XCTUnwrap(input["pk_json"] as? String)
            let pkValue = try JSONSerialization.jsonObject(with: Data(pkSource.utf8), options: [.fragmentsAllowed])
            let rowSource = try XCTUnwrap(input["row_json"] as? String)
            let rawRow = try strictJSONObject(rowSource)
            let row = rawRow.mapValues(AnyCodable.init)
            let serverVersion = try XCTUnwrap(input["server_version"] as? String)
            let preimage = try Integrity.rowDigestPreimage(
                schemaHash: manifest.schemaHash,
                table: table,
                pk: [table.primaryKeyFieldID: AnyCodable(pkValue)],
                row: row,
                serverVersion: serverVersion
            ).preimage
            let digest = try Integrity.rowDigest(
                schemaHash: manifest.schemaHash,
                table: table,
                pk: [table.primaryKeyFieldID: AnyCodable(pkValue)],
                row: row,
                serverVersion: serverVersion
            ).checksum.digest
            return VectorExecution(preimage: preimage, digest: digest)
        }

        if kind == "row_identity" {
            let manifestJSON = try XCTUnwrap(input["manifest_json"] as? String)
            let manifest = try JSONDecoder().decode(SchemaManifest.self, from: Data(manifestJSON.utf8))
            let tableID = try XCTUnwrap(input["table_id"] as? String)
            let table = try XCTUnwrap(try manifest.localTables().first(where: { $0.tableID == tableID }))
            let pkJSON = try XCTUnwrap(input["pk_json"] as? String)
            let pkValue = try JSONSerialization.jsonObject(with: Data(pkJSON.utf8), options: [.fragmentsAllowed])
            return VectorExecution(preimage: try Integrity.rowIdentity(
                table: table,
                pk: [table.primaryKeyFieldID: AnyCodable(pkValue)]
            ), digest: nil)
        }

        let schemaHash = try XCTUnwrap(input["schema_hash"] as? String)
        let scopeID = try XCTUnwrap(input["scope_id"] as? String)
        let rawEntries = try XCTUnwrap(input["entries"] as? [[String: Any]])
        let entries = try rawEntries.map { entry in
            let identity = try Data(lowerHex: XCTUnwrap(entry["row_identity_hex"] as? String))
            let digest = try XCTUnwrap(entry["row_digest_hex"] as? String)
            return (
                identity,
                ChecksumObject(algorithm: "sha256", version: 1, encoding: "hex", digest: digest)
            )
        }
        let preimage = try Integrity.scopeDigestPreimage(schemaHash: schemaHash, scopeID: scopeID, entries: entries)
        let digest = try Integrity.scopeDigest(schemaHash: schemaHash, scopeID: scopeID, entries: entries).digest
        return VectorExecution(preimage: preimage, digest: digest)
    }

    private func strictJSONObject(_ source: String) throws -> [String: Any] {
        try rejectDuplicateTopLevelKeys(source)
        let value = try JSONSerialization.jsonObject(with: Data(source.utf8))
        return try XCTUnwrap(value as? [String: Any])
    }

    private func rejectDuplicateTopLevelKeys(_ source: String) throws {
        let bytes = Array(source.utf8)
        var index = 0
        skipWhitespace(bytes, index: &index)
        guard index < bytes.count, bytes[index] == 123 else { throw IntegrityError.invalidValue("row is not an object") }
        index += 1
        var keys = Set<String>()
        while true {
            skipWhitespace(bytes, index: &index)
            if index < bytes.count, bytes[index] == 125 { return }
            let token = try consumeString(bytes, index: &index)
            let key = try XCTUnwrap(JSONSerialization.jsonObject(with: Data(token), options: [.fragmentsAllowed]) as? String)
            guard keys.insert(key).inserted else { throw IntegrityError.invalidValue("duplicate row field") }
            skipWhitespace(bytes, index: &index)
            guard index < bytes.count, bytes[index] == 58 else { throw IntegrityError.invalidValue("invalid row object") }
            index += 1
            try consumeValue(bytes, index: &index)
            skipWhitespace(bytes, index: &index)
            if index < bytes.count, bytes[index] == 44 {
                index += 1
                continue
            }
            guard index < bytes.count, bytes[index] == 125 else { throw IntegrityError.invalidValue("invalid row object") }
            return
        }
    }

    private func consumeString(_ bytes: [UInt8], index: inout Int) throws -> ArraySlice<UInt8> {
        guard index < bytes.count, bytes[index] == 34 else { throw IntegrityError.invalidValue("invalid JSON string") }
        let start = index
        index += 1
        while index < bytes.count {
            if bytes[index] == 92 {
                index += 2
            } else if bytes[index] == 34 {
                index += 1
                return bytes[start..<index]
            } else {
                index += 1
            }
        }
        throw IntegrityError.invalidValue("unterminated JSON string")
    }

    private func consumeValue(_ bytes: [UInt8], index: inout Int) throws {
        skipWhitespace(bytes, index: &index)
        guard index < bytes.count else { throw IntegrityError.invalidValue("missing JSON value") }
        if bytes[index] == 34 {
            _ = try consumeString(bytes, index: &index)
            return
        }
        if bytes[index] == 123 || bytes[index] == 91 {
            var closers: [UInt8] = [bytes[index] == 123 ? 125 : 93]
            index += 1
            while index < bytes.count, !closers.isEmpty {
                if bytes[index] == 34 {
                    _ = try consumeString(bytes, index: &index)
                } else if bytes[index] == 123 || bytes[index] == 91 {
                    closers.append(bytes[index] == 123 ? 125 : 93)
                    index += 1
                } else if bytes[index] == closers.last {
                    closers.removeLast()
                    index += 1
                } else {
                    index += 1
                }
            }
            guard closers.isEmpty else { throw IntegrityError.invalidValue("unterminated JSON value") }
            return
        }
        while index < bytes.count, bytes[index] != 44, bytes[index] != 125 {
            index += 1
        }
    }

    private func skipWhitespace(_ bytes: [UInt8], index: inout Int) {
        while index < bytes.count, [9, 10, 13, 32].contains(bytes[index]) {
            index += 1
        }
    }

    private func fixtureURL(path: String) throws -> URL {
        var current = URL(fileURLWithPath: #filePath)
        for _ in 0..<8 {
            let candidate = current.deletingLastPathComponent().appendingPathComponent(path)
            if FileManager.default.fileExists(atPath: candidate.path) {
                return candidate
            }
            current.deleteLastPathComponent()
        }
        throw NSError(domain: "IntegrityTests", code: 1, userInfo: [NSLocalizedDescriptionKey: "fixture not found: \(path)"])
    }
}

private struct VectorExecution {
    let preimage: Data
    let digest: String?
}

private extension Data {
    init(lowerHex: String) throws {
        guard lowerHex.count.isMultiple(of: 2), lowerHex.allSatisfy({ $0.isNumber || ("a"..."f").contains(String($0)) }) else {
            throw IntegrityError.invalidValue("invalid lowercase hex")
        }
        var data = Data(capacity: lowerHex.count / 2)
        var index = lowerHex.startIndex
        while index < lowerHex.endIndex {
            let next = lowerHex.index(index, offsetBy: 2)
            guard let byte = UInt8(lowerHex[index..<next], radix: 16) else {
                throw IntegrityError.invalidValue("invalid lowercase hex")
            }
            data.append(byte)
            index = next
        }
        self = data
    }

    var hex: String {
        map { String(format: "%02x", $0) }.joined()
    }
}
