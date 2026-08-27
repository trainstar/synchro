import Foundation
import XCTest
@testable @_spi(Inspection) import Synchro

final class InspectionFacadeContractTests: XCTestCase {
    func testSwiftInspectionFacadeMatchesSharedContract() throws {
        let data = try Data(contentsOf: try contractURL())
        let contract = try JSONDecoder().decode(FacadeContract.self, from: data)

        XCTAssertEqual(contract.schemaVersion, 1)
        XCTAssertEqual(contract.facade, String(describing: SynchroInspection.self))
        XCTAssertEqual(contract.operations.sorted { $0.name < $1.name }, actualOperations.sorted { $0.name < $1.name })
        XCTAssertEqual(contract.models.sorted { $0.name < $1.name }, actualModels.sorted { $0.name < $1.name })
    }

    private var actualOperations: [Operation] {
        [
            operation("currentSchema", SynchroInspection.currentSchema),
            operation("scopeStates", SynchroInspection.scopeStates),
            operation("scopeRows", SynchroInspection.scopeRows),
            operation("captureState", SynchroInspection.captureState, parameter: ("maximumRecords", Int.self)),
            operation(
                "rowMetadata",
                SynchroInspection.rowMetadata,
                parameters: [("tableName", String.self), ("recordID", String.self)]
            ),
            operation("rebuildAttempts", SynchroInspection.rebuildAttempts),
            operation("rebuildReceipts", SynchroInspection.rebuildReceipts),
        ]
    }

    private var actualModels: [Model] {
        let schema = SchemaRef(version: 1, hash: "hash")
        let scopeState = ScopeStateInspection(
            scopeID: "scope", cursor: "cursor", checksum: "checksum", localChecksum: "local", generation: 1
        )
        let scopeRow = ScopeRowInspection(
            scopeID: "scope", tableName: "table", recordID: "record", checksum: "checksum", generation: 1
        )
        let metadata = RowMetadataInspection(
            tableName: "table", recordID: "record", serverVersion: "version", rowChecksum: "checksum"
        )
        let attempt = RebuildAttemptInspection(
            scopeID: "scope", rebuildID: "rebuild", clientGeneration: 1, schemaVersion: 1,
            schemaHash: "hash", generation: 1, cursor: "cursor", pageLimit: 1
        )
        let receipt = RebuildReceiptInspection(
            rebuildIDFingerprint: "fingerprint", pageCount: 1, returnedRecordCount: 1,
            requestChainExpected: ["expected"], requestChainObserved: ["observed"],
            recordIdentitiesHex: ["identity"], receivedRowChecksums: ["received"],
            computedRowChecksums: ["computed"], computedScopeChecksum: "computed",
            finalScopeChecksum: "final", storedScopeChecksum: "stored", localScopeChecksum: "local"
        )
        let capture = ClientStateCaptureInspection(
            schema: schema, scopeStates: [scopeState], scopeStatesTruncated: false,
            scopeRows: [scopeRow], scopeRowsTruncated: false, rebuildAttempts: [attempt],
            rebuildAttemptsTruncated: false, rebuildReceipts: [receipt], rebuildReceiptsTruncated: false,
            rowMetadata: [metadata], rowMetadataTruncated: false, overflowed: false,
            applicationRowCount: 1, mutationLedgerCount: 1, mutationOutcomeCount: 1, sealedBatchCount: 1,
            rejectedMutationCount: 1, scopeStateCount: 1, scopeRowCount: 1, provenanceCount: 1,
            rowMetadataCount: 1, rebuildAttemptCount: 1, rebuildReceiptCount: 1,
            provenanceMaintenanceWorkCursor: 1
        )
        return [schema, scopeState, scopeRow, capture, metadata, attempt, receipt].map(model)
    }

    private func operation<Result>(
        _ name: String,
        _ function: @escaping (SynchroInspection) -> () throws -> Result
    ) -> Operation {
        _ = function
        return Operation(name: name, parameters: [], result: typeShape(Result.self))
    }

    private func operation<Parameter, Result>(
        _ name: String,
        _ function: @escaping (SynchroInspection) -> (Parameter) throws -> Result,
        parameter: (String, Parameter.Type)
    ) -> Operation {
        _ = function
        return Operation(
            name: name,
            parameters: [Member(name: parameter.0, type: typeShape(parameter.1))],
            result: typeShape(Result.self)
        )
    }

    private func operation<FirstParameter, SecondParameter, Result>(
        _ name: String,
        _ function: @escaping (SynchroInspection) -> (FirstParameter, SecondParameter) throws -> Result,
        parameters: [(String, Any.Type)]
    ) -> Operation {
        _ = function
        return Operation(
            name: name,
            parameters: parameters.map { Member(name: $0.0, type: typeShape($0.1)) },
            result: typeShape(Result.self)
        )
    }

    private func model(_ value: Any) -> Model {
        let mirror = Mirror(reflecting: value)
        return Model(
            name: String(describing: type(of: value)),
            fields: mirror.children.map { Member(name: $0.label!, type: typeShape($0.value)) }
        )
    }

    private func typeShape(_ type: Any.Type) -> TypeShape {
        if type == Optional<SchemaRef>.self { return TypeShape(name: "SchemaRef", nullable: true) }
        if type == [ScopeStateInspection].self {
            return TypeShape(name: "array", nullable: false, element: TypeShape(name: "ScopeStateInspection", nullable: false))
        }
        if type == [ScopeRowInspection].self {
            return TypeShape(name: "array", nullable: false, element: TypeShape(name: "ScopeRowInspection", nullable: false))
        }
        if type == ClientStateCaptureInspection.self { return TypeShape(name: "ClientStateCaptureInspection", nullable: false) }
        if type == Optional<RowMetadataInspection>.self { return TypeShape(name: "RowMetadataInspection", nullable: true) }
        if type == [RebuildAttemptInspection].self {
            return TypeShape(name: "array", nullable: false, element: TypeShape(name: "RebuildAttemptInspection", nullable: false))
        }
        if type == [RebuildReceiptInspection].self {
            return TypeShape(name: "array", nullable: false, element: TypeShape(name: "RebuildReceiptInspection", nullable: false))
        }
        switch type {
        case is String.Type: return TypeShape(name: "string", nullable: false)
        case is Bool.Type: return TypeShape(name: "bool", nullable: false)
        case is Int.Type: return TypeShape(name: "int", nullable: false)
        case is Int64.Type: return TypeShape(name: "int64", nullable: false)
        default: return TypeShape(name: String(describing: type), nullable: false)
        }
    }

    private func typeShape(_ value: Any) -> TypeShape {
        let mirror = Mirror(reflecting: value)
        if mirror.displayStyle == .optional {
            return TypeShape(name: typeShape(mirror.children.first!.value).name, nullable: true)
        }
        if mirror.displayStyle == .collection {
            return TypeShape(name: "array", nullable: false, element: typeShape(mirror.children.first!.value))
        }
        return typeShape(type(of: value))
    }

    private func contractURL() throws -> URL {
        var current = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
        for _ in 0..<8 {
            let candidate = current.appendingPathComponent("conformance/protocol/inspection-facade-v1.json")
            if FileManager.default.fileExists(atPath: candidate.path) {
                return candidate
            }
            current.deleteLastPathComponent()
        }
        throw NSError(domain: "InspectionFacadeContractTests", code: 1)
    }
}

private struct FacadeContract: Decodable {
    let schemaVersion: Int
    let facade: String
    let operations: [Operation]
    let models: [Model]

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case facade, operations, models
    }
}

private struct Operation: Codable, Equatable {
    let name: String
    let parameters: [Member]
    let result: TypeShape
}

private struct Model: Codable, Equatable {
    let name: String
    let fields: [Member]
}

private struct Member: Codable, Equatable {
    let name: String
    let type: TypeShape
}

private final class TypeShape: Codable, Equatable {
    let name: String
    let nullable: Bool
    let element: TypeShape?

    init(name: String, nullable: Bool, element: TypeShape? = nil) {
        self.name = name
        self.nullable = nullable
        self.element = element
    }

    static func == (lhs: TypeShape, rhs: TypeShape) -> Bool {
        lhs.name == rhs.name && lhs.nullable == rhs.nullable && lhs.element == rhs.element
    }
}
