import Foundation
@preconcurrency import GRDB

// MARK: - Push

public struct PushRecord: Codable, Sendable {
    public var id: String
    public var tableName: String
    public var operation: String
    public var data: [String: AnyCodable]?
    public var clientUpdatedAt: String
    public var baseUpdatedAt: String?
    var localRevision: Int64 = 0
    var fieldValuesByID: [String: StoredFieldValue]? = nil

    enum CodingKeys: String, CodingKey {
        case id
        case tableName = "table_name"
        case operation
        case data
        case clientUpdatedAt = "client_updated_at"
        case baseUpdatedAt = "base_updated_at"
    }

    init(
        id: String,
        tableName: String,
        operation: String,
        data: [String: AnyCodable]?,
        clientUpdatedAt: String,
        baseUpdatedAt: String?,
        localRevision: Int64,
        fieldValuesByID: [String: StoredFieldValue]? = nil
    ) {
        self.id = id
        self.tableName = tableName
        self.operation = operation
        self.data = data
        self.clientUpdatedAt = clientUpdatedAt
        self.baseUpdatedAt = baseUpdatedAt
        self.localRevision = localRevision
        self.fieldValuesByID = fieldValuesByID
    }
}

// MARK: - Schema

public struct SchemaResponse: Codable, Sendable {
    public var schemaVersion: Int64
    public var schemaHash: String
    public var serverTime: Date
    public var manifest: SchemaManifest

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case schemaHash = "schema_hash"
        case serverTime = "server_time"
        case manifest
    }

    func localTables() throws -> [LocalSchemaTable] {
        try manifest.localTables()
    }
}

// MARK: - Table Meta

// MARK: - SDK Types

public enum SyncStatus: String, Codable, CaseIterable, Sendable, Equatable {
    case uninitialized
    case localReady = "local_ready"
    case connecting
    case schemaApplying = "schema_applying"
    case ready
    case pushing
    case pulling
    case rebuilding
    case backoff
    case error
    case stopped

    public func permitsTransition(to next: SyncStatus) -> Bool {
        switch self {
        case .uninitialized:
            return [.localReady, .error, .stopped].contains(next)
        case .localReady:
            return [.connecting, .error, .stopped].contains(next)
        case .connecting:
            return [.schemaApplying, .ready, .backoff, .error, .stopped].contains(next)
        case .schemaApplying:
            return [.ready, .rebuilding, .error, .stopped].contains(next)
        case .ready:
            return [.connecting, .pushing, .pulling, .rebuilding, .error, .stopped].contains(next)
        case .pushing:
            return [.pushing, .ready, .pulling, .connecting, .backoff, .error, .stopped].contains(next)
        case .pulling:
            return [.pulling, .ready, .rebuilding, .connecting, .backoff, .error, .stopped].contains(next)
        case .rebuilding:
            return [.rebuilding, .ready, .connecting, .backoff, .error, .stopped].contains(next)
        case .backoff:
            return [.connecting, .pushing, .pulling, .rebuilding, .error, .stopped].contains(next)
        case .error:
            return [.localReady, .stopped].contains(next)
        case .stopped:
            return next == .localReady
        }
    }
}

public enum SyncOperationKind: String, Codable, Sendable, Equatable {
    case opening
    case connecting
    case schema
    case pushing
    case pulling
    case rebuilding
    case lifecycle
    case database
}

public enum SyncRecoveryAction: String, Codable, Sendable, Equatable {
    case retry
    case schemaReset = "schema_reset"
    case none
}

public enum SyncFailureCode: String, Codable, Sendable, Equatable {
    case authenticationRequired = "auth_required"
    case clientRetired = "client_retired"
    case idempotencyConflict = "idempotency_conflict"
    case invalidRequest = "invalid_request"
    case invalidResponse = "invalid_response"
    case invalidSchemaReference = "invalid_schema_reference"
    case invalidStateTransition = "invalid_state_transition"
    case localDatabase = "local_database"
    case schemaApplicationFailed = "schema_application_failed"
    case syncIntegrityFailure = "sync_integrity_failure"
    case unsupportedSchema = "unsupported_schema"
    case upgradeRequired = "upgrade_required"
}

public struct SyncFailure: Codable, Sendable, Equatable {
    public let operation: SyncOperationKind
    public let code: SyncFailureCode
    public let retryable: Bool
    public let message: String
    public let recoveryAction: SyncRecoveryAction
    public let metadata: [String: String]

    public init(
        operation: SyncOperationKind,
        code: SyncFailureCode,
        retryable: Bool,
        message: String,
        recoveryAction: SyncRecoveryAction,
        metadata: [String: String] = [:]
    ) {
        self.operation = operation
        self.code = code
        self.retryable = retryable
        self.message = String(message.prefix(256))
        self.recoveryAction = recoveryAction
        self.metadata = Dictionary(
            uniqueKeysWithValues: metadata.prefix(8).map {
                (String($0.key.prefix(64)), String($0.value.prefix(128)))
            }
        )
    }
}

public struct SyncStateChangeEvent: Sendable, Equatable {
    public let from: SyncStatus
    public let to: SyncStatus
}

public struct SyncBackoffEvent: Sendable, Equatable {
    public let operation: SyncOperationKind
    public let attempt: Int64
    public let retryAt: Date
}

public struct SyncSchemaEvent: Sendable, Equatable {
    public let source: SchemaRef
    public let target: SchemaRef
    public let action: SchemaAction
}

public struct SyncMutationEvent: Sendable, Equatable {
    public let mutationID: String
    public let tableID: String
    public let status: MutationStatus
    public let rejectionCode: MutationRejectionCode?
}

public struct SyncRebuildEvent: Sendable, Equatable {
    public let scopeID: String
    public let rebuildID: String
}

public enum SyncEvent: Sendable, Equatable {
    case stateChanged(SyncStateChangeEvent)
    case backoff(SyncBackoffEvent)
    case schemaApplying(SyncSchemaEvent)
    case schemaApplied(SyncSchemaEvent)
    case mutationAccepted(SyncMutationEvent)
    case mutationRejected(SyncMutationEvent)
    case rebuildRequested(SyncRebuildEvent)
    case rebuildCompleted(SyncRebuildEvent)
    case failure(SyncFailure)
}

public enum LocalMutationStatus: String, Codable, Sendable, Equatable {
    case pending
    case sealed
    case serverRejected = "server_rejected"
    case supersededBeforeSend = "superseded_before_send"
    case cancelledBeforeSend = "cancelled_before_send"
    case blockedByPredecessor = "blocked_by_predecessor"
}

public struct AuthoredMutationField: Sendable, Equatable {
    public let fieldID: String
    public let logicalType: String
    public let value: AnyCodable

    public init(fieldID: String, logicalType: String, value: AnyCodable) {
        self.fieldID = fieldID
        self.logicalType = logicalType
        self.value = value
    }
}

@_spi(Inspection)
public struct ScopeStateInspection: Sendable, Equatable {
    public let scopeID: String
    public let cursor: String?
    public let checksum: String?
    public let localChecksum: String
    public let generation: Int64

    public init(scopeID: String, cursor: String?, checksum: String?, localChecksum: String, generation: Int64) {
        self.scopeID = scopeID
        self.cursor = cursor
        self.checksum = checksum
        self.localChecksum = localChecksum
        self.generation = generation
    }
}

@_spi(Inspection)
public struct ScopeRowInspection: Sendable, Equatable {
    public let scopeID: String
    public let tableName: String
    public let recordID: String
    public let checksum: String
    public let generation: Int64

    public init(scopeID: String, tableName: String, recordID: String, checksum: String, generation: Int64) {
        self.scopeID = scopeID
        self.tableName = tableName
        self.recordID = recordID
        self.checksum = checksum
        self.generation = generation
    }
}

@_spi(Inspection)
public struct ClientStateCaptureInspection: Sendable, Equatable {
    public let schema: SchemaRef?
    public let scopeStates: [ScopeStateInspection]
    public let scopeStatesTruncated: Bool
    public let scopeRows: [ScopeRowInspection]
    public let scopeRowsTruncated: Bool
    public let rebuildAttempts: [RebuildAttemptInspection]
    public let rebuildAttemptsTruncated: Bool
    public let rebuildReceipts: [RebuildReceiptInspection]
    public let rebuildReceiptsTruncated: Bool
    public let rowMetadata: [RowMetadataInspection]
    public let rowMetadataTruncated: Bool
    public let overflowed: Bool
    public let applicationRowCount: Int
    public let mutationLedgerCount: Int
    public let mutationOutcomeCount: Int
    public let sealedBatchCount: Int
    public let rejectedMutationCount: Int
    public let scopeStateCount: Int
    public let scopeRowCount: Int
    public let provenanceCount: Int
    public let rowMetadataCount: Int
    public let rebuildAttemptCount: Int
    public let rebuildReceiptCount: Int
    public let provenanceMaintenanceWorkCursor: Int64

    public init(
        schema: SchemaRef?,
        scopeStates: [ScopeStateInspection],
        scopeStatesTruncated: Bool,
        scopeRows: [ScopeRowInspection],
        scopeRowsTruncated: Bool,
        rebuildAttempts: [RebuildAttemptInspection],
        rebuildAttemptsTruncated: Bool,
        rebuildReceipts: [RebuildReceiptInspection],
        rebuildReceiptsTruncated: Bool,
        rowMetadata: [RowMetadataInspection],
        rowMetadataTruncated: Bool,
        overflowed: Bool,
        applicationRowCount: Int,
        mutationLedgerCount: Int,
        mutationOutcomeCount: Int,
        sealedBatchCount: Int,
        rejectedMutationCount: Int,
        scopeStateCount: Int,
        scopeRowCount: Int,
        provenanceCount: Int,
        rowMetadataCount: Int,
        rebuildAttemptCount: Int,
        rebuildReceiptCount: Int,
        provenanceMaintenanceWorkCursor: Int64
    ) {
        self.schema = schema
        self.scopeStates = scopeStates
        self.scopeStatesTruncated = scopeStatesTruncated
        self.scopeRows = scopeRows
        self.scopeRowsTruncated = scopeRowsTruncated
        self.rebuildAttempts = rebuildAttempts
        self.rebuildAttemptsTruncated = rebuildAttemptsTruncated
        self.rebuildReceipts = rebuildReceipts
        self.rebuildReceiptsTruncated = rebuildReceiptsTruncated
        self.rowMetadata = rowMetadata
        self.rowMetadataTruncated = rowMetadataTruncated
        self.overflowed = overflowed
        self.applicationRowCount = applicationRowCount
        self.mutationLedgerCount = mutationLedgerCount
        self.mutationOutcomeCount = mutationOutcomeCount
        self.sealedBatchCount = sealedBatchCount
        self.rejectedMutationCount = rejectedMutationCount
        self.scopeStateCount = scopeStateCount
        self.scopeRowCount = scopeRowCount
        self.provenanceCount = provenanceCount
        self.rowMetadataCount = rowMetadataCount
        self.rebuildAttemptCount = rebuildAttemptCount
        self.rebuildReceiptCount = rebuildReceiptCount
        self.provenanceMaintenanceWorkCursor = provenanceMaintenanceWorkCursor
    }
}

@_spi(Inspection)
public struct RowMetadataInspection: Sendable, Equatable {
    public let tableName: String
    public let recordID: String
    public let serverVersion: String
    public let rowChecksum: String?

    public init(tableName: String, recordID: String, serverVersion: String, rowChecksum: String?) {
        self.tableName = tableName
        self.recordID = recordID
        self.serverVersion = serverVersion
        self.rowChecksum = rowChecksum
    }
}

@_spi(Inspection)
public struct RebuildAttemptInspection: Sendable, Equatable {
    public let scopeID: String
    public let rebuildID: String
    public let clientGeneration: Int64
    public let schemaVersion: Int64
    public let schemaHash: String
    public let generation: Int64
    public let cursor: String?
    public let pageLimit: Int

    public init(
        scopeID: String,
        rebuildID: String,
        clientGeneration: Int64,
        schemaVersion: Int64,
        schemaHash: String,
        generation: Int64,
        cursor: String?,
        pageLimit: Int
    ) {
        self.scopeID = scopeID
        self.rebuildID = rebuildID
        self.clientGeneration = clientGeneration
        self.schemaVersion = schemaVersion
        self.schemaHash = schemaHash
        self.generation = generation
        self.cursor = cursor
        self.pageLimit = pageLimit
    }
}

@_spi(Inspection)
public struct RebuildReceiptInspection: Sendable, Equatable {
    public let rebuildIDFingerprint: String
    public let pageCount: Int
    public let returnedRecordCount: Int
    public let requestChainExpected: [String]
    public let requestChainObserved: [String]
    public let recordIdentitiesHex: [String]
    public let receivedRowChecksums: [String]
    public let computedRowChecksums: [String]
    public let computedScopeChecksum: String?
    public let finalScopeChecksum: String?
    public let storedScopeChecksum: String?
    public let localScopeChecksum: String?

    public init(
        rebuildIDFingerprint: String,
        pageCount: Int,
        returnedRecordCount: Int,
        requestChainExpected: [String],
        requestChainObserved: [String],
        recordIdentitiesHex: [String],
        receivedRowChecksums: [String],
        computedRowChecksums: [String],
        computedScopeChecksum: String?,
        finalScopeChecksum: String?,
        storedScopeChecksum: String?,
        localScopeChecksum: String?
    ) {
        self.rebuildIDFingerprint = rebuildIDFingerprint
        self.pageCount = pageCount
        self.returnedRecordCount = returnedRecordCount
        self.requestChainExpected = requestChainExpected
        self.requestChainObserved = requestChainObserved
        self.recordIdentitiesHex = recordIdentitiesHex
        self.receivedRowChecksums = receivedRowChecksums
        self.computedRowChecksums = computedRowChecksums
        self.computedScopeChecksum = computedScopeChecksum
        self.finalScopeChecksum = finalScopeChecksum
        self.storedScopeChecksum = storedScopeChecksum
        self.localScopeChecksum = localScopeChecksum
    }
}


public struct PendingMutationInspection: Sendable, Equatable {
    public let mutationID: String
    public let localOrder: Int64
    public let tableID: String
    public let tableName: String
    public let recordID: String
    public let primaryKeyFieldID: String
    public let primaryKeyLogicalType: String
    public let operation: Operation
    public let authoredSchema: SchemaRef
    public let baseVersion: String?
    public let clientVersion: String
    public let status: LocalMutationStatus
    public let sourceKind: String
    public let dependsOnMutationID: String?
    public let normalizedMutationID: String?
    public let sealedBatchID: String?
    public let sealedOrdinal: Int?
    public let authoredFields: [AuthoredMutationField]

    public init(
        mutationID: String,
        localOrder: Int64,
        tableID: String,
        tableName: String,
        recordID: String,
        primaryKeyFieldID: String,
        primaryKeyLogicalType: String,
        operation: Operation,
        authoredSchema: SchemaRef,
        baseVersion: String?,
        clientVersion: String,
        status: LocalMutationStatus,
        sourceKind: String,
        dependsOnMutationID: String?,
        normalizedMutationID: String?,
        sealedBatchID: String?,
        sealedOrdinal: Int?,
        authoredFields: [AuthoredMutationField]
    ) {
        self.mutationID = mutationID
        self.localOrder = localOrder
        self.tableID = tableID
        self.tableName = tableName
        self.recordID = recordID
        self.primaryKeyFieldID = primaryKeyFieldID
        self.primaryKeyLogicalType = primaryKeyLogicalType
        self.operation = operation
        self.authoredSchema = authoredSchema
        self.baseVersion = baseVersion
        self.clientVersion = clientVersion
        self.status = status
        self.sourceKind = sourceKind
        self.dependsOnMutationID = dependsOnMutationID
        self.normalizedMutationID = normalizedMutationID
        self.sealedBatchID = sealedBatchID
        self.sealedOrdinal = sealedOrdinal
        self.authoredFields = authoredFields
    }
}

public struct RejectedMutationInspection: Sendable, Equatable {
    public let mutationID: String
    public let localOrder: Int64
    public let tableName: String
    public let recordID: String
    public let status: MutationStatus
    public let code: MutationRejectionCode
    public let message: String?
    public let serverRowJSON: String?
    public let serverVersion: String?
    public let mutationJSON: String
    public let rejectionJSON: String
    public let mutation: Mutation
    public let rejection: RejectedMutation
    public let createdAt: String
    public let updatedAt: String

    public init(
        mutationID: String,
        localOrder: Int64,
        tableName: String,
        recordID: String,
        status: MutationStatus,
        code: MutationRejectionCode,
        message: String?,
        serverRowJSON: String?,
        serverVersion: String?,
        mutationJSON: String,
        rejectionJSON: String,
        mutation: Mutation,
        rejection: RejectedMutation,
        createdAt: String,
        updatedAt: String
    ) {
        self.mutationID = mutationID
        self.localOrder = localOrder
        self.tableName = tableName
        self.recordID = recordID
        self.status = status
        self.code = code
        self.message = message
        self.serverRowJSON = serverRowJSON
        self.serverVersion = serverVersion
        self.mutationJSON = mutationJSON
        self.rejectionJSON = rejectionJSON
        self.mutation = mutation
        self.rejection = rejection
        self.createdAt = createdAt
        self.updatedAt = updatedAt
    }
}

public struct ConflictEvent: Sendable {
    public let table: String
    public let recordID: String
    public let clientData: [String: AnyCodable]?
    public let serverData: [String: AnyCodable]?
}

public struct ExecResult: Sendable {
    public let rowsAffected: Int
}

public struct SQLStatement: @unchecked Sendable {
    public let sql: String
    public let params: [(any DatabaseValueConvertible)?]?

    public init(sql: String, params: [(any DatabaseValueConvertible)?]? = nil) {
        self.sql = sql
        self.params = params
    }
}

public struct ColumnDef: Sendable {
    public let name: String
    public let type: String
    public let nullable: Bool
    public let defaultValue: String?
    public let primaryKey: Bool

    public init(name: String, type: String, nullable: Bool = true, defaultValue: String? = nil, primaryKey: Bool = false) {
        self.name = name
        self.type = type
        self.nullable = nullable
        self.defaultValue = defaultValue
        self.primaryKey = primaryKey
    }
}

public struct TableOptions: Sendable {
    public let ifNotExists: Bool
    public let withoutRowid: Bool

    public init(ifNotExists: Bool = true, withoutRowid: Bool = false) {
        self.ifNotExists = ifNotExists
        self.withoutRowid = withoutRowid
    }
}

public protocol Cancellable: Sendable {
    func cancel()
}

// MARK: - AnyCodable

public struct AnyCodable: Codable, @unchecked Sendable, Equatable {
    public let value: Any

    public init(_ value: Any) {
        self.value = value
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        if container.decodeNil() {
            value = NSNull()
        } else if let bool = try? container.decode(Bool.self) {
            value = bool
        } else if let int = try? container.decode(Int64.self) {
            value = int
        } else if let double = try? container.decode(Double.self) {
            value = double
        } else if let string = try? container.decode(String.self) {
            value = string
        } else if let array = try? container.decode([AnyCodable].self) {
            value = array.map { $0.value }
        } else if let dict = try? container.decode([String: AnyCodable].self) {
            value = dict.mapValues { $0.value }
        } else {
            throw DecodingError.dataCorruptedError(in: container, debugDescription: "unsupported type")
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch value {
        case is NSNull:
            try container.encodeNil()
        case let bool as Bool:
            try container.encode(bool)
        case let int as Int:
            try container.encode(int)
        case let int64 as Int64:
            try container.encode(int64)
        case let double as Double:
            try container.encode(double)
        case let string as String:
            try container.encode(string)
        case let array as [Any]:
            try container.encode(array.map { AnyCodable($0) })
        case let dict as [String: Any]:
            try container.encode(dict.mapValues { AnyCodable($0) })
        default:
            throw EncodingError.invalidValue(value, .init(codingPath: encoder.codingPath, debugDescription: "unsupported type: \(type(of: value))"))
        }
    }

    public static func == (lhs: AnyCodable, rhs: AnyCodable) -> Bool {
        switch (lhs.value, rhs.value) {
        case is (NSNull, NSNull):
            return true
        case let (l as Bool, r as Bool):
            return l == r
        case let (l as Int64, r as Int64):
            return l == r
        case let (l as Double, r as Double):
            return l == r
        case let (l as String, r as String):
            return l == r
        default:
            return false
        }
    }
}

// MARK: - JSON Date Coding

extension JSONDecoder {
    static func synchroDecoder() -> JSONDecoder {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .custom { decoder in
            let container = try decoder.singleValueContainer()
            let string = try container.decode(String.self)
            if let date = SynchroDateCoding.parse(string) {
                return date
            }
            throw DecodingError.dataCorruptedError(in: container, debugDescription: "invalid date: \(string)")
        }
        return decoder
    }
}

extension JSONEncoder {
    static func synchroEncoder() -> JSONEncoder {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        encoder.dateEncodingStrategy = .custom { date, encoder in
            var container = encoder.singleValueContainer()
            try container.encode(SynchroDateCoding.string(from: date))
        }
        return encoder
    }
}

enum SynchroDateCoding {
    private static let lock = NSLock()
    private static let fractionalFormatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }()
    private static let fallbackFormatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        return formatter
    }()

    static func parse(_ string: String) -> Date? {
        lock.lock()
        defer { lock.unlock() }
        if let date = fractionalFormatter.date(from: string) {
            return date
        }
        return fallbackFormatter.date(from: string)
    }

    static func string(from date: Date) -> String {
        lock.lock()
        defer { lock.unlock() }
        return fractionalFormatter.string(from: date)
    }

    static func now() -> String {
        string(from: Date())
    }
}
