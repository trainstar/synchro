import Foundation
import Darwin
import GRDB
@_spi(Inspection) import Synchro

private let maximumRunnerLineBytes = 1 << 20
private let maximumCaptureValueBytes = 1 << 20

private struct RunnerCommand: Decodable {
    let schemaVersion: Int
    let operation: String
    let databasePath: String?
    let serverURL: String?
    let authToken: String?
    let clientID: String?
    let seedDatabasePath: String?
    let platform: String?
    let appVersion: String?
    let pullPageSize: Int?
    let pushBatchSize: Int?
    let transportCapacity: Int?
    let localAction: LocalActionPayload?
    let lifecycleOperation: String?
    let transportOperation: String?
    let callID: String?
    let method: String?
    let rowSelectors: [RowSelector]?
    private let suppliedKeys: Set<String>

    enum CodingKeys: String, CodingKey, CaseIterable {
        case schemaVersion = "schema_version"
        case operation
        case databasePath = "database_path"
        case serverURL = "server_url"
        case authToken = "auth_token"
        case clientID = "client_id"
        case seedDatabasePath = "seed_database_path"
        case platform
        case appVersion = "app_version"
        case pullPageSize = "pull_page_size"
        case pushBatchSize = "push_batch_size"
        case transportCapacity = "transport_capacity"
        case localAction = "local_action"
        case lifecycleOperation = "lifecycle_operation"
        case transportOperation = "transport_operation"
        case callID = "call_id"
        case method
        case rowSelectors = "row_selectors"
    }

    init(from decoder: Decoder) throws {
        let allKeys = try decoder.container(keyedBy: RunnerCodingKey.self).allKeys
        let knownKeys = Set(CodingKeys.allCases.map(\.rawValue))
        guard allKeys.allSatisfy({ knownKeys.contains($0.stringValue) }) else {
            throw DecodingError.dataCorrupted(.init(
                codingPath: decoder.codingPath,
                debugDescription: "unknown runner command member"
            ))
        }
        suppliedKeys = Set(allKeys.map(\.stringValue))
        let container = try decoder.container(keyedBy: CodingKeys.self)
        schemaVersion = try container.decode(Int.self, forKey: .schemaVersion)
        operation = try container.decode(String.self, forKey: .operation)
        databasePath = try container.decodeIfPresent(String.self, forKey: .databasePath)
        serverURL = try container.decodeIfPresent(String.self, forKey: .serverURL)
        authToken = try container.decodeIfPresent(String.self, forKey: .authToken)
        clientID = try container.decodeIfPresent(String.self, forKey: .clientID)
        seedDatabasePath = try container.decodeIfPresent(String.self, forKey: .seedDatabasePath)
        platform = try container.decodeIfPresent(String.self, forKey: .platform)
        appVersion = try container.decodeIfPresent(String.self, forKey: .appVersion)
        pullPageSize = try container.decodeIfPresent(Int.self, forKey: .pullPageSize)
        pushBatchSize = try container.decodeIfPresent(Int.self, forKey: .pushBatchSize)
        transportCapacity = try container.decodeIfPresent(Int.self, forKey: .transportCapacity)
        localAction = try container.decodeIfPresent(LocalActionPayload.self, forKey: .localAction)
        lifecycleOperation = try container.decodeIfPresent(String.self, forKey: .lifecycleOperation)
        transportOperation = try container.decodeIfPresent(String.self, forKey: .transportOperation)
        callID = try container.decodeIfPresent(String.self, forKey: .callID)
        method = try container.decodeIfPresent(String.self, forKey: .method)
        rowSelectors = try container.decodeIfPresent([RowSelector].self, forKey: .rowSelectors)
    }

    func containsOnly(_ allowed: [CodingKeys]) -> Bool {
        suppliedKeys.isSubset(of: Set(allowed.map(\.rawValue)))
    }
}

private enum LocalActionOperation: String, Decodable {
    case insert
    case update
    case delete
}

private enum RunnerJSONValue: Decodable {
    case null
    case string(String)
    case boolean(Bool)
    case integer(Int64)
    case double(Double)
    case bytes(Data)

    init(from decoder: Decoder) throws {
        let scalar = try decoder.singleValueContainer()
        if scalar.decodeNil() {
            self = .null
            return
        }
        if let value = Self.tryDecode(Bool.self, from: scalar) {
            self = .boolean(value)
            return
        }
        if let value = Self.tryDecode(Int64.self, from: scalar) {
            self = .integer(value)
            return
        }
        if let value = Self.tryDecode(Double.self, from: scalar), value.isFinite {
            self = .double(value)
            return
        }
        if let value = Self.tryDecode(String.self, from: scalar) {
            self = .string(value)
            return
        }

        guard let container = try? decoder.container(keyedBy: RunnerCodingKey.self) else {
            throw DecodingError.dataCorrupted(.init(
                codingPath: decoder.codingPath,
                debugDescription: "local action value must be a supported JSON scalar"
            ))
        }
        let keys = Set(container.allKeys.map(\.stringValue))
        guard keys == Set(["type", "value"]) else {
            throw DecodingError.dataCorrupted(.init(
                codingPath: decoder.codingPath,
                debugDescription: "local action value must be a supported JSON scalar"
            ))
        }
        let type = try container.decode(String.self, forKey: RunnerCodingKey(stringValue: "type")!)
        let valueKey = RunnerCodingKey(stringValue: "value")!
        switch type {
        case "null":
            guard try container.decodeNil(forKey: valueKey) else {
                throw DecodingError.dataCorrupted(.init(
                    codingPath: decoder.codingPath,
                    debugDescription: "typed null value is not null"
                ))
            }
            self = .null
        case "string":
            self = .string(try container.decode(String.self, forKey: valueKey))
        case "boolean":
            self = .boolean(try container.decode(Bool.self, forKey: valueKey))
        case "integer":
            self = .integer(try container.decode(Int64.self, forKey: valueKey))
        case "double":
            let value = try container.decode(Double.self, forKey: valueKey)
            guard value.isFinite else {
                throw DecodingError.dataCorrupted(.init(
                    codingPath: decoder.codingPath,
                    debugDescription: "typed double is not finite"
                ))
            }
            self = .double(value)
        case "bytes":
            let encoded = try container.decode(String.self, forKey: valueKey)
            guard let bytes = decodeBase64URL(encoded) else {
                throw DecodingError.dataCorrupted(.init(
                    codingPath: decoder.codingPath,
                    debugDescription: "typed bytes value is not canonical base64url"
                ))
            }
            self = .bytes(bytes)
        default:
            throw DecodingError.dataCorrupted(.init(
                codingPath: decoder.codingPath,
                debugDescription: "unsupported typed local action value"
            ))
        }
    }

    private static func tryDecode<T: Decodable>(
        _ type: T.Type,
        from container: SingleValueDecodingContainer
    ) -> T? {
        try? container.decode(type)
    }

    func databaseValue() -> (any DatabaseValueConvertible)? {
        switch self {
        case .null:
            return nil
        case .string(let value):
            return value
        case .boolean(let value):
            return value ? 1 : 0
        case .integer(let value):
            return value
        case .double(let value):
            return value
        case .bytes(let value):
            return value
        }
    }
}

private struct LocalActionPayload: Decodable {
    let operation: LocalActionOperation
    let tableName: String
    let primaryKeyField: String
    let primaryKey: RunnerJSONValue
    let fields: [String: RunnerJSONValue]
    let authoredColumns: [String]

    enum CodingKeys: String, CodingKey, CaseIterable {
        case operation
        case tableName = "table_name"
        case primaryKeyField = "primary_key_field"
        case primaryKey = "primary_key"
        case fields
        case authoredColumns = "authored_columns"
    }

    init(from decoder: Decoder) throws {
        let allKeys = try decoder.container(keyedBy: RunnerCodingKey.self).allKeys
        let knownKeys = Set(CodingKeys.allCases.map(\.rawValue))
        guard allKeys.allSatisfy({ knownKeys.contains($0.stringValue) }),
              Set(allKeys.map(\.stringValue)) == Set(CodingKeys.allCases.map(\.rawValue)) else {
            throw DecodingError.dataCorrupted(.init(
                codingPath: decoder.codingPath,
                debugDescription: "local action payload is incomplete or contains an unknown member"
            ))
        }
        let container = try decoder.container(keyedBy: CodingKeys.self)
        operation = try container.decode(LocalActionOperation.self, forKey: .operation)
        tableName = try container.decode(String.self, forKey: .tableName)
        primaryKeyField = try container.decode(String.self, forKey: .primaryKeyField)
        primaryKey = try container.decode(RunnerJSONValue.self, forKey: .primaryKey)
        fields = try container.decode([String: RunnerJSONValue].self, forKey: .fields)
        authoredColumns = try container.decode([String].self, forKey: .authoredColumns)
    }
}

private struct RowSelector: Decodable {
    let tableName: String
    let primaryKeyField: String
    let primaryKey: RunnerJSONValue

    enum CodingKeys: String, CodingKey, CaseIterable {
        case tableName = "table_name"
        case primaryKeyField = "primary_key_field"
        case primaryKey = "primary_key"
    }

    init(from decoder: Decoder) throws {
        let allKeys = try decoder.container(keyedBy: RunnerCodingKey.self).allKeys
        let knownKeys = Set(CodingKeys.allCases.map(\.rawValue))
        guard allKeys.allSatisfy({ knownKeys.contains($0.stringValue) }),
              Set(allKeys.map(\.stringValue)) == Set(CodingKeys.allCases.map(\.rawValue)) else {
            throw DecodingError.dataCorrupted(.init(
                codingPath: decoder.codingPath,
                debugDescription: "row selector is incomplete or contains an unknown member"
            ))
        }
        let container = try decoder.container(keyedBy: CodingKeys.self)
        tableName = try container.decode(String.self, forKey: .tableName)
        primaryKeyField = try container.decode(String.self, forKey: .primaryKeyField)
        primaryKey = try container.decode(RunnerJSONValue.self, forKey: .primaryKey)
    }
}

private struct RunnerCodingKey: CodingKey {
    let stringValue: String
    let intValue: Int? = nil

    init?(stringValue: String) {
        self.stringValue = stringValue
    }

    init?(intValue: Int) {
        return nil
    }
}

private struct RunnerResponse: Encodable {
    let schemaVersion = 1
    let outcome: String
    let result: RunnerResult?
    let errorCode: String?

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case outcome
        case result
        case errorCode = "error_code"
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(schemaVersion, forKey: .schemaVersion)
        try container.encode(outcome, forKey: .outcome)
        try container.encode(result, forKey: .result)
        try container.encode(errorCode, forKey: .errorCode)
    }
}

private extension RunnerResult {
    /// Encodes each collection separately so an oversized response names the
    /// section that dominates it. This runs only on the overflow path.
    func sectionSizeReport(_ encoder: JSONEncoder) -> String {
        func size<T: Encodable>(_ values: [T]?) -> Int {
            guard let values else { return -1 }
            return ((try? encoder.encode(values))?.count) ?? -2
        }
        let sections: [(String, Int)] = [
            ("application_rows", size(applicationRows)),
            ("retained_mutations", size(retainedMutations)),
            ("rejected_mutations", size(rejectedMutations)),
            ("scope_states", size(scopeStates)),
            ("scope_rows", size(scopeRows)),
            ("row_metadata_records", size(rowMetadataRecords)),
            ("rebuild_attempts", size(rebuildAttempts)),
            ("rebuild_receipts", size(rebuildReceipts)),
            ("events", size(events)),
        ]
        return sections
            .filter { $0.1 != -1 }
            .sorted { $0.1 > $1.1 }
            .map { "\($0.0)=\($0.1)" }
            .joined(separator: " ")
    }
}

private struct RunnerResult: Encodable {
    var callID: String? = nil
    var state: String? = nil
    var completion: String? = nil
    var callErrorCategory: String? = nil
    var status: String? = nil
    var rowsAffected: Int? = nil
    var pendingChangeCount: Int? = nil
    var applicationRowCount: Int? = nil
    var mutationLedgerCount: Int? = nil
    var mutationOutcomeCount: Int? = nil
    var sealedBatchCount: Int? = nil
    var rejectedMutationCount: Int? = nil
    var scopeStateCount: Int? = nil
    var scopeRowCount: Int? = nil
    var provenanceCount: Int? = nil
    var rowMetadataCount: Int? = nil
    var rebuildAttemptCount: Int? = nil
    var rebuildReceiptCount: Int? = nil
    var schema: SchemaRef? = nil
    var applicationRows: [[String: AnyCodable]]? = nil
    var retainedMutations: [RetainedMutation]? = nil
    var rejectedMutations: [RetainedRejection]? = nil
    var scopeStates: [ScopeStateRecord]? = nil
    var scopeRows: [ScopeRowRecord]? = nil
    var rowMetadata: RowMetadataRecord? = nil
    var rowMetadataRecords: [RowMetadataRecord]? = nil
    var rebuildAttempts: [RebuildAttemptRecord]? = nil
    var rebuildReceipts: [RebuildReceiptRecord]? = nil
    var scopeStatesTruncated: Bool? = nil
    var scopeRowsTruncated: Bool? = nil
    var rebuildAttemptsTruncated: Bool? = nil
    var rebuildReceiptsTruncated: Bool? = nil
    var rowMetadataTruncated: Bool? = nil
    var captureOverflowed: Bool? = nil
    var provenanceMaintenanceWorkCursor: Int64? = nil
    var events: [EventRecord]? = nil
    var failure: SyncFailure? = nil
    var transportObservations: RunnerTransportObservationSnapshot? = nil

    enum CodingKeys: String, CodingKey {
        case callID = "call_id"
        case state
        case completion
        case callErrorCategory = "call_error_category"
        case status
        case rowsAffected = "rows_affected"
        case pendingChangeCount = "pending_change_count"
        case applicationRowCount = "application_row_count"
        case mutationLedgerCount = "mutation_ledger_count"
        case mutationOutcomeCount = "mutation_outcome_count"
        case sealedBatchCount = "sealed_batch_count"
        case rejectedMutationCount = "rejected_mutation_count"
        case scopeStateCount = "scope_state_count"
        case scopeRowCount = "scope_row_count"
        case provenanceCount = "provenance_count"
        case rowMetadataCount = "row_metadata_count"
        case rebuildAttemptCount = "rebuild_attempt_count"
        case rebuildReceiptCount = "rebuild_receipt_count"
        case schema
        case applicationRows = "application_rows"
        case retainedMutations = "retained_mutations"
        case rejectedMutations = "rejected_mutations"
        case scopeStates = "scope_states"
        case scopeRows = "scope_rows"
        case rowMetadata = "row_metadata"
        case rowMetadataRecords = "row_metadata_records"
        case rebuildAttempts = "rebuild_attempts"
        case rebuildReceipts = "rebuild_receipts"
        case scopeStatesTruncated = "scope_states_truncated"
        case scopeRowsTruncated = "scope_rows_truncated"
        case rebuildAttemptsTruncated = "rebuild_attempts_truncated"
        case rebuildReceiptsTruncated = "rebuild_receipts_truncated"
        case rowMetadataTruncated = "row_metadata_truncated"
        case captureOverflowed = "capture_overflowed"
        case provenanceMaintenanceWorkCursor = "provenance_maintenance_work_cursor"
        case events
        case failure
        case transportObservations = "transport_observations"
    }
}

private struct RunnerTransportObservationSnapshot: Encodable {
    let observations: [RunnerTransportObservation]
    let overflowed: Bool
    let sequenceCheckpoint: UInt64

    enum CodingKeys: String, CodingKey {
        case observations
        case overflowed
        case sequenceCheckpoint = "sequence_checkpoint"
    }

    init(_ snapshot: TransportObservationSnapshot) {
        observations = snapshot.observations.map(RunnerTransportObservation.init)
        overflowed = snapshot.overflowed
        sequenceCheckpoint = snapshot.sequenceCheckpoint
    }
}

private struct RunnerTransportObservation: Encodable {
    let sequence: UInt64
    let operationClass: TransportOperationClass
    let statusCode: Int
    let errorCode: String?
    let retryable: Bool
    let durationNanoseconds: UInt64
    let cursorFingerprints: [String]?
    let cursorFingerprintsComplete: Bool?
    let requestFacts: TransportRequestFacts?
    let rebuildResponseFacts: TransportRebuildResponseFacts?
    let pullResponseFacts: TransportPullResponseFacts?

    enum CodingKeys: String, CodingKey {
        case sequence
        case operationClass = "operation_class"
        case statusCode = "status_code"
        case errorCode = "error_code"
        case retryable
        case durationNanoseconds = "duration_nanoseconds"
        case cursorFingerprints = "cursor_fingerprints"
        case cursorFingerprintsComplete = "cursor_fingerprints_complete"
        case requestFacts = "request_facts"
        case rebuildResponseFacts = "rebuild_response_facts"
        case pullResponseFacts = "pull_response_facts"
    }

    init(_ observation: TransportObservation) {
        sequence = observation.sequence
        operationClass = observation.operationClass
        statusCode = observation.statusCode
        let (mappedCode, mappedRetryable) = transportFailureFacts(
            statusCode: observation.statusCode,
            operationClass: observation.operationClass
        )
        // One status code carries more than one error code, so the code the
        // server reported wins over the code the status implies.
        errorCode = observation.errorCode ?? mappedCode
        retryable = mappedRetryable
        durationNanoseconds = observation.durationNanoseconds
        cursorFingerprints = observation.cursorFingerprints
        cursorFingerprintsComplete = observation.cursorFingerprintsComplete
        requestFacts = observation.requestFacts
        rebuildResponseFacts = observation.rebuildResponseFacts
        pullResponseFacts = observation.pullResponseFacts
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(sequence, forKey: .sequence)
        try container.encode(operationClass, forKey: .operationClass)
        try container.encode(statusCode, forKey: .statusCode)
        try container.encode(errorCode, forKey: .errorCode)
        try container.encode(retryable, forKey: .retryable)
        try container.encode(durationNanoseconds, forKey: .durationNanoseconds)
        try container.encodeIfPresent(cursorFingerprints, forKey: .cursorFingerprints)
        try container.encodeIfPresent(cursorFingerprintsComplete, forKey: .cursorFingerprintsComplete)
        try container.encodeIfPresent(requestFacts, forKey: .requestFacts)
        try container.encodeIfPresent(rebuildResponseFacts, forKey: .rebuildResponseFacts)
        try container.encodeIfPresent(pullResponseFacts, forKey: .pullResponseFacts)
    }
}

private func transportFailureFacts(
    statusCode: Int,
    operationClass: TransportOperationClass
) -> (String?, Bool) {
    guard statusCode != 0 else { return (nil, true) }
    guard !(200..<300).contains(statusCode) else { return (nil, false) }
    switch statusCode {
    case 400:
        return ("invalid_request", false)
    case 401:
        return ("auth_required", false)
    case 409:
        switch operationClass {
        case .rebuild:
            return ("rebuild_restart_required", false)
        case .push:
            return ("idempotency_conflict", false)
        default:
            return ("client_generation_expired", false)
        }
    case 422:
        return ("schema_mismatch", false)
    case 426:
        return ("upgrade_required", false)
    case 429:
        return ("retry_later", true)
    case 500:
        return ("sync_integrity_failure", false)
    case 503:
        return ("capture_pending", true)
    default:
        return ("invalid_response", false)
    }
}

private struct RebuildReceiptRecord: Encodable {
    let rebuildIDFingerprint: String
    let pageCount: Int
    let returnedRecordCount: Int
    let requestChainExpected: [String]
    let requestChainObserved: [String]
    let recordIdentitiesHex: [String]
    let receivedRowChecksums: [String]
    let computedRowChecksums: [String]
    let computedScopeChecksum: String?
    let finalScopeChecksum: String?
    let storedScopeChecksum: String?
    let localScopeChecksum: String?

    enum CodingKeys: String, CodingKey {
        case rebuildIDFingerprint = "rebuild_id_fingerprint"
        case pageCount = "page_count"
        case returnedRecordCount = "returned_record_count"
        case requestChainExpected = "request_chain_expected"
        case requestChainObserved = "request_chain_observed"
        case recordIdentitiesHex = "record_identities_hex"
        case receivedRowChecksums = "received_row_checksums"
        case computedRowChecksums = "computed_row_checksums"
        case computedScopeChecksum = "computed_scope_checksum"
        case finalScopeChecksum = "final_scope_checksum"
        case storedScopeChecksum = "stored_scope_checksum"
        case localScopeChecksum = "local_scope_checksum"
    }

    init(_ receipt: RebuildReceiptInspection) {
        rebuildIDFingerprint = receipt.rebuildIDFingerprint
        pageCount = receipt.pageCount
        returnedRecordCount = receipt.returnedRecordCount
        requestChainExpected = receipt.requestChainExpected
        requestChainObserved = receipt.requestChainObserved
        recordIdentitiesHex = receipt.recordIdentitiesHex
        receivedRowChecksums = receipt.receivedRowChecksums
        computedRowChecksums = receipt.computedRowChecksums
        computedScopeChecksum = receipt.computedScopeChecksum
        finalScopeChecksum = receipt.finalScopeChecksum
        storedScopeChecksum = receipt.storedScopeChecksum
        localScopeChecksum = receipt.localScopeChecksum
    }
}

private struct ScopeStateRecord: Encodable {
    let scopeID: String
    let cursor: String?
    let checksum: String?
    let localChecksum: String
    let generation: Int64

    enum CodingKeys: String, CodingKey {
        case scopeID = "scope_id"
        case cursor
        case checksum
        case localChecksum = "local_checksum"
        case generation
    }

    init(_ state: ScopeStateInspection) {
        scopeID = state.scopeID
        cursor = state.cursor
        checksum = state.checksum
        localChecksum = state.localChecksum
        generation = state.generation
    }
}

private struct ScopeRowRecord: Encodable {
    let scopeID: String
    let tableName: String
    let recordID: String
    let checksum: String
    let generation: Int64

    enum CodingKeys: String, CodingKey {
        case scopeID = "scope_id"
        case tableName = "table_name"
        case recordID = "record_id"
        case checksum
        case generation
    }

    init(_ row: ScopeRowInspection) {
        scopeID = row.scopeID
        tableName = row.tableName
        recordID = row.recordID
        checksum = row.checksum
        generation = row.generation
    }
}

private struct RebuildAttemptRecord: Encodable {
    let scopeID: String
    let rebuildID: String
    let clientGeneration: Int64
    let schemaVersion: Int64
    let schemaHash: String
    let generation: Int64
    let cursor: String?
    let pageLimit: Int

    enum CodingKeys: String, CodingKey {
        case scopeID = "scope_id"
        case rebuildID = "rebuild_id"
        case clientGeneration = "client_generation"
        case schemaVersion = "schema_version"
        case schemaHash = "schema_hash"
        case generation
        case cursor
        case pageLimit = "page_limit"
    }

    init(_ attempt: RebuildAttemptInspection) {
        scopeID = attempt.scopeID
        rebuildID = attempt.rebuildID
        clientGeneration = attempt.clientGeneration
        schemaVersion = attempt.schemaVersion
        schemaHash = attempt.schemaHash
        generation = attempt.generation
        cursor = attempt.cursor
        pageLimit = attempt.pageLimit
    }
}

private struct RowMetadataRecord: Encodable {
    let tableName: String
    let recordID: String
    let serverVersion: String
    let rowChecksum: String?

    enum CodingKeys: String, CodingKey {
        case tableName = "table_name"
        case recordID = "record_id"
        case serverVersion = "server_version"
        case rowChecksum = "row_checksum"
    }

    init(_ metadata: RowMetadataInspection) {
        tableName = metadata.tableName
        recordID = metadata.recordID
        serverVersion = metadata.serverVersion
        rowChecksum = metadata.rowChecksum
    }
}

private struct RetainedMutation: Encodable {
    let mutationID: String
    let localOrder: Int64
    let tableID: String
    let tableName: String
    let recordID: String
    let primaryKeyFieldID: String
    let primaryKeyLogicalType: String
    let operation: String
    let authoredSchema: SchemaRef
    let baseVersion: String?
    let clientVersion: String
    let status: String
    let authoredFields: [RetainedField]

    enum CodingKeys: String, CodingKey {
        case mutationID = "mutation_id"
        case localOrder = "local_order"
        case tableID = "table_id"
        case tableName = "table_name"
        case recordID = "record_id"
        case primaryKeyFieldID = "primary_key_field_id"
        case primaryKeyLogicalType = "primary_key_logical_type"
        case operation
        case authoredSchema = "authored_schema"
        case baseVersion = "base_version"
        case clientVersion = "client_version"
        case status
        case authoredFields = "authored_fields"
    }

    init(_ mutation: PendingMutationInspection) {
        mutationID = mutation.mutationID
        localOrder = mutation.localOrder
        tableID = mutation.tableID
        tableName = mutation.tableName
        recordID = mutation.recordID
        primaryKeyFieldID = mutation.primaryKeyFieldID
        primaryKeyLogicalType = mutation.primaryKeyLogicalType
        operation = mutation.operation.rawValue
        authoredSchema = mutation.authoredSchema
        baseVersion = mutation.baseVersion
        clientVersion = mutation.clientVersion
        status = mutation.status.rawValue
        authoredFields = mutation.authoredFields.map(RetainedField.init)
    }
}

private struct RetainedField: Encodable {
    let fieldID: String
    let logicalType: String
    let value: AnyCodable

    enum CodingKeys: String, CodingKey {
        case fieldID = "field_id"
        case logicalType = "logical_type"
        case value
    }

    init(_ field: AuthoredMutationField) {
        fieldID = field.fieldID
        logicalType = field.logicalType
        value = field.value
    }
}

private struct RetainedRejection: Encodable {
    let localOrder: Int64
    let mutation: Mutation
    let rejection: RejectedMutation

    enum CodingKeys: String, CodingKey {
        case localOrder = "local_order"
        case mutation
        case rejection
    }

    init(_ inspection: RejectedMutationInspection) {
        localOrder = inspection.localOrder
        mutation = inspection.mutation
        rejection = inspection.rejection
    }
}

private struct EventRecord: Encodable {
    let type: String
    let status: String?
    let mutationID: String?
    let tableID: String?
    let rejectionCode: String?
    let sourceSchema: SchemaRef?
    let targetSchema: SchemaRef?
    let schemaAction: String?
    let scopeID: String?
    let rebuildID: String?
    let failure: SyncFailure?

    enum CodingKeys: String, CodingKey {
        case type
        case status
        case mutationID = "mutation_id"
        case tableID = "table_id"
        case rejectionCode = "rejection_code"
        case sourceSchema = "source_schema"
        case targetSchema = "target_schema"
        case schemaAction = "schema_action"
        case scopeID = "scope_id"
        case rebuildID = "rebuild_id"
        case failure
    }

    init(_ event: SyncEvent) {
        var type = ""
        var status: String?
        var mutationID: String?
        var tableID: String?
        var rejectionCode: String?
        var sourceSchema: SchemaRef?
        var targetSchema: SchemaRef?
        var schemaAction: String?
        var scopeID: String?
        var rebuildID: String?
        var failure: SyncFailure?
        switch event {
        case .stateChanged(let change):
            type = "state_changed"
            status = change.to.rawValue
        case .backoff:
            type = "backoff"
        case .schemaApplying(let schema):
            type = "schema_applying"
            sourceSchema = schema.source
            targetSchema = schema.target
            schemaAction = schema.action.rawValue
        case .schemaApplied(let schema):
            type = "schema_applied"
            sourceSchema = schema.source
            targetSchema = schema.target
            schemaAction = schema.action.rawValue
        case .mutationAccepted(let mutation):
            type = "mutation_accepted"
            mutationID = mutation.mutationID
            tableID = mutation.tableID
        case .mutationRejected(let mutation):
            type = "mutation_rejected"
            mutationID = mutation.mutationID
            tableID = mutation.tableID
            rejectionCode = mutation.rejectionCode?.rawValue
        case .rebuildRequested(let rebuild):
            type = "rebuild_requested"
            scopeID = rebuild.scopeID
            rebuildID = rebuild.rebuildID
        case .rebuildCompleted(let rebuild):
            type = "rebuild_completed"
            scopeID = rebuild.scopeID
            rebuildID = rebuild.rebuildID
        case .failure(let value):
            type = "failure"
            failure = value
        }
        self.type = type
        self.status = status
        self.mutationID = mutationID
        self.tableID = tableID
        self.rejectionCode = rejectionCode
        self.sourceSchema = sourceSchema
        self.targetSchema = targetSchema
        self.schemaAction = schemaAction
        self.scopeID = scopeID
        self.rebuildID = rebuildID
        self.failure = failure
    }
}

private final class EventStore: @unchecked Sendable {
    private static let maximumEvents = 512
    private let lock = NSLock()
    private var values: [EventRecord] = []

    func append(_ event: SyncEvent) {
        lock.lock()
        values.append(EventRecord(event))
        if values.count > Self.maximumEvents {
            values.removeFirst(values.count - Self.maximumEvents)
        }
        lock.unlock()
    }

    func snapshot() -> [EventRecord] {
        lock.lock()
        defer { lock.unlock() }
        return values
    }
}

private final class Runner: @unchecked Sendable {
    private static let transportPauseTimeout: TimeInterval = 10
    private static let defaultTransportCapacity = 512
    private static let maximumTransportCapacity = 512
    private static let maximumRows = 256
    private static let maximumSelectors = 128
    private static let maximumFields = 256

    private var client: SynchroClient?
    private var eventSubscription: (any Cancellable)?
    private var inFlightCalls: [String: Task<Void, Error>] = [:]
    private let events = EventStore()
    private var transportObservations: TransportObservationCollector?

    func execute(_ command: RunnerCommand) async throws -> RunnerResult {
        guard command.schemaVersion == 1 else {
            throw RunnerError.invalidCommand
        }
        var result: RunnerResult
        switch command.operation {
        case "open":
            result = try open(command)
        case "local-action":
            result = try localAction(command)
        case "begin-call":
            result = try beginCall(command)
        case "await-call":
            result = try await awaitCall(command)
        case "cancel-call":
            result = try await cancelCall(command)
        case "lifecycle":
            result = try await lifecycle(command)
        case "arm-transport-pause":
            result = try armTransportPause(command)
        case "await-transport-pause":
            result = try await awaitTransportPause(command)
        case "resume-transport-pause":
            result = try resumeTransportPause(command)
        case "capture":
            result = try capture(command)
        default:
            throw RunnerError.invalidCommand
        }
        result.transportObservations = transportObservations.map { RunnerTransportObservationSnapshot($0.snapshot()) }
        return result
    }

    func close() async {
        transportObservations?.cancelPauseBarrier()
        let tasks = Array(inFlightCalls.values)
        for task in tasks {
            task.cancel()
        }
        for task in tasks {
            _ = try? await task.value
        }
        inFlightCalls.removeAll()
        eventSubscription?.cancel()
        eventSubscription = nil
        try? await client?.close()
        client = nil
        transportObservations = nil
    }

    private func open(_ command: RunnerCommand) throws -> RunnerResult {
        guard commandOnly(command, allowed: [
            .schemaVersion, .operation, .databasePath, .serverURL, .authToken, .clientID,
            .seedDatabasePath, .platform, .appVersion, .pullPageSize, .pushBatchSize,
            .transportCapacity
        ]),
        client == nil,
        let databasePath = command.databasePath,
        let server = command.serverURL,
        let serverURL = URL(string: server),
        let authToken = command.authToken,
        let clientID = command.clientID,
        !databasePath.isEmpty,
        databasePath.count <= 4096,
        !server.isEmpty,
        !authToken.isEmpty,
        !clientID.isEmpty,
        command.seedDatabasePath.map({ !$0.isEmpty && $0.count <= 4096 }) ?? true,
        command.pullPageSize.map({ (1...1000).contains($0) }) ?? true,
        command.pushBatchSize.map({ (1...1000).contains($0) }) ?? true,
        command.transportCapacity.map({ (1...Self.maximumTransportCapacity).contains($0) }) ?? true else {
            throw RunnerError.invalidCommand
        }
        let platform = command.platform ?? "macos"
        let appVersion = command.appVersion ?? "0.3.0"
        guard !platform.isEmpty, platform.count <= 128,
              !appVersion.isEmpty, appVersion.count <= 128 else {
            throw RunnerError.invalidCommand
        }
        let collector = TransportObservationCollector(
            capacity: command.transportCapacity ?? Self.defaultTransportCapacity
        )
        let config = SynchroConfig(
            dbPath: databasePath,
            serverURL: serverURL,
            authProvider: { authToken },
            clientID: clientID,
            platform: platform,
            appVersion: appVersion,
            syncInterval: 3600,
            pushDebounce: 3600,
            pullPageSize: command.pullPageSize ?? 100,
            pushBatchSize: command.pushBatchSize ?? 100,
            seedDatabasePath: command.seedDatabasePath,
            transportObservationCollector: collector
        )
        let client = try SynchroClient(config: config)
        self.client = client
        transportObservations = collector
        eventSubscription = client.onSyncEvent { [events] event in
            events.append(event)
        }
        return RunnerResult(status: client.getSyncStatus().rawValue)
    }

    // localTableDefinition reports the stored definition of one local table. A
    // constraint violation names the column but not the schema that declared
    // it, so a failing local write reports the definition alongside it.
    func localTableDefinition(_ table: String) -> String? {
        guard let client = try? requireClient() else { return nil }
        return try? client.readTransaction { database in
            try String.fetchOne(
                database,
                sql: "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
                arguments: [table]
            )
        }
    }

    private func localAction(_ command: RunnerCommand) throws -> RunnerResult {
        guard commandOnly(command, allowed: [.schemaVersion, .operation, .localAction]),
              let payload = command.localAction else {
            throw RunnerError.invalidCommand
        }
        let client = try requireClient()
        guard payload.fields.count <= Self.maximumFields,
              !isReservedTable(payload.tableName) else {
            throw RunnerError.invalidCommand
        }
        let table = try quoteIdentifier(payload.tableName)
        let primaryKeyField = try quoteIdentifier(payload.primaryKeyField)
        let primaryKey = payload.primaryKey.databaseValue()
        var fields = payload.fields
        if let suppliedPrimaryKey = fields.removeValue(forKey: payload.primaryKeyField) {
            guard valuesEqual(suppliedPrimaryKey, payload.primaryKey) else {
                throw RunnerError.invalidCommand
            }
        }
        // The authored columns bound the capture context, so an injected
        // runtime support value stays out of the captured payload.
        guard Set(payload.authoredColumns).count == payload.authoredColumns.count,
              payload.authoredColumns.allSatisfy({ fields[$0] != nil }) else {
            throw RunnerError.invalidCommand
        }
        let result: ExecResult
        switch payload.operation {
        case .insert:
            let orderedFields = try fields.keys.sorted().map { key in
                (try quoteIdentifier(key), fields[key]!)
            }
            let columns = ([primaryKeyField] + orderedFields.map { $0.0 }).joined(separator: ", ")
            let placeholders = Array(repeating: "?", count: orderedFields.count + 1).joined(separator: ", ")
            let values = [primaryKey] + orderedFields.map { $0.1.databaseValue() }
            result = try client.authoredWriteTransaction(
                tableName: payload.tableName,
                operation: "insert",
                columnNames: payload.authoredColumns
            ) { transaction in
                try transaction.execute(
                    "INSERT INTO \(table) (\(columns)) VALUES (\(placeholders))",
                    params: values
                )
            }
        case .update:
            guard !fields.isEmpty else {
                throw RunnerError.invalidCommand
            }
            let orderedFields = try fields.keys.sorted().map { key in
                (try quoteIdentifier(key), fields[key]!)
            }
            let assignments = orderedFields.map { "\($0.0) = ?" }.joined(separator: ", ")
            let values = orderedFields.map { $0.1.databaseValue() } + [primaryKey]
            result = try client.authoredWriteTransaction(
                tableName: payload.tableName,
                operation: "update",
                columnNames: payload.authoredColumns
            ) { transaction in
                try transaction.execute(
                    "UPDATE \(table) SET \(assignments) WHERE \(primaryKeyField) = ?",
                    params: values
                )
            }
        case .delete:
            guard fields.isEmpty else {
                throw RunnerError.invalidCommand
            }
            result = try client.authoredWriteTransaction(
                tableName: payload.tableName,
                operation: "delete",
                columnNames: payload.authoredColumns
            ) { transaction in
                try transaction.execute(
                    "DELETE FROM \(table) WHERE \(primaryKeyField) = ?",
                    params: [primaryKey]
                )
            }
        }
        guard result.rowsAffected == 1 else {
            throw RunnerError.unexpectedResult
        }
        return RunnerResult(
            status: client.getSyncStatus().rawValue,
            rowsAffected: result.rowsAffected
        )
    }

    private func lifecycle(_ command: RunnerCommand) async throws -> RunnerResult {
        guard commandOnly(command, allowed: [.schemaVersion, .operation, .lifecycleOperation]),
              let operation = command.lifecycleOperation else {
            throw RunnerError.invalidCommand
        }
        let client = try requireClient()
        switch operation {
        case "stop":
            await client.stop()
        case "enter-background":
            await client.enterBackground()
        case "enter-foreground":
            try await client.enterForeground()
        default:
            throw RunnerError.invalidCommand
        }
        return RunnerResult(status: client.getSyncStatus().rawValue)
    }

    private func armTransportPause(_ command: RunnerCommand) throws -> RunnerResult {
        guard commandOnly(command, allowed: [.schemaVersion, .operation, .transportOperation]),
              let operation = command.transportOperation,
              let operationClass = TransportOperationClass(rawValue: operation) else {
            throw RunnerError.invalidCommand
        }
        let client = try requireClient()
        try requireTransportObservations().armPause(for: operationClass)
        return RunnerResult(status: client.getSyncStatus().rawValue)
    }

    private func awaitTransportPause(_ command: RunnerCommand) async throws -> RunnerResult {
        guard commandOnly(command, allowed: [.schemaVersion, .operation, .transportOperation]),
              let operation = command.transportOperation,
              let operationClass = TransportOperationClass(rawValue: operation) else {
            throw RunnerError.invalidCommand
        }
        let client = try requireClient()
        try await requireTransportObservations().awaitPause(
            for: operationClass,
            timeout: Self.transportPauseTimeout
        )
        return RunnerResult(status: client.getSyncStatus().rawValue)
    }

    private func resumeTransportPause(_ command: RunnerCommand) throws -> RunnerResult {
        guard commandOnly(command, allowed: [.schemaVersion, .operation]) else {
            throw RunnerError.invalidCommand
        }
        let client = try requireClient()
        try requireTransportObservations().resumePause()
        return RunnerResult(status: client.getSyncStatus().rawValue)
    }

    private func capture(_ command: RunnerCommand) throws -> RunnerResult {
        let client = try requireClient()
        let inspection = SynchroInspection(client: client)
        guard commandOnly(command, allowed: [.schemaVersion, .operation, .rowSelectors]),
              command.rowSelectors.map({ $0.count <= Self.maximumSelectors }) ?? true else {
            throw RunnerError.invalidCommand
        }
        let capture = try inspection.captureState(maximumRecords: maximumBoundedRecords)
        let scopeStates = capture.scopeStatesTruncated ? nil : capture.scopeStates
        let scopeRows = capture.scopeRowsTruncated ? nil : capture.scopeRows
        let rebuildAttempts = capture.rebuildAttemptsTruncated ? nil : capture.rebuildAttempts
        let rebuildReceipts = capture.rebuildReceiptsTruncated ? nil : capture.rebuildReceipts
        let counts = capture
        let retainedMutations: [RetainedMutation]? = counts.mutationLedgerCount <= maximumBoundedRecords ? try retainedMutationRecords(client.inspectRetainedMutations()) : nil
        let rejectedMutations: [RetainedRejection]? = counts.rejectedMutationCount <= maximumBoundedRecords ? try bounded(
            client.inspectRejectedMutations().map(RetainedRejection.init),
            subject: "rejected mutations"
        ) : nil
        var capturedApplicationRows: [[String: AnyCodable]] = []
        var captureValueBytes = 0
        let scopedTables = Set<String>((scopeRows ?? []).map(\.tableName))
        if counts.applicationRowCount <= Self.maximumRows {
            for selector in command.rowSelectors ?? [] {
                if scopedTables.contains(selector.tableName) {
                    continue
                }
                guard !isReservedTable(selector.tableName) else {
                    throw RunnerError.invalidCommand
                }
                let table = try quoteIdentifier(selector.tableName)
                let field = try quoteIdentifier(selector.primaryKeyField)
                let rows: [Row]
                do {
                    rows = try client.query(
                        "SELECT * FROM \(table) WHERE \(field) = ?",
                        params: [selector.primaryKey.databaseValue()]
                    )
                } catch {
                    throw RunnerError.captureQuery
                }
                guard rows.count <= 1 else {
                    throw RunnerError.captureRowCardinality
                }
                if let row = rows.first {
                    capturedApplicationRows.append(try rowObject(row, valueBytes: &captureValueBytes))
                }
            }
            for tableName in scopedTables {
                guard !isReservedTable(tableName) else { throw RunnerError.invalidCommand }
                let table = try quoteIdentifier(tableName)
                let rows: [Row]
                do {
                    rows = try client.query("SELECT * FROM \(table)")
                } catch {
                    throw RunnerError.captureQuery
                }
                for row in rows {
                    capturedApplicationRows.append(try rowObject(row, valueBytes: &captureValueBytes))
                }
            }
        }
        let metadataRecords = counts.rowMetadataTruncated
            ? nil
            : counts.rowMetadata.map(RowMetadataRecord.init)
        guard capturedApplicationRows.count <= Self.maximumRows else {
            throw RunnerError.outputLimit("captured application rows \(capturedApplicationRows.count) exceeds \(Self.maximumRows)")
        }
        do {
            let scopeStateRecords: [ScopeStateRecord]? = try scopeStates.map { try bounded($0.map(ScopeStateRecord.init), subject: "scope states") }
            let scopeRowRecords: [ScopeRowRecord]? = try scopeRows.map { try bounded($0.map(ScopeRowRecord.init), subject: "scope rows") }
            let rebuildAttemptRecords: [RebuildAttemptRecord]? = try rebuildAttempts.map { try bounded($0.map(RebuildAttemptRecord.init), subject: "rebuild attempts") }
            let rebuildReceiptRecords: [RebuildReceiptRecord]? = try rebuildReceipts.map { try bounded($0.map(RebuildReceiptRecord.init), subject: "rebuild receipts") }
            return RunnerResult(
                status: client.getSyncStatus().rawValue,
                pendingChangeCount: try client.pendingChangeCount(),
                applicationRowCount: counts.applicationRowCount,
                mutationLedgerCount: counts.mutationLedgerCount,
                mutationOutcomeCount: counts.mutationOutcomeCount,
                sealedBatchCount: counts.sealedBatchCount,
                rejectedMutationCount: counts.rejectedMutationCount,
                scopeStateCount: counts.scopeStateCount,
                scopeRowCount: counts.scopeRowCount,
                provenanceCount: counts.provenanceCount,
                rowMetadataCount: counts.rowMetadataCount,
                rebuildAttemptCount: counts.rebuildAttemptCount,
                rebuildReceiptCount: counts.rebuildReceiptCount,
                schema: counts.schema,
                applicationRows: counts.applicationRowCount <= Self.maximumRows ? capturedApplicationRows : nil,
                retainedMutations: retainedMutations,
                rejectedMutations: rejectedMutations,
                scopeStates: scopeStateRecords,
                scopeRows: scopeRowRecords,
                rowMetadata: metadataRecords?.first,
                rowMetadataRecords: metadataRecords,
                rebuildAttempts: rebuildAttemptRecords,
                rebuildReceipts: rebuildReceiptRecords,
                scopeStatesTruncated: counts.scopeStatesTruncated,
                scopeRowsTruncated: counts.scopeRowsTruncated,
                rebuildAttemptsTruncated: counts.rebuildAttemptsTruncated,
                rebuildReceiptsTruncated: counts.rebuildReceiptsTruncated,
                rowMetadataTruncated: counts.rowMetadataTruncated,
                captureOverflowed: counts.overflowed,
                provenanceMaintenanceWorkCursor: counts.provenanceMaintenanceWorkCursor,
                events: events.snapshot(),
                failure: try client.getBlockingFailure()
            )
        } catch {
            throw RunnerError.captureInspection
        }
    }

    private func requireClient() throws -> SynchroClient {
        guard let client else {
            throw RunnerError.invalidCommand
        }
        return client
    }

    private func requireTransportObservations() throws -> TransportObservationCollector {
        guard let transportObservations else {
            throw RunnerError.invalidCommand
        }
        return transportObservations
    }

    private func beginCall(_ command: RunnerCommand) throws -> RunnerResult {
        guard commandOnly(command, allowed: [.schemaVersion, .operation, .callID, .method]),
              let callID = command.callID,
              isValidCallID(callID),
              let method = command.method,
              ["start", "sync-now", "retry-after-error", "reset-schema-and-start"].contains(method) else {
            throw RunnerError.invalidCommand
        }
        let client = try requireClient()
        guard inFlightCalls.isEmpty else {
            throw RunnerError.invalidCommand
        }
        inFlightCalls[callID] = makeCallTask(client: client, method: method)
        return RunnerResult(callID: callID, state: "in_flight")
    }

    private func awaitCall(_ command: RunnerCommand) async throws -> RunnerResult {
        guard commandOnly(command, allowed: [.schemaVersion, .operation, .callID]),
              let callID = command.callID,
              isValidCallID(callID) else {
            throw RunnerError.invalidCommand
        }
        guard let task = inFlightCalls.removeValue(forKey: callID) else {
            throw RunnerError.invalidCommand
        }
        var callErrorCategory: String?
        do {
            _ = try await task.value
        } catch {
            callErrorCategory = rawCallErrorCategory(error)
        }
        return try callCompletionResult(
            callID: callID,
            callErrorCategory: callErrorCategory
        )
    }

    private func cancelCall(_ command: RunnerCommand) async throws -> RunnerResult {
        guard commandOnly(command, allowed: [.schemaVersion, .operation, .callID]),
              let callID = command.callID,
              isValidCallID(callID),
              let task = inFlightCalls.removeValue(forKey: callID) else {
            throw RunnerError.invalidCommand
        }
        // Release a completed transport pause before cancelling the public task.
        try? requireTransportObservations().resumePause()
        task.cancel()
        _ = try? await task.value
        return RunnerResult(status: try requireClient().getSyncStatus().rawValue)
    }

    private func makeCallTask(client: SynchroClient, method: String) -> Task<Void, Error> {
        Task { [self, client] in
            try await performPublicMethod(client: client, method: method)
        }
    }

    private func performPublicMethod(client: SynchroClient, method: String) async throws {
        switch method {
        case "start":
            try await client.start()
        case "sync-now":
            try await client.syncNow()
        case "retry-after-error":
            try await client.retryAfterError()
        case "reset-schema-and-start":
            try await client.resetSchemaAndStart()
        default:
            throw RunnerError.invalidCommand
        }
    }

    private func callCompletionResult(
        callID: String,
        callErrorCategory: String?
    ) throws -> RunnerResult {
        let client = try requireClient()
        if let callErrorCategory {
            return RunnerResult(
                callID: callID,
                state: "completed",
                completion: "error",
                callErrorCategory: callErrorCategory,
                status: client.getSyncStatus().rawValue
            )
        }
        let failure = try client.getBlockingFailure()
        let status = client.getSyncStatus()
        let completion: String = {
            if status == .backoff {
                return "blocked"
            }
            if let failure {
                return failure.recoveryAction == .none ? "error" : "blocked"
            }
            if status == .error {
                return "error"
            }
            return "idle"
        }()
        return RunnerResult(
            callID: callID,
            state: "completed",
            completion: completion,
            callErrorCategory: callErrorCategory,
            status: status.rawValue
        )
    }
}

private func commandOnly(
    _ command: RunnerCommand,
    allowed: [RunnerCommand.CodingKeys]
) -> Bool {
    command.containsOnly(allowed)
}

private func valuesEqual(_ left: RunnerJSONValue, _ right: RunnerJSONValue) -> Bool {
    switch (left, right) {
    case (.null, .null):
        return true
    case let (.string(left), .string(right)):
        return left == right
    case let (.boolean(left), .boolean(right)):
        return left == right
    case let (.integer(left), .integer(right)):
        return left == right
    case let (.double(left), .double(right)):
        return left == right
    case let (.bytes(left), .bytes(right)):
        return left == right
    default:
        return false
    }
}

private func decodeBase64URL(_ value: String) -> Data? {
    guard value.utf8.allSatisfy({
        ($0 >= 65 && $0 <= 90)
            || ($0 >= 97 && $0 <= 122)
            || ($0 >= 48 && $0 <= 57)
            || $0 == 45
            || $0 == 95
    }),
    value.count % 4 != 1 else {
        return nil
    }
    let padded = value
        .replacingOccurrences(of: "-", with: "+")
        .replacingOccurrences(of: "_", with: "/")
        + String(repeating: "=", count: (4 - value.count % 4) % 4)
    guard let data = Data(base64Encoded: padded) else {
        return nil
    }
    return data.base64URLEncodedString == value ? data : nil
}

private extension Data {
    var base64URLEncodedString: String {
        base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }
}

private let maximumBoundedRecords = 512

private func bounded<T>(_ values: [T], subject: String, limit: Int = maximumBoundedRecords) throws -> [T] {
    guard values.count <= limit else {
        throw RunnerError.outputLimit("\(subject) count \(values.count) exceeds \(limit)")
    }
    return values
}

private func retainedMutationRecords(
    _ values: [PendingMutationInspection]
) throws -> [RetainedMutation] {
    guard values.allSatisfy({ $0.authoredFields.count <= maximumBoundedRecords }) else {
        throw RunnerError.outputLimit("retained mutation authored fields exceed \(maximumBoundedRecords)")
    }
    return try bounded(values.map(RetainedMutation.init), subject: "retained mutations")
}

private func isReservedTable(_ value: String) -> Bool {
    let normalized = value.lowercased(with: Locale(identifier: "en_US_POSIX"))
    return normalized.hasPrefix("_synchro_")
        || normalized == "grdb_migrations"
        || normalized == "_grdb_migrations"
        || normalized.hasPrefix("sqlite_")
}

private func rawCallErrorCategory(_ error: Error) -> String {
    switch error {
    case is CancellationError:
        return "cancelled"
    case is RunnerError:
        return "runner_error"
    default:
        break
    }
    guard let error = error as? SynchroError else {
        return "unknown_error"
    }
    switch error {
    case .notConnected:
        return "not_connected"
    case .schemaNotLoaded:
        return "schema_not_loaded"
    case .tableNotSynced(_):
        return "table_not_synced"
    case .upgradeRequired(_, _):
        return "upgrade_required"
    case .schemaMismatch(_, _):
        return "schema_mismatch"
    case .pushRejected(_):
        return "push_rejected"
    case .networkError(_):
        return "network_error"
    case .serverError(_, _):
        return "server_error"
    case .protocolError(_, _, _):
        return "protocol_error"
    case .databaseError(_):
        return "database_error"
    case .invalidResponse(_):
        return "invalid_response"
    case .blocked(_):
        return "blocked"
    case .unsupportedSchema(_):
        return "unsupported_schema"
    case .invalidStateTransition(_, _):
        return "invalid_state_transition"
    case .alreadyStarted:
        return "already_started"
    case .notStarted:
        return "not_started"
    }
}

private func isValidCallID(_ value: String) -> Bool {
    let bytes = Array(value.utf8)
    guard bytes.count <= 128,
          let first = bytes.first,
          (97...122).contains(first) else {
        return false
    }
    return bytes.dropFirst().allSatisfy { byte in
        (97...122).contains(byte)
            || (48...57).contains(byte)
            || byte == 45
            || byte == 95
    }
}

private enum RunnerError: Error {
    case invalidCommand
    case inputLimit
    case unexpectedResult
    case captureQuery
    case captureRowCardinality
    case captureInspection
    case outputLimit(String)

    var code: String {
        switch self {
        case .invalidCommand:
            return "invalid_command"
        case .inputLimit:
            return "invalid_command"
        case .unexpectedResult:
            return "execution_failed"
        case .captureQuery:
            return "capture_query_failed"
        case .captureRowCardinality:
            return "capture_row_cardinality"
        case .captureInspection:
            return "capture_inspection_failed"
        case .outputLimit:
            return "execution_failed"
        }
    }

    /// The protocol code stays bounded, so the tripped bound reaches the
    /// harness through stderr instead of the response.
    var detail: String {
        switch self {
        case .outputLimit(let subject):
            return "runner output limit: \(subject)"
        default:
            return "runner error: \(code)"
        }
    }
}

private final class BoundedLineReader {
    private static let readChunkBytes = 4096

    private let input: FileHandle
    private var buffer = Data()

    init(input: FileHandle = .standardInput) {
        self.input = input
    }

    func nextLine() throws -> String? {
        while true {
            if let newline = buffer.firstIndex(of: 0x0a) {
                let line = buffer.prefix(upTo: newline)
                buffer.removeSubrange(...newline)
                guard line.count <= maximumRunnerLineBytes else {
                    throw RunnerError.inputLimit
                }
                let content = line.last == 0x0d ? line.dropLast() : line[...]
                guard let value = String(data: Data(content), encoding: .utf8) else {
                    throw RunnerError.invalidCommand
                }
                return value
            }

            guard buffer.count <= maximumRunnerLineBytes else {
                buffer.removeAll(keepingCapacity: true)
                while let chunk = try readChunk() {
                    if let newline = chunk.firstIndex(of: 0x0a) {
                        buffer.append(contentsOf: chunk.suffix(from: chunk.index(after: newline)))
                        break
                    }
                }
                throw RunnerError.inputLimit
            }

            guard let chunk = try readChunk() else {
                guard !buffer.isEmpty else { return nil }
                let line = buffer
                buffer.removeAll(keepingCapacity: true)
                guard line.count <= maximumRunnerLineBytes,
                      let value = String(data: line, encoding: .utf8) else {
                    throw RunnerError.invalidCommand
                }
                return value
            }
            buffer.append(chunk)
        }
    }

    private func readChunk() throws -> Data? {
        var bytes = [UInt8](repeating: 0, count: Self.readChunkBytes)
        while true {
            let count = bytes.withUnsafeMutableBytes { buffer in
                Darwin.read(input.fileDescriptor, buffer.baseAddress, buffer.count)
            }
            if count > 0 {
                return Data(bytes.prefix(count))
            }
            if count == 0 {
                return nil
            }
            if errno != EINTR {
                throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
            }
        }
    }
}

private func quoteIdentifier(_ value: String) throws -> String {
    guard !value.isEmpty,
          value.count <= 128,
          value.first?.isLetter == true || value.first == "_",
          value.allSatisfy({ $0.isLetter || $0.isNumber || $0 == "_" }) else {
        throw RunnerError.invalidCommand
    }
    return "\"\(value)\""
}
 
private extension RunnerJSONValue {
    func recordID() throws -> String {
        switch self {
        case .null:
            throw RunnerError.invalidCommand
        case .string(let value):
            return value
        case .boolean(let value):
            return value ? "1" : "0"
        case .integer(let value):
            return String(value)
        case .double(let value):
            guard value.isFinite else {
                throw RunnerError.invalidCommand
            }
            return String(value)
        case .bytes(let value):
            return value.base64URLEncodedString
        }
    }
}

private func rowObject(_ row: Row, valueBytes: inout Int) throws -> [String: AnyCodable] {
    guard row.columnNames.count <= 256 else {
        throw RunnerError.outputLimit("captured row columns \(row.columnNames.count) exceeds 256")
    }
    var result: [String: AnyCodable] = [:]
    for column in row.columnNames {
        let value: DatabaseValue = row[column]
        let output: Any
        switch value.storage {
        case .null:
            output = NSNull()
        case .int64(let value):
            output = value
        case .double(let value):
            guard value.isFinite else {
                throw RunnerError.captureInspection
            }
            output = value
        case .string(let value):
            guard value.utf8.count <= 1_048_576,
                  valueBytes <= maximumCaptureValueBytes - value.utf8.count else {
                throw RunnerError.outputLimit("captured text value bytes \(valueBytes + value.utf8.count) exceeds \(maximumCaptureValueBytes)")
            }
            valueBytes += value.utf8.count
            output = value
        case .blob(let value):
            guard value.count <= 1_048_576,
                  valueBytes <= maximumCaptureValueBytes - value.count else {
                throw RunnerError.outputLimit("captured blob value bytes \(valueBytes + value.count) exceeds \(maximumCaptureValueBytes)")
            }
            valueBytes += value.count
            output = value.base64URLEncodedString
        }
        result[column] = AnyCodable(output)
    }
    return result
}

@main
private enum Main {
    static func main() async {
        let runner = Runner()
        let decoder = JSONDecoder()
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let input = BoundedLineReader()

        while true {
            let response: RunnerResponse
            var attempted: RunnerCommand?
            do {
                guard let line = try input.nextLine() else {
                    break
                }
                guard let data = line.data(using: .utf8) else {
                    throw RunnerError.invalidCommand
                }
                do {
                    try Integrity.validateCanonicalWireJSON(data)
                } catch {
                    throw RunnerError.invalidCommand
                }
                let command = try decoder.decode(RunnerCommand.self, from: data)
                attempted = command
                let result = try await runner.execute(command)
                response = RunnerResponse(outcome: "passed", result: result, errorCode: nil)
            } catch is DecodingError {
                response = RunnerResponse(outcome: "error", result: nil, errorCode: "invalid_command")
            } catch let error as RunnerError {
                FileHandle.standardError.write(Data("\(error.detail)\n".utf8))
                response = RunnerResponse(outcome: "error", result: nil, errorCode: error.code)
            } catch {
                // The protocol response carries a bounded code, so the
                // underlying failure reaches the harness through stderr. A
                // local write also reports the table definition it violated,
                // because the constraint alone cannot name the schema that
                // declared it.
                var report = "runner execution failed: \(error)\n"
                if let table = attempted?.localAction?.tableName,
                   let definition = await runner.localTableDefinition(table) {
                    report += "local table definition: \(definition)\n"
                }
                FileHandle.standardError.write(Data(report.utf8))
                response = RunnerResponse(outcome: "error", result: nil, errorCode: "execution_failed")
            }
            let data: Data
            do {
                let encoded = try encoder.encode(response)
                guard encoded.count <= maximumRunnerLineBytes - 1 else {
                    let sections = response.result?.sectionSizeReport(encoder) ?? "no result sections"
                    throw RunnerError.outputLimit(
                        "encoded response bytes \(encoded.count) exceeds \(maximumRunnerLineBytes - 1); sections: \(sections)"
                    )
                }
                data = encoded
            } catch {
                // This fallback replaced a response the runner could not emit,
                // so without this report the harness sees a bounded code and
                // no cause at all.
                let detail = (error as? RunnerError)?.detail ?? "runner response encoding failed: \(error)"
                FileHandle.standardError.write(Data("\(detail)\n".utf8))
                data = (try? encoder.encode(
                    RunnerResponse(outcome: "error", result: nil, errorCode: "execution_failed")
                )) ?? Data("{\"error_code\":\"execution_failed\",\"outcome\":\"error\",\"result\":null,\"schema_version\":1}".utf8)
            }
            if let output = String(data: data, encoding: .utf8) {
                print(output)
                fflush(stdout)
            }
        }
        await runner.close()
    }
}
