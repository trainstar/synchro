import Foundation

public enum ContractViolation: Error, Equatable {
    case emptyScopeID
    case duplicateAddedScope(String)
    case duplicateRemovedScope(String)
    case conflictingScopeAssignment(String)
    case schemaDefinitionMismatch(action: SchemaAction, hasSchemaDefinition: Bool)
    case emptyTableName
    case duplicateTableName(String)
    case missingPrimaryKey(tableName: String)
    case missingUpdatedAtColumn(tableName: String)
    case missingDeletedAtColumn(tableName: String)
    case missingColumns(tableName: String)
    case emptyColumnName(tableName: String)
    case duplicateColumnName(tableName: String, columnName: String)
    case unknownPrimaryKeyColumn(tableName: String, columnName: String)
    case unknownUpdatedAtColumn(tableName: String, columnName: String)
    case unknownDeletedAtColumn(tableName: String, columnName: String)
    case emptyIndexName(tableName: String)
    case duplicateIndexName(tableName: String, indexName: String)
    case unknownIndexColumn(tableName: String, indexName: String, columnName: String)
    case requiredChecksumsMissing
    case finalRebuildCursorMissing
    case finalRebuildChecksumMissing
    case partialRebuildHasFinalCursor
    case partialRebuildHasChecksum
}

public enum Operation: String, Codable, Sendable {
    case insert
    case upsert
    case update
    case delete
}

public enum SchemaAction: String, Codable, Sendable {
    case none
    case fetch
    case replace = "replace"
    case rebuildLocal = "rebuild_local"
    case unsupported

    public var requiresSchemaDefinition: Bool {
        self == .replace || self == .rebuildLocal
    }

    public var requiresLocalRebuild: Bool {
        self == .rebuildLocal
    }

    public var isCompatible: Bool {
        self != .unsupported
    }
}

public enum ChecksumMode: String, Codable, Sendable {
    case none
    case requested
    case required
}

public enum MutationStatus: String, Codable, Sendable {
    case applied
    case conflict
    case rejectedTerminal = "rejected_terminal"
    case rejectedRetryable = "rejected_retryable"
}

public enum MutationRejectionCode: String, Codable, Sendable {
    case versionConflict = "version_conflict"
    case policyRejected = "policy_rejected"
    case validationFailed = "validation_failed"
    case tableNotSynced = "table_not_synced"
    case unknownScopeEffect = "unknown_scope_effect"
    case serverRetryable = "server_retryable"
}

public enum ProtocolErrorCode: String, Codable, Sendable {
    case invalidRequest = "invalid_request"
    case upgradeRequired = "upgrade_required"
    case authRequired = "auth_required"
    case schemaMismatch = "schema_mismatch"
    case retryLater = "retry_later"
    case temporaryUnavailable = "temporary_unavailable"
}

public enum CompositionClass: String, Codable, Sendable {
    case singleScope = "single_scope"
    case multiScope = "multi_scope"
}

public struct SchemaRef: Codable, Sendable, Equatable {
    public var version: Int64
    public var hash: String
}

public struct ScopeCursorRef: Codable, Sendable, Equatable {
    public var cursor: String?
}

public struct ScopeAssignment: Codable, Sendable, Equatable {
    public var id: String
    public var cursor: String?
}

public struct ScopeAssignmentDelta: Codable, Sendable, Equatable {
    public var add: [ScopeAssignment]
    public var remove: [String]

    public func validate() throws {
        var added = Set<String>()
        for scope in add {
            if scope.id.isEmpty {
                throw ContractViolation.emptyScopeID
            }
            if !added.insert(scope.id).inserted {
                throw ContractViolation.duplicateAddedScope(scope.id)
            }
        }

        var removed = Set<String>()
        for scopeID in remove {
            if scopeID.isEmpty {
                throw ContractViolation.emptyScopeID
            }
            if !removed.insert(scopeID).inserted {
                throw ContractViolation.duplicateRemovedScope(scopeID)
            }
            if added.contains(scopeID) {
                throw ContractViolation.conflictingScopeAssignment(scopeID)
            }
        }
    }
}

public struct SchemaDescriptor: Codable, Sendable, Equatable {
    public var version: Int64
    public var hash: String
    public var action: SchemaAction
}

public struct ColumnSchema: Codable, Sendable, Equatable {
    public var name: String
    public var type: String
    public var nullable: Bool
}

public struct IndexSchema: Codable, Sendable, Equatable {
    public var name: String
    public var columns: [String]
}

public struct TableSchema: Codable, Sendable, Equatable {
    public var name: String
    public var primaryKey: [String]?
    public var updatedAtColumn: String?
    public var deletedAtColumn: String?
    public var composition: CompositionClass?
    public var columns: [ColumnSchema]?
    public var indexes: [IndexSchema]?

    enum CodingKeys: String, CodingKey {
        case name
        case primaryKey = "primary_key"
        case updatedAtColumn = "updated_at_column"
        case deletedAtColumn = "deleted_at_column"
        case composition
        case columns
        case indexes
    }
}

public struct SchemaManifest: Codable, Sendable, Equatable {
    public var tables: [TableSchema]

    public func validate() throws {
        var tableNames = Set<String>()
        for table in tables {
            if table.name.isEmpty {
                throw ContractViolation.emptyTableName
            }
            if !tableNames.insert(table.name).inserted {
                throw ContractViolation.duplicateTableName(table.name)
            }

            var columnNames = Set<String>()
            if let columns = table.columns {
                for column in columns {
                    if column.name.isEmpty {
                        throw ContractViolation.emptyColumnName(tableName: table.name)
                    }
                    if !columnNames.insert(column.name).inserted {
                        throw ContractViolation.duplicateColumnName(tableName: table.name, columnName: column.name)
                    }
                }
                if let primaryKey = table.primaryKey {
                    for columnName in primaryKey where !columnNames.contains(columnName) {
                        throw ContractViolation.unknownPrimaryKeyColumn(tableName: table.name, columnName: columnName)
                    }
                }
                if let updatedAtColumn = table.updatedAtColumn, !columnNames.contains(updatedAtColumn) {
                    throw ContractViolation.unknownUpdatedAtColumn(tableName: table.name, columnName: updatedAtColumn)
                }
                if let deletedAtColumn = table.deletedAtColumn, !columnNames.contains(deletedAtColumn) {
                    throw ContractViolation.unknownDeletedAtColumn(tableName: table.name, columnName: deletedAtColumn)
                }
            }

            var indexNames = Set<String>()
            if let indexes = table.indexes {
                for index in indexes {
                    if index.name.isEmpty {
                        throw ContractViolation.emptyIndexName(tableName: table.name)
                    }
                    if !indexNames.insert(index.name).inserted {
                        throw ContractViolation.duplicateIndexName(tableName: table.name, indexName: index.name)
                    }
                    if !columnNames.isEmpty {
                        for columnName in index.columns where !columnNames.contains(columnName) {
                            throw ContractViolation.unknownIndexColumn(tableName: table.name, indexName: index.name, columnName: columnName)
                        }
                    }
                }
            }
        }
    }
}

public struct ConnectRequest: Codable, Sendable, Equatable {
    public var clientID: String
    public var platform: String
    public var appVersion: String
    public var protocolVersion: Int
    public var schema: SchemaRef
    public var scopeSetVersion: Int64
    public var knownScopes: [String: ScopeCursorRef]

    enum CodingKeys: String, CodingKey {
        case clientID = "client_id"
        case platform
        case appVersion = "app_version"
        case protocolVersion = "protocol_version"
        case schema
        case scopeSetVersion = "scope_set_version"
        case knownScopes = "known_scopes"
    }
}

public struct ConnectResponse: Codable, Sendable, Equatable {
    public var serverTime: String
    public var protocolVersion: Int
    public var scopeSetVersion: Int64
    public var schema: SchemaDescriptor
    public var scopes: ScopeAssignmentDelta
    public var schemaDefinition: SchemaManifest?

    enum CodingKeys: String, CodingKey {
        case serverTime = "server_time"
        case protocolVersion = "protocol_version"
        case scopeSetVersion = "scope_set_version"
        case schema
        case scopes
        case schemaDefinition = "schema_definition"
    }

    public func validate() throws {
        if schema.action.requiresSchemaDefinition != (schemaDefinition != nil) {
            throw ContractViolation.schemaDefinitionMismatch(
                action: schema.action,
                hasSchemaDefinition: schemaDefinition != nil
            )
        }
        try scopes.validate()
        try schemaDefinition?.validate()
    }
}

public struct Mutation: Codable, Sendable, Equatable {
    public var mutationID: String
    public var table: String
    public var op: Operation
    public var pk: [String: AnyCodable]
    public var baseVersion: String?
    public var clientVersion: String? = nil
    public var columns: [String: AnyCodable]?

    enum CodingKeys: String, CodingKey {
        case mutationID = "mutation_id"
        case table
        case op
        case pk
        case baseVersion = "base_version"
        case clientVersion = "client_version"
        case columns
    }
}

public struct PushRequest: Codable, Sendable, Equatable {
    public var clientID: String
    public var batchID: String
    public var schema: SchemaRef
    public var mutations: [Mutation]

    enum CodingKeys: String, CodingKey {
        case clientID = "client_id"
        case batchID = "batch_id"
        case schema
        case mutations
    }
}

public struct AcceptedMutation: Codable, Sendable, Equatable {
    public var mutationID: String
    public var table: String
    public var pk: [String: AnyCodable]
    public var status: MutationStatus
    public var serverRow: [String: AnyCodable]?
    public var serverVersion: String

    enum CodingKeys: String, CodingKey {
        case mutationID = "mutation_id"
        case table
        case pk
        case status
        case serverRow = "server_row"
        case serverVersion = "server_version"
    }
}

public struct RejectedMutation: Codable, Sendable, Equatable {
    public var mutationID: String
    public var table: String
    public var pk: [String: AnyCodable]
    public var status: MutationStatus
    public var code: MutationRejectionCode
    public var message: String?
    public var serverRow: [String: AnyCodable]?
    public var serverVersion: String?

    enum CodingKeys: String, CodingKey {
        case mutationID = "mutation_id"
        case table
        case pk
        case status
        case code
        case message
        case serverRow = "server_row"
        case serverVersion = "server_version"
    }
}

public struct PushResponse: Codable, Sendable, Equatable {
    public var serverTime: String
    public var accepted: [AcceptedMutation]
    public var rejected: [RejectedMutation]

    enum CodingKeys: String, CodingKey {
        case serverTime = "server_time"
        case accepted
        case rejected
    }
}

public struct PullRequest: Codable, Sendable, Equatable {
    public var clientID: String
    public var schema: SchemaRef
    public var scopeSetVersion: Int64
    public var scopes: [String: ScopeCursorRef]
    public var limit: Int
    public var checksumMode: ChecksumMode?

    enum CodingKeys: String, CodingKey {
        case clientID = "client_id"
        case schema
        case scopeSetVersion = "scope_set_version"
        case scopes
        case limit
        case checksumMode = "checksum_mode"
    }
}

public struct ChangeRecord: Codable, Sendable, Equatable {
    public var scope: String
    public var table: String
    public var op: Operation
    public var pk: [String: AnyCodable]
    public var row: [String: AnyCodable]?
    public var serverVersion: String

    enum CodingKeys: String, CodingKey {
        case scope
        case table
        case op
        case pk
        case row
        case serverVersion = "server_version"
    }
}

public struct PullResponse: Codable, Sendable, Equatable {
    public var changes: [ChangeRecord]
    public var scopeSetVersion: Int64
    public var scopeCursors: [String: String]
    public var scopeUpdates: ScopeAssignmentDelta
    public var rebuild: [String]
    public var hasMore: Bool
    public var checksums: [String: String]?

    enum CodingKeys: String, CodingKey {
        case changes
        case scopeSetVersion = "scope_set_version"
        case scopeCursors = "scope_cursors"
        case scopeUpdates = "scope_updates"
        case rebuild
        case hasMore = "has_more"
        case checksums
    }

    public func requestsRebuild() -> Bool {
        !rebuild.isEmpty
    }

    public func validate(for request: PullRequest) throws {
        try scopeUpdates.validate()
        if request.checksumMode == .required && checksums == nil {
            throw ContractViolation.requiredChecksumsMissing
        }
    }
}

public struct RebuildRequest: Codable, Sendable, Equatable {
    public var clientID: String
    public var scope: String
    public var cursor: String?
    public var limit: Int

    enum CodingKeys: String, CodingKey {
        case clientID = "client_id"
        case scope
        case cursor
        case limit
    }
}

public struct RebuildRecord: Codable, Sendable, Equatable {
    public var table: String
    public var pk: [String: AnyCodable]
    public var row: [String: AnyCodable]?
    public var serverVersion: String

    enum CodingKeys: String, CodingKey {
        case table
        case pk
        case row
        case serverVersion = "server_version"
    }
}

public struct RebuildResponse: Codable, Sendable, Equatable {
    public var scope: String
    public var records: [RebuildRecord]
    public var cursor: String?
    public var hasMore: Bool
    public var finalScopeCursor: String?
    public var checksum: String?

    enum CodingKeys: String, CodingKey {
        case scope
        case records
        case cursor
        case hasMore = "has_more"
        case finalScopeCursor = "final_scope_cursor"
        case checksum
    }

    public func isFinalPage() -> Bool {
        !hasMore && finalScopeCursor != nil
    }

    public func validate() throws {
        if hasMore {
            if finalScopeCursor != nil {
                throw ContractViolation.partialRebuildHasFinalCursor
            }
            if checksum != nil {
                throw ContractViolation.partialRebuildHasChecksum
            }
        } else {
            if finalScopeCursor == nil {
                throw ContractViolation.finalRebuildCursorMissing
            }
            if checksum == nil {
                throw ContractViolation.finalRebuildChecksumMissing
            }
        }
    }
}

public struct ErrorBody: Codable, Sendable, Equatable {
    public var code: ProtocolErrorCode
    public var message: String
    public var retryable: Bool
}

public struct ErrorResponse: Codable, Sendable, Equatable {
    public var error: ErrorBody
}
