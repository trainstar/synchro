import Foundation

public enum ContractViolation: Error, Equatable {
    case invalidProtocolVersion(Int)
    case emptyScopeID
    case duplicateAddedScope(String)
    case duplicateRemovedScope(String)
    case conflictingScopeAssignment(String)
    case schemaDefinitionMismatch(action: SchemaAction, hasSchemaDefinition: Bool)
    case schemaManifestHashMismatch
    case emptyTableName
    case duplicateTableName(String)
    case missingPrimaryKey(tableName: String)
    case missingUpdatedAtColumn(tableName: String)
    case missingDeletedAtColumn(tableName: String)
    case missingColumns(tableName: String)
    case emptyColumnName(tableName: String)
    case duplicateColumnName(tableName: String, columnName: String)
    case unsupportedColumnType(tableName: String, columnName: String, type: String)
    case unknownPrimaryKeyColumn(tableName: String, columnName: String)
    case unknownUpdatedAtColumn(tableName: String, columnName: String)
    case unknownDeletedAtColumn(tableName: String, columnName: String)
    case emptyIndexName(tableName: String)
    case duplicateIndexName(tableName: String, indexName: String)
    case unknownIndexColumn(tableName: String, indexName: String, columnName: String)
    case finalPullChecksumsMissing
    case finalRebuildCursorMissing
    case finalRebuildChecksumMissing
    case partialRebuildCursorMissing
    case partialRebuildHasFinalCursor
    case partialRebuildHasChecksum
    case finalRebuildHasCursor
    case nonterminalPullChecksumsPresent
    case invalidPullOperation(String)
    case invalidPullScope(String)
    case invalidPullAssignment(String)
    case invalidChecksum
    case invalidMutationShape(String)
    case pushBatchIDMismatch
    case pushOutcomeMismatch(String)
    case invalidOutcomeOrder
    case invalidScopeCursorUpdate(String)
    case missingScopeCursorUpdate(String)
    case invalidAffectedScopes
    case invalidScopeSetVersion(request: Int64, response: Int64)
    case invalidRebuildScope(expected: String, received: String)
}

public enum Operation: String, Codable, Sendable {
    case insert
    case upsert
    case update
    case delete
}

public enum SchemaAction: String, Codable, Sendable {
    case none
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

public enum MutationStatus: String, Codable, Sendable {
    case applied
    case conflict
    case rejectedTerminal = "rejected_terminal"
}

public enum MutationRejectionCode: String, Codable, Sendable {
    case versionConflict = "version_conflict"
    case rowAlreadyExists = "row_already_exists"
    case rowDeleted = "row_deleted"
    case rowNotFound = "row_not_found"
    case schemaIncompatible = "schema_incompatible"
    case policyRejected = "policy_rejected"
    case validationFailed = "validation_failed"
    case tableNotSynced = "table_not_synced"
}

public enum ProtocolErrorCode: String, Codable, Sendable, Hashable {
    case invalidRequest = "invalid_request"
    case invalidSchemaReference = "invalid_schema_reference"
    case upgradeRequired = "upgrade_required"
    case authRequired = "auth_required"
    case idempotencyConflict = "idempotency_conflict"
    case clientRetired = "client_retired"
    case clientGenerationExpired = "client_generation_expired"
    case rebuildRestartRequired = "rebuild_restart_required"
    case schemaMismatch = "schema_mismatch"
    case retryLater = "retry_later"
    case syncIntegrityFailure = "sync_integrity_failure"
    case capturePending = "capture_pending"
    case temporaryUnavailable = "temporary_unavailable"
}

public enum SchemaUnsupportedReason: String, Codable, Sendable {
    case unknownSchemaLineage = "unknown_schema_lineage"
    case incompatibleSchemaTransition = "incompatible_schema_transition"
}

public enum TemporaryUnavailableReason: String, Codable, Sendable {
    case captureBlocked = "capture_blocked"
}

public enum CompositionClass: String, Codable, Sendable {
    case singleScope = "single_scope"
    case multiScope = "multi_scope"
}

public struct SchemaRef: Codable, Sendable, Equatable {
    public var version: Int64
    public var hash: String

    func validate(allowFresh: Bool = false) throws {
        if allowFresh && version == 0 && hash.isEmpty {
            return
        }
        guard version > 0, version <= 9_007_199_254_740_991,
              hash.count == 64,
              hash.utf8.allSatisfy({ ($0 >= 48 && $0 <= 57) || ($0 >= 97 && $0 <= 102) }) else {
            throw ContractViolation.invalidMutationShape("invalid schema reference")
        }
    }
}

public struct ScopeCursorRef: Codable, Sendable, Equatable {
    public var cursor: String?

    enum CodingKeys: String, CodingKey {
        case cursor
    }

    public init(cursor: String?) {
        self.cursor = cursor
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        cursor = try container.decodeIfPresent(String.self, forKey: .cursor)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        if let cursor {
            try container.encode(cursor, forKey: .cursor)
        } else {
            try container.encodeNil(forKey: .cursor)
        }
    }
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
    public var reason: SchemaUnsupportedReason?
}

public struct ChecksumObject: Codable, Sendable, Equatable {
    public var algorithm: String
    public var version: Int
    public var encoding: String
    public var digest: String

    public func validate() throws {
        guard algorithm == "sha256", version == 1, encoding == "hex",
              digest.count == 64,
              digest.utf8.allSatisfy({ ($0 >= 48 && $0 <= 57) || ($0 >= 97 && $0 <= 102) }) else {
            throw ContractViolation.invalidChecksum
        }
    }
}

public struct ColumnSchema: Codable, Sendable, Equatable {
    public var fieldID: String
    public var name: String
    public var type: String
    public var nullable: Bool
    public var writable: Bool
    public var precision: Int?
    public var scale: Int?

    enum CodingKeys: String, CodingKey {
        case fieldID = "field_id"
        case name
        case type
        case nullable
        case writable
        case precision
        case scale
    }
}

public struct IndexSchema: Codable, Sendable, Equatable {
    public var indexID: String
    public var name: String
    public var fieldIDs: [String]
    public var unique: Bool

    enum CodingKeys: String, CodingKey {
        case indexID = "index_id"
        case name
        case fieldIDs = "field_ids"
        case unique
    }
}

public struct LifecycleSchema: Codable, Sendable, Equatable {
    public var createdAtFieldID: String?
    public var updatedAtFieldID: String?
    public var deletedAtFieldID: String?

    enum CodingKeys: String, CodingKey {
        case createdAtFieldID = "created_at_field_id"
        case updatedAtFieldID = "updated_at_field_id"
        case deletedAtFieldID = "deleted_at_field_id"
    }
}

public struct TableSchema: Codable, Sendable, Equatable {
    public var tableID: String
    public var relationID: String
    public var name: String
    public var primaryKeyFieldID: String
    public var lifecycle: LifecycleSchema
    public var composition: CompositionClass
    public var fields: [ColumnSchema]
    public var indexes: [IndexSchema]

    enum CodingKeys: String, CodingKey {
        case tableID = "table_id"
        case relationID = "relation_id"
        case name
        case primaryKeyFieldID = "primary_key_field_id"
        case lifecycle
        case composition
        case fields
        case indexes
    }
}

public struct SchemaManifest: Codable, Sendable, Equatable {
    private static let supportedFieldTypes: Set<String> = [
        "string", "int", "int64", "decimal", "float", "boolean",
        "datetime", "date", "time", "json", "bytes",
    ]
    private static let primaryKeyFieldTypes: Set<String> = ["string", "int", "int64"]
    private static let maxSafeJSONInteger: Int64 = 9_007_199_254_740_991

    public var schemaVersion: Int64
    public var schemaHash: String
    public var parentSchema: SchemaRef?
    public var transitionClass: String
    public var compatibilityFloor: Int64
    public var tables: [TableSchema]

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case schemaHash = "schema_hash"
        case parentSchema = "parent_schema"
        case transitionClass = "transition_class"
        case compatibilityFloor = "compatibility_floor"
        case tables
    }

    public func validate() throws {
        try SchemaRef(version: schemaVersion, hash: schemaHash).validate()
        guard compatibilityFloor > 0, compatibilityFloor <= Self.maxSafeJSONInteger else {
            throw ContractViolation.invalidMutationShape("invalid schema manifest compatibility floor")
        }
        switch transitionClass {
        case "initial":
            guard parentSchema == nil, compatibilityFloor == schemaVersion else {
                throw ContractViolation.invalidMutationShape("invalid initial schema manifest lineage")
            }
        case "class_2":
            guard let parentSchema else {
                throw ContractViolation.invalidMutationShape("schema manifest parent is missing")
            }
            try parentSchema.validate()
            guard parentSchema.version < schemaVersion, compatibilityFloor <= parentSchema.version else {
                throw ContractViolation.invalidMutationShape("invalid Class 2 schema manifest lineage")
            }
        case "class_3", "class_4":
            guard let parentSchema else {
                throw ContractViolation.invalidMutationShape("schema manifest parent is missing")
            }
            try parentSchema.validate()
            guard parentSchema.version < schemaVersion, compatibilityFloor == schemaVersion else {
                throw ContractViolation.invalidMutationShape("invalid schema manifest lineage boundary")
            }
        default:
            throw ContractViolation.invalidMutationShape("invalid schema manifest transition class")
        }
        guard !tables.isEmpty else {
            throw ContractViolation.invalidMutationShape("schema manifest contains no tables")
        }

        var tableIDs = Set<String>()
        var relationIDs = Set<String>()
        var tableNames = Set<String>()
        for table in tables {
            if table.tableID.isEmpty || table.relationID.isEmpty || table.name.isEmpty {
                throw ContractViolation.emptyTableName
            }
            if !tableIDs.insert(table.tableID).inserted || !relationIDs.insert(table.relationID).inserted {
                throw ContractViolation.invalidMutationShape("duplicate schema manifest table identity")
            }
            if !tableNames.insert(table.name).inserted {
                throw ContractViolation.duplicateTableName(table.name)
            }

            guard !table.fields.isEmpty else {
                throw ContractViolation.missingColumns(tableName: table.name)
            }
            var fieldIDs = Set<String>()
            var fieldNames = Set<String>()
            var fieldsByID: [String: ColumnSchema] = [:]
            for field in table.fields {
                if field.fieldID.isEmpty || field.name.isEmpty {
                    throw ContractViolation.emptyColumnName(tableName: table.name)
                }
                if !fieldIDs.insert(field.fieldID).inserted || !fieldNames.insert(field.name).inserted {
                    throw ContractViolation.duplicateColumnName(tableName: table.name, columnName: field.name)
                }
                if !Self.supportedFieldTypes.contains(field.type) {
                    throw ContractViolation.unsupportedColumnType(
                        tableName: table.name,
                        columnName: field.name,
                        type: field.type
                    )
                }
                if field.type == "decimal" {
                    guard let precision = field.precision, let scale = field.scale,
                          precision > 0, Int64(precision) <= Self.maxSafeJSONInteger,
                          scale >= 0, Int64(scale) <= Self.maxSafeJSONInteger,
                          scale <= precision else {
                        throw ContractViolation.invalidMutationShape("invalid decimal metadata for \(table.name).\(field.name)")
                    }
                } else if field.precision != nil || field.scale != nil {
                    throw ContractViolation.invalidMutationShape("decimal metadata on non-decimal field \(table.name).\(field.name)")
                }
                fieldsByID[field.fieldID] = field
            }
            guard let primaryKey = fieldsByID[table.primaryKeyFieldID] else {
                throw ContractViolation.unknownPrimaryKeyColumn(tableName: table.name, columnName: table.primaryKeyFieldID)
            }
            guard !primaryKey.nullable, !primaryKey.writable,
                  Self.primaryKeyFieldTypes.contains(primaryKey.type) else {
                throw ContractViolation.invalidMutationShape("invalid primary key field for \(table.name)")
            }

            for lifecycleFieldID in [
                table.lifecycle.createdAtFieldID,
                table.lifecycle.updatedAtFieldID,
                table.lifecycle.deletedAtFieldID,
            ].compactMap({ $0 }) {
                guard let field = fieldsByID[lifecycleFieldID],
                      field.type == "datetime", !field.writable else {
                    throw ContractViolation.invalidMutationShape("invalid lifecycle field for \(table.name)")
                }
            }

            var indexIDs = Set<String>()
            var indexNames = Set<String>()
            for index in table.indexes {
                if index.indexID.isEmpty || index.name.isEmpty || index.fieldIDs.isEmpty {
                    throw ContractViolation.emptyIndexName(tableName: table.name)
                }
                if !indexIDs.insert(index.indexID).inserted || !indexNames.insert(index.name).inserted {
                    throw ContractViolation.duplicateIndexName(tableName: table.name, indexName: index.name)
                }
                var indexedFields = Set<String>()
                for fieldID in index.fieldIDs {
                    guard fieldIDs.contains(fieldID) else {
                        throw ContractViolation.unknownIndexColumn(tableName: table.name, indexName: index.name, columnName: fieldID)
                    }
                    guard indexedFields.insert(fieldID).inserted else {
                        throw ContractViolation.invalidMutationShape("duplicate index field for \(table.name).\(index.name)")
                    }
                }
            }
        }
    }
}

public struct ConnectRequest: Codable, Sendable, Equatable {
    public var clientID: String
    public var clientGeneration: Int64?
    public var platform: String
    public var appVersion: String
    public var protocolVersion: Int
    public var schemaReset: Bool?
    public var schema: SchemaRef
    public var scopeSetVersion: Int64
    public var knownScopes: [String: ScopeCursorRef]
    public var seedReceipts: [String: String]?

    enum CodingKeys: String, CodingKey {
        case clientID = "client_id"
        case clientGeneration = "client_generation"
        case platform
        case appVersion = "app_version"
        case protocolVersion = "protocol_version"
        case schemaReset = "schema_reset"
        case schema
        case scopeSetVersion = "scope_set_version"
        case knownScopes = "known_scopes"
        case seedReceipts = "seed_receipts"
    }
}

public struct ConnectResponse: Codable, Sendable, Equatable {
    public var serverTime: String
    public var protocolVersion: Int
    public var clientGeneration: Int64
    public var scopeSetVersion: Int64
    public var schema: SchemaDescriptor
    public var scopes: ScopeAssignmentDelta
    public var scopeCursorUpdates: [String: String?]
    public var schemaDefinition: SchemaManifest?
    public var affectedScopes: [String]?

    enum CodingKeys: String, CodingKey {
        case serverTime = "server_time"
        case protocolVersion = "protocol_version"
        case clientGeneration = "client_generation"
        case scopeSetVersion = "scope_set_version"
        case schema
        case scopes
        case scopeCursorUpdates = "scope_cursor_updates"
        case schemaDefinition = "schema_definition"
        case affectedScopes = "affected_scopes"
    }

    public func validate(
        existingScopes: [String: ScopeCursorRef]? = nil,
        requestScopeSetVersion: Int64? = nil
    ) throws {
        let existingScopeIDs = existingScopes.map { Set($0.keys) }
        guard protocolVersion == 3 else {
            throw ContractViolation.invalidProtocolVersion(protocolVersion)
        }
        guard clientGeneration > 0, scopeSetVersion >= 0 else {
            throw ContractViolation.invalidMutationShape("invalid connect response counters")
        }
        try SchemaRef(version: schema.version, hash: schema.hash).validate()
        if schema.action.requiresSchemaDefinition != (schemaDefinition != nil) {
            throw ContractViolation.schemaDefinitionMismatch(
                action: schema.action,
                hasSchemaDefinition: schemaDefinition != nil
            )
        }
        try scopes.validate()
        try schemaDefinition?.validate()
        if let schemaDefinition,
           schemaDefinition.schemaVersion != schema.version || schemaDefinition.schemaHash != schema.hash {
            throw ContractViolation.schemaDefinitionMismatch(
                action: schema.action,
                hasSchemaDefinition: true
            )
        }
        if let schemaDefinition,
           try Integrity.schemaManifestHash(schemaDefinition) != schemaDefinition.schemaHash {
            throw ContractViolation.schemaManifestHashMismatch
        }

        if let requestScopeSetVersion {
            let assignmentChanged = !scopes.add.isEmpty || !scopes.remove.isEmpty
            guard scopeSetVersion >= requestScopeSetVersion,
                  !assignmentChanged || scopeSetVersion > requestScopeSetVersion else {
                throw ContractViolation.invalidScopeSetVersion(
                    request: requestScopeSetVersion,
                    response: scopeSetVersion
                )
            }
        }

        if let existingScopeIDs {
            let added = Set(scopes.add.map(\.id))
            let removed = Set(scopes.remove)
            guard added.isDisjoint(with: existingScopeIDs), removed.isSubset(of: existingScopeIDs) else {
                throw ContractViolation.invalidPullAssignment("connect assignment delta does not match existing scopes")
            }
        }

        for scopeID in scopeCursorUpdates.keys {
            guard !scopeID.isEmpty else {
                throw ContractViolation.invalidScopeCursorUpdate(scopeID)
            }
            if scopes.add.contains(where: { $0.id == scopeID }) || scopes.remove.contains(scopeID) {
                throw ContractViolation.invalidScopeCursorUpdate(scopeID)
            }
            if let existingScopeIDs, !existingScopeIDs.contains(scopeID) {
                throw ContractViolation.invalidScopeCursorUpdate(scopeID)
            }
        }
        if schema.action == .replace || schema.action == .rebuildLocal, let existingScopes {
            for (scopeID, scope) in existingScopes
                where scope.cursor != nil && !scopes.remove.contains(scopeID) && scopeCursorUpdates[scopeID] == nil {
                throw ContractViolation.missingScopeCursorUpdate(scopeID)
            }
        }

        switch schema.action {
        case .none, .replace:
            guard affectedScopes == nil, schema.reason == nil else { throw ContractViolation.invalidAffectedScopes }
        case .rebuildLocal:
            guard let affectedScopes, !affectedScopes.isEmpty else {
                throw ContractViolation.invalidAffectedScopes
            }
            var seen = Set<String>()
            for scopeID in affectedScopes where scopeID.isEmpty || !seen.insert(scopeID).inserted {
                throw ContractViolation.invalidAffectedScopes
            }
            if let existingScopeIDs {
                let assigned = existingScopeIDs
                    .subtracting(scopes.remove)
                    .union(scopes.add.map(\.id))
                guard Set(affectedScopes).isSubset(of: assigned) else {
                    throw ContractViolation.invalidAffectedScopes
                }
            }
        case .unsupported:
            guard affectedScopes == nil, schema.reason != nil else {
                throw ContractViolation.invalidAffectedScopes
            }
        }
    }
}

public struct Mutation: Codable, Sendable, Equatable {
    public var mutationID: String
    public var table: String
    public var op: Operation
    public var pk: [String: AnyCodable]
    public var authoredSchema: SchemaRef
    public var baseVersion: String?
    public var clientVersion: String
    public var columns: [String: AnyCodable]?

    enum CodingKeys: String, CodingKey {
        case mutationID = "mutation_id"
        case table
        case op
        case pk
        case authoredSchema = "authored_schema"
        case baseVersion = "base_version"
        case clientVersion = "client_version"
        case columns
    }
}

public struct PushRequest: Codable, Sendable, Equatable {
    public var clientID: String
    public var clientGeneration: Int64
    public var batchID: String
    public var schema: SchemaRef
    public var mutations: [Mutation]

    enum CodingKeys: String, CodingKey {
        case clientID = "client_id"
        case clientGeneration = "client_generation"
        case batchID = "batch_id"
        case schema
        case mutations
    }

    func validate(syncedTables: [LocalSchemaTable]) throws {
        guard clientGeneration > 0,
              UUID(uuidString: batchID) != nil,
              batchID == batchID.lowercased(),
              !mutations.isEmpty else {
            throw ContractViolation.invalidMutationShape("push batch is empty")
        }
        try schema.validate()
        let tables = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableID, $0) })
        var mutationIDs = Set<String>()
        for mutation in mutations {
            guard UUID(uuidString: mutation.mutationID) != nil,
                  mutation.mutationID == mutation.mutationID.lowercased(),
                  mutationIDs.insert(mutation.mutationID).inserted else {
                throw ContractViolation.invalidMutationShape("invalid or duplicate mutation ID")
            }
            try mutation.authoredSchema.validate()
            let table = tables[mutation.table]
            do {
                try Integrity.validateCanonicalClientVersion(mutation.clientVersion)
            } catch {
                throw ContractViolation.invalidMutationShape("mutation contains a noncanonical client version")
            }
            let columns = mutation.columns ?? [:]
            // A retained historical payload may contain a removed or renamed
            // field.  Preserve it for the server's schema_incompatible outcome.
            // Validate field membership locally only for the active authored schema.
            if mutation.authoredSchema == schema {
                guard let table else {
                    throw ContractViolation.invalidMutationShape("unknown logical table \(mutation.table)")
                }
                guard mutation.pk.count == 1,
                      mutation.pk.keys.first == table.primaryKeyFieldID,
                      mutation.pk[table.primaryKeyFieldID] != nil else {
                    throw ContractViolation.invalidMutationShape("invalid primary key for \(mutation.table)")
                }
                guard let primaryKey = table.columns.first(where: { $0.fieldID == table.primaryKeyFieldID }),
                      validPrimaryKeyValue(mutation.pk[table.primaryKeyFieldID]!, logicalType: primaryKey.logicalType) else {
                    throw ContractViolation.invalidMutationShape("primary key has the wrong type")
                }
                do {
                    try Integrity.validateTypedValue(
                        mutation.pk[table.primaryKeyFieldID]!,
                        field: primaryKey,
                        requirePresent: true
                    )
                } catch {
                    throw ContractViolation.invalidMutationShape("mutation contains a noncanonical primary key")
                }
                let writableIDs = Set(table.columns.filter(\.writable).map(\.fieldID))
                guard columns.keys.allSatisfy({ writableIDs.contains($0) }) else {
                    throw ContractViolation.invalidMutationShape("mutation contains a non-writable or unknown field")
                }
                for (fieldID, value) in columns {
                    guard let field = table.columns.first(where: { $0.fieldID == fieldID }) else {
                        throw ContractViolation.invalidMutationShape("mutation contains an unknown field")
                    }
                    do {
                        try Integrity.validateTypedValue(value, field: field)
                    } catch {
                        throw ContractViolation.invalidMutationShape("mutation contains a noncanonical portable value")
                    }
                }
            } else {
                guard mutation.pk.count == 1,
                      let value = mutation.pk.values.first,
                      value.value is String || value.value is Int || value.value is Int64 else {
                    throw ContractViolation.invalidMutationShape("historical mutation has an invalid primary key")
                }
            }
            switch mutation.op {
            case .insert:
                guard mutation.baseVersion == nil, !columns.isEmpty else {
                    throw ContractViolation.invalidMutationShape("insert shape is invalid")
                }
            case .update:
                guard let baseVersion = mutation.baseVersion, !baseVersion.isEmpty, !columns.isEmpty else {
                    throw ContractViolation.invalidMutationShape("update shape is invalid")
                }
            case .delete:
                guard let baseVersion = mutation.baseVersion, !baseVersion.isEmpty, mutation.columns == nil else {
                    throw ContractViolation.invalidMutationShape("delete shape is invalid")
                }
            case .upsert:
                throw ContractViolation.invalidMutationShape("upsert is not a push operation")
            }
        }
    }

    private func validPrimaryKeyValue(_ value: AnyCodable, logicalType: String) -> Bool {
        switch logicalType {
        case "string":
            return value.value is String
        case "int":
            if let int = value.value as? Int64 {
                return int >= Int64(Int32.min) && int <= Int64(Int32.max)
            }
            if let int = value.value as? Int {
                return int >= Int(Int32.min) && int <= Int(Int32.max)
            }
            return false
        case "int64":
            guard let string = value.value as? String,
                  !string.isEmpty else { return false }
            if string == "0" { return true }
            let digits = string.first == "-" ? String(string.dropFirst()) : string
            return !digits.isEmpty && digits.first != "0" && digits.allSatisfy(\.isNumber) && Int64(string) != nil
        default:
            return false
        }
    }

}

public struct AcceptedMutation: Codable, Sendable, Equatable {
    public var mutationID: String
    public var table: String
    public var pk: [String: AnyCodable]
    public var outcomeSchema: SchemaRef
    public var status: MutationStatus
    public var serverRow: [String: AnyCodable]?
    public var rowChecksum: ChecksumObject?
    public var serverVersion: String

    enum CodingKeys: String, CodingKey {
        case mutationID = "mutation_id"
        case table
        case pk
        case outcomeSchema = "outcome_schema"
        case status
        case serverRow = "server_row"
        case rowChecksum = "row_checksum"
        case serverVersion = "server_version"
    }
}

public struct RejectedMutation: Codable, Sendable, Equatable {
    public var mutationID: String
    public var table: String
    public var pk: [String: AnyCodable]
    public var outcomeSchema: SchemaRef
    public var status: MutationStatus
    public var code: MutationRejectionCode
    public var message: String
    public var retryable: Bool?
    public var serverRow: [String: AnyCodable]?
    public var rowChecksum: ChecksumObject?
    public var serverVersion: String?
    public var authoredSchema: SchemaRef?
    public var currentSchema: SchemaRef?
    public var incompatibleFieldIDs: [String]?

    enum CodingKeys: String, CodingKey {
        case mutationID = "mutation_id"
        case table
        case pk
        case outcomeSchema = "outcome_schema"
        case status
        case code
        case message
        case retryable
        case serverRow = "server_row"
        case rowChecksum = "row_checksum"
        case serverVersion = "server_version"
        case authoredSchema = "authored_schema"
        case currentSchema = "current_schema"
        case incompatibleFieldIDs = "incompatible_field_ids"
    }
}

public struct PushResponse: Codable, Sendable, Equatable {
    public var batchID: String
    public var serverTime: String
    public var accepted: [AcceptedMutation]
    public var rejected: [RejectedMutation]

    enum CodingKeys: String, CodingKey {
        case batchID = "batch_id"
        case serverTime = "server_time"
        case accepted
        case rejected
    }

    public func validate(for request: PushRequest? = nil) throws {
        var mutationIDs = Set<String>()
        for outcome in accepted {
            guard outcome.status == .applied,
                   mutationIDs.insert(outcome.mutationID).inserted else {
                throw ContractViolation.invalidChecksum
            }
            try outcome.outcomeSchema.validate()
            try outcome.rowChecksum?.validate()
        }
        for outcome in rejected {
            guard outcome.status == .conflict || outcome.status == .rejectedTerminal,
                   mutationIDs.insert(outcome.mutationID).inserted else {
                throw ContractViolation.invalidChecksum
            }
            try outcome.outcomeSchema.validate()
            try outcome.rowChecksum?.validate()
        }

        if let request {
            guard batchID == request.batchID else {
                throw ContractViolation.pushBatchIDMismatch
            }
            let requested = Dictionary(uniqueKeysWithValues: request.mutations.map { ($0.mutationID, $0) })
            let positions = Dictionary(
                uniqueKeysWithValues: request.mutations.enumerated().map {
                    ($0.element.mutationID, $0.offset)
                }
            )
            guard mutationIDs == Set(requested.keys) else {
                throw ContractViolation.pushOutcomeMismatch("outcome IDs do not exactly match request IDs")
            }
            try validateOutcomeOrder(accepted.map(\.mutationID), positions: positions)
            try validateOutcomeOrder(rejected.map(\.mutationID), positions: positions)
            for outcome in accepted {
                try validateOutcome(outcome, request: requested[outcome.mutationID], schema: request.schema)
            }
            for outcome in rejected {
                try validateOutcome(outcome, request: requested[outcome.mutationID], schema: request.schema)
            }
        }
    }

    /// Validates row digests with the immutable manifest retained for a sealed batch.
    ///
    /// The request schema is not substituted for an outcome schema during this check.
    func validate(for request: PushRequest, historicalTables: [LocalSchemaTable]) throws {
        try validate(for: request)
        let tables = Dictionary(uniqueKeysWithValues: historicalTables.map { ($0.tableID, $0) })
        for outcome in accepted {
            guard let row = outcome.serverRow else { continue }
            guard let checksum = outcome.rowChecksum,
                  let table = tables[outcome.table],
                  let version = Optional(outcome.serverVersion) else {
                throw ContractViolation.pushOutcomeMismatch("accepted row lacks its historical schema")
            }
            let computed = try Integrity.rowDigest(
                schemaHash: outcome.outcomeSchema.hash,
                table: table,
                pk: outcome.pk,
                row: row,
                serverVersion: version
            ).checksum
            guard computed == checksum else {
                throw ContractViolation.pushOutcomeMismatch("accepted row checksum does not match outcome schema")
            }
        }
        for outcome in rejected {
            guard let row = outcome.serverRow else { continue }
            guard let checksum = outcome.rowChecksum,
                  let version = outcome.serverVersion,
                  let table = tables[outcome.table] else {
                throw ContractViolation.pushOutcomeMismatch("rejected row lacks its historical schema")
            }
            let computed = try Integrity.rowDigest(
                schemaHash: outcome.outcomeSchema.hash,
                table: table,
                pk: outcome.pk,
                row: row,
                serverVersion: version
            ).checksum
            guard computed == checksum else {
                throw ContractViolation.pushOutcomeMismatch("rejected row checksum does not match outcome schema")
            }
        }
    }

    private func validateOutcomeOrder(
        _ mutationIDs: [String],
        positions: [String: Int]
    ) throws {
        var previous = -1
        for mutationID in mutationIDs {
            guard let position = positions[mutationID], position > previous else {
                throw ContractViolation.invalidOutcomeOrder
            }
            previous = position
        }
    }

    private func validateOutcome(_ outcome: AcceptedMutation, request: Mutation?, schema: SchemaRef) throws {
        guard let request,
               outcome.table == request.table,
               outcome.pk == request.pk else {
            throw ContractViolation.pushOutcomeMismatch("accepted outcome does not match request")
        }
        try outcome.outcomeSchema.validate()
        guard !outcome.serverVersion.isEmpty else {
            throw ContractViolation.pushOutcomeMismatch("accepted outcome has an empty server version")
        }
        let hasRow = outcome.serverRow != nil
        let hasChecksum = outcome.rowChecksum != nil
        switch request.op {
        case .insert, .update:
            guard hasRow, hasChecksum else {
                throw ContractViolation.pushOutcomeMismatch("accepted insert or update lacks its row or checksum")
            }
        case .delete:
            guard hasRow == hasChecksum else {
                throw ContractViolation.pushOutcomeMismatch("accepted delete row and checksum must be paired")
            }
        case .upsert:
            throw ContractViolation.pushOutcomeMismatch("accepted outcome targets an unsupported push operation")
        }
    }

    private func validateOutcome(_ outcome: RejectedMutation, request: Mutation?, schema: SchemaRef) throws {
        guard let request,
               outcome.table == request.table,
               outcome.pk == request.pk else {
            throw ContractViolation.pushOutcomeMismatch("rejected outcome does not match request")
        }
        try outcome.outcomeSchema.validate()
        let conflictCodes: Set<MutationRejectionCode> = [
            .versionConflict, .rowAlreadyExists, .rowDeleted, .rowNotFound,
        ]
        let terminalCodes: Set<MutationRejectionCode> = [
            .schemaIncompatible, .policyRejected, .validationFailed, .tableNotSynced,
        ]
        switch outcome.status {
        case .conflict:
            guard conflictCodes.contains(outcome.code) else {
                throw ContractViolation.pushOutcomeMismatch("conflict outcome has a terminal rejection code")
            }
            guard (outcome.serverRow == nil) == (outcome.rowChecksum == nil) else {
                throw ContractViolation.pushOutcomeMismatch("conflict row and checksum must be paired")
            }
            if outcome.serverRow != nil && outcome.serverVersion == nil {
                throw ContractViolation.pushOutcomeMismatch("conflict row has no server version")
            }
            if let serverVersion = outcome.serverVersion, serverVersion.isEmpty {
                throw ContractViolation.pushOutcomeMismatch("conflict outcome has an empty server version")
            }
        case .rejectedTerminal:
            guard terminalCodes.contains(outcome.code) else {
                throw ContractViolation.pushOutcomeMismatch("terminal outcome has a conflict rejection code")
            }
            guard outcome.serverRow == nil,
                   outcome.rowChecksum == nil,
                   outcome.serverVersion == nil else {
                throw ContractViolation.pushOutcomeMismatch("terminal outcome contains authoritative row metadata")
            }
            if outcome.code == .schemaIncompatible {
                guard outcome.retryable == false,
                       outcome.authoredSchema == request.authoredSchema,
                       outcome.currentSchema == outcome.outcomeSchema,
                       let fieldIDs = outcome.incompatibleFieldIDs,
                       !fieldIDs.isEmpty || request.op == .delete,
                       Set(fieldIDs).count == fieldIDs.count,
                      fieldIDs.elementsEqual(fieldIDs.sorted(by: utf8Less)) else {
                    throw ContractViolation.pushOutcomeMismatch("schema_incompatible outcome is incomplete")
                }
            } else {
                guard outcome.authoredSchema == nil,
                      outcome.currentSchema == nil,
                      outcome.incompatibleFieldIDs == nil else {
                    throw ContractViolation.pushOutcomeMismatch("non-schema terminal outcome contains schema diagnostics")
                }
            }
        case .applied:
            throw ContractViolation.pushOutcomeMismatch("rejected outcome has applied status")
        }
    }

    private func utf8Less(_ lhs: String, _ rhs: String) -> Bool {
        Array(lhs.utf8).lexicographicallyPrecedes(Array(rhs.utf8))
    }
}

public struct PullRequest: Codable, Sendable, Equatable {
    public var clientID: String
    public var clientGeneration: Int64
    public var schema: SchemaRef
    public var scopeSetVersion: Int64
    public var scopes: [String: ScopeCursorRef]
    public var limit: Int

    enum CodingKeys: String, CodingKey {
        case clientID = "client_id"
        case clientGeneration = "client_generation"
        case schema
        case scopeSetVersion = "scope_set_version"
        case scopes
        case limit
    }
}

public struct ChangeRecord: Codable, Sendable, Equatable {
    public var scope: String
    public var table: String
    public var op: Operation
    public var pk: [String: AnyCodable]
    public var row: [String: AnyCodable]?
    public var rowChecksum: ChecksumObject?
    public var serverVersion: String

    enum CodingKeys: String, CodingKey {
        case scope
        case table
        case op
        case pk
        case row
        case rowChecksum = "row_checksum"
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
    public var checksums: [String: ChecksumObject]?

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

    public func validate() throws {
        try scopeUpdates.validate()
        for change in changes {
            switch change.op {
            case .upsert, .delete:
                break
            case .insert, .update:
                throw ContractViolation.invalidPullOperation(change.op.rawValue)
            }
        }
        if hasMore && checksums != nil {
            throw ContractViolation.nonterminalPullChecksumsPresent
        }
        if !hasMore && checksums == nil {
            throw ContractViolation.finalPullChecksumsMissing
        }
    }

    public func validate(
        activeScopes: Set<String>,
        requestScopeSetVersion: Int64? = nil
    ) throws {
        try validate()
        if let requestScopeSetVersion {
            let assignmentChanged = !scopeUpdates.add.isEmpty || !scopeUpdates.remove.isEmpty
            guard scopeSetVersion >= requestScopeSetVersion,
                  !assignmentChanged || scopeSetVersion > requestScopeSetVersion else {
                throw ContractViolation.invalidScopeSetVersion(
                    request: requestScopeSetVersion,
                    response: scopeSetVersion
                )
            }
        }
        let added = Set(scopeUpdates.add.map(\.id))
        let removed = Set(scopeUpdates.remove)
        guard removed.isSubset(of: activeScopes), added.isDisjoint(with: activeScopes),
              scopeUpdates.add.allSatisfy({ $0.cursor == nil }) else {
            throw ContractViolation.invalidPullAssignment("assignment delta does not match active scopes")
        }

        let expectedScopes = activeScopes.subtracting(removed).union(added)
        let rebuildScopes = Set(rebuild)
        guard changes.allSatisfy({ expectedScopes.contains($0.scope) }),
              Set(scopeCursors.keys).isSubset(of: expectedScopes),
              rebuildScopes.isSubset(of: expectedScopes),
              added.isSubset(of: rebuildScopes) else {
            throw ContractViolation.invalidPullScope("pull members do not match assigned scopes")
        }
        if !hasMore, Set(checksums?.keys.map { $0 } ?? []) != expectedScopes {
            throw ContractViolation.invalidPullScope("terminal checksum map does not match active scopes")
        }
        if !Set(scopeCursors.keys).isDisjoint(with: rebuildScopes) {
            throw ContractViolation.invalidPullScope("rebuild scope received an incremental cursor")
        }
    }
}

public struct RebuildRequest: Codable, Sendable, Equatable {
    public var clientID: String
    public var clientGeneration: Int64
    public var schema: SchemaRef
    public var scope: String
    public var rebuildID: String
    public var cursor: String?
    public var limit: Int

    enum CodingKeys: String, CodingKey {
        case clientID = "client_id"
        case clientGeneration = "client_generation"
        case schema
        case scope
        case rebuildID = "rebuild_id"
        case cursor
        case limit
    }
}

public struct RebuildRecord: Codable, Sendable, Equatable {
    public var table: String
    public var pk: [String: AnyCodable]
    public var row: [String: AnyCodable]
    public var rowChecksum: ChecksumObject
    public var serverVersion: String

    enum CodingKeys: String, CodingKey {
        case table
        case pk
        case row
        case rowChecksum = "row_checksum"
        case serverVersion = "server_version"
    }
}

public struct RebuildResponse: Codable, Sendable, Equatable {
    public var scope: String
    public var records: [RebuildRecord]
    public var cursor: String?
    public var hasMore: Bool
    public var finalScopeCursor: String?
    public var checksum: ChecksumObject?

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
        guard !scope.isEmpty else {
            throw ContractViolation.invalidRebuildScope(expected: "nonempty", received: scope)
        }
        if hasMore {
            if cursor == nil {
                throw ContractViolation.partialRebuildCursorMissing
            }
            if finalScopeCursor != nil {
                throw ContractViolation.partialRebuildHasFinalCursor
            }
            if checksum != nil {
                throw ContractViolation.partialRebuildHasChecksum
            }
        } else {
            if cursor != nil {
                throw ContractViolation.finalRebuildHasCursor
            }
            if finalScopeCursor == nil {
                throw ContractViolation.finalRebuildCursorMissing
            }
            if checksum == nil {
                throw ContractViolation.finalRebuildChecksumMissing
            }
        }
    }

    public func validate(for request: RebuildRequest) throws {
        try validate()
        guard scope == request.scope else {
            throw ContractViolation.invalidRebuildScope(expected: request.scope, received: scope)
        }
    }
}

public struct ErrorBody: Codable, Sendable, Equatable {
    public var code: ProtocolErrorCode
    public var message: String
    public var retryable: Bool
    public var currentSchema: SchemaRef?
    public var receivedSchema: SchemaRef?
    public var currentClientGeneration: Int64?
    public var scopeID: String?
    public var requiredProtocolVersion: Int?
    public var receivedProtocolVersion: Int?
    public var minimumClientVersion: String?
    public var receivedClientVersion: String?
    public var reason: TemporaryUnavailableReason?
    public var field: String?
    public var minimum: Int64?
    public var maximum: Int64?

    enum CodingKeys: String, CodingKey {
        case code, message, retryable, reason, field, minimum, maximum
        case currentSchema = "current_schema"
        case receivedSchema = "received_schema"
        case currentClientGeneration = "current_client_generation"
        case scopeID = "scope_id"
        case requiredProtocolVersion = "required_protocol_version"
        case receivedProtocolVersion = "received_protocol_version"
        case minimumClientVersion = "minimum_client_version"
        case receivedClientVersion = "received_client_version"
    }
}

public struct ErrorResponse: Codable, Sendable, Equatable {
    public var error: ErrorBody
}
