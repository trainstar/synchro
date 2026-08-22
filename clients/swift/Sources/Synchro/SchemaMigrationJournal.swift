import Foundation
@preconcurrency import GRDB

enum SchemaMigrationPhase: String, Codable, Sendable {
    case prepared
    case applied
}

enum SchemaMigrationOperationKind: String, Codable, Sendable {
    case dropTable = "drop_table"
    case createTable = "create_table"
    case addColumn = "add_column"
    case createIndex = "create_index"
    case reinstallCapture = "reinstall_capture"
    case clearSyncedMaterialization = "clear_synced_materialization"
    case activateManifest = "activate_manifest"
    case applyScopeState = "apply_scope_state"
}

struct SchemaMigrationOperation: Codable, Sendable, Equatable {
    let kind: SchemaMigrationOperationKind
    let tableID: String?
    let fieldID: String?
    let indexID: String?
}

struct SchemaMigrationPlan: Codable, Sendable, Equatable {
    static let version: Int64 = 1

    let source: SchemaRef
    let target: SchemaRef
    let schemaReset: Bool
    let operations: [SchemaMigrationOperation]

    static func derive(
        source: SchemaRef,
        sourceTables: [LocalSchemaTable],
        targetManifest: SchemaManifest,
        schemaReset: Bool
    ) throws -> SchemaMigrationPlan {
        try targetManifest.validate()
        guard try Integrity.schemaManifestHash(targetManifest) == targetManifest.schemaHash else {
            throw SynchroError.invalidResponse(message: "schema migration target hash is invalid")
        }
        let target = SchemaRef(
            version: targetManifest.schemaVersion,
            hash: targetManifest.schemaHash
        )
        let targetTables = try targetManifest.localTables()
        var operations: [SchemaMigrationOperation] = []

        if schemaReset {
            for table in sourceTables.sorted(by: reverseTableIDOrder) {
                operations.append(operation(.dropTable, tableID: table.tableID))
            }
            operations.append(operation(.clearSyncedMaterialization))
            for table in targetTables.sorted(by: tableIDOrder) {
                operations.append(operation(.createTable, tableID: table.tableID))
                for index in table.indexes.sorted(by: indexIDOrder) {
                    operations.append(operation(
                        .createIndex,
                        tableID: table.tableID,
                        indexID: index.indexID
                    ))
                }
                operations.append(operation(.reinstallCapture, tableID: table.tableID))
            }
        } else {
            let sourceByID = Dictionary(uniqueKeysWithValues: sourceTables.map { ($0.tableID, $0) })
            let targetByID = Dictionary(uniqueKeysWithValues: targetTables.map { ($0.tableID, $0) })
            guard sourceByID.keys.allSatisfy({ targetByID[$0] != nil }) else {
                throw SynchroError.invalidResponse(message: "compatible schema migration removes a table")
            }

            for targetTable in targetTables.sorted(by: tableIDOrder) {
                guard let sourceTable = sourceByID[targetTable.tableID] else {
                    operations.append(operation(.createTable, tableID: targetTable.tableID))
                    for index in targetTable.indexes.sorted(by: indexIDOrder) {
                        operations.append(operation(
                            .createIndex,
                            tableID: targetTable.tableID,
                            indexID: index.indexID
                        ))
                    }
                    operations.append(operation(.reinstallCapture, tableID: targetTable.tableID))
                    continue
                }
                try validateCompatibleTable(sourceTable, target: targetTable)
                let sourceFields = Dictionary(uniqueKeysWithValues: sourceTable.columns.map { ($0.fieldID, $0) })
                for field in targetTable.columns.sorted(by: fieldIDOrder) where sourceFields[field.fieldID] == nil {
                    operations.append(operation(
                        .addColumn,
                        tableID: targetTable.tableID,
                        fieldID: field.fieldID
                    ))
                }
                let sourceIndexes = Dictionary(uniqueKeysWithValues: sourceTable.indexes.map { ($0.indexID, $0) })
                for index in targetTable.indexes.sorted(by: indexIDOrder) {
                    if let sourceIndex = sourceIndexes[index.indexID] {
                        guard sourceIndex == index else {
                            throw SynchroError.invalidResponse(message: "compatible schema migration changes an index")
                        }
                    } else {
                        operations.append(operation(
                            .createIndex,
                            tableID: targetTable.tableID,
                            indexID: index.indexID
                        ))
                    }
                }
                operations.append(operation(.reinstallCapture, tableID: targetTable.tableID))
            }
        }

        operations.append(operation(.activateManifest))
        operations.append(operation(.applyScopeState))
        return SchemaMigrationPlan(
            source: source,
            target: target,
            schemaReset: schemaReset,
            operations: operations
        )
    }

    private static func validateCompatibleTable(
        _ source: LocalSchemaTable,
        target: LocalSchemaTable
    ) throws {
        guard source.relationID == target.relationID,
              source.tableName == target.tableName,
              source.primaryKeyFieldID == target.primaryKeyFieldID else {
            throw SynchroError.invalidResponse(message: "compatible schema migration changes table identity")
        }
        let targetFields = Dictionary(uniqueKeysWithValues: target.columns.map { ($0.fieldID, $0) })
        for sourceField in source.columns {
            guard let targetField = targetFields[sourceField.fieldID],
                  sourceField.name == targetField.name,
                  sourceField.logicalType == targetField.logicalType,
                  (!sourceField.nullable || targetField.nullable),
                  (!sourceField.writable || targetField.writable),
                  sourceField.isPrimaryKey == targetField.isPrimaryKey else {
                throw SynchroError.invalidResponse(message: "compatible schema migration changes field identity")
            }
        }
    }

    private static func operation(
        _ kind: SchemaMigrationOperationKind,
        tableID: String? = nil,
        fieldID: String? = nil,
        indexID: String? = nil
    ) -> SchemaMigrationOperation {
        SchemaMigrationOperation(
            kind: kind,
            tableID: tableID,
            fieldID: fieldID,
            indexID: indexID
        )
    }

    private static func tableIDOrder(_ lhs: LocalSchemaTable, _ rhs: LocalSchemaTable) -> Bool {
        utf8Less(lhs.tableID, rhs.tableID)
    }

    private static func reverseTableIDOrder(_ lhs: LocalSchemaTable, _ rhs: LocalSchemaTable) -> Bool {
        utf8Less(rhs.tableID, lhs.tableID)
    }

    private static func fieldIDOrder(_ lhs: LocalSchemaColumn, _ rhs: LocalSchemaColumn) -> Bool {
        utf8Less(lhs.fieldID, rhs.fieldID)
    }

    private static func indexIDOrder(_ lhs: LocalSchemaIndex, _ rhs: LocalSchemaIndex) -> Bool {
        utf8Less(lhs.indexID, rhs.indexID)
    }

    private static func utf8Less(_ lhs: String, _ rhs: String) -> Bool {
        Array(lhs.utf8).lexicographicallyPrecedes(Array(rhs.utf8))
    }
}

struct SchemaMigrationJournal: Sendable, Equatable {
    static let version: Int64 = 1

    let source: SchemaRef
    let targetManifest: SchemaManifest
    let action: SchemaAction
    let affectedScopes: [String]
    let scopeCursorUpdates: [String: String?]
    let plan: SchemaMigrationPlan
    let planHash: String
    let phase: SchemaMigrationPhase
    let schemaReset: Bool

    var target: SchemaRef {
        SchemaRef(version: targetManifest.schemaVersion, hash: targetManifest.schemaHash)
    }

    func validated() throws -> SchemaMigrationJournal {
        guard action == .replace || action == .rebuildLocal,
              plan.source == source,
              plan.target == target,
              plan.schemaReset == schemaReset else {
            throw SynchroError.invalidResponse(message: "schema migration journal binding is invalid")
        }
        try targetManifest.validate()
        guard try Integrity.schemaManifestHash(targetManifest) == target.hash else {
            throw SynchroError.invalidResponse(message: "schema migration target manifest is invalid")
        }
        let encodedPlan = try JSONEncoder.synchroEncoder().encode(plan)
        guard planHash == Integrity.sha256Hex(
            domain: "synchro:v3:client-migration-plan:v1",
            data: encodedPlan
        ) else {
            throw SynchroError.invalidResponse(message: "schema migration plan hash is invalid")
        }
        return self
    }
}

extension SynchroMeta {
    static func getBlockingFailure(_ db: GRDB.Database) throws -> SyncFailure? {
        guard let row = try Row.fetchOne(
            db,
            sql: """
                SELECT operation, code, retryable, message, recovery_action, metadata_json
                FROM _synchro_blocking_error WHERE singleton = 1
                """
        ) else { return nil }
        guard let operationValue: String = row["operation"],
              let operation = SyncOperationKind(rawValue: operationValue),
              let codeValue: String = row["code"],
              let code = SyncFailureCode(rawValue: codeValue),
              let retryableValue: Int = row["retryable"],
              retryableValue == 0 || retryableValue == 1,
              let message: String = row["message"],
              let recoveryValue: String = row["recovery_action"],
              let recovery = SyncRecoveryAction(rawValue: recoveryValue),
              let metadataJSON: String = row["metadata_json"],
              let metadata = try? JSONDecoder().decode(
                [String: String].self,
                from: Data(metadataJSON.utf8)
              ) else {
            throw SynchroError.invalidResponse(message: "durable blocking error is invalid")
        }
        return SyncFailure(
            operation: operation,
            code: code,
            retryable: retryableValue == 1,
            message: message,
            recoveryAction: recovery,
            metadata: metadata
        )
    }

    static func setBlockingFailure(_ db: GRDB.Database, failure: SyncFailure) throws {
        let metadata = try JSONEncoder.synchroEncoder().encode(failure.metadata)
        guard let metadataJSON = String(data: metadata, encoding: .utf8) else {
            throw SynchroError.invalidResponse(message: "blocking error metadata is invalid")
        }
        try db.execute(
            sql: """
                INSERT INTO _synchro_blocking_error
                    (singleton, operation, code, retryable, message, recovery_action, metadata_json, created_at)
                VALUES (1, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                    operation = excluded.operation,
                    code = excluded.code,
                    retryable = excluded.retryable,
                    message = excluded.message,
                    recovery_action = excluded.recovery_action,
                    metadata_json = excluded.metadata_json,
                    created_at = excluded.created_at
                """,
            arguments: [
                failure.operation.rawValue,
                failure.code.rawValue,
                failure.retryable ? 1 : 0,
                failure.message,
                failure.recoveryAction.rawValue,
                metadataJSON,
                durableTimestamp(),
            ]
        )
    }

    static func clearBlockingFailure(_ db: GRDB.Database) throws {
        try db.execute(sql: "DELETE FROM _synchro_blocking_error WHERE singleton = 1")
    }

    static func getSchemaMigrationJournal(_ db: GRDB.Database) throws -> SchemaMigrationJournal? {
        guard let row = try Row.fetchOne(db, sql: "SELECT * FROM _synchro_schema_migration WHERE singleton = 1") else {
            return nil
        }
        guard let journalVersion: Int64 = row["journal_version"],
              journalVersion == SchemaMigrationJournal.version,
              let sourceVersion: Int64 = row["source_schema_version"],
              let sourceHash: String = row["source_schema_hash"],
              let targetVersion: Int64 = row["target_schema_version"],
              let targetHash: String = row["target_schema_hash"],
              let manifestJSON: String = row["target_manifest_json"],
              let actionValue: String = row["action"],
              let action = SchemaAction(rawValue: actionValue),
              let affectedJSON: String = row["affected_scopes_json"],
              let cursorJSON: String = row["scope_cursor_updates_json"],
              let planVersion: Int64 = row["migration_plan_version"],
              planVersion == SchemaMigrationPlan.version,
              let planJSON: String = row["migration_plan_json"],
              let planHash: String = row["migration_plan_hash"],
              let phaseValue: String = row["phase"],
              let phase = SchemaMigrationPhase(rawValue: phaseValue),
              let resetValue: Int = row["is_schema_reset"],
              resetValue == 0 || resetValue == 1 else {
            throw SynchroError.invalidResponse(message: "schema migration journal is incomplete")
        }
        let decoder = JSONDecoder.synchroDecoder()
        let manifest = try decoder.decode(SchemaManifest.self, from: Data(manifestJSON.utf8))
        guard manifest.schemaVersion == targetVersion, manifest.schemaHash == targetHash else {
            throw SynchroError.invalidResponse(message: "schema migration target reference is inconsistent")
        }
        let affected = try decoder.decode([String].self, from: Data(affectedJSON.utf8))
        let cursors = try decoder.decode([String: String?].self, from: Data(cursorJSON.utf8))
        let plan = try decoder.decode(SchemaMigrationPlan.self, from: Data(planJSON.utf8))
        return try SchemaMigrationJournal(
            source: SchemaRef(version: sourceVersion, hash: sourceHash),
            targetManifest: manifest,
            action: action,
            affectedScopes: affected,
            scopeCursorUpdates: cursors,
            plan: plan,
            planHash: planHash,
            phase: phase,
            schemaReset: resetValue == 1
        ).validated()
    }

    static func insertSchemaMigrationJournal(
        _ db: GRDB.Database,
        journal: SchemaMigrationJournal
    ) throws {
        let journal = try journal.validated()
        if let existing = try getSchemaMigrationJournal(db) {
            guard existing == journal else {
                throw SynchroError.invalidResponse(message: "another schema migration is already active")
            }
            return
        }
        let encoder = JSONEncoder.synchroEncoder()
        let manifestJSON = try utf8JSON(encoder.encode(journal.targetManifest))
        let affectedJSON = try utf8JSON(encoder.encode(journal.affectedScopes))
        let cursorJSON = try utf8JSON(encoder.encode(journal.scopeCursorUpdates))
        let planJSON = try utf8JSON(encoder.encode(journal.plan))
        try db.execute(
            sql: """
                INSERT INTO _synchro_schema_migration
                    (singleton, journal_version, source_schema_version, source_schema_hash,
                     target_schema_version, target_schema_hash, target_manifest_json, action,
                     affected_scopes_json, scope_cursor_updates_json, migration_plan_version,
                     migration_plan_json, migration_plan_hash, phase, is_schema_reset)
                VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
            arguments: [
                SchemaMigrationJournal.version,
                journal.source.version,
                journal.source.hash,
                journal.target.version,
                journal.target.hash,
                manifestJSON,
                journal.action.rawValue,
                affectedJSON,
                cursorJSON,
                SchemaMigrationPlan.version,
                planJSON,
                journal.planHash,
                journal.phase.rawValue,
                journal.schemaReset ? 1 : 0,
            ]
        )
    }

    static func markSchemaMigrationApplied(_ db: GRDB.Database) throws {
        try db.execute(
            sql: "UPDATE _synchro_schema_migration SET phase = 'applied' WHERE singleton = 1 AND phase = 'prepared'"
        )
        guard db.changesCount == 1 else {
            throw SynchroError.invalidResponse(message: "schema migration phase is invalid")
        }
    }

    static func clearSchemaMigrationJournal(_ db: GRDB.Database) throws {
        try db.execute(sql: "DELETE FROM _synchro_schema_migration WHERE singleton = 1")
    }

    private static func utf8JSON(_ data: Data) throws -> String {
        guard let value = String(data: data, encoding: .utf8) else {
            throw SynchroError.invalidResponse(message: "durable JSON is not UTF-8")
        }
        return value
    }

    private static func durableTimestamp() -> String {
        ISO8601DateFormatter().string(from: Date())
    }
}
