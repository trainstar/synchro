import Foundation
@preconcurrency import GRDB

enum SeedDatabaseInstaller {
    private static let sidecarSuffixes = ["-journal", "-wal", "-shm"]

    static func installIfNeeded(seedPath: String, databasePath: String) throws {
        let fileManager = FileManager.default
        guard !fileManager.fileExists(atPath: databasePath),
              fileManager.fileExists(atPath: seedPath) else {
            return
        }

        let destinationURL = URL(fileURLWithPath: databasePath)
        let temporaryPath = destinationURL.deletingLastPathComponent()
            .appendingPathComponent(".\(destinationURL.lastPathComponent).seed-\(UUID().uuidString)")
            .path
        do {
            try fileManager.copyItem(atPath: seedPath, toPath: temporaryPath)

            let migratingDatabase = try SynchroDatabase(path: temporaryPath)
            try migratingDatabase.close()
            try removeSQLiteSidecars(at: temporaryPath, fileManager: fileManager)

            try SeedDatabaseValidator.validate(at: temporaryPath)
            try removeSQLiteSidecars(at: temporaryPath, fileManager: fileManager)

            guard !fileManager.fileExists(atPath: databasePath) else {
                throw SeedInstallationError.destinationAlreadyExists
            }
            try fileManager.moveItem(atPath: temporaryPath, toPath: databasePath)
        } catch {
            do {
                try removeDatabaseFamily(at: temporaryPath, fileManager: fileManager)
            } catch {
                throw SeedInstallationError.cleanupFailed
            }
            throw error
        }
    }

    private static func removeDatabaseFamily(at path: String, fileManager: FileManager) throws {
        if fileManager.fileExists(atPath: path) {
            try fileManager.removeItem(atPath: path)
        }
        try removeSQLiteSidecars(at: path, fileManager: fileManager)
    }

    private static func removeSQLiteSidecars(at path: String, fileManager: FileManager) throws {
        for suffix in sidecarSuffixes {
            let sidecarPath = path + suffix
            if fileManager.fileExists(atPath: sidecarPath) {
                try fileManager.removeItem(atPath: sidecarPath)
            }
        }
    }
}

private enum SeedDatabaseValidator {
    private static let requiredTables: Set<String> = [
        "_synchro_meta",
        "_synchro_pending_changes",
        "_synchro_mutation_values",
        "_synchro_capture_context",
        "_synchro_capture_fields",
        "_synchro_push_batch_members",
        "_synchro_schema_archive",
        "_synchro_scopes",
        "_synchro_scope_rows",
        "_synchro_seed_receipts",
        "_synchro_row_versions",
        "_synchro_rebuild_attempts",
        "_synchro_rebuild_page_receipts",
        "_synchro_backoff",
        "_synchro_blocking_error",
        "_synchro_schema_migration",
        "_synchro_push_batches",
        "_synchro_rejected_mutations",
        "grdb_migrations",
    ]

    private static let requiredMigrationIdentifiers: Set<String> = [
        "synchro_v1",
        "synchro_v2_buckets",
        "synchro_v3_scopes",
        "synchro_v4_scope_integrity",
        "synchro_v5_rejected_mutations",
        "synchro_v6_protocol_3",
        "synchro_v7_pending_local_revision",
        "synchro_v8_sealed_push_batches",
        "synchro_v9_mutation_ledger",
        "synchro_v10_rebuild_page_receipts",
        "synchro_v11_durable_backoff",
        "synchro_v12_gate2_recovery",
        "synchro_v13_scope_text_affinity",
        "synchro_v14_capture_context",
    ]

    private static let emptyWorkTables = [
        "_synchro_pending_changes",
        "_synchro_mutation_values",
        "_synchro_capture_context",
        "_synchro_capture_fields",
        "_synchro_push_batch_members",
        "_synchro_push_batches",
        "_synchro_rejected_mutations",
        "_synchro_rebuild_attempts",
        "_synchro_rebuild_page_receipts",
        "_synchro_backoff",
        "_synchro_blocking_error",
        "_synchro_schema_migration",
    ]

    private struct SchemaState {
        let reference: SchemaRef
        let tables: [LocalSchemaTable]
    }

    private struct SeedReceipt {
        let scopeID: String
        let receipt: String
        let cardinality: Int64
        let checksum: ChecksumObject
    }

    private struct SeedScope {
        let scopeID: String
        let checksum: ChecksumObject
        let localChecksum: ChecksumObject
    }

    private struct SeedRowVersion {
        let serverVersion: String
        let checksum: ChecksumObject
    }

    private struct SeedScopeRow {
        let scopeID: String
        let tableName: String
        let recordID: String
    }

    private struct SeedRowKey: Hashable {
        let tableName: String
        let recordID: String
    }

    private struct SeedScopeRowKey: Hashable {
        let scopeID: String
        let row: SeedRowKey
    }

    static func validate(at path: String) throws {
        let database = try DatabaseQueue(path: path)
        do {
            try database.read { db in
                try validateIntegrity(db)
                try validateInternalState(db)
                let schema = try validateSchemaState(db)
                try validateSeedState(db, schema: schema)
            }
            try database.close()
        } catch {
            try? database.close()
            throw error
        }
    }

    private static func validateIntegrity(_ db: GRDB.Database) throws {
        let results = try String.fetchAll(db, sql: "PRAGMA integrity_check")
        guard results == ["ok"] else {
            throw SeedInstallationError.invalidDatabase
        }
    }

    private static func validateInternalState(_ db: GRDB.Database) throws {
        let tables = Set(try String.fetchAll(
            db,
            sql: "SELECT name FROM sqlite_master WHERE type = 'table'"
        ))
        guard requiredTables.isSubset(of: tables) else {
            throw SeedInstallationError.missingSynchroState
        }

        let migrations = try String.fetchAll(db, sql: "SELECT identifier FROM grdb_migrations")
        guard migrations.count == requiredMigrationIdentifiers.count,
              Set(migrations) == requiredMigrationIdentifiers else {
            throw SeedInstallationError.missingSynchroState
        }

        for table in emptyWorkTables {
            let count = try Int64.fetchOne(
                db,
                sql: "SELECT COUNT(*) FROM \(SQLiteHelpers.quoteIdentifier(table))"
            )
            guard count == 0 else {
                throw SeedInstallationError.invalidState
            }
        }
    }

    private static func validateSchemaState(_ db: GRDB.Database) throws -> SchemaState {
        let metadata = try loadMetadata(db)
        for key in [
            MetaKey.checkpoint.rawValue,
            MetaKey.syncLock.rawValue,
            MetaKey.schemaVersion.rawValue,
            MetaKey.schemaHash.rawValue,
            MetaKey.localSchema.rawValue,
            MetaKey.schemaManifest.rawValue,
            MetaKey.scopeSetVersion.rawValue,
            MetaKey.snapshotComplete.rawValue,
        ] where metadata[key] == nil {
            throw SeedInstallationError.missingSynchroState
        }

        guard metadata[MetaKey.snapshotComplete.rawValue] == "1",
              metadata[MetaKey.syncLock.rawValue] == "0",
              metadata[MetaKey.checkpoint.rawValue] == "0",
              metadata[MetaKey.clientServerID.rawValue] == nil else {
            throw SeedInstallationError.invalidState
        }
        if let generation = metadata[MetaKey.clientGeneration.rawValue] {
            guard try canonicalInteger(generation) == 0 else {
                throw SeedInstallationError.invalidState
            }
        }
        guard try canonicalInteger(requiredMetadata(metadata, key: MetaKey.scopeSetVersion.rawValue)) == 0 else {
            throw SeedInstallationError.invalidState
        }

        let version = try canonicalInteger(requiredMetadata(metadata, key: MetaKey.schemaVersion.rawValue))
        let hash = try requiredMetadata(metadata, key: MetaKey.schemaHash.rawValue)
        let reference = SchemaRef(version: version, hash: hash)
        try reference.validate()

        let manifest: SchemaManifest = try decodeJSON(
            requiredMetadata(metadata, key: MetaKey.schemaManifest.rawValue)
        )
        try manifest.validate()
        guard manifest.schemaVersion == reference.version,
              manifest.schemaHash == reference.hash,
              try Integrity.schemaManifestHash(manifest) == reference.hash else {
            throw SeedInstallationError.invalidState
        }

        let tables = try manifest.localTables()
        let storedTables: [LocalSchemaTable] = try decodeJSON(
            requiredMetadata(metadata, key: MetaKey.localSchema.rawValue)
        )
        guard storedTables == tables else {
            throw SeedInstallationError.invalidState
        }
        try validateSchemaArchive(db, reference: reference, tables: tables)
        try validateLocalSchema(db, tables: tables)
        return SchemaState(reference: reference, tables: tables)
    }

    private static func loadMetadata(_ db: GRDB.Database) throws -> [String: String] {
        let rows = try Row.fetchAll(db, sql: "SELECT key, value FROM _synchro_meta")
        var metadata: [String: String] = [:]
        for row in rows {
            guard let key: String = row["key"],
                  !key.isEmpty,
                  let value: String = row["value"],
                  metadata.updateValue(value, forKey: key) == nil else {
                throw SeedInstallationError.invalidState
            }
        }
        return metadata
    }

    private static func requiredMetadata(_ metadata: [String: String], key: String) throws -> String {
        guard let value = metadata[key] else {
            throw SeedInstallationError.missingSynchroState
        }
        return value
    }

    private static func canonicalInteger(_ value: String) throws -> Int64 {
        guard let parsed = Int64(value), String(parsed) == value else {
            throw SeedInstallationError.invalidState
        }
        return parsed
    }

    private static func decodeJSON<Value: Decodable>(_ source: String) throws -> Value {
        guard let data = source.data(using: .utf8) else {
            throw SeedInstallationError.invalidState
        }
        return try JSONDecoder.synchroDecoder().decode(Value.self, from: data)
    }

    private static func validateSchemaArchive(
        _ db: GRDB.Database,
        reference: SchemaRef,
        tables: [LocalSchemaTable]
    ) throws {
        let rows = try Row.fetchAll(
            db,
            sql: "SELECT schema_version, schema_hash, schema_json FROM _synchro_schema_archive"
        )
        guard rows.count == 1,
              let version: Int64 = rows[0]["schema_version"],
              let hash: String = rows[0]["schema_hash"],
              let schemaJSON: String = rows[0]["schema_json"],
              version == reference.version,
              hash == reference.hash else {
            throw SeedInstallationError.invalidState
        }
        let archivedTables: [LocalSchemaTable] = try decodeJSON(schemaJSON)
        guard archivedTables == tables else {
            throw SeedInstallationError.invalidState
        }
    }

    private static func validateLocalSchema(
        _ db: GRDB.Database,
        tables: [LocalSchemaTable]
    ) throws {
        for table in tables {
            let tableInfo = try Row.fetchAll(
                db,
                sql: "PRAGMA table_info(\(SQLiteHelpers.quoteIdentifier(table.tableName)))"
            )
            guard tableInfo.count == table.columns.count else {
                throw SeedInstallationError.invalidState
            }
            for (index, column) in table.columns.enumerated() {
                let actual = tableInfo[index]
                let actualName: String? = actual["name"]
                let actualType: String? = actual["type"]
                let actualNotNull: Int64 = actual["notnull"] ?? -1
                let actualPrimaryKey: Int64 = actual["pk"] ?? -1
                let actualDefault: String? = actual["dflt_value"]
                let expectedNotNull: Int64 = !column.nullable && !column.isPrimaryKey ? 1 : 0
                let expectedPrimaryKey: Int64 = column.isPrimaryKey ? 1 : 0
                guard actualName == column.name,
                      actualType == SQLiteSchema.sqliteType(for: column.logicalType),
                      actualNotNull == expectedNotNull,
                      actualPrimaryKey == expectedPrimaryKey,
                      actualDefault == column.sqliteDefaultSQL else {
                    throw SeedInstallationError.invalidState
                }
            }

            let expectedTriggers = Array(SQLiteSchema.generateCDCTriggers(table: table).dropFirst(3))
            let triggerNames = [
                "_synchro_cdc_insert_\(table.tableName)",
                "_synchro_cdc_update_\(table.tableName)",
                "_synchro_cdc_delete_\(table.tableName)",
            ]
            for (name, expected) in zip(triggerNames, expectedTriggers) {
                let actual = try String.fetchOne(
                    db,
                    sql: "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?",
                    arguments: [name]
                )
                guard let actual, normalizedSQL(actual) == normalizedSQL(expected) else {
                    throw SeedInstallationError.invalidState
                }
            }
        }
    }

    private static func normalizedSQL(_ statement: String) -> String {
        statement.split(whereSeparator: { $0.isWhitespace }).joined(separator: " ")
    }

    private static func validateSeedState(_ db: GRDB.Database, schema: SchemaState) throws {
        let tablesByName = Dictionary(uniqueKeysWithValues: schema.tables.map { ($0.tableName, $0) })
        let receipts = try loadSeedReceipts(db, schema: schema.reference)
        let scopes = try loadSeedScopes(db)
        guard Set(receipts.keys) == Set(scopes.keys) else {
            throw SeedInstallationError.invalidState
        }

        let versions = try loadRowVersions(db, tablesByName: tablesByName)
        let scopeRows = try loadScopeRows(
            db,
            scopes: scopes,
            versions: versions,
            tablesByName: tablesByName
        )
        let materializedRows = try loadMaterializedRows(db, tables: schema.tables)
        let scopedRows = Set(scopeRows.map { SeedRowKey(tableName: $0.tableName, recordID: $0.recordID) })
        guard materializedRows == Set(versions.keys),
              materializedRows == scopedRows else {
            throw SeedInstallationError.invalidState
        }

        let rowsByScope = Dictionary(grouping: scopeRows, by: \.scopeID)
        for (scopeID, scope) in scopes {
            guard let receipt = receipts[scopeID],
                  Int64(rowsByScope[scopeID, default: []].count) == receipt.cardinality,
                  scope.checksum == receipt.checksum,
                  scope.localChecksum == receipt.checksum else {
                throw SeedInstallationError.invalidState
            }
            let computed = try PullProcessor.recomputeScopeChecksum(
                db: db,
                scopeID: scopeID,
                schemaHash: schema.reference.hash,
                tablesByName: tablesByName
            )
            guard computed == receipt.checksum else {
                throw SeedInstallationError.invalidState
            }
        }
    }

    private static func loadSeedReceipts(
        _ db: GRDB.Database,
        schema: SchemaRef
    ) throws -> [String: SeedReceipt] {
        let rows = try Row.fetchAll(
            db,
            sql: "SELECT scope_id, receipt, schema_version, schema_hash, cardinality, checksum FROM _synchro_seed_receipts"
        )
        var receipts: [String: SeedReceipt] = [:]
        for row in rows {
            guard let scopeID: String = row["scope_id"],
                  !scopeID.isEmpty,
                  let receipt: String = row["receipt"],
                  !receipt.isEmpty,
                  let version: Int64 = row["schema_version"],
                  let hash: String = row["schema_hash"],
                  let cardinality: Int64 = row["cardinality"],
                  cardinality >= 0,
                  let checksumJSON: String = row["checksum"],
                  version == schema.version,
                  hash == schema.hash else {
                throw SeedInstallationError.invalidState
            }
            let checksum = try decodeChecksum(checksumJSON)
            guard receipts.updateValue(
                SeedReceipt(
                    scopeID: scopeID,
                    receipt: receipt,
                    cardinality: cardinality,
                    checksum: checksum
                ),
                forKey: scopeID
            ) == nil else {
                throw SeedInstallationError.invalidState
            }
        }
        return receipts
    }

    private static func loadSeedScopes(_ db: GRDB.Database) throws -> [String: SeedScope] {
        let rows = try Row.fetchAll(
            db,
            sql: "SELECT scope_id, cursor, checksum, generation, local_checksum FROM _synchro_scopes"
        )
        var scopes: [String: SeedScope] = [:]
        for row in rows {
            let cursor: String? = row["cursor"]
            guard let scopeID: String = row["scope_id"],
                  !scopeID.isEmpty,
                  cursor == nil,
                  let checksumJSON: String = row["checksum"],
                  let localChecksumJSON: String = row["local_checksum"],
                  let generation: Int64 = row["generation"],
                  generation == 0 else {
                throw SeedInstallationError.invalidState
            }
            let checksum = try decodeChecksum(checksumJSON)
            let localChecksum = try decodeChecksum(localChecksumJSON)
            guard scopes.updateValue(
                SeedScope(
                    scopeID: scopeID,
                    checksum: checksum,
                    localChecksum: localChecksum
                ),
                forKey: scopeID
            ) == nil else {
                throw SeedInstallationError.invalidState
            }
        }
        return scopes
    }

    private static func loadRowVersions(
        _ db: GRDB.Database,
        tablesByName: [String: LocalSchemaTable]
    ) throws -> [SeedRowKey: SeedRowVersion] {
        let rows = try Row.fetchAll(
            db,
            sql: "SELECT table_name, record_id, server_version, row_checksum FROM _synchro_row_versions"
        )
        var versions: [SeedRowKey: SeedRowVersion] = [:]
        for row in rows {
            guard let tableName: String = row["table_name"],
                  tablesByName[tableName] != nil,
                  let recordID: String = row["record_id"],
                  let serverVersion: String = row["server_version"],
                  !serverVersion.isEmpty,
                  let checksumJSON: String = row["row_checksum"] else {
                throw SeedInstallationError.invalidState
            }
            let key = SeedRowKey(tableName: tableName, recordID: recordID)
            let version = SeedRowVersion(
                serverVersion: serverVersion,
                checksum: try decodeChecksum(checksumJSON)
            )
            guard versions.updateValue(version, forKey: key) == nil else {
                throw SeedInstallationError.invalidState
            }
        }
        return versions
    }

    private static func loadScopeRows(
        _ db: GRDB.Database,
        scopes: [String: SeedScope],
        versions: [SeedRowKey: SeedRowVersion],
        tablesByName: [String: LocalSchemaTable]
    ) throws -> [SeedScopeRow] {
        let rows = try Row.fetchAll(
            db,
            sql: "SELECT scope_id, table_name, record_id, checksum, generation FROM _synchro_scope_rows"
        )
        var scopeRows: [SeedScopeRow] = []
        var seen = Set<SeedScopeRowKey>()
        for row in rows {
            guard let scopeID: String = row["scope_id"],
                  scopes[scopeID] != nil,
                  let tableName: String = row["table_name"],
                  tablesByName[tableName] != nil,
                  let recordID: String = row["record_id"],
                  let checksum: String = row["checksum"],
                  let generation: Int64 = row["generation"],
                  generation == 0 else {
                throw SeedInstallationError.invalidState
            }
            let rowKey = SeedRowKey(tableName: tableName, recordID: recordID)
            let checksumObject = ChecksumObject(
                algorithm: "sha256",
                version: 1,
                encoding: "hex",
                digest: checksum
            )
            try checksumObject.validate()
            guard versions[rowKey]?.checksum == checksumObject else {
                throw SeedInstallationError.invalidState
            }
            let key = SeedScopeRowKey(scopeID: scopeID, row: rowKey)
            guard seen.insert(key).inserted else {
                throw SeedInstallationError.invalidState
            }
            scopeRows.append(SeedScopeRow(scopeID: scopeID, tableName: tableName, recordID: recordID))
        }
        return scopeRows
    }

    private static func loadMaterializedRows(
        _ db: GRDB.Database,
        tables: [LocalSchemaTable]
    ) throws -> Set<SeedRowKey> {
        var rows = Set<SeedRowKey>()
        for table in tables {
            guard let primaryKey = table.columns.first(where: { $0.fieldID == table.primaryKeyFieldID }) else {
                throw SeedInstallationError.invalidState
            }
            let result = try Row.fetchAll(
                db,
                sql: "SELECT \(SQLiteHelpers.quoteIdentifier(primaryKey.name)) FROM \(SQLiteHelpers.quoteIdentifier(table.tableName))"
            )
            for row in result {
                let value: DatabaseValue = row[primaryKey.name]
                let key = SeedRowKey(
                    tableName: table.tableName,
                    recordID: try recordID(value, primaryKey: primaryKey)
                )
                guard rows.insert(key).inserted else {
                    throw SeedInstallationError.invalidState
                }
            }
        }
        return rows
    }

    private static func recordID(_ value: DatabaseValue, primaryKey: LocalSchemaColumn) throws -> String {
        switch (primaryKey.logicalType, value.storage) {
        case ("string", .string(let value)):
            return value
        case ("int", .int64(let value)) where value >= Int64(Int32.min) && value <= Int64(Int32.max):
            return String(value)
        case ("int64", .int64(let value)):
            return String(value)
        default:
            throw SeedInstallationError.invalidState
        }
    }

    private static func decodeChecksum(_ source: String) throws -> ChecksumObject {
        let data = Data(source.utf8)
        try Integrity.validateCanonicalWireJSON(data)
        let checksum = try JSONDecoder.synchroDecoder().decode(ChecksumObject.self, from: data)
        try checksum.validate()
        return checksum
    }
}

private enum SeedInstallationError: LocalizedError {
    case invalidDatabase
    case missingSynchroState
    case invalidState
    case destinationAlreadyExists
    case cleanupFailed

    var errorDescription: String? {
        switch self {
        case .invalidDatabase:
            return "Seed database integrity check failed"
        case .missingSynchroState:
            return "Seed database is missing required Synchro state"
        case .invalidState:
            return "Seed database semantic verification failed"
        case .destinationAlreadyExists:
            return "Seed database destination already exists"
        case .cleanupFailed:
            return "Seed database temporary cleanup failed"
        }
    }
}
