import Foundation
@preconcurrency import GRDB

final class SchemaManager: @unchecked Sendable {
    private let database: SynchroDatabase

    init(database: SynchroDatabase) {
        self.database = database
    }

    func ensureSchema(httpClient: HttpClient) async throws -> SchemaResponse {
        let (localVersion, localHash) = try database.readTransaction { db in
            let version = try SynchroMeta.getInt64(db, key: .schemaVersion)
            let hash = try SynchroMeta.get(db, key: .schemaHash) ?? ""
            return (version, hash)
        }

        let schema = try await httpClient.fetchSchema()

        if localVersion == schema.schemaVersion && localHash == schema.schemaHash {
            return schema
        }

        try migrateSchema(newSchema: schema)

        return schema
    }

    func loadStoredLocalSchema() throws -> [LocalSchemaTable]? {
        try database.readTransaction { db in
            guard let encoded = try SynchroMeta.get(db, key: .localSchema) else {
                return nil
            }
            return try JSONDecoder().decode([LocalSchemaTable].self, from: Data(encoded.utf8))
        }
    }

    func prepareMigration(
        targetManifest: SchemaManifest,
        action: SchemaAction,
        affectedScopes: [String],
        scopeCursorUpdates: [String: String?],
        schemaReset: Bool
    ) throws -> SchemaMigrationJournal {
        guard action == .replace || action == .rebuildLocal,
              (action == .rebuildLocal) == !affectedScopes.isEmpty else {
            throw SynchroError.invalidResponse(message: "schema migration action is invalid")
        }
        let sortedScopes = affectedScopes.sorted(by: utf8Less)
        guard affectedScopes == sortedScopes, Set(affectedScopes).count == affectedScopes.count else {
            throw SynchroError.invalidResponse(message: "schema migration scopes are not canonical")
        }

        let prepared = try database.writeTransaction { db -> (
            journal: SchemaMigrationJournal,
            sourceTables: [LocalSchemaTable]
        ) in
            let source = SchemaRef(
                version: try SynchroMeta.getInt64(db, key: .schemaVersion),
                hash: try SynchroMeta.get(db, key: .schemaHash) ?? ""
            )
            let sourceTables = try storedLocalSchema(in: db) ?? []
            if source.version > 0 {
                try source.validate()
                guard !sourceTables.isEmpty else {
                    throw SynchroError.invalidResponse(message: "schema migration source manifest is missing")
                }
            } else if source.version != 0 || !source.hash.isEmpty || !sourceTables.isEmpty {
                throw SynchroError.invalidResponse(message: "fresh schema migration source is invalid")
            }
            let plan = try SchemaMigrationPlan.derive(
                source: source,
                sourceTables: sourceTables,
                targetManifest: targetManifest,
                schemaReset: schemaReset
            )
            let encodedPlan = try JSONEncoder.synchroEncoder().encode(plan)
            let journal = SchemaMigrationJournal(
                source: source,
                targetManifest: targetManifest,
                action: action,
                affectedScopes: affectedScopes,
                scopeCursorUpdates: scopeCursorUpdates,
                plan: plan,
                planHash: Integrity.sha256Hex(
                    domain: "synchro:v3:client-migration-plan:v1",
                    data: encodedPlan
                ),
                phase: .prepared,
                schemaReset: schemaReset
            )
            try SynchroMeta.insertSchemaMigrationJournal(db, journal: journal)
            return (journal, sourceTables)
        }
        database.updateApplicationSyncedTables(protectedTableUnion(
            source: prepared.sourceTables,
            target: try targetManifest.localTables()
        ))
        return prepared.journal
    }

    /// Recovers committed migration intent before any network request starts.
    @discardableResult
    func recoverMigrationIfNeeded() throws -> SchemaMigrationJournal? {
        let pending = try database.readTransaction { db -> (
            journal: SchemaMigrationJournal,
            sourceTables: [LocalSchemaTable]
        )? in
            guard let journal = try SynchroMeta.getSchemaMigrationJournal(db) else {
                return nil
            }
            return (journal, try storedLocalSchema(in: db) ?? [])
        }
        guard let pending else { return nil }
        database.updateApplicationSyncedTables(protectedTableUnion(
            source: pending.sourceTables,
            target: try pending.journal.targetManifest.localTables()
        ))

        let recovered = try database.writeSyncLockedTransaction { db -> SchemaMigrationJournal? in
            guard let journal = try SynchroMeta.getSchemaMigrationJournal(db),
                  journal == pending.journal else {
                throw SynchroError.invalidResponse(message: "schema migration changed during recovery")
            }
            let applied = try applyMigrationJournalInTransaction(db, journal: journal)
            if applied.affectedScopes.isEmpty {
                try SynchroMeta.clearSchemaMigrationJournal(db)
            }
            return applied
        }
        if let recovered {
            database.updateApplicationSyncedTables(try recovered.targetManifest.localTables())
        }
        return recovered
    }

    func applyPreparedMigrationInTransaction(
        _ db: GRDB.Database
    ) throws -> SchemaMigrationJournal {
        guard let journal = try SynchroMeta.getSchemaMigrationJournal(db) else {
            throw SynchroError.invalidResponse(message: "schema migration journal is missing")
        }
        return try applyMigrationJournalInTransaction(db, journal: journal)
    }

    func finishAppliedMigrationIfPossible() throws {
        try database.writeTransaction { db in
            guard let journal = try SynchroMeta.getSchemaMigrationJournal(db) else {
                return
            }
            guard journal.phase == .applied else {
                throw SynchroError.invalidResponse(message: "schema migration has not applied local DDL")
            }
            for scopeID in journal.affectedScopes {
                guard let scope = try SynchroMeta.getScope(db, scopeID: scopeID),
                      scope.cursor != nil,
                      scope.checksum != nil,
                      !scope.localChecksum.isEmpty,
                      try SynchroMeta.getRebuildAttempt(db, scopeID: scopeID) == nil else {
                    return
                }
            }
            try SynchroMeta.clearSchemaMigrationJournal(db)
        }
    }

    func activeMigration() throws -> SchemaMigrationJournal? {
        try database.readTransaction { db in
            try SynchroMeta.getSchemaMigrationJournal(db)
        }
    }

    func reconcileLocalSchema(
        schemaVersion: Int64,
        schemaHash: String,
        tables: [LocalSchemaTable],
        scopeCursorUpdates: [String: String?] = [:],
        affectedScopes: [String] = []
    ) throws {
        try database.writeTransaction { db in
            try reconcileLocalSchemaInTransaction(
                db,
                schemaVersion: schemaVersion,
                schemaHash: schemaHash,
                tables: tables,
                scopeCursorUpdates: scopeCursorUpdates,
                affectedScopes: affectedScopes
            )
        }
        database.updateApplicationSyncedTables(tables)
    }

    func reconcileLocalSchemaInTransaction(
        _ db: GRDB.Database,
        schemaVersion: Int64,
        schemaHash: String,
        tables: [LocalSchemaTable],
        scopeCursorUpdates: [String: String?] = [:],
        affectedScopes: [String] = []
    ) throws {
        let localVersion = try SynchroMeta.getInt64(db, key: .schemaVersion)
        let localHash = try SynchroMeta.get(db, key: .schemaHash) ?? ""
        if localVersion != schemaVersion || localHash != schemaHash {
            try migrateLocalSchemaInTransaction(db, newTables: tables)
        }
        try SynchroMeta.setInt64(db, key: .schemaVersion, value: schemaVersion)
        try SynchroMeta.set(db, key: .schemaHash, value: schemaHash)
        try SynchroMeta.archiveSchema(db, version: schemaVersion, hash: schemaHash, tables: tables)
        try persistLocalSchemaTables(db, tables: tables)
        for (scopeID, cursor) in scopeCursorUpdates where cursor != nil {
            try recomputeRetainedScopeIntegrity(
                db,
                scopeID: scopeID,
                schemaHash: schemaHash,
                tables: tables
            )
        }
        try SynchroMeta.applyScopeCursorUpdates(
            db,
            updates: scopeCursorUpdates,
            affectedScopes: affectedScopes
        )
    }

    func createSyncedTables(schema: SchemaResponse) throws {
        let tables = try schema.localTables()
        try database.writeTransaction { db in
            try createSyncedTablesInTransaction(db, schema: schema)
        }
        database.updateApplicationSyncedTables(tables)
    }

    func createSyncedTablesInTransaction(_ db: GRDB.Database, schema: SchemaResponse) throws {
        let tables = try schema.localTables()
        try createSyncedTablesInTransaction(
            db,
            tables: tables,
            installTriggers: schema.schemaVersion > 0 && !schema.schemaHash.isEmpty
        )
        try SynchroMeta.setInt64(db, key: .schemaVersion, value: schema.schemaVersion)
        try SynchroMeta.set(db, key: .schemaHash, value: schema.schemaHash)
        try SynchroMeta.archiveSchema(db, version: schema.schemaVersion, hash: schema.schemaHash, tables: tables)
        try persistLocalSchemaTables(db, tables: tables)
    }

    func createSyncedTablesInTransaction(
        _ db: GRDB.Database,
        tables: [LocalSchemaTable],
        installTriggers: Bool = true
    ) throws {
        for table in tables {
            let createSQL = SQLiteSchema.generateCreateTableSQL(table: table)
            try db.execute(sql: createSQL)

            for index in table.indexes {
                try db.execute(sql: SQLiteSchema.generateIndexSQL(index: index, table: table))
            }

            if installTriggers {
                let triggers = SQLiteSchema.generateCDCTriggers(table: table)
                for trigger in triggers {
                    try db.execute(sql: trigger)
                }
            }
        }
    }

    func migrateSchema(newSchema: SchemaResponse) throws {
        let tables = try newSchema.localTables()
        try database.writeTransaction { db in
            try migrateLocalSchemaInTransaction(db, newTables: tables)
            try SynchroMeta.setInt64(db, key: .schemaVersion, value: newSchema.schemaVersion)
            try SynchroMeta.set(db, key: .schemaHash, value: newSchema.schemaHash)
            try SynchroMeta.archiveSchema(db, version: newSchema.schemaVersion, hash: newSchema.schemaHash, tables: tables)
            try persistLocalSchemaTables(db, tables: tables)
        }
        database.updateApplicationSyncedTables(tables)
    }

    func migrateLocalSchema(newTables: [LocalSchemaTable]) throws {
        try database.writeTransaction { db in
            let version = try SynchroMeta.getInt64(db, key: .schemaVersion)
            let hash = try SynchroMeta.get(db, key: .schemaHash) ?? ""
            guard version > 0, !hash.isEmpty else {
                throw SynchroError.invalidResponse(message: "cannot migrate local schema without verified schema metadata")
            }
            try migrateLocalSchemaInTransaction(db, newTables: newTables)
            try persistLocalSchemaTables(db, tables: newTables)
        }
        database.updateApplicationSyncedTables(newTables)
    }

    private func migrateLocalSchemaInTransaction(
        _ db: GRDB.Database,
        newTables: [LocalSchemaTable]
    ) throws {
        try validateSupportedTransitions(db: db, newTables: newTables)

        for table in newTables {
            let tableExists = try db.tableExists(table.tableName)
            if !tableExists {
                let createSQL = SQLiteSchema.generateCreateTableSQL(table: table)
                try db.execute(sql: createSQL)
            } else {
                let existingColumns = try db.columns(in: table.tableName).map(\.name)
                let existingSet = Set(existingColumns)
                for col in table.columns where !existingSet.contains(col.name) {
                    let sqlType = SQLiteSchema.sqliteType(for: col.logicalType)
                    let quotedTable = SQLiteHelpers.quoteIdentifier(table.tableName)
                    let quotedCol = SQLiteHelpers.quoteIdentifier(col.name)
                    // ALTER TABLE ADD COLUMN in SQLite requires constant defaults for NOT NULL columns.
                    // Non-constant defaults (CURRENT_TIMESTAMP, etc.) are rejected. Adding as nullable
                    // is safe: existing rows get NULL, the server enforces constraints on push.
                    let hasDefault = col.sqliteDefaultSQL != nil && !col.sqliteDefaultSQL!.isEmpty
                    let isConstantDefault = hasDefault && !isNonConstantDefault(col.sqliteDefaultSQL!)
                    var sql = "ALTER TABLE \(quotedTable) ADD COLUMN \(quotedCol) \(sqlType)"
                    if !col.nullable && !col.isPrimaryKey && isConstantDefault {
                        sql += " NOT NULL"
                    }
                    if hasDefault && isConstantDefault {
                        sql += " DEFAULT \(col.sqliteDefaultSQL!)"
                    }
                    try db.execute(sql: sql)
                }
            }

            for index in table.indexes {
                try db.execute(sql: SQLiteSchema.generateIndexSQL(index: index, table: table))
            }

            let triggers = SQLiteSchema.generateCDCTriggers(table: table)
            for trigger in triggers {
                try db.execute(sql: trigger)
            }
        }
    }

    private func applyMigrationJournalInTransaction(
        _ db: GRDB.Database,
        journal: SchemaMigrationJournal
    ) throws -> SchemaMigrationJournal {
        let journal = try journal.validated()
        let active = SchemaRef(
            version: try SynchroMeta.getInt64(db, key: .schemaVersion),
            hash: try SynchroMeta.get(db, key: .schemaHash) ?? ""
        )
        let targetTables = try journal.targetManifest.localTables()

        if active == journal.target {
            guard journal.phase == .applied else {
                throw SynchroError.invalidResponse(message: "schema migration phase does not match active schema")
            }
            try validatePhysicalSchema(db, tables: targetTables)
            return journal
        }

        guard active == journal.source, journal.phase == .prepared else {
            throw SynchroError.invalidResponse(message: "schema migration source does not match active schema")
        }
        let sourceTables = try storedLocalSchema(in: db) ?? []
        let derived = try SchemaMigrationPlan.derive(
            source: journal.source,
            sourceTables: sourceTables,
            targetManifest: journal.targetManifest,
            schemaReset: journal.schemaReset
        )
        guard derived == journal.plan else {
            throw SynchroError.invalidResponse(message: "schema migration plan is not deterministic")
        }
        if journal.source.version > 0 {
            try validatePhysicalSchema(db, tables: sourceTables)
        }
        try executeMigrationPlan(
            db,
            journal: journal,
            sourceTables: sourceTables,
            targetTables: targetTables
        )
        try validatePhysicalSchema(db, tables: targetTables)
        try SynchroMeta.markSchemaMigrationApplied(db)
        return SchemaMigrationJournal(
            source: journal.source,
            targetManifest: journal.targetManifest,
            action: journal.action,
            affectedScopes: journal.affectedScopes,
            scopeCursorUpdates: journal.scopeCursorUpdates,
            plan: journal.plan,
            planHash: journal.planHash,
            phase: .applied,
            schemaReset: journal.schemaReset
        )
    }

    private func executeMigrationPlan(
        _ db: GRDB.Database,
        journal: SchemaMigrationJournal,
        sourceTables: [LocalSchemaTable],
        targetTables: [LocalSchemaTable]
    ) throws {
        let sourceByID = Dictionary(uniqueKeysWithValues: sourceTables.map { ($0.tableID, $0) })
        let targetByID = Dictionary(uniqueKeysWithValues: targetTables.map { ($0.tableID, $0) })

        for operation in journal.plan.operations {
            switch operation.kind {
            case .dropTable:
                guard let tableID = operation.tableID, let table = sourceByID[tableID] else {
                    throw SynchroError.invalidResponse(message: "schema migration drop operation is invalid")
                }
                try dropSyncedTable(db, table: table)

            case .clearSyncedMaterialization:
                try clearSyncedMaterialization(db)

            case .createTable:
                guard let tableID = operation.tableID, let table = targetByID[tableID] else {
                    throw SynchroError.invalidResponse(message: "schema migration create operation is invalid")
                }
                try db.execute(sql: SQLiteSchema.generateCreateTableSQL(table: table))

            case .addColumn:
                guard let tableID = operation.tableID,
                      let fieldID = operation.fieldID,
                      let table = targetByID[tableID],
                      let column = table.columns.first(where: { $0.fieldID == fieldID }) else {
                    throw SynchroError.invalidResponse(message: "schema migration column operation is invalid")
                }
                try addColumn(db, table: table, column: column)

            case .createIndex:
                guard let tableID = operation.tableID,
                      let indexID = operation.indexID,
                      let table = targetByID[tableID],
                      let index = table.indexes.first(where: { $0.indexID == indexID }) else {
                    throw SynchroError.invalidResponse(message: "schema migration index operation is invalid")
                }
                try db.execute(sql: SQLiteSchema.generateIndexSQL(index: index, table: table))

            case .reinstallCapture:
                guard let tableID = operation.tableID, let table = targetByID[tableID] else {
                    throw SynchroError.invalidResponse(message: "schema migration capture operation is invalid")
                }
                for sql in SQLiteSchema.generateCDCTriggers(table: table) {
                    try db.execute(sql: sql)
                }

            case .activateManifest:
                try activateManifest(db, manifest: journal.targetManifest, tables: targetTables)

            case .applyScopeState:
                try SynchroMeta.applyScopeCursorUpdates(
                    db,
                    updates: journal.scopeCursorUpdates,
                    affectedScopes: journal.affectedScopes
                )
            }
        }
    }

    private func addColumn(
        _ db: GRDB.Database,
        table: LocalSchemaTable,
        column: LocalSchemaColumn
    ) throws {
        let sqlType = SQLiteSchema.sqliteType(for: column.logicalType)
        var sql = "ALTER TABLE \(SQLiteHelpers.quoteIdentifier(table.tableName)) ADD COLUMN \(SQLiteHelpers.quoteIdentifier(column.name)) \(sqlType)"
        let hasDefault = column.sqliteDefaultSQL.map { !$0.isEmpty } ?? false
        let constantDefault = hasDefault && !isNonConstantDefault(column.sqliteDefaultSQL!)
        if !column.nullable && !column.isPrimaryKey && constantDefault {
            sql += " NOT NULL"
        }
        if constantDefault {
            sql += " DEFAULT \(column.sqliteDefaultSQL!)"
        }
        try db.execute(sql: sql)
    }

    private func activateManifest(
        _ db: GRDB.Database,
        manifest: SchemaManifest,
        tables: [LocalSchemaTable]
    ) throws {
        try SynchroMeta.setInt64(db, key: .schemaVersion, value: manifest.schemaVersion)
        try SynchroMeta.set(db, key: .schemaHash, value: manifest.schemaHash)
        try SynchroMeta.archiveSchema(db, version: manifest.schemaVersion, hash: manifest.schemaHash, tables: tables)
        try persistLocalSchemaTables(db, tables: tables)
        let encoded = try JSONEncoder.synchroEncoder().encode(manifest)
        guard let manifestJSON = String(data: encoded, encoding: .utf8) else {
            throw SynchroError.invalidResponse(message: "schema manifest is not UTF-8 JSON")
        }
        try SynchroMeta.set(db, key: .schemaManifest, value: manifestJSON)
    }

    private func clearSyncedMaterialization(_ db: GRDB.Database) throws {
        try db.execute(sql: "DELETE FROM _synchro_scope_rows")
        try db.execute(sql: "DELETE FROM _synchro_row_versions")
        try db.execute(sql: "DELETE FROM _synchro_rebuild_page_receipts")
        try db.execute(sql: "DELETE FROM _synchro_rebuild_attempts")
        try db.execute(sql: "DELETE FROM _synchro_backoff")
        try db.execute(sql: "UPDATE _synchro_scopes SET cursor = NULL, checksum = NULL, local_checksum = '', generation = generation + 1")
    }

    private func dropSyncedTable(_ db: GRDB.Database, table: LocalSchemaTable) throws {
        for suffix in ["insert", "update", "delete"] {
            try db.execute(
                sql: "DROP TRIGGER IF EXISTS \(SQLiteHelpers.quoteIdentifier("_synchro_cdc_\(suffix)_\(table.tableName)"))"
            )
        }
        try db.execute(sql: "DROP TABLE IF EXISTS \(SQLiteHelpers.quoteIdentifier(table.tableName))")
    }

    private func validatePhysicalSchema(
        _ db: GRDB.Database,
        tables: [LocalSchemaTable]
    ) throws {
        for table in tables {
            guard try db.tableExists(table.tableName) else {
                throw SynchroError.invalidResponse(message: "schema migration physical table is missing")
            }
            let columns = try db.columns(in: table.tableName)
            let columnsByName = Dictionary(uniqueKeysWithValues: columns.map { ($0.name, $0) })
            for expected in table.columns {
                guard let actual = columnsByName[expected.name],
                      sqliteAffinity(actual.type) == sqliteAffinity(SQLiteSchema.sqliteType(for: expected.logicalType)) else {
                    throw SynchroError.invalidResponse(message: "schema migration physical column is inconsistent")
                }
            }
            let primaryKey = columns
                .filter { $0.primaryKeyIndex > 0 }
                .sorted { $0.primaryKeyIndex < $1.primaryKeyIndex }
                .map(\.name)
            guard primaryKey == table.primaryKey else {
                throw SynchroError.invalidResponse(message: "schema migration physical primary key is inconsistent")
            }
        }
    }

    private func storedLocalSchema(in db: GRDB.Database) throws -> [LocalSchemaTable]? {
        guard let encoded = try SynchroMeta.get(db, key: .localSchema) else {
            return nil
        }
        return try JSONDecoder().decode([LocalSchemaTable].self, from: Data(encoded.utf8))
    }

    private func utf8Less(_ lhs: String, _ rhs: String) -> Bool {
        Array(lhs.utf8).lexicographicallyPrecedes(Array(rhs.utf8))
    }

    private func protectedTableUnion(
        source: [LocalSchemaTable],
        target: [LocalSchemaTable]
    ) -> [LocalSchemaTable] {
        var tables: [String: LocalSchemaTable] = [:]
        for table in source + target {
            tables[table.tableName.lowercased(with: Locale(identifier: "en_US_POSIX"))] = table
        }
        return tables.values.sorted { utf8Less($0.tableName, $1.tableName) }
    }

    /// Returns true if the SQL default expression is non-constant (not allowed in ALTER TABLE ADD COLUMN).
    private func isNonConstantDefault(_ sql: String) -> Bool {
        let upper = sql.uppercased()
        return upper.contains("CURRENT_TIMESTAMP") ||
               upper.contains("CURRENT_DATE") ||
               upper.contains("CURRENT_TIME") ||
               upper.contains("(")
    }

    private func validateSupportedTransitions(
        db: GRDB.Database,
        newTables: [LocalSchemaTable]
    ) throws {
        for table in newTables {
            let tableName = table.tableName
            guard try db.tableExists(tableName) else { continue }
            let existingColumns = try db.columns(in: tableName)
            let existingColumnTypes = Dictionary(uniqueKeysWithValues: existingColumns.map { ($0.name, $0.type) })
            for col in table.columns {
                guard let localType = existingColumnTypes[col.name] else { continue }
                let targetType = SQLiteSchema.sqliteType(for: col.logicalType)
                if sqliteAffinity(localType) != sqliteAffinity(targetType) {
                    throw SynchroError.invalidResponse(
                        message: "unsupported schema transition changes the SQLite type of \(tableName).\(col.name)"
                    )
                }
            }

            let quotedTable = SQLiteHelpers.quoteIdentifier(tableName)
            let primaryKeyRows = try Row.fetchAll(db, sql: "PRAGMA table_info(\(quotedTable))")
            let existingPrimaryKey = primaryKeyRows.compactMap { row -> (position: Int, name: String)? in
                let position: Int = row["pk"] ?? 0
                guard position > 0, let name: String = row["name"] else { return nil }
                return (position, name)
            }.sorted { $0.position < $1.position }.map(\.name)
            if existingPrimaryKey != table.primaryKey {
                throw SynchroError.invalidResponse(
                    message: "unsupported schema transition changes the primary key of \(tableName)"
                )
            }
        }
    }

    private func sqliteAffinity(_ declaredType: String) -> String {
        let type = declaredType.trimmingCharacters(in: .whitespacesAndNewlines).uppercased()
        if type.contains("INT") { return "INTEGER" }
        if type.contains("CHAR") || type.contains("CLOB") || type.contains("TEXT") { return "TEXT" }
        if type.isEmpty || type.contains("BLOB") { return "BLOB" }
        if type.contains("REAL") || type.contains("FLOA") || type.contains("DOUB") { return "REAL" }
        return "NUMERIC"
    }

    func dropSyncedTables(schema: SchemaResponse) throws {
        try database.writeTransaction { db in
            try dropSyncedTablesInTransaction(db, schema: schema)
        }
        database.updateApplicationSyncedTables([])
    }

    func dropSyncedTablesInTransaction(_ db: GRDB.Database, schema: SchemaResponse) throws {
        try dropSyncedTablesInTransaction(db, tables: try schema.localTables())
    }

    func dropSyncedTablesInTransaction(_ db: GRDB.Database, tables: [LocalSchemaTable]) throws {
        for table in tables.reversed() {
            let quoted = SQLiteHelpers.quoteIdentifier(table.tableName)
            let insertTrigger = SQLiteHelpers.quoteIdentifier("_synchro_cdc_insert_\(table.tableName)")
            let updateTrigger = SQLiteHelpers.quoteIdentifier("_synchro_cdc_update_\(table.tableName)")
            let deleteTrigger = SQLiteHelpers.quoteIdentifier("_synchro_cdc_delete_\(table.tableName)")
            try db.execute(sql: "DROP TRIGGER IF EXISTS \(insertTrigger)")
            try db.execute(sql: "DROP TRIGGER IF EXISTS \(updateTrigger)")
            try db.execute(sql: "DROP TRIGGER IF EXISTS \(deleteTrigger)")
            try db.execute(sql: "DROP TABLE IF EXISTS \(quoted)")
        }
    }

    private func persistLocalSchemaTables(_ db: GRDB.Database, tables: [LocalSchemaTable]) throws {
        let encoded = try JSONEncoder().encode(tables)
        try SynchroMeta.set(
            db,
            key: .localSchema,
            value: String(data: encoded, encoding: .utf8) ?? "[]"
        )
    }

    private func recomputeRetainedScopeIntegrity(
        _ db: GRDB.Database,
        scopeID: String,
        schemaHash: String,
        tables: [LocalSchemaTable]
    ) throws {
        guard try SynchroMeta.getScope(db, scopeID: scopeID) != nil else {
            throw SynchroError.invalidResponse(message: "scope cursor update targets an unknown scope \(scopeID)")
        }
        let tablesByName = Dictionary(uniqueKeysWithValues: tables.map { ($0.tableName, $0) })
        let scopeRows = try SynchroMeta.getScopeRowRecordIDs(db, scopeID: scopeID)
        var entries: [(identity: Data, digest: ChecksumObject)] = []
        entries.reserveCapacity(scopeRows.count)

        for scopeRow in scopeRows {
            guard let table = tablesByName[scopeRow.tableName] else {
                throw SynchroError.invalidResponse(message: "scope references unknown table \(scopeRow.tableName)")
            }
            let row = try loadWireRow(db, table: table, recordID: scopeRow.recordID)
            guard let primaryKey = row[table.primaryKeyFieldID] else {
                throw SynchroError.invalidResponse(message: "scope row lacks its primary key field")
            }
            guard let serverVersion = try SynchroMeta.getRowVersion(
                db,
                tableName: table.tableName,
                recordID: scopeRow.recordID
            ) else {
                throw SynchroError.invalidResponse(message: "scope row has no server version")
            }
            let computed = try Integrity.rowDigest(
                schemaHash: schemaHash,
                table: table,
                pk: [table.primaryKeyFieldID: primaryKey],
                row: row,
                serverVersion: serverVersion
            )
            try SynchroMeta.updateScopeRowChecksum(
                db,
                scopeID: scopeID,
                tableName: scopeRow.tableName,
                recordID: scopeRow.recordID,
                checksum: computed.checksum.digest
            )
            entries.append((identity: computed.identity, digest: computed.checksum))
        }

        let localChecksum = try Integrity.scopeDigest(
            schemaHash: schemaHash,
            scopeID: scopeID,
            entries: entries
        )
        let encoded = try JSONEncoder.synchroEncoder().encode(localChecksum)
        guard let checksumJSON = String(data: encoded, encoding: .utf8) else {
            throw SynchroError.invalidResponse(message: "scope checksum is not UTF-8 JSON")
        }
        try SynchroMeta.setScopeLocalChecksum(db, scopeID: scopeID, checksum: checksumJSON)
    }

    private func loadWireRow(
        _ db: GRDB.Database,
        table: LocalSchemaTable,
        recordID: String
    ) throws -> [String: AnyCodable] {
        let columns = table.columns.map { SQLiteHelpers.quoteIdentifier($0.name) }.joined(separator: ", ")
        let primaryKey = SQLiteHelpers.quoteIdentifier(table.primaryKey.first ?? "id")
        let relation = SQLiteHelpers.quoteIdentifier(table.tableName)
        guard let row = try Row.fetchOne(
            db,
            sql: "SELECT \(columns) FROM \(relation) WHERE \(primaryKey) = ?",
            arguments: [recordID]
        ) else {
            throw SynchroError.invalidResponse(message: "scope provenance references a missing row")
        }
        return try Dictionary(uniqueKeysWithValues: table.columns.map { column in
            let value: DatabaseValue = row[column.name]
            return (column.fieldID, try wireValue(value, column: column))
        })
    }

    private func wireValue(_ value: DatabaseValue, column: LocalSchemaColumn) throws -> AnyCodable {
        switch value.storage {
        case .null:
            return AnyCodable(NSNull())
        case .int64(let value):
            switch column.logicalType {
            case "boolean": return AnyCodable(value != 0)
            case "int64": return AnyCodable(String(value))
            default: return AnyCodable(value)
            }
        case .double(let value):
            return AnyCodable(value)
        case .string(let value):
            return AnyCodable(value)
        case .blob(let value):
            return AnyCodable(
                value.base64EncodedString()
                    .replacingOccurrences(of: "+", with: "-")
                    .replacingOccurrences(of: "/", with: "_")
                    .replacingOccurrences(of: "=", with: "")
            )
        }
    }
}
