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

        try database.writeTransaction { db in
            try SynchroMeta.setInt64(db, key: .schemaVersion, value: schema.schemaVersion)
            try SynchroMeta.set(db, key: .schemaHash, value: schema.schemaHash)
        }

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

    func reconcileLocalSchema(schemaVersion: Int64, schemaHash: String, tables: [LocalSchemaTable]) throws {
        let (localVersion, localHash) = try database.readTransaction { db in
            let version = try SynchroMeta.getInt64(db, key: .schemaVersion)
            let hash = try SynchroMeta.get(db, key: .schemaHash) ?? ""
            return (version, hash)
        }

        if localVersion != schemaVersion || localHash != schemaHash {
            try migrateLocalSchema(newTables: tables)
        }

        try database.writeTransaction { db in
            try SynchroMeta.setInt64(db, key: .schemaVersion, value: schemaVersion)
            try SynchroMeta.set(db, key: .schemaHash, value: schemaHash)
            try persistLocalSchemaTables(db, tables: tables)
        }
    }

    func createSyncedTables(schema: SchemaResponse) throws {
        try database.writeTransaction { db in
            try createSyncedTablesInTransaction(db, schema: schema)
        }
    }

    func createSyncedTablesInTransaction(_ db: GRDB.Database, schema: SchemaResponse) throws {
        try createSyncedTablesInTransaction(db, tables: try schema.localTables())
    }

    func createSyncedTablesInTransaction(_ db: GRDB.Database, tables: [LocalSchemaTable]) throws {
        for table in tables {
            let createSQL = SQLiteSchema.generateCreateTableSQL(table: table)
            try db.execute(sql: createSQL)

            let triggers = SQLiteSchema.generateCDCTriggers(table: table)
            for trigger in triggers {
                try db.execute(sql: trigger)
            }
        }
    }

    func migrateSchema(newSchema: SchemaResponse) throws {
        try migrateLocalSchema(newTables: try newSchema.localTables())
    }

    func migrateLocalSchema(newTables: [LocalSchemaTable]) throws {
        try database.writeTransaction { db in
            if try requiresDestructiveRebuild(db: db, newTables: newTables) {
                try db.execute(sql: "DELETE FROM _synchro_pending_changes")
                try SynchroMeta.invalidateAllScopes(db)
                try dropSyncedTablesInTransaction(db, tables: newTables)
                try createSyncedTablesInTransaction(db, tables: newTables)
                try SynchroMeta.setInt64(db, key: .checkpoint, value: 0)
                try SynchroMeta.set(db, key: .knownBuckets, value: "[]")
                try SynchroMeta.set(db, key: .snapshotComplete, value: "0")
                try persistLocalSchemaTables(db, tables: newTables)
                return
            }

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

                let triggers = SQLiteSchema.generateCDCTriggers(table: table)
                for trigger in triggers {
                    try db.execute(sql: trigger)
                }
            }

            try persistLocalSchemaTables(db, tables: newTables)
        }
    }

    /// Returns true if the SQL default expression is non-constant (not allowed in ALTER TABLE ADD COLUMN).
    private func isNonConstantDefault(_ sql: String) -> Bool {
        let upper = sql.uppercased()
        return upper.contains("CURRENT_TIMESTAMP") ||
               upper.contains("CURRENT_DATE") ||
               upper.contains("CURRENT_TIME") ||
               upper.contains("(")
    }

    /// Only triggers destructive rebuild when a synced column's type has changed.
    /// Extra local tables, extra local columns, and removed server columns are all preserved.
    private func requiresDestructiveRebuild(db: GRDB.Database, newTables: [LocalSchemaTable]) throws -> Bool {
        let newTableMap = Dictionary(uniqueKeysWithValues: newTables.map { ($0.tableName, $0) })
        for (tableName, table) in newTableMap {
            guard try db.tableExists(tableName) else { continue }
            let existingColumnTypes = Dictionary(
                uniqueKeysWithValues: try db.columns(in: tableName).map { ($0.name, $0.type.uppercased()) }
            )
            for col in table.columns {
                guard let localType = existingColumnTypes[col.name] else { continue }
                let serverType = SQLiteSchema.sqliteType(for: col.logicalType).uppercased()
                if localType != serverType {
                    return true
                }
            }
        }
        return false
    }

    func dropSyncedTables(schema: SchemaResponse) throws {
        try database.writeTransaction { db in
            try dropSyncedTablesInTransaction(db, schema: schema)
        }
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
}
