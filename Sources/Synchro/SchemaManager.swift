import Foundation
import GRDB

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

        if localVersion == 0 {
            try createSyncedTables(schema: schema)
        } else {
            try migrateSchema(newSchema: schema)
        }

        try database.writeTransaction { db in
            try SynchroMeta.setInt64(db, key: .schemaVersion, value: schema.schemaVersion)
            try SynchroMeta.set(db, key: .schemaHash, value: schema.schemaHash)
        }

        return schema
    }

    func createSyncedTables(schema: SchemaResponse) throws {
        try database.writeTransaction { db in
            try createSyncedTablesInTransaction(db, schema: schema)
        }
    }

    func createSyncedTablesInTransaction(_ db: GRDB.Database, schema: SchemaResponse) throws {
        for table in schema.tables {
            let createSQL = SQLiteSchema.generateCreateTableSQL(table: table)
            try db.execute(sql: createSQL)

            let triggers = SQLiteSchema.generateCDCTriggers(table: table)
            for trigger in triggers {
                try db.execute(sql: trigger)
            }
        }
    }

    func migrateSchema(newSchema: SchemaResponse) throws {
        try database.writeTransaction { db in
            let localTables = try String.fetchAll(
                db,
                sql: "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '_synchro_%'"
            )
            let newTableMap = Dictionary(uniqueKeysWithValues: newSchema.tables.map { ($0.tableName, $0) })
            if try requiresDestructiveRebuild(localTables: localTables, newTableMap: newTableMap, db: db) {
                try db.execute(sql: "DELETE FROM _synchro_pending_changes")
                try dropSyncedTablesInTransaction(db, schema: SchemaResponse(schemaVersion: 0, schemaHash: "", serverTime: Date(), tables: newSchema.tables))
                try createSyncedTablesInTransaction(db, schema: newSchema)
                try SynchroMeta.setInt64(db, key: .checkpoint, value: 0)
                try SynchroMeta.set(db, key: .knownBuckets, value: "[]")
                try SynchroMeta.set(db, key: .snapshotComplete, value: "0")
                return
            }

            for table in newSchema.tables {
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
                        var sql = "ALTER TABLE \(quotedTable) ADD COLUMN \(quotedCol) \(sqlType)"
                        if !col.nullable && !col.isPrimaryKey {
                            sql += " NOT NULL"
                        }
                        if let defaultSQL = col.sqliteDefaultSQL, !defaultSQL.isEmpty {
                            sql += " DEFAULT \(defaultSQL)"
                        }
                        try db.execute(sql: sql)
                    }
                }

                let triggers = SQLiteSchema.generateCDCTriggers(table: table)
                for trigger in triggers {
                    try db.execute(sql: trigger)
                }
            }
        }
    }

    private func requiresDestructiveRebuild(localTables: [String], newTableMap: [String: SchemaTable], db: GRDB.Database) throws -> Bool {
        for localTable in localTables where newTableMap[localTable] == nil {
            return true
        }
        for (tableName, table) in newTableMap {
            guard try db.tableExists(tableName) else { continue }
            let existingColumns = Set(try db.columns(in: tableName).map(\.name))
            let newColumns = Set(table.columns.map(\.name))
            if !existingColumns.isSubset(of: newColumns) {
                return true
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
        for table in schema.tables.reversed() {
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
}
