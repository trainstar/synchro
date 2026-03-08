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
            for table in schema.tables {
                let createSQL = SQLiteSchema.generateCreateTableSQL(table: table)
                try db.execute(sql: createSQL)

                let triggers = SQLiteSchema.generateCDCTriggers(table: table)
                for trigger in triggers {
                    try db.execute(sql: trigger)
                }
            }
        }
    }

    func migrateSchema(newSchema: SchemaResponse) throws {
        try database.writeTransaction { db in
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
                        try db.execute(sql: "ALTER TABLE \(quotedTable) ADD COLUMN \(quotedCol) \(sqlType)")
                    }
                }

                let triggers = SQLiteSchema.generateCDCTriggers(table: table)
                for trigger in triggers {
                    try db.execute(sql: trigger)
                }
            }
        }
    }

    func dropSyncedTables(schema: SchemaResponse) throws {
        try database.writeTransaction { db in
            for table in schema.tables.reversed() {
                let quoted = SQLiteHelpers.quoteIdentifier(table.tableName)
                try db.execute(sql: "DROP TRIGGER IF EXISTS _synchro_cdc_insert_\(table.tableName)")
                try db.execute(sql: "DROP TRIGGER IF EXISTS _synchro_cdc_update_\(table.tableName)")
                try db.execute(sql: "DROP TRIGGER IF EXISTS _synchro_cdc_delete_\(table.tableName)")
                try db.execute(sql: "DROP TABLE IF EXISTS \(quoted)")
            }
        }
    }
}
