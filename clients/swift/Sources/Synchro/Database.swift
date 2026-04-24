import Foundation
@preconcurrency import GRDB

public typealias Row = GRDB.Row

private struct ObservedRows: @unchecked Sendable {
    let rows: [Row]
}

private struct ObservedQueryParams: @unchecked Sendable {
    let values: [(any DatabaseValueConvertible)?]?
}

final class SynchroDatabase: @unchecked Sendable {
    let dbPool: DatabasePool
    let path: String

    init(path: String) throws {
        self.path = path
        var config = Configuration()
        config.journalMode = .wal
        self.dbPool = try DatabasePool(path: path, configuration: config)
        try runMigrations()
    }

    // MARK: - Queries

    func query(_ sql: String, params: [(any DatabaseValueConvertible)?]?) throws -> [Row] {
        try dbPool.read { db in
            try Row.fetchAll(db, sql: sql, arguments: StatementArguments(params ?? []))
        }
    }

    func queryOne(_ sql: String, params: [(any DatabaseValueConvertible)?]?) throws -> Row? {
        try dbPool.read { db in
            try Row.fetchOne(db, sql: sql, arguments: StatementArguments(params ?? []))
        }
    }

    func execute(_ sql: String, params: [(any DatabaseValueConvertible)?]?) throws -> ExecResult {
        try dbPool.write { db in
            try db.execute(sql: sql, arguments: StatementArguments(params ?? []))
            return ExecResult(rowsAffected: db.changesCount)
        }
    }

    // MARK: - Transactions

    func readTransaction<T>(_ block: (GRDB.Database) throws -> T) throws -> T {
        try dbPool.read { db in
            try block(db)
        }
    }

    func writeTransaction<T>(_ block: (GRDB.Database) throws -> T) throws -> T {
        try dbPool.write { db in
            try block(db)
        }
    }

    func writeSyncLockedTransaction<T>(_ block: (GRDB.Database) throws -> T) throws -> T {
        try dbPool.write { db in
            try SynchroMeta.setSyncLock(db, locked: true)
            do {
                let result = try block(db)
                try SynchroMeta.setSyncLock(db, locked: false)
                return result
            } catch {
                try? SynchroMeta.setSyncLock(db, locked: false)
                throw error
            }
        }
    }

    // MARK: - Batch

    func executeBatch(_ statements: [SQLStatement]) throws -> Int {
        try dbPool.write { db in
            var total = 0
            for stmt in statements {
                try db.execute(sql: stmt.sql, arguments: StatementArguments(stmt.params ?? []))
                total += db.changesCount
            }
            return total
        }
    }

    // MARK: - Schema (local-only tables)

    func createTable(_ name: String, columns: [ColumnDef], options: TableOptions?) throws {
        let ifNotExists = options?.ifNotExists ?? true
        let withoutRowid = options?.withoutRowid ?? false
        let quotedName = SQLiteHelpers.quoteIdentifier(name)

        var colDefs: [String] = []
        for col in columns {
            var def = "\(SQLiteHelpers.quoteIdentifier(col.name)) \(col.type)"
            if col.primaryKey { def += " PRIMARY KEY" }
            if !col.nullable { def += " NOT NULL" }
            if let defaultVal = col.defaultValue { def += " DEFAULT \(defaultVal)" }
            colDefs.append(def)
        }

        var sql = "CREATE TABLE"
        if ifNotExists { sql += " IF NOT EXISTS" }
        sql += " \(quotedName) (\(colDefs.joined(separator: ", ")))"
        if withoutRowid { sql += " WITHOUT ROWID" }

        try dbPool.write { db in
            try db.execute(sql: sql)
        }
    }

    func alterTable(_ name: String, addColumns: [ColumnDef]) throws {
        let quotedName = SQLiteHelpers.quoteIdentifier(name)
        try dbPool.write { db in
            for col in addColumns {
                var def = "ALTER TABLE \(quotedName) ADD COLUMN \(SQLiteHelpers.quoteIdentifier(col.name)) \(col.type)"
                if !col.nullable { def += " NOT NULL DEFAULT ''" }
                if let defaultVal = col.defaultValue { def += " DEFAULT \(defaultVal)" }
                try db.execute(sql: def)
            }
        }
    }

    func createIndex(_ table: String, columns: [String], unique: Bool) throws {
        let quotedTable = SQLiteHelpers.quoteIdentifier(table)
        let quotedCols = columns.map { SQLiteHelpers.quoteIdentifier($0) }.joined(separator: ", ")
        let indexName = SQLiteHelpers.quoteIdentifier("idx_\(table)_\(columns.joined(separator: "_"))")
        let uniqueStr = unique ? "UNIQUE " : ""
        let sql = "CREATE \(uniqueStr)INDEX IF NOT EXISTS \(indexName) ON \(quotedTable) (\(quotedCols))"
        try dbPool.write { db in
            try db.execute(sql: sql)
        }
    }

    // MARK: - Observation

    func onChange(tables: [String], callback: @escaping () -> Void) -> DatabaseCancellable {
        let observation = DatabaseRegionObservation(tracking: tables.map { Table($0) })
        let cancellable = observation.start(in: dbPool, onError: { error in
            #if DEBUG
            print("[Synchro] onChange observation error: \(error)")
            #endif
        }, onChange: { _ in
            callback()
        })
        return cancellable
    }

    func watch(_ sql: String, params: [(any DatabaseValueConvertible)?]?, tables: [String], callback: @escaping ([Row]) -> Void) -> DatabaseCancellable {
        let observedParams = ObservedQueryParams(values: params)
        let observation = ValueObservation.tracking(regions: tables.map { Table($0) }, fetch: { db -> ObservedRows in
            ObservedRows(rows: try Row.fetchAll(db, sql: sql, arguments: StatementArguments(observedParams.values ?? [])))
        })
        let cancellable = observation.start(in: dbPool, onError: { error in
            // Observation errors are non-fatal; the observation continues.
            // In production, wire this to your logging infrastructure.
            #if DEBUG
            print("[Synchro] watch observation error: \(error)")
            #endif
        }, onChange: { observed in
            callback(observed.rows)
        })
        return cancellable
    }

    // MARK: - Close

    func close() throws {
        try dbPool.close()
    }

    // MARK: - Migrations

    private func runMigrations() throws {
        var migrator = DatabaseMigrator()
        migrator.registerMigration("synchro_v1") { db in
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS _synchro_pending_changes (
                    record_id TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    base_updated_at TEXT,
                    client_updated_at TEXT NOT NULL,
                    PRIMARY KEY (table_name, record_id)
                )
                """)

            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS _synchro_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """)

            try db.execute(sql: """
                INSERT OR IGNORE INTO _synchro_meta (key, value) VALUES ('sync_lock', '0')
                """)
            try db.execute(sql: """
                INSERT OR IGNORE INTO _synchro_meta (key, value) VALUES ('checkpoint', '0')
                """)
        }
        migrator.registerMigration("synchro_v2_buckets") { db in
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS _synchro_bucket_members (
                    bucket_id TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    record_id TEXT NOT NULL,
                    checksum INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (bucket_id, table_name, record_id)
                )
                """)

            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS _synchro_bucket_checkpoints (
                    bucket_id TEXT PRIMARY KEY,
                    checkpoint INTEGER NOT NULL DEFAULT 0
                )
                """)
        }
        migrator.registerMigration("synchro_v3_scopes") { db in
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS _synchro_scopes (
                    scope_id TEXT PRIMARY KEY,
                    cursor TEXT,
                    checksum TEXT,
                    generation INTEGER NOT NULL DEFAULT 0,
                    local_checksum INTEGER NOT NULL DEFAULT 0
                )
                """)

            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS _synchro_scope_rows (
                    scope_id TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    record_id TEXT NOT NULL,
                    checksum INTEGER NOT NULL DEFAULT 0,
                    generation INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (scope_id, table_name, record_id)
                )
                """)

            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_synchro_scope_rows_record
                ON _synchro_scope_rows (table_name, record_id)
                """)
        }
        migrator.registerMigration("synchro_v4_scope_integrity") { db in
            let scopeColumns = try db.columns(in: "_synchro_scopes").map(\.name)
            if !scopeColumns.contains("local_checksum") {
                try db.execute(sql: """
                    ALTER TABLE _synchro_scopes
                    ADD COLUMN local_checksum INTEGER NOT NULL DEFAULT 0
                    """)
            }

            let scopeRowColumns = try db.columns(in: "_synchro_scope_rows").map(\.name)
            if !scopeRowColumns.contains("checksum") {
                try db.execute(sql: """
                    ALTER TABLE _synchro_scope_rows
                    ADD COLUMN checksum INTEGER NOT NULL DEFAULT 0
                    """)
            }

            try db.execute(sql: """
                UPDATE _synchro_scopes
                SET cursor = NULL,
                    checksum = NULL,
                    generation = 0,
                    local_checksum = 0
                """)
            try db.execute(sql: "DELETE FROM _synchro_scope_rows")
        }
        migrator.registerMigration("synchro_v5_rejected_mutations") { db in
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS _synchro_rejected_mutations (
                    mutation_id TEXT PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    record_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    code TEXT NOT NULL,
                    message TEXT,
                    server_row_json TEXT,
                    server_version TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """)

            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_synchro_rejected_mutations_record
                ON _synchro_rejected_mutations (table_name, record_id)
                """)
        }
        try migrator.migrate(dbPool)
    }
}
