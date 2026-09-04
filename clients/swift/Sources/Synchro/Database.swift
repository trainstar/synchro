import Foundation
import os
@preconcurrency import GRDB

public typealias Row = GRDB.Row

private struct ObservedRows: @unchecked Sendable {
    let rows: [Row]
}

private struct ObservedQueryParams: @unchecked Sendable {
    let values: [(any DatabaseValueConvertible)?]?
}

private final class ProvenanceMaintenanceWorkObserver: TransactionObserver, @unchecked Sendable {
    private static let tableName = "_synchro_scope_rows"

    private let lock = NSLock()
    private var pendingEventCount: Int64 = 0
    private var committedCursor: Int64 = 0

    func observes(eventsOfKind eventKind: DatabaseEventKind) -> Bool {
        guard eventKind.tableName == Self.tableName else { return false }
        switch eventKind {
        case .insert, .update, .delete:
            return true
        }
    }

    func databaseDidChange() {
        // Only row-level events contribute to this cursor.
    }

    func databaseDidChange(with event: DatabaseEvent) {
        guard event.tableName == Self.tableName else { return }
        switch event.kind {
        case .insert, .update, .delete:
            lock.lock()
            if pendingEventCount < Int64.max {
                pendingEventCount += 1
            }
            lock.unlock()
        }
    }

    func databaseDidCommit(_ db: GRDB.Database) {
        _ = db
        lock.lock()
        if Int64.max - committedCursor < pendingEventCount {
            committedCursor = Int64.max
        } else {
            committedCursor += pendingEventCount
        }
        pendingEventCount = 0
        lock.unlock()
    }

    func databaseDidRollback(_ db: GRDB.Database) {
        _ = db
        lock.lock()
        pendingEventCount = 0
        lock.unlock()
    }

    func cursor() -> Int64 {
        lock.lock()
        defer { lock.unlock() }
        return committedCursor
    }
}

final class SynchroDatabase: @unchecked Sendable {
    let dbPool: DatabasePool
    let path: String
    private let applicationPolicy = ApplicationSQLPolicy()
    private let provenanceMaintenanceWorkObserver: ProvenanceMaintenanceWorkObserver
    private var applicationDatabase: ApplicationDatabase!
    private let changeObservers = OSAllocatedUnfairLock(initialState: [UUID: () -> Void]())

    init(path: String) throws {
        self.path = path
        let provenanceMaintenanceWorkObserver = ProvenanceMaintenanceWorkObserver()
        self.provenanceMaintenanceWorkObserver = provenanceMaintenanceWorkObserver
        var config = Configuration()
        config.journalMode = .wal
        config.busyMode = .timeout(5)
        config.prepareDatabase { database in
            database.add(
                transactionObserver: provenanceMaintenanceWorkObserver,
                extent: .databaseLifetime
            )
        }
        self.dbPool = try DatabasePool(path: path, configuration: config)
        try runMigrations()
        let storedTables = try dbPool.read { db -> [LocalSchemaTable] in
            guard let encoded = try SynchroMeta.get(db, key: .localSchema) else {
                return []
            }
            return try JSONDecoder().decode([LocalSchemaTable].self, from: Data(encoded.utf8))
        }
        applicationPolicy.updateSyncedTables(storedTables)
        applicationDatabase = try ApplicationDatabase(path: path, policy: applicationPolicy)
        applicationDatabase.updateSyncedWritableColumns(storedTables)
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
        let result = try applicationDatabase.write { transaction in
            try transaction.execute(sql, params: params)
        }
        notifyDatabaseChange()
        return result
    }

    // MARK: - Transactions

    func readTransaction<T>(_ block: (GRDB.Database) throws -> T) throws -> T {
        try dbPool.read { db in
            try block(db)
        }
    }

    func writeTransaction<T>(_ block: (GRDB.Database) throws -> T) throws -> T {
        let result = try dbPool.write { db in
            try block(db)
        }
        notifyDatabaseChange()
        return result
    }

    func writeSyncLockedTransaction<T>(_ block: (GRDB.Database) throws -> T) throws -> T {
        let result = try dbPool.write { db in
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
        notifyDatabaseChange()
        return result
    }

    func stateInspectionTransaction<T>(
        _ block: (GRDB.Database, Int64) throws -> T
    ) throws -> T {
        try dbPool.write { db in
            try block(db, provenanceMaintenanceWorkObserver.cursor())
        }
    }

    func applicationWriteTransaction<T>(
        _ block: (ApplicationTransaction) throws -> T
    ) throws -> T {
        let result = try applicationDatabase.write(block)
        notifyDatabaseChange()
        return result
    }

    func applicationWritePreparedStatement<T>(
        _ sql: String,
        _ block: (Statement) throws -> T
    ) throws -> T {
        let result = try applicationDatabase.withPreparedStatement(sql: sql, body: block)
        notifyDatabaseChange()
        return result
    }

    func updateApplicationSyncedTables(_ tables: [LocalSchemaTable]) {
        applicationPolicy.updateSyncedTables(tables)
        applicationDatabase.updateSyncedWritableColumns(tables)
    }

    func applicationAuthoredWriteTransaction<T>(
        tableName: String,
        operation: String,
        columnNames: [String],
        _ block: (ApplicationTransaction) throws -> T
    ) throws -> T {
        guard ["insert", "update", "delete"].contains(operation.lowercased()) else {
            throw SynchroError.invalidResponse(message: "authored write operation is invalid")
        }
        guard !tableName.isEmpty else {
            throw SynchroError.invalidResponse(message: "authored write table name must not be empty")
        }
        guard columnNames.allSatisfy({ !$0.isEmpty }) else {
            throw SynchroError.invalidResponse(message: "authored write column names must not be empty")
        }
        guard Set(columnNames).count == columnNames.count else {
            throw SynchroError.invalidResponse(message: "authored write column names must be unique")
        }
        let result = try applicationDatabase.write(
            context: .authored(
                tableName: tableName,
                operation: operation.lowercased(),
                columnNames: columnNames
            ),
            block
        )
        notifyDatabaseChange()
        return result
    }

    // MARK: - Batch

    func executeBatch(_ statements: [SQLStatement]) throws -> Int {
        let total = try applicationDatabase.write { transaction in
            var total = 0
            for stmt in statements {
                total += try transaction.execute(stmt.sql, params: stmt.params).rowsAffected
            }
            return total
        }
        notifyDatabaseChange()
        return total
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

        _ = try execute(sql, params: nil)
    }

    func alterTable(_ name: String, addColumns: [ColumnDef]) throws {
        let quotedName = SQLiteHelpers.quoteIdentifier(name)
        try applicationWriteTransaction { transaction in
            for col in addColumns {
                var def = "ALTER TABLE \(quotedName) ADD COLUMN \(SQLiteHelpers.quoteIdentifier(col.name)) \(col.type)"
                if !col.nullable { def += " NOT NULL DEFAULT ''" }
                if let defaultVal = col.defaultValue { def += " DEFAULT \(defaultVal)" }
                try transaction.execute(def)
            }
        }
    }

    func createIndex(_ table: String, columns: [String], unique: Bool) throws {
        let quotedTable = SQLiteHelpers.quoteIdentifier(table)
        let quotedCols = columns.map { SQLiteHelpers.quoteIdentifier($0) }.joined(separator: ", ")
        let indexName = SQLiteHelpers.quoteIdentifier("idx_\(table)_\(columns.joined(separator: "_"))")
        let uniqueStr = unique ? "UNIQUE " : ""
        let sql = "CREATE \(uniqueStr)INDEX IF NOT EXISTS \(indexName) ON \(quotedTable) (\(quotedCols))"
        _ = try execute(sql, params: nil)
    }

    // MARK: - Observation

    func onChange(tables: [String], callback: @escaping () -> Void) -> DatabaseCancellable {
        _ = tables
        let id = UUID()
        changeObservers.withLock { $0[id] = callback }
        return AnyDatabaseCancellable { [weak self] in
            _ = self?.changeObservers.withLock { $0.removeValue(forKey: id) }
        }
    }

    func watch(_ sql: String, params: [(any DatabaseValueConvertible)?]?, tables: [String], callback: @escaping ([Row]) -> Void) -> DatabaseCancellable {
        _ = tables
        if let rows = try? query(sql, params: params) {
            callback(rows)
        }
        return onChange(tables: tables) { [weak self] in
            guard let self, let rows = try? self.query(sql, params: params) else { return }
            callback(rows)
        }
    }

    // MARK: - Close

    func close() throws {
        try applicationDatabase.close()
        try dbPool.writeWithoutTransaction { db in
            try db.execute(sql: "PRAGMA wal_checkpoint(TRUNCATE)")
        }
        try dbPool.close()
    }

    private func notifyDatabaseChange() {
        let callbacks = changeObservers.withLock { Array($0.values) }
        for callback in callbacks {
            callback()
        }
    }

    // MARK: - Migrations

    private func runMigrations() throws {
        var migrator = DatabaseMigrator()
        // Keep the shipped migration identifiers. Existing installations can have
        // any prefix of this chain recorded in _grdb_migrations.
        migrator.registerMigration("synchro_v1") { db in
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS _synchro_pending_changes (
                    record_id TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    base_updated_at TEXT,
                    client_updated_at TEXT NOT NULL,
                    local_revision INTEGER NOT NULL DEFAULT 0,
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
                INSERT OR IGNORE INTO _synchro_meta (key, value)
                VALUES ('sync_lock', '0')
                """)
            try db.execute(sql: """
                INSERT OR IGNORE INTO _synchro_meta (key, value)
                VALUES ('checkpoint', '0')
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
                SET cursor = NULL, checksum = NULL, generation = 0, local_checksum = 0
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

        migrator.registerMigration("synchro_v6_protocol_3") { db in
            // Protocol 2 scope positions and numeric checksums are not resumable
            // under protocol 3. Authored queue and rejection records stay intact.
            try db.execute(sql: """
                UPDATE _synchro_scopes
                SET cursor = NULL, checksum = NULL, generation = 0, local_checksum = ''
                """)
            try db.execute(sql: "DELETE FROM _synchro_scope_rows")
            try db.execute(sql: "DROP TABLE IF EXISTS _synchro_bucket_members")
            try db.execute(sql: "DROP TABLE IF EXISTS _synchro_bucket_checkpoints")
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS _synchro_scopes (
                    scope_id TEXT PRIMARY KEY,
                    cursor TEXT,
                    checksum TEXT,
                    generation INTEGER NOT NULL DEFAULT 0,
                    local_checksum TEXT NOT NULL DEFAULT ''
                )
                """)

            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS _synchro_scope_rows (
                    scope_id TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    record_id TEXT NOT NULL,
                    checksum TEXT NOT NULL,
                    generation INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (scope_id, table_name, record_id)
                )
                """)

            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_synchro_scope_rows_record
                ON _synchro_scope_rows (table_name, record_id)
                """)

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

            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS _synchro_row_versions (
                    table_name TEXT NOT NULL,
                    record_id TEXT NOT NULL,
                    server_version TEXT NOT NULL,
                    row_checksum TEXT,
                    PRIMARY KEY (table_name, record_id)
                )
                """)
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS _synchro_seed_receipts (
                    scope_id TEXT PRIMARY KEY,
                    receipt TEXT NOT NULL,
                    schema_version INTEGER NOT NULL,
                    schema_hash TEXT NOT NULL,
                    cardinality INTEGER NOT NULL,
                    checksum TEXT NOT NULL
                )
                """)
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS _synchro_rebuild_attempts (
                    scope_id TEXT PRIMARY KEY,
                    rebuild_id TEXT NOT NULL,
                    client_generation INTEGER NOT NULL,
                    schema_version INTEGER NOT NULL,
                    schema_hash TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    cursor TEXT,
                    page_limit INTEGER NOT NULL
                )
                """)
            try db.execute(sql: "DELETE FROM _synchro_rebuild_attempts")
        }
        migrator.registerMigration("synchro_v7_pending_local_revision") { db in
            let columns = try db.columns(in: "_synchro_pending_changes").map(\.name)
            if !columns.contains("local_revision") {
                try db.execute(sql: """
                    ALTER TABLE _synchro_pending_changes
                    ADD COLUMN local_revision INTEGER NOT NULL DEFAULT 0
                """)
            }
        }
        migrator.registerMigration("synchro_v8_sealed_push_batches") { db in
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS _synchro_push_batches (
                    batch_id TEXT PRIMARY KEY,
                    request_json TEXT NOT NULL,
                    pending_json TEXT NOT NULL,
                    schema_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    completed_at TEXT
                )
                """)
        }
        migrator.registerMigration("synchro_v9_mutation_ledger") { db in
            // The old table coalesced one mutable row per application record.  Keep
            // its rows as blocked diagnostics, then replace it with an append-only
            // ledger.  A legacy row has no authored fields, so this migration never
            // invents a payload for it.
            let hadLegacyQueue = try db.tableExists("_synchro_pending_changes")
            var legacyRows: [(recordID: String, tableName: String, operation: String, baseVersion: String?, clientVersion: String, localRevision: Int64)] = []
            if hadLegacyQueue {
                let rows = try Row.fetchAll(
                    db,
                    sql: "SELECT record_id, table_name, operation, base_updated_at, client_updated_at, local_revision FROM _synchro_pending_changes ORDER BY client_updated_at, table_name, record_id"
                )
                legacyRows = rows.compactMap { row in
                    guard let recordID: String = row["record_id"],
                          let tableName: String = row["table_name"],
                          let operation: String = row["operation"],
                          let clientVersion: String = row["client_updated_at"] else {
                        return nil
                    }
                    return (
                        recordID: recordID,
                        tableName: tableName,
                        operation: operation,
                        baseVersion: row["base_updated_at"],
                        clientVersion: clientVersion,
                        localRevision: row["local_revision"] ?? 0
                    )
                }
                try db.execute(sql: "DROP TABLE _synchro_pending_changes")
            }
            let oldCaptureTriggers = try String.fetchAll(
                db,
                sql: "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name LIKE '_synchro_cdc_%'"
            )
            for triggerName in oldCaptureTriggers {
                try db.execute(sql: "DROP TRIGGER IF EXISTS \(SQLiteHelpers.quoteIdentifier(triggerName))")
            }

            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS _synchro_pending_changes (
                    local_order INTEGER PRIMARY KEY AUTOINCREMENT,
                    mutation_id TEXT NOT NULL UNIQUE,
                    capture_uuid TEXT,
                    table_id TEXT,
                    table_name TEXT NOT NULL,
                    record_id TEXT NOT NULL,
                    pk_field_id TEXT,
                    pk_logical_type TEXT,
                    operation TEXT NOT NULL CHECK (operation IN ('insert', 'update', 'delete')),
                    authored_schema_version INTEGER,
                    authored_schema_hash TEXT,
                    base_version TEXT,
                    client_version TEXT NOT NULL,
                    lifecycle_state TEXT NOT NULL,
                    source_kind TEXT NOT NULL,
                    dependency_mutation_id TEXT,
                    normalized_mutation_id TEXT,
                    sealed_batch_id TEXT,
                    sealed_ordinal INTEGER,
                    accepted_json TEXT,
                    rejected_json TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """)
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS _synchro_mutation_values (
                    mutation_id TEXT NOT NULL,
                    field_id TEXT NOT NULL,
                    logical_type TEXT NOT NULL,
                    value_kind TEXT NOT NULL CHECK (value_kind IN ('null', 'boolean', 'integer', 'real', 'text', 'blob')),
                    value_integer INTEGER,
                    value_real REAL,
                    value_text TEXT,
                    value_blob BLOB,
                    PRIMARY KEY (mutation_id, field_id),
                    FOREIGN KEY (mutation_id) REFERENCES _synchro_pending_changes(mutation_id)
                ) WITHOUT ROWID
                """)
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS _synchro_push_batch_members (
                    batch_id TEXT NOT NULL,
                    mutation_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    PRIMARY KEY (batch_id, mutation_id),
                    UNIQUE (batch_id, ordinal)
                ) WITHOUT ROWID
                """)
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS _synchro_schema_archive (
                    schema_version INTEGER NOT NULL,
                    schema_hash TEXT NOT NULL,
                    schema_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (schema_version, schema_hash)
                ) WITHOUT ROWID
                """)
            if try db.tableExists("_synchro_rejected_mutations") {
                let rejectedColumns = try db.columns(in: "_synchro_rejected_mutations").map(\.name)
                if !rejectedColumns.contains("mutation_json") {
                    try db.execute(sql: "ALTER TABLE _synchro_rejected_mutations ADD COLUMN mutation_json TEXT")
                }
                if !rejectedColumns.contains("rejected_json") {
                    try db.execute(sql: "ALTER TABLE _synchro_rejected_mutations ADD COLUMN rejected_json TEXT")
                }
            }
            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_synchro_pending_changes_state_order
                ON _synchro_pending_changes (lifecycle_state, local_order)
                """)
            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_synchro_pending_changes_row_order
                ON _synchro_pending_changes (table_id, table_name, record_id, local_order)
                """)
            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_synchro_pending_changes_dependency
                ON _synchro_pending_changes (dependency_mutation_id, lifecycle_state)
                """)
            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_synchro_pending_changes_batch
                ON _synchro_pending_changes (sealed_batch_id, sealed_ordinal)
                """)
            try db.execute(sql: """
                CREATE INDEX IF NOT EXISTS idx_synchro_mutation_values_field
                ON _synchro_mutation_values (field_id, mutation_id)
                """)

            for legacy in legacyRows {
                let mutationID = Integrity.stableUUID(
                    domain: "synchro:v3:local-mutation-id:v1",
                    values: [legacy.tableName, legacy.recordID, legacy.operation, legacy.clientVersion, String(legacy.localRevision)]
                )
                let operation: String
                switch legacy.operation {
                case "create": operation = "insert"
                case "update": operation = "update"
                case "delete": operation = "delete"
                default: operation = "update"
                }
                let now = legacy.clientVersion
                try db.execute(
                    sql: """
                        INSERT OR IGNORE INTO _synchro_pending_changes
                            (mutation_id, capture_uuid, table_id, table_name, record_id, operation,
                             base_version, client_version, lifecycle_state, source_kind, created_at, updated_at)
                        VALUES (?, ?, NULL, ?, ?, ?, ?, ?, 'legacy_blocked', 'legacy_import', ?, ?)
                        """,
                    arguments: [mutationID, mutationID, legacy.tableName, legacy.recordID, operation, legacy.baseVersion, legacy.clientVersion, now, now]
                )
            }

            // Existing seeded databases already have a verified local manifest.
            // Reinstall its CDC triggers after replacing the queue table so offline
            // writes remain captured before the next connect.
            if let schemaVersionText = try SynchroMeta.get(db, key: .schemaVersion),
               let schemaVersion = Int64(schemaVersionText),
               schemaVersion > 0,
               let schemaHash = try SynchroMeta.get(db, key: .schemaHash),
               !schemaHash.isEmpty,
               let localSchemaJSON = try SynchroMeta.get(db, key: .localSchema),
               let localSchemaData = localSchemaJSON.data(using: .utf8),
               let localTables = try? JSONDecoder().decode([LocalSchemaTable].self, from: localSchemaData) {
                try SynchroMeta.archiveSchema(db, version: schemaVersion, hash: schemaHash, tables: localTables)
                for table in localTables {
                    for trigger in SQLiteSchema.generateCDCTriggers(table: table) {
                        try db.execute(sql: trigger)
                    }
                }
            }

        }
        migrator.registerMigration("synchro_v10_rebuild_page_receipts") { db in
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS _synchro_rebuild_page_receipts (
                    scope_id TEXT NOT NULL,
                    rebuild_id TEXT NOT NULL,
                    request_cursor_is_null INTEGER NOT NULL CHECK (request_cursor_is_null IN (0, 1)),
                    request_cursor TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    response_json TEXT NOT NULL,
                    is_final INTEGER NOT NULL CHECK (is_final IN (0, 1)),
                    final_scope_cursor TEXT,
                    final_checksum TEXT,
                    PRIMARY KEY (scope_id, rebuild_id, request_cursor_is_null, request_cursor),
                    CHECK (
                        (is_final = 0 AND final_scope_cursor IS NULL AND final_checksum IS NULL) OR
                        (is_final = 1 AND final_scope_cursor IS NOT NULL AND final_checksum IS NOT NULL)
                    )
                )
                """)
        }
        migrator.registerMigration("synchro_v11_durable_backoff") { db in
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS _synchro_backoff (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    resume_state TEXT NOT NULL CHECK (
                        resume_state IN ('connecting', 'pushing', 'pulling', 'rebuilding')
                    ),
                    work_identity TEXT NOT NULL,
                    retry_classification TEXT NOT NULL CHECK (
                        retry_classification IN ('network', 'http_429', 'http_503')
                    ),
                    attempt_count INTEGER NOT NULL CHECK (attempt_count > 0),
                    next_retry_at_ms INTEGER NOT NULL
                )
                """)
        }
        migrator.registerMigration("synchro_v12_gate2_recovery") { db in
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS _synchro_blocking_error (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    operation TEXT NOT NULL,
                    code TEXT NOT NULL,
                    retryable INTEGER NOT NULL CHECK (retryable IN (0, 1)),
                    message TEXT NOT NULL,
                    recovery_action TEXT NOT NULL CHECK (
                        recovery_action IN ('retry', 'schema_reset', 'none')
                    ),
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """)
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS _synchro_schema_migration (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    journal_version INTEGER NOT NULL,
                    source_schema_version INTEGER NOT NULL,
                    source_schema_hash TEXT NOT NULL,
                    target_schema_version INTEGER NOT NULL,
                    target_schema_hash TEXT NOT NULL,
                    target_manifest_json TEXT NOT NULL,
                    action TEXT NOT NULL CHECK (action IN ('replace', 'rebuild_local')),
                    affected_scopes_json TEXT NOT NULL,
                    scope_cursor_updates_json TEXT NOT NULL,
                    migration_plan_version INTEGER NOT NULL,
                    migration_plan_json TEXT NOT NULL,
                    migration_plan_hash TEXT NOT NULL,
                    phase TEXT NOT NULL CHECK (phase IN ('prepared', 'applied')),
                    is_schema_reset INTEGER NOT NULL CHECK (is_schema_reset IN (0, 1))
                )
                """)
        }
        migrator.registerMigration("synchro_v13_scope_text_affinity") { db in
            try db.execute(sql: """
                CREATE TABLE _synchro_scopes_v13 (
                    scope_id TEXT PRIMARY KEY,
                    cursor TEXT,
                    checksum TEXT,
                    generation INTEGER NOT NULL DEFAULT 0,
                    local_checksum TEXT NOT NULL DEFAULT ''
                )
                """)
            try db.execute(sql: """
                INSERT INTO _synchro_scopes_v13
                    (scope_id, cursor, checksum, generation, local_checksum)
                SELECT scope_id, cursor, CAST(checksum AS TEXT), generation, CAST(local_checksum AS TEXT)
                FROM _synchro_scopes
                """)
            try db.execute(sql: """
                CREATE TABLE _synchro_scope_rows_v13 (
                    scope_id TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    record_id TEXT NOT NULL,
                    checksum TEXT NOT NULL,
                    generation INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (scope_id, table_name, record_id)
                )
                """)
            try db.execute(sql: """
                INSERT INTO _synchro_scope_rows_v13
                    (scope_id, table_name, record_id, checksum, generation)
                SELECT scope_id, table_name, record_id, CAST(checksum AS TEXT), generation
                FROM _synchro_scope_rows
                """)
            try db.execute(sql: "DROP TABLE _synchro_scope_rows")
            try db.execute(sql: "DROP TABLE _synchro_scopes")
            try db.execute(sql: "ALTER TABLE _synchro_scopes_v13 RENAME TO _synchro_scopes")
            try db.execute(sql: "ALTER TABLE _synchro_scope_rows_v13 RENAME TO _synchro_scope_rows")
            try db.execute(sql: """
                CREATE INDEX idx_synchro_scope_rows_record
                ON _synchro_scope_rows (table_name, record_id)
                """)
        }
        migrator.registerMigration("synchro_v14_capture_context") { db in
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS _synchro_capture_context (
                    statement_token TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    operation TEXT NOT NULL CHECK (operation IN ('insert', 'update', 'delete')),
                    PRIMARY KEY (statement_token, table_name)
                ) WITHOUT ROWID
                """)
            try db.execute(sql: """
                CREATE TABLE IF NOT EXISTS _synchro_capture_fields (
                    statement_token TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    column_name TEXT NOT NULL,
                    PRIMARY KEY (statement_token, table_name, column_name)
                ) WITHOUT ROWID
                """)
            // Live capture triggers predate the context predicates, so a
            // verified installed schema regenerates its trigger set.
            if let schemaVersionText = try SynchroMeta.get(db, key: .schemaVersion),
               let schemaVersion = Int64(schemaVersionText),
               schemaVersion > 0,
               let schemaHash = try SynchroMeta.get(db, key: .schemaHash),
               !schemaHash.isEmpty,
               let localSchemaJSON = try SynchroMeta.get(db, key: .localSchema),
               let localSchemaData = localSchemaJSON.data(using: .utf8),
               let localTables = try? JSONDecoder().decode([LocalSchemaTable].self, from: localSchemaData) {
                for table in localTables {
                    for trigger in SQLiteSchema.generateCDCTriggers(table: table) {
                        try db.execute(sql: trigger)
                    }
                }
            }
        }
        try migrator.migrate(dbPool)
    }
}
