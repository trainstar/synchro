import Foundation
import GRDB

public final class SynchroClient: @unchecked Sendable {
    private let config: SynchroConfig
    private let database: SynchroDatabase
    private let httpClient: HttpClient
    private let schemaManager: SchemaManager
    private let changeTracker: ChangeTracker
    private let pullProcessor: PullProcessor
    private let pushProcessor: PushProcessor
    private let syncEngine: SyncEngine

    public init(config: SynchroConfig) throws {
        self.config = config
        if let seedPath = config.seedDatabasePath {
            let fm = FileManager.default
            if !fm.fileExists(atPath: config.dbPath) && fm.fileExists(atPath: seedPath) {
                try fm.copyItem(atPath: seedPath, toPath: config.dbPath)
                // Copy WAL/SHM files if they exist alongside the seed
                for suffix in ["-wal", "-shm"] {
                    let src = seedPath + suffix
                    let dst = config.dbPath + suffix
                    if fm.fileExists(atPath: src) {
                        try fm.copyItem(atPath: src, toPath: dst)
                    }
                }
            }
        }
        self.database = try SynchroDatabase(path: config.dbPath)
        self.httpClient = HttpClient(config: config)
        self.schemaManager = SchemaManager(database: database)
        self.changeTracker = ChangeTracker(database: database)
        self.pullProcessor = PullProcessor(database: database)
        self.pushProcessor = PushProcessor(database: database, changeTracker: changeTracker)
        self.syncEngine = SyncEngine(
            config: config,
            database: database,
            httpClient: httpClient,
            schemaManager: schemaManager,
            changeTracker: changeTracker,
            pullProcessor: pullProcessor,
            pushProcessor: pushProcessor
        )
    }

    // MARK: - Core SQL

    public func query(_ sql: String, params: [any DatabaseValueConvertible]? = nil) throws -> [Row] {
        try database.query(sql, params: params)
    }

    public func queryOne(_ sql: String, params: [any DatabaseValueConvertible]? = nil) throws -> Row? {
        try database.queryOne(sql, params: params)
    }

    public func execute(_ sql: String, params: [any DatabaseValueConvertible]? = nil) throws -> ExecResult {
        try database.execute(sql, params: params)
    }

    // MARK: - Transactions

    public func readTransaction<T>(_ block: (GRDB.Database) throws -> T) throws -> T {
        try database.readTransaction(block)
    }

    public func writeTransaction<T>(_ block: (GRDB.Database) throws -> T) throws -> T {
        try database.writeTransaction(block)
    }

    // MARK: - Prepared Statements

    public func withPreparedStatement<T>(_ sql: String, _ block: (Statement) throws -> T) throws -> T {
        try database.dbPool.read { db in
            let statement = try db.makeStatement(sql: sql)
            return try block(statement)
        }
    }

    public func withWritePreparedStatement<T>(_ sql: String, _ block: (Statement) throws -> T) throws -> T {
        try database.dbPool.write { db in
            let statement = try db.makeStatement(sql: sql)
            return try block(statement)
        }
    }

    // MARK: - Batch

    public func executeBatch(_ statements: [SQLStatement]) throws -> Int {
        try database.executeBatch(statements)
    }

    // MARK: - Schema (local-only tables)

    public func createTable(_ name: String, columns: [ColumnDef], options: TableOptions? = nil) throws {
        try database.createTable(name, columns: columns, options: options)
    }

    public func alterTable(_ name: String, addColumns: [ColumnDef]) throws {
        try database.alterTable(name, addColumns: addColumns)
    }

    public func createIndex(_ table: String, columns: [String], unique: Bool = false) throws {
        try database.createIndex(table, columns: columns, unique: unique)
    }

    // MARK: - Observation

    public func onChange(tables: [String], callback: @escaping () -> Void) -> any Cancellable {
        DatabaseCancellableWrapper(database.onChange(tables: tables, callback: callback))
    }

    public func watch(_ sql: String, params: [any DatabaseValueConvertible]? = nil, tables: [String], callback: @escaping ([Row]) -> Void) -> any Cancellable {
        DatabaseCancellableWrapper(database.watch(sql, params: params, tables: tables, callback: callback))
    }

    // MARK: - WAL

    public func checkpoint(mode: CheckpointMode = .passive) throws {
        try database.checkpoint(mode: mode)
    }

    // MARK: - Lifecycle

    public func close() throws {
        syncEngine.stop()
        try database.close()
    }

    public var path: String {
        database.path
    }

    // MARK: - Debug

    /// Returns diagnostic information about local sync state for support tickets.
    public func debugInfo() throws -> SynchroDebugInfo {
        try database.readTransaction { db in
            let checkpoint = try SynchroMeta.getInt64(db, key: .checkpoint)
            let bucketCheckpoints = try SynchroMeta.getAllBucketCheckpoints(db)

            var buckets: [BucketDebugInfo] = []
            for (bucketID, cp) in bucketCheckpoints {
                let memberRows = try Row.fetchAll(
                    db,
                    sql: "SELECT checksum FROM _synchro_bucket_members WHERE bucket_id = ?",
                    arguments: [bucketID]
                )
                var xor: Int32 = 0
                for row in memberRows {
                    if let cs: Int64 = row["checksum"] {
                        xor ^= Int32(truncatingIfNeeded: cs)
                    }
                }
                buckets.append(BucketDebugInfo(
                    bucketID: bucketID,
                    checkpoint: cp,
                    memberCount: memberRows.count,
                    checksum: xor
                ))
            }

            let pendingCount = try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM _synchro_pending_changes") ?? 0
            let schemaVersion = try SynchroMeta.getInt64(db, key: .schemaVersion)
            let schemaHash = try SynchroMeta.get(db, key: .schemaHash) ?? ""

            return SynchroDebugInfo(
                clientID: config.clientID,
                buckets: buckets,
                lastSyncCheckpoint: checkpoint,
                schemaVersion: schemaVersion,
                schemaHash: schemaHash,
                pendingChangeCount: pendingCount,
                generatedAt: Date()
            )
        }
    }

    /// Returns debug info as a pretty-printed JSON string for export.
    public func debugInfoJSON() throws -> String {
        let info = try debugInfo()
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        encoder.dateEncodingStrategy = .custom { date, encoder in
            var container = encoder.singleValueContainer()
            try container.encode(formatter.string(from: date))
        }
        let data = try encoder.encode(info)
        return String(data: data, encoding: .utf8) ?? "{}"
    }

    // MARK: - Sync Status

    /// Returns the number of local changes waiting to be pushed to the server.
    public func pendingChangeCount() throws -> Int {
        try changeTracker.pendingChangeCount()
    }

    // MARK: - Sync Control

    public func start(options: SyncOptions? = nil) async throws {
        try await syncEngine.start(options: options)
    }

    public func stop() {
        syncEngine.stop()
    }

    public func syncNow() async throws {
        try await syncEngine.syncNow()
    }

    // MARK: - Status

    public func onStatusChange(_ callback: @escaping (SyncStatus) -> Void) -> any Cancellable {
        syncEngine.onStatusChange(callback)
    }

    public func onConflict(_ callback: @escaping (ConflictEvent) -> Void) -> any Cancellable {
        syncEngine.onConflict(callback)
    }

    public func onSnapshotRequired(_ callback: @escaping () async -> Bool) -> any Cancellable {
        syncEngine.onSnapshotRequired(callback)
    }
}
