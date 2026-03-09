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
