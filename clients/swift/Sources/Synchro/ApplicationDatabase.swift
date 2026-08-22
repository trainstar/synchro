import Foundation
import SQLite3
@preconcurrency import GRDB

/// A transaction-scoped application database surface.
///
/// This type intentionally does not expose the writable SQLite connection.
public final class ApplicationTransaction {
    private let database: GRDB.Database

    fileprivate init(database: GRDB.Database) {
        self.database = database
    }

    @discardableResult
    public func execute(
        _ sql: String,
        params: [(any DatabaseValueConvertible)?]? = nil
    ) throws -> ExecResult {
        try database.execute(
            sql: sql,
            arguments: StatementArguments(params ?? [])
        )
        return ExecResult(rowsAffected: database.changesCount)
    }

    public func query(
        _ sql: String,
        params: [(any DatabaseValueConvertible)?]? = nil
    ) throws -> [Row] {
        try Row.fetchAll(
            database,
            sql: sql,
            arguments: StatementArguments(params ?? [])
        )
    }

    public func queryOne(
        _ sql: String,
        params: [(any DatabaseValueConvertible)?]? = nil
    ) throws -> Row? {
        try Row.fetchOne(
            database,
            sql: sql,
            arguments: StatementArguments(params ?? [])
        )
    }
}

private func applicationSQLiteAuthorizer(
    _ context: UnsafeMutableRawPointer?,
    _ actionCode: CInt,
    _ first: UnsafePointer<CChar>?,
    _ second: UnsafePointer<CChar>?,
    _ databaseName: UnsafePointer<CChar>?,
    _ source: UnsafePointer<CChar>?
) -> CInt {
    guard let context else { return SQLITE_DENY }
    return Unmanaged<ApplicationSQLPolicy>
        .fromOpaque(context)
        .takeUnretainedValue()
        .authorize(
            actionCode: actionCode,
            first: first.map(String.init(cString:)),
            second: second.map(String.init(cString:)),
            databaseName: databaseName.map(String.init(cString:)),
            source: source.map(String.init(cString:))
        )
}

/// Enforces the application SQLite boundary at statement compilation time.
///
/// SQLite reports resolved target objects to the authorizer. Aliases, CTEs,
/// views, and trigger bodies cannot hide the object that an operation changes.
final class ApplicationSQLPolicy: @unchecked Sendable {
    private struct State {
        var syncedTables: Set<String> = []
        var captureTriggers: Set<String> = []
    }

    private let state = NSLock()
    private var protectedState = State()

    func updateSyncedTables(_ tables: [LocalSchemaTable]) {
        state.lock()
        protectedState.syncedTables = Set(tables.map { normalized($0.tableName) })
        protectedState.captureTriggers = Set(tables.flatMap { table in
            [
                "_synchro_cdc_insert_\(table.tableName)",
                "_synchro_cdc_update_\(table.tableName)",
                "_synchro_cdc_delete_\(table.tableName)",
            ].map(normalized)
        })
        state.unlock()
    }

    func install(on database: GRDB.Database) throws {
        // The generated soft-delete trigger performs a nested update.
        try database.execute(sql: "PRAGMA recursive_triggers = ON")
        try database.execute(sql: "PRAGMA trusted_schema = OFF")
        guard sqlite3_set_authorizer(
            database.sqliteConnection,
            applicationSQLiteAuthorizer,
            Unmanaged.passUnretained(self).toOpaque()
        ) == SQLITE_OK else {
            throw SynchroError.databaseError(
                underlying: DatabaseError(message: "could not install the application SQL authorizer")
            )
        }
    }

    fileprivate func authorize(
        actionCode: CInt,
        first: String?,
        second: String?,
        databaseName: String?,
        source: String?
    ) -> CInt {
        _ = databaseName
        let snapshot = withState { $0 }

        switch actionCode {
        case SQLITE_INSERT, SQLITE_UPDATE, SQLITE_DELETE:
            guard let target = first else { return SQLITE_DENY }
            if isSQLiteCatalog(target) {
                // SQLite emits catalog writes for authorized DDL. Direct catalog
                // writes remain unavailable because writable_schema is denied.
                return SQLITE_OK
            }
            guard isReserved(target) else { return SQLITE_OK }
            return isAuthorizedCaptureWrite(
                target: target,
                source: source,
                snapshot: snapshot
            ) ? SQLITE_OK : SQLITE_DENY

        case SQLITE_CREATE_TABLE, SQLITE_CREATE_TEMP_TABLE,
             SQLITE_DROP_TABLE, SQLITE_DROP_TEMP_TABLE,
             SQLITE_CREATE_VTABLE, SQLITE_DROP_VTABLE:
            return protects(first, snapshot: snapshot) ? SQLITE_DENY : SQLITE_OK

        case SQLITE_ALTER_TABLE:
            // SQLite reports the database name first and the table name second.
            return protects(second, snapshot: snapshot) ? SQLITE_DENY : SQLITE_OK

        case SQLITE_CREATE_INDEX, SQLITE_CREATE_TEMP_INDEX,
             SQLITE_DROP_INDEX, SQLITE_DROP_TEMP_INDEX:
            return protects(first, snapshot: snapshot)
                || protects(second, snapshot: snapshot)
                ? SQLITE_DENY : SQLITE_OK

        case SQLITE_CREATE_TRIGGER, SQLITE_CREATE_TEMP_TRIGGER,
             SQLITE_DROP_TRIGGER, SQLITE_DROP_TEMP_TRIGGER:
            return protects(first, snapshot: snapshot)
                || protects(second, snapshot: snapshot)
                ? SQLITE_DENY : SQLITE_OK

        case SQLITE_CREATE_VIEW, SQLITE_CREATE_TEMP_VIEW,
             SQLITE_DROP_VIEW, SQLITE_DROP_TEMP_VIEW:
            return protects(first, snapshot: snapshot) ? SQLITE_DENY : SQLITE_OK

        case SQLITE_ATTACH, SQLITE_DETACH:
            return SQLITE_DENY

        case SQLITE_PRAGMA:
            return authorizePragma(name: first, argument: second)

        default:
            return SQLITE_OK
        }
    }

    private func isAuthorizedCaptureWrite(
        target: String,
        source: String?,
        snapshot: State
    ) -> Bool {
        guard let source,
              snapshot.captureTriggers.contains(normalized(source)) else {
            return false
        }
        switch normalized(target) {
        case "_synchro_pending_changes", "_synchro_mutation_values":
            return true
        default:
            return false
        }
    }

    private func protects(_ name: String?, snapshot: State) -> Bool {
        guard let name else { return false }
        let normalizedName = normalized(name)
        return isReserved(normalizedName)
            || snapshot.syncedTables.contains(normalizedName)
            || snapshot.captureTriggers.contains(normalizedName)
            || isSQLiteCatalog(normalizedName)
    }

    private func isReserved(_ name: String) -> Bool {
        let name = normalized(name)
        return name.hasPrefix("_synchro_")
            || name == "grdb_migrations"
            || name == "_grdb_migrations"
    }

    private func isSQLiteCatalog(_ name: String) -> Bool {
        switch normalized(name) {
        case "sqlite_master", "sqlite_schema", "sqlite_temp_master", "sqlite_temp_schema":
            return true
        default:
            return false
        }
    }

    private func authorizePragma(name: String?, argument: String?) -> CInt {
        guard let name else { return SQLITE_DENY }
        if argument == nil {
            return SQLITE_OK
        }

        // These argument-taking pragmas only inspect schema or integrity.
        switch normalized(name) {
        case "table_info", "table_xinfo", "index_info", "index_xinfo",
             "index_list", "foreign_key_list", "integrity_check", "quick_check":
            return SQLITE_OK
        default:
            return SQLITE_DENY
        }
    }

    private func withState<T>(_ body: (State) -> T) -> T {
        state.lock()
        defer { state.unlock() }
        return body(protectedState)
    }

    private func normalized(_ value: String) -> String {
        value.lowercased(with: Locale(identifier: "en_US_POSIX"))
    }
}

final class ApplicationDatabase: @unchecked Sendable {
    private let queue: DatabaseQueue

    init(path: String, policy: ApplicationSQLPolicy) throws {
        var configuration = Configuration()
        configuration.busyMode = .timeout(5)
        configuration.prepareDatabase { database in
            try policy.install(on: database)
        }
        queue = try DatabaseQueue(path: path, configuration: configuration)
    }

    func write<T>(_ body: (ApplicationTransaction) throws -> T) throws -> T {
        try queue.write { database in
            try body(ApplicationTransaction(database: database))
        }
    }

    func withPreparedStatement<T>(
        sql: String,
        body: (Statement) throws -> T
    ) throws -> T {
        try queue.writeWithoutTransaction { database in
            try body(database.makeStatement(sql: sql))
        }
    }

    func close() throws {
        try queue.close()
    }
}
