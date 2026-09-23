import Foundation
import SQLite3
@preconcurrency import GRDB

/// A transaction-scoped application database surface.
///
/// This type intentionally does not expose the writable SQLite connection.
public final class ApplicationTransaction {
    private let database: GRDB.Database
    private let writeExecutor: ((String, () throws -> ExecResult) throws -> ExecResult)?

    fileprivate init(
        database: GRDB.Database,
        writeExecutor: ((String, () throws -> ExecResult) throws -> ExecResult)? = nil
    ) {
        self.database = database
        self.writeExecutor = writeExecutor
    }

    @discardableResult
    public func execute(
        _ sql: String,
        params: [(any DatabaseValueConvertible)?]? = nil
    ) throws -> ExecResult {
        let execute = {
            try self.database.execute(
                sql: sql,
                arguments: StatementArguments(params ?? [])
            )
            return ExecResult(rowsAffected: self.database.changesCount)
        }
        return try writeExecutor?(sql, execute) ?? execute()
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
        var captureContextWindowDepth = 0
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

    /// The SDK writes the capture context rows itself, and the reserved-table
    /// rule would deny that. The window opens only around the SDK's own
    /// install and clear statements on the serial application queue.
    func openCaptureContextWindow() {
        state.lock()
        protectedState.captureContextWindowDepth += 1
        state.unlock()
    }

    func closeCaptureContextWindow() {
        state.lock()
        protectedState.captureContextWindowDepth -= 1
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
            if isCaptureContextTable(target), snapshot.captureContextWindowDepth > 0 {
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

    private func isCaptureContextTable(_ name: String) -> Bool {
        switch normalized(name) {
        case "_synchro_capture_context", "_synchro_capture_fields":
            return true
        default:
            return false
        }
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

/// The authored capture context for one application write transaction.
enum ApplicationCaptureContext {
    /// The application names the table and the columns it authored, and the
    /// capture triggers retain exactly those columns.
    case authored(tableName: String, operation: String, columnNames: [String])
}

final class ApplicationDatabase: @unchecked Sendable {
    private let queue: DatabaseQueue
    private let policy: ApplicationSQLPolicy
    private let writableColumnsLock = NSLock()
    private var syncedTablesByName: [String: LocalSchemaTable] = [:]

    init(path: String, policy: ApplicationSQLPolicy) throws {
        self.policy = policy
        var configuration = Configuration()
        configuration.busyMode = .timeout(5)
        configuration.prepareDatabase { database in
            try policy.install(on: database)
        }
        queue = try DatabaseQueue(path: path, configuration: configuration)
    }

    func updateSyncedWritableColumns(_ tables: [LocalSchemaTable]) {
        writableColumnsLock.lock()
        syncedTablesByName = Dictionary(uniqueKeysWithValues: tables.map { table in
            (normalized(table.tableName), table)
        })
        writableColumnsLock.unlock()
    }

    func write<T>(_ body: (ApplicationTransaction) throws -> T) throws -> T {
        try queue.write { database in
            try body(ApplicationTransaction(database: database) { sql, execute in
                guard let context = try self.captureContext(for: sql) else {
                    return try execute()
                }
                return try self.withCaptureContext(context, database: database, execute)
            })
        }
    }

    func write<T>(
        context: ApplicationCaptureContext,
        _ body: (ApplicationTransaction) throws -> T
    ) throws -> T {
        try queue.write { database in
            try withCaptureContext(context, database: database) {
                try body(ApplicationTransaction(database: database))
            }
        }
    }

    private func captureContext(for sql: String) throws -> ApplicationCaptureContext? {
        guard let statement = try ApplicationWriteStatement.parse(sql) else { return nil }
        writableColumnsLock.lock()
        let table = syncedTablesByName[normalized(statement.tableName)]
        writableColumnsLock.unlock()
        guard let table else { return nil }

        let requested = statement.columnNames.map { names in
            Set(names.map(normalized))
        }
        let columns = table.columns.compactMap { column -> String? in
            guard column.writable else { return nil }
            guard requested?.contains(normalized(column.name)) ?? true else { return nil }
            return column.name
        }
        return .authored(
            tableName: table.tableName,
            operation: statement.operation,
            columnNames: columns
        )
    }

    private func withCaptureContext<T>(
        _ context: ApplicationCaptureContext,
        database: GRDB.Database,
        _ body: () throws -> T
    ) throws -> T {
        let token = UUID().uuidString
        policy.openCaptureContextWindow()
        do {
            try installCaptureContext(context, token: token, database: database)
            policy.closeCaptureContextWindow()
        } catch {
            policy.closeCaptureContextWindow()
            throw error
        }
        defer {
            policy.openCaptureContextWindow()
            try? clearCaptureContext(token: token, database: database)
            policy.closeCaptureContextWindow()
        }
        return try body()
    }

    private func installCaptureContext(
        _ context: ApplicationCaptureContext,
        token: String,
        database: GRDB.Database
    ) throws {
        var rows: [(table: String, operation: String, columns: [String])]
        switch context {
        case let .authored(tableName, operation, columnNames):
            rows = [(tableName, operation, columnNames)]
        }
        for row in rows {
            try database.execute(
                sql: """
                    INSERT INTO _synchro_capture_context (statement_token, table_name, operation)
                    VALUES (?, ?, ?)
                    """,
                arguments: [token, row.table, row.operation]
            )
            for column in row.columns {
                try database.execute(
                    sql: """
                        INSERT INTO _synchro_capture_fields (statement_token, table_name, column_name)
                        VALUES (?, ?, ?)
                        """,
                    arguments: [token, row.table, column]
                )
            }
        }
    }

    private func clearCaptureContext(token: String, database: GRDB.Database) throws {
        try database.execute(
            sql: "DELETE FROM _synchro_capture_fields WHERE statement_token = ?",
            arguments: [token]
        )
        try database.execute(
            sql: "DELETE FROM _synchro_capture_context WHERE statement_token = ?",
            arguments: [token]
        )
    }

    func close() throws {
        try queue.close()
    }

    private func normalized(_ value: String) -> String {
        value.lowercased(with: Locale(identifier: "en_US_POSIX"))
    }
}

private struct ApplicationWriteStatement {
    let tableName: String
    let operation: String
    let columnNames: [String]?

    static func parse(_ sql: String) throws -> ApplicationWriteStatement? {
        var lexer = Lexer(source: sql)
        var parser = Parser(tokens: try lexer.tokens())
        try parser.skipWithClause()
        switch parser.keyword() {
        case "INSERT":
            return try parser.parseInsert()
        case "UPDATE":
            return try parser.parseUpdate()
        case "DELETE":
            return try parser.parseDelete()
        default:
            return nil
        }
    }

    private enum Token: Equatable {
        case word(String)
        case identifier(String)
        case symbol(Character)
        case other
    }

    private struct Lexer {
        let source: [UnicodeScalar]
        var index = 0

        init(source: String) {
            self.source = Array(source.unicodeScalars)
        }

        mutating func tokens() throws -> [Token] {
            var result: [Token] = []
            while index < source.count {
                if CharacterSet.whitespacesAndNewlines.contains(source[index]) {
                    index += 1
                } else if starts(with: "--") {
                    index += 2
                    while index < source.count, source[index] != "\n", source[index] != "\r" { index += 1 }
                } else if starts(with: "/*") {
                    index += 2
                    while index + 1 < source.count, !starts(with: "*/") { index += 1 }
                    guard index + 1 < source.count else { throw invalidSQL() }
                    index += 2
                } else if source[index] == "'" {
                    try skipQuoted(opening: "'", closing: "'", doubledEscape: true)
                    result.append(.other)
                } else if source[index] == "\"" || source[index] == "`" || source[index] == "[" {
                    let opening = source[index]
                    let closing: UnicodeScalar = opening == "[" ? "]" : opening
                    result.append(.identifier(try quotedIdentifier(
                        opening: opening,
                        closing: closing,
                        doubledEscape: opening != "["
                    )))
                } else if isIdentifierStart(source[index]) {
                    let start = index
                    index += 1
                    while index < source.count, isIdentifierPart(source[index]) { index += 1 }
                    result.append(.word(String(String.UnicodeScalarView(source[start..<index]))))
                } else if source[index] == "?" || source[index] == ":" || source[index] == "@" || source[index] == "$" {
                    index += 1
                    while index < source.count, isIdentifierPart(source[index]) { index += 1 }
                    result.append(.other)
                } else if CharacterSet.decimalDigits.contains(source[index]) {
                    index += 1
                    while index < source.count,
                          CharacterSet.alphanumerics.contains(source[index]) || ".+-".unicodeScalars.contains(source[index]) {
                        index += 1
                    }
                    result.append(.other)
                } else {
                    result.append(.symbol(Character(source[index])))
                    index += 1
                }
            }
            return result
        }

        private mutating func skipQuoted(
            opening: UnicodeScalar,
            closing: UnicodeScalar,
            doubledEscape: Bool
        ) throws {
            _ = try quotedIdentifier(opening: opening, closing: closing, doubledEscape: doubledEscape)
        }

        private mutating func quotedIdentifier(
            opening: UnicodeScalar,
            closing: UnicodeScalar,
            doubledEscape: Bool
        ) throws -> String {
            guard source[index] == opening else { throw invalidSQL() }
            index += 1
            var value: [UnicodeScalar] = []
            while index < source.count {
                let scalar = source[index]
                index += 1
                if scalar == closing {
                    if doubledEscape, index < source.count, source[index] == closing {
                        value.append(closing)
                        index += 1
                    } else {
                        return String(String.UnicodeScalarView(value))
                    }
                } else {
                    value.append(scalar)
                }
            }
            throw invalidSQL()
        }

        private func starts(with value: String) -> Bool {
            let scalars = Array(value.unicodeScalars)
            guard index + scalars.count <= source.count else { return false }
            return Array(source[index..<(index + scalars.count)]) == scalars
        }

        private func isIdentifierStart(_ scalar: UnicodeScalar) -> Bool {
            scalar == "_" || CharacterSet.letters.contains(scalar) || scalar.value >= 128
        }

        private func isIdentifierPart(_ scalar: UnicodeScalar) -> Bool {
            isIdentifierStart(scalar) || CharacterSet.decimalDigits.contains(scalar) || scalar == "$"
        }
    }

    private struct Parser {
        let tokens: [Token]
        var index = 0

        mutating func skipWithClause() throws {
            guard consumeKeyword("WITH") else { return }
            _ = consumeKeyword("RECURSIVE")
            repeat {
                _ = try consumeIdentifier()
                if symbol() == "(" { try skipBalancedParentheses() }
                try requireKeyword("AS")
                if consumeKeyword("NOT") { try requireKeyword("MATERIALIZED") } else { _ = consumeKeyword("MATERIALIZED") }
                try skipBalancedParentheses()
            } while consumeSymbol(",")
        }

        mutating func parseInsert() throws -> ApplicationWriteStatement {
            try requireKeyword("INSERT")
            if consumeKeyword("OR") { index += 1 }
            try requireKeyword("INTO")
            let tableName = try consumeObjectName()
            if consumeKeyword("AS") { _ = try consumeIdentifier() }
            let columns: [String]?
            if symbol() == "(" {
                columns = try consumeIdentifierList()
            } else if keyword() == "DEFAULT" {
                columns = []
            } else {
                columns = nil
            }
            return ApplicationWriteStatement(tableName: tableName, operation: "insert", columnNames: columns)
        }

        mutating func parseUpdate() throws -> ApplicationWriteStatement {
            try requireKeyword("UPDATE")
            if consumeKeyword("OR") { index += 1 }
            let tableName = try consumeObjectName()
            if consumeKeyword("AS") { _ = try consumeIdentifier() }
            try requireKeyword("SET")
            let columns = try consumeUpdateTargets()
            return ApplicationWriteStatement(tableName: tableName, operation: "update", columnNames: columns)
        }

        mutating func parseDelete() throws -> ApplicationWriteStatement {
            try requireKeyword("DELETE")
            try requireKeyword("FROM")
            return ApplicationWriteStatement(
                tableName: try consumeObjectName(),
                operation: "delete",
                columnNames: []
            )
        }

        private mutating func consumeUpdateTargets() throws -> [String] {
            var result: [String] = []
            while true {
                if symbol() == "(" {
                    result.append(contentsOf: try consumeIdentifierList())
                } else {
                    result.append(try consumeIdentifier())
                }
                guard consumeSymbol("=") else { throw invalidSQL() }
                var depth = 0
                var consumedValue = false
                while index < tokens.count {
                    if symbol() == "(" {
                        depth += 1
                    } else if symbol() == ")", depth > 0 {
                        depth -= 1
                    } else if depth == 0, symbol() == "," {
                        index += 1
                        break
                    } else if depth == 0, let keyword = keyword(),
                              ["FROM", "WHERE", "RETURNING", "ORDER", "LIMIT"].contains(keyword) {
                        guard consumedValue else { throw invalidSQL() }
                        return result
                    }
                    consumedValue = true
                    index += 1
                }
                guard consumedValue else { throw invalidSQL() }
                if index >= tokens.count || symbol(at: index - 1) != "," { return result }
            }
        }

        private mutating func consumeIdentifierList() throws -> [String] {
            guard consumeSymbol("(") else { throw invalidSQL() }
            var result: [String] = []
            repeat { result.append(try consumeIdentifier()) } while consumeSymbol(",")
            guard consumeSymbol(")") else { throw invalidSQL() }
            return result
        }

        private mutating func consumeObjectName() throws -> String {
            var result = try consumeIdentifier()
            while consumeSymbol(".") { result = try consumeIdentifier() }
            return result
        }

        private mutating func skipBalancedParentheses() throws {
            guard consumeSymbol("(") else { throw invalidSQL() }
            var depth = 1
            while index < tokens.count {
                if consumeSymbol("(") {
                    depth += 1
                } else if consumeSymbol(")") {
                    depth -= 1
                    if depth == 0 { return }
                } else {
                    index += 1
                }
            }
            throw invalidSQL()
        }

        private mutating func consumeIdentifier() throws -> String {
            defer { index += 1 }
            switch tokens.indices.contains(index) ? tokens[index] : nil {
            case let .word(value), let .identifier(value):
                return value
            default:
                throw invalidSQL()
            }
        }

        private mutating func requireKeyword(_ value: String) throws {
            guard consumeKeyword(value) else { throw invalidSQL() }
        }

        private mutating func consumeKeyword(_ value: String) -> Bool {
            guard keyword() == value else { return false }
            index += 1
            return true
        }

        private mutating func consumeSymbol(_ value: Character) -> Bool {
            guard symbol() == value else { return false }
            index += 1
            return true
        }

        func keyword() -> String? {
            guard case let .word(value)? = tokens.indices.contains(index) ? tokens[index] : nil else { return nil }
            return value.uppercased(with: Locale(identifier: "en_US_POSIX"))
        }

        private func symbol() -> Character? { symbol(at: index) }

        private func symbol(at position: Int) -> Character? {
            guard case let .symbol(value)? = tokens.indices.contains(position) ? tokens[position] : nil else { return nil }
            return value
        }
    }

}

private func invalidSQL() -> SynchroError {
    .invalidResponse(message: "application write SQL is invalid")
}
