import Foundation
import React
import GRDB
import Synchro

// MARK: - Event Delegate Protocol

@objc public protocol SynchroEventEmitting: AnyObject {
    func emitEvent(_ name: String, body: NSDictionary)
}

// MARK: - Transaction Session

private class TransactionSession {
    let semaphore = DispatchSemaphore(value: 0)
    var pendingOp: TransactionOp?
    var committed = false
    var rolledBack = false
    let isWrite: Bool

    init(isWrite: Bool) {
        self.isWrite = isWrite
    }
}

private enum TransactionOp {
    case query(sql: String, params: [Any], completion: (Result<String, Error>) -> Void)
    case queryOne(sql: String, params: [Any], completion: (Result<String?, Error>) -> Void)
    case execute(sql: String, params: [Any], completion: (Result<[String: Any], Error>) -> Void)
    case commit(completion: (Result<Void, Error>) -> Void)
    case rollback(completion: (Result<Void, Error>) -> Void)
}

private struct TransactionTimeoutError: Error {}
private struct TransactionRollbackError: Error {}

// MARK: - Implementation

@objc(SynchroModuleImpl)
public class SynchroModuleImpl: NSObject {
    @objc public weak var eventDelegate: SynchroEventEmitting?

    private var client: SynchroClient?
    private var sessions: [String: TransactionSession] = [:]
    private let sessionsLock = NSLock()
    private var observers: [String: any Synchro.Cancellable] = [:]
    private var statusSubscription: (any Synchro.Cancellable)?
    private var conflictSubscription: (any Synchro.Cancellable)?

    private var pendingAuthContinuations: [String: CheckedContinuation<String, Error>] = [:]
    private let authLock = NSLock()

    private func emit(_ name: String, _ body: [String: Any]) {
        DispatchQueue.main.async { [weak self] in
            self?.eventDelegate?.emitEvent(name, body: body as NSDictionary)
        }
    }

    @objc public func rejectWithError(_ reject: @escaping RCTPromiseRejectBlock, _ error: Error) {
        if let synchroError = error as? SynchroError {
            let (code, userInfo) = mapSynchroError(synchroError)
            let base: [String: Any] = ["message": synchroError.localizedDescription]
            reject(code, synchroError.localizedDescription, NSError(
                domain: "SynchroModule",
                code: 0,
                userInfo: base.merging(userInfo) { _, new in new }
            ))
        } else if error is TransactionTimeoutError {
            reject("TRANSACTION_TIMEOUT", "Transaction timed out due to inactivity", nil)
        } else {
            reject("UNKNOWN", error.localizedDescription, error)
        }
    }

    private func mapSynchroError(_ error: SynchroError) -> (String, [String: Any]) {
        switch error {
        case .notConnected:
            return ("NOT_CONNECTED", [:])
        case .schemaNotLoaded:
            return ("SCHEMA_NOT_LOADED", [:])
        case .tableNotSynced(let table):
            return ("TABLE_NOT_SYNCED", ["table": table])
        case .upgradeRequired(let current, let minimum):
            return ("UPGRADE_REQUIRED", ["currentVersion": current, "minimumVersion": minimum])
        case .schemaMismatch(let version, let hash):
            return ("SCHEMA_MISMATCH", ["serverVersion": "\(version)", "serverHash": hash])
        case .pushRejected(let results):
            let data = try? JSONSerialization.data(withJSONObject: results.map { r in
                ["recordID": r.id, "table": r.tableName, "status": r.status] as [String: Any]
            })
            return ("PUSH_REJECTED", ["results": data.flatMap { String(data: $0, encoding: .utf8) } ?? "[]"])
        case .networkError(let underlying):
            return ("NETWORK_ERROR", ["message": underlying.localizedDescription])
        case .serverError(let status, let msg):
            return ("SERVER_ERROR", ["status": "\(status)", "message": msg])
        case .databaseError(let underlying):
            return ("DATABASE_ERROR", ["message": underlying.localizedDescription])
        case .invalidResponse(let msg):
            return ("INVALID_RESPONSE", ["message": msg])
        case .alreadyStarted:
            return ("ALREADY_STARTED", [:])
        case .notStarted:
            return ("NOT_STARTED", [:])
        }
    }

    // MARK: - Lifecycle

    @objc
    public func initialize(
        _ config: NSDictionary,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let dbPath = config["dbPath"] as? String,
              let serverURL = config["serverURL"] as? String,
              let clientID = config["clientID"] as? String,
              let platform = config["platform"] as? String,
              let appVersion = config["appVersion"] as? String,
              let url = URL(string: serverURL) else {
            reject("INVALID_CONFIG", "Missing required config fields", nil)
            return
        }
        // Resolve relative paths to the app's Documents directory
        let resolvedDbPath: String
        if (dbPath as NSString).isAbsolutePath {
            resolvedDbPath = dbPath
        } else {
            let documentsURL = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask).first!
            resolvedDbPath = documentsURL.appendingPathComponent(dbPath).path
        }

        let syncInterval = config["syncInterval"] as? Double ?? 30
        let pushDebounce = config["pushDebounce"] as? Double ?? 0.5
        let maxRetryAttempts = config["maxRetryAttempts"] as? Int ?? 5
        let pullPageSize = config["pullPageSize"] as? Int ?? 100
        let pushBatchSize = config["pushBatchSize"] as? Int ?? 100
        let seedDatabasePath = config["seedDatabasePath"] as? String

        let resolvedSeedPath: String?
        if let seedPath = seedDatabasePath {
            if (seedPath as NSString).isAbsolutePath {
                resolvedSeedPath = seedPath
            } else {
                // Bundled app resources live in Bundle.main, not Documents.
                // Check the app bundle first, fall back to Documents.
                let name = (seedPath as NSString).deletingPathExtension
                let ext = (seedPath as NSString).pathExtension
                if let bundlePath = Bundle.main.path(forResource: name, ofType: ext.isEmpty ? nil : ext) {
                    resolvedSeedPath = bundlePath
                } else {
                    let documentsURL = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask).first!
                    resolvedSeedPath = documentsURL.appendingPathComponent(seedPath).path
                }
            }
        } else {
            resolvedSeedPath = nil
        }

        do {
            let synchroConfig = SynchroConfig(
                dbPath: resolvedDbPath,
                serverURL: url,
                authProvider: { [weak self] in
                    return try await withCheckedThrowingContinuation { continuation in
                        let requestID = UUID().uuidString
                        self?.authLock.lock()
                        self?.pendingAuthContinuations[requestID] = continuation
                        self?.authLock.unlock()
                        self?.emit("onAuthRequest", ["requestID": requestID])
                    }
                },
                clientID: clientID,
                platform: platform,
                appVersion: appVersion,
                syncInterval: syncInterval,
                pushDebounce: pushDebounce,
                maxRetryAttempts: maxRetryAttempts,
                pullPageSize: pullPageSize,
                pushBatchSize: pushBatchSize,
                seedDatabasePath: resolvedSeedPath
            )
            try client?.close()
            clearRuntimeState()
            let client = try SynchroClient(config: synchroConfig)
            self.client = client
            wireClientEvents(client)
            resolve(nil)
        } catch {
            rejectWithError(reject, error)
        }
    }

    @objc
    public func resolveAuthRequest(_ requestID: String, token: String) {
        authLock.lock()
        let continuation = pendingAuthContinuations.removeValue(forKey: requestID)
        authLock.unlock()
        continuation?.resume(returning: token)
    }

    @objc
    public func rejectAuthRequest(_ requestID: String, error: String) {
        authLock.lock()
        let continuation = pendingAuthContinuations.removeValue(forKey: requestID)
        authLock.unlock()
        continuation?.resume(throwing: NSError(domain: "Auth", code: 0, userInfo: [NSLocalizedDescriptionKey: error]))
    }

    @objc
    public func close(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        do {
            try client?.close()
            clearRuntimeState()
            client = nil
            resolve(nil)
        } catch {
            rejectWithError(reject, error)
        }
    }

    @objc
    public func getPath(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        resolve(client.path)
    }

    // MARK: - Core SQL

    @objc
    public func query(
        _ sql: String,
        paramsJson: String,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        do {
            let params = try parseParams(paramsJson)
            let rows = try client.query(sql, params: params)
            resolve(try rowsToJson(rows))
        } catch {
            rejectWithError(reject, error)
        }
    }

    @objc
    public func queryOne(
        _ sql: String,
        paramsJson: String,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        do {
            let params = try parseParams(paramsJson)
            let row = try client.queryOne(sql, params: params)
            if let row = row {
                resolve(try rowToJson(row))
            } else {
                resolve(nil)
            }
        } catch {
            rejectWithError(reject, error)
        }
    }

    @objc
    public func execute(
        _ sql: String,
        paramsJson: String,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        do {
            let params = try parseParams(paramsJson)
            let result = try client.execute(sql, params: params)
            resolve(["rowsAffected": result.rowsAffected])
        } catch {
            rejectWithError(reject, error)
        }
    }

    @objc
    public func executeBatch(
        _ statementsJson: String,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        do {
            let data = statementsJson.data(using: .utf8)!
            let array = try JSONSerialization.jsonObject(with: data) as! [[String: Any]]
            let statements: [Synchro.SQLStatement] = array.map { item in
                let sql = item["sql"] as! String
                let params = (item["params"] as? [Any])?.compactMap { jsonValueToParam($0) }
                return Synchro.SQLStatement(sql: sql, params: params)
            }
            let total = try client.executeBatch(statements)
            resolve(["totalRowsAffected": total])
        } catch {
            rejectWithError(reject, error)
        }
    }

    // MARK: - Transactions

    @objc
    public func beginWriteTransaction(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        beginTransaction(isWrite: true, resolve: resolve, reject: reject)
    }

    @objc
    public func beginReadTransaction(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        beginTransaction(isWrite: false, resolve: resolve, reject: reject)
    }

    private func beginTransaction(
        isWrite: Bool,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }

        let txID = UUID().uuidString
        let session = TransactionSession(isWrite: isWrite)

        sessionsLock.lock()
        sessions[txID] = session
        sessionsLock.unlock()

        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            do {
                let txBlock: (GRDB.Database) throws -> Void = { db in
                    resolve(txID)

                    while true {
                        let result = session.semaphore.wait(timeout: .now() + 5)
                        if result == .timedOut {
                            throw TransactionTimeoutError()
                        }

                        guard let op = session.pendingOp else {
                            break
                        }
                        session.pendingOp = nil

                        switch op {
                        case .query(let sql, let params, let completion):
                            do {
                                let args = StatementArguments(params.compactMap { self?.jsonValueToParam($0) }) ?? StatementArguments()
                                let rows = try Row.fetchAll(db, sql: sql, arguments: args)
                                let json = try self?.rowsToJson(rows) ?? "[]"
                                completion(.success(json))
                            } catch {
                                completion(.failure(error))
                            }

                        case .queryOne(let sql, let params, let completion):
                            do {
                                let args = StatementArguments(params.compactMap { self?.jsonValueToParam($0) }) ?? StatementArguments()
                                let row = try Row.fetchOne(db, sql: sql, arguments: args)
                                let json = row != nil ? try self?.rowToJson(row!) : nil
                                completion(.success(json))
                            } catch {
                                completion(.failure(error))
                            }

                        case .execute(let sql, let params, let completion):
                            do {
                                let args = StatementArguments(params.compactMap { self?.jsonValueToParam($0) }) ?? StatementArguments()
                                try db.execute(sql: sql, arguments: args)
                                let changes = db.changesCount
                                completion(.success(["rowsAffected": changes]))
                            } catch {
                                completion(.failure(error))
                            }

                        case .commit(let completion):
                            session.committed = true
                            completion(.success(()))
                            return

                        case .rollback(let completion):
                            session.rolledBack = true
                            completion(.success(()))
                            throw TransactionRollbackError()
                        }
                    }
                }

                if isWrite {
                    try client.writeTransaction { db in try txBlock(db) }
                } else {
                    try client.readTransaction { db in try txBlock(db) }
                }
            } catch is TransactionTimeoutError {
                // auto-rollback
            } catch is TransactionRollbackError {
                // intentional rollback
            } catch {
                // other failure
            }

            self?.sessionsLock.lock()
            self?.sessions.removeValue(forKey: txID)
            self?.sessionsLock.unlock()
        }
    }

    @objc
    public func txQuery(
        _ txID: String,
        sql: String,
        paramsJson: String,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let session = getSession(txID) else {
            reject("TRANSACTION_TIMEOUT", "Transaction not found or expired", nil)
            return
        }
        do {
            let params = try parseParamsRaw(paramsJson)
            session.pendingOp = .query(sql: sql, params: params) { result in
                switch result {
                case .success(let json): resolve(json)
                case .failure(let error): reject("DATABASE_ERROR", error.localizedDescription, error)
                }
            }
            session.semaphore.signal()
        } catch {
            rejectWithError(reject, error)
        }
    }

    @objc
    public func txQueryOne(
        _ txID: String,
        sql: String,
        paramsJson: String,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let session = getSession(txID) else {
            reject("TRANSACTION_TIMEOUT", "Transaction not found or expired", nil)
            return
        }
        do {
            let params = try parseParamsRaw(paramsJson)
            session.pendingOp = .queryOne(sql: sql, params: params) { result in
                switch result {
                case .success(let json): resolve(json)
                case .failure(let error): reject("DATABASE_ERROR", error.localizedDescription, error)
                }
            }
            session.semaphore.signal()
        } catch {
            rejectWithError(reject, error)
        }
    }

    @objc
    public func txExecute(
        _ txID: String,
        sql: String,
        paramsJson: String,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let session = getSession(txID) else {
            reject("TRANSACTION_TIMEOUT", "Transaction not found or expired", nil)
            return
        }
        do {
            let params = try parseParamsRaw(paramsJson)
            session.pendingOp = .execute(sql: sql, params: params) { result in
                switch result {
                case .success(let dict): resolve(dict)
                case .failure(let error): reject("DATABASE_ERROR", error.localizedDescription, error)
                }
            }
            session.semaphore.signal()
        } catch {
            rejectWithError(reject, error)
        }
    }

    @objc
    public func commitTransaction(
        _ txID: String,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let session = getSession(txID) else {
            reject("TRANSACTION_TIMEOUT", "Transaction not found or expired", nil)
            return
        }
        session.pendingOp = .commit { result in
            switch result {
            case .success: resolve(nil)
            case .failure(let error): reject("DATABASE_ERROR", error.localizedDescription, error)
            }
        }
        session.semaphore.signal()
    }

    @objc
    public func rollbackTransaction(
        _ txID: String,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let session = getSession(txID) else {
            resolve(nil)
            return
        }
        session.pendingOp = .rollback { result in
            switch result {
            case .success: resolve(nil)
            case .failure(let error): reject("DATABASE_ERROR", error.localizedDescription, error)
            }
        }
        session.semaphore.signal()
    }

    private func getSession(_ txID: String) -> TransactionSession? {
        sessionsLock.lock()
        defer { sessionsLock.unlock() }
        return sessions[txID]
    }

    private func wireClientEvents(_ client: SynchroClient) {
        statusSubscription?.cancel()
        conflictSubscription?.cancel()

        statusSubscription = client.onStatusChange { [weak self] status in
            self?.emit("onStatusChange", self?.statusPayload(status) ?? [:])
        }

        conflictSubscription = client.onConflict { [weak self] event in
            self?.emit("onConflict", self?.conflictPayload(event) ?? [:])
        }
    }

    private func clearRuntimeState() {
        statusSubscription?.cancel()
        conflictSubscription?.cancel()
        statusSubscription = nil
        conflictSubscription = nil

        observers.values.forEach { $0.cancel() }
        observers.removeAll()

        sessionsLock.lock()
        sessions.removeAll()
        sessionsLock.unlock()

        authLock.lock()
        let authContinuations = pendingAuthContinuations
        pendingAuthContinuations.removeAll()
        authLock.unlock()
        authContinuations.values.forEach { continuation in
            continuation.resume(throwing: NSError(
                domain: "SynchroModule",
                code: 0,
                userInfo: [NSLocalizedDescriptionKey: "client closed"]
            ))
        }
    }

    private func statusPayload(_ status: SyncStatus) -> [String: Any] {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]

        switch status {
        case .idle:
            return ["status": "idle", "retryAt": NSNull()]
        case .syncing:
            return ["status": "syncing", "retryAt": NSNull()]
        case .stopped:
            return ["status": "stopped", "retryAt": NSNull()]
        case .error(let retryAt):
            return [
                "status": "error",
                "retryAt": retryAt.map { formatter.string(from: $0) } ?? NSNull()
            ]
        }
    }

    private func conflictPayload(_ event: ConflictEvent) -> [String: Any] {
        [
            "table": event.table,
            "recordID": event.recordID,
            "clientDataJson": encodeAnyCodableMap(event.clientData) ?? NSNull(),
            "serverDataJson": encodeAnyCodableMap(event.serverData) ?? NSNull()
        ]
    }

    private func encodeAnyCodableMap(_ value: [String: AnyCodable]?) -> String? {
        guard let value else { return nil }
        let encoder = JSONEncoder()
        guard let data = try? encoder.encode(value) else { return nil }
        return String(data: data, encoding: .utf8)
    }

    // MARK: - Schema

    @objc
    public func createTable(
        _ name: String,
        columnsJson: String,
        optionsJson: String?,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        do {
            let columns = try parseColumns(columnsJson)
            let options = try optionsJson.flatMap { try parseTableOptions($0) }
            try client.createTable(name, columns: columns, options: options)
            resolve(nil)
        } catch {
            rejectWithError(reject, error)
        }
    }

    @objc
    public func alterTable(
        _ name: String,
        columnsJson: String,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        do {
            let columns = try parseColumns(columnsJson)
            try client.alterTable(name, addColumns: columns)
            resolve(nil)
        } catch {
            rejectWithError(reject, error)
        }
    }

    @objc
    public func createIndex(
        _ table: String,
        columns: [String],
        unique: Bool,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        do {
            try client.createIndex(table, columns: columns, unique: unique)
            resolve(nil)
        } catch {
            rejectWithError(reject, error)
        }
    }

    // MARK: - Observation

    @objc
    public func addChangeObserver(
        _ observerID: String,
        tables: [String],
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        let cancellable = client.onChange(tables: tables) { [weak self] in
            self?.emit("onChange", ["observerID": observerID])
        }
        observers[observerID] = cancellable
        resolve(nil)
    }

    @objc
    public func addQueryObserver(
        _ observerID: String,
        sql: String,
        paramsJson: String,
        tables: [String],
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        do {
            let params = try parseParams(paramsJson)
            let cancellable = client.watch(sql, params: params, tables: tables) { [weak self] rows in
                if let json = try? self?.rowsToJson(rows) {
                    self?.emit("onQueryResult", ["observerID": observerID, "rowsJson": json])
                }
            }
            observers[observerID] = cancellable
            resolve(nil)
        } catch {
            rejectWithError(reject, error)
        }
    }

    @objc
    public func removeObserver(
        _ observerID: String,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        if let cancellable = observers.removeValue(forKey: observerID) {
            cancellable.cancel()
        }
        resolve(nil)
    }

    // MARK: - WAL / Sync

    @objc
    public func checkpoint(
        _ mode: String,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        do {
            let checkpointMode: Synchro.CheckpointMode
            switch mode {
            case "full": checkpointMode = .full
            case "restart": checkpointMode = .restart
            case "truncate": checkpointMode = .truncate
            default: checkpointMode = .passive
            }
            try client.checkpoint(mode: checkpointMode)
            resolve(nil)
        } catch {
            rejectWithError(reject, error)
        }
    }

    @objc
    public func start(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        Task {
            do {
                try await client.start()
                DispatchQueue.main.async {
                    resolve(nil)
                }
            } catch {
                DispatchQueue.main.async {
                    self.rejectWithError(reject, error)
                }
            }
        }
    }

    @objc
    public func stop(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        client.stop()
        DispatchQueue.main.async {
            resolve(nil)
        }
    }

    @objc
    public func syncNow(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        Task {
            do {
                try await client.syncNow()
                DispatchQueue.main.async {
                    resolve(nil)
                }
            } catch {
                DispatchQueue.main.async {
                    self.rejectWithError(reject, error)
                }
            }
        }
    }

    @objc
    public func pendingChangeCount(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        do {
            resolve(try client.pendingChangeCount())
        } catch {
            rejectWithError(reject, error)
        }
    }

    // MARK: - Helpers

    private func jsonValueToParam(_ value: Any) -> (any DatabaseValueConvertible)? {
        switch value {
        case is NSNull:
            return nil
        case let intVal as Int:
            return intVal
        case let doubleVal as Double:
            return doubleVal
        case let stringVal as String:
            return stringVal
        case let boolVal as Bool:
            return boolVal ? 1 : 0
        default:
            return String(describing: value)
        }
    }

    private func parseParams(_ json: String) throws -> [any DatabaseValueConvertible] {
        let data = json.data(using: .utf8)!
        let array = try JSONSerialization.jsonObject(with: data) as? [Any] ?? []
        return array.compactMap { jsonValueToParam($0) }
    }

    private func parseParamsRaw(_ json: String) throws -> [Any] {
        let data = json.data(using: .utf8)!
        return try JSONSerialization.jsonObject(with: data) as? [Any] ?? []
    }

    private func parseColumns(_ json: String) throws -> [ColumnDef] {
        let data = json.data(using: .utf8)!
        let array = try JSONSerialization.jsonObject(with: data) as! [[String: Any]]
        return array.map { item in
            ColumnDef(
                name: item["name"] as! String,
                type: item["type"] as! String,
                nullable: item["nullable"] as? Bool ?? true,
                defaultValue: item["defaultValue"] as? String,
                primaryKey: item["primaryKey"] as? Bool ?? false
            )
        }
    }

    private func parseTableOptions(_ json: String) throws -> Synchro.TableOptions {
        let data = json.data(using: .utf8)!
        let dict = try JSONSerialization.jsonObject(with: data) as! [String: Any]
        return Synchro.TableOptions(
            ifNotExists: dict["ifNotExists"] as? Bool ?? true,
            withoutRowid: dict["withoutRowid"] as? Bool ?? false
        )
    }

    private func databaseValueToFoundation(_ dbValue: DatabaseValue) -> Any {
        switch dbValue.storage {
        case .null:
            return NSNull()
        case .int64(let v):
            return NSNumber(value: v)
        case .double(let v):
            return NSNumber(value: v)
        case .string(let v):
            return v
        case .blob(let v):
            return v.base64EncodedString()
        }
    }

    private func rowToJson(_ row: Row) throws -> String {
        var dict: [String: Any] = [:]
        for column in row.columnNames {
            let dbValue: DatabaseValue = row[column]
            dict[column] = databaseValueToFoundation(dbValue)
        }
        let data = try JSONSerialization.data(withJSONObject: dict)
        return String(data: data, encoding: .utf8)!
    }

    private func rowsToJson(_ rows: [Row]) throws -> String {
        let array: [[String: Any]] = rows.map { row in
            var dict: [String: Any] = [:]
            for column in row.columnNames {
                let dbValue: DatabaseValue = row[column]
                dict[column] = databaseValueToFoundation(dbValue)
            }
            return dict
        }
        let data = try JSONSerialization.data(withJSONObject: array)
        return String(data: data, encoding: .utf8)!
    }
}
