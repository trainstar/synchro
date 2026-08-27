import Foundation
import Darwin
import React
import GRDB
@_spi(Inspection) import Synchro

// MARK: - Event Delegate Protocol

@objc public protocol SynchroEventEmitting: AnyObject {
    func emitEvent(_ name: String, body: NSDictionary)
}

// MARK: - Transaction Session

private class TransactionSession {
    private let condition = NSCondition()
    private var operations: [TransactionOp] = []
    private var closed = false
    private var abortError: Error?
    private var finalCompletion: ((Result<Void, Error>) -> Void)?
    private var finished = false
    private var finishWaiters: [CheckedContinuation<Void, Never>] = []
    let isWrite: Bool

    init(isWrite: Bool) {
        self.isWrite = isWrite
    }

    func enqueue(_ op: TransactionOp) -> Bool {
        condition.lock()
        defer { condition.unlock() }
        guard !closed else {
            return false
        }
        operations.append(op)
        condition.signal()
        return true
    }

    func nextOperation(timeout: TimeInterval) throws -> TransactionOp? {
        condition.lock()
        defer { condition.unlock() }

        let deadline = Date().addingTimeInterval(timeout)
        while operations.isEmpty && !closed {
            if !condition.wait(until: deadline) {
                closed = true
                throw TransactionTimeoutError()
            }
        }

        if let abortError {
            throw abortError
        }
        guard !operations.isEmpty else {
            return nil
        }
        return operations.removeFirst()
    }

    @discardableResult
    func close() -> Bool {
        condition.lock()
        guard !closed else {
            condition.unlock()
            return false
        }
        closed = true
        let pending = operations
        operations.removeAll()
        condition.broadcast()
        condition.unlock()
        let error = TransactionAbortedError(message: "Transaction already completed")
        pending.forEach { $0.fail(error) }
        return true
    }

    func abort(_ error: Error) {
        condition.lock()
        guard !closed else {
            condition.unlock()
            return
        }
        closed = true
        abortError = error
        let pending = operations
        operations.removeAll()
        condition.broadcast()
        condition.unlock()
        pending.forEach { $0.fail(error) }
    }

    func currentAbortError() -> Error? {
        condition.lock()
        defer { condition.unlock() }
        return abortError
    }

    func setFinalCompletion(_ completion: @escaping (Result<Void, Error>) -> Void) {
        condition.lock()
        finalCompletion = completion
        condition.unlock()
    }

    func completeFinal(_ result: Result<Void, Error>) {
        condition.lock()
        let completion = finalCompletion
        condition.unlock()
        completion?(result)
    }

    func markFinished() {
        condition.lock()
        finished = true
        let waiters = finishWaiters
        finishWaiters.removeAll()
        condition.unlock()
        waiters.forEach { $0.resume() }
    }

    func waitUntilFinished() async {
        await withCheckedContinuation { continuation in
            condition.lock()
            if finished {
                condition.unlock()
                continuation.resume()
            } else {
                finishWaiters.append(continuation)
                condition.unlock()
            }
        }
    }
}

private enum TransactionOp {
    case query(sql: String, params: [Any], completion: (Result<[[String: Any]], Error>) -> Void)
    case queryOne(sql: String, params: [Any], completion: (Result<[String: Any]?, Error>) -> Void)
    case execute(sql: String, params: [Any], completion: (Result<[String: Any], Error>) -> Void)
    case commit(completion: (Result<Void, Error>) -> Void)
    case rollback(completion: (Result<Void, Error>) -> Void)

    func fail(_ error: Error) {
        switch self {
        case .query(_, _, let completion): completion(.failure(error))
        case .queryOne(_, _, let completion): completion(.failure(error))
        case .execute(_, _, let completion): completion(.failure(error))
        case .commit(let completion): completion(.failure(error))
        case .rollback(let completion): completion(.failure(error))
        }
    }
}

private struct TransactionTimeoutError: Error {}
private struct TransactionRollbackError: Error {}
private struct DatabaseAlreadyExistsError: Error {}
private struct TransactionAbortedError: LocalizedError {
    let message: String

    var errorDescription: String? { message }
}
private struct TransactionReadOnlyError: LocalizedError {
    var errorDescription: String? { "Read transactions cannot execute SQL" }
}

private actor LifecycleMutex {
    private var locked = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func lock() async {
        if !locked {
            locked = true
            return
        }
        await withCheckedContinuation { continuation in
            waiters.append(continuation)
        }
    }

    func unlock() {
        if waiters.isEmpty {
            locked = false
        } else {
            waiters.removeFirst().resume()
        }
    }
}

// MARK: - Implementation

@objc(SynchroModuleImpl)
public class SynchroModuleImpl: NSObject {
    @objc public weak var eventDelegate: SynchroEventEmitting?

    private var client: SynchroClient?
    private let lifecycleMutex = LifecycleMutex()
    private var sessions: [String: TransactionSession] = [:]
    private let sessionsLock = NSLock()
    private var acceptingTransactions = false
    private var observers: [String: any Synchro.Cancellable] = [:]
    private var statusSubscription: (any Synchro.Cancellable)?
    private var syncEventSubscription: (any Synchro.Cancellable)?
    private var conflictSubscription: (any Synchro.Cancellable)?
    private let statusDetailsLock = NSLock()
    private var cachedBackoff: SyncBackoffEvent?
    private var cachedFailure: SyncFailure?
    private var transportObservations: TransportObservationCollector?

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
        } else if error is TransactionAbortedError {
            reject("NOT_CONNECTED", "Client closed during transaction", error)
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
            return ("PUSH_REJECTED", ["results": encodeRejectedMutations(results)])
        case .networkError(let underlying):
            return ("NETWORK_ERROR", ["message": underlying.localizedDescription])
        case .serverError(let status, let msg):
            return ("SERVER_ERROR", ["status": "\(status)", "message": msg])
        case .protocolError(let status, let code, let msg):
            return (
                "PROTOCOL_ERROR",
                ["status": "\(status)", "protocolCode": code.rawValue, "message": msg]
            )
        case .databaseError(let underlying):
            return ("DATABASE_ERROR", ["message": underlying.localizedDescription])
        case .invalidResponse(let msg):
            return ("INVALID_RESPONSE", ["message": msg])
        case .blocked(let failure):
            return (
                "SYNC_BLOCKED",
                [
                    "failure": failurePayload(failure),
                ]
            )
        case .unsupportedSchema(let reason):
            return ("UNSUPPORTED_SCHEMA", ["reason": reason.rawValue])
        case .invalidStateTransition(let from, let to):
            return (
                "INVALID_STATE_TRANSITION",
                ["from": from.rawValue, "to": to.rawValue]
            )
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
        let resolvedDbPath = resolveDatabasePath(dbPath)

        let syncInterval = config["syncInterval"] as? Double ?? 30
        let pushDebounce = config["pushDebounce"] as? Double ?? 0.5
        let maxRetryAttempts = config["maxRetryAttempts"] as? Int ?? 5
        let pullPageSize = config["pullPageSize"] as? Int ?? 100
        let pushBatchSize = config["pushBatchSize"] as? Int ?? 100
        let seedDatabasePath = config["seedDatabasePath"] as? String
        let configuredTransportCapacity = config["transportObservationCapacity"] as? Int ?? 0
        let requireNewDatabase = config["requireNewDatabase"] as? Bool ?? false
        if configuredTransportCapacity < 0 || configuredTransportCapacity > 512 {
            reject("INVALID_CONFIG", "Transport observation capacity is invalid", nil)
            return
        }
        let transportCapacity = configuredTransportCapacity == 0 ? nil : configuredTransportCapacity

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

        let transportObservations = transportCapacity.map(TransportObservationCollector.init(capacity:))
        let synchroConfig = SynchroConfig(
            dbPath: resolvedDbPath,
            serverURL: url,
            authProvider: { [weak self] in
                try await withCheckedThrowingContinuation { continuation in
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
            seedDatabasePath: resolvedSeedPath,
            transportObservationCollector: transportObservations
        )
        Task { [weak self] in
            guard let self else { return }
            do {
                try await self.withLifecycleLock {
                    await self.clearRuntimeState()
                    try await self.client?.close()
                    if requireNewDatabase && FileManager.default.fileExists(atPath: resolvedDbPath) {
                        throw DatabaseAlreadyExistsError()
                    }
                    let client = try SynchroClient(config: synchroConfig)
                    self.setClient(client, acceptingTransactions: true)
                    self.transportObservations = transportObservations
                    self.wireClientEvents(client)
                }
                DispatchQueue.main.async {
                    resolve(nil)
                }
            } catch is DatabaseAlreadyExistsError {
                DispatchQueue.main.async {
                    reject("INVALID_CONFIG", "Database already exists", nil)
                }
            } catch {
                DispatchQueue.main.async {
                    if resolvedSeedPath != nil {
                        reject("INVALID_SEED", "Seed database failed validation", error)
                    } else {
                        self.rejectWithError(reject, error)
                    }
                }
            }
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
        Task { [weak self] in
            guard let self else { return }
            do {
                try await self.withLifecycleLock {
                    await self.clearRuntimeState()
                    try await self.client?.close()
                    self.setClient(nil, acceptingTransactions: false)
                }
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

    private func resolveDatabasePath(_ dbPath: String) -> String {
        if (dbPath as NSString).isAbsolutePath {
            return dbPath
        }
        let documentsURL = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask).first!
        return documentsURL.appendingPathComponent(dbPath).path
    }

    // MARK: - Core SQL

    @objc
    public func query(
        _ sql: String,
        params: [Any],
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        do {
            let rows = try client.query(sql, params: try bridgeParams(params))
            resolve(rowsToBridgeRows(rows))
        } catch {
            rejectWithError(reject, error)
        }
    }

    @objc
    public func queryOne(
        _ sql: String,
        params: [Any],
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        do {
            let row = try client.queryOne(sql, params: try bridgeParams(params))
            if let row = row {
                resolve(rowToBridgeRow(row))
            } else {
                resolve(NSNull())
            }
        } catch {
            rejectWithError(reject, error)
        }
    }

    @objc
    public func execute(
        _ sql: String,
        params: [Any],
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        do {
            let result = try client.execute(sql, params: try bridgeParams(params))
            resolve(["rowsAffected": result.rowsAffected])
        } catch {
            rejectWithError(reject, error)
        }
    }

    @objc
    public func executeBatch(
        _ statements: [[String: Any]],
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        do {
            let nativeStatements: [Synchro.SQLStatement] = try statements.map { item in
                let sql = item["sql"] as! String
                let params: [(any DatabaseValueConvertible)?]?
                if let bridgeValues = item["params"] as? [Any] {
                    params = try bridgeParams(bridgeValues)
                } else {
                    params = nil
                }
                return Synchro.SQLStatement(sql: sql, params: params)
            }
            let total = try client.executeBatch(nativeStatements)
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

    private func runTransactionLoop(
        session: TransactionSession,
        txID: String,
        resolve: @escaping RCTPromiseResolveBlock,
        query: (String, [(any DatabaseValueConvertible)?]) throws -> [Row],
        queryOne: (String, [(any DatabaseValueConvertible)?]) throws -> Row?,
        execute: ((String, [(any DatabaseValueConvertible)?]) throws -> Int)?
    ) throws {
        resolve(txID)

        while true {
            guard let op = try session.nextOperation(timeout: 5) else {
                break
            }

            switch op {
            case .query(let sql, let params, let completion):
                do {
                    let rows = try query(sql, try bridgeParams(params))
                    if let abortError = session.currentAbortError() {
                        throw abortError
                    }
                    completion(.success(rowsToBridgeRows(rows)))
                } catch {
                    completion(.failure(error))
                }

            case .queryOne(let sql, let params, let completion):
                do {
                    let row = try queryOne(sql, try bridgeParams(params))
                    if let abortError = session.currentAbortError() {
                        throw abortError
                    }
                    completion(.success(row.map(rowToBridgeRow)))
                } catch {
                    completion(.failure(error))
                }

            case .execute(let sql, let params, let completion):
                do {
                    guard let execute else {
                        throw TransactionReadOnlyError()
                    }
                    let rowsAffected = try execute(sql, try bridgeParams(params))
                    if let abortError = session.currentAbortError() {
                        throw abortError
                    }
                    completion(.success(["rowsAffected": rowsAffected]))
                } catch {
                    completion(.failure(error))
                }

            case .commit(let completion):
                guard session.close() else {
                    let error = session.currentAbortError()
                        ?? TransactionAbortedError(message: "Transaction already completed")
                    completion(.failure(error))
                    throw error
                }
                session.setFinalCompletion(completion)
                return

            case .rollback(let completion):
                guard session.close() else {
                    let error = session.currentAbortError()
                        ?? TransactionAbortedError(message: "Transaction already completed")
                    completion(.failure(error))
                    throw error
                }
                session.setFinalCompletion(completion)
                throw TransactionRollbackError()
            }
        }
    }

    private func beginTransaction(
        isWrite: Bool,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        sessionsLock.lock()
        guard acceptingTransactions, let client = client else {
            sessionsLock.unlock()
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }

        let txID = UUID().uuidString
        let session = TransactionSession(isWrite: isWrite)
        sessions[txID] = session
        sessionsLock.unlock()

        DispatchQueue.global(qos: .userInitiated).async {
            defer {
                session.close()
                self.sessionsLock.lock()
                self.sessions.removeValue(forKey: txID)
                self.sessionsLock.unlock()
                session.markFinished()
            }
            do {
                let finalResult: Result<Void, Error>
                if isWrite {
                    try client.writeTransaction { transaction in
                        try self.runTransactionLoop(
                            session: session,
                            txID: txID,
                            resolve: resolve,
                            query: { sql, params in
                                try transaction.query(sql, params: params)
                            },
                            queryOne: { sql, params in
                                try transaction.queryOne(sql, params: params)
                            },
                            execute: { sql, params in
                                try transaction.execute(sql, params: params).rowsAffected
                            }
                        )
                    }
                } else {
                    try client.readTransaction { db in
                        try self.runTransactionLoop(
                            session: session,
                            txID: txID,
                            resolve: resolve,
                            query: { sql, params in
                                let statement = try db.makeStatement(sql: sql)
                                guard statement.isReadonly else {
                                    throw TransactionReadOnlyError()
                                }
                                return try Row.fetchAll(
                                    statement,
                                    arguments: StatementArguments(params)
                                )
                            },
                            queryOne: { sql, params in
                                let statement = try db.makeStatement(sql: sql)
                                guard statement.isReadonly else {
                                    throw TransactionReadOnlyError()
                                }
                                return try Row.fetchOne(
                                    statement,
                                    arguments: StatementArguments(params)
                                )
                            },
                            execute: nil
                        )
                    }
                }
                finalResult = .success(())
                session.completeFinal(finalResult)
            } catch is TransactionTimeoutError {
                session.completeFinal(.failure(TransactionTimeoutError()))
            } catch is TransactionRollbackError {
                session.completeFinal(.success(()))
            } catch {
                session.completeFinal(.failure(error))
            }
        }
    }

    @objc
    public func txQuery(
        _ txID: String,
        sql: String,
        params: [Any],
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let session = getSession(txID) else {
            reject("TRANSACTION_TIMEOUT", "Transaction not found or expired", nil)
            return
        }
        let op = TransactionOp.query(sql: sql, params: params) { result in
            switch result {
            case .success(let rows): resolve(rows)
            case .failure(let error): reject("DATABASE_ERROR", error.localizedDescription, error)
            }
        }
        guard session.enqueue(op) else {
            reject("TRANSACTION_TIMEOUT", "Transaction not found or expired", nil)
            return
        }
    }

    @objc
    public func txQueryOne(
        _ txID: String,
        sql: String,
        params: [Any],
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let session = getSession(txID) else {
            reject("TRANSACTION_TIMEOUT", "Transaction not found or expired", nil)
            return
        }
        let op = TransactionOp.queryOne(sql: sql, params: params) { result in
            switch result {
            case .success(let row): resolve(row ?? NSNull())
            case .failure(let error): reject("DATABASE_ERROR", error.localizedDescription, error)
            }
        }
        guard session.enqueue(op) else {
            reject("TRANSACTION_TIMEOUT", "Transaction not found or expired", nil)
            return
        }
    }

    @objc
    public func txExecute(
        _ txID: String,
        sql: String,
        params: [Any],
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let session = getSession(txID) else {
            reject("TRANSACTION_TIMEOUT", "Transaction not found or expired", nil)
            return
        }
        guard session.isWrite else {
            reject("DATABASE_ERROR", "Read transactions cannot execute SQL", nil)
            return
        }
        let op = TransactionOp.execute(sql: sql, params: params) { result in
            switch result {
            case .success(let dict): resolve(dict)
            case .failure(let error): reject("DATABASE_ERROR", error.localizedDescription, error)
            }
        }
        guard session.enqueue(op) else {
            reject("TRANSACTION_TIMEOUT", "Transaction not found or expired", nil)
            return
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
        let op = TransactionOp.commit { result in
            switch result {
            case .success: resolve(nil)
            case .failure(let error): reject("DATABASE_ERROR", error.localizedDescription, error)
            }
        }
        guard session.enqueue(op) else {
            reject("TRANSACTION_TIMEOUT", "Transaction not found or expired", nil)
            return
        }
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
        let op = TransactionOp.rollback { result in
            switch result {
            case .success: resolve(nil)
            case .failure(let error): reject("DATABASE_ERROR", error.localizedDescription, error)
            }
        }
        guard session.enqueue(op) else {
            resolve(nil)
            return
        }
    }

    private func getSession(_ txID: String) -> TransactionSession? {
        sessionsLock.lock()
        defer { sessionsLock.unlock() }
        return sessions[txID]
    }

    private func wireClientEvents(_ client: SynchroClient) {
        statusSubscription?.cancel()
        syncEventSubscription?.cancel()
        conflictSubscription?.cancel()
        statusDetailsLock.lock()
        cachedBackoff = nil
        cachedFailure = nil
        statusDetailsLock.unlock()

        statusSubscription = client.onStatusChange { [weak self] status in
            guard status != .backoff, status != .error else { return }
            self?.emit("onStatusChange", self?.statusPayload(status) ?? [:])
        }

        syncEventSubscription = client.onSyncEvent { [weak self] event in
            guard let self else { return }
            self.recordStatusDetails(event)
            self.emit("onSyncEvent", self.syncEventPayload(event))
            switch event {
            case .backoff, .failure:
                self.emit(
                    "onStatusChange",
                    self.statusPayload(client.getSyncStatus())
                )
            default:
                break
            }
        }

        conflictSubscription = client.onConflict { [weak self] event in
            self?.emit("onConflict", self?.conflictPayload(event) ?? [:])
        }
    }

    private func setClient(_ client: SynchroClient?, acceptingTransactions: Bool) {
        sessionsLock.lock()
        self.client = client
        self.acceptingTransactions = acceptingTransactions
        sessionsLock.unlock()
    }

    private func withLifecycleLock(_ operation: () async throws -> Void) async throws {
        await lifecycleMutex.lock()
        do {
            try await operation()
            await lifecycleMutex.unlock()
        } catch {
            await lifecycleMutex.unlock()
            throw error
        }
    }

    private func detachRuntimeState() -> [TransactionSession] {
        statusSubscription?.cancel()
        syncEventSubscription?.cancel()
        conflictSubscription?.cancel()
        statusSubscription = nil
        syncEventSubscription = nil
        conflictSubscription = nil
        transportObservations?.cancelPauseBarrier()
        transportObservations = nil
        statusDetailsLock.lock()
        cachedBackoff = nil
        cachedFailure = nil
        statusDetailsLock.unlock()

        observers.values.forEach { $0.cancel() }
        observers.removeAll()

        sessionsLock.lock()
        acceptingTransactions = false
        let activeSessions = Array(sessions.values)
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
        return activeSessions
    }

    private func clearRuntimeState() async {
        let activeSessions = detachRuntimeState()
        activeSessions.forEach {
            $0.abort(TransactionAbortedError(message: "Client closed during transaction"))
        }
        for session in activeSessions {
            await session.waitUntilFinished()
        }
    }

    private func statusPayload(_ status: SyncStatus) -> [String: Any] {
        var payload: [String: Any] = [
            "status": status.rawValue,
            "retryAt": NSNull(),
            "operation": NSNull(),
            "failure": NSNull(),
        ]
        switch status {
        case .uninitialized, .localReady, .connecting, .schemaApplying, .ready, .pushing, .pulling, .rebuilding, .stopped:
            break
        case .backoff:
            statusDetailsLock.lock()
            let backoff = cachedBackoff
            statusDetailsLock.unlock()
            if let backoff {
                payload["retryAt"] = iso8601String(backoff.retryAt)
                payload["operation"] = backoff.operation.rawValue
            }
        case .error:
            statusDetailsLock.lock()
            let cachedFailure = self.cachedFailure
            statusDetailsLock.unlock()
            let persistedFailure: SyncFailure?
            if let client = self.client {
                persistedFailure = try? client.getBlockingFailure()
            } else {
                persistedFailure = nil
            }
            if let failure = cachedFailure ?? persistedFailure {
                payload["failure"] = failurePayload(failure)
            }
        }
        return payload
    }

    private func recordStatusDetails(_ event: SyncEvent) {
        statusDetailsLock.lock()
        defer { statusDetailsLock.unlock() }
        switch event {
        case .backoff(let backoff):
            cachedBackoff = backoff
            cachedFailure = nil
        case .failure(let failure):
            cachedFailure = failure
            cachedBackoff = nil
        default:
            break
        }
    }

    private func schemaPayload(_ schema: SchemaRef) -> [String: Any] {
        ["version": schema.version, "hash": schema.hash]
    }

    private func failurePayload(_ failure: SyncFailure) -> [String: Any] {
        [
            "operation": failure.operation.rawValue,
            "code": failure.code.rawValue,
            "retryable": failure.retryable,
            "message": failure.message,
            "recoveryAction": failure.recoveryAction.rawValue,
            "metadata": failure.metadata,
        ]
    }

    private func iso8601String(_ date: Date) -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter.string(from: date)
    }

    private func syncEventPayload(_ event: SyncEvent) -> [String: Any] {
        var payload: [String: Any] = [
            "type": "",
            "from": NSNull(),
            "to": NSNull(),
            "operation": NSNull(),
            "attempt": NSNull(),
            "retryAt": NSNull(),
            "source": NSNull(),
            "target": NSNull(),
            "action": NSNull(),
            "mutationID": NSNull(),
            "tableID": NSNull(),
            "mutationStatus": NSNull(),
            "rejectionCode": NSNull(),
            "scopeID": NSNull(),
            "rebuildID": NSNull(),
            "failure": NSNull(),
        ]

        switch event {
        case .stateChanged(let state):
            payload["type"] = "state_changed"
            payload["from"] = state.from.rawValue
            payload["to"] = state.to.rawValue
        case .backoff(let backoff):
            payload["type"] = "backoff"
            payload["operation"] = backoff.operation.rawValue
            payload["attempt"] = backoff.attempt
            payload["retryAt"] = iso8601String(backoff.retryAt)
        case .schemaApplying(let schema):
            payload["type"] = "schema_applying"
            payload["source"] = schemaPayload(schema.source)
            payload["target"] = schemaPayload(schema.target)
            payload["action"] = schema.action.rawValue
        case .schemaApplied(let schema):
            payload["type"] = "schema_applied"
            payload["source"] = schemaPayload(schema.source)
            payload["target"] = schemaPayload(schema.target)
            payload["action"] = schema.action.rawValue
        case .mutationAccepted(let mutation):
            payload["type"] = "mutation_accepted"
            payload["mutationID"] = mutation.mutationID
            payload["tableID"] = mutation.tableID
            payload["mutationStatus"] = mutation.status.rawValue
            payload["rejectionCode"] = mutation.rejectionCode?.rawValue ?? NSNull()
        case .mutationRejected(let mutation):
            payload["type"] = "mutation_rejected"
            payload["mutationID"] = mutation.mutationID
            payload["tableID"] = mutation.tableID
            payload["mutationStatus"] = mutation.status.rawValue
            payload["rejectionCode"] = mutation.rejectionCode?.rawValue ?? NSNull()
        case .rebuildRequested(let rebuild):
            payload["type"] = "rebuild_requested"
            payload["scopeID"] = rebuild.scopeID
            payload["rebuildID"] = rebuild.rebuildID
        case .rebuildCompleted(let rebuild):
            payload["type"] = "rebuild_completed"
            payload["scopeID"] = rebuild.scopeID
            payload["rebuildID"] = rebuild.rebuildID
        case .failure(let failure):
            payload["type"] = "failure"
            payload["failure"] = failurePayload(failure)
        }
        return payload
    }

    private func conflictPayload(_ event: ConflictEvent) -> [String: Any] {
        [
            "table": event.table,
            "recordID": event.recordID,
            "clientDataJson": encodeAnyCodableMap(event.clientData) ?? NSNull(),
            "serverDataJson": encodeAnyCodableMap(event.serverData) ?? NSNull()
        ]
    }

    private func encodeRejectedMutations(_ results: [RejectedMutation]) -> String {
        let payload = results.map { result in
            [
                "mutationID": result.mutationID,
                "table": result.table,
                "pk": anyCodableMapToJSONObject(result.pk),
                "status": result.status.rawValue,
                "code": result.code.rawValue,
                "message": result.message,
                "serverRow": anyCodableMapToJSONObject(result.serverRow),
                "serverVersion": result.serverVersion ?? NSNull()
            ] as [String: Any]
        }

        guard JSONSerialization.isValidJSONObject(payload),
              let data = try? JSONSerialization.data(withJSONObject: payload),
              let json = String(data: data, encoding: .utf8) else {
            return "[]"
        }

        return json
    }

    private func anyCodableMapToJSONObject(_ value: [String: AnyCodable]?) -> Any {
        guard let value else { return NSNull() }
        return value.mapValues { anyCodableToJSONObject($0.value) }
    }

    private func anyCodableToJSONObject(_ value: Any?) -> Any {
        switch value {
        case nil:
            return NSNull()
        case is NSNull:
            return NSNull()
        case let value as String:
            return value
        case let value as NSNumber:
            return value
        case let value as Bool:
            return value
        case let value as AnyCodable:
            return anyCodableToJSONObject(value.value)
        case let value as [AnyCodable]:
            return value.map { anyCodableToJSONObject($0.value) }
        case let value as [String: AnyCodable]:
            return value.mapValues { anyCodableToJSONObject($0.value) }
        case let value as [Any]:
            return value.map { anyCodableToJSONObject($0) }
        case let value as [String: Any]:
            return value.mapValues { anyCodableToJSONObject($0) }
        default:
            return String(describing: value)
        }
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
        params: [Any],
        tables: [String],
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        do {
            let nativeParams = try bridgeParams(params)
            let cancellable = client.watch(sql, params: nativeParams, tables: tables) { [weak self] rows in
                if let payload = self?.rowsToBridgeRows(rows) {
                    self?.emit("onQueryResult", ["observerID": observerID, "rows": payload])
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
        Task {
            await client.stop()
            DispatchQueue.main.async {
                resolve(nil)
            }
        }
    }

    @objc
    public func enterBackground(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        Task {
            await client.enterBackground()
            DispatchQueue.main.async {
                resolve(nil)
            }
        }
    }

    @objc
    public func enterForeground(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        Task {
            do {
                try await client.enterForeground()
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
    public func retryAfterError(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        Task {
            do {
                try await client.retryAfterError()
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
    public func resetSchemaAndStart(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        Task {
            do {
                try await client.resetSchemaAndStart()
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

    @objc
    public func getSyncStatus(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        do {
            resolve(try encodeBridgeJSON(statusPayload(client.getSyncStatus())))
        } catch {
            rejectWithError(reject, error)
        }
    }

    @objc
    public func inspectPendingMutations(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        do {
            let payload = try client.inspectPendingMutations().map(pendingMutationPayload)
            resolve(try encodeBridgeJSON(payload))
        } catch {
            rejectWithError(reject, error)
        }
    }

    @objc
    public func inspectRejectedMutations(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        do {
            let payload = try client.inspectRejectedMutations().map(rejectedMutationPayload)
            resolve(try encodeBridgeJSON(payload))
        } catch {
            rejectWithError(reject, error)
        }
    }

    @objc
    public func inspectClientState(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        do {
            let inspection = SynchroInspection(client: client)
            let capture = try inspection.captureState(maximumRecords: 512)
            let schema: Any = capture.schema.map { value in
                ["version": value.version, "hash": value.hash]
            } ?? NSNull()
            let scopeStates = capture.scopeStates.map { value in
                [
                    "scope_id": value.scopeID,
                    "cursor": value.cursor ?? NSNull(),
                    "checksum": value.checksum ?? NSNull(),
                    "local_checksum": value.localChecksum,
                    "generation": value.generation,
                ] as [String: Any]
            }
            let scopeRows = capture.scopeRows.map { value in
                [
                    "scope_id": value.scopeID,
                    "table_name": value.tableName,
                    "record_id": value.recordID,
                    "checksum": value.checksum,
                    "generation": value.generation,
                ] as [String: Any]
            }
            let attempts = capture.rebuildAttempts.map { value in
                [
                    "scope_id": value.scopeID,
                    "rebuild_id": value.rebuildID,
                    "client_generation": value.clientGeneration,
                    "schema_version": value.schemaVersion,
                    "schema_hash": value.schemaHash,
                    "generation": value.generation,
                    "cursor": value.cursor ?? NSNull(),
                    "page_limit": value.pageLimit,
                ] as [String: Any]
            }
            resolve(try encodeBridgeJSON([
                "schema": schema,
                "scope_states": scopeStates,
                "scope_rows": scopeRows,
                "rebuild_attempts": attempts,
                "application_row_count": capture.applicationRowCount,
                "mutation_ledger_count": capture.mutationLedgerCount,
                "mutation_outcome_count": capture.mutationOutcomeCount,
                "sealed_batch_count": capture.sealedBatchCount,
                "rejected_mutation_count": capture.rejectedMutationCount,
                "scope_state_count": capture.scopeStateCount,
                "scope_row_count": capture.scopeRowCount,
                "provenance_count": capture.provenanceCount,
                "row_metadata_count": capture.rowMetadataCount,
                "rebuild_attempt_count": capture.rebuildAttemptCount,
                "rebuild_receipt_count": capture.rebuildReceiptCount,
                "scope_states_truncated": capture.scopeStatesTruncated,
                "scope_rows_truncated": capture.scopeRowsTruncated,
                "rebuild_attempts_truncated": capture.rebuildAttemptsTruncated,
                "rebuild_receipts_truncated": capture.rebuildReceiptsTruncated,
                "row_metadata_truncated": capture.rowMetadataTruncated,
                "capture_overflowed": capture.overflowed,
                "provenance_maintenance_work_cursor": String(
                    capture.provenanceMaintenanceWorkCursor
                ),
            ]))
        } catch {
            rejectWithError(reject, error)
        }
    }

    @objc
    public func inspectDurableState(
        _ tableName: String,
        recordID: String,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        do {
            let inspection = SynchroInspection(client: client)
            let metadata: Any = try inspection.rowMetadata(
                tableName: tableName,
                recordID: recordID
            ).map { value in
                [
                    "table_name": value.tableName,
                    "record_id": value.recordID,
                    "server_version": value.serverVersion,
                    "row_checksum": value.rowChecksum ?? NSNull(),
                ] as [String: Any]
            } ?? NSNull()
            let receipts = try inspection.rebuildReceipts().map { value in
                [
                    "rebuild_id_fingerprint": value.rebuildIDFingerprint,
                    "page_count": value.pageCount,
                    "returned_record_count": value.returnedRecordCount,
                    "request_chain_expected": value.requestChainExpected,
                    "request_chain_observed": value.requestChainObserved,
                    "record_identities_hex": value.recordIdentitiesHex,
                    "received_row_checksums": value.receivedRowChecksums,
                    "computed_row_checksums": value.computedRowChecksums,
                    "computed_scope_checksum": value.computedScopeChecksum ?? NSNull(),
                    "final_scope_checksum": value.finalScopeChecksum ?? NSNull(),
                    "stored_scope_checksum": value.storedScopeChecksum ?? NSNull(),
                    "local_scope_checksum": value.localScopeChecksum ?? NSNull(),
                ] as [String: Any]
            }
            resolve(try encodeBridgeJSON([
                "row_metadata": metadata,
                "rebuild_receipts": receipts,
            ]))
        } catch {
            rejectWithError(reject, error)
        }
    }

    @objc
    public func inspectTransportObservations(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let transportObservations else {
            reject("NOT_CONNECTED", "Transport observation is not configured", nil)
            return
        }
        do {
            let data = try JSONEncoder().encode(transportObservations.snapshot())
            guard let value = String(data: data, encoding: .utf8) else {
                throw SynchroError.invalidResponse(message: "transport observation encoding failed")
            }
            resolve(value)
        } catch {
            rejectWithError(reject, error)
        }
    }

    @objc
    public func armTransportPause(
        _ operationClass: String,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let collector = transportObservations,
              let operation = TransportOperationClass(rawValue: operationClass) else {
            reject("INVALID_CONFIG", "Transport pause operation is invalid", nil)
            return
        }
        do {
            try collector.armPause(for: operation)
            resolve(nil)
        } catch {
            rejectWithError(reject, error)
        }
    }

    @objc
    public func awaitTransportPause(
        _ operationClass: String,
        timeoutMs: Double,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let collector = transportObservations,
              let operation = TransportOperationClass(rawValue: operationClass),
              timeoutMs.isFinite, timeoutMs >= 1, timeoutMs <= 60_000 else {
            reject("INVALID_CONFIG", "Transport pause wait is invalid", nil)
            return
        }
        Task {
            do {
                try await collector.awaitPause(for: operation, timeout: timeoutMs / 1_000)
                DispatchQueue.main.async { resolve(nil) }
            } catch {
                DispatchQueue.main.async { self.rejectWithError(reject, error) }
            }
        }
    }

    @objc
    public func resumeTransportPause(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let collector = transportObservations else {
            reject("NOT_CONNECTED", "Transport observation is not configured", nil)
            return
        }
        do {
            try collector.resumePause()
            resolve(nil)
        } catch {
            rejectWithError(reject, error)
        }
    }

    @objc
    public func getProcessIdentity(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        resolve("ios-app:\(getpid())")
    }

    @objc
    public func clearRejectedMutations(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = client else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        do {
            try client.clearRejectedMutations()
            resolve(nil)
        } catch {
            rejectWithError(reject, error)
        }
    }

    // MARK: - Helpers

    private func encodeBridgeJSON(_ value: Any) throws -> String {
        guard JSONSerialization.isValidJSONObject(value) else {
            throw SynchroError.invalidResponse(message: "bridge JSON value is invalid")
        }
        let data = try JSONSerialization.data(withJSONObject: value, options: [.sortedKeys])
        guard let json = String(data: data, encoding: .utf8) else {
            throw SynchroError.invalidResponse(message: "bridge JSON encoding failed")
        }
        return json
    }

    private func pendingMutationPayload(_ mutation: PendingMutationInspection) -> [String: Any] {
        [
            "mutationID": mutation.mutationID,
            "localOrder": mutation.localOrder,
            "tableID": mutation.tableID,
            "tableName": mutation.tableName,
            "recordID": mutation.recordID,
            "primaryKeyFieldID": mutation.primaryKeyFieldID,
            "primaryKeyLogicalType": mutation.primaryKeyLogicalType,
            "operation": mutation.operation.rawValue,
            "authoredSchema": [
                "version": mutation.authoredSchema.version,
                "hash": mutation.authoredSchema.hash
            ],
            "baseVersion": mutation.baseVersion ?? NSNull(),
            "clientVersion": mutation.clientVersion,
            "status": mutation.status.rawValue,
            "sourceKind": mutation.sourceKind,
            "dependsOnMutationID": mutation.dependsOnMutationID ?? NSNull(),
            "normalizedMutationID": mutation.normalizedMutationID ?? NSNull(),
            "sealedBatchID": mutation.sealedBatchID ?? NSNull(),
            "sealedOrdinal": mutation.sealedOrdinal ?? NSNull(),
            "authoredFields": mutation.authoredFields.map { field in
                [
                    "fieldID": field.fieldID,
                    "logicalType": field.logicalType,
                    "value": anyCodableToJSONObject(field.value.value)
                ] as [String: Any]
            }
        ]
    }

    private func rejectedMutationPayload(_ mutation: RejectedMutationInspection) -> [String: Any] {
        [
            "mutationID": mutation.mutationID,
            "tableName": mutation.tableName,
            "recordID": mutation.recordID,
            "status": mutation.status.rawValue,
            "code": mutation.code.rawValue,
            "message": mutation.message ?? NSNull(),
            "serverRowJSON": mutation.serverRowJSON ?? NSNull(),
            "serverVersion": mutation.serverVersion ?? NSNull(),
            "mutationJSON": mutation.mutationJSON,
            "rejectionJSON": mutation.rejectionJSON,
            "createdAt": mutation.createdAt,
            "updatedAt": mutation.updatedAt
        ]
    }

    private enum BridgeParamError: LocalizedError {
        case invalidNumber(index: Int)
        case invalidTag(index: Int, reason: String)
        case unsupported(index: Int, type: String)

        var errorDescription: String? {
            switch self {
            case .invalidNumber(let index):
                return "Invalid SQL bind number at index \(index)"
            case .invalidTag(let index, let reason):
                return "Invalid SQL bind tag at index \(index): \(reason)"
            case .unsupported(let index, let type):
                return "Unsupported SQL bind value at index \(index): \(type)"
            }
        }
    }

    private static let maxSafeInteger: Int64 = 9_007_199_254_740_991
    private static let minSafeInteger: Int64 = -9_007_199_254_740_991

    private func canonicalBase64URL(_ data: Data) -> String {
        data.base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }

    private func decodeCanonicalBase64URL(_ value: String, index: Int) throws -> Data {
        guard value.utf8.allSatisfy({ byte in
            (byte >= 65 && byte <= 90) ||
            (byte >= 97 && byte <= 122) ||
            (byte >= 48 && byte <= 57) ||
            byte == 45 || byte == 95
        }) else {
            throw BridgeParamError.invalidTag(index: index, reason: "bytes must use base64url")
        }

        let remainder = value.utf8.count % 4
        guard remainder != 1 else {
            throw BridgeParamError.invalidTag(index: index, reason: "bytes have invalid base64url length")
        }

        let translated = value
            .replacingOccurrences(of: "-", with: "+")
            .replacingOccurrences(of: "_", with: "/")
        let padded = translated + String(repeating: "=", count: (4 - remainder) % 4)
        guard let data = Data(base64Encoded: padded), canonicalBase64URL(data) == value else {
            throw BridgeParamError.invalidTag(index: index, reason: "bytes are not canonical base64url")
        }
        return data
    }

    private func decodeCanonicalInt64(_ value: String, index: Int) throws -> Int64 {
        guard value == "0" || value.range(of: "^-?[1-9][0-9]*$", options: .regularExpression) != nil,
              let parsed = Int64(value), String(parsed) == value else {
            throw BridgeParamError.invalidTag(index: index, reason: "int64 is not canonical or is out of range")
        }
        return parsed
    }

    private func bridgeTagToParam(_ value: Any, index: Int) throws -> (any DatabaseValueConvertible)? {
        guard let tag = value as? NSDictionary,
              tag.count == 2,
              let type = tag["type"] as? String else {
            throw BridgeParamError.invalidTag(index: index, reason: "tag must contain exactly type and payload fields")
        }

        switch type {
        case "bytes":
            guard let base64 = tag["base64"] as? String, tag["value"] == nil else {
                throw BridgeParamError.invalidTag(index: index, reason: "bytes tag must contain base64")
            }
            return try decodeCanonicalBase64URL(base64, index: index)
        case "int64":
            guard let value = tag["value"] as? String, tag["base64"] == nil else {
                throw BridgeParamError.invalidTag(index: index, reason: "int64 tag must contain value")
            }
            return try decodeCanonicalInt64(value, index: index)
        default:
            throw BridgeParamError.invalidTag(index: index, reason: "unknown tag type")
        }
    }

    private func bridgeValueToParam(_ value: Any, index: Int) throws -> (any DatabaseValueConvertible)? {
        switch value {
        case is NSNull:
            return nil
        case let boolVal as Bool:
            return boolVal ? 1 : 0
        case let intVal as Int:
            guard Int64(intVal) >= Self.minSafeInteger && Int64(intVal) <= Self.maxSafeInteger else {
                throw BridgeParamError.invalidNumber(index: index)
            }
            return intVal
        case let int64Val as Int64:
            guard int64Val >= Self.minSafeInteger && int64Val <= Self.maxSafeInteger else {
                throw BridgeParamError.invalidNumber(index: index)
            }
            return int64Val
        case let doubleVal as Double:
            guard doubleVal.isFinite else {
                throw BridgeParamError.invalidNumber(index: index)
            }
            if doubleVal.rounded(.towardZero) == doubleVal,
               (doubleVal < Double(Self.minSafeInteger) || doubleVal > Double(Self.maxSafeInteger)) {
                throw BridgeParamError.invalidNumber(index: index)
            }
            return doubleVal
        case let stringVal as String:
            return stringVal
        case let dictionary as NSDictionary:
            return try bridgeTagToParam(dictionary, index: index)
        case let numberVal as NSNumber:
            if CFGetTypeID(numberVal) == CFBooleanGetTypeID() {
                return numberVal.boolValue ? 1 : 0
            }
            let doubleValue = numberVal.doubleValue
            guard doubleValue.isFinite else {
                throw BridgeParamError.invalidNumber(index: index)
            }
            if doubleValue.isFinite,
               doubleValue.rounded(.towardZero) == doubleValue,
               doubleValue >= Double(Self.minSafeInteger),
               doubleValue <= Double(Self.maxSafeInteger) {
                return Int64(doubleValue)
            }
            if doubleValue.rounded(.towardZero) == doubleValue {
                throw BridgeParamError.invalidNumber(index: index)
            }
            return doubleValue
        default:
            throw BridgeParamError.unsupported(index: index, type: String(describing: type(of: value)))
        }
    }

    private func bridgeParams(_ values: [Any]) throws -> [(any DatabaseValueConvertible)?] {
        try values.enumerated().map { index, value in
            try bridgeValueToParam(value, index: index)
        }
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
            if v >= Self.minSafeInteger && v <= Self.maxSafeInteger {
                return NSNumber(value: v)
            }
            return ["type": "int64", "value": String(v)]
        case .double(let v):
            return NSNumber(value: v)
        case .string(let v):
            return v
        case .blob(let v):
            return ["type": "bytes", "base64": canonicalBase64URL(v)]
        }
    }

    private func rowToBridgeRow(_ row: Row) -> [String: Any] {
        var dict: [String: Any] = [:]
        for column in row.columnNames {
            let dbValue: DatabaseValue = row[column]
            dict[column] = databaseValueToFoundation(dbValue)
        }
        return dict
    }

    private func rowsToBridgeRows(_ rows: [Row]) -> [[String: Any]] {
        rows.map { rowToBridgeRow($0) }
    }
}
