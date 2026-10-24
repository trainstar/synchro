import Foundation
import Darwin
import os
import React
import GRDB
@_spi(Inspection) import Synchro

// MARK: - Event Delegate Protocol

@objc public protocol SynchroEventEmitting: AnyObject {
    func emitEvent(_ name: String, body: NSDictionary)
}

// MARK: - Transaction Session

final class TransactionSession: Sendable {
    private struct State: Sendable {
        var operations: [TransactionOp] = []
        var closed = false
        var abortError: Error?
        var finalSettlement: TransactionFinalSettlement?
        var finished = false
        var finishWaiters: [CheckedContinuation<Void, Never>] = []
        var beginSettlement: ReactPromiseSettlement?
    }

    private struct BeginAction: Sendable {
        let settlement: ReactPromiseSettlement?
        let error: Error?
    }

    private enum NextAction: Sendable {
        case operation(TransactionOp)
        case closed
        case aborted(Error)
        case wait
    }

    private let state: OSAllocatedUnfairLock<State>
    private let wake = DispatchSemaphore(value: 0)
    let isWrite: Bool

    init(isWrite: Bool, beginSettlement: ReactPromiseSettlement) {
        self.isWrite = isWrite
        state = OSAllocatedUnfairLock(initialState: State(beginSettlement: beginSettlement))
    }

    /// Settles begin when the native transaction callback starts. A close that
    /// aborted the session before the callback started rejects begin instead.
    func acceptBegin(_ txID: String) throws {
        let action = state.withLock { state -> BeginAction in
            let settlement = state.beginSettlement
            state.beginSettlement = nil
            return BeginAction(settlement: settlement, error: state.abortError)
        }
        if let error = action.error {
            if let settlement = action.settlement {
                SynchroModuleImpl.rejectWithError(settlement, error)
            }
            throw error
        }
        action.settlement?.resolve(.value(.string(txID)))
    }

    /// Rejects begin when the transaction fails before its callback starts.
    func rejectBegin(_ error: Error) {
        let settlement = state.withLock { state in
            defer { state.beginSettlement = nil }
            return state.beginSettlement
        }
        if let settlement {
            SynchroModuleImpl.rejectWithError(settlement, error)
        }
    }

    func enqueue(_ op: TransactionOp) -> Bool {
        let accepted = state.withLock { state -> Bool in
            guard !state.closed else { return false }
            state.operations.append(op)
            return true
        }
        if accepted {
            wake.signal()
        }
        return accepted
    }

    func nextOperation(timeout: TimeInterval) throws -> TransactionOp? {
        let deadline = Date().addingTimeInterval(timeout)
        while true {
            let action = state.withLock { state -> NextAction in
                if let error = state.abortError {
                    return .aborted(error)
                }
                if !state.operations.isEmpty {
                    return .operation(state.operations.removeFirst())
                }
                return state.closed ? .closed : .wait
            }

            switch action {
            case .operation(let operation):
                return operation
            case .closed:
                return nil
            case .aborted(let error):
                throw error
            case .wait:
                let remaining = deadline.timeIntervalSinceNow
                let timedOut = remaining <= 0
                    || wake.wait(timeout: .now() + remaining) == .timedOut
                guard timedOut else { continue }
                let didTimeout = state.withLock { state -> Bool in
                    guard state.operations.isEmpty, !state.closed else { return false }
                    state.closed = true
                    return true
                }
                if didTimeout {
                    throw TransactionTimeoutError()
                }
            }
        }
    }

    @discardableResult
    func close() -> Bool {
        let pending = state.withLock { state -> [TransactionOp]? in
            guard !state.closed else { return nil }
            state.closed = true
            let pending = state.operations
            state.operations.removeAll()
            return pending
        }
        guard let pending else { return false }
        wake.signal()
        let error = TransactionAbortedError(message: "Transaction already completed")
        pending.forEach { $0.fail(error) }
        return true
    }

    func abort(_ error: Error) {
        let pending = state.withLock { state -> [TransactionOp]? in
            guard !state.closed else { return nil }
            state.closed = true
            state.abortError = error
            let pending = state.operations
            state.operations.removeAll()
            return pending
        }
        guard let pending else { return }
        wake.signal()
        pending.forEach { $0.fail(error) }
    }

    func currentAbortError() -> Error? {
        state.withLock { $0.abortError }
    }

    func setFinalSettlement(_ settlement: TransactionFinalSettlement) {
        state.withLock { $0.finalSettlement = settlement }
    }

    func completeFinal(_ result: Result<Void, Error>) {
        let settlement = state.withLock { $0.finalSettlement }
        settlement?.complete(result)
    }

    func markFinished() {
        let waiters = state.withLock { state -> [CheckedContinuation<Void, Never>] in
            state.finished = true
            let waiters = state.finishWaiters
            state.finishWaiters.removeAll()
            return waiters
        }
        waiters.forEach { $0.resume() }
    }

    func waitUntilFinished() async {
        await withCheckedContinuation { continuation in
            let resumeNow = state.withLock { state -> Bool in
                if state.finished {
                    return true
                }
                state.finishWaiters.append(continuation)
                return false
            }
            if resumeNow {
                continuation.resume()
            }
        }
    }
}

enum TransactionFinalSettlement: Sendable {
    case commit(ReactPromiseSettlement)
    case rollback(ReactPromiseSettlement)

    func complete(_ result: Result<Void, Error>) {
        switch (self, result) {
        case (.commit(let settlement), .success), (.rollback(let settlement), .success):
            settlement.resolve(.void)
        case (.commit(let settlement), .failure(let error as SynchroError)):
            SynchroModuleImpl.rejectWithError(settlement, error)
        case (.commit(let settlement), .failure(let error)),
             (.rollback(let settlement), .failure(let error)):
            settlement.reject("DATABASE_ERROR", error.localizedDescription, error)
        }
    }
}

enum TransactionOp: Sendable {
    case query(sql: String, params: Result<[BridgeValue], Error>, settlement: ReactPromiseSettlement)
    case queryOne(sql: String, params: Result<[BridgeValue], Error>, settlement: ReactPromiseSettlement)
    case execute(sql: String, params: Result<[BridgeValue], Error>, settlement: ReactPromiseSettlement)
    case commit(settlement: ReactPromiseSettlement)
    case rollback(settlement: ReactPromiseSettlement)

    func fail(_ error: Error) {
        settlement.reject("DATABASE_ERROR", error.localizedDescription, error)
    }

    private var settlement: ReactPromiseSettlement {
        switch self {
        case .query(_, _, let settlement),
             .queryOne(_, _, let settlement),
             .execute(_, _, let settlement),
             .commit(let settlement),
             .rollback(let settlement):
            return settlement
        }
    }
}

private struct TransactionTimeoutError: Error {}
private struct TransactionRollbackError: Error {}
private struct DatabaseAlreadyExistsError: Error {}
private struct InvalidAuthoredWriteOperationError: Error {}
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
    private struct RuntimeState: Sendable {
        var client: SynchroClient?
        var sessions: [String: TransactionSession] = [:]
        var acceptingTransactions = false
        var observers: [String: any Synchro.Cancellable] = [:]
        var statusSubscription: (any Synchro.Cancellable)?
        var syncEventSubscription: (any Synchro.Cancellable)?
        var conflictSubscription: (any Synchro.Cancellable)?
        var cachedBackoff: SyncBackoffEvent?
        var cachedFailure: SyncFailure?
        var transportObservations: TransportObservationCollector?
    }

    private struct DetachedState: Sendable {
        let sessions: [TransactionSession]
        let observers: [any Synchro.Cancellable]
        let subscriptions: [any Synchro.Cancellable]
        let transportObservations: TransportObservationCollector?
    }

    private let eventTarget = ReactEventTarget()
    private let runtime = OSAllocatedUnfairLock(initialState: RuntimeState())
    private let authRequests = AuthRequestStore()

    @objc public var eventDelegate: SynchroEventEmitting? {
        get { eventTarget.currentDelegate }
        set { eventTarget.setDelegate(newValue) }
    }

    var client: SynchroClient? {
        runtime.withLock { $0.client }
    }

    var activeTransactionIDs: [String] {
        runtime.withLock { Array($0.sessions.keys) }
    }
    private let lifecycleMutex = LifecycleMutex()
    private let databaseBridgeQueue = DispatchQueue(label: "com.trainstar.synchro.react-native.database")

    @objc public func rejectWithError(_ reject: @escaping RCTPromiseRejectBlock, _ error: Error) {
        Self.rejectWithError(reject, error)
    }

    fileprivate static func rejectWithError(_ settlement: ReactPromiseSettlement, _ error: Error) {
        let rejection = rejection(for: error)
        settlement.reject(rejection.code, rejection.message, rejection.error)
    }

    private static func rejectWithError(_ reject: RCTPromiseRejectBlock, _ error: Error) {
        let rejection = rejection(for: error)
        reject(rejection.code, rejection.message, rejection.error)
    }

    private static func rejection(for error: Error) -> (code: String?, message: String?, error: Error?) {
        if let synchroError = error as? SynchroError {
            let (code, userInfo) = mapSynchroError(synchroError)
            let base: [String: Any] = ["message": synchroError.localizedDescription]
            return (
                code,
                synchroError.localizedDescription,
                NSError(
                    domain: "SynchroModule",
                    code: 0,
                    userInfo: base.merging(userInfo) { _, new in new }
                )
            )
        } else if error is TransactionTimeoutError {
            return ("TRANSACTION_TIMEOUT", "Transaction timed out due to inactivity", nil)
        } else if error is TransactionAbortedError {
            return ("NOT_CONNECTED", "Client closed during transaction", error)
        } else if error is InvalidAuthoredWriteOperationError {
            return ("INVALID_CONFIG", "Invalid authored write operation", nil)
        } else {
            return ("UNKNOWN", error.localizedDescription, error)
        }
    }

    private static func mapSynchroError(_ error: SynchroError) -> (String, [String: Any]) {
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
        case .atomicGroupInvalid(let reason):
            return ("atomic_group_invalid", ["reason": reason.rawValue])
        }
    }

    // MARK: - Lifecycle

    @objc
    public func initialize(
        _ config: NSDictionary,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        let settlement = ReactPromiseSettlement(resolve: resolve, reject: reject)
        guard let dbPath = config["dbPath"] as? String,
              let serverURL = config["serverURL"] as? String,
              let clientID = config["clientID"] as? String,
              let platform = config["platform"] as? String,
               let appVersion = config["appVersion"] as? String,
               let url = URL(string: serverURL) else {
            settlement.reject("INVALID_CONFIG", "Missing required config fields", nil)
            return
        }
        let resolvedDbPath = Self.resolveDatabasePath(dbPath)

        let syncInterval = config["syncInterval"] as? Double ?? 30
        let pushDebounce = config["pushDebounce"] as? Double ?? 0.5
        let maxRetryAttempts = config["maxRetryAttempts"] as? Int ?? 5
        let pullPageSize = config["pullPageSize"] as? Int ?? 100
        let pushBatchSize = config["pushBatchSize"] as? Int ?? 100
        let seedDatabasePath = config["seedDatabasePath"] as? String
        let configuredTransportCapacity = config["transportObservationCapacity"] as? Int ?? 0
        let requireNewDatabase = config["requireNewDatabase"] as? Bool ?? false
        if configuredTransportCapacity < 0 || configuredTransportCapacity > 512 {
            settlement.reject("INVALID_CONFIG", "Transport observation capacity is invalid", nil)
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
        let authRequests = authRequests
        let eventTarget = eventTarget
        let synchroConfig = SynchroConfig(
            dbPath: resolvedDbPath,
            serverURL: url,
            authProvider: {
                try await withCheckedThrowingContinuation { continuation in
                    let requestID = UUID().uuidString
                    guard authRequests.insert(continuation, for: requestID) else {
                        continuation.resume(throwing: TransactionAbortedError(
                            message: "Client closed during authentication"
                        ))
                        return
                    }
                    eventTarget.emit(
                        "onAuthRequest",
                        body: .object(["requestID": .string(requestID)])
                    )
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
        let lifecycleMutex = lifecycleMutex
        let runtime = runtime
        let databaseBridgeQueue = databaseBridgeQueue
        Task {
            do {
                try await Self.withLifecycleLock(lifecycleMutex) {
                    await Self.clearRuntimeState(
                        runtime: runtime,
                        authRequests: authRequests,
                        databaseBridgeQueue: databaseBridgeQueue
                    )
                    try await runtime.withLock { $0.client }?.close()
                    if requireNewDatabase && FileManager.default.fileExists(atPath: resolvedDbPath) {
                        throw DatabaseAlreadyExistsError()
                    }
                    let client = try SynchroClient(config: synchroConfig)
                    authRequests.activate()
                    runtime.withLock {
                        $0.client = client
                        $0.acceptingTransactions = true
                        $0.transportObservations = transportObservations
                    }
                    Self.wireClientEvents(client, runtime: runtime, eventTarget: eventTarget)
                }
                DispatchQueue.main.async {
                    settlement.resolve(.void)
                }
            } catch is DatabaseAlreadyExistsError {
                DispatchQueue.main.async {
                    settlement.reject("INVALID_CONFIG", "Database already exists", nil)
                }
            } catch {
                DispatchQueue.main.async {
                    if resolvedSeedPath != nil {
                        settlement.reject("INVALID_SEED", "Seed database failed validation", error)
                    } else {
                        Self.rejectWithError(settlement, error)
                    }
                }
            }
        }
    }

    @objc
    public func resolveAuthRequest(_ requestID: String, token: String) {
        authRequests.resolve(requestID, token: token)
    }

    @objc
    public func rejectAuthRequest(_ requestID: String, error: String) {
        authRequests.reject(
            requestID,
            error: NSError(domain: "Auth", code: 0, userInfo: [NSLocalizedDescriptionKey: error])
        )
    }

    @objc
    public func close(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        let settlement = ReactPromiseSettlement(resolve: resolve, reject: reject)
        let lifecycleMutex = lifecycleMutex
        let runtime = runtime
        let authRequests = authRequests
        let databaseBridgeQueue = databaseBridgeQueue
        Task {
            do {
                try await Self.withLifecycleLock(lifecycleMutex) {
                    await Self.clearRuntimeState(
                        runtime: runtime,
                        authRequests: authRequests,
                        databaseBridgeQueue: databaseBridgeQueue
                    )
                    try await runtime.withLock { $0.client }?.close()
                    runtime.withLock {
                        $0.client = nil
                        $0.acceptingTransactions = false
                    }
                }
                DispatchQueue.main.async {
                    settlement.resolve(.void)
                }
            } catch {
                DispatchQueue.main.async {
                    Self.rejectWithError(settlement, error)
                }
            }
        }
    }

    @objc
    public func getPath(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = runtime.withLock({ $0.client }) else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        resolve(client.path)
    }

    private static func resolveDatabasePath(_ dbPath: String) -> String {
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
        let capturedParams = Result { try Self.captureBridgeValues(params) }
        submitDatabaseOperation(
            resolve: resolve,
            reject: reject,
            operation: { client in
                .value(try Self.rowsToBridgeValue(client.query(sql, params: Self.bridgeParams(capturedParams.get()))))
            }
        )
    }

    @objc
    public func queryOne(
        _ sql: String,
        params: [Any],
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        let capturedParams = Result { try Self.captureBridgeValues(params) }
        submitDatabaseOperation(
            resolve: resolve,
            reject: reject,
            operation: { client in
                if let row = try client.queryOne(sql, params: Self.bridgeParams(capturedParams.get())) {
                    return .value(Self.rowToBridgeValue(row))
                }
                return .value(.null)
            }
        )
    }

    @objc
    public func execute(
        _ sql: String,
        params: [Any],
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        let capturedParams = Result { try Self.captureBridgeValues(params) }
        submitDatabaseOperation(
            resolve: resolve,
            reject: reject,
            operation: { client in
                let result = try client.execute(sql, params: Self.bridgeParams(capturedParams.get()))
                return .value(.object(["rowsAffected": .integer(Int64(result.rowsAffected))]))
            }
        )
    }

    @objc
    public func executeAuthoredWrite(
        _ tableName: String,
        operation: String,
        columnNames: [String],
        sql: String,
        values: [Any],
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        let capturedValues = Result { try Self.captureBridgeValues(values) }
        submitDatabaseOperation(
            resolve: resolve,
            reject: reject,
            operation: { client in
                guard let authoredOperation = Synchro.Operation(rawValue: operation) else {
                    throw InvalidAuthoredWriteOperationError()
                }
                let params = try Self.bridgeParams(capturedValues.get())
                let result = try client.authoredWriteTransaction(
                    tableName: tableName,
                    operation: authoredOperation,
                    columnNames: columnNames
                ) { transaction in
                    try transaction.execute(sql, params: params)
                }
                return .value(.object(["rowsAffected": .integer(Int64(result.rowsAffected))]))
            }
        )
    }

    @objc
    public func executeBatch(
        _ statements: [[String: Any]],
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        let capturedStatements = Result { try Self.captureBridgeStatements(statements) }
        submitDatabaseOperation(
            resolve: resolve,
            reject: reject,
            operation: { client in
                let total = try client.executeBatch(try Self.bridgeStatements(capturedStatements.get()))
                return .value(.object(["totalRowsAffected": .integer(Int64(total))]))
            }
        )
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
    public func beginAtomicWriteTransaction(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        beginTransaction(isWrite: true, isAtomic: true, resolve: resolve, reject: reject)
    }

    @objc
    public func beginReadTransaction(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        beginTransaction(isWrite: false, resolve: resolve, reject: reject)
    }

    private static func runTransactionLoop(
        session: TransactionSession,
        txID: String,
        query: (String, [(any DatabaseValueConvertible)?]) throws -> [Row],
        queryOne: (String, [(any DatabaseValueConvertible)?]) throws -> Row?,
        execute: ((String, [(any DatabaseValueConvertible)?]) throws -> Int)?
    ) throws {
        try session.acceptBegin(txID)

        while true {
            guard let op = try session.nextOperation(timeout: 5) else {
                break
            }

            switch op {
            case .query(let sql, let params, let settlement):
                do {
                    let rows = try query(sql, try bridgeParams(params.get()))
                    if let abortError = session.currentAbortError() {
                        throw abortError
                    }
                    settlement.resolve(.value(rowsToBridgeValue(rows)))
                } catch {
                    settlement.reject("DATABASE_ERROR", error.localizedDescription, error)
                }

            case .queryOne(let sql, let params, let settlement):
                do {
                    let row = try queryOne(sql, try bridgeParams(params.get()))
                    if let abortError = session.currentAbortError() {
                        throw abortError
                    }
                    settlement.resolve(.value(row.map(rowToBridgeValue) ?? .null))
                } catch {
                    settlement.reject("DATABASE_ERROR", error.localizedDescription, error)
                }

            case .execute(let sql, let params, let settlement):
                do {
                    guard let execute else {
                        throw TransactionReadOnlyError()
                    }
                    let rowsAffected = try execute(sql, try bridgeParams(params.get()))
                    if let abortError = session.currentAbortError() {
                        throw abortError
                    }
                    settlement.resolve(.value(.object(["rowsAffected": .integer(Int64(rowsAffected))])))
                } catch {
                    settlement.reject("DATABASE_ERROR", error.localizedDescription, error)
                }

            case .commit(let settlement):
                guard session.close() else {
                    let error = session.currentAbortError()
                        ?? TransactionAbortedError(message: "Transaction already completed")
                    settlement.reject("DATABASE_ERROR", error.localizedDescription, error)
                    throw error
                }
                session.setFinalSettlement(.commit(settlement))
                return

            case .rollback(let settlement):
                guard session.close() else {
                    let error = session.currentAbortError()
                        ?? TransactionAbortedError(message: "Transaction already completed")
                    settlement.reject("DATABASE_ERROR", error.localizedDescription, error)
                    throw error
                }
                session.setFinalSettlement(.rollback(settlement))
                throw TransactionRollbackError()
            }
        }
    }

    private func beginTransaction(
        isWrite: Bool,
        isAtomic: Bool = false,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        let settlement = ReactPromiseSettlement(resolve: resolve, reject: reject)
        let txID = UUID().uuidString
        let session = TransactionSession(isWrite: isWrite, beginSettlement: settlement)
        let admittedClient = runtime.withLock { state -> SynchroClient? in
            guard state.acceptingTransactions, let client = state.client else { return nil }
            state.sessions[txID] = session
            return client
        }
        guard let client = admittedClient else {
            settlement.reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }

        let runtime = runtime
        DispatchQueue.global(qos: .userInitiated).async {
            defer {
                session.close()
                _ = runtime.withLock { $0.sessions.removeValue(forKey: txID) }
                session.markFinished()
            }
            do {
                let finalResult: Result<Void, Error>
                if isWrite {
                    let body: (ApplicationTransaction) throws -> Void = { transaction in
                        try Self.runTransactionLoop(
                            session: session,
                            txID: txID,
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
                    if isAtomic {
                        try client.atomicWriteTransaction(body)
                    } else {
                        try client.writeTransaction(body)
                    }
                } else {
                    try client.readTransaction { db in
                        try Self.runTransactionLoop(
                            session: session,
                            txID: txID,
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
                // Acquisition can fail before the callback settles begin.
                session.rejectBegin(error)
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
        let settlement = ReactPromiseSettlement(resolve: resolve, reject: reject)
        guard let session = getSession(txID) else {
            settlement.reject("TRANSACTION_TIMEOUT", "Transaction not found or expired", nil)
            return
        }
        let capturedParams = Result { try Self.captureBridgeValues(params) }
        let op = TransactionOp.query(sql: sql, params: capturedParams, settlement: settlement)
        guard session.enqueue(op) else {
            settlement.reject("TRANSACTION_TIMEOUT", "Transaction not found or expired", nil)
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
        let settlement = ReactPromiseSettlement(resolve: resolve, reject: reject)
        guard let session = getSession(txID) else {
            settlement.reject("TRANSACTION_TIMEOUT", "Transaction not found or expired", nil)
            return
        }
        let capturedParams = Result { try Self.captureBridgeValues(params) }
        let op = TransactionOp.queryOne(sql: sql, params: capturedParams, settlement: settlement)
        guard session.enqueue(op) else {
            settlement.reject("TRANSACTION_TIMEOUT", "Transaction not found or expired", nil)
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
        let settlement = ReactPromiseSettlement(resolve: resolve, reject: reject)
        guard let session = getSession(txID) else {
            settlement.reject("TRANSACTION_TIMEOUT", "Transaction not found or expired", nil)
            return
        }
        guard session.isWrite else {
            settlement.reject("DATABASE_ERROR", "Read transactions cannot execute SQL", nil)
            return
        }
        let capturedParams = Result { try Self.captureBridgeValues(params) }
        let op = TransactionOp.execute(sql: sql, params: capturedParams, settlement: settlement)
        guard session.enqueue(op) else {
            settlement.reject("TRANSACTION_TIMEOUT", "Transaction not found or expired", nil)
            return
        }
    }

    @objc
    public func commitTransaction(
        _ txID: String,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        let settlement = ReactPromiseSettlement(resolve: resolve, reject: reject)
        guard let session = getSession(txID) else {
            settlement.reject("TRANSACTION_TIMEOUT", "Transaction not found or expired", nil)
            return
        }
        let op = TransactionOp.commit(settlement: settlement)
        guard session.enqueue(op) else {
            settlement.reject("TRANSACTION_TIMEOUT", "Transaction not found or expired", nil)
            return
        }
    }

    @objc
    public func rollbackTransaction(
        _ txID: String,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        let settlement = ReactPromiseSettlement(resolve: resolve, reject: reject)
        guard let session = getSession(txID) else {
            settlement.resolve(.void)
            return
        }
        let op = TransactionOp.rollback(settlement: settlement)
        guard session.enqueue(op) else {
            settlement.resolve(.void)
            return
        }
    }

    private func getSession(_ txID: String) -> TransactionSession? {
        runtime.withLock { $0.sessions[txID] }
    }

    private static func wireClientEvents(
        _ client: SynchroClient,
        runtime: OSAllocatedUnfairLock<RuntimeState>,
        eventTarget: ReactEventTarget
    ) {
        runtime.withLock {
            $0.cachedBackoff = nil
            $0.cachedFailure = nil
        }

        let statusSubscription = client.onStatusChange { status in
            guard status != .backoff, status != .error else { return }
            eventTarget.emit(
                "onStatusChange",
                body: bridgeValue(statusPayload(status, client: client, runtime: runtime))
            )
        }

        let syncEventSubscription = client.onSyncEvent { event in
            recordStatusDetails(event, runtime: runtime)
            eventTarget.emit("onSyncEvent", body: bridgeValue(syncEventPayload(event)))
            switch event {
            case .backoff, .failure:
                eventTarget.emit(
                    "onStatusChange",
                    body: bridgeValue(statusPayload(client.getSyncStatus(), client: client, runtime: runtime))
                )
            default:
                break
            }
        }

        let conflictSubscription = client.onConflict { event in
            eventTarget.emit("onConflict", body: bridgeValue(conflictPayload(event)))
        }

        runtime.withLock {
            $0.statusSubscription = statusSubscription
            $0.syncEventSubscription = syncEventSubscription
            $0.conflictSubscription = conflictSubscription
        }
    }

    private static func withLifecycleLock(
        _ mutex: LifecycleMutex,
        operation: @escaping @Sendable () async throws -> Void
    ) async throws {
        await mutex.lock()
        do {
            try await operation()
            await mutex.unlock()
        } catch {
            await mutex.unlock()
            throw error
        }
    }

    private func submitDatabaseOperation(
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock,
        operation: @escaping @Sendable (SynchroClient) throws -> BridgePromiseValue
    ) {
        let settlement = ReactPromiseSettlement(resolve: resolve, reject: reject)
        let queue = databaseBridgeQueue
        let admissionGate = DispatchSemaphore(value: 0)
        let admitted = runtime.withLock { state -> Bool in
            guard state.acceptingTransactions, let client = state.client else { return false }
            queue.async {
                admissionGate.wait()
                do {
                    settlement.resolve(try operation(client))
                } catch {
                    Self.rejectWithError(settlement, error)
                }
            }
            return true
        }
        admissionGate.signal()
        guard admitted else {
            settlement.reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
    }

    private static func detachRuntimeState(
        _ runtime: OSAllocatedUnfairLock<RuntimeState>
    ) -> DetachedState {
        runtime.withLock { state in
            state.acceptingTransactions = false
            let detached = DetachedState(
                sessions: Array(state.sessions.values),
                observers: Array(state.observers.values),
                subscriptions: [
                    state.statusSubscription,
                    state.syncEventSubscription,
                    state.conflictSubscription,
                ].compactMap { $0 },
                transportObservations: state.transportObservations
            )
            state.sessions.removeAll()
            state.observers.removeAll()
            state.statusSubscription = nil
            state.syncEventSubscription = nil
            state.conflictSubscription = nil
            state.cachedBackoff = nil
            state.cachedFailure = nil
            state.transportObservations = nil
            return detached
        }
    }

    private static func clearRuntimeState(
        runtime: OSAllocatedUnfairLock<RuntimeState>,
        authRequests: AuthRequestStore,
        databaseBridgeQueue: DispatchQueue
    ) async {
        let detached = detachRuntimeState(runtime)
        detached.transportObservations?.cancelPauseBarrier()
        detached.observers.forEach { $0.cancel() }
        detached.subscriptions.forEach { $0.cancel() }
        authRequests.cancelAll()
        detached.sessions.forEach {
            $0.abort(TransactionAbortedError(message: "Client closed during transaction"))
        }
        for session in detached.sessions {
            await session.waitUntilFinished()
        }
        await withCheckedContinuation { continuation in
            databaseBridgeQueue.async {
                continuation.resume()
            }
        }
    }

    private static func statusPayload(
        _ status: SyncStatus,
        client: SynchroClient,
        runtime: OSAllocatedUnfairLock<RuntimeState>
    ) -> [String: Any] {
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
            let backoff = runtime.withLock { $0.cachedBackoff }
            if let backoff {
                payload["retryAt"] = iso8601String(backoff.retryAt)
                payload["operation"] = backoff.operation.rawValue
            }
        case .error:
            let cachedFailure = runtime.withLock { $0.cachedFailure }
            let persistedFailure: SyncFailure?
            persistedFailure = try? client.getBlockingFailure()
            if let failure = cachedFailure ?? persistedFailure {
                payload["failure"] = failurePayload(failure)
            }
        }
        return payload
    }

    private static func recordStatusDetails(
        _ event: SyncEvent,
        runtime: OSAllocatedUnfairLock<RuntimeState>
    ) {
        runtime.withLock { state in
            switch event {
            case .backoff(let backoff):
                state.cachedBackoff = backoff
                state.cachedFailure = nil
            case .failure(let failure):
                state.cachedFailure = failure
                state.cachedBackoff = nil
            default:
                break
            }
        }
    }

    private static func schemaPayload(_ schema: SchemaRef) -> [String: Any] {
        ["version": schema.version, "hash": schema.hash]
    }

    private static func failurePayload(_ failure: SyncFailure) -> [String: Any] {
        [
            "operation": failure.operation.rawValue,
            "code": failure.code.rawValue,
            "retryable": failure.retryable,
            "message": failure.message,
            "recoveryAction": failure.recoveryAction.rawValue,
            "metadata": failure.metadata,
        ]
    }

    private static func iso8601String(_ date: Date) -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter.string(from: date)
    }

    private static func syncEventPayload(_ event: SyncEvent) -> [String: Any] {
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

    private static func conflictPayload(_ event: ConflictEvent) -> [String: Any] {
        [
            "table": event.table,
            "recordID": event.recordID,
            "clientDataJson": encodeAnyCodableMap(event.clientData) ?? NSNull(),
            "serverDataJson": encodeAnyCodableMap(event.serverData) ?? NSNull()
        ]
    }

    private static func encodeRejectedMutations(_ results: [RejectedMutation]) -> String {
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

    private static func anyCodableMapToJSONObject(_ value: [String: AnyCodable]?) -> Any {
        guard let value else { return NSNull() }
        return value.mapValues { anyCodableToJSONObject($0.value) }
    }

    private static func anyCodableToJSONObject(_ value: Any?) -> Any {
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

    private static func encodeAnyCodableMap(_ value: [String: AnyCodable]?) -> String? {
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
        submitDatabaseOperation(
            resolve: resolve,
            reject: reject,
            operation: { client in
                let columns = try Self.parseColumns(columnsJson)
                let options = try optionsJson.flatMap { try Self.parseTableOptions($0) }
                try client.createTable(name, columns: columns, options: options)
                return .void
            }
        )
    }

    @objc
    public func alterTable(
        _ name: String,
        columnsJson: String,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        submitDatabaseOperation(
            resolve: resolve,
            reject: reject,
            operation: { client in
                let columns = try Self.parseColumns(columnsJson)
                try client.alterTable(name, addColumns: columns)
                return .void
            }
        )
    }

    @objc
    public func createIndex(
        _ table: String,
        columns: [String],
        unique: Bool,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        submitDatabaseOperation(
            resolve: resolve,
            reject: reject,
            operation: { client in
                try client.createIndex(table, columns: columns, unique: unique)
                return .void
            }
        )
    }

    // MARK: - Observation

    @objc
    public func addChangeObserver(
        _ observerID: String,
        tables: [String],
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let client = runtime.withLock({ state in
            state.acceptingTransactions ? state.client : nil
        }) else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        let eventTarget = eventTarget
        let cancellable = client.onChange(tables: tables) {
            eventTarget.emit(
                "onChange",
                body: .object(["observerID": .string(observerID)])
            )
        }
        let registration = runtime.withLock { state -> (Bool, (any Synchro.Cancellable)?) in
            guard state.acceptingTransactions, state.client === client else { return (false, nil) }
            return (true, state.observers.updateValue(cancellable, forKey: observerID))
        }
        guard registration.0 else {
            cancellable.cancel()
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        registration.1?.cancel()
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
        guard let client = runtime.withLock({ state in
            state.acceptingTransactions ? state.client : nil
        }) else {
            reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        do {
            let nativeParams = try Self.bridgeParams(Self.captureBridgeValues(params))
            let eventTarget = eventTarget
            let cancellable = client.watch(sql, params: nativeParams, tables: tables) { rows in
                eventTarget.emit(
                    "onQueryResult",
                    body: .object([
                        "observerID": .string(observerID),
                        "rows": Self.rowsToBridgeValue(rows),
                    ])
                )
            }
            let registration = runtime.withLock { state -> (Bool, (any Synchro.Cancellable)?) in
                guard state.acceptingTransactions, state.client === client else { return (false, nil) }
                return (true, state.observers.updateValue(cancellable, forKey: observerID))
            }
            guard registration.0 else {
                cancellable.cancel()
                reject("NOT_CONNECTED", "Client not initialized", nil)
                return
            }
            registration.1?.cancel()
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
        let cancellable = runtime.withLock { $0.observers.removeValue(forKey: observerID) }
        cancellable?.cancel()
        resolve(nil)
    }

    @objc
    public func start(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        let settlement = ReactPromiseSettlement(resolve: resolve, reject: reject)
        guard let client = runtime.withLock({ $0.client }) else {
            settlement.reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        Task {
            do {
                try await client.start()
                DispatchQueue.main.async {
                    settlement.resolve(.void)
                }
            } catch {
                DispatchQueue.main.async {
                    Self.rejectWithError(settlement, error)
                }
            }
        }
    }

    @objc
    public func stop(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        let settlement = ReactPromiseSettlement(resolve: resolve, reject: reject)
        guard let client = runtime.withLock({ $0.client }) else {
            settlement.reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        Task {
            await client.stop()
            DispatchQueue.main.async {
                settlement.resolve(.void)
            }
        }
    }

    @objc
    public func enterBackground(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        let settlement = ReactPromiseSettlement(resolve: resolve, reject: reject)
        guard let client = runtime.withLock({ $0.client }) else {
            settlement.reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        Task {
            await client.enterBackground()
            DispatchQueue.main.async {
                settlement.resolve(.void)
            }
        }
    }

    @objc
    public func enterForeground(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        let settlement = ReactPromiseSettlement(resolve: resolve, reject: reject)
        guard let client = runtime.withLock({ $0.client }) else {
            settlement.reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        Task {
            do {
                try await client.enterForeground()
                DispatchQueue.main.async {
                    settlement.resolve(.void)
                }
            } catch {
                DispatchQueue.main.async {
                    Self.rejectWithError(settlement, error)
                }
            }
        }
    }

    @objc
    public func retryAfterError(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        let settlement = ReactPromiseSettlement(resolve: resolve, reject: reject)
        guard let client = runtime.withLock({ $0.client }) else {
            settlement.reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        Task {
            do {
                try await client.retryAfterError()
                DispatchQueue.main.async {
                    settlement.resolve(.void)
                }
            } catch {
                DispatchQueue.main.async {
                    Self.rejectWithError(settlement, error)
                }
            }
        }
    }

    @objc
    public func resetSchemaAndStart(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        let settlement = ReactPromiseSettlement(resolve: resolve, reject: reject)
        guard let client = runtime.withLock({ $0.client }) else {
            settlement.reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        Task {
            do {
                try await client.resetSchemaAndStart()
                DispatchQueue.main.async {
                    settlement.resolve(.void)
                }
            } catch {
                DispatchQueue.main.async {
                    Self.rejectWithError(settlement, error)
                }
            }
        }
    }

    @objc
    public func syncNow(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        let settlement = ReactPromiseSettlement(resolve: resolve, reject: reject)
        guard let client = runtime.withLock({ $0.client }) else {
            settlement.reject("NOT_CONNECTED", "Client not initialized", nil)
            return
        }
        Task {
            do {
                try await client.syncNow()
                DispatchQueue.main.async {
                    settlement.resolve(.void)
                }
            } catch {
                DispatchQueue.main.async {
                    Self.rejectWithError(settlement, error)
                }
            }
        }
    }

    @objc
    public func pendingChangeCount(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        submitDatabaseOperation(
            resolve: resolve,
            reject: reject,
            operation: { client in
                .value(.integer(Int64(try client.pendingChangeCount())))
            }
        )
    }

    @objc
    public func getSyncStatus(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        let runtime = runtime
        submitDatabaseOperation(
            resolve: resolve,
            reject: reject,
            operation: { client in
                .value(.string(try Self.encodeBridgeJSON(Self.statusPayload(
                    client.getSyncStatus(),
                    client: client,
                    runtime: runtime
                ))))
            }
        )
    }

    @objc
    public func inspectPendingMutations(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        submitDatabaseOperation(
            resolve: resolve,
            reject: reject,
            operation: { client in
                let payload = try client.inspectPendingMutations().map(Self.pendingMutationPayload)
                return .value(.string(try Self.encodeBridgeJSON(payload)))
            }
        )
    }

    @objc
    public func inspectRetainedMutations(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        submitDatabaseOperation(
            resolve: resolve,
            reject: reject,
            operation: { client in
                let payload = try client.inspectRetainedMutations().map(Self.pendingMutationPayload)
                return .value(.string(try Self.encodeBridgeJSON(payload)))
            }
        )
    }

    @objc
    public func inspectRetainedMutationRecords(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        submitDatabaseOperation(
            resolve: resolve,
            reject: reject,
            operation: { client in
                let payload = try client.inspectRetainedMutationRecords().map(Self.retainedMutationPayload)
                return .value(.string(try Self.encodeBridgeJSON(payload)))
            }
        )
    }

    @objc
    public func inspectRejectedMutations(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        submitDatabaseOperation(
            resolve: resolve,
            reject: reject,
            operation: { client in
                let payload = try client.inspectRejectedMutations().map(Self.rejectedMutationPayload)
                return .value(.string(try Self.encodeBridgeJSON(payload)))
            }
        )
    }

    @objc
    public func inspectRejectedMutationRecords(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        submitDatabaseOperation(
            resolve: resolve,
            reject: reject,
            operation: { client in
                let payload = try client.inspectRejectedMutationRecords().map(Self.retainedRejectionPayload)
                return .value(.string(try Self.encodeBridgeJSON(payload)))
            }
        )
    }

    /// Reads client state, retained details, and the requested application rows
    /// from one read-only snapshot, as the native conformance runners do.
    @objc
    public func inspectClientStateSnapshot(
        _ rowStatements: [[String: Any]],
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        let capturedStatements = Result { try Self.captureBridgeStatements(rowStatements) }
        submitDatabaseOperation(
            resolve: resolve,
            reject: reject,
            operation: { client in
                let statements = try Self.bridgeStatements(capturedStatements.get())
                var rows: [Row] = []
                let snapshot = try SynchroInspection(client: client).captureSnapshot(maximumRecords: 512) { _, transaction in
                    for statement in statements {
                        rows += try transaction.query(statement.sql, params: statement.params)
                    }
                }
                let inspection: [String: Any] = [
                    "client_state": Self.clientStatePayload(snapshot.capture),
                    "migration_journal": try snapshot.capture.migrationJournal.map {
                        try JSONSerialization.jsonObject(with: JSONEncoder().encode($0))
                    } ?? NSNull(),
                    "migration_journal_truncated": snapshot.capture.migrationJournalTruncated,
                    "physical_schema": try JSONSerialization.jsonObject(with: JSONEncoder().encode(snapshot.capture.physicalSchema)),
                    "physical_schema_truncated": snapshot.capture.physicalSchemaTruncated,
                    "accepted_mutation_outcomes": snapshot.capture.acceptedMutationOutcomes,
                    "accepted_mutation_outcomes_truncated": snapshot.capture.acceptedMutationOutcomesTruncated,
                    "retained_mutations": snapshot.retainedMutations.map { $0.map(Self.retainedMutationPayload) } ?? NSNull(),
                    "rejected_mutations": snapshot.rejectedMutations.map { $0.map(Self.retainedRejectionPayload) } ?? NSNull(),
                ]
                return .value(.object([
                    "inspection": .string(try Self.encodeBridgeJSON(inspection)),
                    "applicationRows": Self.rowsToBridgeValue(rows),
                ]))
            }
        )
    }

    /// Reads one record's row metadata and every rebuild receipt. The inspection snapshot bounds
    /// row metadata, so a durable proof reads its identity directly.
    @objc
    public func inspectDurableState(
        _ tableName: String,
        recordID: String,
        resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        submitDatabaseOperation(
            resolve: resolve,
            reject: reject,
            operation: { client in
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
                return .value(.string(try Self.encodeBridgeJSON([
                    "row_metadata": metadata,
                    "rebuild_receipts": receipts,
                ])))
            }
        )
    }

    @objc
    public func inspectTransportObservations(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let transportObservations = runtime.withLock({ $0.transportObservations }) else {
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
        guard let collector = runtime.withLock({ $0.transportObservations }) else {
            reject("INVALID_CONFIG", "Transport pause operation is invalid", nil)
            return
        }
        do {
            if let checkpoint = MigrationCheckpoint(rawValue: operationClass) {
                try collector.armPause(for: checkpoint)
            } else if let operation = TransportOperationClass(rawValue: operationClass) {
                try collector.armPause(for: operation)
            } else {
                throw SynchroError.invalidResponse(message: "pause target is invalid")
            }
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
        let settlement = ReactPromiseSettlement(resolve: resolve, reject: reject)
        guard let collector = runtime.withLock({ $0.transportObservations }),
              timeoutMs.isFinite, timeoutMs >= 1, timeoutMs <= 60_000 else {
            settlement.reject("INVALID_CONFIG", "Transport pause wait is invalid", nil)
            return
        }
        Task {
            do {
                if let checkpoint = MigrationCheckpoint(rawValue: operationClass) {
                    try await collector.awaitPause(for: checkpoint, timeout: timeoutMs / 1_000)
                } else if let operation = TransportOperationClass(rawValue: operationClass) {
                    try await collector.awaitPause(for: operation, timeout: timeoutMs / 1_000)
                } else {
                    throw SynchroError.invalidResponse(message: "pause target is invalid")
                }
                DispatchQueue.main.async {
                    settlement.resolve(.void)
                }
            } catch {
                DispatchQueue.main.async {
                    Self.rejectWithError(settlement, error)
                }
            }
        }
    }

    @objc
    public func resumeTransportPause(
        _ resolve: @escaping RCTPromiseResolveBlock,
        reject: @escaping RCTPromiseRejectBlock
    ) {
        guard let collector = runtime.withLock({ $0.transportObservations }) else {
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
        submitDatabaseOperation(
            resolve: resolve,
            reject: reject,
            operation: { client in
                try client.clearRejectedMutations()
                return .void
            }
        )
    }

    // MARK: - Helpers

    private static func clientStatePayload(_ capture: ClientStateCaptureInspection) -> [String: Any] {
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
        return [
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
        ]
    }

    private struct CapturedBridgeStatement: Sendable {
        let sql: String
        let params: [BridgeValue]?
    }

    private static func captureBridgeValues(_ values: [Any]) throws -> [BridgeValue] {
        try values.enumerated().map { index, value in
            try captureBridgeValue(value, index: index)
        }
    }

    private static func captureBridgeValue(_ value: Any, index: Int) throws -> BridgeValue {
        switch value {
        case is NSNull:
            return .null
        case let value as String:
            return .string(value)
        case let value as NSNumber:
            if CFGetTypeID(value) == CFBooleanGetTypeID() {
                return .bool(value.boolValue)
            }
            let doubleValue = value.doubleValue
            guard doubleValue.isFinite else {
                throw BridgeParamError.invalidNumber(index: index)
            }
            if doubleValue.rounded(.towardZero) == doubleValue,
               doubleValue >= Double(Int64.min),
               doubleValue < Double(Int64.max) {
                return .integer(Int64(doubleValue))
            }
            return .double(doubleValue)
        case let value as NSDictionary:
            var object: [String: BridgeValue] = [:]
            for (key, fieldValue) in value {
                guard let key = key as? String else {
                    throw BridgeParamError.unsupported(index: index, type: "non-string object key")
                }
                object[key] = try captureBridgeValue(fieldValue, index: index)
            }
            return .object(object)
        case let value as NSArray:
            return .array(try value.map { try captureBridgeValue($0, index: index) })
        default:
            throw BridgeParamError.unsupported(index: index, type: String(describing: type(of: value)))
        }
    }

    private static func bridgeValue(_ value: Any) -> BridgeValue {
        do {
            return try captureBridgeValue(value, index: 0)
        } catch {
            preconditionFailure("Internal bridge value is invalid: \(error.localizedDescription)")
        }
    }

    private static func captureBridgeStatements(
        _ statements: [[String: Any]]
    ) throws -> [CapturedBridgeStatement] {
        try statements.enumerated().map { index, item in
            guard let sql = item["sql"] as? String else {
                throw SynchroError.invalidResponse(message: "Missing SQL at index \(index)")
            }
            let params = try (item["params"] as? [Any]).map(captureBridgeValues)
            return CapturedBridgeStatement(sql: sql, params: params)
        }
    }

    private static func bridgeStatements(
        _ statements: [CapturedBridgeStatement]
    ) throws -> [Synchro.SQLStatement] {
        try statements.map { statement in
            Synchro.SQLStatement(
                sql: statement.sql,
                params: try statement.params.map(bridgeParams)
            )
        }
    }

    private static func encodeBridgeJSON(_ value: Any) throws -> String {
        guard JSONSerialization.isValidJSONObject(value) else {
            throw SynchroError.invalidResponse(message: "bridge JSON value is invalid")
        }
        let data = try JSONSerialization.data(withJSONObject: value, options: [.sortedKeys])
        guard let json = String(data: data, encoding: .utf8) else {
            throw SynchroError.invalidResponse(message: "bridge JSON encoding failed")
        }
        return json
    }

    private static func retainedMutationPayload(_ value: RetainedMutationInspection) -> [String: Any] {
        switch value {
        case .current(let mutation):
            return pendingMutationPayload(mutation).merging(["representation": "current"]) { current, _ in current }
        case .legacy(let mutation):
            return [
                "representation": "legacy",
                "mutationID": mutation.mutationID,
                "localOrder": mutation.localOrder,
                "tableName": mutation.tableName,
                "recordID": mutation.recordID,
                "operation": mutation.operation.rawValue,
                "baseVersion": mutation.baseVersion ?? NSNull(),
                "clientVersion": mutation.clientVersion,
                "status": mutation.status.rawValue,
                "sourceKind": mutation.sourceKind
            ]
        }
    }

    private static func pendingMutationPayload(_ mutation: PendingMutationInspection) -> [String: Any] {
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

    private static func retainedRejectionPayload(_ value: RetainedRejectionInspection) -> [String: Any] {
        switch value {
        case .current(let rejection):
            return rejectedMutationPayload(rejection).merging(["representation": "current"]) { current, _ in current }
        case .legacy(let rejection):
            return [
                "representation": "legacy",
                "mutationID": rejection.mutationID,
                "tableName": rejection.tableName,
                "recordID": rejection.recordID,
                "status": rejection.status.rawValue,
                "code": rejection.code.rawValue,
                "message": rejection.message ?? NSNull(),
                "serverRowJSON": rejection.serverRowJSON ?? NSNull(),
                "serverVersion": rejection.serverVersion ?? NSNull(),
                "createdAt": rejection.createdAt,
                "updatedAt": rejection.updatedAt
            ]
        }
    }

    private static func rejectedMutationPayload(_ mutation: RejectedMutationInspection) -> [String: Any] {
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

    private static func canonicalBase64URL(_ data: Data) -> String {
        data.base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }

    private static func decodeCanonicalBase64URL(_ value: String, index: Int) throws -> Data {
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

    private static func decodeCanonicalInt64(_ value: String, index: Int) throws -> Int64 {
        guard value == "0" || value.range(of: "^-?[1-9][0-9]*$", options: .regularExpression) != nil,
              let parsed = Int64(value), String(parsed) == value else {
            throw BridgeParamError.invalidTag(index: index, reason: "int64 is not canonical or is out of range")
        }
        return parsed
    }

    private static func bridgeTagToParam(
        _ tag: [String: BridgeValue],
        index: Int
    ) throws -> (any DatabaseValueConvertible)? {
        guard tag.count == 2,
              case .string(let type)? = tag["type"] else {
            throw BridgeParamError.invalidTag(index: index, reason: "tag must contain exactly type and payload fields")
        }

        switch type {
        case "bytes":
            guard case .string(let base64)? = tag["base64"], tag["value"] == nil else {
                throw BridgeParamError.invalidTag(index: index, reason: "bytes tag must contain base64")
            }
            return try decodeCanonicalBase64URL(base64, index: index)
        case "int64":
            guard case .string(let value)? = tag["value"], tag["base64"] == nil else {
                throw BridgeParamError.invalidTag(index: index, reason: "int64 tag must contain value")
            }
            return try decodeCanonicalInt64(value, index: index)
        default:
            throw BridgeParamError.invalidTag(index: index, reason: "unknown tag type")
        }
    }

    private static func bridgeValueToParam(
        _ value: BridgeValue,
        index: Int
    ) throws -> (any DatabaseValueConvertible)? {
        switch value {
        case .null:
            return nil
        case .bool(let boolVal):
            return boolVal ? 1 : 0
        case .integer(let intVal):
            guard intVal >= Self.minSafeInteger && intVal <= Self.maxSafeInteger else {
                throw BridgeParamError.invalidNumber(index: index)
            }
            return intVal
        case .double(let doubleVal):
            guard doubleVal.isFinite else {
                throw BridgeParamError.invalidNumber(index: index)
            }
            if doubleVal.rounded(.towardZero) == doubleVal,
               (doubleVal < Double(Self.minSafeInteger) || doubleVal > Double(Self.maxSafeInteger)) {
                throw BridgeParamError.invalidNumber(index: index)
            }
            return doubleVal
        case .string(let stringVal):
            return stringVal
        case .object(let dictionary):
            return try bridgeTagToParam(dictionary, index: index)
        case .array:
            throw BridgeParamError.unsupported(index: index, type: "array")
        }
    }

    private static func bridgeParams(
        _ values: [BridgeValue]
    ) throws -> [(any DatabaseValueConvertible)?] {
        try values.enumerated().map { index, value in
            try bridgeValueToParam(value, index: index)
        }
    }

    private static func parseColumns(_ json: String) throws -> [ColumnDef] {
        struct Column: Decodable {
            let name: String
            let type: String
            let nullable: Bool?
            let defaultValue: String?
            let primaryKey: Bool?
        }
        let columns: [Column]
        do {
            columns = try JSONDecoder().decode([Column].self, from: Data(json.utf8))
        } catch is DecodingError {
            throw NSError(
                domain: "SynchroModule",
                code: 0,
                userInfo: [NSLocalizedDescriptionKey: "Invalid schema columns"]
            )
        }
        return columns.map { item in
            ColumnDef(
                name: item.name,
                type: item.type,
                nullable: item.nullable ?? true,
                defaultValue: item.defaultValue,
                primaryKey: item.primaryKey ?? false
            )
        }
    }

    private static func parseTableOptions(_ json: String) throws -> Synchro.TableOptions {
        struct Options: Decodable {
            let ifNotExists: Bool?
            let withoutRowid: Bool?
        }
        let options: Options
        do {
            options = try JSONDecoder().decode(Options.self, from: Data(json.utf8))
        } catch is DecodingError {
            throw NSError(
                domain: "SynchroModule",
                code: 0,
                userInfo: [NSLocalizedDescriptionKey: "Invalid table options"]
            )
        }
        return Synchro.TableOptions(
            ifNotExists: options.ifNotExists ?? true,
            withoutRowid: options.withoutRowid ?? false
        )
    }

    private static func databaseValueToBridgeValue(_ dbValue: DatabaseValue) -> BridgeValue {
        switch dbValue.storage {
        case .null:
            return .null
        case .int64(let v):
            if v >= Self.minSafeInteger && v <= Self.maxSafeInteger {
                return .integer(v)
            }
            return .object(["type": .string("int64"), "value": .string(String(v))])
        case .double(let v):
            return .double(v)
        case .string(let v):
            return .string(v)
        case .blob(let v):
            return .object(["type": .string("bytes"), "base64": .string(canonicalBase64URL(v))])
        }
    }

    private static func rowToBridgeValue(_ row: Row) -> BridgeValue {
        var values: [String: BridgeValue] = [:]
        for column in row.columnNames {
            let dbValue: DatabaseValue = row[column]
            values[column] = databaseValueToBridgeValue(dbValue)
        }
        return .object(values)
    }

    private static func rowsToBridgeValue(_ rows: [Row]) -> BridgeValue {
        .array(rows.map(rowToBridgeValue))
    }
}
