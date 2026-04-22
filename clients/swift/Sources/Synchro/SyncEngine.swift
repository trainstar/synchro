import Foundation
import os
@preconcurrency import GRDB

public struct SyncOptions: Sendable {
    public var initialSyncCompleted: (@Sendable () -> Void)?

    public init(initialSyncCompleted: (@Sendable () -> Void)? = nil) {
        self.initialSyncCompleted = initialSyncCompleted
    }
}

private struct SyncEngineState {
    var currentStatus: SyncStatus = .stopped
    var statusCallbacks: [UUID: (SyncStatus) -> Void] = [:]
    var conflictCallbacks: [UUID: (ConflictEvent) -> Void] = [:]
    var started = false
    var cycleRunning = false
    var cycleQueued = false
}

private actor StartupGate {
    private var continuation: CheckedContinuation<Void, Error>?
    private var result: Result<Void, Error>?

    func wait() async throws {
        if let result {
            try result.get()
            return
        }

        try await withCheckedThrowingContinuation { continuation in
            if let result {
                continuation.resume(with: result)
            } else {
                self.continuation = continuation
            }
        }
    }

    func succeed() {
        guard result == nil else { return }
        result = .success(())
        continuation?.resume()
        continuation = nil
    }

    func fail(_ error: Error) {
        guard result == nil else { return }
        result = .failure(error)
        continuation?.resume(throwing: error)
        continuation = nil
    }
}

final class SyncEngine: @unchecked Sendable {
    private let config: SynchroConfig
    private let database: SynchroDatabase
    private let httpClient: HttpClient
    private let schemaManager: SchemaManager
    private let changeTracker: ChangeTracker
    private let pullProcessor: PullProcessor
    private let pushProcessor: PushProcessor

    private var syncTask: Task<Void, Never>?
    private var debounceTask: Task<Void, Never>?
    private var pendingObserver: DatabaseCancellable?

    private let state = OSAllocatedUnfairLock(initialState: SyncEngineState())

    private var syncedTables: [LocalSchemaTable] = []
    private var schemaVersion: Int64 = 0
    private var schemaHash: String = ""
    private var clientID: String { config.clientID }
    private var retryCount: Int = 0

    init(
        config: SynchroConfig,
        database: SynchroDatabase,
        httpClient: HttpClient,
        schemaManager: SchemaManager,
        changeTracker: ChangeTracker,
        pullProcessor: PullProcessor,
        pushProcessor: PushProcessor
    ) {
        self.config = config
        self.database = database
        self.httpClient = httpClient
        self.schemaManager = schemaManager
        self.changeTracker = changeTracker
        self.pullProcessor = pullProcessor
        self.pushProcessor = pushProcessor
    }

    // MARK: - Lifecycle

    func start(options: SyncOptions? = nil) async throws {
        let canStart = state.withLock { state in
            if state.started {
                return false
            }
            state.started = true
            return true
        }
        if !canStart {
            throw SynchroError.alreadyStarted
        }

        // Clear sync lock in case of prior crash
        do {
            try database.writeTransaction { db in
                try SynchroMeta.setSyncLock(db, locked: false)
            }
        } catch {
            state.withLock { $0.started = false }
            throw error
        }

        let startupGate = StartupGate()
        syncTask = Task { [weak self] in
            guard let self else {
                await startupGate.succeed()
                return
            }
            await self.runManagedLoop(startupGate: startupGate, options: options)
        }

        do {
            try await startupGate.wait()
        } catch {
            teardownAfterFailedStart()
            throw error
        }
    }

    func stop() {
        syncTask?.cancel()
        syncTask = nil
        debounceTask?.cancel()
        debounceTask = nil
        pendingObserver?.cancel()
        pendingObserver = nil
        state.withLock {
            $0.started = false
            $0.cycleRunning = false
            $0.cycleQueued = false
        }
        updateStatus(.stopped)
    }

    func syncNow() async throws {
        try await runSyncCycleWithRetry()
    }

    // MARK: - Callbacks

    func onStatusChange(_ callback: @escaping (SyncStatus) -> Void) -> any Cancellable {
        let id = UUID()
        state.withLock { $0.statusCallbacks[id] = callback }
        return CallbackCancellable { [weak self] in
            _ = self?.state.withLock { $0.statusCallbacks.removeValue(forKey: id) }
        }
    }

    func onConflict(_ callback: @escaping (ConflictEvent) -> Void) -> any Cancellable {
        let id = UUID()
        state.withLock { $0.conflictCallbacks[id] = callback }
        return CallbackCancellable { [weak self] in
            _ = self?.state.withLock { $0.conflictCallbacks.removeValue(forKey: id) }
        }
    }

    // MARK: - Sync Loop

    private func syncLoop() async {
        while !Task.isCancelled {
            do {
                try await Task.sleep(nanoseconds: UInt64(config.syncInterval * 1_000_000_000))
            } catch {
                return
            }
            guard !Task.isCancelled else { return }
            do {
                try await runSyncCycleWithRetry()
            } catch {
                // Error already handled in runSyncCycleWithRetry
            }
        }
    }

    private func runManagedLoop(startupGate: StartupGate, options: SyncOptions?) async {
        let startupCompleted = await runStartupSequence(startupGate: startupGate, options: options)
        guard startupCompleted else { return }
        await syncLoop()
    }

    private func runStartupSequence(startupGate: StartupGate, options: SyncOptions?) async -> Bool {
        var startupAttempt = 0
        var gateResolved = false

        while !Task.isCancelled {
            do {
                let connectResponse = try await connect()
                let connectSchema = try await resolveConnectSchema(connectResponse)
                self.syncedTables = connectSchema.tables
                self.schemaVersion = connectSchema.version
                self.schemaHash = connectSchema.hash

                try applyScopeAssignmentDelta(
                    connectResponse.scopes,
                    scopeSetVersion: connectResponse.scopeSetVersion
                )

                updateStatus(.idle)

                try await rebuildAssignedScopesNeedingCursor()
                try await runSyncCycleWithRetry()
                startPendingObserver()

                if !gateResolved {
                    await startupGate.succeed()
                }
                options?.initialSyncCompleted?()
                return true
            } catch let error as RetryableError {
                startupAttempt += 1
                let delay = retryDelay(attempt: startupAttempt, serverRetryAfter: error.retryAfter)
                updateStatus(.error(retryAt: Date().addingTimeInterval(delay)))
                if !gateResolved {
                    await startupGate.succeed()
                    gateResolved = true
                }
                do {
                    try await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
                } catch {
                    return false
                }
            } catch {
                handleSyncError(error)
                if gateResolved {
                    finishStartupFailure()
                } else {
                    await startupGate.fail(error)
                }
                return false
            }
        }

        if !gateResolved {
            await startupGate.succeed()
        }
        return false
    }

    // MARK: - Retry

    private func runSyncCycleWithRetry() async throws {
        let shouldRun = state.withLock { state in
            if state.cycleRunning {
                state.cycleQueued = true
                return false
            }
            state.cycleRunning = true
            return true
        }
        if !shouldRun {
            return
        }
        defer {
            state.withLock { $0.cycleRunning = false }
        }

        repeat {
            state.withLock { $0.cycleQueued = false }

            try await runSingleSyncCycleWithRetry()

            let shouldRepeat = state.withLock { $0.cycleQueued }
            if !shouldRepeat {
                break
            }
        } while true
    }

    private func runSingleSyncCycleWithRetry() async throws {
        var attempt = 0
        var lastError: Error?

        while attempt <= config.maxRetryAttempts {
            do {
                try await runSyncCycle()
                retryCount = 0
                return
            } catch let error as RetryableError {
                attempt += 1
                lastError = error
                let delay = retryDelay(attempt: attempt, serverRetryAfter: error.retryAfter)
                updateStatus(.error(retryAt: Date().addingTimeInterval(delay)))
                do {
                    try await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
                } catch {
                    throw lastError!
                }
                guard !Task.isCancelled else { throw lastError! }
            } catch {
                // Non-retryable errors propagate immediately
                handleSyncError(error)
                throw error
            }
        }

        if let lastError {
            handleSyncError(lastError)
            throw lastError
        }
    }

    private func retryDelay(attempt: Int, serverRetryAfter: TimeInterval?) -> TimeInterval {
        if let serverDelay = serverRetryAfter, serverDelay > 0 {
            return serverDelay
        }
        // Exponential backoff: 1s, 2s, 4s, 8s, 16s cap
        let base = min(pow(2.0, Double(attempt - 1)), 16.0)
        // Add jitter: 0-50% of base
        let jitter = Double.random(in: 0...(base * 0.5))
        return base + jitter
    }

    private func runSyncCycle() async throws {
        updateStatus(.syncing)

        // 1. Push first
        try await runPush()

        // 2. Pull loop
        try await runPullLoop()

        updateStatus(.idle)
    }

    // MARK: - Push

    private func runPush() async throws {
        var hasMore = true
        while hasMore {
            let outcome = try await pushProcessor.processPush(
                httpClient: httpClient,
                clientID: clientID,
                schemaVersion: schemaVersion,
                schemaHash: schemaHash,
                syncedTables: syncedTables,
                batchSize: config.pushBatchSize
            )

            if let outcome {
                for conflict in outcome.conflicts {
                    fireConflict(conflict)
                }
                if outcome.hasRetryableRejections {
                    throw RetryableError(
                        underlying: .pushRejected(results: outcome.response.rejected.filter { $0.status == .rejectedRetryable }),
                        retryAfter: nil
                    )
                }
                hasMore = try changeTracker.hasPendingChanges()
            } else {
                hasMore = false
            }
        }
    }

    // MARK: - Pull

    private func runPullLoop() async throws {
        var hasMore = true
        var scopesToRebuild = Set<String>()
        var scopeSetVersion = try database.readTransaction { db in
            try SynchroMeta.getInt64(db, key: .scopeSetVersion)
        }

        while hasMore {
            let scopes = try loadKnownScopes()
            if scopes.isEmpty {
                return
            }

            let request = PullRequest(
                clientID: clientID,
                schema: SchemaRef(version: schemaVersion, hash: schemaHash),
                scopeSetVersion: scopeSetVersion,
                scopes: scopes,
                limit: config.pullPageSize,
                checksumMode: .requested
            )

            let response = try await httpClient.pull(request: request)
            try response.validate(for: request)

            try pullProcessor.applyScopeChanges(
                changes: response.changes,
                syncedTables: syncedTables,
                scopeCursors: response.scopeCursors,
                checksums: response.checksums
            )
            try applyScopeAssignmentDelta(
                response.scopeUpdates,
                scopeSetVersion: response.scopeSetVersion
            )
            scopesToRebuild.formUnion(response.rebuild)
            scopeSetVersion = response.scopeSetVersion

            hasMore = response.hasMore
        }

        let knownScopeIDs = Set(try database.readTransaction { db in
            try SynchroMeta.getAllScopes(db).map(\.scopeID)
        })
        let missingCursorScopes = try scopeIDsNeedingRebuild()

        for scopeID in scopesToRebuild.union(missingCursorScopes) where knownScopeIDs.contains(scopeID) {
            try await rebuildScope(scopeID: scopeID)
        }
    }

    // MARK: - Scope Rebuild

    private func rebuildScope(scopeID: String) async throws {
        let generation = try pullProcessor.beginScopeRebuild(scopeID: scopeID)
        var cursor: String? = nil

        while true {
            let request = RebuildRequest(
                clientID: clientID,
                scope: scopeID,
                cursor: cursor,
                limit: config.pullPageSize
            )

            let response = try await httpClient.rebuild(request: request)
            try response.validate()

            try pullProcessor.applyScopeRebuildPage(
                scopeID: scopeID,
                generation: generation,
                records: response.records,
                syncedTables: syncedTables
            )

            if response.hasMore {
                cursor = response.cursor
                continue
            }

            guard let finalCursor = response.finalScopeCursor,
                  let checksum = response.checksum else {
                throw SynchroError.invalidResponse(
                    message: "final rebuild page missing final scope cursor or checksum for \(scopeID)"
                )
            }

            try pullProcessor.finalizeScopeRebuild(
                scopeID: scopeID,
                generation: generation,
                finalCursor: finalCursor,
                checksum: checksum,
                syncedTables: syncedTables
            )
            return
        }
    }

    // MARK: - Bootstrap

    private func connect() async throws -> ConnectResponse {
        let schemaState = try database.readTransaction { db in
            (
                version: try SynchroMeta.getInt64(db, key: .schemaVersion),
                hash: try SynchroMeta.get(db, key: .schemaHash) ?? ""
            )
        }

        let request = ConnectRequest(
            clientID: clientID,
            platform: config.platform,
            appVersion: config.appVersion,
            protocolVersion: 1,
            schema: SchemaRef(version: schemaState.version, hash: schemaState.hash),
            scopeSetVersion: try database.readTransaction { db in
                try SynchroMeta.getInt64(db, key: .scopeSetVersion)
            },
            knownScopes: try loadKnownScopes()
        )

        let response = try await httpClient.connect(request: request)
        try response.validate()

        guard response.schema.action.isCompatible else {
            throw SynchroError.invalidResponse(message: "unsupported connect schema action")
        }

        return response
    }

    private func resolveConnectSchema(_ response: ConnectResponse) async throws -> (tables: [LocalSchemaTable], version: Int64, hash: String) {
        switch response.schema.action {
        case .none:
            guard let tables = try schemaManager.loadStoredLocalSchema() else {
                throw SynchroError.invalidResponse(message: "connect returned schema action none without stored local schema")
            }
            try database.writeTransaction { db in
                try SynchroMeta.setInt64(db, key: .schemaVersion, value: response.schema.version)
                try SynchroMeta.set(db, key: .schemaHash, value: response.schema.hash)
            }
            return (tables, response.schema.version, response.schema.hash)

        case .fetch:
            let schema = try await schemaManager.ensureSchema(httpClient: httpClient)
            return (try schema.localTables(), schema.schemaVersion, schema.schemaHash)

        case .replace, .rebuildLocal:
            guard let manifest = response.schemaDefinition else {
                throw SynchroError.invalidResponse(message: "connect schema action \(response.schema.action.rawValue) missing schema_definition")
            }
            let tables = try manifest.localTables()
            try schemaManager.reconcileLocalSchema(
                schemaVersion: response.schema.version,
                schemaHash: response.schema.hash,
                tables: tables
            )
            return (tables, response.schema.version, response.schema.hash)

        case .unsupported:
            throw SynchroError.invalidResponse(message: "unsupported connect schema action")
        }
    }

    private func loadKnownScopes() throws -> [String: ScopeCursorRef] {
        try database.readTransaction { db in
            Dictionary(
                uniqueKeysWithValues: try SynchroMeta.getAllScopes(db).map { scope in
                    (scope.scopeID, ScopeCursorRef(cursor: scope.cursor))
                }
            )
        }
    }

    private func applyScopeAssignmentDelta(_ delta: ScopeAssignmentDelta, scopeSetVersion: Int64) throws {
        for scopeID in delta.remove {
            try pullProcessor.removeScope(scopeID: scopeID, syncedTables: syncedTables)
        }

        try database.writeTransaction { db in
            for scope in delta.add {
                try SynchroMeta.upsertScope(
                    db,
                    scopeID: scope.id,
                    cursor: scope.cursor,
                    checksum: nil
                )
            }
            try SynchroMeta.setInt64(db, key: .scopeSetVersion, value: scopeSetVersion)
        }
    }

    private func scopeIDsNeedingRebuild() throws -> Set<String> {
        try database.readTransaction { db in
            Set(
                try SynchroMeta.getAllScopes(db)
                    .filter { $0.cursor == nil }
                    .map(\.scopeID)
            )
        }
    }

    private func rebuildAssignedScopesNeedingCursor() async throws {
        for scopeID in try scopeIDsNeedingRebuild() {
            try await rebuildScope(scopeID: scopeID)
        }
    }

    // MARK: - Debounce

    private func startPendingObserver() {
        guard pendingObserver == nil else { return }
        pendingObserver = database.onChange(tables: ["_synchro_pending_changes"]) { [weak self] in
            self?.scheduleDebouncedPush()
        }
    }

    private func scheduleDebouncedPush() {
        debounceTask?.cancel()
        debounceTask = Task { [weak self] in
            guard let self else { return }
            do {
                try await Task.sleep(nanoseconds: UInt64(self.config.pushDebounce * 1_000_000_000))
                guard !Task.isCancelled else { return }
                try await self.runPush()
            } catch {
                // Debounced push failed, will retry on next cycle
            }
        }
    }

    // MARK: - Error Handling

    private func handleSyncError(_ error: Error) {
        if let retryable = error as? RetryableError {
            let retryAt = retryable.retryAfter.map { Date().addingTimeInterval($0) }
            updateStatus(.error(retryAt: retryAt))
        } else {
            updateStatus(.error(retryAt: nil))
        }
    }

    private func teardownAfterFailedStart() {
        syncTask?.cancel()
        syncTask = nil
        debounceTask?.cancel()
        debounceTask = nil
        pendingObserver?.cancel()
        pendingObserver = nil
        state.withLock {
            $0.started = false
            $0.cycleRunning = false
            $0.cycleQueued = false
        }
    }

    private func finishStartupFailure() {
        syncTask = nil
        debounceTask?.cancel()
        debounceTask = nil
        pendingObserver?.cancel()
        pendingObserver = nil
        state.withLock {
            $0.started = false
            $0.cycleRunning = false
            $0.cycleQueued = false
        }
    }

    // MARK: - Status

    private func updateStatus(_ status: SyncStatus) {
        let callbacks = state.withLock { state in
            state.currentStatus = status
            return state.statusCallbacks
        }
        for (_, cb) in callbacks {
            cb(status)
        }
    }

    private func fireConflict(_ event: ConflictEvent) {
        let callbacks = state.withLock { $0.conflictCallbacks }
        for (_, cb) in callbacks {
            cb(event)
        }
    }

}

// MARK: - DatabaseCancellableWrapper

final class DatabaseCancellableWrapper: Cancellable, @unchecked Sendable {
    private var inner: (any DatabaseCancellable)?

    init(_ inner: any DatabaseCancellable) {
        self.inner = inner
    }

    func cancel() {
        inner?.cancel()
        inner = nil
    }
}

// MARK: - CallbackCancellable

final class CallbackCancellable: Cancellable, @unchecked Sendable {
    private var onCancel: (() -> Void)?

    init(onCancel: @escaping () -> Void) {
        self.onCancel = onCancel
    }

    func cancel() {
        onCancel?()
        onCancel = nil
    }
}
