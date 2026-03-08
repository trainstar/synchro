import Foundation
import GRDB

public struct SyncOptions: Sendable {
    public var initialSyncCompleted: (() -> Void)?

    public init(initialSyncCompleted: (() -> Void)? = nil) {
        self.initialSyncCompleted = initialSyncCompleted
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

    private var currentStatus: SyncStatus = .stopped
    private var statusCallbacks: [UUID: (SyncStatus) -> Void] = [:]
    private var conflictCallbacks: [UUID: (ConflictEvent) -> Void] = [:]
    private var resyncCallbacks: [UUID: () async -> Bool] = [:]
    private let lock = NSLock()

    private var syncedTables: [SchemaTable] = []
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
        // Fetch schema first so we can register with the correct version
        let schema = try await schemaManager.ensureSchema(httpClient: httpClient)
        self.syncedTables = schema.tables
        self.schemaVersion = schema.schemaVersion
        self.schemaHash = schema.schemaHash

        // Single register with actual schema version
        let registerReq = RegisterRequest(
            clientID: config.clientID,
            clientName: nil,
            platform: config.platform,
            appVersion: config.appVersion,
            schemaVersion: schema.schemaVersion,
            schemaHash: schema.schemaHash
        )
        let registerResp = try await httpClient.register(request: registerReq)

        try database.writeTransaction { db in
            try SynchroMeta.set(db, key: .clientServerID, value: registerResp.id)
        }

        updateStatus(.idle)

        // Start sync loop
        syncTask = Task { [weak self] in
            guard let self else { return }
            await self.syncLoop()
        }

        // Start watching for pending changes (debounced push)
        startPendingObserver()

        // Run initial sync
        try await runSyncCycleWithRetry()
        options?.initialSyncCompleted?()
    }

    func stop() {
        syncTask?.cancel()
        syncTask = nil
        debounceTask?.cancel()
        debounceTask = nil
        pendingObserver?.cancel()
        pendingObserver = nil
        updateStatus(.stopped)
    }

    func syncNow() async throws {
        try await runSyncCycleWithRetry()
    }

    // MARK: - Callbacks

    func onStatusChange(_ callback: @escaping (SyncStatus) -> Void) -> any Cancellable {
        let id = UUID()
        lock.lock()
        statusCallbacks[id] = callback
        lock.unlock()
        return CallbackCancellable { [weak self] in
            self?.lock.lock()
            self?.statusCallbacks.removeValue(forKey: id)
            self?.lock.unlock()
        }
    }

    func onConflict(_ callback: @escaping (ConflictEvent) -> Void) -> any Cancellable {
        let id = UUID()
        lock.lock()
        conflictCallbacks[id] = callback
        lock.unlock()
        return CallbackCancellable { [weak self] in
            self?.lock.lock()
            self?.conflictCallbacks.removeValue(forKey: id)
            self?.lock.unlock()
        }
    }

    func onResyncRequired(_ callback: @escaping () async -> Bool) -> any Cancellable {
        let id = UUID()
        lock.lock()
        resyncCallbacks[id] = callback
        lock.unlock()
        return CallbackCancellable { [weak self] in
            self?.lock.lock()
            self?.resyncCallbacks.removeValue(forKey: id)
            self?.lock.unlock()
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

    // MARK: - Retry

    private func runSyncCycleWithRetry() async throws {
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
                hasMore = try changeTracker.hasPendingChanges()
            } else {
                hasMore = false
            }
        }
    }

    // MARK: - Pull

    private func runPullLoop() async throws {
        var checkpoint = try database.readTransaction { db in
            try SynchroMeta.getInt64(db, key: .checkpoint)
        }
        let knownBucketsStr = try database.readTransaction { db in
            try SynchroMeta.get(db, key: .knownBuckets) ?? "[]"
        }
        let knownBuckets = (try? JSONDecoder().decode([String].self, from: Data(knownBucketsStr.utf8))) ?? []

        var hasMore = true
        while hasMore {
            let request = PullRequest(
                clientID: clientID,
                checkpoint: checkpoint,
                tables: nil,
                limit: config.pullPageSize,
                knownBuckets: knownBuckets.isEmpty ? nil : knownBuckets,
                schemaVersion: schemaVersion,
                schemaHash: schemaHash
            )

            let response = try await httpClient.pull(request: request)

            if response.resyncRequired == true {
                try await handleResync()
                return
            }

            try pullProcessor.applyChanges(changes: response.changes, syncedTables: syncedTables)
            try pullProcessor.applyDeletes(deletes: response.deletes, syncedTables: syncedTables)
            try pullProcessor.updateCheckpoint(response.checkpoint)
            try pullProcessor.updateKnownBuckets(bucketUpdates: response.bucketUpdates)

            checkpoint = response.checkpoint
            hasMore = response.hasMore
        }
    }

    // MARK: - Resync

    private func handleResync() async throws {
        // Push any remaining changes first
        try await runPush()

        // Ask callbacks if resync is approved
        var approved = true
        lock.lock()
        let callbacks = resyncCallbacks
        lock.unlock()

        for (_, callback) in callbacks {
            let result = await callback()
            if !result {
                approved = false
                break
            }
        }

        guard approved else {
            throw SynchroError.resyncRequired
        }

        // Drop synced data and pending changes
        let schema = SchemaResponse(
            schemaVersion: schemaVersion,
            schemaHash: schemaHash,
            serverTime: Date(),
            tables: syncedTables
        )
        try schemaManager.dropSyncedTables(schema: schema)
        try changeTracker.clearAll()
        try schemaManager.createSyncedTables(schema: schema)

        // Page through resync
        var cursor: ResyncCursor? = nil
        var hasMore = true

        while hasMore {
            let request = ResyncRequest(
                clientID: clientID,
                cursor: cursor,
                limit: config.resyncPageSize,
                schemaVersion: schemaVersion,
                schemaHash: schemaHash
            )

            let response = try await httpClient.resync(request: request)

            try pullProcessor.applyResyncPage(records: response.records, syncedTables: syncedTables)

            cursor = response.cursor
            hasMore = response.hasMore

            if !hasMore {
                try pullProcessor.updateCheckpoint(response.checkpoint)
            }
        }
    }

    // MARK: - Debounce

    private func startPendingObserver() {
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

    // MARK: - Status

    private func updateStatus(_ status: SyncStatus) {
        lock.lock()
        currentStatus = status
        let callbacks = statusCallbacks
        lock.unlock()
        for (_, cb) in callbacks {
            cb(status)
        }
    }

    private func fireConflict(_ event: ConflictEvent) {
        lock.lock()
        let callbacks = conflictCallbacks
        lock.unlock()
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
