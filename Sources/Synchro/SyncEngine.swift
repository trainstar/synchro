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
    private var snapshotCallbacks: [UUID: () async -> Bool] = [:]
    private let lock = NSLock()
    private var started = false
    private var cycleRunning = false
    private var cycleQueued = false

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
        lock.lock()
        if started {
            lock.unlock()
            throw SynchroError.alreadyStarted
        }
        started = true
        lock.unlock()

        // Clear sync lock in case of prior crash
        do {
            try database.writeTransaction { db in
                try SynchroMeta.setSyncLock(db, locked: false)
            }

            let schema = try await schemaManager.ensureSchema(httpClient: httpClient)
            self.syncedTables = schema.tables
            self.schemaVersion = schema.schemaVersion
            self.schemaHash = schema.schemaHash

            let registerReq = RegisterRequest(
                clientID: config.clientID,
                clientName: nil,
                platform: config.platform,
                appVersion: config.appVersion,
                schemaVersion: schema.schemaVersion,
                schemaHash: schema.schemaHash
            )
            let registerResp = try await httpClient.register(request: registerReq)

            let needsInitialSnapshot = try database.writeTransaction { db in
                try SynchroMeta.set(db, key: .clientServerID, value: registerResp.id)
                let checkpoint = try SynchroMeta.getInt64(db, key: .checkpoint)
                let snapshotComplete = try SynchroMeta.get(db, key: .snapshotComplete) == "1"
                return checkpoint == 0 || !snapshotComplete
            }

            updateStatus(.idle)

            if needsInitialSnapshot {
                try await rebuildFromSnapshot(reason: "initial_sync_required", requiresApproval: false)
            }

            syncTask = Task { [weak self] in
                guard let self else { return }
                await self.syncLoop()
            }
            startPendingObserver()
            try await runSyncCycleWithRetry()
            options?.initialSyncCompleted?()
        } catch {
            lock.lock()
            started = false
            lock.unlock()
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
        lock.lock()
        started = false
        cycleRunning = false
        cycleQueued = false
        lock.unlock()
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

    func onSnapshotRequired(_ callback: @escaping () async -> Bool) -> any Cancellable {
        let id = UUID()
        lock.lock()
        snapshotCallbacks[id] = callback
        lock.unlock()
        return CallbackCancellable { [weak self] in
            self?.lock.lock()
            self?.snapshotCallbacks.removeValue(forKey: id)
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
        lock.lock()
        if cycleRunning {
            cycleQueued = true
            lock.unlock()
            return
        }
        cycleRunning = true
        lock.unlock()
        defer {
            lock.lock()
            cycleRunning = false
            lock.unlock()
        }

        repeat {
            lock.lock()
            cycleQueued = false
            lock.unlock()

            try await runSingleSyncCycleWithRetry()

            lock.lock()
            let shouldRepeat = cycleQueued
            lock.unlock()
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
                        underlying: .pushRejected(results: responseRetryableResults(outcome.response)),
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
        var checkpoint = try database.readTransaction { db in
            try SynchroMeta.getInt64(db, key: .checkpoint)
        }
        let knownBucketsStr = try database.readTransaction { db in
            try SynchroMeta.get(db, key: .knownBuckets) ?? "[]"
        }
        var knownBuckets = (try? JSONDecoder().decode([String].self, from: Data(knownBucketsStr.utf8))) ?? []

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

            if response.snapshotRequired == true {
                try await rebuildFromSnapshot(reason: response.snapshotReason ?? "snapshot_required", requiresApproval: response.snapshotReason != "initial_sync_required")
                return
            }

            try pullProcessor.applyPullPage(changes: response.changes, deletes: response.deletes, syncedTables: syncedTables)
            try pullProcessor.updateCheckpoint(response.checkpoint)
            try pullProcessor.updateKnownBuckets(bucketUpdates: response.bucketUpdates)

            checkpoint = response.checkpoint
            hasMore = response.hasMore
            if hasMore, response.bucketUpdates != nil {
                let reloadedBuckets = try database.readTransaction { db in
                    try SynchroMeta.get(db, key: .knownBuckets) ?? "[]"
                }
                knownBuckets = (try? JSONDecoder().decode([String].self, from: Data(reloadedBuckets.utf8))) ?? []
            }
        }
    }

    // MARK: - Snapshot

    private func rebuildFromSnapshot(reason: String, requiresApproval: Bool) async throws {
        if requiresApproval {
            try await runPush()
            var approved = true
            lock.lock()
            let callbacks = snapshotCallbacks
            lock.unlock()
            for (_, callback) in callbacks {
                let result = await callback()
                if !result {
                    approved = false
                    break
                }
            }
            guard approved else {
                throw SynchroError.snapshotRequired
            }
        }

        let schema = SchemaResponse(
            schemaVersion: schemaVersion,
            schemaHash: schemaHash,
            serverTime: Date(),
            tables: syncedTables
        )
        try database.writeTransaction { db in
            try schemaManager.dropSyncedTablesInTransaction(db, schema: schema)
            try db.execute(sql: "DELETE FROM _synchro_pending_changes")
            try SynchroMeta.setInt64(db, key: .checkpoint, value: 0)
            try SynchroMeta.set(db, key: .knownBuckets, value: "[]")
            try SynchroMeta.set(db, key: .snapshotComplete, value: "0")
            try schemaManager.createSyncedTablesInTransaction(db, schema: schema)
        }

        var cursor: SnapshotCursor? = nil
        var hasMore = true

        while hasMore {
            let request = SnapshotRequest(
                clientID: clientID,
                cursor: cursor,
                limit: config.snapshotPageSize,
                schemaVersion: schemaVersion,
                schemaHash: schemaHash
            )

            let response = try await httpClient.snapshot(request: request)

            try pullProcessor.applySnapshotPage(records: response.records, syncedTables: syncedTables)

            cursor = response.cursor
            hasMore = response.hasMore

            if !hasMore {
                try database.writeTransaction { db in
                    try SynchroMeta.setInt64(db, key: .checkpoint, value: response.checkpoint)
                    try SynchroMeta.set(db, key: .snapshotComplete, value: "1")
                }
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

    private func responseRetryableResults(_ response: PushResponse) -> [PushResult] {
        response.rejected.filter { $0.status == PushStatus.rejectedRetryable }
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
