import Foundation
import os
@preconcurrency import GRDB
#if canImport(UIKit)
import UIKit
#elseif canImport(AppKit)
import AppKit
#endif

public struct SyncOptions: Sendable {
    public var initialSyncCompleted: (@Sendable () -> Void)?

    public init(initialSyncCompleted: (@Sendable () -> Void)? = nil) {
        self.initialSyncCompleted = initialSyncCompleted
    }
}

private struct SyncEngineState {
    var currentStatus: SyncStatus = .uninitialized
    var statusCallbacks: [UUID: (SyncStatus) -> Void] = [:]
    var eventCallbacks: [UUID: (SyncEvent) -> Void] = [:]
    var conflictCallbacks: [UUID: (ConflictEvent) -> Void] = [:]
    var started = false
    var connectionReady = false
    var closed = false
    var stopping = false
    var backgrounded = false
    var resumeOnForeground = false
    var explicitlyStopped = false
    var lifecycleGeneration: Int64 = 0
    var activeOperations = 0
    var operationWaiters: [CheckedContinuation<Void, Never>] = []
}

private struct ConnectOperationResult {
    let response: ConnectResponse
    let requestBody: Data
}

private struct LifecycleResources {
    let syncTask: Task<Void, Never>?
    let debounceTask: Task<Void, Never>?
    let pendingObserver: DatabaseCancellable?
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

private final class CycleGate: @unchecked Sendable {
    private struct Entry {
        let task: Task<Void, Error>
    }

    private let lock = NSLock()
    private var tail: Task<Void, Never>?
    private var activeGeneration: Int64?
    private var tasks: [UUID: Entry] = [:]

    func beginGeneration(_ generation: Int64) {
        lock.lock()
        let oldTasks = Array(tasks.values)
        tasks.removeAll()
        tail = nil
        activeGeneration = generation
        lock.unlock()
        oldTasks.forEach { $0.task.cancel() }
    }

    func enqueue(
        generation: Int64,
        _ operation: @escaping @Sendable () async throws -> Void
    ) throws -> Task<Void, Error> {
        lock.lock()
        guard activeGeneration == generation else {
            lock.unlock()
            throw CancellationError()
        }
        let predecessor = tail
        let taskID = UUID()
        let task = Task<Void, Error> {
            defer { self.finish(taskID: taskID, generation: generation) }
            if let predecessor {
                await predecessor.value
            }
            try Task.checkCancellation()
            try await operation()
        }
        tasks[taskID] = Entry(task: task)
        tail = Task {
            _ = await task.result
        }
        lock.unlock()
        return task
    }

    func invalidate(generation: Int64) -> [Task<Void, Error>] {
        lock.lock()
        guard activeGeneration == generation else {
            lock.unlock()
            return []
        }
        activeGeneration = nil
        tail = nil
        let tasks = self.tasks.values.map(\.task)
        self.tasks.removeAll()
        lock.unlock()
        tasks.forEach { $0.cancel() }
        return tasks
    }

    private func finish(taskID: UUID, generation: Int64) {
        lock.lock()
        if activeGeneration == generation {
            tasks.removeValue(forKey: taskID)
        }
        lock.unlock()
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
    private let cycleGate = CycleGate()

    private var syncTask: Task<Void, Never>?
    private var debounceTask: Task<Void, Never>?
    private var debounceTaskID: UUID?
    private var pendingObserver: DatabaseCancellable?
    private var pendingObserverGeneration: Int64?
    private var stopTask: Task<Void, Never>?
    private var nativeLifecycleObservers: [NSObjectProtocol] = []

    private let state = OSAllocatedUnfairLock(initialState: SyncEngineState())

    private var syncedTables: [LocalSchemaTable] = []
    private var schemaVersion: Int64 = 0
    private var schemaHash: String = ""
    private var clientGeneration: Int64 = 0
    private var clientID: String { config.clientID }
    private let decoder = JSONDecoder.synchroDecoder()

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
        let blockingFailure = try? database.readTransaction { db in
            try SynchroMeta.getBlockingFailure(db)
        }
        state.withLock { state in
            state.currentStatus = blockingFailure == nil ? .localReady : .error
        }
        installNativeLifecycleObservers()
    }

    // MARK: - Lifecycle

    func start(options: SyncOptions? = nil) async throws {
        await waitForUnfinishedStop()
        let generation = try reserveStart()
        cycleGate.beginGeneration(generation)
        defer { endOperation() }

        do {
            try prepareStateForStart(lifecycleGeneration: generation)

            let existingFailure: SyncFailure?
            do {
                existingFailure = try loadBlockingFailure()
            } catch {
                let failure = blockingFailure(
                    for: error,
                    operation: .opening,
                    fallbackCode: .invalidResponse,
                    recovery: .retry
                )
                try enterReservedError(failure, generation: generation)
                throw SynchroError.blocked(failure)
            }
            if let existingFailure {
                if getSyncStatus() != .error {
                    try transition(to: .error, lifecycleGeneration: generation)
                }
                throw SynchroError.blocked(existingFailure)
            }
            guard getSyncStatus() == .localReady else {
                throw SynchroError.notStarted
            }

            do {
                _ = try schemaManager.recoverMigrationIfNeeded()
                try ensureLifecycleActive(generation)
            } catch is CancellationError {
                throw CancellationError()
            } catch {
                let failure = blockingFailure(
                    for: error,
                    operation: .schema,
                    fallbackCode: .schemaApplicationFailed,
                    recovery: .retry
                )
                try enterReservedError(failure, generation: generation)
                throw SynchroError.blocked(failure)
            }
            try bindConfiguredClientID()
            try ensureLifecycleActive(generation)
            try await launchReservedStart(
                options: options,
                generation: generation,
                schemaReset: false
            )
        } catch {
            teardownAfterFailedStart(generation)
            throw error
        }
    }

    private func launchReservedStart(
        options: SyncOptions?,
        generation: Int64,
        schemaReset: Bool
    ) async throws {
        // Clear sync lock in case of prior crash
        try database.writeTransaction { db in
            try SynchroMeta.setSyncLock(db, locked: false)
        }
        try ensureLifecycleActive(generation)

        let startupGate = StartupGate()
        let task = Task { [weak self] in
            guard let self else {
                await startupGate.succeed()
                return
            }
            await self.runManagedLoop(
                startupGate: startupGate,
                options: options,
                generation: generation,
                schemaReset: schemaReset
            )
        }
        guard installManagedTask(task, generation: generation) else {
            task.cancel()
            throw SynchroError.notStarted
        }
        try await startupGate.wait()
    }

    private func bindConfiguredClientID() throws {
        try database.writeTransaction { db in
            try SynchroMeta.bindClientIDForNonSeedDatabase(db, clientID: clientID)
        }
    }

    func stop() async {
        await requestLifecycleStop(explicit: true)
    }

    func syncNow() async throws {
        guard let generation = beginCallerOperation() else {
            throw SynchroError.notStarted
        }
        defer { endOperation() }
        try await runSerializedSyncCycleWithRetry(
            lifecycleGeneration: generation
        )
    }

    func shutdown() async {
        let alreadyClosed = state.withLock { state -> Bool in
            if state.closed { return true }
            state.closed = true
            state.explicitlyStopped = true
            state.resumeOnForeground = false
            return false
        }
        guard !alreadyClosed else { return }
        await stop()
        removeNativeLifecycleObservers()
    }

    // MARK: - Callbacks

    func getSyncStatus() -> SyncStatus {
        state.withLock { $0.currentStatus }
    }

    func onStatusChange(_ callback: @escaping (SyncStatus) -> Void) -> any Cancellable {
        let id = UUID()
        state.withLock { $0.statusCallbacks[id] = callback }
        return CallbackCancellable { [weak self] in
            _ = self?.state.withLock { $0.statusCallbacks.removeValue(forKey: id) }
        }
    }

    func onEvent(_ callback: @escaping (SyncEvent) -> Void) -> any Cancellable {
        let id = UUID()
        state.withLock { $0.eventCallbacks[id] = callback }
        return CallbackCancellable { [weak self] in
            _ = self?.state.withLock { $0.eventCallbacks.removeValue(forKey: id) }
        }
    }

    func getBlockingFailure() throws -> SyncFailure? {
        try loadBlockingFailure()
    }

    func onConflict(_ callback: @escaping (ConflictEvent) -> Void) -> any Cancellable {
        let id = UUID()
        state.withLock { $0.conflictCallbacks[id] = callback }
        return CallbackCancellable { [weak self] in
            _ = self?.state.withLock { $0.conflictCallbacks.removeValue(forKey: id) }
        }
    }

    func enterBackground() async {
        let shouldStop = state.withLock { state -> Bool in
            state.backgrounded = true
            let shouldResume = state.started && !state.stopping && !state.closed
            state.resumeOnForeground = shouldResume
            return shouldResume
        }
        guard shouldStop else { return }
        await requestLifecycleStop(explicit: false)
    }

    func enterForeground() async throws {
        let shouldResume = state.withLock { state -> Bool in
            state.backgrounded = false
            guard state.resumeOnForeground,
                  !state.explicitlyStopped,
                  !state.closed else {
                return false
            }
            state.resumeOnForeground = false
            return true
        }
        if shouldResume {
            try await start()
        }
    }

    func retryAfterError() async throws {
        await waitForUnfinishedStop()
        let failure = try requireBlockingFailure(recovery: .retry)
        _ = failure
        try database.writeTransaction { db in
            try SynchroMeta.clearBlockingFailure(db)
        }
        try transition(to: .localReady)
        try await start()
    }

    func resetSchemaAndStart() async throws {
        await waitForUnfinishedStop()
        let generation = try reserveStart()
        cycleGate.beginGeneration(generation)
        defer { endOperation() }

        do {
            _ = try requireBlockingFailure(recovery: .schemaReset)
            try database.writeTransaction { db in
                try db.execute(sql: "DELETE FROM _synchro_backoff")
            }
            try transition(to: .localReady, lifecycleGeneration: generation)
            try bindConfiguredClientID()
            try ensureLifecycleActive(generation)
            try await launchReservedStart(
                options: nil,
                generation: generation,
                schemaReset: true
            )
        } catch {
            teardownAfterFailedStart(generation)
            throw error
        }
    }

    // MARK: - Sync Loop

    private func syncLoop(generation: Int64) async {
        while !Task.isCancelled {
            do {
                try await Task.sleep(nanoseconds: UInt64(config.syncInterval * 1_000_000_000))
            } catch {
                return
            }
            guard !Task.isCancelled, isLifecycleActive(generation) else { return }
            do {
                try await runSerializedSyncCycleWithRetry(
                    lifecycleGeneration: generation
                )
            } catch {
                // Error already handled in runSyncCycleWithRetry
            }
        }
    }

    private func runManagedLoop(
        startupGate: StartupGate,
        options: SyncOptions?,
        generation: Int64,
        schemaReset: Bool
    ) async {
        guard beginManagedOperation(generation: generation) else {
            await startupGate.succeed()
            return
        }
        defer { endOperation() }
        let startupCompleted = await runStartupSequence(
            startupGate: startupGate,
            options: options,
            generation: generation,
            schemaReset: schemaReset
        )
        guard startupCompleted else { return }
        await syncLoop(generation: generation)
    }

    private func runStartupSequence(
        startupGate: StartupGate,
        options: SyncOptions?,
        generation: Int64,
        schemaReset: Bool
    ) async -> Bool {
        var gateResolved = false
        var connected = false
        var replayConnectBackoff: LocalBackoffRecord?
        var replayCycleBackoff: LocalBackoffRecord?

        do {
            if let recoveredBackoff = try loadPersistedBackoff() {
                if isFutureDeadline(recoveredBackoff) {
                    emitBackoffEvent(recoveredBackoff)
                    await startupGate.succeed()
                    gateResolved = true
                }
                try await sleep(until: recoveredBackoff.nextRetryAtMS)
                if recoveredBackoff.resumeState == .connecting {
                    replayConnectBackoff = recoveredBackoff
                } else {
                    replayCycleBackoff = recoveredBackoff
                }
            }
            try transition(to: .connecting, lifecycleGeneration: generation)
        } catch is CancellationError {
            if !gateResolved {
                await startupGate.fail(CancellationError())
            }
            return false
        } catch {
            handleSyncError(error)
            await startupGate.fail(error)
            return false
        }

        while !Task.isCancelled {
            do {
                if !connected {
                    let connectResult: ConnectOperationResult
                    if let storedBackoff = replayConnectBackoff {
                        connectResult = try await reconnectUsingBackoff(storedBackoff)
                        replayConnectBackoff = nil
                    } else {
                        connectResult = try await connect(schemaReset: schemaReset)
                    }
                    try ensureLifecycleActive(generation)
                    try await installConnectedState(
                        connectResult.response,
                        completedConnectRequestBody: connectResult.requestBody,
                        lifecycleGeneration: generation
                    )
                    connected = true
                }

                try ensureLifecycleActive(generation)
                try await runSerializedSyncCycleWithRetry(
                    resuming: replayCycleBackoff,
                    lifecycleGeneration: generation
                )
                replayCycleBackoff = nil
                startPendingObserver(generation: generation)

                if !gateResolved {
                    await startupGate.succeed()
                }
                options?.initialSyncCompleted?()
                return true
            } catch let error as RetryableError {
                do {
                    guard !Task.isCancelled, isLifecycleActive(generation) else {
                        if !gateResolved {
                            await startupGate.fail(CancellationError())
                        }
                        return false
                    }
                    let alreadyPersisted = getSyncStatus() == .backoff
                    let backoff: LocalBackoffRecord
                    if alreadyPersisted, let current = try currentBackoff(for: error) {
                        backoff = current
                    } else {
                        backoff = try persistBackoff(error)
                    }
                    if !alreadyPersisted {
                        try transition(to: .backoff, lifecycleGeneration: generation)
                    }
                    emitBackoffEvent(backoff)
                    if !gateResolved {
                        await startupGate.succeed()
                        gateResolved = true
                    }
                    try await sleep(until: backoff.nextRetryAtMS)
                    try transition(
                        to: syncStatus(for: backoff.resumeState),
                        lifecycleGeneration: generation
                    )
                    if backoff.resumeState == .connecting {
                        connected = false
                        replayConnectBackoff = backoff
                    } else {
                        replayCycleBackoff = backoff
                    }
                } catch {
                    if !Task.isCancelled {
                        handleSyncError(error)
                    }
                    return false
                }
            } catch is CancellationError {
                if !gateResolved {
                    await startupGate.fail(CancellationError())
                }
                return false
            } catch {
                handleSyncError(error)
                if gateResolved {
                    finishStartupFailure(generation)
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

    private func runSerializedSyncCycleWithRetry(
        resuming backoff: LocalBackoffRecord? = nil,
        lifecycleGeneration requestedGeneration: Int64? = nil
    ) async throws {
        let generation = requestedGeneration ?? state.withLock { $0.lifecycleGeneration }
        let cycleTask = try cycleGate.enqueue(generation: generation) { [weak self] in
            guard let self else { return }
            try await self.runSyncCycleWithRetry(
                resuming: backoff,
                lifecycleGeneration: generation
            )
        }
        try await withTaskCancellationHandler {
            try await cycleTask.value
        } onCancel: {
            cycleTask.cancel()
        }
    }

    private func runSyncCycleWithRetry(
        resuming backoff: LocalBackoffRecord? = nil,
        lifecycleGeneration: Int64
    ) async throws {
        try await runSingleSyncCycleWithRetry(
            resuming: backoff,
            lifecycleGeneration: lifecycleGeneration
        )
    }

    private func runSingleSyncCycleWithRetry(
        resuming initialBackoff: LocalBackoffRecord? = nil,
        lifecycleGeneration: Int64
    ) async throws {
        var attempt = 0
        var lastError: Error?
        var backoff = initialBackoff

        while attempt <= config.maxRetryAttempts {
            do {
                do {
                    if let resumeBackoff = backoff {
                        let resumeStatus = syncStatus(for: resumeBackoff.resumeState)
                        let currentStatus = getSyncStatus()
                        if currentStatus == .ready {
                            try transition(
                                to: resumeStatus,
                                lifecycleGeneration: lifecycleGeneration
                            )
                        } else if currentStatus != resumeStatus {
                            throw SynchroError.invalidStateTransition(
                                from: currentStatus,
                                to: resumeStatus
                            )
                        }
                        try await resumeDurableWork(
                            resumeBackoff,
                            lifecycleGeneration: lifecycleGeneration
                        )
                        backoff = nil
                    } else {
                        try await runSyncCycle(lifecycleGeneration: lifecycleGeneration)
                    }
                } catch is BindingRenewalError {
                    try await reconnectAfterBindingRenewal(lifecycleGeneration: lifecycleGeneration)
                    backoff = nil
                    continue
                }
                return
            } catch let error as RetryableError {
                try ensureLifecycleActive(lifecycleGeneration)
                attempt += 1
                lastError = error
                let persistedBackoff = try persistBackoff(error)
                try transition(to: .backoff, lifecycleGeneration: lifecycleGeneration)
                emitBackoffEvent(persistedBackoff)
                guard attempt <= config.maxRetryAttempts else {
                    break
                }
                do {
                    try await sleep(until: persistedBackoff.nextRetryAtMS)
                } catch {
                    throw lastError!
                }
                try ensureLifecycleActive(lifecycleGeneration)
                try transition(
                    to: syncStatus(for: persistedBackoff.resumeState),
                    lifecycleGeneration: lifecycleGeneration
                )
                backoff = persistedBackoff
            } catch {
                // Non-retryable errors propagate immediately
                handleSyncError(error)
                throw error
            }
        }

        if let lastError {
            throw lastError
        }
    }

    private func retryDelay(attempt: Int, serverRetryAfter: TimeInterval?) -> TimeInterval {
        // Exponential backoff: 1s, 2s, 4s, 8s, 16s cap
        let base = min(pow(2.0, Double(min(max(attempt - 1, 0), 4))), 16.0)
        // Add jitter: 0-50% of base
        let jitter = Double.random(in: 0...(base * 0.5))
        let exponentialDeadline = base + jitter
        let serverMinimum = serverRetryAfter.flatMap { delay in
            delay.isFinite && delay >= 0 ? delay : nil
        } ?? 0
        return max(exponentialDeadline, serverMinimum)
    }

    private func loadPersistedBackoff() throws -> LocalBackoffRecord? {
        try database.readTransaction { db in
            try SynchroMeta.getBackoffRecord(db)
        }
    }

    private func isFutureDeadline(_ backoff: LocalBackoffRecord) -> Bool {
        backoff.nextRetryAtMS > currentTimeMS()
    }

    private func currentBackoff(for error: RetryableError) throws -> LocalBackoffRecord? {
        try database.readTransaction { db in
            guard let backoff = try SynchroMeta.getBackoffRecord(db),
                  backoff.resumeState == error.resumeState,
                  backoff.workIdentity == error.workIdentity else {
                return nil
            }
            return backoff
        }
    }

    private func persistBackoff(_ error: RetryableError) throws -> LocalBackoffRecord {
        try database.writeTransaction { db in
            let current = try SynchroMeta.getBackoffRecord(db)
            let attemptCount: Int64
            if let current,
               current.resumeState == error.resumeState,
               current.workIdentity == error.workIdentity {
                attemptCount = current.attemptCount == Int64.max
                    ? Int64.max
                    : current.attemptCount + 1
            } else {
                attemptCount = 1
            }
            let delay = retryDelay(
                attempt: Int(min(attemptCount, Int64(Int.max))),
                serverRetryAfter: error.retryAfter
            )
            let record = LocalBackoffRecord(
                resumeState: error.resumeState,
                workIdentity: error.workIdentity,
                retryClassification: error.classification,
                attemptCount: attemptCount,
                nextRetryAtMS: retryDeadline(after: delay)
            )
            try SynchroMeta.upsertBackoffRecord(db, record: record)
            return record
        }
    }

    private func retryDeadline(after delay: TimeInterval) -> Int64 {
        RetryTiming.deadline(nowMS: currentTimeMS(), delaySeconds: delay)
    }

    private func retryDate(for backoff: LocalBackoffRecord) -> Date {
        Date(timeIntervalSince1970: TimeInterval(backoff.nextRetryAtMS) / 1_000)
    }

    private func sleep(until deadlineMS: Int64) async throws {
        while true {
            let remaining = RetryTiming.nanosecondsUntil(
                nowMS: currentTimeMS(),
                deadlineMS: deadlineMS
            )
            guard remaining > 0 else { return }
            try await Task.sleep(
                nanoseconds: min(remaining, RetryTiming.maximumSleepChunkNanoseconds)
            )
        }
    }

    private func currentTimeMS() -> Int64 {
        Int64((Date().timeIntervalSince1970 * 1_000).rounded(.down))
    }

    private func decodeBackoffRequest<T: Decodable>(_ type: T.Type, body: Data) throws -> T {
        do {
            try Integrity.validateCanonicalWireJSON(body)
            return try decoder.decode(type, from: body)
        } catch {
            throw SynchroError.invalidResponse(message: "durable retry request identity is invalid")
        }
    }

    private func exactRequestJSON(_ body: Data) throws -> String {
        try Integrity.validateCanonicalWireJSON(body)
        guard let requestJSON = String(data: body, encoding: .utf8), !requestJSON.isEmpty else {
            throw SynchroError.invalidResponse(message: "request identity is not UTF-8 JSON")
        }
        return requestJSON
    }

    private func reconnectUsingBackoff(_ backoff: LocalBackoffRecord) async throws -> ConnectOperationResult {
        guard backoff.resumeState == .connecting else {
            throw SynchroError.invalidResponse(message: "durable retry does not identify a connect request")
        }
        let requestBody = Data(backoff.workIdentity.utf8)
        let request = try decodeBackoffRequest(ConnectRequest.self, body: requestBody)
        guard request.clientID == clientID else {
            throw SynchroError.invalidResponse(message: "durable connect retry identity targets another client")
        }
        let response = try await httpClient.connect(request: request, requestBody: requestBody)
        return ConnectOperationResult(response: response, requestBody: requestBody)
    }

    private func resumeDurableWork(
        _ backoff: LocalBackoffRecord,
        lifecycleGeneration: Int64
    ) async throws {
        switch backoff.resumeState {
        case .connecting:
            let connectResult = try await reconnectUsingBackoff(backoff)
            try await installConnectedState(
                connectResult.response,
                completedConnectRequestBody: connectResult.requestBody,
                lifecycleGeneration: lifecycleGeneration
            )
            try await runSyncCycle(lifecycleGeneration: lifecycleGeneration)

        case .pushing:
            try await runPush(
                expectedBatchID: backoff.workIdentity,
                lifecycleGeneration: lifecycleGeneration
            )
            try transition(to: .ready, lifecycleGeneration: lifecycleGeneration)
            if !(try scopeIDsNeedingRebuild()).isEmpty {
                try transition(to: .rebuilding, lifecycleGeneration: lifecycleGeneration)
                try await rebuildAssignedScopesNeedingCursor()
                try schemaManager.finishAppliedMigrationIfPossible()
                try transition(to: .ready, lifecycleGeneration: lifecycleGeneration)
            }
            try transition(to: .pulling, lifecycleGeneration: lifecycleGeneration)
            try await runPullLoop(lifecycleGeneration: lifecycleGeneration)
            if getSyncStatus() == .rebuilding {
                try schemaManager.finishAppliedMigrationIfPossible()
            }
            try transition(to: .ready, lifecycleGeneration: lifecycleGeneration)

        case .pulling:
            try await runPullLoop(
                replayRequestBody: Data(backoff.workIdentity.utf8),
                lifecycleGeneration: lifecycleGeneration
            )
            if getSyncStatus() == .rebuilding {
                try schemaManager.finishAppliedMigrationIfPossible()
            }
            try transition(to: .ready, lifecycleGeneration: lifecycleGeneration)

        case .rebuilding:
            let requestBody = Data(backoff.workIdentity.utf8)
            let request = try decodeBackoffRequest(RebuildRequest.self, body: requestBody)
            try await rebuildScope(scopeID: request.scope, replayRequestBody: requestBody)
            try await rebuildAssignedScopesNeedingCursor()
            try schemaManager.finishAppliedMigrationIfPossible()
            try transition(to: .ready, lifecycleGeneration: lifecycleGeneration)
            try transition(to: .pulling, lifecycleGeneration: lifecycleGeneration)
            try await runPullLoop(lifecycleGeneration: lifecycleGeneration)
            if getSyncStatus() == .rebuilding {
                try schemaManager.finishAppliedMigrationIfPossible()
            }
            try transition(to: .ready, lifecycleGeneration: lifecycleGeneration)
        }
    }

    private func runSyncCycle(lifecycleGeneration: Int64) async throws {
        try ensureLifecycleActive(lifecycleGeneration)
        guard getSyncStatus() == .ready else {
            throw SynchroError.invalidStateTransition(from: getSyncStatus(), to: .pushing)
        }

        if try changeTracker.hasPendingChanges() {
            try transition(to: .pushing, lifecycleGeneration: lifecycleGeneration)
            try await runPush(lifecycleGeneration: lifecycleGeneration)
            try transition(to: .ready, lifecycleGeneration: lifecycleGeneration)
        }

        if !(try scopeIDsNeedingRebuild()).isEmpty {
            try transition(to: .rebuilding, lifecycleGeneration: lifecycleGeneration)
            try await rebuildAssignedScopesNeedingCursor()
            try schemaManager.finishAppliedMigrationIfPossible()
            try transition(to: .ready, lifecycleGeneration: lifecycleGeneration)
        }

        try transition(to: .pulling, lifecycleGeneration: lifecycleGeneration)
        try await runPullLoop(lifecycleGeneration: lifecycleGeneration)
        if getSyncStatus() == .rebuilding {
            try schemaManager.finishAppliedMigrationIfPossible()
            try transition(to: .ready, lifecycleGeneration: lifecycleGeneration)
        } else {
            try transition(to: .ready, lifecycleGeneration: lifecycleGeneration)
        }
    }

    // MARK: - Push

    private func runPush(
        expectedBatchID: String? = nil,
        lifecycleGeneration: Int64
    ) async throws {
        if try pushProcessor.hasRenewalRequiredBatches() {
            try await reconnectAfterBindingRenewal(lifecycleGeneration: lifecycleGeneration)
        }
        var hasMore = true
        var expectedBatchID = expectedBatchID
        while hasMore {
            let outcome: PushProcessor.PushOutcome?
            do {
                outcome = try await pushProcessor.processPush(
                    httpClient: httpClient,
                    clientID: clientID,
                    clientGeneration: clientGeneration,
                    schemaVersion: schemaVersion,
                    schemaHash: schemaHash,
                    syncedTables: syncedTables,
                    batchSize: config.pushBatchSize,
                    expectedBatchID: expectedBatchID
                )
            } catch is BindingRenewalError {
                try await reconnectAfterBindingRenewal(lifecycleGeneration: lifecycleGeneration)
                try transition(to: .pushing, lifecycleGeneration: lifecycleGeneration)
                expectedBatchID = nil
                continue
            }

            if let outcome {
                for accepted in outcome.response.accepted {
                    emitEvent(.mutationAccepted(SyncMutationEvent(
                        mutationID: accepted.mutationID,
                        tableID: accepted.table,
                        status: accepted.status,
                        rejectionCode: nil
                    )))
                }
                for rejected in outcome.response.rejected {
                    emitEvent(.mutationRejected(SyncMutationEvent(
                        mutationID: rejected.mutationID,
                        tableID: rejected.table,
                        status: rejected.status,
                        rejectionCode: rejected.code
                    )))
                }
                for conflict in outcome.conflicts {
                    fireConflict(conflict)
                }
                hasMore = try changeTracker.hasPendingChanges()
                if hasMore {
                    try transition(to: .pushing, lifecycleGeneration: lifecycleGeneration)
                }
                expectedBatchID = nil
            } else {
                hasMore = false
            }
        }
    }

    // MARK: - Pull

    private func runPullLoop(
        replayRequestBody: Data? = nil,
        lifecycleGeneration: Int64
    ) async throws {
        var hasMore = true
        var scopesToRebuild = Set<String>()
        var scopeSetVersion = try database.readTransaction { db in
            try SynchroMeta.getInt64(db, key: .scopeSetVersion)
        }
        var replayRequestBody = replayRequestBody

        while hasMore {
            let scopes = try loadKnownScopes()
            if scopes.isEmpty {
                return
            }

            let request: PullRequest
            if let replayRequestBody {
                let replayRequest = try decodeBackoffRequest(PullRequest.self, body: replayRequestBody)
                guard replayRequest.clientID == clientID,
                      replayRequest.clientGeneration == clientGeneration,
                      replayRequest.schema == SchemaRef(version: schemaVersion, hash: schemaHash),
                      replayRequest.scopeSetVersion == scopeSetVersion,
                      replayRequest.scopes == scopes,
                      replayRequest.limit == config.pullPageSize else {
                    throw SynchroError.invalidResponse(message: "durable pull retry identity does not match local state")
                }
                request = replayRequest
            } else {
                request = PullRequest(
                    clientID: clientID,
                    clientGeneration: clientGeneration,
                    schema: SchemaRef(version: schemaVersion, hash: schemaHash),
                    scopeSetVersion: scopeSetVersion,
                    scopes: scopes,
                    limit: config.pullPageSize
                )
            }

            let requestBody: Data
            if let replayRequestBody {
                requestBody = replayRequestBody
            } else {
                requestBody = try httpClient.pullRequestBody(request)
            }
            let requestJSON = try exactRequestJSON(requestBody)
            let response = try await httpClient.pull(request: request, requestBody: requestBody)
            try response.validate(
                activeScopes: Set(scopes.keys),
                requestScopeSetVersion: request.scopeSetVersion
            )

            try pullProcessor.applyScopeChanges(
                changes: response.changes,
                syncedTables: syncedTables,
                scopeCursors: response.scopeCursors,
                checksums: response.checksums,
                schemaHash: schemaHash,
                scopeUpdates: response.scopeUpdates,
                scopeSetVersion: response.scopeSetVersion,
                rebuildScopes: Set(response.rebuild),
                completedPullRequestJSON: requestJSON
            )
            scopesToRebuild.formUnion(response.rebuild)
            scopeSetVersion = response.scopeSetVersion

            hasMore = response.hasMore
            if hasMore {
                try transition(to: .pulling, lifecycleGeneration: lifecycleGeneration)
            }
            replayRequestBody = nil
        }

        let knownScopeIDs = Set(try database.readTransaction { db in
            try SynchroMeta.getAllScopes(db).map(\.scopeID)
        })
        let missingCursorScopes = try scopeIDsNeedingRebuild()

        let rebuilds = scopesToRebuild.union(missingCursorScopes).filter(knownScopeIDs.contains)
        if !rebuilds.isEmpty {
            try transition(to: .rebuilding, lifecycleGeneration: lifecycleGeneration)
        }
        for scopeID in rebuilds.sorted(by: utf8Less) {
            try await rebuildScope(scopeID: scopeID)
        }
    }

    // MARK: - Scope Rebuild

    private func rebuildScope(scopeID: String, replayRequestBody: Data? = nil) async throws {
        var attempt = try pullProcessor.beginScopeRebuild(
            scopeID: scopeID,
            clientGeneration: clientGeneration,
            schemaVersion: schemaVersion,
            schemaHash: schemaHash,
            pageLimit: config.pullPageSize,
            syncedTables: syncedTables
        )
        emitEvent(.rebuildRequested(SyncRebuildEvent(
            scopeID: scopeID,
            rebuildID: attempt.rebuildID
        )))
        var replayRequestBody = replayRequestBody

        while true {
            let request: RebuildRequest
            if let replayRequestBody {
                let replayRequest = try decodeBackoffRequest(RebuildRequest.self, body: replayRequestBody)
                guard replayRequest.clientID == clientID,
                      replayRequest.clientGeneration == attempt.clientGeneration,
                      replayRequest.schema == SchemaRef(version: attempt.schemaVersion, hash: attempt.schemaHash),
                      replayRequest.scope == scopeID,
                      replayRequest.rebuildID == attempt.rebuildID,
                      replayRequest.cursor == attempt.cursor,
                      replayRequest.limit == attempt.pageLimit else {
                    throw SynchroError.invalidResponse(message: "durable rebuild retry identity does not match local state")
                }
                request = replayRequest
            } else {
                request = RebuildRequest(
                    clientID: clientID,
                    clientGeneration: attempt.clientGeneration,
                    schema: SchemaRef(version: attempt.schemaVersion, hash: attempt.schemaHash),
                    scope: scopeID,
                    rebuildID: attempt.rebuildID,
                    cursor: attempt.cursor,
                    limit: attempt.pageLimit
                )
            }
            let requestBody: Data
            if let replayRequestBody {
                requestBody = replayRequestBody
            } else {
                requestBody = try httpClient.rebuildRequestBody(request)
            }

            if let finality = try pullProcessor.pendingRebuildFinality(
                attempt: attempt,
                request: request,
                requestBody: requestBody
            ) {
                do {
                    try pullProcessor.finalizeScopeRebuild(
                        attempt: attempt,
                        finalCursor: finality.finalCursor,
                        checksum: finality.checksum,
                        syncedTables: syncedTables
                    )
                    emitEvent(.rebuildCompleted(SyncRebuildEvent(
                        scopeID: scopeID,
                        rebuildID: attempt.rebuildID
                    )))
                    return
                } catch is RebuildChecksumMismatchError {
                    attempt = try restartScopeRebuild(scopeID: scopeID)
                    replayRequestBody = nil
                    continue
                }
            }

            do {
                let result = try await httpClient.rebuildWithBody(
                    request: request,
                    requestBody: requestBody
                )
                let response = result.response
                attempt = try pullProcessor.applyScopeRebuildPage(
                    attempt: attempt,
                    request: request,
                    requestBody: result.requestBody,
                    response: response,
                    responseBody: result.responseBody,
                    syncedTables: syncedTables
                )
                replayRequestBody = nil

                if response.hasMore {
                    try transition(to: .rebuilding)
                    continue
                }

                guard let finality = try pullProcessor.pendingRebuildFinality(
                    attempt: attempt,
                    request: request,
                    requestBody: result.requestBody
                ) else {
                    throw SynchroError.invalidResponse(message: "final rebuild page did not persist finality")
                }
                do {
                    try pullProcessor.finalizeScopeRebuild(
                        attempt: attempt,
                        finalCursor: finality.finalCursor,
                        checksum: finality.checksum,
                        syncedTables: syncedTables
                    )
                    emitEvent(.rebuildCompleted(SyncRebuildEvent(
                        scopeID: scopeID,
                        rebuildID: attempt.rebuildID
                    )))
                    return
                } catch is RebuildChecksumMismatchError {
                    attempt = try restartScopeRebuild(scopeID: scopeID)
                    replayRequestBody = nil
                }
            } catch let error as RebuildRestartRequiredError {
                guard error.scopeID == scopeID else {
                    throw SynchroError.invalidResponse(message: "rebuild restart response targets an unexpected scope")
                }
                attempt = try restartScopeRebuild(scopeID: scopeID)
                replayRequestBody = nil
            }
        }
    }

    private func restartScopeRebuild(scopeID: String) throws -> LocalRebuildAttempt {
        try pullProcessor.restartScopeRebuild(
            scopeID: scopeID,
            clientGeneration: clientGeneration,
            schemaVersion: schemaVersion,
            schemaHash: schemaHash,
            pageLimit: config.pullPageSize,
            syncedTables: syncedTables
        )
    }

    // MARK: - Bootstrap

    private func connect(schemaReset: Bool = false) async throws -> ConnectOperationResult {
        try pullProcessor.prepareSeedReceiptsForConnect()
        let schemaState = try database.readTransaction { db in
            (
                version: try SynchroMeta.getInt64(db, key: .schemaVersion),
                hash: try SynchroMeta.get(db, key: .schemaHash) ?? "",
                clientGeneration: try SynchroMeta.getInt64(db, key: .clientGeneration),
                scopeSetVersion: try SynchroMeta.getInt64(db, key: .scopeSetVersion),
                seedReceipts: try SynchroMeta.getSeedReceipts(db),
                knownScopes: try SynchroMeta.getAllScopes(db)
            )
        }

        let request = ConnectRequest(
            clientID: clientID,
            clientGeneration: schemaState.clientGeneration > 0 ? schemaState.clientGeneration : nil,
            platform: config.platform,
            appVersion: config.appVersion,
            protocolVersion: 3,
            schemaReset: schemaReset ? true : nil,
            schema: SchemaRef(version: schemaState.version, hash: schemaState.hash),
            scopeSetVersion: schemaState.scopeSetVersion,
            knownScopes: Dictionary(
                uniqueKeysWithValues: schemaState.knownScopes.map { scope in
                    (scope.scopeID, ScopeCursorRef(cursor: scope.cursor))
                }
            ),
            seedReceipts: schemaState.seedReceipts.isEmpty ? nil : schemaState.seedReceipts
        )

        let requestBody = try httpClient.connectRequestBody(request)
        let response = try await httpClient.connect(request: request, requestBody: requestBody)
        try response.validate(
            existingScopes: request.knownScopes,
            requestScopeSetVersion: request.scopeSetVersion
        )

        return ConnectOperationResult(response: response, requestBody: requestBody)
    }

    func installConnectedState(
        _ response: ConnectResponse,
        completedConnectRequestBody: Data? = nil,
        requiringChangeFrom oldBinding: (generation: Int64, schema: SchemaRef)? = nil,
        lifecycleGeneration expectedLifecycleGeneration: Int64? = nil
    ) async throws {
        if response.schema.action == .unsupported {
            guard let reason = response.schema.reason else {
                throw SynchroError.invalidResponse(message: "unsupported schema action has no reason")
            }
            let failure = SyncFailure(
                operation: .schema,
                code: .unsupportedSchema,
                retryable: false,
                message: "The installed schema requires an explicit synchronized reset.",
                recoveryAction: .schemaReset,
                metadata: ["reason": reason.rawValue]
            )
            try persistBlockingFailure(failure)
            if expectedLifecycleGeneration != nil {
                try transition(to: .error, lifecycleGeneration: expectedLifecycleGeneration)
            }
            emitEvent(.failure(failure))
            throw SynchroError.unsupportedSchema(reason: reason)
        }

        let connectSchema = try await resolveConnectSchema(response)
        let installedSchema = SchemaRef(version: connectSchema.version, hash: connectSchema.hash)
        if let oldBinding,
           oldBinding.generation == response.clientGeneration,
           oldBinding.schema == installedSchema {
            throw SynchroError.invalidResponse(message: "connect did not change the rejected request binding")
        }

        if let expectedLifecycleGeneration {
            try ensureLifecycleActive(expectedLifecycleGeneration)
        }

        let completedConnectRequestJSON: String?
        if let completedConnectRequestBody {
            completedConnectRequestJSON = try exactRequestJSON(completedConnectRequestBody)
        } else {
            completedConnectRequestJSON = nil
        }

        let schemaChanged = response.schema.action == .replace || response.schema.action == .rebuildLocal
        let schemaEvent: SyncSchemaEvent?
        if schemaChanged {
            guard let manifest = connectSchema.manifest else {
                throw SynchroError.invalidResponse(message: "schema action has no target manifest")
            }
            let source = try database.readTransaction { db in
                SchemaRef(
                    version: try SynchroMeta.getInt64(db, key: .schemaVersion),
                    hash: try SynchroMeta.get(db, key: .schemaHash) ?? ""
                )
            }
            let event = SyncSchemaEvent(
                source: source,
                target: SchemaRef(version: manifest.schemaVersion, hash: manifest.schemaHash),
                action: response.schema.action
            )
            schemaEvent = event
            if let expectedLifecycleGeneration {
                try transition(to: .schemaApplying, lifecycleGeneration: expectedLifecycleGeneration)
            }
            emitEvent(.schemaApplying(event))
            _ = try schemaManager.prepareMigration(
                targetManifest: manifest,
                action: response.schema.action,
                affectedScopes: response.affectedScopes ?? [],
                scopeCursorUpdates: response.scopeCursorUpdates,
                schemaReset: try loadBlockingFailure()?.recoveryAction == .schemaReset
            )
        } else {
            schemaEvent = nil
        }

        try database.writeSyncLockedTransaction { db in
            if schemaChanged {
                _ = try schemaManager.applyPreparedMigrationInTransaction(db)
            } else {
                let localVersion = try SynchroMeta.getInt64(db, key: .schemaVersion)
                let localHash = try SynchroMeta.get(db, key: .schemaHash) ?? ""
                guard localVersion == response.schema.version,
                      localHash == response.schema.hash else {
                    throw SynchroError.invalidResponse(message: "connect action none does not match local schema")
                }
                try SynchroMeta.applyScopeCursorUpdates(
                    db,
                    updates: response.scopeCursorUpdates,
                    affectedScopes: []
                )
            }
            try pullProcessor.installConnectedAssignmentInTransaction(
                db,
                delta: response.scopes,
                scopeSetVersion: response.scopeSetVersion,
                clientGeneration: response.clientGeneration,
                syncedTables: connectSchema.tables,
                scopeCursorUpdates: response.scopeCursorUpdates
            )
            try SynchroMeta.bindClientIDAfterAuthenticatedConnect(db, clientID: clientID)
            try SynchroMeta.clearBlockingFailure(db)
            if let completedConnectRequestJSON {
                try SynchroMeta.clearMatchingBackoffRecord(
                    db,
                    resumeState: .connecting,
                    workIdentity: completedConnectRequestJSON
                )
            }
        }
        database.updateApplicationSyncedTables(connectSchema.tables)
        if let schemaEvent {
            emitEvent(.schemaApplied(schemaEvent))
        }

        if let expectedLifecycleGeneration {
            try ensureLifecycleActive(expectedLifecycleGeneration)
        }
        try pushProcessor.renewSealedBatchesAfterBindingChange(
            clientID: clientID,
            clientGeneration: response.clientGeneration,
            schemaVersion: connectSchema.version,
            schemaHash: connectSchema.hash,
            syncedTables: connectSchema.tables
        )

        if let expectedLifecycleGeneration {
            try ensureLifecycleActive(expectedLifecycleGeneration)
        }
        if let expectedLifecycleGeneration {
            try state.withLock { state in
                guard state.started,
                      !state.closed,
                      state.lifecycleGeneration == expectedLifecycleGeneration else {
                    throw CancellationError()
                }
                syncedTables = connectSchema.tables
                schemaVersion = connectSchema.version
                schemaHash = connectSchema.hash
                clientGeneration = response.clientGeneration
                state.connectionReady = true
            }
        } else {
            syncedTables = connectSchema.tables
            schemaVersion = connectSchema.version
            schemaHash = connectSchema.hash
            clientGeneration = response.clientGeneration
        }
        if let expectedLifecycleGeneration {
            try transition(to: .ready, lifecycleGeneration: expectedLifecycleGeneration)
        }
    }

    private func reconnectAfterBindingRenewal(lifecycleGeneration: Int64) async throws {
        let oldBinding = (
            generation: clientGeneration,
            schema: SchemaRef(version: schemaVersion, hash: schemaHash)
        )
        try transition(to: .connecting, lifecycleGeneration: lifecycleGeneration)
        let connectResult = try await connect()
        try await installConnectedState(
            connectResult.response,
            completedConnectRequestBody: connectResult.requestBody,
            requiringChangeFrom: oldBinding,
            lifecycleGeneration: lifecycleGeneration
        )
    }

    private func resolveConnectSchema(
        _ response: ConnectResponse
    ) async throws -> (
        tables: [LocalSchemaTable],
        version: Int64,
        hash: String,
        manifest: SchemaManifest?
    ) {
        switch response.schema.action {
        case .none:
            guard let tables = try schemaManager.loadStoredLocalSchema() else {
                throw SynchroError.invalidResponse(message: "connect returned schema action none without stored local schema")
            }
            return (tables, response.schema.version, response.schema.hash, nil)

        case .replace, .rebuildLocal:
            guard let manifest = response.schemaDefinition else {
                throw SynchroError.invalidResponse(message: "connect schema action \(response.schema.action.rawValue) missing schema_definition")
            }
            let tables = try manifest.localTables()
            return (tables, response.schema.version, response.schema.hash, manifest)

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
        for scopeID in try scopeIDsNeedingRebuild().sorted(by: utf8Less) {
            if getSyncStatus() == .rebuilding {
                try transition(to: .rebuilding)
            }
            try await rebuildScope(scopeID: scopeID)
        }
    }

    // MARK: - Debounce

    private func startPendingObserver(generation: Int64) {
        let canStart = state.withLock { state in
            state.started && !state.closed && state.lifecycleGeneration == generation && pendingObserver == nil
        }
        guard canStart else { return }

        let observer = database.onChange(tables: ["_synchro_pending_changes"]) { [weak self] in
            guard let self, (try? self.changeTracker.hasUnsealedChanges()) == true else { return }
            self.scheduleDebouncedPush(generation: generation)
        }
        let installed = state.withLock { state in
            guard state.started,
                  !state.closed,
                  state.lifecycleGeneration == generation,
                  pendingObserver == nil else {
                return false
            }
            pendingObserver = observer
            pendingObserverGeneration = generation
            return true
        }
        if !installed {
            observer.cancel()
        }
    }

    private func scheduleDebouncedPush(generation: Int64) {
        let oldTask = state.withLock { state -> Task<Void, Never>? in
            guard state.started, !state.closed, state.lifecycleGeneration == generation else {
                return nil
            }
            let oldTask = debounceTask
            debounceTask = nil
            debounceTaskID = nil
            return oldTask
        }
        oldTask?.cancel()

        let taskID = UUID()
        let task = Task { [weak self] in
            guard let self else { return }
            guard self.beginManagedOperation(generation: generation) else { return }
            defer { self.endOperation() }
            do {
                try await Task.sleep(nanoseconds: UInt64(self.config.pushDebounce * 1_000_000_000))
                guard !Task.isCancelled,
                      self.claimDebounceTask(taskID, generation: generation) else { return }
                try await self.runSerializedSyncCycleWithRetry(
                    lifecycleGeneration: generation
                )
            } catch {
                // Debounced sync failed, so the managed loop retries it later.
            }
        }
        let installed = state.withLock { state in
            guard state.started, !state.closed, state.lifecycleGeneration == generation else {
                return false
            }
            debounceTask = task
            debounceTaskID = taskID
            return true
        }
        if !installed {
            task.cancel()
        }
    }

    private func claimDebounceTask(_ taskID: UUID, generation: Int64) -> Bool {
        state.withLock { state in
            guard state.started,
                  !state.closed,
                  state.lifecycleGeneration == generation,
                  debounceTaskID == taskID else {
                return false
            }
            debounceTask = nil
            debounceTaskID = nil
            return true
        }
    }

    // MARK: - Error Handling

    private func reserveStart() throws -> Int64 {
        try state.withLock { state in
            if state.closed {
                throw SynchroError.notStarted
            }
            if state.started || state.stopping {
                throw SynchroError.alreadyStarted
            }
            guard !state.backgrounded,
                  state.currentStatus == .localReady
                    || state.currentStatus == .error
                    || state.currentStatus == .stopped else {
                throw SynchroError.notStarted
            }
            state.started = true
            state.explicitlyStopped = false
            state.connectionReady = false
            state.lifecycleGeneration &+= 1
            state.activeOperations += 1
            return state.lifecycleGeneration
        }
    }

    private func beginCallerOperation() -> Int64? {
        state.withLock { state in
            guard state.started,
                  !state.stopping,
                  !state.backgrounded,
                  !state.closed,
                  state.currentStatus != .backoff,
                  state.connectionReady else { return nil }
            state.activeOperations += 1
            return state.lifecycleGeneration
        }
    }

    private func beginManagedOperation(generation: Int64) -> Bool {
        state.withLock { state in
            guard state.started,
                  !state.closed,
                  state.lifecycleGeneration == generation else {
                return false
            }
            state.activeOperations += 1
            return true
        }
    }

    private func endOperation() {
        let waiters = state.withLock { state -> [CheckedContinuation<Void, Never>] in
            precondition(state.activeOperations > 0, "sync operation tracking underflow")
            state.activeOperations -= 1
            guard state.activeOperations == 0 else { return [] }
            let waiters = state.operationWaiters
            state.operationWaiters.removeAll()
            return waiters
        }
        for waiter in waiters {
            waiter.resume()
        }
    }

    private func waitForOperationsToDrain() async {
        await withCheckedContinuation { continuation in
            let drained = state.withLock { state in
                if state.activeOperations == 0 {
                    return true
                }
                state.operationWaiters.append(continuation)
                return false
            }
            if drained {
                continuation.resume()
            }
        }
    }

    private func isLifecycleActive(_ generation: Int64) -> Bool {
        state.withLock { state in
            state.started
                && !state.stopping
                && !state.backgrounded
                && !state.closed
                && state.lifecycleGeneration == generation
        }
    }

    private func ensureLifecycleActive(_ generation: Int64) throws {
        guard isLifecycleActive(generation), !Task.isCancelled else {
            throw CancellationError()
        }
    }

    private func installManagedTask(_ task: Task<Void, Never>, generation: Int64) -> Bool {
        state.withLock { state in
            guard state.started,
                  !state.closed,
                  state.lifecycleGeneration == generation else {
                return false
            }
            syncTask = task
            return true
        }
    }

    private func handleSyncError(_ error: Error) {
        guard !(error is CancellationError) else { return }
        let shouldHandle = state.withLock {
            !$0.stopping && !$0.closed && ($0.started || $0.currentStatus != .error)
        }
        guard shouldHandle else { return }

        let failure: SyncFailure
        if case let SynchroError.blocked(existing) = error {
            failure = existing
        } else {
            failure = blockingFailure(
                for: error,
                operation: operationForCurrentState(),
                fallbackCode: .invalidResponse,
                recovery: .retry
            )
        }
        do {
            let alreadyPublicError = getSyncStatus() == .error
            if !alreadyPublicError {
                try persistBlockingFailure(failure)
            }
            let terminated = state.withLock { state -> (
                generation: Int64,
                resources: LifecycleResources,
                canEnterError: Bool
            ) in
                let generation = state.lifecycleGeneration
                let resources = LifecycleResources(
                    syncTask: syncTask,
                    debounceTask: debounceTask,
                    pendingObserver: pendingObserver
                )
                syncTask = nil
                debounceTask = nil
                debounceTaskID = nil
                pendingObserver = nil
                pendingObserverGeneration = nil
                state.started = false
                state.connectionReady = false
                state.lifecycleGeneration &+= 1
                return (
                    generation,
                    resources,
                    state.currentStatus != .error
                        && state.currentStatus.permitsTransition(to: .error)
                )
            }
            _ = cycleGate.invalidate(generation: terminated.generation)
            terminated.resources.syncTask?.cancel()
            terminated.resources.debounceTask?.cancel()
            terminated.resources.pendingObserver?.cancel()
            if terminated.canEnterError {
                try transition(to: .error)
            }
            if !alreadyPublicError {
                emitEvent(.failure(failure))
            }
        } catch {
            // The original failure remains the caller-visible result.
        }
    }

    private func teardownAfterFailedStart(_ generation: Int64) {
        _ = cycleGate.invalidate(generation: generation)
        let resources = state.withLock { state -> LifecycleResources? in
            guard state.lifecycleGeneration == generation else { return nil }
            let resources = LifecycleResources(
                syncTask: syncTask,
                debounceTask: debounceTask,
                pendingObserver: pendingObserver
            )
            syncTask = nil
            debounceTask = nil
            debounceTaskID = nil
            pendingObserver = nil
            pendingObserverGeneration = nil
            state.started = false
            state.connectionReady = false
            return resources
        }
        resources?.syncTask?.cancel()
        resources?.debounceTask?.cancel()
        resources?.pendingObserver?.cancel()
    }

    private func finishStartupFailure(_ generation: Int64) {
        _ = cycleGate.invalidate(generation: generation)
        let resources = state.withLock { state -> LifecycleResources? in
            guard state.lifecycleGeneration == generation else { return nil }
            let resources = LifecycleResources(
                syncTask: nil,
                debounceTask: debounceTask,
                pendingObserver: pendingObserver
            )
            syncTask = nil
            debounceTask = nil
            debounceTaskID = nil
            pendingObserver = nil
            pendingObserverGeneration = nil
            state.started = false
            state.connectionReady = false
            return resources
        }
        resources?.debounceTask?.cancel()
        resources?.pendingObserver?.cancel()
    }

    // MARK: - Status And Lifecycle Coordination

    private func transition(
        to status: SyncStatus,
        lifecycleGeneration: Int64? = nil
    ) throws {
        let result = try state.withLock { state -> (
            SyncStateChangeEvent,
            [(SyncStatus) -> Void],
            [(SyncEvent) -> Void]
        ) in
            if let lifecycleGeneration {
                guard state.started,
                      !state.stopping,
                      !state.closed,
                      state.lifecycleGeneration == lifecycleGeneration else {
                    throw CancellationError()
                }
            }
            let previous = state.currentStatus
            guard previous.permitsTransition(to: status) else {
                throw SynchroError.invalidStateTransition(from: previous, to: status)
            }
            state.currentStatus = status
            return (
                SyncStateChangeEvent(from: previous, to: status),
                Array(state.statusCallbacks.values),
                Array(state.eventCallbacks.values)
            )
        }
        for callback in result.1 {
            callback(status)
        }
        let event = SyncEvent.stateChanged(result.0)
        for callback in result.2 {
            callback(event)
        }
    }

    private func emitEvent(_ event: SyncEvent) {
        let callbacks = state.withLock { Array($0.eventCallbacks.values) }
        for callback in callbacks {
            callback(event)
        }
    }

    private func requestLifecycleStop(explicit: Bool) async {
        let task = state.withLock { state -> Task<Void, Never> in
            if explicit {
                state.explicitlyStopped = true
                state.resumeOnForeground = false
            }
            if let stopTask {
                return stopTask
            }
            let task = Task<Void, Never> { [weak self] in
                guard let self else { return }
                await self.performStop()
            }
            stopTask = task
            return task
        }
        await task.value
    }

    private func performStop() async {
        let stopState = state.withLock { state in
            let generation = state.lifecycleGeneration
            state.stopping = true
            state.started = false
            state.connectionReady = false
            state.lifecycleGeneration &+= 1
            let resources = LifecycleResources(
                syncTask: syncTask,
                debounceTask: debounceTask,
                pendingObserver: pendingObserver
            )
            syncTask = nil
            debounceTask = nil
            debounceTaskID = nil
            pendingObserver = nil
            pendingObserverGeneration = nil
            return (generation: generation, resources: resources)
        }

        let cycleTasks = cycleGate.invalidate(generation: stopState.generation)
        stopState.resources.syncTask?.cancel()
        stopState.resources.debounceTask?.cancel()
        stopState.resources.pendingObserver?.cancel()
        for task in cycleTasks {
            _ = await task.result
        }
        if let task = stopState.resources.debounceTask {
            _ = await task.result
        }
        if let task = stopState.resources.syncTask {
            await task.value
        }
        await waitForOperationsToDrain()

        if getSyncStatus() != .stopped {
            try? transition(to: .stopped)
        }
        state.withLock { state in
            state.stopping = false
            stopTask = nil
        }
    }

    private func waitForUnfinishedStop() async {
        while let task = state.withLock({ _ in stopTask }) {
            await task.value
        }
    }

    private func prepareStateForStart(lifecycleGeneration: Int64) throws {
        let current = getSyncStatus()
        switch current {
        case .stopped:
            try transition(
                to: .localReady,
                lifecycleGeneration: lifecycleGeneration
            )
        case .error:
            break
        case .localReady:
            break
        default:
            if state.withLock({ $0.started }) {
                throw SynchroError.alreadyStarted
            }
            throw SynchroError.invalidStateTransition(from: current, to: .localReady)
        }
    }

    private func enterReservedError(
        _ failure: SyncFailure,
        generation: Int64
    ) throws {
        try? persistBlockingFailure(failure)
        if getSyncStatus() != .error {
            try transition(to: .error, lifecycleGeneration: generation)
        }
        emitEvent(.failure(failure))
    }

    private func loadBlockingFailure() throws -> SyncFailure? {
        try database.readTransaction { db in
            try SynchroMeta.getBlockingFailure(db)
        }
    }

    private func requireBlockingFailure(
        recovery: SyncRecoveryAction
    ) throws -> SyncFailure {
        guard getSyncStatus() == .error,
              let failure = try loadBlockingFailure(),
              failure.recoveryAction == recovery else {
            throw SynchroError.notStarted
        }
        return failure
    }

    private func persistBlockingFailure(_ failure: SyncFailure) throws {
        try database.writeTransaction { db in
            try SynchroMeta.setBlockingFailure(db, failure: failure)
        }
    }

    private func blockingFailure(
        for error: Error,
        operation: SyncOperationKind,
        fallbackCode: SyncFailureCode,
        recovery: SyncRecoveryAction
    ) -> SyncFailure {
        if case let SynchroError.blocked(failure) = error {
            return failure
        }
        if case let SynchroError.unsupportedSchema(reason) = error {
            return SyncFailure(
                operation: .schema,
                code: .unsupportedSchema,
                retryable: false,
                message: "The installed schema requires an explicit synchronized reset.",
                recoveryAction: .schemaReset,
                metadata: ["reason": reason.rawValue]
            )
        }
        if case let SynchroError.upgradeRequired(current, minimum) = error {
            return SyncFailure(
                operation: operation,
                code: .upgradeRequired,
                retryable: false,
                message: "The client runtime does not satisfy the server requirement.",
                recoveryAction: .none,
                metadata: ["current": current, "minimum": minimum]
            )
        }
        if case let SynchroError.protocolError(status, code, _) = error {
            let failureCode: SyncFailureCode
            let recoveryAction: SyncRecoveryAction
            switch code {
            case .invalidRequest:
                failureCode = .invalidRequest
                recoveryAction = recovery
            case .invalidSchemaReference:
                failureCode = .invalidSchemaReference
                recoveryAction = recovery
            case .authRequired:
                failureCode = .authenticationRequired
                recoveryAction = .none
            case .idempotencyConflict:
                failureCode = .idempotencyConflict
                recoveryAction = .none
            case .clientRetired:
                failureCode = .clientRetired
                recoveryAction = .none
            case .syncIntegrityFailure:
                failureCode = .syncIntegrityFailure
                recoveryAction = .none
            case .upgradeRequired:
                failureCode = .upgradeRequired
                recoveryAction = .none
            case .clientGenerationExpired, .rebuildRestartRequired, .schemaMismatch,
                 .retryLater, .capturePending, .temporaryUnavailable:
                failureCode = fallbackCode
                recoveryAction = recovery
            }
            return SyncFailure(
                operation: operation,
                code: failureCode,
                retryable: false,
                message: "The server rejected the sync operation.",
                recoveryAction: recoveryAction,
                metadata: ["http_status": String(status)]
            )
        }
        if case let SynchroError.invalidStateTransition(from, to) = error {
            return SyncFailure(
                operation: .lifecycle,
                code: .invalidStateTransition,
                retryable: false,
                message: "The sync engine rejected an invalid lifecycle transition.",
                recoveryAction: .retry,
                metadata: ["from": from.rawValue, "to": to.rawValue]
            )
        }
        if case SynchroError.databaseError = error {
            return SyncFailure(
                operation: .database,
                code: .localDatabase,
                retryable: false,
                message: "The local sync database operation failed.",
                recoveryAction: recovery
            )
        }
        return SyncFailure(
            operation: operation,
            code: fallbackCode,
            retryable: false,
            message: "The sync operation failed contract validation.",
            recoveryAction: recovery
        )
    }

    private func operationForCurrentState() -> SyncOperationKind {
        switch getSyncStatus() {
        case .connecting: return .connecting
        case .schemaApplying: return .schema
        case .pushing: return .pushing
        case .pulling: return .pulling
        case .rebuilding: return .rebuilding
        case .uninitialized, .localReady, .ready, .backoff, .error, .stopped:
            return .lifecycle
        }
    }

    private func syncStatus(for resumeState: RetryResumeState) -> SyncStatus {
        switch resumeState {
        case .connecting: return .connecting
        case .pushing: return .pushing
        case .pulling: return .pulling
        case .rebuilding: return .rebuilding
        }
    }

    private func emitBackoffEvent(_ backoff: LocalBackoffRecord) {
        emitEvent(.backoff(SyncBackoffEvent(
            operation: operation(for: backoff.resumeState),
            attempt: backoff.attemptCount,
            retryAt: retryDate(for: backoff)
        )))
    }

    private func operation(for resumeState: RetryResumeState) -> SyncOperationKind {
        switch resumeState {
        case .connecting: return .connecting
        case .pushing: return .pushing
        case .pulling: return .pulling
        case .rebuilding: return .rebuilding
        }
    }

    private func utf8Less(_ lhs: String, _ rhs: String) -> Bool {
        Array(lhs.utf8).lexicographicallyPrecedes(Array(rhs.utf8))
    }

    private func installNativeLifecycleObservers() {
#if canImport(UIKit)
        nativeLifecycleObservers = [
            NotificationCenter.default.addObserver(
                forName: UIApplication.didEnterBackgroundNotification,
                object: nil,
                queue: nil
            ) { [weak self] _ in
                Task { await self?.enterBackground() }
            },
            NotificationCenter.default.addObserver(
                forName: UIApplication.willEnterForegroundNotification,
                object: nil,
                queue: nil
            ) { [weak self] _ in
                Task { try? await self?.enterForeground() }
            },
        ]
#elseif canImport(AppKit)
        nativeLifecycleObservers = [
            NotificationCenter.default.addObserver(
                forName: NSApplication.didResignActiveNotification,
                object: nil,
                queue: nil
            ) { [weak self] _ in
                Task { await self?.enterBackground() }
            },
            NotificationCenter.default.addObserver(
                forName: NSApplication.didBecomeActiveNotification,
                object: nil,
                queue: nil
            ) { [weak self] _ in
                Task { try? await self?.enterForeground() }
            },
        ]
#endif
    }

    private func removeNativeLifecycleObservers() {
        for observer in nativeLifecycleObservers {
            NotificationCenter.default.removeObserver(observer)
        }
        nativeLifecycleObservers.removeAll()
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
