package com.trainstar.synchro

import kotlinx.coroutines.*
import kotlinx.serialization.json.Json
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.min
import kotlin.math.pow

data class SyncOptions(
    val initialSyncCompleted: (() -> Unit)? = null
)

class SyncEngine(
    private val config: SynchroConfig,
    private val database: SynchroDatabase,
    private val httpClient: HttpClient,
    private val schemaManager: SchemaManager,
    private val changeTracker: ChangeTracker,
    private val pullProcessor: PullProcessor,
    private val pushProcessor: PushProcessor
) {
    private var scope: CoroutineScope? = null
    private var syncJob: Job? = null
    private var debounceJob: Job? = null
    private var pendingObserver: Cancellable? = null

    @Volatile
    private var currentStatus: SyncStatus = SyncStatus.Stopped
    private val statusCallbacks = ConcurrentHashMap<String, (SyncStatus) -> Unit>()
    private val conflictCallbacks = ConcurrentHashMap<String, (ConflictEvent) -> Unit>()
    private val snapshotCallbacks = ConcurrentHashMap<String, suspend () -> Boolean>()

    @Volatile
    private var syncedTables: List<SchemaTable> = emptyList()
    @Volatile
    private var schemaVersion: Long = 0
    @Volatile
    private var schemaHash: String = ""
    private val clientID: String get() = config.clientID
    @Volatile
    private var retryCount: Int = 0

    private val started = AtomicReference(false)
    private val cycleRunning = AtomicReference(false)
    private val cycleQueued = AtomicReference(false)

    // MARK: - Lifecycle

    suspend fun start(options: SyncOptions? = null) {
        if (!started.compareAndSet(false, true)) {
            throw SynchroError.AlreadyStarted()
        }

        try {
            // Create a fresh scope
            scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

            // Clear any stale sync lock from a previous crash.
            // If the process was killed while sync_lock was set, CDC triggers
            // would be permanently disabled, silently dropping all local changes.
            database.writeTransaction { db ->
                SynchroMeta.setSyncLock(db, false)
            }

            // Fetch schema first so we can register with the correct version
            val schema = schemaManager.ensureSchema(httpClient)
            syncedTables = schema.tables
            schemaVersion = schema.schemaVersion
            schemaHash = schema.schemaHash

            // Register with actual schema version
            val registerReq = RegisterRequest(
                clientID = config.clientID,
                clientName = null,
                platform = config.platform,
                appVersion = config.appVersion,
                schemaVersion = schema.schemaVersion,
                schemaHash = schema.schemaHash
            )
            val registerResp = httpClient.register(registerReq)

            val needsInitialSnapshot = database.writeTransaction { db ->
                SynchroMeta.set(db, MetaKey.CLIENT_SERVER_ID, registerResp.id)
                val checkpoint = SynchroMeta.getInt64(db, MetaKey.CHECKPOINT)
                val snapshotComplete = SynchroMeta.get(db, MetaKey.SNAPSHOT_COMPLETE) == "1"
                checkpoint == 0L || !snapshotComplete
            }

            updateStatus(SyncStatus.Idle)

            if (needsInitialSnapshot) {
                rebuildFromSnapshot(requiresApproval = false)
            }

            // Start sync loop
            syncJob = scope!!.launch {
                syncLoop()
            }

            // Start watching for pending changes (debounced push)
            startPendingObserver()

            // Run initial sync
            runSyncCycleWithRetry()
            options?.initialSyncCompleted?.invoke()
        } catch (e: Exception) {
            started.set(false)
            scope?.cancel()
            scope = null
            throw e
        }
    }

    fun stop() {
        syncJob?.cancel()
        syncJob = null
        debounceJob?.cancel()
        debounceJob = null
        pendingObserver?.cancel()
        pendingObserver = null
        scope?.cancel()
        scope = null
        started.set(false)
        updateStatus(SyncStatus.Stopped)
    }

    suspend fun syncNow() {
        runSyncCycleWithRetry()
    }

    // MARK: - Callbacks

    fun onStatusChange(callback: (SyncStatus) -> Unit): Cancellable {
        val id = UUID.randomUUID().toString()
        statusCallbacks[id] = callback
        return CallbackCancellable { statusCallbacks.remove(id) }
    }

    fun onConflict(callback: (ConflictEvent) -> Unit): Cancellable {
        val id = UUID.randomUUID().toString()
        conflictCallbacks[id] = callback
        return CallbackCancellable { conflictCallbacks.remove(id) }
    }

    fun onSnapshotRequired(callback: suspend () -> Boolean): Cancellable {
        val id = UUID.randomUUID().toString()
        snapshotCallbacks[id] = callback
        return CallbackCancellable { snapshotCallbacks.remove(id) }
    }

    // MARK: - Sync Loop

    private suspend fun syncLoop() {
        while (currentCoroutineContext().isActive) {
            delay((config.syncInterval * 1000).toLong())
            if (!currentCoroutineContext().isActive) return
            try {
                runSyncCycleWithRetry()
            } catch (e: CancellationException) {
                throw e
            } catch (_: Exception) {
                // Error already handled in runSyncCycleWithRetry
            }
        }
    }

    // MARK: - Retry

    private suspend fun runSyncCycleWithRetry() {
        if (!cycleRunning.compareAndSet(false, true)) {
            cycleQueued.set(true)
            return
        }
        try {
            do {
                cycleQueued.set(false)
                runSingleSyncCycleWithRetry()
            } while (cycleQueued.get())
        } finally {
            cycleRunning.set(false)
        }
    }

    private suspend fun runSingleSyncCycleWithRetry() {
        var attempt = 0
        var lastError: Exception? = null

        while (attempt <= config.maxRetryAttempts) {
            try {
                runSyncCycle()
                retryCount = 0
                return
            } catch (e: CancellationException) {
                throw e
            } catch (e: RetryableError) {
                attempt++
                lastError = e
                val delayMs = retryDelay(attempt, e.retryAfter)
                val retryAt = java.time.Instant.now().plusMillis((delayMs * 1000).toLong())
                updateStatus(SyncStatus.Error(retryAt = retryAt))
                delay((delayMs * 1000).toLong())
                if (!currentCoroutineContext().isActive) throw lastError
            } catch (e: Exception) {
                handleSyncError(e)
                throw e
            }
        }

        if (lastError != null) {
            handleSyncError(lastError)
            throw lastError
        }
    }

    internal fun retryDelay(attempt: Int, serverRetryAfter: Double?): Double {
        if (serverRetryAfter != null && serverRetryAfter > 0) {
            return serverRetryAfter
        }
        // Exponential backoff: 1s, 2s, 4s, 8s, 16s cap
        val base = min(2.0.pow(attempt - 1), 16.0)
        // Add jitter: 0-50% of base
        val jitter = Math.random() * base * 0.5
        return base + jitter
    }

    private suspend fun runSyncCycle() {
        updateStatus(SyncStatus.Syncing)

        // 1. Push first
        runPush()

        // 2. Pull loop
        runPullLoop()

        updateStatus(SyncStatus.Idle)
    }

    // MARK: - Push

    private suspend fun runPush() {
        var hasMore = true
        while (hasMore) {
            val outcome = pushProcessor.processPush(
                httpClient = httpClient,
                clientID = clientID,
                schemaVersion = schemaVersion,
                schemaHash = schemaHash,
                syncedTables = syncedTables,
                batchSize = config.pushBatchSize
            )

            if (outcome != null) {
                for (conflict in outcome.conflicts) {
                    fireConflict(conflict)
                }
                if (outcome.hasRetryableRejections) {
                    throw RetryableError(
                        underlying = SynchroError.PushRejected(
                            outcome.response.rejected.filter { it.status == PushStatus.REJECTED_RETRYABLE }
                        ),
                        retryAfter = null
                    )
                }
                hasMore = changeTracker.hasPendingChanges()
            } else {
                hasMore = false
            }
        }
    }

    // MARK: - Pull

    private suspend fun runPullLoop() {
        var checkpoint = database.readTransaction { db ->
            SynchroMeta.getInt64(db, MetaKey.CHECKPOINT)
        }
        var knownBuckets = loadKnownBuckets()

        var hasMore = true
        while (hasMore) {
            val request = PullRequest(
                clientID = clientID,
                checkpoint = checkpoint,
                tables = null,
                limit = config.effectivePullPageSize,
                knownBuckets = if (knownBuckets.isEmpty()) null else knownBuckets,
                schemaVersion = schemaVersion,
                schemaHash = schemaHash
            )

            val response = httpClient.pull(request)

            if (response.snapshotRequired == true) {
                rebuildFromSnapshot(requiresApproval = response.snapshotReason != "initial_sync_required")
                return
            }

            pullProcessor.applyPullPage(response.changes, response.deletes, syncedTables)
            pullProcessor.updateCheckpoint(response.checkpoint)
            pullProcessor.updateKnownBuckets(response.bucketUpdates)

            checkpoint = response.checkpoint
            hasMore = response.hasMore

            // Reload known buckets after applying updates for next page
            if (hasMore && response.bucketUpdates != null) {
                knownBuckets = loadKnownBuckets()
            }
        }
    }

    private fun loadKnownBuckets(): List<String> {
        val knownBucketsStr = database.readTransaction { db ->
            SynchroMeta.get(db, MetaKey.KNOWN_BUCKETS) ?: "[]"
        }
        return try {
            Json.decodeFromString<List<String>>(knownBucketsStr)
        } catch (_: Exception) {
            emptyList()
        }
    }

    // MARK: - Snapshot

    private suspend fun rebuildFromSnapshot(requiresApproval: Boolean) {
        if (requiresApproval) {
            runPush()
        }

        var approved = true
        if (requiresApproval) {
            for ((_, callback) in snapshotCallbacks) {
                val result = callback()
                if (!result) {
                    approved = false
                    break
                }
            }
        }

        if (!approved) {
            throw SynchroError.SnapshotRequired()
        }

        val schema = SchemaResponse(
            schemaVersion = schemaVersion,
            schemaHash = schemaHash,
            serverTime = java.time.Instant.now().toString(),
            tables = syncedTables
        )
        database.writeTransaction { db ->
            schemaManager.dropSyncedTablesInTransaction(db, schema)
            changeTracker.clearAllInTransaction(db)
            SynchroMeta.setInt64(db, MetaKey.CHECKPOINT, 0L)
            SynchroMeta.set(db, MetaKey.KNOWN_BUCKETS, "[]")
            SynchroMeta.set(db, MetaKey.SNAPSHOT_COMPLETE, "0")
            schemaManager.createSyncedTablesInTransaction(db, schema)
        }

        var cursor: SnapshotCursor? = null
        var hasMore = true

        while (hasMore) {
            val request = SnapshotRequest(
                clientID = clientID,
                cursor = cursor,
                limit = config.effectiveSnapshotPageSize,
                schemaVersion = schemaVersion,
                schemaHash = schemaHash
            )

            val response = httpClient.snapshot(request)

            pullProcessor.applySnapshotPage(response.records, syncedTables)

            cursor = response.cursor
            hasMore = response.hasMore

            if (!hasMore) {
                database.writeTransaction { db ->
                    SynchroMeta.setInt64(db, MetaKey.CHECKPOINT, response.checkpoint)
                    SynchroMeta.set(db, MetaKey.SNAPSHOT_COMPLETE, "1")
                }
            }
        }
    }

    // MARK: - Debounce

    private fun startPendingObserver() {
        pendingObserver = database.onChange(listOf("_synchro_pending_changes")) {
            scheduleDebouncedPush()
        }
    }

    private fun scheduleDebouncedPush() {
        debounceJob?.cancel()
        debounceJob = scope?.launch {
            try {
                delay((config.pushDebounce * 1000).toLong())
                if (!isActive) return@launch
                runPush()
            } catch (e: CancellationException) {
                throw e
            } catch (_: Exception) {
                // Debounced push failed, will retry on next cycle
            }
        }
    }

    // MARK: - Error Handling

    private fun handleSyncError(error: Exception) {
        if (error is RetryableError) {
            val retryAt = error.retryAfter?.let {
                java.time.Instant.now().plusMillis((it * 1000).toLong())
            }
            updateStatus(SyncStatus.Error(retryAt = retryAt))
        } else {
            updateStatus(SyncStatus.Error(retryAt = null))
        }
    }

    // MARK: - Status

    internal fun updateStatus(status: SyncStatus) {
        currentStatus = status
        for ((_, cb) in statusCallbacks) {
            try {
                cb(status)
            } catch (_: Exception) {
                // User callback must not crash the sync engine
            }
        }
    }

    private fun fireConflict(event: ConflictEvent) {
        for ((_, cb) in conflictCallbacks) {
            try {
                cb(event)
            } catch (_: Exception) {
                // User callback must not crash the sync engine
            }
        }
    }
}

class CallbackCancellable(private var onCancel: (() -> Unit)?) : Cancellable {
    @Synchronized
    override fun cancel() {
        onCancel?.invoke()
        onCancel = null
    }
}
