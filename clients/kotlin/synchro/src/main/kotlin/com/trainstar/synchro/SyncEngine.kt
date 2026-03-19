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

            // Ensure bucket tables exist for databases created before bucket support
            database.ensureBucketTables()

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

            val needsInitialSync = database.writeTransaction { db ->
                SynchroMeta.set(db, MetaKey.CLIENT_SERVER_ID, registerResp.id)

                // Store per-bucket checkpoints from registration if provided
                registerResp.bucketCheckpoints?.forEach { (bucketId, cp) ->
                    SynchroMeta.setBucketCheckpoint(db, bucketId, cp)
                }

                val checkpoint = SynchroMeta.getInt64(db, MetaKey.CHECKPOINT)
                val initialSyncComplete = SynchroMeta.get(db, MetaKey.SNAPSHOT_COMPLETE) == "1"
                checkpoint == 0L || !initialSyncComplete
            }

            updateStatus(SyncStatus.Idle)

            if (needsInitialSync) {
                val buckets = registerResp.bucketCheckpoints?.keys?.toList() ?: emptyList()
                initialSync(buckets)
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
        var bucketCheckpoints = loadBucketCheckpoints()
        var pendingRebuilds: List<String> = emptyList()
        var lastBucketChecksums: Map<String, Int>? = null

        var hasMore = true
        while (hasMore) {
            val request = PullRequest(
                clientID = clientID,
                checkpoint = checkpoint,
                tables = null,
                limit = config.effectivePullPageSize,
                knownBuckets = if (knownBuckets.isEmpty()) null else knownBuckets,
                bucketCheckpoints = if (bucketCheckpoints.isEmpty()) null else bucketCheckpoints,
                schemaVersion = schemaVersion,
                schemaHash = schemaHash
            )

            val response = httpClient.pull(request)

            pullProcessor.applyPullPage(response.changes, response.deletes, syncedTables)
            pullProcessor.trackBucketMembership(response.changes)
            pullProcessor.updateCheckpoint(response.checkpoint)
            pullProcessor.updateKnownBuckets(response.bucketUpdates)
            pullProcessor.updateBucketCheckpoints(response.bucketCheckpoints)

            // Accumulate rebuild requests across pages
            response.rebuildBuckets?.let { buckets ->
                pendingRebuilds = (pendingRebuilds + buckets).distinct()
            }

            // Capture bucket checksums from the final page for verification.
            response.bucketChecksums?.let { serverCS ->
                lastBucketChecksums = serverCS
            }

            checkpoint = response.checkpoint
            hasMore = response.hasMore

            // Reload known buckets after applying updates for next page
            if (hasMore && response.bucketUpdates != null) {
                knownBuckets = loadKnownBuckets()
            }
            if (hasMore && response.bucketCheckpoints != null) {
                bucketCheckpoints = loadBucketCheckpoints()
            }
        }

        // Checksum verification: compare server bucket checksums with local.
        // Mismatch triggers a rebuild for that bucket. Runs inline with no
        // extra HTTP call.
        lastBucketChecksums?.forEach { (bucketId, serverCS) ->
            try {
                val localCS = pullProcessor.computeBucketChecksum(bucketId)
                if (localCS != serverCS && bucketId !in pendingRebuilds) {
                    android.util.Log.w("SyncEngine", "Checksum mismatch for bucket $bucketId: server=$serverCS local=$localCS, triggering rebuild")
                    pendingRebuilds = pendingRebuilds + bucketId
                }
            } catch (e: Exception) {
                android.util.Log.e("SyncEngine", "Failed to compute bucket checksum for $bucketId, triggering rebuild", e)
                if (bucketId !in pendingRebuilds) {
                    pendingRebuilds = pendingRebuilds + bucketId
                }
            }
        }

        // Process all pending bucket rebuilds after the pull loop finishes.
        for (bucketId in pendingRebuilds) {
            rebuildBucket(bucketId)
        }
    }

    // MARK: - Bucket Rebuild

    /**
     * Rebuilds a single bucket by clearing its local membership, fetching all
     * records from the server via paginated POST /sync/rebuild calls, upserting
     * each record, and updating the bucket checkpoint.
     */
    private suspend fun rebuildBucket(bucketId: String) {
        // Save old members so we can detect orphans after rebuild
        val oldMembers = database.readTransaction { db ->
            SynchroMeta.getBucketMembers(db, bucketId)
        }

        // Clear existing membership for this bucket before rebuilding
        pullProcessor.clearBucketMembers(bucketId)

        var cursor: String? = null
        var hasMore = true
        var rebuiltCheckpoint = 0L

        while (hasMore) {
            val request = RebuildRequest(
                clientID = clientID,
                bucketId = bucketId,
                cursor = cursor,
                limit = config.effectivePullPageSize,
                schemaVersion = schemaVersion,
                schemaHash = schemaHash
            )

            val response = httpClient.rebuild(request)

            // Apply records and track bucket membership
            pullProcessor.applyChanges(response.records, syncedTables)
            pullProcessor.trackBucketMembership(response.records, overrideBucketId = bucketId)

            cursor = response.cursor
            hasMore = response.hasMore
            rebuiltCheckpoint = response.checkpoint
        }

        // Soft-delete records that were in the old bucket but not in the rebuild
        val newMembers = database.readTransaction { db ->
            SynchroMeta.getBucketMembers(db, bucketId)
        }
        val newMemberSet = newMembers.map { "${it.first}|${it.second}" }.toSet()
        val orphanedMembers = oldMembers.filter { "${it.first}|${it.second}" !in newMemberSet }

        if (orphanedMembers.isNotEmpty()) {
            val tableMap = syncedTables.associateBy { it.tableName }
            database.writeTransaction { db ->
                for ((tableName, recordId) in orphanedMembers) {
                    val schema = tableMap[tableName] ?: continue
                    val pkCol = schema.primaryKey.firstOrNull() ?: "id"
                    val quoted = SQLiteHelpers.quoteIdentifier(tableName)
                    val quotedPK = SQLiteHelpers.quoteIdentifier(pkCol)
                    val quotedDeletedAt = SQLiteHelpers.quoteIdentifier(schema.deletedAtColumn)

                    val stmt = db.compileStatement(
                        "UPDATE $quoted SET $quotedDeletedAt = ${SQLiteHelpers.timestampNow()} WHERE $quotedPK = ? AND $quotedDeletedAt IS NULL"
                    )
                    try {
                        stmt.bindString(1, recordId)
                        stmt.executeUpdateDelete()
                    } finally {
                        stmt.close()
                    }
                }
            }
        }

        // Update the bucket checkpoint
        pullProcessor.updateBucketCheckpoint(bucketId, rebuiltCheckpoint)
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

    private fun loadBucketCheckpoints(): Map<String, Long> {
        return database.readTransaction { db ->
            SynchroMeta.getAllBucketCheckpoints(db)
        }
    }

    // MARK: - Initial Sync

    private suspend fun initialSync(buckets: List<String>) {
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
            SynchroMeta.clearAllBucketCheckpoints(db)
            db.execSQL("DELETE FROM _synchro_bucket_members")
            schemaManager.createSyncedTablesInTransaction(db, schema)
        }

        for (bucketId in buckets) {
            rebuildBucket(bucketId)
        }

        database.writeTransaction { db ->
            SynchroMeta.set(db, MetaKey.SNAPSHOT_COMPLETE, "1")
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
