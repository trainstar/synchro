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
    private var syncedTables: List<LocalSchemaTable> = emptyList()
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

            database.ensureScopeTables()

            val connectResponse = connect()
            val connectSchema = resolveConnectSchema(connectResponse)
            syncedTables = connectSchema.first
            schemaVersion = connectSchema.second.first
            schemaHash = connectSchema.second.second

            applyScopeAssignmentDelta(
                connectResponse.scopes,
                connectResponse.scopeSetVersion
            )

            updateStatus(SyncStatus.Idle)

            rebuildAssignedScopesNeedingCursor()

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
                            outcome.response.rejected.filter { it.status == VNextMutationStatus.REJECTED_RETRYABLE }
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
        var hasMore = true
        var scopeSetVersion = database.readTransaction { db ->
            SynchroMeta.getInt64(db, MetaKey.SCOPE_SET_VERSION)
        }
        val pendingRebuilds = linkedSetOf<String>()

        while (hasMore) {
            val scopes = loadKnownScopes()
            if (scopes.isEmpty()) {
                return
            }

            val request = VNextPullRequest(
                clientID = clientID,
                schema = VNextSchemaRef(version = schemaVersion, hash = schemaHash),
                scopeSetVersion = scopeSetVersion,
                scopes = scopes,
                limit = config.effectivePullPageSize,
                checksumMode = VNextChecksumMode.REQUESTED
            )

            val response = httpClient.pull(request)
            response.validate(request)

            pullProcessor.applyScopeChanges(
                changes = response.changes,
                syncedTables = syncedTables,
                scopeCursors = response.scopeCursors,
                checksums = response.checksums
            )
            applyScopeAssignmentDelta(response.scopeUpdates, response.scopeSetVersion)
            pendingRebuilds.addAll(response.rebuild)
            scopeSetVersion = response.scopeSetVersion
            hasMore = response.hasMore
        }

        pendingRebuilds.addAll(scopeIDsNeedingRebuild())
        val knownScopeIDs = database.readTransaction { db ->
            SynchroMeta.getAllScopes(db).map { it.scopeID }.toSet()
        }

        for (scopeId in pendingRebuilds) {
            if (scopeId in knownScopeIDs) {
                rebuildScope(scopeId)
            }
        }
    }

    // MARK: - Scope Rebuild

    private suspend fun rebuildScope(scopeId: String) {
        val generation = pullProcessor.beginScopeRebuild(scopeId)
        var cursor: String? = null

        while (true) {
            val request = VNextRebuildRequest(
                clientID = clientID,
                scope = scopeId,
                cursor = cursor,
                limit = config.effectivePullPageSize
            )

            val response = httpClient.rebuild(request)
            response.validate()

            pullProcessor.applyScopeRebuildPage(
                scopeId = scopeId,
                generation = generation,
                records = response.records,
                syncedTables = syncedTables
            )

            if (response.hasMore) {
                cursor = response.cursor
                continue
            }

            val finalCursor = response.finalScopeCursor
                ?: throw SynchroError.InvalidResponse("final rebuild page missing final scope cursor for $scopeId")
            val checksum = response.checksum
                ?: throw SynchroError.InvalidResponse("final rebuild page missing checksum for $scopeId")

            pullProcessor.finalizeScopeRebuild(
                scopeId = scopeId,
                generation = generation,
                finalCursor = finalCursor,
                checksum = checksum,
                syncedTables = syncedTables
            )
            return
        }
    }

    // MARK: - Bootstrap

    private suspend fun connect(): VNextConnectResponse {
        val request = VNextConnectRequest(
            clientID = clientID,
            platform = config.platform,
            appVersion = config.appVersion,
            protocolVersion = 1,
            schema = database.readTransaction { db ->
                VNextSchemaRef(
                    version = SynchroMeta.getInt64(db, MetaKey.SCHEMA_VERSION),
                    hash = SynchroMeta.get(db, MetaKey.SCHEMA_HASH) ?: ""
                )
            },
            scopeSetVersion = database.readTransaction { db ->
                SynchroMeta.getInt64(db, MetaKey.SCOPE_SET_VERSION)
            },
            knownScopes = loadKnownScopes()
        )

        val response = httpClient.connect(request)
        response.validate()

        if (!response.schema.action.isCompatible()) {
            throw SynchroError.InvalidResponse("unsupported connect schema action")
        }

        return response
    }

    private suspend fun resolveConnectSchema(response: VNextConnectResponse): Pair<List<LocalSchemaTable>, Pair<Long, String>> {
        return when (response.schema.action) {
            VNextSchemaAction.NONE -> {
                val tables = schemaManager.loadStoredLocalSchema()
                    ?: throw SynchroError.InvalidResponse("connect returned schema action none without stored local schema")
                database.writeTransaction { db ->
                    SynchroMeta.setInt64(db, MetaKey.SCHEMA_VERSION, response.schema.version)
                    SynchroMeta.set(db, MetaKey.SCHEMA_HASH, response.schema.hash)
                }
                tables to (response.schema.version to response.schema.hash)
            }
            VNextSchemaAction.FETCH -> {
                val schema = schemaManager.ensureSchema(httpClient)
                schema.tables.map { it.localSchema } to (schema.schemaVersion to schema.schemaHash)
            }
            VNextSchemaAction.REPLACE, VNextSchemaAction.REBUILD_LOCAL -> {
                val manifest = response.schemaDefinition
                    ?: throw SynchroError.InvalidResponse("connect schema action ${response.schema.action} missing schema_definition")
                val tables = manifest.localTables()
                schemaManager.reconcileLocalSchema(
                    schemaVersion = response.schema.version,
                    schemaHash = response.schema.hash,
                    tables = tables
                )
                tables to (response.schema.version to response.schema.hash)
            }
            VNextSchemaAction.UNSUPPORTED -> {
                throw SynchroError.InvalidResponse("unsupported connect schema action")
            }
        }
    }

    private fun loadKnownScopes(): Map<String, VNextScopeCursorRef> {
        return database.readTransaction { db ->
            SynchroMeta.getAllScopes(db).associate { scope ->
                scope.scopeID to VNextScopeCursorRef(cursor = scope.cursor)
            }
        }
    }

    private fun applyScopeAssignmentDelta(delta: VNextScopeAssignmentDelta, scopeSetVersion: Long) {
        for (scopeId in delta.remove) {
            pullProcessor.removeScope(scopeId, syncedTables)
        }

        database.writeTransaction { db ->
            for (scope in delta.add) {
                SynchroMeta.upsertScope(
                    db,
                    scopeId = scope.id,
                    cursor = scope.cursor,
                    checksum = null
                )
            }
            SynchroMeta.setInt64(db, MetaKey.SCOPE_SET_VERSION, scopeSetVersion)
        }
    }

    private fun scopeIDsNeedingRebuild(): Set<String> {
        return database.readTransaction { db ->
            SynchroMeta.getAllScopes(db)
                .filter { it.cursor == null }
                .map { it.scopeID }
                .toSet()
        }
    }

    private suspend fun rebuildAssignedScopesNeedingCursor() {
        for (scopeId in scopeIDsNeedingRebuild()) {
            rebuildScope(scopeId)
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
