package com.trainstar.synchro

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.min
import kotlin.math.pow

data class SyncOptions(
    val initialSyncCompleted: (() -> Unit)? = null
)

internal class SyncEngine(
    private val config: SynchroConfig,
    private val database: SynchroDatabase,
    private val httpClient: HttpClient,
    private val schemaManager: SchemaManager,
    private val changeTracker: ChangeTracker,
    private val pullProcessor: PullProcessor,
    private val pushProcessor: PushProcessor
) {
    private data class ConnectExecution(
        val response: ConnectResponse,
        val requestJSON: String,
        val schemaReset: Boolean,
    )

    private data class LocalConnectState(
        val schema: SchemaRef,
        val clientGeneration: Long,
        val scopeSetVersion: Long,
        val knownScopes: Map<String, ScopeCursorRef>,
        val seedReceipts: Map<String, String>,
    )

    private var scope: CoroutineScope? = null
    private var syncJob: Job? = null
    private var debounceJob: Job? = null
    private var pendingObserver: Cancellable? = null
    private val ownedCycleJobs = linkedSetOf<Job>()

    @Volatile
    private var currentStatus: SyncStatus = SyncStatus.Uninitialized
    private val statusCallbacks = ConcurrentHashMap<String, (SyncStatus) -> Unit>()
    private val conflictCallbacks = ConcurrentHashMap<String, (ConflictEvent) -> Unit>()
    private val eventCallbacks = ConcurrentHashMap<String, (SyncEvent) -> Unit>()

    @Volatile
    private var syncedTables: List<LocalSchemaTable> = emptyList()
    @Volatile
    private var schemaVersion: Long = 0
    @Volatile
    private var schemaHash: String = ""
    @Volatile
    private var clientGeneration: Long = 0
    @Volatile
    private var connectionReady: Boolean = false
    private val clientID: String get() = config.clientID
    private val retryJSON = Json { ignoreUnknownKeys = false }
    private val started = AtomicReference(false)
    private val cycleMutex = Mutex()
    internal var retryTiming: RetryTiming = SystemRetryTiming
    private val lifecycleLock = Any()
    private var shutdownRequested = false
    private var shutdownInProgress = false
    private var explicitStopRequested = false
    private var appInForeground = true
    private var activeOperations = 0
    private var operationsDrained = CompletableDeferred<Unit>().apply { complete(Unit) }
    private var stoppedPublished = CompletableDeferred<Unit>().apply { complete(Unit) }
    private var lifecycleGeneration = 0L

    init {
        val durable = database.readTransaction { db -> SynchroMeta.getClientState(db) }
        currentStatus = when {
            durable.failure != null && !durable.errorAcknowledged -> SyncStatus.Error(durable.failure)
            durable.lifecycleState == SyncLifecycleState.STOPPED -> SyncStatus.Stopped
            else -> SyncStatus.Uninitialized
        }
    }

    // MARK: - Lifecycle

    suspend fun start(options: SyncOptions? = null) {
        database.requireOutsideApplicationTransaction("start")
        startInternal(options, schemaReset = false, recoveringError = false)
    }

    /** Explicitly acknowledges a remediated non-schema blocking error. */
    suspend fun retry(options: SyncOptions? = null) {
        database.requireOutsideApplicationTransaction("retry")
        database.writeTransaction { db ->
            val state = SynchroMeta.getClientState(db)
            if (state.failure?.recoveryAction == SyncRecoveryAction.SCHEMA_RESET) {
                throw SynchroError.InvalidResponse("schema reset is required before retry")
            }
            SynchroMeta.acknowledgeBlockingError(db)
        }
        startInternal(options, schemaReset = false, recoveringError = true)
    }

    /** Requests the contract-defined incompatible-schema recovery. */
    suspend fun resetSchema(options: SyncOptions? = null) {
        database.requireOutsideApplicationTransaction("resetSchema")
        database.writeTransaction { db ->
            val state = SynchroMeta.getClientState(db)
            if (state.failure?.recoveryAction != SyncRecoveryAction.SCHEMA_RESET) {
                throw SynchroError.InvalidResponse("schema reset requires a schema-reset recovery state")
            }
            SynchroMeta.acknowledgeBlockingError(db)
        }
        startInternal(options, schemaReset = true, recoveringError = true)
    }

    private suspend fun startInternal(
        options: SyncOptions?,
        schemaReset: Boolean,
        recoveringError: Boolean,
    ) {
        val generation: Long
        synchronized(lifecycleLock) {
            if (shutdownRequested || shutdownInProgress) {
                throw SynchroError.NotStarted()
            }
            if (!started.compareAndSet(false, true)) {
                throw SynchroError.AlreadyStarted()
            }
            connectionReady = false
            explicitStopRequested = false
            generation = ++lifecycleGeneration
            beginOperationLocked()
        }

        try {
            if (!recoveringError) {
                database.readTransaction { db ->
                    val durable = SynchroMeta.getClientState(db)
                    durable.failure?.let { failure ->
                        throw SynchroError.BlockingFailure(failure)
                    }
                    if (durable.lifecycleState == SyncLifecycleState.ERROR) {
                        throw SynchroError.InvalidResponse("durable error state has no typed failure")
                    }
                }
            }
            // A pending typed journal is recovered before the first network request.
            schemaManager.recoverPendingMigration()
            val localReadyAlreadyPersisted = database.readTransaction { db ->
                SynchroMeta.getClientState(db).lifecycleState == SyncLifecycleState.LOCAL_READY
            }
            transitionTo(
                SyncStatus.LocalReady,
                processRecovery = currentStatus.state == SyncLifecycleState.UNINITIALIZED,
                persistedStateAlreadyApplied = localReadyAlreadyPersisted,
            )
            bindClientIdentityBeforeConnect()

            // Create a fresh scope
            val createdScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

            // Clear any stale sync lock from a previous crash.
            // If the process was killed while sync_lock was set, CDC triggers
            // would be permanently disabled, silently dropping all local changes.
            database.writeTransaction { db ->
                SynchroMeta.setSyncLock(db, false)
            }

            val startupGate = CompletableDeferred<Unit>()
            synchronized(lifecycleLock) {
                if (shutdownRequested || shutdownInProgress ||
                    !started.get() || lifecycleGeneration != generation
                ) {
                    createdScope.cancel()
                    throw CancellationException("sync engine lifecycle changed")
                }
                scope = createdScope
                syncJob = createdScope.launch {
                    runManagedLoop(startupGate, options, generation, schemaReset)
                }
            }
            startupGate.await()
        } catch (e: Exception) {
            if (e !is CancellationException && e !is SynchroError.BlockingFailure) {
                recordBlockingFailure(e)
            }
            teardownAfterFailedStart(generation)
            throw e
        } finally {
            endOperation()
        }
    }

    /** Cancels lifecycle-owned work and does not return before it drains. */
    suspend fun stop() {
        database.requireOutsideApplicationTransaction("stop")
        stopInternal(permanent = false)
    }

    /** Cancels managed work and waits until every managed job has completed. */
    suspend fun shutdown() {
        database.requireOutsideApplicationTransaction("shutdown")
        stopInternal(permanent = true)
    }

    private suspend fun stopInternal(permanent: Boolean) {
        val jobs: List<Job>
        val drained: CompletableDeferred<Unit>?
        val publication: CompletableDeferred<Unit>
        val publishStopped: Boolean
        synchronized(lifecycleLock) {
            if (permanent) shutdownRequested = true
            if (shutdownInProgress) {
                jobs = emptyList()
                drained = if (activeOperations == 0) null else operationsDrained
                publication = stoppedPublished
                publishStopped = false
            } else if (currentStatus.state == SyncLifecycleState.STOPPED && !started.get()) {
                jobs = emptyList()
                drained = null
                publication = stoppedPublished
                publishStopped = false
            } else {
                shutdownInProgress = true
                explicitStopRequested = true
                jobs = (listOfNotNull(syncJob, debounceJob) + ownedCycleJobs).distinct()
                drained = if (activeOperations == 0) null else operationsDrained
                publication = CompletableDeferred()
                stoppedPublished = publication
                cancelLifecycleLocked()
                publishStopped = true
            }
        }
        jobs.forEach(Job::cancel)
        drained?.await()
        jobs.forEach { job ->
            if (job != currentCoroutineContext()[Job]) job.join()
        }
        if (!publishStopped) {
            publication.await()
            if (currentStatus is SyncStatus.Stopped) publishStatusSnapshot(SyncStatus.Stopped)
            return
        }
        // Keep the stop barrier set until observers have received stopped.
        try {
            transitionTo(SyncStatus.Stopped)
            publication.complete(Unit)
        } catch (error: Throwable) {
            publication.completeExceptionally(error)
            throw error
        } finally {
            synchronized(lifecycleLock) {
                if (!permanent && !shutdownRequested) shutdownInProgress = false
            }
        }
    }

    private fun cancelLifecycleLocked() {
        syncJob?.cancel()
        syncJob = null
        debounceJob?.cancel()
        debounceJob = null
        pendingObserver?.cancel()
        pendingObserver = null
        scope?.cancel()
        scope = null
        ownedCycleJobs.clear()
        connectionReady = false
        started.set(false)
    }

    private fun beginOperationLocked(): Boolean {
        if (shutdownRequested || shutdownInProgress || !started.get()) return false
        if (activeOperations == 0) {
            operationsDrained = CompletableDeferred()
        }
        activeOperations++
        return true
    }

    private fun beginOperation(): Boolean = synchronized(lifecycleLock) {
        beginOperationLocked()
    }

    private fun endOperation() {
        synchronized(lifecycleLock) {
            check(activeOperations > 0) { "sync operation tracking underflow" }
            activeOperations--
            if (activeOperations == 0) {
                operationsDrained.complete(Unit)
            }
        }
    }

    /**
     * The engine owns the scheduled cycle. Cancelling this caller only detaches
     * its waiter. It does not cancel the durable sync operation.
     */
    suspend fun syncNow() {
        database.requireOutsideApplicationTransaction("syncNow")
        val job = scheduleEngineOwnedCycle()
        job.await()
    }

    private fun scheduleEngineOwnedCycle(): Deferred<Unit> {
        synchronized(lifecycleLock) {
            if (shutdownRequested || shutdownInProgress) {
                throw SynchroError.NotStarted()
            }
            if (!connectionReady || !started.get()) {
                throw SynchroError.NotConnected()
            }
            val owner = scope ?: throw SynchroError.NotStarted()
            if (!beginOperationLocked()) throw SynchroError.NotStarted()
            val job = owner.async {
                try {
                    runSyncCycleWithRetry()
                } finally {
                    endOperation()
                }
            }
            ownedCycleJobs += job
            job.invokeOnCompletion {
                synchronized(lifecycleLock) { ownedCycleJobs.remove(job) }
            }
            return job
        }
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

    fun onEvent(callback: (SyncEvent) -> Unit): Cancellable {
        val id = UUID.randomUUID().toString()
        eventCallbacks[id] = callback
        return CallbackCancellable { eventCallbacks.remove(id) }
    }

    /** Native activity state controls automatic work without a bridge callback. */
    fun onApplicationForeground() {
        database.requireOutsideApplicationTransaction("onApplicationForeground")
        val immediateCycle: Deferred<Unit>? = synchronized(lifecycleLock) {
            appInForeground = true
            if (explicitStopRequested || shutdownRequested || shutdownInProgress ||
                !started.get() || !connectionReady
            ) {
                null
            } else {
                runCatching { scheduleEngineOwnedCycle() }.getOrNull()
            }
        }
        immediateCycle?.invokeOnCompletion { /* Engine-owned work retains all failure handling. */ }
    }

    /** Backgrounding prevents new automatic cycles. Durable work remains intact. */
    fun onApplicationBackground() {
        database.requireOutsideApplicationTransaction("onApplicationBackground")
        synchronized(lifecycleLock) {
            appInForeground = false
            debounceJob?.cancel()
            debounceJob = null
        }
    }

    // MARK: - Sync Loop

    private suspend fun syncLoop() {
        while (currentCoroutineContext().isActive) {
            delay((config.syncInterval * 1000).toLong())
            if (!currentCoroutineContext().isActive) return
            if (!isApplicationForeground()) continue
            try {
                runSyncCycleWithRetry()
            } catch (e: CancellationException) {
                throw e
            } catch (_: Exception) {
                Unit
            }
        }
    }

    private suspend fun runManagedLoop(
        startupGate: CompletableDeferred<Unit>,
        options: SyncOptions?,
        generation: Long,
        schemaReset: Boolean,
    ) {
        if (!beginOperation()) {
            startupGate.complete(Unit)
            return
        }
        try {
            val startupCompleted = runStartupSequence(startupGate, options, generation, schemaReset)
            if (!startupCompleted) return
            syncLoop()
        } finally {
            endOperation()
        }
    }

    private suspend fun runStartupSequence(
        startupGate: CompletableDeferred<Unit>,
        options: SyncOptions?,
        generation: Long,
        schemaReset: Boolean,
    ): Boolean {
        var gateResolved = false
        var connected = false
        var recoveryChecked = false
        var replayConnectBackoff: DurableBackoffRecord? = null
        var replayCycleBackoff: DurableBackoffRecord? = null

        while (currentCoroutineContext().isActive) {
            try {
                if (!recoveryChecked) {
                    recoveryChecked = true
                    DurableBackoffStore.load(database)?.let { recovered ->
                        if (!gateResolved && recovered.nextRetryAtMs > retryTiming.currentTimeMillis()) {
                            startupGate.complete(Unit)
                            gateResolved = true
                        }
                        awaitBackoffDeadline(recovered)
                        if (recovered.resumeState == RetryOperation.CONNECTING) {
                            replayConnectBackoff = recovered
                        } else {
                            replayCycleBackoff = recovered
                        }
                    }
                }
                if (!connected) {
                    val storedConnectBackoff = replayConnectBackoff
                    val connectExecution = if (storedConnectBackoff != null) {
                        reconnectUsingBackoff(storedConnectBackoff)
                    } else {
                        connect(schemaReset)
                    }
                    replayConnectBackoff = null
                    installConnectResponse(
                        connectExecution.response,
                        connectExecution.requestJSON,
                        schemaReset = connectExecution.schemaReset,
                    )
                    pushProcessor.renewRequiredBatches(
                        clientID,
                        clientGeneration,
                        schemaVersion,
                        schemaHash,
                        syncedTables,
                    )
                    connected = true

                }

                runSyncCycleWithRetry(replayCycleBackoff, recoverStoredBackoff = false)
                replayCycleBackoff = null
                startPendingObserver()

                if (!gateResolved) {
                    startupGate.complete(Unit)
                }
                options?.initialSyncCompleted?.invoke()
                return true
            } catch (e: CancellationException) {
                if (!gateResolved) {
                    startupGate.complete(Unit)
                }
                throw e
            } catch (e: RetryableError) {
                val backoff = e.persistedBackoff ?: enterBackoff(e)
                if (!gateResolved) {
                    startupGate.complete(Unit)
                    gateResolved = true
                }
                awaitBackoffDeadline(backoff)
                if (backoff.resumeState == RetryOperation.CONNECTING) {
                    connected = false
                    replayConnectBackoff = backoff
                } else {
                    replayCycleBackoff = backoff
                }
            } catch (e: Exception) {
                handleSyncError(e)
                if (gateResolved) {
                    finishStartupFailure(generation)
                } else {
                    startupGate.completeExceptionally(e)
                }
                return false
            }
        }

        if (!gateResolved) {
            startupGate.complete(Unit)
        }
        return false
    }

    // MARK: - Retry

    private suspend fun runSyncCycleWithRetry(
        initialBackoff: DurableBackoffRecord? = null,
        recoverStoredBackoff: Boolean = true,
    ) {
        cycleMutex.withLock {
            val storedBackoff = DurableBackoffStore.load(database)
            val backoff = when {
                initialBackoff != null -> storedBackoff
                recoverStoredBackoff -> storedBackoff
                else -> null
            }
            if (backoff != null && backoff != initialBackoff) {
                awaitBackoffDeadline(backoff)
                ensureLifecycleActive()
            }
            runSingleSyncCycleWithRetry(backoff)
        }
    }

    private suspend fun runSingleSyncCycleWithRetry(initialBackoff: DurableBackoffRecord? = null) {
        var attempt = 0
        var lastError: Exception? = null
        var backoff = initialBackoff

        while (attempt <= config.maxRetryAttempts) {
            try {
                try {
                    if (backoff != null) {
                        resumeDurableWork(backoff)
                    } else {
                        runSyncCycle()
                    }
                } catch (_: ClientGenerationExpiredException) {
                    reconnectAfterBindingRenewal(backoff)
                    backoff = null
                    continue
                } catch (_: SynchroError.SchemaMismatch) {
                    reconnectAfterBindingRenewal(backoff)
                    backoff = null
                    continue
                }
                return
            } catch (e: CancellationException) {
                throw e
            } catch (e: RetryableError) {
                attempt++
                lastError = e
                val persistedBackoff = enterBackoff(e)
                if (attempt > config.maxRetryAttempts) break
                awaitBackoffDeadline(persistedBackoff)
                ensureLifecycleActive()
                backoff = persistedBackoff
            } catch (e: Exception) {
                handleSyncError(e)
                throw e
            }
        }

        if (lastError != null) {
            throw lastError
        }
    }

    internal fun retryDelay(attempt: Int): Double {
        // Exponential backoff: 1s, 2s, 4s, 8s, 16s cap
        val base = min(2.0.pow((attempt - 1).coerceAtLeast(0)), 16.0)
        // Add jitter: 0-50% of base
        val jitter = retryTiming.jitterFraction().coerceIn(0.0, 1.0) * base * 0.5
        return base + jitter
    }

    private inline fun <reified T> decodeBackoffRequest(requestJSON: String): T {
        return try {
            retryJSON.decodeFromString(requestJSON)
        } catch (_: Exception) {
            throw SynchroError.InvalidResponse("durable retry request identity is invalid")
        }
    }

    private suspend fun reconnectUsingBackoff(backoff: DurableBackoffRecord): ConnectExecution {
        if (backoff.resumeState != RetryOperation.CONNECTING) {
            throw SynchroError.InvalidResponse("durable retry does not identify a connect request")
        }
        transitionTo(SyncStatus.Connecting)
        pullProcessor.reconcileSeedReceiptsBeforeConnect()
        val request = decodeBackoffRequest<ConnectRequest>(backoff.workIdentity)
        if (request.clientID != clientID) {
            throw SynchroError.InvalidResponse("durable connect retry identity targets another client")
        }
        val currentSeedReceipts = database.readTransaction { db -> SynchroMeta.getSeedReceipts(db) }
        if (request.seedReceipts.orEmpty() != currentSeedReceipts) {
            database.writeTransaction { db ->
                DurableBackoffStore.clearMatching(db, RetryOperation.CONNECTING, backoff.workIdentity)
            }
            return connect(schemaReset = request.schemaReset == true)
        }
        val response = httpClient.connectExact(request, backoff.workIdentity)
        response.validate(
            request.knownScopes,
            request.scopeSetVersion,
            request.seedReceipts?.keys.orEmpty(),
        )
        return ConnectExecution(response, backoff.workIdentity, schemaReset = request.schemaReset == true)
    }

    private suspend fun resumeDurableWork(backoff: DurableBackoffRecord) {
        when (backoff.resumeState) {
            RetryOperation.CONNECTING -> {
                val execution = reconnectUsingBackoff(backoff)
                installConnectResponse(
                    execution.response,
                    execution.requestJSON,
                    schemaReset = execution.schemaReset,
                )
                pushProcessor.renewRequiredBatches(
                    clientID,
                    clientGeneration,
                    schemaVersion,
                    schemaHash,
                    syncedTables,
                )
                runSyncCycle()
            }
            RetryOperation.PUSHING -> {
                transitionTo(SyncStatus.Pushing)
                runPush(expectedBatchID = backoff.workIdentity)
                transitionTo(SyncStatus.Ready)
                runSyncCycle()
            }
            RetryOperation.PULLING -> {
                transitionTo(SyncStatus.Pulling)
                val rebuilds = runPullLoop(replayRequestJSON = backoff.workIdentity)
                completeRequestedRebuilds(rebuilds)
                transitionTo(SyncStatus.Ready)
            }
            RetryOperation.REBUILDING -> {
                val request = decodeBackoffRequest<RebuildRequest>(backoff.workIdentity)
                transitionTo(SyncStatus.Rebuilding)
                rebuildScope(request.scope, replayRequestJSON = backoff.workIdentity)
                schemaManager.completeMigrationIfReady()
                transitionTo(SyncStatus.Ready)
            }
            else -> throw SynchroError.InvalidResponse("durable backoff resume state is invalid")
        }
    }

    private suspend fun runSyncCycle() {
        while (currentCoroutineContext().isActive) {
            if (changeTracker.hasPendingChanges()) {
                transitionTo(SyncStatus.Pushing)
                runPush()
                if (changeTracker.hasPendingChanges()) {
                    transitionTo(SyncStatus.Pushing)
                    continue
                }
                transitionTo(SyncStatus.Ready)
                continue
            }

            val rebuilds = scopeIDsNeedingRebuild()
            if (rebuilds.isNotEmpty()) {
                transitionTo(SyncStatus.Rebuilding)
                rebuilds.forEach { scopeID ->
                    rebuildScope(scopeID)
                }
                schemaManager.completeMigrationIfReady()
                transitionTo(SyncStatus.Ready)
                continue
            }

            if (loadKnownScopes().isEmpty()) {
                schemaManager.completeMigrationIfReady()
                return
            }

            transitionTo(SyncStatus.Pulling)
            val requestedRebuilds = runPullLoop()
            if (requestedRebuilds.isNotEmpty()) {
                completeRequestedRebuilds(requestedRebuilds)
                transitionTo(SyncStatus.Ready)
                return
            }
            schemaManager.completeMigrationIfReady()
            transitionTo(SyncStatus.Ready)
            return
        }
    }

    // MARK: - Push

    private suspend fun runPush(expectedBatchID: String? = null) {
        var nextExpectedBatchID = expectedBatchID
        if (pushProcessor.hasRenewalRequiredBatches()) {
            reconnectAndRenewPushBatches()
            nextExpectedBatchID = null
        }
        var hasMore = true
        while (hasMore) {
            val outcome = try {
                pushProcessor.processPush(
                    httpClient = httpClient,
                    clientID = clientID,
                    clientGeneration = clientGeneration,
                    schemaVersion = schemaVersion,
                    schemaHash = schemaHash,
                    syncedTables = syncedTables,
                    batchSize = config.pushBatchSize,
                    expectedBatchID = nextExpectedBatchID,
                )
            } catch (_: PushRenewalRequiredException) {
                reconnectAndRenewPushBatches()
                nextExpectedBatchID = null
                continue
            }

            if (outcome != null) {
                for (conflict in outcome.conflicts) {
                    fireConflict(conflict)
                }
                outcome.response.accepted.forEach { accepted ->
                    fireEvent(
                        SyncEvent.MutationAccepted(
                            SyncMutationEvent(
                                mutationID = accepted.mutationID,
                                tableID = accepted.table,
                                status = accepted.status,
                                rejectionCode = null,
                            ),
                        ),
                    )
                }
                outcome.response.rejected.forEach { rejected ->
                    fireEvent(
                        SyncEvent.MutationRejected(
                            SyncMutationEvent(
                                mutationID = rejected.mutationID,
                                tableID = rejected.table,
                                status = rejected.status,
                                rejectionCode = rejected.code,
                            ),
                        ),
                    )
                }
                hasMore = changeTracker.hasPendingChanges()
                nextExpectedBatchID = null
                if (hasMore) transitionTo(SyncStatus.Pushing)
            } else {
                hasMore = false
            }
        }
    }

    private suspend fun reconnectAndRenewPushBatches() {
        val connectExecution = connect()
        installConnectResponse(
            connectExecution.response,
            connectExecution.requestJSON,
            schemaReset = connectExecution.schemaReset,
        )
        if (!pushProcessor.renewRequiredBatches(
                clientID,
                clientGeneration,
                schemaVersion,
                schemaHash,
                syncedTables,
            )
        ) {
            throw SynchroError.InvalidResponse("push renewal lost its required batch")
        }
        transitionTo(SyncStatus.Pushing)
    }

    private suspend fun reconnectAfterBindingRenewal(obsoleteBackoff: DurableBackoffRecord?) {
        val connectExecution = connect()
        installConnectResponse(
            connectExecution.response,
            connectExecution.requestJSON,
            obsoleteBackoff = obsoleteBackoff,
            schemaReset = connectExecution.schemaReset,
        )
    }

    // MARK: - Pull

    private suspend fun runPullLoop(replayRequestJSON: String? = null): Set<String> {
        var hasMore = true
        var nextReplayRequestJSON = replayRequestJSON
        var scopeSetVersion = database.readTransaction { db ->
            SynchroMeta.getInt64(db, MetaKey.SCOPE_SET_VERSION)
        }
        val pendingRebuilds = linkedSetOf<String>()

        while (hasMore) {
            val scopes = loadKnownScopes()
            if (scopes.isEmpty()) {
                return emptySet()
            }

            val request = if (nextReplayRequestJSON != null) {
                decodeBackoffRequest<PullRequest>(nextReplayRequestJSON).also { replay ->
                    if (replay.clientID != clientID ||
                        replay.clientGeneration != clientGeneration ||
                        replay.schema != SchemaRef(version = schemaVersion, hash = schemaHash) ||
                        replay.scopeSetVersion != scopeSetVersion ||
                        replay.scopes != scopes ||
                        replay.limit != config.effectivePullPageSize
                    ) {
                        throw SynchroError.InvalidResponse("durable pull retry identity does not match local state")
                    }
                }
            } else {
                PullRequest(
                    clientID = clientID,
                    clientGeneration = clientGeneration,
                    schema = SchemaRef(version = schemaVersion, hash = schemaHash),
                    scopeSetVersion = scopeSetVersion,
                    scopes = scopes,
                    limit = config.effectivePullPageSize
                )
            }

            val requestJSON = nextReplayRequestJSON ?: httpClient.pullRequestJSON(request)
            val response = httpClient.pullExact(request, requestJSON)
            response.validate(scopes.keys, request.scopeSetVersion)

            pullProcessor.applyScopeChangesResolvingBackoff(
                changes = response.changes,
                syncedTables = syncedTables,
                scopeCursors = response.scopeCursors,
                checksums = response.checksums,
                schemaHash = schemaHash,
                scopeUpdates = response.scopeUpdates,
                scopeSetVersion = response.scopeSetVersion,
                rebuildScopes = response.rebuild.toSet(),
                requestJSON = requestJSON,
            )
            pendingRebuilds.addAll(response.rebuild)
            scopeSetVersion = response.scopeSetVersion
            hasMore = response.hasMore
            nextReplayRequestJSON = null
            if (hasMore) transitionTo(SyncStatus.Pulling)
        }

        pendingRebuilds.addAll(scopeIDsNeedingRebuild())
        val knownScopeIDs = database.readTransaction { db ->
            SynchroMeta.getAllScopes(db).map { it.scopeID }.toSet()
        }

        return pendingRebuilds.filterTo(linkedSetOf()) { it in knownScopeIDs }
    }

    private suspend fun completeRequestedRebuilds(scopeIDs: Set<String>) {
        if (scopeIDs.isEmpty()) return
        transitionTo(SyncStatus.Rebuilding)
        scopeIDs.forEach { scopeID ->
            rebuildScope(scopeID)
        }
        schemaManager.completeMigrationIfReady()
    }

    // MARK: - Scope Rebuild

    private suspend fun rebuildScope(scopeId: String, replayRequestJSON: String? = null) {
        var attempt = pullProcessor.beginScopeRebuild(
            scopeId,
            clientGeneration,
            schemaVersion,
            schemaHash,
            config.effectivePullPageSize,
        )
        fireEvent(
            SyncEvent.RebuildRequested(
                SyncRebuildEvent(scopeID = scopeId, rebuildID = attempt.rebuildID),
            ),
        )
        var nextReplayRequestJSON = replayRequestJSON

        while (true) {
            val request = if (nextReplayRequestJSON != null) {
                decodeBackoffRequest<RebuildRequest>(nextReplayRequestJSON).also { replay ->
                    if (replay.clientID != clientID ||
                        replay.clientGeneration != attempt.clientGeneration ||
                        replay.schema != SchemaRef(attempt.schemaVersion, attempt.schemaHash) ||
                        replay.scope != scopeId ||
                        replay.rebuildID != attempt.rebuildID ||
                        replay.limit != attempt.pageLimit
                    ) {
                        throw SynchroError.InvalidResponse("durable rebuild retry identity does not match local state")
                    }
                }
            } else {
                RebuildRequest(
                    clientID = clientID,
                    clientGeneration = attempt.clientGeneration,
                    schema = SchemaRef(attempt.schemaVersion, attempt.schemaHash),
                    scope = scopeId,
                    rebuildID = attempt.rebuildID,
                    cursor = attempt.cursor,
                    limit = attempt.pageLimit,
                )
            }
            val requestJSON = nextReplayRequestJSON ?: httpClient.rebuildRequestJSON(request)
            if (nextReplayRequestJSON != null && request.cursor != attempt.cursor) {
                if (!pullProcessor.resolveAppliedRebuildBackoff(attempt, request, requestJSON)) {
                    throw SynchroError.InvalidResponse("durable rebuild retry identity does not match local state")
                }
                nextReplayRequestJSON = null
                continue
            }

            val pendingFinality = pullProcessor.pendingRebuildFinality(
                attempt,
                request,
                requestJSON,
            )
            if (pendingFinality != null) {
                try {
                    pullProcessor.finalizeScopeRebuild(
                        attempt,
                        pendingFinality.finalCursor,
                        pendingFinality.checksum,
                        syncedTables,
                    )
                    fireEvent(
                        SyncEvent.RebuildCompleted(
                            SyncRebuildEvent(scopeID = scopeId, rebuildID = attempt.rebuildID),
                        ),
                    )
                    return
                } catch (_: RebuildChecksumMismatchException) {
                    attempt = restartScopeRebuild(scopeId)
                    nextReplayRequestJSON = null
                    continue
                }
            }

            try {
                val result = httpClient.rebuildWithBody(request, requestJSON)
                val response = result.response
                attempt = pullProcessor.applyScopeRebuildPage(
                    attempt = attempt,
                    request = request,
                    requestJSON = result.requestJSON,
                    response = response,
                    responseJSON = result.responseJSON,
                    syncedTables = syncedTables,
                )
                nextReplayRequestJSON = null

                if (response.hasMore) {
                    continue
                }

                val finality = pullProcessor.pendingRebuildFinality(
                    attempt,
                    request,
                    result.requestJSON,
                ) ?: throw SynchroError.InvalidResponse("final rebuild page did not persist finality")
                try {
                    pullProcessor.finalizeScopeRebuild(
                        attempt,
                        finality.finalCursor,
                        finality.checksum,
                        syncedTables,
                    )
                    fireEvent(
                        SyncEvent.RebuildCompleted(
                            SyncRebuildEvent(scopeID = scopeId, rebuildID = attempt.rebuildID),
                        ),
                    )
                    return
                } catch (_: RebuildChecksumMismatchException) {
                    attempt = restartScopeRebuild(scopeId)
                    nextReplayRequestJSON = null
                }
            } catch (e: RebuildRestartRequiredException) {
                if (e.scopeID != scopeId) {
                    throw SynchroError.InvalidResponse("rebuild restart response targets an unexpected scope")
                }
                attempt = restartScopeRebuild(scopeId)
                nextReplayRequestJSON = null
            }
        }
    }

    private fun restartScopeRebuild(scopeId: String): LocalRebuildAttempt =
        pullProcessor.restartScopeRebuild(
            scopeId,
            clientGeneration,
            schemaVersion,
            schemaHash,
            config.effectivePullPageSize,
        )

    // MARK: - Bootstrap

    private suspend fun connect(schemaReset: Boolean = false): ConnectExecution {
        transitionTo(SyncStatus.Connecting)
        pullProcessor.reconcileSeedReceiptsBeforeConnect()
        val localState = database.readTransaction { db ->
            val knownScopes = SynchroMeta.getAllScopes(db).associate { scope ->
                scope.scopeID to ScopeCursorRef(cursor = scope.cursor)
            }
            val seedReceipts = SynchroMeta.getSeedReceipts(db)
            if (!knownScopes.keys.containsAll(seedReceipts.keys)) {
                throw SynchroError.InvalidResponse("portable seed receipt has no locally known scope")
            }
            LocalConnectState(
                schema = SchemaRef(
                    version = SynchroMeta.getInt64(db, MetaKey.SCHEMA_VERSION),
                    hash = SynchroMeta.get(db, MetaKey.SCHEMA_HASH) ?: "",
                ),
                clientGeneration = SynchroMeta.getInt64(db, MetaKey.CLIENT_GENERATION),
                scopeSetVersion = SynchroMeta.getInt64(db, MetaKey.SCOPE_SET_VERSION),
                knownScopes = knownScopes,
                seedReceipts = seedReceipts,
            )
        }
        val request = ConnectRequest(
            clientID = clientID,
            clientGeneration = localState.clientGeneration.takeIf { it > 0 },
            platform = config.platform,
            appVersion = config.appVersion,
            protocolVersion = 3,
            schemaReset = true.takeIf { schemaReset },
            schema = localState.schema,
            scopeSetVersion = localState.scopeSetVersion,
            knownScopes = localState.knownScopes,
            seedReceipts = localState.seedReceipts.takeIf { it.isNotEmpty() },
        )

        val requestJSON = httpClient.connectRequestJSON(request)
        val response = httpClient.connectExact(request, requestJSON)
        response.validate(
            request.knownScopes,
            request.scopeSetVersion,
            request.seedReceipts?.keys.orEmpty(),
        )

        return ConnectExecution(response, requestJSON, schemaReset)
    }

    private suspend fun resolveConnectSchema(response: ConnectResponse): Pair<List<LocalSchemaTable>, Pair<Long, String>> {
        return when (response.schema.action) {
            SchemaAction.NONE -> {
                val tables = schemaManager.loadStoredLocalSchema()
                    ?: throw SynchroError.InvalidResponse("connect returned schema action none without stored local schema")
                val stored = database.readTransaction { db ->
                    SchemaRef(
                        version = SynchroMeta.getInt64(db, MetaKey.SCHEMA_VERSION),
                        hash = SynchroMeta.get(db, MetaKey.SCHEMA_HASH).orEmpty(),
                    )
                }
                if (stored != SchemaRef(response.schema.version, response.schema.hash)) {
                    throw SynchroError.InvalidResponse("connect action none does not match the installed schema")
                }
                tables to (response.schema.version to response.schema.hash)
            }
            SchemaAction.REPLACE, SchemaAction.REBUILD_LOCAL -> {
                val manifest = response.schemaDefinition
                    ?: throw SynchroError.InvalidResponse("connect schema action ${response.schema.action} missing schema_definition")
                val tables = manifest.localTables()
                tables to (response.schema.version to response.schema.hash)
            }
            SchemaAction.UNSUPPORTED -> {
                val reason = response.schema.reason
                    ?: throw SynchroError.InvalidResponse("unsupported schema response has no reason")
                database.writeTransaction { db ->
                    val existingGeneration = SynchroMeta.getInt64(db, MetaKey.CLIENT_GENERATION)
                    if (existingGeneration > response.clientGeneration) {
                        throw SynchroError.InvalidResponse("unsupported schema response regresses client generation")
                    }
                    SynchroMeta.setInt64(db, MetaKey.CLIENT_GENERATION, response.clientGeneration)
                }
                throw SynchroError.UnsupportedSchema(reason)
            }
        }
    }

    internal suspend fun installConnectResponse(
        response: ConnectResponse,
        resolvedRequestJSON: String? = null,
        obsoleteBackoff: DurableBackoffRecord? = null,
        schemaReset: Boolean = false,
    ) {
        val connectSchema = resolveConnectSchema(response)
        if (schemaReset) validateSchemaResetResponse(response)
        val lifecycleManaged = synchronized(lifecycleLock) {
            currentStatus.state != SyncLifecycleState.UNINITIALIZED
        }
        val migration = schemaManager.prepareConnectMigration(
            response = response,
            targetTables = connectSchema.first,
            resetMaterialization = schemaReset,
        )
        val schemaEvent = migration?.let {
            SyncSchemaEvent(source = it.source, target = it.target, action = it.action)
        }
        if (schemaEvent != null && lifecycleManaged) transitionTo(SyncStatus.SchemaApplying)
        schemaEvent?.let { fireEvent(SyncEvent.SchemaApplying(it)) }
        database.writeSyncLockedTransaction { db ->
            bindClientIdentityInTransaction(db)
            if (migration != null) {
                schemaManager.applyPreparedMigrationInTransaction(db)
            } else {
                schemaManager.reconcileLocalSchemaInTransaction(
                    db,
                    response.schema.version,
                    response.schema.hash,
                    connectSchema.first,
                    response.scopeCursorUpdates,
                    response.affectedScopes ?: emptyList(),
                )
            }
            if (schemaReset) {
                pushProcessor.reconcileSchemaResetInTransaction(db)
                DurableBackoffStore.clearSchemaResetWork(db)
            }
            pullProcessor.installConnectedAssignmentInTransaction(
                db,
                response.scopes,
                response.scopeSetVersion,
                response.clientGeneration,
                connectSchema.first,
                response.scopeCursorUpdates,
            )
            resolvedRequestJSON?.let { requestJSON ->
                DurableBackoffStore.clearMatching(db, RetryOperation.CONNECTING, requestJSON)
            }
            obsoleteBackoff?.let { backoff ->
                DurableBackoffStore.clearMatching(db, backoff.resumeState, backoff.workIdentity)
            }
        }
        syncedTables = connectSchema.first
        schemaVersion = connectSchema.second.first
        schemaHash = connectSchema.second.second
        clientGeneration = response.clientGeneration
        connectionReady = true
        schemaEvent?.let { fireEvent(SyncEvent.SchemaApplied(it)) }
        schemaManager.completeMigrationIfReady()
        if (lifecycleManaged) transitionTo(SyncStatus.Ready)
    }

    /** A reset rebuilds every retained assignment and never reuses a cursor. */
    private fun validateSchemaResetResponse(response: ConnectResponse) {
        val existingScopes = database.readTransaction { db ->
            SynchroMeta.getAllScopes(db).map { it.scopeID }.toSet()
        }
        val assignedScopes = (existingScopes - response.scopes.remove.toSet()) + response.scopes.add.map { it.id }
        if (assignedScopes.isEmpty()) {
            if (response.schema.action != SchemaAction.REPLACE) {
                throw SynchroError.InvalidResponse("schema reset without assignments requires replace")
            }
            return
        }
        if (response.schema.action != SchemaAction.REBUILD_LOCAL ||
            response.affectedScopes.orEmpty().toSet() != assignedScopes ||
            response.scopes.add.any { it.cursor != null } ||
            response.scopeCursorUpdates.values.any { it != null }
        ) {
            throw SynchroError.InvalidResponse("schema reset must rebuild every assigned scope")
        }
    }

    private fun bindClientIdentityBeforeConnect() {
        database.writeTransaction { db ->
            val boundClientID = validateBoundClientIdentity(db)
            if (boundClientID == null && !isUnboundSeedBootstrap(db)) {
                SynchroMeta.set(db, MetaKey.CLIENT_ID, clientID)
            }
        }
    }

    private fun bindClientIdentityInTransaction(db: android.database.sqlite.SQLiteDatabase) {
        val boundClientID = validateBoundClientIdentity(db)
        if (boundClientID == null) {
            SynchroMeta.set(db, MetaKey.CLIENT_ID, clientID)
        }
    }

    private fun validateBoundClientIdentity(db: android.database.sqlite.SQLiteDatabase): String? {
        if (clientID.isEmpty()) {
            throw SynchroError.InvalidResponse("configured client ID is empty")
        }
        val bindings = listOfNotNull(
            SynchroMeta.get(db, MetaKey.CLIENT_ID),
            SynchroMeta.get(db, MetaKey.CLIENT_SERVER_ID),
        ).toSet()
        if (bindings.size > 1 || bindings.singleOrNull()?.let { it != clientID } == true) {
            throw SynchroError.InvalidResponse("database is bound to a different client ID")
        }
        return bindings.singleOrNull()
    }

    private fun isUnboundSeedBootstrap(db: android.database.sqlite.SQLiteDatabase): Boolean =
        SynchroMeta.get(db, MetaKey.SNAPSHOT_COMPLETE) == "1" &&
            SynchroMeta.getInt64(db, MetaKey.CLIENT_GENERATION) == 0L

    private fun loadKnownScopes(): Map<String, ScopeCursorRef> {
        return database.readTransaction { db ->
            SynchroMeta.getAllScopes(db).associate { scope ->
                scope.scopeID to ScopeCursorRef(cursor = scope.cursor)
            }
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

    // MARK: - Debounce

    private fun startPendingObserver() {
        if (pendingObserver != null) return
        pendingObserver = database.onChange(listOf("_synchro_pending_changes")) {
            if (changeTracker.hasCapturedChanges()) {
                scheduleDebouncedPush()
            }
        }
    }

    private fun scheduleDebouncedPush() {
        synchronized(lifecycleLock) {
            if (!appInForeground) return
            debounceJob?.cancel()
            debounceJob = scope?.launch {
                if (!beginOperation()) return@launch
                try {
                    delay((config.pushDebounce * 1000).toLong())
                    if (!isActive || !isApplicationForeground()) return@launch
                    runSyncCycleWithRetry()
                } catch (e: CancellationException) {
                    throw e
                } catch (_: Exception) {
                    // Debounced sync failed, so the managed loop retries it later.
                } finally {
                    endOperation()
                }
            }
        }
    }

    // MARK: - Error Handling

    private fun enterBackoff(error: RetryableError): DurableBackoffRecord {
        val record = DurableBackoffStore.persist(
            database = database,
            error = error,
            currentTimeMillis = retryTiming.currentTimeMillis(),
            fallbackDelaySeconds = { attempt -> retryDelay(attempt.coerceAtMost(Int.MAX_VALUE.toLong()).toInt()) },
        )
        error.persistedBackoff = record
        transitionTo(
            SyncStatus.Backoff(
                retryAt = java.time.Instant.ofEpochMilli(record.nextRetryAtMs),
                operation = record.resumeState,
            ),
        )
        return record
    }

    private suspend fun awaitBackoffDeadline(record: DurableBackoffRecord) {
        val currentTime = retryTiming.currentTimeMillis()
        val remaining = if (record.nextRetryAtMs <= currentTime) {
            0L
        } else {
            record.nextRetryAtMs - currentTime
        }
        retryTiming.sleep(remaining)
    }

    private fun ensureLifecycleActive() {
        val active = synchronized(lifecycleLock) {
            started.get() && !shutdownRequested && !shutdownInProgress
        }
        if (!active) throw CancellationException("sync engine lifecycle changed")
    }

    private fun handleSyncError(error: Exception) {
        recordBlockingFailure(error)
    }

    private fun recordBlockingFailure(error: Exception) {
        if (error is CancellationException || error is SynchroError.BlockingFailure) return
        val current = synchronized(lifecycleLock) { currentStatus.state }
        if (current in setOf(SyncLifecycleState.STOPPED, SyncLifecycleState.ERROR)) return
        transitionTo(SyncStatus.Error(failureFor(error, operationForState(current))))
    }

    private fun failureFor(error: Exception, operation: SyncOperationKind): SyncFailure = when (error) {
        is SynchroError.UpgradeRequired -> SyncFailure(
            operation = SyncOperationKind.CONNECTING,
            code = SyncFailureCode.UPGRADE_REQUIRED,
            retryable = false,
            message = "The client runtime does not satisfy the server requirement.",
            recoveryAction = SyncRecoveryAction.NONE,
        )
        is SynchroError.SchemaMismatch -> SyncFailure(
            operation = operation,
            code = SyncFailureCode.SCHEMA_MISMATCH,
            retryable = false,
            message = "The server rejected the installed schema reference.",
            recoveryAction = SyncRecoveryAction.RETRY,
        )
        is SynchroError.ServerError -> SyncFailure(
            operation = operation,
            code = SyncFailureCode.SERVER_ERROR,
            retryable = false,
            message = "The server rejected the sync operation.",
            recoveryAction = SyncRecoveryAction.RETRY,
            metadata = mapOf("http_status" to error.status.toString()),
        )
        is SynchroError.NetworkError -> SyncFailure(
            operation = operation,
            code = SyncFailureCode.NETWORK_ERROR,
            retryable = true,
            message = "The sync operation could not reach the server.",
            recoveryAction = SyncRecoveryAction.RETRY,
        )
        is SynchroError.DatabaseError -> SyncFailure(
            operation = SyncOperationKind.DATABASE,
            code = SyncFailureCode.DATABASE_ERROR,
            retryable = false,
            message = "The local sync database operation failed.",
            recoveryAction = SyncRecoveryAction.RETRY,
        )
        is SynchroError.UnsupportedSchema -> SyncFailure(
            operation = SyncOperationKind.SCHEMA,
            code = SyncFailureCode.UNSUPPORTED_SCHEMA,
            retryable = false,
            message = "The installed schema requires an explicit synchronized reset.",
            recoveryAction = SyncRecoveryAction.SCHEMA_RESET,
            metadata = mapOf("reason" to error.reason.name.lowercase()),
        )
        is SynchroError.InvalidStateTransition -> SyncFailure(
            operation = SyncOperationKind.LIFECYCLE,
            code = SyncFailureCode.INVALID_STATE_TRANSITION,
            retryable = false,
            message = "The sync engine rejected an invalid lifecycle transition.",
            recoveryAction = SyncRecoveryAction.RETRY,
            metadata = mapOf(
                "from" to error.from.wireName,
                "to" to error.to.wireName,
            ),
        )
        is SynchroError.InvalidResponse -> SyncFailure(
            operation = operation,
            code = SyncFailureCode.INVALID_RESPONSE,
            retryable = false,
            message = "The sync operation failed contract validation.",
            recoveryAction = SyncRecoveryAction.RETRY,
        )
        else -> SyncFailure(
            operation = operation,
            code = SyncFailureCode.LOCAL_FAILURE,
            retryable = false,
            message = "The sync operation failed locally.",
            recoveryAction = SyncRecoveryAction.RETRY,
        )
    }

    private fun operationForState(state: SyncLifecycleState): SyncOperationKind = when (state) {
        SyncLifecycleState.CONNECTING -> SyncOperationKind.CONNECTING
        SyncLifecycleState.SCHEMA_APPLYING -> SyncOperationKind.SCHEMA
        SyncLifecycleState.PUSHING -> SyncOperationKind.PUSHING
        SyncLifecycleState.PULLING -> SyncOperationKind.PULLING
        SyncLifecycleState.REBUILDING -> SyncOperationKind.REBUILDING
        SyncLifecycleState.UNINITIALIZED,
        SyncLifecycleState.LOCAL_READY,
        SyncLifecycleState.READY,
        SyncLifecycleState.BACKOFF,
        SyncLifecycleState.ERROR,
        SyncLifecycleState.STOPPED -> SyncOperationKind.LIFECYCLE
    }

    private fun retryOperationKind(value: String): SyncOperationKind =
        SyncOperationKind.fromWireName(value)
            ?: throw SynchroError.InvalidResponse("retry operation is invalid")

    private fun teardownAfterFailedStart(generation: Long) {
        synchronized(lifecycleLock) {
            if (generation != lifecycleGeneration) return
            cancelLifecycleLocked()
        }
    }

    private fun finishStartupFailure(generation: Long) {
        synchronized(lifecycleLock) {
            if (generation != lifecycleGeneration) return
            cancelLifecycleLocked()
        }
    }

    // MARK: - Status

    internal fun getSyncStatus(): SyncStatus = currentStatus

    private fun transitionTo(
        status: SyncStatus,
        processRecovery: Boolean = false,
        persistedStateAlreadyApplied: Boolean = false,
    ) {
        val previous: SyncStatus
        synchronized(lifecycleLock) {
            previous = currentStatus
            if (status.state !in LifecycleTransitions.LEGAL_TRANSITIONS.getValue(previous.state)) {
                throw SynchroError.InvalidStateTransition(previous.state, status.state)
            }
            database.writeTransaction { db ->
                val durableState = SynchroMeta.getClientState(db).lifecycleState
                val statusWasPersistedByRecovery = persistedStateAlreadyApplied ||
                    (status.state == SyncLifecycleState.STOPPED && durableState == SyncLifecycleState.STOPPED)
                when {
                    statusWasPersistedByRecovery -> {
                        if (status.state !in setOf(SyncLifecycleState.LOCAL_READY, SyncLifecycleState.STOPPED) ||
                            durableState != status.state
                        ) {
                            throw SynchroError.InvalidResponse("durable lifecycle state does not match explicit recovery")
                        }
                    }
                    status is SyncStatus.Error -> SynchroMeta.recordBlockingError(db, status.failure)
                    else -> {
                        SynchroMeta.transitionClientLifecycleState(
                            db,
                            status.state,
                            processRecovery = processRecovery,
                        )
                        if (status.state == SyncLifecycleState.READY &&
                            SynchroMeta.getClientState(db).errorAcknowledged
                        ) {
                            SynchroMeta.clearBlockingError(db)
                        }
                    }
                }
            }
            currentStatus = status
        }
        for ((_, cb) in statusCallbacks) {
            try {
                cb(status)
            } catch (_: Exception) {
                // User callback must not crash the sync engine
            }
        }
        fireEvent(SyncEvent.StateChanged(SyncStateChangeEvent(previous.state, status.state)))
        when (status) {
            is SyncStatus.Backoff -> fireEvent(
                SyncEvent.Backoff(
                    SyncBackoffEvent(
                        operation = retryOperationKind(status.operation),
                        attempt = DurableBackoffStore.load(database)?.attemptCount ?: 0L,
                        retryAt = status.retryAt,
                    ),
                ),
            )
            is SyncStatus.Error -> fireEvent(SyncEvent.Failure(status.failure))
            else -> Unit
        }
    }

    /** A repeated stop notifies current listeners without creating a transition. */
    private fun publishStatusSnapshot(status: SyncStatus) {
        for ((_, callback) in statusCallbacks) {
            try {
                callback(status)
            } catch (_: Exception) {
                // User callback must not crash the sync engine.
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

    private fun fireEvent(event: SyncEvent) {
        for ((_, callback) in eventCallbacks) {
            try {
                callback(event)
            } catch (_: Exception) {
                // User callback must not crash the sync engine.
            }
        }
    }

    private fun isApplicationForeground(): Boolean = synchronized(lifecycleLock) { appInForeground }

    private object LifecycleTransitions {
        val LEGAL_TRANSITIONS = mapOf(
            SyncLifecycleState.UNINITIALIZED to setOf(
                SyncLifecycleState.LOCAL_READY,
                SyncLifecycleState.ERROR,
                SyncLifecycleState.STOPPED,
            ),
            SyncLifecycleState.LOCAL_READY to setOf(
                SyncLifecycleState.CONNECTING,
                SyncLifecycleState.ERROR,
                SyncLifecycleState.STOPPED,
            ),
            SyncLifecycleState.CONNECTING to setOf(
                SyncLifecycleState.SCHEMA_APPLYING,
                SyncLifecycleState.READY,
                SyncLifecycleState.BACKOFF,
                SyncLifecycleState.ERROR,
                SyncLifecycleState.STOPPED,
            ),
            SyncLifecycleState.SCHEMA_APPLYING to setOf(
                SyncLifecycleState.READY,
                SyncLifecycleState.REBUILDING,
                SyncLifecycleState.ERROR,
                SyncLifecycleState.STOPPED,
            ),
            SyncLifecycleState.READY to setOf(
                SyncLifecycleState.CONNECTING,
                SyncLifecycleState.PUSHING,
                SyncLifecycleState.PULLING,
                SyncLifecycleState.REBUILDING,
                SyncLifecycleState.ERROR,
                SyncLifecycleState.STOPPED,
            ),
            SyncLifecycleState.PUSHING to setOf(
                SyncLifecycleState.PUSHING,
                SyncLifecycleState.READY,
                SyncLifecycleState.PULLING,
                SyncLifecycleState.CONNECTING,
                SyncLifecycleState.BACKOFF,
                SyncLifecycleState.ERROR,
                SyncLifecycleState.STOPPED,
            ),
            SyncLifecycleState.PULLING to setOf(
                SyncLifecycleState.PULLING,
                SyncLifecycleState.READY,
                SyncLifecycleState.REBUILDING,
                SyncLifecycleState.CONNECTING,
                SyncLifecycleState.BACKOFF,
                SyncLifecycleState.ERROR,
                SyncLifecycleState.STOPPED,
            ),
            SyncLifecycleState.REBUILDING to setOf(
                SyncLifecycleState.REBUILDING,
                SyncLifecycleState.READY,
                SyncLifecycleState.CONNECTING,
                SyncLifecycleState.BACKOFF,
                SyncLifecycleState.ERROR,
                SyncLifecycleState.STOPPED,
            ),
            SyncLifecycleState.BACKOFF to setOf(
                SyncLifecycleState.CONNECTING,
                SyncLifecycleState.PUSHING,
                SyncLifecycleState.PULLING,
                SyncLifecycleState.REBUILDING,
                SyncLifecycleState.ERROR,
                SyncLifecycleState.STOPPED,
            ),
            SyncLifecycleState.ERROR to setOf(
                SyncLifecycleState.LOCAL_READY,
                SyncLifecycleState.STOPPED,
            ),
            SyncLifecycleState.STOPPED to setOf(SyncLifecycleState.LOCAL_READY),
        )
    }
}

class CallbackCancellable(private var onCancel: (() -> Unit)?) : Cancellable {
    @Synchronized
    override fun cancel() {
        onCancel?.invoke()
        onCancel = null
    }
}
