package com.trainstar.synchro

import android.content.Context
import android.app.Activity
import android.app.Application
import android.os.Bundle
import kotlinx.coroutines.runBlocking

class SynchroClient(private val config: SynchroConfig, context: Context) {
    init {
        config.seedDatabasePath?.let { seedPath ->
            SeedDatabaseInstaller.installIfNeeded(context, seedPath, config.dbPath)
        }
    }
    private val database: SynchroDatabase = SynchroDatabase.open(context, config.dbPath)
    private val okHttpClient: okhttp3.OkHttpClient = okhttp3.OkHttpClient.Builder()
        .connectTimeout(30, java.util.concurrent.TimeUnit.SECONDS)
        .readTimeout(60, java.util.concurrent.TimeUnit.SECONDS)
        .writeTimeout(60, java.util.concurrent.TimeUnit.SECONDS)
        .build()
    private val httpClient: HttpClient = HttpClient(config, okHttpClient)
    private val schemaManager: SchemaManager = SchemaManager(database)
    private val changeTracker: ChangeTracker = ChangeTracker(database)
    private val pullProcessor: PullProcessor = PullProcessor(database)
    private val pushProcessor: PushProcessor = PushProcessor(database, changeTracker)
    private val syncEngine: SyncEngine = SyncEngine(
        config = config,
        database = database,
        httpClient = httpClient,
        schemaManager = schemaManager,
        changeTracker = changeTracker,
        pullProcessor = pullProcessor,
        pushProcessor = pushProcessor
    )
    private val application: Application? = context.applicationContext as? Application
    private val lifecycleObserver: Application.ActivityLifecycleCallbacks? = application?.let { app ->
        NativeApplicationLifecycleObserver(
            onForeground = syncEngine::onApplicationForeground,
            onBackground = syncEngine::onApplicationBackground,
        ).also(app::registerActivityLifecycleCallbacks)
    }

    // MARK: - Core SQL

    fun query(sql: String, params: Array<out Any?>? = null): List<Row> =
        database.applicationQuery(sql, params)

    fun queryOne(sql: String, params: Array<out Any?>? = null): Row? =
        database.applicationQueryOne(sql, params)

    fun execute(sql: String, params: Array<out Any?>? = null): ExecResult =
        database.applicationExecute(sql, params)

    // MARK: - Transactions

    fun <T> readTransaction(block: (ApplicationReadTransaction) -> T): T =
        database.applicationReadTransaction(block)

    fun <T> transaction(block: (ApplicationTransaction) -> T): T =
        database.applicationTransaction(block)

    /** Kept as a source-compatible name without exposing SQLiteDatabase. */
    fun <T> writeTransaction(block: (ApplicationTransaction) -> T): T =
        transaction(block)

    // MARK: - Batch

    fun executeBatch(statements: List<SQLStatement>): Int =
        database.applicationExecuteBatch(statements)

    // MARK: - Schema (local-only tables)

    fun createTable(name: String, columns: List<ColumnDef>, options: TableOptions? = null) =
        database.createLocalOnlyTable(name, columns, options)

    fun alterTable(name: String, addColumns: List<ColumnDef>) =
        database.alterLocalOnlyTable(name, addColumns)

    fun createIndex(table: String, columns: List<String>, unique: Boolean = false) =
        database.createLocalOnlyIndex(table, columns, unique)

    // MARK: - Observation

    fun onChange(tables: List<String>, callback: () -> Unit): Cancellable {
        tables.forEach(ApplicationSql::requireApplicationObject)
        return database.onChange(tables, callback)
    }

    fun watch(
        sql: String,
        params: Array<out Any?>? = null,
        tables: List<String>,
        callback: (List<Row>) -> Unit
    ): Cancellable {
        ApplicationSql.authorizeRead(sql)
        tables.forEach(ApplicationSql::requireApplicationObject)
        return database.watch(sql, params, tables, callback)
    }

    // MARK: - WAL

    fun checkpoint(mode: CheckpointMode = CheckpointMode.PASSIVE) =
        database.checkpoint(mode)

    // MARK: - Lifecycle

    fun close() {
        runBlocking {
            syncEngine.shutdown()
        }
        lifecycleObserver?.let { observer -> application?.unregisterActivityLifecycleCallbacks(observer) }
        database.close()
        okHttpClient.dispatcher.executorService.shutdown()
        okHttpClient.connectionPool.evictAll()
    }

    val path: String get() = database.path

    // MARK: - Sync Status

    fun pendingChangeCount(): Int = changeTracker.pendingChangeCount()

    fun getSyncStatus(): SyncStatus = syncEngine.getSyncStatus()

    fun inspectPendingMutations(): List<PendingMutationInspection> =
        changeTracker.inspectPendingMutations()

    fun inspectRejectedMutations(): List<RejectedMutationInspection> =
        database.readTransaction { db ->
            SynchroMeta.listRejectedMutations(db).map { rejected ->
                RejectedMutationInspection(
                    mutationID = rejected.mutationID,
                    tableName = rejected.tableName,
                    recordID = rejected.recordID,
                    status = rejected.status.asRejectedMutationStatus(),
                    code = rejected.code.asMutationRejectionCode(),
                    message = rejected.message,
                    serverRowJSON = rejected.serverRowJson,
                    serverVersion = rejected.serverVersion,
                    mutationJSON = rejected.mutationJSON
                        ?: throw SynchroError.InvalidResponse("retained rejection lacks its exact mutation JSON"),
                    rejectionJSON = rejected.rejectionJSON
                        ?: throw SynchroError.InvalidResponse("retained rejection lacks its exact rejection JSON"),
                    createdAt = rejected.createdAt,
                    updatedAt = rejected.updatedAt,
                )
            }
        }

    fun clearRejectedMutations() {
        database.writeTransaction { db -> SynchroMeta.clearRejectedMutations(db) }
    }

    /** Returns the durable schema reference without exposing reserved SQLite state. */
    fun inspectSchema(): SchemaInspection = database.readTransaction { db ->
        val version = SynchroMeta.getInt64(db, MetaKey.SCHEMA_VERSION)
        val hash = SynchroMeta.get(db, MetaKey.SCHEMA_HASH).orEmpty()
        when {
            version == 0L && hash.isEmpty() -> SchemaInspection(null)
            version > 0L && hash.matches(SCHEMA_HASH) -> {
                val schema = SchemaRef(version, hash)
                schema.validate()
                SchemaInspection(schema)
            }
            else -> throw SynchroError.InvalidResponse("durable schema inspection is invalid")
        }
    }

    /** Returns at most [limit] durable scopes, or fails when that bound is exceeded. */
    fun inspectScopes(limit: Int = MAXIMUM_INSPECTION_RECORDS): List<ScopeInspection> =
        database.readTransaction { db ->
            SynchroMeta.listScopes(db, limit).map {
                ScopeInspection(it.scopeID, it.cursor, it.checksum, it.generation, it.localChecksum)
            }
        }

    /** Returns at most [limit] durable scope-row memberships. */
    fun inspectScopeRows(limit: Int = MAXIMUM_INSPECTION_RECORDS): List<ScopeRowInspection> =
        database.readTransaction { db ->
            SynchroMeta.listScopeRows(db, limit).map {
                ScopeRowInspection(it.scopeID, it.tableName, it.recordID, it.checksum, it.generation)
            }
        }

    /** Returns at most [limit] durable row-version records. */
    fun inspectRowMetadata(limit: Int = MAXIMUM_INSPECTION_RECORDS): List<RowMetadataInspection> =
        database.readTransaction { db ->
            SynchroMeta.listRowMetadata(db, limit).map {
                RowMetadataInspection(it.tableName, it.recordID, it.serverVersion, it.rowChecksumJSON)
            }
        }

    /** Returns at most [limit] scope checkpoints. */
    fun inspectCheckpoints(limit: Int = MAXIMUM_INSPECTION_RECORDS): List<CheckpointInspection> =
        database.readTransaction { db ->
            SynchroMeta.listScopes(db, limit).map {
                CheckpointInspection(it.scopeID, it.cursor, it.checksum, it.localChecksum)
            }
        }

    /** Returns at most [limit] row-to-scope provenance records. */
    fun inspectProvenance(limit: Int = MAXIMUM_INSPECTION_RECORDS): List<ProvenanceInspection> =
        database.readTransaction { db ->
            val rows = SynchroMeta.listScopeRows(db, limit)
            rows.groupBy { it.tableName to it.recordID }.map { (key, members) ->
                ProvenanceInspection(
                    tableName = key.first,
                    recordID = key.second,
                    scopeIDs = members.map { it.scopeID }.sorted(),
                    serverVersion = SynchroMeta.getRowVersion(db, key.first, key.second),
                )
            }.sortedWith(compareBy<ProvenanceInspection> { it.tableName }.thenBy { it.recordID })
        }

    /** Returns bounded read-only state for unfinished rebuild work. */
    fun inspectRebuildState(limit: Int = MAXIMUM_INSPECTION_RECORDS): RebuildStateInspection =
        database.readTransaction { db ->
            val attempts = SynchroMeta.listRebuildAttempts(db, limit).map {
                require(it.schemaVersion > 0 && it.schemaHash.matches(SCHEMA_HASH)) {
                    "durable rebuild inspection is invalid"
                }
                val schema = SchemaRef(it.schemaVersion, it.schemaHash)
                schema.validate()
                RebuildAttemptInspection(
                    scopeID = it.scopeID,
                    rebuildID = it.rebuildID,
                    clientGeneration = it.clientGeneration,
                    schema = schema,
                    generation = it.generation,
                    cursor = it.cursor,
                    pageLimit = it.pageLimit,
                )
            }
            val receipts = SynchroMeta.listRebuildPageReceipts(db, limit).map {
                RebuildPageReceiptInspection(
                    scopeID = it.scopeID,
                    rebuildID = it.rebuildID,
                    requestCursor = it.requestCursor,
                    isFinal = it.isFinal,
                    finalScopeCursor = it.finalScopeCursor,
                    finalChecksumJSON = it.finalChecksumJSON,
                )
            }
            RebuildStateInspection(attempts, receipts)
        }

    // MARK: - Sync Control

    suspend fun start(options: SyncOptions? = null) = syncEngine.start(options)

    suspend fun stop() = syncEngine.stop()

    suspend fun retry(options: SyncOptions? = null) = syncEngine.retry(options)

    suspend fun resetSchema(options: SyncOptions? = null) = syncEngine.resetSchema(options)

    suspend fun syncNow() = syncEngine.syncNow()

    /** Hosts without activities can forward native foreground state explicitly. */
    fun onApplicationForeground() = syncEngine.onApplicationForeground()

    /** Hosts without activities can forward native background state explicitly. */
    fun onApplicationBackground() = syncEngine.onApplicationBackground()

    // MARK: - Status

    fun onStatusChange(callback: (SyncStatus) -> Unit): Cancellable =
        syncEngine.onStatusChange(callback)

    fun onConflict(callback: (ConflictEvent) -> Unit): Cancellable =
        syncEngine.onConflict(callback)

    fun onSyncEvent(callback: (SyncEvent) -> Unit): Cancellable =
        syncEngine.onEvent(callback)

    private fun String.asRejectedMutationStatus(): MutationStatus = when (this) {
        "conflict" -> MutationStatus.CONFLICT
        "rejected_terminal" -> MutationStatus.REJECTED_TERMINAL
        else -> throw SynchroError.InvalidResponse("retained rejection has an invalid status")
    }

    private fun String.asMutationRejectionCode(): MutationRejectionCode =
        MutationRejectionCode.entries.firstOrNull { it.name.lowercase() == this }
            ?: throw SynchroError.InvalidResponse("retained rejection has an invalid code")

    private companion object {
        const val MAXIMUM_INSPECTION_RECORDS = 512
        val SCHEMA_HASH = Regex("[0-9a-f]{64}")
    }

}

/** Tracks native activity visibility without a JavaScript lifecycle dependency. */
internal class NativeApplicationLifecycleObserver(
    private val onForeground: () -> Unit,
    private val onBackground: () -> Unit,
    private val isChangingConfigurations: (Activity) -> Boolean = { activity -> activity.isChangingConfigurations },
) : Application.ActivityLifecycleCallbacks {
    private var startedActivities = 0
    private var awaitingReplacementActivity = false

    override fun onActivityCreated(activity: Activity, savedInstanceState: Bundle?) = Unit

    override fun onActivityStarted(activity: Activity) {
        val becameVisible = startedActivities++ == 0
        if (becameVisible && !awaitingReplacementActivity) onForeground()
        awaitingReplacementActivity = false
    }

    override fun onActivityResumed(activity: Activity) = Unit

    override fun onActivityPaused(activity: Activity) = Unit

    override fun onActivityStopped(activity: Activity) {
        startedActivities = (startedActivities - 1).coerceAtLeast(0)
        if (startedActivities == 0) {
            awaitingReplacementActivity = isChangingConfigurations(activity)
            if (!awaitingReplacementActivity) onBackground()
        }
    }

    override fun onActivitySaveInstanceState(activity: Activity, outState: Bundle) = Unit

    override fun onActivityDestroyed(activity: Activity) = Unit
}
