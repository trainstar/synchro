package com.trainstar.synchro

import android.content.Context

class SynchroClient(private val config: SynchroConfig, context: Context) {
    private val database: SynchroDatabase = SynchroDatabase(context, config.dbPath)
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

    // MARK: - Core SQL

    fun query(sql: String, params: Array<out Any?>? = null): List<Row> =
        database.query(sql, params)

    fun queryOne(sql: String, params: Array<out Any?>? = null): Row? =
        database.queryOne(sql, params)

    fun execute(sql: String, params: Array<out Any?>? = null): ExecResult =
        database.execute(sql, params)

    // MARK: - Transactions

    fun <T> readTransaction(block: (android.database.sqlite.SQLiteDatabase) -> T): T =
        database.readTransaction(block)

    fun <T> writeTransaction(block: (android.database.sqlite.SQLiteDatabase) -> T): T =
        database.writeTransaction(block)

    // MARK: - Batch

    fun executeBatch(statements: List<SQLStatement>): Int =
        database.executeBatch(statements)

    // MARK: - Schema (local-only tables)

    fun createTable(name: String, columns: List<ColumnDef>, options: TableOptions? = null) =
        database.createTable(name, columns, options)

    fun alterTable(name: String, addColumns: List<ColumnDef>) =
        database.alterTable(name, addColumns)

    fun createIndex(table: String, columns: List<String>, unique: Boolean = false) =
        database.createIndex(table, columns, unique)

    // MARK: - Observation

    fun onChange(tables: List<String>, callback: () -> Unit): Cancellable =
        database.onChange(tables, callback)

    fun watch(
        sql: String,
        params: Array<out Any?>? = null,
        tables: List<String>,
        callback: (List<Row>) -> Unit
    ): Cancellable = database.watch(sql, params, tables, callback)

    // MARK: - WAL

    fun checkpoint(mode: CheckpointMode = CheckpointMode.PASSIVE) =
        database.checkpoint(mode)

    // MARK: - Lifecycle

    fun close() {
        syncEngine.stop()
        database.close()
        okHttpClient.dispatcher.executorService.shutdown()
        okHttpClient.connectionPool.evictAll()
    }

    val path: String get() = database.path

    // MARK: - Sync Status

    fun pendingChangeCount(): Int = changeTracker.pendingChangeCount()

    // MARK: - Sync Control

    suspend fun start(options: SyncOptions? = null) = syncEngine.start(options)

    fun stop() = syncEngine.stop()

    suspend fun syncNow() = syncEngine.syncNow()

    // MARK: - Status

    fun onStatusChange(callback: (SyncStatus) -> Unit): Cancellable =
        syncEngine.onStatusChange(callback)

    fun onConflict(callback: (ConflictEvent) -> Unit): Cancellable =
        syncEngine.onConflict(callback)

    fun onSnapshotRequired(callback: suspend () -> Boolean): Cancellable =
        syncEngine.onSnapshotRequired(callback)
}
