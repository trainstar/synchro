package com.trainstar.synchro

import android.content.Context

class SynchroClient(private val config: SynchroConfig, context: Context) {
    init {
        config.seedDatabasePath?.let { seedPath ->
            val dbFile = context.getDatabasePath(config.dbPath)
            val seedFile = java.io.File(seedPath)
            if (!dbFile.exists() && seedFile.exists()) {
                dbFile.parentFile?.mkdirs()
                seedFile.copyTo(dbFile)
                // Copy WAL/SHM files if they exist alongside the seed
                for (suffix in listOf("-wal", "-shm")) {
                    val src = java.io.File(seedPath + suffix)
                    val dst = java.io.File(dbFile.path + suffix)
                    if (src.exists()) src.copyTo(dst)
                }
            }
        }
    }
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

    // MARK: - Debug

    /**
     * Returns diagnostic information about local sync state for support tickets.
     */
    fun debugInfo(): SynchroDebugInfo {
        return database.readTransaction { db ->
            val checkpoint = SynchroMeta.getInt64(db, MetaKey.CHECKPOINT)
            val bucketCheckpoints = SynchroMeta.getAllBucketCheckpoints(db)

            val buckets = bucketCheckpoints.map { (bucketId, cp) ->
                var memberCount = 0
                var xor = 0
                db.rawQuery(
                    "SELECT checksum FROM _synchro_bucket_members WHERE bucket_id = ?",
                    arrayOf(bucketId)
                ).use { cursor ->
                    while (cursor.moveToNext()) {
                        memberCount++
                        if (!cursor.isNull(0)) {
                            xor = xor xor cursor.getInt(0)
                        }
                    }
                }
                BucketDebugInfo(
                    bucketID = bucketId,
                    checkpoint = cp,
                    memberCount = memberCount,
                    checksum = xor
                )
            }

            var pendingCount = 0
            db.rawQuery("SELECT COUNT(*) FROM _synchro_pending_changes", null).use { cursor ->
                if (cursor.moveToFirst()) pendingCount = cursor.getInt(0)
            }

            val schemaVersion = SynchroMeta.getInt64(db, MetaKey.SCHEMA_VERSION)
            val schemaHash = SynchroMeta.get(db, MetaKey.SCHEMA_HASH) ?: ""

            SynchroDebugInfo(
                clientID = config.clientID,
                buckets = buckets,
                lastSyncCheckpoint = checkpoint,
                schemaVersion = schemaVersion,
                schemaHash = schemaHash,
                pendingChangeCount = pendingCount,
                generatedAt = java.time.Instant.now().toString()
            )
        }
    }

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

}
