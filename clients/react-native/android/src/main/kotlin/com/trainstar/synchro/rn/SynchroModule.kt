package com.trainstar.synchro.rn

import com.facebook.react.bridge.*
import com.trainstar.synchro.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.json.JSONArray
import org.json.JSONObject
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

@OptIn(ExperimentalCoroutinesApi::class)
class SynchroModule(reactContext: ReactApplicationContext) :
    NativeSynchroSpec(reactContext) {

    private var client: SynchroClient? = null
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val sessions = ConcurrentHashMap<String, TransactionSession>()
    private val observers = ConcurrentHashMap<String, Cancellable>()
    private val pendingAuthContinuations = ConcurrentHashMap<String, CancellableContinuation<String>>()
    private val pendingSnapshotContinuations = ConcurrentHashMap<String, CancellableContinuation<Boolean>>()
    private var statusSubscription: Cancellable? = null
    private var conflictSubscription: Cancellable? = null
    private var snapshotSubscription: Cancellable? = null

    private fun rejectWithError(promise: Promise, error: Throwable) {
        when (error) {
            is SynchroError -> {
                val (code, userInfo) = mapSynchroError(error)
                val writableMap = Arguments.createMap().apply {
                    userInfo.forEach { (k, v) -> putString(k, v) }
                }
                promise.reject(code, error.message, error, writableMap)
            }
            else -> promise.reject("UNKNOWN", error.message, error)
        }
    }

    private fun mapSynchroError(error: SynchroError): Pair<String, Map<String, String>> {
        return when (error) {
            is SynchroError.NotConnected -> "NOT_CONNECTED" to emptyMap()
            is SynchroError.SchemaNotLoaded -> "SCHEMA_NOT_LOADED" to emptyMap()
            is SynchroError.TableNotSynced -> "TABLE_NOT_SYNCED" to mapOf("table" to error.table)
            is SynchroError.UpgradeRequired -> "UPGRADE_REQUIRED" to mapOf(
                "currentVersion" to error.currentVersion,
                "minimumVersion" to error.minimumVersion
            )
            is SynchroError.SchemaMismatch -> "SCHEMA_MISMATCH" to mapOf(
                "serverVersion" to error.serverVersion.toString(),
                "serverHash" to error.serverHash
            )
            is SynchroError.SnapshotRequired -> "SNAPSHOT_REQUIRED" to emptyMap()
            is SynchroError.PushRejected -> "PUSH_REJECTED" to mapOf(
                "results" to JSONArray(error.results.map { r ->
                    JSONObject().apply {
                        put("recordID", r.id)
                        put("table", r.tableName)
                        put("status", r.status)
                    }
                }).toString()
            )
            is SynchroError.NetworkError -> "NETWORK_ERROR" to mapOf(
                "message" to (error.underlying.message ?: "")
            )
            is SynchroError.ServerError -> "SERVER_ERROR" to mapOf(
                "status" to error.status.toString(),
                "message" to error.serverMessage
            )
            is SynchroError.DatabaseError -> "DATABASE_ERROR" to mapOf(
                "message" to (error.underlying.message ?: "")
            )
            is SynchroError.InvalidResponse -> "INVALID_RESPONSE" to mapOf("message" to error.details)
            is SynchroError.AlreadyStarted -> "ALREADY_STARTED" to emptyMap()
            is SynchroError.NotStarted -> "NOT_STARTED" to emptyMap()
        }
    }

    // MARK: - Lifecycle

    @ReactMethod
    override fun initialize(config: ReadableMap, promise: Promise) {
        try {
            val dbPath = config.getString("dbPath") ?: throw IllegalArgumentException("Missing dbPath")
            val serverURL = config.getString("serverURL") ?: throw IllegalArgumentException("Missing serverURL")
            val clientID = config.getString("clientID") ?: throw IllegalArgumentException("Missing clientID")
            val platform = config.getString("platform") ?: "android"
            val appVersion = config.getString("appVersion") ?: throw IllegalArgumentException("Missing appVersion")
            val syncInterval = if (config.hasKey("syncInterval")) config.getDouble("syncInterval") else 30.0
            val pushDebounce = if (config.hasKey("pushDebounce")) config.getDouble("pushDebounce") else 0.5
            val maxRetryAttempts = if (config.hasKey("maxRetryAttempts")) config.getInt("maxRetryAttempts") else 5
            val pullPageSize = if (config.hasKey("pullPageSize")) config.getInt("pullPageSize") else 100
            val pushBatchSize = if (config.hasKey("pushBatchSize")) config.getInt("pushBatchSize") else 100
            val snapshotPageSize = if (config.hasKey("snapshotPageSize")) config.getInt("snapshotPageSize") else 100
            val rawSeedPath = if (config.hasKey("seedDatabasePath")) config.getString("seedDatabasePath") else null

            // Android bundled assets live inside the APK — they're not on the filesystem.
            // For relative paths, extract from assets to filesDir so the native SDK can read it.
            val seedDatabasePath = rawSeedPath?.let { seedPath ->
                if (java.io.File(seedPath).isAbsolute) {
                    seedPath
                } else {
                    val dest = java.io.File(reactApplicationContext.filesDir, seedPath)
                    if (!dest.exists()) {
                        reactApplicationContext.assets.open(seedPath).use { input ->
                            dest.outputStream().use { output -> input.copyTo(output) }
                        }
                    }
                    dest.absolutePath
                }
            }

            val synchroConfig = SynchroConfig(
                dbPath = dbPath,
                serverURL = serverURL,
                authProvider = {
                    suspendCancellableCoroutine { continuation ->
                        val requestID = UUID.randomUUID().toString()
                        // Store continuation BEFORE emitting event to avoid race condition
                        pendingAuthContinuations[requestID] = continuation
                        continuation.invokeOnCancellation {
                            pendingAuthContinuations.remove(requestID)
                        }
                        val params = Arguments.createMap().apply {
                            putString("requestID", requestID)
                        }
                        emitOnAuthRequest(params)
                    }
                },
                clientID = clientID,
                platform = platform,
                appVersion = appVersion,
                syncInterval = syncInterval,
                pushDebounce = pushDebounce,
                maxRetryAttempts = maxRetryAttempts,
                pullPageSize = pullPageSize,
                pushBatchSize = pushBatchSize,
                snapshotPageSize = snapshotPageSize,
                seedDatabasePath = seedDatabasePath
            )

            client?.close()
            clearRuntimeState()
            client = SynchroClient(synchroConfig, reactApplicationContext)
            wireClientEvents(client!!)
            promise.resolve(null)
        } catch (e: Exception) {
            rejectWithError(promise, e)
        }
    }

    @ReactMethod
    override fun resolveAuthRequest(requestID: String, token: String) {
        pendingAuthContinuations.remove(requestID)?.resume(token) {}
    }

    @ReactMethod
    override fun rejectAuthRequest(requestID: String, error: String) {
        pendingAuthContinuations.remove(requestID)?.cancel(
            Exception(error)
        )
    }

    @ReactMethod
    override fun resolveSnapshotRequest(requestID: String, approved: Boolean) {
        pendingSnapshotContinuations.remove(requestID)?.resume(approved) {}
    }

    @ReactMethod
    override fun close(promise: Promise) {
        try {
            client?.close()
            clearRuntimeState()
            client = null
            promise.resolve(null)
        } catch (e: Exception) {
            rejectWithError(promise, e)
        }
    }

    @ReactMethod
    override fun getPath(promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        promise.resolve(c.path)
    }

    // MARK: - Core SQL

    @ReactMethod
    override fun query(sql: String, paramsJson: String, promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        try {
            val params = parseParams(paramsJson)
            val rows = c.query(sql, params)
            promise.resolve(rowsToJson(rows))
        } catch (e: Exception) {
            rejectWithError(promise, e)
        }
    }

    @ReactMethod
    override fun queryOne(sql: String, paramsJson: String, promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        try {
            val params = parseParams(paramsJson)
            val row = c.queryOne(sql, params)
            promise.resolve(row?.let { rowToJson(it) })
        } catch (e: Exception) {
            rejectWithError(promise, e)
        }
    }

    @ReactMethod
    override fun execute(sql: String, paramsJson: String, promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        try {
            val params = parseParams(paramsJson)
            val result = c.execute(sql, params)
            val map = Arguments.createMap().apply {
                putInt("rowsAffected", result.rowsAffected)
            }
            promise.resolve(map)
        } catch (e: Exception) {
            rejectWithError(promise, e)
        }
    }

    @ReactMethod
    override fun executeBatch(statementsJson: String, promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        try {
            val array = JSONArray(statementsJson)
            val statements = (0 until array.length()).map { i ->
                val obj = array.getJSONObject(i)
                val sql = obj.getString("sql")
                val params = obj.optJSONArray("params")?.let { parseJsonArray(it) } ?: emptyArray()
                SQLStatement(sql, params)
            }
            val total = c.executeBatch(statements)
            val map = Arguments.createMap().apply {
                putInt("totalRowsAffected", total)
            }
            promise.resolve(map)
        } catch (e: Exception) {
            rejectWithError(promise, e)
        }
    }

    // MARK: - Transactions

    private class TransactionSession {
        val operations = Channel<TransactionOp>(Channel.RENDEZVOUS)
    }

    private sealed class TransactionOp {
        data class Query(val sql: String, val params: Array<Any?>, val deferred: CompletableDeferred<String>) : TransactionOp()
        data class QueryOne(val sql: String, val params: Array<Any?>, val deferred: CompletableDeferred<String?>) : TransactionOp()
        data class Execute(val sql: String, val params: Array<Any?>, val deferred: CompletableDeferred<WritableMap>) : TransactionOp()
        class Commit(val deferred: CompletableDeferred<Unit>) : TransactionOp()
        class Rollback(val deferred: CompletableDeferred<Unit>) : TransactionOp()
    }

    private class TransactionRollbackException : Exception("rollback")

    @ReactMethod
    override fun beginWriteTransaction(promise: Promise) {
        beginTransaction(isWrite = true, promise = promise)
    }

    @ReactMethod
    override fun beginReadTransaction(promise: Promise) {
        beginTransaction(isWrite = false, promise = promise)
    }

    private fun beginTransaction(isWrite: Boolean, promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }

        val txID = UUID.randomUUID().toString()
        val session = TransactionSession()
        sessions[txID] = session

        scope.launch {
            try {
                val txBlock: (android.database.sqlite.SQLiteDatabase) -> Unit = { db ->
                    promise.resolve(txID)
                    runBlocking {
                        withTimeout(5000) {
                            transactionLoop@ for (op in session.operations) {
                                when (op) {
                                    is TransactionOp.Query -> {
                                        try {
                                            val rows = queryOnDb(db, op.sql, op.params)
                                            op.deferred.complete(rowsToJson(rows))
                                        } catch (e: Exception) {
                                            op.deferred.completeExceptionally(e)
                                        }
                                    }
                                    is TransactionOp.QueryOne -> {
                                        try {
                                            val rows = queryOnDb(db, op.sql, op.params)
                                            op.deferred.complete(if (rows.isNotEmpty()) rowToJson(rows[0]) else null)
                                        } catch (e: Exception) {
                                            op.deferred.completeExceptionally(e)
                                        }
                                    }
                                    is TransactionOp.Execute -> {
                                        try {
                                            db.execSQL(op.sql, op.params)
                                            val cursor = db.rawQuery("SELECT changes() as c", null)
                                            val count = cursor.use {
                                                if (it.moveToFirst()) it.getInt(0) else 0
                                            }
                                            val map = Arguments.createMap().apply {
                                                putInt("rowsAffected", count)
                                            }
                                            op.deferred.complete(map)
                                        } catch (e: Exception) {
                                            op.deferred.completeExceptionally(e)
                                        }
                                    }
                                    is TransactionOp.Commit -> {
                                        op.deferred.complete(Unit)
                                        session.operations.close()
                                        break@transactionLoop
                                    }
                                    is TransactionOp.Rollback -> {
                                        op.deferred.complete(Unit)
                                        session.operations.close()
                                        throw TransactionRollbackException()
                                    }
                                }
                            }
                        }
                    }
                }

                if (isWrite) {
                    c.writeTransaction { db -> txBlock(db) }
                } else {
                    c.readTransaction { db -> txBlock(db) }
                }
            } catch (_: TimeoutCancellationException) {
                // Auto-rollback on timeout
            } catch (_: TransactionRollbackException) {
                // Intentional rollback
            } catch (_: Exception) {
                // Transaction ended
            }
            sessions.remove(txID)
        }
    }

    private fun queryOnDb(db: android.database.sqlite.SQLiteDatabase, sql: String, params: Array<Any?>): List<Row> {
        val stringParams = params.map { it?.toString() }.toTypedArray()
        val cursor = db.rawQuery(sql, stringParams)
        val rows = mutableListOf<Row>()
        cursor.use {
            while (it.moveToNext()) {
                val row = mutableMapOf<String, Any?>()
                for (i in 0 until it.columnCount) {
                    val name = it.getColumnName(i)
                    row[name] = when (it.getType(i)) {
                        android.database.Cursor.FIELD_TYPE_NULL -> null
                        android.database.Cursor.FIELD_TYPE_INTEGER -> it.getLong(i)
                        android.database.Cursor.FIELD_TYPE_FLOAT -> it.getDouble(i)
                        android.database.Cursor.FIELD_TYPE_BLOB -> it.getBlob(i)
                        else -> it.getString(i)
                    }
                }
                rows.add(row)
            }
        }
        return rows
    }

    @ReactMethod
    override fun txQuery(txID: String, sql: String, paramsJson: String, promise: Promise) {
        val session = sessions[txID] ?: run {
            promise.reject("TRANSACTION_TIMEOUT", "Transaction not found or expired")
            return
        }
        scope.launch {
            try {
                val params = parseParams(paramsJson)
                val deferred = CompletableDeferred<String>()
                session.operations.send(TransactionOp.Query(sql, params, deferred))
                promise.resolve(deferred.await())
            } catch (e: Exception) {
                rejectWithError(promise, e)
            }
        }
    }

    @ReactMethod
    override fun txQueryOne(txID: String, sql: String, paramsJson: String, promise: Promise) {
        val session = sessions[txID] ?: run {
            promise.reject("TRANSACTION_TIMEOUT", "Transaction not found or expired")
            return
        }
        scope.launch {
            try {
                val params = parseParams(paramsJson)
                val deferred = CompletableDeferred<String?>()
                session.operations.send(TransactionOp.QueryOne(sql, params, deferred))
                promise.resolve(deferred.await())
            } catch (e: Exception) {
                rejectWithError(promise, e)
            }
        }
    }

    @ReactMethod
    override fun txExecute(txID: String, sql: String, paramsJson: String, promise: Promise) {
        val session = sessions[txID] ?: run {
            promise.reject("TRANSACTION_TIMEOUT", "Transaction not found or expired")
            return
        }
        scope.launch {
            try {
                val params = parseParams(paramsJson)
                val deferred = CompletableDeferred<WritableMap>()
                session.operations.send(TransactionOp.Execute(sql, params, deferred))
                promise.resolve(deferred.await())
            } catch (e: Exception) {
                rejectWithError(promise, e)
            }
        }
    }

    @ReactMethod
    override fun commitTransaction(txID: String, promise: Promise) {
        val session = sessions[txID] ?: run {
            promise.reject("TRANSACTION_TIMEOUT", "Transaction not found or expired")
            return
        }
        scope.launch {
            try {
                val deferred = CompletableDeferred<Unit>()
                session.operations.send(TransactionOp.Commit(deferred))
                deferred.await()
                promise.resolve(null)
            } catch (e: Exception) {
                rejectWithError(promise, e)
            }
        }
    }

    @ReactMethod
    override fun rollbackTransaction(txID: String, promise: Promise) {
        val session = sessions[txID] ?: run {
            promise.resolve(null) // Already gone
            return
        }
        scope.launch {
            try {
                val deferred = CompletableDeferred<Unit>()
                session.operations.send(TransactionOp.Rollback(deferred))
                deferred.await()
                promise.resolve(null)
            } catch (e: Exception) {
                promise.resolve(null) // Best-effort
            }
        }
    }

    // MARK: - Schema

    @ReactMethod
    override fun createTable(name: String, columnsJson: String, optionsJson: String?, promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        try {
            val columns = parseColumns(columnsJson)
            val options = optionsJson?.let { parseTableOptions(it) }
            c.createTable(name, columns, options)
            promise.resolve(null)
        } catch (e: Exception) {
            rejectWithError(promise, e)
        }
    }

    @ReactMethod
    override fun alterTable(name: String, columnsJson: String, promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        try {
            val columns = parseColumns(columnsJson)
            c.alterTable(name, columns)
            promise.resolve(null)
        } catch (e: Exception) {
            rejectWithError(promise, e)
        }
    }

    @ReactMethod
    override fun createIndex(table: String, columns: ReadableArray, unique: Boolean, promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        try {
            val cols = (0 until columns.size()).mapNotNull { columns.getString(it) }
            c.createIndex(table, cols, unique)
            promise.resolve(null)
        } catch (e: Exception) {
            rejectWithError(promise, e)
        }
    }

    // MARK: - Observation

    @ReactMethod
    override fun addChangeObserver(observerID: String, tables: ReadableArray, promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        val tableList = (0 until tables.size()).mapNotNull { tables.getString(it) }
        val cancellable = c.onChange(tableList) {
            val params = Arguments.createMap().apply {
                putString("observerID", observerID)
            }
            emitOnChange(params)
        }
        observers[observerID] = cancellable
        promise.resolve(null)
    }

    @ReactMethod
    override fun addQueryObserver(
        observerID: String,
        sql: String,
        paramsJson: String,
        tables: ReadableArray,
        promise: Promise
    ) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        try {
            val params = parseParams(paramsJson)
            val tableList = (0 until tables.size()).mapNotNull { tables.getString(it) }
            val cancellable = c.watch(sql, params, tableList) { rows ->
                val eventParams = Arguments.createMap().apply {
                    putString("observerID", observerID)
                    putString("rowsJson", rowsToJson(rows))
                }
                emitOnQueryResult(eventParams)
            }
            observers[observerID] = cancellable
            promise.resolve(null)
        } catch (e: Exception) {
            rejectWithError(promise, e)
        }
    }

    @ReactMethod
    override fun removeObserver(observerID: String, promise: Promise) {
        observers.remove(observerID)?.cancel()
        promise.resolve(null)
    }

    // MARK: - WAL / Sync

    @ReactMethod
    override fun checkpoint(mode: String, promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        try {
            val checkpointMode = when (mode) {
                "full" -> CheckpointMode.FULL
                "restart" -> CheckpointMode.RESTART
                "truncate" -> CheckpointMode.TRUNCATE
                else -> CheckpointMode.PASSIVE
            }
            c.checkpoint(checkpointMode)
            promise.resolve(null)
        } catch (e: Exception) {
            rejectWithError(promise, e)
        }
    }

    @ReactMethod
    override fun start(promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        scope.launch {
            try {
                c.start()
                promise.resolve(null)
            } catch (e: Exception) {
                rejectWithError(promise, e)
            }
        }
    }

    @ReactMethod
    override fun stop(promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        try {
            c.stop()
            promise.resolve(null)
        } catch (e: Exception) {
            rejectWithError(promise, e)
        }
    }

    @ReactMethod
    override fun syncNow(promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        scope.launch {
            try {
                c.syncNow()
                promise.resolve(null)
            } catch (e: Exception) {
                rejectWithError(promise, e)
            }
        }
    }

    @ReactMethod
    override fun pendingChangeCount(promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        try {
            promise.resolve(c.pendingChangeCount())
        } catch (e: Exception) {
            rejectWithError(promise, e)
        }
    }

    // Event listener registration (required by RN)
    @ReactMethod
    override fun addListener(eventName: String) { /* no-op */ }

    @ReactMethod
    override fun removeListeners(count: Double) { /* no-op */ }

    // MARK: - Helpers

    private fun wireClientEvents(client: SynchroClient) {
        statusSubscription?.cancel()
        conflictSubscription?.cancel()
        snapshotSubscription?.cancel()

        statusSubscription = client.onStatusChange { status ->
            val params = Arguments.createMap().apply {
                when (status) {
                    is SyncStatus.Idle -> {
                        putString("status", "idle")
                        putNull("retryAt")
                    }
                    is SyncStatus.Syncing -> {
                        putString("status", "syncing")
                        putNull("retryAt")
                    }
                    is SyncStatus.Stopped -> {
                        putString("status", "stopped")
                        putNull("retryAt")
                    }
                    is SyncStatus.Error -> {
                        putString("status", "error")
                        putString("retryAt", status.retryAt?.toString())
                    }
                }
            }
            emitOnStatusChange(params)
        }

        conflictSubscription = client.onConflict { event ->
            val params = Arguments.createMap().apply {
                putString("table", event.table)
                putString("recordID", event.recordID)
                putString("clientDataJson", event.clientData?.let { anyCodableMapToJson(it) })
                putString("serverDataJson", event.serverData?.let { anyCodableMapToJson(it) })
            }
            emitOnConflict(params)
        }

        snapshotSubscription = client.onSnapshotRequired {
            suspendCancellableCoroutine { continuation ->
                val requestID = UUID.randomUUID().toString()
                pendingSnapshotContinuations[requestID] = continuation
                continuation.invokeOnCancellation {
                    pendingSnapshotContinuations.remove(requestID)
                }
                val params = Arguments.createMap().apply {
                    putString("requestID", requestID)
                }
                emitOnSnapshotRequired(params)
            }
        }
    }

    private fun clearRuntimeState() {
        statusSubscription?.cancel()
        conflictSubscription?.cancel()
        snapshotSubscription?.cancel()
        statusSubscription = null
        conflictSubscription = null
        snapshotSubscription = null
        observers.values.forEach { it.cancel() }
        observers.clear()
        sessions.values.forEach { it.operations.close() }
        sessions.clear()
        pendingAuthContinuations.values.forEach { it.cancel(CancellationException("client closed")) }
        pendingAuthContinuations.clear()
        pendingSnapshotContinuations.values.forEach { it.cancel(CancellationException("client closed")) }
        pendingSnapshotContinuations.clear()
    }

    private fun anyCodableMapToJson(value: Map<String, AnyCodable>): String {
        val obj = JSONObject()
        value.forEach { (key, anyCodable) ->
            obj.put(key, anyCodableToJsonValue(anyCodable.value))
        }
        return obj.toString()
    }

    private fun anyCodableToJsonValue(value: Any?): Any? = when (value) {
        null -> JSONObject.NULL
        is Boolean, is Number, is String -> value
        is List<*> -> JSONArray().apply {
            value.forEach { put(anyCodableToJsonValue(it)) }
        }
        is Map<*, *> -> JSONObject().apply {
            value.forEach { (k, v) ->
                put(k.toString(), anyCodableToJsonValue(v))
            }
        }
        is AnyCodable -> anyCodableToJsonValue(value.value)
        else -> value.toString()
    }

    private fun parseParams(json: String): Array<Any?> {
        val array = JSONArray(json)
        return Array(array.length()) { i ->
            val v = array.get(i)
            if (v == JSONObject.NULL) null else v
        }
    }

    private fun parseJsonArray(array: JSONArray): Array<Any?> {
        return Array(array.length()) { i ->
            val v = array.get(i)
            if (v == JSONObject.NULL) null else v
        }
    }

    private fun parseColumns(json: String): List<ColumnDef> {
        val array = JSONArray(json)
        return (0 until array.length()).map { i ->
            val obj = array.getJSONObject(i)
            ColumnDef(
                name = obj.getString("name"),
                type = obj.getString("type"),
                nullable = obj.optBoolean("nullable", true),
                primaryKey = obj.optBoolean("primaryKey", false),
                defaultValue = if (obj.has("defaultValue")) obj.getString("defaultValue") else null
            )
        }
    }

    private fun parseTableOptions(json: String): TableOptions {
        val obj = JSONObject(json)
        return TableOptions(
            ifNotExists = obj.optBoolean("ifNotExists", true),
            withoutRowid = obj.optBoolean("withoutRowid", false)
        )
    }

    private fun rowToJson(row: Row): String {
        val obj = JSONObject()
        for ((k, v) in row) {
            obj.put(k, v ?: JSONObject.NULL)
        }
        return obj.toString()
    }

    private fun rowsToJson(rows: List<Row>): String {
        val array = JSONArray()
        for (row in rows) {
            val obj = JSONObject()
            for ((k, v) in row) {
                obj.put(k, v ?: JSONObject.NULL)
            }
            array.put(obj)
        }
        return array.toString()
    }
}
