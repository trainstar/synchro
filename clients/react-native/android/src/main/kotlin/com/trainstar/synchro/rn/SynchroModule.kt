package com.trainstar.synchro.rn

import android.database.Cursor
import android.database.sqlite.SQLiteCursor
import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteProgram
import android.util.Base64
import com.facebook.react.bridge.*
import com.trainstar.synchro.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.double
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.long
import kotlinx.serialization.json.longOrNull
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
    private var statusSubscription: Cancellable? = null
    private var conflictSubscription: Cancellable? = null

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
            is SynchroError.PushRejected -> "PUSH_REJECTED" to mapOf(
                "results" to rejectedMutationsJson(error.results)
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
    override fun query(sql: String, params: ReadableArray, promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        try {
            val rows = c.query(sql, parseParams(params))
            promise.resolve(rowsToWritableArray(rows))
        } catch (e: Exception) {
            rejectWithError(promise, e)
        }
    }

    @ReactMethod
    override fun queryOne(sql: String, params: ReadableArray, promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        try {
            val row = c.queryOne(sql, parseParams(params))
            promise.resolve(row?.let { rowToWritableMap(it) })
        } catch (e: Exception) {
            rejectWithError(promise, e)
        }
    }

    @ReactMethod
    override fun execute(sql: String, params: ReadableArray, promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        try {
            val result = c.execute(sql, parseParams(params))
            val map = Arguments.createMap().apply {
                putInt("rowsAffected", result.rowsAffected)
            }
            promise.resolve(map)
        } catch (e: Exception) {
            rejectWithError(promise, e)
        }
    }

    @ReactMethod
    override fun executeBatch(statements: ReadableArray, promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        try {
            val nativeStatements = (0 until statements.size()).map { i ->
                val item = statements.getMap(i) ?: throw IllegalArgumentException("Invalid SQL statement at index $i")
                val sql = item.getString("sql") ?: throw IllegalArgumentException("Missing SQL at index $i")
                val params = if (item.hasKey("params")) item.getArray("params") else null
                SQLStatement(sql, params?.let { parseParams(it) } ?: emptyArray())
            }
            val total = c.executeBatch(nativeStatements)
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
        data class Query(val sql: String, val params: Array<Any?>, val deferred: CompletableDeferred<WritableArray>) : TransactionOp()
        data class QueryOne(val sql: String, val params: Array<Any?>, val deferred: CompletableDeferred<WritableMap?>) : TransactionOp()
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
            var finalDeferred: CompletableDeferred<Unit>? = null
            try {
                val txBlock: (SQLiteDatabase) -> Unit = { db ->
                    promise.resolve(txID)
                    runBlocking {
                        withTimeout(5000) {
                            transactionLoop@ for (op in session.operations) {
                                when (op) {
                                    is TransactionOp.Query -> {
                                        try {
                                            val rows = queryOnDb(db, op.sql, op.params)
                                            op.deferred.complete(rowsToWritableArray(rows))
                                        } catch (e: Exception) {
                                            op.deferred.completeExceptionally(e)
                                        }
                                    }
                                    is TransactionOp.QueryOne -> {
                                        try {
                                            val rows = queryOnDb(db, op.sql, op.params)
                                            op.deferred.complete(rows.firstOrNull()?.let { rowToWritableMap(it) })
                                        } catch (e: Exception) {
                                            op.deferred.completeExceptionally(e)
                                        }
                                    }
                                    is TransactionOp.Execute -> {
                                        try {
                                            executeOnDb(db, op.sql, op.params)
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
                                        finalDeferred = op.deferred
                                        session.operations.close()
                                        break@transactionLoop
                                    }
                                    is TransactionOp.Rollback -> {
                                        finalDeferred = op.deferred
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
                finalDeferred?.complete(Unit)
            } catch (e: TimeoutCancellationException) {
                finalDeferred?.completeExceptionally(e)
            } catch (_: TransactionRollbackException) {
                finalDeferred?.complete(Unit)
            } catch (e: Exception) {
                finalDeferred?.completeExceptionally(e)
            }
            sessions.remove(txID)
        }
    }

    private fun queryOnDb(db: SQLiteDatabase, sql: String, params: Array<Any?>): List<Row> {
        val cursor = db.rawQueryWithFactory({ _, driver, editTable, query ->
            bindStatementParams(query, params)
            SQLiteCursor(driver, editTable, query)
        }, sql, emptyArray(), "")
        val rows = mutableListOf<Row>()
        cursor.use {
            while (it.moveToNext()) {
                val row = mutableMapOf<String, Any?>()
                for (i in 0 until it.columnCount) {
                    val name = it.getColumnName(i)
                    row[name] = when (it.getType(i)) {
                        Cursor.FIELD_TYPE_NULL -> null
                        Cursor.FIELD_TYPE_INTEGER -> it.getLong(i)
                        Cursor.FIELD_TYPE_FLOAT -> it.getDouble(i)
                        Cursor.FIELD_TYPE_BLOB -> it.getBlob(i)
                        else -> it.getString(i)
                    }
                }
                rows.add(row)
            }
        }
        return rows
    }

    private fun executeOnDb(db: SQLiteDatabase, sql: String, params: Array<Any?>) {
        val stmt = db.compileStatement(sql)
        try {
            bindStatementParams(stmt, params)
            stmt.executeUpdateDelete()
        } finally {
            stmt.close()
        }
    }

    @ReactMethod
    override fun txQuery(txID: String, sql: String, params: ReadableArray, promise: Promise) {
        val session = sessions[txID] ?: run {
            promise.reject("TRANSACTION_TIMEOUT", "Transaction not found or expired")
            return
        }
        scope.launch {
            try {
                val deferred = CompletableDeferred<WritableArray>()
                session.operations.send(TransactionOp.Query(sql, parseParams(params), deferred))
                promise.resolve(deferred.await())
            } catch (e: Exception) {
                rejectWithError(promise, e)
            }
        }
    }

    @ReactMethod
    override fun txQueryOne(txID: String, sql: String, params: ReadableArray, promise: Promise) {
        val session = sessions[txID] ?: run {
            promise.reject("TRANSACTION_TIMEOUT", "Transaction not found or expired")
            return
        }
        scope.launch {
            try {
                val deferred = CompletableDeferred<WritableMap?>()
                session.operations.send(TransactionOp.QueryOne(sql, parseParams(params), deferred))
                promise.resolve(deferred.await())
            } catch (e: Exception) {
                rejectWithError(promise, e)
            }
        }
    }

    @ReactMethod
    override fun txExecute(txID: String, sql: String, params: ReadableArray, promise: Promise) {
        val session = sessions[txID] ?: run {
            promise.reject("TRANSACTION_TIMEOUT", "Transaction not found or expired")
            return
        }
        scope.launch {
            try {
                val deferred = CompletableDeferred<WritableMap>()
                session.operations.send(TransactionOp.Execute(sql, parseParams(params), deferred))
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
        params: ReadableArray,
        tables: ReadableArray,
        promise: Promise
    ) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        try {
            val nativeParams = parseParams(params)
            val tableList = (0 until tables.size()).mapNotNull { tables.getString(it) }
            val cancellable = c.watch(sql, nativeParams, tableList) { rows ->
                val eventParams = Arguments.createMap().apply {
                    putString("observerID", observerID)
                    putArray("rows", rowsToWritableArray(rows))
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
    }

    private fun clearRuntimeState() {
        statusSubscription?.cancel()
        conflictSubscription?.cancel()
        statusSubscription = null
        conflictSubscription = null
        observers.values.forEach { it.cancel() }
        observers.clear()
        sessions.values.forEach { it.operations.close() }
        sessions.clear()
        pendingAuthContinuations.values.forEach { it.cancel(CancellationException("client closed")) }
        pendingAuthContinuations.clear()
    }

    private fun rejectedMutationsJson(results: List<RejectedMutation>): String {
        return JSONArray(results.map { result ->
            JSONObject().apply {
                put("mutationID", result.mutationID)
                put("table", result.table)
                put("pk", jsonObjectToJsonObject(result.pk))
                put("status", mutationStatusWireValue(result.status))
                put("code", mutationRejectionCodeWireValue(result.code))
                put("message", result.message ?: JSONObject.NULL)
                put("serverRow", result.serverRow?.let { jsonObjectToJsonObject(it) } ?: JSONObject.NULL)
                put("serverVersion", result.serverVersion ?: JSONObject.NULL)
            }
        }).toString()
    }

    private fun mutationStatusWireValue(status: MutationStatus): String = when (status) {
        MutationStatus.APPLIED -> "applied"
        MutationStatus.CONFLICT -> "conflict"
        MutationStatus.REJECTED_TERMINAL -> "rejected_terminal"
        MutationStatus.REJECTED_RETRYABLE -> "rejected_retryable"
    }

    private fun mutationRejectionCodeWireValue(code: MutationRejectionCode): String = when (code) {
        MutationRejectionCode.VERSION_CONFLICT -> "version_conflict"
        MutationRejectionCode.POLICY_REJECTED -> "policy_rejected"
        MutationRejectionCode.VALIDATION_FAILED -> "validation_failed"
        MutationRejectionCode.TABLE_NOT_SYNCED -> "table_not_synced"
        MutationRejectionCode.UNKNOWN_SCOPE_EFFECT -> "unknown_scope_effect"
        MutationRejectionCode.SERVER_RETRYABLE -> "server_retryable"
    }

    private fun jsonObjectToJsonObject(value: JsonObject): JSONObject {
        val obj = JSONObject()
        value.forEach { (key, element) ->
            obj.put(key, jsonElementToJsonValue(element))
        }
        return obj
    }

    private fun jsonElementToJsonValue(value: JsonElement): Any? = when (value) {
        JsonNull -> JSONObject.NULL
        is JsonPrimitive -> when {
            value.isString -> value.content
            value.booleanOrNull != null -> value.boolean
            value.longOrNull != null -> value.long
            value.doubleOrNull != null -> value.double
            else -> value.content
        }
        is JsonObject -> jsonObjectToJsonObject(value)
        is JsonArray -> JSONArray().apply {
            value.forEach { put(jsonElementToJsonValue(it)) }
        }
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

    private fun parseParams(params: ReadableArray): Array<Any?> {
        return Array(params.size()) { i ->
            when (params.getType(i)) {
                ReadableType.Null -> null
                ReadableType.Boolean -> params.getBoolean(i)
                ReadableType.Number -> sqliteNumberParam(params.getDouble(i), i)
                ReadableType.String -> params.getString(i)
                else -> throw IllegalArgumentException("Unsupported SQL bind value at index $i")
            }
        }
    }

    private fun sqliteNumberParam(value: Double, index: Int): Any {
        if (!value.isFinite()) {
            throw IllegalArgumentException("Invalid SQL number bind value at index $index")
        }
        return if (value % 1.0 == 0.0 && value >= Long.MIN_VALUE.toDouble() && value <= Long.MAX_VALUE.toDouble()) {
            value.toLong()
        } else {
            value
        }
    }

    private fun bindStatementParams(stmt: SQLiteProgram, params: Array<Any?>) {
        for (i in params.indices) {
            val bindIndex = i + 1
            when (val value = params[i]) {
                null -> stmt.bindNull(bindIndex)
                is Long -> stmt.bindLong(bindIndex, value)
                is Int -> stmt.bindLong(bindIndex, value.toLong())
                is Double -> stmt.bindDouble(bindIndex, value)
                is Float -> stmt.bindDouble(bindIndex, value.toDouble())
                is ByteArray -> stmt.bindBlob(bindIndex, value)
                is Boolean -> stmt.bindLong(bindIndex, if (value) 1L else 0L)
                is String -> stmt.bindString(bindIndex, value)
                else -> throw IllegalArgumentException(
                    "Unsupported SQL bind value at index $i: ${value::class.java.name}"
                )
            }
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

    private fun rowToWritableMap(row: Row): WritableMap {
        val map = Arguments.createMap()
        for ((k, v) in row) {
            putSQLiteValue(map, k, v)
        }
        return map
    }

    private fun rowsToWritableArray(rows: List<Row>): WritableArray {
        val array = Arguments.createArray()
        for (row in rows) {
            array.pushMap(rowToWritableMap(row))
        }
        return array
    }

    private fun putSQLiteValue(map: WritableMap, key: String, value: Any?) {
        when (value) {
            null -> map.putNull(key)
            is Boolean -> map.putBoolean(key, value)
            is Int -> map.putInt(key, value)
            is Long -> map.putDouble(key, value.toDouble())
            is Float -> map.putDouble(key, value.toDouble())
            is Double -> map.putDouble(key, value)
            is String -> map.putString(key, value)
            is ByteArray -> map.putString(key, Base64.encodeToString(value, Base64.NO_WRAP))
            else -> map.putString(key, value.toString())
        }
    }
}
