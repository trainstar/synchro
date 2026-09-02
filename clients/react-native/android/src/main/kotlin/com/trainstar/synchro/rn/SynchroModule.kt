// The bridge is the React Native conformance facade. Its transport
// observation wiring consumes the Kotlin proof API deliberately.
@file:OptIn(com.trainstar.synchro.inspection.SynchroProofApi::class)

package com.trainstar.synchro.rn

import android.util.Base64
import android.os.Process
import com.facebook.react.bridge.*
import com.trainstar.synchro.*
import com.trainstar.synchro.inspection.SynchroInspection
import com.trainstar.synchro.inspection.TransportObservationCollector
import com.trainstar.synchro.inspection.TransportObservationSnapshot
import com.trainstar.synchro.inspection.TransportOperationClass
import com.trainstar.synchro.inspection.withTransportObservation
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
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
import java.io.File
import java.security.MessageDigest
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

@OptIn(ExperimentalCoroutinesApi::class, ExperimentalSerializationApi::class)
class SynchroModule(reactContext: ReactApplicationContext) :
    NativeSynchroSpec(reactContext) {

    private companion object {
        const val MAX_SAFE_INTEGER = 9_007_199_254_740_991L
        val BASE64_URL_FLAGS = Base64.URL_SAFE or Base64.NO_WRAP or Base64.NO_PADDING
        val BASE64_URL_PATTERN = Regex("^[A-Za-z0-9_-]*$")
        val INT64_PATTERN = Regex("^(?:0|-?[1-9][0-9]*)$")
    }

    private var client: SynchroClient? = null
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val lifecycleMutex = Mutex()
    private val transactionLock = Any()
    private val sessions = ConcurrentHashMap<String, TransactionSession>()
    private var acceptingTransactions = false
    private val observers = ConcurrentHashMap<String, Cancellable>()
    private val pendingAuthContinuations = ConcurrentHashMap<String, CancellableContinuation<String>>()
    private var statusSubscription: Cancellable? = null
    private var syncEventSubscription: Cancellable? = null
    private var conflictSubscription: Cancellable? = null
    private var transportObservationCollector: TransportObservationCollector? = null

    private fun rejectWithError(promise: Promise, error: Throwable) {
        when (error) {
            is SynchroError -> {
                val (code, userInfo) = mapSynchroError(error)
                val writableMap = Arguments.createMap().apply {
                    userInfo.forEach { (k, v) -> putString(k, v) }
                }
                promise.reject(code, error.message, error, writableMap)
            }
            is TransactionAbortedException -> promise.reject(
                "NOT_CONNECTED",
                "Client closed during transaction",
                error,
            )
            is TransactionExpiredException -> promise.reject(
                "TRANSACTION_TIMEOUT",
                "Transaction timed out due to inactivity",
                error,
            )
            is TimeoutCancellationException -> promise.reject(
                "TRANSACTION_TIMEOUT",
                "Transaction timed out due to inactivity",
                error,
            )
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
            is SynchroError.UnsupportedSchema -> "UNSUPPORTED_SCHEMA" to mapOf(
                "reason" to error.reason.name.lowercase()
            )
            is SynchroError.DatabaseError -> "DATABASE_ERROR" to mapOf(
                "message" to (error.underlying.message ?: "")
            )
            is SynchroError.InvalidResponse -> "INVALID_RESPONSE" to mapOf("message" to error.details)
            is SynchroError.BlockingFailure -> "SYNC_BLOCKED" to mapOf(
                "failure" to failureJson(error.failure).toString(),
                "failureCode" to error.failure.code.wireName,
                "failureOperation" to error.failure.operation.wireName,
                "failureRetryable" to error.failure.retryable.toString(),
                "failureMessage" to error.failure.message,
                "failureRecoveryAction" to error.failure.recoveryAction.wireName,
            )
            is SynchroError.InvalidStateTransition -> "INVALID_STATE_TRANSITION" to mapOf(
                "from" to error.from.wireName,
                "to" to error.to.wireName,
            )
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
            val transportCapacity = if (config.hasKey("transportObservationCapacity")) {
                config.getInt("transportObservationCapacity")
            } else {
                0
            }
            val requireNewDatabase = config.getBoolean("requireNewDatabase")
            require(transportCapacity in 0..512) { "Invalid transport observation capacity" }
            val nextTransportObservationCollector = if (transportCapacity == 0) {
                null
            } else {
                TransportObservationCollector(transportCapacity)
            }

            val seedDatabasePath = rawSeedPath?.let { seedPath ->
                if (File(seedPath).isAbsolute) {
                    seedPath
                } else {
                    extractSeedAsset(seedPath)
                }
            }

            val baseConfig = SynchroConfig(
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
                seedDatabasePath = seedDatabasePath,
            )
            val synchroConfig = nextTransportObservationCollector?.let(baseConfig::withTransportObservation)
                ?: baseConfig

            scope.launch {
                lifecycleMutex.withLock {
                    try {
                        clearRuntimeState()
                        client?.close()
                        if (requireNewDatabase && reactApplicationContext.getDatabasePath(dbPath).exists()) {
                            promise.reject("INVALID_CONFIG", "Database already exists")
                            return@withLock
                        }
                        val nextClient = SynchroClient(synchroConfig, reactApplicationContext)
                        synchronized(transactionLock) {
                            client = nextClient
                            acceptingTransactions = true
                        }
                        transportObservationCollector = nextTransportObservationCollector
                        wireClientEvents(nextClient)
                        promise.resolve(null)
                    } catch (e: Exception) {
                        nextTransportObservationCollector?.cancelPauseBarrier()
                        if (rawSeedPath != null) {
                            promise.reject("INVALID_SEED", "Seed database failed validation", e)
                        } else {
                            rejectWithError(promise, e)
                        }
                    }
                }
            }
        } catch (e: Exception) {
            promise.reject("INVALID_CONFIG", e.message ?: "Invalid configuration", e)
        }
    }

    private fun extractSeedAsset(assetPath: String): String {
        val cacheDirectory = File(reactApplicationContext.filesDir, "synchro-seed-assets")
        check(cacheDirectory.exists() || cacheDirectory.mkdirs()) {
            "Cannot create the seed asset cache"
        }

        val cacheName = UUID.nameUUIDFromBytes(assetPath.toByteArray(Charsets.UTF_8)).toString()
        val destination = File(cacheDirectory, "$cacheName.db")
        val packagedDigest = reactApplicationContext.assets.open(assetPath).use(::sha256)
        val extractedDigest = if (destination.isFile) {
            destination.inputStream().use(::sha256)
        } else {
            null
        }

        if (extractedDigest == null || !packagedDigest.contentEquals(extractedDigest)) {
            val temporary = File(cacheDirectory, "$cacheName-${UUID.randomUUID()}.tmp")
            try {
                reactApplicationContext.assets.open(assetPath).use { input ->
                    temporary.outputStream().use { output -> input.copyTo(output) }
                }
                if (!temporary.renameTo(destination)) {
                    temporary.copyTo(destination, overwrite = true)
                }
            } finally {
                temporary.delete()
            }
        }

        return destination.absolutePath
    }

    private fun sha256(input: java.io.InputStream): ByteArray {
        val digest = MessageDigest.getInstance("SHA-256")
        val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
        while (true) {
            val count = input.read(buffer)
            if (count < 0) break
            digest.update(buffer, 0, count)
        }
        return digest.digest()
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
        scope.launch {
            lifecycleMutex.withLock {
                try {
                    clearRuntimeState()
                    client?.close()
                    synchronized(transactionLock) {
                        client = null
                    }
                    promise.resolve(null)
                } catch (e: Exception) {
                    rejectWithError(promise, e)
                }
            }
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

    private class TransactionSession(val isWrite: Boolean) {
        val operations = Channel<TransactionOp>(Channel.RENDEZVOUS)
        lateinit var job: Job
        private var abortCause: Throwable? = null
        private var terminal = false

        @Synchronized
        fun abort(cause: Throwable) {
            if (terminal) return
            terminal = true
            abortCause = cause
            operations.close(cause)
        }

        @Synchronized
        fun completeNormally(): Boolean {
            if (terminal) return false
            terminal = true
            operations.close()
            return true
        }

        @Synchronized
        fun <T> complete(deferred: CompletableDeferred<T>, value: T) {
            val cause = abortCause
            if (cause == null) {
                deferred.complete(value)
            } else {
                deferred.completeExceptionally(cause)
            }
        }

        @Synchronized
        fun <T> completeExceptionally(deferred: CompletableDeferred<T>, error: Throwable) {
            deferred.completeExceptionally(abortCause ?: error)
        }
    }

    private sealed class TransactionOp {
        data class Query(val sql: String, val params: Array<Any?>, val deferred: CompletableDeferred<WritableArray>) : TransactionOp()
        data class QueryOne(val sql: String, val params: Array<Any?>, val deferred: CompletableDeferred<WritableMap?>) : TransactionOp()
        data class Execute(val sql: String, val params: Array<Any?>, val deferred: CompletableDeferred<WritableMap>) : TransactionOp()
        class Commit(val deferred: CompletableDeferred<Unit>) : TransactionOp()
        class Rollback(val deferred: CompletableDeferred<Unit>) : TransactionOp()
    }

    private class TransactionRollbackException(
        val completion: CompletableDeferred<Unit>,
    ) : Exception("rollback")

    private class TransactionAbortedException : Exception("Client closed during transaction")
    private class TransactionExpiredException : Exception("Transaction timed out due to inactivity")

    @ReactMethod
    override fun beginWriteTransaction(promise: Promise) {
        beginTransaction(isWrite = true, promise = promise)
    }

    @ReactMethod
    override fun beginReadTransaction(promise: Promise) {
        beginTransaction(isWrite = false, promise = promise)
    }

    private fun beginTransaction(isWrite: Boolean, promise: Promise) {
        val c = synchronized(transactionLock) {
            client.takeIf { acceptingTransactions }
        } ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }

        val txID = UUID.randomUUID().toString()
        val session = TransactionSession(isWrite)

        val job = scope.launch(start = CoroutineStart.LAZY) {
            var finalDeferred: CompletableDeferred<Unit>? = null
            try {
                if (isWrite) {
                    c.writeTransaction { transaction ->
                        finalDeferred = runTransactionLoop(
                            txID = txID,
                            session = session,
                            promise = promise,
                            query = transaction::query,
                            queryOne = transaction::queryOne,
                            execute = transaction::execute,
                        )
                    }
                } else {
                    c.readTransaction { transaction ->
                        finalDeferred = runTransactionLoop(
                            txID = txID,
                            session = session,
                            promise = promise,
                            query = transaction::query,
                            queryOne = transaction::queryOne,
                            execute = { _, _ ->
                                throw IllegalStateException("read transactions cannot execute SQL")
                            },
                        )
                    }
                }
                finalDeferred?.complete(Unit)
            } catch (e: TimeoutCancellationException) {
                val timeout = TransactionExpiredException()
                session.abort(timeout)
                finalDeferred?.completeExceptionally(timeout)
            } catch (e: TransactionRollbackException) {
                e.completion.complete(Unit)
            } catch (e: Exception) {
                finalDeferred?.completeExceptionally(e)
            } finally {
                session.completeNormally()
                sessions.remove(txID, session)
            }
        }
        session.job = job
        val registered = synchronized(transactionLock) {
            if (acceptingTransactions && client === c) {
                sessions[txID] = session
                true
            } else {
                false
            }
        }
        if (!registered) {
            job.cancel()
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        job.start()
    }

    private fun runTransactionLoop(
        txID: String,
        session: TransactionSession,
        promise: Promise,
        query: (String, Array<out Any?>?) -> List<Row>,
        queryOne: (String, Array<out Any?>?) -> Row?,
        execute: (String, Array<out Any?>?) -> ExecResult,
    ): CompletableDeferred<Unit>? {
        promise.resolve(txID)
        var finalDeferred: CompletableDeferred<Unit>? = null
        runBlocking {
            transactionLoop@ while (true) {
                val result = withTimeout(5000) { session.operations.receiveCatching() }
                if (result.isClosed) {
                    throw result.exceptionOrNull() ?: TransactionAbortedException()
                }
                when (val op = result.getOrThrow()) {
                    is TransactionOp.Query -> {
                        try {
                            session.complete(
                                op.deferred,
                                rowsToWritableArray(query(op.sql, op.params)),
                            )
                        } catch (e: Exception) {
                            session.completeExceptionally(op.deferred, e)
                        }
                    }
                    is TransactionOp.QueryOne -> {
                        try {
                            session.complete(
                                op.deferred,
                                queryOne(op.sql, op.params)?.let(::rowToWritableMap),
                            )
                        } catch (e: Exception) {
                            session.completeExceptionally(op.deferred, e)
                        }
                    }
                    is TransactionOp.Execute -> {
                        try {
                            val result = execute(op.sql, op.params)
                            val map = Arguments.createMap().apply {
                                putInt("rowsAffected", result.rowsAffected)
                            }
                            session.complete(op.deferred, map)
                        } catch (e: Exception) {
                            session.completeExceptionally(op.deferred, e)
                        }
                    }
                    is TransactionOp.Commit -> {
                        finalDeferred = op.deferred
                        if (!session.completeNormally()) throw TransactionAbortedException()
                        break@transactionLoop
                    }
                    is TransactionOp.Rollback -> {
                        finalDeferred = op.deferred
                        if (!session.completeNormally()) throw TransactionAbortedException()
                        throw TransactionRollbackException(op.deferred)
                    }
                }
            }
        }
        return finalDeferred
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
        if (!session.isWrite) {
            promise.reject("DATABASE_ERROR", "Read transactions cannot execute SQL")
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
        scope.launch {
            try {
                c.stop()
                promise.resolve(null)
            } catch (e: Exception) {
                rejectWithError(promise, e)
            }
        }
    }

    @ReactMethod
    override fun enterBackground(promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        try {
            c.onApplicationBackground()
            promise.resolve(null)
        } catch (e: Exception) {
            rejectWithError(promise, e)
        }
    }

    @ReactMethod
    override fun enterForeground(promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        try {
            c.onApplicationForeground()
            promise.resolve(null)
        } catch (e: Exception) {
            rejectWithError(promise, e)
        }
    }

    @ReactMethod
    override fun retryAfterError(promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        scope.launch {
            try {
                c.retry()
                promise.resolve(null)
            } catch (e: Exception) {
                rejectWithError(promise, e)
            }
        }
    }

    @ReactMethod
    override fun resetSchemaAndStart(promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        scope.launch {
            try {
                c.resetSchema()
                promise.resolve(null)
            } catch (e: Exception) {
                rejectWithError(promise, e)
            }
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

    @ReactMethod
    override fun getSyncStatus(promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        try {
            promise.resolve(syncStatusJson(c.getSyncStatus()).toString())
        } catch (e: Exception) {
            rejectWithError(promise, e)
        }
    }

    @ReactMethod
    override fun inspectPendingMutations(promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        try {
            promise.resolve(JSONArray(c.inspectPendingMutations().map(::pendingMutationJson)).toString())
        } catch (e: Exception) {
            rejectWithError(promise, e)
        }
    }

    @ReactMethod
    override fun inspectRetainedMutations(promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        try {
            promise.resolve(JSONArray(c.inspectRetainedMutations().map(::pendingMutationJson)).toString())
        } catch (e: Exception) {
            rejectWithError(promise, e)
        }
    }

    @ReactMethod
    override fun inspectRejectedMutations(promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        try {
            promise.resolve(JSONArray(c.inspectRejectedMutations().map(::rejectedMutationJson)).toString())
        } catch (e: Exception) {
            rejectWithError(promise, e)
        }
    }

    @ReactMethod
    override fun inspectClientState(promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        try {
            val inspection = SynchroInspection(c)
            val capture = inspection.captureState(maximumRecords = 512)
            val schema: Any = capture.schema?.let {
                JSONObject().put("version", it.version).put("hash", it.hash)
            } ?: JSONObject.NULL
            val scopeStates = JSONArray(capture.scopeStates.map { value ->
                JSONObject().apply {
                    put("scope_id", value.scopeID)
                    put("cursor", value.cursor ?: JSONObject.NULL)
                    put("checksum", value.checksum ?: JSONObject.NULL)
                    put("local_checksum", value.localChecksum)
                    put("generation", value.generation)
                }
            })
            val scopeRows = JSONArray(capture.scopeRows.map { value ->
                JSONObject().apply {
                    put("scope_id", value.scopeID)
                    put("table_name", value.tableName)
                    put("record_id", value.recordID)
                    put("checksum", value.checksum)
                    put("generation", value.generation)
                }
            })
            val attempts = JSONArray(capture.rebuildAttempts.map { value ->
                JSONObject().apply {
                    put("scope_id", value.scopeID)
                    put("rebuild_id", value.rebuildID)
                    put("client_generation", value.clientGeneration)
                    put("schema_version", value.schemaVersion)
                    put("schema_hash", value.schemaHash)
                    put("generation", value.generation)
                    put("cursor", value.cursor ?: JSONObject.NULL)
                    put("page_limit", value.pageLimit)
                }
            })
            promise.resolve(JSONObject().apply {
                put("schema", schema)
                put("scope_states", scopeStates)
                put("scope_rows", scopeRows)
                put("rebuild_attempts", attempts)
                put("application_row_count", capture.applicationRowCount)
                put("mutation_ledger_count", capture.mutationLedgerCount)
                put("mutation_outcome_count", capture.mutationOutcomeCount)
                put("sealed_batch_count", capture.sealedBatchCount)
                put("rejected_mutation_count", capture.rejectedMutationCount)
                put("scope_state_count", capture.scopeStateCount)
                put("scope_row_count", capture.scopeRowCount)
                put("provenance_count", capture.provenanceCount)
                put("row_metadata_count", capture.rowMetadataCount)
                put("rebuild_attempt_count", capture.rebuildAttemptCount)
                put("rebuild_receipt_count", capture.rebuildReceiptCount)
                put(
                    "provenance_maintenance_work_cursor",
                    capture.provenanceMaintenanceWorkCursor.toString(),
                )
            }.toString())
        } catch (error: Exception) {
            rejectWithError(promise, error)
        }
    }

    @ReactMethod
    override fun inspectDurableState(tableName: String, recordID: String, promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        try {
            val inspection = SynchroInspection(c)
            val metadataJson: Any = inspection.rowMetadata(tableName, recordID)?.let { value ->
                JSONObject().apply {
                    put("table_name", value.tableName)
                    put("record_id", value.recordID)
                    put("server_version", value.serverVersion)
                    put("row_checksum", value.rowChecksum ?: JSONObject.NULL)
                }
            } ?: JSONObject.NULL
            val receipts = JSONArray(inspection.rebuildReceipts().map { value ->
                JSONObject().apply {
                    put("rebuild_id_fingerprint", value.rebuildIDFingerprint)
                    put("page_count", value.pageCount)
                    put("returned_record_count", value.returnedRecordCount)
                    put("request_chain_expected", JSONArray(value.requestChainExpected))
                    put("request_chain_observed", JSONArray(value.requestChainObserved))
                    put("record_identities_hex", JSONArray(value.recordIdentitiesHex))
                    put("received_row_checksums", JSONArray(value.receivedRowChecksums))
                    put("computed_row_checksums", JSONArray(value.computedRowChecksums))
                    put("computed_scope_checksum", value.computedScopeChecksum ?: JSONObject.NULL)
                    put("final_scope_checksum", value.finalScopeChecksum ?: JSONObject.NULL)
                    put("stored_scope_checksum", value.storedScopeChecksum ?: JSONObject.NULL)
                    put("local_scope_checksum", value.localScopeChecksum ?: JSONObject.NULL)
                }
            })
            promise.resolve(JSONObject().apply {
                put("row_metadata", metadataJson)
                put("rebuild_receipts", receipts)
            }.toString())
        } catch (error: Exception) {
            rejectWithError(promise, error)
        }
    }

    @ReactMethod
    override fun inspectTransportObservations(promise: Promise) {
        val collector = transportObservationCollector ?: run {
            promise.reject("NOT_CONNECTED", "Transport observation is not configured")
            return
        }
        promise.resolve(
            Json { explicitNulls = false }.encodeToString(
                TransportObservationSnapshot.serializer(),
                collector.snapshot(),
            ),
        )
    }

    @ReactMethod
    override fun armTransportPause(operationClass: String, promise: Promise) {
        try {
            val collector = transportObservationCollector ?: error("Transport observation is not configured")
            collector.armPause(transportOperationClass(operationClass))
            promise.resolve(null)
        } catch (error: Exception) {
            rejectWithError(promise, error)
        }
    }

    @ReactMethod
    override fun awaitTransportPause(operationClass: String, timeoutMs: Double, promise: Promise) {
        scope.launch {
            try {
                require(
                    timeoutMs.isFinite() &&
                        timeoutMs >= 1 &&
                        timeoutMs <= 60_000 &&
                        timeoutMs % 1.0 == 0.0,
                ) { "Transport pause timeout is invalid" }
                val collector = transportObservationCollector ?: error("Transport observation is not configured")
                collector.awaitPause(transportOperationClass(operationClass), timeoutMs.toLong())
                promise.resolve(null)
            } catch (error: Exception) {
                rejectWithError(promise, error)
            }
        }
    }

    @ReactMethod
    override fun resumeTransportPause(promise: Promise) {
        try {
            val collector = transportObservationCollector ?: error("Transport observation is not configured")
            collector.resumePause()
            promise.resolve(null)
        } catch (error: Exception) {
            rejectWithError(promise, error)
        }
    }

    @ReactMethod
    override fun getProcessIdentity(promise: Promise) {
        promise.resolve("android-app:${Process.myPid()}")
    }

    @ReactMethod
    override fun clearRejectedMutations(promise: Promise) {
        val c = client ?: run {
            promise.reject("NOT_CONNECTED", "Client not initialized")
            return
        }
        try {
            c.clearRejectedMutations()
            promise.resolve(null)
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
        syncEventSubscription?.cancel()
        conflictSubscription?.cancel()

        statusSubscription = client.onStatusChange { status ->
            emitOnStatusChange(statusPayload(status))
        }

        syncEventSubscription = client.onSyncEvent { event ->
            emitOnSyncEvent(syncEventPayload(event))
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

    private suspend fun clearRuntimeState() {
        statusSubscription?.cancel()
        syncEventSubscription?.cancel()
        conflictSubscription?.cancel()
        statusSubscription = null
        syncEventSubscription = null
        conflictSubscription = null
        transportObservationCollector?.cancelPauseBarrier()
        transportObservationCollector = null
        observers.values.forEach { it.cancel() }
        observers.clear()
        val activeSessions = synchronized(transactionLock) {
            acceptingTransactions = false
            sessions.values.toList().also { sessions.clear() }
        }
        activeSessions.forEach { it.abort(TransactionAbortedException()) }
        activeSessions.map { it.job }.joinAll()
        pendingAuthContinuations.values.forEach { it.cancel(CancellationException("client closed")) }
        pendingAuthContinuations.clear()
    }

    private fun transportOperationClass(value: String): TransportOperationClass =
        TransportOperationClass.fromWire(value) ?: error("transport operation is invalid")

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

    private fun failurePayload(failure: SyncFailure): WritableMap = Arguments.createMap().apply {
        putString("operation", failure.operation.wireName)
        putString("code", failure.code.wireName)
        putBoolean("retryable", failure.retryable)
        putString("message", failure.message)
        putString("recoveryAction", failure.recoveryAction.wireName)
        putMap("metadata", Arguments.createMap().apply {
            failure.metadata.forEach { (key, value) -> putString(key, value) }
        })
    }

    private fun statusPayload(status: SyncStatus): WritableMap = Arguments.createMap().apply {
        putString("status", status.state.wireName)
        putNull("retryAt")
        putNull("operation")
        putNull("failure")
        when (status) {
            is SyncStatus.Uninitialized -> Unit
            is SyncStatus.LocalReady -> Unit
            is SyncStatus.Connecting -> Unit
            is SyncStatus.SchemaApplying -> Unit
            is SyncStatus.Ready -> Unit
            is SyncStatus.Pushing -> Unit
            is SyncStatus.Pulling -> Unit
            is SyncStatus.Rebuilding -> Unit
            is SyncStatus.Backoff -> {
                putString("retryAt", status.retryAt.toString())
                putString("operation", status.operation)
            }
            is SyncStatus.Error -> putMap("failure", failurePayload(status.failure))
            is SyncStatus.Stopped -> Unit
        }
    }

    private fun schemaPayload(schema: SchemaRef): WritableMap = Arguments.createMap().apply {
        putDouble("version", schema.version.toDouble())
        putString("hash", schema.hash)
    }

    private fun syncEventPayload(event: SyncEvent): WritableMap {
        val payload = Arguments.createMap().apply {
            putString("type", "")
            putNull("from")
            putNull("to")
            putNull("operation")
            putNull("attempt")
            putNull("retryAt")
            putNull("source")
            putNull("target")
            putNull("action")
            putNull("mutationID")
            putNull("tableID")
            putNull("mutationStatus")
            putNull("rejectionCode")
            putNull("scopeID")
            putNull("rebuildID")
            putNull("failure")
        }
        when (event) {
            is SyncEvent.StateChanged -> {
                payload.putString("type", "state_changed")
                payload.putString("from", event.change.from.wireName)
                payload.putString("to", event.change.to.wireName)
            }
            is SyncEvent.MutationAccepted -> {
                payload.putString("type", "mutation_accepted")
                payload.putString("mutationID", event.mutation.mutationID)
                payload.putString("tableID", event.mutation.tableID)
                payload.putString("mutationStatus", mutationStatusWireValue(event.mutation.status))
                event.mutation.rejectionCode?.let { code ->
                    payload.putString("rejectionCode", mutationRejectionCodeWireValue(code))
                }
            }
            is SyncEvent.MutationRejected -> {
                payload.putString("type", "mutation_rejected")
                payload.putString("mutationID", event.mutation.mutationID)
                payload.putString("tableID", event.mutation.tableID)
                payload.putString("mutationStatus", mutationStatusWireValue(event.mutation.status))
                event.mutation.rejectionCode?.let { code ->
                    payload.putString("rejectionCode", mutationRejectionCodeWireValue(code))
                }
            }
            is SyncEvent.SchemaApplying -> {
                payload.putString("type", "schema_applying")
                payload.putMap("source", schemaPayload(event.schema.source))
                payload.putMap("target", schemaPayload(event.schema.target))
                payload.putString("action", schemaActionWireValue(event.schema.action))
            }
            is SyncEvent.SchemaApplied -> {
                payload.putString("type", "schema_applied")
                payload.putMap("source", schemaPayload(event.schema.source))
                payload.putMap("target", schemaPayload(event.schema.target))
                payload.putString("action", schemaActionWireValue(event.schema.action))
            }
            is SyncEvent.RebuildRequested -> {
                payload.putString("type", "rebuild_requested")
                payload.putString("scopeID", event.rebuild.scopeID)
                payload.putString("rebuildID", event.rebuild.rebuildID)
            }
            is SyncEvent.RebuildCompleted -> {
                payload.putString("type", "rebuild_completed")
                payload.putString("scopeID", event.rebuild.scopeID)
                payload.putString("rebuildID", event.rebuild.rebuildID)
            }
            is SyncEvent.Backoff -> {
                payload.putString("type", "backoff")
                payload.putString("operation", event.backoff.operation.wireName)
                payload.putDouble("attempt", event.backoff.attempt.toDouble())
                payload.putString("retryAt", event.backoff.retryAt.toString())
            }
            is SyncEvent.Failure -> {
                payload.putString("type", "failure")
                payload.putMap("failure", failurePayload(event.failure))
            }
        }
        return payload
    }

    private fun syncStatusJson(status: SyncStatus): JSONObject = JSONObject().apply {
        put("status", status.state.wireName)
        put("retryAt", JSONObject.NULL)
        put("operation", JSONObject.NULL)
        put("failure", JSONObject.NULL)
        when (status) {
            is SyncStatus.Uninitialized -> Unit
            is SyncStatus.LocalReady -> Unit
            is SyncStatus.Connecting -> Unit
            is SyncStatus.SchemaApplying -> Unit
            is SyncStatus.Ready -> Unit
            is SyncStatus.Pushing -> Unit
            is SyncStatus.Pulling -> Unit
            is SyncStatus.Rebuilding -> Unit
            is SyncStatus.Backoff -> {
                put("retryAt", status.retryAt.toString())
                put("operation", status.operation)
            }
            is SyncStatus.Error -> put("failure", failureJson(status.failure))
            is SyncStatus.Stopped -> Unit
        }
    }

    private fun failureJson(failure: SyncFailure): JSONObject = JSONObject().apply {
        put("operation", failure.operation.wireName)
        put("code", failure.code.wireName)
        put("retryable", failure.retryable)
        put("message", failure.message)
        put("recoveryAction", failure.recoveryAction.wireName)
        put("metadata", JSONObject().apply {
            failure.metadata.forEach { (key, value) -> put(key, value) }
        })
    }

    private fun pendingMutationJson(mutation: PendingMutationInspection): JSONObject = JSONObject().apply {
        put("mutationID", mutation.mutationID)
        put("localOrder", mutation.localOrder)
        put("tableID", mutation.tableID)
        put("tableName", mutation.tableName)
        put("recordID", mutation.recordID)
        put("primaryKeyFieldID", mutation.primaryKeyFieldID)
        put("primaryKeyLogicalType", mutation.primaryKeyLogicalType)
        put("operation", operationWireValue(mutation.operation))
        put("authoredSchema", JSONObject().apply {
            put("version", mutation.authoredSchema.version)
            put("hash", mutation.authoredSchema.hash)
        })
        put("baseVersion", mutation.baseVersion ?: JSONObject.NULL)
        put("clientVersion", mutation.clientVersion)
        put("status", localMutationStatusWireValue(mutation.status))
        put("sourceKind", mutation.sourceKind)
        put("dependsOnMutationID", mutation.dependsOnMutationID ?: JSONObject.NULL)
        put("normalizedMutationID", mutation.normalizedMutationID ?: JSONObject.NULL)
        put("sealedBatchID", mutation.sealedBatchID ?: JSONObject.NULL)
        put("sealedOrdinal", mutation.sealedOrdinal ?: JSONObject.NULL)
        put("authoredFields", JSONArray(mutation.authoredFields.map { field ->
            JSONObject().apply {
                put("fieldID", field.fieldID)
                put("logicalType", field.logicalType)
                put("value", anyCodableToJsonValue(field.value.value))
            }
        }))
    }

    private fun rejectedMutationJson(mutation: RejectedMutationInspection): JSONObject = JSONObject().apply {
        put("mutationID", mutation.mutationID)
        put("tableName", mutation.tableName)
        put("recordID", mutation.recordID)
        put("status", mutationStatusWireValue(mutation.status))
        put("code", mutationRejectionCodeWireValue(mutation.code))
        put("message", mutation.message ?: JSONObject.NULL)
        put("serverRowJSON", mutation.serverRowJSON ?: JSONObject.NULL)
        put("serverVersion", mutation.serverVersion ?: JSONObject.NULL)
        put("mutationJSON", mutation.mutationJSON)
        put("rejectionJSON", mutation.rejectionJSON)
        put("createdAt", mutation.createdAt)
        put("updatedAt", mutation.updatedAt)
    }

    private fun operationWireValue(operation: Operation): String = when (operation) {
        Operation.INSERT -> "insert"
        Operation.UPSERT -> "upsert"
        Operation.UPDATE -> "update"
        Operation.DELETE -> "delete"
    }

    private fun localMutationStatusWireValue(status: LocalMutationStatus): String = when (status) {
        LocalMutationStatus.PENDING -> "pending"
        LocalMutationStatus.SEALED -> "sealed"
        LocalMutationStatus.SUPERSEDED_BEFORE_SEND -> "superseded_before_send"
        LocalMutationStatus.CANCELLED_BEFORE_SEND -> "cancelled_before_send"
        LocalMutationStatus.BLOCKED_BY_PREDECESSOR -> "blocked_by_predecessor"
        LocalMutationStatus.SERVER_REJECTED -> "server_rejected"
    }

    private fun mutationStatusWireValue(status: MutationStatus): String = when (status) {
        MutationStatus.APPLIED -> "applied"
        MutationStatus.CONFLICT -> "conflict"
        MutationStatus.REJECTED_TERMINAL -> "rejected_terminal"
    }

    private fun schemaActionWireValue(action: SchemaAction): String = when (action) {
        SchemaAction.NONE -> "none"
        SchemaAction.REPLACE -> "replace"
        SchemaAction.REBUILD_LOCAL -> "rebuild_local"
        SchemaAction.UNSUPPORTED -> "unsupported"
    }

    private fun mutationRejectionCodeWireValue(code: MutationRejectionCode): String = when (code) {
        MutationRejectionCode.VERSION_CONFLICT -> "version_conflict"
        MutationRejectionCode.ROW_ALREADY_EXISTS -> "row_already_exists"
        MutationRejectionCode.ROW_DELETED -> "row_deleted"
        MutationRejectionCode.ROW_NOT_FOUND -> "row_not_found"
        MutationRejectionCode.SCHEMA_INCOMPATIBLE -> "schema_incompatible"
        MutationRejectionCode.POLICY_REJECTED -> "policy_rejected"
        MutationRejectionCode.VALIDATION_FAILED -> "validation_failed"
        MutationRejectionCode.TABLE_NOT_SYNCED -> "table_not_synced"
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
                ReadableType.Map -> parseTaggedParam(
                    params.getMap(i) ?: throw IllegalArgumentException("Invalid SQL bind tag at index $i"),
                    i,
                )
                else -> throw IllegalArgumentException("Unsupported SQL bind value at index $i")
            }
        }
    }

    private fun parseTaggedParam(map: ReadableMap, index: Int): Any {
        val keys = mutableSetOf<String>()
        val iterator = map.keySetIterator()
        while (iterator.hasNextKey()) {
            keys += iterator.nextKey()
        }
        if (keys != setOf("type", "base64") && keys != setOf("type", "value")) {
            throw IllegalArgumentException("Invalid SQL bind tag at index $index")
        }
        if (!map.hasKey("type") || map.getType("type") != ReadableType.String) {
            throw IllegalArgumentException("Invalid SQL bind tag at index $index")
        }

        return when (map.getString("type")) {
            "bytes" -> {
                if (keys != setOf("type", "base64") || map.getType("base64") != ReadableType.String) {
                    throw IllegalArgumentException("Invalid bytes bind tag at index $index")
                }
                decodeCanonicalBase64Url(map.getString("base64") ?: "", index)
            }
            "int64" -> {
                if (keys != setOf("type", "value") || map.getType("value") != ReadableType.String) {
                    throw IllegalArgumentException("Invalid int64 bind tag at index $index")
                }
                parseCanonicalInt64(map.getString("value") ?: "", index)
            }
            else -> throw IllegalArgumentException("Invalid SQL bind tag at index $index")
        }
    }

    private fun decodeCanonicalBase64Url(value: String, index: Int): ByteArray {
        require(BASE64_URL_PATTERN.matches(value) && value.length % 4 != 1) {
            "Invalid bytes bind tag at index $index"
        }
        val decoded = try {
            Base64.decode(value, BASE64_URL_FLAGS)
        } catch (_: IllegalArgumentException) {
            throw IllegalArgumentException("Invalid bytes bind tag at index $index")
        }
        val canonical = Base64.encodeToString(decoded, BASE64_URL_FLAGS)
        require(canonical == value) { "Invalid bytes bind tag at index $index" }
        return decoded
    }

    private fun parseCanonicalInt64(value: String, index: Int): Long {
        require(INT64_PATTERN.matches(value)) {
            "Invalid int64 bind tag at index $index"
        }
        val parsed = value.toLongOrNull()
            ?: throw IllegalArgumentException("Invalid int64 bind tag at index $index")
        require(parsed.toString() == value) {
            "Invalid int64 bind tag at index $index"
        }
        return parsed
    }

    private fun sqliteNumberParam(value: Double, index: Int): Any {
        if (!value.isFinite()) {
            throw IllegalArgumentException("Invalid SQL number bind value at index $index")
        }
        if (value % 1.0 == 0.0 &&
            (value < -MAX_SAFE_INTEGER.toDouble() || value > MAX_SAFE_INTEGER.toDouble())
        ) {
            throw IllegalArgumentException("SQL integer bind value is outside the safe range at index $index")
        }
        return if (value % 1.0 == 0.0) {
            value.toLong()
        } else {
            value
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
            is Long -> if (value in -MAX_SAFE_INTEGER..MAX_SAFE_INTEGER) {
                map.putDouble(key, value.toDouble())
            } else {
                map.putMap(key, Arguments.createMap().apply {
                    putString("type", "int64")
                    putString("value", value.toString())
                })
            }
            is Float -> map.putDouble(key, value.toDouble())
            is Double -> map.putDouble(key, value)
            is String -> map.putString(key, value)
            is ByteArray -> map.putMap(key, Arguments.createMap().apply {
                putString("type", "bytes")
                putString("base64", Base64.encodeToString(value, BASE64_URL_FLAGS))
            })
            else -> map.putString(key, value.toString())
        }
    }
}
