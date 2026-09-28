package com.trainstar.synchro.rn

import android.database.DatabaseUtils
import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteDatabaseLockedException
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import com.facebook.react.bridge.JavaOnlyArray
import com.facebook.react.bridge.JavaOnlyMap
import com.facebook.react.bridge.Promise
import com.facebook.react.bridge.BridgeReactContext
import com.facebook.react.bridge.ReadableMap
import com.facebook.react.bridge.WritableMap
import com.facebook.react.soloader.OpenSourceMergedSoMapping
import com.facebook.soloader.SoLoader
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.json.JSONObject
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

private const val SETTLEMENT_TIMEOUT_SECONDS = 60L
private const val COLUMNS_JSON = """[{"name":"id","type":"TEXT","primaryKey":true},{"name":"name","type":"TEXT"}]"""
private const val INSERT_SQL = "INSERT INTO bridge_items (id, name) VALUES (?, ?)"
private const val SELECT_NAME_SQL = "SELECT name FROM bridge_items WHERE id = ?"

/**
 * Module events: these tests call neither start nor stop, and they register no bridge observer.
 * The module emits only from auth requests, bridge observers, and SDK status, sync-event, and
 * conflict callbacks. The SDK invokes those callbacks only for lifecycle transitions and sync,
 * and module close cancels its subscriptions before it closes the client. The direct native
 * onChange subscription in the held-commit test is not a module event.
 */
@RunWith(AndroidJUnit4::class)
class SynchroModuleTransactionTest {
    private val context = InstrumentationRegistry.getInstrumentation().targetContext
    private val databaseName = "synchro_bridge_${UUID.randomUUID()}.db"
    private val gates = ArrayDeque<AutoCloseable>()
    private lateinit var module: SynchroModule

    @Before
    fun setUp() {
        // Bridge result maps are native React Native maps.
        SoLoader.init(context, OpenSourceMergedSoMapping)
        module = SynchroModule(bridgeTestContext(context))
    }

    @After
    fun tearDown() {
        val failures = mutableListOf<Throwable>()
        // Release every gate before close joins the registered transactions.
        while (gates.isNotEmpty()) {
            try {
                gates.removeLast().close()
            } catch (error: Throwable) {
                failures += error
            }
        }
        if (::module.isInitialized) {
            try {
                val close = RecordingPromise("teardown close")
                module.close(close)
                if (!close.waitForSettlement()) {
                    failures += AssertionError("teardown close did not settle")
                } else if (close.resolutions.size != 1 || close.rejections.isNotEmpty()) {
                    failures += AssertionError(
                        "teardown close did not resolve once: ${close.resolutions.size} resolutions, ${close.rejections}",
                    )
                }
            } catch (error: Throwable) {
                failures += error
            }
        }
        if (failures.isEmpty()) {
            val file = context.getDatabasePath(databaseName)
            if (file.exists() && !context.deleteDatabase(databaseName)) {
                failures += AssertionError("delete database $databaseName failed")
            }
        } else {
            failures += AssertionError("cleanup was incomplete, so database $databaseName was kept")
        }
        if (failures.isNotEmpty()) {
            val first = failures.first()
            failures.drop(1).forEach(first::addSuppressed)
            throw first
        }
    }

    @Test
    fun acquisitionFailureRejectsBeginOnce() {
        initializeModule()
        val external = openExternalConnection()
        external.acquireWriteLock()

        val begin = RecordingPromise("begin")
        module.beginWriteTransaction(begin)
        // The held write lock keeps the transaction callback from starting. The registered
        // transaction job has finished its SDK call and catch when this barrier returns.
        joinRegisteredTransactions()
        assertEquals(emptyList<Any?>(), begin.resolutions)
        assertEquals(listOf("UNKNOWN"), begin.rejections.map { it.code })
        val cause = begin.rejections.single().throwable
        assertTrue("begin rejection is not the SQLite acquisition error: $cause", cause is SQLiteDatabaseLockedException)

        // Client close writes the stopped lifecycle state, so the lock is released before close.
        external.releaseWriteLock()
        val close = closeModule()
        assertEquals(1, begin.rejections.size)
        assertEquals(1, close.resolutions.size)
        assertEquals(0L, external.rowCount())

        initializeModule()
        val id = UUID.randomUUID().toString()
        val transaction = commitRow(id, "after-acquisition-failure")
        val reopenedClose = closeModule()
        requireSettledOnce(transaction + reopenedClose)
        assertEquals(listOf("after-acquisition-failure"), external.names(id))
    }

    @Test
    fun commitResolvesAfterDurableCompletion() {
        initializeModule()
        val external = openExternalConnection()
        val id = UUID.randomUUID().toString()

        val transaction = commitRow(id, "committed")
        // The commit settled, so another connection sees the row.
        assertEquals(listOf("committed"), external.names(id))

        val close = closeModule()
        requireSettledOnce(transaction + close)
        assertEquals(listOf("committed"), external.names(id))
    }

    @Test
    fun rollbackResolvesAfterRollback() {
        initializeModule()
        val external = openExternalConnection()
        val id = UUID.randomUUID().toString()

        val begin = settle("begin") { module.beginWriteTransaction(it) }
        val txID = begin.resolvedValue() as String
        val insert = settle("insert") { module.txExecute(txID, INSERT_SQL, JavaOnlyArray.of(id, "rolled-back"), it) }
        assertEquals(1, (insert.resolvedValue() as ReadableMap).getInt("rowsAffected"))
        val observe = settle("observe") { module.txQueryOne(txID, SELECT_NAME_SQL, JavaOnlyArray.of(id), it) }
        assertEquals("rolled-back", (observe.resolvedValue() as ReadableMap).getString("name"))

        val rollback = settle("rollback") { module.rollbackTransaction(txID, it) }
        rollback.resolvedValue()
        assertEquals(emptyList<String>(), external.names(id))
        val after = settle("query after rollback") { module.queryOne(SELECT_NAME_SQL, JavaOnlyArray.of(id), it) }
        assertNull(after.resolvedValue())

        val close = closeModule()
        requireSettledOnce(listOf(begin, insert, observe, rollback, after, close))
    }

    /**
     * Observes an accepted commit after its durable write and before its SDK call returns.
     * This is not a window inside SQLite COMMIT.
     */
    @Test
    fun closeWaitsForAcceptedCommitHeldAfterDurableWrite() {
        initializeModule()
        val external = openExternalConnection()
        val client = checkNotNull(module.client)
        val hold = ChangeHold().also(gates::addLast)
        val subscription = client.onChange(listOf("bridge_items")) { hold.enter() }
        gates.addLast(AutoCloseable { subscription.cancel() })
        val id = UUID.randomUUID().toString()

        val begin = settle("begin") { module.beginWriteTransaction(it) }
        val txID = begin.resolvedValue() as String
        val insert = settle("insert") { module.txExecute(txID, INSERT_SQL, JavaOnlyArray.of(id, "held"), it) }
        assertEquals(1, (insert.resolvedValue() as ReadableMap).getInt("rowsAffected"))
        val commit = RecordingPromise("commit")
        module.commitTransaction(txID, commit)
        hold.awaitEntry()

        // The write is durable, but the SDK call has not returned, so the promise is pending.
        assertEquals(listOf("held"), external.names(id))
        assertEquals(0, commit.settlementCount)
        assertEquals(setOf(txID), module.sessions.keys.toSet())

        val close = RecordingPromise("close")
        module.close(close)
        awaitCloseDetachesSessions()
        assertEquals(0, close.settlementCount)
        assertEquals(0, commit.settlementCount)

        hold.release()
        commit.awaitSettlement()
        close.awaitSettlement()
        assertTrue(hold.heldUntilRelease)
        assertEquals(1, hold.entryCount)
        subscription.cancel()
        requireSettledOnce(listOf(begin, insert, commit, close))
        assertEquals(listOf("held"), external.names(id))

        initializeModule()
        val laterID = UUID.randomUUID().toString()
        val later = commitRow(laterID, "after-held-commit")
        val laterClose = closeModule()
        requireSettledOnce(later + laterClose)
        assertEquals(listOf("after-held-commit"), external.names(laterID))
    }

    @Test
    fun reinitializationKeepsCommittedRowsAndAcceptsWork() {
        initializeModule()
        val firstID = UUID.randomUUID().toString()
        val first = commitRow(firstID, "before-close")
        val firstClose = closeModule()

        initializeModule()
        val read = settle("query after reinitialize") { module.queryOne(SELECT_NAME_SQL, JavaOnlyArray.of(firstID), it) }
        assertEquals("before-close", (read.resolvedValue() as ReadableMap).getString("name"))
        val secondID = UUID.randomUUID().toString()
        val second = commitRow(secondID, "after-reinitialize")
        val external = openExternalConnection()
        val secondClose = closeModule()

        requireSettledOnce(first + firstClose + read + second + secondClose)
        assertEquals(2L, external.rowCount())
        assertEquals(listOf("before-close"), external.names(firstID))
        assertEquals(listOf("after-reinitialize"), external.names(secondID))
    }

    @Test
    fun clientStateSnapshotReadsRequestedRowsInsideReadOnlySnapshot() {
        initializeModule()
        val firstID = UUID.randomUUID().toString()
        val secondID = UUID.randomUUID().toString()
        val commits = commitRow(firstID, "first") + commitRow(secondID, "second")

        val snapshot = settle("snapshot") {
            module.inspectClientStateSnapshot(
                JavaOnlyArray.of(JavaOnlyMap.of("sql", "SELECT id, name FROM bridge_items WHERE id = ?", "params", JavaOnlyArray.of(firstID))),
                it,
            )
        }
        val result = snapshot.resolvedValue() as ReadableMap
        assertEquals(setOf("inspection", "applicationRows"), result.toHashMap().keys)
        val rows = result.getArray("applicationRows")!!
        assertEquals(1, rows.size())
        assertEquals(firstID, rows.getMap(0)!!.getString("id"))
        assertEquals("first", rows.getMap(0)!!.getString("name"))
        val inspection = JSONObject(result.getString("inspection")!!)
        assertEquals(
            setOf("client_state", "retained_mutations", "rejected_mutations"),
            inspection.keys().asSequence().toSet(),
        )
        // bridge_items is a local table, so the mutation ledger stays empty.
        for (member in listOf("retained_mutations", "rejected_mutations")) {
            assertEquals(member, 0, inspection.getJSONArray(member).length())
        }
        val clientState = inspection.getJSONObject("client_state")
        assertEquals(0, clientState.getInt("mutation_ledger_count"))
        assertEquals(0, clientState.getInt("rejected_mutation_count"))

        val write = settle("snapshot write") {
            module.inspectClientStateSnapshot(
                JavaOnlyArray.of(JavaOnlyMap.of("sql", "DELETE FROM bridge_items WHERE id = ?", "params", JavaOnlyArray.of(firstID))),
                it,
            )
        }
        assertEquals(0, write.resolutions.size)
        assertEquals(1, write.rejections.size)
        val external = openExternalConnection()
        val close = closeModule()

        requireSettledOnce(commits + snapshot + close)
        assertEquals(listOf("first"), external.names(firstID))
        assertEquals(2L, external.rowCount())
    }

    private fun initializeModule() {
        val config = JavaOnlyMap.of(
            "dbPath", databaseName,
            "serverURL", "https://synchro.invalid",
            "clientID", UUID.randomUUID().toString(),
            "platform", "android",
            "appVersion", "1.0.0",
            "requireNewDatabase", false,
        )
        settle("initialize") { module.initialize(config, it) }.resolvedValue()
        settle("create table") { module.createTable("bridge_items", COLUMNS_JSON, null, it) }.resolvedValue()
    }

    private fun openExternalConnection(): ExternalConnection {
        val name = settle("get path") { module.getPath(it) }.resolvedValue() as String
        // SQLiteOpenHelper resolves a bare database name in the app database directory.
        val path = context.getDatabasePath(name).absolutePath
        return ExternalConnection(path).also(gates::addLast).also { it.open() }
    }

    private fun commitRow(id: String, name: String): List<RecordingPromise> {
        val begin = settle("begin") { module.beginWriteTransaction(it) }
        val txID = begin.resolvedValue() as String
        val insert = settle("insert") { module.txExecute(txID, INSERT_SQL, JavaOnlyArray.of(id, name), it) }
        assertEquals(1, (insert.resolvedValue() as ReadableMap).getInt("rowsAffected"))
        val commit = settle("commit") { module.commitTransaction(txID, it) }
        commit.resolvedValue()
        return listOf(begin, insert, commit)
    }

    private fun closeModule(): RecordingPromise =
        settle("close") { module.close(it) }.also { it.resolvedValue() }

    /**
     * beginWriteTransaction registers its session before it returns. The job removes the session
     * in finally after the SDK call and its catch finish, so an empty registry is a completion.
     */
    private fun joinRegisteredTransactions() {
        val jobs = module.sessions.values.map { it.job }
        runBlocking { withTimeout(TimeUnit.SECONDS.toMillis(SETTLEMENT_TIMEOUT_SECONDS)) { jobs.joinAll() } }
        assertTrue("registered transactions remain after their jobs finished", module.sessions.isEmpty())
    }

    /**
     * Close removes every registered session under the registry lock before it joins their jobs.
     * The held job cannot reach its own removal, so an empty registry shows that close detached it.
     */
    private fun awaitCloseDetachesSessions() {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(SETTLEMENT_TIMEOUT_SECONDS)
        while (module.sessions.isNotEmpty()) {
            assertTrue("close did not detach the registered session", System.nanoTime() < deadline)
            Thread.sleep(10)
        }
    }

    private fun settle(name: String, call: (Promise) -> Unit): RecordingPromise =
        RecordingPromise(name).also {
            call(it)
            it.awaitSettlement()
        }

    /** Checks the counters after a close that joined every registered transaction job. */
    private fun requireSettledOnce(promises: List<RecordingPromise>) {
        for (promise in promises) {
            assertEquals("${promise.name} resolutions", 1, promise.resolutions.size)
            assertEquals("${promise.name} rejections", emptyList<Rejection>(), promise.rejections)
        }
    }
}

/**
 * RN 0.83 makes ReactApplicationContext abstract. BridgeReactContext is its concrete
 * @VisibleForTesting subclass. Its legacy-architecture assertion is inert only because the pinned
 * react-android 0.83.0 debug artifact sets UNSTABLE_ENABLE_MINIFY_LEGACY_ARCHITECTURE to false.
 * This context runs no JavaScript, so these tests do not cover the bridgeless JavaScript host.
 */
@Suppress("DEPRECATION")
internal fun bridgeTestContext(context: android.content.Context): BridgeReactContext = BridgeReactContext(context)

private data class Rejection(val code: String?, val throwable: Throwable?)

/** Records every settlement of one bridge promise, including a settlement after the first. */
private class RecordingPromise(val name: String) : Promise {
    private val lock = Any()
    private val resolved = mutableListOf<Any?>()
    private val rejected = mutableListOf<Rejection>()
    private val firstSettlement = CountDownLatch(1)

    val resolutions: List<Any?> get() = synchronized(lock) { resolved.toList() }
    val rejections: List<Rejection> get() = synchronized(lock) { rejected.toList() }
    val settlementCount: Int get() = synchronized(lock) { resolved.size + rejected.size }

    fun waitForSettlement(): Boolean = firstSettlement.await(SETTLEMENT_TIMEOUT_SECONDS, TimeUnit.SECONDS)

    fun awaitSettlement() {
        assertTrue("$name did not settle", waitForSettlement())
    }

    fun resolvedValue(): Any? {
        assertEquals("$name rejections", emptyList<Rejection>(), rejections)
        val values = resolutions
        assertEquals("$name resolutions", 1, values.size)
        return values.single()
    }

    override fun resolve(value: Any?) = record { resolved.add(value) }
    override fun reject(code: String, message: String?) = rejectWith(code, null)
    override fun reject(code: String, throwable: Throwable?) = rejectWith(code, throwable)
    override fun reject(code: String, message: String?, throwable: Throwable?) = rejectWith(code, throwable)
    override fun reject(throwable: Throwable) = rejectWith(null, throwable)
    override fun reject(throwable: Throwable, userInfo: WritableMap) = rejectWith(null, throwable)
    override fun reject(code: String, userInfo: WritableMap) = rejectWith(code, null)
    override fun reject(code: String, throwable: Throwable?, userInfo: WritableMap) = rejectWith(code, throwable)
    override fun reject(code: String, message: String?, userInfo: WritableMap) = rejectWith(code, null)
    override fun reject(code: String?, message: String?, throwable: Throwable?, userInfo: WritableMap?) =
        rejectWith(code, throwable)

    @Deprecated("Promise declares this overload.")
    override fun reject(message: String) = rejectWith(null, null)

    private fun rejectWith(code: String?, throwable: Throwable?) = record { rejected.add(Rejection(code, throwable)) }

    private fun record(change: () -> Unit) {
        synchronized(lock) { change() }
        firstSettlement.countDown()
    }
}

/**
 * Holds the first native change callback until the test releases it. Later callbacks pass.
 * The SDK calls it inside its own exception handler, so it records results and never throws.
 */
private class ChangeHold : AutoCloseable {
    private val entered = CountDownLatch(1)
    private val released = CountDownLatch(1)
    private val entries = AtomicInteger()

    @Volatile
    var heldUntilRelease = false
        private set

    val entryCount: Int get() = entries.get()

    fun enter() {
        if (entries.incrementAndGet() != 1) return
        entered.countDown()
        heldUntilRelease = try {
            released.await(SETTLEMENT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            false
        }
    }

    fun awaitEntry() {
        assertTrue("native change callback did not run", entered.await(SETTLEMENT_TIMEOUT_SECONDS, TimeUnit.SECONDS))
    }

    fun release() = released.countDown()

    override fun close() = release()
}

/**
 * A test-owned connection to the module database file.
 * Android binds a SQLite transaction to one thread, so every call runs on one owned thread.
 * The test registers the connection for cleanup before [open] starts, so a late open still has an owner.
 */
private class ExternalConnection(private val path: String) : AutoCloseable {
    private val thread: ExecutorService = Executors.newSingleThreadExecutor()

    // Only the owned thread reads or writes these fields.
    private var database: SQLiteDatabase? = null
    private var holdsWriteLock = false

    fun open() = onThread {
        check(database == null) { "external connection is already open" }
        database = SQLiteDatabase.openDatabase(
            path,
            null,
            SQLiteDatabase.OPEN_READWRITE or SQLiteDatabase.ENABLE_WRITE_AHEAD_LOGGING,
        )
    }

    fun acquireWriteLock() = onThread {
        openDatabase().beginTransaction()
        holdsWriteLock = true
    }

    fun releaseWriteLock() = onThread { releaseOnThread() }

    fun names(id: String): List<String> = onThread {
        openDatabase().rawQuery(SELECT_NAME_SQL, arrayOf(id)).use { cursor ->
            buildList { while (cursor.moveToNext()) add(cursor.getString(0)) }
        }
    }

    fun rowCount(): Long = onThread {
        DatabaseUtils.longForQuery(openDatabase(), "SELECT count(*) FROM bridge_items", null)
    }

    /**
     * Ends a held transaction, closes the database, and joins the owned thread. The close task
     * runs after an earlier open task, so it also closes a database that opened after a timeout.
     */
    override fun close() {
        val failures = mutableListOf<Throwable>()
        try {
            failures += onThread {
                val releaseFailure = runCatching { releaseOnThread() }.exceptionOrNull()
                val closeFailure = runCatching {
                    database?.close()
                    database = null
                }.exceptionOrNull()
                listOfNotNull(releaseFailure, closeFailure)
            }
        } catch (error: Throwable) {
            failures += error
        }
        thread.shutdown()
        try {
            if (!thread.awaitTermination(SETTLEMENT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                failures += AssertionError("external connection thread did not stop")
            }
        } catch (error: InterruptedException) {
            Thread.currentThread().interrupt()
            failures += error
        }
        if (failures.isNotEmpty()) {
            val first = failures.first()
            failures.drop(1).forEach(first::addSuppressed)
            throw first
        }
    }

    private fun openDatabase(): SQLiteDatabase = checkNotNull(database) { "external connection is not open" }

    private fun releaseOnThread() {
        if (!holdsWriteLock) return
        openDatabase().endTransaction()
        holdsWriteLock = false
    }

    /** Runs [block] on the owned thread and rethrows its original failure. */
    private fun <T> onThread(block: () -> T): T {
        val task = thread.submit(Callable { block() })
        return try {
            task.get(SETTLEMENT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        } catch (error: ExecutionException) {
            throw error.cause ?: error
        }
    }
}
