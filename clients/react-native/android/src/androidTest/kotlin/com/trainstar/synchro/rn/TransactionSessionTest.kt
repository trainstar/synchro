package com.trainstar.synchro.rn

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import com.trainstar.synchro.ExecResult
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertSame
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith

private const val GUARD_SECONDS = 60L

private class SessionError(name: String) : Exception(name)

/** Records the begin settlements that the actual session delivers. */
private class BeginRecorder {
    private val lock = Any()
    private val results = mutableListOf<Result<String>>()

    fun record(result: Result<String>) {
        synchronized(lock) { results += result }
    }

    val successes: List<String> get() = synchronized(lock) { results.mapNotNull { it.getOrNull() } }
    val failures: List<Throwable> get() = synchronized(lock) { results.mapNotNull { it.exceptionOrNull() } }
}

/** A received terminal completion and a count of its observable completion deliveries. */
private class ReceivedTerminal {
    val deferred = CompletableDeferred<Unit>()
    private val deliveries = AtomicInteger()

    init {
        deferred.invokeOnCompletion { deliveries.incrementAndGet() }
    }

    val deliveryCount: Int get() = deliveries.get()
}

/**
 * Direct tests of the production TransactionSession and one terminal-only run of the actual loop.
 * These tests select the order of the session calls. They do not claim a physical Channel schedule.
 */
@OptIn(ExperimentalCoroutinesApi::class)
@RunWith(AndroidJUnit4::class)
class TransactionSessionTest {
    @Test
    fun failureBeforeCallbackRejectsStoredBeginOnce() {
        val begin = BeginRecorder()
        val session = SynchroModule.TransactionSession(isWrite = true, beginSettlement = begin::record)
        val acquisition = SessionError("acquisition")

        session.rejectBegin(acquisition)
        session.rejectBegin(SessionError("later failure"))

        assertEquals(emptyList<String>(), begin.successes)
        assertEquals(1, begin.failures.size)
        assertSame(acquisition, begin.failures.single())
    }

    @Test
    fun abortBeforeBeginAcceptanceRejectsBeginAndUnwindsCallback() {
        val begin = BeginRecorder()
        val session = SynchroModule.TransactionSession(isWrite = true, beginSettlement = begin::record)
        val close = SessionError("close")

        session.abort(close)
        val thrown = assertThrows(SessionError::class.java) { session.acceptBegin("abort-before-begin") }
        session.rejectBegin(SessionError("callback exit"))

        assertSame(close, thrown)
        assertEquals(emptyList<String>(), begin.successes)
        assertEquals(1, begin.failures.size)
        assertSame(close, begin.failures.single())
    }

    @Test
    fun acceptedBeginIsNotSettledAgainByLaterFailure() {
        val begin = BeginRecorder()
        val session = SynchroModule.TransactionSession(isWrite = true, beginSettlement = begin::record)

        session.acceptBegin("accepted")
        session.abort(SessionError("close"))
        session.rejectBegin(SessionError("callback exit"))

        assertEquals(listOf("accepted"), begin.successes)
        assertEquals(emptyList<Throwable>(), begin.failures)
    }

    /** The exact lost-owner order: close wins after the loop received a terminal operation. */
    @Test
    fun abortBeforeAcceptanceKeepsReceivedTerminalUntilFinish() {
        val session = acceptedSession()
        val received = ReceivedTerminal()
        val exit = SessionError("transaction exit")

        session.abort(SessionError("close"))
        assertFalse(session.acceptTerminal(received.deferred))
        assertFalse(received.deferred.isCompleted)
        assertEquals(0, received.deliveryCount)

        session.finishTerminal(exit)
        assertTrue("received terminal operation lost its owner", received.deferred.isCompleted)
        assertSame(exit, received.deferred.getCompletionExceptionOrNull())
        assertEquals(1, received.deliveryCount)
    }

    @Test
    fun acceptanceBeforeAbortKeepsTerminalUntilSuccessfulFinish() {
        val session = acceptedSession()
        val received = ReceivedTerminal()

        assertTrue(session.acceptTerminal(received.deferred))
        session.abort(SessionError("close"))
        assertFalse(received.deferred.isCompleted)
        assertEquals(0, received.deliveryCount)

        session.finishTerminal(null)
        assertTrue(received.deferred.isCompleted)
        assertEquals(null, received.deferred.getCompletionExceptionOrNull())
        assertEquals(1, received.deliveryCount)
    }

    @Test
    fun acceptedTerminalRejectsWithTransactionExitError() {
        val session = acceptedSession()
        val received = ReceivedTerminal()
        val exit = SessionError("transaction exit")

        assertTrue(session.acceptTerminal(received.deferred))
        session.finishTerminal(exit)

        assertTrue(received.deferred.isCompleted)
        assertSame(exit, received.deferred.getCompletionExceptionOrNull())
        assertEquals(1, received.deliveryCount)
    }

    @Test
    fun commitLoopTransfersReceivedDeferredToSession() {
        val result = runTerminalLoop(commit = true)

        assertEquals(null, result.loopFailure)
        result.requireOwnedUntilFinish()
    }

    @Test
    fun rollbackLoopTransfersReceivedDeferredToSession() {
        val result = runTerminalLoop(commit = false)

        assertTrue(
            "rollback loop did not exit with its control exception: ${result.loopFailure}",
            result.loopFailure is SynchroModule.TransactionRollbackException,
        )
        result.requireOwnedUntilFinish()
    }

    private fun acceptedSession(): SynchroModule.TransactionSession {
        val begin = BeginRecorder()
        val session = SynchroModule.TransactionSession(isWrite = true, beginSettlement = begin::record)
        session.acceptBegin("terminal")
        assertEquals(listOf("terminal"), begin.successes)
        return session
    }

    private class LoopResult(
        val session: SynchroModule.TransactionSession,
        val received: ReceivedTerminal,
        val begin: BeginRecorder,
        val sqlCalls: Int,
        val loopFailure: Throwable?,
    ) {
        fun requireOwnedUntilFinish() {
            assertEquals(listOf("wiring"), begin.successes)
            assertEquals(0, sqlCalls)
            assertFalse(received.deferred.isCompleted)
            assertEquals(0, received.deliveryCount)
            val exit = SessionError("selected transaction exit")
            session.finishTerminal(exit)
            assertTrue("loop did not transfer the received terminal to the session", received.deferred.isCompleted)
            assertSame(exit, received.deferred.getCompletionExceptionOrNull())
            assertEquals(1, received.deliveryCount)
        }
    }

    /** Runs the actual loop on an owned thread and sends one terminal operation through its channel. */
    private fun runTerminalLoop(commit: Boolean): LoopResult {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val module = SynchroModule(bridgeTestContext(context))
        val begin = BeginRecorder()
        val session = SynchroModule.TransactionSession(isWrite = true, beginSettlement = begin::record)
        val received = ReceivedTerminal()
        val sqlCalls = AtomicInteger()
        val loopThread = Executors.newSingleThreadExecutor()
        var primary: Throwable? = null
        try {
            val loop = loopThread.submit(Callable {
                module.runTransactionLoop(
                    txID = "wiring",
                    session = session,
                    query = { _, _ -> sqlCalls.incrementAndGet(); emptyList() },
                    queryOne = { _, _ -> sqlCalls.incrementAndGet(); null },
                    execute = { _, _ -> sqlCalls.incrementAndGet(); ExecResult(rowsAffected = 0) },
                )
            })
            val operation = if (commit) {
                SynchroModule.TransactionOp.Commit(received.deferred)
            } else {
                SynchroModule.TransactionOp.Rollback(received.deferred)
            }
            runBlocking { withTimeout(TimeUnit.SECONDS.toMillis(GUARD_SECONDS)) { session.operations.send(operation) } }
            val failure = try {
                loop.get(GUARD_SECONDS, TimeUnit.SECONDS)
                null
            } catch (error: ExecutionException) {
                error.cause
            }
            return LoopResult(session, received, begin, sqlCalls.get(), failure)
        } catch (error: Throwable) {
            primary = error
            // Closing the actual session channel ends a loop that still waits for an operation.
            session.abort(error)
            throw error
        } finally {
            loopThread.shutdown()
            val cleanup = try {
                if (loopThread.awaitTermination(GUARD_SECONDS, TimeUnit.SECONDS)) {
                    null
                } else {
                    AssertionError("loop thread did not stop")
                }
            } catch (error: InterruptedException) {
                Thread.currentThread().interrupt()
                error
            }
            if (cleanup != null) {
                primary?.addSuppressed(cleanup) ?: throw cleanup
            }
        }
    }
}
