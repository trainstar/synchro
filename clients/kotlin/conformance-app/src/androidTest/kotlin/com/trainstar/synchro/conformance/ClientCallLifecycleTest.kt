package com.trainstar.synchro.conformance

import com.trainstar.synchro.SyncFailure
import com.trainstar.synchro.SyncFailureCode
import com.trainstar.synchro.SyncOperationKind
import com.trainstar.synchro.SyncRecoveryAction
import com.trainstar.synchro.SyncStatus
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.time.Instant

class ClientCallLifecycleTest {
    @Test
    fun beginAndAwaitUseOnePublicCall() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val started = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        var invocations = 0
        val lifecycle = ClientCallLifecycle(
            scope,
            invokeMethod = { _, _ ->
                invocations++
                started.complete(Unit)
                release.await()
            },
            readStatus = { SyncStatus.Ready },
        )

        try {
            val begun = lifecycle.begin("client-a", "call-a", "start")
            started.await()
            assertEquals("in_flight", begun.state)
            assertNull(begun.completion)
            assertEquals(1, invocations)

            release.complete(Unit)
            val completed = lifecycle.await("client-a", "call-a")
            assertEquals(CallCompletion.IDLE, completed.completion)
            assertEquals(1, invocations)
        } finally {
            scope.cancel()
            lifecycle.joinAll()
        }
    }

    @Test
    fun dispatchesEachSupportedMethodWithoutRewritingIt() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val methods = mutableListOf<String>()
        val lifecycle = ClientCallLifecycle(
            scope,
            invokeMethod = { _, method -> methods += method },
            readStatus = { SyncStatus.Ready },
        )
        val expected = listOf("start", "sync-now", "retry-after-error", "reset-schema-and-start")

        try {
            expected.forEachIndexed { index, method ->
                lifecycle.begin("client-$index", "call-$index", method)
                assertEquals(CallCompletion.IDLE, lifecycle.await("client-$index", "call-$index").completion)
            }
            assertEquals(expected, methods)
        } finally {
            scope.cancel()
            lifecycle.joinAll()
        }
    }

    @Test
    fun derivesBlockedAndErrorFromRawStatusAndFailure() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        var status: SyncStatus = SyncStatus.Backoff(Instant.EPOCH, "connecting")
        var fail = false
        val lifecycle = ClientCallLifecycle(
            scope,
            invokeMethod = { _, _ -> if (fail) error("public call failed") },
            readStatus = { status },
        )

        try {
            lifecycle.begin("client-a", "call-a", "start")
            assertEquals(CallCompletion.BLOCKED, lifecycle.await("client-a", "call-a").completion)
            status = SyncStatus.Error(
                SyncFailure(
                    operation = SyncOperationKind.CONNECTING,
                    code = SyncFailureCode.NETWORK_ERROR,
                    retryable = false,
                    message = "failed",
                    recoveryAction = SyncRecoveryAction.RETRY,
                ),
            )
            lifecycle.begin("client-b", "call-b", "retry-after-error")
            assertEquals(CallCompletion.BLOCKED, lifecycle.await("client-b", "call-b").completion)
            status = SyncStatus.Ready
            fail = true
            lifecycle.begin("client-c", "call-c", "sync-now")
            val failed = lifecycle.await("client-c", "call-c")
            assertEquals(CallCompletion.ERROR, failed.completion)
            assertEquals("unknown_error", failed.errorCategory)
        } finally {
            scope.cancel()
            lifecycle.joinAll()
        }
    }

    @Test
    fun rejectsDuplicateUnknownAndMismatchedCalls() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val lifecycle = ClientCallLifecycle(
            scope,
            invokeMethod = { _, _ -> Unit },
            readStatus = { SyncStatus.Ready },
        )

        try {
            lifecycle.begin("client-a", "call-a", "start")
            assertFails { lifecycle.begin("client-b", "call-a", "start") }
            assertFailsSuspend { lifecycle.await("client-a", "missing") }
            assertFailsSuspend { lifecycle.await("client-b", "call-a") }
            lifecycle.await("client-a", "call-a")
            assertFailsSuspend { lifecycle.await("client-a", "call-a") }
        } finally {
            scope.cancel()
            lifecycle.joinAll()
        }
    }

    private fun assertFails(block: () -> Unit) {
        var failed = false
        try {
            block()
        } catch (_: IllegalStateException) {
            failed = true
        }
        assertTrue(failed)
    }

    private suspend fun assertFailsSuspend(block: suspend () -> Unit) {
        var failed = false
        try {
            block()
        } catch (_: IllegalStateException) {
            failed = true
        }
        assertTrue(failed)
    }
}
