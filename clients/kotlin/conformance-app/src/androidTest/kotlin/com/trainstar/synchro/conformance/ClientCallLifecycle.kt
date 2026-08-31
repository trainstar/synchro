package com.trainstar.synchro.conformance

import com.trainstar.synchro.SyncStatus
import com.trainstar.synchro.SyncRecoveryAction
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.joinAll

internal enum class CallCompletion(val wireName: String) {
    IDLE("idle"),
    BLOCKED("blocked"),
    ERROR("error"),
}

internal data class ClientCallObservation(
    val callID: String,
    val state: String,
    val completion: CallCompletion? = null,
    val errorCategory: String? = null,
)

internal class ClientCallLifecycle(
    private val scope: CoroutineScope,
    private val invokeMethod: suspend (clientKey: String, method: String) -> Unit,
    private val readStatus: (clientKey: String) -> SyncStatus,
) {
    private val calls = linkedMapOf<String, ClientCall>()

    fun begin(clientKey: String, callID: String, method: String): ClientCallObservation {
        requireMethod(method)
        check(callID !in calls) { "call is already registered" }
        check(calls.values.none { it.clientKey == clientKey && !it.awaited }) {
            "client already has an active call"
        }
        val task = scope.async {
            val failure = try {
                invokeMethod(clientKey, method)
                null
            } catch (error: CancellationException) {
                error
            } catch (error: Throwable) {
                error
            }
            CompletedCall(
                completion(readStatus(clientKey), failure),
                failure?.let(::errorCategory),
            )
        }
        calls[callID] = ClientCall(clientKey, task)
        return ClientCallObservation(callID, "in_flight")
    }

    suspend fun await(clientKey: String, callID: String): ClientCallObservation {
        val call = activeCall(clientKey, callID)
        val completed = call.task.await()
        call.awaited = true
        return ClientCallObservation(callID, "completed", completed.completion, completed.errorCategory)
    }

    suspend fun joinAll() {
        calls.values.map { it.task }.joinAll()
    }

    private fun activeCall(clientKey: String, callID: String): ClientCall {
        val call = calls[callID] ?: throw IllegalStateException("call is not registered")
        check(call.clientKey == clientKey) { "call belongs to another client" }
        check(!call.awaited) { "call is not active" }
        return call
    }

    private fun completion(status: SyncStatus, failure: Throwable?): CallCompletion = when {
        // A call that throws completes in error. The contract reaches an
        // explicit schema reset from the error state, so a thrown unsupported
        // schema is an error rather than a blocked call. A client that holds a
        // recovery action without throwing stays blocked below.
        failure != null -> CallCompletion.ERROR
        status is SyncStatus.Backoff -> CallCompletion.BLOCKED
        status is SyncStatus.Error && status.failure.recoveryAction != SyncRecoveryAction.NONE -> CallCompletion.BLOCKED
        status is SyncStatus.Error -> CallCompletion.ERROR
        else -> CallCompletion.IDLE
    }

    private fun errorCategory(error: Throwable): String = when (error) {
        is CancellationException -> "cancelled"
        is com.trainstar.synchro.SynchroError.NotConnected -> "not_connected"
        is com.trainstar.synchro.SynchroError.SchemaNotLoaded -> "schema_not_loaded"
        is com.trainstar.synchro.SynchroError.TableNotSynced -> "table_not_synced"
        is com.trainstar.synchro.SynchroError.UpgradeRequired -> "upgrade_required"
        is com.trainstar.synchro.SynchroError.SchemaMismatch -> "schema_mismatch"
        is com.trainstar.synchro.SynchroError.PushRejected -> "push_rejected"
        is com.trainstar.synchro.SynchroError.NetworkError -> "network_error"
        is com.trainstar.synchro.SynchroError.ServerError -> "server_error"
        is com.trainstar.synchro.SynchroError.DatabaseError -> "database_error"
        is com.trainstar.synchro.SynchroError.InvalidResponse -> "invalid_response"
        is com.trainstar.synchro.SynchroError.BlockingFailure -> "blocked"
        is com.trainstar.synchro.SynchroError.UnsupportedSchema -> "unsupported_schema"
        is com.trainstar.synchro.SynchroError.InvalidStateTransition -> "invalid_state_transition"
        is com.trainstar.synchro.SynchroError.AlreadyStarted -> "already_started"
        is com.trainstar.synchro.SynchroError.NotStarted -> "not_started"
        else -> "unknown_error"
    }

    private fun requireMethod(method: String) {
        require(method in METHODS) { "unsupported synchronization method" }
    }

    private data class ClientCall(
        val clientKey: String,
        val task: Deferred<CompletedCall>,
        var awaited: Boolean = false,
    )

    private data class CompletedCall(
        val completion: CallCompletion,
        val errorCategory: String?,
    )

    private companion object {
        val METHODS = setOf("start", "sync-now", "retry-after-error", "reset-schema-and-start")
    }
}
