package com.trainstar.synchro.inspection

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import java.security.MessageDigest
import java.util.ArrayDeque
import java.util.Locale

@Serializable
@SynchroProofApi
enum class TransportOperationClass {
    @SerialName("connect")
    CONNECT,

    @SerialName("pull")
    PULL,

    @SerialName("push")
    PUSH,

    @SerialName("checkpoint")
    CHECKPOINT,

    @SerialName("schemas")
    SCHEMAS,

    @SerialName("rebuild")
    REBUILD,

    @SerialName("other")
    OTHER;

    companion object {
        fun classify(path: String): TransportOperationClass {
            val segments = path.split('/').filter(String::isNotEmpty)
            if (segments.size < 2 || segments[segments.lastIndex - 1] != "sync") return OTHER
            return when (segments.last()) {
                "connect" -> CONNECT
                "pull" -> PULL
                "push" -> PUSH
                "checkpoint", "checkpoints" -> CHECKPOINT
                "schema", "schemas" -> SCHEMAS
                "rebuild" -> REBUILD
                else -> OTHER
            }
        }

        fun fromWire(value: String): TransportOperationClass? = when (value) {
            "connect" -> CONNECT
            "pull" -> PULL
            "push" -> PUSH
            "checkpoint" -> CHECKPOINT
            "schemas" -> SCHEMAS
            "rebuild" -> REBUILD
            "other" -> OTHER
            else -> null
        }
    }
}

@Serializable
@SynchroProofApi
data class TransportRequestFacts(
    @SerialName("client_generation") val clientGeneration: Long? = null,
    @SerialName("schema_version") val schemaVersion: Long,
    @SerialName("schema_hash") val schemaHash: String,
    @SerialName("protocol_version") val protocolVersion: Int? = null,
    @SerialName("scope_set_version") val scopeSetVersion: Long? = null,
    @SerialName("scope_count") val scopeCount: Int? = null,
    val limit: Int? = null,
    @SerialName("scope_fingerprint") val scopeFingerprint: String? = null,
    @SerialName("rebuild_id_fingerprint") val rebuildIDFingerprint: String? = null,
    @SerialName("cursor_fingerprint") val cursorFingerprint: String? = null,
    @SerialName("cursor_present") val cursorPresent: Boolean? = null,
    @SerialName("mutation_count") val mutationCount: Int? = null,
)

@Serializable
@SynchroProofApi
data class TransportRebuildResponseFacts(
    @SerialName("record_count") val recordCount: Int,
    @SerialName("has_more") val hasMore: Boolean,
    @SerialName("has_cursor") val hasCursor: Boolean,
    @SerialName("has_final_scope_cursor") val hasFinalScopeCursor: Boolean,
    @SerialName("has_checksum") val hasChecksum: Boolean,
    @SerialName("scope_fingerprint") val scopeFingerprint: String,
    @SerialName("final_scope_cursor_fingerprint") val finalScopeCursorFingerprint: String? = null,
)

@Serializable
@SynchroProofApi
data class TransportPullResponseFacts(
    @SerialName("change_count") val changeCount: Int,
    @SerialName("has_more") val hasMore: Boolean,
    @SerialName("rebuild_scope_count") val rebuildScopeCount: Int,
    @SerialName("checksum_count") val checksumCount: Int,
    @SerialName("scope_cursor_fingerprints") val scopeCursorFingerprints: List<String>,
    @SerialName("scope_cursor_fingerprints_complete") val scopeCursorFingerprintsComplete: Boolean,
)

@Serializable
@SynchroProofApi
data class TransportObservation(
    val sequence: Long,
    @SerialName("operation_class") val operationClass: TransportOperationClass,
    @SerialName("status_code") val statusCode: Int,
    @SerialName("error_code") val errorCode: String? = null,
    @SerialName("duration_nanoseconds") val durationNanoseconds: Long,
    @SerialName("cursor_fingerprints") val cursorFingerprints: List<String>? = null,
    @SerialName("cursor_fingerprints_complete") val cursorFingerprintsComplete: Boolean? = null,
    @SerialName("request_facts") val requestFacts: TransportRequestFacts? = null,
    @SerialName("rebuild_response_facts") val rebuildResponseFacts: TransportRebuildResponseFacts? = null,
    @SerialName("pull_response_facts") val pullResponseFacts: TransportPullResponseFacts? = null,
)

@Serializable
@SynchroProofApi
data class TransportObservationSnapshot(
    val observations: List<TransportObservation>,
    val overflowed: Boolean,
    @SerialName("sequence_checkpoint") val sequenceCheckpoint: Long,
)

@SynchroProofApi
enum class TransportPauseBarrierError {
    ALREADY_ARMED,
    WRONG_OPERATION,
    NOT_PAUSED,
    TIMED_OUT,
    CANCELLED,
}

@SynchroProofApi
class TransportPauseBarrierException(
    val error: TransportPauseBarrierError,
) : IllegalStateException(error.name.lowercase().replace('_', ' '))

@SynchroProofApi
class TransportObservationCollector(capacity: Int = 256) {
    val capacity = capacity.coerceAtLeast(1)

    private val lock = Any()
    private val observations = ArrayDeque<TransportObservation>()
    private var sequence = 0L
    private var pauseState: PauseState = PauseState.Idle
    private var nextPauseOperation: TransportOperationClass? = null
    private var pauseWaiter: CompletableDeferred<Unit>? = null
    private var rebuildCursorOverride: String? = null

    private sealed class PauseState {
        data object Idle : PauseState()
        data class Armed(val operationClass: TransportOperationClass) : PauseState()
        data class Paused(
            val operationClass: TransportOperationClass,
            val resume: CompletableDeferred<Unit>,
        ) : PauseState()
        data class Failed(val error: TransportPauseBarrierException) : PauseState()
        data object Cancelled : PauseState()
    }

    fun snapshot(after: Long = 0): TransportObservationSnapshot {
        require(after >= 0) { "transport observation checkpoint is invalid" }
        return synchronized(lock) {
            val oldestSequence = observations.firstOrNull()?.sequence
            val overflowed = oldestSequence != null && after < oldestSequence - 1
            TransportObservationSnapshot(
                observations = observations.filter { it.sequence > after },
                overflowed = overflowed,
                sequenceCheckpoint = sequence,
            )
        }
    }

    fun armPause(operationClass: TransportOperationClass) {
        synchronized(lock) {
            when (val state = pauseState) {
                PauseState.Idle -> pauseState = PauseState.Armed(operationClass)
                is PauseState.Armed -> throw failPauseBarrier(TransportPauseBarrierError.ALREADY_ARMED)
                is PauseState.Paused -> {
                    if (nextPauseOperation != null) {
                        throw failPauseBarrier(TransportPauseBarrierError.ALREADY_ARMED)
                    }
                    nextPauseOperation = operationClass
                }
                is PauseState.Failed -> throw state.error
                PauseState.Cancelled -> throw failPauseBarrier(TransportPauseBarrierError.CANCELLED)
            }
        }
    }

    suspend fun awaitPause(operationClass: TransportOperationClass, timeoutMillis: Long) {
        if (timeoutMillis <= 0) throw failPauseBarrier(TransportPauseBarrierError.TIMED_OUT)
        val waiter = synchronized(lock) {
            when (val state = pauseState) {
                is PauseState.Armed -> {
                    if (state.operationClass != operationClass || pauseWaiter != null) {
                        throw failPauseBarrier(TransportPauseBarrierError.WRONG_OPERATION)
                    }
                    CompletableDeferred<Unit>().also { pauseWaiter = it }
                }
                is PauseState.Paused -> {
                    if (state.operationClass != operationClass) {
                        throw failPauseBarrier(TransportPauseBarrierError.WRONG_OPERATION)
                    }
                    null
                }
                is PauseState.Failed -> throw state.error
                PauseState.Cancelled -> throw failPauseBarrier(TransportPauseBarrierError.CANCELLED)
                PauseState.Idle -> throw failPauseBarrier(TransportPauseBarrierError.WRONG_OPERATION)
            }
        }

        if (waiter == null) return
        try {
            withTimeout(timeoutMillis) { waiter.await() }
        } catch (_: TimeoutCancellationException) {
            throw failPauseBarrier(TransportPauseBarrierError.TIMED_OUT)
        } catch (error: CancellationException) {
            failPauseBarrier(TransportPauseBarrierError.CANCELLED)
            throw error
        }
    }

    fun resumePause() {
        val resume = synchronized(lock) {
            when (val state = pauseState) {
                is PauseState.Paused -> {
                    pauseState = nextPauseOperation?.let(PauseState::Armed) ?: PauseState.Idle
                    nextPauseOperation = null
                    state.resume
                }
                is PauseState.Failed -> throw state.error
                PauseState.Cancelled -> throw failPauseBarrier(TransportPauseBarrierError.CANCELLED)
                else -> throw failPauseBarrier(TransportPauseBarrierError.NOT_PAUSED)
            }
        }
        resume.complete(Unit)
    }

    fun cancelPauseBarrier() {
        failPauseBarrier(TransportPauseBarrierError.CANCELLED)
    }

    fun overridePausedRebuildCursor(cursor: String) {
        require(cursor.isNotEmpty() && cursor.toByteArray(Charsets.UTF_8).size <= 4_096) {
            "rebuild cursor override is invalid"
        }
        synchronized(lock) {
            val state = pauseState
            check(
                state is PauseState.Paused &&
                    state.operationClass == TransportOperationClass.REBUILD &&
                    rebuildCursorOverride == null,
            ) { "no paused rebuild response is available" }
            rebuildCursorOverride = cursor
        }
    }

    internal suspend fun pauseIfArmed(operationClass: TransportOperationClass): String? {
        val resume: CompletableDeferred<Unit>? = synchronized(lock) {
            when (val state = pauseState) {
                is PauseState.Armed -> if (state.operationClass == operationClass) {
                    CompletableDeferred<Unit>().also { deferred ->
                        pauseState = PauseState.Paused(operationClass, deferred)
                        pauseWaiter?.complete(Unit)
                        pauseWaiter = null
                    }
                } else {
                    null
                }
                is PauseState.Failed -> throw state.error
                PauseState.Cancelled -> throw failPauseBarrier(TransportPauseBarrierError.CANCELLED)
                else -> null
            }
        }
        try {
            resume?.await()
        } catch (error: CancellationException) {
            failPauseBarrier(TransportPauseBarrierError.CANCELLED)
            throw error
        }
        if (resume == null || operationClass != TransportOperationClass.REBUILD) return null
        return synchronized(lock) {
            rebuildCursorOverride.also { rebuildCursorOverride = null }
        }
    }

    internal fun record(
        operationClass: TransportOperationClass,
        statusCode: Int,
        errorCode: String? = null,
        durationNanoseconds: Long,
        cursorFingerprints: List<String>?,
        cursorFingerprintsComplete: Boolean?,
        requestFacts: TransportRequestFacts? = null,
        rebuildResponseFacts: TransportRebuildResponseFacts? = null,
        pullResponseFacts: TransportPullResponseFacts? = null,
    ) {
        synchronized(lock) {
            check(sequence < Long.MAX_VALUE) { "transport observation sequence exhausted" }
            sequence += 1
            if (observations.size == capacity) observations.removeFirst()
            observations.addLast(
                TransportObservation(
                    sequence = sequence,
                    operationClass = operationClass,
                    statusCode = statusCode,
                    errorCode = errorCode,
                    durationNanoseconds = durationNanoseconds,
                    cursorFingerprints = cursorFingerprints,
                    cursorFingerprintsComplete = cursorFingerprintsComplete,
                    requestFacts = requestFacts,
                    rebuildResponseFacts = rebuildResponseFacts,
                    pullResponseFacts = pullResponseFacts,
                ),
            )
        }
    }

    private fun failPauseBarrier(error: TransportPauseBarrierError): TransportPauseBarrierException {
        val failure = TransportPauseBarrierException(error)
        val deferred = synchronized(lock) {
            when (val state = pauseState) {
                is PauseState.Failed -> return state.error
                PauseState.Cancelled -> return TransportPauseBarrierException(TransportPauseBarrierError.CANCELLED)
                is PauseState.Paused -> state.resume
                else -> null
            }.also {
                pauseWaiter?.completeExceptionally(failure)
                pauseWaiter = null
                nextPauseOperation = null
                rebuildCursorOverride = null
                pauseState = if (error == TransportPauseBarrierError.CANCELLED) {
                    PauseState.Cancelled
                } else {
                    PauseState.Failed(failure)
                }
            }
        }
        deferred?.completeExceptionally(failure)
        return failure
    }

    companion object {
        const val maximumCursorFingerprints = 16

        fun cursorFingerprint(cursor: String): String = MessageDigest.getInstance("SHA-256")
            .digest(cursor.toByteArray(Charsets.UTF_8))
            .joinToString("") { byte -> "%02x".format(Locale.US, byte.toInt() and 0xff) }
    }
}
