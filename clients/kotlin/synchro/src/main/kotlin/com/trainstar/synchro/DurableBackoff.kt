package com.trainstar.synchro

import android.database.sqlite.SQLiteDatabase
import kotlinx.coroutines.delay
import kotlin.math.ceil

internal data class DurableBackoffRecord(
    val resumeState: String,
    val workIdentity: String,
    val retryClassification: String,
    val attemptCount: Long,
    val nextRetryAtMs: Long,
)

internal interface RetryTiming {
    fun currentTimeMillis(): Long
    fun jitterFraction(): Double
    suspend fun sleep(delayMillis: Long)
}

internal object SystemRetryTiming : RetryTiming {
    override fun currentTimeMillis(): Long = System.currentTimeMillis()
    override fun jitterFraction(): Double = Math.random()
    override suspend fun sleep(delayMillis: Long) = delay(delayMillis)
}

internal object DurableBackoffStore {
    fun load(database: SynchroDatabase): DurableBackoffRecord? =
        database.readTransaction(::load)

    fun persist(
        database: SynchroDatabase,
        error: RetryableError,
        currentTimeMillis: Long,
        fallbackDelaySeconds: (Long) -> Double,
    ): DurableBackoffRecord = database.writeTransaction { db ->
        val previous = load(db)
        val attemptCount = if (
            previous?.resumeState == error.interruptedOperation &&
            previous.workIdentity == error.workIdentity
        ) {
            if (previous.attemptCount == Long.MAX_VALUE) Long.MAX_VALUE else previous.attemptCount + 1
        } else {
            1L
        }
        // Compute jitter for every retry. Retry-After is a lower bound, never a
        // replacement for native exponential backoff.
        val exponentialDelay = fallbackDelaySeconds(attemptCount)
        if (!exponentialDelay.isFinite() || exponentialDelay < 0.0) {
            throw SynchroError.InvalidResponse("native retry delay is invalid")
        }
        val nativeDeadline = safeAdd(currentTimeMillis, secondsToMillis(exponentialDelay))
        val retryAfterDeadline = error.retryAfterDeadlineMs ?: error.retryAfter
            ?.takeIf { it.isFinite() && it >= 0.0 }
            ?.let { delay -> safeAdd(currentTimeMillis, secondsToMillis(delay)) }
        val nextRetryAtMs = maxOf(nativeDeadline, retryAfterDeadline ?: Long.MIN_VALUE)
        val record = DurableBackoffRecord(
            resumeState = error.interruptedOperation,
            workIdentity = error.workIdentity,
            retryClassification = error.retryClassification,
            attemptCount = attemptCount,
            nextRetryAtMs = nextRetryAtMs,
        )
        executeUpsert(
            db,
            table = "_synchro_backoff",
            keyColumns = listOf("singleton"),
            keyValues = listOf(1L),
            dataColumns = listOf(
                "resume_state", "work_identity", "retry_classification",
                "attempt_count", "next_retry_at_ms",
            ),
            dataValues = listOf(
                record.resumeState,
                record.workIdentity,
                record.retryClassification,
                record.attemptCount,
                record.nextRetryAtMs,
            ),
        )
        record
    }

    fun clearMatching(
        db: SQLiteDatabase,
        resumeState: String,
        workIdentity: String,
    ) {
        db.execSQL(
            "DELETE FROM _synchro_backoff WHERE singleton = 1 AND resume_state = ? AND work_identity = ?",
            arrayOf(resumeState, workIdentity),
        )
    }

    /** Pull and rebuild requests bind old cursors and cannot survive a schema reset. */
    fun clearSchemaResetWork(db: SQLiteDatabase) {
        val record = load(db) ?: return
        if (record.resumeState in setOf(RetryOperation.PULLING, RetryOperation.REBUILDING)) {
            clearMatching(db, record.resumeState, record.workIdentity)
        }
    }

    fun load(db: SQLiteDatabase): DurableBackoffRecord? {
        db.rawQuery(
            """
            SELECT resume_state, work_identity, retry_classification, attempt_count, next_retry_at_ms
            FROM _synchro_backoff
            WHERE singleton = 1
            """.trimIndent(),
            null,
        ).use { cursor ->
            if (!cursor.moveToFirst()) return null
            val record = DurableBackoffRecord(
                resumeState = cursor.getString(0),
                workIdentity = cursor.getString(1),
                retryClassification = cursor.getString(2),
                attemptCount = cursor.getLong(3),
                nextRetryAtMs = cursor.getLong(4),
            )
            if (record.resumeState !in RESUME_STATES ||
                record.workIdentity.isEmpty() ||
                record.retryClassification !in RETRY_CLASSIFICATIONS ||
                record.attemptCount <= 0
            ) {
                throw SynchroError.InvalidResponse("durable backoff record is invalid")
            }
            return record
        }
    }

    private fun secondsToMillis(seconds: Double): Long {
        if (!seconds.isFinite() || seconds <= 0.0) return 0L
        if (seconds >= Long.MAX_VALUE.toDouble() / 1_000.0) return Long.MAX_VALUE
        return ceil(seconds * 1_000.0).toLong()
    }

    private fun safeAdd(left: Long, right: Long): Long {
        if (right > 0 && left > Long.MAX_VALUE - right) return Long.MAX_VALUE
        return left + right
    }

    private val RESUME_STATES = setOf(
        RetryOperation.CONNECTING,
        RetryOperation.PUSHING,
        RetryOperation.PULLING,
        RetryOperation.REBUILDING,
    )
    private val RETRY_CLASSIFICATIONS = setOf(
        RetryClassification.NETWORK,
        RetryClassification.HTTP_429,
        RetryClassification.HTTP_503,
    )
}
