@file:OptIn(com.trainstar.synchro.inspection.SynchroProofApi::class)

package com.trainstar.synchro

import com.trainstar.synchro.inspection.TransportObservationCollector

data class SynchroConfig(
    val dbPath: String,
    val serverURL: String,
    val authProvider: suspend () -> String,
    val clientID: String,
    val platform: String = "android",
    val appVersion: String,
    /** Zero disables periodic polling. Explicit sync and durable retry wakeups remain available. */
    val syncInterval: Double = 30.0,
    val pushDebounce: Double = 0.5,
    val maxRetryAttempts: Int = 5,
    val pullPageSize: Int = 100,
    val pushBatchSize: Int = 100,
    val seedDatabasePath: String? = null,
) {
    internal var transportObservationCollector: TransportObservationCollector? = null
        private set

    init {
        require(syncInterval.isFinite() && syncInterval >= 0.0) { "syncInterval must be finite and nonnegative" }
        require(pushDebounce.isFinite() && pushDebounce >= 0.0) { "pushDebounce must be finite and nonnegative" }
        require(maxRetryAttempts >= 0) { "maxRetryAttempts must be nonnegative" }
        require(pullPageSize in 1..1000) { "pullPageSize must be between 1 and 1000" }
        require(pushBatchSize in 1..1000) { "pushBatchSize must be between 1 and 1000" }
    }

    val effectivePullPageSize: Int get() = pullPageSize

    internal fun withTransportObservationCollector(collector: TransportObservationCollector): SynchroConfig =
        copy().also { it.transportObservationCollector = collector }
}
