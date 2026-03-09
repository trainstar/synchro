package com.trainstar.synchro

data class SynchroConfig(
    val dbPath: String,
    val serverURL: String,
    val authProvider: suspend () -> String,
    val clientID: String,
    val platform: String = "android",
    val appVersion: String,
    val syncInterval: Double = 30.0,
    val pushDebounce: Double = 0.5,
    val maxRetryAttempts: Int = 5,
    val pullPageSize: Int = 100,
    val pushBatchSize: Int = 100,
    val snapshotPageSize: Int = 100
) {
    init {
        require(pullPageSize in 1..1000) { "pullPageSize must be between 1 and 1000" }
        require(snapshotPageSize in 1..1000) { "snapshotPageSize must be between 1 and 1000" }
        require(pushBatchSize in 1..1000) { "pushBatchSize must be between 1 and 1000" }
    }

    val effectivePullPageSize: Int get() = pullPageSize.coerceAtMost(1000)
    val effectiveSnapshotPageSize: Int get() = snapshotPageSize.coerceAtMost(1000)
}
