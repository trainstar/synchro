package com.trainstar.synchro

import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test

class SynchroConfigTests {
    private val config = SynchroConfig(
        dbPath = "unused",
        serverURL = "http://localhost:8080",
        authProvider = { error("configuration must not request authentication") },
        clientID = "config-test",
        appVersion = "1.0.0",
    )

    @Test
    fun invalidSettingsFailDuringConstruction() {
        for (value in listOf(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, -1.0)) {
            assertThrows(IllegalArgumentException::class.java) { config.copy(syncInterval = value) }
            assertThrows(IllegalArgumentException::class.java) { config.copy(pushDebounce = value) }
        }
        for (value in listOf(-1, Int.MIN_VALUE)) {
            assertThrows(IllegalArgumentException::class.java) { config.copy(maxRetryAttempts = value) }
        }
        for (value in listOf(Int.MIN_VALUE, -1, 0, 1001, Int.MAX_VALUE)) {
            assertThrows(IllegalArgumentException::class.java) { config.copy(pullPageSize = value) }
            assertThrows(IllegalArgumentException::class.java) { config.copy(pushBatchSize = value) }
        }
    }

    @Test
    fun validLimitsPreserveManualSchedulingAndZeroRetries() {
        val manual = config.copy(syncInterval = 0.0, pushDebounce = 0.0, maxRetryAttempts = 0, pullPageSize = 1, pushBatchSize = 1)
        assertEquals(0.0, manual.syncInterval, 0.0)
        assertEquals(0.0, manual.pushDebounce, 0.0)
        assertEquals(0, manual.maxRetryAttempts)
        assertEquals(1, manual.effectivePullPageSize)
        val maximum = config.copy(pullPageSize = 1000, pushBatchSize = 1000, maxRetryAttempts = Int.MAX_VALUE)
        assertEquals(1000, maximum.effectivePullPageSize)
        assertEquals(1000, maximum.pushBatchSize)
        assertEquals(Int.MAX_VALUE, maximum.maxRetryAttempts)
    }
}
