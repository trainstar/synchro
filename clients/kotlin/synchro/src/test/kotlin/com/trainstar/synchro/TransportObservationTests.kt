package com.trainstar.synchro

import kotlinx.coroutines.async
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.TimeUnit

class TransportObservationTests {
    @Test
    fun operationClassificationUsesSyncRouteSegments() {
        assertEquals(TransportOperationClass.CONNECT, TransportOperationClass.classify("/sync/connect"))
        assertEquals(TransportOperationClass.PULL, TransportOperationClass.classify("/v1/sync/pull"))
        assertEquals(TransportOperationClass.SCHEMAS, TransportOperationClass.classify("/sync/schema"))
        assertEquals(TransportOperationClass.OTHER, TransportOperationClass.classify("/other/pull"))
    }

    @Test
    fun boundedSnapshotReportsCheckpointRelativeOverflow() {
        val collector = TransportObservationCollector(capacity = 2)
        repeat(3) { index ->
            collector.record(
                operationClass = TransportOperationClass.PULL,
                statusCode = 200,
                durationNanoseconds = index + 1L,
                cursorFingerprints = emptyList(),
                cursorFingerprintsComplete = true,
            )
        }

        val snapshot = collector.snapshot()
        assertEquals(listOf(2L, 3L), snapshot.observations.map { it.sequence })
        assertEquals(3L, snapshot.sequenceCheckpoint)
        assertTrue(snapshot.overflowed)
        assertFalse(collector.snapshot(after = 1).overflowed)
    }

    @Test
    fun pauseOccursAfterObservationAndRequiresResume() = runTest {
        val collector = TransportObservationCollector()
        collector.armPause(TransportOperationClass.PULL)
        val request = async {
            collector.record(
                operationClass = TransportOperationClass.PULL,
                statusCode = 200,
                durationNanoseconds = 1,
                cursorFingerprints = emptyList(),
                cursorFingerprintsComplete = true,
            )
            collector.pauseIfArmed(TransportOperationClass.PULL)
        }

        collector.awaitPause(TransportOperationClass.PULL, TimeUnit.SECONDS.toMillis(1))
        assertEquals(1, collector.snapshot().observations.size)
        assertFalse(request.isCompleted)
        collector.resumePause()
        request.await()
    }

    @Test
    fun cursorFingerprintsAreOpaqueAndStable() {
        val fingerprint = TransportObservationCollector.cursorFingerprint("private-cursor")

        assertEquals(64, fingerprint.length)
        assertTrue(fingerprint.matches(Regex("[0-9a-f]{64}")))
        assertEquals(fingerprint, TransportObservationCollector.cursorFingerprint("private-cursor"))
        assertFalse(fingerprint.contains("private-cursor"))
    }

    @Test
    fun configCarriesTheSdkCollectorWithoutChangingTheServerURL() {
        val collector = TransportObservationCollector()
        val config = SynchroConfig(
            dbPath = "test.db",
            serverURL = "https://sync.example.test",
            authProvider = { "token" },
            clientID = "client",
            appVersion = "1.0.0",
            transportObservationCollector = collector,
        )

        assertEquals("https://sync.example.test", config.serverURL)
        assertSame(collector, config.transportObservationCollector)
    }
}
