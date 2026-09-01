@file:OptIn(com.trainstar.synchro.inspection.SynchroProofApi::class)

package com.trainstar.synchro

import com.trainstar.synchro.inspection.TransportObservationCollector
import com.trainstar.synchro.inspection.TransportOperationClass
import com.trainstar.synchro.inspection.TransportPauseBarrierError
import com.trainstar.synchro.inspection.TransportPauseBarrierException
import com.trainstar.synchro.inspection.TransportRequestFacts
import com.trainstar.synchro.inspection.withTransportObservation
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertSame
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import java.net.ServerSocket
import java.util.concurrent.TimeUnit

class TransportObservationTests {
    @Test
    fun operationClassificationUsesSyncRouteSegments() {
        assertEquals(TransportOperationClass.CONNECT, TransportOperationClass.classify("/sync/connect"))
        assertEquals(TransportOperationClass.PULL, TransportOperationClass.classify("/v1/sync/pull"))
        assertEquals(TransportOperationClass.SCHEMAS, TransportOperationClass.classify("/sync/schema"))
        assertEquals(TransportOperationClass.OTHER, TransportOperationClass.classify("/other/pull"))
        assertEquals(TransportOperationClass.PULL, TransportOperationClass.fromWire("pull"))
        assertEquals(null, TransportOperationClass.fromWire("PULL"))
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
    fun rebuildCursorOverrideBelongsOnlyToThePausedResponse() = runTest {
        val collector = TransportObservationCollector()
        collector.armPause(TransportOperationClass.REBUILD)
        val pausedRequest = async { collector.pauseIfArmed(TransportOperationClass.REBUILD) }
        collector.awaitPause(TransportOperationClass.REBUILD, TimeUnit.SECONDS.toMillis(1))
        collector.overridePausedRebuildCursor("forged-cursor")

        assertEquals(null, collector.pauseIfArmed(TransportOperationClass.REBUILD))
        collector.resumePause()

        assertEquals("forged-cursor", pausedRequest.await())
        assertEquals(null, collector.pauseIfArmed(TransportOperationClass.REBUILD))
    }

    @Test
    fun secondArmFailsTheBarrierClosed() {
        val collector = TransportObservationCollector()
        collector.armPause(TransportOperationClass.PULL)

        val failure = assertThrows(TransportPauseBarrierException::class.java) {
            collector.armPause(TransportOperationClass.PUSH)
        }
        assertEquals(TransportPauseBarrierError.ALREADY_ARMED, failure.error)
        assertSame(
            failure,
            assertThrows(TransportPauseBarrierException::class.java) { collector.resumePause() },
        )
    }

    @Test
    fun secondQueuedArmFailsThePausedRequestClosed() = runTest {
        supervisorScope {
            val collector = TransportObservationCollector()
            collector.armPause(TransportOperationClass.PULL)
            val request = async { collector.pauseIfArmed(TransportOperationClass.PULL) }
            collector.awaitPause(TransportOperationClass.PULL, TimeUnit.SECONDS.toMillis(1))
            collector.armPause(TransportOperationClass.PUSH)

            val failure = assertThrows(TransportPauseBarrierException::class.java) {
                collector.armPause(TransportOperationClass.CONNECT)
            }
            assertEquals(TransportPauseBarrierError.ALREADY_ARMED, failure.error)
            try {
                request.await()
                throw AssertionError("Expected the paused request to fail")
            } catch (error: TransportPauseBarrierException) {
                assertSame(failure, error)
            }
        }
    }

    @Test
    fun wrongAwaitOperationFailsTheBarrierClosed() = runTest {
        val collector = TransportObservationCollector()
        collector.armPause(TransportOperationClass.PULL)

        val failure = try {
            collector.awaitPause(TransportOperationClass.PUSH, TimeUnit.SECONDS.toMillis(1))
            throw AssertionError("Expected the wrong operation to fail")
        } catch (error: TransportPauseBarrierException) {
            error
        }
        assertEquals(TransportPauseBarrierError.WRONG_OPERATION, failure.error)
        assertSame(
            failure,
            assertThrows(TransportPauseBarrierException::class.java) {
                collector.armPause(TransportOperationClass.PULL)
            },
        )
    }

    @Test
    fun invalidPauseTimeoutFailsTheBarrierClosed() = runTest {
        val collector = TransportObservationCollector()
        collector.armPause(TransportOperationClass.PULL)

        val failure = try {
            collector.awaitPause(TransportOperationClass.PULL, 0)
            throw AssertionError("Expected the invalid timeout to fail")
        } catch (error: TransportPauseBarrierException) {
            error
        }
        assertEquals(TransportPauseBarrierError.TIMED_OUT, failure.error)
        assertSame(
            failure,
            assertThrows(TransportPauseBarrierException::class.java) {
                collector.armPause(TransportOperationClass.PULL)
            },
        )
    }

    @Test
    fun resumeWhileIdleFailsTheBarrierClosed() {
        val collector = TransportObservationCollector()

        val failure = assertThrows(TransportPauseBarrierException::class.java) { collector.resumePause() }
        assertEquals(TransportPauseBarrierError.NOT_PAUSED, failure.error)
        assertSame(
            failure,
            assertThrows(TransportPauseBarrierException::class.java) {
                collector.armPause(TransportOperationClass.PULL)
            },
        )
    }

    @Test
    fun cancellingPausedTransportCancelsTheBarrier() = runTest {
        val collector = TransportObservationCollector()
        collector.armPause(TransportOperationClass.PULL)
        val request = async { collector.pauseIfArmed(TransportOperationClass.PULL) }
        collector.awaitPause(TransportOperationClass.PULL, TimeUnit.SECONDS.toMillis(1))

        request.cancelAndJoin()

        val failure = assertThrows(TransportPauseBarrierException::class.java) { collector.resumePause() }
        assertEquals(TransportPauseBarrierError.CANCELLED, failure.error)
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
        ).withTransportObservation(collector)

        assertEquals("https://sync.example.test", config.serverURL)
        assertSame(collector, config.transportObservationCollector)
    }

    @Test
    fun networkFailureRecordsSafeTransportObservation() = runTest {
        val collector = TransportObservationCollector()
        val unavailablePort = ServerSocket(0).use { it.localPort }
        val client = HttpClient(
            SynchroConfig(
                dbPath = "",
                serverURL = "http://127.0.0.1:$unavailablePort",
                authProvider = { "test-token" },
                clientID = "test-device",
                appVersion = "1.0.0",
            ).withTransportObservation(collector),
        )

        assertTrue(runCatching { client.fetchSchema() }.exceptionOrNull() is SynchroError.NetworkError)

        val observation = collector.snapshot().observations.single()
        assertEquals(TransportOperationClass.SCHEMAS, observation.operationClass)
        assertEquals(0, observation.statusCode)
        assertTrue(observation.durationNanoseconds > 0)
        assertEquals(null, observation.requestFacts)
    }

    @Test
    fun HTTPFailuresPreserveReportedErrorCodes() = runTest {
        val server = MockWebServer()
        try {
            server.enqueue(
                MockResponse()
                    .setResponseCode(503)
                    .setHeader("Retry-After", "1")
                    .setBody("""{"error":{"code":"temporary_unavailable","message":"temporarily unavailable","retryable":true}}"""),
            )
            server.enqueue(MockResponse().setResponseCode(500).setBody("""{"error":"internal error"}"""))
            server.start()
            val collector = TransportObservationCollector()
            val client = HttpClient(
                SynchroConfig(
                    dbPath = "",
                    serverURL = server.url("/").toString().trimEnd('/'),
                    authProvider = { "test-token" },
                    clientID = "test-device",
                    appVersion = "1.0.0",
                ).withTransportObservation(collector),
            )

            assertTrue(runCatching { client.fetchSchema() }.exceptionOrNull() is SynchroError.ServerError)
            assertTrue(runCatching { client.fetchSchema() }.exceptionOrNull() is SynchroError.ServerError)

            val observations = collector.snapshot().observations
            assertEquals("temporary_unavailable", observations[0].errorCode)
            assertEquals(null, observations[1].errorCode)
        } finally {
            server.shutdown()
        }
    }

    @Test
    @OptIn(ExperimentalSerializationApi::class)
    fun requestFactsSerializeMutationCountOnlyWhenPresent() {
        val json = Json { explicitNulls = false }
        val push = TransportRequestFacts(schemaVersion = 1, schemaHash = "hash", mutationCount = 3)
        val pull = TransportRequestFacts(schemaVersion = 1, schemaHash = "hash")

        assertTrue(json.encodeToString(push).contains("\"mutation_count\":3"))
        assertFalse(json.encodeToString(pull).contains("mutation_count"))
    }
}
