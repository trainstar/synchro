package com.trainstar.synchro

import kotlinx.coroutines.test.runTest
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.SocketPolicy
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test

class HttpClientTests {

    private lateinit var server: MockWebServer
    private lateinit var httpClient: HttpClient

    @Before
    fun setUp() {
        server = MockWebServer()
        server.start()

        val config = SynchroConfig(
            dbPath = "",
            serverURL = server.url("/").toString().trimEnd('/'),
            authProvider = { "test-token" },
            clientID = "test-device",
            appVersion = "1.0.0"
        )
        httpClient = HttpClient(config)
    }

    @After
    fun tearDown() {
        server.shutdown()
    }

    @Test
    fun testResponseBodyDisconnectWithoutDurableWorkIsNetworkError() = runTest {
        server.enqueue(
            MockResponse()
                .setBody("{" + "\"schema_version\":1,".repeat(2_000))
                .setSocketPolicy(SocketPolicy.DISCONNECT_DURING_RESPONSE_BODY),
        )

        try {
            httpClient.fetchSchema()
            fail("Expected retryable response-body disconnect")
        } catch (error: SynchroError.NetworkError) {
            assertNotNull(error.underlying)
        }
    }

    @Test
    fun testMalformedResponseBodyRemainsInvalidResponse() = runTest {
        server.enqueue(MockResponse().setBody("{"))

        assertThrows(SynchroError.InvalidResponse::class.java) {
            kotlinx.coroutines.runBlocking { httpClient.fetchSchema() }
        }
    }

    @Test
    fun testConnectRejectsNoncanonicalSuccessJson() = runTest {
        val responseBody = """
            {"server_time":"2026-03-20T18:22:11Z","protocol_version":3,"client_generation":4.0,"scope_set_version":13,"schema":{"version":8,"hash":"${"8".repeat(64)}","action":"none"},"scopes":{"add":[],"remove":[]},"scope_cursor_updates":{}}
        """.trimIndent()
        server.enqueue(MockResponse().setBody(responseBody).setResponseCode(200))

        val request = ConnectRequest(
            clientID = "test-device",
            platform = "android",
            appVersion = "1.0.0",
            protocolVersion = 3,
            schema = SchemaRef(version = 8, hash = "8".repeat(64)),
            scopeSetVersion = 13,
            knownScopes = emptyMap(),
        )
        try {
            httpClient.connect(request)
            fail("Expected invalid response")
        } catch (_: SynchroError.InvalidResponse) {
        }
    }

    @Test
    fun testSchemaMismatch422() = runTest {
        val currentHash = "b".repeat(64)
        val receivedHash = "a".repeat(64)
        val responseBody = """
            {
                "error": {
                    "code": "schema_mismatch",
                    "message": "client schema does not match server schema",
                    "retryable": false,
                    "current_schema": {"version": 2, "hash": "$currentHash"},
                    "received_schema": {"version": 1, "hash": "$receivedHash"}
                }
            }
        """.trimIndent()

        server.enqueue(MockResponse().setBody(responseBody).setResponseCode(422))

        val req = PullRequest(
            clientID = "test",
            clientGeneration = 1,
            schema = SchemaRef(version = 1, hash = "old"),
            scopeSetVersion = 0,
            scopes = emptyMap(),
            limit = 100
        )
        try {
            httpClient.pull(req)
            fail("Expected schemaMismatch error")
        } catch (e: SynchroError.SchemaMismatch) {
            assertEquals(2L, e.serverVersion)
            assertEquals(currentHash, e.serverHash)
        }
    }

    @Test
    fun testSchemaMismatch422RejectsMissingSchemaReferences() = runTest {
        server.enqueue(
            MockResponse().setBody(
                """{"error":{"code":"schema_mismatch","message":"schema changed","retryable":false}}""",
            ).setResponseCode(422),
        )

        val request = PullRequest(
            clientID = "test",
            clientGeneration = 1,
            schema = SchemaRef(1, "a".repeat(64)),
            scopeSetVersion = 0,
            scopes = emptyMap(),
            limit = 100,
        )
        assertThrows(SynchroError.InvalidResponse::class.java) {
            kotlinx.coroutines.runBlocking { httpClient.pull(request) }
        }
    }

    @Test
    fun testUpgradeRequired426() = runTest {
        server.enqueue(MockResponse().setBody("""{"error":"client upgrade required"}""").setResponseCode(426))

        val req = ConnectRequest(
            clientID = "test",
            platform = "android",
            appVersion = "0.1.0",
            protocolVersion = 3,
            schema = SchemaRef(version = 0, hash = ""),
            scopeSetVersion = 0,
            knownScopes = emptyMap()
        )
        try {
            httpClient.connect(req)
            fail("Expected upgradeRequired error")
        } catch (e: SynchroError.UpgradeRequired) {
            assertEquals("1.0.0", e.currentVersion)
        }
    }

    @Test
    fun testRetryAfter429() = runTest {
        server.enqueue(
            MockResponse()
                .setBody(RETRYABLE_429_ERROR_JSON)
                .setResponseCode(429)
                .setHeader("Retry-After", "10")
        )

        val req = PushRequest(
            clientID = "test",
            clientGeneration = 1,
            batchID = "batch-1",
            schema = SchemaRef(version = 1, hash = "abc"),
            mutations = emptyList()
        )
        try {
            httpClient.push(req)
            fail("Expected retryable error")
        } catch (e: RetryableError) {
            assertEquals(10.0, e.retryAfter!!, 0.01)
            assertTrue(e.underlying is SynchroError.ServerError)
            assertEquals(429, (e.underlying as SynchroError.ServerError).status)
            assertEquals(RetryOperation.PUSHING, e.interruptedOperation)
            assertEquals("batch-1", e.workIdentity)
            assertEquals(RetryClassification.HTTP_429, e.retryClassification)
        }
    }

    @Test
    fun testRetryAfter503() = runTest {
        server.enqueue(
            MockResponse()
                .setBody(RETRYABLE_503_ERROR_JSON)
                .setResponseCode(503)
                .setHeader("Retry-After", "5")
        )

        val req = PullRequest(
            clientID = "test",
            clientGeneration = 1,
            schema = SchemaRef(version = 1, hash = "abc"),
            scopeSetVersion = 0,
            scopes = emptyMap(),
            limit = 100
        )
        try {
            httpClient.pull(req)
            fail("Expected retryable error")
        } catch (e: RetryableError) {
            assertEquals(5.0, e.retryAfter!!, 0.01)
            assertEquals(RetryOperation.PULLING, e.interruptedOperation)
            assertEquals(RetryClassification.HTTP_503, e.retryClassification)
            assertEquals(server.takeRequest().body.readUtf8(), e.workIdentity)
        }
    }

    @Test
    fun malformedRetryResponsesAreInvalidAndNeverRetryable() = runTest {
        val request = PullRequest(
            clientID = "test",
            clientGeneration = 1,
            schema = SchemaRef(version = 1, hash = "abc"),
            scopeSetVersion = 0,
            scopes = emptyMap(),
            limit = 100,
        )
        val cases = listOf(
            MockResponse().setResponseCode(429).setHeader("Retry-After", "1").setBody(
                """{"error":{"code":"temporary_unavailable","message":"wrong code","retryable":true}}"""
            ),
            MockResponse().setResponseCode(429).setHeader("Retry-After", "1").setBody(
                """{"error":{"code":"retry_later","message":"not retryable","retryable":false}}"""
            ),
            MockResponse().setResponseCode(429).setBody(RETRYABLE_429_ERROR_JSON),
            MockResponse().setResponseCode(429).setHeader("Retry-After", "not-a-delay")
                .setBody(RETRYABLE_429_ERROR_JSON),
            MockResponse().setResponseCode(503).setHeader("Retry-After", "1").setBody(
                """{"error":{"code":"retry_later","message":"wrong code","retryable":true}}"""
            ),
            MockResponse().setResponseCode(503).setHeader("Retry-After", "1").setBody(
                """{"error":{"code":"capture_pending","message":"not retryable","retryable":false}}"""
            ),
            MockResponse().setResponseCode(503).setHeader("Retry-After", "1")
                .setBody("""{"error":"legacy error"}"""),
            MockResponse().setResponseCode(503).setBody(RETRYABLE_503_ERROR_JSON),
            MockResponse().setResponseCode(503).setHeader("Retry-After", "1").setBody(
                """{"error":{"code":"capture_pending","message":"unknown member","retryable":true,"extra":1}}"""
            ),
        )

        for (response in cases) {
            server.enqueue(response)
            val failure = runCatching { httpClient.pull(request) }.exceptionOrNull()
            assertTrue(failure is SynchroError.InvalidResponse)
            assertFalse(failure is RetryableError)
        }
    }

    @Test
    fun capturePending503WithRetryAfterIsRetryable() = runTest {
        server.enqueue(
            MockResponse()
                .setResponseCode(503)
                .setHeader("Retry-After", "1")
                .setBody(
                    """{"error":{"code":"capture_pending","message":"capture pending","retryable":true}}"""
                )
        )
        val request = PullRequest(
            clientID = "test",
            clientGeneration = 1,
            schema = SchemaRef(version = 1, hash = "abc"),
            scopeSetVersion = 0,
            scopes = emptyMap(),
            limit = 100,
        )

        val failure = runCatching { httpClient.pull(request) }.exceptionOrNull()

        assertTrue(failure is RetryableError)
        assertEquals(RetryClassification.HTTP_503, (failure as RetryableError).retryClassification)
    }

    @Test
    fun testServerError500() = runTest {
        server.enqueue(MockResponse().setBody("""{"error":"internal server error"}""").setResponseCode(500))

        val req = PullRequest(
            clientID = "test",
            clientGeneration = 1,
            schema = SchemaRef(version = 1, hash = "abc"),
            scopeSetVersion = 0,
            scopes = emptyMap(),
            limit = 100
        )
        try {
            httpClient.pull(req)
            fail("Expected serverError")
        } catch (e: SynchroError.ServerError) {
            assertEquals(500, e.status)
            assertEquals("internal server error", e.serverMessage)
        }
    }

}
