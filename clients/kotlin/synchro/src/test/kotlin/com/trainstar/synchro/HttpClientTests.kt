package com.trainstar.synchro

import kotlinx.coroutines.test.runTest
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import kotlinx.serialization.json.Json

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
    fun testRegisterSuccess() = runTest {
        val responseBody = """
            {
                "id": "server-id-123",
                "server_time": "2026-01-01T12:00:00.000Z",
                "checkpoint": 0,
                "schema_version": 1,
                "schema_hash": "abc123"
            }
        """.trimIndent()

        server.enqueue(MockResponse().setBody(responseBody).setResponseCode(200))

        val req = RegisterRequest(
            clientID = "test-device", platform = "android",
            appVersion = "1.0.0", schemaVersion = 0, schemaHash = ""
        )
        val resp = httpClient.register(req)

        assertEquals("server-id-123", resp.id)
        assertEquals(1L, resp.schemaVersion)
        assertEquals("abc123", resp.schemaHash)

        // Verify request
        val recorded = server.takeRequest()
        assertEquals("POST", recorded.method)
        assertTrue(recorded.path!!.endsWith("/sync/register"))
        assertEquals("Bearer test-token", recorded.getHeader("Authorization"))
        assertEquals("1.0.0", recorded.getHeader("X-App-Version"))
        assertTrue(recorded.getHeader("Content-Type")!!.startsWith("application/json"))

        val body = Json.decodeFromString<Map<String, kotlinx.serialization.json.JsonElement>>(recorded.body.readUtf8())
        assertEquals("\"test-device\"", body["client_id"].toString())
        assertEquals("\"android\"", body["platform"].toString())
    }

    @Test
    fun testFetchSchemaSuccess() = runTest {
        val responseBody = """
            {
                "schema_version": 3,
                "schema_hash": "def456",
                "server_time": "2026-01-01T12:00:00.000Z",
                "tables": [
                    {
                        "table_name": "orders",
                        "push_policy": "owner_only",
                        "updated_at_column": "updated_at",
                        "deleted_at_column": "deleted_at",
                        "primary_key": ["id"],
                        "columns": [
                            {"name": "id", "db_type": "uuid", "logical_type": "string", "nullable": false, "default_kind": "none", "is_primary_key": true}
                        ]
                    }
                ]
            }
        """.trimIndent()

        server.enqueue(MockResponse().setBody(responseBody).setResponseCode(200))

        val resp = httpClient.fetchSchema()
        assertEquals(3L, resp.schemaVersion)
        assertEquals(1, resp.tables.size)
        assertEquals("orders", resp.tables[0].tableName)

        val recorded = server.takeRequest()
        assertEquals("GET", recorded.method)
        assertTrue(recorded.path!!.endsWith("/sync/schema"))
    }

    @Test
    fun testConnectVNextSuccess() = runTest {
        val responseBody = """
            {
                "server_time": "2026-03-20T18:22:11Z",
                "protocol_version": 1,
                "scope_set_version": 13,
                "schema": {
                    "version": 8,
                    "hash": "8b21d2a1",
                    "action": "none"
                },
                "scopes": {
                    "add": [],
                    "remove": []
                }
            }
        """.trimIndent()

        server.enqueue(MockResponse().setBody(responseBody).setResponseCode(200))

        val req = VNextConnectRequest(
            clientID = "test-device",
            platform = "android",
            appVersion = "1.0.0",
            protocolVersion = 1,
            schema = VNextSchemaRef(version = 8, hash = "8b21d2a1"),
            scopeSetVersion = 13,
            knownScopes = emptyMap()
        )
        val resp = httpClient.connect(req)

        assertEquals(VNextSchemaAction.NONE, resp.schema.action)
        resp.validate()

        val recorded = server.takeRequest()
        assertEquals("POST", recorded.method)
        assertTrue(recorded.path!!.endsWith("/sync/connect"))
        val body = Json.decodeFromString<Map<String, kotlinx.serialization.json.JsonElement>>(recorded.body.readUtf8())
        assertEquals("\"test-device\"", body["client_id"].toString())
        assertEquals("1", body["protocol_version"].toString())
    }

    @Test
    fun testPullVNextEncoding() = runTest {
        val responseBody = """
            {
                "changes": [],
                "scope_set_version": 13,
                "scope_cursors": {
                    "workouts_user:u_123": "c_890"
                },
                "scope_updates": {
                    "add": [],
                    "remove": []
                },
                "rebuild": [],
                "has_more": false,
                "checksums": {
                    "workouts_user:u_123": "cs_a19d"
                }
            }
        """.trimIndent()

        server.enqueue(MockResponse().setBody(responseBody).setResponseCode(200))

        val req = VNextPullRequest(
            clientID = "test-device",
            schema = VNextSchemaRef(version = 8, hash = "8b21d2a1"),
            scopeSetVersion = 13,
            scopes = mapOf("workouts_user:u_123" to VNextScopeCursorRef(cursor = "c_890")),
            limit = 100,
            checksumMode = VNextChecksumMode.REQUIRED
        )
        val resp = httpClient.pull(req)

        resp.validate(req)
        assertEquals(13L, resp.scopeSetVersion)
        assertEquals("c_890", resp.scopeCursors["workouts_user:u_123"])

        val recorded = server.takeRequest()
        assertEquals("POST", recorded.method)
        assertTrue(recorded.path!!.endsWith("/sync/pull"))
        val body = Json.decodeFromString<Map<String, kotlinx.serialization.json.JsonElement>>(recorded.body.readUtf8())
        assertEquals("\"test-device\"", body["client_id"].toString())
        assertEquals("13", body["scope_set_version"].toString())
        assertEquals("\"required\"", body["checksum_mode"].toString())
    }

    @Test
    fun testSchemaMismatch422() = runTest {
        val responseBody = """
            {
                "error": {
                    "code": "schema_mismatch",
                    "message": "client schema does not match server schema",
                    "retryable": false
                }
            }
        """.trimIndent()

        server.enqueue(MockResponse().setBody(responseBody).setResponseCode(422))

        val req = VNextPullRequest(
            clientID = "test",
            schema = VNextSchemaRef(version = 1, hash = "old"),
            scopeSetVersion = 0,
            scopes = emptyMap(),
            limit = 100,
            checksumMode = VNextChecksumMode.NONE
        )
        try {
            httpClient.pull(req)
            fail("Expected schemaMismatch error")
        } catch (e: SynchroError.SchemaMismatch) {
            assertEquals(0L, e.serverVersion)
            assertEquals("", e.serverHash)
        }
    }

    @Test
    fun testUpgradeRequired426() = runTest {
        server.enqueue(MockResponse().setBody("""{"error":"client upgrade required"}""").setResponseCode(426))

        val req = RegisterRequest(clientID = "test", platform = "android", appVersion = "0.1.0", schemaVersion = 0, schemaHash = "")
        try {
            httpClient.register(req)
            fail("Expected upgradeRequired error")
        } catch (e: SynchroError.UpgradeRequired) {
            assertEquals("1.0.0", e.currentVersion)
        }
    }

    @Test
    fun testRetryAfter429() = runTest {
        server.enqueue(
            MockResponse()
                .setBody("""{"error":"rate limited"}""")
                .setResponseCode(429)
                .setHeader("Retry-After", "10")
        )

        val req = PushRequest(clientID = "test", changes = emptyList(), schemaVersion = 1, schemaHash = "abc")
        try {
            httpClient.push(req)
            fail("Expected retryable error")
        } catch (e: RetryableError) {
            assertEquals(10.0, e.retryAfter!!, 0.01)
            assertTrue(e.underlying is SynchroError.ServerError)
            assertEquals(429, (e.underlying as SynchroError.ServerError).status)
        }
    }

    @Test
    fun testRetryAfter503() = runTest {
        server.enqueue(
            MockResponse()
                .setBody("""{"error":"service temporarily unavailable"}""")
                .setResponseCode(503)
                .setHeader("Retry-After", "5")
        )

        val req = PullRequest(clientID = "test", checkpoint = 0, schemaVersion = 1, schemaHash = "abc")
        try {
            httpClient.pull(req)
            fail("Expected retryable error")
        } catch (e: RetryableError) {
            assertEquals(5.0, e.retryAfter!!, 0.01)
        }
    }

    @Test
    fun testServerError500() = runTest {
        server.enqueue(MockResponse().setBody("""{"error":"internal server error"}""").setResponseCode(500))

        val req = PullRequest(clientID = "test", checkpoint = 0, schemaVersion = 1, schemaHash = "abc")
        try {
            httpClient.pull(req)
            fail("Expected serverError")
        } catch (e: SynchroError.ServerError) {
            assertEquals(500, e.status)
            assertEquals("internal server error", e.serverMessage)
        }
    }

    @Test
    fun testPullRequestEncoding() = runTest {
        val pullResponseBody = """
            {
                "changes": [],
                "deletes": [],
                "checkpoint": 42,
                "has_more": false,
                "schema_version": 1,
                "schema_hash": "abc"
            }
        """.trimIndent()

        server.enqueue(MockResponse().setBody(pullResponseBody).setResponseCode(200))

        val req = PullRequest(
            clientID = "dev-1",
            checkpoint = 100,
            tables = listOf("orders"),
            limit = 50,
            knownBuckets = listOf("user:123", "global"),
            schemaVersion = 7,
            schemaHash = "hash7"
        )
        httpClient.pull(req)

        val recorded = server.takeRequest()
        val body = Json.decodeFromString<kotlinx.serialization.json.JsonObject>(recorded.body.readUtf8())
        assertEquals("\"dev-1\"", body["client_id"].toString())
        assertEquals("100", body["checkpoint"].toString())
        assertEquals("[\"orders\"]", body["tables"].toString())
        assertEquals("50", body["limit"].toString())
        assertEquals("[\"user:123\",\"global\"]", body["known_buckets"].toString())
        assertEquals("7", body["schema_version"].toString())
        assertEquals("\"hash7\"", body["schema_hash"].toString())
    }

    @Test
    fun testFetchTablesSuccess() = runTest {
        val responseBody = """
            {
                "server_time": "2026-01-01T12:00:00.000Z",
                "schema_version": 2,
                "schema_hash": "xyz",
                "tables": [
                    {
                        "table_name": "orders",
                        "push_policy": "owner_only",
                        "dependencies": []
                    }
                ]
            }
        """.trimIndent()

        server.enqueue(MockResponse().setBody(responseBody).setResponseCode(200))

        val resp = httpClient.fetchTables()
        assertEquals(2L, resp.schemaVersion)
        assertEquals(1, resp.tables.size)
        assertEquals("orders", resp.tables[0].tableName)
        assertEquals("owner_only", resp.tables[0].pushPolicy)

        val recorded = server.takeRequest()
        assertEquals("GET", recorded.method)
        assertTrue(recorded.path!!.endsWith("/sync/tables"))
    }

    @Test
    fun testPushRequestEncoding() = runTest {
        val pushResponseBody = """
            {
                "accepted": [],
                "rejected": [],
                "checkpoint": 0,
                "server_time": "2026-01-01T12:00:00.000Z",
                "schema_version": 1,
                "schema_hash": "abc"
            }
        """.trimIndent()

        server.enqueue(MockResponse().setBody(pushResponseBody).setResponseCode(200))

        val req = PushRequest(
            clientID = "dev-1",
            changes = listOf(
                PushRecord(
                    id = "rec-1",
                    tableName = "orders",
                    operation = "create",
                    data = mapOf("ship_address" to AnyCodable("123 Main St")),
                    clientUpdatedAt = "2026-01-01T12:00:00.000Z"
                )
            ),
            schemaVersion = 7,
            schemaHash = "hash7"
        )
        httpClient.push(req)

        val recorded = server.takeRequest()
        val body = Json.decodeFromString<kotlinx.serialization.json.JsonObject>(recorded.body.readUtf8())
        assertEquals("\"dev-1\"", body["client_id"].toString())
        val changes = body["changes"] as kotlinx.serialization.json.JsonArray
        assertEquals(1, changes.size)
        val change = changes[0] as kotlinx.serialization.json.JsonObject
        assertEquals("\"rec-1\"", change["id"].toString())
        assertEquals("\"create\"", change["operation"].toString())
    }
}
