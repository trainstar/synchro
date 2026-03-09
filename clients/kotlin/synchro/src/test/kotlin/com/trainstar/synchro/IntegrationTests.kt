package com.trainstar.synchro

import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import org.junit.Assume.assumeNotNull
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.util.UUID
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

class IntegrationTests {

    private var serverURL: String? = null
    private var jwtSecret: String? = null

    @Before
    fun setUp() {
        serverURL = System.getenv("SYNCHRO_TEST_URL")
        jwtSecret = System.getenv("SYNCHRO_TEST_JWT_SECRET")
    }

    private fun skipIfNoServer() {
        assumeNotNull("SYNCHRO_TEST_URL not set", serverURL)
        assumeNotNull("SYNCHRO_TEST_JWT_SECRET not set", jwtSecret)
    }

    // -- JWT Helper --

    private fun signTestJWT(userID: String): String {
        val header = """{"alg":"HS256","typ":"JWT"}"""
        val now = System.currentTimeMillis() / 1000
        val exp = now + 3600
        val payload = """{"sub":"$userID","iat":$now,"exp":$exp}"""

        val headerB64 = base64URLEncode(header.toByteArray())
        val payloadB64 = base64URLEncode(payload.toByteArray())
        val signingInput = "$headerB64.$payloadB64"

        val signature = hmacSHA256(jwtSecret!!.toByteArray(), signingInput.toByteArray())
        return "$signingInput.${base64URLEncode(signature)}"
    }

    private fun base64URLEncode(data: ByteArray): String {
        return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(data)
    }

    private fun hmacSHA256(key: ByteArray, data: ByteArray): ByteArray {
        val mac = Mac.getInstance("HmacSHA256")
        mac.init(SecretKeySpec(key, "HmacSHA256"))
        return mac.doFinal(data)
    }

    // -- Helpers --

    private fun makeHttpClient(userID: String, clientID: String? = null, appVersion: String = "1.0.0"): Pair<HttpClient, String> {
        val cid = clientID ?: UUID.randomUUID().toString().lowercase()
        val token = signTestJWT(userID)
        val config = SynchroConfig(
            dbPath = "",
            serverURL = serverURL!!,
            authProvider = { token },
            clientID = cid,
            appVersion = appVersion,
            syncInterval = 999.0,
            maxRetryAttempts = 1
        )
        return Pair(HttpClient(config), cid)
    }

    private suspend fun register(http: HttpClient, clientID: String): RegisterResponse {
        return http.register(RegisterRequest(
            clientID = clientID,
            platform = "test",
            appVersion = "1.0.0",
            schemaVersion = 0,
            schemaHash = ""
        ))
    }

    private suspend fun bootstrapSnapshot(
        http: HttpClient,
        clientID: String,
        schemaVersion: Long,
        schemaHash: String
    ) {
        var cursor: SnapshotCursor? = null

        while (true) {
            val resp = http.snapshot(SnapshotRequest(
                clientID = clientID,
                cursor = cursor,
                limit = 100,
                schemaVersion = schemaVersion,
                schemaHash = schemaHash
            ))
            if (!resp.hasMore) {
                return
            }
            cursor = resp.cursor
        }
    }

    private suspend fun pushOrder(
        http: HttpClient,
        clientID: String,
        orderID: String,
        userID: String,
        shipAddress: String,
        schemaVersion: Long,
        schemaHash: String,
        clientUpdatedAt: String = java.time.Instant.now().toString()
    ): PushResponse {
        return http.push(PushRequest(
            clientID = clientID,
            changes = listOf(
                PushRecord(
                    id = orderID,
                    tableName = "orders",
                    operation = "create",
                    data = mapOf(
                        "id" to AnyCodable(orderID),
                        "user_id" to AnyCodable(userID),
                        "ship_address" to AnyCodable(shipAddress),
                    ),
                    clientUpdatedAt = clientUpdatedAt
                )
            ),
            schemaVersion = schemaVersion,
            schemaHash = schemaHash
        ))
    }

    private suspend fun pollForRecord(
        http: HttpClient,
        clientID: String,
        checkpoint: Long,
        schemaVersion: Long,
        schemaHash: String,
        recordID: String,
        timeoutMs: Long = 10000
    ): PullResponse {
        val deadline = System.currentTimeMillis() + timeoutMs
        var lastResponse: PullResponse? = null

        while (System.currentTimeMillis() < deadline) {
            val resp = http.pull(PullRequest(
                clientID = clientID,
                checkpoint = checkpoint,
                tables = null,
                limit = 100,
                knownBuckets = null,
                schemaVersion = schemaVersion,
                schemaHash = schemaHash
            ))
            lastResponse = resp
            if (resp.changes.any { it.id == recordID }) {
                return resp
            }
            delay(250)
        }

        return lastResponse ?: http.pull(PullRequest(
            clientID = clientID,
            checkpoint = checkpoint,
            tables = null,
            limit = 100,
            knownBuckets = null,
            schemaVersion = schemaVersion,
            schemaHash = schemaHash
        ))
    }

    // -- Test 1: Push and Pull Round Trip --

    @Test
    fun testPushAndPullRoundTrip() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, clientID) = makeHttpClient(userID)

        val reg = register(http, clientID)
        bootstrapSnapshot(http, clientID, reg.schemaVersion, reg.schemaHash)

        val orderID = UUID.randomUUID().toString().lowercase()
        val pushResp = pushOrder(
            http, clientID, orderID, userID,
            "123 Main St",
            reg.schemaVersion, reg.schemaHash
        )
        assertEquals(1, pushResp.accepted.size)
        assertEquals("applied", pushResp.accepted.first().status)

        val pullResp = pollForRecord(
            http, clientID, 0, reg.schemaVersion, reg.schemaHash, orderID
        )

        val found = pullResp.changes.firstOrNull { it.id == orderID }
        assertNotNull("pushed order should appear in pull response", found)
        assertEquals("orders", found?.tableName)
        assertNotNull("pulled record should contain 'ship_address' field", found?.data?.get("ship_address"))
    }

    // -- Test 2: Pull Checkpoint Advancement --

    @Test
    fun testPullCheckpointAdvancement() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, clientID) = makeHttpClient(userID)

        val reg = register(http, clientID)
        bootstrapSnapshot(http, clientID, reg.schemaVersion, reg.schemaHash)

        val orderIDs = mutableListOf<String>()
        for (i in 0 until 3) {
            val orderID = UUID.randomUUID().toString().lowercase()
            orderIDs.add(orderID)
            val resp = pushOrder(
                http, clientID, orderID, userID,
                "Checkpoint Address $i",
                reg.schemaVersion, reg.schemaHash
            )
            assertEquals(1, resp.accepted.size)
        }

        pollForRecord(
            http, clientID, 0, reg.schemaVersion, reg.schemaHash, orderIDs.last()
        )

        var checkpoint: Long = 0
        val allPulledIDs = mutableListOf<String>()
        var iterations = 0

        do {
            val pullResp = http.pull(PullRequest(
                clientID = clientID,
                checkpoint = checkpoint,
                tables = null,
                limit = 1,
                knownBuckets = null,
                schemaVersion = reg.schemaVersion,
                schemaHash = reg.schemaHash
            ))

            for (change in pullResp.changes) {
                allPulledIDs.add(change.id)
            }

            assertTrue("checkpoint must advance on each pull page", pullResp.checkpoint > checkpoint)
            checkpoint = pullResp.checkpoint
            iterations++

            if (!pullResp.hasMore) break
        } while (iterations < 20)

        for (orderID in orderIDs) {
            assertTrue("order $orderID should appear in paginated pull", allPulledIDs.contains(orderID))
        }
    }

    // -- Test 3: Conflict Resolution (LWW) --

    @Test
    fun testConflictResolution() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, clientID) = makeHttpClient(userID)

        val reg = register(http, clientID)
        bootstrapSnapshot(http, clientID, reg.schemaVersion, reg.schemaHash)

        val orderID = UUID.randomUUID().toString().lowercase()
        val createResp = pushOrder(
            http, clientID, orderID, userID,
            "456 Conflict Ave",
            reg.schemaVersion, reg.schemaHash
        )
        assertEquals(1, createResp.accepted.size)
        assertEquals("applied", createResp.accepted.first().status)

        val ancientDate = "2000-01-01T00:00:00.000Z"
        val conflictResp = http.push(PushRequest(
            clientID = clientID,
            changes = listOf(
                PushRecord(
                    id = orderID,
                    tableName = "orders",
                    operation = "update",
                    data = mapOf(
                        "id" to AnyCodable(orderID),
                        "user_id" to AnyCodable(userID),
                        "ship_address" to AnyCodable("789 Conflicting Blvd"),
                    ),
                    clientUpdatedAt = ancientDate,
                    baseUpdatedAt = ancientDate
                )
            ),
            schemaVersion = reg.schemaVersion,
            schemaHash = reg.schemaHash
        ))

        assertEquals("conflicting update should be rejected", 1, conflictResp.rejected.size)
        assertEquals("conflict", conflictResp.rejected.first().status)
        assertEquals(orderID, conflictResp.rejected.first().id)
        assertNotNull("rejected conflict should include server version",
            conflictResp.rejected.first().serverVersion)
    }

    // -- Test 4: Soft Delete Sync --

    @Test
    fun testSoftDeleteSync() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, clientID) = makeHttpClient(userID)

        val reg = register(http, clientID)
        bootstrapSnapshot(http, clientID, reg.schemaVersion, reg.schemaHash)

        val orderID = UUID.randomUUID().toString().lowercase()
        val createResp = pushOrder(
            http, clientID, orderID, userID,
            "999 Delete Lane",
            reg.schemaVersion, reg.schemaHash
        )
        assertEquals(1, createResp.accepted.size)

        val pullAfterCreate = pollForRecord(
            http, clientID, 0, reg.schemaVersion, reg.schemaHash, orderID
        )
        val checkpointAfterCreate = pullAfterCreate.checkpoint

        val deleteResp = http.push(PushRequest(
            clientID = clientID,
            changes = listOf(
                PushRecord(
                    id = orderID,
                    tableName = "orders",
                    operation = "delete",
                    data = mapOf(
                        "id" to AnyCodable(orderID),
                        "user_id" to AnyCodable(userID),
                    ),
                    clientUpdatedAt = java.time.Instant.now().toString()
                )
            ),
            schemaVersion = reg.schemaVersion,
            schemaHash = reg.schemaHash
        ))
        assertEquals("delete should be accepted", 1, deleteResp.accepted.size)
        assertNotNull("delete result should include server deleted_at timestamp",
            deleteResp.accepted.first().serverDeletedAt)

        val deadline = System.currentTimeMillis() + 10000
        var foundDelete = false
        var checkpoint = checkpointAfterCreate

        while (System.currentTimeMillis() < deadline && !foundDelete) {
            val pullResp = http.pull(PullRequest(
                clientID = clientID,
                checkpoint = checkpoint,
                tables = null,
                limit = 100,
                knownBuckets = null,
                schemaVersion = reg.schemaVersion,
                schemaHash = reg.schemaHash
            ))

            if (pullResp.deletes.any { it.id == orderID }) {
                foundDelete = true
                break
            }

            val record = pullResp.changes.firstOrNull { it.id == orderID }
            if (record?.deletedAt != null) {
                foundDelete = true
                break
            }

            checkpoint = pullResp.checkpoint
            if (!pullResp.hasMore) {
                delay(250)
            }
        }

        assertTrue("soft-deleted record should appear in pull as deleted", foundDelete)
    }

    // -- Test 5: Auth Failure --

    @Test
    fun testAuthFailure() = runTest {
        skipIfNoServer()

        val config = SynchroConfig(
            dbPath = "",
            serverURL = serverURL!!,
            authProvider = { "invalid-jwt-token" },
            clientID = UUID.randomUUID().toString().lowercase(),
            appVersion = "1.0.0",
            syncInterval = 999.0,
            maxRetryAttempts = 1
        )
        val http = HttpClient(config)

        try {
            http.fetchSchema()
            fail("expected auth failure with invalid JWT")
        } catch (e: SynchroError.ServerError) {
            assertEquals(401, e.status)
        }

        try {
            http.register(RegisterRequest(
                clientID = config.clientID,
                platform = "test",
                appVersion = "1.0.0",
                schemaVersion = 0,
                schemaHash = ""
            ))
            fail("expected auth failure on register")
        } catch (e: SynchroError.ServerError) {
            assertEquals(401, e.status)
        }
    }

    // -- Test 6: Version Mismatch --

    @Test
    fun testVersionMismatch() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, _) = makeHttpClient(userID, appVersion = "0.0.1")

        try {
            http.register(RegisterRequest(
                clientID = UUID.randomUUID().toString().lowercase(),
                platform = "test",
                appVersion = "0.0.1",
                schemaVersion = 0,
                schemaHash = ""
            ))
            fail("expected upgrade required error")
        } catch (_: SynchroError.UpgradeRequired) {
            // Expected
        }
    }

    // -- Test 7: Schema Drift Detection --

    @Test
    fun testSchemaDriftDetection() = runTest {
        skipIfNoServer()
        val dbURL = System.getenv("TEST_DATABASE_URL")
        assumeNotNull("TEST_DATABASE_URL not set", dbURL)

        val userID = UUID.randomUUID().toString().lowercase()
        val (http, clientID) = makeHttpClient(userID)

        val reg = register(http, clientID)
        val oldVersion = reg.schemaVersion
        val oldHash = reg.schemaHash

        // Alter the DB schema
        runSQL(dbURL!!, "ALTER TABLE orders ADD COLUMN _drift TEXT")
        try {
            val orderID = UUID.randomUUID().toString().lowercase()
            try {
                pushOrder(
                    http, clientID, orderID, userID,
                    "Should Be Rejected",
                    oldVersion, oldHash
                )
                fail("expected schemaMismatch error after schema drift")
            } catch (e: SynchroError.SchemaMismatch) {
                assertTrue("server schema version should advance after ALTER TABLE",
                    e.serverVersion > oldVersion)
                assertNotEquals("server schema hash should differ after column addition",
                    oldHash, e.serverHash)
            }
        } finally {
            runSQL(dbURL, "ALTER TABLE orders DROP COLUMN IF EXISTS _drift")
        }
    }

    private fun runSQL(dbURL: String, sql: String) {
        val process = ProcessBuilder("psql", dbURL, "-c", sql)
            .redirectErrorStream(true)
            .start()
        val exitCode = process.waitFor()
        if (exitCode != 0) {
            val output = process.inputStream.bufferedReader().readText()
            fail("psql failed: $output")
        }
    }

    // -- Test 8: Multi-User Isolation --

    @Test
    fun testMultiUserIsolation() = runTest {
        skipIfNoServer()

        val userA = UUID.randomUUID().toString().lowercase()
        val userB = UUID.randomUUID().toString().lowercase()
        val (httpA, clientA) = makeHttpClient(userA)
        val (httpB, clientB) = makeHttpClient(userB)

        val regA = register(httpA, clientA)
        val regB = register(httpB, clientB)
        bootstrapSnapshot(httpA, clientA, regA.schemaVersion, regA.schemaHash)
        bootstrapSnapshot(httpB, clientB, regB.schemaVersion, regB.schemaHash)

        val orderA = UUID.randomUUID().toString().lowercase()
        val pushA = pushOrder(
            httpA, clientA, orderA, userA,
            "User A Private Address",
            regA.schemaVersion, regA.schemaHash
        )
        assertEquals(1, pushA.accepted.size)

        val orderB = UUID.randomUUID().toString().lowercase()
        val pushB = pushOrder(
            httpB, clientB, orderB, userB,
            "User B Private Address",
            regB.schemaVersion, regB.schemaHash
        )
        assertEquals(1, pushB.accepted.size)

        val pullB = pollForRecord(
            httpB, clientB, 0, regB.schemaVersion, regB.schemaHash, orderB
        )
        assertTrue("User B should see their own order",
            pullB.changes.any { it.id == orderB })
        assertFalse("User B must not see User A's order (bucket isolation)",
            pullB.changes.any { it.id == orderA })

        val pullA = pollForRecord(
            httpA, clientA, 0, regA.schemaVersion, regA.schemaHash, orderA
        )
        assertTrue("User A should see their own order",
            pullA.changes.any { it.id == orderA })
        assertFalse("User A must not see User B's order (bucket isolation)",
            pullA.changes.any { it.id == orderB })
    }
}
