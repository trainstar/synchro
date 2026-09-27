package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import com.trainstar.synchro.inspection.SynchroProofApi
import com.trainstar.synchro.inspection.TransportObservationCollector
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
import java.io.Closeable
import java.nio.file.Files
import java.nio.file.Paths
import java.security.MessageDigest
import java.util.UUID
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28])
class IntegrationTests {
    private lateinit var serverURL: String
    private lateinit var jwtSecret: String

    @Before
    fun setUp() {
        serverURL = checkNotNull(System.getenv("SYNCHRO_TEST_URL")) {
            "SYNCHRO_TEST_URL must be set for integration tests"
        }
        jwtSecret = checkNotNull(System.getenv("SYNCHRO_TEST_JWT_SECRET")) {
            "SYNCHRO_TEST_JWT_SECRET must be set for integration tests"
        }
    }

    private fun signTestJWT(userID: String): String {
        val header = """{"alg":"HS256","typ":"JWT"}"""
        val now = System.currentTimeMillis() / 1000
        val exp = now + 3600
        val payload = """{"sub":"$userID","iat":$now,"exp":$exp}"""

        val headerB64 = base64URLEncode(header.toByteArray())
        val payloadB64 = base64URLEncode(payload.toByteArray())
        val signingInput = "$headerB64.$payloadB64"
        val signature = hmacSHA256(jwtSecret.toByteArray(), signingInput.toByteArray())
        return "$signingInput.${base64URLEncode(signature)}"
    }

    private fun base64URLEncode(data: ByteArray): String =
        java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(data)

    private fun hmacSHA256(key: ByteArray, data: ByteArray): ByteArray {
        val mac = Mac.getInstance("HmacSHA256")
        mac.init(SecretKeySpec(key, "HmacSHA256"))
        return mac.doFinal(data)
    }

    private val context: Context
        get() = ApplicationProvider.getApplicationContext()

    private fun makeConfig(
        userID: String,
        dbPath: String = "test_${UUID.randomUUID()}.sqlite",
        clientID: String = UUID.randomUUID().toString()
    ): SynchroConfig {
        val token = signTestJWT(userID)
        return SynchroConfig(
            dbPath = dbPath,
            serverURL = serverURL,
            authProvider = { token },
            clientID = clientID,
            appVersion = "1.0.0",
            syncInterval = 999.0,
            maxRetryAttempts = 1
        )
    }

    private fun makeBadTokenConfig(clientID: String = UUID.randomUUID().toString()): SynchroConfig {
        return SynchroConfig(
            dbPath = "bad_${UUID.randomUUID()}.sqlite",
            serverURL = serverURL,
            authProvider = { "bad.token" },
            clientID = clientID,
            appVersion = "1.0.0",
            syncInterval = 999.0,
            maxRetryAttempts = 1
        )
    }

    private fun makeConnectRequest(clientID: String): ConnectRequest {
        return ConnectRequest(
            clientID = clientID,
            platform = "android",
            appVersion = "1.0.0",
            protocolVersion = 3,
            schema = SchemaRef(version = 0, hash = ""),
            scopeSetVersion = 0,
            knownScopes = emptyMap()
        )
    }

    private fun seedOrder(
        client: SynchroClient,
        userID: String,
        customerID: String,
        orderID: String,
        shipAddress: String,
        updatedAt: String
    ) {
        client.execute(
            "INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, ?, ?)",
            arrayOf(customerID, userID, "Integration Customer", updatedAt, updatedAt)
        )
        client.execute(
            "INSERT INTO orders (id, customer_id, user_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, ?, 'pending', 0, 'USD', ?, ?, ?)",
            arrayOf(orderID, customerID, userID, shipAddress, updatedAt, updatedAt)
        )
    }


    @OptIn(ExperimentalSerializationApi::class)
    private val pushJSON = Json {
        ignoreUnknownKeys = false
        encodeDefaults = true
        explicitNulls = false
    }

    private fun database(client: SynchroClient): SynchroDatabase =
        SynchroClient::class.java.getDeclaredField("database").apply { isAccessible = true }.get(client) as SynchroDatabase

    private fun insertCustomer(userID: String, customerID: String, name: String): SQLStatement =
        SQLStatement(
            "INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, ?, ?)",
            arrayOf(customerID, userID, name, "2026-01-05T00:00:00.000Z", "2026-01-05T00:00:00.000Z"),
        )

    private fun sentRequests(database: SynchroDatabase): List<Pair<String, PushRequest>> =
        database.query("SELECT request_json FROM _synchro_push_batches WHERE state = 'completed'")
            .map { row ->
                val body = row.getValue("request_json") as String
                body to pushJSON.decodeFromString<PushRequest>(body)
            }

    private fun octets(text: String): Int = text.toByteArray(Charsets.UTF_8).size

    private fun ledgerState(database: SynchroDatabase, recordID: String): Any? =
        database.queryOne(
            "SELECT lifecycle_state FROM _synchro_pending_changes WHERE table_name = 'customers' AND record_id = ?",
            arrayOf(recordID),
        )?.get("lifecycle_state")

    /** Each shared source binary64 text and its RFC 8785 text. */
    private fun floatWireCases(): List<Pair<String, String>> {
        val path = generateSequence(Paths.get("").toAbsolutePath().normalize()) { it.parent }
            .take(8)
            .map { it.resolve("conformance/protocol/float-wire-boundaries-v1.json") }
            .first { Files.exists(it) }
        val document = Json.parseToJsonElement(String(Files.readAllBytes(path), Charsets.UTF_8)).jsonObject
        assertEquals(1, document.getValue("version").jsonPrimitive.int)
        return document.getValue("cases").jsonArray.map { element ->
            val case = element.jsonObject
            case.getValue("source").jsonPrimitive.content to case.getValue("canonical").jsonPrimitive.content
        }.also { assertTrue(it.isNotEmpty()) }
    }

    /** Syncs until every row has the wanted text, then compares each stored value with its canonical binary64. */
    private suspend fun waitForFloatWireRows(
        client: SynchroClient,
        userID: String,
        ids: List<String>,
        cases: List<Pair<String, String>>,
        text: String,
    ) {
        waitForCondition(timeoutMs = 30_000) {
            client.syncNow()
            val rows = client.query("SELECT col_text FROM type_zoo WHERE user_id = ?", arrayOf(userID))
            rows.size == ids.size && rows.all { it["col_text"] == text }
        }
        ids.zip(cases).forEach { (id, case) ->
            val value = client.queryOne("SELECT col_double FROM type_zoo WHERE id = ?", arrayOf(id))?.get("col_double") as Double
            assertEquals(
                "${case.first} must arrive as ${case.second}",
                case.second.toDouble().toRawBits(),
                value.toRawBits(),
            )
        }
    }

    private suspend fun waitForCondition(timeoutMs: Long = 5000, intervalMs: Long = 250, condition: suspend () -> Boolean) {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (true) {
            if (condition()) {
                return
            }
            if (System.currentTimeMillis() >= deadline) {
                fail("timed out waiting for sync condition")
            }
            delay(intervalMs)
        }
    }

    @OptIn(SynchroProofApi::class)
    @Test
    fun testRealFloatWireValuesSurviveRebuildPullAndLaterWork() = runBlocking {
        val cases = floatWireCases()
        val userID = UUID.randomUUID().toString()
        val ids = cases.map { UUID.randomUUID().toString() }
        val collector = TransportObservationCollector(capacity = 4096)
        // close() shuts each engine down. use attempts every close and keeps the first failure.
        val writer = SynchroClient(makeConfig(userID = userID), context)
        Closeable(writer::close).use {
            // A page limit below the row count requires rebuild continuation.
            val reader = SynchroClient(
                makeConfig(userID = userID).copy(pullPageSize = 4).withTransportObservationCollector(collector),
                context,
            )
            Closeable(reader::close).use {
                writer.start()
                writer.executeBatch(
                    ids.zip(cases).map { (id, case) ->
                        SQLStatement(
                            "INSERT INTO type_zoo (id, user_id, col_text, col_double, created_at, updated_at) VALUES (?, ?, 'float-wire', ?, ?, ?)",
                            arrayOf(id, userID, case.first.toDouble(), "2026-01-09T00:00:00.000Z", "2026-01-09T00:00:00.000Z"),
                        )
                    },
                )
                waitForCondition(timeoutMs = 30_000) {
                    writer.syncNow()
                    writer.pendingChangeCount() == 0
                }
                reader.start()
                waitForFloatWireRows(reader, userID, ids, cases, "float-wire")
                val scopeFingerprint = MessageDigest.getInstance("SHA-256")
                    .digest("user:$userID".toByteArray(Charsets.UTF_8))
                    .joinToString("") { byte -> "%02x".format(byte.toInt() and 0xff) }
                val snapshot = collector.snapshot()
                assertFalse(snapshot.overflowed)
                val pages = snapshot.observations.mapNotNull { it.rebuildResponseFacts }.filter { it.scopeFingerprint == scopeFingerprint }
                assertTrue(pages.size > 1)
                assertEquals(ids.size, pages.sumOf { it.recordCount })
                assertTrue(pages.dropLast(1).all { it.hasMore && it.hasCursor })
                assertFalse(pages.last().hasMore)

                writer.executeBatch(
                    ids.map { id ->
                        SQLStatement(
                            "UPDATE type_zoo SET col_text = 'float-wire-later', updated_at = ? WHERE id = ?",
                            arrayOf("2026-01-10T00:00:00.000Z", id),
                        )
                    },
                )
                waitForCondition(timeoutMs = 30_000) {
                    writer.syncNow()
                    writer.pendingChangeCount() == 0
                }
                waitForFloatWireRows(reader, userID, ids, cases, "float-wire-later")
            }
        }
    }

    @Test
    fun testAuthFailure() = runBlocking {
        val config = makeBadTokenConfig()
        val http = HttpClient(config)

        try {
            http.connect(makeConnectRequest(config.clientID))
            fail("Expected auth failure")
        } catch (e: SynchroError.ServerError) {
            assertEquals(401, e.status)
        }
    }

    @Test
    fun testPushPullBetweenTwoClients() = runBlocking {
        val userID = UUID.randomUUID().toString()
        val clientA = SynchroClient(makeConfig(userID = userID), context)
        val clientB = SynchroClient(makeConfig(userID = userID), context)
        val customerID = UUID.randomUUID().toString()
        val orderID = UUID.randomUUID().toString()

        try {
            clientA.start()
            seedOrder(clientA, userID, customerID, orderID, """{"street":"123 Main St"}""", "2026-01-01T00:00:00.000Z")
            clientA.syncNow()

            clientB.start()
            waitForCondition {
                clientB.syncNow()
                val row = clientB.queryOne("SELECT ship_address FROM orders WHERE id = ?", arrayOf(orderID))
                row?.get("ship_address") == """{"street":"123 Main St"}"""
            }
        } finally {
            clientA.stop()
            clientA.close()
            clientB.stop()
            clientB.close()
        }
    }

    @Test
    fun testFreshClientBootstrapsExistingServerState() = runBlocking {
        val userID = UUID.randomUUID().toString()
        val writer = SynchroClient(makeConfig(userID = userID), context)
        val reader = SynchroClient(makeConfig(userID = userID), context)
        val customerID = UUID.randomUUID().toString()
        val orderID = UUID.randomUUID().toString()

        try {
            writer.start()
            seedOrder(writer, userID, customerID, orderID, """{"street":"Bootstrap Ave"}""", "2026-01-02T00:00:00.000Z")
            writer.syncNow()
            writer.stop()
            writer.close()

            reader.start()
            waitForCondition {
                reader.syncNow()
                val row = reader.queryOne("SELECT ship_address FROM orders WHERE id = ?", arrayOf(orderID))
                row?.get("ship_address") == """{"street":"Bootstrap Ave"}"""
            }
        } finally {
            reader.stop()
            reader.close()
        }
    }

    @Test
    fun testSoftDeletePropagatesBetweenClients() = runBlocking {
        val userID = UUID.randomUUID().toString()
        val clientA = SynchroClient(makeConfig(userID = userID), context)
        val clientB = SynchroClient(makeConfig(userID = userID), context)
        val customerID = UUID.randomUUID().toString()
        val orderID = UUID.randomUUID().toString()

        try {
            clientA.start()
            seedOrder(clientA, userID, customerID, orderID, """{"street":"Delete Me"}""", "2026-01-03T00:00:00.000Z")
            clientA.syncNow()

            clientB.start()
            waitForCondition {
                val row = clientB.queryOne("SELECT ship_address FROM orders WHERE id = ?", arrayOf(orderID))
                row?.get("ship_address") == """{"street":"Delete Me"}"""
            }

            clientA.execute(
                "UPDATE orders SET deleted_at = ?, updated_at = ? WHERE id = ?",
                arrayOf("2026-01-04T00:00:00.000Z", "2026-01-04T00:00:00.000Z", orderID)
            )
            clientA.syncNow()
            val expectedDeletedAt = clientA.queryOne(
                "SELECT deleted_at FROM orders WHERE id = ?",
                arrayOf(orderID)
            )?.get("deleted_at") as? String
            assertNotNull(expectedDeletedAt)
            waitForCondition {
                clientB.syncNow()
                val row = clientB.queryOne("SELECT deleted_at FROM orders WHERE id = ?", arrayOf(orderID))
                row?.get("deleted_at") == expectedDeletedAt
            }
        } finally {
            clientA.stop()
            clientA.close()
            clientB.stop()
            clientB.close()
        }
    }

    @Test
    fun testPushQueueOverTheRequestLimitDrainsInSeveralBatches() = runBlocking {
        val userID = UUID.randomUUID().toString()
        val clientA = SynchroClient(makeConfig(userID = userID), context)
        val clientB = SynchroClient(makeConfig(userID = userID), context)
        val names = (0 until 20).associate { index -> UUID.randomUUID().toString() to "${'a' + index}".repeat(60_000) }

        try {
            clientA.start()
            clientA.executeBatch(names.map { (id, name) -> insertCustomer(userID, id, name) })
            val database = database(clientA)
            waitForCondition(timeoutMs = 30_000) {
                clientA.syncNow()
                names.keys.all { ledgerState(database, it) == "accepted" }
            }

            val requests = sentRequests(database)
            assertTrue(requests.size >= 2)
            assertTrue(
                requests.sumOf { (body, _) -> octets(Integrity.canonicalJSON(Json.parseToJsonElement(body))) } >
                    PushLimits.MAX_REQUEST_OCTETS,
            )
            requests.forEach { (body, _) ->
                assertTrue(octets(body) <= PushLimits.MAX_REQUEST_OCTETS)
                assertTrue(octets(Integrity.canonicalJSON(Json.parseToJsonElement(body))) <= PushLimits.MAX_REQUEST_OCTETS)
            }
            val sentIDs = requests.flatMap { (_, request) -> request.mutations.map { it.mutationID } }
            val ledgerIDs = database.query(
                "SELECT mutation_id FROM _synchro_pending_changes WHERE table_name = 'customers'",
            ).map { it.getValue("mutation_id") }
            assertEquals(names.size, sentIDs.size)
            assertEquals(ledgerIDs.toSet(), sentIDs.toSet())
            assertEquals(
                names,
                clientA.query("SELECT id, name FROM customers").associate { it["id"] as String to it["name"] as String },
            )

            clientB.start()
            waitForCondition(timeoutMs = 30_000) {
                clientB.syncNow()
                clientB.query("SELECT id, name FROM customers")
                    .associate { it["id"] as String to it["name"] as String } == names
            }
        } finally {
            clientA.stop()
            clientA.close()
            clientB.stop()
            clientB.close()
        }
    }

    @Test
    fun testNormalizedMutationLimitAtTheServer() = runBlocking {
        val userID = UUID.randomUUID().toString()
        val clientA = SynchroClient(makeConfig(userID = userID), context)
        val clientB = SynchroClient(makeConfig(userID = userID), context)
        val probeID = UUID.randomUUID().toString()
        val exactID = UUID.randomUUID().toString()
        val oversizeID = UUID.randomUUID().toString()
        val laterID = UUID.randomUUID().toString()

        try {
            clientA.start()
            val database = database(clientA)
            clientA.executeBatch(listOf(insertCustomer(userID, probeID, "")))
            waitForCondition(timeoutMs = 30_000) {
                clientA.syncNow()
                ledgerState(database, probeID) == "accepted"
            }
            val probe = sentRequests(database).single().second.mutations.single()
            val atLimit = PushLimits.MAX_NORMALIZED_MUTATION_OCTETS - PushLimits.mutation(pushJSON, probe).normalized
            val exactName = "e".repeat(atLimit)

            clientA.executeBatch(
                listOf(
                    insertCustomer(userID, exactID, exactName),
                    insertCustomer(userID, oversizeID, "o".repeat(atLimit + 1)),
                    insertCustomer(userID, laterID, "later"),
                ),
            )
            waitForCondition(timeoutMs = 30_000) {
                clientA.syncNow()
                ledgerState(database, exactID) == "accepted" && ledgerState(database, laterID) == "accepted"
            }

            val sent = sentRequests(database).flatMap { (_, request) -> request.mutations }
            val exact = sent.single { it.pk.values.single().jsonPrimitive.content == exactID }
            assertEquals(PushLimits.MAX_NORMALIZED_MUTATION_OCTETS, PushLimits.mutation(pushJSON, exact).normalized)
            assertTrue(sent.none { it.pk.values.single().jsonPrimitive.content == oversizeID })
            assertEquals("exceeds_push_limit", ledgerState(database, oversizeID))
            assertEquals(
                LocalMutationStatus.EXCEEDS_PUSH_LIMIT,
                clientA.inspectRetainedMutations().currentRecords().single { it.recordID == oversizeID }.status,
            )

            clientB.start()
            waitForCondition(timeoutMs = 30_000) {
                clientB.syncNow()
                val later = clientB.queryOne("SELECT name FROM customers WHERE id = ?", arrayOf(laterID))
                val exactRow = clientB.queryOne("SELECT name FROM customers WHERE id = ?", arrayOf(exactID))
                later?.get("name") == "later" && exactRow?.get("name") == exactName
            }
            assertNull(clientB.queryOne("SELECT id FROM customers WHERE id = ?", arrayOf(oversizeID)))
        } finally {
            clientA.stop()
            clientA.close()
            clientB.stop()
            clientB.close()
        }
    }
}
