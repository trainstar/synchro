package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonPrimitive
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
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
                clientA.inspectRetainedMutations().single { it.recordID == oversizeID }.status,
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

    @Test
    fun testAtomicGroupIsSentInOneRequestAndApplied() = runBlocking {
        val userID = UUID.randomUUID().toString()
        val clientA = SynchroClient(makeConfig(userID = userID), context)
        val clientB = SynchroClient(makeConfig(userID = userID), context)
        val firstID = UUID.randomUUID().toString()
        val secondID = UUID.randomUUID().toString()
        val orderID = UUID.randomUUID().toString()
        val updatedAt = "2026-01-06T00:00:00.000Z"

        try {
            clientA.start()
            clientA.atomicWriteTransaction { transaction ->
                listOf(insertCustomer(userID, firstID, "first"), insertCustomer(userID, secondID, "second"))
                    .forEach { transaction.execute(it.sql, it.params) }
                transaction.execute(
                    "INSERT INTO orders (id, customer_id, user_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, ?, 'pending', 0, 'USD', ?, ?, ?)",
                    arrayOf(orderID, firstID, userID, """{"street":"Atomic Way"}""", updatedAt, updatedAt),
                )
            }
            val database = database(clientA)
            waitForCondition(timeoutMs = 30_000) {
                clientA.syncNow()
                clientA.pendingChangeCount() == 0
            }

            val request = sentRequests(database).single().second
            assertEquals(true, request.atomic)
            assertEquals(
                setOf(firstID, secondID, orderID),
                request.mutations.map { it.pk.values.single().jsonPrimitive.content }.toSet(),
            )
            assertEquals(
                listOf("accepted", "accepted", "accepted"),
                database.query("SELECT lifecycle_state FROM _synchro_pending_changes").map { it.getValue("lifecycle_state") },
            )

            clientB.start()
            waitForCondition(timeoutMs = 30_000) {
                clientB.syncNow()
                clientB.query("SELECT id FROM customers").map { it["id"] }.toSet() == setOf(firstID, secondID) &&
                    clientB.queryOne("SELECT customer_id FROM orders WHERE id = ?", arrayOf(orderID))?.get("customer_id") == firstID
            }
        } finally {
            clientA.stop()
            clientA.close()
            clientB.stop()
            clientB.close()
        }
    }

    @Test
    fun testConflictInAnAtomicGroupRejectsTheWholeGroupWithoutRevert() = runBlocking {
        val userID = UUID.randomUUID().toString()
        val clientA = SynchroClient(makeConfig(userID = userID), context)
        val clientB = SynchroClient(makeConfig(userID = userID), context)
        val conflictingID = UUID.randomUUID().toString()
        val groupedID = UUID.randomUUID().toString()

        try {
            clientA.start()
            val database = database(clientA)
            clientA.executeBatch(listOf(insertCustomer(userID, conflictingID, "original")))
            waitForCondition(timeoutMs = 30_000) {
                clientA.syncNow()
                ledgerState(database, conflictingID) == "accepted"
            }

            clientB.start()
            waitForCondition(timeoutMs = 30_000) {
                clientB.syncNow()
                clientB.queryOne("SELECT name FROM customers WHERE id = ?", arrayOf(conflictingID))?.get("name") == "original"
            }
            clientB.execute("UPDATE customers SET name = ? WHERE id = ?", arrayOf("server", conflictingID))
            waitForCondition(timeoutMs = 30_000) {
                clientB.syncNow()
                clientB.pendingChangeCount() == 0
            }

            clientA.atomicWriteTransaction { transaction ->
                transaction.execute("UPDATE customers SET name = ? WHERE id = ?", arrayOf("local", conflictingID))
                insertCustomer(userID, groupedID, "grouped").let { transaction.execute(it.sql, it.params) }
            }
            waitForCondition(timeoutMs = 30_000) {
                clientA.syncNow()
                clientA.pendingChangeCount() == 0
            }

            val conflictingUpdate = database.queryOne(
                "SELECT mutation_id, lifecycle_state FROM _synchro_pending_changes WHERE record_id = ? AND operation = 'update'",
                arrayOf(conflictingID),
            )!!
            assertEquals("conflict", conflictingUpdate["lifecycle_state"])
            assertEquals("rejected_terminal", ledgerState(database, groupedID))
            val rejections = clientA.inspectRejectedMutations().associateBy { it.recordID }
            assertEquals(setOf(conflictingID, groupedID), rejections.keys)
            assertEquals(conflictingUpdate["mutation_id"], rejections.getValue(conflictingID).mutationID)
            assertEquals(MutationStatus.CONFLICT, rejections.getValue(conflictingID).status)
            assertEquals(MutationRejectionCode.VERSION_CONFLICT, rejections.getValue(conflictingID).code)
            assertEquals(MutationStatus.REJECTED_TERMINAL, rejections.getValue(groupedID).status)
            assertEquals(MutationRejectionCode.ATOMIC_BATCH_REJECTED, rejections.getValue(groupedID).code)
            assertEquals("server", clientA.queryOne("SELECT name FROM customers WHERE id = ?", arrayOf(conflictingID))?.get("name"))
            assertEquals("grouped", clientA.queryOne("SELECT name FROM customers WHERE id = ?", arrayOf(groupedID))?.get("name"))

            clientB.syncNow()
            assertEquals("server", clientB.queryOne("SELECT name FROM customers WHERE id = ?", arrayOf(conflictingID))?.get("name"))
            assertNull(clientB.queryOne("SELECT id FROM customers WHERE id = ?", arrayOf(groupedID)))
        } finally {
            clientA.stop()
            clientA.close()
            clientB.stop()
            clientB.close()
        }
    }

    /**
     * The fixture trigger gives the customer insert an order with the same ID. Thus the grouped
     * order insert conflicts, and after the group rollback no row exists for the order.
     */
    @Test
    fun testAtomicGroupConflictWithoutServerRowRemovesTheLocalRow() = runBlocking {
        val userID = UUID.randomUUID().toString()
        val client = SynchroClient(makeConfig(userID = userID), context)
        val rowID = UUID.randomUUID().toString()
        val updatedAt = "2026-01-11T00:00:00.000Z"

        try {
            client.start()
            client.atomicWriteTransaction { transaction ->
                transaction.execute(
                    "INSERT INTO customers (id, user_id, name, balance, is_active, market_segment, created_at, updated_at) VALUES (?, ?, 'shadow parent', 0, 1, 'test-shadow-order', ?, ?)",
                    arrayOf(rowID, userID, updatedAt, updatedAt),
                )
                transaction.execute(
                    "INSERT INTO orders (id, customer_id, user_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, ?, 'pending', 0, 'USD', ?, ?, ?)",
                    arrayOf(rowID, rowID, userID, """{"street":"Shadow Way"}""", updatedAt, updatedAt),
                )
            }
            waitForCondition(timeoutMs = 30_000) {
                client.syncNow()
                client.pendingChangeCount() == 0
            }

            val rejections = client.inspectRejectedMutations().associateBy { it.tableName }
            assertEquals(setOf("customers", "orders"), rejections.keys)
            assertEquals(MutationRejectionCode.ATOMIC_BATCH_REJECTED, rejections.getValue("customers").code)
            val order = rejections.getValue("orders")
            assertEquals(MutationStatus.CONFLICT, order.status)
            assertEquals(MutationRejectionCode.ROW_ALREADY_EXISTS, order.code)
            assertNull(order.serverRowJSON)
            assertNull(order.serverVersion)
            assertNull(client.queryOne("SELECT id FROM orders WHERE id = ?", arrayOf(rowID)))
            assertEquals("shadow parent", client.queryOne("SELECT name FROM customers WHERE id = ?", arrayOf(rowID))?.get("name"))
        } finally {
            client.stop()
            client.close()
        }
    }
}
