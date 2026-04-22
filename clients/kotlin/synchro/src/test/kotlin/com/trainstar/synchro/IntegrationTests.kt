package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.fail
import org.junit.Assume.assumeNotNull
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
            serverURL = serverURL!!,
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
            serverURL = serverURL!!,
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
            protocolVersion = 2,
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
            "INSERT INTO orders (id, customer_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, 'pending', 0, 'USD', ?, ?, ?)",
            arrayOf(orderID, customerID, shipAddress, updatedAt, updatedAt)
        )
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

    @Test
    fun testAuthFailure() = runTest {
        skipIfNoServer()

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
    fun testPushPullBetweenTwoClients() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString()
        val clientA = SynchroClient(makeConfig(userID = userID), context)
        val clientB = SynchroClient(makeConfig(userID = userID), context)
        val customerID = UUID.randomUUID().toString()
        val orderID = UUID.randomUUID().toString()

        try {
            clientA.start()
            seedOrder(clientA, userID, customerID, orderID, "123 Main St", "2026-01-01T00:00:00.000Z")
            clientA.syncNow()

            clientB.start()
            waitForCondition {
                clientB.syncNow()
                val row = clientB.queryOne("SELECT ship_address FROM orders WHERE id = ?", arrayOf(orderID))
                row?.get("ship_address") == "123 Main St"
            }
        } finally {
            clientA.stop()
            clientA.close()
            clientB.stop()
            clientB.close()
        }
    }

    @Test
    fun testFreshClientBootstrapsExistingServerState() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString()
        val writer = SynchroClient(makeConfig(userID = userID), context)
        val reader = SynchroClient(makeConfig(userID = userID), context)
        val customerID = UUID.randomUUID().toString()
        val orderID = UUID.randomUUID().toString()

        try {
            writer.start()
            seedOrder(writer, userID, customerID, orderID, "Bootstrap Ave", "2026-01-02T00:00:00.000Z")
            writer.syncNow()
            writer.stop()
            writer.close()

            reader.start()
            waitForCondition {
                reader.syncNow()
                val row = reader.queryOne("SELECT ship_address FROM orders WHERE id = ?", arrayOf(orderID))
                row?.get("ship_address") == "Bootstrap Ave"
            }
        } finally {
            reader.stop()
            reader.close()
        }
    }

    @Test
    fun testSoftDeletePropagatesBetweenClients() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString()
        val clientA = SynchroClient(makeConfig(userID = userID), context)
        val clientB = SynchroClient(makeConfig(userID = userID), context)
        val customerID = UUID.randomUUID().toString()
        val orderID = UUID.randomUUID().toString()

        try {
            clientA.start()
            seedOrder(clientA, userID, customerID, orderID, "Delete Me", "2026-01-03T00:00:00.000Z")
            clientA.syncNow()

            clientB.start()
            clientB.syncNow()

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
}
