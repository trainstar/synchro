package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import kotlinx.coroutines.test.runTest
import org.junit.Assume.assumeNotNull
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
import java.util.UUID
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/// Integration tests for schema reconciliation and seed database loading.
/// Requires SYNCHRO_TEST_URL and SYNCHRO_TEST_JWT_SECRET environment variables.
/// Skips when env vars are not set.
@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28])
class SchemaIntegrationTests {

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

    private val context: Context get() = ApplicationProvider.getApplicationContext()

    private fun makeConfig(
        userID: String,
        dbPath: String = "test_${UUID.randomUUID()}.sqlite",
        seedPath: String? = null,
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
            maxRetryAttempts = 1,
            seedDatabasePath = seedPath
        )
    }

    private fun fetchServerSchema(): SchemaResponse {
        val config = makeConfig(userID = UUID.randomUUID().toString())
        val okHttp = okhttp3.OkHttpClient()
        val http = HttpClient(config, okHttp)
        return kotlinx.coroutines.runBlocking { http.fetchSchema() }
    }

    /** Create a seed database with the given tables. Returns the absolute path. */
    private fun createSeedDB(
        tables: List<SchemaTable>,
        schemaVersion: Long = 1,
        schemaHash: String = "seed"
    ): String {
        val dbName = "seed_${UUID.randomUUID()}.sqlite"
        val db = SynchroDatabase(context, dbName)
        val schemaManager = SchemaManager(db)
        val schema = SchemaResponse(schemaVersion, schemaHash, "", tables)
        schemaManager.createSyncedTables(schema)
        db.writeTransaction { rawDb ->
            SynchroMeta.setInt64(rawDb, MetaKey.SCHEMA_VERSION, schemaVersion)
            SynchroMeta.set(rawDb, MetaKey.SCHEMA_HASH, schemaHash)
        }
        db.close()
        return context.getDatabasePath(dbName).absolutePath
    }

    // -- 1. testAdditiveSchemaChangePreservesData --

    @Test
    fun testAdditiveSchemaChangePreservesData() = runTest {
        skipIfNoServer()

        val serverSchema = fetchServerSchema()
        val userID = UUID.randomUUID().toString()
        val clientID = UUID.randomUUID().toString()
        val dbPath = "schema_integ_1_${UUID.randomUUID()}.sqlite"

        val ordersTable = serverSchema.tables.firstOrNull { it.tableName == "orders" }
            ?: return@runTest fail("server schema must include 'orders' table")

        // 1. Full initial sync — creates all local tables from server schema
        val client1 = SynchroClient(makeConfig(userID = userID, dbPath = dbPath, clientID = clientID), context)
        client1.start()

        // 2. Insert data and push it to server
        val orderID = UUID.randomUUID().toString()
        client1.execute(
            "INSERT INTO orders (id, user_id, ship_address, updated_at) VALUES ('$orderID', '$userID', '123 Main St', '2026-01-01T00:00:00.000Z')"
        )
        client1.syncNow()

        client1.stop()
        client1.close()

        // 3. Force schema re-fetch by resetting version/hash
        val rawDb = SynchroDatabase(context, dbPath)
        rawDb.writeTransaction { db ->
            SynchroMeta.setInt64(db, MetaKey.SCHEMA_VERSION, 0)
            SynchroMeta.set(db, MetaKey.SCHEMA_HASH, "")
        }
        rawDb.close()

        // 4. Reconnect with SAME clientID — incremental pull, data preserved
        val client2 = SynchroClient(makeConfig(userID = userID, dbPath = dbPath, clientID = clientID), context)
        client2.start()

        // 5. Pushed data should still be there
        val row = client2.queryOne("SELECT id, ship_address FROM orders WHERE id = ?", arrayOf(orderID))
        assertNotNull("pushed data should survive schema reconciliation on reconnect", row)
        assertEquals("123 Main St", row?.get("ship_address"))

        // 6. All server columns still exist
        val columns = mutableListOf<String>()
        client2.readTransaction { rawDb2 ->
            rawDb2.rawQuery("PRAGMA table_info(orders)", null).use { cursor ->
                val nameIdx = cursor.getColumnIndex("name")
                while (cursor.moveToNext()) {
                    columns.add(cursor.getString(nameIdx))
                }
            }
        }
        for (serverCol in ordersTable.columns) {
            assertTrue("column '${serverCol.name}' should exist after reconciliation", serverCol.name in columns)
        }

        client2.stop()
        client2.close()
    }

    // -- 2. testLocalOnlyTablesSurviveReconnect --

    @Test
    fun testLocalOnlyTablesSurviveReconnect() = runTest {
        skipIfNoServer()

        val userID = UUID.randomUUID().toString()
        val dbPath = "schema_integ_2_${UUID.randomUUID()}.sqlite"

        // Connect and sync (creates synced tables)
        val client1 = SynchroClient(makeConfig(userID = userID, dbPath = dbPath), context)
        client1.start()

        // Create a local-only table with data
        client1.createTable("app_settings", listOf(
            ColumnDef(name = "key", type = "TEXT", nullable = false, primaryKey = true),
            ColumnDef(name = "value", type = "TEXT", nullable = false),
        ))
        client1.execute("INSERT INTO app_settings (key, value) VALUES ('theme', 'dark')")
        client1.execute("INSERT INTO app_settings (key, value) VALUES ('locale', 'en')")

        client1.stop()
        client1.close()

        // Force schema re-check by resetting hash
        val rawDb = SynchroDatabase(context, dbPath)
        rawDb.writeTransaction { db ->
            SynchroMeta.set(db, MetaKey.SCHEMA_HASH, "")
        }
        rawDb.close()

        // Reconnect — schema re-fetched and reconciled
        val client2 = SynchroClient(makeConfig(userID = userID, dbPath = dbPath), context)
        client2.start()

        // Verify local-only table and data survived
        val settings = client2.query("SELECT key, value FROM app_settings ORDER BY key")
        assertEquals(2, settings.size)
        assertEquals("locale", settings[0]["key"])
        assertEquals("en", settings[0]["value"])
        assertEquals("theme", settings[1]["key"])
        assertEquals("dark", settings[1]["value"])

        client2.stop()
        client2.close()
    }

    // -- 3. testSeedDatabaseWorksOffline --

    @Test
    fun testSeedDatabaseWorksOffline() = runTest {
        skipIfNoServer()

        // Fetch server schema to build a correct seed
        val serverSchema = fetchServerSchema()
        val seedPath = createSeedDB(
            tables = serverSchema.tables,
            schemaVersion = serverSchema.schemaVersion,
            schemaHash = serverSchema.schemaHash
        )

        // Create SynchroClient with seed — do NOT call start()
        val dbPath = "schema_integ_3_${UUID.randomUUID()}.sqlite"
        val client = SynchroClient(makeConfig(userID = UUID.randomUUID().toString(), dbPath = dbPath, seedPath = seedPath), context)

        // Tables should be queryable immediately (offline)
        val orders = client.query("SELECT * FROM orders")
        assertEquals(0, orders.size)

        // Insert data offline — CDC triggers should fire
        val orderID = UUID.randomUUID().toString()
        client.execute(
            "INSERT INTO orders (id, user_id, ship_address, updated_at) VALUES ('$orderID', '${UUID.randomUUID()}', '456 Oak Ave', '2026-01-01T00:00:00.000Z')"
        )

        // Query back
        val row = client.queryOne("SELECT ship_address FROM orders WHERE id = ?", arrayOf(orderID))
        assertNotNull(row)
        assertEquals("456 Oak Ave", row?.get("ship_address"))

        // Verify CDC trigger fired
        val pending = client.query("SELECT record_id, operation FROM _synchro_pending_changes WHERE table_name = 'orders'")
        assertEquals(1, pending.size)
        assertEquals(orderID, pending[0]["record_id"])
        assertEquals("create", pending[0]["operation"])

        client.close()
    }

    // -- 4. testSeedDatabaseReconcilesOnConnect --

    @Test
    fun testSeedDatabaseReconcilesOnConnect() = runTest {
        skipIfNoServer()

        val serverSchema = fetchServerSchema()
        val ordersTable = serverSchema.tables.firstOrNull { it.tableName == "orders" }
            ?: return@runTest fail("server schema must include 'orders' table")

        // Create STALE seed: only orders table with minimal columns (no other tables)
        val minimalColumns = ordersTable.columns.filter {
            it.name in listOf("id", "user_id", "ship_address", "updated_at", "deleted_at")
        }
        val staleOrders = ordersTable.copy(columns = minimalColumns)
        val seedPath = createSeedDB(tables = listOf(staleOrders), schemaVersion = 0, schemaHash = "stale-seed")

        // Create SynchroClient with stale seed, then connect
        val dbPath = "schema_integ_4_${UUID.randomUUID()}.sqlite"
        val client = SynchroClient(makeConfig(userID = UUID.randomUUID().toString(), dbPath = dbPath, seedPath = seedPath), context)
        client.start()

        // Verify missing columns were added by reconciliation
        val columns = mutableListOf<String>()
        client.readTransaction { rawDb ->
            rawDb.rawQuery("PRAGMA table_info(orders)", null).use { cursor ->
                val nameIdx = cursor.getColumnIndex("name")
                while (cursor.moveToNext()) {
                    columns.add(cursor.getString(nameIdx))
                }
            }
        }
        for (serverCol in ordersTable.columns) {
            assertTrue("column '${serverCol.name}' should be added by reconciliation", serverCol.name in columns)
        }

        // Verify tables from server that weren't in the seed were also created
        for (serverTable in serverSchema.tables) {
            val tableRows = client.query("SELECT name FROM sqlite_master WHERE type='table' AND name='${serverTable.tableName}'")
            assertEquals("table '${serverTable.tableName}' should exist after reconciliation", 1, tableRows.size)
        }

        // Verify sync works — can insert after reconciliation
        val orderID = UUID.randomUUID().toString()
        client.execute(
            "INSERT INTO orders (id, user_id, ship_address, updated_at) VALUES ('$orderID', '${UUID.randomUUID()}', 'post-reconcile', '2026-01-01T00:00:00.000Z')"
        )
        val row = client.queryOne("SELECT ship_address FROM orders WHERE id = ?", arrayOf(orderID))
        assertNotNull(row)
        assertEquals("post-reconcile", row?.get("ship_address"))

        client.stop()
        client.close()
    }
}
