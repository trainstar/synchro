package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.delay
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
@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28])
class SchemaIntegrationTests {

    private lateinit var serverURL: String
    private lateinit var jwtSecret: String
    private lateinit var canonicalSeedPath: String

    @Before
    fun setUp() {
        serverURL = checkNotNull(System.getenv("SYNCHRO_TEST_URL")) {
            "SYNCHRO_TEST_URL must be set for schema integration tests"
        }
        jwtSecret = checkNotNull(System.getenv("SYNCHRO_TEST_JWT_SECRET")) {
            "SYNCHRO_TEST_JWT_SECRET must be set for schema integration tests"
        }
        canonicalSeedPath = checkNotNull(System.getenv("SYNCHRO_TEST_SEED_PATH")) {
            "SYNCHRO_TEST_SEED_PATH must be set for schema integration tests"
        }
        check(java.io.File(canonicalSeedPath).exists()) {
            "SYNCHRO_TEST_SEED_PATH must point to an existing bundled seed database"
        }
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

        val signature = hmacSHA256(jwtSecret.toByteArray(), signingInput.toByteArray())
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
            serverURL = serverURL,
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
        val serverSchema = fetchServerSchema()
        val userID = UUID.randomUUID().toString().lowercase()
        val clientID = UUID.randomUUID().toString()
        val dbPath = "schema_integ_1_${UUID.randomUUID()}.sqlite"

        val ordersTable = serverSchema.tables.firstOrNull { it.tableName == "orders" }
            ?: return@runTest fail("server schema must include 'orders' table")

        // 1. Full initial sync — creates all local tables from server schema
        val client1 = SynchroClient(makeConfig(userID = userID, dbPath = dbPath, clientID = clientID), context)
        client1.start()

        // 2. Insert customer (required FK for orders) and order, push to server
        val custID = UUID.randomUUID().toString()
        client1.execute(
            "INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, ?, ?)",
            arrayOf(custID, userID, "Schema Test Customer", "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00.000Z")
        )
        val orderID = UUID.randomUUID().toString()
        client1.execute(
            "INSERT INTO orders (id, customer_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, 'pending', 0, 'USD', ?, ?, ?)",
            arrayOf(orderID, custID, "123 Main St", "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00.000Z")
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

        // Insert customer (FK required) and order offline — CDC triggers should fire
        val custID = UUID.randomUUID().toString()
        client.execute(
            "INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, ?, ?)",
            arrayOf(custID, UUID.randomUUID().toString(), "Offline Customer", "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00.000Z")
        )
        val orderID = UUID.randomUUID().toString()
        client.execute(
            "INSERT INTO orders (id, customer_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, 'pending', 0, 'USD', ?, ?, ?)",
            arrayOf(orderID, custID, "456 Oak Ave", "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00.000Z")
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

    @Test
    fun testOfflineWritesBeforeFirstConnectArePushedOnFirstSync() = runTest {
        val userID = UUID.randomUUID().toString().lowercase()
        val clientID = UUID.randomUUID().toString()
        val dbPath = "schema_integ_offline_${UUID.randomUUID()}.sqlite"
        val customerID = UUID.randomUUID().toString()

        val offlineClient = SynchroClient(
            makeConfig(
                userID = userID,
                dbPath = dbPath,
                seedPath = canonicalSeedPath,
                clientID = clientID
            ),
            context
        )

        offlineClient.execute(
            "INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, ?, ?)",
            arrayOf(customerID, userID, "Offline First Customer", "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00.000Z")
        )

        val pendingBeforeConnect = offlineClient.query(
            "SELECT table_name, record_id FROM _synchro_pending_changes ORDER BY table_name, record_id"
        )
        val offlineRow = offlineClient.queryOne(
            "SELECT name FROM customers WHERE id = ?",
            arrayOf(customerID)
        )
        offlineClient.close()

        val onlineClient = SynchroClient(
            makeConfig(userID = userID, dbPath = dbPath, clientID = clientID),
            context
        )
        onlineClient.start()
        onlineClient.syncNow()

        val pendingAfterConnect = onlineClient.query("SELECT record_id FROM _synchro_pending_changes")
        val localRow = onlineClient.queryOne(
            "SELECT name FROM customers WHERE id = ?",
            arrayOf(customerID)
        )
        val rejectedAfterConnect = onlineClient.query(
            "SELECT mutation_id FROM _synchro_rejected_mutations"
        )
        onlineClient.stop()
        onlineClient.close()

        assertEquals(1, pendingBeforeConnect.size)
        assertEquals("Offline First Customer", offlineRow?.get("name"))
        assertEquals(0, pendingAfterConnect.size)
        assertEquals("Offline First Customer", localRow?.get("name"))
        assertTrue(rejectedAfterConnect.isEmpty())
    }

    // -- 4. testSeedDatabaseReconcilesOnConnect --

    @Test
    fun testSeedDatabaseReconcilesOnConnect() = runTest {
        val serverSchema = fetchServerSchema()
        val ordersTable = serverSchema.tables.firstOrNull { it.tableName == "orders" }
            ?: return@runTest fail("server schema must include 'orders' table")

        // Create STALE seed: only orders table with minimal columns (no other tables)
        val minimalColumns = ordersTable.columns.filter {
            it.name in listOf("id", "customer_id", "ship_address", "updated_at", "deleted_at")
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
        val custID = UUID.randomUUID().toString()
        client.execute(
            "INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, ?, ?)",
            arrayOf(custID, UUID.randomUUID().toString(), "Reconcile Customer", "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00.000Z")
        )
        val orderID = UUID.randomUUID().toString()
        client.execute(
            "INSERT INTO orders (id, customer_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, 'pending', 0, 'USD', ?, ?, ?)",
            arrayOf(orderID, custID, "post-reconcile insert", "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00.000Z")
        )
        val row = client.queryOne("SELECT ship_address FROM orders WHERE id = ?", arrayOf(orderID))
        assertNotNull(row)
        assertEquals("post-reconcile insert", row?.get("ship_address"))

        client.stop()
        client.close()
    }

    @Test
    fun testBundledSeedRepairsPortableScopeCorruptionOnConnect() = runTest {
        val dbPath = "schema_integ_5_${UUID.randomUUID()}.sqlite"
        val bootstrap = SynchroClient(
            makeConfig(
                userID = UUID.randomUUID().toString(),
                dbPath = dbPath,
                seedPath = canonicalSeedPath
            ),
            context
        )

        val seededCategoryID = "10000000-0000-0000-0000-000000000006"
        val seededCategoryName = "Seed Category"

        val seededScope = bootstrap.readTransaction { db ->
            SynchroMeta.getScope(db, "global")
        }
        assertEquals("global", seededScope?.scopeID)
        assertTrue((seededScope?.cursor ?: "").isNotEmpty())
        assertTrue((seededScope?.checksum ?: "").isNotEmpty())

        val seededRow = bootstrap.queryOne(
            "SELECT name FROM categories WHERE id = ?",
            arrayOf(seededCategoryID)
        )
        assertEquals(seededCategoryName, seededRow?.get("name"))

        bootstrap.close()

        val rawDb = SynchroDatabase(context, dbPath)
        rawDb.writeSyncLockedTransaction { db ->
            SynchroMeta.deleteScopeRow(db, "global", "categories", seededCategoryID)
            db.execSQL("DELETE FROM categories WHERE id = ?", arrayOf(seededCategoryID))
        }
        rawDb.close()

        val client = SynchroClient(
            makeConfig(userID = UUID.randomUUID().toString(), dbPath = dbPath),
            context
        )
        client.start()

        val repairedRow = client.queryOne(
            "SELECT name FROM categories WHERE id = ?",
            arrayOf(seededCategoryID)
        )
        assertEquals(seededCategoryName, repairedRow?.get("name"))

        val repairedScope = client.readTransaction { db ->
            SynchroMeta.getScope(db, "global")
        }
        assertEquals("global", repairedScope?.scopeID)
        assertTrue((repairedScope?.cursor ?: "").isNotEmpty())
        assertTrue((repairedScope?.checksum ?: "").isNotEmpty())

        val pending = client.queryOne("SELECT COUNT(*) AS count FROM _synchro_pending_changes")
        assertEquals(0L, (pending?.get("count") as Number).toLong())

        client.stop()
        client.close()
    }

    @Test
    fun testBundledSeedContinuesIncrementallyWithoutRebuild() = runTest {
        val dbPath = "schema_integ_6_${UUID.randomUUID()}.sqlite"
        val client = SynchroClient(
            makeConfig(
                userID = UUID.randomUUID().toString(),
                dbPath = dbPath,
                seedPath = canonicalSeedPath
            ),
            context
        )

        val seededCategoryID = "10000000-0000-0000-0000-000000000006"
        val initialScope = client.readTransaction { db ->
            SynchroMeta.getScope(db, "global")
        }
        assertEquals("global", initialScope?.scopeID)
        assertTrue((initialScope?.cursor ?: "").isNotEmpty())
        assertTrue((initialScope?.checksum ?: "").isNotEmpty())

        val initialGeneration = initialScope?.generation
        val initialCategory = client.queryOne(
            "SELECT name FROM categories WHERE id = ?",
            arrayOf(seededCategoryID)
        )
        assertEquals("Seed Category", initialCategory?.get("name"))

        client.start()

        val resumedScope = client.readTransaction { db ->
            SynchroMeta.getScope(db, "global")
        }
        assertEquals("global", resumedScope?.scopeID)
        assertEquals(initialGeneration, resumedScope?.generation)
        assertTrue((resumedScope?.cursor ?: "").isNotEmpty())
        assertTrue((resumedScope?.checksum ?: "").isNotEmpty())

        val resumedCategory = client.queryOne(
            "SELECT name FROM categories WHERE id = ?",
            arrayOf(seededCategoryID)
        )
        assertEquals("Seed Category", resumedCategory?.get("name"))

        val pending = client.queryOne("SELECT COUNT(*) AS count FROM _synchro_pending_changes")
        assertEquals(0L, (pending?.get("count") as Number).toLong())

        client.stop()
        client.close()
    }

    @Test
    fun testGlobalScopeRepairLeavesUserRowsUntouched() = runTest {
        val userID = UUID.randomUUID().toString()
        val clientID = UUID.randomUUID().toString()
        val dbPath = "schema_integ_7_${UUID.randomUUID()}.sqlite"
        val seededCategoryID = "10000000-0000-0000-0000-000000000006"
        val customerID = UUID.randomUUID().toString()
        val orderID = UUID.randomUUID().toString()

        val bootstrap = SynchroClient(
            makeConfig(
                userID = userID,
                clientID = clientID,
                dbPath = dbPath,
                seedPath = canonicalSeedPath
            ),
            context
        )

        bootstrap.start()
        bootstrap.execute(
            "INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, ?, ?)",
            arrayOf(customerID, userID, "Scoped Repair Customer", "2026-01-06T00:00:00.000Z", "2026-01-06T00:00:00.000Z")
        )
        bootstrap.execute(
            "INSERT INTO orders (id, customer_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, 'pending', 0, 'USD', ?, ?, ?)",
            arrayOf(orderID, customerID, "User Scope Row", "2026-01-06T00:00:00.000Z", "2026-01-06T00:00:00.000Z")
        )
        bootstrap.syncNow()
        bootstrap.stop()
        bootstrap.close()

        val rawDb = SynchroDatabase(context, dbPath)
        rawDb.writeSyncLockedTransaction { db ->
            SynchroMeta.deleteScopeRow(db, "global", "categories", seededCategoryID)
            db.execSQL("DELETE FROM categories WHERE id = ?", arrayOf(seededCategoryID))
        }
        rawDb.close()

        val client = SynchroClient(
            makeConfig(
                userID = userID,
                clientID = clientID,
                dbPath = dbPath
            ),
            context
        )
        client.start()

        val repairedCategory = client.queryOne(
            "SELECT name FROM categories WHERE id = ?",
            arrayOf(seededCategoryID)
        )
        assertEquals("Seed Category", repairedCategory?.get("name"))

        val preservedOrder = client.queryOne(
            "SELECT ship_address FROM orders WHERE id = ?",
            arrayOf(orderID)
        )
        assertEquals("User Scope Row", preservedOrder?.get("ship_address"))

        val pending = client.queryOne("SELECT COUNT(*) AS count FROM _synchro_pending_changes")
        assertEquals(0L, (pending?.get("count") as Number).toLong())

        client.stop()
        client.close()
    }

    @Test
    fun testSharedSeedRowsStayInSharedScopeOnly() = runTest {
        val userID = UUID.randomUUID().toString().lowercase()
        val clientID = UUID.randomUUID().toString()
        val dbPath = "schema_integ_8_${UUID.randomUUID()}.sqlite"
        val seededCategoryID = "10000000-0000-0000-0000-000000000006"
        val customerID = UUID.randomUUID().toString()
        val orderID = UUID.randomUUID().toString()

        val client = SynchroClient(
            makeConfig(
                userID = userID,
                clientID = clientID,
                dbPath = dbPath,
                seedPath = canonicalSeedPath
            ),
            context
        )

        client.start()
        client.execute(
            "INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, ?, ?)",
            arrayOf(customerID, userID, "Shared Scope Customer", "2026-01-07T00:00:00.000Z", "2026-01-07T00:00:00.000Z")
        )
        client.execute(
            "INSERT INTO orders (id, customer_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, 'pending', 0, 'USD', ?, ?, ?)",
            arrayOf(orderID, customerID, "User Scoped Order", "2026-01-07T00:00:00.000Z", "2026-01-07T00:00:00.000Z")
        )
        client.syncNow()

        val categoryScopes = client.query(
            """
            SELECT scope_id
            FROM _synchro_scope_rows
            WHERE table_name = 'categories' AND record_id = ?
            ORDER BY scope_id
            """.trimIndent(),
            arrayOf(seededCategoryID)
        )
        assertEquals(1, categoryScopes.size)
        assertEquals("global", categoryScopes.first()["scope_id"])

        val duplicatedCategoryScopes = client.queryOne(
            """
            SELECT COUNT(*) AS count
            FROM _synchro_scope_rows
            WHERE table_name = 'categories' AND record_id = ? AND scope_id != 'global'
            """.trimIndent(),
            arrayOf(seededCategoryID)
        )
        assertEquals(0L, (duplicatedCategoryScopes?.get("count") as Number).toLong())

        val orderRow = client.queryOne(
            "SELECT ship_address FROM orders WHERE id = ?",
            arrayOf(orderID)
        )
        assertEquals("User Scoped Order", orderRow?.get("ship_address"))

        client.stop()
        client.close()
    }
}
