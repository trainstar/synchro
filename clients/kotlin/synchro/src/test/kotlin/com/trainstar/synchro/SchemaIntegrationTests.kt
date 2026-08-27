package com.trainstar.synchro

import android.content.Context
import android.database.sqlite.SQLiteDatabase
import androidx.test.core.app.ApplicationProvider
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
import java.io.File
import java.util.UUID
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/// Integration tests for schema reconciliation and seed database loading.
/// Requires SYNCHRO_TEST_URL and SYNCHRO_TEST_JWT_SECRET environment variables.
@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28])
class SchemaIntegrationTests {

    private data class CatalogEntry(
        val type: String,
        val name: String,
        val tableName: String,
        val sql: String,
    )

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

    private fun <T> withInternalDatabase(dbPath: String, block: (SQLiteDatabase) -> T): T {
        val database = SynchroDatabase.open(context, dbPath)
        return try {
            database.readTransaction(block)
        } finally {
            database.close()
        }
    }

    private fun internalQuery(
        dbPath: String,
        sql: String,
        params: Array<out Any?>? = null,
    ): List<Row> = withInternalDatabase(dbPath) { db -> queryWithTypedBindings(db, sql, params) }

    private fun internalQueryOne(
        dbPath: String,
        sql: String,
        params: Array<out Any?>? = null,
    ): Row? = withInternalDatabase(dbPath) { db -> queryOneWithTypedBindings(db, sql, params) }

    private fun schemaCatalog(
        database: SynchroDatabase,
        excludedNames: Set<String> = emptySet(),
    ): List<CatalogEntry> = database.readTransaction { db ->
        db.rawQuery(
            """
            SELECT type, name, tbl_name, sql
            FROM sqlite_master
            WHERE type IN ('table', 'index', 'trigger')
              AND name NOT LIKE 'sqlite_%'
              AND sql IS NOT NULL
            ORDER BY type, name
            """.trimIndent(),
            null,
        ).use { cursor ->
            buildList {
                while (cursor.moveToNext()) {
                    val name = cursor.getString(1)
                    if (name !in excludedNames) {
                        add(
                            CatalogEntry(
                                type = cursor.getString(0),
                                name = name,
                                tableName = cursor.getString(2),
                                sql = canonicalCatalogDDL(cursor.getString(3)),
                            ),
                        )
                    }
                }
            }
        }
    }

    private fun assertCatalogEquals(expected: List<CatalogEntry>, actual: List<CatalogEntry>) {
        assertEquals("catalog entry count", expected.size, actual.size)
        expected.zip(actual).firstOrNull { (expectedEntry, actualEntry) ->
            expectedEntry != actualEntry
        }?.let { (expectedEntry, actualEntry) ->
            fail(
                "catalog entry ${actualEntry.type}/${actualEntry.name} differs: " +
                    "${actualEntry.sql} != ${expectedEntry.sql}",
            )
        }
    }

    private fun canonicalCatalogDDL(source: String): String {
        val output = StringBuilder(source.length)
        var pendingSpace = false
        var quoteEnd: Char? = null
        var index = 0
        while (index < source.length) {
            val character = source[index]
            if (quoteEnd != null) {
                output.append(character)
                if (character == quoteEnd) {
                    if (index + 1 < source.length && source[index + 1] == quoteEnd) {
                        index += 1
                        output.append(source[index])
                    } else {
                        quoteEnd = null
                    }
                }
                index += 1
                continue
            }
            if (character.isWhitespace()) {
                pendingSpace = output.isNotEmpty()
                index += 1
                continue
            }
            if (
                pendingSpace &&
                output.isNotEmpty() &&
                output.last() !in "(," &&
                character !in "),;"
            ) {
                output.append(' ')
            }
            pendingSpace = false
            quoteEnd = when (character) {
                '\'', '"', '`' -> character
                '[' -> ']'
                else -> null
            }
            output.append(character)
            index += 1
        }
        return output.toString()
    }

    // -- 1. testAdditiveSchemaChangePreservesData --

    @Test
    fun testAdditiveSchemaChangePreservesData() = runBlocking {
        val serverSchema = fetchServerSchema()
        val userID = UUID.randomUUID().toString().lowercase()
        val clientID = UUID.randomUUID().toString()
        val dbPath = "schema_integ_1_${UUID.randomUUID()}.sqlite"

        val ordersTable = serverSchema.tables.firstOrNull { it.tableName == "orders" }
            ?: return@runBlocking fail("server schema must include 'orders' table")

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
            "INSERT INTO orders (id, customer_id, user_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, ?, 'pending', 0, 'USD', ?, ?, ?)",
            arrayOf(orderID, custID, userID, """{"street":"123 Main St"}""", "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00.000Z")
        )
        client1.syncNow()

        client1.stop()
        client1.close()

        // 3. Reconnect with the same client identity and installed schema.
        val client2 = SynchroClient(makeConfig(userID = userID, dbPath = dbPath, clientID = clientID), context)
        client2.start()

        // 4. Pushed data remains available.
        val row = client2.queryOne("SELECT id, ship_address FROM orders WHERE id = ?", arrayOf(orderID))
        assertNotNull("pushed data should survive schema reconciliation on reconnect", row)
        assertEquals("""{"street":"123 Main St"}""", row?.get("ship_address"))

        // 5. All server columns still exist.
        val columns = mutableListOf<String>()
        withInternalDatabase(dbPath) { rawDb2 ->
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
    fun testLocalOnlyTablesSurviveReconnect() = runBlocking {
        val userID = UUID.randomUUID().toString()
        val clientID = UUID.randomUUID().toString()
        val dbPath = "schema_integ_2_${UUID.randomUUID()}.sqlite"

        // Connect and sync (creates synced tables)
        val client1 = SynchroClient(makeConfig(userID = userID, dbPath = dbPath, clientID = clientID), context)
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

        // Reconnect with the same client identity.
        val client2 = SynchroClient(makeConfig(userID = userID, dbPath = dbPath, clientID = clientID), context)
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
    fun testSeedDatabaseWorksOffline() = runBlocking {
        // Install the canonical producer artifact. Do not call start().
        val dbPath = "schema_integ_3_${UUID.randomUUID()}.sqlite"
        val userID = UUID.randomUUID().toString()
        val client = SynchroClient(
            makeConfig(userID = userID, dbPath = dbPath, seedPath = canonicalSeedPath),
            context,
        )

        // Tables should be queryable immediately (offline)
        val orders = client.query("SELECT * FROM orders")
        assertEquals(0, orders.size)

        // Insert customer (FK required) and order offline — CDC triggers should fire
        val custID = UUID.randomUUID().toString()
        client.execute(
            "INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, ?, ?)",
            arrayOf(custID, userID, "Offline Customer", "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00.000Z")
        )
        val orderID = UUID.randomUUID().toString()
        client.execute(
            "INSERT INTO orders (id, customer_id, user_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, ?, 'pending', 0, 'USD', ?, ?, ?)",
            arrayOf(orderID, custID, userID, """{"street":"456 Oak Ave"}""", "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00.000Z")
        )

        // Query back
        val row = client.queryOne("SELECT ship_address FROM orders WHERE id = ?", arrayOf(orderID))
        assertNotNull(row)
        assertEquals("""{"street":"456 Oak Ave"}""", row?.get("ship_address"))

        // Verify CDC trigger fired
        val pending = internalQuery(
            dbPath,
            "SELECT record_id, operation FROM _synchro_pending_changes WHERE table_name = 'orders'",
        )
        assertEquals(1, pending.size)
        assertEquals(orderID, pending[0]["record_id"])
        assertEquals("insert", pending[0]["operation"])

        client.close()
    }

    @Test
    fun testCanonicalGoSeedDDLConvergesWithFreshKotlinDDL() {
        val migratedName = "ddl_migrated_${UUID.randomUUID()}.sqlite"
        val freshName = "ddl_fresh_${UUID.randomUUID()}.sqlite"
        val canonicalSeed = File(canonicalSeedPath)
        val sourceBytes = canonicalSeed.readBytes()
        try {
            SeedDatabaseInstaller.installIfNeeded(context, canonicalSeedPath, migratedName)
            val migrated = SynchroDatabase.open(context, migratedName)
            val fresh = SynchroDatabase.open(context, freshName)
            try {
                val tables = requireNotNull(SchemaManager(migrated).loadStoredLocalSchema())
                fresh.writeTransaction { db ->
                    createTestSyncedTablesInTransaction(db, tables)
                }

                val expected = schemaCatalog(fresh)
                assertCatalogEquals(expected, schemaCatalog(migrated, setOf("grdb_migrations")))
                assertEquals(
                    SynchroDatabase.DATABASE_VERSION.toLong(),
                    migrated.queryOne("PRAGMA user_version")?.get("user_version"),
                )
                assertEquals(
                    SynchroDatabase.DATABASE_VERSION.toLong(),
                    fresh.queryOne("PRAGMA user_version")?.get("user_version"),
                )

                migrated.writeTransaction { db ->
                    db.execSQL("ALTER TABLE _synchro_scopes ADD COLUMN ddl_identity_drift TEXT")
                }
                assertNotEquals(expected, schemaCatalog(migrated, setOf("grdb_migrations")))
                assertArrayEquals(sourceBytes, canonicalSeed.readBytes())
                for (suffix in listOf("-journal", "-wal", "-shm")) {
                    assertFalse(File(canonicalSeedPath + suffix).exists())
                }
            } finally {
                migrated.close()
                fresh.close()
            }
        } finally {
            context.deleteDatabase(migratedName)
            context.deleteDatabase(freshName)
        }
    }

    @Test
    fun testOfflineWritesBeforeFirstConnectArePushedOnFirstSync() = runBlocking {
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

        val pendingBeforeConnect = internalQuery(
            dbPath,
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

        val pendingAfterConnect = onlineClient.pendingChangeCount()
        val localRow = onlineClient.queryOne(
            "SELECT name FROM customers WHERE id = ?",
            arrayOf(customerID)
        )
        val rejectedAfterConnect = internalQuery(dbPath, "SELECT mutation_id FROM _synchro_rejected_mutations")
        onlineClient.stop()
        onlineClient.close()

        assertEquals(1, pendingBeforeConnect.size)
        assertEquals("Offline First Customer", offlineRow?.get("name"))
        assertEquals(0, pendingAfterConnect)
        assertEquals("Offline First Customer", localRow?.get("name"))
        assertTrue(rejectedAfterConnect.isEmpty())
    }

    // -- 4. testIncompleteSeedDoesNotPublish --

    @Test
    fun testIncompleteSeedDoesNotPublish() {
        val corruptSeed = context.getDatabasePath("corrupt_seed_${UUID.randomUUID()}.sqlite")
        File(canonicalSeedPath).copyTo(corruptSeed)
        val raw = SQLiteDatabase.openDatabase(
            corruptSeed.absolutePath,
            null,
            SQLiteDatabase.OPEN_READWRITE,
        )
        try {
            raw.execSQL("UPDATE _synchro_meta SET value = '0' WHERE key = 'snapshot_complete'")
        } finally {
            raw.close()
        }

        val dbPath = "schema_integ_4_${UUID.randomUUID()}.sqlite"
        try {
            assertThrows(Exception::class.java) {
                SynchroClient(
                    makeConfig(
                        userID = UUID.randomUUID().toString(),
                        dbPath = dbPath,
                        seedPath = corruptSeed.absolutePath,
                    ),
                    context,
                )
            }
            val destination = context.getDatabasePath(dbPath)
            assertFalse(destination.exists())
            assertTrue(
                destination.parentFile?.listFiles().orEmpty().none {
                    it.name.startsWith(".${destination.name}.seed-")
                }
            )
        } finally {
            corruptSeed.delete()
            context.deleteDatabase(dbPath)
        }
    }

    @Test
    fun testBundledSeedRepairsPortableScopeCorruptionOnConnect() = runBlocking {
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

        val seededScope = withInternalDatabase(dbPath) { db ->
            SynchroMeta.getScope(db, "global")
        }
        assertEquals("global", seededScope?.scopeID)
        assertNull(seededScope?.cursor)
        assertTrue((seededScope?.checksum ?: "").isNotEmpty())

        val seededRow = bootstrap.queryOne(
            "SELECT name FROM categories WHERE id = ?",
            arrayOf(seededCategoryID)
        )
        assertEquals(seededCategoryName, seededRow?.get("name"))

        bootstrap.close()

        val rawDb = SynchroDatabase.open(context, dbPath)
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

        val repairedScope = withInternalDatabase(dbPath) { db ->
            SynchroMeta.getScope(db, "global")
        }
        assertEquals("global", repairedScope?.scopeID)
        assertTrue((repairedScope?.cursor ?: "").isNotEmpty())
        assertTrue((repairedScope?.checksum ?: "").isNotEmpty())
        assertTrue((repairedScope?.generation ?: 0L) > (seededScope?.generation ?: 0L))

        assertEquals(0, client.pendingChangeCount())

        client.stop()
        client.close()
    }

    @Test
    fun testBundledSeedContinuesIncrementallyWithoutRebuild() = runBlocking {
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
        val initialScope = withInternalDatabase(dbPath) { db ->
            SynchroMeta.getScope(db, "global")
        }
        assertEquals("global", initialScope?.scopeID)
        assertNull(initialScope?.cursor)
        assertTrue((initialScope?.checksum ?: "").isNotEmpty())

        val initialGeneration = initialScope?.generation
        val initialCategory = client.queryOne(
            "SELECT name FROM categories WHERE id = ?",
            arrayOf(seededCategoryID)
        )
        assertEquals("Seed Category", initialCategory?.get("name"))

        client.start()

        val resumedScope = withInternalDatabase(dbPath) { db ->
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

        assertEquals(0, client.pendingChangeCount())

        client.stop()
        client.close()
    }

    @Test
    fun testGlobalScopeRepairLeavesUserRowsUntouched() = runBlocking {
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
            "INSERT INTO orders (id, customer_id, user_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, ?, 'pending', 0, 'USD', ?, ?, ?)",
            arrayOf(orderID, customerID, userID, """{"street":"User Scope Row"}""", "2026-01-06T00:00:00.000Z", "2026-01-06T00:00:00.000Z")
        )
        bootstrap.syncNow()
        bootstrap.stop()
        bootstrap.close()

        val rawDb = SynchroDatabase.open(context, dbPath)
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
        assertEquals("""{"street":"User Scope Row"}""", preservedOrder?.get("ship_address"))

        assertEquals(0, client.pendingChangeCount())

        client.stop()
        client.close()
    }

    @Test
    fun testSharedSeedRowsStayInSharedScopeOnly() = runBlocking {
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
            "INSERT INTO orders (id, customer_id, user_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, ?, 'pending', 0, 'USD', ?, ?, ?)",
            arrayOf(orderID, customerID, userID, """{"street":"User Scoped Order"}""", "2026-01-07T00:00:00.000Z", "2026-01-07T00:00:00.000Z")
        )
        client.syncNow()

        val categoryScopes = internalQuery(
            dbPath,
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

        val duplicatedCategoryScopes = internalQueryOne(
            dbPath,
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
        assertEquals("""{"street":"User Scoped Order"}""", orderRow?.get("ship_address"))

        client.stop()
        client.close()
    }
}
