package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28])
class SynchroClientTests {

    private fun makeConfig(): SynchroConfig {
        val dbName = "synchro_client_test_${UUID.randomUUID()}.sqlite"
        return SynchroConfig(
            dbPath = dbName,
            serverURL = "http://localhost:8080",
            authProvider = { "test-token" },
            clientID = "test-device",
            appVersion = "1.0.0"
        )
    }

    private fun makeClient(): SynchroClient {
        val config = makeConfig()
        val context = ApplicationProvider.getApplicationContext<Context>()
        return SynchroClient(config, context)
    }

    @Test
    fun testClientInitCreatesDatabase() {
        val client = makeClient()

        val rows = client.query("SELECT name FROM sqlite_master WHERE type='table'")
        val tableNames = rows.map { it["name"] as String }
        assertTrue(tableNames.contains("_synchro_pending_changes"))
        assertTrue(tableNames.contains("_synchro_meta"))

        client.close()
    }

    @Test
    fun testCoreSQL() {
        val client = makeClient()

        client.createTable("local_notes", listOf(
            ColumnDef(name = "id", type = "TEXT", nullable = false, primaryKey = true),
            ColumnDef(name = "body", type = "TEXT"),
        ))

        val result = client.execute(
            "INSERT INTO local_notes (id, body) VALUES (?, ?)",
            arrayOf("n1", "hello")
        )
        assertEquals(1, result.rowsAffected)

        val rows = client.query("SELECT * FROM local_notes WHERE id = ?", arrayOf("n1"))
        assertEquals(1, rows.size)
        assertEquals("hello", rows[0]["body"])

        val one = client.queryOne("SELECT * FROM local_notes WHERE id = ?", arrayOf("n1"))
        assertNotNull(one)

        client.close()
    }

    @Test
    fun testBatchExecution() {
        val client = makeClient()

        client.createTable("orders", listOf(
            ColumnDef(name = "id", type = "TEXT", nullable = false, primaryKey = true),
            ColumnDef(name = "value", type = "INTEGER"),
        ))

        val total = client.executeBatch(listOf(
            SQLStatement("INSERT INTO orders (id, value) VALUES (?, ?)", arrayOf("a", 1)),
            SQLStatement("INSERT INTO orders (id, value) VALUES (?, ?)", arrayOf("b", 2)),
            SQLStatement("INSERT INTO orders (id, value) VALUES (?, ?)", arrayOf("c", 3)),
        ))
        assertEquals(3, total)

        val rows = client.query("SELECT COUNT(*) as cnt FROM orders")
        assertEquals(3L, rows[0]["cnt"])

        client.close()
    }

    @Test
    fun testCreateIndex() {
        val client = makeClient()

        client.createTable("orders", listOf(
            ColumnDef(name = "id", type = "TEXT", nullable = false, primaryKey = true),
            ColumnDef(name = "category", type = "TEXT"),
        ))

        client.createIndex("orders", listOf("category"), unique = false)

        val indexes = client.query("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='orders'")
        val names = indexes.map { it["name"] as String }
        assertTrue(names.contains("idx_orders_category"))

        client.close()
    }

    @Test
    fun testOnChange() {
        val client = makeClient()

        client.createTable("events", listOf(
            ColumnDef(name = "id", type = "TEXT", nullable = false, primaryKey = true),
            ColumnDef(name = "name", type = "TEXT"),
        ))

        val latch = CountDownLatch(1)
        val cancellable = client.onChange(listOf("events")) {
            latch.countDown()
        }

        client.execute("INSERT INTO events (id, name) VALUES (?, ?)", arrayOf("e1", "test"))

        assertTrue(latch.await(2, TimeUnit.SECONDS))
        cancellable.cancel()
        client.close()
    }

    @Test
    fun testWatch() {
        val client = makeClient()

        client.createTable("counters", listOf(
            ColumnDef(name = "id", type = "TEXT", nullable = false, primaryKey = true),
            ColumnDef(name = "value", type = "INTEGER"),
        ))

        client.execute("INSERT INTO counters (id, value) VALUES (?, ?)", arrayOf("c1", 0))

        val latch = CountDownLatch(2) // initial + after update
        val receivedRows = mutableListOf<List<Row>>()

        val cancellable = client.watch(
            "SELECT * FROM counters WHERE id = ?",
            arrayOf("c1"),
            listOf("counters")
        ) { rows ->
            receivedRows.add(rows)
            latch.countDown()
        }

        // Trigger an update
        Thread {
            Thread.sleep(300)
            client.execute("UPDATE counters SET value = ? WHERE id = ?", arrayOf(42, "c1"))
        }.start()

        assertTrue(latch.await(3, TimeUnit.SECONDS))
        assertTrue(receivedRows.size >= 2)

        // Last callback should have the updated value
        val lastRows = receivedRows.last()
        if (lastRows.isNotEmpty()) {
            assertEquals(42L, lastRows[0]["value"])
        }

        cancellable.cancel()
        client.close()
    }

    // MARK: - Seed Database

    @Test
    fun testSeedDatabaseCopiedOnInit() {
        val context = ApplicationProvider.getApplicationContext<Context>()

        // Create a seed database with a local table and data
        val seedConfig = SynchroConfig(
            dbPath = "seed_${UUID.randomUUID()}.sqlite",
            serverURL = "http://localhost:8080",
            authProvider = { "token" },
            clientID = "seed-builder",
            appVersion = "1.0.0"
        )
        val seedClient = SynchroClient(seedConfig, context)
        seedClient.createTable("local_cache", listOf(
            ColumnDef(name = "id", type = "TEXT", nullable = false, primaryKey = true),
            ColumnDef(name = "data", type = "TEXT"),
        ))
        seedClient.execute(
            "INSERT INTO local_cache (id, data) VALUES (?, ?)",
            arrayOf("row1", "seeded-value")
        )
        val seedFile = context.getDatabasePath(seedConfig.dbPath)
        seedClient.close()

        // Init a new client with a fresh path, pointing to the seed
        val destPath = "dest_${UUID.randomUUID()}.sqlite"
        val destFile = context.getDatabasePath(destPath)
        assertFalse("destination should not exist yet", destFile.exists())

        val client = SynchroClient(
            SynchroConfig(
                dbPath = destPath,
                serverURL = "http://localhost:8080",
                authProvider = { "token" },
                clientID = "test-device",
                appVersion = "1.0.0",
                seedDatabasePath = seedFile.absolutePath
            ),
            context
        )

        // Seed data should be available
        val row = client.queryOne("SELECT data FROM local_cache WHERE id = ?", arrayOf("row1"))
        assertNotNull("seed data should be queryable after copy", row)
        assertEquals("seeded-value", row?.get("data"))

        client.close()
    }

    @Test
    fun testSeedNotCopiedWhenDatabaseAlreadyExists() {
        val context = ApplicationProvider.getApplicationContext<Context>()

        // Create a seed with distinctive data
        val seedConfig = SynchroConfig(
            dbPath = "seed_${UUID.randomUUID()}.sqlite",
            serverURL = "http://localhost:8080",
            authProvider = { "token" },
            clientID = "seed-builder",
            appVersion = "1.0.0"
        )
        val seedClient = SynchroClient(seedConfig, context)
        seedClient.createTable("marker", listOf(
            ColumnDef(name = "id", type = "TEXT", nullable = false, primaryKey = true),
        ))
        seedClient.execute("INSERT INTO marker (id) VALUES (?)", arrayOf("from-seed"))
        val seedFile = context.getDatabasePath(seedConfig.dbPath)
        seedClient.close()

        // Create the destination database FIRST (simulates existing app data)
        val destPath = "dest_${UUID.randomUUID()}.sqlite"
        val existingClient = SynchroClient(
            SynchroConfig(
                dbPath = destPath,
                serverURL = "http://localhost:8080",
                authProvider = { "token" },
                clientID = "existing-device",
                appVersion = "1.0.0"
            ),
            context
        )
        existingClient.close()
        assertTrue(context.getDatabasePath(destPath).exists())

        // Init with seed. Seed should NOT overwrite the existing database.
        val client = SynchroClient(
            SynchroConfig(
                dbPath = destPath,
                serverURL = "http://localhost:8080",
                authProvider = { "token" },
                clientID = "existing-device",
                appVersion = "1.0.0",
                seedDatabasePath = seedFile.absolutePath
            ),
            context
        )

        // The "marker" table from the seed should NOT exist
        val tables = client.query("SELECT name FROM sqlite_master WHERE type='table' AND name='marker'")
        assertEquals("seed should not overwrite an existing database", 0, tables.size)

        client.close()
    }

    @Test
    fun testSeedNilPathDoesNotCrash() {
        val context = ApplicationProvider.getApplicationContext<Context>()
        val client = SynchroClient(
            SynchroConfig(
                dbPath = "noseed_${UUID.randomUUID()}.sqlite",
                serverURL = "http://localhost:8080",
                authProvider = { "token" },
                clientID = "test-device",
                appVersion = "1.0.0",
                seedDatabasePath = null
            ),
            context
        )

        // Should have meta tables from fresh init
        val meta = client.queryOne("SELECT value FROM _synchro_meta WHERE key = 'checkpoint'")
        assertNotNull(meta)

        client.close()
    }

    // MARK: - Schema

    @Test
    fun testAlterTable() {
        val client = makeClient()

        client.createTable("people", listOf(
            ColumnDef(name = "id", type = "TEXT", nullable = false, primaryKey = true),
            ColumnDef(name = "name", type = "TEXT"),
        ))

        client.alterTable("people", listOf(
            ColumnDef(name = "age", type = "INTEGER"),
        ))

        client.execute("INSERT INTO people (id, name, age) VALUES (?, ?, ?)", arrayOf("p1", "Alice", 30))
        val row = client.queryOne("SELECT age FROM people WHERE id = ?", arrayOf("p1"))
        assertEquals(30L, row?.get("age"))

        client.close()
    }

    @Test
    fun testTransactions() {
        val client = makeClient()

        client.createTable("txtest", listOf(
            ColumnDef(name = "id", type = "TEXT", nullable = false, primaryKey = true),
            ColumnDef(name = "val", type = "TEXT"),
        ))

        // Write transaction
        val written = client.writeTransaction { db ->
            db.execSQL("INSERT INTO txtest (id, val) VALUES (?, ?)", arrayOf("t1", "hello"))
            db.rawQuery("SELECT changes()", null).use { c ->
                c.moveToFirst()
                c.getInt(0)
            }
        }
        assertEquals(1, written)

        // Read transaction
        val value = client.readTransaction { db ->
            db.rawQuery("SELECT val FROM txtest WHERE id = ?", arrayOf("t1")).use { cursor ->
                if (cursor.moveToFirst()) cursor.getString(0) else null
            }
        }
        assertEquals("hello", value)

        client.close()
    }

    @Test
    fun testMetaTablesInitialized() {
        val client = makeClient()

        // sync_lock should be initialized to '0'
        val lockRow = client.queryOne("SELECT value FROM _synchro_meta WHERE key = 'sync_lock'")
        assertEquals("0", lockRow?.get("value"))

        // checkpoint should be initialized to '0'
        val cpRow = client.queryOne("SELECT value FROM _synchro_meta WHERE key = 'checkpoint'")
        assertEquals("0", cpRow?.get("value"))

        client.close()
    }
}
