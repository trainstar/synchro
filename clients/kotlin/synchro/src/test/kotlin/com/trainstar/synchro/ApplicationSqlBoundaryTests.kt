package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import java.util.UUID
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assert.assertThrows
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28])
class ApplicationSqlBoundaryTests {
    private val context = ApplicationProvider.getApplicationContext<Context>()

    @Test
    fun applicationSqlRejectsMetadataAliasesCtesAndDdl() {
        val dbName = databaseName()
        val client = SynchroClient(config(dbName), context)
        try {
            client.createTable(
                "notes",
                listOf(
                    ColumnDef("id", "TEXT", nullable = false, primaryKey = true),
                    ColumnDef("body", "TEXT", nullable = false),
                ),
            )

            client.execute(
                "WITH source AS (SELECT 1) INSERT INTO notes (id, body) SELECT 'n1', 'safe' FROM source",
            )

            assertThrows(IllegalArgumentException::class.java) {
                client.execute("UPDATE _synchro_meta SET value = '1' WHERE key = 'sync_lock'")
            }
            assertThrows(IllegalArgumentException::class.java) {
                client.execute("WITH source AS (SELECT 1) UPDATE main._synchro_meta SET value = '1'")
            }
            assertThrows(IllegalArgumentException::class.java) {
                client.execute(
                    "WITH source AS (SELECT 1) INSERT INTO notes (id, body) " +
                        "SELECT 'n2', 'blocked' FROM source AS _synchro_alias",
                )
            }
            assertThrows(IllegalArgumentException::class.java) {
                client.query("WITH source AS (SELECT value FROM _synchro_meta) SELECT * FROM source")
            }
            assertThrows(IllegalArgumentException::class.java) {
                client.execute("CREATE TRIGGER app_trigger AFTER INSERT ON notes BEGIN SELECT 1; END")
            }
            assertThrows(IllegalArgumentException::class.java) {
                client.query("SELECT name FROM sqlite_master")
            }

            assertEquals(listOf("n1"), client.query("SELECT id FROM notes ORDER BY id").map { it.getValue("id") })
        } finally {
            client.close()
            context.deleteDatabase(dbName)
        }
    }

    @Test
    fun applicationWritesRejectViewsAndTamperedCaptureTriggers() {
        val viewDBName = databaseName()
        createSyncedDatabase(viewDBName) { database ->
            database.execute(
                "CREATE VIEW app_metadata_writer AS SELECT value FROM _synchro_meta WHERE key = 'sync_lock'",
            )
            database.execute(
                """
                CREATE TRIGGER app_metadata_writer_update
                INSTEAD OF UPDATE ON app_metadata_writer
                BEGIN
                    UPDATE _synchro_meta SET value = NEW.value WHERE key = 'sync_lock';
                END
                """.trimIndent(),
            )
        }
        val viewClient = SynchroClient(config(viewDBName), context)
        try {
            assertThrows(IllegalArgumentException::class.java) {
                viewClient.execute("UPDATE app_metadata_writer SET value = '1'")
            }
        } finally {
            viewClient.close()
        }
        val viewInspection = SynchroDatabase.open(context, viewDBName)
        try {
            assertEquals(
                "0",
                viewInspection.queryOne("SELECT value FROM _synchro_meta WHERE key = 'sync_lock'")?.get("value"),
            )
        } finally {
            viewInspection.close()
            context.deleteDatabase(viewDBName)
        }

        val tamperedDBName = databaseName()
        createSyncedDatabase(tamperedDBName) { database ->
            database.execute("DROP TRIGGER _synchro_cdc_insert_orders")
        }
        val tamperedClient = SynchroClient(config(tamperedDBName), context)
        try {
            assertThrows(IllegalStateException::class.java) {
                tamperedClient.execute(
                    "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                    arrayOf("blocked", "Address", "u1", "2026-01-01T00:00:00.000000Z"),
                )
            }
            assertTrue(tamperedClient.query("SELECT id FROM orders").isEmpty())
        } finally {
            tamperedClient.close()
            context.deleteDatabase(tamperedDBName)
        }
    }

    @Test
    fun applicationTransactionCachesTriggerValidationButRechecksLaterTransaction() {
        val dbName = databaseName()
        createSyncedDatabase(dbName)
        val client = SynchroClient(config(dbName), context)
        try {
            client.transaction { transaction ->
                transaction.execute(
                    "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                    arrayOf("first", "Address 1", "u1", "2026-01-01T00:00:00.000000Z"),
                )
                transaction.execute(
                    "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                    arrayOf("second", "Address 2", "u1", "2026-01-01T00:00:01.000000Z"),
                )
            }
        } finally {
            client.close()
        }

        val captured = SynchroDatabase.open(context, dbName)
        try {
            assertEquals(
                2L,
                captured.queryOne("SELECT COUNT(*) AS count FROM _synchro_pending_changes")?.get("count"),
            )
            captured.execute("DROP TRIGGER _synchro_cdc_insert_orders")
        } finally {
            captured.close()
        }

        val tamperedClient = SynchroClient(config(dbName), context)
        try {
            assertThrows(IllegalStateException::class.java) {
                tamperedClient.transaction { transaction ->
                    transaction.execute(
                        "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                        arrayOf("third", "Address 3", "u1", "2026-01-01T00:00:02.000000Z"),
                    )
                }
            }
        } finally {
            tamperedClient.close()
            context.deleteDatabase(dbName)
        }
    }

    @Test
    fun applicationWriteRejectsLocalAfterTriggerThatCanReachReservedState() {
        val dbName = databaseName()
        createSyncedDatabase(dbName) { database ->
            database.createLocalOnlyTable(
                "drafts",
                listOf(
                    ColumnDef("id", "TEXT", nullable = false, primaryKey = true),
                    ColumnDef("body", "TEXT", nullable = false),
                ),
            )
            database.execute(
                """
                CREATE TRIGGER application_after_drafts
                AFTER INSERT ON drafts
                BEGIN
                    UPDATE _synchro_meta SET value = '1' WHERE key = 'sync_lock';
                END
                """.trimIndent(),
            )
        }
        val client = SynchroClient(config(dbName), context)
        try {
            assertThrows(IllegalStateException::class.java) {
                client.execute("INSERT INTO drafts (id, body) VALUES ('d1', 'blocked')")
            }
            assertTrue(client.query("SELECT id FROM drafts").isEmpty())
            assertEquals(
                "0",
                SynchroDatabase.open(context, dbName).useInternal { database ->
                    database.queryOne("SELECT value FROM _synchro_meta WHERE key = 'sync_lock'")?.get("value")
                },
            )
        } finally {
            client.close()
            context.deleteDatabase(dbName)
        }
    }

    @Test
    fun applicationWriteRejectsSyncedAfterTriggerThatCanBypassCapture() {
        val dbName = databaseName()
        createSyncedDatabase(dbName) { database ->
            database.execute(
                """
                CREATE TRIGGER application_after_orders
                AFTER INSERT ON orders
                BEGIN
                    DELETE FROM _synchro_pending_changes;
                END
                """.trimIndent(),
            )
        }
        val client = SynchroClient(config(dbName), context)
        try {
            assertThrows(IllegalStateException::class.java) {
                client.execute(
                    "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES (?, ?, ?, ?)",
                    arrayOf("blocked", "Address", "u1", "2026-01-01T00:00:00.000000Z"),
                )
            }
            assertTrue(client.query("SELECT id FROM orders").isEmpty())
        } finally {
            client.close()
            context.deleteDatabase(dbName)
        }
    }

    private fun createSyncedDatabase(dbName: String, mutate: (SynchroDatabase) -> Unit = {}) {
        val database = SynchroDatabase.open(context, dbName)
        try {
            installTestSchema(
                database,
                schemaVersion = 1,
                schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
                tables = protocolOrdersSchemaManifest().localTables(),
            )
            mutate(database)
        } finally {
            database.close()
        }
    }

    private fun config(dbName: String): SynchroConfig = SynchroConfig(
        dbPath = dbName,
        serverURL = "http://localhost:8080",
        authProvider = { "test-token" },
        clientID = "sql-boundary-client",
        appVersion = "1.0.0",
    )

    private fun databaseName(): String = "synchro_sql_boundary_${UUID.randomUUID()}.sqlite"

    private fun <T> SynchroDatabase.useInternal(block: (SynchroDatabase) -> T): T =
        try {
            block(this)
        } finally {
            close()
        }
}
