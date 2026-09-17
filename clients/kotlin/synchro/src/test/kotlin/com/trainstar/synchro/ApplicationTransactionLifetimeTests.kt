package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28])
class ApplicationTransactionLifetimeTests {
    private val context = ApplicationProvider.getApplicationContext<Context>()
    private val databaseName = "synchro_transaction_lifetime_${UUID.randomUUID()}.sqlite"
    private lateinit var database: SynchroDatabase
    private val insertSQL = "INSERT INTO orders (id, ship_address, user_id, updated_at) " +
        "VALUES (?, 'Address', 'u1', '2026-01-01T00:00:00.000000Z')"

    @Before
    fun setUp() {
        database = SynchroDatabase.open(context, databaseName)
        installTestSchema(database, 1, PROTOCOL_TEST_SCHEMA_HASH, protocolOrdersSchemaManifest().localTables())
    }

    @After
    fun tearDown() {
        database.close()
        context.deleteDatabase(databaseName)
    }

    @Test
    fun returnedWriteHandlesExpireWithoutLosingCommittedCapture() {
        for (authored in listOf(false, true)) {
            val escaped = writeTransaction(authored) { transaction ->
                assertTrue(transaction.query("SELECT id FROM orders WHERE id = 'missing'").isEmpty())
                transaction.executeBatch(listOf(SQLStatement(insertSQL, arrayOf("row-$authored"))))
                assertEquals("row-$authored", transaction.queryOne("SELECT id FROM orders WHERE id = ?", arrayOf("row-$authored"))?.get("id"))
                transaction
            }
            assertExpired(escaped)
            database.applicationTransaction { assertExpired(escaped) }
        }
        assertEquals(2L, database.queryOne("SELECT COUNT(*) AS count FROM orders")?.get("count"))
        assertEquals(2L, database.queryOne("SELECT COUNT(*) AS count FROM _synchro_pending_changes")?.get("count"))
    }

    @Test
    fun failedWriteCallbacksExpireTheirHandlesAndRollBackCapture() {
        for (authored in listOf(false, true)) {
            lateinit var escaped: ApplicationTransaction
            assertThrows(UnsupportedOperationException::class.java) {
                writeTransaction(authored) { transaction ->
                    escaped = transaction
                    transaction.execute(insertSQL, arrayOf("rolled-back"))
                    throw UnsupportedOperationException("callback failed")
                }
            }
            assertExpired(escaped)
        }
        assertTrue(database.query("SELECT id FROM orders").isEmpty())
        assertTrue(database.query("SELECT mutation_id FROM _synchro_pending_changes").isEmpty())
        assertTrue(database.query("SELECT statement_token FROM _synchro_capture_context").isEmpty())
        assertTrue(database.query("SELECT statement_token FROM _synchro_capture_fields").isEmpty())
    }

    @Test
    fun readHandlesExpireOnReturnAndFailureWhileTheOuterHandleRemainsValid() {
        val returned = database.applicationReadTransaction { outer ->
            val inner = database.applicationReadTransaction { it }
            assertExpired(inner)
            assertTrue(outer.query("SELECT id FROM orders").isEmpty())
            assertEquals(null, outer.queryOne("SELECT id FROM orders"))
            outer
        }
        assertExpired(returned)
        lateinit var failed: ApplicationReadTransaction
        assertThrows(UnsupportedOperationException::class.java) {
            database.applicationReadTransaction {
                failed = it
                throw UnsupportedOperationException("callback failed")
            }
        }
        assertExpired(failed)
    }

    @Test
    fun activeHandlesRejectOtherThreadsBeforeAccessingSqlite() {
        val executor = Executors.newSingleThreadExecutor()
        try {
            database.applicationReadTransaction { transaction ->
                executor.submit { assertExpired(transaction) }.get(5, TimeUnit.SECONDS)
                assertTrue(transaction.query("SELECT id FROM orders").isEmpty())
            }
            for (authored in listOf(false, true)) {
                writeTransaction(authored) { transaction ->
                    executor.submit { assertExpired(transaction) }.get(5, TimeUnit.SECONDS)
                    transaction.execute(insertSQL, arrayOf("owner-$authored"))
                }
            }
            assertEquals(2L, database.queryOne("SELECT COUNT(*) AS count FROM orders")?.get("count"))
            assertEquals(2L, database.queryOne("SELECT COUNT(*) AS count FROM _synchro_pending_changes")?.get("count"))
        } finally {
            executor.shutdownNow()
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS))
        }
    }

    @Test
    fun anInnerWriteHandleExpiresBeforeItsOuterCallbackEnds() {
        database.applicationTransaction { outer ->
            val inner = database.applicationTransaction { it }
            assertExpired(inner)
            outer.execute(insertSQL, arrayOf("outer"))
        }
        assertEquals(1L, database.queryOne("SELECT COUNT(*) AS count FROM _synchro_pending_changes")?.get("count"))
    }

    private fun <T> writeTransaction(authored: Boolean, block: (ApplicationTransaction) -> T): T =
        if (authored) {
            database.applicationAuthoredWriteTransaction("orders", Operation.INSERT, listOf("ship_address"), block)
        } else {
            database.applicationTransaction(block)
        }

    private fun assertExpired(transaction: ApplicationReadTransaction) {
        assertThrows(IllegalStateException::class.java) { transaction.query("SELECT id FROM orders") }
        assertThrows(IllegalStateException::class.java) { transaction.queryOne("SELECT id FROM orders") }
    }

    private fun assertExpired(transaction: ApplicationTransaction) {
        assertThrows(IllegalStateException::class.java) { transaction.query("SELECT id FROM orders") }
        assertThrows(IllegalStateException::class.java) { transaction.queryOne("SELECT id FROM orders") }
        assertThrows(IllegalStateException::class.java) { transaction.execute(insertSQL, arrayOf("escaped")) }
        assertThrows(IllegalStateException::class.java) { transaction.executeBatch(emptyList()) }
        assertThrows(IllegalStateException::class.java) {
            transaction.executeBatch(listOf(SQLStatement(insertSQL, arrayOf("escaped-batch"))))
        }
    }
}
