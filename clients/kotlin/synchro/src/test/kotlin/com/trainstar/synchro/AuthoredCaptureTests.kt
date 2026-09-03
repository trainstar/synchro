package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import java.util.UUID
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28])
class AuthoredCaptureTests {
    private val context = ApplicationProvider.getApplicationContext<Context>()

    @Test
    fun omittedDefaultRemainsAbsentFromTheCapturedInsert() {
        val databaseName = databaseName()
        val client = clientWithSchema(databaseName)
        try {
            client.authoredWriteTransaction(
                tableName = authoredTable.tableName,
                operation = Operation.INSERT,
                columnNames = listOf("body"),
            ) { transaction ->
                transaction.execute(
                    "INSERT INTO authored_rows (id, body, updated_at) VALUES (?, ?, ?)",
                    arrayOf("row-1", "authored", "2026-01-01T00:00:00.000000Z"),
                )
            }

            assertEquals("default", query(databaseName, "SELECT default_value FROM authored_rows")
                .single().getValue("default_value"))
            assertLedger(
                databaseName,
                expectedOperations = listOf("insert"),
                expectedFields = listOf(listOf("field-body")),
            )
        } finally {
            client.close()
            context.deleteDatabase(databaseName)
        }
    }

    @Test
    fun explicitDefaultValuedWriteRemainsAuthored() {
        val databaseName = databaseName()
        val client = clientWithSchema(databaseName)
        try {
            client.authoredWriteTransaction(
                tableName = authoredTable.tableName,
                operation = Operation.INSERT,
                columnNames = listOf("default_value"),
            ) { transaction ->
                transaction.execute(
                    "INSERT INTO authored_rows (id, default_value, updated_at) VALUES (?, ?, ?)",
                    arrayOf("row-1", "default", "2026-01-01T00:00:00.000000Z"),
                )
            }

            assertEquals("default", query(databaseName, "SELECT default_value FROM authored_rows")
                .single().getValue("default_value"))
            assertLedger(
                databaseName,
                expectedOperations = listOf("insert"),
                expectedFields = listOf(listOf("field-default")),
            )
        } finally {
            client.close()
            context.deleteDatabase(databaseName)
        }
    }

    @Test
    fun supportColumnInjectionRemainsAbsentFromTheCapturedInsert() {
        val databaseName = databaseName()
        val client = clientWithSchema(databaseName)
        try {
            client.authoredWriteTransaction(
                tableName = authoredTable.tableName,
                operation = Operation.INSERT,
                columnNames = listOf("body"),
            ) { transaction ->
                transaction.execute(
                    "INSERT INTO authored_rows (id, body, support_value, updated_at) VALUES (?, ?, ?, ?)",
                    arrayOf("row-1", "authored", "runtime-support", "2026-01-01T00:00:00.000000Z"),
                )
            }

            assertEquals("runtime-support", query(databaseName, "SELECT support_value FROM authored_rows")
                .single().getValue("support_value"))
            assertLedger(
                databaseName,
                expectedOperations = listOf("insert"),
                expectedFields = listOf(listOf("field-body")),
            )
        } finally {
            client.close()
            context.deleteDatabase(databaseName)
        }
    }

    @Test
    fun updateCapturesOnlyChangedAuthoredColumns() {
        val databaseName = databaseName()
        val client = clientWithSchema(databaseName)
        try {
            client.authoredWriteTransaction(
                tableName = authoredTable.tableName,
                operation = Operation.INSERT,
                columnNames = listOf("body"),
            ) { transaction ->
                transaction.execute(
                    "INSERT INTO authored_rows (id, body, updated_at) VALUES (?, ?, ?)",
                    arrayOf("row-1", "before", "2026-01-01T00:00:00.000000Z"),
                )
            }
            client.authoredWriteTransaction(
                tableName = authoredTable.tableName,
                operation = Operation.UPDATE,
                columnNames = listOf("body"),
            ) { transaction ->
                transaction.execute(
                    "UPDATE authored_rows SET body = ?, support_value = ? WHERE id = ?",
                    arrayOf("after", "runtime-support", "row-1"),
                )
            }

            assertLedger(
                databaseName,
                expectedOperations = listOf("insert", "update"),
                expectedFields = listOf(listOf("field-body"), listOf("field-body")),
            )

            client.authoredWriteTransaction(
                tableName = authoredTable.tableName,
                operation = Operation.UPDATE,
                columnNames = listOf("body"),
            ) { transaction ->
                transaction.execute(
                    "UPDATE authored_rows SET support_value = ? WHERE id = ?",
                    arrayOf("another-support", "row-1"),
                )
            }
            assertLedger(
                databaseName,
                expectedOperations = listOf("insert", "update"),
                expectedFields = listOf(listOf("field-body"), listOf("field-body")),
            )
        } finally {
            client.close()
            context.deleteDatabase(databaseName)
        }
    }

    @Test
    fun insertWithoutAnAuthoredWritableFieldAborts() {
        val databaseName = databaseName()
        val client = clientWithSchema(databaseName)
        try {
            assertThrows(RuntimeException::class.java) {
                client.authoredWriteTransaction(
                    tableName = authoredTable.tableName,
                    operation = Operation.INSERT,
                    columnNames = listOf("id"),
                ) { transaction ->
                    transaction.execute(
                        "INSERT INTO authored_rows (id, updated_at) VALUES (?, ?)",
                        arrayOf("row-1", "2026-01-01T00:00:00.000000Z"),
                    )
                }
            }

            assertTrue(query(databaseName, "SELECT id FROM authored_rows").isEmpty())
            assertLedger(
                databaseName,
                expectedOperations = emptyList(),
                expectedFields = emptyList(),
            )
        } finally {
            client.close()
            context.deleteDatabase(databaseName)
        }
    }

    private fun clientWithSchema(databaseName: String): SynchroClient {
        val database = SynchroDatabase.open(context, databaseName)
        try {
            installTestSchema(
                database,
                schemaVersion = 1,
                schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
                tables = listOf(authoredTable),
            )
        } finally {
            database.close()
        }
        return SynchroClient(
            SynchroConfig(
                dbPath = databaseName,
                serverURL = "http://localhost:8080",
                authProvider = { "test-token" },
                clientID = "authored-capture-test",
                appVersion = "1.0.0",
            ),
            context,
        )
    }

    private fun query(databaseName: String, sql: String): List<Row> {
        val database = SynchroDatabase.open(context, databaseName)
        return try {
            database.query(sql)
        } finally {
            database.close()
        }
    }

    private fun assertLedger(
        databaseName: String,
        expectedOperations: List<String>,
        expectedFields: List<List<String>>,
    ) {
        val database = SynchroDatabase.open(context, databaseName)
        try {
            val ledger = database.query(
                "SELECT mutation_id, operation FROM _synchro_pending_changes ORDER BY local_order",
            )
            assertEquals(expectedOperations, ledger.map { it.getValue("operation") })
            assertEquals(
                expectedFields,
                ledger.map { mutation ->
                    database.query(
                        "SELECT field_id FROM _synchro_mutation_values WHERE mutation_id = ? ORDER BY field_id",
                        arrayOf(mutation.getValue("mutation_id")),
                    ).map { it.getValue("field_id") }
                },
            )
            assertEquals(0L, database.queryOne("SELECT COUNT(*) AS count FROM _synchro_capture_context")?.get("count"))
            assertEquals(0L, database.queryOne("SELECT COUNT(*) AS count FROM _synchro_capture_fields")?.get("count"))
        } finally {
            database.close()
        }
    }

    private fun databaseName(): String = "synchro_authored_capture_${UUID.randomUUID()}.sqlite"

    private companion object {
        val authoredTable = LocalSchemaTable(
            tableID = "table-authored-rows",
            relationID = "relation-authored-rows",
            tableName = "authored_rows",
            primaryKeyFieldID = "field-id",
            updatedAtColumn = "updated_at",
            deletedAtColumn = "deleted_at",
            primaryKey = listOf("id"),
            columns = listOf(
                LocalSchemaColumn(
                    fieldID = "field-id",
                    name = "id",
                    logicalType = "string",
                    nullable = false,
                    writable = false,
                    isPrimaryKey = true,
                ),
                LocalSchemaColumn(
                    fieldID = "field-body",
                    name = "body",
                    logicalType = "string",
                    nullable = true,
                    writable = true,
                    isPrimaryKey = false,
                ),
                LocalSchemaColumn(
                    fieldID = "field-default",
                    name = "default_value",
                    logicalType = "string",
                    nullable = false,
                    writable = true,
                    sqliteDefaultSQL = "'default'",
                    isPrimaryKey = false,
                ),
                LocalSchemaColumn(
                    fieldID = "field-support",
                    name = "support_value",
                    logicalType = "string",
                    nullable = false,
                    writable = true,
                    sqliteDefaultSQL = "''",
                    isPrimaryKey = false,
                ),
                LocalSchemaColumn(
                    fieldID = "field-updated-at",
                    name = "updated_at",
                    logicalType = "datetime",
                    nullable = false,
                    writable = false,
                    isPrimaryKey = false,
                ),
                LocalSchemaColumn(
                    fieldID = "field-deleted-at",
                    name = "deleted_at",
                    logicalType = "datetime",
                    nullable = true,
                    writable = false,
                    isPrimaryKey = false,
                ),
            ),
        )
    }
}
