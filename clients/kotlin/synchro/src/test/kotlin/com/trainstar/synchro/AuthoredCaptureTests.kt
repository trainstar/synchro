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
    fun ordinaryWritesCaptureOnlyStatementColumns() {
        val databaseName = databaseName()
        val client = clientWithSchema(databaseName)
        try {
            client.execute(
                "INSERT INTO authored_rows (id, body, updated_at) VALUES (?, ?, ?)",
                arrayOf("row-1", "before", "2026-01-01T00:00:00.000000Z"),
            )
            client.execute(
                "UPDATE authored_rows SET support_value = ?, updated_at = ? WHERE id = ?",
                arrayOf("runtime-support", "2026-01-02T00:00:00.000000Z", "row-1"),
            )

            assertLedger(
                databaseName,
                expectedOperations = listOf("insert", "update"),
                expectedFields = listOf(listOf("field-body"), listOf("field-support")),
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

    @Test
    fun authoredContextRejectsAnotherTableBeforeChangingRows() {
        val databaseName = databaseName()
        val otherTable = authoredTable.copy(
            tableID = "table-other",
            relationID = "relation-other",
            tableName = "other_rows",
            primaryKeyFieldID = "other-field-id",
            columns = authoredTable.columns.map { it.copy(fieldID = "other-${it.fieldID}") },
        )
        val client = clientWithSchema(databaseName, listOf(authoredTable, otherTable))
        try {
            for (table in listOf("authored_rows", "other_rows")) {
                client.execute(
                    "INSERT INTO $table (id, body, updated_at) VALUES ('row-1', 'before', '2026-01-01T00:00:00.000000Z')",
                )
            }
            client.authoredWriteTransaction("authored_rows", Operation.UPDATE, listOf("body")) { transaction ->
                assertThrows(IllegalArgumentException::class.java) {
                    transaction.execute("UPDATE other_rows SET body = 'lost' WHERE id = 'row-1'")
                }
                assertEquals("before", transaction.queryOne("SELECT body FROM other_rows")?.get("body"))
                transaction.execute("UPDATE authored_rows SET body = 'captured' WHERE id = 'row-1'")
            }
            client.execute("UPDATE other_rows SET body = 'also captured' WHERE id = 'row-1'")
            assertLedger(
                databaseName,
                listOf("insert", "insert", "update", "update"),
                listOf(listOf("field-body"), listOf("other-field-body"), listOf("field-body"), listOf("other-field-body")),
            )
        } finally {
            client.close()
            context.deleteDatabase(databaseName)
        }
    }

    @Test
    fun authoredContextRejectsEveryDifferentOperationBeforeSql() {
        val databaseName = databaseName()
        val client = clientWithSchema(databaseName)
        try {
            client.execute(
                "INSERT INTO authored_rows (id, body, updated_at) VALUES ('row-1', 'before', '2026-01-01T00:00:00.000000Z')",
            )
            val statements = mapOf(
                Operation.INSERT to
                    "INSERT INTO authored_rows (id, body, updated_at) VALUES ('row-2', 'lost', '2026-01-01T00:00:00.000000Z')",
                Operation.UPDATE to "UPDATE authored_rows SET body = 'lost' WHERE id = 'row-1'",
                Operation.DELETE to "DELETE FROM authored_rows WHERE id = 'row-1'",
            )
            for (authoredOperation in statements.keys) {
                client.authoredWriteTransaction("authored_rows", authoredOperation, listOf("body")) { transaction ->
                    for ((operation, sql) in statements) {
                        if (operation != authoredOperation) {
                            assertThrows(IllegalArgumentException::class.java) { transaction.execute(sql) }
                        }
                    }
                    assertEquals(listOf("before"), transaction.query("SELECT body FROM authored_rows").map { it["body"] })
                    assertEquals(null, transaction.queryOne("SELECT deleted_at FROM authored_rows")?.get("deleted_at"))
                }
            }
            client.authoredWriteTransaction("authored_rows", Operation.DELETE, emptyList()) { transaction ->
                transaction.execute("DELETE FROM authored_rows WHERE id = 'row-1'")
            }
            assertTrue(client.queryOne("SELECT deleted_at FROM authored_rows")?.get("deleted_at") is String)
            assertLedger(databaseName, listOf("insert", "delete"), listOf(listOf("field-body"), emptyList()))
        } finally {
            client.close()
            context.deleteDatabase(databaseName)
        }
    }

    @Test
    fun authoredIdentifiersUseSchemaSpellingWithoutCapturingSupportColumns() {
        val databaseName = databaseName()
        val client = clientWithSchema(databaseName)
        try {
            client.authoredWriteTransaction("AUTHORED_ROWS", "INSERT", listOf("BODY")) { transaction ->
                transaction.execute(
                    "INSERT INTO \"Authored_Rows\" (id, body, support_value, updated_at) " +
                        "VALUES ('row-1', 'authored', 'support', '2026-01-01T00:00:00.000000Z')",
                )
            }
            assertEquals("support", client.queryOne("SELECT support_value FROM authored_rows")?.get("support_value"))
            assertLedger(databaseName, listOf("insert"), listOf(listOf("field-body")))
        } finally {
            client.close()
            context.deleteDatabase(databaseName)
        }
    }

    @Test
    fun authoredContextKeepsNonAsciiTableNamesDistinct() {
        val databaseName = databaseName()
        val lower = authoredTable.copy(tableName = "\u00e9_rows")
        val upper = authoredTable.copy(
            tableID = "table-other",
            relationID = "relation-other",
            tableName = "\u00c9_rows",
            primaryKeyFieldID = "other-field-id",
            columns = authoredTable.columns.map { it.copy(fieldID = "other-${it.fieldID}") },
        )
        val client = clientWithSchema(databaseName, listOf(lower, upper))
        try {
            for (table in listOf(lower, upper)) {
                client.authoredWriteTransaction(table.tableName, Operation.INSERT, listOf("body")) { transaction ->
                    transaction.execute(
                        "INSERT INTO \"${table.tableName}\" (id, body, updated_at) " +
                            "VALUES ('row-1', 'before', '2026-01-01T00:00:00.000000Z')",
                    )
                }
            }
            client.authoredWriteTransaction(lower.tableName, Operation.UPDATE, listOf("body")) { transaction ->
                assertThrows(IllegalArgumentException::class.java) {
                    transaction.execute("UPDATE \"${upper.tableName}\" SET body = 'lost' WHERE id = 'row-1'")
                }
                assertEquals("before", transaction.queryOne("SELECT body FROM \"${upper.tableName}\"")?.get("body"))
                transaction.execute("UPDATE \"${lower.tableName}\" SET body = 'captured' WHERE id = 'row-1'")
            }
            client.execute("UPDATE \"${upper.tableName}\" SET body = 'also captured' WHERE id = 'row-1'")
            assertLedger(
                databaseName,
                listOf("insert", "insert", "update", "update"),
                listOf(listOf("field-body"), listOf("other-field-body"), listOf("field-body"), listOf("other-field-body")),
            )
        } finally {
            client.close()
            context.deleteDatabase(databaseName)
        }
    }

    private fun clientWithSchema(
        databaseName: String,
        tables: List<LocalSchemaTable> = listOf(authoredTable),
    ): SynchroClient {
        val database = SynchroDatabase.open(context, databaseName)
        try {
            installTestSchema(
                database,
                schemaVersion = 1,
                schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
                tables = tables,
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
