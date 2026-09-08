package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import org.junit.After
import org.junit.Assert.assertFalse
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28])
class SQLiteCompatibilityTests {
    private val databases = TestDatabaseTracker()

    @After
    fun tearDown() {
        databases.closeAll()
    }

    @Test
    fun emittedClientSQLUsesSQLite392Syntax() {
        val database = databases.create(ApplicationProvider.getApplicationContext<Context>())
        val table = LocalSchemaTable(
            tableID = "orders",
            relationID = "orders",
            tableName = "orders",
            primaryKeyFieldID = "id",
            updatedAtColumn = "",
            deletedAtColumn = "",
            primaryKey = listOf("id"),
            columns = listOf(
                LocalSchemaColumn("id", "id", "string", false, false, isPrimaryKey = true),
                LocalSchemaColumn("title", "title", "string", false, true, isPrimaryKey = false),
            ),
        )
        installTestSchema(database, 1, "schema-hash", listOf(table))

        val storedDDL = database.query("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL")
            .map { it.getValue("sql") as String }
        val upsertSQL = listOf(
            portableUpsertStatements("orders", listOf("id"), listOf("title")),
            portableUpsertStatements("_synchro_scope_rows", listOf("scope_id", "table_name", "record_id"), emptyList()),
        ).flatMap { listOfNotNull(it.update, it.insert) }

        (storedDDL + upsertSQL).forEach(::assertSQLite392Compatible)
    }

    private fun assertSQLite392Compatible(sql: String) {
        val upper = sql.uppercase()
        val unsupported = listOf(
            "ON CONFLICT",
            " RETURNING ",
            " GENERATED ALWAYS ",
            " STRICT",
            " DROP COLUMN ",
            " RENAME COLUMN ",
            " UPDATE FROM ",
            " RIGHT JOIN ",
            " FULL OUTER JOIN ",
            " MATERIALIZED ",
            " NOT MATERIALIZED ",
            " NULLS FIRST",
            " NULLS LAST",
            " FILTER (",
            " OVER (",
        )
        unsupported.forEach { feature ->
            assertFalse("SQLite 3.9.2 does not support $feature in $sql", upper.contains(feature))
        }
    }
}
