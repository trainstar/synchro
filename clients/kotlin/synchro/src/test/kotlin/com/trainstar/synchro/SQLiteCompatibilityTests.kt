package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [24])
class SQLiteCompatibilityTests {
    private val databases = TestDatabaseTracker()

    @After
    fun tearDown() {
        databases.closeAll()
    }

    @Test
    fun emittedClientSQLExecutesOnSupportedSQLite() {
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

        database.writeTransaction { db ->
            db.execSQL("CREATE TABLE portable_upsert (id TEXT PRIMARY KEY, title TEXT)")
            assertEquals(
                1,
                executeUpsert(
                    db,
                    "portable_upsert",
                    listOf("id"),
                    listOf("order-1"),
                    listOf("title"),
                    listOf("first title"),
                ),
            )
            assertEquals(
                1,
                executeUpsert(
                    db,
                    "portable_upsert",
                    listOf("id"),
                    listOf("order-1"),
                    listOf("title"),
                    listOf("updated title"),
                ),
            )
            db.execSQL("CREATE TABLE portable_key_only (id TEXT PRIMARY KEY)")
            assertEquals(
                1,
                executeUpsert(
                    db,
                    "portable_key_only",
                    listOf("id"),
                    listOf("key-1"),
                    emptyList(),
                    emptyList(),
                ),
            )
            assertEquals(
                0,
                executeUpsert(
                    db,
                    "portable_key_only",
                    listOf("id"),
                    listOf("key-1"),
                    emptyList(),
                    emptyList(),
                ),
            )
        }

        assertEquals(
            "updated title",
            database.queryOne("SELECT title FROM portable_upsert WHERE id = ?", arrayOf("order-1"))?.get("title"),
        )
        assertEquals(1L, database.queryOne("SELECT COUNT(*) AS count FROM portable_key_only")?.get("count"))
    }
}
