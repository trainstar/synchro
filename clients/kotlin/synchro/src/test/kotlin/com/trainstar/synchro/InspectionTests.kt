package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import java.util.UUID
import org.junit.Assert.assertEquals
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28])
class InspectionTests {
    private val context = ApplicationProvider.getApplicationContext<Context>()

    private val table = SchemaTable(
        tableName = "orders",
        updatedAtColumn = "updated_at",
        deletedAtColumn = "deleted_at",
        primaryKey = listOf("id"),
        columns = listOf(
            SchemaColumn("id", logicalType = "string", nullable = false, isPrimaryKey = true),
            SchemaColumn("title", logicalType = "string"),
            SchemaColumn("updated_at", logicalType = "datetime", nullable = false),
            SchemaColumn("deleted_at", logicalType = "datetime"),
        ),
    )

    @Test
    fun pendingInspectionSurvivesRestartAndUsesAuthoredValues() {
        val config = prepareClientConfig()
        val firstClient = SynchroClient(config, context)
        try {
            assertSame(SyncStatus.Uninitialized, firstClient.getSyncStatus())
            firstClient.execute(
                "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
                arrayOf("o1", "first authored", "2026-01-01T00:00:00.000000Z"),
            )
            firstClient.execute(
                "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
                arrayOf("o2", "second authored", "2026-01-01T00:00:01.000000Z"),
            )
        } finally {
            firstClient.close()
        }

        val rawDatabase = SynchroDatabase.open(context, config.dbPath)
        try {
            rawDatabase.writeTransaction { db ->
                db.execSQL(
                    "UPDATE _synchro_pending_changes SET lifecycle_state = 'superseded_before_send' WHERE record_id = 'o1'",
                )
                db.execSQL("UPDATE _synchro_meta SET value = '1' WHERE key = 'sync_lock'")
                db.execSQL("UPDATE orders SET title = 'current row value'")
                db.execSQL("UPDATE _synchro_meta SET value = '0' WHERE key = 'sync_lock'")
            }
        } finally {
            rawDatabase.close()
        }

        val restartedClient = SynchroClient(config, context)
        try {
            val inspections = restartedClient.inspectPendingMutations()
            assertEquals(listOf("o1", "o2"), inspections.map { it.recordID })
            assertEquals(inspections.map { it.localOrder }.sorted(), inspections.map { it.localOrder })
            assertEquals(LocalMutationStatus.SUPERSEDED_BEFORE_SEND, inspections[0].status)
            assertEquals(LocalMutationStatus.PENDING, inspections[1].status)
            assertEquals(Operation.INSERT, inspections[0].operation)
            assertEquals(SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH), inspections[0].authoredSchema)
            assertEquals(
                AnyCodable("first authored"),
                inspections[0].authoredFields.single { it.fieldID == "title" }.value,
            )
            assertEquals(
                AnyCodable("second authored"),
                inspections[1].authoredFields.single { it.fieldID == "title" }.value,
            )
            assertTrue(inspections.none { inspection ->
                inspection.authoredFields.any { it.value == AnyCodable("current row value") }
            })
        } finally {
            restartedClient.close()
            context.deleteDatabase(config.dbPath)
        }
    }

    @Test
    fun rejectedInspectionSurvivesRestartAndClearRetainsQueueIntent() {
        val config = prepareClientConfig()
        val mutationJSON = "{\"mutation_id\":\"m1\",\"columns\":{\"title\":\"authored\"}}"
        val rejectionJSON = "{\"mutation_id\":\"m1\",\"status\":\"rejected_terminal\",\"code\":\"policy_rejected\"}"
        lateinit var exactMutationJSON: String
        lateinit var exactRejectionJSON: String
        val firstClient = SynchroClient(config, context)
        var firstClientClosed = false
        try {
            firstClient.execute(
                "INSERT INTO orders (id, title, updated_at) VALUES (?, ?, ?)",
                arrayOf("o1", "authored", "2026-01-01T00:00:00.000000Z"),
            )
            val mutationID = firstClient.inspectPendingMutations().single().mutationID
            exactMutationJSON = mutationJSON.replace("m1", mutationID)
            exactRejectionJSON = rejectionJSON.replace("m1", mutationID)
            firstClient.close()
            firstClientClosed = true
            val rawDatabase = SynchroDatabase.open(context, config.dbPath)
            rawDatabase.writeTransaction { db ->
                db.execSQL(
                    "UPDATE _synchro_pending_changes SET lifecycle_state = 'rejected_terminal' WHERE mutation_id = ?",
                    arrayOf(mutationID),
                )
                SynchroMeta.upsertRejectedMutation(
                    db = db,
                    mutationID = mutationID,
                    tableName = "orders",
                    recordId = "o1",
                    status = "rejected_terminal",
                    code = "policy_rejected",
                    message = "not allowed",
                    serverRowJson = null,
                    serverVersion = null,
                    mutationJSON = exactMutationJSON,
                    rejectionJSON = exactRejectionJSON,
                )
            }
            rawDatabase.close()
        } finally {
            if (!firstClientClosed) firstClient.close()
        }

        val restartedClient = SynchroClient(config, context)
        try {
            val rejected = restartedClient.inspectRejectedMutations().single()
            assertEquals(MutationStatus.REJECTED_TERMINAL, rejected.status)
            assertEquals(MutationRejectionCode.POLICY_REJECTED, rejected.code)
            assertEquals("not allowed", rejected.message)
            assertEquals(exactMutationJSON, rejected.mutationJSON)
            assertEquals(exactRejectionJSON, rejected.rejectionJSON)
            val queueBeforeClear = internalQuery(config,
                "SELECT mutation_id, lifecycle_state FROM _synchro_pending_changes ORDER BY local_order",
            )
            val valuesBeforeClear = internalQuery(config,
                "SELECT mutation_id, field_id, logical_type, value_kind, value_text FROM _synchro_mutation_values ORDER BY mutation_id, field_id",
            )

            restartedClient.clearRejectedMutations()

            assertTrue(restartedClient.inspectRejectedMutations().isEmpty())
            assertEquals(queueBeforeClear, internalQuery(config,
                "SELECT mutation_id, lifecycle_state FROM _synchro_pending_changes ORDER BY local_order",
            ))
            assertEquals(valuesBeforeClear, internalQuery(config,
                "SELECT mutation_id, field_id, logical_type, value_kind, value_text FROM _synchro_mutation_values ORDER BY mutation_id, field_id",
            ))
        } finally {
            restartedClient.close()
        }

        val afterClearRestart = SynchroClient(config, context)
        try {
            assertTrue(afterClearRestart.inspectRejectedMutations().isEmpty())
            assertEquals("rejected_terminal", internalQueryOne(
                config,
                "SELECT lifecycle_state FROM _synchro_pending_changes",
            )?.get("lifecycle_state"))
            assertTrue(internalQuery(config, "SELECT field_id FROM _synchro_mutation_values").isNotEmpty())
        } finally {
            afterClearRestart.close()
            context.deleteDatabase(config.dbPath)
        }
    }

    private fun prepareClientConfig(): SynchroConfig {
        val dbPath = "synchro_inspection_${UUID.randomUUID()}.sqlite"
        val database = SynchroDatabase.open(context, dbPath)
        try {
            SchemaManager(database).createSyncedTables(
                SchemaResponse(
                    1,
                    PROTOCOL_TEST_SCHEMA_HASH,
                    "2026-01-01T00:00:00.000000Z",
                    listOf(table),
                ),
            )
        } finally {
            database.close()
        }
        return SynchroConfig(
            dbPath = dbPath,
            serverURL = "http://localhost:8080",
            authProvider = { "test-token" },
            clientID = "inspection-device",
            appVersion = "1.0.0",
        )
    }

    private fun internalQuery(config: SynchroConfig, sql: String): List<Row> =
        withInternalDatabase(config) { database -> database.query(sql) }

    private fun internalQueryOne(config: SynchroConfig, sql: String): Row? =
        withInternalDatabase(config) { database -> database.queryOne(sql) }

    private fun <T> withInternalDatabase(config: SynchroConfig, block: (SynchroDatabase) -> T): T {
        val database = SynchroDatabase.open(context, config.dbPath)
        return try {
            block(database)
        } finally {
            database.close()
        }
    }
}
