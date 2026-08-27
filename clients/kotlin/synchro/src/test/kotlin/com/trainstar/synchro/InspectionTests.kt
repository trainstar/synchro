package com.trainstar.synchro

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import com.trainstar.synchro.inspection.RebuildAttemptInspection
import com.trainstar.synchro.inspection.RebuildReceiptInspection
import com.trainstar.synchro.inspection.ScopeInspection
import com.trainstar.synchro.inspection.ScopeRowInspection
import com.trainstar.synchro.inspection.TransportObservationCollector
import java.util.UUID
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertSame
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [28])
class InspectionTests {
    private data class RebuildReceiptProof(
        val rebuildIDFingerprint: String,
        val pageCount: Int,
        val returnedRecordCount: Int,
        val requestChainValid: Boolean,
        val recordsInCanonicalOrder: Boolean,
        val rowChecksumsValid: Boolean,
        val scopeChecksumValid: Boolean,
        val finalChecksumMatchesLocal: Boolean,
    )

    private fun rebuildReceiptProof(value: RebuildReceiptInspection): RebuildReceiptProof =
        RebuildReceiptProof(
            rebuildIDFingerprint = value.rebuildIDFingerprint,
            pageCount = value.pageCount,
            returnedRecordCount = value.returnedRecordCount,
            requestChainValid = value.requestChainExpected == value.requestChainObserved,
            recordsInCanonicalOrder = value.recordIdentitiesHex.size == value.recordIdentitiesHex.toSet().size &&
                value.recordIdentitiesHex == value.recordIdentitiesHex.sorted(),
            rowChecksumsValid = value.receivedRowChecksums == value.computedRowChecksums,
            scopeChecksumValid = value.computedScopeChecksum != null && value.computedScopeChecksum == value.finalScopeChecksum,
            finalChecksumMatchesLocal = value.finalScopeChecksum != null &&
                value.finalScopeChecksum == value.storedScopeChecksum &&
                value.finalScopeChecksum == value.localScopeChecksum,
        )

    private val context = ApplicationProvider.getApplicationContext<Context>()
    private val wireJSON = Json { encodeDefaults = true }

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

    @Test
    fun aggregateClientStateInspectionReturnsBoundedDurableState() {
        val config = prepareClientConfig()
        val rebuildID = "00000000-0000-4000-8000-000000000001"
        withInternalDatabase(config) { database ->
            database.writeTransaction { db ->
                SynchroMeta.upsertScope(
                    db,
                    scopeId = "orders:user-1",
                    cursor = "cursor-1",
                    checksum = "scope-checksum",
                    generation = 3,
                    localChecksum = "local-checksum",
                )
                SynchroMeta.upsertScopeRow(
                    db,
                    scopeId = "orders:user-1",
                    tableName = "orders",
                    recordId = "order-1",
                    checksum = "row-checksum",
                    generation = 3,
                )
                SynchroMeta.upsertRebuildAttempt(
                    db,
                    LocalRebuildAttempt(
                        scopeID = "orders:user-1",
                        rebuildID = rebuildID,
                        clientGeneration = 4,
                        schemaVersion = 1,
                        schemaHash = PROTOCOL_TEST_SCHEMA_HASH,
                        generation = 3,
                        cursor = "rebuild-page-2",
                        pageLimit = 100,
                    ),
                )
            }
        }

        val client = SynchroClient(config, context)
        try {
            val inspection = client.inspectClientState(limit = 1)

            assertEquals(SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH), inspection.schema)
            assertEquals(
                listOf(
                    ScopeInspection(
                        scopeID = "orders:user-1",
                        cursor = "cursor-1",
                        checksum = "scope-checksum",
                        generation = 3,
                        localChecksum = "local-checksum",
                    ),
                ),
                inspection.scopeStates,
            )
            assertEquals(
                listOf(ScopeRowInspection("orders:user-1", "orders", "order-1", "row-checksum", 3)),
                inspection.scopeRows,
            )
            assertEquals(
                listOf(
                    RebuildAttemptInspection(
                        scopeID = "orders:user-1",
                        rebuildID = rebuildID,
                        clientGeneration = 4,
                        schema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
                        generation = 3,
                        cursor = "rebuild-page-2",
                        pageLimit = 100,
                    ),
                ),
                inspection.rebuildAttempts,
            )
            assertEquals(0L, inspection.provenanceMaintenanceWorkCursor)
            val counts = client.inspectClientStateCounts()
            assertEquals(inspection.schema, counts.schema)
            assertEquals(1, counts.applicationRowCount)
            assertEquals(0, counts.mutationLedgerCount)
            assertEquals(0, counts.mutationOutcomeCount)
            assertEquals(0, counts.sealedBatchCount)
            assertEquals(0, counts.rejectedMutationCount)
            assertEquals(1, counts.scopeStateCount)
            assertEquals(1, counts.scopeRowCount)
            assertEquals(1, counts.provenanceCount)
            assertEquals(0, counts.rowMetadataCount)
            assertEquals(1, counts.rebuildAttemptCount)
            assertEquals(0, counts.rebuildReceiptCount)
            assertEquals(0L, counts.provenanceMaintenanceWorkCursor)
            assertEquals(inspection.schema, client.inspectSchema().currentSchema)
            assertEquals(inspection.scopeStates, client.inspectScopes(limit = 1))
            assertEquals(inspection.scopeRows, client.inspectScopeRows(limit = 1))
            assertEquals(inspection.rebuildAttempts, client.inspectRebuildState(limit = 1).attempts)
        } finally {
            client.close()
            context.deleteDatabase(config.dbPath)
        }
    }

    @Test
    fun rebuildReceiptProofAcceptsValidEmptyTerminalReceipt() {
        val fixture = makeRebuildReceiptFixture(recordCount = 0)
        try {
            val proof = fixture.client.inspectRebuildReceipts().single().let(::rebuildReceiptProof)

            assertEquals(TransportObservationCollector.cursorFingerprint(PROOF_REBUILD_ID), proof.rebuildIDFingerprint)
            assertEquals(1, proof.pageCount)
            assertEquals(0, proof.returnedRecordCount)
            assertTrue(proof.requestChainValid)
            assertTrue(proof.recordsInCanonicalOrder)
            assertTrue(proof.rowChecksumsValid)
            assertTrue(proof.scopeChecksumValid)
            assertTrue(proof.finalChecksumMatchesLocal)
        } finally {
            closeRebuildReceiptFixture(fixture)
        }
    }

    @Test
    fun rebuildReceiptProofAcceptsValidTwoPageReceipts() {
        val fixture = makeRebuildReceiptFixture(recordCount = 3)
        try {
            val proof = fixture.client.inspectRebuildReceipts().single().let(::rebuildReceiptProof)

            assertEquals(TransportObservationCollector.cursorFingerprint(PROOF_REBUILD_ID), proof.rebuildIDFingerprint)
            assertEquals(2, proof.pageCount)
            assertEquals(3, proof.returnedRecordCount)
            assertTrue(proof.requestChainValid)
            assertTrue(proof.recordsInCanonicalOrder)
            assertTrue(proof.rowChecksumsValid)
            assertTrue(proof.scopeChecksumValid)
            assertTrue(proof.finalChecksumMatchesLocal)
        } finally {
            closeRebuildReceiptFixture(fixture)
        }
    }

    @Test
    fun rebuildReceiptProofRejectsUnknownResponseMember() {
        val fixture = makeRebuildReceiptFixture(recordCount = 3)
        try {
            updateReceiptSource(fixture.config, null) { source ->
                source.removeSuffix("}") + ",\"unexpected\":true}"
            }

            assertThrows(SynchroError.InvalidResponse::class.java) {
                fixture.client.inspectRebuildReceipts()
            }
        } finally {
            closeRebuildReceiptFixture(fixture)
        }
    }

    @Test
    fun rebuildReceiptProofDetectsBrokenCursorChain() {
        val fixture = makeRebuildReceiptFixture(recordCount = 3)
        try {
            updateReceiptResponse(fixture.config, null) { it.copy(cursor = "unconsumed") }

            val proof = fixture.client.inspectRebuildReceipts().single().let(::rebuildReceiptProof)
            assertFalse(proof.requestChainValid)
            assertTrue(proof.recordsInCanonicalOrder)
            assertTrue(proof.rowChecksumsValid)
        } finally {
            closeRebuildReceiptFixture(fixture)
        }
    }

    @Test
    fun rebuildReceiptProofDetectsNoncanonicalRecordOrderIndependently() {
        val fixture = makeRebuildReceiptFixture(recordCount = 3)
        try {
            updateReceiptResponse(fixture.config, null) { response ->
                response.copy(records = response.records.reversed())
            }

            val proof = fixture.client.inspectRebuildReceipts().single().let(::rebuildReceiptProof)
            assertTrue(proof.requestChainValid)
            assertFalse(proof.recordsInCanonicalOrder)
            assertTrue(proof.rowChecksumsValid)
            assertTrue(proof.scopeChecksumValid)
            assertTrue(proof.finalChecksumMatchesLocal)
        } finally {
            closeRebuildReceiptFixture(fixture)
        }
    }

    @Test
    fun rebuildReceiptProofDetectsWrongRowChecksumIndependently() {
        val fixture = makeRebuildReceiptFixture(recordCount = 3)
        try {
            updateReceiptResponse(fixture.config, null) { response ->
                response.copy(
                    records = response.records.toMutableList().also { records ->
                        records[0] = records[0].copy(
                            rowChecksum = records[0].rowChecksum.copy(digest = "f".repeat(64)),
                        )
                    },
                )
            }

            val proof = fixture.client.inspectRebuildReceipts().single().let(::rebuildReceiptProof)
            assertTrue(proof.requestChainValid)
            assertTrue(proof.recordsInCanonicalOrder)
            assertFalse(proof.rowChecksumsValid)
            assertTrue(proof.scopeChecksumValid)
            assertTrue(proof.finalChecksumMatchesLocal)
        } finally {
            closeRebuildReceiptFixture(fixture)
        }
    }

    @Test
    fun rebuildReceiptProofDetectsWrongScopeChecksumIndependently() {
        val fixture = makeRebuildReceiptFixture(recordCount = 3)
        val forged = ChecksumObject("sha256", 1, "hex", "e".repeat(64))
        try {
            updateReceiptResponse(fixture.config, PROOF_SECOND_CURSOR) { response ->
                response.copy(checksum = forged)
            }
            withInternalDatabase(fixture.config) { database ->
                database.writeTransaction { db ->
                    db.execSQL(
                        """
                        UPDATE _synchro_rebuild_page_receipts
                        SET final_checksum = ?
                        WHERE scope_id = ? AND rebuild_id = ? AND request_cursor_is_null = 0 AND request_cursor = ?
                        """.trimIndent(),
                        arrayOf(wireJSON.encodeToString(forged), PROOF_SCOPE_ID, PROOF_REBUILD_ID, PROOF_SECOND_CURSOR),
                    )
                }
            }

            val proof = fixture.client.inspectRebuildReceipts().single().let(::rebuildReceiptProof)
            assertTrue(proof.requestChainValid)
            assertTrue(proof.recordsInCanonicalOrder)
            assertTrue(proof.rowChecksumsValid)
            assertFalse(proof.scopeChecksumValid)
            assertFalse(proof.finalChecksumMatchesLocal)
        } finally {
            closeRebuildReceiptFixture(fixture)
        }
    }

    private fun prepareClientConfig(): SynchroConfig {
        val dbPath = "synchro_inspection_${UUID.randomUUID()}.sqlite"
        val database = SynchroDatabase.open(context, dbPath)
        try {
            installTestSchema(
                database,
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

    private data class RebuildReceiptFixture(
        val client: SynchroClient,
        val config: SynchroConfig,
    )

    private fun makeRebuildReceiptFixture(recordCount: Int): RebuildReceiptFixture {
        require(recordCount == 0 || recordCount == 3)
        val config = prepareClientConfig()
        val localTable = table.localSchema
        val recordsWithDigests = (1..recordCount).map { index ->
            val id = "o$index"
            val serverVersion = "2026-01-01T00:00:0$index.000000Z"
            val pk = buildJsonObject { put("id", id) }
            val row = buildJsonObject {
                put("id", id)
                put("title", "title-$id")
                put("updated_at", serverVersion)
                put("deleted_at", JsonNull)
            }
            val digest = Integrity.rowDigest(PROTOCOL_TEST_SCHEMA_HASH, localTable, pk, row, serverVersion)
            RebuildRecord(
                table = localTable.tableID,
                pk = pk,
                row = row,
                rowChecksum = digest.checksum,
                serverVersion = serverVersion,
            ) to digest
        }.sortedWith { left, right -> compareUnsigned(left.second.identity, right.second.identity) }
        val records = recordsWithDigests.map { it.first }
        val finalChecksum = Integrity.scopeDigest(
            PROTOCOL_TEST_SCHEMA_HASH,
            PROOF_SCOPE_ID,
            recordsWithDigests.map { it.second.identity to it.second.checksum },
        )
        withInternalDatabase(config) { database ->
            database.writeTransaction { db ->
                SynchroMeta.upsertScope(
                    db,
                    scopeId = PROOF_SCOPE_ID,
                    cursor = PROOF_FINAL_CURSOR,
                    checksum = wireJSON.encodeToString(finalChecksum),
                    generation = 1,
                    localChecksum = wireJSON.encodeToString(finalChecksum),
                )
                if (recordCount == 0) {
                    insertRebuildReceipt(
                        db = db,
                        requestCursor = null,
                        records = emptyList(),
                        responseCursor = null,
                        hasMore = false,
                        finalChecksum = finalChecksum,
                    )
                } else {
                    insertRebuildReceipt(
                        db = db,
                        requestCursor = null,
                        records = records.take(2),
                        responseCursor = PROOF_SECOND_CURSOR,
                        hasMore = true,
                        finalChecksum = null,
                    )
                    insertRebuildReceipt(
                        db = db,
                        requestCursor = PROOF_SECOND_CURSOR,
                        records = records.drop(2),
                        responseCursor = null,
                        hasMore = false,
                        finalChecksum = finalChecksum,
                    )
                }
            }
        }
        return RebuildReceiptFixture(SynchroClient(config, context), config)
    }

    private fun insertRebuildReceipt(
        db: android.database.sqlite.SQLiteDatabase,
        requestCursor: String?,
        records: List<RebuildRecord>,
        responseCursor: String?,
        hasMore: Boolean,
        finalChecksum: ChecksumObject?,
    ) {
        val request = RebuildRequest(
            clientID = "inspection-device",
            clientGeneration = 1,
            schema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
            scope = PROOF_SCOPE_ID,
            rebuildID = PROOF_REBUILD_ID,
            cursor = requestCursor,
            limit = 2,
        )
        val response = RebuildResponse(
            scope = PROOF_SCOPE_ID,
            records = records,
            cursor = responseCursor,
            hasMore = hasMore,
            finalScopeCursor = if (hasMore) null else PROOF_FINAL_CURSOR,
            checksum = finalChecksum,
        )
        SynchroMeta.insertRebuildPageReceipt(
            db = db,
            scopeId = PROOF_SCOPE_ID,
            rebuildId = PROOF_REBUILD_ID,
            requestCursor = requestCursor,
            requestJSON = wireJSON.encodeToString(request),
            responseJSON = wireJSON.encodeToString(response),
            finalScopeCursor = response.finalScopeCursor,
            finalChecksumJSON = finalChecksum?.let(wireJSON::encodeToString),
        )
    }

    private fun updateReceiptResponse(
        config: SynchroConfig,
        requestCursor: String?,
        update: (RebuildResponse) -> RebuildResponse,
    ) {
        updateReceiptSource(config, requestCursor) { source ->
            wireJSON.encodeToString(update(wireJSON.decodeFromString<RebuildResponse>(source)))
        }
    }

    private fun updateReceiptSource(
        config: SynchroConfig,
        requestCursor: String?,
        update: (String) -> String,
    ) {
        withInternalDatabase(config) { database ->
            database.writeTransaction { db ->
                val source = db.rawQuery(
                    """
                    SELECT response_json FROM _synchro_rebuild_page_receipts
                    WHERE scope_id = ? AND rebuild_id = ? AND request_cursor_is_null = ? AND request_cursor = ?
                    """.trimIndent(),
                    arrayOf(
                        PROOF_SCOPE_ID,
                        PROOF_REBUILD_ID,
                        if (requestCursor == null) "1" else "0",
                        requestCursor ?: "",
                    ),
                ).use { cursor ->
                    check(cursor.moveToFirst())
                    cursor.getString(0)
                }
                db.execSQL(
                    """
                    UPDATE _synchro_rebuild_page_receipts SET response_json = ?
                    WHERE scope_id = ? AND rebuild_id = ? AND request_cursor_is_null = ? AND request_cursor = ?
                    """.trimIndent(),
                    arrayOf(
                        update(source),
                        PROOF_SCOPE_ID,
                        PROOF_REBUILD_ID,
                        if (requestCursor == null) 1 else 0,
                        requestCursor ?: "",
                    ),
                )
            }
        }
    }

    private fun closeRebuildReceiptFixture(fixture: RebuildReceiptFixture) {
        fixture.client.close()
        context.deleteDatabase(fixture.config.dbPath)
    }

    private fun compareUnsigned(left: ByteArray, right: ByteArray): Int {
        for (index in 0 until minOf(left.size, right.size)) {
            val difference = (left[index].toInt() and 0xff) - (right[index].toInt() and 0xff)
            if (difference != 0) return difference
        }
        return left.size - right.size
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

    private companion object {
        const val PROOF_SCOPE_ID = "proof-scope"
        const val PROOF_REBUILD_ID = "proof-rebuild"
        const val PROOF_SECOND_CURSOR = "page-2"
        const val PROOF_FINAL_CURSOR = "scope-final"
    }
}
