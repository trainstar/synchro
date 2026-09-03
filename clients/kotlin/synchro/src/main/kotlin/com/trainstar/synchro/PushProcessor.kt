package com.trainstar.synchro

import android.database.sqlite.SQLiteDatabase
import android.util.Base64
import java.util.UUID
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/** A sealed request needs a new binding after reconnect. */
internal class PushRenewalRequiredException : Exception("sealed push batch requires renewal")

internal class PushProcessor(
    private val database: SynchroDatabase,
    private val changeTracker: ChangeTracker,
) {
    private data class SealedPushBatch(
        val request: PushRequest,
        val requestJSON: String,
        val members: List<PendingChange>,
        val historicalTables: List<LocalSchemaTable>,
    )

    @OptIn(ExperimentalSerializationApi::class)
    private val json = Json {
        ignoreUnknownKeys = false
        encodeDefaults = true
        explicitNulls = false
    }

    data class PushOutcome(
        val response: PushResponse,
        val conflicts: List<ConflictEvent>,
    )

    suspend fun processPush(
        httpClient: HttpClient,
        clientID: String,
        clientGeneration: Long,
        schemaVersion: Long,
        schemaHash: String,
        syncedTables: List<LocalSchemaTable>,
        batchSize: Int = 100,
        expectedBatchID: String? = null,
    ): PushOutcome? {
        val batch = loadOrSealBatch(
            clientID,
            clientGeneration,
            schemaVersion,
            schemaHash,
            syncedTables,
            batchSize,
        ) ?: return resolveMissingExpectedBatch(expectedBatchID)
        if (expectedBatchID != null && batch.request.batchID != expectedBatchID) {
            throw SynchroError.InvalidResponse("durable push retry identity does not match the sealed batch")
        }
        val network = try {
            httpClient.pushSealedWithBody(batch.requestJSON, batch.request.batchID)
        } catch (_: ClientGenerationExpiredException) {
            markBatchRenewalRequired(batch.request.batchID)
            throw PushRenewalRequiredException()
        } catch (_: SynchroError.SchemaMismatch) {
            markBatchRenewalRequired(batch.request.batchID)
            throw PushRenewalRequiredException()
        }
        val response = network.response
        response.validate(batch.request)

        val sent = batch.members.associateBy { it.mutationID }
        val acceptedJSON = exactObjectJSONMap(network.bodyJSON, "accepted") { source ->
            json.decodeFromString<AcceptedMutation>(source).mutationID
        }
        val rejectedJSON = exactObjectJSONMap(network.bodyJSON, "rejected") { source ->
            json.decodeFromString<RejectedMutation>(source).mutationID
        }
        val mutationJSON = exactObjectJSONMap(batch.requestJSON, "mutations") { source ->
            json.decodeFromString<Mutation>(source).mutationID
        }
        val reconciliation = database.writeSyncLockedTransaction { db ->
            val conflicts = applyAcceptedInTransaction(
                db,
                response.accepted,
                syncedTables,
                sent,
                acceptedJSON,
                mapOf(batch.request.schema to batch.historicalTables),
            )
            val rejected = applyRejectedOutcomeInTransaction(
                db,
                response.rejected,
                syncedTables,
                sent,
                rejectedJSON,
                mutationJSON,
                mapOf(batch.request.schema to batch.historicalTables),
            )
            completeBatchInTransaction(db, batch.request.batchID)
            DurableBackoffStore.clearMatching(
                db,
                RetryOperation.PUSHING,
                batch.request.batchID,
            )
            conflicts + rejected.conflicts
        }
        return PushOutcome(response, reconciliation)
    }

    private fun resolveMissingExpectedBatch(expectedBatchID: String?): PushOutcome? {
        if (expectedBatchID == null) return null
        database.writeTransaction { db ->
            val state = db.rawQuery(
                "SELECT state FROM _synchro_push_batches WHERE batch_id = ?",
                arrayOf(expectedBatchID),
            ).use { cursor -> if (cursor.moveToFirst()) cursor.getString(0) else null }
            if (state !in setOf("completed", "superseded")) {
                throw SynchroError.InvalidResponse("durable push retry identity has no resolved sealed batch")
            }
            DurableBackoffStore.clearMatching(db, RetryOperation.PUSHING, expectedBatchID)
        }
        return null
    }

    private fun loadOrSealBatch(
        clientID: String,
        clientGeneration: Long,
        schemaVersion: Long,
        schemaHash: String,
        syncedTables: List<LocalSchemaTable>,
        batchSize: Int,
    ): SealedPushBatch? = database.writeTransaction { db ->
        loadPendingSealedBatch(
            db,
            clientID,
            SchemaRef(schemaVersion, schemaHash),
            syncedTables,
        )?.let { return@writeTransaction it }

        normalizeUnsealedChains(db)
        val candidates = eligibleForSealing(db, batchSize)
        if (candidates.isEmpty()) return@writeTransaction null
        val requestSchema = SchemaRef(schemaVersion, schemaHash)
        val authoredTablesCache = HashMap<SchemaRef, List<LocalSchemaTable>?>()
        val mutations = candidates.map { candidate ->
            val built = buildMutation(db, candidate)
            if (built.authoredSchema == requestSchema) {
                built
            } else {
                // A mutation authored before the installed schema can carry
                // captured defaults of since-removed fields. The same
                // reconciliation rule as the renewal path applies here.
                val authoredTables = authoredTablesCache.getOrPut(built.authoredSchema) {
                    schemaTablesForReference(db, built.authoredSchema, null)
                }
                if (authoredTables == null) built else reconcileRemovedDefaults(built, authoredTables, syncedTables)
            }
        }
        validateNewMutations(db, mutations, requestSchema, syncedTables)
        val request = PushRequest(
            clientID = clientID,
            clientGeneration = clientGeneration,
            batchID = UUID.randomUUID().toString(),
            schema = requestSchema,
            mutations = mutations,
        )
        request.validate()
        val requestJSON = json.encodeToString(request)
        db.execSQL(
            """
            INSERT INTO _synchro_push_batches
                (batch_id, request_json, pending_json, schema_json, state, created_at)
            VALUES (?, ?, '[]', ?, 'pending', substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z')
            """.trimIndent(),
            arrayOf(request.batchID, requestJSON, json.encodeToString(syncedTables)),
        )
        candidates.forEachIndexed { ordinal, candidate ->
            db.execSQL(
                """
                UPDATE _synchro_pending_changes
                SET lifecycle_state = 'sealed', sealed_batch_id = ?, sealed_ordinal = ?,
                    updated_at = substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z'
                WHERE mutation_id = ? AND lifecycle_state = 'captured'
                """.trimIndent(),
                arrayOf(request.batchID, ordinal, candidate.mutationID),
            )
            requireExactlyOneChange(db, "mutation seal was not durable")
            db.execSQL(
                "INSERT INTO _synchro_push_batch_members (batch_id, mutation_id, ordinal) VALUES (?, ?, ?)",
                arrayOf(request.batchID, candidate.mutationID, ordinal),
            )
        }
        SealedPushBatch(
            request = request,
            requestJSON = requestJSON,
            members = candidates.mapIndexed { ordinal, candidate ->
                candidate.copy(sealedBatchID = request.batchID, sealedOrdinal = ordinal, lifecycleState = "sealed")
            },
            historicalTables = syncedTables,
        )
    }

    /**
     * Pre-v7 batches do not have membership rows. Their request JSON is already
     * sealed, so this imports members from that immutable request only.
     */
    private fun loadPendingSealedBatch(
        db: SQLiteDatabase,
        clientID: String,
        currentSchema: SchemaRef,
        currentTables: List<LocalSchemaTable>,
    ): SealedPushBatch? {
        val batches = mutableListOf<Triple<String, String, String>>()
        db.rawQuery(
            """
            SELECT batch_id, request_json, schema_json
            FROM _synchro_push_batches
            WHERE state = 'pending'
            ORDER BY created_at, batch_id
            LIMIT 2
            """.trimIndent(),
            null,
        ).use { cursor ->
            while (cursor.moveToNext()) batches += Triple(cursor.getString(0), cursor.getString(1), cursor.getString(2))
        }
        if (batches.size > 1) throw SynchroError.InvalidResponse("multiple active sealed push batches")
        val (batchID, requestJSON, schemaJSON) = batches.singleOrNull() ?: return null
        val request = try {
            json.decodeFromString<PushRequest>(requestJSON)
        } catch (_: Exception) {
            throw SynchroError.InvalidResponse("stored sealed push batch is invalid")
        }
        if (request.batchID != batchID || request.clientID != clientID) {
            throw SynchroError.InvalidResponse("stored sealed push batch identity is invalid")
        }
        if (request.mutations.isEmpty() || request.mutations.map { it.mutationID }.toSet().size != request.mutations.size) {
            throw SynchroError.InvalidResponse("stored sealed push batch has invalid mutation membership")
        }
        val decodedTables = try {
            json.decodeFromString<List<LocalSchemaTable>>(schemaJSON)
        } catch (_: Exception) {
            throw SynchroError.InvalidResponse("stored sealed push batch is invalid")
        }
        val historicalTables = if (decodedTables.isNotEmpty()) {
            decodedTables
        } else {
            recoverLegacyBatchSchema(db, batchID, request, schemaJSON, currentSchema, currentTables)
        }
        validateSealedRequest(db, request, historicalTables)

        var members = membersForBatch(db, batchID)
        if (members.isEmpty()) {
            materializeLegacySealedBatch(db, request, historicalTables)
            members = membersForBatch(db, batchID)
        }
        if (members.size != request.mutations.size || members.map { it.mutationID } != request.mutations.map { it.mutationID }) {
            throw SynchroError.InvalidResponse("stored sealed push membership is invalid")
        }
        return SealedPushBatch(request, requestJSON, members, historicalTables)
    }

    /**
     * Version-six batches stored an empty schema array. Repair only an exact
     * active binding or an exact retained archive. The request JSON stays immutable.
     */
    private fun recoverLegacyBatchSchema(
        db: SQLiteDatabase,
        batchID: String,
        request: PushRequest,
        storedSchemaJSON: String,
        currentSchema: SchemaRef,
        currentTables: List<LocalSchemaTable>,
    ): List<LocalSchemaTable> {
        val tables = schemaTablesForReference(db, request.schema, null)
            ?: currentTables.takeIf { request.schema == currentSchema && it.isNotEmpty() }
            ?: throw SynchroError.InvalidResponse("legacy sealed push batch has no exact schema")
        val encoded = json.encodeToString(tables)
        db.execSQL(
            "UPDATE _synchro_push_batches SET schema_json = ? WHERE batch_id = ? AND schema_json = ? AND state = 'pending'",
            arrayOf(encoded, batchID, storedSchemaJSON),
        )
        requireExactlyOneChange(db, "legacy sealed push schema repair was not durable")
        db.execSQL(
            """
            INSERT OR IGNORE INTO _synchro_schema_archives (schema_version, schema_hash, manifest_json, created_at)
            VALUES (?, ?, ?, substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z')
            """.trimIndent(),
            arrayOf(request.schema.version, request.schema.hash, encoded),
        )
        return tables
    }

    private fun materializeLegacySealedBatch(
        db: SQLiteDatabase,
        request: PushRequest,
        syncedTables: List<LocalSchemaTable>,
    ) {
        val tables = syncedTables.associateBy { it.tableID }
        request.mutations.forEachIndexed { ordinal, mutation ->
            if (changeTracker.changeByID(db, mutation.mutationID) != null) {
                throw SynchroError.InvalidResponse("legacy batch mutation identity collides with local ledger")
            }
            val table = tables[mutation.table]
            val pkField = mutation.pk.keys.singleOrNull()
                ?: throw SynchroError.InvalidResponse("legacy batch has an invalid primary key")
            val pkType = table?.columns?.singleOrNull { it.fieldID == pkField }?.logicalType
                ?: inferredPrimaryKeyType(mutation.pk.getValue(pkField))
            val recordID = recordID(mutation.pk.getValue(pkField), pkType)
            insertLedgerMutation(
                db = db,
                mutationID = mutation.mutationID,
                tableID = mutation.table,
                tableName = table?.tableName ?: "legacy_unknown",
                recordID = recordID,
                pkFieldID = pkField,
                pkLogicalType = pkType,
                operation = mutation.op.wireName(),
                authoredSchema = mutation.authoredSchema,
                baseVersion = mutation.baseVersion,
                clientVersion = mutation.clientVersion,
                lifecycleState = "sealed",
                sourceKind = "legacy_sealed",
                dependsOnMutationID = null,
                normalizedMutationID = null,
                sealedBatchID = request.batchID,
                sealedOrdinal = ordinal,
                values = mutation.columns?.map { (fieldID, value) ->
                    ledgerValueFromJson(fieldID, table?.columns?.singleOrNull { it.fieldID == fieldID }?.logicalType ?: "json", value)
                }.orEmpty(),
            )
            db.execSQL(
                "INSERT INTO _synchro_push_batch_members (batch_id, mutation_id, ordinal) VALUES (?, ?, ?)",
                arrayOf(request.batchID, mutation.mutationID, ordinal),
            )
        }
    }

    private fun membersForBatch(db: SQLiteDatabase, batchID: String): List<PendingChange> {
        val ids = mutableListOf<String>()
        db.rawQuery(
            "SELECT mutation_id FROM _synchro_push_batch_members WHERE batch_id = ? ORDER BY ordinal",
            arrayOf(batchID),
        ).use { cursor -> while (cursor.moveToNext()) ids += cursor.getString(0) }
        return ids.map { id ->
            changeTracker.changeByID(db, id)
                ?: throw SynchroError.InvalidResponse("sealed batch member is missing from the mutation ledger")
        }
    }

    /**
     * A schema archive is part of a sealed request's immutable validation state.
     * Prefer the exact schema JSON retained by a sealed batch over mutable local metadata.
     */
    private fun schemaTablesForReference(
        db: SQLiteDatabase,
        schema: SchemaRef,
        preferred: List<LocalSchemaTable>?,
    ): List<LocalSchemaTable>? {
        preferred?.takeIf { it.isNotEmpty() }?.let { return it }
        db.rawQuery(
            "SELECT request_json, schema_json FROM _synchro_push_batches ORDER BY created_at, batch_id",
            null,
        ).use { cursor ->
            while (cursor.moveToNext()) {
                val request = runCatching { json.decodeFromString<PushRequest>(cursor.getString(0)) }.getOrNull()
                if (request?.schema != schema) continue
                val tables = runCatching { json.decodeFromString<List<LocalSchemaTable>>(cursor.getString(1)) }.getOrNull()
                if (!tables.isNullOrEmpty()) return tables
            }
        }
        db.rawQuery(
            "SELECT manifest_json FROM _synchro_schema_archives WHERE schema_version = ? AND schema_hash = ?",
            arrayOf(schema.version.toString(), schema.hash),
        ).use { cursor ->
            if (!cursor.moveToFirst()) return null
            return try {
                json.decodeFromString<List<LocalSchemaTable>>(cursor.getString(0))
            } catch (_: Exception) {
                throw SynchroError.InvalidResponse("stored historical schema is invalid")
            }
        }
    }

    private fun validateSealedRequest(
        db: SQLiteDatabase,
        request: PushRequest,
        historicalTables: List<LocalSchemaTable>,
    ) {
        try {
            request.validate()
        } catch (_: ContractException) {
            throw SynchroError.InvalidResponse("stored sealed push request is invalid")
        }
        validateNewMutations(db, request.mutations, request.schema, historicalTables)
    }

    private fun markBatchRenewalRequired(batchID: String) {
        database.writeTransaction { db ->
            db.execSQL(
                """
                UPDATE _synchro_push_batches
                SET state = 'renewal_required'
                WHERE batch_id = ? AND state = 'pending'
                """.trimIndent(),
                arrayOf(batchID),
            )
            requireExactlyOneChange(db, "sealed push batch renewal state was not durable")
            DurableBackoffStore.clearMatching(db, RetryOperation.PUSHING, batchID)
        }
    }

    /**
     * Rebinds a request only after connect installed a different generation or schema.
     * It keeps each mutation record and authored payload unchanged.
     */
    internal fun renewRequiredBatches(
        clientID: String,
        clientGeneration: Long,
        schemaVersion: Long,
        schemaHash: String,
        syncedTables: List<LocalSchemaTable>,
    ): Boolean = database.writeTransaction { db ->
        renewRequiredBatchesInTransaction(db, clientID, clientGeneration, schemaVersion, schemaHash, syncedTables)
    }

    /**
     * A schema reset cannot replay a request bound to old materialization.
     * Keep the old batch immutable and require a durable successor binding.
     */
    internal fun reconcileSchemaResetInTransaction(db: SQLiteDatabase) {
        val batches = mutableListOf<Pair<String, String>>()
        db.rawQuery(
            "SELECT batch_id, state FROM _synchro_push_batches WHERE state IN ('pending', 'renewal_required', 'reset_renewal_required') ORDER BY created_at, batch_id",
            null,
        ).use { cursor -> while (cursor.moveToNext()) batches += cursor.getString(0) to cursor.getString(1) }
        if (batches.size > 1) {
            throw SynchroError.InvalidResponse("schema reset found multiple unresolved push batches")
        }
        batches.singleOrNull()?.let { (batchID, state) ->
            if (state != "reset_renewal_required") {
                db.execSQL(
                    "UPDATE _synchro_push_batches SET state = 'reset_renewal_required' WHERE batch_id = ? AND state = ?",
                    arrayOf(batchID, state),
                )
                requireExactlyOneChange(db, "schema reset batch renewal state was not durable")
            }
            DurableBackoffStore.load(db)?.let { backoff ->
                if (backoff.resumeState == RetryOperation.PUSHING && backoff.workIdentity == batchID) {
                    DurableBackoffStore.clearMatching(db, backoff.resumeState, backoff.workIdentity)
                }
            }
        }
    }

    // The insert triggers capture every writable field with its declared
    // default. A class-4 transition can remove such a field, and a captured
    // default carries no authored intent, so the renewed request drops the
    // column when the reconciled schema removed the field and the captured
    // value equals the authored default. An authored non-default value stays,
    // and the server rejects that mutation, which preserves the authored
    // outcome of the incompatible write.
    private fun reconcileRemovedDefaults(
        mutation: Mutation,
        authoredTables: List<LocalSchemaTable>,
        currentTables: List<LocalSchemaTable>,
    ): Mutation {
        val columns = mutation.columns ?: return mutation
        val authoredColumns = authoredTables.firstOrNull { it.tableID == mutation.table }?.columns
            ?: return mutation
        val currentFieldIDs = currentTables.firstOrNull { it.tableID == mutation.table }
            ?.columns?.mapTo(HashSet()) { it.fieldID }
            ?: return mutation
        val retained = columns.filterKeys { fieldID ->
            if (fieldID in currentFieldIDs) return@filterKeys true
            val authored = authoredColumns.firstOrNull { it.fieldID == fieldID }
                ?: return@filterKeys true
            val defaultValue = constantDefaultWireValue(authored)
                ?: return@filterKeys true
            columns[fieldID] != defaultValue
        }
        if (retained.size == columns.size) return mutation
        return mutation.copy(columns = JsonObject(retained))
    }

    // Only a constant literal default can prove capture-time equality. A
    // dynamic default, or any form this parser does not recognize, keeps the
    // column so the mismatch stays visible at the server.
    private fun constantDefaultWireValue(column: LocalSchemaColumn): JsonElement? {
        val sql = column.sqliteDefaultSQL?.trim() ?: return null
        if (sql.equals("NULL", ignoreCase = true)) return JsonNull
        if (sql.length >= 2 && sql.startsWith("'") && sql.endsWith("'")) {
            return JsonPrimitive(sql.substring(1, sql.length - 1).replace("''", "'"))
        }
        sql.toLongOrNull()?.let { return JsonPrimitive(it) }
        return null
    }

    private fun renewRequiredBatchesInTransaction(
        db: SQLiteDatabase,
        clientID: String,
        clientGeneration: Long,
        schemaVersion: Long,
        schemaHash: String,
        syncedTables: List<LocalSchemaTable>,
    ): Boolean {
        data class RenewableBatch(val id: String, val requestJSON: String, val state: String)
        val batches = mutableListOf<RenewableBatch>()
        db.rawQuery(
            """
            SELECT batch_id, request_json, state
            FROM _synchro_push_batches
            WHERE state IN ('renewal_required', 'reset_renewal_required')
            ORDER BY created_at, batch_id
            """.trimIndent(),
            null,
        ).use { cursor ->
            while (cursor.moveToNext()) {
                batches += RenewableBatch(cursor.getString(0), cursor.getString(1), cursor.getString(2))
            }
        }
        if (batches.isEmpty()) return false
        if (batches.size > 1) throw SynchroError.InvalidResponse("multiple renewal-required push batches")
        val installed = SchemaRef(schemaVersion, schemaHash)
        installed.validate()
        batches.forEach { batch ->
            val oldBatchID = batch.id
            val requestJSON = batch.requestJSON
            val oldRequest = try {
                json.decodeFromString<PushRequest>(requestJSON)
            } catch (_: Exception) {
                throw SynchroError.InvalidResponse("renewal request is invalid")
            }
            try {
                oldRequest.validate()
            } catch (_: ContractException) {
                throw SynchroError.InvalidResponse("renewal request is invalid")
            }
            if (oldRequest.batchID != oldBatchID || oldRequest.clientID != clientID ||
                (batch.state != "reset_renewal_required" &&
                    oldRequest.clientGeneration == clientGeneration && oldRequest.schema == installed)
            ) {
                throw SynchroError.InvalidResponse("reconnect did not change the sealed push binding")
            }
            val oldTables = schemaTablesForReference(db, oldRequest.schema, null)
                ?: throw SynchroError.InvalidResponse("renewal request has no retained schema")
            validateNewMutations(db, oldRequest.mutations, oldRequest.schema, oldTables)
            val members = membersForBatch(db, oldBatchID)
            if (members.map { it.mutationID } != oldRequest.mutations.map { it.mutationID }) {
                throw SynchroError.InvalidResponse("renewal batch membership is invalid")
            }
            members.forEach { member ->
                if (member.lifecycleState != "sealed" || member.sealedBatchID != oldBatchID) {
                    throw SynchroError.InvalidResponse("renewal mutation is no longer sealed by its original batch")
                }
            }
            val successor = PushRequest(
                clientID = clientID,
                clientGeneration = clientGeneration,
                batchID = UUID.randomUUID().toString(),
                schema = installed,
                mutations = oldRequest.mutations.map { reconcileRemovedDefaults(it, oldTables, syncedTables) },
            )
            try {
                successor.validate()
            } catch (_: ContractException) {
                throw SynchroError.InvalidResponse("renewed push request is invalid")
            }
            validateNewMutations(db, successor.mutations, installed, syncedTables)
            val successorJSON = json.encodeToString(successor)
            db.execSQL(
                """
                INSERT INTO _synchro_push_batches
                    (batch_id, request_json, pending_json, schema_json, state, created_at)
                VALUES (?, ?, '[]', ?, 'pending', substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z')
                """.trimIndent(),
                arrayOf(successor.batchID, successorJSON, json.encodeToString(syncedTables)),
            )
            db.execSQL(
                "UPDATE _synchro_push_batches SET state = 'superseded' WHERE batch_id = ? AND state = ?",
                arrayOf(oldBatchID, batch.state),
            )
            requireExactlyOneChange(db, "renewal batch supersession was not durable")
            DurableBackoffStore.clearMatching(db, RetryOperation.PUSHING, oldBatchID)
            members.forEachIndexed { ordinal, member ->
                // The captured transition is internal to this transaction. It keeps
                // IDs and payloads unchanged before the successor is sealed.
                db.execSQL(
                    """
                    UPDATE _synchro_pending_changes
                    SET lifecycle_state = 'captured', sealed_batch_id = NULL, sealed_ordinal = NULL,
                        updated_at = substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z'
                    WHERE mutation_id = ? AND lifecycle_state = 'sealed' AND sealed_batch_id = ?
                    """.trimIndent(),
                    arrayOf(member.mutationID, oldBatchID),
                )
                requireExactlyOneChange(db, "renewal mutation could not return to sendable state")
                db.execSQL(
                    """
                    UPDATE _synchro_pending_changes
                    SET lifecycle_state = 'sealed', sealed_batch_id = ?, sealed_ordinal = ?,
                        updated_at = substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z'
                    WHERE mutation_id = ? AND lifecycle_state = 'captured'
                    """.trimIndent(),
                    arrayOf(successor.batchID, ordinal, member.mutationID),
                )
                requireExactlyOneChange(db, "renewal successor mutation was not sealed")
                db.execSQL(
                    "INSERT INTO _synchro_push_batch_members (batch_id, mutation_id, ordinal) VALUES (?, ?, ?)",
                    arrayOf(successor.batchID, member.mutationID, ordinal),
                )
            }
        }
        return true
    }

    internal fun hasRenewalRequiredBatches(): Boolean = database.readTransaction { db ->
        db.rawQuery(
            "SELECT 1 FROM _synchro_push_batches WHERE state IN ('renewal_required', 'reset_renewal_required') LIMIT 1",
            null,
        )
            .use { it.moveToFirst() }
    }

    private fun eligibleForSealing(db: SQLiteDatabase, limit: Int): List<PendingChange> {
        val selected = mutableListOf<String>()
        db.rawQuery(
            """
            SELECT mutation_id
            FROM _synchro_pending_changes candidate
            WHERE candidate.lifecycle_state = 'captured'
              AND (candidate.operation = 'insert' OR candidate.base_version IS NOT NULL)
              AND NOT EXISTS (
                  SELECT 1 FROM _synchro_pending_changes predecessor
                  WHERE predecessor.mutation_id = candidate.depends_on_mutation_id
                    AND predecessor.lifecycle_state <> 'accepted'
              )
            ORDER BY candidate.local_order
            LIMIT ?
            """.trimIndent(),
            arrayOf(limit.toString()),
        ).use { cursor -> while (cursor.moveToNext()) selected += cursor.getString(0) }
        return selected.map { id ->
            changeTracker.changeByID(db, id)
                ?: throw SynchroError.InvalidResponse("selected ledger mutation disappeared")
        }
    }

    /** Normalization creates a new immutable record and never edits a source intent. */
    private fun normalizeUnsealedChains(db: SQLiteDatabase) {
        data class LogicalRow(
            val tableID: String,
            val pkFieldID: String,
            val pkLogicalType: String,
            val recordID: String,
        )
        val rows = mutableListOf<LogicalRow>()
        db.rawQuery(
            """
            SELECT table_id, pk_field_id, pk_logical_type, record_id
            FROM _synchro_pending_changes
            WHERE lifecycle_state = 'captured'
            GROUP BY table_id, pk_field_id, pk_logical_type, record_id
            HAVING COUNT(*) > 1
            """.trimIndent(),
            null,
        ).use { cursor ->
            while (cursor.moveToNext()) {
                rows += LogicalRow(cursor.getString(0), cursor.getString(1), cursor.getString(2), cursor.getString(3))
            }
        }

        rows.forEach { row ->
            val chain = changeTracker.capturedChain(
                db,
                row.tableID,
                row.pkFieldID,
                row.pkLogicalType,
                row.recordID,
            )
            if (chain.size < 2) return@forEach
            if (chain.map { SchemaRef(it.authoredSchemaVersion, it.authoredSchemaHash) }.toSet().size != 1) {
                // A newer authored schema cannot be folded into its predecessor.
                // The predecessor stays sendable and the successor stays dependent.
                return@forEach
            }
            val first = chain.first()
            val deleteIndex = chain.indexOfFirst { it.operation == "delete" }
            if (first.operation == "delete") {
                blockAfterDelete(db, chain.drop(1))
                return@forEach
            }
            if (deleteIndex >= 0) {
                val sources = chain.take(deleteIndex + 1)
                val suffix = chain.drop(deleteIndex + 1)
                if (first.operation == "insert") {
                    cancelBeforeSend(db, sources)
                } else {
                    normalizeChain(db, sources, "delete", emptyList())
                }
                blockAfterDelete(db, suffix)
                return@forEach
            }
            when (first.operation) {
                "insert" -> normalizeChain(db, chain, "insert", mergedValues(db, chain))
                "update" -> normalizeChain(db, chain, "update", mergedValues(db, chain))
                else -> throw SynchroError.InvalidResponse("unknown local mutation operation")
            }
        }
    }

    private fun normalizeChain(
        db: SQLiteDatabase,
        sources: List<PendingChange>,
        operation: String,
        values: List<LedgerValue>,
    ) {
        val first = sources.first()
        val last = sources.last()
        if ((operation == "update" || operation == "delete") && first.baseUpdatedAt.isNullOrEmpty()) {
            blockBeforeSend(db, sources)
            return
        }
        val normalizedID = UUID.randomUUID().toString()
        insertLedgerMutation(
            db = db,
            mutationID = normalizedID,
            tableID = first.tableID,
            tableName = first.tableName,
            recordID = first.recordID,
            pkFieldID = first.pkFieldID,
            pkLogicalType = first.pkLogicalType,
            operation = operation,
            authoredSchema = SchemaRef(first.authoredSchemaVersion, first.authoredSchemaHash),
            baseVersion = if (operation == "insert") null else first.baseUpdatedAt,
            clientVersion = last.clientUpdatedAt,
            lifecycleState = "captured",
            sourceKind = "normalized",
            dependsOnMutationID = null,
            normalizedMutationID = null,
            sealedBatchID = null,
            sealedOrdinal = null,
            values = values,
        )
        markSources(db, sources, "superseded_before_send", normalizedID)
    }

    private fun cancelBeforeSend(db: SQLiteDatabase, sources: List<PendingChange>) {
        val cancellationID = UUID.randomUUID().toString()
        markSources(db, sources, "cancelled_before_send", cancellationID)
    }

    private fun blockBeforeSend(db: SQLiteDatabase, sources: List<PendingChange>) {
        sources.forEach { source ->
            db.execSQL(
                """
                UPDATE _synchro_pending_changes
                SET lifecycle_state = 'blocked_by_predecessor', updated_at = substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z'
                WHERE mutation_id = ? AND lifecycle_state = 'captured'
                """.trimIndent(),
                arrayOf(source.mutationID),
            )
        }
    }

    private fun blockAfterDelete(db: SQLiteDatabase, sources: List<PendingChange>) = blockBeforeSend(db, sources)

    private fun markSources(db: SQLiteDatabase, sources: List<PendingChange>, state: String, normalizedID: String) {
        sources.forEach { source ->
            db.execSQL(
                """
                UPDATE _synchro_pending_changes
                SET lifecycle_state = ?, normalized_mutation_id = ?,
                    updated_at = substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z'
                WHERE mutation_id = ? AND lifecycle_state = 'captured'
                """.trimIndent(),
                arrayOf(state, normalizedID, source.mutationID),
            )
            requireExactlyOneChange(db, "normalization source was not durable")
        }
    }

    private fun mergedValues(db: SQLiteDatabase, source: List<PendingChange>): List<LedgerValue> {
        val values = linkedMapOf<String, LedgerValue>()
        source.forEach { intent ->
            changeTracker.valuesForMutation(db, intent.mutationID).forEach { values[it.fieldID] = it }
        }
        return values.values.sortedBy { it.fieldID }
    }

    private fun buildMutation(db: SQLiteDatabase, change: PendingChange): Mutation {
        val values = changeTracker.valuesForMutation(db, change.mutationID)
        val operation = when (change.operation) {
            "insert" -> Operation.INSERT
            "update" -> Operation.UPDATE
            "delete" -> Operation.DELETE
            else -> throw SynchroError.InvalidResponse("stored mutation has an invalid operation")
        }
        val columns = when (operation) {
            Operation.DELETE -> null
            else -> JsonObject(values.associate { it.fieldID to it.toJson() })
        }
        return Mutation(
            mutationID = change.mutationID,
            table = change.tableID,
            op = operation,
            pk = JsonObject(mapOf(change.pkFieldID to primaryKeyElement(change.recordID, change.pkLogicalType))),
            authoredSchema = SchemaRef(change.authoredSchemaVersion, change.authoredSchemaHash),
            baseVersion = change.baseUpdatedAt,
            clientVersion = change.clientUpdatedAt,
            columns = columns,
        )
    }

    private fun validateNewMutations(
        db: SQLiteDatabase,
        mutations: List<Mutation>,
        requestSchema: SchemaRef,
        currentTables: List<LocalSchemaTable>,
    ) {
        if (mutations.isEmpty() || mutations.map { it.mutationID }.toSet().size != mutations.size) {
            throw SynchroError.InvalidResponse("cannot seal an empty or duplicate mutation batch")
        }
        mutations.forEach { mutation ->
            mutation.authoredSchema.validate()
            Integrity.validateCanonicalClientVersion(mutation.clientVersion)
            val authoredTables = schemaTablesForReference(
                db,
                mutation.authoredSchema,
                if (mutation.authoredSchema == requestSchema) currentTables else null,
            ) ?: throw SynchroError.InvalidResponse("stored mutation has no retained authored schema")
            val table = authoredTables.singleOrNull { it.tableID == mutation.table }
                ?: throw SynchroError.InvalidResponse("stored mutation has no authored logical table")
            val primaryKey = table.columns.singleOrNull { it.fieldID == table.primaryKeyFieldID }
                ?: throw SynchroError.InvalidResponse("stored mutation has no authored primary key")
            if (mutation.pk.size != 1 || mutation.pk.keys.singleOrNull() != table.primaryKeyFieldID) {
                throw SynchroError.InvalidResponse("stored mutation has an invalid primary key")
            }
            recordID(mutation.pk.getValue(table.primaryKeyFieldID), primaryKey.logicalType)
            when (mutation.op) {
                Operation.INSERT -> if (mutation.baseVersion != null || mutation.columns.isNullOrEmpty()) {
                    throw SynchroError.InvalidResponse("stored insert has an invalid shape")
                }
                Operation.UPDATE -> if (mutation.baseVersion.isNullOrEmpty() || mutation.columns.isNullOrEmpty()) {
                    throw SynchroError.InvalidResponse("stored update has an invalid shape")
                }
                Operation.DELETE -> if (mutation.baseVersion.isNullOrEmpty() || mutation.columns != null) {
                    throw SynchroError.InvalidResponse("stored delete has an invalid shape")
                }
                Operation.UPSERT -> throw SynchroError.InvalidResponse("stored upsert is not sendable")
            }
            mutation.columns?.forEach { (fieldID, value) ->
                val field = table.columns.singleOrNull { it.fieldID == fieldID }
                    ?: throw SynchroError.InvalidResponse("stored mutation has an unknown authored field")
                if (!field.writable) {
                    throw SynchroError.InvalidResponse("stored mutation has a non-writable authored field")
                }
                try {
                    Integrity.validateTypedValue(value, field)
                } catch (_: IllegalArgumentException) {
                    throw SynchroError.InvalidResponse("stored mutation has an invalid portable value")
                }
            }
        }
    }

    // MARK: - Exact-ID reconciliation

    private data class AuthoritativeProjection(
        val currentTable: LocalSchemaTable,
        val values: List<Pair<LocalSchemaColumn, JsonElement>>,
    )

    private data class LocalPatch(
        val change: PendingChange,
        val values: List<Pair<LocalSchemaColumn, JsonElement>>,
        val deletedAtValue: Any?,
    )

    fun applyAccepted(
        accepted: List<AcceptedMutation>,
        syncedTables: List<LocalSchemaTable>,
        sentPending: Map<String, PendingChange> = emptyMap(),
    ): List<ConflictEvent> = database.writeSyncLockedTransaction { db ->
        applyAcceptedInTransaction(db, accepted, syncedTables, sentPending)
    }

    private fun applyAcceptedInTransaction(
        db: SQLiteDatabase,
        accepted: List<AcceptedMutation>,
        syncedTables: List<LocalSchemaTable>,
        sentPending: Map<String, PendingChange>,
        outcomeJSONByID: Map<String, String> = emptyMap(),
        sealedSchemas: Map<SchemaRef, List<LocalSchemaTable>> = emptyMap(),
    ): List<ConflictEvent> {
        accepted.forEach { outcome ->
            val source = exactLedgerSource(db, outcome.mutationID, sentPending)
            val recordID = validateOutcomeIdentity(source, outcome.table, outcome.pk)
            val later = laterUnresolvedIntents(db, source)
            val current = currentTableForSource(source, syncedTables)
            val row = outcome.serverRow
            val projection = row?.let {
                val historical = historicalTableForOutcome(db, outcome.outcomeSchema, outcome.table, sealedSchemas)
                verifyAuthoritativeChecksum(it, outcome.pk, outcome.rowChecksum, outcome.serverVersion, outcome.outcomeSchema, historical)
                projectAuthoritativeRow(historical, current, source, outcome.pk, it)
            }
            val patches = if (current != null && (row == null || projection != null)) {
                prepareLocalPatches(db, current, source, later)
            } else {
                null
            }
            val hasAuthoritativeAbsence = row == null && source.operation == "delete"
            val canApply = current != null &&
                (row == null || projection != null) &&
                patches != null &&
                !(hasAuthoritativeAbsence && later.isNotEmpty())
            val outcomeJSON = outcomeJSONByID[outcome.mutationID] ?: json.encodeToString(outcome)
            val newlyApplied = recordAcceptedOutcome(db, source, outcomeJSON)
            if (!newlyApplied) return@forEach

            if (canApply) {
                if (row != null) {
                    applyAuthoritativeProjection(db, projection!!)
                } else if (hasAuthoritativeAbsence) {
                    applyAuthoritativeAbsence(db, current!!, recordID, source.pkLogicalType)
                }
                applyLocalPatches(db, current!!, recordID, source.pkLogicalType, patches!!)
                SynchroMeta.upsertRowVersion(db, current.tableName, recordID, outcome.serverVersion, outcome.rowChecksum)
            } else {
                if (hasAuthoritativeAbsence && current != null) {
                    SynchroMeta.upsertRowVersion(db, current.tableName, recordID, outcome.serverVersion, null)
                }
                invalidateAffectedScopes(db, recordID, listOfNotNull(current?.tableName))
            }
            // Only mutable, unsealed successors can receive an accepted server base.
            refreshUnsealedSuccessors(db, source.mutationID, outcome.serverVersion)
        }
        return emptyList()
    }

    data class RejectedOutcome(val conflicts: List<ConflictEvent>)

    fun applyRejected(
        rejected: List<RejectedMutation>,
        syncedTables: List<LocalSchemaTable>,
        sentPending: Map<String, PendingChange> = emptyMap(),
    ): List<ConflictEvent> = database.writeSyncLockedTransaction { db ->
        applyRejectedOutcomeInTransaction(db, rejected, syncedTables, sentPending).conflicts
    }

    private fun applyRejectedOutcomeInTransaction(
        db: SQLiteDatabase,
        rejected: List<RejectedMutation>,
        syncedTables: List<LocalSchemaTable>,
        sentPending: Map<String, PendingChange>,
        outcomeJSONByID: Map<String, String> = emptyMap(),
        mutationJSONByID: Map<String, String> = emptyMap(),
        sealedSchemas: Map<SchemaRef, List<LocalSchemaTable>> = emptyMap(),
    ): RejectedOutcome {
        val conflicts = mutableListOf<ConflictEvent>()
        rejected.forEach { outcome ->
            val source = exactLedgerSource(db, outcome.mutationID, sentPending)
            val recordID = validateOutcomeIdentity(source, outcome.table, outcome.pk)
            val later = laterUnresolvedIntents(db, source)
            val current = currentTableForSource(source, syncedTables)
            val row = outcome.serverRow
            val projection = row?.let {
                val version = outcome.serverVersion
                    ?: throw SynchroError.InvalidResponse("conflict row lacks its server version")
                val historical = historicalTableForOutcome(db, outcome.outcomeSchema, outcome.table, sealedSchemas)
                verifyAuthoritativeChecksum(it, outcome.pk, outcome.rowChecksum, version, outcome.outcomeSchema, historical)
                projectAuthoritativeRow(historical, current, source, outcome.pk, it)
            }
            val patches = if (current != null && (row == null || projection != null)) {
                prepareLocalPatches(db, current, source, later)
            } else {
                null
            }
            val hasAuthoritativeAbsence = row == null &&
                outcome.status == MutationStatus.CONFLICT &&
                outcome.code in setOf(MutationRejectionCode.ROW_DELETED, MutationRejectionCode.ROW_NOT_FOUND)
            val canApply = current != null &&
                (row == null || projection != null) &&
                patches != null &&
                !(hasAuthoritativeAbsence && later.isNotEmpty())
            val rejectionJSON = outcomeJSONByID[outcome.mutationID] ?: json.encodeToString(outcome)
            val originalMutationJSON = mutationJSONByID[outcome.mutationID] ?: json.encodeToString(buildMutation(db, source))
            val newlyApplied = recordRejectedOutcome(db, source, outcome, rejectionJSON)
            if (!newlyApplied) return@forEach

            SynchroMeta.upsertRejectedMutation(
                db = db,
                mutationID = source.mutationID,
                tableName = current?.tableName ?: source.tableName,
                recordId = source.recordID,
                status = outcome.status.name.lowercase(),
                code = outcome.code.name.lowercase(),
                message = outcome.message,
                serverRowJson = outcome.serverRow?.toString(),
                serverVersion = outcome.serverVersion,
                mutationJSON = originalMutationJSON,
                rejectionJSON = rejectionJSON,
            )

            if (canApply) {
                if (row != null) {
                    applyAuthoritativeProjection(db, projection!!)
                } else if (hasAuthoritativeAbsence) {
                    applyAuthoritativeAbsence(db, current!!, recordID, source.pkLogicalType)
                }
                applyLocalPatches(db, current!!, recordID, source.pkLogicalType, patches!!)
                outcome.serverVersion?.let { version ->
                    SynchroMeta.upsertRowVersion(db, current.tableName, recordID, version, outcome.rowChecksum)
                }
            } else if (row != null || hasAuthoritativeAbsence) {
                if (hasAuthoritativeAbsence && current != null && outcome.serverVersion != null) {
                    SynchroMeta.upsertRowVersion(db, current.tableName, recordID, outcome.serverVersion, null)
                }
                invalidateAffectedScopes(db, recordID, listOfNotNull(current?.tableName))
            }

            // Rejection never updates a dependent base. It only blocks every descendant.
            blockUnsealedDependents(db, source.mutationID)
            if (outcome.status == MutationStatus.CONFLICT && current != null) {
                val serverData = outcome.serverRow?.let { serverRow ->
                    current.columns.mapNotNull { column ->
                        serverRow[column.fieldID]?.let { column.name to AnyCodable(fromJsonElement(it)) }
                    }.toMap()
                }
                conflicts += ConflictEvent(current.tableName, recordID, null, serverData)
            }
        }
        return RejectedOutcome(conflicts)
    }

    private fun exactLedgerSource(
        db: SQLiteDatabase,
        mutationID: String,
        sent: Map<String, PendingChange>,
    ): PendingChange {
        val source = changeTracker.changeByID(db, mutationID)
            ?: throw SynchroError.InvalidResponse("push outcome targets an unknown mutation ID")
        if (sent.isNotEmpty() && mutationID !in sent) {
            throw SynchroError.InvalidResponse("push outcome is not a sealed batch member")
        }
        sent[mutationID]?.let { expected ->
            if (expected.mutationID != source.mutationID || expected.localOrder != source.localOrder ||
                expected.tableID != source.tableID || expected.pkFieldID != source.pkFieldID ||
                expected.pkLogicalType != source.pkLogicalType || expected.recordID != source.recordID
            ) {
                throw SynchroError.InvalidResponse("push outcome does not match sealed membership")
            }
        }
        return source
    }

    private fun validateOutcomeIdentity(source: PendingChange, tableID: String, pk: JsonObject): String {
        if (source.tableID != tableID || pk.size != 1 || pk.keys.singleOrNull() != source.pkFieldID) {
            throw SynchroError.InvalidResponse("push outcome does not match the local mutation ledger")
        }
        val id = recordID(pk.getValue(source.pkFieldID), source.pkLogicalType)
        if (id != source.recordID) {
            throw SynchroError.InvalidResponse("push outcome has a different typed primary key")
        }
        return id
    }

    private fun recordAcceptedOutcome(db: SQLiteDatabase, source: PendingChange, outcomeJSON: String): Boolean =
        recordTerminalOutcome(db, source, "accepted", "accepted_outcome_json", outcomeJSON)

    private fun recordRejectedOutcome(
        db: SQLiteDatabase,
        source: PendingChange,
        outcome: RejectedMutation,
        outcomeJSON: String,
    ): Boolean = recordTerminalOutcome(
        db,
        source,
        when (outcome.status) {
            MutationStatus.CONFLICT -> "conflict"
            MutationStatus.REJECTED_TERMINAL -> "rejected_terminal"
            MutationStatus.APPLIED -> throw SynchroError.InvalidResponse("rejected outcome has applied status")
        },
        "rejected_outcome_json",
        outcomeJSON,
    )

    /** Returns false only for an exact raw replay of an already recorded outcome. */
    private fun recordTerminalOutcome(
        db: SQLiteDatabase,
        source: PendingChange,
        expectedState: String,
        column: String,
        value: String,
    ): Boolean {
        val stored = db.rawQuery(
            "SELECT lifecycle_state, accepted_outcome_json, rejected_outcome_json FROM _synchro_pending_changes WHERE mutation_id = ?",
            arrayOf(source.mutationID),
        ).use { cursor ->
            if (!cursor.moveToFirst()) throw SynchroError.InvalidResponse("mutation ledger record disappeared")
            Triple(
                cursor.getString(0),
                if (cursor.isNull(1)) null else cursor.getString(1),
                if (cursor.isNull(2)) null else cursor.getString(2),
            )
        }
        val prior = if (column == "accepted_outcome_json") stored.second else stored.third
        if (stored.first == expectedState && prior == value) return false
        if (stored.first in setOf("accepted", "conflict", "rejected_terminal") || prior != null) {
            throw SynchroError.InvalidResponse("mutation ledger has a different terminal outcome")
        }
        db.execSQL(
            """
            UPDATE _synchro_pending_changes
            SET lifecycle_state = ?, $column = ?,
                updated_at = substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z'
            WHERE mutation_id = ?
            """.trimIndent(),
            arrayOf(expectedState, value, source.mutationID),
        )
        requireExactlyOneChange(db, "mutation outcome was not durable")
        return true
    }

    private fun laterUnresolvedIntents(db: SQLiteDatabase, source: PendingChange): List<PendingChange> {
        val ids = mutableListOf<String>()
        db.rawQuery(
            """
            SELECT mutation_id
            FROM _synchro_pending_changes
            WHERE table_id = ? AND pk_field_id = ? AND pk_logical_type = ? AND record_id = ?
              AND local_order > ?
              AND lifecycle_state IN ('captured', 'sealed', 'legacy_blocked', 'blocked_by_predecessor')
            ORDER BY local_order
            """.trimIndent(),
            arrayOf(
                source.tableID,
                source.pkFieldID,
                source.pkLogicalType,
                source.recordID,
                source.localOrder.toString(),
            ),
        ).use { cursor -> while (cursor.moveToNext()) ids += cursor.getString(0) }
        return ids.map { id ->
            changeTracker.changeByID(db, id)
                ?: throw SynchroError.InvalidResponse("later mutation ledger record disappeared")
        }
    }

    private fun currentTableForSource(
        source: PendingChange,
        syncedTables: List<LocalSchemaTable>,
    ): LocalSchemaTable? = syncedTables.singleOrNull { table ->
        table.tableID == source.tableID && table.primaryKeyFieldID == source.pkFieldID &&
            table.columns.singleOrNull { it.fieldID == source.pkFieldID }?.logicalType == source.pkLogicalType
    }

    private fun historicalTableForOutcome(
        db: SQLiteDatabase,
        outcomeSchema: SchemaRef,
        tableID: String,
        sealedSchemas: Map<SchemaRef, List<LocalSchemaTable>>,
    ): LocalSchemaTable {
        val tables = sealedSchemas[outcomeSchema]
            ?: schemaTablesForReference(db, outcomeSchema, null)
            ?: throw SynchroError.InvalidResponse("outcome schema is not retained")
        return tables.singleOrNull { it.tableID == tableID }
            ?: throw SynchroError.InvalidResponse("outcome schema does not retain its logical table")
    }

    private fun verifyAuthoritativeChecksum(
        row: JsonObject,
        pk: JsonObject,
        checksum: ChecksumObject?,
        serverVersion: String,
        outcomeSchema: SchemaRef,
        historicalTable: LocalSchemaTable,
    ) {
        val expected = checksum ?: throw SynchroError.InvalidResponse("authoritative row is missing its checksum")
        val computed = try {
            Integrity.rowDigest(outcomeSchema.hash, historicalTable, pk, row, serverVersion).checksum
        } catch (_: IllegalArgumentException) {
            throw SynchroError.InvalidResponse("authoritative row is invalid for its outcome schema")
        }
        if (computed != expected) throw SynchroError.InvalidResponse("authoritative row checksum mismatch")
    }

    private fun projectAuthoritativeRow(
        historical: LocalSchemaTable,
        current: LocalSchemaTable?,
        source: PendingChange,
        pk: JsonObject,
        row: JsonObject,
    ): AuthoritativeProjection? {
        if (current == null || historical.tableID != source.tableID ||
            historical.primaryKeyFieldID != source.pkFieldID || current.primaryKeyFieldID != source.pkFieldID ||
            pk.keys.singleOrNull() != source.pkFieldID
        ) {
            return null
        }
        val historicalPK = historical.columns.singleOrNull { it.fieldID == historical.primaryKeyFieldID }
        val currentPK = current.columns.singleOrNull { it.fieldID == current.primaryKeyFieldID }
        if (historicalPK?.logicalType != source.pkLogicalType || currentPK?.logicalType != source.pkLogicalType ||
            current.primaryKey.size != 1
        ) {
            return null
        }
        val historicalByID = historical.columns.associateBy { it.fieldID }
        val projected = mutableListOf<Pair<LocalSchemaColumn, JsonElement>>()
        for (column in current.columns) {
            val historicalColumn = historicalByID[column.fieldID] ?: continue
            if (historicalColumn.logicalType != column.logicalType) return null
            val value = row[column.fieldID] ?: return null
            projected += column to value
        }
        if (projected.none { it.first.fieldID == current.primaryKeyFieldID }) return null
        return AuthoritativeProjection(current, projected)
    }

    private fun prepareLocalPatches(
        db: SQLiteDatabase,
        current: LocalSchemaTable,
        source: PendingChange,
        later: List<PendingChange>,
    ): List<LocalPatch>? {
        val patches = mutableListOf<LocalPatch>()
        val columnsByID = current.columns.associateBy { it.fieldID }
        later.forEach { intent ->
            if (intent.tableID != source.tableID || intent.pkFieldID != source.pkFieldID ||
                intent.pkLogicalType != source.pkLogicalType || intent.recordID != source.recordID
            ) {
                return null
            }
            val values = when (intent.operation) {
                "insert", "update" -> {
                    val authored = changeTracker.valuesForMutation(db, intent.mutationID)
                    if (authored.isEmpty()) return null
                    authored.map { value ->
                        val column = columnsByID[value.fieldID] ?: return null
                        if (column.logicalType != value.logicalType) return null
                        val element = try {
                            value.toJson()
                        } catch (_: SynchroError.InvalidResponse) {
                            throw SynchroError.InvalidResponse("later local patch has an invalid portable value")
                        }
                        try {
                            Integrity.validateTypedValue(element, column)
                        } catch (_: IllegalArgumentException) {
                            throw SynchroError.InvalidResponse("later local patch has an invalid portable value")
                        }
                        column to element
                    }
                }
                "delete" -> emptyList()
                else -> return null
            }
            val deletedAtValue = if (intent.operation == "delete" && current.deletedAtColumn.isNotEmpty()) {
                readColumnValue(db, current, source.recordID, source.pkLogicalType, current.deletedAtColumn)
            } else {
                null
            }
            patches += LocalPatch(intent, values, deletedAtValue)
        }
        return patches
    }

    private fun applyAuthoritativeProjection(
        db: SQLiteDatabase,
        projection: AuthoritativeProjection,
    ) {
        val table = projection.currentTable
        val pkColumn = table.primaryKey.singleOrNull()
            ?: throw SynchroError.InvalidResponse("local table has no single primary key")
        val relation = SQLiteHelpers.quoteIdentifier(table.tableName)
        val quotedPK = SQLiteHelpers.quoteIdentifier(pkColumn)
        val columns = projection.values.map { it.first.name }
        val values = projection.values.map { (column, element) -> databaseValue(element, column) }
        val pkIndex = columns.indexOf(pkColumn)
        if (pkIndex < 0) throw SynchroError.InvalidResponse("projection omits the local primary key")
        val dataIndexes = columns.indices.filter { it != pkIndex }
        executeUpsert(
            db = db,
            table = relation,
            keyColumns = listOf(quotedPK),
            keyValues = listOf(values[pkIndex]),
            dataColumns = dataIndexes.map { SQLiteHelpers.quoteIdentifier(columns[it]) },
            dataValues = dataIndexes.map(values::get),
        )
    }

    private fun applyAuthoritativeAbsence(
        db: SQLiteDatabase,
        table: LocalSchemaTable,
        recordID: String,
        pkLogicalType: String,
    ) {
        val pkColumn = table.primaryKey.singleOrNull()
            ?: throw SynchroError.InvalidResponse("local table has no single primary key")
        executeWithTypedBindings(
            db,
            "DELETE FROM ${SQLiteHelpers.quoteIdentifier(table.tableName)} WHERE ${SQLiteHelpers.quoteIdentifier(pkColumn)} = ?",
            listOf(primaryKeyDatabaseValue(recordID, pkLogicalType)),
        )
    }

    private fun applyLocalPatches(
        db: SQLiteDatabase,
        table: LocalSchemaTable,
        recordID: String,
        pkLogicalType: String,
        patches: List<LocalPatch>,
    ) {
        val pkColumn = table.primaryKey.singleOrNull()
            ?: throw SynchroError.InvalidResponse("local table has no single primary key")
        patches.forEach { patch ->
            when (patch.change.operation) {
                "insert", "update" -> {
                    val assignments = patch.values.joinToString(", ") { (column, _) ->
                        "${SQLiteHelpers.quoteIdentifier(column.name)} = ?"
                    }
                    if (assignments.isNotEmpty()) {
                        executeWithTypedBindings(
                            db,
                            "UPDATE ${SQLiteHelpers.quoteIdentifier(table.tableName)} SET $assignments WHERE ${SQLiteHelpers.quoteIdentifier(pkColumn)} = ?",
                            patch.values.map { (column, value) -> databaseValue(value, column) } +
                                primaryKeyDatabaseValue(recordID, pkLogicalType),
                        )
                    }
                }
                "delete" -> {
                    if (table.deletedAtColumn.isNotEmpty() && patch.deletedAtValue != null) {
                        executeWithTypedBindings(
                            db,
                            "UPDATE ${SQLiteHelpers.quoteIdentifier(table.tableName)} SET ${SQLiteHelpers.quoteIdentifier(table.deletedAtColumn)} = ? WHERE ${SQLiteHelpers.quoteIdentifier(pkColumn)} = ?",
                            listOf(patch.deletedAtValue, primaryKeyDatabaseValue(recordID, pkLogicalType)),
                        )
                    } else {
                        applyAuthoritativeAbsence(db, table, recordID, pkLogicalType)
                    }
                }
            }
        }
    }

    private fun readColumnValue(
        db: SQLiteDatabase,
        table: LocalSchemaTable,
        recordID: String,
        pkLogicalType: String,
        column: String,
    ): Any? {
        val pkColumn = table.primaryKey.singleOrNull() ?: return null
        return queryOneWithTypedBindings(
            db,
            "SELECT ${SQLiteHelpers.quoteIdentifier(column)} FROM ${SQLiteHelpers.quoteIdentifier(table.tableName)} WHERE ${SQLiteHelpers.quoteIdentifier(pkColumn)} = ?",
            arrayOf(primaryKeyDatabaseValue(recordID, pkLogicalType)),
        )?.get(column)
    }

    private fun databaseValue(value: JsonElement, column: LocalSchemaColumn): Any? {
        if (value is JsonNull) return null
        val primitive = value as? JsonPrimitive
            ?: throw SynchroError.InvalidResponse("portable value is not a primitive")
        return when (column.logicalType) {
            "boolean" -> when (primitive.content) {
                "true" -> 1L
                "false" -> 0L
                else -> throw SynchroError.InvalidResponse("portable Boolean value is invalid")
            }
            "int", "int64" -> primitive.content.toLongOrNull()
                ?: throw SynchroError.InvalidResponse("portable integer value is invalid")
            "float" -> primitive.content.toDoubleOrNull()?.takeIf { it.isFinite() }
                ?: throw SynchroError.InvalidResponse("portable floating-point value is invalid")
            "bytes" -> try {
                Base64.decode(primitive.content, Base64.URL_SAFE or Base64.NO_WRAP or Base64.NO_PADDING)
            } catch (_: IllegalArgumentException) {
                throw SynchroError.InvalidResponse("portable bytes value is invalid")
            }
            else -> primitive.takeIf { it.isString }?.content
                ?: throw SynchroError.InvalidResponse("portable text value is invalid")
        }
    }

    private fun invalidateAffectedScopes(db: SQLiteDatabase, recordID: String, tableNames: List<String>) {
        val scopes = mutableSetOf<String>()
        if (tableNames.isNotEmpty()) {
            val placeholders = SQLiteHelpers.placeholders(tableNames.size)
            db.rawQuery(
                "SELECT DISTINCT scope_id FROM _synchro_scope_rows WHERE record_id = ? AND table_name IN ($placeholders)",
                arrayOf(recordID, *tableNames.toTypedArray()),
            ).use { cursor -> while (cursor.moveToNext()) scopes += cursor.getString(0) }
        }
        if (scopes.isEmpty()) {
            SynchroMeta.invalidateAllScopes(db)
        } else {
            scopes.forEach { SynchroMeta.bumpScopeGeneration(db, it) }
        }
    }

    /** Only unsealed update and delete successors can receive a returned server base. */
    private fun refreshUnsealedSuccessors(db: SQLiteDatabase, mutationID: String, serverVersion: String) {
        db.execSQL(
            """
            UPDATE _synchro_pending_changes
            SET base_version = ?, depends_on_mutation_id = NULL,
                updated_at = substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z'
            WHERE depends_on_mutation_id = ? AND lifecycle_state = 'captured'
              AND operation IN ('update', 'delete')
            """.trimIndent(),
            arrayOf(serverVersion, mutationID),
        )
    }

    private fun blockUnsealedDependents(db: SQLiteDatabase, mutationID: String) {
        db.execSQL(
            """
            WITH RECURSIVE descendants(mutation_id) AS (
                SELECT mutation_id FROM _synchro_pending_changes
                WHERE depends_on_mutation_id = ?
                  AND lifecycle_state IN ('captured', 'sealed', 'legacy_blocked', 'blocked_by_predecessor')
                UNION
                SELECT child.mutation_id
                FROM _synchro_pending_changes child
                JOIN descendants parent ON child.depends_on_mutation_id = parent.mutation_id
                WHERE child.lifecycle_state IN ('captured', 'sealed', 'legacy_blocked', 'blocked_by_predecessor')
            )
            UPDATE _synchro_pending_changes
            SET lifecycle_state = 'blocked_by_predecessor',
                updated_at = substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z'
            WHERE mutation_id IN descendants
              AND lifecycle_state IN ('captured', 'sealed', 'legacy_blocked', 'blocked_by_predecessor')
            """.trimIndent(),
            arrayOf(mutationID),
        )
    }

    private fun completeBatchInTransaction(db: SQLiteDatabase, batchID: String) {
        db.execSQL(
            """
            UPDATE _synchro_push_batches
            SET state = 'completed',
                completed_at = substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z'
            WHERE batch_id = ? AND state = 'pending'
            """.trimIndent(),
            arrayOf(batchID),
        )
        requireExactlyOneChange(db, "sealed push batch completion was not durable")
    }

    /**
     * Extracts an array's object members without serializing them again.
     * HttpClient validates the full body before this parser receives it.
     */
    private fun exactObjectJSONMap(
        source: String,
        member: String,
        mutationID: (String) -> String,
    ): Map<String, String> {
        data class Cursor(var index: Int = 0)
        val cursor = Cursor()
        fun skipWhitespace() {
            while (cursor.index < source.length && source[cursor.index] in " \t\r\n") cursor.index += 1
        }
        fun skipString() {
            if (cursor.index >= source.length || source[cursor.index] != '"') {
                throw SynchroError.InvalidResponse("push JSON string is invalid")
            }
            cursor.index += 1
            while (cursor.index < source.length) {
                when (source[cursor.index]) {
                    '\\' -> cursor.index += 2
                    '"' -> {
                        cursor.index += 1
                        return
                    }
                    else -> cursor.index += 1
                }
            }
            throw SynchroError.InvalidResponse("push JSON string is unterminated")
        }
        fun skipValue() {
            skipWhitespace()
            if (cursor.index >= source.length) throw SynchroError.InvalidResponse("push JSON value is missing")
            when (source[cursor.index]) {
                '"' -> skipString()
                '{' -> {
                    cursor.index += 1
                    skipWhitespace()
                    if (cursor.index < source.length && source[cursor.index] == '}') {
                        cursor.index += 1
                        return
                    }
                    while (true) {
                        skipWhitespace()
                        skipString()
                        skipWhitespace()
                        if (cursor.index >= source.length || source[cursor.index] != ':') {
                            throw SynchroError.InvalidResponse("push JSON object is invalid")
                        }
                        cursor.index += 1
                        skipValue()
                        skipWhitespace()
                        when {
                            cursor.index < source.length && source[cursor.index] == ',' -> cursor.index += 1
                            cursor.index < source.length && source[cursor.index] == '}' -> {
                                cursor.index += 1
                                return
                            }
                            else -> throw SynchroError.InvalidResponse("push JSON object is invalid")
                        }
                    }
                }
                '[' -> {
                    cursor.index += 1
                    skipWhitespace()
                    if (cursor.index < source.length && source[cursor.index] == ']') {
                        cursor.index += 1
                        return
                    }
                    while (true) {
                        skipValue()
                        skipWhitespace()
                        when {
                            cursor.index < source.length && source[cursor.index] == ',' -> cursor.index += 1
                            cursor.index < source.length && source[cursor.index] == ']' -> {
                                cursor.index += 1
                                return
                            }
                            else -> throw SynchroError.InvalidResponse("push JSON array is invalid")
                        }
                    }
                }
                else -> {
                    while (cursor.index < source.length && source[cursor.index] !in " \t\r\n,]}") cursor.index += 1
                }
            }
        }

        skipWhitespace()
        if (cursor.index >= source.length || source[cursor.index] != '{') {
            throw SynchroError.InvalidResponse("push JSON is not an object")
        }
        cursor.index += 1
        val exact = linkedMapOf<String, String>()
        var found = false
        while (true) {
            skipWhitespace()
            if (cursor.index < source.length && source[cursor.index] == '}') {
                cursor.index += 1
                break
            }
            val keyStart = cursor.index
            skipString()
            val key = try {
                json.decodeFromString<String>(source.substring(keyStart, cursor.index))
            } catch (_: Exception) {
                throw SynchroError.InvalidResponse("push JSON member name is invalid")
            }
            skipWhitespace()
            if (cursor.index >= source.length || source[cursor.index] != ':') {
                throw SynchroError.InvalidResponse("push JSON member lacks a colon")
            }
            cursor.index += 1
            skipWhitespace()
            if (key == member) {
                if (found || cursor.index >= source.length || source[cursor.index] != '[') {
                    throw SynchroError.InvalidResponse("push JSON member is invalid")
                }
                found = true
                cursor.index += 1
                skipWhitespace()
                while (cursor.index < source.length && source[cursor.index] != ']') {
                    val valueStart = cursor.index
                    skipValue()
                    val raw = source.substring(valueStart, cursor.index)
                    val id = try {
                        mutationID(raw)
                    } catch (_: Exception) {
                        throw SynchroError.InvalidResponse("push JSON outcome is invalid")
                    }
                    if (exact.put(id, raw) != null) {
                        throw SynchroError.InvalidResponse("push JSON has duplicate mutation outcomes")
                    }
                    skipWhitespace()
                    if (cursor.index < source.length && source[cursor.index] == ',') {
                        cursor.index += 1
                        skipWhitespace()
                    } else if (cursor.index >= source.length || source[cursor.index] != ']') {
                        throw SynchroError.InvalidResponse("push JSON array is invalid")
                    }
                }
                if (cursor.index >= source.length || source[cursor.index] != ']') {
                    throw SynchroError.InvalidResponse("push JSON array is unterminated")
                }
                cursor.index += 1
            } else {
                skipValue()
            }
            skipWhitespace()
            when {
                cursor.index < source.length && source[cursor.index] == ',' -> cursor.index += 1
                cursor.index < source.length && source[cursor.index] == '}' -> {
                    cursor.index += 1
                    break
                }
                else -> throw SynchroError.InvalidResponse("push JSON object is invalid")
            }
        }
        skipWhitespace()
        if (!found || cursor.index != source.length) {
            throw SynchroError.InvalidResponse("push JSON lacks its required member")
        }
        return exact
    }

    private fun insertLedgerMutation(
        db: SQLiteDatabase,
        mutationID: String,
        tableID: String,
        tableName: String,
        recordID: String,
        pkFieldID: String,
        pkLogicalType: String,
        operation: String,
        authoredSchema: SchemaRef,
        baseVersion: String?,
        clientVersion: String,
        lifecycleState: String,
        sourceKind: String,
        dependsOnMutationID: String?,
        normalizedMutationID: String?,
        sealedBatchID: String?,
        sealedOrdinal: Int?,
        values: List<LedgerValue>,
    ) {
        db.execSQL(
            """
            INSERT INTO _synchro_pending_changes (
                mutation_id, table_id, table_name, record_id, pk_field_id, pk_logical_type,
                operation, authored_schema_version, authored_schema_hash, base_version, client_version,
                lifecycle_state, source_kind, depends_on_mutation_id, normalized_mutation_id,
                sealed_batch_id, sealed_ordinal, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z',
                substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z')
            """.trimIndent(),
            arrayOf(
                mutationID, tableID, tableName, recordID, pkFieldID, pkLogicalType, operation,
                authoredSchema.version, authoredSchema.hash, baseVersion, clientVersion, lifecycleState,
                sourceKind, dependsOnMutationID, normalizedMutationID, sealedBatchID, sealedOrdinal,
            ),
        )
        values.forEach { value ->
            executeWithTypedBindings(
                db,
                """
                INSERT INTO _synchro_mutation_values
                    (mutation_id, field_id, logical_type, value_kind, value_integer, value_real, value_text, value_blob)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """.trimIndent(),
                listOf(
                    mutationID, value.fieldID, value.logicalType, value.valueKind,
                    value.integerValue, value.realValue, value.textValue, value.blobValue,
                ),
            )
        }
    }

    private fun requireExactlyOneChange(db: SQLiteDatabase, message: String) {
        val count = db.rawQuery("SELECT changes()", null).use { cursor ->
            if (cursor.moveToFirst()) cursor.getInt(0) else 0
        }
        if (count != 1) throw SynchroError.InvalidResponse(message)
    }

    private fun primaryKeyElement(recordID: String, logicalType: String): JsonPrimitive = when (logicalType) {
        "string" -> if (Integrity.isValidText(recordID)) JsonPrimitive(recordID) else {
            throw SynchroError.InvalidResponse("stored string primary key is invalid")
        }
        "int" -> recordID.takeIf { it.matches(Regex("0|-?[1-9][0-9]*")) }?.toIntOrNull()?.let(::JsonPrimitive)
            ?: throw SynchroError.InvalidResponse("stored integer primary key is invalid")
        "int64" -> if (recordID.matches(Regex("0|-?[1-9][0-9]*")) && recordID.toLongOrNull() != null) {
            JsonPrimitive(recordID)
        } else {
            throw SynchroError.InvalidResponse("stored int64 primary key is invalid")
        }
        else -> throw SynchroError.InvalidResponse("stored primary key type is unsupported")
    }

    private fun inferredPrimaryKeyType(value: JsonElement): String = when (value) {
        is JsonPrimitive -> when {
            value.isString -> "string"
            value.content.matches(Regex("0|-?[1-9][0-9]*")) && value.content.toIntOrNull() != null -> "int"
            else -> throw SynchroError.InvalidResponse("legacy primary key has an invalid type")
        }
        else -> throw SynchroError.InvalidResponse("legacy primary key has an invalid type")
    }

    private fun recordID(value: JsonElement, logicalType: String): String {
        val primitive = value as? JsonPrimitive
            ?: throw SynchroError.InvalidResponse("primary key is not a primitive")
        return when (logicalType) {
            "string" -> primitive.takeIf { it.isString && Integrity.isValidText(it.content) }?.content
                ?: throw SynchroError.InvalidResponse("string primary key is invalid")
            "int" -> primitive.takeIf { !it.isString && it.content.matches(Regex("0|-?[1-9][0-9]*")) }
                ?.content?.toIntOrNull()?.toString()
                ?: throw SynchroError.InvalidResponse("integer primary key is invalid")
            "int64" -> primitive.takeIf { it.isString && it.content.matches(Regex("0|-?[1-9][0-9]*")) }
                ?.content?.toLongOrNull()?.toString()
                ?: throw SynchroError.InvalidResponse("int64 primary key is invalid")
            else -> throw SynchroError.InvalidResponse("primary key has an unsupported type")
        }
    }

    private fun primaryKeyDatabaseValue(recordID: String, logicalType: String): Any = when (logicalType) {
        "string" -> recordID
        "int", "int64" -> recordID.toLongOrNull()
            ?: throw SynchroError.InvalidResponse("stored integer primary key is invalid")
        else -> throw SynchroError.InvalidResponse("stored primary key type is unsupported")
    }

    private fun Operation.wireName(): String = when (this) {
        Operation.INSERT -> "insert"
        Operation.UPDATE -> "update"
        Operation.DELETE -> "delete"
        Operation.UPSERT -> throw SynchroError.InvalidResponse("upsert is not a push operation")
    }

    private fun LedgerValue.toJson(): JsonElement = when (valueKind) {
        "null" -> JsonNull
        "boolean" -> {
            if (logicalType != "boolean" || integerValue !in setOf(0L, 1L)) {
                throw SynchroError.InvalidResponse("stored Boolean value is invalid")
            }
            JsonPrimitive(integerValue == 1L)
        }
        "integer" -> when (logicalType) {
            "int" -> integerValue?.takeIf { it in Int.MIN_VALUE.toLong()..Int.MAX_VALUE.toLong() }?.toInt()?.let(::JsonPrimitive)
                ?: throw SynchroError.InvalidResponse("stored integer value is invalid")
            "int64" -> JsonPrimitive(integerValue?.toString() ?: throw SynchroError.InvalidResponse("stored int64 value is invalid"))
            else -> throw SynchroError.InvalidResponse("stored integer value has an invalid type")
        }
        "real" -> {
            val value = realValue?.takeIf { it.isFinite() }
                ?: throw SynchroError.InvalidResponse("stored real value is invalid")
            if (logicalType != "float") throw SynchroError.InvalidResponse("stored real value has an invalid type")
            JsonPrimitive(value)
        }
        "text" -> {
            if (logicalType !in setOf("string", "decimal", "datetime", "date", "time", "json")) {
                throw SynchroError.InvalidResponse("stored text value has an invalid type")
            }
            JsonPrimitive(textValue ?: throw SynchroError.InvalidResponse("stored text value is invalid"))
        }
        "blob" -> {
            if (logicalType != "bytes") throw SynchroError.InvalidResponse("stored blob value has an invalid type")
            JsonPrimitive(
                Base64.encodeToString(
                    blobValue ?: throw SynchroError.InvalidResponse("stored blob value is invalid"),
                    Base64.URL_SAFE or Base64.NO_WRAP or Base64.NO_PADDING,
                )
            )
        }
        else -> throw SynchroError.InvalidResponse("stored mutation value has an invalid kind")
    }

    private fun ledgerValueFromJson(fieldID: String, logicalType: String, value: JsonElement): LedgerValue {
        if (value is JsonNull) return LedgerValue(fieldID, logicalType, "null", null, null, null, null)
        val primitive = value as? JsonPrimitive
            ?: throw SynchroError.InvalidResponse("legacy mutation field is not a portable value")
        return when (logicalType) {
            "boolean" -> when {
                primitive.isString || primitive.content !in setOf("true", "false") ->
                    throw SynchroError.InvalidResponse("legacy Boolean field is invalid")
                else -> LedgerValue(fieldID, logicalType, "boolean", if (primitive.content == "true") 1L else 0L, null, null, null)
            }
            "int" -> primitive.takeIf { !it.isString && it.content.matches(Regex("0|-?[1-9][0-9]*")) }
                ?.content?.toLongOrNull()?.takeIf { it in Int.MIN_VALUE.toLong()..Int.MAX_VALUE.toLong() }
                ?.let { LedgerValue(fieldID, logicalType, "integer", it, null, null, null) }
                ?: throw SynchroError.InvalidResponse("legacy int field is invalid")
            "int64" -> primitive.takeIf { it.isString && it.content.matches(Regex("0|-?[1-9][0-9]*")) }
                ?.content?.toLongOrNull()
                ?.let { LedgerValue(fieldID, logicalType, "integer", it, null, null, null) }
                ?: throw SynchroError.InvalidResponse("legacy int64 field is invalid")
            "float" -> primitive.takeIf { !it.isString }?.content?.toDoubleOrNull()?.takeIf { it.isFinite() }
                ?.let { LedgerValue(fieldID, logicalType, "real", null, it, null, null) }
                ?: throw SynchroError.InvalidResponse("legacy float field is invalid")
            "bytes" -> LedgerValue(
                fieldID,
                logicalType,
                "blob",
                null,
                null,
                null,
                runCatching {
                    if (!primitive.isString || '=' in primitive.content) throw IllegalArgumentException()
                    val decoded = Base64.decode(primitive.content, Base64.URL_SAFE or Base64.NO_WRAP or Base64.NO_PADDING)
                    if (Base64.encodeToString(decoded, Base64.URL_SAFE or Base64.NO_WRAP or Base64.NO_PADDING) != primitive.content) {
                        throw IllegalArgumentException()
                    }
                    decoded
                }
                    .getOrElse { throw SynchroError.InvalidResponse("legacy bytes field is invalid") },
            )
            else -> LedgerValue(fieldID, logicalType, "text", null, null, primitive.content, null)
        }
    }

    private fun fromJsonElement(element: JsonElement): Any? = when (element) {
        JsonNull -> null
        is JsonPrimitive -> when {
            element.isString -> element.content
            element.content == "true" -> true
            element.content == "false" -> false
            element.content.contains('.') -> element.content.toDoubleOrNull()
            else -> element.content.toLongOrNull() ?: element.content
        }
        is JsonArray -> element.map(::fromJsonElement)
        is JsonObject -> element.mapValues { (_, value) -> fromJsonElement(value) }
    }
}
