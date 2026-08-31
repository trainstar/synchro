package com.trainstar.synchro

import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteProgram
import android.util.Base64
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.intOrNull
import java.util.UUID

internal class RebuildChecksumMismatchException(val scopeID: String) : Exception("rebuild checksum mismatch")

internal data class PendingRebuildFinality(
    val finalCursor: String,
    val checksum: ChecksumObject,
)

internal class PullProcessor(private val database: SynchroDatabase) {
    @OptIn(ExperimentalSerializationApi::class)
    private val rebuildJSON = Json {
        ignoreUnknownKeys = true
        encodeDefaults = true
        explicitNulls = false
    }
    private val seedReceiptJSON = Json { ignoreUnknownKeys = false }

    fun updateCheckpoint(checkpoint: Long) {
        database.writeTransaction { db ->
            val current = SynchroMeta.getInt64(db, MetaKey.CHECKPOINT)
            if (checkpoint > current) {
                SynchroMeta.setInt64(db, MetaKey.CHECKPOINT, checkpoint)
            }
        }
    }

    fun applyScopeChanges(
        changes: List<ChangeRecord>,
        syncedTables: List<LocalSchemaTable>,
        scopeCursors: Map<String, String>,
        checksums: Map<String, ChecksumObject>?,
        schemaHash: String,
        scopeUpdates: ScopeAssignmentDelta = ScopeAssignmentDelta(emptyList(), emptyList()),
        scopeSetVersion: Long? = null,
        rebuildScopes: Set<String> = emptySet(),
    ) = applyScopeChangesInTransaction(
        changes,
        syncedTables,
        scopeCursors,
        checksums,
        schemaHash,
        scopeUpdates,
        scopeSetVersion,
        rebuildScopes,
        resolvedRequestJSON = null,
    )

    internal fun applyScopeChangesResolvingBackoff(
        changes: List<ChangeRecord>,
        syncedTables: List<LocalSchemaTable>,
        scopeCursors: Map<String, String>,
        checksums: Map<String, ChecksumObject>?,
        schemaHash: String,
        scopeUpdates: ScopeAssignmentDelta,
        scopeSetVersion: Long,
        rebuildScopes: Set<String>,
        requestJSON: String,
    ) = applyScopeChangesInTransaction(
        changes,
        syncedTables,
        scopeCursors,
        checksums,
        schemaHash,
        scopeUpdates,
        scopeSetVersion,
        rebuildScopes,
        resolvedRequestJSON = requestJSON,
    )

    private fun applyScopeChangesInTransaction(
        changes: List<ChangeRecord>,
        syncedTables: List<LocalSchemaTable>,
        scopeCursors: Map<String, String>,
        checksums: Map<String, ChecksumObject>?,
        schemaHash: String,
        scopeUpdates: ScopeAssignmentDelta,
        scopeSetVersion: Long?,
        rebuildScopes: Set<String>,
        resolvedRequestJSON: String?,
    ) {
        val checksumMap = checksums ?: emptyMap()
        if (changes.isEmpty() && scopeCursors.isEmpty() && checksumMap.isEmpty() &&
            scopeUpdates.add.isEmpty() && scopeUpdates.remove.isEmpty() && scopeSetVersion == null &&
            resolvedRequestJSON == null
        ) return
        val tablesByID = syncedTables.associateBy { it.tableID }
        val tablesByName = syncedTables.associateBy { it.tableName }

        database.writeSyncLockedTransaction { db ->
            for (change in changes) {
                val schema = tablesByID[change.table]
                    ?: throw SynchroError.InvalidResponse("unknown logical table ${change.table}")
                val recordId = scopeRecordID(change.pk, schema)

                when (change.op) {
                    Operation.INSERT, Operation.UPDATE -> {
                        throw SynchroError.InvalidResponse("invalid pull operation ${change.op}")
                    }
                    Operation.DELETE -> applyScopeDeleteChange(db, change, recordId, schema, schemaHash)
                    Operation.UPSERT -> {
                        val localRow = validatedLocalRow(
                            change.table,
                            recordId,
                            change.pk,
                            change.row,
                            change.rowChecksum,
                            change.serverVersion,
                            schemaHash,
                            schema,
                        )
                        if (!isApplicationRowProtected(db, schema.tableName, recordId)) {
                            upsertRecord(db, recordId, localRow, schema)
                        }
                        val rowChecksum = change.rowChecksum
                            ?: throw SynchroError.InvalidResponse("missing row checksum for ${change.table}/$recordId")
                        SynchroMeta.upsertRowVersion(
                            db,
                            schema.tableName,
                            recordId,
                            change.serverVersion,
                            rowChecksum,
                        )
                        val generation = SynchroMeta.getScopeGeneration(db, change.scope)
                        SynchroMeta.upsertScopeRow(
                            db,
                            change.scope,
                            schema.tableName,
                            recordId,
                            requiredScopeRowChecksum(change.rowChecksum, change.table, recordId),
                            generation
                        )
                    }
                }
            }

            for (scopeId in scopeUpdates.remove) {
                removeScope(db, scopeId, tablesByName)
            }
            for (scope in scopeUpdates.add) {
                SynchroMeta.upsertScope(
                    db,
                    scopeId = scope.id,
                    cursor = null,
                    checksum = null,
                )
            }

            val scopeIds = (scopeCursors.keys + checksumMap.keys).toSet()
            for (scopeId in scopeIds) {
                val existingScope = SynchroMeta.getScope(db, scopeId) ?: continue
                val nextCursor = scopeCursors[scopeId] ?: existingScope.cursor
                val localChecksum = computeScopeChecksum(db, scopeId, schemaHash, tablesByName)
                if (scopeId in rebuildScopes) {
                    SynchroMeta.upsertScope(
                        db,
                        scopeId = scopeId,
                        cursor = null,
                        checksum = null,
                        generation = existingScope.generation,
                        localChecksum = checksumJSON(localChecksum),
                    )
                    continue
                }
                val serverChecksum = checksumMap[scopeId]
                if (serverChecksum != null) {
                    serverChecksum.validate()
                    val localChecksumJSON = checksumJSON(localChecksum)
                    val serverChecksumJSON = checksumJSON(serverChecksum)
                    if (localChecksum == serverChecksum) {
                        SynchroMeta.upsertScope(
                            db,
                            scopeId = scopeId,
                            cursor = nextCursor,
                            checksum = serverChecksumJSON,
                            generation = existingScope.generation,
                            localChecksum = localChecksumJSON
                        )
                    } else {
                        SynchroMeta.upsertScope(
                            db,
                            scopeId = scopeId,
                            cursor = null,
                            checksum = null,
                            generation = existingScope.generation,
                            localChecksum = localChecksumJSON
                        )
                    }
                    continue
                }
                SynchroMeta.upsertScope(
                    db,
                    scopeId = scopeId,
                    cursor = nextCursor,
                    checksum = existingScope.checksum,
                    generation = existingScope.generation,
                    localChecksum = checksumJSON(localChecksum)
                )
            }
            scopeSetVersion?.let { SynchroMeta.setInt64(db, MetaKey.SCOPE_SET_VERSION, it) }
            resolvedRequestJSON?.let { requestJSON ->
                DurableBackoffStore.clearMatching(db, RetryOperation.PULLING, requestJSON)
            }
        }
    }

    fun beginScopeRebuild(
        scopeId: String,
        clientGeneration: Long,
        schemaVersion: Long,
        schemaHash: String,
        pageLimit: Int,
    ): LocalRebuildAttempt {
        return database.writeSyncLockedTransaction { db ->
            if (SynchroMeta.getScope(db, scopeId) == null) {
                throw SynchroError.InvalidResponse("rebuild targets an unknown scope $scopeId")
            }
            val scopeGeneration = SynchroMeta.getScopeGeneration(db, scopeId)
            val existing = SynchroMeta.getRebuildAttempt(db, scopeId)
            if (existing != null &&
                existing.clientGeneration == clientGeneration &&
                existing.schemaVersion == schemaVersion &&
                existing.schemaHash == schemaHash &&
                existing.pageLimit == pageLimit &&
                existing.generation == scopeGeneration
            ) {
                return@writeSyncLockedTransaction existing
            }
            startScopeRebuildAttempt(
                db,
                scopeId,
                clientGeneration,
                schemaVersion,
                schemaHash,
                pageLimit,
            )
        }
    }

    fun restartScopeRebuild(
        scopeId: String,
        clientGeneration: Long,
        schemaVersion: Long,
        schemaHash: String,
        pageLimit: Int,
    ): LocalRebuildAttempt = database.writeSyncLockedTransaction { db ->
        startScopeRebuildAttempt(
            db,
            scopeId,
            clientGeneration,
            schemaVersion,
            schemaHash,
            pageLimit,
        )
    }

    fun applyScopeRebuildPage(
        attempt: LocalRebuildAttempt,
        request: RebuildRequest,
        requestJSON: String,
        response: RebuildResponse,
        responseJSON: String,
        syncedTables: List<LocalSchemaTable>,
    ): LocalRebuildAttempt {
        val tableMap = syncedTables.associateBy { it.tableID }
        response.validate(request)
        if (!response.hasMore) {
            response.checksum?.validate()
                ?: throw SynchroError.InvalidResponse("final rebuild page checksum is missing")
        }
        val validatedRequestJSON = rebuildRequestJSON(request, requestJSON)
        val validatedResponseJSON = rebuildResponseJSON(response, responseJSON)

        return database.writeSyncLockedTransaction { db ->
            if (request.scope != attempt.scopeID ||
                request.rebuildID != attempt.rebuildID ||
                request.clientGeneration != attempt.clientGeneration ||
                request.schema != SchemaRef(attempt.schemaVersion, attempt.schemaHash) ||
                request.limit != attempt.pageLimit ||
                request.cursor != attempt.cursor
            ) {
                throw SynchroError.InvalidResponse("rebuild page request does not match its attempt")
            }
            val receipt = SynchroMeta.getRebuildPageReceipt(
                db,
                attempt.scopeID,
                attempt.rebuildID,
                request.cursor,
            )
            if (receipt != null) {
                if (receipt.requestJSON != validatedRequestJSON ||
                    receipt.responseJSON != validatedResponseJSON
                ) {
                    throw SynchroError.InvalidResponse("rebuild page replay differs from its receipt")
                }
                val currentAttempt = SynchroMeta.getRebuildAttempt(db, attempt.scopeID)
                if (currentAttempt == null || !sameRebuildAttempt(currentAttempt, attempt)) {
                    throw SynchroError.InvalidResponse("rebuild page receipt has no active attempt")
                }
                DurableBackoffStore.clearMatching(
                    db,
                    RetryOperation.REBUILDING,
                    validatedRequestJSON,
                )
                return@writeSyncLockedTransaction currentAttempt
            }

            if (SynchroMeta.getRebuildAttempt(db, attempt.scopeID) != attempt) {
                throw SynchroError.InvalidResponse("rebuild attempt is no longer active")
            }

            for (record in response.records) {
                val schema = tableMap[record.table]
                    ?: throw SynchroError.InvalidResponse("unknown logical table ${record.table}")
                val recordId = scopeRecordID(record.pk, schema)
                val localRow = validatedLocalRow(
                    record.table,
                    recordId,
                    record.pk,
                    record.row,
                    record.rowChecksum,
                    record.serverVersion,
                    attempt.schemaHash,
                    schema,
                )
                if (!isApplicationRowProtected(db, schema.tableName, recordId)) {
                    upsertRecord(db, recordId, localRow, schema)
                }
                SynchroMeta.upsertRowVersion(
                    db,
                    schema.tableName,
                    recordId,
                    record.serverVersion,
                    record.rowChecksum,
                )
                SynchroMeta.upsertScopeRow(
                    db,
                    attempt.scopeID,
                    schema.tableName,
                    recordId,
                    requiredScopeRowChecksum(record.rowChecksum, record.table, recordId),
                    attempt.generation
                )
            }
            val finalChecksumJSON = response.checksum?.let(::checksumJSON)
            SynchroMeta.insertRebuildPageReceipt(
                db,
                attempt.scopeID,
                attempt.rebuildID,
                request.cursor,
                validatedRequestJSON,
                validatedResponseJSON,
                response.finalScopeCursor,
                finalChecksumJSON,
            )

            DurableBackoffStore.clearMatching(
                db,
                RetryOperation.REBUILDING,
                validatedRequestJSON,
            )

            if (!response.hasMore) return@writeSyncLockedTransaction attempt
            val nextCursor = response.cursor
                ?: throw SynchroError.InvalidResponse("intermediate rebuild page cursor is missing")
            attempt.copy(cursor = nextCursor).also {
                SynchroMeta.upsertRebuildAttempt(db, it)
            }
        }
    }

    internal fun resolveAppliedRebuildBackoff(
        attempt: LocalRebuildAttempt,
        request: RebuildRequest,
        requestJSON: String,
    ): Boolean {
        val validatedRequestJSON = rebuildRequestJSON(request, requestJSON)
        return database.writeTransaction { db ->
            val currentAttempt = SynchroMeta.getRebuildAttempt(db, attempt.scopeID)
            if (currentAttempt != attempt) return@writeTransaction false
            val receipt = SynchroMeta.getRebuildPageReceipt(
                db,
                attempt.scopeID,
                attempt.rebuildID,
                request.cursor,
            ) ?: return@writeTransaction false
            if (receipt.requestJSON != validatedRequestJSON) {
                throw SynchroError.InvalidResponse("rebuild retry differs from its applied page receipt")
            }
            DurableBackoffStore.clearMatching(
                db,
                RetryOperation.REBUILDING,
                validatedRequestJSON,
            )
            true
        }
    }

    internal fun pendingRebuildFinality(
        attempt: LocalRebuildAttempt,
        request: RebuildRequest,
        requestJSON: String,
    ): PendingRebuildFinality? {
        if (request.scope != attempt.scopeID ||
            request.rebuildID != attempt.rebuildID ||
            request.clientGeneration != attempt.clientGeneration ||
            request.schema != SchemaRef(attempt.schemaVersion, attempt.schemaHash) ||
            request.limit != attempt.pageLimit ||
            request.cursor != attempt.cursor
        ) {
            throw SynchroError.InvalidResponse("rebuild finality request does not match its attempt")
        }
        val validatedRequestJSON = rebuildRequestJSON(request, requestJSON)
        return database.readTransaction { db ->
            if (SynchroMeta.getRebuildAttempt(db, attempt.scopeID) != attempt) {
                return@readTransaction null
            }
            val receipt = SynchroMeta.getFinalRebuildPageReceipt(
                db,
                attempt.scopeID,
                attempt.rebuildID,
            ) ?: return@readTransaction null
            val finalCursor = receipt.finalScopeCursor
            val finalChecksumJSON = receipt.finalChecksumJSON
            if (receipt.requestCursor != attempt.cursor ||
                receipt.requestJSON != validatedRequestJSON ||
                finalCursor == null ||
                finalChecksumJSON == null
            ) {
                throw SynchroError.InvalidResponse("final rebuild receipt does not match its request")
            }
            val response = try {
                rebuildJSON.decodeFromString(RebuildResponse.serializer(), receipt.responseJSON)
            } catch (_: Exception) {
                throw SynchroError.InvalidResponse("final rebuild receipt response is invalid")
            }
            rebuildResponseJSON(response, receipt.responseJSON)
            response.validate(request)
            if (response.hasMore || response.finalScopeCursor != finalCursor || response.checksum == null) {
                throw SynchroError.InvalidResponse("final rebuild receipt does not match its attempt")
            }
            val checksum = try {
                rebuildJSON.decodeFromString(ChecksumObject.serializer(), finalChecksumJSON)
            } catch (_: Exception) {
                throw SynchroError.InvalidResponse("final rebuild receipt checksum is invalid")
            }
            checksum.validate()
            if (checksum != response.checksum) {
                throw SynchroError.InvalidResponse("final rebuild receipt checksum differs from its content")
            }
            PendingRebuildFinality(finalCursor, checksum)
        }
    }

    fun finalizeScopeRebuild(
        attempt: LocalRebuildAttempt,
        finalCursor: String,
        checksum: ChecksumObject,
        syncedTables: List<LocalSchemaTable>,
    ) {
        val tableMap = syncedTables.associateBy { it.tableName }

        database.writeSyncLockedTransaction { db ->
            val expectedChecksumJSON = checksumJSON(checksum)
            val currentAttempt = SynchroMeta.getRebuildAttempt(db, attempt.scopeID)
            val receipt = SynchroMeta.getFinalRebuildPageReceipt(
                db,
                attempt.scopeID,
                attempt.rebuildID,
            )
            if (currentAttempt != null) {
                if (currentAttempt != attempt ||
                    SynchroMeta.getScopeGeneration(db, attempt.scopeID) != attempt.generation
                ) {
                    throw SynchroError.InvalidResponse("rebuild finality targets an inactive attempt")
                }
                if (receipt != null &&
                    (receipt.requestCursor != attempt.cursor ||
                        receipt.finalScopeCursor != finalCursor ||
                        receipt.finalChecksumJSON != expectedChecksumJSON)
                ) {
                    throw SynchroError.InvalidResponse("rebuild finality differs from its page receipt")
                }
            }
            val staleRows = SynchroMeta.getStaleScopeRows(db, attempt.scopeID, attempt.generation)
            SynchroMeta.deleteStaleScopeRows(db, attempt.scopeID, attempt.generation)

            for ((tableName, recordId) in staleRows) {
                val schema = tableMap[tableName] ?: continue
                removeLocalRowIfUnreferenced(db, tableName, recordId, schema)
            }

            val localChecksum = computeScopeChecksum(db, attempt.scopeID, attempt.schemaHash, tableMap)
            checksum.validate()
            if (localChecksum != checksum) {
                throw RebuildChecksumMismatchException(attempt.scopeID)
            }

            SynchroMeta.upsertScope(
                db,
                attempt.scopeID,
                finalCursor,
                checksumJSON(checksum),
                attempt.generation,
                checksumJSON(localChecksum)
            )
            SynchroMeta.deleteRebuildAttempt(db, attempt.scopeID)
            receipt?.let {
                DurableBackoffStore.clearMatching(
                    db,
                    RetryOperation.REBUILDING,
                    it.requestJSON,
                )
            }
        }
    }

    fun removeScope(scopeId: String, syncedTables: List<LocalSchemaTable>) {
        val tableMap = syncedTables.associateBy { it.tableName }

        database.writeSyncLockedTransaction { db ->
            removeScope(db, scopeId, tableMap)
        }
    }

    fun installConnectedAssignment(
        delta: ScopeAssignmentDelta,
        scopeSetVersion: Long,
        clientGeneration: Long,
        syncedTables: List<LocalSchemaTable>,
    ) {
        database.writeSyncLockedTransaction { db ->
            installConnectedAssignmentInTransaction(
                db,
                delta,
                scopeSetVersion,
                clientGeneration,
                syncedTables,
            )
        }
    }

    internal fun installConnectedAssignmentInTransaction(
        db: SQLiteDatabase,
        delta: ScopeAssignmentDelta,
        scopeSetVersion: Long,
        clientGeneration: Long,
        syncedTables: List<LocalSchemaTable>,
        scopeCursorUpdates: Map<String, String?> = emptyMap(),
    ) {
        val receiptScopes = SynchroMeta.getSeedReceipts(db).keys
        if (receiptScopes.isNotEmpty()) {
            val removedScopes = delta.remove.toSet()
            for (scopeID in receiptScopes) {
                val hasCursorUpdate = scopeCursorUpdates.containsKey(scopeID)
                val isRemoved = scopeID in removedScopes
                if (hasCursorUpdate == isRemoved) {
                    throw SynchroError.InvalidResponse(
                        "connect did not explicitly resolve portable seed scope $scopeID"
                    )
                }
            }
        }

        val tableMap = syncedTables.associateBy { it.tableName }
        for (scopeId in delta.remove) {
            removeScope(db, scopeId, tableMap)
        }
        for (scope in delta.add) {
            SynchroMeta.upsertScope(db, scope.id, scope.cursor, checksum = null)
        }
        SynchroMeta.setInt64(db, MetaKey.SCOPE_SET_VERSION, scopeSetVersion)
        SynchroMeta.setInt64(db, MetaKey.CLIENT_GENERATION, clientGeneration)
        if (receiptScopes.isNotEmpty()) {
            for (scopeID in receiptScopes) {
                if (scopeID in delta.remove) {
                    if (SynchroMeta.getScope(db, scopeID) != null) {
                        throw SynchroError.InvalidResponse("removed portable seed scope is still installed")
                    }
                } else {
                    val scope = SynchroMeta.getScope(db, scopeID)
                        ?: throw SynchroError.InvalidResponse("portable seed scope disappeared during connect")
                    if (scope.cursor != scopeCursorUpdates.getValue(scopeID)) {
                        throw SynchroError.InvalidResponse("portable seed scope cursor was not installed")
                    }
                }
            }
            SynchroMeta.clearSeedReceipts(db)
        }
    }

    internal fun reconcileSeedReceiptsBeforeConnect(): Set<String> {
        return database.writeSyncLockedTransaction { db ->
            val receipts = SynchroMeta.getSeedReceiptStates(db)
            if (receipts.isEmpty()) return@writeSyncLockedTransaction emptySet()

            val schemaVersion = SynchroMeta.getInt64(db, MetaKey.SCHEMA_VERSION)
            val schemaHash = SynchroMeta.get(db, MetaKey.SCHEMA_HASH) ?: ""
            val encodedSchema = SynchroMeta.get(db, MetaKey.LOCAL_SCHEMA)
                ?: throw SynchroError.InvalidResponse("portable seed has no installed local schema")
            val tablesByName = try {
                seedReceiptJSON.decodeFromString<List<LocalSchemaTable>>(encodedSchema).associateBy { it.tableName }
            } catch (_: Exception) {
                throw SynchroError.InvalidResponse("portable seed has invalid local schema metadata")
            }

            val invalidated = linkedSetOf<String>()
            for (receipt in receipts) {
                if (seedReceiptMatchesCurrentState(db, receipt, schemaVersion, schemaHash, tablesByName)) {
                    continue
                }
                SynchroMeta.deleteSeedReceipt(db, receipt.scopeID)
                val generation = SynchroMeta.getScope(db, receipt.scopeID)?.generation ?: 0L
                SynchroMeta.upsertScope(
                    db,
                    receipt.scopeID,
                    cursor = null,
                    checksum = null,
                    generation = generation,
                    localChecksum = "",
                )
                invalidated += receipt.scopeID
            }
            invalidated
        }
    }

    private fun seedReceiptMatchesCurrentState(
        db: SQLiteDatabase,
        receipt: LocalSeedReceipt,
        schemaVersion: Long,
        schemaHash: String,
        tablesByName: Map<String, LocalSchemaTable>,
    ): Boolean {
        if (receipt.scopeID.isEmpty() || receipt.receipt.isEmpty() || receipt.cardinality < 0L ||
            receipt.schemaVersion != schemaVersion || receipt.schemaHash != schemaHash
        ) {
            return false
        }
        val scope = SynchroMeta.getScope(db, receipt.scopeID) ?: return false
        if (scope.cursor != null) return false

        val receiptChecksum = decodedSeedChecksum(receipt.checksumJSON) ?: return false
        if (decodedSeedChecksum(scope.checksum) != receiptChecksum ||
            decodedSeedChecksum(scope.localChecksum) != receiptChecksum
        ) {
            return false
        }

        val scopeRows = SynchroMeta.getSeedScopeRows(db, receipt.scopeID)
        if (scopeRows.size.toLong() != receipt.cardinality) return false
        for (row in scopeRows) {
            if (row.tableName !in tablesByName || row.generation != scope.generation ||
                row.serverVersion.isNullOrEmpty()
            ) {
                return false
            }
            val provenanceChecksum = try {
                ChecksumObject("sha256", 1, "hex", row.checksum).also { it.validate() }
            } catch (_: IllegalArgumentException) {
                return false
            }
            if (decodedSeedChecksum(row.rowChecksumJSON) != provenanceChecksum) return false
        }

        val computed = try {
            computeScopeChecksum(db, receipt.scopeID, schemaHash, tablesByName)
        } catch (_: SynchroError.InvalidResponse) {
            return false
        } catch (_: IllegalArgumentException) {
            return false
        }
        return computed == receiptChecksum
    }

    private fun decodedSeedChecksum(source: String?): ChecksumObject? {
        if (source == null) return null
        return try {
            seedReceiptJSON.decodeFromString<ChecksumObject>(source).also { it.validate() }
        } catch (_: IllegalArgumentException) {
            null
        }
    }

    fun clearAllScopeState() {
        database.writeTransaction { db ->
            SynchroMeta.clearAllScopes(db)
            SynchroMeta.clearAllScopeRows(db)
        }
    }

    private fun requiredScopeRowChecksum(
        checksum: ChecksumObject?,
        tableName: String,
        recordId: String,
    ): String {
        val value = checksum
            ?: throw SynchroError.InvalidResponse(
                "missing scope row checksum for $tableName/$recordId"
            )
        value.validate()
        return value.digest
    }

    private fun startScopeRebuildAttempt(
        db: SQLiteDatabase,
        scopeId: String,
        clientGeneration: Long,
        schemaVersion: Long,
        schemaHash: String,
        pageLimit: Int,
    ): LocalRebuildAttempt {
        if (pageLimit <= 0) {
            throw SynchroError.InvalidResponse("rebuild page limit is invalid")
        }
        if (SynchroMeta.getScope(db, scopeId) == null) {
            throw SynchroError.InvalidResponse("rebuild targets an unknown scope $scopeId")
        }
        SynchroMeta.getRebuildAttempt(db, scopeId)?.let { existing ->
            SynchroMeta.deleteRebuildPageReceipts(db, existing.scopeID, existing.rebuildID)
        }
        clearRebuildingBackoffForScope(db, scopeId)
        resetScopeProvenance(db, scopeId)
        val attempt = LocalRebuildAttempt(
            scopeID = scopeId,
            rebuildID = UUID.randomUUID().toString(),
            clientGeneration = clientGeneration,
            schemaVersion = schemaVersion,
            schemaHash = schemaHash,
            generation = SynchroMeta.bumpScopeGeneration(db, scopeId),
            cursor = null,
            pageLimit = pageLimit,
        )
        SynchroMeta.upsertRebuildAttempt(db, attempt)
        return attempt
    }

    private fun sameRebuildAttempt(left: LocalRebuildAttempt, right: LocalRebuildAttempt): Boolean =
        left.scopeID == right.scopeID &&
            left.rebuildID == right.rebuildID &&
            left.clientGeneration == right.clientGeneration &&
            left.schemaVersion == right.schemaVersion &&
            left.schemaHash == right.schemaHash &&
            left.generation == right.generation &&
            left.pageLimit == right.pageLimit

    private fun rebuildRequestJSON(request: RebuildRequest, body: String): String {
        return try {
            Integrity.validateCanonicalWireJSON(body)
            if (rebuildJSON.decodeFromString(RebuildRequest.serializer(), body) != request) {
                throw SynchroError.InvalidResponse("rebuild request body differs from its decoded value")
            }
            body
        } catch (error: SynchroError.InvalidResponse) {
            throw error
        } catch (_: Exception) {
            throw SynchroError.InvalidResponse("rebuild request body is invalid")
        }
    }

    private fun rebuildResponseJSON(response: RebuildResponse, body: String): String {
        return try {
            Integrity.validateCanonicalWireJSON(body)
            if (rebuildJSON.decodeFromString(RebuildResponse.serializer(), body) != response) {
                throw SynchroError.InvalidResponse("rebuild response body differs from its decoded value")
            }
            body
        } catch (error: SynchroError.InvalidResponse) {
            throw error
        } catch (_: Exception) {
            throw SynchroError.InvalidResponse("rebuild response body is invalid")
        }
    }

    private fun removeScope(
        db: SQLiteDatabase,
        scopeId: String,
        tablesByName: Map<String, LocalSchemaTable>,
    ) {
        val scopeRows = SynchroMeta.getScopeRows(db, scopeId)
        // An active rebuild loses its pages with its scope. A completed rebuild
        // keeps its page receipts, because the receipt records that the rebuild
        // happened rather than that the scope is still assigned. A later
        // rebuild of a reassigned scope clears that scope's receipts first.
        if (SynchroMeta.getRebuildAttempt(db, scopeId) != null) {
            SynchroMeta.deleteAllRebuildPageReceipts(db, scopeId)
        }
        SynchroMeta.deleteRebuildAttempt(db, scopeId)
        clearRebuildingBackoffForScope(db, scopeId)
        SynchroMeta.deleteScopeRows(db, scopeId)
        SynchroMeta.deleteScope(db, scopeId)

        for ((tableName, recordId) in scopeRows) {
            val schema = tablesByName[tableName] ?: continue
            removeLocalRowIfUnreferenced(db, tableName, recordId, schema)
        }
    }

    private fun resetScopeProvenance(db: SQLiteDatabase, scopeId: String) {
        val scopeRows = SynchroMeta.getScopeRows(db, scopeId)
        val encodedSchema = SynchroMeta.get(db, MetaKey.LOCAL_SCHEMA)
            ?: throw SynchroError.InvalidResponse("rebuild reset has no installed local schema")
        val tablesByName = try {
            rebuildJSON.decodeFromString<List<LocalSchemaTable>>(encodedSchema).associateBy { it.tableName }
        } catch (_: Exception) {
            throw SynchroError.InvalidResponse("rebuild reset has invalid local schema metadata")
        }
        SynchroMeta.deleteScopeRows(db, scopeId)
        for ((tableName, recordId) in scopeRows) {
            val schema = tablesByName[tableName]
                ?: throw SynchroError.InvalidResponse("rebuild reset references an unknown local table")
            removeLocalRowIfUnreferenced(db, tableName, recordId, schema)
        }
    }

    private fun clearRebuildingBackoffForScope(db: SQLiteDatabase, scopeId: String) {
        val backoff = DurableBackoffStore.load(db) ?: return
        if (backoff.resumeState != RetryOperation.REBUILDING) return
        val request = try {
            Integrity.validateCanonicalWireJSON(backoff.workIdentity)
            rebuildJSON.decodeFromString<RebuildRequest>(backoff.workIdentity)
        } catch (_: Exception) {
            throw SynchroError.InvalidResponse("durable rebuild retry identity is invalid")
        }
        if (request.scope == scopeId) {
            DurableBackoffStore.clearMatching(
                db,
                RetryOperation.REBUILDING,
                backoff.workIdentity,
            )
        }
    }

    // MARK: - Private

    private fun upsertRecord(
        db: SQLiteDatabase,
        recordId: String,
        data: Map<String, Any?>,
        schema: LocalSchemaTable,
    ) {
        val pkCol = schema.primaryKey.firstOrNull() ?: "id"
        val quoted = SQLiteHelpers.quoteIdentifier(schema.tableName)
        val quotedPK = SQLiteHelpers.quoteIdentifier(pkCol)

        val columns = schema.columns.map { it.name }
        val dbValues = buildDatabaseValues(columns, pkCol, recordId, data)

        val pkIndex = columns.indexOf(pkCol)
        if (pkIndex < 0) throw SynchroError.InvalidResponse("local table omits its primary key column")
        val dataIndexes = columns.indices.filter { it != pkIndex }
        executeUpsert(
            db = db,
            table = quoted,
            keyColumns = listOf(quotedPK),
            keyValues = listOf(dbValues[pkIndex]),
            dataColumns = dataIndexes.map { SQLiteHelpers.quoteIdentifier(columns[it]) },
            dataValues = dataIndexes.map(dbValues::get),
        )
    }

    private fun buildDatabaseValues(
        columns: List<String>,
        pkCol: String,
        recordID: String,
        data: Map<String, Any?>
    ): List<Any?> {
        return columns.map { col ->
            if (data.containsKey(col)) {
                data[col]
            } else if (col == pkCol) {
                recordID
            } else {
                null
            }
        }
    }

    private fun validatedLocalRow(
        tableID: String,
        recordId: String,
        pk: JsonObject,
        row: JsonObject?,
        rowChecksum: ChecksumObject?,
        serverVersion: String,
        schemaHash: String,
        schema: LocalSchemaTable,
    ): Map<String, Any?> {
        val completeRow = row ?: throw SynchroError.InvalidResponse("missing row for $tableID")
        val checksum = rowChecksum
            ?: throw SynchroError.InvalidResponse("missing row checksum for $tableID/$recordId")
        val computed = try {
            Integrity.rowDigest(schemaHash, schema, pk, completeRow, serverVersion).checksum
        } catch (_: IllegalArgumentException) {
            throw SynchroError.InvalidResponse("invalid row for $tableID/$recordId")
        }
        if (computed != checksum) {
            throw SynchroError.InvalidResponse("row checksum mismatch for $tableID/$recordId")
        }
        return schema.columns.associate { column ->
            column.name to databaseValue(completeRow.getValue(column.fieldID), column)
        }.toMap()
    }

    private fun scopeRecordID(pk: JsonObject, schema: LocalSchemaTable): String {
        val value = pk[schema.primaryKeyFieldID] as? JsonPrimitive
            ?: throw SynchroError.InvalidResponse("missing primary key ${schema.primaryKeyFieldID} for ${schema.tableName}")
        return value.content
    }

    private fun applyScopeDeleteChange(
        db: SQLiteDatabase,
        change: ChangeRecord,
        recordId: String,
        schema: LocalSchemaTable,
        schemaHash: String,
    ) {
        change.row?.let { row ->
            val deletedAt = schema.deletedAtFieldID?.let(row::get)
            if (deletedAt == null || deletedAt is JsonNull) {
                throw SynchroError.InvalidResponse(
                    "delete change for ${change.table} $recordId included a row without ${schema.deletedAtColumn}"
                )
            }
            val localRow = validatedLocalRow(
                change.table,
                recordId,
                change.pk,
                row,
                change.rowChecksum,
                change.serverVersion,
                schemaHash,
                schema,
            )
            if (!isApplicationRowProtected(db, schema.tableName, recordId)) {
                upsertRecord(db, recordId, localRow, schema)
            }
            SynchroMeta.upsertRowVersion(
                db,
                schema.tableName,
                recordId,
                change.serverVersion,
                change.rowChecksum,
            )
        } ?: SynchroMeta.upsertRowVersion(
            db,
            schema.tableName,
            recordId,
            change.serverVersion,
            null,
        )

        SynchroMeta.deleteScopeRow(db, change.scope, schema.tableName, recordId)

        if (change.row == null) {
            removeLocalRowIfUnreferenced(db, schema.tableName, recordId, schema)
        }
    }

    internal fun computeScopeChecksum(
        db: SQLiteDatabase,
        scopeId: String,
        schemaHash: String,
        tablesByName: Map<String, LocalSchemaTable>,
    ): ChecksumObject {
        val entries = SynchroMeta.getScopeRowChecksums(db, scopeId).map { scopeRow ->
            val table = tablesByName[scopeRow.tableName]
                ?: throw SynchroError.InvalidResponse("scope references unknown table ${scopeRow.tableName}")
            if (isApplicationRowProtected(db, scopeRow.tableName, scopeRow.recordID)) {
                return@map scopeRowIdentity(table, scopeRow.recordID) to
                    ChecksumObject("sha256", 1, "hex", scopeRow.checksum)
            }
            val row = loadWireRow(db, table, scopeRow.recordID)
            val pk = JsonObject(mapOf(table.primaryKeyFieldID to row.getValue(table.primaryKeyFieldID)))
            val serverVersion = SynchroMeta.getRowVersion(db, table.tableName, scopeRow.recordID)
                ?: throw SynchroError.InvalidResponse("scope row has no server version")
            val computed = Integrity.rowDigest(schemaHash, table, pk, row, serverVersion)
            if (computed.checksum.digest != scopeRow.checksum) {
                throw SynchroError.InvalidResponse("scope row checksum does not match local row")
            }
            computed.identity to computed.checksum
        }
        return Integrity.scopeDigest(schemaHash, scopeId, entries)
    }

    private fun databaseValue(value: JsonElement, column: LocalSchemaColumn): Any? {
        if (value is JsonNull) return null
        val primitive = value as? JsonPrimitive
            ?: throw SynchroError.InvalidResponse("invalid ${column.logicalType} value for ${column.fieldID}")
        return when (column.logicalType) {
            "string", "decimal", "datetime", "date", "time", "json" -> primitive
                .takeIf { it.isString }
                ?.content
                ?: throw SynchroError.InvalidResponse("invalid ${column.logicalType} value for ${column.fieldID}")
            "int" -> primitive
                .takeIf { !it.isString }
                ?.intOrNull
                ?.toLong()
                ?: throw SynchroError.InvalidResponse("invalid int value for ${column.fieldID}")
            "int64" -> primitive
                .takeIf { it.isString }
                ?.content
                ?.toLongOrNull()
                ?: throw SynchroError.InvalidResponse("invalid int64 value for ${column.fieldID}")
            "float" -> primitive
                .takeIf { !it.isString }
                ?.doubleOrNull
                ?.takeIf { it.isFinite() }
                ?: throw SynchroError.InvalidResponse("invalid float value for ${column.fieldID}")
            "boolean" -> primitive.booleanOrNull?.let { if (it) 1L else 0L }
                ?: throw SynchroError.InvalidResponse("invalid boolean value for ${column.fieldID}")
            "bytes" -> primitive
                .takeIf { it.isString }
                ?.content
                ?.let { decodeBase64URL(it, column.fieldID) }
                ?: throw SynchroError.InvalidResponse("invalid bytes value for ${column.fieldID}")
            else -> throw SynchroError.InvalidResponse("unsupported portable type ${column.logicalType}")
        }
    }

    private fun decodeBase64URL(value: String, fieldID: String): ByteArray {
        if (value.length % 4 == 1 || value.any { it !in BASE64_URL_CHARACTERS }) {
            throw SynchroError.InvalidResponse("invalid bytes value for $fieldID")
        }
        val decoded = try {
            Base64.decode(value, Base64.URL_SAFE or Base64.NO_WRAP or Base64.NO_PADDING)
        } catch (_: IllegalArgumentException) {
            throw SynchroError.InvalidResponse("invalid bytes value for $fieldID")
        }
        val canonical = Base64.encodeToString(
            decoded,
            Base64.URL_SAFE or Base64.NO_WRAP or Base64.NO_PADDING,
        )
        if (canonical != value) throw SynchroError.InvalidResponse("invalid bytes value for $fieldID")
        return decoded
    }

    private fun isApplicationRowProtected(
        db: SQLiteDatabase,
        tableName: String,
        recordId: String,
    ): Boolean = db.rawQuery(
        """
        SELECT 1
        FROM _synchro_pending_changes AS pending
        WHERE pending.table_name = ? AND pending.record_id = ?
          AND (
            pending.lifecycle_state IN ('captured', 'sealed', 'blocked_by_predecessor', 'legacy_blocked')
            OR (
                pending.lifecycle_state = 'rejected_terminal'
                AND NOT EXISTS (
                    SELECT 1
                    FROM _synchro_pending_changes AS replacement
                    WHERE replacement.table_name = pending.table_name
                      AND replacement.record_id = pending.record_id
                      AND replacement.local_order > pending.local_order
                      AND replacement.lifecycle_state IN ('accepted', 'conflict')
                )
            )
          )
        LIMIT 1
        """.trimIndent(),
        arrayOf(tableName, recordId),
    ).use { it.moveToFirst() }

    private fun scopeRowIdentity(table: LocalSchemaTable, recordId: String): ByteArray {
        val primaryKey = table.columns.singleOrNull { it.fieldID == table.primaryKeyFieldID }
            ?: throw SynchroError.InvalidResponse("scope row has no primary key metadata")
        val value = when (primaryKey.logicalType) {
            "string" -> JsonPrimitive(recordId)
            "int" -> recordId.toIntOrNull()
                ?.takeIf { recordId == "0" || recordId.matches(Regex("-?[1-9][0-9]*")) }
                ?.let(::JsonPrimitive)
                ?: throw SynchroError.InvalidResponse("scope row has an invalid integer primary key")
            "int64" -> recordId
                .takeIf { it == "0" || it.matches(Regex("-?[1-9][0-9]*")) }
                ?.takeIf { it.toLongOrNull() != null }
                ?.let(::JsonPrimitive)
                ?: throw SynchroError.InvalidResponse("scope row has an invalid int64 primary key")
            else -> throw SynchroError.InvalidResponse("scope row has an unsupported primary key type")
        }
        return Integrity.rowIdentity(table, JsonObject(mapOf(table.primaryKeyFieldID to value)))
    }

    private fun loadWireRow(db: SQLiteDatabase, table: LocalSchemaTable, recordId: String): JsonObject {
        val columns = table.columns.joinToString(", ") { SQLiteHelpers.quoteIdentifier(it.name) }
        val primaryKey = SQLiteHelpers.quoteIdentifier(table.primaryKey.firstOrNull() ?: "id")
        val relation = SQLiteHelpers.quoteIdentifier(table.tableName)
        db.rawQuery("SELECT $columns FROM $relation WHERE $primaryKey = ?", arrayOf(recordId)).use { cursor ->
            if (!cursor.moveToFirst()) {
                throw SynchroError.InvalidResponse("scope provenance references a missing row")
            }
            return JsonObject(table.columns.mapIndexed { index, column ->
                column.fieldID to wireValue(cursor, index, column)
            }.toMap())
        }
    }

    private fun wireValue(
        cursor: android.database.Cursor,
        index: Int,
        column: LocalSchemaColumn,
    ): JsonElement {
        if (cursor.isNull(index)) {
            if (!column.nullable) {
                throw SynchroError.InvalidResponse("non-null field ${column.fieldID} is null")
            }
            return JsonNull
        }
        val storageType = cursor.getType(index)
        return when (column.logicalType) {
            "boolean" -> {
                val value = cursor.getLong(index)
                if (storageType != android.database.Cursor.FIELD_TYPE_INTEGER || value !in 0L..1L) {
                    throw SynchroError.InvalidResponse("invalid SQLite value for ${column.fieldID}")
                }
                JsonPrimitive(value == 1L)
            }
            "int" -> {
                val value = cursor.getLong(index)
                if (storageType != android.database.Cursor.FIELD_TYPE_INTEGER ||
                    value !in Int.MIN_VALUE.toLong()..Int.MAX_VALUE.toLong()
                ) {
                    throw SynchroError.InvalidResponse("invalid SQLite value for ${column.fieldID}")
                }
                JsonPrimitive(value.toInt())
            }
            "int64" -> {
                if (storageType != android.database.Cursor.FIELD_TYPE_INTEGER) {
                    throw SynchroError.InvalidResponse("invalid SQLite value for ${column.fieldID}")
                }
                JsonPrimitive(cursor.getLong(index).toString())
            }
            "float" -> {
                if (storageType != android.database.Cursor.FIELD_TYPE_FLOAT &&
                    storageType != android.database.Cursor.FIELD_TYPE_INTEGER
                ) {
                    throw SynchroError.InvalidResponse("invalid SQLite value for ${column.fieldID}")
                }
                val value = cursor.getDouble(index)
                if (!value.isFinite()) {
                    throw SynchroError.InvalidResponse("invalid SQLite value for ${column.fieldID}")
                }
                JsonPrimitive(value)
            }
            "bytes" -> {
                if (storageType != android.database.Cursor.FIELD_TYPE_BLOB) {
                    throw SynchroError.InvalidResponse("invalid SQLite value for ${column.fieldID}")
                }
                JsonPrimitive(
                    android.util.Base64.encodeToString(
                        cursor.getBlob(index),
                        android.util.Base64.URL_SAFE or android.util.Base64.NO_WRAP or android.util.Base64.NO_PADDING,
                    )
                )
            }
            "string", "decimal", "datetime", "date", "time", "json" -> {
                if (storageType != android.database.Cursor.FIELD_TYPE_STRING) {
                    throw SynchroError.InvalidResponse("invalid SQLite value for ${column.fieldID}")
                }
                JsonPrimitive(cursor.getString(index))
            }
            else -> throw SynchroError.InvalidResponse("unsupported portable type ${column.logicalType}")
        }
    }

    private fun checksumJSON(checksum: ChecksumObject): String =
        kotlinx.serialization.json.Json.encodeToString(ChecksumObject.serializer(), checksum)

    private fun removeLocalRowIfUnreferenced(
        db: SQLiteDatabase,
        tableName: String,
        recordId: String,
        schema: LocalSchemaTable
    ) {
        if (SynchroMeta.hasScopeRows(db, tableName, recordId)) {
            return
        }
        if (isApplicationRowProtected(db, tableName, recordId)) {
            return
        }

        val pkCol = schema.primaryKey.firstOrNull() ?: "id"
        val quoted = SQLiteHelpers.quoteIdentifier(tableName)
        val quotedPK = SQLiteHelpers.quoteIdentifier(pkCol)

        val stmt = db.compileStatement(
            "DELETE FROM $quoted WHERE $quotedPK = ?"
        )
        try {
            stmt.bindString(1, recordId)
            stmt.executeUpdateDelete()
        } finally {
            stmt.close()
        }
    }

}

private const val BASE64_URL_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

/**
 * Executes a SQL statement with properly typed parameter bindings.
 * Unlike `execSQL(String, Object[])` which converts everything to strings,
 * this uses `compileStatement` with typed bind methods to correctly handle
 * null, Long, Double, String, ByteArray, and Boolean values.
 */
internal fun executeWithTypedBindings(db: SQLiteDatabase, sql: String, values: List<Any?>) {
    executeChangeCount(db, sql, values)
}

/** Executes a statement as `executeWithTypedBindings` does and reports the changed-row count. */
internal fun executeChangeCount(db: SQLiteDatabase, sql: String, values: List<Any?>): Int {
    val stmt = db.compileStatement(sql)
    try {
        bindTypedValues(stmt, values)
        return stmt.executeUpdateDelete()
    } finally {
        stmt.close()
    }
}

/**
 * Applies an upsert with syntax that every supported Android version accepts.
 * SQLite adds UPSERT in 3.24 and Android API 24 ships SQLite 3.9, so
 * `ON CONFLICT ... DO UPDATE` fails on the minimum supported cell.
 *
 * The portable form updates the conflicting row first and inserts only when no
 * row matched. `INSERT OR REPLACE` is not an equivalent, because it deletes the
 * existing row, which fires change-capture delete triggers and discards every
 * column the statement does not name.
 *
 * Identifiers arrive already quoted. The result is the changed-row count.
 */
internal fun executeUpsert(
    db: SQLiteDatabase,
    table: String,
    keyColumns: List<String>,
    keyValues: List<Any?>,
    dataColumns: List<String>,
    dataValues: List<Any?>,
): Int {
    require(keyColumns.isNotEmpty()) { "an upsert requires at least one key column" }
    require(keyColumns.size == keyValues.size) { "upsert key column and value counts differ" }
    require(dataColumns.size == dataValues.size) { "upsert data column and value counts differ" }
    if (dataColumns.isNotEmpty()) {
        val assignments = dataColumns.joinToString(", ") { "$it = ?" }
        val predicate = keyColumns.joinToString(" AND ") { "$it = ?" }
        val updated = executeChangeCount(
            db,
            "UPDATE $table SET $assignments WHERE $predicate",
            dataValues + keyValues,
        )
        if (updated > 0) return updated
    }
    val columns = keyColumns + dataColumns
    // Every column is a key column, so an existing row already holds the
    // intended state and the insert yields to it.
    val verb = if (dataColumns.isEmpty()) "INSERT OR IGNORE" else "INSERT"
    return executeChangeCount(
        db,
        "$verb INTO $table (${columns.joinToString(", ")}) " +
            "VALUES (${SQLiteHelpers.placeholders(columns.size)})",
        keyValues + dataValues,
    )
}

internal fun bindTypedValues(stmt: SQLiteProgram, values: List<Any?>) {
    for (i in values.indices) {
        val bindIndex = i + 1
        when (val value = sqliteBindValue(values[i], i)) {
            null -> stmt.bindNull(bindIndex)
            is Long -> stmt.bindLong(bindIndex, value)
            is Double -> stmt.bindDouble(bindIndex, value)
            is ByteArray -> stmt.bindBlob(bindIndex, value)
            is String -> stmt.bindString(bindIndex, value)
            else -> throw IllegalArgumentException(
                "Unsupported SQL bind value at index $i: ${value::class.java.name}"
            )
        }
    }
}
