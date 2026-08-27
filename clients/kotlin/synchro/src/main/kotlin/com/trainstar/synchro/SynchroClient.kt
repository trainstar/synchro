package com.trainstar.synchro

import android.content.Context
import android.app.Activity
import android.app.Application
import android.database.sqlite.SQLiteDatabase
import android.os.Bundle
import com.trainstar.synchro.inspection.CheckpointInspection
import com.trainstar.synchro.inspection.ClientStateCountsInspection
import com.trainstar.synchro.inspection.ClientStateInspection
import com.trainstar.synchro.inspection.ProvenanceInspection
import com.trainstar.synchro.inspection.ProvenanceMaintenanceWorkInspection
import com.trainstar.synchro.inspection.RebuildAttemptInspection
import com.trainstar.synchro.inspection.RebuildPageReceiptInspection
import com.trainstar.synchro.inspection.RebuildReceiptInspection
import com.trainstar.synchro.inspection.RebuildStateInspection
import com.trainstar.synchro.inspection.RowMetadataInspection
import com.trainstar.synchro.inspection.SchemaInspection
import com.trainstar.synchro.inspection.ScopeInspection
import com.trainstar.synchro.inspection.ScopeRowInspection
import com.trainstar.synchro.inspection.TransportObservationCollector
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject

class SynchroClient(private val config: SynchroConfig, context: Context) {
    init {
        config.seedDatabasePath?.let { seedPath ->
            SeedDatabaseInstaller.installIfNeeded(context, seedPath, config.dbPath)
        }
    }
    private val database: SynchroDatabase = SynchroDatabase.open(context, config.dbPath)
    private val okHttpClient: okhttp3.OkHttpClient = okhttp3.OkHttpClient.Builder()
        .connectTimeout(30, java.util.concurrent.TimeUnit.SECONDS)
        .readTimeout(60, java.util.concurrent.TimeUnit.SECONDS)
        .writeTimeout(60, java.util.concurrent.TimeUnit.SECONDS)
        .build()
    private val httpClient: HttpClient = HttpClient(config, okHttpClient)
    private val schemaManager: SchemaManager = SchemaManager(database)
    private val changeTracker: ChangeTracker = ChangeTracker(database)
    private val pullProcessor: PullProcessor = PullProcessor(database)
    private val pushProcessor: PushProcessor = PushProcessor(database, changeTracker)
    private val syncEngine: SyncEngine = SyncEngine(
        config = config,
        database = database,
        httpClient = httpClient,
        schemaManager = schemaManager,
        changeTracker = changeTracker,
        pullProcessor = pullProcessor,
        pushProcessor = pushProcessor
    )
    private val application: Application? = context.applicationContext as? Application
    private val lifecycleObserver: Application.ActivityLifecycleCallbacks? = application?.let { app ->
        NativeApplicationLifecycleObserver(
            onForeground = syncEngine::onApplicationForeground,
            onBackground = syncEngine::onApplicationBackground,
        ).also(app::registerActivityLifecycleCallbacks)
    }

    // MARK: - Core SQL

    fun query(sql: String, params: Array<out Any?>? = null): List<Row> =
        database.applicationQuery(sql, params)

    fun queryOne(sql: String, params: Array<out Any?>? = null): Row? =
        database.applicationQueryOne(sql, params)

    fun execute(sql: String, params: Array<out Any?>? = null): ExecResult =
        database.applicationExecute(sql, params)

    // MARK: - Transactions

    fun <T> readTransaction(block: (ApplicationReadTransaction) -> T): T =
        database.applicationReadTransaction(block)

    fun <T> transaction(block: (ApplicationTransaction) -> T): T =
        database.applicationTransaction(block)

    /** Kept as a source-compatible name without exposing SQLiteDatabase. */
    fun <T> writeTransaction(block: (ApplicationTransaction) -> T): T =
        transaction(block)

    // MARK: - Batch

    fun executeBatch(statements: List<SQLStatement>): Int =
        database.applicationExecuteBatch(statements)

    // MARK: - Schema (local-only tables)

    fun createTable(name: String, columns: List<ColumnDef>, options: TableOptions? = null) =
        database.createLocalOnlyTable(name, columns, options)

    fun alterTable(name: String, addColumns: List<ColumnDef>) =
        database.alterLocalOnlyTable(name, addColumns)

    fun createIndex(table: String, columns: List<String>, unique: Boolean = false) =
        database.createLocalOnlyIndex(table, columns, unique)

    // MARK: - Observation

    fun onChange(tables: List<String>, callback: () -> Unit): Cancellable {
        tables.forEach(ApplicationSql::requireApplicationObject)
        return database.onChange(tables, callback)
    }

    fun watch(
        sql: String,
        params: Array<out Any?>? = null,
        tables: List<String>,
        callback: (List<Row>) -> Unit
    ): Cancellable {
        ApplicationSql.authorizeRead(sql)
        tables.forEach(ApplicationSql::requireApplicationObject)
        return database.watch(sql, params, tables, callback)
    }

    // MARK: - WAL

    fun checkpoint(mode: CheckpointMode = CheckpointMode.PASSIVE) =
        database.checkpoint(mode)

    // MARK: - Lifecycle

    fun close() {
        runBlocking {
            syncEngine.shutdown()
        }
        lifecycleObserver?.let { observer -> application?.unregisterActivityLifecycleCallbacks(observer) }
        database.close()
        okHttpClient.dispatcher.executorService.shutdown()
        okHttpClient.connectionPool.evictAll()
    }

    val path: String get() = database.path

    // MARK: - Sync Status

    fun pendingChangeCount(): Int = changeTracker.pendingChangeCount()

    fun getSyncStatus(): SyncStatus = syncEngine.getSyncStatus()

    fun inspectPendingMutations(): List<PendingMutationInspection> =
        changeTracker.inspectPendingMutations()

    fun inspectRejectedMutations(): List<RejectedMutationInspection> =
        database.readTransaction { db ->
            SynchroMeta.listRejectedMutations(db).map { rejected ->
                RejectedMutationInspection(
                    mutationID = rejected.mutationID,
                    tableName = rejected.tableName,
                    recordID = rejected.recordID,
                    status = rejected.status.asRejectedMutationStatus(),
                    code = rejected.code.asMutationRejectionCode(),
                    message = rejected.message,
                    serverRowJSON = rejected.serverRowJson,
                    serverVersion = rejected.serverVersion,
                    mutationJSON = rejected.mutationJSON
                        ?: throw SynchroError.InvalidResponse("retained rejection lacks its exact mutation JSON"),
                    rejectionJSON = rejected.rejectionJSON
                        ?: throw SynchroError.InvalidResponse("retained rejection lacks its exact rejection JSON"),
                    createdAt = rejected.createdAt,
                    updatedAt = rejected.updatedAt,
                )
            }
        }

    fun clearRejectedMutations() {
        database.writeTransaction { db -> SynchroMeta.clearRejectedMutations(db) }
    }

    /** Returns the durable schema reference without exposing reserved SQLite state. */
    internal fun inspectSchema(): SchemaInspection = database.readTransaction(::inspectSchema)

    private fun inspectSchema(db: SQLiteDatabase): SchemaInspection {
        val version = SynchroMeta.getInt64(db, MetaKey.SCHEMA_VERSION)
        val hash = SynchroMeta.get(db, MetaKey.SCHEMA_HASH).orEmpty()
        return when {
            version == 0L && hash.isEmpty() -> SchemaInspection(null)
            version > 0L && hash.matches(SCHEMA_HASH) -> {
                val schema = SchemaRef(version, hash)
                schema.validate()
                SchemaInspection(schema)
            }
            else -> throw SynchroError.InvalidResponse("durable schema inspection is invalid")
        }
    }

    /** Returns at most [limit] durable scopes, or fails when that bound is exceeded. */
    internal fun inspectScopes(limit: Int = MAXIMUM_INSPECTION_RECORDS): List<ScopeInspection> =
        database.readTransaction { db -> inspectScopes(db, limit) }

    private fun inspectScopes(db: SQLiteDatabase, limit: Int): List<ScopeInspection> =
        SynchroMeta.listScopes(db, limit).map {
            ScopeInspection(it.scopeID, it.cursor, it.checksum, it.generation, it.localChecksum)
        }

    /** Returns at most [limit] durable scope-row memberships. */
    internal fun inspectScopeRows(limit: Int = MAXIMUM_INSPECTION_RECORDS): List<ScopeRowInspection> =
        database.readTransaction { db -> inspectScopeRows(db, limit) }

    private fun inspectScopeRows(db: SQLiteDatabase, limit: Int): List<ScopeRowInspection> =
        SynchroMeta.listScopeRows(db, limit).map {
            ScopeRowInspection(it.scopeID, it.tableName, it.recordID, it.checksum, it.generation)
        }

    /** Returns at most [limit] durable row-version records. */
    internal fun inspectRowMetadata(limit: Int = MAXIMUM_INSPECTION_RECORDS): List<RowMetadataInspection> =
        database.readTransaction { db ->
            SynchroMeta.listRowMetadata(db, limit).map {
                RowMetadataInspection(it.tableName, it.recordID, it.serverVersion, it.rowChecksumJSON)
            }
        }

    /** Returns at most [limit] scope checkpoints. */
    internal fun inspectCheckpoints(limit: Int = MAXIMUM_INSPECTION_RECORDS): List<CheckpointInspection> =
        database.readTransaction { db ->
            SynchroMeta.listScopes(db, limit).map {
                CheckpointInspection(it.scopeID, it.cursor, it.checksum, it.localChecksum)
            }
        }

    /** Returns at most [limit] row-to-scope provenance records. */
    internal fun inspectProvenance(limit: Int = MAXIMUM_INSPECTION_RECORDS): List<ProvenanceInspection> =
        database.readTransaction { db ->
            val rows = SynchroMeta.listScopeRows(db, limit)
            rows.groupBy { it.tableName to it.recordID }.map { (key, members) ->
                ProvenanceInspection(
                    tableName = key.first,
                    recordID = key.second,
                    scopeIDs = members.map { it.scopeID }.sorted(),
                    serverVersion = SynchroMeta.getRowVersion(db, key.first, key.second),
                )
            }.sortedWith(compareBy<ProvenanceInspection> { it.tableName }.thenBy { it.recordID })
        }

    /** Returns committed scope-row maintenance work for this client process. */
    internal fun inspectProvenanceMaintenanceWork(): ProvenanceMaintenanceWorkInspection =
        database.inspectProvenanceMaintenanceWork()

    /** Returns one bounded snapshot of client state and its maintenance-work cursor. */
    internal fun inspectClientState(limit: Int = MAXIMUM_INSPECTION_RECORDS): ClientStateInspection =
        database.stateInspectionTransaction { db, provenanceMaintenanceWork ->
            ClientStateInspection(
                schema = inspectSchema(db).currentSchema,
                scopeStates = inspectScopes(db, limit),
                scopeRows = inspectScopeRows(db, limit),
                rebuildAttempts = inspectRebuildAttempts(db, limit),
                provenanceMaintenanceWorkCursor = provenanceMaintenanceWork.cursor,
            )
        }

    /** Returns exact counts without materializing unbounded durable records. */
    internal fun inspectClientStateCounts(): ClientStateCountsInspection =
        database.stateInspectionTransaction { db, provenanceMaintenanceWork ->
            val provenanceCount = inspectionCount(
                db,
                "SELECT COUNT(*) FROM (SELECT table_name, record_id FROM _synchro_scope_rows GROUP BY table_name, record_id)",
            )
            ClientStateCountsInspection(
                schema = inspectSchema(db).currentSchema,
                applicationRowCount = provenanceCount,
                mutationLedgerCount = inspectionCount(db, "SELECT COUNT(*) FROM _synchro_pending_changes"),
                mutationOutcomeCount = inspectionCount(
                    db,
                    "SELECT COUNT(*) FROM _synchro_pending_changes WHERE lifecycle_state IN ('accepted', 'conflict', 'rejected_terminal')",
                ),
                sealedBatchCount = inspectionCount(db, "SELECT COUNT(*) FROM _synchro_push_batches"),
                rejectedMutationCount = inspectionCount(db, "SELECT COUNT(*) FROM _synchro_rejected_mutations"),
                scopeStateCount = inspectionCount(db, "SELECT COUNT(*) FROM _synchro_scopes"),
                scopeRowCount = inspectionCount(db, "SELECT COUNT(*) FROM _synchro_scope_rows"),
                provenanceCount = provenanceCount,
                rowMetadataCount = inspectionCount(db, "SELECT COUNT(*) FROM _synchro_row_versions"),
                rebuildAttemptCount = inspectionCount(db, "SELECT COUNT(*) FROM _synchro_rebuild_attempts"),
                rebuildReceiptCount = inspectionCount(db, "SELECT COUNT(*) FROM _synchro_rebuild_page_receipts"),
                provenanceMaintenanceWorkCursor = provenanceMaintenanceWork.cursor,
            )
        }

    private fun inspectionCount(db: SQLiteDatabase, sql: String): Int =
        db.rawQuery(sql, null).use { cursor ->
            require(cursor.moveToFirst()) { "durable state count is absent" }
            val count = cursor.getLong(0)
            require(count in 0..Int.MAX_VALUE.toLong()) { "durable state count is invalid" }
            count.toInt()
        }

    /** Returns bounded read-only state for unfinished rebuild work. */
    internal fun inspectRebuildState(limit: Int = MAXIMUM_INSPECTION_RECORDS): RebuildStateInspection =
        database.readTransaction { db ->
            val attempts = inspectRebuildAttempts(db, limit)
            val receipts = SynchroMeta.listRebuildPageReceipts(db, limit).map {
                RebuildPageReceiptInspection(
                    scopeID = it.scopeID,
                    rebuildID = it.rebuildID,
                    requestCursor = it.requestCursor,
                    isFinal = it.isFinal,
                    finalScopeCursor = it.finalScopeCursor,
                    finalChecksumJSON = it.finalChecksumJSON,
                )
            }
            RebuildStateInspection(attempts, receipts)
        }

    /** Returns normalized facts for at most [limit] durable rebuild page receipts. */
    internal fun inspectRebuildReceipts(
        limit: Int = MAXIMUM_INSPECTION_RECORDS,
    ): List<RebuildReceiptInspection> = database.readTransaction { db ->
        val grouped = SynchroMeta.listRebuildPageReceipts(db, limit)
            .groupBy { RebuildReceiptGroupKey(it.scopeID, it.rebuildID) }
        grouped.keys.sortedWith { left, right ->
            val scopeOrder = compareUTF8(left.scopeID, right.scopeID)
            if (scopeOrder != 0) scopeOrder else compareUTF8(left.rebuildID, right.rebuildID)
        }.map { key -> inspectRebuildReceipts(db, grouped[key].orEmpty()) }
    }

    private fun inspectRebuildReceipts(
        db: SQLiteDatabase,
        receipts: List<LocalRebuildPageReceipt>,
    ): RebuildReceiptInspection {
        val first = receipts.firstOrNull() ?: return RebuildReceiptInspection(
            rebuildIDFingerprint = "",
            pageCount = 0,
            returnedRecordCount = 0,
            requestChainExpected = emptyList(),
            requestChainObserved = emptyList(),
            recordIdentitiesHex = emptyList(),
            receivedRowChecksums = emptyList(),
            computedRowChecksums = emptyList(),
            computedScopeChecksum = null,
            finalScopeChecksum = null,
            storedScopeChecksum = null,
            localScopeChecksum = null,
        )
        val decoded = receipts.map { receipt ->
            DecodedRebuildReceipt(
                receipt = receipt,
                request = decodeExactReceiptJSON<RebuildRequest>(receipt.requestJSON, ReceiptJSONType.REQUEST),
                response = decodeExactReceiptJSON<RebuildResponse>(receipt.responseJSON, ReceiptJSONType.RESPONSE),
                finalChecksum = receipt.finalChecksumJSON?.let {
                    decodeExactReceiptJSON<ChecksumObject>(it, ReceiptJSONType.CHECKSUM)
                },
            )
        }

        val requestChainExpected = mutableListOf<String>()
        val requestChainObserved = mutableListOf<String>()
        fun appendChain(expected: String?, observed: String?) {
            requestChainExpected += expected ?: "null"
            requestChainObserved += observed ?: "null"
        }
        val requestCursorIndexes = mutableMapOf<String, MutableList<Int>>()
        decoded.forEachIndexed { index, item ->
            requestCursorIndexes.getOrPut(cursorKey(item.receipt.requestCursor), ::mutableListOf) += index
            appendChain(item.receipt.scopeID, item.request.scope)
            appendChain(
                TransportObservationCollector.cursorFingerprint(item.receipt.rebuildID),
                TransportObservationCollector.cursorFingerprint(item.request.rebuildID),
            )
            appendChain(item.receipt.requestCursor?.let(TransportObservationCollector::cursorFingerprint), item.request.cursor?.let(TransportObservationCollector::cursorFingerprint))
            appendChain(item.receipt.scopeID, item.response.scope)
            appendChain(item.receipt.isFinal.toString(), (!item.response.hasMore).toString())
            appendChain(
                (if (item.response.hasMore) null else item.response.finalScopeCursor)?.let(TransportObservationCollector::cursorFingerprint),
                item.receipt.finalScopeCursor?.let(TransportObservationCollector::cursorFingerprint),
            )
            appendChain(item.response.checksum?.let(::checksumKey), item.finalChecksum?.let(::checksumKey))
            appendChain(if (item.response.hasMore) "cursor" else "final", if (item.response.cursor == null) "final" else "cursor")
            appendChain(
                if (item.response.hasMore) "no-final-cursor" else "final-cursor",
                if (item.response.finalScopeCursor == null) "no-final-cursor" else "final-cursor",
            )
            appendChain(if (item.response.hasMore) "no-checksum" else "checksum", if (item.response.checksum == null) "no-checksum" else "checksum")
        }

        val orderedIndexes = mutableListOf<Int>()
        val consumed = mutableSetOf<Int>()
        var expectedCursor: String? = null
        var finalPageCount = 0
        while (true) {
            val indexes = requestCursorIndexes[cursorKey(expectedCursor)]
            if (indexes?.size != 1) break
            val index = indexes.single()
            if (!consumed.add(index)) break
            val item = decoded[index]
            orderedIndexes += index
            if (item.response.hasMore) {
                val nextCursor = item.response.cursor
                if (nextCursor == null) {
                    break
                }
                expectedCursor = nextCursor
            } else {
                finalPageCount += 1
                break
            }
        }
        appendChain(decoded.size.toString(), consumed.size.toString())
        appendChain("1", finalPageCount.toString())
        appendChain("final", orderedIndexes.lastOrNull()?.let { if (decoded[it].response.hasMore) "partial" else "final" })

        val traversalIndexes = if (consumed.size == decoded.size) {
            orderedIndexes
        } else {
            decoded.indices.sortedWith { left, right ->
                compareUTF8(cursorKey(decoded[left].receipt.requestCursor), cursorKey(decoded[right].receipt.requestCursor))
            }
        }
        var returnedRecordCount = 0
        val recordIdentitiesHex = mutableListOf<String>()
        val receivedRowChecksums = mutableListOf<String>()
        val computedRowChecksums = mutableListOf<String>()
        val entries = mutableListOf<Pair<ByteArray, ChecksumObject>>()
        val schemaCache = mutableMapOf<String, Map<String, LocalSchemaTable>>()
        traversalIndexes.forEach { index ->
            val item = decoded[index]
            returnedRecordCount += item.response.records.size
            val schemaKey = "${item.request.schema.version}:${item.request.schema.hash}"
            val tables = schemaCache.getOrPut(schemaKey) {
                archivedSchemaTables(db, item.request.schema).associateByUniqueTableID()
            }
            item.response.records.forEach { record ->
                val table = tables[record.table]
                    ?: throw SynchroError.InvalidResponse("rebuild receipt table metadata is missing")
                val digest = try {
                    Integrity.rowDigest(
                        item.request.schema.hash,
                        table,
                        record.pk,
                        record.row,
                        record.serverVersion,
                    )
                } catch (_: Exception) {
                    throw SynchroError.InvalidResponse("rebuild receipt record metadata is invalid")
                }
                recordIdentitiesHex += digest.identity.joinToString("") { "%02x".format(it.toInt() and 0xff) }
                receivedRowChecksums += checksumKey(record.rowChecksum)
                computedRowChecksums += checksumKey(digest.checksum)
                entries += digest.identity to digest.checksum
            }
        }

        val finalIndexes = decoded.indices.filter { !decoded[it].response.hasMore }
        val finalChecksum = finalIndexes.singleOrNull()?.let { decoded[it].response.checksum }
        val schemaHashes = decoded.map { it.request.schema.hash }.toSet()
        val computedScopeChecksum = schemaHashes.singleOrNull()?.let { schemaHash ->
            Integrity.scopeDigest(
                schemaHash,
                first.scopeID,
                entries.sortedWith { left, right -> compareUnsigned(left.first, right.first) },
            ).let(::checksumKey)
        }
        val scope = SynchroMeta.getScope(db, first.scopeID)
        val storedScopeChecksum = scope?.checksum?.let {
            checksumKey(decodeExactReceiptJSON(it, ReceiptJSONType.CHECKSUM))
        }
        val localScopeChecksum = scope?.localChecksum?.let {
            checksumKey(decodeExactReceiptJSON(it, ReceiptJSONType.CHECKSUM))
        }
        return RebuildReceiptInspection(
            rebuildIDFingerprint = TransportObservationCollector.cursorFingerprint(first.rebuildID),
            pageCount = receipts.size,
            returnedRecordCount = returnedRecordCount,
            requestChainExpected = requestChainExpected,
            requestChainObserved = requestChainObserved,
            recordIdentitiesHex = recordIdentitiesHex,
            receivedRowChecksums = receivedRowChecksums,
            computedRowChecksums = computedRowChecksums,
            computedScopeChecksum = computedScopeChecksum,
            finalScopeChecksum = finalChecksum?.let(::checksumKey),
            storedScopeChecksum = storedScopeChecksum,
            localScopeChecksum = localScopeChecksum,
        )
    }

    private fun checksumKey(value: ChecksumObject): String =
        "${value.algorithm}:${value.version}:${value.encoding}:${value.digest}"

    private inline fun <reified T> decodeExactReceiptJSON(source: String, type: ReceiptJSONType): T = try {
        Integrity.validateCanonicalWireJSON(source)
        val element = RECEIPT_JSON.parseToJsonElement(source)
        validateReceiptJSONShape(element, type)
        RECEIPT_JSON.decodeFromString<T>(source)
    } catch (_: Exception) {
        throw SynchroError.InvalidResponse("rebuild receipt JSON is invalid")
    }

    private fun validateReceiptJSONShape(element: kotlinx.serialization.json.JsonElement, type: ReceiptJSONType) {
        val objectValue = element as? JsonObject
            ?: throw SynchroError.InvalidResponse("rebuild receipt JSON shape is invalid")
        when (type) {
            ReceiptJSONType.REQUEST -> {
                requireReceiptKeys(
                    objectValue,
                    setOf("client_id", "client_generation", "schema", "scope", "rebuild_id", "limit"),
                    setOf("cursor"),
                )
                val schema = objectValue["schema"] as? JsonObject
                    ?: throw SynchroError.InvalidResponse("rebuild receipt request schema is invalid")
                requireReceiptKeys(schema, setOf("version", "hash"))
            }
            ReceiptJSONType.RESPONSE -> {
                requireReceiptKeys(
                    objectValue,
                    setOf("scope", "records", "has_more"),
                    setOf("cursor", "final_scope_cursor", "checksum"),
                )
                val records = objectValue["records"] as? JsonArray
                    ?: throw SynchroError.InvalidResponse("rebuild receipt records are invalid")
                records.forEach { elementRecord ->
                    val record = elementRecord as? JsonObject
                        ?: throw SynchroError.InvalidResponse("rebuild receipt record shape is invalid")
                    requireReceiptKeys(record, setOf("table", "pk", "row", "row_checksum", "server_version"))
                    if (record["pk"] !is JsonObject || record["row"] !is JsonObject) {
                        throw SynchroError.InvalidResponse("rebuild receipt record shape is invalid")
                    }
                    val checksum = record["row_checksum"] as? JsonObject
                        ?: throw SynchroError.InvalidResponse("rebuild receipt record shape is invalid")
                    requireReceiptKeys(checksum, CHECKSUM_KEYS)
                }
                objectValue["checksum"]?.takeUnless { it is JsonNull }?.let { checksum ->
                    val checksumObject = checksum as? JsonObject
                        ?: throw SynchroError.InvalidResponse("rebuild receipt checksum shape is invalid")
                    requireReceiptKeys(checksumObject, CHECKSUM_KEYS)
                }
            }
            ReceiptJSONType.CHECKSUM -> requireReceiptKeys(objectValue, CHECKSUM_KEYS)
        }
    }

    private fun requireReceiptKeys(
        value: JsonObject,
        required: Set<String>,
        optional: Set<String> = emptySet(),
    ) {
        if (!value.keys.containsAll(required) || value.keys.any { it !in required && it !in optional }) {
            throw SynchroError.InvalidResponse("rebuild receipt JSON members are invalid")
        }
    }

    private fun archivedSchemaTables(db: SQLiteDatabase, schema: SchemaRef): List<LocalSchemaTable> {
        db.rawQuery(
            "SELECT manifest_json FROM _synchro_schema_archives WHERE schema_version = ? AND schema_hash = ?",
            arrayOf(schema.version.toString(), schema.hash),
        ).use { cursor ->
            if (!cursor.moveToFirst()) {
                throw SynchroError.InvalidResponse("rebuild receipt schema archive is missing")
            }
            return try {
                RECEIPT_JSON.decodeFromString(cursor.getString(0))
            } catch (_: Exception) {
                throw SynchroError.InvalidResponse("rebuild receipt schema archive is invalid")
            }
        }
    }

    private fun List<LocalSchemaTable>.associateByUniqueTableID(): Map<String, LocalSchemaTable> {
        val result = mutableMapOf<String, LocalSchemaTable>()
        forEach { table ->
            if (result.put(table.tableID, table) != null) {
                throw SynchroError.InvalidResponse("rebuild receipt schema archive is invalid")
            }
        }
        return result
    }

    private fun cursorKey(cursor: String?): String = cursor?.let { "value:$it" } ?: "null"

    private fun compareUTF8(left: String, right: String): Int =
        compareUnsigned(left.toByteArray(Charsets.UTF_8), right.toByteArray(Charsets.UTF_8))

    private fun compareUnsigned(left: ByteArray, right: ByteArray): Int {
        val shared = minOf(left.size, right.size)
        for (index in 0 until shared) {
            val difference = (left[index].toInt() and 0xff) - (right[index].toInt() and 0xff)
            if (difference != 0) return difference
        }
        return left.size - right.size
    }

    private fun inspectRebuildAttempts(db: SQLiteDatabase, limit: Int): List<RebuildAttemptInspection> =
        SynchroMeta.listRebuildAttempts(db, limit).map {
            require(it.schemaVersion > 0 && it.schemaHash.matches(SCHEMA_HASH)) {
                "durable rebuild inspection is invalid"
            }
            val schema = SchemaRef(it.schemaVersion, it.schemaHash)
            schema.validate()
            RebuildAttemptInspection(
                scopeID = it.scopeID,
                rebuildID = it.rebuildID,
                clientGeneration = it.clientGeneration,
                schema = schema,
                generation = it.generation,
                cursor = it.cursor,
                pageLimit = it.pageLimit,
            )
        }

    // MARK: - Sync Control

    suspend fun start(options: SyncOptions? = null) = syncEngine.start(options)

    suspend fun stop() = syncEngine.stop()

    suspend fun retry(options: SyncOptions? = null) = syncEngine.retry(options)

    suspend fun resetSchema(options: SyncOptions? = null) = syncEngine.resetSchema(options)

    suspend fun syncNow() = syncEngine.syncNow()

    /** Hosts without activities can forward native foreground state explicitly. */
    fun onApplicationForeground() = syncEngine.onApplicationForeground()

    /** Hosts without activities can forward native background state explicitly. */
    fun onApplicationBackground() = syncEngine.onApplicationBackground()

    // MARK: - Status

    fun onStatusChange(callback: (SyncStatus) -> Unit): Cancellable =
        syncEngine.onStatusChange(callback)

    fun onConflict(callback: (ConflictEvent) -> Unit): Cancellable =
        syncEngine.onConflict(callback)

    fun onSyncEvent(callback: (SyncEvent) -> Unit): Cancellable =
        syncEngine.onEvent(callback)

    private fun String.asRejectedMutationStatus(): MutationStatus = when (this) {
        "conflict" -> MutationStatus.CONFLICT
        "rejected_terminal" -> MutationStatus.REJECTED_TERMINAL
        else -> throw SynchroError.InvalidResponse("retained rejection has an invalid status")
    }

    private fun String.asMutationRejectionCode(): MutationRejectionCode =
        MutationRejectionCode.entries.firstOrNull { it.name.lowercase() == this }
            ?: throw SynchroError.InvalidResponse("retained rejection has an invalid code")

    private companion object {
        const val MAXIMUM_INSPECTION_RECORDS = 512
        val SCHEMA_HASH = Regex("[0-9a-f]{64}")
        val CHECKSUM_KEYS = setOf("algorithm", "version", "encoding", "digest")
        val RECEIPT_JSON = Json { ignoreUnknownKeys = false }
    }

    private data class RebuildReceiptGroupKey(val scopeID: String, val rebuildID: String)

    private data class DecodedRebuildReceipt(
        val receipt: LocalRebuildPageReceipt,
        val request: RebuildRequest,
        val response: RebuildResponse,
        val finalChecksum: ChecksumObject?,
    )

    private enum class ReceiptJSONType { REQUEST, RESPONSE, CHECKSUM }

}

/** Tracks native activity visibility without a JavaScript lifecycle dependency. */
internal class NativeApplicationLifecycleObserver(
    private val onForeground: () -> Unit,
    private val onBackground: () -> Unit,
    private val isChangingConfigurations: (Activity) -> Boolean = { activity -> activity.isChangingConfigurations },
) : Application.ActivityLifecycleCallbacks {
    private var startedActivities = 0
    private var awaitingReplacementActivity = false

    override fun onActivityCreated(activity: Activity, savedInstanceState: Bundle?) = Unit

    override fun onActivityStarted(activity: Activity) {
        val becameVisible = startedActivities++ == 0
        if (becameVisible && !awaitingReplacementActivity) onForeground()
        awaitingReplacementActivity = false
    }

    override fun onActivityResumed(activity: Activity) = Unit

    override fun onActivityPaused(activity: Activity) = Unit

    override fun onActivityStopped(activity: Activity) {
        startedActivities = (startedActivities - 1).coerceAtLeast(0)
        if (startedActivities == 0) {
            awaitingReplacementActivity = isChangingConfigurations(activity)
            if (!awaitingReplacementActivity) onBackground()
        }
    }

    override fun onActivitySaveInstanceState(activity: Activity, outState: Bundle) = Unit

    override fun onActivityDestroyed(activity: Activity) = Unit
}
