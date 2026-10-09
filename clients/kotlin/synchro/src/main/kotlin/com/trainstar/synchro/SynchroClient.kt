@file:OptIn(com.trainstar.synchro.inspection.SynchroProofApi::class)

package com.trainstar.synchro

import android.content.Context
import android.app.Activity
import android.app.Application
import android.database.sqlite.SQLiteDatabase
import android.os.Bundle
import com.trainstar.synchro.inspection.ClientStateCaptureInspection
import com.trainstar.synchro.inspection.MigrationJournalInspection
import com.trainstar.synchro.inspection.PhysicalSchemaColumnInspection
import com.trainstar.synchro.inspection.ClientStateSnapshotInspection
import com.trainstar.synchro.inspection.RebuildAttemptInspection
import com.trainstar.synchro.inspection.RebuildReceiptInspection
import com.trainstar.synchro.inspection.RowMetadataInspection
import com.trainstar.synchro.inspection.ScopeRowInspection
import com.trainstar.synchro.inspection.ScopeStateInspection
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

    /**
     * Runs a write transaction whose synced mutations the server applies all
     * together or not at all. A nested call joins the enclosing group.
     *
     * @throws SynchroError.AtomicGroupInvalid when the group cannot be sent as
     * one request. The local transaction then rolls back.
     */
    fun <T> atomicWriteTransaction(block: (ApplicationTransaction) -> T): T =
        transaction { transaction ->
            val groupID = database.writeTransaction(pushProcessor::beginAtomicGroup)
            val result = block(transaction)
            if (groupID != null) {
                database.writeTransaction { db -> pushProcessor.completeAtomicGroup(db, config.clientID, groupID) }
            }
            result
        }

    fun <T> authoredWriteTransaction(
        tableName: String,
        operation: Operation,
        columnNames: List<String>,
        block: (ApplicationTransaction) -> T,
    ): T = database.applicationAuthoredWriteTransaction(tableName, operation, columnNames, block)

    fun <T> authoredWriteTransaction(
        tableName: String,
        operation: String,
        columnNames: List<String>,
        block: (ApplicationTransaction) -> T,
    ): T = database.applicationAuthoredWriteTransaction(tableName, operation, columnNames, block)

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

    /**
     * Returns the unresolved queue records.
     *
     * Deprecated: use [inspectRetainedMutationRecords]. This method keeps its
     * published behavior.
     */
    fun inspectPendingMutations(): List<PendingMutationInspection> =
        changeTracker.inspectPendingMutations()

    /**
     * Returns every mutation the client retains, including one the server
     * rejected and one that is larger than a push limit. These mutations leave the
     * pending set, so an application that reports the complete retained
     * ledger reads this instead.
     *
     * Deprecated: use [inspectRetainedMutationRecords]. This method keeps its
     * published behavior.
     */
    fun inspectRetainedMutations(): List<PendingMutationInspection> =
        changeTracker.inspectRetainedMutations()

    /**
     * Returns every retained mutation in its stored representation. The Kotlin
     * ledger requires every binding, so each record is [RetainedMutationInspection.Current].
     */
    fun inspectRetainedMutationRecords(): List<RetainedMutationInspection> =
        changeTracker.inspectRetainedMutationRecords()

    /** Returns the exact number of mutations retained for local reconciliation. */
    fun retainedMutationCount(): Int = changeTracker.retainedMutationCount()

    /**
     * Returns the retained terminal outcomes.
     *
     * Deprecated: use [inspectRejectedMutationRecords]. This method keeps its
     * published behavior. It throws [SynchroError.InvalidResponse] when a legacy
     * rejection is present, because a legacy rejection has no exact JSON.
     */
    fun inspectRejectedMutations(): List<RejectedMutationInspection> =
        inspectRejectedMutationRecords().map { record ->
            (record as? RetainedRejectionInspection.Current)?.rejection
                ?: throw SynchroError.InvalidResponse("retained rejection lacks its exact mutation JSON")
        }

    /**
     * Returns the retained terminal outcomes in their stored representation. A
     * rejection stored before the mutation ledger is a [RetainedRejectionInspection.Legacy].
     */
    fun inspectRejectedMutationRecords(): List<RetainedRejectionInspection> =
        database.readTransaction(::inspectRejectedMutations)

    private fun inspectRejectedMutations(db: SQLiteDatabase): List<RetainedRejectionInspection> =
        SynchroMeta.listRejectedMutations(db).map { rejected ->
            val status = rejected.status.asRejectedMutationStatus()
            val code = rejected.code.asMutationRejectionCode()
            val mutationJSON = rejected.mutationJSON
            val rejectionJSON = rejected.rejectionJSON
            when {
                // A rejection stored before the mutation ledger has no exact JSON.
                mutationJSON == null && rejectionJSON == null -> RetainedRejectionInspection.Legacy(
                    LegacyRejectionInspection(
                        mutationID = rejected.mutationID,
                        tableName = rejected.tableName,
                        recordID = rejected.recordID,
                        status = status,
                        code = code,
                        message = rejected.message,
                        serverRowJSON = rejected.serverRowJson,
                        serverVersion = rejected.serverVersion,
                        createdAt = rejected.createdAt,
                        updatedAt = rejected.updatedAt,
                    ),
                )
                mutationJSON == null || rejectionJSON == null ->
                    throw SynchroError.InvalidResponse("retained rejection lacks its exact mutation or rejection JSON")
                else -> RetainedRejectionInspection.Current(
                    RejectedMutationInspection(
                        mutationID = rejected.mutationID,
                        tableName = rejected.tableName,
                        recordID = rejected.recordID,
                        status = status,
                        code = code,
                        message = rejected.message,
                        serverRowJSON = rejected.serverRowJson,
                        serverVersion = rejected.serverVersion,
                        mutationJSON = mutationJSON,
                        rejectionJSON = rejectionJSON,
                        createdAt = rejected.createdAt,
                        updatedAt = rejected.updatedAt,
                    ),
                )
            }
        }

    fun clearRejectedMutations() {
        database.writeTransaction { db -> SynchroMeta.clearRejectedMutations(db) }
    }

    /** Returns the durable schema reference without exposing reserved SQLite state. */
    internal fun inspectCurrentSchema(): SchemaRef? = database.readTransaction(::inspectCurrentSchema)

    private fun inspectCurrentSchema(db: SQLiteDatabase): SchemaRef? {
        val version = SynchroMeta.getInt64(db, MetaKey.SCHEMA_VERSION)
        val hash = SynchroMeta.get(db, MetaKey.SCHEMA_HASH).orEmpty()
        return when {
            version == 0L && hash.isEmpty() -> null
            version > 0L && hash.matches(SCHEMA_HASH) -> {
                val schema = SchemaRef(version, hash)
                schema.validate()
                schema
            }
            else -> throw SynchroError.InvalidResponse("durable schema inspection is invalid")
        }
    }

    /** Returns at most [limit] durable scopes, or fails when that bound is exceeded. */
    internal fun inspectScopeStates(limit: Int = MAXIMUM_INSPECTION_RECORDS): List<ScopeStateInspection> =
        database.readTransaction { db -> inspectScopeStates(db, limit) }

    private fun inspectScopeStates(
        db: SQLiteDatabase,
        limit: Int,
        truncate: Boolean = false,
    ): List<ScopeStateInspection> =
        SynchroMeta.listScopes(db, limit, truncate).map {
            ScopeStateInspection(it.scopeID, it.cursor, it.checksum, it.localChecksum, it.generation)
        }

    /** Returns at most [limit] durable scope-row memberships. */
    internal fun inspectScopeRows(limit: Int = MAXIMUM_INSPECTION_RECORDS): List<ScopeRowInspection> =
        database.readTransaction { db -> inspectScopeRows(db, limit) }

    private fun inspectScopeRows(
        db: SQLiteDatabase,
        limit: Int,
        truncate: Boolean = false,
    ): List<ScopeRowInspection> =
        SynchroMeta.listScopeRows(db, limit, truncate).map {
            ScopeRowInspection(it.scopeID, it.tableName, it.recordID, it.checksum, it.generation)
        }

    /** Returns server metadata for one application-row identity. */
    internal fun inspectRowMetadata(tableName: String, recordID: String): RowMetadataInspection? =
        database.readTransaction { db ->
            db.rawQuery(
                """
                SELECT table_name, record_id, server_version, row_checksum
                FROM _synchro_row_versions
                WHERE table_name = ? AND record_id = ?
                """.trimIndent(),
                arrayOf(tableName, recordID),
            ).use { cursor ->
                if (!cursor.moveToFirst()) return@readTransaction null
                RowMetadataInspection(
                    tableName = cursor.getString(0),
                    recordID = cursor.getString(1),
                    serverVersion = cursor.getString(2),
                    rowChecksum = if (cursor.isNull(3)) null else cursor.getString(3),
                )
            }
        }

    /** Returns one bounded atomic capture with exact durable-state counts. */
    internal fun inspectClientStateCapture(maximumRecords: Int): ClientStateCaptureInspection {
        require(maximumRecords in 0 until Int.MAX_VALUE) { "inspection record limit is invalid" }
        return database.stateInspectionTransaction { db, provenanceMaintenanceWork ->
            inspectClientStateCapture(db, maximumRecords, provenanceMaintenanceWork)
        }
    }

    /**
     * Reads counts, details, and application rows from one read-only snapshot.
     * [readApplicationRows] runs inside that snapshot. The blocking failure is the
     * engine status at snapshot time, because Kotlin does not store it durably.
     */
    internal fun inspectClientStateSnapshot(
        maximumRecords: Int,
        readApplicationRows: (ClientStateCaptureInspection, ApplicationReadTransaction) -> Unit,
    ): ClientStateSnapshotInspection {
        require(maximumRecords in 0 until Int.MAX_VALUE) { "inspection record limit is invalid" }
        return database.stateInspectionTransaction { db, provenanceMaintenanceWork ->
            val capture = inspectClientStateCapture(db, maximumRecords, provenanceMaintenanceWork)
            val snapshot = ClientStateSnapshotInspection(
                capture = capture,
                pendingChangeCount = changeTracker.pendingChangeCount(db),
                retainedMutations = if (capture.mutationLedgerCount <= maximumRecords) {
                    changeTracker.inspectMutations(db, includeTerminal = true)
                } else {
                    null
                },
                rejectedMutations = if (capture.rejectedMutationCount <= maximumRecords) {
                    inspectRejectedMutations(db)
                } else {
                    null
                },
                blockingFailure = (syncEngine.getSyncStatus() as? SyncStatus.Error)?.failure,
            )
            database.applicationRead(db) { transaction -> readApplicationRows(capture, transaction) }
            snapshot
        }
    }

    private fun inspectClientStateCapture(
        db: SQLiteDatabase,
        maximumRecords: Int,
        provenanceMaintenanceWork: Long,
    ): ClientStateCaptureInspection {
        val scopeStateCount = inspectionCount(db, "SELECT COUNT(*) FROM _synchro_scopes")
        val scopeRowCount = inspectionCount(db, "SELECT COUNT(*) FROM _synchro_scope_rows")
        val rebuildAttemptCount = inspectionCount(db, "SELECT COUNT(*) FROM _synchro_rebuild_attempts")
        val rebuildReceiptCount = inspectionCount(db, "SELECT COUNT(*) FROM _synchro_rebuild_page_receipts")
        val rebuildReceiptGroupCount = inspectionCount(
            db,
            "SELECT COUNT(*) FROM (SELECT scope_id, rebuild_id FROM _synchro_rebuild_page_receipts GROUP BY scope_id, rebuild_id)",
        )
        val rowMetadataCount = inspectionCount(db, "SELECT COUNT(*) FROM _synchro_row_versions")
        val scopeStates = if (maximumRecords == 0) emptyList() else {
            inspectScopeStates(db, maximumRecords, truncate = true)
        }
        val scopeRows = if (maximumRecords == 0) emptyList() else {
            inspectScopeRows(db, maximumRecords, truncate = true)
        }
        val rebuildAttempts = if (maximumRecords == 0) emptyList() else {
            inspectRebuildAttempts(db, maximumRecords, truncate = true)
        }
        val rebuildReceipts = if (maximumRecords == 0) emptyList() else {
            inspectRebuildReceipts(db, maximumRecords, limitGroups = true)
        }
        val rowMetadata = if (maximumRecords == 0) emptyList() else {
            SynchroMeta.listRowMetadata(db, maximumRecords, truncate = true).map {
                RowMetadataInspection(it.tableName, it.recordID, it.serverVersion, it.rowChecksumJSON)
            }
        }
        val scopeStatesTruncated = scopeStateCount > maximumRecords
        val scopeRowsTruncated = scopeRowCount > maximumRecords
        val rebuildAttemptsTruncated = rebuildAttemptCount > maximumRecords
        val rebuildReceiptsTruncated = rebuildReceiptGroupCount > maximumRecords
        val rowMetadataTruncated = rowMetadataCount > maximumRecords
        val migration = inspectMigrationJournal(db, maximumRecords)
        val physicalSchema = inspectPhysicalSchema(db, maximumRecords, migration)
        val provenanceCount = inspectionCount(
            db,
            "SELECT COUNT(*) FROM (SELECT table_name, record_id FROM _synchro_scope_rows GROUP BY table_name, record_id)",
        )
        return ClientStateCaptureInspection(
            schema = inspectCurrentSchema(db),
            scopeStates = scopeStates,
            scopeStatesTruncated = scopeStatesTruncated,
            scopeRows = scopeRows,
            scopeRowsTruncated = scopeRowsTruncated,
            rebuildAttempts = rebuildAttempts,
            rebuildAttemptsTruncated = rebuildAttemptsTruncated,
            rebuildReceipts = rebuildReceipts.take(maximumRecords),
            rebuildReceiptsTruncated = rebuildReceiptsTruncated,
            rowMetadata = rowMetadata,
            rowMetadataTruncated = rowMetadataTruncated,
            overflowed = scopeStatesTruncated ||
                scopeRowsTruncated ||
                rebuildAttemptsTruncated ||
                rebuildReceiptsTruncated ||
                rowMetadataTruncated || migration.second || physicalSchema.second,
            applicationRowCount = inspectApplicationRowCount(db),
            mutationLedgerCount = inspectionCount(db, "SELECT COUNT(*) FROM _synchro_pending_changes"),
            mutationOutcomeCount = inspectionCount(
                db,
                "SELECT COUNT(*) FROM _synchro_pending_changes WHERE lifecycle_state IN ('accepted', 'conflict', 'rejected_terminal')",
            ),
            sealedBatchCount = inspectionCount(db, "SELECT COUNT(*) FROM _synchro_push_batches"),
            rejectedMutationCount = inspectionCount(db, "SELECT COUNT(*) FROM _synchro_rejected_mutations"),
            scopeStateCount = scopeStateCount,
            scopeRowCount = scopeRowCount,
            provenanceCount = provenanceCount,
            rowMetadataCount = rowMetadataCount,
            rebuildAttemptCount = rebuildAttemptCount,
            rebuildReceiptCount = rebuildReceiptCount,
            provenanceMaintenanceWorkCursor = provenanceMaintenanceWork,
            migrationJournal = migration.first,
            migrationJournalTruncated = migration.second,
            physicalSchema = physicalSchema.first,
            physicalSchemaTruncated = physicalSchema.second,
        )
    }

    private fun inspectMigrationJournal(db: SQLiteDatabase, maximumRecords: Int): Pair<MigrationJournalInspection?, Boolean> {
        val preflight = db.rawQuery("""
            SELECT typeof(source_schema_version) = 'integer' AND typeof(target_schema_version) = 'integer' AND
                typeof(journal_version) = 'integer' AND typeof(migration_plan_version) = 'integer' AND
                typeof(reset_materialization) = 'integer' AND typeof(source_schema_hash) = 'text' AND
                typeof(target_schema_hash) = 'text' AND typeof(action) = 'text' AND typeof(phase) = 'text' AND
                typeof(target_manifest_json) = 'text' AND typeof(affected_scopes_json) = 'text' AND
                typeof(scope_cursor_updates_json) = 'text' AND typeof(target_tables_json) = 'text' AND
                typeof(migration_plan_json) = 'text' AND typeof(migration_plan_hash) = 'text' AS storage_valid,
                CASE WHEN ? = 0 THEN 0 ELSE
                coalesce(length(CAST(source_schema_version AS BLOB)), 0) + coalesce(length(CAST(target_schema_version AS BLOB)), 0) +
                coalesce(length(CAST(journal_version AS BLOB)), 0) + coalesce(length(CAST(target_manifest_json AS BLOB)), 0) +
                coalesce(length(CAST(affected_scopes_json AS BLOB)), 0) + coalesce(length(CAST(scope_cursor_updates_json AS BLOB)), 0) +
                coalesce(length(CAST(target_tables_json AS BLOB)), 0) + coalesce(length(CAST(migration_plan_version AS BLOB)), 0) +
                coalesce(length(CAST(migration_plan_json AS BLOB)), 0) + coalesce(length(CAST(migration_plan_hash AS BLOB)), 0) +
                coalesce(length(CAST(reset_materialization AS BLOB)), 0) + coalesce(length(CAST(action AS BLOB)), 0) +
                coalesce(length(CAST(phase AS BLOB)), 0) + coalesce(length(CAST(source_schema_hash AS BLOB)), 0) +
                coalesce(length(CAST(target_schema_hash AS BLOB)), 0) END AS byte_count
            FROM _synchro_migration_journal WHERE singleton = 1
            """.trimIndent(), arrayOf(maximumRecords.toString())).use { cursor ->
            if (!cursor.moveToFirst()) return null to false
            if (cursor.getType(0) != android.database.Cursor.FIELD_TYPE_INTEGER ||
                cursor.getType(1) != android.database.Cursor.FIELD_TYPE_INTEGER
            ) throw SynchroError.InvalidResponse("migration inspection preflight is invalid")
            (cursor.getInt(0) == 1) to cursor.getLong(1)
        }
        val byteCount = preflight.second
        if (maximumRecords == 0 || byteCount > 65_536) return null to true
        if (!preflight.first || byteCount < 0) throw SynchroError.InvalidResponse("migration inspection journal storage is invalid")
        return db.rawQuery("""
            SELECT source_schema_version, source_schema_hash, target_schema_version, target_schema_hash,
                   action, phase, journal_version, target_manifest_json, affected_scopes_json,
                   scope_cursor_updates_json, target_tables_json, migration_plan_version,
                   migration_plan_json, migration_plan_hash, reset_materialization
            FROM _synchro_migration_journal WHERE singleton = 1
            """.trimIndent(), null).use { cursor ->
            if (!cursor.moveToFirst()) throw SynchroError.InvalidResponse("migration inspection journal is missing")
            fun text(key: String): String {
                val index = cursor.getColumnIndexOrThrow(key)
                if (cursor.getType(index) != android.database.Cursor.FIELD_TYPE_STRING) {
                    throw SynchroError.InvalidResponse("migration inspection journal text is invalid")
                }
                return cursor.getString(index)
            }
            fun number(key: String): Long {
                val index = cursor.getColumnIndexOrThrow(key)
                if (cursor.getType(index) != android.database.Cursor.FIELD_TYPE_INTEGER) {
                    throw SynchroError.InvalidResponse("migration inspection journal integer is invalid")
                }
                return cursor.getLong(index)
            }
            val keys = listOf("target_manifest_json", "affected_scopes_json", "scope_cursor_updates_json",
                "target_tables_json", "migration_plan_json", "migration_plan_hash")
            val stored = keys.associateWith(::text).toMutableMap()
            for (key in listOf("journal_version", "migration_plan_version", "reset_materialization")) {
                stored[key] = number(key).toString()
            }
            val action = text("action")
            val phase = text("phase")
            val sourceHash = text("source_schema_hash")
            val targetHash = text("target_schema_hash")
            MigrationJournalInspection(
                SchemaRef(number("source_schema_version"), sourceHash),
                SchemaRef(number("target_schema_version"), targetHash), action, phase, stored,
            ) to false
        }
    }

    private fun inspectPhysicalSchema(
        db: SQLiteDatabase,
        maximumRecords: Int,
        migration: Pair<MigrationJournalInspection?, Boolean>,
    ): Pair<List<PhysicalSchemaColumnInspection>, Boolean> {
        if (migration.second) return emptyList<PhysicalSchemaColumnInspection>() to true
        val source: Pair<List<LocalSchemaTable>?, Boolean>
        var targetNames = emptyList<String>()
        val journal = migration.first
        if (journal != null) {
            try {
                if (journal.source != SchemaRef(0, "")) journal.source.validate()
                journal.target.validate()
                val encoded = journal.stored["target_manifest_json"]
                    ?: throw SynchroError.InvalidResponse("migration inspection target is missing")
                val target = RECEIPT_JSON.decodeFromString<SchemaManifest>(encoded)
                target.validate()
                if (SchemaRef(target.schemaVersion, target.schemaHash) != journal.target ||
                    Integrity.schemaManifestHash(target) != journal.target.hash
                ) throw SynchroError.InvalidResponse("migration inspection target binding is invalid")
                targetNames = target.tables.map { it.name }
                source = if (journal.source.version == 0L) {
                    emptyList<LocalSchemaTable>() to false
                } else {
                    inspectSchemaProjection(db, maximumRecords,
                        "SELECT manifest_json AS metadata FROM _synchro_schema_archives WHERE schema_version = ? AND schema_hash = ?",
                        arrayOf(journal.source.version.toString(), journal.source.hash)).also {
                        if (it.first.isNullOrEmpty() && !it.second) {
                            throw SynchroError.InvalidResponse("migration inspection source archive is missing")
                        }
                    }
                }
            } catch (_: Exception) {
                throw SynchroError.InvalidResponse("migration inspection identities are invalid")
            }
        } else {
            source = inspectSchemaProjection(db, maximumRecords,
                "SELECT value AS metadata FROM _synchro_meta WHERE key = 'local_schema'", null)
        }
        if (source.second) return emptyList<PhysicalSchemaColumnInspection>() to true
        val tables = source.first.orEmpty()
        if (tables.map { it.tableID }.toSet().size != tables.size ||
            tables.map { it.tableName }.toSet().size != tables.size ||
            tables.any { it.tableID.isEmpty() || it.relationID.isEmpty() }
        ) throw SynchroError.InvalidResponse("inspection source table identities are invalid")
        val names = (tables.map { it.tableName } + targetNames).toSortedSet()
        if (names.any { it.isEmpty() || '\u0000' in it || it.startsWith("_synchro_", ignoreCase = true) ||
                it.startsWith("sqlite_", ignoreCase = true) }
        ) throw SynchroError.InvalidResponse("inspection table names are invalid")
        if (names.size > maximumRecords) return emptyList<PhysicalSchemaColumnInspection>() to true
        val columns = mutableListOf<PhysicalSchemaColumnInspection>()
        var byteCount = 0L
        for (tableName in names) {
            db.rawQuery("PRAGMA table_info(${SQLiteHelpers.quoteIdentifier(tableName)})", null).use { cursor ->
                val nameColumn = cursor.getColumnIndexOrThrow("name")
                val typeColumn = cursor.getColumnIndexOrThrow("type")
                val notNullColumn = cursor.getColumnIndexOrThrow("notnull")
                val primaryKeyColumn = cursor.getColumnIndexOrThrow("pk")
                while (cursor.moveToNext()) {
                    if (columns.size == maximumRecords) return columns to true
                    val name = cursor.getString(nameColumn)
                    val type = cursor.getString(typeColumn)
                    byteCount += (tableName.toByteArray(Charsets.UTF_8).size.toLong() +
                        name.toByteArray(Charsets.UTF_8).size + type.toByteArray(Charsets.UTF_8).size)
                    if (byteCount > 65_536) return columns to true
                    columns += PhysicalSchemaColumnInspection(tableName, name, type,
                        cursor.getInt(notNullColumn) != 0, cursor.getInt(primaryKeyColumn))
                }
            }
        }
        return columns to false
    }

    private fun inspectSchemaProjection(
        db: SQLiteDatabase,
        maximumRecords: Int,
        sql: String,
        arguments: Array<String>?,
    ): Pair<List<LocalSchemaTable>?, Boolean> {
        val byteCount = db.rawQuery("SELECT length(CAST(metadata AS BLOB)) FROM ($sql)", arguments).use { cursor ->
            if (!cursor.moveToFirst()) return null to false
            cursor.getLong(0)
        }
        if (maximumRecords == 0 || byteCount > 65_536) return null to true
        if (byteCount < 0) throw SynchroError.InvalidResponse("inspection schema projection is invalid")
        val encoded = db.rawQuery(sql, arguments).use { cursor ->
            if (!cursor.moveToFirst()) throw SynchroError.InvalidResponse("inspection schema projection is missing")
            cursor.getString(0)
        }
        val tables = try {
            RECEIPT_JSON.decodeFromString<List<LocalSchemaTable>>(encoded)
        } catch (_: Exception) {
            throw SynchroError.InvalidResponse("inspection schema projection is invalid")
        }
        if (tables.size > maximumRecords) return null to true
        return tables to false
    }

    internal fun inspectProvenanceMaintenanceWorkCursor(): Long =
        database.provenanceMaintenanceWorkCursor()

    private fun inspectionCount(db: SQLiteDatabase, sql: String): Int =
        db.rawQuery(sql, null).use { cursor ->
            require(cursor.moveToFirst()) { "durable state count is absent" }
            val count = cursor.getLong(0)
            require(count in 0..Int.MAX_VALUE.toLong()) { "durable state count is invalid" }
            count.toInt()
        }

    private fun inspectApplicationRowCount(db: SQLiteDatabase): Int {
        val encoded = SynchroMeta.get(db, MetaKey.LOCAL_SCHEMA) ?: return 0
        val tables = try {
            RECEIPT_JSON.decodeFromString<List<LocalSchemaTable>>(encoded)
        } catch (_: Exception) {
            throw SynchroError.InvalidResponse("stored local schema is invalid")
        }
        var total = 0L
        for (table in tables) {
            total += inspectionCount(
                db,
                "SELECT COUNT(*) FROM ${SQLiteHelpers.quoteIdentifier(table.tableName)}",
            )
            if (total > Int.MAX_VALUE) throw SynchroError.InvalidResponse("application row count is invalid")
        }
        return total.toInt()
    }

    /** Returns bounded read-only state for unfinished rebuild work. */
    internal fun inspectRebuildAttempts(
        limit: Int = MAXIMUM_INSPECTION_RECORDS,
    ): List<RebuildAttemptInspection> = database.readTransaction { db -> inspectRebuildAttempts(db, limit) }

    /** Returns normalized facts for at most [limit] durable rebuild page receipts. */
    internal fun inspectRebuildReceipts(
        limit: Int = MAXIMUM_INSPECTION_RECORDS,
    ): List<RebuildReceiptInspection> = database.readTransaction { db -> inspectRebuildReceipts(db, limit) }

    private fun inspectRebuildReceipts(
        db: SQLiteDatabase,
        limit: Int,
        limitGroups: Boolean = false,
    ): List<RebuildReceiptInspection> {
        val grouped = SynchroMeta.listRebuildPageReceipts(db, limit, limitGroups)
            .groupBy { RebuildReceiptGroupKey(it.scopeID, it.rebuildID) }
        return grouped.keys.sortedWith { left, right ->
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
                recordIdentitiesHex += Integrity.hex(digest.identity)
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
        // Scope invalidation stores an absent local checksum as empty text.
        val localScopeChecksum = scope?.localChecksum?.takeIf { it.isNotEmpty() }?.let {
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

    private inline fun <reified T> decodeExactReceiptJSON(source: String, type: ReceiptJSONType): T {
        try {
            Integrity.validateCanonicalWireJSON(source)
        } catch (_: Exception) {
            throw SynchroError.InvalidResponse(
                "rebuild ${type.diagnosticName} receipt canonical JSON check failed",
            )
        }
        val element = try {
            RECEIPT_JSON.parseToJsonElement(source)
        } catch (_: Exception) {
            throw SynchroError.InvalidResponse(
                "rebuild ${type.diagnosticName} receipt exact shape check failed",
            )
        }
        try {
            validateReceiptJSONShape(element, type)
        } catch (_: Exception) {
            throw SynchroError.InvalidResponse(
                "rebuild ${type.diagnosticName} receipt exact shape check failed",
            )
        }
        return try {
            RECEIPT_JSON.decodeFromString<T>(source)
        } catch (_: Exception) {
            throw SynchroError.InvalidResponse(
                "rebuild ${type.diagnosticName} receipt deserialization check failed",
            )
        }
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

    private fun inspectRebuildAttempts(
        db: SQLiteDatabase,
        limit: Int,
        truncate: Boolean = false,
    ): List<RebuildAttemptInspection> =
        SynchroMeta.listRebuildAttempts(db, limit, truncate).map {
            require(it.schemaVersion > 0 && it.schemaHash.matches(SCHEMA_HASH)) {
                "durable rebuild inspection is invalid"
            }
            val schema = SchemaRef(it.schemaVersion, it.schemaHash)
            schema.validate()
            RebuildAttemptInspection(
                scopeID = it.scopeID,
                rebuildID = it.rebuildID,
                clientGeneration = it.clientGeneration,
                schemaVersion = schema.version,
                schemaHash = schema.hash,
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

    /**
     * Delivers each status synchronously from engine work, in transition order.
     * A callback must return before the application calls [start], [stop], [retry],
     * [resetSchema], [syncNow], or [close]. A synchronous call from a callback throws
     * [IllegalStateException] before it has an effect. Schedule the call independently,
     * for example on another coroutine, and do not block the callback until it finishes.
     */
    fun onStatusChange(callback: (SyncStatus) -> Unit): Cancellable =
        syncEngine.onStatusChange(callback)

    /** Delivers conflicts under the synchronous callback rule of [onStatusChange]. */
    fun onConflict(callback: (ConflictEvent) -> Unit): Cancellable =
        syncEngine.onConflict(callback)

    /** Delivers sync events under the synchronous callback rule of [onStatusChange]. */
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

    private enum class ReceiptJSONType(val diagnosticName: String) {
        REQUEST("request"),
        RESPONSE("response"),
        CHECKSUM("checksum"),
    }

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
