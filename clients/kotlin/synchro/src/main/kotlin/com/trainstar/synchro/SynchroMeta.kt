package com.trainstar.synchro

import android.database.sqlite.SQLiteDatabase
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.time.Instant
import java.time.format.DateTimeFormatter

enum class MetaKey(val key: String) {
    CHECKPOINT("checkpoint"),
    SCHEMA_VERSION("schema_version"),
    SCHEMA_HASH("schema_hash"),
    LOCAL_SCHEMA("local_schema"),
    CLIENT_ID("client_id"),
    CLIENT_SERVER_ID("client_server_id"),
    CLIENT_GENERATION("client_generation"),
    SCHEMA_MANIFEST("schema_manifest"),
    SCOPE_SET_VERSION("scope_set_version"),
    SNAPSHOT_COMPLETE("snapshot_complete"),
    SYNC_LOCK("sync_lock")
}

data class LocalScopeState(
    val scopeID: String,
    val cursor: String?,
    val checksum: String?,
    val generation: Long,
    val localChecksum: String,
)

data class LocalRebuildAttempt(
    val scopeID: String,
    val rebuildID: String,
    val clientGeneration: Long,
    val schemaVersion: Long,
    val schemaHash: String,
    val generation: Long,
    val cursor: String?,
    val pageLimit: Int,
)

data class LocalRebuildPageReceipt(
    val scopeID: String,
    val rebuildID: String,
    val requestCursor: String?,
    val requestJSON: String,
    val responseJSON: String,
    val isFinal: Boolean,
    val finalScopeCursor: String?,
    val finalChecksumJSON: String?,
)

data class LocalScopeRow(
    val scopeID: String,
    val tableName: String,
    val recordID: String,
    val checksum: String,
    val generation: Long,
)

data class LocalRowMetadata(
    val tableName: String,
    val recordID: String,
    val serverVersion: String,
    val rowChecksumJSON: String?,
)

data class LocalSeedReceipt(
    val scopeID: String,
    val receipt: String,
    val schemaVersion: Long,
    val schemaHash: String,
    val cardinality: Long,
    val checksumJSON: String,
)

data class LocalSeedScopeRow(
    val tableName: String,
    val recordID: String,
    val checksum: String,
    val generation: Long,
    val serverVersion: String?,
    val rowChecksumJSON: String?,
)

data class LocalRejectedMutation(
    val mutationID: String,
    val tableName: String,
    val recordID: String,
    val status: String,
    val code: String,
    val message: String?,
    val serverRowJson: String?,
    val serverVersion: String?,
    val mutationJSON: String?,
    val rejectionJSON: String?,
    val createdAt: String,
    val updatedAt: String,
)

data class DurableClientState(
    val lifecycleState: SyncLifecycleState,
    val failure: SyncFailure?,
    val errorAcknowledged: Boolean,
)

internal object SynchroMeta {
    fun get(db: SQLiteDatabase, key: MetaKey): String? {
        db.rawQuery(
            "SELECT value FROM _synchro_meta WHERE key = ?",
            arrayOf(key.key)
        ).use { cursor ->
            return if (cursor.moveToFirst()) cursor.getString(0) else null
        }
    }

    @JvmSynthetic
    internal fun set(db: SQLiteDatabase, key: MetaKey, value: String) {
        db.execSQL(
            """
            INSERT INTO _synchro_meta (key, value) VALUES (?, ?)
            ON CONFLICT (key) DO UPDATE SET value = excluded.value
            """.trimIndent(),
            arrayOf(key.key, value)
        )
    }

    fun getInt64(db: SQLiteDatabase, key: MetaKey): Long {
        val str = get(db, key) ?: return 0L
        return str.toLongOrNull() ?: 0L
    }

    @JvmSynthetic
    internal fun setInt64(db: SQLiteDatabase, key: MetaKey, value: Long) {
        set(db, key, value.toString())
    }

    @JvmSynthetic
    internal fun setSyncLock(db: SQLiteDatabase, locked: Boolean) {
        set(db, MetaKey.SYNC_LOCK, if (locked) "1" else "0")
    }

    fun isSyncLocked(db: SQLiteDatabase): Boolean {
        return get(db, MetaKey.SYNC_LOCK) == "1"
    }

    // MARK: - Durable Client State

    fun getClientState(db: SQLiteDatabase): DurableClientState {
        db.rawQuery(
            """
            SELECT lifecycle_state, error_operation, error_code, error_retryable,
                   error_message, error_recovery_action, error_diagnostics,
                   error_acknowledged
            FROM _synchro_client_state WHERE singleton = 1
            """.trimIndent(),
            null,
        ).use { cursor ->
            if (!cursor.moveToFirst()) {
                throw SynchroError.InvalidResponse("durable client state is missing")
            }
            val lifecycle = SyncLifecycleState.entries.firstOrNull { it.wireName == cursor.getString(0) }
                ?: throw SynchroError.InvalidResponse("durable client state is invalid")
            val operation = if (cursor.isNull(1)) null else cursor.getString(1)
            val code = if (cursor.isNull(2)) null else cursor.getString(2)
            val retryable = if (cursor.isNull(3)) null else cursor.getInt(3).let { value ->
                if (value !in 0..1) throw SynchroError.InvalidResponse("durable client error is invalid")
                value == 1
            }
            val message = if (cursor.isNull(4)) null else cursor.getString(4)
            val recoveryAction = if (cursor.isNull(5)) null else cursor.getString(5)
            val metadataJSON = if (cursor.isNull(6)) null else cursor.getString(6)
            val acknowledged = cursor.getInt(7).let { value ->
                if (value !in 0..1) throw SynchroError.InvalidResponse("durable client state is invalid")
                value == 1
            }
            val failure = when {
                operation == null && code == null && retryable == null &&
                    message == null && recoveryAction == null && metadataJSON == null -> null
                operation != null && code != null && retryable != null &&
                    message != null && recoveryAction != null && metadataJSON != null -> {
                    val operationKind = SyncOperationKind.fromWireName(operation)
                        ?: throw SynchroError.InvalidResponse("durable client error is invalid")
                    val failureCode = SyncFailureCode.fromWireName(code)
                        ?: throw SynchroError.InvalidResponse("durable client error is invalid")
                    val recovery = SyncRecoveryAction.fromWireName(recoveryAction)
                        ?: throw SynchroError.InvalidResponse("durable client error is invalid")
                    try {
                        SyncFailure(
                            operation = operationKind,
                            code = failureCode,
                            retryable = retryable,
                            message = message,
                            recoveryAction = recovery,
                            metadata = decodeMetadata(metadataJSON),
                        )
                    } catch (_: IllegalArgumentException) {
                        throw SynchroError.InvalidResponse("durable client error is invalid")
                    }
                }
                else -> throw SynchroError.InvalidResponse("durable client error is invalid")
            }
            return DurableClientState(lifecycle, failure, acknowledged)
        }
    }

    /**
     * Persist a legal lifecycle transition. A new process may reconstruct work
     * through local_ready after an interrupted in-flight operation.
     */
    @JvmSynthetic
    internal fun transitionClientLifecycleState(
        db: SQLiteDatabase,
        state: SyncLifecycleState,
        processRecovery: Boolean = false,
    ) {
        val current = getClientState(db).lifecycleState
        val allowed = LEGAL_LIFECYCLE_ADJACENCY.getValue(current)
        val recoveryTransition = processRecovery && state == SyncLifecycleState.LOCAL_READY &&
            current in RECOVERABLE_PROCESS_STATES
        if (state !in allowed && !recoveryTransition) {
            throw SynchroError.InvalidStateTransition(current, state)
        }
        db.execSQL(
            """
            UPDATE _synchro_client_state
            SET lifecycle_state = ?, updated_at = ?
            WHERE singleton = 1
            """.trimIndent(),
            arrayOf(state.wireName, timestampNow()),
        )
        requireExactlyOneStateRow(db)
    }

    @JvmSynthetic
    internal fun recordBlockingError(db: SQLiteDatabase, failure: SyncFailure) {
        validateMetadata(failure.metadata)
        val current = getClientState(db).lifecycleState
        if (SyncLifecycleState.ERROR !in LEGAL_LIFECYCLE_ADJACENCY.getValue(current)) {
            throw SynchroError.InvalidStateTransition(current, SyncLifecycleState.ERROR)
        }
        db.execSQL(
            """
            UPDATE _synchro_client_state
            SET lifecycle_state = 'error', error_operation = ?, error_code = ?,
                error_retryable = ?, error_message = ?, error_recovery_action = ?,
                error_diagnostics = ?, error_acknowledged = 0,
                updated_at = ?
            WHERE singleton = 1
            """.trimIndent(),
            arrayOf(
                failure.operation.wireName,
                failure.code.wireName,
                if (failure.retryable) 1 else 0,
                failure.message,
                failure.recoveryAction.wireName,
                Json.encodeToString(failure.metadata),
                timestampNow(),
            ),
        )
        requireExactlyOneStateRow(db)
    }

    /** An explicit recovery action records acknowledgement before reconnecting. */
    @JvmSynthetic
    internal fun acknowledgeBlockingError(db: SQLiteDatabase): DurableClientState {
        val state = getClientState(db)
        if (state.failure == null || state.lifecycleState !in setOf(
                SyncLifecycleState.ERROR,
                SyncLifecycleState.STOPPED,
            )
        ) {
            throw SynchroError.InvalidResponse("no blocking error is available for recovery")
        }
        db.execSQL(
            """
            UPDATE _synchro_client_state
            SET lifecycle_state = 'local_ready', error_acknowledged = 1, updated_at = ?
            WHERE singleton = 1
            """.trimIndent(),
            arrayOf(timestampNow()),
        )
        requireExactlyOneStateRow(db)
        return state
    }

    @JvmSynthetic
    internal fun clearBlockingError(db: SQLiteDatabase) {
        db.execSQL(
            """
            UPDATE _synchro_client_state
            SET error_operation = NULL, error_code = NULL, error_retryable = NULL,
                error_message = NULL, error_recovery_action = NULL,
                error_diagnostics = NULL, error_acknowledged = 0, updated_at = ?
            WHERE singleton = 1
            """.trimIndent(),
            arrayOf(timestampNow()),
        )
        requireExactlyOneStateRow(db)
    }

    private fun decodeMetadata(source: String): Map<String, String> {
        val decoded = try {
            Json.decodeFromString<Map<String, String>>(source)
        } catch (_: Exception) {
            throw SynchroError.InvalidResponse("durable client metadata is invalid")
        }
        validateMetadata(decoded)
        return decoded
    }

    private fun validateMetadata(values: Map<String, String>) {
        if (values.size > 8 || values.any { (key, value) ->
                key.isEmpty() || key.length > 64 || value.length > 128
            }
        ) {
            throw SynchroError.InvalidResponse("durable client metadata is invalid")
        }
    }

    private fun requireExactlyOneStateRow(db: SQLiteDatabase) {
        db.rawQuery("SELECT changes()", null).use { cursor ->
            if (!cursor.moveToFirst() || cursor.getInt(0) != 1) {
                throw SynchroError.InvalidResponse("durable client state is missing")
            }
        }
    }

    private val LEGAL_LIFECYCLE_ADJACENCY = mapOf(
        SyncLifecycleState.UNINITIALIZED to setOf(
            SyncLifecycleState.LOCAL_READY,
            SyncLifecycleState.ERROR,
            SyncLifecycleState.STOPPED,
        ),
        SyncLifecycleState.LOCAL_READY to setOf(
            SyncLifecycleState.CONNECTING,
            SyncLifecycleState.ERROR,
            SyncLifecycleState.STOPPED,
        ),
        SyncLifecycleState.CONNECTING to setOf(
            SyncLifecycleState.SCHEMA_APPLYING,
            SyncLifecycleState.READY,
            SyncLifecycleState.BACKOFF,
            SyncLifecycleState.ERROR,
            SyncLifecycleState.STOPPED,
        ),
        SyncLifecycleState.SCHEMA_APPLYING to setOf(
            SyncLifecycleState.READY,
            SyncLifecycleState.REBUILDING,
            SyncLifecycleState.ERROR,
            SyncLifecycleState.STOPPED,
        ),
        SyncLifecycleState.READY to setOf(
            SyncLifecycleState.CONNECTING,
            SyncLifecycleState.PUSHING,
            SyncLifecycleState.PULLING,
            SyncLifecycleState.REBUILDING,
            SyncLifecycleState.ERROR,
            SyncLifecycleState.STOPPED,
        ),
        SyncLifecycleState.PUSHING to setOf(
            SyncLifecycleState.PUSHING,
            SyncLifecycleState.READY,
            SyncLifecycleState.PULLING,
            SyncLifecycleState.CONNECTING,
            SyncLifecycleState.BACKOFF,
            SyncLifecycleState.ERROR,
            SyncLifecycleState.STOPPED,
        ),
        SyncLifecycleState.PULLING to setOf(
            SyncLifecycleState.PULLING,
            SyncLifecycleState.READY,
            SyncLifecycleState.REBUILDING,
            SyncLifecycleState.CONNECTING,
            SyncLifecycleState.BACKOFF,
            SyncLifecycleState.ERROR,
            SyncLifecycleState.STOPPED,
        ),
        SyncLifecycleState.REBUILDING to setOf(
            SyncLifecycleState.REBUILDING,
            SyncLifecycleState.READY,
            SyncLifecycleState.CONNECTING,
            SyncLifecycleState.BACKOFF,
            SyncLifecycleState.ERROR,
            SyncLifecycleState.STOPPED,
        ),
        SyncLifecycleState.BACKOFF to setOf(
            SyncLifecycleState.CONNECTING,
            SyncLifecycleState.PUSHING,
            SyncLifecycleState.PULLING,
            SyncLifecycleState.REBUILDING,
            SyncLifecycleState.ERROR,
            SyncLifecycleState.STOPPED,
        ),
        SyncLifecycleState.ERROR to setOf(
            SyncLifecycleState.LOCAL_READY,
            SyncLifecycleState.STOPPED,
        ),
        SyncLifecycleState.STOPPED to setOf(SyncLifecycleState.LOCAL_READY),
    )

    private val RECOVERABLE_PROCESS_STATES = setOf(
        SyncLifecycleState.CONNECTING,
        SyncLifecycleState.SCHEMA_APPLYING,
        SyncLifecycleState.READY,
        SyncLifecycleState.PUSHING,
        SyncLifecycleState.PULLING,
        SyncLifecycleState.REBUILDING,
        SyncLifecycleState.BACKOFF,
    )

    // MARK: - Scope State

    fun getAllScopes(db: SQLiteDatabase): List<LocalScopeState> {
        val result = mutableListOf<LocalScopeState>()
        db.rawQuery(
            "SELECT scope_id, cursor, checksum, generation, local_checksum FROM _synchro_scopes ORDER BY scope_id",
            null
        ).use { cursor ->
            while (cursor.moveToNext()) {
                result.add(
                    LocalScopeState(
                        scopeID = cursor.getString(0),
                        cursor = if (cursor.isNull(1)) null else cursor.getString(1),
                        checksum = if (cursor.isNull(2)) null else cursor.getString(2),
                        generation = cursor.getLong(3),
                        localChecksum = cursor.getString(4),
                    )
                )
            }
        }
        return result
    }

    fun listScopes(db: SQLiteDatabase, limit: Int): List<LocalScopeState> {
        requireInspectionLimit(limit)
        val result = mutableListOf<LocalScopeState>()
        db.rawQuery(
            """
            SELECT scope_id, cursor, checksum, generation, local_checksum
            FROM _synchro_scopes
            ORDER BY scope_id
            LIMIT ?
            """.trimIndent(),
            arrayOf((limit + 1).toString()),
        ).use { cursor ->
            while (cursor.moveToNext()) {
                result += LocalScopeState(
                    scopeID = cursor.getString(0),
                    cursor = if (cursor.isNull(1)) null else cursor.getString(1),
                    checksum = if (cursor.isNull(2)) null else cursor.getString(2),
                    generation = cursor.getLong(3),
                    localChecksum = cursor.getString(4),
                )
            }
        }
        require(result.size <= limit) { "Inspection scope result exceeds its bound" }
        return result
    }

    fun getScope(db: SQLiteDatabase, scopeId: String): LocalScopeState? {
        db.rawQuery(
            "SELECT scope_id, cursor, checksum, generation, local_checksum FROM _synchro_scopes WHERE scope_id = ?",
            arrayOf(scopeId)
        ).use { cursor ->
            if (!cursor.moveToFirst()) {
                return null
            }
            return LocalScopeState(
                scopeID = cursor.getString(0),
                cursor = if (cursor.isNull(1)) null else cursor.getString(1),
                checksum = if (cursor.isNull(2)) null else cursor.getString(2),
                generation = cursor.getLong(3),
                localChecksum = cursor.getString(4)
            )
        }
    }

    fun getScopeGeneration(db: SQLiteDatabase, scopeId: String): Long {
        db.rawQuery(
            "SELECT generation FROM _synchro_scopes WHERE scope_id = ?",
            arrayOf(scopeId)
        ).use { cursor ->
            return if (cursor.moveToFirst()) cursor.getLong(0) else 0L
        }
    }

    @JvmSynthetic
    internal fun upsertScope(
        db: SQLiteDatabase,
        scopeId: String,
        cursor: String?,
        checksum: String?,
        generation: Long? = null,
        localChecksum: String? = null
    ) {
        val effectiveGeneration = generation ?: getScopeGeneration(db, scopeId)
        val effectiveLocalChecksum = localChecksum ?: getScopeLocalChecksum(db, scopeId)
        db.execSQL(
            """
            INSERT INTO _synchro_scopes (scope_id, cursor, checksum, generation, local_checksum) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (scope_id) DO UPDATE SET
                cursor = excluded.cursor,
                checksum = excluded.checksum,
                generation = excluded.generation,
                local_checksum = excluded.local_checksum
            """.trimIndent(),
            arrayOf(scopeId, cursor, checksum, effectiveGeneration.toString(), effectiveLocalChecksum)
        )
    }

    @JvmSynthetic
    internal fun bumpScopeGeneration(db: SQLiteDatabase, scopeId: String): Long {
        val nextGeneration = getScopeGeneration(db, scopeId) + 1
        upsertScope(
            db,
            scopeId,
            cursor = null,
            checksum = null,
            generation = nextGeneration,
            localChecksum = ""
        )
        return nextGeneration
    }

    @JvmSynthetic
    internal fun applyScopeCursorUpdates(
        db: SQLiteDatabase,
        updates: Map<String, String?>,
        affectedScopes: List<String>,
    ) {
        val rebuildScopes = affectedScopes.toMutableSet()
        for ((scopeId, cursor) in updates) {
            if (getScope(db, scopeId) == null) {
                throw SynchroError.InvalidResponse("scope cursor update targets an unknown scope $scopeId")
            }
            if (cursor == null) {
                rebuildScopes += scopeId
            } else {
                upsertScope(db, scopeId, cursor, checksum = null)
            }
        }
        for (scopeId in rebuildScopes) {
            if (getScope(db, scopeId) != null) {
                bumpScopeGeneration(db, scopeId)
            }
        }
    }

    @JvmSynthetic
    internal fun deleteScope(db: SQLiteDatabase, scopeId: String) {
        db.execSQL("DELETE FROM _synchro_scopes WHERE scope_id = ?", arrayOf(scopeId))
    }

    @JvmSynthetic
    internal fun clearAllScopes(db: SQLiteDatabase) {
        db.execSQL("DELETE FROM _synchro_scopes")
    }

    @JvmSynthetic
    internal fun invalidateAllScopes(db: SQLiteDatabase) {
        db.execSQL("UPDATE _synchro_scopes SET cursor = NULL, checksum = NULL, generation = 0, local_checksum = ''")
        clearAllScopeRows(db)
    }

    @JvmSynthetic
    internal fun clearAllScopeRows(db: SQLiteDatabase) {
        executeScopeRowUpdate(db, "DELETE FROM _synchro_scope_rows")
        db.execSQL("UPDATE _synchro_scopes SET local_checksum = ''")
    }

    // MARK: - Rejected Mutations

    @JvmSynthetic
    internal fun upsertRejectedMutation(
        db: SQLiteDatabase,
        mutationID: String,
        tableName: String,
        recordId: String,
        status: String,
        code: String,
        message: String?,
        serverRowJson: String?,
        serverVersion: String?,
        mutationJSON: String,
        rejectionJSON: String,
    ) {
        val existing = db.rawQuery(
            """
            SELECT status, code, mutation_json, rejection_json
            FROM _synchro_rejected_mutations WHERE mutation_id = ?
            """.trimIndent(),
            arrayOf(mutationID),
        ).use { cursor ->
            if (!cursor.moveToFirst()) null else listOf(
                cursor.getString(0),
                cursor.getString(1),
                if (cursor.isNull(2)) null else cursor.getString(2),
                if (cursor.isNull(3)) null else cursor.getString(3),
            )
        }
        if (existing != null) {
            if (existing[0] != status || existing[1] != code ||
                (existing[2] != null && existing[2] != mutationJSON) ||
                (existing[3] != null && existing[3] != rejectionJSON)
            ) {
                throw SynchroError.InvalidResponse("rejection persistence has a different terminal outcome")
            }
            if (existing[2] == null || existing[3] == null) {
                db.execSQL(
                    """
                    UPDATE _synchro_rejected_mutations
                    SET mutation_json = COALESCE(mutation_json, ?),
                        rejection_json = COALESCE(rejection_json, ?),
                        updated_at = ?
                    WHERE mutation_id = ?
                    """.trimIndent(),
                    arrayOf(mutationJSON, rejectionJSON, timestampNow(), mutationID),
                )
            }
            return
        }
        val now = timestampNow()
        db.execSQL(
            """
            INSERT INTO _synchro_rejected_mutations
                (mutation_id, table_name, record_id, status, code, message, server_row_json, server_version,
                 mutation_json, rejection_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """.trimIndent(),
            arrayOf(
                mutationID,
                tableName,
                recordId,
                status,
                code,
                message,
                serverRowJson,
                serverVersion,
                mutationJSON,
                rejectionJSON,
                now,
                now,
            )
        )
    }

    fun listRejectedMutations(db: SQLiteDatabase): List<LocalRejectedMutation> {
        val result = mutableListOf<LocalRejectedMutation>()
        db.rawQuery(
            """
            SELECT mutation_id, table_name, record_id, status, code, message, server_row_json, server_version,
                   mutation_json, rejection_json, created_at, updated_at
            FROM _synchro_rejected_mutations
            ORDER BY created_at, mutation_id
            """.trimIndent(),
            null
        ).use { cursor ->
            while (cursor.moveToNext()) {
                result.add(
                    LocalRejectedMutation(
                        mutationID = cursor.getString(0),
                        tableName = cursor.getString(1),
                        recordID = cursor.getString(2),
                        status = cursor.getString(3),
                        code = cursor.getString(4),
                        message = if (cursor.isNull(5)) null else cursor.getString(5),
                        serverRowJson = if (cursor.isNull(6)) null else cursor.getString(6),
                        serverVersion = if (cursor.isNull(7)) null else cursor.getString(7),
                        mutationJSON = if (cursor.isNull(8)) null else cursor.getString(8),
                        rejectionJSON = if (cursor.isNull(9)) null else cursor.getString(9),
                        createdAt = cursor.getString(10),
                        updatedAt = cursor.getString(11),
                    )
                )
            }
        }
        return result
    }

    @JvmSynthetic
    internal fun clearRejectedMutations(db: SQLiteDatabase) {
        db.execSQL("DELETE FROM _synchro_rejected_mutations")
    }

    // MARK: - Scope Rows

    @JvmSynthetic
    internal fun upsertScopeRow(
        db: SQLiteDatabase,
        scopeId: String,
        tableName: String,
        recordId: String,
        checksum: String,
        generation: Long
    ) {
        executeScopeRowUpdate(
            db,
            """
            INSERT INTO _synchro_scope_rows (scope_id, table_name, record_id, checksum, generation) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (scope_id, table_name, record_id) DO UPDATE SET
                checksum = excluded.checksum,
                generation = excluded.generation
            """.trimIndent(),
            arrayOf(scopeId, tableName, recordId, checksum, generation),
        )
    }

    @JvmSynthetic
    internal fun deleteScopeRow(db: SQLiteDatabase, scopeId: String, tableName: String, recordId: String) {
        executeScopeRowUpdate(
            db,
            "DELETE FROM _synchro_scope_rows WHERE scope_id = ? AND table_name = ? AND record_id = ?",
            arrayOf(scopeId, tableName, recordId),
        )
    }

    @JvmSynthetic
    internal fun updateScopeRowChecksum(
        db: SQLiteDatabase,
        scopeId: String,
        tableName: String,
        recordId: String,
        checksum: String,
    ) {
        val statement = db.compileStatement(
            "UPDATE _synchro_scope_rows SET checksum = ? WHERE scope_id = ? AND table_name = ? AND record_id = ?"
        )
        try {
            statement.bindString(1, checksum)
            statement.bindString(2, scopeId)
            statement.bindString(3, tableName)
            statement.bindString(4, recordId)
            val changed = statement.executeUpdateDelete()
            ProvenanceMaintenanceWork.record(db, changed)
            if (changed != 1) {
                throw SynchroError.InvalidResponse("scope row disappeared during schema activation")
            }
        } finally {
            statement.close()
        }
    }

    @JvmSynthetic
    internal fun deleteScopeRows(db: SQLiteDatabase, scopeId: String) {
        executeScopeRowUpdate(db, "DELETE FROM _synchro_scope_rows WHERE scope_id = ?", arrayOf(scopeId))
    }

    fun getScopeRows(db: SQLiteDatabase, scopeId: String): List<Pair<String, String>> {
        val result = mutableListOf<Pair<String, String>>()
        db.rawQuery(
            "SELECT table_name, record_id FROM _synchro_scope_rows WHERE scope_id = ?",
            arrayOf(scopeId)
        ).use { cursor ->
            while (cursor.moveToNext()) {
                result.add(Pair(cursor.getString(0), cursor.getString(1)))
            }
        }
        return result
    }

    fun listScopeRows(db: SQLiteDatabase, limit: Int): List<LocalScopeRow> {
        requireInspectionLimit(limit)
        val result = mutableListOf<LocalScopeRow>()
        db.rawQuery(
            """
            SELECT scope_id, table_name, record_id, checksum, generation
            FROM _synchro_scope_rows
            ORDER BY scope_id, table_name, record_id
            LIMIT ?
            """.trimIndent(),
            arrayOf((limit + 1).toString()),
        ).use { cursor ->
            while (cursor.moveToNext()) {
                result += LocalScopeRow(
                    scopeID = cursor.getString(0),
                    tableName = cursor.getString(1),
                    recordID = cursor.getString(2),
                    checksum = cursor.getString(3),
                    generation = cursor.getLong(4),
                )
            }
        }
        require(result.size <= limit) { "Inspection scope-row result exceeds its bound" }
        return result
    }

    fun getStaleScopeRows(db: SQLiteDatabase, scopeId: String, generation: Long): List<Pair<String, String>> {
        val result = mutableListOf<Pair<String, String>>()
        db.rawQuery(
            "SELECT table_name, record_id FROM _synchro_scope_rows WHERE scope_id = ? AND generation <> ?",
            arrayOf(scopeId, generation.toString())
        ).use { cursor ->
            while (cursor.moveToNext()) {
                result.add(Pair(cursor.getString(0), cursor.getString(1)))
            }
        }
        return result
    }

    @JvmSynthetic
    internal fun deleteStaleScopeRows(db: SQLiteDatabase, scopeId: String, generation: Long) {
        executeScopeRowUpdate(
            db,
            "DELETE FROM _synchro_scope_rows WHERE scope_id = ? AND generation <> ?",
            arrayOf(scopeId, generation),
        )
    }

    private fun executeScopeRowUpdate(
        db: SQLiteDatabase,
        sql: String,
        params: Array<out Any?> = emptyArray(),
    ): Int {
        val statement = db.compileStatement(sql)
        try {
            bindTypedValues(statement, params.toList())
            return statement.executeUpdateDelete().also { ProvenanceMaintenanceWork.record(db, it) }
        } finally {
            statement.close()
        }
    }

    fun hasScopeRows(db: SQLiteDatabase, tableName: String, recordId: String): Boolean {
        db.rawQuery(
            "SELECT 1 FROM _synchro_scope_rows WHERE table_name = ? AND record_id = ? LIMIT 1",
            arrayOf(tableName, recordId)
        ).use { cursor ->
            return cursor.moveToFirst()
        }
    }

    fun getScopeLocalChecksum(db: SQLiteDatabase, scopeId: String): String {
        db.rawQuery(
            "SELECT local_checksum FROM _synchro_scopes WHERE scope_id = ?",
            arrayOf(scopeId)
        ).use { cursor ->
            return if (cursor.moveToFirst()) cursor.getString(0) else ""
        }
    }

    @JvmSynthetic
    internal fun setScopeLocalChecksum(db: SQLiteDatabase, scopeId: String, checksum: String) {
        db.execSQL(
            "UPDATE _synchro_scopes SET local_checksum = ? WHERE scope_id = ?",
            arrayOf(checksum, scopeId)
        )
    }

    data class ScopeRowChecksum(val tableName: String, val recordID: String, val checksum: String)

    fun getScopeRowChecksums(
        db: SQLiteDatabase,
        scopeId: String,
    ): List<ScopeRowChecksum> {
        val result = mutableListOf<ScopeRowChecksum>()
        db.rawQuery(
            "SELECT table_name, record_id, checksum FROM _synchro_scope_rows WHERE scope_id = ?",
            arrayOf(scopeId)
        ).use { cursor ->
            while (cursor.moveToNext()) {
                result += ScopeRowChecksum(cursor.getString(0), cursor.getString(1), cursor.getString(2))
            }
        }
        return result
    }

    @JvmSynthetic
    internal fun upsertRowVersion(
        db: SQLiteDatabase,
        tableName: String,
        recordId: String,
        serverVersion: String,
        rowChecksum: ChecksumObject?,
    ) {
        val checksumJSON = rowChecksum?.let { kotlinx.serialization.json.Json.encodeToString(ChecksumObject.serializer(), it) }
        db.execSQL(
            """
            INSERT INTO _synchro_row_versions (table_name, record_id, server_version, row_checksum)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (table_name, record_id) DO UPDATE SET
                server_version = excluded.server_version,
                row_checksum = excluded.row_checksum
            """.trimIndent(),
            arrayOf(tableName, recordId, serverVersion, checksumJSON)
        )
    }

    fun getRowVersion(db: SQLiteDatabase, tableName: String, recordId: String): String? {
        db.rawQuery(
            "SELECT server_version FROM _synchro_row_versions WHERE table_name = ? AND record_id = ?",
            arrayOf(tableName, recordId)
        ).use { cursor ->
            return if (cursor.moveToFirst()) cursor.getString(0) else null
        }
    }

    fun listRowMetadata(db: SQLiteDatabase, limit: Int): List<LocalRowMetadata> {
        requireInspectionLimit(limit)
        val result = mutableListOf<LocalRowMetadata>()
        db.rawQuery(
            """
            SELECT table_name, record_id, server_version, row_checksum
            FROM _synchro_row_versions
            ORDER BY table_name, record_id
            LIMIT ?
            """.trimIndent(),
            arrayOf((limit + 1).toString()),
        ).use { cursor ->
            while (cursor.moveToNext()) {
                result += LocalRowMetadata(
                    tableName = cursor.getString(0),
                    recordID = cursor.getString(1),
                    serverVersion = cursor.getString(2),
                    rowChecksumJSON = if (cursor.isNull(3)) null else cursor.getString(3),
                )
            }
        }
        require(result.size <= limit) { "Inspection row-metadata result exceeds its bound" }
        return result
    }

    fun getSeedReceipts(db: SQLiteDatabase): Map<String, String> {
        val result = linkedMapOf<String, String>()
        db.rawQuery("SELECT scope_id, receipt FROM _synchro_seed_receipts ORDER BY scope_id", null).use { cursor ->
            while (cursor.moveToNext()) result[cursor.getString(0)] = cursor.getString(1)
        }
        return result
    }

    fun getSeedReceiptStates(db: SQLiteDatabase): List<LocalSeedReceipt> {
        val result = mutableListOf<LocalSeedReceipt>()
        db.rawQuery(
            """
            SELECT scope_id, receipt, schema_version, schema_hash, cardinality, checksum
            FROM _synchro_seed_receipts
            ORDER BY scope_id
            """.trimIndent(),
            null,
        ).use { cursor ->
            while (cursor.moveToNext()) {
                result += LocalSeedReceipt(
                    scopeID = cursor.getString(0),
                    receipt = cursor.getString(1),
                    schemaVersion = cursor.getLong(2),
                    schemaHash = cursor.getString(3),
                    cardinality = cursor.getLong(4),
                    checksumJSON = cursor.getString(5),
                )
            }
        }
        return result
    }

    fun getSeedScopeRows(db: SQLiteDatabase, scopeId: String): List<LocalSeedScopeRow> {
        val result = mutableListOf<LocalSeedScopeRow>()
        db.rawQuery(
            """
            SELECT scope_rows.table_name, scope_rows.record_id, scope_rows.checksum, scope_rows.generation,
                   versions.server_version, versions.row_checksum
            FROM _synchro_scope_rows AS scope_rows
            LEFT JOIN _synchro_row_versions AS versions
              ON versions.table_name = scope_rows.table_name
             AND versions.record_id = scope_rows.record_id
            WHERE scope_rows.scope_id = ?
            ORDER BY scope_rows.table_name, scope_rows.record_id
            """.trimIndent(),
            arrayOf(scopeId),
        ).use { cursor ->
            while (cursor.moveToNext()) {
                result += LocalSeedScopeRow(
                    tableName = cursor.getString(0),
                    recordID = cursor.getString(1),
                    checksum = cursor.getString(2),
                    generation = cursor.getLong(3),
                    serverVersion = if (cursor.isNull(4)) null else cursor.getString(4),
                    rowChecksumJSON = if (cursor.isNull(5)) null else cursor.getString(5),
                )
            }
        }
        return result
    }

    @JvmSynthetic
    internal fun deleteSeedReceipt(db: SQLiteDatabase, scopeId: String) {
        db.execSQL("DELETE FROM _synchro_seed_receipts WHERE scope_id = ?", arrayOf(scopeId))
    }

    @JvmSynthetic
    internal fun clearSeedReceipts(db: SQLiteDatabase) {
        db.execSQL("DELETE FROM _synchro_seed_receipts")
    }

    fun getRebuildAttempt(db: SQLiteDatabase, scopeId: String): LocalRebuildAttempt? {
        db.rawQuery(
            """
            SELECT scope_id, rebuild_id, client_generation, schema_version, schema_hash, generation, cursor, page_limit
            FROM _synchro_rebuild_attempts WHERE scope_id = ?
            """.trimIndent(),
            arrayOf(scopeId)
        ).use { cursor ->
            if (!cursor.moveToFirst()) return null
            return LocalRebuildAttempt(
                scopeID = cursor.getString(0),
                rebuildID = cursor.getString(1),
                clientGeneration = cursor.getLong(2),
                schemaVersion = cursor.getLong(3),
                schemaHash = cursor.getString(4),
                generation = cursor.getLong(5),
                cursor = if (cursor.isNull(6)) null else cursor.getString(6),
                pageLimit = cursor.getInt(7),
            )
        }
    }

    fun listRebuildAttempts(db: SQLiteDatabase, limit: Int): List<LocalRebuildAttempt> {
        requireInspectionLimit(limit)
        val result = mutableListOf<LocalRebuildAttempt>()
        db.rawQuery(
            """
            SELECT scope_id, rebuild_id, client_generation, schema_version, schema_hash, generation, cursor, page_limit
            FROM _synchro_rebuild_attempts
            ORDER BY scope_id
            LIMIT ?
            """.trimIndent(),
            arrayOf((limit + 1).toString()),
        ).use { cursor ->
            while (cursor.moveToNext()) {
                result += LocalRebuildAttempt(
                    scopeID = cursor.getString(0),
                    rebuildID = cursor.getString(1),
                    clientGeneration = cursor.getLong(2),
                    schemaVersion = cursor.getLong(3),
                    schemaHash = cursor.getString(4),
                    generation = cursor.getLong(5),
                    cursor = if (cursor.isNull(6)) null else cursor.getString(6),
                    pageLimit = cursor.getInt(7),
                )
            }
        }
        require(result.size <= limit) { "Inspection rebuild-attempt result exceeds its bound" }
        return result
    }

    fun listRebuildPageReceipts(db: SQLiteDatabase, limit: Int): List<LocalRebuildPageReceipt> {
        requireInspectionLimit(limit)
        val result = mutableListOf<LocalRebuildPageReceipt>()
        db.rawQuery(
            """
            SELECT scope_id, rebuild_id, request_cursor_is_null, request_cursor, request_json,
                   response_json, is_final, final_scope_cursor, final_checksum
            FROM _synchro_rebuild_page_receipts
            ORDER BY scope_id, rebuild_id, request_cursor_is_null, request_cursor
            LIMIT ?
            """.trimIndent(),
            arrayOf((limit + 1).toString()),
        ).use { cursor -> while (cursor.moveToNext()) result += rebuildPageReceipt(cursor) }
        require(result.size <= limit) { "Inspection rebuild-receipt result exceeds its bound" }
        return result
    }

    @JvmSynthetic
    internal fun upsertRebuildAttempt(db: SQLiteDatabase, attempt: LocalRebuildAttempt) {
        db.execSQL(
            """
            INSERT INTO _synchro_rebuild_attempts
                (scope_id, rebuild_id, client_generation, schema_version, schema_hash, generation, cursor, page_limit)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (scope_id) DO UPDATE SET
                rebuild_id = excluded.rebuild_id,
                client_generation = excluded.client_generation,
                schema_version = excluded.schema_version,
                schema_hash = excluded.schema_hash,
                generation = excluded.generation,
                cursor = excluded.cursor,
                page_limit = excluded.page_limit
            """.trimIndent(),
            arrayOf(
                attempt.scopeID, attempt.rebuildID, attempt.clientGeneration, attempt.schemaVersion,
                attempt.schemaHash, attempt.generation, attempt.cursor, attempt.pageLimit,
            )
        )
    }

    @JvmSynthetic
    internal fun deleteRebuildAttempt(db: SQLiteDatabase, scopeId: String) {
        db.execSQL("DELETE FROM _synchro_rebuild_attempts WHERE scope_id = ?", arrayOf(scopeId))
    }

    fun getRebuildPageReceipt(
        db: SQLiteDatabase,
        scopeId: String,
        rebuildId: String,
        requestCursor: String?,
    ): LocalRebuildPageReceipt? {
        db.rawQuery(
            """
            SELECT scope_id, rebuild_id, request_cursor_is_null, request_cursor, request_json,
                   response_json, is_final, final_scope_cursor, final_checksum
            FROM _synchro_rebuild_page_receipts
            WHERE scope_id = ?
              AND rebuild_id = ?
              AND request_cursor_is_null = ?
              AND request_cursor = ?
            """.trimIndent(),
            arrayOf(scopeId, rebuildId, if (requestCursor == null) "1" else "0", requestCursor ?: ""),
        ).use { cursor ->
            return if (cursor.moveToFirst()) rebuildPageReceipt(cursor) else null
        }
    }

    fun getFinalRebuildPageReceipt(
        db: SQLiteDatabase,
        scopeId: String,
        rebuildId: String,
    ): LocalRebuildPageReceipt? {
        db.rawQuery(
            """
            SELECT scope_id, rebuild_id, request_cursor_is_null, request_cursor, request_json,
                   response_json, is_final, final_scope_cursor, final_checksum
            FROM _synchro_rebuild_page_receipts
            WHERE scope_id = ? AND rebuild_id = ? AND is_final = 1
            """.trimIndent(),
            arrayOf(scopeId, rebuildId),
        ).use { cursor ->
            return if (cursor.moveToFirst()) rebuildPageReceipt(cursor) else null
        }
    }

    @JvmSynthetic
    internal fun insertRebuildPageReceipt(
        db: SQLiteDatabase,
        scopeId: String,
        rebuildId: String,
        requestCursor: String?,
        requestJSON: String,
        responseJSON: String,
        finalScopeCursor: String?,
        finalChecksumJSON: String?,
    ) {
        val isFinal = finalScopeCursor != null
        db.execSQL(
            """
            INSERT INTO _synchro_rebuild_page_receipts
                (scope_id, rebuild_id, request_cursor_is_null, request_cursor, request_json,
                 response_json, is_final, final_scope_cursor, final_checksum)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """.trimIndent(),
            arrayOf(
                scopeId,
                rebuildId,
                if (requestCursor == null) 1 else 0,
                requestCursor ?: "",
                requestJSON,
                responseJSON,
                if (isFinal) 1 else 0,
                finalScopeCursor,
                finalChecksumJSON,
            ),
        )
    }

    @JvmSynthetic
    internal fun deleteRebuildPageReceipts(db: SQLiteDatabase, scopeId: String, rebuildId: String) {
        db.execSQL(
            "DELETE FROM _synchro_rebuild_page_receipts WHERE scope_id = ? AND rebuild_id = ?",
            arrayOf(scopeId, rebuildId),
        )
    }

    @JvmSynthetic
    internal fun deleteAllRebuildPageReceipts(db: SQLiteDatabase, scopeId: String) {
        db.execSQL(
            "DELETE FROM _synchro_rebuild_page_receipts WHERE scope_id = ?",
            arrayOf(scopeId),
        )
    }

    private fun rebuildPageReceipt(cursor: android.database.Cursor): LocalRebuildPageReceipt {
        val cursorIsNull = cursor.getInt(2)
        val isFinalValue = cursor.getInt(6)
        if (cursorIsNull !in 0..1 || isFinalValue !in 0..1) {
            throw SynchroError.InvalidResponse("rebuild page receipt is invalid")
        }
        val finalScopeCursor = if (cursor.isNull(7)) null else cursor.getString(7)
        val finalChecksumJSON = if (cursor.isNull(8)) null else cursor.getString(8)
        val isFinal = isFinalValue == 1
        if (isFinal != (finalScopeCursor != null && finalChecksumJSON != null)) {
            throw SynchroError.InvalidResponse("rebuild page receipt finality is invalid")
        }
        return LocalRebuildPageReceipt(
            scopeID = cursor.getString(0),
            rebuildID = cursor.getString(1),
            requestCursor = if (cursorIsNull == 1) null else cursor.getString(3),
            requestJSON = cursor.getString(4),
            responseJSON = cursor.getString(5),
            isFinal = isFinal,
            finalScopeCursor = finalScopeCursor,
            finalChecksumJSON = finalChecksumJSON,
        )
    }

    private fun requireInspectionLimit(limit: Int) {
        require(limit in 1..MAXIMUM_INSPECTION_RECORDS) { "Inspection limit is invalid" }
    }

    private fun timestampNow(): String =
        DateTimeFormatter.ISO_INSTANT.format(Instant.now())

    private const val MAXIMUM_INSPECTION_RECORDS = 512
}
