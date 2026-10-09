package com.trainstar.synchro

import android.database.sqlite.SQLiteDatabase
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

internal class SchemaManager(private val database: SynchroDatabase) {
    private val json = Json {
        ignoreUnknownKeys = true
        encodeDefaults = true
    }

    /**
     * Persists a verified, typed migration plan before any synced-table DDL.
     * The caller applies this journal with assignment state in one later SQLite
     * transaction.
     */
    internal fun prepareConnectMigration(
        response: ConnectResponse,
        targetTables: List<LocalSchemaTable>,
        resetMaterialization: Boolean,
    ): LocalMigrationJournal? {
        if (response.schema.action == SchemaAction.NONE) return null
        if (response.schema.action !in setOf(SchemaAction.REPLACE, SchemaAction.REBUILD_LOCAL)) {
            throw SynchroError.InvalidResponse("connect response has no applicable schema migration")
        }
        val manifest = response.schemaDefinition
            ?: throw SynchroError.InvalidResponse("schema migration has no manifest")
        try {
            manifest.validate()
        } catch (_: ContractException) {
            throw SynchroError.InvalidResponse("schema migration manifest is invalid")
        }
        if (manifest.schemaVersion != response.schema.version ||
            manifest.schemaHash != response.schema.hash ||
            Integrity.schemaManifestHash(manifest) != manifest.schemaHash
        ) {
            throw SynchroError.InvalidResponse("schema migration manifest does not match its reference")
        }

        return database.writeTransaction { db ->
            val source = currentSchemaRef(db)
            val target = SchemaRef(response.schema.version, response.schema.hash)
            if (!resetMaterialization && source == target) return@writeTransaction null
            val existing = loadMigrationJournal(db)
            if (existing != null) {
                validateJournal(existing)
                if (existing.target == target && existing.resetMaterialization == resetMaterialization &&
                    existing.action == response.schema.action && existing.targetManifest == manifest
                ) {
                    return@writeTransaction existing
                }
                // A committed journal holds only rebuild work. The new journal
                // carries that work, so the server schema action can proceed.
                if (existing.phase == MigrationPhase.PREPARED || existing.target != source) {
                    throw SynchroError.InvalidResponse("a different schema migration is already pending")
                }
                applyPreparedMigrationInTransaction(db)
                db.execSQL("DELETE FROM _synchro_migration_journal WHERE singleton = 1")
            }
            val carriedScopes = existing?.affectedScopes.orEmpty().filterNot { scopeRebuildFinal(db, it) }
            val sourceTables = sourceProjection(db, source)
            validateMigrationSource(db, sourceTables, targetTables)
            val affected = (response.affectedScopes.orEmpty() + carriedScopes).distinct().sortedWith(unsignedUTF8Comparator)
            val updates = response.scopeCursorUpdates.toMutableMap()
            affected.forEach { updates[it] = null }
            val plan = buildMigrationPlan(source, target, sourceTables, targetTables, resetMaterialization)
            val planJSON = json.encodeToString(plan)
            val journal = LocalMigrationJournal(
                journalVersion = MIGRATION_JOURNAL_VERSION,
                source = source,
                target = target,
                action = if (affected.isEmpty()) SchemaAction.REPLACE else SchemaAction.REBUILD_LOCAL,
                affectedScopes = affected,
                scopeCursorUpdates = updates.toSortedMap(),
                targetManifest = manifest,
                targetTables = targetTables.sortedWith(compareBy { it.tableID }),
                plan = plan,
                planHash = migrationPlanHash(planJSON),
                resetMaterialization = resetMaterialization,
                phase = MigrationPhase.PREPARED,
            )
            validateJournal(journal)
            persistMigrationJournal(db, journal, planJSON)
            journal
        }
    }

    /** Applies only the journal that was committed before local DDL. */
    internal fun applyPreparedMigrationInTransaction(db: SQLiteDatabase): LocalMigrationJournal? {
        val journal = loadMigrationJournal(db) ?: return null
        validateJournal(journal)
        val current = currentSchemaRef(db)
        val sourceTables = sourceProjection(db, journal.source)
        if (journal.plan != buildMigrationPlan(journal.source, journal.target, sourceTables, journal.targetTables, journal.resetMaterialization)) {
            throw SynchroError.InvalidResponse("schema migration plan is not authorized by its source and target")
        }
        when {
            current == journal.source && journal.phase == MigrationPhase.PREPARED -> {
                validateMigrationSource(db, sourceTables, journal.targetTables)
                applyMigrationPlan(db, journal)
                validateTargetPhysicalSchema(db, journal.targetTables)
                archiveSchemaTables(db, journal.target.version, journal.target.hash, journal.targetTables)
                SynchroMeta.setInt64(db, MetaKey.SCHEMA_VERSION, journal.target.version)
                SynchroMeta.set(db, MetaKey.SCHEMA_HASH, journal.target.hash)
                persistLocalSchemaTables(db, journal.targetTables)
                journal.scopeCursorUpdates.forEach { (scopeID, cursor) ->
                    if (cursor != null && !journal.resetMaterialization) {
                        recomputeRetainedScopeIntegrity(db, scopeID, journal.target.hash, journal.targetTables)
                    }
                }
                SynchroMeta.applyScopeCursorUpdates(
                    db,
                    journal.scopeCursorUpdates.filterNot { (scopeID, cursor) ->
                        cursor == null && scopeID in journal.affectedScopes && SynchroMeta.getScope(db, scopeID) == null
                    },
                    journal.affectedScopes,
                )
                val phase = if (journal.affectedScopes.isEmpty()) {
                    MigrationPhase.DDL_APPLIED
                } else {
                    MigrationPhase.AWAITING_REBUILD
                }
                updateMigrationPhase(db, phase)
                return journal.copy(phase = phase)
            }
            current == journal.target && journal.phase in setOf(
                MigrationPhase.DDL_APPLIED,
                MigrationPhase.AWAITING_REBUILD,
            ) -> {
                if (sourceProjection(db, journal.target) != journal.targetTables) {
                    throw SynchroError.InvalidResponse("schema migration target projection does not match its journal")
                }
                validateTargetPhysicalSchema(db, journal.targetTables)
                return journal
            }
            else -> throw SynchroError.InvalidResponse("schema migration journal does not match local schema state")
        }
    }

    /** Recovery runs before any connect, push, pull, or rebuild request. */
    internal fun recoverPendingMigration() {
        database.writeTransaction { db ->
            applyPreparedMigrationInTransaction(db)
        }
    }

    /** Clears a journal only after every required scope reached verified finality. */
    internal fun completeMigrationIfReady(authoritativeAssignmentsInstalled: Boolean = false) {
        database.writeTransaction { db ->
            var journal = loadMigrationJournal(db) ?: return@writeTransaction
            validateJournal(journal)
            if (journal.plan != buildMigrationPlan(journal.source, journal.target, sourceProjection(db, journal.source), journal.targetTables, journal.resetMaterialization)) {
                throw SynchroError.InvalidResponse("schema migration plan is not authorized by its source and target")
            }
            if (currentSchemaRef(db) != journal.target) {
                throw SynchroError.InvalidResponse("schema migration journal lost its target schema")
            }
            if (sourceProjection(db, journal.target) != journal.targetTables) {
                throw SynchroError.InvalidResponse("schema migration target projection does not match its journal")
            }
            validateTargetPhysicalSchema(db, journal.targetTables)
            if (authoritativeAssignmentsInstalled && journal.phase != MigrationPhase.PREPARED) {
                val absent = journal.affectedScopes.filter { SynchroMeta.getScope(db, it) == null }.toSet()
                if (absent.isNotEmpty()) {
                    val affected = journal.affectedScopes.filterNot { it in absent }
                    journal = journal.copy(
                        affectedScopes = affected,
                        scopeCursorUpdates = journal.scopeCursorUpdates.filterKeys { it !in absent },
                        action = if (affected.isEmpty()) SchemaAction.REPLACE else SchemaAction.REBUILD_LOCAL,
                        phase = if (affected.isEmpty()) MigrationPhase.DDL_APPLIED else MigrationPhase.AWAITING_REBUILD,
                    )
                    db.execSQL(
                        "UPDATE _synchro_migration_journal SET affected_scopes_json = ?, scope_cursor_updates_json = ?, action = ?, phase = ? WHERE singleton = 1",
                        arrayOf(json.encodeToString(affected), json.encodeToString(journal.scopeCursorUpdates), journal.action.name.lowercase(), journal.phase.name.lowercase()),
                    )
                }
            }
            val complete = when (journal.phase) {
                MigrationPhase.DDL_APPLIED -> journal.affectedScopes.isEmpty()
                MigrationPhase.AWAITING_REBUILD -> journal.affectedScopes.all { scopeRebuildFinal(db, it) }
                MigrationPhase.PREPARED -> false
            }
            if (complete) {
                db.execSQL("DELETE FROM _synchro_migration_journal WHERE singleton = 1")
            }
        }
    }

    private fun scopeRebuildFinal(db: SQLiteDatabase, scopeID: String): Boolean {
        val scope = SynchroMeta.getScope(db, scopeID) ?: return false
        return scope.cursor != null && scope.checksum != null && SynchroMeta.getRebuildAttempt(db, scopeID) == null
    }

    fun loadStoredLocalSchema(): List<LocalSchemaTable>? {
        return database.readTransaction { db ->
            val encoded = SynchroMeta.get(db, MetaKey.LOCAL_SCHEMA) ?: return@readTransaction null
            json.decodeFromString<List<LocalSchemaTable>>(encoded)
        }
    }

    internal fun reconcileLocalSchemaInTransaction(
        db: android.database.sqlite.SQLiteDatabase,
        schemaVersion: Long,
        schemaHash: String,
        tables: List<LocalSchemaTable>,
        scopeCursorUpdates: Map<String, String?> = emptyMap(),
        affectedScopes: List<String> = emptyList(),
    ) {
        val localVersion = SynchroMeta.getInt64(db, MetaKey.SCHEMA_VERSION)
        val localHash = SynchroMeta.get(db, MetaKey.SCHEMA_HASH) ?: ""
        val schemaChanged = localVersion != schemaVersion || localHash != schemaHash
        validateSchemaCompatibility(db, tables)
        if (schemaChanged) {
            applyAdditiveSchemaMigration(db, tables)
        }
        archiveSchemaTables(db, schemaVersion, schemaHash, tables)
        SynchroMeta.setInt64(db, MetaKey.SCHEMA_VERSION, schemaVersion)
        SynchroMeta.set(db, MetaKey.SCHEMA_HASH, schemaHash)
        persistLocalSchemaTables(db, tables)
        scopeCursorUpdates.forEach { (scopeId, cursor) ->
            if (cursor != null && schemaChanged) {
                recomputeRetainedScopeIntegrity(db, scopeId, schemaHash, tables)
            }
        }
        SynchroMeta.applyScopeCursorUpdates(db, scopeCursorUpdates, affectedScopes)
    }

    private fun applyAdditiveSchemaMigration(
        db: android.database.sqlite.SQLiteDatabase,
        newTables: List<LocalSchemaTable>,
    ) {
        for (table in newTables) {
            val tableExists = db.rawQuery(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                arrayOf(table.tableName)
            ).use { it.moveToFirst() }

            if (!tableExists) {
                val createSQL = SQLiteSchema.generateCreateTableSQL(table)
                db.execSQL(createSQL)
            } else {
                val existingColumns = mutableSetOf<String>()
                db.rawQuery("PRAGMA table_info(${SQLiteHelpers.quoteIdentifier(table.tableName)})", null).use { cursor ->
                    val nameIdx = cursor.getColumnIndex("name")
                    while (cursor.moveToNext()) existingColumns.add(cursor.getString(nameIdx))
                }

                for (col in table.columns) {
                    if (col.name !in existingColumns) {
                        val sqlType = SQLiteSchema.sqliteType(col.logicalType)
                        val quotedTable = SQLiteHelpers.quoteIdentifier(table.tableName)
                        val quotedCol = SQLiteHelpers.quoteIdentifier(col.name)
                        // SQLite requires constant defaults for NOT NULL columns added with ALTER TABLE.
                        val hasDefault = !col.sqliteDefaultSQL.isNullOrEmpty()
                        val isConstantDefault = hasDefault && !isNonConstantDefault(col.sqliteDefaultSQL!!)
                        val notNullClause = if (!col.nullable && !col.isPrimaryKey && isConstantDefault) " NOT NULL" else ""
                        val defaultClause = if (isConstantDefault) " DEFAULT ${col.sqliteDefaultSQL}" else ""
                        db.execSQL("ALTER TABLE $quotedTable ADD COLUMN $quotedCol $sqlType$notNullClause$defaultClause")
                    }
                }
            }

            for (trigger in SQLiteSchema.generateCDCTriggers(table)) db.execSQL(trigger)
            ensureTargetIndexes(db, table)
        }
    }

    /** Returns true if the SQL default expression is non-constant (not allowed in ALTER TABLE ADD COLUMN). */
    private fun isNonConstantDefault(sql: String): Boolean {
        val upper = sql.uppercase()
        return "CURRENT_TIMESTAMP" in upper ||
               "CURRENT_DATE" in upper ||
               "CURRENT_TIME" in upper ||
               "(" in upper
    }

    private fun validateSchemaCompatibility(
        db: android.database.sqlite.SQLiteDatabase,
        newTables: List<LocalSchemaTable>,
    ) {
        for (table in newTables) {
            val tableName = table.tableName
            val tableExists = db.rawQuery(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                arrayOf(tableName)
            ).use { it.moveToFirst() }
            if (!tableExists) continue

            val existingColumnTypes = mutableMapOf<String, String>()
            val existingPrimaryKey = mutableListOf<Pair<Int, String>>()
            db.rawQuery("PRAGMA table_info(${SQLiteHelpers.quoteIdentifier(tableName)})", null).use { cursor ->
                val nameIdx = cursor.getColumnIndex("name")
                val typeIdx = cursor.getColumnIndex("type")
                val primaryKeyIdx = cursor.getColumnIndex("pk")
                while (cursor.moveToNext()) {
                    val name = cursor.getString(nameIdx)
                    existingColumnTypes[name] = cursor.getString(typeIdx).uppercase()
                    val primaryKeyPosition = cursor.getInt(primaryKeyIdx)
                    if (primaryKeyPosition > 0) {
                        existingPrimaryKey.add(primaryKeyPosition to name)
                    }
                }
            }

            val localPrimaryKey = existingPrimaryKey.sortedBy { it.first }.map { it.second }
            if (localPrimaryKey != table.primaryKey) {
                throw SynchroError.InvalidResponse(
                    "unsupported schema transition changes the primary key for $tableName"
                )
            }

            for (col in table.columns) {
                val localType = existingColumnTypes[col.name] ?: continue
                val serverType = SQLiteSchema.sqliteType(col.logicalType).uppercase()
                if (localType != serverType) {
                    throw SynchroError.InvalidResponse(
                        "unsupported schema transition changes the SQLite type for $tableName.${col.name}"
                    )
                }
            }
        }
    }

    private fun buildMigrationPlan(
        source: SchemaRef,
        target: SchemaRef,
        sourceTables: List<LocalSchemaTable>,
        targetTables: List<LocalSchemaTable>,
        resetMaterialization: Boolean,
    ): MigrationPlan {
        val operations = if (resetMaterialization) {
            listOf(MigrationOperation(MigrationOperationKind.REPLACE_SYNCED_MATERIALIZATION))
        } else {
            val sourceByID = sourceTables.associateBy { it.tableID }
            val targetByID = targetTables.associateBy { it.tableID }
            val planned = mutableListOf<MigrationOperation>()

            sourceTables
                .filter { sourceTable ->
                    targetByID[sourceTable.tableID]?.tableName != sourceTable.tableName
                }
                .sortedWith(compareBy { it.tableID })
                .forEach { sourceTable ->
                    planned += MigrationOperation(MigrationOperationKind.DROP_TABLE, table = sourceTable)
                }

            targetTables.sortedWith(compareBy { it.tableID }).forEach { table ->
                val sourceTable = sourceByID[table.tableID]?.takeIf { it.tableName == table.tableName }
                if (sourceTable == null) {
                    planned += MigrationOperation(MigrationOperationKind.CREATE_TABLE, table = table)
                    table.indexes.sortedWith(compareBy { it.indexID }).forEach { index ->
                        planned += MigrationOperation(MigrationOperationKind.CREATE_INDEX, table = table, index = index)
                    }
                    return@forEach
                }

                val targetColumns = table.columns.associateBy { it.fieldID }
                if (sourceTable.primaryKeyFieldID != table.primaryKeyFieldID ||
                    sourceTable.columns.any { old ->
                        val next = targetColumns[old.fieldID]
                        next == null || next.name != old.name || next.logicalType != old.logicalType ||
                            next.isPrimaryKey != old.isPrimaryKey || (old.nullable && !next.nullable) ||
                            next.sqliteDefaultSQL != old.sqliteDefaultSQL
                    }
                ) {
                    throw SynchroError.InvalidResponse("schema migration has an incompatible retained field")
                }
                if (sourceTable.columns.any { !it.nullable && targetColumns.getValue(it.fieldID).nullable }) {
                    planned += MigrationOperation(MigrationOperationKind.RECREATE_TABLE, table = table)
                    table.indexes.sortedWith(compareBy { it.indexID }).forEach { index ->
                        planned += MigrationOperation(MigrationOperationKind.CREATE_INDEX, table = table, index = index)
                    }
                    return@forEach
                }
                val present = sourceTable.columns.map { it.fieldID }.toSet()
                val additions = table.columns
                    .filter { it.fieldID !in present }
                    .map { it.fieldID }
                    .sortedWith(unsignedUTF8Comparator)
                if (additions.isNotEmpty()) {
                    planned += MigrationOperation(MigrationOperationKind.ADD_COLUMNS, table, additions)
                }

                val ownedIndexes = sourceTable.indexes.associateBy { it.name }
                val targetIndexes = table.indexes.associateBy { it.name }
                sourceTable.indexes.sortedWith(compareBy { it.indexID }).forEach { index ->
                    val targetIndex = targetIndexes[index.name]
                    if (targetIndex == null || targetIndex != index) {
                        planned += MigrationOperation(MigrationOperationKind.DROP_INDEX, table = sourceTable, index = index)
                    }
                }
                table.indexes.sortedWith(compareBy { it.indexID }).forEach { index ->
                    if (ownedIndexes[index.name] != index) {
                        planned += MigrationOperation(MigrationOperationKind.CREATE_INDEX, table = table, index = index)
                    }
                }
            }
            planned
        }
        return MigrationPlan(
            version = MIGRATION_PLAN_VERSION,
            source = source,
            target = target,
            resetMaterialization = resetMaterialization,
            operations = operations,
        )
    }

    private fun applyMigrationPlan(db: SQLiteDatabase, journal: LocalMigrationJournal) {
        if (journal.resetMaterialization) {
            resetSyncedMaterialization(db, journal.targetTables)
            return
        }
        validateSchemaCompatibility(db, journal.targetTables)
        val views = mutableListOf<Pair<String, String>>()
        val viewTriggers = mutableListOf<String>()
        if (journal.plan.operations.any { it.kind == MigrationOperationKind.RECREATE_TABLE }) {
            // SQLite has no view dependency catalog. Save all views to include transitive dependencies without parsing SQL.
            db.rawQuery(
                "SELECT type, name, sql FROM sqlite_master WHERE type = 'view' OR " +
                    "(type = 'trigger' AND tbl_name COLLATE NOCASE IN (SELECT name FROM sqlite_master WHERE type = 'view')) ORDER BY rowid",
                null,
            ).use { cursor ->
                while (cursor.moveToNext()) {
                    if (cursor.isNull(2)) throw SynchroError.InvalidResponse("schema migration view definition is missing")
                    val sql = cursor.getString(2)
                    if (cursor.getString(0) == "view") views += cursor.getString(1) to sql else viewTriggers += sql
                }
            }
            views.forEach { (name, _) -> db.execSQL("DROP VIEW ${SQLiteHelpers.quoteIdentifier(name)}") }
        }
        journal.plan.operations.forEach { operation ->
            when (operation.kind) {
                MigrationOperationKind.CREATE_TABLE -> {
                    val table = operation.table
                        ?: throw SynchroError.InvalidResponse("schema migration create-table operation is invalid")
                    db.execSQL(SQLiteSchema.generateCreateTableSQL(table))
                }
                MigrationOperationKind.DROP_TABLE -> {
                    val table = operation.table
                        ?: throw SynchroError.InvalidResponse("schema migration drop-table operation is invalid")
                    SQLiteSchema.expectedCDCTriggerSQL(table).keys.forEach { trigger ->
                        db.execSQL("DROP TRIGGER IF EXISTS ${SQLiteHelpers.quoteIdentifier(trigger)}")
                    }
                    db.execSQL("DROP TABLE IF EXISTS ${SQLiteHelpers.quoteIdentifier(table.tableName)}")
                }
                MigrationOperationKind.ADD_COLUMNS -> {
                    val table = operation.table
                        ?: throw SynchroError.InvalidResponse("schema migration add-column operation is invalid")
                    val byID = table.columns.associateBy { it.fieldID }
                    operation.columnFieldIDs.forEach { fieldID ->
                        val column = byID[fieldID]
                            ?: throw SynchroError.InvalidResponse("schema migration references an unknown field")
                        addSyncedColumn(db, table, column)
                    }
                }
                MigrationOperationKind.RECREATE_TABLE -> {
                    val target = operation.table!!
                    val source = sourceProjection(db, journal.source).single { it.tableID == target.tableID }
                    val temporaryName = "_synchro_migration_${target.tableID}"
                    if (hasTable(db, temporaryName)) {
                        throw SynchroError.InvalidResponse("schema migration temporary table already exists")
                    }
                    val physical = physicalColumns(db, source.tableName).associateBy { it.name }
                    val oldColumns = source.columns.associateBy { it.fieldID }
                    // Required additions need nullable storage when retained rows have no target value.
                    val storage = target.copy(tableName = temporaryName, columns = target.columns.map { column ->
                        val old = oldColumns[column.fieldID]
                        val nullableStorage = !column.isPrimaryKey && !column.nullable && column.sqliteDefaultSQL == null &&
                            (old == null || physical.getValue(old.name).notNull.not())
                        if (nullableStorage) column.copy(nullable = true) else column
                    })
                    db.execSQL(SQLiteSchema.generateCreateTableSQL(storage))
                    val retained = target.columns.mapNotNull { next -> oldColumns[next.fieldID]?.let { it.name to next.name } }
                    val into = retained.joinToString(", ") { SQLiteHelpers.quoteIdentifier(it.second) }
                    val from = retained.joinToString(", ") { SQLiteHelpers.quoteIdentifier(it.first) }
                    db.execSQL("INSERT INTO ${SQLiteHelpers.quoteIdentifier(temporaryName)} ($into) SELECT $from FROM ${SQLiteHelpers.quoteIdentifier(source.tableName)}")
                    db.execSQL("DROP TABLE ${SQLiteHelpers.quoteIdentifier(source.tableName)}")
                    db.execSQL("ALTER TABLE ${SQLiteHelpers.quoteIdentifier(temporaryName)} RENAME TO ${SQLiteHelpers.quoteIdentifier(target.tableName)}")
                }
                MigrationOperationKind.DROP_INDEX -> {
                    val index = operation.index
                        ?: throw SynchroError.InvalidResponse("schema migration drop-index operation is invalid")
                    db.execSQL("DROP INDEX IF EXISTS ${SQLiteHelpers.quoteIdentifier(index.name)}")
                }
                MigrationOperationKind.CREATE_INDEX -> {
                    val table = operation.table
                        ?: throw SynchroError.InvalidResponse("schema migration create-index operation is invalid")
                    val index = operation.index
                        ?: throw SynchroError.InvalidResponse("schema migration create-index operation is invalid")
                    db.execSQL(SQLiteSchema.generateCreateIndexSQL(table, index, ifNotExists = false))
                }
                MigrationOperationKind.REPLACE_SYNCED_MATERIALIZATION -> {
                    throw SynchroError.InvalidResponse("schema migration reset operation is inconsistent")
                }
            }
        }
        journal.targetTables.forEach { table ->
            SQLiteSchema.generateCDCTriggers(table).forEach(db::execSQL)
        }
        views.forEach { (_, sql) -> db.execSQL(sql) }
        viewTriggers.forEach(db::execSQL)
        views.forEach { (name, _) ->
            db.rawQuery("SELECT * FROM ${SQLiteHelpers.quoteIdentifier(name)} LIMIT 0", null).use { it.columnCount }
        }
    }

    /** Explicit reset replaces only materialized synced tables and state. */
    private fun resetSyncedMaterialization(db: SQLiteDatabase, targetTables: List<LocalSchemaTable>) {
        val currentTables = loadStoredLocalSchemaInTransaction(db).orEmpty()
        val targetsByID = targetTables.associateBy { it.tableID }
        val protectedRows = currentTables.mapNotNull { table ->
            targetsByID[table.tableID]?.let { target -> table.tableID to loadProtectedRows(db, table, target) }
        }.toMap()
        currentTables.reversed().forEach { table ->
            val quotedTable = SQLiteHelpers.quoteIdentifier(table.tableName)
            listOf(
                "_synchro_cdc_insert_${table.tableName}",
                "_synchro_cdc_update_${table.tableName}",
                "_synchro_cdc_delete_${table.tableName}",
                "_synchro_cdc_pk_guard_${table.tableName}",
            ).forEach { trigger ->
                db.execSQL("DROP TRIGGER IF EXISTS ${SQLiteHelpers.quoteIdentifier(trigger)}")
            }
            db.execSQL("DROP TABLE IF EXISTS $quotedTable")
        }
        db.execSQL("DELETE FROM _synchro_row_versions")
        db.execSQL("DELETE FROM _synchro_rebuild_page_receipts")
        db.execSQL("DELETE FROM _synchro_rebuild_attempts")
        SynchroMeta.invalidateAllScopes(db)
        targetTables.forEach { table ->
            db.execSQL(SQLiteSchema.generateCreateTableSQL(table))
            // The restore runs before capture triggers exist, so it creates no intent.
            protectedRows[table.tableID]?.let { restoreProtectedRows(db, it, table) }
            SQLiteSchema.generateCDCTriggers(table).forEach(db::execSQL)
            table.indexes.forEach { index ->
                db.execSQL(SQLiteSchema.generateCreateIndexSQL(table, index))
            }
        }
    }

    /**
     * The target values of the application rows that hold unresolved local
     * intent. A reset rebuild must not overwrite or remove those rows (spec
     * schema evolution, client migration step 8), so the reset keeps each field
     * that the target declares with the same field ID and type. The restore
     * skips a row that the target shape cannot hold, and the rebuild then
     * installs the server row for it.
     */
    private class ProtectedRows(val columns: List<String>, val rows: List<Array<Any?>>)

    private fun loadProtectedRows(db: SQLiteDatabase, source: LocalSchemaTable, target: LocalSchemaTable): ProtectedRows {
        val sourceColumns = source.columns.associateBy { it.fieldID }
        val kept = target.columns.mapNotNull { column ->
            sourceColumns[column.fieldID]?.takeIf { it.logicalType == column.logicalType }?.let { it.name to column.name }
        }
        val primaryKey = sourceColumns[source.primaryKeyFieldID]
        if (source.primaryKeyFieldID != target.primaryKeyFieldID || primaryKey == null ||
            kept.none { it.first == primaryKey.name }
        ) {
            return ProtectedRows(emptyList(), emptyList())
        }
        val selected = kept.joinToString(", ") { SQLiteHelpers.quoteIdentifier(it.first) }
        val rows = mutableListOf<Array<Any?>>()
        db.rawQuery(
            "SELECT $selected FROM ${SQLiteHelpers.quoteIdentifier(source.tableName)} " +
                "WHERE ${SQLiteHelpers.quoteIdentifier(primaryKey.name)} IN ($PROTECTED_RECORD_IDS_SQL)",
            arrayOf(source.tableName),
        ).use { cursor ->
            while (cursor.moveToNext()) {
                rows += Array(kept.size) { index ->
                    when (cursor.getType(index)) {
                        android.database.Cursor.FIELD_TYPE_NULL -> null
                        android.database.Cursor.FIELD_TYPE_INTEGER -> cursor.getLong(index)
                        android.database.Cursor.FIELD_TYPE_FLOAT -> cursor.getDouble(index)
                        android.database.Cursor.FIELD_TYPE_BLOB -> cursor.getBlob(index)
                        else -> cursor.getString(index)
                    }
                }
            }
        }
        return ProtectedRows(kept.map { it.second }, rows)
    }

    private fun restoreProtectedRows(db: SQLiteDatabase, protectedRows: ProtectedRows, target: LocalSchemaTable) {
        if (protectedRows.rows.isEmpty()) return
        val columns = protectedRows.columns.joinToString(", ") { SQLiteHelpers.quoteIdentifier(it) }
        val placeholders = protectedRows.columns.joinToString(", ") { "?" }
        // OR IGNORE skips only a row that violates a target NOT NULL, CHECK,
        // UNIQUE, or PRIMARY KEY constraint. Other errors still fail the reset.
        val sql = "INSERT OR IGNORE INTO ${SQLiteHelpers.quoteIdentifier(target.tableName)} ($columns) VALUES ($placeholders)"
        protectedRows.rows.forEach { values -> db.execSQL(sql, values) }
    }

    private fun addSyncedColumn(db: SQLiteDatabase, table: LocalSchemaTable, column: LocalSchemaColumn) {
        val sqlType = SQLiteSchema.sqliteType(column.logicalType)
        val quotedTable = SQLiteHelpers.quoteIdentifier(table.tableName)
        val quotedColumn = SQLiteHelpers.quoteIdentifier(column.name)
        val default = column.sqliteDefaultSQL
        val constantDefault = default != null && !isNonConstantDefault(default)
        val nonNull = !column.nullable && !column.isPrimaryKey && constantDefault
        val sql = buildString {
            append("ALTER TABLE $quotedTable ADD COLUMN $quotedColumn $sqlType")
            if (nonNull) append(" NOT NULL")
            if (constantDefault) append(" DEFAULT $default")
        }
        db.execSQL(sql)
    }

    private fun currentSchemaRef(db: SQLiteDatabase): SchemaRef = SchemaRef(
        SynchroMeta.getInt64(db, MetaKey.SCHEMA_VERSION),
        SynchroMeta.get(db, MetaKey.SCHEMA_HASH).orEmpty(),
    )

    private fun loadStoredLocalSchemaInTransaction(db: SQLiteDatabase): List<LocalSchemaTable>? {
        val encoded = SynchroMeta.get(db, MetaKey.LOCAL_SCHEMA) ?: return null
        return try {
            json.decodeFromString(encoded)
        } catch (_: Exception) {
            throw SynchroError.InvalidResponse("stored local schema metadata is invalid")
        }
    }

    private fun sourceProjection(db: SQLiteDatabase, reference: SchemaRef): List<LocalSchemaTable> {
        val tables = if (reference == SchemaRef(0, "")) emptyList() else {
            val encoded = db.rawQuery(
                "SELECT manifest_json FROM _synchro_schema_archives WHERE schema_version = ? AND schema_hash = ?",
                arrayOf(reference.version.toString(), reference.hash),
            ).use { cursor ->
                if (!cursor.moveToFirst()) throw SynchroError.InvalidResponse("schema migration source archive is missing")
                cursor.getString(0)
            }
            try {
                json.decodeFromString<List<LocalSchemaTable>>(encoded)
            } catch (_: Exception) {
                throw SynchroError.InvalidResponse("schema migration source archive is invalid")
            }
        }
        if (tables.map { it.tableID }.toSet().size != tables.size || tables.any { table ->
                table.columns.map { it.fieldID }.toSet().size != table.columns.size ||
                    table.indexes.map { it.indexID }.toSet().size != table.indexes.size
            }
        ) {
            throw SynchroError.InvalidResponse("schema migration source projection has duplicate identifiers")
        }
        if (reference == currentSchemaRef(db) && loadStoredLocalSchemaInTransaction(db).orEmpty() != tables) {
            throw SynchroError.InvalidResponse("schema migration source projection does not match active metadata")
        }
        return tables
    }

    private fun validateMigrationSource(db: SQLiteDatabase, sourceTables: List<LocalSchemaTable>, targetTables: List<LocalSchemaTable>) {
        validateTargetPhysicalSchema(db, sourceTables)
        val ownedNames = sourceTables.map { it.tableName }.toSet()
        if (targetTables.any { hasTable(db, it.tableName) && it.tableName !in ownedNames }) {
            throw SynchroError.InvalidResponse("schema migration target table collides with an unowned table")
        }
    }

    private fun hasTable(db: SQLiteDatabase, tableName: String): Boolean = db.rawQuery(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        arrayOf(tableName),
    ).use { it.moveToFirst() }

    private data class PhysicalColumn(
        val name: String,
        val type: String,
        val notNull: Boolean,
        val primaryKeyPosition: Int,
        val defaultSQL: String?,
    )

    private data class PhysicalIndex(
        val name: String,
        val unique: Boolean,
        val columns: List<String>,
        val partial: Boolean,
    )

    private fun physicalColumns(db: SQLiteDatabase, tableName: String): List<PhysicalColumn> {
        val columns = mutableListOf<PhysicalColumn>()
        val virtual = db.rawQuery("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?", arrayOf(tableName)).use { cursor ->
            cursor.moveToFirst() && cursor.getString(0).trimStart().startsWith("CREATE VIRTUAL TABLE", ignoreCase = true)
        }
        if (virtual) throw SynchroError.InvalidResponse("synced table has an unsupported virtual representation")
        val version = db.rawQuery("SELECT sqlite_version()", null).use { cursor ->
            if (!cursor.moveToFirst()) throw SynchroError.InvalidResponse("SQLite version is unavailable")
            cursor.getString(0).split('.').map { it.toInt() }
        }
        // SQLite before 3.26 has no table_xinfo and cannot store generated columns.
        val extended = version[0] > 3 || (version[0] == 3 && version[1] >= 26)
        val pragma = if (extended) "table_xinfo" else "table_info"
        db.rawQuery("PRAGMA $pragma(${SQLiteHelpers.quoteIdentifier(tableName)})", null).use { cursor ->
            val nameIndex = cursor.getColumnIndexOrThrow("name")
            val typeIndex = cursor.getColumnIndexOrThrow("type")
            val notNullIndex = cursor.getColumnIndexOrThrow("notnull")
            val defaultIndex = cursor.getColumnIndexOrThrow("dflt_value")
            val primaryKeyIndex = cursor.getColumnIndexOrThrow("pk")
            val hiddenIndex = if (extended) cursor.getColumnIndexOrThrow("hidden") else -1
            while (cursor.moveToNext()) {
                if (hiddenIndex >= 0 && cursor.getInt(hiddenIndex) != 0) {
                    throw SynchroError.InvalidResponse("synced table has an unsupported hidden or generated column")
                }
                columns += PhysicalColumn(
                    name = cursor.getString(nameIndex),
                    type = cursor.getString(typeIndex).uppercase(),
                    notNull = cursor.getInt(notNullIndex) == 1,
                    primaryKeyPosition = cursor.getInt(primaryKeyIndex),
                    defaultSQL = if (cursor.isNull(defaultIndex)) null else cursor.getString(defaultIndex),
                )
            }
        }
        return columns
    }

    private fun physicalClientIndexes(db: SQLiteDatabase, tableName: String): List<PhysicalIndex> {
        data class ListedIndex(val name: String, val unique: Boolean, val origin: String, val partial: Boolean)
        val listed = mutableListOf<ListedIndex>()
        db.rawQuery("PRAGMA index_list(${SQLiteHelpers.quoteIdentifier(tableName)})", null).use { cursor ->
            val nameIndex = cursor.getColumnIndexOrThrow("name")
            val uniqueIndex = cursor.getColumnIndexOrThrow("unique")
            val originIndex = cursor.getColumnIndexOrThrow("origin")
            val partialIndex = cursor.getColumnIndexOrThrow("partial")
            while (cursor.moveToNext()) {
                listed += ListedIndex(
                    name = cursor.getString(nameIndex),
                    unique = cursor.getInt(uniqueIndex) == 1,
                    origin = cursor.getString(originIndex),
                    partial = cursor.getInt(partialIndex) == 1,
                )
            }
        }
        return listed.filter { it.origin == "c" }.map { index ->
            val columns = mutableListOf<Pair<Int, String>>()
            db.rawQuery("PRAGMA index_xinfo(${SQLiteHelpers.quoteIdentifier(index.name)})", null).use { cursor ->
                val sequenceIndex = cursor.getColumnIndexOrThrow("seqno")
                val columnIndex = cursor.getColumnIndexOrThrow("name")
                val keyIndex = cursor.getColumnIndexOrThrow("key")
                while (cursor.moveToNext()) {
                    if (cursor.getInt(keyIndex) == 1) {
                        if (cursor.isNull(columnIndex)) {
                            throw SynchroError.InvalidResponse("schema migration index has an expression")
                        }
                        columns += cursor.getInt(sequenceIndex) to cursor.getString(columnIndex)
                    }
                }
            }
            PhysicalIndex(
                name = index.name,
                unique = index.unique,
                columns = columns.sortedBy { it.first }.map { it.second },
                partial = index.partial,
            )
        }
    }

    private fun ensureTargetIndexes(db: SQLiteDatabase, table: LocalSchemaTable) {
        val existing = physicalClientIndexes(db, table.tableName).associateBy { it.name }
        table.indexes.forEach { index ->
            val expected = PhysicalIndex(index.name, index.unique, index.columnNames, partial = false)
            val current = existing[index.name]
            when {
                current == null -> db.execSQL(SQLiteSchema.generateCreateIndexSQL(table, index))
                current != expected -> throw SynchroError.InvalidResponse("synced table index does not match its manifest")
            }
        }
    }

    private fun validateTargetPhysicalSchema(
        db: SQLiteDatabase,
        tables: List<LocalSchemaTable>,
    ) {
        tables.forEach { table ->
            if (!hasTable(db, table.tableName)) {
                throw SynchroError.InvalidResponse("schema migration target table is missing")
            }
            val expectedColumns = table.columns.map { column ->
                PhysicalColumn(
                    name = column.name,
                    type = SQLiteSchema.sqliteType(column.logicalType).uppercase(),
                    notNull = !column.nullable && !column.isPrimaryKey,
                    primaryKeyPosition = if (column.isPrimaryKey) 1 else 0,
                    defaultSQL = column.sqliteDefaultSQL,
                )
            }
            val actualColumns = physicalColumns(db, table.tableName)
            val actualByName = actualColumns.associateBy { it.name }
            if (actualColumns.size != expectedColumns.size ||
                expectedColumns.any { expected ->
                    val actual = actualByName[expected.name]
                    actual != expected && !(expected.notNull && expected.primaryKeyPosition == 0 &&
                        expected.defaultSQL == null && actual == expected.copy(notNull = false))
                }
            ) {
                throw SynchroError.InvalidResponse("schema migration target columns do not match the manifest")
            }
            val expectedIndexes = table.indexes.map {
                PhysicalIndex(it.name, it.unique, it.columnNames, partial = false)
            }.sortedBy { it.name }
            val actualIndexes = physicalClientIndexes(db, table.tableName).sortedBy { it.name }
            if (actualIndexes != expectedIndexes) {
                throw SynchroError.InvalidResponse("schema migration target indexes do not match the manifest")
            }
        }
        val expectedTriggers = tables.flatMap { table ->
            SQLiteSchema.expectedCDCTriggerSQL(table).map { (name, sql) ->
                name to (table.tableName to SQLiteSchema.canonicalDDL(sql))
            }
        }.toMap()
        val actualTriggers = mutableMapOf<String, Pair<String, String?>>()
        db.rawQuery(
            "SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger' AND " +
                "tbl_name COLLATE NOCASE NOT IN (SELECT name FROM sqlite_master WHERE type = 'view')",
            null,
        ).use { cursor ->
            while (cursor.moveToNext()) {
                actualTriggers[cursor.getString(0)] = cursor.getString(1) to
                    if (cursor.isNull(2)) null else SQLiteSchema.canonicalDDL(cursor.getString(2))
            }
        }
        if (actualTriggers.keys != expectedTriggers.keys || expectedTriggers.any { (name, expected) ->
                val actual = actualTriggers[name]
                actual == null || !actual.first.equals(expected.first, ignoreCase = true) || actual.second != expected.second
            }
        ) {
            throw SynchroError.InvalidResponse("schema migration target triggers do not match the manifest")
        }
    }

    private fun persistMigrationJournal(
        db: SQLiteDatabase,
        journal: LocalMigrationJournal,
        planJSON: String,
    ) {
        db.execSQL(
            """
            INSERT INTO _synchro_migration_journal (
                singleton, journal_version, source_schema_version, source_schema_hash,
                target_schema_version, target_schema_hash, action, affected_scopes_json,
                scope_cursor_updates_json, target_manifest_json, target_tables_json,
                migration_plan_version, migration_plan_json, migration_plan_hash,
                reset_materialization, phase, created_at, updated_at
            ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z',
                substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z'
            )
            """.trimIndent(),
            arrayOf(
                journal.journalVersion,
                journal.source.version,
                journal.source.hash,
                journal.target.version,
                journal.target.hash,
                journal.action.name.lowercase(),
                json.encodeToString(journal.affectedScopes),
                json.encodeToString(journal.scopeCursorUpdates),
                json.encodeToString(journal.targetManifest),
                json.encodeToString(journal.targetTables),
                journal.plan.version,
                planJSON,
                journal.planHash,
                if (journal.resetMaterialization) 1 else 0,
                journal.phase.name.lowercase(),
            ),
        )
    }

    private fun loadMigrationJournal(db: SQLiteDatabase): LocalMigrationJournal? {
        db.rawQuery(
            """
            SELECT journal_version, source_schema_version, source_schema_hash,
                   target_schema_version, target_schema_hash, action, affected_scopes_json,
                   scope_cursor_updates_json, target_manifest_json, target_tables_json,
                   migration_plan_version, migration_plan_json, migration_plan_hash,
                   reset_materialization, phase
            FROM _synchro_migration_journal WHERE singleton = 1
            """.trimIndent(),
            null,
        ).use { cursor ->
            if (!cursor.moveToFirst()) return null
            val action = when (cursor.getString(5)) {
                "replace" -> SchemaAction.REPLACE
                "rebuild_local" -> SchemaAction.REBUILD_LOCAL
                else -> throw SynchroError.InvalidResponse("schema migration journal action is invalid")
            }
            val phase = when (cursor.getString(14)) {
                "prepared" -> MigrationPhase.PREPARED
                "ddl_applied" -> MigrationPhase.DDL_APPLIED
                "awaiting_rebuild" -> MigrationPhase.AWAITING_REBUILD
                else -> throw SynchroError.InvalidResponse("schema migration journal phase is invalid")
            }
            val resetValue = cursor.getInt(13)
            if (resetValue !in 0..1) throw SynchroError.InvalidResponse("schema migration journal reset state is invalid")
            return try {
                LocalMigrationJournal(
                    journalVersion = cursor.getInt(0),
                    source = SchemaRef(cursor.getLong(1), cursor.getString(2)),
                    target = SchemaRef(cursor.getLong(3), cursor.getString(4)),
                    action = action,
                    affectedScopes = json.decodeFromString(cursor.getString(6)),
                    scopeCursorUpdates = json.decodeFromString(cursor.getString(7)),
                    targetManifest = json.decodeFromString(cursor.getString(8)),
                    targetTables = json.decodeFromString(cursor.getString(9)),
                    plan = json.decodeFromString(cursor.getString(11)),
                    planHash = cursor.getString(12),
                    resetMaterialization = resetValue == 1,
                    phase = phase,
                )
            } catch (error: SynchroError.InvalidResponse) {
                throw error
            } catch (_: Exception) {
                throw SynchroError.InvalidResponse("schema migration journal is invalid")
            }
        }
    }

    private fun validateJournal(journal: LocalMigrationJournal) {
        if (journal.journalVersion != MIGRATION_JOURNAL_VERSION ||
            journal.plan.version != MIGRATION_PLAN_VERSION ||
            journal.action !in setOf(SchemaAction.REPLACE, SchemaAction.REBUILD_LOCAL) ||
            journal.target.version <= 0L || journal.target.hash.isEmpty() ||
            journal.target != SchemaRef(journal.targetManifest.schemaVersion, journal.targetManifest.schemaHash) ||
            journal.plan.source != journal.source || journal.plan.target != journal.target ||
            journal.plan.resetMaterialization != journal.resetMaterialization
        ) {
            throw SynchroError.InvalidResponse("schema migration journal is inconsistent")
        }
        try {
            journal.targetManifest.validate()
        } catch (_: ContractException) {
            throw SynchroError.InvalidResponse("schema migration journal manifest is invalid")
        }
        if (Integrity.schemaManifestHash(journal.targetManifest) != journal.target.hash) {
            throw SynchroError.InvalidResponse("schema migration journal manifest hash is invalid")
        }
        val expectedTables = journal.targetManifest.localTables().associateBy { it.tableID }
        if (journal.targetTables.size != expectedTables.size || journal.targetTables.associateBy { it.tableID } != expectedTables) {
            throw SynchroError.InvalidResponse("schema migration journal tables are invalid")
        }
        if (journal.affectedScopes.any { it.isEmpty() } ||
            journal.affectedScopes != journal.affectedScopes.distinct().sortedWith(unsignedUTF8Comparator) ||
            journal.scopeCursorUpdates.keys.any { it.isEmpty() } ||
            journal.scopeCursorUpdates.values.any { it != null && it.isEmpty() } ||
            (journal.action == SchemaAction.REBUILD_LOCAL) != journal.affectedScopes.isNotEmpty() ||
            journal.affectedScopes.any { it !in journal.scopeCursorUpdates || journal.scopeCursorUpdates[it] != null } ||
            journal.scopeCursorUpdates.any { (scope, cursor) -> cursor == null && scope !in journal.affectedScopes } ||
            (journal.resetMaterialization && journal.scopeCursorUpdates.keys != journal.affectedScopes.toSet()) ||
            (journal.phase == MigrationPhase.DDL_APPLIED && journal.affectedScopes.isNotEmpty()) ||
            (journal.phase == MigrationPhase.AWAITING_REBUILD && journal.affectedScopes.isEmpty())
        ) {
            throw SynchroError.InvalidResponse("schema migration journal scope state is invalid")
        }
        val planJSON = json.encodeToString(journal.plan)
        if (migrationPlanHash(planJSON) != journal.planHash) {
            throw SynchroError.InvalidResponse("schema migration journal plan is invalid")
        }
    }

    private fun updateMigrationPhase(db: SQLiteDatabase, phase: MigrationPhase) {
        db.execSQL(
            """
            UPDATE _synchro_migration_journal
            SET phase = ?, updated_at = substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z'
            WHERE singleton = 1
            """.trimIndent(),
            arrayOf(phase.name.lowercase()),
        )
    }

    private fun persistLocalSchemaTables(db: android.database.sqlite.SQLiteDatabase, tables: List<LocalSchemaTable>) {
        SynchroMeta.set(db, MetaKey.LOCAL_SCHEMA, json.encodeToString(tables))
    }

    /**
     * Capture triggers require an immutable archive when they execute.
     * Store the archive only after all schema DDL succeeds.
     */
    private fun archiveSchemaTables(
        db: android.database.sqlite.SQLiteDatabase,
        schemaVersion: Long,
        schemaHash: String,
        tables: List<LocalSchemaTable>,
    ) {
        if (schemaVersion <= 0L || schemaHash.isEmpty()) {
            throw SynchroError.InvalidResponse("synced-table capture requires a verified schema reference")
        }
        db.execSQL(
            """
            INSERT OR IGNORE INTO _synchro_schema_archives (schema_version, schema_hash, manifest_json, created_at)
            VALUES (?, ?, ?, substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z')
            """.trimIndent(),
            arrayOf(schemaVersion, schemaHash, json.encodeToString(tables)),
        )
    }

    private fun recomputeRetainedScopeIntegrity(
        db: android.database.sqlite.SQLiteDatabase,
        scopeId: String,
        schemaHash: String,
        tables: List<LocalSchemaTable>,
    ) {
        if (SynchroMeta.getScope(db, scopeId) == null) {
            throw SynchroError.InvalidResponse("scope cursor update targets an unknown scope $scopeId")
        }
        val tablesByName = tables.associateBy { it.tableName }
        val entries = SynchroMeta.getScopeRows(db, scopeId).map { (tableName, recordId) ->
            val table = tablesByName[tableName]
                ?: throw SynchroError.InvalidResponse("scope references unknown table $tableName")
            val row = loadWireRow(db, table, recordId)
            val primaryKey = row[table.primaryKeyFieldID]
                ?: throw SynchroError.InvalidResponse("scope row lacks its primary key field")
            val serverVersion = SynchroMeta.getRowVersion(db, table.tableName, recordId)
                ?: throw SynchroError.InvalidResponse("scope row has no server version")
            val computed = Integrity.rowDigest(
                schemaHash,
                table,
                JsonObject(mapOf(table.primaryKeyFieldID to primaryKey)),
                row,
                serverVersion,
            )
            SynchroMeta.updateScopeRowChecksum(
                db,
                scopeId,
                tableName,
                recordId,
                computed.checksum.digest,
            )
            computed.identity to computed.checksum
        }
        val localChecksum = Integrity.scopeDigest(schemaHash, scopeId, entries)
        SynchroMeta.setScopeLocalChecksum(
            db,
            scopeId,
            json.encodeToString(ChecksumObject.serializer(), localChecksum),
        )
    }

    private fun loadWireRow(
        db: android.database.sqlite.SQLiteDatabase,
        table: LocalSchemaTable,
        recordId: String,
    ): JsonObject {
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
        if (cursor.isNull(index)) return JsonNull
        return when (column.logicalType) {
            "boolean" -> JsonPrimitive(cursor.getLong(index) != 0L)
            "int" -> JsonPrimitive(cursor.getInt(index))
            "int64" -> JsonPrimitive(cursor.getLong(index).toString())
            "float" -> JsonPrimitive(cursor.getDouble(index))
            "bytes" -> JsonPrimitive(
                android.util.Base64.encodeToString(
                    cursor.getBlob(index),
                    android.util.Base64.URL_SAFE or android.util.Base64.NO_WRAP or android.util.Base64.NO_PADDING,
                )
            )
            else -> JsonPrimitive(cursor.getString(index))
        }
    }

    private companion object {
        const val MIGRATION_JOURNAL_VERSION = 2
        const val MIGRATION_PLAN_VERSION = 2

        val unsignedUTF8Comparator = Comparator<String> { left, right ->
            val leftBytes = left.toByteArray(Charsets.UTF_8)
            val rightBytes = right.toByteArray(Charsets.UTF_8)
            val length = minOf(leftBytes.size, rightBytes.size)
            for (index in 0 until length) {
                val comparison = (leftBytes[index].toInt() and 0xff).compareTo(rightBytes[index].toInt() and 0xff)
                if (comparison != 0) return@Comparator comparison
            }
            leftBytes.size.compareTo(rightBytes.size)
        }
    }
}
