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
                if (existing.target != target || existing.resetMaterialization != resetMaterialization ||
                    existing.action != response.schema.action || existing.targetManifest != manifest
                ) {
                    throw SynchroError.InvalidResponse("a different schema migration is already pending")
                }
                return@writeTransaction existing
            }
            val plan = buildMigrationPlan(db, source, target, targetTables, resetMaterialization)
            val planJSON = json.encodeToString(plan)
            val journal = LocalMigrationJournal(
                journalVersion = MIGRATION_JOURNAL_VERSION,
                source = source,
                target = target,
                action = response.schema.action,
                affectedScopes = response.affectedScopes.orEmpty().sortedWith(unsignedUTF8Comparator),
                scopeCursorUpdates = response.scopeCursorUpdates.toSortedMap(),
                targetManifest = manifest,
                targetTables = targetTables.sortedWith(compareBy { it.tableID }),
                plan = plan,
                planHash = migrationPlanHash(planJSON),
                resetMaterialization = resetMaterialization,
                phase = MigrationPhase.PREPARED,
            )
            persistMigrationJournal(db, journal, planJSON)
            journal
        }
    }

    /** Applies only the journal that was committed before local DDL. */
    internal fun applyPreparedMigrationInTransaction(db: SQLiteDatabase): LocalMigrationJournal? {
        val journal = loadMigrationJournal(db) ?: return null
        validateJournal(journal)
        val current = currentSchemaRef(db)
        when {
            current == journal.source && journal.phase == MigrationPhase.PREPARED -> {
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
                    journal.scopeCursorUpdates,
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
                validateTargetPhysicalSchema(db, journal.targetTables)
                return journal
            }
            else -> throw SynchroError.InvalidResponse("schema migration journal does not match local schema state")
        }
    }

    /** Recovery runs before any connect, push, pull, or rebuild request. */
    internal fun recoverPendingMigration() {
        database.writeTransaction { db ->
            val journal = loadMigrationJournal(db) ?: return@writeTransaction
            validateJournal(journal)
            applyPreparedMigrationInTransaction(db)
        }
    }

    /** Clears a journal only after every required scope reached verified finality. */
    internal fun completeMigrationIfReady() {
        database.writeTransaction { db ->
            val journal = loadMigrationJournal(db) ?: return@writeTransaction
            validateJournal(journal)
            if (currentSchemaRef(db) != journal.target) {
                throw SynchroError.InvalidResponse("schema migration journal lost its target schema")
            }
            validateTargetPhysicalSchema(db, journal.targetTables)
            val complete = when (journal.phase) {
                MigrationPhase.DDL_APPLIED -> journal.affectedScopes.isEmpty()
                MigrationPhase.AWAITING_REBUILD -> journal.affectedScopes.all { scopeID ->
                    val scope = SynchroMeta.getScope(db, scopeID)
                    scope == null || (
                        scope.cursor != null &&
                            scope.checksum != null &&
                            SynchroMeta.getRebuildAttempt(db, scopeID) == null
                        )
                }
                MigrationPhase.PREPARED -> false
            }
            if (complete) {
                db.execSQL("DELETE FROM _synchro_migration_journal WHERE singleton = 1")
            }
        }
    }

    suspend fun ensureSchema(httpClient: HttpClient): SchemaResponse {
        val (localVersion, localHash) = database.readTransaction { db ->
            val version = SynchroMeta.getInt64(db, MetaKey.SCHEMA_VERSION)
            val hash = SynchroMeta.get(db, MetaKey.SCHEMA_HASH) ?: ""
            Pair(version, hash)
        }

        val schema = httpClient.fetchSchema()

        if (localVersion == schema.schemaVersion && localHash == schema.schemaHash) {
            return schema
        }

        migrateSchema(schema)

        return schema
    }

    fun loadStoredLocalSchema(): List<LocalSchemaTable>? {
        return database.readTransaction { db ->
            val encoded = SynchroMeta.get(db, MetaKey.LOCAL_SCHEMA) ?: return@readTransaction null
            json.decodeFromString<List<LocalSchemaTable>>(encoded)
        }
    }

    fun reconcileLocalSchema(
        schemaVersion: Long,
        schemaHash: String,
        tables: List<LocalSchemaTable>,
        scopeCursorUpdates: Map<String, String?> = emptyMap(),
        affectedScopes: List<String> = emptyList(),
    ) {
        database.writeTransaction { db ->
            reconcileLocalSchemaInTransaction(
                db,
                schemaVersion,
                schemaHash,
                tables,
                scopeCursorUpdates,
                affectedScopes,
            )
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

    fun createSyncedTables(schema: SchemaResponse) {
        database.writeTransaction { db ->
            createSyncedTablesInTransaction(db, schema)
        }
    }

    internal fun createSyncedTablesInTransaction(db: android.database.sqlite.SQLiteDatabase, schema: SchemaResponse) {
        val tables = schema.localTables()
        validateSchemaCompatibility(db, tables)
        createSyncedTablesInTransaction(db, tables)
        archiveSchemaTables(db, schema.schemaVersion, schema.schemaHash, tables)
        SynchroMeta.setInt64(db, MetaKey.SCHEMA_VERSION, schema.schemaVersion)
        SynchroMeta.set(db, MetaKey.SCHEMA_HASH, schema.schemaHash)
        persistLocalSchemaTables(db, tables)
    }

    internal fun createSyncedTablesInTransaction(db: android.database.sqlite.SQLiteDatabase, tables: List<LocalSchemaTable>) {
        for (table in tables) {
            val createSQL = SQLiteSchema.generateCreateTableSQL(table)
            db.execSQL(createSQL)

            val triggers = SQLiteSchema.generateCDCTriggers(table)
            for (trigger in triggers) {
                db.execSQL(trigger)
            }
            table.indexes.forEach { index ->
                db.execSQL(SQLiteSchema.generateCreateIndexSQL(table, index))
            }
        }
    }

    fun migrateSchema(newSchema: SchemaResponse) {
        val tables = newSchema.localTables()
        database.writeTransaction { db ->
            migrateLocalSchemaInTransaction(db, tables)
            archiveSchemaTables(db, newSchema.schemaVersion, newSchema.schemaHash, tables)
            SynchroMeta.setInt64(db, MetaKey.SCHEMA_VERSION, newSchema.schemaVersion)
            SynchroMeta.set(db, MetaKey.SCHEMA_HASH, newSchema.schemaHash)
            persistLocalSchemaTables(db, tables)
        }
    }

    fun migrateLocalSchema(newTables: List<LocalSchemaTable>) {
        database.writeTransaction { db ->
            ensureCurrentSchemaIsArchived(db)
            migrateLocalSchemaInTransaction(db, newTables)
            persistLocalSchemaTables(db, newTables)
        }
    }

    private fun migrateLocalSchemaInTransaction(
        db: android.database.sqlite.SQLiteDatabase,
        newTables: List<LocalSchemaTable>,
    ) {
        validateSchemaCompatibility(db, newTables)
        applyAdditiveSchemaMigration(db, newTables)
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
        db: SQLiteDatabase,
        source: SchemaRef,
        target: SchemaRef,
        targetTables: List<LocalSchemaTable>,
        resetMaterialization: Boolean,
    ): MigrationPlan {
        val operations = if (resetMaterialization) {
            listOf(MigrationOperation(MigrationOperationKind.REPLACE_SYNCED_MATERIALIZATION))
        } else {
            val sourceTables = loadStoredLocalSchemaInTransaction(db).orEmpty()
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
                val exists = hasTable(db, table.tableName)
                if (exists && sourceTable == null) {
                    throw SynchroError.InvalidResponse("schema migration target table collides with an unowned table")
                }
                if (!exists) {
                    planned += MigrationOperation(MigrationOperationKind.CREATE_TABLE, table = table)
                    table.indexes.sortedWith(compareBy { it.indexID }).forEach { index ->
                        planned += MigrationOperation(MigrationOperationKind.CREATE_INDEX, table = table, index = index)
                    }
                    return@forEach
                }

                val present = tableColumnNames(db, table.tableName)
                val retired = present - table.columns.map { it.name }.toSet()
                if (retired.isNotEmpty()) {
                    throw SynchroError.InvalidResponse("schema migration removes synced columns without a reset")
                }
                val additions = table.columns
                    .filter { it.name !in present }
                    .map { it.fieldID }
                    .sortedWith(unsignedUTF8Comparator)
                if (additions.isNotEmpty()) {
                    planned += MigrationOperation(MigrationOperationKind.ADD_COLUMNS, table, additions)
                }

                val existingIndexes = physicalClientIndexes(db, table.tableName).associateBy { it.name }
                val ownedIndexes = sourceTable!!.indexes.associateBy { it.name }
                if (existingIndexes.keys != ownedIndexes.keys) {
                    throw SynchroError.InvalidResponse("schema migration source indexes are not client-owned")
                }
                val targetIndexes = table.indexes.associateBy { it.name }
                sourceTable.indexes.sortedWith(compareBy { it.indexID }).forEach { index ->
                    val targetIndex = targetIndexes[index.name]
                    val current = existingIndexes.getValue(index.name)
                    val sourcePhysical = PhysicalIndex(index.name, index.unique, index.columnNames, partial = false)
                    if (current != sourcePhysical) {
                        throw SynchroError.InvalidResponse("schema migration source index is invalid")
                    }
                    if (targetIndex == null || targetIndex != index) {
                        planned += MigrationOperation(MigrationOperationKind.DROP_INDEX, table = sourceTable, index = index)
                    }
                }
                table.indexes.sortedWith(compareBy { it.indexID }).forEach { index ->
                    val current = existingIndexes[index.name]
                    val expected = PhysicalIndex(index.name, index.unique, index.columnNames, partial = false)
                    if (current == null || current != expected || ownedIndexes[index.name] != index) {
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
                        if (column.name !in tableColumnNames(db, table.tableName)) {
                            addSyncedColumn(db, table, column)
                        }
                    }
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
    }

    /** Explicit reset replaces only materialized synced tables and state. */
    private fun resetSyncedMaterialization(db: SQLiteDatabase, targetTables: List<LocalSchemaTable>) {
        val currentTables = loadStoredLocalSchemaInTransaction(db).orEmpty()
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
            SQLiteSchema.generateCDCTriggers(table).forEach(db::execSQL)
            table.indexes.forEach { index ->
                db.execSQL(SQLiteSchema.generateCreateIndexSQL(table, index))
            }
        }
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

    private fun hasTable(db: SQLiteDatabase, tableName: String): Boolean = db.rawQuery(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        arrayOf(tableName),
    ).use { it.moveToFirst() }

    private fun tableColumnNames(db: SQLiteDatabase, tableName: String): Set<String> {
        val names = mutableSetOf<String>()
        db.rawQuery("PRAGMA table_info(${SQLiteHelpers.quoteIdentifier(tableName)})", null).use { cursor ->
            val index = cursor.getColumnIndex("name")
            while (cursor.moveToNext()) names += cursor.getString(index)
        }
        return names
    }

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
        db.rawQuery("PRAGMA table_info(${SQLiteHelpers.quoteIdentifier(tableName)})", null).use { cursor ->
            val nameIndex = cursor.getColumnIndexOrThrow("name")
            val typeIndex = cursor.getColumnIndexOrThrow("type")
            val notNullIndex = cursor.getColumnIndexOrThrow("notnull")
            val defaultIndex = cursor.getColumnIndexOrThrow("dflt_value")
            val primaryKeyIndex = cursor.getColumnIndexOrThrow("pk")
            while (cursor.moveToNext()) {
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

    private fun validateTargetPhysicalSchema(db: SQLiteDatabase, tables: List<LocalSchemaTable>) {
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
            if (actualColumns.size != expectedColumns.size ||
                actualColumns.associateBy { it.name } != expectedColumns.associateBy { it.name }
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
        try {
            ApplicationWriteGuard.requireExactAllowedTriggerSet(db, tables)
        } catch (_: IllegalStateException) {
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
        if (journal.targetTables.associateBy { it.tableID } != expectedTables) {
            throw SynchroError.InvalidResponse("schema migration journal tables are invalid")
        }
        if (journal.affectedScopes.any { it.isEmpty() } ||
            journal.affectedScopes != journal.affectedScopes.distinct().sortedWith(unsignedUTF8Comparator) ||
            journal.scopeCursorUpdates.keys.any { it.isEmpty() }
        ) {
            throw SynchroError.InvalidResponse("schema migration journal scope state is invalid")
        }
        val planJSON = json.encodeToString(journal.plan)
        if (migrationPlanHash(planJSON) != journal.planHash) {
            throw SynchroError.InvalidResponse("schema migration journal plan is invalid")
        }
        validateMigrationPlan(journal.plan, journal.targetTables)
    }

    private fun validateMigrationPlan(plan: MigrationPlan, targetTables: List<LocalSchemaTable>) {
        val targetByID = targetTables.associateBy { it.tableID }
        plan.operations.forEach { operation ->
            val table = operation.table
            when (operation.kind) {
                MigrationOperationKind.CREATE_TABLE,
                MigrationOperationKind.DROP_TABLE -> {
                    if (table == null || operation.columnFieldIDs.isNotEmpty() || operation.index != null) {
                        throw SynchroError.InvalidResponse("schema migration plan table operation is invalid")
                    }
                }
                MigrationOperationKind.ADD_COLUMNS -> {
                    if (table == null || operation.index != null || operation.columnFieldIDs.isEmpty() ||
                        operation.columnFieldIDs != operation.columnFieldIDs.distinct().sortedWith(unsignedUTF8Comparator) ||
                        operation.columnFieldIDs.any { fieldID -> table.columns.none { it.fieldID == fieldID } }
                    ) {
                        throw SynchroError.InvalidResponse("schema migration plan column operation is invalid")
                    }
                }
                MigrationOperationKind.DROP_INDEX,
                MigrationOperationKind.CREATE_INDEX -> {
                    val index = operation.index
                    if (table == null || index == null || operation.columnFieldIDs.isNotEmpty() ||
                        index.columnNames.isEmpty() || index.columnNames.any { name -> table.columns.none { it.name == name } }
                    ) {
                        throw SynchroError.InvalidResponse("schema migration plan index operation is invalid")
                    }
                    if (operation.kind == MigrationOperationKind.CREATE_INDEX &&
                        targetByID[table.tableID]?.indexes?.none { it == index } != false
                    ) {
                        throw SynchroError.InvalidResponse("schema migration plan creates an unknown target index")
                    }
                }
                MigrationOperationKind.REPLACE_SYNCED_MATERIALIZATION -> {
                    if (table != null || operation.columnFieldIDs.isNotEmpty() || operation.index != null) {
                        throw SynchroError.InvalidResponse("schema migration reset operation is invalid")
                    }
                }
            }
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

    fun dropSyncedTables(schema: SchemaResponse) {
        database.writeTransaction { db ->
            dropSyncedTablesInTransaction(db, schema)
        }
    }

    internal fun dropSyncedTablesInTransaction(db: android.database.sqlite.SQLiteDatabase, schema: SchemaResponse) {
        dropSyncedTablesInTransaction(db, schema.localTables())
    }

    internal fun dropSyncedTablesInTransaction(db: android.database.sqlite.SQLiteDatabase, tables: List<LocalSchemaTable>) {
        for (table in tables.reversed()) {
            val quoted = SQLiteHelpers.quoteIdentifier(table.tableName)
            val trigInsert = SQLiteHelpers.quoteIdentifier("_synchro_cdc_insert_${table.tableName}")
            val trigUpdate = SQLiteHelpers.quoteIdentifier("_synchro_cdc_update_${table.tableName}")
            val trigDelete = SQLiteHelpers.quoteIdentifier("_synchro_cdc_delete_${table.tableName}")
            db.execSQL("DROP TRIGGER IF EXISTS $trigInsert")
            db.execSQL("DROP TRIGGER IF EXISTS $trigUpdate")
            db.execSQL("DROP TRIGGER IF EXISTS $trigDelete")
            db.execSQL("DROP TABLE IF EXISTS $quoted")
        }
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
            INSERT INTO _synchro_schema_archives (schema_version, schema_hash, manifest_json, created_at)
            VALUES (?, ?, ?, substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z')
            ON CONFLICT (schema_version, schema_hash) DO NOTHING
            """.trimIndent(),
            arrayOf(schemaVersion, schemaHash, json.encodeToString(tables)),
        )
    }

    private fun ensureCurrentSchemaIsArchived(db: android.database.sqlite.SQLiteDatabase) {
        val schemaVersion = SynchroMeta.getInt64(db, MetaKey.SCHEMA_VERSION)
        val schemaHash = SynchroMeta.get(db, MetaKey.SCHEMA_HASH)
        if (schemaVersion <= 0L || schemaHash.isNullOrEmpty()) {
            throw SynchroError.InvalidResponse("synced-table capture requires a verified schema reference")
        }
        val archived = db.rawQuery(
            "SELECT 1 FROM _synchro_schema_archives WHERE schema_version = ? AND schema_hash = ?",
            arrayOf(schemaVersion.toString(), schemaHash),
        ).use { it.moveToFirst() }
        if (!archived) {
            throw SynchroError.InvalidResponse("synced-table capture requires archived schema metadata")
        }
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
