package com.trainstar.synchro

class SchemaManager(private val database: SynchroDatabase) {

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

        database.writeTransaction { db ->
            SynchroMeta.setInt64(db, MetaKey.SCHEMA_VERSION, schema.schemaVersion)
            SynchroMeta.set(db, MetaKey.SCHEMA_HASH, schema.schemaHash)
        }

        return schema
    }

    fun createSyncedTables(schema: SchemaResponse) {
        database.writeTransaction { db ->
            createSyncedTablesInTransaction(db, schema)
        }
    }

    internal fun createSyncedTablesInTransaction(db: android.database.sqlite.SQLiteDatabase, schema: SchemaResponse) {
        for (table in schema.tables) {
            val createSQL = SQLiteSchema.generateCreateTableSQL(table)
            db.execSQL(createSQL)

            val triggers = SQLiteSchema.generateCDCTriggers(table)
            for (trigger in triggers) {
                db.execSQL(trigger)
            }
        }
    }

    fun migrateSchema(newSchema: SchemaResponse) {
        database.writeTransaction { db ->
            if (requiresDestructiveRebuild(db, newSchema)) {
                dropSyncedTablesInTransaction(db, SchemaResponse(0, "", "", newSchema.tables))
                clearLocalStateForRebuild(db)
                createSyncedTablesInTransaction(db, newSchema)
                return@writeTransaction
            }

            for (table in newSchema.tables) {
                // Check if table exists
                val tableExists = db.rawQuery(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    arrayOf(table.tableName)
                ).use { it.moveToFirst() }

                if (!tableExists) {
                    val createSQL = SQLiteSchema.generateCreateTableSQL(table)
                    db.execSQL(createSQL)
                } else {
                    // Get existing columns
                    val existingColumns = mutableSetOf<String>()
                    db.rawQuery("PRAGMA table_info(${SQLiteHelpers.quoteIdentifier(table.tableName)})", null).use { cursor ->
                        val nameIdx = cursor.getColumnIndex("name")
                        while (cursor.moveToNext()) {
                            existingColumns.add(cursor.getString(nameIdx))
                        }
                    }

                    for (col in table.columns) {
                        if (col.name !in existingColumns) {
                            val sqlType = SQLiteSchema.sqliteType(col.logicalType)
                            val quotedTable = SQLiteHelpers.quoteIdentifier(table.tableName)
                            val quotedCol = SQLiteHelpers.quoteIdentifier(col.name)
                            // ALTER TABLE ADD COLUMN in SQLite requires constant defaults for NOT NULL columns.
                            // Non-constant defaults (CURRENT_TIMESTAMP, etc.) are rejected. Adding as nullable
                            // is safe: existing rows get NULL, the server enforces constraints on push.
                            val hasDefault = !col.sqliteDefaultSQL.isNullOrEmpty()
                            val isConstantDefault = hasDefault && !isNonConstantDefault(col.sqliteDefaultSQL!!)
                            val notNullClause = if (!col.nullable && !col.isPrimaryKey && isConstantDefault) " NOT NULL" else ""
                            val defaultClause = if (isConstantDefault) " DEFAULT ${col.sqliteDefaultSQL}" else ""
                            db.execSQL("ALTER TABLE $quotedTable ADD COLUMN $quotedCol $sqlType$notNullClause$defaultClause")
                        }
                    }
                }

                // Recreate all CDC triggers
                val triggers = SQLiteSchema.generateCDCTriggers(table)
                for (trigger in triggers) {
                    db.execSQL(trigger)
                }
            }
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

    /**
     * Only triggers destructive rebuild when a synced column's type has changed.
     * Extra local tables, extra local columns, and removed server columns are all preserved.
     */
    private fun requiresDestructiveRebuild(db: android.database.sqlite.SQLiteDatabase, newSchema: SchemaResponse): Boolean {
        val newTableMap = newSchema.tables.associateBy { it.tableName }

        for ((tableName, table) in newTableMap) {
            val tableExists = db.rawQuery(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                arrayOf(tableName)
            ).use { it.moveToFirst() }
            if (!tableExists) continue

            val existingColumnTypes = mutableMapOf<String, String>()
            db.rawQuery("PRAGMA table_info(${SQLiteHelpers.quoteIdentifier(tableName)})", null).use { cursor ->
                val nameIdx = cursor.getColumnIndex("name")
                val typeIdx = cursor.getColumnIndex("type")
                while (cursor.moveToNext()) {
                    existingColumnTypes[cursor.getString(nameIdx)] = cursor.getString(typeIdx).uppercase()
                }
            }

            for (col in table.columns) {
                val localType = existingColumnTypes[col.name] ?: continue
                val serverType = SQLiteSchema.sqliteType(col.logicalType).uppercase()
                if (localType != serverType) {
                    return true
                }
            }
        }

        return false
    }

    private fun clearLocalStateForRebuild(db: android.database.sqlite.SQLiteDatabase) {
        db.execSQL("DELETE FROM _synchro_pending_changes")
        SynchroMeta.setInt64(db, MetaKey.CHECKPOINT, 0L)
        SynchroMeta.set(db, MetaKey.KNOWN_BUCKETS, "[]")
        SynchroMeta.set(db, MetaKey.SNAPSHOT_COMPLETE, "0")
        SynchroMeta.clearAllBucketCheckpoints(db)
        db.execSQL("DELETE FROM _synchro_bucket_members")
    }

    fun dropSyncedTables(schema: SchemaResponse) {
        database.writeTransaction { db ->
            dropSyncedTablesInTransaction(db, schema)
        }
    }

    internal fun dropSyncedTablesInTransaction(db: android.database.sqlite.SQLiteDatabase, schema: SchemaResponse) {
        for (table in schema.tables.reversed()) {
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
}
