package com.trainstar.synchro

object SQLiteSchema {
    fun sqliteType(logicalType: String): String = when (logicalType) {
        "string" -> "TEXT"
        "int" -> "INTEGER"
        "int64" -> "INTEGER"
        "float" -> "REAL"
        "decimal" -> "TEXT"
        "boolean" -> "INTEGER"
        "datetime" -> "TEXT"
        "date" -> "TEXT"
        "time" -> "TEXT"
        "json" -> "TEXT"
        "bytes" -> "BLOB"
        else -> error("unsupported portable type $logicalType")
    }

    fun generateCreateTableSQL(table: LocalSchemaTable): String {
        val quotedName = SQLiteHelpers.quoteIdentifier(table.tableName)
        val colDefs = table.columns.map { col ->
            val quotedCol = SQLiteHelpers.quoteIdentifier(col.name)
            val sqlType = sqliteType(col.logicalType)
            buildString {
                append("$quotedCol $sqlType")
                if (col.isPrimaryKey) append(" PRIMARY KEY")
                if (!col.nullable && !col.isPrimaryKey) append(" NOT NULL")
                if (!col.sqliteDefaultSQL.isNullOrEmpty()) append(" DEFAULT ${col.sqliteDefaultSQL}")
            }
        }
        return "CREATE TABLE IF NOT EXISTS $quotedName (${colDefs.joinToString(", ")})"
    }

    fun generateCreateIndexSQL(
        table: LocalSchemaTable,
        index: LocalSchemaIndex,
        ifNotExists: Boolean = true,
    ): String {
        val uniqueness = if (index.unique) "UNIQUE " else ""
        val existence = if (ifNotExists) "IF NOT EXISTS " else ""
        val columns = index.columnNames.joinToString(", ") { SQLiteHelpers.quoteIdentifier(it) }
        return "CREATE ${uniqueness}INDEX ${existence}${SQLiteHelpers.quoteIdentifier(index.name)} " +
            "ON ${SQLiteHelpers.quoteIdentifier(table.tableName)} ($columns)"
    }

    fun generateCDCTriggers(table: LocalSchemaTable): List<String> {
        val name = table.tableName
        val quoted = SQLiteHelpers.quoteIdentifier(name)
        val safeName = SQLiteHelpers.escapeSQLString(name)
        val safeTableID = SQLiteHelpers.escapeSQLString(table.tableID)
        val safePKFieldID = SQLiteHelpers.escapeSQLString(table.primaryKeyFieldID)
        val quotedTriggerInsert = SQLiteHelpers.quoteIdentifier("_synchro_cdc_insert_$name")
        val quotedTriggerUpdate = SQLiteHelpers.quoteIdentifier("_synchro_cdc_update_$name")
        val quotedTriggerDelete = SQLiteHelpers.quoteIdentifier("_synchro_cdc_delete_$name")
        val quotedTriggerPrimaryKey = SQLiteHelpers.quoteIdentifier("_synchro_cdc_pk_guard_$name")
        val pkCol = table.primaryKey.firstOrNull() ?: "id"
        val quotedPK = SQLiteHelpers.quoteIdentifier(pkCol)
        val primaryKey = table.columns.singleOrNull { it.fieldID == table.primaryKeyFieldID }
            ?: throw IllegalArgumentException("missing primary key field for ${table.tableName}")
        val deletedAtCol = table.deletedAtColumn.takeIf { it.isNotBlank() }
        val quotedDeletedAt = deletedAtCol?.let(SQLiteHelpers::quoteIdentifier)
        val writable = table.columns.filter { it.writable }

        val lockCheck = "(SELECT value FROM _synchro_meta WHERE key = 'sync_lock') = '0'"
        val tsNow = "substr(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), 1, 23) || '000Z'"
        val verifiedSchema = """
            EXISTS (
                SELECT 1
                FROM _synchro_schema_archives archive
                JOIN _synchro_meta version_meta ON version_meta.key = 'schema_version'
                JOIN _synchro_meta hash_meta ON hash_meta.key = 'schema_hash'
                WHERE archive.schema_version = CAST(version_meta.value AS INTEGER)
                  AND archive.schema_hash = hash_meta.value
                  AND archive.schema_version > 0
            )
        """.trimIndent().replace("\n", " ")
        val schemaGuard = "SELECT CASE WHEN $verifiedSchema THEN 1 ELSE RAISE(ABORT, 'verified Synchro schema metadata is required for capture') END;"
        fun intentHas(fieldID: String): String {
            val safeFieldID = SQLiteHelpers.escapeSQLString(fieldID)
            return """
                EXISTS (
                    SELECT 1
                    FROM _synchro_capture_context AS context
                    JOIN _synchro_capture_fields AS field
                      ON field.statement_token = context.statement_token
                    WHERE context.singleton = 1
                      AND context.table_id = '$safeTableID'
                      AND field.field_id = '$safeFieldID'
                )
            """.trimIndent().replace("\n", " ")
        }
        val intentHasWritable = if (writable.isEmpty()) {
            "0"
        } else {
            val writableFieldIDs = writable.joinToString(", ") { column ->
                "'${SQLiteHelpers.escapeSQLString(column.fieldID)}'"
            }
            """
                EXISTS (
                    SELECT 1
                    FROM _synchro_capture_context AS context
                    JOIN _synchro_capture_fields AS field
                      ON field.statement_token = context.statement_token
                    WHERE context.singleton = 1
                      AND context.table_id = '$safeTableID'
                      AND field.field_id IN ($writableFieldIDs)
                )
            """.trimIndent().replace("\n", " ")
        }
        val triggers = mutableListOf<String>()

        // DROP existing triggers first (for re-creation on schema update)
        triggers.add("DROP TRIGGER IF EXISTS $quotedTriggerInsert")
        triggers.add("DROP TRIGGER IF EXISTS $quotedTriggerUpdate")
        triggers.add("DROP TRIGGER IF EXISTS $quotedTriggerDelete")
        triggers.add("DROP TRIGGER IF EXISTS $quotedTriggerPrimaryKey")

        // A primary-key edit would change the protocol row identity. Reject it
        // before the application write commits or the capture trigger can run.
        triggers.add("""
            CREATE TRIGGER $quotedTriggerPrimaryKey
            BEFORE UPDATE OF $quotedPK ON $quoted
            WHEN $lockCheck AND NEW.$quotedPK IS NOT OLD.$quotedPK
            BEGIN
                SELECT RAISE(ABORT, 'synced primary key cannot change');
            END
        """.trimIndent())

        val insertMutation = mutationInsertSQL(
            tableID = safeTableID,
            tableName = safeName,
            primaryKeyFieldID = safePKFieldID,
            primaryKeyType = primaryKey.logicalType,
            recordExpression = "NEW.$quotedPK",
            operation = "'insert'",
            baseVersionExpression = "NULL",
            timestampExpression = tsNow,
        )

        // Inserts have no server base. They retain every authored writable field.
        triggers.add("""
            CREATE TRIGGER $quotedTriggerInsert
            AFTER INSERT ON $quoted
            WHEN $lockCheck
            BEGIN
                $schemaGuard
                ${if (writable.isEmpty()) "SELECT RAISE(ABORT, 'synced insert has no writable fields');" else ""}
                ${if (writable.isEmpty()) "" else "SELECT CASE WHEN NOT ($intentHasWritable) THEN RAISE(ABORT, 'synced insert has no authored writable fields') END;"}
                $insertMutation
                ${writable.joinToString("\n") {
                    valueInsertSQL(
                        it,
                        "NEW.${SQLiteHelpers.quoteIdentifier(it.name)}",
                        safeTableID,
                        safePKFieldID,
                         primaryKey.logicalType,
                         "NEW.$quotedPK",
                         captureCondition = intentHas(it.fieldID),
                     )
                 }}
            END
        """.trimIndent())

        val changedWritable = writable.joinToString(" OR ") { column ->
            "(${intentHas(column.fieldID)}) AND " +
                "NEW.${SQLiteHelpers.quoteIdentifier(column.name)} IS NOT OLD.${SQLiteHelpers.quoteIdentifier(column.name)}"
        }.ifEmpty { "0" }
        val deleteTransition = deletedAtCol?.let { column ->
            "NEW.${SQLiteHelpers.quoteIdentifier(column)} IS NOT NULL AND OLD.${SQLiteHelpers.quoteIdentifier(column)} IS NULL"
        } ?: "0"
        val updateOperation = "CASE WHEN $deleteTransition THEN 'delete' ELSE 'update' END"
        val updateMutation = mutationInsertSQL(
            tableID = safeTableID,
            tableName = safeName,
            primaryKeyFieldID = safePKFieldID,
            primaryKeyType = primaryKey.logicalType,
            recordExpression = "NEW.$quotedPK",
            operation = updateOperation,
            baseVersionExpression = "(SELECT server_version FROM _synchro_row_versions WHERE table_name = '$safeName' AND record_id = CAST(NEW.$quotedPK AS TEXT))",
            timestampExpression = tsNow,
        )

        // Updates retain only changed writable fields. Soft deletion becomes delete.
        triggers.add("""
            CREATE TRIGGER $quotedTriggerUpdate
            AFTER UPDATE ON $quoted
            WHEN $lockCheck AND ($deleteTransition OR $changedWritable)
            BEGIN
                $schemaGuard
                $updateMutation
                ${writable.joinToString("\n") { column ->
                    valueInsertSQL(
                        column,
                        "NEW.${SQLiteHelpers.quoteIdentifier(column.name)}",
                        safeTableID,
                        safePKFieldID,
                         primaryKey.logicalType,
                         "NEW.$quotedPK",
                          captureCondition = intentHas(column.fieldID),
                         changedCondition = "NOT ($deleteTransition) AND NEW.${SQLiteHelpers.quoteIdentifier(column.name)} IS NOT OLD.${SQLiteHelpers.quoteIdentifier(column.name)}",
                     )
                }}
            END
        """.trimIndent())

        if (quotedDeletedAt != null) {
            // A hard application delete uses the same soft-delete capture path.
            triggers.add("""
                CREATE TRIGGER $quotedTriggerDelete
                BEFORE DELETE ON $quoted
                WHEN $lockCheck
                BEGIN
                    UPDATE $quoted SET $quotedDeletedAt = $tsNow WHERE $quotedPK = OLD.$quotedPK;
                    SELECT RAISE(IGNORE);
                END
            """.trimIndent())
        } else {
            val hardDeleteMutation = mutationInsertSQL(
                tableID = safeTableID,
                tableName = safeName,
                primaryKeyFieldID = safePKFieldID,
                primaryKeyType = primaryKey.logicalType,
                recordExpression = "OLD.$quotedPK",
                operation = "'delete'",
                baseVersionExpression = "(SELECT server_version FROM _synchro_row_versions WHERE table_name = '$safeName' AND record_id = CAST(OLD.$quotedPK AS TEXT))",
                timestampExpression = tsNow,
            )
            triggers.add("""
                CREATE TRIGGER $quotedTriggerDelete
                AFTER DELETE ON $quoted
                WHEN $lockCheck
                BEGIN
                    $schemaGuard
                    $hardDeleteMutation
                END
            """.trimIndent())
        }

        return triggers
    }

    /** The client owns this complete trigger set for each synced table. */
    internal fun expectedCDCTriggerSQL(table: LocalSchemaTable): Map<String, String> {
        val names = listOf(
            "_synchro_cdc_pk_guard_${table.tableName}",
            "_synchro_cdc_insert_${table.tableName}",
            "_synchro_cdc_update_${table.tableName}",
            "_synchro_cdc_delete_${table.tableName}",
        )
        val generated = generateCDCTriggers(table)
        if (generated.size != names.size * 2) {
            throw SynchroError.InvalidResponse("generated capture trigger set is invalid")
        }
        return names.zip(generated.takeLast(names.size)).toMap()
    }

    /** SQLite preserves token content but may normalize formatting in stored DDL. */
    internal fun canonicalDDL(source: String): String {
        val output = StringBuilder(source.length)
        var pendingSpace = false
        source.forEach { character ->
            if (character.isWhitespace()) {
                pendingSpace = output.isNotEmpty()
            } else {
                if (pendingSpace) output.append(' ')
                output.append(character)
                pendingSpace = false
            }
        }
        return output.toString().trim()
    }

    private fun mutationInsertSQL(
        tableID: String,
        tableName: String,
        primaryKeyFieldID: String,
        primaryKeyType: String,
        recordExpression: String,
        operation: String,
        baseVersionExpression: String,
        timestampExpression: String,
    ): String {
        val unresolvedPredecessor = """
            (
                SELECT mutation_id
                FROM _synchro_pending_changes
                WHERE table_id = '$tableID' AND pk_field_id = '$primaryKeyFieldID'
                  AND pk_logical_type = '$primaryKeyType'
                  AND record_id = CAST($recordExpression AS TEXT)
                  AND lifecycle_state IN ('captured', 'sealed', 'legacy_blocked', 'blocked_by_predecessor')
                ORDER BY local_order DESC
                LIMIT 1
            )
        """.trimIndent()
        return """
        INSERT INTO _synchro_pending_changes (
            mutation_id, table_id, table_name, record_id, pk_field_id, pk_logical_type,
            operation, authored_schema_version, authored_schema_hash, base_version, client_version,
            lifecycle_state, source_kind, depends_on_mutation_id, created_at, updated_at
        )
        VALUES (
            ${SynchroDatabase.SQLITE_UUID}, '$tableID', '$tableName', CAST($recordExpression AS TEXT), '$primaryKeyFieldID', '$primaryKeyType',
            $operation,
            (SELECT CAST(value AS INTEGER) FROM _synchro_meta WHERE key = 'schema_version'),
            (SELECT value FROM _synchro_meta WHERE key = 'schema_hash'),
            CASE WHEN $unresolvedPredecessor IS NULL THEN $baseVersionExpression ELSE NULL END,
            $timestampExpression,
            'captured', 'capture',
            $unresolvedPredecessor,
            $timestampExpression, $timestampExpression
        );
        """.trimIndent()
    }

    private fun valueInsertSQL(
        column: LocalSchemaColumn,
        valueExpression: String,
        tableID: String,
        primaryKeyFieldID: String,
        primaryKeyType: String,
        recordExpression: String,
        captureCondition: String? = null,
        changedCondition: String? = null,
    ): String {
        val fieldID = SQLiteHelpers.escapeSQLString(column.fieldID)
        val type = SQLiteHelpers.escapeSQLString(column.logicalType)
        val valueKind = when (column.logicalType) {
            "boolean" -> "boolean"
            "int", "int64" -> "integer"
            "float" -> "real"
            "bytes" -> "blob"
            else -> "text"
        }
        val condition = buildString {
            captureCondition?.let { append(" AND ($it)") }
            changedCondition?.let { append(" AND ($it)") }
        }
        val integerValue = if (valueKind == "boolean" || valueKind == "integer") valueExpression else "NULL"
        val realValue = if (valueKind == "real") valueExpression else "NULL"
        val textValue = if (valueKind == "text") "CAST($valueExpression AS TEXT)" else "NULL"
        val blobValue = if (valueKind == "blob") valueExpression else "NULL"
        return """
            INSERT INTO _synchro_mutation_values (
                mutation_id, field_id, logical_type, value_kind, value_integer, value_real, value_text, value_blob
            )
            SELECT mutation_id, '$fieldID', '$type',
                CASE WHEN $valueExpression IS NULL THEN 'null' ELSE '$valueKind' END,
                CASE WHEN $valueExpression IS NULL THEN NULL ELSE $integerValue END,
                CASE WHEN $valueExpression IS NULL THEN NULL ELSE $realValue END,
                CASE WHEN $valueExpression IS NULL THEN NULL ELSE $textValue END,
                CASE WHEN $valueExpression IS NULL THEN NULL ELSE $blobValue END
            FROM _synchro_pending_changes
            WHERE mutation_id = (
                SELECT mutation_id
                FROM _synchro_pending_changes
                WHERE table_id = '$tableID' AND pk_field_id = '$primaryKeyFieldID'
                  AND pk_logical_type = '$primaryKeyType'
                  AND record_id = CAST($recordExpression AS TEXT)
                ORDER BY local_order DESC
                LIMIT 1
            )$condition;
        """.trimIndent()
    }

}

object SQLiteHelpers {
    fun quoteIdentifier(name: String): String {
        val escaped = name.replace("\"", "\"\"")
        return "\"$escaped\""
    }

    /** Escapes a string for use inside SQL single-quoted string literals. */
    fun escapeSQLString(value: String): String {
        return value.replace("'", "''")
    }

    fun placeholders(count: Int): String =
        (1..count).joinToString(", ") { "?" }

    fun timestampNow(): String =
        "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"

    fun databaseValue(anyCodable: AnyCodable): Any? = when (val v = anyCodable.value) {
        null -> null
        is Boolean -> if (v) 1L else 0L
        is Int -> v.toLong()
        is Long -> v
        is Double -> v
        is Float -> v.toDouble()
        is String -> v
        else -> {
            try {
                val json = kotlinx.serialization.json.Json.encodeToString(AnyCodableSerializer, anyCodable)
                json
            } catch (_: Exception) {
                null
            }
        }
    }
}
