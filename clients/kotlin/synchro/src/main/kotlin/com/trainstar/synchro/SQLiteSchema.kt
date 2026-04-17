package com.trainstar.synchro

object SQLiteSchema {
    fun normalizedLogicalType(logicalType: String): String {
        val normalized = logicalType.trim().lowercase()
        return when {
            normalized.endsWith("[]") -> "json"
            normalized.startsWith("numeric(") || normalized.startsWith("decimal(") -> "float"
            normalized.startsWith("character varying")
                || normalized.startsWith("varchar(")
                || normalized.startsWith("character(") -> "string"
            normalized.endsWith("range") -> "string"
            normalized in setOf(
                "string", "text", "uuid", "varchar", "character",
                "interval", "inet", "cidr", "macaddr", "macaddr8", "xml",
                "point", "line", "lseg", "box", "path", "polygon", "circle"
            ) -> "string"
            normalized in setOf("int", "int32", "smallint", "integer") -> "int"
            normalized in setOf("int64", "bigint") -> "int64"
            normalized in setOf("float", "float64", "numeric", "decimal", "real", "double precision") -> "float"
            normalized in setOf("boolean", "bool") -> "boolean"
            normalized in setOf("datetime", "timestamp", "timestamp with time zone", "timestamp without time zone") -> "datetime"
            normalized == "date" -> "date"
            normalized == "time" || normalized == "time without time zone" -> "time"
            normalized in setOf("json", "jsonb") -> "json"
            normalized in setOf("bytes", "blob", "bytea") -> "bytes"
            else -> normalized
        }
    }

    fun sqliteType(logicalType: String): String = when (normalizedLogicalType(logicalType)) {
        "string" -> "TEXT"
        "int" -> "INTEGER"
        "int64" -> "INTEGER"
        "float" -> "REAL"
        "boolean" -> "INTEGER"
        "datetime" -> "TEXT"
        "date" -> "TEXT"
        "time" -> "TEXT"
        "json" -> "TEXT"
        "bytes" -> "BLOB"
        else -> "TEXT"
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

    fun generateCDCTriggers(table: LocalSchemaTable): List<String> {
        val name = table.tableName
        val quoted = SQLiteHelpers.quoteIdentifier(name)
        val safeName = SQLiteHelpers.escapeSQLString(name)
        val quotedTriggerInsert = SQLiteHelpers.quoteIdentifier("_synchro_cdc_insert_$name")
        val quotedTriggerUpdate = SQLiteHelpers.quoteIdentifier("_synchro_cdc_update_$name")
        val quotedTriggerDelete = SQLiteHelpers.quoteIdentifier("_synchro_cdc_delete_$name")
        val pkCol = table.primaryKey.firstOrNull() ?: "id"
        val quotedPK = SQLiteHelpers.quoteIdentifier(pkCol)
        val updatedAtCol = table.updatedAtColumn
        val deletedAtCol = table.deletedAtColumn
        val quotedUpdatedAt = SQLiteHelpers.quoteIdentifier(updatedAtCol)
        val quotedDeletedAt = SQLiteHelpers.quoteIdentifier(deletedAtCol)

        val lockCheck = "(SELECT value FROM _synchro_meta WHERE key = 'sync_lock') = '0'"
        val tsNow = "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"

        val triggers = mutableListOf<String>()

        // DROP existing triggers first (for re-creation on schema update)
        triggers.add("DROP TRIGGER IF EXISTS $quotedTriggerInsert")
        triggers.add("DROP TRIGGER IF EXISTS $quotedTriggerUpdate")
        triggers.add("DROP TRIGGER IF EXISTS $quotedTriggerDelete")

        // INSERT trigger
        triggers.add("""
            CREATE TRIGGER $quotedTriggerInsert
            AFTER INSERT ON $quoted
            WHEN $lockCheck
            BEGIN
                INSERT INTO _synchro_pending_changes (record_id, table_name, operation, client_updated_at)
                VALUES (NEW.$quotedPK, '$safeName', 'create', $tsNow)
                ON CONFLICT (table_name, record_id) DO UPDATE SET
                    operation = CASE
                        WHEN _synchro_pending_changes.operation = 'delete' THEN 'update'
                        ELSE _synchro_pending_changes.operation
                    END,
                    client_updated_at = excluded.client_updated_at;
            END
        """.trimIndent())

        // UPDATE trigger
        triggers.add("""
            CREATE TRIGGER $quotedTriggerUpdate
            AFTER UPDATE ON $quoted
            WHEN $lockCheck
            BEGIN
                INSERT INTO _synchro_pending_changes (record_id, table_name, operation, base_updated_at, client_updated_at)
                VALUES (
                    NEW.$quotedPK, '$safeName',
                    CASE WHEN NEW.$quotedDeletedAt IS NOT NULL AND OLD.$quotedDeletedAt IS NULL THEN 'delete' ELSE 'update' END,
                    OLD.$quotedUpdatedAt,
                    $tsNow
                )
                ON CONFLICT (table_name, record_id) DO UPDATE SET
                    operation = CASE
                        WHEN _synchro_pending_changes.operation = 'create' AND excluded.operation = 'update' THEN 'create'
                        WHEN _synchro_pending_changes.operation = 'create' AND excluded.operation = 'delete' THEN 'delete'
                        ELSE excluded.operation
                    END,
                    base_updated_at = CASE
                        WHEN _synchro_pending_changes.operation = 'create' AND excluded.operation = 'delete' THEN NULL
                        ELSE COALESCE(_synchro_pending_changes.base_updated_at, excluded.base_updated_at)
                    END,
                    client_updated_at = excluded.client_updated_at;
                DELETE FROM _synchro_pending_changes
                WHERE table_name = '$safeName' AND record_id = NEW.$quotedPK
                  AND operation = 'delete'
                  AND base_updated_at IS NULL;
            END
        """.trimIndent())

        // BEFORE DELETE trigger (converts hard delete to soft delete)
        triggers.add("""
            CREATE TRIGGER $quotedTriggerDelete
            BEFORE DELETE ON $quoted
            WHEN $lockCheck
            BEGIN
                UPDATE $quoted SET $quotedDeletedAt = $tsNow WHERE $quotedPK = OLD.$quotedPK;
                SELECT RAISE(IGNORE);
            END
        """.trimIndent())

        return triggers
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
