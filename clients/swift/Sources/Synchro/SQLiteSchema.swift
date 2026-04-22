import Foundation

enum SQLiteSchema {
    static func normalizedLogicalType(_ logicalType: String) -> String {
        let normalized = logicalType.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()

        if normalized.hasSuffix("[]") {
            return "json"
        }
        if normalized.hasPrefix("numeric(") || normalized.hasPrefix("decimal(") {
            return "float"
        }
        if normalized.hasPrefix("character varying")
            || normalized.hasPrefix("varchar(")
            || normalized.hasPrefix("character(")
        {
            return "string"
        }
        if normalized.hasSuffix("range") {
            return "string"
        }

        switch normalized {
        case "string", "text", "uuid", "varchar", "character",
             "interval", "inet", "cidr", "macaddr", "macaddr8", "xml",
             "point", "line", "lseg", "box", "path", "polygon", "circle":
            return "string"
        case "int", "int32", "smallint", "integer":
            return "int"
        case "int64", "bigint":
            return "int64"
        case "float", "float64", "numeric", "decimal", "real", "double precision":
            return "float"
        case "boolean", "bool":
            return "boolean"
        case "datetime", "timestamp", "timestamp with time zone", "timestamp without time zone":
            return "datetime"
        case "date":
            return "date"
        case "time", "time without time zone":
            return "time"
        case "json", "jsonb":
            return "json"
        case "bytes", "blob", "bytea":
            return "bytes"
        default:
            return normalized
        }
    }

    static func sqliteType(for logicalType: String) -> String {
        switch normalizedLogicalType(logicalType) {
        case "string":   return "TEXT"
        case "int":      return "INTEGER"
        case "int64":    return "INTEGER"
        case "float":    return "REAL"
        case "boolean":  return "INTEGER"
        case "datetime": return "TEXT"
        case "date":     return "TEXT"
        case "time":     return "TEXT"
        case "json":     return "TEXT"
        case "bytes":    return "BLOB"
        default:         return "TEXT"
        }
    }

    static func generateCreateTableSQL(table: LocalSchemaTable) -> String {
        let quotedName = SQLiteHelpers.quoteIdentifier(table.tableName)
        var colDefs: [String] = []

        for col in table.columns {
            let quotedCol = SQLiteHelpers.quoteIdentifier(col.name)
            let sqlType = sqliteType(for: col.logicalType)
            var def = "\(quotedCol) \(sqlType)"
            if col.isPrimaryKey {
                def += " PRIMARY KEY"
            }
            if !col.nullable && !col.isPrimaryKey {
                def += " NOT NULL"
            }
            if let defaultSQL = col.sqliteDefaultSQL, !defaultSQL.isEmpty {
                def += " DEFAULT \(defaultSQL)"
            }
            colDefs.append(def)
        }

        return "CREATE TABLE IF NOT EXISTS \(quotedName) (\(colDefs.joined(separator: ", ")))"
    }

    static func generateCDCTriggers(table: LocalSchemaTable) -> [String] {
        let name = table.tableName
        let quoted = SQLiteHelpers.quoteIdentifier(name)
        let pkCol = table.primaryKey.first ?? "id"
        let quotedPK = SQLiteHelpers.quoteIdentifier(pkCol)
        let updatedAtCol = table.updatedAtColumn
        let deletedAtCol = table.deletedAtColumn
        let quotedUpdatedAt = SQLiteHelpers.quoteIdentifier(updatedAtCol)
        let quotedDeletedAt = SQLiteHelpers.quoteIdentifier(deletedAtCol)

        let lockCheck = "(SELECT value FROM _synchro_meta WHERE key = 'sync_lock') = '0'"
        let tsNow = "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"

        var triggers: [String] = []

        let escapedName = name.replacingOccurrences(of: "'", with: "''")
        let quotedInsertTrigger = SQLiteHelpers.quoteIdentifier("_synchro_cdc_insert_\(name)")
        let quotedUpdateTrigger = SQLiteHelpers.quoteIdentifier("_synchro_cdc_update_\(name)")
        let quotedDeleteTrigger = SQLiteHelpers.quoteIdentifier("_synchro_cdc_delete_\(name)")

        // DROP existing triggers first (for re-creation on schema update)
        triggers.append("DROP TRIGGER IF EXISTS \(quotedInsertTrigger)")
        triggers.append("DROP TRIGGER IF EXISTS \(quotedUpdateTrigger)")
        triggers.append("DROP TRIGGER IF EXISTS \(quotedDeleteTrigger)")

        // INSERT trigger
        triggers.append("""
            CREATE TRIGGER \(quotedInsertTrigger)
            AFTER INSERT ON \(quoted)
            WHEN \(lockCheck)
            BEGIN
                INSERT INTO _synchro_pending_changes (record_id, table_name, operation, client_updated_at)
                VALUES (NEW.\(quotedPK), '\(escapedName)', 'create', \(tsNow))
                ON CONFLICT (table_name, record_id) DO UPDATE SET
                    operation = CASE
                        WHEN _synchro_pending_changes.operation = 'delete' THEN 'update'
                        ELSE _synchro_pending_changes.operation
                    END,
                    client_updated_at = excluded.client_updated_at;
            END
            """)

        // UPDATE trigger
        triggers.append("""
            CREATE TRIGGER \(quotedUpdateTrigger)
            AFTER UPDATE ON \(quoted)
            WHEN \(lockCheck)
            BEGIN
                INSERT INTO _synchro_pending_changes (record_id, table_name, operation, base_updated_at, client_updated_at)
                VALUES (
                    NEW.\(quotedPK), '\(escapedName)',
                    CASE WHEN NEW.\(quotedDeletedAt) IS NOT NULL AND OLD.\(quotedDeletedAt) IS NULL THEN 'delete' ELSE 'update' END,
                    OLD.\(quotedUpdatedAt),
                    \(tsNow)
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
                WHERE table_name = '\(escapedName)' AND record_id = NEW.\(quotedPK)
                  AND operation = 'delete'
                  AND base_updated_at IS NULL;
            END
            """)

        // BEFORE DELETE trigger (converts hard delete to soft delete)
        triggers.append("""
            CREATE TRIGGER \(quotedDeleteTrigger)
            BEFORE DELETE ON \(quoted)
            WHEN \(lockCheck)
            BEGIN
                UPDATE \(quoted) SET \(quotedDeletedAt) = \(tsNow) WHERE \(quotedPK) = OLD.\(quotedPK);
                SELECT RAISE(IGNORE);
            END
            """)

        return triggers
    }

}
