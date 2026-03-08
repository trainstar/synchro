import Foundation

enum SQLiteSchema {
    static func sqliteType(for logicalType: String) -> String {
        switch logicalType {
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

    static func generateCreateTableSQL(table: SchemaTable) -> String {
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
            colDefs.append(def)
        }

        return "CREATE TABLE IF NOT EXISTS \(quotedName) (\(colDefs.joined(separator: ", ")))"
    }

    static func generateCDCTriggers(table: SchemaTable) -> [String] {
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

        // DROP existing triggers first (for re-creation on schema update)
        triggers.append("DROP TRIGGER IF EXISTS _synchro_cdc_insert_\(name)")
        triggers.append("DROP TRIGGER IF EXISTS _synchro_cdc_update_\(name)")
        triggers.append("DROP TRIGGER IF EXISTS _synchro_cdc_delete_\(name)")

        // INSERT trigger
        triggers.append("""
            CREATE TRIGGER _synchro_cdc_insert_\(name)
            AFTER INSERT ON \(quoted)
            WHEN \(lockCheck)
            BEGIN
                INSERT INTO _synchro_pending_changes (record_id, table_name, operation, client_updated_at)
                VALUES (NEW.\(quotedPK), '\(name)', 'create', \(tsNow))
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
            CREATE TRIGGER _synchro_cdc_update_\(name)
            AFTER UPDATE ON \(quoted)
            WHEN \(lockCheck)
            BEGIN
                INSERT INTO _synchro_pending_changes (record_id, table_name, operation, base_updated_at, client_updated_at)
                VALUES (
                    NEW.\(quotedPK), '\(name)',
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
                WHERE table_name = '\(name)' AND record_id = NEW.\(quotedPK)
                  AND operation = 'delete'
                  AND base_updated_at IS NULL;
            END
            """)

        // BEFORE DELETE trigger (converts hard delete to soft delete)
        triggers.append("""
            CREATE TRIGGER _synchro_cdc_delete_\(name)
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
