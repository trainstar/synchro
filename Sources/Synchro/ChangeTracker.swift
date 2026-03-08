import Foundation
import GRDB

struct PendingChange: Sendable {
    let recordID: String
    let tableName: String
    let operation: String
    let baseUpdatedAt: String?
    let clientUpdatedAt: String
}

final class ChangeTracker: @unchecked Sendable {
    private let database: SynchroDatabase

    init(database: SynchroDatabase) {
        self.database = database
    }

    func pendingChanges(limit: Int = 100) throws -> [PendingChange] {
        try database.readTransaction { db in
            let rows = try Row.fetchAll(db, sql: """
                SELECT record_id, table_name, operation, base_updated_at, client_updated_at
                FROM _synchro_pending_changes
                ORDER BY client_updated_at ASC
                LIMIT ?
                """, arguments: [limit])

            return rows.map { row in
                PendingChange(
                    recordID: row["record_id"],
                    tableName: row["table_name"],
                    operation: row["operation"],
                    baseUpdatedAt: row["base_updated_at"],
                    clientUpdatedAt: row["client_updated_at"]
                )
            }
        }
    }

    func hydratePendingForPush(pending: [PendingChange], syncedTables: [SchemaTable]) throws -> [PushRecord] {
        let tableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })
        let decoder = JSONDecoder.synchroDecoder()
        let dateFormatter = ISO8601DateFormatter()
        dateFormatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]

        return try database.readTransaction { db in
            var records: [PushRecord] = []
            for change in pending {
                let clientDate = dateFormatter.date(from: change.clientUpdatedAt) ?? Date()
                var baseDate: Date?
                if let base = change.baseUpdatedAt {
                    baseDate = dateFormatter.date(from: base)
                }

                if change.operation == "delete" {
                    records.append(PushRecord(
                        id: change.recordID,
                        tableName: change.tableName,
                        operation: "delete",
                        data: nil,
                        clientUpdatedAt: clientDate,
                        baseUpdatedAt: baseDate
                    ))
                    continue
                }

                guard let schema = tableMap[change.tableName] else { continue }
                let pkCol = schema.primaryKey.first ?? "id"
                let quoted = SQLiteHelpers.quoteIdentifier(change.tableName)
                let quotedPK = SQLiteHelpers.quoteIdentifier(pkCol)

                guard let row = try Row.fetchOne(db, sql: "SELECT * FROM \(quoted) WHERE \(quotedPK) = ?", arguments: [change.recordID]) else {
                    continue
                }

                var data: [String: AnyCodable] = [:]
                for col in schema.columns {
                    let dbValue: DatabaseValue = row[col.name]
                    switch dbValue.storage {
                    case .null:
                        data[col.name] = AnyCodable(NSNull())
                    case .int64(let v):
                        if col.logicalType == "boolean" {
                            data[col.name] = AnyCodable(v != 0)
                        } else {
                            data[col.name] = AnyCodable(v)
                        }
                    case .double(let v):
                        data[col.name] = AnyCodable(v)
                    case .string(let v):
                        data[col.name] = AnyCodable(v)
                    case .blob(let v):
                        data[col.name] = AnyCodable(v.base64EncodedString())
                    }
                }

                records.append(PushRecord(
                    id: change.recordID,
                    tableName: change.tableName,
                    operation: change.operation,
                    data: data,
                    clientUpdatedAt: clientDate,
                    baseUpdatedAt: baseDate
                ))
            }
            return records
        }
    }

    func removePending(entries: [PendingChange]) throws {
        guard !entries.isEmpty else { return }
        try database.writeTransaction { db in
            for entry in entries {
                try db.execute(
                    sql: "DELETE FROM _synchro_pending_changes WHERE table_name = ? AND record_id = ?",
                    arguments: [entry.tableName, entry.recordID]
                )
            }
        }
    }

    func removePendingByIDs(entries: [(tableName: String, recordID: String)]) throws {
        guard !entries.isEmpty else { return }
        try database.writeTransaction { db in
            for entry in entries {
                try db.execute(
                    sql: "DELETE FROM _synchro_pending_changes WHERE table_name = ? AND record_id = ?",
                    arguments: [entry.tableName, entry.recordID]
                )
            }
        }
    }

    func clearTable(table: String) throws {
        try database.writeTransaction { db in
            try db.execute(
                sql: "DELETE FROM _synchro_pending_changes WHERE table_name = ?",
                arguments: [table]
            )
        }
    }

    func clearAll() throws {
        try database.writeTransaction { db in
            try db.execute(sql: "DELETE FROM _synchro_pending_changes")
        }
    }

    func hasPendingChanges() throws -> Bool {
        try pendingChangeCount() > 0
    }

    func pendingChangeCount() throws -> Int {
        try database.readTransaction { db in
            try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM _synchro_pending_changes") ?? 0
        }
    }
}
