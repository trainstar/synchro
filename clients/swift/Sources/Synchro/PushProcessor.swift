import Foundation
import GRDB

final class PushProcessor: @unchecked Sendable {
    private let database: SynchroDatabase
    private let changeTracker: ChangeTracker

    init(database: SynchroDatabase, changeTracker: ChangeTracker) {
        self.database = database
        self.changeTracker = changeTracker
    }

    struct PushOutcome: Sendable {
        let response: PushResponse
        let conflicts: [ConflictEvent]
        let hasRetryableRejections: Bool
    }

    func processPush(httpClient: HttpClient, clientID: String, schemaVersion: Int64, schemaHash: String, syncedTables: [SchemaTable], batchSize: Int = 100) async throws -> PushOutcome? {
        let pending = try changeTracker.pendingChanges(limit: batchSize)
        guard !pending.isEmpty else { return nil }

        let pushRecords = try changeTracker.hydratePendingForPush(pending: pending, syncedTables: syncedTables)
        guard !pushRecords.isEmpty else {
            try changeTracker.removePending(entries: pending)
            return nil
        }

        let request = PushRequest(
            clientID: clientID,
            changes: pushRecords,
            schemaVersion: schemaVersion,
            schemaHash: schemaHash
        )

        let response = try await httpClient.push(request: request)

        let conflicts = try applyAccepted(accepted: response.accepted, syncedTables: syncedTables)
        let rejectedOutcome = try applyRejectedOutcome(rejected: response.rejected, syncedTables: syncedTables)

        return PushOutcome(
            response: response,
            conflicts: conflicts + rejectedOutcome.conflicts,
            hasRetryableRejections: rejectedOutcome.hasRetryableRejections
        )
    }

    // MARK: - Internal (visible for testing)

    func applyAccepted(accepted: [PushResult], syncedTables: [SchemaTable]) throws -> [ConflictEvent] {
        guard !accepted.isEmpty else { return [] }
        let tableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]

        try database.writeTransaction { db in
            try SynchroMeta.setSyncLock(db, locked: true)
            defer { try? SynchroMeta.setSyncLock(db, locked: false) }

            for result in accepted {
                // Remove from pending queue
                try db.execute(
                    sql: "DELETE FROM _synchro_pending_changes WHERE table_name = ? AND record_id = ?",
                    arguments: [result.tableName, result.id]
                )

                guard let schema = tableMap[result.tableName] else { continue }
                let pkCol = schema.primaryKey.first ?? "id"
                let quoted = SQLiteHelpers.quoteIdentifier(result.tableName)
                let quotedPK = SQLiteHelpers.quoteIdentifier(pkCol)

                // RYOW: apply server timestamps to local records using schema column names
                if let serverUpdatedAt = result.serverUpdatedAt {
                    let quotedUpdatedAt = SQLiteHelpers.quoteIdentifier(schema.updatedAtColumn)
                    let ts = formatter.string(from: serverUpdatedAt)
                    try db.execute(
                        sql: "UPDATE \(quoted) SET \(quotedUpdatedAt) = ? WHERE \(quotedPK) = ?",
                        arguments: [ts, result.id]
                    )
                }
                if let serverDeletedAt = result.serverDeletedAt {
                    let quotedDeletedAt = SQLiteHelpers.quoteIdentifier(schema.deletedAtColumn)
                    let ts = formatter.string(from: serverDeletedAt)
                    try db.execute(
                        sql: "UPDATE \(quoted) SET \(quotedDeletedAt) = ? WHERE \(quotedPK) = ?",
                        arguments: [ts, result.id]
                    )
                }
            }

            try SynchroMeta.setSyncLock(db, locked: false)
        }

        return []
    }

    private struct RejectedOutcome {
        var conflicts: [ConflictEvent]
        var hasRetryableRejections: Bool
    }

    func applyRejected(rejected: [PushResult], syncedTables: [SchemaTable]) throws -> [ConflictEvent] {
        try applyRejectedOutcome(rejected: rejected, syncedTables: syncedTables).conflicts
    }

    private func applyRejectedOutcome(rejected: [PushResult], syncedTables: [SchemaTable]) throws -> RejectedOutcome {
        guard !rejected.isEmpty else { return RejectedOutcome(conflicts: [], hasRetryableRejections: false) }
        let tableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })
        var conflicts: [ConflictEvent] = []
        var hasRetryableRejections = false

        try database.writeTransaction { db in
            try SynchroMeta.setSyncLock(db, locked: true)
            defer { try? SynchroMeta.setSyncLock(db, locked: false) }

            for result in rejected {
                if result.status == PushStatus.rejectedRetryable {
                    hasRetryableRejections = true
                    continue
                }

                try db.execute(
                    sql: "DELETE FROM _synchro_pending_changes WHERE table_name = ? AND record_id = ?",
                    arguments: [result.tableName, result.id]
                )

                // Apply server version if provided
                if let serverVersion = result.serverVersion, let schema = tableMap[result.tableName] {
                    let pkCol = schema.primaryKey.first ?? "id"
                    let columns = schema.columns.map(\.name)
                    let quoted = SQLiteHelpers.quoteIdentifier(result.tableName)
                    let quotedPK = SQLiteHelpers.quoteIdentifier(pkCol)

                    let dbValues = columns.map { col -> DatabaseValue in
                        if let anyCodable = serverVersion.data[col] {
                            return SQLiteHelpers.databaseValue(from: anyCodable)
                        } else if col == pkCol {
                            return serverVersion.id.databaseValue
                        } else {
                            return .null
                        }
                    }

                    let quotedColumns = columns.map { SQLiteHelpers.quoteIdentifier($0) }.joined(separator: ", ")
                    let placeholders = SQLiteHelpers.placeholders(count: columns.count)
                    let updateClauses = columns
                        .filter { $0 != pkCol }
                        .map { "\(SQLiteHelpers.quoteIdentifier($0)) = excluded.\(SQLiteHelpers.quoteIdentifier($0))" }
                        .joined(separator: ", ")

                    try db.execute(
                        sql: "INSERT INTO \(quoted) (\(quotedColumns)) VALUES (\(placeholders)) ON CONFLICT (\(quotedPK)) DO UPDATE SET \(updateClauses)",
                        arguments: StatementArguments(dbValues)
                    )
                }

                if result.status == PushStatus.conflict {
                    conflicts.append(ConflictEvent(
                        table: result.tableName,
                        recordID: result.id,
                        clientData: nil,
                        serverData: result.serverVersion?.data
                    ))
                }
            }

            try SynchroMeta.setSyncLock(db, locked: false)
        }

        return RejectedOutcome(conflicts: conflicts, hasRetryableRejections: hasRetryableRejections)
    }
}
