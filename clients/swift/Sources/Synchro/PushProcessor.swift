import Foundation
@preconcurrency import GRDB

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

    func processPush(httpClient: HttpClient, clientID: String, schemaVersion: Int64, schemaHash: String, syncedTables: [LocalSchemaTable], batchSize: Int = 100) async throws -> PushOutcome? {
        let pending = try changeTracker.pendingChanges(limit: batchSize)
        guard !pending.isEmpty else { return nil }

        let pushRecords = try changeTracker.hydratePendingForPush(pending: pending, syncedTables: syncedTables)
        guard !pushRecords.isEmpty else {
            try changeTracker.removePending(entries: pending)
            return nil
        }

        let mutations = try buildMutations(from: pushRecords, syncedTables: syncedTables)
        guard !mutations.isEmpty else {
            try changeTracker.removePending(entries: pending)
            return nil
        }

        let request = PushRequest(
            clientID: clientID,
            batchID: buildBatchID(for: mutations),
            schema: SchemaRef(version: schemaVersion, hash: schemaHash),
            mutations: mutations
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

    private func buildMutations(from pushRecords: [PushRecord], syncedTables: [LocalSchemaTable]) throws -> [Mutation] {
        let tableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })
        return try pushRecords.compactMap { record in
            guard let schema = tableMap[record.tableName] else { return nil }
            let pkColumn = schema.primaryKey.first ?? "id"
            return Mutation(
                mutationID: mutationID(for: record),
                table: record.tableName,
                op: try mutationOperation(for: record.operation),
                pk: [pkColumn: AnyCodable(record.id)],
                baseVersion: record.baseUpdatedAt.map(Self.isoFormatter.string(from:)),
                clientVersion: Self.isoFormatter.string(from: record.clientUpdatedAt),
                columns: record.data
            )
        }
    }

    private func mutationOperation(for operation: String) throws -> Operation {
        switch operation {
        case "create":
            return .insert
        case "update":
            return .update
        case "delete":
            return .delete
        default:
            throw SynchroError.invalidResponse(message: "unknown local operation \(operation)")
        }
    }

    private func mutationID(for record: PushRecord) -> String {
        "\(record.tableName):\(record.id):\(record.operation):\(Self.isoFormatter.string(from: record.clientUpdatedAt))"
    }

    private func buildBatchID(for mutations: [Mutation]) -> String {
        guard let first = mutations.first?.mutationID, let last = mutations.last?.mutationID else {
            return "empty"
        }
        return "\(first)|\(last)"
    }

    // MARK: - Internal (visible for testing)

    func applyAccepted(accepted: [AcceptedMutation], syncedTables: [LocalSchemaTable]) throws -> [ConflictEvent] {
        guard !accepted.isEmpty else { return [] }
        let tableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })

        try database.writeSyncLockedTransaction { db in
            for mutation in accepted {
                guard let schema = tableMap[mutation.table] else { continue }
                let recordID = try recordID(from: mutation.pk, schema: schema)

                if let serverRow = mutation.serverRow {
                    try upsertServerRow(
                        db: db,
                        tableName: mutation.table,
                        schema: schema,
                        recordID: recordID,
                        row: serverRow
                    )
                }

                try db.execute(
                    sql: "DELETE FROM _synchro_pending_changes WHERE table_name = ? AND record_id = ?",
                    arguments: [mutation.table, recordID]
                )
            }
        }

        return []
    }

    private struct RejectedOutcome {
        var conflicts: [ConflictEvent]
        var hasRetryableRejections: Bool
    }

    func applyRejected(rejected: [RejectedMutation], syncedTables: [LocalSchemaTable]) throws -> [ConflictEvent] {
        try applyRejectedOutcome(rejected: rejected, syncedTables: syncedTables).conflicts
    }

    private func applyRejectedOutcome(rejected: [RejectedMutation], syncedTables: [LocalSchemaTable]) throws -> RejectedOutcome {
        guard !rejected.isEmpty else { return RejectedOutcome(conflicts: [], hasRetryableRejections: false) }
        let tableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })
        var conflicts: [ConflictEvent] = []
        var hasRetryableRejections = false

        try database.writeSyncLockedTransaction { db in
            for mutation in rejected {
                if mutation.status == .rejectedRetryable {
                    hasRetryableRejections = true
                    continue
                }

                guard let schema = tableMap[mutation.table] else { continue }
                let recordID = try recordID(from: mutation.pk, schema: schema)

                if let serverRow = mutation.serverRow {
                    try upsertServerRow(
                        db: db,
                        tableName: mutation.table,
                        schema: schema,
                        recordID: recordID,
                        row: serverRow
                    )
                }

                try db.execute(
                    sql: "DELETE FROM _synchro_pending_changes WHERE table_name = ? AND record_id = ?",
                    arguments: [mutation.table, recordID]
                )

                if mutation.status == .conflict {
                    conflicts.append(
                        ConflictEvent(
                            table: mutation.table,
                            recordID: recordID,
                            clientData: nil,
                            serverData: mutation.serverRow
                        )
                    )
                }
            }
        }

        return RejectedOutcome(conflicts: conflicts, hasRetryableRejections: hasRetryableRejections)
    }

    private func upsertServerRow(
        db: GRDB.Database,
        tableName: String,
        schema: LocalSchemaTable,
        recordID: String,
        row: [String: AnyCodable]
    ) throws {
        let pkCol = schema.primaryKey.first ?? "id"
        let columns = schema.columns.map(\.name)
        let quoted = SQLiteHelpers.quoteIdentifier(tableName)
        let quotedPK = SQLiteHelpers.quoteIdentifier(pkCol)

        let dbValues = columns.map { col -> DatabaseValue in
            if let anyCodable = row[col] {
                return SQLiteHelpers.databaseValue(from: anyCodable)
            } else if col == pkCol {
                return recordID.databaseValue
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
        let conflictAction = updateClauses.isEmpty ? "DO NOTHING" : "DO UPDATE SET \(updateClauses)"

        try db.execute(
            sql: "INSERT INTO \(quoted) (\(quotedColumns)) VALUES (\(placeholders)) ON CONFLICT (\(quotedPK)) \(conflictAction)",
            arguments: StatementArguments(dbValues)
        )
    }

    private func recordID(from pk: [String: AnyCodable], schema: LocalSchemaTable) throws -> String {
        let pkCol = schema.primaryKey.first ?? "id"
        guard let value = pk[pkCol] else {
            throw SynchroError.invalidResponse(message: "missing primary key \(pkCol) for table \(schema.tableName)")
        }
        switch value.value {
        case let string as String:
            return string
        case let int as Int:
            return String(int)
        case let int64 as Int64:
            return String(int64)
        case let double as Double:
            return String(double)
        case let bool as Bool:
            return bool ? "true" : "false"
        default:
            throw SynchroError.invalidResponse(message: "unsupported primary key value for table \(schema.tableName)")
        }
    }

    private static let isoFormatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }()
}
