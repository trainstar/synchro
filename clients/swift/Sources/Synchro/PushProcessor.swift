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
        let response: VNextPushResponse
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

        let request = VNextPushRequest(
            clientID: clientID,
            batchID: buildBatchID(for: mutations),
            schema: VNextSchemaRef(version: schemaVersion, hash: schemaHash),
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

    func processPush(httpClient: HttpClient, clientID: String, schemaVersion: Int64, schemaHash: String, syncedTables: [SchemaTable], batchSize: Int = 100) async throws -> PushOutcome? {
        try await processPush(
            httpClient: httpClient,
            clientID: clientID,
            schemaVersion: schemaVersion,
            schemaHash: schemaHash,
            syncedTables: syncedTables.map(\.localSchema),
            batchSize: batchSize
        )
    }

    private func buildMutations(from pushRecords: [PushRecord], syncedTables: [LocalSchemaTable]) throws -> [VNextMutation] {
        let tableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })
        return try pushRecords.compactMap { record in
            guard let schema = tableMap[record.tableName] else { return nil }
            let pkColumn = schema.primaryKey.first ?? "id"
            return VNextMutation(
                mutationID: mutationID(for: record),
                table: record.tableName,
                op: try mutationOperation(for: record.operation),
                pk: [pkColumn: AnyCodable(record.id)],
                baseVersion: record.baseUpdatedAt.map(Self.isoFormatter.string(from:)),
                columns: record.data
            )
        }
    }

    private func mutationOperation(for operation: String) throws -> VNextOperation {
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

    private func buildBatchID(for mutations: [VNextMutation]) -> String {
        guard let first = mutations.first?.mutationID, let last = mutations.last?.mutationID else {
            return "empty"
        }
        return "\(first)|\(last)"
    }

    // MARK: - Internal (visible for testing)

    func applyAccepted(accepted: [PushResult], syncedTables: [LocalSchemaTable]) throws -> [ConflictEvent] {
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

    func applyAccepted(accepted: [VNextAcceptedMutation], syncedTables: [LocalSchemaTable]) throws -> [ConflictEvent] {
        guard !accepted.isEmpty else { return [] }
        let tableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })

        try database.writeTransaction { db in
            try SynchroMeta.setSyncLock(db, locked: true)
            defer { try? SynchroMeta.setSyncLock(db, locked: false) }

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

    func applyAccepted(accepted: [VNextAcceptedMutation], syncedTables: [SchemaTable]) throws -> [ConflictEvent] {
        try applyAccepted(accepted: accepted, syncedTables: syncedTables.map(\.localSchema))
    }

    func applyAccepted(accepted: [PushResult], syncedTables: [SchemaTable]) throws -> [ConflictEvent] {
        try applyAccepted(accepted: accepted, syncedTables: syncedTables.map(\.localSchema))
    }

    private struct RejectedOutcome {
        var conflicts: [ConflictEvent]
        var hasRetryableRejections: Bool
    }

    func applyRejected(rejected: [PushResult], syncedTables: [LocalSchemaTable]) throws -> [ConflictEvent] {
        try applyRejectedOutcome(rejected: rejected, syncedTables: syncedTables).conflicts
    }

    func applyRejected(rejected: [PushResult], syncedTables: [SchemaTable]) throws -> [ConflictEvent] {
        try applyRejected(rejected: rejected, syncedTables: syncedTables.map(\.localSchema))
    }

    func applyRejected(rejected: [VNextRejectedMutation], syncedTables: [LocalSchemaTable]) throws -> [ConflictEvent] {
        try applyRejectedOutcome(rejected: rejected, syncedTables: syncedTables).conflicts
    }

    func applyRejected(rejected: [VNextRejectedMutation], syncedTables: [SchemaTable]) throws -> [ConflictEvent] {
        try applyRejected(rejected: rejected, syncedTables: syncedTables.map(\.localSchema))
    }

    private func applyRejectedOutcome(rejected: [PushResult], syncedTables: [LocalSchemaTable]) throws -> RejectedOutcome {
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

    private func applyRejectedOutcome(rejected: [VNextRejectedMutation], syncedTables: [LocalSchemaTable]) throws -> RejectedOutcome {
        guard !rejected.isEmpty else { return RejectedOutcome(conflicts: [], hasRetryableRejections: false) }
        let tableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })
        var conflicts: [ConflictEvent] = []
        var hasRetryableRejections = false

        try database.writeTransaction { db in
            try SynchroMeta.setSyncLock(db, locked: true)
            defer { try? SynchroMeta.setSyncLock(db, locked: false) }

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
