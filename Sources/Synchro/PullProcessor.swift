import Foundation
import GRDB

final class PullProcessor: @unchecked Sendable {
    private let database: SynchroDatabase

    init(database: SynchroDatabase) {
        self.database = database
    }

    func applyChanges(changes: [Record], syncedTables: [SchemaTable]) throws {
        guard !changes.isEmpty else { return }
        let tableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })

        try database.writeTransaction { db in
            try SynchroMeta.setSyncLock(db, locked: true)
            defer { try? SynchroMeta.setSyncLock(db, locked: false) }

            for record in changes {
                guard let schema = tableMap[record.tableName] else { continue }
                try upsertRecord(db: db, record: record, schema: schema)
            }

            try SynchroMeta.setSyncLock(db, locked: false)
        }
    }

    func applyDeletes(deletes: [DeleteEntry], syncedTables: [SchemaTable]) throws {
        guard !deletes.isEmpty else { return }
        let tableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })

        try database.writeTransaction { db in
            try SynchroMeta.setSyncLock(db, locked: true)
            defer { try? SynchroMeta.setSyncLock(db, locked: false) }

            for entry in deletes {
                guard let schema = tableMap[entry.tableName] else { continue }
                let pkCol = schema.primaryKey.first ?? "id"
                let quoted = SQLiteHelpers.quoteIdentifier(entry.tableName)
                let quotedPK = SQLiteHelpers.quoteIdentifier(pkCol)
                let quotedDeletedAt = SQLiteHelpers.quoteIdentifier(schema.deletedAtColumn)

                try db.execute(
                    sql: "UPDATE \(quoted) SET \(quotedDeletedAt) = \(SQLiteHelpers.timestampNow()) WHERE \(quotedPK) = ? AND \(quotedDeletedAt) IS NULL",
                    arguments: [entry.id]
                )
            }

            try SynchroMeta.setSyncLock(db, locked: false)
        }
    }

    func applyResyncPage(records: [Record], syncedTables: [SchemaTable]) throws {
        guard !records.isEmpty else { return }
        let tableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })

        try database.writeTransaction { db in
            try SynchroMeta.setSyncLock(db, locked: true)
            defer { try? SynchroMeta.setSyncLock(db, locked: false) }

            for record in records {
                guard let schema = tableMap[record.tableName] else { continue }
                try insertOrReplace(db: db, record: record, schema: schema)
            }

            try SynchroMeta.setSyncLock(db, locked: false)
        }
    }

    func updateCheckpoint(_ checkpoint: Int64) throws {
        try database.writeTransaction { db in
            let current = try SynchroMeta.getInt64(db, key: .checkpoint)
            if checkpoint > current {
                try SynchroMeta.setInt64(db, key: .checkpoint, value: checkpoint)
            }
        }
    }

    func updateKnownBuckets(bucketUpdates: BucketUpdate?) throws {
        guard let updates = bucketUpdates else { return }
        try database.writeTransaction { db in
            let existing = try SynchroMeta.get(db, key: .knownBuckets) ?? "[]"
            var buckets = (try? JSONDecoder().decode([String].self, from: Data(existing.utf8))) ?? []

            if let added = updates.added {
                for b in added where !buckets.contains(b) {
                    buckets.append(b)
                }
            }
            if let removed = updates.removed {
                buckets.removeAll { removed.contains($0) }
            }

            let encoded = try JSONEncoder().encode(buckets)
            try SynchroMeta.set(db, key: .knownBuckets, value: String(data: encoded, encoding: .utf8) ?? "[]")
        }
    }

    // MARK: - Private

    private func upsertRecord(db: GRDB.Database, record: Record, schema: SchemaTable) throws {
        let pkCol = schema.primaryKey.first ?? "id"
        let quoted = SQLiteHelpers.quoteIdentifier(record.tableName)
        let quotedPK = SQLiteHelpers.quoteIdentifier(pkCol)
        let quotedUpdatedAt = SQLiteHelpers.quoteIdentifier(schema.updatedAtColumn)

        // RYOW dedup: skip if local updated_at is >= server (we already applied via push RYOW)
        let existingRow = try Row.fetchOne(db, sql: "SELECT \(quotedUpdatedAt) FROM \(quoted) WHERE \(quotedPK) = ?", arguments: [record.id])
        if let existing = existingRow {
            let localUpdatedAt: String? = existing[schema.updatedAtColumn]
            let formatter = ISO8601DateFormatter()
            formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
            if let local = localUpdatedAt, let localDate = formatter.date(from: local), localDate >= record.updatedAt {
                return
            }
        }

        let columns = schema.columns.map(\.name)
        let dbValues = buildDatabaseValues(columns: columns, pkCol: pkCol, recordID: record.id, data: record.data, schema: schema)

        let quotedColumns = columns.map { SQLiteHelpers.quoteIdentifier($0) }.joined(separator: ", ")
        let placeholders = SQLiteHelpers.placeholders(count: columns.count)
        let updateClauses = columns
            .filter { $0 != pkCol }
            .map { "\(SQLiteHelpers.quoteIdentifier($0)) = excluded.\(SQLiteHelpers.quoteIdentifier($0))" }
            .joined(separator: ", ")

        let sql = "INSERT INTO \(quoted) (\(quotedColumns)) VALUES (\(placeholders)) ON CONFLICT (\(quotedPK)) DO UPDATE SET \(updateClauses)"

        try db.execute(sql: sql, arguments: StatementArguments(dbValues))
    }

    private func insertOrReplace(db: GRDB.Database, record: Record, schema: SchemaTable) throws {
        let columns = schema.columns.map(\.name)
        let pkCol = schema.primaryKey.first ?? "id"
        let quoted = SQLiteHelpers.quoteIdentifier(record.tableName)

        let dbValues = buildDatabaseValues(columns: columns, pkCol: pkCol, recordID: record.id, data: record.data, schema: schema)

        let quotedColumns = columns.map { SQLiteHelpers.quoteIdentifier($0) }.joined(separator: ", ")
        let placeholders = SQLiteHelpers.placeholders(count: columns.count)

        try db.execute(
            sql: "INSERT OR REPLACE INTO \(quoted) (\(quotedColumns)) VALUES (\(placeholders))",
            arguments: StatementArguments(dbValues)
        )
    }

    private func buildDatabaseValues(columns: [String], pkCol: String, recordID: String, data: [String: AnyCodable], schema: SchemaTable) -> [DatabaseValue] {
        columns.map { col in
            if let anyCodable = data[col] {
                return SQLiteHelpers.databaseValue(from: anyCodable)
            } else if col == pkCol {
                return recordID.databaseValue
            } else {
                return .null
            }
        }
    }
}

