import Foundation
@preconcurrency import GRDB

final class PullProcessor: @unchecked Sendable {
    private let database: SynchroDatabase
    private let formatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }()

    init(database: SynchroDatabase) {
        self.database = database
    }

    func applyPullPage(changes: [Record], deletes: [DeleteEntry], syncedTables: [LocalSchemaTable]) throws {
        guard !changes.isEmpty || !deletes.isEmpty else { return }
        let tableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })

        try database.writeTransaction { db in
            try SynchroMeta.setSyncLock(db, locked: true)
            defer { try? SynchroMeta.setSyncLock(db, locked: false) }

            for record in changes {
                guard let schema = tableMap[record.tableName] else { continue }
                try upsertRecord(db: db, record: record, schema: schema)
            }
            try applyDeletesInTransaction(db: db, deletes: deletes, tableMap: tableMap)

            try SynchroMeta.setSyncLock(db, locked: false)
        }
    }

    func applyPullPage(changes: [Record], deletes: [DeleteEntry], syncedTables: [SchemaTable]) throws {
        try applyPullPage(changes: changes, deletes: deletes, syncedTables: syncedTables.map(\.localSchema))
    }

    func applyChanges(changes: [Record], syncedTables: [LocalSchemaTable]) throws {
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

    func applyChanges(changes: [Record], syncedTables: [SchemaTable]) throws {
        try applyChanges(changes: changes, syncedTables: syncedTables.map(\.localSchema))
    }

    func applyDeletes(deletes: [DeleteEntry], syncedTables: [LocalSchemaTable]) throws {
        guard !deletes.isEmpty else { return }
        let tableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })

        try database.writeTransaction { db in
            try SynchroMeta.setSyncLock(db, locked: true)
            defer { try? SynchroMeta.setSyncLock(db, locked: false) }

            try applyDeletesInTransaction(db: db, deletes: deletes, tableMap: tableMap)

            try SynchroMeta.setSyncLock(db, locked: false)
        }
    }

    func applyDeletes(deletes: [DeleteEntry], syncedTables: [SchemaTable]) throws {
        try applyDeletes(deletes: deletes, syncedTables: syncedTables.map(\.localSchema))
    }

    private func applyDeletesInTransaction(db: GRDB.Database, deletes: [DeleteEntry], tableMap: [String: LocalSchemaTable]) throws {
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
    }

    func updateCheckpoint(_ checkpoint: Int64) throws {
        try database.writeTransaction { db in
            let current = try SynchroMeta.getInt64(db, key: .checkpoint)
            if checkpoint > current {
                try SynchroMeta.setInt64(db, key: .checkpoint, value: checkpoint)
            }
        }
    }

    func applyScopeChanges(
        changes: [VNextChangeRecord],
        syncedTables: [LocalSchemaTable],
        scopeCursors: [String: String],
        checksums: [String: String]?
    ) throws {
        guard !changes.isEmpty || !scopeCursors.isEmpty else { return }
        let tableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })

        try database.writeTransaction { db in
            try SynchroMeta.setSyncLock(db, locked: true)
            defer { try? SynchroMeta.setSyncLock(db, locked: false) }

            for change in changes {
                guard let schema = tableMap[change.table] else { continue }
                let recordID = try scopeRecordID(pk: change.pk, schema: schema)

                switch change.op {
                case .delete:
                    if change.row != nil {
                        let record = try scopeRecord(from: change, schema: schema)
                        try upsertRecord(db: db, record: record, schema: schema)
                    }
                    try SynchroMeta.deleteScopeRow(
                        db,
                        scopeID: change.scope,
                        tableName: change.table,
                        recordID: recordID
                    )
                    try softDeleteIfOrphaned(
                        db: db,
                        tableName: change.table,
                        recordID: recordID,
                        schema: schema
                    )
                case .insert, .upsert, .update:
                    let record = try scopeRecord(from: change, schema: schema)
                    try upsertRecord(db: db, record: record, schema: schema)
                    let generation = try SynchroMeta.getScopeGeneration(db, scopeID: change.scope)
                    try SynchroMeta.upsertScopeRow(
                        db,
                        scopeID: change.scope,
                        tableName: change.table,
                        recordID: recordID,
                        generation: generation
                    )
                }
            }

            for (scopeID, cursor) in scopeCursors {
                try SynchroMeta.upsertScope(
                    db,
                    scopeID: scopeID,
                    cursor: cursor,
                    checksum: checksums?[scopeID]
                )
            }
        }
    }

    func beginScopeRebuild(scopeID: String) throws -> Int64 {
        try database.writeTransaction { db in
            try SynchroMeta.bumpScopeGeneration(db, scopeID: scopeID)
        }
    }

    func applyScopeRebuildPage(scopeID: String, generation: Int64, records: [VNextRebuildRecord], syncedTables: [LocalSchemaTable]) throws {
        guard !records.isEmpty else { return }
        let tableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })

        try database.writeTransaction { db in
            try SynchroMeta.setSyncLock(db, locked: true)
            defer { try? SynchroMeta.setSyncLock(db, locked: false) }

            for record in records {
                guard let schema = tableMap[record.table] else { continue }
                let recordID = try scopeRecordID(pk: record.pk, schema: schema)
                let scopedRecord = try scopeRecord(from: record, schema: schema)
                try upsertRecord(db: db, record: scopedRecord, schema: schema)
                try SynchroMeta.upsertScopeRow(
                    db,
                    scopeID: scopeID,
                    tableName: record.table,
                    recordID: recordID,
                    generation: generation
                )
            }
        }
    }

    func finalizeScopeRebuild(scopeID: String, generation: Int64, finalCursor: String, checksum: String, syncedTables: [LocalSchemaTable]) throws {
        let tableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })

        try database.writeTransaction { db in
            let staleRows = try SynchroMeta.getStaleScopeRowRecordIDs(db, scopeID: scopeID, generation: generation)
            try SynchroMeta.deleteStaleScopeRows(db, scopeID: scopeID, generation: generation)

            for staleRow in staleRows {
                guard let schema = tableMap[staleRow.tableName] else { continue }
                try softDeleteIfOrphaned(
                    db: db,
                    tableName: staleRow.tableName,
                    recordID: staleRow.recordID,
                    schema: schema
                )
            }

            try SynchroMeta.upsertScope(
                db,
                scopeID: scopeID,
                cursor: finalCursor,
                checksum: checksum,
                generation: generation
            )
        }
    }

    func removeScope(scopeID: String, syncedTables: [LocalSchemaTable]) throws {
        let tableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })

        try database.writeTransaction { db in
            let scopeRows = try SynchroMeta.getScopeRowRecordIDs(db, scopeID: scopeID)
            try SynchroMeta.deleteScopeRows(db, scopeID: scopeID)
            try SynchroMeta.deleteScope(db, scopeID: scopeID)

            for scopeRow in scopeRows {
                guard let schema = tableMap[scopeRow.tableName] else { continue }
                try softDeleteIfOrphaned(
                    db: db,
                    tableName: scopeRow.tableName,
                    recordID: scopeRow.recordID,
                    schema: schema
                )
            }
        }
    }

    func clearAllScopeState() throws {
        try database.writeTransaction { db in
            try SynchroMeta.clearAllScopes(db)
            try SynchroMeta.clearAllScopeRows(db)
        }
    }

    func updateBucketCheckpoints(_ bucketCheckpoints: [String: Int64]?) throws {
        guard let checkpoints = bucketCheckpoints, !checkpoints.isEmpty else { return }
        try database.writeTransaction { db in
            for (bucketID, checkpoint) in checkpoints {
                try SynchroMeta.setBucketCheckpoint(db, bucketID: bucketID, checkpoint: checkpoint)
            }
        }
    }

    func getBucketCheckpoints() throws -> [String: Int64] {
        try database.readTransaction { db in
            try SynchroMeta.getAllBucketCheckpoints(db)
        }
    }

    func trackBucketMembership(records: [Record], overrideBucketID: String? = nil) throws {
        let recordsToTrack: [(record: Record, bucketID: String)]
        if let override = overrideBucketID {
            recordsToTrack = records.map { ($0, override) }
        } else {
            recordsToTrack = records.compactMap { record in
                guard let bucketID = record.bucketID else { return nil }
                return (record, bucketID)
            }
        }
        guard !recordsToTrack.isEmpty else { return }

        try database.writeTransaction { db in
            for (record, bucketID) in recordsToTrack {
                // Use server-provided checksum if available, fall back to local CRC32.
                let checksum = record.checksum ?? Int32(bitPattern: Self.crc32Checksum(for: record.data))
                try SynchroMeta.upsertBucketMember(
                    db,
                    bucketID: bucketID,
                    tableName: record.tableName,
                    recordID: record.id,
                    checksum: checksum
                )
            }
        }
    }

    func clearBucketMembers(bucketID: String) throws {
        try database.writeTransaction { db in
            try SynchroMeta.deleteBucketMembers(db, bucketID: bucketID)
        }
    }

    func clearAllBucketData() throws {
        try database.writeTransaction { db in
            try SynchroMeta.deleteAllBucketMembers(db)
            try SynchroMeta.deleteAllBucketCheckpoints(db)
        }
    }

    func applyRebuildPage(records: [Record], bucketID: String, syncedTables: [LocalSchemaTable]) throws {
        guard !records.isEmpty else { return }
        let tableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })

        try database.writeTransaction { db in
            try SynchroMeta.setSyncLock(db, locked: true)
            defer { try? SynchroMeta.setSyncLock(db, locked: false) }

            for record in records {
                guard let schema = tableMap[record.tableName] else { continue }
                try upsertRecord(db: db, record: record, schema: schema)

                // Use server-provided checksum if available, fall back to local CRC32.
                let checksum = record.checksum ?? Int32(bitPattern: Self.crc32Checksum(for: record.data))
                try SynchroMeta.upsertBucketMember(
                    db,
                    bucketID: bucketID,
                    tableName: record.tableName,
                    recordID: record.id,
                    checksum: checksum
                )
            }

            try SynchroMeta.setSyncLock(db, locked: false)
        }
    }

    func applyRebuildPage(records: [Record], bucketID: String, syncedTables: [SchemaTable]) throws {
        try applyRebuildPage(records: records, bucketID: bucketID, syncedTables: syncedTables.map(\.localSchema))
    }

    func deleteBucketOrphanedRecords(bucketID: String, syncedTables: [LocalSchemaTable]) throws {
        let tableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })

        try database.writeTransaction { db in
            let members = try SynchroMeta.getBucketMemberRecordIDs(db, bucketID: bucketID)
            for member in members {
                guard let schema = tableMap[member.tableName] else { continue }
                let pkCol = schema.primaryKey.first ?? "id"
                let quoted = SQLiteHelpers.quoteIdentifier(member.tableName)
                let quotedPK = SQLiteHelpers.quoteIdentifier(pkCol)
                let quotedDeletedAt = SQLiteHelpers.quoteIdentifier(schema.deletedAtColumn)

                // Soft-delete records that were in this bucket but not refreshed during rebuild
                try db.execute(
                    sql: "UPDATE \(quoted) SET \(quotedDeletedAt) = \(SQLiteHelpers.timestampNow()) WHERE \(quotedPK) = ? AND \(quotedDeletedAt) IS NULL",
                    arguments: [member.recordID]
                )
            }
        }
    }

    func deleteBucketOrphanedRecords(bucketID: String, syncedTables: [SchemaTable]) throws {
        try deleteBucketOrphanedRecords(bucketID: bucketID, syncedTables: syncedTables.map(\.localSchema))
    }

    // MARK: - Bucket Checksum Verification

    /// Computes the aggregate checksum for a bucket by XOR-ing all stored
    /// per-record checksums from `_synchro_bucket_members`.
    func computeBucketChecksum(bucketID: String) throws -> Int32 {
        try database.readTransaction { db in
            let rows = try Row.fetchAll(
                db,
                sql: "SELECT checksum FROM _synchro_bucket_members WHERE bucket_id = ?",
                arguments: [bucketID]
            )
            var xor: Int32 = 0
            for row in rows {
                if let cs: Int64 = row["checksum"] {
                    xor ^= Int32(truncatingIfNeeded: cs)
                }
            }
            return xor
        }
    }

    // MARK: - CRC32 Checksum

    static func crc32Checksum(for data: [String: AnyCodable]) -> UInt32 {
        guard let jsonData = try? JSONEncoder.synchroEncoder().encode(data.mapValues { $0 }) else {
            return 0
        }
        return crc32(bytes: jsonData)
    }

    private static func crc32(bytes: Data) -> UInt32 {
        // CRC32 (ISO 3309 / ITU-T V.42) lookup table
        let table: [UInt32] = (0..<256).map { i -> UInt32 in
            var crc = UInt32(i)
            for _ in 0..<8 {
                if crc & 1 == 1 {
                    crc = (crc >> 1) ^ 0xEDB88320
                } else {
                    crc >>= 1
                }
            }
            return crc
        }

        var crc: UInt32 = 0xFFFFFFFF
        for byte in bytes {
            let index = Int((crc ^ UInt32(byte)) & 0xFF)
            crc = (crc >> 8) ^ table[index]
        }
        return crc ^ 0xFFFFFFFF
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

    private func upsertRecord(db: GRDB.Database, record: Record, schema: LocalSchemaTable) throws {
        let pkCol = schema.primaryKey.first ?? "id"
        let quoted = SQLiteHelpers.quoteIdentifier(record.tableName)
        let quotedPK = SQLiteHelpers.quoteIdentifier(pkCol)
        let quotedUpdatedAt = SQLiteHelpers.quoteIdentifier(schema.updatedAtColumn)
        let quotedDeletedAt = SQLiteHelpers.quoteIdentifier(schema.deletedAtColumn)

        let existingRow = try Row.fetchOne(
            db,
            sql: "SELECT \(quotedUpdatedAt), \(quotedDeletedAt) FROM \(quoted) WHERE \(quotedPK) = ?",
            arguments: [record.id]
        )
        if let existing = existingRow {
            let localUpdatedAt: String? = existing[schema.updatedAtColumn]
            let localDeletedAt: String? = existing[schema.deletedAtColumn]
            let localVersion = effectiveSyncDate(updatedAt: localUpdatedAt, deletedAt: localDeletedAt)
            let serverVersion = record.deletedAt ?? record.updatedAt
            if let localVersion, localVersion >= serverVersion {
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

    private func buildDatabaseValues(columns: [String], pkCol: String, recordID: String, data: [String: AnyCodable], schema: LocalSchemaTable) -> [DatabaseValue] {
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

    private func scopeRecord(from change: VNextChangeRecord, schema: LocalSchemaTable) throws -> Record {
        guard let row = change.row else {
            throw SynchroError.invalidResponse(message: "missing row for \(change.table) \(change.op)")
        }
        let recordID = try scopeRecordID(pk: change.pk, schema: schema)
        return try scopeRecord(tableName: change.table, recordID: recordID, row: row, schema: schema)
    }

    private func scopeRecord(from rebuild: VNextRebuildRecord, schema: LocalSchemaTable) throws -> Record {
        guard let row = rebuild.row else {
            throw SynchroError.invalidResponse(message: "missing rebuild row for \(rebuild.table)")
        }
        let recordID = try scopeRecordID(pk: rebuild.pk, schema: schema)
        return try scopeRecord(tableName: rebuild.table, recordID: recordID, row: row, schema: schema)
    }

    private func scopeRecord(tableName: String, recordID: String, row: [String: AnyCodable], schema: LocalSchemaTable) throws -> Record {
        let updatedAtRaw = row[schema.updatedAtColumn]?.value as? String
        guard let updatedAtRaw, let updatedAt = formatter.date(from: updatedAtRaw) else {
            throw SynchroError.invalidResponse(message: "missing or invalid \(schema.updatedAtColumn) for \(tableName)")
        }

        let deletedAt = (row[schema.deletedAtColumn]?.value as? String).flatMap { formatter.date(from: $0) }

        return Record(
            id: recordID,
            tableName: tableName,
            data: row,
            updatedAt: updatedAt,
            deletedAt: deletedAt,
            bucketID: nil,
            checksum: nil
        )
    }

    private func scopeRecordID(pk: [String: AnyCodable], schema: LocalSchemaTable) throws -> String {
        guard schema.primaryKey.count == 1, let primaryKey = schema.primaryKey.first else {
            throw SynchroError.invalidResponse(message: "composite primary keys are not supported for \(schema.tableName)")
        }
        guard let value = pk[primaryKey]?.value else {
            throw SynchroError.invalidResponse(message: "missing primary key \(primaryKey) for \(schema.tableName)")
        }
        return String(describing: value)
    }

    private func effectiveSyncDate(updatedAt: String?, deletedAt: String?) -> Date? {
        if let deletedAt, let deletedDate = formatter.date(from: deletedAt) {
            return deletedDate
        }
        if let updatedAt, let updatedDate = formatter.date(from: updatedAt) {
            return updatedDate
        }
        return nil
    }

    private func softDeleteIfOrphaned(db: GRDB.Database, tableName: String, recordID: String, schema: LocalSchemaTable) throws {
        guard try !SynchroMeta.hasScopeRows(db, tableName: tableName, recordID: recordID) else {
            return
        }

        let pkCol = schema.primaryKey.first ?? "id"
        let quoted = SQLiteHelpers.quoteIdentifier(tableName)
        let quotedPK = SQLiteHelpers.quoteIdentifier(pkCol)
        let quotedDeletedAt = SQLiteHelpers.quoteIdentifier(schema.deletedAtColumn)

        try db.execute(
            sql: "UPDATE \(quoted) SET \(quotedDeletedAt) = \(SQLiteHelpers.timestampNow()) WHERE \(quotedPK) = ? AND \(quotedDeletedAt) IS NULL",
            arguments: [recordID]
        )
    }
}
