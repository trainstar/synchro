import Foundation
@preconcurrency import GRDB

struct RebuildChecksumMismatchError: Error, Sendable, Equatable {
    let scopeID: String
}

struct PendingRebuildFinality: Sendable, Equatable {
    let finalCursor: String
    let checksum: ChecksumObject
}

private struct SeedReceiptForConnect {
    let scopeID: String
    let receipt: String
    let schemaVersion: Int64?
    let schemaHash: String?
    let cardinality: Int64?
    let checksum: ChecksumObject?
}

final class PullProcessor: @unchecked Sendable {
    private static let protectionLookupChunkSize = 400

    private let database: SynchroDatabase

    init(database: SynchroDatabase) {
        self.database = database
    }

    func updateCheckpoint(_ checkpoint: Int64) throws {
        try database.writeTransaction { db in
            let current = try SynchroMeta.getInt64(db, key: .checkpoint)
            if checkpoint > current {
                try SynchroMeta.setInt64(db, key: .checkpoint, value: checkpoint)
            }
        }
    }

    /// Removes only seed receipts that no longer describe the local scope state.
    /// Invalidated scopes remain assigned and rebuild after the first connect.
    func prepareSeedReceiptsForConnect() throws {
        try database.writeSyncLockedTransaction { db in
            let receipts = try loadSeedReceiptsForConnect(db)
            guard !receipts.isEmpty else { return }

            let schema = try localSchemaReference(db)
            let tablesByName = try localTablesByName(db)
            for receipt in receipts {
                guard try !seedReceiptMatches(
                    receipt,
                    db: db,
                    schema: schema,
                    tablesByName: tablesByName
                ) else {
                    continue
                }

                try SynchroMeta.deleteSeedReceipt(db, scopeID: receipt.scopeID)
                if try SynchroMeta.getScope(db, scopeID: receipt.scopeID) != nil {
                    _ = try SynchroMeta.bumpScopeGeneration(db, scopeID: receipt.scopeID)
                }
            }
        }
    }

    func applyScopeChanges(
        changes: [ChangeRecord],
        syncedTables: [LocalSchemaTable],
        scopeCursors: [String: String],
        checksums: [String: ChecksumObject]?,
        schemaHash: String,
        scopeUpdates: ScopeAssignmentDelta = ScopeAssignmentDelta(add: [], remove: []),
        scopeSetVersion: Int64? = nil,
        rebuildScopes: Set<String> = [],
        completedPullRequestJSON: String? = nil
    ) throws {
        let checksumMap = checksums ?? [:]
        guard !changes.isEmpty || !scopeCursors.isEmpty || !checksumMap.isEmpty ||
              !scopeUpdates.add.isEmpty || !scopeUpdates.remove.isEmpty || scopeSetVersion != nil ||
              completedPullRequestJSON != nil else { return }
        if let completedPullRequestJSON {
            guard !completedPullRequestJSON.isEmpty else {
                throw SynchroError.invalidResponse(message: "completed pull request identity is invalid")
            }
            try Integrity.validateCanonicalWireJSON(Data(completedPullRequestJSON.utf8))
        }
        let tablesByID = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableID, $0) })
        let tablesByName = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })

        try database.writeSyncLockedTransaction { db in
            for change in changes {
                guard let schema = tablesByID[change.table] else {
                    throw SynchroError.invalidResponse(message: "unknown logical table \(change.table)")
                }
                let recordID = try scopeRecordID(pk: change.pk, schema: schema)

                switch change.op {
                case .insert, .update:
                    throw SynchroError.invalidResponse(message: "invalid pull operation \(change.op.rawValue)")
                case .delete:
                    try applyScopeDeleteChange(
                        db: db,
                        change: change,
                        recordID: recordID,
                        schema: schema,
                        schemaHash: schemaHash
                    )
                case .upsert:
                    let localRow = try validatedLocalRow(
                        tableID: change.table,
                        recordID: recordID,
                        pk: change.pk,
                        row: change.row,
                        rowChecksum: change.rowChecksum,
                        serverVersion: change.serverVersion,
                        schemaHash: schemaHash,
                        schema: schema
                    )
                    let protected = try Self.isProtectedApplicationRow(
                        db: db,
                        tableName: schema.tableName,
                        recordID: recordID
                    )
                    if !protected {
                        try upsertRecord(db: db, recordID: recordID, data: localRow, schema: schema)
                    }
                    guard let rowChecksum = change.rowChecksum else {
                        throw SynchroError.invalidResponse(message: "missing row checksum for \(change.table)/\(recordID)")
                    }
                    try SynchroMeta.upsertRowVersion(
                        db,
                        tableName: schema.tableName,
                        recordID: recordID,
                        serverVersion: change.serverVersion,
                        rowChecksum: rowChecksum
                    )
                    let generation = try SynchroMeta.getScopeGeneration(db, scopeID: change.scope)
                    try SynchroMeta.upsertScopeRow(
                        db,
                        scopeID: change.scope,
                        tableName: schema.tableName,
                        recordID: recordID,
                        checksum: try requiredScopeRowChecksum(
                            change.rowChecksum,
                            tableName: change.table,
                            recordID: recordID
                        ),
                        generation: generation
                    )
                }
            }

            for scopeID in scopeUpdates.remove {
                try removeScope(db: db, scopeID: scopeID, tablesByName: tablesByName)
            }
            for scope in scopeUpdates.add {
                try SynchroMeta.upsertScope(
                    db,
                    scopeID: scope.id,
                    cursor: nil,
                    checksum: nil
                )
            }

            let scopeIDs = Set(scopeCursors.keys).union(checksumMap.keys)
            for scopeID in scopeIDs {
                guard let existingScope = try SynchroMeta.getScope(db, scopeID: scopeID) else {
                    continue
                }
                let nextCursor = scopeCursors[scopeID] ?? existingScope.cursor
                let localChecksum = try computeScopeChecksum(
                    db: db,
                    scopeID: scopeID,
                    schemaHash: schemaHash,
                    tablesByName: tablesByName
                )
                if rebuildScopes.contains(scopeID) {
                    try SynchroMeta.upsertScope(
                        db,
                        scopeID: scopeID,
                        cursor: nil,
                        checksum: nil,
                        generation: existingScope.generation,
                        localChecksum: try checksumJSON(localChecksum)
                    )
                    continue
                }
                if let serverChecksum = checksumMap[scopeID] {
                    try serverChecksum.validate()
                    let localChecksumJSON = try checksumJSON(localChecksum)
                    let serverChecksumJSON = try checksumJSON(serverChecksum)
                    if localChecksum == serverChecksum {
                        try SynchroMeta.upsertScope(
                            db,
                            scopeID: scopeID,
                            cursor: nextCursor,
                            checksum: serverChecksumJSON,
                            generation: existingScope.generation,
                            localChecksum: localChecksumJSON
                        )
                    } else {
                        try SynchroMeta.upsertScope(
                            db,
                            scopeID: scopeID,
                            cursor: nil,
                            checksum: nil,
                            generation: existingScope.generation,
                            localChecksum: localChecksumJSON
                        )
                    }
                    continue
                }
                try SynchroMeta.upsertScope(
                    db,
                    scopeID: scopeID,
                    cursor: nextCursor,
                    checksum: existingScope.checksum,
                    generation: existingScope.generation,
                    localChecksum: try checksumJSON(localChecksum)
                )
            }
            if let scopeSetVersion {
                try SynchroMeta.setInt64(db, key: .scopeSetVersion, value: scopeSetVersion)
            }
            if let completedPullRequestJSON {
                try SynchroMeta.clearMatchingBackoffRecord(
                    db,
                    resumeState: .pulling,
                    workIdentity: completedPullRequestJSON
                )
            }
        }
    }

    func beginScopeRebuild(
        scopeID: String,
        clientGeneration: Int64,
        schemaVersion: Int64,
        schemaHash: String,
        pageLimit: Int,
        syncedTables: [LocalSchemaTable]
    ) throws -> LocalRebuildAttempt {
        let tablesByName = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })
        return try database.writeSyncLockedTransaction { db in
            guard try SynchroMeta.getScope(db, scopeID: scopeID) != nil else {
                throw SynchroError.invalidResponse(message: "rebuild targets an unknown scope \(scopeID)")
            }
            let scopeGeneration = try SynchroMeta.getScopeGeneration(db, scopeID: scopeID)
            if let existing = try SynchroMeta.getRebuildAttempt(db, scopeID: scopeID),
               existing.clientGeneration == clientGeneration,
               existing.schemaVersion == schemaVersion,
               existing.schemaHash == schemaHash,
               existing.pageLimit == pageLimit,
               existing.generation == scopeGeneration {
                return existing
            }
            return try startScopeRebuildAttempt(
                db: db,
                scopeID: scopeID,
                clientGeneration: clientGeneration,
                schemaVersion: schemaVersion,
                schemaHash: schemaHash,
                pageLimit: pageLimit,
                tablesByName: tablesByName
            )
        }
    }

    func restartScopeRebuild(
        scopeID: String,
        clientGeneration: Int64,
        schemaVersion: Int64,
        schemaHash: String,
        pageLimit: Int,
        syncedTables: [LocalSchemaTable]
    ) throws -> LocalRebuildAttempt {
        let tablesByName = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })
        return try database.writeSyncLockedTransaction { db in
            try startScopeRebuildAttempt(
                db: db,
                scopeID: scopeID,
                clientGeneration: clientGeneration,
                schemaVersion: schemaVersion,
                schemaHash: schemaHash,
                pageLimit: pageLimit,
                tablesByName: tablesByName
            )
        }
    }

    func applyScopeRebuildPage(
        attempt: LocalRebuildAttempt,
        request: RebuildRequest,
        requestBody: Data,
        response: RebuildResponse,
        responseBody: Data,
        syncedTables: [LocalSchemaTable]
    ) throws -> LocalRebuildAttempt {
        let tableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableID, $0) })
        try response.validate(for: request)
        if !response.hasMore {
            guard let checksum = response.checksum else {
                throw SynchroError.invalidResponse(message: "final rebuild page checksum is missing")
            }
            try checksum.validate()
        }
        let requestJSON = try rebuildRequestJSON(request, body: requestBody)
        let responseJSON = try rebuildResponseJSON(response, body: responseBody)

        return try database.writeSyncLockedTransaction { db in
            guard request.scope == attempt.scopeID,
                  request.rebuildID == attempt.rebuildID,
                  request.clientGeneration == attempt.clientGeneration,
                  request.schema == SchemaRef(version: attempt.schemaVersion, hash: attempt.schemaHash),
                  request.limit == attempt.pageLimit,
                  request.cursor == attempt.cursor else {
                throw SynchroError.invalidResponse(message: "rebuild page request does not match its attempt")
            }
            if let receipt = try SynchroMeta.getRebuildPageReceipt(
                db,
                scopeID: attempt.scopeID,
                rebuildID: attempt.rebuildID,
                requestCursor: request.cursor
            ) {
                guard receipt.requestJSON == requestJSON,
                      receipt.responseJSON == responseJSON else {
                    throw SynchroError.invalidResponse(message: "rebuild page replay differs from its receipt")
                }
                guard let currentAttempt = try SynchroMeta.getRebuildAttempt(db, scopeID: attempt.scopeID),
                      currentAttempt.scopeID == attempt.scopeID,
                      currentAttempt.rebuildID == attempt.rebuildID,
                      currentAttempt.clientGeneration == attempt.clientGeneration,
                      currentAttempt.schemaVersion == attempt.schemaVersion,
                      currentAttempt.schemaHash == attempt.schemaHash,
                      currentAttempt.generation == attempt.generation,
                      currentAttempt.pageLimit == attempt.pageLimit else {
                    throw SynchroError.invalidResponse(message: "rebuild page receipt has no active attempt")
                }
                return currentAttempt
            }

            guard let currentAttempt = try SynchroMeta.getRebuildAttempt(db, scopeID: attempt.scopeID),
                  currentAttempt == attempt else {
                throw SynchroError.invalidResponse(message: "rebuild attempt is no longer active")
            }

            var pageRecords: [(
                record: RebuildRecord,
                schema: LocalSchemaTable,
                recordID: String,
                databaseValues: [DatabaseValue],
                scopeRowChecksum: String
            )] = []
            pageRecords.reserveCapacity(response.records.count)
            for record in response.records {
                guard let schema = tableMap[record.table] else {
                    throw SynchroError.invalidResponse(message: "unknown logical table \(record.table)")
                }
                let recordID = try scopeRecordID(pk: record.pk, schema: schema)
                let localRow = try validatedLocalRow(
                    tableID: record.table,
                    recordID: recordID,
                    pk: record.pk,
                    row: record.row,
                    rowChecksum: record.rowChecksum,
                    serverVersion: record.serverVersion,
                    schemaHash: attempt.schemaHash,
                    schema: schema
                )
                let databaseValues = try buildDatabaseValues(
                    columns: schema.columns.map(\.name),
                    pkCol: schema.primaryKey.first ?? "id",
                    recordID: recordID,
                    data: localRow,
                    schema: schema
                )
                pageRecords.append((
                    record: record,
                    schema: schema,
                    recordID: recordID,
                    databaseValues: databaseValues,
                    scopeRowChecksum: try requiredScopeRowChecksum(
                        record.rowChecksum,
                        tableName: record.table,
                        recordID: recordID
                    )
                ))
            }

            var protectedRecordIDsByTable: [String: Set<String>] = [:]
            var requestedRecordIDsByTable: [String: Set<String>] = [:]
            var protectionKeys: [(tableName: String, recordID: String)] = []
            protectionKeys.reserveCapacity(pageRecords.count)
            for pageRecord in pageRecords {
                let tableName = pageRecord.schema.tableName
                if requestedRecordIDsByTable[tableName, default: []].insert(pageRecord.recordID).inserted {
                    protectionKeys.append((tableName: tableName, recordID: pageRecord.recordID))
                }
            }
            for start in stride(from: 0, to: protectionKeys.count, by: Self.protectionLookupChunkSize) {
                let end = min(start + Self.protectionLookupChunkSize, protectionKeys.count)
                let chunk = protectionKeys[start..<end]
                let values = Array(repeating: "(?, ?)", count: chunk.count).joined(separator: ", ")
                var arguments: [DatabaseValue] = []
                arguments.reserveCapacity(chunk.count * 2)
                for key in chunk {
                    arguments.append(key.tableName.databaseValue)
                    arguments.append(key.recordID.databaseValue)
                }
                let protectedRows = try Row.fetchAll(
                    db,
                    sql: """
                        WITH requested(table_name, record_id) AS (VALUES \(values))
                        SELECT pending.table_name, pending.record_id
                        FROM _synchro_pending_changes AS pending
                        JOIN requested
                          ON requested.table_name = pending.table_name
                         AND requested.record_id = pending.record_id
                        WHERE pending.lifecycle_state IN ('unsealed', 'sealed', 'blocked_by_predecessor', 'legacy_blocked')
                        UNION
                        SELECT rejected.table_name, rejected.record_id
                        FROM _synchro_rejected_mutations AS rejected
                        JOIN requested
                          ON requested.table_name = rejected.table_name
                         AND requested.record_id = rejected.record_id
                        WHERE rejected.status = 'rejected_terminal'
                          AND rejected.server_row_json IS NULL
                          AND rejected.server_version IS NULL
                        """,
                    arguments: StatementArguments(arguments)
                )
                for row in protectedRows {
                    let tableName: String = row["table_name"]
                    let recordID: String = row["record_id"]
                    protectedRecordIDsByTable[tableName, default: []].insert(recordID)
                }
            }

            var upsertStatements: [String: Statement] = [:]
            for pageRecord in pageRecords {
                let protected = protectedRecordIDsByTable[pageRecord.schema.tableName]?.contains(pageRecord.recordID) == true
                if !protected {
                    let statement: Statement
                    if let existing = upsertStatements[pageRecord.schema.tableName] {
                        statement = existing
                    } else {
                        let prepared = try db.makeStatement(sql: upsertSQL(schema: pageRecord.schema))
                        upsertStatements[pageRecord.schema.tableName] = prepared
                        statement = prepared
                    }
                    try statement.execute(arguments: StatementArguments(pageRecord.databaseValues))
                }
                try SynchroMeta.upsertRowVersion(
                    db,
                    tableName: pageRecord.schema.tableName,
                    recordID: pageRecord.recordID,
                    serverVersion: pageRecord.record.serverVersion,
                    rowChecksum: pageRecord.record.rowChecksum
                )
                try SynchroMeta.upsertScopeRow(
                    db,
                    scopeID: attempt.scopeID,
                    tableName: pageRecord.schema.tableName,
                    recordID: pageRecord.recordID,
                    checksum: pageRecord.scopeRowChecksum,
                    generation: attempt.generation
                )
            }
            let finalChecksumJSON = try response.checksum.map(checksumJSON)
            try SynchroMeta.insertRebuildPageReceipt(
                db,
                scopeID: attempt.scopeID,
                rebuildID: attempt.rebuildID,
                requestCursor: request.cursor,
                requestJSON: requestJSON,
                responseJSON: responseJSON,
                finalScopeCursor: response.finalScopeCursor,
                finalChecksumJSON: finalChecksumJSON
            )
            try SynchroMeta.clearMatchingBackoffRecord(
                db,
                resumeState: .rebuilding,
                workIdentity: requestJSON
            )

            guard response.hasMore else { return attempt }
            guard let nextCursor = response.cursor else {
                throw SynchroError.invalidResponse(message: "intermediate rebuild page cursor is missing")
            }
            let nextAttempt = LocalRebuildAttempt(
                scopeID: attempt.scopeID,
                rebuildID: attempt.rebuildID,
                clientGeneration: attempt.clientGeneration,
                schemaVersion: attempt.schemaVersion,
                schemaHash: attempt.schemaHash,
                generation: attempt.generation,
                cursor: nextCursor,
                pageLimit: attempt.pageLimit
            )
            try SynchroMeta.upsertRebuildAttempt(db, attempt: nextAttempt)
            return nextAttempt
        }
    }

    func pendingRebuildFinality(
        attempt: LocalRebuildAttempt,
        request: RebuildRequest,
        requestBody: Data
    ) throws -> PendingRebuildFinality? {
        guard request.scope == attempt.scopeID,
              request.rebuildID == attempt.rebuildID,
              request.clientGeneration == attempt.clientGeneration,
              request.schema == SchemaRef(version: attempt.schemaVersion, hash: attempt.schemaHash),
              request.limit == attempt.pageLimit,
              request.cursor == attempt.cursor else {
            throw SynchroError.invalidResponse(message: "rebuild finality request does not match its attempt")
        }
        let requestJSON = try rebuildRequestJSON(request, body: requestBody)
        return try database.readTransaction { db -> PendingRebuildFinality? in
            guard let currentAttempt = try SynchroMeta.getRebuildAttempt(db, scopeID: attempt.scopeID),
                  currentAttempt == attempt,
                  let receipt = try SynchroMeta.getFinalRebuildPageReceipt(
                    db,
                    scopeID: attempt.scopeID,
                    rebuildID: attempt.rebuildID
                  ) else {
                return nil
            }
            guard receipt.requestCursor == attempt.cursor,
                  receipt.requestJSON == requestJSON,
                  let finalCursor = receipt.finalScopeCursor,
                  let finalChecksumJSON = receipt.finalChecksumJSON else {
                throw SynchroError.invalidResponse(message: "final rebuild receipt does not match its request")
            }
            let responseBody = Data(receipt.responseJSON.utf8)
            let response = try JSONDecoder.synchroDecoder().decode(RebuildResponse.self, from: responseBody)
            _ = try rebuildResponseJSON(response, body: responseBody)
            try response.validate(for: request)
            guard response.hasMore == false,
                  response.finalScopeCursor == finalCursor,
                  let responseChecksum = response.checksum else {
                throw SynchroError.invalidResponse(message: "final rebuild receipt does not match its attempt")
            }
            let checksum = try JSONDecoder.synchroDecoder().decode(
                ChecksumObject.self,
                from: Data(finalChecksumJSON.utf8)
            )
            try checksum.validate()
            guard checksum == responseChecksum else {
                throw SynchroError.invalidResponse(message: "final rebuild receipt checksum differs from its content")
            }
            return PendingRebuildFinality(finalCursor: finalCursor, checksum: checksum)
        }
    }

    func finalizeScopeRebuild(
        attempt: LocalRebuildAttempt,
        finalCursor: String,
        checksum: ChecksumObject,
        syncedTables: [LocalSchemaTable]
    ) throws {
        let tableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })

        try database.writeSyncLockedTransaction { db in
            let expectedChecksumJSON = try checksumJSON(checksum)
            if let currentAttempt = try SynchroMeta.getRebuildAttempt(db, scopeID: attempt.scopeID) {
                guard currentAttempt == attempt,
                      try SynchroMeta.getScopeGeneration(db, scopeID: attempt.scopeID) == attempt.generation else {
                    throw SynchroError.invalidResponse(message: "rebuild finality targets an inactive attempt")
                }
                if let receipt = try SynchroMeta.getFinalRebuildPageReceipt(
                    db,
                    scopeID: attempt.scopeID,
                    rebuildID: attempt.rebuildID
                ) {
                    guard receipt.requestCursor == attempt.cursor,
                          receipt.finalScopeCursor == finalCursor,
                          let finalChecksumJSON = receipt.finalChecksumJSON,
                          finalChecksumJSON == expectedChecksumJSON else {
                        throw SynchroError.invalidResponse(message: "rebuild finality differs from its page receipt")
                    }
                }
            }
            let staleRows = try SynchroMeta.getStaleScopeRowRecordIDs(
                db,
                scopeID: attempt.scopeID,
                generation: attempt.generation
            )
            try SynchroMeta.deleteStaleScopeRows(db, scopeID: attempt.scopeID, generation: attempt.generation)

            for staleRow in staleRows {
                guard let schema = tableMap[staleRow.tableName] else { continue }
                try removeLocalRowIfUnreferenced(
                    db: db,
                    tableName: staleRow.tableName,
                    recordID: staleRow.recordID,
                    schema: schema
                )
            }

            let localChecksum = try computeScopeChecksum(
                db: db,
                scopeID: attempt.scopeID,
                schemaHash: attempt.schemaHash,
                tablesByName: tableMap
            )
            try checksum.validate()
            guard localChecksum == checksum else {
                throw RebuildChecksumMismatchError(scopeID: attempt.scopeID)
            }

            try SynchroMeta.upsertScope(
                db,
                scopeID: attempt.scopeID,
                cursor: finalCursor,
                checksum: try checksumJSON(checksum),
                generation: attempt.generation,
                localChecksum: try checksumJSON(localChecksum)
            )
            try SynchroMeta.deleteRebuildAttempt(db, scopeID: attempt.scopeID)
        }
    }

    func removeScope(scopeID: String, syncedTables: [LocalSchemaTable]) throws {
        let tableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })

        try database.writeSyncLockedTransaction { db in
            try removeScope(db: db, scopeID: scopeID, tablesByName: tableMap)
        }
    }

    func installConnectedAssignment(
        _ delta: ScopeAssignmentDelta,
        scopeSetVersion: Int64,
        clientGeneration: Int64,
        syncedTables: [LocalSchemaTable],
        scopeCursorUpdates: [String: String?] = [:]
    ) throws {
        try database.writeSyncLockedTransaction { db in
            try installConnectedAssignmentInTransaction(
                db,
                delta: delta,
                scopeSetVersion: scopeSetVersion,
                clientGeneration: clientGeneration,
                syncedTables: syncedTables,
                scopeCursorUpdates: scopeCursorUpdates
            )
        }
    }

    func installConnectedAssignmentInTransaction(
        _ db: GRDB.Database,
        delta: ScopeAssignmentDelta,
        scopeSetVersion: Int64,
        clientGeneration: Int64,
        syncedTables: [LocalSchemaTable],
        scopeCursorUpdates: [String: String?]
    ) throws {
        let tableMap = Dictionary(uniqueKeysWithValues: syncedTables.map { ($0.tableName, $0) })
        try validateSeedReceiptDispositions(
            db,
            delta: delta,
            scopeCursorUpdates: scopeCursorUpdates
        )
        for scopeID in delta.remove {
            try removeScope(db: db, scopeID: scopeID, tablesByName: tableMap)
        }
        for scope in delta.add {
            try SynchroMeta.upsertScope(
                db,
                scopeID: scope.id,
                cursor: scope.cursor,
                checksum: nil
            )
        }
        try SynchroMeta.setInt64(db, key: .scopeSetVersion, value: scopeSetVersion)
        try SynchroMeta.setInt64(db, key: .clientGeneration, value: clientGeneration)
        try SynchroMeta.clearSeedReceipts(db)
    }

    func clearAllScopeState() throws {
        try database.writeTransaction { db in
            try SynchroMeta.clearAllScopes(db)
            try SynchroMeta.clearAllScopeRows(db)
        }
    }

    private func requiredScopeRowChecksum(
        _ checksum: ChecksumObject?,
        tableName: String,
        recordID: String
    ) throws -> String {
        guard let checksum else {
            throw SynchroError.invalidResponse(
                message: "missing scope row checksum for \(tableName)/\(recordID)"
            )
        }
        try checksum.validate()
        return checksum.digest
    }

    private func loadSeedReceiptsForConnect(_ db: GRDB.Database) throws -> [SeedReceiptForConnect] {
        let rows = try Row.fetchAll(
            db,
            sql: "SELECT scope_id, receipt, schema_version, schema_hash, cardinality, checksum FROM _synchro_seed_receipts ORDER BY scope_id"
        )
        var scopeIDs = Set<String>()
        return try rows.map { row in
            guard let scopeID: String = row["scope_id"], scopeIDs.insert(scopeID).inserted else {
                throw SynchroError.invalidResponse(message: "seed receipt scope is malformed")
            }
            let receipt: String = row["receipt"] ?? ""
            let schemaVersion: Int64? = row["schema_version"]
            let schemaHash: String? = row["schema_hash"]
            let cardinality: Int64? = row["cardinality"]
            let checksum: ChecksumObject? = checksumObject(row["checksum"])
            return SeedReceiptForConnect(
                scopeID: scopeID,
                receipt: receipt,
                schemaVersion: schemaVersion,
                schemaHash: schemaHash,
                cardinality: cardinality,
                checksum: checksum
            )
        }
    }

    private func localSchemaReference(_ db: GRDB.Database) throws -> SchemaRef {
        let schema = SchemaRef(
            version: try SynchroMeta.getInt64(db, key: .schemaVersion),
            hash: try SynchroMeta.get(db, key: .schemaHash) ?? ""
        )
        try schema.validate()
        return schema
    }

    private func localTablesByName(_ db: GRDB.Database) throws -> [String: LocalSchemaTable] {
        guard let encoded = try SynchroMeta.get(db, key: .localSchema) else {
            throw SynchroError.invalidResponse(message: "seed receipt validation has no local schema")
        }
        let tables = try JSONDecoder.synchroDecoder().decode(
            [LocalSchemaTable].self,
            from: Data(encoded.utf8)
        )
        var tablesByName: [String: LocalSchemaTable] = [:]
        for table in tables {
            guard !table.tableName.isEmpty,
                  tablesByName.updateValue(table, forKey: table.tableName) == nil else {
                throw SynchroError.invalidResponse(message: "seed receipt validation has an invalid local schema")
            }
        }
        return tablesByName
    }

    private func seedReceiptMatches(
        _ receipt: SeedReceiptForConnect,
        db: GRDB.Database,
        schema: SchemaRef,
        tablesByName: [String: LocalSchemaTable]
    ) throws -> Bool {
        guard !receipt.scopeID.isEmpty,
              !receipt.receipt.isEmpty,
              receipt.schemaVersion == schema.version,
              receipt.schemaHash == schema.hash,
              let cardinality = receipt.cardinality,
              cardinality >= 0,
              let expectedChecksum = receipt.checksum,
              let scope = try SynchroMeta.getScope(db, scopeID: receipt.scopeID),
              scope.cursor == nil,
              scope.generation == 0,
              checksumObject(scope.checksum) == expectedChecksum,
              checksumObject(scope.localChecksum) == expectedChecksum else {
            return false
        }

        guard let scopeRowCount = try Int64.fetchOne(
            db,
            sql: "SELECT COUNT(*) FROM _synchro_scope_rows WHERE scope_id = ?",
            arguments: [receipt.scopeID]
        ), scopeRowCount == cardinality else {
            return false
        }
        let scopeRows = try SynchroMeta.getScopeRowChecksums(db, scopeID: receipt.scopeID)
        guard Int64(scopeRows.count) == scopeRowCount,
              scopeRows.allSatisfy({ $0.generation == 0 }),
              try scopeRowsMatchStoredVersions(scopeRows, db: db) else {
            return false
        }

        do {
            return try Self.recomputeScopeChecksum(
                db: db,
                scopeID: receipt.scopeID,
                schemaHash: schema.hash,
                tablesByName: tablesByName
            ) == expectedChecksum
        } catch is SynchroError {
            return false
        } catch is IntegrityError {
            return false
        } catch is ContractViolation {
            return false
        }
    }

    private func scopeRowsMatchStoredVersions(
        _ scopeRows: [(tableName: String, recordID: String, checksum: String, generation: Int64)],
        db: GRDB.Database
    ) throws -> Bool {
        for scopeRow in scopeRows {
            guard let expectedChecksum = scopeRowChecksum(scopeRow.checksum),
                  let versionRow = try Row.fetchOne(
                      db,
                      sql: "SELECT server_version, row_checksum FROM _synchro_row_versions WHERE table_name = ? AND record_id = ?",
                      arguments: [scopeRow.tableName, scopeRow.recordID]
                  ),
                  let serverVersion: String = versionRow["server_version"],
                  !serverVersion.isEmpty,
                  let rowChecksumJSON: String = versionRow["row_checksum"],
                  checksumObject(rowChecksumJSON) == expectedChecksum else {
                return false
            }
        }
        return true
    }

    private func checksumObject(_ source: String?) -> ChecksumObject? {
        guard let source else { return nil }
        do {
            let data = Data(source.utf8)
            try Integrity.validateCanonicalWireJSON(data)
            let checksum = try JSONDecoder.synchroDecoder().decode(ChecksumObject.self, from: data)
            try checksum.validate()
            return checksum
        } catch {
            return nil
        }
    }

    private func scopeRowChecksum(_ digest: String) -> ChecksumObject? {
        do {
            let checksum = ChecksumObject(
                algorithm: "sha256",
                version: 1,
                encoding: "hex",
                digest: digest
            )
            try checksum.validate()
            return checksum
        } catch {
            return nil
        }
    }

    private func validateSeedReceiptDispositions(
        _ db: GRDB.Database,
        delta: ScopeAssignmentDelta,
        scopeCursorUpdates: [String: String?]
    ) throws {
        let receiptScopeIDs = try Set(SynchroMeta.getSeedReceipts(db).keys)
        for scopeID in receiptScopeIDs {
            if delta.remove.contains(scopeID) {
                continue
            }
            guard scopeCursorUpdates.keys.contains(scopeID) else {
                throw SynchroError.invalidResponse(
                    message: "seed receipt scope has no cursor disposition \(scopeID)"
                )
            }
        }
    }

    private func startScopeRebuildAttempt(
        db: GRDB.Database,
        scopeID: String,
        clientGeneration: Int64,
        schemaVersion: Int64,
        schemaHash: String,
        pageLimit: Int,
        tablesByName: [String: LocalSchemaTable]
    ) throws -> LocalRebuildAttempt {
        guard pageLimit > 0 else {
            throw SynchroError.invalidResponse(message: "rebuild page limit is invalid")
        }
        guard try SynchroMeta.getScope(db, scopeID: scopeID) != nil else {
            throw SynchroError.invalidResponse(message: "rebuild targets an unknown scope \(scopeID)")
        }
        try SynchroMeta.deleteRebuildPageReceipts(db, scopeID: scopeID)
        try SynchroMeta.deleteRebuildAttempt(db, scopeID: scopeID)
        try resetScopeProvenanceForRebuild(
            db: db,
            scopeID: scopeID,
            tablesByName: tablesByName
        )
        let generation = try SynchroMeta.bumpScopeGeneration(db, scopeID: scopeID)
        let attempt = LocalRebuildAttempt(
            scopeID: scopeID,
            rebuildID: UUID().uuidString.lowercased(),
            clientGeneration: clientGeneration,
            schemaVersion: schemaVersion,
            schemaHash: schemaHash,
            generation: generation,
            cursor: nil,
            pageLimit: pageLimit
        )
        try SynchroMeta.upsertRebuildAttempt(db, attempt: attempt)
        return attempt
    }

    private func resetScopeProvenanceForRebuild(
        db: GRDB.Database,
        scopeID: String,
        tablesByName: [String: LocalSchemaTable]
    ) throws {
        let scopeRows = try SynchroMeta.getScopeRowRecordIDs(db, scopeID: scopeID)
        try SynchroMeta.deleteScopeRows(db, scopeID: scopeID)
        for scopeRow in scopeRows {
            guard let schema = tablesByName[scopeRow.tableName] else { continue }
            try removeLocalRowIfUnreferenced(
                db: db,
                tableName: scopeRow.tableName,
                recordID: scopeRow.recordID,
                schema: schema
            )
        }
    }

    private func rebuildRequestJSON(_ request: RebuildRequest, body: Data) throws -> String {
        try rebuildWireJSON(body, expected: request, name: "request")
    }

    private func rebuildResponseJSON(_ response: RebuildResponse, body: Data) throws -> String {
        try rebuildWireJSON(body, expected: response, name: "response")
    }

    private func rebuildWireJSON<T: Decodable & Equatable>(
        _ body: Data,
        expected: T,
        name: String
    ) throws -> String {
        do {
            try Integrity.validateCanonicalWireJSON(body)
            let decoded = try JSONDecoder.synchroDecoder().decode(T.self, from: body)
            guard decoded == expected,
                  let json = String(data: body, encoding: .utf8) else {
                throw SynchroError.invalidResponse(message: "rebuild \(name) body differs from its decoded value")
            }
            return json
        } catch let error as SynchroError {
            throw error
        } catch {
            throw SynchroError.invalidResponse(message: "rebuild \(name) body is invalid")
        }
    }

    private func removeScope(
        db: GRDB.Database,
        scopeID: String,
        tablesByName: [String: LocalSchemaTable]
    ) throws {
        try SynchroMeta.deleteRebuildPageReceipts(db, scopeID: scopeID)
        try SynchroMeta.deleteRebuildAttempt(db, scopeID: scopeID)
        try SynchroMeta.clearRebuildingBackoffForScope(db, scopeID: scopeID)
        let scopeRows = try SynchroMeta.getScopeRowRecordIDs(db, scopeID: scopeID)
        try SynchroMeta.deleteScopeRows(db, scopeID: scopeID)
        try SynchroMeta.deleteScope(db, scopeID: scopeID)

        for scopeRow in scopeRows {
            guard let schema = tablesByName[scopeRow.tableName] else { continue }
            try removeLocalRowIfUnreferenced(
                db: db,
                tableName: scopeRow.tableName,
                recordID: scopeRow.recordID,
                schema: schema
            )
        }
    }

    // MARK: - Private

    private func upsertRecord(
        db: GRDB.Database,
        recordID: String,
        data: [String: AnyCodable],
        schema: LocalSchemaTable
    ) throws {
        let columns = schema.columns.map(\.name)
        let dbValues = try buildDatabaseValues(
            columns: columns,
            pkCol: schema.primaryKey.first ?? "id",
            recordID: recordID,
            data: data,
            schema: schema
        )
        try db.execute(sql: upsertSQL(schema: schema), arguments: StatementArguments(dbValues))
    }

    private func upsertSQL(schema: LocalSchemaTable) -> String {
        let pkCol = schema.primaryKey.first ?? "id"
        let quoted = SQLiteHelpers.quoteIdentifier(schema.tableName)
        let quotedPK = SQLiteHelpers.quoteIdentifier(pkCol)
        let columns = schema.columns.map(\.name)
        let quotedColumns = columns.map { SQLiteHelpers.quoteIdentifier($0) }.joined(separator: ", ")
        let placeholders = SQLiteHelpers.placeholders(count: columns.count)
        let updateClauses = columns
            .filter { $0 != pkCol }
            .map { "\(SQLiteHelpers.quoteIdentifier($0)) = excluded.\(SQLiteHelpers.quoteIdentifier($0))" }
            .joined(separator: ", ")

        return "INSERT INTO \(quoted) (\(quotedColumns)) VALUES (\(placeholders)) ON CONFLICT (\(quotedPK)) DO UPDATE SET \(updateClauses)"
    }

    private func buildDatabaseValues(
        columns: [String],
        pkCol: String,
        recordID: String,
        data: [String: AnyCodable],
        schema: LocalSchemaTable
    ) throws -> [DatabaseValue] {
        let columnsByName = Dictionary(uniqueKeysWithValues: schema.columns.map { ($0.name, $0) })
        return try columns.map { col in
            if let anyCodable = data[col], let column = columnsByName[col] {
                return try SQLiteHelpers.databaseValue(from: anyCodable, column: column)
            } else if col == pkCol {
                return recordID.databaseValue
            } else {
                return .null
            }
        }
    }

    private static func canonicalInteger(_ value: String) -> Bool {
        if value == "0" { return true }
        let bytes = Array(value.utf8)
        let start = bytes.first == 45 ? 1 : 0
        guard start < bytes.count, bytes[start] != 48 else { return false }
        return bytes[start...].allSatisfy { $0 >= 48 && $0 <= 57 }
    }

    private func validatedLocalRow(
        tableID: String,
        recordID: String,
        pk: [String: AnyCodable],
        row: [String: AnyCodable]?,
        rowChecksum: ChecksumObject?,
        serverVersion: String,
        schemaHash: String,
        schema: LocalSchemaTable
    ) throws -> [String: AnyCodable] {
        guard let row else {
            throw SynchroError.invalidResponse(message: "missing row for \(tableID)")
        }
        guard let rowChecksum else {
            throw SynchroError.invalidResponse(message: "missing row checksum for \(tableID)/\(recordID)")
        }
        let computed = try Integrity.rowDigest(
            schemaHash: schemaHash,
            table: schema,
            pk: pk,
            row: row,
            serverVersion: serverVersion
        ).checksum
        guard computed == rowChecksum else {
            throw SynchroError.invalidResponse(message: "row checksum mismatch for \(tableID)/\(recordID)")
        }

        let fieldsByID = Dictionary(uniqueKeysWithValues: schema.columns.map { ($0.fieldID, $0.name) })
        return Dictionary(uniqueKeysWithValues: row.compactMap { fieldID, value in
            fieldsByID[fieldID].map { ($0, value) }
        })
    }

    private func scopeRecordID(pk: [String: AnyCodable], schema: LocalSchemaTable) throws -> String {
        guard let value = pk[schema.primaryKeyFieldID]?.value else {
            throw SynchroError.invalidResponse(
                message: "missing primary key \(schema.primaryKeyFieldID) for \(schema.tableName)"
            )
        }
        return String(describing: value)
    }

    private func applyScopeDeleteChange(
        db: GRDB.Database,
        change: ChangeRecord,
        recordID: String,
        schema: LocalSchemaTable,
        schemaHash: String
    ) throws {
        let protected = try Self.isProtectedApplicationRow(
            db: db,
            tableName: schema.tableName,
            recordID: recordID
        )
        if let row = change.row {
            guard let deletedAtFieldID = schema.deletedAtFieldID,
                  row[deletedAtFieldID]?.value as? String != nil else {
                throw SynchroError.invalidResponse(
                    message: "delete change for \(change.table) \(recordID) included a row without \(schema.deletedAtColumn)"
                )
            }
            let localRow = try validatedLocalRow(
                tableID: change.table,
                recordID: recordID,
                pk: change.pk,
                row: row,
                rowChecksum: change.rowChecksum,
                serverVersion: change.serverVersion,
                schemaHash: schemaHash,
                schema: schema
            )
            if !protected {
                try upsertRecord(db: db, recordID: recordID, data: localRow, schema: schema)
            }
            try SynchroMeta.upsertRowVersion(
                db,
                tableName: schema.tableName,
                recordID: recordID,
                serverVersion: change.serverVersion,
                rowChecksum: change.rowChecksum
            )
        } else {
            try SynchroMeta.upsertRowVersion(
                db,
                tableName: schema.tableName,
                recordID: recordID,
                serverVersion: change.serverVersion,
                rowChecksum: nil
            )
        }

        try SynchroMeta.deleteScopeRow(
            db,
            scopeID: change.scope,
            tableName: schema.tableName,
            recordID: recordID
        )

        if change.row == nil {
            try removeLocalRowIfUnreferenced(
                db: db,
                tableName: schema.tableName,
                recordID: recordID,
                schema: schema
            )
        }
    }

    private func computeScopeChecksum(
        db: GRDB.Database,
        scopeID: String,
        schemaHash: String,
        tablesByName: [String: LocalSchemaTable]
    ) throws -> ChecksumObject {
        try Self.recomputeScopeChecksum(
            db: db,
            scopeID: scopeID,
            schemaHash: schemaHash,
            tablesByName: tablesByName
        )
    }

    static func recomputeScopeChecksum(
        db: GRDB.Database,
        scopeID: String,
        schemaHash: String,
        tablesByName: [String: LocalSchemaTable]
    ) throws -> ChecksumObject {
        let rows = try SynchroMeta.getScopeRowChecksums(db, scopeID: scopeID)
        var entries: [(identity: Data, digest: ChecksumObject)] = []
        entries.reserveCapacity(rows.count)
        for scopeRow in rows {
            guard let table = tablesByName[scopeRow.tableName] else {
                throw SynchroError.invalidResponse(message: "scope references unknown table \(scopeRow.tableName)")
            }
            let protected = try Self.isProtectedApplicationRow(
                db: db,
                tableName: table.tableName,
                recordID: scopeRow.recordID
            )
            if protected {
                let pk = try Self.primaryKeyValue(recordID: scopeRow.recordID, schema: table)
                let identity = try Integrity.rowIdentity(table: table, pk: pk)
                let digest = ChecksumObject(
                    algorithm: "sha256",
                    version: 1,
                    encoding: "hex",
                    digest: scopeRow.checksum
                )
                try digest.validate()
                entries.append((identity: identity, digest: digest))
                continue
            }

            let localRow = try Self.loadWireRow(db: db, table: table, recordID: scopeRow.recordID)
            let pk = [table.primaryKeyFieldID: localRow[table.primaryKeyFieldID]!]
            guard let version = try SynchroMeta.getRowVersion(db, tableName: table.tableName, recordID: scopeRow.recordID) else {
                throw SynchroError.invalidResponse(message: "scope row has no server version")
            }
            let computed = try Integrity.rowDigest(
                schemaHash: schemaHash,
                table: table,
                pk: pk,
                row: localRow,
                serverVersion: version
            )
            guard computed.checksum.digest == scopeRow.checksum else {
                throw SynchroError.invalidResponse(message: "scope row checksum does not match local row")
            }
            entries.append((identity: computed.identity, digest: computed.checksum))
        }
        return try Integrity.scopeDigest(schemaHash: schemaHash, scopeID: scopeID, entries: entries)
    }

    private static func primaryKeyValue(recordID: String, schema: LocalSchemaTable) throws -> [String: AnyCodable] {
        guard let primaryKey = schema.columns.first(where: { $0.fieldID == schema.primaryKeyFieldID }) else {
            throw SynchroError.invalidResponse(message: "missing primary key metadata for \(schema.tableName)")
        }
        let value: AnyCodable
        switch primaryKey.logicalType {
        case "string":
            value = AnyCodable(recordID)
        case "int":
            guard let integer = Int64(recordID), integer >= Int64(Int32.min), integer <= Int64(Int32.max) else {
                throw SynchroError.invalidResponse(message: "invalid primary key for \(schema.tableName)")
            }
            value = AnyCodable(integer)
        case "int64":
            guard Self.canonicalInteger(recordID), Int64(recordID) != nil else {
                throw SynchroError.invalidResponse(message: "invalid primary key for \(schema.tableName)")
            }
            value = AnyCodable(recordID)
        default:
            throw SynchroError.invalidResponse(message: "unsupported primary key type for \(schema.tableName)")
        }
        return [schema.primaryKeyFieldID: value]
    }

    private static func loadWireRow(
        db: GRDB.Database,
        table: LocalSchemaTable,
        recordID: String
    ) throws -> [String: AnyCodable] {
        let columns = table.columns.map { SQLiteHelpers.quoteIdentifier($0.name) }.joined(separator: ", ")
        let primaryKey = SQLiteHelpers.quoteIdentifier(table.primaryKey.first ?? "id")
        let relation = SQLiteHelpers.quoteIdentifier(table.tableName)
        guard let row = try Row.fetchOne(
            db,
            sql: "SELECT \(columns) FROM \(relation) WHERE \(primaryKey) = ?",
            arguments: [recordID]
        ) else {
            throw SynchroError.invalidResponse(message: "scope provenance references a missing row")
        }
        return try Dictionary(uniqueKeysWithValues: table.columns.map { column in
            let value: DatabaseValue = row[column.name]
            return (column.fieldID, try Self.wireValue(value, column: column))
        })
    }

    private static func wireValue(_ value: DatabaseValue, column: LocalSchemaColumn) throws -> AnyCodable {
        switch value.storage {
        case .null:
            return AnyCodable(NSNull())
        case .int64(let value):
            switch column.logicalType {
            case "boolean": return AnyCodable(value != 0)
            case "int64": return AnyCodable(String(value))
            default: return AnyCodable(value)
            }
        case .double(let value):
            return AnyCodable(value)
        case .string(let value):
            return AnyCodable(value)
        case .blob(let value):
            return AnyCodable(
                value.base64EncodedString()
                    .replacingOccurrences(of: "+", with: "-")
                    .replacingOccurrences(of: "/", with: "_")
                    .replacingOccurrences(of: "=", with: "")
            )
        }
    }

    private func checksumJSON(_ checksum: ChecksumObject) throws -> String {
        let data = try JSONEncoder.synchroEncoder().encode(checksum)
        guard let value = String(data: data, encoding: .utf8) else {
            throw SynchroError.invalidResponse(message: "checksum is not UTF-8 JSON")
        }
        return value
    }

    private static func isProtectedApplicationRow(
        db: GRDB.Database,
        tableName: String,
        recordID: String
    ) throws -> Bool {
        if try Row.fetchOne(
            db,
            sql: """
                SELECT 1
                FROM _synchro_pending_changes
                WHERE table_name = ?
                  AND record_id = ?
                  AND lifecycle_state IN ('unsealed', 'sealed', 'blocked_by_predecessor', 'legacy_blocked')
                LIMIT 1
                """,
            arguments: [tableName, recordID]
        ) != nil {
            return true
        }

        return try Row.fetchOne(
            db,
            sql: """
                SELECT 1
                FROM _synchro_rejected_mutations
                WHERE table_name = ?
                  AND record_id = ?
                  AND status = 'rejected_terminal'
                  AND server_row_json IS NULL
                  AND server_version IS NULL
                LIMIT 1
                """,
            arguments: [tableName, recordID]
        ) != nil
    }

    private func removeLocalRowIfUnreferenced(db: GRDB.Database, tableName: String, recordID: String, schema: LocalSchemaTable) throws {
        guard try !Self.isProtectedApplicationRow(db: db, tableName: tableName, recordID: recordID) else {
            return
        }
        guard try !SynchroMeta.hasScopeRows(db, tableName: tableName, recordID: recordID) else {
            return
        }

        let pkCol = schema.primaryKey.first ?? "id"
        let quoted = SQLiteHelpers.quoteIdentifier(tableName)
        let quotedPK = SQLiteHelpers.quoteIdentifier(pkCol)

        try db.execute(
            sql: "DELETE FROM \(quoted) WHERE \(quotedPK) = ?",
            arguments: [recordID]
        )
    }
}
