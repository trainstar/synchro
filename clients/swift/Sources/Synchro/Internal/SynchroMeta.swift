import Foundation
@preconcurrency import GRDB

enum MetaKey: String {
    case checkpoint
    case schemaVersion = "schema_version"
    case schemaHash = "schema_hash"
    case localSchema = "local_schema"
    case clientServerID = "client_server_id"
    case clientGeneration = "client_generation"
    case schemaManifest = "schema_manifest"
    case scopeSetVersion = "scope_set_version"
    case snapshotComplete = "snapshot_complete"
    case syncLock = "sync_lock"
}

struct LocalScopeState: Sendable, Equatable {
    let scopeID: String
    let cursor: String?
    let checksum: String?
    let generation: Int64
    let localChecksum: String
}

struct LocalRebuildAttempt: Sendable, Equatable {
    let scopeID: String
    let rebuildID: String
    let clientGeneration: Int64
    let schemaVersion: Int64
    let schemaHash: String
    let generation: Int64
    let cursor: String?
    let pageLimit: Int
}

struct LocalRowMetadata: Sendable, Equatable {
    let tableName: String
    let recordID: String
    let serverVersion: String
    let rowChecksum: String?
}

struct LocalRebuildPageReceipt: Sendable, Equatable {
    let scopeID: String
    let rebuildID: String
    let requestCursor: String?
    let requestJSON: String
    let responseJSON: String
    let isFinal: Bool
    let finalScopeCursor: String?
    let finalChecksumJSON: String?
}

enum RetryResumeState: String, Sendable, Equatable {
    case connecting
    case pushing
    case pulling
    case rebuilding
}

enum RetryClassification: String, Sendable, Equatable {
    case network
    case http429 = "http_429"
    case http503 = "http_503"
}

struct LocalBackoffRecord: Sendable, Equatable {
    let resumeState: RetryResumeState
    let workIdentity: String
    let retryClassification: RetryClassification
    let attemptCount: Int64
    let nextRetryAtMS: Int64
}

struct LocalRejectedMutation: Sendable, Equatable {
    let mutationID: String
    let localOrder: Int64
    let tableName: String
    let recordID: String
    let status: String
    let code: String
    let message: String?
    let serverRowJSON: String?
    let serverVersion: String?
    let mutationJSON: String?
    let rejectedJSON: String?
    let createdAt: String
    let updatedAt: String
}

enum SynchroMeta {
    static func get(_ db: GRDB.Database, key: MetaKey) throws -> String? {
        try String.fetchOne(db, sql: "SELECT value FROM _synchro_meta WHERE key = ?", arguments: [key.rawValue])
    }

    static func set(_ db: GRDB.Database, key: MetaKey, value: String) throws {
        try db.execute(
            sql: """
                INSERT INTO _synchro_meta (key, value) VALUES (?, ?)
                ON CONFLICT (key) DO UPDATE SET value = excluded.value
                """,
            arguments: [key.rawValue, value]
        )
    }

    static func getInt64(_ db: GRDB.Database, key: MetaKey) throws -> Int64 {
        guard let str = try get(db, key: key), let val = Int64(str) else {
            return 0
        }
        return val
    }

    static func setInt64(_ db: GRDB.Database, key: MetaKey, value: Int64) throws {
        try set(db, key: key, value: String(value))
    }

    /// Binds an ordinary local database to its first configured client identity.
    /// A portable seed remains unbound until its authenticated bootstrap completes.
    static func bindClientIDForNonSeedDatabase(_ db: GRDB.Database, clientID: String) throws {
        guard !clientID.isEmpty else {
            throw SynchroError.invalidResponse(message: "configured client ID is empty")
        }

        if let boundClientID = try get(db, key: .clientServerID) {
            guard boundClientID == clientID else {
                throw SynchroError.invalidResponse(message: "database is bound to another client ID")
            }
            return
        }

        guard try !isPortableSeedBootstrap(db) else {
            return
        }
        try set(db, key: .clientServerID, value: clientID)
    }

    /// Binds the configured identity only after the server accepted the connect.
    static func bindClientIDAfterAuthenticatedConnect(_ db: GRDB.Database, clientID: String) throws {
        guard !clientID.isEmpty else {
            throw SynchroError.invalidResponse(message: "configured client ID is empty")
        }
        if let boundClientID = try get(db, key: .clientServerID) {
            guard boundClientID == clientID else {
                throw SynchroError.invalidResponse(message: "database is bound to another client ID")
            }
            return
        }
        try set(db, key: .clientServerID, value: clientID)
    }

    private static func isPortableSeedBootstrap(_ db: GRDB.Database) throws -> Bool {
        guard try getInt64(db, key: .clientGeneration) == 0 else {
            return false
        }
        return try Row.fetchOne(
            db,
            sql: "SELECT 1 FROM _synchro_seed_receipts LIMIT 1"
        ) != nil
    }

    static func archiveSchema(
        _ db: GRDB.Database,
        version: Int64,
        hash: String,
        tables: [LocalSchemaTable]
    ) throws {
        let encoded = try JSONEncoder().encode(tables)
        guard let schemaJSON = String(data: encoded, encoding: .utf8) else {
            throw SynchroError.invalidResponse(message: "local schema archive is not UTF-8 JSON")
        }
        try db.execute(
            sql: """
                INSERT OR IGNORE INTO _synchro_schema_archive
                    (schema_version, schema_hash, schema_json, created_at)
                VALUES (?, ?, ?, ?)
                """,
            arguments: [version, hash, schemaJSON, timestampNow()]
        )
    }

    static func setSyncLock(_ db: GRDB.Database, locked: Bool) throws {
        try set(db, key: .syncLock, value: locked ? "1" : "0")
    }

    static func isSyncLocked(_ db: GRDB.Database) throws -> Bool {
        try get(db, key: .syncLock) == "1"
    }

    // MARK: - Scope State

    static func getAllScopes(_ db: GRDB.Database) throws -> [LocalScopeState] {
        let rows = try Row.fetchAll(
            db,
            sql: "SELECT scope_id, cursor, checksum, generation, local_checksum FROM _synchro_scopes ORDER BY scope_id"
        )
        return rows.compactMap { row in
            guard let scopeID: String = row["scope_id"] else { return nil }
            let cursor: String? = row["cursor"]
            let checksum: String? = row["checksum"]
            let generation: Int64 = row["generation"] ?? 0
            let localChecksum: String = row["local_checksum"] ?? ""
            return LocalScopeState(
                scopeID: scopeID,
                cursor: cursor,
                checksum: checksum,
                generation: generation,
                localChecksum: localChecksum
            )
        }
    }

    static func getScope(_ db: GRDB.Database, scopeID: String) throws -> LocalScopeState? {
        let row = try Row.fetchOne(
            db,
            sql: "SELECT scope_id, cursor, checksum, generation, local_checksum FROM _synchro_scopes WHERE scope_id = ?",
            arguments: [scopeID]
        )
        guard let row, let currentScopeID: String = row["scope_id"] else {
            return nil
        }
        return LocalScopeState(
            scopeID: currentScopeID,
            cursor: row["cursor"],
            checksum: row["checksum"],
            generation: row["generation"] ?? 0,
            localChecksum: row["local_checksum"] ?? ""
        )
    }

    static func upsertScope(
        _ db: GRDB.Database,
        scopeID: String,
        cursor: String?,
        checksum: String?,
        generation: Int64? = nil,
        localChecksum: String? = nil
    ) throws {
        let currentGeneration: Int64
        if let generation {
            currentGeneration = generation
        } else {
            currentGeneration = try getScopeGeneration(db, scopeID: scopeID)
        }
        let effectiveLocalChecksum: String
        if let localChecksum {
            effectiveLocalChecksum = localChecksum
        } else {
            effectiveLocalChecksum = try getScopeLocalChecksum(db, scopeID: scopeID)
        }
        try db.execute(
            sql: """
                INSERT INTO _synchro_scopes (scope_id, cursor, checksum, generation, local_checksum) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (scope_id) DO UPDATE SET
                    cursor = excluded.cursor,
                    checksum = excluded.checksum,
                    generation = excluded.generation,
                    local_checksum = excluded.local_checksum
                """,
            arguments: [scopeID, cursor, checksum, currentGeneration, effectiveLocalChecksum]
        )
    }

    static func getScopeGeneration(_ db: GRDB.Database, scopeID: String) throws -> Int64 {
        let row = try Row.fetchOne(
            db,
            sql: "SELECT generation FROM _synchro_scopes WHERE scope_id = ?",
            arguments: [scopeID]
        )
        return row?["generation"] as? Int64 ?? 0
    }

    static func bumpScopeGeneration(_ db: GRDB.Database, scopeID: String) throws -> Int64 {
        let nextGeneration = try getScopeGeneration(db, scopeID: scopeID) + 1
        try upsertScope(
            db,
            scopeID: scopeID,
            cursor: nil,
            checksum: nil,
            generation: nextGeneration,
            localChecksum: ""
        )
        return nextGeneration
    }

    static func applyScopeCursorUpdates(
        _ db: GRDB.Database,
        updates: [String: String?],
        affectedScopes: [String]
    ) throws {
        var rebuildScopes = Set(affectedScopes)
        for (scopeID, cursor) in updates {
            guard try getScope(db, scopeID: scopeID) != nil else {
                throw SynchroError.invalidResponse(message: "scope cursor update targets an unknown scope \(scopeID)")
            }
            if cursor == nil {
                rebuildScopes.insert(scopeID)
            } else {
                try upsertScope(
                    db,
                    scopeID: scopeID,
                    cursor: cursor,
                    checksum: nil
                )
            }
        }
        for scopeID in rebuildScopes {
            if try getScope(db, scopeID: scopeID) != nil {
                _ = try bumpScopeGeneration(db, scopeID: scopeID)
            }
        }
    }

    static func deleteScope(_ db: GRDB.Database, scopeID: String) throws {
        try db.execute(sql: "DELETE FROM _synchro_scopes WHERE scope_id = ?", arguments: [scopeID])
    }

    static func clearAllScopes(_ db: GRDB.Database) throws {
        try db.execute(sql: "DELETE FROM _synchro_scopes")
    }

    static func invalidateAllScopes(_ db: GRDB.Database) throws {
        try db.execute(sql: "UPDATE _synchro_scopes SET cursor = NULL, checksum = NULL, generation = 0, local_checksum = ''")
        try clearAllScopeRows(db)
    }

    static func clearAllScopeRows(_ db: GRDB.Database) throws {
        try db.execute(sql: "DELETE FROM _synchro_scope_rows")
        try db.execute(sql: "UPDATE _synchro_scopes SET local_checksum = ''")
    }

    // MARK: - Rejected Mutations

    static func upsertRejectedMutation(
        _ db: GRDB.Database,
        mutationID: String,
        tableName: String,
        recordID: String,
        status: String,
        code: String,
        message: String?,
        serverRow: [String: AnyCodable]?,
        serverVersion: String?,
        mutationJSON: String? = nil,
        rejectedJSON: String? = nil
    ) throws {
        let now = timestampNow()
        let serverRowJSON = try serverRow.flatMap { row -> String? in
            let data = try JSONEncoder.synchroEncoder().encode(row)
            return String(data: data, encoding: .utf8)
        }
        if let existing = try Row.fetchOne(
            db,
            sql: "SELECT status, code, mutation_json, rejected_json FROM _synchro_rejected_mutations WHERE mutation_id = ?",
            arguments: [mutationID]
        ) {
            let existingStatus: String = existing["status"]
            let existingCode: String = existing["code"]
            let existingMutationJSON: String? = existing["mutation_json"]
            let existingJSON: String? = existing["rejected_json"]
            guard existingStatus == status, existingCode == code else {
                throw SynchroError.invalidResponse(message: "mutation has a different terminal outcome")
            }
            if let existingJSON, let rejectedJSON, existingJSON != rejectedJSON {
                throw SynchroError.invalidResponse(message: "mutation has a different terminal outcome")
            }
            if let existingMutationJSON, let mutationJSON, existingMutationJSON != mutationJSON {
                throw SynchroError.invalidResponse(message: "mutation has a different authored payload")
            }
            if existingMutationJSON == nil || existingJSON == nil {
                try db.execute(
                    sql: """
                        UPDATE _synchro_rejected_mutations
                        SET mutation_json = COALESCE(mutation_json, ?),
                            rejected_json = COALESCE(rejected_json, ?),
                            updated_at = ?
                        WHERE mutation_id = ?
                        """,
                    arguments: [mutationJSON, rejectedJSON, now, mutationID]
                )
            }
            return
        }
        try db.execute(
            sql: """
                INSERT INTO _synchro_rejected_mutations
                    (mutation_id, table_name, record_id, status, code, message, server_row_json, server_version,
                     mutation_json, rejected_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
            arguments: [mutationID, tableName, recordID, status, code, message, serverRowJSON, serverVersion, mutationJSON, rejectedJSON, now, now]
        )
    }

    static func listRejectedMutations(_ db: GRDB.Database) throws -> [LocalRejectedMutation] {
        let rows = try Row.fetchAll(
            db,
            sql: """
                SELECT rejected.mutation_id, pending.local_order, rejected.table_name, rejected.record_id,
                       rejected.status, rejected.code, rejected.message, rejected.server_row_json,
                       rejected.server_version, rejected.mutation_json, rejected.rejected_json,
                       rejected.created_at, rejected.updated_at
                FROM _synchro_rejected_mutations AS rejected
                LEFT JOIN _synchro_pending_changes AS pending
                  ON pending.mutation_id = rejected.mutation_id
                ORDER BY rejected.created_at, rejected.mutation_id
                """
        )
        return try rows.map { row in
            guard
                let mutationID: String = row["mutation_id"],
                let localOrder: Int64 = row["local_order"],
                let tableName: String = row["table_name"],
                let recordID: String = row["record_id"],
                let status: String = row["status"],
                let code: String = row["code"],
                let createdAt: String = row["created_at"],
                let updatedAt: String = row["updated_at"]
            else {
                throw SynchroError.invalidResponse(message: "retained rejection has no complete durable mutation")
            }
            return LocalRejectedMutation(
                mutationID: mutationID,
                localOrder: localOrder,
                tableName: tableName,
                recordID: recordID,
                status: status,
                code: code,
                message: row["message"],
                serverRowJSON: row["server_row_json"],
                serverVersion: row["server_version"],
                mutationJSON: row["mutation_json"],
                rejectedJSON: row["rejected_json"],
                createdAt: createdAt,
                updatedAt: updatedAt
            )
        }
    }

    static func clearRejectedMutations(_ db: GRDB.Database) throws {
        try db.execute(sql: "DELETE FROM _synchro_rejected_mutations")
    }

    // MARK: - Scope Rows

    static func upsertScopeRow(
        _ db: GRDB.Database,
        scopeID: String,
        tableName: String,
        recordID: String,
        checksum: String,
        generation: Int64
    ) throws {
        try db.execute(
            sql: """
                INSERT INTO _synchro_scope_rows (scope_id, table_name, record_id, checksum, generation) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (scope_id, table_name, record_id) DO UPDATE SET
                    checksum = excluded.checksum,
                    generation = excluded.generation
                """,
            arguments: [scopeID, tableName, recordID, checksum, generation]
        )
    }

    static func deleteScopeRow(_ db: GRDB.Database, scopeID: String, tableName: String, recordID: String) throws {
        try db.execute(
            sql: "DELETE FROM _synchro_scope_rows WHERE scope_id = ? AND table_name = ? AND record_id = ?",
            arguments: [scopeID, tableName, recordID]
        )
    }

    static func updateScopeRowChecksum(
        _ db: GRDB.Database,
        scopeID: String,
        tableName: String,
        recordID: String,
        checksum: String
    ) throws {
        try db.execute(
            sql: "UPDATE _synchro_scope_rows SET checksum = ? WHERE scope_id = ? AND table_name = ? AND record_id = ?",
            arguments: [checksum, scopeID, tableName, recordID]
        )
        guard db.changesCount == 1 else {
            throw SynchroError.invalidResponse(message: "scope row disappeared during schema activation")
        }
    }

    static func deleteScopeRows(_ db: GRDB.Database, scopeID: String) throws {
        try db.execute(
            sql: "DELETE FROM _synchro_scope_rows WHERE scope_id = ?",
            arguments: [scopeID]
        )
    }

    static func getScopeRowRecordIDs(_ db: GRDB.Database, scopeID: String) throws -> [(tableName: String, recordID: String)] {
        let rows = try Row.fetchAll(
            db,
            sql: "SELECT table_name, record_id FROM _synchro_scope_rows WHERE scope_id = ?",
            arguments: [scopeID]
        )
        return rows.compactMap { row in
            guard let tableName: String = row["table_name"],
                  let recordID: String = row["record_id"] else { return nil }
            return (tableName: tableName, recordID: recordID)
        }
    }

    static func getStaleScopeRowRecordIDs(_ db: GRDB.Database, scopeID: String, generation: Int64) throws -> [(tableName: String, recordID: String)] {
        let rows = try Row.fetchAll(
            db,
            sql: "SELECT table_name, record_id FROM _synchro_scope_rows WHERE scope_id = ? AND generation <> ?",
            arguments: [scopeID, generation]
        )
        return rows.compactMap { row in
            guard let tableName: String = row["table_name"],
                  let recordID: String = row["record_id"] else { return nil }
            return (tableName: tableName, recordID: recordID)
        }
    }

    static func deleteStaleScopeRows(_ db: GRDB.Database, scopeID: String, generation: Int64) throws {
        try db.execute(
            sql: "DELETE FROM _synchro_scope_rows WHERE scope_id = ? AND generation <> ?",
            arguments: [scopeID, generation]
        )
    }

    static func hasScopeRows(_ db: GRDB.Database, tableName: String, recordID: String) throws -> Bool {
        let row = try Row.fetchOne(
            db,
            sql: "SELECT 1 AS present FROM _synchro_scope_rows WHERE table_name = ? AND record_id = ? LIMIT 1",
            arguments: [tableName, recordID]
        )
        return row != nil
    }

    static func getScopeLocalChecksum(_ db: GRDB.Database, scopeID: String) throws -> String {
        let row = try Row.fetchOne(
            db,
            sql: "SELECT local_checksum FROM _synchro_scopes WHERE scope_id = ?",
            arguments: [scopeID]
        )
        return row?["local_checksum"] ?? ""
    }

    static func setScopeLocalChecksum(_ db: GRDB.Database, scopeID: String, checksum: String) throws {
        try db.execute(
            sql: "UPDATE _synchro_scopes SET local_checksum = ? WHERE scope_id = ?",
            arguments: [checksum, scopeID]
        )
    }

    static func getScopeRowChecksums(
        _ db: GRDB.Database,
        scopeID: String
    ) throws -> [(tableName: String, recordID: String, checksum: String, generation: Int64)] {
        let rows = try Row.fetchAll(
            db,
            sql: "SELECT table_name, record_id, checksum, generation FROM _synchro_scope_rows WHERE scope_id = ?",
            arguments: [scopeID]
        )
        return rows.compactMap { row in
            guard let tableName: String = row["table_name"],
                  let recordID: String = row["record_id"],
                  let checksum: String = row["checksum"],
                  let generation: Int64 = row["generation"] else { return nil }
            return (tableName, recordID, checksum, generation)
        }
    }

    static func upsertRowVersion(
        _ db: GRDB.Database,
        tableName: String,
        recordID: String,
        serverVersion: String,
        rowChecksum: ChecksumObject?
    ) throws {
        let checksumJSON: String?
        if let rowChecksum {
            let encoded = try JSONEncoder.synchroEncoder().encode(rowChecksum)
            guard let value = String(data: encoded, encoding: .utf8) else {
                throw SynchroError.invalidResponse(message: "row checksum is not UTF-8 JSON")
            }
            checksumJSON = value
        } else {
            checksumJSON = nil
        }
        try db.execute(
            sql: """
                INSERT INTO _synchro_row_versions (table_name, record_id, server_version, row_checksum)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (table_name, record_id) DO UPDATE SET
                    server_version = excluded.server_version,
                    row_checksum = excluded.row_checksum
                """,
            arguments: [tableName, recordID, serverVersion, checksumJSON]
        )
    }

    static func getRowVersion(_ db: GRDB.Database, tableName: String, recordID: String) throws -> String? {
        try String.fetchOne(
            db,
            sql: "SELECT server_version FROM _synchro_row_versions WHERE table_name = ? AND record_id = ?",
            arguments: [tableName, recordID]
        )
    }

    static func getRowMetadata(_ db: GRDB.Database, tableName: String, recordID: String) throws -> LocalRowMetadata? {
        let row = try Row.fetchOne(
            db,
            sql: "SELECT table_name, record_id, server_version, row_checksum FROM _synchro_row_versions WHERE table_name = ? AND record_id = ?",
            arguments: [tableName, recordID]
        )
        guard let row,
              let currentTableName: String = row["table_name"],
              let currentRecordID: String = row["record_id"],
              let serverVersion: String = row["server_version"] else {
            return nil
        }
        return LocalRowMetadata(
            tableName: currentTableName,
            recordID: currentRecordID,
            serverVersion: serverVersion,
            rowChecksum: row["row_checksum"]
        )
    }

    static func getSeedReceipts(_ db: GRDB.Database) throws -> [String: String] {
        let rows = try Row.fetchAll(db, sql: "SELECT scope_id, receipt FROM _synchro_seed_receipts ORDER BY scope_id")
        return Dictionary(uniqueKeysWithValues: rows.compactMap { row in
            guard let scopeID: String = row["scope_id"], let receipt: String = row["receipt"] else { return nil }
            return (scopeID, receipt)
        })
    }

    static func clearSeedReceipts(_ db: GRDB.Database) throws {
        try db.execute(sql: "DELETE FROM _synchro_seed_receipts")
    }

    static func deleteSeedReceipt(_ db: GRDB.Database, scopeID: String) throws {
        try db.execute(
            sql: "DELETE FROM _synchro_seed_receipts WHERE scope_id = ?",
            arguments: [scopeID]
        )
        guard db.changesCount == 1 else {
            throw SynchroError.invalidResponse(message: "seed receipt disappeared during validation")
        }
    }

    // MARK: - Durable Backoff

    static func getBackoffRecord(_ db: GRDB.Database) throws -> LocalBackoffRecord? {
        let row = try Row.fetchOne(
            db,
            sql: """
                SELECT resume_state, work_identity, retry_classification, attempt_count, next_retry_at_ms
                FROM _synchro_backoff
                WHERE singleton = 1
                """
        )
        guard let row else { return nil }
        guard let resumeStateValue: String = row["resume_state"],
              let resumeState = RetryResumeState(rawValue: resumeStateValue),
              let workIdentity: String = row["work_identity"],
              !workIdentity.isEmpty,
              let classificationValue: String = row["retry_classification"],
              let retryClassification = RetryClassification(rawValue: classificationValue),
              let attemptCount: Int64 = row["attempt_count"],
              attemptCount > 0,
              let nextRetryAtMS: Int64 = row["next_retry_at_ms"] else {
            throw SynchroError.invalidResponse(message: "durable backoff record is invalid")
        }
        return LocalBackoffRecord(
            resumeState: resumeState,
            workIdentity: workIdentity,
            retryClassification: retryClassification,
            attemptCount: attemptCount,
            nextRetryAtMS: nextRetryAtMS
        )
    }

    static func upsertBackoffRecord(_ db: GRDB.Database, record: LocalBackoffRecord) throws {
        guard !record.workIdentity.isEmpty,
              record.attemptCount > 0 else {
            throw SynchroError.invalidResponse(message: "durable backoff record is invalid")
        }
        try db.execute(
            sql: """
                INSERT INTO _synchro_backoff
                    (singleton, resume_state, work_identity, retry_classification, attempt_count, next_retry_at_ms)
                VALUES (1, ?, ?, ?, ?, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                    resume_state = excluded.resume_state,
                    work_identity = excluded.work_identity,
                    retry_classification = excluded.retry_classification,
                    attempt_count = excluded.attempt_count,
                    next_retry_at_ms = excluded.next_retry_at_ms
                """,
            arguments: [
                record.resumeState.rawValue,
                record.workIdentity,
                record.retryClassification.rawValue,
                record.attemptCount,
                record.nextRetryAtMS,
            ]
        )
    }

    /// Clears only the record that describes completed durable work.
    static func clearMatchingBackoffRecord(
        _ db: GRDB.Database,
        resumeState: RetryResumeState,
        workIdentity: String
    ) throws {
        guard !workIdentity.isEmpty else {
            throw SynchroError.invalidResponse(message: "durable backoff work identity is invalid")
        }
        try db.execute(
            sql: """
                DELETE FROM _synchro_backoff
                WHERE singleton = 1
                  AND resume_state = ?
                  AND work_identity = ?
                """,
            arguments: [resumeState.rawValue, workIdentity]
        )
    }

    /// Removes a rebuild retry only when its exact durable request targets the removed scope.
    static func clearRebuildingBackoffForScope(
        _ db: GRDB.Database,
        scopeID: String
    ) throws {
        guard let backoff = try getBackoffRecord(db), backoff.resumeState == .rebuilding else {
            return
        }
        let requestData = Data(backoff.workIdentity.utf8)
        guard (try? Integrity.validateCanonicalWireJSON(requestData)) != nil,
              let request = try? JSONDecoder.synchroDecoder().decode(
                RebuildRequest.self,
                from: requestData
              ),
              request.scope == scopeID else {
            return
        }
        try clearMatchingBackoffRecord(
            db,
            resumeState: .rebuilding,
            workIdentity: backoff.workIdentity
        )
    }

    static func getRebuildAttempt(_ db: GRDB.Database, scopeID: String) throws -> LocalRebuildAttempt? {
        let row = try Row.fetchOne(
            db,
            sql: """
                SELECT scope_id, rebuild_id, client_generation, schema_version, schema_hash, generation, cursor, page_limit
                FROM _synchro_rebuild_attempts WHERE scope_id = ?
                """,
            arguments: [scopeID]
        )
        guard let row,
              let currentScopeID: String = row["scope_id"],
              let rebuildID: String = row["rebuild_id"],
              let clientGeneration: Int64 = row["client_generation"],
              let schemaVersion: Int64 = row["schema_version"],
              let schemaHash: String = row["schema_hash"],
              let generation: Int64 = row["generation"],
              let pageLimit: Int = row["page_limit"] else { return nil }
        return LocalRebuildAttempt(
            scopeID: currentScopeID,
            rebuildID: rebuildID,
            clientGeneration: clientGeneration,
            schemaVersion: schemaVersion,
            schemaHash: schemaHash,
            generation: generation,
            cursor: row["cursor"],
            pageLimit: pageLimit
        )
    }

    static func upsertRebuildAttempt(_ db: GRDB.Database, attempt: LocalRebuildAttempt) throws {
        try db.execute(
            sql: """
                INSERT INTO _synchro_rebuild_attempts
                    (scope_id, rebuild_id, client_generation, schema_version, schema_hash, generation, cursor, page_limit)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (scope_id) DO UPDATE SET
                    rebuild_id = excluded.rebuild_id,
                    client_generation = excluded.client_generation,
                    schema_version = excluded.schema_version,
                    schema_hash = excluded.schema_hash,
                    generation = excluded.generation,
                    cursor = excluded.cursor,
                    page_limit = excluded.page_limit
                """,
            arguments: [
                attempt.scopeID, attempt.rebuildID, attempt.clientGeneration, attempt.schemaVersion,
                attempt.schemaHash, attempt.generation, attempt.cursor, attempt.pageLimit,
            ]
        )
    }

    static func deleteRebuildAttempt(_ db: GRDB.Database, scopeID: String) throws {
        try db.execute(sql: "DELETE FROM _synchro_rebuild_attempts WHERE scope_id = ?", arguments: [scopeID])
    }

    static func getRebuildPageReceipt(
        _ db: GRDB.Database,
        scopeID: String,
        rebuildID: String,
        requestCursor: String?
    ) throws -> LocalRebuildPageReceipt? {
        let row = try Row.fetchOne(
            db,
            sql: """
                SELECT scope_id, rebuild_id, request_cursor_is_null, request_cursor, response_json,
                       request_json, is_final, final_scope_cursor, final_checksum
                FROM _synchro_rebuild_page_receipts
                WHERE scope_id = ?
                  AND rebuild_id = ?
                  AND request_cursor_is_null = ?
                  AND request_cursor = ?
                """,
            arguments: [scopeID, rebuildID, requestCursor == nil ? 1 : 0, requestCursor ?? ""]
        )
        return try rebuildPageReceipt(from: row)
    }

    static func listRebuildPageReceipts(_ db: GRDB.Database) throws -> [LocalRebuildPageReceipt] {
        let rows = try Row.fetchAll(
            db,
            sql: """
                SELECT scope_id, rebuild_id, request_cursor_is_null, request_cursor, response_json,
                       request_json, is_final, final_scope_cursor, final_checksum
                FROM _synchro_rebuild_page_receipts
                ORDER BY scope_id, rebuild_id, request_cursor_is_null DESC, request_cursor
                """
        )
        return try rows.map { row in
            guard let receipt = try rebuildPageReceipt(from: row) else {
                throw SynchroError.invalidResponse(message: "rebuild page receipt is incomplete")
            }
            return receipt
        }
    }

    static func getArchivedSchemaTables(
        _ db: GRDB.Database,
        version: Int64,
        hash: String
    ) throws -> [LocalSchemaTable]? {
        guard let schemaJSON: String = try String.fetchOne(
            db,
            sql: """
                SELECT schema_json
                FROM _synchro_schema_archive
                WHERE schema_version = ? AND schema_hash = ?
                """,
            arguments: [version, hash]
        ) else {
            return nil
        }
        guard let data = schemaJSON.data(using: .utf8) else {
            throw SynchroError.invalidResponse(message: "archived schema metadata is invalid")
        }
        do {
            return try JSONDecoder().decode([LocalSchemaTable].self, from: data)
        } catch {
            throw SynchroError.invalidResponse(message: "archived schema metadata is invalid")
        }
    }

    static func getFinalRebuildPageReceipt(
        _ db: GRDB.Database,
        scopeID: String,
        rebuildID: String
    ) throws -> LocalRebuildPageReceipt? {
        let row = try Row.fetchOne(
            db,
            sql: """
                SELECT scope_id, rebuild_id, request_cursor_is_null, request_cursor, response_json,
                       request_json, is_final, final_scope_cursor, final_checksum
                FROM _synchro_rebuild_page_receipts
                WHERE scope_id = ? AND rebuild_id = ? AND is_final = 1
                """,
            arguments: [scopeID, rebuildID]
        )
        return try rebuildPageReceipt(from: row)
    }

    static func insertRebuildPageReceipt(
        _ db: GRDB.Database,
        scopeID: String,
        rebuildID: String,
        requestCursor: String?,
        requestJSON: String,
        responseJSON: String,
        finalScopeCursor: String?,
        finalChecksumJSON: String?
    ) throws {
        let isFinal = finalScopeCursor != nil
        try db.execute(
            sql: """
                INSERT INTO _synchro_rebuild_page_receipts
                    (scope_id, rebuild_id, request_cursor_is_null, request_cursor, request_json,
                     response_json, is_final, final_scope_cursor, final_checksum)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
            arguments: [
                scopeID,
                rebuildID,
                requestCursor == nil ? 1 : 0,
                requestCursor ?? "",
                requestJSON,
                responseJSON,
                isFinal ? 1 : 0,
                finalScopeCursor,
                finalChecksumJSON,
            ]
        )
    }

    static func deleteRebuildPageReceipts(
        _ db: GRDB.Database,
        scopeID: String,
        rebuildID: String
    ) throws {
        try db.execute(
            sql: "DELETE FROM _synchro_rebuild_page_receipts WHERE scope_id = ? AND rebuild_id = ?",
            arguments: [scopeID, rebuildID]
        )
    }

    static func deleteRebuildPageReceipts(
        _ db: GRDB.Database,
        scopeID: String
    ) throws {
        try db.execute(
            sql: "DELETE FROM _synchro_rebuild_page_receipts WHERE scope_id = ?",
            arguments: [scopeID]
        )
    }

    private static func rebuildPageReceipt(from row: Row?) throws -> LocalRebuildPageReceipt? {
        guard let row else { return nil }
        guard let scopeID: String = row["scope_id"],
              let rebuildID: String = row["rebuild_id"],
              let cursorIsNull: Int = row["request_cursor_is_null"],
              let requestCursor: String = row["request_cursor"],
              let requestJSON: String = row["request_json"],
              let responseJSON: String = row["response_json"],
              let isFinalValue: Int = row["is_final"] else {
            throw SynchroError.invalidResponse(message: "rebuild page receipt is incomplete")
        }
        guard cursorIsNull == 0 || cursorIsNull == 1,
              isFinalValue == 0 || isFinalValue == 1,
              cursorIsNull == 0 || requestCursor.isEmpty else {
            throw SynchroError.invalidResponse(message: "rebuild page receipt is invalid")
        }
        let isFinal = isFinalValue == 1
        let finalScopeCursor: String? = row["final_scope_cursor"]
        let finalChecksumJSON: String? = row["final_checksum"]
        guard isFinal == (finalScopeCursor != nil && finalChecksumJSON != nil) else {
            throw SynchroError.invalidResponse(message: "rebuild page receipt finality is invalid")
        }
        return LocalRebuildPageReceipt(
            scopeID: scopeID,
            rebuildID: rebuildID,
            requestCursor: cursorIsNull == 1 ? nil : requestCursor,
            requestJSON: requestJSON,
            responseJSON: responseJSON,
            isFinal: isFinal,
            finalScopeCursor: finalScopeCursor,
            finalChecksumJSON: finalChecksumJSON
        )
    }

    private static let rejectedMutationTimestampFormatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        return formatter
    }()

    private static func timestampNow() -> String {
        rejectedMutationTimestampFormatter.string(from: Date())
    }
}
