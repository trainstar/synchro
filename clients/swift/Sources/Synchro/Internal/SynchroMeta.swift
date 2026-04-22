import Foundation
@preconcurrency import GRDB

enum MetaKey: String {
    case checkpoint
    case schemaVersion = "schema_version"
    case schemaHash = "schema_hash"
    case localSchema = "local_schema"
    case clientServerID = "client_server_id"
    case scopeSetVersion = "scope_set_version"
    case knownBuckets = "known_buckets"
    case snapshotComplete = "snapshot_complete"
    case syncLock = "sync_lock"
}

struct LocalScopeState: Sendable, Equatable {
    let scopeID: String
    let cursor: String?
    let checksum: String?
    let generation: Int64
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

    static func setSyncLock(_ db: GRDB.Database, locked: Bool) throws {
        try set(db, key: .syncLock, value: locked ? "1" : "0")
    }

    static func isSyncLocked(_ db: GRDB.Database) throws -> Bool {
        try get(db, key: .syncLock) == "1"
    }

    // MARK: - Bucket Checkpoints

    static func getBucketCheckpoint(_ db: GRDB.Database, bucketID: String) throws -> Int64 {
        let row = try Row.fetchOne(
            db,
            sql: "SELECT checkpoint FROM _synchro_bucket_checkpoints WHERE bucket_id = ?",
            arguments: [bucketID]
        )
        return row?["checkpoint"] as? Int64 ?? 0
    }

    static func setBucketCheckpoint(_ db: GRDB.Database, bucketID: String, checkpoint: Int64) throws {
        try db.execute(
            sql: """
                INSERT INTO _synchro_bucket_checkpoints (bucket_id, checkpoint) VALUES (?, ?)
                ON CONFLICT (bucket_id) DO UPDATE SET checkpoint = excluded.checkpoint
                """,
            arguments: [bucketID, checkpoint]
        )
    }

    static func getAllBucketCheckpoints(_ db: GRDB.Database) throws -> [String: Int64] {
        let rows = try Row.fetchAll(db, sql: "SELECT bucket_id, checkpoint FROM _synchro_bucket_checkpoints")
        var result: [String: Int64] = [:]
        for row in rows {
            if let bucketID: String = row["bucket_id"], let checkpoint: Int64 = row["checkpoint"] {
                result[bucketID] = checkpoint
            }
        }
        return result
    }

    static func deleteBucketCheckpoint(_ db: GRDB.Database, bucketID: String) throws {
        try db.execute(
            sql: "DELETE FROM _synchro_bucket_checkpoints WHERE bucket_id = ?",
            arguments: [bucketID]
        )
    }

    static func deleteAllBucketCheckpoints(_ db: GRDB.Database) throws {
        try db.execute(sql: "DELETE FROM _synchro_bucket_checkpoints")
    }

    // MARK: - Bucket Members

    static func upsertBucketMember(_ db: GRDB.Database, bucketID: String, tableName: String, recordID: String, checksum: Int32) throws {
        try db.execute(
            sql: """
                INSERT INTO _synchro_bucket_members (bucket_id, table_name, record_id, checksum) VALUES (?, ?, ?, ?)
                ON CONFLICT (bucket_id, table_name, record_id) DO UPDATE SET checksum = excluded.checksum
                """,
            arguments: [bucketID, tableName, recordID, Int64(checksum)]
        )
    }

    static func deleteBucketMembers(_ db: GRDB.Database, bucketID: String) throws {
        try db.execute(
            sql: "DELETE FROM _synchro_bucket_members WHERE bucket_id = ?",
            arguments: [bucketID]
        )
    }

    static func deleteAllBucketMembers(_ db: GRDB.Database) throws {
        try db.execute(sql: "DELETE FROM _synchro_bucket_members")
    }

    static func getBucketMemberRecordIDs(_ db: GRDB.Database, bucketID: String) throws -> [(tableName: String, recordID: String)] {
        let rows = try Row.fetchAll(
            db,
            sql: "SELECT table_name, record_id FROM _synchro_bucket_members WHERE bucket_id = ?",
            arguments: [bucketID]
        )
        return rows.compactMap { row in
            guard let tableName: String = row["table_name"],
                  let recordID: String = row["record_id"] else { return nil }
            return (tableName: tableName, recordID: recordID)
        }
    }

    static func hasBucketMembers(_ db: GRDB.Database, tableName: String, recordID: String) throws -> Bool {
        let row = try Row.fetchOne(
            db,
            sql: "SELECT 1 AS present FROM _synchro_bucket_members WHERE table_name = ? AND record_id = ? LIMIT 1",
            arguments: [tableName, recordID]
        )
        return row != nil
    }

    // MARK: - Scope State

    static func getAllScopes(_ db: GRDB.Database) throws -> [LocalScopeState] {
        let rows = try Row.fetchAll(
            db,
            sql: "SELECT scope_id, cursor, checksum, generation FROM _synchro_scopes ORDER BY scope_id"
        )
        return rows.compactMap { row in
            guard let scopeID: String = row["scope_id"] else { return nil }
            let cursor: String? = row["cursor"]
            let checksum: String? = row["checksum"]
            let generation: Int64 = row["generation"] ?? 0
            return LocalScopeState(
                scopeID: scopeID,
                cursor: cursor,
                checksum: checksum,
                generation: generation
            )
        }
    }

    static func upsertScope(_ db: GRDB.Database, scopeID: String, cursor: String?, checksum: String?, generation: Int64? = nil) throws {
        let currentGeneration: Int64
        if let generation {
            currentGeneration = generation
        } else {
            currentGeneration = try getScopeGeneration(db, scopeID: scopeID)
        }
        try db.execute(
            sql: """
                INSERT INTO _synchro_scopes (scope_id, cursor, checksum, generation) VALUES (?, ?, ?, ?)
                ON CONFLICT (scope_id) DO UPDATE SET
                    cursor = excluded.cursor,
                    checksum = excluded.checksum,
                    generation = excluded.generation
                """,
            arguments: [scopeID, cursor, checksum, currentGeneration]
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
        try upsertScope(db, scopeID: scopeID, cursor: nil, checksum: nil, generation: nextGeneration)
        return nextGeneration
    }

    static func deleteScope(_ db: GRDB.Database, scopeID: String) throws {
        try db.execute(sql: "DELETE FROM _synchro_scopes WHERE scope_id = ?", arguments: [scopeID])
    }

    static func clearAllScopes(_ db: GRDB.Database) throws {
        try db.execute(sql: "DELETE FROM _synchro_scopes")
    }

    static func invalidateAllScopes(_ db: GRDB.Database) throws {
        try db.execute(sql: "UPDATE _synchro_scopes SET cursor = NULL, checksum = NULL, generation = 0")
        try clearAllScopeRows(db)
    }

    static func clearAllScopeRows(_ db: GRDB.Database) throws {
        try db.execute(sql: "DELETE FROM _synchro_scope_rows")
    }

    // MARK: - Scope Rows

    static func upsertScopeRow(_ db: GRDB.Database, scopeID: String, tableName: String, recordID: String, generation: Int64) throws {
        try db.execute(
            sql: """
                INSERT INTO _synchro_scope_rows (scope_id, table_name, record_id, generation) VALUES (?, ?, ?, ?)
                ON CONFLICT (scope_id, table_name, record_id) DO UPDATE SET generation = excluded.generation
                """,
            arguments: [scopeID, tableName, recordID, generation]
        )
    }

    static func deleteScopeRow(_ db: GRDB.Database, scopeID: String, tableName: String, recordID: String) throws {
        try db.execute(
            sql: "DELETE FROM _synchro_scope_rows WHERE scope_id = ? AND table_name = ? AND record_id = ?",
            arguments: [scopeID, tableName, recordID]
        )
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
}
