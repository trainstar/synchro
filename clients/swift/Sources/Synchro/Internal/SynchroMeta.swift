import Foundation
import GRDB

enum MetaKey: String {
    case checkpoint
    case schemaVersion = "schema_version"
    case schemaHash = "schema_hash"
    case clientServerID = "client_server_id"
    case knownBuckets = "known_buckets"
    case snapshotComplete = "snapshot_complete"
    case syncLock = "sync_lock"
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
}
