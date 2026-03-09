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
}
