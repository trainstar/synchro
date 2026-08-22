import XCTest
import GRDB
@testable import Synchro

final class DatabaseMigrationTests: XCTestCase {
    func testShippedVersionFiveUpgradePreservesIntentAndAddsProtocolThreeState() throws {
        let path = (NSTemporaryDirectory() as NSString).appendingPathComponent("synchro_migration_\(UUID().uuidString).sqlite")
        let legacy = try DatabaseQueue(path: path)
        try legacy.write { db in
            try db.execute(sql: "CREATE TABLE _grdb_migrations (identifier TEXT NOT NULL PRIMARY KEY)")
            for identifier in ["synchro_v1", "synchro_v2_buckets", "synchro_v3_scopes", "synchro_v4_scope_integrity", "synchro_v5_rejected_mutations"] {
                try db.execute(sql: "INSERT INTO _grdb_migrations (identifier) VALUES (?)", arguments: [identifier])
            }
            try db.execute(sql: "CREATE TABLE _synchro_pending_changes (record_id TEXT NOT NULL, table_name TEXT NOT NULL, operation TEXT NOT NULL, base_updated_at TEXT, client_updated_at TEXT NOT NULL, PRIMARY KEY (table_name, record_id))")
            try db.execute(sql: "CREATE TABLE _synchro_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            try db.execute(sql: "INSERT INTO _synchro_meta (key, value) VALUES ('sync_lock', '0'), ('checkpoint', '0')")
            try db.execute(sql: "CREATE TABLE _synchro_scopes (scope_id TEXT PRIMARY KEY, cursor TEXT, checksum TEXT, generation INTEGER NOT NULL DEFAULT 0, local_checksum INTEGER NOT NULL DEFAULT 0)")
            try db.execute(sql: "CREATE TABLE _synchro_scope_rows (scope_id TEXT NOT NULL, table_name TEXT NOT NULL, record_id TEXT NOT NULL, checksum INTEGER NOT NULL DEFAULT 0, generation INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (scope_id, table_name, record_id))")
            try db.execute(sql: "CREATE TABLE _synchro_rejected_mutations (mutation_id TEXT PRIMARY KEY, table_name TEXT NOT NULL, record_id TEXT NOT NULL, status TEXT NOT NULL, code TEXT NOT NULL, message TEXT, server_row_json TEXT, server_version TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)")
            try db.execute(sql: "CREATE TABLE _synchro_bucket_members (bucket_id TEXT NOT NULL, table_name TEXT NOT NULL, record_id TEXT NOT NULL, checksum INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (bucket_id, table_name, record_id))")
            try db.execute(sql: "CREATE TABLE _synchro_bucket_checkpoints (bucket_id TEXT PRIMARY KEY, checkpoint INTEGER NOT NULL DEFAULT 0)")
            try db.execute(sql: "INSERT INTO _synchro_pending_changes VALUES (?, ?, ?, NULL, ?)", arguments: ["r1", "orders", "create", "2026-01-01T00:00:00.000000Z"])
            try db.execute(sql: "INSERT INTO _synchro_rejected_mutations VALUES ('m1', 'orders', 'r0', 'rejected_terminal', 'policy_rejected', 'blocked', NULL, NULL, '2026-01-01T00:00:00.000000Z', '2026-01-01T00:00:00.000000Z')")
            try db.execute(sql: "INSERT INTO _synchro_scopes VALUES ('orders:user-1', 'old-cursor', 'old-checksum', 4, 7)")
            try db.execute(sql: "INSERT INTO _synchro_scope_rows VALUES ('orders:user-1', 'orders', 'r0', 7, 4)")
        }
        try legacy.close()

        let db = try SynchroDatabase(path: path)
        XCTAssertEqual(try db.query("SELECT record_id FROM _synchro_pending_changes", params: nil).count, 1)
        let pending = try db.queryOne(
            "SELECT local_order, operation, lifecycle_state, source_kind FROM _synchro_pending_changes WHERE table_name = 'orders' AND record_id = 'r1'",
            params: nil
        )
        XCTAssertEqual(pending?["local_order"] as Int64?, 1)
        XCTAssertEqual(pending?["operation"] as String?, "insert")
        XCTAssertEqual(pending?["lifecycle_state"] as String?, "legacy_blocked")
        XCTAssertEqual(pending?["source_kind"] as String?, "legacy_import")
        XCTAssertEqual(
            try db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='_synchro_pending_changes'", params: nil).count,
            1
        )
        let legacyRejection = try db.queryOne(
            "SELECT mutation_id, table_name, record_id, status, code, message, created_at, updated_at, mutation_json, rejected_json FROM _synchro_rejected_mutations WHERE mutation_id = 'm1'",
            params: nil
        )
        XCTAssertEqual(legacyRejection?["mutation_id"] as String?, "m1")
        XCTAssertEqual(legacyRejection?["table_name"] as String?, "orders")
        XCTAssertEqual(legacyRejection?["record_id"] as String?, "r0")
        XCTAssertEqual(legacyRejection?["status"] as String?, "rejected_terminal")
        XCTAssertEqual(legacyRejection?["code"] as String?, "policy_rejected")
        XCTAssertEqual(legacyRejection?["message"] as String?, "blocked")
        XCTAssertEqual(legacyRejection?["created_at"] as String?, "2026-01-01T00:00:00.000000Z")
        XCTAssertEqual(legacyRejection?["updated_at"] as String?, "2026-01-01T00:00:00.000000Z")
        XCTAssertNil(legacyRejection?["mutation_json"] as String?)
        XCTAssertNil(legacyRejection?["rejected_json"] as String?)
        XCTAssertEqual(try db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='_synchro_row_versions'", params: nil).count, 1)
        XCTAssertEqual(try db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='_synchro_seed_receipts'", params: nil).count, 1)
        XCTAssertEqual(try db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='_synchro_rebuild_attempts'", params: nil).count, 1)
        XCTAssertEqual(try db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='_synchro_rebuild_page_receipts'", params: nil).count, 1)
        XCTAssertEqual(try db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='_synchro_backoff'", params: nil).count, 1)
        XCTAssertEqual(try db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='_synchro_blocking_error'", params: nil).count, 1)
        XCTAssertEqual(try db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='_synchro_schema_migration'", params: nil).count, 1)
        XCTAssertEqual(try db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='_synchro_push_batches'", params: nil).count, 1)
        XCTAssertEqual(try db.query("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '_synchro_bucket_%'", params: nil).count, 0)
        let scope = try db.readTransaction { try SynchroMeta.getScope($0, scopeID: "orders:user-1") }
        XCTAssertNil(scope?.cursor)
        XCTAssertNil(scope?.checksum)
        XCTAssertEqual(scope?.generation, 0)
        XCTAssertEqual(try db.query("SELECT * FROM _synchro_scope_rows", params: nil).count, 0)
    }

    func testVersionTenUpgradeAddsBackoffWithoutChangingQueueOrRebuildState() throws {
        let path = (NSTemporaryDirectory() as NSString).appendingPathComponent("synchro_backoff_migration_\(UUID().uuidString).sqlite")
        let legacy = try DatabaseQueue(path: path)
        try legacy.write { db in
            try db.execute(sql: "CREATE TABLE grdb_migrations (identifier TEXT NOT NULL PRIMARY KEY)")
            for identifier in [
                "synchro_v1",
                "synchro_v2_buckets",
                "synchro_v3_scopes",
                "synchro_v4_scope_integrity",
                "synchro_v5_rejected_mutations",
                "synchro_v6_protocol_3",
                "synchro_v7_pending_local_revision",
                "synchro_v8_sealed_push_batches",
                "synchro_v9_mutation_ledger",
                "synchro_v10_rebuild_page_receipts",
            ] {
                try db.execute(sql: "INSERT INTO grdb_migrations (identifier) VALUES (?)", arguments: [identifier])
            }
            try db.execute(sql: "CREATE TABLE _synchro_pending_changes (mutation_id TEXT PRIMARY KEY, lifecycle_state TEXT NOT NULL)")
            try db.execute(sql: "INSERT INTO _synchro_pending_changes VALUES ('mutation-1', 'sealed')")
            try db.execute(sql: "CREATE TABLE _synchro_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            try db.execute(sql: "INSERT INTO _synchro_meta (key, value) VALUES ('sync_lock', '0'), ('checkpoint', '0')")
            try db.execute(sql: "CREATE TABLE _synchro_push_batches (batch_id TEXT PRIMARY KEY, request_json TEXT NOT NULL, state TEXT NOT NULL)")
            try db.execute(sql: "INSERT INTO _synchro_push_batches VALUES ('batch-1', '{\"batch_id\":\"batch-1\"}', 'pending')")
            try db.execute(sql: "CREATE TABLE _synchro_rebuild_attempts (scope_id TEXT PRIMARY KEY, rebuild_id TEXT NOT NULL, cursor TEXT)")
            try db.execute(sql: "INSERT INTO _synchro_rebuild_attempts VALUES ('orders:user-1', 'rebuild-1', 'cursor-1')")
            try db.execute(sql: "CREATE TABLE _synchro_rebuild_page_receipts (scope_id TEXT NOT NULL, rebuild_id TEXT NOT NULL, request_json TEXT NOT NULL)")
            try db.execute(sql: "INSERT INTO _synchro_rebuild_page_receipts VALUES ('orders:user-1', 'rebuild-1', '{\"cursor\":\"cursor-1\"}')")
        }
        try legacy.close()

        let db = try SynchroDatabase(path: path)
        XCTAssertEqual(
            try db.query("SELECT mutation_id, lifecycle_state FROM _synchro_pending_changes", params: nil).first?["mutation_id"] as String?,
            "mutation-1"
        )
        XCTAssertEqual(
            try db.query("SELECT batch_id, request_json FROM _synchro_push_batches", params: nil).first?["batch_id"] as String?,
            "batch-1"
        )
        XCTAssertEqual(
            try db.query("SELECT rebuild_id, cursor FROM _synchro_rebuild_attempts WHERE scope_id = 'orders:user-1'", params: nil).first?["cursor"] as String?,
            "cursor-1"
        )
        XCTAssertEqual(
            try db.query("SELECT request_json FROM _synchro_rebuild_page_receipts WHERE rebuild_id = 'rebuild-1'", params: nil).first?["request_json"] as String?,
            "{\"cursor\":\"cursor-1\"}"
        )

        let columns = try db.query("PRAGMA table_info(_synchro_backoff)", params: nil)
            .compactMap { $0["name"] as String? }
        XCTAssertEqual(
            columns,
            ["singleton", "resume_state", "work_identity", "retry_classification", "attempt_count", "next_retry_at_ms"]
        )
        XCTAssertNil(try db.readTransaction { db in
            try SynchroMeta.getBackoffRecord(db)
        })
    }
}
