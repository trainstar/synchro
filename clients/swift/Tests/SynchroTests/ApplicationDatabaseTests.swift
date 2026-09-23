import XCTest
@preconcurrency import GRDB
@testable import Synchro

final class ApplicationDatabaseTests: XCTestCase {
    private func makeDatabase() throws -> SynchroDatabase {
        let path = (NSTemporaryDirectory() as NSString)
            .appendingPathComponent("synchro_application_sql_\(UUID().uuidString).sqlite")
        let database = try SynchroDatabase(path: path)
        try SchemaManager(database: database).reconcileLocalSchema(
            schemaVersion: 1,
            schemaHash: protocolTestSchemaHash,
            tables: try protocolOrdersSchemaManifest().localTables()
        )
        return database
    }

    private func assertDenied(
        _ database: SynchroDatabase,
        _ sql: String,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        XCTAssertThrowsError(
            try database.execute(sql, params: nil),
            file: file,
            line: line
        )
    }

    func testReservedStateRejectsDirectQuotedAliasAndCTEWrites() throws {
        let database = try makeDatabase()
        defer { try? database.close() }

        let syncLockBefore = try database.queryOne(
            "SELECT value FROM _synchro_meta WHERE key = 'sync_lock'",
            params: nil
        )?["value"] as String?

        assertDenied(
            database,
            "UPDATE _synchro_meta SET value = '1' WHERE key = 'sync_lock'"
        )
        assertDenied(
            database,
            "UPDATE \"_SyNcHrO_mEtA\" AS metadata SET value = '1' WHERE metadata.key = 'sync_lock'"
        )
        assertDenied(
            database,
            "WITH protected_keys(key) AS (VALUES ('sync_lock')) DELETE FROM _synchro_meta WHERE key IN (SELECT key FROM protected_keys)"
        )
        assertDenied(
            database,
            "INSERT INTO _synchro_pending_changes (mutation_id) VALUES ('forged')"
        )

        XCTAssertEqual(
            try database.queryOne(
                "SELECT value FROM _synchro_meta WHERE key = 'sync_lock'",
                params: nil
            )?["value"] as String?,
            syncLockBefore
        )
    }

    func testViewsAndTriggersCannotHideReservedWrites() throws {
        let database = try makeDatabase()
        defer { try? database.close() }

        _ = try database.execute(
            "CREATE TABLE local_commands (id TEXT PRIMARY KEY, value TEXT)",
            params: nil
        )
        _ = try database.execute(
            "CREATE VIEW local_sync_lock AS SELECT key, value FROM _synchro_meta WHERE key = 'sync_lock'",
            params: nil
        )

        _ = try database.execute(
            """
            CREATE TRIGGER local_commands_reserved_write
            AFTER INSERT ON local_commands
            BEGIN
                UPDATE _synchro_meta SET value = NEW.value WHERE key = 'sync_lock';
            END
            """,
            params: nil
        )
        _ = try database.execute(
            """
            CREATE TRIGGER local_sync_lock_reserved_write
            INSTEAD OF UPDATE ON local_sync_lock
            BEGIN
                UPDATE _synchro_meta SET value = NEW.value WHERE key = NEW.key;
            END
            """,
            params: nil
        )
        assertDenied(
            database,
            "INSERT INTO local_commands (id, value) VALUES ('c1', '1')"
        )
        assertDenied(database, "UPDATE local_sync_lock SET value = '1'")
        assertDenied(
            database,
            "CREATE VIEW _synchro_hidden_state AS SELECT * FROM local_commands"
        )

        XCTAssertNil(try database.queryOne(
            "SELECT id FROM local_commands WHERE id = 'c1'",
            params: nil
        ))
        XCTAssertEqual(
            try database.queryOne(
                "SELECT value FROM _synchro_meta WHERE key = 'sync_lock'",
                params: nil
            )?["value"] as String?,
            "0"
        )
    }

    func testSyncedDDLReservedIdentifiersAttachAndWritePragmasAreDenied() throws {
        let database = try makeDatabase()
        defer { try? database.close() }

        assertDenied(database, "CREATE TABLE _synchro_forged (id TEXT)")
        assertDenied(database, "ALTER TABLE orders ADD COLUMN forged TEXT")
        assertDenied(database, "DROP TABLE orders")
        assertDenied(database, "CREATE INDEX forged_orders_index ON orders (ship_address)")
        assertDenied(database, "DROP TRIGGER _synchro_cdc_insert_orders")
        assertDenied(database, "PRAGMA writable_schema = ON")
        assertDenied(database, "PRAGMA journal_mode = DELETE")

        let attachedPath = (NSTemporaryDirectory() as NSString)
            .appendingPathComponent("synchro_attached_\(UUID().uuidString).sqlite")
        assertDenied(database, "ATTACH DATABASE '\(attachedPath)' AS auxiliary")

        let tableInfo = try database.applicationWriteTransaction { transaction in
            try transaction.query("PRAGMA table_info(orders)")
        }
        XCTAssertFalse(tableInfo.isEmpty)
        XCTAssertNotNil(try database.queryOne(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'orders'",
            params: nil
        ))
    }

    func testLocalOnlySQLAndResolvedSyncedWritesRemainAuthorized() throws {
        let database = try makeDatabase()
        defer { try? database.close() }

        _ = try database.execute(
            "CREATE TABLE local_notes (id TEXT PRIMARY KEY, body TEXT)",
            params: nil
        )
        _ = try database.execute(
            "CREATE TABLE local_note_audit (id TEXT PRIMARY KEY)",
            params: nil
        )
        _ = try database.execute(
            """
            CREATE TRIGGER local_note_capture
            AFTER INSERT ON local_notes
            BEGIN
                INSERT INTO local_note_audit (id) VALUES (NEW.id);
            END
            """,
            params: nil
        )
        _ = try database.execute(
            "INSERT INTO local_notes (id, body) VALUES ('n1', 'local')",
            params: nil
        )

        _ = try database.execute(
            "INSERT INTO orders (id, ship_address, user_id, updated_at) VALUES ('o1', 'first', 'u1', '2026-01-01T00:00:00.000000Z')",
            params: nil
        )
        _ = try database.execute(
            """
            WITH replacement(value) AS (VALUES ('second'))
            UPDATE orders AS target
            SET ship_address = (SELECT value FROM replacement)
            WHERE target.id = 'o1'
            """,
            params: nil
        )

        XCTAssertEqual(
            try database.queryOne(
                "SELECT COUNT(*) AS count FROM local_note_audit",
                params: nil
            )?["count"] as Int64?,
            1
        )
        XCTAssertEqual(try ChangeTracker(database: database).pendingChangeCount(), 1)
        XCTAssertEqual(
            try database.queryOne(
                "SELECT ship_address FROM orders WHERE id = 'o1'",
                params: nil
            )?["ship_address"] as String?,
            "second"
        )
    }
}
