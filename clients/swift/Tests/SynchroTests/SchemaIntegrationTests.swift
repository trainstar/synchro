import XCTest
import Foundation
#if canImport(CommonCrypto)
import CommonCrypto
#endif
@testable import Synchro

/// Integration tests for schema reconciliation and seed database loading.
/// Requires SYNCHRO_TEST_URL and SYNCHRO_TEST_JWT_SECRET environment variables.
final class SchemaIntegrationTests: XCTestCase {

    private var serverURL: URL!
    private var jwtSecret: String!
    private var canonicalSeedPath: String!

    override func setUpWithError() throws {
        try super.setUpWithError()
        let urlString = try XCTUnwrap(
            ProcessInfo.processInfo.environment["SYNCHRO_TEST_URL"],
            "SYNCHRO_TEST_URL must be set for schema integration tests"
        )
        let secret = try XCTUnwrap(
            ProcessInfo.processInfo.environment["SYNCHRO_TEST_JWT_SECRET"],
            "SYNCHRO_TEST_JWT_SECRET must be set for schema integration tests"
        )
        canonicalSeedPath = try XCTUnwrap(
            ProcessInfo.processInfo.environment["SYNCHRO_TEST_SEED_PATH"],
            "SYNCHRO_TEST_SEED_PATH must be set for schema integration tests"
        )
        guard FileManager.default.fileExists(atPath: canonicalSeedPath) else {
            throw NSError(
                domain: "SchemaIntegrationTests",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "SYNCHRO_TEST_SEED_PATH must point to an existing bundled seed database"]
            )
        }
        serverURL = try XCTUnwrap(
            URL(string: urlString),
            "SYNCHRO_TEST_URL must be a valid URL"
        )
        jwtSecret = secret
    }

    // MARK: - JWT Helper

    private func signTestJWT(userID: String) -> String {
        let header = #"{"alg":"HS256","typ":"JWT"}"#
        let now = Int(Date().timeIntervalSince1970)
        let exp = now + 3600
        let payload = #"{"sub":"\#(userID)","iat":\#(now),"exp":\#(exp)}"#

        let headerB64 = base64URLEncode(Data(header.utf8))
        let payloadB64 = base64URLEncode(Data(payload.utf8))
        let signingInput = "\(headerB64).\(payloadB64)"

        let signature = hmacSHA256(key: Data(jwtSecret.utf8), data: Data(signingInput.utf8))
        return "\(signingInput).\(base64URLEncode(signature))"
    }

    private func base64URLEncode(_ data: Data) -> String {
        data.base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }

    private func hmacSHA256(key: Data, data: Data) -> Data {
        var digest = [UInt8](repeating: 0, count: Int(CC_SHA256_DIGEST_LENGTH))
        key.withUnsafeBytes { keyBytes in
            data.withUnsafeBytes { dataBytes in
                CCHmac(
                    CCHmacAlgorithm(kCCHmacAlgSHA256),
                    keyBytes.baseAddress, key.count,
                    dataBytes.baseAddress, data.count,
                    &digest
                )
            }
        }
        return Data(digest)
    }

    // MARK: - Helpers

    private func tempDBPath() -> String {
        NSTemporaryDirectory() + UUID().uuidString + ".sqlite"
    }

    private func makeConfig(userID: String, dbPath: String, seedPath: String? = nil) -> SynchroConfig {
        let token = signTestJWT(userID: userID)
        return SynchroConfig(
            dbPath: dbPath,
            serverURL: serverURL,
            authProvider: { token },
            clientID: UUID().uuidString.lowercased(),
            appVersion: "1.0.0",
            syncInterval: 999,
            maxRetryAttempts: 1,
            seedDatabasePath: seedPath
        )
    }

    private func makeConfigWithClientID(userID: String, clientID: String, dbPath: String, seedPath: String? = nil) -> SynchroConfig {
        let token = signTestJWT(userID: userID)
        return SynchroConfig(
            dbPath: dbPath,
            serverURL: serverURL,
            authProvider: { token },
            clientID: clientID,
            appVersion: "1.0.0",
            syncInterval: 999,
            maxRetryAttempts: 1,
            seedDatabasePath: seedPath
        )
    }

    /// Fetch the live schema from the test server.
    private func fetchServerSchema() async throws -> SchemaResponse {
        let config = makeConfig(userID: UUID().uuidString.lowercased(), dbPath: tempDBPath())
        let http = HttpClient(config: config)
        return try await http.fetchSchema()
    }

    // MARK: - 1. testAdditiveSchemaChangePreservesData

    func testAdditiveSchemaChangePreservesData() async throws {
        let serverSchema = try await fetchServerSchema()
        let userID = UUID().uuidString.lowercased()
        let clientID = UUID().uuidString.lowercased()
        let dbPath = tempDBPath()

        guard let ordersTable = serverSchema.tables.first(where: { $0.tableName == "orders" }) else {
            return XCTFail("server schema must include 'orders' table")
        }

        // 1. Full initial sync — creates all local tables from server schema
        let config1 = makeConfigWithClientID(userID: userID, clientID: clientID, dbPath: dbPath)
        let client1 = try SynchroClient(config: config1)
        try await client1.start()

        // 2. Insert customer (required FK for orders) and order, push to server
        let custID = UUID().uuidString.lowercased()
        _ = try client1.execute(
            "INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, ?, ?)",
            params: [custID, userID, "Schema Test Customer", "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00.000Z"]
        )
        let orderID = UUID().uuidString.lowercased()
        _ = try client1.execute(
            "INSERT INTO orders (id, customer_id, user_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, ?, 'pending', 0, 'USD', ?, ?, ?)",
            params: [orderID, custID, userID, #"{"street":"123 Main St"}"#, "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00.000Z"]
        )
        try await client1.syncNow()

        await client1.stop()
        try await client1.close()

        // 3. Reconnect with the same client identity and installed schema.
        let config2 = makeConfigWithClientID(userID: userID, clientID: clientID, dbPath: dbPath)
        let client2 = try SynchroClient(config: config2)
        try await client2.start()

        // 4. Data pushed before restart remains available.
        let row = try client2.queryOne("SELECT id, ship_address FROM orders WHERE id = ?", params: [orderID])
        XCTAssertNotNil(row, "pushed data should survive schema reconciliation on reconnect")
        XCTAssertEqual(row?["ship_address"] as? String, #"{"street":"123 Main St"}"#)

        // 5. All server columns still exist locally.
        let colRows = try client2.query("PRAGMA table_info(orders)", params: nil)
        let colNames = Set(colRows.map { $0["name"] as! String })
        for serverCol in ordersTable.columns {
            XCTAssertTrue(colNames.contains(serverCol.name), "column '\(serverCol.name)' should exist after reconciliation")
        }

        await client2.stop()
        try await client2.close()
    }

    // MARK: - 2. testLocalOnlyTablesSurviveReconnect

    func testLocalOnlyTablesSurviveReconnect() async throws {
        let userID = UUID().uuidString.lowercased()
        let clientID = UUID().uuidString.lowercased()
        let dbPath = tempDBPath()

        // Connect, sync (creates synced tables from server schema)
        let client1 = try SynchroClient(config: makeConfigWithClientID(userID: userID, clientID: clientID, dbPath: dbPath))
        try await client1.start()

        // Create a local-only table with data
        try client1.createTable("app_settings", columns: [
            ColumnDef(name: "key", type: "TEXT", nullable: false, primaryKey: true),
            ColumnDef(name: "value", type: "TEXT", nullable: false),
        ])
        _ = try client1.execute(
            "INSERT INTO app_settings (key, value) VALUES (?, ?)",
            params: ["theme", "dark"]
        )
        _ = try client1.execute(
            "INSERT INTO app_settings (key, value) VALUES (?, ?)",
            params: ["locale", "en"]
        )

        await client1.stop()
        try await client1.close()

        // Reconnect with the same client identity.
        let client2 = try SynchroClient(config: makeConfigWithClientID(userID: userID, clientID: clientID, dbPath: dbPath))
        try await client2.start()

        // Verify local-only table and data survived
        let settings = try client2.query("SELECT key, value FROM app_settings ORDER BY key", params: nil)
        XCTAssertEqual(settings.count, 2)
        XCTAssertEqual(settings[0]["key"] as? String, "locale")
        XCTAssertEqual(settings[0]["value"] as? String, "en")
        XCTAssertEqual(settings[1]["key"] as? String, "theme")
        XCTAssertEqual(settings[1]["value"] as? String, "dark")

        await client2.stop()
        try await client2.close()
    }

    // MARK: - 3. portable seed installation

    func testCanonicalGoSeedMigratesAndInstallsWithoutMutatingSource() async throws {
        let dbPath = tempDBPath()
        let sourceBeforeInstall = try Data(contentsOf: URL(fileURLWithPath: canonicalSeedPath))
        let client = try SynchroClient(
            config: makeConfig(
                userID: UUID().uuidString.lowercased(),
                dbPath: dbPath,
                seedPath: canonicalSeedPath
            )
        )

        let migrations = try client.query("SELECT identifier FROM grdb_migrations", params: nil)
        let identifiers = Set(migrations.compactMap { $0["identifier"] as? String })
        XCTAssertTrue(identifiers.contains("synchro_v9_mutation_ledger"))
        XCTAssertTrue(identifiers.contains("synchro_v10_rebuild_page_receipts"))
        XCTAssertTrue(identifiers.contains("synchro_v11_durable_backoff"))
        XCTAssertTrue(identifiers.contains("synchro_v12_gate2_recovery"))
        XCTAssertTrue(identifiers.contains("synchro_v13_scope_text_affinity"))
        XCTAssertEqual(
            try Data(contentsOf: URL(fileURLWithPath: canonicalSeedPath)),
            sourceBeforeInstall
        )
        for suffix in ["-journal", "-wal", "-shm"] {
            XCTAssertFalse(FileManager.default.fileExists(atPath: canonicalSeedPath + suffix))
        }

        try await client.close()
    }

    func testCanonicalSeedWorksOffline() async throws {
        let dbPath = tempDBPath()
        let userID = UUID().uuidString.lowercased()
        let client = try SynchroClient(
            config: makeConfig(userID: userID, dbPath: dbPath, seedPath: canonicalSeedPath)
        )

        // Insert customer (FK required) and order offline — CDC triggers should fire
        let custID = UUID().uuidString.lowercased()
        _ = try client.execute(
            "INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, ?, ?)",
            params: [custID, userID, "Offline Customer", "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00.000Z"]
        )
        let orderID = UUID().uuidString.lowercased()
        _ = try client.execute(
            "INSERT INTO orders (id, customer_id, user_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, ?, 'pending', 0, 'USD', ?, ?, ?)",
            params: [orderID, custID, userID, #"{"street":"456 Oak Ave"}"#, "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00.000Z"]
        )

        // Query back
        let row = try client.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: [orderID])
        XCTAssertNotNil(row)
        XCTAssertEqual(row?["ship_address"] as? String, #"{"street":"456 Oak Ave"}"#)

        // Verify CDC trigger fired (pending change exists)
        let pending = try client.query(
            "SELECT record_id, operation FROM _synchro_pending_changes WHERE table_name = 'orders'",
            params: nil
        )
        XCTAssertEqual(pending.count, 1)
        XCTAssertEqual(pending[0]["record_id"] as? String, orderID)
        XCTAssertEqual(pending[0]["operation"] as? String, "insert")

        try await client.close()
    }

    func testOfflineWritesBeforeFirstConnectArePushedOnFirstSync() async throws {
        let userID = UUID().uuidString.lowercased()
        let clientID = UUID().uuidString.lowercased()
        let dbPath = tempDBPath()
        let customerID = UUID().uuidString.lowercased()

        let offlineClient = try SynchroClient(
            config: makeConfigWithClientID(
                userID: userID,
                clientID: clientID,
                dbPath: dbPath,
                seedPath: canonicalSeedPath
            )
        )

        _ = try offlineClient.execute(
            "INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, ?, ?)",
            params: [customerID, userID, "Offline First Customer", "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00.000Z"]
        )

        let pendingBeforeConnect = try offlineClient.query(
            "SELECT table_name, record_id FROM _synchro_pending_changes ORDER BY table_name, record_id",
            params: nil
        )
        let offlineRow = try offlineClient.queryOne(
            "SELECT name FROM customers WHERE id = ?",
            params: [customerID]
        )
        try await offlineClient.close()

        let onlineClient = try SynchroClient(
            config: makeConfigWithClientID(userID: userID, clientID: clientID, dbPath: dbPath)
        )
        try await onlineClient.start()
        try await onlineClient.syncNow()

        let pendingAfterConnect = try onlineClient.query(
            "SELECT record_id FROM _synchro_pending_changes WHERE lifecycle_state NOT IN ('accepted', 'rejected', 'superseded_before_send', 'cancelled_before_send')",
            params: nil
        )
        let localRow = try onlineClient.queryOne(
            "SELECT name FROM customers WHERE id = ?",
            params: [customerID]
        )
        let rejectedAfterConnect = try onlineClient.query(
            "SELECT mutation_id FROM _synchro_rejected_mutations",
            params: nil
        )
        await onlineClient.stop()
        try await onlineClient.close()

        XCTAssertEqual(pendingBeforeConnect.count, 1)
        XCTAssertEqual(offlineRow?["name"] as? String, "Offline First Customer")
        XCTAssertEqual(pendingAfterConnect.count, 0)
        XCTAssertEqual(localRow?["name"] as? String, "Offline First Customer")
        XCTAssertTrue(rejectedAfterConnect.isEmpty)
    }

    // MARK: - 4. invalid seed rejection

    func testIncompleteSeedIsRejectedBeforeConnect() throws {
        let seedPath = tempDBPath()
        let dbPath = tempDBPath()
        let seed = try SynchroDatabase(path: seedPath)
        try seed.close()

        XCTAssertThrowsError(
            try SynchroClient(
                config: makeConfig(
                    userID: UUID().uuidString.lowercased(),
                    dbPath: dbPath,
                    seedPath: seedPath
                )
            )
        )
        XCTAssertFalse(FileManager.default.fileExists(atPath: dbPath))
        for suffix in ["-journal", "-wal", "-shm"] {
            XCTAssertFalse(FileManager.default.fileExists(atPath: dbPath + suffix))
        }
    }

    // MARK: - 5. testBundledSeedRepairsPortableScopeCorruptionOnConnect

    func testBundledSeedRepairsPortableScopeCorruptionOnConnect() async throws {
        let dbPath = tempDBPath()
        let bootstrap = try SynchroClient(
            config: makeConfig(userID: UUID().uuidString.lowercased(), dbPath: dbPath, seedPath: canonicalSeedPath)
        )

        let seededCategoryID = "10000000-0000-0000-0000-000000000006"
        let seededCategoryName = "Seed Category"

        let seededScope = try bootstrap.readTransaction { db in
            try SynchroMeta.getScope(db, scopeID: "global")
        }
        XCTAssertEqual(seededScope?.scopeID, "global")
        XCTAssertNil(seededScope?.cursor)
        XCTAssertFalse((seededScope?.checksum ?? "").isEmpty)

        let seededRow = try bootstrap.queryOne(
            "SELECT name FROM categories WHERE id = ?",
            params: [seededCategoryID]
        )
        XCTAssertEqual(seededRow?["name"] as? String, seededCategoryName)
        let seedReceipts = try bootstrap.readTransaction { db in
            try SynchroMeta.getSeedReceipts(db)
        }
        XCTAssertNotNil(seedReceipts["global"])

        try await bootstrap.close()

        let rawDb = try SynchroDatabase(path: dbPath)
        try rawDb.writeSyncLockedTransaction { db in
            try SynchroMeta.deleteScopeRow(
                db,
                scopeID: "global",
                tableName: "categories",
                recordID: seededCategoryID
            )
            try db.execute(
                sql: "DELETE FROM categories WHERE id = ?",
                arguments: [seededCategoryID]
            )
        }
        try rawDb.close()

        let client = try SynchroClient(
            config: makeConfig(userID: UUID().uuidString.lowercased(), dbPath: dbPath)
        )
        try await client.start()

        let repairedRow = try client.queryOne(
            "SELECT name FROM categories WHERE id = ?",
            params: [seededCategoryID]
        )
        XCTAssertEqual(repairedRow?["name"] as? String, seededCategoryName)

        let repairedScope = try client.readTransaction { db in
            try SynchroMeta.getScope(db, scopeID: "global")
        }
        XCTAssertEqual(repairedScope?.scopeID, "global")
        XCTAssertFalse((repairedScope?.cursor ?? "").isEmpty)
        XCTAssertFalse((repairedScope?.checksum ?? "").isEmpty)
        let remainingReceipts = try client.readTransaction { db in
            try SynchroMeta.getSeedReceipts(db)
        }
        XCTAssertTrue(remainingReceipts.isEmpty)

        let pendingCount = try client.queryOne(
            "SELECT COUNT(*) AS count FROM _synchro_pending_changes WHERE lifecycle_state NOT IN ('accepted', 'rejected', 'superseded_before_send', 'cancelled_before_send')",
            params: nil
        )
        XCTAssertEqual(pendingCount?["count"] as? Int64, 0)

        await client.stop()
        try await client.close()
    }

    // MARK: - 6. testBundledSeedContinuesIncrementallyWithoutRebuild

    func testBundledSeedContinuesIncrementallyWithoutRebuild() async throws {
        let dbPath = tempDBPath()
        let client = try SynchroClient(
            config: makeConfig(
                userID: UUID().uuidString,
                dbPath: dbPath,
                seedPath: canonicalSeedPath
            )
        )

        let seededCategoryID = "10000000-0000-0000-0000-000000000006"
        let initialScope = try client.readTransaction { db in
            try SynchroMeta.getScope(db, scopeID: "global")
        }
        XCTAssertEqual(initialScope?.scopeID, "global")
        XCTAssertNil(initialScope?.cursor)
        XCTAssertFalse((initialScope?.checksum ?? "").isEmpty)

        let initialGeneration = initialScope?.generation
        let initialCategory = try client.queryOne(
            "SELECT name FROM categories WHERE id = ?",
            params: [seededCategoryID]
        )
        XCTAssertEqual(initialCategory?["name"] as? String, "Seed Category")

        try await client.start()

        let resumedScope = try client.readTransaction { db in
            try SynchroMeta.getScope(db, scopeID: "global")
        }
        XCTAssertEqual(resumedScope?.scopeID, "global")
        XCTAssertEqual(resumedScope?.generation, initialGeneration)
        XCTAssertFalse((resumedScope?.cursor ?? "").isEmpty)
        XCTAssertFalse((resumedScope?.checksum ?? "").isEmpty)

        let resumedCategory = try client.queryOne(
            "SELECT name FROM categories WHERE id = ?",
            params: [seededCategoryID]
        )
        XCTAssertEqual(resumedCategory?["name"] as? String, "Seed Category")

        let pendingCount = try client.queryOne(
            "SELECT COUNT(*) AS count FROM _synchro_pending_changes WHERE lifecycle_state NOT IN ('accepted', 'rejected', 'superseded_before_send', 'cancelled_before_send')",
            params: nil
        )
        XCTAssertEqual(pendingCount?["count"] as? Int64, 0)

        await client.stop()
        try await client.close()
    }

    // MARK: - 7. testGlobalScopeRepairLeavesUserRowsUntouched

    func testGlobalScopeRepairLeavesUserRowsUntouched() async throws {
        let userID = UUID().uuidString.lowercased()
        let clientID = UUID().uuidString.lowercased()
        let dbPath = tempDBPath()
        let seededCategoryID = "10000000-0000-0000-0000-000000000006"
        let customerID = UUID().uuidString.lowercased()
        let orderID = UUID().uuidString.lowercased()

        let bootstrap = try SynchroClient(
            config: makeConfigWithClientID(
                userID: userID,
                clientID: clientID,
                dbPath: dbPath,
                seedPath: canonicalSeedPath
            )
        )

        // Keep the portable receipt pending while local intent is captured.
        _ = try bootstrap.execute(
            "INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, ?, ?)",
            params: [customerID, userID, "Scoped Repair Customer", "2026-01-06T00:00:00.000Z", "2026-01-06T00:00:00.000Z"]
        )
        _ = try bootstrap.execute(
            "INSERT INTO orders (id, customer_id, user_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, ?, 'pending', 0, 'USD', ?, ?, ?)",
            params: [orderID, customerID, userID, #"{"street":"User Scope Row"}"#, "2026-01-06T00:00:00.000Z", "2026-01-06T00:00:00.000Z"]
        )
        try await bootstrap.close()

        let rawDb = try SynchroDatabase(path: dbPath)
        let pendingReceipt = try rawDb.readTransaction { db in
            try SynchroMeta.getSeedReceipts(db)
        }
        XCTAssertNotNil(pendingReceipt["global"])
        try rawDb.writeSyncLockedTransaction { db in
            try SynchroMeta.deleteScopeRow(
                db,
                scopeID: "global",
                tableName: "categories",
                recordID: seededCategoryID
            )
            try db.execute(
                sql: "DELETE FROM categories WHERE id = ?",
                arguments: [seededCategoryID]
            )
        }
        try rawDb.close()

        let client = try SynchroClient(
            config: makeConfigWithClientID(
                userID: userID,
                clientID: clientID,
                dbPath: dbPath
            )
        )
        try await client.start()

        let repairedCategory = try client.queryOne(
            "SELECT name FROM categories WHERE id = ?",
            params: [seededCategoryID]
        )
        XCTAssertEqual(repairedCategory?["name"] as? String, "Seed Category")

        let preservedOrder = try client.queryOne(
            "SELECT ship_address FROM orders WHERE id = ?",
            params: [orderID]
        )
        XCTAssertEqual(preservedOrder?["ship_address"] as? String, #"{"street":"User Scope Row"}"#)

        let remainingReceipts = try client.readTransaction { db in
            try SynchroMeta.getSeedReceipts(db)
        }
        XCTAssertTrue(remainingReceipts.isEmpty)

        let pendingCount = try client.queryOne(
            "SELECT COUNT(*) AS count FROM _synchro_pending_changes WHERE lifecycle_state NOT IN ('accepted', 'rejected', 'superseded_before_send', 'cancelled_before_send')",
            params: nil
        )
        XCTAssertEqual(pendingCount?["count"] as? Int64, 0)

        await client.stop()
        try await client.close()
    }

    // MARK: - 8. testSharedSeedRowsStayInSharedScopeOnly

    func testSharedSeedRowsStayInSharedScopeOnly() async throws {
        let userID = UUID().uuidString.lowercased()
        let clientID = UUID().uuidString.lowercased()
        let dbPath = tempDBPath()
        let seededCategoryID = "10000000-0000-0000-0000-000000000006"
        let customerID = UUID().uuidString.lowercased()
        let orderID = UUID().uuidString.lowercased()

        let client = try SynchroClient(
            config: makeConfigWithClientID(
                userID: userID,
                clientID: clientID,
                dbPath: dbPath,
                seedPath: canonicalSeedPath
            )
        )

        try await client.start()
        _ = try client.execute(
            "INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, ?, ?)",
            params: [customerID, userID, "Shared Scope Customer", "2026-01-07T00:00:00.000Z", "2026-01-07T00:00:00.000Z"]
        )
        _ = try client.execute(
            "INSERT INTO orders (id, customer_id, user_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, ?, 'pending', 0, 'USD', ?, ?, ?)",
            params: [orderID, customerID, userID, #"{"street":"User Scoped Order"}"#, "2026-01-07T00:00:00.000Z", "2026-01-07T00:00:00.000Z"]
        )
        try await client.syncNow()

        let categoryScopes = try client.query(
            """
            SELECT scope_id
            FROM _synchro_scope_rows
            WHERE table_name = 'categories' AND record_id = ?
            ORDER BY scope_id
            """,
            params: [seededCategoryID]
        )
        XCTAssertEqual(categoryScopes.count, 1)
        XCTAssertEqual(categoryScopes.first?["scope_id"] as? String, "global")

        let duplicatedCategoryScopes = try client.queryOne(
            """
            SELECT COUNT(*) AS count
            FROM _synchro_scope_rows
            WHERE table_name = 'categories' AND record_id = ? AND scope_id != 'global'
            """,
            params: [seededCategoryID]
        )
        XCTAssertEqual(duplicatedCategoryScopes?["count"] as? Int64, 0)

        let orderRow = try client.queryOne(
            "SELECT ship_address FROM orders WHERE id = ?",
            params: [orderID]
        )
        XCTAssertEqual(orderRow?["ship_address"] as? String, #"{"street":"User Scoped Order"}"#)

        await client.stop()
        try await client.close()
    }
}

final class ClientSchemaIdentityTests: XCTestCase {
    private struct CatalogEntry: Equatable {
        let type: String
        let name: String
        let tableName: String
        let sql: String
    }

    func testCanonicalGoSeedDDLConvergesWithFreshSwiftDDL() throws {
        let migratedPath = NSTemporaryDirectory() + UUID().uuidString + ".sqlite"
        let freshPath = NSTemporaryDirectory() + UUID().uuidString + ".sqlite"
        var clientsDirectory = URL(fileURLWithPath: #filePath)
        for _ in 0..<4 {
            clientsDirectory.deleteLastPathComponent()
        }
        let seedPath = clientsDirectory
            .appendingPathComponent("swift/.build/test-results/schema-identity-seed.db")
            .path
        let sourceBytes = try Data(contentsOf: URL(fileURLWithPath: seedPath))
        addTeardownBlock {
            for path in [migratedPath, freshPath] {
                for suffix in ["", "-journal", "-wal", "-shm"] {
                    try? FileManager.default.removeItem(atPath: path + suffix)
                }
            }
        }

        try SeedDatabaseInstaller.installIfNeeded(
            seedPath: seedPath,
            databasePath: migratedPath
        )
        let migrated = try SynchroDatabase(path: migratedPath)
        let fresh = try SynchroDatabase(path: freshPath)
        defer {
            try? migrated.close()
            try? fresh.close()
        }

        let tables = try XCTUnwrap(SchemaManager(database: migrated).loadStoredLocalSchema())
        try fresh.writeTransaction { db in
            try SchemaManager(database: fresh).createSyncedTablesInTransaction(
                db,
                tables: tables
            )
        }

        XCTAssertEqual(
            Set(try migrated.query("SELECT identifier FROM grdb_migrations", params: nil).map { row in
                row["identifier"] as String
            }),
            Set(try fresh.query("SELECT identifier FROM grdb_migrations", params: nil).map { row in
                row["identifier"] as String
            })
        )
        let expected = try schemaCatalog(fresh)
        XCTAssertEqual(try schemaCatalog(migrated), expected)

        try migrated.writeTransaction { db in
            try db.execute(sql: "ALTER TABLE _synchro_scopes ADD COLUMN ddl_identity_drift TEXT")
        }
        XCTAssertNotEqual(try schemaCatalog(migrated), expected)
        XCTAssertEqual(try Data(contentsOf: URL(fileURLWithPath: seedPath)), sourceBytes)
        for suffix in ["-journal", "-wal", "-shm"] {
            XCTAssertFalse(FileManager.default.fileExists(atPath: seedPath + suffix))
        }
    }

    private func schemaCatalog(_ database: SynchroDatabase) throws -> [CatalogEntry] {
        try database.query(
            """
            SELECT type, name, tbl_name, sql
            FROM sqlite_master
            WHERE type IN ('table', 'index', 'trigger')
              AND name NOT LIKE 'sqlite_%'
              AND name != 'grdb_migrations'
              AND sql IS NOT NULL
            ORDER BY type, name
            """,
            params: nil
        ).map { row in
            CatalogEntry(
                type: row["type"],
                name: row["name"],
                tableName: row["tbl_name"],
                sql: canonicalDDL(row["sql"])
            )
        }
    }

    private func canonicalDDL(_ source: String) -> String {
        let characters = Array(source)
        var output = ""
        var pendingSpace = false
        var quoteEnd: Character?
        var index = 0
        while index < characters.count {
            let character = characters[index]
            if let activeQuote = quoteEnd {
                output.append(character)
                if character == activeQuote {
                    if index + 1 < characters.count, characters[index + 1] == activeQuote {
                        index += 1
                        output.append(characters[index])
                    } else {
                        quoteEnd = nil
                    }
                }
                index += 1
                continue
            }
            if character.isWhitespace {
                pendingSpace = !output.isEmpty
                index += 1
                continue
            }
            if pendingSpace,
               let previous = output.last,
               !"(,".contains(previous),
               !"),;".contains(character) {
                output.append(" ")
            }
            pendingSpace = false
            switch character {
            case "'", "\"", "`": quoteEnd = character
            case "[": quoteEnd = "]"
            default: break
            }
            output.append(character)
            index += 1
        }
        return output
    }

}
