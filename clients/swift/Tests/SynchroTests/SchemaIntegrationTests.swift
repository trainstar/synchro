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
            clientID: UUID().uuidString,
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
        let config = makeConfig(userID: UUID().uuidString, dbPath: tempDBPath())
        let http = HttpClient(config: config)
        return try await http.fetchSchema()
    }

    /// Create a copy of a SchemaTable with different columns.
    private func withColumns(_ table: SchemaTable, columns: [SchemaColumn]) -> SchemaTable {
        SchemaTable(
            tableName: table.tableName,
            updatedAtColumn: table.updatedAtColumn,
            deletedAtColumn: table.deletedAtColumn,
            primaryKey: table.primaryKey,
            columns: columns
        )
    }

    /// Create a seed database file with the given tables.
    /// Returns the path to the seed file.
    private func createSeedDB(tables: [SchemaTable], schemaVersion: Int64 = 1, schemaHash: String = "seed") throws -> String {
        let seedPath = tempDBPath()
        let db = try SynchroDatabase(path: seedPath)
        let schemaManager = SchemaManager(database: db)
        let schema = SchemaResponse(schemaVersion: schemaVersion, schemaHash: schemaHash, serverTime: Date(), tables: tables)
        try schemaManager.createSyncedTables(schema: schema)
        try db.writeTransaction { grdb in
            try SynchroMeta.setInt64(grdb, key: .schemaVersion, value: schemaVersion)
            try SynchroMeta.set(grdb, key: .schemaHash, value: schemaHash)
        }
        try db.close()
        return seedPath
    }

    // MARK: - 1. testAdditiveSchemaChangePreservesData

    func testAdditiveSchemaChangePreservesData() async throws {
        let serverSchema = try await fetchServerSchema()
        let userID = UUID().uuidString.lowercased()
        let clientID = UUID().uuidString
        let dbPath = tempDBPath()

        guard let ordersTable = serverSchema.tables.first(where: { $0.tableName == "orders" }) else {
            return XCTFail("server schema must include 'orders' table")
        }

        // 1. Full initial sync — creates all local tables from server schema
        let config1 = makeConfigWithClientID(userID: userID, clientID: clientID, dbPath: dbPath)
        let client1 = try SynchroClient(config: config1)
        try await client1.start()

        // 2. Insert customer (required FK for orders) and order, push to server
        let custID = UUID().uuidString
        _ = try client1.execute(
            "INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, ?, ?)",
            params: [custID, userID, "Schema Test Customer", "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00.000Z"]
        )
        let orderID = UUID().uuidString
        _ = try client1.execute(
            "INSERT INTO orders (id, customer_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, 'pending', 0, 'USD', ?, ?, ?)",
            params: [orderID, custID, "123 Main St", "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00.000Z"]
        )
        try await client1.syncNow()

        client1.stop()
        try client1.close()

        // 3. Force schema re-fetch by resetting version/hash
        let rawDb = try SynchroDatabase(path: dbPath)
        _ = try rawDb.execute("UPDATE _synchro_meta SET value = '0' WHERE key = 'schema_version'", params: nil)
        _ = try rawDb.execute("UPDATE _synchro_meta SET value = '' WHERE key = 'schema_hash'", params: nil)
        try rawDb.close()

        // 4. Reconnect with SAME clientID — schema re-fetched, reconciled additively, incremental pull
        let config2 = makeConfigWithClientID(userID: userID, clientID: clientID, dbPath: dbPath)
        let client2 = try SynchroClient(config: config2)
        try await client2.start()

        // 5. Data pushed in step 2 should still be there (persisted on server, pulled back)
        let row = try client2.queryOne("SELECT id, ship_address FROM orders WHERE id = ?", params: [orderID])
        XCTAssertNotNil(row, "pushed data should survive schema reconciliation on reconnect")
        XCTAssertEqual(row?["ship_address"] as? String, "123 Main St")

        // 6. All server columns still exist locally
        let colRows = try client2.query("PRAGMA table_info(orders)", params: nil)
        let colNames = Set(colRows.map { $0["name"] as! String })
        for serverCol in ordersTable.columns {
            XCTAssertTrue(colNames.contains(serverCol.name), "column '\(serverCol.name)' should exist after reconciliation")
        }

        client2.stop()
        try client2.close()
    }

    // MARK: - 2. testLocalOnlyTablesSurviveReconnect

    func testLocalOnlyTablesSurviveReconnect() async throws {
        let userID = UUID().uuidString
        let dbPath = tempDBPath()

        // Connect, sync (creates synced tables from server schema)
        let client1 = try SynchroClient(config: makeConfig(userID: userID, dbPath: dbPath))
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

        client1.stop()
        try client1.close()

        // Force schema re-check by resetting hash
        let rawDb = try SynchroDatabase(path: dbPath)
        _ = try rawDb.execute(
            "UPDATE _synchro_meta SET value = '' WHERE key = 'schema_hash'",
            params: nil
        )
        try rawDb.close()

        // Reconnect — schema re-fetched and reconciled
        let client2 = try SynchroClient(config: makeConfig(userID: userID, dbPath: dbPath))
        try await client2.start()

        // Verify local-only table and data survived
        let settings = try client2.query("SELECT key, value FROM app_settings ORDER BY key", params: nil)
        XCTAssertEqual(settings.count, 2)
        XCTAssertEqual(settings[0]["key"] as? String, "locale")
        XCTAssertEqual(settings[0]["value"] as? String, "en")
        XCTAssertEqual(settings[1]["key"] as? String, "theme")
        XCTAssertEqual(settings[1]["value"] as? String, "dark")

        client2.stop()
        try client2.close()
    }

    // MARK: - 3. testSeedDatabaseWorksOffline

    func testSeedDatabaseWorksOffline() async throws {
        // Fetch server schema to build a correct seed
        let serverSchema = try await fetchServerSchema()
        let seedPath = try createSeedDB(
            tables: serverSchema.tables,
            schemaVersion: serverSchema.schemaVersion,
            schemaHash: serverSchema.schemaHash
        )

        // Create SynchroClient with seed — do NOT call start()
        let dbPath = tempDBPath()
        let client = try SynchroClient(config: makeConfig(userID: UUID().uuidString, dbPath: dbPath, seedPath: seedPath))

        // Tables should be queryable immediately (offline)
        let orders = try client.query("SELECT * FROM orders", params: nil)
        XCTAssertEqual(orders.count, 0, "seed DB should have empty tables")

        // Insert customer (FK required) and order offline — CDC triggers should fire
        let custID = UUID().uuidString
        _ = try client.execute(
            "INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, ?, ?)",
            params: [custID, UUID().uuidString, "Offline Customer", "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00.000Z"]
        )
        let orderID = UUID().uuidString
        _ = try client.execute(
            "INSERT INTO orders (id, customer_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, 'pending', 0, 'USD', ?, ?, ?)",
            params: [orderID, custID, "456 Oak Ave", "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00.000Z"]
        )

        // Query back
        let row = try client.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: [orderID])
        XCTAssertNotNil(row)
        XCTAssertEqual(row?["ship_address"] as? String, "456 Oak Ave")

        // Verify CDC trigger fired (pending change exists)
        let pending = try client.query(
            "SELECT record_id, operation FROM _synchro_pending_changes WHERE table_name = 'orders'",
            params: nil
        )
        XCTAssertEqual(pending.count, 1)
        XCTAssertEqual(pending[0]["record_id"] as? String, orderID)
        XCTAssertEqual(pending[0]["operation"] as? String, "create")

        try client.close()
    }

    func testOfflineWritesBeforeFirstConnectArePushedOnFirstSync() async throws {
        let userID = UUID().uuidString.lowercased()
        let clientID = UUID().uuidString
        let dbPath = tempDBPath()
        let customerID = UUID().uuidString

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
        try offlineClient.close()

        let onlineClient = try SynchroClient(
            config: makeConfigWithClientID(userID: userID, clientID: clientID, dbPath: dbPath)
        )
        try await onlineClient.start()
        try await onlineClient.syncNow()

        let pendingAfterConnect = try onlineClient.query(
            "SELECT record_id FROM _synchro_pending_changes",
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
        onlineClient.stop()
        try onlineClient.close()

        XCTAssertEqual(pendingBeforeConnect.count, 1)
        XCTAssertEqual(offlineRow?["name"] as? String, "Offline First Customer")
        XCTAssertEqual(pendingAfterConnect.count, 0)
        XCTAssertEqual(localRow?["name"] as? String, "Offline First Customer")
        XCTAssertTrue(rejectedAfterConnect.isEmpty)
    }

    // MARK: - 4. testSeedDatabaseReconcilesOnConnect

    func testSeedDatabaseReconcilesOnConnect() async throws {
        let serverSchema = try await fetchServerSchema()

        guard let ordersTable = serverSchema.tables.first(where: { $0.tableName == "orders" }) else {
            return XCTFail("server schema must include 'orders' table")
        }

        // Create a STALE seed: only the orders table with minimal columns (no other tables)
        let minimalColumns = ordersTable.columns.filter {
            ["id", "customer_id", "ship_address", "updated_at", "deleted_at"].contains($0.name)
        }
        let staleOrders = withColumns(ordersTable, columns: minimalColumns)
        let seedPath = try createSeedDB(tables: [staleOrders], schemaVersion: 0, schemaHash: "stale-seed")

        // Create SynchroClient with stale seed, then connect to server
        let dbPath = tempDBPath()
        let client = try SynchroClient(config: makeConfig(userID: UUID().uuidString, dbPath: dbPath, seedPath: seedPath))
        try await client.start()

        // Verify missing columns were added by reconciliation
        let colRows = try client.query("PRAGMA table_info(orders)", params: nil)
        let colNames = Set(colRows.map { $0["name"] as! String })
        for serverCol in ordersTable.columns {
            XCTAssertTrue(colNames.contains(serverCol.name), "column '\(serverCol.name)' should be added by reconciliation")
        }

        // Verify tables from server that weren't in the seed were also created
        for serverTable in serverSchema.tables {
            let tableRows = try client.query(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                params: [serverTable.tableName]
            )
            XCTAssertEqual(tableRows.count, 1, "table '\(serverTable.tableName)' should exist after reconciliation")
        }

        // Verify sync works — can insert and push after reconciliation
        let custID = UUID().uuidString
        _ = try client.execute(
            "INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, ?, ?)",
            params: [custID, UUID().uuidString, "Reconcile Customer", "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00.000Z"]
        )
        let orderID = UUID().uuidString
        _ = try client.execute(
            "INSERT INTO orders (id, customer_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, 'pending', 0, 'USD', ?, ?, ?)",
            params: [orderID, custID, "post-reconcile insert", "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00.000Z"]
        )
        let row = try client.queryOne("SELECT ship_address FROM orders WHERE id = ?", params: [orderID])
        XCTAssertNotNil(row)
        XCTAssertEqual(row?["ship_address"] as? String, "post-reconcile insert")

        client.stop()
        try client.close()
    }

    // MARK: - 5. testBundledSeedRepairsPortableScopeCorruptionOnConnect

    func testBundledSeedRepairsPortableScopeCorruptionOnConnect() async throws {
        let dbPath = tempDBPath()
        let bootstrap = try SynchroClient(
            config: makeConfig(userID: UUID().uuidString, dbPath: dbPath, seedPath: canonicalSeedPath)
        )

        let seededCategoryID = "10000000-0000-0000-0000-000000000006"
        let seededCategoryName = "Seed Category"

        let seededScope = try bootstrap.readTransaction { db in
            try SynchroMeta.getScope(db, scopeID: "global")
        }
        XCTAssertEqual(seededScope?.scopeID, "global")
        XCTAssertFalse((seededScope?.cursor ?? "").isEmpty)
        XCTAssertFalse((seededScope?.checksum ?? "").isEmpty)

        let seededRow = try bootstrap.queryOne(
            "SELECT name FROM categories WHERE id = ?",
            params: [seededCategoryID]
        )
        XCTAssertEqual(seededRow?["name"] as? String, seededCategoryName)

        try bootstrap.close()

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
            config: makeConfig(userID: UUID().uuidString, dbPath: dbPath)
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

        let pendingCount = try client.queryOne(
            "SELECT COUNT(*) AS count FROM _synchro_pending_changes",
            params: nil
        )
        XCTAssertEqual(pendingCount?["count"] as? Int64, 0)

        client.stop()
        try client.close()
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
        XCTAssertFalse((initialScope?.cursor ?? "").isEmpty)
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
            "SELECT COUNT(*) AS count FROM _synchro_pending_changes",
            params: nil
        )
        XCTAssertEqual(pendingCount?["count"] as? Int64, 0)

        client.stop()
        try client.close()
    }

    // MARK: - 7. testGlobalScopeRepairLeavesUserRowsUntouched

    func testGlobalScopeRepairLeavesUserRowsUntouched() async throws {
        let userID = UUID().uuidString
        let clientID = UUID().uuidString
        let dbPath = tempDBPath()
        let seededCategoryID = "10000000-0000-0000-0000-000000000006"
        let customerID = UUID().uuidString
        let orderID = UUID().uuidString

        let bootstrap = try SynchroClient(
            config: makeConfigWithClientID(
                userID: userID,
                clientID: clientID,
                dbPath: dbPath,
                seedPath: canonicalSeedPath
            )
        )

        try await bootstrap.start()
        _ = try bootstrap.execute(
            "INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, ?, ?)",
            params: [customerID, userID, "Scoped Repair Customer", "2026-01-06T00:00:00.000Z", "2026-01-06T00:00:00.000Z"]
        )
        _ = try bootstrap.execute(
            "INSERT INTO orders (id, customer_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, 'pending', 0, 'USD', ?, ?, ?)",
            params: [orderID, customerID, "User Scope Row", "2026-01-06T00:00:00.000Z", "2026-01-06T00:00:00.000Z"]
        )
        try await bootstrap.syncNow()
        bootstrap.stop()
        try bootstrap.close()

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
        XCTAssertEqual(preservedOrder?["ship_address"] as? String, "User Scope Row")

        let pendingCount = try client.queryOne(
            "SELECT COUNT(*) AS count FROM _synchro_pending_changes",
            params: nil
        )
        XCTAssertEqual(pendingCount?["count"] as? Int64, 0)

        client.stop()
        try client.close()
    }

    // MARK: - 8. testSharedSeedRowsStayInSharedScopeOnly

    func testSharedSeedRowsStayInSharedScopeOnly() async throws {
        let userID = UUID().uuidString.lowercased()
        let clientID = UUID().uuidString
        let dbPath = tempDBPath()
        let seededCategoryID = "10000000-0000-0000-0000-000000000006"
        let customerID = UUID().uuidString
        let orderID = UUID().uuidString

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
            "INSERT INTO orders (id, customer_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, 'pending', 0, 'USD', ?, ?, ?)",
            params: [orderID, customerID, "User Scoped Order", "2026-01-07T00:00:00.000Z", "2026-01-07T00:00:00.000Z"]
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
        XCTAssertEqual(orderRow?["ship_address"] as? String, "User Scoped Order")

        client.stop()
        try client.close()
    }
}
