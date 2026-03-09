import XCTest
@testable import Synchro

final class SynchroClientTests: XCTestCase {
    private func makeConfig() -> SynchroConfig {
        let tmpDir = NSTemporaryDirectory()
        let path = (tmpDir as NSString).appendingPathComponent("synchro_client_test_\(UUID().uuidString).sqlite")
        return SynchroConfig(
            dbPath: path,
            serverURL: URL(string: "http://localhost:8080")!,
            authProvider: { "test-token" },
            clientID: "test-device",
            appVersion: "1.0.0"
        )
    }

    func testCreateIndex() throws {
        let config = makeConfig()
        let client = try SynchroClient(config: config)

        try client.createTable("orders", columns: [
            ColumnDef(name: "id", type: "TEXT", nullable: false, primaryKey: true),
            ColumnDef(name: "category", type: "TEXT"),
        ])

        try client.createIndex("orders", columns: ["category"], unique: false)

        let indexes = try client.query("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='orders'")
        let names = indexes.map { $0["name"] as String }
        XCTAssertTrue(names.contains("idx_orders_category"))

        try client.close()
    }

    func testOnChange() throws {
        let config = makeConfig()
        let client = try SynchroClient(config: config)

        try client.createTable("events", columns: [
            ColumnDef(name: "id", type: "TEXT", nullable: false, primaryKey: true),
            ColumnDef(name: "name", type: "TEXT"),
        ])

        let expectation = XCTestExpectation(description: "onChange fires")
        let cancellable = client.onChange(tables: ["events"]) {
            expectation.fulfill()
        }

        _ = try client.execute("INSERT INTO events (id, name) VALUES (?, ?)", params: ["e1", "test"])

        wait(for: [expectation], timeout: 2.0)
        cancellable.cancel()
        try client.close()
    }

    func testWatch() throws {
        let config = makeConfig()
        let client = try SynchroClient(config: config)

        try client.createTable("counters", columns: [
            ColumnDef(name: "id", type: "TEXT", nullable: false, primaryKey: true),
            ColumnDef(name: "value", type: "INTEGER"),
        ])

        // Insert initial row
        _ = try client.execute("INSERT INTO counters (id, value) VALUES (?, ?)", params: ["c1", 0])

        let expectation = XCTestExpectation(description: "watch fires with updated data")
        expectation.expectedFulfillmentCount = 2 // initial + after update

        var receivedRows: [[Row]] = []
        let cancellable = client.watch(
            "SELECT * FROM counters WHERE id = ?",
            params: ["c1"],
            tables: ["counters"]
        ) { rows in
            receivedRows.append(rows)
            expectation.fulfill()
        }

        // Trigger an update
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.3) {
            _ = try? client.execute("UPDATE counters SET value = ? WHERE id = ?", params: [42, "c1"])
        }

        wait(for: [expectation], timeout: 3.0)

        XCTAssertGreaterThanOrEqual(receivedRows.count, 2)
        // Last callback should have the updated value
        if let lastRows = receivedRows.last, let row = lastRows.first {
            XCTAssertEqual(row["value"] as Int, 42)
        }

        cancellable.cancel()
        try client.close()
    }

    func testAlterTable() throws {
        let config = makeConfig()
        let client = try SynchroClient(config: config)

        try client.createTable("people", columns: [
            ColumnDef(name: "id", type: "TEXT", nullable: false, primaryKey: true),
            ColumnDef(name: "name", type: "TEXT"),
        ])

        try client.alterTable("people", addColumns: [
            ColumnDef(name: "age", type: "INTEGER"),
        ])

        _ = try client.execute("INSERT INTO people (id, name, age) VALUES (?, ?, ?)", params: ["p1", "Alice", 30])
        let row = try client.queryOne("SELECT age FROM people WHERE id = ?", params: ["p1"])
        XCTAssertEqual(row?["age"] as Int?, 30)

        try client.close()
    }

    func testTransactions() throws {
        let config = makeConfig()
        let client = try SynchroClient(config: config)

        try client.createTable("txtest", columns: [
            ColumnDef(name: "id", type: "TEXT", nullable: false, primaryKey: true),
            ColumnDef(name: "val", type: "TEXT"),
        ])

        // Write transaction
        let written = try client.writeTransaction { db -> Int in
            try db.execute(sql: "INSERT INTO txtest (id, val) VALUES (?, ?)", arguments: ["t1", "hello"])
            return db.changesCount
        }
        XCTAssertEqual(written, 1)

        // Read transaction
        let value = try client.readTransaction { db -> String? in
            let row = try Row.fetchOne(db, sql: "SELECT val FROM txtest WHERE id = ?", arguments: ["t1"])
            return row?["val"]
        }
        XCTAssertEqual(value, "hello")

        try client.close()
    }

}
