import SQLite3
import Synchro
@testable import SynchroReactNative
import XCTest

private let settlementTimeout: TimeInterval = 60
private let promptDispatchTimeout: TimeInterval = 4
private let sqliteTransient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
private let columnsJSON = #"[{"name":"id","type":"TEXT","primaryKey":true},{"name":"name","type":"TEXT"}]"#
private let insertSQL = "INSERT INTO bridge_items (id, name) VALUES (?, ?)"
private let selectNameSQL = "SELECT name FROM bridge_items WHERE id = ?"

/// Records every settlement of one bridge promise, including a settlement after the first.
private final class PromiseRecorder: @unchecked Sendable {
    let name: String
    let firstSettlement: XCTestExpectation
    private let lock = NSLock()
    private var resolvedValues: [Any?] = []
    private var rejectedCodes: [String?] = []
    private var rejectedErrors: [Error?] = []

    init(_ name: String) {
        self.name = name
        firstSettlement = XCTestExpectation(description: "\(name) settles")
    }

    var resolve: (Any?) -> Void {
        { [self] value in record { resolvedValues.append(value) } }
    }

    var reject: (String?, String?, Error?) -> Void {
        { [self] code, _, error in
            record {
                rejectedCodes.append(code)
                rejectedErrors.append(error)
            }
        }
    }

    var resolutions: [Any?] { lock.withLock { resolvedValues } }
    var rejections: [String?] { lock.withLock { rejectedCodes } }
    var rejectionErrors: [Error?] { lock.withLock { rejectedErrors } }
    var settlementCount: Int { lock.withLock { resolvedValues.count + rejectedCodes.count } }

    private func record(_ change: () -> Void) {
        let first = lock.withLock { () -> Bool in
            let first = resolvedValues.isEmpty && rejectedCodes.isEmpty
            change()
            return first
        }
        if first {
            firstSettlement.fulfill()
        }
    }
}

/// Records every module event that reaches the existing event delegate.
private final class EventRecorder: NSObject, SynchroEventEmitting {
    private let lock = NSLock()
    private var recorded: [String] = []

    func emitEvent(_ name: String, body: NSDictionary) {
        lock.withLock { recorded.append(name) }
    }

    var names: [String] { lock.withLock { recorded } }
}

/// Records a cleanup failure so teardown keeps the database files for diagnosis.
private final class CleanupState: @unchecked Sendable {
    private let lock = NSLock()
    private var failed = false

    func recordFailure() {
        lock.withLock { failed = true }
    }

    var hasFailure: Bool { lock.withLock { failed } }
}

/// Holds the first native change callback until the test releases it. Later callbacks pass.
private final class ChangeHold: @unchecked Sendable {
    let entered = XCTestExpectation(description: "native change callback runs")
    private let condition = NSCondition()
    private var entries = 0
    private var released = false
    private var returnedAfterRelease = false

    func enter() {
        condition.lock()
        entries += 1
        let first = entries == 1
        condition.unlock()
        guard first else { return }
        entered.fulfill()
        condition.lock()
        let deadline = Date().addingTimeInterval(settlementTimeout)
        while !released && condition.wait(until: deadline) {}
        returnedAfterRelease = released
        condition.unlock()
    }

    func release() {
        condition.lock()
        released = true
        condition.broadcast()
        condition.unlock()
    }

    var entryCount: Int {
        condition.lock()
        defer { condition.unlock() }
        return entries
    }

    var heldUntilRelease: Bool {
        condition.lock()
        defer { condition.unlock() }
        return returnedAfterRelease
    }
}

private struct SQLiteFailure: Error, CustomStringConvertible {
    let code: Int32
    let message: String

    var description: String { "SQLite error \(code): \(message)" }
}

private struct CleanupFailures: Error, CustomStringConvertible {
    let failures: [SQLiteFailure]

    var description: String { failures.map(\.description).joined(separator: "; ") }
}

/// A test-owned SQLite connection to the module database file.
private final class ExternalConnection {
    private var handle: OpaquePointer?
    private var holdsWriteLock = false

    init(path: String, flags: Int32 = SQLITE_OPEN_READWRITE) throws {
        let code = sqlite3_open_v2(path, &handle, flags, nil)
        guard code == SQLITE_OK else {
            let failure = SQLiteFailure(code: code, message: Self.message(handle))
            let closeCode = sqlite3_close(handle)
            guard closeCode == SQLITE_OK else {
                throw CleanupFailures(failures: [failure, SQLiteFailure(code: closeCode, message: "close after failed open")])
            }
            handle = nil
            throw failure
        }
        // A zero timeout makes a write-lock attempt report whether another connection writes now.
        sqlite3_busy_timeout(handle, 0)
    }

    func acquireWriteLock() throws {
        try execute("BEGIN IMMEDIATE")
        holdsWriteLock = true
    }

    func releaseWriteLock() throws {
        guard holdsWriteLock else { return }
        try execute("ROLLBACK")
        holdsWriteLock = false
    }

    func names(id: String) throws -> [String] {
        try strings(selectNameSQL, binding: id)
    }

    func rowCount() throws -> Int {
        let values = try strings("SELECT count(*) FROM bridge_items", binding: nil)
        guard values.count == 1, let count = Int(values[0]) else {
            throw SQLiteFailure(code: SQLITE_MISMATCH, message: "row count query returned \(values)")
        }
        return count
    }

    /// Attempts rollback and close. A failed close keeps the handle and the lock state.
    func close() throws {
        guard let handle else { return }
        var failures: [SQLiteFailure] = []
        if holdsWriteLock {
            let code = sqlite3_exec(handle, "ROLLBACK", nil, nil, nil)
            if code == SQLITE_OK {
                holdsWriteLock = false
            } else {
                failures.append(SQLiteFailure(code: code, message: Self.message(handle)))
            }
        }
        let code = sqlite3_close(handle)
        if code == SQLITE_OK {
            // Closing a connection also rolls back its open transaction.
            self.handle = nil
            holdsWriteLock = false
        } else {
            failures.append(SQLiteFailure(code: code, message: Self.message(handle)))
        }
        if !failures.isEmpty {
            throw CleanupFailures(failures: failures)
        }
    }

    func execute(_ sql: String) throws {
        let code = sqlite3_exec(handle, sql, nil, nil, nil)
        guard code == SQLITE_OK else {
            throw SQLiteFailure(code: code, message: Self.message(handle))
        }
    }

    func strings(_ sql: String, binding value: String?) throws -> [String] {
        var statement: OpaquePointer?
        let prepared = sqlite3_prepare_v2(handle, sql, -1, &statement, nil)
        guard prepared == SQLITE_OK else {
            throw SQLiteFailure(code: prepared, message: Self.message(handle))
        }
        defer { sqlite3_finalize(statement) }
        if let value {
            let bound = sqlite3_bind_text(statement, 1, value, -1, sqliteTransient)
            guard bound == SQLITE_OK else {
                throw SQLiteFailure(code: bound, message: Self.message(handle))
            }
        }
        var values: [String] = []
        while true {
            let code = sqlite3_step(statement)
            if code == SQLITE_DONE {
                return values
            }
            guard code == SQLITE_ROW, let text = sqlite3_column_text(statement, 0) else {
                throw SQLiteFailure(code: code, message: Self.message(handle))
            }
            values.append(String(cString: text))
        }
    }

    private static func message(_ handle: OpaquePointer?) -> String {
        handle.flatMap { sqlite3_errmsg($0) }.map { String(cString: $0) } ?? "no connection"
    }
}

final class SynchroModuleTransactionTests: XCTestCase {
    private var module: SynchroModuleImpl!
    private var events: EventRecorder!
    private var cleanup: CleanupState!
    private var databasePath: String!
    private var externalConnections: [ExternalConnection] = []

    override func setUpWithError() throws {
        try super.setUpWithError()
        let module = SynchroModuleImpl()
        let events = EventRecorder()
        let cleanup = CleanupState()
        let databasePath = (NSTemporaryDirectory() as NSString)
            .appendingPathComponent("synchro-bridge-\(UUID().uuidString).sqlite")
        module.eventDelegate = events
        self.module = module
        self.events = events
        self.cleanup = cleanup
        self.databasePath = databasePath
    }

    override func tearDownWithError() throws {
        let teardownDatabasePath = databasePath ?? "unknown database path"
        var moduleClosed = false
        for connection in externalConnections.reversed() {
            do {
                try connection.close()
            } catch {
                cleanup.recordFailure()
                XCTFail("close external connection: \(error)")
            }
        }
        externalConnections.removeAll()

        if let module {
            let close = PromiseRecorder("teardown close")
            module.close(close.resolve, reject: close.reject)
            let settled = XCTWaiter().wait(for: [close.firstSettlement], timeout: settlementTimeout) == .completed
            moduleClosed = settled && close.resolutions.count == 1 && close.rejections.isEmpty
            if !moduleClosed {
                XCTFail("teardown close did not resolve once (settled: \(settled), resolutions: \(close.resolutions.count), rejections: \(close.rejections)). Kept \(teardownDatabasePath).")
            }
            module.eventDelegate = nil
        }
        withExtendedLifetime(events) {}

        if cleanup.hasFailure {
            XCTFail("a test-owned resource did not close. Kept \(teardownDatabasePath).")
        } else if moduleClosed {
            for suffix in ["", "-wal", "-shm"] {
                let path = teardownDatabasePath + suffix
                guard FileManager.default.fileExists(atPath: path) else { continue }
                do {
                    try FileManager.default.removeItem(atPath: path)
                } catch {
                    XCTFail("remove \(path): \(error)")
                }
            }
        }
        module = nil
        events = nil
        cleanup = nil
        databasePath = nil
        try super.tearDownWithError()
    }

    func testAcquisitionFailureRejectsBeginOnce() throws {
        try initializeModule()
        let external = try openExternalConnection()
        try external.acquireWriteLock()

        let begin = PromiseRecorder("begin")
        module.beginWriteTransaction(begin.resolve, reject: begin.reject)
        // The held write lock keeps the transaction callback from starting. Close waits for the
        // registered transaction to finish before it closes the client, so the counters are final.
        let close = try closeModule()

        XCTAssertEqual(begin.resolutions.count, 0)
        XCTAssertEqual(begin.rejections, ["UNKNOWN"])
        let rejection = try XCTUnwrap(begin.rejectionErrors.first ?? nil) as NSError
        XCTAssertEqual(rejection.domain, "GRDB.DatabaseError")
        XCTAssertEqual(rejection.code & 0xFF, Int(SQLITE_BUSY))
        XCTAssertEqual(close.resolutions.count, 1)
        XCTAssertEqual(close.rejections, [])
        try external.releaseWriteLock()
        XCTAssertEqual(try external.rowCount(), 0)

        try initializeModule()
        let id = UUID().uuidString
        let transaction = try commitRow(id: id, name: "after-acquisition-failure")
        let reopenedClose = try closeModule()
        requireSettledOnce(transaction + [reopenedClose])
        XCTAssertEqual(try external.names(id: id), ["after-acquisition-failure"])
        XCTAssertEqual(events.names, [])
    }

    func testCommitResolvesAfterDurableCompletion() throws {
        try initializeModule()
        let external = try openExternalConnection()
        let id = UUID().uuidString

        let transaction = try commitRow(id: id, name: "committed")
        // The commit settled, so another connection sees the row and the write lock is free.
        XCTAssertEqual(try external.names(id: id), ["committed"])
        try external.acquireWriteLock()
        try external.releaseWriteLock()

        let close = try closeModule()
        requireSettledOnce(transaction + [close])
        XCTAssertEqual(try external.names(id: id), ["committed"])
        XCTAssertEqual(events.names, [])
    }

    func testRollbackResolvesAfterRollback() throws {
        try initializeModule()
        let external = try openExternalConnection()
        let id = UUID().uuidString

        let begin = settle("begin") { module.beginWriteTransaction($0.resolve, reject: $0.reject) }
        let txID = try XCTUnwrap(try resolvedValue(begin) as? String)
        let insert = settle("insert") {
            module.txExecute(txID, sql: insertSQL, params: [id, "rolled-back"], resolve: $0.resolve, reject: $0.reject)
        }
        let inserted = try XCTUnwrap(try resolvedValue(insert) as? [String: Any])
        XCTAssertEqual(inserted["rowsAffected"] as? Int, 1)
        let observe = settle("observe") {
            module.txQueryOne(txID, sql: selectNameSQL, params: [id], resolve: $0.resolve, reject: $0.reject)
        }
        let observed = try XCTUnwrap(try resolvedValue(observe) as? [String: Any])
        XCTAssertEqual(observed["name"] as? String, "rolled-back")

        let rollback = settle("rollback") { module.rollbackTransaction(txID, resolve: $0.resolve, reject: $0.reject) }
        _ = try resolvedValue(rollback)
        // The rollback settled, so the module no longer holds the write lock.
        try external.acquireWriteLock()
        try external.releaseWriteLock()
        XCTAssertEqual(try external.names(id: id), [])
        let after = settle("query after rollback") {
            module.queryOne(selectNameSQL, params: [id], resolve: $0.resolve, reject: $0.reject)
        }
        XCTAssertTrue(try resolvedValue(after) is NSNull)

        let close = try closeModule()
        requireSettledOnce([begin, insert, observe, rollback, after, close])
        XCTAssertEqual(events.names, [])
    }

    func testFourReadersLeaveCapacityForOrdinaryQuery() throws {
        try initializeModule()
        let held = try holdReadTransactions(count: 4)
        let ordinary = settle("ordinary query with four readers") {
            module.query("SELECT 1", params: [], resolve: $0.resolve, reject: $0.reject)
        }
        XCTAssertEqual((try resolvedValue(ordinary) as? [[String: Any]])?.count, 1)

        let commits = try held.ids.enumerated().map { index, txID in
            let commit = settle("commit reader \(index)") {
                module.commitTransaction(txID, resolve: $0.resolve, reject: $0.reject)
            }
            _ = try resolvedValue(commit)
            return commit
        }
        let close = try closeModule()

        requireSettledOnce(held.recorders + [ordinary] + commits + [close])
        XCTAssertEqual(events.names, [])
    }

    func testFiveReadersDoNotBlockOrdinaryQueryAheadOfCommits() throws {
        try initializeModule()
        let held = try holdReadTransactions(count: 5)
        let ordinary = PromiseRecorder("ordinary query with five readers")
        let commits = held.ids.indices.map { PromiseRecorder("commit reader \($0)") }
        let module = try XCTUnwrap(self.module)

        let invocationStarted = Date()
        module.query("SELECT 1", params: [], resolve: ordinary.resolve, reject: ordinary.reject)
        XCTAssertLessThan(Date().timeIntervalSince(invocationStarted), promptDispatchTimeout)
        for (index, txID) in held.ids.enumerated() {
            module.commitTransaction(txID, resolve: commits[index].resolve, reject: commits[index].reject)
        }

        waitForSettlements([ordinary] + commits, timeout: promptDispatchTimeout)
        XCTAssertEqual((try resolvedValue(ordinary) as? [[String: Any]])?.count, 1)
        let close = try closeModule()

        requireSettledOnce(held.recorders + [ordinary] + commits + [close])
        XCTAssertEqual(events.names, [])
    }

    func testFiveReadersDoNotBlockOrdinaryQueryOneAheadOfRollbacks() throws {
        try initializeModule()
        let held = try holdReadTransactions(count: 5)
        let ordinary = PromiseRecorder("ordinary query one with five readers")
        let rollbacks = held.ids.indices.map { PromiseRecorder("rollback reader \($0)") }
        let module = try XCTUnwrap(self.module)

        let invocationStarted = Date()
        module.queryOne("SELECT 1", params: [], resolve: ordinary.resolve, reject: ordinary.reject)
        XCTAssertLessThan(Date().timeIntervalSince(invocationStarted), promptDispatchTimeout)
        for (index, txID) in held.ids.enumerated() {
            module.rollbackTransaction(txID, resolve: rollbacks[index].resolve, reject: rollbacks[index].reject)
        }

        waitForSettlements([ordinary] + rollbacks, timeout: promptDispatchTimeout)
        XCTAssertEqual((try resolvedValue(ordinary) as? [String: Any])?.count, 1)
        let close = try closeModule()

        requireSettledOnce(held.recorders + [ordinary] + rollbacks + [close])
        XCTAssertEqual(events.names, [])
    }

    func testOrdinaryWriterSurfacesDoNotBlockWriteTransactionCommands() throws {
        try initializeModule()
        let module = try XCTUnwrap(self.module)
        let ordinaryCalls: [(String, (PromiseRecorder) -> Void)] = [
            ("execute", { recorder in
                module.execute("SELECT 1", params: [], resolve: recorder.resolve, reject: recorder.reject)
            }),
            ("authored write", { recorder in
                module.executeAuthoredWrite(
                    "bridge_items",
                    operation: "update",
                    columnNames: ["name"],
                    sql: "SELECT 1",
                    values: [],
                    resolve: recorder.resolve,
                    reject: recorder.reject
                )
            }),
            ("batch", { recorder in
                module.executeBatch(
                    [["sql": "SELECT 1", "params": []]],
                    resolve: recorder.resolve,
                    reject: recorder.reject
                )
            }),
        ]
        var recorders: [PromiseRecorder] = []

        for (name, call) in ordinaryCalls {
            let begin = settle("begin before \(name)") {
                module.beginWriteTransaction($0.resolve, reject: $0.reject)
            }
            let txID = try XCTUnwrap(try resolvedValue(begin) as? String)
            let probe = settle("transaction query before \(name)") {
                module.txQuery(txID, sql: "SELECT 1", params: [], resolve: $0.resolve, reject: $0.reject)
            }
            let ordinary = PromiseRecorder(name)
            let commit = PromiseRecorder("commit after \(name)")
            let invocationStarted = Date()
            call(ordinary)
            XCTAssertLessThan(Date().timeIntervalSince(invocationStarted), promptDispatchTimeout, name)
            module.commitTransaction(txID, resolve: commit.resolve, reject: commit.reject)

            waitForSettlements([ordinary, commit])
            recorders += [begin, probe, ordinary, commit]
        }

        let close = try closeModule()
        requireSettledOnce(recorders + [close])
        XCTAssertEqual(events.names, [])
    }

    func testCloseDrainsAdmittedQueryAfterAbortingSaturatedReaders() throws {
        try initializeModule()
        let held = try holdReadTransactions(count: 5)
        let pendingBegin = PromiseRecorder("pending sixth reader")
        module.beginReadTransaction(pendingBegin.resolve, reject: pendingBegin.reject)
        let query = PromiseRecorder("admitted query")
        module.query("SELECT 1", params: [], resolve: query.resolve, reject: query.reject)

        let close = PromiseRecorder("close with admitted query")
        let recordClose = close.resolve
        module.close({ value in
            XCTAssertEqual(pendingBegin.rejections, ["NOT_CONNECTED"])
            XCTAssertEqual(pendingBegin.settlementCount, 1)
            XCTAssertEqual(query.settlementCount, 1)
            recordClose(value)
        }, reject: close.reject)
        waitForSettlements([query, pendingBegin, close], timeout: promptDispatchTimeout)

        XCTAssertEqual(query.settlementCount, 1)
        XCTAssertEqual((try resolvedValue(query) as? [[String: Any]])?.count, 1)
        requireSettledOnce(held.recorders + [query, close])
        XCTAssertEqual(pendingBegin.resolutions.count, 0)
        XCTAssertEqual(pendingBegin.rejections, ["NOT_CONNECTED"])
        XCTAssertEqual(pendingBegin.settlementCount, 1)

        let afterClose = settle("query after close") {
            module.query("SELECT 1", params: [], resolve: $0.resolve, reject: $0.reject)
        }
        XCTAssertEqual(afterClose.resolutions.count, 0)
        XCTAssertEqual(afterClose.rejections, ["NOT_CONNECTED"])
        XCTAssertEqual(afterClose.settlementCount, 1)
        XCTAssertEqual(events.names, [])
    }

    func testCloseDrainsQueuedOrdinaryCallsBeforeClosingClient() throws {
        try initializeModule()
        let external = try openExternalConnection()
        let client = try XCTUnwrap(module.client)
        let hold = ChangeHold()
        let subscription = client.onChange(tables: ["bridge_items"]) { hold.enter() }
        addTeardownBlock {
            hold.release()
            subscription.cancel()
        }
        let activeReader = try holdReadTransactions(count: 1)
        let id = UUID().uuidString

        let write = PromiseRecorder("held ordinary write")
        module.execute(insertSQL, params: [id, "ordinary-held"], resolve: write.resolve, reject: write.reject)
        XCTAssertEqual(XCTWaiter().wait(for: [hold.entered], timeout: settlementTimeout), .completed)
        XCTAssertEqual(try external.names(id: id), ["ordinary-held"])
        XCTAssertEqual(write.settlementCount, 0)

        let read = PromiseRecorder("queued ordinary read")
        module.queryOne(selectNameSQL, params: [id], resolve: read.resolve, reject: read.reject)
        let close = PromiseRecorder("close draining ordinary calls")
        let recordClose = close.resolve
        module.close({ value in
            XCTAssertEqual(write.settlementCount, 1)
            XCTAssertEqual(read.settlementCount, 1)
            recordClose(value)
        }, reject: close.reject)

        XCTAssertTrue(try waitUntilCloseDetachesSessions())
        XCTAssertEqual(write.settlementCount, 0)
        XCTAssertEqual(read.settlementCount, 0)
        XCTAssertEqual(close.settlementCount, 0)

        let rejected = PromiseRecorder("ordinary call after close admission stops")
        module.query("SELECT 1", params: [], resolve: rejected.resolve, reject: rejected.reject)
        waitForSettlements([rejected], timeout: promptDispatchTimeout)
        XCTAssertEqual(rejected.resolutions.count, 0)
        XCTAssertEqual(rejected.rejections, ["NOT_CONNECTED"])
        XCTAssertEqual(rejected.settlementCount, 1)
        XCTAssertEqual(write.settlementCount, 0)
        XCTAssertEqual(read.settlementCount, 0)
        XCTAssertEqual(close.settlementCount, 0)

        hold.release()
        waitForSettlements([write, read, close], timeout: promptDispatchTimeout)
        XCTAssertTrue(hold.heldUntilRelease)
        XCTAssertEqual(hold.entryCount, 1)
        subscription.cancel()
        let row = try XCTUnwrap(try resolvedValue(read) as? [String: Any])
        XCTAssertEqual(row["name"] as? String, "ordinary-held")
        requireSettledOnce(activeReader.recorders + [write, read, close])
        XCTAssertEqual(try external.names(id: id), ["ordinary-held"])
        XCTAssertEqual(events.names, [])
    }

    /// Observes an accepted commit after its durable write and before its SDK call returns.
    /// This is not a window inside SQLite COMMIT.
    func testCloseWaitsForAcceptedCommitHeldAfterDurableWrite() throws {
        try initializeModule()
        let external = try openExternalConnection()
        let client = try XCTUnwrap(module.client)
        let hold = ChangeHold()
        let subscription = client.onChange(tables: ["bridge_items"]) { hold.enter() }
        // Registered after the module-close teardown, so it releases the callback first.
        addTeardownBlock {
            hold.release()
            subscription.cancel()
        }
        let id = UUID().uuidString

        let begin = settle("begin") { module.beginWriteTransaction($0.resolve, reject: $0.reject) }
        let txID = try XCTUnwrap(try resolvedValue(begin) as? String)
        let insert = settle("insert") {
            module.txExecute(txID, sql: insertSQL, params: [id, "held"], resolve: $0.resolve, reject: $0.reject)
        }
        XCTAssertEqual(try XCTUnwrap(try resolvedValue(insert) as? [String: Any])["rowsAffected"] as? Int, 1)
        let commit = PromiseRecorder("commit")
        module.commitTransaction(txID, resolve: commit.resolve, reject: commit.reject)
        XCTAssertEqual(XCTWaiter().wait(for: [hold.entered], timeout: settlementTimeout), .completed)

        // The write is durable, but the SDK call has not returned, so the promise is pending.
        XCTAssertEqual(try external.names(id: id), ["held"])
        XCTAssertEqual(commit.settlementCount, 0)
        XCTAssertFalse(try registrySnapshot().isEmpty)

        let close = PromiseRecorder("close")
        module.close(close.resolve, reject: close.reject)
        XCTAssertTrue(try waitUntilCloseDetachesSessions())
        XCTAssertEqual(close.settlementCount, 0)
        XCTAssertEqual(commit.settlementCount, 0)

        hold.release()
        XCTAssertEqual(XCTWaiter().wait(for: [commit.firstSettlement, close.firstSettlement], timeout: settlementTimeout), .completed)
        XCTAssertTrue(hold.heldUntilRelease)
        XCTAssertEqual(hold.entryCount, 1)
        subscription.cancel()
        requireSettledOnce([begin, insert, commit, close])
        XCTAssertEqual(try external.names(id: id), ["held"])

        try initializeModule()
        let laterID = UUID().uuidString
        let later = try commitRow(id: laterID, name: "after-held-commit")
        let laterClose = try closeModule()
        requireSettledOnce(later + [laterClose])
        XCTAssertEqual(try external.names(id: laterID), ["after-held-commit"])
        XCTAssertEqual(events.names, [])
    }

    func testReinitializationKeepsCommittedRowsAndAcceptsWork() throws {
        try initializeModule()
        let firstID = UUID().uuidString
        let first = try commitRow(id: firstID, name: "before-close")
        let firstClose = try closeModule()

        try initializeModule()
        let read = settle("query after reinitialize") {
            module.queryOne(selectNameSQL, params: [firstID], resolve: $0.resolve, reject: $0.reject)
        }
        let row = try XCTUnwrap(try resolvedValue(read) as? [String: Any])
        XCTAssertEqual(row["name"] as? String, "before-close")
        let secondID = UUID().uuidString
        let second = try commitRow(id: secondID, name: "after-reinitialize")
        let external = try openExternalConnection()
        let secondClose = try closeModule()

        requireSettledOnce(first + [firstClose, read] + second + [secondClose])
        XCTAssertEqual(try external.rowCount(), 2)
        XCTAssertEqual(try external.names(id: firstID), ["before-close"])
        XCTAssertEqual(try external.names(id: secondID), ["after-reinitialize"])
        XCTAssertEqual(events.names, [])
    }

    func testUnsupportedCyclicSQLParametersRejectPromptlyAndSettleOnce() throws {
        try initializeModule()
        let cyclicArray = NSMutableArray()
        cyclicArray.add(cyclicArray)
        let cyclicDictionary = NSMutableDictionary()
        cyclicDictionary["type"] = "bytes"
        cyclicDictionary["base64"] = cyclicDictionary
        let inputs: [(String, Any)] = [
            ("cyclic array", cyclicArray),
            ("cyclic dictionary", cyclicDictionary),
        ]
        var rejections: [PromiseRecorder] = []

        for (name, value) in inputs {
            let recorder = PromiseRecorder(name)
            let invocationStarted = Date()
            module.query("SELECT ?", params: [value], resolve: recorder.resolve, reject: recorder.reject)
            XCTAssertLessThan(Date().timeIntervalSince(invocationStarted), promptDispatchTimeout, name)
            waitForSettlements([recorder], timeout: promptDispatchTimeout)
            rejections.append(recorder)
        }

        cyclicArray.removeAllObjects()
        cyclicDictionary.removeAllObjects()
        let close = try closeModule()
        for recorder in rejections {
            XCTAssertEqual(recorder.resolutions.count, 0, recorder.name)
            XCTAssertEqual(recorder.rejections, ["UNKNOWN"], recorder.name)
            XCTAssertEqual(recorder.settlementCount, 1, recorder.name)
        }
        requireSettledOnce([close])
        XCTAssertEqual(events.names, [])
    }

    // MARK: - Inspection snapshot

    func testClientStateSnapshotReadsRequestedRowsInsideReadOnlySnapshot() throws {
        try initializeModule()
        let firstID = UUID().uuidString
        let secondID = UUID().uuidString
        let commits = try commitRow(id: firstID, name: "first") + commitRow(id: secondID, name: "second")

        let snapshot = settle("snapshot") {
            module.inspectClientStateSnapshot(
                [["sql": "SELECT id, name FROM bridge_items WHERE id = ?", "params": [firstID]]],
                resolve: $0.resolve,
                reject: $0.reject
            )
        }
        let result = try XCTUnwrap(try resolvedValue(snapshot) as? [String: Any])
        XCTAssertEqual(Set(result.keys), ["inspection", "applicationRows"])
        let rows = try XCTUnwrap(result["applicationRows"] as? [[String: Any]])
        XCTAssertEqual(rows.count, 1)
        XCTAssertEqual(rows.first?["id"] as? String, firstID)
        XCTAssertEqual(rows.first?["name"] as? String, "first")
        let json = try XCTUnwrap((result["inspection"] as? String)?.data(using: .utf8))
        let inspection = try XCTUnwrap(try JSONSerialization.jsonObject(with: json) as? [String: Any])
        XCTAssertEqual(
            Set(inspection.keys),
            ["client_state", "retained_mutations", "rejected_mutations", "migration_journal",
             "migration_journal_truncated", "physical_schema", "physical_schema_truncated",
             "accepted_mutation_outcomes", "accepted_mutation_outcomes_truncated"]
        )
        // bridge_items is a local table, so the mutation ledger stays empty.
        for member in ["retained_mutations", "rejected_mutations"] {
            XCTAssertEqual((inspection[member] as? [Any])?.count, 0, member)
        }
        let clientState = try XCTUnwrap(inspection["client_state"] as? [String: Any])
        XCTAssertEqual(clientState["mutation_ledger_count"] as? Int, 0)
        XCTAssertEqual(clientState["rejected_mutation_count"] as? Int, 0)

        let write = settle("snapshot write") {
            module.inspectClientStateSnapshot(
                [["sql": "DELETE FROM bridge_items WHERE id = ?", "params": [firstID]]],
                resolve: $0.resolve,
                reject: $0.reject
            )
        }
        XCTAssertEqual(write.resolutions.count, 0)
        XCTAssertEqual(write.rejections.count, 1)
        let external = try openExternalConnection()
        let close = try closeModule()

        requireSettledOnce(commits + [snapshot, close])
        XCTAssertEqual(try external.names(id: firstID), ["first"])
        XCTAssertEqual(try external.rowCount(), 2)
    }

    func testRejectedInspectionMapsLegacyRejectionWithStoredFieldsOnly() throws {
        try initializeModule()
        let external = try openExternalConnection()
        // A rejection stored before the mutation ledger has no exact mutation or rejection JSON.
        try external.execute("""
            INSERT INTO _synchro_rejected_mutations
                (mutation_id, table_name, record_id, status, code, message, server_row_json, server_version, created_at, updated_at)
            VALUES ('m1', 'orders', 'r0', 'rejected_terminal', 'policy_rejected', 'blocked', '{"id":"r0"}', 'server-v7',
                '2026-01-01T00:00:00.000000Z', '2026-01-01T00:00:00.000000Z')
            """)
        let expected: NSDictionary = [
            "representation": "legacy",
            "mutationID": "m1",
            "tableName": "orders",
            "recordID": "r0",
            "status": "rejected_terminal",
            "code": "policy_rejected",
            "message": "blocked",
            "serverRowJSON": #"{"id":"r0"}"#,
            "serverVersion": "server-v7",
            "createdAt": "2026-01-01T00:00:00.000000Z",
            "updatedAt": "2026-01-01T00:00:00.000000Z",
        ]

        let inspect = settle("inspect rejected records") {
            module.inspectRejectedMutationRecords($0.resolve, reject: $0.reject)
        }
        let records = try XCTUnwrap((try resolvedValue(inspect) as? String)?.data(using: .utf8))
        XCTAssertEqual(try JSONSerialization.jsonObject(with: records) as? NSArray, [expected])
        // The deprecated method keeps its published result: a legacy rejection cannot be inspected.
        let deprecated = settle("inspect rejected") {
            module.inspectRejectedMutations($0.resolve, reject: $0.reject)
        }
        XCTAssertEqual(deprecated.resolutions.count, 0)
        XCTAssertEqual(deprecated.rejections.count, 1)
        let snapshot = settle("snapshot") {
            module.inspectClientStateSnapshot([], resolve: $0.resolve, reject: $0.reject)
        }
        let result = try XCTUnwrap(try resolvedValue(snapshot) as? [String: Any])
        let json = try XCTUnwrap((result["inspection"] as? String)?.data(using: .utf8))
        let inspection = try XCTUnwrap(try JSONSerialization.jsonObject(with: json) as? [String: Any])
        XCTAssertEqual(inspection["rejected_mutations"] as? NSArray, [expected])
        let close = try closeModule()

        requireSettledOnce([inspect, snapshot, close])
        XCTAssertEqual(deprecated.settlementCount, 1)
    }

    func testRetainedInspectionMapsAcceptedLegacyImportWithStoredFieldsOnly() throws {
        // A version-five queue row stored no mutation identity, binding, or field values.
        let legacy = try ExternalConnection(path: databasePath, flags: SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE)
        try legacy.execute("""
            CREATE TABLE _grdb_migrations (identifier TEXT NOT NULL PRIMARY KEY);
            INSERT INTO _grdb_migrations (identifier) VALUES ('synchro_v1'), ('synchro_v2_buckets'), ('synchro_v3_scopes'),
                ('synchro_v4_scope_integrity'), ('synchro_v5_rejected_mutations');
            CREATE TABLE _synchro_pending_changes (record_id TEXT NOT NULL, table_name TEXT NOT NULL, operation TEXT NOT NULL,
                base_updated_at TEXT, client_updated_at TEXT NOT NULL, PRIMARY KEY (table_name, record_id));
            CREATE TABLE _synchro_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO _synchro_meta (key, value) VALUES ('sync_lock', '0'), ('checkpoint', '0');
            CREATE TABLE _synchro_scopes (scope_id TEXT PRIMARY KEY, cursor TEXT, checksum TEXT,
                generation INTEGER NOT NULL DEFAULT 0, local_checksum INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE _synchro_scope_rows (scope_id TEXT NOT NULL, table_name TEXT NOT NULL, record_id TEXT NOT NULL,
                checksum INTEGER NOT NULL DEFAULT 0, generation INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (scope_id, table_name, record_id));
            CREATE TABLE _synchro_rejected_mutations (mutation_id TEXT PRIMARY KEY, table_name TEXT NOT NULL, record_id TEXT NOT NULL,
                status TEXT NOT NULL, code TEXT NOT NULL, message TEXT, server_row_json TEXT, server_version TEXT,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
            CREATE TABLE _synchro_bucket_members (bucket_id TEXT NOT NULL, table_name TEXT NOT NULL, record_id TEXT NOT NULL,
                checksum INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (bucket_id, table_name, record_id));
            CREATE TABLE _synchro_bucket_checkpoints (bucket_id TEXT PRIMARY KEY, checkpoint INTEGER NOT NULL DEFAULT 0);
            INSERT INTO _synchro_pending_changes VALUES ('r1', 'orders', 'create', NULL, '2026-01-01T00:00:00.000000Z');
            """)
        try legacy.close()
        try initializeModule()
        let external = try openExternalConnection()
        // The import assigns the mutation identity, so the test reads the stored value.
        let mutationIDs = try external.strings("SELECT mutation_id FROM _synchro_pending_changes WHERE record_id = 'r1'", binding: nil)
        let expected: NSDictionary = [
            "representation": "legacy",
            "mutationID": try XCTUnwrap(mutationIDs.count == 1 ? mutationIDs.first : nil),
            "localOrder": 1,
            "tableName": "orders",
            "recordID": "r1",
            "operation": "insert",
            "baseVersion": NSNull(),
            "clientVersion": "2026-01-01T00:00:00.000000Z",
            "status": "blocked_by_predecessor",
            "sourceKind": "legacy_import",
        ]

        let inspect = settle("inspect retained records") {
            module.inspectRetainedMutationRecords($0.resolve, reject: $0.reject)
        }
        let records = try XCTUnwrap((try resolvedValue(inspect) as? String)?.data(using: .utf8))
        XCTAssertEqual(try JSONSerialization.jsonObject(with: records) as? NSArray, [expected])
        let snapshot = settle("snapshot") {
            module.inspectClientStateSnapshot([], resolve: $0.resolve, reject: $0.reject)
        }
        let result = try XCTUnwrap(try resolvedValue(snapshot) as? [String: Any])
        let json = try XCTUnwrap((result["inspection"] as? String)?.data(using: .utf8))
        let inspection = try XCTUnwrap(try JSONSerialization.jsonObject(with: json) as? [String: Any])
        XCTAssertEqual(inspection["retained_mutations"] as? NSArray, [expected])
        let close = try closeModule()

        requireSettledOnce([inspect, snapshot, close])
    }

    // MARK: - Transaction capacity

    private func holdReadTransactions(count: Int) throws -> (ids: [String], recorders: [PromiseRecorder]) {
        var ids: [String] = []
        var recorders: [PromiseRecorder] = []
        for index in 0..<count {
            let begin = settle("begin reader \(index)") {
                module.beginReadTransaction($0.resolve, reject: $0.reject)
            }
            let txID = try XCTUnwrap(try resolvedValue(begin) as? String)
            let query = settle("query reader \(index)") {
                module.txQuery(txID, sql: "SELECT 1", params: [], resolve: $0.resolve, reject: $0.reject)
            }
            XCTAssertEqual((try resolvedValue(query) as? [[String: Any]])?.count, 1)
            ids.append(txID)
            recorders += [begin, query]
        }
        return (ids, recorders)
    }

    // MARK: - Bridge calls

    private func initializeModule() throws {
        let config: NSDictionary = [
            "dbPath": databasePath!,
            "serverURL": "https://synchro.invalid",
            "clientID": UUID().uuidString,
            "platform": "ios",
            "appVersion": "1.0.0",
        ]
        let initialize = settle("initialize") { module.initialize(config, resolve: $0.resolve, reject: $0.reject) }
        _ = try resolvedValue(initialize)
        let createTable = settle("create table") {
            module.createTable("bridge_items", columnsJson: columnsJSON, optionsJson: nil, resolve: $0.resolve, reject: $0.reject)
        }
        _ = try resolvedValue(createTable)
    }

    private func openExternalConnection() throws -> ExternalConnection {
        let getPath = settle("get path") { module.getPath($0.resolve, reject: $0.reject) }
        let connection = try ExternalConnection(path: try XCTUnwrap(try resolvedValue(getPath) as? String))
        externalConnections.append(connection)
        return connection
    }

    private func commitRow(id: String, name: String) throws -> [PromiseRecorder] {
        let begin = settle("begin") { module.beginWriteTransaction($0.resolve, reject: $0.reject) }
        let txID = try XCTUnwrap(try resolvedValue(begin) as? String)
        let insert = settle("insert") {
            module.txExecute(txID, sql: insertSQL, params: [id, name], resolve: $0.resolve, reject: $0.reject)
        }
        let inserted = try XCTUnwrap(try resolvedValue(insert) as? [String: Any])
        XCTAssertEqual(inserted["rowsAffected"] as? Int, 1)
        let commit = settle("commit") { module.commitTransaction(txID, resolve: $0.resolve, reject: $0.reject) }
        _ = try resolvedValue(commit)
        return [begin, insert, commit]
    }

    private func closeModule() throws -> PromiseRecorder {
        let close = settle("close") { module.close($0.resolve, reject: $0.reject) }
        _ = try resolvedValue(close)
        return close
    }

    // MARK: - Registry observation

    private func registrySnapshot() throws -> [String] {
        let module = try XCTUnwrap(self.module)
        return module.activeTransactionIDs
    }

    /// Close removes every registered session under the registry lock before it waits for them.
    /// The held session cannot remove itself, so an empty registry shows that close detached it.
    private func waitUntilCloseDetachesSessions() throws -> Bool {
        let deadline = Date().addingTimeInterval(settlementTimeout)
        while Date() < deadline {
            if try registrySnapshot().isEmpty {
                return true
            }
            RunLoop.current.run(until: Date().addingTimeInterval(0.01))
        }
        return false
    }

    // MARK: - Settlement

    private func settle(_ name: String, _ call: (PromiseRecorder) -> Void) -> PromiseRecorder {
        let recorder = PromiseRecorder(name)
        call(recorder)
        let result = XCTWaiter().wait(for: [recorder.firstSettlement], timeout: settlementTimeout)
        XCTAssertEqual(result, .completed, "\(name) did not settle")
        return recorder
    }

    private func waitForSettlements(
        _ recorders: [PromiseRecorder],
        timeout: TimeInterval = settlementTimeout,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        let result = XCTWaiter().wait(
            for: recorders.map(\.firstSettlement),
            timeout: timeout
        )
        XCTAssertEqual(result, .completed, "promises did not settle", file: file, line: line)
    }

    private func resolvedValue(
        _ recorder: PromiseRecorder,
        file: StaticString = #filePath,
        line: UInt = #line
    ) throws -> Any? {
        XCTAssertEqual(recorder.rejections, [], "\(recorder.name) rejected", file: file, line: line)
        return try XCTUnwrap(recorder.resolutions.first, "\(recorder.name) did not resolve", file: file, line: line)
    }

    /// Checks the counters after a close that joined every registered transaction.
    private func requireSettledOnce(
        _ recorders: [PromiseRecorder],
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        for recorder in recorders {
            XCTAssertEqual(recorder.resolutions.count, 1, "\(recorder.name) resolutions", file: file, line: line)
            XCTAssertEqual(recorder.rejections, [], "\(recorder.name) rejections", file: file, line: line)
        }
    }
}
