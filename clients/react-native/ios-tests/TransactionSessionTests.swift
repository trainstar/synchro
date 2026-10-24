@testable import SynchroReactNative
import os
import XCTest

private struct SessionError: Error, Equatable {
    let name: String
}

/// Records the begin completions that the actual session delivers.
private final class BeginRecorder: Sendable {
    private struct State: Sendable {
        var successes: [String] = []
        var failures: [SessionError?] = []
    }

    private let state = OSAllocatedUnfairLock(initialState: State())

    var successes: [String] {
        state.withLock { $0.successes }
    }

    var failures: [SessionError?] {
        state.withLock { $0.failures }
    }

    func settlement() -> ReactPromiseSettlement {
        ReactPromiseSettlement(
            resolve: { [self] value in
                let stringValue = value as? String
                state.withLock { state in
                    if let stringValue {
                        state.successes.append(stringValue)
                    }
                }
            },
            reject: { [self] _, _, error in
                state.withLock { $0.failures.append(error as? SessionError) }
            }
        )
    }
}

private final class SettlementCounter: Sendable {
    private let countState = OSAllocatedUnfairLock(initialState: 0)

    var count: Int {
        countState.withLock { $0 }
    }

    func increment() {
        countState.withLock { $0 += 1 }
    }
}

private final class EventRecorder: NSObject, SynchroEventEmitting {
    private struct State {
        var name: String?
        var body: NSDictionary?
        var deliveredOnMain = false
    }

    private let state = NSLock()
    private var value = State()
    let expectation: XCTestExpectation

    init(expectation: XCTestExpectation) {
        self.expectation = expectation
    }

    func emitEvent(_ name: String, body: NSDictionary) {
        state.lock()
        value.name = name
        value.body = body
        value.deliveredOnMain = Thread.isMainThread
        state.unlock()
        expectation.fulfill()
    }

    func snapshot() -> (String?, NSDictionary?, Bool) {
        state.lock()
        defer { state.unlock() }
        return (value.name, value.body, value.deliveredOnMain)
    }
}

private struct AuthStoreTimeout: Error {}

private func pendingAuthRequest(
    _ store: AuthRequestStore,
    id: String,
    insertion: CheckedContinuation<Bool, Never>
) async throws -> String {
    try await withCheckedThrowingContinuation { continuation in
        let inserted = store.insert(continuation, for: id)
        insertion.resume(returning: inserted)
        guard inserted else {
            continuation.resume(throwing: AuthStoreTimeout())
            return
        }
    }
}

/// Direct tests of begin ownership in the production TransactionSession.
final class TransactionSessionTests: XCTestCase {
    func testFailureBeforeCallbackRejectsStoredBeginOnce() {
        let begin = BeginRecorder()
        let session = TransactionSession(isWrite: true, beginSettlement: begin.settlement())

        session.rejectBegin(SessionError(name: "acquisition"))
        session.rejectBegin(SessionError(name: "later failure"))

        XCTAssertEqual(begin.successes, [])
        XCTAssertEqual(begin.failures, [SessionError(name: "acquisition")])
    }

    func testAbortBeforeBeginAcceptanceRejectsBeginAndUnwindsCallback() {
        let begin = BeginRecorder()
        let session = TransactionSession(isWrite: true, beginSettlement: begin.settlement())

        session.abort(SessionError(name: "close"))
        XCTAssertThrowsError(try session.acceptBegin("abort-before-begin")) { error in
            XCTAssertEqual(error as? SessionError, SessionError(name: "close"))
        }
        session.rejectBegin(SessionError(name: "callback exit"))

        XCTAssertEqual(begin.successes, [])
        XCTAssertEqual(begin.failures, [SessionError(name: "close")])
    }

    func testAcceptedBeginIsNotSettledAgainByLaterFailure() throws {
        let begin = BeginRecorder()
        let session = TransactionSession(isWrite: true, beginSettlement: begin.settlement())

        try session.acceptBegin("accepted")
        session.abort(SessionError(name: "close"))
        session.rejectBegin(SessionError(name: "callback exit"))

        XCTAssertEqual(begin.successes, ["accepted"])
        XCTAssertEqual(begin.failures, [])
    }

    func testPromiseSettlementRaceInvokesOneBlock() {
        let counter = SettlementCounter()
        let settlement = ReactPromiseSettlement(
            resolve: { _ in counter.increment() },
            reject: { _, _, _ in counter.increment() }
        )

        DispatchQueue.concurrentPerform(iterations: 100) { index in
            if index.isMultiple(of: 2) {
                settlement.resolve(.void)
            } else {
                settlement.reject("TEST", "test", nil)
            }
        }

        XCTAssertEqual(counter.count, 1)
    }

    func testPromiseSettlementUnlocksBeforeReentrantInvocation() {
        let counter = SettlementCounter()
        let holder = OSAllocatedUnfairLock<ReactPromiseSettlement?>(initialState: nil)
        let settlement = ReactPromiseSettlement(
            resolve: { _ in
                counter.increment()
                holder.withLock { $0 }?.reject("REENTRANT", "reentrant", nil)
            },
            reject: { _, _, _ in counter.increment() }
        )
        holder.withLock { $0 = settlement }

        settlement.resolve(.void)

        XCTAssertEqual(counter.count, 1)
    }

    func testEventTargetIsWeakAndDeliversOnMain() {
        let event = expectation(description: "event")
        let target = ReactEventTarget()
        var recorder: EventRecorder? = EventRecorder(expectation: event)
        target.setDelegate(recorder)

        target.emit("onTest", body: .object(["value": .string("ok")]))
        wait(for: [event], timeout: 1)

        let snapshot = recorder?.snapshot()
        XCTAssertEqual(snapshot?.0, "onTest")
        XCTAssertEqual(snapshot?.1?["value"] as? String, "ok")
        XCTAssertEqual(snapshot?.2, true)

        weak let weakRecorder = recorder
        recorder = nil
        XCTAssertNil(weakRecorder)
        XCTAssertNil(target.currentDelegate)
    }

    func testAuthStoreCloseRejectsPendingAndIgnoresLateCompletion() async throws {
        let store = AuthRequestStore()
        store.activate()
        var pendingTask: Task<String, Error>?
        let inserted = await withCheckedContinuation { insertion in
            pendingTask = Task {
                try await pendingAuthRequest(store, id: "request", insertion: insertion)
            }
        }
        XCTAssertTrue(inserted)
        XCTAssertEqual(store.count, 1)
        let task = try XCTUnwrap(pendingTask)

        store.cancelAll()
        store.resolve("request", token: "late")

        do {
            _ = try await task.value
            XCTFail("Expected close to reject the request")
        } catch {
            XCTAssertEqual((error as NSError).domain, "SynchroModule")
        }
        XCTAssertEqual(store.count, 0)
    }
}
