@testable import SynchroReactNative
import XCTest

private struct SessionError: Error, Equatable {
    let name: String
}

/// Records the begin completions that the actual session delivers.
private final class BeginRecorder {
    private(set) var results: [Result<String, Error>] = []

    func record(_ result: Result<String, Error>) {
        results.append(result)
    }

    var successes: [String] {
        results.compactMap { try? $0.get() }
    }

    var failures: [SessionError?] {
        results.compactMap { result in
            guard case .failure(let error) = result else { return nil }
            return .some(error as? SessionError)
        }
    }
}

/// Direct tests of begin ownership in the production TransactionSession.
final class TransactionSessionTests: XCTestCase {
    func testFailureBeforeCallbackRejectsStoredBeginOnce() {
        let begin = BeginRecorder()
        let session = TransactionSession(isWrite: true, beginCompletion: begin.record)

        session.rejectBegin(SessionError(name: "acquisition"))
        session.rejectBegin(SessionError(name: "later failure"))

        XCTAssertEqual(begin.successes, [])
        XCTAssertEqual(begin.failures, [SessionError(name: "acquisition")])
    }

    func testAbortBeforeBeginAcceptanceRejectsBeginAndUnwindsCallback() {
        let begin = BeginRecorder()
        let session = TransactionSession(isWrite: true, beginCompletion: begin.record)

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
        let session = TransactionSession(isWrite: true, beginCompletion: begin.record)

        try session.acceptBegin("accepted")
        session.abort(SessionError(name: "close"))
        session.rejectBegin(SessionError(name: "callback exit"))

        XCTAssertEqual(begin.successes, ["accepted"])
        XCTAssertEqual(begin.failures, [])
    }
}
