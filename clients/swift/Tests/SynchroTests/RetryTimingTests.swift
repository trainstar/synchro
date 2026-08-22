import XCTest
@testable import Synchro

final class RetryTimingTests: XCTestCase {
    func testHugeFiniteDelaySaturatesDeadline() {
        XCTAssertEqual(
            RetryTiming.deadline(
                nowMS: 1_000,
                delaySeconds: Double.greatestFiniteMagnitude
            ),
            Int64.max
        )
        XCTAssertEqual(
            RetryTiming.deadline(nowMS: Int64.max - 5, delaySeconds: 0.006),
            Int64.max
        )
        XCTAssertEqual(
            RetryTiming.deadline(nowMS: 1_000, delaySeconds: 0.001),
            1_001
        )
    }

    func testSleepDistanceSaturatesAcrossIntegerDomain() {
        XCTAssertEqual(
            RetryTiming.nanosecondsUntil(
                nowMS: Int64.min,
                deadlineMS: Int64.max
            ),
            UInt64.max
        )
        XCTAssertEqual(
            RetryTiming.nanosecondsUntil(nowMS: 10, deadlineMS: 11),
            1_000_000
        )
        XCTAssertEqual(
            RetryTiming.nanosecondsUntil(nowMS: 11, deadlineMS: 10),
            0
        )
    }
}
