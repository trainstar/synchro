import Foundation

enum RetryTiming {
    private static let nanosecondsPerMillisecond: UInt64 = 1_000_000
    static let maximumSleepChunkNanoseconds: UInt64 = 86_400_000_000_000

    static func deadline(nowMS: Int64, delaySeconds: TimeInterval) -> Int64 {
        guard delaySeconds.isFinite, delaySeconds >= 0 else {
            return nowMS
        }
        let milliseconds = delaySeconds * 1_000
        guard milliseconds.isFinite,
              milliseconds < Double(Int64.max) else {
            return Int64.max
        }
        let delayMS = Int64(milliseconds.rounded(.up))
        let (deadline, overflow) = nowMS.addingReportingOverflow(delayMS)
        return overflow ? Int64.max : deadline
    }

    static func nanosecondsUntil(nowMS: Int64, deadlineMS: Int64) -> UInt64 {
        let milliseconds = positiveDifference(later: deadlineMS, earlier: nowMS)
        guard milliseconds <= UInt64.max / nanosecondsPerMillisecond else {
            return UInt64.max
        }
        return milliseconds * nanosecondsPerMillisecond
    }

    private static func positiveDifference(later: Int64, earlier: Int64) -> UInt64 {
        guard later > earlier else { return 0 }
        if earlier >= 0 || later < 0 {
            return UInt64(later - earlier)
        }
        let earlierMagnitude = UInt64(-(earlier + 1)) + 1
        return UInt64(later) + earlierMagnitude
    }
}
