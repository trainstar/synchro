import CryptoKit
import Foundation

public enum TransportOperationClass: String, Codable, Sendable, CaseIterable {
    case connect
    case pull
    case push
    case checkpoint
    case schemas
    case rebuild
    case other

    static func classify(path: String) -> Self {
        let components = path.split(separator: "/")
        guard components.count >= 2,
              components[components.count - 2] == "sync" else {
            return .other
        }
        switch components.last {
        case "connect": return .connect
        case "pull": return .pull
        case "push": return .push
        case "checkpoint", "checkpoints": return .checkpoint
        case "schema", "schemas": return .schemas
        case "rebuild": return .rebuild
        default: return .other
        }
    }
}

public struct TransportRequestFacts: Codable, Sendable, Equatable {
    public let clientGeneration: Int64?
    public let schemaVersion: Int64
    public let schemaHash: String
    public let protocolVersion: Int?
    public let scopeSetVersion: Int64?
    public let scopeCount: Int?
    public let limit: Int?
    public let rebuildIDFingerprint: String?
    public let cursorFingerprint: String?
    public let cursorPresent: Bool?

    enum CodingKeys: String, CodingKey {
        case clientGeneration = "client_generation"
        case schemaVersion = "schema_version"
        case schemaHash = "schema_hash"
        case protocolVersion = "protocol_version"
        case scopeSetVersion = "scope_set_version"
        case scopeCount = "scope_count"
        case limit
        case rebuildIDFingerprint = "rebuild_id_fingerprint"
        case cursorFingerprint = "cursor_fingerprint"
        case cursorPresent = "cursor_present"
    }

    public init(
        clientGeneration: Int64?,
        schemaVersion: Int64,
        schemaHash: String,
        protocolVersion: Int? = nil,
        scopeSetVersion: Int64? = nil,
        scopeCount: Int? = nil,
        limit: Int? = nil,
        rebuildIDFingerprint: String? = nil,
        cursorFingerprint: String? = nil,
        cursorPresent: Bool? = nil
    ) {
        self.clientGeneration = clientGeneration
        self.schemaVersion = schemaVersion
        self.schemaHash = schemaHash
        self.protocolVersion = protocolVersion
        self.scopeSetVersion = scopeSetVersion
        self.scopeCount = scopeCount
        self.limit = limit
        self.rebuildIDFingerprint = rebuildIDFingerprint
        self.cursorFingerprint = cursorFingerprint
        self.cursorPresent = cursorPresent
    }
}

public struct TransportRebuildResponseFacts: Codable, Sendable, Equatable {
    public let recordCount: Int
    public let hasMore: Bool
    public let hasCursor: Bool
    public let hasFinalScopeCursor: Bool
    public let hasChecksum: Bool
    public let scopeMatchesRequest: Bool

    enum CodingKeys: String, CodingKey {
        case recordCount = "record_count"
        case hasMore = "has_more"
        case hasCursor = "has_cursor"
        case hasFinalScopeCursor = "has_final_scope_cursor"
        case hasChecksum = "has_checksum"
        case scopeMatchesRequest = "scope_matches_request"
    }

    public init(
        recordCount: Int,
        hasMore: Bool,
        hasCursor: Bool,
        hasFinalScopeCursor: Bool,
        hasChecksum: Bool,
        scopeMatchesRequest: Bool
    ) {
        self.recordCount = recordCount
        self.hasMore = hasMore
        self.hasCursor = hasCursor
        self.hasFinalScopeCursor = hasFinalScopeCursor
        self.hasChecksum = hasChecksum
        self.scopeMatchesRequest = scopeMatchesRequest
    }
}

public struct TransportPullResponseFacts: Codable, Sendable, Equatable {
    public let changeCount: Int
    public let hasMore: Bool
    public let rebuildScopeCount: Int
    public let checksumCount: Int

    enum CodingKeys: String, CodingKey {
        case changeCount = "change_count"
        case hasMore = "has_more"
        case rebuildScopeCount = "rebuild_scope_count"
        case checksumCount = "checksum_count"
    }

    public init(changeCount: Int, hasMore: Bool, rebuildScopeCount: Int, checksumCount: Int) {
        self.changeCount = changeCount
        self.hasMore = hasMore
        self.rebuildScopeCount = rebuildScopeCount
        self.checksumCount = checksumCount
    }
}

public struct TransportObservation: Codable, Sendable, Equatable {
    public let sequence: UInt64
    public let operationClass: TransportOperationClass
    public let statusCode: Int
    public let durationNanoseconds: UInt64
    public let cursorFingerprints: [String]?
    public let cursorFingerprintsComplete: Bool?
    public let requestFacts: TransportRequestFacts?
    public let rebuildResponseFacts: TransportRebuildResponseFacts?
    public let pullResponseFacts: TransportPullResponseFacts?

    enum CodingKeys: String, CodingKey {
        case sequence
        case operationClass = "operation_class"
        case statusCode = "status_code"
        case durationNanoseconds = "duration_nanoseconds"
        case cursorFingerprints = "cursor_fingerprints"
        case cursorFingerprintsComplete = "cursor_fingerprints_complete"
        case requestFacts = "request_facts"
        case rebuildResponseFacts = "rebuild_response_facts"
        case pullResponseFacts = "pull_response_facts"
    }

    public init(
        sequence: UInt64,
        operationClass: TransportOperationClass,
        statusCode: Int,
        durationNanoseconds: UInt64,
        cursorFingerprints: [String]? = nil,
        cursorFingerprintsComplete: Bool? = nil,
        requestFacts: TransportRequestFacts? = nil,
        rebuildResponseFacts: TransportRebuildResponseFacts? = nil,
        pullResponseFacts: TransportPullResponseFacts? = nil
    ) {
        self.sequence = sequence
        self.operationClass = operationClass
        self.statusCode = statusCode
        self.durationNanoseconds = durationNanoseconds
        self.cursorFingerprints = cursorFingerprints
        self.cursorFingerprintsComplete = cursorFingerprintsComplete
        self.requestFacts = requestFacts
        self.rebuildResponseFacts = rebuildResponseFacts
        self.pullResponseFacts = pullResponseFacts
    }
}

public struct TransportObservationSnapshot: Codable, Sendable, Equatable {
    public let observations: [TransportObservation]
    public let overflowed: Bool
    public let sequenceCheckpoint: UInt64

    enum CodingKeys: String, CodingKey {
        case observations
        case overflowed
        case sequenceCheckpoint = "sequence_checkpoint"
    }

    public init(
        observations: [TransportObservation],
        overflowed: Bool,
        sequenceCheckpoint: UInt64
    ) {
        self.observations = observations
        self.overflowed = overflowed
        self.sequenceCheckpoint = sequenceCheckpoint
    }
}

public enum TransportPauseBarrierError: Error, Sendable, Equatable {
    case alreadyArmed
    case wrongOperation
    case notPaused
    case timedOut
    case cancelled
}

public final class TransportObservationCollector: @unchecked Sendable {
    static let maximumCursorFingerprints = 16

    public let capacity: Int

    private let lock = NSLock()
    private var observations: [TransportObservation] = []
    private var sequence: UInt64 = 0
    private var pausePhase: PausePhase = .idle
    private var nextPauseOperation: TransportOperationClass?
    private var pauseWaiter: PauseWaiter?
    private var pauseTimeoutTask: Task<Void, Never>?

    private enum PausePhase {
        case idle
        case armed(TransportOperationClass)
        case paused(TransportOperationClass, CheckedContinuation<Void, Error>)
        case failed(TransportPauseBarrierError)
        case cancelled
    }

    private struct PauseWaiter {
        let operationClass: TransportOperationClass
        let continuation: CheckedContinuation<Void, Error>
    }

    public init(capacity: Int = 256) {
        self.capacity = max(1, capacity)
    }

    public func snapshot(after sequenceCheckpoint: UInt64 = 0) -> TransportObservationSnapshot {
        lock.lock()
        defer { lock.unlock() }

        let overflowed: Bool
        if let oldestSequence = observations.first?.sequence {
            overflowed = sequenceCheckpoint < oldestSequence - 1
        } else {
            overflowed = false
        }
        return TransportObservationSnapshot(
            observations: observations.filter { $0.sequence > sequenceCheckpoint },
            overflowed: overflowed,
            sequenceCheckpoint: sequence
        )
    }

    public func armPause(for operationClass: TransportOperationClass) throws {
        lock.lock()
        switch pausePhase {
        case .idle:
            pausePhase = .armed(operationClass)
        case .paused where nextPauseOperation == nil:
            nextPauseOperation = operationClass
        default:
            lock.unlock()
            throw failPauseBarrier(with: .alreadyArmed)
        }
        lock.unlock()
    }

    public func awaitPause(
        for operationClass: TransportOperationClass,
        timeout: TimeInterval
    ) async throws {
        guard timeout.isFinite, timeout > 0 else {
            throw failPauseBarrier(with: .timedOut)
        }

        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                lock.lock()
                switch pausePhase {
                case .armed(let armedOperation) where armedOperation == operationClass:
                    guard pauseWaiter == nil else {
                        lock.unlock()
                        continuation.resume(throwing: failPauseBarrier(with: .wrongOperation))
                        return
                    }
                    pauseWaiter = PauseWaiter(
                        operationClass: operationClass,
                        continuation: continuation
                    )
                    let maximumSleepSeconds = Double(UInt64.max - 1) / 1_000_000_000
                    let nanoseconds = UInt64(min(timeout, maximumSleepSeconds) * 1_000_000_000)
                    let timeoutTask = Task { [weak self] in
                        do {
                            try await Task.sleep(nanoseconds: nanoseconds)
                        } catch {
                            return
                        }
                        self?.pauseWaitDidTimeOut(operationClass: operationClass)
                    }
                    pauseTimeoutTask = timeoutTask
                    lock.unlock()
                case .paused(let pausedOperation, _) where pausedOperation == operationClass:
                    lock.unlock()
                    continuation.resume()
                case .failed(let error):
                    lock.unlock()
                    continuation.resume(throwing: error)
                case .cancelled:
                    lock.unlock()
                    continuation.resume(throwing: TransportPauseBarrierError.cancelled)
                default:
                    lock.unlock()
                    continuation.resume(throwing: failPauseBarrier(with: .wrongOperation))
                }
            }
        } onCancel: {
            self.cancelPauseBarrier()
        }
    }

    public func resumePause() throws {
        let pausedContinuation: CheckedContinuation<Void, Error>
        lock.lock()
        switch pausePhase {
        case .paused(_, let continuation):
            pausedContinuation = continuation
            if let nextPauseOperation {
                pausePhase = .armed(nextPauseOperation)
                self.nextPauseOperation = nil
            } else {
                pausePhase = .idle
            }
            lock.unlock()
            pausedContinuation.resume()
        case .failed(let error):
            lock.unlock()
            throw error
        case .cancelled:
            lock.unlock()
            throw TransportPauseBarrierError.cancelled
        default:
            lock.unlock()
            throw failPauseBarrier(with: .notPaused)
        }
    }

    public func cancelPauseBarrier() {
        _ = failPauseBarrier(with: .cancelled)
    }

    func pauseIfArmed(for operationClass: TransportOperationClass) async throws {
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                var waiter: PauseWaiter?
                lock.lock()
                switch pausePhase {
                case .armed(let armedOperation) where armedOperation == operationClass:
                    pausePhase = .paused(operationClass, continuation)
                    if pauseWaiter?.operationClass == operationClass {
                        waiter = pauseWaiter
                        pauseWaiter = nil
                        pauseTimeoutTask?.cancel()
                        pauseTimeoutTask = nil
                    }
                    lock.unlock()
                    waiter?.continuation.resume()
                case .failed(let error):
                    lock.unlock()
                    continuation.resume(throwing: error)
                case .cancelled:
                    lock.unlock()
                    continuation.resume(throwing: TransportPauseBarrierError.cancelled)
                default:
                    lock.unlock()
                    continuation.resume()
                }
            }
        } onCancel: {
            _ = self.failPauseBarrier(with: .cancelled)
        }
    }

    func record(
        operationClass: TransportOperationClass,
        statusCode: Int,
        durationNanoseconds: UInt64,
        cursorFingerprints: [String]?,
        cursorFingerprintsComplete: Bool?,
        requestFacts: TransportRequestFacts? = nil,
        rebuildResponseFacts: TransportRebuildResponseFacts? = nil,
        pullResponseFacts: TransportPullResponseFacts? = nil
    ) {
        lock.lock()
        defer { lock.unlock() }

        precondition(sequence < UInt64.max, "transport observation sequence exhausted")
        sequence += 1
        if observations.count == capacity {
            observations.removeFirst()
        }
        observations.append(TransportObservation(
            sequence: sequence,
            operationClass: operationClass,
            statusCode: statusCode,
            durationNanoseconds: durationNanoseconds,
            cursorFingerprints: cursorFingerprints,
            cursorFingerprintsComplete: cursorFingerprintsComplete,
            requestFacts: requestFacts,
            rebuildResponseFacts: rebuildResponseFacts,
            pullResponseFacts: pullResponseFacts
        ))
    }

    static func cursorFingerprint(_ cursor: String) -> String {
        SHA256.hash(data: Data(cursor.utf8)).map { String(format: "%02x", $0) }.joined()
    }

    @discardableResult
    private func failPauseBarrier(
        with error: TransportPauseBarrierError
    ) -> TransportPauseBarrierError {
        var pausedContinuation: CheckedContinuation<Void, Error>?
        var waiterContinuation: CheckedContinuation<Void, Error>?

        lock.lock()
        if case .failed(let existingError) = pausePhase {
            lock.unlock()
            return existingError
        }
        if case .cancelled = pausePhase {
            lock.unlock()
            return .cancelled
        }
        if case .paused(_, let continuation) = pausePhase {
            pausedContinuation = continuation
        }
        waiterContinuation = pauseWaiter?.continuation
        pauseWaiter = nil
        nextPauseOperation = nil
        pauseTimeoutTask?.cancel()
        pauseTimeoutTask = nil
        pausePhase = error == .cancelled ? .cancelled : .failed(error)
        lock.unlock()

        pausedContinuation?.resume(throwing: error)
        waiterContinuation?.resume(throwing: error)
        return error
    }

    private func pauseWaitDidTimeOut(operationClass: TransportOperationClass) {
        var waiterContinuation: CheckedContinuation<Void, Error>?
        lock.lock()
        if case .armed(let armedOperation) = pausePhase {
            if armedOperation == operationClass,
               pauseWaiter?.operationClass == operationClass {
                waiterContinuation = pauseWaiter?.continuation
                pauseWaiter = nil
                pauseTimeoutTask = nil
                pausePhase = .failed(.timedOut)
            }
        }
        lock.unlock()
        waiterContinuation?.resume(throwing: TransportPauseBarrierError.timedOut)
    }
}
