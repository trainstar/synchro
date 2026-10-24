import Foundation
import os
import React

/// Values that can cross wrapper execution boundaries without carrying Foundation containers.
enum BridgeValue: Sendable {
    case null
    case bool(Bool)
    case integer(Int64)
    case double(Double)
    case string(String)
    case array([BridgeValue])
    case object([String: BridgeValue])

    func foundationValue() -> Any {
        switch self {
        case .null:
            return NSNull()
        case .bool(let value):
            return NSNumber(value: value)
        case .integer(let value):
            return NSNumber(value: value)
        case .double(let value):
            return NSNumber(value: value)
        case .string(let value):
            return value
        case .array(let values):
            return values.map { $0.foundationValue() }
        case .object(let values):
            return values.mapValues { $0.foundationValue() }
        }
    }
}

enum BridgePromiseValue: Sendable {
    case void
    case value(BridgeValue)
}

/// Owns the imported React promise blocks and settles them exactly once.
///
/// React Native copies these blocks for an escaping module invocation. Its TurboModule
/// implementation protects its shared resolve/reject state with a mutex and schedules
/// JavaScript through `AsyncCallback`. React Native also permits native callbacks from
/// any queue. Swift cannot express those Objective-C runtime guarantees as `Sendable`.
final class ReactPromiseSettlement: @unchecked Sendable {
    private let lock = NSLock()
    private var resolveBlock: RCTPromiseResolveBlock?
    private var rejectBlock: RCTPromiseRejectBlock?

    init(resolve: @escaping RCTPromiseResolveBlock, reject: @escaping RCTPromiseRejectBlock) {
        resolveBlock = resolve
        rejectBlock = reject
    }

    @discardableResult
    func resolve(_ value: BridgePromiseValue) -> Bool {
        lock.lock()
        guard let resolveBlock else {
            lock.unlock()
            return false
        }
        self.resolveBlock = nil
        rejectBlock = nil
        lock.unlock()

        switch value {
        case .void:
            resolveBlock(nil)
        case .value(let value):
            resolveBlock(value.foundationValue())
        }
        return true
    }

    @discardableResult
    func reject(_ code: String?, _ message: String?, _ error: Error?) -> Bool {
        lock.lock()
        guard let rejectBlock else {
            lock.unlock()
            return false
        }
        resolveBlock = nil
        self.rejectBlock = nil
        lock.unlock()

        rejectBlock(code, message, error)
        return true
    }
}

/// Owns the Objective-C event target without creating an implementation cycle.
///
/// The delegate is installed by Objective-C++, can be released outside Swift, and is
/// invoked only on main. The lock protects the weak reference, not callback execution.
final class ReactEventTarget: @unchecked Sendable {
    private let lock = NSLock()
    private weak var delegate: SynchroEventEmitting?

    var currentDelegate: SynchroEventEmitting? {
        lock.lock()
        defer { lock.unlock() }
        return delegate
    }

    func setDelegate(_ delegate: SynchroEventEmitting?) {
        lock.lock()
        self.delegate = delegate
        lock.unlock()
    }

    func emit(_ name: String, body: BridgeValue) {
        DispatchQueue.main.async { [self, name, body] in
            lock.lock()
            let delegate = self.delegate
            lock.unlock()
            guard let payload = body.foundationValue() as? NSDictionary else {
                preconditionFailure("Event payload must be an object")
            }
            delegate?.emitEvent(name, body: payload)
        }
    }
}

final class AuthRequestStore: Sendable {
    private struct State: Sendable {
        var acceptsRequests = false
        var continuations: [String: CheckedContinuation<String, Error>] = [:]
    }

    private let state = OSAllocatedUnfairLock(initialState: State())

    func activate() {
        state.withLock { $0.acceptsRequests = true }
    }

    func insert(_ continuation: CheckedContinuation<String, Error>, for requestID: String) -> Bool {
        state.withLock { state in
            guard state.acceptsRequests else { return false }
            state.continuations[requestID] = continuation
            return true
        }
    }

    func resolve(_ requestID: String, token: String) {
        let continuation = state.withLock { $0.continuations.removeValue(forKey: requestID) }
        continuation?.resume(returning: token)
    }

    func reject(_ requestID: String, error: Error) {
        let continuation = state.withLock { $0.continuations.removeValue(forKey: requestID) }
        continuation?.resume(throwing: error)
    }

    func cancelAll() {
        let continuations = state.withLock { state in
            state.acceptsRequests = false
            let continuations = Array(state.continuations.values)
            state.continuations.removeAll()
            return continuations
        }
        let error = NSError(
            domain: "SynchroModule",
            code: 0,
            userInfo: [NSLocalizedDescriptionKey: "client closed"]
        )
        continuations.forEach { $0.resume(throwing: error) }
    }

    var count: Int {
        state.withLock { $0.continuations.count }
    }
}
