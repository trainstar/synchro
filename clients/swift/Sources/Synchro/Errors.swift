import Foundation

public enum SynchroError: Error, Sendable {
    case notConnected
    case schemaNotLoaded
    case tableNotSynced(String)
    case upgradeRequired(currentVersion: String, minimumVersion: String)
    case schemaMismatch(serverVersion: Int64, serverHash: String)
    case pushRejected(results: [RejectedMutation])
    case networkError(underlying: Error)
    case serverError(status: Int, message: String)
    case protocolError(status: Int, code: ProtocolErrorCode, message: String)
    case databaseError(underlying: Error)
    case invalidResponse(message: String)
    case blocked(SyncFailure)
    case unsupportedSchema(reason: SchemaUnsupportedReason)
    case invalidStateTransition(from: SyncStatus, to: SyncStatus)
    case alreadyStarted
    case notStarted
}

extension SynchroError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case .notConnected:
            return "Not connected to sync server"
        case .schemaNotLoaded:
            return "Schema has not been loaded from server"
        case .tableNotSynced(let table):
            return "Table '\(table)' is not a synced table"
        case .upgradeRequired(let current, let minimum):
            return "App version \(current) is below minimum \(minimum)"
        case .schemaMismatch(let version, let hash):
            return "Schema mismatch: server version \(version), hash \(hash)"
        case .pushRejected(let results):
            return "Push rejected: \(results.count) mutation(s)"
        case .networkError(let err):
            return "Network error: \(err.localizedDescription)"
        case .serverError(let status, let message):
            return "Server error \(status): \(message)"
        case .protocolError(let status, let code, let message):
            return "Protocol error \(status) \(code.rawValue): \(message)"
        case .databaseError(let err):
            return "Database error: \(err.localizedDescription)"
        case .invalidResponse(let message):
            return "Invalid response: \(message)"
        case .blocked(let failure):
            return failure.message
        case .unsupportedSchema(let reason):
            return "Schema recovery is required: \(reason.rawValue)"
        case .invalidStateTransition(let from, let to):
            return "Invalid sync state transition from \(from.rawValue) to \(to.rawValue)"
        case .alreadyStarted:
            return "Sync has already been started"
        case .notStarted:
            return "Sync has not been started"
        }
    }
}
