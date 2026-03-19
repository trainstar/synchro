import Foundation

public struct SynchroConfig: Sendable {
    public let dbPath: String
    public let serverURL: URL
    public let authProvider: @Sendable () async throws -> String
    public let clientID: String
    public let platform: String
    public let appVersion: String
    public let syncInterval: TimeInterval
    public let pushDebounce: TimeInterval
    public let maxRetryAttempts: Int
    /// Max records per pull page (default 100, server caps at 1000).
    public let pullPageSize: Int
    /// Max pending changes per push batch (default 100).
    public let pushBatchSize: Int
    /// Path to a pre-built seed database for offline-first bootstrap.
    /// If set and no database exists at `dbPath`, the seed file is copied before opening.
    public let seedDatabasePath: String?

    public init(
        dbPath: String,
        serverURL: URL,
        authProvider: @escaping @Sendable () async throws -> String,
        clientID: String,
        platform: String = "ios",
        appVersion: String,
        syncInterval: TimeInterval = 30,
        pushDebounce: TimeInterval = 0.5,
        maxRetryAttempts: Int = 5,
        pullPageSize: Int = 100,
        pushBatchSize: Int = 100,
        seedDatabasePath: String? = nil
    ) {
        self.dbPath = dbPath
        self.serverURL = serverURL
        self.authProvider = authProvider
        self.clientID = clientID
        self.platform = platform
        self.appVersion = appVersion
        self.syncInterval = syncInterval
        self.pushDebounce = pushDebounce
        self.maxRetryAttempts = maxRetryAttempts
        self.pullPageSize = min(pullPageSize, 1000)
        self.pushBatchSize = pushBatchSize
        self.seedDatabasePath = seedDatabasePath
    }
}
