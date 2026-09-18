import Foundation

public struct SynchroConfig: Sendable {
    public let dbPath: String
    public let serverURL: URL
    public let authProvider: @Sendable () async throws -> String
    public let clientID: String
    public let platform: String
    public let appVersion: String
    /// Seconds between periodic cycles. Zero disables periodic scheduling.
    public let syncInterval: TimeInterval
    public let pushDebounce: TimeInterval
    public let maxRetryAttempts: Int
    /// Max records per pull page, from 1 through 1000 (default 100).
    public let pullPageSize: Int
    /// Max pending changes per push batch (default 100).
    public let pushBatchSize: Int
    /// Path to a pre-built seed database for offline-first bootstrap.
    /// If set and no database exists at `dbPath`, the seed file is copied before opening.
    public let seedDatabasePath: String?
    @_spi(Inspection)
    public let transportObservationCollector: TransportObservationCollector?

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
        self.init(
            dbPath: dbPath,
            serverURL: serverURL,
            authProvider: authProvider,
            clientID: clientID,
            platform: platform,
            appVersion: appVersion,
            syncInterval: syncInterval,
            pushDebounce: pushDebounce,
            maxRetryAttempts: maxRetryAttempts,
            pullPageSize: pullPageSize,
            pushBatchSize: pushBatchSize,
            seedDatabasePath: seedDatabasePath,
            transportObservationCollector: nil
        )
    }

    @_spi(Inspection)
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
        seedDatabasePath: String? = nil,
        transportObservationCollector: TransportObservationCollector?
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
        self.pullPageSize = pullPageSize
        self.pushBatchSize = pushBatchSize
        self.seedDatabasePath = seedDatabasePath
        self.transportObservationCollector = transportObservationCollector
    }

    func validate() throws {
        guard (1...1000).contains(pullPageSize), (1...1000).contains(pushBatchSize),
              maxRetryAttempts >= 0 else {
            throw SynchroError.invalidResponse(message: "sync configuration limits are invalid")
        }
        for interval in [syncInterval, pushDebounce] {
            let nanoseconds = interval * 1_000_000_000
            guard interval.isFinite, interval >= 0,
                  interval == 0 || nanoseconds >= 1,
                  UInt64(exactly: nanoseconds.rounded(.towardZero)) != nil else {
                throw SynchroError.invalidResponse(message: "sync configuration timers are invalid")
            }
        }
    }
}
