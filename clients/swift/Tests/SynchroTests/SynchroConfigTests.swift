import XCTest
@testable import Synchro

final class SynchroConfigTests: XCTestCase {
    private func config(
        syncInterval: TimeInterval = 30,
        pushDebounce: TimeInterval = 0.5,
        maxRetryAttempts: Int = 5,
        pullPageSize: Int = 100,
        pushBatchSize: Int = 100
    ) -> SynchroConfig {
        SynchroConfig(
            dbPath: NSTemporaryDirectory() + "synchro_config_\(UUID().uuidString).sqlite",
            serverURL: URL(string: "http://config.invalid")!,
            authProvider: { XCTFail("Configuration checks must not request authentication"); return "" },
            clientID: "config-client",
            appVersion: "1.0.0",
            syncInterval: syncInterval,
            pushDebounce: pushDebounce,
            maxRetryAttempts: maxRetryAttempts,
            pullPageSize: pullPageSize,
            pushBatchSize: pushBatchSize
        )
    }

    func testInvalidLimitsAndTimersFailBeforeDatabaseCreation() async throws {
        var invalid = [
            config(maxRetryAttempts: -1),
            config(pullPageSize: 0), config(pullPageSize: 1001),
            config(pushBatchSize: 0), config(pushBatchSize: 1001),
        ]
        for timer in [-1, 0.0000000001, .nan, .infinity, -.infinity, Double.greatestFiniteMagnitude, Double(UInt64.max)] {
            invalid.append(config(syncInterval: timer))
            invalid.append(config(pushDebounce: timer))
        }
        for config in invalid {
            do {
                let client = try SynchroClient(config: config)
                try await client.close()
                XCTFail("Invalid configuration was accepted")
            } catch SynchroError.invalidResponse {
                XCTAssertFalse(FileManager.default.fileExists(atPath: config.dbPath))
            }
        }
    }

    func testZeroRetriesAndManualSchedulingRemainValid() async throws {
        for pageSize in [1, 1000] {
            let config = config(
                syncInterval: 0, pushDebounce: 0, maxRetryAttempts: 0,
                pullPageSize: pageSize, pushBatchSize: pageSize
            )
            let client = try SynchroClient(config: config)
            XCTAssertEqual(config.pullPageSize, pageSize)
            XCTAssertEqual(client.getSyncStatus(), .localReady)
            try await client.close()
        }
    }
}
