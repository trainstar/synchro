import Synchro
import UIKit

private struct PackagedSmokeConfig: Decodable {
    let schemaVersion: Int
    let cellID: String
    let platform: String
    let serverURL: String
    let token: String
    let clientID: String
    let phase: String
    let initialSQL: [String]
    let durableSQL: [String]
    let observeSQL: String

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case cellID = "cell_id"
        case platform
        case serverURL = "server_url"
        case token
        case clientID = "client_id"
        case phase
        case initialSQL = "initial_sql"
        case durableSQL = "durable_sql"
        case observeSQL = "observe_sql"
    }
}

private struct PackagedSmokePhaseResult: Encodable {
    let schemaVersion = 1
    let phase: String
    let status = "passed"
    let pid: Int32
    let pendingChangeCount: Int
    let observed: [String: String]

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case phase
        case status
        case pid
        case pendingChangeCount = "pending_change_count"
        case observed
    }
}

@main
final class AppDelegate: UIResponder, UIApplicationDelegate {
    var window: UIWindow?
    private var client: SynchroClient?

    func application(
        _ application: UIApplication,
        didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]? = nil
    ) -> Bool {
        let window = UIWindow(frame: UIScreen.main.bounds)
        let viewController = UIViewController()
        viewController.view.backgroundColor = .systemBackground
        window.rootViewController = viewController
        window.makeKeyAndVisible()
        self.window = window

        do {
            let documents = try FileManager.default.url(
                for: .documentDirectory,
                in: .userDomainMask,
                appropriateFor: nil,
                create: true
            )
            let smokeConfigURL = documents.appendingPathComponent("packaged-smoke-config.json")
            if FileManager.default.fileExists(atPath: smokeConfigURL.path) {
                Task {
                    do {
                        try await self.runPackagedSmoke(configURL: smokeConfigURL, documents: documents)
                    } catch {
                        fatalError("Packaged Synchro smoke failed: \(error)")
                    }
                }
                return true
            }

            let databaseURL = documents.appendingPathComponent("consumer.db")
            let config = SynchroConfig(
                dbPath: databaseURL.path,
                serverURL: URL(string: "http://127.0.0.1")!,
                authProvider: { "unused" },
                clientID: "packaged-ios-consumer",
                platform: "ios",
                appVersion: "consumer"
            )
            let client = try SynchroClient(config: config)
            try client.createTable(
                "consumer_probe",
                columns: [
                    ColumnDef(name: "id", type: "TEXT", nullable: false, primaryKey: true),
                    ColumnDef(name: "value", type: "TEXT", nullable: false),
                ]
            )
            _ = try client.execute(
                "DELETE FROM consumer_probe WHERE id = ?",
                params: ["probe"]
            )
            _ = try client.execute(
                "INSERT INTO consumer_probe (id, value) VALUES (?, ?)",
                params: ["probe", "packaged"]
            )
            self.client = client
        } catch {
            fatalError("Packaged Synchro probe failed: \(error)")
        }

        return true
    }

    private func runAndWaitForScheduledRetry(
        _ client: SynchroClient,
        operation: () async throws -> Void
    ) async throws {
        do {
            try await operation()
            return
        } catch {
            guard client.getSyncStatus() == .backoff else {
                throw error
            }
            let deadline = Date().addingTimeInterval(30)
            while Date() < deadline {
                switch client.getSyncStatus() {
                case .ready:
                    return
                case .error, .stopped, .uninitialized:
                    throw error
                default:
                    try await Task.sleep(nanoseconds: 100_000_000)
                }
            }
            throw error
        }
    }

    private func runPackagedSmoke(configURL: URL, documents: URL) async throws {
        let data = try Data(contentsOf: configURL)
        let smoke = try JSONDecoder().decode(PackagedSmokeConfig.self, from: data)
        guard smoke.schemaVersion == 1,
              !smoke.cellID.isEmpty,
              let serverURL = URL(string: smoke.serverURL),
              smoke.phase == "initial" || smoke.phase == "resume"
        else {
            throw CocoaError(.fileReadCorruptFile)
        }
        let client = try SynchroClient(
            config: SynchroConfig(
                dbPath: documents.appendingPathComponent("consumer.db").path,
                serverURL: serverURL,
                authProvider: { smoke.token },
                clientID: smoke.clientID,
                platform: smoke.platform,
                // The application version, not the package version. The test
                // adapter gates clients below MIN_CLIENT_VERSION 1.0.0.
                appVersion: "1.0.0",
                syncInterval: 3_600,
                pushDebounce: 3_600,
                maxRetryAttempts: 1
            )
        )
        self.client = client

        if smoke.phase == "initial" {
            try await runAndWaitForScheduledRetry(client) {
                try await client.start()
            }
            // start() returns after local recovery and runs the first cycle
            // in the background, so the server schema is not applied yet.
            // The dataset inserts require that schema.
            try await runAndWaitForScheduledRetry(client) {
                try await client.syncNow()
            }
            for statement in smoke.initialSQL {
                _ = try client.execute(statement)
            }
            try await awaitConvergence(client, smoke)
            for statement in smoke.durableSQL {
                _ = try client.execute(statement)
            }
            let pending = try client.pendingChangeCount()
            guard pending == smoke.durableSQL.count else {
                throw CocoaError(.fileWriteUnknown)
            }
            try writePhaseResult(
                phase: smoke.phase,
                pendingCount: pending,
                observed: try observe(client, smoke),
                documents: documents
            )
            return
        }

        guard try client.pendingChangeCount() > 0 else {
            throw CocoaError(.fileReadCorruptFile)
        }
        try await runAndWaitForScheduledRetry(client) {
            try await client.start()
        }
        // The harness authors remote rows while this process is dead. Only
        // ordinary synchronization can deliver them to the local query path.
        try await awaitConvergence(client, smoke)
        let pendingAfterResume = try client.pendingChangeCount()
        let observed = try observe(client, smoke)
        await client.stop()
        try await client.close()
        try writePhaseResult(
            phase: smoke.phase,
            pendingCount: pendingAfterResume,
            observed: observed,
            documents: documents
        )
    }

    // Waits until the queue is empty and the pulled server total equals the
    // sum of the local sets. Only a server rollup and a pull can make them equal.
    private func awaitConvergence(_ client: SynchroClient, _ smoke: PackagedSmokeConfig) async throws {
        let deadline = Date().addingTimeInterval(90)
        while true {
            try await runAndWaitForScheduledRetry(client) {
                try await client.syncNow()
            }
            if try client.pendingChangeCount() == 0,
               try client.queryOne(smoke.observeSQL)?["converged"] as? String == "1" {
                return
            }
            guard Date() < deadline else {
                throw CocoaError(.fileReadCorruptFile)
            }
            try await Task.sleep(nanoseconds: 500_000_000)
        }
    }

    // Reports each observation column as the text of the value that the
    // public query path returned. An INTEGER value arrives as an Int64.
    private func observe(_ client: SynchroClient, _ smoke: PackagedSmokeConfig) throws -> [String: String] {
        guard let row = try client.queryOne(smoke.observeSQL) else {
            throw CocoaError(.fileReadCorruptFile)
        }
        var observed: [String: String] = [:]
        for field in row.columnNames where field != "converged" {
            switch row[field] {
            case let value as String:
                observed[field] = value
            case let value as Int64:
                observed[field] = String(value)
            default:
                throw CocoaError(.fileReadCorruptFile)
            }
        }
        return observed
    }

    private func writePhaseResult(
        phase: String,
        pendingCount: Int,
        observed: [String: String],
        documents: URL
    ) throws {
        let result = PackagedSmokePhaseResult(
            phase: phase,
            pid: ProcessInfo.processInfo.processIdentifier,
            pendingChangeCount: pendingCount,
            observed: observed
        )
        let destination = documents.appendingPathComponent("\(phase)-result.json")
        try JSONEncoder().encode(result).write(to: destination, options: .atomic)
    }
}
