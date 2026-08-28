@_spi(Inspection) import Synchro
import UIKit

private struct PackagedSmokeConfig: Decodable {
    let schemaVersion: Int
    let cellID: String
    let platform: String
    let serverURL: String
    let token: String
    let userID: String
    let clientID: String
    let customerID: String
    let orderID: String
    let phase: String

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case cellID = "cell_id"
        case platform
        case serverURL = "server_url"
        case token
        case userID = "user_id"
        case clientID = "client_id"
        case customerID = "customer_id"
        case orderID = "order_id"
        case phase
    }
}

private struct PackagedSmokePhaseResult: Encodable {
    let schemaVersion = 1
    let phase: String
    let status = "passed"
    let pid: Int32
    let pendingChangeCount: Int

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case phase
        case status
        case pid
        case pendingChangeCount = "pending_change_count"
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
        let transportCollector = TransportObservationCollector(capacity: 256)
        let client = try SynchroClient(
            config: SynchroConfig(
                dbPath: documents.appendingPathComponent("consumer.db").path,
                serverURL: serverURL,
                authProvider: { smoke.token },
                clientID: smoke.clientID,
                platform: smoke.platform,
                appVersion: "0.3.0",
                syncInterval: 3_600,
                pushDebounce: 3_600,
                maxRetryAttempts: 1,
                transportObservationCollector: transportCollector
            )
        )
        self.client = client

        if smoke.phase == "initial" {
            try await client.start()
            let timestamp = ISO8601DateFormatter().string(from: Date())
            _ = try client.execute(
                "INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, ?, ?)",
                params: [smoke.customerID, smoke.userID, "Packaged Consumer", timestamp, timestamp]
            )
            _ = try client.execute(
                "INSERT INTO orders (id, customer_id, user_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, ?, 'pending', 0, 'USD', ?, ?, ?)",
                params: [
                    smoke.orderID,
                    smoke.customerID,
                    smoke.userID,
                    #"{"street":"Packaged Initial"}"#,
                    timestamp,
                    timestamp,
                ]
            )
            try await client.syncNow()
            guard try client.pendingChangeCount() == 0 else {
                throw CocoaError(.fileWriteUnknown)
            }
            let snapshot = transportCollector.snapshot()
            guard !snapshot.overflowed else {
                throw CocoaError(.fileReadCorruptFile)
            }
            let observations = snapshot.observations
            let requiredOperations: [TransportOperationClass] = [.connect, .push, .pull]
            guard requiredOperations.allSatisfy({ operation in
                observations.contains { observation in
                    observation.operationClass == operation && (200 ..< 300).contains(observation.statusCode)
                }
            }) else {
                throw CocoaError(.fileReadCorruptFile)
            }
            _ = try client.execute(
                "UPDATE orders SET ship_address = ?, updated_at = ? WHERE id = ?",
                params: [
                    #"{"street":"Packaged Durable"}"#,
                    ISO8601DateFormatter().string(from: Date()),
                    smoke.orderID,
                ]
            )
            let pending = try client.pendingChangeCount()
            guard pending == 1 else {
                throw CocoaError(.fileWriteUnknown)
            }
            try writePhaseResult(phase: smoke.phase, pendingCount: pending, documents: documents)
            return
        }

        let durable = try client.queryOne(
            "SELECT ship_address FROM orders WHERE id = ?",
            params: [smoke.orderID]
        )?["ship_address"] as? String
        let pendingBeforeResume = try client.pendingChangeCount()
        guard durable == #"{"street":"Packaged Durable"}"#, pendingBeforeResume > 0 else {
            throw CocoaError(.fileReadCorruptFile)
        }
        try await client.start()
        try await client.syncNow()
        let pendingAfterResume = try client.pendingChangeCount()
        guard pendingAfterResume == 0 else {
            throw CocoaError(.fileWriteUnknown)
        }
        try writePhaseResult(
            phase: smoke.phase,
            pendingCount: pendingAfterResume,
            documents: documents
        )
        await client.stop()
        try await client.close()
    }

    private func writePhaseResult(phase: String, pendingCount: Int, documents: URL) throws {
        let result = PackagedSmokePhaseResult(
            phase: phase,
            pid: ProcessInfo.processInfo.processIdentifier,
            pendingChangeCount: pendingCount
        )
        let destination = documents.appendingPathComponent("\(phase)-result.json")
        try JSONEncoder().encode(result).write(to: destination, options: .atomic)
    }
}
