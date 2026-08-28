import Foundation
@_spi(Inspection) import Synchro

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

private func writePhaseResult(_ result: PackagedSmokePhaseResult, to path: String) throws {
    let destination = URL(fileURLWithPath: path)
    let temporary = destination
        .deletingLastPathComponent()
        .appendingPathComponent(".\(destination.lastPathComponent).\(UUID().uuidString)")
    let data = try JSONEncoder().encode(result)
    try data.write(to: temporary, options: .atomic)
    _ = try FileManager.default.replaceItemAt(destination, withItemAt: temporary)
}

private func writeNewPhaseResult(_ result: PackagedSmokePhaseResult, to path: String) throws {
    let destination = URL(fileURLWithPath: path)
    let data = try JSONEncoder().encode(result)
    if FileManager.default.fileExists(atPath: destination.path) {
        try writePhaseResult(result, to: path)
    } else {
        try data.write(to: destination, options: .atomic)
    }
}

private func runPackagedSmoke(
    configPath: String,
    databasePath: String,
    phase: String,
    resultPath: String
) async throws {
    let data = try Data(contentsOf: URL(fileURLWithPath: configPath))
    let config = try JSONDecoder().decode(PackagedSmokeConfig.self, from: data)
    guard config.schemaVersion == 1,
          !config.cellID.isEmpty,
          let serverURL = URL(string: config.serverURL),
          phase == "initial" || phase == "resume"
    else {
        throw CocoaError(.fileReadCorruptFile)
    }
    let transportCollector = TransportObservationCollector(capacity: 256)
    let client = try SynchroClient(
        config: SynchroConfig(
            dbPath: databasePath,
            serverURL: serverURL,
            authProvider: { config.token },
            clientID: config.clientID,
            platform: config.platform,
            appVersion: "0.3.0",
            syncInterval: 3_600,
            pushDebounce: 3_600,
            maxRetryAttempts: 1,
            transportObservationCollector: transportCollector
        )
    )

    if phase == "initial" {
        try await client.start()
        let timestamp = ISO8601DateFormatter().string(from: Date())
        _ = try client.execute(
            "INSERT INTO customers (id, user_id, name, balance, is_active, created_at, updated_at) VALUES (?, ?, ?, 0, 1, ?, ?)",
            params: [config.customerID, config.userID, "Packaged Consumer", timestamp, timestamp]
        )
        _ = try client.execute(
            "INSERT INTO orders (id, customer_id, user_id, status, total_price, currency, ship_address, created_at, updated_at) VALUES (?, ?, ?, 'pending', 0, 'USD', ?, ?, ?)",
            params: [
                config.orderID,
                config.customerID,
                config.userID,
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
                config.orderID,
            ]
        )
        let pending = try client.pendingChangeCount()
        guard pending == 1 else {
            throw CocoaError(.fileWriteUnknown)
        }
        try writeNewPhaseResult(
            PackagedSmokePhaseResult(
                phase: phase,
                pid: ProcessInfo.processInfo.processIdentifier,
                pendingChangeCount: pending
            ),
            to: resultPath
        )
        while true {
            try await Task.sleep(nanoseconds: 60_000_000_000)
        }
    }

    let durable = try client.queryOne(
        "SELECT ship_address FROM orders WHERE id = ?",
        params: [config.orderID]
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
    try writeNewPhaseResult(
        PackagedSmokePhaseResult(
            phase: phase,
            pid: ProcessInfo.processInfo.processIdentifier,
            pendingChangeCount: pendingAfterResume
        ),
        to: resultPath
    )
    await client.stop()
    try await client.close()
}

if let configPath = ProcessInfo.processInfo.environment["SYNCHRO_PACKAGED_SMOKE_CONFIG"] {
    guard let databasePath = ProcessInfo.processInfo.environment["SYNCHRO_PACKAGED_SMOKE_DATABASE"],
          let phase = ProcessInfo.processInfo.environment["SYNCHRO_PACKAGED_SMOKE_PHASE"],
          let resultPath = ProcessInfo.processInfo.environment["SYNCHRO_PACKAGED_SMOKE_PHASE_RESULT"]
    else {
        fatalError("Packaged smoke process environment is incomplete")
    }
    try await runPackagedSmoke(
        configPath: configPath,
        databasePath: databasePath,
        phase: phase,
        resultPath: resultPath
    )
    print("Packaged Swift smoke phase passed")
    exit(EXIT_SUCCESS)
}

let databaseURL = FileManager.default.temporaryDirectory
    .appendingPathComponent("synchro-consumer-\(UUID().uuidString).db")
defer {
    try? FileManager.default.removeItem(at: databaseURL)
}

let config = SynchroConfig(
    dbPath: databaseURL.path,
    serverURL: URL(string: "http://127.0.0.1")!,
    authProvider: { "unused" },
    clientID: UUID().uuidString.lowercased(),
    platform: "macos",
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
    "INSERT INTO consumer_probe (id, value) VALUES (?, ?)",
    params: ["probe", "packaged"]
)
guard try client.queryOne("SELECT id FROM consumer_probe WHERE value = ?", params: ["packaged"]) != nil else {
    fatalError("Packaged Swift client did not preserve the local row")
}

try await client.close()

print("Packaged Swift consumer passed")
