import Foundation
import Synchro

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

_ = try client.execute("CREATE TABLE consumer_probe (id TEXT PRIMARY KEY, value TEXT NOT NULL)")
_ = try client.execute(
    "INSERT INTO consumer_probe (id, value) VALUES (?, ?)",
    params: ["probe", "packaged"]
)
guard try client.queryOne("SELECT id FROM consumer_probe WHERE value = ?", params: ["packaged"]) != nil else {
    fatalError("Packaged Swift client did not preserve the local row")
}

try await client.close()

print("Packaged Swift consumer passed")
