import Foundation
import GRDB
import Synchro

// Runs the steps that conformance/upgrade publishes for one package phase
// and reports each observation. The application uses only the public SDK.

struct UpgradeFailure: Error, CustomStringConvertible {
    let description: String
}

let environment = ProcessInfo.processInfo.environment
guard let controlURL = environment["SYNCHRO_UPGRADE_CONTROL_URL"].flatMap(URL.init(string:)),
      let dataDirectory = environment["SYNCHRO_UPGRADE_DATA_DIR"],
      let packageVersion = environment["SYNCHRO_UPGRADE_PACKAGE_VERSION"]
else {
    fatalError("SYNCHRO_UPGRADE_CONTROL_URL, SYNCHRO_UPGRADE_DATA_DIR, and SYNCHRO_UPGRADE_PACKAGE_VERSION are required")
}

func request(_ path: String, body: Data? = nil) async throws -> Data {
    var request = URLRequest(url: controlURL.appendingPathComponent(path))
    request.timeoutInterval = 30
    if let body {
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = body
    }
    let (data, response) = try await URLSession.shared.data(for: request)
    guard let status = (response as? HTTPURLResponse)?.statusCode, (200..<300).contains(status) else {
        throw UpgradeFailure(description: "control \(path) request failed")
    }
    return data
}

func bindings(_ values: [Any]?) -> [(any DatabaseValueConvertible)?] {
    (values ?? []).map { value in
        switch value {
        case let text as String: return text
        case let number as NSNumber:
            return CFNumberIsFloatType(number) ? number.doubleValue : number.int64Value
        default: return nil
        }
    }
}

func jsonValue(_ value: DatabaseValue) -> Any {
    switch value.storage {
    case .null: return NSNull()
    case .int64(let integer): return integer
    case .double(let double): return double
    case .string(let text): return text
    case .blob(let data): return data.base64EncodedString()
    }
}

func jsonValue(_ value: AnyCodable) throws -> Any {
    let data = try JSONEncoder().encode(value)
    return try JSONSerialization.jsonObject(with: data, options: .fragmentsAllowed)
}

func inspection(_ mutation: PendingMutationInspection) throws -> [String: Any] {
    [
        "mutation_id": mutation.mutationID,
        "local_order": mutation.localOrder,
        "table_id": mutation.tableID,
        "table_name": mutation.tableName,
        "record_id": mutation.recordID,
        "primary_key_field_id": mutation.primaryKeyFieldID,
        "primary_key_logical_type": mutation.primaryKeyLogicalType,
        "operation": mutation.operation.rawValue,
        "schema_version": mutation.authoredSchema.version,
        "schema_hash": mutation.authoredSchema.hash,
        "base_version": mutation.baseVersion ?? NSNull(),
        "client_version": mutation.clientVersion,
        "status": mutation.status.rawValue,
        "source_kind": mutation.sourceKind,
        "depends_on_mutation_id": mutation.dependsOnMutationID ?? NSNull(),
        "normalized_mutation_id": mutation.normalizedMutationID ?? NSNull(),
        "sealed_batch_id": mutation.sealedBatchID ?? NSNull(),
        "sealed_ordinal": mutation.sealedOrdinal ?? NSNull(),
        "fields": try mutation.authoredFields.map { field in
            ["field_id": field.fieldID, "logical_type": field.logicalType, "value": try jsonValue(field.value)]
        },
    ]
}

// A retryable failure moves the engine to backoff. The engine retries on its
// own schedule, so the step waits for that retry to reach ready. A sync that
// does not finish fails the step, so the earlier observations still report.
func synchronize(_ client: SynchroClient) async throws {
    do {
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask { try await client.syncNow() }
            group.addTask {
                try await Task.sleep(nanoseconds: 120_000_000_000)
                throw UpgradeFailure(description: "sync did not finish within 120 seconds")
            }
            try await group.next()
            group.cancelAll()
        }
    } catch let failure as UpgradeFailure {
        throw failure
    } catch {
        let deadline = Date().addingTimeInterval(60)
        while Date() < deadline {
            switch client.getSyncStatus() {
            case .ready: return
            case .error, .stopped, .uninitialized: throw error
            default: try await Task.sleep(nanoseconds: 200_000_000)
            }
        }
        throw error
    }
}

// Observations collect in order, so a failed step still reports the earlier ones.
var observations: [[String: Any]] = []

@MainActor
func run(_ config: [String: Any]) async throws {
    guard let serverText = config["server_url"] as? String, let serverURL = URL(string: serverText),
          let token = config["token"] as? String,
          let appVersion = config["app_version"] as? String,
          let snapshots = config["snapshots"] as? [[String: Any]],
          let steps = config["steps"] as? [[String: Any]]
    else {
        throw UpgradeFailure(description: "phase configuration is invalid")
    }
    var client: SynchroClient?
    func current() throws -> SynchroClient {
        guard let client else { throw UpgradeFailure(description: "no database is open") }
        return client
    }
    for (index, step) in steps.enumerated() {
        let operation = step["op"] as? String ?? ""
        do {
            switch operation {
            case "open":
                guard let database = step["database"] as? String, let clientID = step["client_id"] as? String else {
                    throw UpgradeFailure(description: "open step is invalid")
                }
                client = try SynchroClient(config: SynchroConfig(
                    dbPath: (dataDirectory as NSString).appendingPathComponent(database),
                    serverURL: serverURL,
                    authProvider: { token },
                    clientID: clientID,
                    platform: "macos",
                    appVersion: appVersion,
                    syncInterval: 3_600,
                    pushDebounce: 3_600
                ))
            case "start": try await current().start()
            case "sync": try await synchronize(current())
            case "stop": try await current().stop()
            case "close":
                try await current().close()
                client = nil
            case "create_local_table":
                try current().createTable("local_notes", columns: [
                    ColumnDef(name: "id", type: "TEXT", nullable: false, primaryKey: true),
                    ColumnDef(name: "body", type: "TEXT", nullable: false),
                ])
            case "execute":
                _ = try current().execute(step["sql"] as? String ?? "", params: bindings(step["params"] as? [Any]))
            case "observe":
                let open = try current()
                var tables: [String: Any] = [:]
                for snapshot in snapshots {
                    guard let name = snapshot["name"] as? String, let sql = snapshot["sql"] as? String else {
                        throw UpgradeFailure(description: "snapshot query is invalid")
                    }
                    tables[name] = try open.query(sql).map { row in
                        Dictionary(uniqueKeysWithValues: row.columnNames.map { ($0, jsonValue(row[$0] as DatabaseValue)) })
                    }
                }
                observations.append([
                    "name": step["name"] as? String ?? "",
                    "snapshots": tables,
                    "pending": try open.inspectPendingMutations().map(inspection),
                    "pending_count": try open.pendingChangeCount(),
                    "rejected_count": try open.inspectRejectedMutations().count,
                ])
            default:
                throw UpgradeFailure(description: "unknown step \(operation)")
            }
        } catch {
            throw UpgradeFailure(description: "step \(index) \(operation) failed: \(error)")
        }
    }
}

let phaseConfig = try JSONSerialization.jsonObject(with: try await request("config")) as? [String: Any] ?? [:]
var failure = ""
do {
    try await run(phaseConfig)
} catch {
    failure = String(describing: error)
}
let result: [String: Any] = [
    "phase": phaseConfig["phase"] as? String ?? "",
    "package_version": packageVersion,
    "error": failure,
    "observations": observations,
]
_ = try await request("result", body: try JSONSerialization.data(withJSONObject: result))
exit(failure.isEmpty ? EXIT_SUCCESS : EXIT_FAILURE)
