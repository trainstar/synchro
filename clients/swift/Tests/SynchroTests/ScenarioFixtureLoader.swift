import CryptoKit
import Foundation

enum ScenarioFixtureLoader {
    private struct Catalog: Decodable {
        let schemaVersion: Int
        let scenarios: [Entry]

        enum CodingKeys: String, CodingKey {
            case schemaVersion = "schema_version"
            case scenarios
        }
    }

    private struct Entry: Decodable {
        let scenarioID: String
        let path: String
        let sha256: String

        enum CodingKeys: String, CodingKey {
            case scenarioID = "scenario_id"
            case path
            case sha256
        }
    }

    static func load(id: String) throws -> [String: Any] {
        let root = try repositoryRoot()
        let catalogData = try Data(contentsOf: root.appendingPathComponent("conformance/catalog.json"))
        let catalog = try JSONDecoder().decode(Catalog.self, from: catalogData)
        guard catalog.schemaVersion == 1 else {
            throw loaderError("unsupported scenario catalog schema version")
        }
        let matchingEntries = catalog.scenarios.filter { $0.scenarioID == id }
        guard matchingEntries.count == 1, let entry = matchingEntries.first else {
            throw loaderError("scenario is not present in the catalog: \(id)")
        }

        let resolvedRoot = root.resolvingSymlinksInPath().standardizedFileURL
        let scenarioURL = root.appendingPathComponent(entry.path).resolvingSymlinksInPath().standardizedFileURL
        guard scenarioURL.path.hasPrefix(resolvedRoot.path + "/") else {
            throw loaderError("scenario path escapes the repository: \(id)")
        }
        let data = try Data(contentsOf: scenarioURL)
        let digest = SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
        guard digest == entry.sha256 else {
            throw loaderError("scenario digest does not match the catalog: \(id)")
        }
        guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              object["id"] as? String == id else {
            throw loaderError("scenario identity does not match the catalog: \(id)")
        }
        return object
    }

    private static func repositoryRoot() throws -> URL {
        var current = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
        for _ in 0..<8 {
            if FileManager.default.fileExists(atPath: current.appendingPathComponent("conformance/catalog.json").path) {
                return current
            }
            current.deleteLastPathComponent()
        }
        throw loaderError("repository root was not found")
    }

    private static func loaderError(_ message: String) -> NSError {
        NSError(domain: "ScenarioFixtureLoader", code: 1, userInfo: [NSLocalizedDescriptionKey: message])
    }
}
