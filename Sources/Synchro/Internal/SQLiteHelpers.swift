import Foundation
import GRDB

enum SQLiteHelpers {
    static func quoteIdentifier(_ name: String) -> String {
        let escaped = name.replacingOccurrences(of: "\"", with: "\"\"")
        return "\"\(escaped)\""
    }

    static func placeholders(count: Int) -> String {
        Array(repeating: "?", count: count).joined(separator: ", ")
    }

    static func timestampNow() -> String {
        "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"
    }

    static func databaseValue(from anyCodable: AnyCodable) -> DatabaseValue {
        switch anyCodable.value {
        case is NSNull:
            return .null
        case let bool as Bool:
            return (bool ? 1 : 0).databaseValue
        case let int as Int:
            return int.databaseValue
        case let int64 as Int64:
            return int64.databaseValue
        case let double as Double:
            return double.databaseValue
        case let string as String:
            return string.databaseValue
        default:
            if let data = try? JSONEncoder().encode(anyCodable),
               let str = String(data: data, encoding: .utf8) {
                return str.databaseValue
            }
            return .null
        }
    }
}
