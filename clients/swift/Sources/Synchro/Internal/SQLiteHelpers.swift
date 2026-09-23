import Foundation
@preconcurrency import GRDB

enum SQLiteHelpers {
    static func quoteIdentifier(_ name: String) -> String {
        let escaped = name.replacingOccurrences(of: "\"", with: "\"\"")
        return "\"\(escaped)\""
    }

    static func placeholders(count: Int) -> String {
        Array(repeating: "?", count: count).joined(separator: ", ")
    }

    static func databaseValue(
        from value: AnyCodable,
        column: LocalSchemaColumn
    ) throws -> DatabaseValue {
        if value.value is NSNull {
            return .null
        }

        switch column.logicalType {
        case "string", "decimal", "datetime", "date", "time", "json":
            guard let text = value.value as? String else {
                throw invalidValue(column.fieldID)
            }
            return text.databaseValue
        case "int":
            let integer = try exactInt64(value.value, fieldID: column.fieldID)
            guard integer >= Int64(Int32.min), integer <= Int64(Int32.max) else {
                throw invalidValue(column.fieldID)
            }
            return integer.databaseValue
        case "int64":
            guard let text = value.value as? String,
                  canonicalInteger(text),
                  let integer = Int64(text) else {
                throw invalidValue(column.fieldID)
            }
            return integer.databaseValue
        case "float":
            return try exactDouble(value.value, fieldID: column.fieldID).databaseValue
        case "boolean":
            guard let boolean = value.value as? Bool else {
                throw invalidValue(column.fieldID)
            }
            return (boolean ? 1 : 0).databaseValue
        case "bytes":
            guard let encoded = value.value as? String,
                  let decoded = decodeBase64URL(encoded) else {
                throw invalidValue(column.fieldID)
            }
            return decoded.databaseValue
        default:
            throw SynchroError.invalidResponse(
                message: "unsupported portable type \(column.logicalType)"
            )
        }
    }

    private static func exactInt64(_ value: Any, fieldID: String) throws -> Int64 {
        if let integer = value as? Int64 {
            return integer
        }
        if let integer = value as? Int {
            return Int64(integer)
        }
        if let number = value as? NSNumber,
           CFGetTypeID(number) != CFBooleanGetTypeID(),
           number.doubleValue == Double(number.int64Value) {
            return number.int64Value
        }
        throw invalidValue(fieldID)
    }

    private static func exactDouble(_ value: Any, fieldID: String) throws -> Double {
        let number: Double
        if let value = value as? Double {
            number = value
        } else if let value = value as? Float {
            number = Double(value)
        } else if let value = value as? Int64 {
            number = Double(value)
        } else if let value = value as? Int {
            number = Double(value)
        } else if let value = value as? NSNumber, CFGetTypeID(value) != CFBooleanGetTypeID() {
            number = value.doubleValue
        } else {
            throw invalidValue(fieldID)
        }
        guard number.isFinite else {
            throw invalidValue(fieldID)
        }
        return number
    }

    private static func canonicalInteger(_ value: String) -> Bool {
        if value == "0" { return true }
        let bytes = Array(value.utf8)
        let start = bytes.first == 45 ? 1 : 0
        guard start < bytes.count, bytes[start] != 48 else { return false }
        return bytes[start...].allSatisfy { $0 >= 48 && $0 <= 57 }
    }

    private static func decodeBase64URL(_ value: String) -> Data? {
        guard !value.contains("=") else { return nil }
        var standard = value.replacingOccurrences(of: "-", with: "+")
            .replacingOccurrences(of: "_", with: "/")
        standard += String(repeating: "=", count: (4 - standard.count % 4) % 4)
        guard let decoded = Data(base64Encoded: standard) else { return nil }
        let canonical = decoded.base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
        return canonical == value ? decoded : nil
    }

    private static func invalidValue(_ fieldID: String) -> SynchroError {
        SynchroError.invalidResponse(message: "invalid value for \(fieldID)")
    }
}
