import CryptoKit
import Foundation

enum IntegrityError: Error {
    case invalidValue(String)
}

package enum Integrity {
    static let maxWireJSONBytes = 64 * 1024 * 1024
    private static let maxWireJSONDepth = 128
    private static let rowIdentityDomain = Data("synchro:v3:row-identity:v1\0".utf8)
    private static let rowDigestDomain = Data("synchro:v3:row-digest:v1\0".utf8)
    private static let scopeDigestDomain = Data("synchro:v3:scope-digest:v1\0".utf8)
    private static let schemaManifestDomain = Data("synchro:v3:schema-manifest:v1\0".utf8)

    // A Formatter allocation for each byte dominates hex encoding, and state
    // inspection encodes every retained row identity and checksum.
    private static let hexDigits = Array("0123456789abcdef".utf8)
    static func hexString<S: Sequence>(_ bytes: S) -> String where S.Element == UInt8 {
        var output = [UInt8]()
        output.reserveCapacity(64)
        for byte in bytes {
            output.append(Self.hexDigits[Int(byte >> 4)])
            output.append(Self.hexDigits[Int(byte & 0x0f)])
        }
        return String(decoding: output, as: UTF8.self)
    }

    static func sha256Hex(domain: String, data: Data) -> String {
        var preimage = Data(domain.utf8)
        preimage.append(0)
        preimage.append(data)
        return hexString(SHA256.hash(data: preimage))
    }

    static func schemaManifestHash(_ manifest: SchemaManifest) throws -> String {
        let parent: Any
        if let reference = manifest.parentSchema {
            parent = ["version": reference.version, "hash": reference.hash] as [String: Any]
        } else {
            parent = NSNull()
        }
        let tables: [[String: Any]] = manifest.tables.sorted(by: { utf8Less($0.tableID, $1.tableID) }).map { table in
            let fields: [[String: Any]] = table.fields.sorted(by: { utf8Less($0.fieldID, $1.fieldID) }).map { field in
                var value: [String: Any] = [
                    "field_id": field.fieldID,
                    "name": field.name,
                    "type": field.type,
                    "nullable": field.nullable,
                    "writable": field.writable,
                ]
                if let precision = field.precision { value["precision"] = precision }
                if let scale = field.scale { value["scale"] = scale }
                return value
            }
            let indexes: [[String: Any]] = table.indexes.sorted(by: { utf8Less($0.indexID, $1.indexID) }).map { index in
                [
                    "index_id": index.indexID,
                    "name": index.name,
                    "field_ids": index.fieldIDs,
                    "unique": index.unique,
                ]
            }
            return [
                "table_id": table.tableID,
                "relation_id": table.relationID,
                "name": table.name,
                "composition": table.composition.rawValue,
                "primary_key_field_id": table.primaryKeyFieldID,
                "lifecycle": [
                    "created_at_field_id": table.lifecycle.createdAtFieldID.map { $0 as Any } ?? NSNull(),
                    "updated_at_field_id": table.lifecycle.updatedAtFieldID.map { $0 as Any } ?? NSNull(),
                    "deleted_at_field_id": table.lifecycle.deletedAtFieldID.map { $0 as Any } ?? NSNull(),
                ] as [String: Any],
                "fields": fields,
                "indexes": indexes,
            ]
        }
        let body: [String: Any] = [
            "schema_version": manifest.schemaVersion,
            "parent_schema": parent,
            "transition_class": manifest.transitionClass,
            "compatibility_floor": manifest.compatibilityFloor,
            "tables": tables,
        ]
        let encoded = try JSONSerialization.data(withJSONObject: body)
        guard let source = String(data: encoded, encoding: .utf8) else {
            throw IntegrityError.invalidValue("manifest body is not UTF-8")
        }
        var preimage = schemaManifestDomain
        preimage.append(try canonicalJSON(source))
        return hexString(SHA256.hash(data: preimage))
    }

    static func rowIdentity(table: LocalSchemaTable, pk: [String: AnyCodable]) throws -> Data {
        guard pk.count == 1, let value = pk[table.primaryKeyFieldID],
              let field = table.columns.first(where: { $0.fieldID == table.primaryKeyFieldID }) else {
            throw IntegrityError.invalidValue("invalid primary key for \(table.tableID)")
        }
        var data = rowIdentityDomain
        appendText(table.tableID, to: &data)
        appendText(table.primaryKeyFieldID, to: &data)
        try appendTypedValue(value.value, field: field, requirePresent: true, to: &data)
        return data
    }

    static func rowDigest(
        schemaHash: String,
        table: LocalSchemaTable,
        pk: [String: AnyCodable],
        row: [String: AnyCodable],
        serverVersion: String
    ) throws -> (identity: Data, checksum: ChecksumObject) {
        let result = try rowDigestPreimage(
            schemaHash: schemaHash,
            table: table,
            pk: pk,
            row: row,
            serverVersion: serverVersion
        )
        return (result.identity, checksum(SHA256.hash(data: result.preimage)))
    }

    static func rowDigestPreimage(
        schemaHash: String,
        table: LocalSchemaTable,
        pk: [String: AnyCodable],
        row: [String: AnyCodable],
        serverVersion: String
    ) throws -> (identity: Data, preimage: Data) {
        guard !serverVersion.isEmpty else {
            throw IntegrityError.invalidValue("server version is empty")
        }
        let identity = try rowIdentity(table: table, pk: pk)
        let body = try rowBody(table: table, pk: pk, row: row)
        var input = rowDigestDomain
        input.append(try decodeHex(schemaHash))
        appendBlob(identity, to: &input)
        appendBlob(body, to: &input)
        appendText(serverVersion, to: &input)
        return (identity, input)
    }

    static func scopeDigest(
        schemaHash: String,
        scopeID: String,
        entries: [(identity: Data, digest: ChecksumObject)]
    ) throws -> ChecksumObject {
        checksum(SHA256.hash(data: try scopeDigestPreimage(
            schemaHash: schemaHash,
            scopeID: scopeID,
            entries: entries
        )))
    }

    static func scopeDigestPreimage(
        schemaHash: String,
        scopeID: String,
        entries: [(identity: Data, digest: ChecksumObject)]
    ) throws -> Data {
        guard !scopeID.isEmpty else {
            throw IntegrityError.invalidValue("scope id is empty")
        }
        let ordered = entries.sorted(by: { $0.identity.lexicographicallyPrecedes($1.identity) })
        for entry in ordered {
            try validateRowIdentity(entry.identity)
        }
        for pair in zip(ordered, ordered.dropFirst()) where pair.0.identity == pair.1.identity {
            throw IntegrityError.invalidValue("scope contains a duplicate row identity")
        }
        var input = scopeDigestDomain
        input.append(try decodeHex(schemaHash))
        appendText(scopeID, to: &input)
        appendUInt64(UInt64(ordered.count), to: &input)
        for entry in ordered {
            try entry.digest.validate()
            appendBlob(entry.identity, to: &input)
            input.append(try decodeHex(entry.digest.digest))
        }
        return input
    }

    static func encodedTypedValue(json source: String, field: LocalSchemaColumn) throws -> Data {
        guard let sourceData = source.data(using: .utf8) else { throw invalid(field) }
        let raw = try JSONSerialization.jsonObject(with: sourceData, options: [.fragmentsAllowed])
        if !(raw is NSNull), field.logicalType == "int", !canonicalInteger(source) {
            throw invalid(field)
        }
        if !(raw is NSNull), field.logicalType == "float" {
            guard let number = raw as? NSNumber,
                  CFGetTypeID(number) != CFBooleanGetTypeID(),
                  try canonicalDouble(number.doubleValue) == source else {
                throw invalid(field)
            }
        }
        var data = Data()
        try appendTypedValue(raw, field: field, requirePresent: false, to: &data)
        return data
    }

    /// Validates one already-decoded portable wire value without changing it.
    ///
    /// Capture stores immutable values before a mutation is sealed. Sealing must
    /// validate that stored value, rather than reading or repairing the current row.
    static func validateTypedValue(
        _ value: AnyCodable,
        field: LocalSchemaColumn,
        requirePresent: Bool = false
    ) throws {
        var data = Data()
        try appendTypedValue(value.value, field: field, requirePresent: requirePresent, to: &data)
    }

    static func validateCanonicalClientVersion(_ value: String) throws {
        guard canonicalDateTime(value) else {
            throw IntegrityError.invalidValue("client version is not canonical")
        }
    }

    package static func validateCanonicalWireJSON(_ data: Data) throws {
        guard data.count <= maxWireJSONBytes else { throw IntegrityError.invalidValue("JSON response is too large") }
        let bytes = Array(data)
        var index = 0
        try parseWireJSONValue(bytes, index: &index, depth: 0)
        skipJSONWhitespace(bytes, index: &index)
        guard index == bytes.count else { throw IntegrityError.invalidValue("trailing JSON data") }
    }

    static func stableUUID(domain: String, values: [String]) -> String {
        var input = Data(domain.utf8)
        input.append(0)
        for value in values {
            appendText(value, to: &input)
        }
        var bytes = Array(SHA256.hash(data: input).prefix(16))
        bytes[6] = (bytes[6] & 0x0f) | 0x50
        bytes[8] = (bytes[8] & 0x3f) | 0x80
        let hex = hexString(bytes)
        return "\(hex.prefix(8))-\(hex.dropFirst(8).prefix(4))-\(hex.dropFirst(12).prefix(4))-\(hex.dropFirst(16).prefix(4))-\(hex.dropFirst(20))"
    }

    private static func rowBody(
        table: LocalSchemaTable,
        pk: [String: AnyCodable],
        row: [String: AnyCodable]
    ) throws -> Data {
        let fieldIDs = Set(table.columns.map(\.fieldID))
        guard Set(row.keys) == fieldIDs,
              row[table.primaryKeyFieldID] == pk[table.primaryKeyFieldID] else {
            throw IntegrityError.invalidValue("row field set or primary key is invalid for \(table.tableID)")
        }
        let ordered = table.columns.sorted {
            Array($0.fieldID.utf8).lexicographicallyPrecedes(Array($1.fieldID.utf8))
        }
        var body = Data()
        appendUInt32(UInt32(ordered.count), to: &body)
        for field in ordered {
            appendText(field.fieldID, to: &body)
            try appendTypedValue(row[field.fieldID]!.value, field: field, requirePresent: false, to: &body)
        }
        return body
    }

    private static func appendTypedValue(
        _ raw: Any,
        field: LocalSchemaColumn,
        requirePresent: Bool,
        to data: inout Data
    ) throws {
        let type = field.logicalType
        data.append(try typeTag(type))
        if raw is NSNull {
            if requirePresent || !field.nullable {
                throw IntegrityError.invalidValue("null is not valid for \(field.fieldID)")
            }
            data.append(0)
            return
        }
        data.append(1)
        switch type {
        case "string":
            guard let value = raw as? String else { throw invalid(field) }
            appendText(value, to: &data)
        case "int":
            let value = try exactInt64(raw, field: field)
            guard value >= Int64(Int32.min), value <= Int64(Int32.max) else { throw invalid(field) }
            appendUInt32(UInt32(bitPattern: Int32(value)), to: &data)
        case "int64":
            guard let value = raw as? String, canonicalInteger(value), let parsed = Int64(value) else { throw invalid(field) }
            appendUInt64(UInt64(bitPattern: parsed), to: &data)
        case "decimal":
            guard let value = raw as? String,
                  canonicalDecimal(value),
                  decimalFits(value, precision: field.precision, scale: field.scale) else { throw invalid(field) }
            appendBlob(Data(value.utf8), to: &data)
        case "float":
            let value = try exactDouble(raw, field: field)
            let normalized = value == 0 ? 0.0 : value
            appendUInt64(normalized.bitPattern, to: &data)
        case "boolean":
            guard let value = raw as? NSNumber,
                  CFGetTypeID(value) == CFBooleanGetTypeID() else {
                throw invalid(field)
            }
            data.append(value.boolValue ? 1 : 0)
        case "datetime":
            guard let value = raw as? String, canonicalDateTime(value) else { throw invalid(field) }
            appendBlob(Data(value.utf8), to: &data)
        case "date":
            guard let value = raw as? String, canonicalDate(value) else { throw invalid(field) }
            appendBlob(Data(value.utf8), to: &data)
        case "time":
            guard let value = raw as? String, canonicalTime(value) else { throw invalid(field) }
            appendBlob(Data(value.utf8), to: &data)
        case "json":
            guard let value = raw as? String else { throw invalid(field) }
            let canonical = try canonicalJSON(value)
            guard canonical == Data(value.utf8) else { throw invalid(field) }
            appendBlob(canonical, to: &data)
        case "bytes":
            guard let value = raw as? String, let decoded = decodeBase64URL(value) else { throw invalid(field) }
            appendBlob(decoded, to: &data)
        default:
            throw invalid(field)
        }
    }

    private static func typeTag(_ type: String) throws -> UInt8 {
        switch type {
        case "string": return 0x01
        case "int": return 0x02
        case "int64": return 0x03
        case "decimal": return 0x04
        case "float": return 0x05
        case "boolean": return 0x06
        case "datetime": return 0x07
        case "date": return 0x08
        case "time": return 0x09
        case "json": return 0x0a
        case "bytes": return 0x0b
        default: throw IntegrityError.invalidValue("unsupported portable type \(type)")
        }
    }

    private static func exactInt64(_ raw: Any, field: LocalSchemaColumn) throws -> Int64 {
        if let value = raw as? Int64 { return value }
        if let value = raw as? Int { return Int64(value) }
        if let value = raw as? NSNumber, CFGetTypeID(value) != CFBooleanGetTypeID() {
            guard !CFNumberIsFloatType(value) else { throw invalid(field) }
            let result = value.int64Value
            return result
        }
        throw invalid(field)
    }

    private static func exactDouble(_ raw: Any, field: LocalSchemaColumn) throws -> Double {
        let value: Double
        if let raw = raw as? Double { value = raw }
        else if let raw = raw as? Float { value = Double(raw) }
        else if let raw = raw as? Int64 { value = Double(raw) }
        else if let raw = raw as? Int { value = Double(raw) }
        else if let raw = raw as? NSNumber, CFGetTypeID(raw) != CFBooleanGetTypeID() { value = raw.doubleValue }
        else { throw invalid(field) }
        guard value.isFinite else { throw invalid(field) }
        return value
    }

    private static func canonicalInteger(_ value: String) -> Bool {
        if value == "0" { return true }
        let bytes = Array(value.utf8)
        let start = bytes.first == 45 ? 1 : 0
        guard start < bytes.count, bytes[start] != 48 else { return false }
        return bytes[start...].allSatisfy { $0 >= 48 && $0 <= 57 }
    }

    private static func canonicalDecimal(_ value: String) -> Bool {
        if value == "0" { return true }
        let parts = value.split(separator: ".", omittingEmptySubsequences: false)
        guard parts.count <= 2 else { return false }
        var integer = String(parts[0])
        if integer.first == "-" { integer.removeFirst() }
        guard !integer.isEmpty, integer.utf8.allSatisfy({ (48...57).contains($0) }), integer == "0" || integer.first != "0" else { return false }
        if value.hasPrefix("-0") && parts.count == 1 { return false }
        if parts.count == 2 {
            let fraction = parts[1]
            guard !fraction.isEmpty, fraction.utf8.allSatisfy({ (48...57).contains($0) }), fraction.last != "0" else { return false }
        }
        return true
    }

    private static func decimalFits(_ value: String, precision: Int?, scale: Int?) -> Bool {
        guard let precision, let scale, precision > 0, scale >= 0, scale <= precision else { return false }
        let unsigned = value.first == "-" ? String(value.dropFirst()) : value
        let parts = unsigned.split(separator: ".", omittingEmptySubsequences: false)
        let integerDigits = parts[0].drop(while: { $0 == "0" }).count
        let fractionDigits = parts.count == 2 ? parts[1].count : 0
        return integerDigits <= precision - scale
            && fractionDigits <= scale
            && integerDigits + fractionDigits <= precision
    }

    private static func canonicalDateTime(_ value: String) -> Bool {
        guard value.count == 27,
              value[value.index(value.startIndex, offsetBy: 4)] == "-",
              value[value.index(value.startIndex, offsetBy: 7)] == "-",
              value[value.index(value.startIndex, offsetBy: 10)] == "T",
              value[value.index(value.startIndex, offsetBy: 13)] == ":",
              value[value.index(value.startIndex, offsetBy: 16)] == ":",
              value[value.index(value.startIndex, offsetBy: 19)] == ".",
              value.last == "Z" else { return false }
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"
        formatter.isLenient = false
        return formatter.date(from: value) != nil
    }

    private static func canonicalDate(_ value: String) -> Bool {
        guard value.count == 10 else { return false }
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyy-MM-dd"
        formatter.isLenient = false
        return formatter.date(from: value) != nil
    }

    private static func canonicalTime(_ value: String) -> Bool {
        guard value.count == 15,
              value[value.index(value.startIndex, offsetBy: 2)] == ":",
              value[value.index(value.startIndex, offsetBy: 5)] == ":",
              value[value.index(value.startIndex, offsetBy: 8)] == ".",
              let hour = Int(value.prefix(2)), hour < 24,
              let minute = Int(value.dropFirst(3).prefix(2)), minute < 60,
              let second = Int(value.dropFirst(6).prefix(2)), second < 60 else { return false }
        return value.enumerated().allSatisfy { index, character in
            [2, 5, 8].contains(index) || character.isNumber
        }
    }

    private static func utf8Less(_ lhs: String, _ rhs: String) -> Bool {
        Array(lhs.utf8).lexicographicallyPrecedes(Array(rhs.utf8))
    }

    private static func canonicalJSON(_ source: String) throws -> Data {
        guard let sourceData = source.data(using: .utf8) else { throw IntegrityError.invalidValue("invalid JSON") }
        try validateSafeJSONIntegers(source)
        let value = try JSONSerialization.jsonObject(with: sourceData, options: [.fragmentsAllowed])
        return try canonicalJSONValue(value)
    }

    private static func canonicalJSONValue(_ value: Any) throws -> Data {
        switch value {
        case is NSNull:
            return Data("null".utf8)
        case let value as String:
            return try JSONSerialization.data(withJSONObject: [value], options: [.withoutEscapingSlashes]).dropArrayBrackets()
        case let value as NSNumber:
            if CFGetTypeID(value) == CFBooleanGetTypeID() {
                return Data((value.boolValue ? "true" : "false").utf8)
            }
            guard value.doubleValue.isFinite else {
                throw IntegrityError.invalidValue("invalid JSON number")
            }
            return Data(try canonicalDouble(value.doubleValue).utf8)
        case let value as [Any]:
            var data = Data("[".utf8)
            for (index, item) in value.enumerated() {
                if index > 0 { data.append(44) }
                data.append(try canonicalJSONValue(item))
            }
            data.append(93)
            return data
        case let value as [String: Any]:
            var data = Data("{".utf8)
            let keys = value.keys.sorted { Array($0.utf16).lexicographicallyPrecedes(Array($1.utf16)) }
            for (index, key) in keys.enumerated() {
                if index > 0 { data.append(44) }
                data.append(try canonicalJSONValue(key))
                data.append(58)
                data.append(try canonicalJSONValue(value[key]!))
            }
            data.append(125)
            return data
        default:
            throw IntegrityError.invalidValue("unsupported JSON value")
        }
    }

    private static func decodeBase64URL(_ value: String) -> Data? {
        guard !value.contains("=") else { return nil }
        var standard = value.replacingOccurrences(of: "-", with: "+").replacingOccurrences(of: "_", with: "/")
        standard += String(repeating: "=", count: (4 - standard.count % 4) % 4)
        guard let decoded = Data(base64Encoded: standard) else { return nil }
        let canonical = decoded.base64EncodedString().replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_").replacingOccurrences(of: "=", with: "")
        return canonical == value ? decoded : nil
    }

    private static func canonicalDouble(_ value: Double) throws -> String {
        guard value.isFinite else { throw IntegrityError.invalidValue("invalid JSON number") }
        if value == 0 { return "0" }

        let negative = value < 0
        let source = String(abs(value)).lowercased()
        let exponentParts = source.split(separator: "e", maxSplits: 1, omittingEmptySubsequences: false)
        let mantissa = String(exponentParts[0])
        let exponent = exponentParts.count == 2 ? Int(exponentParts[1]) : 0
        guard let exponent else { throw IntegrityError.invalidValue("invalid JSON number") }

        let decimalParts = mantissa.split(separator: ".", maxSplits: 1, omittingEmptySubsequences: false)
        var digits = decimalParts.joined()
        var decimalPosition = decimalParts[0].count + exponent
        while digits.first == "0" {
            digits.removeFirst()
            decimalPosition -= 1
        }
        while digits.count > 1, digits.last == "0" {
            digits.removeLast()
        }
        guard !digits.isEmpty else { return "0" }

        let sign = negative ? "-" : ""
        let magnitude = abs(value)
        if magnitude >= 1e-6, magnitude < 1e21 {
            if decimalPosition <= 0 {
                return sign + "0." + String(repeating: "0", count: -decimalPosition) + digits
            }
            if decimalPosition >= digits.count {
                return sign + digits + String(repeating: "0", count: decimalPosition - digits.count)
            }
            let split = digits.index(digits.startIndex, offsetBy: decimalPosition)
            return sign + digits[..<split] + "." + digits[split...]
        }

        let first = digits.removeFirst()
        let scientificMantissa = digits.isEmpty ? String(first) : "\(first).\(digits)"
        let scientificExponent = decimalPosition - 1
        let exponentText = scientificExponent < 0 ? String(scientificExponent) : "+\(scientificExponent)"
        return sign + scientificMantissa + "e" + exponentText
    }

    private static func validateSafeJSONIntegers(_ source: String) throws {
        let bytes = Array(source.utf8)
        var index = 0
        while index < bytes.count {
            if bytes[index] == 34 {
                index += 1
                while index < bytes.count {
                    if bytes[index] == 92 {
                        index += 2
                    } else if bytes[index] == 34 {
                        index += 1
                        break
                    } else {
                        index += 1
                    }
                }
                continue
            }
            if bytes[index] == 45 || (48...57).contains(bytes[index]) {
                let start = index
                index += 1
                while index < bytes.count, ![9, 10, 13, 32, 44, 93, 125].contains(bytes[index]) {
                    index += 1
                }
                let token = String(decoding: bytes[start..<index], as: UTF8.self)
                if unsafeJSONInteger(token) {
                    throw IntegrityError.invalidValue("unsafe JSON integer")
                }
                continue
            }
            index += 1
        }
    }

    private static func unsafeJSONInteger(_ source: String) -> Bool {
        var unsigned = source.first == "-" ? String(source.dropFirst()) : source
        var exponent = 0
        if let exponentIndex = unsigned.firstIndex(where: { $0 == "e" || $0 == "E" }) {
            let text = String(unsigned[unsigned.index(after: exponentIndex)...])
            unsigned = String(unsigned[..<exponentIndex])
            guard text.count <= 7, let parsed = Int(text) else { return true }
            exponent = parsed
        }
        var fractionDigits = 0
        if let point = unsigned.firstIndex(of: ".") {
            fractionDigits = unsigned.distance(from: unsigned.index(after: point), to: unsigned.endIndex)
            unsigned.remove(at: point)
        }
        while unsigned.first == "0" { unsigned.removeFirst() }
        if unsigned.isEmpty { return false }
        let scale = fractionDigits - exponent
        if scale > 0 {
            if scale >= unsigned.count { return false }
            let split = unsigned.index(unsigned.endIndex, offsetBy: -scale)
            if unsigned[split...].contains(where: { $0 != "0" }) { return false }
            unsigned = String(unsigned[..<split])
        } else if scale < 0 {
            if unsigned.count - scale > 16 { return true }
            unsigned += String(repeating: "0", count: -scale)
        }
        while unsigned.first == "0" { unsigned.removeFirst() }
        if unsigned.count != 16 { return unsigned.count > 16 }
        return unsigned > "9007199254740991"
    }

    private static func parseWireJSONValue(_ bytes: [UInt8], index: inout Int, depth: Int) throws {
        guard depth <= maxWireJSONDepth else { throw IntegrityError.invalidValue("JSON nesting is too deep") }
        skipJSONWhitespace(bytes, index: &index)
        guard index < bytes.count else { throw IntegrityError.invalidValue("missing JSON value") }
        switch bytes[index] {
        case 34:
            _ = try parseWireJSONString(bytes, index: &index)
        case 91:
            index += 1
            skipJSONWhitespace(bytes, index: &index)
            if index < bytes.count, bytes[index] == 93 {
                index += 1
                return
            }
            while true {
                try parseWireJSONValue(bytes, index: &index, depth: depth + 1)
                skipJSONWhitespace(bytes, index: &index)
                guard index < bytes.count else { throw IntegrityError.invalidValue("unterminated JSON array") }
                if bytes[index] == 93 {
                    index += 1
                    return
                }
                guard bytes[index] == 44 else { throw IntegrityError.invalidValue("invalid JSON array") }
                index += 1
            }
        case 123:
            index += 1
            skipJSONWhitespace(bytes, index: &index)
            if index < bytes.count, bytes[index] == 125 {
                index += 1
                return
            }
            var keys = Set<String>()
            while true {
                skipJSONWhitespace(bytes, index: &index)
                let key = try parseWireJSONString(bytes, index: &index)
                guard keys.insert(key).inserted else { throw IntegrityError.invalidValue("duplicate JSON member") }
                skipJSONWhitespace(bytes, index: &index)
                guard index < bytes.count, bytes[index] == 58 else { throw IntegrityError.invalidValue("invalid JSON object") }
                index += 1
                try parseWireJSONValue(bytes, index: &index, depth: depth + 1)
                skipJSONWhitespace(bytes, index: &index)
                guard index < bytes.count else { throw IntegrityError.invalidValue("unterminated JSON object") }
                if bytes[index] == 125 {
                    index += 1
                    return
                }
                guard bytes[index] == 44 else { throw IntegrityError.invalidValue("invalid JSON object") }
                index += 1
            }
        case 45, 48...57:
            let start = index
            while index < bytes.count, ![9, 10, 13, 32, 44, 93, 125].contains(bytes[index]) {
                index += 1
            }
            let token = String(decoding: bytes[start..<index], as: UTF8.self)
            guard let number = Double(token), number.isFinite,
                  try canonicalDouble(number) == token else {
                throw IntegrityError.invalidValue("noncanonical JSON number")
            }
        case 102:
            try consumeWireLiteral("false", bytes: bytes, index: &index)
        case 110:
            try consumeWireLiteral("null", bytes: bytes, index: &index)
        case 116:
            try consumeWireLiteral("true", bytes: bytes, index: &index)
        default:
            throw IntegrityError.invalidValue("invalid JSON value")
        }
    }

    private static func parseWireJSONString(_ bytes: [UInt8], index: inout Int) throws -> String {
        guard index < bytes.count, bytes[index] == 34 else { throw IntegrityError.invalidValue("invalid JSON string") }
        let start = index
        index += 1
        while index < bytes.count {
            if bytes[index] == 92 {
                index += 2
            } else if bytes[index] == 34 {
                index += 1
                let token = Data(bytes[start..<index])
                guard let value = try JSONSerialization.jsonObject(with: token, options: [.fragmentsAllowed]) as? String else {
                    throw IntegrityError.invalidValue("invalid JSON string")
                }
                return value
            } else {
                index += 1
            }
        }
        throw IntegrityError.invalidValue("unterminated JSON string")
    }

    private static func consumeWireLiteral(_ literal: String, bytes: [UInt8], index: inout Int) throws {
        let expected = Array(literal.utf8)
        guard index <= bytes.count - expected.count,
              Array(bytes[index..<(index + expected.count)]) == expected else {
            throw IntegrityError.invalidValue("invalid JSON literal")
        }
        index += expected.count
    }

    private static func skipJSONWhitespace(_ bytes: [UInt8], index: inout Int) {
        while index < bytes.count, [9, 10, 13, 32].contains(bytes[index]) { index += 1 }
    }

    private static func decodeHex(_ value: String) throws -> Data {
        guard value.count == 64 else { throw IntegrityError.invalidValue("invalid SHA-256 hex") }
        var data = Data(capacity: 32)
        var index = value.startIndex
        for _ in 0..<32 {
            let next = value.index(index, offsetBy: 2)
            guard let byte = UInt8(value[index..<next], radix: 16), value[index..<next].lowercased() == value[index..<next] else {
                throw IntegrityError.invalidValue("invalid SHA-256 hex")
            }
            data.append(byte)
            index = next
        }
        return data
    }

    private static func validateRowIdentity(_ identity: Data) throws {
        let bytes = Array(identity)
        var position = 0

        guard consumeExact(Array(rowIdentityDomain), from: bytes, position: &position) else {
            throw IntegrityError.invalidValue("row identity has an invalid domain")
        }
        try consumeNonemptyText(from: bytes, position: &position)
        try consumeNonemptyText(from: bytes, position: &position)
        guard position + 2 <= bytes.count else {
            throw IntegrityError.invalidValue("row identity primary key is truncated")
        }
        let tag = bytes[position]
        let presence = bytes[position + 1]
        position += 2
        guard presence == 1 else {
            throw IntegrityError.invalidValue("row identity primary key has invalid presence")
        }
        switch tag {
        case 0x01:
            let value = try consumeBlob(from: bytes, position: &position)
            guard String(data: Data(value), encoding: .utf8) != nil else {
                throw IntegrityError.invalidValue("row identity primary key is not valid UTF-8")
            }
        case 0x02:
            try consumeFixed(4, from: bytes, position: &position)
        case 0x03:
            try consumeFixed(8, from: bytes, position: &position)
        default:
            throw IntegrityError.invalidValue("row identity primary key has an invalid type tag")
        }
        guard position == bytes.count else {
            throw IntegrityError.invalidValue("row identity has trailing bytes")
        }
    }

    private static func consumeExact(_ expected: [UInt8], from input: [UInt8], position: inout Int) -> Bool {
        guard position + expected.count <= input.count,
              Array(input[position..<(position + expected.count)]) == expected else {
            return false
        }
        position += expected.count
        return true
    }

    private static func consumeNonemptyText(from input: [UInt8], position: inout Int) throws {
        let value = try consumeBlob(from: input, position: &position)
        guard !value.isEmpty, String(data: Data(value), encoding: .utf8) != nil else {
            throw IntegrityError.invalidValue("row identity text is empty or invalid")
        }
    }

    private static func consumeBlob(from input: [UInt8], position: inout Int) throws -> ArraySlice<UInt8> {
        guard position + 8 <= input.count else {
            throw IntegrityError.invalidValue("row identity length is truncated")
        }
        var length: UInt64 = 0
        for byte in input[position..<(position + 8)] {
            length = (length << 8) | UInt64(byte)
        }
        position += 8
        guard length <= UInt64(Int.max), position <= input.count - Int(length) else {
            throw IntegrityError.invalidValue("row identity value is truncated")
        }
        let value = input[position..<(position + Int(length))]
        position += Int(length)
        return value
    }

    private static func consumeFixed(_ count: Int, from input: [UInt8], position: inout Int) throws {
        guard position <= input.count - count else {
            throw IntegrityError.invalidValue("row identity primary key is truncated")
        }
        position += count
    }

    private static func checksum(_ digest: SHA256.Digest) -> ChecksumObject {
        ChecksumObject(algorithm: "sha256", version: 1, encoding: "hex", digest: hexString(digest))
    }

    private static func appendText(_ value: String, to data: inout Data) {
        appendBlob(Data(value.utf8), to: &data)
    }

    private static func appendBlob(_ value: Data, to data: inout Data) {
        appendUInt64(UInt64(value.count), to: &data)
        data.append(value)
    }

    private static func appendUInt32(_ value: UInt32, to data: inout Data) {
        var bigEndian = value.bigEndian
        withUnsafeBytes(of: &bigEndian) { data.append(contentsOf: $0) }
    }

    private static func appendUInt64(_ value: UInt64, to data: inout Data) {
        var bigEndian = value.bigEndian
        withUnsafeBytes(of: &bigEndian) { data.append(contentsOf: $0) }
    }

    private static func invalid(_ field: LocalSchemaColumn) -> IntegrityError {
        IntegrityError.invalidValue("invalid value for \(field.fieldID)")
    }
}

private extension Data {
    func dropArrayBrackets() -> Data {
        guard count >= 2 else { return self }
        return subdata(in: 1..<(count - 1))
    }
}
