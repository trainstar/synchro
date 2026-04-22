import Foundation
@preconcurrency import GRDB

public struct Record: Codable, Sendable {
    public var id: String
    public var tableName: String
    public var data: [String: AnyCodable]
    public var updatedAt: Date
    public var deletedAt: Date?
    public var bucketID: String?
    public var checksum: Int32?

    enum CodingKeys: String, CodingKey {
        case id
        case tableName = "table_name"
        case data
        case updatedAt = "updated_at"
        case deletedAt = "deleted_at"
        case bucketID = "bucket_id"
        case checksum
    }
}

public struct DeleteEntry: Codable, Sendable {
    public var id: String
    public var tableName: String

    enum CodingKeys: String, CodingKey {
        case id
        case tableName = "table_name"
    }
}

public struct BucketUpdate: Codable, Sendable {
    public var added: [String]?
    public var removed: [String]?
}

// MARK: - Push

public struct PushRecord: Codable, Sendable {
    public var id: String
    public var tableName: String
    public var operation: String
    public var data: [String: AnyCodable]?
    public var clientUpdatedAt: Date
    public var baseUpdatedAt: Date?

    enum CodingKeys: String, CodingKey {
        case id
        case tableName = "table_name"
        case operation
        case data
        case clientUpdatedAt = "client_updated_at"
        case baseUpdatedAt = "base_updated_at"
    }
}

// MARK: - Schema

public struct SchemaResponse: Codable, Sendable {
    public var schemaVersion: Int64
    public var schemaHash: String
    public var serverTime: Date
    public var manifest: SchemaManifest

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case schemaHash = "schema_hash"
        case serverTime = "server_time"
        case manifest
    }

    func localTables() throws -> [LocalSchemaTable] {
        try manifest.localTables()
    }
}

// MARK: - Table Meta

public struct TableMetaResponse: Codable, Sendable {
    public var tables: [TableMeta]
    public var serverTime: Date
    public var schemaVersion: Int64
    public var schemaHash: String

    enum CodingKeys: String, CodingKey {
        case tables
        case serverTime = "server_time"
        case schemaVersion = "schema_version"
        case schemaHash = "schema_hash"
    }
}

public struct TableMeta: Codable, Sendable {
    public var tableName: String
    public var pushPolicy: String
    public var dependencies: [String]
    public var parentTable: String?
    public var parentFKCol: String?
    public var updatedAtColumn: String?
    public var deletedAtColumn: String?
    public var bucketByColumn: String?
    public var bucketPrefix: String?
    public var globalWhenBucketNull: Bool?
    public var allowGlobalRead: Bool?
    public var bucketFunction: String?

    enum CodingKeys: String, CodingKey {
        case tableName = "table_name"
        case pushPolicy = "push_policy"
        case dependencies
        case parentTable = "parent_table"
        case parentFKCol = "parent_fk_col"
        case updatedAtColumn = "updated_at_column"
        case deletedAtColumn = "deleted_at_column"
        case bucketByColumn = "bucket_by_column"
        case bucketPrefix = "bucket_prefix"
        case globalWhenBucketNull = "global_when_bucket_null"
        case allowGlobalRead = "allow_global_read"
        case bucketFunction = "bucket_function"
    }
}

// MARK: - SDK Types

public enum SyncStatus: Sendable {
    case idle
    case syncing
    case error(retryAt: Date?)
    case stopped
}

public struct ConflictEvent: Sendable {
    public let table: String
    public let recordID: String
    public let clientData: [String: AnyCodable]?
    public let serverData: [String: AnyCodable]?
}

public struct ExecResult: Sendable {
    public let rowsAffected: Int
}

public struct SQLStatement: @unchecked Sendable {
    public let sql: String
    public let params: [any DatabaseValueConvertible]?

    public init(sql: String, params: [any DatabaseValueConvertible]? = nil) {
        self.sql = sql
        self.params = params
    }
}

public struct ColumnDef: Sendable {
    public let name: String
    public let type: String
    public let nullable: Bool
    public let defaultValue: String?
    public let primaryKey: Bool

    public init(name: String, type: String, nullable: Bool = true, defaultValue: String? = nil, primaryKey: Bool = false) {
        self.name = name
        self.type = type
        self.nullable = nullable
        self.defaultValue = defaultValue
        self.primaryKey = primaryKey
    }
}

public struct TableOptions: Sendable {
    public let ifNotExists: Bool
    public let withoutRowid: Bool

    public init(ifNotExists: Bool = true, withoutRowid: Bool = false) {
        self.ifNotExists = ifNotExists
        self.withoutRowid = withoutRowid
    }
}

public protocol Cancellable: Sendable {
    func cancel()
}

// MARK: - AnyCodable

public struct AnyCodable: Codable, @unchecked Sendable, Equatable {
    public let value: Any

    public init(_ value: Any) {
        self.value = value
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        if container.decodeNil() {
            value = NSNull()
        } else if let bool = try? container.decode(Bool.self) {
            value = bool
        } else if let int = try? container.decode(Int64.self) {
            value = int
        } else if let double = try? container.decode(Double.self) {
            value = double
        } else if let string = try? container.decode(String.self) {
            value = string
        } else if let array = try? container.decode([AnyCodable].self) {
            value = array.map { $0.value }
        } else if let dict = try? container.decode([String: AnyCodable].self) {
            value = dict.mapValues { $0.value }
        } else {
            throw DecodingError.dataCorruptedError(in: container, debugDescription: "unsupported type")
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch value {
        case is NSNull:
            try container.encodeNil()
        case let bool as Bool:
            try container.encode(bool)
        case let int as Int:
            try container.encode(int)
        case let int64 as Int64:
            try container.encode(int64)
        case let double as Double:
            try container.encode(double)
        case let string as String:
            try container.encode(string)
        case let array as [Any]:
            try container.encode(array.map { AnyCodable($0) })
        case let dict as [String: Any]:
            try container.encode(dict.mapValues { AnyCodable($0) })
        default:
            throw EncodingError.invalidValue(value, .init(codingPath: encoder.codingPath, debugDescription: "unsupported type: \(type(of: value))"))
        }
    }

    public static func == (lhs: AnyCodable, rhs: AnyCodable) -> Bool {
        switch (lhs.value, rhs.value) {
        case is (NSNull, NSNull):
            return true
        case let (l as Bool, r as Bool):
            return l == r
        case let (l as Int64, r as Int64):
            return l == r
        case let (l as Double, r as Double):
            return l == r
        case let (l as String, r as String):
            return l == r
        default:
            return false
        }
    }
}

// MARK: - Push Status Constants

public enum PushStatus {
    public static let applied = "applied"
    public static let conflict = "conflict"
    public static let rejectedTerminal = "rejected_terminal"
    public static let rejectedRetryable = "rejected_retryable"
}

// MARK: - JSON Date Coding

extension JSONDecoder {
    static func synchroDecoder() -> JSONDecoder {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .custom { decoder in
            let container = try decoder.singleValueContainer()
            let string = try container.decode(String.self)
            if let date = SynchroDateCoding.parse(string) {
                return date
            }
            throw DecodingError.dataCorruptedError(in: container, debugDescription: "invalid date: \(string)")
        }
        return decoder
    }
}

extension JSONEncoder {
    static func synchroEncoder() -> JSONEncoder {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .custom { date, encoder in
            var container = encoder.singleValueContainer()
            try container.encode(SynchroDateCoding.string(from: date))
        }
        return encoder
    }
}

private enum SynchroDateCoding {
    private static let lock = NSLock()
    private static let fractionalFormatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }()
    private static let fallbackFormatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        return formatter
    }()

    static func parse(_ string: String) -> Date? {
        lock.lock()
        defer { lock.unlock() }
        if let date = fractionalFormatter.date(from: string) {
            return date
        }
        return fallbackFormatter.date(from: string)
    }

    static func string(from date: Date) -> String {
        lock.lock()
        defer { lock.unlock() }
        return fractionalFormatter.string(from: date)
    }
}
