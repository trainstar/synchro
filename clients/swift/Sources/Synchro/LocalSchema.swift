import Foundation
@preconcurrency import GRDB

struct LocalSchemaColumn: Codable, Sendable, Equatable {
    let fieldID: String
    let name: String
    let logicalType: String
    let nullable: Bool
    let writable: Bool
    let precision: Int?
    let scale: Int?
    let sqliteDefaultSQL: String?
    let isPrimaryKey: Bool

    func sqliteDefaultWireValue(_ db: GRDB.Database) throws -> AnyCodable? {
        guard let sqliteDefaultSQL,
              !sqliteDefaultSQL.isEmpty,
              SQLiteSchema.isConstantDefaultSQL(sqliteDefaultSQL) else {
            return nil
        }
        guard let row = try Row.fetchOne(
            db,
            sql: "SELECT CAST((\(sqliteDefaultSQL)) AS \(SQLiteSchema.sqliteType(for: logicalType))) AS value"
        ) else {
            throw SynchroError.invalidResponse(message: "SQLite default did not produce a value")
        }
        let value: DatabaseValue = row["value"]
        let wireValue: AnyCodable
        switch (logicalType, value.storage) {
        case (_, .null):
            wireValue = AnyCodable(NSNull())
        case ("boolean", .int64(let value)) where value == 0 || value == 1:
            wireValue = AnyCodable(value == 1)
        case ("int", .int64(let value)):
            wireValue = AnyCodable(value)
        case ("int64", .int64(let value)):
            wireValue = AnyCodable(String(value))
        case ("float", .double(let value)):
            wireValue = AnyCodable(value)
        case ("bytes", .blob(let value)):
            wireValue = AnyCodable(
                value.base64EncodedString()
                    .replacingOccurrences(of: "+", with: "-")
                    .replacingOccurrences(of: "/", with: "_")
                    .replacingOccurrences(of: "=", with: "")
            )
        case ("string", .string(let value)),
             ("decimal", .string(let value)),
             ("datetime", .string(let value)),
             ("date", .string(let value)),
             ("time", .string(let value)),
             ("json", .string(let value)):
            wireValue = AnyCodable(value)
        default:
            return nil
        }
        do {
            try Integrity.validateTypedValue(wireValue, field: self)
        } catch {
            return nil
        }
        return wireValue
    }
}

struct LocalSchemaIndex: Codable, Sendable, Equatable {
    let indexID: String
    let name: String
    let fieldIDs: [String]
    let unique: Bool
}

struct LocalSchemaTable: Codable, Sendable, Equatable {
    let tableID: String
    let relationID: String
    let tableName: String
    let primaryKeyFieldID: String
    let createdAtFieldID: String?
    let updatedAtFieldID: String?
    let deletedAtFieldID: String?
    let updatedAtColumn: String
    let deletedAtColumn: String
    let composition: CompositionClass?
    let primaryKey: [String]
    let columns: [LocalSchemaColumn]
    let indexes: [LocalSchemaIndex]

    init(
        tableID: String,
        relationID: String,
        tableName: String,
        primaryKeyFieldID: String,
        createdAtFieldID: String?,
        updatedAtFieldID: String?,
        deletedAtFieldID: String?,
        updatedAtColumn: String,
        deletedAtColumn: String,
        composition: CompositionClass?,
        primaryKey: [String],
        columns: [LocalSchemaColumn],
        indexes: [LocalSchemaIndex] = []
    ) {
        self.tableID = tableID
        self.relationID = relationID
        self.tableName = tableName
        self.primaryKeyFieldID = primaryKeyFieldID
        self.createdAtFieldID = createdAtFieldID
        self.updatedAtFieldID = updatedAtFieldID
        self.deletedAtFieldID = deletedAtFieldID
        self.updatedAtColumn = updatedAtColumn
        self.deletedAtColumn = deletedAtColumn
        self.composition = composition
        self.primaryKey = primaryKey
        self.columns = columns
        self.indexes = indexes
    }

    private enum CodingKeys: String, CodingKey {
        case tableID
        case relationID
        case tableName
        case primaryKeyFieldID
        case createdAtFieldID
        case updatedAtFieldID
        case deletedAtFieldID
        case updatedAtColumn
        case deletedAtColumn
        case composition
        case primaryKey
        case columns
        case indexes
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.init(
            tableID: try container.decode(String.self, forKey: .tableID),
            relationID: try container.decode(String.self, forKey: .relationID),
            tableName: try container.decode(String.self, forKey: .tableName),
            primaryKeyFieldID: try container.decode(String.self, forKey: .primaryKeyFieldID),
            createdAtFieldID: try container.decodeIfPresent(String.self, forKey: .createdAtFieldID),
            updatedAtFieldID: try container.decodeIfPresent(String.self, forKey: .updatedAtFieldID),
            deletedAtFieldID: try container.decodeIfPresent(String.self, forKey: .deletedAtFieldID),
            updatedAtColumn: try container.decode(String.self, forKey: .updatedAtColumn),
            deletedAtColumn: try container.decode(String.self, forKey: .deletedAtColumn),
            composition: try container.decodeIfPresent(CompositionClass.self, forKey: .composition),
            primaryKey: try container.decode([String].self, forKey: .primaryKey),
            columns: try container.decode([LocalSchemaColumn].self, forKey: .columns),
            indexes: try container.decodeIfPresent([LocalSchemaIndex].self, forKey: .indexes) ?? []
        )
    }
}

extension SchemaManifest {
    func localTables() throws -> [LocalSchemaTable] {
        try validate()

        return try tables.map { table in
            let fieldsByID = Dictionary(uniqueKeysWithValues: table.fields.map { ($0.fieldID, $0) })
            guard let primaryKeyField = fieldsByID[table.primaryKeyFieldID] else {
                throw ContractViolation.missingPrimaryKey(tableName: table.name)
            }
            let updatedAtColumn = table.lifecycle.updatedAtFieldID.flatMap { fieldsByID[$0]?.name } ?? ""
            let deletedAtColumn = table.lifecycle.deletedAtFieldID.flatMap { fieldsByID[$0]?.name } ?? ""
            guard !table.fields.isEmpty else {
                throw ContractViolation.missingColumns(tableName: table.name)
            }

            return LocalSchemaTable(
                tableID: table.tableID,
                relationID: table.relationID,
                tableName: table.name,
                primaryKeyFieldID: table.primaryKeyFieldID,
                createdAtFieldID: table.lifecycle.createdAtFieldID,
                updatedAtFieldID: table.lifecycle.updatedAtFieldID,
                deletedAtFieldID: table.lifecycle.deletedAtFieldID,
                updatedAtColumn: updatedAtColumn,
                deletedAtColumn: deletedAtColumn,
                composition: table.composition,
                primaryKey: [primaryKeyField.name],
                columns: table.fields.map { field in
                    LocalSchemaColumn(
                        fieldID: field.fieldID,
                        name: field.name,
                        logicalType: field.type,
                        nullable: field.nullable,
                        writable: field.writable,
                        precision: field.precision,
                        scale: field.scale,
                        sqliteDefaultSQL: nil,
                        isPrimaryKey: field.fieldID == table.primaryKeyFieldID
                    )
                },
                indexes: table.indexes.map {
                    LocalSchemaIndex(
                        indexID: $0.indexID,
                        name: $0.name,
                        fieldIDs: $0.fieldIDs,
                        unique: $0.unique
                    )
                }
            )
        }
    }
}
