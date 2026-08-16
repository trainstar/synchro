import Foundation

struct LocalSchemaColumn: Codable, Sendable, Equatable {
    let name: String
    let logicalType: String
    let nullable: Bool
    let sqliteDefaultSQL: String?
    let isPrimaryKey: Bool
}

struct LocalSchemaTable: Codable, Sendable, Equatable {
    let tableName: String
    let updatedAtColumn: String
    let deletedAtColumn: String
    let composition: CompositionClass?
    let primaryKey: [String]
    let columns: [LocalSchemaColumn]
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
                tableName: table.name,
                updatedAtColumn: updatedAtColumn,
                deletedAtColumn: deletedAtColumn,
                composition: table.composition,
                primaryKey: [primaryKeyField.name],
                columns: table.fields.map { field in
                    LocalSchemaColumn(
                        name: field.name,
                        logicalType: SQLiteSchema.normalizedLogicalType(field.type),
                        nullable: field.nullable,
                        sqliteDefaultSQL: nil,
                        isPrimaryKey: field.fieldID == table.primaryKeyFieldID
                    )
                }
            )
        }
    }
}
