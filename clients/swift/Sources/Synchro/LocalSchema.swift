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
            guard let primaryKey = table.primaryKey, !primaryKey.isEmpty else {
                throw ContractViolation.missingPrimaryKey(tableName: table.name)
            }
            guard let updatedAtColumn = table.updatedAtColumn, !updatedAtColumn.isEmpty else {
                throw ContractViolation.missingUpdatedAtColumn(tableName: table.name)
            }
            guard let deletedAtColumn = table.deletedAtColumn, !deletedAtColumn.isEmpty else {
                throw ContractViolation.missingDeletedAtColumn(tableName: table.name)
            }
            guard let columns = table.columns, !columns.isEmpty else {
                throw ContractViolation.missingColumns(tableName: table.name)
            }

            let columnNames = Set(columns.map(\.name))
            for columnName in primaryKey where !columnNames.contains(columnName) {
                throw ContractViolation.unknownPrimaryKeyColumn(tableName: table.name, columnName: columnName)
            }
            if !columnNames.contains(updatedAtColumn) {
                throw ContractViolation.unknownUpdatedAtColumn(tableName: table.name, columnName: updatedAtColumn)
            }
            if !columnNames.contains(deletedAtColumn) {
                throw ContractViolation.unknownDeletedAtColumn(tableName: table.name, columnName: deletedAtColumn)
            }

            return LocalSchemaTable(
                tableName: table.name,
                updatedAtColumn: updatedAtColumn,
                deletedAtColumn: deletedAtColumn,
                composition: table.composition,
                primaryKey: primaryKey,
                columns: columns.map { column in
                    LocalSchemaColumn(
                        name: column.name,
                        logicalType: SQLiteSchema.normalizedLogicalType(column.type),
                        nullable: column.nullable,
                        sqliteDefaultSQL: nil,
                        isPrimaryKey: primaryKey.contains(column.name)
                    )
                }
            )
        }
    }
}
