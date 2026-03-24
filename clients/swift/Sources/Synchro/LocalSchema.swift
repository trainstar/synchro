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
    let composition: VNextCompositionClass?
    let primaryKey: [String]
    let columns: [LocalSchemaColumn]
}

extension SchemaTable {
    var localSchema: LocalSchemaTable {
        LocalSchemaTable(
            tableName: tableName,
            updatedAtColumn: updatedAtColumn,
            deletedAtColumn: deletedAtColumn,
            composition: nil,
            primaryKey: primaryKey,
            columns: columns.map {
                LocalSchemaColumn(
                    name: $0.name,
                    logicalType: $0.logicalType,
                    nullable: $0.nullable,
                    sqliteDefaultSQL: $0.sqliteDefaultSQL,
                    isPrimaryKey: $0.isPrimaryKey
                )
            }
        )
    }
}

extension VNextSchemaManifest {
    func localTables() throws -> [LocalSchemaTable] {
        try validate()

        return try tables.map { table in
            guard let primaryKey = table.primaryKey, !primaryKey.isEmpty else {
                throw VNextContractViolation.missingPrimaryKey(tableName: table.name)
            }
            guard let updatedAtColumn = table.updatedAtColumn, !updatedAtColumn.isEmpty else {
                throw VNextContractViolation.missingUpdatedAtColumn(tableName: table.name)
            }
            guard let deletedAtColumn = table.deletedAtColumn, !deletedAtColumn.isEmpty else {
                throw VNextContractViolation.missingDeletedAtColumn(tableName: table.name)
            }
            guard let columns = table.columns, !columns.isEmpty else {
                throw VNextContractViolation.missingColumns(tableName: table.name)
            }

            let columnNames = Set(columns.map(\.name))
            for columnName in primaryKey where !columnNames.contains(columnName) {
                throw VNextContractViolation.unknownPrimaryKeyColumn(tableName: table.name, columnName: columnName)
            }
            if !columnNames.contains(updatedAtColumn) {
                throw VNextContractViolation.unknownUpdatedAtColumn(tableName: table.name, columnName: updatedAtColumn)
            }
            if !columnNames.contains(deletedAtColumn) {
                throw VNextContractViolation.unknownDeletedAtColumn(tableName: table.name, columnName: deletedAtColumn)
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
                        logicalType: column.type,
                        nullable: column.nullable,
                        sqliteDefaultSQL: nil,
                        isPrimaryKey: primaryKey.contains(column.name)
                    )
                }
            )
        }
    }
}
