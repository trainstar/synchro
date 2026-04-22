import Foundation
@testable import Synchro

typealias SchemaColumn = LocalSchemaColumn
typealias SchemaTable = LocalSchemaTable

extension LocalSchemaColumn {
    init(
        name: String,
        dbType: String = "text",
        logicalType: String = "string",
        nullable: Bool = true,
        defaultSQL: String? = nil,
        defaultKind: String = "none",
        sqliteDefaultSQL: String? = nil,
        isPrimaryKey: Bool = false
    ) {
        let _ = dbType
        let _ = defaultKind
        self.init(
            name: name,
            logicalType: SQLiteSchema.normalizedLogicalType(logicalType),
            nullable: nullable,
            sqliteDefaultSQL: sqliteDefaultSQL ?? defaultSQL,
            isPrimaryKey: isPrimaryKey
        )
    }
}

extension LocalSchemaTable {
    init(
        tableName: String,
        pushPolicy: String = "owner_only",
        parentTable: String? = nil,
        parentFKCol: String? = nil,
        dependencies: [String]? = nil,
        updatedAtColumn: String,
        deletedAtColumn: String,
        primaryKey: [String],
        bucketByColumn: String? = nil,
        bucketPrefix: String? = nil,
        globalWhenBucketNull: Bool? = nil,
        allowGlobalRead: Bool? = nil,
        bucketFunction: String? = nil,
        columns: [LocalSchemaColumn]
    ) {
        let _ = pushPolicy
        let _ = parentTable
        let _ = parentFKCol
        let _ = dependencies
        let _ = bucketByColumn
        let _ = bucketPrefix
        let _ = globalWhenBucketNull
        let _ = allowGlobalRead
        let _ = bucketFunction
        self.init(
            tableName: tableName,
            updatedAtColumn: updatedAtColumn,
            deletedAtColumn: deletedAtColumn,
            composition: nil,
            primaryKey: primaryKey,
            columns: columns
        )
    }

    fileprivate var testManifestTable: TableSchema {
        TableSchema(
            name: tableName,
            primaryKey: primaryKey,
            updatedAtColumn: updatedAtColumn,
            deletedAtColumn: deletedAtColumn,
            composition: composition,
            columns: columns.map { column in
                ColumnSchema(
                    name: column.name,
                    type: column.logicalType,
                    nullable: column.nullable
                )
            },
            indexes: nil
        )
    }

    var localSchema: LocalSchemaTable {
        self
    }
}

extension SchemaResponse {
    init(schemaVersion: Int64, schemaHash: String, serverTime: Date, tables: [LocalSchemaTable]) {
        self.init(
            schemaVersion: schemaVersion,
            schemaHash: schemaHash,
            serverTime: serverTime,
            manifest: SchemaManifest(tables: tables.map(\.testManifestTable))
        )
    }

    var tables: [SchemaTable] {
        (try? localTables()) ?? []
    }
}
