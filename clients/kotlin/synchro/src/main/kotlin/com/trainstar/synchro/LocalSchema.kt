package com.trainstar.synchro

import kotlinx.serialization.Serializable

@Serializable
data class LocalSchemaColumn(
    val name: String,
    val logicalType: String,
    val nullable: Boolean,
    val sqliteDefaultSQL: String? = null,
    val isPrimaryKey: Boolean,
)

@Serializable
data class LocalSchemaTable(
    val tableName: String,
    val updatedAtColumn: String,
    val deletedAtColumn: String,
    val composition: CompositionClass? = null,
    val primaryKey: List<String>,
    val columns: List<LocalSchemaColumn>,
)

fun SchemaManifest.localTables(): List<LocalSchemaTable> {
    validate()

    return tables.map { table ->
        val primaryKey = table.primaryKey?.takeIf { it.isNotEmpty() }
            ?: throw ContractException("missing primary key for ${table.name}")
        val updatedAtColumn = table.updatedAtColumn?.takeIf { it.isNotEmpty() }
            ?: throw ContractException("missing updated_at_column for ${table.name}")
        val deletedAtColumn = table.deletedAtColumn?.takeIf { it.isNotEmpty() }
            ?: throw ContractException("missing deleted_at_column for ${table.name}")
        val columns = table.columns?.takeIf { it.isNotEmpty() }
            ?: throw ContractException("missing columns for ${table.name}")

        val columnNames = columns.map { it.name }.toSet()
        for (columnName in primaryKey) {
            if (columnName !in columnNames) {
                throw ContractException("unknown primary key column ${table.name}.$columnName")
            }
        }
        if (updatedAtColumn !in columnNames) {
            throw ContractException("unknown updated_at column ${table.name}.$updatedAtColumn")
        }
        if (deletedAtColumn !in columnNames) {
            throw ContractException("unknown deleted_at column ${table.name}.$deletedAtColumn")
        }

        LocalSchemaTable(
            tableName = table.name,
            updatedAtColumn = updatedAtColumn,
            deletedAtColumn = deletedAtColumn,
            composition = table.composition,
            primaryKey = primaryKey,
            columns = columns.map { column ->
                LocalSchemaColumn(
                    name = column.name,
                    logicalType = SQLiteSchema.normalizedLogicalType(column.typeName),
                    nullable = column.nullable,
                    sqliteDefaultSQL = null,
                    isPrimaryKey = column.name in primaryKey,
                )
            },
        )
    }
}
