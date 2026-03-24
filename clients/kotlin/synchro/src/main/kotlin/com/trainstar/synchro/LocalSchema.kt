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
    val composition: VNextCompositionClass? = null,
    val primaryKey: List<String>,
    val columns: List<LocalSchemaColumn>,
)

val SchemaTable.localSchema: LocalSchemaTable
    get() = LocalSchemaTable(
        tableName = tableName,
        updatedAtColumn = updatedAtColumn,
        deletedAtColumn = deletedAtColumn,
        composition = null,
        primaryKey = primaryKey,
        columns = columns.map {
            LocalSchemaColumn(
                name = it.name,
                logicalType = it.logicalType,
                nullable = it.nullable,
                sqliteDefaultSQL = it.sqliteDefaultSQL,
                isPrimaryKey = it.isPrimaryKey,
            )
        },
    )

fun VNextSchemaManifest.localTables(): List<LocalSchemaTable> {
    validate()

    return tables.map { table ->
        val primaryKey = table.primaryKey?.takeIf { it.isNotEmpty() }
            ?: throw VNextContractException("missing primary key for ${table.name}")
        val updatedAtColumn = table.updatedAtColumn?.takeIf { it.isNotEmpty() }
            ?: throw VNextContractException("missing updated_at_column for ${table.name}")
        val deletedAtColumn = table.deletedAtColumn?.takeIf { it.isNotEmpty() }
            ?: throw VNextContractException("missing deleted_at_column for ${table.name}")
        val columns = table.columns?.takeIf { it.isNotEmpty() }
            ?: throw VNextContractException("missing columns for ${table.name}")

        val columnNames = columns.map { it.name }.toSet()
        for (columnName in primaryKey) {
            if (columnName !in columnNames) {
                throw VNextContractException("unknown primary key column ${table.name}.$columnName")
            }
        }
        if (updatedAtColumn !in columnNames) {
            throw VNextContractException("unknown updated_at column ${table.name}.$updatedAtColumn")
        }
        if (deletedAtColumn !in columnNames) {
            throw VNextContractException("unknown deleted_at column ${table.name}.$deletedAtColumn")
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
                    logicalType = column.typeName,
                    nullable = column.nullable,
                    sqliteDefaultSQL = null,
                    isPrimaryKey = column.name in primaryKey,
                )
            },
        )
    }
}
