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
        val fieldsByID = table.fields.associateBy { it.fieldID }
        val primaryKey = fieldsByID[table.primaryKeyFieldID]
            ?: throw ContractException("missing primary key field for ${table.name}")
        val updatedAtColumn = table.lifecycle.updatedAtFieldID?.let(fieldsByID::get)?.name.orEmpty()
        val deletedAtColumn = table.lifecycle.deletedAtFieldID?.let(fieldsByID::get)?.name.orEmpty()
        val columns = table.fields.takeIf { it.isNotEmpty() }
            ?: throw ContractException("missing columns for ${table.name}")

        LocalSchemaTable(
            tableName = table.name,
            updatedAtColumn = updatedAtColumn,
            deletedAtColumn = deletedAtColumn,
            composition = table.composition,
            primaryKey = listOf(primaryKey.name),
            columns = columns.map { column ->
                LocalSchemaColumn(
                    name = column.name,
                    logicalType = SQLiteSchema.normalizedLogicalType(column.typeName),
                    nullable = column.nullable,
                    sqliteDefaultSQL = null,
                    isPrimaryKey = column.fieldID == table.primaryKeyFieldID,
                )
            },
        )
    }
}
