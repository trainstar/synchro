package com.trainstar.synchro

import kotlinx.serialization.Serializable

@Serializable
data class LocalSchemaColumn(
    val fieldID: String,
    val name: String,
    val logicalType: String,
    val nullable: Boolean,
    val writable: Boolean,
    val precision: Int? = null,
    val scale: Int? = null,
    val sqliteDefaultSQL: String? = null,
    val isPrimaryKey: Boolean,
)

@Serializable
data class LocalSchemaIndex(
    val indexID: String,
    val name: String,
    val columnNames: List<String>,
    val unique: Boolean,
)

@Serializable
data class LocalSchemaTable(
    val tableID: String,
    val relationID: String,
    val tableName: String,
    val primaryKeyFieldID: String,
    val createdAtFieldID: String? = null,
    val updatedAtFieldID: String? = null,
    val deletedAtFieldID: String? = null,
    val updatedAtColumn: String,
    val deletedAtColumn: String,
    val composition: CompositionClass? = null,
    val primaryKey: List<String>,
    val columns: List<LocalSchemaColumn>,
    val indexes: List<LocalSchemaIndex> = emptyList(),
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
            tableID = table.tableID,
            relationID = table.relationID,
            tableName = table.name,
            primaryKeyFieldID = table.primaryKeyFieldID,
            createdAtFieldID = table.lifecycle.createdAtFieldID,
            updatedAtFieldID = table.lifecycle.updatedAtFieldID,
            deletedAtFieldID = table.lifecycle.deletedAtFieldID,
            updatedAtColumn = updatedAtColumn,
            deletedAtColumn = deletedAtColumn,
            composition = table.composition,
            primaryKey = listOf(primaryKey.name),
            columns = columns.map { column ->
                LocalSchemaColumn(
                    fieldID = column.fieldID,
                    name = column.name,
                    logicalType = column.typeName,
                    nullable = column.nullable,
                    writable = column.writable,
                    precision = column.precision,
                    scale = column.scale,
                    sqliteDefaultSQL = null,
                    isPrimaryKey = column.fieldID == table.primaryKeyFieldID,
                )
            },
            indexes = table.indexes.map { index ->
                LocalSchemaIndex(
                    indexID = index.indexID,
                    name = index.name,
                    columnNames = index.fieldIDs.map { fieldID ->
                        fieldsByID[fieldID]?.name
                            ?: throw ContractException("unknown index field for ${table.name}")
                    },
                    unique = index.unique,
                )
            },
        )
    }
}
