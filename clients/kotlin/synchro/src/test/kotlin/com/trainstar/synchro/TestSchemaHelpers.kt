package com.trainstar.synchro

data class SchemaColumn(
    val name: String,
    val dbType: String = "text",
    val logicalType: String = "string",
    val nullable: Boolean = true,
    val defaultSQL: String? = null,
    val defaultKind: String = "none",
    val sqliteDefaultSQL: String? = null,
    val isPrimaryKey: Boolean = false,
)

data class SchemaTable(
    val tableName: String,
    val pushPolicy: String = "owner_only",
    val parentTable: String? = null,
    val parentFKCol: String? = null,
    val dependencies: List<String>? = null,
    val updatedAtColumn: String,
    val deletedAtColumn: String,
    val primaryKey: List<String>,
    val bucketByColumn: String? = null,
    val bucketPrefix: String? = null,
    val globalWhenBucketNull: Boolean? = null,
    val allowGlobalRead: Boolean? = null,
    val bucketFunction: String? = null,
    val columns: List<SchemaColumn>,
)

fun ColumnSchema(name: String, typeName: String, nullable: Boolean): ColumnSchema =
    ColumnSchema(
        fieldID = "field-$name",
        name = name,
        typeName = typeName,
        nullable = nullable,
        writable = name != "id" && name != "updated_at" && name != "deleted_at",
    )

fun TableSchema(
    name: String,
    primaryKey: List<String>,
    updatedAtColumn: String,
    deletedAtColumn: String,
    composition: CompositionClass,
    columns: List<ColumnSchema>,
    indexes: List<IndexSchema>?,
): TableSchema {
    val fieldsByName = columns.associate { it.name to it.fieldID }
    return TableSchema(
        tableID = "table-$name",
        relationID = "relation-$name",
        name = name,
        primaryKeyFieldID = fieldsByName.getValue(primaryKey.single()),
        lifecycle = LifecycleSchema(
            createdAtFieldID = fieldsByName["created_at"],
            updatedAtFieldID = fieldsByName[updatedAtColumn],
            deletedAtFieldID = fieldsByName[deletedAtColumn],
        ),
        composition = composition,
        fields = columns,
        indexes = indexes.orEmpty(),
    )
}

fun SchemaManifest(tables: List<TableSchema>): SchemaManifest =
    SchemaManifest(
        schemaVersion = 1,
        schemaHash = "0".repeat(64),
        parentSchema = null,
        transitionClass = "initial",
        compatibilityFloor = 1,
        tables = tables,
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
                logicalType = SQLiteSchema.normalizedLogicalType(it.logicalType),
                nullable = it.nullable,
                sqliteDefaultSQL = it.sqliteDefaultSQL ?: it.defaultSQL,
                isPrimaryKey = it.isPrimaryKey,
            )
        },
    )

private fun SchemaTable.toManifestTable(): TableSchema =
    TableSchema(
        name = tableName,
        primaryKey = primaryKey,
        updatedAtColumn = updatedAtColumn,
        deletedAtColumn = deletedAtColumn,
        composition = CompositionClass.SINGLE_SCOPE,
        columns = columns.map { column ->
            ColumnSchema(
                name = column.name,
                typeName = SQLiteSchema.normalizedLogicalType(column.logicalType),
                nullable = column.nullable,
            )
        },
        indexes = null,
    )

val com.trainstar.synchro.SchemaResponse.tables: List<SchemaTable>
    get() = manifest.tables.map { table ->
        val fieldsByID = table.fields.associateBy { it.fieldID }
        SchemaTable(
            tableName = table.name,
            updatedAtColumn = table.lifecycle.updatedAtFieldID?.let(fieldsByID::get)?.name.orEmpty(),
            deletedAtColumn = table.lifecycle.deletedAtFieldID?.let(fieldsByID::get)?.name.orEmpty(),
            primaryKey = listOf(fieldsByID.getValue(table.primaryKeyFieldID).name),
            columns = table.fields.map { column ->
                SchemaColumn(
                    name = column.name,
                    dbType = column.typeName,
                    logicalType = SQLiteSchema.normalizedLogicalType(column.typeName),
                    nullable = column.nullable,
                    isPrimaryKey = column.fieldID == table.primaryKeyFieldID,
                )
            },
        )
    }

@Suppress("FunctionName")
fun SchemaResponse(
    schemaVersion: Long,
    schemaHash: String,
    serverTime: String,
    tables: List<SchemaTable>,
): com.trainstar.synchro.SchemaResponse =
    com.trainstar.synchro.SchemaResponse(
        schemaVersion = schemaVersion,
        schemaHash = schemaHash,
        serverTime = serverTime,
        manifest = SchemaManifest(tables.map { it.toManifestTable() }),
    )
