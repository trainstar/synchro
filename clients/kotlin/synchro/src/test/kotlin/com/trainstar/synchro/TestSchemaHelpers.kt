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
        composition = null,
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
        SchemaTable(
            tableName = table.name,
            updatedAtColumn = table.updatedAtColumn.orEmpty(),
            deletedAtColumn = table.deletedAtColumn.orEmpty(),
            primaryKey = table.primaryKey.orEmpty(),
            columns = table.columns.orEmpty().map { column ->
                SchemaColumn(
                    name = column.name,
                    dbType = column.typeName,
                    logicalType = SQLiteSchema.normalizedLogicalType(column.typeName),
                    nullable = column.nullable,
                    isPrimaryKey = table.primaryKey.orEmpty().contains(column.name),
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
