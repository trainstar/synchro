package com.trainstar.synchro

internal const val RETRYABLE_429_ERROR_JSON =
    "{\"error\":{\"code\":\"retry_later\",\"message\":\"retry later\",\"retryable\":true}}"
internal const val RETRYABLE_503_ERROR_JSON =
    "{\"error\":{\"code\":\"temporary_unavailable\",\"message\":\"temporarily unavailable\",\"retryable\":true}}"

internal fun installDurableBackoff(
    database: SynchroDatabase,
    resumeState: String,
    workIdentity: String,
    nextRetryAtMs: Long = 0L,
) {
    database.execute(
        """
        INSERT INTO _synchro_backoff (
            singleton, resume_state, work_identity, retry_classification,
            attempt_count, next_retry_at_ms
        ) VALUES (1, ?, ?, 'network', 1, ?)
        ON CONFLICT (singleton) DO UPDATE SET
            resume_state = excluded.resume_state,
            work_identity = excluded.work_identity,
            retry_classification = excluded.retry_classification,
            attempt_count = excluded.attempt_count,
            next_retry_at_ms = excluded.next_retry_at_ms
        """.trimIndent(),
        arrayOf(resumeState, workIdentity, nextRetryAtMs),
    )
}

val PROTOCOL_TEST_SCHEMA_HASH = Integrity.schemaManifestHash(protocolOrdersSchemaManifest())

fun protocolOrdersSchemaManifest(
    includeNotes: Boolean = false,
    schemaVersion: Long = 1,
    parentSchema: SchemaRef? = null,
    transitionClass: String = "initial",
    compatibilityFloor: Long = 1,
): SchemaManifest {
    val fields = mutableListOf(
        ColumnSchema("field-id", "id", "string", false, false),
        ColumnSchema("field-ship-address", "ship_address", "string", true, true),
        ColumnSchema("field-user-id", "user_id", "string", false, true),
        ColumnSchema("field-updated-at", "updated_at", "datetime", false, false),
        ColumnSchema("field-deleted-at", "deleted_at", "datetime", true, false),
    )
    if (includeNotes) {
        fields += ColumnSchema("field-notes", "notes", "string", true, true)
    }
    return SchemaManifest(
        schemaVersion = schemaVersion,
        schemaHash = "0".repeat(64),
        parentSchema = parentSchema,
        transitionClass = transitionClass,
        compatibilityFloor = compatibilityFloor,
        tables = listOf(
            TableSchema(
                tableID = "table-orders",
                relationID = "relation-orders",
                name = "orders",
                primaryKeyFieldID = "field-id",
                lifecycle = LifecycleSchema(
                    updatedAtFieldID = "field-updated-at",
                    deletedAtFieldID = "field-deleted-at",
                ),
                composition = CompositionClass.SINGLE_SCOPE,
                fields = fields,
                indexes = emptyList(),
            ),
        ),
    )
}

data class SchemaColumn(
    val name: String,
    val dbType: String = "text",
    val logicalType: String = "string",
    val nullable: Boolean = true,
    val precision: Int? = null,
    val scale: Int? = null,
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
    val columns: List<SchemaColumn>,
)

@Suppress("FunctionName")
fun LocalSchemaColumn(
    name: String,
    logicalType: String,
    nullable: Boolean,
    sqliteDefaultSQL: String? = null,
    isPrimaryKey: Boolean,
): com.trainstar.synchro.LocalSchemaColumn =
    com.trainstar.synchro.LocalSchemaColumn(
        fieldID = name,
        name = name,
        logicalType = logicalType,
        nullable = nullable,
        writable = !isPrimaryKey && name != "created_at" && name != "updated_at" && name != "deleted_at",
        sqliteDefaultSQL = sqliteDefaultSQL,
        isPrimaryKey = isPrimaryKey,
    )

@Suppress("FunctionName")
fun LocalSchemaTable(
    tableName: String,
    updatedAtColumn: String,
    deletedAtColumn: String,
    composition: CompositionClass? = null,
    primaryKey: List<String>,
    columns: List<LocalSchemaColumn>,
): com.trainstar.synchro.LocalSchemaTable =
    com.trainstar.synchro.LocalSchemaTable(
        tableID = tableName,
        relationID = "relation-$tableName",
        tableName = tableName,
        primaryKeyFieldID = primaryKey.single(),
        createdAtFieldID = columns.firstOrNull { it.name == "created_at" }?.fieldID,
        updatedAtFieldID = columns.firstOrNull { it.name == updatedAtColumn }?.fieldID,
        deletedAtFieldID = columns.firstOrNull { it.name == deletedAtColumn }?.fieldID,
        updatedAtColumn = updatedAtColumn,
        deletedAtColumn = deletedAtColumn,
        composition = composition,
        primaryKey = primaryKey,
        columns = columns,
    )

fun ColumnSchema(
    name: String,
    typeName: String,
    nullable: Boolean,
    precision: Int? = null,
    scale: Int? = null,
): ColumnSchema =
    ColumnSchema(
        fieldID = name,
        name = name,
        typeName = typeName,
        nullable = nullable,
        writable = name != "id" && name != "updated_at" && name != "deleted_at",
        precision = precision,
        scale = scale,
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
        tableID = name,
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
        tableID = tableName,
        relationID = "relation-$tableName",
        tableName = tableName,
        primaryKeyFieldID = primaryKey.single(),
        createdAtFieldID = columns.firstOrNull { it.name == "created_at" }?.name,
        updatedAtFieldID = columns.firstOrNull { it.name == updatedAtColumn }?.name,
        deletedAtFieldID = columns.firstOrNull { it.name == deletedAtColumn }?.name,
        updatedAtColumn = updatedAtColumn,
        deletedAtColumn = deletedAtColumn,
        composition = null,
        primaryKey = primaryKey,
        columns = columns.map {
            LocalSchemaColumn(
                fieldID = it.name,
                name = it.name,
                logicalType = it.logicalType,
                nullable = it.nullable,
                writable = !it.isPrimaryKey && it.name != "created_at" && it.name != updatedAtColumn && it.name != deletedAtColumn,
                precision = it.precision,
                scale = it.scale,
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
                fieldID = column.name,
                name = column.name,
                typeName = column.logicalType,
                nullable = column.nullable,
                writable = !column.isPrimaryKey &&
                    column.name != "created_at" &&
                    column.name != updatedAtColumn &&
                    column.name != deletedAtColumn,
                precision = column.precision,
                scale = column.scale,
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
                    logicalType = column.typeName,
                    nullable = column.nullable,
                    precision = column.precision,
                    scale = column.scale,
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

fun makeAcceptedMutation(
    mutationID: String,
    schema: LocalSchemaTable,
    pk: kotlinx.serialization.json.JsonObject,
    status: MutationStatus,
    serverRow: kotlinx.serialization.json.JsonObject?,
    serverVersion: String,
): com.trainstar.synchro.AcceptedMutation {
    return com.trainstar.synchro.AcceptedMutation(
        mutationID = mutationID,
        table = schema.tableID,
        pk = pk,
        outcomeSchema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
        status = status,
        serverRow = serverRow,
        rowChecksum = serverRow?.let { Integrity.rowDigest(PROTOCOL_TEST_SCHEMA_HASH, schema, pk, it, serverVersion).checksum },
        serverVersion = serverVersion,
    )
}

fun makeRejectedMutation(
    mutationID: String,
    schema: LocalSchemaTable,
    pk: kotlinx.serialization.json.JsonObject,
    status: MutationStatus,
    code: MutationRejectionCode,
    message: String,
    retryable: Boolean? = null,
    serverRow: kotlinx.serialization.json.JsonObject? = null,
    serverVersion: String? = null,
): com.trainstar.synchro.RejectedMutation {
    return com.trainstar.synchro.RejectedMutation(
        mutationID = mutationID,
        table = schema.tableID,
        pk = pk,
        outcomeSchema = SchemaRef(1, PROTOCOL_TEST_SCHEMA_HASH),
        status = status,
        code = code,
        message = message,
        retryable = retryable,
        serverRow = serverRow,
        rowChecksum = if (serverRow != null && serverVersion != null) {
            Integrity.rowDigest(PROTOCOL_TEST_SCHEMA_HASH, schema, pk, serverRow, serverVersion).checksum
        } else {
            null
        },
        serverVersion = serverVersion,
    )
}

fun makeChangeRecord(
    scope: String,
    schema: LocalSchemaTable,
    op: Operation,
    pk: kotlinx.serialization.json.JsonObject,
    row: kotlinx.serialization.json.JsonObject?,
    serverVersion: String,
): ChangeRecord = ChangeRecord(
    scope = scope,
    table = schema.tableID,
    op = op,
    pk = pk,
    row = row,
    rowChecksum = row?.let {
        Integrity.rowDigest(PROTOCOL_TEST_SCHEMA_HASH, schema, pk, it, serverVersion).checksum
    },
    serverVersion = serverVersion,
)

fun protocolEmptyScopeChecksum(scopeID: String): ChecksumObject =
    Integrity.scopeDigest(PROTOCOL_TEST_SCHEMA_HASH, scopeID, emptyList())
