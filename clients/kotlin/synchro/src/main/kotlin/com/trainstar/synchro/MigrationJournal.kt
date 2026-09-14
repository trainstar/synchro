package com.trainstar.synchro

import kotlinx.serialization.Serializable
import java.security.MessageDigest

/** The persisted schema migration phases have one deterministic meaning. */
@Serializable
internal enum class MigrationPhase {
    @kotlinx.serialization.SerialName("prepared") PREPARED,
    @kotlinx.serialization.SerialName("ddl_applied") DDL_APPLIED,
    @kotlinx.serialization.SerialName("awaiting_rebuild") AWAITING_REBUILD,
}

@Serializable
internal enum class MigrationOperationKind {
    @kotlinx.serialization.SerialName("create_table") CREATE_TABLE,
    @kotlinx.serialization.SerialName("drop_table") DROP_TABLE,
    @kotlinx.serialization.SerialName("add_columns") ADD_COLUMNS,
    @kotlinx.serialization.SerialName("drop_index") DROP_INDEX,
    @kotlinx.serialization.SerialName("create_index") CREATE_INDEX,
    @kotlinx.serialization.SerialName("replace_synced_materialization") REPLACE_SYNCED_MATERIALIZATION,
}

/** A typed operation contains no caller-provided raw SQL. */
@Serializable
internal data class MigrationOperation(
    val kind: MigrationOperationKind,
    val table: LocalSchemaTable? = null,
    val columnFieldIDs: List<String> = emptyList(),
    val index: LocalSchemaIndex? = null,
)

@Serializable
internal data class MigrationPlan(
    val version: Int,
    val source: SchemaRef,
    val target: SchemaRef,
    val resetMaterialization: Boolean,
    val operations: List<MigrationOperation>,
)

internal data class LocalMigrationJournal(
    val journalVersion: Int,
    val source: SchemaRef,
    val target: SchemaRef,
    val action: SchemaAction,
    val affectedScopes: List<String>,
    val scopeCursorUpdates: Map<String, String?>,
    val targetManifest: SchemaManifest,
    val targetTables: List<LocalSchemaTable>,
    val plan: MigrationPlan,
    val planHash: String,
    val resetMaterialization: Boolean,
    val phase: MigrationPhase,
)

internal fun migrationPlanHash(planJSON: String): String = Integrity.hex(
    MessageDigest.getInstance("SHA-256").digest(planJSON.toByteArray(Charsets.UTF_8)),
)
