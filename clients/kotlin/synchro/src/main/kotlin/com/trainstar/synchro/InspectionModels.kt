package com.trainstar.synchro

/** A bounded read-only view of the durable local schema reference. */
data class SchemaInspection(
    val currentSchema: SchemaRef?,
)

/** A bounded read-only view of one durable server scope. */
data class ScopeInspection(
    val scopeID: String,
    val cursor: String?,
    val checksum: String?,
    val generation: Long,
    val localChecksum: String,
)

/** A bounded read-only view of one scope membership record. */
data class ScopeRowInspection(
    val scopeID: String,
    val tableName: String,
    val recordID: String,
    val checksum: String,
    val generation: Long,
)

/** A bounded read-only view of server metadata for one application row. */
data class RowMetadataInspection(
    val tableName: String,
    val recordID: String,
    val serverVersion: String,
    val rowChecksumJSON: String?,
)

/** A bounded read-only checkpoint view for one scope. */
data class CheckpointInspection(
    val scopeID: String,
    val cursor: String?,
    val checksum: String?,
    val localChecksum: String,
)

/** A bounded read-only view of the scopes that currently contain one row. */
data class ProvenanceInspection(
    val tableName: String,
    val recordID: String,
    val scopeIDs: List<String>,
    val serverVersion: String?,
)

/** A bounded read-only view of an unfinished rebuild. */
data class RebuildAttemptInspection(
    val scopeID: String,
    val rebuildID: String,
    val clientGeneration: Long,
    val schema: SchemaRef,
    val generation: Long,
    val cursor: String?,
    val pageLimit: Int,
)

/** A bounded read-only view of one durable rebuild page receipt. */
data class RebuildPageReceiptInspection(
    val scopeID: String,
    val rebuildID: String,
    val requestCursor: String?,
    val isFinal: Boolean,
    val finalScopeCursor: String?,
    val finalChecksumJSON: String?,
)

/** A bounded read-only view of rebuild attempts and receipts. */
data class RebuildStateInspection(
    val attempts: List<RebuildAttemptInspection>,
    val receipts: List<RebuildPageReceiptInspection>,
)
