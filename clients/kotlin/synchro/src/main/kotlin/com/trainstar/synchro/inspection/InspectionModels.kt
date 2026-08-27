package com.trainstar.synchro.inspection

import com.trainstar.synchro.SchemaRef

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

/** A process-local cursor for committed scope-row maintenance work. */
data class ProvenanceMaintenanceWorkInspection(
    val cursor: Long,
)

/** A bounded read-only aggregate of durable client state and maintenance work. */
data class ClientStateInspection(
    val schema: SchemaRef?,
    val scopeStates: List<ScopeInspection>,
    val scopeRows: List<ScopeRowInspection>,
    val rebuildAttempts: List<RebuildAttemptInspection>,
    val provenanceMaintenanceWorkCursor: Long,
)

/** Exact counts for durable state that can exceed detailed inspection bounds. */
data class ClientStateCountsInspection(
    val schema: SchemaRef?,
    val applicationRowCount: Int,
    val mutationLedgerCount: Int,
    val mutationOutcomeCount: Int,
    val sealedBatchCount: Int,
    val rejectedMutationCount: Int,
    val scopeStateCount: Int,
    val scopeRowCount: Int,
    val provenanceCount: Int,
    val rowMetadataCount: Int,
    val rebuildAttemptCount: Int,
    val rebuildReceiptCount: Int,
    val provenanceMaintenanceWorkCursor: Long,
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

/** Normalized facts for one durable rebuild receipt chain. */
data class RebuildReceiptInspection(
    val rebuildIDFingerprint: String,
    val pageCount: Int,
    val returnedRecordCount: Int,
    val requestChainExpected: List<String>,
    val requestChainObserved: List<String>,
    val recordIdentitiesHex: List<String>,
    val receivedRowChecksums: List<String>,
    val computedRowChecksums: List<String>,
    val computedScopeChecksum: String?,
    val finalScopeChecksum: String?,
    val storedScopeChecksum: String?,
    val localScopeChecksum: String?,
)
