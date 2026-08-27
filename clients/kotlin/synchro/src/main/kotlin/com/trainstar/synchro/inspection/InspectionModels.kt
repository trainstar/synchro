package com.trainstar.synchro.inspection

import com.trainstar.synchro.SchemaRef

/** A bounded read-only view of one durable server scope. */
@SynchroProofApi
data class ScopeStateInspection(
    val scopeID: String,
    val cursor: String?,
    val checksum: String?,
    val localChecksum: String,
    val generation: Long,
)

/** A bounded read-only view of one scope membership record. */
@SynchroProofApi
data class ScopeRowInspection(
    val scopeID: String,
    val tableName: String,
    val recordID: String,
    val checksum: String,
    val generation: Long,
)

/** A bounded read-only view of server metadata for one application row. */
@SynchroProofApi
data class RowMetadataInspection(
    val tableName: String,
    val recordID: String,
    val serverVersion: String,
    val rowChecksum: String?,
)

/** One bounded atomic capture of durable client state and exact counts. */
@SynchroProofApi
data class ClientStateCaptureInspection(
    val schema: SchemaRef?,
    val scopeStates: List<ScopeStateInspection>,
    val scopeStatesTruncated: Boolean,
    val scopeRows: List<ScopeRowInspection>,
    val scopeRowsTruncated: Boolean,
    val rebuildAttempts: List<RebuildAttemptInspection>,
    val rebuildAttemptsTruncated: Boolean,
    val rebuildReceipts: List<RebuildReceiptInspection>,
    val rebuildReceiptsTruncated: Boolean,
    val rowMetadata: List<RowMetadataInspection>,
    val rowMetadataTruncated: Boolean,
    val overflowed: Boolean,
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
@SynchroProofApi
data class RebuildAttemptInspection(
    val scopeID: String,
    val rebuildID: String,
    val clientGeneration: Long,
    val schemaVersion: Long,
    val schemaHash: String,
    val generation: Long,
    val cursor: String?,
    val pageLimit: Int,
)

/** Normalized facts for one durable rebuild receipt chain. */
@SynchroProofApi
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
