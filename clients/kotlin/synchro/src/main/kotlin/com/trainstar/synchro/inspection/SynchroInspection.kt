package com.trainstar.synchro.inspection

import com.trainstar.synchro.ApplicationReadTransaction
import com.trainstar.synchro.SynchroClient
import com.trainstar.synchro.SynchroConfig
import com.trainstar.synchro.SchemaRef

@SynchroProofApi
fun SynchroConfig.withTransportObservation(collector: TransportObservationCollector): SynchroConfig =
    withTransportObservationCollector(collector)

@SynchroProofApi
fun SynchroClient.provenanceMaintenanceWorkCursor(): Long = inspectProvenanceMaintenanceWorkCursor()

/** Shipped inspection access that is outside the primary client API. */
@SynchroProofApi
class SynchroInspection(private val client: SynchroClient) {
    fun currentSchema(): SchemaRef? = client.inspectCurrentSchema()

    fun scopeStates(): List<ScopeStateInspection> = client.inspectScopeStates()

    fun scopeRows(): List<ScopeRowInspection> = client.inspectScopeRows()

    fun captureState(maximumRecords: Int): ClientStateCaptureInspection =
        client.inspectClientStateCapture(maximumRecords)

    /**
     * Reads counts, retained details, and application rows from one read-only
     * snapshot. [readApplicationRows] runs inside that snapshot.
     */
    fun captureSnapshot(
        maximumRecords: Int,
        readApplicationRows: (ClientStateCaptureInspection, ApplicationReadTransaction) -> Unit,
    ): ClientStateSnapshotInspection = client.inspectClientStateSnapshot(maximumRecords, readApplicationRows)

    fun rowMetadata(tableName: String, recordID: String): RowMetadataInspection? =
        client.inspectRowMetadata(tableName, recordID)

    fun rebuildAttempts(): List<RebuildAttemptInspection> = client.inspectRebuildAttempts()

    fun rebuildReceipts(): List<RebuildReceiptInspection> = client.inspectRebuildReceipts()
}
