package com.trainstar.synchro.inspection

import com.trainstar.synchro.SynchroClient
import com.trainstar.synchro.SynchroConfig

fun SynchroConfig.withTransportObservation(collector: TransportObservationCollector): SynchroConfig =
    withTransportObservationCollector(collector)

/** Shipped inspection access that is outside the primary client API. */
class SynchroInspection(private val client: SynchroClient) {
    fun schema(): SchemaInspection = client.inspectSchema()

    fun scopes(): List<ScopeInspection> = client.inspectScopes()

    fun scopeRows(): List<ScopeRowInspection> = client.inspectScopeRows()

    fun rowMetadata(): List<RowMetadataInspection> = client.inspectRowMetadata()

    fun checkpoints(): List<CheckpointInspection> = client.inspectCheckpoints()

    fun provenance(): List<ProvenanceInspection> = client.inspectProvenance()

    fun provenanceMaintenanceWork(): ProvenanceMaintenanceWorkInspection =
        client.inspectProvenanceMaintenanceWork()

    fun clientState(): ClientStateInspection = client.inspectClientState()

    fun clientStateCounts(): ClientStateCountsInspection = client.inspectClientStateCounts()

    fun rebuildState(): RebuildStateInspection = client.inspectRebuildState()

    fun rebuildReceipts(): List<RebuildReceiptInspection> = client.inspectRebuildReceipts()
}
