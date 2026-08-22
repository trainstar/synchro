package com.trainstar.synchro

sealed class SynchroError(message: String, cause: Throwable? = null) : Exception(message, cause) {
    class NotConnected : SynchroError("Not connected to sync server")
    class SchemaNotLoaded : SynchroError("Schema has not been loaded from server")
    class TableNotSynced(val table: String) : SynchroError("Table '$table' is not a synced table")
    class UpgradeRequired(val currentVersion: String, val minimumVersion: String) :
        SynchroError("App version $currentVersion is below minimum $minimumVersion")

    class SchemaMismatch(val serverVersion: Long, val serverHash: String) :
        SynchroError("Schema mismatch: server version $serverVersion, hash $serverHash")

    class PushRejected(val results: List<RejectedMutation>) :
        SynchroError("Push rejected: ${results.size} mutation(s)")

    class NetworkError(val underlying: Throwable) :
        SynchroError("Network error: ${underlying.message}", underlying)

    class ServerError(val status: Int, val serverMessage: String) :
        SynchroError("Server error $status: $serverMessage")

    class DatabaseError(val underlying: Throwable) :
        SynchroError("Database error: ${underlying.message}", underlying)

    class InvalidResponse(val details: String) : SynchroError("Invalid response: $details")
    class BlockingFailure(val failure: SyncFailure) :
        SynchroError(failure.message)
    class UnsupportedSchema(val reason: SchemaUnsupportedReason) :
        SynchroError("Schema recovery is required: ${reason.name.lowercase()}")
    class InvalidStateTransition(
        val from: SyncLifecycleState,
        val to: SyncLifecycleState,
    ) : SynchroError(
        "Invalid sync state transition from ${from.wireName} to ${to.wireName}",
    )
    class AlreadyStarted : SynchroError("Sync has already been started")
    class NotStarted : SynchroError("Sync has not been started")
}

class RetryableError(
    val underlying: SynchroError,
    val retryAfter: Double?,
    val interruptedOperation: String,
    val workIdentity: String,
    val retryClassification: String,
    internal val retryAfterDeadlineMs: Long? = null,
) : Exception(underlying.message, underlying) {
    internal var persistedBackoff: DurableBackoffRecord? = null
}

internal object RetryOperation {
    const val CONNECTING = "connecting"
    const val PUSHING = "pushing"
    const val PULLING = "pulling"
    const val REBUILDING = "rebuilding"
}

internal object RetryClassification {
    const val NETWORK = "network"
    const val HTTP_429 = "http_429"
    const val HTTP_503 = "http_503"
}

/** The server invalidated one rebuild continuation, so only that scope must restart. */
internal class RebuildRestartRequiredException(
    val scopeID: String,
) : Exception("rebuild continuation requires restart")
