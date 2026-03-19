package com.trainstar.synchro

sealed class SynchroError(message: String, cause: Throwable? = null) : Exception(message, cause) {
    class NotConnected : SynchroError("Not connected to sync server")
    class SchemaNotLoaded : SynchroError("Schema has not been loaded from server")
    class TableNotSynced(val table: String) : SynchroError("Table '$table' is not a synced table")
    class UpgradeRequired(val currentVersion: String, val minimumVersion: String) :
        SynchroError("App version $currentVersion is below minimum $minimumVersion")

    class SchemaMismatch(val serverVersion: Long, val serverHash: String) :
        SynchroError("Schema mismatch: server version $serverVersion, hash $serverHash")

    class PushRejected(val results: List<PushResult>) :
        SynchroError("Push rejected: ${results.size} record(s)")

    class NetworkError(val underlying: Throwable) :
        SynchroError("Network error: ${underlying.message}", underlying)

    class ServerError(val status: Int, val serverMessage: String) :
        SynchroError("Server error $status: $serverMessage")

    class DatabaseError(val underlying: Throwable) :
        SynchroError("Database error: ${underlying.message}", underlying)

    class InvalidResponse(val details: String) : SynchroError("Invalid response: $details")
    class AlreadyStarted : SynchroError("Sync has already been started")
    class NotStarted : SynchroError("Sync has not been started")
}

class RetryableError(
    val underlying: SynchroError,
    val retryAfter: Double?
) : Exception(underlying.message, underlying)
