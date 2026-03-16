---
title: "Kotlin / Android"
description: "Kotlin SDK for Android with SQLiteOpenHelper-backed sync."
---

## Installation

### Gradle (Kotlin DSL)

```kotlin
dependencies {
    implementation("fit.trainstar:synchro:0.1.0")
}
```

### Gradle (Groovy)

```groovy
dependencies {
    implementation 'fit.trainstar:synchro:0.1.0'
}
```

**Android:** minSdk 24, compileSdk 34

**Dependencies:** [OkHttp](https://square.github.io/okhttp/) 4.12+, [Kotlinx Serialization](https://github.com/Kotlin/kotlinx.serialization) 1.6+, [Kotlinx Coroutines](https://github.com/Kotlin/kotlinx.coroutines) 1.8+

## Configuration

```kotlin
val config = SynchroConfig(
    dbPath = "synchro.db",
    serverURL = "https://api.example.com",
    authProvider = { getToken() },
    clientID = Settings.Secure.getString(contentResolver, Settings.Secure.ANDROID_ID),
    appVersion = "1.0.0"
)
val client = SynchroClient(config, context)
```

### SynchroConfig Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dbPath` | `String` | Required | SQLite database file name or path |
| `serverURL` | `String` | Required | Sync server base URL |
| `authProvider` | `suspend () -> String` | Required | Returns a JWT token for authentication |
| `clientID` | `String` | Required | Unique device identifier |
| `platform` | `String` | `"android"` | Platform name sent during registration |
| `appVersion` | `String` | Required | Semantic version of the app |
| `syncInterval` | `Double` | `30.0` | Seconds between sync cycles |
| `pushDebounce` | `Double` | `0.5` | Seconds after a write before triggering push |
| `maxRetryAttempts` | `Int` | `5` | Maximum retry count before entering error state |
| `pullPageSize` | `Int` | `100` | Rows per pull page (validated 1--1000) |
| `pushBatchSize` | `Int` | `100` | Pending changes per push batch (validated 1--1000) |
| `snapshotPageSize` | `Int` | `100` | Rows per snapshot page (validated 1--1000) |
| `seedDatabasePath` | `String?` | `null` | Path to a pre-built seed database for offline-first bootstrap |

:::note[Validation]
`pullPageSize`, `pushBatchSize`, and `snapshotPageSize` are validated at construction time. An `IllegalArgumentException` is thrown if any value is outside the 1--1000 range.
:::

## Core Usage

### Queries

```kotlin
// Fetch multiple rows
val rows = client.query(
    "SELECT * FROM tasks WHERE user_id = ?",
    params = arrayOf(userId)
)

// Fetch a single row
val task = client.queryOne(
    "SELECT * FROM tasks WHERE id = ?",
    params = arrayOf(id)
)
```

`Row` is a type alias for `Map<String, Any?>`. Column values are returned as their SQLite-native types: `Long` for integers, `Double` for floats, `String` for text, `ByteArray` for blobs, and `null` for NULL.

### Writes

```kotlin
val result = client.execute(
    "INSERT INTO tasks (id, title, user_id) VALUES (?, ?, ?)",
    params = arrayOf(UUID.randomUUID().toString(), "Review proposal", userId)
)
// result.rowsAffected == 1
```

:::note[CDC triggers track writes automatically]
Any INSERT, UPDATE, or DELETE on a synced table is captured by SQLite triggers and queued for push. No special write API is needed.
:::

### Batch Execution

```kotlin
val affected = client.executeBatch(listOf(
    SQLStatement(
        sql = "INSERT INTO tasks (id, title, user_id) VALUES (?, ?, ?)",
        params = arrayOf(UUID.randomUUID().toString(), "Write report", userId)
    ),
    SQLStatement(
        sql = "INSERT INTO tasks (id, title, user_id) VALUES (?, ?, ?)",
        params = arrayOf(UUID.randomUUID().toString(), "Update docs", userId)
    ),
))
// affected == 2
```

## Transactions

```kotlin
// Write transaction: multiple operations atomically
client.writeTransaction { db ->
    db.execSQL(
        "INSERT INTO tasks (id, title, user_id) VALUES (?, ?, ?)",
        arrayOf(UUID.randomUUID().toString(), "Review proposal", userId)
    )
    db.execSQL(
        "INSERT INTO comments (id, task_id, body, status) VALUES (?, ?, ?, ?)",
        arrayOf(UUID.randomUUID().toString(), taskId, "Looks good", "open")
    )
}

// Read transaction: consistent snapshot
val count = client.readTransaction { db ->
    db.rawQuery("SELECT COUNT(*) FROM tasks", null).use { cursor ->
        if (cursor.moveToFirst()) cursor.getInt(0) else 0
    }
}
```

## Observation

### Change Notification

```kotlin
val cancellable = client.onChange(listOf("tasks")) {
    println("tasks table changed")
}

// Later: stop observing
cancellable.cancel()
```

### Reactive Query

```kotlin
val cancellable = client.watch(
    "SELECT * FROM tasks ORDER BY created_at DESC",
    tables = listOf("tasks")
) { rows ->
    this.tasks = rows
}
```

The callback fires immediately with the current result set, then again whenever the observed tables change. The SDK uses a SQL-parsing heuristic to detect which tables are affected by each write.

## Sync Control

```kotlin
// Start sync: register, fetch schema, begin sync loop
client.start()

// Trigger an immediate sync cycle
client.syncNow()

// Stop the sync loop
client.stop()

// Close the database and HTTP connections
client.close()
```

:::note[`start()` and `syncNow()` are suspend functions]
Both `start()` and `syncNow()` are Kotlin suspend functions. Call them from a coroutine scope:

```kotlin
lifecycleScope.launch {
    client.start()
}
```
:::

## Status and Events

### Sync Status

```kotlin
val cancellable = client.onStatusChange { status ->
    when (status) {
        is SyncStatus.Idle -> println("Idle")
        is SyncStatus.Syncing -> println("Syncing...")
        is SyncStatus.Error -> {
            val retryAt = status.retryAt
            if (retryAt != null) {
                println("Error, retrying at $retryAt")
            } else {
                println("Error, no retry scheduled")
            }
        }
        is SyncStatus.Stopped -> println("Stopped")
    }
}
```

`SyncStatus` is a sealed class:

```kotlin
sealed class SyncStatus {
    data object Idle : SyncStatus()
    data object Syncing : SyncStatus()
    data class Error(val retryAt: java.time.Instant?) : SyncStatus()
    data object Stopped : SyncStatus()
}
```

### Conflict Events

```kotlin
val cancellable = client.onConflict { event ->
    println("Conflict on ${event.table} record ${event.recordID}")
    println("Client data: ${event.clientData}")
    println("Server data: ${event.serverData}")
}
```

### Snapshot Required

```kotlin
val cancellable = client.onSnapshotRequired {
    // Prompt the user or decide programmatically
    // Return true to proceed with snapshot, false to abort
    promptUser("Full resync needed. Continue?")
}
```

:::caution[Snapshot callback is required for recovery]
If no `onSnapshotRequired` handler is registered and the server requests a snapshot (due to bucket reassignment, compaction, or data loss), the sync engine will enter an error state. Always register a handler.
:::

## Error Handling

All errors are represented by the `SynchroError` sealed class:

```kotlin
sealed class SynchroError(message: String, cause: Throwable? = null)
    : Exception(message, cause) {

    class NotConnected : SynchroError(...)
    class SchemaNotLoaded : SynchroError(...)
    class TableNotSynced(val table: String) : SynchroError(...)
    class UpgradeRequired(
        val currentVersion: String,
        val minimumVersion: String
    ) : SynchroError(...)
    class SchemaMismatch(
        val serverVersion: Long,
        val serverHash: String
    ) : SynchroError(...)
    class SnapshotRequired : SynchroError(...)
    class PushRejected(val results: List<PushResult>) : SynchroError(...)
    class NetworkError(val underlying: Throwable) : SynchroError(...)
    class ServerError(
        val status: Int,
        val serverMessage: String
    ) : SynchroError(...)
    class DatabaseError(val underlying: Throwable) : SynchroError(...)
    class InvalidResponse(val details: String) : SynchroError(...)
    class AlreadyStarted : SynchroError(...)
    class NotStarted : SynchroError(...)
}
```

| Class | When It Occurs |
|-------|----------------|
| `NotConnected` | Operation attempted before `start()` completes registration |
| `SchemaNotLoaded` | Schema has not been fetched from server yet |
| `TableNotSynced` | Write attempted on a table not in the server schema |
| `UpgradeRequired` | Server rejected the client's app version (HTTP 426) |
| `SchemaMismatch` | Client schema hash does not match server (HTTP 409) |
| `SnapshotRequired` | Server indicates a full snapshot is needed |
| `PushRejected` | One or more push records were rejected by the server |
| `NetworkError` | Network connectivity failure |
| `ServerError` | Server returned a non-200 HTTP status |
| `DatabaseError` | SQLite operation failed |
| `InvalidResponse` | Server response could not be decoded |
| `AlreadyStarted` | `start()` called when sync is already running |
| `NotStarted` | `syncNow()` called before `start()` |

Errors follow standard Kotlin exception handling:

```kotlin
try {
    client.start()
} catch (e: SynchroError.UpgradeRequired) {
    showUpgradeDialog(e.minimumVersion)
} catch (e: SynchroError.NetworkError) {
    showOfflineBanner()
} catch (e: SynchroError) {
    log.error("Sync error", e)
}
```

## Seed Database (Optional)

Ship a pre-built SQLite file so the app works offline on first launch — no server required. Useful when users may not have connectivity on first open, or when your onboarding flow writes data before sign-in.

Without a seed, tables are created on first `start()` from the server schema. The seed removes that dependency.

```kotlin
// Copy seed from assets to files dir on first install
val seedFile = File(filesDir, "seed.db")
if (!seedFile.exists()) {
    assets.open("seed.db").use { input ->
        seedFile.outputStream().use { output -> input.copyTo(output) }
    }
}

val config = SynchroConfig(
    dbPath = "synchro.db",
    serverURL = "https://api.example.com",
    authProvider = { getToken() },
    clientID = deviceID,
    appVersion = "1.0.0",
    seedDatabasePath = seedFile.absolutePath
)
```

If `seedDatabasePath` is set and no database exists at `dbPath`, the seed is copied. If a database already exists, the seed is ignored. Generate seeds with the `synchroseed` CLI — see [Seed Database](/synchro/server/seed-database/).

## Schema Reconciliation

On connect, schema updates are reconciled **additively** — the client never drops tables or columns:

- **New columns/tables** from the server are added
- **Removed columns/tables** from the server are preserved locally
- **Local-only tables** (`createTable()`) and extra columns are never touched
- **Type changes** (e.g., `TEXT` → `INTEGER`) trigger a destructive rebuild (rare, intentional migrations only)
