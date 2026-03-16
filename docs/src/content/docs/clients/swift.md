---
title: "Swift / iOS"
description: "Swift SDK for iOS and macOS with GRDB-backed SQLite sync."
---

## Installation

### Swift Package Manager

Add to `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/trainstar/synchro.git", from: "0.1.0")
]
```

Then add `"Synchro"` to your target's dependencies:

```swift
.target(
    name: "MyApp",
    dependencies: ["Synchro"]
)
```

### CocoaPods

```ruby
pod 'Synchro', '~> 0.1.0'
```

**Platforms:** iOS 16.0+, macOS 13.0+

**Dependencies:** [GRDB.swift](https://github.com/groue/GRDB.swift) 7.0+

## Configuration

```swift
let config = SynchroConfig(
    dbPath: dbURL.path,
    serverURL: URL(string: "https://api.example.com")!,
    authProvider: { await getToken() },
    clientID: UIDevice.current.identifierForVendor!.uuidString,
    appVersion: "1.0.0"
)
let client = try SynchroClient(config: config)
```

### SynchroConfig Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dbPath` | `String` | Required | Path to the SQLite database file |
| `serverURL` | `URL` | Required | Sync server base URL |
| `authProvider` | `@Sendable () async throws -> String` | Required | Returns a JWT token for authentication |
| `clientID` | `String` | Required | Unique device identifier |
| `platform` | `String` | `"ios"` | Platform name sent during registration |
| `appVersion` | `String` | Required | Semantic version of the app |
| `syncInterval` | `TimeInterval` | `30` | Seconds between sync cycles |
| `pushDebounce` | `TimeInterval` | `0.5` | Seconds after a write before triggering push |
| `maxRetryAttempts` | `Int` | `5` | Maximum retry count before entering error state |
| `pullPageSize` | `Int` | `100` | Rows per pull page (capped at 1000) |
| `pushBatchSize` | `Int` | `100` | Pending changes per push batch (max 1000) |
| `snapshotPageSize` | `Int` | `100` | Rows per snapshot page (capped at 1000) |

## Core Usage

### Queries

```swift
// Fetch multiple rows
let rows = try client.query(
    "SELECT * FROM tasks WHERE user_id = ?",
    params: [userId]
)

// Fetch a single row
let task = try client.queryOne(
    "SELECT * FROM tasks WHERE id = ?",
    params: [id]
)
```

### Writes

```swift
let result = try client.execute(
    "INSERT INTO tasks (id, title, user_id) VALUES (?, ?, ?)",
    params: [UUID().uuidString, "Review proposal", userId]
)
// result.rowsAffected == 1
```

:::note[CDC triggers track writes automatically]
Any INSERT, UPDATE, or DELETE on a synced table is captured by SQLite triggers and queued for push. No special write API is needed.
:::

### Batch Execution

```swift
let affected = try client.executeBatch([
    SQLStatement(sql: "INSERT INTO tasks (id, title, user_id) VALUES (?, ?, ?)",
                 params: [UUID().uuidString, "Write report", userId]),
    SQLStatement(sql: "INSERT INTO tasks (id, title, user_id) VALUES (?, ?, ?)",
                 params: [UUID().uuidString, "Update docs", userId]),
])
// affected == 2
```

## Transactions

```swift
// Write transaction: multiple operations atomically
try client.writeTransaction { db in
    try db.execute(
        sql: "INSERT INTO tasks (id, title, user_id) VALUES (?, ?, ?)",
        arguments: [UUID().uuidString, "Review proposal", userId]
    )
    try db.execute(
        sql: "INSERT INTO comments (id, task_id, body, status) VALUES (?, ?, ?, ?)",
        arguments: [UUID().uuidString, taskId, "Looks good", "open"]
    )
}

// Read transaction: consistent snapshot
let count = try client.readTransaction { db in
    try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM tasks") ?? 0
}
```

## Observation

### Change Notification

```swift
let cancel = client.onChange(tables: ["tasks"]) {
    print("tasks table changed")
}

// Later: stop observing
cancel.cancel()
```

### Reactive Query

```swift
let cancel = client.watch(
    "SELECT * FROM tasks ORDER BY created_at DESC",
    tables: ["tasks"]
) { rows in
    self.tasks = rows
}
```

The callback fires immediately with the current result, then again whenever the observed tables change. Observation is backed by GRDB's `ValueObservation`, which uses SQLite's `sqlite3_update_hook` for efficient change detection.

## Sync Control

```swift
// Start sync: register, fetch schema, begin sync loop
try await client.start()

// Trigger an immediate sync cycle
try await client.syncNow()

// Stop the sync loop
client.stop()

// Close the database (also stops sync)
try client.close()
```

## Status and Events

### Sync Status

```swift
let cancel = client.onStatusChange { status in
    switch status {
    case .idle:
        print("Idle")
    case .syncing:
        print("Syncing...")
    case .error(let retryAt):
        if let retryAt {
            print("Error, retrying at \(retryAt)")
        } else {
            print("Error, no retry scheduled")
        }
    case .stopped:
        print("Stopped")
    }
}
```

### Conflict Events

```swift
let cancel = client.onConflict { event in
    print("Conflict on \(event.table) record \(event.recordID)")
    print("Client data: \(String(describing: event.clientData))")
    print("Server data: \(String(describing: event.serverData))")
}
```

### Snapshot Required

```swift
let cancel = client.onSnapshotRequired { () async -> Bool in
    // Prompt the user or decide programmatically
    return await promptUser("Full resync needed. Continue?")
}
```

:::caution[Snapshot callback is required for recovery]
If no `onSnapshotRequired` handler is registered and the server requests a snapshot (due to bucket reassignment, compaction, or data loss), the sync engine will enter an error state. Always register a handler.
:::

## Error Handling

All errors are represented by the `SynchroError` enum:

```swift
public enum SynchroError: Error {
    case notConnected
    case schemaNotLoaded
    case tableNotSynced(String)
    case upgradeRequired(currentVersion: String, minimumVersion: String)
    case schemaMismatch(serverVersion: Int64, serverHash: String)
    case snapshotRequired
    case pushRejected(results: [PushResult])
    case networkError(underlying: Error)
    case serverError(status: Int, message: String)
    case databaseError(underlying: Error)
    case invalidResponse(message: String)
    case alreadyStarted
    case notStarted
}
```

| Case | When It Occurs |
|------|----------------|
| `notConnected` | Operation attempted before `start()` completes registration |
| `schemaNotLoaded` | Schema has not been fetched from server yet |
| `tableNotSynced` | Write attempted on a table not in the server schema |
| `upgradeRequired` | Server rejected the client's app version (HTTP 426) |
| `schemaMismatch` | Client schema hash does not match server (HTTP 409) |
| `snapshotRequired` | Server indicates a full snapshot is needed |
| `pushRejected` | One or more push records were rejected by the server |
| `networkError` | Network connectivity failure |
| `serverError` | Server returned a non-200 HTTP status |
| `databaseError` | SQLite operation failed |
| `invalidResponse` | Server response could not be decoded |
| `alreadyStarted` | `start()` called when sync is already running |
| `notStarted` | `syncNow()` called before `start()` |
