---
title: "Client SDK Overview"
description: "Overview of native SDKs for iOS, Android, and React Native with shared interface contracts."
---

Native SDKs for iOS, Android, and React Native. These are not JavaScript wrappers -- they are native implementations using platform-standard database libraries (GRDB for Swift, Room/SQLiteOpenHelper for Kotlin). Each SDK embeds a full sync engine that handles registration, schema management, CDC-based change tracking, push/pull, conflict resolution, and snapshot recovery.

## SDK Architecture

| SDK | Platform | Database | Status |
|-----|----------|----------|--------|
| Swift | iOS / macOS | GRDB (SQLite) | Available |
| Kotlin | Android | SQLiteOpenHelper | Available |
| React Native | iOS + Android | Swift + Kotlin bridge | Available |

## Layer Model

The SDK stack has two layers:

- **Layer 1 (Native):** Swift wraps GRDB, Kotlin wraps Android SQLiteOpenHelper. Each contains a full sync engine -- schema manager, change tracker, push/pull processors, HTTP client, and retry logic.
- **Layer 2 (React Native):** A TurboModule bridge wraps both native SDKs. SQL strings go down, JSON rows come back up. React hooks provide reactive bindings on top.

<pre class="mermaid">
graph TB
    subgraph "React Native"
        JS[JavaScript / TypeScript]
        Bridge[TurboModule Bridge]
    end
    subgraph "Native"
        Swift[Swift SDK<br/>GRDB + SQLite]
        Kotlin[Kotlin SDK<br/>SQLiteOpenHelper + SQLite]
    end
    JS --> Bridge
    Bridge --> Swift
    Bridge --> Kotlin
</pre>

## Shared Interface Contract

All SDKs expose the same logical API. Method names and signatures are consistent across platforms, differing only in language idioms (async/await in Swift, suspend in Kotlin, Promise in TypeScript).

### Core SQL

| Method | Description |
|--------|-------------|
| `query(sql, params)` | Execute a SELECT, return all matching rows |
| `queryOne(sql, params)` | Execute a SELECT, return the first row or null |
| `execute(sql, params)` | Execute an INSERT/UPDATE/DELETE, return rows affected |
| `executeBatch(statements)` | Execute multiple statements in a single transaction |

### Transactions

| Method | Description |
|--------|-------------|
| `readTransaction(block)` | Execute a block inside a read transaction |
| `writeTransaction(block)` | Execute a block inside a write transaction |

### Schema (local-only tables)

| Method | Description |
|--------|-------------|
| `createTable(name, columns, options?)` | Create a local-only table |
| `alterTable(name, addColumns)` | Add columns to an existing table |
| `createIndex(table, columns, unique?)` | Create an index on a table |

:::note[Synced tables are managed automatically]
Schema methods are for **local-only** tables (caches, preferences, drafts). Synced tables are created and migrated automatically by the schema manager during `start()`.
:::

### Observation

| Method | Description |
|--------|-------------|
| `onChange(tables, callback)` | Notify when any of the listed tables change; returns a cancellable |
| `watch(sql, params, tables, callback)` | Re-execute the query whenever the listed tables change; returns a cancellable |

### Sync Control

| Method | Description |
|--------|-------------|
| `start()` | Register with the server, fetch schema, begin the sync loop |
| `stop()` | Stop the sync loop |
| `syncNow()` | Trigger an immediate sync cycle |

### Status and Events

| Method | Description |
|--------|-------------|
| `onStatusChange(callback)` | Observe sync status transitions (`idle`, `syncing`, `error`, `stopped`) |
| `onConflict(callback)` | Receive conflict events with client and server data |
| `onSnapshotRequired(handler)` | Prompt the user when a full resync is required |

## Client-Side CDC

SQLite triggers automatically track all writes to synced tables. There is no special write API -- standard SQL INSERT, UPDATE, and DELETE statements work. The CDC system intercepts changes transparently.

<pre class="mermaid">
graph LR
    A[client.execute] --> B[SQLite Write]
    B --> C[CDC Trigger]
    C --> D[_synchro_pending_changes]
    D --> E[Push to Server]
    E --> F[Server Accepts]
    F --> G[Drain from Queue]
</pre>

### Trigger Behavior

| Trigger | Fires On | Records As |
|---------|----------|------------|
| **AFTER INSERT** | Any INSERT into a synced table | `create` operation |
| **AFTER UPDATE** | Any UPDATE on a synced table | `update` operation (or `delete` if `deleted_at` becomes non-null) |
| **BEFORE DELETE** | Any DELETE on a synced table | Converts hard delete to soft delete (sets `deleted_at`, cancels the DELETE) |

:::caution[Sync lock prevents self-tracking]
During pull processing, the SDK sets a sync lock (`_synchro_meta.sync_lock = '1'`). CDC triggers check this flag and skip recording changes that originate from the server. This prevents pull-applied rows from bouncing back as pushes.
:::

## Logical Type Mapping

The server schema uses PostgreSQL types. Each column is assigned a logical type that determines how values are stored in SQLite and serialized over the wire.

| Logical Type | SQLite Type | Example |
|-------------|-------------|---------|
| `string` | `TEXT` | UUID, varchar, enum |
| `int` | `INTEGER` | integer, smallint |
| `int64` | `INTEGER` | bigint |
| `float` | `REAL` | real, double precision |
| `boolean` | `INTEGER` | 0 / 1 |
| `datetime` | `TEXT` | ISO 8601 |
| `date` | `TEXT` | YYYY-MM-DD |
| `time` | `TEXT` | HH:MM:SS |
| `json` | `TEXT` | JSON string |
| `bytes` | `BLOB` | bytea |

## Configuration Defaults

All SDKs share the same configuration parameters and defaults.

| Parameter | Type | Default | Constraint |
|-----------|------|---------|------------|
| `dbPath` | String | Required | -- |
| `serverURL` | String / URL | Required | -- |
| `authProvider` | Async function | Required | Returns JWT token |
| `clientID` | String | Required | Unique device identifier |
| `platform` | String | `"ios"` / `"android"` | -- |
| `appVersion` | String | Required | Semantic version |
| `syncInterval` | Seconds | `30` | -- |
| `pushDebounce` | Seconds | `0.5` | -- |
| `maxRetryAttempts` | Int | `5` | -- |
| `pullPageSize` | Int | `100` | 1--1000 |
| `pushBatchSize` | Int | `100` | 1--1000 |
| `snapshotPageSize` | Int | `100` | 1--1000 |

:::note[Page size capping]
`pullPageSize` and `snapshotPageSize` are capped at 1000 by the SDK. Values above 1000 are silently clamped. `pushBatchSize` is validated to be within 1--1000 on Kotlin; on Swift it is passed through (the server enforces the cap).
:::
