# Synchro SDK Architecture

Shared architecture for all Synchro client SDKs (Swift, Kotlin, React Native).

## React Native Bridge

The React Native SDK is a thin TurboModule bridge wrapping both native SDKs. No sync logic is reimplemented in JS.

- **Bridge format**: SQL strings down, JSON rows up, typed TurboModule events up
- **Transactions**: Handle/session pattern — native holds the transaction open on a background thread, JS sends operations via semaphore (iOS) / channel (Kotlin), 5s inactivity timeout
- **Auth**: Bidirectional — native emits `onAuthRequest`, JS calls `authProvider()`, resolves back via `resolveAuthRequest()`
- **Error mapping**: Native `SynchroError` → bridge wire format (`code` + `userInfo`) → typed JS error classes

## Interface Contract

Every native SDK (Swift/Kotlin) exposes the same logical interface:

### Core SQL
- `query(sql, params) -> [Row]` -- Read query returning rows
- `queryOne(sql, params) -> Row?` -- Read query returning at most one row
- `execute(sql, params) -> ExecResult` -- Write query returning changes count
- `readTransaction(block) -> T` -- Read transaction
- `writeTransaction(block) -> T` -- Write transaction
- `executeBatch(statements) -> Int` -- Batch execution
- `prepareStatement(sql) -> PreparedStatement` -- Prepared statement

### Schema (local-only tables)
- `createTable(name, columns, options)` -- Create a local-only table
- `alterTable(name, addColumns)` -- Add columns to a local-only table
- `createIndex(table, columns, unique)` -- Create an index

### Observation
- `onChange(tables, callback) -> Cancellable` -- Fire callback when tables change
- `watch(sql, params, tables, callback) -> Cancellable` -- Re-run query when tables change

### WAL
- `checkpoint(mode)` -- SQLite WAL checkpoint

### Lifecycle
- `close()` -- Close the database
- `path` -- Database file path

### Sync Control
- `start(options)` -- Start sync (register, fetch schema, begin sync loop)
- `stop()` -- Stop sync loop
- `syncNow()` -- Trigger immediate sync cycle

### Status
- `onStatusChange(callback) -> Cancellable` -- Observe sync status changes
- `onConflict(callback) -> Cancellable` -- Observe conflict events
- `onSnapshotRequired(handler) -> Cancellable` -- Handle full snapshot approval prompts

## Internal SQLite Tables

### `_synchro_pending_changes`

Local change queue. Lightweight -- no data blob, hydrate on push.

```sql
CREATE TABLE _synchro_pending_changes (
    record_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    base_updated_at TEXT,
    client_updated_at TEXT NOT NULL,
    PRIMARY KEY (table_name, record_id)
);
```

### `_synchro_meta`

SDK state key-value store.

```sql
CREATE TABLE _synchro_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
```

Keys: `checkpoint`, `schema_version`, `schema_hash`, `client_server_id`, `known_buckets`, `sync_lock`

## CDC Triggers

Auto-generated per synced table when built from server schema.

### INSERT trigger
Logs `create` operation. On conflict (same record already pending), preserves original operation.

### UPDATE trigger
Logs `update` or `delete` (when `deleted_at` transitions from NULL to non-NULL). Dedup rules:
- create + update = still create
- create + delete = remove from queue (never reached server)
- update + update = update with latest timestamps
- update + delete = delete

### BEFORE DELETE trigger
Converts hard deletes to soft deletes (`SET deleted_at`, `RAISE(IGNORE)`). The UPDATE trigger then handles CDC.

### Sync Lock
Before applying pull/resync results, set `sync_lock = '1'`. After, set back to `'0'`. Triggers skip when locked. All within a single write transaction.

## Sync Orchestration

### Sync Cycle (push-before-pull)
1. **Push**: Read pending queue, hydrate records, send to `POST /sync/push`, process accepted/rejected
2. **Pull loop**: `POST /sync/pull` with checkpoint, apply changes/deletes, advance checkpoint, repeat while `has_more`
3. **Resync**: If pull returns `resync_required`, push first, prompt user, drop synced data, page through `POST /sync/resync`, rebuild

### Retry
Exponential backoff (1s, 2s, 4s, 8s, 16s cap) with jitter. Respect `Retry-After` headers.

### Timer
Periodic sync at configurable interval (default 30s).

### Debounce
After writes to synced tables, schedule push after configurable debounce (default 0.5s).

## Protocol Mapping

| SDK Method | Server Endpoint | HTTP Method |
|-----------|-----------------|-------------|
| register | `/sync/register` | POST |
| pull | `/sync/pull` | POST |
| push | `/sync/push` | POST |
| resync | `/sync/resync` | POST |
| fetchSchema | `/sync/schema` | GET |
| fetchTables | `/sync/tables` | GET |

## Logical Type Mapping (Server -> SQLite)

| Logical Type | SQLite Type |
|-------------|-------------|
| `string` | `TEXT` |
| `int` | `INTEGER` |
| `int64` | `INTEGER` |
| `float` | `REAL` |
| `boolean` | `INTEGER` |
| `datetime` | `TEXT` |
| `date` | `TEXT` |
| `time` | `TEXT` |
| `json` | `TEXT` |
| `bytes` | `BLOB` |
