---
title: "React Native"
description: "React Native SDK with TurboModule bridge to native Swift and Kotlin sync engines."
---

## Installation

```bash
npm install @trainstar/synchro-react-native
cd ios && pod install
```

**Requirements:** React Native 0.83+, iOS 16.0+, Android minSdk 24

The package uses the TurboModule (Codegen) architecture. It bridges to the native Swift SDK on iOS and the native Kotlin SDK on Android -- there is no JavaScript SQLite driver involved.

## Configuration

```typescript
import { SynchroClient } from '@trainstar/synchro-react-native';

const client = new SynchroClient({
  dbPath: 'synchro.db',
  serverURL: 'https://api.example.com',
  authProvider: async () => await getToken(),
  clientID: deviceId,
  appVersion: '1.0.0',
});

// Initialize the native module (must be called before any other method)
await client.initialize();
```

### SynchroConfig Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dbPath` | `string` | Required | SQLite database file name or path |
| `serverURL` | `string` | Required | Sync server base URL |
| `authProvider` | `() => Promise<string>` | Required | Returns a JWT token for authentication |
| `clientID` | `string` | Required | Unique device identifier |
| `platform` | `string` | `Platform.OS` | Platform name sent during registration |
| `appVersion` | `string` | Required | Semantic version of the app |
| `syncInterval` | `number` | `30` | Seconds between sync cycles |
| `pushDebounce` | `number` | `0.5` | Seconds after a write before triggering push |
| `maxRetryAttempts` | `number` | `5` | Maximum retry count before entering error state |
| `pullPageSize` | `number` | `100` | Rows per pull page (max 1000) |
| `pushBatchSize` | `number` | `100` | Pending changes per push batch (max 1000) |
| `snapshotPageSize` | `number` | `100` | Rows per snapshot page (max 1000) |
| `seedDatabasePath` | `string` | `undefined` | Path to a pre-built seed database for offline-first bootstrap |

:::note[Two-step initialization]
The constructor wires up the auth callback bridge. `initialize()` creates the native database and configures the sync engine. All SQL and sync methods require `initialize()` to have completed.
:::

## Core Usage

### Queries

```typescript
// Fetch multiple rows
const rows = await client.query(
  'SELECT * FROM tasks WHERE user_id = ?',
  [userId]
);

// Fetch a single row
const task = await client.queryOne(
  'SELECT * FROM tasks WHERE id = ?',
  [id]
);
```

`Row` is typed as `Record<string, unknown>`. Values cross the bridge as JSON, so numbers arrive as `number`, strings as `string`, booleans as `boolean`, and nulls as `null`.

### Writes

```typescript
const result = await client.execute(
  'INSERT INTO tasks (id, title, user_id) VALUES (?, ?, ?)',
  [crypto.randomUUID(), 'Review proposal', userId]
);
// result.rowsAffected === 1
```

:::note[CDC triggers track writes automatically]
Any INSERT, UPDATE, or DELETE on a synced table is captured by SQLite triggers on the native side and queued for push. No special write API is needed.
:::

### Batch Execution

```typescript
const result = await client.executeBatch([
  {
    sql: 'INSERT INTO tasks (id, title, user_id) VALUES (?, ?, ?)',
    params: [crypto.randomUUID(), 'Write report', userId],
  },
  {
    sql: 'INSERT INTO tasks (id, title, user_id) VALUES (?, ?, ?)',
    params: [crypto.randomUUID(), 'Update docs', userId],
  },
]);
// result.totalRowsAffected === 2
```

## Transactions

```typescript
// Write transaction
await client.writeTransaction(async (tx) => {
  await tx.execute(
    'INSERT INTO tasks (id, title, user_id) VALUES (?, ?, ?)',
    [taskId, 'Review proposal', userId]
  );
  await tx.execute(
    'INSERT INTO comments (id, task_id, body, status) VALUES (?, ?, ?, ?)',
    [crypto.randomUUID(), taskId, 'Looks good', 'open']
  );
});

// Read transaction
const count = await client.readTransaction(async (tx) => {
  const row = await tx.queryOne('SELECT COUNT(*) as c FROM tasks');
  return (row?.c as number) ?? 0;
});
```

The `Transaction` interface exposes `query`, `queryOne`, and `execute` -- the same SQL methods as the top-level client.

:::caution[Transaction timeout]
Transactions are held open on the native side across async bridge calls. If no operation is performed within **5 seconds**, the transaction is automatically rolled back and a `TransactionTimeoutError` is thrown. Keep transactions short and avoid awaiting user interaction inside them.
:::

## Schema (Local-Only Tables)

```typescript
await client.createTable('drafts', [
  { name: 'id', type: 'TEXT', primaryKey: true },
  { name: 'content', type: 'TEXT', nullable: false },
  { name: 'created_at', type: 'TEXT', nullable: false },
]);

await client.alterTable('drafts', [
  { name: 'title', type: 'TEXT', nullable: true },
]);

await client.createIndex('drafts', ['created_at']);
```

## Observation

### Change Notification

```typescript
const unsubscribe = client.onChange(['tasks'], () => {
  console.log('tasks table changed');
});

// Later: stop observing
unsubscribe();
```

### Reactive Query

```typescript
const unsubscribe = client.watch(
  'SELECT * FROM tasks ORDER BY created_at DESC',
  undefined,
  ['tasks'],
  (rows) => {
    setTasks(rows);
  }
);
```

The callback fires immediately with the current result set, then again whenever the observed tables change.

## React Hooks

The React Native SDK provides three hooks that wrap the observation and status APIs for use in functional components.

### useQuery

Reactive queries that automatically re-execute when observed tables change.

```tsx
import { useQuery } from '@trainstar/synchro-react-native';

function TaskList() {
  const { data, loading, error, refresh } = useQuery(
    client,
    'SELECT * FROM tasks ORDER BY created_at DESC',
    [],
    ['tasks']
  );

  if (loading) return <ActivityIndicator />;
  if (error) return <Text>Error: {error.message}</Text>;

  return <FlatList data={data} renderItem={({ item }) => (
    <Text>{item.name as string}</Text>
  )} />;
}
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `client` | `SynchroClient` | The initialized client instance |
| `sql` | `string` | SQL query to execute |
| `params` | `unknown[]` (optional) | Bind parameters |
| `tables` | `string[]` (optional) | Tables to observe for changes |

**Returns:** `{ data: Row[], loading: boolean, error: SynchroError | null, refresh: () => void }`

:::note[Reactive vs one-shot mode]
When `tables` is provided and non-empty, `useQuery` uses `client.watch()` for reactive updates. When `tables` is omitted or empty, it performs a single `client.query()` call. Use `refresh()` to manually re-execute a one-shot query.
:::

### useSyncStatus

Observe the sync engine status in a component.

```tsx
import { useSyncStatus } from '@trainstar/synchro-react-native';

function SyncIndicator() {
  const { status, retryAt } = useSyncStatus(client);

  switch (status) {
    case 'idle':
      return <Icon name="check" color="green" />;
    case 'syncing':
      return <ActivityIndicator />;
    case 'error':
      return <Icon name="warning" color="red" />;
    case 'stopped':
      return <Icon name="pause" color="gray" />;
  }
}
```

**Returns:** `SyncStatus` with `{ status: SyncStatusType, retryAt: Date | null }`

`SyncStatusType` is one of: `'idle'` | `'connecting'` | `'syncing'` | `'error'` | `'stopped'`

### usePendingChanges

Observe the count of local changes waiting to be pushed.

```tsx
import { usePendingChanges } from '@trainstar/synchro-react-native';

function PendingBadge() {
  const count = usePendingChanges(client, 2000);

  if (count === 0) return null;
  return <Badge count={count} />;
}
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `client` | `SynchroClient` | Required | The initialized client instance |
| `pollInterval` | `number` | `2000` | Milliseconds between polls |

**Returns:** `number` -- the count of pending changes

## Sync Control

```typescript
// Start sync: register, fetch schema, begin sync loop
await client.start();

// Trigger an immediate sync cycle
await client.syncNow();

// Stop the sync loop
await client.stop();

// Close the database and clean up
await client.close();
```

## Status and Events

### Sync Status

```typescript
const unsubscribe = client.onStatusChange((status) => {
  console.log(`Status: ${status.status}`);
  if (status.retryAt) {
    console.log(`Retrying at: ${status.retryAt.toISOString()}`);
  }
});
```

### Conflict Events

```typescript
const unsubscribe = client.onConflict((event) => {
  console.log(`Conflict on ${event.table} record ${event.recordID}`);
  console.log('Client data:', event.clientData);
  console.log('Server data:', event.serverData);
});
```

### Snapshot Required

```typescript
const unsubscribe = client.onSnapshotRequired(async () => {
  // Prompt the user or decide programmatically
  // Return true to proceed with snapshot, false to abort
  const approved = await showConfirmDialog('Full resync needed. Continue?');
  return approved;
});
```

:::caution[Snapshot callback is required for recovery]
If no `onSnapshotRequired` handler is registered and the server requests a snapshot (due to bucket reassignment, compaction, or data loss), the sync engine will enter an error state. Always register a handler.
:::

## Error Handling

All errors extend the base `SynchroError` class:

```typescript
class SynchroError extends Error {
  readonly code: string;
}
```

### Error Types

| Class | Code | When It Occurs |
|-------|------|----------------|
| `NotConnectedError` | `NOT_CONNECTED` | Operation attempted before `start()` completes |
| `SchemaNotLoadedError` | `SCHEMA_NOT_LOADED` | Schema has not been fetched from server |
| `TableNotSyncedError` | `TABLE_NOT_SYNCED` | Write on a table not in the server schema |
| `UpgradeRequiredError` | `UPGRADE_REQUIRED` | Server rejected the client's app version (HTTP 426) |
| `SchemaMismatchError` | `SCHEMA_MISMATCH` | Client schema hash does not match server (HTTP 409) |
| `SnapshotRequiredError` | `SNAPSHOT_REQUIRED` | Server indicates a full snapshot is needed |
| `PushRejectedError` | `PUSH_REJECTED` | One or more push records were rejected |
| `NetworkError` | `NETWORK_ERROR` | Network connectivity failure |
| `ServerError` | `SERVER_ERROR` | Server returned a non-200 HTTP status |
| `DatabaseError` | `DATABASE_ERROR` | SQLite operation failed |
| `InvalidResponseError` | `INVALID_RESPONSE` | Server response could not be decoded |
| `AlreadyStartedError` | `ALREADY_STARTED` | `start()` called when sync is already running |
| `NotStartedError` | `NOT_STARTED` | `syncNow()` called before `start()` |
| `TransactionTimeoutError` | `TRANSACTION_TIMEOUT` | Transaction rolled back due to 5s inactivity |

Native errors are automatically mapped to TypeScript error classes via `mapNativeError()`. You can catch specific error types:

```typescript
try {
  await client.start();
} catch (error) {
  if (error instanceof UpgradeRequiredError) {
    showUpgradeDialog(error.minimumVersion);
  } else if (error instanceof NetworkError) {
    showOfflineBanner();
  } else if (error instanceof SynchroError) {
    console.error(`Sync error [${error.code}]: ${error.message}`);
  }
}
```

## Seed Database (Optional)

Ship a pre-built SQLite file so the app works offline on first launch — no server required. Useful when users may not have connectivity on first open, or when your onboarding flow writes data before sign-in.

Without a seed, tables are created on first `start()` from the server schema. The seed removes that dependency.

```typescript
import RNFS from 'react-native-fs';
import { Platform } from 'react-native';

// Resolve platform-specific seed path
const seedPath = Platform.select({
  ios: `${RNFS.MainBundlePath}/seed.db`,
  android: `${RNFS.DocumentDirectoryPath}/seed.db`,
});

// On Android, copy from assets on first install
if (Platform.OS === 'android' && !(await RNFS.exists(seedPath))) {
  await RNFS.copyFileAssets('seed.db', seedPath);
}

const client = new SynchroClient({
  dbPath: 'synchro.db',
  serverURL: 'https://api.example.com',
  authProvider: async () => await getToken(),
  clientID: deviceId,
  appVersion: '1.0.0',
  seedDatabasePath: seedPath,
});
```

If `seedDatabasePath` is set and no database exists at `dbPath`, the seed is copied. If a database already exists, the seed is ignored. Generate seeds with the `synchroseed` CLI — see [Seed Database](/synchro/server/seed-database/).

## Schema Reconciliation

On connect, schema updates are reconciled **additively** — the client never drops tables or columns:

- **New columns/tables** from the server are added
- **Removed columns/tables** from the server are preserved locally
- **Local-only tables** (`createTable()`) and extra columns are never touched
- **Type changes** (e.g., `TEXT` → `INTEGER`) trigger a destructive rebuild (rare, intentional migrations only)
