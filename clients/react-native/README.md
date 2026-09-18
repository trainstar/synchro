# `@trainstar/synchro-react-native`

React Native TurboModule bridge for Synchro. The package wraps the native Swift and Kotlin SDKs; it does not implement a separate sync engine in JavaScript.

## Requirements

- React Native `0.83.x`. Use `0.83.5` or later with Xcode `26.4` or later.
- iOS `16.0+`
- Android `minSdk 24`
- Node `20+`
- Android development and CI should use JDK `17`

## Installation

```sh
npm install @trainstar/synchro-react-native
```

iOS:

```sh
cd ios
pod install
```

Android autolinks through the React Native Gradle plugin. No manual `MainApplication` edits are required.

The Android library resolves the native SDK from Maven Central.
Local Maven resolution is explicit in `example/android/build.gradle` for development.

## Usage

```ts
import { SynchroClient } from '@trainstar/synchro-react-native';

const client = new SynchroClient({
  dbPath: 'synchro.db',
  serverURL: 'https://api.example.com',
  authProvider: async () => '<jwt>',
  clientID: 'device-1',
  appVersion: '1.0.0',
});

await client.initialize();
await client.start();

const rows = await client.query('SELECT * FROM tasks WHERE done = ?', [0]);
await client.execute('UPDATE tasks SET done = ? WHERE id = ?', [1, rows[0].id]);
```

SQL bind params are passed as native typed arrays, not as JSON strings. Supported React Native bind values are `null`, `string`, `number`, and `boolean`. Positional `null` values are preserved across iOS and Android for direct queries, batch execution, transactions, and query observers.

## Public API

Core methods:

- `initialize()`
- `query(sql, params?)`
- `queryOne(sql, params?)`
- `execute(sql, params?)`
- `executeBatch(statements)`
- `createTable(...)`
- `alterTable(...)`
- `createIndex(...)`
- `pendingChangeCount()`
- `start()`
- `stop()`
- `syncNow()`
- `close()`

Events and hooks:

- `onStatusChange(callback)`
- `onConflict(callback)`
- `useQuery(client, sql, params?, tables?)`
- `useSyncStatus(client)`
- `usePendingChanges(client, pollInterval?)`

`useQuery` compares `int64` and `bytes` parameters by tag type and payload.
Equivalent tag objects do not restart the query or subscription.

`useSyncStatus` subscribes before it reads current native status.
New events take priority over the initial snapshot.
Changing clients resets the displayed status until the new client supplies state.
Snapshot failures reach the React error boundary unless a newer status event has arrived.

## Transactions

`writeTransaction()` and `readTransaction()` mirror the native SDKs through a bridge-held transaction session.

Contract:

- The callback must only perform database work.
- Do not perform network I/O or long-running waits inside the callback.
- Transactions time out after 5 seconds of inactivity and reject with `TransactionTimeoutError`.

```ts
await client.writeTransaction(async (tx) => {
  await tx.execute(
    'INSERT INTO tasks (id, title, done) VALUES (?, ?, ?)',
    ['task-1', 'Ship RN SDK', 0]
  );
});
```

## Errors

Native errors are normalized to typed JS errors, including:

- `NotConnectedError`
- `SchemaNotLoadedError`
- `TableNotSyncedError`
- `UpgradeRequiredError`
- `SchemaMismatchError`
- `PushRejectedError`
- `NetworkError`
- `ServerError`
- `DatabaseError`
- `InvalidResponseError`
- `AlreadyStartedError`
- `NotStartedError`
- `TransactionTimeoutError`

The iOS schema bridge rejects malformed JSON with the existing `UNKNOWN` error code.

## Development

```sh
npm run typecheck
npm run test:unit
npm run prepare
npm run pack:dry-run
```

The example app in [`example/`](./example) is the end-to-end harness used for RN bridge verification on iOS and Android.
