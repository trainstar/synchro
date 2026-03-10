# `@trainstar/synchro-react-native`

React Native TurboModule bridge for Synchro. The package wraps the native Swift and Kotlin SDKs; it does not implement a separate sync engine in JavaScript.

## Requirements

- React Native `0.83.x`
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
- `onSnapshotRequired(handler)`
- `useQuery(client, sql, params?, tables?)`
- `useSyncStatus(client)`
- `usePendingChanges(client, pollInterval?)`

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
- `SnapshotRequiredError`
- `PushRejectedError`
- `NetworkError`
- `ServerError`
- `DatabaseError`
- `InvalidResponseError`
- `AlreadyStartedError`
- `NotStartedError`
- `TransactionTimeoutError`

## Development

```sh
npm run typecheck
npm run test:unit
npm run prepare
npm run pack:dry-run
```

The example app in [`example/`](./example) is the end-to-end harness used for RN bridge verification on iOS and Android.
