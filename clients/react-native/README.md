# `@trainstar/synchro-react-native`

React Native TurboModule bridge for Synchro. The package wraps the native Swift and Kotlin SDKs; it does not implement a separate sync engine in JavaScript.

## Requirements

- React Native `0.83.x`. Use `0.83.5` or later with Xcode `26.4` or later.
- iOS `16.0+`
- Android `minSdk 24`
- Node `20.19.4+`
- Android development and CI should use JDK `17`

## Installation

Install the published package only after `0.3.0` is available. Before that,
use the local artifact flow in
[Client Consumption](https://github.com/trainstar/synchro/tree/dev/docs/src/content/docs/clients/consumption.mdx).

```sh
npm install @trainstar/synchro-react-native@0.3.0
```

Before you run `pod install`, add these published `0.3.0` dependencies to the
application `ios/Podfile`:

```ruby
pod 'Synchro', :git => 'https://github.com/trainstar/synchro.git', :tag => 'v0.3.0'
pod 'GRDB.swift', :git => 'https://github.com/groue/GRDB.swift.git', :tag => 'v7.0.0'
```

Then install the pods:

```sh
cd ios
pod install
```

Android autolinks through the React Native Gradle plugin. No manual `MainApplication` edits are required.

The Android library resolves the native SDK from Maven Central.
Local Maven resolution is explicit in `example/android/build.gradle` for development.

## Usage

```ts
import { Platform } from 'react-native';
import { SynchroClient } from '@trainstar/synchro-react-native';

export async function syncQuickstart(
  accessToken: string,
  clientID: string,
  databaseFileName: string
): Promise<void> {
  const serverURL = Platform.OS === 'android'
    ? 'http://10.0.2.2:8091'
    : 'http://127.0.0.1:8091';
  const noteID = '00000000-0000-4000-8000-000000000001';
  const client = new SynchroClient({
    dbPath: databaseFileName,
    serverURL,
    authProvider: async () => accessToken,
    clientID,
    appVersion: '0.3.0',
  });

  try {
    await client.initialize();
    await client.start();
    await client.syncNow();

    const note = await client.queryOne(
      'SELECT id, body FROM notes WHERE id = ?',
      [noteID]
    );
    if (note === null || typeof note.id !== 'string' || note.id !== noteID) {
      throw new Error('The first pull did not contain the quickstart note');
    }

    await client.stop();
    await client.execute(
      'UPDATE notes SET body = ? WHERE id = ?',
      ['Edited offline', noteID]
    );
    await client.start();
    await client.syncNow();

    const updated = await client.queryOne(
      'SELECT body FROM notes WHERE id = ?',
      [noteID]
    );
    if (
      updated === null ||
      typeof updated.body !== 'string' ||
      updated.body !== 'Edited offline'
    ) {
      throw new Error('The synced note did not contain the offline edit');
    }
  } finally {
    await client.close();
  }
}
```

The example uses the local tutorial adapter only. Android emulators reach the
host at `10.0.2.2`; iOS simulators use `127.0.0.1`. The example app permits
that cleartext development traffic. Production applications must use HTTPS and
must not copy the cleartext network configuration.

`start()` arms the lifecycle. Do not use it as a schema-readiness signal.
`syncNow()` completes the requested cycle before this example reads the local
database.

SQL bind params are passed as native typed arrays, not as JSON strings. Supported React Native bind values are `null`, `string`, `number`, `boolean`, `{ type: 'int64', value: '...' }`, and `{ type: 'bytes', base64: '...' }`. Positional `null` values are preserved across iOS and Android for direct queries, batch execution, transactions, and query observers.

`queryOne()` returns `Row | null`, and each row value is `unknown`. Check for
both a non-null row and a string `id` before using the value as a note ID.

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
    'UPDATE notes SET body = ? WHERE id = ?',
    ['Edited offline', '00000000-0000-4000-8000-000000000001']
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

The published package accepts Node `20.19.4` or later. Repository development
uses the Node `22.20.0` pin in `.nvmrc`.

```sh
make lint-rn
make test-rn-unit
```

The example app in [`example/`](./example) is the end-to-end harness used for RN bridge verification on iOS and Android.
