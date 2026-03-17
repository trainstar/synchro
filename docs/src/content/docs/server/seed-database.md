---
title: "Seed Database"
description: "Generate and bundle a seed SQLite database for offline-first client bootstrap."
---

A seed database is a pre-built SQLite file that ships with your app. It contains synced table schemas, CDC triggers, and optionally pre-loaded data. The app can query and write locally on first launch, before connecting to the server.

## Two Modes

**Schema-only** (default): Empty tables with correct columns, types, triggers, and infrastructure metadata. The app can write locally immediately. Data arrives on first sync.

**Schema + data** (`-with-data`): Same as above, plus pre-populated rows from the server's current snapshot. Useful for reference tables (products, categories, config) so users see real content on first launch.

## How It Works on First Connect

When the client calls `start()` for the first time:

1. Schema reconciliation runs additively (adds missing columns/tables, never drops)
2. The client performs a normal initial sync (register → snapshot → incremental)
3. The snapshot **replaces** seed data with the server's authoritative dataset

The seed is a **warm cache**, not a source of truth. It gives the app something to show before connectivity. Once the client syncs, the server is authoritative. Any local writes made before connecting are pushed to the server before the snapshot runs. They are not lost.

## Generating a Seed Database

### Build the CLI

```bash
make seed-build
```

### Schema-Only Seed

```bash
bin/synchroseed -server=http://localhost:8080 -token=<jwt> -output=seed.db
```

### Schema + Data Seed

```bash
bin/synchroseed -server=http://localhost:8080 -token=<jwt> -output=seed.db -with-data
```

The `-with-data` flag registers a temporary client, pages through the snapshot endpoint, and inserts all records with the sync lock engaged (CDC triggers don't fire). The auth token determines which data is included: the same bucket-scoped data the token's user would receive on first sync.

For reference data visible to all users, use a token with access to global buckets.

| Flag | Default | Description |
|------|---------|-------------|
| `-server` | Required | URL of the synchrod server |
| `-token` | `""` | JWT authentication token |
| `-jwt-secret` | `""` | JWT secret to auto-generate a token (alternative to `-token`) |
| `-output` | `seed.db` | Output file path |
| `-with-data` | `false` | Include snapshot data from the server |

### What the Seed Contains

- **Infrastructure tables**: `_synchro_pending_changes`, `_synchro_meta` (initial sync state)
- **GRDB migrations table**: `grdb_migrations` with `synchro_v1` applied (Swift compatibility)
- **Synced tables**: All server-schema tables with correct columns, types, constraints
- **CDC triggers**: 3 per table (insert, update, before-delete) for change tracking
- **Data** (with `-with-data`): Snapshot records inserted with sync lock to avoid CDC
- **WAL journal mode**: Enabled for concurrent access

### CI/CD Integration

```bash
bin/synchroseed \
  -server=$SYNCHRO_SERVER_URL \
  -token=$SYNCHRO_CI_TOKEN \
  -output=assets/seed.db \
  -with-data
```

Regenerate whenever the server schema changes or when you want fresher reference data.

## Bundling Per Platform

### iOS (Swift)

Add `seed.db` to your Xcode project's "Copy Bundle Resources" build phase.

```swift
let config = SynchroConfig(
    dbPath: dbURL.path,
    serverURL: URL(string: "https://api.example.com")!,
    authProvider: { await getToken() },
    clientID: deviceID,
    appVersion: "1.0.0",
    seedDatabasePath: Bundle.main.path(forResource: "seed", ofType: "db")
)
```

### Android (Kotlin)

Place `seed.db` in `src/main/assets/`. Copy to an accessible path at startup:

```kotlin
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

### React Native

```typescript
import RNFS from 'react-native-fs';
import { Platform } from 'react-native';

const seedPath = Platform.select({
  ios: `${RNFS.MainBundlePath}/seed.db`,
  android: `${RNFS.DocumentDirectoryPath}/seed.db`,
});

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

## Schema Reconciliation

On first connect, the schema manager compares the seed's tables against the server schema:

- **Missing columns/tables** from the server are added
- **Extra local columns/tables** are preserved
- **Local-only tables** are never touched
- **Type changes** (e.g., `TEXT` → `INTEGER`) trigger a destructive rebuild

## Edge Cases

- **Stale schema**: If the server schema evolved since the seed was generated, reconciliation adds missing columns/tables. No data loss.
- **Stale data** (`-with-data`): Seed data is replaced by the authoritative snapshot on first sync. The seed shows content during the offline window; the server is the source of truth after connect.
- **WAL files**: The seed generator sets WAL mode. The client copies WAL/SHM sidecar files alongside the seed if they exist.
- **GRDB migrations**: The seed includes `grdb_migrations` with `synchro_v1` applied. GRDB skips re-running the migration.
- **Android SQLiteOpenHelper**: The seed is opened with version 1, so `onCreate` is not called (DB exists) and `onUpgrade` is not called (version matches).
