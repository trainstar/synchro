# Synchro

[![Go](https://img.shields.io/badge/Go-1.22+-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-trainstar.github.io%2Fsynchro-blue)](https://trainstar.github.io/synchro)

Offline-first sync for Go + PostgreSQL. Your tables. Your server. Minimal changes.

## What You Need

| Component | What Changes |
|-----------|-------------|
| Your tables | Add `deleted_at TIMESTAMPTZ NULL` column |
| PostgreSQL | `wal_level = logical` |
| Your server | Register tables + wire 6 HTTP endpoints |
| Client app | `client.query()` / `client.execute()` -- standard SQL |

## Why Synchro

- **Full bidirectional sync with built-in conflict resolution** -- not read-only, not BYOB writes
- **Embed in your app or deploy standalone** -- scale without rewriting
- **RLS-enforced authorization at the database layer** -- Postgres guards your data, not application code
- **Native SDKs: Swift, Kotlin, React Native** -- local SQLite, automatic change tracking, background sync

## Install

```bash
go get github.com/trainstar/synchro
```

## Server

```go
registry := synchro.NewRegistry()
registry.Register(&synchro.TableConfig{
    TableName:   "workouts",
    OwnerColumn: "user_id",
})

engine, _ := synchro.NewEngine(synchro.Config{
    DB:       db,
    Registry: registry,
})

h := handler.New(engine)
http.HandleFunc("POST /sync/register",  h.ServeRegister)
http.HandleFunc("POST /sync/pull",      h.ServePull)
http.HandleFunc("POST /sync/push",      h.ServePush)
http.HandleFunc("POST /sync/snapshot",  h.ServeSnapshot)
http.HandleFunc("GET /sync/tables",     h.ServeTableMeta)
http.HandleFunc("GET /sync/schema",     h.ServeSchema)
```

## Clients

**Swift**

```swift
let client = try SynchroClient(config: SynchroConfig(
    dbPath: dbPath, serverURL: url, authProvider: { token }, clientID: "device-1", appVersion: "1.0.0"
))
try await client.start()
let rows = try client.query("SELECT * FROM workouts")
```

**Kotlin**

```kotlin
val client = SynchroClient(SynchroConfig(
    dbPath = dbPath, serverURL = url, authProvider = { token }, clientID = "device-1", appVersion = "1.0.0"
), context)
client.start()
val rows = client.query("SELECT * FROM workouts")
```

**React Native**

```typescript
const client = new SynchroClient({
    dbPath, serverURL: url, authProvider: () => getToken(), clientID: 'device-1', appVersion: '1.0.0',
});
await client.initialize();
await client.start();
const rows = await client.query('SELECT * FROM workouts');
```

## Links

- [Documentation](https://trainstar.github.io/synchro)
- [API Reference](https://trainstar.github.io/synchro/protocol/api-reference/)
- [License](LICENSE)
