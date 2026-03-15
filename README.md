<p align="center">
  <img src="docs/assets/logo.svg" alt="Synchro" width="200"><br><br>
  <a href="https://pkg.go.dev/github.com/trainstar/synchro"><img src="https://img.shields.io/github/go-mod/go-version/trainstar/synchro?logo=go&logoColor=white" alt="Go"></a>
  <a href="https://pkg.go.dev/github.com/trainstar/synchro"><img src="https://pkg.go.dev/badge/github.com/trainstar/synchro.svg" alt="Go Reference"></a>
  <a href="https://www.npmjs.com/package/@trainstar/synchro-react-native"><img src="https://img.shields.io/npm/v/@trainstar/synchro-react-native?logo=npm&logoColor=white&label=npm" alt="npm"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/trainstar/synchro" alt="License"></a>
  <a href="https://trainstar.github.io/synchro"><img src="https://img.shields.io/badge/docs-trainstar.github.io%2Fsynchro-blue" alt="Docs"></a>
</p>

<p align="center">Offline-first sync between PostgreSQL and native client SDKs for Swift, Kotlin, and React Native. Go server you can embed or deploy standalone. Your tables. Minimal changes.</p>

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
