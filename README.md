<p align="center">
  <img src="docs/public/logo.svg" alt="Synchro" width="280"><br><br>
  <a href="https://github.com/trainstar/synchro/actions/workflows/release.yml"><img src="https://img.shields.io/github/actions/workflow/status/trainstar/synchro/release.yml?branch=master&event=workflow_dispatch&label=release&logo=github" alt="Release"></a>
  <a href="https://pkg.go.dev/github.com/trainstar/synchro"><img src="https://img.shields.io/github/go-mod/go-version/trainstar/synchro?logo=go&logoColor=white" alt="Go"></a>
  <a href="https://pkg.go.dev/github.com/trainstar/synchro"><img src="https://pkg.go.dev/badge/github.com/trainstar/synchro.svg" alt="Go Reference"></a>
  <a href="https://www.npmjs.com/package/@trainstar/synchro-react-native"><img src="https://img.shields.io/npm/v/@trainstar/synchro-react-native?logo=npm&logoColor=white&label=npm" alt="npm"></a>
  <a href="https://central.sonatype.com/artifact/fit.trainstar/synchro"><img src="https://img.shields.io/maven-central/v/fit.trainstar/synchro?logo=apache-maven&logoColor=white&label=maven" alt="Maven Central"></a>
  <a href="https://github.com/trainstar/synchro"><img src="https://img.shields.io/github/v/tag/trainstar/synchro?filter=!v*&logo=swift&logoColor=white&label=SPM" alt="SPM"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/trainstar/synchro" alt="License"></a>
  <a href="https://trainstar.github.io/synchro"><img src="https://img.shields.io/badge/docs-trainstar.github.io%2Fsynchro-blue" alt="Docs"></a>
</p>

<p align="center">Offline-first sync between PostgreSQL and native client SDKs for Swift, Kotlin, and React Native. Go server you can embed or deploy standalone. Your tables. Zero schema changes.</p>

---

## How It Works

Every client reads and writes to a local SQLite database using standard SQL. Synchro syncs changes bidirectionally with your PostgreSQL server in the background. WAL-based change detection means no triggers, no polling, no custom APIs. Conflicts are resolved automatically using last-writer-wins with configurable strategies.

```mermaid
flowchart TB
    APP[Your App] -- "query / execute" --> DB

    subgraph Client["Client Device"]
        subgraph SDK["Native SDK"]
            DB[(SQLite)]
            CDC[CDC Triggers]
            PQ[Pending Queue]
            DB --> CDC --> PQ
        end
    end

    subgraph Server["Go Server"]
        PG[("PostgreSQL")]
        WAL[WAL Consumer]
        CL[Changelog]
        PG --> WAL --> CL
    end

    PQ -- "push" --> PG
    CL -- "pull" --> DB

    style Client fill:#1a1a2e,color:#fff
    style Server fill:#16213e,color:#fff
```

> **Swift, Kotlin, and React Native** all use the same architecture. React Native bridges to the native Swift (iOS) and Kotlin (Android) SDKs. Your app writes standard SQL to a local SQLite database. CDC triggers detect changes and queue them for push. The server uses PostgreSQL WAL to detect changes and serves them to clients via pull.

## Why Synchro

- **Standard SQL.** `query()`, `execute()`, transactions, batch writes, prepared statements, and reactive observation. Plain SQL with parameter binding. No proprietary query language, no object wrappers.
- **Full bidirectional sync with conflict resolution.** Reads and writes sync automatically. Not read-only replication, not bring-your-own-write-path.
- **WAL-based change detection.** PostgreSQL logical replication captures changes at the database level. No triggers, no polling, no application-layer diffing.
- **RLS-enforced authorization.** Row-level security policies in Postgres guard your data. Authorization lives in the database, not in application code.
- **Embed or deploy standalone.** Import as a Go library into your existing server, or run `synchrod` as a standalone binary. Scale without rewriting.
- **Native SDKs.** Swift, Kotlin, and React Native. Local SQLite, automatic change tracking, background sync, offline queue.

## Quick Start

### Install

**Server**

```bash
go get github.com/trainstar/synchro
```

**Swift / iOS** (Swift Package Manager)

```swift
.package(url: "https://github.com/trainstar/synchro.git", from: "0.1.2")
```

**Kotlin / Android** (Gradle)

```kotlin
implementation("fit.trainstar:synchro:0.1.2")
```

**React Native** (bridges to the native Swift and Kotlin SDKs above)

```bash
npm install @trainstar/synchro-react-native
cd ios && pod install  # installs the native iOS dependency
```

### Server Setup

Register the tables you want to sync and wire the HTTP endpoints. Synchro handles the rest: WAL subscription, changelog management, conflict resolution, and client state tracking.

```go
registry := synchro.NewRegistry()
registry.Register(&synchro.TableConfig{
    TableName:   "tasks",
    OwnerColumn: "user_id",
})
registry.Register(&synchro.TableConfig{
    TableName:    "comments",
    OwnerColumn:  "user_id",
    ParentTable:  "tasks",
    ParentColumn: "task_id",
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

### Client Usage

Every client SDK exposes a full SQL interface: `query()`, `execute()`, transactions, batch writes, prepared statements, and reactive observation. You pass standard SQL with parameter binding. Changes sync automatically in the background.

**Swift**

```swift
let client = try SynchroClient(config: SynchroConfig(
    dbPath: dbPath, serverURL: url,
    authProvider: { token }, clientID: deviceID, appVersion: "1.0.0"
))
try await client.start()

// Write locally, syncs to server automatically
try client.execute("INSERT INTO tasks (id, user_id, title) VALUES (?, ?, ?)",
    params: [uuid, userId, "Ship v1"])

// Read from local SQLite, always fast
let tasks = try client.query("SELECT * FROM tasks WHERE completed = 0")
```

**Kotlin**

```kotlin
val client = SynchroClient(SynchroConfig(
    dbPath = "synchro.db", serverURL = url,
    authProvider = { token }, clientID = deviceId, appVersion = "1.0.0"
), context)
client.start()

// Write locally, syncs to server automatically
client.execute("INSERT INTO tasks (id, user_id, title) VALUES (?, ?, ?)",
    listOf(uuid, userId, "Ship v1"))

// Read from local SQLite, always fast
val tasks = client.query("SELECT * FROM tasks WHERE completed = 0")
```

**React Native**

```typescript
const client = new SynchroClient({
    dbPath: 'synchro.db', serverURL: url,
    authProvider: () => getToken(), clientID: deviceId, appVersion: '1.0.0',
});
await client.initialize();
await client.start();

// Write locally, syncs to server automatically
await client.execute('INSERT INTO tasks (id, user_id, title) VALUES (?, ?, ?)',
    [uuid, userId, 'Ship v1']);

// Read from local SQLite, always fast
const tasks = await client.query('SELECT * FROM tasks WHERE completed = 0');
```

## What You Need

| Component | What Changes |
|-----------|-------------|
| Your tables | No schema changes required. Synchro adapts to your columns. |
| PostgreSQL | Set `wal_level = logical` (one-time config) |
| Your server | Register tables + wire 6 HTTP endpoints |
| Client app | `query()` and `execute()` against local SQLite |

Synchro introspects your tables at startup and adapts automatically. If a change-tracking timestamp column exists, you get conflict resolution. If a soft-delete column exists, you get soft deletes. Neither is required. Tables without them still sync, with last-push-wins and hard deletes. The column names to look for default to `updated_at` and `deleted_at` but are configurable.

## Requirements

| Component | Minimum Version |
|-----------|----------------|
| Go | 1.22+ |
| PostgreSQL | 14+ (logical replication) |
| iOS | 16.0+ |
| macOS | 13.0+ |
| Android | API 24+ (minSdk 24) |
| React Native | 0.83+ |
| Node.js | 20+ |
| JDK | 17+ (for Android builds) |

## Links

- [Documentation](https://trainstar.github.io/synchro)
- [Quick Start Guide](https://trainstar.github.io/synchro/getting-started/quickstart/)
- [Architecture](https://trainstar.github.io/synchro/server/architecture/)
- [API Reference](https://trainstar.github.io/synchro/protocol/api-reference/)
- [License](LICENSE)
