---
title: "Synchro"
description: "Offline-first sync between PostgreSQL and native client SDKs"
template: splash
hero:
  tagline: "Offline-first sync between PostgreSQL and native client SDKs for Swift, Kotlin, and React Native. Your tables. Minimal changes."
  actions:
    - text: Quick Start
      link: /synchro/getting-started/quickstart/
      icon: right-arrow
    - text: GitHub
      link: https://github.com/trainstar/synchro
      variant: minimal
      icon: external
---

import { Card, CardGrid } from '@astrojs/starlight/components';

## Why Synchro

<CardGrid>
  <Card title="Standard SQL" icon="pencil">
    `query()`, `execute()`, transactions, batch writes, prepared statements, and reactive observation. Plain SQL with parameter binding. No ORM, no proprietary query language.
  </Card>
  <Card title="Bidirectional sync" icon="random">
    Clients read and write locally. Changes push to the server and pull to other devices automatically. Works offline, syncs when connectivity returns.
  </Card>
  <Card title="WAL-based CDC" icon="seti:db">
    PostgreSQL logical replication captures changes at the database level. No triggers on your tables, no polling, no audit tables, no shadow tables.
  </Card>
  <Card title="RLS authorization" icon="approve-check">
    Push writes execute under `SET LOCAL app.user_id`. PostgreSQL row-level security enforces access. Authorization lives in the database, not application code.
  </Card>
</CardGrid>

---

## How it works

<pre class="mermaid">
flowchart TB
    subgraph Client["Client Device"]
        direction LR
        APP[Your App] -- "query / execute" --> SDK
        subgraph SDK["Native SDK"]
            direction TB
            DB[(SQLite)]
            CDC[CDC Triggers]
            PQ[Pending Queue]
            DB --> CDC --> PQ
        end
    end

    subgraph Server["Go Server"]
        direction TB
        PG[("PostgreSQL")]
        WAL[WAL Consumer]
        CL[Changelog]
        PG --> WAL --> CL
    end

    PQ -- "push" --> PG
    CL -- "pull" --> DB
</pre>

Your app writes standard SQL to a local SQLite database. CDC triggers detect changes and queue them for push. The Go server uses PostgreSQL WAL to detect changes and serves them to clients via pull. Conflicts are resolved automatically using last-writer-wins with configurable strategies.

---

## One interface, every platform

```swift
// Swift
try client.execute("INSERT INTO tasks (id, user_id, title) VALUES (?, ?, ?)",
    params: [uuid, userId, "Ship v1"])
let tasks = try client.query("SELECT * FROM tasks WHERE completed = 0")
```

```kotlin
// Kotlin
client.execute("INSERT INTO tasks (id, user_id, title) VALUES (?, ?, ?)",
    arrayOf(uuid, userId, "Ship v1"))
val tasks = client.query("SELECT * FROM tasks WHERE completed = 0")
```

```typescript
// React Native (bridges to native Swift and Kotlin SDKs)
await client.execute('INSERT INTO tasks (id, user_id, title) VALUES (?, ?, ?)',
    [uuid, userId, 'Ship v1']);
const tasks = await client.query('SELECT * FROM tasks WHERE completed = 0');
```

---

## Deploy your way

<CardGrid>
  <Card title="Embedded" icon="laptop">
    Import as a Go library into your existing server. Add sync routes to your router. No extra processes.
  </Card>
  <Card title="Standalone" icon="rocket">
    Run `synchrod` as a dedicated binary. Or split the WAL consumer and HTTP handlers across services for scale.
  </Card>
</CardGrid>

Same library. Same protocol. Same SDKs. Moving between modes is a configuration change, not a rewrite.

---

<CardGrid>
  <Card title="Get started" icon="right-arrow">
    Follow the [quick start guide](/synchro/getting-started/quickstart/) to set up sync in minutes.
  </Card>
  <Card title="Architecture" icon="open-book">
    Read the [architecture docs](/synchro/server/architecture/) to understand the sync protocol in depth.
  </Card>
</CardGrid>
