# Synchro

**Offline-first sync for Go + PostgreSQL.**
*Your tables. Your server. Minimal changes.*

---

## What changes?

Your existing table:

```sql
CREATE TABLE tasks (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    title TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);
```

To enable sync, add one column:

```sql
ALTER TABLE tasks ADD COLUMN deleted_at TIMESTAMPTZ;
```

!!! tip
    If your tables already have `id`, `created_at`, `updated_at`, and a user ownership column — you may need **zero schema changes**. Just add `deleted_at` for soft deletes.

On the server — register what you already have:

```go
registry := synchro.NewRegistry()

registry.Register(&synchro.TableConfig{
    TableName:   "tasks",
    OwnerColumn: "user_id",
})
```

On the client — initialize and use standard SQL:

=== "Swift"

    ```swift
    let client = try SynchroClient(config: SynchroConfig(
        dbPath: "synchro.db",
        serverURL: URL(string: "https://api.example.com")!,
        authProvider: { await getToken() },
        clientID: UIDevice.current.identifierForVendor!.uuidString,
        appVersion: "1.0.0"
    ))
    try await client.start()

    let rows = try client.query("SELECT * FROM tasks")
    ```

=== "Kotlin"

    ```kotlin
    val client = SynchroClient(SynchroConfig(
        dbPath = "synchro.db",
        serverURL = "https://api.example.com",
        authProvider = { getToken() },
        clientID = Settings.Secure.getString(contentResolver, Settings.Secure.ANDROID_ID),
        appVersion = "1.0.0"
    ), context)
    client.start()

    val rows = client.query("SELECT * FROM tasks")
    ```

=== "React Native"

    ```typescript
    const client = new SynchroClient({
        dbPath: 'synchro.db',
        serverURL: 'https://api.example.com',
        authProvider: async () => await getToken(),
        clientID: await getUniqueId(),
        appVersion: '1.0.0',
    });
    await client.start();

    const rows = await client.query('SELECT * FROM tasks');
    ```

No special write API. No ORM. No framework lock-in. **Standard SQL on every platform.**

---

## What Synchro does NOT require

!!! info
    - No triggers on your tables
    - No audit tables or shadow tables
    - No stored procedures or CDC functions
    - No special indexes or extensions
    - No ORM or framework lock-in
    - No separate service to deploy (unless you want one)

---

## How it works

Three steps, nothing hidden:

1. **PostgreSQL WAL captures changes automatically** — no triggers, no polling. Synchro reads the write-ahead log your database already produces.
2. **Synchro assigns changes to user buckets** — each change is routed to the user(s) who own it, based on your ownership column and parent chains.
3. **Client SDKs pull their bucket and sync offline** — each device maintains a local SQLite database, pulls only its data, and pushes local writes back.

```mermaid
flowchart LR
    A["App Tables"] --> B["PostgreSQL WAL"]
    B --> C["Synchro Engine"]
    C --> D["Changelog + Buckets"]
    D --> E["HTTP API"]
    E --> F["Client SQLite"]

    style A fill:#4051b5,color:#fff
    style B fill:#4051b5,color:#fff
    style C fill:#4051b5,color:#fff
    style D fill:#4051b5,color:#fff
    style E fill:#4051b5,color:#fff
    style F fill:#4051b5,color:#fff
```

---

## Full bidirectional sync, built in

Synchro handles both directions out of the box:

- **Push** — client writes flow to the server in a single transaction, validated through PostgreSQL Row-Level Security. No application-layer authorization code required.
- **Pull** — the server sends only the changes each client needs, paginated via checkpoint cursors. Clients never download the full dataset after the initial sync.
- **Conflict resolution** — Last-Write-Wins (LWW) by default, with clock skew tolerance. Switch to ServerWins or plug in a custom resolver per table.
- **Protected columns** — deny-list filtering prevents clients from overwriting server-controlled fields like `id`, `created_at`, or any column you specify.
- **Schema governance** — a version handshake between client and server ensures clients stay compatible. Outdated clients get a clear upgrade signal, not silent data corruption.

---

## Embed or deploy — scale without rewriting

Three deployment modes, one library:

**Mode A: Embedded** — sync runs inside your existing Go application. No extra processes. Add the sync routes to your router and you are done.

**Mode B: Split** — a dedicated sync worker reads WAL and writes the changelog. Your app server handles push/pull HTTP. Shared PostgreSQL database.

**Mode C: Scale** — dedicated sync store with read replicas. WAL consumer, HTTP handlers, and your application each run independently.

Same library. Same protocol. Same SDKs. Moving between modes is a configuration change, not a rewrite.

---

## Platform SDKs

<div class="grid-cards" markdown>

<div class="card" markdown>

### Go Server

The sync engine, WAL consumer, and HTTP handlers. Embeds into any `net/http` compatible router.

[Server docs](server/configuration.md){ .md-button }

</div>

<div class="card" markdown>

### Swift / iOS

Native Swift SDK with local SQLite, background sync, and CDC triggers. SwiftPM distribution.

[Swift docs](clients/swift.md){ .md-button }

</div>

<div class="card" markdown>

### Kotlin / Android

Native Kotlin SDK with local SQLite, background sync, and CDC triggers. Maven Central distribution.

[Kotlin docs](clients/kotlin.md){ .md-button }

</div>

<div class="card" markdown>

### React Native

TypeScript SDK bridging to native SQLite on both platforms. npm distribution.

[React Native docs](clients/react-native.md){ .md-button }

</div>

</div>

---

## Feature highlights

<div class="grid-cards" markdown>

<div class="card" markdown>

### Bidirectional sync

Full push and pull. Clients write locally and sync when connectivity returns.

</div>

<div class="card" markdown>

### Conflict resolution

LWW with clock skew tolerance, ServerWins, or custom resolvers — per table.

</div>

<div class="card" markdown>

### RLS authorization

Push writes execute under `SET LOCAL app.user_id`. PostgreSQL enforces access, not your code.

</div>

<div class="card" markdown>

### Schema governance

Version handshake between client and server. Incompatible clients get a clear upgrade signal.

</div>

<div class="card" markdown>

### Automatic CDC

WAL-based change detection. No triggers, no polling, no audit tables.

</div>

<div class="card" markdown>

### Compaction + snapshot

Changelog compaction keeps storage bounded. Snapshot sync for new or stale clients.

</div>

<div class="card" markdown>

### Column protection

Deny-list filtering prevents clients from overwriting server-controlled columns.

</div>

</div>

---

<div style="text-align: center; margin: 2rem 0;" markdown>

[Get started](getting-started/quickstart.md){ .md-button .md-button--primary }

</div>
