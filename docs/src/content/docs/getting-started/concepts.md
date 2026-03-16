---
title: "Core Concepts"
description: "How WAL capture, buckets, checkpoints, and conflict resolution work in Synchro."
---

This page explains the foundational ideas behind Synchro: how changes are captured, how data is partitioned, how clients stay in sync, and how conflicts are resolved.

---

## WAL-Based Change Detection

Synchro captures data changes through PostgreSQL **logical replication**, not triggers. When your application writes to a synced table, PostgreSQL records the change in the Write-Ahead Log (WAL). A long-running Synchro consumer decodes these WAL events and writes structured entries into the `sync_changelog`.

<pre class="mermaid">
graph LR
    A[App Write] --> B[PostgreSQL WAL]
    B --> C[Synchro Consumer]
    C --> D[sync_changelog]
</pre>

**Why not triggers?**

| | WAL Replication | Triggers |
|---|---|---|
| **Transaction overhead** | Zero -- WAL is written regardless | Trigger function executes inside every write transaction |
| **Maintenance** | One consumer process | Trigger DDL on every synced table; must be recreated on schema changes |
| **Decoupling** | Consumer can lag, restart, or crash without affecting the application | Trigger failure can abort the application transaction |
| **Visibility** | Captures all changes including those from migrations, backfills, and direct SQL | Only captures changes routed through the trigger |

The consumer connects using PostgreSQL's replication protocol (`replication=database` connection parameter) and subscribes to a named publication. It processes events in LSN order, persists its position for crash recovery, and sends standby heartbeats to prevent slot invalidation.

---

## Buckets and Subscriptions

Every changelog entry is tagged with a **bucket ID**. Buckets are the unit of data partitioning -- they determine which changes a client receives during pull.

### How Buckets Are Assigned

When the WAL consumer processes a change, it calls the configured `BucketAssigner` to determine which bucket(s) the record belongs to.

The default `JoinResolver` uses the registry metadata:

- **Tables with an `OwnerColumn`** -- Bucket ID is `user:<owner_column_value>`. A task owned by user `abc-123` goes into bucket `user:abc-123`.
- **Child tables (via `ParentTable`)** -- The resolver walks the parent chain up to the root table and uses the root's owner column.
- **Tables without ownership** -- Records go into the `global` bucket, visible to all clients.
- **Custom resolvers** -- Implement the `BucketAssigner` interface for multi-tenant, team-based, or content-sharing bucket strategies.

### Client Subscriptions

When a client registers, the server computes its bucket subscriptions (typically `["user:<user_id>", "global"]`). During pull, only changelog entries matching the client's subscriptions are returned.

:::tip[Bucket changes are tracked automatically]
If a record's ownership changes (e.g., transferred to another user), the WAL consumer detects the bucket change and emits both a DELETE in the old bucket and an INSERT in the new bucket. Clients in the old bucket see a deletion; clients in the new bucket see the record appear.
:::

---

## Checkpoints

The `sync_changelog` table uses a monotonically increasing `BIGSERIAL` column (`seq`) as its cursor. Each client tracks its **checkpoint** -- the highest `seq` value it has processed.

### How Checkpoints Work

1. Client sends a pull request with its current checkpoint (e.g., `checkpoint: 500`).
2. Server queries `sync_changelog` for entries where `seq > 500` matching the client's bucket subscriptions.
3. Server returns the changes along with the new checkpoint (e.g., `checkpoint: 742`).
4. Client applies the changes locally and stores `742` as its new checkpoint.

### Properties

- **Idempotent advancement** -- Checkpoint only moves forward. Re-sending the same checkpoint returns the same changes. The server enforces `last_pull_seq < new_seq` on update.
- **Per-client isolation** -- Each client has its own checkpoint. Slow clients do not block fast ones.
- **Compaction boundary** -- If changelog compaction is enabled, entries below a retention threshold are deleted. If a client's checkpoint falls behind the compaction boundary, the server responds with `snapshot_required: true` and the client must re-bootstrap via the snapshot endpoint.

---

## Conflict Resolution

Conflicts occur when a client pushes a change to a record that was modified on the server since the client last pulled it.

### Strategies

Synchro supports three conflict resolution strategies:

### LWW (Last-Write-Wins)

The default strategy. Compares the client's `client_updated_at` timestamp against the server's `updated_at`, adjusting for configurable clock skew tolerance.

```go
engine, _ := synchro.NewEngine(synchro.Config{
    DB:                 db,
    Registry:           registry,
    ClockSkewTolerance: 5 * time.Second, // favour the client within 5s
})
```

If the client provides a `base_updated_at` (optimistic concurrency), the resolver first checks whether the server record changed since that base version:

- **Server unchanged since base** -- Client wins (no true conflict).
- **Server changed since base** -- Falls back to timestamp comparison.

### ServerWins

The server version always wins. Client changes are rejected with a `conflict` status and the current server version is returned so the client can reconcile.

```go
engine, _ := synchro.NewEngine(synchro.Config{
    DB:               db,
    Registry:         registry,
    ConflictResolver: &synchro.ServerWinsResolver{},
})
```

### Custom

Implement the `ConflictResolver` interface for domain-specific logic.

```go
type ConflictResolver interface {
    Resolve(ctx context.Context, conflict Conflict) (Resolution, error)
}
```

The `Conflict` struct provides full context: table name, record ID, client/server data as JSON, timestamps, and the client's base version.

### Push Flow

<pre class="mermaid">
sequenceDiagram
    participant Client
    participant Server
    participant DB as PostgreSQL

    Client->>Server: POST /sync/push (changes)
    Server->>DB: BEGIN + SET LOCAL app.user_id
    loop Each change
        Server->>DB: Read current server version
        alt No conflict
            Server->>DB: Apply change
            Server-->>Server: Status: applied
        else Conflict detected
            Server->>Server: ConflictResolver.Resolve()
            alt Client wins
                Server->>DB: Apply client change
                Server-->>Server: Status: applied
            else Server wins
                Server-->>Server: Status: conflict + server version
            end
        end
    end
    Server->>DB: COMMIT
    Server-->>Client: accepted[] + rejected[]
</pre>

Each push is processed in a single database transaction under RLS context. The client receives per-record results: `applied`, `conflict` (with the current server version), `rejected_terminal`, or `rejected_retryable`.

---

## Schema Governance

Synchro enforces **server-authoritative schema**. The server computes a canonical schema from `pg_catalog` for all registered tables and produces a versioned hash. Clients must present a matching version and hash on every request.

### How It Works

1. **Server computes schema** -- On first request, the server reads column definitions from `pg_catalog`, computes a SHA-256 hash, and persists it in `sync_schema_manifest` with an auto-incrementing version.
2. **Client receives schema on registration** -- The register response includes `schema_version` and `schema_hash`.
3. **Handshake on every request** -- Push, pull, and snapshot requests include the client's `schema_version` and `schema_hash`. The server compares them against the current manifest.
4. **Mismatch handling** -- If the client's schema does not match, the server returns HTTP `409 Conflict` with the current server version and hash. The client re-fetches the schema via `GET /sync/schema` and migrates its local SQLite tables.

<pre class="mermaid">
graph TD
    A[Client sends request] --> B{schema_version + schema_hash match?}
    B -->|Yes| C[Process normally]
    B -->|No| D[409 Conflict]
    D --> E[Client calls GET /sync/schema]
    E --> F[Client migrates local tables]
    F --> G[Client retries request]
</pre>

:::note[No drift]
The schema contract is deterministic. Two servers with the same PostgreSQL schema and the same registered tables will always produce the same hash. There is no manual versioning -- the version increments automatically when the schema changes.
:::

### Schema Reconciliation

When a client connects and receives a new schema version, the schema manager reconciles the local SQLite database **additively**:

- **New columns** from the server are added via `ALTER TABLE ADD COLUMN`
- **New tables** from the server are created with CDC triggers
- **Removed columns** (present locally but not in server schema) are **preserved** — no data is dropped
- **Removed tables** (present locally but not in server schema) are **preserved** — no data is dropped
- **Local-only tables** (created by the app, not in server schema) are **never touched**
- **Extra columns** on synced tables (added locally by the app) are **never touched**

The only scenario that triggers a destructive rebuild is when a synced column's **type changes** (e.g., a column that was `TEXT` is now `INTEGER`). This is a rare, intentional schema migration that requires re-bootstrapping the local database.

:::tip[Safe for local-only tables]
You can freely create local-only tables (e.g., `app_settings`, `drafts`, `cache`) alongside synced tables. Schema reconciliation will never affect them.
:::

---

## Sync Lifecycle

The full lifecycle from first connection to steady-state sync:

<pre class="mermaid">
sequenceDiagram
    participant Client
    participant Server

    Client->>Server: POST /sync/register
    Server-->>Client: client_id, schema_version, schema_hash

    Client->>Server: GET /sync/schema
    Server-->>Client: table definitions, columns, types
    Note over Client: Create/migrate local SQLite tables

    Client->>Server: POST /sync/snapshot (page 1)
    Server-->>Client: records, cursor, has_more=true
    Client->>Server: POST /sync/snapshot (page N)
    Server-->>Client: records, has_more=false, checkpoint
    Note over Client: Bootstrap complete

    loop Sync Loop
        Client->>Server: POST /sync/push (pending changes)
        Server-->>Client: accepted/rejected results
        Client->>Server: POST /sync/pull (checkpoint)
        Server-->>Client: changes, deletes, new checkpoint
    end
</pre>

### Phase 1: Registration

The client registers with the server, providing its `client_id`, `platform`, and `app_version`. The server creates or updates the client record and returns the current schema version and hash.

### Phase 2: Schema Sync

The client fetches the full schema definition (`GET /sync/schema`) which includes every synced table's columns, types, primary key, push policy, and parent relationships. The client uses this to create or migrate local SQLite tables.

### Phase 3: Snapshot Bootstrap

The client pages through a full snapshot of its subscribed data. The snapshot endpoint returns records in table-dependency order with a stateless cursor for pagination. When the final page arrives (`has_more: false`), the client stores the checkpoint and transitions to incremental sync.

### Phase 4: Incremental Sync

The client enters a push-then-pull loop:

1. **Push** -- Send any locally queued changes. The server returns per-record results.
2. **Pull** -- Send the current checkpoint. The server returns all changes since that checkpoint, along with deletes and a new checkpoint value.

This loop runs on a configurable interval (typically 5-30 seconds) and on-demand when the user makes local changes.

---

## Client-Side CDC

Client SDKs use SQLite triggers to automatically track local changes. There is no special write API -- applications use normal SQL `INSERT`, `UPDATE`, and `DELETE` statements against their local tables.

### How It Works

For each synced table, the SDK creates three SQLite triggers:

| Trigger | Purpose |
|---|---|
| `AFTER INSERT` | Records the new row ID and table name in the pending changes queue |
| `AFTER UPDATE` | Records the updated row ID in the pending changes queue |
| `BEFORE DELETE` | Converts the hard delete into a soft delete (`SET deleted_at = datetime('now')`) and records it in the pending queue |

### Sync Lock

During pull application, the SDK sets a **sync lock** flag that disables CDC triggers. This prevents incoming server changes from being re-queued as pending pushes, which would create an infinite echo loop.

### Pending Changes Queue

The pending queue is a local SQLite table that tracks which records have been modified since the last push. During push, the SDK:

1. Reads all pending entries.
2. Hydrates each entry by reading the current row from the local table.
3. Sends the hydrated changes to the server.
4. On success, drains the acknowledged entries from the queue.

:::note[Soft deletes everywhere]
The `BEFORE DELETE` trigger converts hard deletes to soft deletes locally, matching the server convention. The `deleted_at` timestamp is set, and the sync engine pushes this as a delete operation. The server marks the record as deleted and propagates it via the changelog to other clients.
:::
