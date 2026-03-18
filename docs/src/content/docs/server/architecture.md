---
title: "Architecture"
description: "Detailed architecture documentation for the Synchro sync engine."
---

## Overview

Synchro is a standalone, offline-first sync library for Go + PostgreSQL. It provides bidirectional data synchronization between mobile/desktop clients and a PostgreSQL server using WAL-based change detection, a bucketed changelog, and checkpoint-based cursors.

<pre class="mermaid">
graph LR
    subgraph Client
        A[Local DB<br/>SQLite] --- B[Changelog]
    end
    subgraph Server
        C[App Tables<br/>RLS optional]
        D[sync_changelog<br/>bucketed, monotonic]
        E[sync_clients<br/>per-device state]
        F[WAL Consumer<br/>pglogrepl]
    end
    A <-->|push/pull<br/>HTTP/JSON| C
    C --> F
    F --> D
</pre>

---

## Configuration at a Glance

The engine is configured with a single `Config` struct. Table metadata (PKs, FKs, column types, nullability, timestamps) is auto-introspected from `pg_catalog`. You only declare table names and optional column exclusions.

```go
engine := synchro.NewEngine(ctx, &synchro.Config{
    DB: db,
    Tables: []synchro.Table{
        {Name: "exercises", Exclude: []string{"search_vector"}},
        {Name: "workouts"},
        {Name: "products"},
    },
    AuthorizeWrite: synchro.Chain(
        synchro.ReadOnly("products"),
        synchro.StampColumn("user_id"),
        synchro.VerifyOwner("user_id"),
    ),
    BucketFunc: synchro.UserBucket("user_id"),
})
```

See the [Configuration](/server/configuration/) page for the full reference.

---

## High-Level Architecture

### WAL --> Changelog --> Bucketed Pull

1. **WAL capture:** A `wal.Consumer` connects to a PostgreSQL logical replication slot (pgoutput protocol). It receives INSERT/UPDATE/DELETE events for registered tables.
2. **Bucket assignment:** Each WAL event passes through the `BucketFunc` (or `JoinResolver` for child table FK chain resolution) to determine which buckets the record belongs to (e.g., `user:abc-123`, `global`).
3. **Changelog write:** One `sync_changelog` row per bucket is written with a monotonic `BIGSERIAL` sequence number.
4. **Pull:** Clients pull changes by querying `sync_changelog WHERE bucket_id = ANY(subs) AND seq > checkpoint`, hydrating records from app tables, then advancing the checkpoint.

This gives O(log n) pull queries (indexed by `bucket_id, seq`) and complete decoupling between write and read paths.

---

## Component Responsibilities

### Engine (`engine.go`)

Top-level orchestrator. Holds all dependencies and exposes `RegisterClient()`, `Push()`, `Pull()`, `Rebuild()`, `Snapshot()`, `TableMetadata()`, `Schema()`, `RunCompaction()`, `StartCompaction()`, and manifest inspection helpers (`CurrentSchemaManifest()`, `SchemaManifestHistory()`). Wires together the push/pull processors, `AuthorizeWrite` chain, `BucketFunc`, conflict resolver, compactor, schema store, and hooks.

### Registry (`registry.go`)

Built automatically at engine startup from `Config.Tables` (a slice of `Table{Name, Exclude}`). The engine introspects `pg_catalog` to discover primary keys, foreign keys, column nullability, and timestamp columns for each table. Parent/child relationships and dependency ordering are auto-detected from the FK graph, so no manual `ParentTable`, `ParentFKCol`, or `Dependencies` configuration is required.

The registry validates the introspected graph at startup: checks FK chains are acyclic and that all referenced tables are registered.

### Push Processor (`push.go`)

Handles client-to-server writes inside a single database transaction:

- **Create:** Inserts new records, handles resurrection of soft-deleted records.
- **Update:** Runs conflict resolution (LWW by default), applies allowed columns.
- **Delete:** Soft-deletes via `deleted_at = now()`, idempotent for already-deleted records.

Authorization is delegated to the `AuthorizeWrite` hook, a composable chain of helpers that replace the old `PushPolicy` / `OwnerColumn` / `ProtectedColumns` fields:

- `ReadOnly(...)`: rejects writes to reference tables.
- `StampColumn("user_id")`: stamps the owner column on INSERT, strips it on UPDATE (server-derived from JWT, never client-provided).
- `VerifyOwner("user_id")`: checks ownership on UPDATE/DELETE by comparing the authenticated user against the existing record.
- `protectedColumns()`: strips server-computed columns (`id`, timestamps) from client payloads.

Push operations still execute under RLS context (`SET LOCAL app.user_id`) for defense-in-depth, but correctness does not depend on RLS. `AuthorizeWrite` is the primary authorization gate.

### Pull Processor (`pull.go`)

Handles server-to-client reads via two paths:

**Legacy pull** (single global checkpoint):

- Queries `sync_changelog` for the client's subscribed buckets after their checkpoint.
- Deduplicates entries for the same record (keeps latest operation).
- Separates deletes from changes, batch-hydrates changed records per table.
- Returns paginated results with `has_more` flag and new checkpoint.
- Detects invalid incremental state and returns `snapshot_required` with a reason.

**Per-bucket pull** (`processPerBucketPull()`):

- Each subscribed bucket has its own independent checkpoint stored in `sync_client_checkpoints`.
- On each pull, `detectStaleBuckets()` compares each bucket's checkpoint against the compaction boundary. If a bucket's checkpoint has fallen behind, that bucket is flagged as stale.
- Stale buckets are not pulled incrementally. Instead, the response includes a `RebuildBuckets` list, signaling the client to call `POST /sync/rebuild` for each stale bucket.
- Non-stale buckets are pulled normally, each advancing its own checkpoint independently.
- This avoids full-database snapshots: only the affected buckets need rebuilding.

### WAL Consumer (`wal/consumer.go`)

Long-running goroutine that connects to PostgreSQL logical replication:

- Uses `pglogrepl` to manage replication slot and standby status.
- Decodes pgoutput messages (relation, insert, update, delete) via `wal.Decoder`.
- Assigns buckets via `BucketFunc` (direct owner column) or `JoinResolver` (FK chain for child tables) and writes changelog entries for each event.
- Tracks LSN position for crash recovery.

### WAL Decoder (`wal/decoder.go`)

Converts raw pgoutput protocol messages into `WALEvent` structs. Maintains a relation cache for column name resolution. Only emits events for tables registered in the `Registry`.

### Bucket Assignment (`ownership.go`)

Determines which buckets a record belongs to. Primary bucket assignment is via `BucketFunc`, a plain function with the signature `func(table, op string, data map[string]any) []string`. It receives the table name, operation type, and record data, then returns the list of bucket IDs the record belongs to.

For the common case, `UserBucket("user_id")` returns a function that reads the `user_id` column from the data map. If the column is present and non-nil, it returns `["user:<value>"]`. If the column is nil, it returns `["global"]`. If the column is missing entirely, it returns nil, allowing FK chain fallback.

The `JoinResolver` handles FK chain resolution for child tables that lack a direct owner column. It walks the parent chain via a single JOIN query to find the root owner. The `JoinResolver` accepts an optional `BucketFunc` for tables where direct column lookup is possible, falling back to FK chain resolution when `BucketFunc` returns nil.

### Conflict Resolver (`conflict.go`)

Determines winner when client and server versions diverge. Built-in resolvers:

- **LWWResolver:** Last-Write-Wins with configurable clock skew tolerance. Supports optimistic concurrency via `BaseUpdatedAt`.
- **ServerWinsResolver:** Server always wins.

Custom resolvers implement the `ConflictResolver` interface.

### Client Store (`client.go`)

Manages `sync_clients` table: registration (upsert), bucket subscriptions, last sync timestamps, and checkpoint tracking.

### Checkpoint Store (`checkpoint.go`)

Tracks per-client pull progress via two mechanisms:

- **Per-bucket checkpoints** (primary): Stored in `sync_client_checkpoints` with one row per `(client_id, bucket_id)`. Each bucket's checkpoint advances independently, enabling granular rebuild of individual buckets without affecting others.
- **Legacy global checkpoint**: The `last_pull_seq` column on `sync_clients` is kept for backwards compatibility with clients that have not migrated to per-bucket cursors. Monotonically advances (never goes backward).

### Changelog Store (`changelog.go`)

Reads and writes `sync_changelog` entries. Supports single writes, batch writes, range queries for pull, and `MinSeq()` for compaction boundary detection.

### Schema Store (`schema.go`)

Builds canonical schema payloads from `pg_catalog`, computes deterministic schema hashes, and persists monotonic manifest versions in `sync_schema_manifest` under advisory lock protection.

### Compactor (`compaction.go`)

Manages changelog compaction to prevent unbounded growth of `sync_changelog`:

- **Stale client deactivation:** Marks clients inactive if they haven't synced within a configurable threshold (default 7 days). Prevents one stale client from blocking compaction for all others.
- **Safe sequence calculation:** Reads the minimum checkpoint from `sync_client_checkpoints` across all active clients. Falls back to `MIN(last_pull_seq)` from `sync_clients` for clients that have not yet migrated to per-bucket checkpoints. The lower of the two values is used as the safe compaction boundary.
- **Batched deletion:** Deletes changelog entries with `seq <= safeSeq` in configurable batches (default 10,000) to avoid long-running transactions.
- **Orchestration:** `RunCompaction()` runs all three steps. `StartCompaction()` runs on a background ticker.

---

## Data Flow

### Push Path

<pre class="mermaid">
sequenceDiagram
    participant Client
    participant Handler as POST /sync/push
    participant Engine
    participant DB as PostgreSQL

    Client->>Handler: Push request
    Handler->>Engine: Push(ctx, userID, req)
    Engine->>DB: BEGIN transaction
    Engine->>DB: SET LOCAL app.user_id (RLS context)

    loop For each PushRecord
        Engine->>Engine: Validate table is registered
        Engine->>Engine: Run AuthorizeWrite chain (ReadOnly, StampColumn, VerifyOwner)
        Engine->>Engine: Parse operation (create/update/delete)
        alt Create
            Engine->>DB: Check for existing record
            Engine->>DB: INSERT RETURNING updated_at
        else Update
            Engine->>DB: Fetch existing record
            Engine->>Engine: Run ConflictResolver
            Engine->>DB: UPDATE allowed columns RETURNING updated_at
        else Delete
            Engine->>DB: Fetch existing record
            Engine->>DB: Soft-delete via deleted_at RETURNING deleted_at
        end
    end

    Engine->>Engine: Fire OnPushAccepted hook (within tx)
    Engine->>DB: COMMIT
    Engine->>DB: Update client last_push_at
    Engine-->>Client: Accepted/rejected results with server-set timestamps (RYOW)
</pre>

The `AuthorizeWrite` chain handles push authorization. RLS policies provide defense-in-depth at the database level but are not required for correctness.

### Pull Path

<pre class="mermaid">
sequenceDiagram
    participant Client
    participant Handler as POST /sync/pull
    participant Engine
    participant DB as PostgreSQL

    Client->>Handler: Pull request
    Handler->>Engine: Pull(ctx, userID, req)
    Engine->>Engine: Validate schema_version + schema_hash against manifest
    Engine->>Engine: Check for initial bootstrap / stale checkpoint

    Engine->>DB: BEGIN read-only tx
    Engine->>DB: SET LOCAL app.user_id (RLS auth context)
    Engine->>DB: Get client bucket subscriptions

    Engine->>DB: SELECT sync_changelog WHERE bucket_id = ANY(subs) AND seq > checkpoint
    Engine->>Engine: Deduplicate (keep latest per table, record_id)
    Engine->>Engine: Separate deletes from changes

    loop For each table with changes
        Engine->>DB: Batch SELECT to hydrate records
    end

    Engine->>DB: COMMIT read-only tx
    Engine->>DB: Advance checkpoint (best-effort)
    Engine->>DB: Update client last_pull_at
    Engine-->>Client: Changes / deletes / checkpoint / schema identifiers
</pre>

### Rebuild Path

When a pull detects that one or more buckets have stale checkpoints (behind the compaction boundary), the client receives a `RebuildBuckets` list in the pull response. For each stale bucket, the client calls `POST /sync/rebuild` to rebuild that bucket's data without affecting other buckets.

<pre class="mermaid">
sequenceDiagram
    participant Client
    participant PullHandler as POST /sync/pull
    participant RebuildHandler as POST /sync/rebuild
    participant Engine
    participant DB as PostgreSQL

    Client->>PullHandler: Pull request
    PullHandler->>Engine: Pull(ctx, userID, req)
    Engine->>Engine: detectStaleBuckets() per bucket checkpoint
    Engine-->>Client: Response with RebuildBuckets: ["user:abc-123"]

    Client->>RebuildHandler: Rebuild request (bucket_id, cursor)
    RebuildHandler->>Engine: Rebuild(ctx, userID, req)
    Engine->>Engine: Validate schema_version + schema_hash against manifest

    Engine->>DB: BEGIN read-only tx
    Engine->>DB: SET LOCAL app.user_id (RLS auth context)

    alt First page
        Engine->>DB: Capture MAX(seq) as rebuild checkpoint
    end

    Engine->>DB: Read records from sync_bucket_edges for bucket
    Engine->>Engine: Paginate by primary key cursor

    Engine->>DB: COMMIT read-only tx

    alt Final page
        Engine->>DB: Persist per-bucket checkpoint in sync_client_checkpoints
    end

    Engine-->>Client: Records + cursor + has_more

    Note over Client: Resumes incremental pull for this bucket from rebuild checkpoint
</pre>

---

## Design Decisions

### WAL-Only (No Triggers)

Change detection uses PostgreSQL logical replication, not database triggers. This avoids:

- Trigger overhead on every write (sync and non-sync paths).
- Trigger maintenance when tables change.
- Coupling the changelog write to the application transaction.

The WAL consumer runs as a separate process, decoupling changelog production from application write latency.

### Ownership Scoping Across All Sync Paths

Ownership scoping is split into three distinct concerns, each with a clear responsibility:

**Bucketing (`BucketFunc`)** determines who gets what data. A plain function `func(table, op string, data map[string]any) []string` that assigns records to buckets. For the common case, `UserBucket("user_id")` reads the owner column from the data map, returning `["user:<value>"]` for user-owned records and `["global"]` for null-owner records. Used by:

- **Pull:** Bucketing ensures clients only receive changelog entries for their subscribed buckets (`user:<id>`, `global`).
- **Rebuild:** `sync_bucket_edges` (populated by the WAL consumer using `BucketFunc`) is used to paginate records for per-bucket rebuilds.

**Push authorization (`AuthorizeWrite`)** handles write-path authorization without requiring RLS. A composable chain of helpers (`ReadOnly`, `StampColumn`, `VerifyOwner`) that validates and transforms each push operation before it hits the database. This is the primary authorization gate for writes.

**RLS (optional defense-in-depth)** provides PostgreSQL row-level security policies as an additional safety net. All push operations still execute under `SET LOCAL app.user_id`, and `GenerateRLSPolicies()` generates standard policies. However, the application works correctly without RLS enabled, since `AuthorizeWrite` and `BucketFunc` are sufficient for correctness.

### Deny-List for Column Filtering (Not Allow-List)

Protected column filtering uses a deny-list: all columns are accepted by default, except server-computed ones (`id`, `created_at`, `updated_at`, `deleted_at`) which are stripped by the `protectedColumns()` helper within the `AuthorizeWrite` chain. The owner column is handled separately by `StampColumn` (set on INSERT, stripped on UPDATE). This avoids needing to update a column allow-list every time a table gains a column.

### database/sql (Not sqlx)

The `DB` interface requires only `ExecContext`, `QueryRowContext`, and `QueryContext` from `database/sql`. This makes Synchro compatible with any database layer (raw `*sql.DB`, `*sql.Tx`, sqlx, pgx stdlib adapter) without adding dependencies.

### net/http (Not Framework)

HTTP handlers use `net/http.Handler` and `http.HandlerFunc`. The consuming application wires them into whatever router they use (Echo, Chi, stdlib mux). Context-based user ID injection via `handler.WithUserID()`.

### slog Logging

Logging uses `log/slog` (stdlib). If you need OTel log correlation, configure that in the host application's logging pipeline and pass the resulting logger via `Config.Logger`.

---

## Bucketing Model

Records are assigned to buckets that determine pull visibility:

| Bucket Type | Format | Contents |
|-------------|--------|----------|
| **User** | `user:<uuid>` | Records owned by a specific user |
| **Global** | `global` | Reference data and rows explicitly configured for global visibility |
| **Group** | `group:<id>` | (Future) Shared group data |
| **Share** | `share:<id>` | (Future) 1:N sharing via multi-bucket assignment |

Each client subscribes to a set of buckets (stored in `sync_clients.bucket_subs`). On registration, a client is automatically subscribed to `["user:<user_id>", "global"]`.

A single record can appear in multiple buckets when resolver logic emits multiple bucket IDs.

---

## Checkpoint Consistency Model

- Checkpoints are monotonic `BIGSERIAL` values from `sync_changelog.seq`.
- **Per-bucket checkpoints** are stored in `sync_client_checkpoints` with one row per `(client_id, bucket_id)`. Each bucket advances independently.
- **Legacy global checkpoint** (`last_pull_seq` on `sync_clients`) is maintained for backwards compatibility. Clients using the legacy path continue to work as before.
- `AdvanceCheckpoint` only moves forward (guards against regression).
- Pull returns `has_more: true` when the result set was truncated by `limit`, signaling the client should pull again.
- Checkpoint advancement is best-effort for pull state, but bootstrap correctness does not depend on `pull(checkpoint=0)`.
- The changelog is append-only; compaction removes entries below the minimum checkpoint across all active clients (considering both per-bucket and legacy checkpoints).
- **Stale bucket detection:** When a bucket's per-bucket checkpoint falls behind the compaction boundary, that bucket is flagged as stale. The pull response includes a `RebuildBuckets` list for the affected buckets, and the client calls `POST /sync/rebuild` to rebuild only those buckets. This replaces the old full-database snapshot approach, where a single stale checkpoint forced a complete rebuild of all data.

### Checksum Verification

Synchro includes a per-record and per-bucket checksum protocol for detecting data drift between server and client.

**Per-record checksums:** The server computes a CRC32 (IEEE polynomial) checksum for each record during hydration. The computation unmarshals the `row_to_json` output into a `map[string]any`, re-marshals with `json.Marshal` (which sorts keys alphabetically), and computes `crc32.ChecksumIEEE`. This ensures deterministic checksums regardless of the original column order. Per-record checksums are included on each `Record` in pull and rebuild responses.

**Per-bucket checksums:** On the final page of a pull response (`has_more: false`), the server includes a `bucket_checksums` map. Each value is the XOR of all per-record checksums stored in `sync_bucket_edges` for that bucket, computed via `BIT_XOR(checksum::int)` in PostgreSQL.

**WAL consumer:** The WAL consumer computes and stores `ComputeRecordChecksumFromMap(event.Data)` in the `sync_bucket_edges.checksum` column when upserting bucket edges. This is the source of truth for per-bucket aggregate checksums.

**Client verification:** After the pull loop completes, the client computes the local bucket checksum (XOR of all stored per-record checksums from `_synchro_bucket_members`) and compares with the server's `bucket_checksums`. A mismatch triggers an automatic bucket rebuild. No extra HTTP call is needed -- verification is inline in the pull response processing.

**Backwards compatibility:** Clients that receive records from older servers (without `checksum` fields) fall back to computing CRC32 locally from the record data.

### Debug Endpoint

`GET /sync/debug?client_id=xxx` returns diagnostic information about a sync client. Requires authentication (UserID from context). Returns:

- **client:** Registration state (ID, platform, app_version, is_active, timestamps, bucket subscriptions, legacy checkpoint)
- **buckets:** Per-bucket state (checkpoint, member count, aggregate checksum)
- **changelog_stats:** Changelog min/max sequence and total entry count
- **server_time:** Current server timestamp

### Failure Semantics

- If a pull succeeds but checkpoint advancement fails, the client will re-pull the same entries on the next request. This is safe because pull responses are idempotent.
- If a rebuild page succeeds but the final checkpoint persist fails, the client re-requests the rebuild from the beginning. Rebuild responses are idempotent.
- If a push transaction fails, no changelog entries are written (the WAL consumer only sees committed changes).
- If the WAL consumer crashes, it resumes from the last durably persisted confirmed LSN. Changes are reprocessed idempotently.
