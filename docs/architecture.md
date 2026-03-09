# Synchro Architecture

## Overview

Synchro is a standalone, offline-first sync library for Go + PostgreSQL. It provides bidirectional data synchronization between mobile/desktop clients and a PostgreSQL server using WAL-based change detection, a bucketed changelog, and checkpoint-based cursors.

```
Client Device                          Server
+------------------+                   +-------------------------------------------+
|  Local DB        |   push/pull       |  PostgreSQL                               |
|  (SQLite, etc.)  | <--------------> |  +-- App Tables (RLS-enforced)            |
|  Changelog       |   HTTP/JSON       |  +-- sync_changelog (bucketed, monotonic) |
|  Checkpoint seq  |                   |  +-- sync_clients (per-device state)      |
+------------------+                   |                                           |
                                       |  WAL Consumer (pglogrepl)                 |
                                       |    reads WAL -> assigns buckets ->        |
                                       |    writes sync_changelog entries          |
                                       +-------------------------------------------+
```

## High-Level Architecture

### WAL -> Changelog -> Bucketed Pull

1. **WAL capture** -- A `wal.Consumer` connects to a PostgreSQL logical replication slot (pgoutput protocol). It receives INSERT/UPDATE/DELETE events for registered tables.
2. **Bucket assignment** -- Each WAL event passes through a `BucketAssigner` (typically the `OwnershipResolver`) to determine which buckets the record belongs to (e.g., `user:abc-123`, `global`).
3. **Changelog write** -- One `sync_changelog` row per bucket is written with a monotonic `BIGSERIAL` sequence number.
4. **Pull** -- Clients pull changes by querying `sync_changelog WHERE bucket_id = ANY(subs) AND seq > checkpoint`, hydrating records from app tables, then advancing the checkpoint.

This gives O(log n) pull queries (indexed by `bucket_id, seq`) and complete decoupling between write and read paths.

## Component Responsibilities

### Engine (`engine.go`)

Top-level orchestrator. Holds all dependencies and exposes `RegisterClient()`, `Push()`, `Pull()`, `Snapshot()`, `TableMetadata()`, `Schema()`, `RunCompaction()`, `StartCompaction()`, and manifest inspection helpers (`CurrentSchemaManifest()`, `SchemaManifestHistory()`). Wires together the push/pull processors, conflict resolver, ownership resolver, compactor, schema store, and hooks.

### Registry (`registry.go`)

Holds `TableConfig` entries that define sync behavior per table: `PushPolicy`, ownership column, parent relationships, protected columns, sync column projections, dependencies, and bucket configuration.

Validates the full config graph at engine startup: checks parent chains terminate at an owner, detects cycles, and enforces that pushable tables have ownership paths.

### Push Processor (`push.go`)

Handles client-to-server writes inside a single database transaction:

- **Create** -- Inserts new records, enforces ownership column, handles resurrection of soft-deleted records.
- **Update** -- Runs conflict resolution (LWW by default), applies allowed columns via deny-list filtering.
- **Delete** -- Soft-deletes via `deleted_at = now()`, idempotent for already-deleted records.

All push operations execute under RLS context (`SET LOCAL app.user_id`).

### Pull Processor (`pull.go`)

Handles server-to-client reads:

- Queries `sync_changelog` for the client's subscribed buckets after their checkpoint.
- Deduplicates entries for the same record (keeps latest operation).
- Separates deletes from changes, batch-hydrates changed records per table.
- Returns paginated results with `has_more` flag and new checkpoint.
- Detects invalid incremental state and returns `snapshot_required` with a reason.
- Handles full snapshot pagination for bootstrap and rebuild flows.

### WAL Consumer (`wal/consumer.go`)

Long-running goroutine that connects to PostgreSQL logical replication:

- Uses `pglogrepl` to manage replication slot and standby status.
- Decodes pgoutput messages (relation, insert, update, delete) via `wal.Decoder`.
- Assigns buckets and writes changelog entries for each event.
- Tracks LSN position for crash recovery.

### WAL Decoder (`wal/decoder.go`)

Converts raw pgoutput protocol messages into `WALEvent` structs. Maintains a relation cache for column name resolution. Only emits events for tables registered in the `Registry`.

### Ownership Resolver (`ownership.go`)

Determines which buckets a record belongs to. The default `JoinResolver`:

- Tables with `OwnerColumn` -- reads owner directly from record data (zero queries).
- Tables with `BucketByColumn` + `BucketPrefix` -- uses fast-path bucket assignment without extra joins.
- Nullable bucket values can emit `global` only when `GlobalWhenBucketNull=true`.
- Child tables -- walks the parent chain via a single JOIN query to find the root owner.

### Conflict Resolver (`conflict.go`)

Determines winner when client and server versions diverge. Built-in resolvers:

- **LWWResolver** -- Last-Write-Wins with configurable clock skew tolerance. Supports optimistic concurrency via `BaseUpdatedAt`.
- **ServerWinsResolver** -- Server always wins.

Custom resolvers implement the `ConflictResolver` interface.

### Client Store (`client.go`)

Manages `sync_clients` table: registration (upsert), bucket subscriptions, last sync timestamps, and checkpoint tracking.

### Checkpoint Store (`checkpoint.go`)

Tracks per-client pull progress. Monotonically advances `last_pull_seq` (never goes backward).

### Changelog Store (`changelog.go`)

Reads and writes `sync_changelog` entries. Supports single writes, batch writes, range queries for pull, and `MinSeq()` for compaction boundary detection.

### Schema Store (`schema.go`)

Builds canonical schema payloads from `pg_catalog`, computes deterministic schema hashes, and persists monotonic manifest versions in `sync_schema_manifest` under advisory lock protection.

### Compactor (`compaction.go`)

Manages changelog compaction to prevent unbounded growth of `sync_changelog`:

- **Stale client deactivation** -- Marks clients inactive if they haven't synced within a configurable threshold (default 7 days). Prevents one stale client from blocking compaction for all others.
- **Safe sequence calculation** -- Computes `MIN(last_pull_seq)` across all active clients as the safe compaction boundary.
- **Batched deletion** -- Deletes changelog entries with `seq <= safeSeq` in configurable batches (default 10,000) to avoid long-running transactions.
- **Orchestration** -- `RunCompaction()` runs all three steps. `StartCompaction()` runs on a background ticker.

## Data Flow

### Push Path

```
Client -> POST /sync/push
  1. Begin transaction
  2. SET LOCAL app.user_id = '<user_id>'  (RLS context)
  3. For each PushRecord:
     a. Validate table is registered and pushable
     b. Parse operation (create/update/delete)
     c. For creates: check for existing record, enforce ownership, INSERT RETURNING updated_at
     d. For updates: fetch existing, run ConflictResolver, UPDATE allowed columns RETURNING updated_at
     e. For deletes: fetch existing, soft-delete via deleted_at RETURNING deleted_at
  4. Fire OnPushAccepted hook (within transaction)
  5. Commit transaction
  6. Update client last_push_at
  7. Return accepted/rejected results with server-set timestamps (RYOW)
```

RLS policies enforce authorization at the database level. The push processor does not walk FK chains -- Postgres RLS handles it.

### Pull Path

```
Client -> POST /sync/pull
  1. Validate `schema_version` + `schema_hash` against server manifest
  2. If initial bootstrap is incomplete, return `snapshot_required=initial_sync_required`
  3. Check for stale checkpoint: if client checkpoint < MIN(seq) in changelog, return `snapshot_required=checkpoint_before_retention`
  4. Start read-only tx and set RLS auth context
  5. Get client's bucket subscriptions (e.g., ["user:abc-123", "global"])
  6. Query sync_changelog WHERE bucket_id = ANY(subs) AND seq > checkpoint
  7. Deduplicate: keep latest entry per (table, record_id)
  8. Separate deletes from changes
  9. Hydrate changed records: batch SELECT per table
  10. Commit read-only tx, then best-effort advance checkpoint in client state
  11. Update client last_pull_at and return changes/deletes/checkpoint/schema identifiers
```

### Snapshot Path

```text
Client -> POST /sync/snapshot
  1. Validate `schema_version` + `schema_hash` against server manifest
  2. Start read-only tx and set RLS auth context
  3. On the first page, capture `MAX(seq)` as the snapshot checkpoint
  4. Read visible rows directly from app tables in registration order
  5. Page deterministically by table index + primary key
  6. Return records plus a stateless cursor carrying the captured checkpoint
  7. On the final page, reactivate the client and persist the snapshot checkpoint
  8. Client resumes normal incremental pull from that checkpoint
```

## Design Decisions

### WAL-Only (No Triggers)

Change detection uses PostgreSQL logical replication, not database triggers. This avoids:

- Trigger overhead on every write (sync and non-sync paths).
- Trigger maintenance when tables change.
- Coupling the changelog write to the application transaction.

The WAL consumer runs as a separate process, decoupling changelog production from application write latency.

### RLS for Push Authorization (Not FK Walking)

Push operations set `app.user_id` via `SET LOCAL` and let PostgreSQL Row-Level Security enforce access. This means:

- Authorization logic lives in the database, not application code.
- Child table access is enforced via `EXISTS (SELECT 1 FROM parent WHERE ...)` subquery policies.
- The push processor does not need to walk parent chains -- RLS handles it.
- Ownership resolution (for bucketing) is separate from authorization.

### Deny-List for Column Filtering (Not Allow-List)

The `ProtectedColumns` model is a deny-list: all columns are accepted by default, except explicitly protected ones (`id`, `created_at`, `updated_at`, `deleted_at`, owner column, parent FK). This avoids needing to update a column allow-list every time a table gains a column.

### database/sql (Not sqlx)

The `DB` interface requires only `ExecContext`, `QueryRowContext`, and `QueryContext` from `database/sql`. This makes synchro compatible with any database layer (raw `*sql.DB`, `*sql.Tx`, sqlx, pgx stdlib adapter) without adding dependencies.

### net/http (Not Framework)

HTTP handlers use `net/http.Handler` and `http.HandlerFunc`. The consuming application wires them into whatever router they use (Echo, Chi, stdlib mux). Context-based user ID injection via `handler.WithUserID()`.

### slog Logging

Logging uses `log/slog` (stdlib). If you need OTel log correlation, configure that in the host application's logging pipeline and pass the resulting logger via `Config.Logger`.

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

## Checkpoint Consistency Model

- Checkpoints are monotonic `BIGSERIAL` values from `sync_changelog.seq`.
- A client's checkpoint is the highest seq it has successfully processed.
- `AdvanceCheckpoint` only moves forward (guards against `last_pull_seq < $2`).
- Pull returns `has_more: true` when the result set was truncated by `limit`, signaling the client should pull again.
- Checkpoint advancement is best-effort for pull state, but bootstrap correctness does not depend on `pull(checkpoint=0)`.
- The changelog is append-only; compaction removes entries below the minimum checkpoint across all active clients. When a client's checkpoint falls behind the compaction boundary, the server returns `snapshot_required` and the client must rebuild via `/sync/snapshot`.

### Failure Semantics

- If a pull succeeds but checkpoint advancement fails, the client will re-pull the same entries on the next request. This is safe because pull responses are idempotent.
- If a push transaction fails, no changelog entries are written (the WAL consumer only sees committed changes).
- If the WAL consumer crashes, it resumes from the last durably persisted confirmed LSN. Changes are reprocessed idempotently.
