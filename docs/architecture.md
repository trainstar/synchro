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

Top-level orchestrator. Holds all dependencies and exposes `RegisterClient()`, `Push()`, `Pull()`, and `TableMetadata()`. Wires together the push/pull processors, conflict resolver, ownership resolver, and hooks.

### Registry (`registry.go`)

Holds `TableConfig` entries that define sync behavior per table: direction (bidirectional, server-only, system-and-user), ownership column, parent relationships, protected columns, sync column projections, and dependencies.

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
- `ServerOnly` tables -- always assigned to `global` bucket.
- `SystemAndUser` tables with NULL owner -- assigned to `global`.
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

Reads and writes `sync_changelog` entries. Supports single writes, batch writes, and range queries for pull.

## Data Flow

### Push Path

```
Client -> POST /sync/push
  1. Begin transaction
  2. SET LOCAL app.user_id = '<user_id>'  (RLS context)
  3. For each PushRecord:
     a. Validate table is registered and pushable
     b. Parse operation (create/update/delete)
     c. For creates: check for existing record, enforce ownership, INSERT
     d. For updates: fetch existing, run ConflictResolver, UPDATE allowed columns
     e. For deletes: fetch existing, soft-delete via deleted_at
  4. Fire OnPushAccepted hook (within transaction)
  5. Commit transaction
  6. Update client last_push_at
  7. Return accepted/rejected results
```

RLS policies enforce authorization at the database level. The push processor does not walk FK chains -- Postgres RLS handles it.

### Pull Path

```
Client -> POST /sync/pull
  1. Get client's bucket subscriptions (e.g., ["user:abc-123", "global"])
  2. Query sync_changelog WHERE bucket_id = ANY(subs) AND seq > checkpoint
  3. Deduplicate: keep latest entry per (table, record_id)
  4. Separate deletes from changes
  5. Hydrate changed records: batch SELECT per table
  6. Advance checkpoint to max seq
  7. Update client last_pull_at
  8. Fire OnPullComplete hook
  9. Return changes, deletes, new checkpoint, has_more flag
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

### slog + Optional OTel

Logging uses `log/slog` (stdlib). OTel tracing and metrics are opt-in via `TracerProvider` and `MeterProvider` in `Config`. If nil, no tracing/metrics overhead is added.

## Bucketing Model

Records are assigned to buckets that determine pull visibility:

| Bucket Type | Format | Contents |
|-------------|--------|----------|
| **User** | `user:<uuid>` | Records owned by a specific user |
| **Global** | `global` | Server-only reference data, system records with NULL owner |
| **Group** | `group:<id>` | (Phase 2) Shared group data |
| **Share** | `share:<id>` | (Phase 2) 1:N sharing via multi-bucket assignment |

Each client subscribes to a set of buckets (stored in `sync_clients.bucket_subs`). On registration, a client is automatically subscribed to `["user:<user_id>", "global"]`.

A single record can appear in multiple buckets. For example, a `SystemAndUser` record with an owner gets changelog entries in both `user:<owner_id>` and `global`.

## Checkpoint Consistency Model

- Checkpoints are monotonic `BIGSERIAL` values from `sync_changelog.seq`.
- A client's checkpoint is the highest seq it has successfully processed.
- `AdvanceCheckpoint` only moves forward (guards against `last_pull_seq < $2`).
- Pull returns `has_more: true` when the result set was truncated by `limit`, signaling the client should pull again.
- Checkpoint advancement is best-effort (logged warning on failure, does not fail the pull).
- The changelog is append-only; compaction (Phase 2) removes entries below the minimum checkpoint across all active clients.

### Failure Semantics

- If a pull succeeds but checkpoint advancement fails, the client will re-pull the same entries on the next request. This is safe because pull responses are idempotent.
- If a push transaction fails, no changelog entries are written (the WAL consumer only sees committed changes).
- If the WAL consumer crashes, it resumes from the last confirmed LSN. Changes are reprocessed idempotently.
