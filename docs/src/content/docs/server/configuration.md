---
title: "Configuration"
description: "Full reference for Config, Table, composable helpers, hooks, middleware, and advanced options."
---

:::tip[What does NOT change]
Your application routes, your ORM, your existing queries, your auth middleware: all stay the same. Synchro mounts alongside your existing server.
:::

---

## Engine Setup

The engine is configured with a single `Config` struct. Table metadata (primary keys, foreign keys, column nullability, timestamps, parent/child relationships, dependency ordering) is auto-introspected from `pg_catalog` at startup.

```go
engine := synchro.NewEngine(ctx, &synchro.Config{
    DB: db, // *sql.DB
    Tables: []synchro.Table{
        {Name: "categories"},
        {Name: "tasks"},
        {Name: "comments"},
        {Name: "tags", Exclude: []string{"search_vector"}},
    },
    AuthorizeWrite: synchro.Chain(
        synchro.ReadOnly("categories"),
        synchro.StampColumn("user_id"),
        synchro.VerifyOwner("user_id"),
    ),
    BucketFunc:         synchro.UserBucket("user_id"),
    Hooks:              hooks,             // optional
    ConflictResolver:   nil,               // defaults to LWWResolver
    MinClientVersion:   "1.2.0",           // optional semver gate
    ClockSkewTolerance: 5 * time.Second,   // optional LWW tolerance
    UpdatedAtColumn:    "updated_at",      // optional, default "updated_at"
    DeletedAtColumn:    "deleted_at",      // optional, default "deleted_at"
    Logger:             slog.Default(),    // optional, defaults to slog.Default()
    Compactor: &synchro.CompactorConfig{   // optional, enables changelog compaction
        StaleThreshold: 7 * 24 * time.Hour, // deactivate clients inactive for 7 days
        BatchSize:      10000,               // rows deleted per batch
    },
})
```

### Config Fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `DB` | Yes | - | `*sql.DB` connection pool |
| `Tables` | Yes | - | Slice of `Table` structs declaring synced tables |
| `AuthorizeWrite` | Yes | - | Composable write authorization chain (see below) |
| `BucketFunc` | Yes | - | Bucket assignment function for data routing (see below) |
| `Hooks` | No | `Hooks{}` | Lifecycle callbacks |
| `ConflictResolver` | No | `LWWResolver` | Conflict resolution strategy |
| `MinClientVersion` | No | `""` | Semver gate: reject clients below this version |
| `ClockSkewTolerance` | No | `0` | LWW tolerance for clock drift |
| `UpdatedAtColumn` | No | `"updated_at"` | Convention name for update timestamp column |
| `DeletedAtColumn` | No | `"deleted_at"` | Convention name for soft-delete timestamp column |
| `Logger` | No | `slog.Default()` | Structured logger |
| `Compactor` | No | `nil` | Enables changelog compaction when set |

---

## Table Declaration

Tables are declared as a flat slice. You only provide the table name and optional column exclusions. Everything else (PKs, FKs, column types, nullability, parent/child relationships, dependency ordering) is auto-introspected from `pg_catalog`.

```go
Tables: []synchro.Table{
    {Name: "categories"},
    {Name: "tasks"},
    {Name: "comments"},
    {Name: "tags", Exclude: []string{"search_vector", "embedding"}},
}
```

### Table Fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `Name` | Yes | - | Database table name |
| `Exclude` | No | `nil` | Columns to exclude from sync (e.g., `tsvector`, `halfvec`, `ltree` types that have no SQLite equivalent) |

### What Introspection Detects

At startup, `NewEngine` queries `pg_catalog` for each declared table:

- **Primary key** column (replaces the old `IDColumn` field)
- **Foreign keys** to other registered tables (replaces `ParentTable`, `ParentFKCol`, `Dependencies`)
- **Column nullability** and types
- **Timestamp columns** (`updated_at`, `deleted_at`, `created_at`), detected by convention name

Parent/child relationships and dependency ordering are derived from the FK graph automatically. No manual configuration required.

### Validation

`NewEngine` validates the introspected graph at startup:

- All FK targets reference registered tables.
- No cycles in the FK dependency graph.
- All declared table names exist in the database.

---

## AuthorizeWrite

`AuthorizeWrite` is a composable chain of helpers that handles push authorization. It replaces the old `PushPolicy`, `OwnerColumn`, and `ProtectedColumns` fields.

```go
AuthorizeWrite: synchro.Chain(
    synchro.ReadOnly("categories", "units"),
    synchro.StampColumn("user_id"),
    synchro.VerifyOwner("user_id"),
),
```

### Composable Helpers

| Helper | Purpose |
|--------|---------|
| `Chain(...)` | Composes multiple `AuthorizeWrite` functions into a single chain. Each runs in order; first rejection stops the chain. |
| `ReadOnly(tables...)` | Rejects all writes (create/update/delete) to the listed tables. Use for reference data. |
| `StampColumn(col)` | On INSERT: stamps `col` with the authenticated user ID (server-derived from JWT, never client-provided). On UPDATE: strips `col` from the payload (owner is immutable). |
| `VerifyOwner(col)` | On UPDATE/DELETE: fetches the existing record and verifies the authenticated user matches `col`. Rejects the operation if they don't match. |

The `protectedColumns()` helper (called internally) strips server-computed columns (`id`, `created_at`, `updated_at`, `deleted_at`) from client payloads.

### Migration from Old Fields

| Old Field | Replacement |
|-----------|-------------|
| `PushPolicy: PushPolicyDisabled` | `ReadOnly("table_name")` |
| `PushPolicy: PushPolicyOwnerOnly` | `StampColumn("user_id")` + `VerifyOwner("user_id")` |
| `OwnerColumn: "user_id"` | `StampColumn("user_id")` + `VerifyOwner("user_id")` |
| `ProtectedColumns: [...]` | Server-computed columns are handled automatically; custom protected columns can be stripped in a custom `AuthorizeWrite` function |

---

## BucketFunc

`BucketFunc` determines which buckets a record belongs to, controlling who gets what data. It is a plain function type:

```go
type BucketFunc func(table, op string, data map[string]any) []string
```

It receives the table name, operation type (`insert`, `update`, `delete`), and the record data map. It returns a slice of bucket IDs the record belongs to. Returning nil signals that the function cannot resolve buckets for this record (allowing FK chain fallback via the `JoinResolver`).

```go
BucketFunc: synchro.UserBucket("user_id"),
```

### Built-in Bucket Functions

| Function | Behavior | Description |
|----------|----------|-------------|
| `UserBucket(col)` | Reads `col` from data map. Non-nil: `["user:<value>"]`. Nil: `["global"]`. Missing: `nil` (FK fallback). | Covers the common case of user-owned + reference data. |

For custom bucketing logic (e.g., group ownership, sharing), write a function matching the `BucketFunc` signature.

### Migration from Old Fields

| Old Fields | Replacement |
|-----------|-------------|
| `BucketByColumn: "user_id"` + `BucketPrefix: "user:"` | `UserBucket("user_id")` |
| `AllowGlobalRead: true` + `GlobalWhenBucketNull: true` | Built into `UserBucket` (null = global) |
| `BucketFunction: "my_sql_func"` | Write a custom `BucketFunc` function |

---

## Schema Introspection

At startup, `NewEngine` introspects each registered table via `pg_catalog` to detect which timestamp columns exist. No schema modifications are required. Synchro adapts to your tables as they are.

| Column | When present | When absent |
|--------|-------------|-------------|
| `updated_at` | LWW conflict resolution: concurrent edits detected and resolved by timestamp. Column is protected from client writes. | Last-push-wins: every update applied unconditionally. Column not referenced. |
| `deleted_at` | Soft deletes: `UPDATE SET deleted_at = now()`. Row preserved for resurrection. Column protected from client writes. | Hard deletes: `DELETE FROM`. Row permanently removed, WAL captures the event. |
| `created_at` | Protected from client writes (server-managed). | No effect on sync behavior. |

`UpdatedAtColumn` and `DeletedAtColumn` on `Config` set the **convention name** to look for. Each table is checked independently, so a single engine can contain tables with and without these columns.

```go
// Use custom column names across all tables
engine := synchro.NewEngine(ctx, &synchro.Config{
    DB:              db,
    Tables:          tables,
    AuthorizeWrite:  authChain,
    BucketFunc:      bucketFunc,
    UpdatedAtColumn: "modified_at",
    DeletedAtColumn: "removed_at",
})
```

:::tip[When to add timestamp columns]
For tables where multiple clients may edit the same record concurrently, `updated_at` is strongly recommended. Without it there is no conflict detection. For tables where you need to preserve deletion history or support undo, `deleted_at` is recommended. Reference data tables that are read-only work fine without either.
:::

```go
// Manual compaction (e.g., from a cron job)
result, err := engine.RunCompaction(ctx)

// Or start background compaction on an interval
engine.StartCompaction(ctx, 1*time.Hour)
```

---

## Hooks

All hooks are optional. Set the ones you need.

```go
hooks := synchro.Hooks{
    // Called within the push transaction after records are applied.
    // Use for side effects like rebuilding search indexes.
    OnPushAccepted: func(ctx context.Context, tx *sql.Tx, accepted []synchro.AcceptedRecord) error {
        for _, rec := range accepted {
            if rec.TableName == "tasks" {
                // Rebuild search index for the modified task
                _, err := tx.ExecContext(ctx,
                    "SELECT rebuild_task_search_index($1)", rec.ID)
                if err != nil {
                    return err
                }
            }
        }
        return nil
    },

    // Informational callback when a conflict is detected.
    // Cannot change the resolution.
    OnConflict: func(ctx context.Context, conflict synchro.Conflict, resolution synchro.Resolution) {
        slog.InfoContext(ctx, "sync conflict resolved",
            "table", conflict.Table,
            "record_id", conflict.RecordID,
            "winner", resolution.Winner)
    },

    // Called after a successful pull.
    OnPullComplete: func(ctx context.Context, clientID string, checkpoint int64, count int) {
        slog.InfoContext(ctx, "pull complete",
            "client_id", clientID,
            "checkpoint", checkpoint,
            "count", count)
    },

    // Called when a client version is below minimum.
    OnSchemaIncompatible: func(ctx context.Context, clientID, clientVer, minVer string) {
        slog.WarnContext(ctx, "client version too old",
            "client_id", clientID,
            "client_version", clientVer,
            "min_version", minVer)
    },

    // Called when a client hasn't synced recently.
    // Return true to allow, false to reject.
    OnStaleClient: func(ctx context.Context, clientID string, lastSync time.Time) bool {
        return time.Since(lastSync) < 30*24*time.Hour // allow up to 30 days
    },

    // Called after a compaction run completes.
    OnCompaction: func(ctx context.Context, result synchro.CompactResult) {
        slog.InfoContext(ctx, "compaction complete",
            "deactivated", result.DeactivatedClients,
            "safe_seq", result.SafeSeq,
            "deleted", result.DeletedEntries)
    },

    // Called when a client must rebuild from a full snapshot.
    // DEPRECATED: Use OnBucketRebuild instead. Per-bucket rebuilds replace
    // full snapshots in the new architecture. This hook is still called for
    // legacy clients using the global checkpoint path.
    OnSnapshotRequired: func(ctx context.Context, clientID string, checkpoint, minSeq int64, reason string) {
        slog.WarnContext(ctx, "client requires snapshot rebuild",
            "client_id", clientID,
            "checkpoint", checkpoint,
            "min_seq", minSeq,
            "reason", reason)
    },

    // Called when a per-bucket rebuild is triggered for a client.
    // Fires once per stale bucket detected during pull.
    OnBucketRebuild: func(ctx context.Context, clientID string, bucketID string, checkpoint, minSeq int64) {
        slog.WarnContext(ctx, "bucket rebuild triggered",
            "client_id", clientID,
            "bucket_id", bucketID,
            "checkpoint", checkpoint,
            "min_seq", minSeq)
    },
}
```

---

## Custom BucketFunc

The built-in `UserBucket` handles the common case. For custom bucketing logic (e.g., sharing, group ownership), write a function matching the `BucketFunc` signature.

A `BucketFunc` receives the table name, operation, and record data, then returns bucket IDs. Return nil when the function cannot determine buckets (e.g., child tables without the owner column), which allows the `JoinResolver` to fall back to FK chain resolution.

### Example: Sharing BucketFunc

```go
func SharingBucket(db *sql.DB) synchro.BucketFunc {
    base := synchro.UserBucket("user_id")

    return func(table, op string, data map[string]any) []string {
        // Get the standard owner buckets
        buckets := base(table, op, data)
        if buckets == nil {
            return nil // let JoinResolver handle FK chain
        }

        // Add buckets for shared users
        if table == "tasks" {
            recordID, _ := data["id"].(string)
            rows, err := db.Query(
                "SELECT shared_with_user_id FROM task_shares WHERE task_id = $1",
                recordID)
            if err != nil {
                return buckets // fall back to standard buckets on error
            }
            defer rows.Close()

            for rows.Next() {
                var sharedUserID string
                if err := rows.Scan(&sharedUserID); err != nil {
                    continue
                }
                buckets = append(buckets, fmt.Sprintf("user:%s", sharedUserID))
            }
        }

        return buckets
    }
}
```

---

## Custom ConflictResolver

```go
type ConflictResolver interface {
    Resolve(ctx context.Context, conflict synchro.Conflict) (synchro.Resolution, error)
}
```

The `Conflict` struct provides:

| Field | Type | Description |
|-------|------|-------------|
| `Table` | `string` | Table name |
| `RecordID` | `string` | Record primary key |
| `ClientID` | `string` | Pushing client |
| `UserID` | `string` | Authenticated user |
| `ClientData` | `json.RawMessage` | Client's version of the record |
| `ServerData` | `json.RawMessage` | Server's current version |
| `ClientTime` | `time.Time` | Client's `client_updated_at` |
| `ServerTime` | `time.Time` | Server's `updated_at` |
| `BaseVersion` | `*time.Time` | Client's `base_updated_at` (optional, for optimistic concurrency) |

Return `Resolution{Winner: "client"}` to accept the push, or `Resolution{Winner: "server"}` to reject it.

### Example: Per-Table Resolver

```go
type PerTableResolver struct {
    resolvers map[string]synchro.ConflictResolver
    fallback  synchro.ConflictResolver
}

func (r *PerTableResolver) Resolve(ctx context.Context, c synchro.Conflict) (synchro.Resolution, error) {
    if resolver, ok := r.resolvers[c.Table]; ok {
        return resolver.Resolve(ctx, c)
    }
    return r.fallback.Resolve(ctx, c)
}
```

---

## Telemetry Integration

Synchro uses `Config.Logger` for structured logs. For OpenTelemetry correlation, configure your logger pipeline in the host application (for example via `otelslog`) and pass that logger into `Config.Logger`.

### Structured Logging

Pass a custom `*slog.Logger` to `Config.Logger`. If nil, `slog.Default()` is used.

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelInfo,
}))

engine := synchro.NewEngine(ctx, &synchro.Config{
    // ...
    Logger: logger,
})
```

---

## HTTP Handler Wiring

Synchro provides `net/http` handlers. Wire them into your router.

### Standard Library (net/http)

```go
import "github.com/trainstar/synchro/handler"

h := handler.New(engine)

mux := http.NewServeMux()
mux.HandleFunc("POST /sync/register", h.ServeRegister)
mux.HandleFunc("POST /sync/pull", h.ServePull)
mux.HandleFunc("POST /sync/push", h.ServePush)
mux.HandleFunc("POST /sync/snapshot", h.ServeSnapshot)
mux.HandleFunc("POST /sync/rebuild", h.ServeRebuild)
mux.HandleFunc("GET /sync/tables", h.ServeTableMeta)
mux.HandleFunc("GET /sync/schema", h.ServeSchema)
```

### Echo

```go
import "github.com/trainstar/synchro/handler"

h := handler.New(engine)

g := e.Group("/sync")
g.POST("/register", echo.WrapHandler(http.HandlerFunc(h.ServeRegister)))
g.POST("/pull", echo.WrapHandler(http.HandlerFunc(h.ServePull)))
g.POST("/push", echo.WrapHandler(http.HandlerFunc(h.ServePush)))
g.POST("/snapshot", echo.WrapHandler(http.HandlerFunc(h.ServeSnapshot)))
g.POST("/rebuild", echo.WrapHandler(http.HandlerFunc(h.ServeRebuild)))
g.GET("/tables", echo.WrapHandler(http.HandlerFunc(h.ServeTableMeta)))
g.GET("/schema", echo.WrapHandler(http.HandlerFunc(h.ServeSchema)))
```

Or wrap directly in your Echo handler to use Echo's context:

```go
g.POST("/push", func(c echo.Context) error {
    userID := c.Get("user_id").(string)
    ctx := handler.WithUserID(c.Request().Context(), userID)
    h.ServePush(c.Response(), c.Request().WithContext(ctx))
    return nil
})
```

### Chi

```go
import "github.com/trainstar/synchro/handler"

h := handler.New(engine)

r := chi.NewRouter()
r.Route("/sync", func(r chi.Router) {
    r.Post("/register", h.ServeRegister)
    r.Post("/pull", h.ServePull)
    r.Post("/push", h.ServePush)
    r.Post("/snapshot", h.ServeSnapshot)
    r.Post("/rebuild", h.ServeRebuild)
    r.Get("/tables", h.ServeTableMeta)
    r.Get("/schema", h.ServeSchema)
})
```

### User ID Injection

Synchro handlers read the user ID from context via `handler.UserIDFromContext(ctx)`. You must inject it.

### Built-in header middleware (development)

```go
wrapped := handler.UserIDMiddleware("X-User-ID", mux)
```

### Custom auth middleware (production)

```go
func authMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        userID := extractUserIDFromJWT(r) // your auth logic
        ctx := handler.WithUserID(r.Context(), userID)
        next.ServeHTTP(w, r.WithContext(ctx))
    })
}
```

### Version Check Middleware

Optional middleware that rejects clients below a minimum version:

```go
wrapped := handler.VersionCheckMiddleware("X-App-Version", "1.2.0", mux)
```

### Retry-After

Synchro automatically returns `503 Service Unavailable` with a `Retry-After` header when it detects transient errors (closed DB pool, connection timeout, network failure). The default retry interval is 5 seconds:

```go
h := handler.New(engine, handler.WithDefaultRetryAfter(10)) // 10 seconds
```

For application-level backpressure (rate limiting, maintenance mode), use the middleware. The consuming app provides the policy:

### Rate limiting (429)

```go
wrapped := handler.RetryAfterMiddleware(func(r *http.Request) (bool, int, int) {
    if rateLimiter.IsExceeded(r) {
        return true, 429, 60
    }
    return false, 0, 0
}, mux)
```

### Maintenance mode (503)

```go
wrapped := handler.RetryAfterMiddleware(func(r *http.Request) (bool, int, int) {
    if maintenanceMode {
        return true, 503, 300
    }
    return false, 0, 0
}, mux)
```

Both the HTTP `Retry-After` header and a `retry_after` field in the JSON body are set, so clients can use either.

---

## WAL Consumer Setup

The WAL consumer runs as a long-lived goroutine alongside your server.

```go
import "github.com/trainstar/synchro/wal"

consumer := wal.NewConsumer(wal.ConsumerConfig{
    ConnString:      "postgres://user:pass@host:5432/db?replication=database",
    SlotName:        "synchro_slot",
    PublicationName: "synchro_pub",
    Engine:          engine,                            // provides registry + BucketFunc
    ChangelogDB:     db,                                // *sql.DB
    Logger:          logger,
    StandbyTimeout:  10 * time.Second,                  // default
})

// Run in a goroutine. Blocks until context is cancelled.
go func() {
    if err := consumer.Start(ctx); err != nil && ctx.Err() == nil {
        log.Fatal("WAL consumer failed", "err", err)
    }
}()
```

### PostgreSQL Prerequisites

```sql
-- Create the publication for tables you want to sync
CREATE PUBLICATION synchro_pub FOR TABLE
    tasks, comments, tags, categories;

-- Ensure wal_level = logical (requires restart)
ALTER SYSTEM SET wal_level = 'logical';
```

:::note
The consumer creates the replication slot automatically on first start.
:::

---

## Migration Setup

Synchro provides migration SQL but does not run migrations itself. Integrate with your migration system.

```go
import "github.com/trainstar/synchro/migrate"

// Get infrastructure table DDL
stmts := migrate.Migrations()

// Get RLS policies (optional defense-in-depth)
rlsStmts := synchro.GenerateRLSPolicies("user_id") // owner column name

// Run through your migration system
for _, stmt := range append(stmts, rlsStmts...) {
    _, err := db.Exec(stmt)
    if err != nil {
        log.Fatal(err)
    }
}
```

### Infrastructure Tables Created

| Table | Purpose |
|-------|---------|
| `sync_changelog` | Append-only changelog with `BIGSERIAL` seq, indexed by `(bucket_id, seq)` |
| `sync_clients` | Client registration, bucket subscriptions, legacy global checkpoint tracking |
| `sync_client_checkpoints` | Per-bucket checkpoint tracking with one row per `(client_id, bucket_id)`. Stores the highest seq each client has processed for each bucket independently. |
| `sync_wal_position` | WAL consumer LSN tracking for crash recovery |
| `sync_bucket_edges` | Membership index for bucket delta assignment and rebuild pagination |
| `sync_rule_failures` | Resolver failures for operational debugging and replay tooling |
| `sync_schema_manifest` | Schema contract version/hash history |

### RLS Policies Generated (Optional)

`GenerateRLSPolicies(ownerColumn)` takes the owner column name and generates standard RLS policies for all registered tables. RLS is optional defense-in-depth. The application works correctly without it, since `AuthorizeWrite` handles push authorization and `BucketFunc` handles pull/snapshot scoping.

Generated policies:

- **Read-only tables** (listed in `ReadOnly(...)`): no write policies generated.
- **Tables with the owner column:** `SELECT`, `INSERT`, `UPDATE`, `DELETE` policies scoped to `owner_col::text = current_setting('app.user_id', true)`.
- **Nullable owner columns:** `SELECT` additionally allows `owner_col IS NULL` (global/reference rows).
- **Child tables:** `EXISTS` subquery through the auto-detected FK chain to verify ownership.

---

## Debug Endpoint

The sync handler mounts a debug endpoint at `GET /sync/debug?client_id=xxx`. It requires authentication and returns a `ClientDebugResponse` containing:

| Field | Type | Description |
|-------|------|-------------|
| `client` | `ClientDebugInfo` | Client registration state |
| `buckets` | `[]ServerBucketDebug` | Per-bucket checkpoint, member count, checksum |
| `changelog_stats` | `ChangelogDebugStats` | Min/max seq, total entries |
| `server_time` | `time.Time` | Current server timestamp |

This endpoint is read-only and safe to call at any time. It does not modify any state.

## Checksum Protocol

The `Record` type includes an optional `checksum` field (CRC32, IEEE polynomial). The pull response includes `bucket_checksums` on the final page. The rebuild response includes `bucket_checksum` on the final page.

Checksums are computed by `ComputeRecordChecksum(jsonStr)` in `checksum.go`: unmarshal into `map[string]any`, re-marshal with `json.Marshal` (sorted keys), `crc32.ChecksumIEEE`.

The `sync_bucket_edges` table has a `checksum BIGINT` column populated by the WAL consumer. Bucket aggregate checksums are computed via `BIT_XOR(checksum::int)` in PostgreSQL.

Client SDKs store server-provided per-record checksums in `_synchro_bucket_members.checksum` and verify the bucket aggregate after each pull cycle. Mismatches trigger automatic bucket rebuilds.
