# Synchro Configuration Guide

## Table Registration

Register every table that participates in sync. Registration order matters for dependency resolution.

```go
registry := synchro.NewRegistry()

// User-owned table: push and pull enabled
registry.Register(&synchro.TableConfig{
    TableName:      "workouts",
    PushPolicy:     synchro.PushPolicyOwnerOnly,
    OwnerColumn:    "user_id",
    BucketByColumn: "user_id",
    BucketPrefix:   "user:",
})

// Child table: inherits ownership through parent chain
registry.Register(&synchro.TableConfig{
    TableName:      "workout_sets",
    PushPolicy:     synchro.PushPolicyOwnerOnly,
    ParentTable:    "workouts",
    ParentFKCol:    "workout_id",
    Dependencies:   []string{"workouts"},
    BucketByColumn: "workout_id",
    BucketPrefix:   "workout:",
})

// Reference table: pull-only
registry.Register(&synchro.TableConfig{
    TableName:  "exercise_types",
    PushPolicy: synchro.PushPolicyDisabled,
})

// Nullable owner + global-read behavior is explicit
registry.Register(&synchro.TableConfig{
    TableName:            "food_brands",
    PushPolicy:           synchro.PushPolicyOwnerOnly,
    OwnerColumn:          "user_id",
    BucketByColumn:       "user_id",
    BucketPrefix:         "user:",
    GlobalWhenBucketNull: true,
    AllowGlobalRead:      true,
})
```

### TableConfig Fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `TableName` | Yes | -- | Database table name |
| `PushPolicy` | No | inferred | `owner_only` or `disabled` |
| `OwnerColumn` | Conditional | `""` | Column holding user ID. Required for pushable tables without `ParentTable`. |
| `ParentTable` | No | `""` | Parent table name for child records |
| `ParentFKCol` | Conditional | `""` | FK column to parent. Required when `ParentTable` is set. |
| `SyncColumns` | No | `nil` | Column subset to include in pull responses. `nil` = all columns. |
| `Dependencies` | No | `nil` | Tables that must sync first (push ordering hint for clients) |
| `IDColumn` | No | `"id"` | Primary key column name |
| `UpdatedAtColumn` | No | `"updated_at"` | Timestamp column for conflict detection |
| `DeletedAtColumn` | No | `"deleted_at"` | Soft delete column |
| `ProtectedColumns` | No | `nil` | Additional columns clients cannot write (beyond defaults) |
| `BucketByColumn` | No | `OwnerColumn` | Fast-path bucket source column |
| `BucketPrefix` | No | `"user:"` | Prefix applied to `BucketByColumn` values |
| `GlobalWhenBucketNull` | No | `false` | Emits `global` when bucket source value is null/empty |
| `AllowGlobalRead` | No | `false` | Adds global-read RLS behavior for null-owner rows |
| `BucketFunction` | No | `""` | Optional SQL bucket resolver function override |

**Default protected columns** (always enforced): `id`, `created_at`, `updated_at`, `deleted_at`, the owner column, and the parent FK column.

### Validation Rules

`Registry.Validate()` runs automatically on `NewEngine()` and checks:

- `ParentTable` references a registered table.
- `ParentFKCol` is set when `ParentTable` is set.
- Parent chains terminate at a table with `OwnerColumn` (no orphaned chains).
- No cycles in parent chains.
- Pushable tables have either `OwnerColumn` or `ParentTable`.
- `ProtectedColumns` does not redundantly list default protected columns.

## Engine Setup

```go
engine, err := synchro.NewEngine(synchro.Config{
    DB:                 db,                // *sql.DB
    Registry:           registry,
    Hooks:              hooks,             // optional
    ConflictResolver:   nil,               // defaults to LWWResolver
    Ownership:          nil,               // defaults to JoinResolver
    MinClientVersion:   "1.2.0",           // optional semver gate
    ClockSkewTolerance: 5 * time.Second,   // optional LWW tolerance
    Logger:             slog.Default(),    // optional, defaults to slog.Default()
    Compactor: &synchro.CompactorConfig{   // optional, enables changelog compaction
        StaleThreshold: 7 * 24 * time.Hour, // deactivate clients inactive for 7 days
        BatchSize:      10000,               // rows deleted per batch
    },
})

// Manual compaction (e.g., from a cron job)
result, err := engine.RunCompaction(ctx)

// Or start background compaction on an interval
engine.StartCompaction(ctx, 1*time.Hour)
```

## Hooks

All hooks are optional. Set the ones you need.

```go
hooks := synchro.Hooks{
    // Called within the push transaction after records are applied.
    // Use for side effects like rebuilding search indexes.
    OnPushAccepted: func(ctx context.Context, tx *sql.Tx, accepted []synchro.AcceptedRecord) error {
        for _, rec := range accepted {
            if rec.TableName == "foods" {
                // Rebuild search index for the modified food
                _, err := tx.ExecContext(ctx,
                    "SELECT rebuild_food_search_index($1)", rec.ID)
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

    // Called when a client's checkpoint is behind the compaction boundary.
    OnResyncRequired: func(ctx context.Context, clientID string, checkpoint, minSeq int64) {
        slog.WarnContext(ctx, "client requires resync",
            "client_id", clientID,
            "checkpoint", checkpoint,
            "min_seq", minSeq)
    },
}
```

## Custom OwnershipResolver

The default `JoinResolver` handles most cases. Implement `OwnershipResolver` for custom bucketing logic (e.g., sharing, group ownership).

```go
type OwnershipResolver interface {
    ResolveOwner(ctx context.Context, db synchro.DB, table string, recordID string, data map[string]any) ([]string, error)
}
```

The returned slice contains bucket IDs. A record can belong to multiple buckets.

```go
type SharingResolver struct {
    inner synchro.OwnershipResolver
    db    *sql.DB
}

func (r *SharingResolver) ResolveOwner(ctx context.Context, db synchro.DB, table string, recordID string, data map[string]any) ([]string, error) {
    // Get the standard owner buckets
    buckets, err := r.inner.ResolveOwner(ctx, db, table, recordID, data)
    if err != nil {
        return nil, err
    }

    // Check if record is shared with other users
    if table == "workouts" {
        rows, err := r.db.QueryContext(ctx,
            "SELECT shared_with_user_id FROM workout_shares WHERE workout_id = $1",
            recordID)
        if err != nil {
            return nil, err
        }
        defer rows.Close()

        for rows.Next() {
            var sharedUserID string
            if err := rows.Scan(&sharedUserID); err != nil {
                return nil, err
            }
            buckets = append(buckets, fmt.Sprintf("user:%s", sharedUserID))
        }
    }

    return buckets, nil
}
```

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

## Telemetry Integration

Synchro uses `Config.Logger` for structured logs. For OpenTelemetry correlation, configure your logger pipeline in the host application (for example via `otelslog`) and pass that logger into `Config.Logger`.

### Structured Logging

Pass a custom `*slog.Logger` to `Config.Logger`. If nil, `slog.Default()` is used.

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelInfo,
}))

engine, _ := synchro.NewEngine(synchro.Config{
    // ...
    Logger: logger,
})
```

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
mux.HandleFunc("POST /sync/resync", h.ServeResync)
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
g.POST("/resync", echo.WrapHandler(http.HandlerFunc(h.ServeResync)))
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
    r.Post("/resync", h.ServeResync)
    r.Get("/tables", h.ServeTableMeta)
    r.Get("/schema", h.ServeSchema)
})
```

### User ID Injection

Synchro handlers read the user ID from context via `handler.UserIDFromContext(ctx)`. You must inject it.

**Option A: Use the built-in header middleware** (for development):

```go
wrapped := handler.UserIDMiddleware("X-User-ID", mux)
```

**Option B: Inject from your own auth middleware** (production):

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

## WAL Consumer Setup

The WAL consumer runs as a long-lived goroutine alongside your server.

```go
import "github.com/trainstar/synchro/wal"

consumer := wal.NewConsumer(wal.ConsumerConfig{
    ConnString:      "postgres://user:pass@host:5432/db?replication=database",
    SlotName:        "synchro_slot",
    PublicationName: "synchro_pub",
    Registry:        registry,
    Assigner:        synchro.NewJoinResolver(registry), // implements BucketAssigner
    ChangelogDB:     db,                                // *sql.DB
    Logger:          logger,
    StandbyTimeout:  10 * time.Second,                  // default
})

// Run in a goroutine — blocks until context is cancelled
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
    workouts, workout_sets, foods, food_brands, exercise_types;

-- Ensure wal_level = logical (requires restart)
ALTER SYSTEM SET wal_level = 'logical';
```

The consumer creates the replication slot automatically on first start.

## Migration Setup

Synchro provides migration SQL but does not run migrations itself. Integrate with your migration system.

```go
import "github.com/trainstar/synchro/migrate"

// Get infrastructure table DDL
stmts := migrate.Migrations()

// Get RLS policies from your registry
rlsStmts := synchro.GenerateRLSPolicies(registry)

// Run through your migration system
for _, stmt := range append(stmts, rlsStmts...) {
    _, err := db.Exec(stmt)
    if err != nil {
        log.Fatal(err)
    }
}
```

### Infrastructure Tables Created

- `sync_changelog` -- Append-only changelog with `BIGSERIAL` seq, indexed by `(bucket_id, seq)`.
- `sync_clients` -- Client registration, bucket subscriptions, checkpoint tracking.

### RLS Policies Generated

`GenerateRLSPolicies(registry)` produces:

- `PushPolicyDisabled` tables: read-only behavior (no write policies generated).
- Tables with `OwnerColumn`: `SELECT`, `INSERT`, `UPDATE`, `DELETE` policies scoped to `owner_col::text = current_setting('app.user_id', true)`.
- Tables with `AllowGlobalRead=true`: `SELECT` additionally allows `owner_col IS NULL`.
- Child tables: `EXISTS` subquery through parent chain to verify ownership.
