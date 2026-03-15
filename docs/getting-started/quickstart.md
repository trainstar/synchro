# Quick Start

Zero to working sync in 15 minutes. This guide walks you through adding Synchro to an existing Go + PostgreSQL application and connecting a client SDK.

---

## Prerequisites

| Requirement | Minimum Version |
|---|---|
| Go | 1.22+ |
| PostgreSQL | 14+ with `wal_level=logical` |

---

## 1. Install

```bash
go get github.com/trainstar/synchro
```

---

## 2. Prepare Your Schema

Synchro works with your existing tables. The only requirement is a nullable `deleted_at` column for soft-delete tracking.

!!! info "You are describing your existing table, not changing it"
    Synchro does not generate tables, impose naming conventions, or require a migration framework. You own your schema. The only addition is the `deleted_at` column if you do not already have one.

```sql
-- Your existing table
CREATE TABLE workouts (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id    UUID NOT NULL REFERENCES users(id),
    title      TEXT NOT NULL,
    duration   INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ           -- (1)!
);
```

1. This is the only column Synchro requires. If your table already has a soft-delete column, point `DeletedAtColumn` at it.

---

## 3. Register Tables

Tell Synchro which tables to sync and how ownership works.

```go
registry := synchro.NewRegistry()

registry.Register(&synchro.TableConfig{
    TableName:   "workouts",
    OwnerColumn: "user_id",
})
```

That is the minimal registration. `PushPolicy`, `BucketByColumn`, and `BucketPrefix` are inferred from the `OwnerColumn` automatically. See [Configuration](../server/configuration.md) for the full set of options.

---

## 4. Create the Engine

```go
engine, err := synchro.NewEngine(synchro.Config{
    DB:       db, // *sql.DB
    Registry: registry,
})
```

The engine validates the registry graph at startup and returns an error if any table references are broken.

---

## 5. Run Migrations

Synchro uses a small set of sidecar tables (`sync_changelog`, `sync_clients`, etc.) alongside your application tables. Run the infrastructure DDL through your migration system, then apply RLS policies.

```go
import "github.com/trainstar/synchro/migrate"

// Infrastructure tables
for _, stmt := range migrate.Migrations() {
    if _, err := db.Exec(stmt); err != nil {
        log.Fatalf("migration failed: %v", err)
    }
}

// Row-level security policies
for _, stmt := range synchro.GenerateRLSPolicies(registry) {
    if _, err := db.Exec(stmt); err != nil {
        log.Fatalf("RLS policy failed: %v", err)
    }
}
```

!!! warning "RLS requires a non-superuser connection for push operations"
    PostgreSQL superusers bypass RLS. Your application's runtime database connection should use a non-superuser role for push authorization to take effect.

---

## 6. Wire HTTP Handlers

Synchro provides stdlib `net/http` handlers. Mount them on any router.

```go
import "github.com/trainstar/synchro/handler"

h := handler.New(engine)

mux := http.NewServeMux()
mux.HandleFunc("POST /sync/register", h.ServeRegister)
mux.HandleFunc("POST /sync/pull",     h.ServePull)
mux.HandleFunc("POST /sync/push",     h.ServePush)
mux.HandleFunc("POST /sync/snapshot", h.ServeSnapshot)
mux.HandleFunc("GET /sync/tables",    h.ServeTableMeta)
mux.HandleFunc("GET /sync/schema",    h.ServeSchema)
```

Every POST endpoint requires a user identity in the request context. Use the built-in middleware or inject your own:

```go
// Option A: Header-based (development / API gateway)
wrapped := handler.UserIDMiddleware("X-User-ID", mux)

// Option B: JWT-based (production)
wrapped := handler.JWTAuthMiddleware(handler.JWTAuthConfig{
    JWKSURL:   "https://auth.example.com/.well-known/jwks.json",
    UserClaim: "sub",
}, mux)

http.ListenAndServe(":8080", wrapped)
```

---

## 7. Start the WAL Consumer

The WAL consumer captures changes from PostgreSQL logical replication and writes them to the changelog.

```go
import "github.com/trainstar/synchro/wal"

consumer := wal.NewConsumer(wal.ConsumerConfig{
    ConnString:      "postgres://user:pass@localhost:5432/mydb?replication=database",
    SlotName:        "synchro_slot",
    PublicationName: "synchro_pub",
    Registry:        registry,
    Assigner:        synchro.NewJoinResolverWithDB(registry, db),
    ChangelogDB:     db,
})

go consumer.Start(ctx) // blocks until ctx is cancelled
```

!!! note "The connection string must include `replication=database`"
    This enables the PostgreSQL replication protocol. Use a separate connection string from your application pool.

---

## 8. PostgreSQL Setup

Enable logical replication and create a publication for your synced tables.

```sql
-- Enable logical replication (requires restart)
ALTER SYSTEM SET wal_level = 'logical';
-- Then restart PostgreSQL

-- Create the publication
CREATE PUBLICATION synchro_pub FOR TABLE workouts;
```

To add more tables later:

```sql
ALTER PUBLICATION synchro_pub ADD TABLE exercises, sets;
```

---

## 9. Connect a Client

=== "Swift"

    ```swift
    let client = try SynchroClient(config: SynchroConfig(
        dbPath: "synchro.db",
        serverURL: URL(string: "https://api.example.com")!,
        authProvider: { await getToken() },
        clientID: "device-123",
        appVersion: "1.0.0"
    ))
    try await client.start()

    let workouts = try client.query(
        "SELECT * FROM workouts ORDER BY created_at DESC"
    )
    ```

=== "Kotlin"

    ```kotlin
    val client = SynchroClient(SynchroConfig(
        dbPath = "synchro.db",
        serverURL = "https://api.example.com",
        authProvider = { getToken() },
        clientID = "device-123",
        appVersion = "1.0.0"
    ), context)
    client.start()

    val workouts = client.query(
        "SELECT * FROM workouts ORDER BY created_at DESC"
    )
    ```

=== "React Native"

    ```typescript
    import { SynchroClient } from '@trainstar/synchro-react-native';

    const client = new SynchroClient({
        dbPath: 'synchro.db',
        serverURL: 'https://api.example.com',
        authProvider: async () => await getToken(),
        clientID: 'device-123',
        appVersion: '1.0.0',
    });
    await client.start();

    const workouts = await client.query(
        'SELECT * FROM workouts ORDER BY created_at DESC'
    );
    ```

The client SDK handles registration, schema sync, snapshot bootstrap, and the ongoing push/pull loop automatically. Write to your local SQLite tables with normal SQL -- CDC triggers capture changes and queue them for push.

---

## Next Steps

- [Core Concepts](concepts.md) -- Understand how WAL capture, buckets, checkpoints, and conflict resolution work together.
- [Configuration](../server/configuration.md) -- Full reference for `TableConfig`, `Config`, hooks, middleware, and advanced options.
- [API Reference](../protocol/api-reference.md) -- Wire protocol specification for building custom clients.
