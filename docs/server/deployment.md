# Deployment Guide

## PostgreSQL Requirements

Synchro uses PostgreSQL logical replication for change detection. This requires configuration changes that persist across restarts.

### WAL Configuration

Set `wal_level` to `logical` (requires a PostgreSQL restart):

```sql
ALTER SYSTEM SET wal_level = 'logical';
```

Recommended settings:

| Parameter | Minimum | Recommended | Notes |
|-----------|---------|-------------|-------|
| `wal_level` | `logical` | `logical` | Requires restart |
| `max_replication_slots` | 1 | 4+ | One per WAL consumer instance |
| `max_wal_senders` | 1 | 4+ | One per replication connection |

!!! warning "Restart required"
    Changing `wal_level` requires a full PostgreSQL restart. A reload is not sufficient. Plan this during a maintenance window.

### Publication

Create a publication for all tables that participate in sync:

```sql
CREATE PUBLICATION synchro_pub FOR TABLE tasks, comments, categories;
```

The `synchrod` reference server creates the publication automatically on startup if it does not exist. When embedding the library, you must create it manually or as part of your migration pipeline.

!!! tip "Adding tables later"
    To add a table to an existing publication:
    ```sql
    ALTER PUBLICATION synchro_pub ADD TABLE new_table;
    ```

### Replication Slot

The WAL consumer creates its replication slot automatically on first connection. No manual slot creation is needed. The default slot name is `synchro_slot`.

---

## synchrod Reference Server

Synchro ships `synchrod`, a standalone sync server binary for development and production use.

### Build and Run

```bash
# Build
go build -o bin/synchrod ./cmd/synchrod

# Run
DATABASE_URL="postgres://user:pass@host:5432/db?sslmode=disable" \
REPLICATION_URL="postgres://user:pass@host:5432/db?replication=database&sslmode=disable" \
JWT_SECRET="your-secret" \
bin/synchrod
```

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | Yes | | PostgreSQL connection string |
| `REPLICATION_URL` | Yes | | Replication connection string (append `replication=database`) |
| `JWT_SECRET` | Yes* | | JWT HMAC signing secret |
| `JWKS_URL` | Yes* | | JWKS endpoint URL for RS256/ES256 verification |
| `JWT_USER_CLAIM` | No | `sub` | JWT claim containing the user ID |
| `LISTEN_ADDR` | No | `:8080` | HTTP listen address |
| `SLOT_NAME` | No | `synchro_slot` | Logical replication slot name |
| `PUBLICATION_NAME` | No | `synchro_pub` | PostgreSQL publication name |
| `MIN_CLIENT_VERSION` | No | | Minimum client SDK version (semver) |
| `LOG_LEVEL` | No | `info` | Log level: `debug`, `info`, `warn`, `error` |

*One of `JWT_SECRET` or `JWKS_URL` is required for authentication. If neither is set, `synchrod` falls back to the `X-User-ID` header (development only).

!!! danger "Do not use X-User-ID in production"
    The `X-User-ID` header fallback has no authentication. It exists for local development only. Always configure `JWT_SECRET` or `JWKS_URL` in production.

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/sync/register` | Register or re-register a client |
| `POST` | `/sync/pull` | Pull changes from server |
| `POST` | `/sync/push` | Push changes to server |
| `POST` | `/sync/snapshot` | Full snapshot (initial sync or recovery) |
| `GET` | `/sync/schema` | Schema definition with column types and defaults |
| `GET` | `/sync/tables` | Table metadata (sync config, dependencies) |
| `GET` | `/healthz` | Health check |

---

## Deployment Modes

### Mode A: Embedded

Sync runs inside your application process. Zero extra infrastructure.

```mermaid
graph TB
    subgraph "Your Application Process"
        API[App API]
        Sync[Synchro Engine]
        WAL[WAL Consumer]
    end
    DB[(PostgreSQL)]
    API --> DB
    Sync --> DB
    WAL --> DB
```

**Use when:** early stage, low-to-medium write volume, simplicity is the priority.

=== "Go"

    ```go
    // In your main application
    engine, err := synchro.NewEngine(synchro.Config{
        DB:       db,
        Registry: registry,
        Logger:   logger,
    })

    // Start WAL consumer in-process
    consumer := wal.NewConsumer(wal.ConsumerConfig{
        ConnString:      replicationURL,
        SlotName:        "synchro_slot",
        PublicationName: "synchro_pub",
        Registry:        registry,
        Assigner:        synchro.NewJoinResolverWithDB(registry, db),
        ChangelogDB:     db,
        Logger:          logger,
    })
    go consumer.Start(ctx)

    // Mount sync routes alongside your API
    h := handler.New(engine)
    mux.Handle("/sync/", authMiddleware(h.Routes()))
    ```

**Tradeoffs:**

| Advantage | Disadvantage |
|-----------|--------------|
| Single binary to deploy | WAL consumer competes for CPU/memory |
| Shared connection pool | Application restart restarts WAL consumer |
| No inter-process latency | Harder to scale sync independently |

---

### Mode B: Split Runtime, Shared DB

Dedicated sync worker process. Same database.

```mermaid
graph TB
    subgraph "App Process"
        API[App API]
    end
    subgraph "Sync Worker"
        Sync[Synchro Engine]
        WAL[WAL Consumer]
    end
    DB[(PostgreSQL)]
    API --> DB
    Sync --> DB
    WAL --> DB
```

**Use when:** sync CPU competes with API, you need process isolation before splitting the database.

**Tradeoffs:**

| Advantage | Disadvantage |
|-----------|--------------|
| Process isolation | Two processes to deploy |
| Independent scaling | Shared DB connection limits |
| App restarts don't affect sync | Shared DB can still be a bottleneck |

---

### Mode C: Scale Topology

Full isolation with a dedicated sync store.

```mermaid
graph TB
    subgraph "App Process"
        API[App API]
    end
    subgraph "Sync Worker"
        Sync[Synchro Engine]
        WAL[WAL Consumer]
    end
    PrimaryDB[(Primary DB)]
    SyncDB[(Sync Store)]
    Replica[(Read Replica)]
    API --> PrimaryDB
    WAL --> PrimaryDB
    Sync --> SyncDB
    Sync --> Replica
```

**Use when:** high write throughput, large bucket fanout, strict OLTP protection needed.

**Tradeoffs:**

| Advantage | Disadvantage |
|-----------|--------------|
| Full DB isolation | Multiple databases to manage |
| Independent resource allocation | Cross-DB consistency is eventual |
| Read replicas for pull scale-out | Higher operational complexity |

!!! info "Same library, same protocol"
    All three modes use the same Synchro library and the same client SDKs. Moving between modes is a configuration change, not a rewrite.

---

## Production Checklist

### Database

- [ ] `wal_level = logical` confirmed (`SHOW wal_level;`)
- [ ] Publication created for all synced tables
- [ ] Replication slot capacity sufficient (`max_replication_slots`)
- [ ] Infrastructure tables migrated (`migrate.Migrations()`)

### Security

- [ ] RLS policies applied (`GenerateRLSPolicies`)
- [ ] JWT authentication configured (`JWT_SECRET` or `JWKS_URL`)
- [ ] Database user for sync has `REPLICATION` privilege
- [ ] Application database role (for push RLS) is non-superuser

### Runtime

- [ ] WAL consumer running with replication connection
- [ ] Compaction enabled (background goroutine or cron)
- [ ] `MIN_CLIENT_VERSION` set if enforcing SDK upgrades

### Monitoring

- [ ] WAL consumer replication lag
- [ ] `sync_changelog` table size and growth rate
- [ ] Client sync latency (time since `last_sync_at`)
- [ ] Push/pull error rates and latency histograms
- [ ] Replication slot `pg_replication_slots.active` status

### Backup and Recovery

- [ ] Backup strategy includes `sync_changelog` and `sync_clients` tables
- [ ] Tested restore procedure for sync infrastructure tables
- [ ] Replication slot recreation documented for disaster recovery

!!! warning "Replication slot retention"
    An inactive replication slot causes PostgreSQL to retain WAL segments indefinitely, which can fill disk. Monitor `pg_replication_slots` and drop orphaned slots.
