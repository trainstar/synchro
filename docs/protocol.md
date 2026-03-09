# Synchro Wire Protocol

All endpoints accept and return `application/json`. Authentication is handled by the consuming application. Synchro expects a user ID in request context for `register`, `pull`, `push`, and `snapshot`.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/sync/register` | Register or re-register a client device |
| `POST` | `/sync/pull` | Pull incremental changes after a checkpoint |
| `POST` | `/sync/push` | Push local changes to the server |
| `POST` | `/sync/snapshot` | Read a full snapshot for bootstrap or rebuild |
| `GET` | `/sync/tables` | Get sync metadata for all tables |
| `GET` | `/sync/schema` | Get the canonical schema contract for SDK table creation |

## POST /sync/register

Registers a client device for sync. Upserts on `(user_id, client_id)`. On first registration, the client is subscribed to `["user:<user_id>", "global"]` buckets.

### Request

```json
{
  "client_id": "device-abc-123",
  "client_name": "Matt's iPhone",
  "platform": "ios",
  "app_version": "1.3.0",
  "schema_version": 0,
  "schema_hash": ""
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `client_id` | `string` | Yes | Stable device identifier (UUID or vendor ID) |
| `client_name` | `string` | No | Human-readable device name |
| `platform` | `string` | Yes | `ios`, `android`, `web`, etc. |
| `app_version` | `string` | Yes | Semver app version for compatibility checks |
| `schema_version` | `int64` | Yes | Schema contract version (`0` before first schema fetch) |
| `schema_hash` | `string` | Yes | Schema contract hash (`""` before first schema fetch) |

### Response (200)

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "server_time": "2026-03-05T12:00:00Z",
  "last_sync_at": "2026-03-05T11:55:00Z",
  "checkpoint": 42,
  "schema_version": 7,
  "schema_hash": "d16b7d9d..."
}
```

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | Server-assigned client row ID |
| `server_time` | `string` | Current server time (ISO 8601) |
| `last_sync_at` | `string?` | Last sync timestamp, null on first registration |
| `checkpoint` | `int64` | Last known pull checkpoint (0 on first registration) |
| `schema_version` | `int64` | Current server schema version |
| `schema_hash` | `string` | Current server schema hash |

### Error (426 Upgrade Required)

Returned when `app_version` is below the server's `MinClientVersion`.

```json
{
  "error": "client upgrade required"
}
```

## POST /sync/pull

Retrieves incremental changes for the client after its checkpoint. `pull` is not the bootstrap endpoint. Fresh clients and stale clients rebuild through `POST /sync/snapshot`.

### Request

```json
{
  "client_id": "device-abc-123",
  "checkpoint": 42,
  "tables": ["workouts", "workout_sets"],
  "limit": 100,
  "known_buckets": ["user:123", "global"],
  "schema_version": 7,
  "schema_hash": "d16b7d9d..."
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `client_id` | `string` | Yes | -- | Registered client ID |
| `checkpoint` | `int64` | Yes | -- | Last processed changelog seq |
| `tables` | `string[]` | No | all tables | Optional table filter |
| `limit` | `int` | No | 100 | Max records per response (capped at 1000) |
| `known_buckets` | `string[]` | No | -- | Buckets the client currently knows about |
| `schema_version` | `int64` | Yes | -- | Client schema version |
| `schema_hash` | `string` | Yes | -- | Client schema hash |

### Response (200)

```json
{
  "changes": [
    {
      "id": "a1b2c3d4-...",
      "table_name": "workouts",
      "data": {
        "id": "a1b2c3d4-...",
        "name": "Push Day",
        "user_id": "user-123",
        "created_at": "2026-03-05T10:00:00Z",
        "updated_at": "2026-03-05T11:30:00Z"
      },
      "updated_at": "2026-03-05T11:30:00Z"
    }
  ],
  "deletes": [
    {
      "id": "e5f6g7h8-...",
      "table_name": "workout_sets"
    }
  ],
  "checkpoint": 58,
  "has_more": true,
  "bucket_updates": {
    "added": ["share:xyz"],
    "removed": []
  },
  "schema_version": 7,
  "schema_hash": "d16b7d9d..."
}
```

| Field | Type | Description |
|-------|------|-------------|
| `changes` | `Record[]` | Inserted or updated records with full data |
| `deletes` | `DeleteEntry[]` | Records that were deleted (soft-deleted) |
| `checkpoint` | `int64` | New checkpoint to send on next pull |
| `has_more` | `bool` | `true` if more records are available (pull again) |
| `snapshot_required` | `bool` | `true` if the client must rebuild from `/sync/snapshot` |
| `snapshot_reason` | `string` | Why incremental pull is invalid |
| `bucket_updates` | `BucketUpdate?` | Changes to bucket subscriptions |
| `schema_version` | `int64` | Current server schema version |
| `schema_hash` | `string` | Current server schema hash |

When `snapshot_required` is `true`, incremental replay is invalid. Current reasons:

| Value | Meaning |
|-------|---------|
| `initial_sync_required` | The client has not completed initial bootstrap yet |
| `checkpoint_before_retention` | The client's checkpoint is behind the compaction boundary |
| `history_unavailable` | Incremental history cannot satisfy the request |

The client must call `POST /sync/snapshot` and rebuild local state before returning to normal incremental pull.

**Record** fields:

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | Record primary key |
| `table_name` | `string` | Source table |
| `data` | `object` | Full record as JSON (respects `SyncColumns` if configured) |
| `updated_at` | `string` | Server-side `updated_at` timestamp |
| `deleted_at` | `string?` | Present when the record is soft-deleted |

**DeleteEntry** fields:

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | Deleted record primary key |
| `table_name` | `string` | Source table |

### Pull Pagination

When `has_more` is `true`, the client should immediately call `pull` again using the new `checkpoint`. Repeat until `has_more` is `false`.

```text
Pull(checkpoint=42)   -> changes[...], checkpoint=100, has_more=true
Pull(checkpoint=100)  -> changes[...], checkpoint=185, has_more=true
Pull(checkpoint=185)  -> changes[...], checkpoint=192, has_more=false
```

## POST /sync/push

Pushes local changes from the client to the server. All changes are applied inside a single database transaction under RLS context.

### Request

```json
{
  "client_id": "device-abc-123",
  "schema_version": 7,
  "schema_hash": "d16b7d9d...",
  "changes": [
    {
      "id": "new-uuid-here",
      "table_name": "workouts",
      "operation": "create",
      "data": {
        "name": "Leg Day",
        "description": "Squats and deadlifts"
      },
      "client_updated_at": "2026-03-05T11:00:00Z"
    },
    {
      "id": "existing-uuid",
      "table_name": "workouts",
      "operation": "update",
      "data": {
        "name": "Updated Push Day"
      },
      "client_updated_at": "2026-03-05T11:05:00Z",
      "base_updated_at": "2026-03-05T10:00:00Z"
    },
    {
      "id": "deleted-uuid",
      "table_name": "workout_sets",
      "operation": "delete",
      "client_updated_at": "2026-03-05T11:10:00Z"
    }
  ]
}
```

**PushRecord** fields:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | `string` | Yes | Record primary key (client-generated UUID for creates) |
| `table_name` | `string` | Yes | Target table |
| `operation` | `string` | Yes | `create`, `update`, or `delete` |
| `data` | `object` | For create/update | Record fields to write |
| `client_updated_at` | `string` | Yes | Client-side timestamp for conflict resolution |
| `base_updated_at` | `string` | No | Server timestamp client last saw (optimistic concurrency) |

### Response (200)

```json
{
  "accepted": [
    {
      "id": "new-uuid-here",
      "table_name": "workouts",
      "operation": "create",
      "status": "applied",
      "server_updated_at": "2026-03-05T11:00:01Z"
    }
  ],
  "rejected": [
    {
      "id": "existing-uuid",
      "table_name": "workouts",
      "operation": "update",
      "status": "conflict",
      "reason_code": "server_newer",
      "message": "server version is newer",
      "server_version": {
        "id": "existing-uuid",
        "table_name": "workouts",
        "data": { "name": "Server Push Day" },
        "updated_at": "2026-03-05T11:03:00Z"
      }
    }
  ],
  "checkpoint": 0,
  "server_time": "2026-03-05T12:00:00Z",
  "schema_version": 7,
  "schema_hash": "d16b7d9d..."
}
```

**PushResult** fields:

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | Record primary key |
| `table_name` | `string` | Target table |
| `operation` | `string` | `create`, `update`, or `delete` |
| `status` | `string` | Processing outcome |
| `reason_code` | `string?` | Machine-readable rejection or conflict reason |
| `message` | `string?` | Human-readable message |
| `server_version` | `Record?` | Current server record for conflicts |
| `server_updated_at` | `string?` | Server timestamp for successfully applied writes |
| `server_deleted_at` | `string?` | Server delete timestamp when relevant |

Push statuses:

| Status | Meaning |
|--------|---------|
| `applied` | The write was accepted and committed |
| `conflict` | The server version won; `server_version` is returned |
| `rejected_terminal` | The write is invalid and should be removed from the local queue |
| `rejected_retryable` | The write should remain queued and be retried later |

## POST /sync/snapshot

Reads a full snapshot for bootstrap or local rebuild. Snapshot pages are stateless except for the cursor the client sends back.

The server captures a snapshot checkpoint once at the start of the flow. That checkpoint is carried in the cursor and returned on every page. After the final page, the client uses that checkpoint for normal incremental `pull`.

### Request

```json
{
  "client_id": "device-abc-123",
  "cursor": null,
  "limit": 100,
  "schema_version": 7,
  "schema_hash": "d16b7d9d..."
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `client_id` | `string` | Yes | -- | Registered client ID |
| `cursor` | `SnapshotCursor?` | No | `null` | Cursor from the previous snapshot page |
| `limit` | `int` | No | 100 | Max records per response (capped at 1000) |
| `schema_version` | `int64` | Yes | -- | Client schema version |
| `schema_hash` | `string` | Yes | -- | Client schema hash |

**SnapshotCursor** fields:

| Field | Type | Description |
|-------|------|-------------|
| `checkpoint` | `int64` | Snapshot checkpoint captured on the first page |
| `table_idx` | `int` | Current table index in registration order |
| `after_id` | `string` | Last emitted primary key within the current table |

### Response (200)

```json
{
  "records": [
    {
      "id": "a1b2c3d4-...",
      "table_name": "workouts",
      "data": {
        "id": "a1b2c3d4-...",
        "name": "Push Day",
        "user_id": "user-123",
        "created_at": "2026-03-05T10:00:00Z",
        "updated_at": "2026-03-05T11:30:00Z"
      },
      "updated_at": "2026-03-05T11:30:00Z"
    }
  ],
  "cursor": {
    "checkpoint": 58,
    "table_idx": 1,
    "after_id": "a1b2c3d4-..."
  },
  "checkpoint": 58,
  "has_more": true,
  "schema_version": 7,
  "schema_hash": "d16b7d9d..."
}
```

| Field | Type | Description |
|-------|------|-------------|
| `records` | `Record[]` | Full non-deleted rows visible to the client |
| `cursor` | `SnapshotCursor?` | Cursor for the next page, omitted on the final page |
| `checkpoint` | `int64` | Snapshot checkpoint captured at snapshot start |
| `has_more` | `bool` | `true` if more snapshot pages remain |
| `schema_version` | `int64` | Current server schema version |
| `schema_hash` | `string` | Current server schema hash |

### Snapshot Flow

```text
1. POST /sync/register
2. GET /sync/schema
3. POST /sync/snapshot {client_id, cursor: null}
4. POST /sync/snapshot {client_id, cursor: {...}}
5. Repeat until has_more=false
6. Persist the returned checkpoint
7. Resume normal POST /sync/pull from that checkpoint
```

## GET /sync/tables

Returns sync metadata for all registered tables. No authentication required.

### Response (200)

```json
{
  "tables": [
    {
      "table_name": "workouts",
      "push_policy": "owner_only",
      "dependencies": [],
      "parent_table": ""
    },
    {
      "table_name": "workout_sets",
      "push_policy": "owner_only",
      "dependencies": ["workouts"],
      "parent_table": "workouts"
    },
    {
      "table_name": "exercise_types",
      "push_policy": "disabled",
      "dependencies": []
    }
  ],
  "server_time": "2026-03-05T12:00:00Z",
  "schema_version": 7,
  "schema_hash": "d16b7d9d..."
}
```

Clients use this to understand push ordering and pull-only vs pushable tables. The schema endpoint remains the authoritative source for local table creation.

## GET /sync/schema

Returns the server-authoritative schema contract for local table creation and migration.

### Response (200)

```json
{
  "schema_version": 7,
  "schema_hash": "d16b7d9d...",
  "server_time": "2026-03-05T12:00:00Z",
  "tables": [
    {
      "table_name": "workouts",
      "push_policy": "owner_only",
      "updated_at_column": "updated_at",
      "deleted_at_column": "deleted_at",
      "primary_key": ["id"],
      "columns": [
        {
          "name": "id",
          "db_type": "uuid",
          "logical_type": "string",
          "nullable": false,
          "default_kind": "none",
          "is_primary_key": true
        },
        {
          "name": "created_at",
          "db_type": "timestamp with time zone",
          "logical_type": "datetime",
          "nullable": false,
          "default_sql": "CURRENT_TIMESTAMP",
          "default_kind": "portable",
          "sqlite_default_sql": "CURRENT_TIMESTAMP",
          "is_primary_key": false
        }
      ]
    }
  ]
}
```

### Schema Column Fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | `string` | Column name |
| `db_type` | `string` | Source PostgreSQL type |
| `logical_type` | `string` | Client SDK logical type |
| `nullable` | `bool` | Whether the column is nullable |
| `default_sql` | `string?` | Raw PostgreSQL default expression |
| `default_kind` | `string` | `none`, `portable`, or `server_only` |
| `sqlite_default_sql` | `string?` | SQLite-safe default expression when `default_kind=portable` |
| `is_primary_key` | `bool` | Whether the column is part of the primary key |

`sqlite_default_sql` is the only default expression client SDKs should use when generating local SQLite tables. `server_only` defaults are informative only and cannot be translated locally.

## Error Responses

Most errors return a JSON body with an `error` field.

| Status | Condition |
|--------|-----------|
| `400` | Malformed request body or missing required fields |
| `401` | Missing user identity in context |
| `404` | Snapshot requested for an unregistered client |
| `409` | Schema mismatch (`code = schema_mismatch`) |
| `426` | Client version below `MinClientVersion` |
| `429` | Rate limited (via `RetryAfterMiddleware`) |
| `500` | Internal server error |
| `503` | Transient error (DB down, connection lost, timeout) |

```json
{
  "error": "description of the error"
}
```

`429` and `503` responses include a `Retry-After` header and a `retry_after` field in the body:

```json
{
  "error": "service temporarily unavailable",
  "retry_after": 5
}
```

Schema mismatch response body:

```json
{
  "code": "schema_mismatch",
  "message": "client schema does not match server schema",
  "server_schema_version": 7,
  "server_schema_hash": "d16b7d9d..."
}
```
