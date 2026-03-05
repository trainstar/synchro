# Synchro Wire Protocol

All endpoints accept and return `application/json`. Authentication is handled by the consuming application; synchro expects a user ID in the request context.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/sync/register` | Register or re-register a client device |
| `GET` | `/sync/pull` | Pull changes since checkpoint |
| `POST` | `/sync/push` | Push local changes to server |
| `GET` | `/sync/tables` | Get sync metadata for all tables |

## POST /sync/register

Registers a client device for sync. Upserts on `(user_id, client_id)`. On first registration, the client is subscribed to `["user:<user_id>", "global"]` buckets.

### Request

```json
{
  "client_id": "device-abc-123",
  "client_name": "Matt's iPhone",
  "platform": "ios",
  "app_version": "1.3.0"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `client_id` | `string` | Yes | Stable device identifier (UUID or vendor ID) |
| `client_name` | `string` | No | Human-readable device name |
| `platform` | `string` | Yes | `ios`, `android`, `web`, etc. |
| `app_version` | `string` | Yes | Semver app version for compatibility checks |

### Response (200)

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "server_time": "2026-03-05T12:00:00Z",
  "last_sync_at": "2026-03-05T11:55:00Z",
  "checkpoint": 42
}
```

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | Server-assigned client row ID |
| `server_time` | `string` | Current server time (ISO 8601) |
| `last_sync_at` | `string?` | Last sync timestamp, null on first registration |
| `checkpoint` | `int64` | Last known pull checkpoint (0 on first registration) |

### Error (426 Upgrade Required)

Returned when `app_version` is below the server's `MinClientVersion`.

```json
{
  "error": "client upgrade required"
}
```

## GET /sync/pull

Retrieves changes for the client since their checkpoint. Uses query parameters (GET request — idempotent read).

### Query Parameters

```
GET /sync/pull?client_id=device-abc-123&checkpoint=42&tables=workouts,workout_sets&limit=100&known_buckets=user:123,global
```

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `client_id` | `string` | Yes | -- | The registered client ID |
| `checkpoint` | `int64` | Yes | -- | Last processed changelog seq (0 for initial sync) |
| `tables` | `string` | No | all tables | Comma-separated table filter |
| `limit` | `int` | No | 100 | Max records per response (capped at 1000) |
| `known_buckets` | `string` | No | -- | Comma-separated bucket IDs the client knows about (enables `bucket_updates` in response) |

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
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `changes` | `Record[]` | Inserted or updated records with full data |
| `deletes` | `DeleteEntry[]` | Records that were deleted (soft-deleted) |
| `checkpoint` | `int64` | New checkpoint to send on next pull |
| `has_more` | `bool` | `true` if more records are available (pull again) |
| `bucket_updates` | `BucketUpdate?` | Changes to client's bucket subscriptions |

**Record** fields:

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | Record primary key |
| `table_name` | `string` | Source table |
| `data` | `object` | Full record as JSON (respects `SyncColumns` if configured) |
| `updated_at` | `string` | Server-side updated_at timestamp |
| `deleted_at` | `string?` | Present if record is soft-deleted |

**DeleteEntry** fields:

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | Deleted record primary key |
| `table_name` | `string` | Source table |

### Pull Pagination

When `has_more` is `true`, the client should immediately pull again using the new `checkpoint`. Repeat until `has_more` is `false`.

```
Pull(checkpoint=0)     -> changes[...], checkpoint=100, has_more=true
Pull(checkpoint=100)   -> changes[...], checkpoint=185, has_more=true
Pull(checkpoint=185)   -> changes[...], checkpoint=192, has_more=false
```

## POST /sync/push

Pushes local changes from the client to the server. All changes are applied within a single database transaction under RLS context.

### Request

```json
{
  "client_id": "device-abc-123",
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
| `client_updated_at` | `string` | Yes | Client-side timestamp for LWW conflict resolution |
| `base_updated_at` | `string` | No | Server timestamp client last saw (optimistic concurrency) |

### Response (200)

```json
{
  "accepted": [
    {
      "id": "new-uuid-here",
      "table_name": "workouts",
      "operation": "create",
      "status": "applied"
    }
  ],
  "rejected": [
    {
      "id": "existing-uuid",
      "table_name": "workouts",
      "operation": "update",
      "status": "conflict",
      "reason": "server version is newer",
      "server_version": {
        "id": "existing-uuid",
        "table_name": "workouts",
        "data": { "name": "Server Push Day", "...": "..." },
        "updated_at": "2026-03-05T11:03:00Z"
      }
    }
  ],
  "checkpoint": 0,
  "server_time": "2026-03-05T12:00:00Z"
}
```

**PushResult** fields:

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | Record primary key |
| `table_name` | `string` | Target table |
| `operation` | `string` | The operation that was attempted |
| `status` | `string` | `applied`, `conflict`, or `error` |
| `reason` | `string?` | Explanation for conflict or error |
| `server_version` | `Record?` | Current server version (on conflict) |

### Push Operations

**create**
- Inserts a new record. The `id` field in `data` is set server-side.
- The owner column is always set to the authenticated user.
- If a record with the same ID already exists and is not deleted, returns `conflict` with `"record already exists"`.
- If the existing record is soft-deleted, the create is treated as an update (resurrection).

**update**
- Updates an existing record. Only non-protected columns from `data` are applied.
- Runs conflict resolution (LWW by default). If `base_updated_at` is provided and the server version matches it, the client always wins (optimistic concurrency). Otherwise, timestamp comparison determines the winner.
- Cannot update a deleted record (returns `conflict` with `"record is deleted"`).

**delete**
- Soft-deletes by setting `deleted_at = now()`.
- Idempotent: deleting an already-deleted record returns `applied` with reason `"record already deleted"`.
- Returns `error` if the record does not exist or is not accessible via RLS.

### Protected Columns

The following columns are silently stripped from push data and never written by clients:

- `created_at`, `updated_at`, `deleted_at` (managed by server)
- The primary key column (set once on create)
- The owner column (set once on create, enforced to authenticated user)
- The parent FK column (set once on create)
- Any additional columns listed in `TableConfig.ProtectedColumns`

## Checkpoint Semantics

- Checkpoints are monotonically increasing `BIGSERIAL` values from `sync_changelog.seq`.
- A checkpoint of `0` means "give me everything."
- The pull response's `checkpoint` is the `seq` of the last changelog entry returned.
- The client must persist the checkpoint locally and send it on the next pull.
- Checkpoints only advance forward. Sending a lower checkpoint re-pulls already-seen data (safe but wasteful).
- If no changes exist since the checkpoint, the response returns the same checkpoint with empty changes.

### Initial Sync Flow

```
1. POST /sync/register          -> checkpoint: 0
2. GET  /sync/pull?client_id=...&checkpoint=0&limit=1000
   -> changes[...], checkpoint: 950, has_more: true
3. GET  /sync/pull?client_id=...&checkpoint=950&limit=1000
   -> changes[...], checkpoint: 1023, has_more: false
4. Client is now caught up. Store checkpoint: 1023.
```

## GET /sync/tables

Returns sync metadata for all registered tables. No authentication required.

### Response (200)

```json
{
  "tables": [
    {
      "table_name": "workouts",
      "direction": "bidirectional",
      "dependencies": [],
      "parent_table": ""
    },
    {
      "table_name": "workout_sets",
      "direction": "bidirectional",
      "dependencies": ["workouts"],
      "parent_table": "workouts"
    },
    {
      "table_name": "exercise_types",
      "direction": "server_only",
      "dependencies": []
    }
  ],
  "server_time": "2026-03-05T12:00:00Z"
}
```

Clients use this to understand push ordering (via `dependencies`) and which tables accept push vs. pull-only.

## Error Responses

All errors return a JSON body with an `error` field.

| Status | Condition |
|--------|-----------|
| `400` | Malformed request body |
| `401` | Missing user identity in context |
| `426` | Client version below `MinClientVersion` |
| `500` | Internal server error |

```json
{
  "error": "description of the error"
}
```
