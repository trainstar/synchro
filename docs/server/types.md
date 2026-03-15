# Type Reference

## Supported Types

Synchro maps PostgreSQL column types to a logical type system, which client SDKs then map to their local storage (SQLite on mobile).

### Core Type Mapping

| PostgreSQL Type | Logical Type | SQLite Type | Notes |
|----------------|-------------|-------------|-------|
| `uuid` | `string` | `TEXT` | |
| `text`, `varchar`, `char` | `string` | `TEXT` | Includes `character varying(n)`, `character(n)` |
| `integer`, `smallint` | `int` | `INTEGER` | Includes `serial`, `smallserial` |
| `bigint` | `int64` | `INTEGER` | Includes `bigserial` |
| `real`, `double precision` | `float` | `REAL` | |
| `numeric`, `decimal` | `float` | `REAL` | Includes `numeric(p,s)`, `decimal(p,s)` |
| `boolean` | `boolean` | `INTEGER` | `0` / `1` |
| `timestamp with time zone` | `datetime` | `TEXT` | ISO 8601 format |
| `timestamp without time zone` | `datetime` | `TEXT` | ISO 8601 format |
| `date` | `date` | `TEXT` | `YYYY-MM-DD` |
| `time without time zone` | `time` | `TEXT` | `HH:MM:SS` |
| `time with time zone` | `time` | `TEXT` | `HH:MM:SS+offset` |
| `json`, `jsonb` | `json` | `TEXT` | Serialized JSON string |
| `bytea` | `bytes` | `BLOB` | |
| `interval` | `string` | `TEXT` | PostgreSQL interval string representation |
| `tsvector` | `string` | `TEXT` | Full-text search vector as string |
| `ltree` | `string` | `TEXT` | Label tree path as string |

### Array Types

| PostgreSQL Type | Logical Type | SQLite Type | Notes |
|----------------|-------------|-------------|-------|
| `text[]`, `varchar[]` | `json` | `TEXT` | Serialized as JSON array |
| `integer[]`, `bigint[]` | `json` | `TEXT` | Serialized as JSON array |
| `uuid[]` | `json` | `TEXT` | Serialized as JSON array |
| Any type with `[]` suffix | `json` | `TEXT` | All arrays serialize to JSON |

### Special Types

| PostgreSQL Type | Logical Type | SQLite Type | Notes |
|----------------|-------------|-------------|-------|
| User-defined enums | `string` | `TEXT` | Stored as string value |
| Domains | *(base type)* | *(base type)* | Resolved to underlying type |
| `halfvec(n)` | `string` | `TEXT` | pgvector half-precision vector |

!!! info "Enum and domain resolution"
    When Synchro encounters a column type not in the core mapping, it queries `pg_type` to check if the type is a user-defined enum (mapped to `string`) or a domain (resolved to its base type and re-mapped). If neither, the column is rejected as unsupported.

---

## Unsupported Types

These PostgreSQL types are not supported for sync. Using them on a synced table will produce an error at schema load time.

| Type Category | Examples | Reason |
|--------------|----------|--------|
| Geometric | `point`, `line`, `polygon`, `circle` | No mobile equivalent |
| Network | `inet`, `cidr`, `macaddr` | Not applicable to mobile sync |
| Composite | User-defined composite types | Complex nested structure |
| Bit strings | `bit`, `bit varying` | Rarely used in application tables |
| XML | `xml` | Use `json`/`jsonb` instead |
| Range | `int4range`, `tsrange`, `daterange` | No SQLite equivalent |

!!! tip "Workaround for unsupported types"
    If you need a column with an unsupported type on a synced table, store it in a supported type and convert at the application layer. For example, store `inet` as `text`, or store `int4range` as two separate `integer` columns.

---

## Known Limitations

### Primary Keys

1. **Composite primary keys** are not supported. Each synced table must have a single-column primary key. UUID is recommended.
2. Primary keys are always synced and cannot be listed in `ProtectedColumns`.

### Numeric Precision

3. **`numeric`/`decimal` maps to `float`**, which means values are stored as `REAL` in SQLite. Floating-point precision loss may occur for values exceeding 15-17 significant digits. If exact decimal precision is critical, consider storing values as `text` and parsing in the application layer.

### Timestamps

4. **Timestamp precision**: SQLite stores timestamps as `TEXT` in ISO 8601 format. Sub-second precision depends on client SDK parsing.
5. **Timezone handling**: Both `timestamp with time zone` and `timestamp without time zone` map to the same `datetime` logical type. The server normalizes to UTC.

### Boolean Representation

6. Stored as `INTEGER` (`0`/`1`) on the client. Client SDKs handle the conversion transparently.

### UUID Storage

7. Stored as `TEXT` on the client. No binary UUID optimization is applied.

### Enums

8. **Enum values are not enforced client-side.** The client stores the string value and can write any string. The server validates enum membership on push, rejecting invalid values.

### Untested Features

9. **Generated columns** (PostgreSQL 12+): Server-computed values may not round-trip correctly. The generated expression is not replicated to the client.
10. **Partitioned tables**: Not tested with logical replication or the schema introspection queries.

---

## Default Value Portability

Not all PostgreSQL defaults can be replicated to SQLite. The schema endpoint classifies each default and provides a SQLite-compatible expression where possible.

### Default Kinds

| Kind | Meaning |
|------|---------|
| `none` | Column has no default |
| `portable` | Default can be applied on the client |
| `server_only` | Default only applies on the server; omitted on client |

### Portability Matrix

| Default Kind | Example | Portable? | Client Behavior |
|-------------|---------|-----------|-----------------|
| `NULL` | `DEFAULT NULL` | Yes | Applied locally |
| Literal number | `DEFAULT 0`, `DEFAULT 3.14` | Yes | Applied locally |
| Literal string | `DEFAULT 'active'` | Yes | Applied locally |
| Boolean literal | `DEFAULT true`, `DEFAULT false` | Yes | Mapped to `1`/`0` |
| JSON literal | `DEFAULT '{}'::jsonb` | Yes | Cast stripped, literal preserved |
| `CURRENT_TIMESTAMP` | `DEFAULT CURRENT_TIMESTAMP` | Yes | SQLite `CURRENT_TIMESTAMP` |
| `CURRENT_DATE` | `DEFAULT CURRENT_DATE` | Yes | SQLite `CURRENT_DATE` |
| `CURRENT_TIME` | `DEFAULT CURRENT_TIME` | Yes | SQLite `CURRENT_TIME` |
| `now()` | `DEFAULT now()` | Yes | Mapped to `CURRENT_TIMESTAMP` |
| Function call | `DEFAULT gen_random_uuid()` | No | Server-only, omitted locally |
| Sequence | `DEFAULT nextval(...)` | No | Server-only, omitted locally |
| Expression | `DEFAULT (now() + interval '30 days')` | No | Server-only, omitted locally |

!!! warning "Non-portable defaults on NOT NULL columns"
    If a push-enabled table has a `NOT NULL` column with a `server_only` default and no client-side default, inserts from the client will fail. Either make the column nullable, add a portable default, or list it in `ProtectedColumns` so clients never write to it.

### Schema Endpoint Response

The `/sync/schema` endpoint returns `default_kind` and `sqlite_default_sql` for each column:

```json
{
  "name": "created_at",
  "db_type": "timestamp with time zone",
  "logical_type": "datetime",
  "nullable": false,
  "default_sql": "now()",
  "default_kind": "portable",
  "sqlite_default_sql": "CURRENT_TIMESTAMP",
  "is_primary_key": false
}
```

---

## Column Constraints

### Constraint Replication

| Constraint | Server | Client | Notes |
|-----------|--------|--------|-------|
| `PRIMARY KEY` | Enforced | Enforced | Single-column only |
| `NOT NULL` | Enforced | Replicated | Applied to client schema |
| `UNIQUE` | Enforced | Single-column only | Multi-column unique not replicated |
| `FOREIGN KEY` | Enforced | Not enforced | Client is a cache; FK integrity maintained by server |
| `CHECK` | Enforced | Not enforced | Server validates on push |
| `DEFAULT` | Enforced | Portable only | See portability matrix above |

!!! info "Foreign keys on the client"
    Client SDKs do not create foreign key constraints. The client database is a local cache of server-authoritative data. Referential integrity is enforced by the server on push, and the sync protocol's dependency ordering ensures parent records arrive before children during pull and snapshot.

### Protected Columns

Columns listed in `ProtectedColumns` on a `TableConfig` are excluded from client writes (push). The server ignores values for these columns in push payloads. Common uses:

- Server-computed columns (`created_at`, `updated_at`)
- Ownership columns (`user_id`) that the server sets from the authenticated identity
- Derived or aggregated values

The system columns `id`, `created_at`, `updated_at`, and `deleted_at` are always protected implicitly, along with the configured owner column.
