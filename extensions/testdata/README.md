# Test Seed Database

TPC-H inspired schema with modernized types, extended with collaboration and
hierarchical patterns for full sync testing coverage.

## Structure

- `schema.sql`: DDL for PostgreSQL (table creation, indexes, constraints)
- `register.sql`: synchro_register_table calls for all tables
- `seed.sql`: test data (3 users, ~2000 rows across all tables)

## Tables

### TPC-H core (ownership + reference data)

| Table | Bucket Pattern | Push Policy | Tests |
|---|---|---|---|
| regions | global | read_only | Reference data, global bucket |
| nations | global | read_only | Reference data, FK to regions |
| suppliers | global | read_only | Reference data, FK to nations |
| parts | global | read_only | Reference data, type_zoo columns |
| customers | user:{user_id} | enabled | Direct ownership, root bucket table |
| orders | user:{customer.user_id} | enabled | Parent chain (1 level), single-owner |
| line_items | user:{order.customer.user_id} | enabled | Parent chain (2 levels), deep FK |
| part_suppliers | global | read_only | Many-to-many reference |

### Collaboration + hierarchy extensions

| Table | Bucket Pattern | Push Policy | Tests |
|---|---|---|---|
| documents | user:{owner_id} | enabled | Ownership transfer testing |
| document_members | user:{user_id} per member | enabled | Shared ownership, multi-bucket |
| document_comments | user:{doc.owner} + user:{author} | enabled | Multiple ownership paths, 4-level chain |
| categories | global | read_only | Self-referential FK, hierarchical data |

### Type coverage

| Table | Purpose |
|---|---|
| type_zoo | One column per PG type. Push/pull round-trip validation |

## Sync patterns covered

- Single-owner bucket (customers, orders)
- Parent chain bucket resolution (line_items -> orders -> customers)
- Global/read-only reference (regions, nations, parts, suppliers, categories)
- Shared ownership / multi-bucket (document_members)
- Multiple ownership paths (document_comments: via author AND via document owner)
- Ownership transfer (documents: change owner_id, bucket edges must update)
- Self-referential FK (categories: parent_id -> categories.id)
- Deep FK chain 4+ levels (comment -> document -> member -> user)
- Every PG column type round-trip (type_zoo)

## Usage

```sql
-- Create tables
\i schema.sql

-- Register for sync
\i register.sql

-- Load test data
\i seed.sql
```
