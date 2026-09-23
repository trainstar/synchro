# Test Seed Database

This directory owns the PostgreSQL fixture schema and seed inputs for
extension and client integration testing.

## Files

- `schema.sql` creates the application tables, indexes, constraints, and
  fixture triggers.
- `register.sql` creates projection views, membership functions, registrations,
  dependencies, grants, and row-level-security policies.
- `canonical-seed.sql` contains the deterministic six-row relational fixture
  for normal seed-backed integration flows.
- `seed.sql` is the generated larger fixture.
- `generate/` contains the generator and its TPC-H, collaboration, and type
  data helpers.

## Fixture Model

The schema combines global reference tables, user-owned rows, foreign-key
ownership chains, shared document membership, and hierarchical categories.
`type_zoo` covers a finite set of supported PostgreSQL values:

- text, character, boolean, numeric, date, timestamp, interval, JSON, arrays,
  bytes, network, UUID, point, range, and XML values.

It is not a claim to cover every PostgreSQL type.

`register.sql` registers fixture relations with the `synchro` extension. It
also creates the required projection and membership functions, grants the
extension-managed roles access, and enables row-level security. Run it only
after `schema.sql` and `CREATE EXTENSION synchro_pg`.

## Use

Use the disposable fixture setup in
[the development guide](https://trainstar.github.io/synchro/getting-started/development/) to
load `ADAPTER_TEST_URL`. The PostgreSQL installation must match
`PGRX_PG_CONFIG`.

The provisioner already installs the matching extension and its roles.
Do not replace its extension library while the instance is running.
Before starting the test adapter, generate the larger fixture from the repository root:

```sh
make ext-seed
```

Generation requires Python 3, Git, Make, a C compiler, and network access to
the `electrum/tpch-dbgen` repository.
Use a fresh disposable fixture, not an instance that already contains the canonical seed.
Load the generated input in this order:

```sh
: "${ADAPTER_TEST_URL:?Load the disposable fixture environment first}"
psql -X -v ON_ERROR_STOP=1 "$ADAPTER_TEST_URL" -f extensions/testdata/schema.sql
psql -X -v ON_ERROR_STOP=1 "$ADAPTER_TEST_URL" -f extensions/testdata/register.sql
psql -X -v ON_ERROR_STOP=1 "$ADAPTER_TEST_URL" -f extensions/testdata/seed.sql
```

The provisioner's extension installation supplies the extension-managed roles.
Do not create or substitute those roles manually.

Use a disposable database because the fixture creates application objects,
extension registrations, role grants, and row-level-security policies. Do not
run this sequence against a production database or one with application data.

Normal seed-backed integration flows use `canonical-seed.sql`, not the larger
generated `seed.sql`. Do not replace the canonical fixture with generated
data.
