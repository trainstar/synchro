# Synchro

Synchro is an offline-first sync system for native apps using local SQLite, a PostgreSQL extension, and a thin HTTP adapter.

The current release architecture is:

- `extensions/synchro-core`: shared deterministic sync semantics in Rust
- `extensions/synchro-pg`: PostgreSQL extension that executes sync logic near the data
- `api/go`: thin HTTP adapter over the extension
- `clients/swift`: native Apple SDK
- `clients/kotlin`: native Android SDK
- `clients/react-native`: thin React Native bridge over the native SDKs

## Status

The current release contract is documented, implemented, and validated for the current supported surface:

- PostgreSQL 18
- Swift on iOS 16+ and macOS 13+
- Kotlin on Android API 24+
- React Native bridge over the native SDKs

The validated test matrix for the current release scope is:

- `make test-rust-core`
- `make test-rust-pg`
- `make test-adapter`
- `make test-swift`
- `make test-kotlin`
- `make test-rn`

## Documentation

- Docs site: `https://trainstar.github.io/synchro`
- Published release spec: `https://trainstar.github.io/synchro/spec/00-principles/`
- Docs source: [docs/src/content/docs/spec/00-principles.mdx](/Users/mdspinali/Documents/projects/trainstar/repos/synchro/docs/src/content/docs/spec/00-principles.mdx)
- Shared conformance fixtures: [conformance/README.md](/Users/mdspinali/Documents/projects/trainstar/repos/synchro/conformance/README.md)

## Scope Design Guidelines

Use scopes to keep server-side fanout low.

Recommended modeling rules:

- private data belongs in private scopes such as `documents_user:{user_id}`
- shared or public data belongs in shared scopes such as `templates_public`
- the same local SQLite table may be populated from both shared and private scopes
- do not create seed-only scopes, bundled seeds should export the same shared runtime scopes clients continue syncing after login

Example:

- public templates that every user should see: `templates_public`
- user-created templates for one user: `templates_user:{user_id}`

Do not copy the public templates into every user scope.

That would make one public-row update touch every user scope. The better design is one shared scope with many subscribers. High scope count is acceptable. High row-change fanout is the real scaling problem.

Use one shared `global` scope only when the bundled public datasets are small, change together, and do not need independent rebuild or observability boundaries.

Split public data into multiple shared scopes when:

- one dataset is materially larger than the others
- it changes at a different rate
- it may need independent rebuild
- some clients may later subscribe to one shared dataset but not another

## Adapter Auth Modes

`api/go` supports two auth integration modes for authenticated sync routes:

- trusted upstream auth via `UserIDResolver`, recommended when Synchro is mounted behind an existing API router that already validates identity and resolves the internal user UUID
- direct JWT validation in the adapter via `JWTSecret` or `JWKSURL`

For Trainstar-style integration, the best practice is trusted upstream auth:

1. the main API validates the WorkOS bearer token
2. the main API resolves the Trainstar internal user UUID
3. Synchro receives that canonical internal user UUID through a trusted resolver
4. the PostgreSQL extension sets `app.user_id` and runs DB-side policy and sync logic

## Quick Start

### 1. Run the Rust core tests

```bash
make test-rust-core
```

### 2. Validate the PostgreSQL extension on PG 18

```bash
make test-rust-pg
```

### 3. Validate the Go adapter against the extension-backed test database

```bash
make test-adapter
```

### 4. Validate the native SDKs and React Native bridge

```bash
make test-swift
make test-kotlin
make test-rn
```

### 5. Start the local extension-backed adapter for manual work

```bash
make synchrod-pg-test-start
```

The test adapter listens on `http://localhost:8081` by default.

### 6. Generate a preinitialized seed database

Use the canonical PostgreSQL schema manifest and portable-scope export to build a client-compatible SQLite seed:

```bash
cd api/go
GOWORK=off go run ./cmd/synchro-seed \
  --database-url "postgres://user:pass@localhost:5432/app?sslmode=disable" \
  --output ./build/seed.db
```

This generator creates:

- the current synced-table SQLite schema
- the local CDC triggers the native SDKs expect
- any server-declared portable or public scope data
- portable scope continuation metadata in `_synchro_scopes`, `_synchro_scope_rows`, and compatibility checkpoint tables
- `_synchro_meta` entries for `schema_version`, `schema_hash`, `local_schema`, `scope_set_version`, `known_buckets`, and `snapshot_complete`

If no portable scopes are declared on the server, the generator falls back to a schema-only seed.

Use it when you want a warm-start database artifact for app bundling or installation-time copying. Clients open the seed, log in normally, and continue sync from that seeded state. The seed is not a different sync mode, and the generator does not require JWT input.

## Repository Layout

```text
api/go/                 Thin Go HTTP adapter
clients/swift/          Apple SDK
clients/kotlin/         Android SDK
clients/react-native/   React Native bridge
extensions/synchro-core Shared Rust semantics
extensions/synchro-pg   PostgreSQL extension
docs/                   Published docs site
conformance/            Shared protocol and scenario fixtures
```

## Release Surfaces

The intended public release surfaces are:

- PostgreSQL extension for PG 18
- `synchrod-pg` adapter binary from `api/go/cmd/synchrod-pg`
- `synchro-seed` generator binary from `api/go/cmd/synchro-seed`
- Swift Package Manager package from the repo root `Package.swift`
- CocoaPods package from the repo root `Synchro.podspec`
- Kotlin package from `clients/kotlin/synchro`
- React Native package from `clients/react-native`

React Native on iOS currently depends on the native Apple SDK and GRDB being added in the consuming app's Podfile. That installation requirement is part of the current public contract.

Example Podfile entries:

```ruby
pod 'Synchro', :git => 'https://github.com/trainstar/synchro.git', :tag => 'v0.2.0'
pod 'GRDB.swift', :git => 'https://github.com/groue/GRDB.swift.git', :tag => 'v7.0.0'
```
