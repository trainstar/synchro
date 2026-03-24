<p align="center">
  <img src="docs/public/logo.svg" alt="Synchro" width="320">
</p>

<p align="center"><img src="https://github.com/trainstar/synchro/actions/workflows/ci.yml/badge.svg?branch=dev" alt="CI">&nbsp;<img src="https://github.com/trainstar/synchro/actions/workflows/codeql.yml/badge.svg?branch=dev" alt="CodeQL">&nbsp;<img src="https://github.com/trainstar/synchro/actions/workflows/dependency-review.yml/badge.svg?branch=dev" alt="Dependency Scan">&nbsp;<img src="https://github.com/trainstar/synchro/actions/workflows/docs.yml/badge.svg?branch=dev" alt="Docs"></p>

# Synchro

Synchro is an offline-first sync system for native apps using local SQLite, a PostgreSQL extension, and a thin HTTP adapter.

It is built for teams that want SQLite-first clients, deterministic selective sync, and a server implementation that stays close to PostgreSQL instead of recreating sync semantics in application code.

## Why Synchro

- local SQLite is the product center, not a cache afterthought
- sync semantics live in Rust and PostgreSQL, close to the data and WAL
- the HTTP adapter stays thin and transport-focused
- Swift and Kotlin are the real client engines, React Native is a bridge over them
- portable seed generation gives apps a real warm-start path instead of a schema-only bootstrap

## Architecture

The current release architecture is:

- `extensions/synchro-core`: shared deterministic sync semantics in Rust
- `extensions/synchro-pg`: PostgreSQL extension that executes sync logic near the data
- `api/go`: thin HTTP adapter over the extension
- `clients/swift`: native Apple SDK
- `clients/kotlin`: native Android SDK
- `clients/react-native`: thin React Native bridge over the native SDKs

## Scope model

The README should only give the mental model:

- private data belongs in private scopes
- shared data belongs in shared runtime scopes
- bundled seeds export those same shared runtime scopes, not seed-only copies

Detailed scope design guidance lives in the [Scope modeling](https://trainstar.github.io/synchro/architecture/scope-modeling/) guide. The normative architectural rules live in the [Principles](https://trainstar.github.io/synchro/spec/00-principles/) spec.

## Current support

The current supported release surface is:

- PostgreSQL 18
- Swift on iOS 16+ and macOS 13+
- Kotlin on Android API 24+
- React Native bridge over the native SDKs

The validated test matrix for that surface is:

- `make test-rust-core`
- `make test-rust-pg`
- `make test-adapter`
- `make test-swift`
- `make test-kotlin`
- `make test-rn`

## Documentation

- [Docs site](https://trainstar.github.io/synchro)
- [Welcome](https://trainstar.github.io/synchro/)
- [Architecture overview](https://trainstar.github.io/synchro/architecture/overview/)
- [Scope modeling](https://trainstar.github.io/synchro/architecture/scope-modeling/)
- [Portable seeds](https://trainstar.github.io/synchro/architecture/portable-seeds/)
- [Auth integration](https://trainstar.github.io/synchro/architecture/auth-integration/)
- [Client integration overview](https://trainstar.github.io/synchro/clients/overview/)
- [Client consumption](https://trainstar.github.io/synchro/clients/consumption/)
- [Quickstart](https://trainstar.github.io/synchro/getting-started/quickstart/)
- [Published release spec](https://trainstar.github.io/synchro/spec/00-principles/)
- Docs source: [docs/src/content/docs/spec/00-principles.mdx](/Users/mdspinali/Documents/projects/trainstar/repos/synchro/docs/src/content/docs/spec/00-principles.mdx)
- Shared conformance fixtures: [conformance/README.md](/Users/mdspinali/Documents/projects/trainstar/repos/synchro/conformance/README.md)

## Quickstart

### 1. Validate the core release surface

```bash
make test-rust-core
make test-rust-pg
make test-adapter
```

### 2. Validate the native SDKs and React Native bridge

```bash
make test-swift
make test-kotlin
make test-rn
```

### 3. Start the extension-backed adapter locally

```bash
make synchrod-pg-test-start
```

The default test adapter listens on `http://localhost:8081`.

### 4. Generate a preinitialized seed database

```bash
cd api/go
GOWORK=off go run ./cmd/synchro-seed \
  --database-url "postgres://user:pass@localhost:5432/app?sslmode=disable" \
  --output ./build/seed.db
```

This generator creates a client-compatible SQLite seed from the canonical PostgreSQL schema manifest plus any explicitly declared portable scopes. If no portable scopes are configured, it falls back to a schema-only seed.

See the [portable seeds guide](https://trainstar.github.io/synchro/architecture/portable-seeds/) for the full contract.

## Auth integration

The adapter supports two integration modes for protected sync routes:

- trusted upstream auth, recommended when Synchro is mounted behind an existing product API
- direct JWT validation inside the adapter

For hosted product APIs, trusted upstream auth is the better default. See the [auth integration guide](https://trainstar.github.io/synchro/architecture/auth-integration/) for the decision framework and wiring model.

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

## Public release surfaces

Synchro currently publishes or intends to publish these consumer surfaces:

- PostgreSQL extension for PG 18
- `synchrod-pg` adapter binary from `api/go/cmd/synchrod-pg`
- `synchro-seed` generator binary from `api/go/cmd/synchro-seed`
- Swift Package Manager package from the repo root `Package.swift`
- CocoaPods package from the repo root `Synchro.podspec`
- Kotlin package from `clients/kotlin/synchro`
- React Native package from `clients/react-native`

## Local and published consumption

Supported consumers should switch dependency source only. Package identity and runtime behavior stay the same.

| Surface | Local development | Published release |
| --- | --- | --- |
| Native Apple SDK | local SPM package path to the repo root | tagged SPM release from the repo root |
| Apple pod surface | `pod 'Synchro', :path => '/absolute/path/to/synchro'` | `pod 'Synchro', :git => 'https://github.com/trainstar/synchro.git', :tag => 'v<version>'` |
| Kotlin SDK | `mavenLocal()`, then `mavenCentral()` | Maven Central |
| React Native package | local packed `.tgz` from `make local-consumer-artifacts` | npm |
| RN iOS native dependency | local pod path to the repo root `Synchro.podspec` | tagged git pod `v<version>` |
| RN Android native dependency | `mavenLocal()`, then `mavenCentral()`, optional `synchroVersion=<version>` override | Maven Central |

Use `make local-consumer-artifacts` to prepare the supported local-consumer flow:

- a packed React Native tarball in `dist/local-consumer/`
- the Kotlin SDK published to `mavenLocal()`
- validated Apple local-consumer surfaces from the repo root

The full installation matrix lives in the [client consumption guide](https://trainstar.github.io/synchro/clients/consumption/).

## React Native iOS note

React Native on iOS currently depends on the native Apple SDK and GRDB being added in the consuming app's Podfile. That installation requirement is part of the current public contract in both local and published modes.

Example Podfile entries:

```ruby
pod 'Synchro', :git => 'https://github.com/trainstar/synchro.git', :tag => 'v<version>'
pod 'GRDB.swift', :git => 'https://github.com/groue/GRDB.swift.git', :tag => 'v7.0.0'
```
