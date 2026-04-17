<p align="center">
  <img src="docs/public/logo.svg" alt="Synchro" width="320">
</p>

<p align="center"><img src="https://github.com/trainstar/synchro/actions/workflows/ci.yml/badge.svg?branch=dev" alt="CI">&nbsp;<img src="https://github.com/trainstar/synchro/actions/workflows/codeql.yml/badge.svg?branch=dev" alt="CodeQL">&nbsp;<img src="https://github.com/trainstar/synchro/actions/workflows/dependency-review.yml/badge.svg?branch=dev" alt="Dependency Scan">&nbsp;<img src="https://github.com/trainstar/synchro/actions/workflows/docs.yml/badge.svg?branch=dev" alt="Docs"></p>

# Synchro

Synchro gives teams building serious native apps production-grade offline sync without a separate sync tier.

Its core differentiator is an extension-based server architecture: server-side sync logic runs inside PostgreSQL instead of being rebuilt in a standalone sync service or spread across application handlers.

Swift and Kotlin are first-class client engines. React Native is a bridge over them.

That architecture gives Synchro its shape:

- no separate sync tier on the server
- thin, replaceable host adapters
- end-to-end synced CRUD
- lower adoption friction for relational apps
- deterministic selective sync and explicit rebuilds

## When Synchro Fits

Use Synchro when:

- you ship a native app where offline has to work in production
- you want end-to-end sync without building a custom sync backend
- you want a leaner server shape than app API plus separate sync service plus database glue
- you want native client engines with built-in local change capture, queueing, and safe apply
- you want control of auth, backend, and data model without a black-box sync vendor

Synchro is not the right fit when:

- you need support for a server database other than PostgreSQL
- you want a browser-first sync client instead of native SQLite clients
- you want arbitrary client-authored replication predicates as the primary sync model

## Requirements

### Runtime Support

- PostgreSQL 18
- iOS 16 or later
- macOS 13 or later
- Android API 24 or later
- React Native through the native Apple and Android SDKs

### Local Validation and Development

- Rust toolchain with `cargo pgrx` configured for PostgreSQL 18
- Go 1.25 for `api/go`
- Xcode and Swift for Apple SDK validation
- JDK 17 and Android SDK for Kotlin and Android validation
- Node.js 22 and Yarn for React Native validation
- CocoaPods for React Native iOS validation

The `Makefile` is the supported entry point for validation. Android targets expect `ANDROID_HOME` and `ANDROID_JAVA_HOME` to be set correctly.

## Status and Support

- project status: active development
- repository owner: `trainstar`
- default development branch: `dev`
- issue tracker: [github.com/trainstar/synchro/issues](https://github.com/trainstar/synchro/issues)
- license: [LICENSE](LICENSE)

## What This Repository Contains

- `extensions/synchro-core`: shared deterministic sync semantics in Rust
- `extensions/synchro-pg`: PostgreSQL extension that applies sync logic close to the data
- `api/go`: reference Go host library for HTTP, auth integration, and version gating
- `clients/swift`: native Apple SDK
- `clients/kotlin`: native Android SDK
- `clients/react-native`: React Native bridge over the native SDKs
- `conformance/`: shared executable contract fixtures
- `docs/`: published docs site and specification

## Why Synchro

- Extension-based server architecture. Sync logic runs inside PostgreSQL, which removes the need for a separate sync tier.
- End-to-end synced CRUD. Synchro owns connect, push, pull, rebuild, conflict handling, and canonical server responses.
- Native-first client engines. Swift and Kotlin own local SQLite integration, change capture, durable queueing, retry, and safe apply. React Native stays a bridge over them.
- Lower adoption friction. Synchro is designed for minimal required schema changes, side-table metadata where possible, and SQL-based configuration in the database repo.
- Deterministic selective sync. Server-defined scopes, materialized membership, and explicit rebuilds make sync behavior predictable.
- Portable seeds. Generate client-compatible SQLite seeds from PostgreSQL for faster first start and bundled shared data.

## Quick Start

Validate the core server surface:

```bash
make test-rust-core
make test-rust-pg
make test-adapter
```

Validate the client surfaces:

```bash
make test-swift
make test-kotlin
make test-rn
```

Start the extension-backed local adapter:

```bash
make synchrod-pg-test-start
```

The default local test URL is `http://localhost:8081`.

Stop it when you are done:

```bash
make synchrod-pg-test-stop
```

Build the seed database generator:

```bash
make build-seed
```

For the full local validation and release matrix, see:

- [Quickstart](https://trainstar.github.io/synchro/getting-started/quickstart/)
- [Client consumption](https://trainstar.github.io/synchro/clients/consumption/)

## Public Release Surfaces

Synchro currently publishes or is structured to publish these surfaces:

- PostgreSQL extension for PostgreSQL 18
- `synchrod-pg` reference host binary
- `synchro-seed` seed database generator
- Swift Package Manager package from the repo root
- CocoaPods package from the repo root
- Kotlin package from `clients/kotlin/synchro`
- React Native package from `clients/react-native`

## Architecture and Spec

Start here if you are evaluating the system:

- [Docs site](https://trainstar.github.io/synchro/)
- [Architecture overview](https://trainstar.github.io/synchro/architecture/overview/)
- [Scope modeling](https://trainstar.github.io/synchro/architecture/scope-modeling/)
- [Portable seeds](https://trainstar.github.io/synchro/architecture/portable-seeds/)
- [Auth integration](https://trainstar.github.io/synchro/architecture/auth-integration/)
- [Wire protocol](https://trainstar.github.io/synchro/spec/01-wire-protocol/)
- [Client contract](https://trainstar.github.io/synchro/spec/02-client-contract/)
- [State machines](https://trainstar.github.io/synchro/spec/03-state-machines/)
- [Schema evolution](https://trainstar.github.io/synchro/spec/05-schema-evolution/)
- [Conformance fixtures](conformance/)

## Repository Layout

```text
api/go/                 Reference Go host library and release utilities
clients/swift/          Apple SDK
clients/kotlin/         Android SDK
clients/react-native/   React Native bridge
conformance/            Shared contract fixtures
docs/                   Published docs site
extensions/synchro-core Shared Rust sync semantics
extensions/synchro-pg   PostgreSQL extension
```
