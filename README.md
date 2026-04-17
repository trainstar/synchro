<p align="center">
  <img src="docs/public/logo.svg" alt="Synchro" width="320">
</p>

<p align="center"><img src="https://github.com/trainstar/synchro/actions/workflows/ci.yml/badge.svg?branch=dev" alt="CI">&nbsp;<img src="https://github.com/trainstar/synchro/actions/workflows/codeql.yml/badge.svg?branch=dev" alt="CodeQL">&nbsp;<img src="https://github.com/trainstar/synchro/actions/workflows/dependency-review.yml/badge.svg?branch=dev" alt="Dependency Scan">&nbsp;<img src="https://github.com/trainstar/synchro/actions/workflows/docs.yml/badge.svg?branch=dev" alt="Docs"></p>

# Synchro

Synchro is a sync stack for apps that keep a real SQLite database on device and PostgreSQL on the server.

It combines a PostgreSQL extension, a thin Go HTTP adapter, native Swift and Kotlin SDKs, and a React Native bridge over those native SDKs.

The goal is straightforward: keep sync semantics close to PostgreSQL, keep the adapter thin, and let client apps work against local SQLite instead of a remote-first cache.

## When Synchro Fits

Use Synchro when:

- your app treats local SQLite as the primary on-device data store
- you need deterministic selective sync with server-defined scope assignment
- you want synced CRUD, rebuild, and schema evolution to be part of the platform contract
- you want PostgreSQL to own sync behavior instead of reimplementing it in product API handlers

Synchro is not the right fit when:

- you need support for a server database other than PostgreSQL
- you want a browser-first sync client instead of native SQLite clients
- you want application code to define ad hoc sync rules on each request

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
- `api/go`: HTTP adapter, auth integration, and transport translation
- `clients/swift`: native Apple SDK
- `clients/kotlin`: native Android SDK
- `clients/react-native`: React Native bridge over the native SDKs
- `conformance/`: shared executable contract fixtures
- `docs/`: published docs site and specification

## Core Capabilities

- deterministic server-defined scopes
- synced create, update, delete, pull, and rebuild flows
- schema evolution with explicit client actions
- portable seed database generation for warm starts
- native SQLite change capture on clients
- thin adapter auth integration with either trusted upstream auth or direct JWT validation

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
- `synchrod-pg` adapter binary
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
api/go/                 Go HTTP adapter and release utilities
clients/swift/          Apple SDK
clients/kotlin/         Android SDK
clients/react-native/   React Native bridge
conformance/            Shared contract fixtures
docs/                   Published docs site
extensions/synchro-core Shared Rust sync semantics
extensions/synchro-pg   PostgreSQL extension
```
