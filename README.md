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
