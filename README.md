<p align="center">
  <img src="docs/public/logo.svg" alt="Synchro" width="320">
</p>

# Synchro

[![Release](https://img.shields.io/github/v/release/trainstar/synchro)](https://github.com/trainstar/synchro/releases/latest)
[![CI](https://github.com/trainstar/synchro/actions/workflows/ci.yml/badge.svg?branch=master&event=push)](https://github.com/trainstar/synchro/actions/workflows/ci.yml?query=branch%3Amaster)

Synchro synchronizes PostgreSQL data with local SQLite databases in native applications.
Applications can read and write local data offline, then synchronize through an authenticated HTTP adapter.

PostgreSQL owns server synchronization through the Synchro extension.
Swift and Kotlin own local storage, change capture, durable queues, and synchronization.
React Native wraps those native clients.
The Go adapter handles HTTP and authentication without a separate synchronization service.

## Get started

Follow the [first-sync tutorial](https://trainstar.github.io/synchro/getting-started/quickstart/).
It connects a client, changes a note offline, and verifies the change on the server and another client.

- [Set up PostgreSQL and the adapter](https://trainstar.github.io/synchro/getting-started/server-setup/)
- [Install and initialize a client](https://trainstar.github.io/synchro/clients/consumption/)
- [Configure authentication](https://trainstar.github.io/synchro/architecture/auth-integration/)

These guides describe the Synchro `0.3.1` source and package interfaces.
Before that version is published, use the documented local-consumer installation.
Do not substitute an older published package into the current tutorial.

## Supported environments

| Component | Supported environment |
| --- | --- |
| PostgreSQL extension and Go tools | PostgreSQL 18, Ubuntu 24.04, Linux x64 |
| Swift client | iOS 16 and current stable iOS, Swift 6 toolchain |
| Kotlin client | Android API 24 and current stable Android |
| React Native bridge | React Native 0.83.x, current stable iOS and Android |

macOS hosts Apple development and validation. It is not a supported PostgreSQL deployment target.
See the [support policy](https://trainstar.github.io/synchro/reference/support-policy/) for dependency constraints and support boundaries.

## Documentation

| Goal | Guide |
| --- | --- |
| Understand component responsibilities | [Architecture](https://trainstar.github.io/synchro/architecture/overview/) |
| Assign private and shared data | [Scope modeling](https://trainstar.github.io/synchro/architecture/scope-modeling/) |
| Bundle an initial SQLite database | [Portable seeds](https://trainstar.github.io/synchro/architecture/portable-seeds/) |
| Use the client SQL APIs | [Application SQL limits](https://trainstar.github.io/synchro/clients/application-sql/) |
| Configure and operate the server | [Configuration](https://trainstar.github.io/synchro/operations/configuration/) |
| Implement or inspect protocol behavior | [Wire protocol](https://trainstar.github.io/synchro/spec/01-wire-protocol/) and [client contract](https://trainstar.github.io/synchro/spec/02-client-contract/) |
| Understand validation evidence | [Testing evidence](https://trainstar.github.io/synchro/verification/overview/) |
| Prepare or recover a release | [RELEASE.md](RELEASE.md) |

Scopes are server-defined. Clients cannot supply arbitrary replication predicates.
Synchro does not provide a browser synchronization client or support server databases other than PostgreSQL.

## Development and contributions

The `Makefile` is the supported build, lint, and test entry point.
Use the [local validation instructions](https://trainstar.github.io/synchro/getting-started/development/) before running integration tests.
Tests require disposable fixtures, not an application database.
The [client overview](clients/README.md) identifies the native and bridge packages.

Report reproducible problems through [GitHub Issues](https://github.com/trainstar/synchro/issues).
Include the version, platform, reproduction steps, and expected result.
Do not include credentials or private application data.
Keep pull requests focused and run the applicable Make checks.
Target ordinary development pull requests at `dev`.
`master` is the stable default branch and accepts checked release promotions and stable hotfixes.
Return stable hotfixes to `dev` so subsequent releases retain their corrections.

## License

Synchro uses the [MIT License](LICENSE).
