# Synchro Client SDKs

Swift and Kotlin own local SQLite, mutation capture, durable queues, scheduling, and server-state application.
React Native exposes these native engines through a TurboModule bridge.
PostgreSQL owns authoritative server-side synchronization.

## SDKs

| SDK | Implementation |
| --- | --- |
| [Swift](swift/) | GRDB and the native Apple sync engine |
| [Kotlin](kotlin/) | Android SQLite APIs and the native Android sync engine |
| [React Native](react-native/) | Bridge over the Swift and Kotlin engines |

## Maintained Guides

- [Client SDK overview](../docs/src/content/docs/clients/overview.mdx)
- [Client consumption](../docs/src/content/docs/clients/consumption.mdx)
- [Application SQL limits](../docs/src/content/docs/clients/application-sql.mdx)
- [Client contract](../docs/src/content/docs/spec/02-client-contract.mdx)
- [Architecture overview](../docs/src/content/docs/architecture/overview.mdx)
- [Support policy](../docs/src/content/docs/reference/support-policy.mdx)

## Local Checks

Run the supported Make targets from the repository root.

| Surface | Unit tests |
| --- | --- |
| Swift | `make test-swift-unit` |
| Kotlin | `make test-kotlin-unit` |
| React Native | `make test-rn-unit` |

Use the [testing evidence guide](../docs/src/content/docs/spec/07-release-verification.mdx) for integration and platform checks.
Use [RELEASE.md](../RELEASE.md) for release operations.
