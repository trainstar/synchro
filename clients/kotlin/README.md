# Synchro Kotlin SDK

The Kotlin SDK uses Android SQLite APIs for local storage and mutation capture.
Its native sync engine owns durable queues, scheduling, and server-state application.
React Native uses this engine through its Android bridge.

## Maintained Guides

- [Client SDK overview](../../docs/src/content/docs/clients/overview.mdx)
- [Client consumption](../../docs/src/content/docs/clients/consumption.mdx)
- [Application SQL limits](../../docs/src/content/docs/clients/application-sql.mdx)
- [Client contract](../../docs/src/content/docs/spec/02-client-contract.mdx)
- [Architecture overview](../../docs/src/content/docs/architecture/overview.mdx)
- [Support policy](../../docs/src/content/docs/reference/support-policy.mdx)

## Local Checks

Configure `ANDROID_HOME` for the Android SDK and `ANDROID_JAVA_HOME` for JDK 17.
Run these targets from the repository root:

```sh
make build-kotlin-library
make test-kotlin-unit
```

Use the [testing evidence guide](../../docs/src/content/docs/spec/07-release-verification.mdx) for integration and device checks.
Use [RELEASE.md](../../RELEASE.md) for release operations.
