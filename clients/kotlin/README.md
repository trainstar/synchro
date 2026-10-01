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

## Atomic Write Transactions

`atomicWriteTransaction` runs one write transaction. The server applies all of its synced mutations or none of them.

```kotlin
import com.trainstar.synchro.SynchroClient
import com.trainstar.synchro.SynchroError

fun publishReview(client: SynchroClient, noteID: String, reviewID: String) {
    try {
        client.atomicWriteTransaction { transaction ->
            transaction.execute(
                "UPDATE notes SET body = ? WHERE id = ?",
                arrayOf("Reviewed", noteID),
            )
            transaction.execute(
                "INSERT INTO notes (id, owner_id, body) VALUES (?, ?, ?)",
                arrayOf(reviewID, "alice", "Review of $noteID"),
            )
        }
    } catch (failure: SynchroError.AtomicGroupInvalid) {
        println("Atomic group rolled back: ${failure.reason}")
    }
}
```

An invalid group throws `SynchroError.AtomicGroupInvalid` and rolls back the local transaction.
The atomic API requires the Synchro `0.4.0` extension and adapter.
See [atomic write transactions](../../docs/src/content/docs/clients/application-sql.mdx#atomic-write-transactions) for the validation rules, failed-group outcomes, and deployment order.

## Changes

See [CHANGELOG.md](../../CHANGELOG.md) for the changes in each release.

## Local Checks

Configure `ANDROID_HOME` for the Android SDK and `ANDROID_JAVA_HOME` for JDK 17.
Run these targets from the repository root:

```sh
make build-kotlin-library
make test-kotlin-unit
make test-kotlin
```

Use the [development guide](../../docs/src/content/docs/getting-started/development.mdx) for the repository-owned fixture prerequisites and Make sequence.

Use the [testing evidence guide](../../docs/src/content/docs/spec/07-release-verification.mdx) for integration and device checks.
Use [RELEASE.md](../../RELEASE.md) for release operations.
