# Release Synchro

## Routine Actions

1. Merge the release pull request into `dev`.
2. Dispatch the Release workflow for `dev`.
3. Approve the protected `release` environment.

The Release workflow performs all release operations after approval.

## One-Time Setup

Configure protected `dev` and release tags.

Configure the protected `release` environment with one approval.

Configure registry credentials, signing, and trusted publication.

Configure the required Linux fixture host and mobile runtimes.

Do not continue when a required control, credential, or runtime is unavailable.

## Prepare A Release

1. Run `make set-version VERSION=X.Y.Z`.
2. Run `make version-check`.
3. Confirm the support matrix in `conformance/support-matrix.json`.
4. Merge the release pull request into `dev`.
5. Confirm Candidate CI passed for that exact commit.
6. Dispatch Release from the `dev` head.

`VERSION` is the release version authority.

## Support And Compatibility

The support matrix is the sole machine-readable platform declaration.

The server cell is PostgreSQL 18 on Ubuntu 24.04 Linux x64.

Direct Go binaries support Linux x64.

Swift supports iOS 16 and current iOS with Swift 6.

Kotlin supports Android API 24 and current Android.

React Native 0.83.x supports current iOS and Android.

macOS hosts Apple and Swift validation. It is not a PostgreSQL support cell.

The Go module tag is `api/go/v<version>` at the same commit as `v<version>`.

Swift uses the root Git package. CocoaPods trunk publication does not occur.

React Native iOS uses exact Git-backed Synchro and GRDB dependencies.

Version `0.3.0` is the first supported Protocol 3 baseline.

Synchro does not promise migration from `0.1.x` or preview databases.

Patch releases preserve API and storage in a supported minor line.

A breaking minor requires an explicit compatibility window and data-preserving migration procedure.

## Gates

| Gate | Outcome |
| --- | --- |
| Candidate | Required source CI passes for the exact commit. |
| Package | Sealed distributions pass clean installation and lifecycle checks. |
| Publish | One approval authorizes dependency-ordered publication. |
| Public | Anonymous consumers resolve intended versions and pass live smoke. |

Candidate CI owns source correctness. Release does not run completed source suites again.

Release builds distributions once. Package checks and publication use the identical sealed payloads.

The manifest records candidate environment resolution in `release-manifest.json`.

## Automated Release Sequence

1. Verify the selected `dev` commit and Candidate CI result.
2. Build, seal, hash, and verify each distribution once.
3. Run clean package installation and lifecycle checks.
4. Wait for protected-environment approval.
5. Create immutable `v<version>` and `api/go/v<version>` tags.
6. Publish GitHub assets without marking them latest.
7. Verify source and asset access.
8. Publish Maven and verify public consumption.
9. Publish npm and verify public consumption.
10. Mark GitHub and npm latest after all public checks pass.

## Success Evidence

Record repository, source SHA, workflow run and attempt, commands, resolved environments, artifact hashes, and publication identifiers.

Reject missing jobs, skipped work, failed work, stale results, incomplete records, and unexplained retry-only passes.

Correctness checks currently enforce contract, integration, scenario, fault, zero-skip, seeded-stateful, and package-smoke behavior.

Synchro has no numeric performance guarantee. Performance budgets remain deferred.

## Failure And Recovery

| Observed state | Required action |
| --- | --- |
| No sealed candidate | Start a new candidate. |
| Sealed candidate without tags | Resume with original sealed bytes. |
| One source tag exists | Verify its commit and create the missing tag there. |
| GitHub published and a registry is missing | Keep non-latest status and publish the original payload. |
| Registry outcome is unknown | Query the recorded operation before retry. |
| Published bytes match | Skip upload and repeat incomplete public checks only. |
| Bytes, tag, source, or version differ | Stop and record the conflict. |
| Original artifacts expired | Stop. Never rebuild an existing release version. |
| Published defect | Retain immutable artifacts and release a corrected patch. |

Never move tags or replace published bytes.

## References

- [Support policy](docs/src/content/docs/reference/support-policy.mdx)
- [Testing evidence](docs/src/content/docs/spec/07-release-verification.mdx)
- [Release-process plan](docs/superpowers/plans/2026-09-14-synchro-release-process.md)
