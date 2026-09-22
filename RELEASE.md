# Release Synchro

## Routine Actions

1. Merge the release pull request into `dev`.
2. Dispatch the Release workflow for `dev`.
3. Approve the protected `release` environment.

The workflow builds and tests sealed artifacts before approval.

The `publish` job performs every tag and public operation after approval.

## One-Time Setup

Configure protected `dev` and release tags.

Configure `release-signing` without required reviewers.
Store only `GPG_PRIVATE_KEY` and `GPG_PASSPHRASE` in that environment.

Configure the protected `release` environment with one required approval.
Store only `MAVEN_CENTRAL_USERNAME` and `MAVEN_CENTRAL_PASSWORD` there.

Configure npm trusted publishing for `.github/workflows/release.yml` and the `release` environment.
Do not configure an npm publication token.

The `publish` job is the only job that uses the protected `release` environment.
Do not configure approval rules on `release-signing`.

Use the required GitHub-hosted macOS and Ubuntu runners.
Install the required mobile runtimes through the workflow.

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

Candidate Apple tests use PostgreSQL 18 on their GitHub-hosted macOS runners.

Release Apple package cells use the same internal macOS PostgreSQL fixture.
They test exact sealed client artifacts through the source adapter.
The macOS PostgreSQL fixture is not a supported server output.

The Ubuntu server package cell verifies the exact sealed Linux server artifacts.
It also runs packaged projection bootstrap with separate operator and worker credentials.

Android and React Native Android run against Linux PostgreSQL on Ubuntu.
They provide representative client-to-Linux end-to-end proof.

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
| Package | Exact sealed distributions pass connect, push, pull, kill, and resume on each required support cell. |
| Publish | One approval authorizes dependency-ordered publication. |
| Public | Public bytes match the sealed payloads, and clean consumers resolve and build from public coordinates. |

Candidate CI owns source correctness. Release does not run completed source suites again.

Each React Native Candidate job runs its smoke suite and all 14 authored journeys.
Each journey uses a fresh local PostgreSQL instance.
The corpus rejects missing scenario runners before execution.

Release builds distributions once. Package checks and publication use the identical sealed payloads.

Maven bundles contain only version-specific artifacts, signatures, and checksums.
Maven Central owns repository-level version metadata.

Artifact transfers can remove file permissions. Verification checks payload bytes and binary format.
Execution boundaries restore executable permissions after verification.

Candidate Swift and React Native iOS jobs use independent host-local PostgreSQL instances.

The `publish` job waits for `package-gate` and the protected `release` approval.
No attestation, tag, release, or registry write occurs before that job starts.
Package validation can run while its acceptance issues remain open.
Publication requires zero open issues in the release milestone.

The `release-signing` job creates detached signatures only.
It has no publication credentials and performs no public operation.

The Package gate owns the full five-operation lifecycle.
The consumer's terminal result owns phase completion. An empty queue alone cannot certify success.
Native and React Native consumers report resume success only after stop and close finish.

The Public gate does not repeat that lifecycle.
It verifies public GitHub assets, Maven files, and the npm tarball against sealed identities.
Public Maven observation contains only public file identities.
Private Central deployment identifiers and states remain in the protected publisher's operation record.
It then resolves and builds clean Go, Swift, Kotlin, and React Native consumers from public coordinates.
The gate fails when it cannot prove a public identity.
A successful build never replaces missing byte or integrity proof.

The manifest records candidate environment resolution in `release-manifest.json`.

## Automated Release Sequence

1. Verify the selected `dev` commit and Candidate CI result.
2. Build, seal, hash, and verify each distribution once.
3. Run clean package installation and lifecycle checks.
4. Complete every Package-gate cell.
5. Wait for the protected `release` environment approval.
6. Recheck the approved candidate and sealed identity.
7. Attest the sealed files with the exact sealed release manifest.
8. Create immutable `v<version>` and `api/go/v<version>` tags.
9. Publish GitHub assets without marking them latest.
10. Verify source and asset access.
11. Publish Maven and verify public consumption.
12. Publish npm directly under `latest` through trusted OIDC.
13. Verify the exact public npm bytes, provenance, and clean React Native builds.
14. Mark GitHub latest after all public checks pass.

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
Recovery never rebuilds or republishes an existing package version.

Before publication, source or dependency corrections require a new candidate.
Retain the failed candidate unchanged and repeat every required gate.
Its package results do not certify the new candidate.

## References

- [Support policy](docs/src/content/docs/reference/support-policy.mdx)
- [Testing evidence](docs/src/content/docs/spec/07-release-verification.mdx)
- [Release-process plan](docs/superpowers/plans/2026-09-14-synchro-release-process.md)
