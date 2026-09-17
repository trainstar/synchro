# Synchro Release Process Implementation Plan

**Date:** 2026-09-14
**Target:** `v0.3.0` and later releases
**Status:** Implementation plan, not release certification
**Reviewed source:** `fcb931a804376c82bb98e4f90f3b76946dba4864`

## Outcome And Boundaries

The routine release has three maintainer actions:

1. Merge the release pull request.
2. Dispatch Release on `dev`.
3. Approve publication in the protected `release` environment.

Automation checks the exact commit, builds each distribution payload once, tests installation, publishes dependencies first, and verifies public consumption.

`RELEASE.md` becomes the only active release procedure.

This change removes duplicate execution and the documented 72-hour release requirement.
It does not require a new test framework, a complete test rewrite, or closure of unrelated repository cleanup.

No finite review or test suite proves the absence of unknown defects.
The process must produce evidence for declared behavior, not a statement of perfection.
Record execution time and maintainer actions before and after implementation.
Do not promise a measured speed improvement before that comparison exists.

## Design Authority

Use product requirements and failure risks to select proof.
Use official platform and registry rules to select publication controls.
Use repository code to identify existing implementation, missing connections, and safe deletions.

Current implementation output is not a correctness oracle.
Current specifications can contain contradictions.
Resolve a contradiction explicitly before changing its behavior or expected result.
Record product decisions in the applicable issue.

Preserve a test because its assertions protect required behavior, not because its name or count looks useful.
Each behavior has one authoritative proof home for each independent implementation.
Shared scenarios across independent engines are not duplicate proof.
Package smoke verifies installation, not the complete semantic corpus again.
Do not require every proof type or a mutant for every requirement.
Choose proof depth from the risk of an undetected failure.

## Support And Distribution

The existing support matrix remains the single machine-readable support declaration.

| Surface | Supported release boundary | Distribution |
|---|---|---|
| Server | PostgreSQL 18, Linux x64 | Extension archive with control file and generated SQL |
| Direct tools | Linux x64 | `synchrod-pg` and `synchro-seed` binaries |
| Go host library | Go 1.25 minimum, Linux x64 host | `github.com/trainstar/synchro/api/go` at `api/go/v<version>` |
| Swift | iOS 16 and current stable iOS | Root Swift package at `v<version>` |
| Kotlin | Android API 24 and current stable Android | `fit.trainstar:synchro` on Maven Central |
| React Native | React Native 0.83.x, current stable iOS and Android | `@trainstar/synchro-react-native` on npm |
| Apple pod dependency | Exact release version | Root Git-backed `Synchro.podspec` |

Use Ubuntu 24.04 LTS x64 with official PGDG PostgreSQL 18 packages as the initial prebuilt-server environment.
PGDG supports this combination.
This is a narrow binary-support decision, not a claim about every Linux distribution.
Record actual shared-library dependencies from the artifact.
Do not invent a libc minimum.

Resolve current stable versions from vendor releases before candidate creation.
Freeze exact OS, runtime, architecture, toolchain, and dependency versions for that candidate.
A preinstalled runner image does not establish the current stable runtime.
Missing required runtimes block the affected gate.

Keep macOS as an Apple toolchain and Swift semantic-test host.
Use host-local PostgreSQL 18 and the host-local adapter as real Apple test dependencies.
Do not certify or publish a macOS PostgreSQL extension.
Android host selection does not add support cells.
Run semantics on representative hosts and package boundaries on the declared minimum/current cells.
Do not create an OS, architecture, database, and dependency cross-product.

Test declared dependency floors and current versions inside supported ranges through clean consumers.
Keep iOS 16, Android API 24, and GRDB 7.x as the native package floors.
Require the Swift 6 toolchain because GRDB 7.0.0 requires it.
The current Swift 5.9 manifest declaration does not establish dependency compatibility.
Do not change Swift language mode as part of release-process cleanup.
Align the unbounded root GRDB pod dependency with the Swift package's supported major version.
React Native 0.83.0 requires Node 20.19.4 or later and iOS 15.1 or later.
Synchro's iOS 16 minimum therefore controls its bridge deployment floor.
Declare the Node floor in npm metadata instead of promising unspecified Node 20 compatibility.
Keep one supported React Native architecture, the existing TurboModule path.

There is no CocoaPods trunk publication.
The RN iOS installation instructions must include the Git-backed Synchro and GRDB dependencies.
The RN Android package must resolve the matching Maven release without workspace substitution.
Publish both `v<version>` and `api/go/v<version>` at the same source commit.
The root tag alone does not version the Go subdirectory module.
Verify Go module resolution and host startup from a clean consumer without `replace` or workspace overrides.

Do not publish conformance runners, generated test seeds, internal receipts, or unsupported server binaries.
Generate portable test seeds from the isolated server instance that consumes them.

## Four Gates

| Gate | Required outcome |
|---|---|
| Candidate | Required source CI passes for the exact release commit |
| Package | Sealed distributions pass clean installation and lifecycle checks |
| Publish | One approval authorizes dependency-ordered publication |
| Public | Anonymous consumers resolve the intended versions and pass live smoke |

Release consumes the successful candidate CI result.
It does not repeat that commit's completed source suites.
A different merge commit requires its own result.

Bind results to repository, source SHA, workflow identity, run ID, attempt, commands, and resolved environment.
Reject stale results, missing jobs, failed jobs, skipped required work, and incomplete result files.
Derive exact required artifact, cell, operation, and destination sets from the canonical declarations.
The final verifier rejects missing or extra records independently of earlier workflow sequencing.
Keep original failures and their diagnoses.
An unexplained retry-only pass is not release evidence.

## Testing Without Duplicate Systems

| Proof | Required CI | Scheduled extension |
|---|---|---|
| Deterministic rules and shared vectors | Unit and contract tests | None |
| Real server and native behavior | Existing integration and authored scenarios | Additional environments only after support approval |
| Critical faults and durability | Deterministic failures at relevant transaction boundaries | Wider fault combinations |
| Stateful sequences | Bounded seeded execution against real implementation and independent invariants | More seeds and longer workloads |
| Negative controls | Critical assertion controls and gate-integrity controls | Broad mutation search |
| Endurance and capacity | Existing deterministic safety bounds | Bounded endurance and capacity exploration |

Pull requests run fast source checks.
Every push to `dev` runs the complete candidate set, including real native and both bridge paths.
Required correctness does not depend on the scheduled workflow running first.

Retain the existing shared scenarios and test drivers.
Trace each required assertion to its executable consumer before deleting or moving a test.
Do not create a second scenario catalog, coverage database, reference engine, or receipt framework.

The risk map must cover these connected behaviors:

- Offline SQL edits, capture suppression, same-row dependency normalization, and durable intent.
- Push conflicts, exact replay, response loss, later local edits, and canonical reconciliation.
- Atomic pull apply, cursor commits, terminal checksums, and overlapping-scope provenance.
- Rebuild interruption, snapshot boundaries, concurrent writes, pruning, and finality.
- Schema changes, interrupted migration, queued intent, scope changes, and retention expiry.
- WAL ordering, acknowledgment, replay, poison, reset, and worker recovery.
- Authentication, role boundaries, strict wire decoding, readiness, limits, and redaction.
- Empty startup, portable seeds, authenticated continuation, cancellation, stop, background, and restart.

Use authored expected values and independent invariants.
Compare observable rows, outcomes, queues, cursors, and durable state.
Test presence, a model-only run, and implementation-generated expectations do not prove production behavior.
Minimize every discovered production defect into permanent regression coverage.

Retain structured result parsers and their negative controls.
Required gates reject zero matches, filtered execution, missing results, skips, and substituted runtimes.
Do not replace these checks with shell exit status or a handwritten coverage claim.

### Performance And Endurance

New performance budgets and benchmark design are a separate work item.
They do not block this release-process implementation.
`RELEASE.md` must distinguish enforced checks from deferred performance work.

Retain correctness and request-bound assertions inside performance-named targets.
Separate these assertions from timing characterization before changing their cadence.
Do not delete the existing R1 benchmark merely because it compares measured baselines.
Remove its separate release-blocking status.
Keep timing comparison available as scheduled or manual performance evidence.
Retain its required correctness assertions through their semantic proof homes.

The later performance policy covers initial sync, incremental pull, backlog push, rebuild, and restart.
It must state workload sizes, environment, sampling, noise handling, and user-relevant latency, memory, storage, and throughput limits.
Measured release baselines support regression detection.
User requirements establish acceptable absolute limits.
Neither kind of limit is a correctness oracle.

Schedule wider seeded and endurance runs without a release timer.
Record the seed and operation schedule before execution.
A known supported-contract failure blocks release through its issue and deterministic regression.
No release waits 72 hours.

## Supported Upgrades And Recovery

`v0.3.0` is the first supported Protocol 3 baseline.
Do not promise migration from `0.1.x` or experimental server databases.
Reject unsupported state explicitly without deleting local writes.
Keep all historical tags, assets, and package versions unchanged.

For supported releases, preserve durable application data and queued intent across SDK upgrades.
Test the previous supported package and each distinct supported stored-data format.
Use the old package to create the database before opening it with the candidate.
Include pending writes, rejections, partial rebuilds, and interrupted schema migration.

Test server-first rolling upgrades with the previous supported clients.
Test new clients against the preceding supported server within the declared compatibility window.
During `0.x`, patch releases preserve the minor line's public API and storage compatibility.
A breaking minor release requires an explicit compatibility window and data-preserving migration procedure before its candidate starts.
Do not infer this project's support promise from SemVer's permissive `0.x` rule.

`v0.3.0` requires fresh extension installation, not an update from an unsupported preview.
The next supported server release requires versioned update SQL and `ALTER EXTENSION UPDATE`.
Keep fresh-install SQL generation separate from update scripts.
Prove preservation of authoritative rows, registrations, ledgers, checkpoints, and worker recovery.
Document required PostgreSQL restarts, configuration, roles, backup, and restore steps.
Do not require Android to verify a server installation.

Do not promise an untested binary downgrade after a storage migration.
Recovery uses a verified backup before new writes, or a forward repair that preserves subsequent writes.
Never drop synchronization state merely to make an upgrade pass.
Preview-code removal and broad migration cleanup are not prerequisites for this workflow change.

## Implementation Sequence

Complete each prerequisite before its dependent step.
Parallelize independent host jobs, not changes to a shared unverified contract.

### 1. Establish One Procedure

**Files:** `RELEASE.md`, `RELEASING.md`, `README.md`, `AGENTS.md`, support matrix, documentation, and documentation validators.

1. Write the complete `RELEASE.md` procedure.
2. Put the three routine maintainer actions first.
3. Specify one-time setup, version preparation, support, gates, approval, publication, public checks, and recovery.
4. Include the upgrade and deferred-performance policies above.
5. Replace `RELEASING.md` with a link to `RELEASE.md`.
6. Remove competing release procedures from specifications 06 and 07 and verification documentation.
7. Preserve protocol semantics and shared scenario definitions outside the release procedure.
8. Replace duplicated support claims with references to the support matrix.
9. Update validators and self-tests that currently require old release schemas or hard-coded version values.
10. Mark historical release plans and trackers as superseded procedure.
11. Preserve unresolved findings in the existing issue ledger.
12. Keep `VERSION` authoritative across package metadata, SQL filenames, and release checks.
13. Remove hard-coded release versions and obsolete client installation requirements.

**Acceptance:** `make docs-build` passes, including contract verification.
Active release instructions and links resolve to one procedure.
Deleting every historical report is not an acceptance criterion.

### 2. Configure External Controls

**Depends on:** Step 1.

1. Protect `dev` with pull requests, resolved conversations, and the required pull-request aggregate.
2. Protect release tags against unauthorized creation, movement, and deletion.
3. Enable GitHub immutable releases.
4. Create the protected `release` environment with one publication approval.
5. Restrict signing material to trusted `dev` staging jobs in a separate signing environment.
6. Restrict publication credentials to publication jobs.
7. Default workflow permissions to read-only.
8. Pin action commits, tool versions, and supported build environments.
9. Enable required workflows, secret scanning, push protection, and dependency security updates.
10. Cover actual Go, Cargo, Swift, Gradle, npm, and Actions dependency locations.
11. Block unresolved applicable Critical and High security findings.
12. Configure npm trusted publishing for this workflow and environment.
13. Require npm two-factor authentication and disable token publication.
14. Verify the Maven account, namespace, Portal token, signing key, and public-key distribution.
15. Verify the GitHub-hosted Ubuntu, macOS, and minimum/current mobile runtimes.

Missing credentials or external approval block the affected step explicitly.
They never produce a skipped success.
The normal publication approval does not authorize a bypass of failed tests.
CI rewiring can proceed while external setup runs.
Its integrated gate requires the corresponding host and repository controls.

### 3. Connect Existing Tests To Required CI

**Depends on:** Step 1 for implementation and Step 2 for integrated execution.
**Files:** `Makefile`, CI/security workflows, scenario ownership, test-result parsers, and existing host drivers.

1. Reuse existing requirement IDs, proof mappings, vectors, scenario runners, and meaningful negative controls.
2. Repair demonstrated coverage gaps without duplicating existing native journeys.
3. Add stable `CI / pull-request` and `CI / candidate` aggregate jobs.
4. Keep security workflows as reusable CI jobs with scheduled rescans.
5. Remove duplicate push and pull-request triggers from those reusable workflows.
6. Include iOS bridge correctness in candidate CI instead of scheduled-only validation.
7. Keep critical deterministic faults and bounded real stateful execution in candidate CI.
8. Move only wider exploration, broad mutation search, timing characterization, and endurance to scheduled validation.
9. Keep focused Make targets without adding redundant host wrappers.
10. Run Apple clients with host-local PostgreSQL and the adapter on GitHub-hosted macOS.
11. Run the certified server, Android, and React Native Android on GitHub-hosted Ubuntu.
12. Reuse one host-local provisioner for control, reset, fault operations, per-run isolation, and cleanup.
13. Reject preview databases without converting or deleting pending writes.
14. Preserve current Protocol 3 schema journals and storage transitions.
15. Remove stale mutation selectors without removing required assertion controls.

Use `synchro-local-postgres` as the single fixture mechanism on Ubuntu and macOS.
Each job owns one temporary database, adapter, credentials, and cleanup lifecycle.

Apple semantic and package checks use the real host-local extension and adapter.
This internal dependency does not add a macOS server support claim.

Ubuntu server checks exercise the sealed Linux extension, adapter, and seed tool.
Android and React Native Android provide representative client-to-certified-server end-to-end proof.
Shared authored scenarios and wire vectors bind Apple behavior to the same protocol.

Do not add cross-host forwarding or test every client and server-host combination.
Those combinations exercise no additional Synchro code path.
Package checks test exact client distributions.
The separate Linux package cell tests exact server distributions.

**Acceptance:** Each required engine and bridge runs against real dependencies.
Negative controls reject missing tests, failed children, wrong environments, and incomplete results.
Candidate CI passes on a clean commit.

### 4. Stage And Verify Exact Distributions

**Depends on:** Steps 2 and 3.
**Files:** Artifact staging script, artifact inventory, Make targets, `verification/`, package metadata, and consumer fixtures.

1. Evolve the existing artifact staging and verification implementation.
2. Add `make release-stage VERSION=x.y.z`.
3. Add `make release-verify RELEASE_DIR=...`.
4. Stage final-version extension, tools, Go and Apple source inputs, npm tarball, and Maven bundle.
5. Include required package metadata, licenses, Maven signatures, and checksums before sealing.
6. Generate an SPDX JSON SBOM covering shipped components.
7. Generate provenance with supported GitHub and registry tooling.
8. Generate `release-manifest.json` and `SHA256SUMS` from the completed distribution inputs.
9. Persist the sealed candidate as an immutable Actions artifact before any publication operation.
10. Record its artifact ID, digest, originating run, and expiration.
11. Install only from sealed paths in clean consumer environments.
12. Recheck payload identity after testing and before publication.
13. Verify provenance signer, repository, workflow, source SHA, and subject hashes with official tools.
14. Validate the signed Maven bundle privately and remove that unpublished test deployment.

The manifest binds source, CI result, build inputs, dependencies, provenance, destinations, paths, sizes, and payload SHA-256 values.
It does not contain its own content hash.
Swift and pod distributions bind the exact source tree and package metadata.
The Go module binds its subdirectory source and required metadata to the same commit.
Consumer compilation is necessary and does not violate the build-once distribution rule.
Do not require GitHub-generated archive compression bytes to equal a local archive.

Use clean caches and dependency resolution for public checks.
Reject workspace links, `mavenLocal()`, local overrides, stale installed packages, and unexpected files.
Reuse existing consumers for install, connect, SQL write, push, pull, process kill, and resume.
Public consumer entry points must not import inspection SPI, proof annotations, or internal inspection modules.
Keep those inspection tools in semantic tests, not public installation proof.
Compare downloaded binaries, npm tarballs, and Maven payload files with the sealed manifest hashes.
Verify real data and durable outcomes, not only command success.
Include an unsigned iOS device build and an Android release build.
Simulator and debug builds do not establish release-configuration compatibility.
Exercise the packaged seed tool through seed generation and normal authenticated continuation.

Server installation must configure `shared_preload_libraries`, database identity, worker login, replication access, and runtime roles.
Restart PostgreSQL before checking worker readiness and committed-WAL delivery.
Exercise the packaged `synchrod-pg projection-bootstrap` command with distinct operator and worker credentials.
Retain generated-SQL drift checks and distinguish role-preserving reinstall from a version upgrade.

**Acceptance:** Changed bytes, extra files, wrong dependencies, and source substitution fail.
Verification and publication cannot rebuild a distribution.
Linux server smoke does not require a mobile emulator.

### 5. Publish And Resume Without Rebuilding

**Depends on:** Step 4.
**File:** `.github/workflows/release.yml` and focused publication helpers.

Use `workflow_dispatch`.
The normal dispatch has no skip controls.
An optional original run ID selects recovery instead of creating a new candidate.
Serialize publication with one repository-wide concurrency group.

**New candidate**

1. Freeze the selected `dev` head and validate its canonical `VERSION`.
2. Require the exact commit's successful candidate CI.
3. Require closure of applicable release blockers, not every open repository issue.
4. Reject an existing version that does not belong to this candidate.
5. Build, seal, persist, and verify distributions.
6. Wait for the single publication approval.
7. Recheck candidate identity, required results, credentials, and release blockers.
8. Create both immutable source tags at the candidate commit.
9. Create a draft GitHub release and attach every intended GitHub asset.
10. Publish the GitHub release with `make_latest: false`.
11. Verify anonymous GitHub downloads, Go module consumption, and tag-backed Apple dependencies.
12. Upload the signed Maven bundle with `USER_MANAGED`.
13. Record its deployment ID before requesting publication.
14. Require `VALIDATED`, publish that deployment, and verify public Maven files and consumption.
15. Publish the sealed npm tarball as specified in [RELEASE.md](../../../RELEASE.md#automated-release-sequence).
16. Verify public npm identity and clean iOS/Android consumer behavior.
17. Mark GitHub latest only after every required public check passes.

GitHub drafts cannot prove anonymous download access.
Public verification requires publication, so cross-registry publication is not an atomic transaction.
Do not describe partial publication as a completed release.

**Recovery**

| Observed state | Required action |
|---|---|
| No sealed candidate | Start a new candidate without claiming prior package verification |
| Sealed candidate, no tag | Restore original bytes and resume approved publication |
| Only one required source tag | Verify its commit and create the missing tag at that same commit |
| Tag only or partial draft assets | Verify tag identity and finish missing draft assets |
| GitHub public, registry missing | Keep non-latest status and publish remaining original payloads |
| Registry operation interrupted | Query its recorded operation and published content before retry |
| Maven upload outcome unknown | Recover or remove the unpublished Portal deployment before another upload |
| Published bytes match | Skip upload and repeat only incomplete public checks |
| Bytes, tag, source, or version differ | Stop and record the conflict |
| Original artifacts expired or disappeared | Stop recovery, never rebuild under an already published version |
| Published product defect | Warn users, retain immutable versions, and release a corrected patch |

Retain candidate artifacts for the configured 90-day Actions recovery window.
Persist operation identifiers separately from the sealed manifest.
Never edit that manifest to record progress.
Resume must work from the original run ID before a tag exists.
Reject missing evidence rather than manufacture replacement receipts.

**Acceptance:** Inject interruption at every publication boundary.
Resume tag-only, partial-upload, partial-registry, and public-check failures with identical payloads.
Wrong-source and wrong-byte attempts fail.
Do not test failure recovery by publishing disposable production versions.

### 6. Remove Replaced Machinery And Prove The Procedure

**Depends on:** Steps 1 through 5.

1. Remove repeated source suites from Release.
2. Remove obsolete `validation-check`, `phase-5-check`, and release receipt wrappers after replacement gates pass.
3. Keep focused Make targets and structured test-result parsing.
4. Remove old candidate/evidence schemas only after all live consumers migrate.
5. Update documentation validators, package consumers, inventories, and tests in the same migration.
6. Remove scheduled-only iOS workflow machinery after candidate CI owns its required behavior.
7. Retain useful benchmark code and unresolved defect evidence.
8. Remove plan-specific release instructions from active documentation and `AGENTS.md`.
9. Run an independent deletion and simplification review.
10. Run focused validation and the complete candidate gate on clean commits.
11. Run package verification and publication failure controls.
12. Reach the approval gate and cancel without changing tags, releases, or public registries.
13. Record elapsed time, duplicate executions, and required maintainer actions.

**Acceptance:** One procedure and one implementation of each check remain.
Required correctness still executes.
No dependent gate accepts missing work.
The first real release supplies public-install evidence through the normal procedure.
Publication failure controls supply interruption and recovery evidence.
Implementation completion and a successful public release are separate recorded outcomes.

Do not close product issues merely because a tracker or report was removed.
Do not require every R2/R3 cleanup item before replacing duplicate release orchestration.
Keep only demonstrated release blockers on the release path.

## Review Evidence

This review examined product, test, packaging, automation, and documentation areas.
Source inspection establishes implementation and wiring, not a fresh passing runtime result.
This was not a complete line-by-line defect audit.
Access to `clients/react-native/example/ios/.xcode.env` was denied.
That file was not inspected or accessed through another route.

| Finding | Evidence at the reviewed commit | Plan consequence |
|---|---|---|
| Source suites run twice | `.github/workflows/ci.yml:217-246`, `.github/workflows/release.yml:197-254` | Reuse exact-commit CI |
| Native response-loss proof already exists | `conformance/swift/push_response_loss.go:40-197`, `conformance/kotlin/push_response_loss.go:39-199` | Reuse, do not replace wholesale |
| Correctness uses performance-named targets | `Makefile:863-876`, `Makefile:926-953` | Split cadence by assertion purpose |
| Server smoke depends on Android | `Makefile:1627-1631` | Give server installation its own driver |
| Release publication ignores native dependency order | `.github/workflows/release.yml:1012-1073` | Publish native dependencies before npm |
| Old release schemas have live validators | `docs/scripts/verify-contract.mjs:32-43`, `docs/scripts/validators/ci-summary.mjs:1-16` | Migrate consumers before deletion |
| Support prose conflicts with Linux-only packaging | `README.md:45-62`, `docs/src/content/docs/reference/support-policy.mdx:19-29`, `.github/workflows/release.yml:954-965` | One support declaration |
| Current Android is hard-coded | `.github/workflows/release.yml:650`, `.github/workflows/release.yml:843` | Resolve vendor versions per candidate |
| Apple dependency ranges differ | `Package.swift:15`, `Synchro.podspec:14` | Align GRDB support and test consumer resolution |
| Manifest generation accepts incomplete smoke | `scripts/rc-artifacts.py:277-300`, `scripts/ci/test_rc_artifacts.py:50-66` | Verify exact required sets |
| Consumers use inspection APIs | `verification/consumers/swift/Sources/SynchroConsumer/main.swift:1-2`, `verification/consumers/react-native/App.tsx:3-5` | Add public-API-only entry points |
| Real bounded stateful proof exists | `conformance/blackbox/integration/soak_test.go:39-103`, `conformance/soak/runner.go:88-167` | Reuse in candidate CI |
| Existing server staging omits the seed tool | `Makefile:608-627`, `Makefile:698-735` | Stage and exercise the existing seed artifact |
| Server has an operator command | `api/go/cmd/synchrod-pg/main.go:179-230` | Include projection-bootstrap package smoke |
| Go is a subdirectory module | `api/go/go.mod:1-4` | Publish `api/go/v<version>` and verify Go consumers |

GitHub API inspection on 2026-09-14 UTC found unprotected `dev`, no rulesets, and no release environment.
It also found disabled secret scanning, push protection, and dependency security updates.
CodeQL and React Native iOS Validation reported `disabled_inactivity`.
Verify actual control activation during implementation, not only checked-in configuration.

## Engineering Basis

- [Google SRE release engineering](https://sre.google/sre-book/release-engineering/): shared test targets, exact revisions, automation, and packaged-system verification.
- [Google SRE testing for reliability](https://sre.google/sre-book/testing-reliability/): risk-based layered tests, failure coverage, and testing cost.
- [Semantic Versioning](https://semver.org/): explicit public API and immutable published versions.
- [PostgreSQL extension packaging](https://www.postgresql.org/docs/18/extend-extensions.html): installation, control files, update scripts, and durable database changes.
- [PostgreSQL Ubuntu support](https://www.postgresql.org/download/linux/ubuntu/): supported initial Linux binary environment.
- [Go module repository conventions](https://go.dev/doc/modules/managing-source): subdirectory version tags and clean source consumption.
- [GitHub immutable releases](https://docs.github.com/en/code-security/concepts/supply-chain-security/immutable-releases): attach assets before publication and preserve bytes.
- [GitHub release API](https://docs.github.com/en/rest/releases/releases): draft visibility and latest-release controls.
- [GitHub artifact retention](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/download-workflow-artifacts): bounded recovery storage.
- [GitHub artifact attestations](https://docs.github.com/en/actions/how-tos/secure-your-work/use-artifact-attestations/use-artifact-attestations): standard provenance generation and verification.
- [npm trusted publishing](https://docs.npmjs.com/trusted-publishers/): scoped OIDC publication and provenance.
- [React Native 0.83 package requirements](https://github.com/facebook/react-native/blob/v0.83.0/packages/react-native/package.json) and [Apple deployment requirements](https://github.com/facebook/react-native/blob/v0.83.0/packages/react-native/scripts/cocoapods/helpers.rb): dependency and platform floors.
- [GRDB 7.0.0 manifest](https://github.com/groue/GRDB.swift/blob/v7.0.0/Package.swift): required Swift 6 toolchain.
- [Maven deployment bundles](https://central.sonatype.org/publish/publish-portal-upload/) and [Publisher API](https://central.sonatype.org/publish/publish-portal-api/): signed payloads and explicit publication state.
- [Android benchmarks in CI](https://developer.android.com/topic/performance/benchmarking/benchmarking-in-ci): measured regression baselines and noise control.
