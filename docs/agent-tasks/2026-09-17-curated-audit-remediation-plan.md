# Curated Audit Remediation Plan

**Approved:** 2026-09-17.
**Scope:** All 53 findings in [the curated audit](2026-09-17-whole-repository-simplicity-review.md).
**Ledger:** #105. Do not create an issue per finding.
**Starting commit:** `b8709e3a65f3883b09cad2c7464cd4d5ef968dc9`.
**Current status:** 53 implemented, 51 accepted, two awaiting the evidence specified below.

## Binding rules

- Resolve all 53 findings. Do not silently drop or defer difficult items.
- Use four bounded implementation workers. Every worker uses Astra, without a fast variant.
- Use CLI-native task workers only. Do not invoke OpenCode or its dispatch seats.
- The primary owns shared verification, tooling, integration, GitHub writes, commits, and final acceptance.
- Give each worker exclusive file ownership and exact finding IDs.
- Use the existing audit evidence. Do not repeat a repository-wide audit.
- Preserve the current protocol, public behavior, data-integrity guarantees, and release procedure.
- Do not add a new framework, generic interpreter, second engine, or speculative abstraction.
- Correct behavior before consolidating it. Independent mechanical deletions can proceed immediately.
- Preserve unrelated worktree changes.
- Do not run Docker locally.
- Do not install or restore dependencies without a manifest change or a missing-dependency failure.
- Run language tests, builds, and lint through supported Make targets.
- Do not start the unsafe adapter lifecycle targets until T04 has been corrected.
- Serialize commands that share a PostgreSQL installation, fixture database, native build, emulator, simulator, or local package repository.
- Keep development checks distinct from acceptance evidence on a clean integrated commit.
- Every accepted correction needs its relevant positive case and failure-sensitive check.
- Cleanup needs verified consumers and the applicable compile or test checks, not invented replacement behavior.
- A required gate must reject failures, skipped work, and zero matching tests.
- Do not mark a finding resolved until its correction and applicable checks pass.
- If evidence contradicts a finding, report the exact conflict. Do not change correct behavior to satisfy the report.
- A real blocker applies to its named finding. Continue independent work.
- Keep #105 concise: addressed IDs, commits, exact results, and named blockers.
- Do not push, publish, create a pull request, or close an implementation issue without its required acceptance evidence.

`RELEASE.md` remains the authority for release operation.
This plan does not authorize publication or change approved release phase order.

## Exclusive ownership

| Lane | Owned files | Assigned findings |
| --- | --- | --- |
| Server and Go | `extensions/`, `api/go/` | R01, Go part of R02, R03, R09, R16, V01, V10, V11, C04, Rust dependency part of C05, C07, C11, T01, T02, T03, T11 |
| Swift | `clients/swift/` | Swift part of R02, R04, R07, R08, Swift part of R12, R13, Swift part of V12, Swift part of C06 |
| Kotlin | `clients/kotlin/`, except `clients/kotlin/README.md` | R05, R06, R11, Kotlin part of R12, Kotlin part of V12, C03, Kotlin part of C06 |
| React Native | `clients/react-native/` | R10, R14, R15, V13, C01, Turbo part of C05, T05 |
| Primary | `conformance/`, `verification/`, `scripts/`, `Makefile`, `.github/`, `docs/`, `clients/README.md`, `clients/kotlin/README.md`, local `AGENTS.md` | V02-V09, V14, V15, C02, Mermaid part of C05, C08-C10, T04, T06-T10 |

R02, R12, V12, C05, and C06 have multiple owned parts.
The primary accepts those findings only after every part and integrated check passes.

A worker must request a cross-owner change instead of editing another lane's files.
Shared conformance inputs and build wiring stay with the primary.
Workers must not regenerate another lane's artifacts or run broad formatters outside their ownership.

After a lane returns its implementation, the primary can transfer a bounded unused file set to that existing worker.
Record the transfer before editing. Keep at most four workers and one editor per file.
The primary retains design and acceptance ownership for transferred shared work.

## Sequence

1. Commit this plan and record all 53 IDs and owned parts in the execution ledger.
2. Perform one environment preflight. Reuse valid local toolchains, caches, and fixtures.
3. Start the four workers with the decided scope and file references.
4. Repair shared gate and observation defects while workers implement their owned corrections.
5. Review each complete worker diff. Resolve cross-lane interfaces centrally.
6. Commit coherent corrections in focused batches.
7. Run the affected full suites on the clean integrated commit.
8. Reopen failed corrections and rerun the affected checks. Do not restart the whole audit.
9. Reconcile all 53 IDs against their code, proof, and commit before declaring completion.

## Validation ownership

| Surface | Supported checks |
| --- | --- |
| Rust core | `make test-rust-core`, `make lint-rust-core` |
| PostgreSQL extension | `make test-rust-pg`, `make lint-rust-pg`, `make generate-pg-sql`, `make check-pg-sql` |
| Go adapter and seed tooling | `make test-adapter` with applicable `GO_TEST_PKGS` and `GO_TEST_ARGS`, plus `make lint-go` |
| Swift | `make test-swift-unit`, then affected extension-backed integration and scenario targets |
| Kotlin | `make test-kotlin-unit` with supported selectors, then affected real device and scenario targets |
| React Native | `make test-rn-unit`, `make lint-rn`, native parity, and affected real iOS and Android journeys |
| Shared conformance | Focused contract, vector, invariant, fault, driver, test-result, and blackbox Make targets |
| Tooling and docs | Existing focused release-helper, packaged-consumer, and contract checks, with targeted regressions for changed entry points |

The primary selects actual commands from the current Makefile.
Add or repair a narrowly required Make target before considering direct language test commands.
Do not substitute mocked native success, log text, field counts, or process liveness for the required outcome.

## Completion record

Each finding must retain:

- Its original ID.
- The correction and owned files.
- Its focused regression or deletion evidence.
- The clean commit used for acceptance.
- Applicable successful Make results.
- Any failed attempt and its final resolution.

This plan and #105 are the persistent authority if a session or worker context changes.
The session ledger tracks individual finding parts. It does not replace the committed plan or issue.

The user changed worker execution to CLI-native tasks on 2026-09-17.
Preserve partial edits from the stopped external workers. Review and validate them before acceptance.

## Integrated implementation decisions

- Swift adds local migration `synchro_v16_capture_storage_validation` to upgrade existing capture triggers.
- Swift's existing migration journal adds `relax_nullability` for atomic physical constraint changes.
- Go seeds retain their existing v1-v8 migration history and SQLite version 6.
- Each native installer upgrades its private seed copy before validation. Do not claim later native migrations already ran in exported seeds.
- Server-only soak marks in-memory client observations as reference-only. Such observations cannot establish native restart history.
- Python gates require executed tests and reject skips and expected failures.
- The adapter supervisor retains its actual child handle. Stop uses its private control identity instead of signaling a stored PID.
- Source checks for native scenarios use independent provisioned fixtures when previous tests change registrations or assignments.

Shared input construction deletes the three Go queue builders.
The Markdown fence fixture has two concrete consumers: the Go contract scanner and JavaScript documentation scanner.
Its new cases replace the incorrect short-closing-fence assumption instead of adding another Markdown parser.

## Acceptance results

All 53 findings have implementation corrections.
Acceptance is complete for 51 findings.
T05 and T06 remain open because their linked issues require additional evidence.
Issue #105 remains open until those conditions pass.

The implementation ends at `d13753e94dbb553df24e3974a74ffadfad221c17`.
All changes are local. No code was pushed, and no publication occurred.
T08 changed the intentionally ignored local `AGENTS.md` and its linked `CLAUDE.md`, not a tracked repository file.

### Accepted findings

The letters identify clean source commits in the next table.
The original audit retains each finding's correction and source references.

| Findings | Acceptance commit | Evidence |
| --- | --- | --- |
| R01, R02, R16, C04, C07, T02, T03, T11 | B | Full adapter suite and cross-native seed convergence |
| R03, V01, V10, V11, C11 | A | Rust and PostgreSQL suites, production-valid fixtures, and SQL comparison |
| R04, R07, R08, R13 | A | Swift capture, migration, retry, and observation regressions |
| R05, R06, R11, C03 | A | Kotlin transaction, capture, quoted DDL, and lifecycle regressions |
| R12, V12, C06 | A | Both native unit suites and production raw-input vector validation |
| R09, C02 | B | Full real native scenarios after required-null encoding corrections |
| R10, V13 | D | Complete smoke suites on both bridges, including malformed schema and timeout rollback |
| R14, R15 | A | React Native hook regressions |
| V02, V14 | D | Real checksum-corruption and checkpoint-preservation regressions |
| V03, V07, V08, V09, V15, C08, C09, T10 | A | Shared conformance, strict result controls, and authored Markdown cases |
| V04, V05, V06 | A | Original exchange controls, real soak, and replayable corruption detection |
| C01 | B | All 14 original journeys on each bridge |
| C10 | D | Packaged controls and fresh isolated Android, iOS device, and iOS simulator consumer builds |
| C05, T07, T08, T09 | A | Consumer checks, package metadata, maintained documentation, and local instruction inspection |
| T01, T04 | C | Actual release validators and six process-ownership controls |

### Clean source commits

| Key | Exact commit |
| --- | --- |
| A | `564bc4be2e02efbab0557ffbfa3c7e7b60858e9f` |
| B | `4545b4ffe22a3a170fc086abe41d80b3a91fb442` |
| C | `aff23799ad7c78f69f06101afc5f9d48101555e9` |
| D | `d13753e94dbb553df24e3974a74ffadfad221c17` |

These results apply to their named commits, not automatically to every later commit.
Dirty-worktree diagnostics are not acceptance evidence.

### Executed checks

| Commit | Make targets or command | Result |
| --- | --- | --- |
| A | `lint-conformance`, `test-conformance`, `test-soak-controls`, `test-docs-contract` | Shared suites and authored contract passed |
| A | `lint-go`, `lint-rust-core`, `lint-rust-pg`, `test-rust-core`, `build-check` | Lint and builds passed, with 71 Rust core tests |
| A | `test-rust-pg`, `check-pg-sql` | 246 PostgreSQL tests and generated-SQL equality passed |
| A | `test-swift-unit`, `test-kotlin-unit`, `test-rn-unit`, `lint-rn` | 271 Swift tests, 578 Kotlin executions, and 210 React Native tests passed |
| A | `test-kotlin-instrumentation` | Eight API24 device-local tests passed |
| A | `make soak SOAK_SEED=42 SOAK_DURATION=35s` | Real baseline and replayable corruption checks passed |
| A | `make -o verify-contract docs-build`, `version-check`, `release-pods-check` | 26 documentation pages and package metadata passed |
| B | `test-adapter`, `test-client-schema-identity` | 304 adapter test nodes and Go/Swift/Kotlin seed convergence passed |
| B | `test-swift-scenarios`, `test-kotlin-scenarios` | Full real suites passed with 14 Swift and 15 Kotlin test nodes |
| B | `test-rn-scenarios-ios`, `test-rn-scenarios-android` | Each platform passed all 14 journeys and reported 26 test nodes |
| C | `test-version-contract`, `test-ci-process-lifecycle`, `test-release-artifacts`, `test-release-publish`, `test-packaged-smoke-structure`, `test-docs-contract`, `version-check` | Release contract passed, with six lifecycle, 23 artifact, 36 publication, and 15 packaged controls |
| D | `test-rn-e2e-ios-smoke`, `test-rn-e2e-android-smoke`, each with `DETOX_ARGS=e2e/sync.test.ts` | Each complete smoke suite passed 22 tests |
| D | `make test-blackbox "GO_TEST_ARGS=-v -count=1 -p 1 -run 'TestRealNativeCaptureServerObservationSignals\|TestRealS03PullHydrationFailurePreservesCursors'"` | 162 harness tests, three observer tests, and both real regressions passed |
| D | `test-consumer-rn-android`, `test-consumer-rn-ios` | Fresh packaged Android release, iOS device, and iOS simulator consumers compiled |

Required structured test gates reported zero failures and skips.
React Native lint reported zero errors and three unchanged warnings.
Packaged builds used `.ignore/remediation/final-consumer-artifacts` and `.ignore/remediation/final-consumer-tmp`, with the configured Android SDK and JDK 17.
They prove prepublication builds, not public registry acceptance.
The default Go release-version tests also passed without Node at commit D.

### Remaining acceptance

| Finding | Existing issue | Required evidence |
| --- | --- | --- |
| T05 | #101 | A controlled same-coordinate Maven artifact must not replace the public dependency. Prepublication's exclusive repository does not prove this condition. |
| T06 | #94 | Hosted workflow results must demonstrate independent push runs, PR cancellation, and queued-run behavior. No push is authorized. |

The earlier session count of 53 verified findings was too broad.
Issue reconciliation exposed these conditions and the missing packaged-consumer builds.
The packaged builds subsequently passed at commit D.
The corrected count is 51 accepted findings, with two implemented findings awaiting acceptance.

Issues #93, #98, #100, #102, and #103 have complete evidence and are closed.
Issues #94, #101, and #105 remain open.
The separate oracle-replacement work in #36 remains unchanged.

### Retained failures and corrections

- The initial worker provider returned HTTP 429. Native workers preserved and completed the partial changes.
- Initial consolidation checks exposed compile, parser, import, and test-setup failures. Corrections passed subsequent checks without weakening production rules.
- Initial native scenarios rejected omitted null cursors. Swift and Kotlin now encode the required members.
- Initial iOS smoke used an incompatible generated seed. Rebuilding with the relational seed restored the full suite.
- Initial Android smoke queried reserved SQLite metadata. The corrected assertion uses the public creation operation without weakening the SQL guard.
- CI review found an unintended Node dependency in default Go tests. The actual cross-tool proof now runs in its dedicated metadata gate.
- Seed refresh rejected an existing WAL sidecar before starting the adapter. The closed seed family remains archived.

Raw successful and failed logs remain in the session's `files/simplicity-remediation/` directory.
Key records include `native-scenarios-acceptance.log`, `native-scenarios-acceptance-2.log`, `final-ci-acceptance.log`, and `final-packaged-consumers.log`.
The issue comments retain the applicable commands, exact commits, and results.
