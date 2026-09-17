# Curated Audit Remediation Plan

**Approved:** 2026-09-17.
**Scope:** All 53 findings in [the curated audit](2026-09-17-whole-repository-simplicity-review.md).
**Ledger:** #105. Do not create an issue per finding.
**Starting commit:** `b8709e3a65f3883b09cad2c7464cd4d5ef968dc9`.

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
