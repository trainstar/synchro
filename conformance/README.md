# Conformance

## Purpose

This directory holds Synchro conformance assets. The normative specification is authoritative.

The goal is to exercise the normative contract across:

- `extensions/synchro-core`
- `extensions/synchro-pg`
- `api/go`
- Swift
- Kotlin
- React Native bridge where relevant

## Test Architecture

The test system has seven layers. Each behavior has exactly one authoritative proof home. Do not add a second proof in another layer.

1. **Contract layer.** The specification and authored vectors define expected behavior. Every applicable surface consumes the same vector files.
2. **Unit layer.** Deterministic tests stay beside the code. Mocks follow the Client Validation policy only.
3. **Real integration layer.** Tests use the real extension, adapter, and PostgreSQL. Happy paths do not use mocks.
4. **Scenario layer.** Authored semantic scenarios run on representative platform cells. Each production defect adds one permanent minimized scenario.
5. **Cell smoke layer.** Each support cell installs packaged artifacts. It then connects, pushes, pulls, terminates a process, and resumes.
6. **Adversarial layer.** Mutation gates, negative controls, and mechanical zero-skip enforcement prove that the other layers can fail.
7. **Randomized soak layer.** Seeded generative workloads check invariants. Each failure replays from its recorded seed.

Layers six and seven allow layers one through five to stay small. Do not replace a missing adversarial or soak proof with more example tests.

## Authored Scenarios

The authored scenario corpus is an executable contract input. Each scenario is schema-valid and independently authored from the normative specification.

Native rebuild-apply and rebuild-cardinality drivers construct deterministic inputs from the authored scenario.
They do not execute the reference model to generate those inputs or expected runtime results.

Proof metadata binds server, native, fault, and negative-control execution.
It no longer claims the retired reference-model proof type.
Authored inputs, expected state, wire expectations, assertions, and negative controls remain independent of implementation output.
The multi-scope wire assertion binds the existing Swift and Kotlin observed-wire checks.
Those checks follow the native bindings. A warm synchronization call can pull without another connect request.

## React Native Journeys

Run the complete corpora with `make test-rn-scenarios-ios` and `make test-rn-scenarios-android`.

The example and isolated consumers pin the same Community CLI version.
The consumer script replaces older template CLI pins before dependency installation.

Detox 20.47.0 uses `stream-json/jsonl/Parser`, and bunyamin 1.6.3 uses `StreamArray.withParser`.
Neither path uses the filters affected by [GHSA-528h-pc64-c93x](https://github.com/advisories/GHSA-528h-pc64-c93x).
Recheck these call paths when updating either tool. [Issue #86](https://github.com/trainstar/synchro/issues/86) records the dependency evidence.

The example harness displays raw JSON responses without wrapping in a horizontal scroll view.
Wrapping a large paragraph can retain many full-text copies in Android's native line-layout cache.
Only completed commands expose the response view.
This avoids reflowing a new response at the prior placeholder's narrow layout width.
Scroll horizontally to inspect the complete response. Selection, inspection, and the command and response bounds remain unchanged.

Each journey's Go context owns its aggregate deadline. Jest receives the remaining scenario budget.
Readiness, exchange, and command deadlines remain separate. Do not add independent whole-journey timeout overrides.

Use `await-step` to observe retry backoff while a managed call continues.
Use `await-call` with explicit `idle` or `error` completion to await terminal recovery.
An observed backoff does not prove that the public call has returned.
Retention recovery observers require a terminal pull after renewal before sampling readiness.
Reconnect can report ready before the client restores its scope cursors.
After an observed push backoff, delayed captures can show an active retry of the same sealed request.
Queue replay stops the public client before each offline write wave.
Long timer settings do not stop managed recovery or foreground-triggered work.

Use `make test-rn-e2e-ios-smoke` for focused smoke validation.
Smoke result observers use the existing Jest test budget.
Native waits, readiness checks, and controller handoffs keep their own bounds.

Use `make test-swift-integration` to run XCTest without repeating the scenario corpus.
Each required Make gate runs its declared selection and rejects a changed selector.
`PARTIAL=1` permits `GO_TEST_ARGS`, `GO_TEST_PKGS`, `SWIFT_TEST_ARGS`, `GRADLE_TEST_ARGS`, `DETOX_ARGS`, or `BLACKBOX_TEST_COUNT`.
A `PARTIAL=1` run is diagnostic output, not required-gate evidence.
`BLACKBOX_TIMEOUT` and `SWIFT_SCENARIOS_TIMEOUT` only bound run time. A slow host can raise them without a partial label.
`test-swift-unit`, `test-swift-integration`, and `test-kotlin-unit` parse structured results even when their runners fail.

The Swift retained-schema retry control measures actual SQLite reads across mixed-table mutations.
Sealing and replay each resolve a retained schema once per transaction.
The control also requires unchanged retry bytes and rejection of archive corruption between transactions.
The Swift queue-index upgrade control verifies column order and an unforced newest-mutation lookup, with index removal as a negative control.

## Real PostgreSQL Scenarios

Run the real PostgreSQL black-box tests with:

```text
make test-blackbox
```

The integration package runs direct semantic tests against packaged PostgreSQL and adapter artifacts. The scenario catalog separately binds the authored scenario bytes. Server tests do not satisfy native-client proof obligations.

Use `make server-consumer-smoke-phase` with its `SERVER_SMOKE_*` inputs to diagnose the public HTTP probe.

## Realistic Dataset

`dataset/` defines one synthetic training-application dataset. Its schema follows a real consumer: tenants, members, a shared catalog, private and shared programs, parent and child rows, many-to-many rows, stored generated columns, triggers that write other registered tables, soft deletes, and portable value boundaries.

- `TestRealDatasetAuthoredFlow` runs the authored flow through the real server. It compares each user's rows with hand-written expectations and with the authored business rule over live source rows.
- `dataset.RunNativeFlow` runs the same authored flow through native clients. The Swift, Kotlin, and React Native scenario suites each run it as their `dataset` subtest. Each native client must hold exactly the hand-written live rows, the canonical source values, and the hand-written values. A client may keep a tombstone only for a row that it held at the authored initial checkpoint and that the authored history deleted. A freshly rebuilt client keeps no tombstone.
- `make characterize-dataset DATASET_SEED=<n> DATASET_SIZE=s|m|l DATASET_CHARACTERIZATION_RESULT=<file>` records complete-work samples for one seeded workload. It has no numerical pass or fail rule. The file must be outside the repository.
- `make synchrod-pg-test-serve CLIENT_DATASET=1` also prepares the dataset and its authored seed for client flows.
- `TestRealSourceAdmissionRecovery` characterizes the source transaction record limit and stream-reset recovery.

After extension source changes, run `make generate-pg-sql` and commit any generated SQL changes.
Required pull-request CI runs `make check-pg-sql` before changes reach Candidate packaging.

## Negative Controls

Each control binds its fault plan, control metadata, and one requirement-owned semantic assertion.

The same assertion evaluates baseline and mutated subjects.

Production-artifact control execution uses the release procedure in `RELEASE.md`.

Generated package-fixture credentials cover the six-hour GitHub-hosted job limit.
This includes dependency installation, native compilation, and the application lifecycle.
The isolated fixture still requires signed, unexpired credentials.
Production token policy and lifecycle timeouts do not change.

## Server Mutation Gate

Run the production mutation gate with:

```text
make test-integration-mutants
```

The gate copies the current worktree into isolated temporary directories. It applies seven critical production-source mutations for cursor advancement, WAL acknowledgment, mutation conservation, checksum correctness, scope isolation, progress order, and pull deduplication.

Each copy gets new file modification times. So Cargo builds each workspace crate from the source of that mutant. The mutants share only the compiled third-party crates.

Each mutant must compile and fail its approved focused PostgreSQL 18 test. A surviving mutant, stale patch, build failure, or harness failure fails the gate.

Pull-request source checks run `make test-integration-mutant-manifest` before the full Candidate mutation gate.

Scheduled validation runs every manifest mutant through `make test-integration-mutants-broad`.

The WAL mutant uses the real packaged extension and black-box environment. It requires the same `SYNCHRO_CONFORMANCE_*` variables as `make test-blackbox`.

## Independent Semantic Checks

Use `make test-conformance-invariants` for the invariant checkers and their negative controls.
Use `make test-blackbox` for the real extension-backed server tests.
The `synchro-conformance` CLI manages the authored catalog.

## Fixture Format

The JSON fixtures under `protocol/`, `schema/`, and `scopes/` are legacy engineering assets.
They illustrate focused wire, schema, and scope cases. They are not authoritative certification evidence.

Fixture presence, decoder tests, and implementation-derived expected values are not proof of semantic conformance. Authored scenarios must be schema-valid and independently authored from the normative specification.

`scenarios/` holds the authored semantic corpus. Its expected state, wire expectations, assertions, faults, and negative controls are contract inputs.

## Directory Layout

- `scenarios/`: authored semantic scenarios
- `vectors/`: canonical protocol-value vectors
- `faults/`, `invariants/`, and `mutants/`: fault definitions, invariant checks, and adversarial controls
- `blackbox/`, `swift/`, `kotlin/`, and `reactnative/`: real-server and native-client conformance drivers
- `soak/`: seeded randomized invariant workloads
- `dataset/`: the realistic synthetic dataset, its authored flow, and its seeded generator
- `protocol/`, `schema/`, and `scopes/`: legacy illustrative fixtures
- `performance/`: authored budget inputs in `budgets.json`
- `artifacts/`, `schemas/`, and `internal/`: contract metadata, schemas, and validation
- `barriers/`, `execution/`, and `cmd/testresult/`: runtime trace coordination and structured test-result support

## Evidence

The specification, authored requirements, support matrix, and scenarios define expected behavior. They do not report an outcome.

Required gates run their declared Make selection and use structured results.
The result parser rejects failed, skipped, and zero-test results.
It cannot detect a nonempty subset, so each required Make gate rejects a changed selector.

Soak wire records preserve the original request and response from each exchange.
Cursor issuance and later acknowledgment use separate exchange identities.
WAL records and durable progress share one read-only repeatable-read observation transaction.
Worker and replication-slot status remain live observations.
The real extension-reinstall test covers registrations committed before a replacement slot exists.
Its missing-replay control acknowledges later WAL while registry activation remains blocked.
The same cluster then proves cold reinstall recovery, readiness, and source-to-client delivery across repeated reinstalls.
This adds the real slot-boundary proof that SQL-only activation-message checks do not establish.
The live server soak is bounded seeded stress. `SOAK_OPERATIONS` sets its explicit operation budget, and each run reports its measured elapsed time.
It executes response loss on push or pull and WAL-worker replay interruption.
Every operation ends at a quiescent point. The soak then reads the isolated source tables directly and compares them with an independent model of the authored rows.
It also compares the client's complete scope membership and every held synced field, in wire form, with those source rows. Expected membership comes from each table's business rule, not from Synchro output.
Each run writes its journal to a new directory under `SOAK_ARTIFACT_DIR`. A failed run also keeps its original wire bodies there before cleanup.
The journal holds the seed, configuration, planned operations, and one bounded terminal failure identity.
A violation failure keeps the violation count, a digest of the complete violation set, and a bounded sample. Violation evidence uses stable authored table and field names, not per-cluster runtime IDs.
A harness failure keeps a stage and failure class only when a specific check names it, such as a missing WAL replay boundary, a wrong acknowledgement, or a changed durable result. Other harness failures keep no identity.
Replay a retained journal in a new cluster with `make soak-replay SOAK_REPLAY_JOURNAL=<journal>`. Replay passes only when it reproduces that identity. A failure without a precise identity makes replay inconclusive, and replay fails.
Its in-memory client is reference state, not native process-recovery evidence.
Native recovery remains covered by the real native scenario gates.

Use `RELEASE.md` for release procedure.

The representative relational corpus under `extensions/testdata/` is the canonical seeded end-to-end fixture source.
The pinned `clients/react-native/example/seed.db` is generated from that source with `make refresh-rn-seed`.

- healthy seeded continuation
- seeded corruption repair
- shared public plus private-data composition
- rebuild and integrity recovery

## Working Rule

The normative specification defines expected behavior. Implementation output cannot define expectations. Review legacy fixtures that disagree with the specification.
