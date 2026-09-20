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

The example harness uses simple Android line breaking for its raw JSON response view.
This prevents high-quality paragraph layout from retaining large copies of the response in the native text cache.
The displayed and inspected response stays complete. Do not truncate it or relax the command and response bounds.

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
`SWIFT_TEST_ARGS` selects diagnostic XCTest cases in `test-swift-unit` and `test-swift-integration`. Required CI leaves it empty.
`test-swift-unit`, `test-swift-integration`, and `test-kotlin-unit` parse structured results even when their runners fail.

## Real PostgreSQL Scenarios

Run the real PostgreSQL black-box tests with:

```text
make test-blackbox
```

The integration package runs direct semantic tests against packaged PostgreSQL and adapter artifacts. The scenario catalog separately binds the authored scenario bytes. Server tests do not satisfy native-client proof obligations.

Use `make server-consumer-smoke-phase` with its `SERVER_SMOKE_*` inputs to diagnose the public HTTP probe.

After extension source changes, run `make generate-pg-sql` and commit any generated SQL changes.
Required pull-request CI runs `make check-pg-sql` before changes reach Candidate packaging.

## Negative Controls

Each control binds its fault plan, control metadata, and one requirement-owned semantic assertion.

The same assertion evaluates baseline and mutated subjects.

Production-artifact control execution uses the release procedure in `RELEASE.md`.

## Server Mutation Gate

Run the production mutation gate with:

```text
make test-integration-mutants
```

The gate copies the current worktree into isolated temporary directories. It applies seven critical production-source mutations for cursor advancement, WAL acknowledgment, mutation conservation, checksum correctness, scope isolation, progress order, and pull deduplication.

Each mutant must compile and fail its approved focused PostgreSQL 18 test. A surviving mutant, stale patch, build failure, or harness failure fails the gate.

Pull-request source checks run `make test-integration-mutant-manifest` before the full Candidate mutation gate.

Scheduled validation runs every manifest mutant through `make test-integration-mutants-broad`.

The WAL mutant uses the real packaged extension and black-box environment. It requires the same `SYNCHRO_CONFORMANCE_*` variables as `make test-blackbox`.

## Independent Semantic Checks

Use `make test-conformance-invariants` for the invariant checkers and their negative controls.
Use `make test-blackbox` for the real extension-backed server tests.
The `synchro-conformance` CLI manages the authored catalog.

The removed synthetic HTTP wrapper used the reference model for both expected and observed results.
Its fault classes remain covered by the independent checker controls and real protocol tests.
The `reference/` and `modelrunner/` interpreters and their model-only Make target are removed.
The existing deletion-contract tables retain every original check and its proof home or reason for retirement.

## Fixture Format

Existing JSON fixtures with `fixture_version = 1` are legacy engineering assets. They are not authoritative certification evidence.

Fixture presence, decoder tests, and implementation-derived expected values are not proof of semantic conformance. Future scenarios must be schema-valid and independently authored from the normative specification.

If the corpus outgrows plain JSON later, the format can evolve deliberately.

## Directory Layout

- `protocol/`: connect, push, pull, rebuild, and error fixtures
- `schema/`: schema evolution fixtures
- `scopes/`: scope composition, cursor, and rebuild fixtures
- `mutations/`: mutation acceptance, rejection, and reconciliation fixtures
- `traces/`: client and server state-machine traces
- `performance/`: budgets and measurement scenario definitions
- `artifacts/`: artifact roles
- `faults/`: typed fault recipes and negative controls
- `schemas/`: versioned contract schemas

## Current Seed Corpus

The initial fixture set is legacy engineering coverage for high-risk flows:

- `connect` with no schema action
- `connect` with `rebuild_local`
- mixed push acceptance and rejection
- pull returning delta plus rebuild request
- single-scope rebuild pagination
- offline write before first connect
- additive schema change requiring rebuild
- hot-path round-trip budgets

These files are not a certification result. A decoder that accepts a legacy fixture does not prove contract conformance.

## Evidence

The specification, authored requirements, support matrix, and scenarios define expected behavior. They do not report an outcome.

Use structured results for required gates. Skipped, filtered, and zero-test results fail the gate.

Soak wire records preserve the original request and response from each exchange.
Cursor issuance and later acknowledgment use separate exchange identities.
WAL records and durable progress share one read-only repeatable-read observation transaction.
Worker and replication-slot status remain live observations.
The real extension-reinstall test covers registrations committed before a replacement slot exists.
Its missing-replay control acknowledges later WAL while registry activation remains blocked.
The same cluster then proves cold reinstall recovery, readiness, and source-to-client delivery across repeated reinstalls.
This adds the real slot-boundary proof that SQL-only activation-message checks do not establish.
The live server soak executes response loss on push or pull and WAL-worker replay interruption.
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
