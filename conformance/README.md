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

## Real PostgreSQL Scenarios

Run the real PostgreSQL black-box tests with:

```text
make test-blackbox
```

The integration package runs direct semantic tests against packaged PostgreSQL and adapter artifacts. The scenario catalog separately binds the authored scenario bytes. Server tests do not satisfy native-client proof obligations.

## Negative Controls

Each control binds its fault plan, control metadata, and one requirement-owned semantic assertion.

The same assertion evaluates baseline and mutated subjects.

Production-artifact control execution and evidence belong to the phase that owns each production artifact.

## Server Mutation Gate

Run the Phase 4 production mutation gate with:

```text
make test-integration-mutants
```

The gate copies the current worktree into isolated temporary directories. It applies five exact production-source mutations for cursor advancement, WAL acknowledgment, mutation conservation, checksum correctness, and scope isolation.

Each mutant must compile and fail its approved focused PostgreSQL 18 test. A surviving mutant, stale patch, build failure, or harness failure fails the gate.

The WAL mutant uses the real packaged extension and black-box environment. It requires the same `SYNCHRO_CONFORMANCE_*` variables as `make test-blackbox`.

## Synthetic Harness Proof

Run the loopback synthetic harness with:

```text
synchro-conformance blackbox --repo-root PATH --mode harness
```

This command proves that the protocol 3 harness detects and compares typed HTTP behavior. It uses a synthetic reference system. It is not real adapter, extension, or PostgreSQL proof.

The harness emits signed `harness-only` receipts. These receipts support harness self-tests. They cannot enter `evidence-v2` or candidate evidence.

Strict real protocol 3 black-box execution is unavailable. The `blackbox --mode strict` command fails closed. It creates no release evidence.

## Evidence And Promotion

`evidence-v2` binds terminal execution receipts to exact candidate artifacts, scenarios, contract files, and environment dimensions. Evidence validation is not a release promotion.

Phase 6 promotion requires the complete verified RC evidence. This includes strict real protocol 3, native client, attestation, soak, and canary evidence. The repository remains fail-closed until that evidence exists.

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
- `artifacts/`: exact candidate artifact roles, including the independent
  conformance runner
- `faults/`: one requirement-owned negative control and typed fault recipe per
  release requirement
- `schemas/`: versioned authored-contract and generated-evidence schemas
- `test-inventory.md`: notice describing the transition from the former manual inventory to generated evidence

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

## Contract And Evidence

The specification, authored requirements, support matrix, and scenarios define what a release must prove. They do not report whether a candidate has proved it.

Generated `evidence-v2` records results for an exact candidate, resolved environment versions, and artifact hashes. Release tooling computes eligibility from immutable evidence. A manually maintained coverage table cannot substitute for it.

The current fixtures and harness proof are not v0.3.0 certification. See `test-inventory.md` and [Release Verification](../docs/src/content/docs/spec/07-release-verification.mdx).

## Gate Model

The repo has two test tiers:

- `make test`: fast local validation
- `make release-check`: release validation entry point

Neither command alone is proof unless it produces the complete validated evidence required by the release contract for the exact candidate artifacts. Required release tests permit zero skips and zero unexplained flakes.

`make release-check` intentionally fails closed while the Phase 3 conformance,
black-box, and evidence targets or the Phase 6 RC verification target remain
unimplemented. A partial repository state cannot report a release pass.

The representative relational corpus under `extensions/testdata/` is the canonical seeded end-to-end fixture source.
The pinned `clients/react-native/example/seed.db` is generated from that source with `make refresh-rn-seed`.

- healthy seeded continuation
- seeded corruption repair
- shared public plus private-data composition
- rebuild and integrity recovery

## Working Rule

The normative specification defines expected behavior. Implementation output cannot define expectations. If implementation behavior disagrees with a legacy fixture, the fixture does not override the normative specification and must be reviewed during Phase 3 migration. A conflict with the normative specification is an implementation defect unless the specification is deliberately changed.
