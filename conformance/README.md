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

## Authored Scenarios And Model Execution

The authored scenario corpus is an executable contract input. Each scenario is schema-valid and independently authored from the normative specification.

Run the independent protocol 3 model with:

```text
synchro-conformance model --repo-root PATH [--scenario ID]
```

The command loads authored scenarios. It fails if any selected scenario does not pass the reference model. A model pass validates the model execution only. It does not prove a production candidate.

## Phase 3 Negative Controls

Phase 3 preserves all authored control metadata for later production-owning phases. It does not emulate those controls by changing completed model traces.

Phase 3 proves independent detector behavior with four typed harness self-mutants:

- omitted mutation outcome
- constant digest
- duplicate delivered effect
- wrong-scope row

Each self-mutant changes one eligible semantic outcome. Its requirement-owned assertion must detect that change for the intended contract reason.

Production-artifact control execution and evidence belong to the later phase that owns each production artifact.

## Synthetic Harness Proof

Run the loopback synthetic harness with:

```text
synchro-conformance blackbox --repo-root PATH --mode harness
```

This command proves that the protocol 3 harness detects and compares typed HTTP behavior. It uses a synthetic reference system. It is not real adapter, extension, or PostgreSQL proof. It cannot create release evidence.

## Protocol 2 Baseline Diagnostics

Run current-production diagnostics with:

```text
synchro-conformance baseline --repo-root PATH --output PATH
synchro-conformance blackbox --repo-root PATH --mode baseline
```

The baseline sends only protocol 2 diagnostic requests. It records known production divergences in `baseline-report-v1` output. Every report and attachment is permanently `non_release_diagnostic`.

Baseline output cannot satisfy a proof obligation. It cannot enter `evidence-v2`, an inventory, or a candidate directory. It does not define protocol behavior.

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

The current fixtures, model runs, harness proof, and baseline diagnostics are not v0.3.0 certification. See `test-inventory.md` and [Release Verification](../docs/src/content/docs/spec/07-release-verification.mdx).

## Gate Model

The repo has two test tiers:

- `make test`: fast local validation
- `make release-check`: release validation entry point

Neither command alone is proof unless it produces the complete validated evidence required by the release contract for the exact candidate artifacts. Required release tests permit zero skips and zero unexplained flakes.

`make release-check` intentionally fails closed while the Phase 3 conformance,
black-box, and evidence targets or the Phase 6 RC verification target remain
unimplemented. A partial repository state cannot report a release pass.

The representative relational corpus under `extensions/testdata/`, and the bundled `clients/react-native/example/seed.db` generated from it, are the canonical seeded end-to-end fixtures for:

- healthy seeded continuation
- seeded corruption repair
- shared public plus private-data composition
- rebuild and integrity recovery

## Working Rule

The normative specification defines expected behavior. Implementation output cannot define expectations. If implementation behavior disagrees with a legacy fixture, the fixture does not override the normative specification and must be reviewed during Phase 3 migration. A conflict with the normative specification is an implementation defect unless the specification is deliberately changed.
