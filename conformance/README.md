# Conformance

## Purpose

This directory holds conformance assets for Synchro. The normative specification is authoritative. Future schema-valid, independently authored scenarios will provide executable contract inputs when the Phase 3 verification pipeline consumes them.

The goal is to exercise the normative contract across:

- `extensions/synchro-core`
- `extensions/synchro-pg`
- `api/go`
- Swift
- Kotlin
- React Native bridge where relevant

## Fixture Format

Existing JSON fixtures with `fixture_version = 1` are legacy engineering assets. They remain available pending Phase 3 migration, but are not authoritative certification evidence.

Fixture presence, decoder tests, and implementation-derived expected values are not proof of semantic conformance. Future scenarios must be schema-valid and independently authored from the normative specification.

If the corpus outgrows plain JSON later, the format can evolve deliberately.

## Directory Layout

- `protocol/`: connect, push, pull, rebuild, and error fixtures
- `schema/`: schema evolution fixtures
- `scopes/`: scope composition, cursor, and rebuild fixtures
- `mutations/`: mutation acceptance, rejection, and reconciliation fixtures
- `traces/`: client and server state-machine traces
- `performance/`: budgets and measurement scenario definitions
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

These files are not a certification result. Phase 3 must migrate or replace them with independently authored, schema-valid scenarios and generated evidence. A decoder that accepts a legacy fixture does not prove that the implementation satisfies the contract.

## Contract And Evidence

The specification, authored requirements, support matrix, and scenarios define what a release must prove. They do not report whether a candidate has proved it.

Generated evidence will record execution results for an exact candidate, resolved environment versions, and artifact hashes. Release tooling computes eligibility from that immutable evidence. A manually maintained coverage table cannot substitute for it.

The current fixtures and tests are engineering assets, not v0.3.0 certification. See `test-inventory.md` and [Release Verification](../docs/src/content/docs/spec/07-release-verification.mdx).

## Gate Model

The repo has two test tiers:

- `make test`: fast local validation
- `make release-check`: release validation entry point

Neither command alone is proof unless it produces the complete validated evidence required by the release contract for the exact candidate artifacts. Required release tests permit zero skips and zero unexplained flakes.

The representative relational corpus under `extensions/testdata/`, and the bundled `clients/react-native/example/seed.db` generated from it, are the canonical seeded end-to-end fixtures for:

- healthy seeded continuation
- seeded corruption repair
- shared public plus private-data composition
- rebuild and integrity recovery

## Working Rule

The normative specification defines expected behavior. Implementation output cannot define expectations. If implementation behavior disagrees with a legacy fixture, the fixture does not override the normative specification and must be reviewed during Phase 3 migration. A conflict with the normative specification is an implementation defect unless the specification is deliberately changed.
