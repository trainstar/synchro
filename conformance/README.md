# Conformance

## Purpose

This directory holds the shared executable contract corpus for Synchro.

The goal is to make the spec testable across:

- `extensions/synchro-core`
- `extensions/synchro-pg`
- `api/go`
- Swift
- Kotlin
- React Native bridge where relevant

## Fixture Format

The corpus uses JSON fixtures with `fixture_version = 1`.

This is intentionally simple and portable.

If the corpus outgrows plain JSON later, the format can evolve deliberately.

## Directory Layout

- `protocol/`: connect, push, pull, rebuild, and error fixtures
- `schema/`: schema evolution fixtures
- `scopes/`: scope composition, cursor, and rebuild fixtures
- `mutations/`: mutation acceptance, rejection, and reconciliation fixtures
- `traces/`: client and server state-machine traces
- `performance/`: budgets and measurement scenario definitions
- `test-inventory.md`: current mapping of protocol and invariant surfaces to automated coverage

## Current Seed Corpus

The initial fixture set covers the highest-risk flows:

- `connect` with no schema action
- `connect` with `rebuild_local`
- mixed push acceptance and rejection
- pull returning delta plus rebuild request
- single-scope rebuild pagination
- offline write before first connect
- additive schema change requiring rebuild
- hot-path round-trip budgets

## Gate Model

The repo has two test tiers:

- `make test`: fast local validation
- `make release-check`: authoritative release gate

Release claims should be based on `make release-check`, not the fast subset.

The representative relational corpus under `extensions/testdata/`, and the bundled `clients/react-native/example/seed.db` generated from it, are the canonical seeded end-to-end fixtures for:

- healthy seeded continuation
- seeded corruption repair
- shared public plus private-data composition
- rebuild and integrity recovery

## Working Rule

If implementation behavior disagrees with these fixtures and the current spec, implementation is wrong unless the spec is deliberately changed.
