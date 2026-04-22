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

## Working Rule

If implementation behavior disagrees with these fixtures and the current spec, implementation is wrong unless the spec is deliberately changed.
