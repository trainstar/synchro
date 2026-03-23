# Audit Sequence

## Purpose

This document defines the order and method for auditing the current codebase against the vNext spec.

The goal is to avoid the usual failure mode where outer layers are cleaned up before inner contract truth is stable.

## Governing Rule

The audit does not ask, "What does the current code do?"

It asks, "Does the current code conform to the new spec?"

Current implementation is evidence only.

It is not authority.

## Audit Strategy

Audit from the semantic center outward.

That means:

1. freeze spec and conformance assets
2. audit shared core semantics
3. audit PostgreSQL extension behavior
4. audit HTTP adapter behavior
5. audit native clients
6. audit React Native bridge

This order is required because each outer layer depends on the contract below it.

## Phase 0: Freeze Contract Inputs

Before implementation audit starts, the team should freeze:

- docs `00` through `06`
- shared conformance fixtures
- round-trip and performance expectations that matter for acceptance

Exit criteria:

- spec changes require explicit conscious approval
- conformance fixtures exist for the highest-risk flows

## Phase 1: Audit `extensions/synchro-core`

### Focus

Shared deterministic semantics.

### Questions

- do core data structures represent the spec cleanly
- do transition rules match `03-state-machines`
- do invariant-sensitive operations match `04-invariants`
- do schema and version primitives match `05-schema-evolution`

### Required Outputs

- gap list against spec
- code changes closing those gaps
- passing core conformance tests

### Exit Criteria

- core semantic fixtures pass
- no unresolved contradictions between core logic and spec
- any ambiguous spec issue is resolved before moving outward

## Phase 2: Audit `extensions/synchro-pg`

### Focus

Database-near execution and materialization.

### Questions

- does change capture match the intended WAL or CDC model
- are scope membership and fanout behavior correct
- are rebuild semantics correct
- does synced CRUD apply authoritatively
- does schema manifest generation match the canonical model
- are schema action decisions correct

### Required Outputs

- integration gaps against spec
- real PostgreSQL test coverage for those gaps
- measured fanout and rebuild behavior for representative cases

### Exit Criteria

- PostgreSQL extension passes conformance integration tests
- shared and private scope cases behave correctly
- schema evolution scenarios behave correctly

## Phase 3: Audit `api/go`

### Focus

HTTP contract fidelity and thin-adapter discipline.

### Questions

- does `api/go` expose the exact wire contract from `01-wire-protocol`
- does it keep semantics in the extension instead of reimplementing them
- are errors, auth failures, and upgrade failures translated correctly
- does it preserve idempotency and retry semantics

### Required Outputs

- HTTP black-box test coverage
- removal of any accidental semantic ownership in the adapter
- passing protocol conformance suite

### Exit Criteria

- adapter behavior matches the spec exactly
- adapter does not own divergent sync logic

## Phase 4: Audit Swift And Kotlin Clients

### Focus

Native SDK correctness and thin-client boundaries.

### Questions

- do local SQLite responsibilities match `02-client-contract`
- is the queue durable
- are push, pull, and rebuild ordered correctly
- is schema migration safe
- is local provenance correct for multi-scope tables
- are typed events and status surfaces correct

### Required Outputs

- passing end-to-end conformance scenarios for both native clients
- removal of any accidental server-semantic ownership
- measured warm-start and rebuild behavior

### Exit Criteria

- Swift and Kotlin pass the same core behavior matrix
- native clients remain thin without becoming fake-thin

## Phase 5: Audit React Native Bridge

### Focus

Bridge parity, not independent semantics.

### Questions

- does the bridge expose the right surface
- does it forward typed errors and events correctly
- has any sync logic leaked into the bridge layer

### Required Outputs

- parity tests
- removal of any accidental duplicated semantics

### Exit Criteria

- React Native remains a bridge over native semantics

## Audit Method Inside Each Phase

Each phase should use the same working pattern:

1. inventory the current code path
2. map current behavior to the relevant spec sections
3. classify each mismatch:
   - missing
   - incorrect
   - extra and unsupported
   - ambiguous spec needing decision
4. write or update conformance tests first when the spec is clear
5. change implementation until tests pass
6. remove unsupported behavior that has no place in vNext

## What To Keep Versus Delete

Code should be kept only if it satisfies one of these conditions:

- already conforms
- can be reshaped cleanly to conform
- provides useful low-level machinery without imposing wrong semantics

Code should be deleted or replaced if it:

- encodes legacy contract assumptions
- owns semantics in the wrong layer
- depends on unsupported behavior
- makes the architecture harder to reason about than rewriting it

## Performance Audit Position

Performance audit should follow correctness inside each phase, not precede it.

The required order is:

1. prove correctness
2. measure against expected budgets
3. optimize if measurements miss the target

Do not optimize outer layers while inner-layer correctness is still unstable.

## Stop Conditions

Do not advance to the next phase if:

- current phase conformance tests are still failing
- there are unresolved spec contradictions
- the implementation still depends on unsupported legacy behavior

## Final Acceptance

The vNext audit is complete only when:

- all phases passed their exit criteria
- the shared conformance suite passes end to end
- no layer still relies on deleted or intentionally abandoned legacy contract assumptions

## Open Follow-Up Questions

- whether fanout profiling should be required during the PostgreSQL extension phase or also repeated during adapter and client validation
- whether some client performance checks should be nightly-only once the correctness suite is stable
