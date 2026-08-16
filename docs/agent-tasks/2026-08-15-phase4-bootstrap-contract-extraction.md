# Phase 4 Bootstrap Contract Extraction

Date: 2026-08-15

This report records the frozen contract evidence extracted during Phase 4 work.
It is research evidence only. It is not implementation evidence or an exit-gate result.

## Closed Conformance Surface

The closed operation registry has no dedicated production Class 3 bootstrap operation.
Unknown operations and unknown payload fields fail closed.

Relevant operations:

- `model/install-current-contract`
- `model/publish-schema`
- `model/stage-registry-membership-generation`
- `model/activate-registry-membership-generation`
- `model/commit-source-transaction`
- `process/materialize-source-transaction`
- `process/restart-wal-worker`
- `connect/send`
- `model/set-client-assignments`
- `workload/prepare`

Evidence:

- `conformance/scenarios/operations.go:13-39`
- `conformance/scenarios/operations.go:179-249`
- `conformance/scenarios/operations_test.go:8-57`

## Membership Generation Staging

`model/stage-registry-membership-generation` requires these fields:

- `registry_generation`
- `membership_generation`
- `batch_size`
- `activation_boundary`
- `affected_scopes`
- `scope_rules`
- `dependency_impacts`

The activation boundary must equal the complete durable materialization boundary.
The boundary must use the active stream and `transaction_end` position kind.
The current registry generation must be active, validated, and without a bootstrap stage.
The staged generation and each affected membership generation must increase.
The affected scope set must be complete, unique, and authoritative.
Invalid input must not change durable state.

Evidence:

- `conformance/reference/membership.go:11-48`
- `conformance/reference/membership.go:69-178`
- `conformance/reference/membership.go:240-468`
- `conformance/reference/wal_operations_test.go:438-576`

## Membership Generation Activation

`model/activate-registry-membership-generation` accepts one positive `registry_generation`.
Activation requires a complete, verified candidate stage.
The candidate generation and activation boundary must match the requested generation.
Main WAL materialization must reach the activation boundary.
The candidate must contain at least one complete affected scope.

Activation performs these changes atomically:

- Replace only affected live scope states.
- Advance the active registry generation.
- Clear the bootstrap stage.
- Retain prior registry history.
- Install the activated relation graph.
- Mark affected assignments for rebuild.
- Remove only affected checkpoints.
- Invalidate only affected rebuild sessions.

Evidence:

- `conformance/reference/membership.go:181-238`
- `conformance/reference/membership.go:505-566`
- `conformance/reference/wal_operations_test.go:361-436`

## Class 3 Projection Bootstrap

A Class 3 change remains pending while required projections are absent.
The server must create one permanent candidate logical slot through the replication protocol.
The server must import that slot's exported snapshot.
The server must stage complete source, dependency, version, membership, and integrity projections.
The candidate and main workers must reach one matching transaction-end activation boundary.
Activation must publish the registry, manifest, projections, membership generations, and invalidation atomically.
Process failure must resume the exact candidate or discard the complete stage.

Evidence:

- `docs/src/content/docs/spec/05-schema-evolution.mdx:523-559`
- `docs/src/content/docs/spec/04-invariants.mdx:224-226`
- `docs/src/content/docs/architecture/decisions/001-wal-change-stream.mdx:83-132`
- `docs/src/content/docs/architecture/decisions/004-membership-schema-and-retention.mdx:306-338`
- `conformance/requirements.json:970-976`
- `conformance/faults/catalog.json:226`

## Candidate State

Each candidate stage binds these values:

- Registry generation.
- Schema identity.
- Stream generation.
- Snapshot boundary.
- Activation barrier.
- Verification state.
- Candidate rows.
- Candidate projections.
- Candidate fences.
- Candidate scope states.

Evidence:

- `conformance/reference/types.go:382-460`
- `conformance/reference/types.go:566-642`
- `conformance/reference/state_test.go:246-300`
- `conformance/reference/state_test.go:619-662`

## Projection-Only Functions

Membership and impact functions must be schema-qualified SQL functions.
They must be `STABLE` and `SECURITY INVOKER`.
They must use the exact registered signatures and return types.
They may read only declared extension-owned projection views and fields.
They must not read live application relations.
They must not use temporary objects, dynamic SQL, clocks, randomness, sequences, or session state.
Each fanout and impact bound must be positive and configured.

Evidence:

- `docs/src/content/docs/architecture/decisions/004-membership-schema-and-retention.mdx:197-257`
- `docs/src/content/docs/spec/04-invariants.mdx:208-214`
- `conformance/requirements.json:808-815`
- `conformance/faults/catalog.json:166`

## Frozen Scenarios

`SCN-REGISTRY-RELOAD-001` stages generation 2 at the durable LSN 10 boundary.
It activates generation 2 before LSN 20 materialization.
Replay of LSN 10 must retain generation 1.

Evidence:

- `conformance/scenarios/server/registry-reload-001.json:423-720`

`SCN-MEMBERSHIP-REASSIGNMENT-001` stages and activates dependency generations 2 and 3.
It requires old-scope deletion and new-scope insertion for reassignment.

Evidence:

- `conformance/scenarios/server/membership-reassignment-001.json:513-747`
- `conformance/scenarios/server/membership-reassignment-001.json:841-861`

## Required Negative Controls

- Reject an independent live snapshot.
- Reject a temporary or mismatched candidate slot.
- Detect omitted candidate catch-up.
- Detect early manifest exposure.
- Detect partial activation after process failure.
- Detect missing old-state or new-state dependency propagation.
- Detect reused membership generations.
- Detect invalidation of unrelated scopes.
- Reject invalid function signatures, volatility, security, dependencies, or bounds.

Evidence:

- `conformance/faults/catalog.json:53-56`
- `conformance/faults/catalog.json:95-113`
- `conformance/faults/catalog.json:166`
- `conformance/faults/catalog.json:208-226`

## Implementation Consequence

The contract requires one durable pending-generation lifecycle.
Immediate activation cannot satisfy historical projection or catch-up requirements.
Fail-closed rejection is safe but does not complete the accepted behavior.
Separate table, dependency, and function bootstrap paths would duplicate authority.
One shared bootstrap engine is the smallest contract-complete design.
