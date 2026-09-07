# R3.2 Invariant-Engine Design

This is the binding design contract for R3.2 and R3.3.
The primary authored it from `.ignore/r3/tmp/r3-2-design-inputs.md`, which cites repository evidence for every input.
Parallel implementation streams follow this contract with disjoint file ownership.

## Decision 1: Package And Dependencies

The engine lives in a new package `conformance/invariants/`.

- It imports contract-layer types (`conformance/scenarios` state facts, `conformance/vectors` digest functions).
- It has zero imports from `conformance/reference/` and `conformance/modelrunner/`.
- It has zero imports from platform packages. Adapters in existing packages assemble its inputs.

## Decision 2: Observation Model

The engine consumes a neutral `Observation` assembled from surfaces that already exist:

- Server state capture: the repeatable-read `StateFacts` projection from the native controller.
- Operator observations: checkpoints, WAL progress, slot flush, rebuild and retention facts.
- Client captures: durable rows, queues, outcomes, checkpoints, scope states, raw cursors.
- Wire exchanges: raw request and response bodies where identity-level checks need them.

The engine never queries a database and never performs HTTP.
Drivers observe, the engine judges.

## Decision 3: Five Checkers With Typed Violations

Each family is one pure function from observations to a typed violation list.
Each generalizes an already-verified validator instead of inventing new semantics:

- `mutation-conservation` generalizes the accepted-rejected partition check from `real_mutation_controls_test.go:171-258`.
- `cursor-monotonicity` generalizes the cursor advancement and checkpoint checks from `real_mutation_controls_test.go:18-74`, ordered by server checkpoint positions and raw client cursors, not by fingerprints.
- `checksum-convergence` recomputes row and scope digests with `vectors.RowDigest` and `vectors.ScopeDigest` and compares client-held authoritative and local checksums, generalizing `real_mutation_controls_test.go:261-313`.
- `scope-isolation` generalizes the selected-scope negative check from `real_mutation_controls_test.go:316-347` plus membership and cardinality facts.
- `no-state-forks` generalizes the Kotlin and React Native process-death checks: changed process identity, unchanged database identity, durable state equality across the kill boundary.

Every checker lands with a demonstrated mutant observation that it catches.

## Decision 4: Required Signal Additions

Four signals are missing today. Each becomes an owned work item inside R3.2:

1. Server `StateFacts` gains mutation outcome identities (today only counts exist).
2. Server `StateFacts` gains complete row-to-scope edges (today only cardinalities exist).
3. Swift capture gains process identity and database identity fingerprint, reaching parity with Kotlin and React Native.
4. The soak driver records raw wire bodies for identity-level conservation checks (the recorder already content-addresses bodies).

No production sync behavior changes.
Every addition is an observation surface in conformance code or client inspection.

## Decision 5: Soak Topology (R3.3)

The soak drives the real extension and adapter through the blackbox harness on the desktop host.

- The workload generator is seeded, and the seed is recorded in the run artifact.
- Workloads mix connect, push, pull, rebuild, schema transitions, process death, and wire faults from the existing fault catalog.
- The invariant engine judges observations continuously during the run.
- A failure replays from its recorded seed alone, then becomes a permanent minimized scenario.
- Native device cells stay in the scenario and smoke layers. The soak proves server and protocol invariants, not device UI stacks.

Rationale: a 72-hour run through mobile simulators would measure simulator flake, not sync correctness, and the client-native invariants already execute in the scenario layer through the same engine functions.

## Decision 6: Deterministic Validation Rows Are Not Engine Work

The R3.1 contract contains rows that verify deterministic protocol validation (lifecycle transition rules, installation shape, schema gates, write policy).
A randomized invariant engine is the wrong home for deterministic rejection rules.
Their proof home is the contract layer and the real integration layer.
The disposition of each such row is decided in the R3.1 closure, and R3.4 migrates each row to its named home, engine or not.
This refines the parent's engine-coverage wording. The deletion condition stays the same: every row has a named executable proof before deletion.

## File Ownership For Parallel Streams

- Stream A (engine): `conformance/invariants/` plus the server `StateFacts` signal additions in the native controller.
- Stream B (soak driver): a new `conformance/soak/` package, the Make target, and seed plumbing.
- Stream C (Swift capture parity): Swift conformance capture surfaces only.

Streams touch disjoint files.
The engine `Observation` API in this document is the binding interface between them.
