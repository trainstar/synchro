# Phase 4 Completion Report

Date: 2026-08-16

## Result

Phase 4 satisfies its binding server exit gate on the current worktree.

- S-01 through S-28 have implemented dispositions and executable evidence.
- Real PostgreSQL 18.3 WAL and Class 3 bootstrap tests passed.
- Server component and black-box gates passed with no skipped tests.
- Core mutation testing reported no surviving mutant.
- Curated integration testing killed all five production mutants.
- Independent review found no unresolved Critical or High server finding.
- PostgreSQL 18.3 remains the only certified database runtime.
- Release promotion remains blocked by later release phases.

## Implemented Outcome

Phase 4 now provides these server surfaces:

- WAL-only pull visibility with commit-safe typed stream positions.
- Durable replay identity, contiguous acknowledgement, quarantine, recovery, and bounded capture health.
- Scope-correct pull pagination and authenticated immutable rebuild sessions.
- Durable push replay with exact canonical outcomes and opaque compare-and-swap versions.
- Complete membership dependency propagation and scope-local invalidation.
- Immutable schema lineages with Class 1 through Class 4 dispatch.
- A production Class 3 projection-bootstrap coordinator with a permanent candidate slot.
- Real process-termination recovery plus deterministic actions for every persisted Class 3 lifecycle.
- Canonical SHA-256 row and scope digests with independent vectors.
- Snapshot-consistent portable seeds with complete artifact verification.
- Six least-privilege PostgreSQL roles and separate operator and worker credentials.
- pgrx-authored installation SQL with an executable drift check.
- Curated Rust and production integration mutation gates in CI.

## Finding Dispositions

| Finding | Implemented disposition | Independent proof and negative control |
| --- | --- | --- |
| S-01 | `stream_position.rs`, `wal_decoder.rs`, and `bgworker.rs` use WAL commit-order positions. | `wal-order-001.json` proves ordering and rejects sequence-style ordering. |
| S-02 | `pull.rs` resolves each scope before the page limit and computes global `has_more`. | `test_pull_applies_limit_after_each_scope_is_eligible` and `pull-divergent-checkpoints-001.json` cover divergent scopes. |
| S-03 | `pull.rs` reads immutable captured projections and fails before cursor acknowledgement. | `test_pull_missing_projection_fails_without_progress` and `pull-hydration-failure-001.json` reject partial progress. |
| S-04 | `rebuild.rs` stages immutable sessions. `rebuild_token.rs` authenticates every continuation binding. | `test_rebuild_cursor_pagination` and `rebuild-forged-cursor-001.json` reject forged or cross-context cursors. |
| S-05 | Rebuild returns an issued cursor but does not acknowledge any checkpoint. | `TestRealS05SelectiveRebuildPreservesCheckpoints` proves target isolation, scope-set stability, and later delivery of unread unrelated history. |
| S-06 | `bgworker.rs` acknowledges only contiguous durable transactions and blocks on poison. | `wal-decode-failure-001.json` and the WAL acknowledgement mutant prove fail-closed behavior. |
| S-07 | Registry activation travels through ordered WAL control messages. | `registry-reload-001.json` rejects activation before the source transaction boundary. |
| S-08 | `bucketing.rs` and `bgworker.rs` propagate complete old-state and new-state dependency impacts. | Membership tests reject duplicate impacts and prove valid empty string primary keys. |
| S-09 | `materialize.rs` stages replacement edges and invalidates only affected scope generations. | Backfill tests preserve unrelated scope generations and reject digest mismatch. |
| S-10 | `schema.rs` stores immutable manifests. `client.rs` implements complete class dispatch. | Schema tests reject manifest mutation. The real Class 3 test rejects early or partial activation. |
| S-11 | `push.rs` stores batch and mutation outcomes and replays exact canonical responses. | Push idempotency tests reject changed batch content and changed mutation identity. |
| S-12 | `compaction.rs` uses typed scope checkpoints, retention floors, and strict positive limits. | Retention tests reject stale cursors, nonpositive limits, and unrelated-scope deletion. |
| S-13 | `checksum.rs` uses canonical typed bytes and SHA-256 row and scope digests. | Canonical vectors and the checksum production mutant reject changed bound input. |
| S-14 | Checksums use exact structured shapes. State transitions use one complete adjacency map. | Core tests reject malformed checksums, incomplete maps, and illegal state transitions. |
| S-15 | Server versions are opaque equality tokens with no timestamp or sequence meaning. | The complete push CAS matrix rejects stale update and delete versions. |
| S-16 | Push conflict decisions use locked base-version equality. Client time has no authority. | `TestRealS16ConcurrentPushCASIgnoresClientTime` queues past and future timestamps in deterministic lock order and proves one compare-and-swap winner. |
| S-17 | Push accepts only insert, update, and delete with canonical status dispatch. | Invalid-shape tests reject `upsert`, missing bases, duplicates, and unknown fields before ledger creation. |
| S-18 | Authored vectors define expected canonical bytes and digests independently from production. | Vector mutants and `test_scope_checksum_matches_authored_canonical_vector` detect semantic changes. |
| S-19 | Semantic assertions, negative controls, and demonstrated mutants replace presence-only evidence. | Both mutation gates fail when required semantics are removed or changed. |
| S-20 | Streaming SHA-256 and finite positive operational limits have measurable contracts. | Configured-bound scenarios and health tests reject missing, invalid, or unknown limits. |
| S-21 | Real source writes become pull-visible only through WAL decoding and durable materialization. | `TestRealWALPipeline` proves the flow. The WAL acknowledgement mutant breaks that flow. |
| S-22 | Make and CI require generated contract, evidence, artifact, mutation, and server checks. | Evidence tests reject missing closure, missing vectors, and changed artifacts. |
| S-23 | The fixed `synchro` schema uses exact grants and six restricted group roles. | Authorization tests and the real five-login harness reject broad or cross-role authority. |
| S-24 | Registry identity is schema-qualified and bound to immutable logical IDs and relation OIDs. | Schema tests reject recreated relations, key drift, trigger drift, and ambiguous names. |
| S-25 | Unsupported truncate, replica identity, relation drift, and fence drift block capture. | WAL and schema tests reject each condition without acknowledgement or partial progress. |
| S-26 | Readiness verifies database, worker, registry, publication, slot, progress, poison, and finite lag. | Health tests return generic unready status for every missing or invalid required check. |
| S-27 | Portable seed export uses one deferrable serializable transaction and bound tokens. | Seed tests reject metadata, trigger, schema, digest, receipt, queue, and sidecar corruption. |
| S-28 | pgrx source owns DDL. Version `0.3.0` is the clean installation baseline. | `make check-pg-sql` and packaged-install tests reject SQL drift and artifact tampering. |

## Required Gate

| Command | Result | Test exclusions |
| --- | --- | --- |
| `make verify-contract` | Passed through the final documentation build. | 0 |
| `make test-conformance` | Passed all conformance packages, catalogs, evidence, inventory, and self-mutants. | 0 |
| `make test-blackbox` | Passed in 237.427 seconds with immutable PostgreSQL 18.3, extension, and adapter artifacts. | 0 |
| `make test-rust-core` | Passed 89 tests. | 0 |
| `make test-rust-pg` | Passed 185 tests. | 0 |
| `make test-adapter` | Passed all Go adapter and seed tests. | 0 |
| `make docs-build` | Passed contract verification and built 24 pages. | 0 |
| `make lint-go` | Passed formatting and `go vet`. | Not applicable |
| `make lint-rust-pg` | Passed formatting and Clippy with warnings denied. | Not applicable |
| `make lint-conformance` | Passed formatting and `go vet`. | Not applicable |
| `make check-pg-sql` | Passed byte-for-byte pgrx SQL regeneration. | Not applicable |
| `git diff --check` | Passed. | Not applicable |

The final real black-box gate used these nonsecret bindings:

- PostgreSQL: version 18.3
- Extension artifact: `dist/conformance/synchro-pg-pg18-r45`
- Adapter artifact: `dist/conformance/synchrod-pg-adapter-r34/synchrod-pg`

Private mode-0600 prerequisite files remained outside the repository.

## Mutation Gate

| Gate | Result |
| --- | --- |
| `make test-rust-mutants` | Passed. 745 mutants tested, 676 caught, 69 unviable, and 0 survived. |
| `make test-integration-mutants` | Passed. Five curated production mutants were killed and none survived. |

The curated integration categories were cursor advancement, WAL acknowledgement, mutation conservation, checksum correctness, and scope isolation.

The gate restores the unmodified pgrx installation after every run.
It now removes each isolated mutant target after its result to keep disk use bounded.

## Independent Review

Independent reviews covered these areas:

- PostgreSQL authority and the thin adapter boundary.
- Class 3 slot ownership, recovery, activation, and quiescent progress.
- Operator and worker credential separation.
- Pull, rebuild, push, membership, schema, integrity, seed, and installation evidence.
- CI and release-gate enforcement.
- Finding dispositions and negative-control coverage.
- Real selective-rebuild checkpoint isolation and concurrent compare-and-swap behavior.
- Scripted recovery actions for `preparing`, `baseline_staged`, `catching_up`, and `activated` states.
- Real coordinator termination, durable interruption, old-stage discard, and new-candidate completion.
- Registration source-gate coverage, Class 2 historical projection migration, adapter timeouts, strict authorization headers, and seed finalization.
- Empty string primary-key handling through source capture and registered membership resolution.

Unresolved Critical findings: 0.

Unresolved High findings: 0.

Unresolved confirmed Medium findings: 0.

One Low risk remains. A replication-privileged actor can recreate an exact aborted candidate slot name before cleanup.
Such an actor already has direct authority to remove replication slots.

## Failed Attempts

The following failures occurred and remained visible until correction:

- The first registration concurrency proof queried a busy asynchronous `dblink` connection and hung.
- The first full PostgreSQL rerun exposed durable registry state left by that proof.
- The first adapter rerun exposed a missing worker `UPDATE` grant for Class 2 projection migration.
- The first final black-box invocation omitted required local credential-file bindings.
- The first integration mutation rerun exhausted temporary disk space during mutant packaging.
- Final review found that empty string primary keys were valid but rejected during membership resolution.

The concurrency proof now uses isolated transactions and rolls back its remote registry changes.
The worker has the minimum additional projection privilege required by Class 2 migration.
Generated build caches were removed, and the integration mutation gate passed on rerun.
Empty string primary keys now pass source capture and registered membership resolution.

## Unresolved Items

- Full release promotion remains blocked by later release phases and candidate evidence.
- GitHub issue 34 remains open and unchanged.
- `npm ci` reports one moderate and three high dependency advisories.

## Exit Decision

Phase 4 is complete against its binding server exit gate.

Phase 5 can start only after the user or approved release sequence authorizes it.
