# Issue 49 Requirement Coverage Disposition

Date: 2026-09-08

## Scope

Issue 49 identified 85 requirements without authored scenario ownership.
The original gap was 85 of 111 requirements.
No requirements move to `proof authored`.

| Disposition | Count |
| --- | ---: |
| `moved to proof authored` | 0 |
| `genuinely unproven` | 85 |

## Running Counts

Updated: 2026-09-09

| State | Count |
| --- | ---: |
| `proof authored and all required controls demonstrated` | 0 |
| `proof implementation in progress` | 85 |
| `genuinely unproven` | 85 |

No requirement leaves `genuinely unproven` until every required proof type executes.
Each requirement also requires a demonstrated failing negative control.

The 26 requirements that were already mapped are outside this report.
This report covers only the original 85 unmapped requirements.

## Method

The required proof types come from `conformance/requirements.json`.
The catalog references identify a planned control only.
A catalog-only control is not an executed proof.

`Partial test` identifies an existing behavior test.
It does not satisfy the omitted proof obligations.
`Catalog-only` means no complete eligible behavior proof was identified for the requirement.

## Requirement Dispositions

| Requirement | Disposition | Evidence |
| --- | --- | --- |
| `SYNC-TIME-001` | genuinely unproven | Catalog-only: `CTRL-TIMESTAMP-001` defines the noncanonical-time fault at `conformance/faults/catalog.json:119`. It is not executed. Missing: server-black-box, native-e2e, negative-control. |
| `SYNC-CURSOR-001` | genuinely unproven | Catalog-only: `CTRL-CURSOR-001` defines cursor rewriting at `conformance/faults/catalog.json:120`. It is not executed. Missing: reference-model, native-e2e, negative-control. |
| `SYNC-VERSION-001` | genuinely unproven | Catalog-only: `CTRL-VERSION-001` defines opaque-version rewriting at `conformance/faults/catalog.json:121`. It is not executed. Missing: reference-model, native-e2e, negative-control. |
| `SYNC-SCOPE-002` | genuinely unproven | Catalog-only: `CTRL-SCOPE-002` defines unchanged-set advancement and changed-set reuse at `conformance/faults/catalog.json:123`. It is not executed. Missing: reference-model, server-black-box, native-e2e, negative-control. |
| `SYNC-SCHEMA-001` | genuinely unproven | Partial test: `TestRealSchemaIncompatibleMutationPersistsCanonicalIntent` transitions one schema at `conformance/blackbox/integration/real_registry_schema_test.go:103`. It does not test server schema choice or the continuing-client fresh sentinel. Catalog-only: `CTRL-SCHEMA-001` at `conformance/faults/catalog.json:124`. Missing: server-black-box, native-e2e, negative-control. |
| `SYNC-LOCALSQL-001` | genuinely unproven | Catalog-only: `CTRL-LOCALSQL-001` defines a rerouted local CRUD path at `conformance/faults/catalog.json:125`. It is not executed. Missing: native-e2e, negative-control. |
| `SYNC-CRUD-001` | genuinely unproven | Partial test: `TestRealMutationControlMutationConservation` submits server mutations at `conformance/blackbox/integration/real_mutation_controls_test.go:171`. It does not cover all registered tables or local client CRUD. Catalog-only: `CTRL-CRUD-001` at `conformance/faults/catalog.json:126`. Missing: reference-model, server-black-box, native-e2e, negative-control. |
| `SYNC-MUTATION-001` | genuinely unproven | Partial test: `TestRealMutationControlMutationConservation` checks one server response partition at `conformance/blackbox/integration/real_mutation_controls_test.go:223`. It does not prove durable client outcomes across every terminal state. Catalog-only: `CTRL-MUTATION-001` at `conformance/faults/catalog.json:127`. Missing: reference-model, native-e2e, fault-injection, negative-control. |
| `SYNC-MUTATION-003` | genuinely unproven | Catalog-only: `CTRL-MUTATION-003` defines replay and echo duplication at `conformance/faults/catalog.json:129`. It is not executed. Missing: reference-model, native-e2e, fault-injection, negative-control. |
| `SYNC-APPLY-001` | genuinely unproven | Catalog-only: `CTRL-APPLY-001` defines an apply-trigger bypass at `conformance/faults/catalog.json:130`. It is not executed. Missing: native-e2e, negative-control. |
| `SYNC-VERSION-002` | genuinely unproven | Partial test: `TestRealWALPipeline` checks opaque UUID row versions at `conformance/blackbox/integration/real_baseline_test.go:340`. It does not prove every accepted write mints a new server version. Catalog-only: `CTRL-VERSION-002` at `conformance/faults/catalog.json:131`. Missing: server-black-box, native-e2e, negative-control. |
| `SYNC-CLEANUP-001` | genuinely unproven | Catalog-only: `CTRL-CLEANUP-001` defines synthetic cleanup mutations at `conformance/faults/catalog.json:132`. It is not executed. Missing: native-e2e, negative-control. |
| `SYNC-WAL-001` | genuinely unproven | Partial test: `TestRealWALDecodeFailureRepairsSameIdentity` at `conformance/blackbox/integration/real_baseline_test.go:766` observes same-identity repair. It does not execute the materialization-commit crash or prove complete pull-visible representation. Missing: server-black-box, fault-injection, negative-control. |
| `SYNC-REBUILD-001` | genuinely unproven | Partial test: `TestRealS05SelectiveRebuildPreservesCheckpoints` preserves server checkpoints at `conformance/blackbox/integration/real_baseline_test.go:1089`. It does not prove local scope isolation or local-only row preservation. Catalog-only: `CTRL-REBUILD-001` at `conformance/faults/catalog.json:135`. Missing: reference-model, server-black-box, native-e2e, negative-control. |
| `SYNC-CURSOR-002` | genuinely unproven | Partial test: `TestRealMutationControlCursorAdvancement` checks returned server cursor shape at `conformance/blackbox/integration/real_mutation_controls_test.go:18`. It does not prove durable local apply before cursor advancement. Catalog-only: `CTRL-CURSOR-002` at `conformance/faults/catalog.json:136`. Missing: reference-model, native-e2e, fault-injection, negative-control. |
| `SYNC-INTEGRITY-001` | genuinely unproven | Partial test: `TestRealMutationControlChecksumCorrectness` verifies response digests at `conformance/blackbox/integration/real_mutation_controls_test.go:261`. It does not prove client health remains false with a valid cursor and mismatched materialization. Catalog-only: `CTRL-INTEGRITY-001` at `conformance/faults/catalog.json:137`. Missing: reference-model, server-black-box, native-e2e, negative-control. |
| `SYNC-PROVENANCE-001` | genuinely unproven | Catalog-only: `CTRL-PROVENANCE-001` defines loss of an overlapping row during rebuild at `conformance/faults/catalog.json:140`. It is not executed. Missing: reference-model, native-e2e, negative-control. |
| `SYNC-QUEUE-001` | genuinely unproven | Catalog-only: `CTRL-QUEUE-001` defines a crash before first transmission at `conformance/faults/catalog.json:142`. It is not executed. Missing: native-e2e, fault-injection, negative-control. |
| `SYNC-REBUILD-004` | genuinely unproven | Catalog-only: `CTRL-REBUILD-004` defines a crash between page apply and final state at `conformance/faults/catalog.json:143`. It is not executed. Missing: native-e2e, fault-injection, negative-control. |
| `SYNC-CURSOR-003` | genuinely unproven | Catalog-only: `CTRL-CURSOR-003` defines split apply and cursor persistence at `conformance/faults/catalog.json:144`. It is not executed. Missing: native-e2e, fault-injection, negative-control. |
| `SYNC-MUTATION-004` | genuinely unproven | Partial test: `TestRealSchemaIncompatibleMutationPersistsCanonicalIntent` retains one server-side incompatible intent at `conformance/blackbox/integration/real_registry_schema_test.go:103`. It does not prove native rejected-reason retention across restart. Catalog-only: `CTRL-MUTATION-004` at `conformance/faults/catalog.json:145`. Missing: native-e2e, fault-injection, negative-control. |
| `SYNC-SEED-002` | genuinely unproven | Catalog-only: `CTRL-SEED-002` defines unauthorized portable-seed contents at `conformance/faults/catalog.json:148`. It is not executed. Missing: server-black-box, native-e2e, negative-control. |
| `SYNC-SEED-003` | genuinely unproven | Catalog-only: `CTRL-SEED-003` defines a metadata-free seed continuation at `conformance/faults/catalog.json:149`. It is not executed. Missing: server-black-box, native-e2e, fault-injection, negative-control. |
| `SYNC-QUEUE-002` | genuinely unproven | Catalog-only: `CTRL-QUEUE-002` defines pre-connect intent and fabricated seed bases at `conformance/faults/catalog.json:150`. It is not executed. Missing: native-e2e, fault-injection, negative-control. |
| `SYNC-FAILURE-001` | genuinely unproven | Catalog-only: `CTRL-FAILURE-001` defines retryable transport loss at `conformance/faults/catalog.json:151`. It is not executed. Missing: native-e2e, fault-injection, negative-control. |
| `SYNC-FAILURE-002` | genuinely unproven | Catalog-only: `CTRL-FAILURE-002` defines silent continuation after a non-retryable error at `conformance/faults/catalog.json:152`. It is not executed. Missing: server-black-box, native-e2e, negative-control. |
| `SYNC-REBUILD-005` | genuinely unproven | Catalog-only: `CTRL-REBUILD-005` defines delayed automatic rebuild at `conformance/faults/catalog.json:153`. It is not executed. Missing: native-e2e, negative-control. |
| `SYNC-BOUNDARY-001` | genuinely unproven | Catalog-only: `CTRL-BOUNDARY-001` defines semantic work in the Go adapter at `conformance/faults/catalog.json:154`. It is not executed. Missing: server-black-box, negative-control. |
| `SYNC-BOUNDARY-002` | genuinely unproven | Catalog-only: `CTRL-BOUNDARY-002` defines JavaScript-owned sync behavior at `conformance/faults/catalog.json:155`. It is not executed. Missing: native-e2e, negative-control. |
| `SYNC-BOUNDARY-003` | genuinely unproven | Catalog-only: `CTRL-BOUNDARY-003` defines client override of server decisions at `conformance/faults/catalog.json:156`. It is not executed. Missing: native-e2e, negative-control. |
| `SYNC-CRUD-002` | genuinely unproven | Partial test: `TestRealMutationControlMutationConservation` uses the canonical push route at `conformance/blackbox/integration/real_mutation_controls_test.go:217`. It does not prove local SQL CRUD completes without an application upload endpoint. Catalog-only: `CTRL-CRUD-002` at `conformance/faults/catalog.json:157`. Missing: server-black-box, native-e2e, negative-control. |
| `SYNC-SCOPE-005` | genuinely unproven | Catalog-only: `CTRL-SCOPE-005` defines ad hoc scope SQL and predicates at `conformance/faults/catalog.json:158`. It is not executed. Missing: server-black-box, negative-control. |
| `SYNC-VOCAB-001` | genuinely unproven | Partial test: `TestRealS17InvalidPushShapesDoNoDurableWork` rejects `upsert` at `conformance/blackbox/integration/real_push_retention_test.go:785`. It does not prove the complete push and pull vocabulary. Catalog-only: `CTRL-VOCAB-001` at `conformance/faults/catalog.json:160`. Missing: reference-model, server-black-box, negative-control. |
| `SYNC-WAL-002` | genuinely unproven | Partial test: `TestRealWALPipeline` observes materialized WAL records at `conformance/blackbox/integration/real_baseline_test.go:283`. It does not reject a second synchronous publication path. Catalog-only: `CTRL-WAL-002` at `conformance/faults/catalog.json:161`. Missing: server-black-box, fault-injection, negative-control. |
| `SYNC-WAL-004` | genuinely unproven | Partial test: `TestRealWALPipeline` restarts the worker before acknowledgement at `conformance/blackbox/integration/real_baseline_test.go:352`. It does not prove one atomic replay result for every source transaction. Catalog-only: `CTRL-WAL-004` at `conformance/faults/catalog.json:163`. Missing: server-black-box, fault-injection, negative-control. |
| `SYNC-WAL-005` | genuinely unproven | Partial test: `TestRealMutationControlWALAcknowledgement` checks one acknowledged end LSN at `conformance/blackbox/integration/real_mutation_controls_test.go:77`. It does not test an incomplete noncontiguous predecessor. Catalog-only: `CTRL-WAL-005` at `conformance/faults/catalog.json:164`. Missing: server-black-box, fault-injection, negative-control. |
| `SYNC-REGISTRY-001` | genuinely unproven | Partial test: `TestRealRegistryGenerationReloadAtCommitBoundary` checks one activation boundary at `conformance/blackbox/integration/real_registry_schema_test.go:25`. It does not test same-name schemas or OID drift. Catalog-only: `CTRL-REGISTRY-001` at `conformance/faults/catalog.json:167`. Missing: server-black-box, fault-injection, negative-control. |
| `SYNC-REGISTRY-002` | genuinely unproven | Catalog-only: `CTRL-REGISTRY-002` defines nonportable keys and replica-identity drift at `conformance/faults/catalog.json:168`. It is not executed. Missing: server-black-box, negative-control. |
| `SYNC-WAL-008` | genuinely unproven | Catalog-only: `CTRL-WAL-008` defines partial capture readiness at `conformance/faults/catalog.json:169`. It is not executed. Missing: server-black-box, fault-injection, negative-control. |
| `SYNC-IDEMPOTENCY-002` | genuinely unproven | Partial test: `TestRealS11PushResponseLossReplaysExactCanonicalResponse` replays one batch at `conformance/blackbox/integration/real_push_retention_test.go:27`. It does not test reuse of one mutation identity in a new batch. Catalog-only: `CTRL-IDEMPOTENCY-002` at `conformance/faults/catalog.json:171`. Missing: reference-model, server-black-box, native-e2e, negative-control. |
| `SYNC-IDEMPOTENCY-003` | genuinely unproven | Catalog-only: `CTRL-IDEMPOTENCY-003` defines ledger deletion before retirement at `conformance/faults/catalog.json:172`. It is not executed. Missing: server-black-box, fault-injection, negative-control. |
| `SYNC-ATOMICITY-001` | genuinely unproven | Partial test: `TestRealS20PushMutationCountBoundsAreAtomic` rejects one oversized push before durable work at `conformance/blackbox/integration/real_push_retention_test.go:896`. It does not test an operational failure during first-push commit. Catalog-only: `CTRL-ATOMICITY-001` at `conformance/faults/catalog.json:173`. Missing: server-black-box, fault-injection, negative-control. |
| `SYNC-CONFLICT-001` | genuinely unproven | Partial test: `TestRealS16ConcurrentPushCASIgnoresClientTime` runs a concurrent compare-and-swap case at `conformance/blackbox/integration/real_baseline_test.go:1198`. It does not prove insert reservation or atomic fence transition. Catalog-only: `CTRL-CONFLICT-001` at `conformance/faults/catalog.json:175`. Missing: reference-model, server-black-box, fault-injection, negative-control. |
| `SYNC-CONFLICT-002` | genuinely unproven | Partial test: `TestRealS16ConcurrentPushCASIgnoresClientTime` checks one concurrent write race at `conformance/blackbox/integration/real_baseline_test.go:1198`. It does not prove concurrent update-delete conservation. Catalog-only: `CTRL-CONFLICT-002` at `conformance/faults/catalog.json:176`. Missing: reference-model, server-black-box, fault-injection, negative-control. |
| `SYNC-TIME-002` | genuinely unproven | Partial test: `TestRealS16ConcurrentPushCASIgnoresClientTime` varies client time in one CAS race at `conformance/blackbox/integration/real_baseline_test.go:1198`. It does not provide the required reference model or negative control. Catalog-only: `CTRL-TIME-002` at `conformance/faults/catalog.json:177`. Missing: reference-model, server-black-box, negative-control. |
| `SYNC-FAILURE-003` | genuinely unproven | Partial test: `TestRealS11PushResponseLossReplaysExactCanonicalResponse` covers response loss at `conformance/blackbox/integration/real_push_retention_test.go:27`. It does not cover HTTP 429, HTTP 503, or native sealed retries. Catalog-only: `CTRL-FAILURE-003` at `conformance/faults/catalog.json:179`. Missing: server-black-box, native-e2e, fault-injection, negative-control. |
| `SYNC-QUEUE-003` | genuinely unproven | Catalog-only: `CTRL-QUEUE-003` defines changed and unchanged queued intent at `conformance/faults/catalog.json:180`. It is not executed. Missing: native-e2e, fault-injection, negative-control. |
| `SYNC-PULL-004` | genuinely unproven | Partial test: `TestRealS02DivergentPullPaginationIsStarvationFree` at `conformance/blackbox/integration/real_pull_rebuild_test.go:12` uses divergent scoped pages. It lacks one table-key in two scopes and repeated positions for one tuple. Missing: reference-model, server-black-box, negative-control. |
| `SYNC-CURSOR-004` | genuinely unproven | Partial test: `TestRealS05SelectiveRebuildPreservesCheckpoints` shows no checkpoint advance before later acknowledgement at `conformance/blackbox/integration/real_baseline_test.go:1177`. It does not test old-token rejection or native acknowledgement. Catalog-only: `CTRL-CURSOR-004` at `conformance/faults/catalog.json:185`. Missing: server-black-box, native-e2e, fault-injection, negative-control. |
| `SYNC-CURSOR-005` | genuinely unproven | Partial test: `TestRealS04RebuildRejectsForgedCursorAndFreezesBoundary` checks a rebuild cursor at `conformance/blackbox/integration/real_pull_rebuild_test.go:238`. It does not test every incremental cursor binding or forged-versus-stale dispatch. Catalog-only: `CTRL-CURSOR-005` at `conformance/faults/catalog.json:186`. Missing: reference-model, server-black-box, native-e2e, negative-control. |
| `SYNC-REBUILD-008` | genuinely unproven | Partial test: `TestRealS05SelectiveRebuildPreservesCheckpoints` delivers one unrelated unread change after rebuild at `conformance/blackbox/integration/real_baseline_test.go:1177`. It does not prove post-boundary source writes remain out of a staged snapshot. Catalog-only: `CTRL-REBUILD-008` at `conformance/faults/catalog.json:189`. Missing: reference-model, server-black-box, native-e2e, negative-control. |
| `SYNC-REBUILD-009` | genuinely unproven | Partial test: `TestRealS04RebuildRejectsForgedCursorAndFreezesBoundary` exercises one rebuild session at `conformance/blackbox/integration/real_pull_rebuild_test.go:238`. It does not test exact page replay. Catalog-only: `CTRL-REBUILD-009` at `conformance/faults/catalog.json:190`. Missing: server-black-box, native-e2e, fault-injection, negative-control. |
| `SYNC-REBUILD-010` | genuinely unproven | Partial test: `TestRealS05SelectiveRebuildPreservesCheckpoints` checks checkpoint isolation for one rebuild at `conformance/blackbox/integration/real_baseline_test.go:1168`. It does not cover native behavior or injected failure. Catalog-only: `CTRL-REBUILD-010` at `conformance/faults/catalog.json:191`. Missing: server-black-box, native-e2e, fault-injection, negative-control. |
| `SYNC-INTEGRITY-003` | genuinely unproven | Partial test: `TestRealMutationControlChecksumCorrectness` computes one row digest independently at `conformance/blackbox/integration/real_mutation_controls_test.go:261`. It does not reject all invalid typed-row encodings before apply. Catalog-only: `CTRL-INTEGRITY-003` at `conformance/faults/catalog.json:192`. Missing: reference-model, server-black-box, native-e2e, negative-control. |
| `SYNC-INTEGRITY-004` | genuinely unproven | Partial test: `TestRealMutationControlChecksumCorrectness` binds one returned row digest at `conformance/blackbox/integration/real_mutation_controls_test.go:292`. It does not cover every row-bearing protocol path or client rejection. Catalog-only: `CTRL-INTEGRITY-004` at `conformance/faults/catalog.json:193`. Missing: reference-model, server-black-box, native-e2e, negative-control. |
| `SYNC-INTEGRITY-005` | genuinely unproven | Partial test: `TestRealMutationControlChecksumCorrectness` checks one terminal scope digest at `conformance/blackbox/integration/real_mutation_controls_test.go:305`. It does not test duplicate, omitted, or changed digest inputs. Catalog-only: `CTRL-INTEGRITY-005` at `conformance/faults/catalog.json:194`. Missing: reference-model, server-black-box, native-e2e, negative-control. |
| `SYNC-INTEGRITY-006` | genuinely unproven | Partial test: `TestRealMutationControlChecksumCorrectness` observes one terminal checksum map at `conformance/blackbox/integration/real_mutation_controls_test.go:305`. It does not prove the complete active-scope map or client failure without cursor advance. Catalog-only: `CTRL-INTEGRITY-006` at `conformance/faults/catalog.json:195`. Missing: reference-model, server-black-box, native-e2e, negative-control. |
| `SYNC-STATE-001` | genuinely unproven | Catalog-only: `CTRL-STATE-001` defines unknown and unlisted client transitions at `conformance/faults/catalog.json:196`. It is not executed. Missing: reference-model, native-e2e, negative-control. |
| `SYNC-CLIENT-VERSION-001` | genuinely unproven | Catalog-only: `CTRL-CLIENT-VERSION-001` defines malformed and leading-v SemVer inputs at `conformance/faults/catalog.json:197`. It is not executed. Missing: reference-model, server-black-box, negative-control. |
| `SYNC-DBAUTH-001` | genuinely unproven | Catalog-only: `CTRL-DBAUTH-001` defines public authority and direct metadata access at `conformance/faults/catalog.json:198`. It is not executed. Missing: server-black-box, negative-control. |
| `SYNC-DBAUTH-002` | genuinely unproven | Catalog-only: `CTRL-DBAUTH-002` defines excess runtime role authority at `conformance/faults/catalog.json:199`. It is not executed. Missing: server-black-box, negative-control. |
| `SYNC-HEALTH-001` | genuinely unproven | Catalog-only: `CTRL-HEALTH-001` defines incomplete readiness checks and detailed output at `conformance/faults/catalog.json:200`. It is not executed. Missing: server-black-box, fault-injection, negative-control. |
| `SYNC-HEALTH-002` | genuinely unproven | Catalog-only: `CTRL-HEALTH-002` defines invalid capture-lag observations and limits at `conformance/faults/catalog.json:201`. It is not executed. Missing: server-black-box, fault-injection, negative-control. |
| `SYNC-LOGGING-001` | genuinely unproven | Catalog-only: `CTRL-LOGGING-001` defines operational-data disclosure at `conformance/faults/catalog.json:202`. It is not executed. Missing: server-black-box, fault-injection, negative-control. |
| `SYNC-SEED-004` | genuinely unproven | Catalog-only: `CTRL-SEED-004` defines cross-transaction seed reads at `conformance/faults/catalog.json:203`. It is not executed. Missing: server-black-box, native-e2e, fault-injection, negative-control. |
| `SYNC-SEED-005` | genuinely unproven | Catalog-only: `CTRL-SEED-005` defines a misbound seed token or receipt at `conformance/faults/catalog.json:204`. It is not executed. Missing: server-black-box, native-e2e, fault-injection, negative-control. |
| `SYNC-SEED-006` | genuinely unproven | Catalog-only: `CTRL-SEED-006` defines seed publication after skipped verification at `conformance/faults/catalog.json:205`. It is not executed. Missing: server-black-box, native-e2e, fault-injection, negative-control. |
| `SYNC-INSTALL-001` | genuinely unproven | Partial test: `TestExtensionControlUsesFixedSynchroSchema` checks the fixed schema declaration at `api/go/schema_qualification_test.go:43`. It does not compare generated pgrx SQL byte-for-byte. Catalog-only: `CTRL-INSTALL-001` at `conformance/faults/catalog.json:206`. Missing: server-black-box, negative-control. |
| `SYNC-INSTALL-002` | genuinely unproven | Partial test: `TestRealExtensionReinstallRebindsWorkerSlot` reinstalls the extension at `conformance/blackbox/integration/real_extension_reinstall_test.go:11`. It does not prove a clean PostgreSQL 18 baseline or reject pre-0.3.0 migration logic. Catalog-only: `CTRL-INSTALL-002` at `conformance/faults/catalog.json:207`. Missing: server-black-box, negative-control. |
| `SYNC-MEMBERSHIP-002` | genuinely unproven | Partial test: `TestRealRegistryGenerationReloadAtCommitBoundary` observes one generation change at `conformance/blackbox/integration/real_registry_schema_test.go:76`. It does not cover membership invalidation, native rebuild, or injected failure. Catalog-only: `CTRL-MEMBERSHIP-002` at `conformance/faults/catalog.json:210`. Missing: server-black-box, native-e2e, fault-injection, negative-control. |
| `SYNC-MEMBERSHIP-003` | genuinely unproven | Catalog-only: `CTRL-MEMBERSHIP-003` defines partial membership backfill at `conformance/faults/catalog.json:211`. It is not executed. Missing: server-black-box, fault-injection, negative-control. |
| `SYNC-SCHEMA-003` | genuinely unproven | Partial test: `TestRealSchemaIncompatibleMutationPersistsCanonicalIntent` retains stable table identity through one transition at `conformance/blackbox/integration/real_registry_schema_test.go:103`. It does not prove immutable manifests or all required proof types. Catalog-only: `CTRL-SCHEMA-003` at `conformance/faults/catalog.json:212`. Missing: reference-model, server-black-box, native-e2e, fault-injection, negative-control. |
| `SYNC-RETENTION-001` | genuinely unproven | Partial test: `TestRealS12StaleClientCompactionAndReconnect` performs one stale-client reconnect at `conformance/blackbox/integration/real_push_retention_test.go:465`. It does not test an empty effect log or an inclusive retention floor. Catalog-only: `CTRL-RETENTION-001` at `conformance/faults/catalog.json:214`. Missing: reference-model, server-black-box, fault-injection, negative-control. |
| `SYNC-QUEUE-004` | genuinely unproven | Catalog-only: `CTRL-QUEUE-004` defines dependent same-row intent and insert-delete normalization at `conformance/faults/catalog.json:218`. It is not executed. Missing: reference-model, native-e2e, fault-injection, negative-control. |
| `SYNC-WAL-009` | genuinely unproven | Partial test: `TestRealWALPipeline` observes one materialized fence record at `conformance/blackbox/integration/real_baseline_test.go:340`. It does not prove one-to-one AFTER-fence correlation or capture_pending behavior. Catalog-only: `CTRL-WAL-009` at `conformance/faults/catalog.json:219`. Missing: server-black-box, fault-injection, negative-control. |
| `SYNC-PROTOCOL-004` | genuinely unproven | Catalog-only: `CTRL-PROTOCOL-004` defines portable integer overflow and rounding at `conformance/faults/catalog.json:220`. It is not executed. Missing: reference-model, server-black-box, native-e2e, negative-control. |
| `SYNC-OUTCOME-002` | genuinely unproven | Partial test: `TestRealSchemaIncompatibleMutationPersistsCanonicalIntent` retains one incompatible intent at `conformance/blackbox/integration/real_registry_schema_test.go:103`. It does not prove historical-schema outcome replay or safe client reconciliation. Catalog-only: `CTRL-OUTCOME-002` at `conformance/faults/catalog.json:221`. Missing: reference-model, server-black-box, native-e2e, negative-control. |
| `SYNC-WAL-010` | genuinely unproven | Partial test: `TestRealClass3ProjectionBootstrap` observes one candidate slot during bootstrap at `conformance/blackbox/integration/real_baseline_test.go:25`. It does not prove durable reset lifecycle or gap-free activation. Catalog-only: `CTRL-WAL-010` at `conformance/faults/catalog.json:222`. Missing: server-black-box, fault-injection, negative-control. |
| `SYNC-WAL-011` | genuinely unproven | Partial test: `TestRealClass3ProjectionBootstrap` stages one projection bootstrap at `conformance/blackbox/integration/real_baseline_test.go:25`. It does not prove exact baseline fence coverage. Catalog-only: `CTRL-WAL-011` at `conformance/faults/catalog.json:223`. Missing: server-black-box, fault-injection, negative-control. |
| `SYNC-PULL-005` | genuinely unproven | Catalog-only: `CTRL-PULL-005` defines sibling-effect replay, skip, and ordering faults at `conformance/faults/catalog.json:224`. It is not executed. Missing: reference-model, server-black-box, native-e2e, negative-control. |
| `SYNC-REBUILD-011` | genuinely unproven | Catalog-only: `CTRL-REBUILD-011` defines replay from an older accepted-write epoch at `conformance/faults/catalog.json:225`. It is not executed. Missing: reference-model, server-black-box, native-e2e, fault-injection, negative-control. |
| `SYNC-SCHEMA-005` | genuinely unproven | Partial test: `TestRealClass3ProjectionBootstrap` stages and catches up one late source at `conformance/blackbox/integration/real_baseline_test.go:25`. It does not provide the required reference model or fault-injection proof. Catalog-only: `CTRL-SCHEMA-005` at `conformance/faults/catalog.json:226`. Missing: reference-model, server-black-box, fault-injection, negative-control. |
| `SYNC-SCHEMA-006` | genuinely unproven | Catalog-only: `CTRL-SCHEMA-006` defines interrupted client migration and mixed physical state at `conformance/faults/catalog.json:227`. It is not executed. Missing: reference-model, native-e2e, fault-injection, negative-control. |
| `SYNC-SCOPE-006` | genuinely unproven | Catalog-only: `CTRL-SCOPE-006` defines cleanup that deletes intent-protected local rows at `conformance/faults/catalog.json:228`. It is not executed. Missing: reference-model, native-e2e, fault-injection, negative-control. |
| `SYNC-SCHEMA-007` | genuinely unproven | Partial test: `TestRealS12StaleClientCompactionAndReconnect` checks one reconnect schema response at `conformance/blackbox/integration/real_push_retention_test.go:465`. It does not prove same-position cursor replacement or native atomic installation. Catalog-only: `CTRL-SCHEMA-007` at `conformance/faults/catalog.json:229`. Missing: reference-model, server-black-box, native-e2e, negative-control. |

## Validation Results

The following results were reported from local execution.

| Command | Result |
| --- | --- |
| `make evidence` | Passed. |
| `make coverage-report` | Failed closed at `SYNC-TIME-001`. |
| `make test-conformance` | Passed. |
| `make verify-contract` | Passed. |
| `make test-blackbox-harness` | Passed 126 tests. |
| Scenario tests | Passed 350 tests locally. |
| `make test-blackbox` | First desktop run failed from an unsourced environment. |
| `. .ignore/r2/cf-secrets` then `make test-blackbox` | The second desktop run loaded the environment. Both partial real tests passed. The full suite failed at `TestRealNativeCaptureServerObservationSignals`. |

## Desktop Execution

| Command | Result |
| --- | --- |
| `ssh desktop 'ls ~'` | Passed. The desktop home directory was available. |
| Required `rsync` command | Timed out after 120 seconds while copying ignored build products. |
| Required `rsync` command and secret-file copy | Timed out after 600 seconds. |
| `ssh desktop 'ls ~/synchro-req49/conformance/scenarios/server/wal-decode-failure-001.json'` | Failed. The incomplete copy had not reached the scenario. |
| `rsync` with additional build-directory exclusions | Timed out after 600 seconds. |
| Desktop size and process diagnostics | Found a 35 GB partial checkout and no active `rsync` process. |
| Remove scratch, tracked-file `rsync`, and secret-file copy | Passed. This avoided ignored build products. |
| Initialize scratch Git metadata and fetch `79e54d7cdd5f963d49ecffb7bd014b9f03b79448` | Passed. |
| Locked `make test-blackbox` without loading the environment file | Failed because required `SYNCHRO_CONFORMANCE_*` variables were absent. |
| Locked `. .ignore/r2/cf-secrets` and `make test-blackbox` | Both partial real tests passed. The suite failed at `TestRealNativeCaptureServerObservationSignals`. |
| Verify and remove `~/synchro-req49` | Passed. The scratch checkout no longer exists. |
