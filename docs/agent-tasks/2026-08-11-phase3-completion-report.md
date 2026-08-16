# Phase 3 Completion Report

Date: 2026-08-13

## Result

Phase 3 satisfies its binding exit gate on the current worktree.

- The complete ordered gate passed.
- The gate reported zero failed tests.
- The gate reported zero skipped, ignored, filtered, quarantined, or expected-failure tests.
- Independent review reports zero unresolved Critical or High Phase 3 findings.
- Release evidence remains fail-closed without a clean, complete candidate and Phase 6 verification.
- Protocol 4 work did not start.

## Implemented Outcome

Phase 3 now provides these verified surfaces:

- A standalone Go 1.25 conformance module with closed dependency and import policy.
- Strict contract, schema, scenario, catalog, vector, and source closure.
- An independent Protocol 3 reference model and scenario runner.
- Twenty-four authored scenarios with complete ownership and proof-obligation bindings.
- One hundred twenty-three canonical vectors with independent expected values.
- Four exact harness self-mutants and twenty-six authored negative controls.
- A strict synthetic HTTP black-box runner and a diagnostic Protocol 2 baseline.
- Real PostgreSQL 18.3 provisioning, role isolation, observer restrictions, cleanup, and restoration.
- Immutable candidate-lock and final-manifest evidence lifecycle rules.
- Content-addressed attachments and exact runner artifact bindings.
- An evidence command capability that runs `/usr/bin/make` from an immutable Git archive.
- A deterministic source-snapshot digest bound to the locked source commit.
- Sanitized Make, Git, loader, test-selector, and recursive Make environments.
- Separate authenticated Make observations and overall runner outcomes.
- Signed failed receipts for semantic failures after a successful Make gate.
- Active runner executable measurement against the sole locked runner payload.
- Locked scenario binding against the committed scenario catalog and supplied runner object.
- Concurrent receipt-consumption protection in `Builder.Build`.

## Binding Counts

- Scenarios: 24 total.
- Initial protocol scenarios: 10.
- Performance scenarios: 14.
- Canonical vectors: 123 total.
- Valid vectors: 57.
- Invalid vectors: 66.
- Closed operations: 25.
- Workload macros: 1.
- Configured-limit samples: 63 across 21 strata and 3 runs.
- Harness self-mutants: 4.
- Authored negative-control records: 26.
- Authored fault-plan records: 26.

Canonical vector digests:

- Source SHA-256: `db4eaa3f56fb4117b731b2ea0f362c8933c234056d3dea8b249c82c81ae86287`
- Aggregate SHA-256: `68a7c888c317ad64dde088e89873db8079d5fc7ba0f89ab4ec6b575ec1c01cb5`

## Final Gate

The commands ran in the required order from the current worktree.

| Command | Result | Test exclusions |
| --- | --- | --- |
| `make verify-contract` | Passed. Contract verification completed. | 0 |
| `make conformance-mod-download` | Passed. Standalone module dependencies resolved with `GOWORK=off`. | 0 |
| `make lint-conformance` | Passed. Formatting check and `go vet ./...` completed. | 0 |
| `make test-conformance` | Passed. All conformance packages, catalog checks, evidence, inventory, and mutants completed. | 0 |
| `make check-conformance-catalog` | Passed. The generated catalog matches authored sources. | 0 |
| `make test-blackbox` | Passed with the prepared PostgreSQL 18.3 gate environment. | 0 |
| `make test-rust-core` | Passed. 102 tests passed. Doc tests had 0 tests. | 0 |
| `make test-rust-pg` | Passed. 87 tests passed. Binary and doc test targets had 0 tests. | 0 |
| `make test-adapter` | Passed. Adapter and seed integration tests completed. Command-only packages had no test files. | 0 |
| `make docs-build` | Passed. Contract verification and 24-page static build completed. | 0 |
| `git diff --check` | Passed before and after this report was created. | Not applicable |

The real black-box command used these nonsecret environment bindings:

- PostgreSQL binaries: `~/.pgrx/18.3/pgrx-install/bin`
- Extension bundle: `dist/conformance/synchro-pg-pg18-gate`
- Adapter executable: `dist/conformance/synchrod-pg-adapter-gate/synchrod-pg`
- Private prerequisite files: the mode-0600 files under the prepared temporary gate directory.

## Fail-Closed Checks

| Command | Expected result | Observed result |
| --- | --- | --- |
| `make evidence` | Fail because `RC_CANDIDATE_DIR` is absent. | Failed with `RC_CANDIDATE_DIR is required`. |
| `make evidence RC_CANDIDATE_DIR="$PWD/dist/conformance"` | Reject an incomplete candidate path. | Failed before candidate reads because the source worktree is dirty. |
| `make release-check` | Remain nonzero until later release phases provide complete evidence. | Failed through the required `evidence` prerequisite. |

The partial-candidate test demonstrates the first source-integrity boundary.
Executable evidence tests also reject missing evidence and incomplete candidate closure.

## Focused Validation

These focused commands also passed during implementation:

| Command | Result |
| --- | --- |
| `make test-evidence` | Passed after command, receipt, scenario, and executable hardening. |
| `make test-blackbox-harness` | Passed after failed-receipt and scenario-binding changes. |
| `make test-inventory` | Passed after command and runner projections changed. |
| `make test-conformance-imports` | Passed after protected import edges changed. |
| `make lint-conformance` | Passed after all Go edits. |
| `make verify-contract` | Passed after evidence schema and self-test fixture changes. |
| `git diff --check` | Passed throughout final verification. |

## Failed Attempts

The following failures occurred during implementation and remained visible until fixed:

- The first `make lint-conformance` run found unformatted `conformance/evidence/evidence_test.go`.
- The first final-gate `make test-blackbox` run lacked required environment variables.
- Initial command snapshots rejected Git archive metadata and directory entries.
- Initial receipt integration omitted command observations from schemas and fixtures.
- Initial failed-receipt tests used obligations with unrelated performance or vector requirements.
- Initial runner executable authorization did not propagate through diagnostic fixture issuers.
- Initial scenario digest checks did not bind supplied scenario semantics correctly.
- Independent review found ambient Make, Git, loader, source mutation, runner artifact, and scenario-substitution risks.

Each implementation failure was fixed. Each applicable focused target then passed.
The final ordered gate reran from the integrated worktree.

## Independent Review

Independent reviews covered these areas:

- Reference-model independence and semantic correctness.
- Black-box process isolation and observer permissions.
- Protocol 2 diagnostic isolation from release evidence.
- Evidence hashing, closure, receipt issuance, and false-green paths.
- Mutant quality and negative controls.
- Source snapshots, command environments, scenario bindings, and runner executable identity.
- PostgreSQL 18.3 provisioning, installation, cleanup, and restoration.

The final focused review confirmed these closures:

- Changed caller-supplied scenario objects fail before runner execution.
- Candidate-local scenarios must match the committed catalog path and digest.
- Candidate Git validation removes ambient `PATH`, `GIT_*`, `DYLD_*`, and `LD_*` controls.
- `/usr/bin/git` and `/usr/bin/make` are explicit executable paths.

Unresolved Critical or High Phase 3 findings: 0.

## Changed Files

### Repository Orchestration And Contract Files

- `.github/workflows/ci.yml`
- `Makefile`
- `conformance/README.md`
- `conformance/catalog.json`
- `conformance/go.mod`
- `conformance/go.sum`
- `conformance/performance/budgets.json`
- `conformance/schemas/evidence-v2.schema.json`
- `conformance/schemas/rc-candidate-lock-v1.schema.json`
- `conformance/schemas/rc-manifest-v2.schema.json`
- `conformance/schemas/scenario-v2.schema.json`
- `conformance/test-inventory.md`
- `docs/scripts/verify-contract.mjs`

### Barriers

- `conformance/barriers/controller.go`
- `conformance/barriers/controller_test.go`
- `conformance/barriers/trace.go`
- `conformance/barriers/types.go`

### Black-Box Harness

- `conformance/blackbox/baseline/legacy_probe.go`
- `conformance/blackbox/baseline/runner.go`
- `conformance/blackbox/baseline/runner_test.go`
- `conformance/blackbox/baseline/types.go`
- `conformance/blackbox/compare.go`
- `conformance/blackbox/environment.go`
- `conformance/blackbox/environment_test.go`
- `conformance/blackbox/http_client.go`
- `conformance/blackbox/integration/real_baseline_test.go`
- `conformance/blackbox/jwt.go`
- `conformance/blackbox/normalize.go`
- `conformance/blackbox/process.go`
- `conformance/blackbox/process_test.go`
- `conformance/blackbox/recorder.go`
- `conformance/blackbox/runner.go`
- `conformance/blackbox/runner_test.go`
- `conformance/blackbox/synthetic.go`
- `conformance/blackbox/testdata/register-diagnostic-v2.sql`
- `conformance/blackbox/testdata/schema.sql`

### Commands

- `conformance/cmd/synchro-conformance/main.go`
- `conformance/cmd/synchro-conformance/main_test.go`
- `conformance/cmd/synchro-evidence/main.go`
- `conformance/cmd/synchro-evidence/main_test.go`

### Evidence And Execution

- `conformance/evidence/attachments.go`
- `conformance/evidence/builder.go`
- `conformance/evidence/candidate.go`
- `conformance/evidence/closure.go`
- `conformance/evidence/evidence_test.go`
- `conformance/evidence/inventory.go`
- `conformance/evidence/types.go`
- `conformance/evidence/validate.go`
- `conformance/execution/command.go`
- `conformance/execution/command_test.go`
- `conformance/execution/receipt.go`
- `conformance/execution/receipt_test.go`

### Faults

- `conformance/faults/artifact.go`
- `conformance/faults/controller.go`
- `conformance/faults/faults_test.go`
- `conformance/faults/load.go`
- `conformance/faults/process.go`
- `conformance/faults/types.go`
- `conformance/faults/wire.go`

### Internal Contract And Policy

- `conformance/internal/contract/contract_test.go`
- `conformance/internal/contract/ids.go`
- `conformance/internal/contract/load.go`
- `conformance/internal/contract/policy.go`
- `conformance/internal/contract/snapshot.go`
- `conformance/internal/contract/source_closure.go`
- `conformance/internal/contract/types.go`
- `conformance/internal/importguard/importguard.go`
- `conformance/internal/importguard/importguard_test.go`
- `conformance/internal/importguard/process_other.go`
- `conformance/internal/importguard/process_unix.go`
- `conformance/internal/importguard/process_unix_test.go`
- `conformance/internal/jsonstrict/decode.go`
- `conformance/internal/jsonstrict/decode_test.go`
- `conformance/internal/schemavalidator/validator.go`
- `conformance/internal/schemavalidator/validator_test.go`

### Inventory

- `conformance/inventory/generate.go`
- `conformance/inventory/inventory_test.go`
- `conformance/inventory/render.go`
- `conformance/inventory/types.go`
- `conformance/inventory/validate.go`

### Model Runner

- `conformance/modelrunner/macro.go`
- `conformance/modelrunner/runner.go`
- `conformance/modelrunner/runner_test.go`
- `conformance/modelrunner/seed.go`
- `conformance/modelrunner/seeded_startup_test.go`
- `conformance/modelrunner/semantic.go`
- `conformance/modelrunner/state_facts.go`
- `conformance/modelrunner/types.go`
- `conformance/modelrunner/workload_cardinality.go`
- `conformance/modelrunner/workload_cardinality_test.go`
- `conformance/modelrunner/workload_configured_limits.go`
- `conformance/modelrunner/workload_queue_limits.go`
- `conformance/modelrunner/workload_queue_limits_test.go`
- `conformance/modelrunner/workload_topology.go`
- `conformance/modelrunner/workload_topology_test.go`

### Mutants

- `conformance/mutants/base.go`
- `conformance/mutants/constant_checksum.go`
- `conformance/mutants/duplicate_delivery.go`
- `conformance/mutants/mutants_test.go`
- `conformance/mutants/omit_mutation.go`
- `conformance/mutants/runner.go`
- `conformance/mutants/types.go`
- `conformance/mutants/wrong_scope.go`

### Observer

- `conformance/observer/observer_test.go`
- `conformance/observer/postgres.go`
- `conformance/observer/read_only.go`
- `conformance/observer/types.go`

### Reference Model

- `conformance/reference/client.go`
- `conformance/reference/client_operations_test.go`
- `conformance/reference/clock.go`
- `conformance/reference/clock_test.go`
- `conformance/reference/connect.go`
- `conformance/reference/import_guard_test.go`
- `conformance/reference/install.go`
- `conformance/reference/membership.go`
- `conformance/reference/model.go`
- `conformance/reference/model_test.go`
- `conformance/reference/normalize.go`
- `conformance/reference/operations.go`
- `conformance/reference/operations_test.go`
- `conformance/reference/pull.go`
- `conformance/reference/pull_fault_test.go`
- `conformance/reference/pull_rebuild_operations_test.go`
- `conformance/reference/push.go`
- `conformance/reference/push_operations_test.go`
- `conformance/reference/rebuild.go`
- `conformance/reference/resolved.go`
- `conformance/reference/resolved_operations_test.go`
- `conformance/reference/retention.go`
- `conformance/reference/schema.go`
- `conformance/reference/seed.go`
- `conformance/reference/seed_connect_test.go`
- `conformance/reference/state.go`
- `conformance/reference/state_test.go`
- `conformance/reference/types.go`
- `conformance/reference/wal.go`
- `conformance/reference/wal_operations_test.go`

### Scenarios

- `conformance/scenarios/catalog.go`
- `conformance/scenarios/load.go`
- `conformance/scenarios/load_test.go`
- `conformance/scenarios/operations.go`
- `conformance/scenarios/operations_test.go`
- `conformance/scenarios/types.go`
- `conformance/scenarios/validate.go`
- `conformance/scenarios/validate_test.go`
- `conformance/scenarios/server/membership-reassignment-001.json`
- `conformance/scenarios/server/pull-divergent-checkpoints-001.json`
- `conformance/scenarios/server/pull-hydration-failure-001.json`
- `conformance/scenarios/server/push-response-loss-001.json`
- `conformance/scenarios/server/rebuild-forged-cursor-001.json`
- `conformance/scenarios/server/registry-reload-001.json`
- `conformance/scenarios/server/retention-reconnect-001.json`
- `conformance/scenarios/server/scenarios_test.go`
- `conformance/scenarios/server/schema-queued-mutation-001.json`
- `conformance/scenarios/server/wal-decode-failure-001.json`
- `conformance/scenarios/server/wal-order-001.json`
- `conformance/scenarios/performance/configured-bounds-001.json`
- `conformance/scenarios/performance/core-sync-path-001.json`
- `conformance/scenarios/performance/fanout-001.json`
- `conformance/scenarios/performance/multi-scope-provenance-001.json`
- `conformance/scenarios/performance/pending-cycle-001.json`
- `conformance/scenarios/performance/queue-replay-001.json`
- `conformance/scenarios/performance/rebuild-apply-001.json`
- `conformance/scenarios/performance/rebuild-cardinality-001.json`
- `conformance/scenarios/performance/rebuild-requests-001.json`
- `conformance/scenarios/performance/schema-check-001.json`
- `conformance/scenarios/performance/seeded-empty-startup-001.json`
- `conformance/scenarios/performance/shared-private-scopes-001.json`
- `conformance/scenarios/performance/steady-pull-001.json`
- `conformance/scenarios/performance/warm-connect-001.json`

### Vectors

- `conformance/vectors/canonical-v1.json`
- `conformance/vectors/catalog.json`
- `conformance/vectors/fingerprint.go`
- `conformance/vectors/load.go`
- `conformance/vectors/load_test.go`
- `conformance/vectors/mutants_test.go`
- `conformance/vectors/row_digest.go`
- `conformance/vectors/row_identity.go`
- `conformance/vectors/scope_digest.go`
- `conformance/vectors/typed_value.go`
- `conformance/vectors/types.go`
- `conformance/vectors/vectors_test.go`

### Local Task Records

- `docs/agent-tasks/2026-08-11-phase3-completion.md`
- `docs/agent-tasks/2026-08-11-phase3-completion-report.md`
- `docs/agent-tasks/2026-08-11-phase3-contract-repair-diagnosis.md`
- `docs/agent-tasks/2026-08-11-phase3-scenario-ownership-diagnosis.md`

### Concurrent Changes Outside Phase 3 Ownership

These modified files were present outside Phase 3 ownership.
They were not reverted or included as Phase 3-authored changes:

- `docs/src/content/docs/spec/06-conformance-plan.mdx`
- `docs/src/content/docs/spec/07-release-verification.mdx`

## Unresolved Items

- `make release-check` remains intentionally nonzero until later release phases provide complete candidate evidence.
- Phase 6 still owns Sigstore trust, signer custody, all-language vector closure, and promotion disposition.
- Native matrix, soak, canary, and final release closure remain later-phase work.
- `npm ci` reports four dependency advisories: one moderate and three high. The required docs commands still pass.

## Exit Decision

Phase 3 is complete against its binding gate.
Release promotion remains blocked by the intended later-phase controls.
