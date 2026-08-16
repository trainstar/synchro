# Phase 3 completion

## Result

Complete Phase 3 of the independent verification plan.

The complete exit gate in `docs/superpowers/plans/2026-07-19-phase-3-independent-verification.md` must pass.

Do not start Protocol 4.

## Roles

The orchestrator owns all design decisions in this brief.

The implementer changes code and tests only. The implementer must not commit, push, use the network, or change this brief.

If this brief conflicts with the repository, stop. Report the conflict with file and line evidence. Do not change code to make a false premise true.

## Existing work

Tasks 1 through 6 entered this work as an uncommitted implementation.

First, validate that implementation against the plan. Preserve correct work. Fix defects only when the plan or this brief gives an exact result.

Complete Tasks 7 through 14 in order. Do not omit a task, test, negative control, or exit-gate command.

Use these findings as evidence:

- `/tmp/trainstar-synchro-phase3-ownership-proposal.md`
- `/tmp/trainstar-synchro-phase3-repair-evidence.md`

The plan and this brief are binding. The findings files supply traceable evidence. They do not override this brief.

## File ownership

This task owns only these paths:

- `conformance/**`
- `Makefile`
- `.github/workflows/ci.yml`
- `docs/scripts/verify-contract.mjs`
- `docs/agent-tasks/2026-08-11-phase3-completion-report.md`

Do not change another path. Report a required out-of-scope change to the orchestrator.

## Closed scenario catalog

Create these ten protocol scenario IDs:

```text
SCN-WAL-ORDER-001
SCN-PULL-DIVERGENT-CHECKPOINTS-001
SCN-PULL-HYDRATION-FAILURE-001
SCN-WAL-DECODE-FAILURE-001
SCN-REGISTRY-RELOAD-001
SCN-PUSH-RESPONSE-LOSS-001
SCN-REBUILD-FORGED-CURSOR-001
SCN-SCHEMA-QUEUED-MUTATION-001
SCN-RETENTION-RECONNECT-001
SCN-MEMBERSHIP-REASSIGNMENT-001
```

Create these fourteen performance scenario IDs:

```text
SCN-PERF-WARM-CONNECT-001
SCN-PERF-STEADY-PULL-001
SCN-PERF-PENDING-CYCLE-001
SCN-PERF-REBUILD-REQUESTS-001
SCN-PERF-CORE-SYNC-PATH-001
SCN-PERF-FANOUT-001
SCN-PERF-SHARED-PRIVATE-SCOPES-001
SCN-PERF-REBUILD-CARDINALITY-001
SCN-PERF-SCHEMA-CHECK-001
SCN-PERF-SEEDED-EMPTY-STARTUP-001
SCN-PERF-QUEUE-REPLAY-001
SCN-PERF-REBUILD-APPLY-001
SCN-PERF-MULTI-SCOPE-PROVENANCE-001
SCN-PERF-CONFIGURED-BOUNDS-001
```

Use the exact ownership map in `/tmp/trainstar-synchro-phase3-ownership-proposal.md`. Do not add or remove ownership tuples.

The file stem and the scenario ID must match after the `SCN-` prefix is removed and both values are normalized to uppercase hyphen form.

## Closed operation registry

The scenario registry contains these operations only:

```text
artifact/install-portable-seed
connect/send
local/apply-pull-page
local/apply-rebuild-page
local/begin-rebuild
local/finalize-rebuild
local/write
model/activate-registry-membership-generation
model/commit-source-transaction
model/compact-scope
model/expire-client-generation
model/install-current-contract
model/publish-schema
model/set-client-assignments
model/stage-registry-membership-generation
process/acknowledge-contiguous-prefix
process/materialize-source-transaction
process/repair-and-retry-source-transaction
process/response-loss
process/restart-client
process/restart-wal-worker
pull/request-page
push/submit
rebuild/request-page
workload/prepare
```

Reject all other operation names.

`workload/prepare` is a model-runner macro. It is not a reference-model
operation. The scenario validator accepts it from the closed scenario
registry. The reference `OperationRegistry()` must contain every closed
operation above except `workload/prepare`, with no additional operation.

The model runner intercepts `workload/prepare` before reference dispatch. It
must never call `reference.Model.Apply` or `ApplyResolved` with that key. It
expands the macro into typed operations from the reference registry. Each
expanded operation must pass through the normal reference handler. The model
runner must not write reference state directly.

Expose the distinction in `scenarios` through one closed operation-class
lookup. Use exactly two classes: `reference` and `model_runner_macro`. Only
`workload/prepare` has the second class. Tests must compare the reference
handler keys with the scenario keys in the `reference` class.

In particular, reject these names:

```text
local/recover-error
local/start-sync
local/stop-sync
model/retire-client
push/send-timestamp
```

Keep the existing canonical payload fields that Section 2 of `/tmp/trainstar-synchro-phase3-repair-evidence.md` lists. Reject the `local/write` aliases `user_id`, `table`, and `op`.

## New operation payloads

`local/apply-pull-page` has this exact payload:

```json
{
  "user_id": "string",
  "client_id": "string",
  "source_step_id": "STEP-...-001"
}
```

`source_step_id` selects one earlier `pull/request-page` result. Apply its rows, provenance, checksums, and issued cursors atomically.

`artifact/install-portable-seed` has this exact payload:

```json
{
  "user_id": "string",
  "client_id": "string",
  "portable_seed_artifact_id": "ARTDEF-PORTABLE-SEED-001",
  "seed_fixture_id": "string"
}
```

Verify the manifest, portable-scope declaration, lineage, cardinality, digest, and receipt bindings. Seed installation does not validate or grant server assignment.

Use the one fixture ID `SEEDFIX-PORTABLE-SHARED-1000-001`. The fixture contains exactly one portable shared scope and 1,000 deterministic live rows.

## Resolved execution inputs

The model runner resolves scenario references. A reference handler does not load a scenario file, prior step, or artifact file.

Add this explicit reference API:

```go
type ResolvedStep struct {
	StepID       string
	OperationKey string
	Result       StepResult
}

type ResolvedOperationInput struct {
	SourceStep  *ResolvedStep
	PortableSeed *PortableSeedFixture
}

func (m *Model) ApplyResolved(
	ctx context.Context,
	op scenarios.Operation,
	input ResolvedOperationInput,
) (StepResult, error)
```

Keep `Model.Apply`. It delegates to `ApplyResolved` with an empty input.

Pass `ResolvedOperationInput` as an explicit argument to the operation handler. Do not use a context value, global registry, package variable, direct state write, callback, or interface that can perform I/O.

Reject a nonempty resolved input for every operation except `local/apply-pull-page` and `artifact/install-portable-seed`.

Clone the input before execution. The normal transactional working-model copy still controls atomic commit or rollback.

### Prior pull result

The model runner keeps completed step results in a map that is private to one run. For `local/apply-pull-page`, it must:

1. Resolve `source_step_id` to one earlier completed step in the same scenario run.
2. Require `OperationKey == "pull/request-page"`.
3. Pass the exact cloned `StepResult` and step ID in `ResolvedOperationInput.SourceStep`.

The reference handler must require that the input step ID equals the payload value, the result kind is `pull`, and the HTTP result is `200`.

The reference handler reconstructs the delivered rows from the current captured projections that match every source change by scope, row, version, and checksum. It copies the current issued server cursor for each scope whose source result says `issued`. It applies rows, scope provenance, terminal checksums, and local checkpoints in one working-state transaction.

The runner never receives or writes a raw token. The reference model owns token lookup and token copying.

If a matching projection, issued cursor, client, assignment, scope, or checksum is absent or changed, reject the apply and change no state.

### Portable seed fixture

Add these reference input types:

```go
type PortableSeedFixture struct {
	FixtureID             string
	ArtifactDefinitionID  string
	ArtifactBytes         []byte
	ArtifactSHA256        [32]byte
	ManifestBytes         []byte
	ManifestSHA256        [32]byte
	ExportID              ExportID
	Schema                SchemaRef
	RegistryGeneration    Generation
	StreamGeneration      StreamGeneration
	SnapshotBoundary      StreamPosition
	PortableScopeIDs      []ScopeID
	Scopes                []PortableSeedScopeFixture
	Rows                  []PortableSeedRowFixture
}

type PortableSeedScopeFixture struct {
	Scope                ScopeID
	MembershipGeneration Generation
	RetentionGeneration  Generation
	Cardinality          Cardinality
	Checksum             Checksum
}

type PortableSeedRowFixture struct {
	Scope   ScopeID
	Ordinal uint64
	Row     AuthoritativeRow
}
```

The model runner owns one closed, deterministic fixture builder for `SEEDFIX-PORTABLE-SHARED-1000-001`. It does not read production code or use the network. It builds the fixture from the scenario's installed schema and registry contract. It passes a defensive copy in `ResolvedOperationInput.PortableSeed`.

The builder uses one portable scope, row ordinals `1..1000`, and canonical string primary keys `seed-000001` through `seed-001000`. It computes each row checksum, the ordered scope checksum, the manifest hash, and the artifact SHA-256 with the independent conformance vector implementation. It does not supply a `verified` boolean.

For `artifact/install-portable-seed`, require the input fixture ID and artifact definition ID to equal the payload. Verify all of these facts again in the reference package:

- Both byte-slice SHA-256 values match their declared hashes.
- The schema and registry generation match the installed contract.
- The snapshot boundary uses the active stream and is not after the materialized boundary.
- The portable scope list is exact, sorted, unique, and equal to the scope records.
- Scope generations agree with authoritative scope state.
- Ordinals are exactly `1..1000` without a gap or duplicate.
- Every row uses a registered synced table, has a complete field set, has a nonempty version, and has the correct row checksum.
- Recomputed scope cardinality and checksum equal the declared values.
- The target local client exists and has no local row, provenance, checkpoint, seed receipt, pending mutation, sealed batch, or rebuild state.

Install local rows, local scope provenance, and one seed receipt per scope atomically. Mint each opaque receipt inside the reference token authority from the exact export, manifest, schema, scope, registry, membership, retention, stream, boundary, cardinality, and checksum bindings.

Do not install a runtime cursor or local checkpoint. Do not change server assignments or treat the seed as authorization. The later normal authenticated `connect/send` operation validates the receipt and current assignment.

`workload/prepare` has one closed discriminated payload. The `profile` value selects one exact shape:

```json
{"profile":"scope_topology","scope_fanout":8,"impact_rows":8}
```

```json
{"profile":"scope_cardinality","scope_id":"string","record_count":1000,"page_size":100}
```

```json
{"profile":"pending_mutations","user_id":"string","client_id":"string","table_id":"string","accepted_count":999,"rejected_count":1}
```

```json
{"profile":"configured_limits","max_scope_fanout":8,"max_impact_rows":1000,"pull_maximum":1000,"rebuild_maximum":1000,"compaction_batch_maximum":10000,"backfill_batch_maximum":1000}
```

Reject fields from another profile. Reject unknown fields. The model runner expands this helper through typed public reference operations. It must not write reference state directly.

For the queue-replay strata, use these exact accepted and rejected counts:

- Small: `1` accepted and `1` rejected.
- Medium: `99` accepted and `1` rejected.
- Large: `999` accepted and `1` rejected.

## Initial state

Create each model as `reference.State{ProtocolVersion: 3}`.

Use `model/install-current-contract` as the only operation in `model.setup`.

Its payload has these exact top-level members:

```text
installation
initial_schema
initial_registry
stream
empty_scopes
clients
write_policies
configured_limits
```

Use the exact closed nested shape in Section 3 of `/tmp/trainstar-synchro-phase3-repair-evidence.md`.

The four `initial_registry` arrays can contain values. Use the closed element shapes below.

Each `relations` element has every member in this shape:

```json
{
  "relation": "string",
  "registration_kind": "synced",
  "table_id": "string",
  "physical": {
    "schema": "string",
    "name": "string",
    "oid": 101,
    "replica_identity": "default"
  },
  "primary_key_field_id": "string",
  "primary_key_physical_column": "string",
  "primary_key_portable_type": "string",
  "capture_key_field_ids": [],
  "captured_field_ids": ["string"],
  "membership_function": "string",
  "positive_fanout_bound": 8,
  "dependency_impact_function": null,
  "dependency_captured_field_ids": [],
  "positive_dependency_row_bound": null
}
```

`registration_kind` is `synced` or `capture_dependency`. `replica_identity` is `default`, `nothing`, `full`, or `index`.

For `synced`, require non-null `table_id`, primary-key fields, and `membership_function`. Require an empty `capture_key_field_ids` array. The table and fields must exist in `initial_schema` and must agree with its primary key and portable types.

For `capture_dependency`, require null `table_id`, primary-key fields, and `membership_function`. Require at least one `capture_key_field_id`.

For either kind, require a nonempty `captured_field_ids` array and a positive `positive_fanout_bound` that does not exceed `configured_limits.max_scope_fanout`.

`dependency_impact_function` and `positive_dependency_row_bound` are both null, or both non-null. When they are null, require an empty `dependency_captured_field_ids` array. When they are non-null, require a nonempty field array and a positive bound that does not exceed `configured_limits.max_impact_rows`.

Each `capture_dependencies` element has this exact shape:

```json
{
  "capture_dependency_id": "string",
  "relation": "capture-dependency-relation",
  "depends_on": "synced-relation"
}
```

Both relations must exist. `relation` must name a `capture_dependency` registration. `depends_on` must name a `synced` registration. Reject self-reference, duplicates, and cycles.

Each `scope_rules` element has this exact shape:

```json
{
  "scope_rule_id": "string",
  "relation": "synced-relation",
  "membership_function": "string",
  "positive_fanout_bound": 8,
  "evaluations": [
    {
      "row": {
        "canonical_identity_bytes": "string",
        "table_id": "string",
        "primary_key_field_id": "string",
        "portable_type": "string",
        "canonical_wire_json": "string"
      },
      "scopes": ["string"]
    }
  ]
}
```

The relation must be `synced`. The function and bound must equal its relation definition. Each row must use that relation's table and primary-key contract. Require unique row evaluations and unique nonempty scopes. The number of scopes must not exceed the bound. An empty `evaluations` array is valid.

Each `dependency_impacts` element has this exact shape:

```json
{
  "dependency_impact_id": "string",
  "relation": "string",
  "function": "string",
  "captured_field_ids": ["string"],
  "positive_row_bound": 1000,
  "affected_rows": [
    {
      "canonical_identity_bytes": "string",
      "table_id": "string",
      "primary_key_field_id": "string",
      "portable_type": "string",
      "canonical_wire_json": "string"
    }
  ],
  "requires_rebuild": false
}
```

The relation and function must agree with one registered relation definition. The fields must be unique and must equal that definition's dependency-captured field set. The positive bound must equal that definition's bound and must not exceed `configured_limits.max_impact_rows`. Each affected row must use a registered synced table. Require unique rows. The row count must not exceed the bound. An empty `affected_rows` array is valid.

Require unique IDs, relation identities, physical identities, table identities, rule IDs, dependency IDs, and impact IDs. Derive the active `Relations` map from the validated registry generation. Do not accept a second direct relation-state input.

The configured release maxima are:

```text
max_scope_fanout = 8
max_impact_rows = 1000
pull_maximum = 1000
rebuild_maximum = 1000
compaction_batch_maximum = 10000
backfill_batch_maximum = 1000
```

At every payload depth, reject these members:

```text
rows
effects
projections
cursors
checkpoints
fences
batches
mutations
rebuilds
retention_floors
seed_exports
seed_records
seed_receipts
source_transactions
materializations
acknowledgements
poison
local_rows
provenance
durable_queue
outcomes
events
tokens
```

Setup must derive contract state. It must not seed internal effects or ledgers.

## Expected step results

Add an `expected_outcome` member to each scenario step.

It is one of these closed shapes:

```json
{"disposition":"success"}
```

```json
{"disposition":"error","error_code":"source_transaction_predecessor_pending"}
```

```json
{"disposition":"error","error_code":"source_transaction_poison_blocked"}
```

For `success`, reject `error_code`. For `error`, require one of the two exact codes. Do not compare free-form error text.

Use `source_transaction_predecessor_pending` when a later source transaction waits for its predecessor.

Use `source_transaction_poison_blocked` when poison blocks a later source transaction.

## Workload values

Use these exact performance values:

- Fanout: low `1/1`, medium `2/2`, high `8/8` for `scope_fanout/impact_rows`.
- Rebuild cardinality: `1`, `101`, and `1000` records with page size `100`.
- Queue replay: `2`, `100`, and `1000` total pending mutations.
- Rebuild apply: `1`, `101`, and `1000` records with page size `100`.

Use these exact schema strata:

```text
STR-SCHEMA-CURRENT-001                 current
STR-SCHEMA-CLASS-1-001                 class_1
STR-SCHEMA-CLASS-2-001                 class_2
STR-SCHEMA-CLASS-3-AFFECTED-001        class_3_affected
STR-SCHEMA-CLASS-3-UNAFFECTED-001      class_3_unaffected
STR-SCHEMA-CLASS-4-001                 class_4
```

Keep `minimum_sample_count_per_stratum` at `3`.

Use these strict configured boundaries:

| Family | Lower | Upper | Invalid |
| --- | ---: | ---: | ---: |
| scope fanout | 1 | 8 | 9 |
| impact rows | 1 | 1000 | 1001 |
| pull rows | 1 | 1000 | 1001 |
| rebuild rows | 1 | 1000 | 1001 |
| compaction batch | 1 | 10000 | 10001 |
| backfill batch | 1 | 1000 | 1001 |
| push mutations | 1 | 1000 | 1001 |

The reference model must reject invalid values before work starts. It must not clamp, default, or partially process them.

Do not change production behavior in Phase 3. The diagnostic baseline must record the current production clamping or missing-bound behavior as a contract divergence. It must not treat that behavior as reference authority or release evidence.

## Performance catalog correction

Remove only `SUP-PG-018` from `MEAS-REBUILD-APPLY-001.support_cell_ids`.

Remove only `SUP-PG-018` from `MEAS-MULTI-SCOPE-PROVENANCE-001.support_cell_ids`.

Do not change another support cell for these measurements.

Recompute all affected digests after the final scenario and performance files are stable.

## Package direction

Use this import direction:

```text
modelrunner -> scenarios
modelrunner -> reference -> scenarios
```

`scenarios` must not import `reference` or `modelrunner`.

`reference` must not import `modelrunner`.

`scenarios` owns the JSON types, closed operation names, closed payload DTOs, schema validation, catalog loading, and stable ID checks.

`reference` owns operation handlers, state transitions, atomic rollback, observations, and exact handler closure.

`modelrunner` owns setup execution, ordered step execution, prior-step references, expected error matching, faults, measurements, and predicate evaluation.

## Tasks 8 through 14

Implement Tasks 8 through 14 exactly as the plan specifies.

All negative controls must fail for the intended semantic reason. A panic, parse failure, missing field, unrelated assertion, skip, or expected failure does not count.

The real PostgreSQL baseline is diagnostic only. It must never produce release evidence.

The evidence path must remain fail-closed. A partial Phase 3 candidate must fail full release closure.

## Required validation

Use Make targets only.

Run focused targets during implementation. Then run this exact final gate:

```bash
make verify-contract
make conformance-mod-download
make lint-conformance
make test-conformance
make check-conformance-catalog
make test-blackbox
make test-rust-core
make test-rust-pg
make test-adapter
make docs-build
git diff --check
```

Also prove these expected failures:

- `make evidence` fails because `RC_CANDIDATE_DIR` is absent.
- `make evidence RC_CANDIDATE_DIR=<phase-3-partial-candidate>` fails full candidate closure.
- `make release-check` remains nonzero because later release phases are incomplete.

The final report must list every command, result, skip count, changed file, and unresolved item.

## Stop conditions

Stop and report if any of these conditions occurs:

- A design choice is not fixed by this brief or the plan.
- A required change is outside file ownership.
- The current implementation contradicts a premise in this brief.
- A test requires network access.
- A command would commit, push, or change external data.

Do not use a placeholder, deferred implementation, skipped test, expected failure, or weakened assertion.
