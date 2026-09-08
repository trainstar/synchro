# Server Finding Verification

## Method

I inspected the audited server code at `3542946`.
The audited commit `da07df1` is an ancestor of that commit.
The investigated source files have no changes between those commits.
I did not run PostgreSQL-dependent tests.

## C1. Membership reads historical source state

**Verdict:** REFUTED  
**Severity:** None

The worker does not evaluate membership against physical source rows.
`resolve_membership` states that it evaluates against the worker projection:

> `/// Evaluate a registered membership function against the worker projection.`  
> `extensions/synchro-pg/src/bucketing.rs:8`

The projection view reads `synchro.sync_current_projections`, not the physical relation:

> `FROM synchro.sync_current_projections projection`  
> `extensions/synchro-pg/src/registry.rs:279-283`

The worker persists each event into that projection before it collects impacts.

> `let persisted = persist_events_and_projections(...)`  
> `let impacts = collect_membership_impacts(...)`  
> `extensions/synchro-pg/src/bgworker.rs:3009-3025`

`persist_events_and_projections` processes events in order and updates the current projection for each event.

> `for event in events {`  
> `persist_current_row(client, target, transaction, event, &after)?;`  
> `extensions/synchro-pg/src/bgworker.rs:3807-3842`

The membership result therefore uses the transaction's final captured projection.
This matches atomic source-transaction visibility.
No direct regression test covers repeated changes to one record in one source transaction.

## C2. Slot advancement can exceed durable acknowledgement

**Verdict:** CONFIRMED  
**Severity:** Medium

Each materialized transaction commits before the worker advances the slot.

> `run_worker_transaction(|| {`  
> `Spi::connect_mut(|client| materialize_transaction(client, transaction))`  
> `extensions/synchro-pg/src/bgworker.rs:2912-2916`

The later acknowledgement path advances PostgreSQL's replication slot before it updates the Synchro durable acknowledgement record.

> `FROM pg_catalog.pg_replication_slot_advance($1, $2::pg_lsn)`  
> `extensions/synchro-pg/src/bgworker.rs:5355-5367`

> `UPDATE synchro.sync_wal_progress`  
> `SET acknowledged_end_lsn = $1::pg_lsn`  
> `extensions/synchro-pg/src/bgworker.rs:5373-5385`

The two operations share an SQL transaction, but a replication slot is external persistent PostgreSQL state. It is not an update to `sync_wal_progress`.

Failure sequence:

1. The worker commits materialization for transaction `T`.
2. `pg_replication_slot_advance` advances the logical slot to `T.end_lsn`.
3. PostgreSQL persists that slot state during a checkpoint.
4. The worker process stops before its transaction commits the `sync_wal_progress` update.
5. On restart, the slot has `T.end_lsn` but `acknowledged_end_lsn` is older.
6. Startup detects the mismatch and poisons the worker.

> `if actual != expected {`  
> `detail: "logical slot acknowledgement did not match durable progress"`  
> `extensions/synchro-pg/src/bgworker.rs:2850-2857`

This is a fail-closed availability failure. The materialization already committed, so the evidence does not show data loss.
No direct crash-window test exists.

## C3. Compaction deactivates a future-expiry client

**Verdict:** CONFIRMED  
**Severity:** Medium

Compaction deactivates any active client with a non-null expiry value.

> `generation_expires_at IS NOT NULL`  
> `extensions/synchro-pg/src/compaction.rs:95-104`

It does not require the expiry to be at or before the statement time.
The push gate gives a future expiry different meaning.

> `generation_expires_at IS NULL OR generation_expires_at > now()`  
> `extensions/synchro-pg/src/push.rs:579-583`

Failure sequence:

1. A client has `generation_expires_at = T`, where `T > now()`.
2. The client remains valid under the push generation gate.
3. Compaction runs before `T`.
4. The non-null predicate sets `is_active = false`.
5. The client loses its active generation before its declared expiry.

`test_compact_deactivates_marked_retention_client` exists, but it sets expiry to the current statement time.
It does not test a future expiry.

## C4. Reset source locks are not associated with the reset

**Verdict:** REFUTED  
**Severity:** None

The verifier does not store a reset ID in an advisory lock.
That is not required for the protected operation.
It requires one PostgreSQL backend to hold every required global lock.

> `GROUP BY lock.pid`  
> `WHERE holders.value = required_count.value`  
> `extensions/synchro-pg/src/stream_reset.rs:1977-2000`

The Go coordinator pins those session locks to one connection.

> `sourceLockConnection, err = coordinator.operatorDB.Conn(ctx)`  
> `api/go/operator/operator.go:258-261`

It stages the imported snapshot while that connection remains open.

> `SELECT synchro.synchro_stage_projection_bootstrap(...)`  
> `api/go/operator/operator.go:300-317`

It closes the source-lock connection only after snapshot commit.

> `if err := closeSourceLocks(ctx, sourceLockConnection); err != nil {`  
> `api/go/operator/operator.go:325-328`

Any backend that holds the required global locks prevents source writes during staging.
The safety property is global source-write exclusion, not reset identity.
`stream_reset_operator_can_lock_registered_sources` tests lock acquisition.
No cross-session ownership test exists.

## C11. WAL decoder has no transaction-memory bound

**Verdict:** CONFIRMED  
**Severity:** High

`BEGIN` creates unbounded event, truncate, and message vectors.

> `events: Vec::new(),`  
> `truncates: Vec::new(),`  
> `messages: Vec::new(),`  
> `extensions/synchro-pg/src/wal_decoder.rs:243-251`

Every DML record appends a full event image to the pending transaction.

> `transaction.events.push(WalEvent {`  
> `extensions/synchro-pg/src/wal_decoder.rs:484-502`

The decoder exposes the vectors only at `COMMIT`.

> `events: pending.events,`  
> `truncates: pending.truncates,`  
> `messages: pending.messages,`  
> `extensions/synchro-pg/src/wal_decoder.rs:285-294`

The worker read batch of 500 messages does not bound one open transaction.

> `FROM pg_catalog.pg_logical_slot_peek_binary_changes(`  
> `&[slot.into(), BATCH_SIZE.into(), publication.into()]`  
> `extensions/synchro-pg/src/bgworker.rs:2880-2889`

Failure sequence:

1. A valid source transaction changes many wide rows.
2. The decoder retains every row image until the transaction commits.
3. Worker memory exceeds its backend memory limit or host capacity.
4. The worker stops before materializing the transaction.
5. Restart replays the same unbounded transaction from retained WAL.

`emits_complete_transaction_with_begin_and_commit_metadata` exists.
It does not enforce an event count or byte limit.

## C12. Pull exposes cursor parser details

**Verdict:** CONFIRMED  
**Severity:** Low

The cursor parser returns distinguishable token errors.

> `return Err("incremental cursor binding is invalid".to_string());`  
> `extensions/synchro-pg/src/cursor_token.rs:148-155`

> `.map_err(|_| "incremental cursor signature is invalid".to_string())`  
> `extensions/synchro-pg/src/cursor_token.rs:224-231`

Pull places that parser error in the protocol response.

> `format!("scope {scope_id} cursor is invalid: {err}")`  
> `extensions/synchro-pg/src/pull.rs:437-443`

Failure sequence:

1. A caller submits a cursor with an invalid signature.
2. The parser returns `incremental cursor signature is invalid`.
3. Pull returns that exact classification to the caller.

This exposes validation state, but not a signing secret or user value.
No direct pull-token error-redaction test exists.

## C13. Candidate catch-up recomputes all membership edges

**Verdict:** CONFIRMED  
**Severity:** Medium

Candidate materialization calls full recomputation for every processed transaction.

> `if matches!(target, ProjectionTarget::Candidate { .. }) {`  
> `recompute_candidate_membership(client, target, registry)`  
> `extensions/synchro-pg/src/bgworker.rs:5070-5073`

The recomputation deletes all candidate edges, scans every synced relation, evaluates each row, and inserts each resulting edge.

> `DELETE FROM synchro.sync_stream_reset_membership_edges`  
> `WHERE reset_id = $1::uuid`  
> `extensions/synchro-pg/src/bgworker.rs:2300-2306`

> `for registration in registry.iter().filter(|registration| registration.is_synced())`  
> `extensions/synchro-pg/src/bgworker.rs:2308-2311`

> `let scopes = resolve_membership(client, registration, &record_id)`  
> `extensions/synchro-pg/src/bgworker.rs:2332-2366`

For `R` synced relations, `N` candidate rows, and `E` candidate edges, each candidate transaction performs:

- one full edge delete
- `R` candidate-row queries
- `N` membership queries
- `E` individual edge inserts

This work does not depend on the number of rows changed by the transaction.
`projection_bootstrap_activates_verified_stage_atomically` exists.
It does not measure repeated catch-up work.

## C14. Materialization and reset staging use per-row SPI work

**Verdict:** CONFIRMED  
**Severity:** Medium

Some writes use JSONB batches, but critical row processing remains per row.
Each active event loads its current projection before it processes the event.

> `let prior = load_captured_row(client, target, event.registration, &event.record_id)`  
> `extensions/synchro-pg/src/bgworker.rs:3820-3822`

Each digest loads the schema hash through SPI.

> `let schema_hash = schema_hash_for_generation(client, table_reg.registry_generation)?;`  
> `extensions/synchro-pg/src/pull.rs:720-729`

> `let hash = client.select(`  
> `extensions/synchro-pg/src/pull.rs:662-683`

Each final active impact also evaluates membership independently.

> `resolve_membership(client, registration, &impact.record_id)`  
> `extensions/synchro-pg/src/bgworker.rs:5127-5137`

Reset staging loads all source rows, then performs row-level version lookup, digest computation, and insert work.

> `for source in load_source_rows(client, registration)? {`  
> `extensions/synchro-pg/src/stream_reset.rs:2053-2083`

> `SELECT row_version::text AS row_version, deleted`  
> `extensions/synchro-pg/src/stream_reset.rs:2296-2308`

For a baseline row without an existing version, staging performs at least four SPI operations after the source scan:

1. one version lookup
2. one version insert
3. one schema-hash lookup for the digest
4. one captured-row insert

Membership staging adds one membership query per non-deleted row and one insert per returned scope.
`projection_bootstrap_activates_verified_stage_atomically` exists.
It does not measure row-level query volume.

## C15. Registry loading has child queries per relation

**Verdict:** CONFIRMED  
**Severity:** Medium

The generation loader queries the registry once, then loads two field collections for each relation.

> `for row in rows {`  
> `registration.fields = load_field_registrations(...)`  
> `registration.capture_fields = load_capture_field_registrations(...)`  
> `extensions/synchro-pg/src/registry.rs:3783-3804`

Both helper functions issue relation-specific SPI queries.

> `WHERE registry_generation = $1 AND relation_id = $2::uuid`  
> `extensions/synchro-pg/src/registry.rs:2999-3009`

> `WHERE registry_generation = $1 AND relation_id = $2::uuid`  
> `extensions/synchro-pg/src/registry.rs:3046-3054`

For `R` registrations, loading uses one registry query plus at least `2R` child queries before validation.
Validation adds relation-specific catalog checks.

> `validate_loaded_registration(client, &registration)?;`  
> `extensions/synchro-pg/src/registry.rs:3799-3803`

No test counts registry-load queries.

## C16. Push reloads and scans manifest history

**Verdict:** CONFIRMED  
**Severity:** Medium

Each new push loads all manifests and the full active registry.

> `let manifests = load_manifest_history(client);`  
> `let registry = load_registry_inner(client);`  
> `extensions/synchro-pg/src/push.rs:362-369`

The manifest loader reads and parses every stored manifest body.

> `SELECT schema_version, schema_hash, canonical_manifest_body`  
> `FROM sync_schema_manifest`  
> `extensions/synchro-pg/src/push.rs:951-991`

Each mutation linearly searches historical manifests, tables, and the registry.

> `.iter().find(|stored| stored.reference == *reference)`  
> `extensions/synchro-pg/src/push.rs:994-1002`

> `.tables.iter().find(|table| table.table_id == table_id)`  
> `extensions/synchro-pg/src/push.rs:1004-1009`

> `registry.iter().find(|table| table.table_id == mutation.table)`  
> `extensions/synchro-pg/src/push.rs:1126-1133`

For `H` manifests, `T` tables, `R` registrations, and `M` new mutations, the request performs:

- one parse and validation pass over all `H` manifests
- at least `M` scans of up to `H` manifests
- at least two table scans and one registry scan per mutation
- one catalog query per insert mutation for required columns

> `if mutation.op == Operation::Insert`  
> `&& !has_required_insert_columns(client, table_reg, &dml_data)`  
> `extensions/synchro-pg/src/push.rs:1325-1337`

`test_schema_manifest_history_keeps_original_body` exists.
It does not measure push lookup work.

## C17. Compaction batch size does not bound transaction size

**Verdict:** CONFIRMED  
**Severity:** Medium

`synchro_compact` opens one SPI transaction around deactivation, locking, and deletion.

> `Spi::connect_mut(|client| {`  
> `let deactivated = deactivate_stale_clients(client, p_stale_threshold);`  
> `let (deleted_entries, last_deleted_seq) = delete_acknowledged_effects(client, p_batch_size);`  
> `extensions/synchro-pg/src/compaction.rs:20-33`

The deletion helper loops until fewer than one batch remains.

> `loop {`  
> `LIMIT $1`  
> `if count < i64::from(batch_size) { return (total, last_deleted_seq); }`  
> `extensions/synchro-pg/src/compaction.rs:123-235`

For `E` deletable effects and batch size `B`, one compaction transaction deletes all `E` effects in approximately `ceil(E / B)` batches.
It also updates retention floors in that same transaction.

The stale-client update has no batch limit.

> `UPDATE sync_clients SET is_active = false, updated_at = now()`  
> `extensions/synchro-pg/src/compaction.rs:93-109`

`test_compact_rejects_oversized_batch_without_mutation` exists.
It does not test a compaction run with more effects or clients than one batch.

## Summary

| Finding | Verdict | Severity | Existing direct test |
|---|---|---|---|
| C1 | REFUTED | None | No |
| C2 | CONFIRMED | Medium | No |
| C3 | CONFIRMED | Medium | No future-expiry test |
| C4 | REFUTED | None | No cross-session test |
| C11 | CONFIRMED | High | No limit test |
| C12 | CONFIRMED | Low | No |
| C13 | CONFIRMED | Medium | No repeated catch-up test |
| C14 | CONFIRMED | Medium | No query-volume test |
| C15 | CONFIRMED | Medium | No query-count test |
| C16 | CONFIRMED | Medium | No lookup-work test |
| C17 | CONFIRMED | Medium | No multi-batch test |
