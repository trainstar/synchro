# Synchro Production Codebase Audit

Audit snapshot: `da07df14b19736e3e5a4a7d53244df2fc0082524`.

This is a static, read-only audit. The standard is a textbook, dead-simple, production-grade offline-first sync library.

## 1. Product Understanding

Synchro is an opinionated offline-first sync product for native mobile applications. It provides local SQLite, durable mutation queues, selective sync, and full synced CRUD.

Its primary architectural difference is server placement. Server-side sync execution runs inside PostgreSQL instead of a separate sync service.

The product competes with offline-sync products that use a separate sync tier. Phase R4 names OSS PowerSync as the direct benchmark competitor.

R4 requires identical topology, production guidance, fixed versions, two datasets, four metrics, and five runs. No comparative claim is valid before that benchmark runs.

PostgreSQL is the server authority. The extension owns scopes, WAL materialization, checkpoints, CRUD, schemas, compaction, and workers.

The portable Rust core owns deterministic protocol rules. The PostgreSQL extension executes server behavior and owns durable server state.

The Go HTTP adapter owns transport, identity resolution, version gates, and HTTP translation. Each endpoint must call one canonical extension function.

Swift and Kotlin own local SQLite, capture, queues, scheduling, apply, retry, and status. React Native must remain a typed native bridge.

The stated design rules are:

- WAL is the only server path that creates pull-visible changes.
- PostgreSQL assigns scopes, schema actions, row versions, cursors, checksums, and mutation outcomes.
- Clients preserve opaque tokens and do not infer server semantics.
- Local SQL is the application CRUD surface.
- Server-originated apply must not create local intent.
- The Go adapter must not reimplement server semantics.
- React Native must not contain a third sync engine.
- Deterministic reusable rules belong in `synchro-core`.
- One endpoint operation must use one canonical extension call.
- Portable seeds are verified caches and not a second authority path.

The code violates several of these rules. Findings B6, C1, C5, and C9 identify explicit contradictions.

### Contract contradictions found before code review

The published support scope is inconsistent.

- `README.md:43` says PostgreSQL 18 uses published `linux-x64` and `macos-arm64` architectures.
- `README.md:54` says these architectures create two required server cells.
- `conformance/support-matrix.json:69-82` requires Linux and excludes macOS.
- `docs/src/content/docs/spec/07-release-verification.mdx:89-96` also requires Linux and excludes macOS.
- The Linux matrix note still says, `"The derived PostgreSQL server-cell count is 2."`

The malformed connect-schema error is also inconsistent.

- `docs/src/content/docs/spec/01-wire-protocol.mdx:43` says `400 invalid_schema_reference`.
- `docs/src/content/docs/spec/01-wire-protocol.mdx:86` says `400 invalid_request`.
- `docs/src/content/docs/spec/05-schema-evolution.mdx:1050` uses `invalid_schema_reference`.

These contradictions increase implementation and test drift. Fix the specification before treating either behavior as authoritative.

## 2. Coverage

I read every production file in the requested trees. I read files above 1,000 lines completely.

The counts include inline tests because they share production source files. Dedicated test files were not audit targets.

| Audited path | Production files read | Lines read | Coverage |
| --- | ---: | ---: | --- |
| `extensions/synchro-core/src` | 8 | 7,169 | Full |
| `extensions/synchro-pg/src` | 23 | 32,452 | Full |
| `api/go`, excluding `_test.go` | 18 | 7,150 | Full |
| `clients/swift/Sources` | 24 | 17,493 | Full |
| Kotlin files under `src/main` | 26 | 14,361 | Full |
| `clients/react-native/src` | 12 | 2,876 | Full |
| **Total** | **111** | **81,501** | **Full** |

`extensions/synchro-pg/src/pg_tests` contains 11,366 dedicated test lines. I excluded those files as production targets and searched them narrowly for proof quality.

### Files above 1,000 lines

Only `wal_decoder.rs` supports one-pass understanding. Its production decoder is linear, and tests account for much of the file.

| File | Lines | One-pass result |
| --- | ---: | --- |
| `extensions/synchro-core/src/checksum.rs` | 1,994 | No. It combines portable codecs, canonical JSON, identities, hashing, and extensive tests. |
| `extensions/synchro-core/src/contract.rs` | 3,667 | No. It combines all wire models, validation, dispatch, schema rules, and tests. |
| `extensions/synchro-pg/src/bgworker.rs` | 5,811 | No. It combines startup, slots, decoding, projections, reset catch-up, poison, and persistence. |
| `extensions/synchro-pg/src/client.rs` | 1,135 | No. Connect spans identity, schema, assignments, cursors, seeds, and dispatch. |
| `extensions/synchro-pg/src/health.rs` | 1,068 | No. A large SQL contract is manually mapped into many Rust fields. |
| `extensions/synchro-pg/src/lib.rs` | 3,501 | No. It combines DDL, triggers, security, initialization, GUCs, and test support. |
| `extensions/synchro-pg/src/materialize.rs` | 1,165 | No. It combines backfill, activation, migration, locking, and checkpoints. |
| `extensions/synchro-pg/src/portable_seed.rs` | 1,563 | No. It combines administration, sessions, paging, hashes, tokens, and receipts. |
| `extensions/synchro-pg/src/pull.rs` | 1,596 | No. It combines request flow, tokens, checkpoints, hydration, and digest caching. |
| `extensions/synchro-pg/src/push.rs` | 2,434 | No. It combines ledgers, validation, policy, DML, replay, and response construction. |
| `extensions/synchro-pg/src/rebuild.rs` | 1,112 | No. It combines sessions, snapshots, staging, hashing, paging, and replay. |
| `extensions/synchro-pg/src/registry.rs` | 5,089 | No. It combines public commands, catalog checks, ACLs, functions, publications, and triggers. |
| `extensions/synchro-pg/src/schema.rs` | 1,277 | No. It combines schema authority, transition logic, bootstrap, and debug endpoints. |
| `extensions/synchro-pg/src/stream_reset.rs` | 3,801 | No. It combines APIs, locks, state machines, staging, verification, and activation. |
| `extensions/synchro-pg/src/wal_decoder.rs` | 1,040 | Yes. Its main responsibility is coherent and linear. |
| `api/go/operator/operator.go` | 1,005 | No. It combines replication, snapshots, polling, recovery, and cleanup. |
| `api/go/seeddb/integrity.go` | 1,219 | No. It combines schema checks, portable encoding, digests, and canonical JSON. |
| `api/go/seeddb/seeddb.go` | 2,175 | No. It combines export, SQLite creation, validation, and trigger generation. |
| `clients/swift/Sources/Synchro/ContractModels.swift` | 1,230 | No. It combines every protocol domain and its handwritten validators. |
| `clients/swift/Sources/Synchro/Internal/SynchroMeta.swift` | 1,004 | No. It stores unrelated client, scope, row, queue, retry, and rebuild state. |
| `clients/swift/Sources/Synchro/PullProcessor.swift` | 1,388 | No. It combines pull, rebuild, seed, provenance, cleanup, and digest behavior. |
| `clients/swift/Sources/Synchro/PushProcessor.swift` | 1,675 | No. It combines sealing, renewal, reconciliation, projection, SQL, and JSON scanning. |
| `clients/swift/Sources/Synchro/SyncEngine.swift` | 2,200 | No. It combines lifecycle, scheduling, retries, transport, connection, and notifications. |
| `clients/swift/Sources/SynchroNativeRunner/SynchroNativeRunner.swift` | 1,805 | No. It combines framing, SQL actions, sessions, capture, diagnostics, and output. |
| `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/PullProcessor.kt` | 1,330 | No. It combines pull, rebuild, seed, digests, codecs, and SQL. |
| `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/PushProcessor.kt` | 1,766 | No. It combines batching, repair, normalization, reconciliation, codecs, and JSON parsing. |
| `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SyncEngine.kt` | 1,598 | No. It combines lifecycle, scheduling, retries, transport, rebuild, and events. |
| `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroDatabase.kt` | 1,506 | No. It combines migrations, transactions, application SQL, guards, and notifications. |
| `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroMeta.kt` | 1,258 | No. It owns unrelated durable domains and repeats SQL operations. |
| `clients/react-native/src/SynchroClient.ts` | 1,203 | No. It combines native ownership, SQL forwarding, lifecycle, events, inspection, and validation. |

## 3. Findings

## A. Protocol And Specification Inefficiency

### A1. Rebuild duplicates the pull paging protocol

**Impact:** High complexity across every server and client implementation.

**Evidence:**

- `docs/src/content/docs/spec/01-wire-protocol.mdx:28-31` defines separate pull and rebuild endpoints.
- `extensions/synchro-pg/src/rebuild.rs:1-1112` implements another token, session, page, replay, and finality path.
- `clients/swift/Sources/Synchro/PullProcessor.swift:311` starts rebuild-page application separately.
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/PullProcessor.kt:289` starts the equivalent second path.

> "`POST /sync/pull` | Read one incremental page across assigned scopes."
>
> "`POST /sync/rebuild` | Read one page from an immutable single-scope rebuild snapshot."

The receiver needs authoritative records, a continuation, and final scope state in both paths. The separate endpoint forces duplicated durable page machinery.

**Change:** Add `mode: incremental|rebuild` to pull. Return rebuild continuation and finality fields in the pull union.

Delete the rebuild endpoint, request type, transport method, and separate scheduler branch. Keep immutable server snapshots behind pull.

**Estimated line delta:** `-700` production lines after all surfaces migrate.

**Confidence:** Low. This changes the published protocol and requires migration analysis.

### A2. Terminal pull requires work for every active scope

**Impact:** High steady-state client cost.

**Evidence:**

- `docs/src/content/docs/spec/01-wire-protocol.mdx:875-886` requires the complete active-scope checksum map on terminal pull.
- `docs/src/content/docs/spec/02-client-contract.mdx:609-625` requires client recomputation for every active scope.
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/PullProcessor.kt:981-1009` calls `getScopeRowChecksums` and maps every scope row.
- Swift performs the same complete-scope recomputation in `PullProcessor.swift`.

> "the client recomputes the canonical scope digest for every active scope"

An idle scope did not change. Rehashing it cannot detect a new server-side effect because the cursor did not advance.

**Change:** Return terminal checksums only for scopes with represented effects or changed lineage. Verify only those scopes.

Keep periodic full verification as an explicit integrity operation. Remove mandatory all-scope recomputation from ordinary pull.

**Estimated line delta:** `-90` production lines across the server and native clients.

**Confidence:** Medium.

### A3. Pull performs repeated database work for each scope

**Impact:** High latency when clients have many scopes.

**Evidence:**

- `extensions/synchro-pg/src/pull.rs:192-214` loops through current scopes.
- `extensions/synchro-pg/src/pull.rs:246-270` issues cursors in another scope loop.
- `extensions/synchro-pg/src/pull.rs:407-444` parses and loads cursor state per scope.
- `extensions/synchro-pg/src/pull.rs:457-535` performs more scope-state work.

> `for scope_id in &current_scopes`
>
> `for scope_id in active_scopes`

The endpoint can issue about ten scope-dependent statements for each active scope.

**Change:** Load all scope states and token keys once. Query candidates for all scopes through one input relation.

Persist checkpoints with one `jsonb_to_recordset` upsert. Sign tokens in Rust after the bulk state load.

**Estimated line delta:** `-80` production lines.

**Confidence:** High.

### A4. Portable-seed paging rebuilds the complete scope for every page

**Impact:** High CPU and memory cost for large seed exports.

**Evidence:**

- `extensions/synchro-pg/src/portable_seed.rs:621` executes `let rows = load_seed_rows(`.
- `extensions/synchro-pg/src/portable_seed.rs:629` executes `let checksum = compute_scope_checksum`.

> `let rows = load_seed_rows(`
>
> `let checksum = compute_scope_checksum`

An export with `P` pages performs approximately `P` full scope scans and hashes. Memory follows scope size instead of page size.

**Change:** Validate the full digest once when the signed export session starts. Fetch later pages by stable keyset boundary and limit.

**Estimated line delta:** `+60` production lines. This change adds bounded session state and removes repeated runtime work.

**Confidence:** High.

### A5. Adapter startup uses nine serial PostgreSQL calls

**Impact:** Low runtime impact, but the work is direct waste.

**Evidence:**

- `api/go/contract.go:32-52` queries extension placement.
- `api/go/contract.go:54-65` queries contract information.
- `api/go/contract.go:94` starts `for _, signature := range required {`.
- The loop executes seven `QueryRowContext` calls.

> `for _, signature := range required {`

This path runs before listening. It is not a request hot path.

**Change:** Validate all required signatures with one aggregate catalog query. Keep `synchro_contract_info()` as the second call.

**Estimated line delta:** `-25` production lines and seven startup round trips.

**Confidence:** High.

## B. Bloat

### B1. Swift and Kotlin handwrite the same protocol model and validator layer

**Impact:** Highest bloat and drift risk in the native clients.

**Evidence:**

- `clients/swift/Sources/Synchro/ContractModels.swift:616-620` starts equivalent push-request validation.
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/ContractModels.kt:470-472` repeats that validation.
- Swift fails to reject an empty push `client_id` in that block.
- Kotlin performs stricter conflict-field checks at `ContractModels.kt:661-676` than Swift does at `ContractModels.swift:938-950`.

> `guard clientGeneration > 0,`
>
> `if (clientID.isEmpty() || clientGeneration <= 0L || !isCanonicalUUID(batchID) || mutations.isEmpty()) {`

The files contain 2,148 handwritten lines for one exact protocol. Existing drift proves that authored fixtures alone do not prevent divergence.

**Change:** Define one exact protocol schema with members, unions, constraints, and dispatch rules.

Generate both complete model files during the build. Do not add a shared runtime sync engine.

**Estimated line delta:** `-1,548` net handwritten production lines after a roughly 600-line generator and schema.

**Confidence:** Medium.

### B2. Native seed validators duplicate one invariant graph

**Impact:** High release-critical maintenance cost.

**Evidence:**

- `clients/swift/Sources/Synchro/SeedDatabaseInstaller.swift:370-409` compares receipts, scopes, rows, and versions.
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SeedDatabaseInstaller.kt:188-223` repeats the same graph.
- Swift contains 546 lines of seed invariant validation.
- Kotlin contains 474 equivalent lines.

> `Set(receipts.keys) == Set(scopes.keys)`
>
> `scopes.keys != receipts.keys`

**Change:** Author one seed-invariant query and evaluation catalog. Generate native validators for GRDB and Android SQLite.

Keep file copying, opening, migration, publication, and atomic replacement platform-specific.

**Estimated line delta:** `-770` net production lines after a roughly 250-line catalog and emitter.

**Confidence:** Medium.

### B3. Native schema projection and migration planning are duplicated

**Impact:** High semantic drift risk.

**Evidence:**

- `clients/swift/Sources/Synchro/SchemaMigrationJournal.swift:97-108` indexes by immutable `indexID`.
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SchemaManager.kt:339-346` indexes by mutable name.
- `SchemaManager.kt:303-320` uses JVM string ordering with `sortedWith(compareBy { it.tableID })`.
- The contract requires unsigned UTF-8 ordering and immutable IDs.

> `sourceTable.indexes.map { ($0.indexID, $0) }`
>
> `associateBy { it.name }`

**Change:** Generate local-schema projection, migration DTOs, pure migration planning, and UTF-8 comparators from one rule set.

Keep historical database migrations and platform DDL execution native.

**Estimated line delta:** `-447` net production lines after a roughly 150-line rule source and emitter.

**Confidence:** Medium.

### B4. Capture-trigger semantics are handwritten twice

**Impact:** High maintenance cost in the most sensitive client path.

**Evidence:**

- `clients/swift/Sources/Synchro/SQLiteSchema.swift:157-168` emits mutation identity, dependency, state, and origin columns.
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SQLiteSchema.kt:298-313` emits equivalent behavior with different storage names.

> `dependency_mutation_id`
>
> `depends_on_mutation_id`

Both files independently implement operation classification, field presence, dependencies, soft delete, guards, and primary-key protection.

**Change:** Define one semantic trigger plan with target-specific storage bindings. Generate both SQL builders.

Keep native transaction control and SQLite execution unchanged.

**Estimated line delta:** `-469` net production lines after a roughly 200-line plan and emitter.

**Confidence:** Medium.

### B5. Portable SQLite codecs are duplicated within and across clients

**Impact:** Medium correctness and maintenance cost.

**Evidence:**

- `clients/swift/Sources/Synchro/Internal/SQLiteHelpers.swift:28-40` separates `int` and string-form `int64`.
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/PushProcessor.kt:1315-1316` merges `"int", "int64"`.
- Pull, push, and schema code also contain local conversion variants.

> `case "int":`
>
> `case "int64":`
>
> `"int", "int64" -> primitive.content.toLongOrNull()`

**Change:** Generate one native codec per language from the portable-type matrix. Keep platform bind and cursor operations native.

**Estimated line delta:** `-263` net production lines after a roughly 120-line type catalog and emitters.

**Confidence:** Medium.

### B6. Deterministic protocol rules remain in `synchro-pg`

**Impact:** Medium direct architecture violation and drift risk.

**Evidence:**

- `extensions/synchro-pg/src/schema.rs:867` defines `fn classify_transition(`.
- `extensions/synchro-pg/src/health.rs:815` hashes `SCHEMA_MANIFEST_DOMAIN`.
- `extensions/synchro-pg/src/portable_seed.rs:790` hashes `b"synchro:v3:schema-manifest:v1\0"` again.
- `extensions/synchro-pg/src/stream_position.rs:6-162` owns portable stream ordering.
- `AGENTS.md:260` says deterministic reusable server rules belong in `synchro-core`.

> `fn classify_transition(`
>
> `hasher.update(SCHEMA_MANIFEST_DOMAIN);`

This finding explicitly contradicts the stated design rule.

**Change:** Move schema transition classification, canonical manifest hashing, stream positions, LSN syntax, and portable key conversion into `synchro-core`.

Delete the duplicate PostgreSQL implementations. Keep SPI loading and PostgreSQL catalog work in `synchro-pg`.

**Estimated line delta:** `-80` net production lines.

**Confidence:** High.

### B7. Three token modules duplicate envelope and HMAC mechanics

**Impact:** Medium security-review surface.

**Evidence:**

- `extensions/synchro-pg/src/cursor_token.rs:186-278` defines canonical payload, key loading, and signing.
- `extensions/synchro-pg/src/rebuild_token.rs:181-243` repeats those mechanics.
- `extensions/synchro-pg/src/seed_token.rs:167-214` repeats them again.

> `fn canonical_payload`
>
> `fn load_key`
>
> `fn sign`

**Change:** Consolidate purpose-scoped key loading, raw HMAC, canonical JSON, and envelope decoding.

Keep payload validation and token-specific bindings in each module.

**Estimated line delta:** `-45` production lines.

**Confidence:** High.

### B8. Confirmed dead native APIs and state remain in production

**Impact:** Medium audit and public-surface cost.

**Evidence:**

- `clients/swift/Sources/Synchro/PullProcessor.swift:31-38` exposes an unused global checkpoint helper.
- `clients/swift/Sources/Synchro/PullProcessor.swift:682-687` contains unused global scope cleanup.
- `clients/swift/Sources/Synchro/ChangeTracker.swift:586-605` contains two unused clearing APIs.
- `clients/swift/Sources/Synchro/SyncEngine.swift:165` writes but never reads `pendingObserverGeneration`.
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/PullProcessor.kt:34-41` repeats the unused global checkpoint.
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/ChangeTracker.kt:230-255` contains unused cleanup APIs.
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/HttpClient.kt:145-146` ships an unused prohibited `/sync/schema` call.
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SQLiteSchema.kt:383-402` contains two unused helpers.

> `fun updateCheckpoint(checkpoint: Long)`
>
> `get("/sync/schema", retryContext = null)`

**Change:** Delete these APIs, state fields, old endpoint models, and self-contained tests.

**Estimated line delta:** `-156` production lines.

**Confidence:** High for repository reachability. External public consumers require an API-compatibility decision.

### B9. React Native ships handwritten SHA-256 for an inspection identity

**Impact:** Medium review cost and unnecessary cryptographic surface.

**Evidence:**

- `clients/react-native/src/digest.ts:2` exports `sha256Hex`.
- `clients/react-native/src/inspection.ts:215` exports the helper publicly.
- The only production use is a conformance-runner database path fingerprint.

> `export function sha256Hex(value: string): string`

React Native is not a second sync engine. Its event parsing is bridge validation, not queue or retry ownership.

**Change:** Return an opaque native database identity. Delete `digest.ts` and its public export.

**Estimated line delta:** `-33` net lines after native bridge additions.

**Confidence:** Medium.

### B10. Candidate digest verification recomputes its own result

**Impact:** Medium CPU cost and false assurance.

**Evidence:**

- `extensions/synchro-pg/src/bgworker.rs:2415-2416` calls `replace_candidate_scope_digests` and then `verify_candidate_scope_digests`.
- `extensions/synchro-pg/src/bgworker.rs:2511-2582` recomputes the same digest from the same edges.

> `replace_candidate_scope_digests(client, bootstrap, &registry)?;`
>
> `verify_candidate_scope_digests(client, bootstrap, &registry)?;`

This is self-referential verification. It doubles full candidate digest work without an independent oracle.

**Change:** Compute each digest set once. Insert it with one statement and verify inserted counts.

Use authored vectors and a negative control for semantic proof.

**Estimated line delta:** `-45` production lines.

**Confidence:** High.

## C. Other Issues

### C1. WAL materialization evaluates historical membership against live rows

**Impact:** Critical authorization and correctness risk.

**Evidence:**

- `extensions/synchro-pg/src/bgworker.rs:5132-5136` calls `resolve_membership(client, registration, &impact.record_id)` for a decoded transaction.
- `extensions/synchro-pg/src/bucketing.rs:70-76` calls the registered SQL membership function by current row key.
- `docs/src/content/docs/spec/04-invariants.mdx:214` forbids `"reading later live state"` during membership evaluation.

> `resolve_membership(client, registration, &impact.record_id)`
>
> `FROM {}($1::{}) AS membership(scope_id)`

A later commit can be visible while the worker materializes an earlier WAL transaction. The earlier projection can receive the later scope.

This explicitly violates final commit-ordered projection authority. Ownership scopes can expose a row to the wrong user.

**Change:** Capture old and new scope sets in the source transaction fence. Materialization must consume those captured sets.

Do not query the live source relation for historical membership.

**Estimated line delta:** `+120` production lines.

**Confidence:** High.

### C2. Slot advancement can outrun durable acknowledgement

**Impact:** High availability and recovery risk.

**Evidence:**

- `extensions/synchro-pg/src/bgworker.rs:2203-2255` advances the candidate slot before updating durable acknowledgement.
- `extensions/synchro-pg/src/bgworker.rs:5353-5386` repeats the sequence for the active slot.
- `extensions/synchro-pg/src/bgworker.rs:2850-2858` poisons a slot and durable-position mismatch.
- `docs/src/content/docs/spec/04-invariants.mdx:168` requires acknowledgement only after durable materialization.

> `FROM pg_catalog.pg_replication_slot_advance($1, $2::pg_lsn)`
>
> `SET acknowledged_end_lsn = $1::pg_lsn`

Replication slot advancement is not rolled back with the later metadata update. A failure between both operations leaves recoverable state marked as poison.

**Change:** Treat `materialized_end_lsn` as durable advancement intent. Reconcile slot-ahead-of-acknowledgement on startup when it does not exceed materialized state.

Keep slot positions beyond materialized state fatal.

**Estimated line delta:** `+35` production lines.

**Confidence:** High.

### C3. Compaction deactivates clients with future expiry times

**Impact:** High retention correctness risk.

**Evidence:**

- `extensions/synchro-pg/src/push.rs:579-605` treats a future expiry as unexpired.
- `extensions/synchro-pg/src/compaction.rs:95-104` deactivates every row where `generation_expires_at IS NOT NULL`.

> `generation_expires_at IS NULL OR generation_expires_at > now() AS unexpired`
>
> `generation_expires_at IS NOT NULL`

Compaction can exclude a still-valid client checkpoint and delete history that the client still requires.

**Change:** Deactivate an expiring client only when `generation_expires_at <= statement_timestamp()`.

**Estimated line delta:** `0` production lines.

**Confidence:** High.

### C4. Stream-reset locks are not bound to the designated session

**Impact:** High snapshot-integrity risk.

**Evidence:**

- `extensions/synchro-pg/src/stream_reset.rs:1997` accepts any PID where `holders.value = required_count.value`.
- The reset record does not bind that PID and backend start time.

> `WHERE holders.value = required_count.value`

Any backend can hold the known advisory keys during verification. It can release them immediately and allow concurrent source writes.

**Change:** Persist holder PID and backend start time in the reset record. Verify that exact backend owns every required lock.

Clear the binding on abort, activation, and cleanup.

**Estimated line delta:** `+35` production lines.

**Confidence:** High.

### C5. Swift prepared writes can commit without captured intent

**Impact:** High silent mutation-loss risk.

**Evidence:**

- `clients/swift/Sources/Synchro/SynchroClient.swift:107-109` exposes `withWritePreparedStatement`.
- `clients/swift/Sources/Synchro/ApplicationDatabase.swift:370-376` uses `queue.writeWithoutTransaction`.
- That path does not install authored capture context.
- `docs/src/content/docs/spec/04-invariants.mdx:72-74` forbids silent mutation loss.

> `queue.writeWithoutTransaction`

An update can commit while its trigger records no mutation.

**Change:** Delete the public writable prepared-statement API and internal pass-through methods. Use the contract transaction API.

**Estimated line delta:** `-19` production lines.

**Confidence:** High.

### C6. Ordinary SQL inserts lose absent-field semantics in both clients

**Impact:** High mutation correctness risk.

**Evidence:**

- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroDatabase.kt:968-990` marks every writable insert field as authored.
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/ApplicationSql.kt:217-224` parses the target but not the column list.
- `clients/swift/Sources/Synchro/ApplicationDatabase.swift:331-337` also builds default context from every writable column.
- `docs/src/content/docs/spec/02-client-contract.mdx:145-158` requires exact absent-versus-null preservation.

> `columnNames = table.columns.filter { it.writable }.map { it.name }`

An omitted defaulted column becomes present in the pushed mutation.

**Change:** Parse exact insert columns and update targets. Install only those columns in capture context.

Reject SQL shapes where the client cannot prove field presence.

**Estimated line delta:** `+140` production lines across both clients.

**Confidence:** High.

### C7. Rowless deletes bypass strict identity validation in both clients

**Impact:** High local integrity risk.

**Evidence:**

- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/PullProcessor.kt:920-924` returns only `value.content`.
- `clients/swift/Sources/Synchro/PullProcessor.swift:1081-1088` follows the same shortcut.
- Row-bearing changes pass through digest validation, but rowless deletes do not.

> `return value.content`

Malformed primary-key types and empty server versions can reach provenance and version storage.

**Change:** Validate every pull change before mutation. Require one canonical typed key and a nonempty server version.

Require `row_checksum` to be absent for rowless deletes.

**Estimated line delta:** `+70` production lines across both clients.

**Confidence:** High.

### C8. Both clients advance scope versions without assignment changes

**Impact:** High continuation-state risk.

**Evidence:**

- `clients/swift/Sources/Synchro/ContractModels.swift:1071-1080` accepts a future version with an empty delta.
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/ContractModels.kt:792-803` repeats that acceptance.
- Both pull processors persist the accepted value.
- `docs/src/content/docs/spec/04-invariants.mdx:50-52` permits advancement only when the assigned set changes.

> `scopeSetVersion >= requestScopeSetVersion`

The client can claim assignment state that it never applied.

**Change:** Require equality for an empty assignment delta. Require strict increase for a nonempty delta.

**Estimated line delta:** `+20` production lines across both clients.

**Confidence:** High.

### C9. Rebuild final-page application is not atomic in either client

**Impact:** High process-death correctness risk.

**Evidence:**

- `clients/swift/Sources/Synchro/PullProcessor.swift:311-503` commits final records and a receipt.
- `clients/swift/Sources/Synchro/PullProcessor.swift:565` begins finalization in another transaction.
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/PullProcessor.kt:289-384` applies the page.
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/PullProcessor.kt:470-530` finalizes separately.
- `docs/src/content/docs/spec/02-client-contract.mdx:712-722` requires one atomic final transition.

> `guard response.hasMore else { return attempt }`
>
> `if (currentAttempt != null) {`

Final rows can become visible before pruning, checksum verification, cursor installation, and attempt removal.

This explicitly violates the stated atomic local-transition rule.

**Change:** Apply the final page and all finality work in one transaction. Delete the separate pending-finality state and path.

**Estimated line delta:** `-160` production lines across both clients.

**Confidence:** High.

### C10. Swift retry exhaustion leaves a future illegal transition

**Impact:** High availability risk.

**Evidence:**

- `clients/swift/Sources/Synchro/SyncEngine.swift:721` transitions to `.backoff`.
- `clients/swift/Sources/Synchro/SyncEngine.swift:744-745` throws the last error after retry exhaustion.
- `clients/swift/Sources/Synchro/SyncEngine.swift:927-929` later rejects the retained `.backoff` state.

> `try transition(to: .backoff`
>
> `throw lastError`

A retryable periodic failure can become a permanent lifecycle failure on the next cycle.

**Change:** Give one durable scheduler ownership of retries. Resume the stored operation after its deadline.

Delete the second bounded retry loop.

**Estimated line delta:** `-45` production lines.

**Confidence:** High.

### C11. Valid large source transactions can exhaust worker memory

**Impact:** High availability risk.

**Evidence:**

- `extensions/synchro-pg/src/wal_decoder.rs:111-119` stores `events: Vec<WalEvent>` and `messages: Vec<WalLogicalMessage>`.
- `extensions/synchro-pg/src/bgworker.rs:2876-2908` accumulates decoded messages until commit.

> `events: Vec<WalEvent>`
>
> `messages: Vec<WalLogicalMessage>`

The configured batch size does not bound one transaction. Repeated worker restarts cannot process a transaction that exceeds memory.

**Change:** Define a transaction byte and event limit. Spool supported large transactions into bounded PostgreSQL staging storage.

Reject oversized canonical writes before commit when staging is not enabled.

**Estimated line delta:** `+180` production lines.

**Confidence:** High.

### C12. Pull returns token parser details to clients

**Impact:** Medium information exposure and unstable error behavior.

**Evidence:**

- `extensions/synchro-pg/src/pull.rs:431-442` returns `format!("scope {scope_id} cursor is invalid: {err}")`.
- `extensions/synchro-pg/src/cursor_token.rs:141-147` validates payload structure before MAC verification.

> `format!("scope {scope_id} cursor is invalid: {err}")`

Clients can distinguish parser, key, signature, and binding failures through error text.

**Change:** Parse only the key selector, verify authenticated bytes, then validate structure.

Return one bounded `invalid_request` message for malformed or forged cursors.

**Estimated line delta:** `-4` production lines.

**Confidence:** High.

### C13. Candidate catch-up rebuilds all membership after every transaction

**Impact:** Medium reset-time complexity.

**Evidence:**

- `extensions/synchro-pg/src/bgworker.rs:2300-2369` calls `recompute_candidate_membership`.
- That path deletes `sync_stream_reset_membership_edges` before rebuilding.

> `recompute_candidate_membership(client, target, registry)`
>
> `DELETE FROM synchro.sync_stream_reset_membership_edges`

The work scales with transaction count, candidate rows, and scope fanout.

**Change:** Apply only the transaction impact set during catch-up. Run one set-based reconciliation at the activation barrier.

**Estimated line delta:** `-120` production lines.

**Confidence:** High.

### C14. Materialization and reset staging perform SPI work per row

**Impact:** Medium database and lock cost.

**Evidence:**

- `extensions/synchro-pg/src/bgworker.rs:3504-3518` starts `for record in records`.
- `extensions/synchro-pg/src/bgworker.rs:3807-3877` resolves membership and writes edges in nested loops.
- `extensions/synchro-pg/src/materialize.rs:836-896` repeats per-row work during migration.
- `extensions/synchro-pg/src/stream_reset.rs:2053` loops over `load_source_rows`.
- `extensions/synchro-pg/src/stream_reset.rs:2154` starts another row loop during verification.

> `for record in records`
>
> `for source in load_source_rows(client, registration)?`

**Change:** Pass bounded key batches through `jsonb_to_recordset`. Use lateral membership calls and set-based edge changes.

Validate reset sets with aggregate mismatch queries.

**Estimated line delta:** `-430` production lines.

This estimate excludes the candidate catch-up ranges in C13.

**Confidence:** High.

### C15. Registry loading performs child queries for each relation

**Impact:** Medium repeated cost across most extension operations.

**Evidence:**

- `extensions/synchro-pg/src/registry.rs:3736` starts `for row in rows`.
- `extensions/synchro-pg/src/registry.rs:3738` calls `load_field_registrations`.
- `extensions/synchro-pg/src/registry.rs:3743` calls `load_capture_field_registrations`.

> `for row in rows`
>
> `load_field_registrations(`
>
> `load_capture_field_registrations(`

Pull, push, worker, seed, schema, and reset paths all load the registry.

**Change:** Load registrations and child fields with JSON aggregates in one query.

Perform catalog and capture-control checks with set-based mismatch queries.

**Estimated line delta:** `-160` production lines.

**Confidence:** High.

### C16. Push repeats lookup work and scans all schema history

**Impact:** Medium batch latency.

**Evidence:**

- `extensions/synchro-pg/src/push.rs:285-291` searches zipped fingerprints repeatedly.
- `extensions/synchro-pg/src/push.rs:951-960` selects all manifests with `ORDER BY schema_version`.
- `extensions/synchro-pg/src/push.rs:2191-2202` loads insert metadata for each insert.

> `FROM sync_schema_manifest ORDER BY schema_version`
>
> `SELECT a.attname::text AS attname`

**Change:** Pair fingerprints by ordinal. Fetch only distinct authored schema references.

Index manifests and registrations by immutable identity. Load insert metadata once per physical relation.

**Estimated line delta:** `-35` production lines.

**Confidence:** High.

### C17. Compaction batch size does not bound one transaction

**Impact:** Medium lock, WAL, and rollback cost.

**Evidence:**

- `extensions/synchro-pg/src/compaction.rs:123-126` starts `loop {`.
- `extensions/synchro-pg/src/compaction.rs:231-235` returns only when `count < batch_size`.

> `loop {`
>
> `if count < i64::from(batch_size)`

One invocation deletes every full batch in one transaction.

**Change:** Delete one batch per invocation. Return after advancing floors for that batch.

**Estimated line delta:** `-12` production lines.

**Confidence:** High.

### C18. Some tests preserve defects or prove no semantic outcome

**Impact:** Medium false confidence.

**Evidence:**

- `clients/swift/Tests/SynchroTests/SyncEngineTests.swift:741-750` asserts the prohibited intermediate rebuild-finality state.
- `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/SQLiteCompatibilityTests.kt:47` scans a SQL blacklist instead of executing supported SQLite.
- `clients/react-native/__tests__/hooks/useSyncStatus.test.ts:64-78` emits after unmount without asserting an observable result.
- `api/go/synchro_test.go:364` checks only whether `body["protocol_version"]` exists.

> `(storedDDL + upsertSQL).forEach(::assertSQLite392Compatible)`
>
> `if body["protocol_version"] == nil {`

**Change:** Replace these tests with atomic rollback, real SQLite execution, post-unmount observer, and exact pass-through assertions.

Add demonstrated negative controls where a parser or gate can pass without behavior.

**Estimated line delta:** `+40` test lines after deleting the ineffective assertions.

**Confidence:** High.

## 4. Executive Summary

The architecture is coherent, but the implementation is not dead simple. Server correctness depends on several large stateful files and many per-row database operations.

The native clients duplicate too much deterministic protocol code. The duplication has already produced observable Swift and Kotlin divergence.

The Go count needs context. The request adapter is much smaller than 7,150 lines.

The total includes a portable-seed generator, an operational bootstrap coordinator, release-version code, commands, and test support. The four HTTP handlers each use one canonical extension call.

I rejected two initial claims after independent review:

- Go seeds are not incompatible with current clients. Native installers migrate the intended seed baseline before final validation.
- `api/go/operator` does not reimplement extension state transitions. It coordinates required replication and snapshot operations.

### Defensible reducible lines

The estimates below are non-overlapping. They exclude positive-line correctness fixes and test changes.

C13 covers candidate catch-up only. C14 covers active materialization and reset staging outside those ranges.

| Confidence | Reducible production lines | Basis |
| --- | ---: | --- |
| High | 1,416 | Confirmed dead code, duplicated server work, bulk-query consolidation, atomic rebuild simplification, and retry deletion. |
| Medium | 3,620 | Native generation and consolidation work, plus terminal checksum and React Native digest removal. |
| Low | 540 | Incremental reduction from folding rebuild into pull, after excluding overlap with finality deletion. |
| **Total** | **5,576** | Non-overlapping defended estimate. |

The medium estimate removes handwritten source. It includes the replacement generator or catalog in each net value.

### Ten highest-impact changes

1. Stop historical WAL membership evaluation against live rows.
2. Make final rebuild application atomic in Swift and Kotlin.
3. Generate Swift and Kotlin protocol models and validators from one exact schema.
4. Replace row-by-row materialization and reset staging with bounded set operations.
5. Reconcile slot advancement with durable materialization intent after failure.
6. Fix ordinary SQL field-presence capture in both native clients.
7. Reject malformed rowless deletes before local mutation.
8. Bulk-load registry relations, fields, and capture metadata.
9. Fix compaction so future client expiries remain active.
10. Bind stream-reset source locks to the exact designated backend.

## 5. What This Audit Could Not Assess

- I did not execute PostgreSQL-backed tests, client integration tests, or long builds.
- I did not measure throughput, latency, memory, or lock duration.
- I did not inspect PowerSync code or run the Phase R4 benchmark.
- I did not audit dedicated test files completely. I used narrow test searches for finding coverage.
- I did not audit generated extension SQL, packaging, CI, conformance infrastructure, examples, or verification consumers.
- I did not prove external usage for exported Swift and Kotlin APIs. Repository reachability only proves no in-repository consumer.
- I did not reproduce concurrency failures. The report derives those paths from static lock, transaction, and state ordering.
- I did not validate dependency vulnerabilities or platform API behavior outside the certified support matrix.
- Current-stable iOS, Android, and React Native versions remain unresolved until release-candidate start.
- The support-scope and malformed-schema contradictions prevent one unambiguous conformance judgment for those cases.
