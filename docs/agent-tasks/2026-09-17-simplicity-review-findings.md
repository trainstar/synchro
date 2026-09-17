# Detailed Simplicity Review Findings

Source baseline: `98b1507537eaa708eab7cf14e3bf8eddc5eb9b7e`.

[Read the assessment and coordinated repair groups](2026-09-17-whole-repository-simplicity-review.md).

This file contains static findings and proposed acceptance checks, not release signoff.
Finding records can describe different sides of one repair. Do not count them as independent defects.
No protocol, schema, security, retention, support, or migration change is approved by this report.


<a id="area-01-adapter"></a>

## Go adapter and seed tooling

<a id="01-adapter-f01"></a>

### 01-adapter-F01: Seed conversion rejects every non-null portable int64

- **Severity:** Medium. A supported field type prevents seed generation, including valid full-width integer primary keys.
- **Classification:** Correctness defect.
- **Problem:** `api/go/seeddb/seeddb.go:1877-1912` combines `int` and `int64` under a `json.Number` assertion.
- **Compared implementation:** `api/go/seeddb/integrity.go:706-717` correctly requires a string for `int64`.
  `api/go/seeddb/seeddb.go:1383-1394` also returns a string when reading an SQLite int64 back into wire form.
- **Evidence:** `writePortableSeedData` verifies each record before calling `upsertPortableRecord` at `seeddb.go:1735-1744`.
  A valid int64 therefore reaches the writer as a string. The writer rejects that same value.
  The existing hydrated integration fixture uses text and timestamps at `seeddb_test.go:217-224,917-945`.
- **Smallest fix:** Give `int64` its required string-to-`int64` conversion. Remove the incorrect shared numeric branch.
  Keep canonical grammar validation in the existing record validator.
- **Invariant:** Full-width signed integers round trip without binary64 conversion or changed row digests.
- **Acceptance:** Add an extension-backed seed case with int64 minimum, maximum, zero, nullable values, and an int64 primary key.
  Run `make test-adapter GO_TEST_PKGS=./seeddb`.
- **Contract:** `docs/src/content/docs/spec/01-wire-protocol.mdx:98-114` and ADR 005, portable values.
  No supplied issue lead establishes this defect as already tracked.

<a id="01-adapter-f02"></a>

### 01-adapter-F02: Primary-key-only rows cannot appear in two portable scopes

- **Severity:** Medium. A valid shared row can make an otherwise valid portable export fail.
- **Classification:** Correctness defect.
- **Problem:** `api/go/seeddb/seeddb.go:1824-1862` adds conflict handling only when a table has non-primary-key columns.
- **Compared implementation:** `seeddb.go:1676-1819` processes every scope separately and intentionally upserts shared materialized rows.
  `seeddb.go:505-513,550-562` accepts a table with one supported primary-key field and `multi_scope` composition.
- **Evidence:** For a primary-key-only table, `assignments` is empty. The emitted SQL is a plain `INSERT`.
  An in-memory SQLite reproduction rejected the second identical row with `IntegrityError`.
  This reproduction used the source algorithm, not the Go implementation.
- **Smallest fix:** Emit `ON CONFLICT (primary_key) DO NOTHING` for the empty-assignment case.
  Keep the normal update branch for tables with other fields.
- **Invariant:** Each materialized row remains unique while each scope retains separate provenance and digest coverage.
- **Acceptance:** Export one primary-key-only row through two declared portable scopes.
  Require one local row and two verified scope edges.
  Run `make test-adapter GO_TEST_PKGS=./seeddb`.
- **Contract:** ADR 005 complete row and provenance rules, and `spec/04-invariants.mdx:438-444`.

<a id="01-adapter-f03"></a>

### 01-adapter-F03: Seed verification reparses quoted trigger names incorrectly

- **Severity:** Medium. Valid quoted PostgreSQL table names can prevent seed publication.
- **Classification:** Correctness defect.
- **Problem:** `api/go/seeddb/seeddb.go:1008-1014,1074-1080` extracts a trigger name with `strings.Fields` and quote trimming.
- **Compared implementation:** `seeddb.go:2023-2035,2091-2095,2169-2171` correctly quotes the full generated identifier.
  `extensions/synchro-pg/src/registry.rs:378-384,1726-1750` resolves quoted identifiers and retains the relation name as the logical table name.
- **Evidence:** For table `order items`, the generated name is `_synchro_cdc_insert_order items`.
  The verifier derives `_synchro_cdc_insert_order` instead.
  An in-memory SQLite reproduction confirmed the actual stored name and this mismatch.
  Embedded quotes also require unescaping that `strings.Trim` does not perform.
- **Smallest fix:** Derive expected trigger names directly from the table name and the three fixed trigger prefixes.
  Delete `triggerName`. Do not introduce an SQL parser for names the generator already owns.
- **Invariant:** Verification must compare the complete expected trigger set and definitions, including safely quoted identifiers.
- **Acceptance:** Generate seeds for names with spaces and embedded double quotes.
  Run `make test-adapter GO_TEST_PKGS=./seeddb` and `make test-client-schema-identity`.
- **Contract:** Schema names are descriptive manifest attributes, not a restricted identifier grammar.
  See `spec/01-wire-protocol.mdx:88-94`.

<a id="01-adapter-f04"></a>

### 01-adapter-F04: Seed integrity uses handwritten quadratic sorting

- **Severity:** Medium. Scope verification has avoidable quadratic work as scope cardinality grows.
- **Classification:** Behavior-preserving cleanup.
- **Problem:** `api/go/seeddb/integrity.go:1058-1064,1205-1219` implements three insertion sorts.
- **Compared implementation:** The same file already uses `sort.Slice` at `445-471`.
  Whole-scope verification calls `sortDigestRows` at `645-655`.
  SQLite provenance reads have no ordering clause at `api/go/seeddb/seeddb.go:1488-1529`.
- **Evidence:** Reversed input with 100,000 rows requires 4,999,950,000 adjacent comparisons in this algorithm.
  This count follows directly from the loops. It is not a measured runtime claim.
  The exporter already checks ordered page identities at `seeddb.go:1739-1742`.
- **Smallest simplification:** Use the standard-library sort with the existing byte and UTF-16 comparators.
  Delete the three handwritten swap loops. Do not replace the standard digest with a different aggregate.
- **Invariant:** Preserve unsigned byte identity order, UTF-16 JSON member order, and duplicate-row rejection.
- **Acceptance:** Check identical digests for sorted, reversed, and permuted inputs, including duplicate negative controls.
  Run `make test-adapter GO_TEST_PKGS=./seeddb`.
- **Contract:** ADR 005 ordered streaming SHA-256, `005-integrity-authorization-and-seeds.mdx:523-528,568`.

<a id="01-adapter-f05"></a>

### 01-adapter-F05: The seed exporter reimplements transaction lifetime management

- **Severity:** Medium. Cancellation can leave transaction cleanup dependent on later driver activity.
- **Classification:** Correctness defect.
- **Problem:** `api/go/seeddb/seeddb.go:47-88` implements a private transaction over `*sql.Conn`, a `done` flag, and manual SQL.
  `Generate` defers `pgTx.Close(ctx)` at `241-245`, using the work context for rollback.
- **Compared implementation:** `api/go/operator/operator.go:108-120,189-205` separates cleanup lifetime from canceled work.
  The standard `database/sql` transaction owns rollback on cancellation.
- **Evidence:** The wrapper ignores rollback failure and then returns the connection to the pool.
  If its context is already canceled, it cannot establish that rollback ran.
  Cached pgx v5.9.2 `stdlib/sql.go:553-561` discards an open transaction on the next checkout.
  That mitigates reuse. It does not make this `Close` end an idle transaction immediately.
- **Smallest simplification:** Use `sql.BeginTx` with serializable, read-only options.
  Set `DEFERRABLE` before any authoritative read on that transaction.
  Delete the custom `done`, `Commit`, `Close`, and forwarding method.
  Verify equivalent effective transaction characteristics before accepting this change.
- **Invariant:** Keep one physical connection and one enforced serializable, read-only, deferrable snapshot through verification.
- **Acceptance:** Cancel immediately after transaction start and verify that no transaction remains active.
  Keep the concurrent-source-write seed test.
  Run `make test-adapter GO_TEST_PKGS=./seeddb`.
- **Contract:** Snapshot Consistent Portable Seeds, `spec/04-invariants.mdx:430-432`.
  This report does not claim observed cross-transaction data contamination.

<a id="01-adapter-f06"></a>

### 01-adapter-F06: Two seed tests claim workflow proof while calling helpers directly

- **Severity:** Medium. The tests can pass after the production cleanup or publication sequencing regresses.
- **Classification:** Correctness defect in verification.
- **Problem:** `api/go/seeddb/seeddb_test.go:881-914` explicitly closes a transaction after calling the verifier.
  It does not invoke `Generate` on that failure path.
  `seeddb_test.go:1439-1451` checks an unpublished destination that no called function receives.
- **Compared implementation:** Production cleanup and publication are in `api/go/seeddb/seeddb.go:233-245,333-348`.
  The MAC-corruption test actually calls `Generate` and observes destination preservation at `seeddb_test.go:1298-1332`.
- **Evidence:** Deleting the deferred PostgreSQL cleanup in `Generate` does not alter the first test's explicit `pgTx.Close`.
  Deleting a publication guard cannot make the second test write its unused `destinationPath`.
  These are source-level negative controls, not executed mutants.
- **Smallest simplification:** Keep the useful artifact-corruption helper tests under accurate names.
  Delete the unrelated destination assertion and the claimed workflow meaning of manually recorded `BEGIN` and `ROLLBACK` calls.
  Put any publication-order proof on the actual generator path, with a demonstrated failing mutant.
- **Invariant:** Failed artifact verification must preserve the old destination and end the export transaction.
- **Acceptance:** Run `make test-adapter GO_TEST_PKGS=./seeddb` after replacing the misleading assertions.
  A workflow claim additionally requires a mutant that bypasses the actual verification or cleanup boundary.
- **Contract:** Verified Portable Seed Artifacts, `spec/04-invariants.mdx:438-440`.
  Current evidence establishes the proof gap, not a reproduced corrupt publication.

<a id="01-adapter-f07"></a>

### 01-adapter-F07: Final seed verification omits two installed runtime-state checks

- **Severity:** Medium. The verifier accepts changes to state that native clients use when opening or resuming a database.
- **Classification:** Correctness defect in verification.
- **Problem:** `api/go/seeddb/seeddb.go:789-836,926-972` never checks `PRAGMA user_version` or an empty `_synchro_rebuild_attempts` table.
- **Compared implementation:** The generator installs the version and rebuild table at `seeddb.go:1586-1595,1621`.
  `seeddb_test.go:396-402` checks the version only in one successful generation test.
  `api/go/seeddb/integrity.go:138-149` checks rebuild-table shape, not its contents.
- **Evidence:** An exact search found no other production `user_version` read or rebuild-attempt data check in the seed package.
  Changing the version or inserting a well-shaped rebuild row leaves all existing verifier queries unchanged.
- **Smallest fix:** Compare the stored version with `sqliteUserVersion` during verification.
  Add `_synchro_rebuild_attempts` to the empty runtime-work table check.
  Use the existing verification functions rather than a new validation layer.
- **Invariant:** A published seed has the canonical local schema version and no client-bound in-progress runtime work.
- **Acceptance:** Add both artifact mutations to the existing corruption table and require rejection.
  Run `make test-adapter GO_TEST_PKGS=./seeddb` and `make test-client-schema-identity`.
- **Contract:** `spec/05-schema-evolution.mdx:1012-1023` and Verified Portable Seed Artifacts.
  This is a static acceptance-gap finding. I did not mutate a repository seed.

<a id="01-adapter-f08"></a>

### 01-adapter-F08: A seed error includes row and scope identities in command output

- **Severity:** Medium. A handled write failure can disclose identifiers through build or operator logs.
- **Classification:** Correctness defect.
- **Problem:** `api/go/seeddb/seeddb.go:1764-1777` adds `scope.ID` and `recordID` to an error.
- **Compared implementation:** `api/go/cmd/synchro-seed/main.go:17-20,51-59` prints that returned error to stderr.
  HTTP errors deliberately return bounded messages at `api/go/errors.go:99-112`.
- **Evidence:** The identity values are direct error-format arguments. No redaction boundary removes them before command output.
  Portable shared data does not exempt record IDs or scope instances from the logging rule.
- **Smallest fix:** Remove identity values from operational errors.
  Retain a bounded operation classification and an appropriate error cause for internal handling.
  Do not print unbounded driver details through the command boundary.
- **Invariant:** Operators can identify the failed operation without receiving row values, identities, SQL, or tokens.
- **Acceptance:** Inject a write error using synthetic identity canaries and inspect command stderr.
  Run `make test-adapter GO_TEST_PKGS='./seeddb ./cmd/synchro-seed'`.
- **Contract:** ADR 005, `005-integrity-authorization-and-seeds.mdx:398-400`.
  No real identities or private data were inspected for this finding.

<a id="01-adapter-f09"></a>

### 01-adapter-F09: Numeric admission lacks a coherent boundary for non-fingerprintable JSON numbers

- **Severity:** Medium. An adapter-only parser change would turn current HTTP 400 rejection into an extension argument-conversion failure.
- **Classification:** Contract decision with an extension argument-decoding defect.
- **Primary correction:** The original proposal to add `UseNumber` alone was incorrect and is withdrawn.
  The follow-up traced the complete SQL argument and fingerprint path.
- **Current behavior:** `api/go/handlers.go:339-349,405-413` rejects numeric `1e400` while scanning the request.
  The request returns `400 invalid_request` before SQL.
- **Cross-boundary evidence:** `api/go/handlers.go:135-154` forwards the original body as JSONB.
  PostgreSQL can represent that number, but the locked pgrx JSONB decoder cannot convert it into finite-binary64 `serde_json::Value`.
  Its `expect` panics before `extensions/synchro-pg/src/push.rs:128-150` starts.
  The pgrx guard produces `XX000`, which `api/go/errors.go:101-112,131-145` maps to `500 sync_integrity_failure`.
  Dependency evidence: pgrx 0.17.0, `src/datum/json.rs:55-87`.
- **Fingerprint evidence:** `extensions/synchro-pg/src/push.rs:152-194,459-487` fingerprints before mutable value evaluation.
  `extensions/synchro-core/src/fingerprint.rs:176-210,243-247` retains submitted values in the normalized array.
  RFC 8785 cannot encode numeric `1e400` as finite binary64.
- **Decision needed:** Define whether non-fingerprintable numbers fail structural admission or require another approved fingerprint representation.
  An explicit admission rule can retain canonical `400 invalid_request` before ledger work.
  Terminal field outcomes instead require a compatible normalization design.
  Quoting, clamping, dropping, or substituting the submitted number is not an acceptable shortcut.
- **Invariant:** Preserve complete immutable request identity, exact replay, and canonical failure before side effects.
- **Acceptance:** Exercise the same raw request through HTTP and canonical SQL after the decision.
  Require the selected stable outcome without a pgrx panic or partial ledger work.
  Run `make test-adapter GO_TEST_PKGS=.` and `make test-rust-pg` for the approved boundary change.
- **Contract conflict:** `spec/01-wire-protocol.mdx:43,484-486,655-668` describes terminal field validation.
  ADR 002 at `002-mutation-idempotency-and-conflicts.mdx:85-90,131-165` requires complete RFC 8785 fingerprints first.
  Static dependency tracing established this correction. No request or database reproduction ran.

<a id="01-adapter-f10"></a>

### 01-adapter-F10: Four handlers repeat one transport pipeline and decode it three times

- **Severity:** Low. Changes to intake or forwarding require synchronized edits across four equivalent paths.
- **Classification:** Behavior-preserving cleanup.
- **Problem:** `api/go/handlers.go:22-210` repeats method, media type, identity, body, client ID, SQL error, and response handling.
  `handlers.go:276-311` builds a member map that every handler discards before unmarshaling the body again.
- **Compared implementation:** `api/go/protocol.go:3-22` has three identical one-field request structures.
  `api/go/synchro.go:124-134` repeats the same four route names in two mux registrations.
- **Evidence:** The endpoint differences are fixed SQL, allowed members, and the rebuild scope requirement.
  Searches found no repository consumer of the returned member map.
  This is shared transport behavior, not shared sync semantics.
- **Smallest simplification:** Use one small transport handler with four explicit endpoint configurations.
  Validate required string members from the already decoded map.
  Remove the extra full-body decode and duplicate transport branches.
  Register the wrapped endpoints from the same fixed list.
  Do not silently remove exported request types without a separate public API decision.
- **Invariant:** Each endpoint keeps exact intake behavior and one fixed, parameterized canonical SQL call.
  No caller can supply an SQL function name.
- **Acceptance:** Run `make test-adapter GO_TEST_PKGS=.` and `make lint-go`.
  Keep endpoint-specific allowlist, raw-byte, authentication-order, and canonical pass-through cases.
- **Contract:** `spec/01-wire-protocol.mdx:22-43`.

<a id="01-adapter-f11"></a>

### 01-adapter-F11: Startup compatibility performs one query per required signature

- **Severity:** Low. Seven avoidable startup round trips add serial work and error paths.
- **Classification:** Behavior-preserving cleanup.
- **Problem:** `api/go/contract.go:84-112` calls `to_regprocedure` separately for each fixed signature.
- **Evidence:** Each query reads the same catalog predicate and differs only by one signature value.
  No step depends on another result.
- **Smallest simplification:** Use one parameterized `VALUES` relation and return missing signatures with their original ordinals.
  Delete the query loop. Keep the earlier schema and contract-info checks.
  This needs only `database/sql`, not driver-specific array support.
- **Invariant:** Report every missing canonical signature and reject incompatible extension contracts.
- **Acceptance:** Cover no missing signatures and multiple missing signatures.
  Run `make test-adapter GO_TEST_PKGS=.`.
- **Related work:** The supplied `#54` lead concerns server query loops.
  This startup loop is a separate adapter-side instance. I did not verify that issue's full scope.

<a id="01-adapter-f12"></a>

### 01-adapter-F12: Version sync and version check duplicate their target inventory

- **Severity:** Low. A release surface can be added to one command and omitted from the other.
- **Classification:** Behavior-preserving cleanup.
- **Problem:** `api/go/internal/releaseversion/releaseversion.go:92-153` and `176-237` repeat the same 12 path and pattern entries.
- **Compared implementation:** Cargo replacement and matching already have special handling at `265-304`.
- **Evidence:** The two lists encode the same release ownership relationship.
  They are production policy, not independent expected test data.
- **Smallest simplification:** Build one release-target list consumed by `Sync` and `Check`.
  Express the Cargo replacement and comparison difference explicitly in that list.
  Delete the second inventory. Do not add a generic template system.
- **Invariant:** Keep all current targets, literal Gradle interpolation, aggregate drift errors, and manual PostgreSQL upgrade protection.
- **Acceptance:** Keep independent fixture expectations for each surface.
  Run `make test-adapter GO_TEST_PKGS=./internal/releaseversion` and `make version-check`.
- **Related work:** This is not a confirmed duplicate of `#100`.
  Catalog ownership outside this inventory still needs the assigned release review.

<a id="01-adapter-f13"></a>

### 01-adapter-F13: The release validator accepts noncanonical leading-zero versions

- **Severity:** Low. The release tool can write versions that the runtime SemVer gate rejects.
- **Classification:** Correctness defect.
- **Problem:** `api/go/internal/releaseversion/releaseversion.go:12,56-60,76-83` accepts `01.2.3` and writes it to release surfaces.
- **Compared implementation:** `api/go/version.go:61-65` rejects leading zeros.
  `api/go/synchro_test.go:113-132` explicitly protects that rejection.
- **Evidence:** A static regular-expression reproduction accepted `01.2.3`.
  The release tests omit leading-zero cases at `releaseversion_test.go:10-25`.
- **Smallest fix:** Restrict each release component to `0` or a nonzero digit followed by digits.
  Keep the release policy's stable three-component subset. Do not merge it with the broader prerelease parser.
- **Invariant:** Every accepted release version remains a canonical SemVer core without a leading `v`.
- **Acceptance:** Add leading-zero negative cases and verify that invalid `Set` input leaves fixture files unchanged.
  Run `make test-adapter GO_TEST_PKGS=./internal/releaseversion`.
- **Contract:** ADR 005 SemVer rules, `005-integrity-authorization-and-seeds.mdx:299-306`.

<a id="01-adapter-f14"></a>

### 01-adapter-F14: Release root discovery excludes linked Git worktrees

- **Severity:** Low. All version commands fail before argument dispatch when `.git` is a worktree file.
- **Classification:** Correctness defect.
- **Problem:** `api/go/internal/releaseversion/releaseversion.go:36-54` recognizes `.git` only when it is a directory.
- **Compared implementation:** `api/go/cmd/synchro-version/main.go:19-23` requires this lookup for every command.
  `releaseversion_test.go:104-108` only creates a directory-shaped `.git` fixture.
- **Evidence:** A linked worktree uses a `.git` indirection file. The current `IsDir` condition excludes that representation.
  No linked worktree was created during this review.
- **Smallest fix:** Recognize the supported `.git` file form as well as the directory form, or use Git's root discovery.
  Keep one root-discovery path, not separate behavior in each command.
- **Invariant:** Version commands operate on the intended repository root and fail outside a repository.
- **Acceptance:** Add nested-start fixtures for directory and file forms, plus a missing-root case.
  Run `make test-adapter GO_TEST_PKGS='./internal/releaseversion ./cmd/synchro-version'`.

<a id="01-adapter-f15"></a>

### 01-adapter-F15: Advisory-lock cleanup has duplicated paths and an uncertain-acquisition gap

- **Severity:** Medium. An acquisition error can return a session without establishing that its session lock was released.
- **Classification:** Correctness defect with behavior-preserving cleanup.
- **Problem:** `api/go/operator/operator.go:436-451` closes the pooled connection after any acquisition query or scan error.
- **Compared implementation:** `operator.go:456-469` and `678-691` contain identical unlock, discard-on-failure, and close logic.
  `operator.go:472-491` also discards uncertain lock state.
- **Evidence:** `sql.Conn.Close` returns the session to the pool. It is not a physical session termination.
  If the server acquired the lock before the client observed an error, the acquisition branch has no cleanup owner.
  The existing failure test covers release and downgrade, not acquisition, at `operator_test.go:236-287`.
- **Smallest fix:** Discard the session on uncertain acquisition errors.
  Close normally only after a definite `locked=false` result.
  Share the identical release implementation between the operation-lock and source-lock consumers.
  Preserve contextual errors and `driver.ErrBadConn` disposal.
- **Invariant:** Never return a session with uncertain advisory-lock state to the pool.
  Keep operation serialization and source-lock lifetime unchanged.
- **Acceptance:** Inject acquisition-success-with-client-error and verify a replacement physical connection.
  Retain release and downgrade failure tests.
  Run `make test-adapter GO_TEST_PKGS=./operator`.
- **Limit:** The uncertain-result path is established statically. I did not reproduce a network failure against PostgreSQL.

<a id="01-adapter-f16"></a>

### 01-adapter-F16: Strict operator decoding accepts null as a false state flag

- **Severity:** Medium. Malformed state can be treated as an absent slot or absent interrupted operation.
- **Classification:** Correctness defect.
- **Problem:** `api/go/operator/responses.go:102-106,289-295,309-346` verifies field presence but decodes flags into nonpointer booleans.
- **Compared implementation:** `api/go/operator/operator.go:915-925` uses the same decoder before trusting `Present`.
  `operator.go:781-786` treats an absent slot as successful cleanup.
- **Evidence:** Go `json.Unmarshal` accepts `null` for a bool and leaves its zero value.
  `{"present":null,"active":false,"valid":true}` therefore passes `parseSlotDropState` as an absent slot.
  The existing strict-response cases omit null flags at `operator_test.go:93-120`.
- **Smallest fix:** Require non-null booleans for these state responses.
  Keep intentionally nullable schema, boundary, and lifecycle fields distinct.
  Do not replace the decoder with a general reflection validator.
- **Invariant:** Unknown or malformed recovery evidence cannot authorize cleanup success.
- **Acceptance:** Add null-flag controls for slot state and interrupted state.
  Run `make test-adapter GO_TEST_PKGS=./operator`.
- **Limit:** This proves a fail-closed parser gap. It does not claim that the current extension emits null flags.

<a id="01-adapter-f17"></a>

### 01-adapter-F17: Scripted recovery tests duplicate successful recovery without checking arguments

- **Severity:** Low. The tests maintain a second scripted recovery model while missing wrong-target calls.
- **Classification:** Behavior-preserving cleanup of verification, with replacement evidence required before deletion.
- **Problem:** `api/go/operator/operator_test.go:290-343,525-569` scripts the successful query sequence for each lifecycle.
  `operator_test.go:579-590,625-640` matches query substrings and ignores all bound arguments.
- **Compared implementation:** Real process recovery is exercised at `conformance/blackbox/integration/real_baseline_test.go:178-277`.
  Pure lifecycle planning already has focused tests at `operator_test.go:190-221`.
- **Evidence:** A wrong bootstrap ID passed to an abort or completion query does not affect the scripted result.
  The real test checks discarded candidate identity, slot absence, and cleared staging state.
  It does not establish deterministic coverage of every pre-activation lifecycle or activated recovery.
- **Smallest simplification:** Keep pure parser/planner tests and controlled connection-failure tests.
  Put successful recovery state coverage in the real scenario proof.
  Then delete the duplicate successful script and its lifecycle response builder.
  Do not delete the shared fake driver while its failure-injection consumers still require it.
- **Invariant:** Preserve both pre-activation abort recovery and committed-activation cleanup recovery.
- **Acceptance:** First establish deterministic real lifecycle cases and a wrong-target mutant.
  Run `make test-adapter GO_TEST_PKGS=./operator`.
  Run the existing real recovery case with:

  ```text
  make test-blackbox GO_TEST_ARGS='-v -count=1 -p 1 -run ^TestRealClass3ProjectionBootstrapRecoversAfterProcessTermination$'
  ```
- **Contract:** `SCN-PERF-SCHEMA-CHECK-001`, `OBL-PERF-SCHEMA-CHECK-PROJECTION-FAULT-001`.
  This finding does not authorize deleting the recovery algorithm or its failure cleanup.

<a id="01-adapter-f18"></a>

### 01-adapter-F18: The legacy SQLite syntax test is a substring blacklist, not compatibility proof

- **Severity:** Medium. New unsupported syntax can pass while the test claims SQLite 3.9.2 compatibility.
- **Classification:** Correctness defect in verification.
- **Problem:** `api/go/seeddb/seeddb_test.go:570-651` opens current `modernc.org/sqlite` and checks 15 literal substrings.
- **Compared implementation:** `api/go/seeddb/seeddb.go:2016-2022` identifies the real requirement: minimum Android must parse the stored trigger bodies.
- **Evidence:** A trigger containing `ON\nCONFLICT` parses on the available current SQLite and misses the `ON CONFLICT` substring.
  Whitespace changes therefore defeat the existing negative check without changing SQL semantics.
  I executed this bounded in-memory reproduction. I did not run SQLite 3.9.2.
- **Smallest simplification:** Use the supported API 24 cell to open the generated artifact and exercise capture triggers.
  Keep that as the authoritative compatibility proof.
  Delete the keyword list once that proof has an unsupported-syntax negative control.
  Do not grow the list into a private SQL parser.
- **Invariant:** The oldest supported client must parse every stored schema statement and perform offline capture.
- **Acceptance:** Run the generated-seed open and capture case through `make test-kotlin` on the API 24 cell.
  Demonstrate failure with an UPSERT trigger mutant.
  Run `make test-adapter GO_TEST_PKGS=./seeddb` for remaining generator tests.
- **Contract:** `README.md:41-46` support scope. The primary owns device configuration and execution.

<a id="01-adapter-f19"></a>

### 01-adapter-F19: The SQL-qualification check has a stale function whitelist

- **Severity:** Low. The gate does not cover several current production SQL calls.
- **Classification:** Correctness defect in verification.
- **Problem:** `api/go/schema_qualification_test.go:13-35` only recognizes nine named functions.
- **Compared implementation:** Readiness uses `synchro_readiness` at `api/go/synchro.go:144`.
  The operator uses many canonical projection-bootstrap functions, including `operator.go:610-615,870-875`.
- **Evidence:** Removing `synchro.` from those calls does not produce a regex match.
  The test can also pass when its scan finds no matching call.
- **Smallest simplification:** Replace the endpoint-specific alternation with the canonical function-name rule.
  Add negative controls for unqualified runtime, readiness, seed, and operator calls.
  Require a nonzero production match count.
- **Invariant:** Every production canonical SQL function reference remains explicitly schema-qualified.
  This static gate is not a substitute for runtime authorization or search-path tests.
- **Acceptance:** Run `make test-adapter GO_TEST_PKGS=.` with demonstrated qualification mutants.
- **Contract:** ADR 005 fixed, nonrelocatable `synchro` schema, `005-integrity-authorization-and-seeds.mdx:538-544`.

<a id="01-adapter-f20"></a>

### 01-adapter-F20: Test identifier truncation produces 64-byte names

- **Severity:** Low. Long prefixes can make PostgreSQL truncate a fixture name and break activation or cleanup lookups.
- **Classification:** Correctness defect.
- **Problem:** `api/go/internal/testsupport/postgres.go:63-75` reserves only one separator in the long-prefix truncation branch.
- **Compared implementation:** The final expression includes two separators.
  `postgres_test.go:9-29` checks the length only with a short prefix.
- **Evidence:** The long-prefix branch retains 51 prefix bytes, one test-name byte, two separators, and ten hash bytes.
  The resulting total is 64 bytes. A direct arithmetic reproduction confirmed the total.
- **Smallest fix:** Budget both separators before truncating the prefix.
  Keep the existing unique suffix and deterministic identifier formatting.
- **Invariant:** Every generated identifier fits PostgreSQL's 63-byte limit without server truncation.
- **Acceptance:** Add a prefix longer than the full identifier budget.
  Run `make test-adapter GO_TEST_PKGS=./internal/testsupport`.

<a id="01-adapter-f21"></a>

### 01-adapter-F21: Clarify whether an unpublished completion marker requires two complete artifact scans

- **Severity:** Medium. The current interpretation repeats all SQLite row and scope recomputation and lengthens the open PostgreSQL export transaction.
- **Classification:** Contract decision. No change is approved.
- **Problem:** `api/go/seeddb/seeddb.go:333-340` performs full verification before and after changing only `snapshot_complete`.
- **Compared implementation:** `seeddb.go:839-886` changes that one metadata value and checkpoints the file.
  `seeddb.go:789-836,1142-1168,1477-1533` repeats integrity, schema, metadata, queues, row digests, and scope digests.
- **Evidence:** Both calls use the same expected schema, tables, scopes, and digests.
  The contract requires verified final bytes and says completion is set only in the verified final artifact.
  See ADR 005 `005-integrity-authorization-and-seeds.mdx:519-534`.
- **Coherent alternative:** Define the completion marker as a property of the unpublished candidate.
  Write it in the initial atomic SQLite build, close the file, perform all final verification once, and publish only on success.
  Delete the second writable open, finalization transaction, and duplicate full scan.
- **Invariant:** No consumer sees an unverified artifact. Verification failure preserves the destination and removes the candidate.
  Keep complete final-file verification, digest recomputation, no-sidecar checks, and atomic publication.
- **Decision needed:** Confirm whether marking an inaccessible, unpublished candidate is allowed before final verification.
  Do not simply remove either current verification call without resolving this interpretation.
- **Acceptance after approval:** Run `make test-adapter GO_TEST_PKGS=./seeddb` with completion, corruption, cancellation, and publication-failure controls.

<a id="01-adapter-f22"></a>

### 01-adapter-F22: Decide whether equivalent index spelling is part of seed integrity

- **Severity:** Low. Textual index checks reject equivalent SQL while duplicating structural index verification.
- **Classification:** Contract decision. No deletion is approved.
- **Problem:** `api/go/seeddb/integrity.go:328-332,355-369` requires normalized SQL text equality in addition to index metadata equality.
- **Compared implementation:** `integrity.go:271-343,371-403` checks origin, uniqueness, partial predicates, column names, collations, expressions, and sort direction.
  `integrity_test.go:130-135` explicitly treats quoting the same column names as corruption.
- **Evidence:** `(table_name, record_id)` and `("table_name", "record_id")` describe the same columns and index semantics.
  The test targets spelling alone, not a changed constraint or lookup path.
- **Coherent alternative:** Define accepted seed index integrity by exact structural properties.
  Delete the redundant SQL-text comparison and the spelling-only corruption case.
  Retain expression, collation, order, uniqueness, partial-index, missing-index, and extra-index rejection.
- **Invariant:** The index must provide the exact required keys and semantics.
  Native schema identity and packaged-artifact checks must still detect actual schema drift.
- **Decision needed:** Determine whether canonical SQL spelling itself is a supported artifact contract or only a generator convention.
- **Acceptance after approval:** Run `make test-adapter GO_TEST_PKGS=./seeddb` and `make test-client-schema-identity`.

<a id="01-adapter-f23"></a>

### 01-adapter-F23: Version synchronization omits catalogs that the release validator requires

- **Severity:** Medium. A version update can pass the version check while leaving the authored contract at another release version.
- **Classification:** Correctness defect in release tooling.
- **Existing issue:** `#100`.
- **Evidence:** `api/go/internal/releaseversion/releaseversion.go:92-153,176-237` updates and checks the artifact inventory.
  Neither list includes the requirements catalog or support matrix.
  `docs/scripts/validators/support-policy.mjs:29-33` requires both release fields to equal `VERSION`.
  `conformance/schemas/requirements-v2.schema.json:11` independently fixes the accepted requirements release to `0.3.0`.
  The version fixture at `api/go/internal/releaseversion/releaseversion_test.go:104-125` omits those catalogs.
- **Smallest simplification:** Use the single target inventory proposed in F12 for every distribution-version field.
  Include the catalogs and validators that explicitly bind to `VERSION`.
  Keep distribution version, protocol version, schema version, and historical authored expectations distinct.
  Do not change protocol semantics to make a package-version check pass.
- **Invariant:** One version authority must produce a self-consistent next-release candidate.
  Drift and mismatched source tags must still fail.
- **Acceptance:** Change a representative fixture to a next release version.
  Run both the version checker and the actual support-policy validator against that fixture.
  Keep negative controls for catalog drift and tag mismatch.
  Use `make test-adapter GO_TEST_PKGS=./internal/releaseversion` and the supported contract checks after implementation.
- **Evidence limit:** The primary reconfirmed the source omission and validator requirements.
  No repository version was changed, and no version-update execution is claimed.


<a id="area-02-rust-core"></a>

## Portable Rust core

<a id="02-rust-core-f01"></a>

### 02-rust-core-F01: Required nullable members deserialize as optional members

- **Severity:** Medium. The core accepts missing protocol members and loses the distinction between an incomplete object and an explicit null.
- **Classification:** Correctness defect.
- **Problem references:**
  - `extensions/synchro-core/src/contract.rs:280-303`
  - `extensions/synchro-core/src/contract.rs:397-403`
  - `extensions/synchro-core/src/contract.rs:430-439`
  - `extensions/synchro-core/src/contract.rs:1282-1303`
  - `extensions/synchro-core/src/contract.rs:1328-1381`
  - `extensions/synchro-core/src/contract.rs:2516-2526`
- **Compared contract and consumer:**
  - `docs/src/content/docs/spec/01-wire-protocol.mdx:41-45`
  - `docs/src/content/docs/spec/01-wire-protocol.mdx:915-932`
  - `docs/src/content/docs/spec/05-schema-evolution.mdx:287-309`
  - `extensions/synchro-pg/src/client.rs:139-154`

**Observation and evidence**

Required nullable members use plain `Option<T>` with derived `Deserialize`.
Serde treats an absent `Option<T>` as `None`.
`deny_unknown_fields` does not change this behavior.

For example, deserializing `{}` as `ScopeCursorRef` produces `cursor: None`.
Its validator then succeeds.
An empty lifecycle object similarly becomes three null lifecycle values.
An initial manifest can omit `parent_schema` without triggering its lineage check.
A first rebuild request can omit `cursor` and become an explicit first-page request.

The lifecycle test checks serialization of null and a successful round trip.
It never removes a required member.
Its name therefore claims a requirement that its assertions do not establish.

Installed dependency evidence confirms the missing-member behavior:
`serde-1.0.228/src/private/de.rs:22-60` explicitly maps a missing `Option<T>` to `visit_none`.
The package version is locked at `extensions/Cargo.lock:1541-1549`.

**Smallest coherent change**

Use one required-nullable deserializer for these fields, without a `default` attribute.
Keep the existing omitted-only deserializer for genuinely optional non-null members.
Replace the round-trip-only lifecycle claim with missing, explicit-null, and valid-value cases.
This removes the implicit absence fallback rather than adding another post-parse presence tracker.

**Invariant**

Preserve all three contract states where applicable: required value, required explicit null, and permitted omission.

**Acceptance check**

Add direct raw-object decoding cases for each affected structure.
Remove one required nullable member at a time and require decoding failure.
Keep valid explicit-null cases successful.
Run `make test-rust-core` and `make lint-rust-core`.
The integrated extension path also needs the primary's focused boundary check.

**Traceability:** Wire protocol common JSON rules and schema-manifest required semantics.
No supplied known-issue lead establishes an existing match.

<a id="02-rust-core-f02"></a>

### 02-rust-core-F02: Three Rust JSON visitors own the same duplicate-member rule

- **Severity:** Medium. Parser fixes require several edits, and the checksum path allocates a disposable tree before parsing the same value again.
- **Classification:** Behavior-preserving cleanup.
- **Problem and comparison references:**
  - `extensions/synchro-core/src/checksum.rs:872-1019`
  - `extensions/synchro-core/src/contract.rs:1891-2012`
  - `extensions/synchro-core/src/fingerprint.rs:726-835`
  - Raw-token preservation: `extensions/synchro-core/src/checksum.rs:399-411`
  - Different numeric policies: `extensions/synchro-core/src/checksum.rs:1021-1066` and `extensions/synchro-core/src/fingerprint.rs:250-288`

**Observation and evidence**

`StrictJson`, `StrictJsonValue`, and the test-only `UniqueValue` each implement recursive JSON decoding and duplicate-member rejection.
The checksum visitor builds a separate tree that discards boolean values.
`parse_json_value` then discards that tree and parses the source into `serde_json::Value` again.
The fingerprint vector adapter maintains another nearly identical visitor instead of exercising the existing production decoder.

This duplication is within Rust and within one semantic rule.
It is not necessary cross-language independence.
The authored canonical bytes and digests provide the independent expectations.

**Smallest coherent change**

Keep one crate-private duplicate-rejecting `Value` decoder for contract fields, checksum parsing, and the fingerprint vector adapter.
Return its parsed value from checksum parsing instead of reparsing the same source.
Delete the other visitor implementations and the disposable `StrictJson` representation.

Keep original raw field tokens where canonical lexical spelling matters.
Do not serialize a parsed value back into text before validating `int`, `float`, or other raw wire spellings.
Keep the distinct numeric validation policies explicit.
Fingerprinting must still accept finite portable float values outside the safe integer range.

**Invariant**

Reject duplicate members at every nesting level.
Preserve scalar values, lexical canonical checks, Unicode rules, and existing independent expected bytes.

**Acceptance check**

Run `make test-rust-core` and `make lint-rust-core`.
Keep every existing authored-vector comparison unchanged.
Exercise duplicate keys through the production decoder, not an independent test-only parser.
The primary should retain the applicable parser mutation checks after consolidation.

**Traceability:** ADR 002 canonical fingerprints and ADR 005 canonical typed encoding.
This does not replace the independent oracle discussed in known issue `#36`.

<a id="02-rust-core-f03"></a>

### 02-rust-core-F03: The accepted JSON depth test bypasses the parser that rejects that depth

- **Severity:** Medium. The tests claim support for the boundary while valid-looking public inputs encounter a stricter hidden limit.
- **Classification:** Correctness defect in the boundary proof and inconsistent implementation limits.
- **Problem references:**
  - `extensions/synchro-core/src/checksum.rs:18-19`
  - `extensions/synchro-core/src/checksum.rs:852-857`
  - `extensions/synchro-core/src/checksum.rs:880-907`
  - `extensions/synchro-core/src/checksum.rs:1045-1057`
  - `extensions/synchro-core/src/checksum.rs:1806-1861`
  - Comparable private-only tests: `extensions/synchro-core/src/fingerprint.rs:448-488`
- **Dependency references:**
  - `extensions/Cargo.lock:1581-1592`
  - Installed `serde_json-1.0.149/src/de.rs:63`
  - Installed `serde_json-1.0.149/src/de.rs:1372-1385`
  - Installed `serde_json-1.0.149/src/de.rs:1433-1453`

**Observation and evidence**

The checksum validator accepts 128 nested containers.
Its test constructs `StrictJson` directly and asserts that 128 nested arrays and objects are valid.
The public path first uses Serde's default JSON decoder.
That decoder starts with 128 remaining levels and rejects when the decremented value reaches zero.
It therefore rejects the 128th container before the core depth validator runs.

A concrete input is 128 opening brackets, `null`, and 128 closing brackets.
Encode that text as the wire string for a portable `json` field.
The private validator accepts the represented tree.
The public decoding path rejects the inner document.
This conclusion follows from the locked decoder's control flow, not an executed Rust test.

**Smallest coherent change**

Put the intended depth limit in the actual decoding path.
Use one bounded parser policy rather than an inaccessible post-parse allowance.
Replace the synthetic deepest-tree proof with public typed-value cases immediately below, at, and above the selected boundary.
Do not disable parser protection without enforcing a bound during parsing.

**Invariant**

All public entry points must enforce the same selected JSON limits without unbounded recursion.

**Acceptance check**

Use the concrete bracket input through `encode_typed_value`, not `validate_i_json` alone.
Run `make test-rust-core` after adding the boundary cases.
Keep the above-limit negative control.

**Traceability:** ADR 005 portable JSON encoding and Phase 4 adversarial proof.
The core constants specify 128, but the reviewed ADR excerpt does not define the nesting-count convention.
The primary should confirm that convention before changing the externally accepted boundary.

<a id="02-rust-core-f04"></a>

### 02-rust-core-F04: Scope hashing materializes the complete preimage before hashing

- **Severity:** Medium. Each scope digest allocates another buffer proportional to every identity in the scope.
- **Classification:** Behavior-preserving cleanup.
- **Problem references:** `extensions/synchro-core/src/checksum.rs:645-690`
- **Compared consumers and contract:**
  - `extensions/synchro-core/src/checksum.rs:1608-1659`
  - `extensions/synchro-pg/src/scope_digest.rs:74`
  - `extensions/synchro-pg/src/pull.rs:1536`
  - `extensions/synchro-pg/src/rebuild.rs:719`
  - `extensions/synchro-pg/src/portable_seed.rs:1054`
  - `docs/src/content/docs/architecture/decisions/005-integrity-authorization-and-seeds.mdx:208-232`

**Observation and evidence**

`scope_digest` calls `scope_digest_preimage`, then feeds the resulting complete buffer into one SHA-256 update.
The preimage builder also makes an ordered reference vector and a separate capacity-calculation pass.
The extra preimage storage contains each complete row identity plus 40 bytes per entry, in addition to the scope header.
Production needs the digest, while the vector test also needs the preimage bytes.

The current algorithm produces the specified SHA-256 formula.
The finding concerns its avoidable complete buffer, not a different hash algorithm.

**Smallest coherent change**

Emit the framed scope bytes through one private encoding routine.
Its two concrete consumers are the SHA-256 context and the test preimage collector.
Delete the production preimage allocation and its size-calculation pass.
Keep sorting and duplicate-identity rejection until an independently verified ordered-input contract replaces them.

**Invariant**

Preserve the exact domain, schema, scope ID, cardinality, ordering, identity-to-digest pairing, and duplicate rejection.

**Acceptance check**

Run `make test-rust-core` against the unchanged authored preimage and digest vectors.
Inspect the production call graph to confirm that hashing no longer collects the complete preimage.
The primary should measure memory on a large scope if it needs a performance claim.
No memory benchmark ran in this review.

**Traceability:** ADR 005 Scope digest and the Scope Digest Binding invariant.
This is distinct from known issue `#50`, which concerns WAL transaction buffering.

<a id="02-rust-core-f05"></a>

### 02-rust-core-F05: Vector tests can pass when their kind filter selects no vectors

- **Severity:** Medium. A required semantic proof can silently disappear while the Rust test count stays nonzero.
- **Classification:** Correctness defect in verification.
- **Problem references:**
  - `extensions/synchro-core/src/checksum.rs:1437-1450`
  - `extensions/synchro-core/src/checksum.rs:1512-1531`
  - `extensions/synchro-core/src/checksum.rs:1533-1606`
  - `extensions/synchro-core/src/checksum.rs:1608-1659`
  - `extensions/synchro-core/src/fingerprint.rs:348-372`
- **Compared gates:**
  - `Makefile:1868-1869`
  - `conformance/vectors/load.go:123-129`
  - `conformance/vectors/load.go:175-218`

**Observation and evidence**

Each vector test filters by a string kind and then performs assertions only inside the loop.
None asserts that the filter matched an input.
The combined row test also does not require both row kinds.
A filter typo can leave an otherwise valid vector catalog unchanged while removing all assertions from that test.
The Rust test runner still reports that test as executed.

The separate catalog loader verifies source hashes and counts.
It does not prove that each Rust filter consumed its intended kind.
Other handwritten tests do not replace the missing independent expected bytes.

**Smallest coherent change**

Require a nonzero consumed count for every intended vector kind.
Make the combined row test check both categories.
Keep the checks local to these consumers rather than adding another vector framework.

**Invariant**

Every required Rust vector category must execute comparisons against authored expectations.

**Acceptance check**

Run `make test-rust-core` with the normal vector source.
Then use `SYNCHRO_REPO_ROOT` to select an isolated fixture copy missing one required kind.
Require the same Make gate to fail.
Alternatively, demonstrate a filter-string mutant that matches no vectors and require failure.
No negative-control execution ran during this review.

**Traceability:** Repository Gate Integrity rules and Phase 4 zero-match enforcement.
No supplied known-issue lead establishes an existing match.

<a id="02-rust-core-f06"></a>

### 02-rust-core-F06: Semantic-version validation constructs an unused owned model

- **Severity:** Low. Validation allocates strings, vectors, and detailed errors that its only production caller discards.
- **Classification:** Behavior-preserving cleanup within repository consumers.
- **Problem references:** `extensions/synchro-core/src/version.rs:3-116`
- **Only production consumer:** `extensions/synchro-core/src/contract.rs:1875-1877`
- **Related tests and configuration:**
  - `extensions/synchro-core/src/version.rs:122-165`
  - `extensions/synchro-core/src/contract.rs:3606-3629`
  - `extensions/.cargo/mutants.toml:17`

**Observation and evidence**

The production caller invokes `Semver::parse(value).is_ok()`.
It never reads the parsed major, minor, patch, prerelease, build, or detailed error.
Display and owned-field behavior have test consumers only.
The mutation selector still names removed comparison methods such as `less_than` and `cmp_precedence`.

**Smallest coherent change**

Keep one allocation-free syntax validator over borrowed strings.
Delete the unused owned result, formatting implementation, and detailed error construction.
Consolidate the overlapping syntax tables in one proof home.
Remove stale mutation-selector alternatives.

**Invariant**

Preserve Semantic Versioning 2.0.0 syntax, including arbitrarily long numeric identifiers.
Do not replace this grammar with a parser that silently imposes a new integer-width limit.

**Acceptance check**

Run `make test-rust-core` and `make lint-rust-core`.
Keep valid and invalid syntax cases, including long numeric components.
Confirm that no production consumer requires the removed representation.

**Traceability:** Connect `app_version` validation.
External Rust API compatibility remains a primary decision.

<a id="02-rust-core-f07"></a>

### 02-rust-core-F07: Scope-name compatibility wrappers have no production consumer

- **Severity:** Low. The module exports and tests a second vocabulary without removing the original vocabulary.
- **Classification:** Behavior-preserving cleanup within repository consumers.
- **Problem references:**
  - `extensions/synchro-core/src/edge_diff.rs:18-22`
  - `extensions/synchro-core/src/edge_diff.rs:32`
  - `extensions/synchro-core/src/edge_diff.rs:60-62`
  - `extensions/synchro-core/src/edge_diff.rs:161-163`
  - `extensions/synchro-core/src/edge_diff.rs:348-354`
- **Actual consumer:** `extensions/synchro-pg/src/bgworker.rs:5479-5485,5529-5543`
- **Related selector:** `extensions/.cargo/mutants.toml:15`

**Observation and evidence**

The extension uses `build_edge_diff_entries`, `diff_bucket_sets`, and `bucket_id` directly.
Repository searches found no production calls to `scope_id`, `diff_scope_sets`, or `dedup_scope_ids`.
`ScopeDiff` exists only for the wrapper.
The alias test proves delegation rather than another required behavior.

**Smallest coherent change**

Delete the unused wrappers, alias, alias assertions, and corresponding selector alternatives.
Keep the existing bucket implementation and its actual callers.
Do not perform an unrelated terminology migration.

**Invariant**

Preserve actual WAL edge behavior, including insert-to-update conversion, delete targets, deduplication, and effect ordering.

**Acceptance check**

Repeat the exact symbol search and require no remaining callers.
Run `make test-rust-core` and `make lint-rust-core`.
The primary must check any external API commitment before deleting exported symbols.

**Traceability:** Shared deterministic edge semantics.
No supplied known-issue lead establishes an existing match.

<a id="02-rust-core-f08"></a>

### 02-rust-core-F08: The core declares an unused assertion dependency

- **Severity:** Low. Test dependency resolution retains a package and its two helper packages without a code consumer.
- **Classification:** Behavior-preserving cleanup.
- **Problem references:**
  - `extensions/synchro-core/Cargo.toml:15-16`
  - `extensions/Cargo.lock:401-404`
  - `extensions/Cargo.lock:1293-1301`
  - `extensions/Cargo.lock:1707-1717`
  - `extensions/Cargo.lock:2527-2531`

**Observation and evidence**

`pretty_assertions` appears in the manifest and generated lockfile.
No assigned Rust file imports or invokes it.
The repository-wide source search found no other use.
The reviewed lockfile attaches `diff` and `yansi` to this dependency.

**Smallest coherent change**

Remove the unused dev dependency.
Regenerate the lockfile through the supported dependency workflow.
Remove only packages that resolution no longer needs.
Do not manually prune unrelated generated entries.

**Invariant**

All existing assertion behavior and authored-vector comparisons remain unchanged.

**Acceptance check**

Run `make test-rust-core` and `make lint-rust-core`.
Confirm that the dependency and now-unreferenced helper packages leave the resolved graph.

**Traceability:** Repository direct-dependency use rule.
No supplied known-issue lead establishes an existing match.

<a id="02-rust-core-f09"></a>

### 02-rust-core-F09: Row hashing repeats immutable constructor guarantees

- **Severity:** Low. Every row digest builds another field-name set and executes checks that valid constructed rows cannot fail.
- **Classification:** Behavior-preserving cleanup.
- **Constructor references:** `extensions/synchro-core/src/checksum.rs:257-308,370-397`
- **Repeated checks:** `extensions/synchro-core/src/checksum.rs:693-730`

**Observation and evidence**

`CanonicalRow::new` rejects duplicate field IDs.
Its fields are private, and its public getter returns an immutable slice.
`CanonicalTable::new` similarly guarantees unique table field IDs.
`encode_row_body` checks equal field counts and rejects unknown row fields.
It also rebuilds a duplicate-detection set and then scans for omitted table fields.

Once the constructor guarantees hold, equal cardinality plus known field IDs proves completeness.
The second duplicate guard and final missing-field scan cannot add another public-input rejection.

**Smallest coherent change**

Delete the second `seen` set, its duplicate branch, and the final completeness scan.
Retain constructor rejection, equal cardinality, unknown-field rejection, typed-value validation, and separate-primary-key comparison.

**Invariant**

No malformed row may bypass duplicate, completeness, type, or primary-key validation at its public boundary.

**Acceptance check**

Run `make test-rust-core` with all authored row vectors unchanged.
Check duplicate rejection through `CanonicalRow::new` and `CanonicalRow::from_json`.
Keep unknown, omitted, and mismatched-primary-key negative controls.

**Traceability:** Canonical Typed Row Encoding and Row Digest Binding invariants.
This proposal removes only repeated checks justified by private immutable state.

<a id="02-rust-core-f10"></a>

### 02-rust-core-F10: The generation-binding assertion changes the user instead of the generation

- **Severity:** Low. The apparent generation check establishes batch user binding instead of isolating the generation field.
- **Classification:** Correctness defect in a test.
- **Problem reference:** `extensions/synchro-core/src/fingerprint.rs:403-408`
- **Related mutation identity check:** `extensions/synchro-core/src/fingerprint.rs:379-387`
- **Independent expected-form proof:** `extensions/synchro-core/src/fingerprint.rs:412-429,533-560`
- **Contract:** `docs/src/content/docs/architecture/decisions/002-mutation-idempotency-and-conflicts.mdx:113-165`

**Observation and evidence**

The code sets `changed_generation.client_generation = 2`.
Both compared fingerprints then use that same request.
The two calls differ only in authenticated user ID.
An implementation that ignores generation can still satisfy this assertion.

Other fixed expected-form checks already cover the encoded generation field.
This finding does not claim that all generation proof is absent.

**Smallest coherent change**

Delete the misleading fragment if the existing expected-form proof remains authoritative.
If a dedicated binding control is required, compare otherwise identical requests with different generations and the same user.
Do not retain both forms merely to increase the test count.

**Invariant**

The batch fingerprint binds client generation.
The mutation fingerprint intentionally excludes request generation.

**Acceptance check**

Run `make test-rust-core`.
If retaining a binding control, demonstrate that removing generation from the normalized batch makes that control fail.

**Traceability:** ADR 002 canonical fingerprint scope.
No supplied known-issue lead establishes an existing match.

<a id="02-rust-core-f11"></a>

### 02-rust-core-F11: The legacy queue-operation parser has no runtime consumer

- **Severity:** Low. A test-only parser retains another operation vocabulary and corresponding mutation-gate work.
- **Classification:** Behavior-preserving cleanup within repository consumers.
- **Problem references:** `extensions/synchro-core/src/change.rs:23-31,59-70,80-85`
- **Related selector:** `extensions/.cargo/mutants.toml:12`
- **Compared public wire vocabulary:**
  - `extensions/synchro-core/src/contract.rs:15-22,776-812`
  - `docs/src/content/docs/architecture/decisions/002-mutation-idempotency-and-conflicts.mdx:25-33`

**Observation and evidence**

`ChangeOperation::parse_wire` accepts `create`, `update`, and `delete` as queue operation strings.
Its only repository calls are tests in `change.rs`.
Current push parsing uses `Operation`, whose insertion name is `insert`.
The existing parser is therefore not the public protocol parser or a consumed Rust queue boundary.

**Smallest coherent change**

Delete `parse_wire`, its dedicated assertions, and its mutation-selector alternative.
Keep `from_i16`, `to_i16`, the operation enum, and their production callers.
This finding does not require removing diagnostic formatting or merging push and stored operation types.

**Invariant**

Preserve changelog numeric operation values and the exact current push vocabulary.

**Acceptance check**

Repeat the repository-wide `(wire_name|parse_wire)` source search to confirm the removed parser has no callers.
Run `make test-rust-core` and `make lint-rust-core`.
Confirm external Rust API compatibility before deleting the exported method.

**Traceability:** ADR 002 Push vocabulary.
No supplied known-issue lead establishes an existing match.


<a id="area-03-postgres-capture"></a>

## PostgreSQL capture and recovery

<a id="03-postgres-capture-f01"></a>

### 03-postgres-capture-F01: Projection activation can replace newer authoritative row versions with older candidate versions

- Severity: **High**. Cutover can invalidate a current compare-and-swap token and restore an older token for newer source data.
- Classification: **correctness defect**.
- Problem references:
  - `extensions/synchro-pg/src/stream_reset.rs:1339-1423`
  - `extensions/synchro-pg/src/stream_reset.rs:3210-3232`
- Compared paths:
  - `extensions/synchro-pg/src/bgworker.rs:4177-4217`
  - `extensions/synchro-pg/src/bgworker.rs:4849-4933`
  - `extensions/synchro-pg/src/lib.rs:1844-1874`
  - `extensions/synchro-pg/src/push.rs:2139-2167`
  - `api/go/operator/operator.go:317-364`

**Observed behavior and cost**

The operator releases source locks before it requests the activation barrier.
Candidate replay stops at that barrier.
Source transactions can commit after the barrier and before activation acquires its source-write lock.
Their triggers update authoritative `sync_row_versions` immediately.
Activation then calls the shared reset installer, which deletes every authoritative version and inserts the candidate versions.
The candidate contains only versions through the earlier barrier.

For example, candidate version `V1` can replace a later committed source version `V2`.
The source row still contains the `V2` data.
Push reads its equality token from `sync_row_versions`.
Normal active WAL materialization updates captured rows, not this authoritative version table.
Later WAL replay therefore does not repair that overwritten token.

This is not a request to remove candidate versions.
The defect comes from sharing a destructive installer between a blocked stream reset and an online projection bootstrap.

**Smallest coherent simplification**

Separate authoritative version installation from captured projection replacement.
For projection bootstrap, preserve existing authoritative versions and tombstones.
Install only missing baseline-generated reservations under the source gate, with explicit conflict checks.
Delete the full authoritative-version replacement from the projection-bootstrap path.
Keep the blocked stream-reset behavior subject to its stronger source-lock proof.

**Invariant**

Cutover must not rewind a committed source transition or change the equality token for its current authoritative data.
Candidate captured state must still represent exactly the selected barrier.

**Acceptance and evidence limit**

Add a real bootstrap scenario with an update and a delete committed after the barrier, before cutover.
Verify current tokens, tombstones, stale-token rejection, and subsequent WAL capture after activation.
Use `make test-blackbox-wal` after the scenario enters that proof home.
Static control flow establishes the overwrite window. No live reproduction ran.
Related contract: ADR 001, lines 127-132 and 152-168. Related invariants: Atomic Compare-and-Swap and Opaque Server Version.
No matching supplied issue was identified.

<a id="03-postgres-capture-f02"></a>

### 03-postgres-capture-F02: Baseline verification requires every historical pending fence to equal one final row version

- Severity: **High**. A normal backlog with repeated changes to one row can prevent the authorized recovery path.
- Classification: **correctness defect**.
- Problem references:
  - `extensions/synchro-pg/src/stream_reset.rs:2341-2374`
  - `extensions/synchro-pg/src/stream_reset.rs:2397-2527`
- Compared implementations:
  - `extensions/synchro-pg/src/stream_reset.rs:2021-2042`
  - `extensions/synchro-pg/src/stream_reset.rs:2570-2694`
  - `extensions/synchro-pg/src/lib.rs:1804-1868`

**Observed behavior and cost**

Staging copies every pending fence visible in the source snapshot.
The trigger inserts a distinct fence for each transition but keeps only the latest authoritative version per row.
The verifier requires each pending insert or update fence to match the staged final version.
Two pending updates with versions `V1` and `V2` cannot both equal that one version.
An insert followed by a delete also cannot satisfy the insert fence's required captured-row presence.
The capture-dependency branch has the same historical-presence problem for an insert followed by a delete.

The verifier mixes transition-history proof with final-snapshot proof.
Keeping more historical row images would add machinery without fixing that conceptual mismatch.

**Smallest coherent simplification**

Keep exact snapshot-visible fence coverage and its immutable metadata checks.
Validate the final row and version once per identity against the imported snapshot.
Use explicit fence lineage to connect earlier covered transitions to that final state.
Delete the requirement that each historical fence version or operation equal the final snapshot image.
Do not replace this proof with an LSN guess or omit earlier fences.

**Invariant**

Every snapshot-visible accepted fence needs exact baseline coverage.
The installed projection and authoritative final version must match the same causally bound snapshot.

**Acceptance and evidence limit**

Exercise two pending updates and an insert-then-delete sequence before a real reset snapshot.
Require successful verified activation and coverage for every fence.
Keep negative controls for missing coverage, a wrong final version, and a fence outside the imported snapshot.
Use `make test-blackbox-wal` after adding these reset cases.
The contradiction follows from the single final version and the per-fence equality predicates. No database run occurred.
Related contract: ADR 001, lines 93-108 and 154-166. Related invariant: Reset Baseline Fence Coverage.
No matching supplied issue was identified.

<a id="03-postgres-capture-f03"></a>

### 03-postgres-capture-F03: A verified candidate repeats full finalization on every worker poll

- Severity: **Medium**. An idle, verified candidate repeatedly scans and rewrites its complete membership and digest state.
- Classification: **behavior-preserving cleanup**.
- References:
  - `extensions/synchro-pg/src/bgworker.rs:525-548`
  - `extensions/synchro-pg/src/bgworker.rs:1644-1742`
  - `extensions/synchro-pg/src/bgworker.rs:1890-1897`
  - `extensions/synchro-pg/src/bgworker.rs:2369-2412`
  - `extensions/synchro-pg/src/bgworker.rs:2666-2740`
  - `extensions/synchro-pg/src/stream_reset.rs:1370-1418`

**Observed behavior and cost**

Candidate loading selects every `catching_up` operation, without checking `candidate_verified`.
When acknowledgement equals the barrier, the poll always calls `finalize_candidate`.
Finalization deletes and rebuilds all candidate membership edges, computes digests twice, and updates counts.
It sets `candidate_verified = true` but leaves the lifecycle as `catching_up`.
The next poll repeats the same work after the 100-millisecond idle wait.
Activation needs the worker gate, so each redundant pass also extends its wait.

**Smallest coherent simplification**

Make verification a one-time transition for one immutable barrier.
Load or filter on `candidate_verified` and stop candidate work after successful verification.
Keep the existing activation-time binding, integrity, digest, and count checks.
Delete repeated finalization, not those checks.

**Invariant**

A change to candidate contents or the selected barrier must clear verification.
Only a complete candidate at that barrier can activate.

**Acceptance and evidence limit**

Leave a verified candidate unactivated across several polls.
Confirm stable candidate row identities, unchanged staged timestamps, and no repeated membership evaluation.
Then activate and verify the same digest and scope set.
Use a focused case under `make test-blackbox-wal`.
The repeated call path is statically established. Its runtime cost was not measured.
Related contract: ADR 001, lines 110-132. No matching supplied issue was identified.

<a id="03-postgres-capture-f04"></a>

### 03-postgres-capture-F04: Schema activation migrates every retained digest twice

- Severity: **Medium**. Class 2 and Class 4 publication repeat full retained-row and historical-projection migrations under table locks.
- Classification: **behavior-preserving cleanup**.
- References:
  - `extensions/synchro-pg/src/bgworker.rs:5671-5678`
  - `extensions/synchro-pg/src/schema.rs:229-239`
  - `extensions/synchro-pg/src/schema.rs:288-313`
  - `extensions/synchro-pg/src/materialize.rs:359-669`
  - `extensions/synchro-pg/src/materialize.rs:446-473`
  - `extensions/synchro-pg/src/materialize.rs:707-754`

**Observed behavior and cost**

`publish_schema_manifest` already migrates digests after Class 2 or Class 4 publication.
`activate_generations` then calls the same migration unconditionally.
Each pass loads every retained current row and historical projection, computes hashes, and issues updates.
The second pass has no source-generation filter to skip records already migrated.
The current-row loop also checks identity and source digest immediately before `migrate_schema_row` repeats those checks.

These are duplicate executions, not independent verification implementations.

**Smallest coherent simplification**

Give publication and activation an explicit result that prevents a second completed migration.
Keep migration for activation paths where publication did not perform it.
Delete the redundant outer identity and source-digest checks because `migrate_schema_row` owns them for both record types.
Preserve standalone publication callers.

**Invariant**

Publish the child manifest before computing child digests.
Commit manifest, row projections, edge digests, and generation state atomically.
Validate each original source digest before replacement.

**Acceptance and evidence limit**

Use Class 2, Class 4, and unchanged-manifest activation cases with current and historical rows.
Confirm one migration pass, identical digests, and rejection of a corrupted original digest.
Use `make test-rust-pg` and `make lint-rust-pg` after the cleanup.
The Make target has no separate Rust test-filter variable. No tests ran during this review.
Related contract: schema-bound row integrity in ADR 005. This is distinct from issue #95, which concerns Swift historical-schema validation.

<a id="03-postgres-capture-f05"></a>

### 03-postgres-capture-F05: Reset staging keeps a row-at-a-time membership implementation beside the batched candidate implementation

- Severity: **Medium**. Reset and bootstrap baseline work holds source locks while issuing per-row and per-edge SPI calls.
- Classification: **behavior-preserving cleanup**.
- Problem references:
  - `extensions/synchro-pg/src/stream_reset.rs:2045-2111`
  - `extensions/synchro-pg/src/stream_reset.rs:2133-2181`
  - `extensions/synchro-pg/src/stream_reset.rs:2184-2287`
  - `extensions/synchro-pg/src/stream_reset.rs:2290-2338`
  - `extensions/synchro-pg/src/stream_reset.rs:2570-2773`
- Existing batched implementations:
  - `extensions/synchro-pg/src/bgworker.rs:2490-2659`
  - `extensions/synchro-pg/src/materialize.rs:143-224`
  - `extensions/synchro-pg/src/materialize.rs:893-1042`

**Observed behavior and cost**

`stage_registration_membership` reads all rows, resolves membership once per row, and inserts each edge separately.
The candidate worker already resolves membership by bounded key batches and inserts the same staging-table edges through `jsonb_to_recordset`.
The baseline path therefore maintains a second implementation of the same operation.
Baseline source loaders also collect whole relations into Rust vectors.
Verification then reloads source rows and queries staged rows and memberships individually.

For `N` live rows and `E` edges, membership staging alone executes one membership query per row and one insert per edge.
The required source gate remains held during this work.

**Smallest coherent simplification**

Reuse bounded membership loading and edge insertion for reset staging and candidate catch-up.
Use keyset batches for source rows and capture-dependency rows.
Fetch staged versions and verification counterparts as bounded sets.
Delete the duplicate per-row membership writer and per-edge insertion loop.
Keep final-snapshot verification separate from construction.

**Invariant**

All batches must use one imported snapshot and one atomic staging transaction.
Keep complete key-set, row-version, digest, and membership verification.
Never split one WAL transaction into independently acknowledged units.

**Acceptance and evidence limit**

Compare baseline contents for mixed rows, tombstones, and dependency-driven memberships before and after the change.
Measure SPI calls for multiple baseline sizes and require batch-based growth.
Use `make test-blackbox-configured-bounds` after adding the reset measurement.
Static inspection proves the query loops. No timing or query-count measurement ran.
This matches the supplied issue #54, server query loops.

<a id="03-postgres-capture-f06"></a>

### 03-postgres-capture-F06: Reset and projection bootstrap duplicate the same baseline construction and staging verification

- Severity: **Medium**. Two production paths maintain the same snapshot-building sequence and storage format.
- Classification: **behavior-preserving cleanup**.
- Compared references:
  - `extensions/synchro-pg/src/stream_reset.rs:1036-1115`
  - `extensions/synchro-pg/src/stream_reset.rs:1155-1238`
  - `extensions/synchro-pg/src/bgworker.rs:2743-2888`
  - `extensions/synchro-pg/src/stream_reset.rs:2776-2845`
  - `extensions/synchro-pg/src/stream_reset.rs:2905-2951`

**Observed behavior and cost**

Both baseline functions clear staging, copy versions, stage registrations, prune versions, select projections, stage membership, verify fences, and store digests.
Both then persist the same snapshot markers and counts.
The operation-specific differences concern binding, target generation, candidate progress, and response fields.
Digest verification also exists twice for the same staging tables and the same shared digest calculator.
Candidate integrity verification already differs between worker finalization and activation.
The worker checks captured-to-version equality and registry generation, while the activation variant checks a smaller subset.

**Smallest coherent simplification**

Extract one baseline builder used by the two existing entry points.
Keep operation-specific preconditions and lifecycle updates explicit in those entry points.
Use one staging digest verifier and one complete candidate integrity predicate at their existing verification boundaries.
Delete duplicate sequences and predicates.
Do not merge the distinct authoritative-version installation rules identified in F01.

**Invariant**

Preserve imported-snapshot binding, exact fence coverage, slot checks, and activation-time revalidation.
Shared verification must retain the stronger checks, not the intersection of current checks.

**Acceptance and evidence limit**

Run both operation types with valid staging and with wrong versions, missing edges, wrong generations, and changed digests.
Use `make test-rust-pg` and the applicable real reset cases in `make test-blackbox-wal`.
Static comparison establishes duplicate ownership. It does not prove every caller's transaction behavior after a refactor.
Related contract: ADR 001, lines 93-99 and 127-132. No matching supplied issue was identified.

<a id="03-postgres-capture-f07"></a>

### 03-postgres-capture-F07: The worker copies the complete raw WAL result before decoder limits can protect it

- Severity: **Medium**. Large source transactions can allocate a raw-message batch larger than the decoder's safety budget before rejection.
- Classification: **correctness defect**.
- References:
  - `extensions/synchro-pg/src/bgworker.rs:1898-1919`
  - `extensions/synchro-pg/src/bgworker.rs:3005-3062`
  - `extensions/synchro-pg/src/bgworker.rs:3163-3202`
  - `extensions/synchro-pg/src/wal_decoder.rs:18-19`
  - `extensions/synchro-pg/src/wal_decoder.rs:174-224`
  - `extensions/synchro-pg/src/wal_decoder.rs:914-986`

**Observed behavior and cost**

The decoder now has explicit 16-MiB and 10,000-record limits.
That corrects the claim that its pending transaction has no limit.
However, `peek_messages` first copies every returned `data` value into `Vec<PeekedMessage>`.
Only after that function returns does either caller charge the decoder budget.
The SQL call's numeric batch argument is not a byte limit.
A single oversized message is already copied before `TransactionTooLarge` can reject it.
The worker also retains completed decoded transactions while consuming the raw batch.

The decoder unit tests feed messages directly.
They cannot prove bounded worker ingestion or successful quarantine under input memory pressure.

**Smallest coherent simplification**

Remove the intermediate whole-result raw-message vector.
Enforce the byte budget at the retrieval boundary before converting oversized payloads into owned Rust buffers.
Decode through a bounded ingestion path while preserving complete-transaction materialization.
If larger transactions must be supported, use bounded spillable staging rather than removing the limit.
That support decision requires primary approval.

**Invariant**

Oversized input must fail before partial materialization or acknowledgement.
Quarantine must remain possible without first buffering the rejected payload in full.

**Acceptance and evidence limit**

Use a real source transaction with one oversized value and another with many ordinary values.
Measure peak worker memory and verify durable poison, unchanged acknowledgement, and unchanged later effects.
Use `make test-blackbox-wal` after adding ingestion-boundary coverage.
No allocation measurement ran. The pre-check copy order is established statically.
This is the remaining ingestion-boundary concern associated with the supplied issue #50.

<a id="03-postgres-capture-f08"></a>

### 03-postgres-capture-F08: Candidate acknowledgement lacks the active slot's bounded crash reconciliation

- Severity: **Medium**. A recoverable acknowledgement gap forces candidate abandonment instead of bounded continuation.
- Classification: **correctness defect**.
- Compared references:
  - `extensions/synchro-pg/src/bgworker.rs:1485-1563`
  - `extensions/synchro-pg/src/bgworker.rs:1623-1641`
  - `extensions/synchro-pg/src/bgworker.rs:1769-1810`
  - `extensions/synchro-pg/src/bgworker.rs:2271-2332`
  - `extensions/synchro-pg/src/bgworker.rs:649-658`

**Observed behavior and cost**

Both paths advance a replication slot before recording its durable acknowledgement.
The active startup path explicitly reconciles advancement that remains within durable materialized progress.
The candidate path requires exact equality between slot position and stored acknowledgement before it does any work.
It has no corresponding reconciliation path after worker restart.
A stop after slot advancement, before the acknowledgement update commits, therefore repeats the same boundary error.
Clearing the in-memory decoder does not change either boundary.

Authorized abort and a new candidate remain available.
The finding is unnecessary loss of resumable work, not silent acknowledgement past missing materialization.

**Smallest coherent simplification**

Use one bounded acknowledgement-reconciliation rule for active and candidate recovery.
For candidates, bind the check to the operation, slot, generation, committed transaction ledger, and activation barrier.
Delete the candidate-only equality dead end for a boundary already proven durable.
Keep rejection for backward movement, unknown slot identity, and advancement beyond materialized state.

**Invariant**

Never acknowledge beyond complete durable materialization.
Resume only the same permanent slot and candidate identity.

**Acceptance and evidence limit**

Stop the worker between candidate slot advancement and acknowledgement persistence.
Restart it and require one final candidate result without duplicate events or effects.
Add a negative control with advancement beyond candidate materialization.
Use `make test-blackbox-wal` after adding the crash window.
This report establishes asymmetric recovery code. It does not claim a live crash reproduction.
Related contract: ADR 001, lines 62-68 and 123-125. No matching supplied issue was identified.

<a id="03-postgres-capture-f09"></a>

### 03-postgres-capture-F09: Data-bearing error construction defeats otherwise bounded capture diagnostics

- Severity: **Medium**. Failure paths can place source identifiers or malformed WAL values in PostgreSQL logs.
- Classification: **correctness defect**.
- References:
  - `extensions/synchro-pg/src/bgworker.rs:3659-3665`
  - `extensions/synchro-pg/src/materialize.rs:95-102`
  - `extensions/synchro-pg/src/materialize.rs:946-970`
  - `extensions/synchro-pg/src/bgworker.rs:6046-6078`
  - `docs/src/content/docs/spec/04-invariants.mdx:486-488`

**Observed behavior and cost**

Fence parsing logs the full `serde_json` error instead of a bounded failure class.
Such errors can include an unexpected field name or invalid scalar text from the logical message.
Backfill errors explicitly format `record.record_id`, then propagate that string into `pgrx::error!`.
Primary keys are prohibited telemetry even when an operator initiated the operation.
The file already has a safer bounded classification path for poison failures.

**Smallest coherent simplification**

Keep stable operation and failure-class fields.
Delete raw parser error interpolation and primary-key interpolation from capture and backfill diagnostics.
Retain detailed assertions inside tests without transporting source values into production logs.

**Invariant**

Operators must distinguish the failing stage without exposing source values, keys, or WAL payloads.

**Acceptance and evidence limit**

Use synthetic sentinel values in malformed fence fields and a corrupted backfill row.
Require failure and useful bounded classification without those sentinels in captured logs.
After extending that case, use `make test-blackbox GO_TEST_ARGS='-run TestRealIssue49SecurityOperationalRedaction'`.
Static inspection proves primary-key interpolation. No log-capture test ran.
Related invariant: Operational Redaction. No matching supplied issue was identified.

<a id="03-postgres-capture-f10"></a>

### 03-postgres-capture-F10: Backfill duplicates the canonical row-digest implementation

- Severity: **Low**. Two byte-sensitive implementations must change together when canonical row encoding changes.
- Classification: **behavior-preserving cleanup**.
- Compared references:
  - `extensions/synchro-pg/src/materialize.rs:1045-1065`
  - `extensions/synchro-pg/src/pull.rs:720-751`
  - `extensions/synchro-pg/src/materialize.rs:936-965`

Both functions canonicalize data, create the canonical table and primary key, encode `CanonicalRow`, and call the same row digest.
Backfill correctly caches the schema hash, so calling the uncached SPI wrapper would reintroduce per-row queries.
The duplication is unnecessary because the existing pure helper already accepts that hash.

**Smallest coherent simplification**

Expose the existing schema-hash-taking helper within the crate.
Use it from backfill and delete the duplicate implementation.
Keep the schema-hash cache.

**Invariant**

Digest bytes must remain identical for every portable type, logical field identity, schema hash, and row version.

**Acceptance and evidence limit**

Use `make test-rust-pg` and `make lint-rust-pg` with existing digest and backfill coverage.
Static comparison supports deletion. No test ran.
Related contract: ADR 005 row digest. No matching supplied issue was identified.

<a id="03-postgres-capture-f11"></a>

### 03-postgres-capture-F11: Existing-edge validation joins captured rows but ignores every joined value

- Severity: **Low**. The query carries an unnecessary join and selected fields that suggest checks it does not perform.
- Classification: **behavior-preserving cleanup**.
- References:
  - `extensions/synchro-pg/src/materialize.rs:832-875`
  - `extensions/synchro-pg/src/materialize.rs:1071-1103`
  - `extensions/synchro-pg/src/lib.rs:1124-1137`

The query selects captured identity, deletion state, and checksum through a left join.
The loop reads none of them.
Its actual checks only need edge table name, relation identity, and digest length.
The captured-row key is unique, so removing the join does not change edge multiplicity.
The staged-edge verifier already performs its own explicit captured-row checks.

**Smallest coherent simplification**

Delete the unused selected columns and left join.
Do not invent extra old-edge validation while making this cleanup.
Backfill may need to repair old edges, so that would change its behavior.

**Invariant**

Keep the existing relation-identity checks and the complete replacement-stage verification.

**Acceptance and evidence limit**

Use `make test-rust-pg` for backfill coverage.
An `EXPLAIN` comparison can confirm removal of the unused lookup.
Neither command ran here. Static field-use and uniqueness checks establish behavioral equivalence.
Related issue family: #54 query simplification.

<a id="03-postgres-capture-f12"></a>

### 03-postgres-capture-F12: Backfill reports an acknowledgement LSN as a logical transaction-end position

- Severity: **Low**. The operator response gives a misleading boundary, although this path does not issue a pull cursor.
- Classification: **correctness defect**.
- Compared references:
  - `extensions/synchro-pg/src/materialize.rs:128-139`
  - `extensions/synchro-pg/src/materialize.rs:1261-1282`
  - `extensions/synchro-pg/src/stream_position.rs:170-210`
  - `docs/src/content/docs/architecture/decisions/001-wal-change-stream.mdx:25-39`
  - `docs/src/content/docs/architecture/decisions/001-wal-change-stream.mdx:62-66`

The private backfill loader reads `materialized_end_lsn` and returns it as `transaction_end` with field name `commit_lsn`.
The shared boundary loader correctly reads `materialized_commit_lsn` and validates runtime/progress lineage.
The contract distinguishes logical commit position from slot acknowledgement position.

**Smallest coherent simplification**

Use `load_materialized_boundary` and preserve the existing response shape when encoding its typed position.
Delete the private boundary query.

**Invariant**

Logical transaction-end positions use commit LSNs.
Replication acknowledgement alone uses end LSNs.

**Acceptance and evidence limit**

After a transaction with different commit and end LSNs, require backfill's boundary to equal the shared materialized boundary.
Also test an empty generation and mismatched runtime/progress lineage.
Use `make test-rust-pg` after adding the focused assertion.
Static query comparison establishes the discrepancy. No test ran.
No matching supplied issue was identified.

<a id="03-postgres-capture-f13"></a>

### 03-postgres-capture-F13: One malformed-message assertion runs on an already poisoned decoder

- Severity: **Low**. The second assertion cannot detect a parser that accepts its supposedly rejected tag.
- Classification: **correctness defect** in test proof.
- References:
  - `extensions/synchro-pg/src/wal_decoder.rs:174-186`
  - `extensions/synchro-pg/src/wal_decoder.rs:1118-1143`
  - `extensions/synchro-pg/src/wal_decoder.rs:825-827`

The test first feeds tag `S` to `fresh`, which poisons that decoder.
It then feeds tag `Z` to the same instance.
The second call fails at the poison guard before tag dispatch.
That assertion proves persistent poison, not rejection of `Z`.
The test factory also accepts relation arguments that it ignores, which obscures the actual setup.

**Smallest coherent simplification**

Use a new decoder for each independent unknown-tag case.
Keep persistent-poison behavior as an explicit assertion with that purpose.
Replace the ignored-argument factory with `WalDecoder::new()`.

**Invariant**

Each negative parser case must reach the branch it claims to test.
An already failed decoder must still reject subsequent input.

**Acceptance and evidence limit**

Temporarily accepting `Z` must fail the fresh-decoder case.
Use `make test-rust-pg` after correcting the test.
No mutant or test execution ran here. The early return proves the present assertion cannot exercise tag dispatch.
No matching supplied issue was identified.

<a id="03-postgres-capture-f14"></a>

### 03-postgres-capture-F14: Capture helpers retain no-op control flow and duplicate an existing clone operation

- Severity: **Low**. These paths make reviewers inspect nonexistent failure handling and maintain redundant field-copy code.
- Classification: **behavior-preserving cleanup**.
- References:
  - `extensions/synchro-pg/src/bgworker.rs:1920-1949`
  - `extensions/synchro-pg/src/bgworker.rs:1951-1956`
  - `extensions/synchro-pg/src/bgworker.rs:2078-2080`
  - `extensions/synchro-pg/src/bgworker.rs:3156-3160`

`clone_candidate_bootstrap` only copies fields whose types already implement `Clone`.
`materialize_candidate` captures `commit_lsn` only for an `inspect_err` closure that discards it.
`validate_slot_boundary` maps every error variant to that identical variant.
None of these constructs adds validation, context, logging, or recovery.

**Smallest coherent simplification**

Derive `Clone` for `CandidateBootstrap` and delete its field-by-field copying function.
Delete the unused local, empty error observer, and identity error mapping.
Do not replace them with new wrappers.

**Invariant**

Return the same transaction results and failure variants with unchanged ownership.

**Acceptance and evidence limit**

Use `make lint-rust-pg` and `make test-rust-pg` after deletion.
Static inspection establishes that these operations add no behavior. No build or test ran.
No matching supplied issue was identified.

### Contract requirements that merit a product decision

#### Source transaction limits and recovery

The decoder rejects already committed source transactions above fixed byte or record limits.
The operations guide tells writers to split transactions in `docs/src/content/docs/operations/configuration.mdx:22`.
An operator cannot split a source transaction after it has committed.
The current recovery choices then require repairing the reader or rebuilding from a new baseline.

The primary should decide whether these are supported source-write limits or only implementation safety limits.
Supported limits need enforceable pre-commit rejection where the product promises acceptance.
Larger supported transactions need bounded spillable decoding with the same atomic replay unit.
This review does not approve either option or removal of current safety bounds.

#### Historical capture retention

The assigned compactor deletes changelog effects but does not retire captured projections, WAL ledgers, or fence history.
Schema migration scans all retained current rows and historical captured projections at `materialize.rs:386-418` and `519-568`.
A targeted source search found no literal production deletion for those history tables.
That search does not prove the absence of every dynamic or external maintenance path.

The primary should define a retention horizon for history no longer reachable by effects, replay, accepted fences, rebuilds, or recovery.
An explicit safe frontier could bound migration cost and storage without weakening exact replay.
Deleting history merely because a client acknowledged a scope is not sufficient.
No retention-schema or protocol change is approved here.

#### Baseline final-state proof

The contract requires complete coverage for every accepted fence, including repeated same-row transitions.
It also requires one final source projection at the selected snapshot boundary.
F02 shows why those facts need separate checks.
The primary should make that distinction explicit in the normative reset proof before changing its verifier.
The coherent alternative preserves all fences and proves one final state per identity.
It does not weaken coverage to a timestamp or LSN heuristic.


<a id="area-04-postgres-api"></a>

## PostgreSQL API and schema

<a id="04-postgres-api-f01"></a>

### 04-postgres-api-F01: Connect can commit an unreported generation renewal

- **Severity: High.** A valid expired client can lose its usable generation during scope removal reconciliation.
- **Classification:** correctness defect.
- **Problem:** `extensions/synchro-pg/src/client.rs:258-283`, `733-918`, and `973-1005`.
- **Related validation:** `extensions/synchro-pg/src/client.rs:559-615` and `extensions/synchro-pg/src/pg_tests/schema.rs:2603-2704`.

Connect persists the new generation before it checks known-scope history.
The history check uses the new generation, not the generation that supplied the known scopes.
Renewal records only current assignments in that new generation.

Consider a client with generation 1 and a previously assigned scope that the server has removed.
The expired client correctly presents generation 1 and its old known scope.
Connect writes generation 2, removes old checkpoints, and records only current scope history.
The known-scope check cannot find the removed scope in generation 2.
Connect returns `invalid_request` without returning generation 2.
This return does not raise a PostgreSQL error, so the earlier writes remain eligible to commit.
A retry with generation 1 then fails the generation comparison at lines 757-763.

The same ordering writes client metadata before rejecting a future `scope_set_version`.
The special forged-cursor prevalidation avoids only one instance of this broader ordering problem.

**Smallest simplification:** Separate validation from persistence.
Validate known scopes against the presented prior generation before renewal.
Calculate the proposed state, complete all request rejection checks, and then persist it once.
Reuse validated cursor results instead of adding another prevalidation path for each rejection case.

**Invariant:** Expired clients can reconnect with their actual generation and previously assigned scopes.
A rejected request must not leave an unreported generation transition.

**Acceptance:** Add an expired-client case with a removed known scope and retry the same request.
Assert one successful renewal, authoritative removal, and the returned durable generation.
Also assert unchanged client, history, and checkpoint rows after an invented scope or future assignment version.
Run `make test-rust-pg`.

**Contract:** ADR 004, `004-membership-schema-and-retention.mdx:627-670`.
The continuing-client rule is in `spec/01-wire-protocol.mdx:174-187`.
No supplied issue lead establishes this exact defect.

<a id="04-postgres-api-f02"></a>

### 04-postgres-api-F02: Decimal narrowing bypasses incompatible-schema classification

- **Severity: High.** Connect can authorize a compatible replacement for a schema that cannot represent earlier decimal values.
- **Classification:** correctness defect.
- **Problem:** `extensions/synchro-pg/src/schema.rs:867-947`.
- **Compared implementation:** `extensions/synchro-pg/src/push.rs:1145-1215`.
- **Stored metadata:** `extensions/synchro-pg/src/registry.rs:2852-2879` and `extensions/synchro-pg/src/schema.rs:1097-1108`.

The schema classifier compares field name, portable type, nullability, and writability.
It does not compare decimal precision or scale.
Push compatibility separately checks precision, scale, and integer-digit capacity.

For example, changing `numeric(8,2)` to `numeric(6,2)` leaves the classifier's tested field properties unchanged.
The classifier returns Class 2 unless another change selects a stronger class.
Push correctly finds the same narrowing incompatible.
This is two implementations of field compatibility with different meanings.

**Smallest simplification:** Give decimal representability one deterministic predicate with both callers.
Use that predicate when classifying schema changes and evaluating authored mutations.
Remove the independent partial comparison.
Keep naming, lifecycle, and composition rules at their applicable transition level.

**Invariant:** A compatible transition must preserve representability of earlier accepted field values.

**Acceptance:** Add authored transitions for precision reduction, scale reduction, and reduced integer capacity.
Assert incompatible lineage and the matching authored-mutation result.
Include an allowed widening control.
Run `make test-rust-core` if the predicate moves into core, then `make test-rust-pg`.

**Contract:** `docs/src/content/docs/spec/05-schema-evolution.mdx:483-492` and `561-577`.
No supplied issue lead establishes this exact defect.

<a id="04-postgres-api-f03"></a>

### 04-postgres-api-F03: Seed receipt validation omits the retention floor

- **Severity: High.** An old, correctly signed seed can repeatedly fail first connect instead of requesting rebuild.
- **Classification:** correctness defect.
- **Problem:** `extensions/synchro-pg/src/portable_seed.rs:1269-1341`.
- **Failure path:** `extensions/synchro-pg/src/client.rs:238-262`, `303-315`, and `391-409`.
- **Compared validation:** `extensions/synchro-pg/src/cursor_token.rs:96-109` and `157-165`.

Receipt validation checks stream, registry, membership, retention generation, and the upper materialized boundary.
It does not load or compare the scope's current floor.
Ordinary compaction moves the floor without changing `retention_generation`.
Such a receipt passes validation and reaches `issue_scope_cursor` below the floor.
Cursor issuance returns an error, which connect converts into `pgrx::error!`.
The request does not reach the documented stale-receipt rebuild result.

**Smallest simplification:** Include floor columns in the existing receipt-state query.
Validate the receipt position before adding it to the accepted position map.
Use the existing stale-receipt result for a below-floor receipt.
Do not add an exception or retry path around cursor issuance.

**Invariant:** A receipt never creates a cursor below the durable floor.
Stale seed continuation falls back to normal rebuild.

**Acceptance:** Export a valid seed, advance a scope floor beyond its boundary, and retain the same retention generation.
First connect must return a null cursor and rebuild requirement, not a SQL error.
Keep a receipt exactly at the floor as a positive control.
Run `make test-rust-pg` with the regression in the portable-seed proof home.

**Contract:** `docs/src/content/docs/architecture/decisions/004-membership-schema-and-retention.mdx:600-610`.
Stale-receipt behavior appears in `docs/src/content/docs/spec/01-wire-protocol.mdx:353`.
No supplied issue lead establishes this exact defect.

<a id="04-postgres-api-f04"></a>

### 04-postgres-api-F04: Nullable byte fields pass validation and then abort push

- **Severity: Medium.** A legal null byte value becomes an operational failure for the entire batch.
- **Classification:** correctness defect.
- **Problem:** `extensions/synchro-pg/src/push.rs:2258-2295`.
- **Validation path:** `extensions/synchro-pg/src/push.rs:1614-1629`, `1632-1675`, and `1990-2002`.
- **Authoritative null behavior:** `extensions/synchro-core/src/checksum.rs:560-572`.

The core accepts JSON null for every nullable portable type.
`sql_wire_value` handles null specially for `json`, but always calls `as_str()` for `bytes`.
A nullable `bytea` field set to null therefore reaches `pgrx::error!` before DML.

**Smallest simplification:** Return null before portable-type-specific conversion.
Remove the JSON-only null branch.

**Invariant:** Null remains distinct from an absent patch member and from an empty byte string.

**Acceptance:** Push insert and update operations with a nullable byte field set to null.
Include absent, empty, and nonempty byte values, plus a non-nullable null rejection.
A mixed batch must not roll back a valid mutation because another legal byte value is null.
Run `make test-rust-pg`.

**Contract:** Shared core portable-value encoding and ADR 002 first-execution transaction semantics.
No supplied issue lead establishes this exact defect.

<a id="04-postgres-api-f05"></a>

### 04-postgres-api-F05: Pending registration bypasses unrelated drift checks

- **Severity: High.** Pending configuration can suppress capture-trigger, publication, type, function, and ownership validation.
- **Classification:** correctness defect.
- **Problem:** `extensions/synchro-pg/src/registry.rs:4826-4884`.
- **Checks bypassed:** `extensions/synchro-pg/src/registry.rs:4595-4782`, `4887-5010`, and `5013-5024`.
- **Existing safer separation:** `extensions/synchro-pg/src/registry.rs:3784-3792` and `3830-3893`.

`validate_loaded_registration` returns success when a pending registration has the same set of column names as the live relation.
That predicate does not validate the pending registration against the live catalog.
It does not compare types, nullability, function definitions, trigger state, publication membership, or ownership.
The comment claims an exact staged-shape match, but the implementation checks only names.

A pending validated registration followed by a disabled fence trigger still satisfies this predicate.
The same bypass applies when loading that pending registration through the final-generation path if a later pending generation exists.

**Smallest simplification:** Remove the broad early-success path.
Use explicit historical-metadata loading when a newer generation owns the live shape.
Validate the selected live generation completely, then validate only persisted historical metadata for the older generation.
The activation loader already expresses this distinction.

**Invariant:** Intentional schema staging must not disable unrelated drift detection.

**Acceptance:** Stage a valid change, then separately disable a required trigger, remove publication membership, and change a field type.
Each case must fail closed before serving or accepting new work.
The unchanged staged transition must remain usable through its approved activation path.
Run `make test-rust-pg` and the existing real registry-transition scenario through its Make entry point.

**Contract:** Qualified Registered Relation Identity, Registered Key And Replica Identity, and Capture Readiness invariants.
The source comment references issue `#43`.
This report does not claim that the issue already records this bypass.

<a id="04-postgres-api-f06"></a>

### 04-postgres-api-F06: Trigger validation searches deparsed SQL instead of comparing arguments

- **Severity: High.** Incorrect fence identities can pass the required-trigger check.
- **Classification:** correctness defect.
- **Problem:** `extensions/synchro-pg/src/registry.rs:4897-5010`.
- **Second implementation:** `extensions/synchro-pg/src/registry.rs:3360-3434`.
- **Argument consumer:** `extensions/synchro-pg/src/lib.rs:1729-1782` and `1804-1842`.

The runtime check searches `pg_get_triggerdef` for expected strings.
The retirement check uses SQL `LIKE` searches over the same deparsed definition.
Neither compares the ordered argument array.
Swapping the relation UUID and table UUID preserves every tested substring while changing fence meaning.
The checks also depend on SQL quoting and wildcard behavior rather than catalog argument identity.

A static predicate reproduction returned:

```text
Swapped relation/table arguments satisfy runtime substring checks: True
Swapped arguments preserve required ordered identity: False
```

This reproduction evaluates the shown string predicates, not PostgreSQL execution.

**Smallest simplification:** Read and compare exact trigger metadata and decoded ordered `tgargs`.
Use one comparison for installed and retired capture configurations.
Delete both deparsed-SQL matching implementations.
Include function identity, event mask, enabled state, argument order, and absence of an unexpected condition.

**Invariant:** Every required trigger must invoke the exact capture operation with the exact registered identity.

**Acceptance:** Add negative controls for swapped UUID arguments, a different capture key containing the expected substring, and an added trigger condition.
Include quoted identifiers as a valid control.
Run `make test-rust-pg`.

**Contract:** One To One Version Fence Correlation, `docs/src/content/docs/spec/04-invariants.mdx:194-196`.
No supplied issue lead establishes this exact defect.

<a id="04-postgres-api-f07"></a>

### 04-postgres-api-F07: The deterministic-function validator permits nondeterministic built-ins

- **Severity: High.** The same projected row state can produce different scope membership without a source change.
- **Classification:** correctness defect.
- **Problem:** `extensions/synchro-pg/src/registry.rs:1380-1457`, `1460-1532`, and `1535-1584`.

The validator checks the outer function's `STABLE` declaration.
Its dependency query exempts all functions in `pg_catalog`.
It also exempts catalog and information-schema relations from the projection-only relation rule.
There is no check here for random values, time, session settings, or mutable catalog reads.

A SQL function can declare itself `STABLE` while its body calls `pg_catalog.random()` or `pg_catalog.current_setting()`.
The displayed checks do not reject that body.
For example, a body that returns a scope selected by `random()` can always return one valid scope within the fanout limit.
The declaration and bound do not prove deterministic membership.

**Smallest coherent correction:** Replace blanket namespace exemptions with validation of the analyzed function expression and permitted dependencies.
Use the same validator for registration and drift checks.
Reject context-dependent operations rather than adding runtime retries or comparing a few sample executions.
The primary must select the exact supported expression policy.
This is also a contract-design boundary.
A `STABLE` declaration and an expanding text blacklist do not prove arbitrary SQL deterministic.
Define a supported deterministic subset or an explicit trusted-operator obligation before expanding the validator.
Neither alternative is approved by this review.

**Invariant:** Equal captured row and dependency state produces the same normalized scope set.

**Acceptance:** Attempt to register parsed SQL functions that use `random()`, wall-clock time, session settings, and mutable catalog state.
Each must fail registration before activation.
A projection-only deterministic function must pass.
Run `make test-rust-pg` with fixtures that do not use the test-schema bypass in F09.
Static review did not execute these SQL counterexamples.

**Contract:** ADR 004 function restrictions, `004-membership-schema-and-retention.mdx:241-257`.
No supplied issue lead establishes this exact defect.

<a id="04-postgres-api-f08"></a>

### 04-postgres-api-F08: Mutation-ledger immutability contains an unused deletion exception

- **Severity: Medium.** The database guard permits deletion outside the only approved retention event.
- **Classification:** correctness defect.
- **Problem:** `extensions/synchro-pg/src/lib.rs:591-617`.
- **Generated copy:** `extensions/synchro-pg/sql/synchro_pg--0.3.0.sql:563-589`.
- **Actual setting consumer:** `extensions/synchro-pg/src/materialize.rs:1198-1245`.

The mutation-ledger trigger allows deletion when a transaction setting names a pending membership stage.
It does not require client retirement or relate the deleted mutation to that stage.
The only production writer of that setting uses it to delete rebuild pages, staged rows, and sessions.
Repository searches found no production membership-activation deletion of mutation ledgers.

**Smallest simplification:** Delete the membership-stage exception from the mutation-ledger trigger.
Keep the justified exceptions on rebuild state.
Regenerate the extension SQL.

**Invariant:** Only irreversible scoped-client retirement makes mutation ledgers eligible for deletion.

**Acceptance:** Extend the ledger-immutability test with a pending membership stage and the matching transaction setting.
Deletion must still fail without the retirement marker.
Retirement deletion must still pass.
Run `make test-rust-pg` and `make check-pg-sql`.

**Contract:** ADR 002, `002-mutation-idempotency-and-conflicts.mdx:196-202`.
This is not a demonstrated adapter privilege escalation.
The concern is an unnecessary privileged invariant bypass.

<a id="04-postgres-api-f09"></a>

### 04-postgres-api-F09: Legacy fixtures require three test-only production bypasses

- **Severity: High.** Integration tests can exercise a different membership contract from the shipped extension.
- **Classification:** correctness defect in verification.
- **Problem:** `extensions/synchro-pg/src/registry.rs:1441-1451`, `1539-1542`, and `4024-4027`.
- **Fixture implementation:** `extensions/synchro-pg/src/lib.rs:2329-2505` and `2633-2666`.
- **Consumers:** `extensions/synchro-pg/src/pg_tests/schema.rs:2604-2621` and `extensions/synchro-pg/src/pg_tests/membership.rs:1108-1123`.

The `pg_test` build exempts functions in schema `tests` from parsed-body and fixed-search-path checks.
It also skips dependency validation and generation-level projection validation for those functions.
`register_legacy_test_table` generates string-body functions that read live application tables.
The helper then installs permissive RLS and access configuration to preserve that legacy test path.

These branches do not merely prepare input data.
They change the production validator and allow behavior that ADR 004 forbids.
The source carries a compatibility adapter because the fixtures do not use the current public contract.

**Smallest simplification:** Replace the legacy membership fixture definitions with parsed SQL over prepared projection views.
Delete the three production bypasses and the legacy SQL registration adapter.
Share only ordinary fixture setup that follows the shipped registration path.

**Invariant:** Tests and production reject the same unsupported scope functions.

**Acceptance:** The same invalid function must fail in the test build regardless of its schema name.
Run `make test-rust-pg`, `make lint-rust-pg`, and `make check-pg-sql` after removing the bypasses.
Do not restore failing fixtures with another production exception.

**Contract:** Deterministic Scope Function Contract and Dependency Driven Membership Propagation invariants.
The supplied `#36` lead concerns oracle replacement, not proof that these exact bypasses are tracked.

<a id="04-postgres-api-f10"></a>

### 04-postgres-api-F10: Push outcome tests derive their expected row from production hydration

- **Severity: Medium.** A shared projection defect can produce matching actual and expected rows.
- **Classification:** correctness defect in verification.
- **Problem:** `extensions/synchro-pg/src/lib.rs:3101-3127`.
- **Compared production paths:** `extensions/synchro-pg/src/pull.rs:1244-1354` and `extensions/synchro-pg/src/push.rs:2172-2199`.
- **Actual assertions:** `extensions/synchro-pg/src/pg_tests/conflicts.rs:195-297`.

`assert_row_outcome_matches_source` gets its expected row, checksum, and version from `pull::hydrate_records`.
The push path and hydration both use `synced_row_projection_sql` and `canonicalize_synced_row_data`.
A shared physical-to-wire projection error can therefore pass the comparison.
The test's authored title assertions help, but they do not independently validate the complete row projection.

**Smallest simplification:** Replace this expected-value helper with authored complete wire values and independent source-state assertions.
Use the authoritative checksum vectors for checksum semantics.
Retain the useful fence and CAS assertions.
Delete `source_wire_record` if no non-oracle consumer remains.

**Invariant:** Production serialization must not define its own expected result.

**Acceptance:** Demonstrate a mutant in a non-title projection expression that the authoritative outcome proof rejects.
Run `make test-rust-pg` and the relevant existing mutation gate through Make.
The primary must select the existing mutant identifier or add one at the proof home.

**Issue match:** The verified pattern matches the supplied oracle-replacement lead `#36`.
This is distinct from the test-only validator bypass in F09.

<a id="04-postgres-api-f11"></a>

### 04-postgres-api-F11: Portable seed pagination repeatedly loads and hashes the complete scope

- **Severity: Medium.** Small page limits multiply full-scope work and do not bound backend memory.
- **Classification:** behavior-preserving cleanup.
- **Problem:** `extensions/synchro-pg/src/portable_seed.rs:367-375`, `407-464`, and `619-652`.
- **Full scan and sort:** `extensions/synchro-pg/src/portable_seed.rs:840-1040` and `1043-1056`.
- **Snapshot guarantee:** `extensions/synchro-pg/src/portable_seed.rs:695-713`.

Manifest creation retains every row for every scope in `scope_rows`, although later code needs only counts and digests.
Every page then loads all scope rows, clones table metadata per row, sorts all rows, and recomputes the complete scope digest.
Only after this work does it slice the requested page.

For a scope with N rows and page limit L, the row validation work is proportional to `N * ceil(N/L)`.
That cost follows directly from the loops and has not been benchmarked here.
The required serializable read-only transaction already fixes the export snapshot.

**Smallest coherent simplification:** Retain only scope summaries after manifest validation.
Read later pages from the same snapshot with a bounded, protocol-correct row ordering query.
Keep row verification for delivered rows and complete export verification at the existing manifest boundary.
If typed ordering needs database support, implement that support once rather than silently using lexical record order.
Do not introduce a second export transaction or a permanent alternate seed store.

**Invariant:** Export provenance, canonical order, row digests, complete scope digest, and one snapshot remain unchanged.

**Acceptance:** Export the same fixture with several page limits and verify identical ordered rows and final scope digests.
Measure rows processed and peak memory for increasing N.
Pagination must not repeat a full-scope scan for every page.
Use the portable-seed integration proof through `make test-rust-pg` and the existing seed-generator Make test entry point.
The primary must execute the performance measurement.

**Contract:** `docs/src/content/docs/architecture/portable-seeds.mdx:16-24`.
This extends the query-work concern in supplied issue `#54`, but the exact issue scope was not fetched.

<a id="04-postgres-api-f12"></a>

### 04-postgres-api-F12: Loading the latest manifest reads and validates every historical manifest

- **Severity: Medium.** Normal requests perform work proportional to permanently retained schema history.
- **Classification:** behavior-preserving cleanup.
- **Problem:** `extensions/synchro-pg/src/schema.rs:714-817`.
- **Repeated callers:** `extensions/synchro-pg/src/client.rs:187-216` and `extensions/synchro-pg/src/push.rs:343-362`.
- **Partial optimization and test:** `extensions/synchro-pg/src/push.rs:969-1029` and `extensions/synchro-pg/src/pg_tests/push_idempotency.rs:179-225`.

`load_latest_manifest` calls `load_manifest_history` and discards every row except the first.
Each discarded row was already decoded, canonicalized, and hashed.
Connect calls the latest-loader through ensure and then calls it again directly.
Historical lineage resolution adds another full read.
Push also loads full history before its separately bounded authored-manifest query.

The focused test proves only that `load_authored_manifests` returns one selected record.
It does not exercise the complete push path, which still loads all history.

**Smallest simplification:** Select the latest row with `ORDER BY schema_version DESC LIMIT 1`.
Use one row parser for latest and historical queries.
Pass the already loaded current manifest through the request.
Read lineage metadata only when historical dispatch needs it.
Keep validation for every manifest actually consumed by the operation.

**Invariant:** Historical manifests remain immutable and exact authored references remain verifiable.

**Acceptance:** Add many valid historical manifests and execute the complete connect and push entry points.
Measure selected manifest rows, not just returned map size.
Current-schema operations must not read every historical body.
Run `make test-rust-pg`.

**Issue match:** This is current source evidence for the repeated-query concern in supplied issue `#54`.
The supplied Swift-specific issue `#95` is not a match for this server implementation.

<a id="04-postgres-api-f13"></a>

### 04-postgres-api-F13: Rebuild reloads the same schema hash for each staged row

- **Severity: Medium.** A rebuild issues one immutable-schema query per row while holding the materialization boundary lock.
- **Classification:** behavior-preserving cleanup.
- **Problem:** `extensions/synchro-pg/src/rebuild.rs:542-547`, `580-673`, and `709-721`.
- **Repeated query:** `extensions/synchro-pg/src/pull.rs:658-690` and `720-750`.
- **Existing batch-local approach:** `extensions/synchro-pg/src/pull.rs:1078-1081` and `1135-1150`.

`stage_records` already receives the validated schema hash.
Its row loop calls `synced_row_digest`, which queries the schema hash again for every row.
The scope digest later parses the original hash separately.
No row-specific value affects this lookup for the staged generation.

**Smallest simplification:** Parse the supplied hash once before the loop.
Call the existing explicit-hash digest implementation for each row.
Delete the per-row SPI lookup from this caller.

**Invariant:** Every row and the final scope digest use the exact session schema.

**Acceptance:** Stage a multirow scope and count schema-hash queries.
The count must remain constant as row count grows.
Retain the corrupt-row digest negative control.
Run `make test-rust-pg`.

**Issue match:** This directly matches supplied issue `#54`, server query loops.

<a id="04-postgres-api-f14"></a>

### 04-postgres-api-F14: Per-batch and per-mutation locks add no exclusion beyond the client lock

- **Severity: Medium.** Push adds a sorted allocation and one SPI lock call per mutation without allowing additional concurrency.
- **Classification:** behavior-preserving cleanup.
- **Problem:** `extensions/synchro-pg/src/push.rs:194-230` and `539-546`.
- **Same pattern:** `extensions/synchro-pg/src/rebuild.rs:120-124` and `287-301`.
- **Outer lock:** `extensions/synchro-pg/src/client.rs:543-556`.
- **Identity contract:** `docs/src/content/docs/architecture/decisions/002-mutation-idempotency-and-conflicts.mdx:35-49`.

Every push first takes the exclusive transaction lock for `(user_id, client_id)`.
Every subsequent batch and mutation identity includes that same pair.
The client lock already serializes all competing push calls that could share those ledger identities.
The rebuild-specific identity lock is likewise acquired after the same client lock.
The in-scope production ledger writes occur only within the locked push entry point.

**Smallest simplification:** Keep the client lock and remove the redundant identity lock loops and helpers.
Keep database unique constraints.
Do not remove the client lock as part of this cleanup.
Finer client concurrency would require a separate concurrency design.

**Invariant:** Equal identities execute once, replay exactly, and serialize with generation renewal.

**Acceptance:** Run concurrent same-batch, reused-mutation, different-batch, and connect-renewal tests.
Measure a constant number of advisory lock calls per push regardless of mutation count.
Run `make test-rust-pg` and the existing real concurrent push proof through Make.

No supplied issue lead establishes this exact redundant-lock implementation.
The supplied `#94` title is not enough to assert a match.

<a id="04-postgres-api-f15"></a>

### 04-postgres-api-F15: Removing a shared scope also removes an independent user grant

- **Severity: Medium.** The server emits a false scope removal and advances the assignment version for an unchanged authoritative set.
- **Classification:** correctness defect.
- **Problem:** `extensions/synchro-pg/src/portable_seed.rs:287-324`.
- **Allowed overlapping grant:** `extensions/synchro-pg/src/portable_seed.rs:212-239` and `264-269`.
- **Authoritative assignment calculation:** `extensions/synchro-pg/src/client.rs:676-730`.

The grant API permits a user grant for a shared scope ID.
Connect treats shared and granted scopes as a union.
Unregistering the shared declaration removes that scope from every active client's stored set without checking remaining user grants.
A later connect adds the scope back from the grant.
This produces unnecessary history transitions and can force avoidable client cleanup and rebuild.

**Smallest simplification:** Apply shared-scope removal only when no other authoritative assignment remains for that user.
Keep one assignment-set meaning between administration and connect.
Delete the false remove-and-add cycle rather than compensating in clients.

**Invariant:** `scope_set_version` advances only when the authoritative assigned set changes.

**Acceptance:** Give a user both a shared declaration and a direct grant for the same scope.
Remove the shared declaration and assert unchanged assignment, version, and usable cursor for that user.
Another user without the grant must receive removal.
Run `make test-rust-pg`.

**Contract:** `docs/src/content/docs/spec/04-invariants.mdx:48-52`.
No supplied issue lead establishes this exact defect.

<a id="04-postgres-api-f16"></a>

### 04-postgres-api-F16: Registry metadata validation has two independently maintained copies

- **Severity: Medium.** Historical and live loading can disagree about the validity of the same persisted registration.
- **Classification:** behavior-preserving cleanup.
- **First copy:** `extensions/synchro-pg/src/registry.rs:3897-3986`.
- **Second copy:** `extensions/synchro-pg/src/registry.rs:4595-4615`, `4661-4713`, and `4752-4766`.
- **Consumers:** `extensions/synchro-pg/src/registry.rs:3873-3886` and `4826-4839`.

Both functions independently check required IDs, capture-only shape, primary-key identity, portable type, column coverage, lifecycle fields, and logical-ID kinds.
The difference is that live validation also checks physical catalog state.
Duplicating the common part does not provide independent proof because both are production acceptance paths.

**Smallest simplification:** Run `validate_persisted_registration_metadata` once at the start of live metadata validation.
Keep only live catalog checks in the latter function.
Delete the duplicate branches, sets, and logical-ID queries.
Keep the historical-versus-live distinction needed by staged activation.

**Invariant:** Historical loading validates immutable metadata without requiring an obsolete physical shape.
Live loading additionally detects current drift.

**Acceptance:** Exercise synced and capture-only registrations through both loading modes.
Malformed persisted metadata must fail both modes.
Intentional historical physical differences must fail only the applicable live check.
Run `make test-rust-pg` and `make lint-rust-pg`.

No supplied issue lead establishes this exact duplication.

<a id="04-postgres-api-f17"></a>

### 04-postgres-api-F17: Seed and rebuild duplicate snapshot validation and already disagree

- **Severity: Medium.** Seed export accepts a missing edge version that rebuild correctly rejects.
- **Classification:** correctness defect with a shared-validation cleanup.
- **Seed path:** `extensions/synchro-pg/src/portable_seed.rs:840-1040`, especially `965-978`.
- **Rebuild path:** `extensions/synchro-pg/src/rebuild.rs:542-721`, especially `607-619`.
- **Nullable storage:** `extensions/synchro-pg/src/lib.rs:687-696`.

Both paths load the same edge, captured-row, and reset-provenance relations.
Both validate schema generation, stream boundary, deletion state, checksum, row identity, and server version.
Seed export treats a null edge version as acceptable and compares only a present version.
Rebuild requires a nonempty edge version and exact equality.
The database column permits null, so the difference is reachable in corrupted internal state.

**Smallest simplification:** Require matching edge and captured versions in both paths.
Extract only the common captured-row and provenance validation used by seed and rebuild.
Keep export transaction state, rebuild persistence, and paging separate.
This should delete the duplicated row-validation branches without creating a general snapshot framework.

**Invariant:** An exported or staged row must have complete matching provenance and a verified canonical digest.

**Acceptance:** Clear an edge version while preserving its captured row and checksum.
Seed manifest creation and rebuild must both fail closed.
Retain valid WAL and activated-reset provenance cases.
Run `make test-rust-pg`.

**Contract:** Portable seed row/version integrity and Explicit Pull Candidate Outcomes.
No supplied issue lead establishes this exact inconsistency.

<a id="04-postgres-api-f18"></a>

### 04-postgres-api-F18: Three token modules duplicate the same database key-selection policy

- **Severity: Low.** Key rotation changes require edits to three implementations of one policy.
- **Classification:** behavior-preserving cleanup.
- **Incremental implementation:** `extensions/synchro-pg/src/cursor_token.rs:91-94` and `234-278`.
- **Rebuild implementation:** `extensions/synchro-pg/src/rebuild_token.rs:176-179` and `186-231`.
- **Seed implementation:** `extensions/synchro-pg/src/portable_seed.rs:131-135` and `1428-1465`.

Each loader reads `sync_token_keys`, selects one purpose, signs with an active key, and verifies with active or verify-only keys.
Each separately enforces one result, nonempty key ID, and minimum secret length.
The incremental implementation also constructs SQL predicates that the other two express as fixed queries.

**Smallest simplification:** Use one internal purpose-aware key loader with these three concrete consumers.
Delete the three local key structs and duplicate selection implementations.
Do not unify token payloads or signing envelopes.
Those have different contracts.

**Invariant:** Purpose isolation, active-only signing, verify-only acceptance, retired-key rejection, and constant-time MAC verification remain unchanged.

**Acceptance:** Exercise active, verify-only, retired, missing, and wrong-purpose keys for each token family.
Run `make test-rust-pg` and `make lint-rust-pg`.

No supplied issue lead establishes this exact duplication.

<a id="04-postgres-api-f19"></a>

### 04-postgres-api-F19: Unused legacy state and an unreachable lookup branch remain installed

- **Severity: Low.** Dead compatibility remnants add schema, diagnostics, and control-flow ownership without production behavior.
- **Classification:** behavior-preserving cleanup.
- **Unused secret:** `extensions/synchro-pg/src/lib.rs:41-61` and generated SQL `13-33`.
- **Unused table:** `extensions/synchro-pg/src/lib.rs:762-771` and generated SQL `734-743`.
- **Diagnostic reference:** `conformance/blackbox/process.go:79-89`.
- **Unreachable branch:** `extensions/synchro-pg/src/registry.rs:1726-1792`.

Repository searches found `cursor_secret` only in bootstrap source and generated SQL.
All cursor signing uses `sync_token_keys`.
`sync_rule_failures` has no production writer or semantic reader.
Its only additional reference is a legacy diagnostic table list.

The only caller of `load_physical_relation_candidates` always supplies `Some(schema)`.
The helper still contains an unqualified search branch, argument alternatives, and ambiguous-candidate handling for that unreachable mode.

**Smallest simplification:** Remove the obsolete secret and unused failure table from the next approved installation schema.
Remove the stale diagnostic entry with its owning scope.
Make physical lookup explicitly schema-qualified and delete its `None` branch.
Do not drop objects from an existing deployment without the approved migration procedure.

**Invariant:** Token key rotation remains authoritative, and all physical relation lookup remains schema-qualified.

**Acceptance:** Repeat exact consumer searches after removal.
Run `make lint-rust-pg`, `make test-rust-pg`, and `make check-pg-sql`.
The primary must verify the diagnostic consumer when integrating the cross-scope deletion.

No supplied issue lead establishes these exact remnants.

<a id="04-postgres-api-f20"></a>

### 04-postgres-api-F20: Rebuild integrity logging includes application primary-key values

- **Severity: Medium.** Corrupt-row handling can put user-owned identifiers into server logs.
- **Classification:** correctness defect.
- **Problem:** `extensions/synchro-pg/src/rebuild.rs:673-684`.
- **Handling path:** `extensions/synchro-pg/src/rebuild.rs:256-265` and `1080-1082`.

The checksum mismatch error contains `record_id` and physical table details.
The handler logs the entire error with `pgrx::warning!`.
Registered string primary keys are application-owned values, not safe diagnostic categories.
This code logs those values precisely when integrity checking fails.

**Smallest simplification:** Return and log a bounded integrity failure category without row values.
Remove formatted record IDs and digest payload details from this error path.
Keep operation context and the public generic response.

**Invariant:** Integrity failures remain visible without recording user-owned data.

**Acceptance:** Corrupt a row using a recognizable synthetic string primary key.
Assert the integrity response and absence of that marker from captured logs.
Run the real rebuild regression through its Make entry point, then `make test-rust-pg`.

**Requirement:** Repository Errors And Security and Observability rules prohibit user-owned values in logs.
No supplied issue lead establishes this exact defect.

<a id="04-postgres-api-f21"></a>

### 04-postgres-api-F21: The rebuild token contract duplicates immutable session state

- **Severity: Low.** The contract requires repeated payload fields, copies, and comparison logic without removing the session lookup.
- **Classification:** contract decision.
- **Normative requirement:** `docs/src/content/docs/architecture/decisions/003-pull-cursor-and-rebuild.mdx:502-533`.
- **Payload and copied input:** `extensions/synchro-pg/src/rebuild_token.rs:16-81`.
- **Durable authority:** `extensions/synchro-pg/src/rebuild.rs:64-82`, `165-205`, and `492-540`.
- **Issuance copy:** `extensions/synchro-pg/src/rebuild.rs:960-978`.

The token repeats user, client, generation, scope, schema, membership, retention, boundary, page limit, write epoch, and expiry.
Every continuation still loads the immutable session and compares these duplicated fields.
The input struct repeats nearly the complete token struct solely to construct it.

**Product decision to consider:** Permit a versioned, purpose-separated token containing key ID, immutable session ID, next ordinal, and MAC.
Resolve the other bindings from the immutable session and validate the request and current state exactly as today.
Keep the client-supplied rebuild UUID in the stored session and request comparison.
This can remove most payload copying and `continuation_matches_session` field comparisons.

This alternative is not approved.
The primary must decide whether self-contained diagnostic bindings justify their continued contract cost.
Existing tokens and stored replay pages require an explicit compatibility strategy before any format change.

**Invariant:** No request can use another identity's session, change page identity, extend expiry, or bypass generation and write-epoch invalidation.

**Acceptance:** If approved, run wrong-user, wrong-client, wrong-scope, expiry, epoch-change, missing-session, tampered-ordinal, and exact-page-replay cases.
Run `make verify-contract`, `make test-rust-pg`, and the existing real rebuild continuation proof through Make.
No contract change is needed for the other behavior-preserving findings.


<a id="area-05-postgres-tests"></a>

## PostgreSQL tests and relational fixtures

<a id="05-postgres-tests-f01"></a>

### 05-postgres-tests-F01: Legacy membership fixtures require production validation exemptions

- **Severity:** Medium. These fixtures add a second registration model and can hide production registration failures.
- **Classification:** Correctness defect in test architecture.
- **Problem references:**
  - `extensions/synchro-pg/src/pg_tests/schema.rs:412-421`, `462-471`, `3198-3207`.
  - `extensions/synchro-pg/src/pg_tests/conflicts.rs:759-767`, `837-845`.
  - `extensions/synchro-pg/src/pg_tests/membership.rs:1109-1135`.
- **Dependency references:**
  - `extensions/synchro-pg/src/lib.rs:2331-2505`, `2633-2666`.
  - `extensions/synchro-pg/src/registry.rs:1441-1456`, `1535-1542`, `4020-4027`.
- **Production-valid comparison:**
  - `extensions/synchro-pg/src/pg_tests/membership.rs:40-102`, `183-211`.
  - `extensions/testdata/register.sql:10-97`.

The legacy helper wraps caller SQL in a dynamically created membership function.
Its functions read live application tables and use `search_path = pg_catalog, public`.
Three `pg_test` branches exempt the `tests` schema from production restrictions.
They bypass parsed-body checks, projection dependency checks, and generation dependency validation.

This is not necessary fixture syntax duplication.
The tests require behavior that the production validator explicitly refuses.
It also conflicts with the commit-ordered projection contract.

The smallest coherent replacement uses registered projection functions for the shared test tables.
Existing projection fixtures show the required pattern.
Reconfiguration tests should change those valid declarations rather than submit legacy bucket SQL.
This can delete the compatibility helper and its three validation exemptions after all consumers migrate.
The primary must coordinate helper and production changes with their owning scope.

**Invariant:** Tests and production must enforce the same membership restrictions.
Keep intentional corrupt-state tests separate from valid registration setup.

**Acceptance:** Run `make lint-rust-pg` and `make test-rust-pg` after removing the exemptions.
A membership function that reads a live table must fail under `pg_test` too.
No runtime mutation was executed during this review.

**Contract:** `docs/src/content/docs/spec/04-invariants.mdx:208-214` and ADR 004, lines 241-257.
No exact match to a supplied open issue was established.

<a id="05-postgres-tests-f02"></a>

### 05-postgres-tests-F02: Two rebuild tests do not construct the state they claim to test

- **Severity:** Medium. Both tests can pass without protecting their named rebuild behavior.
- **Classification:** Correctness defect in tests.
- **Problem references:** `extensions/synchro-pg/src/pg_tests/rebuild.rs:75-108`, `229-248`.
- **Comparison references:**
  - `extensions/synchro-pg/src/pg_tests/rebuild.rs:15-72`.
  - `extensions/synchro-pg/src/rebuild.rs:542-625`.
  - `extensions/synchro-pg/src/lib.rs:3234-3269`, `3357-3410`, `3460-3501`, `3545-3554`.

The missing-version test creates a source row and an edge but no captured row.
It then deletes `sync_row_versions` and expects an integrity error.
The rebuild reader already fails because the captured projection is absent.
Its reader uses captured and edge versions, not the deleted source-version row.
The deletion therefore does not isolate the named failure.

The soft-delete test creates only a live-table tombstone.
The worker is disabled, and the test creates neither an edge nor a captured projection.
An empty rebuild satisfies the assertion.
The predicate also reads `record["pk"]["id"]`, although wire primary keys use logical field IDs.
Even an incorrectly returned tombstone with a valid logical key would not match that predicate.

Replace these obsolete live-row assumptions with one explicitly materialized live-to-delete flow.
Assert that the live row is visible before deletion and absent after committed materialization.
Use the logical primary-key field ID.
For malformed captured state, keep a separate focused integrity check with exactly one changed precondition.
Do not preserve a source-version test name if the contract only requires captured-version validation at this boundary.

**Invariant:** Rebuild must deliver the complete live captured scope and reject inconsistent captured state.
It must not read later live application rows.

**Acceptance:** Run `make test-rust-pg` after correction.
Demonstrate that the live control fails when the reader returns no records.
Demonstrate that the deletion control fails when the deleted record remains visible.
For the integrity case, prove the baseline succeeds before corrupting its required version or projection.

**Contract:** Wire protocol lines 88-94 and ADR 001 lines 53-60.
No exact match to a supplied open issue was established.

<a id="05-postgres-tests-f03"></a>

### 05-postgres-tests-F03: Boundary rejection tests have unrelated failure paths

- **Severity:** Medium. Removing the intended bounds can leave these tests green.
- **Classification:** Correctness defect in negative controls.
- **Problem references:**
  - `extensions/synchro-pg/src/pg_tests/conflicts.rs:887-925`, `951-963`.
  - `extensions/synchro-pg/src/pg_tests/membership.rs:1138-1163`.
- **Compared validators:**
  - `extensions/synchro-core/src/contract.rs:825-841`.
  - `extensions/synchro-pg/src/bucketing.rs:26-51`.

The 1,001-mutation request repeats the same mutation ID 1,001 times.
The validator independently rejects duplicate mutation IDs.
Removing the maximum-count check still produces the asserted `invalid_request` response.
The same test already has a separate duplicate-ID case.

The membership overflow test supplies `tests.unreachable_membership_function` with OID zero.
It accepts every PostgreSQL exception as `Err(())`.
Replacing checked addition with a permissive bound can reach the nonexistent function and still satisfy the test.

Use unique valid mutations for the count boundary.
Use an existing valid one-result function for the arithmetic boundary.
Keep a valid boundary control before each rejection case.
This removes competing invalid conditions rather than adding another test layer.

**Invariant:** Configured bounds must reject excess work before side effects or unbounded evaluation.
Duplicate mutation rejection remains a separate invariant.

**Acceptance:** Run `make test-rust-pg` with each bound independently disabled in a temporary mutation.
The corrected case must fail for its own mutation.
The current alternate failure paths are statically established, not experimentally demonstrated here.

**Contract:** Wire protocol lines 359 onward and ADR 004 lines 253-258.
No exact match to a supplied open issue was established.

<a id="05-postgres-tests-f04"></a>

### 05-postgres-tests-F04: Schema activation assertions bypass or omit activation

- **Severity:** Medium. The tests cannot detect missing behavior in the actual activation path.
- **Classification:** Correctness defect in proof ownership.
- **Problem references:** `extensions/synchro-pg/src/pg_tests/schema.rs:748-806`, `3191-3214`.
- **Comparison references:**
  - `extensions/synchro-pg/src/pg_tests/membership.rs:1375-1420`.
  - `extensions/synchro-pg/src/pg_tests/schema.rs:1917-2025`.
  - `extensions/synchro-pg/src/lib.rs:2507-2591`.

`test_registry_activation_publishes_schema_manifest` directly changes registry state and then explicitly calls the manifest publisher.
It does not call the production activation path whose publication hook its name promises to test.
Removing that hook would not change this test.

`test_schema_manifest_hash_ignores_bucket_sql_only_changes` stages a registration but never activates it.
Reading the unchanged active manifest does not prove that activated membership changes preserve its hash.

Delete the misleading standalone proofs after placing their distinct assertions in an actual activation flow.
The existing membership manifest flow already performs activation and compares table shape.
It can also compare the contract-relevant schema reference.
Keep publisher formatting tests as publisher tests, not activation evidence.

**Invariant:** Activation must publish the required manifest atomically.
A membership-only change must not invent a client schema change.

**Acceptance:** Run `make test-rust-pg`.
A temporary mutation that omits publication from the real activation path must fail the authoritative activation proof.
A post-activation hash change must fail the membership-only proof.
This review did not execute either mutation.

**Contract:** ADR 001 lines 134-140 and the membership generation invariant.
The local test comments reference issue `#43` for related activation work, not these specific proof defects.

<a id="05-postgres-tests-f05"></a>

### 05-postgres-tests-F05: Reset replacement and cleanup checks start with empty or unchanged state

- **Severity:** Medium. The assertions can miss failure to replace or clear durable state.
- **Classification:** Correctness defect in tests.
- **Problem references:**
  - `extensions/synchro-pg/src/pg_tests/stream_reset.rs:175-208`, `217-255`.
  - `extensions/synchro-pg/src/pg_tests/stream_reset.rs:367-398`.
  - `extensions/synchro-pg/src/pg_tests/stream_reset.rs:400-480`.
- **Comparison references:**
  - `extensions/synchro-pg/src/pg_tests/stream_reset.rs:666-709`.
  - `extensions/synchro-pg/src/lib.rs:2595-2666`, `3357-3410`, `3545-3554`.
  - `extensions/synchro-pg/src/stream_reset.rs:1036-1052`, `2057-2085`, `3724-3749`.

The reset activation test inserts a source row but does not create an old live captured projection.
After staging, its attempted live-projection corruption can update zero rows.
It never checks the affected count.
Consequently, the later absence of the added `title` key does not prove replacement of corrupted old state.

The stream-reset abort test stages the empty shared fixture.
It then asserts zero staged rows without first proving that any rows existed.
The bootstrap abort test similarly checks cleared materialized and acknowledged positions that its setup never populates.
Its barrier and staged-scope checks are meaningful and should remain.

Populate the specific old and staged state before exercising replacement or cleanup.
Assert the corruption affected exactly one row.
Assert nonempty stage counts before abort.
Where a lifecycle does not permit a populated field, remove the misleading clearing assertion instead of fabricating an impossible state.

**Invariant:** Activation replaces verified state atomically.
Abort removes staged state and leaves active state unchanged.

**Acceptance:** Run `make test-rust-pg`.
Disable one replacement or cleanup statement at a time and require the corresponding assertion to fail.
Keep the existing missing-edge and missing-fence negative controls.
The ineffective preconditions were identified statically.

**Contract:** ADR 001 lines 83-125 and the reset baseline fence coverage invariant.
No exact match to a supplied open issue was established.

<a id="05-postgres-tests-f06"></a>

### 05-postgres-tests-F06: Visibility checks can pass with no valid allowed result

- **Severity:** Medium. These checks can report isolation or redaction success when delivery fails entirely.
- **Classification:** Correctness defect in tests.
- **Problem references:**
  - `extensions/synchro-pg/src/pg_tests/pull.rs:771-788`, `847-897`.
  - `extensions/synchro-pg/src/pg_tests/portable_seed.rs:304-325`.
- **Stronger local comparison:** `extensions/synchro-pg/src/pg_tests/pull.rs:790-844`.

The excluded-column test only loops over returned rows and checks that matching rows have seven fields.
An empty `changes` array passes.
Replacing a required field with an excluded field also preserves the count.

The bucket-isolation test only asserts that the other user's primary key is absent.
An empty allowed result passes this check too.

The ungranted-user scope check maps an absent `scopes.add` array to `false`.
An error response therefore counts as successful non-disclosure.

Use one expected allowed result and an exact forbidden result check in each flow.
For projection shape, compare the expected logical field IDs rather than a magic field count.
For the ungranted user, require a successful response and its own expected assignment before checking exclusion.
Delete the weaker count-only and fallback assertions.

**Invariant:** Authorization must both permit authorized data and exclude unauthorized data.
Redaction must preserve the complete allowed row shape.

**Acceptance:** Run `make test-rust-pg`.
Demonstrate failure with an empty-success result, a same-size wrong field set, and an error substituted for the ungranted-user response.
These are static counterexamples, not executed mutants.

**Contract:** Wire protocol lines 90-94 and deterministic server-defined scopes.
No exact match to a supplied open issue was established.

<a id="05-postgres-tests-f07"></a>

### 05-postgres-tests-F07: Several proofs are strict subsets of existing proofs

- **Severity:** Low. Duplicate setup and assertions add maintenance without an independent failure detector.
- **Classification:** Behavior-preserving cleanup.

| Removable proof | Existing proof to retain | Evidence and invariant |
| --- | --- | --- |
| `extensions/synchro-pg/src/pg_tests/integrity.rs:1-51` | `extensions/synchro-core/src/checksum.rs:1608-1659` | Both call the same core `scope_digest` with the same authored vector. The core test also checks the preimage and invalid vectors. |
| `extensions/synchro-pg/src/pg_tests/membership.rs:1165-1255` | `extensions/synchro-pg/src/pg_tests/membership.rs:1257-1373` | The later flow repeats the same two activations and checks row 7, then adds row 8 captured between activations. Both check current digest and registry generation. |
| `extensions/synchro-pg/src/pg_tests/retention.rs:417-435` | `extensions/synchro-pg/src/pg_tests/retention.rs:258-281` | Both compact all available history with no clients. The retained case has two rows rather than one. |
| `extensions/synchro-pg/src/pg_tests/schema.rs:3022-3038` | `extensions/synchro-pg/src/pg_tests/schema.rs:3040-3080` | The same fixture and SQL function feed both tests. The retained test validates the manifest and checks specific fields and excluded fields. |
| Retirement deletion block at `extensions/synchro-pg/src/pg_tests/schema.rs:2374-2381` | `extensions/synchro-pg/src/pg_tests/push_idempotency.rs:373-390` | Both directly delete the same kind of permanent retirement record. The retained check requires SQLSTATE `55000`. Keep the connect rejection assertions. |

The core vector is confirmed at `conformance/vectors/canonical-v1.json:717-739`.
The PostgreSQL integrity test does not call SPI or any extension checksum integration path.
It therefore does not establish that the extension selects the correct rows or binds the correct scope.

Delete these redundant bodies instead of introducing a generic test framework.
For membership, retain the later-row case and its checks for both capture generations.
For compaction, retain exact row-count evidence when consolidating the two equivalent scenarios.

**Invariant:** Each retained proof must still fail for its existing realistic defect.
Do not delete independent database selection, storage, or protocol checks merely because they also use digests.

**Acceptance:** Run `make test-rust-core` and `make test-rust-pg` after consolidation.
For the digest activation consolidation, demonstrate a mutation that leaves row 7 on the old digest.
Also demonstrate a mutation that misses row 8.
No runtime deletion experiment was performed here.

**Related identifiers:** `VEC-SCOPE-ALTERED-PAIRING-001` and the repository's one-proof-home rule.
No exact match to a supplied open issue was established.

<a id="05-postgres-tests-f08"></a>

### 05-postgres-tests-F08: Query-text assertions overconstrain syntax without proving bounded callers

- **Severity:** Medium. The tests can reject valid SQL rewrites while missing the intended unbounded execution defect.
- **Classification:** Correctness defect in performance proof.
- **Problem references:**
  - `extensions/synchro-pg/src/pg_tests/wal_pipeline.rs:811-855`.
  - `extensions/synchro-pg/src/pg_tests/membership.rs:970-1005`, `1066-1105`.
- **Execution comparison:** `extensions/synchro-pg/src/bucketing.rs:26-77`.
- **Existing measurement pattern:** `extensions/synchro-pg/src/pg_tests/schema.rs:3247-3294`.

The backfill test counts `jsonb_to_recordset` and `CROSS JOIN LATERAL` in a separately built query string.
It then checks only membership values and row totals from execution.
A caller can invoke the same query once per row and preserve every assertion.
A valid rewrite with different SQL syntax can fail the text checks.

The membership tests separately execute a query builder with an explicit limit.
They also assert that the resolver eventually rejects excessive results.
An unbounded resolver that rejects after fetching everything can still satisfy both checks if the separate builder remains bounded.

Remove the syntax assertions after replacing them with observations from the production invocation.
Use a bounded database execution observation that detects a per-row caller or excess materialized results.
The registry scan test provides a local example of measuring actual database work.
Do not replace these checks with wall-clock timing thresholds.

**Invariant:** Production callers must bound database work and preserve complete results.
The exact SQL spelling is not that invariant.

**Acceptance:** Run `make test-rust-pg` with temporary per-row and unbounded-fetch mutations.
The authoritative bounded-work tests must fail for those mutations.
An equivalent set-based SQL rewrite must still pass.
The mutation survival argument is static in this report.

**Related issue:** The supplied `#54` server-query-loop issue is related context.
This finding concerns proof strength, not a claim that the current production caller still loops.

<a id="05-postgres-tests-f09"></a>

### 05-postgres-tests-F09: Concurrency observation uses inconsistent and scheduler-dependent budgets

- **Severity:** Medium. A correct implementation can miss the test's lock observation window on a loaded runner.
- **Classification:** Behavior-preserving cleanup of brittle test control flow.
- **Problem references:**
  - `extensions/synchro-pg/src/pg_tests/schema.rs:95-136`.
  - `extensions/synchro-pg/src/pg_tests/schema.rs:2937-2960`.
- **Existing better pattern:** `extensions/synchro-pg/src/pg_tests/schema.rs:250-315`.

The generation-renewal race polls `pg_locks` at most 1,000 times without a deadline or sleep.
The number of fast local queries does not establish a scheduling budget for the remote backend.
The test releases the driver lock after that count, even if the contender has not reached it.

The backfill test permits 3,000 polls with ten-millisecond sleeps.
It gives the contender a five-second statement timeout.
The documented generous observation budget cannot extend the contender's lifetime.

Use the existing monotonic-deadline pattern with a short sleep and early completion detection.
Align the remote timeout with the observation deadline and cleanup budget.
Retain lock identity checks and unconditional release of the driver's lock.
Do not simplify these tests to fixed sleeps.

**Invariant:** The test must observe real blocking before release and then observe the required post-release result.

**Acceptance:** Run `make test-rust-pg` under the primary's normal and loaded validation conditions.
Preserve failures where the contender completes before the intended lock.
No flaky run was observed or claimed during this static review.

**Related identifiers:** Client generation serialization and source-write gate ordering.
No exact match to a supplied open issue was established.

<a id="05-postgres-tests-f10"></a>

### 05-postgres-tests-F10: Seed receipt idempotency depends on a physical tuple address

- **Severity:** Low. A storage-only rewrite can fail a semantic idempotency test.
- **Classification:** Behavior-preserving cleanup.
- **Problem reference:** `extensions/synchro-pg/src/pg_tests/portable_seed.rs:35-92`.
- **Semantic continuation proof:** `extensions/synchro-pg/src/pg_tests/portable_seed.rs:94-117`.

The before-and-after snapshot contains `sync_shared_scopes.ctid`.
Equality therefore requires the same physical heap tuple, not just unchanged scope eligibility and generations.
The test already checks registry generation, manifest content, manifest count, scope generations, and receipt acceptance.

Remove the `scope_row` physical-address member.
Keep the logical state snapshot and authenticated receipt continuation.
If avoiding a redundant write needs a separate performance requirement, state and measure that requirement directly.

**Invariant:** Idempotent registration must preserve logical state and existing valid continuation receipts.

**Acceptance:** Run `make test-rust-pg`.
A same-value physical rewrite must not fail this semantic test when all contract state and receipt behavior remain unchanged.
No physical rewrite experiment was executed here.

**Related identifiers:** Portable seed continuation and registry idempotency.
No exact match to a supplied open issue was established.

<a id="05-postgres-tests-f11"></a>

### 05-postgres-tests-F11: The canonical category upsert does not restore its parent

- **Severity:** Low. Repeated preparation can retain a noncanonical foreign-key value in a supposedly exact seed row.
- **Classification:** Correctness defect in test data.
- **Problem references:**
  - `extensions/testdata/canonical-seed.sql:1-2`, `125-141`.
  - `extensions/testdata/schema.sql:75-84`.
- **Compared upserts:** `extensions/testdata/canonical-seed.sql:19-37`, `100-123`.

The canonical category is initially a root because omitted `parent_id` defaults to null.
Its conflict update restores every other category value but never restores `parent_id`.
If an earlier test assigns another category as its parent, preparation preserves that assignment.
This contradicts the file's exact-convergence claim.

Include `parent_id = NULL` in the authored row and its conflict update.
This removes an implicit default assumption from canonical state restoration.
It does not require another seed format or a general reset framework.

**Invariant:** Repeated canonical preparation must restore the same authored values for every canonical row.

**Acceptance:** On a disposable prepared database, change the canonical category's parent and reload the canonical SQL.
Assert that its parent returns to null and its other authored values remain unchanged.
The caller is `conformance/cmd/synchro-local-postgres/main.go:701`.
This inspection did not execute SQL or claim a supported focused Make target for that new check.

**Related identifiers:** Canonical client seed preparation.
No exact match to a supplied open issue was established.

<a id="05-postgres-tests-f12"></a>

### 05-postgres-tests-F12: The seed generator carries unused ownership state and parameters

- **Severity:** Low. Unused state enlarges the generator's interface and suggests behavior it does not implement.
- **Classification:** Behavior-preserving cleanup.
- **Problem references:**
  - `extensions/testdata/generate/collaboration.py:12-78`, `81-98`, `110-144`, `175-242`.
  - `extensions/testdata/generate/generate.py:19`, `148-183`.
  - `extensions/testdata/generate/tpch_transform.py:38-42`.

`generate_categories` does not use its random generator.
`generate_documents` does not use `n_users`.
It builds and returns `doc_owners`, but the only receiving function does not read it.
`tempfile` and `sql_int_array` have no consumers.
Exact Python consumer searches found no other call sites.

Delete the unused owner map, return member, parameters, import, and helper.
Keep `doc_members`, the actual customer-to-user mapping, and the seeded random stream.
The optional full-data generator remains useful and is not proposed for deletion.

**Invariant:** The same inputs must produce the same SQL and preserve all used relationships.

**Executed static reproduction:** Changing `n_users` from 2 to 999 did not change generated document output.
Passing an empty `doc_owners` map did not change generated comments.
Changing the category random seed did not change category output.
The bounded reproduction generated ten document statements without writing files.

**Acceptance:** Repeat that bounded equivalence check after deletion.
The primary can compare generated SQL from an existing small `.tbl` input without cloning or rebuilding dbgen.
No generator test Make target was identified in `Makefile:1865-1866`.

**Related identifiers:** Deterministic relational fixture generation.
No exact match to a supplied open issue was established.

<a id="05-postgres-tests-f13"></a>

### 05-postgres-tests-F13: Document-member fixtures read peer rows without registering peer impacts

- **Severity:** Medium. Membership revocation in this fixture can leave another member row in a revoked user's scope.
- **Classification:** Correctness defect in relational test configuration.
- **Problem references:**
  - `extensions/testdata/register.sql:115-124`, `261-269`, `325-339`.
  - `extensions/testdata/schema.sql:187-200`.
- **Direct execution dependencies:**
  - `extensions/synchro-pg/src/bgworker.rs:4149-4173`, `4969-5018`.
  - `extensions/synchro-pg/src/registry.rs:4028-4065`.

The membership function returns every active member's user scope for each member row in the same document.
Its result therefore depends on other rows of `document_members`.
The registration declares impacts only for `orders -> line_items` and `documents -> document_comments`.
It declares no peer-row impact for `document_members`.

The worker directly reevaluates the changed row and expands that set through declared dependencies.
The validator requires an impact dependency only when source and target relation IDs differ.

A concrete static counterexample uses members A and B inserted together for one document.
Both member rows initially belong to both user scopes.
Deleting B in a later transaction directly reevaluates B but does not identify unchanged row A.
A's previous membership in B's user scope has no declared removal path.

Preserving the advertised all-member behavior requires one explicit bounded old-and-new peer impact declaration.
It must cover both old and new document IDs on reassignment.
Do not add a broad scan of every registered row as an implicit repair.
If the fixture should model only direct member ownership, that is a product decision, not an approved cleanup.

**Invariant:** Membership removal must identify every affected row and must not retain revoked scope provenance.

**Acceptance:** Use a committed two-member fixture, then delete or reassign one member in a later transaction.
Observe both member rows' scopes after each materialization boundary.
The negative control must omit the peer impact and expose the stale scope.
This flow needs real WAL execution through the primary's integration gate.
It was not executed here, so the finding does not claim a demonstrated production security exploit.

**Contract:** ADR 004 lines 243-285 and requirement `S-08` at line 768.
The same-relation dependency wording also merits the product decision below.
No exact match to a supplied open issue was established.

<a id="05-postgres-tests-f14"></a>

### 05-postgres-tests-F14: Relational fixture documentation overstates transitive ownership coverage

- **Severity:** Low. Maintainers can rely on parent-change coverage that the fixture does not provide.
- **Classification:** Behavior-preserving documentation cleanup, unless transitive ownership is required.
- **Problem references:**
  - `extensions/testdata/README.md:23-25`, `34`, `45-53`.
  - `extensions/testdata/register.sql:229-247`.
- **Actual implementation:**
  - `extensions/testdata/schema.sql:110-145`.
  - `extensions/testdata/register.sql:80-97`, `325-339`.

The README describes orders as resolving ownership through customers and line items as resolving a two-level ownership chain.
Orders instead store `user_id` directly.
A trigger copies it from the customer only when the order is inserted or its `customer_id` changes.
Changing `customers.user_id` does not refresh existing orders.
Line items read that stored order owner through one declared dependency.

The claimed four-level comment membership chain is also absent.
Comment membership reads the document owner and comment author, not a comment-to-member-to-user chain.

Document the actual scope dependencies and distinguish foreign-key structure from membership propagation.
Delete the unsupported depth and propagation claims.
Do not replace the working direct-owner fixture with a new transitive model without a contract decision.

**Invariant:** Documentation must describe the executable test configuration and its actual proof coverage.

**Acceptance:** Static comparison of the corrected table and pattern descriptions with the cited membership SQL is sufficient.
If transitive ownership is selected instead, require a committed customer-owner change that updates descendant memberships.
That alternative is not approved by this report.

**Related identifiers:** Deterministic scope membership and requirement `S-08`.
No exact match to a supplied open issue was established.


<a id="area-06-swift"></a>

## Swift SDK and tests

Abbreviated SDK paths use `clients/swift/Sources/Synchro/`.
Abbreviated test paths use `clients/swift/Tests/SynchroTests/`.

<a id="06-swift-f01"></a>

### 06-swift-F01: Two schema paths hide missing retained-scope digest migration

**Severity:** High. A compatible schema update can make the next unchanged pull enter an integrity error.

**Classification:** Correctness defect.

**Evidence**

- `clients/swift/Sources/Synchro/SyncEngine.swift:1359-1425` uses the migration journal for live connect installation.
- `clients/swift/Sources/Synchro/SchemaManager.swift:394-456,477-490` activates the new manifest and applies scope cursors without converting retained digests.
- `clients/swift/Sources/Synchro/PullProcessor.swift:1186-1254` recomputes rows under the new schema hash and compares them with old stored digests.
- `clients/swift/Sources/Synchro/Integrity.swift:130-141,156-180` includes the schema hash in both digest domains.
- The other migration path performs digest conversion at `clients/swift/Sources/Synchro/SchemaManager.swift:193-223,644-745`.
- `clients/swift/Tests/SynchroTests/SchemaManagerTests.swift:910-1040` tests that other path, not the live connect path.

A retained, unmodified row has an old scope-row digest and an old row-metadata digest.
After a Class 2 manifest activation, its newly computed digest matches neither stored value.
The terminal pull then throws at `PullProcessor.swift:1252`.
An added local index is sufficient to change the manifest hash without changing the row.

**Smallest coherent simplification**

Make the journal path the only schema-transition implementation.
Put required retained-scope integrity conversion in that transaction.
Move tests from the alternate path before deleting its migration and rehash implementation.
Do not copy its current-row rehash blindly for protected local rows or rows ahead of their WAL echo.
Those rows require a verified authoritative projection or an explicit recovery decision.

**Invariant:** Schema activation, authoritative provenance, row versions, checksums, and replacement cursors must describe one compatible state.

**Acceptance:** Run `make test-swift-unit` and `make test-swift-scenarios` after adding a live-connect Class 2 case.
Use a nonempty retained scope and an index-only change.
Require the following unchanged pull to succeed without losing intent or substituting an unverified digest.

**Contract:** `docs/src/content/docs/spec/02-client-contract.mdx:108-111,315-320` and `docs/src/content/docs/spec/05-schema-evolution.mdx:507-521`.

<a id="06-swift-f02"></a>

### 06-swift-F02: Nullable relaxation changes the manifest but leaves SQLite NOT NULL

**Severity:** High. A contract-valid server row or offline write can fail after an accepted Class 2 migration.

**Classification:** Correctness defect.

**Evidence**

- `clients/swift/Sources/Synchro/SchemaMigrationJournal.swift:88-112,125-145` accepts `nullable: false` to `true`.
- The plan adds only missing fields. It emits no operation for an existing field's nullability.
- `clients/swift/Sources/Synchro/SQLiteSchema.swift:25-38` creates non-primary fields with `NOT NULL`.
- `clients/swift/Sources/Synchro/SchemaManager.swift:511-535` checks affinity and primary keys, but not the changed nullability.
- `docs/src/content/docs/spec/05-schema-evolution.mdx:514-521` explicitly permits this Class 2 transition.

The in-memory SQLite reproduction retained `notnull=1` and rejected `UPDATE ... SET title = NULL`.
That reproduction confirms the unchanged-constraint behavior, not the complete migration flow.

**Smallest coherent simplification:** Add one explicit local table-rebuild operation for supported constraint relaxation.
Use the journal transaction to preserve rows and reinstall the declared indexes and capture triggers.
Do not add another migration entry point.

**Invariant:** The physical SQLite schema must accept every value allowed by the activated portable schema.
Preserve local-only tables and durable authored intent.

**Acceptance:** Run `make test-swift-unit` and `make test-swift-scenarios`.
Migrate a populated NOT NULL field to nullable through connect, then apply and author null values.
Inject failure during the local table replacement and verify complete rollback.

<a id="06-swift-f03"></a>

### 06-swift-F03: Writes without a parsed capture context can change synced rows without intent

**Severity:** High. A successful local update can remain absent from the durable queue.

**Classification:** Correctness defect.

**Evidence**

- `clients/swift/Sources/Synchro/ApplicationDatabase.swift:309-350` derives ordinary capture context from the top-level parsed table name.
- `clients/swift/Sources/Synchro/ApplicationDatabase.swift:147-162,179-187` permits nonreserved application views and their resolved writes to synced rows.
- `clients/swift/Sources/Synchro/ApplicationDatabase.swift:35-55` executes transaction queries without the execute wrapper.
- `clients/swift/Sources/Synchro/SQLiteSchema.swift:95-138,255-271` captures updates only when the context names a changed writable field.
- The insert path instead rejects missing authored context at `SQLiteSchema.swift:238-252`.

For example, an application view can use an `INSTEAD OF UPDATE` trigger to update an existing synced row.
The parser finds the view, not the synced table, so it installs no context.
The generated update trigger permits the row change but records no mutation.
An update executed through transaction `query` with `RETURNING` has the same missing-context risk.

**Smallest coherent simplification:** Fail closed when a synced writable update has no valid authored context.
Keep the SQLite authorizer as the resolved-object authority.
Decide which indirect write forms to support before extending the SQL parser.
Explicit authored transactions can remain the supported route for application-generated complex writes.

**Invariant:** A synced application change and its immutable capture record commit together, or neither commits.

**Acceptance:** Add view-trigger and `UPDATE ... RETURNING` boundary cases under `make test-swift-unit`.
Require either complete exact capture or an atomic rejection.
Static inspection establishes the branch gap. The SDK reproduction remains unexecuted.

**Contract:** `docs/src/content/docs/spec/02-client-contract.mdx:190-211`.

<a id="06-swift-f04"></a>

### 06-swift-F04: Capture casts can replace invalid authored values with different valid values

**Severity:** High. The ledger can retain a value different from the application value without reporting an error.

**Classification:** Correctness defect.

**Evidence**

- `clients/swift/Sources/Synchro/SQLiteSchema.swift:185-214` casts integer, boolean, real, and text capture values.
- `clients/swift/Sources/Synchro/SQLiteSchema.swift:21-41` creates ordinary affinity tables without storage-class checks.
- `clients/swift/Sources/Synchro/PushProcessor.swift:194-274` validates the result after the cast, not the original SQLite storage value.

The in-memory reproduction stored `not-an-integer` as TEXT in an INTEGER-affinity column.
The exact capture expression `CAST(amount AS INTEGER)` returned `0`.
The resulting stored integer has a valid shape and portable value.
The original invalid text is no longer available to the sealing validator.

**Smallest coherent simplification:** Reject incompatible SQLite storage classes in the capture transaction instead of repairing them with casts.
Retain the exact accepted typed value.
Delete lossy conversion branches where SQLite already stores the required type.

**Invariant:** Capture must preserve authored values and must not silently invent valid replacements.

**Acceptance:** Run `make test-swift-unit` with invalid TEXT, fractional REAL, and overflow inputs for integer fields.
Require row and ledger rollback, or exact retained invalid intent under an explicitly approved policy.
Keep valid native Int64 and canonical textual inputs working.

**Contract:** `docs/src/content/docs/spec/02-client-contract.mdx:145-169,190-195`.

<a id="06-swift-f05"></a>

### 06-swift-F05: Startup backoff cancellation leaves the startup continuation unresolved

**Severity:** High. `start()` and a concurrent `stop()` can both wait indefinitely.

**Classification:** Correctness defect.

**Evidence**

- `clients/swift/Sources/Synchro/SyncEngine.swift:213-217,284-301` counts startup as an active operation and waits on `StartupGate`.
- `clients/swift/Sources/Synchro/SyncEngine.swift:566-612` handles startup retry delay in a nested catch.
- For a connecting, pulling, or rebuilding failure within the retry budget, `gateResolved` remains false.
- Cancellation of the sleep at line 596 reaches lines 607-612, which return without resolving the gate.
- `clients/swift/Sources/Synchro/SyncEngine.swift:1935-1952` cancels the managed task and then waits for active operations to drain.

The unresolved startup caller cannot execute its `endOperation()` defer.
The stop operation therefore cannot drain that caller.
Existing backoff stop coverage uses `maxRetryAttempts: 0`, which resolves the gate first.
See `clients/swift/Tests/SynchroTests/SyncEngineTests.swift:1519-1555`.

**Smallest coherent simplification:** Give startup one terminal-result owner for every exit.
Resolve cancellation and persistence failures through that owner.
Remove the nested exit path that can bypass gate completion.

**Invariant:** Stop must cancel active network work while preserving durable retry state and releasing every caller.

**Acceptance:** Run `make test-swift-unit` with default retry attempts, a retryable connect response, and stop during the first delay.
Require bounded completion of both calls and unchanged durable retry identity.
Repeat with a backoff-persistence failure.
This report provides a static control-flow proof, not a measured hang.

<a id="06-swift-f06"></a>

### 06-swift-F06: Retried connect responses bypass normal contract validation

**Severity:** High. Retry can install a response that the initial connect path rejects.

**Classification:** Correctness defect.

**Evidence**

- `clients/swift/Sources/Synchro/SyncEngine.swift:1274-1311` validates normal connect responses against request scopes and scope-set version.
- `clients/swift/Sources/Synchro/SyncEngine.swift:920-930` sends a durable connect retry and returns without that validation.
- Both paths install through `SyncEngine.swift:536-549,938-944,1314-1467`.
- `clients/swift/Sources/Synchro/ContractModels.swift:483-576` owns protocol, counter, assignment, manifest-hash, and cursor-disposition checks.

A decoded retry response with `protocol_version: 2` or a regressing scope-set version reaches installation without those checks.
The `.none` installation branch does not repeat them.

**Smallest coherent simplification:** Use one send-and-validate connect operation for fresh and replayed request bodies.
Keep exact request bytes as an input, not as a separate validation route.

**Invariant:** Retry changes timing, not response-validation strength or assignment authority.

**Acceptance:** Under `make test-swift-unit`, retry a stored connect request and return each invalid response class.
Require no schema, generation, assignment, or cursor progress.

<a id="06-swift-f07"></a>

### 06-swift-F07: Configuration clamps page limits and permits timer conversion traps

**Severity:** Medium. Invalid configuration can change request semantics or terminate the process instead of returning a local error.

**Classification:** Correctness defect.

**Evidence**

- `clients/swift/Sources/Synchro/SynchroConfig.swift:70-82` clamps `pullPageSize` and does not validate lower bounds or finite timing values.
- `clients/swift/Sources/Synchro/SyncEngine.swift:831-836,1580-1587` converts timing values directly to `UInt64`.
- `clients/swift/Sources/Synchro/ChangeTracker.swift:340-356` uses the configured push limit in SQLite `LIMIT`.
- `docs/src/content/docs/spec/02-client-contract.mdx:831-834` requires local limit rejection and explicitly forbids clamping.

Negative, infinite, or NaN timing values do not have a valid `UInt64` conversion.
A negative push limit also removes SQLite's row bound instead of representing a valid batch size.

**Smallest coherent simplification:** Validate configuration once before database or lifecycle work.
Delete the clamp and rely on one validated configuration domain.

**Invariant:** Invalid local configuration must fail before transmission or durable work.

**Acceptance:** Run `make test-swift-unit` with zero, negative, over-limit, NaN, and infinite inputs.
No case may crash, transmit, or silently substitute a value.
The conversion-trap conclusion is static. No crashing Swift process ran.

<a id="06-swift-f08"></a>

### 06-swift-F08: Test-only reconciliation permits the exact-ID behavior that production forbids

**Severity:** Medium. Tests keep permissive production branches alive and can pass without proving production reconciliation.

**Classification:** Behavior-preserving cleanup of the live sync path.

**Evidence**

- Live push supplies complete `sentPending` at `clients/swift/Sources/Synchro/PushProcessor.swift:58-100`.
- Test entry points default it to empty at `PushProcessor.swift:639-651,799-820`.
- `PushProcessor.swift:1093-1116` then permits a missing ledger source.
- `PushProcessor.swift:734-742,944-949,1516-1535` falls back to retiring unsealed rows by table and record ID.
- `clients/swift/Tests/SynchroTests/PushProcessorTests.swift:604-642,790-820,1123-1157` uses synthetic `m1` outcomes unrelated to captured mutation IDs.
- `PushProcessorTests.swift:822-912` also maps synthetic outcome IDs to different captured IDs through `sentPending`.

The runtime outcome partition validation rejects those identities before reconciliation.
These tests exercise a different behavior rather than a smaller form of the live behavior.

**Smallest coherent simplification:** Require one exact durable source for every reconciliation outcome.
Update tests to capture, seal, and use the real mutation ID.
Delete row-based compatibility retirement, optional-source branches, and defaults that exist only for these tests.
Keep any genuinely supported legacy disk migration separate and explicit.

**Invariant:** An outcome changes only its exact scoped mutation and preserves later same-row intent.

**Acceptance:** Run `make test-swift-unit` and `make test-swift-scenarios`.
Include an unknown-ID negative control and accepted/rejected successors with exact membership.

**Contract:** `docs/src/content/docs/spec/02-client-contract.mdx:299-320`.

<a id="06-swift-f09"></a>

### 06-swift-F09: Dormant protocol-era paths and their tests remain in the SDK

**Severity:** Medium. Maintainers must preserve code that has no live SDK consumer.

**Classification:** Behavior-preserving cleanup, subject to public symbol compatibility review.

**Evidence**

- `clients/swift/Sources/Synchro/ChangeTracker.swift:363-404` hydrates `PushRecord`, while live sealing builds `Mutation` at `PushProcessor.swift:113-159`.
- `clients/swift/Sources/Synchro/Models.swift:6-44` defines the old physical-name push representation.
- Its only repository call sites are hydration tests in `PushProcessorTests.swift:50-96,546-586,869-912` and `ChangeTrackerTests.swift:331-395`.
- `clients/swift/Sources/Synchro/PullProcessor.swift:39-46` updates the old numeric checkpoint only for `PullProcessorTests.swift:261-283`.
- `clients/swift/Sources/Synchro/SchemaManager.swift:11-27` has no caller. It retains a separate GET-schema path.
- `clients/swift/Sources/Synchro/ContractModels.swift:842-884` has an unused historical-table response validator beside live verification at `PushProcessor.swift:1158-1183`.
- `clients/swift/Sources/Synchro/ChangeTracker.swift:586-605` has unused row/table cancellation helpers.
- `clients/swift/Sources/Synchro/Database.swift:7-13` has unused observation wrapper structs.

Exact Swift searches found these consumers or their absence.
The numeric checkpoint's migration and seed sentinel are separate compatibility concerns.
Deleting its dead updater does not require deleting stored metadata.

**Smallest coherent simplification:** Delete dead internal entry points and tests that prove only those paths.
Use the live immutable mutation and journal paths for retained behavior.
Check public `PushRecord` and `SchemaResponse` compatibility before removing public declarations.

**Invariant:** Preserve shipped disk migrations, portable-seed validation, exact replay, and current public behavior.

**Acceptance:** Run `make test-swift-unit`, `make build-swift-native-runner`, and `make test-swift-scenarios` after consumer cleanup.
Static call-site absence is the current evidence. No binary ABI analysis ran.

<a id="06-swift-f10"></a>

### 06-swift-F10: Pending selection repeats whole-queue normalization and per-row field loads

**Severity:** Medium. Small batches and count queries can perform work proportional to the full offline queue.

**Classification:** Behavior-preserving cleanup.

**Evidence**

- `clients/swift/Sources/Synchro/ChangeTracker.swift:333-341` normalizes twice through nested overloads.
- `ChangeTracker.swift:629-636` normalizes during a count query.
- `ChangeTracker.swift:641-718` loads and groups all unsealed entries, then repeats the load and grouping after normalization.
- `ChangeTracker.swift:654-656,720-733,807-824` fetches field values once per row in both passes.
- `clients/swift/Sources/Synchro/SyncEngine.swift:998-1001,1072-1075` calls the count-backed pending check during normal cycles and batch draining.

The repeated pass is visible in control flow. It is not a measured timing claim.
Source sorting is also repeated after SQL already orders by `local_order`.

**Smallest coherent simplification:** Remove the duplicate invocation first.
Load each required ledger and field set once per normalization transaction.
Use affected typed-row identities for the delete blocker pass rather than reloading unrelated rows.
Keep normalization semantics before selecting sendable mutations.

**Invariant:** Preserve schema boundaries, local order, immutable source records, cancellation links, and predecessor blockers.

**Acceptance:** Run `make test-swift-unit` and `make test-swift-warm-connect`.
Add bounded query-count evidence for a large queue with a small push batch.

**Issue lead:** Related to supplied issue `#47`. The current finding proves queue scans, not an unmeasured capture timing claim.

<a id="06-swift-f11"></a>

### 06-swift-F11: Historical schema resolution scans and decodes every retained push batch

**Severity:** Medium. Sealing and reconciliation costs grow with completed history rather than the active schema set.

**Classification:** Behavior-preserving cleanup.

**Evidence**

- `clients/swift/Sources/Synchro/PushProcessor.swift:1025-1090` selects all batch requests and schema bodies for each cache miss.
- It decodes complete `PushRequest` values merely to compare `request.schema`.
- The cache key includes `tableID`, so tables from one schema can repeat the same scan.
- Separate build, validation, accepted, and rejected operations use separate caches at `PushProcessor.swift:124,385,411,667,837`.
- `PushProcessor.swift:488-499` retains completed batches.
- `clients/swift/Sources/Synchro/Internal/SynchroMeta.swift:161-178` already has a schema-keyed archive.
- `clients/swift/Tests/SynchroTests/PushProcessorTests.swift:229-299` proves conflicting retained bindings must still fail.

**Smallest coherent simplification:** Resolve a complete schema once per transaction, keyed by its exact schema reference.
Use an indexed authoritative archive rather than decoding all historical request payloads.
Validate conflicting schema copies when importing or sealing them.
Compare the archive with the current operation's retained sealed manifest during lookup.
Do not replace consistency checks with a first-match fallback.

**Invariant:** One schema reference identifies one immutable definition, including during historical replay.

**Acceptance:** Run `make test-swift-unit` and `make test-swift-warm-connect`.
Retain the archive-conflict negative control and add query-count evidence independent of completed batch count.

**Existing issue:** The current source matches the supplied `#95` historical-schema validation lead.

<a id="06-swift-f12"></a>

### 06-swift-F12: Portable value conversion has several owners inside the same Swift engine

**Severity:** Medium. Duplicated validation and encoding rules already differ and make protocol changes harder to audit.

**Classification:** Behavior-preserving cleanup.

**Evidence**

- Canonical integer checks repeat at these locations:
  - `clients/swift/Sources/Synchro/Integrity.swift:368-374`
  - `clients/swift/Sources/Synchro/Internal/SQLiteHelpers.swift:97-103`
  - `clients/swift/Sources/Synchro/PullProcessor.swift:1044-1050`
  - `clients/swift/Sources/Synchro/PushProcessor.swift:1433-1439`
- Additional primary-key spelling logic appears at `clients/swift/Sources/Synchro/ContractModels.swift:705-725` and `clients/swift/Sources/Synchro/PushProcessor.swift:172-190`.
- Base64url decoding repeats at `clients/swift/Sources/Synchro/Integrity.swift:493-501` and `clients/swift/Sources/Synchro/Internal/SQLiteHelpers.swift:105-116`.
- SQLite-to-wire conversion repeats at `clients/swift/Sources/Synchro/PullProcessor.swift:1298-1341` and `clients/swift/Sources/Synchro/SchemaManager.swift:702-745`.
- `clients/swift/Sources/Synchro/ChangeTracker.swift:13-73` contains permissive and throwing conversions for the same stored value.
- `clients/swift/Sources/Synchro/PushProcessor.swift:234-274` separately owns stored-value shape validation.

These are repeated rules inside one language, not unavoidable cross-platform implementations.
For example, integer NSNumber acceptance differs between `Integrity.swift:345-353` and `SQLiteHelpers.swift:61-74`.

**Smallest coherent simplification:** Put each typed conversion rule in its existing owning type or `SQLiteHelpers`.
Use one throwing stored-value conversion that validates its shape.
Delete duplicate helpers and silent defaults after callers use the shared rule.
Do not add a general codec framework.

**Invariant:** Preserve exact values, null presence, strict booleans, canonical integer text, and canonical bytes.

**Acceptance:** Run `make test-swift-unit` and `make test-swift-scenarios`.
Use the authored portable vectors plus malformed storage-shape controls.

<a id="06-swift-f13"></a>

### 06-swift-F13: Pull generates invalid upsert SQL for a primary-key-only table

**Severity:** Medium. A valid read-only synced table can fail during pull or rebuild.

**Classification:** Correctness defect.

**Evidence**

- `clients/swift/Sources/Synchro/ContractModels.swift:360-398` permits a table with its primary-key field only.
- `clients/swift/Sources/Synchro/PullProcessor.swift:1010-1023` always emits `DO UPDATE SET`, even when no update columns exist.
- The corresponding push projection already emits `DO NOTHING` for this case at `PushProcessor.swift:1315-1329`.
- `clients/swift/Tests/SynchroTests/IntegrityTests.swift:270-297` constructs a primary-key-only schema, but does not exercise pull SQL.

The in-memory SQLite reproduction returned `incomplete input` for the generated empty update clause.

**Smallest coherent simplification:** Use `DO NOTHING` when the update set is empty.
Share only the upsert statement construction that has identical meaning for its callers.

**Invariant:** Preserve typed row identity, digest verification, metadata updates, and capture suppression.

**Acceptance:** Run `make test-swift-unit` with pull and rebuild of a primary-key-only table.
Apply the same row twice and require one row with correct provenance and no local intent.

<a id="06-swift-f14"></a>

### 06-swift-F14: Bounded state inspection performs unbounded loading before truncation

**Severity:** Medium. A small inspection limit still allocates and decodes the complete retained state while holding the writer.

**Classification:** Behavior-preserving cleanup.

**Evidence**

- `clients/swift/Sources/Synchro/SynchroClient.swift:242-306` fetches all scope rows and rebuild receipts before applying `prefix(maximumRecords)`.
- `SynchroClient.swift:383-419` performs per-scope queries and sorts complete results.
- `SynchroClient.swift:425-440,497-677` loads every receipt body and recomputes every row and scope digest.
- `clients/swift/Sources/Synchro/Database.swift:160-166` executes capture on the serialized writer.
- Row metadata already uses a SQL limit at `Internal/SynchroMeta.swift:627-637`.

A limit of zero still executes receipt decoding and checksum reconstruction.
The public limit bounds output arrays, not the work needed to create them.

**Smallest coherent simplification:** Use counts and bounded ordered queries before constructing records.
Separate requested receipt verification from a lightweight bounded state summary.
Delete full-array construction on overflow paths.

**Invariant:** Counts, truncation flags, and returned records must come from one consistent snapshot.
An omitted verification result must never appear as a successful verification.

**Acceptance:** Run `make test-swift-unit` and `make test-swift-warm-connect`.
Show that limits zero and one do not decode all retained receipt bodies.
Keep overflow reporting and atomic-snapshot tests.

<a id="06-swift-f15"></a>

### 06-swift-F15: Inspection omits orphan provenance and rebuild attempts

**Severity:** Medium. Inspection can hide durable rows that are especially important during corruption diagnosis.

**Classification:** Correctness defect in verification and inspection.

**Evidence**

- `clients/swift/Sources/Synchro/SynchroClient.swift:383-419` enumerates scope rows and attempts by first enumerating `_synchro_scopes`.
- `SynchroClient.swift:251-254` counts the underlying tables directly.
- `SynchroClient.swift:268-272` derives several truncation flags from the filtered arrays rather than those counts.
- `clients/swift/Sources/Synchro/Database.swift:373-382,508-518` does not constrain these tables with a foreign key to scopes.

A scope-row record or rebuild attempt without a scope can exist in the database.
Its direct count is nonzero, but its inspection array can be empty with `overflowed: false`.

**Smallest coherent simplification:** Read these tables directly in one ordered query each.
Return orphan records for diagnosis or fail explicitly on their invalid binding.
Delete the scope-driven query loops.

**Invariant:** Inspection must not silently discard stored evidence or label an incomplete view complete.

**Acceptance:** Add orphan scope-row and orphan rebuild-attempt controls under `make test-swift-unit`.
Require explicit rejection or complete visibility with accurate counts and flags.

<a id="06-swift-f16"></a>

### 06-swift-F16: Runner transport facts invent protocol codes from HTTP status

**Severity:** Medium. Conformance output can report a server code that the server never sent.

**Classification:** Correctness defect in verification evidence.

**Evidence**

- `clients/swift/Sources/SynchroNativeRunner/SynchroNativeRunner.swift:466-477,502-535` maps status and endpoint to an assumed protocol code.
- A missing parsed code uses that assumption.
- Retryability also comes from status alone.
- `clients/swift/Sources/Synchro/HttpClient.swift:255-272,595-612` distinguishes malformed service envelopes from valid retryable responses.
- `clients/swift/Tests/SynchroTests/TransportObservationTests.swift:154-185` already supplies malformed HTTP 503 data as a negative input.

For that malformed response, the SDK rejects the envelope.
The runner can nevertheless report `capture_pending` and `retryable: true`.

**Smallest coherent simplification:** Report the observed code only.
Represent absent or invalid envelope facts explicitly.
Delete `transportFailureFacts` and its fallback protocol classifications.

**Invariant:** Verification observations must record actual transport facts, not expected behavior inferred from them.

**Acceptance:** Run `make build-swift-native-runner` and `make test-swift-scenarios` after adding malformed 409 and 503 runner cases.
Require no invented code and no false retryable classification.

**Related issue:** This concerns evidence integrity relevant to supplied issue `#36`. Its full issue scope was not independently checked.

<a id="06-swift-f17"></a>

### 06-swift-F17: Requirement-proof suites duplicate existing flows instead of keeping one proof home

**Severity:** Medium. Changes require editing multiple large proofs with the same behavior and different setup code.

**Classification:** Behavior-preserving test cleanup.

**Compared evidence**

- Queue normalization, response faults, restart, and successor preservation repeat in these ranges:
  - `clients/swift/Tests/SynchroTests/Issue49RequirementProofTests.swift:616-799`
  - `clients/swift/Tests/SynchroTests/Issue49CompleteRequirementProofTests.swift:543-679`
  - `clients/swift/Tests/SynchroTests/PushProcessorTests.swift:98-227`
- Rebuild isolation and protected rows repeat in these ranges:
  - `clients/swift/Tests/SynchroTests/Issue49RequirementProofTests.swift:437-614`
  - `clients/swift/Tests/SynchroTests/Issue49CompleteRequirementProofTests.swift:130-293`
  - `clients/swift/Tests/SynchroTests/PullProcessorTests.swift:1238-1429`
- Queued seed rejection repeats in `clients/swift/Tests/SynchroTests/Issue49RequirementProofTests.swift:232-268` and `clients/swift/Tests/SynchroTests/Issue49CompleteRequirementProofTests.swift:1893-1910`.
- Prepared migration recovery repeats in these ranges:
  - `clients/swift/Tests/SynchroTests/Issue49CompleteRequirementProofTests.swift:681-752`
  - `clients/swift/Tests/SynchroTests/SchemaManagerTests.swift:1139-1207`
  - `clients/swift/Tests/SynchroTests/SyncEngineTests.swift:2665-2741`
- Happy-path mocked orchestration remains at `clients/swift/Tests/SynchroTests/SyncEngineTests.swift:220-492`.
- Real client flows exist at `clients/swift/Tests/SynchroTests/IntegrationTests.swift:189-280`.

Not every assertion in these ranges is redundant.
The duplication is the repeated complete setup and flow for overlapping invariants.
The added issue-named suites do not replace the previous proof homes.

**Smallest coherent simplification:** Map each invariant to one authoritative flow.
Move unique failure controls into that flow, then delete duplicate journeys and their private builders.
Keep isolated transport-fault tests only where deterministic real-server execution cannot supply the fault.

**Invariant:** Deletion must retain exact replay, crash recovery, independent expected values, and meaningful negative controls.

**Acceptance:** Run `make test-swift-unit` and `make test-swift-scenarios` after updating requirement ownership.
Demonstrate that the retained proof fails for each deleted test's distinct realistic defect.

**Existing issue references:** Both suites explicitly identify `#49` in their names. The repository requires one authoritative proof home.

<a id="06-swift-f18"></a>

### 06-swift-F18: The checksum-vector test can reject invalid input before production sees it

**Severity:** Medium. A passing authored-vector run can credit test code with rejecting a production input defect.

**Classification:** Correctness defect in proof.

**Evidence**

- `clients/swift/Tests/SynchroTests/IntegrityTests.swift:198-234` accepts any execution error as success for an invalid vector.
- `IntegrityTests.swift:359-368,416-501` runs a test-only JSON parser that rejects duplicate row keys before calling `Integrity.rowDigest`.
- Production JSON validation exists at `clients/swift/Sources/Synchro/Integrity.swift:220-227,611-714`.

The test helper, rather than the production boundary, can supply the required failure.
Keeping another handwritten parser also creates a separate grammar to maintain.

**Smallest coherent simplification:** Send raw vector input through the relevant production raw-JSON boundary before typed digest processing.
Delete the test-only duplicate-key parser.
Do not claim duplicate-member rejection from a dictionary-only digest API.

**Invariant:** Authored vectors define expected behavior. Production code must perform the observed acceptance or rejection.

**Acceptance:** Run `make test-swift-unit` with a demonstrated mutation that disables production duplicate-key rejection.
The applicable authored vector must fail without a test-helper precheck.

<a id="06-swift-f19"></a>

### 06-swift-F19: Several negative controls do not exercise the behavior they name

**Severity:** Low. These tests create misleading confidence and unnecessary maintenance.

**Classification:** Behavior-preserving test cleanup.

**Evidence**

- `clients/swift/Tests/SynchroTests/SyncEngineTests.swift:7-59` tests callback cancellation by calling `stop()` a second time.
- Stop is idempotent at `clients/swift/Sources/Synchro/SyncEngine.swift:1954-1956`, so no second event occurs even if cancellation does nothing.
- `clients/swift/Tests/SynchroTests/SynchroClientTests.swift:527-565` ignores returned rows and passes when the initial watch callback runs.
- An empty result caused by a lost null bind can therefore satisfy the test.
- `clients/swift/Tests/SynchroTests/Issue49RequirementProofTests.swift:205-225,824-829` creates different expected-value objects and asserts inequality.
- Those checks do not mutate production behavior or its observed execution.

**Smallest coherent simplification:** Remove comparison-only controls.
After cancelling one callback, cause a real permitted transition and keep another callback as a positive control.
For null binding, assert exact returned rows and a nonmatching row control.

**Invariant:** A test must fail when its named production behavior breaks.

**Acceptance:** Run `make test-swift-unit` with no-op callback cancellation and dropped-null-binding mutants.
The retained tests must fail for those mutants.

<a id="06-swift-f20"></a>

### 06-swift-F20: Legacy test schema helpers ignore inputs and manufacture inconsistent schema bindings

**Severity:** Medium. The helpers obscure which schema and protocol rules a test actually exercises.

**Classification:** Behavior-preserving test cleanup.

**Evidence**

- `clients/swift/Tests/SynchroTests/TestSchemaHelpers.swift:56-57,114-174` aliases current types to old names and ignores `dbType`, `defaultKind`, policy, parent, and dependency inputs.
- `TestSchemaHelpers.swift:176-220` converts local tables into a manifest with synthetic identity and fixed initial lineage.
- Its `SchemaResponse` wrapper can carry a different version/hash from that manifest.
- `TestSchemaHelpers.swift:218-220` turns schema conversion errors into an empty table list.
- `clients/swift/Tests/SynchroTests/SchemaManagerTests.swift:119-206` uses noncanonical hashes such as `portable-v1` and `portable-v2`.

The helper compatibility layer allows old tests to compile without adopting the current contract.
It also supports the alternate migration path described in F01.

**Smallest coherent simplification:** Use explicit current manifest fixtures and small current-type builders.
Delete ignored parameters, aliases, identity pass-through properties, and the empty-array fallback.
Keep physical names distinct from field IDs in protocol-sensitive tests.

**Invariant:** A test must state whether it exercises local SQL construction or a verified protocol schema.

**Acceptance:** Run `make test-swift-unit`.
Require contract-sensitive fixtures to pass manifest validation and hash checks without helper repair.

<a id="06-swift-f21"></a>

### 06-swift-F21: The scenario loader has no semantic consumer

**Severity:** Low. Unused verification machinery appears to connect SDK tests to shared scenarios but does not execute them.

**Classification:** Behavior-preserving cleanup.

**Evidence**

- `clients/swift/Tests/SynchroTests/ScenarioFixtureLoader.swift:1-70` implements catalog lookup, digest checking, and path validation.
- Its only Swift consumer is `ScenarioFixtureLoaderTests.swift:1-15`.
- That test checks fixture identity, version, proof-type strings, and an unknown ID.
- No SDK flow consumes the loaded scenario.

**Smallest coherent simplification:** Delete this loader and its self-test unless the primary assigns it a real authoritative scenario consumer.
Do not add another scenario runner merely to justify the helper.

**Invariant:** Existing direct Swift scenario execution must remain the semantic proof path.

**Acceptance:** Run `make test-swift-unit` and `make test-swift-scenarios` after deletion.
Use a static consumer search to confirm no semantic test depended on this loader.

**Issue lead:** This resembles supplied issue `#102`, but that issue's stated unused probes and import scanner are not this exact helper.

<a id="06-swift-f22"></a>

### 06-swift-F22: Pull duplicate detection serializes every complete change only to choose error wording

**Severity:** Low. Large valid pages incur avoidable payload encoding and allocation.

**Classification:** Behavior-preserving cleanup.

**Evidence**

- `clients/swift/Sources/Synchro/PullProcessor.swift:686-704` JSON-encodes every change and stores the encoded body by effect key.
- Both a repeated identical body and a different body for the same key throw the same public error category.
- Only the message differs.
- `clients/swift/Tests/SynchroTests/PullProcessorTests.swift:474-564` asserts the exact internal messages.

**Smallest coherent simplification:** Keep a `Set<PullEffectKey>` and reject every repeated key.
Delete the payload serialization and message-specific assertions.

**Invariant:** No response may contain two effects for the same scope and typed row identity.

**Acceptance:** Run `make test-swift-unit` with identical and conflicting duplicate effects.
Both must fail before row, provenance, or cursor changes.

<a id="06-swift-f23"></a>

### 06-swift-F23: Disabled transport observation still decodes complete request bodies

**Severity:** Low. Every ordinary request pays for an observation-only decode even when observation is disabled.

**Classification:** Behavior-preserving cleanup.

**Evidence**

- `clients/swift/Sources/Synchro/HttpClient.swift:293-304` unconditionally calls `transportRequestFacts`.
- `HttpClient.swift:400-459` decodes full connect, pull, rebuild, and push request models.
- A push decode includes all authored mutation values.
- `HttpClient.swift:571-582` already guards cursor fingerprint extraction when no collector exists.
- `clients/swift/Tests/SynchroTests/TransportObservationTests.swift:259-269` checks only that the collector field is nil.

**Smallest coherent simplification:** Guard all observation-only fact extraction with collector presence.
Delete the default-path decode and unused temporary facts.

**Invariant:** Observation remains opt-in and cannot change request bytes, retries, or validation.

**Acceptance:** Run `make test-swift-unit`.
Compare observation-enabled and disabled request bytes and outcomes.
Use a focused allocation or decode-count check to verify the disabled path performs no fact extraction.

<a id="06-swift-f24"></a>

### 06-swift-F24: HTTP 426 still uses the legacy error-message path

**Severity:** Medium. Upgrade errors can report a message as a minimum version and accept malformed protocol envelopes.

**Classification:** Correctness defect.

**Evidence**

- `clients/swift/Sources/Synchro/HttpClient.swift:248-253` assigns `errorMessage` to `minimumVersion`.
- `HttpClient.swift:585-592` also accepts the old flat string envelope.
- `clients/swift/Sources/Synchro/ContractModels.swift:1200-1227` already represents protocol and runtime upgrade metadata.
- `clients/swift/Tests/SynchroTests/HttpClientTests.swift:221-249` supplies a flat legacy envelope and checks only the current version.
- The resulting metadata becomes public at `clients/swift/Sources/Synchro/SyncEngine.swift:2042-2050`.

**Smallest coherent simplification:** Decode and validate the canonical `upgrade_required` envelope once.
Use its actual protocol/runtime fields instead of a message-to-version fallback.
Delete the legacy 426 test fixture and fallback use for this status.

**Invariant:** Preserve the authoritative error category and bounded upgrade metadata without inventing version values.

**Acceptance:** Run `make test-swift-unit` with canonical protocol-upgrade, runtime-upgrade, and malformed 426 responses.
Check exact metadata values, not only the thrown enum case.

<a id="06-swift-f25"></a>

### 06-swift-F25: Watch reads before subscribing and ignores the requested table set

**Severity:** Medium. A watch can miss a committed update, while unrelated writes cause needless query execution.

**Classification:** Correctness defect with a simpler observation boundary.

**Evidence**

- `clients/swift/Sources/Synchro/Database.swift:272-289` ignores `tables` in both observation methods.
- `Database.swift:283-286` completes the initial read and callback before registering the change observer.
- `Database.swift:302-306` notifies every callback after every wrapped database write.
- `clients/swift/Sources/Synchro/SynchroClient.swift:129-134` exposes the table-scoped observation API.
- `clients/swift/Tests/SynchroTests/SynchroClientTests.swift:464-525` exercises writes only after subscription returns.

A write between the initial query and observer registration has no registered callback.
The watch can retain its previous result until another write happens.
The unconditional fanout also makes metadata writes repeat unrelated application queries.

**Smallest coherent simplification:** Give initial snapshot acquisition and subscription one coordinated observation owner.
Use committed table-change information to select observers.
Remove ignored table parameters only if the primary explicitly changes the public observation contract.

**Invariant:** A committed relevant update must not disappear between initial observation and ongoing notifications.
Callbacks must not run inside a database transaction that permits unsafe reentry.

**Acceptance:** Run `make test-swift-unit` with a write during the initial callback.
The watch must subsequently expose that committed value without another write.
An unrelated-table write must not repeat the watched query under the retained table-scoped API.
This is a static interleaving proof. No concurrency test ran.

<a id="06-swift-d01"></a>

### 06-swift-D01: Define one startup completion rule

**Severity:** Medium. Mixed completion rules duplicate startup scheduling and complicate caller cancellation.

**Classification:** Contract decision.

`SyncEngine.swift:496-632` combines initial synchronization, retry budgeting, lifecycle arming, and caller completion.
Push failures resolve startup immediately, while connect, pull, and rebuild failures can keep it waiting through several delays.
This difference requires `StartupGate`, `gateResolved`, `deferredCycles`, and separate startup retry control flow.

The public contract calls startup an arming operation at `docs/src/content/docs/spec/02-client-contract.mdx:924-926`.
A simpler product contract could return after durable lifecycle arming and report initial completion through the existing event/callback surface.
That would remove the mixed retry-budget completion policy, not durable retry or initial-sync guarantees.
The primary must decide the public API behavior and compatibility cost.
F05 still requires a correctness fix under the current behavior.

**Invariant:** Preserve durable work, typed errors, cancellation, and an observable initial-sync completion event.

**Acceptance:** After approval, run `make test-swift-unit` and `make test-swift-scenarios` against the selected completion rule.
Static review cannot approve this public API change.

<a id="06-swift-d02"></a>

### 06-swift-D02: Define retention and compaction for completed client history

**Severity:** Medium. Completed payload and schema copies grow with lifetime mutation history.

**Classification:** Contract decision.

`ChangeTracker.swift:736-787` retains every source action and creates additional normalized records.
`PushProcessor.swift:336-344,488-499,585-625` retains complete request, pending, schema, and successor-batch history.
`Internal/SynchroMeta.swift:161-178` also retains schema copies.
The inspected production source contains no deletion path for completed batches, accepted ledger rows, or archived schemas.

This is distinct from F11's avoidable historical lookup cost.
The contract requires immutable intent and sealed history but does not define a bounded client-history retention policy in the inspected sections.
See `docs/src/content/docs/spec/02-client-contract.mdx:166-169,224-234,267-275,315-320`.

The primary should decide when terminal client history may compact after every dependency and replay obligation ends.
A coherent policy could retain exact unresolved work and required outcomes while removing redundant terminal payload copies.
It must preserve mutation-ID nonreuse and every required diagnostic or historical-schema reference.
Do not delete sealed payloads, terminal evidence, or schema archives based only on age.

**Invariant:** Retain exact unresolved requests, dependency links, required outcomes, and every referenced historical manifest.

**Acceptance:** Define the retention policy before implementation.
Then run `make test-swift-unit` and `make test-swift-scenarios` with restart and replay across each compaction boundary.
No compaction behavior was executed or approved here.

<a id="06-swift-d03"></a>

### 06-swift-D03: Bound ordinary SQL syntax instead of building a second SQL interpreter

**Severity:** Medium. Partial SQL inference duplicates grammar and creates a second source of authored-field decisions.

**Classification:** Contract decision.

`ApplicationDatabase.swift:425-715` maintains a custom lexer and partial INSERT, UPDATE, DELETE, and WITH parser.
The explicit authored-write API already supplies the information this parser tries to infer at `SynchroClient.swift:67-95`.

The primary should define which ordinary SQL forms receive automatic authored-column inference.
Complex SQL could require the explicit authored transaction, while unsupported ordinary forms fail atomically.
This can reduce parser scope without losing field-presence semantics.
It must not reduce the resolved-object protection supplied by the SQLite authorizer.
F03's silent missing-capture path is not an acceptable compatibility fallback.

**Invariant:** Preserve exact authored fields, absent-versus-null values, and atomic local capture.

**Acceptance:** After approval, run `make test-swift-unit` with every documented supported form and rejected-form negative controls.
Static review identifies the cost but does not approve removal of supported SQL behavior.

<a id="06-swift-d04"></a>

### 06-swift-D04: Separate durable rebuild recovery receipts from historical verification payloads

**Severity:** Medium. Full page-body retention duplicates materialized data and makes state capture decode historical payloads.

**Classification:** Contract decision.

`Database.swift:714-732` stores complete request and response JSON for rebuild pages.
`PullProcessor.swift:359-379,509-519,965-974` uses exact receipts for replay and retains completed receipts after scope removal.
`SynchroClient.swift:497-780` reimplements substantial receipt verification for inspection.

The primary should decide whether full historical response bodies must remain in every production database after finality.
Compact exact-body hashes can detect byte-different replay, but cannot replace row-level historical inspection without changing that contract.
Any change requires an explicit proof-ownership and retention decision.
Do not remove atomic applied-page receipts or weaken exact replay to semantic JSON equality.

**Invariant:** Preserve atomic page completion, exact replay detection, and independent historical proof wherever the approved contract requires it.

**Acceptance:** After approval, run `make test-swift-unit` and `make test-swift-scenarios` with byte-different replay and process-restart controls.
The primary must assign any removed historical proof to a remaining authoritative evidence path.


<a id="area-07-kotlin"></a>

## Kotlin SDK and tests

<a id="07-kotlin-f01"></a>

### 07-kotlin-F01: Public transaction objects outlive their ownership boundary

**Severity:** High. An escaped transaction permits writes without the wrapper transaction, capture-context cleanup boundary, or commit notification.

**Classification:** Correctness defect.

**References**

- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroDatabase.kt:848-870`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroDatabase.kt:1003-1044`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroDatabase.kt:1261-1329`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroClient.kt:71-93`
- `docs/src/content/docs/spec/02-client-contract.mdx:190-195,934-937`

**Evidence and cost**

The callback receives an object that retains `SQLiteDatabase`. The object has no active-scope or owning-thread check.
The generic callback can return that object. For example, `val escaped = client.transaction { it }` preserves a usable write handle.
`escaped.execute(...)` subsequently calls SQLite directly. It does not enter `owner.writeTransaction` or publish the recorded changes.
Its `triggerSetValidated` flag also survives the original transaction.

The read callback has a related ownership gap. `applicationReadTransaction` does not set `applicationTransactionDepth`.
Consequently, the lifecycle guard does not reject a lifecycle call inside this application transaction.
This contradicts the explicit callback rule and can reintroduce opposite database and lifecycle lock acquisition.

**Smallest coherent simplification**

Give application transaction objects one callback-scoped ownership check. Check activity and owning thread before each operation.
Invalidate that ownership in the callback's `finally` block. Apply the lifecycle depth guard to read callbacks too.
This removes the implicit lifetime assumption and the separate unguarded read-callback path.
Do not add a second SQL execution path for escaped objects.

**Invariant**

Application writes, capture context, durable intent, and notification ownership must remain within their owning transaction.
Lifecycle calls must fail before acquiring lifecycle locks when an application transaction is active.

**Acceptance**

Add escaped-read, escaped-write, cross-thread, and read-callback lifecycle negative cases to the public transaction tests.
Assert no row, queue, or context change after rejected use. Run checks K1 and K2 below.
Static review establishes the missing guards. This review did not execute the race or escaped-handle cases.

**Tracking:** Client contract transaction boundary. No exact issue match was established.

<a id="07-kotlin-f02"></a>

### 07-kotlin-F02: Authored capture context can silently suppress another table's update

**Severity:** High. A public SQL update can commit without its required mutation record.

**Classification:** Correctness defect.

**References**

- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroDatabase.kt:873-926,929-1001`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroDatabase.kt:1298-1322`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SQLiteSchema.kt:78-111,166-206`
- `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/AuthoredCaptureTests.kt:102-181`

**Evidence and cost**

`authoredWriteTransaction` installs one table and operation context before invoking an unrestricted transaction callback.
`withDefaultCaptureContext` returns immediately whenever any context exists. It does not compare the statement's table or operation.
The capture trigger requires the context's table name to match its own table.

An authored context for table A can therefore contain `UPDATE B SET ...` for a different synced table B.
The target and trigger checks pass. B's authored-field predicates are false, so its update trigger captures nothing.
The operation member is persisted but does not constrain the executed statement.

**Smallest coherent simplification**

Bind explicit capture context to each executed statement's target and operation.
Reject a mismatch before executing SQL, unless the primary approves a defined multi-table authored-write contract.
Remove the unconditional context-present bypass.
Keep support-column omission behavior. Do not require every supplied SQL column to be an authored field.

**Invariant**

Every successful authored synced write must retain the intended field-presence semantics and atomic mutation capture.

**Acceptance**

Add a two-synced-table update case and an operation-mismatch case to `AuthoredCaptureTests`.
Assert rollback of application rows and capture context. Keep the existing support-column cases. Run K3.

**Tracking:** Local capture rules and the source's issue `#42` context reference. No exact existing defect match was established.

<a id="07-kotlin-f03"></a>

### 07-kotlin-F03: Schema tests preserve a second migration implementation that production does not use for schema changes

**Severity:** High. Tests report migration coverage while bypassing the durable migration path and its ownership checks.

**Classification:** Correctness defect in verification, with behavior-preserving cleanup of the obsolete path.

**References**

- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SchemaManager.kt:22-128,165-239,288-428`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SyncEngine.kt:1110-1130,1147-1181`
- `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/TestSchemaHelpers.kt:8-30,110-316`
- `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/SchemaManagerTests.kt:120-272,343-403,558-742`
- `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/LifecycleDurabilityTests.kt:26-78,365-469`
- `.r2-completion-tracker.md:174`

**Evidence and cost**

Production schema changes prepare and apply a typed journal.
The fallback reconciliation call runs when no migration exists. The normal `none` action first verifies the exact installed schema reference.
The fallback's `schemaChanged` branch therefore does not perform ordinary production schema transitions.

`installTestSchema` directly calls that fallback with arbitrary version, hash, and table values.
Many `SchemaManagerTests` use this setup helper as the operation under test.
The tests expect removed columns and tables to remain. The journal planner rejects retired columns and plans owned table removal.
`testDropSyncedTables` directly executes its own DROP statements and then checks their effects.

The compatibility fixture layer also creates `SchemaTable`, `SchemaColumn`, and constructor-shaped conversion functions.
It accepts obsolete fields such as `pushPolicy`, `dbType`, and `defaultKind` that do not drive the tested production contract.
This layer keeps obsolete tests looking source-compatible instead of exposing their different semantics.

**Smallest coherent simplification**

Separate raw fixture setup from the operation being verified.
Run migration assertions through `prepareConnectMigration` and `applyPreparedMigrationInTransaction`, or the public connect path.
Delete the fallback's schema-changing implementation after its production non-use is confirmed by the primary.
Retain a narrow exact-current-schema reconciliation path.
Replace compatibility fixture models with actual manifest or local-schema models.
Delete the test-authored DROP proof rather than treating it as implementation coverage.

**Invariant**

Preserve journal recovery, atomic schema activation, owned-object validation, local-only data, and all retained authored intent.

**Acceptance**

Run K4. Add a negative control that disables journal application and makes migration tests fail.
Keep the physical-object tampering and reopen cases. Run the affected real schema scenario through K9 after integration.

**Tracking:** Direct match to local tracker item R2-135, which requires removal of Kotlin journal bypasses.

<a id="07-kotlin-f04"></a>

### 07-kotlin-F04: Blocking-error cleanup belongs to the caller instead of the engine-owned operation

**Severity:** High. Automatic failures can leave `retry()` unable to restart the engine.

**Classification:** Correctness defect.

**References**

- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SyncEngine.kt:122-132,147-164`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SyncEngine.kt:333-374,397-409`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SyncEngine.kt:424-456,615-659`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SyncEngine.kt:1294-1313,1365-1374,1483-1501`
- `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/SyncEngineTests.kt:2587-2625`

**Evidence and cost**

The cycle records a blocking error but does not terminate lifecycle ownership.
Only the `syncNow()` caller catches that failure and invokes `terminateAfterFailedSync`.
Foreground work schedules the same deferred operation without that caller.
The periodic loop and debounce path catch ordinary exceptions without clearing `started`.
A caller that cancels its waiter also loses this cleanup path.

After such a failure, `retry()` acknowledges durable error state and then reaches an already-true `started` flag.
It throws `AlreadyStarted` instead of recovering.
The existing recovery test exercises only the attached `syncNow()` caller.

**Smallest coherent simplification**

Handle terminal cycle failure once at the engine-owned execution boundary.
Remove caller-dependent teardown and redundant failure wrappers.
Retain generation checks and cancel-and-drain guarantees when consolidating ownership.

**Invariant**

Caller cancellation must not cancel durable work. Every blocking failure must stop normal work and permit its defined explicit recovery.

**Acceptance**

Add automatic, foreground, debounce, and detached-waiter failures to K2.
Assert one error publication, durable failure retention, no later automatic request, and successful explicit retry.
Use deterministic transport failures and controlled scheduling. No runtime reproduction ran in this review.

**Tracking:** Client contract `02-client-contract.mdx:842-847`. No exact issue match was established.

<a id="07-kotlin-f05"></a>

### 07-kotlin-F05: The read SQL boundary misses SQLite single-quoted table names

**Severity:** Medium. Public reads can expose reserved metadata despite the explicit reserved-object restriction.

**Classification:** Correctness defect.

**References**

- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/ApplicationSql.kt:35-63,117-133,182-195`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroDatabase.kt:834-841,1265-1272`
- `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/ApplicationSqlBoundaryTests.kt:19-63`
- `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/SynchroClientTests.kt:760-769`

**Evidence and cost**

The lexer represents every single-quoted token as `Literal`.
Reserved-name validation checks only `Identifier` tokens, while the SELECT parser accepts the statement head without resolving table names.
Thus `SELECT * FROM '_synchro_meta'` passes the source-level authorization checks.
An in-memory SQLite check returned the reserved table's row for this exact query.

The existing negative tests use bare reserved names. They do not cover SQLite's contextual quoted-name interpretation.

**Smallest coherent simplification**

Use one explicit rule for object-name positions in the accepted SQL subset.
Reject string-literal tokens in those positions, rather than treating them as safe because they are quoted.
Keep legitimate literals in expressions and parameters.

**Invariant**

Application SQL cannot access reserved objects through alternate quoting or nesting.

**Acceptance**

Add single-quoted FROM and JOIN cases, including a nested SELECT, to K5.
Keep a positive case containing reserved-name text as an ordinary string value.
The host SQLite reproduction is not an Android API 24 test.

**Tracking:** Existing SQL-boundary tests. No exact issue match was established.

<a id="07-kotlin-f06"></a>

### 07-kotlin-F06: The exact-trigger comparator changes quoted token contents

**Severity:** High. The guard can accept a trigger whose capture predicates differ from the generated trigger.

**Classification:** Correctness defect.

**References**

- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SQLiteSchema.kt:78-92,259-273,285-313`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroDatabase.kt:1364-1405`
- `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/SchemaIntegrationTests.kt:174-217`
- `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/LifecycleDurabilityTests.kt:375-409`

**Evidence and cost**

`canonicalDDL` collapses whitespace everywhere, including quoted identifiers and string literals.
Generated capture SQL embeds table names, field IDs, and protocol identities inside quoted tokens.
Changing `'a  b'` to `'a b'` changes a SQL value but produces the same comparator output.
A host SQLite check confirmed different trigger effects for those two strings.

The integration test already contains a quote-aware DDL normalizer.
Maintaining a second, weaker production normalizer defeats the guard's claim of exact generated trigger validation.

**Smallest coherent simplification**

Preserve all quoted token contents when normalizing DDL.
Use one quote-aware normalization implementation for this shared meaning, with independent expected test results.
Delete the whitespace-only implementation.

**Invariant**

Formatting differences may compare equal. A changed identifier, literal, predicate, or trigger action must not compare equal.

**Acceptance**

Add a generated trigger containing whitespace in a logical identity.
Change only that quoted value and require rejection before application SQL executes. Run K5 and K4.
Static comparison and host SQLite establish the defect. Android execution remains required.

**Tracking:** Exact client-owned trigger validation. No exact issue match was established.

<a id="07-kotlin-f07"></a>

### 07-kotlin-F07: Two production tables define the same lifecycle graph

**Severity:** Medium. Each lifecycle change requires synchronized edits to two independent transition authorities.

**Classification:** Behavior-preserving cleanup.

**References**

- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SyncEngine.kt:1508-1545,1604-1677`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroMeta.kt:209-242,331-412`
- `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/Issue49RequirementProofTests.kt:898-1026`
- `docs/src/content/docs/spec/02-client-contract.mdx:891-909`

**Evidence and cost**

`LEGAL_TRANSITIONS` and `LEGAL_LIFECYCLE_ADJACENCY` repeat the same 11-state adjacency table.
`transitionTo` checks the in-memory copy and then calls metadata code that checks the durable copy.
These checks protect different states, but their permitted-edge rule has one meaning.

**Smallest coherent simplification**

Share one internal transition predicate or adjacency value between the engine and metadata layer.
Delete one production graph. Keep both state checks and the separate process-recovery exception.
Do not derive expected test edges from that production value.

**Invariant**

Illegal edges must fail before state mutation. Durable state and in-memory state must each satisfy the same contract.

**Acceptance**

Run K2 and K6. Change one permitted edge in a negative control and require the independent contract test to fail.

**Tracking:** Normative lifecycle graph. No exact issue match was established.

<a id="07-kotlin-f08"></a>

### 07-kotlin-F08: SQLite-to-wire conversion has a strict owner and a permissive duplicate

**Severity:** Medium. Schema activation can compute integrity from coerced SQLite values that normal pull verification rejects.

**Classification:** Correctness defect caused by duplicate behavior.

**References**

- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/PullProcessor.kt:1080-1160`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SchemaManager.kt:827-906`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/PushProcessor.kt:1308-1330`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/PullProcessor.kt:981-1030`

**Evidence and cost**

Pull checksum verification checks SQLite storage type, integer range, Boolean domain, and nullability.
Schema checksum recomputation uses a different `wireValue` function.
It converts any nonzero integer to Boolean true and reads `int` through `getInt` without the range check.
The digest then verifies the converted value, not the invalid stored representation.

Push and pull also maintain separate wire-to-SQL conversion switches with different validation assumptions.
Those assumptions are not visible in a shared type or function contract.

**Smallest coherent simplification**

Give the shared SQLite portable-value mapping one implementation with explicit validation.
Use it in pull verification and schema recomputation first. Delete the permissive duplicate.
Consolidate reverse conversion only where both callers require the same accepted value domain.
Do not introduce a generic codec framework.

**Invariant**

Canonical portable values must retain their exact meaning. Invalid SQLite storage must fail rather than become canonical through coercion.

**Acceptance**

Add Boolean value `2`, out-of-range `int`, and wrong-storage-class migration cases.
Require rollback of schema, cursor, and checksum changes. Run K4 and K7.

**Tracking:** Portable-value and integrity rules. No exact issue match was established.

<a id="07-kotlin-f09"></a>

### 07-kotlin-F09: Record loading decomposes bounded sets into repeated single-row queries

**Severity:** Medium. Large queues and scopes require avoidable SQLite calls while holding transaction ownership.

**Classification:** Behavior-preserving cleanup.

**References**

- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/PushProcessor.kt:327-337,570-592,1073-1096`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/ChangeTracker.kt:94-132,175-206,291-318`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/PullProcessor.kt:943-978,1033-1059,1080-1091`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroMeta.kt:971-998`

**Evidence and cost**

Three push loaders first select mutation IDs and then call `changeByID` for every result.
A 100-member selection therefore requires 101 SELECT statements before field-value reads.
Inspection similarly loads each mutation's values separately.
Scope checksum computation performs protection, row, and version queries for each unprotected member.

The seed-scope query already demonstrates set-based metadata loading with a LEFT JOIN.
The cost is a source-derived query count, not a measured latency claim.

**Smallest coherent simplification**

Select complete mutation records in the ordered membership or eligibility query.
Load field values for a bounded mutation set together.
For scope verification, group by physical table and join metadata without removing typed validation.
Delete ID-list materialization and repeated lookup loops.
Keep SQL compatible with SQLite 3.9.2 and Android API 24.

**Invariant**

Preserve durable order, missing-member detection, exact identity, row protection, and snapshot consistency.

**Acceptance**

Run K7 and K8. Add query-count evidence for a bounded 100-mutation case and a multi-table scope.
Use the real API 24 cell for the changed SQL before acceptance.

**Tracking:** Related to the query-loop concern in `#54`, but this finding is Kotlin-local, not server-side.

<a id="07-kotlin-f10"></a>

### 07-kotlin-F10: The bounded capture API bounds receipt groups, not their materialized contents

**Severity:** Medium. A nominally small capture can allocate every page and row from a large rebuild.

**Classification:** Correctness defect in the inspection bound.

**References**

- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroClient.kt:258-329,372-411,477-542`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroMeta.kt:1060-1100`
- `clients/kotlin/conformance-app/src/androidTest/kotlin/com/trainstar/synchro/conformance/NativeSession.kt:469-529,1137-1144`
- `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/InspectionTests.kt:536-555`

**Evidence and cost**

`captureState(maximumRecords)` selects at most that many rebuild groups.
The JOIN then returns every page in each selected group without a page, row, or byte limit.
The client decodes all bodies and builds identity, checksum, entry, and expected-chain collections.
The test explicitly accepts two receipts and three returned rows under `maximumRecords = 1` without receipt truncation.

The runner's one-megabyte response check occurs after these allocations.
Its database-size guard also runs after `captureState` returns.

**Smallest coherent simplification**

Define and enforce a bound on materialized receipt facts before decoding bodies.
Return exact counts and an explicit omitted-detail state when the bound is exceeded.
If full-chain proof is required, compute a bounded streaming summary instead of returning parallel per-row lists.
Delete unbounded detail construction from the general bounded capture operation.

**Invariant**

Overflow must remain explicit. A truncated capture must never appear to prove a complete receipt chain.

**Acceptance**

Add one rebuild group with more pages and rows than the detail limit.
Assert bounded details, exact counts, explicit truncation, and no false successful chain proof. Run K10 and K11.

**Tracking:** Inspection facade bound. API result-shape changes require the primary's shared-facade review.

<a id="07-kotlin-f11"></a>

### 07-kotlin-F11: Completed rebuild receipts retain whole row payloads without a bounded retention rule

**Severity:** Medium. Repeated rebuilds and removed scopes can retain complete historical snapshots indefinitely.

**Classification:** Contract decision.

**References**

- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/PullProcessor.kt:391-420,457-494,683-713,753-775`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroMeta.kt:1166-1213`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroDatabase.kt:495-515`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroClient.kt:385-542`
- `docs/src/content/docs/spec/02-client-contract.mdx:691-728`

**Evidence and cost**

Each page receipt stores complete request and response JSON, including row payloads.
Finality deletes the active attempt but keeps every page receipt.
Starting another attempt deletes receipts only when an active attempt still exists.
Removing a scope also preserves its completed receipts.
The removal comment says a later rebuild clears that scope's receipts, but the new-attempt code does not do this after finality.

Active exact replay requires durable page identity. It does not itself explain indefinite retention of every completed response body.
Inspection currently creates an additional consumer of that historical content.

**Smallest coherent alternative for decision**

Define the completion and retention boundary explicitly.
Consider retaining compact completion identity and content fingerprints after the exact-replay window closes.
Keep full active-page receipts until recovery and replay no longer require them.
Move any longer audit history to an explicit opt-in policy rather than making every local database an unlimited archive.
This could delete historical full-body storage and its repeated inspection decoding.

**Invariant**

Do not remove active recovery state, exact replay checks, verified finality, or required independent proof.
No deletion or new retention policy is approved by this report.

**Acceptance**

After a contract decision, add repeated rebuild, reassignment, restart, and changed-page replay cases to K7 and K9.
Measure retained rows and bytes after the selected retention boundary.

**Tracking:** Rebuild page and finality contract. The contradictory source comment is directly verified.

<a id="07-kotlin-f12"></a>

### 07-kotlin-F12: HTTP error decoding discards canonical codes before durable failure mapping

**Severity:** Medium. Distinct authentication, retired-client, and integrity errors become generic server errors with a generic recovery action.

**Classification:** Correctness defect caused by duplicate interpretation.

**References**

- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/HttpClient.kt:289-384,552-576`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/Errors.kt:19-20`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SyncEngine.kt:1376-1463`
- `clients/kotlin/conformance-app/src/androidTest/kotlin/com/trainstar/synchro/conformance/NativeSession.kt:624-672`
- `docs/src/content/docs/spec/02-client-contract.mdx:799-821,949-951`

**Evidence and cost**

The HTTP layer decodes canonical error bodies but preserves only status and message for several errors.
The engine then maps every `ServerError` to `SERVER_ERROR` with `RETRY` recovery.
Declared failure codes such as `AUTHENTICATION_REQUIRED`, `CLIENT_RETIRED`, and `SYNC_INTEGRITY_FAILURE` are not selected here.
The runner separately reconstructs error facts from status and operation, creating another interpretation table.

**Smallest coherent simplification**

Carry the validated canonical code through the transport error type.
Map that code once to durable native failure and recovery facts.
Keep runner observations factual. Do not invent a canonical error solely from an HTTP status when the body did not provide one.
Delete the status-only reconstruction where canonical facts exist.

**Invariant**

Preserve server error identity and non-retryable handling. Keep diagnostics bounded and exclude payload data.

**Acceptance**

Add canonical 400, 401, `client_retired`, and 500 error cases through HTTP and durable status.
Assert exact stable codes and no automatic retry. Run K2 and K12.

**Tracking:** Canonical error dispatch. Existing tests currently expect generic 500 behavior and need contract-based correction.

<a id="07-kotlin-f13"></a>

### 07-kotlin-F13: Issue-named proof suites repeat behavior tests and contain ineffective negative controls

**Severity:** Medium. Parallel proof implementations increase maintenance while some claimed mutants cannot expose a production defect.

**Classification:** Behavior-preserving verification cleanup.

**References**

- `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/Issue49CompleteRequirementProofTests.kt:170-288,476-594,735-809`
- `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/Issue49RequirementProofTests.kt:218-270,435-602,605-775,998-1007`
- `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/PushProcessorTests.kt:569-677,1110-1153`
- `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/PullProcessorTests.kt:960-1002,1520-1635`
- `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/SyncEngineTests.kt:371-429,1016-1072`
- `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/IntegrationTests.kt:146-197`

**Evidence and cost**

The two issue-named suites repeat queue normalization, exact retry, reopen, and protected scope cleanup flows already present beside the processors.
They retain separate environments, schema builders, mock servers, and snapshots.
Some added assertions are useful, but another complete suite is not necessary to retain them.

The queue identity mutant changes a copied observation and asserts that it differs from the original.
The lifecycle mutant changes a test-local adjacency map, then separately tests unchanged production behavior.
Neither alteration reaches production or the asserted acceptance predicate for the claimed behavior.

Mocked successful connect, rebuild, and pull flows also remain in `SyncEngineTests` despite the real integration and native scenario paths.
This creates parallel protocol scripts for happy paths that the repository policy assigns to real integration.

**Smallest coherent simplification**

Assign each behavior one proof home. Move unique rollback or recovery assertions into that home, then delete the duplicated flow.
Keep mocks for deterministic transport failures, timing, and impossible responses.
Remove test-local inequality mutants. Mutate the production behavior or the actual acceptance observation instead.
Do not delete distinct fault boundaries merely because their fixtures look similar.

**Invariant**

Retain exact replay, dependent intent, process recovery, and meaningful negative controls.

**Acceptance**

The primary must compare the shared scenario proof map before deletion.
Run K6, K7, K8, and the affected real scenario in K9.
Demonstrate that disabling exact-request replay or scope isolation fails the retained proof.

**Tracking:** Tests explicitly refer to issue `#49`. Current issue status was not queried.

<a id="07-kotlin-f14"></a>

### 07-kotlin-F14: The authored digest-vector consumer rejects malformed JSON in test-only code

**Severity:** Medium. A vector can pass because the test harness rejects its input before production receives it.

**Classification:** Correctness defect in proof ownership.

**References**

- `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/IntegrityTests.kt:197-236,392-408,443-516`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/Integrity.kt:190-210,461-543`

**Evidence and cost**

The row-digest vector path calls `parseStrictObject` before `Integrity.rowDigestPreimage`.
Its private scanner rejects duplicate top-level keys.
The invalid-vector loop counts any thrown exception as successful rejection.
Thus this malformed-input proof can succeed in the test scanner without exercising the production wire validator.

Production already has duplicate-member, Unicode, depth, and canonical-number validation.
The second scanner adds a different accepted language solely for the test path.

**Smallest coherent simplification**

Pass raw vector JSON through the actual production JSON boundary before decoding the row.
Delete `parseStrictObject`, its duplicate-key scanner, cursor, and scanning helpers.
Keep authored expected bytes and digests independent from production output.

**Invariant**

An invalid vector must fail because the implementation under test rejects it, not because setup rejects it.

**Acceptance**

Run K13. Disable duplicate-member rejection in a negative control and require the applicable raw-input proof to fail.
The separate `canonicalWireJsonValidation` test is useful and must remain independent.

**Tracking:** Related proof-ownership concern to `#36`. This report does not claim an exact issue-scope match.

<a id="07-kotlin-f15"></a>

### 07-kotlin-F15: Unconsumed compatibility and test-support paths remain in production

**Severity:** Low. Dead paths expand maintenance and preserve tests for behavior that the sync engine never calls.

**Classification:** Behavior-preserving cleanup for internal symbols. Public symbol removal requires compatibility review.

**References**

- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/ChangeTracker.kt:208-272`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/PullProcessor.kt:29-36,663-668`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/HttpClient.kt:107-125,145-146,189-200`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroMeta.kt:1148-1164`
- `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/ChangeTrackerTests.kt:100-109`
- `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/PullProcessorTests.kt:352-369`
- `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/ScenarioFixtureLoader.kt:1-59`
- `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/ScenarioFixtureLoaderTests.kt:1-28`
- `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/SchemaIntegrationTests.kt:101-106`

**Evidence and cost**

Exact symbol searches found these internal paths without production consumers:

- Legacy `hydratePendingForPush`, whose only caller is one unit test.
- Global numeric `updateCheckpoint`, whose only callers are its forward-only unit test.
- `clearAllForTesting`, `clearTableForTesting`, and their otherwise-unused `clearAllInTransaction` helper.
- `clearAllScopeState`, `getFinalRebuildPageReceipt`, and the unused `pushSealed` overload.
- `ScenarioFixtureLoader`, whose only consumer is its own loader test, not an executed SDK scenario.

`fetchSchema` has only test consumers in this scope. It preserves GET transport wrappers and the old `SchemaResponse` model.
The client contract expressly uses inline connect manifests rather than a separate runtime schema fetch.
However, `HttpClient` and `SchemaResponse` are public, so external source compatibility cannot be settled by repository search alone.

**Smallest coherent simplification**

Delete the unconsumed internal methods and their self-only tests.
Move retained value assertions to mutation inspection or the existing actual push test.
Delete the unused scenario loader pair without deleting the authored scenario.
Use connect to obtain manifests in integration setup.
Ask the primary to decide compatibility before removing public legacy models or methods.
Keep seed-required checkpoint metadata and historical database migrations.

**Invariant**

Preserve per-scope opaque continuation, durable migration, exact sealed requests, and authored scenario execution.

**Acceptance**

Repeat exact symbol searches and run `make test-kotlin-unit` after the bounded deletions.
Any public removal also needs `make build-kotlin-library` and packaged consumer validation.

**Tracking:** Related to known unused-probe issue `#102` and local tracker R2-135. Exact issue membership requires primary confirmation.

<a id="07-kotlin-f16"></a>

### 07-kotlin-F16: Temporary memory diagnostics run on every production rebuild page

**Severity:** Low. Unconditional diagnostic work adds log noise and native heap sampling to normal synchronization.

**Classification:** Behavior-preserving cleanup.

**References**

- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/PullProcessor.kt:316,389,409-419,477-479`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SyncEngine.kt:1015-1027`

**Evidence and cost**

Nine `Log.i("SynchroMemory", ...)` statements sample native heap usage around ordinary rebuild work.
They have no opt-in setting. Exact repository searches found no checked-in consumer for this tag.
Several samples occur inside the apply transaction.

**Smallest coherent simplification**

Delete the temporary diagnostic calls.
If operational memory telemetry is required, define its bounded use and opt-in contract separately.

**Invariant**

Do not change rebuild apply, checksum verification, retry, or finality behavior.

**Acceptance**

Require zero `SynchroMemory` references in production and run K7 and K2.

**Tracking:** Repository observability rules. No exact issue match was established.

<a id="07-kotlin-f17"></a>

### 07-kotlin-F17: Maven metadata has two local sources of truth

**Severity:** Low. Release metadata changes require matching edits in property defaults and publication DSL.

**Classification:** Behavior-preserving cleanup.

**References**

- `clients/kotlin/gradle.properties:5-16`
- `clients/kotlin/synchro/build.gradle.kts:60-84`

**Evidence and cost**

Group, artifact, name, description, URLs, license, developer, and SCM facts appear in both locations.
The publication DSL repeats literal values instead of referring to the declared properties.
This is duplicated release meaning, not necessary syntax across different languages or platforms.

**Smallest coherent simplification**

Choose one metadata source supported by the existing publishing plugin.
Delete redundant defaults or derive the DSL from the retained source.
Do not change signing, repository selection, coordinates, or dependency versions.

**Invariant**

The generated POM and release coordinates must remain unchanged.

**Acceptance**

The primary should compare generated POM artifacts before and after the change through the existing Kotlin publication gate.
`make build-kotlin-library` provides a focused build check, not POM equivalence proof.
No publication or Gradle configuration execution ran in this review.

**Tracking:** Distinct from version-catalog drift issue `#100`. No exact issue match was established.

<a id="07-kotlin-f18"></a>

### 07-kotlin-F18: The Kotlin README describes an obsolete implementation and a missing design file

**Severity:** Low. The SDK entry point directs consumers toward the wrong database layer and an unavailable document.

**Classification:** Behavior-preserving documentation cleanup.

**References**

- `clients/kotlin/README.md:1-7`
- `clients/kotlin/synchro/build.gradle.kts:36-49`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroDatabase.kt:66-91`
- `README.md:39-63,75-93,150-163`

**Evidence and cost**

The README says the SDK is planned and wraps Room.
The implemented SDK owns `SQLiteOpenHelper`, and the declared dependencies do not include Room.
The linked `clients/ARCHITECTURE.md` does not exist in the checked directory.

**Smallest coherent simplification**

Replace the obsolete status and Room description with a short pointer to the maintained client and architecture documentation.
Delete the broken design link rather than creating another architecture document.

**Invariant**

Keep native SQLite ownership and Android API 24 support explicit.

**Acceptance**

Static link-target and dependency review is sufficient for the text change. Run `make docs-build` if published documentation changes too.

**Tracking:** No exact issue match was established.

<a id="07-kotlin-f19"></a>

### 07-kotlin-F19: A negative retry count skips the initial sync cycle without an error

**Severity:** Medium. Accepted configuration can leave queued work untouched while startup reports initial completion.

**Classification:** Correctness defect.

**References**

- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroConfig.kt:7-29`
- `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SyncEngine.kt:545-553,615-665`

**Evidence and cost**

Configuration validates page and batch sizes but not `maxRetryAttempts`.
The initial attempt counter is zero. With `maxRetryAttempts = -1`, the retry loop never executes.
`lastError` remains null, so the function returns successfully without running push, pull, or rebuild.
Startup then invokes its completion callback.

**Smallest coherent simplification**

Reject a negative retry count at configuration construction.
Keep the initial attempt separate from retry allowance so invalid configuration cannot become a silent success path.
Remove the unreachable-success case rather than adding a fallback result.

**Invariant**

Zero retries still permits the initial attempt. Configuration must not silently disable required synchronization work.

**Acceptance**

Add negative-count rejection and zero-retry initial-attempt cases. Run K2.
Static control flow establishes the skipped operation. This review did not run startup with that configuration.

**Tracking:** Public-boundary validation and sync ordering. No exact issue match was established.

### Additional contract questions for the primary

#### Shipped inspection can change a received rebuild response

`TransportObservationCollector.overridePausedRebuildCursor` changes transport behavior rather than only observing it.

- Control state: `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/inspection/TransportObservation.kt:249-290`
- Production rewrite: `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/HttpClient.kt:273-285,440-463`
- Runner command: `clients/kotlin/conformance-app/src/androidTest/kotlin/com/trainstar/synchro/conformance/NativeSession.kt:438-442`

The source identifies `SCN-REBUILD-FORGED-CURSOR-001` as the consumer.
The current cost includes mutable fault state, a public proof control, production response rewriting, and separate observation versus applied-body identities.
Decide whether this capability belongs in the shipped artifact or a harness-owned transport fault boundary.
An alternative could inject the impossible response before the SDK receives it and delete the production rewrite path.
Keep the forged-cursor negative control and packaged-client execution. This review does not approve a replacement boundary.

#### Retained intent and completed history need explicit policy boundaries

F11 addresses completed rebuild bodies.
Accepted ledger rows, superseded batches, and schema archives also remain durable history in the inspected code.
The contract explicitly protects authored intent and rejected outcomes, so unconditional deletion would be incorrect.
Before adding cleanup, specify which resolved records remain required for replay, dependency resolution, inspection, and backup recovery.
Do not use a code-size target to decide that policy.

#### Public legacy API removal needs compatibility evidence

`PushRecord`, `SchemaResponse`, and `HttpClient.fetchSchema` expose older concepts outside the small primary SDK surface.
Repository consumers alone cannot prove that downstream applications do not use exported symbols.
The primary must decide deprecation or removal separately from the safe internal deletions in F15.

### Focused acceptance commands

These commands are proposed, not executed.
The Gradle arguments select debug unit tests and exclude the duplicate release unit-test task.
The Make target still parses JUnit results. The primary must run the applicable unfiltered integrated gate before acceptance.

| Check | Proposed command |
| --- | --- |
| K1 | `make test-kotlin-unit GRADLE_TEST_ARGS=':synchro:testDebugUnitTest --tests com.trainstar.synchro.SynchroClientTests -x :synchro:testReleaseUnitTest'` |
| K2 | `make test-kotlin-unit GRADLE_TEST_ARGS=':synchro:testDebugUnitTest --tests com.trainstar.synchro.SyncEngineTests -x :synchro:testReleaseUnitTest'` |
| K3 | `make test-kotlin-unit GRADLE_TEST_ARGS=':synchro:testDebugUnitTest --tests com.trainstar.synchro.AuthoredCaptureTests -x :synchro:testReleaseUnitTest'` |
| K4 | `make test-kotlin-unit GRADLE_TEST_ARGS=':synchro:testDebugUnitTest --tests com.trainstar.synchro.SchemaManagerTests --tests com.trainstar.synchro.LifecycleDurabilityTests -x :synchro:testReleaseUnitTest'` |
| K5 | `make test-kotlin-unit GRADLE_TEST_ARGS=':synchro:testDebugUnitTest --tests com.trainstar.synchro.ApplicationSqlBoundaryTests -x :synchro:testReleaseUnitTest'` |
| K6 | `make test-kotlin-unit GRADLE_TEST_ARGS=':synchro:testDebugUnitTest --tests com.trainstar.synchro.Issue49RequirementProofTests --tests com.trainstar.synchro.Issue49CompleteRequirementProofTests -x :synchro:testReleaseUnitTest'` |
| K7 | `make test-kotlin-unit GRADLE_TEST_ARGS=':synchro:testDebugUnitTest --tests com.trainstar.synchro.PullProcessorTests --tests com.trainstar.synchro.SQLiteCompatibilityTests -x :synchro:testReleaseUnitTest'` |
| K8 | `make test-kotlin-unit GRADLE_TEST_ARGS=':synchro:testDebugUnitTest --tests com.trainstar.synchro.PushProcessorTests --tests com.trainstar.synchro.ChangeTrackerTests -x :synchro:testReleaseUnitTest'` |
| K9 | `make test-kotlin-scenarios KOTLIN_ANDROID_SERIAL="$KOTLIN_ANDROID_SERIAL"` |
| K10 | `make test-kotlin-unit GRADLE_TEST_ARGS=':synchro:testDebugUnitTest --tests com.trainstar.synchro.InspectionTests --tests com.trainstar.synchro.InspectionFacadeContractTests -x :synchro:testReleaseUnitTest'` |
| K11 | `make test-kotlin-instrumentation KOTLIN_ANDROID_SERIAL="$KOTLIN_ANDROID_SERIAL"` |
| K12 | `make test-kotlin-unit GRADLE_TEST_ARGS=':synchro:testDebugUnitTest --tests com.trainstar.synchro.HttpClientTests --tests com.trainstar.synchro.TransportObservationTests -x :synchro:testReleaseUnitTest'` |
| K13 | `make test-kotlin-unit GRADLE_TEST_ARGS=':synchro:testDebugUnitTest --tests com.trainstar.synchro.IntegrityTests -x :synchro:testReleaseUnitTest'` |

`KOTLIN_ANDROID_SERIAL` must identify the primary's selected support cell.
The primary owns prerequisite setup and execution. No device, service, or artifact was prepared by this review.


<a id="area-08-react-native"></a>

## React Native SDK, bridges, and examples

<a id="08-react-native-f01"></a>

### 08-react-native-F01: Native transaction failures can leave bridge promises pending

- Severity: High. A failed transaction can leave its JavaScript caller waiting without a terminal result.
- Classification: correctness defect.
- Problem: `clients/react-native/android/src/main/kotlin/com/trainstar/synchro/rn/SynchroModule.kt:488-539,558-620,677-692,1204-1222`.
- Native dependency: `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroClient.kt:71-79`.
- Native transaction entry: `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroDatabase.kt:848-870,1011-1043`.
- Current mocked coverage: `clients/react-native/__tests__/SynchroClient.test.ts:226-301`.

The bridge stores completion in two nullable `finalDeferred` variables with different lifetimes.
The outer variable receives the loop result only after `runTransactionLoop` returns normally.
The native database starts its transaction before it invokes the bridge callback.
If that startup fails, the catch block completes only the null outer deferred.
It never rejects the original begin promise. The session then disappears.

A second path affects commit during close.
The loop can receive a commit operation before `clearRuntimeState` aborts its session.
If abort wins before `completeNormally()`, line 608 throws after the inner variable receives the commit deferred.
The outer assignment never occurs, so the outer catch cannot reject that commit deferred.
The `commitTransaction` coroutine remains at `deferred.await()`.

The final range found the same transaction-entry failure on iOS.
`clients/react-native/ios/SynchroModule.swift:670-753` accepts `reject` but never uses it after dispatching native transaction entry.
Its catch blocks call `completeFinal`, whose callback remains unset until commit or rollback at lines 647-665.
An entry failure therefore leaves the original begin promise unresolved.
The native dependency uses throwing database entry at `clients/swift/Sources/Synchro/SynchroClient.swift:59-64` and `Database.swift:130-139,168-173`.
The iOS close-versus-commit branch explicitly rejects the received operation at lines 647-652.
Do not attribute the Android terminal-deferred race to that iOS branch.

Smallest coherent simplification:

- Give the existing transaction session one explicit owner for begin and terminal-operation completion.
- Reject the begin promise when native transaction entry fails before the callback begins.
- Preserve and settle the received terminal operation on every abort and exception path.
- Remove the two-variable completion handoff that depends on a successful loop return.

Preserve native transaction ownership, rollback on abort, read-only enforcement, and post-commit acknowledgement.
Do not remove the close-drain barrier or five-second inactivity limit.

Acceptance requires real bridge transaction-entry failure tests on both platforms and a controlled Android close-versus-commit interleaving.
Each call must settle exactly once. The failed transaction must not commit, and a later transaction must work.
Run the focused regression through `make test-rn-e2e-android DETOX_ARGS="e2e/sync.test.ts"` after adding those cases.
The Makefile forwards `DETOX_ARGS` at lines 1535 and 1563-1565.
Run the corresponding iOS entry-failure regression through `make test-rn-e2e-ios DETOX_ARGS="e2e/sync.test.ts"`.
The existing recovery action at `example/src/App.tsx:522-599` tests close before callback release, not the identified commit interleaving.
This review established static control-flow evidence only. It did not execute either failure path.
No matching issue was established from the supplied issue leads.

<a id="08-react-native-f02"></a>

### 08-react-native-F02: Detox journeys duplicate one coordinator protocol

- Severity: Medium. Protocol changes require repeated edits, and the copies already disagree on envelope validation.
- Classification: behavior-preserving cleanup.
- Existing issue match: `#93`, repeated React Native journey protocol.
- Compared consumers under `clients/react-native/example/e2e/`:
  - `pending-cycle.test.ts:5-142,180-210`
  - `push-response-loss.test.ts:5-110,122-199`
  - `queue-replay.test.ts:5-111,149-174`
  - `retention-reconnect.test.ts:5-134,170-200`
  - `schema-check.test.ts:5-153,177-200`
  - `forged-cursor.test.ts:5-75`
  - `seeded-empty-startup.test.ts:5-117,138-238`
  - `steady-pull.test.ts:5-153,176-214`
  - `warm-connect.test.ts:5-150,173-203`
- Existing shared operations: `clients/react-native/example/e2e/corpus-harness.ts:5-33`.

Each journey repeats coordinator configuration, response decoding, authenticated exchange, result polling, and sequence completion.
These are the same transport contract, not separate synchronization behaviors.
The existing helper shares launch, input submission, and teardown but leaves the protocol copies intact.

The copies differ in observable ways:

- `push-response-loss.test.ts:105-106` requires string error details for error envelopes.
- `schema-check.test.ts:146-149` also permits null error details.
- `pending-cycle.test.ts:104-110` does not validate error-detail type.
- `forged-cursor.test.ts:50-53` checks only the parsed outcome before returning the raw envelope.
- `seeded-empty-startup.test.ts:218-228` counts commands separately from the final exchange.
- Other journeys count the completion exchange inside `stageCount`.

Smallest coherent simplification:

- Share coordinator configuration, the canonical envelope decoder, HTTP exchange, and command-result polling in the existing harness area.
- Remove the corresponding local copies from the named consumers.
- Keep scenario assertions, expected errors, restart boundaries, command deadlines, and completion counts explicit at each call site.
- Preserve the existing coordinator wire contract. Do not silently normalize different count meanings.
- Use bounded diagnostics that exclude command bodies, response bodies, and runtime credentials.

Preserve exact sequence checks, nonzero command execution, raw result forwarding, process-restart evidence, and unconditional app termination.
Do not replace real coordinator assertions with mocked synchronization success.

Acceptance requires shared decoder negative controls for missing members, wrong sequence, invalid completion, and invalid error details.
Then run representative existing consumers through these supported targets:

```text
make test-rn-unit
make test-rn-pending-cycle-ios
make test-rn-forged-ios
make test-rn-queue-replay-ios
make test-rn-seeded-empty-startup-ios
```

Retain the remaining journey gates when integrating the shared implementation.
This finding has static comparison evidence only. No journey gate ran during this review.

<a id="08-react-native-f03"></a>

### 08-react-native-F03: The Xcode scheme references a missing test target

- Severity: Low. The shared scheme advertises a test bundle that the project does not declare.
- Classification: behavior-preserving cleanup.
- Reference: `clients/react-native/example/ios/SynchroReactNativeExample.xcodeproj/xcshareddata/xcschemes/SynchroReactNativeExample.xcscheme:25-42`.
- Project declarations: `clients/react-native/example/ios/SynchroReactNativeExample.xcodeproj/project.pbxproj:109-158`.
- Actual Detox entry point: `clients/react-native/example/.detoxrc.js:3-19` and `example/e2e/jest.config.js:2-11`.

The scheme includes `SynchroReactNativeExampleTests.xctest` with blueprint identifier `00E356ED1AD99517003FC87E`.
The complete project defines only the application target `13B07F861A680F5B00A75B9A`.
No project object resolves the test reference.

Remove the stale `TestableReference` instead of adding an empty XCTest target.
Preserve the application build target and the Detox/Jest test entry points.
Static acceptance requires every remaining scheme blueprint reference to resolve to a declared project target.
The primary can confirm unchanged native harness execution through `make test-rn-e2e-ios`.
This review did not invoke Xcode. No matching issue was established from the supplied leads.

<a id="08-react-native-f04"></a>

### 08-react-native-F04: iOS schema decoding can trap instead of rejecting invalid input

- Severity: High. An invalid JavaScript schema value can terminate the application process.
- Classification: correctness defect.
- Boundary: `clients/react-native/src/SynchroClient.ts:875-896`.
- Decoder: `clients/react-native/ios/SynchroModule.swift:1977-1997`.
- Callers: `clients/react-native/ios/SynchroModule.swift:1192-1231`.

The public JavaScript methods serialize columns and options without runtime shape checks.
Swift force-casts the parsed JSON container and each required column string.
For example, a JavaScript caller can pass a column without `name`.
JSON parsing succeeds, but `item["name"] as! String` traps outside Swift error handling.
TypeScript declarations do not validate runtime JavaScript input.

Replace the forced casts with one checked decoder that throws a stable boundary error.
Preserve native schema validation and leave the database unchanged on invalid input.
Do not add a second schema engine in JavaScript.
Acceptance: real iOS bridge calls with invalid containers and missing or wrongly typed fields must reject without process death.
Then verify a valid schema call still succeeds through `make test-rn-e2e-ios DETOX_ARGS="e2e/sync.test.ts"`.
Static evidence only. No crash reproduction ran.

<a id="08-react-native-f05"></a>

### 08-react-native-F05: Tagged query parameters defeat hook dependency stabilization

- Severity: Medium. Valid inline bytes or int64 parameters can cause repeated queries or observer registration.
- Classification: correctness defect.
- Implementation: `clients/react-native/src/hooks/useQuery.ts:13-46,53-112`.
- Supported values: `clients/react-native/src/types.ts:3-17`.
- Existing coverage: `clients/react-native/__tests__/hooks/useQuery.test.ts:20-125`.

The hook compares array elements with object identity.
An inline `{type: 'int64', value: '9223372036854775807'}` object therefore changes the dependencies on every render.
Query results update state, causing another render and another query with an equivalent parameter.
Reactive results can similarly replace the observer repeatedly.
Existing hook tests use no tagged parameters.

Compare the two supported tag shapes by type and canonical payload when stabilizing SQL parameters.
Keep ordinary string-array comparison for table names.
Do not add a generic deep-equality dependency or require every caller to memoize valid values.
Acceptance: equivalent newly allocated tags must cause one query or observer registration.
A changed tag payload must cause exactly one new operation. Verify with `make test-rn-unit`.
Static evidence only. No hook test ran.

<a id="08-react-native-f06"></a>

### 08-react-native-F06: The status hook can retain another client's status

- Severity: Medium. A newly mounted or replaced client can display a false lifecycle state until another event arrives.
- Classification: correctness defect.
- Implementation: `clients/react-native/src/hooks/useSyncStatus.ts:5-16`.
- Available snapshot and subscription: `clients/react-native/src/SynchroClient.ts:1012-1025,1128-1134`.
- Existing coverage: `clients/react-native/__tests__/hooks/useSyncStatus.test.ts:24-83`.

The hook sets `uninitialized` once and subscribes only to future events.
It does not read the current native status or reset the previous client's state when `client` changes.
The subscription method does not replay a snapshot.

Use the existing status getter when the client changes and preserve newer subscribed events over an older asynchronous snapshot.
Prevent the previous client's pending snapshot from updating the new client's state.
Preserve subscription cleanup and the true uninitialized state before initialization.
Acceptance: mount after native readiness, replace the client, and resolve old snapshots out of order through `make test-rn-unit`.
Static evidence only.

<a id="08-react-native-f07"></a>

### 08-react-native-F07: Inspection projection removes capture completeness evidence

- Severity: Medium. Consumers cannot directly distinguish complete evidence from bounded detail or a retained event suffix.
- Classification: correctness defect in inspection fidelity.
- Native flags: `clients/react-native/ios/SynchroModule.swift:1606-1611`.
- Dropped fields: `clients/react-native/src/SynchroClient.ts:513-575` and `src/types.ts:264-281`.
- Event removal: `clients/react-native/example/src/conformance/runner.ts:599-606,523-525,982-987`.
- Consumer: `conformance/reactnative/validation.go:121-138` and `rebuild_cardinality.go:1023-1034`.

Swift supplies explicit detail-truncation and aggregate-overflow fields, but the JavaScript projection omits them.
The runner also drops the oldest event after 256 entries without recording overflow.
Its later array-length check cannot detect events already removed.

The cardinality consumer intentionally permits 512 scope-row details for larger captures.
Therefore, rejecting every bounded preview would change correct consumer behavior.
Preserve explicit completeness information through the existing inspection path instead.
Use that information where an assertion requires complete evidence.
For events, fail capture on overflow or expose explicit suffix semantics through a primary-approved contract change.
Delete the silent assumption that a capped array necessarily represents the complete history.

Acceptance: a native overflow flag must remain observable after JavaScript decoding.
The 257th event must not produce a result indistinguishable from a complete 256-event history.
Retain the intentional large-cardinality preview and its total-count checks.
Use `make test-rn-unit` and `make test-rn-cardinality-ios` after the primary approves the completeness representation.
Static evidence only. No existing journey false pass is claimed.

<a id="08-react-native-f08"></a>

### 08-react-native-F08: The iOS wrapper retains an unsupported legacy bridge

- Severity: Low. A second declaration path increases drift and suggests support that the package does not provide.
- Classification: behavior-preserving cleanup.
- Duplicate path: `clients/react-native/ios/SynchroModule.mm:355-495`.
- Supported path: `clients/react-native/ios/SynchroModule.mm:15-353`.
- Support evidence: `clients/react-native/BUILD_TRACKER.md:5-7`, `package.json:88-90,114-125`, and `example/ios/Podfile:1`.

The fallback declares an `RCT_EXTERN_MODULE` while the supported implementation uses Codegen and `SynchroModuleImpl`.
The fallback already names `fieldIDs` where the supported implementation names `columnNames`.
Remove the fallback declarations and redundant architecture branching for this New Architecture-only package.
Retain the necessary Objective-C++ Codegen wrapper and Swift delegate.
Acceptance: `make test-rn-ios-parity` and the existing iOS bridge smoke must pass without adding legacy compatibility behavior.
Static evidence only.

<a id="08-react-native-f09"></a>

### 08-react-native-F09: A coordinator validation error can disclose runtime credentials

- Severity: Medium. Malformed coordinator responses can copy command credentials into test failure output.
- Classification: correctness defect at a diagnostic boundary.
- Sink: `clients/react-native/example/e2e/schema-queued-mutation.test.ts:24-28`.
- Credential-bearing command: `clients/react-native/example/src/conformance/types.ts:47-61,192-228`.
- Consumer: `clients/react-native/example/src/conformance/runner.ts:577-580`.
- Safe comparison: `clients/react-native/example/e2e/schema-check.test.ts:74-102,123-125`.

The invalid-response error interpolates the full raw coordinator response.
That response contains the command object, whose runtime can contain `auth_token`.
A valid command under an invalid sequence or envelope therefore places the token in the thrown error.

Remove raw payload interpolation. Report bounded sequence, field names, and status facts as the schema-check consumer already does.
Preserve strict rejection and diagnostic operation context.
Acceptance: feed a malformed envelope containing a synthetic credential marker and confirm the error excludes that marker and the body.
Run that negative control through `make test-rn-unit` and retain `make test-rn-sqm-ios`.
Static evidence only. No real credential was used or disclosed by this review.

<a id="08-react-native-f10"></a>

### 08-react-native-F10: The timeout rollback test contains no write to roll back

- Severity: Medium. The test can pass if timeout commits transaction contents.
- Classification: correctness defect in test proof.
- Assertion: `clients/react-native/example/e2e/sync.test.ts:164-166`.
- Action: `clients/react-native/example/src/App.tsx:506-520`.
- Distinct close recovery: `clients/react-native/example/src/App.tsx:522-599`.

The action opens an empty transaction, waits six seconds, and accepts a timeout code or matching text.
It never writes a row or checks rollback and subsequent lock release.
The separate close recovery action exercises another abort cause and does not prove timeout behavior.

Strengthen this existing action instead of adding another overlapping smoke test.
Write a sentinel before inactivity, require the typed timeout, verify the sentinel is absent, and perform a later successful transaction.
A negative control that commits the timed-out write must fail this proof.
Preserve the five-second inactivity contract and run both platform smoke targets through Make.
Static evidence only. No mutant or smoke ran.

<a id="08-react-native-f11"></a>

### 08-react-native-F11: The error-mapping smoke accepts an unknown error code

- Severity: Medium. Broken native error normalization can pass the packaged bridge smoke assertion.
- Classification: correctness defect in test proof.
- Assertion: `clients/react-native/example/e2e/sync.test.ts:196-198`.
- Action: `clients/react-native/example/src/App.tsx:847-854`.
- Mapping: `clients/react-native/src/errors.ts:399-400,442-443`.

The nonexistent-table operation passes whenever the caught value has any nonempty code.
The generic `UNKNOWN` fallback satisfies that assertion.
Replace the generic predicate with the expected `DatabaseError` and stable `DATABASE_ERROR` code.
Keep this real boundary proof and remove the permissive predicate.
A negative control that maps the native database error to `UNKNOWN` must fail.
Run `make test-rn-e2e-ios DETOX_ARGS="e2e/sync.test.ts"` and its Android counterpart after the change.
Static evidence only.

<a id="08-react-native-f12"></a>

### 08-react-native-F12: Subscriber tests repeat the same routing proof

- Severity: Low. Equivalent assertions require parallel maintenance without protecting another boundary.
- Classification: behavior-preserving cleanup.
- Repeated tests: `clients/react-native/__tests__/events.test.ts:140-165`.
- Stronger retained flow: `clients/react-native/__tests__/SynchroClient.test.ts:380-415`.
- Implementation: `clients/react-native/src/SynchroClient.ts:1128-1134`.

The retained flow already verifies two subscribers, removal of one subscriber, and continued delivery to the other.
Delete the weaker standalone removal and fan-out tests, or move the retained flow into the event suite.
Do not delete malformed-event tests, observer-ID isolation, or cleanup-failure coverage.
Acceptance: `make test-rn-unit` must retain one failing negative control for removal and one for subscriber isolation.
Static comparison only.

<a id="08-react-native-f13"></a>

### 08-react-native-F13: An unused Turbo task layer remains installed

- Severity: Low. The project retains an unused task configuration and platform-specific dependency family.
- Classification: behavior-preserving cleanup.
- Declaration: `clients/react-native/package.json:85` and `turbo.json:1-43`.
- Lock entries: `clients/react-native/yarn.lock:2772-2812,9077-9104`.
- Actual scripts: `clients/react-native/package.json:38-46` and `example/package.json:5-14`.
- Supported validation consumers: `Makefile:1063-1075`.

No inspected package script invokes Turbo.
An exact search of Makefiles, shell scripts, package configuration, workflows, and Markdown found no Turbo invocation.
It found only the dependency, schema URL, and unrelated `turbo-module` template metadata.

Remove the Turbo dependency and `turbo.json`, then regenerate the lockfile to remove only newly unreferenced entries.
Keep Make, Bob, the React Native CLI, and the real native build tasks.
Acceptance: `make lint-rn`, `make test-rn-unit`, and `make test-rn-native-parity` retain their existing behavior.
Static consumer evidence only. No dependency installation or regeneration ran.

<a id="08-react-native-f14"></a>

### 08-react-native-F14: Published Android configuration prefers uncontrolled local Maven artifacts

- Severity: Medium. The same dependency coordinate can resolve to machine-local bytes instead of the intended published SDK.
- Classification: behavior-preserving cleanup of development dependency selection.
- Repository order: `clients/react-native/android/build.gradle:17-21,76-85`.
- Example order: `clients/react-native/example/android/build.gradle:25-30`.
- Publication inclusion: `clients/react-native/package.json:21-36`.
- Local development consumer: `Makefile:1069-1072,1577-1581`.
- Release invariant: `RELEASE.md:91-94,120-128`.
- Existing issue match: `#101`, local Maven substitution.

The library ships its Android build file and enables `mavenLocal()` before Maven Central without an explicit development option.
The source parity target has a legitimate local consumer because it publishes the current Kotlin SDK before compiling the bridge.
That need does not require every installed consumer to search arbitrary local Maven contents.

Make local SDK resolution an explicit repository-development configuration with an exact artifact source.
Remove the unconditional local repository from the shipped default path.
Preserve same-version native SDK selection and the source parity workflow.
Acceptance: the source parity Make target must still compile the intended Kotlin artifact.
A clean package consumer with a same-coordinate local artifact must resolve and verify the intended sealed or public bytes instead.
The primary owns package-cell execution through `make release-run-support-cell` with the approved cell and sealed inputs.
Static repository-order evidence only. No actual substitution was executed.

<a id="08-react-native-f15"></a>

### 08-react-native-F15: The build tracker duplicates release evidence without identity

- Severity: Low. Unqualified PASS entries can be mistaken for current verification and require manual synchronization.
- Classification: behavior-preserving cleanup.
- Duplicate ledger: `clients/react-native/BUILD_TRACKER.md:9-24`.
- Authority: `RELEASE.md:87-102,149-153`.

The tracker records PASS and IN PROGRESS values without source SHA, workflow identity, commands with results, or artifact identity.
The release procedure already defines the authoritative gates and required evidence.
An exact documentation/configuration search found no external reference to the tracker.
Delete the redundant status table or replace the file with a short pointer to release evidence.
Preserve the requirement for both native platform cells.
Static acceptance: one release authority remains, and no unqualified verification table remains in package documentation.
No executable validation is needed for this documentation deletion.

<a id="08-react-native-f16"></a>

### 08-react-native-F16: Android idempotency errors lose their native code in JavaScript

- Severity: Medium. Callers cannot distinguish the emitted idempotency conflict from an unknown failure.
- Classification: correctness defect.
- Native mapping: `clients/react-native/android/src/main/kotlin/com/trainstar/synchro/rn/SynchroModule.kt:117`.
- JavaScript decoder: `clients/react-native/src/errors.ts:15-37,339-445`.
- Contract: `docs/src/content/docs/spec/02-client-contract.mdx:949-967`.

The Android bridge emits `IDEMPOTENCY_CONFLICT`.
The JavaScript code union and switch have no matching case, so `mapNativeError` returns `UNKNOWN`.
This is a concrete mismatch, not a request to preserve arbitrary unknown codes.
Align the native and JavaScript mappings under the existing typed protocol or blocking-error representation.
Delete the unmatched mapping rather than adding a second conflict policy.
Preserve the canonical idempotency code and native recovery decision.
Acceptance: the emitted Android conflict must survive the bridge with its stable typed meaning.
Use `make test-rn-unit` for the mapping regression and the applicable existing Android response-loss journey for integration.
Static mapping evidence only. The primary must select the canonical error representation before implementation.

All findings without an explicit issue match need primary issue-ledger reconciliation.

### Contract decisions

No deletion or requirement change is approved.
F07 requires the primary to choose an explicit completeness representation before changing the inspection contract.
Preserve intentional bounded previews and full-count checks during that decision.
F16 requires the primary to select the existing canonical error representation for the Android idempotency case.
Do not introduce a new conflict or retry policy.
The reporting contact at `clients/react-native/CODE_OF_CONDUCT.md:62-64` remains a template placeholder.
It requires an owner-approved contact method. No contact information is invented by this review.
No other product requirement reduction is proposed.


<a id="area-09-blackbox-runtime"></a>

## Blackbox runtime and observers

<a id="09-blackbox-runtime-f01"></a>

### 09-blackbox-runtime-F01: Native capture substitutes controller state for server evidence

**Severity: High.** The observer can hide incorrect versions, checksums, membership generations, retained rows, and premature WAL progress.

**Classification:** correctness defect.

**References**

- `conformance/blackbox/native_controller.go:2271-2293` deletes or replaces the controller's record model after authored DML.
- `conformance/blackbox/native_controller.go:3131-3144` returns `source_transaction_predecessor_pending` without observing PostgreSQL.
- `conformance/blackbox/native_controller.go:3633-3677` constructs stream and transaction facts from controller bindings.
- `conformance/blackbox/native_controller.go:3680-3735` constructs row and scope facts from the controller's selected records.
- `conformance/blackbox/native_controller.go:3801-3848` validates row fields, but not the emitted authored version or checksum.
- `conformance/blackbox/integration/real_native_capture_test.go:247-261` expects these authored values in captured facts.
- `conformance/scenarios/server/wal-order-001.json:1023-1039` declares the same predecessor error that the controller manufactures.

**Observed behavior and cost**

The row query reads a runtime version and checksum. Capture validates their shapes, then emits `record.Image.Version` and `record.Image.Checksum` instead.
Every scope receives `MembershipGeneration: 1`. Row count comes from the controller's record list, not the complete relevant database relation.
Deleting a controller record also removes its later observation. A retained server row can therefore disappear from the reported state.
Transaction capture ignores bindings without `Materialized`. The predecessor branch reports the modeled rejection before any server query.

These paths implement a second state machine inside the observer. They are not independent observations of the extension.
The existing edge and mutation-identity queries are stronger. They read actual durable sets and must remain.

**Smallest coherent simplification**

Keep generated-identity bindings, but remove predicted row lifecycle and generated success or rejection facts from capture.
Read complete scenario-owned row, scope, transaction, and progress sets in the existing read-only snapshot.
Apply only explicit identity substitutions to observed values. Compare those facts with authored expectations outside the observer.
Use a real worker barrier and durable state observation for predecessor blocking.

**Invariant:** PostgreSQL alone determines durable server state and WAL progress. An observer must expose unexpected extra state.

**Acceptance**

- Extend the existing native-capture integration proof with changed checksums, changed versions, advanced membership generations, extra rows, and retained deleted rows.
- Each injected difference must change observed facts or fail the comparison.
- A worker-order mutant must fail even when the controller's modeled order remains correct.
- Proposed commands: `make test-blackbox-harness` and `make test-blackbox GO_TEST_ARGS='-v -count=1 -p 1 -run ^TestRealNativeCaptureServerObservationSignals$'`.
- The current static review did not execute these controls.

**Traceability:** Matches the oracle-replacement concern in open issue `#36`. Applicable requirements include `SYNC-WAL-003`, `SYNC-WAL-005`, and `SYNC-INTEGRITY-001`.

<a id="09-blackbox-runtime-f02"></a>

### 09-blackbox-runtime-F02: Native realization infers fixture semantics instead of declaring them

**Severity: Medium.** Fixture changes require changes to table selection, scope inference, value conversion, schema rebinding, and their private tests.

**Classification:** contract decision. This concerns the authored test-operation contract, not approval to change production semantics.

**References**

- `conformance/blackbox/native_controller.go:597-865` infers scopes from assignments and write policies, then reselects shared tables.
- `conformance/blackbox/native_controller.go:897-1055` chooses preferred fixture names and changes a string field into a JSON field.
- `conformance/blackbox/native_controller.go:517-557,1268-1321` recognizes specific authored scope and relation names to choose membership operations.
- `conformance/blackbox/native_controller.go:2486-2504` falls back to the first sorted scope when no row rule exists.
- `conformance/blackbox/native_controller.go:2507-2591` implements separate DML recipes for the selected fixture names.
- `conformance/blackbox/native_controller.go:1704-1717` encodes one authored string as a JSON string stored inside another wire string.
- `conformance/blackbox/native_controller_test.go:174-328,394-446` tests this translation machinery.
- `conformance/blackbox/testdata/schema.sql:11-19,73-82` already defines the concrete application fixtures.

**Observed behavior and cost**

This mapping does more than replace generated identifiers. It selects ownership policy, table storage, field types, and scope behavior.
A membership stage outside the recognized name combinations can return success without applying a membership change.
The preferred-table list and sorted-scope fallback make an apparently generic scenario depend on hidden fixture choices.
The JSON field conversion adds a special path to application writes, pushes, source DML, capture validation, and schema transitions.

**Smallest coherent simplification**

Let each executable scenario declare its concrete fixture and required setup recipe.
Use the same physical field types and scope rules in authored operations and runtime fixtures.
Retain explicit aliases only for genuinely generated identities.
Delete preferred-table search, shared-table correction, guessed scope assignment, special string-to-JSON conversion, and their translation-only tests.
Reject unsupported setup operations instead of returning success.

**Invariant:** A scenario must exercise its declared semantics. Fixture binding must not substitute another field type or membership rule.

**Acceptance**

- The primary must approve the revised scenario representation under `#36` before implementation.
- Rename authored scope labels without changing their meaning. Execution must not depend on the original spelling.
- Run `make test-conformance-scenarios`, `make test-conformance-drivers`, and the affected native scenario targets after migration.
- This review does not approve a new scenario language or a broader generic interpreter.

**Traceability:** Open issue `#36`, Phase R3 oracle replacement. The fixed scope model in `docs/src/content/docs/spec/00-principles.mdx:115-139` must remain.

<a id="09-blackbox-runtime-f03"></a>

### 09-blackbox-runtime-F03: WAL bindings can assign two authored transactions to one runtime transaction

**Severity: High.** The binding can conceal lost or collapsed source transactions in commit-order proof.

**Classification:** correctness defect.

**References**

- `conformance/blackbox/native_controller.go:2245-2251` records `SourceXID` only for event-free transactions.
- `conformance/blackbox/native_controller.go:3344-3435` resolves each nonempty transaction through the latest matching row operation.
- `conformance/blackbox/native_controller.go:3462-3479` accepts equal runtime commit positions for distinct authored commit positions.
- `conformance/blackbox/native_controller.go:3438-3459` already uses source transaction identity for the event-free case.
- `conformance/blackbox/native_controller.go:3650-3677` counts authored bindings after verifying each selected runtime transaction.

**Observed behavior and cost**

The query selects `ORDER BY event.commit_lsn DESC LIMIT 1` by relation, record, and operation.
Two updates to the same row can both select the latest update transaction.
The order check ignores `runtimeOrder == 0`, so it does not reject that collapse.
Capture can then report two authored transactions backed by the same runtime row.
Per-event lookup also adds a query loop where transaction identity could select the complete set.

**Smallest coherent simplification**

Record the actual source transaction identity for every controlled source transaction.
Resolve its complete WAL event set by that identity, then require a one-to-one authored-to-runtime transaction mapping.
For application pushes, use their observed request and fence identity instead of latest-row matching.
Delete the latest-event search and equality exception in order validation.

**Invariant:** Distinct committed source transactions retain distinct replay identities and strict relative commit order.

**Acceptance**

- Commit two same-operation updates to one row before resolving either transaction.
- Require two distinct runtime commit positions and the correct event set for each.
- Remove the first transaction's evidence as a negative control. The binding must fail instead of selecting the second.
- Proposed commands: `make test-blackbox-harness` and `make test-blackbox-wal` after adding the focused case.
- Static inspection establishes the ambiguous lookup. No collapsed-binding execution was performed.

**Traceability:** `SYNC-WAL-003`, `SYNC-WAL-009`, and issue `#36`.

<a id="09-blackbox-runtime-f04"></a>

### 09-blackbox-runtime-F04: Replay reconstruction replaces the caller's request with server-owned bytes

**Severity: High.** A changed batch context can become an unchanged replay before the server receives it.

**Classification:** correctness defect.

**References**

- `conformance/blackbox/native_controller.go:2815-2827` bypasses normal request rewriting when sealed bytes exist.
- `conformance/blackbox/native_controller.go:4432-4459` hashes only the authored mutation list.
- `conformance/blackbox/native_controller.go:4461-4503` selects a binding by authored batch ID and that mutation digest.
- `conformance/blackbox/process.go:6861-6883` reads the server's sealed request using only `batch_id`.
- `docs/src/content/docs/architecture/decisions/002-mutation-idempotency-and-conflicts.mdx:113-120,154-175` includes user, client, generation, and request schema in the batch fingerprint.

**Observed behavior and cost**

Changing `client_generation` or request schema leaves the mutation digest unchanged.
The helper then sends the original server-sealed request and removes the intended fingerprint difference.
The lookup also ignores the user and client parts of scoped batch identity.
This requires privileged database access to recreate a request that the harness or native transport already sent.

**Smallest coherent simplification**

Retain the actual outbound request at the transport observation point, keyed by complete scoped identity.
Replay that request only for an explicitly unchanged replay.
Send changed authored requests through normal identity translation without replacing their context.
Delete the mutation-only replay digest and `SealedPushRequest` reconstruction path.

**Invariant:** Equal complete batch fingerprints replay exactly. Changed batch context reaches the server and produces the required conflict.

**Acceptance**

- Replay equal mutations with changed client generation and changed request schema under the same batch ID.
- Require `409 idempotency_conflict` and no new source work.
- Exercise equal batch UUIDs under two different scoped client identities.
- Keep exact-response and no-new-side-effect checks for unchanged replay.
- Use `make test-blackbox-harness` and the affected native push scenario target after adding these cases.

**Traceability:** `SYNC-IDEMPOTENCY-001`. The broader independent-evidence work belongs with issue `#36`.

<a id="09-blackbox-runtime-f05"></a>

### 09-blackbox-runtime-F05: Native LSN parsing loses the 32-bit low-word boundary

**Severity: Medium.** Order and acknowledgement checks become wrong when PostgreSQL LSNs cross a high-word boundary.

**Classification:** correctness defect.

**References**

- `conformance/blackbox/native_controller.go:4152-4174` concatenates hexadecimal words and falls back to lexical comparison.
- `conformance/blackbox/native_controller.go:3471-3475,3637-3644` uses this parser for runtime ordering and progress checks.
- `conformance/invariants/cursor_monotonicity.go:484-497` correctly parses two 32-bit words and computes `high<<32 | low`.

**Observed behavior and cost**

The native parser converts `1/0` to hexadecimal `10`, which equals 16.
The correct value is 4,294,967,296. It must follow `0/FFFFFFFF`, not precede it.
The duplicate parser also uses arbitrary-precision arithmetic and silently accepts malformed positions through lexical comparison.

**Smallest coherent simplification**

Use strict unsigned parsing for authored decimal positions and PostgreSQL high/low positions.
Share the PostgreSQL parsing rule with the existing conformance consumer if the primary approves a common internal home.
Delete concatenation, `math/big`, and lexical fallback.

**Invariant:** Runtime LSN comparison must preserve PostgreSQL's unsigned 64-bit position order.

**Acceptance**

- Cover `0/FFFFFFFF < 1/0`, equivalent zero padding, invalid words, overflow, and authored decimal positions.
- Proposed commands: `make test-blackbox-harness` and `make test-invariants`.
- A static Python arithmetic reproduction confirmed the reversed comparison. It did not execute the Go implementation.

**Traceability:** `SYNC-WAL-003` and `SYNC-WAL-005`.

<a id="09-blackbox-runtime-f06"></a>

### 09-blackbox-runtime-F06: A normal WAL polling miss starts a ten-second diagnostic sampler

**Severity: Medium.** Normal asynchronous waits acquire large fixed delays and can exceed their cancellation deadline.

**Classification:** correctness defect.

**References**

- `conformance/blackbox/native_controller.go:3147-3158` repeatedly calls transaction resolution while waiting for WAL.
- `conformance/blackbox/native_controller.go:3403-3412` runs the diagnostic helper on every unresolved event.
- `conformance/blackbox/native_controller.go:4227-4344` creates a fresh background context and samples worker state six times.
- `conformance/blackbox/native_controller.go:4307-4322` adds five unconditional two-second sleeps.
- `conformance/blackbox/process.go:4630-4689` already supplies a bounded, single-query WAL diagnostic projection.

**Observed behavior and cost**

The comment says the resolution deadline has expired. The caller invokes the helper before deciding whether to retry.
A diagnostic pass that reaches worker sampling adds ten seconds of sleep, plus database work, even during healthy asynchronous materialization.
The helper discards the caller's context. Cancellation cannot interrupt its sleeps.

**Smallest coherent simplification**

Return a small unresolved-event error from the polling path.
Collect one bounded diagnostic snapshot only after the terminal wait fails.
Reuse the existing WAL diagnostic projection where it contains the required facts.
Delete background worker sampling and sleeps from transaction resolution.

**Invariant:** Polling remains responsive to its declared deadline. Terminal failures retain bounded operational evidence.

**Acceptance:** Add a delayed-event case and a canceled-wait case. Run `make test-blackbox-harness` and `make test-blackbox-wal`.
The static sleep cost is ten seconds per completed sampling pass. Earlier diagnostic errors can return before sampling.
No runtime timing claim is made.

**Traceability:** No verified matching issue. This affects native WAL-controller diagnostics, not production worker scheduling.

<a id="09-blackbox-runtime-f07"></a>

### 09-blackbox-runtime-F07: Two strict JSON validators enforce different Unicode rules

**Severity: Medium.** Malformed Unicode can become valid replacement characters before semantic or replay comparison.

**Classification:** correctness defect with a behavior-preserving consolidation opportunity for valid input.

**References**

- `conformance/blackbox/normalize.go:57-167` implements its own UTF-8, duplicate-member, and trailing-value checks.
- `conformance/internal/jsonstrict/decode.go:14-42,127-190` also rejects lone UTF-16 surrogate escapes.
- `conformance/internal/jsonstrict/decode_test.go:64-78` contains the malformed-surrogate controls.
- `conformance/blackbox/http_client.go:160-168` uses the weaker canonicalization path on HTTP responses.
- `conformance/blackbox/runner_test.go:41-75` covers unknown members, duplicates, and dynamic fields, but not surrogate replacement.

**Observed behavior and cost**

UTF-8 validation does not reject an ASCII JSON escape such as `\ud800`.
Go's decoder replaces a lone surrogate. The shared validator explicitly prevents that replacement, while the black-box validator does not.
Consequently, distinct malformed strings can normalize to the same replacement-character value.
Maintaining two recursive token walkers caused a concrete validation difference, not merely repeated syntax.

**Smallest coherent simplification**

Use one shared lexical validator for UTF-8, Unicode scalar values, duplicate members, and complete JSON consumption.
Keep separate root-shape policies where required.
Keep `DisallowUnknownFields` in the closed-response decoder. The existing `jsonstrict.Decode` intentionally permits partial object projections.
Delete the duplicate token traversal from `normalize.go`.

**Invariant:** Normalization must not repair malformed input or merge distinct wire values.

**Acceptance:** Reject lone high and low surrogates at the raw HTTP boundary. Accept valid pairs and escaped literal backslashes.
Run `make test-blackbox-harness` and `make test-conformance-contract` after consolidation.

**Traceability:** The Unicode scalar requirement appears in ADR 002 at lines 85-90. No verified matching issue.

<a id="09-blackbox-runtime-f08"></a>

### 09-blackbox-runtime-F08: Recorder bounds apply after attachment writes and outside their synchronization boundary

**Severity: Medium.** Rejected records can create unlimited orphan attachments. Concurrent equal bodies can cause false corruption failures.

**Classification:** correctness defect.

**References**

- `conformance/blackbox/recorder.go:147-191` writes both bodies before taking the lock and checking `MaxRecords`.
- `conformance/blackbox/recorder.go:216-249` exposes the final attachment name before writing and syncing its contents.
- `conformance/blackbox/recorder.go:273-293` immediately verifies any existing attachment.
- `conformance/blackbox/runner_test.go:113-124` checks the overflow error, but not filesystem effects.

**Observed behavior and cost**

After metadata reaches its limit, each unique rejected request can still create two files.
Two concurrent calls with the same body can race between `O_EXCL` creation and the first write.
The second call then reads an incomplete file and reports changed content.
The mutex gives a misleading partial guarantee because it protects only the metadata append.

**Smallest coherent simplification**

Serialize the complete record operation with the existing mutex.
Check capacity before storing either attachment.
Keep content-addressed deduplication and fail-closed content verification.
If multiple recorder instances may share a root, explicitly disallow that ownership or publish complete files atomically.

**Invariant:** Rejected capacity overflow stores no new evidence. Accepted concurrent records never observe partial attachments.

**Acceptance:** Extend the existing test with filesystem counts after repeated overflow and concurrent identical-body writes.
Run `make test-blackbox-harness`. No filesystem race reproduction was executed during this review.

**Traceability:** No verified matching issue. This is a harness evidence-boundary defect.

<a id="09-blackbox-runtime-f09"></a>

### 09-blackbox-runtime-F09: Owned-cluster cleanup restarts PostgreSQL to delete objects that disappear with the cluster

**Severity: Medium.** Cleanup adds a restart, database reconnections, catalog queries, SQL failures, and state flags without preserving additional resources.

**Classification:** behavior-preserving cleanup, subject to preserving existing failure diagnostics and ownership checks.

**References**

- `conformance/blackbox/process.go:6051-6077` keeps attached-cluster cleanup separate.
- `conformance/blackbox/process.go:6078-6127` performs topology cleanup, then stops and removes the owned cluster.
- `conformance/blackbox/process.go:6167-6204` deletes publication, slots, database, and roles.
- `conformance/blackbox/process.go:6254-6272` disables worker startup and restarts the owned postmaster during cleanup.
- `conformance/blackbox/process.go:6219-6232,6275-6395` implements the object-by-object deletion paths.
- `conformance/blackbox/process.go:6408-6435` removes the entire owned data directory and run directory afterward.

**Observed behavior and cost**

These SQL cleanup operations run only for a disposable, harness-owned cluster.
Attached runs use another branch and do not benefit from them.
Stopping the owned process group and removing its data directory already removes its databases, roles, publications, and slots.
The restart can introduce a new cleanup failure after the actual test has ended.
The flags and fallback database-drop logic maintain an object lifecycle that is immediately discarded.

**Smallest coherent simplification**

Stop the adapter, close owned connections, stop the owned PostgreSQL process group, and remove the private cluster directories.
Then restore extension files and release the installation lock under the existing safety conditions.
Delete the cleanup restart and redundant SQL topology teardown.
Do not change attached-cluster ownership or external lifecycle behavior.

**Invariant:** Never remove cluster files or restore shared extension files while an owned PostgreSQL process remains alive.

**Acceptance**

- Keep the unrelated-directory, retained-lock, and changed-artifact negative controls.
- Test normal shutdown, already-exited PostgreSQL, live child backends, and failed process termination.
- Run `make test-blackbox-harness` and `make test-blackbox-wal`.
- Static inspection proves the redundant lifecycle. No cleanup speed measurement was taken.

**Traceability:** No verified matching issue. Issue `#98` concerns different adapter command behavior and is not claimed here.

<a id="09-blackbox-runtime-f10"></a>

### 09-blackbox-runtime-F10: Synchronous command runners do not cancel their process groups

**Severity: Medium.** A timed-out helper can retain descendants and keep `Cmd.Run` waiting on inherited output pipes.

**Classification:** correctness defect.

**References**

- `conformance/blackbox/process.go:1585-1613` runs attached lifecycle commands directly with `CommandContext`.
- `conformance/blackbox/process.go:3705-3723` separately runs projection-bootstrap commands.
- `conformance/blackbox/process.go:6637-6665` separately runs other bounded commands.
- `conformance/blackbox/process.go:6703-6705` configures only `Setpgid`.
- `conformance/blackbox/process.go:6754-6785,6848-6859` already implements group signaling for owned long-running processes.
- `conformance/blackbox/process_test.go:624-652,857-903` tests output bounds and owned-process escalation, but not synchronous-runner descendants.

**Observed behavior and cost**

`CommandContext` uses process cancellation unless the caller changes `Cancel`. `Setpgid` does not change that cancellation action.
The synchronous runners set neither group cancellation nor a pipe-drain bound.
Their bounded output buffers limit bytes, not command lifetime or descendant lifetime.
Three command-launch paths duplicate this incomplete lifecycle behavior and similar diagnostic formatting.

**Smallest coherent simplification**

Give synchronous helpers one bounded process-group cancellation and wait policy.
Preserve separate stdout where the command returns structured JSON.
Delete duplicated cancellation and diagnostic assembly once the three concrete consumers use the same runner.

**Invariant:** Canceling a harness command must not leave its descendants or unbounded pipe readers running.

**Acceptance:** Add a helper that spawns a child holding stdout open. Cancel the parent and require bounded return plus complete group termination.
Run `make test-blackbox-harness`. This report makes a static lifecycle finding, not an executed process-leak claim.

**Traceability:** No verified matching issue.

<a id="09-blackbox-runtime-f11"></a>

### 09-blackbox-runtime-F11: The synthetic harness maintains a second protocol and assertion stack for its own self-tests

**Severity: Medium.** Scenario, reference-model, artifact-binding, HTTP-server, replay-cache, and assertion changes must remain synchronized without testing the real assertion path.

**Classification:** behavior-preserving cleanup of harness self-tests. Preserve useful negative controls at their actual proof homes.

**References**

- `conformance/blackbox/syntheticproof/synthetic.go:25-31,75-90,99-158` defines `/v3/execute`, custom envelopes, and another model-runner invocation.
- `conformance/blackbox/syntheticproof/synthetic.go:245-359` implements a response cache and six tailored mutations.
- `conformance/blackbox/syntheticproof/runner.go:178-275` runs the same model again and compares the synthetic response.
- `conformance/blackbox/syntheticproof/compare.go:54-109` contains semantic assertion logic used only by this synthetic runner.
- `conformance/blackbox/syntheticproof/runner.go:83-100,298-345` requires artifact and vector declarations for non-evidence self-tests.
- `conformance/blackbox/syntheticproof/runner_test.go:141-153` manufactures artifact bindings to satisfy those declarations.
- `conformance/cmd/synchro-conformance/main.go:107-201` repeats the fixture list and synthetic artifact-binding construction.
- `conformance/README.md:92-104` explicitly excludes this path from real server proof.
- `Makefile:469-470,498-505` runs the CLI self-test and black-box package separately, not the synthetic package's direct test.

**Observed behavior and cost**

The synthetic server and client both obtain their expected results from `modelrunner.RunScenario`.
The tailored comparator checks this invented protocol, not the real integration test's semantic assertions.
The package correctly disclaims authoritative server proof. That disclaimer does not justify a second end-to-end scenario runner for transport self-tests.
The synthetic artifact bindings identify no executed artifacts and are unused outside input bookkeeping.
`oneDuplicateDelivery` adds quadratic work to choose a different message for the same assertion category that the following comparison already rejects.

The direct synthetic test checks exact failure categories and private attachments.
The supported CLI test instead checks only semantic failure and fault application.
No inspected Make test target names `./blackbox/syntheticproof` or recursively tests `./blackbox/...`.

**Smallest coherent simplification**

Keep small loopback fixtures for the actual raw client, recorder, authentication header, and replay comparator.
Use fixed authored responses instead of running the full reference scenario twice.
Put semantic mutants through the actual real-proof assertions before deleting their synthetic counterparts.
Delete the custom execution protocol, scenario replay cache, fake artifact bindings, clock bookkeeping, and assertion-category mirror.
Remove the synthetic-only normalization path if it has no remaining consumer.

**Invariant:** Every required negative control must fail the same checker that accepts the corresponding real evidence.

**Acceptance**

- Preserve direct controls for malformed JSON, changed status, changed replay bytes, bounds, and secret exclusion.
- Demonstrate required semantic mutants against their real assertion homes.
- Run `make test-blackbox-harness` and `make test-conformance-scenarios` after replacement.
- If the package remains, add it to an appropriate structured Make gate before treating its direct tests as evidence.

**Traceability:** Related to issue `#36`. This is not a claim that current documentation presents synthetic output as server certification.

<a id="09-blackbox-runtime-f12"></a>

### 09-blackbox-runtime-F12: Two WAL-record observers duplicate the same query and return partially different facts

**Severity: Low.** A schema or correlation fix must update two SQL projections and scan loops.

**Classification:** behavior-preserving cleanup.

**References**

- `conformance/blackbox/process.go:5184-5310` observes only `cf_items` and fills all `WALPipelineObservation` progress fields.
- `conformance/blackbox/pull_rebuild_controls.go:127-227` repeats the event joins and scan loop with a table parameter.
- The generic path fills only three pipeline flags, leaving the other progress fields at zero values.
- `conformance/blackbox/native_artifact.go:240-257` consumes the generic method.
- `conformance/blackbox/integration/real_pull_rebuild_test.go:667` and `conformance/blackbox/integration/soak_harness_test.go:850` provide additional generic consumers.
- `conformance/blackbox/process.go:5090-5107,5162-5175` consumes the complete fixed-table result.

**Smallest coherent simplification**

Keep one table-parameterized observer with the complete result projection and existing table validation.
Change fixed-table callers to pass `cf_items`.
Delete the duplicated query, scan loop, and fixed-table entry point instead of adding another wrapper.

**Invariant:** Preserve event/fence correlation, strict bounds, exact acknowledgement checks, and slot-position checks.

**Acceptance:** Run `make test-blackbox-harness` and `make test-blackbox-wal`.
Check generic-table callers against the same complete result shape. No query timing claim is made.

**Traceability:** No verified matching issue. This is test-observer duplication, not issue `#54`'s production query-loop finding.

<a id="09-blackbox-runtime-f13"></a>

### 09-blackbox-runtime-F13: Dead private state and an unused HBA generator retain tests and lifecycle code

**Severity: Low.** Unused helpers and write-only state create false maintenance obligations.

**Classification:** behavior-preserving cleanup.

**References and consumer evidence**

- `conformance/blackbox/process.go:1126-1140` defines `workerHBAConfiguration`.
- Its only call is `conformance/blackbox/process_test.go:571-584`.
- Actual provisioning uses `provisionedHBAConfiguration` at `conformance/blackbox/process.go:1077-1124`.
- `conformance/blackbox/environment.go:70-76,96-104,308-308,336-336,377-419` computes and stores unused secret digests.
- `conformance/blackbox/process.go:6667-6675,6693-6699` stores `waitErr` under a mutex, but no caller reads either state.
- `conformance/blackbox/push_retention_controls.go:10-10` defines an unused client ID constant.
- `conformance/blackbox/native_controller.go:4111-4121` puts a one-consumer wrapper around `sha256.Sum256`.
- `conformance/blackbox/process_test.go:654-663` repeats a single JSON-tag check already required by the strict parser test at lines 602-620.

Repository-wide exact Go searches found no additional readers or callers for these private items.

**Smallest coherent simplification**

Delete the unused HBA generator and its generator-only test.
Delete secret digest fields and the unused digest return value from `loadSecretFile`.
Delete the unread process wait error and its dedicated mutex. Keep the completion channel and stop mutex.
Delete the unused constant and inline the single SHA-256 call.
Delete the redundant JSON-tag-only test.

**Invariant:** Keep the active SCRAM configuration, artifact hashes, process completion signaling, and strict command-result decoding.

**Acceptance:** Repeat the exact consumer searches, then run `make test-blackbox-harness` and `make lint-conformance`.

**Traceability:** This does not match issue `#102`, which names different packaged-consumer files and an import scanner.

<a id="09-blackbox-runtime-f14"></a>

### 09-blackbox-runtime-F14: Seed preparation sends 1,000 separate inserts inside one fixed fixture transaction

**Severity: Low.** Fixture setup performs 1,000 SQL executions and 1,000 affected-row checks for one deterministic batch.

**Classification:** behavior-preserving cleanup.

**References**

- `conformance/blackbox/native_artifact.go:20-23` fixes the fixture cardinality at 1,000.
- `conformance/blackbox/native_artifact.go:204-237` builds deterministic identities and executes each insert separately.
- `conformance/blackbox/native_artifact.go:240-257` then independently waits for WAL visibility.

**Smallest coherent simplification**

Build the existing deterministic ID and value arrays once.
Insert them through one parameterized set-based statement in the same restricted source transaction.
Require exactly 1,000 affected rows and retain the existing WAL-materialization wait.
Delete the per-row execution and affected-row branches.

**Invariant:** The same 1,000 fixture rows commit atomically and enter the normal WAL and authenticated seed-export paths.

**Acceptance:** Compare the exact generated row set, then exercise the existing native portable-seed scenario.
Run `make test-blackbox-harness` and `make test-swift-scenarios` after the focused fixture change.
The 1,000-call cost is static. No elapsed-time or throughput result is claimed.

**Traceability:** Portable-seed behavior remains unchanged. No verified matching issue.

<a id="09-blackbox-runtime-f15"></a>

### 09-blackbox-runtime-F15: Generic internal SQL errors count as configured-limit validation evidence

**Severity: Medium.** An internal failure on invalid input can satisfy the measurement without proving the configured-limit check ran.

**Classification:** correctness defect in proof classification. A new public SQL error contract would require a separate decision.

**References**

- `conformance/blackbox/configured_bounds_controls.go:58-96` treats every PostgreSQL `XX000` error as the expected rejected result.
- `conformance/blackbox/integration/real_configured_bounds_test.go:170-188,191-205` converts that result into an accepted invalid-bound sample.
- `conformance/blackbox/configured_bounds_controls.go:80-84` rolls back both successful and failed operations.

**Observed behavior and cost**

The helper's error classification cannot distinguish limit validation from another internal error or panic.
Lower and upper controls reject a wholly broken endpoint, but do not distinguish a defect restricted to the invalid-input branch.
Rollback also prevents the measurement from showing whether invalid input reached prohibited mutation work before failing.
The binding ledger correctly proves that a call occurred. It cannot strengthen the meaning of this weak terminal observation.

**Smallest coherent simplification**

Record a bounded, identifiable validation outcome where the existing API provides one.
Otherwise, describe this sample only as rejection evidence and add a negative control that distinguishes internal failure from intended validation.
Do not label generic `XX000` as proof of the validation path.
Do not add another measurement-binding layer to compensate for the missing observation.

**Invariant:** Harness or operational failures must fail the measurement rather than satisfy its expected semantic outcome.

**Acceptance:** Inject an unrelated internal failure only on the invalid-input branch. The measurement must reject that run.
Then run `make test-blackbox-configured-bounds` with valid and invalid authored samples.
No such mutant was executed in this review.

**Traceability:** Configured-bound measurement proof. No verified matching issue or approved new SQLSTATE requirement.

<a id="09-blackbox-runtime-f16"></a>

### 09-blackbox-runtime-F16: Alias-shape negative tests also fail when their shape checks are removed

**Severity: Medium.** The tests cannot establish that invalid declaration shapes are rejected by the intended validator.

**Classification:** correctness defect in test proof.

**References**

- `conformance/blackbox/native_identity_test.go:93-103` supplies no observations for unknown-kind and malformed-schema declarations.
- `conformance/blackbox/native_identity_test.go:117-129` also supplies no observations for every wrong-scalar declaration.
- `conformance/blackbox/native_identity.go:76-89` contains the declaration checks those tests intend to protect.
- `conformance/blackbox/native_identity.go:148-153` rejects every unresolved declaration with the same `ErrNativeIdentityEvidence` error category.
- `conformance/blackbox/native_identity_test.go:12-32` demonstrates the complete owner and observation setup that a valid control needs.

**Observed behavior and cost**

The tests assert only `errors.Is(err, ErrNativeIdentityEvidence)`.
Removing the intended kind or shape check still leaves a declaration with no runtime observation.
The later missing-observation check returns the expected error category, so the negative test remains green.
These cases add apparent validation coverage without proving the named rule.

**Smallest coherent simplification**

Provide otherwise complete, consistent owner observations for each invalid declaration case.
Change only the declaration property under test.
Keep one valid control with the same ownership and runtime evidence.
Remove any case that still fails for an unrelated missing-evidence reason.
If a corrected test exposes an implementation defect, fix the validator rather than weakening the test.

**Invariant:** Each negative control must fail because of its intended defect, not because its setup is incomplete.

**Acceptance:** Disable each intended declaration check in a temporary mutant. Its corresponding corrected test must fail.
Run `make test-blackbox-harness` after correction.
This static review did not execute a mutant or claim that every malformed value passes full resolution.

**Traceability:** Native identity evidence integrity and issue `#36`'s oracle-replacement boundary. No separate matching issue was verified.


<a id="area-10-blackbox-tests"></a>

## Blackbox integration and soak proofs

<a id="10-blackbox-tests-f01"></a>

### 10-blackbox-tests-F01: Soak client durability observations come from an in-memory test client

**Severity:** High. The soak can report client restart integrity without terminating or reopening a client database.

**Classification:** Correctness defect in verification.

**References**

- `conformance/blackbox/integration/soak_harness_test.go:35-59,149-166`
- `conformance/blackbox/integration/soak_harness_test.go:368-372,447-480,483-557`
- `conformance/blackbox/integration/soak_harness_test.go:867-945,1134-1200,1308-1317`
- Consumer: `conformance/invariants/no_state_forks.go:27-30,64-114`
- Contract: `docs/src/content/docs/spec/03-state-machines.mdx:317`

**Observed behavior and cost**

The soak maintains rows, memberships, cursors, and checksums in Go maps. It applies push, pull, and rebuild results itself.
`captureClient` converts these maps into a complete `ClientDurabilityFact`. It assigns zero queue and outcome counts.
The database fingerprint hashes a constant client label. The process identity comes from an incrementing counter.
The process-death operation restarts the PostgreSQL WAL worker. It does not restart the Go client or a Swift or Kotlin client.
The harness then changes the synthetic client process ID and sets `RestartBoundary`.

The state-fork checker compares these supplied identities and snapshots. Its successful comparison cannot establish durable client recovery.
The unchanged Go maps satisfy the preservation condition by construction.
The capture also removes independent server rows, scopes, and scope edges before invariant evaluation.
For pull and rebuild, it supplies an empty server identity list instead of observing those identities.

This is not a complaint about an independent test oracle. A reference model can be useful.
The defect is labeling reference-model state as observed, complete client durability and process-replacement evidence.

**Smallest coherent simplification**

Keep the server protocol workload separate from native durability proof.
Remove fabricated client process identities, restart flags, complete durability claims, and constant queue facts from server-only captures.
Use an existing native runner for required client recovery observations.
Retain independent expected rows where they check server output, but do not present those rows as a native database snapshot.

This deletes the duplicate client-lifecycle simulation. It does not delete the real WAL restart control.

**Invariant to preserve**

Client recovery proof must observe the same durable database after a real client process replacement.
Server WAL replay proof must still verify exact replay and contiguous acknowledgement.

**Acceptance**

- Run `make soak SOAK_SEED=1 SOAK_DURATION=1s` after the observation boundary changes.
- Run the selected existing native recovery proof through its Make target.
- A control that changes or loses the native database must fail the state-fork invariant.
- A WAL worker restart alone must not create a client restart observation.

**Tracking:** Matches the oracle-replacement concern in open issue `#36`, especially Phase R3. This report does not approve phase resequencing.

<a id="10-blackbox-tests-f02"></a>

### 10-blackbox-tests-F02: Soak combines different HTTP exchanges to construct cursor proof

**Severity:** High. The cursor proof cannot distinguish issued progress from acknowledged progress reliably.

**Classification:** Correctness defect in verification.

**References**

- `conformance/blackbox/integration/soak_harness_test.go:405-444,560-601`
- `conformance/blackbox/integration/soak_harness_test.go:989-1045`
- `conformance/blackbox/integration/soak_harness_test.go:1415-1436,1454-1468`
- Consumer: `conformance/invariants/cursor_monotonicity.go:65-138,141-184`
- Stronger existing observation pattern: `conformance/blackbox/integration/real_baseline_test.go:1174-1191`

**Observed behavior and cost**

`executePullControl` records the data pull and its later acknowledgement separately.
It then calls `wireFromCalls(operation, ackCall, mainCall, ...)`.
The resulting exchange contains the acknowledgement request and the earlier data response. That exchange never occurred.
`bind` assigns both the pull result and cursor acknowledgements to this combined exchange sequence.

`captureCheckpoints` also assigns each raw response cursor the position read from the current durable checkpoint.
The invariant later compares that supplied position with the same durable checkpoint position.
This does not independently establish the cursor's issued position or the checkpoint position before acknowledgement.

`bindWireTarget` further changes recorded request bodies to carry observation identity.
Identity metadata belongs beside the captured bytes, not inside a reconstructed request presented as wire evidence.

**Smallest coherent simplification**

Keep each request, response, and sequence from one recorder exchange.
Bind the data result to `mainCall` and acknowledgements to `ackCall`.
Observe checkpoints before and after the acknowledgement request.
Keep authenticated identity as observation metadata.
Remove the two-call exchange constructor and request-body rewriting.

**Invariant to preserve**

The server must not acknowledge a selected cursor before the client presents it.
The checker must compare independent issued-cursor and durable-checkpoint observations.

**Acceptance**

- Run `make soak SOAK_SEED=1 SOAK_DURATION=1s` and `make test-invariants`.
- Verify that every reported exchange resolves to one unchanged recorder request and response pair.
- A server control that acknowledges selected progress early must fail before the acknowledgement request.
- A control that ignores acknowledgement must fail after that request.

**Tracking:** Related to issue `#36`. Applies to cursor acknowledgement and monotonicity proof.

<a id="10-blackbox-tests-f03"></a>

### 10-blackbox-tests-F03: Soak applies push and pull fault plans to an unrelated connect request

**Severity:** High. Randomized fault coverage can pass without faulting the selected operation.

**Classification:** Correctness defect in verification.

**References**

- `conformance/blackbox/integration/soak_harness_test.go:306-342`
- `conformance/blackbox/integration/soak_harness_test.go:1320-1378`
- `conformance/blackbox/integration/soak_harness_test.go:1419-1436`
- Generator dependency: `conformance/soak/generator.go:166-196`

**Observed behavior and cost**

The generator attaches wire fault plans to selected operations, including push and pull.
The harness always executes those faults through a separate `/sync/connect` request.
It ignores that request's response and error. It sets `Activated` before sending the request.
It closes the fault transport before executing the actual selected operation through the healthy client.

The observation still labels the fault exchange with `operation.Kind`.
A push plan can therefore report a push fault while the faulted request was connect and the push remained healthy.
The extra connect also omits the returning client's generation. The normal connect helper correctly includes that generation.

**Smallest coherent simplification**

Install the fault transport around the selected operation's actual request.
Remove the preparatory connect request and unconditional activation claim.
Record activation only after the controller observes the intended boundary.
Keep cleanup reporting separate from activation reporting.

**Invariant to preserve**

Each fault must affect its declared operation and target. Replay must preserve the recorded operation and fault identity.

**Acceptance**

- Run `make soak SOAK_SEED=1 SOAK_DURATION=1s` after adding a deterministic affected-operation control.
- For a push response-loss plan, observe one committed push and a lost response on that push.
- For a pull timeout plan, observe the timeout on the selected pull.
- Verify the recorded route and operation class agree.

**Tracking:** Related to issue `#36` and Layer 7 fault coverage. No runtime fault result was measured during this review.

<a id="10-blackbox-tests-f04"></a>

### 10-blackbox-tests-F04: Issue-specific suites repeat existing semantic proof implementations

**Severity:** Medium. Duplicate harness provisioning and copied assertions increase runtime and make correctness fixes diverge.

**Classification:** Behavior-preserving cleanup.

**References and concrete duplicates**

All paths below start with `conformance/blackbox/integration/`.

| Behavior | First implementation | Repeated implementation |
| --- | --- | --- |
| First-response failure rolls back 17 near-limit mutations | `real_issue49_data_semantics_test.go:945-1001` | `real_issue49_remaining_semantics_test.go:360-410` |
| Historical projection bootstrap with candidate catch-up | `real_baseline_test.go:25-176` | `real_issue49_remaining_semantics_test.go:1427-1515` |
| Maximum portable counter and overflow | `real_issue49_data_semantics_test.go:278-355` | `real_issue49_remaining_semantics_test.go:1245-1285` |
| Cross-client cursor rejection | `real_issue49_data_semantics_test.go:1238-1247` | `real_issue49_remaining_semantics_test.go:704-713` |
| String and integer key separation | `real_pull_rebuild_test.go:163-180` | `real_issue49_remaining_semantics_test.go:662-702` |
| Row and scope digest checks for a single inserted row | `real_mutation_controls_test.go:261-314` | `real_issue49_remaining_semantics_test.go:783-831` |
| Endpoint authority revocation and restoration | `real_issue49_wal_authority_test.go:988-1114` | `real_issue49_security_completeness_test.go:24-159` |
| Registered OID, key update, and replica identity drift | `real_issue49_wal_authority_test.go:331-466` | `real_issue49_security_completeness_test.go:163-344` |
| Finite health limit and public readiness | `real_issue49_wal_authority_test.go:470-524` | `real_issue49_security_completeness_test.go:348-534` |
| Installed authority and packaged SQL | `real_issue49_wal_authority_test.go:528-665` | `real_issue49_security_completeness_test.go:538-717,828-891` |
| Mixed push partition conservation | `real_push_retention_test.go:387-463` | `real_mutation_controls_test.go:171-259` |

Additional exact repetition occurs within `real_issue49_remaining_semantics_test.go:120-174`.
Two differently named wrappers call the same CRUD flow with different identifier strings.
The proof map explicitly maps many obligations to both implementations at `real_proof_map_test.go:46-63,65-78,93-105`.

**Observed behavior and cost**

These tests repeat actions and predicates, not merely syntax or fixture data.
For example, both atomicity tests construct 17 mutations and fill the request to one byte below the same limit.
Both then expect `sync_integrity_failure` and identical zero durable-work counts.
Changing one limit or protocol field requires two changes with no additional failure condition gained.

Some repeated suites contain useful extra assertions. Those differences do not justify preserving each complete copied flow.
The security endpoint copy adds additional unauthorized-input checks. Preserve those checks during consolidation.
The original mixed-outcome test checks durable state and distinguishes requested values from conflict values. Prefer that stronger proof.

**Smallest coherent simplification**

Select one authoritative executable proof for each behavior.
Move unique assertions into that proof, then delete the repeated flow and its dedicated setup.
Point all applicable requirements and mutants to the retained proof.
Do not replace duplicate flows with another issue-specific wrapper layer.

**Invariant to preserve**

Keep every distinct observable condition and demonstrated mutant failure. Delete only duplicate executions and assertions.

**Acceptance**

- Compare the retained assertions against every row above before deletion.
- Run affected exact mutation targets through `make test-blackbox-mutation-control`.
- Run the retained non-mutation proofs through `make test-blackbox` during primary integration.
- Preserve each mutant's semantic failure, not merely a setup failure.

**Tracking:** Closed issue `#49` added exact server bindings. Open issue `#36` requires one authoritative proof home.

<a id="10-blackbox-tests-f05"></a>

### 10-blackbox-tests-F05: Proof bookkeeping duplicates bindings and depends on subtest positions

**Severity:** Medium. Adding or deleting an earlier assertion can redirect many mutant targets without changing their stored names.

**Classification:** Behavior-preserving cleanup with a verification integrity risk.

**References**

- `conformance/blackbox/integration/real_issue49_remaining_semantics_test.go:24-56`
- `conformance/blackbox/integration/real_proof_map_test.go:20-44,46-129,273-326,337-445,448-532`
- `conformance/blackbox/integration/issue49_security_proof_bindings_test.go:14-42,44-138`
- Compared registry: `conformance/mutants/integration/manifest.json:20-50,62`
- Execution boundary: `Makefile:518-532`

**Observed behavior and cost**

The remaining-semantics suite gives all 31 subtests the name `assertion`.
The mutant manifest addresses them as `assertion`, `assertion#01`, through `assertion#30`.
The Make target explicitly allows only that positional naming scheme.
Deleting a duplicate test therefore renumbers later targets and forces widespread metadata changes.

`serverProofBindings` calls itself the sole server and fault proof map.
The security bindings repeat scenario, requirement, assertion, control, fault plan, obligation, patch, and test identities in another Go table.
The mutant manifest stores patch and test bindings again.

The source scanner checks declaration names, signatures, build availability, and classifications.
It does not inspect assertion bodies or bind a semantic assertion to a successful runtime observation.
An empty body with the same signature satisfies this static part of the map check.
This static check is useful bookkeeping, but it must not serve as semantic completeness evidence.

**Smallest coherent simplification**

Give retained subtests stable behavior names.
Use one authored binding registry for test targets and mutant targets.
Keep one small referential-integrity check for that registry.
Delete the issue-specific duplicate binding table and ordinal-dependent target restriction.
Continue to require structured execution and demonstrated semantic mutant failure.

**Invariant to preserve**

Every required assertion must have an explicit executable proof and a stable target.
Renaming or deleting a proof must fail referential validation.

**Acceptance**

- Reordering unrelated subtests must not change any mutant target.
- Removing a referenced named subtest must fail binding validation.
- Removing its semantic predicate must fail the applicable mutant gate.
- Run `make test-blackbox` and affected `make test-blackbox-mutation-control` targets after integration.

**Tracking:** Directly related to closed issue `#49`. This finding does not claim that every current mutant gate is ineffective.

<a id="10-blackbox-tests-f06"></a>

### 10-blackbox-tests-F06: Sealed retry proof checks requests created by the test itself

**Severity:** High. The asserted client retry property remains true even if native clients generate new retry content.

**Classification:** Correctness defect in verification.

**References**

- `conformance/blackbox/integration/real_issue49_remaining_semantics_test.go:554-660`
- Binding: `conformance/mutants/integration/manifest.json:33`
- Mutant: `conformance/mutants/integration/issue49-remaining-sealed-retry.patch:1-16`
- Existing server replay proof: `conformance/blackbox/integration/real_push_retention_test.go:27-195`
- Contract: `docs/src/content/docs/spec/03-state-machines.mdx:175-189`

**Observed behavior and cost**

The test marshals one request into `sealed` and manually sends those same bytes four times.
The proxy returns response loss, 429, and 503 on predefined attempts.
The test then checks that its manually supplied requests equal `sealed`.
No native queue, batch sealing, retry scheduler, persisted deadline, or native retry serialization participates.
The test does not wait for `Retry-After`.

The associated mutant changes the extension's stored-response replay branch.
It can demonstrate server replay failure, but not client sealed-request reuse.
The existing S-11 test already checks real server exact replay and durable once-only work after response loss.

**Smallest coherent simplification**

Retain server replay proof in S-11 and delete this manual retry simulation as a second server proof.
Bind client sealed-retry requirements to the existing native retry flow with a transport fault controller.
If that flow lacks the required observation, extend that proof rather than adding another Go client simulation.

**Invariant to preserve**

After response loss, 429, and 503, native clients must preserve batch identity, mutation order, canonical bytes, and retry timing.

**Acceptance**

- A native control that changes a mutation or batch ID after 429 must fail.
- A native control that sends before its durable retry deadline must fail.
- Keep the server replay mutant failing through the retained server proof.
- The current focused baseline command is `make test-blackbox-mutation-control MUTATION_CONTROL_TEST='TestRealIssue49RemainingSemantics/assertion#13' MUTATION_CONTROL_EXPECT=target_pass`.
- Rebind that target before deleting the duplicate flow.

**Tracking:** `SYNC-FAILURE-003`, `CTRL-FAILURE-003`, and issue `#49`.

<a id="10-blackbox-tests-f07"></a>

### 10-blackbox-tests-F07: Several negative controls change multiple rejection conditions together

**Severity:** Medium. A surviving validation condition can hide a missing independent guard.

**Classification:** Correctness defect in verification.

**References**

- Synced key and version fault: `conformance/blackbox/integration/real_issue49_wal_authority_test.go:948-981,1611-1626`
- Capture-key fault: `conformance/blackbox/integration/real_issue49_wal_completeness_test.go:270-343`
- Mutant: `conformance/mutants/integration/issue49-wal-complete-capture-key-correlation.patch:4-11`
- Registration controls: `conformance/blackbox/integration/real_issue49_security_completeness_test.go:198-257,322-325`
- Validly typed comparison: `conformance/blackbox/integration/real_issue49_wal_authority_test.go:391-417`
- Function signature evidence: `conformance/blackbox/integration/real_release_semantics_test.go:239`

**Observed behavior and cost**

The synced fence control changes both `new_record_id` and `row_version` before checking rejection.
The capture control changes both old and new capture keys.
Its mutant removes both key predicates together.
Therefore, the capture test does not independently prove the old-key and new-key clauses named in its comment.
Removing only one predicate can still leave the other mismatched predicate to reject the transaction.

The security registration controls also use `cf_items_membership(uuid)` for bigint and numeric key tables.
They accept any registration error as proof of the requested key or replica-identity rejection.
The fixture supplies additional invalid conditions instead of isolating the tested condition.
The earlier numeric-key test creates a numeric membership function, which demonstrates a better control shape.

**Smallest coherent simplification**

Start from one accepted fixture and change one property per independent guard.
Use matching membership function signatures for key and replica-identity controls.
Split the combined capture-key mutant into individual predicate removals.
Delete compound fault setup and redundant broad rejection claims.

**Invariant to preserve**

Every fence identity component and every required registry condition must independently cause rejection when invalid.

**Acceptance**

- Each single-predicate mutant must fail its matching semantic assertion.
- The corresponding unchanged fixture must succeed.
- Current focused targets include `TestRealIssue49FenceCorrelatesCaptureKeys` and `TestRealIssue49SecurityRegistryIdentityAndKeys`.
- Run them through `make test-blackbox-mutation-control MUTATION_CONTROL_TEST=<target> MUTATION_CONTROL_EXPECT=target_pass` before mutation.

**Tracking:** `SYNC-WAL-009`, `SYNC-REGISTRY-001`, `SYNC-REGISTRY-002`, and issue `#49`.
The static review establishes masking conditions. It does not claim that a single-predicate mutant was executed.

<a id="10-blackbox-tests-f08"></a>

### 10-blackbox-tests-F08: Hydration recovery checks checkpoint count instead of checkpoint position

**Severity:** Medium. Premature acknowledgement can preserve the map size and pass the named assertion.

**Classification:** Correctness defect in verification.

**References**

- `conformance/blackbox/integration/real_pull_rebuild_test.go:316-333`
- Existing comparison helper: `conformance/blackbox/integration/real_pull_rebuild_test.go:702-713`
- Stronger comparable assertion: `conformance/blackbox/integration/real_baseline_test.go:1184-1191`

**Observed behavior and cost**

The successful hydration-recovery response must not acknowledge a cursor before presentation.
The test compares only `len(afterSuccess)` with `len(beforeFailure)`.
An implementation that advances both existing checkpoint positions leaves that count unchanged.
The failure message describes a position property that the expression does not test.

**Smallest coherent simplification**

Replace the count check with the existing checkpoint-position comparison helper.
Keep the later acknowledgement and advancement checks.
This deletes a false proxy assertion without adding a new abstraction.

**Invariant to preserve**

Hydration failure changes no progress. Successful selection changes no acknowledged position before cursor presentation.

**Acceptance**

- Run the S-03 proof through `make test-blackbox` during integration.
- A control that writes the selected cursor during the successful recovery response must fail this assertion.
- The current focused mutation Make target does not accept S-03. Add a supported focused target before isolated execution.

**Tracking:** `SCN-PULL-HYDRATION-FAILURE-001`, S-03, and cursor acknowledgement requirements.

<a id="10-blackbox-tests-f09"></a>

### 10-blackbox-tests-F09: Test helper families duplicate the same protocol parsing and checks

**Severity:** Low. Equivalent helpers already use different bounds and failure rules, which increases maintenance risk.

**Classification:** Behavior-preserving cleanup.

**References**

- Schema fetch and field lookup: `conformance/blackbox/integration/real_push_retention_test.go:993-1048`
- Existing complete table reference: `conformance/blackbox/integration/real_registry_schema_test.go:18-23,253-340`
- Additional schema fetches: `conformance/blackbox/integration/real_issue49_data_semantics_test.go:1566-1588`
- Additional schema fetches: `conformance/blackbox/integration/real_mutation_controls_test.go:410-440`
- Pull helpers: `conformance/blackbox/integration/real_baseline_test.go:1654-1686`
- Pull helpers: `conformance/blackbox/integration/real_pull_rebuild_test.go:461-504`
- Error helpers: `conformance/blackbox/integration/real_pull_rebuild_test.go:643-655`
- Error helpers: `conformance/blackbox/integration/real_push_retention_test.go:1113-1128`
- Error helpers: `conformance/blackbox/integration/real_issue49_wal_authority_test.go:1808-1817`
- Packaged SQL loaders: `conformance/blackbox/integration/real_issue49_wal_authority_test.go:1527-1556`
- Packaged SQL loaders: `conformance/blackbox/integration/real_issue49_security_completeness_test.go:1092-1128`

**Observed behavior and cost**

Several helpers fetch the same schema endpoint and reconstruct overlapping table and field maps.
Some readers enforce a maximum-plus-one byte check. Others truncate at the limit before JSON decoding.
Two pull helpers duplicate cursor installation, differing mainly in configurable limits and selected scope maps.
Three error helpers perform the same envelope, code, and retryability checks.
Two loaders independently locate the same packaged SQL file.

These helpers share concrete consumers and one protocol meaning. Their duplication is not independent semantic proof.

**Smallest coherent simplification**

Reuse the existing full schema table reference for field lookup.
Use one bounded raw schema fetch where raw manifest consumers need independent vector parsing.
Keep one configurable pull helper and one protocol-error helper.
Reuse the generic packaged-file loader for SQL.
Delete superseded implementations instead of adding pass-through wrappers.

**Invariant to preserve**

Keep raw bytes for exact replay and independent digest checks. Preserve strict parsing and requested-scope validation.

**Acceptance**

- Inspect the complete caller diff before deletion.
- Run `make test-blackbox`, affected exact mutation targets, and `make test-r1-benchmark-units`.
- Retain one malformed and oversized-response control at the shared transport proof home.

**Tracking:** Structural cleanup under issue `#36`. No protocol change is necessary.

<a id="10-blackbox-tests-f10"></a>

### 10-blackbox-tests-F10: Benchmark comparison policy binds a permanent baseline to one host and one source file

**Severity:** Medium. A different machine or a non-measurement source edit blocks comparison before performance is evaluated.

**Classification:** Contract decision.

**References**

- `conformance/blackbox/integration/real_r1_benchmark_test.go:412-438,453-483`
- `conformance/blackbox/integration/real_r1_benchmark_test.go:1337-1352,1378-1661`
- Timed helper dependency: `conformance/blackbox/integration/real_r1_benchmark_test.go:667-670`
- Helper implementation outside the definition hash: `conformance/blackbox/integration/real_baseline_test.go:1816-1849`
- Fixed baseline: `conformance/blackbox/integration/testdata/r1-benchmark-baseline.json:3-11`
- Runner policy: `Makefile:549-601`

**Observed behavior and cost**

The baseline includes one hardware fingerprint. Comparison requires that exact fingerprint, CPU count, OS, and architecture.
This excludes an otherwise equivalent replacement host.
The definition hash covers the complete benchmark Go file, including parser tests, path-safety tests, and comments.
Changing a unit assertion or comment invalidates the baseline definition.
Changing `postSync`, which runs inside the timed interval, does not change that definition hash.

Thus, the identity policy rejects some irrelevant changes while permitting some measurement-definition changes.
The machine restriction also makes the baseline depend on continued access to one host.
These are concrete gate costs, not reasons to compare uncontrolled machines.

**Proposed decision, not approval**

Consider running baseline and candidate artifacts with the same benchmark harness on the same current host.
Keep the approved baseline revision fixed and record both artifact digests.
Compare those paired measurements under the existing regression thresholds.
This can delete the permanent host-identity restriction and the whole-Go-file equality gate.

The tradeoff is an additional baseline execution and artifact preparation.
The primary must decide whether that cost is preferable to permanent host dependence and repeated baseline replacement.

**Invariant to preserve**

Both measurements must use identical workload semantics and a controlled environment.
The change must not permit arbitrary baseline replacement or relaxed regression thresholds.

**Acceptance**

- Run `make test-r1-benchmark-units` after the approved comparison-policy change.
- Run the benchmark comparison through its supported Make entry point.
- A non-measurement comment change must not require new performance acceptance.
- Changing timed workload behavior must not silently compare different harness definitions.
- Preserve failed comparison output and both artifact identities.

**Tracking:** R1 benchmark gate and issue `#36`. This review does not approve a new benchmark policy.

<a id="10-blackbox-tests-f11"></a>

### 10-blackbox-tests-F11: RSS sampling can wait forever for a readiness message after cancellation

**Severity:** Medium. A canceled benchmark can block until the outer test timeout instead of reporting its cause.

**Classification:** Correctness defect in verification control flow.

**References**

- `conformance/blackbox/integration/real_r1_benchmark_test.go:1054-1101`
- RSS subprocess: `conformance/blackbox/integration/real_r1_benchmark_test.go:1124-1140`

**Observed behavior and cost**

The sampling goroutine checks `samplingContext.Err()` immediately after the first RSS read.
If cancellation occurs there, it sends only to `result` and returns.
The parent waits unconditionally on `<-ready`.
No remaining sender can satisfy that receive.

This follows directly from the channel branches. No failing process execution is claimed.

**Smallest coherent simplification**

Perform the initial RSS read synchronously before starting the sampling loop.
Use that reading as the initial maximum, then start one result-producing goroutine.
Delete the readiness channel and `first` flag.
Alternatively, every early return must report readiness failure and the parent must select on cancellation.

**Invariant to preserve**

Sampling must start before commit. Cancellation must terminate all waits and preserve a bounded failure result.

**Acceptance**

- Add a canceled-before-first-sample case under the benchmark unit Make target.
- The case must return within a short explicit bound without a live database.
- Run `make test-r1-benchmark-units`.
- The static review did not execute this path.

**Tracking:** R1 benchmark reliability. No existing issue match was established.

<a id="10-blackbox-tests-f12"></a>

### 10-blackbox-tests-F12: Quarantine redaction proof does not create quarantine output

**Severity:** Medium. The named security proof can pass when the quarantine serialization path never executes.

**Classification:** Correctness defect in verification.

**References**

- `conformance/blackbox/integration/real_issue49_security_completeness_test.go:719-824`
- Real poison flow for comparison: `conformance/blackbox/integration/real_issue49_wal_authority_test.go:134-156,173-231`

**Observed behavior and cost**

The security test claims quarantine redaction coverage.
Its setup performs a successful push and invalid pull input, then waits for healthy readiness.
It reads `COALESCE(string_agg(failure_detail, ...), '')` from `sync_wal_poison` without creating a poison record.
It never requires a nonempty quarantine result or a row count.
An empty result passes both the canary scan and the 512-byte limit.

The separate poison test reaches an actual decoder failure and checks diagnostics.
That test supplies the correct event boundary for quarantine redaction proof.

**Smallest coherent simplification**

Move the quarantine-specific redaction assertion into the existing real poison flow.
Require the expected poison row and a nonempty bounded detail before scanning for protected values.
Delete the empty-table check from the healthy security flow.
Keep healthy readiness and disabled metrics or traces checks in their current narrow role.

**Invariant to preserve**

Actual handled failures must not disclose credentials, request values, SQL, or WAL payloads.

**Acceptance**

- Run the retained poison target through `make test-blackbox-mutation-control`.
- A control that copies a protected canary into the actual quarantine detail must fail.
- An empty quarantine table must fail setup rather than count as redaction evidence.

**Tracking:** `SYNC-LOGGING-001`, `CTRL-LOGGING-001`, and issue `#49`.


<a id="area-11-swift-conformance"></a>

## Shared native conformance findings

<a id="11-swift-conformance-f01"></a>

### 11-swift-conformance-F01: Go journey orchestration has multiple owners

- **Severity:** Medium. A protocol or fixture change requires parallel edits to equivalent Go control flows.
- **Classification:** Behavior-preserving cleanup.

#### Evidence

These are duplicate Go implementations, not necessary Swift and Kotlin engine implementations.

| Repeated meaning | Swift implementation | Kotlin implementation |
| --- | --- | --- |
| Rebuild-apply workload, source expansion, page assertions, identity resolution | `conformance/swift/rebuild_apply.go:71-542` | `conformance/kotlin/rebuild_apply.go:71-694` |
| Second copy for rebuild cardinality | `conformance/swift/rebuild_cardinality.go:71-538` | `conformance/kotlin/rebuild_cardinality.go:71-700` |
| Queue workload expansion and schema publication | `conformance/swift/queue_replay.go:701-1048` | `conformance/kotlin/queue_replay.go:779-1038` |
| CRUD and immutable successor journey | `conformance/swift/queue_replay.go:226-681` | `conformance/kotlin/queue_replay.go:241-728` |
| Pending-cycle journey and evidence assembly | `conformance/swift/pending_cycle.go:25-399,646-826` | `conformance/kotlin/pending_cycle.go:25-418,613-814` |
| Schema-check step order and lifecycle dispatch | `conformance/swift/schema_check.go:20-347` | `conformance/kotlin/schema_check.go:20-347` |
| Forged-cursor scenario predicates | `conformance/swift/forged_cursor.go:161-352` | `conformance/kotlin/forged_cursor.go:173-364` |
| Held push fault and sealed retry sequence | `conformance/swift/platform.go:225-479` | `conformance/kotlin/platform.go:394-634` |
| Pull response fault mutations | `conformance/swift/steady_pull.go:330-573` | `conformance/kotlin/steady_pull.go:291-546` |
| Seed mutant construction and cleanup | `conformance/swift/seeded_empty_startup.go:197-299` | `conformance/kotlin/seeded_empty_startup.go:184-286` |
| Merge and project native state facts | `conformance/swift/scenario_helpers.go:244-351` | `conformance/kotlin/scenario_helpers.go:230-338` |
| Shared server reset | `conformance/swift/scenario_integration_test.go:214-272` | `conformance/kotlin/scenario_integration_test.go:330-384` |

The copies already differ in non-platform behavior:

- Kotlin validates each queue sample's accepted and rejected outcome counts at `conformance/kotlin/queue_replay.go:195-202,730-759`.
- Swift checks the final projection but lacks that sample-level check at `conformance/swift/queue_replay.go:176-214`.
- Swift resets one diagnostic table shape. Kotlin resets all diagnostic table shapes in the shared server reset.
- Schema-check repeats the authored step sequence in two Go files, then separately validates the corpus bindings.

Each platform also has two ordinary public-call paths:

- `conformance/swift/platform.go:949-978` and `conformance/swift/scenario_helpers.go:66-103`.
- `conformance/kotlin/platform.go:1030-1053` and `conformance/kotlin/scenario_helpers.go:65-98`.

The Swift scenario path does not consume `restarted` or populate replay counts.
The grouped path does both and sets `started` unconditionally.
The Kotlin paths also disagree on how completion updates `started`.
This is concrete bookkeeping drift, not a formatting concern.

#### Smallest coherent simplification

Move platform-neutral workload preparation, fixture reset, fault mutation, and semantic predicates to shared Go code.
Give the Swift and Kotlin drivers one common journey implementation where their sequence has the same meaning.
Start with the four rebuild consumers and the two queue consumers.
Retain small platform adapters for actual installation, public calls, transport pauses, captures, and process replacement.
Do not introduce a general script interpreter or another synchronization engine.

Within each platform, use one ordinary call path before applying scenario-specific transport matching.
Delete the second bookkeeping path and the duplicate pure helper bodies.
Keep every current native run and negative control during this extraction.

#### Invariants and acceptance

- Preserve authored inputs, exact response replay, WAL-only visibility, native queue state, and independent assertions.
- Preserve Kotlin per-sample queue outcomes when consolidating the driver.
- Preserve all platform differences listed later in this report.
- Run `make test-conformance-drivers` and the four native targets listed under validation.
- Demonstrate failure for changed request identity, omitted rebuild pages, and lost queued intent on both native platforms.
- Related requirements include `SYNC-REBUILD-002`, `SYNC-REBUILD-006`, `SYNC-MUTATION-004`, and `SYNC-PROVENANCE-001`.

<a id="11-swift-conformance-f02"></a>

### 11-swift-conformance-F02: Dead setup and test-only helpers create false dependencies

- **Severity:** Low. Unused setup adds prerequisites, cleanup paths, and misleading maintenance work.
- **Classification:** Behavior-preserving cleanup.

#### Evidence

`RunMultiScopeProvenanceScenario` only checks its artifact argument for nil.
Both implementations install every client with empty initialization.

- `conformance/swift/multi_scope_provenance.go:161-163,256-264`
- `conformance/kotlin/multi_scope_provenance.go:151-153,225-229`
- Unused artifact construction: `conformance/swift/scenario_integration_test.go:80-84,415-439`.
- Matching Kotlin setup: `conformance/kotlin/scenario_integration_test.go:110-133`.

Exact consumer searches also found these dead paths:

| Path | Unused code |
| --- | --- |
| `conformance/swift/multi_scope_provenance.go:852-871` | Uncalled page-size helper |
| `conformance/swift/retention_reconnect.go:702-711` | Uncalled step-wire validator |
| `conformance/swift/warm_connect.go:491-505` | Uncalled sorted-string validator |
| `conformance/kotlin/platform.go:141,719,775,780-784` | Write-only installation flag and uncalled reader |
| `conformance/kotlin/platform.go:2280-2287` | Uncalled client-session closer |
| `conformance/kotlin/platform.go:3595-3598,3638-3649` | Dispatcher called only by its own unit test |
| `conformance/kotlin/platform_test.go:521-537` | Test of that unused dispatcher |
| `conformance/kotlin/platform.go:3685-3691` | Uncalled selector-key wrappers |

The schema-check client map is written but never read in these paths:

- `conformance/swift/schema_check.go:37-43,267-277`
- `conformance/kotlin/schema_check.go:37-43,267-277`

The optional `call.Steps` branches are unreachable from the scenario call paths, which always return nil steps.
See `conformance/swift/schema_check.go:417-425` and `conformance/kotlin/schema_check.go:417-425`.

#### Smallest coherent simplification

Remove the unused multi-scope artifact argument, construction, helper, and cleanup.
Keep artifacts for forged-cursor and seeded startup because those journeys use them.
Delete the proven dead helpers, write-only flag, unused client map, and dispatcher-only test.
Retain the actual `ApplyStep`, `RequestStep`, and `ProcessStep` boundary tests.

#### Invariants and acceptance

- Keep real seed validation and the real multi-scope restart proof.
- Repeat the exact consumer search before deletion to detect concurrent consumers.
- Run `make test-conformance-drivers`.
- Run the focused multi-scope scenario commands listed under validation.
- Related scenario: `SCN-PERF-MULTI-SCOPE-PROVENANCE-001`.
- This is not a verified match to issue `#102`, which names other unused probes and scanners.

<a id="11-swift-conformance-f03"></a>

### 11-swift-conformance-F03: Observation copying does not preserve immutable evidence

- **Severity:** Medium. A caller can modify stored evidence through supposedly detached observations.
- **Classification:** Correctness defect.

#### Evidence

Both manual clone functions copy the enclosing structs but retain two pointer fields:

- `RequestFacts.ScopeFingerprint`
- `RebuildResponseFacts.ResponseBodySHA256`

References:

- `conformance/swift/protocol.go:1307-1390`
- `conformance/kotlin/protocol.go:1064-1149`
- Public Swift immutability promise: `conformance/swift/session.go:171-187`.
- Existing partial clone test: `conformance/swift/protocol_test.go:366-400`.
- Kotlin scalar-only history test: `conformance/kotlin/session_test.go:241-267`.

Changing either retained pointer through `ObservationsAfter` changes the stored snapshot.
The history comparison can then compare against caller-modified data.
The omitted response hash also participates in exact rebuild replay assertions.

The decoders also collapse an explicit empty cursor array into nil through `append([]string(nil), empty...)`.
Their validators subsequently require a non-nil pull cursor array.

- Swift decoder and validator: `conformance/swift/protocol.go:386-391,1205-1207`.
- Kotlin decoder and validator: `conformance/kotlin/protocol.go:829-832,956-958`.
- Kotlin's in-memory empty-cursor mapping is accepted at `conformance/kotlin/platform_test.go:733-795`.

Thus, that in-memory evidence shape does not survive its JSON boundary.

#### Smallest coherent simplification

Use one shared observation value model and complete copy routine for the two Go consumers.
Keep platform envelope parsing outside that shared model.
Copy every optional pointer and preserve explicit empty-array presence.
Delete the parallel clone implementations after their consumers migrate.

#### Invariants and acceptance

- Preserve absent, null, and empty distinctions required by each runner envelope.
- Preserve immutable accepted history and response-body hashes.
- Add mutations for both omitted pointer fields to the driver tests.
- Test mutations through both the input snapshot and a returned observation.
- Decode an otherwise valid pull with an explicit empty cursor array and verify the intended boundary behavior.
- Run `make test-conformance-drivers`.
- Related scenario: `SCN-PERF-REBUILD-REQUESTS-001` and its exact-page replay control.
- Static pointer analysis establishes this finding. No executable mutant ran during review.

<a id="11-swift-conformance-f04"></a>

### 11-swift-conformance-F04: Several Swift negative parser tests fail for unrelated reasons

- **Severity:** Medium. The tests can remain green after their named validation is removed.
- **Classification:** Correctness defect in test proof.

#### Evidence

`conformance/swift/protocol_test.go:180-195` constructs negative transport envelopes without process identity or database identity.
Several observations also omit required `retryable`.
These omissions trigger rejection independently of status bounds, cursor checks, or sequence checks.

- Required transport members: `conformance/swift/protocol.go:358-378`.
- Required result identity: `conformance/swift/protocol.go:1023-1026`.

`conformance/swift/protocol_test.go:71-81` uses obsolete receipt members such as `record_identities_hex`.
It omits the required `records_in_canonical_order` and `row_checksums_valid` members.
Both cases can fail without the condition named by the test.
The current receipt decoder is at `conformance/swift/protocol.go:472-492`.

The Kotlin push decoder test demonstrates a simpler valid-base pattern at `conformance/kotlin/session_test.go:156-180`.

#### Smallest coherent simplification

Build one valid envelope per proof family and assert that it passes.
Change one required property for each negative control.
Delete the stale receipt shape and separately assembled malformed transport fixtures.
Keep distinct semantic negative controls instead of reducing their coverage.

#### Invariants and acceptance

- Every negative control must fail because of its intended defect.
- Temporarily remove each targeted validator and show that the corresponding control detects its absence.
- Run `make test-conformance-drivers` after restoring the validator.
- This finding concerns gate integrity, not native protocol behavior.
- The masked branches were established statically. No validator was edited during review.

<a id="11-swift-conformance-f05"></a>

### 11-swift-conformance-F05: Shared queue projection emits the wrong int64 identity form

- **Severity:** Medium. Correct native int64 keys become incorrect conformance facts.
- **Classification:** Correctness defect.

#### Evidence

Both converters put `int` and `int64` in the same branch and return an unquoted decimal value:

- `conformance/swift/platform.go:2416-2432`
- `conformance/kotlin/platform.go:3444-3460`

Their queue consumers use that result as `CanonicalWireJSON`:

- `conformance/swift/platform.go:2323-2351`
- `conformance/kotlin/platform.go:3284-3322`

Protocol `int64` uses a canonical decimal JSON string, unlike protocol `int`.
See `docs/src/content/docs/spec/01-wire-protocol.mdx:90-114`.
For example, an int64 key `9223372036854775807` must remain quoted in its canonical wire fact.

The converters also accept primary-key types that the same contract forbids, including decimal and temporal types.

#### Smallest coherent simplification

Use one shared primary-key fact conversion with the three supported types only.
Keep `int` as a bounded JSON number and `int64` as a validated canonical JSON string.
Delete the duplicated broader type switches.

#### Invariants and acceptance

- Preserve the full int64 range without float conversion.
- Reject disallowed primary-key types and noncanonical integer text.
- Test string, int boundaries, and quoted int64 boundaries through both native fact adapters.
- Run `make test-conformance-drivers`.
- This is a static contract mismatch. The assigned native journeys primarily use string keys.

<a id="11-swift-conformance-f06"></a>

### 11-swift-conformance-F06: Provenance matching assumes identity translation preserves sort order

- **Severity:** Low. A valid authored identity change can break the proof without changing native behavior.
- **Classification:** Correctness defect in comparison logic.

#### Evidence

Both validators independently sort authored names and runtime identifiers, then compare matching indexes through aliases:

- `conformance/swift/multi_scope_provenance.go:1328-1381`
- `conformance/kotlin/multi_scope_provenance.go:1123-1152`

The controller derives runtime row UUIDs from SHA-256, which does not preserve lexical order.
See `conformance/blackbox/native_controller.go:4111-4116`.

A read-only reproduction of that algorithm produced:

| Authored row | Runtime UUID |
| --- | --- |
| `row-a` | `e9791968-1509-4499-9d32-97ab657001b6` |
| `row-b` | `dbad292e-ba64-4d05-a101-2d3d02b125a6` |

The authored order is A then B. The runtime order is B then A.
The current two corpus row names happen to preserve order, so this is not evidence of a current flaky run.

#### Smallest coherent simplification

Resolve expected table and row identities first.
Compare maps keyed by the resolved table and row identity.
Delete the separate pre-resolution sorts and positional pairing.
Use the existing alias bindings, not a new identity authority.

#### Invariants and acceptance

- Keep exact row identity, scope membership, version, and duplicate rejection.
- Add a valid reversed-order alias case and a wrong-scope negative control.
- Run `make test-conformance-drivers` and both focused multi-scope scenarios.
- Related requirement: `SYNC-PROVENANCE-001`.

<a id="11-swift-conformance-f07"></a>

### 11-swift-conformance-F07: Schema-transition proof omits part of immutable authored intent

- **Severity:** Medium. Timestamp changes can pass a test intended to prove complete intent preservation.
- **Classification:** Correctness defect in test proof.

#### Evidence

The scenario promises complete authored intent preservation across an incompatible schema change.
See `conformance/scenarios/server/schema-queued-mutation-001.json:4-11`.
The client contract forbids rewriting the primary key or client timestamp at `docs/src/content/docs/spec/02-client-contract.mdx:145-169`.

Both scenario drivers remove the full queue and outcome families from the ordinary projection:

- `conformance/swift/schema_queued_mutation.go:145-158,333-339`
- `conformance/kotlin/schema_queued_mutation.go:157-167,336-344`

The replacement entry comparisons check mutation ID, table, base-version correlation, operation, status, order, schema, and fields.
They do not compare `ClientVersion`.
They resolve a primary-key alias but do not compare that alias against the queued key.

- `conformance/swift/schema_queued_mutation.go:355-399,434-512`
- `conformance/kotlin/schema_queued_mutation.go:370-405,435-496`

Keeping every checked value equal while changing only `ClientVersion` therefore leaves these comparisons unchanged.
The driver does not retain a pre-transition mutation capture that could detect that change.

Both implementations also translate expected queue schemas before immediately dropping the translated queue:

- `conformance/swift/schema_queued_mutation.go:571-582` followed by lines `152,333-339`.
- `conformance/kotlin/schema_queued_mutation.go:547-558` followed by lines `161,336-344`.

#### Smallest coherent simplification

Capture the immutable authored mutation once before schema publication.
Compare its complete authored payload after rejection and restart, with status transitions checked separately.
Resolve authored logical identities at the initial binding boundary.
Delete discarded queue translations and the incomplete hand-maintained field comparison where the complete comparison replaces it.

#### Invariants and acceptance

- Preserve mutation identity, typed key, client timestamp, authored schema, field presence, values, and local order.
- Preserve permitted server-owned base-version behavior only where the contract allows it.
- Add negative controls that change only the retained timestamp or typed key.
- Run `make test-conformance-drivers` and both focused schema-queued-mutation scenarios.
- Related requirements: `SYNC-SCHEMA-002`, `SYNC-SCHEMA-003`, and `SYNC-MUTATION-004`.
- This identifies a proof gap, not a demonstrated native payload corruption.

<a id="11-swift-conformance-f08"></a>

### 11-swift-conformance-F08: The scope-digest fault exercises malformed syntax instead of aggregate integrity

- **Severity:** Medium. A named integrity control can pass without exercising scope-digest comparison.
- **Classification:** Correctness defect in proof attribution.

#### Evidence

The `scope-digest` mutation changes `algorithm` to `sha1`:

- `conformance/swift/steady_pull.go:515-529`
- `conformance/kotlin/steady_pull.go:476-498`

Both consumers expect `invalid_response` and unchanged durable state:

- `conformance/swift/steady_pull.go:575-603`
- `conformance/kotlin/steady_pull.go:548-558,580-605`

This is the malformed checksum-object path described at `docs/src/content/docs/spec/01-wire-protocol.mdx:122-137`.
The authored aggregate control instead changes the canonical scope digest input stream.
See `conformance/scenarios/performance/steady-pull-001.json:1066`.

A valid checksum object with a wrong aggregate digest has different required behavior.
The client invalidates the affected scope and rebuilds it.
See `docs/src/content/docs/spec/02-client-contract.mdx:609-625`.

Native engine tests already exercise that separate behavior with mocked transport:

- `clients/swift/Tests/SynchroTests/SyncEngineTests.swift:494-564`
- `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/SyncEngineTests.kt:1075-1149`

#### Smallest coherent simplification

Give malformed checksum syntax and a valid-but-wrong scope digest distinct proof attribution.
Use the shared response mutator from F01 for the real-adapter journey.
Preserve the malformed-object negative control under its correct requirement.
Move the aggregate mismatch flow to its chosen authoritative proof home instead of maintaining a mislabeled duplicate.
Do not remove the existing native engine proof before its replacement has executable evidence.

#### Invariants and acceptance

- A wrong aggregate digest must never install a healthy cursor or checksum.
- Preserve unaffected scopes and verify recovery of the affected scope.
- Demonstrate a mutant that disables aggregate digest comparison.
- Run both focused steady-pull scenarios and applicable native tests after the proof-home change.
- Related requirements: `SYNC-INTEGRITY-005` and `SYNC-INTEGRITY-006`.
- This review does not claim that the repository lacks every aggregate mismatch test.

<a id="11-swift-conformance-f09"></a>

### 11-swift-conformance-F09: The native floor-resume proof never establishes floor equality

- **Severity:** Medium. The test can pass when compaction makes no relevant progress.
- **Classification:** Correctness defect in test proof.

#### Evidence

The native consumers run compaction, take any nonempty local scope cursor, restart, and require successful continuation.

- `conformance/swift/retention_reconnect.go:206-231,248-384`
- `conformance/kotlin/retention_reconnect.go:191-216,233-385`

Their compaction validators check only a matching active rebuild pin and the existence of a scope:

- `conformance/swift/retention_reconnect.go:891-928`
- `conformance/kotlin/retention_reconnect.go:868-899`

The shared controller discards the compaction result at `conformance/blackbox/native_controller.go:1376-1380`.
The operator validates result shape but does not require deletion or floor movement at `conformance/blackbox/push_retention_controls.go:128-156`.

No native assertion binds the resumed cursor to the server's advanced floor.
A no-op compactor can leave the pin and scope present while the same cursor remains usable.

The server proof already demonstrates a clearer authoritative home:
`conformance/blackbox/integration/real_push_retention_test.go:645-697` compares exact pre-compaction and post-compaction floors and effect counts.

#### Smallest coherent simplification

Reuse authoritative compaction observations to establish the native scenario prerequisite.
Keep the native proof focused on durable process restart and opaque cursor reuse.
Delete the weaker duplicate pin-and-scope validator after the prerequisite replaces it.
Keep below-floor rejection and scope-local deletion assertions in the server proof home.
Do not decode server cursor internals in the native client or native driver.

#### Invariants and acceptance

- Establish that the presented cursor denotes the intended retained floor before native restart.
- A mutant that suppresses floor advancement must fail the prerequisite.
- Run both focused retention-reconnect scenarios.
- Preserve the independent server S-12 compaction proof.
- Related requirement: `SYNC-RETENTION-001`.
- No compaction mutant ran during this static review.

<a id="11-swift-conformance-f10"></a>

### 11-swift-conformance-F10: Recovery observation returns its expected completion

- **Severity:** Low. A circular assertion obscures the actual recovery evidence.
- **Classification:** Behavior-preserving cleanup.

#### Evidence

Both recovery loops accept a `want` argument and return it when observed status is ready.
Their callers then compare the returned value with the same expected value.

- `conformance/swift/retention_reconnect.go:494-515,542-570`
- `conformance/kotlin/retention_reconnect.go:483-503,523-544`

The meaningful proof is the observed ready status, absent failure, and rejected-push/connect pair.
The returned expected string adds no independent evidence.
The Swift loop also carries an unused scenario argument at lines `542-543`.

#### Smallest coherent simplification

Return observed recovery facts only, or derive idle directly from observed ready status.
Remove the expected-completion argument and the tautological comparison.
Remove the unused Swift scenario argument and unused operation arguments in both renewal helpers.

#### Invariants and acceptance

- Keep the ready status, failure absence, and exact wire pair checks.
- A conflicting expected completion must not be reported as an observed result.
- Run `make test-conformance-drivers` and both focused retention-reconnect scenarios.
- Related scenario: `SCN-RETENTION-RECONNECT-001`.

<a id="11-swift-conformance-f11"></a>

### 11-swift-conformance-F11: Native performance sample results do not reach measurement assertions

- **Severity:** High. The assigned native gates can pass without satisfying their authored measurement obligations.
- **Classification:** Correctness defect in gate evidence.

#### Evidence

The rebuild scenarios require native measurements on their listed support cells:

- `conformance/scenarios/performance/rebuild-apply-001.json:75-181`
- `conformance/scenarios/performance/rebuild-cardinality-001.json:104-210`

The required IDs are `MEAS-REBUILD-APPLY-001` and `MEAS-REBUILD-CARDINALITY-001`.
Their assertions require all configured strata and required metrics:

- `conformance/scenarios/performance/rebuild-apply-001.json:1662-1679`
- `conformance/scenarios/performance/rebuild-cardinality-001.json:1700-1717`

The assigned integration callers validate only identity-resolution counts after semantic execution:

- `conformance/swift/scenario_integration_test.go:119-140`
- `conformance/kotlin/scenario_integration_test.go:223-234,260-271`

The call results contain duration and work values, but these callers do not consume or emit measurement records.
Exact consumer searches found no other callers of these scenario functions.

The multi-scope path makes the disconnect explicit:

- It removes `performance-contract-satisfied` from model expectations at `conformance/swift/multi_scope_provenance.go:367-392`.
- Kotlin does the same at `conformance/kotlin/multi_scope_provenance.go:306-327`.
- The Swift comment says the native consumer validates the samples, but neither consumer does so.
- `MeasuredScopeCount` and `KnownScopeCount` are only assigned and checked by parser unit tests.
- References: Swift lines `752-784` and test lines `12-51`, Kotlin lines `651-676` and test lines `12-35`.
- The final integration callers check only identity-resolution counts at Swift lines `80-90` and Kotlin lines `110-139`.

The Make recipes invoke ordinary structured test-result parsing, not a native measurement collector.
See `Makefile:953-964,1024-1042`.

#### Smallest coherent simplification

Give measurement collection and validation one explicit owner outside the duplicated semantic wrappers.
Pass each observed native sample to that owner with its authored sample, stratum, and measurement ID.
Remove parser-only measurement fields and misleading validation claims from semantic plans when that owner replaces them.
Do not use model-produced metrics as native observations.

#### Invariants and acceptance

- Preserve each required sample and native semantic outcome until the contract changes explicitly.
- Require missing, malformed, unbound, or contract-invalid native measurement evidence to fail.
- A control that discards the observed duration or work data must fail the measurement obligation.
- Run both native rebuild families and multi-scope scenarios through their measurement collector.
- Run `make verify-contract` for any evidence-binding change.
- This finding does not claim that a complete release gate was executed during review.

<a id="11-swift-conformance-f12"></a>

### 11-swift-conformance-F12: Reconsider separate repeated executions for overlapping rebuild measurements

- **Severity:** Medium. The contract causes repeated native setup without a corresponding difference in the assigned semantic flow.
- **Classification:** Contract decision. No reduction is approved.

Both rebuild families use the same nine workload sizes and page size:

- Three samples with one row.
- Three samples with 101 rows.
- Three samples with 1,000 rows.
- Page size 100 throughout.

Sources:

- `conformance/scenarios/performance/rebuild-apply-001.json:926-1634`
- `conformance/scenarios/performance/rebuild-cardinality-001.json:964-1672`
- Four execution loops: `conformance/swift/rebuild_apply.go:101-132`, `conformance/swift/rebuild_cardinality.go:101-132`, `conformance/kotlin/rebuild_apply.go:109-155`, `conformance/kotlin/rebuild_cardinality.go:109-155`.

Together, these loops require 36 fresh native client installations across the two assigned platforms.
This is a static execution count, not a measured runtime claim.

The primary should decide whether one correctly instrumented rebuild sample can report both distinct measurement families.
That alternative can remove repeated installation and source setup while retaining both requirements and their separate metrics.
It must preserve independent authored sample identities, boundary sizes, page checks, and required statistical coverage.
It must not replace native runs with Go-only proof.

Acceptance requires an approved evidence mapping, `make verify-contract`, and both native measurement gates.
Resolve F11 before claiming that any reduced execution set preserves performance evidence.

### Proposed focused validation

These commands were inspected in `Makefile`, but were not executed.

#### Shared driver and complete native regression checks

```sh
make test-conformance-drivers
make test-swift-warm-connect
make test-kotlin-warm-connect
make test-swift-scenarios
make test-kotlin-scenarios
```

#### Focused semantic checks

```sh
make test-swift-scenarios GO_TEST_ARGS='-v -count=1 -p 1 -run TestRealSwiftScenarios/multi-scope-provenance'
make test-kotlin-scenarios GO_TEST_ARGS='-v -count=1 -p 1 -run TestRealKotlinScenarios/assertion/multi-scope-provenance'
make test-swift-scenarios GO_TEST_ARGS='-v -count=1 -p 1 -run TestRealSwiftScenarios/schema-queued-mutation'
make test-kotlin-scenarios GO_TEST_ARGS='-v -count=1 -p 1 -run TestRealKotlinScenarios/assertion/schema-queued-mutation'
make test-swift-scenarios GO_TEST_ARGS='-v -count=1 -p 1 -run TestRealSwiftScenarios/steady-pull'
make test-kotlin-scenarios GO_TEST_ARGS='-v -count=1 -p 1 -run TestRealKotlinScenarios/assertion/steady-pull'
make test-swift-scenarios GO_TEST_ARGS='-v -count=1 -p 1 -run TestRealSwiftScenarios/retention-reconnect'
make test-kotlin-scenarios GO_TEST_ARGS='-v -count=1 -p 1 -run TestRealKotlinScenarios/assertion/retention-reconnect'
```

The primary owns prerequisite setup, negative-control execution, and final acceptance.
The existing driver Make target has no package or test filter variable.
Do not claim that a proposed new targeted driver filter already exists.


<a id="area-12-kotlin-conformance"></a>

## Kotlin conformance-specific findings

<a id="12-kotlin-conformance-f01"></a>

### 12-kotlin-conformance-F01: Socket execution does not observe context cancellation while waiting

- **Severity:** Medium. A canceled command can still execute, and a canceled response wait can continue until the socket deadline.
- **Classification:** Correctness defect.

#### Evidence

`Session.Execute` checks the context before acquiring `requestMu`.
It does not check cancellation again after acquiring that mutex.
It also does not observe `ctx.Done()` while writing or reading the socket.

- `conformance/kotlin/protocol.go:312-361`
- Default request timeout: `conformance/kotlin/session.go:25-31`.

The command deadline uses `ctx.Deadline()` when available.
Calling `cancel()` on a context without a deadline does not change that socket deadline.
That leaves a response wait bounded by the default three-minute timeout rather than cancellation.
If cancellation occurs while another command owns `requestMu`, the waiting command can later be sent despite cancellation.

The Swift boundary handles both cases explicitly:

- Context recheck after serialization: `conformance/swift/protocol.go:632-643`.
- Cancellation during pipe I/O: `conformance/swift/protocol.go:691-718`.
- Negative controls: `conformance/swift/session_test.go:82-137`.

The assigned Kotlin session tests do not cover this cancellation behavior.

#### Smallest coherent simplification

Keep one socket operation owner that handles context cancellation and connection validity.
Check the context again after command serialization.
Connect cancellation to the current I/O deadline or transport close.
Do not retain a second command path or allow a late response to satisfy another command.

Do not copy Swift's per-client process kill into Kotlin.
Kotlin must account for every logical session on the shared instrumentation host if the transport becomes unusable.

#### Invariant

A canceled unsent command must not execute.
A canceled in-flight command must not leave a response that can be attributed to later work.
Host invalidation must not leave peer clients falsely available.

#### Acceptance

- Use a pipe-backed session test with two serialized requests.
- Cancel the second request before releasing the first request.
- Verify that the second request sends no bytes.
- Cancel an in-flight response wait without a context deadline and verify prompt termination.
- Verify that later commands cannot consume the canceled response.
- Run `make test-conformance-drivers`.
- Run a focused real Kotlin scenario that replaces the shared host after interrupted work.

This is static control-flow evidence, not an observed three-minute test failure.
No applicable issue or requirement ID was verified beyond the native process and evidence-boundary invariants.

<a id="12-kotlin-conformance-f02"></a>

### 12-kotlin-conformance-F02: Captured facts remain raw through repeated decoding and projection

- **Severity:** Medium. Multiple consumers repeat the same parsing and completeness checks for one accepted capture.
- **Classification:** Behavior-preserving cleanup.

#### Evidence

`Result` keeps inspection payloads as raw JSON at `conformance/kotlin/protocol.go:83-127`.
The platform then decodes the same data in several layers:

1. `captureClientStateBatch` validates the capture at `conformance/kotlin/platform.go:2819-2911`.
2. That validation decodes retained mutations, scopes, and receipt proofs.
3. `Capture` projects the same result at `conformance/kotlin/platform.go:2740-2760,3005-3077`.
4. The projection decodes those arrays again through `androidQueuedMutationFacts`, `androidCheckpointFacts`, and related functions.

Other consumers repeat these conversions:

- Pending-cycle evidence: `conformance/kotlin/pending_cycle.go:613-750`.
- Queue successor evidence: `conformance/kotlin/queue_replay.go:323-342`.
- Queue CRUD evidence: `conformance/kotlin/queue_replay.go:636-707`.
- Retention queue checks: `conformance/kotlin/retention_reconnect.go:791-820,841-865`.
- Restart observation construction: `conformance/kotlin/platform.go:2030-2204`.
- The broadly reused `warmConnectSnapshot` decoder: `conformance/kotlin/warm_connect.go:366-413`.

The retention queue path directly decodes retained mutations, then calls another function that decodes the same raw array for validation.
The general capture validator has already decoded it before that path runs.
This is repeated interpretation of one evidence schema, not independent native proof.

Swift shows a simpler typed capture boundary at `conformance/swift/protocol.go:90-132`.
Its projections accept typed records at `conformance/swift/platform.go:2215-2413`.
Swift's independent correctness defects in F03 and F05 must not be copied.

#### Smallest coherent simplification

Decode and validate each bounded capture array once at the Go capture boundary.
Pass typed records to projections and scenario assertions.
Start with retained mutations and scope records, which have several concrete consumers.
Delete repeated `decodeFactArray` calls and the second validation pass over unchanged raw data.
Replace the scenario-named `warmConnectSnapshot` with the same decoded capture where its consumers need the same fields.

Keep raw bytes only where exact-byte evidence is required.
Do not merge Kotlin's instrumentation envelope, typed local-value encoding, or receipt proof format with Swift by assumption.

#### Invariant

- Preserve missing, null, explicit-empty, and omitted-over-limit distinctions.
- Preserve retained detail count separately from the full mutation ledger count.
- Reject duplicate identities and unknown members at the boundary.
- Keep raw response hashes and canonical replay data independent of decoded projections.
- Keep aggregate durable fingerprints for captures above detail bounds.

#### Acceptance

- Run `make test-conformance-drivers`.
- Retain tests for unknown members, retained counts, aggregate counts, malformed receipt proofs, and duplicate scope rows.
- Run the focused Kotlin pending-cycle, queue-replay, rebuild-requests, and retention-reconnect commands below.
- Confirm that each accepted capture has one decoding owner by static consumer inspection.
- No runtime speedup is claimed. The observed cost is repeated parsing and schema ownership.

### Proposed focused validation

These are acceptance commands for the primary. They did not run during this review.

```sh
make test-conformance-drivers
make test-kotlin-warm-connect
make test-kotlin-scenarios GO_TEST_ARGS='-v -count=1 -p 1 -run TestRealKotlinScenarios/assertion/pending-cycle'
make test-kotlin-scenarios GO_TEST_ARGS='-v -count=1 -p 1 -run TestRealKotlinScenarios/assertion/queue-replay'
make test-kotlin-scenarios GO_TEST_ARGS='-v -count=1 -p 1 -run TestRealKotlinScenarios/assertion/rebuild-requests'
make test-kotlin-scenarios GO_TEST_ARGS='-v -count=1 -p 1 -run TestRealKotlinScenarios/assertion/retention-reconnect'
make test-kotlin-scenarios GO_TEST_ARGS='-v -count=1 -p 1 -run TestRealKotlinScenarios/assertion/multi-scope-provenance'
make test-kotlin-scenarios GO_TEST_ARGS='-v -count=1 -p 1 -run TestRealKotlinScenarios/assertion/schema-queued-mutation'
make test-kotlin-scenarios GO_TEST_ARGS='-v -count=1 -p 1 -run TestRealKotlinScenarios/assertion/steady-pull'
```

The shared extraction also requires the Swift regression commands in the companion report.
Measurement and proof-obligation changes require `make verify-contract` and their explicit negative controls.
The primary must record the exact clean commit used for executable validation.


<a id="area-13-react-native-conformance"></a>

## React Native conformance

<a id="13-react-native-conformance-f01"></a>

### 13-react-native-conformance-F01: Client detail captures become count-only proof

- Severity: High. Wrong local rows and wrong scope ownership can retain the required counts and pass this proof.
- Classification: correctness defect.
- Problem: `conformance/reactnative/multi_scope_provenance.go:585-627` and `conformance/reactnative/multi_scope_provenance.go:978-1049`.
- Authored comparison: `conformance/scenarios/performance/multi-scope-provenance-001.json:954-1082`.
- Server comparison excludes clients: `conformance/reactnative/validation.go:1270-1284`.
- `validateMultiScopeProvenanceClient` checks counts, not the authored `Provenance` records, row identities, versions, scopes, or row values.
- `validateMultiScopeProvenanceCapture` only decodes application rows and checks internal count consistency.
- `validateMultiScopeProvenanceNoProgress` compares two observations. Stable incorrect state can pass that comparison.
- The authored client facts include specific row identities, scope sets, and versions. Those fields are not optional evidence here.
- Simplification: project observed client facts into the existing authored fact comparison. Delete the separate count-only approximation.
- Invariant: selective synchronization must preserve each row's authoritative scope provenance.
- Acceptance: change a captured row's scope or identity without changing counts. The native assertion must fail.
- Proposed commands: `make test-conformance-drivers`, `make test-rn-provenance-ios`, and `make test-rn-provenance-android`.
- Requirement references: `SCN-PERF-MULTI-SCOPE-PROVENANCE-001`, `EXPECT-PERF-MULTI-SCOPE-PROVENANCE-SEMANTIC-001`, and `CTRL-PROVENANCE-002`.
- Static proof only. No mutant or integrated run occurred.

The same count-only defect affects rejected mutation details in queue-replay.
`conformance/reactnative/queue_replay.go:1357-1362` checks array length but never inspects an entry.
Its passing fixture uses empty strings as rejected records at `conformance/reactnative/queue_replay_test.go:539-558`.
The extra capture exchange therefore does not prove an inspectable rejection, its identity, or its reason.
The client contract requires inspectable authored intent and outcomes at `docs/src/content/docs/spec/02-client-contract.mdx:145-175`.
Keep the bounded capture, but compare the rejected identities and reasons with the authored workload partition.
Reject empty entries and duplicated identities while retaining the expected count.
Use `make test-rn-queue-replay-ios` and `make test-rn-queue-replay-android` for that acceptance flow.

<a id="13-react-native-conformance-f02"></a>

### 13-react-native-conformance-F02: Result validators do not consistently enforce closed envelopes or operation completion

- Severity: Medium. Some scenarios accept incomplete command results that other scenarios reject.
- Classification: correctness defect.
- Problem: `conformance/reactnative/coordinator.go:507-508`, `conformance/reactnative/coordinator.go:534-538`, and `conformance/reactnative/multi_scope_provenance.go:412-415`.
- Shared decoder: `conformance/reactnative/validation.go:358-370` and `conformance/reactnative/validation.go:432-459`.
- Stronger comparison: `conformance/reactnative/pending_cycle.go:1029-1049`, `conformance/reactnative/pending_cycle.go:1067-1087`, and `conformance/reactnative/pending_cycle.go:1254-1277`.
- Warm-connect accepts a synchronized result based only on `kind`.
- Multi-scope accepts stop and synchronization results based only on `kind`.
- Thus `{"kind":"synchronized"}` satisfies these stage checks without completion, status, or process evidence.
- `decodeCapture` requires three outer members but does not require or validate `process`.
- A capture with `kind`, `capture`, and an unknown replacement member satisfies that outer-shape check.
- Pending-cycle separately validates the capture process. Warm-connect, multi-scope, forged-cursor, and rebuild-apply do not use that check.
- Simplification: make each shared result decoder enforce its full closed shape. Reuse the existing process and completion checks.
- Delete per-scenario partial decoders after their complete replacements exist. Do not add another command framework.
- Invariant: each stage must prove the requested public operation and preserve the applicable process/database identity.
- Acceptance: remove completion, replace process with an unknown key, change the process, and return a wrong completion.
- Every affected coordinator must reject each invalid result. Run `make test-conformance-drivers` and the affected focused RN targets.
- `conformance/reactnative/validation.go:1163-1168` also accepts JSON `null` as an empty array.
- `conformance/reactnative/validation.go:727-740` accepts `null` as integer zero, including fresh-connect zero-valued facts.
- `conformance/reactnative/validation.go:413-419` accepts arbitrary short status names and unknown replacement members.
- `conformance/internal/jsonstrict/decode.go:45-61` rejects duplicate JSON members, but it does not reject unknown struct fields.
- These checks need explicit member presence and value types. A member count alone does not close an envelope.
- `conformance/reactnative/steady_pull.go:702-710` checks the reopened database but omits the new-process requirement.
- Its Detox consumer actually terminates and relaunches at `clients/react-native/example/e2e/steady-pull.test.ts:198-205`.
- Reuse the restart identity check already present at `conformance/reactnative/multi_scope_provenance.go:870-881`.
- Static proof only. These are acceptance defects, not evidence that the current bridge emits the invalid results.

<a id="13-react-native-conformance-f03"></a>

### 13-react-native-conformance-F03: Full-flow tests construct their own flow instead of executing it

- Severity: Medium. The tests can pass after a coordinator transition disappears or changes.
- Classification: correctness defect.
- Problem: `conformance/reactnative/queue_replay_test.go:257-400` and `conformance/reactnative/rebuild_apply_test.go:66-131`.
- Compared implementation: `conformance/reactnative/queue_replay.go:920-1060`, `conformance/reactnative/rebuild_apply.go:458-500`.
- Queue-replay constructs `want`, loops over `want`, and counts those entries. It never calls the actual transition function.
- Rebuild-apply builds open and synchronize commands from the expected actor and command itself.
- Only capture commands execute a real transition. Its terminal assertion checks its own expected object.
- Simplification: remove the synthetic full-flow simulation. Keep focused command serialization tests only where they protect transport behavior.
- Put exchange-count and terminal-state assertions on the existing actual coordinator journey.
- Invariant: the real authored journey must execute all required stages and finish exactly once.
- Acceptance: change an actual next-stage transition while leaving `ExchangeCount` unchanged. The actual-flow test must fail.
- Proposed command: `make test-conformance-drivers`. Integrated journeys remain under the existing RN targets.
- Static call-graph proof only. No mutant ran.

<a id="13-react-native-conformance-f04"></a>

### 13-react-native-conformance-F04: Rebuild-apply collects integrity proof and ignores its semantic results

- Severity: High. A checksum or receipt-chain failure can keep valid counts and satisfy the RN rebuild-apply validator.
- Classification: correctness defect.
- Problem: `conformance/reactnative/rebuild_apply.go:555-667`.
- Compared stronger receipt validation: `conformance/reactnative/rebuild_cardinality.go:995-1021` and `conformance/reactnative/push_response_loss.go:1381-1396`.
- Test fixture: `conformance/reactnative/rebuild_apply_test.go:306-388`.
- The validator checks receipt page totals and attempt counts.
- It does not check `RequestChainValid`, `RecordsInCanonicalOrder`, `RowChecksumsValid`, `ScopeChecksumValid`, or `FinalChecksumMatches`.
- It does not check each receipt's returned record total against the authored workload.
- The fixture supplies true integrity flags, but no inspected test makes a false flag fail.
- Simplification: validate the collected terminal integrity proof directly. Delete the count-only substitute for semantic proof.
- Keep bounded aggregate inspection for large workloads. Do not require an unbounded row dump.
- Invariant: finalized rebuild data and durable checkpoints must agree with validated server receipt content.
- Acceptance: independently set each integrity flag false while retaining all counts. Every mutation must fail the existing assertion.
- Proposed commands: `make test-conformance-drivers`, `make test-rn-rebuild-apply-ios`, and `make test-rn-rebuild-apply-android`.
- References: `SCN-PERF-REBUILD-APPLY-001`, `CTRL-REBUILD-002`.
- Static proof only.

<a id="13-react-native-conformance-f05"></a>

### 13-react-native-conformance-F05: Repeated Go coordinator plumbing has already diverged

- Severity: Medium. Each protocol fix requires parallel edits and permits inconsistent failure handling.
- Classification: behavior-preserving cleanup.
- Compared handlers: `conformance/reactnative/coordinator.go:422-486`, `conformance/reactnative/forged_cursor.go:689-744`, `conformance/reactnative/multi_scope_provenance.go:332-386`, and `conformance/reactnative/pending_cycle.go:669-739`.
- Warm-connect and pending-cycle report oversized declared bodies as 413. Forged-cursor and multi-scope report 415.
- Multi-scope does not persist a sequence mismatch as failure. Warm-connect and pending-cycle do.
- Multi-scope omits the response-size check present in the other inspected handlers.
- Constructors, lifecycle methods, command assembly, and identity-observation fan-out repeat the same Go behavior.
- Compared runners: `conformance/reactnative/forged_cursor_integration_test.go:31-174` and `conformance/reactnative/multi_scope_provenance_integration_test.go:25-129`.
- Further copies: `conformance/reactnative/pending_cycle_integration_test.go:25-153` and `conformance/reactnative/queue_replay_integration_test.go:26-135`.
- These runners repeat provisioning, cleanup, environment filtering, Detox invocation, selected-result validation, and shutdown joins.
- Simplification: use small shared functions for the existing HTTP boundary and common integration setup. Keep scenario transitions explicit.
- Delete copied transport and harness mechanics. Do not create a generic scenario interpreter or new framework.
- Invariant: preserve capability authentication, bounds, strict sequencing, isolated databases, selected Jest identity, and scenario-specific barriers.
- Acceptance: exercise one shared boundary matrix and all existing scenario journeys through the same helpers.
- Preserve scenario transitions and fault behavior during extraction. The primary must resolve transport differences explicitly, not silently.
- Proposed command: `make test-conformance-drivers`. Then run each affected existing focused RN target on both platforms.
- This matches the repeated RN journey protocol problem identified by issue `#93` in the dispatch.
- I did not inspect or change the issue itself.

<a id="13-react-native-conformance-f06"></a>

### 13-react-native-conformance-F06: Go platform drivers duplicate deterministic workload and schema construction

- Severity: Medium. Shared input construction has three implementations and has already acquired different validation.
- Classification: behavior-preserving cleanup.
- Problem: `conformance/reactnative/queue_replay.go:1992-2147`.
- Compared Go implementations: `conformance/kotlin/queue_replay.go:875-1022` and `conformance/swift/queue_replay.go:822-983`.
- All three expand the same authored workload, create the same deterministic UUIDs, and verify the authored operation digest.
- All three construct the same next schema and apply the same canonical manifest hash domain.
- Swift and Kotlin reject an existing generated field name. React Native omits that check.
- This is repeated Go input construction, not necessary repetition between Swift and Kotlin production engines.
- Simplification: give these deterministic scenario inputs one implementation in the existing shared conformance layer.
- Concrete consumers are the Swift, Kotlin, and React Native Go drivers. Delete their copied builders and schema transport types.
- Keep platform launch, process control, and client observations in their current drivers.
- Invariant: every platform must consume the same authored operation digest and schema transition.
- Acceptance: verify the shared builder against the existing authored digests and reject a colliding generated field.
- Proposed commands: `make test-conformance-drivers` and `make test-conformance-scenarios`.
- Reference: `SCN-PERF-QUEUE-REPLAY-001`. Static comparison only.

<a id="13-react-native-conformance-f07"></a>

### 13-react-native-conformance-F07: Diagnostic text tests preserve unnecessary validation machinery

- Severity: Low. Harmless diagnostic changes require test changes and preserve large formatting-only paths.
- Classification: behavior-preserving cleanup.
- Problem tests: `conformance/reactnative/queue_replay_test.go:128-149` and `conformance/reactnative/rebuild_requests_test.go:609-704`.
- Compared implementation: `conformance/reactnative/queue_replay.go:1108-1137` and `conformance/reactnative/rebuild_requests.go:1548-1681`.
- A one-consumer facts object retains values and errors mainly to print every field, including fields that already match.
- Tests require those successful-field summaries and their exact spelling.
- Other examples occur at `conformance/reactnative/forged_cursor_test.go:244-251` and `conformance/reactnative/push_response_loss_test.go:517-545`.
- The contract does not make those internal diagnostic sentences part of the wire protocol.
- Simplification: report the failed constraint with observed and expected values. Delete complete-summary string assertions and formatting-only state.
- Keep semantic negative controls and useful operation context.
- Source scans at `conformance/reactnative/schema_check_test.go:279-289` and `conformance/reactnative/schema_queued_mutation_test.go:198-208` have a related weakness.
- They reject comments containing a forbidden substring and miss renamed or indirect access.
- Remove those substring tests as architectural proof. Keep the dependency boundary in the existing architecture checks or review.
- Invariant: errors must remain useful, and semantic defects must still fail tests.
- Acceptance: change diagnostic wording without changing behavior. Semantic tests must remain unchanged.
- Proposed command: `make test-conformance-drivers`. Static proof only. No issue match was established.

<a id="13-react-native-conformance-f08"></a>

### 13-react-native-conformance-F08: Dead helpers and unused configuration remain in the coordinator package

- Severity: Low. The package exposes configuration and helper paths that do not affect a journey.
- Classification: behavior-preserving cleanup.
- Unused functions: `conformance/reactnative/pending_cycle.go:2062-2082` and `conformance/reactnative/validation.go:473-479`.
- Exact package searches found only the definitions of `pendingCyclePullScopeCount`, `validatePendingCycleCapture`, and `captureTrace`.
- `conformance/reactnative/queue_replay.go:425-426` keeps a `StageCount` alias used only by its alias-equality test.
- The actual runner uses `ExchangeCount` at `conformance/reactnative/queue_replay_integration_test.go:99`.
- All 14 coordinator configuration types contain `AppVersion`. Package searches found declarations, defaults, and test assignments, but no effective use.
- Examples: `conformance/reactnative/coordinator.go:39-49`, `conformance/reactnative/coordinator.go:207-209`, and `conformance/reactnative/coordinator.go:769-783`.
- Another example: `conformance/reactnative/seeded_empty_startup.go:27-38` and `conformance/reactnative/seeded_empty_startup.go:833-847`.
- Simplification: delete unused helpers, the queue alias, its tautological test, unused version fields, and their defaults.
- Do not delete seeded-startup's separate `StageCount`, which has a real integration consumer.
- Replace local slice-equality implementations with `slices.Equal` where their semantics match.
- Examples: `conformance/reactnative/push_response_loss.go:148-170`, `conformance/reactnative/push_response_loss.go:1813-1823`, and `conformance/reactnative/rebuild_requests.go:1891-1901`.
- Invariant: effective command runtime values and scenario behavior must not change.
- Acceptance: confirm zero remaining consumers for deleted symbols and run `make test-conformance-drivers`.
- No issue match was established. These are static consumer checks.

<a id="13-react-native-conformance-f09"></a>

### 13-react-native-conformance-F09: Close deadlines cannot interrupt several blocked exchanges

- Severity: Medium. Cleanup can exceed its deadline while waiting for the exchange mutex.
- Classification: correctness defect.
- Problem: `conformance/reactnative/forged_cursor.go:663-679`, `conformance/reactnative/forged_cursor.go:716-730`, and `conformance/reactnative/forged_cursor.go:959-994`.
- Further copies: `conformance/reactnative/push_response_loss.go:724-740`, `conformance/reactnative/push_response_loss.go:777-789`, and `conformance/reactnative/push_response_loss.go:1911-1925`.
- An exchange holds `c.mu` while it waits for a proxy signal.
- `Close(ctx)` waits for `c.mu` before it releases the signals or calls `Shutdown(ctx)`.
- The close deadline does not apply to mutex acquisition. The exchange uses its request context, not the Serve context.
- Thus a live blocked request prevents Close from reaching the code intended to release it.
- Correct comparison: `conformance/reactnative/rebuild_requests.go:616-633` signals failure and releases barriers before acquiring `c.mu`.
- Its focused test covers this exact dependency at `conformance/reactnative/rebuild_requests_test.go:384-427`.
- Simplification: apply that existing cancellation order to the other signal-based coordinators.
- Delete reliance on a later device fetch timeout to release cleanup.
- Invariant: cancellation must fail waiters, release held responses, and preserve the original failure.
- Acceptance: hold the exchange mutex in a barrier wait with a live request context. Close must finish within its own finite deadline.
- Proposed command: `make test-conformance-drivers`.
- Static wait-dependency proof only. I did not reproduce a stalled process.

<a id="13-react-native-conformance-f10"></a>

### 13-react-native-conformance-F10: Schema dispatch proof does not check the dispatch outcome

- Severity: High. An affected Class 3 client can omit its rebuild and still satisfy the inspected RN checks.
- Classification: correctness defect.
- Problem: `conformance/reactnative/schema_check.go:946-983` and `conformance/reactnative/schema_check.go:1091-1158`.
- Authored cases: `conformance/scenarios/performance/schema-check-001.json:1008-1042`.
- Normative distinction: `docs/src/content/docs/spec/02-client-contract.mdx:432-446`.
- The validator checks final schema, one scope identifier, status, and the presence of a successful connect.
- It does not check the connect schema action or the required affected-scope rebuild behavior.
- Sample counters count authored case labels. They do not establish observed dispatch.
- `capture.Events` is collected but does not participate in this validation.
- Simplification: compare each call's observed dispatch and rebuild outcome with its authored case in one validator.
- Delete the connect-presence substitute and redundant sample-count inference as semantic proof.
- Keep the authored sample inventory and existing measurement obligations.
- Invariant: affected Class 3 scopes rebuild. Unaffected Class 3 scopes replace without an empty rebuild action.
- Acceptance: remove the affected rebuild, then add an unnecessary unaffected rebuild. Each mutant must fail the applicable case.
- Proposed commands: `make test-conformance-drivers`, `make test-rn-check-ios`, and `make test-rn-check-android`.
- References: `EXPECT-PERF-SCHEMA-CHECK-DISPATCH-001`, `CTRL-SCHEMA-004`.
- Static proof only. The current native engines may perform the correct dispatch.

<a id="13-react-native-conformance-f11"></a>

### 13-react-native-conformance-F11: Retention proof calls a cursor floor-equal without proving the floor

- Severity: High. A no-op compaction can pass the RN compaction and floor-resume checks.
- Classification: correctness defect.
- Problem: `conformance/reactnative/retention_reconnect.go:1181-1309` and `conformance/reactnative/retention_reconnect.go:1591-1670`.
- Weak passing fixture: `conformance/reactnative/retention_reconnect_test.go:305-326`.
- The compaction validator proves only that one scope and one continuing rebuild pin exist.
- It does not compare the retained floor, deleted prefix, or unaffected scope history.
- `floorScopes` labels a nonempty local scope cursor as the floor without comparing it with authoritative floor evidence.
- A normal cursor also passes the restart and zero-change resume checks when compaction does nothing.
- The controller does not provide a hidden semantic assertion here.
- `conformance/blackbox/native_controller.go:1376-1380` returns success when the diagnostic compaction call returns without error.
- `conformance/blackbox/push_retention_controls.go:129-156` accepts zero floor and deletion results. It does not establish the missing semantic outcome.
- The authored requirement explicitly requires floor advance and atomic prefix deletion at `conformance/scenarios/server/retention-reconnect-001.json:1168-1191`.
- Simplification: bind the resume cursor to independent compacted-floor evidence and compare the authored retention effect.
- Delete the misleading pin-presence substitute for compaction proof.
- Invariant: compaction preserves active pins, changes only the authorized scope prefix, and retains valid floor-equal continuation.
- Acceptance: make compaction a no-op. The test must fail before it claims floor-equal resume.
- Proposed commands: `make test-conformance-drivers`, `make test-rn-retention-ios`, and `make test-rn-retention-android`.
- References: `SYNC-RETENTION-001`, `SYNC-RETENTION-003`, `EXPECT-RETENTION-RECONNECT-COMPACTION-001`.
- Static proof only.

<a id="13-react-native-conformance-f12"></a>

### 13-react-native-conformance-F12: Queued-schema proof derives its expected base from the client under test

- Severity: Medium. The proof can accept a consistently wrong retained base version.
- Classification: correctness defect.
- Problem: `conformance/reactnative/schema_queued_mutation.go:853-869` and `conformance/reactnative/schema_queued_mutation.go:900-947`.
- Dependent checks: `conformance/reactnative/schema_queued_mutation.go:1019-1048` and `conformance/reactnative/schema_queued_mutation.go:1124-1146`.
- `serverEvidence` reads `rowVersion` and `rowChecksum` from the final native `DurableProof`, not the captured server row.
- Those values replace the runtime aliases later used to validate the native queued and stored mutation.
- Therefore, changing both native metadata and the retained base to the same wrong token can satisfy this comparison.
- The test explicitly builds this path without server row evidence at `conformance/reactnative/schema_queued_mutation_test.go:162-195`.
- Existing independent resolution is available in `conformance/reactnative/multi_scope_provenance.go:718-731`.
- It reads the runtime version from the controller's observed row-version mapping.
- Simplification: resolve the base version and checksum from the independent server capture or its controller mapping.
- Delete the client-derived expected alias overwrite.
- Invariant: the retained authored mutation must preserve the actual server-issued base across schema reset and restart.
- Acceptance: corrupt native metadata and both retained base representations after the observed push. Independent server evidence must make the assertion fail.
- Proposed commands: `make test-conformance-drivers`, `make test-rn-sqm-ios`, and `make test-rn-sqm-android`.
- References: `SCN-SCHEMA-QUEUED-MUTATION-001`, `SYNC-MUTATION-004`, `SYNC-BOUNDARY-002`.
- Related known issue: `#36`, oracle replacement. This finding identifies one concrete client-derived expectation path.
- Static proof only.

<a id="13-react-native-conformance-f13"></a>

### 13-react-native-conformance-F13: Queue trace combination removes evidence of missing terminal observations

- Severity: Medium. A terminal snapshot with a mismatched checkpoint becomes a valid-looking combined trace.
- Classification: correctness defect.
- Problem: `conformance/reactnative/queue_replay.go:1237-1259`.
- Decoder: `conformance/reactnative/validation.go:481-489`.
- Stronger comparison: `conformance/reactnative/push_response_loss.go:1482-1498` and `conformance/reactnative/rebuild_requests.go:1470-1490`.
- Queue-replay validates observation sequence numbers but does not require its terminal checkpoint to equal the observation count.
- It then replaces that checkpoint with the combined count and renumbers the records.
- A one-record terminal trace with checkpoint two loses that discrepancy during combination.
- Simplification: use the same complete-snapshot precondition for every segment before renumbering.
- Delete the weaker parallel combination path, or make it call the existing common trace validation.
- Invariant: a combined trace must not manufacture completeness from an incomplete segment.
- Acceptance: retain contiguous records but increase the terminal checkpoint. Combination must fail.
- Proposed command: `make test-conformance-drivers`. Reference: `SCN-PERF-QUEUE-REPLAY-001`.
- Static proof only.

<a id="13-react-native-conformance-f14"></a>

### 13-react-native-conformance-F14: Exact sidecar exchange counts duplicate the scenario state machine

- Severity: Low. Every added capture or restart requires a separate count change and more protocol tests.
- Classification: contract decision.
- Count implementations: `conformance/reactnative/queue_replay.go:404-426`, `conformance/reactnative/rebuild_cardinality.go:405-424`, and `conformance/reactnative/schema_check.go:621-627`.
- Separate fixed count: `conformance/reactnative/schema_queued_mutation.go:407-408`.
- Consumer: `clients/react-native/example/e2e/multi-scope-provenance.test.ts:72-81`.
- Another consumer hardcodes 22 exchanges at `clients/react-native/example/e2e/steady-pull.test.ts:184-196`.
- The sidecar already owns command order, sequence checks, final evidence, and an explicit complete response.
- Consider removing exact exchange cardinality from the sidecar protocol.
- A consumer can run until validated completion under one finite deadline and a generous safety bound.
- Keep expected scenario-step coverage in the coordinator. Exact network request counts remain where an authored performance requirement needs them.
- This deletes `ExchangeCount` formulas, environment count plumbing, duplicated consumer literals, and synthetic count tests.
- It must not permit early semantic completion, zero-step success, or an infinite command loop.
- Acceptance: add a diagnostic capture without changing semantic results. No independent exact-count contract should require an edit.
- Separately remove a required scenario step. Terminal semantic validation must still fail.
- Proposed commands: `make test-conformance-drivers` and the existing affected focused RN targets.
- Related issue: `#93`. This proposal is not approved and changes no product protocol requirement.


<a id="area-14-reference-model"></a>

## Reference model and modelrunner

<a id="14-reference-model-f01"></a>

### 14-reference-model-F01: Workload preparation requires a complete protocol interpreter

- **Severity:** Medium. Real native tests must maintain and execute unrelated model behavior before they can prepare their workloads.
- **Classification:** Behavior-preserving cleanup, deferred under the approved R3 replacement contract.
- **Existing issue:** Matches the oracle-replacement scope of `#36`. This review does not claim that issue's acceptance criteria are satisfied.

**Evidence**

`RunScenario` runs all model operations, repeats the execution, and evaluates semantic predicates.
See `conformance/modelrunner/runner.go:51-58,114-149` and `conformance/modelrunner/semantic.go:21-46`.

Swift cardinality needs `modelStep.Expanded` to prepare the real source workload.
It therefore executes client rebuild behavior inside the model before executing the real native client.
See `conformance/swift/rebuild_cardinality.go:89-125`.

Kotlin provenance checks that model operations equal the authored operations, then uses those model copies.
See `conformance/kotlin/multi_scope_provenance.go:159-184,290-303`.
React Native rebuild requests uses the model as a pass prerequisite, then applies operations from its authored step map.
See `conformance/reactnative/rebuild_requests.go:455-464,477-497`.

This is not just repeated syntax across languages.
Input generation, model correctness, and real-system execution have separate reasons to change, but one dependency couples all three.

**Smallest coherent simplification**

During R3 migration, let drivers consume authored operations directly where they already do so.
For workload macros, retain deterministic input generation without model server or client execution.
Use the existing authored facts and independent invariant engine to judge real observations.
Delete each corresponding model pass prerequisite only when its semantic checks have a named replacement home.

Do not relocate the whole model into a new workload package.
Do not create another protocol engine to replace this engine.
Do not delete the existing oracle before replacement coverage exists.

**Invariant**

Expected behavior must remain independent of production output.
Every migrated semantic check must retain executable proof and its required negative control.

**Acceptance**

- Track each removed dependency through the existing R3 deletion tables.
- Run `make test-conformance-drivers test-conformance-scenarios test-invariants` on the integrated commit.
- Run each affected real native scenario through its supported Make gate.
- Search for remaining imports before deleting the packages.
- Static inspection cannot establish replacement coverage or authorize R3.5.

<a id="14-reference-model-f02"></a>

### 14-reference-model-F02: WAL effects and pull hydration disagree on valid effect shapes

- **Severity:** High. The oracle cannot deliver required deletion and dependency-driven membership effects through its own pull path.
- **Classification:** Correctness defect in the reference model. This is not a claim about production server behavior.
- **Applicable contract:** Wire protocol pull shapes and selected-candidate conservation.

**Evidence**

`conformance/reference/wal.go:1113-1128` creates scoped deletes without a captured projection or row checksum.
`conformance/reference/wal.go:1502-1512` stores those absent fields unchanged.
`conformance/reference/pull.go:902-913` rejects every candidate without both fields.
The endpoint converts that rejection into HTTP 500 at `conformance/reference/pull.go:229-239`.

The same mismatch affects dependency-driven upserts.
The materializer assigns their dependency event as `source`, but selects the row's latest captured projection independently.
See `conformance/reference/wal.go:1062-1075,1129-1147,1455-1467`.
Hydration incorrectly requires that projection event to equal the causal dependency event.
See `conformance/reference/pull.go:907-912`.

`TestWALDependencyChangesEnterAndLeaveScopesUsingCurrentProjectedVersion` checks the effects but never pulls them.
See `conformance/reference/wal_operations_test.go:257-301`.
The pull fixture helper creates only upserts with projections and checksums.
See `conformance/reference/pull_rebuild_operations_test.go:749-770`.
These separate tests do not cover the incompatible producer and consumer together.

The wire contract expressly permits rowless scoped deletes.
It distinguishes captured row data from the effect that causes scope entry or removal.
See `docs/src/content/docs/spec/01-wire-protocol.mdx:806-810,824-842,866-870`.

**Smallest coherent simplification**

Make hydration dispatch on the effect operation before requiring row data.
Handle rowless deletes without projection hydration.
For upserts, validate the selected projection key and row content without conflating the projection event with the causal event.
Apply the same shape distinction to local planning at `conformance/reference/pull.go:428-449`.
Delete the unconditional projection and checksum requirements for rowless deletes.

**Invariant**

Every selected effect must produce its required change or fail the entire page.
Preserve exact captured-row integrity for upserts and atomic progress on failure.

**Acceptance**

Extend the existing WAL membership flow through pull and local apply.
Cover scope entry from a dependency event and scope removal without a row payload.
Require HTTP 200, exact effects, preserved unrelated provenance, and safe cursor progress.
Run `make test-reference test-conformance-scenarios`.
Restore each erroneous hydration condition as a mutant and require the composed flow to fail.

Static branch tracing establishes the mismatch. This review did not execute that reproduction.

<a id="14-reference-model-f03"></a>

### 14-reference-model-F03: Local apply obtains missing response evidence from mutable server state

- **Severity:** High. Model success can conceal absent client tokens and unverified local contents.
- **Classification:** Correctness defect in the reference model's client boundary.
- **Applicable inventory:** `REF-299`, `REF-301`, `REF-303` through `REF-306` require careful migration, not literal preservation of these shortcuts.

**Evidence**

`resolveModelToken` first searches local state, then supplies a server-held token when local state has none.
Both incremental cursors and rebuild continuations have this fallback.
See `conformance/reference/pull.go:1019-1063`.
The source names remain `local_checkpoint` and `local_rebuild_continuation`.
Issuance alone therefore can satisfy a request that claims durable local possession.

Pull observations contain only a cursor disposition, not the issued token and response-bound position.
See `conformance/reference/operations.go:118-140`.
Local apply reads the current server checkpoint instead of evidence retained with the selected source step.
See `conformance/reference/pull.go:491-525`.
A later server issuance can change the token that an earlier source step installs.

Terminal verification compares the response checksum with the current server scope checksum.
It does not recompute the post-apply local scope digest.
It then sets `Verified = true`.
See `conformance/reference/pull.go:380-423,529-561`.
For example, an omitted local row outside the current page can remain undetected while the model installs a verified checkpoint.

The current test explicitly compares local progress with server state.
Its negative cases mutate server projection, checksum, or cursor state rather than an unrelated local row.
See `conformance/reference/resolved_operations_test.go:78-144`.

The contract requires durable client possession, response-bound cursors, and local terminal checksum recomputation.
See `docs/src/content/docs/spec/01-wire-protocol.mdx:120,848-860,875-886`.

**Smallest coherent simplification**

Delete the server-token fallbacks for explicitly local token sources.
Retain immutable page evidence with the resolved source result where the model still needs client apply.
Judge local finality from that page and the resulting local row/provenance set.
Do not consult current server state to replace missing received evidence.

Prefer migrating these checks to the existing independent observation engine during R3.
Do not expand the frozen model into another native runtime.

**Invariant**

Server issuance must not imply client receipt or durable apply.
An older response must not install a newer response's progress.
Only independently verified local contents may produce a verified terminal checkpoint.

**Acceptance**

Require negative controls for these cases:

1. A server-issued token exists, but its local token is absent.
2. A second pull issues another token before the first page applies.
3. An unrelated local row is missing or corrupted before a terminal page applies.

Run `make test-reference test-conformance-scenarios` for a bounded oracle correction.
For migration, also run `make test-invariants` and the affected real native scenario gate.
No executable result is claimed here.

<a id="14-reference-model-f04"></a>

### 14-reference-model-F04: Transport failures fabricate an HTTP response

- **Severity:** Medium. The oracle distinguishes equivalent no-response failures inconsistently and can validate the wrong retry observation.
- **Classification:** Correctness defect with a behavior-preserving structural cleanup after correction.
- **Applicable contract:** Durable retry distinguishes network failure from HTTP 503.

**Evidence**

`executePush` handles `transport_failure` before server dispatch, but returns HTTP 503 with a canonical error body and retry header.
See `conformance/reference/push.go:420-427,2036-2056`.
`process/response-loss` also creates that synthetic response at `conformance/reference/push.go:408-413`.

The `drop_after_server` path already uses the correct no-response representation.
See `conformance/reference/push.go:494-500,1175-1181,2032-2034`.
The replay function retains another transport branch that the earlier dispatch branch makes unreachable.
See `conformance/reference/push.go:1170-1174`.

Tests require fabricated HTTP 503 for transport failure, but require status zero for a dropped response.
Compare `conformance/reference/push_operations_test.go:886-912` with `1549-1557`.
The modelrunner rejects a fabricated body on a status-zero transport observation.
See `conformance/modelrunner/runner.go:571-572` and `conformance/modelrunner/runner_test.go:783-803`.

**Smallest coherent simplification**

Use the existing `pushTransportFailureResult` for all actual no-response failures.
Keep HTTP 503 only for an observed HTTP response.
Delete the unreachable replay transport branch.
Correct the affected tests rather than preserving their incorrect HTTP expectation.

**Invariant**

Preserve the sealed request, exact retry identity, server commit boundary, and durable backoff.
Do not invent response bytes or `Retry-After` when no response arrived.

**Acceptance**

Run `make test-reference test-conformance-scenarios`.
Require status zero, no error body, no retry header, and unchanged server state for pre-dispatch transport failure.
Retain a separate real HTTP 503 case.

<a id="14-reference-model-f05"></a>

### 14-reference-model-F05: Seed verification does not bind artifact bytes to installed rows

- **Severity:** Medium. The model's artifact-integrity checks can pass for bytes unrelated to the installed seed data.
- **Classification:** Correctness defect in verification evidence, not a demonstrated production seed defect.
- **Applicable inventory:** `REF-488` and modelrunner seed-validation rows need a real artifact proof home during R3.

**Evidence**

`PortableSeedFixture` carries artifact bytes, manifest bytes, and separately supplied schema, scope, and row facts.
See `conformance/reference/resolved.go:16-47`.
The builder serializes those facts into bytes at `conformance/modelrunner/seed.go:476-499`.
Neither validator decodes those bytes to establish that they describe the supplied rows.
See `conformance/modelrunner/seed.go:503-547` and `conformance/reference/seed.go:115-145,198-253`.

The reference test fixture makes the gap explicit.
It supplies `deterministic portable seed artifact` as artifact bytes and an unrelated fixture-ID object as manifest bytes.
The rows come from separate fields.
See `conformance/reference/resolved_operations_test.go:249-289`.

Rehashing substituted nonempty artifact bytes leaves the reference verifier's digest checks satisfied.
The remaining row checks inspect the separate row representation.
Thus a valid hash pair does not prove that the installed data came from the verified artifact.

**Smallest coherent simplification**

During migration, use one verified artifact as the source of installed data.
Decode its contents and compare the resulting schema, lineage, rows, and scopes with authored expectations.
Delete the parallel unbound row representation from the artifact-proof path.
Keep abstract model seed setup only if its result is explicitly not artifact-integrity evidence.

Do not remove seed integrity checks or replace them with trusted precomputed model fields.

**Invariant**

The exact verified bytes must determine the installed data.
Installation must not grant assignment, authentication, or a runtime checkpoint.

**Acceptance**

Add a control that replaces artifact content, updates its hash, and leaves the separate expected rows unchanged.
The artifact verifier must reject that mismatch.
Run `make test-reference test-conformance-scenarios` for any current-model correction.
The final migration also needs the existing real seed-installation proof, not only this model gate.

<a id="14-reference-model-f06"></a>

### 14-reference-model-F06: Unsampled macro operations copy and normalize the complete state repeatedly

- **Severity:** Medium. Workload size multiplies avoidable complete-state allocations and sorting during every model run and replay.
- **Classification:** Behavior-preserving cleanup.
- **Applicable phase:** R3.0 permits deletions and bounded defect fixes without growing the oracle.

**Evidence**

For every expanded operation, the runner takes a snapshot for input resolution, another before dispatch, and another after dispatch.
See `conformance/modelrunner/runner.go:206-217`.
Most operations use neither the resolved snapshot nor the before/after sample snapshots.
`resolvedInputForOperation` uses the snapshot only for seed construction.
See `conformance/modelrunner/runner.go:449-477`.
Before/after sample snapshots are consumed only when the operation is a configured sample.

Every `Snapshot` clones the complete state and normalizes its nested collections.
See `conformance/reference/model.go:66-72` and `conformance/reference/normalize.go:1031-1115`.
Queue and configured-limit workloads generate hundreds or thousands of local operations.
See `conformance/modelrunner/workload_queue_limits.go:77-118` and `conformance/modelrunner/workload_configured_limits.go:455-485`.
Replay repeats these costs at `conformance/modelrunner/semantic.go:21-46`.

This finding does not require a benchmark to establish unused copies.
It does not claim a measured runtime or memory improvement.

**Smallest coherent simplification**

Take per-operation before/after snapshots only for operations whose sample checks consume them.
Resolve prior results without copying unrelated model state.
Take a model snapshot for seed construction only when that operation needs it.
Retain macro-level snapshots and failure snapshots.

**Invariant**

Preserve replay equality, sample error atomicity, authored state checks, and isolated observations.
Do not remove transaction clones that provide rollback.

**Acceptance**

Run `make test-conformance-scenarios`.
Compare complete results, sample records, final snapshots, and replay hashes before and after the cleanup.
Use a focused allocation measurement if the primary needs a performance claim.

<a id="14-reference-model-f07"></a>

### 14-reference-model-F07: Two result-clone implementations omit the same nested mutable fields

- **Severity:** Medium. Copies described as isolated can share nested slices and time pointers.
- **Classification:** Correctness defect and behavior-preserving consolidation of clone ownership.

**Evidence**

The modelrunner and reference packages each implement a complete `cloneStepResult` switch.
Compare `conformance/modelrunner/runner.go:779-839` with `conformance/reference/resolved.go:73-134`.
Both shallow-copy `Connect.Schema.AffectedScopes` inside the copied connect observation.
Both shallow-copy the `ExpiresAt` pointers inside retention floors.

The nested field definitions are at:

- `conformance/reference/operations.go:58-65,188-205`.
- `conformance/reference/types.go:1156-1162`.

Changing a nested field through one result copy can change its source result.
The existing defensive-copy test mutates only selected HTTP, pull, and seed members.
See `conformance/reference/resolved_operations_test.go:34-56`.
The duplicated implementations therefore retain the same untested omissions.

The dispatch path also clones resolved input immediately before the reference boundary clones it again.
Compare `conformance/modelrunner/runner.go:373-393,756-776` with `conformance/reference/model.go:183` and `conformance/reference/resolved.go:50-70`.

**Smallest coherent simplification**

Give the reference result type one complete clone operation for its existing consumers.
Delete the modelrunner's second result-clone implementation.
Delete its redundant dispatch-time resolved-input clone after confirming the reference boundary retains ownership.
Include nested schema scopes and retention timestamps in the retained copy operation.

This shares representation ownership, not production protocol semantics.

**Invariant**

Caller mutation must not change retained prior results, model state, or later observations.
Keep independent copies where an ownership boundary requires them.

**Acceptance**

Mutate every nested mutable result member through a copy and check the source.
Specifically mutate `Connect.Schema.AffectedScopes[0]` and both retention-floor timestamps.
Run `make test-reference test-conformance-scenarios`.

<a id="14-reference-model-f08"></a>

### 14-reference-model-F08: Runtime interfaces describe alternatives that cannot exist

- **Severity:** Low. Impossible branches and partial interfaces obscure concrete dependencies and add false failure modes.
- **Classification:** Behavior-preserving cleanup.
- **Applicable inventory:** `MOD-041` is an oracle-internal dispatch availability check.

**Evidence**

The runner accepts `*reference.Model`, converts it to `any`, and tests interfaces for methods that this exact type defines.
See `conformance/modelrunner/runner.go:17-23,373-393` and `conformance/reference/model.go:79-115`.
The missing-method error branches cannot occur in a successfully compiled program with this concrete argument type.

`TokenAuthority` has only one implementation.
Its callers repeatedly assert `*tokenAuthority` to obtain behavior absent from the interface.
See `conformance/reference/clock.go:15-26,78-95,142-170` and `conformance/reference/pull.go:1005-1016,1071-1081`.
`Config` does not accept an alternate authority.
See `conformance/reference/model.go:20-26,57-63`.
Repository searches found no alternate implementation or injected consumer.

**Smallest coherent simplification**

Call the concrete model methods directly.
Use the private concrete token-authority type where concrete access is already required.
Delete the two runner interfaces, their availability branches, and `RunErrorApplyResolvedMissing`.
Delete token-authority type assertions and their impossible unsupported-authority branches.

**Invariant**

Keep token binding validation, token rollback, mutex protection, and model cancellation behavior.
Do not weaken token checks to remove the interface.

**Acceptance**

Run `make test-reference test-conformance-scenarios` and `make lint-conformance`.
Search for remaining interface and availability-error references.

<a id="14-reference-model-f09"></a>

### 14-reference-model-F09: Unused helpers and obsolete options remain inside the frozen oracle

- **Severity:** Low. Dead code expands the maintenance and deletion inventory without exercising required behavior.
- **Classification:** Behavior-preserving cleanup.
- **Applicable phase:** R3.0 and the `oracle-internal` inventory category.

**Evidence**

Repository-wide Go searches found definitions but no call sites for these helpers:

| File | Removable definitions |
|---|---|
| `conformance/modelrunner/runner.go` | `RunModel`, lines 46-49. |
| `conformance/modelrunner/seed.go` | `BuildPortableSeedFixtureFromModel`, lines 196-202. |
| `conformance/modelrunner/semantic.go` | `localApplyExecutionSatisfied`, lines 923-949. Its `requestScopeID` helper has no other caller, lines 1721-1727. `scopeChecksum`, lines 2172-2178. `sortedOperationKeys`, lines 2300-2308. |
| `conformance/reference/connect.go` | `affectedAssignedScopes`, lines 381-399. |
| `conformance/reference/normalize.go` | `lessSeedRecordKey`, lines 1685-1693. |
| `conformance/reference/retention.go` | `sameOrAfterTime`, lines 266-268. |
| `conformance/reference/push.go` | `derivePushRowIdentityFromState`, lines 2165-2171. `incompatibleMutationFields`, lines 2537-2546. `dereferenceRowVersion`, lines 3493-3498. |
| `conformance/reference/push_operations_test.go` | `pushOpsSortedTableIDs`, lines 1689-1696. `pushOpsRequireDistinctSnapshotChange`, lines 1724-1729. `pushOpsStringSliceContains`, lines 1777-1784. `pushOpsWithoutField`, lines 1864-1874. `pushOpsTableFieldIDs`, lines 1876-1882. `pushOpsSameWithoutTimes`, lines 1897-1907. `pushOpsCanonicalResponseWithChecksum`, lines 1909-1945. `pushOpsJoinReasons`, lines 1947-1953. |

`RunErrorNegativeControl` also has no consumer outside its declaration at `conformance/modelrunner/types.go:23`.

Several active seams retain no-op behavior:

- `outcomeValues` returns its input unchanged at `conformance/reference/push.go:2096`.
- `reconcileLocalBatchWithResponse` discards the `first` argument at `conformance/reference/push.go:1205`.
- Its wrappers propagate that unused option at `conformance/reference/push.go:1193-1203`.
- `prepareFinalLocalRebuild` returns the same result for both `keepChecksum` values at `conformance/reference/pull_rebuild_operations_test.go:878-905`.
- `staleBindingsDiffer` cannot affect the result because both branches return `TokenStatusStale` at `conformance/reference/clock.go:135-139,196-223`.

Token restoration is another obsolete internal branch.
The only non-test caller passes no reservations.
See `conformance/reference/clock.go:33-75`.
The model explicitly rejects initial tokens and creates a fresh authority.
See `conformance/reference/model.go:20-26,53-63,292-348`.
The restoration tests only exercise the private restoration facility.
See `conformance/reference/clock_test.go:295-374`.
Its inventory rows `REF-401` and `REF-402` are classified `oracle-internal`.

**Smallest coherent simplification**

Delete uncalled helpers, unused options, and the identity wrapper.
Construct a fresh token authority directly instead of passing through unused reservation restoration.
Retain the exhaustion check, with a focused private-state test if needed.
Delete restoration-only tests when the restoration branch disappears.
Replace stale-binding classification with the existing final stale result after request-binding checks.

Repository searches establish repository-local nonuse only.
The primary should check any separately maintained consumer before removing exported convenience functions.

**Invariant**

Do not delete semantic tests merely because their private setup operation is no longer publicly registered.
Preserve token classification, deterministic labels, clone isolation, and meaningful model checks.

**Acceptance**

Repeat the exact-symbol searches before and after deletion.
Run `make test-reference test-conformance-scenarios test-conformance-drivers`.
Update applicable oracle-internal inventory dispositions without marking unrelated semantic rows covered.

<a id="14-reference-model-f10"></a>

### 14-reference-model-F10: WAL negative controls test a private checker that never judges model executions

- **Severity:** Medium. The named negative-control test can pass while a defect remains in the model's WAL execution path.
- **Classification:** Correctness defect in proof, with removable duplicate checking logic.
- **Applicable phase:** R3 requires demonstrated negative controls for migrated invariants.

**Evidence**

`TestWALNegativeControlsDetectRequiredMutants` creates a hand-written snapshot and mutates that snapshot.
It checks the mutations with private test-only validators.
See `conformance/reference/wal_operations_test.go:549-615`.

`walOpsWALOracleViolations` and `walOpsMembershipIsolationViolations` have no consumers outside that test.
Their implementations are at `conformance/reference/wal_operations_test.go:1098-1169`.
The first also duplicates registry selection through `walOpsIndependentRegistrySelection` at lines 1136-1153.
Neither validator judges the snapshots from the behavioral WAL tests at lines 49-488.

Thus the controls prove that these private helpers reject their constructed inputs.
They do not prove that the actual WAL tests detect the named implementation mutants.

**Smallest coherent simplification**

Attach each required mutant to the checker or behavioral test that actually supplies the proof.
Use the existing invariant-engine proof home during R3 migration where applicable.
Delete the disconnected private validators and their duplicate registry-selection implementation once that attachment exists.
Do not keep a second oracle solely to demonstrate that the second oracle can fail.

**Invariant**

The same executable proof must accept the valid execution and reject the relevant defect.
Preserve order, poison blocking, duplicate-effect detection, generation selection, and scope isolation.

**Acceptance**

Demonstrate a mutant in each named behavior and require its actual proof-home test to fail.
Run `make test-reference` for current-model controls and `make test-invariants` for migrated controls.
A green fixture-only checker is not sufficient acceptance evidence.

<a id="14-reference-model-f11"></a>

### 14-reference-model-F11: Scalar canonicalization falls back after every strict-validation failure

- **Severity:** Medium. The helper can turn invalid scalar input into valid canonical bytes instead of rejecting it.
- **Classification:** Correctness defect with an unnecessary alternate decoding path.

**Evidence**

`canonicalJSONValue` catches any `jsonstrict.ValidateValue` failure and retries scalar input through `json.Decoder`.
It decodes one value, marshals that value, and canonicalizes the replacement bytes.
It does not check trailing input or retain raw Unicode validation.
See `conformance/reference/push.go:3427-3447`.

The strict helper requires a top-level object, but also rejects invalid UTF-8, trailing values, and lone surrogates.
See `conformance/internal/jsonstrict/decode.go:12-42,127-159`.
The fallback does not distinguish the expected object-shape rejection from those other failures.

Static examples follow directly from the branch:

- `1 2` decodes as the first number and discards the remaining value.
- A lone surrogate string passes through Go's replacement decoding before JCS sees it.

Current enclosing parsers reject many malformed public inputs first.
This limits the demonstrated impact to the helper and directly supplied model values.
This review does not claim a production request bypass.

**Smallest coherent simplification**

Validate the raw scalar value completely, then canonicalize those raw bytes.
Delete the decode-and-remarshal fallback that repairs invalid input.
Retain duplicate-member and Unicode validation for object values.

**Invariant**

Canonicalization must preserve a valid value's identity and reject malformed input.
It must not silently remove trailing values or replace invalid Unicode.

**Acceptance**

Add focused cases for trailing scalar input and lone surrogates, plus valid scalar controls.
Run `make test-reference`.
This review traced the code but did not execute these cases.

### Contract requirements that merit a product decision

#### Repeated deterministic model samples

The model requires three repetitions for each closed performance stratum.
Configured limits require 63 sample records and exactly three records for every family/boundary pair.
See `conformance/modelrunner/workload_configured_limits.go:109-140,551-577` and `conformance/modelrunner/semantic.go:1393-1451`.
Schema dispatch also requires distinct clients and a minimum count for each stratum.
See `conformance/modelrunner/schema_dispatch.go:119-142`.

These requirements create workload builders, snapshot storage, replay work, and tests that protect fixed sample counts.
The interpreter supplies no independent performance measurement for those repetitions.

A coherent alternative is one semantic example per boundary with a meaningful negative control.
Real performance measurement can retain repeated samples when an enforced performance claim requires them.
`RELEASE.md:157` says numeric performance guarantees remain deferred.

The primary must decide how this affects legacy scenario and proof contracts.
Do not silently reduce current sample requirements as a cleanup.
Do not interpret deferred performance work as permission to remove semantic limit checks.

#### R3 inventory should preserve behavior, not oracle representation

The replacement should preserve row conservation, replay, progress, provenance, and integrity.
It should not preserve obsolete restoration machinery or current server-state shortcuts merely because the inventory names them.
Decision 6 already distinguishes deterministic validation from randomized invariant work.
See `docs/superpowers/plans/2026-09-07-r3-2-invariant-engine-design.md:72-78`.

Use the existing inventory for justified closures and corrected proof homes.
This report does not approve a new protocol rule or weaken an existing invariant.


<a id="area-15-scenario-corpus"></a>

## Authored scenarios and vectors

<a id="15-scenario-corpus-f01"></a>

### 15-scenario-corpus-F01: Delete unused vector source retention

**Severity:** Low. Each catalog retains an unused full source copy and copies it again for each successful `Set` call.

**Classification:** Behavior-preserving cleanup.

**References:**

- `conformance/vectors/types.go:155-163` declares private `VectorSet.sourceBytes`.
- `conformance/vectors/load.go:221-225` copies the source into that field.
- `conformance/vectors/types.go:186-202` copies the field again before returning a vector set.
- `conformance/vectors/load.go:104-143` retains the separate capture used for source-integrity checks.

**Evidence and cost:** An exact package search found only the field declaration, initialization, and defensive copy.
No code reads the retained bytes for validation, hashing, or output.
The scope manifest records 3,889,774 source bytes for `canonical-v1.json`.
Thus this field retains that many unnecessary source bytes and copies them again on each successful `Set` call.
This is a code-derived copy size, not a measured runtime result.

**Smallest simplification:** Delete the private field and both copies.
Keep the local capture map and its source comparison.
Keep defensive copies of vector inputs and expected values.

**Preserved invariant:** Consumers cannot change catalog inputs or authored expected values through returned aliases.
Catalog loading must still reject changed source bytes.

**Acceptance:** Run `make test-vectors` after deletion.
Retain `TestLoadValidCatalogAndDefensiveCopies` and `TestLoaderTOCTOUBoundaries`.
An exact package search must find no remaining `sourceBytes` reference.

**Applicable identifiers:** `VSET-CANONICAL-001`. No existing issue match was established.

<a id="15-scenario-corpus-f02"></a>

### 15-scenario-corpus-F02: Remove duplicate-ID maps where strict ordering already proves uniqueness

**Severity:** Low. The loader maintains redundant maps and rejection paths for one invariant.

**Classification:** Behavior-preserving cleanup.

**References:**

- `conformance/vectors/load.go:147-172` checks strict catalog ID ordering and separately maintains `seenIDs`.
- `conformance/vectors/load.go:198-213` checks strict vector ID ordering and separately maintains another `seenIDs`.
- `conformance/scenarios/catalog.go:493-519` already checks sorted scenario IDs without a separate ID map.
- `conformance/vectors/load_test.go:82-105` and `108-140` cover duplicate and unordered input rejection.

**Evidence and cost:** A strictly increasing sequence cannot contain duplicate IDs.
For catalog entries, the preceding ordering check rejects duplicates before the map check can detect them.
For vector entries, the ordering check rejects the same duplicate input if the map check is removed.
The tests require rejection, not a distinct duplicate-ID error string.

**Smallest simplification:** Remove both ID maps, their assignments, and their duplicate-ID branches.
Retain strict ordering checks and the separate path-uniqueness map.
Path uniqueness does not follow from ID ordering.

**Preserved invariant:** Duplicate or unordered vector IDs and duplicate catalog paths remain invalid.

**Acceptance:** Run `make test-vectors`.
Both duplicate-ID and unordered-ID negative controls must still reject their inputs.

**Applicable identifiers:** `VSET-CANONICAL-001`. No existing issue match was established.

<a id="15-scenario-corpus-f03"></a>

### 15-scenario-corpus-F03: Stop native workload validation after an invalid record bound

**Severity:** Medium. A malformed in-memory scenario can keep the validator in a loop controlled by an invalid record count.

**Classification:** Correctness defect.

**References:**

- `conformance/scenarios/native_binding.go:191-204` records an invalid `RecordCount` but continues.
- `conformance/scenarios/native_binding.go:219-236` then loops from zero to `RecordCount` when targets exist.
- `conformance/scenarios/validate.go:152-166` accepts an in-memory scenario without repeating schema validation.
- `conformance/scenarios/validate.go:372-383` invokes native validation.
- `conformance/scenarios/validate_test.go:162-199` tests this direct entry point with a count of only 1001.
- `conformance/scenarios/validate_test.go:202-247` separately checks the schema loader boundary.

**Evidence and cost:** The limit is 1000, but the iteration condition does not use that limit.
For example, a direct call with `RecordCount = 9007199254740991` and one target reaches that many cardinality iterations.
The accumulated validation error cannot return until the loop finishes.
The loop has no cancellation check.
The schema loader rejects oversized serialized counts, so this finding does not claim a normal file-loading or remote attack path.

**Smallest simplification:** Return from workload validation immediately after rejecting an invalid record count.
Do not generate cardinalities or compute batch counts from that invalid input.
Keep all validations for valid bounded workloads.

**Preserved invariant:** Invalid workloads fail before count-dependent work.
Valid workloads still require exact operation, batch, and per-scope cardinality closure.

**Acceptance:** Extend the existing bound test with a very large count and zero.
Use a bounded test process to detect a validator hang.
Run `make test-conformance-scenarios` after the fix.
No hang reproduction was executed during this review.

**Applicable identifiers:** Native workload bound enforced by `maxNativeWorkloadRecords`.
No existing issue match was established.

**Additional evidence from packets 36-55:** `conformance/scenarios/types.go:493-501` represents `RecordCount` as `uint64`.
The native workload fixture at `conformance/scenarios/validate_test.go:942-979` supplies a target that reaches the count-dependent loop.
These sources confirm the direct in-memory boundary described above.

<a id="15-scenario-corpus-f04"></a>

### 15-scenario-corpus-F04: Remove obsolete endpoint classes from native CRUD acceptance

**Severity:** Medium. The checker accepts transport evidence containing endpoints outside the four-endpoint runtime contract.

**Classification:** Correctness defect.

**References:**

- `conformance/scenarios/native_crud.go:756-778` permits `checkpoint` and `schemas` alongside the four runtime operations.
- `conformance/scenarios/native_crud_test.go:104-108` tests an unknown upload operation, but not those two obsolete classes.
- `docs/src/content/docs/spec/01-wire-protocol.mdx:24-31` defines exactly four runtime endpoints.
- `conformance/swift/queue_replay.go:664-680` copies observed operation classes into this evidence without remapping them.
- `conformance/scenarios/performance/core-sync-path-001.json:143-155` independently authors the same four-operation set.

**Evidence and cost:** An additional `checkpoint` or `schemas` observation passes the operation-class switch.
It then bypasses the push-specific checks because it is not a push.
One otherwise valid push still satisfies the final push-count check.
Therefore these extra observations cannot cause this validator to reject otherwise valid evidence.
This is a static control-flow proof, not an observed production request.

**Smallest simplification:** Delete `checkpoint` and `schemas` from this allowed set.
Do not add a compatibility path for these classes.

**Preserved invariant:** Native CRUD uses only connect, push, pull, and rebuild.
It still requires exactly one successful canonical push with the expected mutation count.

**Acceptance:** Extend the existing endpoint negative control with an added `checkpoint` observation and an added `schemas` observation.
Both must fail while the valid four-operation evidence passes.
Run `make test-conformance-scenarios`.

**Applicable identifiers:** Four runtime endpoints in the wire contract and `SYNC-BOUNDARY-001`.
No existing issue match was established.

**Additional evidence from packets 36-55:** `conformance/scenarios/validate.go:26-44` limits canonical wire cases to connect, push, pull, and rebuild.
This provides a second in-scope comparison against the obsolete CRUD transport classes.

<a id="15-scenario-corpus-f05"></a>

### 15-scenario-corpus-F05: Store each authored measurement parameter object once

**Severity:** Low. Authored samples repeat one parameter object and require a validator branch solely to prevent drift between the copies.

**Classification:** Contract decision. The change affects the authored scenario format and its consumers.

**References:**

- `conformance/scenarios/performance/configured-bounds-001.json:479-497` stores the same object in `parameters` and `operation.value`.
- `conformance/scenarios/performance/configured-bounds-001.json:476-1926` repeats this shape for all 63 samples.
- `conformance/scenarios/performance/fanout-001.json:486-503` repeats the same authored duplication in an inline sample.
- `conformance/scenarios/measurement_bindings.go:117-130` requires parameters to match the stratum and operation value to match parameters.
- `conformance/scenarios/measurement_bindings.go:198-210` reads configured-bound fields from the duplicate parameter object.
- `conformance/scenarios/measurement_bindings.go:415-448` and `475-487` separately validate observed runtime operations.
- `conformance/scenarios/measurement_bindings_test.go:193-195` already checks an observed operation-value mismatch.

**Evidence and cost:** Both authored objects must have the same meaning because the validator rejects every difference.
They are not independent expected and observed evidence.
The runtime observation is a separate object and must remain separate.

**Smallest simplification:** Subject to format approval, retain authored `operation.value` and remove authored `parameters`.
Compare `operation.value` directly with the selected stratum parameters.
Read configured-bound fields from that same authored value.
Remove the copy-equality branch and its copy-only negative control.

**Preserved invariant:** Keep every sample ID, operation ID, stratum, boundary, and required observation.
Keep the minimum sample count and all runtime mismatch controls.
Do not derive authored parameters or expected results from production observations.

**Acceptance:** After an approved format migration, run `make test-conformance-scenarios` and `make check-conformance-catalog`.
The configured-bounds scenario must still define 63 distinct samples.
Wrong runtime values, missing samples, duplicate observations, and incorrect strata must still fail.
The primary must select migration compatibility and versioning before implementation.

**Applicable identifiers:** `SCN-PERF-CONFIGURED-BOUNDS-001`, `MEAS-CONFIGURED-BOUNDS-001`, and `SYNC-LIMIT-001`.
No existing issue match was established.

**Additional evidence from packets 16-35:** The same duplication occurs in six more fully reviewed scenario files.
The following ranges show the first complete sample in each file.

| Path | Duplicated sample range | Authored sample count |
| --- | --- | --- |
| `conformance/scenarios/performance/queue-replay-001.json` | 1080-1099 | 9 |
| `conformance/scenarios/performance/rebuild-apply-001.json` | 971-988 | 9 |
| `conformance/scenarios/performance/rebuild-cardinality-001.json` | 1009-1026 | 9 |
| `conformance/scenarios/performance/schema-check-001.json` | 1498-1513 | 18 |
| `conformance/scenarios/performance/seeded-empty-startup-001.json` | 1067-1082 | 6 |
| `conformance/scenarios/performance/shared-private-scopes-001.json` | 475-490 | 6 |

All 57 samples repeat their complete parameter object in `operation.value`.
This extends F05 evidence. It does not create a separate finding or authorize removal of repeated executions.
Preserve all 57 distinct sample IDs and operation IDs if the format change is approved.
The corresponding measurement IDs are `MEAS-QUEUE-REPLAY-001`, `MEAS-REBUILD-APPLY-001`, `MEAS-REBUILD-CARDINALITY-001`, `MEAS-SCHEMA-CHECK-001`, `MEAS-SEEDED-EMPTY-STARTUP-001`, and `MEAS-SHARED-PRIVATE-SCOPES-001`.

**Additional evidence from packets 36-55:** `conformance/scenarios/types.go:376-393` stores both authored parameter objects.
`conformance/scenarios/types.go:408-417` separately represents runtime observations, which must remain independent.
`conformance/scenarios/validate_test.go:696-707` explicitly copies parameters into `Operation.Value` to construct a valid sample.
The duplication therefore affects typed data and test construction, not only JSON formatting.

<a id="15-scenario-corpus-f06"></a>

### 15-scenario-corpus-F06: Preserve explicit-empty state projections through JSON serialization

**Severity:** Medium. Serialization can change an empty-state requirement into an omitted check and permit a false positive.

**Classification:** Correctness defect.

**References:**

- `conformance/scenarios/types.go:192-212` uses `omitempty` for optional state-fact slices, including `Rows`.
- `conformance/scenarios/types.go:293-307` does the same for optional client provenance, checkpoint, queue, and outcome slices.
- `conformance/scenarios/state_facts.go:231-270` compares only selected fact families.
- `conformance/scenarios/state_facts.go:319-327` treats a nil expected list as unrestricted and an empty expected list as requiring emptiness.
- `conformance/scenarios/state_facts_test.go:49-57` explicitly protects that distinction.
- `conformance/scenarios/validate_test.go:1029-1041` clones scenarios through JSON encoding and decoding.
- The previously reviewed `conformance/scenarios/load.go:44-57` uses the same round trip in public `Clone`.

**Evidence and cost:** The JSON `omitempty` rule omits both nil slices and non-nil slices of length zero.
For example, `StateFacts{Rows: []RowFact{}}` encodes as `{}` and decodes with `Rows == nil`.
Before that round trip, `StateFactsProjectionEqual` rejects an observation containing a row.
After that round trip, the same comparison accepts that observation because the row family is no longer selected.
The test clone and public clone can therefore weaken an authored expectation while otherwise returning successfully.
This is a static language-and-control-flow proof. No executable reproduction ran.
This finding does not claim that an inspected release run exercised this counterexample.

**Smallest simplification:** Replace `omitempty` with the supported `omitzero` option for these optional fact slices.
Do not combine the options, because `omitempty` would still omit explicitly empty slices.
Keep nil families omitted and encode explicitly empty families as `[]`.
Apply the same rule to the top-level and client fact families.
Do not add a clone-only workaround that leaves normal JSON serialization lossy.

**Preserved invariant:** An omitted family makes no assertion.
An explicitly empty family requires an empty observation before and after serialization or cloning.
Cloning must still isolate mutable state from its source.

**Acceptance:** Extend the existing omitted-versus-empty test through JSON round trip and public `Clone`.
Use a nonempty observed row as the negative control before and after each round trip.
Cover an explicitly empty client queue through the same boundary.
Run `make test-conformance-scenarios` after the correction.

**Applicable identifiers:** Authored partial-projection semantics in `StateFacts` and `TestStateFactsProjectionEqualDistinguishesOmittedAndEmptyLists`.
No existing issue match was established.

<a id="15-scenario-corpus-f07"></a>

### 15-scenario-corpus-F07: Remove unused catalogs from the performance-item helper

**Severity:** Low. Two unused parameters make the helper appear dependent on complete catalogs that it never reads.

**Classification:** Behavior-preserving cleanup.

**References:**

- `conformance/scenarios/validate.go:1231-1237` passes both `budgets` and `measurements` to each performance-item validation call.
- `conformance/scenarios/validate.go:1260-1290` declares those parameters but never uses either one.
- `conformance/scenarios/validate.go:1239-1255` uses the maps elsewhere, so their enclosing construction must remain.

**Evidence and cost:** The complete helper body uses the item IDs, support cells, artifacts, scenario obligations, and assertion map.
It performs no lookup in either catalog parameter.
Both callers pass unnecessary dependencies without affecting any branch or result.

**Smallest simplification:** Delete the two unused parameters and their arguments at both calls.
Keep the maps in `validatePerformance`, where they have real consumers.
Do not replace the removed arguments with a context object or interface.

**Preserved invariant:** Every performance item still requires one owner per support cell, exact artifact ownership, and a performance assertion.

**Acceptance:** Run `make test-conformance-scenarios` after deletion.
Retain `TestValidatePerformanceClosureAndValidateAll` at `conformance/scenarios/validate_test.go:658-758`.
This review established dead arguments statically and did not execute the test.

**Applicable identifiers:** Scenario performance ownership validation. No existing issue match was established.


<a id="area-16-verification-framework"></a>

## Verification framework and gates

<a id="16-verification-framework-f01"></a>

### 16-verification-framework-F01: Soak capture rules require reconstructed HTTP exchanges

**Severity: High.** The framework can judge an exchange that never occurred and bind its identity from the requested operation.

**Classification:** Correctness defect.

**References**

- `conformance/soak/capture.go:287-320` requires authenticated identity inside `RequestBody`.
- `conformance/soak/capture.go:375-423` requires an acknowledgment to reference the issuing pull result's exchange sequence.
- `conformance/invariants/cursor_monotonicity.go:141-185` instead reads that sequence as the acknowledging request.
- `conformance/invariants/observation.go:123-160` describes issuing results, acknowledging requests, and raw exchanges.
- `conformance/blackbox/integration/soak_harness_test.go:989-1045` combines the acknowledgment request with the earlier change response.
- `conformance/blackbox/integration/soak_harness_test.go:405-442` assigns the combined sequence to both facts.
- `conformance/blackbox/integration/soak_harness_test.go:1419-1436,1454-1468` rewrites request bytes and assigns operation metadata.
- `docs/src/content/docs/spec/01-wire-protocol.mdx:33-43` makes authenticated identity transport context, not a request member.

**Evidence and cost**

The framework conflates two distinct events: cursor issuance and later client acknowledgment.
The live consumer adapts to this requirement with `wireFromCalls(operation, ackCall, mainCall, ...)`.
Its output pairs bytes from different requests and responses.
`bindWireTarget` also inserts `authenticated_user_id` from the planned operation and can insert missing client and scope identities.
These assignments turn requested identity into observed identity.
The journal then hashes reconstructed bytes, not the original exchange bytes.
The synthetic fixture repeats the same arrangement at `conformance/soak/soak_test.go:692-707`.

This is not a naming preference. The two APIs require incompatible meanings for one sequence field.

**Smallest coherent simplification**

Keep each recorded request and response together without rewriting either body.
Store authenticated transport identity beside the body, using identity captured from the executed request context.
Let acknowledgment facts reference the later request sequence and the previously issued cursor identity separately.
Delete request-body identity insertion, cross-exchange pairing, and the same-sequence requirement.

**Invariant to retain**

Preserve exact cursor bytes, authenticated user binding, issuance position, later presentation, and durable checkpoint acknowledgment.

**Acceptance**

Add a two-exchange fixture with different request cursors and different response bodies.
Require rejection after substituting either exchange, authenticated identity, or acknowledgment sequence.
Require journal body hashes to equal the recorder's original body hashes.
Run `make test-invariants`, `make test-conformance-invariants`, and `make soak SOAK_SEED=42 SOAK_DURATION=35s`.
Static inspection establishes the mismatch. No live soak ran here.

**Applicable contract:** `SYNC-CURSOR-004`, `SYNC-CURSOR-005`, `SYNC-SCOPE-001`.

<a id="16-verification-framework-f02"></a>

### 16-verification-framework-F02: Random fault labels do not identify the fault that executes

**Severity: High.** A run can record successful activation of a requirement-owned control without executing that control's defect.

**Classification:** Correctness defect.

**References**

- `conformance/soak/generator.go:144-196,355-383,424-445` selects controls by mechanism and copies their complete descriptive recipe.
- `conformance/soak/capture.go:211-221` checks activation flags and control identity, but not the selected target or defect.
- `conformance/blackbox/integration/soak_harness_test.go:1320-1378` executes transport faults on connect and uses an operator fallback.
- `conformance/faults/catalog.json:193` defines `CTRL-INTEGRITY-004` as row-digest corruption.
- `conformance/faults/catalog.json:188,203,222,227` defines rebuild, seed, reset, and client migration controls with distinct preconditions.

**Evidence and cost**

The generator treats every `wire-fault` control as suitable for connect, push, pull, or a generic wire operation.
The consumer always sends a connect request for the wire fault.
Its `corrupt`, `misbind`, `omit`, and other unmatched operators all become a temporary-unavailable response.
Thus a selected row-digest corruption control can become an unrelated connect 503 while retaining the original control ID.
Process-fault controls similarly receive activation and cleanup flags before the consumer's generic process-death operation.
Exact catalog prose matching does not repair this missing execution relationship.

**Smallest coherent simplification**

Generate only explicitly implemented operation-and-fault combinations.
Use one closed executable fault definition for each supported combination.
Reject an unsupported recipe instead of translating it through a default operator branch.
Keep requirement-owned source mutants separate from generic transport disruption recipes.
Delete copied recipes that the runtime cannot execute and the fallback that changes their meaning.

**Invariant to retain**

A recorded control must execute its selected defect at the required boundary and retain its real detection assertion.

**Acceptance**

Add negative controls for a digest-corruption recipe sent to a connect-only injector and an unsupported process target.
Both must fail before an activation result is recorded.
Each accepted recipe must expose evidence of its actual operation and fault boundary.
Run `make test-conformance-faults`, `make test-conformance-invariants`, and a fixed-seed `make soak` run.
The listed mismatch follows directly from the selection and dispatch branches. No fault execution ran here.

**Applicable contract:** Requirement-owned controls in `conformance/README.md:68-74`, including `SYNC-INTEGRITY-004` and `SYNC-SCHEMA-006`.

<a id="16-verification-framework-f03"></a>

### 16-verification-framework-F03: Issue 49 tests maintain a test-only protocol implementation

**Severity: Medium.** These tests add a second maintenance surface without exercising production or the runtime invariant checkers.

**Classification:** Behavior-preserving cleanup.

**References**

- `conformance/invariants/issue49_scenarios_test.go:14-105` checks a hand-maintained requirement list and accepts two precomputed booleans.
- `conformance/invariants/issue49_protocol_test.go:5-55` builds observed transitions from a local adjacency table.
- `conformance/invariants/issue49_predicates_test.go:368-432` checks them against another local adjacency table.
- `conformance/invariants/issue49_integrity_test.go:14-30` assigns one constant to all four implementation results.
- `conformance/invariants/issue49_integrity_predicates_test.go:62-91,93-405` checks those assignments and implements another canonical encoder and digest path.
- `conformance/invariants/issue49_cursor_rebuild_test.go:143-192` signs tokens with the same test-only MAC implementation that verifies them.
- `conformance/invariants/issue49_cursor_predicates_test.go:417-503` owns that separate token implementation.
- `conformance/invariants/issue49_schema_queue_test.go:41-83` represents crash recovery with authored boolean fields rather than a crash or runtime capture.

**Evidence and cost**

The relevant predicates and encoders exist only in `_test.go` files.
Exact symbol searches found no runtime consumer of these implementations.
The platform result map does not call PostgreSQL, Swift, Kotlin, or React Native.
The state-machine test compares two handwritten copies of the same transition table.
`issue49Proof` checks catalog presence, a true positive boolean, and a false mutant boolean.
It does not run the catalog recipe or bind a real observed result.
The completeness test can pass from its 40-entry list without locating an executable proof for each entry.

Useful authored values do not justify the separate test-only protocol runtime.

**Smallest coherent simplification**

Remove the Issue 49 shadow predicates, duplicate encoders, fabricated platform result maps, and catalog-presence proof wrapper.
First compare their authored cases with the shared vectors and existing real scenarios.
Move only unique required inputs and independently authored expected values to those existing proof homes.
Test the actual shared invariant predicates with positive captures and meaningful mutants.
Do not replace expected values with production output.

**Invariant to retain**

Retain independent expected values and real mutation sensitivity for every required behavior.
Do not treat a test-only encoder as evidence that a client implementation conforms.

**Acceptance**

Require an explicit mapping from each retained case to its executable proof home.
Run `make test-vectors`, `make test-invariants`, and the affected focused server or native scenario targets.
Use the corresponding production mutants when the retained proof is release-critical.
Static inspection proves the isolated ownership. It does not prove that every case already has another home.

**Existing issue:** Related to the supplied `#36` oracle-replacement lead. The primary must confirm the issue's exact scope.
**Applicable identifiers:** The requirements listed at `issue49_scenarios_test.go:14-55`, especially `SYNC-STATE-001`, `SYNC-INTEGRITY-003`, and `SYNC-SCHEMA-006`.

<a id="16-verification-framework-f04"></a>

### 16-verification-framework-F04: Exact result parsing accepts skipped assertion descendants

**Severity: High.** A required mutation-control gate can pass after a selected assertion skips a child test.

**Classification:** Correctness defect.

**References**

- `conformance/cmd/testresult/parser.go:299-345` stores descendant `skip` events but propagates only descendant failure.
- `conformance/cmd/testresult/parser.go:352-393` checks skips only on the target and selected assertion.
- `conformance/cmd/testresult/suite.go:151-158,228-250` correctly rejects skipped tests anywhere in the suite map.
- `conformance/cmd/testresult/parser_test.go:113-185` covers passing, failed, and unfinished descendants, but not skipped descendants.
- `Makefile:529-532` uses the exact parser for mutation-control execution.

**Static reproduction**

For one package, submit these valid event relationships:

1. Start the package.
2. Run `TestControl`.
3. Run `TestControl/assertion`.
4. Run and skip `TestControl/assertion/required-case`.
5. Pass `TestControl/assertion`.
6. Pass `TestControl`.
7. Pass the package.

The descendant state becomes `skip`.
No active descendant remains, and `descendantFail` remains false.
`result()` therefore returns `target_pass`.
A failed sibling can likewise produce `target_semantic_test_failure` despite the skipped descendant.

**Smallest coherent simplification**

Reject any skipped descendant before classifying pass or semantic failure.
Use the existing descendant state map rather than adding another parser layer.
Keep exact assertion selection and setup-failure separation.

**Invariant to retain**

Required gates reject every skip and unfinished test. A setup failure must never count as a killed mutant.

**Acceptance**

Add both event streams described above as negative controls.
Run `make test-conformance-testresult`.
This report derives the result from the state machine. It does not claim an executed parser reproduction.

**Applicable contract:** Zero-skip gate policy and `conformance/README.md:84-86,145`.

<a id="16-verification-framework-f05"></a>

### 16-verification-framework-F05: Mutation controls have two registries and positional assertion identities

**Severity: Medium.** Harmless test insertion can change a mutant's selected assertion without changing its recorded requirement.

**Classification:** Behavior-preserving cleanup.

**References**

- `conformance/mutants/integration_gate.sh:244-285,288-320` separates manifest mutants from seven handwritten controls.
- `conformance/mutants/integration/manifest.json:4-72` records positional names such as `assertion#03` and `assertion#30`.
- `conformance/mutants/manifest_test.go:164-201,257-348` recognizes only Issue 49 patches and infers targets from Makefile text.
- `conformance/mutants/manifest_test.go:271-328` counts any literal `.Run("assertion", ...)` call in each function body.
- `conformance/cmd/testresult/parser.go:71-98` restricts the selector to generated assertion names.
- `Makefile:520-528` repeats the allowed test list and assertion-name pattern.

**Evidence and cost**

The AST scan checks a count, not the selected assertion's requirement identity.
It does not resolve the receiver as `testing.T` or distinguish nested calls from sibling calls.
The Makefile scan accepts a test name anywhere in Makefile text, not only the executable target allowlist.
The seven critical patches bypass the manifest's metadata binding checks.
The runtime exact parser remains the meaningful execution check, but the static registries add maintenance work around it.

**Smallest coherent simplification**

Use one manifest for all controls, with an explicit critical or broad selection field.
Give semantic subtests stable descriptive names or stable assertion IDs instead of automatic numeric suffixes.
Derive target selection from that manifest.
Delete the separate shell list, repeated Makefile test-name list, and AST assertion-count approximation.
Retain runtime proof that the exact selected assertion ran and produced the required result.

**Invariant to retain**

Every mutant must apply, compile, pass its baseline, fail its intended semantic assertion, and pass the post-baseline.

**Acceptance**

Insert or reorder an unrelated subtest and require the selected assertion identity to remain unchanged.
Remove the selected assertion and require a missing-test failure.
Run `make test-conformance-testresult`, `make test-integration-mutant-manifest`, and `make test-integration-mutants`.
No mutation gate ran during this review.

**Applicable contract:** Server mutation gate and requirement-owned assertion binding.

<a id="16-verification-framework-f06"></a>

### 16-verification-framework-F06: Separate JSON validation paths have conflicting guarantees

**Severity: Medium.** The framework accepts malformed values in some paths while maintaining duplicate strict decoders in others.

**Classification:** Correctness defect.

**References**

- `conformance/faults/load.go:235-482,640-789` implements manual object decoding and a separate strict JSON scanner.
- `conformance/faults/types.go:13-52,118-154` duplicates catalog types and schema enumerations.
- `conformance/internal/contract/types.go:84-119` defines the same fault catalog structure.
- `conformance/internal/contract/load.go:94-132` already validates captured catalog bytes with schemas and `jsonstrict`.
- `conformance/internal/jsonstrict/decode.go:12-190` owns duplicate-member and Unicode-scalar rejection.
- `conformance/internal/schemavalidator/validator.go:671-758` has another decoder without lone-surrogate rejection.
- `conformance/invariants/wire.go:13-54,86-101` decodes objects and integers without the same strictness.
- `conformance/invariants/no_state_forks.go:215-229` normalizes JSON through a last-member-wins map.
- `conformance/invariants/pull_wire.go:69-72` decodes nullable JSON into a Go boolean.
- `conformance/schemas/fault-catalog-v1.schema.json:30-57` declares the catalog field rules.

**Evidence and cost**

The fault loader repeats the shared duplicate-member and Unicode scanner almost line for line.
It also repeats the field lists, enumerations, and structure already owned by the catalog schema.
Its optional `precondition` decoder accepts an empty string, while the schema requires a nonempty value when present.
The invariant object decoder silently overwrites duplicate members.
`has_more: null` decodes successfully into `false`, so the pull checker can treat it as a terminal boolean.
The integer decoder accepts JSON strings through `json.Number` and has no portable safe-integer bound.
These are observable validation differences, not formatting choices.

**Smallest coherent simplification**

Use one strict JSON syntax policy and the existing catalog schema for the two catalog consumers.
Delete the duplicated fault catalog decoder, Unicode scanner, and duplicate catalog type family.
Route invariant raw objects through strict duplicate-member validation before extracting fields.
Validate booleans and envelope integers as their exact JSON types.
Preserve the distinction between object-only catalog documents and scalar schema instances.
Do not replace scalar schema decoding with an object-only API without preserving that behavior.

**Invariant to retain**

Preserve exact numeric values, Unicode scalars, duplicate-member rejection, required fields, and unknown-field rejection where the schema requires it.

**Acceptance**

Check duplicate and escaped-duplicate members, lone surrogates, `has_more: null`, quoted counters, and unsafe integer values.
Check optional empty `precondition` consistently through both catalog entry points.
Run `make test-conformance-contract`, `make test-conformance-faults`, and `make test-invariants`.
The acceptance differences above follow from static decoding paths. They were not executed here.

**Applicable contract:** `SYNC-INTEGRITY-003`, `SYNC-PROTOCOL-004`, exact request and response types.

<a id="16-verification-framework-f07"></a>

### 16-verification-framework-F07: Performance freezing reimplements JavaScript object-order behavior

**Severity: Medium.** A property reorder requires digest maintenance despite preserving the performance requirement's meaning.

**Classification:** Contract decision.

**References**

- `conformance/internal/contract/policy.go:28,762-782,809-1143` implements two digest paths and a JavaScript JSON value runtime.
- `conformance/internal/contract/load.go:133-137` stores a typed fingerprint to select the raw or typed digest path.
- `conformance/internal/contract/contract_test.go:305-370` explicitly accepts numeric normalization but rejects raw property-order changes.
- `conformance/internal/contract/snapshot.go:308-331` already uses RFC 8785 canonicalization for snapshot hashing.
- `conformance/performance/budgets.json:6-42` contains an ordinary metric definition affected by the ordering rule.

**Evidence and cost**

The custom runtime implements token parsing, numeric conversion, JavaScript array-index ordering, string escaping, and number formatting.
The test at lines 352-370 swaps `id` and `scenario_id` without changing either value and expects rejection.
The raw-versus-typed fingerprint branch exists to preserve this distinction after decoding.
This is not an independent semantic expectation for measured request counts.
It is a second serialization contract around the authored performance definitions.

**Smallest coherent simplification**

Choose one freeze meaning: exact source bytes or canonical semantic JSON.
Use the existing raw file binding for exact bytes, or existing RFC 8785 support for semantic JSON.
Delete the custom JavaScript value runtime and raw-versus-typed fingerprint branch.
Keep the authored metric, comparator, limit, strata, and sample requirements unchanged.

**Invariant to retain**

A Candidate must bind the approved performance definition. Measurement output must never define its expected limit.

**Acceptance**

If semantic canonicalization is approved, equivalent property order must produce the same digest.
Changing a limit, metric, stratum, or required sample count must change the binding.
Run `make test-conformance-contract` and the affected performance proof target.
Existing Candidate digest compatibility requires an explicit primary decision.

**Applicable phase:** Candidate contract freeze. No digest migration is approved by this report.

<a id="16-verification-framework-f08"></a>

### 16-verification-framework-F08: Validators repeat release catalogs and dependency versions as source constants

**Severity: Medium.** Routine approved catalog or dependency changes require synchronized edits to multiple authorities and their tests.

**Classification:** Contract decision.

**References**

- `conformance/internal/contract/policy.go:31-118,183-188,311-326,556-767` pins support tuples, artifact roles, budgets, measurements, and counts.
- `conformance/support-matrix.json:5-159` owns the authored support policy.
- `conformance/schemas/support-matrix.schema.json:25-47` repeats the current-track policy and exact semantic-cell list.
- `conformance/artifacts/inventory.json:5-111` owns artifact roles.
- `conformance/performance/budgets.json:6-1362` owns performance definitions.
- `conformance/internal/importguard/importguard.go:23-38,217-237,278-331,356-360` repeats Go and dependency versions.
- `conformance/go.mod:3-17` already declares those versions.
- `conformance/internal/importguard/importguard_test.go:481-499` repeats them again.
- `conformance/internal/contract/contract_test.go:25-35,106-121,163-205` locks the copied record counts and values.

**Evidence and cost**

The validator checks the requirement count and invariant-heading count against the same literal `111` in several places.
It also checks their complete ownership relationship, which already detects missing or duplicate owners.
Support-cell and artifact-role maps restate authored JSON values.
The import guard requires the same versions as `go.mod`, rather than limiting itself to the independence boundary.
An approved version update must edit both the dependency declaration and the guard's private dependency catalog.
These copied constants do not protect against an editor who can change both copies.

**Smallest coherent simplification**

Keep authored catalogs as the release policy source and bind them at Candidate freeze.
Keep schema shape, cross-reference integrity, uniqueness, and complete requirement ownership checks.
Delete redundant exact counts that follow from those relationships.
Keep `go.mod` and `go.sum` as dependency-version authority.
Retain the import guard's production-prefix, replacement, workspace, source-location, and forbidden-edge checks.
If a separately approved dependency allowlist is required, keep package identities without copying every resolved version.

**Invariant to retain**

Do not permit missing proof obligations, unauthorized production dependencies, local replacements, or an unapproved Candidate contract change.

**Acceptance**

An approved catalog edit should require one policy edit plus a new Candidate binding, not private validator-table updates.
Missing owners, duplicate IDs, forbidden imports, and local replacements must still fail.
Run `make test-conformance-imports`, `make test-conformance-contract`, and `make verify-contract`.
Removing release locks needs approval. This report does not authorize a support or dependency policy change.

**Existing issue:** The current Go `ParseImports` path is active. This finding does not establish the supplied `#102` unused-scanner lead.

<a id="16-verification-framework-f09"></a>

### 16-verification-framework-F09: Proof cardinality rules multiply overlapping requirements

**Severity: Medium.** The contract demands parallel metadata and proof types even when one assertion can establish several related requirements.

**Classification:** Contract decision.

**References**

- `conformance/internal/contract/policy.go:219-240,250-285` requires one invariant heading per requirement and one control per requirement.
- `conformance/internal/contract/policy.go:534-552` derives mandatory proof types from component presence.
- `conformance/schemas/fault-catalog-v1.schema.json:35` limits a control to one requirement.
- `conformance/schemas/scenario-v2.schema.json:579-602` limits fault and negative-control obligations to one requirement and assertion.
- `conformance/requirements.json:61-76,349-355` splits local CRUD access, Synchro CRUD authority, and no custom upload requirement.
- `conformance/requirements.json:178-184,691-697` overlaps final pull checksum requirements.
- `conformance/requirements.json:763-769` assigns native proof to the exporter's physical PostgreSQL transaction requirement.
- `conformance/README.md:18-28` requires one authoritative proof home for each behavior.

**Evidence and cost**

A control cannot reference two requirements even when one real operation and one assertion establish both.
The physical export transaction is a server observation, but its requirement also mandates `native-e2e`.
Native seed continuation and artifact acceptance are separate behaviors with their own requirements.
The one-control-per-requirement rule creates pressure to give similar mutations different requirement labels.
It also creates catalog prose and binding maintenance that does not itself improve assertion sensitivity.

**Smallest coherent simplification**

Assign proof obligations to concrete observable behavior and its owning surface.
Allow one real assertion or mutant to reference all requirements it actually establishes.
Keep distinct native implementation proofs when native behavior differs by implementation.
Do not require native execution to prove an internal PostgreSQL transaction property.
Merge only genuinely identical proof obligations, not merely similar wording or syntax across languages.

**Invariant to retain**

Every required behavior needs an authoritative executable proof and an appropriate negative control.
Independent expected values and all applicable implementation surfaces remain required.

**Acceptance**

Produce a requirement-to-assertion map before changing cardinality rules.
Show that removing one required behavior or its effective mutant still fails the revised gate.
Run `make test-conformance-contract`, `make test-conformance-scenarios`, and the retained runtime proof targets.
Static inspection identifies duplication pressure. It does not approve deletion of a product guarantee.

<a id="16-verification-framework-f10"></a>

### 16-verification-framework-F10: Observer SQL scanning repeats a boundary already enforced by construction and PostgreSQL

**Severity: Medium.** The scanner rejects safe identifiers without proving read-only behavior, and a mock test restates its own permission failure.

**Classification:** Behavior-preserving cleanup.

**References**

- `conformance/observer/read_only.go:10-115` implements a SQL token scanner and mutation-word blacklist.
- `conformance/observer/postgres.go:91-103,107-139,228-270` scans only framework-generated queries, often twice.
- `conformance/observer/types.go:19-25` exposes preconfigured names, not caller-supplied SQL.
- `conformance/observer/postgres.go:156-163` sets read-only mode twice.
- `conformance/observer/observer_test.go:63-72,190-199` hardcodes a permission error and then asserts that error.
- `conformance/blackbox/integration/real_baseline_test.go:1852-1927` already checks the real observer role and PostgreSQL permission-denied code.

**Evidence and cost**

Exact searches found no SQL scanner consumer outside the observer package.
All observer queries come from validated identifiers and fixed query constructors.
The scanner treats a quoted column named `update` as a mutation verb.
It accepts `SELECT pg_catalog.pg_terminate_backend(123)` because it does not resolve function behavior.
The mock role test returns permission denied regardless of the database's real role configuration.
The real integration helper already tests the actual authority boundary.

**Smallest coherent simplification**

Keep explicit relation and function allowlists, safe identifier quoting, and the restricted database connection.
Delete the general SQL lexer and duplicate query scans.
Keep structural rejection of internal relation names where required.
Use one driver-backed read-only transaction configuration.
Delete the mock role-denial test and preserve the real PostgreSQL permission test.

**Invariant to retain**

Observation cannot mutate synced state or acquire direct internal metadata authority.
Real database grants and transaction mode remain the authority for this behavior.

**Acceptance**

Test safe quoted identifiers, rejected internal relations, and actual denied writes with the restricted role.
Run `make test-blackbox-components`.
Run `make test-blackbox GO_TEST_ARGS='-run ^TestRealHTTPHarness$'` for the real observer boundary.
The scanner examples above are static token-path deductions, not executed SQL.

<a id="16-verification-framework-f11"></a>

### 16-verification-framework-F11: Soak operations store unused duplicate instructions

**Severity: Medium.** A journal appears to bind schema choices that the live consumer never reads.

**Classification:** Behavior-preserving cleanup.

**References**

- `conformance/soak/generator.go:82-139,166-174,385-421` stores typed fields and a JSON copy of the same fields.
- `conformance/soak/generator.go:308-344` validates one operation representation.
- `conformance/soak/runner.go:224-248` repeats most of that validation.
- `conformance/soak/journal.go:193-205,461-475` repeats operation checks at write and read boundaries.
- `conformance/soak/soak_test.go:399-418` tests mutation of the unused JSON input.
- `conformance/blackbox/integration/soak_harness_test.go:290-379` dispatches without reading `operation.Input` or `operation.SchemaVersion`.

**Evidence and cost**

`operationInput` serializes the operation's kind, identities, schema version, and fault plan a second time.
It also assigns `process_target` and `schema_transition` for every operation kind.
`validateOperationInput` checks only whether that copy is a JSON object.
The live consumer uses operation kind and target identities, not the duplicate JSON or random schema version.
The `Generator` object has no independent caller beyond the `Generate` wrapper.
The JSON input adds hashing and equality work without another executed instruction.

**Smallest coherent simplification**

Keep one typed operation representation.
Delete its unused JSON copy and the unused random schema-version field, unless an approved executor starts consuming that choice.
Inline the one-use `Generator` object into `Generate`.
Use one operation-shape check at the existing generation, execution, and journal boundaries.
Keep boundary validation rather than deleting the checks themselves.

**Invariant to retain**

The recorded seed and configuration must reproduce the exact instructions that execute.
Every recorded instruction must have a concrete consumer.

**Acceptance**

Require a retained operation-field mutation to change execution or fail plan validation.
Require seed replay to preserve operation order and real fault choices.
Run `make test-conformance-invariants` and a fixed-seed `make soak` run.
Exact consumer searches support the unused-field conclusion. No runtime schema-coverage claim is made.

<a id="16-verification-framework-f12"></a>

### 16-verification-framework-F12: Execution and performance APIs retain unused members

**Severity: Low.** Unused exported types and accessors increase the apparent framework contract and its maintenance cost.

**Classification:** Behavior-preserving cleanup.

**References**

- `conformance/execution/types.go:21,25-29,55-58,77-81` defines unused size, environment, publisher, and metric members.
- `conformance/internal/contract/performance_binding.go:10-14,29-39,46-69` snapshots all budgets and exposes a budget accessor.
- `conformance/internal/contract/performance_binding_test.go:21-29,43-46` provides the only budget-accessor calls.
- `conformance/blackbox/integration/real_configured_bounds_test.go:60-64` consumes only `RequiredMeasurement`.
- `conformance/internal/performance/evaluator.go:107-108` supports an unconfigured legacy metric.
- `conformance/internal/performance/evaluator_test.go:27` is that metric's only Go consumer.
- `conformance/schemas/performance-budgets-v2.schema.json:107` still advertises it.
- `conformance/faults/wire.go:133-150` includes a wrapper that only forwards identical arguments.

**Evidence and cost**

Exact searches found no consumer for `ArtifactBinding.SizeBytes`, `EnvironmentDimension`, `AttachmentPublisher`, or `MetricValue`.
The real performance-binding consumer never requests a budget.
`warm_connect_non_connect_http_requests` is absent from the authored budget catalog and retained only in evaluator and schema support.
`newRetryableServiceResponse` adds no validation or behavior to `newServiceResponse`.

**Smallest coherent simplification**

Delete the unused execution members and unused budget snapshot accessor.
Retain immutable copies for the required-measurement consumer.
Delete the unconfigured metric branch, its schema alternative, and its isolated test row.
Call `newServiceResponse` directly and delete the forwarding helper.

**Invariant to retain**

Keep the active artifact binding, required measurement, immutable-copy behavior, and canonical transport fault responses.

**Acceptance**

Repeat the exact consumer searches and compile the complete conformance module.
Run `make lint-conformance`, `make test-conformance-contract`, and `make test-conformance-faults`.
Run `make test-blackbox-configured-bounds` after removing the unused budget accessor.
The absence conclusions apply to this repository tree, not unknown external consumers.

<a id="16-verification-framework-f13"></a>

### 16-verification-framework-F13: Soak duplicates the invariant violation ordering rule

**Severity: Low.** A change to deterministic evidence ordering must update two identical implementations.

**Classification:** Behavior-preserving cleanup.

**References**

- `conformance/invariants/order.go:24-59` orders violations by sequence, family, rule, and evidence.
- `conformance/soak/runner.go:343-379` repeats the same comparator and sorting function.
- `conformance/soak/runner.go:141-143,289-298` sorts combined results and then sorts them again.
- `conformance/soak/soak_test.go:461-486` checks ordering with the same local comparator.

**Evidence and cost**

Both implementations define the same ordering over the same exported violation type.
They have the same reason to change and the same two concrete consumers.
The runner also orders the result of `checkLatest`, although `checkLatest` already orders its combined result.

**Smallest coherent simplification**

Keep one ordering operation with the violation type and use it for combined checker results.
Delete the soak comparator copy and unnecessary second sort.
Keep a fixed expected-order test rather than validating only with the comparator under test.

**Invariant to retain**

Equivalent captures produce stable, bounded, deterministic violation output.

**Acceptance**

Use deliberately unordered violations from several families with equal primary sort keys.
Require exact expected order after composition.
Run `make test-invariants` and `make test-conformance-invariants`.

<a id="16-verification-framework-f14"></a>

### 16-verification-framework-F14: Mutant patches maintain generated SQL and source-location comments by hand

**Severity: Medium.** Source changes require parallel patch maintenance for derived SQL, including unrelated generated comment positions.

**Classification:** Behavior-preserving cleanup.

**References**

- `conformance/mutants/integration/issue49-security-health-aggregation.patch:14-26` edits generated source-location comments.
- `conformance/mutants/integration/issue49-security-health-limits.patch:24-36` repeats this work after a different source change.
- `conformance/mutants/integration/issue49-remaining-ledger-retention.patch:21-89` duplicates source SQL changes and updates derived line comments.
- `conformance/mutants/integration/issue49-data-portable-counter-bound.patch:1-23` repeats the same SQL constraint mutation in source and generated SQL.
- `conformance/mutants/integration_gate.sh:60-67,101-113,211-232` applies and packages the combined patch.
- `conformance/requirements.json:790-796` makes pgrx declarations the sole authored installation source.
- `conformance/README.md:65-66` identifies the supported SQL generation and comparison commands.

**Evidence and cost**

Several patches contain both the intentional source defect and its generated representation.
Other SQL hunks change only comments such as `health.rs:1065` to `health.rs:1066`.
Those comments are derived packaging content, not independent negative controls.
The combined patch can become stale from generated offsets unrelated to the intended defect.

**Smallest coherent simplification**

Apply the mutant to authored source in the isolated workspace.
Generate installation SQL from that mutated source through the supported Make target before packaging.
Delete handwritten generated SQL hunks and source-location comment edits.
Keep a deliberate generated-artifact tamper only when generated-artifact integrity is the behavior under test.

**Invariant to retain**

The packaged mutant must contain the intended production defect and otherwise follow the real packaging path.
Regeneration or compilation failure must still fail the gate, not count as detection.

**Acceptance**

Run `make generate-pg-sql` and `make check-pg-sql` inside each affected isolated mutant workspace.
Run a selected control with `make test-integration-mutant INTEGRATION_MUTANT_ID=issue49-security-health-aggregation`.
Require baseline pass, semantic mutant failure, and post-baseline pass.
All existing patches passed read-only `git apply --check` during this review.
That static result does not establish build or runtime sensitivity.

<a id="16-verification-framework-f15"></a>

### 16-verification-framework-F15: Wire fault ownership cannot cancel a request waiting for response headers

**Severity: Medium.** Fault cleanup can finish while an upstream request remains active under a different request context.

**Classification:** Correctness defect.

**References**

- `conformance/faults/wire.go:46-82` binds fault cleanup to the owner context.
- `conformance/faults/wire.go:94-123,315-328` sends the original request context to the upstream transport.
- `conformance/faults/wire.go:172-214,236-260,331-364` registers only response bodies after `RoundTrip` returns.
- `conformance/faults/faults_test.go:207-254` tests cancellation only after a response body exists.

**Evidence and cost**

A request can enter `RoundTrip` with an uncanceled context that differs from the fault's context.
If upstream waits for headers, the fault has no active closer for that request.
Canceling the fault or closing its owner empties the response-body map and closes `Done`.
The request still uses its original context and can remain active.
The cleanup bookkeeping therefore does not cover the full transport lifetime it manages.

**Smallest coherent simplification**

Use one owned cancellation boundary for the active request and its response body.
Propagate fault closure to an upstream request before headers arrive.
Retain the request's own cancellation and stop any linking callback when its transport lifetime ends.
Do not add a separate transport implementation.

**Invariant to retain**

Closing a fault must release its owned work without changing the requested fault's upstream completion semantics.

**Acceptance**

Use a deterministic upstream test double that waits for its request context to close before returning headers.
Close the fault while the caller's request context remains live.
Require the upstream call to stop and the ownership state to become idle.
Run `make test-conformance-faults`.
The uncovered lifetime follows from the registration order. No network request ran here.

<a id="16-verification-framework-f16"></a>

### 16-verification-framework-F16: Soak opens a journal before validation installs cleanup

**Severity: Low.** Invalid public inputs can truncate a caller-owned journal and leave its descriptor open.

**Classification:** Correctness defect.

**References**

- `conformance/soak/runner.go:77-109` creates the journal before checking context, harness, and complete plan validity.
- `conformance/soak/journal.go:151-190` opens with `O_TRUNC` and writes the header.
- `conformance/soak/journal.go:305-317` provides cleanup, but the early `runPlan` returns bypass it.

**Evidence and cost**

With a valid generated plan and nil harness, `Run` opens and truncates the journal first.
`runPlan` then returns `ErrHarnessRequired` before its cleanup defer is registered.
The same structure applies to nil context and invalid operation shape.
Duplicated entry-point validation separates resource acquisition from resource ownership.

**Smallest coherent simplification**

Validate public run inputs before opening the journal.
Install journal cleanup immediately after creation in the function that owns creation.
Delete the later conditional cleanup ownership from `runPlan`.
Preserve close errors beside an existing run error instead of silently discarding them.

**Invariant to retain**

Every acquired journal closes on every return path, and replay never overwrites its source journal.

**Acceptance**

Use an existing sentinel journal with nil context, nil harness, and an invalid plan.
Require each rejected call to preserve the sentinel bytes and acquire no persistent descriptor.
Run `make test-conformance-invariants`.
This is a static control-flow reproduction, not an executed descriptor-leak test.

<a id="16-verification-framework-f17"></a>

### 16-verification-framework-F17: Runtime invariant checkers embed individual probe shapes

**Severity: Medium.** Reusing these checkers requires recreating narrow example flows instead of supplying the actual operation observations.

**Classification:** Behavior-preserving cleanup.

**References**

- `conformance/invariants/mutation_conservation.go:63-114` permits only insert requests with matching current and authored schemas.
- `conformance/invariants/mutation_conservation.go:238-295` also requires conflict rows to contain the submitted column values.
- `conformance/invariants/cursor_monotonicity.go:293-307` requires exactly one change and two cursor positions.
- `conformance/invariants/checksum_convergence.go:50-55` requires exactly one change on a terminal pull.
- `conformance/invariants/scope_isolation.go:49-67` requires one selected scope and rejects every returned change.
- `conformance/invariants/observation.go:123-162` documents these as checker-owned control captures.
- `conformance/soak/capture.go:247-281,375-401` repeats their narrow shapes as capture requirements.
- `conformance/blackbox/integration/soak_harness_test.go:972-1045` drains existing work and creates special pull probes to meet those shapes.
- `conformance/requirements.json:88-94,502-508,592-607` defines reconciliation, partition, scope, and cursor requirements without those cardinalities.

**Evidence and cost**

These functions intentionally inherit individual source-test assumptions, as their comments state.
They are therefore not general checks for the invariant families named by their APIs.
A legitimate conflict can return an authoritative value different from the submitted value.
One scope or several changes can also satisfy the protocol's cursor and checksum rules.
The soak layer recreates a one-change, two-scope flow and an extra empty pull rather than checking arbitrary generated pull traffic.
The same example-shape rules then appear in both capture validation and invariant validation.

**Smallest coherent simplification**

Keep generic partition, digest, cursor, and membership checks independent of one probe's row and scope counts.
Keep exact probe expectations in the authored scenario or its existing test, not in a second capture schema.
Pass independently authored expected rows when a probe needs row equality.
Do not derive expected rows from the response being checked.
Delete repeated one-change, two-cursor, and insert-only assumptions from the shared invariant path.

**Invariant to retain**

Preserve complete outcome partitions, canonical authoritative reconciliation, exact digest computation, scoped membership, and acknowledged monotonic progress.
Keep specific negative controls for omissions, duplicates, wrong rows, and unauthorized scopes.

**Acceptance**

Check a one-scope pull, a multi-change terminal pull, and an update conflict with a different authoritative row.
Require valid captures to pass and their corresponding omission, digest, and scope mutants to fail.
Run `make test-invariants` and `make test-conformance-invariants`.
Run the affected production mutation controls before deleting their special probe setup.
Static inspection establishes the duplicated probe constraints, not a failure of an executed client operation.

**Applicable contract:** `SYNC-MUTATION-002`, `SYNC-OUTCOME-001`, `SYNC-CURSOR-004`, `SYNC-INTEGRITY-004`, `SYNC-INTEGRITY-005`.


<a id="area-17-build-release"></a>

## Build, release, and packaged consumers

<a id="17-build-release-f01"></a>

### 17-build-release-F01: Client smoke infers transfer success from local queue counts

**Severity:** High. A broken transfer path can receive five terminal operation passes.

**Classification:** Correctness defect.

**References**

- `verification/consumers/kotlin/app/src/main/kotlin/com/trainstar/synchro/consumer/MainActivity.kt:72-135`.
- `verification/consumers/swift-ios/SynchroConsumer/AppDelegate.swift:167-239`.
- `verification/consumers/swift/Sources/SynchroConsumer/main.swift:121-205`.
- `verification/consumers/react-native/App.tsx:91-174`.
- `verification/packaged_smoke.py:436-485`.
- Compared implementation: `verification/consumers/server/public_smoke.go:328-337,410-475,494-540,643-721`.
- Contract: `RELEASE.md:92,120-128` and `docs/src/content/docs/spec/02-client-contract.mdx:322-369`.

Each client authors its own rows, drains the queue, queues one update, and drains that update after restart.
None reads an independently authored remote value after pull.
Accepted push reconciliation already applies canonical rows before WAL echoes.
Thus, a pull implementation that discards incoming rows can leave every asserted local value unchanged.

The common completion helper accepts two queue counts and process identifiers.
It then generates passed entries for connect, push, pull, kill, and resume.
The entries add no independent transfer evidence.
Terminal rejection also differs from acceptance under the client contract.
A drained sendable queue does not identify which outcome occurred.

The server consumer shows the stronger, bounded alternative.
It requires an accepted mutation and an exact authored row from incremental pull.

**Smallest simplification:** Use one observable cross-client transfer in the client lifecycle.
Verify an independently authored value after pull and the durable update at its receiving endpoint after resume.
Delete queue-count-only transfer certification.
Keep queue counts as durability evidence, not acceptance or delivery evidence.
Do not add another general protocol runner.

**Invariant:** Each required operation needs its own observable terminal outcome through public APIs.
Stop and close must finish before resume success is reported.

**Acceptance:** Run `make test-packaged-smoke-structure` and each affected `make release-run-support-cell` invocation.
Use a mutant that discards pulled rows while preserving schema, cursors, and successful calls.
Use a terminal-rejection control that drains sendable work.
Neither control may produce a passed cell.

**Binding:** Release-process Step 4 and all required client support cells.
No existing issue match was established from the supplied issue list.

<a id="17-build-release-f02"></a>

### 17-build-release-F02: Client package evidence hashes sealed inputs instead of the consumed copies

**Severity:** High. Consumer substitution can remain invisible to the sealed-byte gate.

**Classification:** Correctness defect.

**References**

- `Makefile:790-817,846-865,1588-1643`.
- `verification/consumers/kotlin/test-consumer-device.sh:14-15,172-189`.
- `verification/consumers/swift-ios/test-consumer.sh:10-18,52-54,223-240`.
- `verification/consumers/react-native/test-consumer.sh:24-35,121-136,404-421`.
- `verification/packaged_smoke.py:462-467,758-799`.
- Compared implementation: `scripts/release-artifacts.py:847-856,867-881`.

`release-consumer-artifacts` reuses an existing directory after checking only `.release-manifest.sha256`.
The prepared-artifact branches check file presence, not content identity.
Kotlin consumes the extracted Maven repository.
Swift consumes the copied source directory.
React Native consumes the copied tarball and native dependencies.

The completion calls instead hash the original sealed ZIP, tarball, or release manifest.
Changing an extracted AAR or copied Swift source leaves those original hashes unchanged.
The final `release-verify` also checks the sealed directory, not those consumed copies.

The server path already has a direct verified-execution boundary.
Its adapter materializer compares the copied executable with the sealed digest.

**Smallest simplification:** Remove marker-only reuse of mutable consumer staging.
Materialize a fresh consumer input set for each run and verify the exact consumed payloads against sealed identities.
For source packages, verify the consumed source tree against the bound Git tree.
Keep one sealed-to-consumer boundary instead of three preparation flags with different guarantees.

**Invariant:** The bytes exercised by a package cell must be the bytes authorized for publication.
Consumer compilation remains allowed.

**Acceptance:** Run `make test-release-artifacts` and `make test-packaged-smoke-structure`.
Run `make release-consumer-artifacts VERSION=0.3.0 RELEASE_DIR="$RELEASE_DIR"` against a sealed candidate.
Alter one prepared AAR and one prepared Swift source before their respective cell runs.
Each affected `make release-run-support-cell` must reject substitution before reporting success.

**Binding:** Release-process Step 4, especially its changed-byte and source-substitution acceptance criteria.

<a id="17-build-release-f03"></a>

### 17-build-release-F03: Local lifecycle targets terminate processes they do not own

**Severity:** High. A routine test command can terminate an unrelated service or emulator.

**Classification:** Correctness defect.

**References**

- `Makefile:1433-1447,2005-2016,2067-2094`.
- Related PID-only ownership checks: `Makefile:1941-1944,1973-1988` and `scripts/ci/start-adapter.sh:14-20`.
- Compared owned-process flow: `verification/consumers/server/test-consumer.sh:29-46,85-104`.

Adapter start kills every listener on the selected port before checking its PID file.
Adapter stop repeats the same port-owner search after its PID-file branch.
The emulator reset terminates every emulator with a matching AVD name.
Neither port ownership nor a shared AVD name proves ownership by this invocation.

PID-file checks also identify a process only by a reusable integer.
The foreground server consumer instead retains child identities from its own launches.

**Smallest simplification:** Delete port-owner and AVD-name termination loops.
Reject occupied resources that the invocation does not own.
Use explicit instance ownership for persistent local helpers and child handles for bounded runs.
Retain intentional process kills inside owned recovery tests.

**Invariant:** Cleanup may terminate only resources owned by the current test fixture.

**Acceptance:** Add the focused target `make test-ci-process-lifecycle`.
Start an unrelated listener on the configured port and an unrelated matching-name emulator fixture.
Start, stop, restart, and failure cleanup must leave both untouched.
Test a stale PID record without sending a signal to that process.

**Existing issue:** Matches supplied issue `#98` for adapter port-owner termination.
The emulator and PID-reuse evidence extends that ownership concern.

<a id="17-build-release-f04"></a>

### 17-build-release-F04: Adapter startup has two readiness owners and loses cleanup authority

**Severity:** Medium. Failure paths can leave an adapter running without its PID record.

**Classification:** Correctness defect.

**References**

- `scripts/ci/start-adapter.sh:23-58`.
- `Makefile:2017-2065`.

The script starts the adapter and polls its schema endpoint.
The Make recipe then sleeps, checks liveness, and polls the same endpoint again.
Both layers own readiness failures.

After the script returns, the outer HTTP failure removes the PID file without terminating the adapter.
The seed-generation failure does the same thing.
An in-use seed failure exits without cleaning up the already started process.
These branches force later cleanup toward the unsafe port scan in F03.

**Smallest simplification:** Give the startup script sole ownership of spawn, liveness, readiness, and failed-start cleanup.
Prepare the database and seed before launching the adapter where their dependencies permit it.
Otherwise retain one outer ownership trap until all initialization finishes.
Delete the second readiness loop and its separate failure paths.

**Invariant:** Failed initialization must preserve diagnostics and terminate every child it started.

**Acceptance:** Use the proposed `make test-ci-process-lifecycle` target for occupied ports, readiness failure, and seed failure.
Each control must terminate the owned child, preserve the failure, and remove only its own PID record.

**Binding:** Release-process Steps 3 and 6.
This finding is separate from F03 because duplicate lifecycle ownership causes the cleanup loss.

<a id="17-build-release-f05"></a>

### 17-build-release-F05: Several required test gates accept native runner success without result integrity

**Severity:** Medium. Zero-test or skipped-test execution can remain green.

**Classification:** Correctness defect.

**References**

- `Makefile:867-871,1734-1742,1829-1833`.
- `.github/workflows/ci.yml:65-66,321-338`.
- Compared parsed gates: `Makefile:905-911,995-1002,1044-1049,1063-1067`.
- Test entry points: `scripts/ci/test_release_artifacts.py:509-510`, `scripts/ci/test_release_publish.py:428-429`, `verification/test_packaged_smoke.py:542-543`.

The Python targets use ordinary `unittest` success without checking executed and skipped counts.
The Kotlin packaged-device target invokes Gradle without the JUnit parser used by neighboring targets.

An in-memory reproduction confirmed that ordinary `unittest` accepts an empty suite and a skipped test.
This is a concrete gate property, not a claim that current tests were skipped.

**Smallest simplification:** Apply one small count-aware Python runner to the three concrete Python consumers.
Reuse the existing JUnit parser for the Kotlin device target.
Do not introduce another general evidence framework.

**Invariant:** Required gates reject zero execution, skipped work, missing results, and failures.

**Acceptance:** Run `make test-release-artifacts test-release-publish test-packaged-smoke-structure`.
Add empty-suite and skipped-test controls to those entry points.
Run `make test-consumer-kotlin-device` with a zero-match instrumentation control and require failure.

**Binding:** `AGENTS.md:369-375` and release-process Steps 3 and 6.

<a id="17-build-release-f06"></a>

### 17-build-release-F06: Private Maven classification has no workflow producer

**Severity:** Medium. Tests maintain an unused state model that contradicts actual recovery behavior.

**Classification:** Behavior-preserving cleanup.

**References**

- `scripts/release-publish.py:310-338,463-470,592-601,642-650,684-692`.
- `scripts/ci/test_release_publish.py:90-145,273-286`.
- `.github/workflows/release.yml:1077-1144,1172-1202`.

`observe_public` always supplies null private deployment fields.
The workflow classifies only that public observation.
It handles private Central state through `central-select`, `central-recovery-action`, and explicit workflow branches.
No workflow call produces the classifier's private deployment state.

The classifier tests say a matching validated deployment resumes publication.
The actual recovery function replaces an unpublished validated deployment.
The classifier rejects `FAILED`, while actual recovery replaces that unpublished deployment.
The two implementations therefore describe different recovery policies.

**Smallest simplification:** Remove private deployment fields and branches from public-state classification.
Delete tests for those unreachable classifier states.
Keep the actual Central recovery action and its controls.
Remove the unused standalone `classify` command if no supported external consumer requires it.

**Invariant:** Never replace an irreversible Central deployment.
Always verify public Maven bytes before continuing publication.

**Acceptance:** Run `make test-release-publish`.
Retain controls for `PENDING`, `VALIDATING`, `VALIDATED`, `FAILED`, `PUBLISHING`, and `PUBLISHED` through the live recovery function.

**Existing issue:** Matches supplied issue `#99`.
Exact source searches found no workflow use of the standalone classifier command.

<a id="17-build-release-f07"></a>

### 17-build-release-F07: Obsolete support routes include a mobile substitute for server proof

**Severity:** Medium. One exposed route mislabels proof, and another route cannot begin.

**Classification:** Correctness defect with behavior-preserving deletion of unreachable paths.

**References**

- `Makefile:1775-1818`, especially `1789-1793` and `1800-1805`.
- `Makefile:1693-1697`.
- `verification/packaged_smoke.py:95-114,166-168,436-485,553-632`.
- `verification/consumers/swift/test-consumer.sh:1-125`.
- `verification/consumers/swift/Sources/SynchroConsumer/main.swift:4-223`.
- Compared proper server route: `Makefile:819-845`.
- Matrix: `conformance/support-matrix.json:62-71,104-111`.

The generic platform target routes the PostgreSQL cell to the Kotlin device smoke.
Adding an extension-manifest hash does not turn client queue evidence into server replay evidence.
The cell validator does not bind lifecycle kind to support-cell component.
An in-memory control confirmed that it accepts client-only lifecycle evidence for the PostgreSQL cell.

The same target offers a macOS package route.
Its preceding `begin-cell` call accepts only required cells.
macOS has policy `tested`, so this route always fails before dispatch.
The separate macOS lifecycle script also reaches completion through a helper that rejects the macOS cell.

**Smallest simplification:** Remove PostgreSQL from the client router and retain the dedicated sealed-server route.
Reject a lifecycle kind that does not match its matrix component.
Remove the unreachable macOS package lifecycle, script, and corresponding CLI branch.
Keep the small macOS local package consumer used by Candidate CI.
This also removes a second copy of the Swift lifecycle without creating a shared abstraction.

**Invariant:** A support cell must certify its declared component.
macOS semantic testing must remain separate from required package certification.

**Acceptance:** Run `make test-packaged-smoke-structure`.
Add negative controls for client evidence under the server cell and server evidence under a client cell.
Run `make release-run-support-cell SUPPORT_CELL_ID=SUP-PG-LINUX-X64-001 VERSION=0.3.0 RELEASE_DIR="$RELEASE_DIR"` with its server prerequisites.
It must not require an Android emulator.
Run `make test-consumer-swift` to preserve the remaining local package consumer.

**Binding:** Release-process Step 4 and its explicit server-without-emulator acceptance criterion.

<a id="17-build-release-f08"></a>

### 17-build-release-F08: Unconsumed verification code adds a false maintenance surface

**Severity:** Low. Maintainers can mistake unused checks for active protection.

**Classification:** Behavior-preserving cleanup.

**References**

- `verification/consumers/react-native/artifactSmoke.ts:1-11`.
- `verification/packaged_smoke.py:37-49,132-151,1051-1052,1104-1106`.
- `verification/test_packaged_smoke.py:528-539`.
- `Makefile:264,1595,1617,1643`.
- Actual consumer selection: `verification/consumers/react-native/test-consumer.sh:81-88,121-136`.

No tracked consumer imports `makePackagedClient` or copies `artifactSmoke.ts` into the generated application.
The public-source scanner has only its CLI branch and synthetic unit-test callers.
No Make target or workflow invokes that CLI.
Its `.package(path:` prohibition also exempts `Package.swift`, where that syntax actually occurs.

`RELEASE_STAGED_ARTIFACTS` has no tracked setter.
The active release path uses `release-consumer-artifacts` and `CLIENT_ARTIFACTS_PREPARED` instead.
The unused flag only verifies a release and returns without preparing consumer paths.

**Smallest simplification:** Delete the unused probe, scanner, scanner-only test, and unused staged-artifact flag branches.
Retain isolated package resolution and real consumer compilation.
Do not connect a brittle text scanner merely to justify retaining it.

**Invariant:** Public package consumers must use the intended public API and distribution inputs.

**Acceptance:** Run `make test-packaged-smoke-structure` and the affected `make test-consumer-rn-ios` or `make test-consumer-rn-android`.
Exact source searches must show no remaining deleted identifiers.

**Existing issue:** The unused probe and import scanner match supplied issue `#102`.
The unused staged-artifact flag is additional cleanup evidence.

<a id="17-build-release-f09"></a>

### 17-build-release-F09: React Native focused targets duplicate the same execution recipe

**Severity:** Medium. One runner change requires edits across many copies and platform variants.

**Classification:** Behavior-preserving cleanup.

**References**

- `Makefile:1088-1394`.
- Compared existing common recipe: `Makefile:1549-1561`.
- Shared build entry points: `Makefile:1449-1451,1492-1496`.

Focused targets repeat Android environment checks, Detox builds, warm-connect setup, and exact test-result invocation.
Their changing data are the platform, configuration, test name, timeout, and occasional seed prerequisite.
Some iOS targets use `rn-ios-build`, while others repeat its build command and prerequisite list.

The corpus target already demonstrates a shared platform recipe with explicit dispatch.
This finding concerns repeated shell meaning, not repeated Swift and Kotlin semantics.

**Smallest simplification:** Keep focused target names and explicit test bindings.
Use target-specific variables and one execution recipe per genuinely different setup path.
Reuse existing platform build targets.
Delete copied build and test-launch blocks.
Keep exceptional prerequisites and timeouts visible in the target declarations.

**Invariant:** Each focused target must select its exact intended test and fail on zero matches.

**Acceptance:** Compare dry-run recipes for every affected target.
Run `make test-rn-warm-connect-ios`, `make test-rn-warm-connect-android`, and one seed-dependent target on prepared fixtures.
Retain the exact-test negative control.

**Binding:** Release-process Step 6.
This is adjacent to `#93`, but it is not the repeated journey-protocol implementation described by that issue.

<a id="17-build-release-f10"></a>

### 17-build-release-F10: Recovery repeats every package cell before resuming publication

**Severity:** Medium. A public-check interruption requires rebuilding and rerunning all package consumers.

**Classification:** Behavior-preserving cleanup, subject to evidence reuse verification.

**References**

- `.github/workflows/release.yml:52-85,94-101,283-321,416-419,476-492,651-677,784-838`.
- `.github/workflows/release.yml:889-893`.
- Recovery contract: `RELEASE.md:164-174`.

Recovery restores the original sealed distribution, which is correct.
It nevertheless reruns the complete server, Android, and Apple package matrix.
The workflow retains `package-gate-evidence` for 90 days but never restores it during recovery.
Publication always consumes a newly generated summary from the recovery run.

This repeats completed installation and lifecycle work after failures that occur only during public checks.
It also adds runtime-availability dependencies to recovery of unchanged public bytes.

**Smallest simplification:** Restore original package evidence when its source, sealed identity, and originating workflow are verified.
Rerun package cells only when their original evidence is absent or incomplete.
Retain the final summary verification before approval and publication.
Do not manufacture replacement success records.

**Invariant:** Reused evidence must bind the exact original sealed candidate and successful original package execution.

**Acceptance:** Extend `make test-release-publish` with recovery-routing controls.
A recovery after a public-consumer failure must reuse valid package evidence without launching package builds.
Wrong-source, wrong-hash, failed, or missing package evidence must never authorize publication.

**Binding:** Release-process Step 5 and the documented incomplete-public-check recovery rule.

<a id="17-build-release-f11"></a>

### 17-build-release-F11: The scheduled aggregate omits broad integration mutation

**Severity:** Medium. The named aggregate can pass while a scheduled validation job fails or remains incomplete.

**Classification:** Correctness defect.

**References**

- `.github/workflows/scheduled-validation.yml:66-85,119-132`.
- `Makefile:1893-1901`.

The workflow defines `broad-integration-mutation` as a required-looking scheduled job.
The final `scheduled` job neither needs it nor checks its result.
The aggregate therefore does not represent every job in that workflow.
This does not claim that GitHub hides the failed individual job.

**Smallest simplification:** Include the missing job in the aggregate dependency and result checks.
If the job is intentionally advisory, remove the misleading aggregate claim through an explicit policy decision.

**Invariant:** An aggregate status must reflect every job that it claims to require.

**Acceptance:** Add a workflow-structure control to `make test-release-artifacts`.
Removing any required job from the aggregate must fail that control.
The primary can then verify the aggregate with an injected failed dependency in a controlled workflow run.

**Binding:** Release-process Step 3 and scheduled broad mutation policy.

<a id="17-build-release-f12"></a>

### 17-build-release-F12: Candidate server jobs serialize unrelated hosted installations

**Severity:** Medium. Independent candidate runs wait on a repository-wide lock without a shared installation resource.

**Classification:** Behavior-preserving cleanup.

**Existing issue:** `#94` already records this cross-run installation lock.

**References**

- `.github/workflows/ci.yml:133-145,167-190`.
- `scripts/ci/configure-conformance-adapter.sh:9,38`.
- Compared justified serialization: `.github/workflows/release.yml:14-16`.

Every candidate server job uses `ubuntu-24.04` on its own hosted runner.
Its PostgreSQL installation and test lock live on that runner.
The fixed `candidate-server-installation` concurrency group serializes all these independent jobs.
Publication serialization has a shared external resource, but candidate installation does not.

**Smallest simplification:** Delete the candidate job's repository-wide concurrency group.
Retain fixture-local installation locks and serialized public publication.

**Invariant:** Tests sharing one local installation must remain mutually exclusive.

**Acceptance:** Static inspection must show no cross-run shared installation path or resource.
The primary can run two controlled Candidate jobs concurrently to verify fixture independence.
No product test execution was necessary to establish the redundant lock scope.

**Binding:** Release-process Step 3.

<a id="17-build-release-f13"></a>

### 17-build-release-F13: A handwritten OpenPGP parser validates shape, not signatures

**Severity:** Medium. Package validation can accept unusable detached signatures before public GitHub assets are published.

**Classification:** Correctness defect with a simpler official-tool replacement.

**References**

- `scripts/release-artifacts.py:304-366`.
- `scripts/ci/test_release_artifacts.py:120-166`.
- `Makefile:749-754`.
- `.github/workflows/release.yml:988-1033,1065-1144`.

The parser implements armor parsing, Base64 handling, CRC-24, and a packet-tag check.
It never verifies the signature against the payload or an expected signing key.
The test fixture supplies `b"\xc2\x08fixture!"`, which is not a real signing result.
An in-memory reproduction confirmed that the validator accepts that fixture packet.

The current code proves limited syntax only.
Central validation occurs later, after the workflow makes GitHub assets public.
Keeping a partial OpenPGP implementation adds maintenance without establishing the stronger property suggested by signed-payload validation.

**Smallest simplification:** Verify detached signatures with the official signing tool at the package-build boundary.
Use the public identity corresponding to the configured release signing key.
Delete the custom CRC and packet parser.
Retain payload presence, exact coordinate, archive-path, and checksum checks.

**Invariant:** Each sealed detached signature must verify the exact associated payload under the authorized signing identity.

**Acceptance:** Run `make test-release-artifacts` with a valid signature, changed payload, wrong signer, and nonsigning packet control.
The nonsigning packet and changed payload must fail before sealing.
The primary owns signing-identity configuration and any credential-dependent validation.

**Binding:** Release-process Step 4, signed Maven bundle validation.
This review does not approve a signing-key policy change.

<a id="17-build-release-f14"></a>

### 17-build-release-F14: Duplicated support-resolution validation has already diverged

**Severity:** Low. A sealed-manifest verifier accepts duplicate support records that staging rejects.

**Classification:** Correctness defect.

**References**

- `scripts/release-artifacts.py:531-547`.
- Compared verifier: `scripts/release-artifacts.py:804-813`.

Staging checks duplicate cell IDs with an explicit `seen` set.
Verification compares only a set of IDs with the required set.
Repeated records disappear in that comparison.
The two blocks otherwise repeat environment shape and unresolved-selector checks.

**Smallest simplification:** Use one support-resolution validator from staging and sealed verification.
Delete the duplicated verifier block.
Keep exact cardinality, unique IDs, required-cell coverage, and concrete environment values.

**Invariant:** A candidate has exactly one environment resolution for each required support cell.

**Acceptance:** Run `make test-release-artifacts`.
Add a duplicate support record, recompute the fixture checksum file, and require sealed verification to reject it.

**Binding:** Release-process Step 4 and exact-set verification.

<a id="17-build-release-f15"></a>

### 17-build-release-F15: Registry polling repeatedly downloads already verified distributions

**Severity:** Medium. Waiting for one registry repeatedly depends on all other registry downloads.

**Classification:** Behavior-preserving cleanup.

**References**

- `scripts/release-publish.py:531-602`.
- `.github/workflows/release.yml:1196-1202,1232-1251,1307-1310`.

Each `observe-public` call downloads all public GitHub release assets, the npm tarball when present, and every Maven bundle entry.
The Maven loop can invoke this full observation 120 times.
The npm loop can invoke it 40 times.
Only one registry state is changing during either loop.

This couples a Maven visibility wait to unrelated GitHub downloads.
It couples npm visibility to another complete Maven verification.
The final full public check is independently necessary and already exists.

**Smallest simplification:** Poll only the changing registry during its visibility wait.
Verify that registry's exact public bytes before advancing.
Keep one final cross-registry identity check before declaring completion.
Delete repeated full-distribution observation from the inner polling loops.

**Invariant:** No registry success may substitute for exact public-byte verification.
The final state must still include tags, GitHub assets, Maven, npm, provenance, and latest status.

**Acceptance:** Run `make test-release-publish` with request-count controls around registry visibility polling.
An unrelated registry must not be fetched on every poll.
Changed bytes in the polled registry and final full observation must still fail.

**Binding:** Release-process Step 5.

<a id="17-build-release-f16"></a>

### 17-build-release-F16: Compound negative compilation probes do not identify the failed boundary

**Severity:** Medium. One remaining compile error can hide a newly exposed internal API.

**Classification:** Correctness defect in verification.

**References**

- `verification/consumers/kotlin/test-internal-api-rejection.sh:91-137`.
- Duplicate project configuration: `verification/consumers/kotlin/test-internal-api-rejection.sh:22-89`.
- Compared configuration: `verification/consumers/kotlin/settings.gradle.kts:1-32`.
- Compared configuration: `verification/consumers/kotlin/build.gradle.kts:1-4`.
- Compared configuration: `verification/consumers/kotlin/app/build.gradle.kts:1-41`.
- Actual database boundary: `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroDatabase.kt:66-86`.
- Actual metadata boundary: `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroMeta.kt:108-119`.

Each probe combines multiple forbidden accesses in one compilation.
The helper accepts any failed compilation whose text mentions both type names.
The Java probe also invokes a private constructor and a getter that the wrapper does not expose.
These errors can remain after a different boundary becomes public.

The test therefore proves that the compound program fails.
It does not prove each intended restriction independently.
The shell embeds another Gradle consumer configuration alongside the checked-in consumer project.

**Smallest simplification:** Reuse the existing consumer project setup for small, isolated forbidden-access probes.
Give each probe one intended failure and a positive compile control using the same toolchain.
Require the relevant access diagnostic rather than arbitrary type-name mentions.
Delete repeated project setup and compound failure programs.

**Invariant:** Verification must fail when the protected API becomes accessible, not merely when any source error disappears.

**Acceptance:** Run `make test-consumer-kotlin` against the normal package and controlled visibility mutants.
Each exposed-boundary mutant must make its corresponding negative probe fail the gate.
This report establishes the static masking path, not an executed Kotlin mutant result.

**Binding:** Release-process Step 4, public package boundary proof.

<a id="17-build-release-f17"></a>

### 17-build-release-F17: Current-stable policy requires a product decision about release availability

**Severity:** Medium. The policy creates recurring platform-resolution work that the workflow represents as copied constants.

**Classification:** Contract decision.

**References**

- `conformance/support-matrix.json:5-8,95-101,124-156`.
- `.github/workflows/release.yml:323-340,485-492,663-677,709-727,1280-1294`.
- `verification/consumers/react-native/test-consumer.sh:81-85`.
- Release-process plan: `docs/superpowers/plans/2026-09-14-synchro-release-process.md:66-69,78-85`.

The contract says to resolve current stable vendor versions at candidate creation.
The workflow instead writes fixed iOS, Xcode, Android, and React Native values into the resolution file.
It repeats those values in execution matrices and public-consumer setup.
The generated consumer script repeats the React Native patch version again.

Frozen candidate versions are necessary.
Calling copied constants a current-stable resolution does not establish how or when that resolution occurred.
Every vendor update can require coordinated edits and new runtime acquisition before another candidate can satisfy the policy.

**Decision needed:** Retain a true current-stable obligation, or certify an explicitly named supported runtime set per release.
The second option preserves minimum-platform checks and exact candidate identities without automatic vendor-release timing pressure.
Either option should use one candidate environment input for manifest records and execution selection.
Delete independently maintained copies after the primary selects the policy.

**Invariant:** The published support claim must match the environments actually exercised.
Never weaken missing-runtime failures or silently substitute a runner's installed runtime.

**Acceptance:** Add environment-selection controls to `make test-release-artifacts`.
One changed input must update both declared and selected versions, or produce a mismatch failure.
Vendor resolution and hosted-runner availability require primary-owned external evidence.
No claim here says that a particular pinned runtime is currently unavailable.

**Binding:** Release-process Steps 1 and 4 and the support matrix's current-track policy.


<a id="area-18-history-plans"></a>

## Historical plans and reports

<a id="18-history-plans-f01"></a>

### 18-history-plans-F01: Some archived task records still present obsolete release instructions as active work

- **Severity:** Medium. A reader can restart removed verification machinery or follow an obsolete publication sequence.
- **Classification:** Behavior-preserving cleanup.
- **Problem references:**
  - `docs/agent-tasks/2026-08-11-phase3-completion.md:3-32,563-611`
  - `docs/agent-tasks/2026-08-11-phase3-completion-report.md:5-14,387-397`
  - `docs/agent-tasks/2026-08-15-phase4-completion-report.md:5-16,147-157`
  - `docs/agent-tasks/2026-09-12-ci-green-handoff.md:3-21,55-69,163-170`
- **Compared authority:**
  - `docs/superpowers/plans/2026-09-14-synchro-release-process.md:18-24,174-180,217-237,414-441`
  - `RELEASE.md:3-12,34-43,132-147`
  - `.r2-completion-tracker.md:1-9`
  - `.r3-completion-tracker.md:1-8`

The September 12 handoff calls one CI result the only active blocker.
It then directs closure of nine specific issues, a merge to `r2-integration`, and a tag.
The current procedure instead selects the exact `dev` candidate and requires Package, Publish, and Public gates.
The handoff also prohibits PostgreSQL on the Mac, while the current plan requires host-local PostgreSQL for Apple validation.

The earlier completion brief directs work against the former evidence framework and its removed release commands.
These files lack the historical-status notice already used by the trackers and older public plans.
Dates alone do not distinguish a record from a still-binding dispatch.

The same root cause affects local plans in the other assigned scope:

- `docs/superpowers/plans/2026-07-17-synchro-v0.3.0-verified-rc.md:13-38,666-717`
- `docs/superpowers/plans/2026-07-19-phase-3-independent-verification.md:13-26,1123-1242`
- `docs/superpowers/plans/phase-5-client-verification.md:9-33,55-66,164-173`

The local Phase 5 plan still requires one generic executor and a 140-obligation batch.
The superseding remediation plan explicitly deletes that execution model at lines 101-108 and 213-223.

**Smallest simplification:** Add a short historical-status notice and a current-procedure link to each obsolete task brief and handoff.
Remove their status as active instructions, not their failed evidence or unresolved findings.
Keep dated historical results unchanged.
Do not delete every report merely to reduce size.

**Invariant:** Current release eligibility must come from current, commit-bound executable evidence.
Historical issue references and failed attempts must remain traceable.

**Acceptance:** Run `make docs-build` after the documentation change.
Review every active entry point for one release procedure.
This static review cannot establish that historical commands passed or that the cited issues are closed.

**Related work:** Release-process plan Step 1, especially lines 225-237, and Step 6 at lines 418-441.

<a id="18-history-plans-f02"></a>

### 18-history-plans-F02: The original production audit does not link its later refutations

- **Severity:** Low. Readers must repeat diagnosis to reconcile incompatible remediation recommendations.
- **Classification:** Behavior-preserving cleanup.
- **Problem references:**
  - `docs/superpowers/plans/2026-09-08-production-codebase-audit.md:472-496,544-563,915-926`
- **Compared report:**
  - `docs/superpowers/plans/2026-09-08-audit-verification-server.md:5-10,12-42,109-140,387-401`

The original audit labels C1 and C4 high-confidence defects and includes both fixes among its highest-impact changes.
The later verification explicitly refutes both findings against unchanged investigated source.
It explains projected-state membership and global source-write exclusion.
The original audit provides no local disposition link at either recommendation or its final priority list.

This is a documentation conflict, not a new claim that current production code has either defect.
The generic historical notice only supersedes release operation.
It does not tell a reader which technical findings the later report rejected.

**Smallest simplification:** Add a short disposition note beside C1, C4, and their priority entries.
Link the verification report and identify the original recommendation as refuted by that review.
Do not silently rewrite the original evidence or certify the remaining findings.

**Invariant:** Preserve the original allegation, the contrary evidence, and the source revision for both.
Do not implement a protocol change from a historical allegation alone.

**Acceptance:** Statically verify the two disposition links and run `make docs-build`.
No production test is necessary for a report-link correction.
Any renewed production allegation needs current source and focused execution evidence.

**Related work:** Historical C1 and C4. This review did not independently reopen their production diagnosis.

<a id="18-history-plans-f03"></a>

### 18-history-plans-F03: A hand-maintained R3 category summary disagrees with its rows

- **Severity:** Low. The summary misstates six checks' migration destinations.
- **Classification:** Behavior-preserving cleanup.
- **Problem reference:** `docs/superpowers/plans/2026-09-07-r3-gap-resolution-reference.md:5-122`
- **Compared references:**
  - `docs/superpowers/plans/2026-09-07-r3-gap-resolution-modelrunner.md:3-19`
  - `.r3-completion-tracker.md:21-42`

The reference gap table contains 37 `family-extension` rows and 58 `uncovered-semantics` rows.
Its summary instead reports 31 and 64.
The modelrunner table contributes one family extension and 12 uncovered rows.
The resulting 38 and 70 match the tracker, not the reference table's summary.

Exact content searches confirmed the 37 and 58 row counts.
This is measured documentation drift, not an objection to the deletion inventory's size.

**Smallest simplification:** Delete the redundant manual category summary, or derive it from the disposition rows.
Do not add a new permanent reporting framework for this one historical table.

**Invariant:** Every check ID keeps its actual disposition and required surviving proof.
Counts never authorize semantic deletion.

**Acceptance:** Count both classifications from the rows and reconcile any retained summary.
Verify that all 114 reference-gap rows retain their IDs and dispositions.
No runtime test can repair this arithmetic inconsistency.

**Related work:** R3.1 and issue `#36`, the oracle-replacement work identified by the dispatch.
The current issue status was not queried.


<a id="area-19-product-docs"></a>

## Product documentation and requirements

<a id="19-product-docs-f01"></a>

### 19-product-docs-F01: Repeated normative definitions disagree on durable finality and error handling

- **Severity:** High. The documents permit incompatible recovery implementations at a data-integrity boundary.
- **Classification:** Contract decision.
- **Problem and comparison references:**
  - `docs/src/content/docs/spec/02-client-contract.mdx:691-728`
  - `docs/src/content/docs/spec/03-state-machines.mdx:259-285`
  - `docs/src/content/docs/architecture/decisions/003-pull-cursor-and-rebuild.mdx:679-693`
  - `docs/src/content/docs/spec/01-wire-protocol.mdx:1040-1075`
  - `docs/src/content/docs/spec/05-schema-evolution.mdx:733-767`
  - `docs/src/content/docs/spec/01-wire-protocol.mdx:672-684,1081-1093`

The client contract requires one transaction for final records, pruning, digest verification, cursor installation, and completion.
The state-machine document explicitly permits process death after final-page commit but before finality.
It requires recovery from the stored final result in that intermediate state.
Those are different durable transaction boundaries.

Two smaller disagreements show the same maintenance failure:

- ADR 003 says the adapter can supply a retry header for HTTP 503.
  The wire contract requires a valid `Retry-After` header on every HTTP 429 and 503 response.
- The schema document unconditionally requires the current schema for push and says historical references are valid only for connect.
  The wire contract requires completed push replay before current-schema and generation gates.
  A historical sealed request can therefore replay successfully under that rule.

The documents declare themselves normative, not illustrative alternatives.
The wire specification treats either an ADR or specification disagreement as implementation nonconformance at lines 10-20.
Implementers cannot satisfy both finality boundaries without an explicit interpretation.

The duplication also has a visible editing cost.
The complete lifecycle adjacency appears in ADR 005, the client contract, and the state-machine document:

- `docs/src/content/docs/architecture/decisions/005-integrity-authorization-and-seeds.mdx:267-294`
- `docs/src/content/docs/spec/02-client-contract.mdx:882-909`
- `docs/src/content/docs/spec/03-state-machines.mdx:44-68`

**Smallest simplification:** Resolve the finality boundary explicitly before changing code or tests.
Give each exact rule one normative definition and make other documents link to it.
Keep ADR rationale and state-machine explanations, but remove repeated transaction algorithms and transport tables that can drift.
State the completed-replay exception at the authoritative schema gate.
Do not choose a boundary by copying current implementation output.

**Invariant:** No cursor can certify incomplete or unverified materialization.
Process death must preserve exact replay, pending intent, and scope-local recovery.
Required retry headers and completed-response replay must have one deterministic meaning.

**Acceptance:** Run `make verify-contract` and `make docs-build` after reconciliation.
Review the final-page crash states, HTTP 503 header rule, and historical completed replay against one approved definition each.
Bind the selected rules to existing crash, retry, and replay proof homes.
Use `make test-swift`, `make test-kotlin`, and `make test-blackbox` for any resulting implementation change.
Documentation validators alone cannot prove semantic consistency.

**Related requirements:** `SYNC-REBUILD-004`, `SYNC-FAILURE-003`, `SYNC-IDEMPOTENCY-001`, and the durable cursor and rebuild invariants.
The historical audit's C9 is related evidence, not current implementation proof.

<a id="19-product-docs-f02"></a>

### 19-product-docs-F02: The client README describes a removed capture and queue model

- **Severity:** Medium. Consumers receive instructions that contradict durable intent and supported application SQL.
- **Classification:** Correctness defect in documentation.
- **Problem reference:** `clients/README.md:15-44`
- **Compared references:**
  - `docs/src/content/docs/spec/02-client-contract.mdx:145-175,190-234,277-281`
  - `docs/src/content/docs/clients/application-sql.mdx:8-36`
  - `docs/src/content/docs/clients/overview.mdx:16-39,47-52`

The README says the pending queue deduplicates with `ON CONFLICT DO UPDATE`.
It says push hydrates current local row data.
It also says ordinary native SQL needs no special write boundary.
The active contract instead captures immutable authored payloads and forbids rebuilding a retry from current row state.
The application SQL guide specifies guarded SDK statements and transactions, with explicit cross-platform conflict-clause restrictions.

The README links `clients/ARCHITECTURE.md`, which an exact file lookup did not find.
It also lists direct language test commands instead of supported Make targets.

**Smallest simplification:** Replace the obsolete change-detection section and missing architecture link with links to the current client guides.
Keep only the SDK overview and supported focused Make commands in this README.
Delete the second queue and capture explanation rather than rewriting another detailed copy.

**Invariant:** Local writes and captured intent commit together.
Sealed requests never derive their retry payload from later row contents.
React Native remains a bridge.

**Acceptance:** Verify the replacement links and run `make docs-build`.
An exact search must find no active instruction to hydrate a sealed retry from current rows.
No production code change is required for this documentation correction.

**Related requirements:** `SYNC-LOCALSQL-001`, `SYNC-QUEUE-001`, `SYNC-QUEUE-004`, and `SYNC-MUTATION-001`.

<a id="19-product-docs-f03"></a>

### 19-product-docs-F03: Catalog validation still requires one separate negative control for every requirement

- **Severity:** Medium. The policy forces redundant declarations and prevents risk-based proof selection.
- **Classification:** Contract decision.
- **Problem references:**
  - `docs/scripts/validators/catalogs.mjs:81-137`
  - `docs/scripts/validators/support-policy.mjs:134-154`
  - `docs/scripts/verify-contract.mjs:330-362`
- **Compared implementation and policy:**
  - `conformance/internal/contract/policy.go:250-286,311-325`
  - `docs/superpowers/plans/2026-09-14-synchro-release-process.md:39-44,119-156`
  - `docs/src/content/docs/spec/07-release-verification.mdx:12-22`

The JavaScript validator requires every control to own exactly one requirement.
It requires every requirement to have exactly one authored negative control.
The Go contract validator repeats that ownership constraint and requires exactly 111 invariant headings.
The support validator also requires native E2E whenever a requirement names any client component.

The current release plan explicitly rejects requiring every proof type or a mutant for every requirement.
It selects proof depth from the risk of an undetected failure.
The current evidence page likewise requires meaningful controls where silent failure presents material risk.

The older policy makes declaration cardinality a gate, even when a simpler proof arrangement preserves the actual behavior.
It also requires identical normative reference lists in both requirement and control records.
A shared defect mechanism cannot serve multiple requirements without separate control records.

**Smallest simplification:** Approve proof requirements from an explicit risk assessment using the existing requirement IDs.
Require each selected control to name the assertions it actually proves.
Allow shared control definitions only when their executed evidence proves each named requirement independently.
Delete universal singleton ownership and duplicate normative-reference lists when the existing requirement reference suffices.
Do not replace executable coverage with a count or a broad unqualified shared control.

**Invariant:** Every required behavior retains an executable proof for each independent implementation.
Critical durability, authorization, replay, and gate-integrity checks retain meaningful demonstrated failure controls.
Missing required work must still fail closed.

**Acceptance:** Review each affected requirement's retained proof before deleting any control.
Run `make test-conformance-contract`, `make test-conformance-scenarios`, and `make verify-contract` after an approved policy change.
The primary must run the affected real proof gates and negative controls.
This review did not prove that any specific current control is safe to delete without that mapping.

**Related work:** Release-process plan Step 3 and historical issue `#49` coverage work.
The old local plan explains the rule's origin at `docs/superpowers/plans/2026-07-19-phase-3-independent-verification.md:290,407-417`.
It is historical evidence, not current release authority.

<a id="19-product-docs-f04"></a>

### 19-product-docs-F04: Support and performance policy values have handwritten validator copies

- **Severity:** Medium. One approved policy change requires synchronized edits in several authority copies.
- **Classification:** Contract decision for removing frozen policy-value locks. Consolidating identical checks alone can preserve behavior.
- **Problem references:**
  - `docs/scripts/validators/support-policy.mjs:46-132`
  - `docs/scripts/validators/catalogs.mjs:161-218,231-240`
- **Compared implementation and authority:**
  - `conformance/internal/contract/policy.go:21-52,82-118`
  - `RELEASE.md:45-47`
  - `docs/src/content/docs/spec/06-conformance-plan.mdx:12-18`

The support validator recreates the complete expected support matrix and semantic cell list.
The Go validator contains the same tuples and list.
The performance validator contains a frozen catalog digest, 17 budget triples, and nine measurement IDs.
The Go validator repeats those values.

The authored matrix and performance catalog already contain the policy data these validators compare.
The historical tracker records the resulting maintenance cost during the Linux-only support change:
`.r2-completion-tracker.md:2721-2734` lists updates to both validator copies, the matrix, scenarios, and locked performance digests.

This is not necessary independent implementation of sync behavior.
It is repeated authoring of one policy input.
The current plan requires the matrix to remain the single machine-readable support declaration.

**Smallest simplification:** Approve the existing authored catalogs as the sole policy-value authority.
Validate schema, references, uniqueness, applicable components, and candidate snapshot identity against those inputs.
Delete handwritten copies of the same value sets and redundant digest locks.
Do not add another policy catalog or generator solely to preserve the duplication.
Any change to the actual supported set remains an explicit product decision.

**Invariant:** Candidate evidence must bind the exact reviewed policy and reject missing or extra required cells.
Budget assertions must still consume authored expected values, never production output.

**Acceptance:** Run `make test-conformance-contract`, `make check-conformance-catalog`, `make verify-contract`, and `make docs-build`.
Compare derived cell and budget obligations before and after consolidation.
Mutate a candidate's bound policy input and confirm rejection without accepting replacement evidence.
No such migration or acceptance run occurred during this review.

**Related work:** Release-process plan Step 1, lines 228-233.

<a id="19-product-docs-f05"></a>

### 19-product-docs-F05: Every docs page loads Mermaid even though no authored page uses it

- **Severity:** Low. The static site has an unnecessary browser dependency and external request.
- **Classification:** Behavior-preserving cleanup.
- **Problem reference:** `docs/astro.config.mjs:21-27`
- **Consumer scope:** All reviewed `.mdx`, `.mjs`, and `.ts` documentation sources.

The global head imports Mermaid from a major-version CDN URL and initializes it on every page.
The complete documentation review found no Mermaid diagram.
An exact `mermaid` search returned only the configuration line.
The state-machine and architecture documents use ordinary text blocks instead.

**Smallest simplification:** Delete the unused global head script entry.
This removes the unused network dependency without adding a build integration or replacement renderer.

**Invariant:** Existing authored documentation content and text diagrams remain available.

**Acceptance:** The source search must return no remaining Mermaid reference.
Run `make docs-build` and inspect the built output for the removed CDN import.
The review did not render the site or measure browser traffic.

**Related requirement:** No protocol or release requirement needs this script.

<a id="19-product-docs-f06"></a>

### 19-product-docs-F06: The Markdown anchor scanner accepts headings hidden inside a longer code fence

- **Severity:** Low. Contract references can resolve to text that the documentation parser does not render as a heading.
- **Classification:** Correctness defect.
- **Problem references:**
  - `docs/scripts/validators/markdown.mjs:31-61`
  - `docs/scripts/validator-self-test.mjs:62-81`
  - `docs/scripts/verify-contract.mjs:103-114`
- **Compared implementation:** `conformance/internal/contract/policy.go:375-424`

The scanner retains only the fence character, not its opening length.
Any later three-character fence with the same character closes the block.
The Go scanner has the same defect.
The self-test uses only matching three-character fences.

The static reproduction used this Markdown string:

```text
"````text\n```\n### Hidden contract\n````\n### Real contract\n"
```

`markdownAnchors` returned only `hidden-contract`.
The installed Markdown parser returned one code block and the actual heading `Real contract`.
The validator therefore accepts the hidden anchor and misses the real one.
No current requirement using this exact malformed binding was identified.

**Smallest simplification:** Track the opening fence length and accept only a valid matching closing fence.
Keep one shared regression input for both scanners if both remain necessary.
Do not build a new Markdown framework for this correction.

**Invariant:** A normative reference must resolve to an actual rendered heading, not a string inside a code block.

**Acceptance:** Add the reproduced four-backtick case and a tilde-fence counterpart to the parser's negative controls.
The result must contain `real-contract` and exclude `hidden-contract`.
Run `make verify-contract`, `make test-conformance-contract`, and `make docs-build`.
Only the pure parsing reproduction ran here.

**Related requirement:** Normative-reference integrity. This is not production sync execution evidence.

<a id="19-product-docs-f07"></a>

### 19-product-docs-F07: Current release documents disagree on npm latest promotion

- **Severity:** Low. The current plan repeats a publication instruction that differs from the active procedure.
- **Classification:** Behavior-preserving documentation cleanup.
- **Problem reference:** `RELEASE.md:132-147`
- **Compared plan:** `docs/superpowers/plans/2026-09-14-synchro-release-process.md:371-385`

The active procedure publishes npm directly under `latest`, then verifies public bytes, provenance, and React Native builds.
The current implementation plan requires a non-default npm tag until all public checks pass.
It promotes both GitHub and npm latest only after those checks.

This matches known open issue `#103` from the dispatch.
The source confirms the conflict.
This review did not query the issue or infer whether a later external decision approved either policy.

**Primary disposition:** `RELEASE.md` is the active procedure, as the repository instructions and existing issue `#103` state.
This documentation repair must not silently change that publication policy.

**Smallest simplification:** Replace the plan's competing instruction with a reference to the active procedure and its recorded policy decision.
Keep one operative instruction and all public identity checks.
Any reconsideration of npm promotion timing requires a separate explicit decision before implementation.

**Invariant:** Immutable payload identity, dependency order, protected approval, and failure recovery must remain enforced.
An incomplete public check must not become a completed-release claim.

**Acceptance:** Review the selected instruction against the publication implementation and its interruption controls.
Run `make test-release-publish` for any publication-policy implementation change.
Run `make docs-build` for the documentation correction.
No registry operation occurred during this review.

**Related issue:** `#103`, confirmed source match.

<a id="19-product-docs-f08"></a>

### 19-product-docs-F08: Portable seed export requires stronger snapshot isolation than its documented consistency argument establishes

- **Severity:** Medium. The requirement adds safe-snapshot waiting and rejects otherwise coherent snapshot exports.
- **Classification:** Contract decision.
- **Problem references:**
  - `docs/src/content/docs/architecture/decisions/005-integrity-authorization-and-seeds.mdx:402-427,572`
  - `docs/src/content/docs/spec/04-invariants.mdx:430-436`
  - `docs/src/content/docs/architecture/portable-seeds.mdx:16-25`
- **Compared snapshot contract:**
  - `docs/src/content/docs/spec/01-wire-protocol.mdx:813-822,934-944`
- **Bounded implementation evidence:**
  - `api/go/seeddb/seeddb.go:61`
  - `extensions/synchro-pg/src/portable_seed.rs:695-713`

Seed export requires one `SERIALIZABLE READ ONLY DEFERRABLE` transaction.
Every seed call rejects weaker transaction characteristics.
The ADR explicitly records that this can wait for a safe snapshot and holds the transaction for the full export.

The documented correctness argument depends on one snapshot of atomically committed, commit-ordered materialized projections and their progress row.
Pull and rebuild use a repeatable-read snapshot of that same authority.
The seed contract does not identify an additional serialization anomaly that requires safe-snapshot waiting for this read-only export.

**Smallest coherent alternative:** Evaluate one `REPEATABLE READ READ ONLY` export transaction over the same materialized projections.
Keep the same connection, export identity, transaction-bound page tokens, and fixed boundary.
Retain all digest, cardinality, provenance, receipt, and SQLite publication checks.
This removes mandatory deferrable waiting and its extra transaction-characteristic requirement.

This proposal does not apply to reset or projection-bootstrap exported snapshots.
Those snapshots must remain causally bound to their permanent logical slots.

**Invariant:** Every exported byte and continuation receipt must describe one consistent materialized boundary.
Concurrent source writes, schema activation, and compaction must not mix states or invalidate the exported continuation claim.

**Acceptance:** First approve or reject the isolation change as a contract decision.
Then test concurrent source commits, materialization, schema publication, and compaction during a multipage export.
Run `make test-adapter GO_TEST_PKGS=./seeddb` and the applicable portable-seed proofs through `make test-rust-pg`.
Retain cross-connection and post-transaction token rejection controls.
The review measured no wait duration and did not execute a lower-isolation export.

**Related requirement:** `SYNC-SEED-004` and ADR 005 snapshot consistency.

<a id="19-product-docs-f09"></a>

### 19-product-docs-F09: Worker isolation forbids unrelated non-superuser replication logins across the cluster

- **Severity:** Medium. An unrelated replication principal makes the configured worker invalid even when no Synchro role or object grant changes.
- **Classification:** Contract decision.
- **Problem references:**
  - `docs/src/content/docs/architecture/decisions/005-integrity-authorization-and-seeds.mdx:326-343`
  - `docs/src/content/docs/spec/04-invariants.mdx:474-476`
- **Compared product boundary:** `docs/src/content/docs/spec/00-principles.mdx:209-226`
- **Bounded implementation evidence:**
  - `extensions/synchro-pg/src/health.rs:11-75,484-517`

The ADR states that no other login principal has `REPLICATION`.
The health query checks every non-superuser login in `pg_roles`, not only members of Synchro runtime groups.
An additional login with `rolcanlogin = true` and `rolreplication = true` makes `sole_replication_principal` false.
`WorkerLoginValidation.is_valid` requires that value to be true.

The predicate contains no database, Synchro membership, or intended-use condition for that other login.
It excludes superusers, so it is not a complete prohibition on other cluster replication authority.
The requirement expands Synchro's deployment boundary beyond its own runtime principals and grants.

**Smallest coherent alternative:** Define the trusted deployment-principal boundary explicitly.
Keep the dedicated worker attributes, exact worker-group membership, restricted credentials, and HBA checks.
Forbid replication authority on Synchro adapter, seed, monitor, operator, and application-facing principals.
Permit separately trusted cluster replication principals only under the approved deployment policy.
That decision can remove the unqualified cluster-wide uniqueness check.

This is a security decision, not permission to weaken runtime role isolation.
The review does not claim that all extra replication logins are safe.

**Invariant:** No application-facing principal can acquire worker, owner, or identity-bearing authority through an unintended grant.
Only the configured worker may serve the designated Synchro worker role.

**Acceptance:** Record the trust model before changing the predicate.
Test an unrelated allowed principal and each forbidden Synchro role escalation.
Run the applicable authorization and health tests through `make test-rust-pg` and `make test-blackbox`.
This finding follows from the static predicate. No deployed customer failure was observed.

**Related requirement:** `SYNC-DBAUTH-002`.

<a id="19-product-docs-f10"></a>

### 19-product-docs-F10: The mandatory schema rollout prescribes dual representations for every semantic transition

- **Severity:** Medium. The requirement can impose extra fields, dual writes, and backfill where no representation change needs them.
- **Classification:** Contract decision.
- **Problem reference:** `docs/src/content/docs/spec/05-schema-evolution.mdx:919-936`
- **Compared requirements:**
  - `docs/src/content/docs/spec/05-schema-evolution.mdx:494-559,902-917,938-972`
  - `docs/superpowers/plans/2026-09-14-synchro-release-process.md:182-210`

The schema document requires every destructive or semantic transition to expand with new IDs, dual-read, dual-write, backfill, and then contract.
The same document defines membership changes that do not mint a manifest.
It also defines composition changes that use the existing staged generation and scoped rebuild path.
Those changes do not inherently need a second application field or table representation.

The current release plan instead requires an explicit compatibility window and data-preserving procedure for a breaking minor release.
It does not require one deployment choreography for every semantic change.

**Smallest coherent alternative:** Require expand/migrate/contract when the declared compatibility window needs simultaneous old and new representations.
Use the existing staged-generation path for metadata and membership changes.
Permit a separately approved maintenance-window migration when it preserves required application data and queued intent.
Remove mandatory dual-read and dual-write steps from transitions that have no second representation.

**Invariant:** Old clients must never silently reinterpret changed identities or lose queued intent.
Class 3 activation remains atomic and scope-local.
Class 4 remains explicit, with `unsupported`, preserved intent, and an approved recovery path.

**Acceptance:** Classify representative membership-only, composition, additive-field, rename, and key-change transitions under the revised rule.
For each case, identify the compatibility promise and required executable proof.
Run `make verify-contract` and `make docs-build` after the approved wording change.
Any changed migration implementation requires its existing server and native migration gates.
This review did not approve a new migration behavior.

**Related requirements:** `SYNC-SCHEMA-004`, `SYNC-SCHEMA-005`, `SYNC-SCHEMA-006`, and release upgrade policy.

<a id="19-product-docs-f11"></a>

### 19-product-docs-F11: A Markdown helper exists only to test itself

- **Severity:** Low. The helper adds an unused API and a test that proves no production validator behavior.
- **Classification:** Behavior-preserving cleanup.
- **Problem references:**
  - `docs/scripts/validators/markdown.mjs:68-74`
  - `docs/scripts/validator-self-test.mjs:11-14,78-81`
- **Actual validator consumer:** `docs/scripts/verify-contract.mjs:19,103-114`

`markdownAnchorsAtLevel` has no production caller.
An exact repository source search found only its definition, self-test import, and self-test assertion.
The actual validator calls `markdownAnchors` without level filtering.

**Smallest simplification:** Delete the unused export, its import, and its self-test-only assertion.
Do not wire an unnecessary caller solely to keep the helper.
Retain the actual anchor parser and its meaningful regression controls.

**Invariant:** Normative references must still resolve through the active anchor validation path.

**Acceptance:** A source search must return zero references after deletion.
Run `make verify-contract` and `make docs-build`.
The search ran during this review. The Make commands did not.

**Related work:** Verification simplicity. No confirmed existing issue match was found in the supplied issue leads.


<a id="area-20-local-authored"></a>

## Local instructions and authored files

<a id="20-local-authored-f01"></a>

### 20-local-authored-F01: Current instructions direct workers to a removed release target

- **Severity:** Low. The documented command cannot run and can cause unnecessary target restoration work.
- **Classification:** Correctness defect in instructions.
- **Problem references:** `AGENTS.md:204-249` and `CLAUDE.md:204-249`, especially lines 243-249.
- **Compared references:**
  - `RELEASE.md:3-12,34-43,87-128`
  - `docs/superpowers/plans/2026-09-14-synchro-release-process.md:414-441`
  - `Makefile:764-790`

The instructions label `make release-check` as full release validation.
They also direct a worker to fix a missing required Make target before considering another command.
The current Makefile defines the new release staging and verification entry points, but no `release-check` target.

The static command `make -n release-check` failed with:

```text
make: *** No rule to make target `release-check'.  Stop.
```

This failure does not justify restoring the removed aggregate.
The current release procedure deliberately uses exact-commit Candidate CI and sealed Package verification instead of repeated source execution.

**Smallest simplification:** Remove the obsolete full-release command from the local instructions.
Point workers to `RELEASE.md` for release operation and retain valid focused Make targets.
Do not add an alias that reconstructs the old release system.

**Invariant:** A focused local test result must not become a complete release claim.
Publication must still require the current procedure's protected approval and exact candidate evidence.

**Acceptance:** Check each retained documented Make command against the current Makefile.
Confirm that release instructions resolve to `RELEASE.md` rather than a removed aggregate.
The dry-run failure above is static evidence only. No release gate executed.

**Related work:** Release-process plan Step 6, particularly lines 418-425.

### Instruction Decisions For The Primary

Two wording changes merit consideration, but this review does not approve a new worker policy.

1. Qualify the one-proof-home rule by independent implementation and behavior.
   `AGENTS.md:352-386` lacks the qualification stated in the current release plan at lines 39-42.
   Swift and Kotlin must each prove their independent implementation against shared expected inputs.
   A global one-home reading could remove necessary cross-engine proof.

2. Limit mandatory one-consumer inlining to unnecessary layers and pass-through abstractions.
   `AGENTS.md:382-383` states the rule without that distinction.
   `AGENTS.md:18-25` also requires clear control flow and small cohesive units.
   A named parsing step can have one caller without creating duplicate authority or a needless layer.
   For example, `docs/scripts/validators/strict-json.mjs:39-45,54-66` separates number scanning from value dispatch.
   Consumer count alone does not establish that inlining reduces maintenance cost.

Keep these as instruction-scope decisions.
Neither decision permits weaker durability tests or implementation-derived expected values.
