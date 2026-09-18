# Synchro: Curated Audit Findings

**Scope:** Production source at `98b1507537eaa708eab7cf14e3bf8eddc5eb9b7e`, unchanged by the subsequent report commits.
**Tracking:** #105, with existing issue links below. No issue per finding.
**Remediation:** All 53 findings are implemented and accepted. See the [acceptance results and approved criteria](2026-09-17-curated-audit-remediation-plan.md#acceptance-results).

This replaces the previous 270-record index and its verbose appendix.
It excludes redesign proposals, historical notes, unsupported operational claims, and duplicate reports of the same problem.
Each retained item identifies a concrete code defect, ineffective test, or removable implementation duplication.
Source confirmation does not mean that every runtime failure has been reproduced.

Synchro's PostgreSQL authority, thin HTTP adapter, native SQLite engines, and React Native bridge remain sound component boundaries.
The corrections below preserve those boundaries, exact replay, atomic capture, and independent expected values.

## 1. Runtime correctness

### R01. Seed generation rejects valid int64 values

The integrity validator requires a decimal string for `int64`, but the SQLite writer requires `json.Number`.
Parse the validated string directly into `int64`.

**Evidence:** `api/go/seeddb/integrity.go:706-717`; `api/go/seeddb/seeddb.go:1907-1912`.

### R02. Primary-key-only tables produce incorrect upsert SQL

The Go seed writer emits a plain INSERT when there are no update columns.
Swift pull emits an empty `DO UPDATE SET` clause.
Use `DO NOTHING` for that case in both writers.

**Evidence:** `api/go/seeddb/seeddb.go:1824-1862`; `clients/swift/Sources/Synchro/PullProcessor.swift:1010-1023`.

### R03. Nullable byte fields fail push conversion

Portable-value validation accepts null for nullable fields.
The PostgreSQL bytes converter then requires a string and raises an error.
Handle null before type-specific conversion.

**Evidence:** `extensions/synchro-core/src/checksum.rs:560-572`; `extensions/synchro-pg/src/push.rs:2258-2295`.

### R04. Swift capture changes invalid values instead of preserving or rejecting them

The capture trigger casts integer fields before recording intent.
SQLite converts text such as `not-an-integer` to integer zero.
Reject incompatible storage types inside the capture transaction instead of laundering them through casts.

**Evidence:** `clients/swift/Sources/Synchro/SQLiteSchema.swift:185-214`.

### R05. Kotlin authored context can suppress another table's capture

An authored transaction installs context for one table.
`withDefaultCaptureContext` accepts any existing context without checking its target.
A different synced table's update then fails its trigger's context predicate and records no mutation.
Reject the mismatch before executing SQL.

**Evidence:** `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroDatabase.kt:873-907,968-1001`; `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SQLiteSchema.kt:166-206`.

### R06. Kotlin transaction handles remain usable after their callback returns

The callback can return its transaction object.
That object retains a live SQLite handle without an active-callback or owning-thread check.
Invalidate it when the callback ends and check ownership before each operation.

**Evidence:** `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroDatabase.kt:848-870,1261-1329`.

### R07. Swift nullable relaxation leaves the old NOT NULL constraint

The migration planner accepts a non-nullable field becoming nullable.
It adds missing columns but never changes the existing constraint.
Implement the constraint change through the existing journal, including atomic table replacement where necessary.

**Evidence:** `clients/swift/Sources/Synchro/SchemaMigrationJournal.swift:88-145`; `clients/swift/Sources/Synchro/SchemaManager.swift:394-456,511-535`.

### R08. Swift connect retries skip normal response validation

Normal connect validates protocol and assignment semantics against its request.
The durable retry path sends and installs the response without that validation.
Use the same response-validation path for both.

**Evidence:** `clients/swift/Sources/Synchro/SyncEngine.swift:920-944,1274-1311`; `clients/swift/Sources/Synchro/HttpClient.swift:160-170`.

### R09. Rust decoding accepts omitted required nullable members

Plain `Option<T>` fields accept omission as `None`.
Thus `{}` is accepted as a scope cursor object even though `cursor` is required.
Use required-nullable decoding rather than an additional post-parse presence map.

**Evidence:** `extensions/synchro-core/src/contract.rs:280-303,397-403,430-439`.

### R10. The iOS schema bridge traps on malformed JavaScript input

`parseColumns` and `parseTableOptions` force-cast decoded JSON.
A missing column name or wrong container type causes a trap instead of a rejected promise.
Use checked decoding and return the existing boundary error.

**Evidence:** `clients/react-native/ios/SynchroModule.swift:1977-1997`.

### R11. Kotlin DDL normalization changes quoted values

`canonicalDDL` collapses whitespace inside literals and identifiers.
It therefore treats `'a  b'` and `'a b'` as equal despite different trigger behavior.
Preserve quoted token contents when comparing generated DDL.

**Evidence:** `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SQLiteSchema.kt:259-273`; `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroDatabase.kt:1364-1405`.

### R12. Native configuration accepts values that disable or break work

Swift silently clamps page size and accepts invalid timer values.
Kotlin accepts a negative retry count, which skips its sync loop without an error.
Reject invalid limits and non-finite timers before database or lifecycle work.

**Evidence:** `clients/swift/Sources/Synchro/SynchroConfig.swift:70-82`; `clients/swift/Sources/Synchro/SyncEngine.swift:831-836,1580-1587`; `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroConfig.kt:7-29`; `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SyncEngine.kt:615-665`.

### R13. Swift watch has a read-before-subscribe gap

The initial read and callback finish before the observer is registered.
A write in that interval produces no notification for the new observer.
Coordinate initial snapshot acquisition with subscription.

**Evidence:** `clients/swift/Sources/Synchro/Database.swift:272-305`; `clients/swift/Sources/Synchro/SynchroClient.swift:129-134`.

### R14. Tagged React Native query parameters defeat stabilization

`useStableArray` compares tag objects by identity.
Recreating an equivalent int64 or bytes parameter causes another query or subscription after rendering.
Compare supported tags by type and payload, without adding a general deep-equality library.

**Evidence:** `clients/react-native/src/hooks/useQuery.ts:13-46,53-112`; `clients/react-native/src/types.ts:3-17`.

### R15. The React Native status hook does not load current state

The hook starts at `uninitialized` and listens only for future events.
It also retains the previous client's state when the client changes.
Read current status on subscription and prevent stale asynchronous results from replacing newer state.

**Evidence:** `clients/react-native/src/hooks/useSyncStatus.ts:5-16`; `clients/react-native/src/SynchroClient.ts:1012-1025,1128-1134`.

### R16. Seed verification splits quoted trigger names at spaces

The generator quotes complete trigger names, but verification extracts them with `strings.Fields`.
A valid table name such as `order items` therefore produces a mismatched expected trigger name.
Derive the name directly from the known table name and trigger prefix.

**Evidence:** `api/go/seeddb/seeddb.go:1008-1014,1074-1080,2023-2035`.

## 2. Verification correctness

### V01. Test builds bypass production membership validation

Three `pg_test` exceptions permit legacy membership functions that the shipped validator rejects.
Replace those fixtures with production-valid projection functions, then remove the exceptions.
This is one finding covering both the validator and its test consumers.

**Evidence:** `extensions/synchro-pg/src/registry.rs:1441-1451,1539-1542,4024-4027`; `extensions/synchro-pg/src/lib.rs:2331-2505`.

### V02. Native observation substitutes an expected checksum

The observer reads the runtime checksum but checks only its length.
It emits `record.Image.Checksum` instead.
Verify the actual digest against observed data before mapping runtime identities to authored facts.

**Evidence:** `conformance/blackbox/native_controller.go:3680-3735`.

### V03. Native LSN comparison parses PostgreSQL positions incorrectly

The parser concatenates hexadecimal words, so `1/0` becomes 16 instead of 4,294,967,296.
Parse the two 32-bit words and reject malformed positions instead of using lexical fallback.

**Evidence:** `conformance/blackbox/native_controller.go:4152-4174`; correct comparison at `conformance/invariants/cursor_monotonicity.go:484-497`.

### V04. Soak combines bytes from different HTTP exchanges

The recorded pull combines the acknowledgment request with an earlier data response.
The framework also assigns that combined sequence to issuance and acknowledgment.
Preserve each original exchange and give the two events separate bindings.

**Evidence:** `conformance/blackbox/integration/soak_harness_test.go:405-444,989-1045`; `conformance/soak/capture.go:375-423`.

### V05. Soak faults do not execute their declared operation

The fault dispatcher always sends connect, then executes the selected operation through the healthy client.
Unknown fault operators become HTTP 503.
Inject supported faults into their actual operation and reject unsupported recipes.

**Evidence:** `conformance/blackbox/integration/soak_harness_test.go:1320-1378`; `conformance/soak/generator.go:144-196`.

### V06. Server-only soak reports a synthetic client restart

The process-death operation restarts the WAL worker.
The harness separately changes an invented client process identity and reports a client restart boundary.
Remove that attribution and retain the actual server recovery evidence.

**Evidence:** `conformance/blackbox/integration/soak_harness_test.go:368-372,483-557,1308-1317`.

### V07. The parity test supplies its own platform results

The test assigns the expected bytes to PostgreSQL, Swift, Kotlin, and React Native result entries.
Its predicate is test-only.
Remove the fictitious platform comparison and bind parity claims to actual vector consumers.

**Evidence:** `conformance/invariants/issue49_integrity_test.go:14-30`; `conformance/invariants/issue49_integrity_predicates_test.go:62-91`.

### V08. Exact test-result parsing accepts skipped descendants

The parser records a descendant skip but checks skips only on the selected parent and assertion.
A selected assertion can therefore pass with a skipped required child.
Reject skipped descendants before classifying the result.

**Evidence:** `conformance/cmd/testresult/parser.go:299-393`.

### V09. Scenario cloning removes explicit empty-state requirements

`omitempty` drops both nil and empty fact slices during JSON cloning.
The comparator treats nil as unrestricted but empty as requiring no records.
Use nil-only omission so cloning preserves the assertion.

**Evidence:** `conformance/scenarios/types.go:192-212,293-307`; `conformance/scenarios/load.go:44-57`; `conformance/scenarios/state_facts.go:319-327`.

### V10. The mutation-count test also violates mutation-ID uniqueness

The 1,001-entry request repeats one mutation ID.
Removing the count limit still leaves another valid rejection reason.
Use unique, otherwise valid mutations for the count boundary.

**Evidence:** `extensions/synchro-pg/src/pg_tests/conflicts.rs:887-925,951-963`; `extensions/synchro-core/src/contract.rs:825-841`.

### V11. An unknown-tag test uses an already poisoned decoder

The first invalid tag poisons the decoder.
The second tag fails at the poison guard without reaching tag dispatch.
Use a fresh decoder for each independent tag case.

**Evidence:** `extensions/synchro-pg/src/wal_decoder.rs:174-186,1118-1143`.

### V12. Invalid-vector tests can fail before calling production validation

Swift and Kotlin test-only parsers reject malformed row JSON before the digest implementation receives it.
The invalid-vector loop counts that setup error as success.
Route raw-input cases through the production boundary and delete the duplicate test parsers.

**Evidence:** `clients/swift/Tests/SynchroTests/IntegrityTests.swift:198-234,359-368,416-501`; `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/IntegrityTests.kt:197-236,392-408,443-516`.

### V13. The transaction-timeout test has no write to roll back

The action opens an empty transaction and waits for a timeout.
It cannot detect a timeout path that commits instead of rolling back.
Write a sentinel, require its absence afterward, and verify that another transaction succeeds.

**Evidence:** `clients/react-native/example/src/App.tsx:506-520`; `clients/react-native/example/e2e/sync.test.ts:164-166`.

### V14. A checkpoint assertion checks map size instead of positions

Advancing existing checkpoints leaves the map length unchanged.
Compare the checkpoint positions before acknowledgment, using the existing comparison helper.

**Evidence:** `conformance/blackbox/integration/real_pull_rebuild_test.go:316-333,702-713`.

### V15. Python test gates accept empty or entirely skipped suites

The Make targets rely on ordinary `unittest` exit status.
That runner reports success for an empty suite and for skipped tests.
Add execution-count and skip checks to the existing entry points.

**Evidence:** `Makefile:867-871,1829-1833`; `scripts/ci/test_release_artifacts.py:509-510`; `scripts/ci/test_release_publish.py:428-429`; `verification/test_packaged_smoke.py:542-543`.

## 3. Concrete duplication and dead code

### C01. Detox journeys repeat the coordinator protocol

The journeys independently implement configuration, envelope decoding, HTTP exchange, and polling.
Their error-detail rules already differ.
Move those shared operations into the existing harness, keeping scenario steps and restart boundaries explicit.

**Evidence:** `clients/react-native/example/e2e/corpus-harness.ts:5-33`; `clients/react-native/example/e2e/pending-cycle.test.ts:5-142`; `clients/react-native/example/e2e/queue-replay.test.ts:5-111`; `clients/react-native/example/e2e/schema-check.test.ts:5-153`. **Existing issue:** #93.

### C02. Three Go drivers implement the same queue workload builder

Swift, Kotlin, and React Native drivers expand the same authored workload and schema transition separately.
Share that input construction in the existing conformance layer.
Keep native process and transport differences in their drivers.

**Evidence:** `conformance/swift/queue_replay.go:822-983`; `conformance/kotlin/queue_replay.go:875-1022`; `conformance/reactnative/queue_replay.go:1992-2147`.

### C03. Kotlin maintains two copies of the lifecycle graph

The engine and metadata layer independently define the same permitted transitions.
Share the transition rule while retaining both in-memory and durable-state checks.

**Evidence:** `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SyncEngine.kt:1604-1677`; `clients/kotlin/synchro/src/main/kotlin/com/trainstar/synchro/SynchroMeta.kt:331-412`.

### C04. HTTP handlers discard a decoded envelope and decode it again

The body helper returns parsed members, but each handler discards them and unmarshals the whole body again.
Read the required identity fields from that existing result.
Preserve original request bytes for the extension.

**Evidence:** `api/go/handlers.go:22-210,276-311`.

### C05. Unused tooling remains declared

Remove the unused `pretty_assertions` dependency, uninvoked Turbo configuration, and Mermaid import with no authored diagram.
Regenerate affected lockfiles normally. Do not manually prune transitive packages.

**Evidence:** `extensions/synchro-core/Cargo.toml:15-16`; `clients/react-native/package.json:85`; `clients/react-native/turbo.json:1-43`; `docs/astro.config.mjs:21-27`.

### C06. Native scenario loaders have only their own tests as consumers

Neither SDK runs a semantic scenario through these loaders.
Delete the loader and self-test pairs instead of adding another runner to justify them.

**Evidence:** `clients/swift/Tests/SynchroTests/ScenarioFixtureLoader.swift:1-70`; `clients/swift/Tests/SynchroTests/ScenarioFixtureLoaderTests.swift:1-15`; `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/ScenarioFixtureLoader.kt:1-59`; `clients/kotlin/synchro/src/test/kotlin/com/trainstar/synchro/ScenarioFixtureLoaderTests.kt:1-28`.

### C07. Seed integrity uses handwritten insertion sorts

Three sorting helpers duplicate standard-library operations.
Replace the swap loops with the existing comparators and standard sort.
Preserve canonical ordering and duplicate rejection.

**Evidence:** `api/go/seeddb/integrity.go:1058-1064,1205-1219`; existing standard sort use at `445-471`.

### C08. Vector catalogs retain an unused full source copy

`VectorSet.sourceBytes` is written and defensively copied but never read.
Delete that field and its copies. Keep the separate source-integrity capture.

**Evidence:** `conformance/vectors/types.go:155-163,186-202`; `conformance/vectors/load.go:104-143,221-225`.

### C09. Vector loaders prove ID uniqueness twice

Strictly increasing ID checks already reject duplicates.
Delete the redundant ID maps, but retain the separate path-uniqueness check.

**Evidence:** `conformance/vectors/load.go:147-172,198-213`.

### C10. Packaged verification retains an unused probe and scanner

The RN consumer does not use `artifactSmoke.ts`.
No active gate invokes the public-source scanner.
Delete both and their self-only tests while retaining actual consumer builds and artifact checks.

**Evidence:** `verification/consumers/react-native/artifactSmoke.ts:1-11`; `verification/packaged_smoke.py:37-49,132-151,1051-1052`; `verification/test_packaged_smoke.py:528-539`. **Existing issue:** #102.

### C11. Capture code retains error handling that does nothing

An error observer discards its only captured value.
Another error mapper returns every error unchanged.
Delete these wrappers and the unused local variable.

**Evidence:** `extensions/synchro-pg/src/bgworker.rs:2078-2080,3156-3160`.

## 4. Tooling and active documentation

### T01. Version synchronization omits required release catalogs

`Sync` and `Check` duplicate a target list that omits the requirements and support-matrix release fields.
The support validator requires both to equal `VERSION`.
Use one complete target inventory, keeping distribution and protocol versions distinct.

**Evidence:** `api/go/internal/releaseversion/releaseversion.go:92-153,176-237`; `docs/scripts/validators/support-policy.mjs:29-33`; `conformance/schemas/requirements-v2.schema.json:11`. **Existing issue:** #100.

### T02. Release version validation accepts leading zeros

The release regex accepts `01.2.3`, while the runtime SemVer parser rejects it.
Use canonical numeric components in the release validator.

**Evidence:** `api/go/internal/releaseversion/releaseversion.go:12,56-60`; `api/go/version.go:61-65`.

### T03. Version commands reject linked Git worktrees

Root discovery requires `.git` to be a directory.
Linked worktrees use a `.git` file.
Recognize the supported file form or use Git's root discovery.

**Evidence:** `api/go/internal/releaseversion/releaseversion.go:36-54`.

### T04. Adapter lifecycle commands terminate unrelated port owners

Start and stop signal listeners merely because they occupy the configured port.
Other failure paths discard the PID file without stopping the child.
Keep one lifecycle owner and terminate only the process it started.

**Evidence:** `Makefile:2005-2065,2067-2094`; `scripts/ci/start-adapter.sh:23-58`. **Existing issue:** #98.

### T05. The Android library searches local Maven by default

The shipped build file prefers machine-local artifacts over Maven Central.
Keep local resolution explicit in development configuration, not the installed library's default.

**Evidence:** `clients/react-native/android/build.gradle:17-21,76-85`; `clients/react-native/example/android/build.gradle:25-30`. **Existing issue:** #101.

### T06. Isolated Candidate installations share a global lock

The fixed concurrency group serializes jobs on separate GitHub-hosted runners.
Remove that installation lock while retaining fixture-local locks and publication serialization.

**Evidence:** `.github/workflows/ci.yml:133-145,167-190`. **Existing issue:** #94.

### T07. Active client documentation describes retired behavior

The client README describes mutable queue hydration, and the Kotlin README describes a planned Room wrapper.
Both direct readers toward a missing architecture document.
Replace those descriptions with links to the maintained client guides.

**Evidence:** `clients/README.md:15-44`; `clients/kotlin/README.md:1-7`.

### T08. Active local instructions name a removed release target

`make release-check` does not exist in the current Makefile.
Remove that instruction and use `RELEASE.md` for release operation.
Do not restore the obsolete aggregate.

**Evidence:** `AGENTS.md:243-249`; `RELEASE.md:87-128`.

### T09. The release plan repeats a conflicting npm instruction

The plan postpones npm `latest`; the active procedure publishes directly under it.
Replace the competing plan instruction with a reference to the active procedure.
Do not change publication behavior as a documentation fix.

**Evidence:** `docs/superpowers/plans/2026-09-14-synchro-release-process.md:371-385`; `RELEASE.md:132-147`. **Existing issue:** #103.

### T10. Markdown anchor validation closes fences too early

The scanners remember the fence character but not its opening length.
A three-backtick line closes a four-backtick block and turns hidden text into a supposed heading.
Track fence length and validate actual closing fences.

**Evidence:** `docs/scripts/validators/markdown.mjs:31-61`; `conformance/internal/contract/policy.go:375-424`.

### T11. Test identifier truncation exceeds PostgreSQL's name limit

The long-prefix branch budgets one separator, but the final name contains two.
It produces 64 bytes instead of the intended maximum of 63.
Include both separators in the length calculation.

**Evidence:** `api/go/internal/testsupport/postgres.go:63-75`; `api/go/internal/testsupport/postgres_test.go:9-29`.

---

[Original source coverage](2026-09-17-simplicity-review-coverage.csv) remains available separately.
The prior expanded report remains in Git history, not as the current implementation list.
