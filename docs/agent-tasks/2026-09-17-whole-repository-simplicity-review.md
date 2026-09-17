# Whole-Repository Simplicity Review

Date: 2026-09-17

## Assessment

**The product boundary is coherent. The codebase is not yet consistently simple or clean.**

The main problem is not the existence of synchronization state.
The main problem is repeated authority over that state, obsolete execution paths, and verification code that sometimes replaces observation with prediction.

Some defects also affect real data, schema transitions, cancellation, and artifact verification.
Removing code without repairing those boundaries would produce a smaller but less reliable system.

The best simplification is deletion and consolidation within existing component boundaries.
It is not a new framework, a shared cross-platform sync engine, or a renamed protocol.

### Highest-priority findings

These are selected findings, not the complete finding inventory.
They have static source evidence unless their detailed entry identifies an executed reproduction.

| Priority | Problem | Evidence |
| --- | --- | --- |
| Data integrity | Online projection activation can replace a newer authoritative row version with an older candidate version. | `03-postgres-capture-F01` |
| Data integrity | Valid local writes can escape capture through unsupported indirect writes or mismatched authored context. | `06-swift-F03`, `06-swift-F04`, `07-kotlin-F02` |
| Recovery | Connect can persist generation renewal before rejecting the request that must receive the new generation. | `04-postgres-api-F01` |
| Schema correctness | Alternate migration implementations hide missing live-path digest conversion and physical constraint updates. | `06-swift-F01`, `06-swift-F02`, `07-kotlin-F03` |
| Lifecycle correctness | Swift startup cancellation can strand its completion gate. Kotlin automatic failure can leave retry unable to restart. | `06-swift-F05`, `07-kotlin-F04` |
| Evidence integrity | Native observers and soak code manufacture state, process identities, fault activation, or exchanges that did not occur. | `09-blackbox-runtime-F01`, `09-blackbox-runtime-F03`, `10-blackbox-tests-F01` through `F03`, `16-verification-framework-F01` and `F02` |
| Evidence integrity | Test-only protocol implementations and production validation exemptions can make the wrong subject pass. | `04-postgres-api-F09`, `05-postgres-tests-F01`, `16-verification-framework-F03` |
| Package evidence | Client package smoke uses queue counts as transfer proof and does not verify every consumed staged copy. | `17-build-release-F01`, `17-build-release-F02` |
| Gate integrity | The exact test-result parser can accept skipped assertion descendants. | `16-verification-framework-F04` |
| Contract integrity | Normative documents disagree about final-page transaction boundaries and repeat rules that have diverged. | `19-product-docs-F01` |

### Clear simplification targets

| Target | Smallest useful change | Finding examples |
| --- | --- | --- |
| Test-only production paths | Move tests onto the real registration, reconciliation, and migration paths. Delete their compatibility bypasses. | `04-postgres-api-F09`, `06-swift-F08`, `06-swift-F20`, `07-kotlin-F03` |
| Duplicate verification engines | Keep independent expected values, but replace fabricated observations and disconnected checkers with real observed facts. Follow the approved R3 migration boundary. | `09-blackbox-runtime-F11`, `14-reference-model-F01`, `14-reference-model-F10`, `16-verification-framework-F03` |
| Same-language driver duplication | Share Go workload construction, transport mechanics, and identical predicates. Keep native platform ownership explicit. | `11-swift-conformance-F01`, `13-react-native-conformance-F05`, `13-react-native-conformance-F06` |
| Repeated parsing | Keep one strict syntax owner and explicit boundary-specific type rules. Delete duplicate visitors and decode-and-repair fallbacks. | `02-rust-core-F02`, `09-blackbox-runtime-F07`, `16-verification-framework-F06` |
| Repeated database work | Load complete bounded sets once. Reuse validated schema state within an operation. | `04-postgres-api-F11` through `F14`, `06-swift-F10`, `06-swift-F11`, `07-kotlin-F09` |
| Handwritten standard operations | Use the standard sort, direct concrete calls, and existing typed helpers where their semantics match. | `01-adapter-F04`, `02-rust-core-F09`, `03-postgres-capture-F14`, `14-reference-model-F08` |
| Dead internal code | Delete unused helpers and tests that exercise only those helpers. Check public compatibility separately. | `02-rust-core-F06` through `F11`, `06-swift-F09`, `06-swift-F21`, `07-kotlin-F15`, `17-build-release-F08` |
| Duplicate lifecycle ownership | Give each operation one owner for startup, cancellation, completion, and cleanup. | `01-adapter-F05`, `06-swift-F05`, `07-kotlin-F04`, `17-build-release-F04` |
| Brittle proof substitutes | Replace counts, syntax searches, compound invalid fixtures, and expected-value self-comparisons with the actual required outcome. | `05-postgres-tests-F02` through `F10`, `10-blackbox-tests-F06` through `F08`, `17-build-release-F16` |
| Repeated policy authoring | Bind reviewed catalogs and one normative definition instead of copying their values into several validators. | `16-verification-framework-F07` through `F09`, `19-product-docs-F01`, `19-product-docs-F03`, `19-product-docs-F04` |

These are not approved edits.
Each detailed finding states the behavior that must remain and the evidence needed before a change is accepted.
Safe internal deletions do not need to wait for unrelated product decisions.
Deleting the active reference model still requires the approved replacement coverage under issue `#36`.

### Requirements that deserve reconsideration

The following requirements have concrete implementation or operating costs.
They need an explicit decision, not an undocumented shortcut.

| Decision | Simpler alternative to evaluate | Required protection |
| --- | --- | --- |
| Final-page finality and numeric admission | Define one transaction boundary and one rule for values that RFC 8785 cannot fingerprint. | Atomic verified progress and immutable replay. |
| Ordinary application SQL | Define a supported inference subset. Require explicit authored context for complex writes. | Exact field presence and atomic capture. |
| Completed client history | Define a safe completion and retention boundary instead of keeping every historical payload forever. | Unresolved intent, exact active replay, dependencies, and referenced schemas. |
| Rebuild inspection history | Separate active recovery receipts from optional historical verification payloads. | Exact page identity, finality, and explicit incomplete inspection. |
| Rebuild token payload | Consider binding an immutable session and ordinal instead of repeating the entire session in each token. | Identity, scope, generation, expiry, and replay authentication. |
| Proof cardinality | Assign proof to observable behavior and independent implementations, not one control per requirement or fixed record counts. | Complete requirement coverage and meaningful negative controls. |
| Performance definition identity | Use exact source binding or existing canonical JSON, not a second JavaScript serialization runtime. | The approved workload and expected limits cannot change silently. |
| Repeated deterministic samples | Remove duplicate semantic executions or collect compatible measurement families together after defining their measurement boundary. | Keep distinct boundaries, real native outcomes, and any actual statistical obligation. |
| Current-stable support | Resolve current stable explicitly, or publish a named certified runtime set per release. | Support claims must match actual execution. |
| Seed snapshot isolation | Evaluate whether one read-only repeatable snapshot provides the required export guarantee. | One materialized boundary, exact provenance, and verified publication. |
| Cluster replication principals | Define trusted administrative principals instead of an unexplained cluster-wide uniqueness rule. | Dedicated worker authority and default-deny application-facing roles. |
| Schema rollout | Require dual representations only when the compatibility promise needs them. | Data preservation, immutable identities, and retained queued intent. |
| Local helper policy | Permit a cohesive named step with one caller. Delete unnecessary layers, not functions merely because they have one caller. | Clear ownership and control flow. |

The detailed entries contain the relevant source ranges and tradeoffs.
No change to security, protocol, retention, support, or migration policy is approved here.

### Coordinated findings

Some area reports describe different sides of one repair.
Do not count these as independent defects or implement competing fixes.

| Shared repair | Related finding records |
| --- | --- |
| Production membership validation and valid test fixtures | `04-postgres-api-F09`, `05-postgres-tests-F01` |
| Original wire exchanges and separate cursor acknowledgment identity | `10-blackbox-tests-F02`, `16-verification-framework-F01` |
| Executable fault recipes and the operation they actually affect | `10-blackbox-tests-F03`, `16-verification-framework-F02` |
| One mutant registry and stable assertion identities | `10-blackbox-tests-F05`, `16-verification-framework-F05` |
| Risk-based proof ownership | `16-verification-framework-F09`, `19-product-docs-F03` |
| One policy-value authority | `16-verification-framework-F08`, `19-product-docs-F04` |
| Real compacted-floor prerequisites for native continuation | `11-swift-conformance-F09`, `13-react-native-conformance-F11` |
| Shared Go workload and journey mechanics | `11-swift-conformance-F01`, `13-react-native-conformance-F05`, `13-react-native-conformance-F06` |

Finding identifiers remain stable so their evidence and later dispositions can be tracked.
Their number is not a quality score or a claim that all possible defects were found.

## Review state

**The frozen source review is complete.**

All 20 assigned areas completed source review.
All 649 source packets matched their complete original tool outputs.
The review includes every represented long-line part, not only file names or sampled ranges.
Binary inspection remains subject to the explicit structural limits below.

The frozen source baseline is `98b1507537eaa708eab7cf14e3bf8eddc5eb9b7e`.
During review, the operator merged that source as `96aa2c8b9d7ec285270bfa9ec94ff6ad0984373d`.
Both commits have tree `e1fe9ea71a8ca57074e98cbdfc67346a4c84dca2`.
The merge did not change any reviewed source bytes.

The inventory contains all 912 tracked files and 20 additional local files.
It contains 918 text files, 451,998 text lines, and 14 binary files.
The additional files contain local instructions, local package locks, editor configuration, and authored plans.
No production source, dependency manifest, test, or release workflow changed as part of this review.

Tracking: [#105](https://github.com/trainstar/synchro/issues/105).
Applicable existing issues retain their scope.
Issue [#50](https://github.com/trainstar/synchro/issues/50) now distinguishes the bounded decoder from the remaining raw-ingestion allocation problem.
No implementation issue closed, and no contract change received approval through this review.

## Product purpose

Synchro provides offline synchronization for native applications that use PostgreSQL on the server and SQLite on the device.
It owns synchronized row-level CRUD rather than requiring each application to build a separate upload protocol.

The server uses a PostgreSQL extension as its synchronization authority.
The Go adapter handles HTTP, resolved identity, transport gates, and canonical SQL calls.
Swift and Kotlin own local capture, durable intent, replay, retry, and atomic application of server results.
React Native exposes those native engines rather than adding another synchronization implementation.

The product uses server-defined scopes, per-scope continuation, explicit rebuilds, and portable seeds.
It does not promise arbitrary business commands, peer-to-peer conflict resolution, or arbitrary client-defined replication queries.

Evidence:

- `README.md:7-42`
- `README.md:82-109`
- `docs/src/content/docs/spec/00-principles.mdx:14-40`
- `docs/src/content/docs/spec/00-principles.mdx:191-218`
- `docs/src/content/docs/spec/02-client-contract.mdx:16-67`

## Position in the market

The useful comparison is deployment ownership and application responsibility, not an unsupported ranking.

| Product | Documented design | Difference relevant to Synchro |
| --- | --- | --- |
| Synchro | PostgreSQL extension, thin HTTP host, native SQLite engines, and owned synchronized CRUD. | Keeps server synchronization authority inside PostgreSQL and supplies the write protocol. |
| PowerSync | Separate PowerSync Service, SQLite client SDKs, several source databases, and application-controlled backend mutations. | Supports a broader database and client range. Its application backend owns the write behavior. |
| Zero | Query-driven local data for web applications, with zero-cache between PostgreSQL and clients. | Focuses on application queries and web interaction. Its deployment includes separate replication and view-serving components. |

Official sources retrieved on the review date:

- [PowerSync overview](https://docs.powersync.com/intro/powersync-overview)
- [PowerSync philosophy](https://docs.powersync.com/intro/powersync-philosophy)
- [Zero introduction](https://zero.rocicorp.dev/docs/introduction)
- [Zero self-hosting](https://zero.rocicorp.dev/docs/self-host)

These sources support an architectural comparison.
They do not establish market share, production adoption, relative throughput, lower operating cost, or greater reliability.
The review makes none of those claims.

Native support alone is not unique. PowerSync also documents Swift, Kotlin, and React Native SDKs.
Synchro's clearer distinction is the combination of extension-owned server behavior and an integrated synchronized CRUD contract.

The intended customer already accepts PostgreSQL and wants native offline behavior without a separate synchronization service.
The documented support scope is narrow: PostgreSQL 18 on Ubuntu Linux x64, with specified native client platforms.
The README identifies the project as active development.

Evidence:

- `README.md:25-72`
- `README.md:74-80`
- `RELEASE.md:48-79`

## Strengths worth preserving

The extension boundary gives the server one place to define synchronization behavior.
Thin host adapters prevent each application server from becoming a separate implementation of the protocol.
Native engines keep durable SQLite behavior below the JavaScript bridge.

Server-owned scopes limit the public model.
Explicit cursor, rebuild, and replay rules replace implicit recovery behavior.
Portable seeds use normal authenticated continuation instead of a second synchronization path.

These boundaries can simplify application integration.
They do not make the synchronization implementation itself simple by default.
The extension still owns logical replication, workers, projections, registry transitions, and operational recovery.
Removing a separate service moves those responsibilities into PostgreSQL. It does not remove them.

Evidence:

- `docs/src/content/docs/architecture/overview.mdx:6-36`
- `docs/src/content/docs/spec/02-client-contract.mdx:24-67`
- `docs/src/content/docs/architecture/decisions/001-wal-change-stream.mdx:15-72`
- `README.md:101-109`

## Standard for a simpler codebase

A simplification must remove an unnecessary responsibility, duplicate meaning, unreachable path, or avoidable decision.
A smaller line count alone does not establish a better design.

The following behavior is necessary for this product:

- One server authority for synchronized behavior.
- Commit-safe WAL visibility and acknowledgement.
- Durable immutable identity for a request whose response can be lost.
- Atomic local row, queue, provenance, and cursor transitions.
- Explicit recovery when retained continuation is no longer valid.
- Observable terminal outcomes rather than success-shaped substitutes.
- Independent expected results and negative controls that detect realistic faults.

The following patterns require evidence before they can remain:

- Several implementations of the same harness transport or lifecycle.
- A helper, interface, or command with no active consumer.
- Several independent parsers for one internal envelope.
- A complete reference execution engine where a smaller invariant check is sufficient.
- Repeated schema decoding inside one operation.
- Test setup that changes a private database into an impossible state.
- A source-text or field-presence check presented as behavioral proof.
- Several active documents that repeat executable release policy.

The review separates behavior-preserving cleanup, correctness defects, and product-contract decisions.
It does not approve a protocol change or authorize deletion of a required safety property.

## Inventory context

The tracked source contains 446,525 text lines.
The local supplementary files add 5,473 text lines.

| Tracked area | Files | Text lines |
| --- | ---: | ---: |
| Go adapter and utilities | 31 | 12,378 |
| Rust extensions and relational test data | 64 | 59,882 |
| Native clients, React Native, examples, and client tests | 249 | 103,049 |
| Conformance system | 448 | 231,922 |
| Documentation | 62 | 22,545 |
| Release and CI scripts | 6 | 2,776 |
| Packaged consumers and package checks | 29 | 5,670 |
| GitHub configuration | 10 | 2,522 |
| Root files | 13 | 5,781 |

These figures include tests, generated files, lockfiles, and vendored code.
They describe review scope. They are not complexity scores.
The conformance system contains more than half of the tracked text lines.
That size warrants careful review of proof ownership, but it does not prove that any individual line is unnecessary.

## Method and limits

The review uses a frozen inventory with a SHA-256 identity for every file.
Every text line appears in a numbered source packet.
Long lines span explicit numbered parts.
Long runs of one repeated character use a lossless representation with the exact character and count.

The coverage check compares each packet with the actual completed tool output.
An emitted receipt alone does not establish review.
The final coverage record also requires the scope report to state its semantic review status.
The count includes the dereferenced `CLAUDE.md` link to `AGENTS.md`.
Those paths share one editable instruction source, not two independent policy files.

Binary files have no source lines.
Their review uses suitable structure, provenance, configuration, and consumer checks.
It does not disclose private key-store material or claim instruction-level verification of compiled dependencies.

Installed dependency directories, compiled output, caches, process logs, watchman cookies, and unmanaged scratch archives are outside this frozen source inventory.
They do not become reviewed source merely because they exist under the working directory.
Tracked generated files and tracked vendored dependencies remain inside the review.

The review is static unless a finding names an executed reproduction.
It is not a release signoff or a claim that every supported platform passed its full runtime suite.

The following baseline checks ran at `96aa2c8b9d7ec285270bfa9ec94ff6ad0984373d`.
That revision has the same source tree as the review baseline.

| Command | Baseline result |
| --- | --- |
| `make test-conformance-imports` | 56 tests passed. |
| `make test-conformance-contract` | 152 tests passed. |
| `make test-vectors` | 162 tests passed. |
| `make test-conformance-testresult` | 67 tests passed. The target also rejected its deliberate zero-match run. |

The successful test runs reported no failed or skipped test events.
The zero-match rejection was an expected negative control, not a failed Make gate.
These results establish only the behavior exercised by those checks.
They do not resolve the untested proof gaps or validate any proposed implementation change.

### Binary inspection

| Binary group | Files | Inspection and limit |
| --- | ---: | --- |
| Android launcher PNGs | 10 | Format, dimensions, sample count, hashes, and application references. No visual-accessibility claim. |
| Gradle wrapper JARs | 2 | Complete archive entry lists, launch references, configuration, and hashes. The Kotlin archive also passed its 35-entry integrity check. |
| Example debug key store | 1 | Nonsecret metadata, hash, and build references only. No key, password, or certificate identity was extracted. |
| Pinned SQLite seed | 1 | Immutable read-only schema, integrity, table counts, metadata relationships, and pinned hash. No current native migration or receipt-signature execution claim. |

The wrapper archives contain 35 and 33 entries.
The seed contains 30 tables, 32 indexes, and 60 triggers.
Its integrity check returned `ok`, and its pinned checksum check passed.
The first ordinary read-only open failed. Immutable read-only access succeeded after inspection confirmed that no seed sidecars existed.
The full seed schema received a repair read after one display truncated its prefix.

The vendored Yarn source and all encoded payload text remained in the text review.
Encoded text inspection does not establish decoded machine-instruction correctness or authenticated upstream provenance.
Neither binary structure nor a dependency lockfile certifies that an upstream implementation is free of defects.

## Review process corrections

The first Swift dispatch failed before session creation because the local review database was locked.
The retry started successfully. No Swift source result came from the failed dispatch.

The first React Native turn stopped after four packets because it treated total future input as a context limit.
The next turn corrected that error and reviewed packets 4 through 23.
Later React Native and corpus runs encountered actual context overflow and required bounded continuation.
Original event outputs distinguished complete source delivery from shortened retained-history displays.
Uncertain semantic coverage did not count as completion.

The numeric-input follow-up rejected an incorrect adapter-only fix.
Adding `UseNumber` alone would move rejection into pgrx argument conversion and produce an internal error.
`01-adapter-F09` records the corrected cross-boundary finding and unresolved fingerprintability rule.
The original recommendation is withdrawn, not presented as a safe cleanup.

## Finding index

[Detailed findings](2026-09-17-simplicity-review-findings.md) contain the complete source evidence, proposed changes, retained invariants, and acceptance checks.
[File coverage](2026-09-17-simplicity-review-coverage.csv) identifies every reviewed file, line count, content hash, and inspection method.

Proposed acceptance checks have not run against implementations of these recommendations.
The baseline checks above are separate from that future acceptance work.
Classification describes the finding, not approval to implement its recommendation.


## Go adapter and seed tooling

| Severity | Classification | Finding |
| --- | --- | --- |
| Medium | Correctness defect | [01-adapter-F01: Seed conversion rejects every non-null portable int64](2026-09-17-simplicity-review-findings.md#01-adapter-f01) |
| Medium | Correctness defect | [01-adapter-F02: Primary-key-only rows cannot appear in two portable scopes](2026-09-17-simplicity-review-findings.md#01-adapter-f02) |
| Medium | Correctness defect | [01-adapter-F03: Seed verification reparses quoted trigger names incorrectly](2026-09-17-simplicity-review-findings.md#01-adapter-f03) |
| Medium | Behavior-preserving cleanup | [01-adapter-F04: Seed integrity uses handwritten quadratic sorting](2026-09-17-simplicity-review-findings.md#01-adapter-f04) |
| Medium | Correctness defect | [01-adapter-F05: The seed exporter reimplements transaction lifetime management](2026-09-17-simplicity-review-findings.md#01-adapter-f05) |
| Medium | Correctness defect in verification | [01-adapter-F06: Two seed tests claim workflow proof while calling helpers directly](2026-09-17-simplicity-review-findings.md#01-adapter-f06) |
| Medium | Correctness defect in verification | [01-adapter-F07: Final seed verification omits two installed runtime-state checks](2026-09-17-simplicity-review-findings.md#01-adapter-f07) |
| Medium | Correctness defect | [01-adapter-F08: A seed error includes row and scope identities in command output](2026-09-17-simplicity-review-findings.md#01-adapter-f08) |
| Medium | Contract decision with an extension argument-decoding defect | [01-adapter-F09: Numeric admission lacks a coherent boundary for non-fingerprintable JSON numbers](2026-09-17-simplicity-review-findings.md#01-adapter-f09) |
| Low | Behavior-preserving cleanup | [01-adapter-F10: Four handlers repeat one transport pipeline and decode it three times](2026-09-17-simplicity-review-findings.md#01-adapter-f10) |
| Low | Behavior-preserving cleanup | [01-adapter-F11: Startup compatibility performs one query per required signature](2026-09-17-simplicity-review-findings.md#01-adapter-f11) |
| Low | Behavior-preserving cleanup | [01-adapter-F12: Version sync and version check duplicate their target inventory](2026-09-17-simplicity-review-findings.md#01-adapter-f12) |
| Low | Correctness defect | [01-adapter-F13: The release validator accepts noncanonical leading-zero versions](2026-09-17-simplicity-review-findings.md#01-adapter-f13) |
| Low | Correctness defect | [01-adapter-F14: Release root discovery excludes linked Git worktrees](2026-09-17-simplicity-review-findings.md#01-adapter-f14) |
| Medium | Correctness defect with behavior-preserving cleanup | [01-adapter-F15: Advisory-lock cleanup has duplicated paths and an uncertain-acquisition gap](2026-09-17-simplicity-review-findings.md#01-adapter-f15) |
| Medium | Correctness defect | [01-adapter-F16: Strict operator decoding accepts null as a false state flag](2026-09-17-simplicity-review-findings.md#01-adapter-f16) |
| Low | Behavior-preserving cleanup of verification, with replacement evidence required before deletion | [01-adapter-F17: Scripted recovery tests duplicate successful recovery without checking arguments](2026-09-17-simplicity-review-findings.md#01-adapter-f17) |
| Medium | Correctness defect in verification | [01-adapter-F18: The legacy SQLite syntax test is a substring blacklist, not compatibility proof](2026-09-17-simplicity-review-findings.md#01-adapter-f18) |
| Low | Correctness defect in verification | [01-adapter-F19: The SQL-qualification check has a stale function whitelist](2026-09-17-simplicity-review-findings.md#01-adapter-f19) |
| Low | Correctness defect | [01-adapter-F20: Test identifier truncation produces 64-byte names](2026-09-17-simplicity-review-findings.md#01-adapter-f20) |
| Medium | Contract decision. No change is approved | [01-adapter-F21: Clarify whether an unpublished completion marker requires two complete artifact scans](2026-09-17-simplicity-review-findings.md#01-adapter-f21) |
| Low | Contract decision. No deletion is approved | [01-adapter-F22: Decide whether equivalent index spelling is part of seed integrity](2026-09-17-simplicity-review-findings.md#01-adapter-f22) |
| Medium | Correctness defect in release tooling | [01-adapter-F23: Version synchronization omits catalogs that the release validator requires](2026-09-17-simplicity-review-findings.md#01-adapter-f23) |

## Portable Rust core

| Severity | Classification | Finding |
| --- | --- | --- |
| Medium | Correctness defect | [02-rust-core-F01: Required nullable members deserialize as optional members](2026-09-17-simplicity-review-findings.md#02-rust-core-f01) |
| Medium | Behavior-preserving cleanup | [02-rust-core-F02: Three Rust JSON visitors own the same duplicate-member rule](2026-09-17-simplicity-review-findings.md#02-rust-core-f02) |
| Medium | Correctness defect in the boundary proof and inconsistent implementation limits | [02-rust-core-F03: The accepted JSON depth test bypasses the parser that rejects that depth](2026-09-17-simplicity-review-findings.md#02-rust-core-f03) |
| Medium | Behavior-preserving cleanup | [02-rust-core-F04: Scope hashing materializes the complete preimage before hashing](2026-09-17-simplicity-review-findings.md#02-rust-core-f04) |
| Medium | Correctness defect in verification | [02-rust-core-F05: Vector tests can pass when their kind filter selects no vectors](2026-09-17-simplicity-review-findings.md#02-rust-core-f05) |
| Low | Behavior-preserving cleanup within repository consumers | [02-rust-core-F06: Semantic-version validation constructs an unused owned model](2026-09-17-simplicity-review-findings.md#02-rust-core-f06) |
| Low | Behavior-preserving cleanup within repository consumers | [02-rust-core-F07: Scope-name compatibility wrappers have no production consumer](2026-09-17-simplicity-review-findings.md#02-rust-core-f07) |
| Low | Behavior-preserving cleanup | [02-rust-core-F08: The core declares an unused assertion dependency](2026-09-17-simplicity-review-findings.md#02-rust-core-f08) |
| Low | Behavior-preserving cleanup | [02-rust-core-F09: Row hashing repeats immutable constructor guarantees](2026-09-17-simplicity-review-findings.md#02-rust-core-f09) |
| Low | Correctness defect in a test | [02-rust-core-F10: The generation-binding assertion changes the user instead of the generation](2026-09-17-simplicity-review-findings.md#02-rust-core-f10) |
| Low | Behavior-preserving cleanup within repository consumers | [02-rust-core-F11: The legacy queue-operation parser has no runtime consumer](2026-09-17-simplicity-review-findings.md#02-rust-core-f11) |

## PostgreSQL capture and recovery

| Severity | Classification | Finding |
| --- | --- | --- |
| High | correctness defect | [03-postgres-capture-F01: Projection activation can replace newer authoritative row versions with older candidate versions](2026-09-17-simplicity-review-findings.md#03-postgres-capture-f01) |
| High | correctness defect | [03-postgres-capture-F02: Baseline verification requires every historical pending fence to equal one final row version](2026-09-17-simplicity-review-findings.md#03-postgres-capture-f02) |
| Medium | behavior-preserving cleanup | [03-postgres-capture-F03: A verified candidate repeats full finalization on every worker poll](2026-09-17-simplicity-review-findings.md#03-postgres-capture-f03) |
| Medium | behavior-preserving cleanup | [03-postgres-capture-F04: Schema activation migrates every retained digest twice](2026-09-17-simplicity-review-findings.md#03-postgres-capture-f04) |
| Medium | behavior-preserving cleanup | [03-postgres-capture-F05: Reset staging keeps a row-at-a-time membership implementation beside the batched candidate implementation](2026-09-17-simplicity-review-findings.md#03-postgres-capture-f05) |
| Medium | behavior-preserving cleanup | [03-postgres-capture-F06: Reset and projection bootstrap duplicate the same baseline construction and staging verification](2026-09-17-simplicity-review-findings.md#03-postgres-capture-f06) |
| Medium | correctness defect | [03-postgres-capture-F07: The worker copies the complete raw WAL result before decoder limits can protect it](2026-09-17-simplicity-review-findings.md#03-postgres-capture-f07) |
| Medium | correctness defect | [03-postgres-capture-F08: Candidate acknowledgement lacks the active slot's bounded crash reconciliation](2026-09-17-simplicity-review-findings.md#03-postgres-capture-f08) |
| Medium | correctness defect | [03-postgres-capture-F09: Data-bearing error construction defeats otherwise bounded capture diagnostics](2026-09-17-simplicity-review-findings.md#03-postgres-capture-f09) |
| Low | behavior-preserving cleanup | [03-postgres-capture-F10: Backfill duplicates the canonical row-digest implementation](2026-09-17-simplicity-review-findings.md#03-postgres-capture-f10) |
| Low | behavior-preserving cleanup | [03-postgres-capture-F11: Existing-edge validation joins captured rows but ignores every joined value](2026-09-17-simplicity-review-findings.md#03-postgres-capture-f11) |
| Low | correctness defect | [03-postgres-capture-F12: Backfill reports an acknowledgement LSN as a logical transaction-end position](2026-09-17-simplicity-review-findings.md#03-postgres-capture-f12) |
| Low | correctness defect in test proof | [03-postgres-capture-F13: One malformed-message assertion runs on an already poisoned decoder](2026-09-17-simplicity-review-findings.md#03-postgres-capture-f13) |
| Low | behavior-preserving cleanup | [03-postgres-capture-F14: Capture helpers retain no-op control flow and duplicate an existing clone operation](2026-09-17-simplicity-review-findings.md#03-postgres-capture-f14) |

[Additional decisions and acceptance details](2026-09-17-simplicity-review-findings.md#area-03-postgres-capture).

## PostgreSQL API and schema

| Severity | Classification | Finding |
| --- | --- | --- |
| High | correctness defect | [04-postgres-api-F01: Connect can commit an unreported generation renewal](2026-09-17-simplicity-review-findings.md#04-postgres-api-f01) |
| High | correctness defect | [04-postgres-api-F02: Decimal narrowing bypasses incompatible-schema classification](2026-09-17-simplicity-review-findings.md#04-postgres-api-f02) |
| High | correctness defect | [04-postgres-api-F03: Seed receipt validation omits the retention floor](2026-09-17-simplicity-review-findings.md#04-postgres-api-f03) |
| Medium | correctness defect | [04-postgres-api-F04: Nullable byte fields pass validation and then abort push](2026-09-17-simplicity-review-findings.md#04-postgres-api-f04) |
| High | correctness defect | [04-postgres-api-F05: Pending registration bypasses unrelated drift checks](2026-09-17-simplicity-review-findings.md#04-postgres-api-f05) |
| High | correctness defect | [04-postgres-api-F06: Trigger validation searches deparsed SQL instead of comparing arguments](2026-09-17-simplicity-review-findings.md#04-postgres-api-f06) |
| High | correctness defect | [04-postgres-api-F07: The deterministic-function validator permits nondeterministic built-ins](2026-09-17-simplicity-review-findings.md#04-postgres-api-f07) |
| Medium | correctness defect | [04-postgres-api-F08: Mutation-ledger immutability contains an unused deletion exception](2026-09-17-simplicity-review-findings.md#04-postgres-api-f08) |
| High | correctness defect in verification | [04-postgres-api-F09: Legacy fixtures require three test-only production bypasses](2026-09-17-simplicity-review-findings.md#04-postgres-api-f09) |
| Medium | correctness defect in verification | [04-postgres-api-F10: Push outcome tests derive their expected row from production hydration](2026-09-17-simplicity-review-findings.md#04-postgres-api-f10) |
| Medium | behavior-preserving cleanup | [04-postgres-api-F11: Portable seed pagination repeatedly loads and hashes the complete scope](2026-09-17-simplicity-review-findings.md#04-postgres-api-f11) |
| Medium | behavior-preserving cleanup | [04-postgres-api-F12: Loading the latest manifest reads and validates every historical manifest](2026-09-17-simplicity-review-findings.md#04-postgres-api-f12) |
| Medium | behavior-preserving cleanup | [04-postgres-api-F13: Rebuild reloads the same schema hash for each staged row](2026-09-17-simplicity-review-findings.md#04-postgres-api-f13) |
| Low | behavior-preserving cleanup | [04-postgres-api-F14: Per-batch and per-mutation locks add no exclusion beyond the client lock](2026-09-17-simplicity-review-findings.md#04-postgres-api-f14) |
| Medium | correctness defect | [04-postgres-api-F15: Removing a shared scope also removes an independent user grant](2026-09-17-simplicity-review-findings.md#04-postgres-api-f15) |
| Medium | behavior-preserving cleanup | [04-postgres-api-F16: Registry metadata validation has two independently maintained copies](2026-09-17-simplicity-review-findings.md#04-postgres-api-f16) |
| Medium | correctness defect with a shared-validation cleanup | [04-postgres-api-F17: Seed and rebuild duplicate snapshot validation and already disagree](2026-09-17-simplicity-review-findings.md#04-postgres-api-f17) |
| Low | behavior-preserving cleanup | [04-postgres-api-F18: Three token modules duplicate the same database key-selection policy](2026-09-17-simplicity-review-findings.md#04-postgres-api-f18) |
| Low | behavior-preserving cleanup | [04-postgres-api-F19: Unused legacy state and an unreachable lookup branch remain installed](2026-09-17-simplicity-review-findings.md#04-postgres-api-f19) |
| Medium | correctness defect | [04-postgres-api-F20: Rebuild integrity logging includes application primary-key values](2026-09-17-simplicity-review-findings.md#04-postgres-api-f20) |
| Low | contract decision | [04-postgres-api-F21: The rebuild token contract duplicates immutable session state](2026-09-17-simplicity-review-findings.md#04-postgres-api-f21) |

## PostgreSQL tests and relational fixtures

| Severity | Classification | Finding |
| --- | --- | --- |
| Medium | Correctness defect in test architecture | [05-postgres-tests-F01: Legacy membership fixtures require production validation exemptions](2026-09-17-simplicity-review-findings.md#05-postgres-tests-f01) |
| Medium | Correctness defect in tests | [05-postgres-tests-F02: Two rebuild tests do not construct the state they claim to test](2026-09-17-simplicity-review-findings.md#05-postgres-tests-f02) |
| Medium | Correctness defect in negative controls | [05-postgres-tests-F03: Boundary rejection tests have unrelated failure paths](2026-09-17-simplicity-review-findings.md#05-postgres-tests-f03) |
| Medium | Correctness defect in proof ownership | [05-postgres-tests-F04: Schema activation assertions bypass or omit activation](2026-09-17-simplicity-review-findings.md#05-postgres-tests-f04) |
| Medium | Correctness defect in tests | [05-postgres-tests-F05: Reset replacement and cleanup checks start with empty or unchanged state](2026-09-17-simplicity-review-findings.md#05-postgres-tests-f05) |
| Medium | Correctness defect in tests | [05-postgres-tests-F06: Visibility checks can pass with no valid allowed result](2026-09-17-simplicity-review-findings.md#05-postgres-tests-f06) |
| Low | Behavior-preserving cleanup | [05-postgres-tests-F07: Several proofs are strict subsets of existing proofs](2026-09-17-simplicity-review-findings.md#05-postgres-tests-f07) |
| Medium | Correctness defect in performance proof | [05-postgres-tests-F08: Query-text assertions overconstrain syntax without proving bounded callers](2026-09-17-simplicity-review-findings.md#05-postgres-tests-f08) |
| Medium | Behavior-preserving cleanup of brittle test control flow | [05-postgres-tests-F09: Concurrency observation uses inconsistent and scheduler-dependent budgets](2026-09-17-simplicity-review-findings.md#05-postgres-tests-f09) |
| Low | Behavior-preserving cleanup | [05-postgres-tests-F10: Seed receipt idempotency depends on a physical tuple address](2026-09-17-simplicity-review-findings.md#05-postgres-tests-f10) |
| Low | Correctness defect in test data | [05-postgres-tests-F11: The canonical category upsert does not restore its parent](2026-09-17-simplicity-review-findings.md#05-postgres-tests-f11) |
| Low | Behavior-preserving cleanup | [05-postgres-tests-F12: The seed generator carries unused ownership state and parameters](2026-09-17-simplicity-review-findings.md#05-postgres-tests-f12) |
| Medium | Correctness defect in relational test configuration | [05-postgres-tests-F13: Document-member fixtures read peer rows without registering peer impacts](2026-09-17-simplicity-review-findings.md#05-postgres-tests-f13) |
| Low | Behavior-preserving documentation cleanup, unless transitive ownership is required | [05-postgres-tests-F14: Relational fixture documentation overstates transitive ownership coverage](2026-09-17-simplicity-review-findings.md#05-postgres-tests-f14) |

## Swift SDK and tests

| Severity | Classification | Finding |
| --- | --- | --- |
| High | Correctness defect | [06-swift-F01: Two schema paths hide missing retained-scope digest migration](2026-09-17-simplicity-review-findings.md#06-swift-f01) |
| High | Correctness defect | [06-swift-F02: Nullable relaxation changes the manifest but leaves SQLite NOT NULL](2026-09-17-simplicity-review-findings.md#06-swift-f02) |
| High | Correctness defect | [06-swift-F03: Writes without a parsed capture context can change synced rows without intent](2026-09-17-simplicity-review-findings.md#06-swift-f03) |
| High | Correctness defect | [06-swift-F04: Capture casts can replace invalid authored values with different valid values](2026-09-17-simplicity-review-findings.md#06-swift-f04) |
| High | Correctness defect | [06-swift-F05: Startup backoff cancellation leaves the startup continuation unresolved](2026-09-17-simplicity-review-findings.md#06-swift-f05) |
| High | Correctness defect | [06-swift-F06: Retried connect responses bypass normal contract validation](2026-09-17-simplicity-review-findings.md#06-swift-f06) |
| Medium | Correctness defect | [06-swift-F07: Configuration clamps page limits and permits timer conversion traps](2026-09-17-simplicity-review-findings.md#06-swift-f07) |
| Medium | Behavior-preserving cleanup of the live sync path | [06-swift-F08: Test-only reconciliation permits the exact-ID behavior that production forbids](2026-09-17-simplicity-review-findings.md#06-swift-f08) |
| Medium | Behavior-preserving cleanup, subject to public symbol compatibility review | [06-swift-F09: Dormant protocol-era paths and their tests remain in the SDK](2026-09-17-simplicity-review-findings.md#06-swift-f09) |
| Medium | Behavior-preserving cleanup | [06-swift-F10: Pending selection repeats whole-queue normalization and per-row field loads](2026-09-17-simplicity-review-findings.md#06-swift-f10) |
| Medium | Behavior-preserving cleanup | [06-swift-F11: Historical schema resolution scans and decodes every retained push batch](2026-09-17-simplicity-review-findings.md#06-swift-f11) |
| Medium | Behavior-preserving cleanup | [06-swift-F12: Portable value conversion has several owners inside the same Swift engine](2026-09-17-simplicity-review-findings.md#06-swift-f12) |
| Medium | Correctness defect | [06-swift-F13: Pull generates invalid upsert SQL for a primary-key-only table](2026-09-17-simplicity-review-findings.md#06-swift-f13) |
| Medium | Behavior-preserving cleanup | [06-swift-F14: Bounded state inspection performs unbounded loading before truncation](2026-09-17-simplicity-review-findings.md#06-swift-f14) |
| Medium | Correctness defect in verification and inspection | [06-swift-F15: Inspection omits orphan provenance and rebuild attempts](2026-09-17-simplicity-review-findings.md#06-swift-f15) |
| Medium | Correctness defect in verification evidence | [06-swift-F16: Runner transport facts invent protocol codes from HTTP status](2026-09-17-simplicity-review-findings.md#06-swift-f16) |
| Medium | Behavior-preserving test cleanup | [06-swift-F17: Requirement-proof suites duplicate existing flows instead of keeping one proof home](2026-09-17-simplicity-review-findings.md#06-swift-f17) |
| Medium | Correctness defect in proof | [06-swift-F18: The checksum-vector test can reject invalid input before production sees it](2026-09-17-simplicity-review-findings.md#06-swift-f18) |
| Low | Behavior-preserving test cleanup | [06-swift-F19: Several negative controls do not exercise the behavior they name](2026-09-17-simplicity-review-findings.md#06-swift-f19) |
| Medium | Behavior-preserving test cleanup | [06-swift-F20: Legacy test schema helpers ignore inputs and manufacture inconsistent schema bindings](2026-09-17-simplicity-review-findings.md#06-swift-f20) |
| Low | Behavior-preserving cleanup | [06-swift-F21: The scenario loader has no semantic consumer](2026-09-17-simplicity-review-findings.md#06-swift-f21) |
| Low | Behavior-preserving cleanup | [06-swift-F22: Pull duplicate detection serializes every complete change only to choose error wording](2026-09-17-simplicity-review-findings.md#06-swift-f22) |
| Low | Behavior-preserving cleanup | [06-swift-F23: Disabled transport observation still decodes complete request bodies](2026-09-17-simplicity-review-findings.md#06-swift-f23) |
| Medium | Correctness defect | [06-swift-F24: HTTP 426 still uses the legacy error-message path](2026-09-17-simplicity-review-findings.md#06-swift-f24) |
| Medium | Correctness defect with a simpler observation boundary | [06-swift-F25: Watch reads before subscribing and ignores the requested table set](2026-09-17-simplicity-review-findings.md#06-swift-f25) |
| Medium | Contract decision | [06-swift-D01: Define one startup completion rule](2026-09-17-simplicity-review-findings.md#06-swift-d01) |
| Medium | Contract decision | [06-swift-D02: Define retention and compaction for completed client history](2026-09-17-simplicity-review-findings.md#06-swift-d02) |
| Medium | Contract decision | [06-swift-D03: Bound ordinary SQL syntax instead of building a second SQL interpreter](2026-09-17-simplicity-review-findings.md#06-swift-d03) |
| Medium | Contract decision | [06-swift-D04: Separate durable rebuild recovery receipts from historical verification payloads](2026-09-17-simplicity-review-findings.md#06-swift-d04) |

## Kotlin SDK and tests

| Severity | Classification | Finding |
| --- | --- | --- |
| High | Correctness defect | [07-kotlin-F01: Public transaction objects outlive their ownership boundary](2026-09-17-simplicity-review-findings.md#07-kotlin-f01) |
| High | Correctness defect | [07-kotlin-F02: Authored capture context can silently suppress another table's update](2026-09-17-simplicity-review-findings.md#07-kotlin-f02) |
| High | Correctness defect in verification, with behavior-preserving cleanup of the obsolete path | [07-kotlin-F03: Schema tests preserve a second migration implementation that production does not use for schema changes](2026-09-17-simplicity-review-findings.md#07-kotlin-f03) |
| High | Correctness defect | [07-kotlin-F04: Blocking-error cleanup belongs to the caller instead of the engine-owned operation](2026-09-17-simplicity-review-findings.md#07-kotlin-f04) |
| Medium | Correctness defect | [07-kotlin-F05: The read SQL boundary misses SQLite single-quoted table names](2026-09-17-simplicity-review-findings.md#07-kotlin-f05) |
| High | Correctness defect | [07-kotlin-F06: The exact-trigger comparator changes quoted token contents](2026-09-17-simplicity-review-findings.md#07-kotlin-f06) |
| Medium | Behavior-preserving cleanup | [07-kotlin-F07: Two production tables define the same lifecycle graph](2026-09-17-simplicity-review-findings.md#07-kotlin-f07) |
| Medium | Correctness defect caused by duplicate behavior | [07-kotlin-F08: SQLite-to-wire conversion has a strict owner and a permissive duplicate](2026-09-17-simplicity-review-findings.md#07-kotlin-f08) |
| Medium | Behavior-preserving cleanup | [07-kotlin-F09: Record loading decomposes bounded sets into repeated single-row queries](2026-09-17-simplicity-review-findings.md#07-kotlin-f09) |
| Medium | Correctness defect in the inspection bound | [07-kotlin-F10: The bounded capture API bounds receipt groups, not their materialized contents](2026-09-17-simplicity-review-findings.md#07-kotlin-f10) |
| Medium | Contract decision | [07-kotlin-F11: Completed rebuild receipts retain whole row payloads without a bounded retention rule](2026-09-17-simplicity-review-findings.md#07-kotlin-f11) |
| Medium | Correctness defect caused by duplicate interpretation | [07-kotlin-F12: HTTP error decoding discards canonical codes before durable failure mapping](2026-09-17-simplicity-review-findings.md#07-kotlin-f12) |
| Medium | Behavior-preserving verification cleanup | [07-kotlin-F13: Issue-named proof suites repeat behavior tests and contain ineffective negative controls](2026-09-17-simplicity-review-findings.md#07-kotlin-f13) |
| Medium | Correctness defect in proof ownership | [07-kotlin-F14: The authored digest-vector consumer rejects malformed JSON in test-only code](2026-09-17-simplicity-review-findings.md#07-kotlin-f14) |
| Low | Behavior-preserving cleanup for internal symbols. Public symbol removal requires compatibility review | [07-kotlin-F15: Unconsumed compatibility and test-support paths remain in production](2026-09-17-simplicity-review-findings.md#07-kotlin-f15) |
| Low | Behavior-preserving cleanup | [07-kotlin-F16: Temporary memory diagnostics run on every production rebuild page](2026-09-17-simplicity-review-findings.md#07-kotlin-f16) |
| Low | Behavior-preserving cleanup | [07-kotlin-F17: Maven metadata has two local sources of truth](2026-09-17-simplicity-review-findings.md#07-kotlin-f17) |
| Low | Behavior-preserving documentation cleanup | [07-kotlin-F18: The Kotlin README describes an obsolete implementation and a missing design file](2026-09-17-simplicity-review-findings.md#07-kotlin-f18) |
| Medium | Correctness defect | [07-kotlin-F19: A negative retry count skips the initial sync cycle without an error](2026-09-17-simplicity-review-findings.md#07-kotlin-f19) |

[Additional decisions and acceptance details](2026-09-17-simplicity-review-findings.md#area-07-kotlin).

## React Native SDK, bridges, and examples

| Severity | Classification | Finding |
| --- | --- | --- |
| High | correctness defect | [08-react-native-F01: Native transaction failures can leave bridge promises pending](2026-09-17-simplicity-review-findings.md#08-react-native-f01) |
| Medium | behavior-preserving cleanup | [08-react-native-F02: Detox journeys duplicate one coordinator protocol](2026-09-17-simplicity-review-findings.md#08-react-native-f02) |
| Low | behavior-preserving cleanup | [08-react-native-F03: The Xcode scheme references a missing test target](2026-09-17-simplicity-review-findings.md#08-react-native-f03) |
| High | correctness defect | [08-react-native-F04: iOS schema decoding can trap instead of rejecting invalid input](2026-09-17-simplicity-review-findings.md#08-react-native-f04) |
| Medium | correctness defect | [08-react-native-F05: Tagged query parameters defeat hook dependency stabilization](2026-09-17-simplicity-review-findings.md#08-react-native-f05) |
| Medium | correctness defect | [08-react-native-F06: The status hook can retain another client's status](2026-09-17-simplicity-review-findings.md#08-react-native-f06) |
| Medium | correctness defect in inspection fidelity | [08-react-native-F07: Inspection projection removes capture completeness evidence](2026-09-17-simplicity-review-findings.md#08-react-native-f07) |
| Low | behavior-preserving cleanup | [08-react-native-F08: The iOS wrapper retains an unsupported legacy bridge](2026-09-17-simplicity-review-findings.md#08-react-native-f08) |
| Medium | correctness defect at a diagnostic boundary | [08-react-native-F09: A coordinator validation error can disclose runtime credentials](2026-09-17-simplicity-review-findings.md#08-react-native-f09) |
| Medium | correctness defect in test proof | [08-react-native-F10: The timeout rollback test contains no write to roll back](2026-09-17-simplicity-review-findings.md#08-react-native-f10) |
| Medium | correctness defect in test proof | [08-react-native-F11: The error-mapping smoke accepts an unknown error code](2026-09-17-simplicity-review-findings.md#08-react-native-f11) |
| Low | behavior-preserving cleanup | [08-react-native-F12: Subscriber tests repeat the same routing proof](2026-09-17-simplicity-review-findings.md#08-react-native-f12) |
| Low | behavior-preserving cleanup | [08-react-native-F13: An unused Turbo task layer remains installed](2026-09-17-simplicity-review-findings.md#08-react-native-f13) |
| Medium | behavior-preserving cleanup of development dependency selection | [08-react-native-F14: Published Android configuration prefers uncontrolled local Maven artifacts](2026-09-17-simplicity-review-findings.md#08-react-native-f14) |
| Low | behavior-preserving cleanup | [08-react-native-F15: The build tracker duplicates release evidence without identity](2026-09-17-simplicity-review-findings.md#08-react-native-f15) |
| Medium | correctness defect | [08-react-native-F16: Android idempotency errors lose their native code in JavaScript](2026-09-17-simplicity-review-findings.md#08-react-native-f16) |

[Additional decisions and acceptance details](2026-09-17-simplicity-review-findings.md#area-08-react-native).

## Blackbox runtime and observers

| Severity | Classification | Finding |
| --- | --- | --- |
| High | correctness defect | [09-blackbox-runtime-F01: Native capture substitutes controller state for server evidence](2026-09-17-simplicity-review-findings.md#09-blackbox-runtime-f01) |
| Medium | contract decision. This concerns the authored test-operation contract, not approval to change production semantics | [09-blackbox-runtime-F02: Native realization infers fixture semantics instead of declaring them](2026-09-17-simplicity-review-findings.md#09-blackbox-runtime-f02) |
| High | correctness defect | [09-blackbox-runtime-F03: WAL bindings can assign two authored transactions to one runtime transaction](2026-09-17-simplicity-review-findings.md#09-blackbox-runtime-f03) |
| High | correctness defect | [09-blackbox-runtime-F04: Replay reconstruction replaces the caller's request with server-owned bytes](2026-09-17-simplicity-review-findings.md#09-blackbox-runtime-f04) |
| High | correctness defect | [09-blackbox-runtime-F05: Native LSN parsing loses the 32-bit low-word boundary](2026-09-17-simplicity-review-findings.md#09-blackbox-runtime-f05) |
| Medium | correctness defect | [09-blackbox-runtime-F06: A normal WAL polling miss starts a ten-second diagnostic sampler](2026-09-17-simplicity-review-findings.md#09-blackbox-runtime-f06) |
| Medium | correctness defect with a behavior-preserving consolidation opportunity for valid input | [09-blackbox-runtime-F07: Two strict JSON validators enforce different Unicode rules](2026-09-17-simplicity-review-findings.md#09-blackbox-runtime-f07) |
| Medium | correctness defect | [09-blackbox-runtime-F08: Recorder bounds apply after attachment writes and outside their synchronization boundary](2026-09-17-simplicity-review-findings.md#09-blackbox-runtime-f08) |
| Medium | behavior-preserving cleanup, subject to preserving existing failure diagnostics and ownership checks | [09-blackbox-runtime-F09: Owned-cluster cleanup restarts PostgreSQL to delete objects that disappear with the cluster](2026-09-17-simplicity-review-findings.md#09-blackbox-runtime-f09) |
| Medium | correctness defect | [09-blackbox-runtime-F10: Synchronous command runners do not cancel their process groups](2026-09-17-simplicity-review-findings.md#09-blackbox-runtime-f10) |
| Medium | behavior-preserving cleanup of harness self-tests. Preserve useful negative controls at their actual proof homes | [09-blackbox-runtime-F11: The synthetic harness maintains a second protocol and assertion stack for its own self-tests](2026-09-17-simplicity-review-findings.md#09-blackbox-runtime-f11) |
| Low | behavior-preserving cleanup | [09-blackbox-runtime-F12: Two WAL-record observers duplicate the same query and return partially different facts](2026-09-17-simplicity-review-findings.md#09-blackbox-runtime-f12) |
| Low | behavior-preserving cleanup | [09-blackbox-runtime-F13: Dead private state and an unused HBA generator retain tests and lifecycle code](2026-09-17-simplicity-review-findings.md#09-blackbox-runtime-f13) |
| Low | behavior-preserving cleanup | [09-blackbox-runtime-F14: Seed preparation sends 1,000 separate inserts inside one fixed fixture transaction](2026-09-17-simplicity-review-findings.md#09-blackbox-runtime-f14) |
| Medium | correctness defect in proof classification. A new public SQL error contract would require a separate decision | [09-blackbox-runtime-F15: Generic internal SQL errors count as configured-limit validation evidence](2026-09-17-simplicity-review-findings.md#09-blackbox-runtime-f15) |
| Medium | correctness defect in test proof | [09-blackbox-runtime-F16: Alias-shape negative tests also fail when their shape checks are removed](2026-09-17-simplicity-review-findings.md#09-blackbox-runtime-f16) |

## Blackbox integration and soak proofs

| Severity | Classification | Finding |
| --- | --- | --- |
| High | Correctness defect in verification | [10-blackbox-tests-F01: Soak client durability observations come from an in-memory test client](2026-09-17-simplicity-review-findings.md#10-blackbox-tests-f01) |
| High | Correctness defect in verification | [10-blackbox-tests-F02: Soak combines different HTTP exchanges to construct cursor proof](2026-09-17-simplicity-review-findings.md#10-blackbox-tests-f02) |
| High | Correctness defect in verification | [10-blackbox-tests-F03: Soak applies push and pull fault plans to an unrelated connect request](2026-09-17-simplicity-review-findings.md#10-blackbox-tests-f03) |
| Medium | Behavior-preserving cleanup | [10-blackbox-tests-F04: Issue-specific suites repeat existing semantic proof implementations](2026-09-17-simplicity-review-findings.md#10-blackbox-tests-f04) |
| Medium | Behavior-preserving cleanup with a verification integrity risk | [10-blackbox-tests-F05: Proof bookkeeping duplicates bindings and depends on subtest positions](2026-09-17-simplicity-review-findings.md#10-blackbox-tests-f05) |
| High | Correctness defect in verification | [10-blackbox-tests-F06: Sealed retry proof checks requests created by the test itself](2026-09-17-simplicity-review-findings.md#10-blackbox-tests-f06) |
| Medium | Correctness defect in verification | [10-blackbox-tests-F07: Several negative controls change multiple rejection conditions together](2026-09-17-simplicity-review-findings.md#10-blackbox-tests-f07) |
| Medium | Correctness defect in verification | [10-blackbox-tests-F08: Hydration recovery checks checkpoint count instead of checkpoint position](2026-09-17-simplicity-review-findings.md#10-blackbox-tests-f08) |
| Low | Behavior-preserving cleanup | [10-blackbox-tests-F09: Test helper families duplicate the same protocol parsing and checks](2026-09-17-simplicity-review-findings.md#10-blackbox-tests-f09) |
| Medium | Contract decision | [10-blackbox-tests-F10: Benchmark comparison policy binds a permanent baseline to one host and one source file](2026-09-17-simplicity-review-findings.md#10-blackbox-tests-f10) |
| Medium | Correctness defect in verification control flow | [10-blackbox-tests-F11: RSS sampling can wait forever for a readiness message after cancellation](2026-09-17-simplicity-review-findings.md#10-blackbox-tests-f11) |
| Medium | Correctness defect in verification | [10-blackbox-tests-F12: Quarantine redaction proof does not create quarantine output](2026-09-17-simplicity-review-findings.md#10-blackbox-tests-f12) |

## Shared native conformance findings

| Severity | Classification | Finding |
| --- | --- | --- |
| Low | Behavior-preserving cleanup | [11-swift-conformance-F01: Go journey orchestration has multiple owners](2026-09-17-simplicity-review-findings.md#11-swift-conformance-f01) |
| Low | Behavior-preserving cleanup | [11-swift-conformance-F02: Dead setup and test-only helpers create false dependencies](2026-09-17-simplicity-review-findings.md#11-swift-conformance-f02) |
| Medium | Correctness defect | [11-swift-conformance-F03: Observation copying does not preserve immutable evidence](2026-09-17-simplicity-review-findings.md#11-swift-conformance-f03) |
| Medium | Correctness defect in test proof | [11-swift-conformance-F04: Several Swift negative parser tests fail for unrelated reasons](2026-09-17-simplicity-review-findings.md#11-swift-conformance-f04) |
| Medium | Correctness defect | [11-swift-conformance-F05: Shared queue projection emits the wrong int64 identity form](2026-09-17-simplicity-review-findings.md#11-swift-conformance-f05) |
| Low | Correctness defect in comparison logic | [11-swift-conformance-F06: Provenance matching assumes identity translation preserves sort order](2026-09-17-simplicity-review-findings.md#11-swift-conformance-f06) |
| Medium | Correctness defect in test proof | [11-swift-conformance-F07: Schema-transition proof omits part of immutable authored intent](2026-09-17-simplicity-review-findings.md#11-swift-conformance-f07) |
| Medium | Correctness defect in proof attribution | [11-swift-conformance-F08: The scope-digest fault exercises malformed syntax instead of aggregate integrity](2026-09-17-simplicity-review-findings.md#11-swift-conformance-f08) |
| Medium | Correctness defect in test proof | [11-swift-conformance-F09: The native floor-resume proof never establishes floor equality](2026-09-17-simplicity-review-findings.md#11-swift-conformance-f09) |
| Low | Behavior-preserving cleanup | [11-swift-conformance-F10: Recovery observation returns its expected completion](2026-09-17-simplicity-review-findings.md#11-swift-conformance-f10) |
| High | Correctness defect in gate evidence | [11-swift-conformance-F11: Native performance sample results do not reach measurement assertions](2026-09-17-simplicity-review-findings.md#11-swift-conformance-f11) |
| Low | Contract decision. No reduction is approved | [11-swift-conformance-F12: Reconsider separate repeated executions for overlapping rebuild measurements](2026-09-17-simplicity-review-findings.md#11-swift-conformance-f12) |

[Additional decisions and acceptance details](2026-09-17-simplicity-review-findings.md#area-11-swift-conformance).

## Kotlin conformance-specific findings

| Severity | Classification | Finding |
| --- | --- | --- |
| Medium | Correctness defect | [12-kotlin-conformance-F01: Socket execution does not observe context cancellation while waiting](2026-09-17-simplicity-review-findings.md#12-kotlin-conformance-f01) |
| Medium | Behavior-preserving cleanup | [12-kotlin-conformance-F02: Captured facts remain raw through repeated decoding and projection](2026-09-17-simplicity-review-findings.md#12-kotlin-conformance-f02) |

[Additional decisions and acceptance details](2026-09-17-simplicity-review-findings.md#area-12-kotlin-conformance).

## React Native conformance

| Severity | Classification | Finding |
| --- | --- | --- |
| High | correctness defect | [13-react-native-conformance-F01: Client detail captures become count-only proof](2026-09-17-simplicity-review-findings.md#13-react-native-conformance-f01) |
| Medium | correctness defect | [13-react-native-conformance-F02: Result validators do not consistently enforce closed envelopes or operation completion](2026-09-17-simplicity-review-findings.md#13-react-native-conformance-f02) |
| Medium | correctness defect | [13-react-native-conformance-F03: Full-flow tests construct their own flow instead of executing it](2026-09-17-simplicity-review-findings.md#13-react-native-conformance-f03) |
| High | correctness defect | [13-react-native-conformance-F04: Rebuild-apply collects integrity proof and ignores its semantic results](2026-09-17-simplicity-review-findings.md#13-react-native-conformance-f04) |
| Medium | behavior-preserving cleanup | [13-react-native-conformance-F05: Repeated Go coordinator plumbing has already diverged](2026-09-17-simplicity-review-findings.md#13-react-native-conformance-f05) |
| Medium | behavior-preserving cleanup | [13-react-native-conformance-F06: Go platform drivers duplicate deterministic workload and schema construction](2026-09-17-simplicity-review-findings.md#13-react-native-conformance-f06) |
| Low | behavior-preserving cleanup | [13-react-native-conformance-F07: Diagnostic text tests preserve unnecessary validation machinery](2026-09-17-simplicity-review-findings.md#13-react-native-conformance-f07) |
| Low | behavior-preserving cleanup | [13-react-native-conformance-F08: Dead helpers and unused configuration remain in the coordinator package](2026-09-17-simplicity-review-findings.md#13-react-native-conformance-f08) |
| Medium | correctness defect | [13-react-native-conformance-F09: Close deadlines cannot interrupt several blocked exchanges](2026-09-17-simplicity-review-findings.md#13-react-native-conformance-f09) |
| High | correctness defect | [13-react-native-conformance-F10: Schema dispatch proof does not check the dispatch outcome](2026-09-17-simplicity-review-findings.md#13-react-native-conformance-f10) |
| High | correctness defect | [13-react-native-conformance-F11: Retention proof calls a cursor floor-equal without proving the floor](2026-09-17-simplicity-review-findings.md#13-react-native-conformance-f11) |
| Medium | correctness defect | [13-react-native-conformance-F12: Queued-schema proof derives its expected base from the client under test](2026-09-17-simplicity-review-findings.md#13-react-native-conformance-f12) |
| Medium | correctness defect | [13-react-native-conformance-F13: Queue trace combination removes evidence of missing terminal observations](2026-09-17-simplicity-review-findings.md#13-react-native-conformance-f13) |
| Low | contract decision | [13-react-native-conformance-F14: Exact sidecar exchange counts duplicate the scenario state machine](2026-09-17-simplicity-review-findings.md#13-react-native-conformance-f14) |

## Reference model and modelrunner

| Severity | Classification | Finding |
| --- | --- | --- |
| Medium | Behavior-preserving cleanup, deferred under the approved R3 replacement contract | [14-reference-model-F01: Workload preparation requires a complete protocol interpreter](2026-09-17-simplicity-review-findings.md#14-reference-model-f01) |
| High | Correctness defect in the reference model. This is not a claim about production server behavior | [14-reference-model-F02: WAL effects and pull hydration disagree on valid effect shapes](2026-09-17-simplicity-review-findings.md#14-reference-model-f02) |
| High | Correctness defect in the reference model's client boundary | [14-reference-model-F03: Local apply obtains missing response evidence from mutable server state](2026-09-17-simplicity-review-findings.md#14-reference-model-f03) |
| Medium | Correctness defect with a behavior-preserving structural cleanup after correction | [14-reference-model-F04: Transport failures fabricate an HTTP response](2026-09-17-simplicity-review-findings.md#14-reference-model-f04) |
| Medium | Correctness defect in verification evidence, not a demonstrated production seed defect | [14-reference-model-F05: Seed verification does not bind artifact bytes to installed rows](2026-09-17-simplicity-review-findings.md#14-reference-model-f05) |
| Medium | Behavior-preserving cleanup | [14-reference-model-F06: Unsampled macro operations copy and normalize the complete state repeatedly](2026-09-17-simplicity-review-findings.md#14-reference-model-f06) |
| Medium | Correctness defect and behavior-preserving consolidation of clone ownership | [14-reference-model-F07: Two result-clone implementations omit the same nested mutable fields](2026-09-17-simplicity-review-findings.md#14-reference-model-f07) |
| Low | Behavior-preserving cleanup | [14-reference-model-F08: Runtime interfaces describe alternatives that cannot exist](2026-09-17-simplicity-review-findings.md#14-reference-model-f08) |
| Low | Behavior-preserving cleanup | [14-reference-model-F09: Unused helpers and obsolete options remain inside the frozen oracle](2026-09-17-simplicity-review-findings.md#14-reference-model-f09) |
| Medium | Correctness defect in proof, with removable duplicate checking logic | [14-reference-model-F10: WAL negative controls test a private checker that never judges model executions](2026-09-17-simplicity-review-findings.md#14-reference-model-f10) |
| Medium | Correctness defect with an unnecessary alternate decoding path | [14-reference-model-F11: Scalar canonicalization falls back after every strict-validation failure](2026-09-17-simplicity-review-findings.md#14-reference-model-f11) |

[Additional decisions and acceptance details](2026-09-17-simplicity-review-findings.md#area-14-reference-model).

## Authored scenarios and vectors

| Severity | Classification | Finding |
| --- | --- | --- |
| Low | Behavior-preserving cleanup | [15-scenario-corpus-F01: Delete unused vector source retention](2026-09-17-simplicity-review-findings.md#15-scenario-corpus-f01) |
| Low | Behavior-preserving cleanup | [15-scenario-corpus-F02: Remove duplicate-ID maps where strict ordering already proves uniqueness](2026-09-17-simplicity-review-findings.md#15-scenario-corpus-f02) |
| Medium | Correctness defect | [15-scenario-corpus-F03: Stop native workload validation after an invalid record bound](2026-09-17-simplicity-review-findings.md#15-scenario-corpus-f03) |
| Medium | Correctness defect | [15-scenario-corpus-F04: Remove obsolete endpoint classes from native CRUD acceptance](2026-09-17-simplicity-review-findings.md#15-scenario-corpus-f04) |
| Low | Contract decision. The change affects the authored scenario format and its consumers | [15-scenario-corpus-F05: Store each authored measurement parameter object once](2026-09-17-simplicity-review-findings.md#15-scenario-corpus-f05) |
| Medium | Correctness defect | [15-scenario-corpus-F06: Preserve explicit-empty state projections through JSON serialization](2026-09-17-simplicity-review-findings.md#15-scenario-corpus-f06) |
| Low | Behavior-preserving cleanup | [15-scenario-corpus-F07: Remove unused catalogs from the performance-item helper](2026-09-17-simplicity-review-findings.md#15-scenario-corpus-f07) |

## Verification framework and gates

| Severity | Classification | Finding |
| --- | --- | --- |
| High | Correctness defect | [16-verification-framework-F01: Soak capture rules require reconstructed HTTP exchanges](2026-09-17-simplicity-review-findings.md#16-verification-framework-f01) |
| High | Correctness defect | [16-verification-framework-F02: Random fault labels do not identify the fault that executes](2026-09-17-simplicity-review-findings.md#16-verification-framework-f02) |
| Medium | Behavior-preserving cleanup | [16-verification-framework-F03: Issue 49 tests maintain a test-only protocol implementation](2026-09-17-simplicity-review-findings.md#16-verification-framework-f03) |
| High | Correctness defect | [16-verification-framework-F04: Exact result parsing accepts skipped assertion descendants](2026-09-17-simplicity-review-findings.md#16-verification-framework-f04) |
| Medium | Behavior-preserving cleanup | [16-verification-framework-F05: Mutation controls have two registries and positional assertion identities](2026-09-17-simplicity-review-findings.md#16-verification-framework-f05) |
| Medium | Correctness defect | [16-verification-framework-F06: Separate JSON validation paths have conflicting guarantees](2026-09-17-simplicity-review-findings.md#16-verification-framework-f06) |
| Medium | Contract decision | [16-verification-framework-F07: Performance freezing reimplements JavaScript object-order behavior](2026-09-17-simplicity-review-findings.md#16-verification-framework-f07) |
| Medium | Contract decision | [16-verification-framework-F08: Validators repeat release catalogs and dependency versions as source constants](2026-09-17-simplicity-review-findings.md#16-verification-framework-f08) |
| Medium | Contract decision | [16-verification-framework-F09: Proof cardinality rules multiply overlapping requirements](2026-09-17-simplicity-review-findings.md#16-verification-framework-f09) |
| Medium | Behavior-preserving cleanup | [16-verification-framework-F10: Observer SQL scanning repeats a boundary already enforced by construction and PostgreSQL](2026-09-17-simplicity-review-findings.md#16-verification-framework-f10) |
| Medium | Behavior-preserving cleanup | [16-verification-framework-F11: Soak operations store unused duplicate instructions](2026-09-17-simplicity-review-findings.md#16-verification-framework-f11) |
| Low | Behavior-preserving cleanup | [16-verification-framework-F12: Execution and performance APIs retain unused members](2026-09-17-simplicity-review-findings.md#16-verification-framework-f12) |
| Low | Behavior-preserving cleanup | [16-verification-framework-F13: Soak duplicates the invariant violation ordering rule](2026-09-17-simplicity-review-findings.md#16-verification-framework-f13) |
| Medium | Behavior-preserving cleanup | [16-verification-framework-F14: Mutant patches maintain generated SQL and source-location comments by hand](2026-09-17-simplicity-review-findings.md#16-verification-framework-f14) |
| Medium | Correctness defect | [16-verification-framework-F15: Wire fault ownership cannot cancel a request waiting for response headers](2026-09-17-simplicity-review-findings.md#16-verification-framework-f15) |
| Low | Correctness defect | [16-verification-framework-F16: Soak opens a journal before validation installs cleanup](2026-09-17-simplicity-review-findings.md#16-verification-framework-f16) |
| Low | Behavior-preserving cleanup | [16-verification-framework-F17: Runtime invariant checkers embed individual probe shapes](2026-09-17-simplicity-review-findings.md#16-verification-framework-f17) |

## Build, release, and packaged consumers

| Severity | Classification | Finding |
| --- | --- | --- |
| High | Correctness defect | [17-build-release-F01: Client smoke infers transfer success from local queue counts](2026-09-17-simplicity-review-findings.md#17-build-release-f01) |
| High | Correctness defect | [17-build-release-F02: Client package evidence hashes sealed inputs instead of the consumed copies](2026-09-17-simplicity-review-findings.md#17-build-release-f02) |
| High | Correctness defect | [17-build-release-F03: Local lifecycle targets terminate processes they do not own](2026-09-17-simplicity-review-findings.md#17-build-release-f03) |
| Medium | Correctness defect | [17-build-release-F04: Adapter startup has two readiness owners and loses cleanup authority](2026-09-17-simplicity-review-findings.md#17-build-release-f04) |
| Medium | Correctness defect | [17-build-release-F05: Several required test gates accept native runner success without result integrity](2026-09-17-simplicity-review-findings.md#17-build-release-f05) |
| Medium | Behavior-preserving cleanup | [17-build-release-F06: Private Maven classification has no workflow producer](2026-09-17-simplicity-review-findings.md#17-build-release-f06) |
| Medium | Correctness defect with behavior-preserving deletion of unreachable paths | [17-build-release-F07: Obsolete support routes include a mobile substitute for server proof](2026-09-17-simplicity-review-findings.md#17-build-release-f07) |
| Low | Behavior-preserving cleanup | [17-build-release-F08: Unconsumed verification code adds a false maintenance surface](2026-09-17-simplicity-review-findings.md#17-build-release-f08) |
| Medium | Behavior-preserving cleanup | [17-build-release-F09: React Native focused targets duplicate the same execution recipe](2026-09-17-simplicity-review-findings.md#17-build-release-f09) |
| Medium | Behavior-preserving cleanup, subject to evidence reuse verification | [17-build-release-F10: Recovery repeats every package cell before resuming publication](2026-09-17-simplicity-review-findings.md#17-build-release-f10) |
| Medium | Correctness defect | [17-build-release-F11: The scheduled aggregate omits broad integration mutation](2026-09-17-simplicity-review-findings.md#17-build-release-f11) |
| Medium | Behavior-preserving cleanup | [17-build-release-F12: Candidate server jobs serialize unrelated hosted installations](2026-09-17-simplicity-review-findings.md#17-build-release-f12) |
| Medium | Correctness defect with a simpler official-tool replacement | [17-build-release-F13: A handwritten OpenPGP parser validates shape, not signatures](2026-09-17-simplicity-review-findings.md#17-build-release-f13) |
| Low | Correctness defect | [17-build-release-F14: Duplicated support-resolution validation has already diverged](2026-09-17-simplicity-review-findings.md#17-build-release-f14) |
| Medium | Behavior-preserving cleanup | [17-build-release-F15: Registry polling repeatedly downloads already verified distributions](2026-09-17-simplicity-review-findings.md#17-build-release-f15) |
| Medium | Correctness defect in verification | [17-build-release-F16: Compound negative compilation probes do not identify the failed boundary](2026-09-17-simplicity-review-findings.md#17-build-release-f16) |
| Low | Contract decision | [17-build-release-F17: Current-stable policy requires a product decision about release availability](2026-09-17-simplicity-review-findings.md#17-build-release-f17) |

## Historical plans and reports

| Severity | Classification | Finding |
| --- | --- | --- |
| Low | Behavior-preserving cleanup | [18-history-plans-F01: Some archived task records still present obsolete release instructions as active work](2026-09-17-simplicity-review-findings.md#18-history-plans-f01) |
| Low | Behavior-preserving cleanup | [18-history-plans-F02: The original production audit does not link its later refutations](2026-09-17-simplicity-review-findings.md#18-history-plans-f02) |
| Low | Behavior-preserving cleanup | [18-history-plans-F03: A hand-maintained R3 category summary disagrees with its rows](2026-09-17-simplicity-review-findings.md#18-history-plans-f03) |

## Product documentation and requirements

| Severity | Classification | Finding |
| --- | --- | --- |
| High | Contract decision | [19-product-docs-F01: Repeated normative definitions disagree on durable finality and error handling](2026-09-17-simplicity-review-findings.md#19-product-docs-f01) |
| Medium | Correctness defect in documentation | [19-product-docs-F02: The client README describes a removed capture and queue model](2026-09-17-simplicity-review-findings.md#19-product-docs-f02) |
| Medium | Contract decision | [19-product-docs-F03: Catalog validation still requires one separate negative control for every requirement](2026-09-17-simplicity-review-findings.md#19-product-docs-f03) |
| Medium | Contract decision for removing frozen policy-value locks. Consolidating identical checks alone can preserve behavior | [19-product-docs-F04: Support and performance policy values have handwritten validator copies](2026-09-17-simplicity-review-findings.md#19-product-docs-f04) |
| Low | Behavior-preserving cleanup | [19-product-docs-F05: Every docs page loads Mermaid even though no authored page uses it](2026-09-17-simplicity-review-findings.md#19-product-docs-f05) |
| Low | Correctness defect | [19-product-docs-F06: The Markdown anchor scanner accepts headings hidden inside a longer code fence](2026-09-17-simplicity-review-findings.md#19-product-docs-f06) |
| Low | Behavior-preserving documentation cleanup | [19-product-docs-F07: Current release documents disagree on npm latest promotion](2026-09-17-simplicity-review-findings.md#19-product-docs-f07) |
| Medium | Contract decision | [19-product-docs-F08: Portable seed export requires stronger snapshot isolation than its documented consistency argument establishes](2026-09-17-simplicity-review-findings.md#19-product-docs-f08) |
| Medium | Contract decision | [19-product-docs-F09: Worker isolation forbids unrelated non-superuser replication logins across the cluster](2026-09-17-simplicity-review-findings.md#19-product-docs-f09) |
| Medium | Contract decision | [19-product-docs-F10: The mandatory schema rollout prescribes dual representations for every semantic transition](2026-09-17-simplicity-review-findings.md#19-product-docs-f10) |
| Low | Behavior-preserving cleanup | [19-product-docs-F11: A Markdown helper exists only to test itself](2026-09-17-simplicity-review-findings.md#19-product-docs-f11) |

## Local instructions and authored files

| Severity | Classification | Finding |
| --- | --- | --- |
| Low | Correctness defect in instructions | [20-local-authored-F01: Current instructions direct workers to a removed release target](2026-09-17-simplicity-review-findings.md#20-local-authored-f01) |

[Additional decisions and acceptance details](2026-09-17-simplicity-review-findings.md#area-20-local-authored).
