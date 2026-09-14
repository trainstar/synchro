# Synchro Release Process Implementation Plan

**Date:** 2026-09-14
**Status:** Ready, except for approved performance limits
**Target:** `v0.3.0` and later releases

## Required Outcome

Release day has three maintainer actions:

1. Merge the release pull request.
2. Run the Release workflow on `dev`.
3. Approve the protected `release` environment.

Automation performs every build, test, publication, and public verification step.

`RELEASE.md` becomes the only human release procedure.

For this plan, "works" has a strict meaning:

- The supported contract passes on the exact published source and payloads.
- Supported platform boundaries pass packaged lifecycle tests.
- Approved user performance limits pass on controlled reference cells.
- Every public destination passes a clean install check.

No finite process can prove that unknown defects do not exist.

This process gives objective evidence for declared behavior. It does not claim an impossible absolute guarantee.

## Design Authority

Use these inputs in this order:

1. The supported public contract defines required behavior.
2. Data-loss, compatibility, security, performance, and distribution risks define proof.
3. Official platform and registry rules define publication controls.
4. Current code identifies migration work and deletion candidates.

Current tests, targets, schemas, plans, and workflows have no presumed design value.

Keep a current mechanism only when it is the smallest proof for one required risk.

Do not preserve a test because it is unique. Preserve it only when its required behavior is unique.

## Fixed Product Contract

| Topic | Decision |
|---|---|
| First supported release | `v0.3.0` |
| Supported wire protocol | Protocol 3 only |
| Other protocol values | One generic unsupported-version rejection |
| PostgreSQL | PostgreSQL 18 on Linux x64 |
| Direct binaries | Linux x64 |
| Public clients | Swift, Kotlin, and React Native |
| Swift distribution | Exact repository tag through Swift Package Manager |
| React Native iOS dependency | Tag-backed root `Synchro.podspec` |
| CocoaPods trunk | No standalone publication |
| Kotlin distribution | `fit.trainstar:synchro` through Maven Central |
| React Native distribution | `@trainstar/synchro-react-native` through npm |
| Historical versions | Keep all tags, assets, and package versions immutable |
| Fixed 72-hour soak | Delete it |
| Release timer | None |
| Build policy | Build each publishable payload once |
| Recovery | Resume identical payloads or issue a patch release |

## Four Release Gates

| Gate | Required evidence |
|---|---|
| 1. Candidate | Exact `dev` commit passes deterministic correctness and security CI |
| 2. Package | Sealed payloads pass lifecycle, boundary, and performance checks |
| 3. Publish | One approval permits immutable tag and registry publication |
| 4. Public | Clean consumers install, verify identity, and run minimal live smoke |

The release workflow does not repeat source tests from Gate 1.

The release workflow never skips a failed or missing publication destination.

## Testing Model

| Proof | Purpose | Cadence |
|---|---|---|
| Unit tests | Local deterministic rules and boundaries | Pull request and candidate CI |
| Contract vectors | Exact wire behavior | Pull request and candidate CI |
| Real integration | PostgreSQL, adapter, and native engine behavior | Candidate CI |
| Invariants | Safety across large state spaces | Candidate CI |
| Seeded random tests | Operation, conflict, retry, and restart order | Scheduled |
| Fault tests | Network, process, storage, and dependency failures | Scheduled |
| Mutation controls | Prove that critical assertions detect defects | Scheduled |
| Endurance | Leaks, races, backlog growth, and resource drift | Scheduled |
| Packaged lifecycle | Install, connect, push, pull, kill, and resume | Release |
| Performance | User workloads and resource limits | Release and scheduled |
| Public smoke | Registry and package consumption | After publication |

### Scheduled Does Not Mean a 72-Hour Soak

Scheduled validation is an automatic nightly or weekly workflow.

Random tests record their seeds. Endurance tests use a bounded workload and time limit.

No release waits for a fresh scheduled run or for elapsed soak time.

A confirmed failure becomes a minimized deterministic regression. That regression blocks release until its fix passes.

An unexplained failure cannot be dismissed as flaky. It needs a reproduced cause or an infrastructure fix.

### Edge Cases

Experts do not enumerate every possible sync sequence.

They combine authored examples, state invariants, generated sequences, fault injection, and production defect regressions.

Run full semantics once for each independent engine:

- PostgreSQL and `synchro-core`
- Swift
- Kotlin

React Native is a bridge. Test bridge parity and lifecycle behavior on iOS and Android.

Run package compatibility on the minimum and current supported client versions.

Do not run a platform cross-product. Do not certify PostgreSQL on macOS.

Client tests can use a separate Linux server through one adapter URL.

### Performance

Performance is a product contract. It is not an ad hoc release-day opinion.

Define five user workloads:

1. Initial synchronization
2. Incremental pull
3. Offline backlog push
4. Rebuild
5. Process termination and resume

Define the supported data size and concurrency for each workload.

Define only user-relevant limits:

- Completion latency
- Peak resident memory
- Local database growth
- Sustained sync throughput
- Server database round trips

Use one controlled reference cell for the server, Swift, and Kotlin.

Use real mobile devices for user-visible timing. Use deterministic counters for query and storage limits.

React Native uses native-engine results plus one bridge-overhead check per mobile platform.

The first supported release must pass approved absolute limits.

Later releases must also compare with the last supported release on the same reference cell.

Current measurements can inform diagnosis. They cannot define acceptable performance.

A limit failure blocks approval. A rerun can diagnose noise but cannot erase an unexplained failure.

**Open product input:** Numeric performance limits are not approved.

The implementation must measure representative workloads and obtain approval for each limit.

Do not invent limits from the current implementation or an arbitrary percentage.

## Public Distribution

| Surface | Published form | Public check |
|---|---|---|
| PostgreSQL extension | PG18 Linux x64 archive | Clean PG18 install |
| `synchrod-pg` | Linux x64 binary | Attestation, startup, and live smoke |
| `synchro-seed` | Linux x64 binary | Attestation and command smoke |
| Swift | Exact Git tag | Clean Swift Package Manager resolution |
| Apple pod metadata | Root tag-backed podspec | Clean Git-backed pod resolution |
| Kotlin | Signed Maven Central bundle | Clean Android dependency and build |
| React Native | Exact npm tarball | Clean npm install and native builds |

Do not publish conformance runners, portable seed data, verification receipts, or unsupported server binaries.

## Verified Transition Blockers

These facts were verified on 2026-09-14. They do not define the target design.

- `.github/workflows/release.yml` blocks `v0.3.*`.
- Missing registry credentials currently skip publication.
- npm and Maven publication currently rebuild payloads.
- `dev` has no ruleset or protected release environment.
- Secret scanning and push protection are disabled.
- npm already contains immutable `0.1.x` versions.
- Maven Central has no public `fit.trainstar:synchro` artifact.
- Maven account and namespace state require authenticated confirmation.
- Current performance budgets mostly count requests.
- The current benchmark accepts one implementation snapshot as its baseline.

## Implementation Work

### 1. Define the Supported Contract

**Files**

- Add `RELEASE.md`.
- Delete `RELEASING.md`.
- Update `README.md`, `AGENTS.md`, and `conformance/support-matrix.json`.
- Update support, consumption, conformance, and verification documentation.
- Replace `docs/src/content/docs/spec/07-release-verification.mdx`.
- Add `docs/src/content/docs/spec/08-performance.mdx`.
- Replace `conformance/performance/budgets.json`.

**Work**

1. Make `RELEASE.md` the only release procedure.
2. Put the three maintainer actions first.
3. Record every fixed product decision from this plan.
4. Define one support matrix with Linux x64 server support.
5. Define minimum and current client boundaries.
6. Define the five performance workloads and supported sizes.
7. Approve absolute performance limits from user needs.
8. Store those limits in one small machine-readable contract.
9. Keep protocol behavior in protocol specifications.
10. Keep release operation only in `RELEASE.md`.

**Done when**

- Support documents agree with the support matrix.
- Each performance limit has a user-facing reason.
- `make verify-contract` and `make docs-build` pass.

### 2. Establish the Protocol 3 Baseline

**Files**

- Swift database, change tracker, pull processor, and related tests
- Kotlin database, change tracker, push, pull, SQLite schema, and related tests
- `extensions/synchro-core/src/fingerprint.rs`
- Protocol documentation and vectors

**Work**

1. Replace preview local migrations with one current database baseline.
2. Reject a preview database explicitly.
3. Do not convert or discard preview pending writes.
4. Remove `legacy_blocked`, `legacy_import`, and `legacy_unsealed`.
5. Keep `blocked_by_predecessor`.
6. Remove Protocol 2 conversion behavior and documentation.
7. Rename current-vector helpers that use `legacy`.
8. Test new, current, and rejected preview databases.
9. Test generic rejection for every wire version other than 3.

**Done when**

- Swift and Kotlin tests pass.
- Production code contains no preview lifecycle state.
- Current Protocol 3 behavior still passes.

### 3. Rebuild Validation From Required Risks

**Files**

- Rewrite `.github/workflows/ci.yml`.
- Add `.github/workflows/scheduled-validation.yml`.
- Delete `.github/workflows/rn-ios-validation.yml`.
- Merge and delete separate CodeQL and dependency workflows.
- Update `Makefile`.
- Add one small static contract-to-proof map.

**Work**

1. Evaluate every current test against the supported contract.
2. Delete tests for unsupported or duplicate behavior.
3. Map each normative requirement to one proof target and cadence.
4. Add stable `CI / pull-request` and `CI / candidate` aggregate jobs.
5. Run candidate CI on every push to `dev`.
6. Include deterministic correctness, security, and structural performance guards.
7. Reject failed, skipped, filtered, and zero-test required gates.
8. Put seeded random, fault, mutation, capacity, and endurance tests in scheduled validation.
9. Record random seeds before execution.
10. Add small Make targets for Linux, Apple, Android, and React Native hosts.
11. Remove `validation-check`, `phase-5-check`, and the current `release-check`.

**Done when**

- Every supported behavior has one proof home.
- No test remains because of current-state precedent.
- Release does not rerun source semantic suites.

### 4. Build and Test Exact Payloads

**Files**

- Replace `scripts/rc-artifacts.py` with `scripts/release-artifacts.py`.
- Replace its tests.
- Update release Make targets.
- Replace `conformance/artifacts/inventory.json`.
- Simplify or replace `verification/packaged_smoke.py`.
- Update packaged consumer fixtures.

**Work**

1. Add `make release-stage VERSION=x.y.z`.
2. Add `make release-verify RELEASE_DIR=...`.
3. Stage the extension, adapter, seed tool, npm tarball, and Maven bundle.
4. Sign the Maven payloads during staging.
5. Create required Maven checksums and detached signatures.
6. Create one `release-manifest.json`.
7. Record source commit, CI run, destination, path, size, and SHA-256.
8. Create `SHA256SUMS` and one SPDX JSON SBOM.
9. Seal the candidate before package tests.
10. Reject missing, extra, renamed, or modified files.
11. Run lifecycle checks from sealed paths.
12. Run performance checks on the sealed payloads.

**Done when**

- A one-byte mutation fails verification.
- An unexpected file fails verification.
- Verification never rebuilds a payload.

### 5. Replace the Release Workflow

**File**

- Rewrite `.github/workflows/release.yml`.

**New release**

1. Use `workflow_dispatch` without release-mode or skip inputs.
2. Require the selected ref to be `dev` and the candidate to equal its current head.
3. Read and validate the version from the candidate.
4. Require zero open issues in the matching release milestone.
5. Require successful `CI / candidate` for the exact commit.
6. Fail on an existing public version.
7. Build, seal, and test payloads once.
8. Wait for the protected `release` environment.
9. Create the immutable tag and a draft GitHub release.
10. Publish the same payloads to GitHub, npm, and Maven Central.
11. Verify every public destination.
12. Finalize the GitHub release.

**Recovery**

1. Prefer rerunning failed jobs in the original run.
2. Permit a new dispatch only from the immutable release tag.
3. Require the tag, draft manifest, and source commit to agree.
4. Download and verify the original sealed payloads.
5. Skip only a destination that contains identical bytes.
6. Resume missing publication and verification.
7. Fail on any identity mismatch.

**Security**

- Default workflow permissions to `contents: read`.
- Scope write, OIDC, attestation, and issue permissions to required jobs.
- Make missing credentials fail.
- Keep the GitHub release draft until all public checks pass.

**Done when**

- Negative controls reject wrong commits, open milestones, changed payloads, tag conflicts, and missing credentials.
- A failed destination resumes without rebuilding.

### 6. Configure External Controls

**GitHub**

1. Protect `dev` with a ruleset.
2. Require a pull request, resolved conversations, and `CI / pull-request`.
3. Block force pushes and deletion.
4. Keep one audited administrator recovery path.
5. Require one review when two eligible maintainers exist.
6. Create a `release-signing` environment for Maven signing material.
7. Create a protected `release` environment for publication.
8. Enable secret scanning, push protection, and Dependabot security updates.
9. Pin actions, toolchains, runner families, and build containers.

**npm**

1. Configure trusted publishing for `.github/workflows/release.yml`.
2. Bind it to the `release` environment.
3. Use a GitHub-hosted runner and a supported pinned npm CLI.
4. Require two-factor authentication and disallow publication tokens.
5. Keep automatic provenance enabled.

**Maven Central**

1. Confirm the account and verify the `fit.trainstar` namespace.
2. Create a Portal user token and artifact-signing key.
3. Publish the signing public key.
4. Store credentials only in scoped environments.
5. Upload one sealed test bundle as `USER_MANAGED`.
6. Require Central to report `VALIDATED`.
7. Drop the test deployment without publication.
8. Use `AUTOMATIC` only after the GitHub approval gate.

Missing Maven setup blocks publication. It never produces a skipped success.

### 7. Delete Duplicate Machinery

Run a reference search before each deletion.

Delete these items after their replacements pass:

- `conformance/schemas/rc-candidate-lock-v1.schema.json`
- `conformance/schemas/rc-manifest-v2.schema.json`
- `conformance/schemas/ci-summary-v1.schema.json`
- `conformance/schemas/performance-budgets-v2.schema.json`
- `conformance/cmd/synchro-evidence/`
- `conformance/evidence/`
- Release-only receipt types in `conformance/execution/`
- `scripts/ci/build-phase-5-input.py`
- `scripts/ci/test_evidence_variables.py`
- `scripts/release-support-check.py`
- `scripts/rc-artifacts.py`
- `conformance/blackbox/integration/real_r1_benchmark_test.go`
- `conformance/blackbox/integration/testdata/r1-benchmark-baseline.json`
- Obsolete release tests and Make targets

Inline a shared helper when only one concrete consumer remains.

Keep or replace proof behavior only when the risk model requires it.

### 8. Migrate Work and Prove the Process

1. Map every incomplete R2 and R3 item to a GitHub issue.
2. Keep oracle-retirement data only when an open issue needs it.
3. Move required row data out of the plans directory.
4. Delete R2 and R3 trackers.
5. Delete superseded plans and agent reports.
6. Remove plan-specific instructions from `AGENTS.md`.
7. Run all focused Make targets on clean commits.
8. Run contract, documentation, workflow, and artifact negative checks.
9. Run complete candidate CI with zero failures and zero required skips.
10. Run the Release workflow to the approval gate.
11. Cancel and prove that no public state changed.
12. Complete the real `v0.3.0` release after product gates pass.
13. Verify GitHub, npm, Maven, Swift, and pod consumption.
14. Deprecate npm `0.1.x` as unsupported previews.
15. Mark old GitHub releases as unsupported previews.
16. Delete this plan after every completion condition passes.

Do not close a product issue because its old plan or report is removed.

## Completion Conditions

- One short `RELEASE.md` contains the complete maintainer procedure.
- One support matrix defines every supported boundary.
- Protocol 3 is the only supported protocol path.
- Exact payloads pass correctness, lifecycle, and approved performance limits.
- Repository and registry controls are active.
- Partial publication resumes with identical bytes.
- Public consumers install and run from every destination.
- No duplicate release evidence system, tracker, or release plan remains.

## Standards Basis

- [Google SRE: Release Engineering](https://sre.google/sre-book/release-engineering/)
- [Google SRE: Testing for Reliability](https://sre.google/sre-book/testing-reliability/)
- [Google SRE: Implementing SLOs](https://sre.google/workbook/implementing-slos/)
- [Semantic Versioning 2.0.0](https://semver.org/)
- [SLSA Build Track](https://slsa.dev/spec/v1.2/build-track-basics)
- [GitHub deployment environments](https://docs.github.com/en/actions/how-tos/deploy/configure-and-manage-deployments/manage-environments)
- [GitHub artifact attestations](https://docs.github.com/en/actions/how-tos/secure-your-work/use-artifact-attestations/use-artifact-attestations)
- [npm trusted publishing](https://docs.npmjs.com/trusted-publishers/)
- [Maven Central deployment bundles](https://central.sonatype.org/publish/publish-portal-upload/)
- [Maven Central Publisher API](https://central.sonatype.org/publish/publish-portal-api/)
- [Android benchmarks in CI](https://developer.android.com/topic/performance/benchmarking/benchmarking-in-ci)
