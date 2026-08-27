# Releasing Synchro

Synchro releases are promoted from a verified release candidate. The specification and authored conformance contract define what a candidate must prove. Generated evidence records what a particular candidate actually proved.

## v0.3.0 Verification Matrix

The public v0.3.0 matrix is:

| Surface | Tier 1 required tracks |
| --- | --- |
| Server | PostgreSQL 18 on `linux-x64` and `macos-arm64` extension architectures |
| Swift | iOS 16 and current stable iOS |
| Kotlin | Android API 24 and current stable Android API |
| React Native | React Native 0.83.x on current stable iOS and current stable Android |

Swift on macOS is Tier 2. It is built and tested in CI without certification. macOS remains an Apple toolchain and Swift scenario host.

PostgreSQL 14 through 17 are outside v0.3.0 verification. A current-stable track resolves to exact environment versions when the release candidate begins. Those versions belong in generated evidence, not in moving authored policy. A new Tier 1 cell requires named production demand that justifies its recurring evidence cost.

## Policy And Evidence

Authored files define obligations:

- the specification defines required behavior
- `conformance/requirements.json` defines required proof
- `conformance/support-matrix.json` defines required and excluded matrix cells
- scenarios define inputs and semantic expectations

These files do not record release outcomes. Test inventory prose, fixture presence, and a passing development workflow are not certification.

Generated evidence records an execution against one candidate. It identifies the candidate, commit, exact environment, scenario and requirement IDs, artifact hashes, results, logs, and attestations. Evidence is immutable after generation. Release eligibility is computed by the release tooling and its exit status, not by editing a readiness field.

## Evidence Rules

Release evidence must resist circular validation and selective reporting:

- The specification is the behavioral authority. Current implementation behavior is not an expected result.
- Expected values must come from the normative contract or an independent reference model, not from the implementation under test.
- Implementation-generated fixtures and assertions that only check field presence do not prove semantics.
- Release-critical tests require meaningful negative controls or demonstrated mutants.
- Happy-path release evidence must exercise real packaged components. Mocks cannot replace the real PostgreSQL, adapter, native client, or React Native path.
- Internal metadata seeding cannot count as black-box proof.
- Every required test must execute. A skipped, ignored, filtered, or quarantined required test blocks promotion.
- A flaky required test blocks promotion until its cause is diagnosed and fixed. Retries cannot turn an unexplained failure into a pass.
- A rerun never erases the original result. It must be linked to the failed run and record the reason, diagnosis, corrective change, candidate identity, and artifact hashes. An infrastructure-only rerun may use unchanged artifacts, but both runs remain in evidence.
- Randomized failures must record a replayable seed. A production defect must become a permanent minimized scenario.

The release standard is zero unresolved critical or high findings, zero skipped required tests, and zero unexplained flaky tests. Security findings may use access-controlled evidence while unresolved. Public release material reports scope and disposition without publishing instructions that would enable exploitation.

## Build Once And Promote Exact Artifacts

1. Assign an RC candidate ID to an unchanged commit.
2. Build final-version `0.3.0` packages and archives once in staging.
3. Record hashes before testing.
4. Install and test those exact files in clean consumer environments across every required matrix cell.
5. Generate and review the immutable evidence bundle.
6. Tag the unchanged certified commit only after approval.
7. Publish the exact tested bytes and verify that published hashes match tested hashes.

Any source, package metadata, build input, or artifact change invalidates the candidate. The replacement is a new candidate with new artifacts and complete evidence. A release must never be rebuilt after verification and represented as the tested output.

## Promotion Gate

Promotion requires all authored requirements and required support cells to have valid generated evidence for the same candidate and exact artifacts. The gate must fail for missing evidence, skips, unexplained flakes, unresolved critical or high findings, mutation survivors that invalidate a required proof, or artifact hash mismatch.

Generated RC evidence becomes authoritative when the verification pipeline is introduced. Until then, existing tests and `conformance/test-inventory.md` describe engineering assets only and do not certify v0.3.0.

Repository automation fails closed for v0.3 publication until the verified RC evidence and exact-artifact promotion workflow replaces that guard.
