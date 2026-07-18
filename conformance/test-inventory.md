# Test Inventory

The former handwritten coverage inventory has been retired. Manual labels and lists of test names can become stale, can confuse fixture presence with semantic proof, and cannot certify an exact release artifact.

The existing fixture corpus and automated tests remain engineering assets. Their presence, or a passing development test run, is not certification for v0.3.0.

Phase 3 will introduce generated evidence as the authoritative inventory. That evidence will be produced from actual executions and will bind stable requirement, support-cell, and scenario IDs to the candidate commit, exact resolved environments, artifact hashes, proof types, outcomes, logs, negative controls, and replay data.

Release eligibility will be computed by tooling from validated, immutable evidence. It will not be maintained by editing coverage or readiness labels in this file. Missing evidence, skipped required tests, unexplained flakes, and artifact mismatches must block the release gate.

Until generated evidence exists, consult the authored requirements and support matrix for obligations only. Do not interpret them, this notice, or the current test suite as a release certification claim.
