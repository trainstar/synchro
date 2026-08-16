# Phase 3 Contract Repair Diagnosis

## Mode

Findings only. Do not change a repository file.

## Authority

Read these files completely:

- `AGENTS.md`
- `docs/superpowers/plans/2026-07-17-synchro-v0.3.0-verified-rc.md`
- `docs/superpowers/plans/2026-07-19-phase-3-independent-verification.md`
- `/tmp/trainstar-synchro-phase3-ownership-proposal.md`

Protocol 3 behavior is fixed. Do not add Protocol 4 behavior.

## Task

Give the orchestrator exact evidence for the five contract repairs in the ownership proposal.

1. Verify the ten proposed protocol scenario IDs for uniqueness and stable naming.
2. Define the minimum closed operation names and typed payload fields that the 24 scenarios need.
3. Verify removal of the PostgreSQL cells from the two native-profiler measurements.
4. Derive exact finite workload values from current limits, plans, tests, and supported development capacity.
5. Define typed initial-state operations without direct effect, cursor, checkpoint, fence, or ledger seeding.

For each workload value, cite the source constraint and explain why smaller values cannot prove the stratum.

Do not select a value without repository evidence. Report a missing decision if evidence cannot select one value.

## Output

Write `/tmp/trainstar-synchro-phase3-repair-evidence.md`.

Include:

- Exact accepted and rejected identifiers.
- Exact operation names and JSON payload fields.
- Exact workload parameters and boundary values.
- Exact catalog edits and digest effects.
- Package ownership and import direction.
- Contradictions with `file:line` evidence.

Do not edit code. Do not use GitHub, the network, a database, or Git.
