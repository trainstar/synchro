# Phase 3 Scenario Ownership Diagnosis

## Mode

Findings only. Do not change a repository file.

## Authority

Read these complete files before the diagnosis:

- `AGENTS.md`
- `docs/superpowers/plans/2026-07-17-synchro-v0.3.0-verified-rc.md`
- `docs/superpowers/plans/2026-07-19-phase-3-independent-verification.md`
- `/tmp/trainstar-synchro-phase3-map.md`

Use only the accepted Protocol 3 ADRs, normative specifications, and authored catalogs as semantic sources.

Do not design Protocol 4.

## Task

Propose the smallest exact authored ownership contract for the ten initial protocol scenarios and fourteen performance scenarios in Phase 3.

The proposal is not authoritative. The orchestrator will review every selection.

For each scenario, give:

- One stable scenario ID.
- The minimum unique requirement IDs that it owns.
- The exact proof obligations.
- The exact applicable support cells.
- The exact assertion ownership tuples.
- The exact requirement-owned fault and negative-control pair.
- The performance budget, measurement, vector, artifact, Make target, and argument bindings.
- The ordered reference operations and payload facts.
- The semantic expected state and assertion predicates.

Do not assign one requirement to two scenarios. Do not add a requirement that the scenario does not prove.

If the existing catalog cannot supply a required unique requirement or control, give exact `file:line` evidence. Propose the minimum contract repair, but do not make it.

## Runner Boundary

Review this orchestrator proposal:

- Add `conformance/modelrunner`.
- `modelrunner` imports `conformance/scenarios` and `conformance/reference`.
- `scenarios` and `reference` do not import `modelrunner`.
- `Run` applies scenario setup and steps through `reference.Model.Apply` and evaluates closed semantic expectations.

Report a better acyclic boundary only if this one has a concrete defect.

## Output

Write the complete nonbinding report to `/tmp/trainstar-synchro-phase3-ownership-proposal.md`.

Return only the path, totals, contradictions, and validation commands.

Do not use the network, GitHub, a database, a deployment, or production packages.
