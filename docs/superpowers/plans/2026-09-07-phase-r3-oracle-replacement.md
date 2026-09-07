# Phase R3 Child Plan: Oracle Replacement

This is the approved execution plan for Phase R3.
The parent definition is `docs/superpowers/plans/2026-08-21-v0.3.0-remediation-and-debloat.md`, section `# Phase R3: Oracle Replacement (post-0.3.0)` at line 162.
The user locked this phase breakdown on 2026-09-07.
`.r3-completion-tracker.md` is the only editable R3 progress tracker.

## Parent Coverage Map

The parent R3 section contains five checkboxes.
Every checkbox maps to exactly one phase below.
No parent requirement is unmapped.

| Parent checkbox | Phase |
|---|---|
| Freeze `conformance/reference/` and `conformance/modelrunner/` | R3.0 |
| Build the soak divergence checker as the invariant-engine core | R3.2 |
| Migrate the remaining reference-model checks after release | R3.4 |
| Drive the invariant engine from the seeded randomized soak | R3.3 |
| Delete `reference/` and `modelrunner/` with negative controls | R3.5 |

Phase R3.1 is an addition to the parent text.
It exists because checkbox five requires that the engine "demonstrably covers" the oracle checks.
A demonstration requires an enumerated contract.
R3.1 produces that contract.

## Ship Dependency

The parent gates R3.4 and R3.5 behind the v0.3.0 release: "Do not start the deletion work before v0.3.0 ships."
The release work itself is not R3 scope.
The master plan names it "Phase 6: Certify And Promote v0.3.0" in `docs/superpowers/plans/2026-07-17-synchro-v0.3.0-verified-rc.md` at line 617, as amended by the remediation plan.
That work remains recorded and required: `rc-check-pg18` implementation, one-shot candidate artifact staging with hashes, the executed 72-hour soak, fault injection, non-superuser isolation scenarios, canaries, a real consumer integration, the evidence bundle, the tag, publication, and the `main` replacement.
R3.2 and R3.3 build the invariant engine and the soak driver that the release certification executes.
Nothing is built twice, and nothing from the certification scope is dropped by this plan.

## Freeze Finding

The parent freeze took effect on 2026-08-21.
Two commits grew the frozen directories after that date.

- `a572c13` (2026-08-21) checkpointed about 3,000 lines of in-flight phase-5 work into `modelrunner/` on the day the parent plan was authored.
- `b2c5b8e` (2026-08-28) added about 90 lines for the R2 model-gate rescope that the approved R2 corpus design required.

Resolution: the freeze holds strictly from 2026-09-07.
The R3.1 inventory baselines against the repository state of 2026-09-07, so both additions are inside the deletion contract.

# Phase R3.0: Standing Freeze

Not a work phase.
A constraint that stays active until R3.5 completes.

- No commit may grow `conformance/reference/` or `conformance/modelrunner/`.
- No new work may depend on either directory.
- Deletions and defect fixes inside them remain permitted.

# Phase R3.1: Oracle Inventory And Deletion Contract

## Objective

Enumerate every check that `conformance/reference/` (30 files, 23,375 lines) and `conformance/modelrunner/` (16 files, 9,413 lines) perform.
Produce the table that later authorizes deletion.

## Tasks

- [ ] Enumerate every assertion, comparison, and invariant in `conformance/reference/`.
- [ ] Enumerate every assertion, comparison, and invariant in `conformance/modelrunner/`.
- [ ] Assign each check one target: an invariant family, an existing proof home with its `file:line`, or `oracle-internal`.
- [ ] An `oracle-internal` check validates only the oracle's own inputs, registries, or run lifecycle. It is deleted with the oracle and needs no migration.
- [ ] Flag every remaining check that verifies sync semantics but fits no invariant family and no proof home as an engine gap.
- [ ] Record the required negative control for each migrated check.
- [ ] Record every consumer that currently depends on the two directories.

## Output

Two committed contract tables beside this plan, one per directory, with these columns: check identifier, source `file:line`, verified behavior, target, negative-control requirement.
The tracker holds the category counts and points to the tables.

## Exit Gate

- Every exported check in both directories appears in the table.
- Every table row names a target or an explicit engine gap.
- An independent review pass confirms the enumeration has no omissions.

# Phase R3.2: Invariant-Engine Core

## Objective

Build the permanent invariant engine that the soak, the release certification, and the R3.4 migration all consume.

## Tasks

- [ ] Assemble the core from the existing verified digest and conservation validators, including `conformance/blackbox/syntheticproof/compare.go`.
- [ ] Implement the five invariant families: mutation conservation, cursor monotonicity, checksum convergence, scope isolation, and no state forks across process death.
- [ ] Land each invariant with a demonstrated mutant that it catches.
- [ ] Keep the engine independent of `reference/` and `modelrunner/`.

## Exit Gate

- All five invariant families execute against a real extension-backed run.
- Each family has a passing negative control.
- The engine has zero imports from the frozen directories.
- `make validation-check` membership passes on the integrated commit.

# Phase R3.3: Seeded Soak Driver

## Objective

Build the generative workload driver that runs the invariant engine continuously and replays every failure from its recorded seed.

## Tasks

- [ ] Build a seeded workload generator over connect, push, pull, rebuild, process death, and schema transitions.
- [ ] Record the seed for every run and every failure.
- [ ] Prove seed replay: an injected failure reproduces from its seed alone.
- [ ] Convert every soak failure into a permanent minimized scenario.
- [ ] Add a Make target for bounded local soak and a duration parameter for the 72-hour certification run.

## Exit Gate

- A bounded soak runs green through the Make target on the desktop host.
- A demonstrated injected defect is caught, replayed from its seed, and minimized into a scenario.
- Zero-skip enforcement covers the soak gate.

# Phase R3.4: Migration (post-tag)

Blocked until the v0.3.0 tag exists on a certified commit.

## Tasks

- [ ] Migrate the remaining reference-model checks into the engine, one invariant group at a time, per the R3.1 table.
- [ ] Land one negative control per migrated invariant.
- [ ] Update the R3.1 table row status as each group lands.

## Exit Gate

- Every R3.1 row is covered or has a recorded justified closure.
- Every migrated invariant has a passing negative control.
- `make validation-check` passes on the integrated commit.

# Phase R3.5: Deletion (post-tag, after R3.4)

## Tasks

- [ ] Delete `conformance/reference/` and `conformance/modelrunner/`.
- [ ] Delete every consumer shim recorded by the R3.1 consumer enumeration.
- [ ] Verify no dangling imports, dead fixtures, or orphaned Make targets remain.

## Exit Gate

- The R3.1 table shows every row covered before the deletion commit.
- `make validation-check` passes on the deletion commit.
- The mutation gates still fail closed after the deletion.
