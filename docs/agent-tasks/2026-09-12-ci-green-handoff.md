# CI Green Handoff — 2026-09-12

## THE GOAL

Ship v0.3.0 as a release candidate the user can actually use.

Four steps, in order. Do not start one before the previous step is proved.

1. **Every CI job green on one commit.** All ten jobs, one run, status `completed`.
   This is the only active blocker. Nothing else proceeds until it holds.
2. **Close nine issues:** #37, #46, #47, #49, #50, #51, #52, #53, #54. Each closing
   comment cites the exact commit its evidence ran on.
3. **Merge to `r2-integration`**, run the `rc-check-pg18` support cells, write release
   notes.
4. **Tag v0.3.0.**

### Definition of done for step 1

One run, one commit, ten jobs green. A job is green only in a run whose status is
`completed`. Zero failures, zero skipped tests, zero unexplained flaky tests. A retry
that turns a failure green without a named cause is not a pass.

### What the goal is not

- Not "pass 85 things." The 85 requirement proofs in issue #49 are finished and are not
  current work.
- Not a green client gate alone, and not a green mutation gate alone. Partial gates have
  been mistaken for progress repeatedly.

### Branch

`scratch/lockdesign-activation-claim`.

### Standing constraint

Regression is the failure mode that has cost the most time on this branch. Work that
fixes one scenario while breaking another is not progress. Before any push, compare the
subtest PASS/FAIL list against the previous completed run. See rule 2 below.

## State at handoff

HEAD `c794508`. Last completed run `34693998505` on `c794508`:

| Job | Result |
|---|---|
| swift-sdk | FAIL — only `pending-cycle` |
| kotlin-sdk | FAIL — same shape |
| conformance-pg18 | FAIL — its fixes are uncommitted |
| conformance-quality | FAIL — `lint-conformance`, new this run |
| metadata, quality, docs, server, linux-pgrx-runtime, rn-android | PASS |

Twelve files are uncommitted. They hold the whole `conformance-pg18` fix and are the
work of the running Astra seat. No CI run has ever contained them.

## Do not repeat these

1. Report a job result only from a run whose status is `completed`. A cancelled or
   in-progress run has no verdict.
2. A green unit gate is not evidence for an engine change. The authored scenarios in
   `conformance/scenarios/` are the contract authority. Before pushing, diff the subtest
   PASS/FAIL list of the previous completed run against the new one. Any PASS to FAIL is
   a regression regardless of unit results.
3. Before changing a helper, count its callers. `951553d` put scope-removal policy inside
   `removeLocalRowIfUnreferenced`, which has four callers, and broke two scenarios.
4. Session timestamps do not prove a seat is idle. A seat blocked in one long `bash` call
   looks idle for the length of that call. Prove liveness from `pgrep -fl "opencode run"`,
   a tool part in `status: running`, and the process it waits on.
5. PostgreSQL never runs on the Mac. Extension-backed scenarios run on the desktop or in
   CI only. The desktop serializes on `/tmp/synchro-pg.lock`.

## Open problem 1 — `pending-cycle` on both SDKs

Assertion: `conformance/scenarios/pending_cycle_native.go:176`, a whole-struct
`reflect.DeepEqual`, so any single field mismatch yields the same message.

History tonight, all from completed runs:

| Run | Commit | pending-cycle | multi-scope-provenance | seeded-empty-startup |
|---|---|---|---|---|
| 34660542844 | 70a1737 | FAIL | PASS | PASS |
| 34677034532 | 7334d2d | PASS | FAIL | FAIL |
| 34686502877 | bf70059 | PASS | FAIL | FAIL |
| 34691341805 | 20174fe | FAIL | PASS | PASS |
| 34693998505 | c794508 | FAIL | PASS | PASS |

`pending-cycle` and the other two traded places exactly with the shared-scope
registration. `c794508` was meant to satisfy both by restoring the assignment only when
the controller itself removed it, and it did not. The registration either is not firing
on the restore step, or restoring is not sufficient to rebuild the expected state.

Next step: get the actual field mismatch instead of the generic message. Add a temporary
diff of `expectedCleaned` against `cleaned` in `ValidatePendingCycleNativeEvidence`, run
the scenario on the desktop when the PG lock is free, and read which field diverges.
Guessing at this assertion has already cost four CI cycles.

Relevant code:
- `conformance/blackbox/native_controller.go` — `ApplyStep`, case
  `model/set-client-assignments`, and the `defaultSharedScopeRemoved` field.
- `conformance/blackbox/process.go` — `RegisterDefaultSharedScope` runs
  `synchro_register_shared_scope('cf:global', false)`. The fixture registers it with
  `true` in `conformance/blackbox/testdata/register-diagnostic.sql:4`. That flag mismatch
  is unresolved and is a live suspect.
- `synchro_register_shared_scope` in `extensions/synchro-pg/src/portable_seed.rs:138`
  appends the scope to every active client and bumps every scope set version. That global
  effect is why an unconditional call broke the other two scenarios.

## Open problem 2 — `conformance-quality`

`make lint-conformance` started failing this run, having passed in every earlier one.
`d75bf24` is the only plausible cause; it makes the suite parser reject a run that
executes zero matching tests. If some lint-adjacent target silently matched nothing
before, it now fails loudly, which is correct behavior exposing a real hole. Reproduce
with `make lint-conformance` and read the first error.

## Open problem 3 — `conformance-pg18`

The seat owns this. Two mutants that previously survived are now killed:
`issue49-remaining-operational-redaction` and `issue49-remaining-retention-floor`.

Root cause found earlier: a DDL mutant must patch the generated
`extensions/synchro-pg/sql/synchro_pg--0.3.0.sql` as well as `lib.rs`, because
`CREATE EXTENSION` installs from the SQL file. Patching only Rust leaves the mutant
unreachable, so it survives. Four patches were fixed for this.

Remaining blocker is not a survivor. Run `A9H90S` reached 53 of ~76 mutants with zero
survivors and stopped because `issue49-remaining-projection-bootstrap` passed as
*baseline* and failed as *post-baseline* on identical code:

```
real_issue49_remaining_semantics_test.go:1490:
  insert projection-bootstrap catch-up row: source mutation failed
--- FAIL: TestRealIssue49RemainingSemantics/assertion#29 (301.16s)
```

301 s suggests a context deadline. Desktop load was 12 on 24 cores with 179 GB disk and
46 GB RAM free, so resource starvation was ruled out. Cause is still unknown. A gate that
returns different results for the same input is itself a defect under the repo's
gate-integrity rules and must not be papered over with a retry.

Gate logs live at `~/synchro-cifix/n/.ignore/mutant-logs/` on the desktop. A directory
survives only when that run failed. Find survivors with:

```sh
for f in "$D"/*-mutant.parser.out; do
  v=$(tr -d '\r\n' < "$f")
  [ "$v" = target_semantic_test_failure ] || echo "$(basename "$f") -> $v"
done
```

A mutant must read `target_semantic_test_failure`. Baseline and post-baseline must read
`target_pass`.

## Running processes

- Astra seat, opencode, session `ses_f6c477352ffeB6G6RPgpAH3QSd`, model `gpt-6-astra`,
  `--auto`. Owns the twelve uncommitted files and `conformance-pg18`. Resume with
  `opencode run --auto --agent dispatch-astra-medium --attach http://localhost:4096
  --session <id>`. Always pass `--agent`, or it silently downgrades to the default model.
- `/tmp/keepalive.sh` restarts the seat when genuinely idle and exits when CI is green.
- Monitor `b6h82vt7g` reports new commits and completed runs.
- `opencode serve` on port 4096.

## Sequence once green

1. Commit and push the seat's twelve files after the gate validates them.
2. One full run, all ten jobs green on one commit.
3. Capture gate receipts, `make evidence`, `make coverage-report`.
4. Close #37, #46, #47, #49, #50, #51, #52, #53, #54 with the closing-comment template in
   `AGENTS.md`, each citing the commit its evidence ran on.
5. Merge to `r2-integration`, run `rc-check-pg18` support cells, release notes, tag.

Not started: #40 (React Native 1.85 GB RSS), #32 (docs dependencies).
