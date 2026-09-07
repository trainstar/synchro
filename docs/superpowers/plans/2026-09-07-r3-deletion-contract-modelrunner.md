# R3.1 modelrunner final deletion-contract table

**Count: 312 total rows.** `mutation-conservation`: 25 `cursor-monotonicity`: 32 `checksum-convergence`: 33 `scope-isolation`: 22 `no-state-forks`: 15 `proven-elsewhere`: 10 `oracle-internal`: 162 `ENGINE-GAP`: 13.

The supplied review lists **312 total rows**, not 603. The merged table therefore contains 312 rows and reconciles to the supplied category counts, while 603 cannot be reconciled from these two inputs.

| check-id | source | verified behavior | target | negative control |
|---|---|---|---|---|
| MOD-001 | `conformance/modelrunner/macro.go:43` | Rejects a workload payload that is not JSON. | oracle-internal | n/a |
| MOD-002 | `conformance/modelrunner/macro.go:47` | Rejects a workload without a nonempty profile. | oracle-internal | n/a |
| MOD-003 | `conformance/modelrunner/macro.go:51` | Rejects a workload profile outside the closed macro set. | oracle-internal | n/a |
| MOD-004 | `conformance/modelrunner/macro.go:77` | Enables topology provenance operations only for fanout two or more and a client with two assignments. | oracle-internal | Remove one assigned scope while retaining the provenance rebuild operations. |
| MOD-005 | `conformance/modelrunner/macro.go:96` | Requires a deterministic assigned client before appending provenance rebuild operations. | oracle-internal | Remove the selected client's assignment. |
| MOD-006 | `conformance/modelrunner/macro.go:103` | Requires both named provenance scopes to remain assigned. | oracle-internal | Unassign `scope-b` while retaining its rebuild sequence. |
| MOD-007 | `conformance/modelrunner/macro.go:123` | Requires each generated provenance operation to satisfy the closed scenario registry. | oracle-internal | n/a |
| MOD-008 | `conformance/modelrunner/macro.go:134` | Requires each named workload string field. | oracle-internal | n/a |
| MOD-009 | `conformance/modelrunner/macro.go:138` | Requires each named workload string value to be nonempty. | oracle-internal | n/a |
| MOD-010 | `conformance/modelrunner/runner.go:74` | Rejects a run without a context. | oracle-internal | n/a |
| MOD-011 | `conformance/modelrunner/runner.go:77` | Rejects a run without a model. | oracle-internal | n/a |
| MOD-012 | `conformance/modelrunner/runner.go:80` | Stops a run when its context has ended. | oracle-internal | n/a |
| MOD-013 | `conformance/modelrunner/runner.go:83` | Requires a fresh Protocol 3 model before setup. | oracle-internal | Prepopulate one stream transaction before the run. |
| MOD-014 | `conformance/modelrunner/runner.go:86` | Requires exactly one model setup operation. | oracle-internal | n/a |
| MOD-015 | `conformance/modelrunner/runner.go:90` | Requires setup to install the current model contract. | oracle-internal | n/a |
| MOD-016 | `conformance/modelrunner/runner.go:93` | Requires setup to be a valid closed operation. | oracle-internal | n/a |
| MOD-017 | `conformance/modelrunner/runner.go:116` | Stops before a step when its context has ended. | oracle-internal | n/a |
| MOD-018 | `conformance/modelrunner/runner.go:132` | Requires deterministic replay, wire expectations, and predicates before a normal run passes. | oracle-internal | Alter one replayed final snapshot while preserving the first run. |
| MOD-019 | `conformance/modelrunner/runner.go:168` | Rejects an invalid authored step operation. | oracle-internal | n/a |
| MOD-020 | `conformance/modelrunner/runner.go:175` | Rejects an operation outside the closed scenario registry. | oracle-internal | n/a |
| MOD-021 | `conformance/modelrunner/runner.go:191` | Requires every configured sample to target an expanded operation. | oracle-internal | n/a |
| MOD-022 | `conformance/modelrunner/runner.go:197` | Requires configured samples to target distinct expanded operations. | oracle-internal | n/a |
| MOD-023 | `conformance/modelrunner/runner.go:208` | Requires every expanded operation to resolve its dependent input. | oracle-internal | n/a |
| MOD-024 | `conformance/modelrunner/runner.go:219` | Requires each sampled expanded operation to meet its sample verdict. | oracle-internal | n/a |
| MOD-025 | `conformance/modelrunner/runner.go:231` | Permits an expanded-operation error only when the macro expects that canonical error. | oracle-internal | n/a |
| MOD-026 | `conformance/modelrunner/runner.go:239` | Requires a successful macro to match its authored outcome. | oracle-internal | n/a |
| MOD-027 | `conformance/modelrunner/runner.go:249` | Requires a normal operation to resolve dependent input. | oracle-internal | n/a |
| MOD-028 | `conformance/modelrunner/runner.go:258` | Requires a measurement binding to derive from its executed connect result. | oracle-internal | n/a |
| MOD-029 | `conformance/modelrunner/runner.go:266` | Requires each normal operation to match its authored success or canonical error outcome. | oracle-internal | n/a |
| MOD-030 | `conformance/modelrunner/runner.go:300` | Requires an expected sample error code and unchanged state after a preserving error. | no-state-forks | Return the expected error after changing one durable field. |
| MOD-031 | `conformance/modelrunner/runner.go:309` | Rejects an unexpected error from a successful sample. | oracle-internal | n/a |
| MOD-032 | `conformance/modelrunner/runner.go:312` | Requires the sampled result kind. | ENGINE-GAP | n/a |
| MOD-033 | `conformance/modelrunner/runner.go:315` | Requires the sampled HTTP status and canonical code shape. | proven-elsewhere `conformance/blackbox/integration/real_configured_bounds_test.go:43` | n/a |
| MOD-034 | `conformance/modelrunner/runner.go:327` | Requires a preserving HTTP error to leave state unchanged. | no-state-forks | Return HTTP 400 after modifying a local checkpoint. |
| MOD-035 | `conformance/modelrunner/runner.go:339` | Requires accepted administrative samples to stage a membership generation. | scope-isolation | Accept a fanout stage without the membership-generation result. |
| MOD-036 | `conformance/modelrunner/runner.go:344` | Requires accepted backfill samples to retain requested batch size and count. | mutation-conservation | Report one fewer backfill batch. |
| MOD-037 | `conformance/modelrunner/runner.go:351` | Requires an accepted pull sample to expose a typed pull observation. | ENGINE-GAP | n/a |
| MOD-038 | `conformance/modelrunner/runner.go:355` | Requires rebuild page records not to exceed the accepted page limit. | cursor-monotonicity | Return limit plus one rebuild record. |
| MOD-039 | `conformance/modelrunner/runner.go:359` | Requires compaction batch size and deleted count not to exceed the accepted limit. | mutation-conservation | Delete limit plus one tombstone. |
| MOD-040 | `conformance/modelrunner/runner.go:363` | Requires a push sample to report exactly its requested mutation count. | mutation-conservation | Drop one mutation observation. |
| MOD-041 | `conformance/modelrunner/runner.go:375` | Requires the model to implement resolved-operation application. | oracle-internal | n/a |
| MOD-042 | `conformance/modelrunner/runner.go:383` | Requires the pull-hydration fault recipe to be valid before fault injection. | oracle-internal | n/a |
| MOD-043 | `conformance/modelrunner/runner.go:400` | Requires the designated hydration fault to target one pull step and one fault plan. | oracle-internal | n/a |
| MOD-044 | `conformance/modelrunner/runner.go:404` | Requires the hydration fault mechanism, target, and operator to match the authored recipe. | oracle-internal | n/a |
| MOD-045 | `conformance/modelrunner/runner.go:414` | Requires the fault barrier to exist in the scenario barrier plan. | oracle-internal | n/a |
| MOD-046 | `conformance/modelrunner/runner.go:440` | Requires the hydration fault to resolve exactly one candidate projection. | oracle-internal | n/a |
| MOD-047 | `conformance/modelrunner/runner.go:454` | Requires apply-pull to name an earlier successful pull step. | oracle-internal | Reference the current pull step from apply-pull. |
| MOD-048 | `conformance/modelrunner/runner.go:482` | Requires apply-pull payloads to contain a nonempty source step identifier. | oracle-internal | n/a |
| MOD-049 | `conformance/modelrunner/runner.go:497` | Requires expected success or typed canonical error disposition. | oracle-internal | n/a |
| MOD-050 | `conformance/modelrunner/runner.go:549` | Requires every wire expectation to have an executed step and matching status, retryability, and code. | ENGINE-GAP | n/a |
| MOD-051 | `conformance/modelrunner/runner.go:571` | Rejects a received HTTP body or retry header for a transport failure. | proven-elsewhere `conformance/blackbox/integration/real_push_retention_test.go:77` | n/a |
| MOD-052 | `conformance/modelrunner/runner.go:582` | Requires the observed canonical wire error code to equal the authored code. | ENGINE-GAP | n/a |
| MOD-053 | `conformance/modelrunner/runner.go:597` | Requires every authored predicate payload and name to be valid before evaluation. | oracle-internal | n/a |
| MOD-054 | `conformance/modelrunner/runner.go:608` | Records and fails the run when any authored predicate fails. | oracle-internal | n/a |
| MOD-055 | `conformance/modelrunner/runner.go:621` | Requires schema-dispatch predicates to decode a valid measurement plan. | oracle-internal | n/a |
| MOD-056 | `conformance/modelrunner/runner.go:625` | Rejects a predicate name outside the closed authored set. | oracle-internal | n/a |
| MOD-057 | `conformance/modelrunner/runner.go:630` | Requires non-measurement predicate payloads to be empty objects. | oracle-internal | n/a |
| MOD-058 | `conformance/modelrunner/runner.go:638` | Requires deterministic replay and exact authored state facts for state equality. | oracle-internal | Change one final-state fact while preserving replay inputs. |
| MOD-059 | `conformance/modelrunner/runner.go:649` | Requires at least one failed operation to preserve its before snapshot. | no-state-forks | Mutate state after every failed operation. |
| MOD-060 | `conformance/modelrunner/runner.go:661` | Requires every successful execution to meet transition semantics. | no-state-forks | Return success without the operation's required state transition. |
| MOD-061 | `conformance/modelrunner/runner.go:668` | Requires portable-seed scenarios to execute seed installation. | oracle-internal | n/a |
| MOD-062 | `conformance/modelrunner/runner.go:675` | Requires each recognized performance trace to satisfy its closed contract. | oracle-internal | n/a |
| MOD-063 | `conformance/modelrunner/runner.go:689` | Requires a fresh model to contain only Protocol 3 and no durable state. | oracle-internal | Preseed a row or transaction. |
| MOD-064 | `conformance/modelrunner/runner_test.go:33` | Tests that ordinary macros expand to valid nonmacro operations without configured samples. | oracle-internal | n/a |
| MOD-065 | `conformance/modelrunner/runner_test.go:71` | Tests that an invented warm-connect request scope fails semantic validation. | scope-isolation | Replace known scope with `client-invented-scope`. |
| MOD-066 | `conformance/modelrunner/runner_test.go:93` | Tests every component of the warm-connect semantic trace. | oracle-internal | n/a |
| MOD-067 | `conformance/modelrunner/runner_test.go:139` | Tests that a successful warm connect cannot carry an invented body. | ENGINE-GAP | n/a |
| MOD-068 | `conformance/modelrunner/runner_test.go:152` | Tests that assignment lineage cannot diverge from scope state. | scope-isolation | Change server and local membership generation to two. |
| MOD-069 | `conformance/modelrunner/runner_test.go:164` | Tests that local and server assignment lineages must agree. | scope-isolation | Change only the local membership generation. |
| MOD-070 | `conformance/modelrunner/runner_test.go:172` | Tests that warm connect requires its measured terminal pull. | oracle-internal | Remove the terminal pull and apply. |
| MOD-071 | `conformance/modelrunner/runner_test.go:182` | Tests that warm connect rejects an extra measured pull. | oracle-internal | n/a |
| MOD-072 | `conformance/modelrunner/runner_test.go:193` | Tests that warm connect rejects a forbidden measured operation. | oracle-internal | n/a |
| MOD-073 | `conformance/modelrunner/runner_test.go:206` | Tests that forged final local fields fail semantic validation. | checksum-convergence | Change the applied local row field without changing its checksum. |
| MOD-074 | `conformance/modelrunner/runner_test.go:225` | Tests that authored checkpoint facts require verified terminal progress. | checksum-convergence | Clear the local checkpoint verified flag. |
| MOD-075 | `conformance/modelrunner/runner_test.go:247` | Tests that authored checkpoint facts reject a wrong verified checksum. | checksum-convergence | Flip one checkpoint checksum byte. |
| MOD-076 | `conformance/modelrunner/runner_test.go:267` | Tests that forged local fields fail steady-pull semantics. | checksum-convergence | Change one local row wire value. |
| MOD-077 | `conformance/modelrunner/runner_test.go:289` | Tests that terminal server and local cursors cannot be zero. | cursor-monotonicity | Replace both terminal cursors with zero tokens. |
| MOD-078 | `conformance/modelrunner/runner_test.go:301` | Tests that pull apply cannot mutate assignments atomically with local rows. | scope-isolation | Change membership generation during apply-pull. |
| MOD-079 | `conformance/modelrunner/runner_test.go:321` | Tests independent checksums against forged authoritative and local fields. | checksum-convergence | Change all stored fields while retaining checksums. |
| MOD-080 | `conformance/modelrunner/runner_test.go:331` | Tests that the pending-cycle steps use contiguous snapshots. | no-state-forks | Disconnect materialize before from push after. |
| MOD-081 | `conformance/modelrunner/runner_test.go:341` | Tests that pending materialization targets the push transaction. | mutation-conservation | Change materialization commit LSN to 999. |
| MOD-082 | `conformance/modelrunner/runner_test.go:351` | Tests that an applied push outcome cannot become conflict in its trace. | mutation-conservation | Change applied outcome to conflict. |
| MOD-083 | `conformance/modelrunner/runner_test.go:370` | Tests pending-cycle field integrity with independent checksums. | checksum-convergence | Forge durable fields through push, WAL, and pull. |
| MOD-084 | `conformance/modelrunner/runner_test.go:399` | Tests that a forged stored and returned scope checksum fails. | checksum-convergence | Flip the scope checksum and return the forged value. |
| MOD-085 | `conformance/modelrunner/runner_test.go:407` | Tests that row checksum verification includes canonical row identity. | checksum-convergence | Replace canonical identity bytes. |
| MOD-086 | `conformance/modelrunner/runner_test.go:428` | Tests that rebuild sessions keep the snapshot boundary. | cursor-monotonicity | Set rebuild boundary to commit 99. |
| MOD-087 | `conformance/modelrunner/runner_test.go:447` | Tests that fresh connect preserves null client-generation wire semantics. | proven-elsewhere `extensions/synchro-core/src/contract.rs:2974` | n/a |
| MOD-088 | `conformance/modelrunner/runner_test.go:457` | Tests that the first rebuild page is not replayed. | cursor-monotonicity | Set first-page replayed to true. |
| MOD-089 | `conformance/modelrunner/runner_test.go:467` | Tests that the final rebuild cursor cannot equal continuation. | cursor-monotonicity | Set final cursor to first-page continuation. |
| MOD-090 | `conformance/modelrunner/runner_test.go:489` | Tests that a rebuild page cannot include a post-boundary row. | cursor-monotonicity | Append concurrent row to rebuild records. |
| MOD-091 | `conformance/modelrunner/runner_test.go:499` | Tests that post-rebuild pull includes the post-boundary row. | cursor-monotonicity | Remove terminal pull changes. |
| MOD-092 | `conformance/modelrunner/runner_test.go:513` | Tests that rebuild terminal pull checksum remains authentic. | checksum-convergence | Flip returned pull checksum. |
| MOD-093 | `conformance/modelrunner/runner_test.go:543` | Tests that rebuild acknowledgement clears the server rebuild requirement. | cursor-monotonicity | Mark the final server assignment rebuild-required. |
| MOD-094 | `conformance/modelrunner/runner_test.go:712` | Tests source-step resolution and defensive copying for apply-pull. | oracle-internal | Alias prior pull result into resolved input. |
| MOD-095 | `conformance/modelrunner/runner_test.go:740` | Tests that fresh-model validation rejects preseeded stream state. | oracle-internal | Add a preseeded transaction. |
| MOD-096 | `conformance/modelrunner/runner_test.go:753` | Tests that expected errors require typed canonical codes, not message text. | oracle-internal | n/a |
| MOD-097 | `conformance/modelrunner/runner_test.go:777` | Tests that transport failure has no fabricated HTTP response. | proven-elsewhere `conformance/blackbox/integration/real_push_retention_test.go:77` | n/a |
| MOD-098 | `conformance/modelrunner/runner_test.go:792` | Tests that a corrupt portable seed fails closed. | checksum-convergence | n/a |
| MOD-099 | `conformance/modelrunner/runner_test.go:800` | Tests deterministic replay hashing for equivalent operations. | oracle-internal | Change one equivalent operation payload. |
| MOD-100 | `conformance/modelrunner/runner_test.go:820` | Tests that wrong authored state facts fail despite deterministic replay. | oracle-internal | Set authored row count to two. |
| MOD-101 | `conformance/modelrunner/runner_test.go:861` | Tests that schema-dispatch coverage rejects repeated strata. | oracle-internal | n/a |
| MOD-102 | `conformance/modelrunner/runner_test.go:903` | Tests that schema-dispatch coverage rejects a missing Class 4 sample. | oracle-internal | n/a |
| MOD-103 | `conformance/modelrunner/runner_test.go:935` | Tests that exact provenance facts reject stale scope, missing scope, wrong row, and wrong version. | scope-isolation | Change provenance scope, row, or version. |
| MOD-104 | `conformance/modelrunner/schema_dispatch.go:13` | Requires complete measurement binding identifiers. | oracle-internal | n/a |
| MOD-105 | `conformance/modelrunner/schema_dispatch.go:16` | Requires a measurement sample to execute connect/send. | oracle-internal | n/a |
| MOD-106 | `conformance/modelrunner/schema_dispatch.go:19` | Requires a measurement sample to contain a successful typed connect observation. | oracle-internal | n/a |
| MOD-107 | `conformance/modelrunner/schema_dispatch.go:73` | Requires a nonempty schema-dispatch plan with a positive per-stratum minimum. | oracle-internal | n/a |
| MOD-108 | `conformance/modelrunner/schema_dispatch.go:78` | Requires each schema-dispatch stratum to be complete and unique. | oracle-internal | n/a |
| MOD-109 | `conformance/modelrunner/schema_dispatch.go:96` | Requires each bound measurement to use the plan measurement identifier. | oracle-internal | n/a |
| MOD-110 | `conformance/modelrunner/schema_dispatch.go:100` | Requires each schema-dispatch stratum and sample pair to be unique. | oracle-internal | n/a |
| MOD-111 | `conformance/modelrunner/schema_dispatch.go:105` | Requires measurement parameters to name an available planned schema case. | oracle-internal | n/a |
| MOD-112 | `conformance/modelrunner/schema_dispatch.go:113` | Requires the observed connect result to implement the bound schema case. | oracle-internal | n/a |
| MOD-113 | `conformance/modelrunner/schema_dispatch.go:120` | Requires schema-dispatch samples to use distinct clients. | oracle-internal | n/a |
| MOD-114 | `conformance/modelrunner/schema_dispatch.go:126` | Requires at least one executed schema-dispatch sample. | oracle-internal | n/a |
| MOD-115 | `conformance/modelrunner/schema_dispatch.go:137` | Requires every planned stratum to meet its sample minimum. | oracle-internal | n/a |
| MOD-116 | `conformance/modelrunner/schema_dispatch.go:148` | Requires parameters to contain exactly one nonempty schema_case. | oracle-internal | n/a |
| MOD-117 | `conformance/modelrunner/schema_dispatch.go:167` | Requires a measurement to exactly mirror a successful Protocol 3 connect result. | oracle-internal | n/a |
| MOD-118 | `conformance/modelrunner/schema_dispatch.go:177` | Excludes explicit schema reset from ordinary schema dispatch evidence. | oracle-internal | n/a |
| MOD-119 | `conformance/modelrunner/schema_dispatch.go:183` | Requires connect target to equal authoritative current schema. | ENGINE-GAP | n/a |
| MOD-120 | `conformance/modelrunner/schema_dispatch.go:183` | Requires exact-current dispatch to have no action, reason, or affected scopes. | proven-elsewhere `extensions/synchro-pg/src/pg_tests/schema.rs:2067` | n/a |
| MOD-121 | `conformance/modelrunner/schema_dispatch.go:191` | Requires Class 1 dispatch to invalidate a changed-scope cursor. | cursor-monotonicity | Retain an issued cursor for a membership-divergent scope. |
| MOD-122 | `conformance/modelrunner/schema_dispatch.go:201` | Requires Class 4 dispatch to be unsupported with its canonical reason. | proven-elsewhere `extensions/synchro-pg/src/pg_tests/schema.rs:2388` | n/a |
| MOD-123 | `conformance/modelrunner/schema_dispatch.go:209` | Requires Class 3 dispatch to replace unaffected clients or rebuild affected scopes. | scope-isolation | Omit one affected assigned scope from rebuild action. |
| MOD-124 | `conformance/modelrunner/schema_dispatch.go:220` | Requires Class 2 dispatch to be a clean schema replacement. | proven-elsewhere `extensions/synchro-pg/src/pg_tests/schema.rs:2388` | n/a |
| MOD-125 | `conformance/modelrunner/schema_dispatch.go:250` | Requires schema lineage endpoints and every parent transition to be complete and supported. | ENGINE-GAP | n/a |
| MOD-126 | `conformance/modelrunner/schema_dispatch.go:288` | Detects local and server assignment membership divergence. | scope-isolation | Change a local membership or retention generation. |
| MOD-127 | `conformance/modelrunner/schema_dispatch.go:335` | Requires affected-scope sets to contain the same unique scope identities. | scope-isolation | Duplicate or omit an affected scope. |
| MOD-128 | `conformance/modelrunner/seed.go:120` | Requires portable seed construction from a Protocol 3 snapshot. | oracle-internal | n/a |
| MOD-129 | `conformance/modelrunner/seed.go:124` | Requires the installed current schema to exist. | oracle-internal | n/a |
| MOD-130 | `conformance/modelrunner/seed.go:139` | Requires portable seed export to use an active stream generation and active-stream boundary. | cursor-monotonicity | Change snapshot boundary generation. |
| MOD-131 | `conformance/modelrunner/seed.go:155` | Requires each generated seed row to bind to the registered seed table. | checksum-convergence | Bind one seed row to another table. |
| MOD-132 | `conformance/modelrunner/seed.go:190` | Revalidates a built portable seed before it is returned. | oracle-internal | Corrupt fixture bytes after construction. |
| MOD-133 | `conformance/modelrunner/seed.go:198` | Requires a model for model-based seed fixture construction. | oracle-internal | n/a |
| MOD-134 | `conformance/modelrunner/seed.go:209` | Requires a scenario identifier for a scenario seed fixture. | oracle-internal | n/a |
| MOD-135 | `conformance/modelrunner/seed.go:227` | Requires installed manifest data to parse and hash to the installed schema reference. | checksum-convergence | Change manifest content while preserving schema reference. |
| MOD-136 | `conformance/modelrunner/seed.go:286` | Requires generated manifest hash to match the installed schema hash. | checksum-convergence | Change one generated manifest field. |
| MOD-137 | `conformance/modelrunner/seed.go:302` | Requires a current registry generation with a registered synced table. | oracle-internal | n/a |
| MOD-138 | `conformance/modelrunner/seed.go:338` | Requires at least one authoritative scope for a portable seed. | scope-isolation | Remove all scopes. |
| MOD-139 | `conformance/modelrunner/seed.go:353` | Requires each seed row identity and digest to be vector-derived. | checksum-convergence | Forge a seed row identity or digest. |
| MOD-140 | `conformance/modelrunner/seed.go:399` | Requires seed manifest table fields to exist. | proven-elsewhere `extensions/synchro-core/src/contract.rs:2723` | n/a |
| MOD-141 | `conformance/modelrunner/seed.go:427` | Requires a seed manifest primary-key field. | proven-elsewhere `extensions/synchro-core/src/contract.rs:2723` | n/a |
| MOD-142 | `conformance/modelrunner/seed.go:495` | Requires portable artifact serialization to canonicalize successfully. | oracle-internal | n/a |
| MOD-143 | `conformance/modelrunner/seed.go:504` | Requires closed portable fixture and artifact identifiers. | oracle-internal | n/a |
| MOD-144 | `conformance/modelrunner/seed.go:507` | Requires artifact and manifest SHA-256 values to match supplied bytes. | checksum-convergence | Flip one artifact or manifest byte. |
| MOD-145 | `conformance/modelrunner/seed.go:513` | Requires seed schema, registry generation, and stream lineage to match the installed contract. | no-state-forks | Change fixture registry generation. |
| MOD-146 | `conformance/modelrunner/seed.go:516` | Requires exactly one declared portable scope with a matching scope fixture. | scope-isolation | Add a second portable scope. |
| MOD-147 | `conformance/modelrunner/seed.go:519` | Requires seed snapshot boundary not to exceed server materialization. | cursor-monotonicity | Advance seed boundary past global materialization. |
| MOD-148 | `conformance/modelrunner/seed.go:522` | Requires exactly 1,000 seed rows and matching declared cardinality. | mutation-conservation | Remove one seed row. |
| MOD-149 | `conformance/modelrunner/seed.go:526` | Requires the seed manifest to parse and hash independently. | checksum-convergence | Substitute another manifest. |
| MOD-150 | `conformance/modelrunner/seed.go:530` | Requires each seed row to have ordered ordinal, live state, valid identity, and valid digest. | checksum-convergence | Change one row checksum or ordinal. |
| MOD-151 | `conformance/modelrunner/seed.go:544` | Requires the portable scope checksum to equal the independently computed digest. | checksum-convergence | Flip the fixture scope checksum. |
| MOD-152 | `conformance/modelrunner/seeded_startup_test.go:26` | Tests that the complete seeded and empty startup workload passes. | oracle-internal | n/a |
| MOD-153 | `conformance/modelrunner/seeded_startup_test.go:32` | Tests that fewer than three empty samples fail. | oracle-internal | n/a |
| MOD-154 | `conformance/modelrunner/seeded_startup_test.go:38` | Tests that fewer than three seeded samples fail. | oracle-internal | n/a |
| MOD-155 | `conformance/modelrunner/semantic.go:30` | Requires replay to preserve setup count, step count, each execution, and final snapshot. | oracle-internal | Change one replay execution after snapshot. |
| MOD-156 | `conformance/modelrunner/semantic.go:50` | Requires replay execution identity, snapshots, results, expansions, samples, and canonical error code to match. | oracle-internal | Change one typed sample record. |
| MOD-157 | `conformance/modelrunner/semantic.go:63` | Requires setup to install the Protocol 3 contract. | oracle-internal | Return setup without installed contract state. |
| MOD-158 | `conformance/modelrunner/semantic.go:69` | Requires an errored step to preserve all model state. | no-state-forks | Modify a row after an expected error. |
| MOD-159 | `conformance/modelrunner/semantic.go:76` | Requires each successful step to retain Protocol 3. | no-state-forks | Change protocol version to two. |
| MOD-160 | `conformance/modelrunner/semantic.go:79` | Requires each successful operation to have its defined result kind. | ENGINE-GAP | n/a |
| MOD-161 | `conformance/modelrunner/semantic.go:82` | Requires each successful operation to produce its defined state transition. | no-state-forks | Return success without changing required state. |
| MOD-162 | `conformance/modelrunner/semantic.go:121` | Defines and enforces the complete operation-to-result-kind mapping. | ENGINE-GAP | n/a |
| MOD-163 | `conformance/modelrunner/semantic.go:130` | Requires each operation class to change only its permitted state surface. | no-state-forks | Change an unrelated client during local write. |
| MOD-164 | `conformance/modelrunner/semantic.go:172` | Requires local write to change durable queue or local rows. | mutation-conservation | Return success without a queue or row mutation. |
| MOD-165 | `conformance/modelrunner/semantic.go:184` | Requires every recognized performance scenario to select its closed performance verifier. | oracle-internal | n/a |
| MOD-166 | `conformance/modelrunner/semantic.go:375` | Requires warm-connect trace order, bootstrap rebuild, acknowledgment, WAL materialization, and terminal pull/apply. | cursor-monotonicity | Omit measured terminal pull. |
| MOD-167 | `conformance/modelrunner/semantic.go:389` | Requires warm baseline pull to acknowledge a verified checkpoint without changing local data. | cursor-monotonicity | Acknowledge with a changed local checkpoint. |
| MOD-168 | `conformance/modelrunner/semantic.go:424` | Requires warm connect to preserve server and local assignment lineage and emit one connection event. | scope-isolation | Add an invented known scope. |
| MOD-169 | `conformance/modelrunner/semantic.go:485` | Requires steady-pull trace order and terminal pull/apply flow. | cursor-monotonicity | Swap pull and apply. |
| MOD-170 | `conformance/modelrunner/semantic.go:522` | Requires baseline rebuild lifecycle, checkpoint lineage, and completed attempt state. | cursor-monotonicity | Finalize rebuild without a verified final cursor. |
| MOD-171 | `conformance/modelrunner/semantic.go:588` | Requires committed WAL materialization to add one row, membership, effect, and independent checksum. | checksum-convergence | Add the row without updating scope checksum. |
| MOD-172 | `conformance/modelrunner/semantic.go:614` | Requires terminal pull to issue a new cursor, return one authentic change, and report scope checksum. | cursor-monotonicity | Reuse baseline cursor. |
| MOD-173 | `conformance/modelrunner/semantic.go:649` | Requires apply-pull to atomically converge local row, provenance, checkpoint, and server checkpoint. | checksum-convergence | Apply row without matching checkpoint checksum. |
| MOD-174 | `conformance/modelrunner/semantic.go:682` | Requires adjacent semantic step snapshots to be continuous. | no-state-forks | Set one step before to a different snapshot. |
| MOD-175 | `conformance/modelrunner/semantic.go:701` | Requires pending-cycle local write, push, materialization, and terminal pull to form one consistent flow. | mutation-conservation | Change push outcome from applied to conflict. |
| MOD-176 | `conformance/modelrunner/semantic.go:729` | Requires rebuild-requests trace cardinality, order, concurrent isolation, and post-boundary pull. | cursor-monotonicity | Include the concurrent row in rebuild page two. |
| MOD-177 | `conformance/modelrunner/semantic.go:748` | Requires fresh rebuild assignment to create one rebuild-required scope without changing rows or scopes. | scope-isolation | Change authoritative rows while assigning scope. |
| MOD-178 | `conformance/modelrunner/semantic.go:760` | Requires fresh connect's null wire inputs and replace dispatch to create the local client state. | ENGINE-GAP | n/a |
| MOD-179 | `conformance/modelrunner/semantic.go:787` | Requires local rebuild operations to use one client, scope, rebuild identity, and completed receipt. | cursor-monotonicity | Use another rebuild ID for final apply. |
| MOD-180 | `conformance/modelrunner/semantic.go:807` | Requires post-rebuild pull to return only the row added after snapshot boundary. | cursor-monotonicity | Return a staged rebuild row. |
| MOD-181 | `conformance/modelrunner/semantic.go:838` | Requires exact semantic step count, operation order, contiguous snapshots, and final snapshot. | no-state-forks | Append an extra operation. |
| MOD-182 | `conformance/modelrunner/semantic.go:861` | Requires a successful connect to contain a typed connect result. | ENGINE-GAP | n/a |
| MOD-183 | `conformance/modelrunner/semantic.go:865` | Requires terminal rebuild-required pull to preserve state and return rebuild cursors for active scopes. | cursor-monotonicity | Return a normal cursor instead of rebuild-required. |
| MOD-184 | `conformance/modelrunner/semantic.go:889` | Requires successful endpoint HTTP 200 shape, error absence, retry absence, and expected body shape. | ENGINE-GAP | n/a |
| MOD-185 | `conformance/modelrunner/semantic.go:903` | Requires apply-pull to preserve nonlocal state and install the expected checkpoint. | cursor-monotonicity | Apply a pull with a nonempty cursor checkpoint. |
| MOD-186 | `conformance/modelrunner/semantic.go:933` | Requires local write and push payloads to identify the same one mutation. | mutation-conservation | Change push mutation ID. |
| MOD-187 | `conformance/modelrunner/semantic.go:940` | Requires local write to create exactly one pending row and queued mutation. | mutation-conservation | Omit durable queue entry. |
| MOD-188 | `conformance/modelrunner/semantic.go:954` | Requires push ledger, batch, outcome, local row, source row, and response body to agree. | mutation-conservation | Drop one ledger outcome. |
| MOD-189 | `conformance/modelrunner/semantic.go:1003` | Requires materialization to preserve client, batch, and mutation ledgers while materializing the pushed transaction. | no-state-forks | Change client state during WAL materialization. |
| MOD-190 | `conformance/modelrunner/semantic.go:1047` | Requires a one-page rebuild session to have valid lineage, completion, page records, and checksum. | cursor-monotonicity | Mark a completed one-page rebuild as replayed. |
| MOD-191 | `conformance/modelrunner/semantic.go:1084` | Requires first rebuild page to be a nonreplayed continuation-bearing snapshot page. | cursor-monotonicity | Remove first-page continuation. |
| MOD-192 | `conformance/modelrunner/semantic.go:1112` | Requires source materialization to advance authoritative rows, transaction state, scope cardinality, and digests together. | mutation-conservation | Materialize a transaction without adding all expected rows. |
| MOD-193 | `conformance/modelrunner/semantic.go:1153` | Requires a concurrent source change to remain outside the rebuild session snapshot. | cursor-monotonicity | Include concurrent row in staged rebuild rows. |
| MOD-194 | `conformance/modelrunner/semantic.go:1169` | Requires final rebuild page to consume continuation and preserve its original snapshot checksum. | cursor-monotonicity | Use final cursor as continuation. |
| MOD-195 | `conformance/modelrunner/semantic.go:1202` | Requires rebuild request client, generation, schema, assignment, local lineage, and scope to match. | scope-isolation | Request an unassigned scope. |
| MOD-196 | `conformance/modelrunner/semantic.go:1217` | Requires rebuild session generations, schema, boundary, and write epoch to match request lineage. | cursor-monotonicity | Change session snapshot boundary. |
| MOD-197 | `conformance/modelrunner/semantic.go:1221` | Requires each rebuild observation to equal its persisted page. | cursor-monotonicity | Change one page record. |
| MOD-198 | `conformance/modelrunner/semantic.go:1234` | Requires staged rebuild rows to be unique, sorted, complete scope rows with valid identities and checksums. | checksum-convergence | Forge one staged row checksum. |
| MOD-199 | `conformance/modelrunner/semantic.go:1267` | Requires rebuild pages to cover staged rows exactly once in page-limit order. | cursor-monotonicity | Skip a staged row in a page. |
| MOD-200 | `conformance/modelrunner/semantic.go:1291` | Requires topology and cardinality workload strata to match their authored distribution. | oracle-internal | n/a |
| MOD-201 | `conformance/modelrunner/semantic.go:1328` | Requires pending-mutation workload strata to contain one rejection and exact totals. | oracle-internal | Change a stratum from 100 to 99. |
| MOD-202 | `conformance/modelrunner/semantic.go:1342` | Requires configured-limit payload to contain the exact seven installed maxima. | oracle-internal | n/a |
| MOD-203 | `conformance/modelrunner/semantic.go:1396` | Requires every configured-limit family and boundary to have three correctly targeted samples. | oracle-internal | n/a |
| MOD-204 | `conformance/modelrunner/semantic.go:1434` | Requires invalid limit samples to preserve state and return specified errors. | no-state-forks | Allow invalid pull to add a cursor. |
| MOD-205 | `conformance/modelrunner/semantic.go:1451` | Requires accepted configured samples to honor family-specific batch, page, and mutation bounds. | mutation-conservation | Return more push mutations than requested. |
| MOD-206 | `conformance/modelrunner/semantic.go:1478` | Requires seeded startup to have three seeded and three empty unique-client samples. | oracle-internal | n/a |
| MOD-207 | `conformance/modelrunner/semantic.go:1532` | Requires seed installation to create 1,000 local rows, provenance records, and one seed receipt only. | mutation-conservation | Install 999 seeded local rows. |
| MOD-208 | `conformance/modelrunner/semantic.go:1544` | Requires assignment to add one rebuild-required scope and increment scope-set version. | scope-isolation | Assign scope without rebuild-required. |
| MOD-209 | `conformance/modelrunner/semantic.go:1556` | Requires seeded connect to consume receipt and issue the seeded scope cursor. | cursor-monotonicity | Retain seed receipt after connect. |
| MOD-210 | `conformance/modelrunner/semantic.go:1573` | Requires empty connect to demand rebuild without seeded data or receipt. | scope-isolation | Give empty client a seed receipt. |
| MOD-211 | `conformance/modelrunner/semantic.go:1626` | Requires wire schema version and lowercase SHA-256 hash to equal current schema. | checksum-convergence | Uppercase one schema hash character. |
| MOD-212 | `conformance/modelrunner/semantic.go:1644` | Requires every assignment to be unique, nonzero, and aligned to current scope state. | scope-isolation | Duplicate a scope assignment. |
| MOD-213 | `conformance/modelrunner/semantic.go:1717` | Requires server and local assignment lineages to have the same unique scopes and generations. | scope-isolation | Change local retention generation. |
| MOD-214 | `conformance/modelrunner/semantic.go:1770` | Requires cursor observations to cover each assigned scope once with requested disposition. | cursor-monotonicity | Duplicate a cursor observation. |
| MOD-215 | `conformance/modelrunner/semantic.go:1799` | Requires pull checksum observations to cover active scopes and equal stored scope checksums. | checksum-convergence | Omit an active scope checksum. |
| MOD-216 | `conformance/modelrunner/semantic.go:1927` | Recomputes row digest from schema, canonical identity, fields, and version. | checksum-convergence | Forge row field while retaining checksum. |
| MOD-217 | `conformance/modelrunner/semantic.go:1975` | Recomputes canonical row identity from manifest and primary-key wire value. | checksum-convergence | Replace canonical identity bytes. |
| MOD-218 | `conformance/modelrunner/semantic.go:1988` | Recomputes scope digest from unique live included rows and schema. | checksum-convergence | Duplicate scope membership or forge stored scope digest. |
| MOD-219 | `conformance/modelrunner/semantic.go:2032` | Requires every active scope stored checksum to equal its independent digest. | checksum-convergence | Change one active scope checksum. |
| MOD-220 | `conformance/modelrunner/semantic.go:2122` | Requires applied push response metadata and checksum to match local row state. | checksum-convergence | Return a different row-checksum digest. |
| MOD-221 | `conformance/modelrunner/semantic.go:2172` | Requires connect to append exactly one matching connected event. | oracle-internal | Append two connection events. |
| MOD-222 | `conformance/modelrunner/semantic.go:2229` | Requires seeded connect payload to have exactly the authorized seed receipt. | oracle-internal | n/a |
| MOD-223 | `conformance/modelrunner/semantic.go:2236` | Requires seeded and empty connect payload to declare known scopes explicitly empty. | oracle-internal | n/a |
| MOD-224 | `conformance/modelrunner/state_facts.go:14` | Requires authored transaction, row, scope, rebuild, batch, and mutation counts. | mutation-conservation | Decrement authored mutation count. |
| MOD-225 | `conformance/modelrunner/state_facts.go:29` | Requires authored registry generation and configured limits. | ENGINE-GAP | n/a |
| MOD-226 | `conformance/modelrunner/state_facts.go:38` | Requires authored materialization boundary and acknowledgement end LSN. | cursor-monotonicity | Decrease acknowledged end LSN. |
| MOD-227 | `conformance/modelrunner/state_facts.go:66` | Requires exact ordered transaction facts and event ordinals. | mutation-conservation | Change one transaction event ordinal. |
| MOD-228 | `conformance/modelrunner/state_facts.go:86` | Requires exact ordered row identity, version, and checksum facts. | checksum-convergence | Flip one row checksum. |
| MOD-229 | `conformance/modelrunner/state_facts.go:102` | Requires exact scope generation, cardinality, and effect-version facts. | scope-isolation | Add an effect to another scope. |
| MOD-230 | `conformance/modelrunner/state_facts.go:122` | Requires exact poison record transaction, relation, reason, and lifecycle facts. | proven-elsewhere `conformance/blackbox/integration/real_baseline_test.go:766` | n/a |
| MOD-231 | `conformance/modelrunner/state_facts.go:142` | Requires exact rebuild session identity, page, continuation, and status facts. | cursor-monotonicity | Change rebuild next row ordinal. |
| MOD-232 | `conformance/modelrunner/state_facts.go:159` | Requires exact client identity, schema, and durable collection counts. | mutation-conservation | Remove one local queued mutation. |
| MOD-233 | `conformance/modelrunner/state_facts.go:207` | Requires exact local provenance row, version, and scope edges. | scope-isolation | Add a stale provenance scope. |
| MOD-234 | `conformance/modelrunner/state_facts.go:226` | Requires checkpoint scope, presence flags, verification, and optional checksum facts. | checksum-convergence | Mark checksum verified with wrong digest. |
| MOD-235 | `conformance/modelrunner/state_facts.go:245` | Requires queued mutation identity, schema, columns, base version, order, and status facts. | mutation-conservation | Change queued mutation local order. |
| MOD-236 | `conformance/modelrunner/state_facts.go:269` | Requires mutation outcome identity, state, and reason facts. | mutation-conservation | Change accepted outcome to terminal rejection. |
| MOD-237 | `conformance/modelrunner/workload_cardinality.go:48` | Requires scope-cardinality workload payload and its closed profile, scope, count, page size, and Protocol 3. | oracle-internal | n/a |
| MOD-238 | `conformance/modelrunner/workload_cardinality.go:69` | Restricts cardinality samples to 1, 101, and 1,000 records. | oracle-internal | n/a |
| MOD-239 | `conformance/modelrunner/workload_cardinality.go:84` | Requires a valid active relation, assigned client, authoritative scope, and nondecreasing cardinality. | oracle-internal | Use an unassigned client. |
| MOD-240 | `conformance/modelrunner/workload_cardinality.go:107` | Requires existing cardinality rows to use deterministic identities. | oracle-internal | Replace an existing deterministic row identity. |
| MOD-241 | `conformance/modelrunner/workload_cardinality.go:114` | Requires membership staging only at a completed active-stream transaction boundary. | oracle-internal | Stage membership at an effect boundary. |
| MOD-242 | `conformance/modelrunner/workload_cardinality.go:176` | Produces one rebuild request and apply operation for each immutable page. | oracle-internal | Omit the second page for 101 rows. |
| MOD-243 | `conformance/modelrunner/workload_cardinality.go:246` | Requires a validated active registry relation, closed items table, manifest, and captured fields. | oracle-internal | n/a |
| MOD-244 | `conformance/modelrunner/workload_cardinality.go:332` | Requires an assigned client with positive current generation. | oracle-internal | Select a client without `scope-a`. |
| MOD-245 | `conformance/modelrunner/workload_cardinality.go:373` | Requires authoritative rows, scope membership, and stored cardinality to agree. | oracle-internal | Include membership without a live source row. |
| MOD-246 | `conformance/modelrunner/workload_cardinality.go:417` | Requires cardinality row identities to use the registered string primary key. | checksum-convergence | Set primary-key portable type to integer. |
| MOD-247 | `conformance/modelrunner/workload_cardinality.go:447` | Requires every desired row to have a scope-rule evaluation for the target scope. | oracle-internal | Omit one target row evaluation. |
| MOD-248 | `conformance/modelrunner/workload_cardinality.go:583` | Requires cardinality growth to insert rows and repeat runs to update the target row. | oracle-internal | Create no source event for a growth row. |
| MOD-249 | `conformance/modelrunner/workload_cardinality.go:642` | Requires a repeat cardinality row update to change a writable captured field. | oracle-internal | Retain all field values on update. |
| MOD-250 | `conformance/modelrunner/workload_cardinality.go:731` | Derives cardinality row checksums through independent vectors. | checksum-convergence | Change generated field without recalculating checksum. |
| MOD-251 | `conformance/modelrunner/workload_cardinality.go:777` | Prevents cardinality workload LSN allocation overflow or zero LSN. | oracle-internal | n/a |
| MOD-252 | `conformance/modelrunner/workload_cardinality.go:815` | Requires every expanded cardinality operation to validate as closed. | oracle-internal | n/a |
| MOD-253 | `conformance/modelrunner/workload_cardinality_test.go:46` | Tests cardinality expansion purity, closed operations, exact lifecycle counts, page ordinals, and continuation sources. | oracle-internal | Omit a continuation page. |
| MOD-254 | `conformance/modelrunner/workload_cardinality_test.go:110` | Tests that an unauthored cardinality count fails expansion. | oracle-internal | n/a |
| MOD-255 | `conformance/modelrunner/workload_cardinality_test.go:129` | Tests owned cardinality scenarios execute all expanded operations. | oracle-internal | n/a |
| MOD-256 | `conformance/modelrunner/workload_cardinality_test.go:167` | Tests final cardinality, one local client per workload, local rows, provenance, checkpoints, and completed rebuild state. | scope-isolation | Drop one local client's rebuild attempt. |
| MOD-257 | `conformance/modelrunner/workload_configured_limits.go:52` | Requires configured-limit expansion to have valid installed maxima and workload context. | oracle-internal | n/a |
| MOD-258 | `conformance/modelrunner/workload_configured_limits.go:59` | Requires administrative neutral limit to fit configured maximums. | oracle-internal | n/a |
| MOD-259 | `conformance/modelrunner/workload_configured_limits.go:160` | Requires every generated configured-limit plan to pass closure validation. | oracle-internal | n/a |
| MOD-260 | `conformance/modelrunner/workload_configured_limits.go:167` | Requires all six authored maxima to equal nonzero installed limits. | oracle-internal | n/a |
| MOD-261 | `conformance/modelrunner/workload_configured_limits.go:201` | Requires the empty installed workload, active relation, assigned client, schema, table, field, scope, and safe generations. | oracle-internal | n/a |
| MOD-262 | `conformance/modelrunner/workload_configured_limits.go:347` | Requires a configured limit to allow lower, upper, and invalid upper-plus-one boundaries. | oracle-internal | n/a |
| MOD-263 | `conformance/modelrunner/workload_configured_limits.go:552` | Requires exactly 63 complete, uniquely targeted samples and only valid closed reference operations. | oracle-internal | n/a |
| MOD-264 | `conformance/modelrunner/workload_queue_limits.go:25` | Requires pending-mutation workload profile, client, table, and authored 1/1, 99/1, or 999/1 partition. | oracle-internal | Change accepted count to two. |
| MOD-265 | `conformance/modelrunner/workload_queue_limits.go:150` | Requires expanded queue workload to exclude macro operations and validate every operation. | oracle-internal | n/a |
| MOD-266 | `conformance/modelrunner/workload_queue_limits.go:163` | Requires the exact requested workload profile. | oracle-internal | n/a |
| MOD-267 | `conformance/modelrunner/workload_queue_limits.go:174` | Requires each named numeric workload input to be unsigned integer data. | oracle-internal | n/a |
| MOD-268 | `conformance/modelrunner/workload_queue_limits.go:186` | Requires installed client generation and durable local state. | oracle-internal | Remove durable local state. |
| MOD-269 | `conformance/modelrunner/workload_queue_limits.go:204` | Requires current schema reference to exist in immutable history. | oracle-internal | n/a |
| MOD-270 | `conformance/modelrunner/workload_queue_limits.go:226` | Requires distinct writable nonlifecycle string fields for terminal rejection and accepted inserts. | oracle-internal | n/a |
| MOD-271 | `conformance/modelrunner/workload_queue_limits.go:258` | Prevents schema version allocation beyond the portable integer range. | oracle-internal | n/a |
| MOD-272 | `conformance/modelrunner/workload_queue_limits.go:493` | Prevents pending workload source LSN allocation overflow. | oracle-internal | n/a |
| MOD-273 | `conformance/modelrunner/workload_queue_limits_test.go:37` | Tests pending workload purity, closed expansion, exact unique local writes, and exact operation counts. | oracle-internal | Duplicate a generated mutation ID. |
| MOD-274 | `conformance/modelrunner/workload_queue_limits_test.go:88` | Tests response-loss first push and replayed second push semantics. | mutation-conservation | Mark replayed push as executed. |
| MOD-275 | `conformance/modelrunner/workload_queue_limits_test.go:94` | Tests schema publication and exact server batch and mutation deltas. | mutation-conservation | Add a second batch ledger. |
| MOD-276 | `conformance/modelrunner/workload_queue_limits_test.go:102` | Tests exact applied and terminal rejected server partition with schema-incompatible reason. | mutation-conservation | Change terminal reason. |
| MOD-277 | `conformance/modelrunner/workload_queue_limits_test.go:124` | Tests exact accepted and rejected durable local queue partition. | mutation-conservation | Mark an accepted local mutation rejected. |
| MOD-278 | `conformance/modelrunner/workload_queue_limits_test.go:151` | Tests that unauthored pending-mutation counts return no operations and an error. | oracle-internal | n/a |
| MOD-279 | `conformance/modelrunner/workload_queue_limits_test.go:170` | Tests configured-limit plan purity and exact 63 sample count. | oracle-internal | n/a |
| MOD-280 | `conformance/modelrunner/workload_queue_limits_test.go:185` | Tests configured samples have in-range distinct targets and matching target values. | oracle-internal | n/a |
| MOD-281 | `conformance/modelrunner/workload_queue_limits_test.go:199` | Tests every configured family and boundary has exactly three samples. | oracle-internal | n/a |
| MOD-282 | `conformance/modelrunner/workload_queue_limits_test.go:217` | Tests configured expansion contains only closed reference operations. | oracle-internal | n/a |
| MOD-283 | `conformance/modelrunner/workload_queue_limits_test.go:235` | Tests configured run passes, replays, has one macro step, and stores each sample outcome. | oracle-internal | n/a |
| MOD-284 | `conformance/modelrunner/workload_queue_limits_test.go:248` | Tests invalid configured samples preserve state and return family-specific error form. | no-state-forks | Let invalid compaction modify scope state. |
| MOD-285 | `conformance/modelrunner/workload_queue_limits_test.go:265` | Tests accepted configured samples contain accepted results and bound-specific observations. | mutation-conservation | Return too many push observations. |
| MOD-286 | `conformance/modelrunner/workload_queue_limits_test.go:287` | Tests replay equality includes typed workload sample records. | oracle-internal | Change one recorded sample value. |
| MOD-287 | `conformance/modelrunner/workload_queue_limits_test.go:295` | Tests queue and configured scenarios retain exact authored strata and successful outcomes. | oracle-internal | n/a |
| MOD-288 | `conformance/modelrunner/workload_topology.go:47` | Requires a valid topology request, registry, scopes, safe LSNs, and safe membership generation. | oracle-internal | n/a |
| MOD-289 | `conformance/modelrunner/workload_topology.go:174` | Requires topology profile with positive fanout and impact rows. | oracle-internal | n/a |
| MOD-290 | `conformance/modelrunner/workload_topology.go:194` | Requires Protocol 3 active stream, schema, validated registry, and independently valid manifest. | oracle-internal | n/a |
| MOD-291 | `conformance/modelrunner/workload_topology.go:231` | Requires unique registry relations and exactly one complete synced scope rule. | oracle-internal | n/a |
| MOD-292 | `conformance/modelrunner/workload_topology.go:258` | Requires requested fanout to fit registered and configured bounds. | oracle-internal | Request fanout larger than configured limit. |
| MOD-293 | `conformance/modelrunner/workload_topology.go:263` | Requires schema table and primary-key field to match the synced relation. | oracle-internal | Remove primary-key field from schema. |
| MOD-294 | `conformance/modelrunner/workload_topology.go:279` | Requires exactly one complete capture dependency impact within configured bound. | oracle-internal | n/a |
| MOD-295 | `conformance/modelrunner/workload_topology.go:315` | Requires requested authoritative scopes to be unique, current-schema, active-stream scopes. | oracle-internal | Select a scope from another stream. |
| MOD-296 | `conformance/modelrunner/workload_topology.go:338` | Requires all prior active-stream transactions to be materialized and LSN allocation to remain safe. | oracle-internal | Stage behind a pending transaction. |
| MOD-297 | `conformance/modelrunner/workload_topology.go:364` | Requires each selected scope to have positive membership generation before incrementing. | oracle-internal | Zero one selected scope generation. |
| MOD-298 | `conformance/modelrunner/workload_topology.go:397` | Requires supported primary-key type and complete primary value for generated topology rows. | oracle-internal | n/a |
| MOD-299 | `conformance/modelrunner/workload_topology.go:437` | Requires deterministic existing topology rows to have matching live identities. | oracle-internal | Mark a deterministic topology row deleted. |
| MOD-300 | `conformance/modelrunner/workload_topology.go:463` | Requires unique row evaluations and fanout no greater than registered bound. | oracle-internal | Duplicate an evaluation row. |
| MOD-301 | `conformance/modelrunner/workload_topology.go:533` | Requires every captured synced field in generated WAL events to exist. | oracle-internal | n/a |
| MOD-302 | `conformance/modelrunner/workload_topology.go:596` | Derives topology row identity from independent manifest vectors. | checksum-convergence | Replace generated canonical identity bytes. |
| MOD-303 | `conformance/modelrunner/workload_topology.go:609` | Rejects unsupported primary-key and field types in topology WAL images. | oracle-internal | n/a |
| MOD-304 | `conformance/modelrunner/workload_topology_test.go:24` | Tests topology expansion purity, exact six-operation sequence, closed operations, staged fanout, and impact evidence. | oracle-internal | Omit one selected scope from evaluations. |
| MOD-305 | `conformance/modelrunner/workload_topology_test.go:84` | Tests topology operations materialize expected registry, transaction, row, and per-scope effect state. | scope-isolation | Materialize row into scope A but not scope B. |
| MOD-306 | `conformance/modelrunner/workload_topology_test.go:128` | Tests missing authoritative scopes or schema fail expansion. | oracle-internal | n/a |
| MOD-307 | `conformance/modelrunner/workload_topology_test.go:156` | Tests cumulative topology samples retain exact strata and cumulative materialization evidence. | mutation-conservation | Drop one topology materialization. |
| MOD-308 | `conformance/modelrunner/workload_topology_test.go:248` | Tests each topology sample carries exact requested fanout and impact evidence. | oracle-internal | Encode fewer scopes in the row evaluation. |
| MOD-A01 | `conformance/modelrunner/runner.go:367` | Rejects an accepted workload sample whose family is outside the closed sample registry. | oracle-internal | n/a |
| MOD-A02 | `conformance/modelrunner/runner.go:390` | Requires the model to implement fault-aware pull hydration when the selected fault applies. | oracle-internal | n/a |
| MOD-A03 | `conformance/modelrunner/runner.go:683` | Requires every validated predicate name to have an evaluator implementation. | oracle-internal | n/a |
| MOD-A04 | `conformance/modelrunner/schema_dispatch.go:59` | Requires each schema-dispatch measurement to derive exactly one request from its connect operation. | oracle-internal | n/a |

## Consumers that import `conformance/modelrunner/`

The following direct imports are consumers. `conformance/scenarios/server/scenarios_test.go:45` drives the R2 semantic corpus through `RunScenario`.

| consumer | import |
|---|---|
| `conformance/blackbox/syntheticproof/runner.go` | `conformance/blackbox/syntheticproof/runner.go:14` |
| `conformance/blackbox/syntheticproof/synthetic.go` | `conformance/blackbox/syntheticproof/synthetic.go:20` |
| `conformance/kotlin/multi_scope_provenance.go` | `conformance/kotlin/multi_scope_provenance.go:13` |
| `conformance/kotlin/multi_scope_provenance_test.go` | `conformance/kotlin/multi_scope_provenance_test.go:8` |
| `conformance/kotlin/rebuild_apply.go` | `conformance/kotlin/rebuild_apply.go:12` |
| `conformance/kotlin/rebuild_cardinality.go` | `conformance/kotlin/rebuild_cardinality.go:12` |
| `conformance/reactnative/multi_scope_provenance.go` | `conformance/reactnative/multi_scope_provenance.go:18` |
| `conformance/reactnative/rebuild_apply.go` | `conformance/reactnative/rebuild_apply.go:16` |
| `conformance/reactnative/rebuild_cardinality.go` | `conformance/reactnative/rebuild_cardinality.go:15` |
| `conformance/reactnative/rebuild_requests.go` | `conformance/reactnative/rebuild_requests.go:18` |
| `conformance/scenarios/server/scenarios_test.go` | `conformance/scenarios/server/scenarios_test.go:8` |
| `conformance/swift/multi_scope_provenance.go` | `conformance/swift/multi_scope_provenance.go:13` |
| `conformance/swift/multi_scope_provenance_test.go` | `conformance/swift/multi_scope_provenance_test.go:8` |
| `conformance/swift/rebuild_apply.go` | `conformance/swift/rebuild_apply.go:12` |
| `conformance/swift/rebuild_cardinality.go` | `conformance/swift/rebuild_cardinality.go:11` |

## Corrected anchors

| check-id | old anchor | new anchor |
|---|---|---|
| MOD-005 | `conformance/modelrunner/macro.go:95` | `conformance/modelrunner/macro.go:96` |
| MOD-008 | `conformance/modelrunner/macro.go:133` | `conformance/modelrunner/macro.go:134` |
| MOD-020 | `conformance/modelrunner/runner.go:174` | `conformance/modelrunner/runner.go:175` |
| MOD-023 | `conformance/modelrunner/runner.go:207` | `conformance/modelrunner/runner.go:208` |
| MOD-024 | `conformance/modelrunner/runner.go:217` | `conformance/modelrunner/runner.go:219` |
| MOD-027 | `conformance/modelrunner/runner.go:248` | `conformance/modelrunner/runner.go:249` |
| MOD-041 | `conformance/modelrunner/runner.go:374` | `conformance/modelrunner/runner.go:375` |
| MOD-042 | `conformance/modelrunner/runner.go:382` | `conformance/modelrunner/runner.go:383` |
| MOD-047 | `conformance/modelrunner/runner.go:453` | `conformance/modelrunner/runner.go:454` |
| MOD-048 | `conformance/modelrunner/runner.go:480` | `conformance/modelrunner/runner.go:482` |
| MOD-049 | `conformance/modelrunner/runner.go:496` | `conformance/modelrunner/runner.go:497` |
| MOD-050 | `conformance/modelrunner/runner.go:547` | `conformance/modelrunner/runner.go:549` |
| MOD-054 | `conformance/modelrunner/runner.go:607` | `conformance/modelrunner/runner.go:608` |
| MOD-060 | `conformance/modelrunner/runner.go:660` | `conformance/modelrunner/runner.go:661` |
| MOD-063 | `conformance/modelrunner/runner.go:688` | `conformance/modelrunner/runner.go:689` |
| MOD-100 | `conformance/modelrunner/runner_test.go:819` | `conformance/modelrunner/runner_test.go:820` |
| MOD-111 | `conformance/modelrunner/schema_dispatch.go:104` | `conformance/modelrunner/schema_dispatch.go:105` |
| MOD-112 | `conformance/modelrunner/schema_dispatch.go:112` | `conformance/modelrunner/schema_dispatch.go:113` |
| MOD-116 | `conformance/modelrunner/schema_dispatch.go:146` | `conformance/modelrunner/schema_dispatch.go:148` |
| MOD-117 | `conformance/modelrunner/schema_dispatch.go:165` | `conformance/modelrunner/schema_dispatch.go:167` |
| MOD-119 | `conformance/modelrunner/schema_dispatch.go:181` | `conformance/modelrunner/schema_dispatch.go:183` |
| MOD-125 | `conformance/modelrunner/schema_dispatch.go:249` | `conformance/modelrunner/schema_dispatch.go:250` |
| MOD-126 | `conformance/modelrunner/schema_dispatch.go:285` | `conformance/modelrunner/schema_dispatch.go:288` |
| MOD-127 | `conformance/modelrunner/schema_dispatch.go:334` | `conformance/modelrunner/schema_dispatch.go:335` |
| MOD-129 | `conformance/modelrunner/seed.go:123` | `conformance/modelrunner/seed.go:124` |
| MOD-133 | `conformance/modelrunner/seed.go:197` | `conformance/modelrunner/seed.go:198` |
| MOD-135 | `conformance/modelrunner/seed.go:224` | `conformance/modelrunner/seed.go:227` |
| MOD-137 | `conformance/modelrunner/seed.go:300` | `conformance/modelrunner/seed.go:302` |
| MOD-138 | `conformance/modelrunner/seed.go:337` | `conformance/modelrunner/seed.go:338` |
| MOD-139 | `conformance/modelrunner/seed.go:351` | `conformance/modelrunner/seed.go:353` |
| MOD-140 | `conformance/modelrunner/seed.go:387` | `conformance/modelrunner/seed.go:399` |
| MOD-141 | `conformance/modelrunner/seed.go:416` | `conformance/modelrunner/seed.go:427` |
| MOD-142 | `conformance/modelrunner/seed.go:494` | `conformance/modelrunner/seed.go:495` |
| MOD-143 | `conformance/modelrunner/seed.go:503` | `conformance/modelrunner/seed.go:504` |
| MOD-149 | `conformance/modelrunner/seed.go:525` | `conformance/modelrunner/seed.go:526` |
| MOD-151 | `conformance/modelrunner/seed.go:543` | `conformance/modelrunner/seed.go:544` |
| MOD-156 | `conformance/modelrunner/semantic.go:49` | `conformance/modelrunner/semantic.go:50` |
| MOD-162 | `conformance/modelrunner/semantic.go:94` | `conformance/modelrunner/semantic.go:121` |
| MOD-164 | `conformance/modelrunner/semantic.go:171` | `conformance/modelrunner/semantic.go:172` |
| MOD-165 | `conformance/modelrunner/semantic.go:183` | `conformance/modelrunner/semantic.go:184` |
| MOD-166 | `conformance/modelrunner/semantic.go:358` | `conformance/modelrunner/semantic.go:375` |
| MOD-167 | `conformance/modelrunner/semantic.go:388` | `conformance/modelrunner/semantic.go:389` |
| MOD-168 | `conformance/modelrunner/semantic.go:423` | `conformance/modelrunner/semantic.go:424` |
| MOD-169 | `conformance/modelrunner/semantic.go:473` | `conformance/modelrunner/semantic.go:485` |
| MOD-170 | `conformance/modelrunner/semantic.go:520` | `conformance/modelrunner/semantic.go:522` |
| MOD-171 | `conformance/modelrunner/semantic.go:587` | `conformance/modelrunner/semantic.go:588` |
| MOD-172 | `conformance/modelrunner/semantic.go:613` | `conformance/modelrunner/semantic.go:614` |
| MOD-173 | `conformance/modelrunner/semantic.go:648` | `conformance/modelrunner/semantic.go:649` |
| MOD-174 | `conformance/modelrunner/semantic.go:681` | `conformance/modelrunner/semantic.go:682` |
| MOD-175 | `conformance/modelrunner/semantic.go:699` | `conformance/modelrunner/semantic.go:701` |
| MOD-176 | `conformance/modelrunner/semantic.go:712` | `conformance/modelrunner/semantic.go:729` |
| MOD-177 | `conformance/modelrunner/semantic.go:747` | `conformance/modelrunner/semantic.go:748` |
| MOD-178 | `conformance/modelrunner/semantic.go:759` | `conformance/modelrunner/semantic.go:760` |
| MOD-179 | `conformance/modelrunner/semantic.go:786` | `conformance/modelrunner/semantic.go:787` |
| MOD-180 | `conformance/modelrunner/semantic.go:806` | `conformance/modelrunner/semantic.go:807` |
| MOD-181 | `conformance/modelrunner/semantic.go:837` | `conformance/modelrunner/semantic.go:838` |
| MOD-182 | `conformance/modelrunner/semantic.go:860` | `conformance/modelrunner/semantic.go:861` |
| MOD-183 | `conformance/modelrunner/semantic.go:864` | `conformance/modelrunner/semantic.go:865` |
| MOD-184 | `conformance/modelrunner/semantic.go:888` | `conformance/modelrunner/semantic.go:889` |
| MOD-185 | `conformance/modelrunner/semantic.go:902` | `conformance/modelrunner/semantic.go:903` |
| MOD-186 | `conformance/modelrunner/semantic.go:930` | `conformance/modelrunner/semantic.go:933` |
| MOD-187 | `conformance/modelrunner/semantic.go:939` | `conformance/modelrunner/semantic.go:940` |
| MOD-188 | `conformance/modelrunner/semantic.go:953` | `conformance/modelrunner/semantic.go:954` |
| MOD-189 | `conformance/modelrunner/semantic.go:1002` | `conformance/modelrunner/semantic.go:1003` |
| MOD-190 | `conformance/modelrunner/semantic.go:1046` | `conformance/modelrunner/semantic.go:1047` |
| MOD-191 | `conformance/modelrunner/semantic.go:1082` | `conformance/modelrunner/semantic.go:1084` |
| MOD-192 | `conformance/modelrunner/semantic.go:1111` | `conformance/modelrunner/semantic.go:1112` |
| MOD-193 | `conformance/modelrunner/semantic.go:1149` | `conformance/modelrunner/semantic.go:1153` |
| MOD-194 | `conformance/modelrunner/semantic.go:1167` | `conformance/modelrunner/semantic.go:1169` |
| MOD-195 | `conformance/modelrunner/semantic.go:1201` | `conformance/modelrunner/semantic.go:1202` |
| MOD-196 | `conformance/modelrunner/semantic.go:1216` | `conformance/modelrunner/semantic.go:1217` |
| MOD-197 | `conformance/modelrunner/semantic.go:1220` | `conformance/modelrunner/semantic.go:1221` |
| MOD-198 | `conformance/modelrunner/semantic.go:1232` | `conformance/modelrunner/semantic.go:1234` |
| MOD-199 | `conformance/modelrunner/semantic.go:1266` | `conformance/modelrunner/semantic.go:1267` |
| MOD-200 | `conformance/modelrunner/semantic.go:1289` | `conformance/modelrunner/semantic.go:1291` |
| MOD-201 | `conformance/modelrunner/semantic.go:1325` | `conformance/modelrunner/semantic.go:1328` |
| MOD-202 | `conformance/modelrunner/semantic.go:1341` | `conformance/modelrunner/semantic.go:1342` |
| MOD-203 | `conformance/modelrunner/semantic.go:1372` | `conformance/modelrunner/semantic.go:1396` |
| MOD-204 | `conformance/modelrunner/semantic.go:1433` | `conformance/modelrunner/semantic.go:1434` |
| MOD-206 | `conformance/modelrunner/semantic.go:1477` | `conformance/modelrunner/semantic.go:1478` |
| MOD-207 | `conformance/modelrunner/semantic.go:1529` | `conformance/modelrunner/semantic.go:1532` |
| MOD-208 | `conformance/modelrunner/semantic.go:1543` | `conformance/modelrunner/semantic.go:1544` |
| MOD-209 | `conformance/modelrunner/semantic.go:1555` | `conformance/modelrunner/semantic.go:1556` |
| MOD-210 | `conformance/modelrunner/semantic.go:1572` | `conformance/modelrunner/semantic.go:1573` |
| MOD-211 | `conformance/modelrunner/semantic.go:1625` | `conformance/modelrunner/semantic.go:1626` |
| MOD-212 | `conformance/modelrunner/semantic.go:1642` | `conformance/modelrunner/semantic.go:1644` |
| MOD-213 | `conformance/modelrunner/semantic.go:1714` | `conformance/modelrunner/semantic.go:1717` |
| MOD-214 | `conformance/modelrunner/semantic.go:1768` | `conformance/modelrunner/semantic.go:1770` |
| MOD-215 | `conformance/modelrunner/semantic.go:1797` | `conformance/modelrunner/semantic.go:1799` |
| MOD-216 | `conformance/modelrunner/semantic.go:1926` | `conformance/modelrunner/semantic.go:1927` |
| MOD-217 | `conformance/modelrunner/semantic.go:1973` | `conformance/modelrunner/semantic.go:1975` |
| MOD-218 | `conformance/modelrunner/semantic.go:1986` | `conformance/modelrunner/semantic.go:1988` |
| MOD-219 | `conformance/modelrunner/semantic.go:2031` | `conformance/modelrunner/semantic.go:2032` |
| MOD-220 | `conformance/modelrunner/semantic.go:2120` | `conformance/modelrunner/semantic.go:2122` |
| MOD-221 | `conformance/modelrunner/semantic.go:2171` | `conformance/modelrunner/semantic.go:2172` |
| MOD-222 | `conformance/modelrunner/semantic.go:2225` | `conformance/modelrunner/semantic.go:2229` |
| MOD-223 | `conformance/modelrunner/semantic.go:2232` | `conformance/modelrunner/semantic.go:2236` |
| MOD-224 | `conformance/modelrunner/state_facts.go:13` | `conformance/modelrunner/state_facts.go:14` |
| MOD-227 | `conformance/modelrunner/state_facts.go:65` | `conformance/modelrunner/state_facts.go:66` |
| MOD-228 | `conformance/modelrunner/state_facts.go:85` | `conformance/modelrunner/state_facts.go:86` |
| MOD-229 | `conformance/modelrunner/state_facts.go:101` | `conformance/modelrunner/state_facts.go:102` |
| MOD-230 | `conformance/modelrunner/state_facts.go:121` | `conformance/modelrunner/state_facts.go:122` |
| MOD-231 | `conformance/modelrunner/state_facts.go:141` | `conformance/modelrunner/state_facts.go:142` |
| MOD-232 | `conformance/modelrunner/state_facts.go:158` | `conformance/modelrunner/state_facts.go:159` |
| MOD-233 | `conformance/modelrunner/state_facts.go:206` | `conformance/modelrunner/state_facts.go:207` |
| MOD-234 | `conformance/modelrunner/state_facts.go:225` | `conformance/modelrunner/state_facts.go:226` |
| MOD-235 | `conformance/modelrunner/state_facts.go:244` | `conformance/modelrunner/state_facts.go:245` |
| MOD-236 | `conformance/modelrunner/state_facts.go:268` | `conformance/modelrunner/state_facts.go:269` |
| MOD-239 | `conformance/modelrunner/workload_cardinality.go:83` | `conformance/modelrunner/workload_cardinality.go:84` |
| MOD-242 | `conformance/modelrunner/workload_cardinality.go:175` | `conformance/modelrunner/workload_cardinality.go:176` |
| MOD-243 | `conformance/modelrunner/workload_cardinality.go:245` | `conformance/modelrunner/workload_cardinality.go:246` |
| MOD-244 | `conformance/modelrunner/workload_cardinality.go:326` | `conformance/modelrunner/workload_cardinality.go:332` |
| MOD-245 | `conformance/modelrunner/workload_cardinality.go:371` | `conformance/modelrunner/workload_cardinality.go:373` |
| MOD-246 | `conformance/modelrunner/workload_cardinality.go:416` | `conformance/modelrunner/workload_cardinality.go:417` |
| MOD-247 | `conformance/modelrunner/workload_cardinality.go:446` | `conformance/modelrunner/workload_cardinality.go:447` |
| MOD-248 | `conformance/modelrunner/workload_cardinality.go:581` | `conformance/modelrunner/workload_cardinality.go:583` |
| MOD-249 | `conformance/modelrunner/workload_cardinality.go:639` | `conformance/modelrunner/workload_cardinality.go:642` |
| MOD-250 | `conformance/modelrunner/workload_cardinality.go:729` | `conformance/modelrunner/workload_cardinality.go:731` |
| MOD-251 | `conformance/modelrunner/workload_cardinality.go:775` | `conformance/modelrunner/workload_cardinality.go:777` |
| MOD-252 | `conformance/modelrunner/workload_cardinality.go:813` | `conformance/modelrunner/workload_cardinality.go:815` |
| MOD-253 | `conformance/modelrunner/workload_cardinality_test.go:45` | `conformance/modelrunner/workload_cardinality_test.go:46` |
| MOD-255 | `conformance/modelrunner/workload_cardinality_test.go:128` | `conformance/modelrunner/workload_cardinality_test.go:129` |
| MOD-256 | `conformance/modelrunner/workload_cardinality_test.go:165` | `conformance/modelrunner/workload_cardinality_test.go:167` |
| MOD-257 | `conformance/modelrunner/workload_configured_limits.go:51` | `conformance/modelrunner/workload_configured_limits.go:52` |
| MOD-260 | `conformance/modelrunner/workload_configured_limits.go:166` | `conformance/modelrunner/workload_configured_limits.go:167` |
| MOD-261 | `conformance/modelrunner/workload_configured_limits.go:200` | `conformance/modelrunner/workload_configured_limits.go:201` |
| MOD-262 | `conformance/modelrunner/workload_configured_limits.go:346` | `conformance/modelrunner/workload_configured_limits.go:347` |
| MOD-263 | `conformance/modelrunner/workload_configured_limits.go:551` | `conformance/modelrunner/workload_configured_limits.go:552` |
| MOD-264 | `conformance/modelrunner/workload_queue_limits.go:24` | `conformance/modelrunner/workload_queue_limits.go:25` |
| MOD-266 | `conformance/modelrunner/workload_queue_limits.go:161` | `conformance/modelrunner/workload_queue_limits.go:163` |
| MOD-267 | `conformance/modelrunner/workload_queue_limits.go:172` | `conformance/modelrunner/workload_queue_limits.go:174` |
| MOD-268 | `conformance/modelrunner/workload_queue_limits.go:184` | `conformance/modelrunner/workload_queue_limits.go:186` |
| MOD-269 | `conformance/modelrunner/workload_queue_limits.go:203` | `conformance/modelrunner/workload_queue_limits.go:204` |
| MOD-270 | `conformance/modelrunner/workload_queue_limits.go:224` | `conformance/modelrunner/workload_queue_limits.go:226` |
| MOD-271 | `conformance/modelrunner/workload_queue_limits.go:257` | `conformance/modelrunner/workload_queue_limits.go:258` |
| MOD-272 | `conformance/modelrunner/workload_queue_limits.go:491` | `conformance/modelrunner/workload_queue_limits.go:493` |
| MOD-273 | `conformance/modelrunner/workload_queue_limits_test.go:36` | `conformance/modelrunner/workload_queue_limits_test.go:37` |
| MOD-276 | `conformance/modelrunner/workload_queue_limits_test.go:101` | `conformance/modelrunner/workload_queue_limits_test.go:102` |
| MOD-277 | `conformance/modelrunner/workload_queue_limits_test.go:122` | `conformance/modelrunner/workload_queue_limits_test.go:124` |
| MOD-279 | `conformance/modelrunner/workload_queue_limits_test.go:169` | `conformance/modelrunner/workload_queue_limits_test.go:170` |
| MOD-283 | `conformance/modelrunner/workload_queue_limits_test.go:234` | `conformance/modelrunner/workload_queue_limits_test.go:235` |
| MOD-287 | `conformance/modelrunner/workload_queue_limits_test.go:293` | `conformance/modelrunner/workload_queue_limits_test.go:295` |
| MOD-288 | `conformance/modelrunner/workload_topology.go:45` | `conformance/modelrunner/workload_topology.go:47` |
| MOD-289 | `conformance/modelrunner/workload_topology.go:172` | `conformance/modelrunner/workload_topology.go:174` |
| MOD-290 | `conformance/modelrunner/workload_topology.go:193` | `conformance/modelrunner/workload_topology.go:194` |
| MOD-291 | `conformance/modelrunner/workload_topology.go:230` | `conformance/modelrunner/workload_topology.go:231` |
| MOD-292 | `conformance/modelrunner/workload_topology.go:257` | `conformance/modelrunner/workload_topology.go:258` |
| MOD-293 | `conformance/modelrunner/workload_topology.go:262` | `conformance/modelrunner/workload_topology.go:263` |
| MOD-294 | `conformance/modelrunner/workload_topology.go:277` | `conformance/modelrunner/workload_topology.go:279` |
| MOD-295 | `conformance/modelrunner/workload_topology.go:313` | `conformance/modelrunner/workload_topology.go:315` |
| MOD-296 | `conformance/modelrunner/workload_topology.go:336` | `conformance/modelrunner/workload_topology.go:338` |
| MOD-297 | `conformance/modelrunner/workload_topology.go:362` | `conformance/modelrunner/workload_topology.go:364` |
| MOD-298 | `conformance/modelrunner/workload_topology.go:395` | `conformance/modelrunner/workload_topology.go:397` |
| MOD-299 | `conformance/modelrunner/workload_topology.go:435` | `conformance/modelrunner/workload_topology.go:437` |
| MOD-300 | `conformance/modelrunner/workload_topology.go:457` | `conformance/modelrunner/workload_topology.go:463` |
| MOD-301 | `conformance/modelrunner/workload_topology.go:531` | `conformance/modelrunner/workload_topology.go:533` |
| MOD-302 | `conformance/modelrunner/workload_topology.go:594` | `conformance/modelrunner/workload_topology.go:596` |
| MOD-303 | `conformance/modelrunner/workload_topology.go:608` | `conformance/modelrunner/workload_topology.go:609` |
| MOD-304 | `conformance/modelrunner/workload_topology_test.go:23` | `conformance/modelrunner/workload_topology_test.go:24` |
| MOD-308 | `conformance/modelrunner/workload_topology_test.go:246` | `conformance/modelrunner/workload_topology_test.go:248` |
